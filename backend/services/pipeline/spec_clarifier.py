"""Spec Clarification service — Phase 14 / T-USE-03.

Two entry points:

  request_clarifying_questions(workspace, db, redis) -> list[dict]
    Calls the cheap judge model (Claude Haiku / GPT-4o Mini / Gemini Flash —
    same selector as services.evals.online_eval) to produce 3–5 targeted
    clarifying questions about the workspace's problem statement. The
    questions are cached in Redis for 900 seconds under a round key so
    ``persist_answers`` can validate that user-submitted answers belong
    to the most recently shown round. Best-effort: a 30-second asyncio
    timeout (sized for the reasoning judge model) plus broad exception
    swallowing keeps the standard /generate path open if the judge model
    is unreachable.

  persist_answers(
      workspace_id, answers, db, redis, mode="round", workspace=None
  ) -> None
    Validates each answer's ``question`` against either the cached Redis
    round (first-time flow) or already-saved workspace Q&A (retry/edit flow),
    then sanitises the answer text (sanitize_text + PromptGuard) before
    writing the Q&A pairs to ``Workspace.clarification_qa`` as JSONB.

Design invariants enforced by the harness:

  - Clarification is free — this module never charges the credit ledger.
    The credit service is intentionally not imported here.
  - The judge-model selector ``JUDGE_MODELS[provider]`` is reused from
    ``services.llm.provider_config`` — no hand-rolled model picks.
  - Every persisted answer flows through ``sanitize_text`` and the
    ``PromptGuard`` scanner (the same pipeline applied to the original
    problem statement). User-supplied text is never trusted.
  - An ``asyncio.timeout(_JUDGE_TIMEOUT_SECONDS)`` wraps the judge-model
    call so a stuck provider cannot block the modal indefinitely; the
    bound is sized for the reasoning judge, well under the frontend cap.

See V1 spec.md §4.4.1, Plan v1.md §18.3, tasks.md T-162.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Literal
from uuid import UUID

from redis.asyncio import Redis
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

import prompts.spec_clarification as clarification_prompt
from config import settings
from models.stage import Stage
from models.workspace import Workspace
from services.llm.cost_ledger import LLMCostContext
from services.llm.gateway import get_llm
from services.llm.instrumented_adapter import InstrumentedAdapter
from services.llm.routing import LLMRoutingError, resolve_platform_judge_route
from services.observability import record_judge_call
from services.pipeline.problem_compressor import get_or_compress, problem_budget
from services.security.prompt_guard import PromptGuard
from services.security.sanitizer import sanitize_text

logger = logging.getLogger(__name__)

# Redis key namespace for the current clarification round (the questions
# the workspace owner most recently saw). TTL is short — clarification
# is a transient pre-generate step, not a persisted dialogue.
_ROUND_KEY_PREFIX = "clarify_round:"
_ROUND_TTL_SECONDS = 900  # 15 minutes — generous slack for slow typists

# Judge-model invocation must never block the modal indefinitely, but the
# clarification judge is now a *reasoning* model (e.g. GPT-5.4 Mini at medium
# effort), whose hidden reasoning routinely runs longer than a non-reasoning
# judge. The old 5s bound was sized for GPT-4o Mini and silently killed the
# slower reasoning call, returning [] -> 204 -> the modal bypassed with no
# questions shown. 30s gives reasoning room while staying well under the
# frontend's 90s sync timeout. The bound is a ceiling, not added latency: a
# fast call still returns immediately.
_JUDGE_TIMEOUT_SECONDS = 30.0

# Output budget for the judge call. Reasoning models bill hidden reasoning
# tokens against this same budget (see services.llm.output_budget), so 600 —
# sized for a non-reasoning judge — could be fully consumed by reasoning,
# leaving the visible JSON empty/truncated -> [] -> 204 -> silent bypass.
# 2048 leaves comfortable headroom for reasoning plus the short JSON array.
# This does not raise cost: providers bill actual output tokens, not the cap.
_JUDGE_MAX_OUTPUT_TOKENS = 2048

# Bounds on the parsed JSON array we accept from the judge model.
_MIN_QUESTIONS = 1
_MAX_QUESTIONS = 5

# The judge model is asked to produce a short JSON array. We do not trust
# any natural-language preface or trailing prose — the regex extracts the
# first ``[ ... ]`` group with DOTALL so multi-line JSON is matched
# verbatim.
_JSON_ARRAY_RE = re.compile(r"\[.*\]", re.DOTALL)

# Singleton scanner — stateless, cheap to instantiate but no reason to
# allocate one per call.
_prompt_guard = PromptGuard()


def _round_key(workspace_id: UUID) -> str:
    return f"{_ROUND_KEY_PREFIX}{workspace_id}"


def _parse_questions(raw: str) -> list[dict[str, str]]:
    """Best-effort parse the judge-model output to a list of {question, why_it_matters}.

    Returns ``[]`` on any failure — empty/garbled output, JSON decode error,
    or shape mismatch. The route translates an empty list to 204 No Content
    so the frontend can silently bypass the modal.
    """
    if not raw:
        return []
    match = _JSON_ARRAY_RE.search(raw)
    if not match:
        return []
    try:
        data: Any = json.loads(match.group(0))
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    cleaned: list[dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        question = item.get("question")
        why = item.get("why_it_matters")
        if not isinstance(question, str) or not isinstance(why, str):
            continue
        question = question.strip()
        why = why.strip()
        if not question or not why:
            continue
        cleaned.append({"question": question[:500], "why_it_matters": why[:500]})
        if len(cleaned) >= _MAX_QUESTIONS:
            break
    return cleaned if len(cleaned) >= _MIN_QUESTIONS else []


async def request_clarifying_questions(
    workspace: Workspace,
    redis: Redis,
) -> list[dict[str, str]]:
    """Ask the judge model for 3–5 clarifying questions about the problem statement.

    Returns an empty list on any failure (timeout, provider error, bad JSON,
    rate-limit-from-provider). The caller is expected to return 204 in that
    case so the frontend silently bypasses the modal.
    """
    try:
        route = resolve_platform_judge_route(
            operation="clarify.questions",
            latency_class="interactive",
        )
    except LLMRoutingError:
        logger.warning("clarify_no_platform_route")
        return []
    provider, judge_model = route.provider, route.model

    system_prompt = clarification_prompt.SYSTEM_PROMPT
    # Compression is for *long*, not *vague* (plan §4): the clarifier still runs
    # on short/rambling input to *add* structure — get_or_compress is a
    # byte-identical no-op under budget, so only an over-budget statement is
    # condensed before the judge sees it, keeping this call bounded too.
    statement = workspace.problem_statement
    if settings.problem_statement_compression:
        statement = await get_or_compress(
            statement,
            problem_budget(provider, judge_model),
            redis,
            provider,
            judge_model,
            cost_context=LLMCostContext(
                workspace_id=workspace.id,
                product_surface="clarifier",
            ),
        )
    user_prompt = clarification_prompt.build_user_prompt(statement)

    try:
        adapter = InstrumentedAdapter(
            get_llm(provider, judge_model),
            provider=provider,
            model=judge_model,
            stage_type="spec",
            action="clarify",
            model_tier="small",
            # Finding #10: without this, telemetry recorded the adapter default
            # prompt_version="local" for every clarifier call, making edits to
            # spec_clarification.py invisible to the cost ledger.
            prompt_version=(clarification_prompt.SPEC_CLARIFICATION_PROMPT_VERSION),
            operation="clarify.questions",
            cost_context=LLMCostContext(
                workspace_id=workspace.id,
                product_surface="clarifier",
            ),
        )
        record_judge_call("clarify")
        async with asyncio.timeout(_JUDGE_TIMEOUT_SECONDS):
            raw = await adapter.complete(
                system_prompt, user_prompt, max_tokens=_JUDGE_MAX_OUTPUT_TOKENS
            )
    except asyncio.TimeoutError:
        logger.info(
            "clarify_judge_timeout",
            extra={"workspace_id": str(workspace.id), "provider": provider},
        )
        return []
    except Exception:
        # Best-effort: any provider exception (auth, network, rate limit,
        # malformed response) silently falls through to standard generate.
        logger.warning(
            "clarify_judge_error",
            extra={"workspace_id": str(workspace.id), "provider": provider},
            exc_info=True,
        )
        return []

    questions = _parse_questions(raw)
    if not questions:
        logger.info(
            "clarify_judge_returned_unparseable",
            extra={"workspace_id": str(workspace.id), "raw_snippet": raw[:200]},
        )
        return []

    # Cache the round so PATCH can validate that submitted answers belong
    # to the questions the user actually saw. Only the question text is
    # cached; why_it_matters is presentation-only.
    questions_in_round = [q["question"] for q in questions]
    await redis.setex(
        _round_key(workspace.id),
        _ROUND_TTL_SECONDS,
        json.dumps(questions_in_round),
    )
    return questions


class ClarificationValidationError(Exception):
    """Raised when a submitted answer's question is not in the cached round."""


ClarificationSubmitMode = Literal["round", "existing"]


def _question_from_qa(item: Any) -> str | None:
    if isinstance(item, dict):
        value = item.get("question")
    else:
        value = getattr(item, "question", None)
    return value if isinstance(value, str) and value.strip() else None


async def _allowed_questions_for_mode(
    workspace_id: UUID,
    redis: Redis,
    mode: ClarificationSubmitMode,
    workspace: Workspace | None,
) -> set[str]:
    if mode == "round":
        raw_round = await redis.get(_round_key(workspace_id))
        if not raw_round:
            raise ClarificationValidationError("clarification_round_expired")
        try:
            loaded = json.loads(raw_round)
        except json.JSONDecodeError:
            raise ClarificationValidationError("clarification_round_corrupt") from None
        if not isinstance(loaded, list):
            raise ClarificationValidationError("clarification_round_corrupt")
        return {q for q in loaded if isinstance(q, str)}

    existing = getattr(workspace, "clarification_qa", None) if workspace else None
    if not existing:
        raise ClarificationValidationError("clarification_answers_missing")

    allowed = {
        question
        for item in existing
        if (question := _question_from_qa(item)) is not None
    }
    if not allowed:
        raise ClarificationValidationError("clarification_answers_missing")
    return allowed


async def persist_answers(
    workspace_id: UUID,
    answers: list[dict[str, str]],
    db: AsyncSession,
    redis: Redis,
    *,
    mode: ClarificationSubmitMode = "round",
    workspace: Workspace | None = None,
) -> None:
    """Sanitise and persist user-supplied clarification answers.

    Raises ``ClarificationValidationError`` if any submitted ``question`` is
    not in the current validation source: Redis for ``mode="round"`` or the
    saved workspace Q&A for ``mode="existing"``. The source is loaded once at
    the start of the call and the check is O(1) per answer.

    Each answer's ``answer`` text is sanitised (HTML stripped) and scanned
    by ``PromptGuard``; if the guard rejects the answer it is dropped from
    the persisted set but the rest are kept. This mirrors how the existing
    problem-statement gate handles a single bad input — graceful, not
    aggressive — but answers that pass through are guaranteed safe to
    inject into the spec prompt.
    """
    allowed_questions = await _allowed_questions_for_mode(
        workspace_id,
        redis,
        mode,
        workspace,
    )
    unknown_question_error = (
        "question_not_in_existing_answers"
        if mode == "existing"
        else "question_not_in_round"
    )

    cleaned: list[dict[str, str]] = []
    for entry in answers:
        question = entry.get("question", "")
        answer = entry.get("answer", "")
        if question not in allowed_questions:
            raise ClarificationValidationError(unknown_question_error)
        if not answer.strip():
            continue
        sanitised = sanitize_text(answer)
        scan = _prompt_guard.scan(sanitised)
        if not scan.is_safe:
            logger.warning(
                "clarify_answer_rejected_by_guard",
                extra={
                    "workspace_id": str(workspace_id),
                    "pattern": scan.matched_pattern,
                },
            )
            continue
        cleaned.append({"question": question[:1000], "answer": sanitised[:1000]})

    if not cleaned:
        if mode == "existing":
            raise ClarificationValidationError("clarification_answers_empty")
        # Caller treats this as a no-op success — same as Skip.
        return

    await db.execute(
        update(Workspace)
        .where(Workspace.id == workspace_id)
        .values(clarification_qa=cleaned)
    )
    # The workspace UPDATE holds the same row lock as finalisation. Invalidate
    # existing artifacts in that transaction so a concurrent finalise cannot
    # leave an artifact marked current after its answers changed.
    await db.execute(
        update(Stage)
        .where(
            Stage.workspace_id == workspace_id,
            Stage.status.in_(["draft", "finalised"]),
            Stage.content.is_not(None),
        )
        .values(status="stale")
    )
    await db.commit()
    # The round key has done its job, and existing-mode saves should also clear
    # any stale round so the next new clarification starts from a clean state.
    await redis.delete(_round_key(workspace_id))
