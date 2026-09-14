"""Delta-aware increment generation (Phase 21 — T-279).

A finalised workspace evolves as a versioned **timeline** — v1 → Increment 1 →
Increment 2 → … — that absorbs new features as **deltas** rather than freezing at
the first export or churning the whole pipeline on a full re-run (spec §4.14.7).

The load-bearing property is a **stable, content-derived ``task_ref``** (spec
Assumption 23): every task the baseline already carries keeps the *same* ref
across the increment, so the incremental GitHub sync (T-280) updates that task's
existing Issue instead of opening a duplicate; only genuinely new tasks get new
refs and therefore new Issues. That key is decided once in Phase A
(``task_parser.compute_task_ref``) and *consumed* here — never re-invented.

How a delta is produced (additive path — the MVP):

1. The finalised stages are the **baseline**: immutable context the prompt is
   explicitly told never to rewrite, renumber, or re-emit. (Treating the baseline
   as mutable is exactly the failure that regresses an increment into a full,
   churny regeneration.)
2. The model returns **only the new tasks** required for the feature request, in
   the same ``### T-NNN: <title>`` shape as TASKS.md.
3. Output is reconciled against the baseline: any task whose content-derived
   ``task_ref`` already exists is dropped (defence-in-depth against a model that
   re-emits an existing task), and the genuinely new tasks are renumbered to
   continue after the highest baseline ``T-NNN``.
4. The new tasks are **appended** to the Tasks stage as a new ``StageVersion`` —
   the workspace's TASKS.md grows; the baseline versions are pinned on the
   ``Increment`` row (``baseline_version_ids``) so the delta is reproducible.

Behaviour-changing increments — compute the blast radius, mark affected items
``stale``, and re-run harness/critic *only* on the affected areas — are the
phase-two cut (spec "v2 fast-follow"). They are scoped here but gated behind
``settings.increment_blast_radius_enabled`` (default off); the additive path is
the only one that ships in the MVP.

Generation is credit-aware and refund-safe, consistent with the rest of the
pipeline: credits are deducted up front and refunded on any failure, and the
increment row is left in a retryable ``draft`` state. An increment is only worth
charging less than a full generation because it is genuinely scoped — token usage
is measured and logged so that claim stays honest.
"""

from __future__ import annotations

import asyncio
import logging
import re
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database import get_shared_redis
from models import (
    CreditLedger,
    GitHubInstallation,
    Increment,
    IntegrationPush,
    IntegrationPushTask,
    Stage,
    StageVersion,
    Workspace,
)
from prompts.base import SECURITY_AND_PRIVACY_RULES, wrap_untrusted_content
from services.credit_service import InsufficientCreditsError, credit_service
from services.integrations.task_issue_reconcile import retire_obsolete_task_issues
from services.integrations.task_parser import (
    AGENT_LABELS,
    ParsedTask,
    compute_task_ref,
    parse_tasks,
)
from services.integrations.task_ref_migration import migrate_legacy_task_refs
from services.llm.base import ProviderError, ProviderTimeoutError
from services.llm.cost_ledger import LLMCostContext
from services.llm.gateway import get_llm
from services.llm.instrumented_adapter import InstrumentedAdapter
from services.llm.output_budget import output_budget_for_operation
from services.llm.routing import (
    LLMRoutingError,
    platform_provider_priority,
    resolve_platform_route_by_provider,
)
from services.llm.tier_policy import generation_tier_policy
from services.observability import (
    GITHUB_AUDIT_INCREMENT_PUSHED,
    GITHUB_AUDIT_PR_OPENED,
    GITHUB_PR_TOTAL,
    github_audit,
)
from services.pipeline.diff_engine import markdown_fences_balanced
from services.security.output_validator import validate_async
from services.security.prompt_guard import scan_async
from services.security.sanitizer import sanitize_text
from services.text_compaction import compact_text

logger = logging.getLogger(__name__)

# A scoped delta is cheaper than a full 10-credit generation but never free — the
# increment still runs a real model call. Held here (not in ``CREDIT_COSTS``) so
# the increment surface owns its own pricing without touching the shared table;
# passed to ``credit_service.deduct`` with ``reason="increment"``.
INCREMENT_CREDIT_COST = 5

# A 'generating' increment older than this is considered stuck — its in-request
# refund never ran (hard process death) — and is refunded + reset to a retryable
# 'draft' by the recovery sweep (audit finding #7). Sized far above the hard LLM
# timeout (settings.llm_complete_timeout_seconds, default 120s) so a legitimately
# in-flight increment is never falsely swept; matches the storyboard threshold's
# generous margin.
INCREMENT_STUCK_THRESHOLD_MINUTES = 15

# Increment generation produces TASKS-shaped output and shares the
# ``tasks.generate`` operation/budget. It follows the same product-wide
# cheap-primary→mid policy as TASKS generation (issue #17 follow-up): the cheap
# tier is the primary and mid is the one-shot escalation. Scoped deltas still
# need reliable traceability and stable task refs, which the cheap primary
# carries via the same operation contract.
_INCREMENT_OPERATION = "tasks.generate"

# Cap the persisted increment title so an oversized feature request cannot bloat
# the row; the full request still drives generation.
_TITLE_MAX_LEN = 120

# Matches a ``### T-NNN: <title>`` heading so the highest baseline number can be
# found and new tasks renumbered to continue after it.
_TASK_HEADING = re.compile(r"^###\s+T-(\d+):", re.MULTILINE)


class IncrementError(Exception):
    """Base class for increment-generation failures."""


class IncrementBaselineError(IncrementError):
    """The workspace is not in a state that can be incremented (e.g. a stage is
    not finalised, so there is no immutable baseline to delta against)."""


class IncrementGenerationError(IncrementError):
    """The model returned no usable delta (no new tasks after reconciliation)."""


class IncrementModeNotSupportedError(IncrementError):
    """A behaviour-changing increment was requested while the blast-radius path
    is still gated off (MVP ships additive only)."""


@dataclass(frozen=True)
class IncrementTask:
    """A single new task introduced by an increment.

    ``human_ref`` is the renumbered ``T-NNN`` heading (continues after the
    baseline); ``task_ref`` is the stable, content-derived matching key used by
    sync to find/create the Issue. The two bodies mirror :class:`ParsedTask`.
    """

    human_ref: str
    task_ref: str
    title: str
    body_md: str
    agent_body_md: str
    raw_body: str = ""
    labels: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class IncrementResult:
    """Outcome of a delta generation."""

    increment_id: UUID
    sequence: int
    title: str
    new_tasks: list[IncrementTask]
    pinned_task_refs: list[str]
    tasks_version_id: UUID


_STAGE_ORDER = ("spec", "plan", "harness", "tasks")


class IncrementService:
    """Generates a delta increment on a finalised workspace baseline (T-279)."""

    def __init__(self, redis_client: Redis | None = None) -> None:
        self._redis = redis_client

    async def _redis_conn(self) -> Redis:
        if self._redis is None:
            self._redis = get_shared_redis()
        return self._redis

    async def generate_increment(
        self,
        workspace_id: UUID,
        feature_request: str,
        user,
        db: AsyncSession,
        *,
        mode: str = "additive",
    ) -> IncrementResult:
        """Generate an additive increment from ``feature_request``.

        The baseline (all four finalised stages) is fixed context; the output is a
        **diff** — only the new tasks — appended to TASKS.md as a new version with
        stable, content-derived ``task_ref``s. Credits are deducted up front and
        refunded on any failure.
        """
        if mode not in ("additive", "behaviour_changing"):
            raise ValueError(f"Unknown increment mode: {mode!r}")
        if mode == "behaviour_changing" and not settings.increment_blast_radius_enabled:
            # Phase-two cut: blast-radius analysis is scoped but not shipped.
            raise IncrementModeNotSupportedError(
                "Behaviour-changing increments (blast-radius analysis) are not yet "
                "available. The MVP ships the additive path only."
            )

        scan_result = await scan_async(feature_request)
        if not scan_result.is_safe:
            raise IncrementError(
                f"Feature request flagged: {scan_result.matched_pattern}"
            )

        workspace = await self._load_workspace(workspace_id, db)
        stages = await self._load_finalised_stages(db, workspace_id)
        baseline_version_ids = await self._baseline_version_ids(db, stages)

        baseline_tasks_md = stages["tasks"].content or ""
        baseline_parsed = parse_tasks(baseline_tasks_md)
        # Two different questions, two different keys:
        #   pinned_refs      — the baseline's PERSISTED identities, shown to the
        #                      model so it does not re-emit existing work.
        #   pinned_ref_set   — "does a task with this TITLE already exist?", the
        #                      dedup key for _reconcile_delta. It must be the
        #                      unsalted base ref: the delta is a separate
        #                      document, so its own occurrence numbering is
        #                      unrelated to the baseline's and comparing salted
        #                      refs across the two would let a re-emitted task
        #                      through as new.
        pinned_refs = [t.task_ref for t in baseline_parsed]
        pinned_ref_set = {compute_task_ref(t.title) for t in baseline_parsed}
        baseline_max = _highest_task_number(baseline_tasks_md)

        route = self._resolve_route(workspace)
        sequence = await self._next_sequence(db, workspace_id)
        title = _derive_title(feature_request)

        increment = Increment(
            workspace_id=workspace_id,
            sequence=sequence,
            title=title,
            status="generating",
            baseline_version_ids=baseline_version_ids,
        )
        db.add(increment)
        await db.flush()
        increment_id = increment.id

        # Tx1 — reserve: deduct and COMMIT *before* the slow LLM call so the
        # user-row FOR UPDATE lock deduct() takes is released immediately rather
        # than held across the whole generation (audit finding #6). deduct()
        # raises InsufficientCreditsError before any spend if the balance is
        # short — roll the placeholder back so no orphan 'generating' row
        # survives. The committed deduction is pinned on the increment so the
        # recovery sweep can refund it idempotently if a hard death strands the
        # row mid-generation (finding #7); the in-request except path below is
        # the primary refund and the sweep is the backstop.
        try:
            deduction = await credit_service.deduct(
                db, user.id, INCREMENT_CREDIT_COST, "increment"
            )
        except InsufficientCreditsError:
            await db.rollback()
            raise
        deduction_id = deduction.id
        increment.deduction_ledger_id = deduction_id
        await db.commit()
        await credit_service.invalidate(user.id)  # post-commit — H-2 (T-219)

        system_prompt = _system_prompt()
        user_prompt = _user_prompt(stages, feature_request, baseline_max)

        # Generate with NO open transaction and NO row lock held across the model
        # call. On any failure: refund the (already-committed) deduction and reset
        # the increment to a retryable 'draft' in a fresh transaction.
        try:
            adapter = InstrumentedAdapter(
                get_llm(route.provider, route.model),
                provider=route.provider,
                model=route.model,
                stage_type="tasks",
                action="increment",
                model_tier=route.model_tier,
                prompt_version=INCREMENT_PROMPT_VERSION,
                operation=route.operation,
                cost_context=LLMCostContext(
                    workspace_id=workspace_id,
                    increment_id=increment_id,
                    credit_reason="increment",
                    product_surface="increment",
                ),
            )
            completion = await asyncio.wait_for(
                adapter.complete(
                    system_prompt,
                    user_prompt,
                    max_tokens=output_budget_for_operation(
                        route.operation, route.provider
                    ),
                ),
                timeout=settings.llm_complete_timeout_seconds,
            )

            # Truncation handling (audit M9), mirroring the core pipeline's
            # refund discriminator: the provider hard-stopping on its output
            # cap is an objective fact and fails the increment (→ refund via
            # the except path below); a merely-missing sentinel on a naturally
            # finished response is a formatting heuristic and only logs.
            completion_info = getattr(adapter, "last_completion", None)
            if getattr(completion_info, "stopped_by_limit", False):
                raise IncrementError(
                    "Increment output hit the provider output-token cap and "
                    "is truncated."
                )
            completion, sentinel_present = _strip_increment_sentinel(completion)
            if not sentinel_present:
                logger.warning(
                    "increment.completion_sentinel_missing increment_id=%s",
                    increment_id,
                )

            validation = await validate_async(completion)
            if not validation.is_safe:
                raise IncrementError(
                    f"Increment output failed validation: {validation.reason}"
                )

            new_tasks = _reconcile_delta(completion, pinned_ref_set, baseline_max)
            if not new_tasks:
                raise IncrementGenerationError(
                    "The increment produced no new tasks for this feature request."
                )

            new_tasks_md = _grow_tasks_markdown(baseline_tasks_md, new_tasks)
            if not markdown_fences_balanced(new_tasks_md):
                raise IncrementError(
                    "Increment output would leave Markdown code fences unbalanced."
                )
        except asyncio.CancelledError:
            # Infrastructure shutdowns and any future server-side cancellation
            # must not strand a charge or a permanently-generating row. A
            # browser disconnect alone is not treated as cancellation because
            # ASGI does not guarantee that it stops the handler.
            await self._refund_and_mark_draft(db, increment_id, deduction_id, user)
            raise
        except (
            ProviderError,
            ProviderTimeoutError,
            IncrementError,
            asyncio.TimeoutError,
        ) as exc:
            await self._refund_and_mark_draft(db, increment_id, deduction_id, user)
            if isinstance(exc, asyncio.TimeoutError):
                raise ProviderError(route.provider, exc) from exc
            raise

        # Tx2 — finalise: reload the increment under lock and persist. If the
        # recovery sweep already failed+refunded it (only possible after a stall
        # far longer than the hard LLM timeout), do NOT resurrect it.
        version_id = await self._finalise_increment(
            db, increment_id, stages["tasks"], new_tasks_md
        )

        await self._invalidate_tasks_cache(workspace_id)

        _log_token_usage(
            increment_id=increment_id,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            completion=completion,
            new_task_count=len(new_tasks),
        )

        return IncrementResult(
            increment_id=increment_id,
            sequence=sequence,
            title=title,
            new_tasks=new_tasks,
            pinned_task_refs=pinned_refs,
            tasks_version_id=version_id,
        )

    # ------------------------------------------------------------------ helpers

    def _resolve_route(self, workspace: Workspace):
        provider_policies = {
            provider: generation_tier_policy(provider)
            for provider in platform_provider_priority()
        }
        try:
            return resolve_platform_route_by_provider(
                operation=_INCREMENT_OPERATION,
                tier_policy=provider_policies,
                latency_class="interactive",
            )
        except LLMRoutingError as exc:
            raise IncrementError("AI generation is temporarily unavailable.") from exc

    async def _load_workspace(self, workspace_id: UUID, db: AsyncSession) -> Workspace:
        workspace = (
            await db.execute(select(Workspace).where(Workspace.id == workspace_id))
        ).scalar_one_or_none()
        if workspace is None:
            raise IncrementBaselineError("Workspace not found")
        return workspace

    async def _load_finalised_stages(
        self, db: AsyncSession, workspace_id: UUID
    ) -> dict[str, Stage]:
        result = await db.execute(
            select(Stage).where(Stage.workspace_id == workspace_id)
        )
        stages = {s.type: s for s in result.scalars()}
        for stage_type in _STAGE_ORDER:
            stage = stages.get(stage_type)
            if stage is None or stage.status != "finalised":
                raise IncrementBaselineError(
                    f"Stage {stage_type!r} is not finalised — a workspace must be "
                    "finalised before it can be incremented."
                )
        return stages

    async def _baseline_version_ids(
        self, db: AsyncSession, stages: dict[str, Stage]
    ) -> list[str]:
        """Pin the immutable StageVersion ids that form the delta's baseline."""
        ids: list[str] = []
        for stage_type in _STAGE_ORDER:
            stage = stages[stage_type]
            version_id = (
                await db.execute(
                    select(StageVersion.id).where(
                        StageVersion.stage_id == stage.id,
                        StageVersion.version == stage.current_version,
                    )
                )
            ).scalar_one_or_none()
            if version_id is not None:
                ids.append(str(version_id))
        return ids

    async def _next_sequence(self, db: AsyncSession, workspace_id: UUID) -> int:
        highest = (
            await db.execute(
                select(func.max(Increment.sequence)).where(
                    Increment.workspace_id == workspace_id
                )
            )
        ).scalar_one_or_none()
        return (highest or 0) + 1

    async def _append_tasks_version(
        self, db: AsyncSession, tasks_stage: Stage, new_content: str
    ) -> UUID:
        """Grow TASKS.md by one additive version.

        The stage stays ``finalised`` — the grown document is still a complete,
        valid TASKS.md — so existing exports/syncs are not staled by the
        increment (drift staling is reserved for an explicit re-finalise, T-273).
        """
        tasks_stage.content = new_content
        tasks_stage.current_version += 1
        tasks_stage.updated_at = datetime.now(UTC)
        version = StageVersion(
            stage_id=tasks_stage.id,
            version=tasks_stage.current_version,
            content=new_content,
            created_by="ai",
        )
        db.add(version)
        await db.flush()
        return version.id

    async def _finalise_increment(
        self,
        db: AsyncSession,
        increment_id: UUID,
        tasks_stage: Stage,
        new_tasks_md: str,
    ) -> UUID:
        """Tx2: append the grown TASKS version and flip the increment to ``ready``.

        Reloads the increment ``FOR UPDATE`` (authoritative under lock, refreshed
        with ``populate_existing``) so a row the recovery sweep already
        terminated + refunded — possible only after a stall far longer than the
        hard LLM timeout — is never resurrected into a ``ready`` state against a
        refunded charge. In that race the delta is dropped and the user retries
        from the clean ``draft`` the sweep left.
        """
        locked = (
            await db.execute(
                select(Increment)
                .where(Increment.id == increment_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if locked is None or locked.status != "generating":
            raise IncrementGenerationError(
                "The increment was reset during generation; please retry."
            )
        version_id = await self._append_tasks_version(db, tasks_stage, new_tasks_md)
        locked.status = "ready"
        locked.updated_at = datetime.now(UTC)
        await db.commit()
        return version_id

    async def _refund_and_mark_draft(
        self, db: AsyncSession, increment_id: UUID, deduction_id: UUID, user
    ) -> None:
        """Refund the increment credit and leave the row in a retryable state.

        The credit is returned exactly once (the ledger refund path is idempotent
        and SAVEPOINT-scoped); the increment is reset to ``draft`` so the user can
        retry without an orphaned ``generating`` row stuck in the timeline.
        Committed here so the refund survives even if the caller's request fails
        afterwards. Co-exists safely with the recovery sweep — both converge the
        row to ``draft`` and the refund is a no-op the second time.
        """
        try:
            await credit_service.refund(db, deduction_id, user.id)
            increment = (
                await db.execute(select(Increment).where(Increment.id == increment_id))
            ).scalar_one_or_none()
            if increment is not None:
                increment.status = "draft"
                increment.updated_at = datetime.now(UTC)
            await db.commit()
            await credit_service.invalidate(user.id)
        except Exception:  # pragma: no cover - defensive cleanup
            logger.exception(
                "increment.refund_cleanup_failed increment_id=%s", increment_id
            )

    async def _invalidate_tasks_cache(self, workspace_id: UUID) -> None:
        try:
            redis = await self._redis_conn()
            await redis.delete(f"stage:{workspace_id}:tasks")
        except RedisError:  # cache eviction is best-effort
            logger.warning(
                "increment.tasks_cache_evict_failed workspace_id=%s", workspace_id
            )


# ---------------------------------------------------------------------------
# Pure helpers (deterministic, unit-testable without a DB)
# ---------------------------------------------------------------------------


def _highest_task_number(tasks_md: str) -> int:
    """Highest ``T-NNN`` number present in TASKS.md, or 0 when none."""
    numbers = [int(m.group(1)) for m in _TASK_HEADING.finditer(tasks_md)]
    return max(numbers, default=0)


def _reconcile_delta(
    completion: str, pinned_ref_set: set[str], baseline_max: int
) -> list[IncrementTask]:
    """Turn raw model output into the genuinely-new tasks.

    Drops any task whose content-derived ``task_ref`` already exists in the
    baseline (a model that re-emits existing work must never duplicate an Issue),
    de-duplicates within the delta itself, and renumbers the survivors to continue
    after the highest baseline ``T-NNN``.
    """
    parsed = parse_tasks(completion)
    seen: set[str] = set()
    new_tasks: list[IncrementTask] = []
    next_number = baseline_max
    for task in parsed:
        # Unsalted base ref on purpose: this is a title-existence test over raw
        # model output, where a repeated title is a generation artifact to drop —
        # not two genuinely distinct tasks the way a repeat in the user's own
        # finalised TASKS.md is. ``task.task_ref`` would salt the second
        # occurrence and let the duplicate through.
        ref = compute_task_ref(task.title)
        if ref in pinned_ref_set or ref in seen:
            continue
        seen.add(ref)
        next_number += 1
        human_ref = f"T-{next_number:03d}"
        new_tasks.append(
            IncrementTask(
                human_ref=human_ref,
                task_ref=ref,
                title=task.title,
                body_md=task.body_md,
                agent_body_md=task.agent_body_md,
                raw_body=task.raw_body,
                labels=task.labels or AGENT_LABELS,
            )
        )
    return new_tasks


def _render_task_section(task: IncrementTask) -> str:
    heading = f"### {task.human_ref}: {task.title}"
    body = task.raw_body.strip()
    return f"{heading}\n\n{body}" if body else heading


def _grow_tasks_markdown(baseline_tasks_md: str, new_tasks: list[IncrementTask]) -> str:
    """Append the new task sections to TASKS.md, existing content pinned verbatim.

    The new sections are re-rendered under their renumbered headings with each
    task's source body preserved verbatim, so the appended block matches the
    source document's shape.
    """
    appended = "\n\n".join(_render_task_section(task) for task in new_tasks)
    base = baseline_tasks_md.rstrip()
    if base:
        return f"{base}\n\n{appended}\n"
    return f"{appended}\n"


# Audit finding #10 (process): increment_service.py was one of three prompts
# with no STAGE_PROMPT_VERSIONS-equivalent constant, so InstrumentedAdapter
# recorded prompt_version="local" for every increment call — edits to
# _system_prompt/_user_prompt below were invisible to telemetry. Bump on any
# future edit to either function (docs/evals/PROMPT_CHANGE_REVIEW.md).
# v2 (prompt-quality audit 2026-07, M9): the baseline artifacts are bounded
# per artifact (they previously embedded the FULL spec+plan+harness+tasks with
# no cap — context overflow and attention dilution on exactly the mature
# workspaces where increments matter most), and the response carries a
# completion sentinel so a truncated delta is detectable.
INCREMENT_PROMPT_VERSION = "increment-prompt-v2"

# Emitted as the response's final line; stripped before parsing. Versioned like
# the core pipeline's sentinels so a format change is detectable.
INCREMENT_COMPLETION_SENTINEL = "<!-- THOUGHT2BUILD_INCREMENT_COMPLETE:v1 -->"

# Per-artifact bounds on the baseline context, summing to prompt_builder's
# _MAX_UPSTREAM_CHARS (200K) total upstream budget (audit M9). TASKS gets the
# largest share — numbering, dedup, and terminology join on it — and
# compact_text keeps head+tail, so the highest task numbers (the tail) always
# survive. A baseline task elided from the middle can at worst be re-proposed
# by the model, which _reconcile_delta already drops deterministically by
# task_ref, so bounding never corrupts the grown document.
_BASELINE_CONTEXT_LIMITS: dict[str, int] = {
    "spec": 50_000,
    "plan": 50_000,
    "harness": 30_000,
    "tasks": 70_000,
}


def _system_prompt() -> str:
    return (
        "You are Thought2Build generating an INCREMENT: a versioned delta on a "
        "finalised workspace baseline.\n\n"
        "The baseline below — SPEC.md, PLAN.md, the test harness, and TASKS.md — "
        "is IMMUTABLE context. You MUST NOT rewrite it, renumber its tasks, "
        "restate its tasks, or reproduce any task it already contains. Treating "
        "the baseline as mutable regresses this into a full regeneration, which is "
        "wrong.\n\n"
        "Output ONLY the NEW tasks required to deliver the requested feature, as a "
        "delta to append to TASKS.md. Use the exact same shape as the baseline "
        "tasks:\n"
        "  ### T-NNN: <concise imperative title>\n"
        "  <the same labelled subsections the baseline tasks use>\n\n"
        "Rules:\n"
        "- Number the new tasks consecutively, continuing AFTER the highest "
        "existing T-NNN.\n"
        "- Reuse the baseline's exact terminology, entity names, requirement IDs, "
        "and test-path conventions. Do not invent synonyms for defined terms.\n"
        "- Reference existing baseline tasks by their T-NNN where a new task "
        "depends on them, but never restate or modify them.\n"
        "- A baseline artifact may contain an elision marker ([... N characters "
        "omitted for context budget ...]) where a middle region was bounded out; "
        "treat elided content as existing, never as absent.\n"
        "- Each new task must be independently implementable and testable.\n"
        "- Emit nothing but the new ### T-NNN task sections — no preamble, no "
        "recap of existing work, no closing commentary.\n"
        "- End the response with this exact sentinel on its own final line, with "
        f"nothing after it: {INCREMENT_COMPLETION_SENTINEL}\n\n"
        f"{SECURITY_AND_PRIVACY_RULES}"
    )


def _user_prompt(
    stages: dict[str, Stage], feature_request: str, baseline_max: int
) -> str:
    sanitized_request = sanitize_text(feature_request)
    spec = compact_text(
        stages["spec"].content or "",
        _BASELINE_CONTEXT_LIMITS["spec"],
        reason="context budget",
    )
    plan = compact_text(
        stages["plan"].content or "",
        _BASELINE_CONTEXT_LIMITS["plan"],
        reason="context budget",
    )
    harness = compact_text(
        stages["harness"].content or "",
        _BASELINE_CONTEXT_LIMITS["harness"],
        reason="context budget",
    )
    tasks = compact_text(
        stages["tasks"].content or "",
        _BASELINE_CONTEXT_LIMITS["tasks"],
        reason="context budget",
    )
    next_number = baseline_max + 1
    return (
        "Baseline workspace (immutable — do not modify or re-emit):\n\n"
        f"{wrap_untrusted_content('baseline_spec', spec)}\n\n"
        f"{wrap_untrusted_content('baseline_plan', plan)}\n\n"
        f"{wrap_untrusted_content('baseline_harness', harness)}\n\n"
        f"{wrap_untrusted_content('baseline_tasks', tasks)}\n\n"
        "Feature request to absorb as an additive increment:\n"
        f"{wrap_untrusted_content('feature_request', sanitized_request)}\n\n"
        f"Append the new tasks only. Start numbering at T-{next_number:03d}. "
        "Do not repeat any task already present in the baseline TASKS."
    )


def _strip_increment_sentinel(completion: str) -> tuple[str, bool]:
    """Remove the trailing completion sentinel; report whether it was present.

    The sentinel must never survive into the grown TASKS.md (it would land
    inside the final task's body), so it is stripped before parsing. Presence
    is reported for the truncation heuristic in the generation flow.
    """
    lines = completion.rstrip().splitlines()
    if lines and lines[-1].strip() == INCREMENT_COMPLETION_SENTINEL:
        return "\n".join(lines[:-1]).rstrip(), True
    return completion, False


def _derive_title(feature_request: str) -> str:
    """A short, single-line, sanitised title for the increment row."""
    cleaned = sanitize_text(feature_request)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) > _TITLE_MAX_LEN:
        cleaned = cleaned[: _TITLE_MAX_LEN - 1].rstrip() + "…"
    return cleaned or "Untitled increment"


def _approx_tokens(text: str) -> int:
    """Rough token estimate (~4 chars/token) — enough to keep the "an increment
    is only cheaper if genuinely scoped" claim measurable in the logs."""
    return max(1, len(text) // 4)


def _log_token_usage(
    *,
    increment_id: UUID,
    system_prompt: str,
    user_prompt: str,
    completion: str,
    new_task_count: int,
) -> None:
    prompt_tokens = _approx_tokens(system_prompt) + _approx_tokens(user_prompt)
    completion_tokens = _approx_tokens(completion)
    logger.info(
        "increment.generated increment_id=%s new_tasks=%d "
        "approx_prompt_tokens=%d approx_completion_tokens=%d",
        increment_id,
        new_task_count,
        prompt_tokens,
        completion_tokens,
    )


async def recover_stuck_increments(db: AsyncSession) -> int:
    """Refund + reset increments stranded in ``generating`` past the threshold.

    Charge-then-generate (audit finding #6) commits the increment deduction
    before the LLM call, so a hard process death can leave a committed charge
    against a ``generating`` row whose in-request refund (``_refund_and_mark_draft``)
    never ran. This sweep is the backstop (finding #7), mirroring
    ``recover_stuck_storyboards``: it is invoked by the shared leader-locked
    recovery cycle, refunds the pinned ``deduction_ledger_id`` idempotently, resets
    the row to a retryable ``draft``, and commits once. Returns the number
    recovered.

    The threshold is far above the hard LLM timeout
    (``settings.llm_complete_timeout_seconds``), so a legitimately in-flight
    increment can never be falsely swept. The credit-ledger refund is idempotent,
    so it is safe even if the in-request path also refunded.
    """
    cutoff = datetime.now(UTC) - timedelta(minutes=INCREMENT_STUCK_THRESHOLD_MINUTES)
    result = await db.execute(
        select(Increment).where(
            Increment.status == "generating",
            Increment.updated_at < cutoff,
        )
    )
    stuck = list(result.scalars())

    recovered = 0
    refunded_users: set[UUID] = set()
    for increment in stuck:
        credits_refunded = 0
        if increment.deduction_ledger_id is not None:
            # Read the ledger before refunding so we can invalidate the right
            # user's credit cache post-commit (the row has no user_id column).
            ledger = await db.get(CreditLedger, increment.deduction_ledger_id)
            credits_refunded = await credit_service.refund(
                db, increment.deduction_ledger_id
            )
            if ledger is not None:
                refunded_users.add(ledger.user_id)
        increment.status = "draft"
        increment.updated_at = datetime.now(UTC)
        logger.warning(
            "increment.recovery increment_id=%s workspace_id=%s credits_refunded=%d",
            increment.id,
            increment.workspace_id,
            credits_refunded,
        )
        recovered += 1

    if recovered > 0:
        await db.commit()
        # Credit cache must reflect post-refund balances on the next read (H-2).
        for user_id in refunded_users:
            await credit_service.invalidate(user_id)
    return recovered


# ===========================================================================
# Incremental GitHub sync — the ``increment_push`` worker job (T-280)
# ===========================================================================
#
# Pushes a generated increment to GitHub as a layer on already-shipped work:
# **new task_refs → new issues only**, changed tasks (same task_ref) → update
# their issue in place, obsoleted tasks → close their issue with a note (not a
# delete — reversible), every new issue filed under **one milestone per
# increment**, and (in ``pr_with_tests`` mode) **one PR per increment** carrying
# the increment's red stubs.
#
# Idempotency is the load-bearing property. The increment reuses the workspace's
# single live :class:`IntegrationPush` (the partial unique index forbids a second
# live push per repo), so a re-run:
#   - matches tasks against the existing ``IntegrationPushTask`` rows by
#     ``task_ref`` — the same human ``T-NNN`` the baseline export persisted — so a
#     baseline task is updated, never duplicated;
#   - reuses the milestone (looked up by title), the branch (422-tolerant create),
#     and the PR (``find_open_pull_number``) from live GitHub state — no
#     per-increment columns are needed on the push;
#   - reads live repo state first (backfill) so done work stays done.
#
# Crucially, a failed increment push **never marks the shared push ``failed``** —
# that would drop the baseline's bidirectional sync via ``find_live_push``. On
# error the push row is left untouched and the increment stays ``ready`` so arq
# retries/backs-off/dead-letters and a later attempt resumes cleanly.

_DEFAULT_BRANCH = "main"


async def run_increment_push(
    ctx: dict[str, Any],
    increment_id: str,
    *,
    db: AsyncSession | None = None,
    client: Any = None,
) -> Increment | None:
    """Worker entrypoint for the ``increment_push`` job (T-280).

    ``db``/``client`` are injectable for tests; production opens a session and
    builds an App-mode client bound to the baseline push's installation.
    """
    if db is not None:
        return await _run_increment_push(db, increment_id, client=client)
    from database import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        return await _run_increment_push(session, increment_id, client=client)


async def _run_increment_push(
    db: AsyncSession, increment_id: str, *, client: Any = None
) -> Increment | None:
    increment = (
        await db.execute(select(Increment).where(Increment.id == UUID(increment_id)))
    ).scalar_one_or_none()
    if increment is None:
        logger.warning("increment.push.missing increment_id=%s", increment_id)
        return None
    if increment.status not in ("ready", "pushed"):
        # draft/generating increments have nothing to push; a stuck/invalid state
        # is logged and ignored rather than retried (no retry would help).
        logger.warning(
            "increment.push.not_ready increment_id=%s status=%s",
            increment_id,
            increment.status,
        )
        return None

    from services.integrations.push_repo import find_workspace_live_push

    push = await find_workspace_live_push(db, increment.workspace_id)
    if push is None or push.repo_full_name is None or push.repo_id is None:
        # No baseline export to layer on. The endpoint validates this, so reaching
        # here means the baseline was lost; log and stop (retry cannot help).
        logger.warning("increment.push.no_baseline increment_id=%s", increment_id)
        return None

    stages = await _load_push_stages(db, increment.workspace_id)
    tasks = parse_tasks(stages["tasks"].content or "")

    if client is not None:
        await _drive_increment_push(
            db, increment, push, stages, tasks, client, governor=None
        )
        return increment

    # Production: build the App-mode client + governor exactly like the export
    # worker — but the failed-marking wrapper is deliberately NOT reused, so a
    # transient increment-push error cannot drop the baseline push.
    from services.integrations.github_api_client import (
        make_app_github_client,
        make_shared_async_client,
    )
    from services.integrations.github_app_auth import make_token_provider
    from services.integrations.github_governor import make_governor

    installation = await _load_push_installation(db, push.installation_id)
    if installation is None:
        logger.warning(
            "increment.push.installation_missing increment_id=%s", increment_id
        )
        return None

    async with make_shared_async_client() as http:
        redis = get_shared_redis()
        token_provider = make_token_provider(redis, http)
        governor = make_governor(installation.installation_id, redis)
        built = make_app_github_client(
            token_provider, installation.installation_id, http, governor=governor
        )
        await _drive_increment_push(
            db, increment, push, stages, tasks, built, governor=governor
        )
    return increment


async def _drive_increment_push(
    db: AsyncSession,
    increment: Increment,
    push: IntegrationPush,
    stages: dict[str, Stage],
    tasks: list[ParsedTask],
    client: Any,
    *,
    governor: Any,
) -> None:
    repo = push.repo_full_name
    assert repo is not None  # nosec B101 — guaranteed by the caller

    # 1. Read live repo state first so the increment layers on shipped work:
    #    reconcile each existing task's done/open state from GitHub. Best-effort —
    #    a backfill hiccup must not block the (idempotent) layering below.
    await _reconcile_live_state(db, push, client)

    # 2. All writes to this repo are serialized under the per-repo_id lock (T-274)
    #    so a concurrent export/increment cannot race on a blob SHA.
    lock = (
        governor.repo_write_lock(push.repo_id)
        if governor is not None
        else nullcontext()
    )
    async with lock:
        milestone = await client.ensure_milestone(
            repo,
            _milestone_title(increment),
            description=f"Thought2Build increment {increment.sequence}.",
        )
        issue_numbers = await _sync_increment_issues(
            db, client, push, increment, tasks, milestone
        )
        # Same source as _sync_increment_issues' loop (each task's own
        # resolved ``task_ref``): if these two ever disagree, a disambiguated
        # same-titled task looks obsolete and its issue is closed on every push.
        await retire_obsolete_task_issues(
            db,
            client,
            repo,
            push.id,
            {task.task_ref for task in tasks},
        )
        if push.export_mode == "pr_with_tests":
            await _open_increment_pr(
                db, client, push, increment, stages, tasks, issue_numbers
            )

    source_version_id = (
        await db.execute(
            select(StageVersion.id).where(
                StageVersion.stage_id == stages["tasks"].id,
                StageVersion.version == stages["tasks"].current_version,
            )
        )
    ).scalar_one_or_none()
    push.source_stage_version_id = source_version_id
    push.status = "completed"
    increment.status = "pushed"
    increment.updated_at = datetime.now(UTC)
    await db.commit()

    github_audit(
        GITHUB_AUDIT_INCREMENT_PUSHED,
        push_id=str(push.id),
        workspace_id=str(push.workspace_id),
        repo_id=push.repo_id,
        status="pushed",
    )

    # Forward-sync the Projects v2 board + milestones (T-281) so the increment's
    # new issues land on the board. Best-effort: the push has already committed.
    from services.integrations import github_projects

    await github_projects.trigger_board_sync(push.id)


async def _reconcile_live_state(
    db: AsyncSession, push: IntegrationPush, client: Any
) -> None:
    """Reconcile the push's task states from GitHub before layering (T-280).

    Reuses the T-273 backfill so done work stays done. Best-effort: any failure
    is logged and swallowed — the new-issues-only layering does not depend on it.
    """
    try:
        from services.integrations import github_reconcile

        await github_reconcile.backfill_repo({}, str(push.id), db=db, client=client)
    except Exception:  # pragma: no cover - reconcile is best-effort
        logger.warning("increment.push.backfill_failed push_id=%s", str(push.id))


async def _sync_increment_issues(
    db: AsyncSession,
    client: Any,
    push: IntegrationPush,
    increment: Increment,
    tasks: list[ParsedTask],
    milestone: int | None,
) -> dict[str, int]:
    """Create issues for NEW tasks only; update existing ones in place.

    Matching/identity is the **stable** ``compute_task_ref(title)`` (audit #2) —
    not the volatile human ``T-NNN`` — so a task already exported keeps its issue
    across a renumber/reorder and only genuinely new tasks open new ones, filed
    under the increment milestone and tagged with ``increment_id`` so the
    timeline records which increment introduced each task. Pre-existing rows on
    the legacy key are migrated in place first. Each new issue is committed
    immediately so a mid-run failure resumes without recreating it.
    """
    repo = push.repo_full_name
    await migrate_legacy_task_refs(db, push.id, client, repo)
    existing = await _load_push_task_rows(db, push.id)
    issue_numbers: dict[str, int] = {
        ref: row.external_issue_number for ref, row in existing.items()
    }
    for parsed in tasks:
        ref = parsed.task_ref
        row = existing.get(ref)
        if row is not None:
            # Changed task (same stable task_ref) → update its issue in place.
            await client.update_issue(
                repo,
                row.external_issue_number,
                parsed.title,
                parsed.agent_body_md,
                labels=parsed.labels,
            )
        else:
            number = await client.create_issue(
                repo,
                parsed.title,
                parsed.agent_body_md,
                labels=parsed.labels,
                milestone=milestone,
            )
            db.add(
                IntegrationPushTask(
                    push_id=push.id,
                    task_ref=ref,
                    external_issue_number=number,
                    increment_id=increment.id,
                )
            )
            await db.commit()
            issue_numbers[ref] = number
    return issue_numbers


async def _open_increment_pr(
    db: AsyncSession,
    client: Any,
    push: IntegrationPush,
    increment: Increment,
    stages: dict[str, Stage],
    tasks: list[ParsedTask],
    issue_numbers: dict[str, int],
) -> None:
    """Open one PR per increment carrying the increment's red stubs (T-276 reuse).

    Only the increment's *own* new tasks (those tagged with this ``increment_id``)
    go on the branch, so the PR is a clean "here is the new red harness" diff.
    Deterministic per-increment branch + ``find_open_pull_number`` make it
    idempotent: a resumed push reuses the branch and never opens a second PR.
    """
    from services.integrations import pr_export_builder
    from services.pipeline.export_service import parse_harness_files

    repo = push.repo_full_name
    # ``_increment_task_refs`` returns stable compute_task_ref keys (audit #2), so
    # match parsed tasks on the same stable key — never the human ``T-NNN``.
    new_refs = await _increment_task_refs(db, push.id, increment.id)
    new_tasks = [t for t in tasks if t.task_ref in new_refs]
    if not new_tasks:
        return

    branch = f"thought2build/increment-{increment.sequence}"
    base_sha = await client.get_ref(repo, f"heads/{_DEFAULT_BRANCH}")
    await client.create_branch(repo, branch, base_sha)

    harness_files = parse_harness_files(
        stages["harness"].content or "", workspace_id=push.workspace_id
    )
    stacks = pr_export_builder.detect_stacks(
        stages["plan"].content or "", harness_files
    )
    scaffold = pr_export_builder.build_scaffold(
        harness_files=harness_files, tasks=new_tasks, stacks=stacks
    )
    message = f"test: Thought2Build increment {increment.sequence}"
    for path, content in scaffold.items():
        sha = await client.get_file_sha(repo, path, ref=branch)
        await client.upsert_file(repo, path, content, sha, message, branch=branch)

    if await client.find_open_pull_number(repo, head=branch) is None:
        body = pr_export_builder.build_pr_body(
            tasks=new_tasks, issue_numbers=issue_numbers, stacks=stacks
        )
        title = f"Thought2Build increment {increment.sequence}: {increment.title}"
        await client.create_pull_request(
            repo, head=branch, base=_DEFAULT_BRANCH, title=title, body=body
        )
        GITHUB_PR_TOTAL.labels(outcome="opened").inc()
        github_audit(
            GITHUB_AUDIT_PR_OPENED,
            push_id=str(push.id),
            workspace_id=str(push.workspace_id),
            repo_id=push.repo_id,
            status="opened",
        )


def _milestone_title(increment: Increment) -> str:
    return f"Increment {increment.sequence}: {increment.title}"


async def _load_push_task_rows(
    db: AsyncSession, push_id: UUID
) -> dict[str, IntegrationPushTask]:
    rows = (
        await db.execute(
            select(IntegrationPushTask).where(IntegrationPushTask.push_id == push_id)
        )
    ).scalars()
    return {row.task_ref: row for row in rows}


async def _increment_task_refs(
    db: AsyncSession, push_id: UUID, increment_id: UUID
) -> set[str]:
    rows = (
        await db.execute(
            select(IntegrationPushTask.task_ref).where(
                IntegrationPushTask.push_id == push_id,
                IntegrationPushTask.increment_id == increment_id,
            )
        )
    ).scalars()
    return set(rows)


async def _load_push_stages(db: AsyncSession, workspace_id: UUID) -> dict[str, Stage]:
    result = await db.execute(select(Stage).where(Stage.workspace_id == workspace_id))
    stages = {s.type: s for s in result.scalars()}
    for stage_type in _STAGE_ORDER:
        stage = stages.get(stage_type)
        if stage is None or stage.status != "finalised":
            raise IncrementBaselineError(
                f"Stage {stage_type!r} is not finalised — cannot push an increment."
            )
    return stages


async def _load_push_installation(
    db: AsyncSession, installation_id: UUID | None
) -> GitHubInstallation | None:
    if installation_id is None:
        return None
    return (
        await db.execute(
            select(GitHubInstallation).where(GitHubInstallation.id == installation_id)
        )
    ).scalar_one_or_none()


increment_service = IncrementService()
