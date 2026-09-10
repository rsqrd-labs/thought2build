from __future__ import annotations

import json
import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import prompts.demo_day as demo_day_prompts
import prompts.harness as harness_prompts
import prompts.plan as plan_prompts
import prompts.spec as spec_prompts
import prompts.tasks as tasks_prompts
from config import settings
from database import get_shared_redis
from models import Stage, Workspace
from services.llm.cost_ledger import LLMCostContext
from services.observability import PIPELINE_UPSTREAM_SECTION_SKIPPED
from services.pipeline.context_budget import ContextBudgetError
from services.pipeline.markdown_structure import headings, matches_heading
from services.pipeline.problem_compressor import (
    classify_compression_rung,
    get_or_compress,
    problem_budget,
)
from services.pipeline.stage_summary_service import summarize_stage_content

logger = logging.getLogger(__name__)

_STAGE_CACHE_PREFIX = "stage:"
_STAGE_CACHE_TTL = 3600  # 1 hour
_MAX_UPSTREAM_CHARS = 200_000  # T-246: raised from 50K (models accept 200K+ ctx)

_PROMPT_MODULES = {
    "spec": spec_prompts,
    "plan": plan_prompts,
    "harness": harness_prompts,
    "tasks": tasks_prompts,
}

_DEPENDENCIES: dict[str, list[str]] = {
    "spec": [],
    "plan": ["spec"],
    "harness": ["spec", "plan"],
    "tasks": ["spec", "plan", "harness"],
}

# Sections that must be kept verbatim downstream — these are the IDs and
# contracts the downstream stage names by reference.  Order matters: kept
# sections are concatenated in the order listed.
_STAGE_KEEP_SECTIONS: dict[str, list[str]] = {
    "spec": [
        "## In-Scope (MVP)",
        # spec-v6 added ## User Stories alongside ## In-Scope (MVP); both carry
        # FR-ID citations the plan's RTM traces, so both must survive the
        # >200K section-aware injection verbatim rather than being summarised.
        "## User Stories",
        "## Functional Requirements",
        "## Non-Functional Requirements",
        "## Security, Privacy, and Abuse Expectations",
        "## Conceptual Domain Model",
        "## Acceptance Criteria",
    ],
    "plan": [
        "## Requirement Traceability Matrix",
        "## API Design",
        "## Security Architecture",
        "## Data Model and Persistence",
        # Tasks must be able to cite it (tasks.py's load-bearing Plan-refs
        # list, "AI slop" frontend remediation) — a plan big enough to trigger
        # this compression path must not starve the one section that carries
        # the committed color/type/signature tokens.
        "## Frontend Architecture",
    ],
    "harness": [
        "## Requirement-to-Test Matrix",
        "## File Tree",
    ],
}

# Demo Day variant (§9.1). The Demo Day artifacts rename/lean some sections (e.g.
# the plan uses ## Interface Contracts, not ## API Design), so the standard
# keep-list above would silently drop the wrong sections under the section-aware
# injection. Selected by workspace.mode. Keyed by the Demo Day section headings
# (artifact_validator.DEMO_DAY_SECTION_CONTRACTS).
_DEMO_DAY_STAGE_KEEP_SECTIONS: dict[str, list[str]] = {
    "spec": [
        "## Functional Requirements",
        "## Acceptance Criteria",
        "## Demo Day Scope",
        "## Out of Scope",
        "## Security Posture",
    ],
    "plan": [
        "## Requirement Traceability Matrix",
        "## Interface Contracts",
        "## Data Model and Persistence",
        # The harness cannot write correct boundary mocks or fixtures for an
        # integration it never sees, so the REAL/MOCKED stance has to survive
        # compression into the downstream harness/tasks prompts.
        "## External Integrations and Secrets",
        "## Security Architecture",
        # Tasks must be able to cite it ("AI slop" frontend remediation) —
        # same reasoning as the standard keep-list above.
        "## Frontend Architecture",
    ],
    "harness": [
        "## Requirement-to-Test Matrix",
        "## End-to-End Smoke Test",
        "## File Tree",
    ],
}


def _keep_sections(stage_type: str, mode: str) -> list[str]:
    if mode == "demo_day":
        return _DEMO_DAY_STAGE_KEEP_SECTIONS.get(stage_type, [])
    return _STAGE_KEEP_SECTIONS.get(stage_type, [])


def _split_by_h2(content: str) -> list[tuple[str, str]]:
    """Split a markdown document on `## ` h2 headings.  Preserves order.

    Any preamble before the first h2 heading is returned as a leading
    section with an empty heading so its content (which may carry
    requirement IDs or API contracts) is summarized rather than silently
    dropped — silent upstream loss is the exact F-7.1 failure mode.
    """
    sections = [
        (h, start, end) for h, start, end in headings(content) if h.startswith("## ")
    ]
    parts: list[tuple[str, str]] = []
    first = sections[0][1] if sections else len(content)
    if content[:first].strip():
        parts.append(("", content[:first]))
    for index, (heading, _, end) in enumerate(sections):
        stop = sections[index + 1][1] if index + 1 < len(sections) else len(content)
        parts.append((heading, content[end:stop]))
    return parts


def _section_aware_injection(
    stage_type: str, content: str, mode: str = "standard"
) -> str:
    """Keep critical sections verbatim, summarize narrative sections.

    Used only when content exceeds _MAX_UPSTREAM_CHARS even after the 200K
    bump. A critical section that cannot fit aborts prompt construction and
    increments pipeline_upstream_section_skipped_total (the legacy metric
    name). Only narrative may be condensed.
    """
    keep = _keep_sections(stage_type, mode)
    sections = _split_by_h2(content)
    preserved: list[str] = []
    summarized: list[str] = []
    budget_remaining = _MAX_UPSTREAM_CHARS
    for heading, body in sections:
        full = f"{heading}\n{body}"
        critical = any(matches_heading(heading, item) for item in keep)
        if critical and len(full) <= budget_remaining:
            preserved.append(full)
            budget_remaining -= len(full) + 2
        elif critical:
            # Fail before the provider call instead of losing a requirement.
            PIPELINE_UPSTREAM_SECTION_SKIPPED.labels(
                stage=stage_type, section=heading
            ).inc()
            raise ContextBudgetError(
                f"Cannot preserve {stage_type} {heading} within the context budget. "
                "Split the upstream artifact before continuing."
            )
        else:
            summarized.append(full)
    summary = summarize_stage_content(stage_type, "\n".join(summarized)).content
    # Only narrative may be condensed. Never slice a preserved contract.
    return "\n\n".join(preserved + [summary[: max(0, budget_remaining)]])


async def build_prompt(
    stage_type: str,
    workspace: Workspace,
    db: AsyncSession,
    redis_client: Redis | None = None,
    *,
    provider: str | None = None,
    model: str | None = None,
    research_context: str = "",
) -> tuple[str, str, str]:
    """Assemble ``(system_prompt, user_prompt, compression_rung)`` for a stage.

    ``compression_rung`` reports how this stage's problem statement was condensed
    ("0" none / "1" lossless / "2" abstractive / "3" clamp). It is the signal the
    caller uses to surface the Phase-D advisory notice; it is "0" whenever the
    compression flag is off or the input was under budget, so the common case is
    unchanged.
    """
    module = _PROMPT_MODULES[stage_type]
    dep_keys = _DEPENDENCIES[stage_type]
    if provider is None or model is None:
        from services.llm.routing import resolve_platform_route  # noqa: PLC0415

        route = resolve_platform_route(
            operation=f"{stage_type}.generate",
            requested_tier="mid",
            fallback_tier=None,
            latency_class="interactive",
        )
        provider, model = route.provider, route.model
    # Demo Day mode selects a parallel set of prompts, section contracts, and
    # keep-lists; any other value takes the unchanged standard path (the §4
    # byte-identical regression pin). getattr keeps callers that pass a lightweight
    # workspace stub (tests) working.
    mode = getattr(workspace, "mode", "standard") or "standard"

    deps: dict[str, str] = {"problem_statement": workspace.problem_statement}

    # Issue #12 (Phase 3): optional Brave web-research grounding. The block is
    # assembled+sanitised upstream (research_service) and threaded through deps
    # for the module to position. Empty string (the fail-open default and the
    # value for stages/workspaces without research) yields a byte-identical
    # prompt — the regression pin. build_prompt stays a pure assembler; the only
    # new I/O (the Brave fetch) lives in the generate() preflight.
    deps["research_context"] = research_context

    # Phase 14: thread persisted Spec Clarification Q&A into the spec
    # prompt so regenerates honour the user's earlier answers without a
    # second round of questioning. JSON-encoded to preserve the existing
    # dict[str, str] dependency contract; spec.build_user_prompt decodes.
    if stage_type == "spec" and getattr(workspace, "clarification_qa", None):
        deps["clarification_qa"] = json.dumps(
            workspace.clarification_qa,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )

    if dep_keys:
        redis = redis_client or get_shared_redis()  # H-1 — T-177
        for dep_type in dep_keys:
            # The worker loads Workspace.stages in the same authoritative DB
            # session immediately before prompt assembly. Prefer that snapshot:
            # it removes Redis from the paid post-charge preflight and guarantees
            # PLAN never observes a stale finalised SPEC after cache invalidation
            # failed. Lightweight test/caller stubs without ``stages`` retain the
            # versioned, fail-open fallback below.
            dep_stage = next(
                (
                    item
                    for item in (getattr(workspace, "stages", None) or ())
                    if getattr(item, "type", None) == dep_type
                ),
                None,
            )
            content = (
                (getattr(dep_stage, "content", None) or "")
                if dep_stage is not None
                else await _fetch_stage_content(dep_type, workspace.id, db, redis)
            )
            if len(content) > _MAX_UPSTREAM_CHARS:
                logger.warning(
                    "upstream_content_section_aware_injection",
                    extra={"stage": dep_type, "original_len": len(content)},
                )
                content = _section_aware_injection(dep_type, content, mode)
            deps[dep_type] = content

    # Problem-statement compression (compression plan Phase B/D), applied lazily
    # here — beside _section_aware_injection, the existing upstream reducer — so
    # the cached compressed value is computed once and reused across stages and
    # surfaces. Gated by `problem_statement_compression` (enabled by default in
    # Phase D); when off this is a no-op and the prompt is byte-identical (the
    # Rung-0 regression pin). Under-budget input is also a byte-identical no-op
    # even when the flag is on, so the common case never changes. The statement is
    # in `deps` for *every* stage, so every stage's prompt is bounded by C_MAX, not
    # just spec.
    compression_rung = "0"
    if settings.problem_statement_compression:
        redis = redis_client or get_shared_redis()
        budget = problem_budget(
            provider,
            model,
            research_context=research_context,
            clarification_qa=deps.get("clarification_qa", ""),
            stage_type=stage_type,
        )
        compressed = await get_or_compress(
            workspace.problem_statement,
            budget,
            redis,
            provider,
            model,
            cost_context=LLMCostContext(
                workspace_id=workspace.id,
                product_surface="problem_compression",
            ),
        )
        from services.pipeline.problem_compressor import (
            _is_normative_block,
            _rung1_cleanup,
            _split_blocks,
        )

        cleaned = _rung1_cleanup(workspace.problem_statement)
        normalized = " ".join(compressed.split())
        if any(
            " ".join(block.split()) not in normalized
            for block in _split_blocks(cleaned)
            if _is_normative_block(block)
        ):
            raise ContextBudgetError(
                "The problem statement cannot fit without losing requirements. "
                "Split the scope before continuing."
            )
        deps["problem_statement"] = compressed
        # Phase D: classify how the statement was condensed so the caller can
        # surface a non-blocking advisory notice for the lossy rungs (2/3). Pure
        # and cache-hit-safe — recomputed from the returned text against the exact
        # budget used, never a second compression. budget can differ per stage_type
        # (problem_budget), so the rung is genuinely per-stage.
        compression_rung = classify_compression_rung(
            workspace.problem_statement,
            compressed,
            budget,
            provider,
            model,
        )

    if mode == "demo_day":
        time_budget_minutes = getattr(workspace, "time_budget_minutes", None)
        restricted_environment = bool(
            getattr(workspace, "restricted_environment", False)
        )
        system_prompt = await demo_day_prompts.get_system_prompt(
            stage_type, time_budget_minutes, restricted_environment
        )
        user_prompt = demo_day_prompts.build_user_prompt(
            stage_type, deps, time_budget_minutes, restricted_environment
        )
    else:
        system_prompt = await module.get_system_prompt()
        user_prompt = module.build_user_prompt(deps)
    return system_prompt, user_prompt, compression_rung


async def _fetch_stage_content(
    stage_type: str,
    workspace_id,
    db: AsyncSession,
    redis: Redis,
) -> str:
    result = await db.execute(
        select(Stage).where(
            Stage.workspace_id == workspace_id,
            Stage.type == stage_type,
        )
    )
    stage = result.scalar_one_or_none()
    content = (stage.content or "") if stage else ""
    # The version is part of the key, so a failed invalidation can never make a
    # later generation consume an older finalised dependency. The DB read above
    # is the authority; Redis is only a best-effort mirror for legacy callers.
    version = int(getattr(stage, "current_version", 0) or 0)
    cache_key = f"{_STAGE_CACHE_PREFIX}{workspace_id}:{stage_type}:v{version}"
    try:
        cached = await redis.get(cache_key)
        if cached is not None:
            return cached.decode("utf-8") if isinstance(cached, bytes) else str(cached)
        await redis.set(cache_key, content, ex=_STAGE_CACHE_TTL)
    except (RedisError, UnicodeError):
        logger.warning(
            "stage_dependency_cache_unavailable",
            extra={"workspace_id": str(workspace_id), "stage": stage_type},
        )
    return content
