from __future__ import annotations

import asyncio
import hmac
import logging
import re
import time
from collections.abc import Callable
from typing import Any

import sentry_sdk
import structlog
from fastapi import FastAPI, Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from prometheus_client.core import GaugeMetricFamily
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.responses import Response as StarletteResponse

from config import settings

logger = structlog.get_logger(__name__)


def get_structured_logger(name: str) -> Any:
    return structlog.get_logger(name)


REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status_code"],
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
)
LLM_REQUEST_COUNT = Counter(
    "llm_request_total",
    "Total instrumented LLM requests",
    ["provider", "model_tier", "operation", "stage_type", "cache_hit"],
)
LLM_ESTIMATED_COST_USD = Counter(
    "llm_estimated_cost_usd_total",
    "Estimated LLM API cost in USD",
    ["provider", "model_tier", "operation", "stage_type"],
)
LLM_INPUT_TOKENS = Counter(
    "llm_input_tokens_total",
    "LLM input tokens",
    ["provider", "model_tier", "operation", "stage_type", "method"],
)
LLM_OUTPUT_TOKENS = Counter(
    "llm_output_tokens_total",
    "LLM output tokens",
    ["provider", "model_tier", "operation", "stage_type", "method"],
)
LLM_CACHED_INPUT_TOKENS = Counter(
    "llm_cached_input_tokens_total",
    "LLM cached input tokens (prompt-cache reads)",
    ["provider", "model_tier", "operation", "stage_type"],
)
LLM_CACHE_WRITE_INPUT_TOKENS = Counter(
    "llm_cache_write_input_tokens_total",
    "LLM prompt-cache write (creation) tokens — premium-priced for Anthropic",
    ["provider", "model_tier", "operation", "stage_type"],
)
LLM_LATENCY_SECONDS = Histogram(
    "llm_latency_seconds",
    "LLM request latency in seconds",
    ["provider", "model_tier", "operation", "stage_type"],
)
LLM_FIRST_EVENT_LATENCY_SECONDS = Histogram(
    "llm_first_event_latency_seconds",
    "Time from request start to the first provider stream event",
    ["provider", "model_tier", "operation", "stage_type"],
)
LLM_PROVIDER_PROMPT_CACHE_REQUESTS = Counter(
    "llm_provider_prompt_cache_requests_total",
    "Instrumented calls by provider prompt-cache outcome",
    ["provider", "model_tier", "operation", "stage_type", "outcome"],
)
LLM_CROSS_PROVIDER_FALLBACK_COUNT = Counter(
    "llm_cross_provider_fallback_total",
    "LLM requests that used an explicit cross-provider fallback route",
    ["provider", "model_tier", "operation", "stage_type"],
)
LLM_PROVIDER_ERROR_COUNT = Counter(
    "llm_provider_errors_total",
    "LLM provider call failures",
    ["provider", "error_type"],
)
LLM_PROVIDER_CONFIGURED = Gauge(
    "llm_provider_configured",
    "Whether a provider API key is configured",
    ["provider"],
)
LLM_PROVIDER_HEALTH = Gauge(
    "llm_provider_health",
    "Provider health state: 0 not configured, 1 unhealthy, 2 degraded, 3 healthy",
    ["provider"],
)

# T-194: SSE, PDF, and eval instrumentation.
# -----------------------------------------
# SSE streaming failure counter — incremented when a stage-generation SSE
# stream terminates with an error before the client receives a completion
# event. This makes streaming failure rate visible in dashboards.
SSE_STREAM_FAILURES = Counter(
    "thought2build_sse_stream_failures_total",
    "SSE stage-generation streams that terminated on error",
    ["stage_type"],
)

PIPELINE_INCOMPLETE_OUTPUTS = Counter(
    "thought2build_pipeline_incomplete_outputs_total",
    "Stage generations blocked because the provider output was incomplete",
    ["stage_type", "provider", "reason"],
)
PIPELINE_COMPLETION_REPAIRS = Counter(
    "thought2build_pipeline_completion_repairs_total",
    # outcome: attempted / succeeded / failed for a funded repair, plus
    # skipped_at_ceiling (Phase 4, issue #28) when a chunk limit-stop repair is
    # skipped because the budget is already maxed and the retry is provably doomed.
    "Platform-funded repair attempts for incomplete stage generation chunks",
    ["stage_type", "provider", "outcome"],
)
PIPELINE_PROVIDER_LIMIT_STOPS = Counter(
    "thought2build_pipeline_provider_limit_stops_total",
    "Provider generations that stopped because max output tokens were reached",
    ["stage_type", "provider", "model", "operation"],
)
PIPELINE_SECTION_DEDUP = Counter(
    "thought2build_pipeline_section_dedup_total",
    # Duplicate contract-section bodies deterministically removed from an
    # assembled artifact before any gate (prompt-quality audit H1 backstop):
    # parallel chunks have no cross-visibility, so a chunk-scope regression can
    # emit the same mandatory H2 section twice with conflicting bodies — the
    # substring section gate passes both silently. A non-zero rate means the
    # disjoint chunk scopes are being violated and should be re-audited.
    "Duplicate contract sections removed by the assembly-time dedup guard",
    ["stage_type", "provider"],
)

PIPELINE_HARNESS_FILE_DEDUP = Counter(
    "thought2build_pipeline_harness_file_dedup_total",
    # Duplicate `### File:` blocks deterministically removed from a harness
    # artifact before persistence — a self-heal for the cheap-tier model (or a
    # chunk merge) emitting the whole Files section more than once. A non-zero
    # rate is a core-generation health signal, not a user-facing failure.
    "Duplicate harness File blocks removed by the deterministic dedup self-heal",
    ["provider"],
)
PIPELINE_INTERRUPTED_STREAMS = Counter(
    "thought2build_pipeline_interrupted_streams_total",
    "Stage generation streams interrupted before a usable completed artifact existed",
    ["stage_type"],
)
# Trailing user-requested gap-patch file blocks dropped before merge because
# their code fence was unbalanced (truncated mid-file). Additive merge makes the
# drop always safe; a non-zero rate flags a too-small patch budget.
HARNESS_PATCH_BLOCK_REJECTED = Counter(
    "thought2build_harness_patch_block_rejected_total",
    "Incomplete trailing harness file blocks dropped before merge",
    ["source"],
)
# TASKS refs whose file exists in the harness but whose individual tests could
# not be parsed (UNVERIFIED_COVERAGE). A rising trend marks a parser blind spot
# (an unsupported language / fence style) to fix — the point is that these stay
# measurable instead of being silently swallowed or shown as scary false gaps.
UNVERIFIED_COVERAGE_FINDINGS = Counter(
    "thought2build_unverified_coverage_findings_total",
    "TASKS refs downgraded to UNVERIFIED_COVERAGE (file present, tests unparsed)",
)
# Paid harness gap-patches that merged NOTHING new (the model re-emitted only
# existing files, or its single block was truncated and rejected). These roll
# back with no charge and surface a "no new coverage" signal instead of
# committing a byte-identical version; a rising rate flags a weak patch prompt.
HARNESS_PATCH_NOOP = Counter(
    "thought2build_harness_patch_noop_total",
    "Paid harness gap-patches that produced no new files (rolled back, no charge)",
)

# Stream-watchdog kills: kind is "idle" (token gap exceeded the idle timeout —
# a stalled provider stream) or "hard_cap" (the absolute per-stream bound hit —
# a runaway generation). Alert on idle-rate: it is the provider-health signal.
PIPELINE_STREAM_WATCHDOG_TIMEOUTS = Counter(
    "thought2build_pipeline_stream_watchdog_timeouts_total",
    "Stage generation streams killed by the idle/hard-cap stream watchdog",
    ["stage_type", "provider", "kind"],
)

# Runtime tier fallbacks: the primary (strong-tier) generation failed with a
# timeout or provider error and the stage was retried once on the fallback
# tier. A rising rate means the frontier route is degraded.
PIPELINE_GENERATION_FALLBACKS = Counter(
    "thought2build_pipeline_generation_fallbacks_total",
    "Stage generations retried on the fallback model tier after a primary failure",
    ["stage_type", "provider", "outcome"],
)

# Provider rate-limit (429/overload) in-place retries during stage generation
# (scalability audit F2). Distinct from PIPELINE_GENERATION_FALLBACKS: a 429 is
# retried on the SAME tier (honoring Retry-After / backoff), never escalated, so
# it never amplifies load against an already-throttled org. outcome is "retried"
# (a backoff retry was issued) or "exhausted" (bounded retries used up; the
# failure surfaces). A rising rate is the binding-constraint signal: the shared
# provider key is hitting its account rate ceiling.
PIPELINE_PROVIDER_RATE_LIMIT_RETRIES = Counter(
    "thought2build_pipeline_provider_rate_limit_retries_total",
    "Stage generation stream retries triggered by a provider 429/overload, "
    "retried in place on the same tier (no tier escalation), by outcome.",
    ["stage_type", "provider", "outcome"],
)

PIPELINE_GENERATION_DURATION = Histogram(
    "thought2build_pipeline_generation_duration_seconds",
    "Wall-clock duration of a full stage artifact generation (all chunks)",
    ["stage_type", "provider"],
    buckets=(15, 30, 60, 120, 180, 300, 450, 600, 900, float("inf")),
)
PIPELINE_STAGE_END_TO_END_DURATION = Histogram(
    "thought2build_pipeline_stage_end_to_end_duration_seconds",
    "End-to-end wall-clock duration of a stage generation pipeline",
    ["stage_type", "provider", "outcome"],
    buckets=(15, 30, 60, 120, 180, 300, 450, 600, 900, float("inf")),
)

PIPELINE_TECH_SAFETY_FAILURES = Counter(
    "thought2build_pipeline_technology_safety_failures_total",
    "Stage artifacts blocked by the deterministic technology safety gate",
    ["stage_type", "code", "severity"],
)

PIPELINE_TECH_SAFETY_REPAIRS = Counter(
    "thought2build_pipeline_technology_safety_repairs_total",
    "Platform-funded repair attempts for unsafe generated technology choices",
    ["stage_type", "provider", "outcome"],
)

PIPELINE_TECH_SAFETY_LOOKUP_FAILURES = Counter(
    "thought2build_pipeline_technology_safety_lookup_failures_total",
    "External technology safety lookup failures",
    ["source", "reason"],
)

PIPELINE_TECH_SAFETY_FINALISE_BLOCKS = Counter(
    "thought2build_pipeline_technology_safety_finalise_blocks_total",
    "Finalise attempts blocked by deterministic technology safety validation",
    ["stage_type", "code"],
)

# Brave LLM Context API research enrichment (issue #12). The integration is a
# purely additive, fail-open grounding layer: every failure path returns an
# empty research block and generation proceeds unchanged. These metrics make the
# fail-open behaviour observable without ever recording the API key or raw
# third-party snippets.
#
# Outcome ownership (avoids double-counting across phases):
#   - the Brave HTTP client (Phase 1) emits the low-level call outcomes
#     hit|empty|timeout|error|rate_limited and the latency histogram;
#   - the research service (Phase 2) emits the cache-level outcomes
#     (disabled|cache hit/miss|quota|insufficient_credits) and the injected
#     context-size histogram.
BRAVE_REQUESTS_TOTAL = Counter(
    "thought2build_brave_requests_total",
    "Brave LLM Context research fetch outcomes",
    # hit|empty|timeout|error|rate_limited (HTTP client) +
    # disabled|quota|insufficient_credits (research service, Phase 2)
    ["outcome"],
)

BRAVE_REQUEST_LATENCY = Histogram(
    "thought2build_brave_request_latency_seconds",
    "Wall-clock latency of a Brave LLM Context API fetch (incl. one retry)",
    buckets=(0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, float("inf")),
)

BRAVE_CACHE_TOTAL = Counter(
    "thought2build_brave_cache_total",
    "Brave research Redis cache lookups (emitted by the research service)",
    ["result"],  # hit|miss
)

BRAVE_CONTEXT_CHARS = Histogram(
    "thought2build_brave_context_chars",
    "Size in characters of the research context block injected into a prompt",
    buckets=(0, 500, 1000, 2000, 4000, 6000, 8000, 12000, float("inf")),
)

# ---------------------------------------------------------------------------
# Problem-statement compression — Phase A instrumentation
# (docs/PROBLEM_STATEMENT_COMPRESSION_PLAN.md §8).
# ---------------------------------------------------------------------------
#
# Two distributions, captured at generation time, that tell us *whether and how
# often* compression would ever fire once the input cap is raised. A histogram
# (not a gauge) is the right instrument: we need the live distribution across
# generations, not the last value. ``estimate_tokens`` (utf-8-byte basis) is
# reused for both — no new tokenizer. Buckets bracket the future compression
# THRESHOLD (~8K tokens) and the raised 50K-char storage cap (~12.5K tokens).
#
# Labels are bounded enums (provider / stage_type) normalised through the helpers
# below so a stray value can never explode Prometheus cardinality.
PROBLEM_STATEMENT_TOKENS = Histogram(
    "thought2build_problem_statement_tokens",
    "Estimated token size of a workspace problem statement entering generation "
    "(emitted once per spec generation, including generation-cache hits).",
    labelnames=["provider"],
    buckets=(250, 500, 1000, 2000, 4000, 8000, 12000, 16000, 24000, float("inf")),
)

ASSEMBLED_PROMPT_TOKENS = Histogram(
    "thought2build_assembled_prompt_tokens",
    "Estimated token size of the fully assembled stage prompt (system + user, "
    "incl. problem statement, research, and upstream artifacts) actually sent to "
    "the model. Emitted once per cache-miss generation.",
    labelnames=["provider", "stage_type"],
    buckets=(
        1000,
        2000,
        4000,
        8000,
        16000,
        32000,
        64000,
        128000,
        float("inf"),
    ),
)

_PROVIDER_LABELS = frozenset({"anthropic", "openai", "google", "openrouter"})
_STAGE_TYPE_LABELS = frozenset({"spec", "plan", "harness", "tasks"})


def _provider_label(provider: str) -> str:
    value = str(provider or "unknown")
    return value if value in _PROVIDER_LABELS else "unknown"


def _stage_type_label(stage_type: str) -> str:
    value = str(stage_type or "unknown")
    return value if value in _STAGE_TYPE_LABELS else "unknown"


def record_problem_statement_tokens(provider: str, tokens: int | None) -> None:
    """Observe the token size of a problem statement entering generation.

    No-op for ``None``/negative (an unestimable input is not a data point). The
    common under-cap input lands in the low buckets; the new large pastes are
    what light up the high ones — that ratio is the Phase-A exit signal.
    """
    if tokens is not None and tokens >= 0:
        PROBLEM_STATEMENT_TOKENS.labels(provider=_provider_label(provider)).observe(
            tokens
        )


def record_assembled_prompt_tokens(
    provider: str, stage_type: str, tokens: int | None
) -> None:
    """Observe the token size of a fully assembled stage prompt sent to a model.

    No-op for ``None``/negative. This is the *whole* prompt (system + user), so
    it is the figure the window-fit reliability ceiling (§5) is computed against.
    """
    if tokens is not None and tokens >= 0:
        ASSEMBLED_PROMPT_TOKENS.labels(
            provider=_provider_label(provider),
            stage_type=_stage_type_label(stage_type),
        ).observe(tokens)


# Problem-statement compression ladder telemetry (compression plan Phase B).
# A counter labelled by the rung that actually fired makes the rollout legible:
# how often the lossless Rung 1 alone returns under budget vs. how often the
# deterministic Rung 3 floor is reached vs. how often the outer fail-open
# bounded-truncate backstop catches a compressor error. Rung 0 (the common
# under-budget no-op) is deliberately *not* counted — it returns before the
# ladder engages, so counting it would swamp the signal with the common case.
# `rung` is a bounded enum normalised through the helper below.
PROBLEM_COMPRESSION_RUNGS = Counter(
    "thought2build_problem_compression_rung_total",
    "Count of problem-statement compressions by the ladder rung that produced "
    "the result (1=lossless structural cleanup, 2=abstractive map-reduce, "
    "3=deterministic clamp, error=fail-open bounded truncate). Rung 0 no-ops "
    "are not counted.",
    labelnames=["rung"],
)

_COMPRESSION_RUNG_LABELS = frozenset({"1", "2", "3", "error"})


def record_problem_compression(rung: str) -> None:
    """Observe which compression rung produced a result.

    No-op for the Rung-0 fast path (the caller never calls this for it). An
    unexpected label collapses to ``"error"`` so Prometheus cardinality is fixed.
    """
    label = rung if rung in _COMPRESSION_RUNG_LABELS else "error"
    PROBLEM_COMPRESSION_RUNGS.labels(rung=label).inc()


# PDF export duration histogram — WeasyPrint is CPU-bound and blocks the
# thread-pool executor thread for 0.5–3 s per render. Observing duration
# makes event-loop-blocking outliers (C-4) visible.
PDF_EXPORT_DURATION = Histogram(
    "thought2build_pdf_export_duration_seconds",
    "Wall-clock duration of PDF export render calls",
    buckets=(0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0),
)

# Eval polling failure counter — incremented when the eval poller gives up
# after max retries. Without this counter, silent eval drops are invisible.
EVAL_POLL_FAILURES = Counter(
    "thought2build_eval_poll_failures_total",
    "Eval polling attempts that exhausted max retries and silently dropped",
    ["stage_type"],
)

# CSRF replay rejection counter — incremented when verify_csrf_token() detects
# a nonce that has already been claimed in Redis (i.e., the token was replayed).
# Distinguishes active replay attacks from token generation bugs.  HF-6 — T-203.
CSRF_REPLAY_REJECTIONS = Counter(
    "thought2build_csrf_replay_rejections_total",
    "CSRF tokens rejected because the nonce was already consumed in Redis",
)

# Billing counters (Phase 18 → Phase 22 — T-236, provider-labelled in T-304)
# -------------------------------------------------------------------------
# These counters power the Grafana alert rules documented in RUNBOOK §9. Phase 22
# (T-304) widened the money-path counters with a ``provider`` label; the live
# Lemon Squeezy path is the only runtime emitter since the Stripe decommission
# (T-308), though the ``provider`` label and the retained ``provider='stripe'``
# audit rows mean aggregating queries/alerts still sum across providers. Labelled
# counters REQUIRE ``.labels(...).inc()`` at every call site. ``credits_expired``
# stays label-less — lazy expiry is provider-agnostic.
BILLING_CHECKOUT_CREATED = Counter(
    "thought2build_billing_checkout_created_total",
    "Hosted checkouts created via POST /billing/checkout",
    ["provider"],
)
BILLING_CHECKOUT_COMPLETED = Counter(
    "thought2build_billing_checkout_completed_total",
    "Paid-order webhook events received and turned into a credit grant",
    ["provider"],
)
BILLING_CREDITS_GRANTED = Counter(
    "thought2build_billing_credits_granted_total",
    "Total credits granted to users via a verified purchase",
    ["provider"],
)
BILLING_PURCHASE_REVENUE_CENTS = Counter(
    "thought2build_billing_purchase_revenue_cents_total",
    "Gross paid-item revenue (cents) recognised when a verified order grant lands, "
    "anchored to the checkout-attempt snapshot price (Phase 22 — T-299).",
    ["provider"],
)
BILLING_CHECKOUT_API_ERROR = Counter(
    "thought2build_billing_checkout_api_error_total",
    "POST /billing/checkout failures, by error_type: 'provider_error' (the provider "
    "checkout API failed) or 'orphaned_commit' (the post-provider commit failed so "
    "the URL was never exposed — reconciled later). Phase 22 — T-304.",
    ["provider", "error_type"],
)
BILLING_CHECKOUT_EXPIRED = Counter(
    "thought2build_billing_checkout_expired_total",
    "Local checkout attempts expired past their TTL by reconcile lane 3 "
    "(Phase 22 — T-301/T-304).",
    ["provider"],
)
BILLING_UNRECOVERABLE_CHECKOUT = Counter(
    "thought2build_billing_unrecoverable_checkout_total",
    "Paid order webhooks the automatic pipeline could not turn into a grant "
    "(the unprovable-paid-checkout path → admin correction T-302). Phase 22 — T-304.",
    ["provider"],
)
BILLING_CREDITS_EXPIRED = Counter(
    "thought2build_billing_credits_expired_total",
    "Credits swept by lazy expiry in _expire_user_packs() (provider-agnostic)",
)
BILLING_CREDITS_CONSUMED = Counter(
    "thought2build_billing_credits_consumed_total",
    "Credits drained by FIFO pack drain in _drain_packs()",
)
BILLING_CREDIT_DEBT_RECOVERED = Counter(
    "thought2build_billing_credit_debt_recovered_total",
    "Credits from a new grant applied to repay pending billing_credit_debts "
    "(debt-first recovery) before any usable surplus is added (Phase 22 — T-294).",
    ["provider"],
)
BILLING_CREDITS_REVOKED = Counter(
    "thought2build_billing_credits_revoked_total",
    "Credits revoked by a refund / fraud / dispute reversal (Phase 22 — T-300). The "
    "provider-neutral successor to the retired pack_disputed_total — a dispute folds "
    "in as reason='disputed'.",
    ["provider", "reason"],
)
BILLING_CREDIT_DEBT_CREATED = Counter(
    "thought2build_billing_credit_debt_created_total",
    "Credits a reversal could not immediately recover (the user had already spent "
    "them) and which became recoverable billing_credit_debts (Phase 22 — T-300).",
    ["provider", "reason"],
)
BILLING_ADMIN_CORRECTION = Counter(
    "thought2build_billing_admin_correction_total",
    "Evidence-backed admin credit corrections applied for an order the automatic "
    "webhook pipeline could not settle (Phase 22 — T-302). Incremented only on a "
    "real grant — the idempotent duplicate no-op does not count.",
    ["provider"],
)
BILLING_RECONCILE_MISMATCH = Counter(
    "thought2build_billing_reconcile_mismatch_total",
    "Reversals the reconcile backstop applied because the webhook path missed them "
    "(a local pack out of sync with the provider's order). Phase 22 — T-301/T-304.",
    ["provider"],
)
BILLING_WEBHOOK_RECEIVED = Counter(
    "thought2build_billing_webhook_received_total",
    "All webhook events received (before idempotency check)",
    ["provider", "event_type"],
)
BILLING_WEBHOOK_DUPLICATE = Counter(
    "thought2build_billing_webhook_duplicate_total",
    "Webhook events rejected as duplicates by the idempotency guard",
    ["provider"],
)
BILLING_WEBHOOK_ERROR = Counter(
    "thought2build_billing_webhook_error_total",
    "Webhook events that failed during processing",
    ["provider", "error_type"],
)
BILLING_WEBHOOK_PENDING_AGE_SECONDS = Gauge(
    "thought2build_billing_webhook_pending_age_seconds",
    "Age (seconds) of the oldest not-yet-processed billing_webhook_events row, "
    "refreshed by the 60s recovery sweep. The lost-webhook alert (>300s) and the "
    "trigger to scale out a dedicated billing worker (Phase 22 — T-304).",
)
BILLING_CHECKOUT_RATE_LIMITED = Counter(
    "thought2build_billing_checkout_rate_limited_total",
    "POST /billing/checkout requests rejected by the 5/hour rate limit",
)

# GitHub App installation-token resolutions (Phase 21 — T-267). The cache keeps
# minting off the hot path (GitHub rate-limits token minting), so the ratio of
# source="mint" to source="cache" is the cache-hit signal.
GITHUB_TOKEN_MINT_TOTAL = Counter(
    "thought2build_github_token_mint_total",
    "GitHub installation token resolutions, by source: 'mint' = a new token "
    "minted from GitHub, 'cache' = served from the Redis token cache.",
    labelnames=["source"],
)

# Durable worker job metrics (Phase 21 — T-269). Retries and dead-letters are the
# reliability signal for the GitHub job queue; queue depth is the backpressure
# signal operators alert on.
GITHUB_JOB_RETRIES_TOTAL = Counter(
    "thought2build_github_job_retries_total",
    "GitHub worker job attempts that failed transiently and were retried "
    "(exponential backoff + jitter), labelled by job name.",
    labelnames=["job"],
)
GITHUB_JOB_DEADLETTERED_TOTAL = Counter(
    "thought2build_github_job_deadlettered_total",
    "GitHub worker jobs that exhausted max_tries and were moved to the "
    "dead-letter record for manual replay, labelled by job name.",
    labelnames=["job"],
)

# Billing worker job metrics (Phase 22 — T-293). The same retry/dead-letter
# reliability signal as the GitHub queue, but for the separate billing job lane
# (its own ``billing:deadletter`` Redis list) so a credit grant is never starved
# or confused with a GitHub export. Surfaced/alerted on in T-304.
BILLING_JOB_RETRIES_TOTAL = Counter(
    "thought2build_billing_job_retries_total",
    "Billing worker job attempts that failed transiently and were retried "
    "(exponential backoff + jitter), labelled by job name.",
    labelnames=["job"],
)
BILLING_JOB_DEADLETTERED_TOTAL = Counter(
    "thought2build_billing_job_deadlettered_total",
    "Billing worker jobs that exhausted max_tries and were moved to the "
    "'billing:deadletter' record for manual replay, labelled by job name.",
    labelnames=["job"],
)
GITHUB_QUEUE_DEPTH = Gauge(
    "thought2build_github_queue_depth",
    "Approximate number of GitHub worker jobs currently queued/in-flight.",
)

# Per-queue worker backpressure (F5 — scalability audit). After the live-queue
# split (latency-sensitive `fast` vs bulk), depth + oldest-job age are sampled
# per queue by a lightweight per-worker cron so an export storm filling the bulk
# queue is visibly distinct from the billing/PR `fast` queue — and a `fast`
# queue with no consumer surfaces as a climbing oldest-age alert. Sampled from a
# cron (not on_job_start): a stalled queue starts no jobs, so an on-job-start
# gauge would read stale exactly when the alert must fire.
WORKER_QUEUE_DEPTH = Gauge(
    "thought2build_worker_queue_depth",
    "Approximate number of worker jobs queued/in-flight, labelled by arq queue.",
    labelnames=["queue"],
)
WORKER_QUEUE_OLDEST_AGE_SECONDS = Gauge(
    "thought2build_worker_queue_oldest_age_seconds",
    "Age of the oldest queued job (seconds since enqueue), labelled by arq "
    "queue. 0 when the queue is empty. A sustained climb on the `fast` queue "
    "means its dedicated worker is down/starved (paid grants not draining).",
    labelnames=["queue"],
)


def record_worker_queue_stats(
    queue: str, depth: int, oldest_age_seconds: float
) -> None:
    """Publish a queue's sampled depth + oldest-job age (F5).

    Called from the per-worker sampler cron with the stats for the queue that
    worker consumes. ``GITHUB_QUEUE_DEPTH`` is kept in sync for the bulk queue so
    existing dashboards/alerts that key on it keep working through the split.
    """
    WORKER_QUEUE_DEPTH.labels(queue=queue).set(max(0, depth))
    WORKER_QUEUE_OLDEST_AGE_SECONDS.labels(queue=queue).set(
        max(0.0, oldest_age_seconds)
    )


BACKGROUND_TASKS = Gauge(
    "thought2build_background_tasks",
    "Live detached background tasks held in a strong-ref registry, labelled by "
    "registry (pipeline / eval / critic / verifier). A runaway count signals a "
    "fan-out leaking past the F1 admission cap (audit §6/§8).",
    labelnames=["registry"],
)


def set_background_task_count(registry: str, count: int) -> None:
    """Publish a background-task registry's live size (F6)."""
    BACKGROUND_TASKS.labels(registry=registry).set(max(0, count))


# ---------------------------------------------------------------------------
# Data retention & purging (issue #43; docs/RETENTION_IMPLEMENTATION_PLAN.md).
# ---------------------------------------------------------------------------
#
# Phase 0 — the size baseline every later "size stabilized" claim is judged
# against (plan §6). Sampled hourly by the worker's ``sample_table_stats`` cron
# over a FIXED table allowlist, so the ``table`` label is bounded and can never
# explode Prometheus cardinality. Postgres-only (the sampler no-ops elsewhere).
DB_TABLE_BYTES = Gauge(
    "thought2build_db_table_bytes",
    "Total on-disk size (pg_total_relation_size, incl. indexes + TOAST) of a "
    "retention-tracked table, sampled hourly. The plateau-not-shrink success "
    "metric: DELETE frees no files, so a flat slope at steady state is the win.",
    labelnames=["table"],
)
DB_TABLE_LIVE_TUPLES = Gauge(
    "thought2build_db_table_live_tuples",
    "Estimated live row count (pg_stat_user_tables.n_live_tup) of a "
    "retention-tracked table, sampled hourly.",
    labelnames=["table"],
)


def record_table_stats(
    table: str, size_bytes: int | None, live_tuples: int | None
) -> None:
    """Publish one retention-tracked table's sampled size + live-tuple count.

    No-op for ``None`` (a stat the sampler could not read is not a data point),
    so a partial sample never publishes a misleading zero. Clamped ``>= 0``.
    """
    if size_bytes is not None:
        DB_TABLE_BYTES.labels(table=table).set(max(0, size_bytes))
    if live_tuples is not None:
        DB_TABLE_LIVE_TUPLES.labels(table=table).set(max(0, live_tuples))


# Phase 4 — retention job telemetry (plan §7). ``candidates`` is set every run
# (including dry-run and flag-off counting) so the backlog alert can watch it
# rise while a tier's purge flag is on; ``purged_rows`` is the money counter;
# ``run_seconds`` catches index rot / lock contention; ``last_success`` powers
# the missed-run pager and is set ONLY on the success path (a crashed run leaves
# it stale so ``time() - last_success > 26h`` fires). ``job``/``table`` labels are
# fixed internal strings (one per purge job), so both label sets are bounded.
RETENTION_CANDIDATES = Gauge(
    "thought2build_retention_candidates",
    "Rows a retention job found eligible for purge at the start of its run "
    "(set even in dry-run / flag-off counting mode), labelled by job.",
    labelnames=["job"],
)
RETENTION_PURGED_ROWS = Counter(
    "thought2build_retention_purged_rows_total",
    "Rows actually deleted by a retention job, labelled by job and the primary "
    "table it targets (cascade-deleted child rows are not separately counted).",
    labelnames=["job", "table"],
)
RETENTION_RUN_SECONDS = Histogram(
    "thought2build_retention_run_seconds",
    "Wall-clock duration of one retention job run, labelled by job.",
    labelnames=["job"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),
)
RETENTION_LAST_SUCCESS_TIMESTAMP = Gauge(
    "thought2build_retention_last_success_timestamp",
    "Unix timestamp of a retention job's last successful completion, labelled by "
    "job. Alert when time() - this > 26h (a missed daily run / persistent error).",
    labelnames=["job"],
)


# Event-loop lag (F7 — scalability audit P2). The acceptance gate for moving
# inline CPU work (bleach / full-document regex / difflib) off the loop is "the
# loop stays responsive while a generation storm is in flight" (audit §8). This
# samples the scheduling delay directly: a watcher sleeps a fixed interval and
# measures how much LONGER than that interval it actually took to be re-scheduled
# — the extra time is loop starvation by a CPU-bound coroutine. A sustained climb
# in the high buckets means CPU work is still blocking the loop (F7 regressed or a
# new inline hot spot appeared).
EVENT_LOOP_LAG_SECONDS = Histogram(
    "thought2build_event_loop_lag_seconds",
    "Sampled asyncio event-loop scheduling delay: time a fixed-interval timer ran "
    "LATE beyond its interval because the loop was busy. High buckets mean "
    "CPU-bound work is starving the loop (audit §8 / F7).",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

_EVENT_LOOP_LAG_SAMPLE_INTERVAL_SECONDS = 5.0


async def run_event_loop_lag_sampler(
    interval: float = _EVENT_LOOP_LAG_SAMPLE_INTERVAL_SECONDS,
) -> None:
    """Continuously sample event-loop scheduling lag (F7 validation).

    One lightweight task per process, started from the app lifespan. Each cycle
    records ``actual_sleep - interval`` (clamped at 0) — the delay the loop added
    on top of the requested sleep. Cancelled cleanly on shutdown; a sampling error
    is swallowed (this is observability, never load-bearing) and the loop
    continues so a single hiccup does not stop the gauge.
    """
    loop = asyncio.get_running_loop()
    while True:
        start = loop.time()
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise
        lag = loop.time() - start - interval
        try:
            EVENT_LOOP_LAG_SECONDS.observe(max(0.0, lag))
        except Exception:  # pragma: no cover — never let metrics break the task
            logger.warning("event_loop_lag.observe_failed")


class _DbPoolCollector:
    """Prometheus collector exposing SQLAlchemy connection-pool stats (F3).

    Surfaces per-process Postgres connection usage so the horizontal-scale-out
    capacity gate (audit §8: pool checked-out / overflow + total open per
    instance vs the confirmed ``max_connections``) is observable, and so a
    PgBouncer rollout (F3) can be verified to flatten the Postgres connection
    count regardless of instance count.

    Read lazily at scrape time from the live engine pool — no background sampler,
    so it always reflects the instant a scrape happens. Fail-soft: a pool that
    does not implement a given stat (e.g. ``NullPool``) simply omits that series,
    and any error yields nothing rather than breaking the whole ``/metrics``
    response.
    """

    def collect(self):  # noqa: D401 — prometheus_client collector protocol
        try:
            from database import async_engine

            pool = async_engine.sync_engine.pool
        except Exception:  # pragma: no cover — engine not built (import order)
            return

        def _stat(name: str):
            method = getattr(pool, name, None)
            if method is None:
                return None
            try:
                return method()
            except Exception:  # pragma: no cover — pool type without this stat
                return None

        checked_out = _stat("checkedout")
        checked_in = _stat("checkedin")
        overflow = _stat("overflow")
        size = _stat("size")

        if checked_out is not None:
            yield GaugeMetricFamily(
                "thought2build_db_pool_checked_out",
                "Connections currently checked out of the SQLAlchemy pool (in use).",
                value=checked_out,
            )
        if checked_in is not None:
            yield GaugeMetricFamily(
                "thought2build_db_pool_checked_in",
                "Idle connections currently held in the SQLAlchemy pool.",
                value=checked_in,
            )
        if overflow is not None:
            yield GaugeMetricFamily(
                "thought2build_db_pool_overflow",
                "Current overflow connections open beyond the configured pool_size "
                "(negative means the pool is not yet full).",
                value=overflow,
            )
        if size is not None:
            yield GaugeMetricFamily(
                "thought2build_db_pool_size",
                "Configured SQLAlchemy pool_size (steady-state pooled connections).",
                value=size,
            )
        if checked_out is not None and checked_in is not None:
            yield GaugeMetricFamily(
                "thought2build_db_pool_total_open",
                "Total open Postgres connections from this process "
                "(checked_in + checked_out) — compare against max_connections.",
                value=checked_in + checked_out,
            )
        # The per-process ceiling: how many connections this process can open at
        # peak (pool_size + max_overflow). An alert compares total_open × the
        # instance count against the confirmed Postgres max_connections.
        yield GaugeMetricFamily(
            "thought2build_db_pool_max",
            "Maximum Postgres connections this process can open at peak "
            "(db_pool_size + db_max_overflow).",
            value=settings.db_pool_size + settings.db_max_overflow,
        )


# Register the pool collector once at import (the module imports once per
# process, so this never double-registers). Guarded so a re-import in a test
# harness that reset the registry does not raise.
try:
    REGISTRY.register(_DbPoolCollector())
except ValueError:  # pragma: no cover — already registered (re-import in tests)
    pass

# LLM batch job metrics (Phase 3 — issue #26). The deferred-batch eval lane has
# its own ``llm:batch:deadletter`` Redis list so a stuck batch is never confused
# with a GitHub export or billing grant. ``submitted`` counts batches created at
# the provider; ``collected`` counts batches whose results were persisted; the
# retry/dead-letter pair is the reliability signal for the lane.
LLM_BATCH_SUBMITTED_TOTAL = Counter(
    "thought2build_llm_batch_submitted_total",
    "Provider Message Batches created for non-interactive judge/eval work, "
    "labelled by operation and provider.",
    labelnames=["operation", "provider"],
)
LLM_BATCH_COLLECTED_TOTAL = Counter(
    "thought2build_llm_batch_collected_total",
    "Deferred batches whose results were collected and persisted, labelled by "
    "operation, provider, and outcome (succeeded / fallback / failed).",
    labelnames=["operation", "provider", "outcome"],
)
LLM_BATCH_JOB_RETRIES_TOTAL = Counter(
    "thought2build_llm_batch_job_retries_total",
    "LLM batch worker job attempts that failed transiently and were retried "
    "(exponential backoff + jitter), labelled by job name.",
    labelnames=["job"],
)
LLM_BATCH_JOB_DEADLETTERED_TOTAL = Counter(
    "thought2build_llm_batch_job_deadlettered_total",
    "LLM batch worker jobs that exhausted max_tries and were moved to the "
    "'llm:batch:deadletter' record for manual replay, labelled by job name.",
    labelnames=["job"],
)
# Per-installation rate governor (Phase 21 — T-274). A throttle is healthy
# backpressure (GitHub 403/429 or local token-bucket exhaustion), distinct from a
# job failure: the job is requeued off the dead-letter try budget. The rate of
# throttles by reason is the signal that an installation is pushing GitHub's
# limits and may need write batching.
GITHUB_THROTTLED_TOTAL = Counter(
    "thought2build_github_throttled_total",
    "GitHub worker jobs requeued by the per-installation rate governor "
    "(backpressure, not failure), labelled by job name and throttle reason "
    "(primary_limit, secondary_limit, repo_contended).",
    labelnames=["job", "reason"],
)

# Inbound webhook ingest metrics (Phase 21 — T-271). received/verified track the
# HMAC gate; deduped counts retried deliveries skipped idempotently; failed is
# labelled by error_type (bad_signature, missing_headers, enqueue_unavailable).
GITHUB_WEBHOOK_RECEIVED_TOTAL = Counter(
    "thought2build_github_webhook_received_total",
    "GitHub webhook deliveries that passed the HMAC gate, by event type.",
    labelnames=["event_type"],
)
GITHUB_WEBHOOK_VERIFIED_TOTAL = Counter(
    "thought2build_github_webhook_verified_total",
    "GitHub webhook deliveries whose signature verified against a current or "
    "previous (rotation) secret.",
)
GITHUB_WEBHOOK_DEDUPED_TOTAL = Counter(
    "thought2build_github_webhook_deduped_total",
    "Retried GitHub webhook deliveries (same X-GitHub-Delivery) skipped as "
    "duplicates, by event type.",
    labelnames=["event_type"],
)
GITHUB_WEBHOOK_REPLAYED_TOTAL = Counter(
    "thought2build_github_webhook_replayed_total",
    "GitHub webhook deliveries re-dispatched by the inbox sweep because they "
    "were recorded on receipt but never processed. Sustained non-zero means "
    "deliveries are being dropped between the ingress and the worker.",
)
GITHUB_WEBHOOK_FAILED_TOTAL = Counter(
    "thought2build_github_webhook_failed_total",
    "GitHub webhook deliveries rejected before dispatch, by error_type.",
    labelnames=["error_type"],
)

# Bidirectional-sync reconcile lag (Phase 21 — T-272): seconds from a webhook
# delivery being received to the worker finishing its reconciliation. This is
# the headline SLO for "closing an issue flips the task done in Thought2Build".
GITHUB_RECONCILE_LAG_SECONDS = Histogram(
    "thought2build_github_reconcile_lag_seconds",
    "Seconds from webhook receipt to reconcile completion.",
    buckets=(0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0),
)

# Workspace exports to GitHub (Phase 21 — T-284), by export mode
# (files_to_default / pr_with_tests) and outcome (completed / failed). Tracks
# export volume + failure rate per mode.
GITHUB_EXPORT_TOTAL = Counter(
    "thought2build_github_export_total",
    "GitHub exports run by the worker, by export_mode and outcome.",
    labelnames=["export_mode", "outcome"],
)
# Pull requests opened by Thought2Build (Phase 21 — T-284): the pr_with_tests export
# and per-increment PRs. ``outcome`` is ``opened`` (reserved for future
# failure attribution).
GITHUB_PR_TOTAL = Counter(
    "thought2build_github_pr_total",
    "Pull requests opened by Thought2Build, by outcome.",
    labelnames=["outcome"],
)
# Thought2Build PR-acceptance checks posted (Phase 21 — T-284), by verdict
# (success / failure / neutral). ``neutral`` is the fail-open verdict (judge
# unavailable / no linked task / budget reached), so a rising neutral rate is
# the signal the evaluator is degraded, not that PRs are failing.
GITHUB_CHECK_TOTAL = Counter(
    "thought2build_github_check_total",
    "Thought2Build PR acceptance checks posted, by verdict.",
    labelnames=["verdict"],
)

# ---------------------------------------------------------------------------
# GitHub structured audit events (Phase 21 — T-284)
# ---------------------------------------------------------------------------
#
# The audit-event vocabulary is anchored here as the single source of truth
# (mirroring AUDIT_EVENT_CRITIC_DISABLED in critic.py). Events are emitted at
# the state-changing sites across the GitHub integration via ``github_audit``;
# several of those sites (the install service, the webhook router) live in
# modules outside the services blob, so centralising the names here also keeps
# the vocabulary discoverable and testable in one place.
GITHUB_AUDIT_INSTALLED = "github.installed"
# Emitted when the install callback refuses to (re)bind an installation because
# the installer could not be verified as an admin of the account (audit #1).
GITHUB_AUDIT_INSTALL_REJECTED = "github.install.rejected"
GITHUB_AUDIT_UNINSTALLED = "github.uninstalled"
GITHUB_AUDIT_WEBHOOK_RECEIVED = "github.webhook.received"
GITHUB_AUDIT_WEBHOOK_DUPLICATE_SKIPPED = "github.webhook.duplicate_skipped"
GITHUB_AUDIT_RECONCILE_TASK_DONE = "github.reconcile.task_done"
GITHUB_AUDIT_EXPORT_COMPLETED = "github.export.completed"
GITHUB_AUDIT_PR_OPENED = "github.pr.opened"
GITHUB_AUDIT_CHECK_POSTED = "github.check.posted"
GITHUB_AUDIT_INCREMENT_PUSHED = "github.increment.pushed"
GITHUB_AUDIT_SYNC_PAUSED = "github.sync.paused"

# The structured fields an audit event may carry. Only id-shaped values ever go
# here — never a token, the App private key, a raw webhook payload, or a PR diff
# (T-284 / spec §12.5). Redaction (redact_structlog_event) is defence-in-depth on
# top of this ids-only contract.
_GITHUB_AUDIT_FIELDS = (
    "installation_id",
    "workspace_id",
    "repo_id",
    "delivery_id",
    "event_type",
    "action",
    "status",
    "push_id",
)

_github_audit_logger = structlog.get_logger("github.audit")


def github_audit(event: str, **fields: Any) -> None:
    """Emit a structured GitHub audit log row (Phase 21 — T-284).

    ``event`` is one of the ``GITHUB_AUDIT_*`` names. Only the recognised
    id-shaped fields in :data:`_GITHUB_AUDIT_FIELDS` are passed through, and any
    that are ``None`` ("where available") are dropped, so every row carries a
    consistent, minimal, content-free schema. Callers must never pass a token,
    private key, raw payload, or PR diff.
    """
    payload = {
        key: fields[key] for key in _GITHUB_AUDIT_FIELDS if fields.get(key) is not None
    }
    _github_audit_logger.info(event, **payload)


PIPELINE_UPSTREAM_SECTION_SKIPPED = Counter(
    "pipeline_upstream_section_skipped_total",
    "Count of upstream sections skipped during section-aware injection "
    "because the 200K budget was exhausted.  A non-zero value here means "
    "the downstream stage saw a summary instead of the verbatim section, "
    "which is a quality regression for large products.",
    labelnames=["stage", "section"],
)

BILLING_CREDITS_CRITIC_REGEN = Counter(
    "thought2build_billing_credits_critic_regen_total",
    "Number of platform-funded stage regenerations triggered by the critic. "
    "Used to attribute the cost of the quality gate against operational P&L. "
    "T-247 (Phase 19).",
    labelnames=["stage"],
)

BILLING_CREDITS_BRAVE_RESEARCH = Counter(
    "thought2build_billing_credits_brave_research_total",
    "User-metered Brave web-research charges — incremented once per successful, "
    "content-bearing (post-sanitisation) paid Brave fetch that debited the user's "
    "credit ledger. Cache hits, empty/all-dropped results, failures, quota skips, "
    "and the disabled/not-opted-in paths charge nothing and never increment here. "
    "Labelled by stage so spend can be attributed to spec vs plan. Issue #12 "
    "(Phase 2).",
    labelnames=["stage"],
)

PIPELINE_VALIDATOR_FAILURES = Counter(
    "pipeline_validator_failures_total",
    "Count of stage generations rejected by the zero-LLM section-presence "
    "validator (a required heading was absent).  Tracks which stage's prompt "
    "most often omits mandatory sections.  T-248 (Phase 19).",
    labelnames=["stage"],
)

PIPELINE_QUALITY_ESCALATIONS = Counter(
    "thought2build_pipeline_quality_escalations_total",
    "Stage generations where a quality-gate failure (critic findings) on the "
    "cheap primary tier triggered an escalation to the mid tier for the "
    "platform-funded regenerate.  A rising rate indicates the cheap primary "
    "regularly falls short of the critic's standards.  Phase 5.1.",
    ["stage_type", "provider"],
)

PIPELINE_CRITIC_ADVISORY_FINDINGS = Counter(
    "thought2build_pipeline_critic_advisory_findings_total",
    "Background (off-critical-path) critic reviews whose verdict FAILED, attaching "
    "non-blocking advisory findings to an already-delivered draft. With the async "
    "advisory critic this is exactly the population that previously triggered the "
    "platform-funded auto-regenerate — so the rate is the signal for the quality "
    "tradeoff of dropping that regenerate (docs/CRITIC_ASYNC_ADVISORY_PLAN.md §4).",
    labelnames=["stage"],
)

PIPELINE_COMPLEXITY_TIER_FLOORS = Counter(
    "thought2build_pipeline_complexity_tier_floors_total",
    "Core generations where the deterministic complexity classifier raised the "
    "starting tier above the cheap primary (e.g. regulated domain, large upstream "
    "chain, prior quality-gate failure), labelled by the assessed complexity "
    "level.  Lets operators see how often — and for which stages/providers — the "
    "classifier chooses to skip the cheap attempt.  Phase 5.2.",
    ["stage_type", "provider", "level"],
)

# ---------------------------------------------------------------------------
# Storyboard (Phase 20).  Counters and histograms are intentionally labelled
# only with bounded enums so a malicious title/slug/error cannot create
# unbounded Prometheus series.  T-262 owns the complete Storyboard metric set.
# ---------------------------------------------------------------------------
STORYBOARD_GENERATION_STARTED = Counter(
    "thought2build_storyboard_generation_started_total",
    "Storyboard generations that acquired a placeholder row and debited credits. "
    "Labelled by action so full generation, full regeneration, and single-section "
    "regeneration can be told apart.  T-254 (Phase 20).",
    labelnames=["action"],
)

STORYBOARD_GENERATION_COMPLETED = Counter(
    "thought2build_storyboard_generation_completed_total",
    "Storyboard generations that validated their LLM payload and reached the "
    "'ready' state.  T-254 (Phase 20).",
    labelnames=["action"],
)

STORYBOARD_GENERATION_FAILED = Counter(
    "thought2build_storyboard_generation_failed_total",
    "Storyboard generations that failed after debiting and were refunded and "
    "marked 'failed'.  ``error_type`` is a coarse, content-free reason (e.g. "
    "'payload_parse', 'payload_schema', 'provider', 'timeout') — never raw "
    "generated text.  T-254 (Phase 20).",
    labelnames=["action", "error_type"],
)

STORYBOARD_SECTION_REGENERATED = Counter(
    "thought2build_storyboard_section_regenerated_total",
    "Single-section Storyboard regenerations that reached the 'ready' state.  "
    "T-254 (Phase 20).",
)

STORYBOARD_GENERATION_DURATION = Histogram(
    "thought2build_storyboard_generation_duration_seconds",
    "Wall-clock duration of the Storyboard LLM generation + validation phase "
    "(excludes the credit/placeholder transaction).  T-254 (Phase 20).",
    labelnames=["action"],
)

STORYBOARD_CREDITS_DEDUCTED = Counter(
    "thought2build_storyboard_credits_deducted_total",
    "Total credits debited for Storyboard generation, by action.  T-254.",
    labelnames=["action"],
)

STORYBOARD_CREDITS_REFUNDED = Counter(
    "thought2build_storyboard_credits_refunded_total",
    "Total credits refunded for failed/recovered Storyboard generations.  "
    "``reason`` is content-free (e.g. 'generation_failed', 'stuck_recovery').  "
    "T-254 (Phase 20).",
    labelnames=["action", "reason"],
)

STORYBOARD_DOWNLOAD = Counter(
    "thought2build_storyboard_download_total",
    "Storyboard artifact downloads.  ``kind`` is the artifact (html, pdf, "
    "notes-md, notes-pdf, demo-script, appendix); ``public`` is 'true' for the "
    "unauthenticated share surface and 'false' for owner downloads.  T-255.",
    labelnames=["kind", "public"],
)

STORYBOARD_PUBLIC_VIEW = Counter(
    "thought2build_storyboard_public_view_total",
    "Unauthenticated public Storyboard views served successfully. 404s and "
    "permission denials are intentionally not counted.  T-262 (Phase 20).",
)

STORYBOARD_SOURCE_MISSING = Counter(
    "thought2build_storyboard_source_missing_total",
    "Expected finalised source sections absent during deterministic Storyboard "
    "source extraction. Labels are bounded source/section enums and never carry "
    "source excerpts.  T-262 (Phase 20).",
    labelnames=["source", "section"],
)

STORYBOARD_ESCALATIONS = Counter(
    "thought2build_storyboard_escalations_total",
    "Storyboard generations where a quality-gate failure on the cheap primary "
    "tier (schema/parse/grounding error, incl. truncation) triggered a one-shot "
    "escalation to the next tier (``mid`` while cheap-primary is live; ``strong`` "
    "when reverted to mid-first).  ``outcome`` is one of: attempted (escalation "
    "started), succeeded (the escalation tier passed validation), failed (the "
    "escalation tier also failed), or no_route (no active model at the escalation "
    "tier for provider — expected for Google, which floors at mid with no further "
    "tier).  A steady no_route rate indicates Google Flash quality is fine; a "
    "rising succeeded rate means the cheap primary is borderline and the "
    "escalation tier regularly saves the generation.  Issue #17 follow-up.",
    labelnames=["action", "provider", "outcome"],
)

STORYBOARD_TRUNCATION_RETRIES = Counter(
    "thought2build_storyboard_truncation_retries_total",
    "Storyboard completions detected as truncated (cut off at the model's output "
    "token ceiling, the dominant parse-failure mode) that triggered a single "
    "budget-doubling retry BEFORE the repair loop — repairing a truncated body "
    "under the same cap is provably futile. Labelled by provider only; carries no "
    "payload content. A rising rate for a provider means its cheap-tier ceiling is "
    "too tight for a full keynote and the doubling on escalation is doing real "
    "work.  P3.3 (storyboard output quality).",
    labelnames=["provider"],
)

_STORYBOARD_ACTION_LABELS = frozenset({"generate", "regenerate", "regenerate_section"})
_STORYBOARD_PROVIDER_LABELS = frozenset({"anthropic", "openai", "google", "openrouter"})
_STORYBOARD_ESCALATION_OUTCOME_LABELS = frozenset(
    {"attempted", "succeeded", "failed", "no_route"}
)
_STORYBOARD_ERROR_TYPE_LABELS = frozenset(
    {
        "payload_parse",
        "payload_schema",
        "provider",
        "timeout",
        "row_missing",
        "unexpected",
    }
)
_STORYBOARD_REFUND_REASON_LABELS = frozenset({"generation_failed", "stuck_recovery"})
_STORYBOARD_DOWNLOAD_KIND_LABELS = frozenset(
    {"html", "pdf", "notes-md", "notes-pdf", "demo-script", "appendix"}
)
_STORYBOARD_SOURCE_LABELS = frozenset({"spec", "plan", "harness", "tasks"})
_STORYBOARD_SECTION_LABELS = frozenset(
    {
        "overview",
        "requirements",
        "journeys",
        "architecture",
        "components",
        "security-architecture",
        "capacity-model",
        "stride",
        "slo",
        "fmea",
        "coverage",
        "must",
    }
)

# ---------------------------------------------------------------------------
# Judge-model spend instrument (issue #27 — Phase 0)
# ---------------------------------------------------------------------------
#
# Every judge-model call in the product has exactly one of four *purposes*:
#   - ``eval.score``  — the post-generation quality score (online_eval / eval_batch)
#   - ``critic``      — the Phase-19 safety/quality gate second pass
#   - ``pr_check``    — the GitHub PR-diff acceptance-criteria judge
#   - ``clarify``     — the pre-generation clarifying-questions judge
#
# ``judge_calls_total`` counts a judge call *actually issued to a provider* (the
# real, billed spend), incremented at each call site — including every retry/
# compact attempt and the deferred-batch submit, so the figure tracks money, not
# logical operations.  ``judge_calls_skipped_total`` counts a judge call that was
# deliberately *not* issued, by reason.  Together they are the before/after
# instrument for the issue #27 rework: as the score is sampled out and the PR
# judge is gated, ``skipped`` rises and ``total`` falls, and the ratio of the two
# proves the cost reduction rather than asserting it.
#
# Phase 0 ships the counters + helpers and wires ``judge_calls_total`` at every
# call site.  Each skip reason is wired by the phase that owns its site
# (``sampled_out`` → Phase 1/2, ``disabled``/``cached``/``deterministic_gate`` →
# Phase 3, ``budget``/``debounce`` → Phase 4); a Counter that has not yet been
# ``.inc()``-ed simply does not export a series, which is expected, not missing.
# Both label vocabularies are bounded enums normalised through the helpers below,
# so an unexpected value collapses to ``unknown`` and can never explode Prometheus
# cardinality.
JUDGE_CALLS_TOTAL = Counter(
    "thought2build_judge_calls_total",
    "LLM judge-model calls actually issued to a provider (billed spend), by "
    "purpose: eval.score / critic / pr_check / clarify. Counts every real "
    "attempt, including compact retries and deferred-batch submits.",
    labelnames=["purpose"],
)
JUDGE_CALLS_SKIPPED_TOTAL = Counter(
    "thought2build_judge_calls_skipped_total",
    "LLM judge-model calls deliberately not issued, by purpose and reason: "
    "sampled_out (below the eval-score sample rate), deterministic_gate (a "
    "deterministic check already decided), disabled (owner/setting opt-out), "
    "budget (daily cap reached), debounce (a recent verdict stands), cached "
    "(an identical artifact verdict was reused).",
    labelnames=["purpose", "reason"],
)

_JUDGE_PURPOSE_LABELS = frozenset({"eval.score", "critic", "pr_check", "clarify"})
_JUDGE_SKIP_REASON_LABELS = frozenset(
    {"sampled_out", "deterministic_gate", "disabled", "budget", "debounce", "cached"}
)


def record_judge_call(purpose: str) -> None:
    """Count one judge-model call actually issued to a provider (issue #27).

    Call at the site where the provider request is made — once per real attempt,
    so retries and compact-prompt re-tries each count as the separate spend they
    are.  ``purpose`` outside the bounded vocabulary collapses to ``unknown``.
    """
    JUDGE_CALLS_TOTAL.labels(purpose=_judge_purpose(purpose)).inc()


def record_judge_call_skipped(purpose: str, reason: str) -> None:
    """Count one judge-model call deliberately not issued (issue #27).

    ``purpose`` and ``reason`` outside their bounded vocabularies collapse to
    ``unknown`` so a stray caller can never explode Prometheus cardinality.
    """
    JUDGE_CALLS_SKIPPED_TOTAL.labels(
        purpose=_judge_purpose(purpose),
        reason=_judge_skip_reason(reason),
    ).inc()


def _judge_purpose(purpose: str) -> str:
    value = str(purpose or "unknown")
    return value if value in _JUDGE_PURPOSE_LABELS else "unknown"


def _judge_skip_reason(reason: str) -> str:
    value = str(reason or "unknown")
    return value if value in _JUDGE_SKIP_REASON_LABELS else "unknown"


_sentry_configured = False
_otel_configured = False
_REDACTED = "[REDACTED]"
_SENSITIVE_KEYS = {
    "api_key",
    "apikey",
    "authorization",
    "client_secret",
    "csrf_token",
    "google_api_key",
    "grafana_otlp_token",
    "jwt_private_key",
    "openai_api_key",
    "anthropic_api_key",
    "password",
    "private_key",
    "refresh_token",
    "refreshtoken",
    "secret",
    "set-cookie",
    "set_cookie",
    # Lemon Squeezy billing credentials (Phase 22 — T-304). The API key is also
    # caught by the Bearer/JWT patterns and the webhook secrets by ``_secret``
    # suffix, but exact key names are listed so a structured field is scrubbed
    # regardless of how it is logged. ``checkout_nonce`` is the raw secret proven
    # back by the signed webhook — only its sha256 is ever persisted (SR4).
    "lemonsqueezy_api_key",
    "lemonsqueezy_webhook_secret",
    "lemonsqueezy_webhook_secret_prev",
    "checkout_nonce",
    # Webhook signature header, scrubbed by key (the pattern below also catches it
    # inline in free text). Normalisation lower-cases and maps '-'→'_', so
    # ``X-Signature`` matches this.
    "x_signature",
    "token",
    # GitHub App credentials (Phase 21 — T-283). Key matching is exact, so the
    # App private key and the various installation-token field names are listed
    # explicitly even though their values are also caught by the patterns below.
    "github_app_private_key",
    "github_app_webhook_secret_prev",
    "access_token",
    "installation_token",
    "inst_token",
}
_LOG_RECORD_BUILTINS = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)
_SECRET_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"Bearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE),
    re.compile(r"Basic\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE),
    re.compile(r"sk-[A-Za-z0-9_-]{8,}", re.IGNORECASE),
    re.compile(r"AIza[0-9A-Za-z_-]{20,}"),
    re.compile(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
        re.DOTALL,
    ),
    re.compile(r"(?i)(refresh[_-]?token\s*[:=]\s*)['\"]?[^'\"\s,;}]+['\"]?"),
    re.compile(r"(?i)(authorization\s*[:=]\s*)['\"]?[^'\"\s,;}]+['\"]?"),
    # GitHub tokens (Phase 21 — T-283): installation (ghs_), user-to-server
    # (ghu_), OAuth (gho_), PAT (ghp_), and refresh (ghr_). Installation tokens
    # are credentials — scrub the value wherever it appears, not just under a
    # known key. The char class includes ``.``/``-``/``_`` so it also matches the
    # new JWT-based *stateless* installation-token format (``ghs_`` + two dots,
    # ~520 chars; GitHub's recommended regex is ``ghs_[A-Za-z0-9.\-_]{36,}``) —
    # an alphanumeric-only class would leak the dotted JWT tail in logs.
    re.compile(r"gh[pousr]_[A-Za-z0-9._-]{20,}"),
    # Lemon Squeezy API keys (Phase 22 — T-304): JWTs (three base64url segments).
    # Scrubbed wherever they appear; the Bearer pattern also catches them in an
    # Authorization header. Three dot-separated segments keep this specific so it
    # never matches arbitrary text or a bare sha256 hash.
    re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"),
    # Webhook signature header (Phase 22 — T-304): the Lemon ``X-Signature`` (hex
    # HMAC-SHA256). Keyed on the header name so the intentionally-logged
    # nonce/payload sha256 hashes (also 64-hex) are NOT scrubbed.
    re.compile(r"(?i)(x-signature\s*[:=]\s*)['\"]?[0-9a-f]{16,}['\"]?"),
)


class SensitiveDataFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact_sensitive_data(record.getMessage())
        record.args = ()

        for key, value in list(record.__dict__.items()):
            if key in _LOG_RECORD_BUILTINS:
                continue
            record.__dict__[key] = redact_sensitive_data({key: value})[key]

        if record.exc_text:
            record.exc_text = redact_sensitive_data(record.exc_text)
        return True


def redact_sensitive_data(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: dict[Any, Any] = {}
        for key, item in value.items():
            if _is_sensitive_key(key):
                redacted[key] = _REDACTED
            else:
                redacted[key] = redact_sensitive_data(item)
        return redacted

    if isinstance(value, list):
        return [redact_sensitive_data(item) for item in value]

    if isinstance(value, tuple):
        return tuple(redact_sensitive_data(item) for item in value)

    if isinstance(value, str):
        return _redact_string(value)

    return value


def redact_structlog_event(
    _logger: Any, _method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    return redact_sensitive_data(event_dict)


def configure_logging() -> None:
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            redact_structlog_event,
            timestamper,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        cache_logger_on_first_use=True,
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
    )
    _install_sensitive_data_filter()


def setup_sentry() -> None:
    global _sentry_configured

    if _sentry_configured or not _is_configured_url(settings.sentry_dsn):
        return

    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        environment=settings.environment,
        integrations=[FastApiIntegration()],
        traces_sample_rate=0.1,
        before_send=_redact_sentry_event,
    )
    _sentry_configured = True


def setup_opentelemetry(app: FastAPI, engine: AsyncEngine) -> None:
    global _otel_configured

    if _otel_configured or not _is_configured_url(settings.grafana_otlp_endpoint):
        return

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = Resource.create(
        {
            "service.name": "thought2build-api",
            "deployment.environment": settings.environment,
        }
    )
    provider = TracerProvider(resource=resource)
    exporter_kwargs: dict[str, object] = {"endpoint": settings.grafana_otlp_endpoint}
    if settings.grafana_otlp_token:
        exporter_kwargs["headers"] = {
            "Authorization": f"Bearer {settings.grafana_otlp_token}"
        }
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(**exporter_kwargs)))
    trace.set_tracer_provider(provider)

    FastAPIInstrumentor.instrument_app(app)
    SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)
    _otel_configured = True


def setup_metrics(app: FastAPI) -> None:
    @app.middleware("http")
    async def metrics_middleware(
        request: Request,
        call_next: Callable[[Request], object],
    ) -> Response:
        start = time.perf_counter()
        status_code = 500
        route = request.url.path

        try:
            response = await call_next(request)
            status_code = response.status_code
            route = _route_path(request)
            return response
        except Exception:
            route = _route_path(request)
            logger.exception(
                "request_failed",
                method=request.method,
                path=route,
                status_code=status_code,
            )
            raise
        finally:
            duration = time.perf_counter() - start
            REQUEST_COUNT.labels(request.method, route, str(status_code)).inc()
            REQUEST_LATENCY.labels(request.method, route).observe(duration)
            logger.info(
                "request_completed",
                method=request.method,
                path=route,
                status_code=status_code,
                duration_ms=round(duration * 1000, 2),
            )

    @app.get("/metrics", include_in_schema=False)
    async def metrics(request: Request) -> StarletteResponse:
        auth_header = request.headers.get("Authorization") or ""
        token = auth_header.removeprefix("Bearer ").strip()
        if settings.metrics_token:
            # Constant-time compare so a scraper can't recover the token byte by
            # byte from response-timing deltas.
            if not hmac.compare_digest(token.encode(), settings.metrics_token.encode()):
                return StarletteResponse("Unauthorized", status_code=401)
        elif settings.environment.lower() == "production":
            return StarletteResponse("Metrics token required", status_code=503)
        else:
            # When no token is configured, restrict to loopback addresses only
            client_host = (request.client.host if request.client else "") or ""
            if client_host not in ("127.0.0.1", "::1", "localhost"):
                return StarletteResponse("Unauthorized", status_code=401)
        return StarletteResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def setup_observability(app: FastAPI, engine: AsyncEngine) -> None:
    configure_logging()
    setup_sentry()
    setup_opentelemetry(app, engine)
    setup_metrics(app)


def record_llm_cost_event(metadata: dict[str, Any]) -> None:
    provider = str(metadata.get("provider") or "unknown")
    model_tier = str(metadata.get("model_tier") or "unknown")
    operation = str(metadata.get("operation") or "unknown")
    stage_type = str(metadata.get("stage_type") or "unknown")
    method = str(metadata.get("usage_estimation_method") or "unknown")
    cache_hit = "true" if bool(metadata.get("cache_hit")) else "false"

    labels = (provider, model_tier, operation, stage_type)
    LLM_REQUEST_COUNT.labels(*labels, cache_hit).inc()
    _inc_counter(
        LLM_ESTIMATED_COST_USD.labels(*labels),
        metadata.get("estimated_cost_usd"),
    )
    _inc_counter(
        LLM_INPUT_TOKENS.labels(*labels, method),
        metadata.get("input_tokens"),
    )
    _inc_counter(
        LLM_OUTPUT_TOKENS.labels(*labels, method),
        metadata.get("output_tokens"),
    )
    _inc_counter(
        LLM_CACHED_INPUT_TOKENS.labels(*labels),
        metadata.get("cached_input_tokens"),
    )
    _inc_counter(
        LLM_CACHE_WRITE_INPUT_TOKENS.labels(*labels),
        metadata.get("cache_write_input_tokens"),
    )
    latency_ms = _as_float(metadata.get("latency_ms"))
    if latency_ms is not None and latency_ms >= 0:
        LLM_LATENCY_SECONDS.labels(*labels).observe(latency_ms / 1000)
    first_event_latency_ms = _as_float(metadata.get("first_event_latency_ms"))
    if first_event_latency_ms is not None and first_event_latency_ms >= 0:
        LLM_FIRST_EVENT_LATENCY_SECONDS.labels(*labels).observe(
            first_event_latency_ms / 1000
        )
    if metadata.get("eligible_prefix_fingerprint"):
        cache_outcome = (
            "hit" if bool(metadata.get("provider_prompt_cache_hit")) else "miss"
        )
        LLM_PROVIDER_PROMPT_CACHE_REQUESTS.labels(*labels, cache_outcome).inc()
    if bool(metadata.get("cross_provider_fallback")):
        LLM_CROSS_PROVIDER_FALLBACK_COUNT.labels(*labels).inc()


def record_llm_provider_failure(provider: str, error_type: str) -> None:
    LLM_PROVIDER_ERROR_COUNT.labels(provider, error_type or "unknown").inc()


def record_llm_provider_configured(provider: str, configured: bool) -> None:
    LLM_PROVIDER_CONFIGURED.labels(provider).set(1 if configured else 0)


def record_llm_provider_health(provider: str, health: str) -> None:
    values = {
        "not_configured": 0,
        "unhealthy": 1,
        "degraded": 2,
        "healthy": 3,
    }
    LLM_PROVIDER_HEALTH.labels(provider).set(values.get(health, 0))


def record_storyboard_generation_started(action: str) -> None:
    STORYBOARD_GENERATION_STARTED.labels(action=_storyboard_action(action)).inc()


def record_storyboard_generation_completed(action: str) -> None:
    STORYBOARD_GENERATION_COMPLETED.labels(action=_storyboard_action(action)).inc()


def record_storyboard_generation_failed(action: str, error_type: str) -> None:
    STORYBOARD_GENERATION_FAILED.labels(
        action=_storyboard_action(action),
        error_type=_storyboard_error_type(error_type),
    ).inc()


def record_storyboard_section_regenerated() -> None:
    STORYBOARD_SECTION_REGENERATED.inc()


def record_storyboard_generation_duration(action: str, duration_seconds: float) -> None:
    if duration_seconds >= 0:
        STORYBOARD_GENERATION_DURATION.labels(
            action=_storyboard_action(action)
        ).observe(duration_seconds)


def record_storyboard_credits_deducted(action: str, amount: int | float) -> None:
    _inc_counter(
        STORYBOARD_CREDITS_DEDUCTED.labels(action=_storyboard_action(action)),
        amount,
    )


def record_storyboard_credits_refunded(
    action: str, reason: str, amount: int | float
) -> None:
    _inc_counter(
        STORYBOARD_CREDITS_REFUNDED.labels(
            action=_storyboard_action(action),
            reason=_storyboard_refund_reason(reason),
        ),
        amount,
    )


def record_storyboard_public_view() -> None:
    STORYBOARD_PUBLIC_VIEW.inc()


def record_storyboard_download(kind: str, *, public: bool) -> str:
    kind_label = _storyboard_download_kind(kind)
    STORYBOARD_DOWNLOAD.labels(
        kind=kind_label,
        public="true" if public else "false",
    ).inc()
    return kind_label


def record_storyboard_source_missing(source: str, section: str) -> None:
    STORYBOARD_SOURCE_MISSING.labels(
        source=_storyboard_source(source),
        section=_storyboard_section(section),
    ).inc()


def record_storyboard_escalation(action: str, provider: str, outcome: str) -> None:
    """Increment the storyboard escalation counter (issue #17 follow-up).

    The escalation tier is the cheap primary's one-tier step up (``mid`` while
    cheap-primary is live; ``strong`` when reverted to mid-first). Call once per
    state transition:
    - "attempted" when starting the escalation retry after a primary quality failure
    - "succeeded" when the escalation attempt passes validation
    - "failed" when the escalation attempt also fails validation
    - "no_route" when no active model exists at the escalation tier (Google)
    """
    STORYBOARD_ESCALATIONS.labels(
        action=_storyboard_action(action),
        provider=_storyboard_provider(provider),
        outcome=_storyboard_escalation_outcome(outcome),
    ).inc()


def record_storyboard_truncation_retry(provider: str) -> None:
    """Count one truncation-triggered budget-doubling retry (P3.3).

    Called when a storyboard completion is detected as truncated (cut off at the
    model output ceiling) and a single doubled-budget retry is issued before the
    repair loop. ``provider`` outside the bounded vocabulary collapses to
    ``unknown``; no payload content is ever passed.
    """
    STORYBOARD_TRUNCATION_RETRIES.labels(provider=_storyboard_provider(provider)).inc()


def _storyboard_action(action: str) -> str:
    value = str(action or "unknown")
    return value if value in _STORYBOARD_ACTION_LABELS else "unknown"


def _storyboard_error_type(error_type: str) -> str:
    value = str(error_type or "unexpected")
    return value if value in _STORYBOARD_ERROR_TYPE_LABELS else "unexpected"


def _storyboard_refund_reason(reason: str) -> str:
    value = str(reason or "generation_failed")
    return value if value in _STORYBOARD_REFUND_REASON_LABELS else "generation_failed"


def _storyboard_download_kind(kind: str) -> str:
    value = str(kind or "unknown")
    if value == "notes":
        value = "notes-md"
    return value if value in _STORYBOARD_DOWNLOAD_KIND_LABELS else "unknown"


def _storyboard_source(source: str) -> str:
    value = str(source or "").lower()
    return value if value in _STORYBOARD_SOURCE_LABELS else "unknown"


def _storyboard_section(section: str) -> str:
    value = str(section or "").split(":", 1)[-1].lower().replace("_", "-")
    return value if value in _STORYBOARD_SECTION_LABELS else "unknown"


def _storyboard_provider(provider: str) -> str:
    value = str(provider or "unknown")
    return value if value in _STORYBOARD_PROVIDER_LABELS else "unknown"


def _storyboard_escalation_outcome(outcome: str) -> str:
    value = str(outcome or "unknown")
    return value if value in _STORYBOARD_ESCALATION_OUTCOME_LABELS else "unknown"


def _inc_counter(counter, value: Any) -> None:
    numeric = _as_float(value)
    if numeric is not None and numeric > 0:
        counter.inc(numeric)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_configured_url(value: str) -> bool:
    return value.startswith(("http://", "https://"))


def _install_sensitive_data_filter() -> None:
    root_logger = logging.getLogger()
    if not any(isinstance(f, SensitiveDataFilter) for f in root_logger.filters):
        root_logger.addFilter(SensitiveDataFilter())

    for handler in root_logger.handlers:
        if not any(isinstance(f, SensitiveDataFilter) for f in handler.filters):
            handler.addFilter(SensitiveDataFilter())


def _redact_sentry_event(
    event: dict[str, Any], _hint: dict[str, Any]
) -> dict[str, Any]:
    return redact_sensitive_data(event)


def _redact_string(value: str) -> str:
    redacted = value
    for pattern in _SECRET_PATTERNS:
        redacted = pattern.sub(_replace_secret_match, redacted)
    return redacted


def _replace_secret_match(match: re.Match[str]) -> str:
    if match.lastindex:
        return f"{match.group(1)}{_REDACTED}"
    return _REDACTED


def _is_sensitive_key(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "_")
    return normalized in _SENSITIVE_KEYS or normalized.endswith("_secret")


def _route_path(request: Request) -> str:
    route = request.scope.get("route")
    path = getattr(route, "path", None)
    return path if isinstance(path, str) else request.url.path
