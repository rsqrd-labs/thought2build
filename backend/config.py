from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str
    redis_url: str

    jwt_private_key: str
    jwt_public_key: str
    google_client_id: str
    google_client_secret: str
    frontend_url: str

    # Marketing-zone canonical origin (issue #18, Phase 7). The Astro marketing
    # zone owns its own ``PUBLIC_SITE_URL``; this is the backend-side mirror for
    # any server-emitted canonical/sitemap concern. Optional (empty = unset, no
    # backend consumer reads it yet) but HTTPS-enforced in prod when set — see
    # ``validate_production_settings``. Hard-requiring it would fail-boot every
    # existing prod deploy that has not configured it, so the guard is
    # HTTPS-when-set (mirroring ``langfuse_host``), not unconditional.
    site_url: str = ""

    anthropic_api_key: str
    openai_api_key: str
    google_api_key: str
    # OpenRouter (issue #152): a fourth, OPTIONAL provider reached through a
    # thin chat_completions adapter over the openai SDK. Unlike the three
    # required keys above, this defaults to "" — a required-with-no-default
    # field would break every existing .env and CI environment the moment
    # openrouter joins REQUIRED_PROVIDERS. An empty value is treated as
    # unset by is_provider_configured, exactly like a "placeholder-" key.
    openrouter_api_key: str = ""
    # Server-owned LLM provider precedence. This is deliberately never exposed
    # through a user API: product traffic is routed by backend policy.
    #
    # Anthropic leads: Claude is the platform's funded provider (Claude Opus 5
    # for full-artifact generation — the four core stages, full regenerate and
    # the harness gap-patch — and Claude Haiku 4.5 for the cheap paths: judge,
    # eval, summary, focused/section refine, storyboard and increment). OpenAI
    # and Google stay in the list as fallbacks and are only reachable when
    # Anthropic's key is unset/placeholder or its circuit is open — routing
    # skips any provider that is not configured (see
    # provider_status.is_provider_configured), so an unconfigured fallback
    # simply never wins. Falling back to a cheaper platform is an env change
    # only: set this to "google,anthropic,openai" and restore GOOGLE_API_KEY.
    llm_provider_priority: str = "anthropic,openai,google"

    @field_validator("llm_provider_priority")
    @classmethod
    def validate_llm_provider_priority(cls, value: str) -> str:
        providers = [item.strip().lower() for item in value.split(",") if item.strip()]
        allowed = {"anthropic", "openai", "google", "openrouter"}
        if not providers:
            raise ValueError("LLM_PROVIDER_PRIORITY must contain at least one provider")
        if len(providers) != len(set(providers)):
            raise ValueError("LLM_PROVIDER_PRIORITY must not contain duplicates")
        unknown = set(providers) - allowed
        if unknown:
            raise ValueError(
                "LLM_PROVIDER_PRIORITY contains unknown providers: "
                + ", ".join(sorted(unknown))
            )
        return ",".join(providers)

    encryption_master_key: str
    csrf_secret: str

    sentry_dsn: str = ""
    grafana_otlp_endpoint: str = ""
    grafana_otlp_token: str = ""

    # LLM observability (optional). Leave langfuse_secret_key empty to disable
    # the Langfuse integration entirely; when empty the SDK is never imported
    # and zero network traffic is sent to a Langfuse host.
    langfuse_secret_key: str = ""
    langfuse_public_key: str = ""
    langfuse_host: str = "https://cloud.langfuse.com"
    langfuse_prompt_cache_ttl: int = 300
    # Remote prompt bodies are deploy-pinned by name to {version, sha256}.
    # Unlisted names use the reviewed local prompt. Never resolve "latest".
    langfuse_prompt_pins: dict[str, dict] = {}

    @field_validator("langfuse_prompt_pins")
    @classmethod
    def validate_prompt_pins(cls, pins: dict[str, dict]) -> dict[str, dict]:
        import re

        for name, pin in pins.items():
            if (
                not isinstance(pin.get("version"), int)
                or isinstance(pin.get("version"), bool)
                or pin["version"] < 1
                or not re.fullmatch(r"[0-9a-f]{64}", str(pin.get("sha256", "")))
            ):
                raise ValueError(f"Invalid immutable prompt pin for {name}")
        return pins

    # Hard upper bound on how long a single Langfuse prompt fetch may take.
    # The SDK's get_prompt is synchronous and on a cache miss issues a
    # blocking HTTP call (default ~10s × 3 retries). We dispatch it to a
    # worker thread and bound the await with this timeout so a slow or
    # unreachable Langfuse host cannot stall the event loop or stage
    # generation. A configured immutable pin fails on timeout; unpinned
    # prompt names use local bodies without a fetch.
    langfuse_prompt_fetch_timeout_seconds: float = 5.0
    langfuse_content_capture_ack: bool = False

    environment: str

    # Empty string disables token auth; fall back to localhost-only IP check
    metrics_token: str = ""
    trusted_proxy_ips: str = ""
    # Comma-separated Host allowlist enforced by TrustedHostMiddleware in
    # production only (dev/CI leave it empty and the middleware is not added, so
    # local/compose flows on arbitrary hosts keep working). Defends against
    # Host-header injection / cache poisoning if the edge ever forwards an
    # unvalidated Host. Include the app host and the Railway internal
    # healthcheck host. A wildcard entry ("*.example.com", Starlette's
    # TrustedHostMiddleware syntax) matches subdomains.
    allowed_hosts: str = ""
    # CSRF replay protection normally fails OPEN on a Redis outage so a transient
    # blip cannot take down every authenticated mutation. For these path
    # prefixes it fails CLOSED instead (403 when Redis is unavailable): a
    # replayed billing/auth mutation is worse than a brief checkout failure
    # during an outage. Comma-separated; empty disables the fail-closed override.
    csrf_fail_closed_prefixes: str = "/billing,/auth"
    max_active_workspaces_per_user: int = 50
    auth_login_burst_limit: int = 5
    auth_login_burst_window_seconds: int = 300
    auth_login_hourly_limit: int = 20
    auth_login_hourly_window_seconds: int = 3600

    db_pool_size: int = 20
    db_max_overflow: int = 10
    # F9 (scalability audit) — fast-fail + server-side DB guards.
    #
    # db_pool_timeout: seconds a checkout waits for a free pooled connection
    #   before raising TimeoutError. SQLAlchemy's default is 30s; a saturated
    #   pool should fast-fail (a 503 the caller can retry) rather than pile up
    #   30s-blocked requests, so the default here is a small 5s.
    # db_statement_timeout_ms: Postgres `statement_timeout` set per connection
    #   via asyncpg server_settings. Bounds any single runaway query. 0 disables.
    #   Migrations run on a separate NullPool engine (migrations/env.py) and are
    #   unaffected.
    # db_idle_in_transaction_timeout_ms: Postgres
    #   `idle_in_transaction_session_timeout`. Frees a connection left idle
    #   inside an open transaction (a leak/bug backstop). The same engine config
    #   serves the API and the arq worker; some worker jobs (e.g. a large GitHub
    #   re-sync) legitimately hold a read transaction open across a sequence of
    #   external GitHub calls, so the default is generous (5 min) — it still
    #   bounds a genuine leak while never false-killing a healthy long job. The
    #   API generation path commits before its multi-minute stream (audit §1), so
    #   it is never idle-in-transaction regardless of this bound. 0 disables.
    # These are applied to the postgres engine only (asyncpg server_settings);
    # a non-postgres DATABASE_URL (e.g. a test sqlite URL) never receives them.
    db_pool_timeout: int = 5
    db_statement_timeout_ms: int = 30_000
    db_idle_in_transaction_timeout_ms: int = 300_000

    # F3 (scalability audit) — transaction-pooler (PgBouncer) compatibility.
    #
    # Horizontal scale-out is gated by the idle Postgres connection footprint ×
    # instance count (each process holds up to db_pool_size+db_max_overflow open
    # connections). A transaction-mode pooler in front of Postgres multiplexes
    # many app connections onto a small fixed server-connection pool, so the
    # Postgres connection count stays flat regardless of instance count. It is
    # ALREADY compatible here because the generation pipeline releases its
    # connection during the multi-minute stream (audit §1) — no app code relies
    # on session-scoped server state across statements.
    #
    # The one runtime hazard is asyncpg's server-side PREPARED STATEMENTS: in
    # transaction mode consecutive statements may land on different backend
    # connections, so a prepared statement cached against one is missing on the
    # next (asyncpg raises InvalidCachedStatementError / a duplicate-name error).
    # When this flag is true the engine disables SQLAlchemy's prepared-statement
    # cache AND asyncpg's own type-introspection cache, and gives every prepared
    # statement a unique name (per the SQLAlchemy asyncpg+PgBouncer guidance),
    # making it safe behind a transaction pooler.
    #
    # CAVEAT (RUNBOOK §15): the F9 server_settings guards (statement_timeout /
    # idle_in_transaction_session_timeout) are sent as connection STARTUP
    # parameters, which a transaction pooler accepts (via the pooler's
    # `ignore_startup_parameters`) but does NOT apply to the backend session. So
    # in pooler mode those guards must be set at the Postgres ROLE/server level
    # instead (e.g. `ALTER ROLE app SET statement_timeout = '30s'`); the app
    # keeps emitting them harmlessly for the no-pooler path.
    #
    # Default False ⇒ direct-to-Postgres connect_args are byte-identical to
    # today; flip true only when a pooler is actually in front (RUNBOOK §15).
    db_transaction_pooler_mode: bool = False

    # F8 (scalability audit) — bound + health-check the shared Redis pool.
    #
    # Every request touches Redis (rate limiting, credit/generation cache,
    # admission control), so an unbounded pool can balloon connections toward
    # Redis `maxclients` under a burst, and a connection left stale after a
    # failover surfaces as an error on next use. These settings flow through the
    # single client factory in database.build_redis_client():
    #   redis_max_connections: hard cap on the pool size per process.
    #   redis_socket_timeout: per-command read/write timeout (fast-fail).
    #   redis_socket_connect_timeout: connection-establishment timeout.
    #   redis_health_check_interval: seconds between idle-connection PINGs, so a
    #     stale post-failover connection is detected and recycled, not handed out.
    redis_max_connections: int = 50
    redis_socket_timeout: float = 5.0
    redis_socket_connect_timeout: float = 2.0
    redis_health_check_interval: int = 30

    # F1 (scalability audit) — admission control on the generation path.
    #
    # max_concurrent_generations_per_process: per-worker-process ceiling on
    #   in-flight stage generations. The primary safety valve so one process
    #   cannot self-immolate (event-loop saturation, background fan-out, shared
    #   provider key). Sized off the measured provider budget; tune via env. 0
    #   disables the per-process limiter.
    # max_concurrent_generations_per_user: per-user ceiling on concurrent
    #   in-flight generations, enforced via a self-healing Redis lease so it holds
    #   across processes/instances. 0 disables.
    # generation_admission_lease_ttl_seconds: how long a per-user/per-provider
    #   admission lease survives without an explicit release (process-death
    #   self-heal). Runtime clamps this to at least generation deadline + 120s,
    #   so a live run is never evicted and stale outage capacity clears promptly.
    # generation_rate_per_minute / _window_seconds: the HTTP-native generation
    #   rate tier enforced in RateLimitMiddleware (true 429 + Retry-After). This
    #   is the authoritative per-minute generation cap; the daily ceiling lives in
    #   the stage manager. 0 disables the tier.
    max_concurrent_generations_per_process: int = 4
    max_concurrent_generations_per_user: int = 3
    # Worker-local call bulkhead. A single generation may draft several chunks
    # concurrently, so generation-count admission alone does not bound actual
    # provider sockets/RPM pressure.
    max_concurrent_provider_calls_per_process: int = 8
    # A PLAN has four independent chunks. Sending all four to one upstream at
    # once turns a single user action into a correlated 429/503 burst and can
    # trip a provider circuit before a successful sibling reports recovery.
    # Keep the wave semantics (all chunks remain independent/checkpointable),
    # but admit at most this many PLAN calls from one generation concurrently.
    max_concurrent_plan_chunks_per_generation: int = 2
    # Production/staging must prove that a generation-capable worker from the
    # same deploy revision is alive before a cache-miss run may be charged.
    # Development/test remain usable without running arq; the runtime helper
    # enforces this flag only outside those local environments.
    generation_worker_readiness_required: bool = True
    # Strict mode: additionally require the worker's deploy revision to equal the
    # API's. Off by default — the API and worker are independently deployed
    # Railway services, so a mismatch is the normal state for the minute between
    # two deploys landing, and gating paid generation on it turns every rolling
    # deploy into a user-visible outage. A live-but-behind worker is logged
    # (``generation.worker_revision_mismatch``) either way.
    generation_worker_revision_match_required: bool = False
    generation_admission_lease_ttl_seconds: int = 720
    generation_rate_per_minute: int = 10
    generation_rate_window_seconds: int = 60

    # F6 (scalability audit) — bound the post-`done` background-task fan-out.
    #
    # max_concurrent_advisory_tasks: shared ceiling on how many ADVISORY
    #   background tasks (the best-effort eval score + the off-critical-path
    #   critic judge + the Demo-Day construction verifier) run concurrently, so a
    #   burst of post-`done` work cannot starve live generation streams for the
    #   shared provider key / DB pool / event loop. The durable generation worker
    #   is bounded separately by F1 admission and provider-call capacity. 0
    #   disables the advisory gate (unbounded — the
    #   pre-F6 behaviour, the instant-revert path).
    # background_tasks_soft_max: per-registry soft ceiling that, when crossed,
    #   emits a one-shot high-water WARNING (a back-pressure signal that a
    #   registry is leaking tasks past the F1 cap). It NEVER drops a task —
    #   shedding advisory work would silently lose eval/critic findings and a
    #   durable pipeline runs in ARQ and is never part of this registry. 0 disables the
    #   warning. The live count is always exported as thought2build_background_tasks.
    max_concurrent_advisory_tasks: int = 12
    background_tasks_soft_max: int = 200

    # F7 (scalability audit P2) — offload inline CPU-bound work off the event loop.
    #
    # bleach.clean (HTML sanitise), the full-document regex validators
    # (output_validator / prompt_guard / artifact_validator), and difflib all run
    # synchronously and hold the GIL. On a multi-KB-to-200KB LLM artifact a single
    # inline pass can stall the asyncio event loop for many milliseconds, blocking
    # every peer coroutine on that worker (only WEB_CONCURRENCY of them). Each is a
    # *many-small-operations* workload (per-pattern .search, per-section/line
    # loops, html5lib's pure-Python tokenizer), so the GIL is released at the
    # interpreter switch interval and a worker thread lets the loop interleave
    # peers between hand-offs. Routed through the dedicated bounded pool in
    # services.cpu_offload (isolated from the Langfuse/PDF I/O offload).
    #   cpu_offload_max_workers: size of the dedicated CPU-offload thread pool.
    #     Deliberately small — extra threads cannot win back wall-clock on
    #     GIL-bound work and only add scheduling contention. Clamped to >=1.
    #   cpu_offload_min_chars: inputs below this many characters run INLINE — the
    #     thread-dispatch round-trip costs more than a short regex/bleach pass, so
    #     only genuinely large payloads are offloaded. Set huge to force inline
    #     everywhere (disable offload); set 0 to offload every call.
    cpu_offload_max_workers: int = 4
    cpu_offload_min_chars: int = 4_096

    # F10 (scalability audit P2) — env-driven PDF render thread-pool size. The
    # WeasyPrint render is CPU-bound and runs on a dedicated bounded pool isolated
    # from the Langfuse/CPU-offload threads (a PDF burst must not starve them).
    # Two workers is the conservative default (the per-tier _PDF_EXPORT_LIMIT caps
    # admission separately); raise only if PDF export becomes a hot path. Clamped
    # to >=1.
    pdf_export_max_workers: int = 2

    # Public-share payload cache (scalability audit P2). The unauthenticated
    # GET /public/{slug} surface is hit by anonymous viewers and scrapers; each
    # miss runs two DB reads (workspace + stages) and a coverage rollup against
    # the shared Postgres pool. A short Redis cache of the assembled, allow-listed
    # response keeps a burst of public traffic off the pool. The TTL sits within
    # the staleness the response already advertises (Cache-Control max-age=60), so
    # it makes nothing staler than the HTTP contract; positives only are cached
    # (never 404s — that would be a memory vector under arbitrary-slug scraping),
    # and the key is evicted on disable/rotate so a killed share is never served.
    # 0 disables the cache (every request reads through to the DB). Fail-open:
    # any Redis error falls back to the DB read.
    public_share_cache_ttl_seconds: int = 60

    # F2 (scalability audit) — global provider budget + 429-aware backoff.
    #
    # provider_max_inflight_generations / provider_max_generations_per_minute:
    #   global (Redis-coordinated) ceilings on concurrent / per-minute generation
    #   calls against the shared platform key for a provider. They feed the F1
    #   admission valve so the system sheds (503/busy) on OUR budget before the
    #   provider returns a 429. SCOPE: these count ONLY the core generate /
    #   regenerate path (the only callers of admit_generation). Storyboard,
    #   increment, and the harness gap-patch also hit the shared key but are
    #   governed by their OWN rate tiers and are NOT counted here — so this is a
    #   core-generation budget, not a whole-account budget (audit F1/F2 scope;
    #   those paths verifiably do not amplify on 429 — they surface it directly).
    #   The in-flight ceiling ships at a conservative 4 durable generations per
    #   provider: one generation can fan out up to four simultaneous chunk calls,
    #   and the worker-local call bulkhead is 8. Tune it from measured org limits.
    #   The RPM ceiling remains 0 (unlimited) until the real account limit is known.
    # provider_rate_limit_max_retries: bounded same-tier retries when a provider
    #   returns 429/overloaded. A rate-limit is a throughput failure, NOT a
    #   quality failure, so it is retried in place (honoring Retry-After, then
    #   exponential backoff + jitter) and never escalated within the throttled
    #   provider. Exhaustion may move to a separately configured provider.
    # provider_rate_limit_backoff_base_seconds / _max_seconds: the backoff used
    #   when the provider sends no Retry-After header.
    provider_max_inflight_generations: int = 4
    provider_max_generations_per_minute: int = 0
    provider_rate_limit_max_retries: int = 3
    provider_rate_limit_backoff_base_seconds: float = 2.0
    provider_rate_limit_backoff_max_seconds: float = 30.0
    # LLM circuit-breaker rejections emit thought2build_llm_circuit_rejections_total.
    #
    # Stream-watchdog policy: a generation stream is killed only when it is
    # actually unhealthy, never merely because the artifact is long.
    # - idle timeout: maximum gap between two provider stream EVENTS, not
    #   visible tokens — adapters yield empty liveness sentinels for
    #   reasoning/thinking deltas, pings, and usage chunks, so a frontier
    #   model reasoning silently for minutes never trips this bound while its
    #   connection is demonstrably alive.
    # - hard cap: absolute upper bound for a single stream call, bounding
    #   runaway provider cost.
    llm_stream_idle_timeout_seconds: int = 180
    llm_stream_hard_cap_seconds: int = 900
    llm_complete_timeout_seconds: int = 120
    # Core stage-run deadline contract. Generation is durable background work,
    # so reliability takes precedence over forcing every model/chunk through a
    # request-shaped deadline. The attempt cap is deliberately less
    # than half of the provider budget: a primary timeout must leave enough time
    # for a real alternate-provider attempt plus terminal persistence.
    stage_generation_deadline_seconds: int = 600
    stage_generation_finalise_reserve_seconds: int = 30
    stage_provider_call_timeout_seconds: int = 240
    # A same-provider tier fallback is useful for fast upstream failures, but it
    # must not consume the independent-provider recovery window. The primary
    # keeps the full call cap; a later route on that same provider is bounded by
    # this smaller cap. Runtime clamps it to the main provider-call cap too, so
    # an existing deployment that lowers only STAGE_PROVIDER_CALL_TIMEOUT_SECONDS
    # does not fail to boot when this new setting is introduced.
    stage_same_provider_fallback_timeout_seconds: int = 120
    stage_provider_idle_timeout_seconds: int = 240
    stage_retry_min_remaining_seconds: int = 120
    stage_generation_cancel_poll_seconds: float = 1.0
    stage_generation_cancel_db_poll_seconds: float = 5.0
    # External technology lifecycle/advisory lookups run after the draft is
    # committed. They are bounded independently and a stale pending result is
    # converted to an advisory so finalisation can never remain blocked.
    stage_technology_check_timeout_seconds: int = 10
    stage_technology_check_stale_seconds: int = 30

    @model_validator(mode="after")
    def validate_stage_generation_bounds(self) -> "Settings":
        if not 300 <= self.stage_generation_deadline_seconds <= 3600:
            raise ValueError(
                "STAGE_GENERATION_DEADLINE_SECONDS must be between 300 and 3600"
            )
        if not 10 <= self.stage_generation_finalise_reserve_seconds <= 120:
            raise ValueError(
                "STAGE_GENERATION_FINALISE_RESERVE_SECONDS must be between 10 and 120"
            )
        # The ceiling is the provider budget the deadline actually grants:
        # deadline - finalise reserve. Anything above it is a bound the run can
        # never reach, which is how a call cap silently becomes a lie. This is
        # the only ceiling. The configured attempt cap intentionally leaves a
        # retry window, but older environment values remain valid during a
        # rolling migration.
        provider_budget = (
            self.stage_generation_deadline_seconds
            - self.stage_generation_finalise_reserve_seconds
        )
        if not 1 <= self.stage_provider_call_timeout_seconds <= provider_budget:
            raise ValueError(
                f"STAGE_PROVIDER_CALL_TIMEOUT_SECONDS must be 1..{provider_budget}"
            )
        if (
            not 1
            <= self.stage_same_provider_fallback_timeout_seconds
            <= provider_budget
        ):
            raise ValueError(
                "STAGE_SAME_PROVIDER_FALLBACK_TIMEOUT_SECONDS must be "
                f"1..{provider_budget}"
            )
        if not 1 <= self.stage_provider_idle_timeout_seconds <= provider_budget:
            raise ValueError(
                f"STAGE_PROVIDER_IDLE_TIMEOUT_SECONDS must be 1..{provider_budget}"
            )
        if (
            self.stage_provider_idle_timeout_seconds
            > self.stage_provider_call_timeout_seconds
        ):
            raise ValueError(
                "STAGE_PROVIDER_IDLE_TIMEOUT_SECONDS cannot exceed the call timeout"
            )
        if self.stage_retry_min_remaining_seconds < 45:
            raise ValueError("STAGE_RETRY_MIN_REMAINING_SECONDS must be at least 45")
        if self.stage_retry_min_remaining_seconds >= provider_budget:
            raise ValueError(
                "STAGE_RETRY_MIN_REMAINING_SECONDS must be smaller than the "
                "generation provider budget"
            )
        if not 1 <= self.max_concurrent_plan_chunks_per_generation <= 4:
            raise ValueError(
                "MAX_CONCURRENT_PLAN_CHUNKS_PER_GENERATION must be between 1 and 4"
            )
        if not 1 <= self.stage_technology_check_timeout_seconds <= 10:
            raise ValueError("STAGE_TECHNOLOGY_CHECK_TIMEOUT_SECONDS must be 1..10")
        if not 1 <= self.stage_technology_check_stale_seconds <= 30:
            raise ValueError("STAGE_TECHNOLOGY_CHECK_STALE_SECONDS must be 1..30")
        if self.stage_generation_cancel_poll_seconds <= 0:
            raise ValueError("STAGE_GENERATION_CANCEL_POLL_SECONDS must be positive")
        if self.stage_generation_cancel_db_poll_seconds <= 0:
            raise ValueError("STAGE_GENERATION_CANCEL_DB_POLL_SECONDS must be positive")
        if self.max_concurrent_provider_calls_per_process < 1:
            raise ValueError(
                "MAX_CONCURRENT_PROVIDER_CALLS_PER_PROCESS must be at least 1"
            )
        return self

    # Phase 0 (issue #26) LLM cost ledger: persist one llm_cost_events row per
    # provider call. Fire-and-forget and fully exception-swallowed, but kept
    # behind a flag so a test/CI environment without a DB never even attempts
    # the write.
    llm_cost_ledger_enabled: bool = True
    # Phase 2 (issue #26): add cache_control hints to the system-prompt block for
    # providers that support explicit prompt caching (Anthropic cache_control).
    # Default True — quality-neutral (same content, same model; only billing and
    # latency change) and the break-even is low for chunked/multi-round paths.
    # Flip False to disable the structured block without a redeploy.
    llm_prompt_cache_enabled: bool = True
    # OpenAI's normal in-memory prompt cache is the privacy-safe launch default.
    # ``24h`` opts into extended retention and must be approved against the
    # deployment's data-retention policy before use.
    openai_prompt_cache_retention: str = "memory"

    @field_validator("openai_prompt_cache_retention")
    @classmethod
    def validate_openai_prompt_cache_retention(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"memory", "24h"}:
            raise ValueError("must be 'memory' or '24h'")
        return normalized

    # Phase 3 (issue #26): submit non-interactive judge/eval calls (eval.score)
    # through the provider Message Batches API for the 50% batch discount,
    # driven on the arq worker (submit → checkpoint batch id → cron poll →
    # collect). Default False — the feature ships behind a flag with automatic
    # fallback to the synchronous in-process path when off, when the provider
    # has no real batch API (only Anthropic today), or when the durable queue is
    # unavailable. Never batches interactive generation or the critic.
    llm_batch_enabled: bool = False
    # Issue #27 Phase 2: fraction of non-harness generations (spec/plan/tasks)
    # whose best-effort LLM *quality score* is computed. The score is no longer a
    # user-facing signal (Phase 1 cut it from the UI in favour of deterministic
    # findings), so it is sampled purely for internal telemetry. Default 0.0 ⇒ the
    # score-only judge call is never issued for those stages, which is the cost
    # win — deterministic findings (task traceability, completeness) still run
    # inline on every generation regardless. HARNESS stages are exempt from this
    # gate: their LLM-derived coverage finding (`coverage_percent`/`uncovered_reqs`)
    # has no deterministic equivalent and must stay visible (Decision A), so the
    # judge always runs there. The single gate lives in `_dispatch_stage_eval`.
    # Raise toward 1.0 only to gather model/provider quality telemetry. Must be in
    # [0.0, 1.0]; an out-of-range value fails startup in every environment.
    eval_score_sample_rate: float = 0.0
    # Issue #21 Phase 2b — honest, data-backed generation ETA. A cheap periodic
    # worker cron rolls llm_cost_events latency_ms up into aggregate p50/p90 per
    # (provider, stage, operation), caches the result in Redis, and the read-only
    # GET /stages/generation-estimates endpoint serves it from cache (the heavy
    # query never runs per request). The frontend prefers these live percentiles
    # and falls back to its constant heuristic table on any miss/empty/low-sample
    # response, so the feature can never degrade the UX below the 2a baseline.
    # Flip enabled False to stop refreshing (the endpoint then serves empty and
    # every client falls back to the heuristic).
    generation_estimates_enabled: bool = True
    # Trailing window the rollup samples. Shorter tracks the current cheap-tier
    # models' latency; long enough to accumulate volume per (provider, stage).
    generation_estimates_window_days: int = 14
    # A (provider, stage, operation) key is only served once it has at least this
    # many samples — below it the client keeps the heuristic baseline.
    generation_estimates_min_samples: int = 50
    # Redis TTL for the cached rollup. The cron recomputes more often than this
    # (see worker.py) so the key never expires while the worker is healthy; if the
    # worker is down past the TTL the key lapses and clients fall back cleanly.
    generation_estimates_cache_ttl_seconds: int = 900
    # latency_ms measures the provider stream only; the post-stream pipeline tail
    # (artifact validator → critic judge → persistence) adds a few seconds of
    # perceived time the stream timer never sees. Added to the served p50/p90 so
    # the band reflects perceived wall-clock, not stream duration (keeps the
    # "still working" flip from firing early on the slow end of normal runs).
    generation_estimates_pipeline_tail_seconds: int = 4
    max_request_body_bytes: int = 1_000_000
    tech_safety_policy_max_age_days: int = 30
    tech_safety_osv_cache_ttl_seconds: int = 86_400
    tech_safety_eol_cache_ttl_seconds: int = 604_800
    tech_safety_advisory_timeout_seconds: float = 3.0
    tech_safety_osv_api_base: str = "https://api.osv.dev"
    tech_safety_eol_api_base: str = "https://endoflife.date/api/v1"
    tech_safety_blocked_severities: str = "critical,high,unknown"

    # GitHub OAuth App — leave blank to disable GitHub export
    github_client_id: str = ""
    github_client_secret: str = ""

    # GitHub App (Phase 21 living integration) — leave blank to disable.
    # github_app_id is GitHub's numeric App id used as the App-JWT `iss`.
    # github_app_private_key is the RS256 PEM that signs the App JWT; it is a
    # secret-manager value and is never persisted to the database (spec §8).
    # github_app_slug is the App's public slug, used to build the installation
    # URL (https://github.com/apps/{slug}/installations/new). Leave blank to
    # disable the App install flow (the Phase-13 OAuth path remains available).
    github_app_id: str = ""
    github_app_private_key: str = ""
    github_app_slug: str = ""
    # Webhook HMAC signing secrets. Two are accepted so a secret rotation does
    # not drop in-flight deliveries: every inbound signature is checked against
    # the current secret and, if set, the previous one (spec §8/§12).
    github_app_webhook_secret: str = ""
    github_app_webhook_secret_prev: str = ""
    # Identity OAuth (user-to-server) for the App. This is the credential that
    # proves the person completing the install callback actually administers the
    # installation's account, closing the install-callback IDOR (GitHub
    # integration audit #1): without it the setup callback would (re)bind an
    # attacker-supplied ``installation_id`` to the caller with no proof of
    # control. It is therefore **required** when the App is enabled in production
    # (see ``validate_production_settings``).
    github_app_client_id: str = ""
    github_app_client_secret: str = ""

    @property
    def github_app_identity_enabled(self) -> bool:
        """True when the App's user-to-server identity OAuth is configured.

        Both the client id and secret are required to exchange the install
        callback's ``code`` for a user token and verify the installer
        administers the installation (audit #1). The single source of truth for
        "can we verify an installer", mirrored by
        ``github_install_service.app_identity_enabled``.
        """
        return bool(self.github_app_client_id and self.github_app_client_secret)

    @property
    def github_app_enabled(self) -> bool:
        """True when the GitHub App is configured (its identity is present).

        The single source of truth for "is the App on": the App cannot mint a JWT
        or build an install URL without both the numeric id and the public slug,
        so those two define "enabled" (matching ``github_install_service``). When
        enabled in production, ``validate_production_settings`` requires the
        signing key + webhook secret too.
        """
        return bool(self.github_app_id and self.github_app_slug)

    @property
    def github_app_webhook_secrets(self) -> list[str]:
        """The non-empty webhook signing secrets, current first (for rotation).

        Passed to ``verify_hmac`` so an inbound signature is accepted against the
        current secret and, during a rotation window, the previous one.
        """
        return [
            s
            for s in (
                self.github_app_webhook_secret,
                self.github_app_webhook_secret_prev,
            )
            if s
        ]

    # Phase 5.3 (issue #26) + issue #17 follow-up: master switch for the
    # product-wide cheap-primary generation policy (Haiku 4.5 / GPT-5.4 Mini
    # start, mid-tier escalation on a runtime/quality-gate failure).  Defaults
    # **True** (cheap-first): running every core generation on the mid tier
    # (Sonnet 4.6 / GPT-5.4) is too expensive to sustain at production volume —
    # it takes an unacceptable hit on per-generation margins — so **every**
    # artifact-generation feature — the four core stages, full regenerate, the
    # harness gap-patch, the storyboard keynote, and increment generation —
    # starts on the provider's cheapest viable tier and only escalates to mid on
    # a runtime/quality-gate failure, via the shared
    # ``services.llm.tier_policy.generation_tier_policy``.  Set False to revert
    # **all** of those features to the pre-cheap-swap mid-first default in one
    # toggle (mid start, strong escalation) if cheap-tier quality regresses.
    core_cheap_primary: bool = True

    # Stage latency lever (docs/STAGE_LATENCY_PLAN.md): when enabled, only the
    # primary stage-generation adapter requests use low reasoning/thinking on the
    # cheap primary model. Other uses of the same model (judge/eval, refine,
    # storyboard, increment, critic regenerate) keep the catalog default because
    # their call sites do not opt into operation-aware adapter policy.
    core_generation_low_reasoning: bool = True

    # Phase 5.2 (issue #26): deterministic, no-LLM complexity classifier that
    # raises the *starting* tier for predictably hard core generations (regulated
    # domains, large upstream chains, prior quality-gate failures) above the cheap
    # primary — so a request that would burn the cheap attempt + funded regenerate
    # starts on a capable model instead.  It is a floor, never a ceiling (it can
    # only raise a tier, never lower it) and only applies while `core_cheap_primary`
    # is on.  Default False — per issue AC, adaptive routing ships behind a flag
    # and is enabled only after the golden-corpus live gate validates it
    # (`docs/evals/ROUTE_PROMOTION.md`).
    core_complexity_routing: bool = False

    # Storyboard quality escape hatch (storyboard output-quality plan P3.5). When
    # True, storyboard generation starts on the provider's `mid` tier escalating
    # to `strong`, bypassing the product-wide cheap-primary policy for THIS feature
    # only (`_resolve_storyboard_primary_route`). Flip it if the deterministic deck
    # quality gate (`storyboard_quality.assess_payload_quality`) shows the cheap
    # tier keeps failing and the escalation rate stays high — one flag, no drift of
    # `services.llm.tier_policy` for any other feature. Default False: cheap primary
    # with a one-shot quality-triggered escalation is the intended steady state.
    storyboard_force_mid_tier: bool = False

    # Problem-statement compression (docs/PROBLEM_STATEMENT_COMPRESSION_PLAN.md,
    # Phase B). When True, an over-budget problem statement is reduced to at most
    # `problem_statement_budget_tokens` (C_MAX) before any model sees it, via the
    # zero-LLM ladder in `services/pipeline/problem_compressor.py` (Rung 0 no-op →
    # Rung 1 lossless structural cleanup → Rung 3 deterministic normative-first
    # clamp). It is the *other half* of the raised input cap (Phase A): input is
    # accepted big and fed small, so per-call cost/latency stay bounded regardless
    # of input size, and no call can blow the model window. Default **False** — the
    # under-threshold common case is a byte-identical no-op, so flipping it changes
    # nothing for the vast majority of inputs; the Rung-0 regression pin and the
    # golden corpus gated the rollout. **Enabled by default in Phase D** (the
    # deterministic ladder is zero-LLM-cost and bounded; only genuinely over-budget
    # pastes condense). When a paste does condense (Rung 2/3), the user is told via
    # a non-blocking advisory notice on the generated stage (`AdvisoryFindingsPanel`).
    # The Rung-2 *abstractive* (paid, meaning-preserving) pass remains a separate,
    # default-off sub-gate (`problem_statement_abstractive`) below.
    problem_statement_compression: bool = True

    # C_MAX — the product token budget compression targets and triggers on
    # (THRESHOLD ≈ C_MAX). A *chosen* small constant set far below the model window
    # (200K–1M tokens): the win is sending less, not fitting the window. Pre-Phase-A
    # inputs (≤10K chars ≈ 2.5K tokens) sit below this and never compress; only the
    # new large pastes (up to the 50K-char `PROBLEM_STATEMENT_MAX_CHARS`) trip it.
    # One constant across providers keeps the cache key and golden corpus
    # deterministic (plan §10). The effective budget is `min(this, WINDOW_FIT_CEILING)`
    # so it can never exceed what the window physically allows.
    problem_statement_budget_tokens: int = 8000

    # Rung 2 — the meaning-preserving *abstractive* pass (Phase C). This is a
    # **sub-gate** of `problem_statement_compression`: it has no effect unless the
    # master flag is also on. When both are on, an over-budget statement that the
    # deterministic ladder would otherwise hand to the Rung-3 clamp is instead
    # routed through a capped map-reduce over the cheap judge model that keeps
    # normative content (requirement IDs, must/shall, lists, tables) verbatim and
    # condenses only the narrative prose, falling open to the Rung-3 floor on any
    # judge error/timeout or when there is no narrative room. Default **False**:
    # like `core_complexity_routing`, the paid LLM layer ships off and is promoted
    # only after the normative-retention + semantic-equivalence gate
    # (docs/evals/PROBLEM_COMPRESSION_PROMOTION.md). Phase B's zero-cost
    # reliability never depends on this flag.
    problem_statement_abstractive: bool = False

    # Overall wall-clock budget for the Rung-2 abstractive pass (all map + reduce
    # judge calls combined). On expiry the pass fails open to the Rung-3 floor, so
    # this caps the added latency, not correctness (plan §9 target: < 3s p95).
    problem_statement_abstractive_timeout_seconds: float = 8.0

    # Demo Day mode (docs/DEMO_DAY_MODE_IMPLEMENTATION_PLAN.md). Master server-side
    # gate for the whole feature: a generation profile producing Demo-Day-shaped
    # artifacts + a construction verifier + an agent operating manual in the
    # export. Default **False** — when off, `workspace.mode` is forced to
    # 'standard' at creation (workspace_service.create), so every prompt, section
    # contract, completeness floor, export bundle, and API response is
    # byte-identical to today (the §4 regression-pin contract). Flip True (with the
    # frontend's VITE_DEMO_DAY_MODE flag) after the golden-corpus live gate.
    demo_day_mode_enabled: bool = False

    # Construction-verifier enforcement. Both flags decide only whether a check
    # can flip `verified` — the checks always run, and their gaps always render in
    # CONSTRUCTION_REPORT.md and the UI. Shipping them un-enforced is what lets a
    # new check be measured against real generations before it can turn an
    # already-green package red: the checks assert `Plan refs`/`Harness refs`
    # citations that the tasks prompts do not yet mandate, and those prompt edits
    # ride a golden-corpus gate. Flip these (env-only, no redeploy) once the
    # matching prompt release has cleared that gate.
    #
    # demo_day_plan_coverage_enforced — Demo Day C6 (plan_coverage): every
    # load-bearing plan section (seed data, bootstrap/demo surface, external
    # integrations, security architecture) is cited by ≥1 task's `Plan refs`.
    demo_day_plan_coverage_enforced: bool = False
    # standard_construction_verifier_enforced — the whole standard-mode verdict
    # (requirement coverage, test coverage, acyclic DAG, plan coverage, e2e
    # reachability). Off ⇒ computed, persisted and displayed with `verified` true.
    standard_construction_verifier_enforced: bool = False

    # Increment generation (Phase 21 — T-279). The MVP ships the *additive* path
    # only: an increment appends new tasks with their existing content pinned by
    # stable, content-derived task_refs. Behaviour-changing increments (compute
    # blast radius, mark affected items stale, re-run harness/critic only on the
    # affected areas) are the phase-two cut and stay gated off until that work
    # lands; flip this to true only once the blast-radius path is implemented.
    increment_blast_radius_enabled: bool = False

    # Lemon Squeezy billing (Phase 22) — the provider-neutral checkout that
    # supersedes Stripe at runtime. Leave the api key / store id / variant id
    # blank to ship with checkout DISABLED (GET /billing/package still works;
    # POST /billing/checkout returns 503). When all three are set the integration
    # is "enabled" (``lemonsqueezy_enabled``) and, in production,
    # ``validate_production_settings`` requires a complete LIVE config. There is
    # deliberately no ``lemonsqueezy_cancel_url`` (Plan §25.6 T-292).
    lemonsqueezy_api_key: str = ""
    # HMAC signing secrets for the X-Signature webhook header. Two are accepted so
    # a secret rotation does not drop in-flight deliveries (current + previous).
    lemonsqueezy_webhook_secret: str = ""
    lemonsqueezy_webhook_secret_prev: str = ""
    lemonsqueezy_store_id: str = ""
    lemonsqueezy_variant_id: str = ""
    lemonsqueezy_price_cents: int = 1500  # $15.00 — 200 credits per purchase
    lemonsqueezy_currency: str = "USD"
    lemonsqueezy_credits_per_purchase: int = 200
    lemonsqueezy_credit_validity_days: int = 30
    lemonsqueezy_success_url: str = ""  # e.g. https://app.thought2build.com/billing
    # test_mode gates whether checkouts are created against Lemon's test store.
    # Production must run with this False (the production guard enforces it).
    lemonsqueezy_test_mode: bool = True
    # How long a created checkout attempt stays pollable before it is swept to
    # 'expired' by the retention job (T-298).
    lemonsqueezy_checkout_ttl_minutes: int = 30
    lemonsqueezy_api_base: str = "https://api.lemonsqueezy.com"
    # Upper bound on provider API calls per reconcile run (bounds lane 2, T-301).
    lemonsqueezy_reconcile_max_calls_per_run: int = 200

    # New checkout uses Razorpay only and ships disabled. The switch gates new
    # checkout, while signed events and recovery continue settling existing money.
    # Legacy provider credentials are retained only for historical settlement.
    payments_enabled: bool = False
    payment_provider: str = "razorpay"

    # Razorpay billing via hosted Payment Links (async HTTP, no SDK).
    # Leave the key id / key secret blank to keep it unconfigured
    # (``razorpay_enabled`` false). When configured — active or not, because
    # its webhook route always processes — production requires a complete LIVE
    # config: webhook secret, HTTPS success URL, positive economics, a live
    # key (rzp_live_…), and a checkout TTL of at least 16 minutes.
    razorpay_key_id: str = ""  # rzp_test_… / rzp_live_…
    razorpay_key_secret: str = ""
    # HMAC signing secrets for the X-Razorpay-Signature webhook header. Two are
    # accepted so a secret rotation does not drop in-flight deliveries.
    razorpay_webhook_secret: str = ""
    razorpay_webhook_secret_prev: str = ""
    # Amounts are integer minor units (paise for INR): ₹1549.00 = 154900. The
    # existing price_cents columns carry these unchanged (plan D8).
    #
    # This is NOT a naive currency conversion of lemonsqueezy_price_cents.
    # Razorpay is not a Merchant of Record, so 18% GST comes out of this gross
    # amount on our side (Lemon absorbs tax on the USD path). ₹1549 gross nets
    # ~₹1313 ≈ $14.9 after GST — actual parity with the $15.00 Lemon price.
    # Re-derive the gross, not the net, if the USD price changes.
    razorpay_price_cents: int = 154900
    razorpay_currency: str = "INR"
    razorpay_credits_per_purchase: int = 200
    razorpay_credit_validity_days: int = 30
    razorpay_success_url: str = ""  # e.g. https://app.thought2build.com/billing
    # Payment-link expire_by horizon. Razorpay rejects expiries under ~15
    # minutes, so the production guard enforces >= 16.
    razorpay_checkout_ttl_minutes: int = 30
    razorpay_api_base: str = "https://api.razorpay.com"
    # Upper bound on provider API calls per reconcile run (bounds lane 2).
    razorpay_reconcile_max_calls_per_run: int = 200
    razorpay_recovery_attempts_per_run: int = 50
    razorpay_reconcile_lookback_days: int = 180

    # Comma-separated allowlist of admin emails authorised to issue billing admin
    # corrections (T-302). The codebase has no role column, so this allowlist is
    # the ONLY admin authorization surface — an empty value means no admin exists
    # and the correction endpoint 403s for everyone (closed by default).
    admin_user_emails: str = ""

    # Brave LLM Context API research enrichment (issue #12) — mirrors the
    # langfuse/lemonsqueezy optional-integration shape. The feature is a purely
    # *additive*, fail-open web-grounding layer: when it is off, unconfigured, or
    # failing at runtime, generation proceeds identically to today on an empty
    # research block. Leaving brave_search_api_key empty disables the feature with
    # zero network traffic; brave_search_flag is a second, explicit kill-switch so
    # the integration can be flipped on/off without rotating the key. Both must be
    # set for ``brave_search_enabled`` to be true (and even then every actual call
    # is further gated per-workspace + per-stage + per-credit; see the plan §3).
    brave_search_api_key: str = ""
    brave_search_flag: bool = False
    # Stages that benefit from grounding (spec/plan); 'tasks' is pure decomposition
    # of upstream artifacts and is intentionally excluded by default.
    brave_research_stages: str = "spec,plan"
    # Hard per-fetch latency budget — far tighter than Brave's 30s suggestion so a
    # slow third party can add at most this many seconds to a generation (and 0s
    # when cached or disabled).
    brave_timeout_seconds: float = 4.0
    # Redis cache TTL for a fetched research block; keeps regenerate loops and
    # bursty traffic off the per-query meter. 6h is a freshness-vs-cost tradeoff.
    brave_cache_ttl_seconds: int = 21600
    # Brave context size control (contract range 1024–32768).
    brave_max_tokens: int = 8192
    # Upper bound on the injected research block so it cannot crowd out upstream
    # deps or blow the context window.
    brave_max_context_chars: int = 12000
    # Per-workspace daily call ceiling enforced in Redis; protects the monthly
    # free/paid quota from runaway loops. Over-ceiling fails open to no research.
    brave_max_calls_per_workspace_per_day: int = 20
    # Freshness bias toward recent best-practices (pd|pw|pm|py or a date range).
    brave_freshness: str = "py"
    # Credits charged per *successful paid* Brave fetch (cache hits, failures,
    # timeouts, empty results, and the disabled/not-opted-in paths cost nothing).
    # Provisional placeholder — final price (§13 open question) is set from the
    # $5/1k Brave cost plus margin and confirmed with billing before launch.
    billing_credits_brave_research: int = 1
    # COGS (Phase 4): platform USD cost of one paid Brave call, recorded on an
    # ``llm_cost_events`` row (provider="brave") per paid fetch so platform spend
    # and the user-facing credit debit reconcile. Default = Brave Search plan
    # $5 / 1000 requests = $0.005 / request; revisit if we move to the
    # token-metered AI-grounding plan (§13 open question).
    brave_cost_usd_per_call: float = 0.005

    # --- Data retention & purging (issue #43; docs/RETENTION_IMPLEMENTATION_PLAN.md).
    #
    # Steady-state DB size proportional to the *active* corpus, not lifetime
    # history. Two keys must turn before ANYTHING deletes: the master enable AND
    # dry_run=False. Per-tier flags then gate each purge job separately so the
    # tiers roll out independently (plan §8). Everything is additive and reversible
    # by flipping a flag — no API surface changes in Phases 0–2.
    #
    #   retention_enabled: master kill-switch for the whole feature — counting,
    #     purging, AND the Phase-0 table-stats sampler. False ⇒ every retention job
    #     and the sampler are no-ops (the instant, total backout).
    #   retention_dry_run: True ⇒ every purge job only COUNTS candidates (feeds the
    #     gauge + audit log) and deletes nothing, regardless of the tier flags.
    #   retention_tierN_purge_enabled: per-tier delete gate. A job deletes only when
    #     retention_enabled AND NOT retention_dry_run AND its tier flag is on; with
    #     the flag off it still counts (so the gauge/backlog alert stay live).
    retention_enabled: bool = True
    retention_dry_run: bool = True
    retention_tier1_purge_enabled: bool = False
    retention_tier2_purge_enabled: bool = False
    retention_tier3_purge_enabled: bool = False
    # Bounded work per batch and per daily run so a single cron tick can never run
    # unboundedly and a backlog drains over several days instead of one lock storm.
    retention_purge_batch_size: int = 1000
    retention_max_rows_per_run: int = 50_000
    # Tier-1 telemetry TTL windows (days). The 180 d cost-events window sits far
    # above the 28 d output-budget analysis lookback (issue #26) so tightening a
    # budget never outruns its evidence.
    retention_cost_events_days: int = 180
    retention_eval_results_days: int = 180
    retention_failed_batch_jobs_days: int = 30
    retention_failed_pushes_days: int = 90
    # Tier-2 content keep-N (the byte win). Keep the last N versions/storyboards and
    # prune the rest only when older than the min-age floor. N is a product policy
    # knob (it truncates visible version/diff history beyond N) — plan §4.1/§8.
    retention_stage_versions_keep: int = 20
    retention_stage_versions_min_age_days: int = 90
    retention_storyboards_keep: int = 5
    retention_storyboards_min_age_days: int = 90
    # Tier-3 workspace trash windows (days). A workspace the user explicitly deleted
    # with a recorded acknowledgment is hard-deleted retention_trash_days after the
    # delete; a legacy / un-acked archived row waits the conservative legacy window.
    retention_trash_days: int = 30
    retention_legacy_archived_days: int = 180
    # Phase-0 table-size sampler (plan §6). The size baseline every later "size
    # stabilized" claim is judged against; also gated by retention_enabled (§8).
    retention_table_stats_enabled: bool = True

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @field_validator("eval_score_sample_rate")
    @classmethod
    def _validate_eval_score_sample_rate(cls, value: float) -> float:
        """Reject a sample rate outside [0.0, 1.0] (issue #27 Phase 2).

        This is a probability, not a count — a value below 0 or above 1 is a
        misconfiguration in any environment, so it fails fast at startup rather
        than silently clamping and quietly under/over-sampling the judge.
        """
        if not 0.0 <= value <= 1.0:
            raise ValueError(
                "eval_score_sample_rate must be between 0.0 and 1.0 " f"(got {value})"
            )
        return value

    @property
    def lemonsqueezy_enabled(self) -> bool:
        """True when Lemon Squeezy checkout is configured.

        The single source of truth for "is Lemon billing on": a checkout cannot be
        minted without the API key, the store, and the variant, so those three
        define "enabled". When enabled in production,
        ``validate_production_settings`` additionally requires a complete LIVE
        config (webhook secret, HTTPS success URL, live mode, …).
        """
        return bool(
            self.lemonsqueezy_api_key
            and self.lemonsqueezy_store_id
            and self.lemonsqueezy_variant_id
        )

    @property
    def lemonsqueezy_webhook_secrets(self) -> tuple[str, ...]:
        """The non-empty webhook signing secrets, current first (for rotation).

        Passed to the inbound HMAC verifier so a signature is accepted against the
        current secret and, during a rotation window, the previous one.
        """
        return tuple(
            s
            for s in (
                self.lemonsqueezy_webhook_secret,
                self.lemonsqueezy_webhook_secret_prev,
            )
            if s
        )

    @property
    def razorpay_enabled(self) -> bool:
        """True when Razorpay checkout is configured (issue #44).

        The Razorpay analogue of ``lemonsqueezy_enabled``: a payment link
        cannot be minted without both halves of the Basic-auth pair, so key id
        + key secret define "configured". When configured in production —
        active or not, because the webhook route always processes —
        ``validate_production_settings`` additionally requires a complete LIVE
        config (webhook secret, HTTPS success URL, live-key prefix, …).
        """
        return bool(self.razorpay_key_id and self.razorpay_key_secret)

    @property
    def razorpay_webhook_secrets(self) -> tuple[str, ...]:
        """The non-empty Razorpay webhook secrets, current first (for rotation).

        Passed to the inbound HMAC verifier so a signature is accepted against
        the current secret and, during a rotation window, the previous one.
        """
        return tuple(
            s
            for s in (
                self.razorpay_webhook_secret,
                self.razorpay_webhook_secret_prev,
            )
            if s
        )

    @property
    def billing_checkout_enabled(self) -> bool:
        """THE gate for checkout creation and package display (issue #44).

        True only when the master ``payments_enabled`` switch is on AND the
        active ``payment_provider`` is fully configured. An unknown provider
        value fails closed here — it must never resolve to some other
        provider's checkout — and fails startup loudly in production (see
        ``validate_production_settings``). This gates POST /billing/checkout
        and the ``enabled`` field on GET /billing/package only; webhook
        processing for both providers is deliberately NOT gated by it.
        """
        if not self.payments_enabled:
            return False
        if self.payment_provider == "razorpay":
            return self.razorpay_enabled
        return False

    @property
    def admin_emails(self) -> set[str]:
        """The parsed, lower-cased billing-admin allowlist (empty when unset).

        An empty set authorises no one — the admin-correction support path (T-302)
        is closed by default and there is no implicit admin.
        """
        return {
            email.strip().lower()
            for email in self.admin_user_emails.split(",")
            if email.strip()
        }

    @property
    def allowed_hosts_list(self) -> list[str]:
        """Parsed Host allowlist for TrustedHostMiddleware (empty when unset)."""
        return [h.strip() for h in self.allowed_hosts.split(",") if h.strip()]

    @property
    def csrf_fail_closed_prefix_tuple(self) -> tuple[str, ...]:
        """Path prefixes where CSRF replay protection fails closed on Redis loss."""
        return tuple(
            p.strip() for p in self.csrf_fail_closed_prefixes.split(",") if p.strip()
        )

    @property
    def brave_search_enabled(self) -> bool:
        """True only when Brave research is fully configured AND switched on.

        The single source of truth for "is Brave on": the integration needs a
        non-empty subscription key *and* the explicit ``brave_search_flag``
        kill-switch flipped on. An empty key alone keeps it off (no traffic);
        the flag lets ops disable it without rotating the key. Note this property
        already folds in the key, so the production guard keys on
        ``brave_search_flag`` (the flag-on/key-missing misconfiguration) rather
        than on this property, which can never be true with an empty key.
        """
        return bool(self.brave_search_api_key) and self.brave_search_flag

    @property
    def brave_research_stage_set(self) -> frozenset[str]:
        """The parsed set of stage types eligible for Brave enrichment.

        Empty when ``brave_research_stages`` is blank, which disables enrichment
        for every stage even with the flag and key set.
        """
        return frozenset(
            stage.strip().lower()
            for stage in self.brave_research_stages.split(",")
            if stage.strip()
        )


settings = Settings()


_CI_ENCRYPTION_KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


def validate_production_settings() -> None:
    if settings.environment.lower() != "production":
        return

    errors = []
    if not settings.metrics_token:
        errors.append("METRICS_TOKEN must be set in production")
    if not settings.frontend_url.startswith("https://"):
        errors.append("FRONTEND_URL must use HTTPS in production")
    if not settings.allowed_hosts_list:
        errors.append(
            "ALLOWED_HOSTS must be set in production (comma-separated Host "
            "allowlist enforced by TrustedHostMiddleware). Include the app's "
            "public host and the Railway internal healthcheck host; a "
            "leading-dot entry matches subdomains."
        )
    # Marketing zone canonical origin (issue #18, Phase 7). HTTPS-when-set: an
    # http:// SITE_URL would emit insecure canonical/OG/sitemap URLs and leak the
    # marketing origin over plaintext. Empty is allowed (the backend has no
    # consumer yet); only a configured-but-insecure value is a misconfiguration.
    if settings.site_url and not settings.site_url.startswith("https://"):
        errors.append("SITE_URL must use HTTPS in production when set")
    if not settings.jwt_private_key.strip().startswith("-----BEGIN"):
        errors.append(
            "JWT_PRIVATE_KEY must be a PEM-encoded RSA or EC private key "
            "(must start with '-----BEGIN'). "
            "The CI stub value is not valid for production."
        )
    if settings.encryption_master_key == _CI_ENCRYPTION_KEY:
        errors.append(
            "ENCRYPTION_MASTER_KEY is set to the known CI placeholder value. "
            "Generate a real key: "
            'python -c "from cryptography.fernet import '
            'Fernet; print(Fernet.generate_key().decode())"'
        )
    if settings.langfuse_secret_key.strip():
        if not settings.langfuse_public_key.strip():
            errors.append(
                "LANGFUSE_PUBLIC_KEY must be set when LANGFUSE_SECRET_KEY is set"
            )
        if not settings.langfuse_host.lower().startswith("https://"):
            errors.append(
                "LANGFUSE_HOST must use HTTPS in production. Plaintext HTTP "
                "exposes the Langfuse public/secret keys, full prompts, and "
                "full model outputs to anyone on the network path. Use "
                "https://cloud.langfuse.com or an HTTPS self-hosted endpoint."
            )
        if not settings.langfuse_content_capture_ack:
            errors.append(
                "LANGFUSE_CONTENT_CAPTURE_ACK must be true when enabling "
                "Langfuse in production. Langfuse receives prompt and model "
                "output content after secret-shaped redaction; only enable it "
                "after approving that telemetry data flow."
            )

    # Lemon Squeezy production guard (Phase 22 — T-292 / SR7). When Lemon is
    # enabled (api key + store + variant all set), production must run a complete
    # LIVE config or the checkout/webhook paths fail at runtime instead of at
    # startup. The api key, store id, and variant id are already guaranteed
    # non-empty by ``lemonsqueezy_enabled``, so they are not re-checked here; a
    # half-configured Lemon (missing one of those three) is "disabled" and
    # intentionally fails to-disabled (checkout 503s, package/history still work).
    if settings.lemonsqueezy_enabled:
        if not settings.lemonsqueezy_webhook_secret.strip():
            errors.append(
                "LEMONSQUEEZY_WEBHOOK_SECRET must be set when Lemon Squeezy "
                "billing is enabled — inbound webhooks (the sole credit-grant "
                "authority) cannot be signature-verified without it."
            )
        if not settings.lemonsqueezy_success_url.lower().startswith("https://"):
            errors.append("LEMONSQUEEZY_SUCCESS_URL must use HTTPS in production.")
        if settings.lemonsqueezy_price_cents <= 0:
            errors.append("LEMONSQUEEZY_PRICE_CENTS must be a positive integer.")
        if settings.lemonsqueezy_credits_per_purchase <= 0:
            errors.append(
                "LEMONSQUEEZY_CREDITS_PER_PURCHASE must be a positive integer."
            )
        if settings.lemonsqueezy_credit_validity_days <= 0:
            errors.append(
                "LEMONSQUEEZY_CREDIT_VALIDITY_DAYS must be a positive integer."
            )
        if not settings.lemonsqueezy_currency.strip():
            errors.append("LEMONSQUEEZY_CURRENCY must be non-empty.")
        if settings.lemonsqueezy_test_mode is not False:
            errors.append(
                "LEMONSQUEEZY_TEST_MODE must be False in production "
                "(live mode required); a test-mode store charges nothing."
            )

    # Payment feature-flag guard (issue #44). The provider selector must name a
    # known gateway: a typo'd value fails closed at runtime
    # (billing_checkout_enabled → False), which would silently disable billing,
    # so production refuses to start instead. And when the master switch is on,
    # the ACTIVE provider must be fully configured — otherwise every checkout
    # 503s at runtime while payments read as enabled.
    if settings.payment_provider != "razorpay":
        errors.append(
            "PAYMENT_PROVIDER must be 'razorpay' for new purchases "
            f"(got {settings.payment_provider!r}); a typo would silently "
            "disable billing."
        )
    elif settings.payments_enabled and not settings.billing_checkout_enabled:
        errors.append(
            "PAYMENTS_ENABLED is true but the active provider "
            f"'{settings.payment_provider}' is not fully configured "
            "(its credentials are incomplete), so every checkout would fail "
            "at runtime. Configure the provider or set PAYMENTS_ENABLED=false."
        )

    # Razorpay production guard (issue #44). Applies whenever Razorpay is
    # CONFIGURED (key id + secret set), active or not: its webhook route
    # processes independently of the active-provider flag (refunds for old
    # orders must settle after a switch), so a configured-but-inactive
    # Razorpay still needs a verifiable webhook secret and sane economics.
    # The key id and secret are already guaranteed non-empty by
    # ``razorpay_enabled``, so they are not re-checked here; a half-configured
    # Razorpay (one of the two blank) is "disabled" and intentionally fails
    # to-disabled.
    if settings.razorpay_enabled:
        if settings.razorpay_recovery_attempts_per_run <= 0:
            errors.append("RAZORPAY_RECOVERY_ATTEMPTS_PER_RUN must be positive")
        if settings.razorpay_reconcile_max_calls_per_run <= 0:
            errors.append("RAZORPAY_RECONCILE_MAX_CALLS_PER_RUN must be positive")
        if settings.razorpay_reconcile_lookback_days < 30:
            errors.append("RAZORPAY_RECONCILE_LOOKBACK_DAYS must be at least 30")
        if not settings.razorpay_webhook_secret.strip():
            errors.append(
                "RAZORPAY_WEBHOOK_SECRET must be set when Razorpay billing is "
                "configured — inbound webhooks (the sole credit-grant "
                "authority) cannot be signature-verified without it."
            )
        if not settings.razorpay_success_url.lower().startswith("https://"):
            errors.append("RAZORPAY_SUCCESS_URL must use HTTPS in production.")
        if settings.razorpay_price_cents <= 0:
            errors.append("RAZORPAY_PRICE_CENTS must be a positive integer.")
        if settings.razorpay_credits_per_purchase <= 0:
            errors.append("RAZORPAY_CREDITS_PER_PURCHASE must be a positive integer.")
        if settings.razorpay_credit_validity_days <= 0:
            errors.append("RAZORPAY_CREDIT_VALIDITY_DAYS must be a positive integer.")
        if not settings.razorpay_currency.strip():
            errors.append("RAZORPAY_CURRENCY must be non-empty.")
        if not settings.razorpay_key_id.startswith("rzp_live_"):
            errors.append(
                "RAZORPAY_KEY_ID must be a live key (rzp_live_…) in "
                "production. Razorpay events carry no test-mode flag — the "
                "key prefix IS the environment (the LEMONSQUEEZY_TEST_MODE "
                "analogue); a test key charges nothing."
            )
        if settings.razorpay_checkout_ttl_minutes < 16:
            errors.append(
                "RAZORPAY_CHECKOUT_TTL_MINUTES must be at least 16: Razorpay "
                "rejects payment-link expire_by values under ~15 minutes."
            )

    # Stream-watchdog guard: an idle timeout below 30s kills healthy frontier
    # reasoning streams (they can think for tens of seconds between tokens);
    # a hard cap below the idle timeout makes every generation time out.
    if settings.llm_stream_idle_timeout_seconds < 30:
        errors.append(
            "LLM_STREAM_IDLE_TIMEOUT_SECONDS must be at least 30 in production; "
            "lower values kill healthy reasoning-model streams."
        )
    if settings.llm_stream_hard_cap_seconds < settings.llm_stream_idle_timeout_seconds:
        errors.append(
            "LLM_STREAM_HARD_CAP_SECONDS must be >= LLM_STREAM_IDLE_TIMEOUT_SECONDS."
        )

    # GitHub App guard (Phase 21 — T-283). When the App is enabled (id + slug
    # set), production must also have the signing key and webhook secret, or the
    # install/JWT/webhook paths fail at runtime instead of at startup. An empty
    # private key is rejected explicitly: without it no installation token can be
    # minted, so every App-backed GitHub write would silently 401-loop.
    if settings.github_app_enabled:
        if not settings.github_app_private_key.strip():
            errors.append(
                "GITHUB_APP_PRIVATE_KEY must be set when the GitHub App is "
                "enabled (GITHUB_APP_ID + GITHUB_APP_SLUG present). It is the "
                "RS256 PEM that signs the App JWT; without it no installation "
                "token can be minted."
            )
        if not settings.github_app_webhook_secret.strip():
            errors.append(
                "GITHUB_APP_WEBHOOK_SECRET must be set when the GitHub App is "
                "enabled, or inbound webhooks cannot be signature-verified."
            )
        # Identity OAuth is the install-callback's proof-of-control (audit #1).
        # Without it the setup callback cannot verify the installer administers
        # the installation, so it would refuse every bind and installs would
        # silently fail closed. Require it loudly at startup instead.
        if not settings.github_app_identity_enabled:
            errors.append(
                "GITHUB_APP_CLIENT_ID and GITHUB_APP_CLIENT_SECRET must be set "
                "when the GitHub App is enabled: they are the user-to-server "
                "identity OAuth credentials the install callback uses to verify "
                "the installer administers the installation (prevents the "
                "install-callback IDOR). Without them every install is refused."
            )

    # Data-retention guard (issue #43). Tier-3 hard-deletes whole workspaces, so
    # when its purge is enabled in production the two trash windows must be sane —
    # a fat-fingered 0/1-day window would delete workspaces users just trashed (or
    # legacy rows) before the restore/export window the UI advertises. The lower
    # bounds mirror the plan §3.1 floors (7 d acked, 90 d legacy).
    if settings.retention_tier3_purge_enabled:
        if settings.retention_trash_days < 7:
            errors.append(
                "RETENTION_TRASH_DAYS must be >= 7 in production when "
                "RETENTION_TIER3_PURGE_ENABLED is on: a shorter window would "
                "hard-delete trashed workspaces before the advertised "
                "restore/export window."
            )
        if settings.retention_legacy_archived_days < 90:
            errors.append(
                "RETENTION_LEGACY_ARCHIVED_DAYS must be >= 90 in production when "
                "RETENTION_TIER3_PURGE_ENABLED is on: the legacy/un-acked window "
                "must stay conservative for rows deleted before retention shipped."
            )

    # Brave research guard (issue #12). The feature is allowed *off* in prod (no
    # hard requirement), but turning the flag on without a key is a silent
    # misconfiguration: every fetch would 401 and fail open to no research, so
    # the flag would look enabled while doing nothing. We key on the flag (not
    # ``brave_search_enabled``, which already folds in the key and so can never be
    # true here with an empty key) to catch exactly that flag-on/key-missing case.
    if settings.brave_search_flag and not settings.brave_search_api_key.strip():
        errors.append(
            "BRAVE_SEARCH_API_KEY must be set when BRAVE_SEARCH_FLAG is true; "
            "otherwise every Brave fetch 401s and silently disables research "
            "while the flag reads as enabled."
        )

    # LLM provider-credential guard. Every generation resolves through
    # ``resolve_platform_route*``, which skips any provider whose key is blank or
    # ``placeholder-``-prefixed. If that skips *every* provider in
    # LLM_PROVIDER_PRIORITY the app still boots green and reports /health ok —
    # the failure surfaces only when a paying user's first generation 503s with
    # LLMRoutingError. Since commit 9e79190 made Anthropic the primary (rather
    # than fallback overflow) that is the default path, not an edge case.
    #
    # This deliberately requires ONE configured provider, not Anthropic
    # specifically: an OpenAI-only or Google-only deploy is a valid
    # configuration (the documented cheap-platform escape hatch is an env-only
    # LLM_PROVIDER_PRIORITY reorder) and must keep booting. ``is_provider_configured``
    # is imported function-locally because provider_status imports ``settings``
    # from this module; reusing it keeps exactly one definition of "placeholder".
    from services.llm.provider_status import is_provider_configured  # noqa: PLC0415

    priority = [p for p in settings.llm_provider_priority.split(",") if p]
    unconfigured = [p for p in priority if not is_provider_configured(p)]
    if len(unconfigured) == len(priority):
        errors.append(
            "No LLM provider is configured: every provider in "
            f"LLM_PROVIDER_PRIORITY ({', '.join(priority)}) has a blank or "
            "'placeholder-'-prefixed API key, so every generation would fail "
            "with 'No platform LLM route is currently available'. A "
            "'placeholder-' prefix counts as UNSET. Set at least one of "
            "ANTHROPIC_API_KEY / OPENAI_API_KEY / GOOGLE_API_KEY / "
            "OPENROUTER_API_KEY to a real value. The configured provider must "
            "also appear in LLM_PROVIDER_PRIORITY."
        )

    if errors:
        raise RuntimeError("; ".join(errors))
