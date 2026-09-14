# Thought2Build Operations Runbook

Operational procedures for Thought2Build V1 on-call engineers and SREs.  
Covers: circuit breaker, finalise race incident response, credit refund
procedures, auth cache limitations, dependency version management, Lemon Squeezy
billing alerts and ops, and prompt pipeline quality gates.

---

## Table of Contents

1. [LLM Circuit Breaker](#1-llm-circuit-breaker)
2. [Finalise Race (CF-1) — SELECT FOR UPDATE](#2-finalise-race-cf-1)
3. [Credit Accounting — Refund and Recovery](#3-credit-accounting--refund-and-recovery)
4. [Auth Cache — Multi-Worker Limitations](#4-auth-cache--multi-worker-limitations)
5. [General Health Checks](#5-general-health-checks)
6. [Langfuse Docker Image — Version Management](#6-langfuse-docker-image--version-management)
7. [Database Migrations — Alembic Runbook](#7-database-migrations--alembic-runbook)
8. [Secret Rotation Procedures](#8-secret-rotation-procedures)
9. [Billing Alerts And Lemon Squeezy Ops](#9-billing-alerts-and-lemon-squeezy-ops)
10. [Prompt Pipeline Quality Gates And Eval Workflow](#10-prompt-pipeline-quality-gates-and-eval-workflow)
11. [Storyboard Operations](#11-storyboard-operations)
12. [GitHub Living Integration — App, Worker & Webhook Ops](#12-github-living-integration--app-worker--webhook-ops)
13. [Demo Day Mode — Construction Verdict & Rollout](#13-demo-day-mode--construction-verdict--rollout)
14. [Generation Admission Control & Provider Budget (Scalability P0)](#14-generation-admission-control--provider-budget-scalability-p0)
15. [Horizontal Scale-Out — Pooler, Workers & Pool Metrics (Scalability P1)](#15-horizontal-scale-out--pooler-workers--pool-metrics-scalability-p1)
16. [Worker Lanes — Fast/Bulk Queue Split (Scalability P1)](#16-worker-lanes--fastbulk-queue-split-scalability-p1)

---

## 1. LLM Circuit Breaker

**Reference:** CF-2, T-197, T-215  
**Module:** `backend/services/llm/provider_status.py`, `backend/services/llm/gateway.py`

### What It Is

The LLM circuit breaker tracks consecutive failures per provider using `_FAILURES` (a module-level dict in `provider_status.py`). After `_UNHEALTHY_FAILURE_THRESHOLD` (3) consecutive failures within a 600-second window, `can_route(provider)` returns `False` and `gateway.get_llm()` raises `HTTPException(503)` for all subsequent requests to that provider.

### Detecting a Circuit Activation

**Prometheus metric:** `thought2build_llm_circuit_rejections_total{provider="<provider>"}`

> **Alert:** if `thought2build_llm_circuit_rejections_total > 0` — a circuit breaker has
> activated. Check provider health via `GET /providers/health` while signed in.

```promql
# Alert: circuit breaker tripped in the last 5 minutes
increase(thought2build_llm_circuit_rejections_total[5m]) > 0

# Grafana dashboard: rejection rate per provider (T-215)
rate(thought2build_llm_circuit_rejections_total[5m])
# — or grouped to sum across all labels:
sum by (provider) (rate(thought2build_llm_circuit_rejections_total[5m]))

# Current open/closed state per provider (0=closed, 1=open): (T-220)
thought2build_llm_circuit_state

# Alert when any provider circuit is open:
max by (provider) (thought2build_llm_circuit_state) == 1
```

**Log signal:**

```
level=WARNING event="llm.circuit_open" provider="anthropic" model="..."
```

Search Grafana / Loki: `{app="thought2build"} |= "llm.circuit_open"`.

### User Impact When the Circuit Is Open

- All generation requests routed to the tripped provider return **HTTP 503** immediately (no LLM call is made).
- Health-check probes (`GET /providers/health`) are **not** blocked — they
  bypass the circuit via `bypass_circuit=True` and can detect recovery for all
  configured providers. The endpoint is admin-gated (see §5).
- The circuit **auto-resets** when either:
  1. `record_provider_success()` is called (health probe succeeds), or
  2. The last failure is older than 600 seconds (the window expires).

### Manual Reset Procedure

If you need to force-reset the circuit breaker for a provider (e.g. after a deployment that fixes the outage):

```python
# Connect to a running worker (e.g. via kubectl exec or Railway shell):
from services.llm.provider_status import record_provider_success
record_provider_success("anthropic")   # replace with the affected provider
```

Or simply restart the worker processes — `_FAILURES` is in-process memory and resets on restart.

### Disabling the Circuit Breaker for a Provider

For emergency bypass (e.g. during circuit tuning or provider-side investigation):

In `provider_status.py`, temporarily raise `_UNHEALTHY_FAILURE_THRESHOLD` or set it to a very high value, then redeploy. **Revert immediately** after the investigation.

### SLA Impact and Escalation

| Scope | Impact | Action |
|---|---|---|
| One provider tripped | Users on that provider's key receive HTTP 503; platform-key users on other providers unaffected | Check provider status page; wait for auto-reset or call `record_provider_success()` |
| All providers tripped | All generation requests return 503 | **Escalate immediately** to LLM provider engineering contact; consider emergency bypass (raise `_UNHEALTHY_FAILURE_THRESHOLD`) while investigating |
| Circuit never resets | Health probes fail to reach provider | Verify network egress from Railway; check provider status API |

### Multi-Worker Note

`_FAILURES` is **per-worker-process**. In multi-worker deployments (Gunicorn + multiple uvicorn workers) each process maintains an independent failure count. A provider may be circuit-open in one worker and healthy in another. This is an accepted trade-off — a distributed circuit would require a shared Redis counter. Monitor `thought2build_llm_circuit_rejections_total` across all instances to detect partial activation.

### Circuit Breaker and Runtime Provider Recovery

Initial routing walks `LLM_PROVIDER_PRIORITY` and skips unconfigured or
circuit-open providers. In addition, an already-running durable generation now
recovers at the chunk-call boundary: transient failures try the configured
same-provider recovery tier, then another configured provider while the run's
retry reserve remains. Exhausted 429 retries skip the same-provider ladder and
move directly to a different provider so throttling is never amplified by a
larger model on the same account.

Know its real semantics before you rely on it:

| Property | Behaviour | Consequence |
|---|---|---|
| Threshold | 3 failures / 600s, **per process** | Each generation-capable worker replica learns independently; runtime fallback protects the current run before fleet-wide circuit convergence |
| Rate limits | 429/529/503 are **excluded** from breaker failures | Retry with `Retry-After`; after the bounded retry budget, move to another configured provider |
| In-flight generations | Rescued at a failed chunk-call boundary while retry reserve remains | Completed chunks stay checkpointed; only the failed chunk moves routes |
| Artifact tier on failover | Every provider runs `(strong, mid)` for full artifacts | OpenAI failover lands on **GPT-5.5**, not the cheap `mini` tier — a user charged frontier-tier credits never silently receives a much weaker artifact |

**Alert on both circuit state and actual runtime fallback:**

```promql
# Anthropic (the primary) has been shed — traffic is now on a fallback provider.
max by (provider) (thought2build_llm_circuit_state{provider="anthropic"}) == 1

sum by (provider, outcome) (
  rate(thought2build_pipeline_generation_fallbacks_total[5m])
) > 0
```

`max by (provider)` is required because the gauge is per-process.

Forensics for what actually ran come from `llm_cost_events` (`provider`,
`model`, `model_tier`, retry metadata) plus `stage.chunk_generation_fallback`
logs keyed by durable generation id.

**Cost note:** GPT-5.5 is frontier-priced. During a sustained Anthropic outage,
watch `llm_estimated_cost_usd_total{provider="openai"}` — fallback generations
cost materially more than the cheap tier they used to land on. That is the
intended trade (artifact quality over cost at the moment quality matters most),
but it should not be a surprise on the invoice.

---

## 2. Finalise Race (CF-1) — SELECT FOR UPDATE

**Reference:** CF-1, T-196  
**Module:** `backend/services/pipeline/stage_manager.py` — `StageManager.finalise()`

### What It Is

`StageManager.finalise()` uses `SELECT FOR UPDATE` (pessimistic locking via SQLAlchemy `.with_for_update()`) to serialise concurrent finalise calls on the same stage. Only one session can hold the row lock at a time; the second caller blocks at the database level, then re-reads the committed status (`READ COMMITTED` isolation — PostgreSQL default) and raises `ValueError("cannot be finalised")`.

### Detecting a Double-Finalise Incident

A double-finalise would appear as two success responses for the same `stage_id` in the API logs with `status='finalised'`. Under normal operation this **cannot happen** because of the `SELECT FOR UPDATE` lock. If you see it, suspect:

1. A bug was introduced that removed `lock=True` from `_load_stage()` in `finalise()`.
2. The database is not PostgreSQL or is running in a non-MVCC mode.
3. A manual DB patch bypassed the application layer.

**Log pattern** (both would appear in the same narrow time window for the same stage_id):

```
event="stage.finalised" stage_id="<uuid>" workspace_id="<uuid>"
```

### Rollback for a Double-Charged User

If a user was double-charged due to a double-finalise bug:

1. Identify the duplicate `CreditLedger` entry via:

```sql
SELECT * FROM credit_ledger
WHERE user_id = '<user_uuid>'
  AND operation = 'finalise'
ORDER BY created_at DESC
LIMIT 10;
```

2. Issue a manual refund via the credit service:

```python
from services.credit_service import CreditService
await CreditService(redis_client=redis).refund(db, user_id, amount, "manual_refund_double_finalise")
```

3. Verify the refund appears in the ledger and the user's `credit_balance` is updated.

### SELECT FOR UPDATE Prerequisites

- Requires PostgreSQL (not SQLite or other non-MVCC databases).
- Requires `READ COMMITTED` isolation or higher (PostgreSQL default is `READ COMMITTED`).
- The lock is released automatically when the transaction commits or rolls back.
- Integration test in `backend/tests/test_finalise_integration.py` verifies this end-to-end with a real PostgreSQL instance (requires `TEST_DATABASE_URL`).

---

## 3. Credit Accounting — Refund and Recovery

**Reference:** MF-3, T-205  
**Module:** `backend/services/credit_service.py`

### Credit Deduction Flow

1. Pre-check: `CreditService.check_balance()` verifies `credit_balance >= cost` before any generation.
2. Deduction: `CreditService.deduct()` creates a `CreditLedger` entry and decrements `User.credit_balance`.
3. On success: the deduction is committed.
4. On failure / disconnect: `CreditService.refund()` is called with a savepoint to reverse the charge.

### Investigating Credit Discrepancies

```sql
-- Sum all ledger entries for a user (positive = credit, negative = deduction)
SELECT SUM(amount) AS ledger_balance, u.credit_balance AS current_balance
FROM credit_ledger cl
JOIN "user" u ON u.id = cl.user_id
WHERE cl.user_id = '<user_uuid>'
GROUP BY u.credit_balance;
```

If `ledger_balance != current_balance`, a write to `credit_ledger` succeeded but `User.credit_balance` was not updated (or vice versa). Investigate recent error logs around the user's last generation.

### Manual Refund

```python
from services.credit_service import CreditService
await CreditService(redis_client=redis).refund(db, user_id, amount, "manual_refund_incident")
```

### Cache Invalidation

`CreditService.deduct()` and `.refund()` both call `_invalidate(user_id)` which:
1. Deletes the Redis key `credits:<user_id>`.
2. Calls `invalidate_user_cache(user_id)` to evict the in-process `_USER_CACHE` entry (H-4 — T-185).

If the credit balance appears stale after a deduction, verify both caches were cleared.

### Escalation Threshold

| Affected Users | Severity | Action |
|---|---|---|
| 1–2 | Low | Issue manual refund; monitor for recurrence |
| 3–9 | Medium | Open incident; assign on-call engineer |
| ≥ 10 | **P1 Incident** | Page on-call lead; escalate to engineering manager; run post-mortem within 48h |

---

## 4. Auth Cache — Multi-Worker Limitations

**Reference:** LF-1, T-210  
**Module:** `backend/middleware/auth.py`

### What It Is

The auth middleware maintains `_USER_CACHE: dict[UUID, tuple[float, dict]]` — an **in-process** LRU-style cache mapping user IDs to their last-fetched user row (including `credit_balance`). Entries are valid for 30 seconds (`_USER_CACHE_TTL_SECONDS = 30`).

### Multi-Worker Incoherence

In multi-worker deployments (Gunicorn + multiple uvicorn workers), each worker process has its own independent `_USER_CACHE`. A `credit_balance` update (deduction, refund, admin adjustment) via worker A is not visible in worker B's cache until worker B's TTL expires (up to 30 seconds).

**Symptom:** A user deducts credits and immediately sees the old balance in the next request if that request is routed to a different worker.

**Mitigation:**
- `invalidate_user_cache(user_id)` is called after every deduction and refund, but only clears the cache **in the same worker process**.
- For single-worker deployments: no issue.
- For multi-worker deployments: users may see a stale `credit_balance` for up to 30 seconds after a deduction on a different worker.

**Long-term fix:** Migrate `_USER_CACHE` to a Redis-backed cache so all workers share the same invalidation signal. Until then, the 30-second TTL bounds the maximum staleness.

### Debugging Stale Cache Readings

```python
# From within the process (e.g. via uvicorn reload or debug endpoint):
from middleware.auth import _USER_CACHE
# Show all cached users and their expiry:
import time
for uid, (exp, data) in _USER_CACHE.items():
    print(uid, "expires_in:", exp - time.time(), "balance:", data.get("credit_balance"))
```

To force-clear the cache for a specific user: `invalidate_user_cache(user_id)` in the affected worker process.

---

## 5. General Health Checks

### Endpoint

```
GET /health
```

Returns HTTP 200 with `{"status":"ok","version":"1.0.0"}` when healthy and
HTTP 503 with `status:"degraded"` on a database or Redis failure. In
non-production the response also includes `db` and `redis`; production omits
dependency details. Generation-worker readiness is enforced only at the
cache-miss generation boundary, before charging, so a rolling worker deploy does
not take unrelated API routes out of service.

### LLM Provider Health

```
GET /providers/health
GET /providers/health?model=claude-opus-5
```

**Admin-gated** — the caller's email must be in `ADMIN_USER_EMAILS` (403
otherwise, and an empty allowlist authorises no one). It is not open to every
logged-in user because each call makes a real outbound request per configured
provider.

Triggers a live 1-token probe against each provider (bypassing the circuit
breaker) and returns, per provider: `id`, `name`, `configured`, `selectable`,
`health`, `message`, and `probed_model` (`null` when the provider is
unconfigured, so no probe was made). The response also carries `priority` — the
server-owned precedence — so you never have to guess which provider will
actually win a route.

Two things to know:

- **`?model=` proves model *permission*, not just key validity.** It takes any
  catalog model id (unknown ids are rejected with 400 before anything reaches a
  provider) and applies to the provider that owns it; the others keep their
  default judge-model probe. This matters because Anthropic keys carry
  per-workspace model permissions: a key that probes healthy against the Haiku
  judge model can still be denied `claude-opus-5`, which is the model
  full-artifact generation actually runs. **Probe both** after installing a key.
- **A successful probe closes an open circuit.** The probe funnels through
  `record_provider_success()`, so this endpoint doubles as the §1 manual reset
  without a Railway shell. The circuit is per-process, so one call resets one
  worker — see §1's multi-worker note.

`configured: false` means the key is blank or `placeholder-`-prefixed. Since
Anthropic is the platform primary, that state means **every generation fails to
route** while `GET /health` still reports `ok`. Production refuses to boot in
that state (see §8.5), but a non-production environment will happily run in it.

### Prometheus Metrics

```
GET /metrics
```

Requires `Authorization: Bearer <METRICS_TOKEN>`.

Key metrics to monitor:

| Metric | Alert Condition |
|---|---|
| `thought2build_llm_circuit_rejections_total` | Any increase → circuit tripped |
| `llm_provider_configured{provider="anthropic"}` | `== 0` → the primary's key is blank/placeholder; generation cannot route (§8.5) |
| `thought2build_llm_circuit_state{provider="anthropic"}` | `max(...) by (provider) == 1` → primary shed, running on a fallback provider (§1) |
| `llm_request_total` | Drop in successful requests |
| `http_request_duration_seconds` | P95 > 30s → LLM latency spike |
| `sse_stream_duration_seconds` | P95 > 120s → streaming hung |
| `pdf_export_duration_seconds` | P95 > 10s → PDF rendering slow |
| `eval_failure_total` | Sustained increase → eval service degraded |
| `thought2build_billing_webhook_error_total` | Any increase → billing webhook processing error |
| `thought2build_billing_webhook_pending_age_seconds` | `> 300` → billing inbox not draining (queue outage / crashed worker) |
| `thought2build_billing_checkout_rate_limited_total` | Sustained increase → checkout abuse or broken retry loop |
| `pipeline_validator_failures_total` | Any increase → prompt output missing mandatory sections |
| `pipeline_upstream_section_skipped_total` | Sustained increase → upstream stage lacks required context |
| `thought2build_billing_credits_critic_regen_total` | Spike → critic loop is burning extra regeneration credits |

---

## 6. Langfuse Docker Image — Version Management

**Reference:** MF-5, T-209  
**File:** `docker-compose.yml` — `langfuse` service

### Why the Image Is Pinned

`docker-compose.yml` pins the Langfuse image to a specific semver tag (e.g.
`langfuse/langfuse:3.175.0`) instead of `:latest`. A floating `:latest` tag
means **any** `docker compose pull` can silently pull a breaking Langfuse
release, disabling prompt management and telemetry in production without any
code change in this repository.

### Checking for Updates

1. Visit **https://github.com/langfuse/langfuse/releases** and note the latest
   stable semver tag.
2. Review the release notes for:
   - **Breaking changes** to the Langfuse API surface (prompt management
     endpoints, SDK payload shapes).
   - **Database migration requirements** — Langfuse runs its own Postgres
     instance; a major release may require running migrations before the new
     container starts.
   - **Environment variable renames** — check `docker-compose.yml` env block
     against the new release's required vars.
3. If Thought2Build integrates with the Langfuse API (`LANGFUSE_PUBLIC_KEY`,
   `LANGFUSE_SECRET_KEY`, `LANGFUSE_HOST`), verify the SDK version in
   `backend/pyproject.toml` is compatible with the new Langfuse server version.

### Upgrade Procedure

```bash
# 1. Edit docker-compose.yml — update the pinned tag:
#    image: langfuse/langfuse:<new-version>
vim docker-compose.yml

# 2. Pull the new image (validates the tag exists on Docker Hub):
docker compose pull langfuse

# 3. Start the langfuse profile in dev to smoke-test:
docker compose --profile langfuse up -d langfuse langfuse-db

# 4. Verify Langfuse UI is reachable:
curl -s http://localhost:3000/api/public/health | jq .

# 5. Send a test generation request through Thought2Build and confirm prompts are
#    fetched from Langfuse (check backend logs for "langfuse.prompt_fetched"):
#    docker compose logs -f backend | grep langfuse

# 6. If everything is healthy, commit the version bump:
git add docker-compose.yml
git commit -m "chore: bump Langfuse image to <new-version>"
git push
```

### Rollback

If the new version causes issues:

```bash
# Revert docker-compose.yml to the previous pinned tag and redeploy:
git revert HEAD  # or manually edit and docker compose up -d
```

Because the image is pinned, rolling back is a single tag change — no data
loss unless Langfuse ran schema migrations (check release notes).

### Langfuse-Specific Breakage Signals

| Symptom | Likely Cause |
|---|---|
| Backend logs `langfuse.prompt_fetch_failed` | SDK/server API version mismatch |
| Langfuse UI blank / 500 on load | DB migration not completed |
| `LANGFUSE_HOST` returns 404 on `/api/public/prompts` | Endpoint path changed in new version |
| Prompts silently falling back to hardcoded defaults | `get_prompt()` returning `None` — check SDK compatibility |

### Current Pinned Version

See `docker-compose.yml` — `langfuse` service `image:` line. Update this
runbook entry when the version is bumped.

---

## 7. Database Migrations — Alembic Runbook

**Reference:** T-213, T-216  
**Files:** `backend/migrations/`, `backend/alembic.ini`

### Running Migrations in Production (Railway)

Migrations run automatically as part of the Railway deploy process via
`backend/entrypoint.sh`:

```bash
alembic upgrade head
```

This is safe to run on every deploy — Alembic tracks applied migrations in the
`alembic_version` table and skips already-applied revisions. Do **not** set
`--sql` (dry-run mode) in production; it generates SQL without executing it.

**Manual trigger** (if needed via Railway shell):

```bash
cd /app
uv run alembic upgrade head
```

### Rolling Back a Migration

```bash
# Roll back the most recent migration:
uv run alembic downgrade -1

# Roll back to a specific revision:
uv run alembic downgrade <revision_id>

# Check current head:
uv run alembic current

# View migration history:
uv run alembic history --verbose
```

> **Warning:** `downgrade -1` is destructive if the migration added columns or
> tables with data. Always check the `downgrade()` function in the migration
> file before running in production. Take a database snapshot first.

### Checking Migration Status

```bash
# Confirm all migrations are applied:
uv run alembic current

# Expected output: <revision_id> (head)
# If a revision shows without "(head)", a migration is pending.
```

```sql
-- Verify from the DB directly:
SELECT version_num FROM alembic_version;
```

### Migration-Specific Notes

#### T-213 — `eval_results` Composite Index (`0012_eval_results_composite_index.py`)

Migration `0012` creates a B-tree composite index on
`eval_results(stage_version_id, created_at DESC)` via a standard
`CREATE INDEX` statement (not `CONCURRENTLY`).

**On small/staging databases:** safe to apply online with no perceptible lock
time. The migration runs in a transaction; if it fails, the table is unchanged.

**On large production tables (millions of eval rows):** the `ShareLock` held
during index build blocks concurrent writes for the full build duration
(typically seconds to minutes depending on table size). Consider a maintenance
window **or** run the index build manually outside this migration:

```sql
-- Run outside a transaction (psql):
\set ON_ERROR_STOP on
CREATE INDEX CONCURRENTLY ix_eval_results_stage_version_created_at
    ON eval_results USING btree (stage_version_id, created_at DESC);
```

Then mark the migration as applied without executing it:

```bash
uv run alembic stamp 0012
```

#### Adding New Migrations

1. Generate a new revision:
   ```bash
   uv run alembic revision --autogenerate -m "describe_the_change"
   ```
2. Review the generated file in `backend/migrations/versions/` — autogenerate
   is not always correct; verify the `upgrade()` and `downgrade()` functions.
3. Test locally: `uv run alembic upgrade head` against a fresh database.
4. Commit the file and let CI/Railway apply it on the next deploy.

### Backup & Restore

The full disaster-recovery design (two layers, retention, RPO/RTO, restore and
drill procedures) is in [BACKUP_RESTORE.md](BACKUP_RESTORE.md). In short:

- **Layer 1** — Railway managed Postgres backups (fast, single-vendor).
- **Layer 2** — an encrypted off-platform logical dump, run daily by the
  **DB Backup** GitHub Action (`.github/workflows/db-backup.yml`) and restorable
  with `scripts/backup/restore_backup.sh`.

**On-demand backup before a risky/major migration:** trigger the workflow
manually (Actions → *DB Backup* → *Run workflow*), which produces a verified,
encrypted full dump off-platform. For a quick schema-only snapshot without the
pipeline:

```bash
# Schema only (structure, not data):
pg_dump --schema-only $DATABASE_URL > schema_backup_$(date +%Y%m%d).sql
```

Store any manual snapshot outside the Railway ephemeral filesystem before
triggering the deploy.

---

## 8. Secret Rotation Procedures

Thought2Build manages several categories of critical secrets, each with a
distinct rotation impact and procedure.  Rotate proactively on a scheduled
cadence or immediately when a compromise is suspected.

| Secret | Section | Two-secret rotation window? |
|---|---|---|
| `ENCRYPTION_MASTER_KEY` | §8.1 | Yes — via the re-encryption script |
| `CSRF_SECRET` | §8.2 | No (tokens refresh on page reload) |
| `JWT_PRIVATE_KEY` / `_PUBLIC_KEY` | §8.3 | No (forces re-authentication) |
| Redis password | §8.4 | No |
| `ANTHROPIC_API_KEY` / other LLM keys | §8.5 | **No — hard cutover** |
| `GITHUB_APP_WEBHOOK_SECRET` | §12.1 | Yes — `_PREV` |
| `LEMONSQUEEZY_WEBHOOK_SECRET` | §9 | Yes — `_PREV` |

### §8.1 — ENCRYPTION_MASTER_KEY Rotation

**Impact:** `ENCRYPTION_MASTER_KEY` (Fernet symmetric key) encrypts all stored
integration tokens in the `user_integrations` table (`encrypted_token` column).
Rotating without re-encrypting those rows leaves them permanently unreadable
under the new key.

**Pre-rotation check:**

```sql
-- Verify all rows are currently readable (count should be non-negative, no errors):
SELECT COUNT(*) FROM user_integrations WHERE encrypted_token IS NOT NULL;
```

**Rotation steps:**

1. Generate a new Fernet key:

   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

2. Add the new key to Railway environment **alongside** the existing one —
   do **not** remove the old key yet:

   ```
   ENCRYPTION_MASTER_KEY=<old_key>        # keep for decryption
   NEW_ENCRYPTION_MASTER_KEY=<new_key>    # new key for re-encryption
   ```

3. Run the rotation script to re-encrypt all rows (idempotent — safe to
   re-run; rows already encrypted under the new key are skipped):

   ```bash
   cd backend
   uv run python scripts/rotate_encryption_key.py \
       --old-key "$ENCRYPTION_MASTER_KEY" \
       --new-key "$NEW_ENCRYPTION_MASTER_KEY" \
       --database-url "$DATABASE_URL"
   ```

   Use `--dry-run` to preview without writing:

   ```bash
   uv run python scripts/rotate_encryption_key.py \
       --old-key "$ENCRYPTION_MASTER_KEY" \
       --new-key "$NEW_ENCRYPTION_MASTER_KEY" \
       --database-url "$DATABASE_URL" \
       --dry-run
   ```

   **Exit codes:** `0` = all rows rotated or already on new key;
   `1` = connection/argument error; `2` = partial failure (some rows
   could not be decrypted — inspect stderr output).

4. Verify the rotation succeeded:

   ```bash
   uv run pytest tests/test_key_vault.py -v
   ```

   Run the tests with `ENCRYPTION_MASTER_KEY` set to the **new** key.

5. Promote the new key in Railway:

   ```
   ENCRYPTION_MASTER_KEY=<new_key>        # replace with new key
   # Remove NEW_ENCRYPTION_MASTER_KEY
   ```

6. Deploy the updated environment.

**Rollback:** Keep the old key in Railway as `OLD_ENCRYPTION_MASTER_KEY` until
the rotation is fully verified.  To roll back, re-run the script with
`--old-key` and `--new-key` swapped.

---

### §8.2 — CSRF_SECRET Rotation

**Impact:** Rotating `CSRF_SECRET` immediately invalidates **all** outstanding
CSRF tokens.  Users will see `403 Forbidden — CSRF token invalid` on their
next mutating request (POST/PUT/DELETE).  They simply need to refresh the page
to obtain a new token — no data loss occurs.

**Best practice:** Rotate during a low-traffic window (e.g., off-peak hours).

**Rotation steps:**

1. Generate a new secret:

   ```bash
   python -c "import secrets; print(secrets.token_hex(32))"
   ```

2. Update `CSRF_SECRET` in Railway environment.

3. Deploy — the new secret takes effect immediately on next process start.

4. Monitor `csrf.verify.failed` structured log events; they should return
   to the pre-rotation baseline within **5 minutes** as users refresh and
   receive new tokens.

**No Redis cleanup required** — existing CSRF nonce keys expire naturally on
their existing TTL.

**Rollback:** Restore the previous value of `CSRF_SECRET` in Railway and
redeploy.

---

### §8.3 — JWT Key Rotation

**Impact:** Rotating `JWT_PRIVATE_KEY` / `JWT_PUBLIC_KEY` invalidates **all**
existing access tokens and refresh tokens (RS256 signature verification fails
against the old public key).  Every active user is forced to re-authenticate.

**Best practice:** Rotate only when key compromise is confirmed, or on an
annual schedule.  Announce to users in advance if possible.

**Rotation steps:**

1. Generate a new RS256 key pair:

   ```bash
   openssl genrsa 4096 | tee jwt_private.pem
   openssl rsa -in jwt_private.pem -pubout > jwt_public.pem
   ```

2. Update `JWT_PRIVATE_KEY` (contents of `jwt_private.pem`) and
   `JWT_PUBLIC_KEY` (contents of `jwt_public.pem`) in Railway.

3. Purge all refresh token entries from Redis so stale tokens cannot be
   exchanged:

   ```bash
   redis-cli -u "$REDIS_URL" KEYS "refresh:*" | xargs redis-cli -u "$REDIS_URL" DEL
   ```

4. Deploy.

5. Monitor authentication error rates — they will spike briefly as users
   re-authenticate, then return to baseline within minutes.

**Rollback:** Restore the old `JWT_PRIVATE_KEY` and `JWT_PUBLIC_KEY` in
Railway and redeploy.  Previously-issued tokens (before the rotation) become
valid again, but any tokens issued under the new key will stop working.

---

### §8.4 — Redis Password Rotation (if applicable)

If the Redis instance requires a password (e.g., Railway managed Redis with
auth enabled):

1. Obtain the new Redis password from the Railway dashboard or your Redis
   provider.
2. Update `REDIS_URL` in Railway to include the new password.
3. Redeploy the service — the connection pool is rebuilt on startup.

No data re-encryption is needed; Redis stores session and rate-limit state,
not long-lived secrets.

---

### §8.5 — LLM Provider API-Key Installation & Rotation

**Impact:** `ANTHROPIC_API_KEY` is the key that matters. Anthropic is the
platform primary and runs **Claude Opus 5** for full-artifact generation (the
four core stages, full regenerate, the harness gap-patch) and **Claude Haiku
4.5** for every cheap path. A blank or `placeholder-`-prefixed value is treated
as *unset* by routing, so a bad key does not degrade the fallback path — it
breaks the **default** path, and every generation fails with
`No platform LLM route is currently available` while `GET /health` still
reports `ok`.

> ⚠️ **There is no `_PREV` two-secret rotation window for LLM keys.** Unlike
> `GITHUB_APP_WEBHOOK_SECRET` or `LEMONSQUEEZY_WEBHOOK_SECRET`, exactly one
> value is live at a time. Rotation is a hard cutover: create the new key,
> paste it, redeploy, verify, *then* revoke the old one in the provider console.
> Never revoke first.

**Rotation steps:**

1. Create the key in the [Anthropic Console](https://console.anthropic.com/).
   Confirm its workspace has **Opus 5 access** — Anthropic keys carry
   per-workspace model permissions, so a key that authenticates fine can still
   be denied the one model this deployment depends on. Also confirm the usage
   tier can absorb *primary* traffic, not just fallback overflow.

2. Set `ANTHROPIC_API_KEY` on **all three Railway services** — `backend`,
   `worker`, and `worker-fast`. Use a **Shared Variable / Variable Reference**
   so it is one value, not three pastes that can drift. A worker missing the key
   fails silently per job; nothing validates it at job start.

3. Redeploy all three (Deployments → Deploy). A variable change alone does not
   restart a service.

4. Revoke the old key in the Anthropic Console **only after** the verification
   below passes.

**Verification:**

1. **Startup guard.** The `backend` deploy now *fails* rather than booting green
   when every provider in `LLM_PROVIDER_PRIORITY` is unconfigured
   (`validate_production_settings`). A successful deploy therefore already
   proves at least one key is non-placeholder — but not *which*, and not that it
   is valid. Continue.

2. **Live probe** (admin token, ~1 token of spend). Run **both**:

   ```bash
   # Does the key authenticate at all?
   curl -s -H "Authorization: Bearer $ADMIN_TOKEN" \
     "https://<prod-host>/providers/health"

   # Is its workspace PERMITTED the model generation actually runs?
   curl -s -H "Authorization: Bearer $ADMIN_TOKEN" \
     "https://<prod-host>/providers/health?model=claude-opus-5"
   ```

   Expect `configured: true`, `health: "healthy"`, and
   `probed_model: "claude-opus-5"` on the second. A 401/403 from Anthropic
   surfaces as `health: "unhealthy"` with the error class in `message`.

3. **Metrics** (`Authorization: Bearer $METRICS_TOKEN`):
   `llm_provider_configured{provider="anthropic"} == 1` and
   `thought2build_llm_circuit_state{provider="anthropic"} == 0`. Both gauges are
   per-process and written as a side effect of a probe, so hit
   `/providers/health` first; use `max(...) by (provider)` under
   `WEB_CONCURRENCY > 1`.

4. **What actually ran** — the only check that proves Opus 5 *routing* rather
   than mere key validity. After the first real generation:

   ```sql
   SELECT provider, model, model_tier, operation, usage_estimation_method
   FROM llm_cost_events ORDER BY created_at DESC LIMIT 10;
   ```

   Expect `model='claude-opus-5'`, `model_tier='strong'`, and
   `usage_estimation_method='provider_reported'` (`tokenizer_estimated` means no
   real usage block came back). A `stage.chunk_generation_fallback` log line
   means it de-escalated to Sonnet 5 — see §1.

5. **Smoke script** (non-paid end-to-end):

   ```bash
   THOUGHT2BUILD_API_URL=https://<prod-host> \
   THOUGHT2BUILD_ACCESS_TOKEN=$ADMIN_TOKEN \
   THOUGHT2BUILD_SMOKE_MODEL=claude-opus-5 \
     python scripts/production_smoke.py
   ```

   Set `THOUGHT2BUILD_RUN_LLM_SMOKE=1` to additionally generate and finalise one
   real SPEC, then generate its dependent PLAN (this **does** consume credits).

**Rollback:** restore the previous `ANTHROPIC_API_KEY` on all three services and
redeploy. If the old key is already revoked and no replacement works, fail over
to another platform with an **env-only** change — no redeploy of code:

```
LLM_PROVIDER_PRIORITY=google,anthropic,openai
GOOGLE_API_KEY=<a real key>
```

Routing skips any provider whose key is blank or `placeholder-`-prefixed, so the
reordered list takes effect on the next process start. Note the quality
trade-off: Google's `strong` tier has no active model, so it runs Gemini Flash
(`mid`). Reordering to `openai,...` with a real `OPENAI_API_KEY` lands on
GPT-5.5, which is the closer substitute.

**A fourth, optional provider — OpenRouter (issue #152).** `OPENROUTER_API_KEY`
defaults to unset and is never required to boot; shipping its catalog/adapter
wiring is inert while `LLM_PROVIDER_PRIORITY` omits `"openrouter"`. The ladder
is **all-DeepSeek-V4** — `deepseek/deepseek-v4-flash` (`mid`, the cheap primary
and judge) escalating to `deepseek/deepseek-v4-pro` (`strong`, full-artifact
generation) — reached through a thin `chat_completions` adapter over the
`openai` SDK pointed at `https://openrouter.ai/api/v1`. If a key is set, install
it the same way as `ANTHROPIC_API_KEY` — a Shared Variable across all three
Railway services (`backend`, `worker`, `worker-fast`) — but do **not** add
`"openrouter"` to `LLM_PROVIDER_PRIORITY` in production without completing the
promotion gate in `docs/evals/ROUTE_PROMOTION.md`.

*Why every tier is DeepSeek, and why DeepSeek's endpoint is required.*
OpenRouter serves one model slug from many upstream hosts. The request uses
matching `provider.order=["deepseek"]` and `provider.only=["deepseek"]` lists,
`require_parameters=true`, and `data_collection="deny"`. Fallback remains enabled
only within that allow-list; cross-provider recovery belongs to the durable
application pipeline. This restriction matters because prompt caching exists on
**1 of 19** hosts serving `deepseek-v4-flash` (DeepSeek's own) and on **0 of 32**
serving the retired `z-ai/glm-5.2`; hosts disagree on real
`max_completion_tokens` by up to **32x** against the 32,768 the pipeline sends;
and catalog rates are the pinned host's, which differ from the model alias's.

*Failure modes specific to this provider.*

* **No privacy-compatible endpoint.** `data_collection: "deny"` can still
  narrow the pool to zero, which OpenRouter answers with "no available model
  provider meets your routing requirements". Detect it
  with `GET /providers/health?model=deepseek/deepseek-v4-pro` (admin) — the
  probe carries the same routing policy a generation does, and `probe_error` on the
  response distinguishes an empty pool from a bad key. Run this **before** the
  env flip, the same way §8.5's Anthropic steps verify `claude-opus-5`.
* **A 502/503 triggers application fallback.** Typed overloads honor
  `Retry-After` with bounded in-place retries. Other transient failures move to
  the next DeepSeek tier and then the next configured platform provider while
  the durable deadline still has a real retry window.
* **Silent catalog drift.** Rates, output ceilings and caching support all move
  under a floating slug. `uv run python ../scripts/check_openrouter_catalog_drift.py`
  diffs the catalog against the *preferred endpoint* and exits non-zero on drift.
  Run it at the quarterly `CATALOG_HYGIENE.md` review; it is deliberately not in
  CI (network dependency, and a scheduled job bills ~1 minute per firing).

---

## 9. Billing Alerts And Lemon Squeezy Ops

Billing runs on **one flag-gated payment provider at a time** (issue #44):
`PAYMENTS_ENABLED` is the master kill switch (default **false**) and
`PAYMENT_PROVIDER` selects the active gateway (`lemonsqueezy` | `razorpay`).
Checkout is live only when `billing_checkout_enabled` =
`payments_enabled and (active provider fully configured)`. **Lemon Squeezy**
(Phase 22) is the Merchant of Record, so it owns tax, chargebacks, and disputes;
chargebacks/disputes surface to Thought2Build through `order_refunded`/fraud
revocation inputs. **Razorpay** (issue #44) is the INR alternative via hosted
Payment Links — it is **not** a Merchant of Record (tax/dispute liability sits
with the account holder), and its ops deltas are in §9.9. **Webhook endpoints for
both providers always process regardless of which one is active** (D3), so
refunds/disputes on old orders keep settling after a provider switch. The Stripe
runtime is
**fully decommissioned** (T-308): there is no Stripe SDK, config, or webhook
processing — `POST /billing/webhook` answers a Stripe-shaped request (a
`Stripe-Signature` header) with `{"status":"ignored_provider_disabled"}` and no
DB write. Only the read-only `stripe_credit_packs` / `stripe_webhook_events`
audit tables (the historical financial record) remain.

Architecture recap (so the procedures below make sense):

- `POST /billing/checkout` is **attempt-first** — a `billing_checkout_attempts`
  row is committed (snapshotting credits/price/currency/validity) before Lemon is
  called; the frontend polls `GET /billing/status?checkout_ref=…`.
- `POST /billing/webhook` verifies the `X-Signature` HMAC (two-secret list),
  commits the event to the durable `billing_webhook_events` **inbox** keyed by the
  Lemon event id, then **enqueues** `billing_process_webhook` on the arq worker.
  The HTTP path never grants credits inline.
- The worker grant is idempotent on `(provider, provider_order_id)` and the
  `billing_purchase:lemonsqueezy:{order}` ledger reason. Refund/fraud revocation
  is idempotent on the `refund:billing:{pack}:{cents}` reason; over-spent value
  becomes **recoverable billing debt** (expired value is never debt).
- Two crons keep the system honest: a **60s pending-sweep**
  (`billing_process_pending_webhooks`) and a **15-minute reconcile**
  (`billing_reconcile`, three lanes).

### 9.1 Grafana Alerts

The following alert rules correspond to the provider-labelled counters defined in
`services/observability.py` (Phase 22 — T-304). `{provider}` is whichever gateway
emitted the event — `lemonsqueezy` or `razorpay` (issue #44); a provider switch
does not silence the retired one's series, since its webhooks keep settling
(§9.9). `provider="stripe"` series persist only as historical audit data after
the T-308 decommission. Every alert below is provider-neutral and fires
identically for Razorpay traffic (drill into the `provider` label to disambiguate).

| Alert | Condition | Severity | Action |
|---|---|---|---|
| BillingWebhookErrorRate | `rate(thought2build_billing_webhook_error_total[5m]) > 0` | Warning | Check logs for `billing.webhook.*` / `billing.job.*`; inspect Lemon dashboard delivery logs. Webhook retries are safe — the inbox dedupes on the event id. Delete a `billing_webhook_events` row only with incident-lead approval and only after confirming credits were not granted |
| BillingWebhookPendingAge | `thought2build_billing_webhook_pending_age_seconds > 300` | Warning | The inbox is not draining (queue outage or crashed worker). Confirm the worker process is up and Redis is reachable; the 60s sweep re-enqueues stale rows. A sustained breach is the trigger to scale the billing worker out (§9.6) |
| BillingCheckoutDropped | `rate(thought2build_billing_checkout_completed_total[30m]) == 0` while `checkout_created` rising; **or** zero `checkout_completed` over 72h | Warning | Verify `/billing/webhook` is reachable and returning 200; check Lemon webhook delivery logs and the inbox; inspect CSRF/rate-limit exemptions |
| BillingCheckoutApiError | `rate(thought2build_billing_checkout_api_error_total[15m]) > 0` | Warning | A `POST /billing/checkout` failed. `error_type="provider_error"` → Lemon's checkout API failed (users get a 502 and **cannot pay**); check Lemon status and the `_API_KEY`/`_STORE_ID`/`_VARIANT_ID` config. `error_type="orphaned_commit"` → the post-Lemon local commit failed so the URL was never exposed; the attempt is recovered by reconcile (lane 3 hygiene + lane 1), but a sustained rate signals a DB/commit fault — inspect `billing.checkout.*` logs |
| BillingReversalSpike | `rate(thought2build_billing_credits_revoked_total[1h])` above baseline | Warning | A burst of `order_refunded`/fraud revocations. Review the Lemon dashboard; confirm reversals are legitimate and debt was created where expected |
| BillingUnprovablePaidCheckout | `increase(thought2build_billing_unrecoverable_checkout_total[1h]) > 0` | Warning | An `order_created` was rejected while the provider reports the order **paid**. Reconcile cannot auto-grant this — settle via the admin-correction path (§9.5) with evidence |
| BillingDebtCreated | `increase(thought2build_billing_credit_debt_created_total[1h]) > 0` | Info | A reversal exceeded remaining balance and created recoverable debt. Expected after refunds on spent credits; investigate if the rate is abnormal |
| BillingReconcileMismatch | `increase(thought2build_billing_reconcile_mismatch_total[1h]) > 0` | Warning | Reconcile lane 2 applied a reversal the webhook path missed. Investigate why the webhook was lost (delivery, signature, inbox) |
| BillingExpirySpike | `rate(thought2build_billing_credits_expired_total[1h])` above baseline | Info | Unusual volume of credits lazily expiring; correlate with a past purchase cohort, not a fault |
| BillingJobDeadlettered | `increase(thought2build_billing_job_deadlettered_total[15m]) > 0` | Critical | A billing job exhausted retries and landed in `billing:deadletter`. Inspect and replay (§9.3) |
| BillingWebhookDuplicate | `rate(thought2build_billing_webhook_duplicate_total[1h]) > 100` | Info | Normal if Lemon is retrying; investigate above 100/hour — the endpoint may be failing silently after the `already_processed` return |
| BillingAdminCorrection | `increase(thought2build_billing_admin_correction_total[1h]) > 0` | Info | **Control-visibility, not a failure.** A privileged manual credit grant landed via `POST /billing/admin/correction` (§9.5). Expected only to settle a `BillingUnprovablePaidCheckout`. Confirm the caller is an authorised `ADMIN_USER_EMAILS` operator and that the `billing_admin_corrections` audit row carries an `evidence_url`; investigate any correction with no corresponding unrecoverable-checkout signal |

**Counters with deliberately no standalone alert** (so the "every failure mode has
an alert" sign-off is met without padding the runbook). These are
dashboard/business/context metrics, not failure modes:
`thought2build_billing_credits_granted_total`,
`thought2build_billing_purchase_revenue_cents_total`,
`thought2build_billing_credits_consumed_total`,
`thought2build_billing_credit_debt_recovered_total`,
`thought2build_billing_webhook_received_total`,
`thought2build_billing_checkout_created_total` (consumed inside `BillingCheckoutDropped`),
`thought2build_billing_checkout_expired_total` (lane-3 hygiene churn), and
`thought2build_billing_checkout_rate_limited_total`. `thought2build_billing_job_retries_total`
is intentionally **not** alerted on its own — retries are transient by design and
the dead-letter is the actionable signal (`BillingJobDeadlettered`, Critical),
matching the GitHub queue pattern in §12. `thought2build_billing_credits_critic_regen_total`
and `thought2build_billing_credits_brave_research_total` are platform-funded
quality/research credits, not payment-flow metrics, and are out of scope here.

### 9.2 Billing Endpoint Recovery

- `GET /billing/package` is public and should keep returning the configured
  package even when checkout is disabled.
- `POST /billing/checkout` requires authentication and is rate limited to five
  attempts per user per hour. A **503** means `billing_checkout_enabled` is false —
  either `PAYMENTS_ENABLED=false` (the shipping default) or the **active** provider
  is not fully configured (Lemon: one of `LEMONSQUEEZY_API_KEY` / `_STORE_ID` /
  `_VARIANT_ID` blank; Razorpay: `RAZORPAY_KEY_ID` / `_KEY_SECRET` blank); a
  **502** means the active provider could not create the checkout, or the
  post-provider commit failed (the URL is never exposed; reconcile settles the
  order later).
- `POST /billing/webhook` is exempt from browser CSRF and app rate limits, but
  every event must pass `X-Signature` HMAC verification before any DB/queue work.
  Webhook retries are safe: the `billing_webhook_events` inbox is unique per Lemon
  event id and duplicate deliveries return `already_processed`.
- In production, Lemon enablement requires `LEMONSQUEEZY_WEBHOOK_SECRET`, an HTTPS
  `LEMONSQUEEZY_SUCCESS_URL`, and `LEMONSQUEEZY_TEST_MODE=false` — half-configured
  Lemon fails `validate_production_settings()` at startup.

### 9.3 Dead-Letter Replay (`billing:deadletter`)

Billing jobs that exhaust `JOB_MAX_TRIES` are routed to the Redis list
`billing:deadletter` (the GitHub stream uses the separate `gh:deadletter` — never
mix them). The constants are `BILLING_DEAD_LETTER_KEY` in
`backend/services/queue.py`.

1. Inspect depth and the head record:
   ```bash
   redis-cli -u "$REDIS_URL" LLEN billing:deadletter
   redis-cli -u "$REDIS_URL" LINDEX billing:deadletter 0
   ```
   Each record carries the job function (`billing_process_webhook`) and its args
   (the `billing_webhook_events` row id).
2. **Find root cause first** in the logs (`billing.job.*` / `_persist_failed`)
   before replaying — replaying a poison job just dead-letters it again.
3. The underlying inbox row is still present and idempotent. The safest replay is
   to let the **60s sweep**/**15-minute reconcile lane 1** re-enqueue it: confirm
   the row's status is `received`/retryable `failed` and wait one cron tick.
4. To force it immediately, re-enqueue `billing_process_webhook` with the inbox
   row id (e.g. via an arq enqueue against `WorkerSettings`' Redis), then
   `LREM billing:deadletter 1 <record>` once it processes cleanly. Grants stay
   idempotent on `(provider, provider_order_id)`, so a double replay cannot
   double-credit.

### 9.4 Pending-Row Recovery & Reconcile

- **Pending sweep (60s):** `billing_process_pending_webhooks` reclaims inbox rows
  stuck in `received`/`failed`/stale `processing` (e.g. after a queue outage or a
  crashed worker) and refreshes `thought2build_billing_webhook_pending_age_seconds`
  to the age of the oldest non-`processed` row. A rising gauge is the primary
  "lost webhook" signal.
- **Reconcile (15-minute backstop):** `billing_reconcile` claims **every
  configured provider's** `billing_reconciliation_cursors` row upfront in one
  session — ordered by provider, `SELECT … FOR UPDATE NOWAIT`, all held to the
  final commit — as the single-active-run lock (D11). If **any** cursor row is
  already locked the whole tick skips cleanly (never a per-provider split across
  two overlapping ticks); on failure `_persist_reconcile_error` stamps
  `last_error` on **every** claimed row. It then runs three bounded lanes:
  - **Lane 1 — inbox replay:** re-enqueues committed/retryable rows (both
    providers; labels come from each inbox row's provider). This is the only
    automatic path that recovers a missed grant (`order_created` /
    `payment_link.paid`) — the signed `checkout_ref`+nonce row is the proof.
  - **Lane 2 — provider re-read:** iterates the configured providers, paging each
    provider's active, consumed, expired, refunded, and disputed packs from its own cursor and re-reading via
    `lemonsqueezy_service.get_order` / `razorpay_service.get_payment`, each under
    its own `*_RECONCILE_MAX_CALLS_PER_RUN` budget and its own 429 back-off; a
    missed refund/fraud applies the **same** `apply_refund_reversal` and
    increments `reconcile_mismatch`. **Caveat:** lane 2 cannot detect a Razorpay
    chargeback — Razorpay payments carry no fraud/dispute status (§9.9), unlike
    Lemon's `fraudulent`.
  - **Lane 3 — hygiene:** expires checkout attempts past `expires_at` and emits a
    stale-attempt operator count, labelled by each attempt row's own provider (a
    batch may mix providers).
- **Razorpay recovery:** each run also reads a bounded page of unresolved,
  recorded Payment Links (including locally expired/failed attempts). Only an
  authenticated link/payment pair matching the original reference, owner, nonce,
  environment, amount, and currency can grant. The shared webhook transaction
  applies known refunds/disputes before credits become available. Recovery scans
  the last `RAZORPAY_RECONCILE_LOOKBACK_DAYS` (default 180 days), up to
  `RAZORPAY_RECOVERY_ATTEMPTS_PER_RUN` (default 50) each run. Older unresolved
  attempts remain stored for explicit support recovery.

### 9.5 Admin-Correction Runbook (`POST /billing/admin/correction`)

The exceptional, evidence-backed manual grant for an order the automatic pipeline
could not settle (e.g. `BillingUnprovablePaidCheckout`).

- **Authorisation:** the caller's email must be in `ADMIN_USER_EMAILS`
  (comma-separated). An empty allowlist authorises nobody — the path is closed by
  default. There is no role column in V1.
- **Request body:** `provider` (`lemonsqueezy` | `razorpay`), `provider_order_id`
  (the Razorpay `pay_…` payment id), `checkout_ref` (required for Razorpay), `target_user_id`, `credits`,
  `price_cents`, `currency`, a `reason` justification, and an `evidence_url` (the
  support ticket or the active provider's dashboard order). The `evidence_url` is
  **required**. `expires_at` is derived from the named provider's
  `credit_validity_days` (provider-aware), not a hardcoded Lemon value.
- **Idempotency:** the write is append-only and unique on
  `(provider, provider_order_id)`. A repeat call returns `applied: false`,
  `credits_granted: 0` — never a second grant. Every call is audited
  (`billing_admin_corrections` row + `thought2build_billing_admin_correction_total`).
- **Procedure:** locate the original checkout and capture a support evidence URL.
  The Razorpay correction endpoint independently fetches the recorded Payment
  Link and payment, validates its proof and original economics, binds the pack,
  and completes the original checkout atomically. Its expiry is anchored to the
  payment timestamp. Verify usable credits and debt recovery, which may differ
  from the purchased amount. A correction is a positive grant, never a tool to
  settle a refund or dispute loss. Replay the signed reversal event instead.

### 9.6 Optional Dedicated Billing Worker (Scale-Out)

By default, billing jobs run on the shared `WorkerSettings` worker (the same
process that drains the GitHub queue) on the default queue — works out of the box;
never route billing to a queue with no consumer. For scale-out under sustained
load (the trigger is a persistently high `webhook_pending_age_seconds`), a
dedicated `queue_name="billing"` consumed by a separate `BillingWorkerSettings`
worker may be added (its own `Procfile` line + `docker-compose` service, sharing
the backend image and Redis). This is an optional future operational step, not
currently wired — add it only when the shared worker can no longer keep the
pending age low.

### 9.7 Account-Deletion Settlement (RESTRICT FKs)

`billing_credit_debts` and `billing_admin_corrections` hold **`ON DELETE
RESTRICT`** foreign keys to `users` / `billing_credit_packs` so the financial
audit trail can never be silently orphaned. V1 exposes **no** user-deletion
endpoint, so this is a **manual ops procedure**, not a code path:

1. Before removing a user row (a GDPR erasure or support deletion), first settle
   billing state: confirm there is **no open (unrecovered) debt** for the user
   (`billing_credit_debts` where `credits_owed > credits_recovered`); recover or
   write it off per finance policy.
2. The `RESTRICT` FKs will **block** the delete while any debt/correction row
   references the user or their packs. Do not `CASCADE` or hand-delete the audit
   rows to force the delete — that destroys the financial record. Retain the audit
   rows (anonymise the user PII elsewhere if required by the erasure request) and
   record the settlement in the deletion ticket.

### 9.8 Lemon API-Key & Webhook-Secret Rotation

**Webhook secret (two-secret window).** The handler verifies `X-Signature`
against `settings.lemonsqueezy_webhook_secrets` =
`[LEMONSQUEEZY_WEBHOOK_SECRET, LEMONSQUEEZY_WEBHOOK_SECRET_PREV]` (blank entries
ignored), so rotation is zero-downtime:

1. Generate the new secret in the Lemon dashboard (do not save it on the webhook
   yet).
2. Stage both secrets in the backend env — the new one is accepted but signs
   nothing yet:
   ```env
   LEMONSQUEEZY_WEBHOOK_SECRET=<new_secret>       # accepted
   LEMONSQUEEZY_WEBHOOK_SECRET_PREV=<old_secret>  # still accepted during window
   ```
   Redeploy. Both secrets now verify.
3. Update the **Lemon webhook** to sign with `<new_secret>`. Deliveries now sign
   with the new secret and still verify against `LEMONSQUEEZY_WEBHOOK_SECRET`.
4. Watch `thought2build_billing_webhook_error_total` and the inbox for one delivery
   cycle. If `bad_signature`/error spikes, set `LEMONSQUEEZY_WEBHOOK_SECRET` back
   to the old value and investigate.
5. Close the window: clear `LEMONSQUEEZY_WEBHOOK_SECRET_PREV=` and redeploy. The
   old secret no longer verifies.

**API key.** `LEMONSQUEEZY_API_KEY` is used only for outbound calls
(`create_checkout`, `get_order`), so there is no two-key acceptance window:
create the new key in Lemon, set `LEMONSQUEEZY_API_KEY=<new>`, redeploy, confirm
a test checkout creates and `get_order` succeeds (reconcile lane 2), then revoke
the old key in Lemon. The key is never logged (redacted by `observability.py`).

### 9.9 Razorpay Provider Ops (issue #44)

Razorpay is the second, flag-selected gateway (INR, hosted Payment Links). It
**reuses every mechanic above** — attempt-first checkout, the durable inbox, the
same `billing_process_webhook` job on the same worker, `billing:deadletter`
(§9.3), reconcile (§9.4), and admin-correction (§9.5). Only the deltas are here.

**Setup (dashboard, before enabling — full checklist in the plan §10):**

- Complete KYC so **live mode** activates (`rzp_live_` keys; `rzp_test_` keys work
  pre-KYC for staging only).
- **Payment capture → auto-capture ON.** A payment left in `authorized` never
  fires `payment_link.paid`, so no grant ever lands.
- Create webhooks in **both test and live modes** (Razorpay webhooks are
  **per-mode** — test and live are configured separately, each with its own
  secret), URL `https://<api-host>/billing/webhook/razorpay`, events
  `payment_link.paid`, `refund.processed`, and all six dispute events:
  `payment.dispute.created`, `.under_review`, `.action_required`, `.lost`,
  `.won`, and `.closed`. Failed/expired link events remain informational.
- Config: `RAZORPAY_KEY_ID` / `_KEY_SECRET`, `RAZORPAY_WEBHOOK_SECRET`,
  HTTPS `RAZORPAY_SUCCESS_URL`, and the economics
  (`RAZORPAY_PRICE_CENTS` in **paise** — 154900 = ₹1549 — `_CURRENCY`,
  `_CREDITS_PER_PURCHASE`, `_CREDIT_VALIDITY_DAYS`). In production
  `validate_production_settings()` requires the webhook secret, an HTTPS success
  URL, positive economics, `RAZORPAY_CHECKOUT_TTL_MINUTES >= 16` (Razorpay rejects
  `expire_by` under ~15 min), and a `rzp_live_` key prefix — the environment guard
  (Razorpay events carry **no** `test_mode` flag; the key prefix *is* the
  environment, round-tripped through `notes.environment` and enforced on every
  `payment_link.paid`).

**Razorpay-only checkout:** set `PAYMENT_PROVIDER=razorpay`; other values cannot
create new purchases and fail production startup. `PAYMENTS_ENABLED=false`
stops new checkout while existing payment, refund, and dispute settlement
continues. Retain legacy provider secrets only if historical purchases require
settlement. Apply migration 0046 before starting the updated API and workers.

**Webhook-secret rotation (two-secret window, per-mode).** Identical mechanism to
§9.8 but with `RAZORPAY_WEBHOOK_SECRET` / `RAZORPAY_WEBHOOK_SECRET_PREV` (the
handler verifies `X-Razorpay-Signature` against both): stage both in env and
redeploy → update the Razorpay webhook (for the mode you are rotating) to sign
with the new secret → watch `thought2build_billing_webhook_error_total` for one
delivery cycle → clear `_PREV` and redeploy. Because Razorpay webhooks are
per-mode, rotate the **live** webhook's secret against the live env; a test-mode
secret change never touches production traffic.

**Key rotation.** `RAZORPAY_KEY_ID` / `RAZORPAY_KEY_SECRET` are outbound-only
(`create_payment_link`, `get_payment`) — no two-key acceptance window: create the
new key pair in the Razorpay dashboard, set both env values, redeploy, confirm a
test checkout creates and reconcile lane 2 (`get_payment`) succeeds, then revoke
the old pair. Keys/secret/nonce are never logged (`razorpay_service` mirrors the
Lemon log-safety; asserted by `test_razorpay_service.py`). **Rotating `KEY_ID`
across the `rzp_test_`↔`rzp_live_` boundary changes the environment guard** — the
server then only accepts webhooks whose `notes.environment` matches, so flip keys
and the active webhook mode together.

**Dispute settlement:** signed dispute events are persisted by dispute and payment
id. Open disputes are recorded without withholding credits; losses revoke credit
value and create recoverable debt when needed. Newer wins release the reversal
with original pack expiry and donor provenance. Older events and regressions to
open do not undo a terminal decision; conflicting terminal events at the same
provider timestamp require an authenticated dispute read. Cash refunds and lost
disputes use the greater cumulative reversal, capped at the purchase, so overlapping
claims cannot revoke the same entitlement twice. A closed event only independently
revokes its `amount_deducted`; cash refunds remain authoritative separately.

Payment re-reads do not discover wholly missing dispute events. Monitor the
Razorpay dispute dashboard and webhook delivery failures; redeliver missing
signed events and check the inbox/dead-letter queue. Do not use positive admin
corrections to reverse money. See [launch verification](PAYMENT_LAUNCH_VERIFICATION.md).

**Dead-letter / reconcile / pending-sweep** are all provider-neutral and cover
Razorpay by construction — §9.3, §9.4 apply unchanged (the dead-letter record's
`billing_process_webhook` arg is the inbox row id regardless of provider; grants
stay idempotent on `(provider='razorpay', pay_…)` so replay never double-credits).

## 10. Prompt Pipeline Quality Gates And Eval Workflow

Phase 19 introduced mandatory structure gates before critic regeneration and an
offline prompt eval suite for prompt changes. The relevant files are:

- `backend/prompts/base.py` for `ASDD_PROMPT_VERSION`.
- `backend/services/pipeline/artifact_validator.py` for stage section
  contracts and zero-LLM validation before critic/regeneration.
- `backend/services/pipeline/prompt_builder.py` for upstream section extraction
  and `pipeline_upstream_section_skipped_total`.
- `backend/services/pipeline/critic.py` for the inline critic repair prompt and
  `thought2build_billing_credits_critic_regen_total`.
- `harness/prompt_eval/` for golden workspaces, deterministic graders, and the
  local CLI.
- `.github/workflows/prompt-eval.yml` for the PR gate on prompt changes.

### When To Run The Eval Suite

- Any structural change to a stage prompt, including a new mandatory section or
  a changed verification checklist.
- Any change to the critic prompt template in `services/pipeline/critic.py`.
- Any change to `SECTION_CONTRACTS` in `artifact_validator.py`.

### Required Workflow For Prompt Changes

1. Branch from `main`.
2. Change the prompt, critic template, or section contract.
3. Bump `ASDD_PROMPT_VERSION` in `backend/prompts/base.py`. Use a minor bump
   for structural requirements and a patch bump for wording-only changes.
4. Run the eval locally:

   ```bash
   cd harness
   uv run python -m prompt_eval.run \
     --version <new-version> \
     --baseline <old-version> \
     --report report.md
   ```

5. Review `report.md`. Per-grader scores must be at or above baseline. Any
   accepted regression needs an owner and written release approval.
6. Open the PR. The `prompt-eval` GitHub workflow fails if files under
   `backend/prompts/**` changed without an `ASDD_PROMPT_VERSION` bump, then runs
   the same eval suite and posts the Markdown report on the PR.

### Quarterly Rebaseline

Once per quarter, refresh the eval suite so it reflects the current product:

1. Choose one representative recent workspace from production or staging.
2. Anonymize it before it enters `harness/prompt_eval/golden_workspaces/`.
3. Run the anonymization guard:

   ```bash
   cd backend
   uv run pytest ../harness/tests/backend/test_prompt_eval_anonymization.py -q
   ```

4. Run the prompt eval on `main` and save the new baseline scores:

   ```bash
   cd harness
   uv run python -m prompt_eval.run \
     --version "$(grep -oE 'asdd-v[0-9.]+' ../backend/prompts/base.py)" \
     --baseline asdd-v1.8.0 \
     --report prompt_eval_report.md
   ```

5. Attach `prompt_eval_report.md` to the quarterly review and update the
   checked-in baseline score files only after the anonymization and eval checks
   pass.

### Incident Response Signals

| Signal | Meaning | Action |
|---|---|---|
| `pipeline_validator_failures_total` increases | A stage output is missing a mandatory contract section before critic repair | Inspect the affected stage output and prompt version; pause prompt promotion if new |
| `thought2build_billing_credits_critic_regen_total` spikes | Critic repair loops are consuming extra regeneration credits | Compare provider latency/errors, recent prompt changes, and validator failures |
| `pipeline_upstream_section_skipped_total` increases | The prompt builder could not find expected upstream context | Inspect the upstream stage for renamed/missing headings; check whether a prompt or parser change caused drift |
| Prompt eval CI fails | New prompt behavior regressed against baseline | Do not merge until the prompt is fixed or the regression has explicit release-owner acceptance |

---

## 11. Storyboard Operations

Storyboard generation creates a paid, versioned keynote artifact from finalised
SPEC, PLAN, HARNESS, and TASKS sources. Operators should treat Storyboard
failures like credit-affecting generation incidents: full generation and full
regeneration cost 25 credits, section regeneration costs 5 credits, and every
failed paid attempt must refund exactly once.

**Primary modules:** `backend/services/pipeline/storyboard_service.py`,
`backend/services/pipeline/storyboard_public_service.py`,
`backend/services/pipeline/storyboard_renderer.py`, and
`backend/routers/storyboards.py`.

### Fast Triage Signals

| Signal | Meaning | First action |
|---|---|---|
| `storyboard.generate_failed` log | Generation failed after a credit debit | Verify the refund ledger entry exists exactly once |
| Storyboard row stuck in `generating` | Worker died or provider call hung after reservation | Run stuck-job recovery checks below |
| `thought2build_storyboard_generation_failed_total` spike | Provider, timeout, parser, or schema failures increased | Split by `action` and `error_type`; inspect provider health |
| `thought2build_storyboard_credits_refunded_total` spike | Failures are refunding credits | Confirm refunds match failed Storyboard rows one-for-one |
| Ready Storyboard becomes `stale` | A source stage was refinalised | Ask owner to regenerate when they need the latest source versions |
| Public leakage report | A `/sb/` link may expose gated content | Disable, rotate, preserve evidence, and verify default privacy |

### Generation Failure And Refund Verification

Use this when a paid Storyboard attempt fails or a user reports missing credits.

1. Find the failed Storyboard row.

   ```sql
   SELECT id, workspace_id, user_id, version, status, credit_ledger_id,
          created_at, updated_at
   FROM storyboards
   WHERE id = '<storyboard_uuid>';
   ```

   Expected: `status = 'failed'` and `credit_ledger_id` is non-null for a paid
   failed attempt.

2. Confirm the failure metric and content-free log exist.

   ```promql
   increase(thought2build_storyboard_generation_failed_total[30m])
   ```

   Search logs for `storyboard.generate_failed` and the `storyboard_id`. Logs
   must not contain `speaker_notes_md`, `technical_appendix_md`,
   `demo_script_md`, `content_json`, raw prompts, or source excerpts.

3. Verify the original debit and exactly one refund.

   ```sql
   WITH failed AS (
     SELECT user_id, credit_ledger_id
     FROM storyboards
     WHERE id = '<storyboard_uuid>'
   )
   SELECT cl.id, cl.amount, cl.reason, cl.created_at
   FROM credit_ledger cl
   JOIN failed f ON cl.user_id = f.user_id
   WHERE cl.id = f.credit_ledger_id
      OR cl.reason = 'refund:' || f.credit_ledger_id::text
   ORDER BY cl.created_at;
   ```

   Expected for full generation/regeneration: one `-25` debit and one `+25`
   refund. Expected for section regeneration: one `-5` debit and one `+5`
   refund.

   ```sql
   SELECT COUNT(*) AS refund_rows
   FROM credit_ledger
   WHERE user_id = '<user_uuid>'
     AND reason = 'refund:<credit_ledger_uuid>';
   ```

   Expected: `refund_rows = 1`. The unique refund reason makes retries
   idempotent; a count above one is a P1 credit-accounting incident.

4. Confirm the refund metric moved by the same credit amount.

   ```promql
   increase(thought2build_storyboard_credits_refunded_total[30m])
   ```

   The `reason` label is `generation_failed` for direct LLM/schema/provider
   failures and `stuck_recovery` for recovery of old `generating` rows.

5. If the ledger is correct but the UI balance is stale, clear the user's
   credit cache by restarting the affected worker or by exercising a balance
   read after the service has invalidated `credits:<user_id>`. Do not create a
   manual refund unless the SQL above proves the refund is missing.

### Stuck `generating` Job Recovery

Storyboard reservations intentionally create a placeholder row before the LLM
call. Recovery handles rows left in `generating` for more than 30 minutes.

1. Identify old placeholders.

   ```sql
   SELECT id, workspace_id, user_id, version, credit_ledger_id, created_at,
          updated_at
   FROM storyboards
   WHERE status = 'generating'
     AND updated_at < now() - interval '30 minutes'
   ORDER BY updated_at;
   ```

2. Confirm the recovery loop is running. The normal recovery path invokes
   Storyboard recovery from the backend recovery service; look for
   `storyboard.recovered_stuck` or `storyboard.generate_failed` events.

3. In a one-off staging shell, operators can run the service helper directly:

   ```bash
   cd backend
   uv run python - <<'PY'
   import asyncio
   from database import AsyncSessionLocal
   from services.pipeline.storyboard_service import recover_stuck_storyboards

   async def main() -> None:
       async with AsyncSessionLocal() as db:
           recovered = await recover_stuck_storyboards(db)
           print(recovered)

   asyncio.run(main())
   PY
   ```

4. Re-run the refund verification query for every recovered row. Expected:
   status is `failed`, previous ready Storyboard versions remain presentable,
   and any `credit_ledger_id` has exactly one matching `refund:<ledger_id>` row.

### Stale Storyboard Recovery

When a source stage is refinalised, ready Storyboards for that workspace are
marked `stale`. The stale deck remains presentable because it is pinned to
immutable `source_stage_version_ids`; do not delete or mutate it in place.

1. Confirm the source stage was refinalised after Storyboard generation.

   ```sql
   SELECT id, stage_type, status, updated_at
   FROM stages
   WHERE workspace_id = '<workspace_uuid>'
   ORDER BY stage_type;
   ```

2. Confirm affected Storyboards are stale.

   ```sql
   SELECT id, version, status, source_stage_version_ids, updated_at
   FROM storyboards
   WHERE workspace_id = '<workspace_uuid>'
   ORDER BY version DESC;
   ```

3. Tell the owner the stale Storyboard is still safe to present, but regeneration
   is required to build a new version from the latest SPEC, PLAN, HARNESS, and
   TASKS versions. If regeneration fails, the previous ready/stale version must
   remain the latest presentable deck.

### Public Slug Disable And Rotation

Public Storyboard links are independent from workspace `/p/` public links. The
public route is `/sb/{slug}` in the frontend and `/storyboards/public/{slug}` in
the backend.

To stop a public link without changing the slug:

```bash
export API_URL=https://api.example.com
export OWNER_ACCESS_TOKEN=replace-with-owner-access-token
export STORYBOARD_ID=replace-with-storyboard-uuid
curl -X DELETE \
  -H "Authorization: Bearer $OWNER_ACCESS_TOKEN" \
  "$API_URL/storyboards/$STORYBOARD_ID/share"
```

Expected: `204 No Content`; the old `/sb/{slug}` returns not found.

To retire a known slug permanently and issue a new one:

```bash
curl -X POST \
  -H "Authorization: Bearer $OWNER_ACCESS_TOKEN" \
  "$API_URL/storyboards/$STORYBOARD_ID/share/rotate"
```

Expected: response contains a new `/sb/{slug}` URL; the old slug returns not
found immediately.

To re-enable sharing with explicit permissions:

```bash
curl -X POST \
  -H "Authorization: Bearer $OWNER_ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "allow_pdf_download": true,
    "allow_notes_download": false,
    "allow_appendix_download": false,
    "allow_source_layer": false
  }' \
  "$API_URL/storyboards/$STORYBOARD_ID/share"
```

Default public privacy is PDF plus demo-script only; speaker notes, appendix,
and source excerpts are hidden until the owner enables the matching permission.

### Public Data Leakage Incident Checklist

1. Disable the public Storyboard share immediately.
2. Rotate the slug so any copied URL is retired permanently.
3. Preserve the suspected slug, Storyboard ID, timestamps, request logs, and
   response samples. Do not paste generated deck content into tickets; attach it
   only in the approved incident store.
4. Verify the public response is allow-list based:

   ```bash
   export API_URL=https://api.example.com
   export STORYBOARD_SLUG=replace-with-public-slug
   curl -s "$API_URL/storyboards/public/$STORYBOARD_SLUG" | \
     python3 -m json.tool
   ```

   The response must not contain account email, user ID, workspace ID, credit
   balance, billing history, previous versions, `credit_ledger_id`, raw prompts,
   or `source_stage_version_ids`.

5. Verify default gates are closed: notes, appendix, and source excerpts are
   redacted unless the owner enabled `allow_notes_download`,
   `allow_appendix_download`, or `allow_source_layer`.
6. Confirm privacy headers on both success and not-found responses:

   ```bash
   curl -i "$API_URL/storyboards/public/$STORYBOARD_SLUG"
   ```

   Expected: `X-Robots-Tag: noindex, nofollow`, `Cache-Control: no-store,
   private`, `X-Content-Type-Options: nosniff`, and a CSP with
   `frame-ancestors 'none'`.
7. File a security incident if any private field or gated content is exposed by
   default. Keep sharing disabled until the fix, tests, and release gate pass.

---

## 12. GitHub Living Integration — App, Worker & Webhook Ops

Operational procedures for the Phase 21 GitHub Living System of Record: the
Thought2Build **GitHub App** identity, the durable **arq worker** that runs all
GitHub I/O off the request path, and the signature-verified **webhook** that
flows repository events back into Thought2Build.

**Architecture recap (where things run):**

- **API process** (`web` in `Procfile`, `api` in `docker-compose.yml`) accepts
  the export/sync/increment requests, owns migrations, and **enqueues** jobs —
  it never blocks on GitHub.
- **Worker processes** drain separate shared-Redis lanes: the bulk worker
  (`arq worker.WorkerSettings`) runs `export_push`, non-issue
  `reconcile_event`, periodic `backfill_repo`, increments/projects, and the
  `reconcile_drift` cron; the required fast worker
  (`arq worker.FastWorkerSettings`) runs `reconcile_issue_event`, user-requested
  `refresh_task_states`, `pr_check`, and billing jobs. This keeps issue closures
  and explicit checks out of a long export backlog.
- **Config:** the App is enabled when `GITHUB_APP_ID` + `GITHUB_APP_SLUG` are
  set. In production, `validate_production_settings()` additionally requires
  `GITHUB_APP_PRIVATE_KEY` and `GITHUB_APP_WEBHOOK_SECRET` (see `config.py`).
  The private key lives in the secret manager — **never** in the DB.

**Endpoints (all on already-registered routers):**

- `POST /integrations/github/webhook` — inbound GitHub deliveries (HMAC-verified,
  CSRF- and rate-limit-exempt).
- `GET /workspaces/{id}/sync` — live task-completion + drift state.
- `POST /workspaces/{id}/sync/resync` — re-push changed tasks' issues (202).
- `POST /workspaces/{id}/sync/backfill` — recover missed events (202).
- `POST /workspaces/{id}/export/github` — enqueue an export (202).
- `GET|POST /workspaces/{id}/increments`, `GET|POST /workspaces/{id}/ideas`.

---

### §12.1 — GitHub App Private-Key Rotation

**Impact:** `GITHUB_APP_PRIVATE_KEY` is the RS256 PEM that signs the short-lived
App JWT (`iss = GITHUB_APP_ID`, `exp ≤ 600s`). The JWT is exchanged for
per-installation access tokens. Rotating it invalidates the old key for **new**
JWT signing immediately; already-minted installation tokens keep working until
their own (short) TTL expires. There is no two-key window for the App private
key — GitHub holds the matching public key, so the new key is live the moment
you generate it on GitHub.

**Rotation steps:**

1. In the GitHub App settings (`https://github.com/settings/apps/<slug>` or the
   org equivalent), generate a **new** private key. GitHub lets multiple keys
   coexist, so generate before deleting.
2. Store the new PEM in the secret manager and update `GITHUB_APP_PRIVATE_KEY`
   in the Railway environment (both the `web` and `worker` services share it).
3. Redeploy. The next App-JWT mint uses the new key. Confirm token mints still
   succeed:

   ```bash
   # thought2build_github_token_mint_total should keep incrementing with no rise in
   # webhook_failed / 401-driven re-mints.
   curl -s -H "Authorization: Bearer $METRICS_TOKEN" "$API_URL/metrics" \
     | grep -E 'thought2build_github_token_mint_total'
   ```

4. Once mints are healthy, **delete the old key** in the GitHub App settings.

**Verification:** trigger any sync (`POST /workspaces/{id}/sync/backfill`) and
confirm the worker logs `github.token.minted` (never the token value) and the
job completes.

**Rollback:** the old key is still valid on GitHub until you delete it in step 4
— revert `GITHUB_APP_PRIVATE_KEY` to the previous PEM and redeploy.

---

### §12.2 — Webhook Secret Rotation (Two-Secret Window)

**Impact:** `GITHUB_APP_WEBHOOK_SECRET` signs the `X-Hub-Signature-256` HMAC on
every inbound delivery. The webhook handler verifies the signature **before**
any DB/queue work, in constant time, against **both** the current and previous
secret so a rotation never drops deliveries.

The accepted-secrets list is `settings.github_app_webhook_secrets` =
`[GITHUB_APP_WEBHOOK_SECRET, GITHUB_APP_WEBHOOK_SECRET_PREV]` (empty entries are
ignored). This is the two-secret window.

**Rotation steps:**

1. Generate a new secret:

   ```bash
   python -c "import secrets; print(secrets.token_hex(32))"
   ```

2. Move the **current** secret into the previous slot and set the new one as
   current, then redeploy `web` (the worker does not verify signatures):

   ```
   GITHUB_APP_WEBHOOK_SECRET=<new_secret>          # signs nothing yet — accepted
   GITHUB_APP_WEBHOOK_SECRET_PREV=<old_secret>     # still accepted during window
   ```

3. Update the **GitHub App's** webhook secret to `<new_secret>`. From this
   moment GitHub signs with the new secret; Thought2Build accepts both, so no
   delivery is rejected.
4. Watch the verify/fail metrics through at least one delivery cycle:

   ```bash
   curl -s -H "Authorization: Bearer $METRICS_TOKEN" "$API_URL/metrics" \
     | grep -E 'thought2build_github_webhook_(verified|failed)_total'
   ```

   `verified` should keep climbing; `failed` must stay flat. A spike in
   `failed` means GitHub is still signing with the old secret or the env was
   mis-set — do **not** remove `*_PREV` yet.
5. After the window (recommend ≥ 24h, or once you have confirmed deliveries
   verifying against the new secret), clear the previous slot and redeploy:

   ```
   GITHUB_APP_WEBHOOK_SECRET_PREV=
   ```

**Rollback:** if `failed` spikes, set `GITHUB_APP_WEBHOOK_SECRET` back to the
old value and revert the GitHub App secret; the previous slot already accepts
it, so recovery is immediate.

---

### §12.3 — Installation-Token Re-Mint

Installation tokens are short-TTL, namespaced, never logged, and Fernet-encrypted
at rest when Redis is shared. The client resolves a token **per request** via the
Redis-cached `TokenProvider` and **re-mints once on a 401** automatically — no
operator action is needed for the normal expiry path.

**Force a re-mint** (e.g. after a suspected cache poisoning, or a manual GitHub
permission change):

```bash
# The token cache key is namespaced per installation (gh:inst_token:{id}) and
# Fernet-encrypted at rest. Drop it so the next call re-mints. (Never print the
# value — it is an installation credential.)
redis-cli --scan --pattern 'gh:inst_token:*' | xargs -r redis-cli del
```

Then trigger any sync; confirm `thought2build_github_token_mint_total` increments
(its `source` label distinguishes a cache hit from a fresh mint) and the job
succeeds. A persistent re-mint storm (many mints per
minute, rising `webhook_failed`/job retries) points at a **revoked installation
or an invalid App private key** — check §12.1 and the install's status on GitHub.

---

### §12.4 — Dead-Letter Inspection & Manual Replay

Every GitHub job runs with bounded retries + exponential backoff + jitter. After
the max-attempt cap it is **dead-lettered** (counted by
`thought2build_github_job_deadlettered_total`) and alerts fire. Jobs are idempotent
and checkpointed (inbound keyed by `X-GitHub-Delivery`, outbound by
`push_id`/`increment_id`), so a replay never duplicates side effects.

**Alert trigger:** `increase(thought2build_github_job_deadlettered_total[15m]) > 0`.

**Inspect the dead-letter queue (arq stores results/failures in Redis):**

```bash
# List arq result keys; failed jobs carry the exception in their result blob.
redis-cli --scan --pattern 'arq:result:*' | head -50

# Inspect one job's stored result (job_id == push_id for export jobs):
redis-cli get "arq:result:<job_id>"
```

**Manual replay** — re-enqueue the same job by its stable key. Because the job_id
is the `push_id`/`increment_id`/`delivery_id`, re-submitting dedups and resumes
from the last completed checkpoint:

```bash
cd backend
# Re-enqueue an export/resync by push_id:
uv run python -c "
import asyncio
from services.queue import enqueue
asyncio.run(enqueue('export_push', '<push_id>', '<repo_name>', 'private', job_id='<push_id>'))
"
```

For an inbound delivery, prefer **backfill** (§12.6) over replaying a raw webhook
— backfill reconstructs state from the issues API and is out-of-order safe.

**After replay:** confirm the push/issue reaches `completed`/`done` and the
dead-letter counter stops rising.

---

### §12.5 — "Sync Paused" / Circuit-Breaker Recovery

The shared httpx client has bounded timeouts, a bounded pool, and a **circuit
breaker** that trips on a sustained GitHub outage. While open, jobs raise
`GitHubUnavailableError`, the push stays **live** (non-`failed`, so
`find_live_push` still sees it), the worker requeues, and the UI surfaces
**"Sync paused — reconnect GitHub"**. This is *not* a failure state — no push is
marked `failed` and no credit is lost.

**Diagnose:**

```bash
# Throttle/breaker pressure shows up here:
curl -s -H "Authorization: Bearer $METRICS_TOKEN" "$API_URL/metrics" \
  | grep -E 'thought2build_github_throttled_total|thought2build_github_queue_depth'

# Worker logs the breaker-open event:
#   github.sync.paused  (and the LLM-style breaker open/close transitions)
```

**Recovery is automatic** once GitHub returns: the breaker half-opens, a probe
succeeds, and queued jobs drain. Operator actions:

1. Confirm GitHub itself is healthy (`https://www.githubstatus.com`).
2. If the queue is backing up, confirm the worker process is alive
   (`Procfile` `worker`; `docker compose ps worker`) and Redis is reachable.
3. If a single installation is the cause (its `Retry-After` keeps deferring),
   the per-installation governor is doing its job — fairness/bulkheads keep
   other tenants flowing; no action needed beyond monitoring.

Do **not** manually mark pushes `failed` to "clear" a paused state — that drops
the live push and breaks resume. Let the breaker and requeue recover.

---

### §12.6 — Backfill Trigger (Recover Missed Events)

Backfill reconciles a repo's task states from GitHub's **issues API**
(`issues?state=all&since=`), filtering out rows carrying a `pull_request` key. It
recovers closures/reopens missed while the worker was down and is idempotent with
the webhook path (a webhook-set `pr_merge` is never downgraded to `manual`).
Transitions are gated on event timestamp, so backfill is out-of-order safe.

**Trigger for one workspace (returns 202, runs on the worker):**

```bash
curl -i -X POST "$API_URL/workspaces/$WORKSPACE_ID/sync/backfill" \
  -H "Authorization: Bearer $ACCESS_TOKEN" -H "X-CSRF-Token: $CSRF"
```

The periodic `reconcile_drift` cron also enqueues `backfill_repo` for every
`completed` push and **fails stuck `pending` pushes whose arq job no longer
exists** (a crashed export), so the repo becomes re-exportable. To force the
sweep immediately:

```bash
cd backend
uv run python -c "
import asyncio
from services.queue import enqueue
asyncio.run(enqueue('reconcile_drift', job_id='reconcile_drift:manual'))
"
```

**Verify:** `GET /workspaces/{id}/sync` reflects the corrected task states and
`thought2build_github_reconcile_lag_seconds` returns to baseline.

---

### §12.7 — Increment-Push Troubleshooting

An increment is an additive delta on top of the shipped baseline. `increment_push`
creates **new issues only** for new tasks (content-derived `task_ref` dedups —
same ref → same issue, no duplicate), updates changed tasks' issues, closes
obsoleted issues with a note, and creates one milestone + one PR per increment.

**Common symptoms & fixes:**

| Symptom | Likely cause | Action |
|---|---|---|
| Increment stuck in `pending` | worker down or job dead-lettered | §12.4 — inspect & replay `increment_push <increment_id>` (idempotent) |
| Duplicate issues for the "same" task | `task_ref` churn (title changed) | Expected if the task content genuinely changed; otherwise check `compute_task_ref` inputs — re-export updates in place by `task_ref` |
| New issues land outside the milestone | milestone create raced/failed | Replay the job; milestone creation is idempotent and re-links existing issues |
| `409` on a content write | concurrent write / stale SHA | Handled automatically (refetch-SHA-and-retry) + per-repo write serialization; a persistent 409 means another writer — confirm only the worker writes |
| `403` on `.github/workflows/*` | App lacks `Workflows: write` | Surfaced as a distinct actionable error; grant the permission on the installation and re-run |

**Inspect an increment's push ledger:**

```sql
SELECT ip.id, ip.status, ip.increment_id, ip.branch_name, ip.pr_number
FROM integration_pushes ip
WHERE ip.increment_id = '<increment_id>';

SELECT task_ref, state, external_issue_number, done_via
FROM integration_push_tasks
WHERE increment_id = '<increment_id>'
ORDER BY external_issue_number;
```

Re-running the push after any fix is always safe — the job resumes from the
ledger and never re-creates an issue already recorded in
`integration_push_tasks`.

---

## 13. Demo Day Mode — Construction Verdict & Rollout

Demo Day mode (`docs/DEMO_DAY_MODE_IMPLEMENTATION_PLAN.md`) is a generation
profile that produces a rubric-shaped Spec/Plan/Harness/Tasks package plus a
**construction-verified** agent handoff bundle. It is gated by two flags that
**ship off** and are flipped on **only after** the live golden-corpus gate
(below) clears:

- `demo_day_mode_enabled` (`backend/config.py`, default `False`) — server gate.
  While `False`, `WorkspaceCreate` forces `mode="standard"` and the whole feature
  is dormant; every standard path stays byte-identical (the §4 regression pin).
- `VITE_DEMO_DAY_MODE` (frontend build flag, default off) — gates only the
  **creation** selector in `CreateWorkspaceModal`. The construction badge and
  handoff panel render data-driven on `workspace.mode == "demo_day"`, so a
  workspace already created in Demo Day mode keeps its handoff UI even if the
  build flag is off.

### §13.1 The construction verdict (what it is, where it lives)

The verdict is **not** an `EvalResult` row and **not** a `Stage.quality_gate`
finding. It is a workspace-level JSONB blob persisted on
**`workspaces.construction_verdict`** (migration `0030`), shaped like
`ConstructionVerdict.to_dict()`:

```json
{
  "verified": true,
  "checks": { "C1": {"name": "dag_acyclic", "passed": true, "gaps": []}, "...": {} },
  "estimated_minutes": 240,
  "time_budget_minutes": 300,
  "stage_versions": {"spec": 1, "plan": 1, "harness": 1, "tasks": 1}
}
```

`verified` is **C1–C4 only** (C5/time-budget is advisory and never flips it). The
verifier is **zero-LLM** (`services/pipeline/demo_day_plan_linter.py`) and runs as
a detached background task off the tasks stage, exactly like the async-advisory
critic. It is **advisory** — it never blocks finalise or export.

**Inspect a workspace's verdict:**

```sql
SELECT id, mode, target_agent, time_budget_minutes,
       construction_verdict->>'verified'        AS verified,
       construction_verdict->'stage_versions'   AS stamped_versions
FROM workspaces
WHERE id = '<workspace_id>';
```

### §13.2 Staleness & the on-demand re-run

The verdict stamps each stage's version. If any stage is regenerated/refined
afterward, the stamped `stage_versions` no longer match the live
`stages.current_version` and the verdict is **stale** (`is_verdict_stale`). A
stale verdict must never read as a green "verified" — the UI shows "out of date".
There is **no separate re-run endpoint**: the export path recomputes a stale
verdict synchronously (`construction_verdict_service.ensure_fresh_verdict`, fail-open) before
rendering `CONSTRUCTION_REPORT.md`, and the frontend re-fetches the workspace
after a handoff-bundle download to pick up the refreshed verdict. To force a
recompute operationally, trigger an export (ZIP) for the workspace.

**This works for BOTH modes.** It briefly did not: when the verifier was extended
to standard mode, the report write and its `ensure_fresh_verdict` call were left
behind a `mode == "demo_day"` gate, so a standard workspace had no refresh path at
all — a verdict went stale on the first refine and stayed "out of date" forever,
and the ZIP carried no report to explain the badge's gap count. The gate is gone
from both the ZIP export and all three GitHub push paths. If you ever see a
standard verdict that will not refresh, check that gate first.

### §13.3 Advisory-only construction verification

A failing verdict is persisted with each gap named. It never starts an LLM
request or mutates a stage after that stage's durable run has succeeded. Owners
may use the normal Regenerate action, which creates a fresh generation run with
the same deadline, checkpointing, cancellation, and credit guarantees as every
other generation. Demo Day costs the same per stage as standard; the manual and
zero-LLM verifier are free.

### §13.4 The live golden-corpus gate (the flag-flip is a manual step)

**Flipping `demo_day_mode_enabled` + `VITE_DEMO_DAY_MODE` on is a manual
post-gate step — never automate it.** Per `docs/evals/ROUTE_PROMOTION.md`, any
change to which artifact is produced rides the golden corpus. The Demo Day
problem-statement corpus is `docs/evals/golden_prompts/demo_day_route_golden.json`
(zero-provisioning bias so the e2e survives the handoff). The promotion gate:

1. Generate the four stages from each corpus problem statement on the candidate
   prompts (the live run — `scripts/run_llm_route_eval.py` drives the routing/
   trait checks; quality is judged live).
2. Assert the construction verifier **passes** on each generated package (the
   offline pass-criterion proof lives in `tests/test_demo_day_phase3.py`; the
   corpus is bound to it by `tests/test_demo_day_phase5_corpus.py`).
3. Only then flip `demo_day_mode_enabled=true` (backend) and
   `VITE_DEMO_DAY_MODE=true` (frontend build) and redeploy.

To roll back, set both flags off and redeploy — existing Demo Day workspaces keep
their data (the columns persist) but new workspaces fall back to standard.

### §13.5 The verifier now covers BOTH modes — and its enforcement flags

The construction verifier is no longer Demo-Day-only. `mode` decides *which*
linter runs, not *whether* one runs:

| Mode | Linter | Checks |
|---|---|---|
| `demo_day` | `demo_day_plan_linter` | C1 dag_acyclic · C2 task_to_test · C3 ac_to_test · C4 e2e_reachable · **C5 time_budget (advisory)** · **C6 plan_coverage** · **C7 task_inventory (advisory)** |
| `standard` | `standard_plan_linter` | C1 requirement_coverage · C2 test_coverage · C3 dag_acyclic · C4 plan_coverage · C5 e2e_reachable · **C6 task_inventory (advisory)** |

Both produce the same `ConstructionVerdict` payload, so one JSONB column, one
`CONSTRUCTION_REPORT.md` writer, and one badge serve both.

Each check carries **two independent flags** in the payload, and the difference
is operationally important:

| Flag | Meaning | Counted as a gap? | Can withhold `verified`? |
|---|---|---|---|
| `advisory: true` | never verdict-bearing *by nature* (`time_budget`, `task_inventory`) | no | no |
| `enforced: false` | a REAL gap whose rollout flag is still off | **yes** | no |
| neither | live and enforced | yes | yes |

The frontend filters on `advisory` alone, so an un-enforced failure is still shown
and still counted by the badge — the flags govern the `verified` boolean, **not**
disclosure. Old verdicts carrying neither field fall back to the legacy `C1`–`C4`
set, which is what makes the frontend and backend deploys order-independent.

> Do not "fix" a red badge by setting `advisory` on a check. That demotes a real
> structural failure to a calibration note and makes the report print
> "✅ Construction-verified" above a list of FAILs — the exact overclaim the
> verdict exists to prevent (pinned by
> `test_unenforced_report_never_claims_a_guarantee_it_has_not_earned`).

**Two enforcement flags** (`backend/config.py`, both default `false`):

- `demo_day_plan_coverage_enforced` — lets Demo Day C6 flip `verified`.
- `standard_construction_verifier_enforced` — lets standard C1–C5 flip
  `verified`. While `false` every check still runs, reports its gaps, and is
  shown; it just cannot withhold the verdict.

They ship off on purpose: the checks join on `Plan refs` / `Harness refs`
citations, and the tasks-prompt edits that mandate them (`tasks-v7`,
Demo Day `tasks-v3`) ride the golden-corpus gate above. Flip after that release
clears: env-only, reversible, no redeploy.

**Join-key note.** C2 `test_coverage` and C5 `e2e_reachable` read
`artifact_validator.harness_test_index` — the multi-language scanner shared with
the online-eval task validator (Python/pytest, Go, TS/JS Vitest-Jest-Mocha,
RSpec). Before that it used the Python-only `_harness_test_refs`, so **every
non-Python harness parsed as zero tests and C2 hard-failed**. A stack the scanner
cannot parse now reports "unverified", never "failed". If C2 starts failing
broadly after a harness-prompt change, check whether the emitted test shape is one
the scanner models before assuming a construction regression.

**Operational safety.** Neither flag can block, slow, or fail a generation. The
verifier is scheduled *after* the tasks artifact is delivered and charged
(`_schedule_construction_verifier`), runs detached in the bounded background
registry under the shared advisory semaphore, opens its own short-lived session,
and is exception-wrapped. On the export path `ensure_fresh_verdict` is fail-open
and falls back to the persisted verdict. **There is no backfill:** a verdict is
recomputed only when a stage version actually moves, so adding a check never
silently turns an existing verified package red.

---

## 14. Generation Admission Control & Provider Budget (Scalability P0)

The P0 remediation of `docs/SCALABILITY_AUDIT.md`. Three layers protect the core
**generate / regenerate** path from a concurrent-generation burst, plus 429-aware
backoff so a throttled provider key degrades gracefully instead of amplifying.

### What it is

- **Admission control** (`services/pipeline/admission.py`) — before any provider
  call (and after the generation-cache miss, so a cache hit consumes no slot), a
  generation must acquire a slot across three budgets, in order:
  1. **Per-process** (`MAX_CONCURRENT_GENERATIONS_PER_PROCESS`, default 4) —
     in-memory; the primary valve so one worker cannot self-immolate.
  2. **Per-user** (`MAX_CONCURRENT_GENERATIONS_PER_USER`, default 3) — a
     self-healing Redis lease (TTL `GENERATION_ADMISSION_LEASE_TTL_SECONDS`), so
     it holds across instances and recovers if a process dies.
  3. **Per-provider** (`PROVIDER_MAX_INFLIGHT_GENERATIONS` /
     `PROVIDER_MAX_GENERATIONS_PER_MINUTE`) — global Redis budget against the
     shared platform key. In-flight defaults to **4**; RPM defaults to **0 =
     unlimited** until the account limit is measured. The API transfers this
     lease with the queued job, so queued and running paid generations both
     occupy capacity and excess work is rejected before charging.
  Over budget ⇒ fast-fail carrying `Retry-After`. **Redis budgets fail OPEN** (a
  Redis blip never blocks a generation; the per-process limiter still applies).
  **Scope:** these budgets count the **core generate/regenerate path only**.
  Storyboard / increment / harness gap-patch hit the same key but are governed by
  their own rate tiers and are NOT counted here — `PROVIDER_MAX_*` is a
  core-generation budget, not a whole-account budget.
- **Provider-call bulkhead** (`MAX_CONCURRENT_PROVIDER_CALLS_PER_PROCESS`,
  default 8) — caps actual worker-local model calls. This is distinct from
  generation admission because one artifact can fan out several chunk calls.
- **HTTP-native generation tier** (`middleware/rate_limit.py`,
  `GENERATION_RATE_PER_MINUTE` / `_WINDOW_SECONDS`) — a true **429 + Retry-After**
  at the edge for `/stages/{id}/(generate|regenerate|regenerate-gaps)`, before the
  stream starts. (Admission rejections, which happen inside the already-200 SSE
  stream, instead surface as the existing `rate_limit_exceeded` **SSE event**.)
- **429-aware backoff** (`services/pipeline/stage_manager.py`) — a provider
  429/529/503 is a *throughput* failure, retried **in place on the same tier**
  (honor `Retry-After` → exponential backoff + jitter, bounded by
  `PROVIDER_RATE_LIMIT_MAX_RETRIES`), never escalated within that throttled
  provider. After bounded retries are exhausted, generation moves to a different
  configured provider when one is healthy. The
  circuit breaker excludes rate-limits, so a 429 cannot open the circuit and then
  hard-fail its own backoff retry.

### Sizing the provider budget (measure, don't guess)

The in-flight ceiling ships conservatively at 4; the RPM ceiling ships at 0
because the binding per-org rate limit is account-specific. Before tuning them:

1. Get the **per-org RPM / TPM / concurrent-request** limits for each provider
   account (Anthropic/OpenAI/Google dashboards or support).
2. Watch `thought2build_llm_provider_rate_limited_total` under real load — a non-zero,
   rising rate means the shared key is already at its ceiling.
3. Set `PROVIDER_MAX_GENERATIONS_PER_MINUTE` / `_INFLIGHT_GENERATIONS` a margin
   **below** the measured org limit (account for the per-generation fan-out: each
   user generation also drives a critic/judge call, and the judge defaults to
   Anthropic for all providers, so Anthropic is the hottest).
4. Roll out per provider; confirm over-budget requests shed as 503/SSE
   `rate_limit_exceeded` (not 5xx/hangs) and that `/health` + login p99 stay flat.

### Detecting & metrics

```promql
# Admission rejections by the budget that tripped (process/user/provider_*):
sum by (reason) (rate(thought2build_generation_admission_rejected_total[5m]))

# Per-process in-flight generations on a worker (a non-zero idle floor = slot leak):
thought2build_generation_inflight_process

# Provider throttling — the binding-constraint signal (the shared key is at its ceiling):
sum by (provider) (rate(thought2build_llm_provider_rate_limited_total[5m]))

# 429-aware retries; "exhausted" means the local retry budget was used before
# alternate-provider recovery or terminal failure:
sum by (provider, outcome) (rate(thought2build_pipeline_provider_rate_limit_retries_total[5m]))
```

**Alert ideas:** sustained `provider_rate_limited` rate > 0 (key at ceiling →
size/raise the budget or add capacity); `generation_inflight_process` pinned at
the cap for minutes (turn up `MAX_CONCURRENT_GENERATIONS_PER_PROCESS` only if the
provider budget allows); a non-zero `inflight_process` floor at idle (slot leak —
investigate the pipeline `finally` release).

### Tuning & safe rollback

Every knob is env-driven (`config.py`) and independently disable-able with **0**:
`MAX_CONCURRENT_GENERATIONS_PER_PROCESS=0` (disable the per-process valve),
`MAX_CONCURRENT_GENERATIONS_PER_USER=0`, `GENERATION_RATE_PER_MINUTE=0` (disable
the HTTP tier), `PROVIDER_MAX_*=0` (unlimited). Keep
`MAX_CONCURRENT_PROVIDER_CALLS_PER_PROCESS` positive; lower it to reduce upstream
pressure. The 429-aware backoff is always on
(it has no off switch — it only ever *helps* under throttling); to make it less
aggressive lower `PROVIDER_RATE_LIMIT_MAX_RETRIES`. No migration or restart
ordering is required — these take effect on the next process start.

### F8/F9 — pool guards (related)

The same P0 cut bounds the shared Redis pool (`REDIS_MAX_CONNECTIONS`,
`REDIS_HEALTH_CHECK_INTERVAL`, built once in `database.build_redis_client`) and
adds postgres fast-fail/server guards (`DB_POOL_TIMEOUT` default 5s,
`DB_STATEMENT_TIMEOUT_MS`, `DB_IDLE_IN_TRANSACTION_TIMEOUT_MS`). If a long-running
worker job legitimately holds a read transaction across external calls and trips
`idle_in_transaction_session_timeout`, raise `DB_IDLE_IN_TRANSACTION_TIMEOUT_MS`
(the API generation path commits before its stream, so it is never affected).

---

## 15. Horizontal Scale-Out — Pooler, Workers & Pool Metrics (Scalability P1)

**Reference:** `docs/SCALABILITY_AUDIT.md` §F3/§F4/§F6
**Modules:** `backend/database.py`, `backend/gunicorn.conf.py`,
`backend/services/observability.py`, `deploy/pgbouncer/`

### What it is

P1 unblocks running **N API + worker instances** without exhausting Postgres
`max_connections`. Three levers:

- **F3 — transaction pooler (PgBouncer).** Each API/worker process holds up to
  `DB_POOL_SIZE`+`DB_MAX_OVERFLOW` (=30) connections; 2 API workers + 3 worker
  lanes can already crowd a default managed-Postgres limit of 100 **before**
  any scale-out. A transaction-mode pooler multiplexes all app connections onto a
  small fixed server pool, so the Postgres connection count stays **flat**
  regardless of instance count. It is compatible because the generation pipeline
  releases its connection during the stream (audit §1).
- **F4 — `WEB_CONCURRENCY`-driven API workers** (`gunicorn.conf.py`).
- **F6 — bounded background-task fan-out** (see also §16).

### ⚠️ BEFORE DEPLOYING P1 — connection-footprint prerequisite

F5 adds a **second worker process** (the fast lane, §16). That is unavoidable —
a separate process is the whole point of the bulkhead — and it raises the
Postgres connection footprint regardless of `WEB_CONCURRENCY`. Worked example
at the defaults: 2 API workers + bulk worker + fast worker, each up to
`DB_POOL_SIZE`+`DB_MAX_OVERFLOW` (=30) ⇒ **~120 peak vs a default
`max_connections` of 100.** Do **one** of these before shipping F5, or a busy
fleet will hit `FATAL: too many connections`:

1. **Preferred — put the pooler in front** (enable `DB_TRANSACTION_POOLER_MODE`
   with a real transaction-mode pooler, below). Postgres connections then stay
   flat regardless of process/instance count.
2. **Or confirm headroom** — verify the managed Postgres `max_connections` is
   comfortably above the peak above (raise it, or it already is).
3. **Or shrink per-process pools** — lower `DB_POOL_SIZE`/`DB_MAX_OVERFLOW` on the
   worker services (set them per-service in the deploy env). The fast lane
   already runs a smaller `max_jobs` (10) since it does light work, so its real
   peak draw is well under a full pool.

### F3 — turning on the pooler

1. Stand up a transaction-mode pooler. Locally:
   `docker compose --profile pgbouncer up -d pgbouncer` (port 6432). The
   production reference config is `deploy/pgbouncer/pgbouncer.ini`
   (`pool_mode = transaction`, `ignore_startup_parameters` for asyncpg, and
   `server_reset_query = DISCARD ALL` with `server_reset_query_always = 1`).
2. Point the app at it and flip the flag:
   `DATABASE_URL=…@<pooler-host>:6432/thought2build` and
   `DB_TRANSACTION_POOLER_MODE=1`. The flag disables SQLAlchemy's **and**
   asyncpg's prepared-statement caches and assigns each prepared statement a
   unique name, so statements routed across pooled backends never collide.
3. **CAVEAT — server-side guards.** In pooler mode the F9 `statement_timeout` /
   `idle_in_transaction_session_timeout` guards are emitted as connection
   *startup parameters*, which the pooler accepts (`ignore_startup_parameters`)
   but does **not** apply to the backend session. Set them at the Postgres role
   instead so the runaway-query / idle-txn protection survives the pooler:
   ```sql
   ALTER ROLE thought2build SET statement_timeout = '30s';
   ALTER ROLE thought2build SET idle_in_transaction_session_timeout = '5min';
   ```
4. **Rollback:** set `DB_TRANSACTION_POOLER_MODE=0` and point `DATABASE_URL`
   back at Postgres directly. No migration; effective on next process start.

> ⚠️ **Must-confirm deploy fact:** the real `max_connections` on the managed
> Postgres. Size the pooler's `default_pool_size` and the app's `DB_POOL_SIZE`
> against it — total Postgres connections ≈ pooler `default_pool_size` ×
> distinct `(db,user)` pairs, independent of API/worker instance count.

### F4 — scaling API workers

`gunicorn.conf.py` reads `WEB_CONCURRENCY` (default **2** — the prior hardcoded
value, so no footprint change on upgrade). Raise toward `~2*cores+1` **only after
the pooler (F3) is in front** — otherwise more workers × the per-process pool
crowds `max_connections`. There is deliberately **no** `max_requests` worker
recycling: a recycle would sever in-flight multi-minute SSE generation streams.

### Metrics & alerts

The `/metrics` endpoint now exposes the SQLAlchemy pool per instance (F3):

| Metric | Meaning |
|---|---|
| `thought2build_db_pool_checked_out` | connections in use right now |
| `thought2build_db_pool_checked_in` | idle pooled connections |
| `thought2build_db_pool_overflow` | overflow beyond `pool_size` (negative = not full) |
| `thought2build_db_pool_total_open` | **total open Postgres connections from this instance** |
| `thought2build_db_pool_max` | this instance's peak ceiling (`pool_size`+`max_overflow`) |
| `thought2build_background_tasks{registry}` | live detached advisory tasks (eval/critic/verifier) — F6 |

**Acceptance gate / alert:** `sum(thought2build_db_pool_total_open)` across instances
must stay under the confirmed `max_connections` with margin. With the pooler in
front, watch the pooler's own server-connection count instead — it should be flat
as you add app instances. Alert on `thought2build_background_tasks` climbing without
bound (a fan-out leak past the F1 admission cap).

---

## 16. Worker Lanes — Fast/Bulk Queues (Scalability P1)

**Reference:** `docs/SCALABILITY_AUDIT.md` §F5/§F6
**Modules:** `backend/worker.py`, `backend/services/queue.py`,
`backend/services/pipeline/background_tasks.py`

### What it is

A single worker queue let a burst of bulk GitHub exports (up to 1800s each) fill
the worker's job slots and **starve the billing webhook processor** — delaying
credit grants users had already paid for. F5 splits the live queues by latency
class, each drained by its **own process**:

- **Bulk lane** — `arq worker.WorkerSettings`, arq's **default** queue
  (`arq:queue`). GitHub bulk I/O (export/backfill/increment/projects/reconcile),
  LLM eval batches, and durable paid stage generation. Generation keeps its own
  concurrency/provider-call bulkheads and per-job retry/timeout contract. The
  default queue ensures nothing enqueued before the split is stranded.
- **Fast lane** — `arq worker.FastWorkerSettings`, a dedicated queue
  (`arq:queue:fast`). Paid credit grants (`billing_process_webhook`) + the PR
  status check (`pr_check`), plus the billing recovery crons.

Routing is by job **name** via `services.queue.queue_for_job` (single source of
truth; every `enqueue()` call site is unchanged). All lanes are stateless and
scale to **N replicas** — jobs are idempotent/checkpointed and arq dedups crons
**per queue**, so each cron fires once per lane regardless of replica count.

**Accepted consequence — paid generation is NOT isolated from bulk exports.**
Generation briefly had its own lane and its own Railway service; that service was
never provisioned, so the API's `/health` failed its Railway healthcheck and the
deploy could not go green. Sharing the already-provisioned bulk lane is the
no-new-infrastructure trade, and it is a real trade: a bulk-export storm can
occupy bulk job slots ahead of a paid run. Two things bound the damage — F1
admission (§14) caps in-flight generations independently of arq, and `max_jobs`
on the bulk lane is 20 against `MAX_CONCURRENT_GENERATIONS_PER_PROCESS`. If
export backlogs start delaying paid runs, the fix is a second `worker` **replica**
(no new service, no new config file), not a new lane.

A second consequence of the shared lane: arq computes ONE in-progress lease per
worker from `max(function timeout)` (`arq/worker.py`), so a generation job's
crash lease is the bulk lane's 1800s, not its own ~360s. Every graceful path
(`Retry`, slice exhaustion, SIGTERM on redeploy) releases it immediately; only a
hard kill (OOM/SIGKILL) delays that run's **resume**. The charge is not stranded —
the API recovery sweep settles and refunds at `stage_generation_deadline_seconds`,
and a late retry no-ops because the run is no longer `running`.

### ⚠️ DEPLOY INVARIANT — both workers must run everywhere

Because `billing_process_webhook` and `pr_check` route to `arq:queue:fast`, those
jobs **only drain if a `FastWorkerSettings` process is running**. The billing 60s
sweep re-enqueues to the same fast queue, so it is **not** a substitute consumer.
The bulk worker also owns paid stage generation; without it a paid run remains
`in_progress` until bounded recovery refunds it. Deploy **both** worker process
types in every environment:

- **docker-compose:** `worker` and `worker-fast`.
- **Procfile (Railway/etc.):** `worker` and `worker_fast`.

For Railway, provision three services named `backend`, `worker`, and
`worker-fast`, all rooted at `/backend`. Set each service's
**Config File Path**
explicitly (Railway config paths are repository-absolute):

| Service | Config File Path |
|---|---|
| `backend` | `/backend/railway.json` |
| `worker` | `/backend/railway.worker.json` |
| `worker-fast` | `/backend/railway.worker-fast.json` |

All three services must auto-deploy the same successful `main` commit. The API
config calls `entrypoint.sh`, preserving migrate → seed templates → Gunicorn;
worker configs intentionally run neither migrations nor template seeding.

A missing/stalled fast worker surfaces as:
`thought2build_worker_queue_oldest_age_seconds{queue="arq:queue:fast"}` climbing, and
`thought2build_billing_webhook_pending_age_seconds > 300` (the existing
`BillingWebhookPendingAge` alert, §9.1). **Recovery:** start the fast worker — the
queued jobs and the next sweep drain it; grants are idempotent so nothing
double-grants.

A missing/stalled bulk worker surfaces as
`thought2build_worker_queue_oldest_age_seconds{queue="arq:queue"}` climbing while
generation runs remain in `preparing`. Production and staging require a **live**
generation-capable worker heartbeat (`worker:heartbeat:generation`, TTL 45s,
refreshed every 10s) before a cache-miss generation is charged; without one the
user gets `generation_worker_unavailable` and **no credits are deducted**. This
dependency deliberately does not fail the whole API `/health` endpoint — that is
what made the deploy healthcheck itself fail.

Readiness is **liveness, not code identity**. The heartbeat payload carries the
worker's deploy revision, and a live worker on a different revision from the API
logs `generation.worker_revision_mismatch` but stays ready — `backend` and
`worker` are independently deployed Railway services, so a mismatch is the normal
state for the minute between two deploys landing, and gating on it would turn
every rolling deploy into a paid-path outage. Set
`GENERATION_WORKER_REVISION_MATCH_REQUIRED=true` only for a deployment that can
guarantee the two services move atomically. `GENERATION_WORKER_READINESS_REQUIRED=false`
disables the gate entirely (generations are then charged with no proof anything
will drain them).

**Recovery:** start the matching `worker`; queued jobs retain their stable run
ids and drain safely.

### Per-queue backpressure metrics

A lightweight per-worker cron (`sample_queue_stats`, every minute at :45) samples
the queue **that worker consumes** and is the only writer of these gauges. The
readiness snapshot reads the same backlog for its own log line but deliberately
does **not** publish it — with generation on the bulk lane, the bulk worker's own
cron already covers that queue, and an API-side write would race it under the
same label:

| Metric | Meaning |
|---|---|
| `thought2build_worker_queue_depth{queue}` | pending jobs in the queue |
| `thought2build_worker_queue_oldest_age_seconds{queue}` | age of the oldest ready job |
| `thought2build_github_queue_depth` | back-compat alias for the bulk queue depth |

**Alert ideas:** `thought2build_worker_queue_oldest_age_seconds{queue="arq:queue:fast"}
> 120` (fast lane starved / no consumer — paid grants delayed); the bulk lane the
same at a looser threshold (export backlog).

### F6 — background-task fan-out (related)

The post-`done` advisory work (eval score + critic judge + Demo-Day verifier)
shares a concurrency ceiling (`MAX_CONCURRENT_ADVISORY_TASKS`, default 12; 0
disables) so it can't starve live generation streams; each registry's live size
is `thought2build_background_tasks{registry}` and crossing
`BACKGROUND_TASKS_SOFT_MAX` logs a one-shot high-water warning (it never drops a
task). The detached generation pipeline is **not** gated — it is bounded upstream
by F1 admission (§14).

## 17. Tail Latency — CPU Offload, Public Cache & Partial Index (Scalability P2)

**Reference:** `docs/SCALABILITY_AUDIT.md` §F7 / §4 / §5 (P2 roadmap items 9–10)
**Modules:** `backend/services/cpu_offload.py`,
`backend/services/sharing/public_share_service.py`, `backend/routers/public.py`,
`backend/services/pipeline/pdf_export_service.py`,
`backend/migrations/versions/0031_stages_in_progress_partial_index.py`

### F7 — CPU offload off the event loop

Sync, GIL-holding passes over LLM-scale payloads used to run **inline on the
event loop**: `bleach.clean` (sanitise-on-persist/refine), the full-document
regex validators (`output_validator`, `prompt_guard`, `artifact_validator`, the
problem-statement gate, the Demo-Day construction linter) and `difflib` in the
diff engine. On only `WEB_CONCURRENCY` async workers, one 200KB pass stalls
every peer coroutine on that worker (SSE heartbeats, logins, health checks).

All of those now route through one seam — `services.cpu_offload.run_cpu_bound`:

- **Dedicated bounded pool** (`CPU_OFFLOAD_MAX_WORKERS`, default 4, clamped
  ≥ 1), isolated from the PDF and Langfuse thread pools so a CPU burst cannot
  starve their I/O offload.
- **Size gate** (`CPU_OFFLOAD_MIN_CHARS`, default 4096): inputs below it run
  inline — the thread round-trip costs more than a short pass. Ops escape
  hatches: set it huge to force everything inline (offload off), `0` to offload
  every call.
- **Contract:** results and exceptions propagate unchanged (validators still
  raise `MissingSectionError` / `IncompleteArtifactError` / gate errors
  identically); every wrapper is byte-identical to its sync form.

Deliberately **left inline**: bounded-small inputs (clarifier answers ≤1K,
increment requests ≤4K, idea text ≤2K, per-snippet research/storyboard strings)
and the arq-worker-side export/reconcile paths (not the API loop; bulk jobs are
long-running by design). The post-`done` eval/verifier internals stay inline
too — they are advisory, F6-gated, and the lag metric below is the tool that
says if they ever matter.

**Validation metric:** `thought2build_event_loop_lag_seconds` (histogram) — a
per-process sampler measures how late a fixed 5s timer fires; the excess is
loop starvation. **Alert idea:** sustained p99 > 250ms means CPU work is
stalling the loop again (F7 regressed or a new inline hot spot appeared).
During a load test (audit §8) this is the "loop stays responsive" acceptance
gate.

### PDF render pool (env-driven)

The WeasyPrint render pool size is `PDF_EXPORT_MAX_WORKERS` (default 2, clamped
≥ 1, still isolated from the other pools). Raise it only if PDF export becomes
a hot path — admission is capped separately by the PDF rate tier. Each render
is CPU-bound for 0.5–3s, so size against available cores, not demand.

### Public share payload cache

`GET /public/{slug}` is unauthenticated and scraper-exposed; every miss costs
two DB reads + a coverage rollup on the shared pool. The assembled response
(etag + JSON body) is now cached in Redis for `PUBLIC_SHARE_CACHE_TTL_SECONDS`
(default 60s — within the `Cache-Control: max-age=60` the response already
advertises, so nothing gets staler than the HTTP contract).

Operational properties:

- **Positives only.** 404s are never cached (an arbitrary-slug scraper must not
  grow Redis); junk-shaped slugs are rejected before Redis or the DB is touched.
- **Immediate eviction** on disable / rotate / re-enable — a killed share stops
  serving from cache at once; the retired slug of a rotate does too.
- **Fail-open.** Any Redis error degrades to the authoritative DB read; cache
  set/evict failures only log (`public_view_cache.*` warnings).
- `0` disables the cache entirely (every request reads through).

If a share must be killed **fleet-wide right now** and Redis is suspect:
disable the share, then `redis-cli DEL "public_view:<slug>"` — the key name is
`public_view:{slug}`.

### Partial index for the recovery sweep (migration 0031)

The 60s stuck-stage sweep filters `status = 'in_progress' AND updated_at <
cutoff`; `in_progress` rows are rare (bounded by live generations) but the full
`ix_stages_status` scans grow with table size. 0031 adds the tiny partial index
`ix_stages_in_progress_updated_at ON stages (updated_at) WHERE status =
'in_progress'` (Postgres-only; additive — `ix_stages_status` is kept for the
other status filters). On a very large `stages` table build it out-of-band with
`CREATE INDEX CONCURRENTLY` + `alembic stamp 0031` (see the migration
docstring); otherwise the inline build is cheap.

### Per-installation GitHub governor (T-274)

Listed under P2 in the audit roadmap but shipped earlier
(`services/integrations/github_governor.py`, tested by
`test_github_governor.py`): per-installation concurrency + hourly budgets so
one tenant's export storm cannot monopolise the worker lanes. No new ops knobs
in P2.

## 18. Data Retention & Purging (issue #43)

Keeps steady-state DB size proportional to the **active** corpus, not lifetime
history. Everything is additive and reversible by flipping a flag — Phases 0–2
change no API surface. Code: `services/retention.py`; crons in `worker.py`
(BULK lane); metrics in `services/observability.py`.

### The two-key safety model

Nothing deletes unless **both** the master enable AND dry-run-off are set, *and*
the tier's own flag is on:

```
will_delete = retention_enabled AND (NOT retention_dry_run) AND retention_tierN_purge_enabled
```

- `retention_enabled` (default **true**) — master kill-switch. `false` disables
  every purge **and** the Phase-0 size sampler (the total backout).
- `retention_dry_run` (default **true**) — every purge job only **counts**
  candidates (feeds the gauge + audit log) and deletes nothing.
- `retention_tier1/2/3_purge_enabled` (default **false**) — per-tier delete gate.
  With a tier's flag off it still counts, so the gauge/backlog alert stay live.

So on a fresh deploy: sampler + counting run, **zero deletions**. You enable a
tier by flipping `retention_dry_run=false` and that tier's flag.

### Cron schedule (BULK lane, collision-free — §1.5 map)

| Job | When (UTC) | What |
|---|---|---|
| `sample_table_stats` | hourly at :41 | size baseline gauges (read-only) |
| `retention_tier1_purge` | 04:11 daily | telemetry TTL + failed-row purges |
| `retention_tier2_purge` | 04:31 daily | stage-version / storyboard keep-N |
| `retention_tier3_purge` | 04:51 daily | trashed-workspace hard-delete |

Each job caps at `retention_max_rows_per_run` (default 50 000) rows/run in
`retention_purge_batch_size` (default 1000) batches, committing per batch — a
backlog drains over several days rather than in one lock storm.

### Enable order (dev/staging first — Thought2Build is pre-production)

Validate with **short windows** (set the `*_days` knobs to `1` so candidates
materialise) + `tests/test_retention.py`, not calendar time.

1. **Phase 0** ships with launch — the size baseline yardstick. Nothing to
   "observe for weeks".
2. **Tier 1** — confirm dry-run parity in staging (candidates counted ≈ rows
   deleted), then `retention_tier1_purge_enabled=true` + `retention_dry_run=false`.
   Row-count growth is bounded; zero user-visible change.
3. **Tier 2** — gate on **EXPLAIN-verify** (staging: the keep-N query uses
   `ix_stage_versions_stage_id`, no seq scan on a hot table) **and product
   sign-off** on `retention_stage_versions_keep` (it truncates visible
   version/diff history beyond N). *Byte* growth becomes bounded here.
4. **Tier 3** — migration 0033 + API + UI ship, **`docs/RETENTION_POLICY.md` +
   ToS/privacy published** (the real gate is legal sign-off) → staging dry-run →
   `retention_tier3_purge_enabled=true`. Production `validate_production_settings`
   then requires `retention_trash_days >= 7` and
   `retention_legacy_archived_days >= 90`.

**Backout at any point:** flip the tier flag off, or `retention_enabled=false`
for everything (sampler included).

### Workspace trash lifecycle (Tier 3)

"Delete workspace" is **Move to trash**: `DELETE /workspaces/{id}` sets
`status='archived'`, stamps `archived_at=now()` and the `retention_ack_version`
the dialog showed. The workspace leaves the active list and appears under
`GET /workspaces/trashed` (Dashboard "Recently deleted") with a countdown +
**Restore** (`POST /workspaces/{id}/restore`, clears the clock, allowed over
quota) + **Export** for the whole window. The Tier-3 cron hard-deletes only when:

- **acked** (`retention_ack_version` set): `archived_at` older than
  `retention_trash_days` (default 30); or
- **legacy / un-acked** (NULL): older than `retention_legacy_archived_days`
  (default 180) — a conservative window for rows deleted before retention shipped
  or by a stale SPA.

A hard-delete cascades the full subtree (stages → versions → evals, increments,
ideas, storyboards, pushes → push tasks). **Financial/identity rows survive:**
`credit_ledger` (only FK is `user_id`) is untouched, and `llm_cost_events`
survive with `workspace_id` nulled (`SET NULL`). Each purged workspace emits a
`retention.workspace_purged` audit row (workspace_id, user_id, archived_at,
ack_version).

### Metrics & alerts

- `thought2build_db_table_bytes{table}` / `_live_tuples{table}` — the size baseline.
- `thought2build_retention_candidates{job}` — set every run (incl. dry-run/flag-off).
- `thought2build_retention_purged_rows_total{job,table}` — rows deleted.
- `thought2build_retention_run_seconds{job}` — duration (index-rot / lock signal).
- `thought2build_retention_last_success_timestamp{job}` — missed-run pager.

Alerts:

- **Job failed / stalled:** `time() - thought2build_retention_last_success_timestamp
  > 26h` per job (the structlog `retention.*_failed` exception is the diagnostic).
- **Backlog:** `retention_candidates{job}` rising for 7 d while that tier's purge
  flag is on ⇒ per-run cap undersized — raise `retention_max_rows_per_run`.
- **Not stabilising:** `thought2build_db_table_bytes` slope still positive 4 weeks
  after a tier enabled.

### `DELETE` does not shrink files

Autovacuum makes dead space **reusable**, so the success criterion is a
**plateau, not a shrink** — a flat `thought2build_db_table_bytes` slope at steady
state. Actual disk reclamation needs `pg_repack` (or `VACUUM FULL`, which takes
an `ACCESS EXCLUSIVE` lock) in a maintenance window — ops-optional, only if the
files must physically shrink.
