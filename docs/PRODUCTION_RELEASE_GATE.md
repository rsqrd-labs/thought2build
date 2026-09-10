# Thought2Build Production Release Gate

Use this checklist before promoting a staging build to production. It is a
release decision document, not a setup guide. For setup details, use
`docs/INTEGRATION_API_SETUP_HANDBOOK.md`; for manual smoke coverage, use
`docs/SMOKE_TEST_CHECKLIST.md`.

## Release Inputs

Record these before starting the gate:

| Field | Value |
|---|---|
| Release owner | |
| Date | |
| Git commit | |
| Staging URL | |
| Production URL | |
| Backend image/build | |
| Frontend image/build | |
| Database migration required | Yes / No |
| Langfuse enabled in production | Yes / No |
| Lemon Squeezy billing enabled in production | Yes / No |
| Verified Lemon webhook event list (recorded) | |
| `ASDD_PROMPT_VERSION` | |
| Prompt eval baseline | |

## Automated Gate

Run from the repository root unless noted.

```bash
cd backend
uv run black --check .
uv run ruff check .
uv run pytest tests -q
uv run pytest \
  ../harness/tests/backend/test_security_audit_contract.py \
  ../harness/tests/backend/test_second_pass_security_contract.py \
  ../harness/tests/backend/test_final_hardening_contract.py \
  ../harness/tests/backend/test_production_readiness_contract.py \
  ../harness/tests/backend/test_langfuse_contract.py \
  ../harness/tests/backend/test_langfuse_live_traffic_contract.py \
  ../harness/tests/backend/test_phase21_stripe_payments_contract.py \
  ../harness/tests/backend/test_phase22_prompt_pipeline_contract.py \
  ../harness/tests/backend/test_phase23_storyboard_contract.py \
  ../harness/tests/backend/test_phase25_lemonsqueezy_billing_contract.py \
  -q
uv run bandit -r config.py database.py main.py middleware models prompts routers schemas services
# No advisories are suppressed: every pip-audit finding fails the gate. Fix by
# bumping the affected dependency.
uv run pip-audit --strict
```

Committed-fixture regression check (does not validate candidate output quality):

```bash
cd harness
uv run --project ../backend python -m prompt_eval.run \
  --version "$(sed -n 's/^ASDD_PROMPT_VERSION = \"\(asdd-v[0-9.]*\)\"/\1/p' ../backend/prompts/base.py)" \
  --baseline asdd-v1.8.0 \
  --report ../prompt_eval_report.md
```

Actual prompt quality gate: run the `generated-prompt-gate` workflow job against
an immutable baseline revision and the candidate, retain its generated evidence,
and review complete standard and Demo Day packages. Both revisions must use the
same corpus. Missing provider credentials or missing outputs is an incomplete
gate, never a pass. The job makes billed provider calls, capped at 100 attempts
per revision. See [pipeline remediation rollout notes](reviews/PIPELINE_RELIABILITY_REMEDIATION_2026-09-10.md).

Storyboard frontend gate:

```bash
cd frontend
pnpm vitest run --root .. harness/tests/frontend/phase23-storyboard.contract.test.ts
pnpm test -- Storyboard
pnpm tsc --noEmit
```

Production smoke against staging:

```bash
THOUGHT2BUILD_API_URL=https://api.example.com \
THOUGHT2BUILD_ACCESS_TOKEN=<short-lived smoke-user access token> \
THOUGHT2BUILD_METRICS_TOKEN=<metrics token> \
THOUGHT2BUILD_RUN_LLM_SMOKE=1 \
python3 scripts/production_smoke.py
```

Pass criteria:

- Formatting and lint pass.
- Unit tests pass.
- Security and production-readiness harness contracts pass.
- Lemon Squeezy billing (`phase25`) and prompt pipeline harness contracts pass.
- Storyboard backend/frontend harness contracts and focused Storyboard tests
  pass.
- Actual generated candidate packages pass production readiness checks and
  show no per-grader regression against freshly generated baseline packages.
  Human review accepts requirement fidelity and cross-stage consistency.
- Technology policy freshness check passes (`python backend/scripts/check_policy_freshness.py`).
- Bandit reports no unresolved issues.
- `pip-audit --strict` reports no known vulnerabilities.
- Production smoke passes against staging with live LLM smoke enabled.

### Tracked security exceptions

None. `pip-audit --strict` runs with no `--ignore-vuln` suppression — every
advisory fails the gate and is fixed by bumping the affected dependency. (The
former PYSEC-2026-161 starlette exception was retired once starlette moved to
1.3.x, well past the affected `< 1.0.1`.)

## Environment Gate

Production must satisfy these backend requirements:

| Variable | Requirement |
|---|---|
| `ENVIRONMENT` | `production` |
| `FRONTEND_URL` | HTTPS URL |
| `JWT_PRIVATE_KEY` | Real PEM private key |
| `JWT_PUBLIC_KEY` | Matching PEM public key |
| `ENCRYPTION_MASTER_KEY` | Real Fernet key, not the CI placeholder |
| `CSRF_SECRET` | Long random secret |
| `METRICS_TOKEN` | Set |
| `DATABASE_URL` | Production database URL |
| `REDIS_URL` | Production Redis URL |
| Provider API keys | Set only for enabled providers |
| `GITHUB_CLIENT_ID` | Blank to disable GitHub export; set both vars together |
| `GITHUB_CLIENT_SECRET` | Required when `GITHUB_CLIENT_ID` is set |

Lemon Squeezy billing requirements (Phase 22):

| Variable | Requirement |
|---|---|
| `LEMONSQUEEZY_API_KEY` | Blank to disable billing; required to enable checkout |
| `LEMONSQUEEZY_STORE_ID` | Required to enable checkout |
| `LEMONSQUEEZY_VARIANT_ID` | Required to enable checkout |
| `LEMONSQUEEZY_WEBHOOK_SECRET` | Required (live secret) when billing is enabled |
| `LEMONSQUEEZY_WEBHOOK_SECRET_PREV` | Set only during a secret-rotation window |
| `LEMONSQUEEZY_PRICE_CENTS` | Positive integer package price in cents |
| `LEMONSQUEEZY_CURRENCY` | Non-empty ISO 4217 currency code |
| `LEMONSQUEEZY_CREDITS_PER_PURCHASE` | Positive integer credit grant per pack |
| `LEMONSQUEEZY_CREDIT_VALIDITY_DAYS` | Positive integer expiry window |
| `LEMONSQUEEZY_SUCCESS_URL` | HTTPS frontend billing URL, normally `{FRONTEND_URL}/billing` (no cancel URL) |
| `LEMONSQUEEZY_TEST_MODE` | **Must be `false`** in production |
| `ADMIN_USER_EMAILS` | Comma-separated allowlist for billing admin corrections (empty = nobody) |

Langfuse production requirements:

| Variable | Requirement |
|---|---|
| `LANGFUSE_SECRET_KEY` | Blank to disable; non-empty to enable |
| `LANGFUSE_PUBLIC_KEY` | Required when `LANGFUSE_SECRET_KEY` is set |
| `LANGFUSE_HOST` | HTTPS URL (cloud or approved self-hosted endpoint) |
| `LANGFUSE_PROMPT_CACHE_TTL` | Positive TTL, default `300` |
| `LANGFUSE_CONTENT_CAPTURE_ACK` | Required as `true` when Langfuse is enabled in production |

Pass criteria:

- Application startup validation succeeds in the production-like environment.
- **Before enabling checkout in production**, verify the Lemon **live** webhook:
  it points to `POST /billing/webhook`, is subscribed to `order_created` (and
  `order_refunded`), uses the configured live `LEMONSQUEEZY_WEBHOOK_SECRET`, and a
  signed `order_created` **test delivery succeeds** (200 + an inbox row + the
  grant). Record the verified event list in the release record above (confirm
  against Lemon's current catalog that chargebacks/disputes map to the
  `order_refunded`/fraud inputs — Lemon is Merchant of Record).
- Langfuse is either intentionally disabled or explicitly approved for
  prompt/output telemetry export.
- No development secrets, CI placeholders, or local URLs are present in
  production variables.
- `LEMONSQUEEZY_TEST_MODE=false` in production with an HTTPS
  `LEMONSQUEEZY_SUCCESS_URL`. The backend rejects a half-configured or test-mode
  Lemon at startup, but the gate should catch it before deploy.

### Billing Cutover & Gated Stripe Removal (Phase 22 rollout)

The Lemon Squeezy migration superseded Stripe **at runtime** (Plan §25.9), and
the Stripe runtime has since been **fully decommissioned** (T-308):

1. Deploy with Lemon enabled and verify the live webhook (above). New checkout is
   Lemon-only.
2. **Stripe runtime decommissioned (T-308 — done).** The `stripe` SDK,
   `stripe_service.py`, the `STRIPE_*` settings + scoped config guard, the Stripe
   observability patterns, and the bounded late-webhook grace adapter are all
   removed. `POST /billing/webhook` now answers any Stripe-shaped request (a
   `Stripe-Signature` header) with `{"status":"ignored_provider_disabled"}` before
   any body read, signature claim, or DB write.
3. **Retained audit trail.** The `stripe_credit_packs` / `stripe_webhook_events`
   tables and their backfilled `provider='stripe'` rows are kept as the historical
   financial record; no migration drops a Stripe table and no runtime path reads
   them.
4. **Operational-gate prerequisite for T-308 (record before relying on this).**
   T-308 may only be deployed once: ≥7 days since the production checkout cutover
   **and** zero `thought2build_billing_webhook_received_total{provider="stripe"}`
   increments over the preceding 72h **and** the grace flag disabled. Confirm and
   attach this evidence in the deploy record before shipping the decommission to
   production — it cannot be reconstructed after the fact.

## Manual Smoke Gate

Complete the staging smoke checklist in `docs/SMOKE_TEST_CHECKLIST.md`.

Minimum release-blocking coverage:

- Google OAuth sign-in works.
- New user receives starting credits.
- Workspace creation works.
- SPEC generation streams tokens and decrements credits.
- Eval badge appears after generation.
- Refine preview works and refunds on reject.
- Stage finalise unlocks downstream stages.
- Full SPEC to TASKS path can complete.
- Export zip downloads and contains expected files.
- Billing package, checkout redirect, `checkout_ref` status polling, and history
  work when Lemon Squeezy billing is enabled.
- `/health` and `/metrics` are reachable as expected.
- Prompt pipeline output includes required Phase 19 sections and the eval suite
  report has been reviewed.
- Storyboard migration has run, Storyboard rate limit tiers are active, public
  CSP blocks framing/scripts, and renderer sanitizer coverage confirms generated
  script, iframe, and remote-asset input is inert.
- Frontend loads without console errors.

### Storyboard Release Gate

These checks are release-blocking for Phase 20:

| Gate | Required evidence | Blocks release if |
|---|---|---|
| Migration applied | `alembic current` on staging includes the Storyboard migration revision and `storyboards` exists with the public slug index | Migration is missing or rollback plan is unknown |
| Rate limits active | Generate, section-regenerate, share-toggle, public-view, and download tiers are present in middleware and visible in staging responses | Any Storyboard mutation/public route bypasses rate limiting |
| Public route security headers | `curl -i "$STAGING_API_URL/storyboards/public/$STORYBOARD_SLUG"` shows `X-Robots-Tag: noindex, nofollow`, `Cache-Control: no-store, private`, `X-Content-Type-Options: nosniff`, and CSP with `frame-ancestors 'none'` | Headers are absent on success or not-found responses |
| HTML/PDF sanitizer tests | `cd backend && uv run pytest tests/test_storyboard_renderer.py tests/test_storyboard_security.py -q` passes | Script, iframe, object/embed, or remote-asset input survives rendering |
| Credit ledger refund smoke | Fake provider failure produces one debit and exactly one `refund:<credit_ledger_id>` row | Refund is missing, duplicated, wrong amount, or balance cache stays stale |
| Public privacy smoke | Default `/sb/{slug}` hides speaker notes, appendix, source excerpts, user/workspace IDs, email, credit balance, billing history, `credit_ledger_id`, raw prompts, and `source_stage_version_ids` | Notes, source, appendix, account, billing, or ledger data leaks by default |
| Previous-ready regeneration failure smoke | Create ready v1, trigger failed full regeneration or section regeneration, then reopen the deck | Previous ready/stale version is deleted, mutated, or no longer presentable |

Copy-paste staging probes:

```bash
export STAGING_API_URL=https://api.example.com
export STORYBOARD_SLUG=replace-with-public-slug
curl -i "$STAGING_API_URL/storyboards/public/$STORYBOARD_SLUG"
curl -i "$STAGING_API_URL/storyboards/public/$STORYBOARD_SLUG/download/pdf"
curl -i "$STAGING_API_URL/storyboards/public/$STORYBOARD_SLUG/download/notes"
```

Expected default: public page and PDF are available, notes are not found unless
the owner enabled notes download, and HTML package download is never public by
default.

Pass criteria:

- No critical smoke item fails.
- Any non-critical note has an owner and explicit acceptance.

### GitHub Living System of Record Release Gate (Phase 21)

This gate ships **phase by phase (A → D), not all at once** (spec §4.14,
`Plan v1.md` §24.13). Each phase is release-blocking for the surface it enables;
a later phase must not ship against an earlier phase that has not passed. It is
the **final checkpoint** of Phase 21 and **must be re-run after T-287–T-289**
(the frontend Settings-install, export-mode, and increments surfaces).

**Locally verifiable now (CI / a clean checkout):**

| Check | Command / evidence | Blocks release if |
|---|---|---|
| Worker image builds | `docker build ./backend` succeeds; the image runs `arq worker.WorkerSettings` | The backend/worker image fails to build |
| Workers register all jobs | Inspect both `WorkerSettings.functions` and `FastWorkerSettings.functions`; bulk includes `export_push, reconcile_event, backfill_repo, increment_push, projects_sync`, fast includes `reconcile_issue_event, refresh_task_states, pr_check, billing_process_webhook`, and `reconcile_drift` is registered once on bulk | Any job or the cron is unregistered |
| Migrations round-trip | `uv run alembic upgrade head` then `downgrade -1`/`upgrade head` for `0016` + `0017` apply cleanly and restore the prior constraint | Migration or rollback fails on a clean DB |
| Backend contract green | `uv run pytest ../harness/tests/backend/test_phase24_github_living_contract.py -q` is fully green | Any Phase 24 backend contract is red |
| Behavioral suite green | `uv run pytest tests/ -q --cov=services --cov-fail-under=80` (incl. `tests/test_phase24_behavioral.py`) | Coverage drops below 80% or a behavioral pin fails |
| Frontend contract | `pnpm vitest run --config vitest.harness.config.ts phase24-github-living` — backend-backed describe blocks green; the **Settings-install** and **export-mode** blocks land with **T-287/T-288** | A *backend-backed* frontend block is red (the two pending blocks are tracked, not blocking until T-287/T-288) |
| Secrets never logged | grep/contract: no code path logs an installation token, the App private key, a raw webhook payload, or a PR diff | Any of those appears in a log call |

**Phase-gated criteria (verify in staging via `docs/SMOKE_TEST_CHECKLIST.md`
§"Phase 21" — the manual end-to-end flow):**

| Phase | Release-blocking criteria | Blocks release if |
|---|---|---|
| **A — Foundation** | Installs persist; every API call uses a **cached installation token**; no static user token in the write path; export runs on the worker and returns **202**; webhook ack **p99 < 300 ms**; signed-fixture tests (valid / invalid / replayed / out-of-order) pass | A write uses a static token, export blocks the request, ack p99 ≥ 300 ms, or a malformed/replayed signature is accepted |
| **B — The loop ("now it's core")** | Closing an issue flips its task to **done within SLO**; **backfill** recovers missed-while-down events; the confused-deputy authz test proves install A cannot touch workspace B; **kill-worker-mid-reconcile** resumes without dupes | A close does not reach SLO, backfill loses events, cross-tenant mutation is possible, or a restart duplicates side effects |
| **C — Executable** | A finalized workspace opens **one PR** with a **red** harness CI run; re-export updates it **in place**; `Workflows: write` 403 and content 409 retry are both handled | A duplicate PR/branch appears, re-export forks state, or a 403/409 surfaces as an opaque failure |
| **C′ — Living** | "Add two features" pushes **only new issues** under a **new milestone** on top of shipped v1 work (no duplicate issues for unchanged tasks) | An increment re-creates existing issues or pushes outside its milestone |
| **D — Team-grade** | Tasks appear on a board reflecting **live** state; PRs carry a **Thought2Build check**; the LLM-check cost is **capped per tenant/day** | The board drifts from live state, the check is absent, or the evaluator has no per-tenant cost cap |

Pass criteria:

- Every locally-verifiable check is green in CI / on a clean checkout.
- Each phase's staging criteria pass before that phase's surface is enabled in
  production; do not enable Phase C/C′/D against an un-passed Phase A/B.
- The full gate is **re-run after T-287–T-289** land the remaining frontend
  surfaces; the two pending frontend contract blocks must then be green.

## Observability Gate

Metrics:

- `/metrics` requires the intended token or trusted-source access.
- Staging scrape target is healthy.
- Production scrape target is ready before deploy.
- Billing counters are present (provider-labelled):
  `thought2build_billing_checkout_created_total`,
  `thought2build_billing_checkout_completed_total`,
  `thought2build_billing_credits_granted_total`,
  `thought2build_billing_credits_revoked_total`,
  `thought2build_billing_webhook_error_total`, the
  `thought2build_billing_webhook_pending_age_seconds` gauge, and
  duplicate/rate-limit counters.
- Prompt quality counters are present: `pipeline_validator_failures_total`,
  `pipeline_upstream_section_skipped_total`, and
  `thought2build_billing_credits_critic_regen_total`.
- Storyboard counters are present: `thought2build_storyboard_generation_started_total`,
  `thought2build_storyboard_generation_failed_total`,
  `thought2build_storyboard_credits_refunded_total`,
  `thought2build_storyboard_public_view_total`,
  `thought2build_storyboard_download_total`, and
  `thought2build_storyboard_source_missing_total`.
- Storyboard dashboards include generation failure rate, refund spike detection,
  public view volume, download failures, PDF render latency, and source-missing
  counts.
- GitHub counters are present when the App is enabled:
  `thought2build_github_webhook_received_total`,
  `thought2build_github_webhook_verified_total`,
  `thought2build_github_webhook_failed_total`,
  `thought2build_github_reconcile_lag_seconds`, `thought2build_github_export_total`,
  `thought2build_github_pr_total`, `thought2build_github_check_total`,
  `thought2build_github_token_mint_total`, `thought2build_github_job_retries_total`,
  `thought2build_github_job_deadlettered_total`, and
  `thought2build_github_queue_depth`.
- GitHub dashboards/alerts cover webhook failure rate, reconcile lag p95,
  dead-letter rate, queue depth, token-mint cache-hit ratio, and check verdicts
  (see `docs/OBSERVABILITY_RUNBOOK.md`).

Sentry:

- Backend DSN is correct or intentionally blank.
- Frontend DSN is correct or intentionally blank.
- Test events in staging reach the intended project when enabled.

Langfuse:

- Disabled mode has been verified when `LANGFUSE_SECRET_KEY` is blank.
- Enabled mode has been verified in staging before enabling in production.
- Langfuse outages do not break generation, refine, eval, credits, or prompt
  fallback.
- Prompt/output capture has privacy and retention approval before production
  enablement.

Pass criteria:

- Observability is sufficient to detect and investigate production regressions.
- Optional integrations can fail without breaking user-facing flows.

## Security Gate

Confirm:

- No secrets are committed to source.
- `.env` files are not staged or deployed as artifacts.
- Dependency audit has no unresolved vulnerabilities.
- Security harness contracts pass.
- Metrics endpoint is not publicly exposed without protection.
- CORS and frontend URLs match production domains.
- Prompt-injection defenses and output validation remain enabled.
- Prompt changes are versioned through `ASDD_PROMPT_VERSION` and gated by
  `harness/prompt_eval`.
- Lemon `X-Signature` HMAC validation (two-secret, fail-closed), durable-inbox
  idempotency, CSRF exemption, and the checkout rate limit are enabled.
- Langfuse payloads go through the shared redaction path.
- Langfuse production enablement has explicit content-capture acknowledgement.
- Storyboard public responses are allow-list based and noindex.
- Storyboard public notes, source excerpts, and appendix remain hidden by
  default. The release is blocked if any of them leak without the matching owner
  permission.
- Storyboard renderer sanitizer tests pass for scripts, iframes, object/embed
  tags, event handlers, and remote assets.

Pass criteria:

- No high or critical security issue is open.
- Medium issues, if any, have explicit release-owner acceptance and a follow-up
  issue.

## Database And Rollback Gate

Before deploy:

- Confirm whether a database migration is required.
- Confirm migration has run against staging.
- Confirm the Storyboard migration is present before enabling the feature:

  ```bash
  cd backend
  uv run alembic current
  ```

- Confirm backup/restore procedure is available — see
  [BACKUP_RESTORE.md](BACKUP_RESTORE.md). The off-platform backup workflow
  (`.github/workflows/db-backup.yml`) and restore helper
  (`scripts/backup/restore_backup.sh`) ship in-repo; before public launch, complete the
  ops-config checklist in that doc (bucket + secrets + one restore drill).
- Confirm rollback target commit/build is known.

Rollback triggers:

- Authentication is unavailable.
- Workspace creation or stage generation is broadly unavailable.
- Credit deductions occur without successful generation and refund paths fail.
- Lemon checkout completes but credits are not granted, or webhook/worker errors
  affect multiple users (watch `webhook_pending_age_seconds` and
  `billing:deadletter`).
- Prompt validator failures or critic-regeneration credit spikes appear after
  deploy and cannot be mitigated by reverting the prompt/version.
- Storyboard generation debits credits without an exactly-once refund on
  provider/schema failure.
- Public `/sb/` responses expose private notes, appendix, source excerpts,
  account identifiers, billing state, or ledger identifiers by default.
- Previous ready Storyboard versions are corrupted or hidden after a failed
  regeneration.
- Production startup validation fails.
- Security controls are disabled or bypassed.
- Langfuse failure propagates into user-facing failures.

Rollback notes:

- If Langfuse causes operational concern but user flows remain healthy, first
  disable it by clearing `LANGFUSE_SECRET_KEY` and redeploying/restarting.
- If a provider outage is isolated to one LLM provider, disable that provider or
  route users to a healthy provider before rolling back application code.

## Final Decision

| Gate | Result | Notes |
|---|---|---|
| Automated tests | Pass / Fail | |
| Production smoke | Pass / Fail | |
| Environment validation | Pass / Fail | |
| Observability | Pass / Fail | |
| Security | Pass / Fail | |
| Database/rollback | Pass / Fail | |

Release decision:

- Go
- No-go
- Go with accepted risk

Approver: ___________________________

Date/time: ___________________________
