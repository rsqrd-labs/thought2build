# GitHub Integration Review — 2026-09-09

Scope: the Phase-21 GitHub App living-system-of-record — install/identity flow,
webhook ingress + reconcile, export/increment/board sync, backfill/drift, the
API client + governor, and the frontend surfaces that read them.

Baseline: `uv run pytest tests/ -k "github or integration_push or reconcile or
increment or task_ref or webhook"` — **323 passed, 27 skipped** after
`alembic upgrade head` (the branch's untracked migration `0047` must be applied;
without it 35 tests fail on `stages.source_identity does not exist`, which is
in-flight work, not a defect).

Each finding below was reproduced against the real DB with the project's own
fixtures. The already-audited items (install IDOR #1, `task_ref` identity #2,
resync status #3, push vocabulary #4) are re-verified as still closed, and the
documented accepted trade-offs (per-client circuit breaker, `keep_result=0`
job-id dedup, governor-free repo picker) are correct as written.

---

## Status — all seven fixed (2026-09-10)

Every finding below is fixed on this branch, with the repros converted into
permanent tests in `backend/tests/test_github_sync_recovery.py` (26 tests) plus
migration `0048_github_sync_recovery`.

| # | Fix |
|---|---|
| F1 | `parse_tasks` resolves each task's identity ONCE, in document order, and carries it on `ParsedTask.task_ref`; `compute_task_ref` gained an `occurrence` salt whose **occurrence 0 is byte-identical to the old key**, so every persisted row still matches. All six consumers (both export loops, increment sync, the retire set, the `Closes #N` map, the Projects board, the sync-panel title map) read the carried field instead of re-deriving it. `_write_push_status` rolls back a flush-poisoned session before writing, so a terminal status can no longer be silently lost. |
| F2 | New `integration_pushes.last_full_backfill_at` watermark, set only by a sweep that completed and captured *before* the fetch. Replaces the `max(task.synced_at)` cursor. The test stub now honours `since`, which is why this class of bug was invisible before. |
| F3 | `integration_pushes.detached_installation_id` records the numeric id at detach; `upsert_installation` re-adopts its own former pushes, scoped to the same `user_id` so a different org admin's reinstall cannot adopt someone else's workspaces. |
| F4 | The monotonic `manual → pr_merge` upgrade is applied before the freshness gate, guarded on `state == "done"` so a stale event can never attribute an open task. |
| F5 | `github_webhook_events.payload` (JSONB, `none_as_null`) stores the verified body; `reconcile_drift` gained an inbox-replay lane that re-dispatches deliveries recorded but unprocessed for >15 min, routed through the same job names so `issues` keeps its fast lane. Counter `thought2build_github_webhook_replayed_total`. |
| F6 | Comment corrected — `allow_signup` is not a re-authorization control; freshness comes from the live `/user/installations` check. |
| F7 | Partial unique index `uq_integration_push_workspace_provider_active` on `(workspace_id, provider) WHERE repo_id IS NULL AND status <> 'failed'` closes the race; the migration dedupes pre-existing rows first. Both readers were also made tolerant (`order_by(created_at.desc()).first()`) so rows predating the index cannot 500 the endpoint. |

### Adversarial pass on the fixes (2026-09-10)

Reviewing the fixes themselves surfaced five more defects, all now fixed and
covered by tests:

- **The F2 watermark could be advanced past unseen issues.** `list_issues`
  truncates *silently* at 2,000 rows, so a repo with more updates than that would
  have had the watermark jump past everything beyond the cap — reintroducing F2's
  silent loss in a form that looks clean. A truncated sweep now resumes from the
  newest row it actually saw (taken over **all** returned rows, PRs included,
  since GitHub returns those from the issues endpoint and a final page of PRs
  would otherwise under-advance). The page bounds are exported from the client
  (`ISSUES_FETCH_CAP`) rather than inferred. The degenerate case where a full page
  shares the current cursor's timestamp — which would re-pull the identical page
  every tick forever — takes the full window and logs `cursor_stalled`.
- **F7 turned a permanent wedge into a transient 500.** With the index in place
  the losing racer's INSERT raises `IntegrityError`, uncaught by the route. Both
  claim paths are now one helper using `INSERT … ON CONFLICT DO NOTHING` + re-read.
  An ORM flush is deliberately not used: a failed flush poisons the whole Session
  (a SAVEPOINT protects database state but not SQLAlchemy's unit-of-work
  bookkeeping), which is the same failure `_write_push_status` had to be hardened
  against.
- **`detached_installation_id` was never cleared** when a push was re-bound to a
  new installation, leaving a spent adoption marker on the row.
- **The F3 re-adoption could take the install bind down with it.** It `UPDATE`s
  rows a worker export may hold locks on, so under the request path's statement
  timeout it can fail. It now runs in a SAVEPOINT and is best-effort — a full
  `db.rollback()` would have discarded the bind *and* expired every other object
  the caller held in that session.
- **F5's replay was unbounded.** A delivery that can never succeed would be
  re-enqueued every tick forever and, the sweep being batched oldest-first, would
  eventually fill the batch and starve the newer deliveries it exists to rescue.
  Bounded at 3 attempts, delay widened to 1 h (above the drift cron's own period,
  since the replay carries no arq `job_id`), and the payload is cleared on
  successful processing so the column holds only genuinely stuck rows.

**Verification.** Backend suite 2860 passed / 3 failed — the 3 are pre-existing
(`test_critic` ×2, `test_section_contract_lockstep`) and belong to unrelated
in-flight work on this branch; the baseline before these fixes was 2833 passed /
4 failed, and the fourth (`test_migration_0031_partial_index`, a stale
migration-head pin) is now green. `harness/tests/backend` is byte-identical
before and after (43 pre-existing failures, 740 passed).

**Behaviour changes worth knowing.** The legacy `T-NNN` task-ref migration now
puts same-titled rows on *distinct* occurrence keys rather than leaving the
second on its legacy ref (`test_increment_sync` updated to the new contract) —
the old behaviour left that issue unmatched, so `retire_obsolete_task_issues`
would close it. `_reconcile_delta` deliberately keeps comparing **unsalted**
refs: a repeated title in raw model output is a generation artifact to drop, not
two real tasks the way a repeat in the user's own finalised TASKS.md is.


---

## F1 — Two tasks with the same title silently collapse into one GitHub issue (and orphan another)

**Severity: High.** `services/pipeline/github_export_service.py:824`
(`_sync_issues`), same defect at `:1095` (`_run_export`) and
`services/pipeline/increment_service.py:1052` (`_sync_increment_issues`).

`compute_task_ref` hashes the case- and whitespace-folded title, so two tasks
with the same title — or titles differing only in case — produce the same
`task_ref`. The create loop reads `existing`, a snapshot taken *before* the
loop, and never records refs it creates into it:

```python
existing = await _load_existing_push_tasks(db, push.id)
issue_numbers: dict[str, int] = dict(existing)
for parsed in tasks:
    ref = compute_task_ref(parsed.title)
    existing_number = existing.get(ref)      # <- never sees this run's inserts
    ...
    issue_numbers[ref] = number              # <- the updated map is the wrong one
```

So the second same-titled task takes the create branch, opens a **duplicate
GitHub issue**, and then violates `uq_push_task_ref` on the immediately
following `db.commit()`.

`migrate_legacy_task_refs` guards this exact case explicitly ("two same-titled
tasks collide by design; we keep the first and leave the rest legacy rather than
crash", `task_ref_migration.py:104-113`), which shows the collision is known —
the three sync paths simply never got the same guard.

**Failure scenario (reproduced end-to-end).** TASKS.md contains
`### T-001: Add unit tests` and `### T-002: Add unit tests`. Export runs:

```
UniqueViolationError: duplicate key value violates unique constraint "uq_push_task_ref"
DETAIL:  Key (push_id, task_ref)=(6aa14663-…, task-6d01992a3e18) already exists.
```

Two GitHub issues (#101, #102) now exist for one tracked task; only #101 is
mapped. Because `compute_task_ref` casefolds, `set up ci` and `Set Up CI` collide
identically.

**Second-order damage differs sharply by path, and the App path is the worse one.**

*App worker path* (`run_export_push`, the route users actually hit —
reproduced with `is_final_attempt=False` twice, as arq passes it):

| | outcome |
|---|---|
| attempt 1 | `IntegrityError`; `_fail_or_retry` is a **no-op** (`IntegrityError` is not in `_PERMANENT_EXPORT_ERRORS` and this is not the final attempt), so the push stays `pending`. GitHub issues **#101 and #102** both now exist. |
| attempt 2 (arq retry) | `_load_existing_push_tasks` now returns attempt 1's committed row, so **both** same-titled tasks take the *update* branch against **#101** — observed `issues_updated == [(101, …), (101, …)]`. The second write overwrites the first's body: T-001's content is silently lost. Export reports **`status='completed'`**. |

Final state: 3 tasks, 2 mapped rows, `issue_count` reports **2**, and orphan
issue **#102** is unmapped forever — `retire_obsolete_task_issues` only walks
`integration_push_tasks`, so nothing will ever close or surface it.

That is silent content loss under a green status, which is worse than a visible
crash: no alert fires and no user-facing error is shown.

*Legacy OAuth path* (`push_to_github` → `_run_export`, entered with
`is_final_attempt=True`) fails loudly instead — and reveals a separate defect.
`_mark_push_failed` (`:1211`) calls `db.commit()` on a session already poisoned
by the failed flush; the resulting `PendingRollbackError` is caught and rolled
back, so **`status='failed'` is never persisted** and the push strands `pending`
until the `reconcile_drift` stuck-pending sweep clears it:

```
ERROR github_export.mark_failed_commit_error
sqlalchemy.exc.PendingRollbackError: This Session's transaction has been rolled
back due to a previous exception during flush.
```

This sub-defect is general to `_mark_push_failed`/`restore_push_status`: neither
rolls back before committing, so **any** flush failure silently loses the status
write. It fires on the App path too, on the final attempt.

**Reachability confirmed.** Nothing rejects duplicate task titles. The tasks gate
counts blocks only (`artifact_validator.py:204` `_TASK_HEADER_RE`,
`insufficient_task_count` at `:2322`); there is no heading-uniqueness check
anywhere in `artifact_validator.py` or `task_parser.py`. A `finalised` TASKS.md
with repeated or case-variant titles passes every gate and reaches export.

**Fix shape.** Dedup refs before the loop (skip or salt a repeat, matching the
migration's "keep the first" policy) and seed the created ref into `existing`
inside the loop. Separately, `_mark_push_failed` and `restore_push_status`
should `db.rollback()` before setting the status so a flush failure can still be
recorded.

---

## F2 — Backfill's `since` cursor is `max(synced_at)`, so it can never recover an older missed event

**Severity: High.** `services/integrations/github_reconcile.py:654-663`
(`_last_synced_iso`).

```python
stamps = [t.synced_at for t in tasks if t.synced_at is not None]
latest = max(stamps)
```

`since` is passed to GitHub's issues endpoint, which returns only issues updated
**at or after** that instant. Using the *newest* per-task cursor as a
*push-wide* floor means any issue whose last update predates the most recently
reconciled task is excluded from every future backfill. Backfill exists
specifically to recover events missed while the worker was down, and this is the
one class of event it structurally cannot see.

**Failure scenario (reproduced).** Issue #5 closes at 10:00; its webhook is lost
(a queue 503, a botched secret rotation, an expired arq job). Issue #6 closes at
12:00 and its webhook lands, setting `task6.synced_at = 12:00`. The next
backfill sends `since=2026-01-01T12:00:00+00:00`; GitHub omits #5's 10:00 update;
task #5 stays `open` forever. The workspace permanently under-reports shipped
work, and every subsequent backfill repeats the same exclusion.

The existing suite misses this because `_StubIssuesClient`
(`tests/test_github_drift_backfill.py:235`) records `since` but ignores it —
it returns the full list regardless.

**Fix shape.** The cursor must be the **minimum** over tasks that could still
change (or `None` when any task has no `synced_at`), not the maximum. A
push-level "last full backfill" watermark would be the cleaner form.

---

## F3 — Uninstalling the App permanently severs a workspace from inbound sync, even after reinstall

**Severity: High.** `services/integrations/github_install_service.py:616-635`
(`_detach_and_stale_pushes`).

On `installation.deleted` the handler sets `installation_id = NULL` on the
install's pushes and deletes the install row. Its docstring asserts:

> drift/reconcile still works via the immutable `repo_id`.

That is not true of the code. Every inbound path resolves through
`find_live_pushes_for_event` (`push_repo.py:150`), which **INNER JOINs**
`GitHubInstallation` on `IntegrationPush.installation_id`. With the column
NULLed, the join yields nothing, so no webhook can ever reach those pushes
again. Nothing re-attaches the column: `upsert_installation` creates a new row
with a fresh UUID `id` and does not adopt orphaned pushes.

**Failure scenario (reproduced).** A user uninstalls the App and reinstalls it
on the same org and repo (same GitHub `installation_id`).
`find_live_pushes_for_event(repo_id, installation_id)` returns `[]` where it
previously returned the workspace's push. Concretely, from the repro:

```
assert [] == [UUID('ddee617f-…')]
```

Every recovery route is closed too:
- `_run_backfill` (`github_reconcile.py:571-577`) returns early when the
  installation lookup is `None`.
- `POST /sync/resync` still finds the push (it is `stale`, not `failed`), enqueues
  `export_push`, and `run_export_push` (`github_export_service.py:474`) then marks
  it **`failed`** — which drops it from `find_live_push` entirely and removes the
  sync surface.

Only a full re-export from the modal recovers it, because `prepare_export_push`
re-binds `installation_id` by `(workspace_id, provider)` regardless of status.

**Fix shape.** Either keep `installation_id` and add an `ON DELETE SET NULL`-free
soft-delete on the install row, or re-adopt orphaned pushes in
`upsert_installation` by `repo_id`. At minimum, correct the docstring — reconcile
does **not** work via `repo_id` alone.

---

## F4 — A merged PR's `pr_merge` attribution is lost whenever the issue event lands first

**Severity: Medium.** `services/integrations/github_reconcile.py:328`
(`_apply_done`).

`_apply_done` is documented to allow a `manual` → `pr_merge` upgrade, but the
out-of-order gate returns **before** the upgrade branch:

```python
if task.synced_at is not None and event_ts < task.synced_at:
    return False              # <- the upgrade below is unreachable
task.synced_at = event_ts
if task.state != "done": ...
if done_via == "pr_merge" and task.done_via != "pr_merge":
    task.done_via = "pr_merge"
```

Merging a PR emits two deliveries. `issues` routes to the **fast** lane and
`pull_request` to the **bulk** lane (`routers/integrations.py:395`), so the
issue event normally wins the race — and its `issue.updated_at` (when GitHub
auto-closed the issue) is at or after the PR's `merged_at`. Whenever it is
strictly after, the PR event is rejected as stale and the attribution stays
`manual`.

**Failure scenario (reproduced).** `merged_at = 12:00:00Z`,
`issue.updated_at = 12:00:01Z`. The issue event sets `done_via='manual'`; the PR
event is dropped by the gate; `done_via` stays `'manual'`. The task list renders
"Shipped" instead of "Shipped · via PR"
(`frontend/src/pages/WorkspaceGitHub.tsx:134`), and `_audit_done`'s `pr_merge`
row is never written.

The existing `test_merged_pr_alone_sets_done_via_pr_merge` passes because it
delivers the PR event *alone* — the interleaving is untested.

**Fix shape.** Attribution is monotonic (`manual` → `pr_merge`, never back), so
the upgrade should be applied before the freshness gate, or the gate should only
guard `state`/`synced_at` and not the attribution.

---

## F5 — Deduped-but-unprocessed webhook deliveries are silently lost forever

**Severity: Medium.** `routers/integrations.py:376-401`, `models/github_webhook_event.py`,
`services/integrations/github_reconcile.py:493`.

The ingress writes the `github_webhook_events` dedup row on **receipt**, before
the worker runs. `_mark_processed` later stamps `processed_at` — and
`processed_at` is read by **nothing**: no sweep, no replay lane, no alert. An
unfiltered `grep -rn processed_at backend harness` returns exactly seven hits —
the migration, the model column, the write at `github_reconcile.py:502`, one
test assertion, and three billing/Stripe rows. There is no production reader.

So if the row commits and the job then never executes — bulk lane down long
enough for the arq job to expire, the worker OOM-killed past its retry budget, a
dead-letter that is never replayed — GitHub's redelivery hits the unique
constraint, returns `{"status": "duplicate"}`, and the event is gone with no
record that it was never applied.

This is the exact failure the billing side engineered against: `billing_reconcile`
lane 1 replays unprocessed inbox rows. GitHub has no equivalent. It also
compounds F2 — the backfill that would otherwise cover the gap cannot see events
older than its cursor.

**Fix shape.** A periodic sweep for `processed_at IS NULL AND received_at <
now() - interval` that re-enqueues (the raw body would need persisting, or the
sweep can trigger a `backfill_repo` for the affected repo), plus a metric so the
condition is visible.

---

## F6 — `allow_signup=false` does not do what its comment claims

**Severity: Low (documentation).** `services/integrations/github_install_service.py:200-207`.

```python
# Force a fresh authorization so a stale cached grant can't satisfy the
# proof-of-control check for an installation the user no longer administers.
"allow_signup": "false",
```

`allow_signup` controls whether an unauthenticated visitor may create a GitHub
account mid-flow; it has no bearing on re-authorization. The stated property is
not delivered by this parameter.

The security outcome is nonetheless sound — `user_can_access_installation` calls
`GET /user/installations` live, which reflects current access rather than a
cached grant — so this is a misleading comment, not an exploitable hole. It
matters because a future reader may rely on the claim.

---

## F7 — Two racing first-time exports wedge a workspace's export endpoint at 500 forever

**Severity: High.** `services/pipeline/github_export_service.py:365-372`
(`prepare_export_push`) and `:1289-1295` (`_upsert_push_row`).

Both resolve the workspace's push with

```python
select(IntegrationPush).where(workspace_id == …, provider == 'github')
... .scalar_one_or_none()
```

with **no status filter and no uniqueness guarantee behind it**. Migration
`0016` *drops* `uq_integration_push_workspace_provider` and replaces it with the
partial index `uq_integration_push_workspace_repo_active` on
`(workspace_id, repo_id) WHERE status <> 'failed'` (`0016:190-196`). A
first-time export inserts with `repo_id IS NULL`, and Postgres treats NULLs as
distinct in a unique index — so nothing prevents two rows for one workspace.

`POST /workspaces/{id}/export/github` (`routers/workspace.py:504`) has no
application-level lock: two concurrent submits (a double-click, an impatient
retry, the frontend re-firing) both SELECT `None` and both INSERT.

**Failure scenario (reproduced).** With two `(workspace_id,'github')` rows
present, `prepare_export_push` raises:

```
MultipleResultsFound: Multiple rows were found when one or none was required
```

The route catches only `ExportNotReadyError`, `GitHubRateLimitError`, and
`GitHubAPIError`, so this surfaces as an **unhandled 500** — and it is not
transient: every future export, and the legacy `push_to_github` path, hits the
same query. The workspace can never be exported again without manual DB
surgery. The codebase's own docstrings already assume this multiplicity is
possible (`find_workspace_live_push`: "the index does not forbid a second live
row"; `list_user_live_exports`: "A workspace may accumulate several non-`failed`
push rows"), and both of those callers correctly use `.order_by(...).first()` —
these two do not.

**Fix shape.** Match the tolerant readers (`order_by(created_at.desc()).first()`),
or restore a uniqueness guarantee for the `repo_id IS NULL` case with a second
partial index.

---

## Verified-correct (no action)

- **Webhook ingress ordering** — raw-bytes read → constant-time HMAC over the
  `[secret, prev]` rotation list → 400 before any DB/queue work → dedup insert →
  enqueue → commit. The deliberate enqueue-before-commit reordering is correct
  and documented.
- **Confused-deputy scoping** — every mutating path resolves through
  `find_live_pushes_for_event` on the immutable `repo_id` **and** the delivery's
  own `installation.id`. Install A cannot reach a push under install B.
- **Push status vocabulary** — `pending`/`completed`/`failed`/`stale` is
  consistent across models, services, schemas, and the frontend; no `in_progress`/
  `success`/`error` residue survives (the `in_progress` hits in `stage_manager.py`
  are the unrelated stage status).
- **Installation token cache** — per-installation Redis key, Fernet at rest, TTL
  margined 300s below GitHub's expiry, fail-open on Redis error, one bounded
  re-mint on 401.
- **`installation_id` uniqueness** — enforced by
  `uq_github_installation_installation_id`, so `_load_installation_by_number`'s
  `scalar_one_or_none()` cannot raise `MultipleResultsFound`.
- **Repo picker has no governor** — confirmed; `list_repos_for_installation`
  builds its client without one, so an interactive burst cannot spend the
  worker's write budget.
- **arq job-id dedup** — `_KEEP_RESULT_SECONDS = 0` means a completed job's key
  clears, so a re-export under the same `push_id` is not silently dropped.
- **In-flight `VERIFICATION.json` change** (`_build_file_map`, uncommitted) —
  imports and resolves correctly; not implicated in anything above.

---

## Not covered

This review was not exhaustive. The following were read only at header/skim
level or not at all, and should not be treated as cleared:

- `services/integrations/pr_evaluator.py` beyond `_drive_pr_check` — the judge
  prompt, `_truncate_diff_by_hunk`, `_verdict_for`, the check-run posting, and
  the daily-budget accounting.
- `services/integrations/pr_export_builder.py` (PR-mode scaffold, CI workflow
  and task-stub generation) — not reviewed.
- `services/integrations/github_projects.py` past its module docstring — the
  GraphQL board/milestone sync and its permission fallbacks.
- `services/integrations/github_governor.py` beyond the Lua token bucket — the
  `observe()` header parsing and the repo-lock acquire/release internals.
- `services/integrations/agents_md_builder.py` — only the guard wiring was
  confirmed (`redact_unsafe_lines` is genuinely invoked at `:306` and `:325`, so
  the export choke point survived the in-flight edits); the rest of the builder
  was not read.
- The frontend beyond a skim of `WorkspaceGitHub.tsx`, `utils/githubHub.ts`, and
  `types/github.ts`: `GitHubHub.tsx`, `Settings.tsx`'s install panel,
  `TaskCompletionPanel`, `SyncStatusBanner`, `RepoPicker`, `ExportGitHubModal`,
  and `IncrementTimeline` were not reviewed.
- `github_auth_service.py` (the legacy Phase-13 OAuth path) — not reviewed.
