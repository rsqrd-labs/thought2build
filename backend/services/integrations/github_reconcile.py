"""Bidirectional sync reconcile job (Phase 21 — T-272).

Runs on the worker as ``reconcile_event(delivery_id, event_type, raw)`` — the
single delivery dispatcher the webhook enqueues (T-271). It re-parses the stored
raw payload and routes by ``(event_type, action)``; closing a task's issue (or
merging the PR that closes it) flips that task to ``done`` in Thought2Build, so the
workspace becomes a live dashboard.

Security (confused-deputy, spec §12): payload identity is never trusted to widen
scope. A delivery for ``repository.id`` may only mutate pushes whose recorded
:class:`GitHubInstallation` matches the delivery's own ``installation.id`` — see
:func:`services.integrations.push_repo.find_live_pushes_for_event`. Resolution is
on the immutable numeric ``repo_id``, never the mutable ``repo_full_name``.

Out-of-order safety: a task carries ``synced_at`` as the high-water mark of the
*event timestamp* that last changed its state. A transition is applied only when
the incoming event's timestamp is not older than ``synced_at``, so a late
``reopened`` cannot regress a task a newer ``closed`` already completed.

INVARIANT — ``synced_at`` MUST only ever hold an *event* timestamp, never
``now()``. ``done_at`` is the wall-clock record; ``synced_at`` is the gate. If
any reconcile/backfill path sets ``synced_at = now()``, every subsequent
real-time event would be ``< now()`` and silently dropped forever (T-273 backfill
must respect this).
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import (
    GitHubInstallation,
    GitHubWebhookEvent,
    IncrementIdea,
    IntegrationPush,
    IntegrationPushTask,
    Stage,
    StageVersion,
)
from services.integrations import github_install_service
from services.integrations.github_api_client import (
    ISSUES_FETCH_CAP,
    make_app_github_client,
    make_shared_async_client,
)
from services.integrations.github_app_auth import (
    GitHubAppAuthError,
    make_token_provider,
)
from services.integrations.push_repo import find_live_pushes_for_event
from services.observability import (
    GITHUB_RECONCILE_LAG_SECONDS,
    GITHUB_WEBHOOK_REPLAYED_TOTAL,
)
from services.security.sanitizer import sanitize_text

GITHUB_PROVIDER = "github"

logger = structlog.get_logger(__name__)

# GitHub closing keywords that auto-close an issue when a PR merges. Parsing
# these from the PR body is the documented issue↔PR linkage (NOT branch-name
# inference, which the spec forbids).
_CLOSING_KEYWORDS_RE = re.compile(
    r"\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s+#(\d+)\b",
    re.IGNORECASE,
)

_ISSUE_ACTIONS = {"closed", "reopened", "edited"}
_PR_CHECK_ACTIONS = {"opened", "synchronize", "reopened"}
_LIFECYCLE_ACTIONS = {"suspend", "unsuspend", "deleted"}

# Issue events that may surface a backlog idea, and the labels that mark one
# (T-280). A GitHub issue tagged ``idea``/``enhancement`` flows back into the
# workspace's idea backlog as ``source='github'``.
_IDEA_ACTIONS = {"opened", "reopened", "labeled", "edited"}
_IDEA_LABELS = {"idea", "enhancement"}


async def reconcile_event(
    ctx: dict[str, Any],
    delivery_id: str,
    event_type: str,
    raw: bytes | str,
    *,
    db: AsyncSession | None = None,
    enqueue_fn: Any = None,
) -> None:
    """Worker entrypoint: dispatch one verified webhook delivery.

    Idempotent + at-least-once safe (the delivery was already deduped by the
    ``github_webhook_events`` row; re-running is a no-op). ``db`` / ``enqueue_fn``
    are injectable for tests; production opens a session and uses the real queue.
    """
    if db is not None:
        await _dispatch(db, delivery_id, event_type, raw, enqueue_fn)
        return
    from database import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        await _dispatch(session, delivery_id, event_type, raw, enqueue_fn)


async def _dispatch(
    db: AsyncSession,
    delivery_id: str,
    event_type: str,
    raw: bytes | str,
    enqueue_fn: Any,
) -> None:
    payload = json.loads(raw)
    action = payload.get("action")

    if event_type == "issues":
        if action in _ISSUE_ACTIONS:
            await _reconcile_issue(db, payload, action)
        if action in _IDEA_ACTIONS:
            await _capture_idea(db, payload)
    elif event_type == "pull_request":
        if action == "closed" and (payload.get("pull_request") or {}).get("merged"):
            await _reconcile_merged_pr(db, payload)
        if action in _PR_CHECK_ACTIONS:
            await _route_pr_check(db, payload, enqueue_fn)
    elif event_type == "check_suite":
        await _route_pr_check(db, payload, enqueue_fn)
    elif event_type == "projects_v2_item":
        await _route_projects_sync(db, payload, enqueue_fn)
    elif event_type == "installation":
        await _route_installation(db, payload, action)
    elif event_type == "installation_repositories":
        await _route_installation_repositories(db, payload, action)
    # Any other event type is acknowledged and ignored.

    await _mark_processed(db, delivery_id)
    await db.commit()


# ---------------------------------------------------------------------------
# Issue / PR reconciliation
# ---------------------------------------------------------------------------


async def _reconcile_issue(
    db: AsyncSession, payload: dict[str, Any], action: str
) -> None:
    """Apply an ``issues`` event. A plain close is attributed ``manual``; a
    merged PR that closed the issue upgrades it to ``pr_merge`` via the
    ``pull_request`` event."""
    repo_id, installation_id = _identity(payload)
    if repo_id is None or installation_id is None:
        return
    issue = payload.get("issue") or {}
    issue_number = issue.get("number")
    if not isinstance(issue_number, int):
        return
    event_ts = _parse_ts(issue.get("updated_at"))
    if event_ts is None:
        return

    if action == "edited":
        # A title/body edit is not a state transition. It must NOT advance the
        # out-of-order high-water mark (synced_at) — doing so could make a
        # later-arriving but earlier-timestamped 'closed' look stale and be
        # dropped. Thought2Build does not sync issue titles back, so ignore it.
        return

    tasks = await _matched_tasks(db, repo_id, installation_id, issue_number)
    for task in tasks:
        if action == "reopened":
            _apply_reopen(task, event_ts=event_ts)
        elif action == "closed":
            changed = _apply_done(task, done_via="manual", event_ts=event_ts)
            if changed:
                _audit_done(task, payload, done_via="manual")
    await _mark_pushes_reconciled(db, tasks)


async def _reconcile_merged_pr(db: AsyncSession, payload: dict[str, Any]) -> None:
    """A merged PR closes the issues it references with closing keywords; those
    tasks are completed with ``done_via='pr_merge'`` (authoritative)."""
    repo_id, installation_id = _identity(payload)
    if repo_id is None or installation_id is None:
        return
    pr = payload.get("pull_request") or {}
    event_ts = _parse_ts(pr.get("merged_at") or pr.get("updated_at"))
    if event_ts is None:
        return
    closed_numbers = _closing_issue_numbers(pr.get("body") or "")
    if not closed_numbers:
        return
    matched: list[IntegrationPushTask] = []
    for issue_number in closed_numbers:
        tasks = await _matched_tasks(db, repo_id, installation_id, issue_number)
        matched.extend(tasks)
        for task in tasks:
            changed = _apply_done(task, done_via="pr_merge", event_ts=event_ts)
            if changed:
                _audit_done(task, payload, done_via="pr_merge")
    await _mark_pushes_reconciled(db, matched)


async def _capture_idea(db: AsyncSession, payload: dict[str, Any]) -> None:
    """Flow a GitHub ``idea``/``enhancement`` issue into the workspace backlog.

    Confused-deputy scoped: the idea is attached only to workspaces whose live
    push matches the delivery's ``(repo_id, installation_id)`` — an issue from
    one installation can never seed another's backlog. Idempotent at-least-once:
    deduped on ``(workspace_id, external_ref)`` so re-seeing the same issue
    (``labeled`` then ``edited`` then a redelivery) inserts at most one row.
    """
    repo_id, installation_id = _identity(payload)
    if repo_id is None or installation_id is None:
        return
    issue = payload.get("issue") or {}
    number = issue.get("number")
    if not isinstance(number, int) or not _has_idea_label(issue):
        return

    pushes = await find_live_pushes_for_event(db, repo_id, installation_id)
    if not pushes:
        return  # not ours / wrong installation → ignore

    external_ref = f"gh-issue:{number}"
    raw_title = issue.get("title")
    text = (
        sanitize_text(str(raw_title)).strip() if isinstance(raw_title, str) else ""
    ) or f"GitHub issue #{number}"

    seen: set[Any] = set()
    for push in pushes:
        workspace_id = push.workspace_id
        if workspace_id in seen:
            continue
        seen.add(workspace_id)
        already = (
            await db.execute(
                select(IncrementIdea).where(
                    IncrementIdea.workspace_id == workspace_id,
                    IncrementIdea.external_ref == external_ref,
                )
            )
        ).scalar_one_or_none()
        if already is not None:
            continue
        db.add(
            IncrementIdea(
                workspace_id=workspace_id,
                source="github",
                external_ref=external_ref,
                text=text,
                status="open",
            )
        )
        logger.info(
            "github.reconcile.idea_captured",
            workspace_id=str(workspace_id),
            external_ref=external_ref,
            repo_id=repo_id,
            installation_id=installation_id,
        )


def _has_idea_label(issue: dict[str, Any]) -> bool:
    """True if the issue carries an ``idea`` or ``enhancement`` label."""
    for label in issue.get("labels") or []:
        name = label.get("name") if isinstance(label, dict) else label
        if isinstance(name, str) and name.lower() in _IDEA_LABELS:
            return True
    return False


async def _matched_tasks(
    db: AsyncSession,
    repo_id: int,
    installation_id: int,
    issue_number: int,
) -> list[IntegrationPushTask]:
    """The task rows a delivery may mutate, confused-deputy scoped.

    Resolves the live pushes for ``(repo_id, installation_id)`` — never by
    ``repo_full_name`` — then the matching ``external_issue_number`` task in each
    (two workspaces may track the same repo; apply to all that match).
    """
    pushes = await find_live_pushes_for_event(db, repo_id, installation_id)
    if not pushes:
        return []  # not ours / wrong installation → ignore
    result = await db.execute(
        select(IntegrationPushTask).where(
            IntegrationPushTask.push_id.in_([p.id for p in pushes]),
            IntegrationPushTask.external_issue_number == issue_number,
        )
    )
    return list(result.scalars())


async def _mark_pushes_reconciled(
    db: AsyncSession, tasks: list[IntegrationPushTask]
) -> None:
    """Record wall-clock completion without touching event-order cursors."""
    push_ids = {task.push_id for task in tasks}
    if not push_ids:
        return
    pushes = list(
        (
            await db.execute(
                select(IntegrationPush).where(IntegrationPush.id.in_(push_ids))
            )
        ).scalars()
    )
    completed_at = datetime.now(UTC)
    for push in pushes:
        push.last_inbound_sync_at = completed_at
        push.last_inbound_sync_error = None


def _apply_done(
    task: IntegrationPushTask,
    *,
    done_via: str,
    event_ts: datetime,
) -> bool:
    """Complete a task. Out-of-order gated on ``synced_at``; allows a
    ``manual`` → ``pr_merge`` upgrade but never a downgrade. Returns True only on
    a fresh open→done transition (for the audit row).

    Attribution is settled BEFORE the freshness gate, deliberately. Merging a PR
    emits two deliveries: ``issues.closed`` (fast lane) and
    ``pull_request.closed`` with ``merged`` (bulk lane). The issue event normally
    wins that race, and GitHub stamps the issue's ``updated_at`` at or *after*
    the PR's ``merged_at`` — so gating first made the authoritative ``pr_merge``
    event look stale and the documented upgrade never fired. ``pr_merge`` is
    monotonic (it is never downgraded and never re-derived), so applying it out
    of order is safe in a way that ``state``/``synced_at`` are not; those stay
    behind the gate.
    """
    if done_via == "pr_merge" and task.state == "done" and task.done_via != "pr_merge":
        # Only ever upgrades an existing completion — never records an
        # attribution for a task that is still open.
        task.done_via = "pr_merge"
    if task.synced_at is not None and event_ts < task.synced_at:
        return False
    task.synced_at = event_ts
    if task.state != "done":
        task.state = "done"
        task.done_at = datetime.now(UTC)
        task.done_via = done_via
        return True
    return False


def _apply_reopen(task: IntegrationPushTask, *, event_ts: datetime) -> bool:
    """Reopen a task iff the event is not older than the last applied event
    (out-of-order guard: a stale ``reopened`` cannot regress a newer close)."""
    if task.synced_at is not None and event_ts < task.synced_at:
        return False
    task.synced_at = event_ts
    if task.state == "done":
        task.state = "open"
        task.done_at = None
        task.done_via = None
        return True
    return False


# ---------------------------------------------------------------------------
# Fan-out routing (forward-coupled jobs: T-281/T-282)
# ---------------------------------------------------------------------------


async def _route_pr_check(
    db: AsyncSession, payload: dict[str, Any], enqueue_fn: Any
) -> None:
    """pull_request(opened|synchronize|reopened) and check_suite → ``pr_check``
    (T-282), one job per matched push.

    A ``check_suite:rerequested`` event is an explicit user re-run, so it is
    tagged ``manual``; every other routed event is an automatic push tagged
    ``auto``. The pr_check worker uses that to honour the installation's
    ``manual`` pr_check_mode (issue #27 Phase 4)."""
    repo_id, installation_id = _identity(payload)
    if repo_id is None or installation_id is None:
        return
    pr_number = _pr_number(payload)
    if pr_number is None:
        return
    trigger = "manual" if payload.get("action") == "rerequested" else "auto"
    pushes = await find_live_pushes_for_event(db, repo_id, installation_id)
    for push in pushes:
        await _enqueue(enqueue_fn, "pr_check", str(push.id), pr_number, trigger)


async def _route_projects_sync(
    db: AsyncSession, payload: dict[str, Any], enqueue_fn: Any
) -> None:
    """projects_v2_item → ``projects_sync`` (T-281) for each matched push, when
    the payload carries a resolvable repository."""
    repo_id, installation_id = _identity(payload)
    if repo_id is None or installation_id is None:
        return
    pushes = await find_live_pushes_for_event(db, repo_id, installation_id)
    for push in pushes:
        await _enqueue(enqueue_fn, "projects_sync", str(push.id))


async def _route_installation(
    db: AsyncSession, payload: dict[str, Any], action: str | None
) -> None:
    """installation suspend/unsuspend/deleted → the T-270 lifecycle handler."""
    if action not in _LIFECYCLE_ACTIONS:
        return
    installation_id = (payload.get("installation") or {}).get("id")
    if isinstance(installation_id, int):
        await github_install_service.apply_installation_event(
            db, action=action, installation_id=installation_id
        )


async def _route_installation_repositories(
    db: AsyncSession, payload: dict[str, Any], action: str | None
) -> None:
    """installation_repositories added/removed → the T-270 lifecycle handler."""
    installation_id = (payload.get("installation") or {}).get("id")
    if not isinstance(installation_id, int):
        return
    removed = [
        r.get("id")
        for r in (payload.get("repositories_removed") or [])
        if isinstance(r.get("id"), int)
    ]
    await github_install_service.apply_installation_repositories_event(
        db,
        action=action or "",
        installation_id=installation_id,
        removed_repo_ids=removed,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _identity(payload: dict[str, Any]) -> tuple[int | None, int | None]:
    """Return ``(repo_id, installation_id)`` from the payload, or Nones.

    The installation id is the confused-deputy key — absent ⇒ we cannot attribute
    the event safely, so the caller ignores it.
    """
    repo = payload.get("repository") or {}
    installation = payload.get("installation") or {}
    repo_id = repo.get("id")
    installation_id = installation.get("id")
    return (
        repo_id if isinstance(repo_id, int) else None,
        installation_id if isinstance(installation_id, int) else None,
    )


def _pr_number(payload: dict[str, Any]) -> int | None:
    pr = payload.get("pull_request")
    if isinstance(pr, dict) and isinstance(pr.get("number"), int):
        return pr["number"]
    # check_suite carries its associated PRs.
    suite = payload.get("check_suite") or {}
    prs = suite.get("pull_requests") or []
    for entry in prs:
        if isinstance(entry, dict) and isinstance(entry.get("number"), int):
            return entry["number"]
    return None


def _closing_issue_numbers(body: str) -> list[int]:
    """Issue numbers a PR body closes via GitHub closing keywords."""
    return [int(m) for m in _CLOSING_KEYWORDS_RE.findall(body)]


def _parse_ts(value: Any) -> datetime | None:
    """Parse a GitHub ISO-8601 timestamp into a tz-aware UTC datetime, or None.

    Always tz-aware so comparisons against the DB's tz-aware ``synced_at`` never
    raise (the bug class fixed in T-267's ``_parse_github_timestamp``).
    """
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


async def _enqueue(enqueue_fn: Any, job: str, *args: Any) -> None:
    if enqueue_fn is None:
        from services.queue import enqueue as _real_enqueue

        await _real_enqueue(job, *args)
    else:
        await enqueue_fn(job, *args)


async def _mark_processed(db: AsyncSession, delivery_id: str) -> None:
    """Stamp ``processed_at`` on the delivery row and record reconcile lag."""
    result = await db.execute(
        select(GitHubWebhookEvent).where(GitHubWebhookEvent.delivery_id == delivery_id)
    )
    row = result.scalar_one_or_none()
    if row is None:
        return
    now = datetime.now(UTC)
    row.processed_at = now
    # The payload is retained only so an unprocessed delivery can be replayed;
    # once applied it is dead weight. Clearing it here keeps the column holding
    # just the handful of stuck rows rather than every delivery for the 30-day
    # retention window, and keeps the row consistent with the replay filter
    # (``processed_at IS NULL AND payload IS NOT NULL``).
    row.payload = None
    received = row.received_at
    if received is not None:
        if received.tzinfo is None:
            received = received.replace(tzinfo=UTC)
        GITHUB_RECONCILE_LAG_SECONDS.observe(max(0.0, (now - received).total_seconds()))


def _audit_done(
    task: IntegrationPushTask, payload: dict[str, Any], *, done_via: str
) -> None:
    """Structured audit of a task completion — identifiers only, never the
    raw payload (spec §24.10)."""
    repo = payload.get("repository") or {}
    installation = payload.get("installation") or {}
    logger.info(
        "github.reconcile.task_done",
        push_id=str(task.push_id),
        task_ref=task.task_ref,
        issue_number=task.external_issue_number,
        repo_id=repo.get("id"),
        installation_id=installation.get("id"),
        done_via=done_via,
    )


# ---------------------------------------------------------------------------
# Backfill — recover events missed while the worker was down (T-273)
# ---------------------------------------------------------------------------


# A push stuck in 'pending' whose arq job no longer exists is treated as a
# crashed export and failed, so the partial unique index (status <> 'failed')
# stops blocking re-export of that repo. We key on arq job ABSENCE rather than a
# created_at age threshold: integration_pushes has no per-attempt timestamp
# (created_at is the first export's time and the row is reused), so a time
# threshold would false-positive a healthy re-export. The arq job_timeout
# (T-269) backstops a genuinely hung job.


async def backfill_repo(
    ctx: dict[str, Any],
    push_id: str,
    *,
    db: AsyncSession | None = None,
    client: Any = None,
) -> None:
    """Reconcile a push's task states from GitHub's issues list (T-273).

    Recovers closures/reopens missed while the worker was down. Idempotent with
    the webhook reconcile path: it reuses the same out-of-order-gated
    transitions, so re-seeing a state is a no-op and a webhook-set ``pr_merge``
    is never downgraded to ``manual``.
    """
    if db is not None:
        await _run_backfill(db, push_id, client)
        return
    from database import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        await _run_backfill(session, push_id, client)


async def _run_backfill(db: AsyncSession, push_id: str, client: Any) -> None:
    push = (
        await db.execute(select(IntegrationPush).where(IntegrationPush.id == push_id))
    ).scalar_one_or_none()
    if push is None or push.repo_full_name is None or push.repo_id is None:
        return
    installation = (
        await db.execute(
            select(GitHubInstallation).where(
                GitHubInstallation.id == push.installation_id
            )
        )
    ).scalar_one_or_none()
    if installation is None:
        return

    tasks = list(
        (
            await db.execute(
                select(IntegrationPushTask).where(
                    IntegrationPushTask.push_id == push.id
                )
            )
        ).scalars()
    )
    if not tasks:
        now = datetime.now(UTC)
        push.last_inbound_sync_at = now
        push.last_full_backfill_at = now
        push.last_inbound_sync_error = None
        await db.commit()
        return
    by_number = {t.external_issue_number: t for t in tasks}
    # Captured BEFORE the fetch so an issue updated while the call is in flight
    # is re-pulled by the next sweep rather than skipped by a watermark that has
    # already moved past it.
    sweep_started_at = datetime.now(UTC)
    previous_watermark = _as_utc(push.last_full_backfill_at)
    since = _backfill_since(push)

    if client is not None:
        issues = await client.list_issues(push.repo_full_name, state="all", since=since)
    else:
        try:
            async with make_shared_async_client() as http:
                from database import get_shared_redis

                provider = make_token_provider(get_shared_redis(), http)
                built = make_app_github_client(
                    provider, installation.installation_id, http
                )
                issues = await built.list_issues(
                    push.repo_full_name, state="all", since=since
                )
        except GitHubAppAuthError as exc:
            if exc.status != 404:
                raise
            # GitHub no longer recognises this installation. Retrying with
            # exponential backoff cannot recover it, so fail fast and persist a
            # safe, actionable result for the waiting UI. Transient auth/server
            # failures still bubble into the durable retry policy.
            installation.suspended_at = datetime.now(UTC)
            push.last_inbound_sync_at = datetime.now(UTC)
            push.last_inbound_sync_error = "installation_unavailable"
            await db.commit()
            logger.warning(
                "github.backfill.installation_unavailable",
                push_id=str(push.id),
                installation_row_id=str(installation.id),
            )
            return

    for issue in issues:
        # GitHub's issues endpoint also returns PRs — they carry a
        # 'pull_request' key. Filtering them out is mandatory (a PR is not a task
        # issue).
        if "pull_request" in issue:
            continue
        number = issue.get("number")
        task = by_number.get(number) if isinstance(number, int) else None
        if task is None:
            continue
        event_ts = _parse_ts(issue.get("updated_at"))
        if event_ts is None:
            continue
        if issue.get("state") == "closed":
            _apply_done(task, done_via="manual", event_ts=event_ts)
        elif issue.get("state") == "open":
            _apply_reopen(task, event_ts=event_ts)
    # Completion is observable even when GitHub is already up to date. This is
    # separate from each task's event-time ``synced_at`` ordering cursor.
    push.last_inbound_sync_at = datetime.now(UTC)
    # Only a sweep that actually completed may advance the watermark — an early
    # return or a raised error must leave the window open for the next attempt.
    push.last_full_backfill_at = _resolved_watermark(
        issues,
        push=push,
        sweep_started_at=sweep_started_at,
        previous_watermark=previous_watermark,
    )
    push.last_inbound_sync_error = None
    await db.commit()


def _as_utc(value: datetime | None) -> datetime | None:
    """Normalise a stored timestamp to tz-aware UTC, or ``None``."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _resolved_watermark(
    issues: list[dict[str, Any]],
    *,
    push: IntegrationPush,
    sweep_started_at: datetime,
    previous_watermark: datetime | None,
) -> datetime:
    """Where the next sweep should resume from after this one.

    ``list_issues`` truncates SILENTLY at :data:`ISSUES_FETCH_CAP`. Advancing the
    watermark to ``sweep_started_at`` after a truncated fetch would skip every
    issue past the cap permanently — the same class of silent loss the watermark
    was introduced to fix, just harder to notice. A truncated sweep therefore
    resumes from the newest row it actually saw.

    The max is taken over EVERY returned row, pull requests included. GitHub
    returns PRs from the issues endpoint and the caller filters them afterwards;
    the cursor is about GitHub's update ordering, not about which rows we cared
    about, and a final page that happens to be all PRs would otherwise
    under-advance or produce no cursor at all.

    ``since`` is inclusive, so resuming at that row's own timestamp re-reads it —
    harmless, and it guarantees no gap.

    One degenerate case has to be caught: if the cap is full of rows that all
    share the current watermark's timestamp, the resume point equals the
    watermark and the sweep re-pulls the identical page every tick forever,
    burning API budget and never progressing. That is worse than a skipped
    window, so it takes the full window and says so loudly.
    """
    if len(issues) < ISSUES_FETCH_CAP:
        return sweep_started_at

    stamps = [
        parsed
        for parsed in (_parse_ts(issue.get("updated_at")) for issue in issues)
        if parsed is not None
    ]
    newest = max(stamps) if stamps else None
    if newest is not None and (
        previous_watermark is None or newest > previous_watermark
    ):
        logger.warning(
            "github.backfill.truncated_resuming",
            push_id=str(push.id),
            rows=len(issues),
            resume_at=newest.isoformat(),
        )
        return newest

    logger.error(
        "github.backfill.cursor_stalled",
        push_id=str(push.id),
        rows=len(issues),
        detail=(
            "a full page of issues shares the current cursor timestamp; "
            "advancing the full window to avoid re-pulling it every tick"
        ),
    )
    return sweep_started_at


def _backfill_since(push: IntegrationPush) -> str | None:
    """The ``since`` cursor for a push's issue sweep, or ``None`` for full history.

    This is the push-level ``last_full_backfill_at`` watermark — the start of the
    last sweep that actually completed — and NOT a function of per-task
    ``synced_at``.

    The per-task derivation this replaced took ``max(synced_at)`` across the
    push's tasks and used it as a push-wide floor. GitHub's ``since`` returns
    only issues updated at or after the cursor, so any issue whose last update
    predated the most recently reconciled task was excluded from every future
    sweep: an issue closed at 10:00 whose webhook was lost, followed by an
    unrelated issue closed and reconciled at 12:00, left the first one
    permanently invisible to the very mechanism meant to recover it. ``min``
    would not have fixed it either (a task that never reconciled has no
    ``synced_at`` to contribute), and falling back to ``None`` in that case
    means a full 20-page history pull on essentially every drift tick.
    """
    watermark = _as_utc(push.last_full_backfill_at)
    if watermark is None:
        return None
    return watermark.isoformat()


# ---------------------------------------------------------------------------
# Periodic drift reconciliation (cron) — stuck-pending sweep + backfill
# ---------------------------------------------------------------------------


async def reconcile_drift(
    ctx: dict[str, Any],
    *,
    db: AsyncSession | None = None,
    enqueue_fn: Any = None,
    is_job_alive: Any = None,
) -> None:
    """Periodic cron (T-273): sweep crashed 'pending' pushes and re-backfill.

    1. Stuck-pending sweep (release-blocking): a push 'pending' whose arq job is
       gone is a crashed export; mark it 'failed' so the repo is not permanently
       locked out of re-export by the live partial index.
    2. Catch missed events: enqueue ``backfill_repo`` for each completed push.
    3. Inbox replay: re-dispatch webhook deliveries that were recorded but never
       processed (see :func:`_replay_unprocessed_deliveries`).
    """
    if is_job_alive is None:
        is_job_alive = _arq_job_alive_checker((ctx or {}).get("redis"))
    if db is not None:
        await _run_reconcile_drift(db, enqueue_fn, is_job_alive)
        return
    from database import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        await _run_reconcile_drift(session, enqueue_fn, is_job_alive)


async def _run_reconcile_drift(
    db: AsyncSession, enqueue_fn: Any, is_job_alive: Any
) -> int:
    pending = list(
        (
            await db.execute(
                select(IntegrationPush).where(IntegrationPush.status == "pending")
            )
        ).scalars()
    )
    swept = 0
    for push in pending:
        if not await is_job_alive(str(push.id)):
            push.status = "failed"  # unblock re-export of this repo
            swept += 1
            logger.warning(
                "github.reconcile.stuck_pending_failed", push_id=str(push.id)
            )
    if swept:
        await db.commit()

    # Only App pushes are backfillable: ``_run_backfill`` returns immediately
    # without a GitHub call when ``repo_id`` is NULL (legacy v1-OAuth pushes,
    # which also carry no installation). Filtering them here avoids enqueuing a
    # job per legacy push every tick now that the legacy terminal status is the
    # canonical ``completed`` (audit #4) — wasted work that scales with the
    # legacy-push count (audit "worth a glance" #1).
    completed = list(
        (
            await db.execute(
                select(IntegrationPush).where(
                    IntegrationPush.status == "completed",
                    IntegrationPush.repo_id.isnot(None),
                )
            )
        ).scalars()
    )
    for push in completed:
        await _enqueue(enqueue_fn, "backfill_repo", str(push.id))

    await _replay_unprocessed_deliveries(db, enqueue_fn)
    return swept


# How long a delivery may sit unprocessed before the sweep replays it. This must
# comfortably exceed the drift cron's own period (15 min) plus a job's retry and
# backoff budget — the replay carries no arq ``job_id``, so a premature sweep
# would queue a second copy underneath a job that is merely slow or mid-retry.
_REPLAY_AFTER_SECONDS = 3600
# Bound the work one tick may schedule; a large backlog drains over several ticks
# rather than flooding the queue in one go.
_REPLAY_BATCH = 100
# Give up after this many replays. Without it, a delivery that can NEVER succeed
# (a handler bug, an installation GitHub now 404s) is re-enqueued every tick
# forever, burning a full retry budget each time — and because the sweep is
# batched and ordered oldest-first, enough such rows fill the batch and starve
# the newer, recoverable deliveries this whole mechanism exists to rescue. An
# exhausted row keeps ``processed_at IS NULL`` so it stays visible to the
# operator query in RUNBOOK §12.13; recovery is a workspace backfill.
_MAX_REPLAY_ATTEMPTS = 3


async def _replay_unprocessed_deliveries(db: AsyncSession, enqueue_fn: Any) -> int:
    """Re-dispatch verified deliveries that were recorded but never applied.

    The ingress commits the ``github_webhook_events`` dedup row on *receipt*, so
    a delivery whose job never ran — bulk lane down long enough for the arq job
    to expire, a worker killed past its retry budget, a dead-letter nobody
    replayed — is answered ``{"status": "duplicate"}`` on GitHub's redelivery and
    disappears with no trace. ``processed_at`` recorded exactly that condition
    and nothing ever read it. This is the reader, mirroring the billing inbox's
    replay lane.

    Replay is safe because dispatch is idempotent: the handlers are gated on
    ``synced_at`` and dedup on ``(workspace_id, external_ref)``, and re-applying
    a state a task already holds is a no-op. Rows with no stored payload (written
    before the column existed, or a body that would not parse) cannot be replayed
    and are left alone — they are visible as a persistently NULL
    ``processed_at``. Routing goes through the same job names the ingress uses,
    so ``issues`` still lands on the fast lane.
    """
    cutoff = datetime.now(UTC) - timedelta(seconds=_REPLAY_AFTER_SECONDS)
    rows = list(
        (
            await db.execute(
                select(GitHubWebhookEvent)
                .where(
                    GitHubWebhookEvent.processed_at.is_(None),
                    GitHubWebhookEvent.received_at < cutoff,
                    GitHubWebhookEvent.payload.isnot(None),
                    GitHubWebhookEvent.replay_count < _MAX_REPLAY_ATTEMPTS,
                )
                .order_by(GitHubWebhookEvent.received_at)
                .limit(_REPLAY_BATCH)
            )
        ).scalars()
    )
    replayed = 0
    for row in rows:
        job = (
            "reconcile_issue_event" if row.event_type == "issues" else "reconcile_event"
        )
        await _enqueue(
            enqueue_fn, job, row.delivery_id, row.event_type, json.dumps(row.payload)
        )
        row.replay_count = (row.replay_count or 0) + 1
        replayed += 1
        logger.warning(
            "github.reconcile.delivery_replayed",
            delivery_id=row.delivery_id,
            event_type=row.event_type,
            attempt=row.replay_count,
        )
        if row.replay_count >= _MAX_REPLAY_ATTEMPTS:
            logger.error(
                "github.reconcile.delivery_replay_exhausted",
                delivery_id=row.delivery_id,
                event_type=row.event_type,
            )
    if replayed:
        await db.commit()
        GITHUB_WEBHOOK_REPLAYED_TOTAL.inc(replayed)
    return replayed


def _arq_job_alive_checker(redis: Any) -> Any:
    """Return an ``async (push_id) -> bool`` that reports whether the push's arq
    job still exists. The push_id is the job_id (T-269). Conservative when the
    queue is unreachable: report alive so a real export is never falsely swept."""

    async def _alive(push_id: str) -> bool:
        if redis is None:
            return True
        from arq.jobs import Job, JobStatus

        status = await Job(job_id=push_id, redis=redis).status()
        return status != JobStatus.not_found

    return _alive


# ---------------------------------------------------------------------------
# Drift detection on Tasks re-finalise (called from StageManager.finalise)
# ---------------------------------------------------------------------------


async def mark_pushes_stale_on_tasks_drift(db: AsyncSession, workspace_id: Any) -> None:
    """Mark a workspace's live GitHub pushes ``stale`` when Tasks has drifted.

    Called inside ``StageManager.finalise`` (same transaction) when the Tasks
    stage is re-finalised: any non-``failed`` push whose
    ``source_stage_version_id`` differs from the workspace's current finalised
    Tasks ``StageVersion`` is now out of sync. The caller commits.
    """
    current_version_id = await _current_tasks_version_id(db, workspace_id)
    if current_version_id is None:
        return
    pushes = list(
        (
            await db.execute(
                select(IntegrationPush).where(
                    IntegrationPush.workspace_id == workspace_id,
                    IntegrationPush.provider == GITHUB_PROVIDER,
                    IntegrationPush.status != "failed",
                )
            )
        ).scalars()
    )
    for push in pushes:
        if (
            push.source_stage_version_id is not None
            and push.source_stage_version_id != current_version_id
        ):
            push.status = "stale"


async def _current_tasks_version_id(db: AsyncSession, workspace_id: Any) -> Any:
    stage = (
        await db.execute(
            select(Stage).where(
                Stage.workspace_id == workspace_id, Stage.type == "tasks"
            )
        )
    ).scalar_one_or_none()
    if stage is None:
        return None
    return (
        await db.execute(
            select(StageVersion.id).where(
                StageVersion.stage_id == stage.id,
                StageVersion.version == stage.current_version,
            )
        )
    ).scalar_one_or_none()
