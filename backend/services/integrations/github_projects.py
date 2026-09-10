"""Projects v2 board + milestone sync — the ``projects_sync`` worker job (T-281).

Surfaces a real project-management layer on GitHub for a pushed workspace:

- **Milestones ← Plan phases.** The Tasks stage groups tasks under
  ``## Phase N: <name>`` headings (the phases the Plan stage defines); each phase
  becomes a GitHub **milestone** (REST), and every task issue in that phase is
  filed under it.
- **Board columns ← task status.** Each task issue is added to a single
  **Projects v2** board (GraphQL-only) and its ``Status`` column is set from the
  task's ``open``/``done`` state — the only two states the model carries, so the
  mapping is binary (``open`` → *Todo*, ``done`` → *Done*; no fabricated *In
  Progress*).
- **Labels ← stage.** Task issues already carry the ``stage:tasks`` label from
  the agent-ready export (T-277); the board surfaces it directly — nothing to add.
- **Dependencies ← sub-issues (task-list fallback).** TASKS.md encodes no
  cross-task dependency graph (tasks are phase-ordered, not a DAG), so the
  phase/milestone grouping *is* the structure — the documented task-list fallback
  from spec §4.14.6. No synthetic dependency edges are invented.

Bidirectional sync (T-272) keeps Thought2Build aligned with the board; this job keeps
the board aligned with task state.

Design mirrors ``increment_service.run_increment_push`` (T-280):

- ``db``/``client`` are injectable for tests; production opens its own session
  and builds an App-mode client + per-installation governor bound to the push's
  installation.
- All writes run under the per-``repo_id`` write lock (T-274) so a concurrent
  export/increment/board-sync cannot race GitHub's secondary abuse limits.
- A failed sync **never marks the baseline push ``failed``** — that would drop
  the workspace's bidirectional sync via ``find_live_push``; the job simply
  raises so arq retries/backs-off/dead-letters and a later attempt resumes
  cleanly. The board is opt-in and additive, so a missing **Projects** App
  permission is caught and logged (not dead-lettered) while the REST milestones
  still apply.
"""

from __future__ import annotations

import re
from contextlib import nullcontext
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_shared_redis
from models import GitHubInstallation, IntegrationPush, IntegrationPushTask, Stage
from services.integrations.github_api_client import GitHubProjectsPermissionError
from services.integrations.task_parser import ParsedTask, parse_tasks

logger = structlog.get_logger(__name__)

# A ``## Phase N: <name>`` heading in the Tasks stage (see ``prompts/tasks.py``).
# Captures the heading text after ``##`` so it can title a milestone verbatim.
_PHASE_HEADING = re.compile(r"^##\s+(Phase\b[^\n]*?)\s*$", re.MULTILINE)

# Binary task-state → board column mapping. The option NAME is matched
# case-insensitively against the board's live ``Status`` options so a board that
# uses "To do" or "TODO" still resolves.
_STATE_TO_COLUMN = {"done": "Done", "open": "Todo"}

# The board's title is derived from the repo so one repo maps to one board.
_BOARD_TITLE_PREFIX = "Thought2Build"


async def trigger_board_sync(push_id: Any) -> None:
    """Best-effort: enqueue a board sync for a just-completed push (T-281).

    The forward producer of the ``projects_sync`` job: called at the end of an
    export (T-276) and an increment push (T-280) so the board reflects live task
    state without waiting for an inbound ``projects_v2_item`` event (the reverse
    direction). Fire-and-forget — the caller's work is already committed, so a
    queue outage must never fail it; the drift cron and the ``projects_v2_item``
    webhook also re-sync. Keyed by ``push_id`` so a redelivery dedups against an
    in-flight sync (a distinct namespace from the ``export_push`` job key, which
    is the bare ``push_id``).
    """
    try:
        from services.queue import enqueue

        await enqueue("projects_sync", str(push_id), job_id=f"projects-sync-{push_id}")
    except Exception:  # best-effort: board sync is eventually consistent
        logger.warning("github.projects.trigger_failed", push_id=str(push_id))


async def sync_board(
    ctx: dict[str, Any],
    push_id: str,
    *,
    db: AsyncSession | None = None,
    client: Any = None,
) -> None:
    """Worker entrypoint for the ``projects_sync`` job (T-281).

    ``db``/``client`` are injectable for tests; production opens a session and
    builds an App-mode client bound to the push's installation.
    """
    if db is not None:
        await _sync_board(db, push_id, client=client)
        return
    from database import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        await _sync_board(session, push_id, client=client)


async def _sync_board(db: AsyncSession, push_id: str, *, client: Any = None) -> None:
    push = (
        await db.execute(
            select(IntegrationPush).where(IntegrationPush.id == UUID(push_id))
        )
    ).scalar_one_or_none()
    if push is None:
        logger.warning("github.projects.missing", push_id=push_id)
        return
    if push.status == "failed" or push.repo_full_name is None or push.repo_id is None:
        # Not a live push to layer a board on (the reconcile producer only
        # enqueues for live pushes; a stale/failed row is logged and skipped —
        # a retry cannot help).
        logger.warning("github.projects.not_live", push_id=push_id, status=push.status)
        return

    tasks_stage = (
        await db.execute(
            select(Stage).where(
                Stage.workspace_id == push.workspace_id, Stage.type == "tasks"
            )
        )
    ).scalar_one_or_none()
    phased = _group_tasks_by_phase((tasks_stage.content if tasks_stage else "") or "")
    task_rows = await _load_push_task_rows(db, push.id)

    if client is not None:
        await _drive_board_sync(push, phased, task_rows, client, governor=None)
        return

    await _drive_board_sync_in_production(db, push, phased, task_rows)


async def _drive_board_sync_in_production(  # pragma: no cover - worker-only wiring
    db: AsyncSession,
    push: IntegrationPush,
    phased: list[tuple[str | None, list[ParsedTask]]],
    task_rows: dict[str, IntegrationPushTask],
) -> None:
    """Build the App-mode client + governor and drive the sync (production path).

    Mirrors the export/increment workers' wiring; deliberately does NOT mark the
    push failed on error so a transient board-sync failure cannot drop the
    baseline push. Exercised end-to-end by the worker, not unit tests.
    """
    from services.integrations.github_api_client import (
        make_app_github_client,
        make_shared_async_client,
    )
    from services.integrations.github_app_auth import make_token_provider
    from services.integrations.github_governor import make_governor

    installation = (
        await db.execute(
            select(GitHubInstallation).where(
                GitHubInstallation.id == push.installation_id
            )
        )
    ).scalar_one_or_none()
    if installation is None:
        logger.warning("github.projects.installation_missing", push_id=str(push.id))
        return

    async with make_shared_async_client() as http:
        redis = get_shared_redis()
        token_provider = make_token_provider(redis, http)
        governor = make_governor(installation.installation_id, redis)
        built = make_app_github_client(
            token_provider, installation.installation_id, http, governor=governor
        )
        await _drive_board_sync(push, phased, task_rows, built, governor=governor)


async def _drive_board_sync(
    push: IntegrationPush,
    phased: list[tuple[str | None, list[ParsedTask]]],
    task_rows: dict[str, IntegrationPushTask],
    client: Any,
    *,
    governor: Any,
) -> None:
    repo = push.repo_full_name
    assert repo is not None  # nosec B101 — guaranteed by the caller

    lock = (
        governor.repo_write_lock(push.repo_id)
        if governor is not None
        else nullcontext()
    )
    async with lock:
        # 1. Milestones (REST) first: independent of the board and resilient to a
        #    missing Projects permission, so phase grouping lands even if the
        #    board can't be written.
        await _sync_milestones(client, repo, phased, task_rows)
        # 2. Board (GraphQL): opt-in/additive — a missing Projects App permission
        #    is logged and skipped, not dead-lettered.
        try:
            await _sync_project_board(client, repo, push, phased, task_rows)
        except GitHubProjectsPermissionError as exc:
            logger.warning(
                "github.projects.permission_missing",
                push_id=str(push.id),
                detail=str(exc),
            )


async def _sync_milestones(
    client: Any,
    repo: str,
    phased: list[tuple[str | None, list[ParsedTask]]],
    task_rows: dict[str, IntegrationPushTask],
) -> None:
    """Ensure one milestone per phase and file each task issue under it (REST).

    Idempotent: ``ensure_milestone`` reuses a milestone by title and
    ``set_issue_milestone`` re-assigning the same value is a no-op PATCH.
    """
    for phase_title, tasks in phased:
        if phase_title is None:
            continue  # tasks outside any phase get no milestone
        number = await client.ensure_milestone(
            repo, phase_title, description="Thought2Build Plan phase."
        )
        if number is None:
            continue
        for task in tasks:
            # Rows are keyed on the stable compute_task_ref (audit #2); they were
            # migrated to it by the export/increment sync that triggers this.
            row = task_rows.get(task.task_ref)
            if row is not None:
                await client.set_issue_milestone(
                    repo, row.external_issue_number, number
                )


async def _sync_project_board(
    client: Any,
    repo: str,
    push: IntegrationPush,
    phased: list[tuple[str | None, list[ParsedTask]]],
    task_rows: dict[str, IntegrationPushTask],
) -> None:
    """Add each task issue to the repo's Projects v2 board, column ← task state.

    Idempotent: the board is found-or-created by title, ``addProjectV2ItemById``
    returns the existing card on a re-run, and setting a column to its current
    value is a no-op.
    """
    node_ids = await client.get_repo_node_ids(repo)
    if node_ids is None:
        logger.warning("github.projects.repo_node_unresolved", repo=repo)
        return
    repository_id, owner_id = node_ids
    project_id = await client.ensure_project_v2(
        owner_id, _board_title(repo), repository_id=repository_id
    )
    if project_id is None:
        logger.warning("github.projects.board_unresolved", repo=repo)
        return
    status_field = await client.get_project_v2_status_field(project_id)
    field_id, options = status_field if status_field is not None else (None, {})

    for _phase_title, tasks in phased:
        for task in tasks:
            row = task_rows.get(task.task_ref)  # stable key (audit #2)
            if row is None:
                continue
            content_id = await client.get_issue_node_id(repo, row.external_issue_number)
            if content_id is None:
                continue
            item_id = await client.add_project_v2_item(project_id, content_id)
            if item_id is None or field_id is None:
                continue
            option_id = _resolve_status_option(row.state, options)
            if option_id is not None:
                await client.set_project_v2_item_status(
                    project_id, item_id, field_id, option_id
                )


def _board_title(repo: str) -> str:
    """One board per repo: ``Thought2Build — owner/name``."""
    return f"{_BOARD_TITLE_PREFIX} — {repo}"


def _resolve_status_option(state: str, options: dict[str, str]) -> str | None:
    """Map a task ``state`` to a live board ``Status`` option id (case-insensitive)."""
    wanted = _STATE_TO_COLUMN.get(state)
    if wanted is None:
        return None
    lowered = {name.lower(): opt_id for name, opt_id in options.items()}
    return lowered.get(wanted.lower())


def _group_tasks_by_phase(
    content: str,
) -> list[tuple[str | None, list[ParsedTask]]]:
    """Bucket tasks by their enclosing ``## Phase N:`` heading, in document order.

    ``parse_tasks`` only tracks ``### T-NNN`` headings, so this splits the Tasks
    markdown on phase boundaries and parses each segment. Tasks before the first
    phase heading (rare) bucket under ``None`` (no milestone).
    """
    if not content:
        return []
    matches = list(_PHASE_HEADING.finditer(content))
    if not matches:
        tasks = parse_tasks(content)
        return [(None, tasks)] if tasks else []

    segments: list[tuple[str | None, str]] = []
    if matches[0].start() > 0:
        segments.append((None, content[: matches[0].start()]))
    for index, match in enumerate(matches):
        title = match.group(1).strip()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(content)
        segments.append((title, content[match.start() : end]))

    grouped: list[tuple[str | None, list[ParsedTask]]] = []
    for title, segment in segments:
        tasks = parse_tasks(segment)
        if tasks:
            grouped.append((title, tasks))
    return grouped


async def _load_push_task_rows(
    db: AsyncSession, push_id: UUID
) -> dict[str, IntegrationPushTask]:
    rows = (
        await db.execute(
            select(IntegrationPushTask).where(IntegrationPushTask.push_id == push_id)
        )
    ).scalars()
    return {row.task_ref: row for row in rows}
