"""Repository helpers for :class:`IntegrationPush` lookups (Phase 21 — T-266).

The "live push" for a repo is the single non-``failed`` :class:`IntegrationPush`
row for a ``(workspace_id, repo_id)`` pair. Migration ``0016`` enforces this with
the partial unique index ``uq_integration_push_workspace_repo_active`` keyed on
``(workspace_id, repo_id) WHERE status <> 'failed'`` — so at most one such row
can exist.

This module is the **single place** the ``status <> 'failed'`` definition is
expressed in query form. Every consumer that needs "the active push" — reconcile
(T-272), drift/sync (T-273), and re-export (T-269/T-276) — must go through
:func:`find_live_push` rather than writing its own predicate, so the rule cannot
drift back into a ``status = 'active'`` query (there is no ``'active'`` status
literal; the enum is ``pending``/``completed``/``failed``/``stale``).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models.github_installation import GitHubInstallation
from models.integration_push import IntegrationPush
from models.integration_push_task import IntegrationPushTask
from models.stage import Stage
from models.stage_version import StageVersion
from models.workspace import Workspace
from services.integrations.task_parser import parse_tasks

# The canonical "live push" predicate, mirroring the partial unique index
# predicate in migration 0016 (``status <> 'failed'``). Kept here, and only here,
# so the definition lives in exactly one place.
_NON_FAILED_STATUS = "failed"

TaskVersionSyncStatus = Literal["up_to_date", "changes_pending", "unknown"]


@dataclass(frozen=True)
class PushSyncMetadata:
    """Independent task-version and GitHub-connection state for one push.

    ``IntegrationPush.status == 'stale'`` is retained as a storage/lifecycle
    value for backwards compatibility, but it historically represented both a
    changed Tasks version and a suspended/removed installation.  Product
    surfaces must use this explicit metadata instead of guessing from that
    overloaded status.
    """

    task_sync_status: TaskVersionSyncStatus
    sync_paused: bool

    @property
    def out_of_sync(self) -> bool:
        return self.task_sync_status == "changes_pending"


async def resolve_push_sync_metadata(
    db: AsyncSession,
    pushes: Sequence[IntegrationPush],
) -> dict[UUID, PushSyncMetadata]:
    """Resolve sync metadata for ``pushes`` with two bounded queries.

    Task drift is an immutable-version comparison, not a push-status alias.
    Connection pause is derived independently from the App installation. Legacy
    OAuth pushes have no ``repo_id`` and therefore do not pretend to have an App
    connection state.
    """
    if not pushes:
        return {}

    workspace_ids = {push.workspace_id for push in pushes}
    current_rows = (
        await db.execute(
            select(Stage.workspace_id, StageVersion.id)
            .join(
                StageVersion,
                (StageVersion.stage_id == Stage.id)
                & (StageVersion.version == Stage.current_version),
            )
            .where(Stage.workspace_id.in_(workspace_ids), Stage.type == "tasks")
        )
    ).all()
    current_versions = {
        workspace_id: version_id for workspace_id, version_id in current_rows
    }

    installation_ids = {
        push.installation_id for push in pushes if push.installation_id is not None
    }
    installations: dict[UUID, object] = {}
    if installation_ids:
        installations = {
            installation_id: suspended_at
            for installation_id, suspended_at in (
                await db.execute(
                    select(
                        GitHubInstallation.id, GitHubInstallation.suspended_at
                    ).where(GitHubInstallation.id.in_(installation_ids))
                )
            ).all()
        }

    result: dict[UUID, PushSyncMetadata] = {}
    for push in pushes:
        current_version_id = current_versions.get(push.workspace_id)
        if push.source_stage_version_id is None or current_version_id is None:
            task_status: TaskVersionSyncStatus = "unknown"
        elif push.source_stage_version_id == current_version_id:
            task_status = "up_to_date"
        else:
            task_status = "changes_pending"

        # repo_id distinguishes App pushes from legacy OAuth pushes. An App push
        # with no installation row is detached/deleted and therefore paused.
        sync_paused = push.repo_id is not None and (
            push.installation_id is None
            or push.installation_id not in installations
            or installations[push.installation_id] is not None
        )
        result[push.id] = PushSyncMetadata(task_status, sync_paused)
    return result


async def find_live_push(
    db: AsyncSession,
    workspace_id: UUID,
    repo_id: int,
) -> IntegrationPush | None:
    """Return the single live (non-``failed``) push for a repo, or ``None``.

    The partial unique index guarantees at most one non-``failed`` row per
    ``(workspace_id, repo_id)``, so this resolves to a single row or nothing.

    ``repo_id`` is GitHub's immutable numeric repository id — the reconciliation
    key — never the mutable ``repo_full_name``.
    """
    result = await db.execute(
        select(IntegrationPush).where(
            IntegrationPush.workspace_id == workspace_id,
            IntegrationPush.repo_id == repo_id,
            IntegrationPush.status != _NON_FAILED_STATUS,
        )
    )
    return result.scalar_one_or_none()


async def find_live_pushes_for_event(
    db: AsyncSession,
    repo_id: int,
    installation_id: int,
) -> Sequence[IntegrationPush]:
    """Return the live pushes a GitHub webhook delivery may mutate.

    The confused-deputy guard (spec §12): a delivery for ``repo_id`` may only
    touch pushes whose recorded :class:`GitHubInstallation` matches the
    delivery's own ``installation.id`` (``installation_id`` here is GitHub's
    numeric id from the payload). An event from install A can never reach a push
    under install B, even for the same repo.

    Unlike :func:`find_live_push`, this is **not** keyed on the full
    ``(workspace_id, repo_id)`` unique tuple — two workspaces may export to the
    same repo under the same installation — so it returns *all* matching live
    pushes (zero ⇒ not ours, ignore). Callers iterate; never assume one row.
    """
    result = await db.execute(
        select(IntegrationPush)
        .join(
            GitHubInstallation,
            IntegrationPush.installation_id == GitHubInstallation.id,
        )
        .where(
            IntegrationPush.repo_id == repo_id,
            IntegrationPush.status != _NON_FAILED_STATUS,
            GitHubInstallation.installation_id == installation_id,
        )
    )
    return result.scalars().all()


@dataclass
class LiveExportRow:
    """One row of the account-wide exports hub feed (see
    :func:`list_user_live_exports`): a live push plus the display name of its
    workspace and the issue-completion counts, resolved together so the router
    never issues a per-push follow-up query."""

    push: IntegrationPush
    workspace_name: str
    total: int
    shipped: int
    task_sync_status: TaskVersionSyncStatus
    sync_paused: bool


async def list_user_live_exports(
    db: AsyncSession,
    user_id: UUID,
) -> list[LiveExportRow]:
    """Return the caller's live (non-``failed``) GitHub exports, one row per
    workspace, newest-first — the feed for the account-wide exports hub.

    A workspace may accumulate several non-``failed`` push rows over its life
    (e.g. an increment push layered on the baseline); ``DISTINCT ON`` keeps the
    newest per workspace, mirroring :func:`find_workspace_live_push`'s
    newest-first resolution so the hub row and the per-workspace ``/sync`` detail
    agree on *which* push is "the" export. Archived/trashed workspaces are
    excluded (``status = 'active'``). Every follow-up query is batched over the
    selected pushes/workspaces — never N+1.
    """
    push_rows = (
        await db.execute(
            select(IntegrationPush, Workspace.name)
            .join(Workspace, IntegrationPush.workspace_id == Workspace.id)
            .where(
                IntegrationPush.user_id == user_id,
                IntegrationPush.provider == "github",
                IntegrationPush.status != _NON_FAILED_STATUS,
                Workspace.status == "active",
            )
            .distinct(IntegrationPush.workspace_id)
            .order_by(
                IntegrationPush.workspace_id,
                IntegrationPush.created_at.desc(),
            )
        )
    ).all()
    if not push_rows:
        return []

    by_id = {push.id: (push, name) for push, name in push_rows}
    counts = (
        await db.execute(
            select(
                IntegrationPushTask.push_id,
                func.count().label("total"),
                func.count()
                .filter(IntegrationPushTask.state == "done")
                .label("shipped"),
            )
            .where(IntegrationPushTask.push_id.in_(list(by_id)))
            .group_by(IntegrationPushTask.push_id)
        )
    ).all()
    count_map = {pid: (total, shipped) for pid, total, shipped in counts}
    metadata = await resolve_push_sync_metadata(
        db, [push for push, _name in by_id.values()]
    )

    rows = [
        LiveExportRow(
            push=push,
            workspace_name=name,
            total=count_map.get(push.id, (0, 0))[0],
            shipped=count_map.get(push.id, (0, 0))[1],
            task_sync_status=metadata[push.id].task_sync_status,
            sync_paused=metadata[push.id].sync_paused,
        )
        for push, name in by_id.values()
    ]
    # Newest export first; a still-pending push has no pushed_at yet, so fall
    # back to created_at for a stable order.
    rows.sort(key=lambda r: (r.push.pushed_at or r.push.created_at), reverse=True)
    return rows


async def resolve_push_task_titles(
    db: AsyncSession,
    push: IntegrationPush,
) -> dict[str, tuple[str, str]]:
    """Map each of a push's ``task_ref``s to its human ``(T-NNN, title)``.

    The titles come from the push's **own** source Tasks :class:`StageVersion`
    (``source_stage_version_id``) — the exact version that produced these issues
    — so the ``compute_task_ref`` join is drift-proof: even if the workspace's
    current Tasks have since been renumbered/reworded, the refs here still match
    the version the issues were cut from. Returns an empty map when the source
    version is missing or empty (e.g. a legacy push), and omits any ref it can't
    resolve (an increment task not present in the baseline), so the caller falls
    back to the issue number for those. Read-only; never raises on bad content.
    """
    if push.source_stage_version_id is None:
        return {}
    version = await db.get(StageVersion, push.source_stage_version_id)
    if version is None or not version.content:
        return {}
    # Keyed on the task's own resolved ``task_ref``: a comprehension over
    # ``compute_task_ref(task.title)`` silently collapses same-titled tasks, and
    # the disambiguated sibling would then resolve no human ref at all (the UI
    # would fall back to "Issue #N" for a task that has a perfectly good title).
    return {
        task.task_ref: (task.ref, task.title) for task in parse_tasks(version.content)
    }


async def find_workspace_live_push(
    db: AsyncSession,
    workspace_id: UUID,
) -> IntegrationPush | None:
    """Return the workspace's live (non-``failed``) GitHub push, or ``None``.

    Used by the sync surface (T-273): a workspace syncs one repo (spec
    Assumption 8), but the ``(workspace_id, repo_id)`` index does not forbid a
    second live row, so this orders newest-first and takes the first rather than
    assuming exactly one.
    """
    result = await db.execute(
        select(IntegrationPush)
        .where(
            IntegrationPush.workspace_id == workspace_id,
            IntegrationPush.provider == "github",
            IntegrationPush.status != _NON_FAILED_STATUS,
        )
        .order_by(IntegrationPush.created_at.desc())
    )
    return result.scalars().first()
