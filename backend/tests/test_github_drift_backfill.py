"""Tests for drift detection, backfill, and stuck-pending recovery (T-273).

Real-DB fixtures (migration applied) drive the drift hook, the backfill
reconcile (with the mandatory pull-request-row filter), and the reconcile_drift
stuck-``pending`` sweep (keyed on arq job absence, injected here).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from config import settings
from models import (
    GitHubInstallation,
    IntegrationPush,
    IntegrationPushTask,
    Stage,
    StageVersion,
    User,
    Workspace,
)
from services.integrations import github_reconcile
from services.integrations.github_app_auth import GitHubAppAuthError

pytestmark = pytest.mark.asyncio

_REPO_ID = 770011
_T0 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)


def _unique_numeric() -> int:
    return uuid4().int % 1_000_000_000


@pytest.fixture
async def session() -> AsyncSession:
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        yield db
    await engine.dispose()


@pytest.fixture
async def user(session: AsyncSession) -> User:
    u = User(
        email=f"test-{uuid4()}@example.com",
        google_id=f"google-{uuid4()}",
        name="Tester",
        avatar_url=None,
    )
    session.add(u)
    await session.commit()
    await session.refresh(u)
    yield u
    await session.execute(delete(User).where(User.id == u.id))
    await session.commit()


@pytest.fixture
async def workspace(session: AsyncSession, user: User) -> Workspace:
    ws = Workspace(
        user_id=user.id,
        name="WS",
        problem_statement="x" * 60,
        provider="anthropic",
        model="claude-sonnet-4-6",
        status="active",
    )
    session.add(ws)
    await session.commit()
    await session.refresh(ws)
    yield ws
    await session.execute(delete(Workspace).where(Workspace.id == ws.id))
    await session.commit()


async def _make_tasks_stage(
    session: AsyncSession, workspace: Workspace, *, version: int = 1
) -> StageVersion:
    """Create a finalised Tasks stage at ``version`` and its StageVersion row."""
    stage = Stage(
        workspace_id=workspace.id,
        type="tasks",
        content="### T-001: do",
        status="finalised",
        current_version=version,
        finalised_at=datetime.now(UTC),
    )
    session.add(stage)
    await session.commit()
    await session.refresh(stage)
    sv = StageVersion(
        stage_id=stage.id, version=version, content="### T-001: do", created_by="user"
    )
    session.add(sv)
    await session.commit()
    await session.refresh(sv)
    return sv


async def _make_install(session: AsyncSession, user: User) -> GitHubInstallation:
    inst = GitHubInstallation(
        installation_id=_unique_numeric(),
        account_login="octo",
        account_type="Organization",
        repository_selection="all",
        user_id=user.id,
    )
    session.add(inst)
    await session.commit()
    await session.refresh(inst)
    return inst


async def _make_push(
    session: AsyncSession,
    *,
    workspace: Workspace,
    user: User,
    installation: GitHubInstallation,
    status: str,
    source_version_id: Any = None,
    repo_id: int = _REPO_ID,
) -> IntegrationPush:
    push = IntegrationPush(
        workspace_id=workspace.id,
        user_id=user.id,
        provider="github",
        installation_id=installation.id,
        repo_id=repo_id,
        repo_full_name="octo/app",
        status=status,
        source_stage_version_id=source_version_id,
    )
    session.add(push)
    await session.commit()
    await session.refresh(push)
    return push


async def _cleanup(session: AsyncSession, inst: GitHubInstallation) -> None:
    await session.execute(
        delete(IntegrationPush).where(IntegrationPush.installation_id == inst.id)
    )
    await session.execute(
        delete(GitHubInstallation).where(GitHubInstallation.id == inst.id)
    )
    await session.commit()


# ---------------------------------------------------------------------------
# Drift detection on Tasks re-finalise
# ---------------------------------------------------------------------------


async def test_tasks_refinalise_marks_push_out_of_sync(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    sv1 = await _make_tasks_stage(session, workspace, version=1)
    inst = await _make_install(session, user)
    # The push was built from version 1.
    push = await _make_push(
        session,
        workspace=workspace,
        user=user,
        installation=inst,
        status="completed",
        source_version_id=sv1.id,
    )
    try:
        # Re-finalise: Tasks advances to version 2 (a new StageVersion).
        stage = (
            await session.execute(
                select(Stage).where(
                    Stage.workspace_id == workspace.id, Stage.type == "tasks"
                )
            )
        ).scalar_one()
        stage.current_version = 2
        session.add(
            StageVersion(
                stage_id=stage.id,
                version=2,
                content="### T-001: changed",
                created_by="user",
            )
        )
        await session.commit()

        await github_reconcile.mark_pushes_stale_on_tasks_drift(session, workspace.id)
        await session.commit()
        await session.refresh(push)
        assert push.status == "stale"  # out of sync with the new Tasks version
    finally:
        await _cleanup(session, inst)


async def test_drift_leaves_matching_push_untouched(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    sv1 = await _make_tasks_stage(session, workspace, version=1)
    inst = await _make_install(session, user)
    push = await _make_push(
        session,
        workspace=workspace,
        user=user,
        installation=inst,
        status="completed",
        source_version_id=sv1.id,
    )
    try:
        # No re-finalise — current version still matches the push's source.
        await github_reconcile.mark_pushes_stale_on_tasks_drift(session, workspace.id)
        await session.commit()
        await session.refresh(push)
        assert push.status == "completed"  # not stale
    finally:
        await _cleanup(session, inst)


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------


class _StubIssuesClient:
    """Stub that HONOURS ``since`` the way GitHub does.

    It used to record the cursor and return every issue regardless, which made
    the whole class of "backfill sent a cursor that excluded the issue it was
    supposed to recover" invisible to the suite.
    """

    def __init__(self, issues: list[dict[str, Any]]) -> None:
        self._issues = issues
        self.calls: list[dict[str, Any]] = []

    async def list_issues(
        self, repo: str, *, state: str = "all", since: str | None = None
    ) -> list[dict[str, Any]]:
        self.calls.append({"repo": repo, "state": state, "since": since})
        if since is None:
            return list(self._issues)
        cutoff = datetime.fromisoformat(since.replace("Z", "+00:00"))
        kept = []
        for issue in self._issues:
            updated = issue.get("updated_at")
            if not isinstance(updated, str):
                kept.append(issue)
                continue
            if datetime.fromisoformat(updated.replace("Z", "+00:00")) >= cutoff:
                kept.append(issue)
        return kept


async def _make_task(
    session: AsyncSession, push: IntegrationPush, *, issue_number: int, state: str
) -> IntegrationPushTask:
    task = IntegrationPushTask(
        push_id=push.id,
        task_ref=f"task-{issue_number}",
        external_issue_number=issue_number,
        state=state,
    )
    session.add(task)
    await session.commit()
    await session.refresh(task)
    return task


async def test_backfill_filters_out_pull_request_rows(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    inst = await _make_install(session, user)
    push = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="completed"
    )
    task = await _make_task(session, push, issue_number=5, state="open")
    # Issue #5 is closed; #6 is a PR that happens to share the issues endpoint
    # and carries a 'pull_request' key — it must be ignored even though a task
    # with that number exists.
    pr_task = await _make_task(session, push, issue_number=6, state="open")
    issues = [
        {
            "number": 5,
            "state": "closed",
            "updated_at": _T0.isoformat().replace("+00:00", "Z"),
        },
        {
            "number": 6,
            "state": "closed",
            "pull_request": {"url": "https://api.github.com/.../pulls/6"},
            "updated_at": _T0.isoformat().replace("+00:00", "Z"),
        },
    ]
    try:
        await github_reconcile.backfill_repo(
            {}, str(push.id), db=session, client=_StubIssuesClient(issues)
        )
        await session.refresh(task)
        await session.refresh(pr_task)
        await session.refresh(push)
        assert task.state == "done"  # the real issue was reconciled
        assert pr_task.state == "open"  # the PR row was filtered out
        assert push.last_inbound_sync_at is not None
    finally:
        await _cleanup(session, inst)


async def test_backfill_does_not_downgrade_pr_merge_attribution(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    inst = await _make_install(session, user)
    push = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="completed"
    )
    task = await _make_task(session, push, issue_number=5, state="open")
    # A webhook already completed this task via a merged PR.
    task.state = "done"
    task.done_via = "pr_merge"
    task.synced_at = _T0
    await session.commit()
    # Backfill re-sees the same closed issue (only knows 'manual').
    issues = [
        {
            "number": 5,
            "state": "closed",
            "updated_at": _T0.isoformat().replace("+00:00", "Z"),
        }
    ]
    try:
        await github_reconcile.backfill_repo(
            {}, str(push.id), db=session, client=_StubIssuesClient(issues)
        )
        await session.refresh(task)
        await session.refresh(push)
        assert task.state == "done"
        assert task.done_via == "pr_merge"  # never downgraded to manual
        # A no-change refresh still records completion for the waiting client.
        assert push.last_inbound_sync_at is not None
    finally:
        await _cleanup(session, inst)


async def test_backfill_marks_deleted_installation_without_retrying(
    session: AsyncSession,
    user: User,
    workspace: Workspace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inst = await _make_install(session, user)
    push = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="completed"
    )
    await _make_task(session, push, issue_number=5, state="open")

    class _UnavailableClient:
        async def list_issues(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            raise GitHubAppAuthError(404, "installation unavailable")

    @asynccontextmanager
    async def _http_client():
        yield object()

    monkeypatch.setattr(github_reconcile, "make_shared_async_client", _http_client)
    monkeypatch.setattr(github_reconcile, "make_token_provider", lambda *_: object())
    monkeypatch.setattr(
        github_reconcile,
        "make_app_github_client",
        lambda *_: _UnavailableClient(),
    )
    try:
        # A 404 is permanent for this installation: the service records the
        # actionable result and returns normally, so the queue does not back off
        # and retry five times.
        await github_reconcile.backfill_repo({}, str(push.id), db=session)
        await session.refresh(push)
        await session.refresh(inst)
        assert push.last_inbound_sync_at is not None
        assert push.last_inbound_sync_error == "installation_unavailable"
        assert inst.suspended_at is not None
    finally:
        await _cleanup(session, inst)


# ---------------------------------------------------------------------------
# Stuck-pending recovery
# ---------------------------------------------------------------------------


async def test_reconcile_drift_fails_stuck_pending_when_job_gone(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    inst = await _make_install(session, user)
    stuck = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="pending"
    )

    async def job_dead(push_id: str) -> bool:
        return False  # arq has no record of the job → crashed export

    try:
        await github_reconcile.reconcile_drift(
            {}, db=session, enqueue_fn=_noop_enqueue, is_job_alive=job_dead
        )
        await session.refresh(stuck)
        # Failed so the partial unique index stops blocking re-export of the repo.
        assert stuck.status == "failed"
    finally:
        await _cleanup(session, inst)


async def test_reconcile_drift_leaves_live_pending_alone(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    inst = await _make_install(session, user)
    live = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="pending"
    )

    async def job_alive(push_id: str) -> bool:
        return True  # the export job is still queued / in flight

    try:
        await github_reconcile.reconcile_drift(
            {}, db=session, enqueue_fn=_noop_enqueue, is_job_alive=job_alive
        )
        await session.refresh(live)
        # A healthy in-flight export is NOT false-positived to failed.
        assert live.status == "pending"
    finally:
        await _cleanup(session, inst)


async def test_reconcile_drift_enqueues_backfill_for_completed_pushes(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    inst = await _make_install(session, user)
    push = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="completed"
    )
    enqueued: list[tuple[Any, ...]] = []

    async def fake_enqueue(job: str, *args: Any) -> None:
        enqueued.append((job, *args))

    async def job_alive(push_id: str) -> bool:
        return True

    try:
        await github_reconcile.reconcile_drift(
            {}, db=session, enqueue_fn=fake_enqueue, is_job_alive=job_alive
        )
        assert ("backfill_repo", str(push.id)) in enqueued
    finally:
        await _cleanup(session, inst)


async def _noop_enqueue(job: str, *args: Any) -> None:
    return None
