"""Behavioural tests for incremental GitHub sync + idea backlog (Phase 21 — T-280).

Two layers, both against the real DB (migrations applied):

- ``run_increment_push`` driven with an in-memory stub client: new task_refs →
  **new issues only**, existing ones updated, obsoleted ones closed-with-a-note,
  one milestone per increment, ``increment_id`` tagging, idempotent re-runs, and
  the load-bearing invariant that a *failed* increment push never drops the
  shared baseline push. PR-mode opens exactly one increment PR.
- The reconcile idea flow-back: a GitHub issue labelled ``enhancement`` lands in
  the workspace's ``increment_ideas`` backlog, deduped on re-delivery.
- The REST surface (increments timeline / push / ideas) through the ASGI stack.

The acceptance test ``test_increment_push_creates_only_new_issues`` seeds baseline
``IntegrationPushTask`` rows exactly as ``github_export_service._sync_issues``
persists them — keyed on the stable, content-derived ``compute_task_ref(title)``
(audit #2) — so the test mirrors how real layering works rather than a synthetic
key. ``test_increment_renumber_resync_keeps_issue_mapping`` covers the legacy →
stable migration on a renumber, including an increment-origin row.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from config import settings
from database import get_db, get_redis
from main import create_app
from middleware.auth import get_current_user
from models import (
    GitHubInstallation,
    Increment,
    IncrementIdea,
    IntegrationPush,
    IntegrationPushTask,
    Stage,
    StageVersion,
    User,
    Workspace,
)
from routers import workspace as workspace_router
from services.integrations import github_reconcile
from services.integrations.task_parser import compute_task_ref
from services.pipeline import increment_service

pytestmark = pytest.mark.asyncio


_BASELINE_TASKS = (
    "# Tasks\n\n"
    "### T-001: Set up project structure\n\n"
    "**Description:** Lay out the repository skeleton.\n\n"
    "### T-002: Build the parser\n\n"
    "**Description:** Implement the deterministic parser.\n"
)
_NEW_TASK = "\n### T-003: Add billing\n\n**Description:** Stripe checkout.\n"


# ---------------------------------------------------------------------------
# Stub GitHub client
# ---------------------------------------------------------------------------


class _StubClient:
    """In-memory stub of GitHubAPIClient covering the increment-push surface."""

    def __init__(self) -> None:
        self.created_issues: list[dict[str, Any]] = []
        self.updated_issues: list[tuple[int, str]] = []
        self.comments: list[tuple[int, str]] = []
        self.closed: list[int] = []
        self.milestones: dict[str, int] = {}
        self.issue_states: dict[int, str] = {}
        self.create_issue_error: Exception | None = None
        # Rows returned by list_issues (used by the legacy→stable task_ref
        # migration and the backfill reconcile). Each item: {number, title, ...}.
        self.list_issues_result: list[dict[str, Any]] = []
        self._issue_counter = 200
        self._milestone_counter = 0
        # PR-mode bookkeeping.
        self.branches: list[str] = []
        self.upserts: list[tuple[str, str | None]] = []
        self.open_prs: dict[str, int] = {}
        self._pr_counter = 500

    async def ensure_milestone(
        self, repo: str, title: str, *, description: str | None = None
    ) -> int:
        if title not in self.milestones:
            self._milestone_counter += 1
            self.milestones[title] = self._milestone_counter
        return self.milestones[title]

    async def create_issue(
        self,
        repo: str,
        title: str,
        body: str,
        *,
        labels: Any = None,
        milestone: int | None = None,
    ) -> int:
        if self.create_issue_error is not None:
            raise self.create_issue_error
        self._issue_counter += 1
        number = self._issue_counter
        self.created_issues.append(
            {"number": number, "title": title, "milestone": milestone}
        )
        self.issue_states[number] = "open"
        return number

    async def update_issue(
        self, repo: str, number: int, title: str, body: str, *, labels: Any = None
    ) -> None:
        self.updated_issues.append((number, title))

    async def get_issue_state(self, repo: str, number: int) -> str | None:
        return self.issue_states.get(number, "open")

    async def add_issue_comment(self, repo: str, number: int, body: str) -> None:
        self.comments.append((number, body))

    async def close_issue(self, repo: str, number: int) -> None:
        self.closed.append(number)
        self.issue_states[number] = "closed"

    async def list_issues(
        self, repo: str, *, state: str = "all", since: str | None = None, **_: Any
    ) -> list[dict[str, Any]]:
        return list(self.list_issues_result)

    # ----- PR mode -----
    async def get_ref(self, repo: str, ref: str) -> str:
        return "base-sha"

    async def create_branch(self, repo: str, branch: str, base_sha: str) -> None:
        self.branches.append(branch)

    async def get_file_sha(self, repo: str, path: str, *, ref: str | None = None):
        return None

    async def upsert_file(
        self,
        repo: str,
        path: str,
        content: str,
        sha: str | None,
        message: str,
        *,
        branch: str | None = None,
    ) -> None:
        self.upserts.append((path, branch))

    async def find_open_pull_number(self, repo: str, *, head: str) -> int | None:
        return self.open_prs.get(head)

    async def create_pull_request(
        self, repo: str, *, head: str, base: str, title: str, body: str
    ) -> int:
        self._pr_counter += 1
        self.open_prs[head] = self._pr_counter
        return self._pr_counter


# ---------------------------------------------------------------------------
# DB fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def session() -> AsyncSession:
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        yield db
    await engine.dispose()


async def _seed_baseline(
    db: AsyncSession,
    *,
    tasks_content: str = _BASELINE_TASKS,
    export_mode: str = "files_to_default",
) -> dict[str, Any]:
    """Create a user + workspace + finalised stages + a completed baseline push
    with two baseline IntegrationPushTask rows (keyed exactly as _sync_issues)."""
    user = User(
        email=f"inc-sync-{uuid4()}@example.com",
        google_id=f"g-{uuid4()}",
        name="U",
        avatar_url=None,
        credit_balance=100,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    ws = Workspace(
        user_id=user.id,
        name="WS",
        problem_statement="x" * 60,
        provider="anthropic",
        model="claude-sonnet-4-6",
        status="active",
    )
    db.add(ws)
    await db.commit()
    await db.refresh(ws)

    for stage_type, content in (
        ("spec", "# Spec\n\nProduct."),
        ("plan", "# Plan\n\nPython FastAPI architecture."),
        (
            "harness",
            "# Harness\n\n`harness/tests/test_api.py`\n```python\n"
            "def test_x():\n    assert True\n```\n",
        ),
        ("tasks", tasks_content),
    ):
        stage = Stage(
            workspace_id=ws.id,
            type=stage_type,
            content=content,
            status="finalised",
            current_version=1,
            finalised_at=datetime.now(UTC),
        )
        db.add(stage)
        await db.flush()
        db.add(
            StageVersion(stage_id=stage.id, version=1, content=content, created_by="ai")
        )
    await db.commit()

    inst = GitHubInstallation(
        installation_id=uuid4().int % 1_000_000_000,
        account_login="octo",
        account_type="Organization",
        repository_selection="all",
        user_id=user.id,
    )
    db.add(inst)
    await db.commit()
    await db.refresh(inst)

    push = IntegrationPush(
        workspace_id=ws.id,
        user_id=user.id,
        provider="github",
        installation_id=inst.id,
        repo_id=uuid4().int % 1_000_000_000,
        repo_full_name="octo/app",
        export_mode=export_mode,
        status="completed",
    )
    db.add(push)
    await db.commit()
    await db.refresh(push)

    db.add_all(
        [
            IntegrationPushTask(
                push_id=push.id,
                task_ref=compute_task_ref("Set up project structure"),
                external_issue_number=101,
                state="open",
            ),
            IntegrationPushTask(
                push_id=push.id,
                task_ref=compute_task_ref("Build the parser"),
                external_issue_number=102,
                state="open",
            ),
        ]
    )
    await db.commit()
    return {"user": user, "workspace": ws, "install": inst, "push": push}


async def _make_increment(
    db: AsyncSession, workspace: Workspace, *, sequence: int = 1, status: str = "ready"
) -> Increment:
    inc = Increment(
        workspace_id=workspace.id,
        sequence=sequence,
        title="Add billing",
        status=status,
        baseline_version_ids=[],
    )
    db.add(inc)
    await db.commit()
    await db.refresh(inc)
    return inc


async def _teardown(db: AsyncSession, seeded: dict[str, Any]) -> None:
    # Workspace delete cascades pushes/tasks/increments/ideas; install then user.
    await db.execute(delete(Workspace).where(Workspace.id == seeded["workspace"].id))
    await db.execute(
        delete(GitHubInstallation).where(GitHubInstallation.id == seeded["install"].id)
    )
    await db.execute(delete(User).where(User.id == seeded["user"].id))
    await db.commit()


# ---------------------------------------------------------------------------
# run_increment_push — service behaviour
# ---------------------------------------------------------------------------


async def test_increment_push_creates_only_new_issues(session: AsyncSession) -> None:
    """Acceptance: only the new task opens an issue (under a new milestone),
    existing tasks are updated, and the new task row is tagged with the
    increment."""
    seeded = await _seed_baseline(session, tasks_content=_BASELINE_TASKS + _NEW_TASK)
    inc = await _make_increment(session, seeded["workspace"])
    stub = _StubClient()
    stub.issue_states[101] = "open"
    stub.issue_states[102] = "open"
    try:
        await increment_service.run_increment_push(
            {}, str(inc.id), db=session, client=stub
        )

        # Exactly one NEW issue, for T-003, filed under the increment milestone.
        assert len(stub.created_issues) == 1
        created = stub.created_issues[0]
        assert created["title"] == "Add billing"
        milestone_title = f"Increment {inc.sequence}: {inc.title}"
        assert created["milestone"] == stub.milestones[milestone_title]
        # The two baseline tasks were updated in place, never recreated.
        assert {n for n, _ in stub.updated_issues} == {101, 102}

        # A new IntegrationPushTask row for T-003, tagged with the increment.
        rows = {
            r.task_ref: r
            for r in (
                await session.execute(
                    select(IntegrationPushTask).where(
                        IntegrationPushTask.push_id == seeded["push"].id
                    )
                )
            ).scalars()
        }
        ref_t1 = compute_task_ref("Set up project structure")
        ref_t3 = compute_task_ref("Add billing")
        assert set(rows) == {ref_t1, compute_task_ref("Build the parser"), ref_t3}
        assert rows[ref_t3].external_issue_number == created["number"]
        assert rows[ref_t3].increment_id == inc.id
        assert rows[ref_t1].increment_id is None  # baseline untouched

        await session.refresh(inc)
        assert inc.status == "pushed"
    finally:
        await _teardown(session, seeded)


async def test_increment_push_is_idempotent(session: AsyncSession) -> None:
    """Re-running an increment push never duplicates the new issue."""
    seeded = await _seed_baseline(session, tasks_content=_BASELINE_TASKS + _NEW_TASK)
    inc = await _make_increment(session, seeded["workspace"])
    stub = _StubClient()
    stub.issue_states[101] = "open"
    stub.issue_states[102] = "open"
    try:
        await increment_service.run_increment_push(
            {}, str(inc.id), db=session, client=stub
        )
        await increment_service.run_increment_push(
            {}, str(inc.id), db=session, client=stub
        )
        # Still exactly one create across two runs; second run updated it instead.
        assert len(stub.created_issues) == 1
        rows = (
            await session.execute(
                select(IntegrationPushTask).where(
                    IntegrationPushTask.push_id == seeded["push"].id
                )
            )
        ).scalars()
        refs = [r.task_ref for r in rows]
        assert sorted(refs) == sorted(
            compute_task_ref(t)
            for t in ("Set up project structure", "Build the parser", "Add billing")
        )  # no duplicate row
    finally:
        await _teardown(session, seeded)


async def test_increment_renumber_resync_keeps_issue_mapping(
    session: AsyncSession,
) -> None:
    """Audit #2 regression: legacy ``T-NNN`` rows (incl. an increment-origin row)
    are migrated to the stable identity from the live issue titles, so a sync
    over a **renumbered** TASKS updates the *same* issues — no duplicate is
    opened and no still-present task is wrongly closed.
    """
    # Current TASKS keeps the same three titles but RENUMBERED (T-005/6/7) vs the
    # legacy rows seeded as T-001/2/3 — the exact shape that corrupts a T-NNN key.
    renumbered = (
        "# Tasks\n\n"
        "### T-005: Set up project structure\n\n**Description:** Skeleton.\n\n"
        "### T-006: Build the parser\n\n**Description:** Parser.\n\n"
        "### T-007: Add billing\n\n**Description:** Stripe checkout.\n"
    )
    seeded = await _seed_baseline(session, tasks_content=renumbered)
    push = seeded["push"]

    # Replace the stable rows _seed_baseline created with LEGACY (T-NNN) rows,
    # including one increment-origin row (tagged with a prior increment).
    prior_inc = await _make_increment(session, seeded["workspace"], sequence=1)
    await session.execute(
        delete(IntegrationPushTask).where(IntegrationPushTask.push_id == push.id)
    )
    session.add_all(
        [
            IntegrationPushTask(
                push_id=push.id,
                task_ref="T-001",
                external_issue_number=101,
                state="open",
            ),
            IntegrationPushTask(
                push_id=push.id,
                task_ref="T-002",
                external_issue_number=102,
                state="open",
            ),
            IntegrationPushTask(
                push_id=push.id,
                task_ref="T-003",
                external_issue_number=103,
                state="open",
                increment_id=prior_inc.id,  # increment-origin row
            ),
        ]
    )
    await session.commit()

    inc = await _make_increment(session, seeded["workspace"], sequence=2)
    stub = _StubClient()
    # The live issue titles are the renumber-invariant identity source.
    stub.list_issues_result = [
        {"number": 101, "title": "Set up project structure"},
        {"number": 102, "title": "Build the parser"},
        {"number": 103, "title": "Add billing"},
    ]
    for n in (101, 102, 103):
        stub.issue_states[n] = "open"
    try:
        await increment_service.run_increment_push(
            {}, str(inc.id), db=session, client=stub
        )

        # No duplicate issue opened; nothing closed; all three updated in place.
        assert stub.created_issues == []
        assert stub.closed == []
        assert {n for n, _ in stub.updated_issues} == {101, 102, 103}

        rows = {
            r.task_ref: r
            for r in (
                await session.execute(
                    select(IntegrationPushTask).where(
                        IntegrationPushTask.push_id == push.id
                    )
                )
            ).scalars()
        }
        # Rows now carry the stable identity, exactly three (no duplicate row).
        assert set(rows) == {
            compute_task_ref("Set up project structure"),
            compute_task_ref("Build the parser"),
            compute_task_ref("Add billing"),
        }
        # The increment-origin row migrated too, keeping its mapping + tag.
        billing = rows[compute_task_ref("Add billing")]
        assert billing.external_issue_number == 103
        assert billing.increment_id == prior_inc.id
    finally:
        await _teardown(session, seeded)


async def test_task_ref_migration_fast_path_skips_github(
    session: AsyncSession,
) -> None:
    """Steady state (no legacy-shaped refs) must NOT call list_issues — the
    migration is free once every row is stable."""
    from services.integrations.task_ref_migration import migrate_legacy_task_refs

    seeded = await _seed_baseline(session)  # seeds rows on the stable key
    push = seeded["push"]

    class _ExplodingList(_StubClient):
        async def list_issues(self, *a: Any, **k: Any) -> list[dict[str, Any]]:
            raise AssertionError("list_issues must not be called on the fast path")

    try:
        migrated = await migrate_legacy_task_refs(
            session, push.id, _ExplodingList(), "octo/app"
        )
        assert migrated == 0
    finally:
        await _teardown(session, seeded)


async def test_task_ref_migration_collision_and_missing_title(
    session: AsyncSession,
) -> None:
    """Defensive paths: two issues with the same title migrate onto DISTINCT
    stable keys (lowest issue number keeps the unsalted ref, the next takes
    occurrence 1 — mirroring parse_tasks' document-order salting, and never
    violating uq_push_task_ref), and an issue whose title cannot be recovered is
    left on its legacy ref."""
    from services.integrations.task_ref_migration import (
        is_legacy_task_ref,
        migrate_legacy_task_refs,
    )

    seeded = await _seed_baseline(session)
    push = seeded["push"]
    await session.execute(
        delete(IntegrationPushTask).where(IntegrationPushTask.push_id == push.id)
    )
    session.add_all(
        [
            IntegrationPushTask(
                push_id=push.id, task_ref="T-001", external_issue_number=101
            ),
            IntegrationPushTask(  # same title as 101 → collision
                push_id=push.id, task_ref="T-002", external_issue_number=102
            ),
            IntegrationPushTask(  # title not in list_issues → unrecoverable
                push_id=push.id, task_ref="T-003", external_issue_number=103
            ),
        ]
    )
    await session.commit()

    stub = _StubClient()
    stub.list_issues_result = [
        {"number": 101, "title": "Same title"},
        {"number": 102, "title": "Same title"},
        # 103 deliberately absent (e.g. issue deleted on GitHub).
    ]
    try:
        migrated = await migrate_legacy_task_refs(session, push.id, stub, "octo/app")
        assert migrated == 2  # both "Same title" rows migrate, onto distinct keys

        rows = {
            r.external_issue_number: r.task_ref
            for r in (
                await session.execute(
                    select(IntegrationPushTask).where(
                        IntegrationPushTask.push_id == push.id
                    )
                )
            ).scalars()
        }
        # Issues are created in document order, so ascending issue number is
        # ascending occurrence: 101 is the first "Same title", 102 the second.
        assert rows[101] == compute_task_ref("Same title")
        assert rows[102] == compute_task_ref("Same title", occurrence=1)
        assert rows[101] != rows[102]
        assert not is_legacy_task_ref(rows[102])
        assert rows[103] == "T-003"  # unrecoverable → left legacy
    finally:
        await _teardown(session, seeded)


async def test_increment_push_closes_obsoleted_issue(session: AsyncSession) -> None:
    """A task dropped from TASKS has its issue closed with a note, idempotently."""
    # Current TASKS keeps only T-001 — T-002 is obsoleted.
    only_t1 = "# Tasks\n\n### T-001: Set up project structure\n\n**Description:** x.\n"
    seeded = await _seed_baseline(session, tasks_content=only_t1)
    inc = await _make_increment(session, seeded["workspace"])
    stub = _StubClient()
    stub.issue_states[101] = "open"
    stub.issue_states[102] = "open"
    try:
        await increment_service.run_increment_push(
            {}, str(inc.id), db=session, client=stub
        )
        # T-002's issue (#102) was commented on and closed; #101 left open.
        assert stub.comments and stub.comments[0][0] == 102
        assert stub.closed == [102]
        assert 101 not in stub.closed
        tracked_numbers = set(
            (
                await session.execute(
                    select(IntegrationPushTask.external_issue_number).where(
                        IntegrationPushTask.push_id == seeded["push"].id
                    )
                )
            ).scalars()
        )
        assert tracked_numbers == {101}

        # Idempotent: the retired mapping is gone, so a re-run cannot re-comment.
        await increment_service.run_increment_push(
            {}, str(inc.id), db=session, client=stub
        )
        assert stub.closed == [102]  # unchanged
        assert len(stub.comments) == 1  # no second comment
    finally:
        await _teardown(session, seeded)


async def test_failed_increment_push_keeps_baseline_push_alive(
    session: AsyncSession,
) -> None:
    """A failed increment push must NOT mark the shared baseline push failed —
    that would kill the workspace's bidirectional sync."""
    seeded = await _seed_baseline(session, tasks_content=_BASELINE_TASKS + _NEW_TASK)
    inc = await _make_increment(session, seeded["workspace"])
    stub = _StubClient()
    stub.issue_states[101] = "open"
    stub.issue_states[102] = "open"
    stub.create_issue_error = RuntimeError("github exploded")
    try:
        with pytest.raises(RuntimeError):
            await increment_service.run_increment_push(
                {}, str(inc.id), db=session, client=stub
            )
        await session.rollback()
        # rollback() expires every ORM instance in the session; re-load the rows
        # the assertions (and teardown) read so attribute access stays async-safe.
        for obj in (
            seeded["workspace"],
            seeded["push"],
            seeded["install"],
            seeded["user"],
            inc,
        ):
            await session.refresh(obj)
        # The baseline push is still live and completed; the increment is still
        # retryable (ready), never advanced to pushed.
        from services.integrations.push_repo import find_workspace_live_push

        live = await find_workspace_live_push(session, seeded["workspace"].id)
        assert live is not None and live.id == seeded["push"].id
        assert live.status == "completed"
        assert inc.status == "ready"
    finally:
        await _teardown(session, seeded)


async def test_increment_push_pr_mode_opens_one_pr(session: AsyncSession) -> None:
    """In pr_with_tests mode the increment opens exactly one PR for its new
    tasks, idempotent on re-run."""
    seeded = await _seed_baseline(
        session,
        tasks_content=_BASELINE_TASKS + _NEW_TASK,
        export_mode="pr_with_tests",
    )
    inc = await _make_increment(session, seeded["workspace"])
    stub = _StubClient()
    stub.issue_states[101] = "open"
    stub.issue_states[102] = "open"
    try:
        await increment_service.run_increment_push(
            {}, str(inc.id), db=session, client=stub
        )
        branch = f"thought2build/increment-{inc.sequence}"
        assert branch in stub.branches
        assert stub.open_prs.get(branch) is not None
        first_pr = stub.open_prs[branch]
        # Only the increment's own new task is scaffolded onto the branch.
        assert any(
            "T-003".lower() in path.lower() or "thought2build" in path
            for path, _ in stub.upserts
        )

        await increment_service.run_increment_push(
            {}, str(inc.id), db=session, client=stub
        )
        assert stub.open_prs[branch] == first_pr  # no second PR opened
    finally:
        await _teardown(session, seeded)


# ---------------------------------------------------------------------------
# Idea flow-back via reconcile
# ---------------------------------------------------------------------------


def _idea_issue_event(*, repo_id: int, installation_numeric: int, number: int) -> bytes:
    return json.dumps(
        {
            "action": "labeled",
            "repository": {"id": repo_id, "full_name": "octo/app"},
            "installation": {"id": installation_numeric},
            "issue": {
                "number": number,
                "title": "Add a dark mode toggle",
                "labels": [{"name": "enhancement"}],
                "updated_at": "2026-06-01T12:00:00Z",
            },
        }
    ).encode()


async def test_enhancement_issue_flows_into_backlog(session: AsyncSession) -> None:
    seeded = await _seed_baseline(session)
    try:
        raw = _idea_issue_event(
            repo_id=seeded["push"].repo_id,
            installation_numeric=seeded["install"].installation_id,
            number=77,
        )
        await github_reconcile.reconcile_event(
            {}, "d-idea", "issues", raw, db=session, enqueue_fn=None
        )
        ideas = (
            (
                await session.execute(
                    select(IncrementIdea).where(
                        IncrementIdea.workspace_id == seeded["workspace"].id
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(ideas) == 1
        assert ideas[0].source == "github"
        assert ideas[0].external_ref == "gh-issue:77"
        assert "dark mode" in ideas[0].text.lower()

        # Idempotent: a redelivery of the same issue inserts no second row.
        await github_reconcile.reconcile_event(
            {}, "d-idea-2", "issues", raw, db=session, enqueue_fn=None
        )
        ideas = (
            (
                await session.execute(
                    select(IncrementIdea).where(
                        IncrementIdea.workspace_id == seeded["workspace"].id
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(ideas) == 1
    finally:
        await _teardown(session, seeded)


# ---------------------------------------------------------------------------
# REST surface
# ---------------------------------------------------------------------------


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.store[key] = value

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def delete(self, *keys: str) -> None:
        for key in keys:
            self.store.pop(key, None)

    async def eval(self, *args: Any) -> int:
        return 1


def _build_app(engine, user: User):
    maker = async_sessionmaker(engine, expire_on_commit=False)
    redis = _FakeRedis()
    app = create_app(redis_client=redis)
    app.state.redis = redis

    async def _get_db():
        async with maker() as db:
            yield db

    app.dependency_overrides[get_db] = _get_db
    app.dependency_overrides[get_redis] = lambda: redis
    app.dependency_overrides[get_current_user] = lambda: user
    return app


def _client(app: Any) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def test_idea_endpoints_create_and_list(session: AsyncSession) -> None:
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        seeded = await _seed_baseline(db)
    app = _build_app(engine, seeded["user"])
    try:
        async with _client(app) as client:
            ws_id = seeded["workspace"].id
            created = await client.post(
                f"/workspaces/{ws_id}/ideas", json={"text": "Add SSO login"}
            )
            assert created.status_code == 201
            assert created.json()["source"] == "user"
            listed = await client.get(f"/workspaces/{ws_id}/ideas")
            assert listed.status_code == 200
            assert any(i["text"] == "Add SSO login" for i in listed.json())
    finally:
        async with maker() as db:
            await _teardown(db, seeded)
        await engine.dispose()


async def test_create_increment_links_promoted_idea_to_generated_version(
    session: AsyncSession,
) -> None:
    """The idea-to-version action must persist; it is not just a UI text copy."""
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        seeded = await _seed_baseline(db)
        idea = IncrementIdea(
            workspace_id=seeded["workspace"].id,
            source="user",
            external_ref=None,
            status="open",
            text="Add SSO login for teams",
        )
        db.add(idea)
        await db.commit()
        await db.refresh(idea)
        idea_id = idea.id

    async def fake_generate(
        workspace_id: Any,
        feature_request: str,
        user: User,
        db: AsyncSession,
        *,
        mode: str,
    ) -> Any:
        assert workspace_id == seeded["workspace"].id
        assert feature_request == "Add SSO login for teams"
        assert mode == "additive"
        increment = Increment(
            workspace_id=workspace_id,
            sequence=1,
            title="Add SSO login for teams",
            status="ready",
            baseline_version_ids=[],
        )
        db.add(increment)
        await db.commit()
        await db.refresh(increment)
        return SimpleNamespace(increment_id=increment.id, new_tasks=[object()])

    app = _build_app(engine, seeded["user"])
    monkey = pytest.MonkeyPatch()
    monkey.setattr(
        workspace_router.increment_service,
        "generate_increment",
        fake_generate,
    )
    try:
        async with _client(app) as client:
            response = await client.post(
                f"/workspaces/{seeded['workspace'].id}/increments",
                json={
                    "feature_request": "  Add SSO login for teams  ",
                    "mode": "additive",
                    "idea_id": str(idea_id),
                },
            )
        assert response.status_code == 201
        assert response.json()["new_task_count"] == 1

        async with maker() as db:
            linked = (
                await db.execute(
                    select(IncrementIdea).where(IncrementIdea.id == idea_id)
                )
            ).scalar_one()
            assert linked.status == "planned"
            assert str(linked.increment_id) == response.json()["id"]
    finally:
        monkey.undo()
        async with maker() as db:
            await _teardown(db, seeded)
        await engine.dispose()


async def test_increment_push_endpoint_enqueues_202(session: AsyncSession) -> None:
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        seeded = await _seed_baseline(db)
        inc = await _make_increment(db, seeded["workspace"])
    enqueued: list[tuple[Any, ...]] = []

    async def fake_enqueue(job: str, *args: Any, **kwargs: Any) -> str:
        enqueued.append((job, *args, kwargs.get("job_id")))
        return "job-id"

    app = _build_app(engine, seeded["user"])
    monkey = pytest.MonkeyPatch()
    monkey.setattr(workspace_router, "enqueue", fake_enqueue)
    try:
        async with _client(app) as client:
            ws_id = seeded["workspace"].id
            resp = await client.post(f"/workspaces/{ws_id}/increments/{inc.id}/push")
            assert resp.status_code == 202
            assert resp.json()["increment_id"] == str(inc.id)
        assert enqueued == [("increment_push", str(inc.id), str(inc.id))]

        # Timeline lists the increment.
        async with _client(app) as client:
            listed = await client.get(f"/workspaces/{ws_id}/increments")
            assert listed.status_code == 200
            assert [r["id"] for r in listed.json()] == [str(inc.id)]
    finally:
        monkey.undo()
        async with maker() as db:
            await _teardown(db, seeded)
        await engine.dispose()


async def test_increment_push_endpoint_409_without_baseline(
    session: AsyncSession,
) -> None:
    """Pushing an increment before any GitHub export is a 409, not a silent
    enqueue."""
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    # Seed a workspace with an increment but NO live push.
    async with maker() as db:
        user = User(
            email=f"inc-nobase-{uuid4()}@example.com",
            google_id=f"g-{uuid4()}",
            name="U",
            avatar_url=None,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
        ws = Workspace(
            user_id=user.id,
            name="WS",
            problem_statement="x" * 60,
            provider="anthropic",
            model="claude-sonnet-4-6",
            status="active",
        )
        db.add(ws)
        await db.commit()
        await db.refresh(ws)
        inc = await _make_increment(db, ws)
    app = _build_app(engine, user)
    try:
        async with _client(app) as client:
            resp = await client.post(f"/workspaces/{ws.id}/increments/{inc.id}/push")
            assert resp.status_code == 409
    finally:
        async with maker() as db:
            await db.execute(delete(Workspace).where(Workspace.id == ws.id))
            await db.execute(delete(User).where(User.id == user.id))
            await db.commit()
        await engine.dispose()
