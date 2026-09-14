"""PostgreSQL integration tests for finalise() — locking, ownership, conflicts.

Requires TEST_DATABASE_URL env var pointing to a live PostgreSQL instance.
Skip if not set (CI injects it and runs this module in its own dedicated step,
outside the shared backend suite — issue #83 / BE29-003).

Run with (set TEST_DATABASE_URL first)::

    uv run pytest tests/test_finalise_integration.py -v

Event-loop safety (BE29-003)
----------------------------
Every fixture here is function-scoped and the engine uses ``NullPool``,
mirroring the other schema-owning integration suites: pytest-asyncio gives each
test its own event loop, so a module-scoped engine would hand asyncpg
connections created on one loop to tests running on another ("future attached
to a different event loop" / "another operation is in progress"). A per-test
engine with no pooled connections cannot cross loops. The engine fixture also
resets the disposable database's public schema at setup so leftover state from
a prior suite run in the same database can never leak in.

Isolation-level note
--------------------
Under READ COMMITTED (PostgreSQL / asyncpg default), session B unblocks after
A commits and re-reads the row, seeing status='finalised'.  The SELECT FOR
UPDATE (with_for_update) row-level lock in _load_stage() serialises the two
concurrent sessions: only one succeeds; the other blocks at the DB level until
the winner commits, then reads the committed status and raises ValueError.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from fastapi import HTTPException
from sqlalchemy import select, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from models import Base
from models.stage import Stage
from models.user import User
from models.workspace import Workspace
from routers.stage import _load_stage as router_load_stage
from services.pipeline.stage_manager import QualityGateBlockedError, StageManager

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason=(
        "TEST_DATABASE_URL not set — integration test skipped. "
        "Set to postgresql+asyncpg://postgres:postgres@localhost:5432/"
        "thought2build_test to run against a real PostgreSQL instance."
    ),
)


class _FakeUser:
    def __init__(self, user_id: uuid.UUID) -> None:
        self.id = user_id


def _manager() -> StageManager:
    # Inject a mock Redis so the tests stay focused on DB behavior.
    return StageManager(redis_client=AsyncMock())


def _assert_disposable_database(url: str) -> None:
    # Same guard as scripts/reset_test_database.py: this fixture drops the
    # whole public schema, so refuse anything but a test-named database.
    database = make_url(url).database or ""
    if database != "test" and not database.endswith("_test"):
        raise RuntimeError(
            f"Refusing to reset non-test database {database!r}; expected 'test' "
            "or a name ending in '_test'."
        )


@pytest.fixture(autouse=True)
def _isolate_document_quality(monkeypatch):
    # These tests exercise real database concurrency with deliberately minimal
    # document fixtures. Structural behavior is tested against real artifacts
    # in test_pipeline_reliability and test_stage_manager.
    monkeypatch.setattr(
        "services.pipeline.stage_manager.validate_readiness_async", AsyncMock()
    )


@pytest_asyncio.fixture
async def db_engine():
    """Per-test engine on the test's own event loop; resets the schema first.

    Reset via DROP SCHEMA ... CASCADE (mirroring scripts/reset_test_database.py)
    rather than Base.metadata.drop_all: an Alembic-migrated database contains
    audit tables with no ORM model (e.g. stripe_credit_packs) whose foreign
    keys make drop_all fail on the tables they reference.
    """
    _assert_disposable_database(TEST_DATABASE_URL)
    engine = create_async_engine(TEST_DATABASE_URL, echo=False, poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.execute(text("GRANT ALL ON SCHEMA public TO public"))
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def seed_data(db_engine):
    """Insert a User, Workspace, and draft tasks Stage; return their ids."""
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    user_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    stage_id = uuid.uuid4()

    async with factory() as session:
        user = User(
            id=user_id,
            email=f"test-{user_id}@example.com",
            google_id=str(user_id),
            name="Test User",
            credit_balance=100,
        )
        workspace = Workspace(
            id=workspace_id,
            user_id=user_id,
            name="Test Workspace",
            # problem_statement constraint: char_length BETWEEN 50 AND 10000
            problem_statement=(
                "Integration test workspace — placeholder problem statement."
            ),
            provider="anthropic",
            model="claude-3-haiku",
            status="active",
        )
        # Use type="tasks" (last in STAGE_ORDER) so _get_next_stage returns None
        # immediately, keeping the tests focused on the DB locking path.
        stage = Stage(
            id=stage_id,
            workspace_id=workspace_id,
            type="tasks",
            content="# Initial content",
            status="draft",
            current_version=1,
        )
        session.add_all([user, workspace, stage])
        await session.commit()

    return user_id, workspace_id, stage_id


async def test_concurrent_finalise_serialised_by_select_for_update(
    db_engine, seed_data
):
    """SELECT FOR UPDATE in finalise() serialises concurrent finalise calls.

    Two coroutines race to finalise the same stage via asyncio.gather().
    Because finalise() uses _load_stage(..., lock=True) — which emits
    SELECT ... FOR UPDATE (with_for_update) — PostgreSQL acquires a row-level
    lock: one session proceeds; the other blocks at the DB level until the
    first commits.  The blocked session then reads status='finalised' (READ
    COMMITTED) and raises ValueError.

    This test FAILS if lock=True is removed from finalise() because both
    sessions then read status='draft' simultaneously and both succeed,
    yielding len(results)==2.
    """
    user_id, workspace_id, stage_id = seed_data
    factory = async_sessionmaker(db_engine, expire_on_commit=False)

    manager = _manager()
    user = _FakeUser(user_id)
    results: list[str] = []
    errors: list[str] = []

    async def try_finalise() -> None:
        async with factory() as session:
            try:
                stage = await manager.finalise(stage_id, user, session)
                results.append(stage.status)
            except ValueError as e:
                errors.append(str(e))

    # asyncio.gather interleaves the two coroutines at every await point.
    # Both reach `await db.execute(SELECT ... FOR UPDATE)` before either
    # commits.  PostgreSQL serialises via the row lock: one proceeds, one
    # blocks and re-reads the committed status.
    await asyncio.gather(try_finalise(), try_finalise())

    assert len(results) == 1, (
        f"Expected exactly 1 successful finalise (SELECT FOR UPDATE serialises "
        f"concurrent calls), got {len(results)}: {results}. "
        f"If lock=True is absent from finalise(), both sessions read "
        f"status='draft' before either commits and both succeed — this "
        f"assertion would fail with 2 results."
    )
    assert (
        results[0] == "finalised"
    ), f"Successful finalise must set status='finalised', got {results[0]!r}"
    assert len(errors) == 1, (
        f"Expected exactly 1 ValueError from the blocked session, "
        f"got {len(errors)}: {errors}"
    )
    assert "cannot be finalised" in errors[0], (
        f"ValueError message must indicate the stage cannot be finalised, "
        f"got: {errors[0]!r}"
    )


async def test_ownership_denial(db_engine, seed_data):
    """The router's owner-scoped load 404s for a non-owner against real SQL.

    finalise_stage() guards every call with _load_stage(id, db, user.id), a
    JOIN on workspaces.user_id.  Prove the actual join denies another user and
    admits the owner.
    """
    user_id, workspace_id, stage_id = seed_data
    factory = async_sessionmaker(db_engine, expire_on_commit=False)

    async with factory() as session:
        # Owner resolves the stage.
        stage = await router_load_stage(stage_id, session, user_id)
        assert stage.id == stage_id

        # A different (even existing) user gets a 404, not someone else's row.
        intruder = User(
            id=uuid.uuid4(),
            email=f"intruder-{uuid.uuid4()}@example.com",
            google_id=str(uuid.uuid4()),
            name="Intruder",
            credit_balance=100,
        )
        session.add(intruder)
        await session.commit()

        with pytest.raises(HTTPException) as exc_info:
            await router_load_stage(stage_id, session, intruder.id)
        assert exc_info.value.status_code == 404


async def test_finalise_non_draft_status_conflict(db_engine, seed_data):
    """A stage whose committed status is not 'draft' cannot be finalised again."""
    user_id, workspace_id, stage_id = seed_data
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    manager = _manager()
    user = _FakeUser(user_id)

    async with factory() as session:
        stage = await manager.finalise(stage_id, user, session)
        assert stage.status == "finalised"

    for status in ("finalised", "stale"):
        async with factory() as session:
            await session.execute(
                update(Stage).where(Stage.id == stage_id).values(status=status)
            )
            await session.commit()
        async with factory() as session:
            with pytest.raises(ValueError, match="cannot be finalised"):
                await manager.finalise(stage_id, user, session)


async def test_finalise_blocked_gate_conflict_and_stale_gate_version(
    db_engine, seed_data
):
    """A blocked gate on the CURRENT version 409s; on a stale version it doesn't.

    finalise() refuses only when quality_gate_status == 'blocked' AND
    quality_gate_version == current_version.  A gate row left over from an
    older version (stale-version conflict resolved by a newer draft) must not
    block the newer version's finalise.
    """
    user_id, workspace_id, stage_id = seed_data
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    manager = _manager()
    user = _FakeUser(user_id)

    async with factory() as session:
        await session.execute(
            update(Stage)
            .where(Stage.id == stage_id)
            .values(
                quality_gate_status="blocked",
                quality_gate_kind="missing_sections",
                quality_gate_payload={},
                quality_gate_version=1,
                quality_gate_failed_at=datetime.now(UTC),
            )
        )
        await session.commit()

    # Blocked on the current version: structured conflict, row stays draft.
    async with factory() as session:
        with pytest.raises(QualityGateBlockedError):
            await manager.finalise(stage_id, user, session)
        await session.rollback()

    async with factory() as session:
        row = (
            await session.execute(select(Stage).where(Stage.id == stage_id))
        ).scalar_one()
        assert row.status == "draft"

    # Same gate row, but the stage has since moved to version 2: the stale
    # gate no longer applies and finalise succeeds.
    async with factory() as session:
        await session.execute(
            update(Stage).where(Stage.id == stage_id).values(current_version=2)
        )
        await session.commit()

    async with factory() as session:
        stage = await manager.finalise(stage_id, user, session)
        assert stage.status == "finalised"


async def test_row_lock_released_by_rollback_after_failed_finalise(
    db_engine, seed_data
):
    """A failed finalise holds its FOR UPDATE lock only until rollback.

    Session A's finalise raises (blocked quality gate) AFTER acquiring the row
    lock; the raise alone must not leak the lock past the session's rollback.
    Prove the lock is really held (NOWAIT fails), then really released
    (NOWAIT succeeds), then that the stage remains finalisable once the gate
    is overridden.
    """
    user_id, workspace_id, stage_id = seed_data
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    manager = _manager()
    user = _FakeUser(user_id)

    async with factory() as session:
        await session.execute(
            update(Stage)
            .where(Stage.id == stage_id)
            .values(
                quality_gate_status="blocked",
                quality_gate_kind="missing_sections",
                quality_gate_payload={},
                quality_gate_version=1,
                quality_gate_failed_at=datetime.now(UTC),
            )
        )
        await session.commit()

    session_a = factory()
    session_b = factory()
    try:
        with pytest.raises(QualityGateBlockedError):
            await manager.finalise(stage_id, user, session_a)

        # A's transaction is still open and still holds the row lock.
        with pytest.raises(DBAPIError, match="(?i)lock"):
            await session_b.execute(
                select(Stage).where(Stage.id == stage_id).with_for_update(nowait=True)
            )
        await session_b.rollback()

        # Rolling back A releases the lock immediately.
        await session_a.rollback()
        locked = await session_b.execute(
            select(Stage).where(Stage.id == stage_id).with_for_update(nowait=True)
        )
        assert locked.scalar_one().id == stage_id
        await session_b.rollback()
    finally:
        await session_a.close()
        await session_b.close()

    # The failure left no poisoned state: override the gate and finalise.
    async with factory() as session:
        await session.execute(
            update(Stage)
            .where(Stage.id == stage_id)
            .values(quality_gate_status="overridden")
        )
        await session.commit()
    async with factory() as session:
        stage = await manager.finalise(stage_id, user, session)
        assert stage.status == "finalised"


async def test_finalise_retry_after_deadlock(db_engine, seed_data):
    """After a PostgreSQL deadlock abort, a fresh-transaction retry succeeds.

    Two sessions lock two stage rows in opposite order to force a real
    DeadlockDetected abort.  The aborted transaction's rollback must leave the
    rows unlocked and unpoisoned so an application-level retry of finalise()
    on a new transaction completes normally.
    """
    user_id, workspace_id, stage_id = seed_data
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    manager = _manager()
    user = _FakeUser(user_id)

    other_stage_id = uuid.uuid4()
    async with factory() as session:
        session.add(
            Stage(
                id=other_stage_id,
                workspace_id=workspace_id,
                type="harness",
                content="# Harness content",
                status="finalised",
                current_version=1,
            )
        )
        await session.commit()

    session_a = factory()
    session_b = factory()
    a_ready = asyncio.Event()
    b_ready = asyncio.Event()
    deadlocks: list[DBAPIError] = []

    async def lock_pair(session, first, second, mine: asyncio.Event, other) -> None:
        await session.execute(select(Stage).where(Stage.id == first).with_for_update())
        mine.set()
        await other.wait()
        try:
            await session.execute(
                select(Stage).where(Stage.id == second).with_for_update()
            )
        except DBAPIError as exc:
            deadlocks.append(exc)

    try:
        await asyncio.gather(
            lock_pair(session_a, stage_id, other_stage_id, a_ready, b_ready),
            lock_pair(session_b, other_stage_id, stage_id, b_ready, a_ready),
        )
        assert len(deadlocks) == 1, (
            f"Expected PostgreSQL to abort exactly one of the two transactions, "
            f"got {len(deadlocks)} aborts: {deadlocks}"
        )
        assert "deadlock" in str(deadlocks[0]).lower()
        await session_a.rollback()
        await session_b.rollback()
    finally:
        await session_a.close()
        await session_b.close()

    # The retry (a fresh transaction) proceeds as if the deadlock never happened.
    async with factory() as session:
        stage = await manager.finalise(stage_id, user, session)
        assert stage.status == "finalised"


async def test_finalise_retry_after_serialization_failure(db_engine, seed_data):
    """After a SERIALIZABLE write-write abort, a fresh-transaction retry succeeds.

    Two SERIALIZABLE transactions update the same stage row; the second commit
    attempt fails with 'could not serialize access'.  A retry on a new
    (default READ COMMITTED) transaction — the application's actual finalise
    path — must succeed.
    """
    user_id, workspace_id, stage_id = seed_data
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    manager = _manager()
    user = _FakeUser(user_id)

    async with db_engine.connect() as conn_a, db_engine.connect() as conn_b:
        await conn_a.execution_options(isolation_level="SERIALIZABLE")
        await conn_b.execution_options(isolation_level="SERIALIZABLE")

        # Both transactions take their snapshot of the row.
        await conn_a.execute(select(Stage).where(Stage.id == stage_id))
        await conn_b.execute(select(Stage).where(Stage.id == stage_id))

        await conn_a.execute(
            update(Stage)
            .where(Stage.id == stage_id)
            .values(updated_at=datetime.now(UTC))
        )
        await conn_a.commit()

        with pytest.raises(DBAPIError, match="could not serialize"):
            await conn_b.execute(
                update(Stage)
                .where(Stage.id == stage_id)
                .values(updated_at=datetime.now(UTC))
            )
        await conn_b.rollback()

    async with factory() as session:
        stage = await manager.finalise(stage_id, user, session)
        assert stage.status == "finalised"


async def test_source_edit_waits_for_finalise_then_marks_result_stale(
    db_engine, seed_data
):
    """Different stage/source writes serialize through the workspace row."""
    from services.workspace_service import WorkspaceService

    user_id, workspace_id, stage_id = seed_data
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    manager = _manager()
    locked, release = asyncio.Event(), asyncio.Event()
    original = manager._load_workspace

    async def pause_after_lock(workspace_id, session):
        workspace = await original(workspace_id, session)
        locked.set()
        await release.wait()
        return workspace

    manager._load_workspace = pause_after_lock

    async def finalise():
        async with factory() as session:
            await manager.finalise(stage_id, _FakeUser(user_id), session)

    async def edit():
        async with factory() as session:
            await WorkspaceService().update(
                workspace_id,
                user_id,
                None,
                session,
                problem_statement=(
                    "I want to build a web app for teams to track inventory "
                    "and approve purchases."
                ),
            )

    finish = asyncio.create_task(finalise())
    await asyncio.wait_for(locked.wait(), timeout=5)
    change = asyncio.create_task(edit())
    try:
        await asyncio.sleep(0.05)
        assert not change.done(), "source edit escaped the workspace lock"
    finally:
        release.set()
    await asyncio.wait_for(asyncio.gather(finish, change), timeout=10)
    async with factory() as session:
        stage = await session.get(Stage, stage_id)
        assert stage.status == "stale"


async def test_source_manifest_rejects_edit_committed_before_finalise(
    db_engine, seed_data
):
    from services.pipeline.input_manifest import source_identity
    from services.pipeline.stage_manager import StageDependencyError
    from services.workspace_service import WorkspaceService

    user_id, workspace_id, stage_id = seed_data
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    manager = _manager()
    async with factory() as session:
        workspace = await manager._load_workspace(workspace_id, session)
        stage = await session.get(Stage, stage_id)
        stage.source_identity = source_identity(workspace, "tasks")
        await session.commit()
    async with factory() as session:
        await WorkspaceService().update(
            workspace_id,
            user_id,
            None,
            session,
            problem_statement=(
                "I want to build a web app for doctors to schedule "
                "appointments and notify patients."
            ),
        )
    async with factory() as session:
        with pytest.raises(StageDependencyError, match="Source inputs changed"):
            await manager.finalise(stage_id, _FakeUser(user_id), session)
    async with factory() as session:
        assert (await session.get(Stage, stage_id)).status == "stale"


async def test_generation_snapshot_roundtrips_in_postgres(db_engine, seed_data):
    from models import StageGenerationRun
    from services.pipeline.generation_runs import create_generation_run
    from services.pipeline.input_manifest import cache_identity, snapshot_workspace

    user_id, workspace_id, stage_id = seed_data
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    manager = _manager()
    async with factory() as session:
        workspace = await manager._load_workspace(workspace_id, session)
        stage = await session.get(Stage, stage_id)
        frozen = snapshot_workspace(workspace, "tasks")
        identity = cache_identity(workspace, "tasks")
        run = await create_generation_run(
            session,
            stage=stage,
            user_id=user_id,
            action="generate",
            deduction_ledger_id=None,
            total_parts=1,
            input_snapshot=frozen,
            input_identity=identity,
        )
        run.prepared_prompt = {"system": "frozen system", "user": "frozen user"}
        run_id = run.id
        await session.commit()
    async with factory() as session:
        run = await session.get(StageGenerationRun, run_id)
        assert run.input_snapshot == frozen
        assert run.input_identity == identity
        assert run.prepared_prompt["system"] == "frozen system"
