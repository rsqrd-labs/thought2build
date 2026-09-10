"""Regression tests for the GitHub integration review (docs/reviews/
GITHUB_INTEGRATION_REVIEW_2026-09-09.md).

Every test here reproduces a defect that shipped, so each one is written to fail
against the pre-fix code rather than merely exercise the new code path.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from models import (
    GitHubInstallation,
    GitHubWebhookEvent,
    IntegrationPush,
    IntegrationPushTask,
    Stage,
    User,
    UserIntegration,
    Workspace,
)
from services.integrations import github_install_service as install_svc
from services.integrations import github_reconcile
from services.integrations.push_repo import find_live_pushes_for_event
from services.integrations.task_parser import compute_task_ref, parse_tasks
from services.pipeline import github_export_service
from services.pipeline.github_export_service import (
    prepare_export_push,
    push_to_github,
    run_export_push,
)

# Fixtures come from the existing suites so these tests exercise the same
# wiring the shipped code runs under, not a parallel scaffold.
from tests.test_export_push_worker import (  # noqa: F401
    _StubClient as _AppStubClient,
)
from tests.test_export_push_worker import (  # noqa: F401
    installation,
    session,
    user,
    workspace,
)
from tests.test_github_drift_backfill import _cleanup, _make_push, _make_task
from tests.test_github_export_service import _StubClient as _LegacyStubClient

# asyncio_mode = "auto" (pyproject) collects the async tests here; a module-level
# asyncio mark would additionally, and wrongly, tag the sync ones.

_DUPLICATE_TITLE_TASKS = """\
### T-001: Add unit tests

**Description:** Cover the parser.

### T-002: Add unit tests

**Description:** Cover the exporter.

### T-003: Ship it

**Description:** Ship.
"""


async def _set_tasks(session: AsyncSession, workspace: Workspace, content: str) -> None:
    await session.execute(
        update(Stage)
        .where(Stage.workspace_id == workspace.id, Stage.type == "tasks")
        .values(content=content)
    )
    await session.commit()


async def _task_rows(session: AsyncSession, push_id: Any) -> list[IntegrationPushTask]:
    return list(
        (
            await session.execute(
                select(IntegrationPushTask).where(
                    IntegrationPushTask.push_id == push_id
                )
            )
        )
        .scalars()
        .all()
    )


# ---------------------------------------------------------------------------
# F1 — same-titled tasks must not collapse onto one task_ref
# ---------------------------------------------------------------------------


def test_same_titled_tasks_get_distinct_refs() -> None:
    """Two identical titles — and two that differ only in case/whitespace, which
    the ref normalisation folds together — must still be distinct identities."""
    tasks = parse_tasks(
        "### T-001: Add unit tests\na\n"
        "### T-002: Add unit tests\nb\n"
        "### T-003: set up   ci\nc\n"
        "### T-004: Set Up CI\nd\n"
    )
    refs = [t.task_ref for t in tasks]
    assert len(set(refs)) == 4, refs


def test_first_occurrence_ref_is_unchanged_by_the_fix() -> None:
    """Renumber-invariance and every already-persisted row depend on the first
    occurrence keeping the historic unsalted key."""
    only = parse_tasks("### T-001: Ship it\nx\n")[0]
    assert only.task_ref == compute_task_ref("Ship it")

    first, second = parse_tasks("### T-001: Ship it\na\n### T-002: Ship it\nb\n")
    assert first.task_ref == compute_task_ref("Ship it")
    assert second.task_ref != first.task_ref


def test_agent_issue_body_carries_the_disambiguated_ref() -> None:
    """The YAML header is what an agent reads back; it must not advertise a ref
    that belongs to a sibling task."""
    for task in parse_tasks("### T-001: Add tests\na\n### T-002: Add tests\nb\n"):
        assert f"task_ref: {task.task_ref}" in task.agent_body_md


async def test_app_export_with_duplicate_titles_maps_every_task(
    session: AsyncSession,
    user: User,
    workspace: Workspace,
    installation: GitHubInstallation,
) -> None:
    """Before the fix this raised IntegrityError on uq_push_task_ref mid-export
    (after opening a duplicate GitHub issue); the arq retry then pointed BOTH
    tasks at the first issue, overwrote one body, and reported 'completed' with
    an orphan issue nothing could ever close."""
    await _set_tasks(session, workspace, _DUPLICATE_TITLE_TASKS)
    push = await prepare_export_push(
        session,
        workspace_id=workspace.id,
        user_id=user.id,
        installation=installation,
        export_mode="files_to_default",
    )

    stub = _AppStubClient()
    result = await run_export_push(
        push.id, "dup-export", "private", db=session, client=stub
    )

    assert result is not None
    assert result.status == "completed"
    assert len(stub.issues_created) == 3
    rows = await _task_rows(session, push.id)
    assert len(rows) == 3
    assert len({r.task_ref for r in rows}) == 3
    assert len({r.external_issue_number for r in rows}) == 3


async def test_legacy_export_with_duplicate_titles_maps_every_task(
    session: AsyncSession,
    user: User,
    workspace: Workspace,
) -> None:
    """The Phase-13 OAuth path has the same loop and needs the same guarantee."""
    integ = UserIntegration(
        user_id=user.id,
        provider="github",
        encrypted_token=_encrypted_token(),
        github_username="octocat",
    )
    session.add(integ)
    await session.commit()
    await _set_tasks(session, workspace, _DUPLICATE_TITLE_TASKS)

    stub = _LegacyStubClient()
    result = await push_to_github(
        workspace_id=workspace.id,
        user_id=user.id,
        repo_name="dup-legacy",
        visibility="private",
        db=session,
        client_factory=stub,
    )
    assert result.status == "completed"
    assert len(stub.issues_created) == 3
    assert len(await _task_rows(session, result.id)) == 3


def _encrypted_token() -> str:
    from config import settings
    from services.security import key_vault

    if not settings.encryption_master_key:
        settings.encryption_master_key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    return key_vault.encrypt("ghp_fake_token_for_test")


async def test_duplicate_titled_task_is_not_retired_on_the_next_push(
    session: AsyncSession,
    user: User,
    workspace: Workspace,
    installation: GitHubInstallation,
) -> None:
    """The retire set and the create loop must derive refs identically, or the
    disambiguated sibling looks obsolete and its issue is closed every run."""
    await _set_tasks(session, workspace, _DUPLICATE_TITLE_TASKS)
    push = await prepare_export_push(
        session,
        workspace_id=workspace.id,
        user_id=user.id,
        installation=installation,
        export_mode="files_to_default",
    )
    stub = _AppStubClient()
    await run_export_push(push.id, "dup-export", "private", db=session, client=stub)

    # Re-export the identical spec: nothing is obsolete, so nothing closes.
    push.status = "pending"
    await session.commit()
    await run_export_push(push.id, "dup-export", "private", db=session, client=stub)

    # The retire path deletes the mapping row for anything it closes, so an
    # intact set of three rows is the assertion that nothing was retired.
    assert len(await _task_rows(session, push.id)) == 3
    assert len(stub.issues_created) == 3  # and no new issues on the re-export


async def test_mark_push_failed_survives_a_poisoned_session(
    session: AsyncSession,
    user: User,
    workspace: Workspace,
    installation: GitHubInstallation,
) -> None:
    """A flush failure poisons the transaction; the status write must still land.

    Previously it did not: commit() raised PendingRollbackError, the handler
    rolled back and discarded the pending status change, and the push stayed
    'pending' forever."""
    push = await prepare_export_push(
        session,
        workspace_id=workspace.id,
        user_id=user.id,
        installation=installation,
        export_mode="files_to_default",
    )
    # Poison the session with a guaranteed constraint violation.
    session.add(
        IntegrationPushTask(push_id=push.id, task_ref="dup", external_issue_number=1)
    )
    session.add(
        IntegrationPushTask(push_id=push.id, task_ref="dup", external_issue_number=2)
    )
    with pytest.raises(Exception):
        await session.commit()

    # Prove we are actually exercising the poisoned branch and not passing
    # because the session happened to be healthy.
    assert session.sync_session.is_active is False

    await github_export_service._mark_push_failed(session, push, status="failed")
    await session.refresh(push)
    assert push.status == "failed"
    # The rollback expired every identity-mapped object; reload the fixture rows
    # inside async context so their teardown does not lazy-load off the event
    # loop (a MissingGreenlet, not a product failure).
    for fixture_row in (user, workspace, installation):
        await session.refresh(fixture_row)


# ---------------------------------------------------------------------------
# F2 — backfill must be able to recover an older missed event
# ---------------------------------------------------------------------------


class _SinceHonouringClient:
    def __init__(self, issues: list[dict[str, Any]]) -> None:
        self._issues = issues
        self.since_seen: str | None = None

    async def list_issues(
        self, repo: str, *, state: str = "all", since: str | None = None
    ) -> list[dict[str, Any]]:
        self.since_seen = since
        if since is None:
            return list(self._issues)
        cutoff = datetime.fromisoformat(since.replace("Z", "+00:00"))
        return [
            i
            for i in self._issues
            if datetime.fromisoformat(i["updated_at"].replace("Z", "+00:00")) >= cutoff
        ]


def _z(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


async def test_backfill_recovers_an_older_missed_closure(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    """Issue #5 closed at 10:00 with a lost webhook; issue #6 closed at 12:00 and
    synced. The old cursor was max(synced_at) = 12:00, which excluded #5 from
    every future sweep."""
    from tests.test_github_drift_backfill import _make_install

    early = datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    late = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    inst = await _make_install(session, user)
    push = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="completed"
    )
    missed = await _make_task(session, push, issue_number=5, state="open")
    seen = await _make_task(session, push, issue_number=6, state="done")
    seen.synced_at = late
    await session.commit()

    client = _SinceHonouringClient(
        [
            {"number": 5, "state": "closed", "updated_at": _z(early)},
            {"number": 6, "state": "closed", "updated_at": _z(late)},
        ]
    )
    try:
        await github_reconcile.backfill_repo(
            {}, str(push.id), db=session, client=client
        )
        await session.refresh(missed)
        assert missed.state == "done", (
            f"backfill sent since={client.since_seen!r}; #5's 10:00 closure was "
            "filtered out and can never be recovered"
        )
    finally:
        await _cleanup(session, inst)


async def test_backfill_watermark_advances_and_narrows_the_next_sweep(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    """A completed sweep sets the watermark so the next one is cheap — the reason
    the cursor is not simply always None."""
    from tests.test_github_drift_backfill import _make_install

    inst = await _make_install(session, user)
    push = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="completed"
    )
    await _make_task(session, push, issue_number=5, state="open")
    client = _SinceHonouringClient(
        [{"number": 5, "state": "open", "updated_at": _z(datetime.now(UTC))}]
    )
    try:
        await github_reconcile.backfill_repo(
            {}, str(push.id), db=session, client=client
        )
        await session.refresh(push)
        assert client.since_seen is None  # first sweep pulls full history
        assert push.last_full_backfill_at is not None

        await github_reconcile.backfill_repo(
            {}, str(push.id), db=session, client=client
        )
        assert client.since_seen is not None  # second sweep is bounded
    finally:
        await _cleanup(session, inst)


async def test_backfill_does_not_advance_watermark_when_it_cannot_run(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    """A sweep that raises must leave the replay window open for the next one.

    Note this exercises the generic raise path: the injected client bypasses the
    production 404 branch that suspends the installation. That branch also
    returns before the watermark assignment, so it is correct too — just covered
    elsewhere.
    """
    from services.integrations.github_app_auth import GitHubAppAuthError
    from tests.test_github_drift_backfill import _make_install

    class _GoneClient:
        async def list_issues(self, *args: Any, **kwargs: Any) -> list[Any]:
            raise GitHubAppAuthError(404, "gone")

    inst = await _make_install(session, user)
    push = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="completed"
    )
    await _make_task(session, push, issue_number=5, state="open")
    try:
        with pytest.raises(GitHubAppAuthError):
            await github_reconcile.backfill_repo(
                {}, str(push.id), db=session, client=_GoneClient()
            )
        await session.refresh(push)
        assert push.last_full_backfill_at is None
    finally:
        await _cleanup(session, inst)


# ---------------------------------------------------------------------------
# F3 — an uninstall must not permanently sever inbound sync
# ---------------------------------------------------------------------------


def _account() -> install_svc.InstallationAccount:
    return install_svc.InstallationAccount(
        account_login="octo",
        account_type="Organization",
        repository_selection="all",
    )


async def test_reinstall_readopts_pushes_the_uninstall_detached(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    """find_live_pushes_for_event INNER JOINs the installation, so a NULLed
    installation_id made the push unreachable by webhook, backfill and resync —
    permanently, even after reinstalling on the same repo."""
    from tests.test_github_drift_backfill import _make_install

    inst = await _make_install(session, user)
    numeric_id = inst.installation_id
    push = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="completed"
    )
    repo_id = push.repo_id

    await install_svc.apply_installation_event(
        session, action="deleted", installation_id=numeric_id
    )
    reinstalled = await install_svc.upsert_installation(
        session, installation_id=numeric_id, user_id=user.id, account=_account()
    )
    try:
        found = await find_live_pushes_for_event(session, repo_id, numeric_id)
        assert [p.id for p in found] == [push.id]
    finally:
        await _cleanup(session, reinstalled)


async def test_reinstall_by_a_different_user_does_not_adopt_someone_elses_pushes(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    """Re-adoption is user-scoped: an org can be re-installed by another admin,
    and adopting the first user's pushes would sync their workspaces under a
    different account's installation token."""
    from tests.test_github_drift_backfill import _make_install

    inst = await _make_install(session, user)
    numeric_id = inst.installation_id
    push = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="completed"
    )
    repo_id = push.repo_id

    other = User(
        email=f"other-{uuid4()}@example.com",
        google_id=f"google-{uuid4()}",
        name="Other",
        avatar_url=None,
    )
    session.add(other)
    await session.commit()
    await session.refresh(other)

    await install_svc.apply_installation_event(
        session, action="deleted", installation_id=numeric_id
    )
    reinstalled = await install_svc.upsert_installation(
        session, installation_id=numeric_id, user_id=other.id, account=_account()
    )
    try:
        assert await find_live_pushes_for_event(session, repo_id, numeric_id) == []
        refreshed = await session.get(IntegrationPush, push.id)
        assert refreshed is not None
        assert refreshed.installation_id is None
    finally:
        await _cleanup(session, reinstalled)
        await session.execute(
            update(IntegrationPush)
            .where(IntegrationPush.id == push.id)
            .values(installation_id=None)
        )
        await session.commit()


# ---------------------------------------------------------------------------
# F4 — a merged PR's attribution must survive the issue event landing first
# ---------------------------------------------------------------------------


async def test_merged_pr_upgrades_attribution_when_issue_event_lands_first(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    """issues -> fast lane, pull_request -> bulk lane, and GitHub stamps the
    issue's updated_at at or after the PR's merged_at, so the authoritative
    pr_merge event was always rejected as stale."""
    from tests.test_github_drift_backfill import _make_install

    merged_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    issue_updated_at = merged_at + timedelta(seconds=1)

    inst = await _make_install(session, user)
    push = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="completed"
    )
    task = await _make_task(session, push, issue_number=5, state="open")
    ident = {
        "repository": {"id": push.repo_id},
        "installation": {"id": inst.installation_id},
    }
    try:
        await github_reconcile.reconcile_event(
            {},
            "d-issues",
            "issues",
            json.dumps(
                {
                    **ident,
                    "action": "closed",
                    "issue": {"number": 5, "updated_at": _z(issue_updated_at)},
                }
            ),
            db=session,
        )
        await session.refresh(task)
        assert task.done_via == "manual"

        await github_reconcile.reconcile_event(
            {},
            "d-pr",
            "pull_request",
            json.dumps(
                {
                    **ident,
                    "action": "closed",
                    "pull_request": {
                        "number": 9,
                        "merged": True,
                        "merged_at": _z(merged_at),
                        "body": "Closes #5",
                    },
                }
            ),
            db=session,
        )
        await session.refresh(task)
        assert task.done_via == "pr_merge"
        # The stale event must not rewind the ordering cursor.
        assert task.synced_at == issue_updated_at
    finally:
        await _cleanup(session, inst)


async def test_stale_pr_merge_does_not_attribute_an_open_task(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    """The upgrade only ever refines an existing completion — it must never
    record an attribution for a task that is still open."""
    from tests.test_github_drift_backfill import _make_install

    inst = await _make_install(session, user)
    push = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="completed"
    )
    task = await _make_task(session, push, issue_number=5, state="open")
    task.synced_at = datetime(2026, 1, 2, tzinfo=UTC)
    await session.commit()
    try:
        await github_reconcile.reconcile_event(
            {},
            "d-pr-stale",
            "pull_request",
            json.dumps(
                {
                    "repository": {"id": push.repo_id},
                    "installation": {"id": inst.installation_id},
                    "action": "closed",
                    "pull_request": {
                        "number": 9,
                        "merged": True,
                        "merged_at": _z(datetime(2026, 1, 1, tzinfo=UTC)),
                        "body": "Closes #5",
                    },
                }
            ),
            db=session,
        )
        await session.refresh(task)
        assert task.state == "open"
        assert task.done_via is None
    finally:
        await _cleanup(session, inst)


# ---------------------------------------------------------------------------
# F5 — a recorded-but-unprocessed delivery must be replayable
# ---------------------------------------------------------------------------


async def test_unprocessed_delivery_is_replayed(session: AsyncSession) -> None:
    """The dedup row commits on receipt, so GitHub answers its own redelivery
    with 'duplicate'. Without a replay the event is lost with no trace."""
    delivery_id = f"d-{uuid4()}"
    payload = {"action": "closed", "issue": {"number": 5}}
    session.add(
        GitHubWebhookEvent(
            delivery_id=delivery_id,
            event_type="issues",
            received_at=datetime.now(UTC) - timedelta(days=1),
            payload=payload,
        )
    )
    await session.commit()

    enqueued: list[tuple[Any, ...]] = []

    async def _fake_enqueue(job: str, *args: Any) -> None:
        enqueued.append((job, *args))

    try:
        replayed = await github_reconcile._replay_unprocessed_deliveries(
            session, _fake_enqueue
        )
        assert replayed == 1
        job, replayed_id, event_type, raw = enqueued[0]
        # 'issues' must keep its fast lane on replay.
        assert job == "reconcile_issue_event"
        assert replayed_id == delivery_id
        assert event_type == "issues"
        assert json.loads(raw) == payload
    finally:
        await session.execute(
            GitHubWebhookEvent.__table__.delete().where(
                GitHubWebhookEvent.delivery_id == delivery_id
            )
        )
        await session.commit()


async def test_replay_gives_up_after_the_attempt_bound(
    session: AsyncSession,
) -> None:
    """A delivery that can never succeed must stop being replayed, or it is
    re-enqueued every tick forever and — the sweep being batched and oldest-first
    — eventually starves the newer deliveries the sweep exists to rescue."""
    delivery_id = f"d-{uuid4()}"
    session.add(
        GitHubWebhookEvent(
            delivery_id=delivery_id,
            event_type="issues",
            received_at=datetime.now(UTC) - timedelta(days=1),
            payload={"action": "closed"},
        )
    )
    await session.commit()

    enqueued: list[Any] = []

    async def _fake_enqueue(job: str, *args: Any) -> None:
        enqueued.append((job, *args))

    try:
        for _ in range(5):
            await github_reconcile._replay_unprocessed_deliveries(
                session, _fake_enqueue
            )
        mine = [c for c in enqueued if c[1] == delivery_id]
        assert len(mine) == github_reconcile._MAX_REPLAY_ATTEMPTS
        # Still unprocessed, so the operator query in RUNBOOK 12.13 surfaces it.
        row = (
            await session.execute(
                select(GitHubWebhookEvent).where(
                    GitHubWebhookEvent.delivery_id == delivery_id
                )
            )
        ).scalar_one()
        assert row.processed_at is None
    finally:
        await session.execute(
            GitHubWebhookEvent.__table__.delete().where(
                GitHubWebhookEvent.delivery_id == delivery_id
            )
        )
        await session.commit()


async def test_processing_a_delivery_clears_its_stored_payload(
    session: AsyncSession,
) -> None:
    """The payload exists only to make a STUCK delivery replayable; retaining it
    for every applied delivery would grow the table for no benefit."""
    delivery_id = f"d-{uuid4()}"
    session.add(
        GitHubWebhookEvent(
            delivery_id=delivery_id,
            event_type="push",
            payload={"action": "irrelevant"},
        )
    )
    await session.commit()
    try:
        # 'push' routes nowhere; _dispatch still marks it processed.
        await github_reconcile.reconcile_event(
            {}, delivery_id, "push", json.dumps({"action": "irrelevant"}), db=session
        )
        row = (
            await session.execute(
                select(GitHubWebhookEvent).where(
                    GitHubWebhookEvent.delivery_id == delivery_id
                )
            )
        ).scalar_one()
        assert row.processed_at is not None
        assert row.payload is None
    finally:
        await session.execute(
            GitHubWebhookEvent.__table__.delete().where(
                GitHubWebhookEvent.delivery_id == delivery_id
            )
        )
        await session.commit()


async def test_replay_skips_processed_recent_and_payloadless_rows(
    session: AsyncSession,
) -> None:
    """Replay must not re-run applied work, race a job still in flight, or trip
    over a row written before the payload column existed."""
    old = datetime.now(UTC) - timedelta(days=1)
    ids = {
        "processed": f"d-{uuid4()}",
        "recent": f"d-{uuid4()}",
        "no_payload": f"d-{uuid4()}",
    }
    session.add_all(
        [
            GitHubWebhookEvent(
                delivery_id=ids["processed"],
                event_type="issues",
                received_at=old,
                processed_at=datetime.now(UTC),
                payload={"action": "closed"},
            ),
            GitHubWebhookEvent(
                delivery_id=ids["recent"],
                event_type="issues",
                received_at=datetime.now(UTC),
                payload={"action": "closed"},
            ),
            GitHubWebhookEvent(
                delivery_id=ids["no_payload"],
                event_type="issues",
                received_at=old,
                payload=None,
            ),
        ]
    )
    await session.commit()

    enqueued: list[Any] = []

    async def _fake_enqueue(job: str, *args: Any) -> None:
        enqueued.append((job, *args))

    try:
        await github_reconcile._replay_unprocessed_deliveries(session, _fake_enqueue)
        replayed_ids = {call[1] for call in enqueued}
        assert replayed_ids.isdisjoint(set(ids.values()))
    finally:
        await session.execute(
            GitHubWebhookEvent.__table__.delete().where(
                GitHubWebhookEvent.delivery_id.in_(list(ids.values()))
            )
        )
        await session.commit()


# ---------------------------------------------------------------------------
# F7 — two racing first exports must not wedge the workspace
# ---------------------------------------------------------------------------


async def test_second_null_repo_push_is_rejected_by_the_database(
    session: AsyncSession, user: User, workspace: Workspace
) -> None:
    """Migration 0016 dropped (workspace_id, provider) uniqueness and its
    replacement keys on repo_id, which is NULL on a first export — and Postgres
    treats NULLs as distinct. 0048 restores the guarantee."""
    from sqlalchemy.exc import IntegrityError

    first = IntegrationPush(
        workspace_id=workspace.id,
        user_id=user.id,
        provider="github",
        status="pending",
    )
    session.add(first)
    await session.commit()
    first_id = first.id
    try:
        session.add(
            IntegrationPush(
                workspace_id=workspace.id,
                user_id=user.id,
                provider="github",
                status="pending",
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()
    finally:
        await session.execute(
            IntegrationPush.__table__.delete().where(IntegrationPush.id == first_id)
        )
        await session.commit()
        # See the note in test_mark_push_failed_survives_a_poisoned_session.
        await session.refresh(user)
        await session.refresh(workspace)


async def test_export_tolerates_pre_existing_duplicate_push_rows(
    session: AsyncSession,
    user: User,
    workspace: Workspace,
    installation: GitHubInstallation,
) -> None:
    """Rows that predate 0048 must still resolve — previously this raised
    MultipleResultsFound and 500ed the export endpoint forever."""
    older = IntegrationPush(
        workspace_id=workspace.id,
        user_id=user.id,
        provider="github",
        status="failed",  # outside the new partial index, so it can coexist
        created_at=datetime.now(UTC) - timedelta(days=1),
    )
    newer = IntegrationPush(
        workspace_id=workspace.id,
        user_id=user.id,
        provider="github",
        status="failed",
        created_at=datetime.now(UTC),
    )
    session.add_all([older, newer])
    await session.commit()
    try:
        push = await prepare_export_push(
            session,
            workspace_id=workspace.id,
            user_id=user.id,
            installation=installation,
            export_mode="files_to_default",
        )
        assert push.id == newer.id  # newest wins, deterministically
        assert push.status == "pending"
    finally:
        await session.execute(
            IntegrationPush.__table__.delete().where(
                IntegrationPush.id.in_([older.id, newer.id])
            )
        )
        await session.commit()


# ---------------------------------------------------------------------------
# Adversarial follow-ups: truncation, the insert race, and stale adoption state
# ---------------------------------------------------------------------------


class _CapturingClient:
    """Returns a fixed page and records the cursor it was asked for."""

    def __init__(self, issues: list[dict[str, Any]]) -> None:
        self._issues = issues
        self.since_seen: list[str | None] = []

    async def list_issues(
        self, repo: str, *, state: str = "all", since: str | None = None
    ) -> list[dict[str, Any]]:
        self.since_seen.append(since)
        return list(self._issues)


async def test_truncated_sweep_resumes_from_the_last_row_seen(
    session: AsyncSession, user: User, workspace: Workspace, monkeypatch: Any
) -> None:
    """list_issues truncates silently at its page cap. Advancing the watermark to
    'now' after a truncated fetch would skip everything past the cap forever —
    the same silent loss the watermark was introduced to fix."""
    from tests.test_github_drift_backfill import _make_install

    monkeypatch.setattr(github_reconcile, "ISSUES_FETCH_CAP", 3)
    newest = datetime(2026, 1, 1, 9, 0, tzinfo=UTC)
    page = [
        {
            "number": 5,
            "state": "open",
            "updated_at": _z(datetime(2026, 1, 1, 7, 0, tzinfo=UTC)),
        },
        {
            "number": 6,
            "state": "open",
            "updated_at": _z(datetime(2026, 1, 1, 8, 0, tzinfo=UTC)),
        },
        # A pull request: filtered out of reconciliation, but it is still the
        # newest row GitHub returned and so must set the resume point.
        {
            "number": 7,
            "state": "open",
            "pull_request": {"url": "x"},
            "updated_at": _z(newest),
        },
    ]

    inst = await _make_install(session, user)
    push = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="completed"
    )
    await _make_task(session, push, issue_number=5, state="open")
    try:
        await github_reconcile.backfill_repo(
            {}, str(push.id), db=session, client=_CapturingClient(page)
        )
        await session.refresh(push)
        assert push.last_full_backfill_at == newest
    finally:
        await _cleanup(session, inst)


async def test_truncated_sweep_with_a_stalled_cursor_takes_the_full_window(
    session: AsyncSession, user: User, workspace: Workspace, monkeypatch: Any
) -> None:
    """If a full page all shares the current cursor timestamp, resuming there
    re-pulls the identical page every tick forever. That is worse than a skipped
    window, so the sweep advances and says so."""
    from tests.test_github_drift_backfill import _make_install

    monkeypatch.setattr(github_reconcile, "ISSUES_FETCH_CAP", 2)
    stuck = datetime(2026, 1, 1, 9, 0, tzinfo=UTC)
    page = [
        {"number": 5, "state": "open", "updated_at": _z(stuck)},
        {"number": 6, "state": "open", "updated_at": _z(stuck)},
    ]

    inst = await _make_install(session, user)
    push = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="completed"
    )
    await _make_task(session, push, issue_number=5, state="open")
    push.last_full_backfill_at = stuck
    await session.commit()
    try:
        await github_reconcile.backfill_repo(
            {}, str(push.id), db=session, client=_CapturingClient(page)
        )
        await session.refresh(push)
        assert push.last_full_backfill_at is not None
        assert push.last_full_backfill_at > stuck
    finally:
        await _cleanup(session, inst)


async def test_complete_sweep_still_advances_to_the_full_window(
    session: AsyncSession, user: User, workspace: Workspace, monkeypatch: Any
) -> None:
    """The truncation guard must not penalise the ordinary, complete sweep."""
    from tests.test_github_drift_backfill import _make_install

    monkeypatch.setattr(github_reconcile, "ISSUES_FETCH_CAP", 10)
    old_stamp = datetime(2026, 1, 1, 9, 0, tzinfo=UTC)
    page = [{"number": 5, "state": "open", "updated_at": _z(old_stamp)}]

    inst = await _make_install(session, user)
    push = await _make_push(
        session, workspace=workspace, user=user, installation=inst, status="completed"
    )
    await _make_task(session, push, issue_number=5, state="open")
    try:
        await github_reconcile.backfill_repo(
            {}, str(push.id), db=session, client=_CapturingClient(page)
        )
        await session.refresh(push)
        assert push.last_full_backfill_at is not None
        assert push.last_full_backfill_at > old_stamp
    finally:
        await _cleanup(session, inst)


class _FirstSelectMissesSession:
    """Proxy that hides the push row from the FIRST select only.

    That is exactly the interleaving the retry exists for: our SELECT ran before
    the competing INSERT committed, so we try to insert and collide. Simulating
    it deterministically beats racing two real sessions and hoping they overlap.
    """

    def __init__(self, inner: AsyncSession) -> None:
        self._inner = inner
        self._selects = 0

    async def execute(self, statement: Any, *args: Any, **kwargs: Any) -> Any:
        result = await self._inner.execute(statement, *args, **kwargs)
        if "SELECT" in str(statement).upper() and "integration_pushes" in str(
            statement
        ):
            self._selects += 1
            if self._selects == 1:
                return _EmptyResult()
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _EmptyResult:
    def scalars(self) -> "_EmptyResult":
        return self

    def first(self) -> None:
        return None


async def test_losing_a_first_export_race_returns_the_winners_row(
    session: AsyncSession,
    user: User,
    workspace: Workspace,
) -> None:
    """With 0048's index the losing INSERT raises IntegrityError. That is a
    transient collision, not a caller error, so it must resolve to the winner's
    row rather than surfacing as a 500."""
    winner = IntegrationPush(
        workspace_id=workspace.id,
        user_id=user.id,
        provider="github",
        status="pending",
    )
    session.add(winner)
    await session.commit()
    winner_id = winner.id
    proxy = _FirstSelectMissesSession(session)
    try:
        claimed = await github_export_service._claim_push_row(
            proxy, workspace.id, user.id
        )
        assert claimed.id == winner_id
        assert proxy._selects == 2  # the re-read after the conflict happened
    finally:
        await session.rollback()
        await session.execute(
            IntegrationPush.__table__.delete().where(IntegrationPush.id == winner_id)
        )
        await session.commit()
        await session.refresh(user)
        await session.refresh(workspace)


async def test_binding_an_installation_clears_the_adoption_marker(
    session: AsyncSession,
    user: User,
    workspace: Workspace,
    installation: GitHubInstallation,
) -> None:
    """A push re-bound to a new installation must not keep pointing at the old
    one, or it stays a candidate for an adoption that no longer applies."""
    push = await prepare_export_push(
        session,
        workspace_id=workspace.id,
        user_id=user.id,
        installation=installation,
        export_mode="files_to_default",
    )
    await session.execute(
        update(IntegrationPush)
        .where(IntegrationPush.id == push.id)
        .values(detached_installation_id=999_999)
    )
    await session.commit()

    rebound = await prepare_export_push(
        session,
        workspace_id=workspace.id,
        user_id=user.id,
        installation=installation,
        export_mode="files_to_default",
    )
    assert rebound.detached_installation_id is None


async def test_a_failing_readopt_does_not_take_the_install_bind_down(
    session: AsyncSession, user: User, monkeypatch: Any
) -> None:
    """Adoption takes row locks a worker export may hold. The bind is what the
    user is waiting on and what everything else depends on; adoption is a
    convenience the next export re-establishes."""

    async def _boom(*args: Any, **kwargs: Any) -> int:
        raise RuntimeError("lock timeout")

    monkeypatch.setattr(install_svc, "_readopt_detached_pushes", _boom)
    numeric_id = uuid4().int % 1_000_000_000
    row = await install_svc.upsert_installation(
        session, installation_id=numeric_id, user_id=user.id, account=_account()
    )
    try:
        assert row.installation_id == numeric_id
        assert row.user_id == user.id
    finally:
        await session.execute(
            GitHubInstallation.__table__.delete().where(GitHubInstallation.id == row.id)
        )
        await session.commit()
