from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from models import CreditLedger, User
from services.credit_service import CreditService, InsufficientCreditsError


class _FakeRedis:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        return self._store.get(key)

    async def set(self, key: str, value: Any, ex: int = 0) -> None:
        self._store[key] = str(value)

    async def delete(self, *keys: str) -> None:
        for k in keys:
            self._store.pop(k, None)


class _SavepointCtx:
    """Async context manager simulating an SQLAlchemy begin_nested() savepoint.

    Mirrors real SQLAlchemy savepoint semantics:
    - On exception inside the ``async with`` block the savepoint is rolled back
      (``savepoint_rolled_back`` flag set on the owning ``_FakeDB``).
    - The outer transaction is NOT touched (``rolled_back`` stays False).
    - The exception is always re-raised so the caller's ``except`` handler fires.
    MF-3 — T-207.
    """

    def __init__(self, db: "_FakeDB") -> None:
        self._db = db

    async def __aenter__(self) -> "_SavepointCtx":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        if exc_type is not None:
            # Savepoint rolled back — record it but do NOT touch the outer tx.
            self._db.savepoint_rolled_back = True
        return False  # never suppress; the exception must propagate


class _FakeDB:
    def __init__(
        self,
        user: User | None = None,
        ledger: list[CreditLedger] | None = None,
        entity_lookup: CreditLedger | None = None,
        existing_refund: CreditLedger | None = None,
        raise_integrity_error_on_flush: bool = False,
    ) -> None:
        self.user = user
        self._ledger: list[CreditLedger] = ledger or []
        self._entity_lookup = entity_lookup
        self._existing_refund = existing_refund
        self._raise_integrity_error = raise_integrity_error_on_flush
        self._execute_count = 0
        self.added: list[Any] = []
        self.rolled_back = False
        # Set to True when begin_nested()'s __aexit__ sees an exception.
        self.savepoint_rolled_back = False

    async def execute(self, statement: Any) -> Any:
        # _expire_user_packs / _drain_packs query the credit-pack table (T-229;
        # migrated to billing_credit_packs in T-294). These tests don't set up any
        # packs — return an empty list so the sweep methods find nothing to expire/
        # drain and exit without side-effects. Pack queries are detected by
        # inspecting the statement's FROM clause; this avoids importing the model.
        try:
            froms = (
                statement.get_final_froms()
                if hasattr(statement, "get_final_froms")
                else statement.froms
            )
            if any(
                getattr(f, "name", None)
                in ("stripe_credit_packs", "billing_credit_packs")
                for f in froms
            ):
                return _EmptyPacksResult()
        except AttributeError:
            # Statement shape isn't FROM-introspectable; fall through to the
            # default dispatch below.
            pass
        # Counter-based dispatch for refund() tests that use entity_lookup.
        # refund() does not call _expire_user_packs, so these counters are
        # unaffected by the pack-query guard above (pack queries are returned
        # early and do not increment _execute_count).
        call = self._execute_count
        self._execute_count += 1
        # refund() first looks up the original deduction by ID
        if call == 0 and self._entity_lookup is not None:
            return _EntityResult(self._entity_lookup)
        # refund() next checks whether a refund row already exists
        if call == 1 and self._entity_lookup is not None:
            return _EntityResult(self._existing_refund)
        return _EntityResult(self.user)

    def add(self, instance: Any) -> None:
        if isinstance(instance, CreditLedger):
            if not hasattr(instance, "id") or instance.id is None:
                instance.id = uuid4()
            self._ledger.append(instance)
        self.added.append(instance)

    async def flush(self) -> None:
        if self._raise_integrity_error:
            raise IntegrityError(None, None, Exception("duplicate"))

    async def rollback(self) -> None:
        self.rolled_back = True

    async def commit(self) -> None:
        pass

    def begin_nested(self) -> _SavepointCtx:
        """Return an async savepoint context manager (simulates begin_nested).

        The returned context manager rolls back only the savepoint on exception,
        leaving the outer transaction intact — mirrors real SQLAlchemy behaviour.
        MF-3 — T-207.
        """
        return _SavepointCtx(self)


class _Scalars:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def all(self) -> list[Any]:
        return list(self._rows)


class _LedgerQueryResult:
    """Supports both scalar_one() (for get_balance) and scalars().all() (for deduct)."""

    def __init__(self, rows: list[CreditLedger]) -> None:
        self._rows = rows

    def scalar_one(self) -> int:
        return sum(r.amount for r in self._rows)

    def scalar_one_or_none(self) -> int:
        return self.scalar_one()

    def scalars(self) -> _Scalars:
        return _Scalars(self._rows)


class _EntityResult:
    def __init__(self, entity: Any) -> None:
        self._entity = entity

    def scalar_one_or_none(self) -> Any:
        return self._entity

    def scalar_one(self) -> Any:
        return self._entity


class _EmptyPacksResult:
    """Empty scalars result for StripeCreditPack queries (T-229).

    _expire_user_packs / _drain_packs call db.execute(select(StripeCreditPack)
    ...).  Pre-T-229 tests don't set up any packs; returning an empty list
    lets those code paths run without side-effects.
    """

    class _Scalars:
        def all(self) -> list:
            return []

    def scalars(self) -> "_EmptyPacksResult._Scalars":
        return self._Scalars()

    def scalar_one_or_none(self) -> None:
        return None


@pytest.fixture
def svc() -> CreditService:
    return CreditService(redis_client=_FakeRedis())


def _user(user_id: Any, balance: int = 0) -> User:
    return User(
        id=user_id,
        email=f"{user_id}@example.com",
        google_id=f"google-{user_id}",
        credit_balance=balance,
    )


@pytest.mark.asyncio
async def test_get_balance_returns_user_balance(svc: CreditService) -> None:
    user_id = uuid4()
    db = _FakeDB(user=_user(user_id, balance=40))
    assert await svc.get_balance(db, user_id) == 40


@pytest.mark.asyncio
async def test_get_balance_ignores_stale_redis_value(svc: CreditService) -> None:
    user_id = uuid4()
    redis = svc._redis
    assert isinstance(redis, _FakeRedis)
    redis._store[f"credits:{user_id}"] = "99"
    db = _FakeDB()
    assert await svc.get_balance(db, user_id) == 0
    assert not db.added


@pytest.mark.asyncio
async def test_deduct_raises_on_insufficient_balance(svc: CreditService) -> None:
    user_id = uuid4()
    db = _FakeDB(user=_user(user_id, balance=0))
    with pytest.raises(InsufficientCreditsError):
        await svc.deduct(db, user_id, 10, "gen")


@pytest.mark.asyncio
async def test_deduct_succeeds_when_balance_sufficient(svc: CreditService) -> None:
    user_id = uuid4()
    user = _user(user_id, balance=50)
    db = _FakeDB(user=user)
    entry = await svc.deduct(db, user_id, 10, "gen")
    assert entry.amount == -10
    assert user.credit_balance == 40
    assert any(e.amount == -10 for e in db._ledger)


@pytest.mark.asyncio
async def test_refund_inserts_positive_entry(svc: CreditService) -> None:
    user_id = uuid4()
    user = _user(user_id, balance=40)
    deduction = CreditLedger(id=uuid4(), user_id=user_id, amount=-10, reason="gen")
    db = _FakeDB(user=user, entity_lookup=deduction)

    await svc.refund(db, deduction.id)

    refund_entries = [e for e in db._ledger if e.amount > 0 and "refund" in e.reason]
    assert len(refund_entries) == 1
    assert refund_entries[0].amount == 10
    assert user.credit_balance == 50


@pytest.mark.asyncio
async def test_refund_is_idempotent(svc: CreditService) -> None:
    user_id = uuid4()
    user = _user(user_id, balance=40)
    deduction = CreditLedger(id=uuid4(), user_id=user_id, amount=-10, reason="gen")
    existing = CreditLedger(
        id=uuid4(),
        user_id=user_id,
        amount=10,
        reason=f"refund:{deduction.id}",
    )
    db = _FakeDB(user=user, entity_lookup=deduction, existing_refund=existing)

    await svc.refund(db, deduction.id)

    assert not db.rolled_back
    assert user.credit_balance == 40


@pytest.mark.asyncio
async def test_refund_is_race_safe_via_integrity_error(svc: CreditService) -> None:
    """Duplicate refund (IntegrityError) is swallowed silently.

    Previously this called db.rollback() which corrupted the outer transaction.
    With begin_nested() only the savepoint is rolled back.  MF-3 — T-207.
    """
    user_id = uuid4()
    user = _user(user_id, balance=40)
    deduction = CreditLedger(id=uuid4(), user_id=user_id, amount=-10, reason="gen")
    # Simulate DB enforcing the unique constraint by raising IntegrityError on flush
    db = _FakeDB(
        user=user,
        entity_lookup=deduction,
        raise_integrity_error_on_flush=True,
    )

    # Must not raise — duplicate refund is silently swallowed
    await svc.refund(db, deduction.id)

    # Savepoint was rolled back, but the outer transaction must NOT have been.
    assert db.savepoint_rolled_back, "savepoint should have been rolled back"
    assert not db.rolled_back, "outer transaction must NOT be rolled back"
    assert user.credit_balance == 40


@pytest.mark.asyncio
async def test_refund_integrity_error_outer_transaction_survives(
    svc: CreditService,
) -> None:
    """After a duplicate-refund IntegrityError the outer session is still usable.

    MF-3 — T-207: The SAVEPOINT is rolled back; the outer transaction (which
    may have stage-status updates, workspace writes, etc.) is unaffected.
    Callers can still commit the outer transaction after refund() returns.
    """
    user_id = uuid4()
    user = _user(user_id, balance=40)
    deduction = CreditLedger(id=uuid4(), user_id=user_id, amount=-10, reason="gen")
    db = _FakeDB(
        user=user,
        entity_lookup=deduction,
        raise_integrity_error_on_flush=True,
    )

    # refund() must return without raising even though flush() raises internally
    await svc.refund(db, deduction.id)

    # Outer transaction was NOT rolled back
    # Outer transaction must survive the savepoint rollback.
    assert not db.rolled_back, "db.rollback() must not be called — outer tx survives"

    # Session is still usable: commit() must not raise
    await db.commit()  # would raise if session were in an invalid state

    # Execute is still usable too (simulates stage-status query continuing)
    _ = await db.execute(object())  # must not raise


@pytest.mark.asyncio
async def test_credit_invalidates_cache(svc: CreditService) -> None:
    user_id = uuid4()
    redis = svc._redis
    assert isinstance(redis, _FakeRedis)
    redis._store[f"credits:{user_id}"] = "50"
    db = _FakeDB(user=_user(user_id, balance=50))
    await svc.credit(db, user_id, 20, "bonus")
    assert f"credits:{user_id}" not in redis._store
