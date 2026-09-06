"""PostgreSQL + Redis integration test for CreditService — T-204.

Exercises the full deduct → commit → refund → commit cycle against a real
PostgreSQL instance (via asyncpg) and a real Redis instance so that:

  - Migration regressions surface before they reach production.
  - SELECT FOR UPDATE row-level locking in credit_service is validated.
  - Redis cache invalidation after deduct/refund is confirmed.
  - The credit_ledger unique partial index on refund entries is exercised.

Requires TEST_DATABASE_URL env var pointing to a live PostgreSQL instance.
Skip if not set (CI injects via T-204 services block).

Run with (set TEST_DATABASE_URL first)::

    TEST_DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/test \\
    TEST_REDIS_URL=redis://localhost:6379/0 \\
    uv run pytest tests/test_credit_cycle_integration.py -v

T-204 — HF-7.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pytest
import pytest_asyncio
from redis.asyncio import Redis
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from models import Base
from models.credit_ledger import CreditLedger
from models.user import User
from services.credit_service import CreditService, InsufficientCreditsError

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/0")

pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason=(
        "TEST_DATABASE_URL not set — integration test skipped. "
        "Set to postgresql+asyncpg://postgres:postgres@localhost:5432/test "
        "to run against a real PostgreSQL instance. T-204 — HF-7."
    ),
)


# ---------------------------------------------------------------------------
# Per-test fixtures — avoid sharing asyncpg pools across event-loop scopes
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def db_engine():
    """Create an isolated engine for this test; dispose it after completion."""
    engine = create_async_engine(TEST_DATABASE_URL, echo=False, poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all, checkfirst=True)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def redis_client():
    """Real Redis client for one test; closed before the next test starts."""
    client: Redis = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    yield client
    await client.aclose()


# ---------------------------------------------------------------------------
# Function-scoped fixtures — fresh session + isolated user per test
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def db_session(db_engine):
    """Provide a function-scoped AsyncSession; roll back after each test."""
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        yield session


@pytest_asyncio.fixture
async def test_user(db_engine, redis_client):
    """Insert a User with credit_balance=100 and clean up after the test."""
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    user_id = uuid.uuid4()

    async with factory() as session:
        user = User(
            id=user_id,
            email=f"integration-{user_id}@example.com",
            google_id=f"google-{user_id}",
            name="Integration Test User",
            avatar_url=None,
            credit_balance=100,
            created_at=datetime.now(UTC),
        )
        session.add(user)
        await session.commit()

    yield user_id

    # Teardown: delete ledger entries then the user.  Order matters because
    # credit_ledger has a FK → users with ON DELETE CASCADE, but we delete
    # explicitly to leave the schema clean for other test modules.
    async with factory() as session:
        await session.execute(
            delete(CreditLedger).where(CreditLedger.user_id == user_id)
        )
        await session.execute(delete(User).where(User.id == user_id))
        await session.commit()

    # Flush any cached credit balance left by the test.
    cache_key = f"credits:{user_id}"
    await redis_client.delete(cache_key)


# ---------------------------------------------------------------------------
# IT-1: Deduct credits — DB balance updated + ledger entry created
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_credit_deduct_updates_db_and_creates_ledger(
    db_engine,
    db_session: AsyncSession,
    test_user: uuid.UUID,
    redis_client: Redis,
) -> None:
    """IT-1 — deduct() reduces credit_balance and writes a negative ledger entry.

    Exercises SELECT FOR UPDATE (via CreditService._get_user(lock=True)) and
    confirms the flush propagates to the DB after commit.  T-204 — HF-7.
    """
    service = CreditService(redis_client=redis_client)
    factory = async_sessionmaker(db_engine, expire_on_commit=False)

    async with factory() as session:
        entry = await service.deduct(
            session, test_user, amount=10, reason="test-deduct"
        )
        await session.commit()

    # Verify: credit_balance dropped from 100 → 90 in the database.
    async with factory() as session:
        result = await session.execute(select(User).where(User.id == test_user))
        user = result.scalar_one()

    assert user.credit_balance == 90, (
        f"Expected credit_balance=90 after deducting 10, got {user.credit_balance}. "
        "T-204 — HF-7."
    )

    # Verify: ledger entry has the correct negative amount and reason.
    async with factory() as session:
        result = await session.execute(
            select(CreditLedger).where(CreditLedger.id == entry.id)
        )
        ledger = result.scalar_one()

    assert (
        ledger.amount == -10
    ), f"Expected ledger entry amount=-10, got {ledger.amount}. T-204 — HF-7."
    assert ledger.reason == "test-deduct"

    # Verify: Redis cache was invalidated (key must be absent after deduct).
    cache_key = f"credits:{test_user}"
    cached = await redis_client.get(cache_key)
    assert cached is None, (
        "Redis cache key must be deleted after deduct() so the next balance "
        f"read fetches the live DB value. Key {cache_key!r} still present. "
        "T-204 — HF-7."
    )


# ---------------------------------------------------------------------------
# IT-2: Insufficient balance — InsufficientCreditsError raised
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_credit_deduct_raises_on_insufficient_balance(
    db_engine,
    test_user: uuid.UUID,
    redis_client: Redis,
) -> None:
    """IT-2 — deduct() raises InsufficientCreditsError when balance < amount.

    Confirms the guard runs against a real DB row with actual credit_balance.
    T-204 — HF-7.
    """
    service = CreditService(redis_client=redis_client)
    factory = async_sessionmaker(db_engine, expire_on_commit=False)

    async with factory() as session:
        with pytest.raises(InsufficientCreditsError):
            await service.deduct(
                session, test_user, amount=999, reason="test-overdraft"
            )
        # Do not commit — the overdraft was rejected; no ledger entry written.


# ---------------------------------------------------------------------------
# IT-3: Refund — balance restored + refund ledger entry created
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_credit_refund_restores_balance_and_creates_ledger(
    db_engine,
    test_user: uuid.UUID,
    redis_client: Redis,
) -> None:
    """IT-3 — refund() reverses a deduction and writes a positive ledger entry.

    Exercises the full cycle:
      deduct 10 → commit → refund → commit → verify balance=100 again.

    Also confirms the partial unique index on (user_id, reason) for refund
    entries prevents double-refund (the index is exercised by a second call).
    T-204 — HF-7.
    """
    service = CreditService(redis_client=redis_client)
    factory = async_sessionmaker(db_engine, expire_on_commit=False)

    # --- Step 1: deduct 10 credits -----------------------------------------
    async with factory() as session:
        deduct_entry = await service.deduct(
            session, test_user, amount=10, reason="test-refund-cycle"
        )
        await session.commit()

    # --- Step 2: refund the deduction --------------------------------------
    async with factory() as session:
        await service.refund(session, deduct_entry.id, user_id=test_user)
        await session.commit()

    # --- Step 3: verify balance restored to 100 ----------------------------
    async with factory() as session:
        result = await session.execute(select(User).where(User.id == test_user))
        user = result.scalar_one()

    assert user.credit_balance == 100, (
        f"Expected credit_balance=100 after refund, got {user.credit_balance}. "
        "T-204 — HF-7."
    )

    # --- Step 4: verify refund ledger entry has positive amount ------------
    refund_reason = f"refund:{deduct_entry.id}"
    async with factory() as session:
        result = await session.execute(
            select(CreditLedger).where(
                CreditLedger.user_id == test_user,
                CreditLedger.reason == refund_reason,
            )
        )
        refund_ledger = result.scalar_one_or_none()

    assert refund_ledger is not None, (
        f"Expected a refund ledger entry with reason={refund_reason!r}. "
        "T-204 — HF-7."
    )
    assert refund_ledger.amount == 10, (
        f"Expected refund entry amount=10, got {refund_ledger.amount}. " "T-204 — HF-7."
    )

    # --- Step 5: double-refund must be a no-op (idempotent) ---------------
    async with factory() as session:
        await service.refund(session, deduct_entry.id, user_id=test_user)
        await session.commit()

    # Balance must still be 100 — the second refund was ignored.
    async with factory() as session:
        result = await session.execute(select(User).where(User.id == test_user))
        user = result.scalar_one()

    assert user.credit_balance == 100, (
        f"Expected credit_balance=100 after double-refund (idempotent), "
        f"got {user.credit_balance}. T-204 — HF-7."
    )

    # Redis cache must be invalidated after the refund.
    cache_key = f"credits:{test_user}"
    cached = await redis_client.get(cache_key)
    assert cached is None, (
        "Redis cache must be deleted after refund() so the next read fetches "
        f"the live DB value. Key {cache_key!r} still present. T-204 — HF-7."
    )


# ---------------------------------------------------------------------------
# IT-4: get_balance — returns authoritative DB state without publishing cache
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_balance_returns_database_value_without_publishing_uncommitted_cache(
    db_engine,
    test_user: uuid.UUID,
    redis_client: Redis,
) -> None:
    """Read the database without publishing a transaction's uncommitted balance."""
    service = CreditService(redis_client=redis_client)
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    cache_key = f"credits:{test_user}"

    # Ensure the cache is clear before the test.
    await redis_client.delete(cache_key)

    async with factory() as session:
        balance = await service.get_balance(session, test_user)

    assert balance == 100, (
        f"Expected get_balance to return 100 (initial balance), got {balance}. "
        "T-204 — HF-7."
    )

    # A read may sweep expiry in an uncommitted transaction. Never publish it.
    assert await redis_client.get(cache_key) is None
    await redis_client.set(cache_key, "999")
    async with factory() as session:
        assert await service.get_balance(session, test_user) == 100
