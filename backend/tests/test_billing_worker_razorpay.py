"""Behavioural tests for Razorpay grant / refund processing (issue #44 — Step 5).

The Razorpay counterparts of ``test_billing_order_created.py`` /
``test_billing_order_refunded.py``: the grant is the security-sensitive money path
(a forged ``user_id`` must grant nothing, economics come from the checkout-attempt
SNAPSHOT, the payment entity is the money authority — D10, and a redelivery must
never double-credit), and the refund is the intricate proportional-reversal path.
None of it is exercisable by the source-grep harness, so these are real-Postgres
integration tests gated on ``TEST_DATABASE_URL`` (they skip locally and run in CI
against the migrated database — the ``billing_purchase:`` / ``refund:billing:``
ledger-reason unique indexes live in migration 0018).

The normalized inbox payloads here are built field-for-field from the router's
``_normalize_razorpay_link_paid`` / ``_normalize_razorpay_refund`` output (the
``notes`` block — not Lemon's ``custom`` — plus a top-level ``payment_id`` and
nested ``payment`` / ``payment_link`` blocks), since the worker reads exactly that.
"""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from config import settings
from models import CreditLedger, User
from models.billing_checkout_attempt import BillingCheckoutAttempt
from models.billing_credit_debt import BillingCreditDebt
from models.billing_credit_pack import BillingCreditPack
from models.billing_webhook_event import BillingWebhookEvent
from services import billing_worker
from services.observability import BILLING_UNRECOVERABLE_CHECKOUT

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not TEST_DATABASE_URL,
        reason=(
            "TEST_DATABASE_URL not set — Postgres Razorpay worker integration test "
            "skipped. Runs in CI against the migrated test database. Issue #44."
        ),
    ),
]

# Distinctive INR economics (paise) — deliberately unlike the Lemon suites' USD
# cents so a cross-wired provider snapshot would show up immediately.
_CREDITS = 200
_PRICE_PAISE = 79900  # ₹799.00
_CURRENCY = "INR"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_maker(monkeypatch) -> async_sessionmaker:
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(billing_worker, "AsyncSessionLocal", maker)
    yield maker
    await engine.dispose()


@pytest.fixture
async def session(db_maker: async_sessionmaker) -> AsyncSession:
    async with db_maker() as db:
        yield db


@pytest.fixture(autouse=True)
def razorpay_config(monkeypatch):
    """Pin a test key so ``razorpay_environment()`` is deterministically 'test'."""
    monkeypatch.setattr(settings, "razorpay_key_id", "rzp_test_abc123")
    yield


class _Tracker:
    def __init__(self) -> None:
        self.users: list[UUID] = []
        self.webhooks: list[UUID] = []


@pytest.fixture
async def cleanup(session: AsyncSession):
    tracker = _Tracker()
    yield tracker
    if tracker.webhooks:
        await session.execute(
            delete(BillingWebhookEvent).where(
                BillingWebhookEvent.id.in_(tracker.webhooks)
            )
        )
    for uid in tracker.users:
        # debts reference packs via RESTRICT FK — delete debts before packs.
        await session.execute(
            delete(BillingCreditDebt).where(BillingCreditDebt.user_id == uid)
        )
        await session.execute(
            delete(BillingCreditPack).where(BillingCreditPack.user_id == uid)
        )
        await session.execute(delete(CreditLedger).where(CreditLedger.user_id == uid))
        await session.execute(
            delete(BillingCheckoutAttempt).where(BillingCheckoutAttempt.user_id == uid)
        )
        await session.execute(delete(User).where(User.id == uid))
    await session.commit()


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


async def _make_user(session: AsyncSession, cleanup, *, balance: int = 0) -> User:
    u = User(
        email=f"rzp-worker-{uuid4()}@example.com",
        google_id=f"google-{uuid4()}",
        name="Razorpay Worker Tester",
        avatar_url=None,
        credit_balance=balance,
    )
    session.add(u)
    await session.commit()
    await session.refresh(u)
    cleanup.users.append(u.id)
    return u


async def _make_attempt(
    session: AsyncSession,
    user: User,
    nonce_hash: str,
    *,
    credits: int = _CREDITS,
    price_cents: int = _PRICE_PAISE,
    currency: str = _CURRENCY,
    validity_days: int = 30,
    status: str = "provider_created",
    provider_checkout_id: str | None = "plink_from_attempt",
) -> BillingCheckoutAttempt:
    attempt = BillingCheckoutAttempt(
        checkout_ref=f"ref_{uuid4().hex}",
        user_id=user.id,
        provider="razorpay",
        provider_checkout_id=provider_checkout_id,
        checkout_nonce_hash=nonce_hash,
        credits=credits,
        price_cents=price_cents,
        currency=currency,
        validity_days=validity_days,
        status=status,
        expires_at=datetime.now(UTC) + timedelta(minutes=30),
    )
    session.add(attempt)
    await session.commit()
    await session.refresh(attempt)
    return attempt


async def _make_pack(
    session: AsyncSession,
    user: User,
    *,
    payment_id: str,
    credits_purchased: int = _CREDITS,
    credits_remaining: int | None = None,
    credits_consumed: int = 0,
    paid_item_cents: int = _PRICE_PAISE,
    order_total_cents: int | None = _PRICE_PAISE,
    currency: str = _CURRENCY,
    status: str = "active",
) -> BillingCreditPack:
    pack = BillingCreditPack(
        user_id=user.id,
        provider="razorpay",
        provider_checkout_id=f"plink_{uuid4().hex[:10]}",
        provider_order_id=payment_id,
        credits_purchased=credits_purchased,
        credits_remaining=(
            credits_purchased if credits_remaining is None else credits_remaining
        ),
        credits_consumed=credits_consumed,
        price_cents=paid_item_cents,
        currency=currency,
        paid_item_amount_cents=paid_item_cents,
        provider_order_total_cents=order_total_cents,
        status=status,
        purchased_at=datetime.now(UTC) - timedelta(days=1),
        expires_at=datetime.now(UTC) + timedelta(days=29),
    )
    session.add(pack)
    await session.commit()
    await session.refresh(pack)
    return pack


def _notes(
    attempt: BillingCheckoutAttempt,
    nonce_hash: str,
    **overrides: Any,
) -> dict[str, Any]:
    """The seven allow-listed Thought2Build notes (router ``_razorpay_notes_block``)."""
    block = {
        "user_id": str(attempt.user_id),
        "checkout_ref": attempt.checkout_ref,
        "checkout_nonce_hash_from_webhook": nonce_hash,
        "environment": "test",
        "credits": str(attempt.credits),
        "price_cents": str(attempt.price_cents),
        "currency": attempt.currency,
    }
    block.update(overrides)
    return block


def _link_paid_payload(
    attempt: BillingCheckoutAttempt,
    nonce_hash: str,
    *,
    payment_id: str,
    notes: dict[str, Any] | None = None,
    payment: dict[str, Any] | None = None,
    payment_link: dict[str, Any] | None = None,
    event_id: str | None = "evt_rzp_1",
) -> dict[str, Any]:
    """A valid sanitised ``payment_link.paid`` payload (mirrors the router)."""
    base_payment = {
        "payment_id": payment_id,
        "amount": attempt.price_cents,
        "currency": attempt.currency,
        "status": "captured",
        "method": "upi",
        "amount_refunded": 0,
        "refund_status": None,
    }
    if payment:
        base_payment.update(payment)
    base_link = {
        "payment_link_id": attempt.provider_checkout_id or "plink_live_1",
        "reference_id": str(attempt.id),
        "amount": attempt.price_cents,
        "currency": attempt.currency,
        "status": "paid",
    }
    if payment_link:
        base_link.update(payment_link)
    return {
        "provider": "razorpay",
        "event_name": "payment_link.paid",
        "payment_id": payment_id,
        "payment_link": base_link,
        "payment": base_payment,
        "refund": None,
        "notes": _notes(attempt, nonce_hash) if notes is None else notes,
        "event_id": event_id,
    }


def _refund_payload(
    *,
    payment_id: str,
    payment_amount: int = _PRICE_PAISE,
    amount_refunded: int,
    refund_status: str | None,
    notes: dict[str, Any] | None = None,
    refund_amount: int | None = None,
    event_id: str | None = "evt_rzp_refund_1",
) -> dict[str, Any]:
    """A valid sanitised ``refund.processed`` payload (mirrors the router)."""
    return {
        "provider": "razorpay",
        "event_name": "refund.processed",
        "payment_id": payment_id,
        "payment_link": None,
        "payment": {
            "payment_id": payment_id,
            "amount": payment_amount,
            "currency": _CURRENCY,
            "status": "captured",
            "method": "upi",
            "amount_refunded": amount_refunded,
            "refund_status": refund_status,
        },
        "refund": {
            "refund_id": f"rfnd_{uuid4().hex[:10]}",
            "payment_id": payment_id,
            "amount": amount_refunded if refund_amount is None else refund_amount,
        },
        "notes": notes if notes is not None else {},
        "event_id": event_id,
    }


async def _make_webhook(
    session: AsyncSession,
    cleanup: _Tracker,
    payload: dict[str, Any],
    *,
    received_at: datetime | None = None,
) -> BillingWebhookEvent:
    row = BillingWebhookEvent(
        provider="razorpay",
        event_name=payload["event_name"],
        provider_object_type="payments",
        provider_object_id=payload["payment_id"],
        payload_hash=hashlib.sha256(repr(payload).encode()).hexdigest(),
        status="received",
        normalized_payload=payload,
    )
    session.add(row)
    await session.flush()
    if received_at is not None:
        row.received_at = received_at
    await session.commit()
    await session.refresh(row)
    cleanup.webhooks.append(row.id)
    return row


# Fresh-session column reads (no stale identity-map objects).


async def _balance(maker: async_sessionmaker, user_id: UUID) -> int:
    async with maker() as db:
        return await db.scalar(select(User.credit_balance).where(User.id == user_id))


async def _pack_count(maker: async_sessionmaker, user_id: UUID) -> int:
    async with maker() as db:
        rows = await db.execute(
            select(BillingCreditPack.id).where(BillingCreditPack.user_id == user_id)
        )
        return len(rows.scalars().all())


async def _pack_by_order(
    maker: async_sessionmaker, payment_id: str
) -> BillingCreditPack | None:
    async with maker() as db:
        return await db.scalar(
            select(BillingCreditPack).where(
                BillingCreditPack.provider_order_id == payment_id
            )
        )


async def _pack(maker: async_sessionmaker, pack_id: UUID) -> BillingCreditPack:
    async with maker() as db:
        return await db.scalar(
            select(BillingCreditPack).where(BillingCreditPack.id == pack_id)
        )


async def _wh_status(maker: async_sessionmaker, wid: UUID) -> tuple:
    async with maker() as db:
        row = await db.execute(
            select(BillingWebhookEvent.status, BillingWebhookEvent.last_error).where(
                BillingWebhookEvent.id == wid
            )
        )
        return row.one()


async def _attempt_row(maker: async_sessionmaker, attempt_id: UUID) -> tuple:
    async with maker() as db:
        row = await db.execute(
            select(
                BillingCheckoutAttempt.status,
                BillingCheckoutAttempt.completed_at,
                BillingCheckoutAttempt.provider_order_id,
            ).where(BillingCheckoutAttempt.id == attempt_id)
        )
        return row.one()


async def _purchase_ledger_count(maker: async_sessionmaker, payment_id: str) -> int:
    async with maker() as db:
        rows = await db.execute(
            select(CreditLedger.id).where(
                CreditLedger.reason == f"billing_purchase:razorpay:{payment_id}"
            )
        )
        return len(rows.scalars().all())


async def _refund_ledger(maker: async_sessionmaker, pack_id: UUID) -> list:
    async with maker() as db:
        rows = await db.execute(
            select(CreditLedger.amount, CreditLedger.reason)
            .where(CreditLedger.reason.like(f"refund:billing:{pack_id}:%"))
            .order_by(CreditLedger.created_at.asc())
        )
        return list(rows.all())


async def _debt(maker: async_sessionmaker, pack_id: UUID) -> BillingCreditDebt | None:
    async with maker() as db:
        return await db.scalar(
            select(BillingCreditDebt).where(BillingCreditDebt.source_pack_id == pack_id)
        )


# ---------------------------------------------------------------------------
# Grant — happy path + snapshot anchoring
# ---------------------------------------------------------------------------


async def test_grants_on_valid_payment_link_paid(session, db_maker, cleanup) -> None:
    nonce_hash = hashlib.sha256(b"rzp-nonce-1").hexdigest()
    user = await _make_user(session, cleanup, balance=10)
    attempt = await _make_attempt(session, user, nonce_hash)
    user_id, attempt_id = user.id, attempt.id
    credits = attempt.credits
    payment_id = f"pay_{uuid4().hex[:14]}"
    webhook = await _make_webhook(
        session, cleanup, _link_paid_payload(attempt, nonce_hash, payment_id=payment_id)
    )
    wid = webhook.id

    await billing_worker.handle_razorpay_link_paid({}, str(wid))

    assert await _balance(db_maker, user_id) == 10 + credits

    pack = await _pack_by_order(db_maker, payment_id)
    assert pack is not None
    assert pack.provider == "razorpay"
    assert pack.credits_purchased == credits
    assert pack.credits_remaining == credits
    assert pack.paid_item_amount_cents == _PRICE_PAISE
    assert pack.provider_order_total_cents == _PRICE_PAISE  # populated, not NULL (D10)
    assert pack.currency == _CURRENCY
    assert pack.provider_checkout_id == "plink_from_attempt"  # from the attempt

    status, completed_at, order_id = await _attempt_row(db_maker, attempt_id)
    assert status == "completed"
    assert completed_at is not None
    assert order_id == payment_id  # stamped so /billing/status can return 200

    assert (await _wh_status(db_maker, wid))[0] == "processed"
    assert await _purchase_ledger_count(db_maker, payment_id) == 1


async def test_config_price_change_midflight_grants_attempt_snapshot(
    session, db_maker, cleanup, monkeypatch
) -> None:
    nonce_hash = hashlib.sha256(b"rzp-snap").hexdigest()
    user = await _make_user(session, cleanup)
    user_id = user.id
    attempt = await _make_attempt(
        session, user, nonce_hash, credits=200, price_cents=79900
    )
    # Config changes AFTER checkout creation — must not affect this purchase.
    monkeypatch.setattr(settings, "razorpay_price_cents", 150000)
    monkeypatch.setattr(settings, "razorpay_credits_per_purchase", 500)
    payment_id = f"pay_{uuid4().hex[:14]}"
    webhook = await _make_webhook(
        session, cleanup, _link_paid_payload(attempt, nonce_hash, payment_id=payment_id)
    )

    await billing_worker.handle_razorpay_link_paid({}, str(webhook.id))

    assert await _balance(db_maker, user_id) == 200  # snapshot credits, not 500
    pack = await _pack_by_order(db_maker, payment_id)
    assert pack.credits_purchased == 200
    assert pack.paid_item_amount_cents == 79900


# ---------------------------------------------------------------------------
# Grant — rejections (grant nothing, durably acknowledge)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"payment": {"amount": 100}}, id="payment-amount-mismatch"),
        pytest.param({"payment": {"currency": "USD"}}, id="payment-currency-mismatch"),
        pytest.param({"payment": {"status": "authorized"}}, id="payment-not-captured"),
        pytest.param({"payment_link": {"amount": 100}}, id="link-amount-mismatch"),
    ],
)
async def test_rejects_invalid_link_paid(session, db_maker, cleanup, kwargs) -> None:
    nonce_hash = hashlib.sha256(b"rzp-rej").hexdigest()
    user = await _make_user(session, cleanup, balance=5)
    user_id = user.id
    attempt = await _make_attempt(session, user, nonce_hash)
    payment_id = f"pay_{uuid4().hex[:14]}"
    webhook = await _make_webhook(
        session,
        cleanup,
        _link_paid_payload(attempt, nonce_hash, payment_id=payment_id, **kwargs),
    )
    wid = webhook.id

    await billing_worker.handle_razorpay_link_paid({}, str(wid))

    assert await _balance(db_maker, user_id) == 5  # unchanged
    assert await _pack_count(db_maker, user_id) == 0
    assert (await _wh_status(db_maker, wid))[0] == "processed"  # terminal ack


async def test_environment_mismatch_grants_nothing(session, db_maker, cleanup) -> None:
    nonce_hash = hashlib.sha256(b"rzp-env").hexdigest()
    user = await _make_user(session, cleanup, balance=0)
    user_id = user.id
    attempt = await _make_attempt(session, user, nonce_hash)
    payment_id = f"pay_{uuid4().hex[:14]}"
    # A live-env note against a test-key server (razorpay_config pins rzp_test_).
    webhook = await _make_webhook(
        session,
        cleanup,
        _link_paid_payload(
            attempt,
            nonce_hash,
            payment_id=payment_id,
            notes=_notes(attempt, nonce_hash, environment="live"),
        ),
    )
    await billing_worker.handle_razorpay_link_paid({}, str(webhook.id))

    assert await _balance(db_maker, user_id) == 0
    assert await _pack_count(db_maker, user_id) == 0


async def test_forged_user_id_grants_nothing(session, db_maker, cleanup) -> None:
    nonce_hash = hashlib.sha256(b"rzp-forge").hexdigest()
    victim = await _make_user(session, cleanup, balance=0)
    attacker = await _make_user(session, cleanup, balance=0)
    victim_id, attacker_id = victim.id, attacker.id
    attempt = await _make_attempt(session, victim, nonce_hash)
    payment_id = f"pay_{uuid4().hex[:14]}"
    payload = _link_paid_payload(
        attempt,
        nonce_hash,
        payment_id=payment_id,
        notes=_notes(attempt, nonce_hash, user_id=str(attacker_id)),
    )
    webhook = await _make_webhook(session, cleanup, payload)

    await billing_worker.handle_razorpay_link_paid({}, str(webhook.id))

    assert await _balance(db_maker, attacker_id) == 0
    assert await _balance(db_maker, victim_id) == 0
    assert await _pack_count(db_maker, attacker_id) == 0
    assert await _pack_count(db_maker, victim_id) == 0


async def test_nonce_mismatch_grants_nothing(session, db_maker, cleanup) -> None:
    nonce_hash = hashlib.sha256(b"rzp-real").hexdigest()
    user = await _make_user(session, cleanup)
    user_id = user.id
    attempt = await _make_attempt(session, user, nonce_hash)
    payment_id = f"pay_{uuid4().hex[:14]}"
    payload = _link_paid_payload(
        attempt,
        nonce_hash,
        payment_id=payment_id,
        notes=_notes(
            attempt,
            nonce_hash,
            checkout_nonce_hash_from_webhook=hashlib.sha256(b"forged").hexdigest(),
        ),
    )
    webhook = await _make_webhook(session, cleanup, payload)

    await billing_worker.handle_razorpay_link_paid({}, str(webhook.id))

    assert await _balance(db_maker, user_id) == 0
    assert await _pack_count(db_maker, user_id) == 0


async def test_unknown_checkout_ref_grants_nothing(session, db_maker, cleanup) -> None:
    nonce_hash = hashlib.sha256(b"rzp-x").hexdigest()
    user = await _make_user(session, cleanup)
    user_id = user.id
    attempt = await _make_attempt(session, user, nonce_hash)
    payment_id = f"pay_{uuid4().hex[:14]}"
    payload = _link_paid_payload(
        attempt,
        nonce_hash,
        payment_id=payment_id,
        notes=_notes(attempt, nonce_hash, checkout_ref="ref_does_not_exist"),
    )
    webhook = await _make_webhook(session, cleanup, payload)

    await billing_worker.handle_razorpay_link_paid({}, str(webhook.id))

    assert await _balance(db_maker, user_id) == 0
    assert await _pack_count(db_maker, user_id) == 0


async def test_unrecoverable_counter_only_when_link_paid(
    session, db_maker, cleanup
) -> None:
    """A rejection on a *paid* link trips the unprovable-paid alert; a not-paid one
    (money never captured) does not."""
    nonce_hash = hashlib.sha256(b"rzp-unrec").hexdigest()
    user = await _make_user(session, cleanup)
    attempt = await _make_attempt(session, user, nonce_hash)

    def _counter() -> float:
        return BILLING_UNRECOVERABLE_CHECKOUT.labels(provider="razorpay")._value.get()

    # (1) Rejection with a PAID link (amount tampered) → counter increments.
    paid = await _make_webhook(
        session,
        cleanup,
        _link_paid_payload(
            attempt,
            nonce_hash,
            payment_id=f"pay_{uuid4().hex[:14]}",
            payment={"amount": 100},
        ),
    )
    before = _counter()
    await billing_worker.handle_razorpay_link_paid({}, str(paid.id))
    assert _counter() == before + 1

    # (2) Rejection with a NOT-paid link → benign, counter unchanged.
    not_paid = await _make_webhook(
        session,
        cleanup,
        _link_paid_payload(
            attempt,
            nonce_hash,
            payment_id=f"pay_{uuid4().hex[:14]}",
            payment={"amount": 100},
            payment_link={"status": "created"},
        ),
    )
    before = _counter()
    await billing_worker.handle_razorpay_link_paid({}, str(not_paid.id))
    assert _counter() == before  # unchanged


# ---------------------------------------------------------------------------
# Grant — idempotency
# ---------------------------------------------------------------------------


async def test_duplicate_link_paid_does_not_double_credit(
    session, db_maker, cleanup
) -> None:
    nonce_hash = hashlib.sha256(b"rzp-dup").hexdigest()
    user = await _make_user(session, cleanup)
    user_id = user.id
    attempt = await _make_attempt(session, user, nonce_hash)
    credits = attempt.credits
    payment_id = f"pay_{uuid4().hex[:14]}"

    wh1 = await _make_webhook(
        session, cleanup, _link_paid_payload(attempt, nonce_hash, payment_id=payment_id)
    )
    # A redelivery that slipped past inbox dedup (different event id → different
    # payload_hash, same payment id).
    wh2 = await _make_webhook(
        session,
        cleanup,
        _link_paid_payload(
            attempt, nonce_hash, payment_id=payment_id, event_id="evt_rzp_2"
        ),
    )

    await billing_worker.handle_razorpay_link_paid({}, str(wh1.id))
    assert await _balance(db_maker, user_id) == credits

    await billing_worker.handle_razorpay_link_paid({}, str(wh2.id))

    assert await _balance(db_maker, user_id) == credits  # not doubled
    assert await _pack_count(db_maker, user_id) == 1
    assert (await _wh_status(db_maker, wh2.id))[0] == "processed"
    assert await _purchase_ledger_count(db_maker, payment_id) == 1


async def test_reprocessing_same_row_is_idempotent(session, db_maker, cleanup) -> None:
    nonce_hash = hashlib.sha256(b"rzp-reproc").hexdigest()
    user = await _make_user(session, cleanup)
    user_id = user.id
    attempt = await _make_attempt(session, user, nonce_hash)
    credits = attempt.credits
    payment_id = f"pay_{uuid4().hex[:14]}"
    webhook = await _make_webhook(
        session, cleanup, _link_paid_payload(attempt, nonce_hash, payment_id=payment_id)
    )
    wid = webhook.id

    await billing_worker.handle_razorpay_link_paid({}, str(wid))
    # The sweep/reconcile can re-invoke the handler for the same row.
    await billing_worker.handle_razorpay_link_paid({}, str(wid))

    assert await _balance(db_maker, user_id) == credits
    assert await _pack_count(db_maker, user_id) == 1
    assert await _purchase_ledger_count(db_maker, payment_id) == 1


async def test_concurrent_pack_flush_conflict_grants_nothing(
    session, db_maker, cleanup, monkeypatch
) -> None:
    """A racing delivery commits the pack between our pre-check and our flush.

    Covers handle_razorpay_link_paid's pack-flush IntegrityError branch: the
    handler must roll back, ack the redelivery, and grant nothing.
    """
    nonce_hash = hashlib.sha256(b"rzp-race-pack").hexdigest()
    user = await _make_user(session, cleanup)
    user_id = user.id
    attempt = await _make_attempt(session, user, nonce_hash)
    credits = attempt.credits
    payment_id = f"pay_{uuid4().hex[:14]}"

    now = datetime.now(UTC)
    winner = BillingCreditPack(
        user_id=user_id,
        provider="razorpay",
        provider_checkout_id=None,
        provider_order_id=payment_id,
        credits_purchased=credits,
        credits_remaining=credits,
        price_cents=_PRICE_PAISE,
        currency=_CURRENCY,
        paid_item_amount_cents=_PRICE_PAISE,
        provider_order_total_cents=_PRICE_PAISE,
        status="active",
        purchased_at=now,
        expires_at=now + timedelta(days=attempt.validity_days),
    )
    session.add(winner)
    session.add(
        CreditLedger(
            user_id=user_id,
            amount=credits,
            reason=f"billing_purchase:razorpay:{payment_id}",
        )
    )
    await session.commit()

    webhook = await _make_webhook(
        session, cleanup, _link_paid_payload(attempt, nonce_hash, payment_id=payment_id)
    )
    wid = webhook.id

    async def _miss(*_args, **_kwargs):
        return None

    monkeypatch.setattr(billing_worker, "_find_existing_pack", _miss)

    await billing_worker.handle_razorpay_link_paid({}, str(wid))

    assert await _pack_count(db_maker, user_id) == 1  # only the winner
    assert await _purchase_ledger_count(db_maker, payment_id) == 1
    assert await _balance(db_maker, user_id) == 0
    assert (await _wh_status(db_maker, wid))[0] == "processed"


async def test_concurrent_ledger_reason_conflict_grants_nothing(
    session, db_maker, cleanup
) -> None:
    """A racing delivery commits the purchase ledger row before our grant flushes.

    Covers handle_razorpay_link_paid's ``granted is None`` branch.
    """
    nonce_hash = hashlib.sha256(b"rzp-race-ledger").hexdigest()
    user = await _make_user(session, cleanup, balance=0)
    user_id = user.id
    attempt = await _make_attempt(session, user, nonce_hash)
    credits = attempt.credits
    payment_id = f"pay_{uuid4().hex[:14]}"

    session.add(
        CreditLedger(
            user_id=user_id,
            amount=credits,
            reason=f"billing_purchase:razorpay:{payment_id}",
        )
    )
    await session.commit()

    webhook = await _make_webhook(
        session, cleanup, _link_paid_payload(attempt, nonce_hash, payment_id=payment_id)
    )
    wid = webhook.id

    await billing_worker.handle_razorpay_link_paid({}, str(wid))

    assert await _pack_count(db_maker, user_id) == 0  # speculative pack rolled back
    assert await _purchase_ledger_count(db_maker, payment_id) == 1
    assert await _balance(db_maker, user_id) == 0
    assert (await _wh_status(db_maker, wid))[0] == "processed"


# ---------------------------------------------------------------------------
# Refund — full / partial / status-driven
# ---------------------------------------------------------------------------


async def test_full_refund_revokes_all_remaining_once(
    session, db_maker, cleanup
) -> None:
    user = await _make_user(session, cleanup, balance=200)
    payment_id = f"pay_{uuid4().hex[:14]}"
    pack = await _make_pack(session, user, payment_id=payment_id)
    pack_id, user_id = pack.id, user.id
    wh = await _make_webhook(
        session,
        cleanup,
        _refund_payload(
            payment_id=payment_id,
            amount_refunded=_PRICE_PAISE,
            refund_status="full",
        ),
    )

    await billing_worker.handle_razorpay_refund({}, str(wh.id))

    assert await _balance(db_maker, user_id) == 0
    p = await _pack(db_maker, pack_id)
    assert p.credits_remaining == 0
    assert p.credits_revoked == 200
    assert p.status == "refunded"
    assert p.refunded_item_amount_cents_processed == _PRICE_PAISE
    rows = await _refund_ledger(db_maker, pack_id)
    assert len(rows) == 1
    assert rows[0][0] == -200
    assert rows[0][1].startswith(f"refund:billing:{pack_id}:{_PRICE_PAISE}:")
    assert await _debt(db_maker, pack_id) is None


async def test_amount_covers_payment_is_full_without_status(
    session, db_maker, cleanup
) -> None:
    """refund_status may lag; amount_refunded >= payment.amount drives a full refund."""
    user = await _make_user(session, cleanup, balance=200)
    payment_id = f"pay_{uuid4().hex[:14]}"
    pack = await _make_pack(session, user, payment_id=payment_id)
    pack_id, user_id = pack.id, user.id
    wh = await _make_webhook(
        session,
        cleanup,
        _refund_payload(
            payment_id=payment_id,
            amount_refunded=_PRICE_PAISE,
            refund_status="partial",  # provider hasn't flipped it to full yet
        ),
    )

    await billing_worker.handle_razorpay_refund({}, str(wh.id))

    p = await _pack(db_maker, pack_id)
    assert p.credits_revoked == 200
    assert p.status == "refunded"
    assert await _balance(db_maker, user_id) == 0


async def test_partial_refund_revokes_proportional(session, db_maker, cleanup) -> None:
    user = await _make_user(session, cleanup, balance=200)
    payment_id = f"pay_{uuid4().hex[:14]}"
    pack = await _make_pack(session, user, payment_id=payment_id)  # 200cr @ 79900p
    pack_id, user_id = pack.id, user.id
    # Refund half the payment → revoke half the credits.
    half = _PRICE_PAISE // 2
    wh = await _make_webhook(
        session,
        cleanup,
        _refund_payload(
            payment_id=payment_id, amount_refunded=half, refund_status="partial"
        ),
    )

    await billing_worker.handle_razorpay_refund({}, str(wh.id))

    p = await _pack(db_maker, pack_id)
    assert p.credits_revoked == 100
    assert p.credits_remaining == 100
    assert p.status == "active"
    assert await _balance(db_maker, user_id) == 100


async def test_refund_after_spend_creates_debt(session, db_maker, cleanup) -> None:
    # 200 purchased, 150 spent → 50 remaining, balance 50.
    user = await _make_user(session, cleanup, balance=50)
    payment_id = f"pay_{uuid4().hex[:14]}"
    pack = await _make_pack(
        session,
        user,
        payment_id=payment_id,
        credits_remaining=50,
        credits_consumed=150,
    )
    pack_id, user_id = pack.id, user.id
    wh = await _make_webhook(
        session,
        cleanup,
        _refund_payload(
            payment_id=payment_id, amount_refunded=_PRICE_PAISE, refund_status="full"
        ),
    )

    await billing_worker.handle_razorpay_refund({}, str(wh.id))

    assert await _balance(db_maker, user_id) == 0
    p = await _pack(db_maker, pack_id)
    assert p.credits_revoked == 200
    debt = await _debt(db_maker, pack_id)
    assert debt is not None
    assert debt.credits_owed == 150
    assert debt.status == "pending"
    assert debt.provider == "razorpay"


# ---------------------------------------------------------------------------
# Refund — idempotency + degradation
# ---------------------------------------------------------------------------


async def test_replay_same_amount_is_noop(session, db_maker, cleanup) -> None:
    user = await _make_user(session, cleanup, balance=200)
    payment_id = f"pay_{uuid4().hex[:14]}"
    pack = await _make_pack(session, user, payment_id=payment_id)
    pack_id, user_id = pack.id, user.id

    wh1 = await _make_webhook(
        session,
        cleanup,
        _refund_payload(
            payment_id=payment_id, amount_refunded=_PRICE_PAISE, refund_status="full"
        ),
    )
    await billing_worker.handle_razorpay_refund({}, str(wh1.id))
    assert await _balance(db_maker, user_id) == 0

    wh2 = await _make_webhook(
        session,
        cleanup,
        _refund_payload(
            payment_id=payment_id,
            amount_refunded=_PRICE_PAISE,
            refund_status="full",
            event_id="evt_rzp_refund_2",
        ),
    )
    await billing_worker.handle_razorpay_refund({}, str(wh2.id))

    assert await _balance(db_maker, user_id) == 0
    p = await _pack(db_maker, pack_id)
    assert p.credits_revoked == 200  # not 400
    assert len(await _refund_ledger(db_maker, pack_id)) == 1


async def test_missing_payment_entity_amounts_degrade_to_noop(
    session, db_maker, cleanup
) -> None:
    """A refund whose payment entity carries no amounts (both absent → coerced 0)
    must NOT over-revoke: it is a monotonic no-op, and lane 2 re-read is the
    backstop for the real cumulative."""
    user = await _make_user(session, cleanup, balance=200)
    payment_id = f"pay_{uuid4().hex[:14]}"
    pack = await _make_pack(session, user, payment_id=payment_id)
    pack_id, user_id = pack.id, user.id
    payload = _refund_payload(
        payment_id=payment_id, amount_refunded=0, refund_status=None
    )
    # Drop the payment amount entirely (a truncated/odd delivery).
    payload["payment"]["amount"] = None
    wh = await _make_webhook(session, cleanup, payload)

    await billing_worker.handle_razorpay_refund({}, str(wh.id))

    assert await _balance(db_maker, user_id) == 200  # untouched
    p = await _pack(db_maker, pack_id)
    assert p.credits_revoked == 0
    assert (await _wh_status(db_maker, wh.id))[0] == "processed"


async def test_reprocessing_same_refund_row_is_idempotent(
    session, db_maker, cleanup
) -> None:
    user = await _make_user(session, cleanup, balance=200)
    payment_id = f"pay_{uuid4().hex[:14]}"
    pack = await _make_pack(session, user, payment_id=payment_id)
    pack_id, user_id = pack.id, user.id
    wh = await _make_webhook(
        session,
        cleanup,
        _refund_payload(
            payment_id=payment_id, amount_refunded=_PRICE_PAISE, refund_status="full"
        ),
    )

    await billing_worker.handle_razorpay_refund({}, str(wh.id))
    await billing_worker.handle_razorpay_refund({}, str(wh.id))

    assert await _balance(db_maker, user_id) == 0
    p = await _pack(db_maker, pack_id)
    assert p.credits_revoked == 200
    assert len(await _refund_ledger(db_maker, pack_id)) == 1


# ---------------------------------------------------------------------------
# Refund — no-pack proof branches (shared _refund_without_pack)
# ---------------------------------------------------------------------------


async def test_missing_pack_with_proof_retains_reversal_evidence(
    session, db_maker, cleanup
) -> None:
    user = await _make_user(session, cleanup, balance=0)
    nonce_hash = hashlib.sha256(b"rzp-retry").hexdigest()
    attempt = await _make_attempt(session, user, nonce_hash)
    payment_id = f"pay_{uuid4().hex[:14]}"  # no pack yet
    # The payment entity carries the round-tripped notes (best-effort proof).
    wh = await _make_webhook(
        session,
        cleanup,
        _refund_payload(
            payment_id=payment_id,
            amount_refunded=_PRICE_PAISE,
            refund_status="full",
            notes=_notes(attempt, nonce_hash),
        ),
    )
    await billing_worker.handle_razorpay_refund({}, str(wh.id))

    status, last_error = await _wh_status(db_maker, wh.id)
    assert status == "processed"
    assert last_error == "refund_waiting_for_payment_grant"
    assert await _balance(db_maker, user.id) == 0


async def test_missing_pack_expired_attempt_retains_reversal_evidence(
    session, db_maker, cleanup
) -> None:
    # Branch (b): the proven attempt has expired and >24h elapsed since the refund
    # was first received → give up as an audited processed no-op (the order_created
    # grant will never arrive), never touching any balance.
    user = await _make_user(session, cleanup, balance=0)
    nonce_hash = hashlib.sha256(b"rzp-giveup").hexdigest()
    attempt = await _make_attempt(session, user, nonce_hash, status="expired")
    payment_id = f"pay_{uuid4().hex[:14]}"  # no pack, and none will ever land
    wh = await _make_webhook(
        session,
        cleanup,
        _refund_payload(
            payment_id=payment_id,
            amount_refunded=_PRICE_PAISE,
            refund_status="full",
            notes=_notes(attempt, nonce_hash),
        ),
        received_at=datetime.now(UTC) - timedelta(hours=25),
    )

    await billing_worker.handle_razorpay_refund({}, str(wh.id))

    status, last_error = await _wh_status(db_maker, wh.id)
    assert status == "processed"
    assert last_error == "refund_waiting_for_payment_grant"
    assert await _balance(db_maker, user.id) == 0


async def test_missing_pack_without_proof_audited_no_unrelated_revoke(
    session, db_maker, cleanup
) -> None:
    user = await _make_user(session, cleanup, balance=200)
    bystander_payment = f"pay_{uuid4().hex[:14]}"
    bystander = await _make_pack(session, user, payment_id=bystander_payment)
    bystander_id, user_id = bystander.id, user.id

    payment_id = f"pay_{uuid4().hex[:14]}"  # no pack, no notes proof
    wh = await _make_webhook(
        session,
        cleanup,
        _refund_payload(
            payment_id=payment_id, amount_refunded=_PRICE_PAISE, refund_status="full"
        ),
    )
    await billing_worker.handle_razorpay_refund({}, str(wh.id))

    status, last_error = await _wh_status(db_maker, wh.id)
    assert status == "processed"
    assert last_error == "refund_waiting_for_payment_grant"
    assert await _balance(db_maker, user_id) == 200  # untouched
    b = await _pack(db_maker, bystander_id)
    assert b.credits_revoked == 0
    assert b.credits_remaining == 200
