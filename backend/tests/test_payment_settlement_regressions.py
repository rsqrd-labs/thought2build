"""Payment-review regressions on migrated PostgreSQL; provider HTTP is mocked."""

import hashlib
import hmac
import json
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from config import settings
from database import get_db
from middleware.auth import get_current_user
from models import (
    BillingAdminCorrection,
    BillingCheckoutAttempt,
    BillingCreditDebt,
    BillingCreditPack,
    BillingDispute,
    BillingWebhookEvent,
    User,
)
from routers import billing, credits
from schemas.billing import AdminCorrectionRequest
from services import billing_worker as worker
from services import razorpay_settlement as settlement
from services.credit_service import credit_service
from services.razorpay_service import RazorpayPayment, razorpay_service

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not os.getenv("TEST_DATABASE_URL"),
        reason="Requires an isolated migrated PostgreSQL database",
    ),
]


@pytest.fixture
async def env(monkeypatch):
    engine = create_async_engine(os.environ["TEST_DATABASE_URL"], poolclass=NullPool)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(worker, "AsyncSessionLocal", maker)
    monkeypatch.setattr(settlement, "AsyncSessionLocal", maker)
    monkeypatch.setattr(settings, "razorpay_key_id", "rzp_test_review")
    monkeypatch.setattr(settings, "razorpay_key_secret", "review-key")
    monkeypatch.setattr(settings, "razorpay_webhook_secret", "review-webhook")
    monkeypatch.setattr(settings, "razorpay_webhook_secret_prev", "")
    monkeypatch.setattr(settings, "payment_provider", "razorpay")
    monkeypatch.setattr(settings, "payments_enabled", False)
    monkeypatch.setattr(credit_service, "_invalidate", AsyncMock())
    monkeypatch.setattr(billing, "enqueue", AsyncMock())
    users, payments = [], []

    async def new_user(balance=0):
        async with maker() as db:
            user = User(
                email=f"review-{uuid4()}@example.com",
                google_id=str(uuid4()),
                credit_balance=balance,
            )
            db.add(user)
            await db.commit()
            users.append(user.id)
            return user

    async def pack(user, amount=200, days=30):
        async with maker() as db:
            payment_id = f"pay_review_{uuid4().hex}"
            payments.append(payment_id)
            p = BillingCreditPack(
                user_id=user.id,
                provider="razorpay",
                provider_order_id=payment_id,
                provider_checkout_id=f"plink_{uuid4().hex}",
                credits_purchased=amount,
                credits_remaining=amount,
                price_cents=154900,
                currency="INR",
                paid_item_amount_cents=154900,
                provider_order_total_cents=154900,
                purchased_at=datetime.now(UTC),
                expires_at=datetime.now(UTC) + timedelta(days=days),
                status="active",
            )
            db.add(p)
            await db.flush()
            await credit_service.grant_credits_with_debt_recovery(
                db,
                user_id=user.id,
                pack=p,
                granted_credits=amount,
                ledger_reason=f"billing_purchase:razorpay:{payment_id}",
            )
            await db.commit()
            return p

    async def attempt(user, expired=False):
        async with maker() as db:
            a = BillingCheckoutAttempt(
                user_id=user.id,
                provider="razorpay",
                checkout_ref=f"review_{uuid4().hex}",
                checkout_nonce_hash=hashlib.sha256(b"review-nonce").hexdigest(),
                provider_checkout_id=f"plink_{uuid4().hex}",
                credits=200,
                price_cents=154900,
                currency="INR",
                validity_days=30,
                status="provider_created",
                expires_at=datetime.now(UTC) + timedelta(minutes=-1 if expired else 30),
            )
            db.add(a)
            await db.commit()
            payments.append(f"pay_{a.id.hex}")
            return a

    def entities(a):
        return {
            "payment_link": {
                "entity": {
                    "id": a.provider_checkout_id,
                    "reference_id": str(a.id),
                    "amount": 154900,
                    "currency": "INR",
                    "status": "paid",
                    "notes": {
                        "user_id": str(a.user_id),
                        "checkout_ref": a.checkout_ref,
                        "checkout_nonce": "review-nonce",
                        "environment": "test",
                    },
                }
            },
            "payment": {
                "entity": {
                    "id": f"pay_{a.id.hex}",
                    "amount": 154900,
                    "currency": "INR",
                    "status": "captured",
                    "amount_refunded": 0,
                    "created_at": int(datetime.now(UTC).timestamp()),
                }
            },
        }

    async def ingest(payload):
        async with maker() as db:
            identity = dict(
                provider="razorpay",
                event_name=payload["event_name"],
                provider_object_id=payload["payment_id"],
                payload_hash=hashlib.sha256(json.dumps(payload).encode()).hexdigest(),
            )
            wid = await db.scalar(
                insert(BillingWebhookEvent)
                .values(
                    **identity,
                    provider_object_type="payments",
                    status="received",
                    normalized_payload=payload,
                )
                .on_conflict_do_nothing()
                .returning(BillingWebhookEvent.id)
            )
            if wid is None:
                wid = await db.scalar(
                    select(BillingWebhookEvent.id).filter_by(**identity)
                )
            await db.commit()
        await worker.billing_process_webhook({}, str(wid))
        return wid

    async def snapshot(user, p=None):
        async with maker() as db:
            u = await db.get(User, user.id)
            debts = (
                (
                    await db.execute(
                        select(BillingCreditDebt).where(
                            BillingCreditDebt.user_id == user.id
                        )
                    )
                )
                .scalars()
                .all()
            )
            outstanding = sum(d.credits_owed - d.credits_recovered for d in debts)
            return (
                u.credit_balance,
                outstanding,
                await db.get(BillingCreditPack, p.id) if p else None,
            )

    async def debit(user, amount=10):
        async with maker() as db:
            entry = await credit_service.deduct(
                db, user.id, amount, "review-generation"
            )
            await db.commit()
            return entry.id

    async def restore(entry):
        async with maker() as db:
            refunded = await credit_service.refund(db, entry)
            await db.commit()
            return refunded

    async def reverse(p, cents=154900):
        async with maker() as db:
            await settlement.lock_payment(db, p.provider_order_id)
            p = await db.get(BillingCreditPack, p.id)
            await settlement.settle_pack(db, p, cents)
            await db.commit()

    @asynccontextmanager
    async def client(user):
        app = FastAPI()
        app.include_router(billing.router)
        app.include_router(credits.router)

        async def db_dep():
            async with maker() as db:
                yield db

        app.dependency_overrides[get_db] = db_dep
        app.dependency_overrides[get_current_user] = lambda: user
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as c:
            yield c

    yield type(
        "Env",
        (),
        {
            name: staticmethod(value)
            for name, value in locals().copy().items()
            if name
            in {
                "new_user",
                "pack",
                "attempt",
                "entities",
                "ingest",
                "snapshot",
                "debit",
                "restore",
                "reverse",
                "client",
            }
        },
    )(), maker
    async with maker() as db:
        await db.execute(
            delete(BillingDispute).where(
                BillingDispute.provider_payment_id.in_(payments)
            )
        )
        await db.execute(
            delete(BillingWebhookEvent).where(
                BillingWebhookEvent.provider_object_id.in_(payments)
            )
        )
        await db.execute(
            delete(BillingAdminCorrection).where(
                BillingAdminCorrection.target_user_id.in_(users)
            )
        )
        await db.execute(
            delete(BillingCreditDebt).where(BillingCreditDebt.user_id.in_(users))
        )
        await db.execute(delete(User).where(User.id.in_(users)))
        await db.commit()
    await engine.dispose()


async def test_valid_payment_after_local_expiry_grants_once(env):
    e, m = env
    u = await e.new_user()
    a = await e.attempt(u, expired=True)
    await worker._reconcile_lane3()
    payload = billing._normalize_razorpay_link_paid(e.entities(a))
    wid = await e.ingest(payload)
    await worker.billing_process_webhook({}, str(wid))
    assert (await e.snapshot(u))[:2] == (200, 0)
    async with m() as db:
        assert (await db.get(BillingCheckoutAttempt, a.id)).status == "completed"


@pytest.mark.parametrize("refund", [0, 154900])
async def test_missing_webhook_recovery_verifies_link_and_current_payment(
    env, monkeypatch, refund
):
    e, m = env
    u = await e.new_user()
    a = await e.attempt(u, expired=True)
    data = e.entities(a)
    link = data["payment_link"]["entity"]
    payment = data["payment"]["entity"]
    payment["amount_refunded"] = refund
    if refund:
        payment["status"] = "refunded"
    link["payments"] = [{"payment_id": payment["id"], "status": "captured"}]
    calls = []

    async def handler(request):
        assert request.headers["authorization"].startswith("Basic ")
        calls.append(request.url.path)
        return httpx.Response(
            200, json=link if "payment_links" in request.url.path else payment
        )

    @asynccontextmanager
    async def ctx(_client):
        async with httpx.AsyncClient(
            base_url="https://api.razorpay.com", transport=httpx.MockTransport(handler)
        ) as c:
            yield c

    monkeypatch.setattr(razorpay_service, "_client_ctx", ctx)
    await settlement.recover_attempts({})
    assert calls == [
        f"/v1/payment_links/{a.provider_checkout_id}",
        f"/v1/payments/{payment['id']}",
    ]
    assert (await e.snapshot(u))[:2] == (0 if refund else 200, 0)


@pytest.mark.parametrize("field", ["user_id", "checkout_nonce"])
async def test_payment_recovery_rejects_wrong_proof(env, field):
    e, m = env
    u = await e.new_user()
    a = await e.attempt(u)
    data = e.entities(a)
    data["payment_link"]["entity"]["notes"][field] = str(uuid4())
    await e.ingest(billing._normalize_razorpay_link_paid(data))
    assert (await e.snapshot(u))[0] == 0


async def test_refund_without_notes_before_grant_never_exposes_credits(env):
    e, m = env
    u = await e.new_user()
    a = await e.attempt(u)
    data = e.entities(a)
    data["payment"]["entity"]["amount_refunded"] = 154900
    data["refund"] = {
        "entity": {
            "id": f"rfnd_{uuid4().hex}",
            "payment_id": f"pay_{a.id.hex}",
            "amount": 154900,
        }
    }
    await e.ingest(billing._normalize_razorpay_refund(data))
    await e.ingest(billing._normalize_razorpay_link_paid(e.entities(a)))
    assert (await e.snapshot(u))[:2] == (0, 0)


async def test_failed_generation_restores_pack_and_cash_refund_has_no_false_debt(env):
    e, m = env
    u = await e.new_user()
    p = await e.pack(u)
    entry = await e.debit(u)
    assert await e.restore(entry) == 10
    assert await e.restore(entry) == 0
    balance, debt, p = await e.snapshot(u, p)
    assert (balance, debt, p.credits_remaining, p.credits_consumed) == (200, 0, 200, 0)
    await e.reverse(p)
    assert (await e.snapshot(u))[:2] == (0, 0)


@pytest.mark.parametrize("expiry_before_restore", [False, True])
async def test_failed_generation_restoration_preserves_expiry(
    env, expiry_before_restore
):
    e, m = env
    u = await e.new_user()
    p = await e.pack(u)
    entry = await e.debit(u)
    if not expiry_before_restore:
        await e.restore(entry)
    async with m() as db:
        (await db.get(BillingCreditPack, p.id)).expires_at = datetime.now(
            UTC
        ) - timedelta(seconds=1)
        await db.commit()
    if expiry_before_restore:
        assert await e.restore(entry) == 0
    async with e.client(u) as c:
        assert (await c.get("/credits/balance")).json()["balance"] == 0
        history = (await c.get("/billing/history")).json()[0]
        assert (history["status"], history["credits_remaining"]) == ("expired", 0)
    balance, debt, p = await e.snapshot(u, p)
    assert (balance, debt, p.credits_expired, p.credits_consumed) == (0, 0, 200, 0)


async def test_refund_restores_multiple_source_packs_and_starter_credits(env):
    e, m = env
    u = await e.new_user(5)
    p1 = await e.pack(u, amount=6, days=10)
    p2 = await e.pack(u, amount=7, days=20)
    entry = await e.debit(u, 16)
    assert await e.restore(entry) == 16
    assert (await e.snapshot(u, p1))[2].credits_remaining == 6
    assert (await e.snapshot(u, p2))[2].credits_remaining == 7
    assert (await e.snapshot(u))[0] == 18


@pytest.mark.parametrize("donor", ["none", "existing", "later"])
async def test_cash_refund_before_operation_restore_offsets_debt_or_returns_donor(
    env, donor
):
    e, m = env
    u = await e.new_user()
    p = await e.pack(u)
    entry = await e.debit(u)
    other = await e.pack(u) if donor == "existing" else None
    await e.reverse(p)
    if donor == "later":
        other = await e.pack(u)
    assert await e.restore(entry) == (0 if donor == "none" else 10)
    assert (await e.snapshot(u))[:2] == (0 if donor == "none" else 200, 0)
    if other:
        assert (await e.snapshot(u, other))[2].credits_remaining == 200


async def test_reconciliation_catches_consumed_and_expired_refunds(env, monkeypatch):
    e, m = env
    u = await e.new_user()
    p = await e.pack(u)
    await e.debit(u, 200)
    v = await e.new_user()
    q = await e.pack(v)
    await e.debit(v, 50)
    async with m() as db:
        (await db.get(BillingCreditPack, q.id)).expires_at = datetime.now(
            UTC
        ) - timedelta(days=1)
        await credit_service.get_balance(db, v.id)
        await db.commit()
    read = AsyncMock(
        side_effect=lambda pid: RazorpayPayment(pid, "refunded", 154900, 154900, "full")
    )
    monkeypatch.setattr(razorpay_service, "get_payment", read)
    await worker._reconcile_lane2(provider="razorpay", state={})
    assert set(c.args[0] for c in read.call_args_list) == {
        p.provider_order_id,
        q.provider_order_id,
    }
    assert (await e.snapshot(u))[:2] == (0, 200)
    assert (await e.snapshot(v))[:2] == (0, 50)


async def dispute(e, p, status, timestamp, dispute_id=None):
    did = dispute_id or f"disp_{p.provider_order_id}"
    payload = billing._normalize_razorpay_dispute(
        {
            "dispute": {
                "entity": {
                    "id": did,
                    "payment_id": p.provider_order_id,
                    "amount": 154900,
                    "currency": "INR",
                    "status": "open" if status == "created" else status,
                    "amount_deducted": 154900 if status == "lost" else 0,
                }
            }
        },
        f"payment.dispute.{status}",
        timestamp,
    )
    return await e.ingest(payload)


async def test_dispute_lost_replayed_and_cash_refunded_only_reverses_once(env):
    e, m = env
    u = await e.new_user()
    p = await e.pack(u)
    wid = await dispute(e, p, "lost", 100)
    await worker.billing_process_webhook({}, str(wid))
    await dispute(e, p, "created", 90)
    await e.reverse(p)
    balance, debt, p = await e.snapshot(u, p)
    assert (balance, debt, p.credits_revoked) == (0, 0, 200)


async def test_open_and_won_disputes_do_not_charge_and_won_restores_lost_debt(env):
    e, m = env
    u = await e.new_user()
    p = await e.pack(u)
    await e.debit(u, 50)
    await dispute(e, p, "created", 100)
    assert (await e.snapshot(u))[:2] == (150, 0)
    await dispute(e, p, "lost", 101)
    assert (await e.snapshot(u))[:2] == (0, 50)
    await dispute(e, p, "won", 102)
    balance, debt, p = await e.snapshot(u, p)
    assert (
        balance,
        debt,
        p.credits_remaining,
        p.credits_consumed,
        p.credits_revoked,
    ) == (150, 0, 150, 50, 0)
    await dispute(e, p, "lost", 101)
    assert (await e.snapshot(u))[:2] == (150, 0)


async def test_status_after_refund_reports_current_usable_credits(env):
    e, m = env
    u = await e.new_user()
    a = await e.attempt(u)
    await e.ingest(billing._normalize_razorpay_link_paid(e.entities(a)))
    async with m() as db:
        p = await db.scalar(
            select(BillingCreditPack).where(
                BillingCreditPack.provider_order_id == f"pay_{a.id.hex}"
            )
        )
    await e.reverse(p)
    async with e.client(u) as c:
        response = (
            await c.get("/billing/status", params={"checkout_ref": a.checkout_ref})
        ).json()
    assert (
        response["status"],
        response["settlement_status"],
        response["credits_added"],
    ) == ("completed", "refunded", 0)


async def test_verified_admin_correction_completes_original_checkout(env, monkeypatch):
    e, m = env
    u = await e.new_user()
    a = await e.attempt(u, expired=True)
    payload = billing._normalize_razorpay_link_paid(e.entities(a))
    monkeypatch.setattr(
        razorpay_service, "get_paid_checkout", AsyncMock(return_value=payload)
    )
    request = AdminCorrectionRequest(
        checkout_ref=a.checkout_ref,
        provider_order_id=payload["payment_id"],
        target_user_id=u.id,
        credits=200,
        price_cents=154900,
        currency="INR",
        reason="Verified recovery",
        evidence_url="https://example.com/support/review",
    )
    async with m() as db:
        assert (await billing.admin_correction(request, admin=u, db=db)).applied
    async with e.client(u) as c:
        response = await c.get(
            "/billing/status", params={"checkout_ref": a.checkout_ref}
        )
    assert response.status_code == 200
    assert response.json()["credits_added"] == 200


async def test_signed_dispute_http_is_persisted_and_invalid_signature_is_rejected(env):
    e, m = env
    u = await e.new_user()
    p = await e.pack(u)
    payload = {
        "event": "payment.dispute.lost",
        "created_at": 100,
        "payload": {
            "dispute": {
                "entity": {
                    "id": f"disp_{uuid4().hex}",
                    "payment_id": p.provider_order_id,
                    "amount": 154900,
                    "currency": "INR",
                    "status": "lost",
                }
            }
        },
    }
    raw = json.dumps(payload).encode()
    signature = hmac.new(b"review-webhook", raw, hashlib.sha256).hexdigest()
    async with e.client(u) as c:
        assert (
            await c.post(
                "/billing/webhook/razorpay",
                content=raw,
                headers={"X-Razorpay-Signature": "bad"},
            )
        ).status_code == 400
        assert (
            await c.post(
                "/billing/webhook/razorpay",
                content=raw,
                headers={"X-Razorpay-Signature": signature},
            )
        ).status_code == 200
    async with m() as db:
        row = await db.scalar(
            select(BillingWebhookEvent).where(
                BillingWebhookEvent.provider_object_id == p.provider_order_id
            )
        )
    await worker.billing_process_webhook({}, str(row.id))
    assert (await e.snapshot(u))[:2] == (0, 0)


async def test_concurrent_operation_refund_and_cash_reversal_preserve_accounting(env):
    import asyncio

    e, m = env
    u = await e.new_user()
    p = await e.pack(u)
    entry = await e.debit(u, 50)
    await asyncio.wait_for(asyncio.gather(e.restore(entry), e.reverse(p)), 10)
    balance, debt, p = await e.snapshot(u, p)
    assert (balance, debt, p.credits_consumed, p.credits_revoked) == (0, 0, 0, 200)
    assert await e.restore(entry) == 0


async def test_concurrent_payment_grant_and_refund_settle_once(env):
    import asyncio

    e, m = env
    u = await e.new_user()
    a = await e.attempt(u)
    entities = e.entities(a)
    paid = billing._normalize_razorpay_link_paid(entities)
    entities["payment"]["entity"]["amount_refunded"] = 154900
    entities["refund"] = {
        "entity": {
            "id": f"rfnd_{uuid4().hex}",
            "payment_id": paid["payment_id"],
            "amount": 154900,
        }
    }
    refund = billing._normalize_razorpay_refund(entities)
    await asyncio.wait_for(asyncio.gather(e.ingest(paid), e.ingest(refund)), 10)
    assert (await e.snapshot(u))[:2] == (0, 0)
    async with m() as db:
        packs = (
            (
                await db.execute(
                    select(BillingCreditPack).where(BillingCreditPack.user_id == u.id)
                )
            )
            .scalars()
            .all()
        )
        assert len(packs) == 1
        assert packs[0].credits_revoked == 200


@pytest.mark.parametrize("donor", ["existing", "later", "expired"])
async def test_won_dispute_returns_donor_with_original_expiry(env, donor):
    e, m = env
    u = await e.new_user()
    p = await e.pack(u)
    await e.debit(u, 50)
    other = await e.pack(u) if donor == "existing" else None
    await dispute(e, p, "lost", 100)
    if other is None:
        other = await e.pack(u)
    if donor == "expired":
        async with m() as db:
            (await db.get(BillingCreditPack, other.id)).expires_at = datetime.now(
                UTC
            ) - timedelta(days=1)
            await db.commit()
    await dispute(e, p, "won", 101)
    assert (await e.snapshot(u))[:2] == (150 if donor == "expired" else 350, 0)
    other = (await e.snapshot(u, other))[2]
    assert other.credits_debt_recovered == 0
    assert other.credits_remaining == (0 if donor == "expired" else 200)


async def test_newer_open_dispute_cannot_undo_terminal_loss(env):
    e, m = env
    u = await e.new_user()
    p = await e.pack(u)
    await dispute(e, p, "lost", 100)
    await dispute(e, p, "created", 101)
    assert (await e.snapshot(u))[:2] == (0, 0)


async def test_conflicting_terminal_timestamp_reads_authoritative_dispute(
    env, monkeypatch
):
    e, m = env
    u = await e.new_user()
    p = await e.pack(u)
    await dispute(e, p, "lost", 100)
    read = AsyncMock(
        return_value={
            "payment_id": p.provider_order_id,
            "currency": "INR",
            "status": "won",
            "amount": 154900,
            "amount_deducted": 0,
        }
    )
    monkeypatch.setattr(razorpay_service, "get_dispute", read)
    await dispute(e, p, "won", 100)
    read.assert_awaited_once()
    assert (await e.snapshot(u))[:2] == (200, 0)


@pytest.mark.parametrize(
    "field",
    [
        "link_id",
        "reference_id",
        "payment_id",
        "order_id",
        "nonce",
        "amount",
        "currency",
    ],
)
async def test_recovery_rejects_mismatched_provider_proof(env, monkeypatch, field):
    from fastapi import HTTPException

    from services.razorpay_service import RazorpayError

    e, m = env
    u = await e.new_user()
    a = await e.attempt(u)
    entities = e.entities(a)
    link, payment = entities["payment_link"]["entity"], entities["payment"]["entity"]
    link["payments"] = [{"payment_id": payment["id"]}]
    link["order_id"] = payment["order_id"] = "order_review"
    if field == "link_id":
        link["id"] = "plink_wrong"
    if field == "reference_id":
        link["reference_id"] = str(uuid4())
    if field == "payment_id":
        payment["id"] = "pay_wrong"
    if field == "order_id":
        payment["order_id"] = "order_wrong"
    if field == "nonce":
        link["notes"]["checkout_nonce"] = "wrong"
    if field == "amount":
        payment["amount"] = 1
    if field == "currency":
        payment["currency"] = "USD"

    async def handler(request):
        return httpx.Response(
            200, json=link if "payment_links" in request.url.path else payment
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://api.razorpay.com"
    ) as client:
        if field in {"link_id", "reference_id", "payment_id", "order_id"}:
            with pytest.raises((RazorpayError, HTTPException)):
                await razorpay_service.get_paid_checkout(a, client=client)
        else:
            payload = await razorpay_service.get_paid_checkout(a, client=client)
            await e.ingest(payload)
    assert (await e.snapshot(u))[:2] == (0, 0)
