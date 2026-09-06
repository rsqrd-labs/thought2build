"""Provider-bound recovery and settlement shared by Razorpay webhooks and cron.

Razorpay cash refunds and disputed value cover the same purchased entitlement.
Use the greater cumulative reversal, capped at the payment, never revoke the
same credit twice. Open disputes are tracked but do not withhold credits.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import structlog
from fastapi import HTTPException
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert

from config import settings
from database import AsyncSessionLocal
from models import (
    BillingCheckoutAttempt,
    BillingCreditPack,
    BillingDispute,
    BillingWebhookEvent,
)
from services.credit_service import credit_service
from services.razorpay_service import (
    RazorpayError,
    RazorpayRateLimitError,
    razorpay_service,
)

logger = structlog.get_logger(__name__)


@dataclass
class SettlementOutcome:
    revoked: int = 0
    debt_created: int = 0
    released: int = 0
    reason: str = "refund"

    def record(self, *, reconciled: bool = False) -> None:
        """Emit only after the caller commits the settlement transaction."""
        from services.observability import (
            BILLING_CREDIT_DEBT_CREATED,
            BILLING_CREDITS_REVOKED,
            BILLING_RECONCILE_MISMATCH,
        )

        if self.revoked:
            BILLING_CREDITS_REVOKED.labels(provider="razorpay", reason=self.reason).inc(
                self.revoked
            )
        if self.debt_created:
            BILLING_CREDIT_DEBT_CREATED.labels(
                provider="razorpay", reason=self.reason
            ).inc(self.debt_created)
        if reconciled and (self.revoked or self.released):
            BILLING_RECONCILE_MISMATCH.labels(provider="razorpay").inc()
        if self.revoked or self.released:
            logger.info(
                "billing.razorpay.settled",
                reason=self.reason,
                credits_revoked=self.revoked,
                debt_created=self.debt_created,
                credits_released=self.released,
                reconciled=reconciled,
            )


async def lock_payment(db, payment_id: str) -> None:
    # Serializes grant/reversal/dispute-before-grant, even without a pack yet.
    # All paths take this before attempt/user/pack locks.
    await db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"razorpay:{payment_id}"},
    )


async def settle_pack(
    db, pack: BillingCreditPack, refunded_cents: int
) -> SettlementOutcome:
    """Settle known cash refunds and latest dispute decisions atomically."""
    await credit_service._expire_user_packs(db, pack.user_id)
    # Refresh after acquiring the canonical user lock.
    pack = (
        await db.execute(
            select(BillingCreditPack)
            .where(
                BillingCreditPack.id == pack.id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    disputed = await db.scalar(
        select(func.coalesce(func.sum(BillingDispute.amount_cents), 0)).where(
            BillingDispute.provider_payment_id == pack.provider_order_id,
            BillingDispute.status.in_(("lost", "closed_loss")),
            BillingDispute.currency == pack.currency,
        )
    )
    cash = min(
        pack.price_cents, max(pack.provider_refunded_total_cents_seen, refunded_cents)
    )
    effective = min(pack.price_cents, max(cash, int(disputed or 0)))
    old_effective = pack.refunded_item_amount_cents_processed
    outcome = SettlementOutcome(reason="dispute" if disputed else "refund")
    if effective > old_effective:
        reversal = await credit_service.apply_refund_reversal(
            db,
            source_pack=pack,
            provider_refunded_amount_cents=effective,
            full_or_fraud=effective == pack.price_cents,
            reason_label="dispute" if disputed else "refund",
            ledger_reason=f"refund:billing:{pack.id}:{effective}:{uuid4()}",
        )
        outcome.revoked = reversal.credits_revoked
        outcome.debt_created = reversal.debt_created
    elif effective < old_effective:
        target = (
            (pack.credits_purchased - pack.credits_expired) * effective
        ) // pack.price_cents
        outcome.released = max(0, pack.credits_revoked - target)
        await credit_service.release_reversal(
            db,
            pack,
            target,
            ledger_reason=f"dispute_release:{pack.id}:{uuid4()}",
        )
        pack.refunded_item_amount_cents_processed = effective
    # This field records CASH only; the processed-item field records the total
    # settled entitlement (which may decrease after a newer won decision).
    pack.provider_refunded_total_cents_seen = cash
    if effective == pack.price_cents:
        pack.status = "disputed" if disputed and cash < pack.price_cents else "refunded"
    elif pack.expires_at <= datetime.now(timezone.utc):
        pack.status = "expired"
    else:
        pack.status = "active" if pack.credits_remaining else "consumed"
    await db.flush()
    return outcome


async def known_refund_cents(db, payment_id: str) -> int:
    # Signed refunds can precede their grant and have no checkout notes. The
    # durable inbox still proves payment identity; never discard that evidence.
    payloads = (
        (
            await db.execute(
                select(BillingWebhookEvent.normalized_payload).where(
                    BillingWebhookEvent.provider == "razorpay",
                    BillingWebhookEvent.provider_object_id == payment_id,
                    BillingWebhookEvent.event_name == "refund.processed",
                )
            )
        )
        .scalars()
        .all()
    )
    return max(
        (int((p.get("payment") or {}).get("amount_refunded") or 0) for p in payloads),
        default=0,
    )


async def recover_attempts(state: dict, *, heartbeat=None) -> dict:
    """Bounded round-robin re-read of unresolved server-recorded Payment Links."""
    if not settings.razorpay_enabled:
        return state
    last = state.get("recovery_last_attempt_id")
    cutoff = datetime.now(timezone.utc) - timedelta(
        days=settings.razorpay_reconcile_lookback_days
    )
    stmt = (
        select(BillingCheckoutAttempt)
        .where(
            BillingCheckoutAttempt.provider == "razorpay",
            BillingCheckoutAttempt.status != "completed",
            BillingCheckoutAttempt.provider_checkout_id.isnot(None),
            BillingCheckoutAttempt.created_at >= cutoff,
        )
        .order_by(BillingCheckoutAttempt.id)
        .limit(settings.razorpay_recovery_attempts_per_run)
    )
    if last:
        stmt = stmt.where(BillingCheckoutAttempt.id > UUID(last))
    async with AsyncSessionLocal() as db:
        attempts = (await db.execute(stmt)).scalars().all()
    stopped = False
    for attempt in attempts:
        if heartbeat:
            await heartbeat()
        try:
            payload = await razorpay_service.get_paid_checkout(attempt)
            if payload is not None:
                await process_recovered_payment(payload)
        except RazorpayRateLimitError:
            stopped = True
            break
        except (RazorpayError, HTTPException):
            logger.warning(
                "billing.recovery.provider_read_failed", attempt_id=str(attempt.id)
            )
        except Exception:
            logger.exception(
                "billing.recovery.attempt_failed", attempt_id=str(attempt.id)
            )
        # A bad link cannot starve all later customers. The cursor wraps and
        # retries unresolved attempts on the next scan.
        state["recovery_last_attempt_id"] = str(attempt.id)
    if not stopped and len(attempts) < settings.razorpay_recovery_attempts_per_run:
        state["recovery_last_attempt_id"] = None
    return state


async def process_recovered_payment(payload: dict) -> None:
    from routers.billing import _payload_hash
    from services.billing_worker import billing_process_webhook

    payload = {**payload, "authority": "razorpay_api"}
    identity = dict(
        provider="razorpay",
        event_name="payment_link.paid",
        provider_object_id=payload["payment_id"],
        payload_hash=_payload_hash(payload),
    )
    async with AsyncSessionLocal() as db:
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
            wid = await db.scalar(select(BillingWebhookEvent.id).filter_by(**identity))
        await db.commit()
    await billing_process_webhook({}, str(wid))
