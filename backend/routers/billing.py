"""Billing router — Phase 22 Lemon Squeezy migration, Razorpay alternative (issue #44).

Endpoint inventory
------------------
GET  /billing/package           Unauthenticated — active provider's package config
POST /billing/checkout          Authenticated   — attempt-first hosted checkout
GET  /billing/status            Authenticated   — poll a checkout by ``checkout_ref``
GET  /billing/history           Authenticated   — the user's billing_credit_packs rows
POST /billing/webhook           No auth/CSRF    — Lemon Squeezy webhook receiver
POST /billing/webhook/razorpay  No auth/CSRF    — Razorpay webhook receiver

Exactly **one** gateway is active at a time (issue #44): ``PAYMENT_PROVIDER``
selects it and ``PAYMENTS_ENABLED`` is the master kill switch. The two flags gate
checkout creation and the package ``enabled`` field ONLY
(``settings.billing_checkout_enabled``); **both** webhook routes stay registered
and keep processing regardless (D3), so refunds/disputes for the inactive
provider's old orders still settle after a switch. A provider with no webhook
secret configured fails closed (400) on its webhook path.

Checkout is **attempt-first** (Plan §25.6 T-296): Thought2Build commits the local
``billing_checkout_attempts`` row — carrying the active provider's economics
snapshot and only the ``sha256(checkout_nonce)`` — **before** calling the
provider, then mints the hosted checkout (Lemon checkout / Razorpay Payment
Link), then commits the ``provider_created`` transition, and only then returns
``checkout_ref`` to the client. The signed paid-order webhook (Lemon
``order_created`` / Razorpay ``payment_link.paid``) is the sole credit-grant
authority; this router never grants credits.

``GET /status`` is IDOR-safe: one query scoped by BOTH ``checkout_ref`` and
``user_id`` (404 on any mismatch — no resource-existence leak). The raw nonce is
never returned by any endpoint; only its hash is persisted.

``POST /billing/webhook`` is the durable, **verify-before-work** Lemon receiver
(T-297): it reads the raw body, verifies the ``X-Signature`` HMAC against the
two-secret rotation list (constant-time), parses, sanitises into an allow-listed
``normalized_payload`` (no PII, no raw nonce, no signature — only
``sha256(checkout_nonce)``), **commits** a ``billing_webhook_events`` inbox row,
then enqueues ``billing_process_webhook`` by row id. It performs **no** money
mutation inline — the signed ``order_created`` event is the sole grant authority,
and the worker (T-298/T-299/T-300) reads only the sanitised inbox. CSRF /
rate-limit exemptions (``middleware/csrf.py``, ``middleware/rate_limit.py``) are
reused.

``POST /billing/webhook/razorpay`` is the Razorpay receiver with the same
five-step verify-before-work shape (issue #44 Plan §5): raw body →
constant-time ``X-Razorpay-Signature`` HMAC (current + previous secret, fail
closed) → dispatch on the top-level ``event`` (only ``payment_link.paid`` and
``refund.processed`` are actionable; everything else is acknowledged without an
inbox row) → allow-listed ``normalized_payload`` (payment/link/refund entities;
``notes`` nonce hashed; PII dropped; ``payload_hash`` computed BEFORE the
``x-razorpay-event-id`` header is injected, so a dashboard resend with a fresh
event id still dedups) → inbox row keyed by the **payment id** (D7) → the same
``billing_process_webhook`` job, so the fast-lane routing, 60s pending sweep,
dead-letter, and purge all apply unchanged.

The Stripe runtime is fully decommissioned (T-308): a Stripe-shaped request (one
carrying a ``Stripe-Signature`` header) is rejected with
``{"status": "ignored_provider_disabled"}`` before any body read, signature
claim, or DB write. The ``stripe_credit_packs`` / ``stripe_webhook_events`` audit
tables and their backfilled rows are retained, but no runtime path reads or writes
them.

Phase 22 — T-296 (Lemon-only checkout flow + ``checkout_ref`` polling),
           T-297 (durable Lemon webhook ingestion → sanitised inbox → enqueue),
           T-308 (Stripe runtime decommission — grace adapter removed).
Issue #44 — payment feature flags + Razorpay provider dispatch and webhook.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, NamedTuple

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database import get_db
from middleware.auth import get_current_user
from models import (
    BillingAdminCorrection,
    BillingCheckoutAttempt,
    BillingCreditPack,
    BillingWebhookEvent,
    User,
)
from schemas.billing import (
    AdminCorrectionRequest,
    AdminCorrectionResponse,
    BillingStatusResponse,
    CheckoutResponse,
    PackageResponse,
    PackHistoryItem,
)
from services.credit_service import credit_service
from services.lemonsqueezy_service import LemonSqueezyError, lemonsqueezy_service
from services.observability import (
    BILLING_ADMIN_CORRECTION,
    BILLING_CHECKOUT_API_ERROR,
    BILLING_CHECKOUT_CREATED,
    BILLING_CREDIT_DEBT_RECOVERED,
    BILLING_WEBHOOK_DUPLICATE,
    BILLING_WEBHOOK_RECEIVED,
)
from services.queue import enqueue
from services.razorpay_service import (
    RazorpayError,
    razorpay_environment,
    razorpay_service,
)

logger = logging.getLogger(__name__)

# High-entropy byte budget for the polling ref and the one-time nonce. 32 bytes
# (256 bits) of os.urandom via secrets — non-sequential, non-guessable (SR4/#10).
_REF_ENTROPY_BYTES = 32

# Lemon webhook (T-297). Only order events are actionable (grant / reverse); the
# worker dispatches on ``event_name``. Other verified events are acknowledged 200
# without an inbox row so the inbox is not bloated with non-actionable traffic.
_LEMON_ORDER_EVENTS = frozenset({"order_created", "order_refunded"})
_LEMON_OBJECT_TYPE = "orders"

# Razorpay webhook (issue #44 Plan §5). Grant authority is ``payment_link.paid``
# (its payload carries BOTH the link entity — with our notes/nonce — and the
# payment entity, D4); reversal is ``refund.processed``. Everything else
# (``payment.captured``, ``payment.failed``, ``payment_link.expired``, dispute
# events subscribed for log-level visibility, …) is acknowledged without an
# inbox row. The inbox object is the **payment** (``pay_…``) — the money object
# refunds reference (D7) — hence object type "payments".
_RAZORPAY_DISPUTE_EVENTS = frozenset(
    f"payment.dispute.{state}"
    for state in ("created", "under_review", "action_required", "lost", "won", "closed")
)
_RAZORPAY_ACTIONABLE_EVENTS = (
    frozenset({"payment_link.paid", "refund.processed"}) | _RAZORPAY_DISPUTE_EVENTS
)
_RAZORPAY_OBJECT_TYPE = "payments"

router = APIRouter(prefix="/billing", tags=["billing"])


class _ProviderEconomics(NamedTuple):
    """The active gateway's package economics, snapshot into checkout attempts."""

    credits: int
    price_cents: int
    currency: str
    validity_days: int
    checkout_ttl_minutes: int


def _economics_for(provider: str) -> _ProviderEconomics:
    """The package economics for ``provider`` (issue #44).

    Unknown values (and the retained ``stripe`` audit provider) fall back to the
    Lemon block: display stays sane while ``billing_checkout_enabled`` fails
    closed, so nothing can be purchased against a misconfigured selector (and
    ``validate_production_settings`` refuses to boot prod on a typo).
    """
    if provider == "razorpay":
        return _ProviderEconomics(
            credits=settings.razorpay_credits_per_purchase,
            price_cents=settings.razorpay_price_cents,
            currency=settings.razorpay_currency,
            validity_days=settings.razorpay_credit_validity_days,
            checkout_ttl_minutes=settings.razorpay_checkout_ttl_minutes,
        )
    return _ProviderEconomics(
        credits=settings.lemonsqueezy_credits_per_purchase,
        price_cents=settings.lemonsqueezy_price_cents,
        currency=settings.lemonsqueezy_currency,
        validity_days=settings.lemonsqueezy_credit_validity_days,
        checkout_ttl_minutes=settings.lemonsqueezy_checkout_ttl_minutes,
    )


def _credit_validity_days_for(provider: str) -> int:
    """Provider-aware pack validity for the admin-correction path (issue #44)."""
    return _economics_for(provider).validity_days


def _checkout_service_for(
    provider: str,
) -> Callable[..., Awaitable[tuple[str, str]]]:
    """The hosted-checkout factory for ``provider`` (D6 — light dispatch).

    Both callables share the ``(attempt, user, *, checkout_nonce) ->
    (provider_checkout_id, checkout_url)`` contract, so the attempt-first flow
    around them is provider-agnostic. Resolved at call time from the module
    singletons (tests monkeypatch the singleton attributes). Only reachable for
    a known provider — ``billing_checkout_enabled`` fails closed on anything
    else before dispatch.
    """
    if provider == "razorpay":
        return razorpay_service.create_payment_link
    return lemonsqueezy_service.create_checkout


async def require_admin(
    current_user: User = Depends(get_current_user),
) -> User:
    """Authorise the billing-admin allowlist, else 403 (T-302).

    The ``admin_user_emails`` allowlist (``settings.admin_emails``, lower-cased) is
    the **only** authorization surface — the ``User`` model has no role column. An
    empty allowlist authorises no one, so the admin-correction support path is closed
    by default and there is no implicit admin.
    """
    if current_user.email.lower() not in settings.admin_emails:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin authorization required",
        )
    return current_user


@router.get("/package", response_model=PackageResponse)
async def get_package() -> PackageResponse:
    """Return the active provider's credit-package configuration (issue #44).

    No authentication required — the pricing page calls this before login. Values
    come from ``config.Settings`` at request time, so changing the env vars and
    restarting is enough to update the displayed price without a code change.

    ``enabled`` mirrors ``settings.billing_checkout_enabled`` — the frontend
    gates the Buy button on it (a disabled server would otherwise 503 on click).
    The configured numbers are returned either way so the package card renders.
    """
    provider = settings.payment_provider
    econ = _economics_for(provider)
    return PackageResponse(
        credits=econ.credits,
        price_cents=econ.price_cents,
        validity_days=econ.validity_days,
        currency=econ.currency,
        enabled=settings.billing_checkout_enabled,
        provider=provider,
    )


@router.post(
    "/checkout",
    response_model=CheckoutResponse,
    status_code=status.HTTP_200_OK,
)
async def create_checkout(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CheckoutResponse:
    """Create the active provider's hosted checkout via the attempt-first flow.

    1. Mint a high-entropy ``checkout_ref`` and a **separate** one-time
       ``checkout_nonce``; persist only ``sha256(checkout_nonce)``.
    2. **Commit** the ``billing_checkout_attempts`` row (``status='created'``)
       snapshotting the **active provider's** ``credits/price_cents/currency/
       validity_days`` from config — Thought2Build is the authority for the attempt
       before any provider call.
    3. Dispatch to the active provider (D6): Lemon ``create_checkout`` or
       Razorpay ``create_payment_link`` (the charged amount is the attempt's
       ``price_cents`` snapshot, immune to in-flight config changes).
    4. **Commit** the ``provider_created`` transition with ``provider_checkout_id``.
       Only after that commit is the ``checkout_url`` returned.

    Returns 503 when checkout is off (``payments_enabled`` false, or the active
    provider unconfigured/unknown — issue #44); 502 when the provider fails (the
    attempt is marked ``failed``) or when the post-provider commit fails
    (orphaned — the URL is never exposed; the reconcile lane settles the order
    later).

    Rate limited: 5 checkouts per user per hour (``RateLimitMiddleware``).
    """
    if not settings.billing_checkout_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Billing is not configured on this server",
        )
    provider = settings.payment_provider
    econ = _economics_for(provider)

    # 1. High-entropy, non-sequential identifiers. The ref is the client polling
    #    key (returned + stored plaintext); the nonce is a secret proven back only
    #    by the signed webhook — only its sha256 is ever persisted (SR4).
    checkout_ref = secrets.token_urlsafe(_REF_ENTROPY_BYTES)
    checkout_nonce = secrets.token_urlsafe(_REF_ENTROPY_BYTES)
    checkout_nonce_hash = hashlib.sha256(checkout_nonce.encode("utf-8")).hexdigest()

    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=econ.checkout_ttl_minutes)

    # 2. Commit the attempt BEFORE the provider call — the local row is the
    #    authority.
    attempt = BillingCheckoutAttempt(
        checkout_ref=checkout_ref,
        user_id=current_user.id,
        provider=provider,
        checkout_nonce_hash=checkout_nonce_hash,
        # Economics snapshot — the grant (T-299) validates against THESE values,
        # not live config, so an in-flight price change is safe.
        credits=econ.credits,
        price_cents=econ.price_cents,
        currency=econ.currency,
        validity_days=econ.validity_days,
        status="created",
        expires_at=expires_at,
    )
    db.add(attempt)
    await db.commit()
    await db.refresh(attempt)

    # 3. Mint the hosted checkout. On any failure mark the attempt failed (so the
    #    reconcile/purge lanes treat it as terminal) and return a safe 502.
    try:
        provider_checkout_id, checkout_url = await _checkout_service_for(provider)(
            attempt,
            current_user,
            checkout_nonce=checkout_nonce,
        )
    except (LemonSqueezyError, RazorpayError) as exc:
        attempt.status = "failed"
        await db.commit()
        BILLING_CHECKOUT_API_ERROR.labels(
            provider=provider, error_type="provider_error"
        ).inc()
        logger.error(
            "billing.checkout_provider_failed checkout_ref=%s attempt_id=%s user_id=%s",
            checkout_ref,
            str(attempt.id),
            str(current_user.id),
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to create checkout. Please try again.",
        ) from exc

    # 4. Commit the provider_created transition. If THIS commit fails the checkout
    #    exists at the provider but Thought2Build could not record it — never expose
    #    the URL (the client would pay against an attempt we cannot poll); the
    #    order will be reconciled from the signed webhook. Emit
    #    billing.checkout.orphaned.
    attempt.provider_checkout_id = provider_checkout_id
    attempt.status = "provider_created"
    try:
        await db.commit()
    except SQLAlchemyError as exc:
        await db.rollback()
        BILLING_CHECKOUT_API_ERROR.labels(
            provider=provider, error_type="orphaned_commit"
        ).inc()
        logger.error(
            "billing.checkout.orphaned checkout_ref=%s attempt_id=%s "
            "provider_checkout_id=%s user_id=%s",
            checkout_ref,
            str(attempt.id),
            provider_checkout_id,
            str(current_user.id),
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to create checkout. Please try again.",
        ) from exc

    BILLING_CHECKOUT_CREATED.labels(provider=provider).inc()
    return CheckoutResponse(checkout_url=checkout_url, checkout_ref=checkout_ref)


@router.get("/status", response_model=BillingStatusResponse)
async def get_billing_status(
    checkout_ref: str | None = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> BillingStatusResponse:
    """Poll a checkout by ``checkout_ref`` (IDOR-safe); 200 only when granted.

    The attempt is fetched by a single query scoped by BOTH ``checkout_ref`` and
    ``user_id`` — a mismatch on either returns 404 (never 403; 403 would confirm
    the ref exists for another user). 200 is returned only when the attempt is
    ``completed`` AND the granted ``billing_credit_packs`` row exists; everything
    else (unknown / not-yet-granted / expired / failed) is 404 (no
    resource-existence leak, SR6).

    The legacy Stripe ``session_id`` polling path was removed with the Stripe
    decommission (T-308); only ``checkout_ref`` is accepted.
    """
    if checkout_ref is not None:
        return await _status_by_checkout_ref(db, current_user, checkout_ref)

    # No usable identifier — reveal nothing.
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Checkout not found",
    )


async def _status_by_checkout_ref(
    db: AsyncSession,
    current_user: User,
    checkout_ref: str,
) -> BillingStatusResponse:
    """The ``checkout_ref`` polling path: attempt → grant lookup."""
    # Single double-predicate query — both checkout_ref AND user_id required.
    attempt = await db.scalar(
        select(BillingCheckoutAttempt).where(
            BillingCheckoutAttempt.checkout_ref == checkout_ref,
            BillingCheckoutAttempt.user_id == current_user.id,
        )
    )
    if attempt is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Checkout not found",
        )

    # Telemetry only — stamp the first browser return on the success redirect.
    # Does not affect the 200/404 decision.
    if attempt.success_redirect_seen_at is None:
        attempt.success_redirect_seen_at = datetime.now(timezone.utc)
        await db.commit()

    if attempt.status != "completed" or attempt.provider_order_id is None:
        # Not yet granted (the webhook stamps status='completed' + provider_order_id
        # and writes the pack atomically, T-299), expired, or failed.
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Checkout not found",
        )

    # Contract with T-299: the granted pack is linked to the attempt by
    # (provider, provider_order_id) — the ATTEMPT's provider, not a hardcoded
    # one, so a Razorpay attempt resolves its Razorpay pack (issue #44).
    # provider_order_id is non-None here (guarded above) so this never
    # degenerates to an IS NULL match on a NULL-keyed pack.
    pack = await db.scalar(
        select(BillingCreditPack).where(
            BillingCreditPack.user_id == current_user.id,
            BillingCreditPack.provider == attempt.provider,
            BillingCreditPack.provider_order_id == attempt.provider_order_id,
        )
    )
    if pack is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Checkout not found",
        )

    await credit_service.get_balance(db, current_user.id)
    await db.commit()
    await credit_service.invalidate(current_user.id)
    await db.refresh(pack)
    settlement_status = pack.status
    if pack.refunded_item_amount_cents_processed and pack.status not in (
        "refunded",
        "disputed",
        "expired",
    ):
        settlement_status = "partially_refunded"
    return BillingStatusResponse(
        status="completed",
        credits_added=pack.credits_remaining,
        credits_purchased=pack.credits_purchased,
        debt_recovered=pack.credits_debt_recovered,
        credits_revoked=pack.credits_revoked,
        settlement_status=settlement_status,
        expires_at=pack.expires_at,
    )


@router.get("/history", response_model=list[PackHistoryItem])
async def get_billing_history(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[PackHistoryItem]:
    """Return the user's credit-pack purchase history, newest first (max 50).

    Sourced from ``billing_credit_packs`` (the provider-neutral pack table), not
    the retained ``StripeCreditPack`` table. Includes every status so users see a
    complete audit trail of their purchases.
    """
    await credit_service.get_balance(db, current_user.id)
    await db.commit()
    await credit_service.invalidate(current_user.id)
    result = await db.execute(
        select(BillingCreditPack)
        .where(BillingCreditPack.user_id == current_user.id)
        .order_by(BillingCreditPack.purchased_at.desc())
        .limit(50)
    )
    packs = result.scalars().all()
    return [PackHistoryItem.model_validate(p) for p in packs]


@router.post(
    "/admin/correction",
    response_model=AdminCorrectionResponse,
    status_code=status.HTTP_200_OK,
)
async def admin_correction(
    body: AdminCorrectionRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> AdminCorrectionResponse:
    """Exceptional, evidence-backed manual credit grant (T-302).

    The support path for a provably-paid order whose first-purchase webhook never
    arrived with valid proof. Authorised only by ``require_admin`` (the
    ``admin_user_emails`` allowlist); also requires auth + CSRF and is rate-limited
    (10/admin/hour, ``RateLimitMiddleware``).

    Idempotent on ``(provider, provider_order_id)``: if a ``BillingCreditPack`` or a
    prior ``billing_admin_corrections`` row already exists for the order, this is a
    no-op (``applied=False``) — never a second grant. Otherwise, in one transaction,
    it creates the pack, runs ``grant_credits_with_debt_recovery`` (so the corrected
    credits repay pending debt before any usable surplus — debt recovery is never
    bypassed) under the ``admin_billing_correction:{provider}:{order_id}`` ledger
    reason, and writes the immutable ``billing_admin_corrections`` audit row. The
    pack/audit ``(provider, order_id)`` unique indexes and the ledger-reason index are
    a triple idempotency barrier against a concurrent double-submit.
    """
    provider = body.provider
    order_id = body.provider_order_id
    attempt = None
    verified_payment = None
    if provider == "razorpay":
        if not body.checkout_ref:
            raise HTTPException(
                422, "Razorpay correction requires the original checkout_ref"
            )
        from services.billing_worker import _razorpay_link_paid_rejection
        from services.razorpay_settlement import lock_payment

        await lock_payment(db, order_id)
        attempt = await db.scalar(
            select(BillingCheckoutAttempt)
            .where(
                BillingCheckoutAttempt.checkout_ref == body.checkout_ref,
                BillingCheckoutAttempt.user_id == body.target_user_id,
                BillingCheckoutAttempt.provider == provider,
            )
            .with_for_update()
        )
        if attempt is None:
            raise HTTPException(404, "Checkout not found")
        try:
            verified_payment = await razorpay_service.get_paid_checkout(attempt)
        except (RazorpayError, HTTPException) as exc:
            raise HTTPException(502, "Unable to verify the Razorpay payment") from exc
        if (
            verified_payment is None
            or verified_payment["payment_id"] != order_id
            or _razorpay_link_paid_rejection(
                verified_payment, verified_payment["notes"], attempt
            )
            or (body.credits, body.price_cents, body.currency)
            != (attempt.credits, attempt.price_cents, attempt.currency)
        ):
            raise HTTPException(409, "Payment does not match the original checkout")

    # Idempotency pre-check: a pack OR a prior correction for this exact order.
    existing_pack = await db.scalar(
        select(BillingCreditPack.id).where(
            BillingCreditPack.provider == provider,
            BillingCreditPack.provider_order_id == order_id,
        )
    )
    existing_correction = await db.scalar(
        select(BillingAdminCorrection.id).where(
            BillingAdminCorrection.provider == provider,
            BillingAdminCorrection.provider_order_id == order_id,
        )
    )
    if existing_pack is not None or existing_correction is not None:
        if attempt is not None and existing_pack is not None:
            pack_owner = await db.scalar(
                select(BillingCreditPack.user_id).where(
                    BillingCreditPack.id == existing_pack
                )
            )
            if pack_owner != attempt.user_id:
                raise HTTPException(409, "Payment already belongs to another account")
            attempt.status = "completed"
            attempt.provider_order_id = order_id
            attempt.completed_at = attempt.completed_at or datetime.now(timezone.utc)
            await db.commit()
        logger.info(
            "billing.admin_correction.noop provider=%s order_id=%s admin_user_id=%s",
            provider,
            order_id,
            str(admin.id),
        )
        return AdminCorrectionResponse(
            applied=False,
            provider=provider,
            provider_order_id=order_id,
            credits_granted=0,
        )

    target = await db.scalar(select(User).where(User.id == body.target_user_id))
    if target is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Target user not found",
        )

    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(days=_credit_validity_days_for(provider))
    purchased_at = now
    if attempt is not None:
        captured_at = (verified_payment.get("payment") or {}).get("created_at")
        if captured_at:
            purchased_at = datetime.fromtimestamp(int(captured_at), tz=timezone.utc)
        expires_at = purchased_at + timedelta(days=attempt.validity_days)
    pack = BillingCreditPack(
        user_id=body.target_user_id,
        provider=provider,
        provider_order_id=order_id,
        provider_checkout_id=(
            attempt.provider_checkout_id if attempt is not None else None
        ),
        credits_purchased=body.credits,
        credits_remaining=body.credits,
        price_cents=body.price_cents,
        currency=body.currency,
        paid_item_amount_cents=body.price_cents,
        provider_order_total_cents=body.price_cents,
        status="active",
        purchased_at=purchased_at,
        expires_at=expires_at,
    )
    db.add(pack)

    try:
        # flush surfaces a (provider, provider_order_id) pack-uniqueness conflict
        # (a racing duplicate that committed first) before we grant.
        await db.flush()
        granted = await credit_service.grant_credits_with_debt_recovery(
            db,
            user_id=body.target_user_id,
            pack=pack,
            granted_credits=body.credits,
            ledger_reason=f"admin_billing_correction:{provider}:{order_id}",
        )
    except IntegrityError:
        await db.rollback()
        logger.info(
            "billing.admin_correction.duplicate_conflict provider=%s order_id=%s",
            provider,
            order_id,
        )
        return AdminCorrectionResponse(
            applied=False,
            provider=provider,
            provider_order_id=order_id,
            credits_granted=0,
        )

    if granted is None:
        # The admin_billing_correction:% ledger index rejected a duplicate; grant()'s
        # SAVEPOINT rolled back its rows but the flushed pack is still pending — never
        # commit it (the T-299 half-state landmine). Roll back to an idempotent no-op.
        await db.rollback()
        logger.info(
            "billing.admin_correction.duplicate_ledger provider=%s order_id=%s",
            provider,
            order_id,
        )
        return AdminCorrectionResponse(
            applied=False,
            provider=provider,
            provider_order_id=order_id,
            credits_granted=0,
        )

    db.add(
        BillingAdminCorrection(
            admin_user_id=admin.id,
            target_user_id=body.target_user_id,
            billing_credit_pack_id=pack.id,
            provider=provider,
            provider_order_id=order_id,
            credits=body.credits,
            price_cents=body.price_cents,
            currency=body.currency,
            reason=body.reason,
            evidence_url=str(body.evidence_url),
        )
    )
    if attempt is not None:
        from services.razorpay_settlement import known_refund_cents, settle_pack

        attempt.status = "completed"
        attempt.provider_order_id = order_id
        attempt.completed_at = now
        await db.flush()
        settlement = await settle_pack(
            db,
            pack,
            max(
                int(
                    (verified_payment.get("payment") or {}).get("amount_refunded") or 0
                ),
                await known_refund_cents(db, order_id),
            ),
        )
    try:
        await db.commit()
    except IntegrityError:
        # The audit-row (provider, order_id) unique index rejected a concurrent
        # duplicate that committed between our pre-check and here. Nothing of ours
        # persists — idempotent no-op.
        await db.rollback()
        logger.info(
            "billing.admin_correction.duplicate_audit provider=%s order_id=%s",
            provider,
            order_id,
        )
        return AdminCorrectionResponse(
            applied=False,
            provider=provider,
            provider_order_id=order_id,
            credits_granted=0,
        )

    await credit_service.invalidate(body.target_user_id)
    if attempt is not None:
        settlement.record()
    BILLING_ADMIN_CORRECTION.labels(provider=provider).inc()
    # Debt repaid out of this correction (granted − surplus). Emitted post-commit so
    # a failed outer commit can never leave an over-counted recovery metric.
    recovered = body.credits - granted.amount
    if recovered > 0:
        BILLING_CREDIT_DEBT_RECOVERED.labels(provider=provider).inc(recovered)
    logger.info(
        "billing.admin_correction.applied provider=%s order_id=%s admin_user_id=%s "
        "target_user_id=%s credits=%d pack_id=%s",
        provider,
        order_id,
        str(admin.id),
        str(body.target_user_id),
        body.credits,
        str(pack.id),
    )
    return AdminCorrectionResponse(
        applied=True,
        provider=provider,
        provider_order_id=order_id,
        credits_granted=body.credits,
    )


@router.post("/webhook", status_code=status.HTTP_200_OK)
async def lemon_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Lemon Squeezy webhook receiver — verify, sanitise, persist, enqueue (T-297).

    The durable trust boundary. It performs **no** money mutation inline:

    1. Read the raw body BEFORE any parse — the HMAC covers the exact bytes.
    2. Verify ``X-Signature`` = HMAC-SHA256(raw, secret), constant-time, against
       every secret in ``lemonsqueezy_webhook_secrets`` (current + previous, for
       the rotation window). Missing / malformed / non-matching → 400.
    3. Parse JSON only after verification. Require ``X-Event-Name`` ==
       ``meta.event_name`` and, for order events, ``data.type == "orders"`` and a
       matching ``test_mode`` — else 400. Non-order events are acknowledged 200.
    4. ``order_created`` requires ``custom_data.checkout_nonce``: hash it to
       ``checkout_nonce_hash_from_webhook`` and DROP the raw nonce. ``order_refunded``
       hashes it only if present (a signed refund is never rejected for a missing
       nonce). Build an allow-listed ``normalized_payload`` — no PII, no URLs, no
       signature, no raw nonce, no unknown custom fields — and **commit** the
       ``billing_webhook_events`` inbox row. A duplicate 4-part identity raises
       ``IntegrityError`` (SAVEPOINT) → ``BILLING_WEBHOOK_DUPLICATE`` → 200.
    5. Enqueue ``billing_process_webhook`` by the inbox row id with the
       deterministic ``billing_wh:{id}`` job id (dedups against the wrapper-retry
       and the pending-sweep). If the enqueue fails the 60s sweep (T-298) recovers
       it, so still return 200. Raw bytes never reach arq or the DB.

    No ``get_current_user`` dependency — the provider has no browser session. The
    endpoint is CSRF- and rate-limit-exempt (``middleware/csrf.py`` /
    ``middleware/rate_limit.py``).
    """
    logger.debug("billing.webhook_received", extra={"method": request.method})

    # Stripe decommissioned (T-308). A Stripe webhook carries a ``Stripe-Signature``
    # header (Lemon uses ``X-Signature``), so the shape is unambiguous. The grace
    # window is closed: reject before any body read, signature claim, or DB write.
    # No Stripe signature-verification is claimed and nothing is persisted.
    if request.headers.get("Stripe-Signature") is not None:
        logger.info("billing.stripe.provider_disabled")
        return {"status": "ignored_provider_disabled"}

    # Step 1: raw bytes BEFORE any parse — the HMAC is computed over these exact
    # bytes; ``await request.json()`` would re-serialise and break verification.
    raw = await request.body()
    signature = request.headers.get("X-Signature", "")

    # Step 2: constant-time HMAC over the two-secret rotation list. Fail closed.
    if not _verify_lemon_signature(raw, signature):
        logger.warning("billing.webhook_invalid_signature")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid signature",
        )

    # Step 3: parse only after the signature is proven.
    try:
        payload = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Malformed webhook payload",
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Malformed webhook payload",
        )

    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    event_name = meta.get("event_name")
    header_event_name = request.headers.get("X-Event-Name", "")

    # X-Event-Name header must match the signed body's meta.event_name.
    if not event_name or header_event_name != event_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Event name mismatch",
        )

    # Only order events are actionable; acknowledge anything else without storing.
    if event_name not in _LEMON_ORDER_EVENTS:
        logger.info("billing.webhook_ignored_event event_name=%s", event_name)
        return {"status": "ignored"}

    if data.get("type") != _LEMON_OBJECT_TYPE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unexpected object type",
        )

    attributes = (
        data.get("attributes") if isinstance(data.get("attributes"), dict) else {}
    )

    # test/live guard — a test-store event must never settle against live config.
    test_mode = attributes.get("test_mode")
    if test_mode is not None and bool(test_mode) != settings.lemonsqueezy_test_mode:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Webhook test mode does not match server configuration",
        )

    order_id = data.get("id")
    if not order_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing order id",
        )
    order_id = str(order_id)

    custom_data = (
        meta.get("custom_data") if isinstance(meta.get("custom_data"), dict) else {}
    )

    # Step 4: nonce → hash, then DROP the raw nonce (it never enters the payload).
    raw_nonce = custom_data.get("checkout_nonce")
    if event_name == "order_created" and not raw_nonce:
        # order_created is the grant authority — the nonce is required to prove the
        # checkout attempt. (A signed refund, by contrast, is processed regardless.)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing checkout nonce",
        )
    nonce_hash = (
        hashlib.sha256(str(raw_nonce).encode("utf-8")).hexdigest()
        if raw_nonce
        else None
    )

    normalized_payload = _build_normalized_payload(
        event_name=event_name,
        order_id=order_id,
        attributes=attributes,
        custom_data=custom_data,
        nonce_hash=nonce_hash,
    )
    payload_hash = _payload_hash(normalized_payload)

    # Commit the sanitised inbox row. The 4-part identity
    # (provider, event_name, provider_object_id, payload_hash) is UNIQUE, so a
    # byte-identical redelivery raises IntegrityError → dedup → 200.
    row = BillingWebhookEvent(
        provider="lemonsqueezy",
        event_name=event_name,
        provider_object_type=_LEMON_OBJECT_TYPE,
        provider_object_id=order_id,
        payload_hash=payload_hash,
        status="received",
        normalized_payload=normalized_payload,
    )
    try:
        async with db.begin_nested():  # SAVEPOINT isolates the unique-violation
            db.add(row)
            await db.flush()  # populates row.id and trips the unique index
    except IntegrityError:
        await db.rollback()
        logger.info(
            "billing.webhook_duplicate event_name=%s order_id=%s",
            event_name,
            order_id,
        )
        BILLING_WEBHOOK_DUPLICATE.labels(provider="lemonsqueezy").inc()
        return {"status": "already_processed"}

    await db.commit()
    BILLING_WEBHOOK_RECEIVED.labels(
        provider="lemonsqueezy", event_type=event_name
    ).inc()

    # Step 5: enqueue by row id. Never pass raw bytes through arq. If the enqueue
    # fails (Redis blip), the row is durably 'received' and the 60s pending sweep
    # (T-298) re-enqueues it — so acknowledge 200 regardless.
    webhook_event_id = str(row.id)
    try:
        await enqueue(
            "billing_process_webhook",
            webhook_event_id,
            job_id=f"billing_wh:{webhook_event_id}",
        )
    except Exception:
        logger.error(
            "billing.webhook_enqueue_failed webhook_event_id=%s", webhook_event_id
        )

    return {"status": "ok"}


def _verify_lemon_signature(raw: bytes, signature: str) -> bool:
    """Constant-time HMAC-SHA256 verification over the two-secret rotation list.

    Returns True only when ``signature`` (the hex ``X-Signature`` header) matches
    the HMAC of ``raw`` under one of the configured webhook secrets. Fails closed:
    an empty header or an empty secret list (no secret configured) returns False,
    so a misconfigured server rejects rather than accepts forged deliveries.
    """
    if not signature:
        return False
    matched = False
    for secret in settings.lemonsqueezy_webhook_secrets:
        if not secret:
            continue
        expected = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
        # Compare every secret (no short-circuit) — constant-time per comparison.
        if hmac.compare_digest(expected, signature):
            matched = True
    return matched


def _build_normalized_payload(
    *,
    event_name: str,
    order_id: str,
    attributes: dict[str, Any],
    custom_data: dict[str, Any],
    nonce_hash: str | None,
) -> dict[str, Any]:
    """Build the allow-listed, PII-free sanitised inbox payload (Plan §25.6 T-297).

    This is the **only** thing the worker (T-298/T-299/T-300) reads, so it is the
    contract those tasks consume. It is an explicit allow-list: every field is
    copied by name, so provider PII (``user_email``/``user_name``), URLs
    (``urls.receipt``), the signature, the API key, the raw nonce, and any
    unrecognised ``custom_data`` field are inherently excluded. The custom block
    is exactly the seven keys Thought2Build itself set on the checkout.
    """
    first_item = (
        attributes.get("first_order_item")
        if isinstance(attributes.get("first_order_item"), dict)
        else {}
    )
    return {
        "provider": "lemonsqueezy",
        "event_name": event_name,
        "order_id": order_id,
        "store_id": attributes.get("store_id"),
        "customer_id": attributes.get("customer_id"),
        "order_item_id": first_item.get("id"),
        "product_id": first_item.get("product_id"),
        "variant_id": first_item.get("variant_id"),
        "status": attributes.get("status"),
        "currency": attributes.get("currency"),
        "test_mode": attributes.get("test_mode"),
        # Economics (cents). order_total/subtotal + refunded_amount drive the
        # proportional, tax-normalised reversal in T-300; item price anchors the
        # grant validation in T-299.
        "item_price_cents": first_item.get("price"),
        "order_subtotal_cents": attributes.get("subtotal"),
        "order_total_cents": attributes.get("total"),
        "discount_total_cents": attributes.get("discount_total"),
        "refunded": attributes.get("refunded"),
        "refunded_amount_cents": attributes.get("refunded_amount"),
        "created_at": attributes.get("created_at"),
        "updated_at": attributes.get("updated_at"),
        # Thought2Build-set custom data — exactly the allow-listed keys. The raw nonce
        # is replaced by its sha256; everything else the provider echoed is dropped.
        "custom": {
            "user_id": custom_data.get("user_id"),
            "checkout_ref": custom_data.get("checkout_ref"),
            "checkout_nonce_hash_from_webhook": nonce_hash,
            "environment": custom_data.get("environment"),
            "credits": custom_data.get("credits"),
            "price_cents": custom_data.get("price_cents"),
            "currency": custom_data.get("currency"),
        },
    }


def _payload_hash(normalized_payload: dict[str, Any]) -> str:
    """sha256 over the canonical (sorted-key) JSON of the sanitised payload.

    Part of the inbox dedup identity: a byte-identical provider redelivery yields
    the same hash and trips the unique index.
    """
    canonical = json.dumps(normalized_payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Razorpay webhook (issue #44 Plan §5)
# ---------------------------------------------------------------------------


@router.post("/webhook/razorpay", status_code=status.HTTP_200_OK)
async def razorpay_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Razorpay webhook receiver — verify, sanitise, persist, enqueue (issue #44).

    The Razorpay counterpart of ``lemon_webhook``, same five-step
    verify-before-work shape; it performs **no** money mutation inline:

    1. Read the raw body BEFORE any parse — the HMAC covers the exact bytes.
    2. Verify ``X-Razorpay-Signature`` = HMAC-SHA256(raw, secret), constant-time,
       against every secret in ``razorpay_webhook_secrets`` (current + previous,
       for the rotation window). Missing / malformed / non-matching → 400; no
       secret configured fails closed (D3 — an unconfigured provider's route
       rejects everything).
    3. Parse JSON only after verification; dispatch on the top-level ``event``.
       Only ``payment_link.paid`` (grant authority, D4) and ``refund.processed``
       (reversal) are actionable; anything else — ``payment.captured``,
       ``payment.failed``, ``payment_link.expired``, dispute events — is
       acknowledged 200 without an inbox row.
    4. Build the allow-listed ``normalized_payload`` (no PII — the payment
       entity's ``email``/``contact`` are dropped; no raw nonce — sha256 only; no
       signature). ``payment_link.paid`` **requires** ``notes.checkout_nonce``
       and a matching ``notes.environment`` (Razorpay events carry no test-mode
       flag; the round-tripped environment marker is the test/live guard) — the
       grant authority requires proof. A signed refund is never rejected for
       missing notes (same asymmetry as Lemon); its environment is enforced only
       when present.
    5. **Commit** the inbox row keyed by the **payment id** (D7 — the money
       object refunds reference). ``payload_hash`` is computed BEFORE the
       ``x-razorpay-event-id`` header is injected into the stored payload, so a
       manual dashboard resend (which mints a fresh event id) still dedups. Then
       enqueue ``billing_process_webhook`` by row id — the same job as Lemon, so
       fast-lane routing, the 60s sweep, dead-letter, and purge apply unchanged.
       If the enqueue fails the sweep recovers it, so still return 200.

    No ``get_current_user`` dependency — the provider has no browser session. The
    endpoint is CSRF- and rate-limit-exempt (``middleware/csrf.py`` /
    ``middleware/rate_limit.py``). It processes independently of the
    ``payments_enabled``/``payment_provider`` flags (D3): refunds for old
    Razorpay orders must still settle after a switch back to Lemon.
    """
    logger.debug("billing.razorpay_webhook_received", extra={"method": request.method})

    # Step 1: raw bytes BEFORE any parse — the HMAC is computed over these exact
    # bytes; ``await request.json()`` would re-serialise and break verification.
    raw = await request.body()
    signature = request.headers.get("X-Razorpay-Signature", "")

    # Step 2: constant-time HMAC over the two-secret rotation list. Fail closed.
    if not _verify_razorpay_signature(raw, signature):
        logger.warning("billing.razorpay_webhook_invalid_signature")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid signature",
        )

    # Step 3: parse only after the signature is proven.
    try:
        payload = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Malformed webhook payload",
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Malformed webhook payload",
        )

    event_name = payload.get("event")
    if not event_name or not isinstance(event_name, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing event name",
        )

    # Only the grant/reversal events are actionable; acknowledge anything else
    # without storing (payment.captured, payment.failed, payment_link.expired,
    # payment.dispute.* subscribed for log-level visibility, …).
    if event_name not in _RAZORPAY_ACTIONABLE_EVENTS:
        logger.info("billing.webhook_ignored_event event_name=%s", event_name)
        return {"status": "ignored"}

    entities = (
        payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    )

    # Step 4: allow-listed, PII-free normalisation (raises 400 on contract
    # violations — missing payment id, missing nonce / environment mismatch on
    # the grant authority).
    if event_name == "payment_link.paid":
        normalized_payload = _normalize_razorpay_link_paid(entities)
    elif event_name == "refund.processed":
        normalized_payload = _normalize_razorpay_refund(entities)
    else:
        normalized_payload = _normalize_razorpay_dispute(
            entities, event_name, payload.get("created_at")
        )

    payment_id = normalized_payload["payment_id"]

    # The dedup identity hashes the sanitised payload WITHOUT the delivery event
    # id: automatic retries keep the id stable, but a manual dashboard resend can
    # mint a new one, and that must not defeat the inbox unique index. The id is
    # then carried inside the stored payload as belt-and-braces dedup evidence
    # for ops.
    payload_hash = _payload_hash(normalized_payload)
    normalized_payload["event_id"] = request.headers.get("x-razorpay-event-id")

    # Step 5: commit the sanitised inbox row. The 4-part identity
    # (provider, event_name, provider_object_id, payload_hash) is UNIQUE, so a
    # byte-identical redelivery raises IntegrityError → dedup → 200.
    row = BillingWebhookEvent(
        provider="razorpay",
        event_name=event_name,
        provider_object_type=_RAZORPAY_OBJECT_TYPE,
        provider_object_id=payment_id,
        payload_hash=payload_hash,
        status="received",
        normalized_payload=normalized_payload,
    )
    try:
        async with db.begin_nested():  # SAVEPOINT isolates the unique-violation
            db.add(row)
            await db.flush()  # populates row.id and trips the unique index
    except IntegrityError:
        await db.rollback()
        logger.info(
            "billing.razorpay_webhook_duplicate event_name=%s payment_id=%s",
            event_name,
            payment_id,
        )
        BILLING_WEBHOOK_DUPLICATE.labels(provider="razorpay").inc()
        return {"status": "already_processed"}

    await db.commit()
    BILLING_WEBHOOK_RECEIVED.labels(provider="razorpay", event_type=event_name).inc()

    # Enqueue by row id. Never pass raw bytes through arq. If the enqueue fails
    # (Redis blip), the row is durably 'received' and the 60s pending sweep
    # (T-298) re-enqueues it — so acknowledge 200 regardless.
    webhook_event_id = str(row.id)
    try:
        await enqueue(
            "billing_process_webhook",
            webhook_event_id,
            job_id=f"billing_wh:{webhook_event_id}",
        )
    except Exception:
        logger.error(
            "billing.webhook_enqueue_failed webhook_event_id=%s", webhook_event_id
        )

    return {"status": "ok"}


def _verify_razorpay_signature(raw: bytes, signature: str) -> bool:
    """Constant-time HMAC-SHA256 verification over the two-secret rotation list.

    The Razorpay analogue of ``_verify_lemon_signature`` — the hex
    ``X-Razorpay-Signature`` header must match the HMAC of ``raw`` under one of
    the configured ``razorpay_webhook_secrets``. Fails closed: an empty header or
    an empty secret list returns False, so an unconfigured/misconfigured server
    rejects rather than accepts forged deliveries.
    """
    if not signature:
        return False
    matched = False
    for secret in settings.razorpay_webhook_secrets:
        if not secret:
            continue
        expected = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
        # Compare every secret (no short-circuit) — constant-time per comparison.
        if hmac.compare_digest(expected, signature):
            matched = True
    return matched


def _razorpay_entity(entities: dict[str, Any], key: str) -> dict[str, Any]:
    """Extract ``payload.{key}.entity`` defensively (empty dict when absent)."""
    block = entities.get(key) if isinstance(entities.get(key), dict) else {}
    entity = block.get("entity") if isinstance(block.get("entity"), dict) else {}
    return entity


def _razorpay_notes_block(
    notes: dict[str, Any], nonce_hash: str | None
) -> dict[str, Any]:
    """The seven allow-listed Thought2Build ``notes`` keys, raw nonce → sha256.

    The Razorpay analogue of the Lemon ``custom`` block: every field is copied by
    name, so anything else the provider echoed is inherently excluded, and the
    raw nonce is replaced by its hash (it never enters the stored payload).
    """
    return {
        "user_id": notes.get("user_id"),
        "checkout_ref": notes.get("checkout_ref"),
        "checkout_nonce_hash_from_webhook": nonce_hash,
        "environment": notes.get("environment"),
        "credits": notes.get("credits"),
        "price_cents": notes.get("price_cents"),
        "currency": notes.get("currency"),
    }


def _razorpay_payment_block(payment: dict[str, Any]) -> dict[str, Any]:
    """The allow-listed payment-entity fields (the money object, D7/D10).

    ``amount``/``currency``/``status`` anchor the grant validation (the payment
    is the authority, the link only corroboration); the cumulative
    ``amount_refunded`` and ``refund_status`` drive the proportional reversal.
    PII on the entity (``email``, ``contact``) is inherently excluded.
    """
    return {
        "payment_id": payment.get("id"),
        "amount": payment.get("amount"),
        "currency": payment.get("currency"),
        "status": payment.get("status"),
        "method": payment.get("method"),
        "amount_refunded": payment.get("amount_refunded"),
        "refund_status": payment.get("refund_status"),
        "created_at": payment.get("created_at"),
        "order_id": payment.get("order_id"),
    }


def _normalize_razorpay_link_paid(entities: dict[str, Any]) -> dict[str, Any]:
    """Sanitise a ``payment_link.paid`` delivery (the grant authority, D4).

    Requires the payment entity's id (the inbox identity and future
    ``provider_order_id``, D7), the link ``notes.checkout_nonce`` (proof of the
    Thought2Build checkout attempt — hashed, never stored raw), and a
    ``notes.environment`` matching this server's key environment (Razorpay
    events carry no test-mode flag, so the round-tripped marker is the test/live
    guard; a link Thought2Build minted always carries it — hard requirement here).
    """
    link = _razorpay_entity(entities, "payment_link")
    payment = _razorpay_entity(entities, "payment")

    payment_id = payment.get("id")
    if not payment_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing payment id",
        )

    notes = link.get("notes") if isinstance(link.get("notes"), dict) else {}

    # The grant authority requires the nonce proof — hash it, DROP the raw value.
    raw_nonce = notes.get("checkout_nonce")
    if not raw_nonce:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing checkout nonce",
        )
    nonce_hash = hashlib.sha256(str(raw_nonce).encode("utf-8")).hexdigest()

    # test/live guard — a test-key event must never settle against live config.
    if notes.get("environment") != razorpay_environment():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Webhook environment does not match server configuration",
        )

    return {
        "provider": "razorpay",
        "event_name": "payment_link.paid",
        "payment_id": str(payment_id),
        # The checkout object — corroboration only; the link amount is a value
        # Thought2Build set at creation, so the grant anchors on the payment (D10).
        "payment_link": {
            "payment_link_id": link.get("id"),
            "reference_id": link.get("reference_id"),
            "amount": link.get("amount"),
            "currency": link.get("currency"),
            "status": link.get("status"),
            "order_id": link.get("order_id"),
        },
        "payment": _razorpay_payment_block(payment),
        "refund": None,
        "notes": _razorpay_notes_block(notes, nonce_hash),
    }


def _normalize_razorpay_refund(entities: dict[str, Any]) -> dict[str, Any]:
    """Sanitise a ``refund.processed`` delivery (the reversal authority).

    The refund's ownership/settlement proof rides the **payment** entity's
    ``notes`` (Razorpay copies the link notes onto the payment) — best-effort:
    absent notes never reject a signed refund (the pack lookup by payment id is
    the primary path; the notes only serve ``_refund_without_pack``'s
    park-and-retry branches). The environment guard is enforced only when the
    marker is present — mismatch → 400, absence → processed (Lemon's
    ``test_mode`` parity).
    """
    refund = _razorpay_entity(entities, "refund")
    payment = _razorpay_entity(entities, "payment")

    refund_id = refund.get("id")
    payment_id = refund.get("payment_id") or payment.get("id")
    if not refund_id or not payment_id or payment.get("id") not in (None, payment_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing refund identity",
        )

    notes = payment.get("notes") if isinstance(payment.get("notes"), dict) else {}

    # Best-effort nonce (never required on a signed refund) — hashed when present.
    raw_nonce = notes.get("checkout_nonce")
    nonce_hash = (
        hashlib.sha256(str(raw_nonce).encode("utf-8")).hexdigest()
        if raw_nonce
        else None
    )

    # Environment: enforced when present; absence is never a rejection ground
    # for a signed refund.
    environment = notes.get("environment")
    if environment is not None and environment != razorpay_environment():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Webhook environment does not match server configuration",
        )

    return {
        "provider": "razorpay",
        "event_name": "refund.processed",
        "payment_id": str(payment_id),
        "payment_link": None,
        # The payment block carries the CUMULATIVE amount_refunded — the same
        # cumulative semantics as Lemon's refunded_amount, so partial + full
        # refunds settle correctly and re-deliveries are no-ops.
        "payment": _razorpay_payment_block(payment),
        "refund": {
            "refund_id": refund_id,
            "payment_id": refund.get("payment_id"),
            "amount": refund.get("amount"),
        },
        "notes": _razorpay_notes_block(notes, nonce_hash),
    }


def _normalize_razorpay_dispute(
    entities: dict[str, Any],
    event_name: str,
    created_at: object,
) -> dict[str, Any]:
    dispute = _razorpay_entity(entities, "dispute")
    payment = _razorpay_entity(entities, "payment")
    payment_id = dispute.get("payment_id")
    if (
        not dispute.get("id")
        or not payment_id
        or payment.get("id") not in (None, payment_id)
    ):
        raise HTTPException(400, "Missing or mismatched dispute identity")
    try:
        timestamp = int(created_at)
        amount = int(dispute["amount"])
        deducted = int(dispute.get("amount_deducted") or 0)
        if timestamp <= 0 or amount < 0 or deducted < 0:
            raise ValueError
    except (TypeError, ValueError, KeyError) as exc:
        raise HTTPException(400, "Malformed dispute economics or timestamp") from exc
    state = event_name.removeprefix("payment.dispute.")
    if state == "created":
        state = "open"
    if dispute.get("status") != state or not dispute.get("currency"):
        raise HTTPException(400, "Mismatched dispute state or currency")
    return {
        "provider": "razorpay",
        "event_name": event_name,
        "payment_id": str(payment_id),
        "event_created_at": timestamp,
        "payment": _razorpay_payment_block(payment),
        "dispute": {
            "dispute_id": str(dispute["id"]),
            "amount": amount,
            "currency": str(dispute["currency"]),
            "status": state,
            "amount_deducted": deducted,
        },
    }
