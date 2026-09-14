"""Durable billing-webhook processing (Phase 22 — T-298).

The arq-side half of the Lemon Squeezy webhook pipeline. The request path
(T-297) verifies + sanitises an inbound order event into a durable
``billing_webhook_events`` inbox row and enqueues by row id; everything money- or
state-mutating happens **here**, on the worker, exactly once under retries.

This module owns the durable scaffolding:

  * :func:`billing_process_webhook` — claim a single inbox row (``SELECT … FOR
    UPDATE``), short-circuit if already ``processed``, mark ``processing`` in its
    own committed transaction (the durable claim + the reclaim clock), dispatch by
    ``event_name``, and on failure persist ``failed``/``retry_count``/``last_error``
    in a **separate** committed transaction before re-raising (so the rollback of
    side effects never also discards the failed state — R14).
  * :func:`billing_process_pending_webhooks` — the 60s recovery sweep: re-enqueue
    committed ``received`` rows the queue lost, retryable ``failed`` rows, and
    ``processing`` rows a crashed worker abandoned (reclaimed → ``failed`` after
    5 minutes). ``FOR UPDATE SKIP LOCKED`` + the deterministic ``billing_wh:{id}``
    job id mean a live worker's lock is the liveness signal and an in-flight
    wrapper-retry is never double-driven.
  * :func:`billing_reconcile` — the 15-minute backstop. T-298 implements **Lane 1**
    (inbox replay, sharing the sweep's logic); T-301 layers the cursor-locked
    provider-check (Lane 2) and checkout-attempt hygiene (Lane 3) on top.
  * :func:`purge_billing_events` — daily retention purge bounding the inbox and the
    terminal checkout attempts.

The money handlers register themselves in :data:`_EVENT_HANDLERS` keyed by
``(provider, event_name)`` via :func:`register_event_handler` at module import:
Lemon ``order_created`` (T-299) / ``order_refunded`` (T-300), and Razorpay
``payment_link.paid`` / ``refund.processed`` (issue #44 Step 5). Keying by
``(provider, event_name)`` is defence-in-depth — the two providers' event-name
namespaces are disjoint today, but the provider makes a future collision
impossible. The ``arq``-registered wrappers and cron schedules live in
``worker.py``.

Handler contract (what T-299/T-300 must conform to)
---------------------------------------------------
A handler is ``async def handler(ctx, webhook_event_id: str) -> None``. It is
invoked by :func:`billing_process_webhook` **after** the inbox row has been
committed to ``processing``. It:
  * owns its own transaction(s) — opens its own session, does the money mutation,
    marks the row ``processed`` / ``processed_at``, and commits (so it can run any
    post-commit step such as the credit-cache invalidation);
  * **must be idempotent** — the sweep/reconcile can re-invoke it for the same row,
    so a provider-order/checkout uniqueness conflict must reload the existing pack,
    mark the row ``processed``, and write no second grant;
  * on raise, leaves the generic processor to persist the ``failed`` state in a
    separate committed transaction and schedule the arq retry.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

import structlog
from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.exc import DBAPIError, IntegrityError

from config import settings
from database import AsyncSessionLocal
from models import User
from models.billing_checkout_attempt import BillingCheckoutAttempt
from models.billing_credit_pack import BillingCreditPack
from models.billing_reconciliation_cursor import BillingReconciliationCursor
from models.billing_webhook_event import BillingWebhookEvent
from services.credit_service import credit_service
from services.lemonsqueezy_service import (
    LemonOrder,
    LemonSqueezyError,
    LemonSqueezyRateLimitError,
    lemonsqueezy_service,
)
from services.observability import (
    BILLING_CHECKOUT_COMPLETED,
    BILLING_CHECKOUT_EXPIRED,
    BILLING_CREDIT_DEBT_CREATED,
    BILLING_CREDIT_DEBT_RECOVERED,
    BILLING_CREDITS_GRANTED,
    BILLING_CREDITS_REVOKED,
    BILLING_PURCHASE_REVENUE_CENTS,
    BILLING_RECONCILE_MISMATCH,
    BILLING_UNRECOVERABLE_CHECKOUT,
    BILLING_WEBHOOK_ERROR,
    BILLING_WEBHOOK_PENDING_AGE_SECONDS,
)
from services.queue import JOB_MAX_TRIES, QueueUnavailableError, enqueue
from services.razorpay_service import (
    RazorpayError,
    RazorpayPayment,
    RazorpayRateLimitError,
    razorpay_environment,
    razorpay_service,
)

logger = structlog.get_logger(__name__)

# The inbox job name + the deterministic dedup job-id prefix. Shared verbatim with
# the request-path enqueue (T-297) so the wrapper-retry and the sweep collapse to a
# single in-flight job per row.
_PROCESS_JOB = "billing_process_webhook"
_JOB_ID_PREFIX = "billing_wh:"

# Order events carry money semantics and MUST have a registered handler before
# their provider is enabled (T-299/T-300; issue #44 for the Razorpay pair —
# handlers land in plan Step 5, so until then a signed Razorpay money event
# dead-letters loudly via _dispatch_claimed instead of being acked as a harmless
# no-op). Any other verified event is acknowledged as a no-op (we will never
# grant for it). Event names cannot collide across providers (Lemon's order_*
# vs Razorpay's dotted names).
_ORDER_EVENTS = frozenset(
    {
        "order_created",  # Lemon Squeezy — grant
        "order_refunded",  # Lemon Squeezy — reversal
        "payment_link.paid",  # Razorpay — grant (issue #44)
        "refund.processed",  # Razorpay — reversal (issue #44)
    }
)

# A 'processing' row whose claim is older than this is treated as abandoned by a
# crashed worker and reclaimed. ``received_at`` is the reclaim clock (the only
# timestamp on the row; the pending partial index is on (status, received_at)).
# 5 minutes is far beyond normal processing (seconds), so a live, fast handler is
# never reclaimed — and ``FOR UPDATE SKIP LOCKED`` skips a row a live worker still
# holds, making the lock the primary liveness signal and the clock the backstop.
_STALE_PROCESSING_MINUTES = 5

# Bounded batch sizes so a single cron tick can never run unboundedly.
_SWEEP_BATCH = 100
_PURGE_BATCH = 1000

# Terminal rows older than this are safe to delete (R9 / DC5). Generous relative
# to the provider redelivery window so dedup protection is never lost early.
_RETENTION_DAYS = 30

# Lemon order statuses that mean a *full* reversal even when the signed refunded
# amount is zero (a fraud chargeback can carry refunded_amount=0). The amount-based
# `>= provider_total` clause in apply_refund_reversal is the PRIMARY full-refund
# detector (robust to enum/spelling drift, per the T-300 event-catalog caveat); these
# strings are a supplementary signal that the catalog confirmation (T-307) validates.
# Shared by the webhook handler (T-300) and reconcile lane 2 (T-301).
_FULL_REVERSAL_STATUSES = frozenset({"refunded", "fraudulent"})

# How long a refund whose order_created has not yet landed is retried before it is
# given up as an audited no-op (Plan §25.6 T-300). `received_at` (never updated) is
# the clock.
_REFUND_GIVE_UP_HOURS = 24

# Sentinel ``last_error`` for a signed refund still waiting on its out-of-order
# ``order_created`` grant (the _refund_without_pack branch-a re-queue). These rows
# are NOT re-driven by the 60s sweep (that would re-run them ~1440×/24h for no gain);
# they ride the 15-minute reconcile lane 1 instead — the grant must land first
# anyway, so a ≤15-minute revocation latency on this rare out-of-order case is fine.
_PACK_NOT_YET_GRANTED = "pack_not_yet_granted"

# Reconcile (T-301). The run-state anchor is EVERY configured provider's cursor
# row (``lemonsqueezy`` seeded by 0018 + ``razorpay`` by 0034) — all claimed
# upfront under one NOWAIT lock so the provider-neutral lanes 1/3 can never be
# double-run by overlapping ticks (D11, issue #44).
# Postgres SQLSTATE for a FOR UPDATE NOWAIT that found the row already locked.
_LOCK_NOT_AVAILABLE_SQLSTATE = "55P03"
# Lane 2 sweeps packs that can still change refund/fraud state.
_RECONCILE_LIVE_PACK_STATUSES = (
    "active",
    "consumed",
    "expired",
    "refunded",
    "disputed",
)
# Lane 3 bounded expiry batch + the stale-attempt count that trips an operator alert.
_RECONCILE_ATTEMPT_BATCH = 500
_RECONCILE_STALE_ATTEMPT_ALERT = 200

# A money/state handler for one (provider, event_name). See "Handler contract".
EventHandler = Callable[[dict, str], Awaitable[None]]

# Keyed by (provider, event_name) — Lemon order_created/order_refunded (T-299/T-300)
# and Razorpay payment_link.paid/refund.processed (issue #44). Populated at the
# bottom of this module at import.
_EVENT_HANDLERS: dict[tuple[str, str], EventHandler] = {}


def register_event_handler(
    provider: str, event_name: str, handler: EventHandler
) -> None:
    """Register the money/state handler for ``(provider, event_name)``.

    Keyed by the pair (issue #44) so a future event-name collision across
    providers can never silently route a Razorpay event to a Lemon handler.
    """
    _EVENT_HANDLERS[(provider, event_name)] = handler


def _now() -> datetime:
    return datetime.now(UTC)


def _format_error(exc: BaseException) -> str:
    """A bounded, secret-free error string for ``last_error`` (no raw payload)."""
    return f"{type(exc).__name__}: {exc}"[:2000]


# ---------------------------------------------------------------------------
# The durable processor
# ---------------------------------------------------------------------------


async def billing_process_webhook(ctx: dict, webhook_event_id: str) -> None:
    """Process one inbox row exactly once (the body of the arq job).

    Claims the row (``FOR UPDATE``), short-circuits a row already ``processed``,
    marks ``processing`` and **commits** (durable claim), then dispatches. On any
    failure it rolls back the side effects, persists ``failed`` + ``retry_count`` +
    ``last_error`` in a SEPARATE committed transaction, and re-raises so the
    ``billing_job`` wrapper schedules the arq retry.
    """
    wid = UUID(webhook_event_id)

    # 1. Durable claim. Lock the row, short-circuit if already done, flip to
    #    'processing' and commit so the claim (and the reclaim clock) survives a
    #    crash and a concurrent worker sees it taken.
    async with AsyncSessionLocal() as db:
        row = await db.scalar(
            select(BillingWebhookEvent)
            .where(BillingWebhookEvent.id == wid)
            .with_for_update()
        )
        if row is None:
            logger.warning("billing.process.row_missing", webhook_event_id=str(wid))
            return
        if row.status == "processed":
            logger.info("billing.process.already_processed", webhook_event_id=str(wid))
            return
        row.status = "processing"
        await db.commit()

    # 2. Dispatch. On success the handler (or the no-op ack) marks the row
    #    'processed'. On failure persist the failed state separately, then re-raise.
    try:
        await _dispatch_claimed(ctx, wid)
    except Exception as exc:
        await _persist_failed(wid, exc)
        raise


async def _dispatch_claimed(ctx: dict, wid: UUID) -> None:
    """Route a claimed row to its handler, or ack/raise when none is registered."""
    async with AsyncSessionLocal() as db:
        row = await db.scalar(
            select(BillingWebhookEvent).where(BillingWebhookEvent.id == wid)
        )
        if row is None or row.status == "processed":
            return
        event_name = row.event_name
        provider = row.provider

    handler = _EVENT_HANDLERS.get((provider, event_name))
    if handler is not None:
        # The handler owns its transaction and the 'processed' transition.
        await handler(ctx, str(wid))
        return

    if event_name in _ORDER_EVENTS:
        # Money event with no handler for this (provider, event_name). Fail loud
        # (it dead-letters and is visible/recoverable) rather than silently acking
        # a paid order. _ORDER_EVENTS stays keyed by event_name alone — the
        # semantic "this is a money event" is provider-independent.
        raise RuntimeError(
            f"no handler registered for order event {event_name!r} "
            f"(provider {provider!r})"
        )

    # An event we will never act on (subscription_*, license_*, …) — ack it.
    await _mark_processed(wid)
    logger.info(
        "billing.process.acked_unhandled", event_name=event_name, provider=provider
    )


async def _mark_processed(wid: UUID) -> None:
    """Mark a row ``processed`` (used for the no-op ack path)."""
    async with AsyncSessionLocal() as db:
        row = await db.scalar(
            select(BillingWebhookEvent)
            .where(BillingWebhookEvent.id == wid)
            .with_for_update()
        )
        if row is None or row.status == "processed":
            return
        row.status = "processed"
        row.processed_at = _now()
        await db.commit()


async def _persist_failed(wid: UUID, exc: BaseException) -> None:
    """Persist ``failed``/``retry_count``/``last_error`` in a SEPARATE committed tx.

    Non-negotiable (R14): the side-effect transaction has already rolled back, so
    the failed state must be written in its own transaction or a crash would lose
    it and the row would look stuck ``processing`` until the 5-minute reclaim.
    """
    async with AsyncSessionLocal() as db:
        row = await db.scalar(
            select(BillingWebhookEvent)
            .where(BillingWebhookEvent.id == wid)
            .with_for_update()
        )
        if row is None:
            return
        row.status = "failed"
        row.retry_count = (row.retry_count or 0) + 1
        row.last_error = _format_error(exc)
        provider = row.provider
        await db.commit()
    BILLING_WEBHOOK_ERROR.labels(provider=provider, error_type=type(exc).__name__).inc()
    logger.warning("billing.process.failed", webhook_event_id=str(wid))


# ---------------------------------------------------------------------------
# The 60s recovery sweep
# ---------------------------------------------------------------------------


async def billing_process_pending_webhooks(ctx: dict) -> None:
    """Re-enqueue stuck/orphaned inbox rows (the 60s recovery cron).

    Plain cron (catch + log — never raises): reclaims ``processing`` rows abandoned
    for more than 5 minutes (→ ``failed``, ``retry_count += 1``), then re-enqueues
    committed ``received`` rows and retryable ``failed`` rows. Uses ``FOR UPDATE
    SKIP LOCKED`` + a bounded batch and the deterministic ``billing_wh:{id}`` job id
    so an in-flight wrapper-retry is never double-driven. Never fetches from Lemon.
    Also refreshes the ``webhook_pending_age_seconds`` gauge (the lost-webhook
    alert, T-304).
    """
    try:
        # The 60s sweep excludes refunds parked on their out-of-order grant — those
        # ride the 15-minute reconcile lane 1 (T-308 review hardening).
        ids = await _claim_pending_ids(
            reclaim_stale=True, exclude_deferred_retries=True
        )
        await _enqueue_ids(ctx, ids)
        if ids:
            logger.info("billing.sweep.enqueued", count=len(ids))
        await _refresh_pending_age_gauge()
    except Exception:  # pragma: no cover - best-effort; the next tick retries
        logger.exception("billing.sweep.failed")


async def _refresh_pending_age_gauge() -> None:
    """Set ``webhook_pending_age_seconds`` to the age of the oldest unprocessed row.

    The lost-webhook signal (T-304): any ``billing_webhook_events`` row not yet
    ``processed`` is a grant/reversal still owed. We publish the age of the oldest
    such row (``received_at`` never moves) so the >300s alert fires when the sweep +
    reconcile lanes cannot drain the inbox. Zero when the inbox is clear.
    """
    async with AsyncSessionLocal() as db:
        oldest = await db.scalar(
            select(func.min(BillingWebhookEvent.received_at)).where(
                BillingWebhookEvent.status != "processed"
            )
        )
    age = 0.0 if oldest is None else max(0.0, (_now() - oldest).total_seconds())
    BILLING_WEBHOOK_PENDING_AGE_SECONDS.set(age)


@dataclass
class _Lane2Result:
    """Lane-2 outcome: the advanced cursor state, calls spent, reversals applied."""

    state: dict
    calls: int
    reversals: int


def _is_lock_not_available(exc: DBAPIError) -> bool:
    """True when a ``FOR UPDATE NOWAIT`` failed because the row was already locked."""
    return getattr(getattr(exc, "orig", None), "sqlstate", None) == (
        _LOCK_NOT_AVAILABLE_SQLSTATE
    )


def _order_has_reversal(order: LemonOrder) -> bool:
    """True when a re-read order shows a refund/fraud the local pack may have missed."""
    return (
        order.refunded
        or order.refunded_amount_cents > 0
        or order.status in ("refunded", "partial_refund", "fraudulent")
    )


def _reversal_decision(order_status: str | None) -> tuple[bool, str]:
    """Map a Lemon order status to ``(full_or_fraud, reason_label)`` for the helper.

    Shared by the ``order_refunded`` webhook handler (T-300) and reconcile lane 2
    (T-301) so both classify a reversal identically.
    """
    full_or_fraud = order_status in _FULL_REVERSAL_STATUSES
    reason_label = "fraud" if order_status == "fraudulent" else "refund"
    return full_or_fraud, reason_label


def _razorpay_has_reversal(payment: RazorpayPayment) -> bool:
    """True when a re-read payment shows a refund the local pack may have missed."""
    return payment.amount_refunded_cents > 0 or payment.refund_status in (
        "partial",
        "full",
    )


def _razorpay_reversal_decision(payment: RazorpayPayment) -> tuple[bool, str]:
    """Map a re-read Razorpay payment to ``(full_or_fraud, reason_label)`` (issue #44).

    Shared by the ``refund.processed`` handler (Step 5) and reconcile lane 2. A
    refund is *full* when the provider marks ``refund_status == "full"`` **or** the
    cumulative refunded amount covers the payment; ``reason_label`` is always
    ``"refund"`` — Razorpay payments carry NO fraud/chargeback status (disputes are
    separate entities, Plan §11), so there is no ``"fraud"`` analogue. The
    ``amount_cents > 0`` guard keeps a payment with a missing/zero amount from
    reading as a spurious full refund.
    """
    full = payment.refund_status == "full" or (
        payment.amount_cents > 0
        and payment.amount_refunded_cents >= payment.amount_cents
    )
    return full, "refund"


@dataclass
class _RereadReversal:
    """A provider re-read normalised to lane-2's reversal decision (issue #44)."""

    has_reversal: bool
    refunded_amount_cents: int
    full_or_fraud: bool
    reason_label: str


async def _reconcile_reread(provider: str, order_id: str) -> _RereadReversal:
    """Re-read one order/payment and normalise its reversal decision (lane 2).

    Provider dispatch: Lemon ``get_order`` (status-driven) or Razorpay
    ``get_payment`` (refund-status/amount-driven). The provider's rate-limit /
    contract-error types propagate unchanged so lane 2 backs off and stops the lane
    with the cursor at the last success.
    """
    if provider == "razorpay":
        payment = await razorpay_service.get_payment(order_id)
        full_or_fraud, reason_label = _razorpay_reversal_decision(payment)
        return _RereadReversal(
            has_reversal=_razorpay_has_reversal(payment),
            refunded_amount_cents=payment.amount_refunded_cents,
            full_or_fraud=full_or_fraud,
            reason_label=reason_label,
        )
    order = await lemonsqueezy_service.get_order(order_id)
    full_or_fraud, reason_label = _reversal_decision(order.status)
    return _RereadReversal(
        has_reversal=_order_has_reversal(order),
        refunded_amount_cents=order.refunded_amount_cents,
        full_or_fraud=full_or_fraud,
        reason_label=reason_label,
    )


async def billing_reconcile(ctx: dict) -> None:
    """15-minute reconciliation backstop (plain cron; catch + log — never raises).

    Authoritative run-state lives in the ``billing_reconciliation_cursors`` rows
    (Postgres, not Redis) — one per configured provider (``lemonsqueezy`` +
    ``razorpay``, issue #44). **All** rows are claimed in one ``SELECT … ORDER BY
    provider FOR UPDATE NOWAIT`` and the locks are **held in this session for the
    entire run** — that is the single-active-run guarantee (D11): an overlapping
    cron tick that cannot claim *any* row gets a lock-not-available error and skips
    the whole tick cleanly (not a failure, no retry/dead-letter). Claiming all rows
    upfront (never per-provider) stops two overlapping ticks from splitting the
    providers and double-running the provider-neutral lanes 1/3. On success every
    cursor's state + ``last_successful_run_at`` advance and the locks release
    atomically; on failure the run rolls back (cursors unchanged) and only
    ``last_error`` is persisted on every claimed row.

    Lane 1 — inbox replay (provider-neutral): re-enqueue committed ``received``/
    retryable ``failed``/reclaimed stale ``processing`` rows. The inbox row carries
    the signed ``checkout_ref`` + nonce proof, so this is the **only** automatic path
    to recover a missed grant.
    Lane 2 — bounded per-provider re-read (D3 — every configured provider, not just
    the active one): for each provider's live packs, ``get_order`` / ``get_payment``
    by id and run that provider's refund/fraud helper. Capped at the provider's
    ``*_reconcile_max_calls_per_run`` and cursor-paged by ``provider_order_id``; backs
    off + stops (that provider's cursor unchanged) on 429/5xx.
    Lane 3 — checkout-attempt hygiene (provider-neutral): expire local attempts past
    ``expires_at`` (metric labelled by each attempt's provider) and alert on stale
    buildup.

    Razorpay additionally recovers first grants through authenticated reads of
    server-recorded Payment Links and their payments, validated by the same
    ownership/economics proof as webhook settlement. It never matches by email
    or amount alone.
    """
    async with AsyncSessionLocal() as lock_db:
        # 1. Claim the single-run lock on ALL cursor rows (ordered by provider ASC,
        #    NOWAIT). A consistent lock order + NOWAIT means any row already held ⇒
        #    the whole statement raises lock-not-available ⇒ skip the tick (D11).
        try:
            cursors = (
                (
                    await lock_db.execute(
                        select(BillingReconciliationCursor)
                        .order_by(BillingReconciliationCursor.provider.asc())
                        .with_for_update(nowait=True)
                    )
                )
                .scalars()
                .all()
            )
        except DBAPIError as exc:
            await lock_db.rollback()
            if _is_lock_not_available(exc):
                logger.info("billing.reconcile.skip_locked")
                return
            logger.exception("billing.reconcile.cursor_lock_failed")
            return
        if not cursors:
            # 0018 seeds 'lemonsqueezy', 0034 seeds 'razorpay'; an empty table is a
            # deploy/migration fault, not something a tick should paper over.
            logger.warning("billing.reconcile.cursor_missing")
            return

        # Snapshot the claimed providers as plain strings NOW — the failure path
        # calls _persist_reconcile_error AFTER lock_db.rollback() expires these ORM
        # rows, and reading cursor.provider then would trigger a sync lazy-refresh
        # (MissingGreenlet).
        claimed_providers = [cursor.provider for cursor in cursors]

        # Held until the final commit/rollback — do NOT commit mid-run (that would
        # release the locks and break the single-active-run guarantee).
        started = _now()
        for cursor in cursors:
            cursor.last_run_started_at = started

        try:
            replayed = await _reconcile_lane1(ctx)
            # Lane 2 per provider: each provider re-reads only its own packs against
            # its own API/budget/back-off; a provider I/O error stops just that
            # provider's lane (returns normally), never the whole run.
            lane2_results: dict[str, _Lane2Result] = {}
            for cursor in cursors:
                lane2_results[cursor.provider] = await _reconcile_lane2(
                    provider=cursor.provider, state=dict(cursor.state or {})
                )
                if cursor.provider == "razorpay":
                    from services.razorpay_settlement import recover_attempts

                    async def heartbeat():
                        # Keep the reconciliation mutex transaction alive while
                        # provider-bound reads run in separate short sessions.
                        await lock_db.execute(select(1))

                    await recover_attempts(
                        lane2_results[cursor.provider].state, heartbeat=heartbeat
                    )
            expired_attempts = await _reconcile_lane3()
        except Exception:
            await lock_db.rollback()  # discards last_run_started_at; cursors unchanged
            logger.exception("billing.reconcile.failed")
            await _persist_reconcile_error(claimed_providers)
            return

        # Success: advance every cursor + timestamps atomically and release the locks.
        completed = _now()
        for cursor in cursors:
            cursor.state = lane2_results[cursor.provider].state
            cursor.last_successful_run_at = completed
            cursor.last_run_completed_at = completed
            cursor.last_error = None
            cursor.updated_at = completed
        await lock_db.commit()
        logger.info(
            "billing.reconcile.done",
            replayed=replayed,
            lane2_calls=sum(r.calls for r in lane2_results.values()),
            lane2_reversals=sum(r.reversals for r in lane2_results.values()),
            expired_attempts=expired_attempts,
        )


async def _reconcile_lane1(ctx: dict) -> int:
    """Lane 1 — re-enqueue pending inbox rows (idempotent, own sessions); count out."""
    ids = await _claim_pending_ids(reclaim_stale=True)
    await _enqueue_ids(ctx, ids)
    if ids:
        logger.info("billing.reconcile.replayed", count=len(ids))
    return len(ids)


async def _reconcile_lane2(*, provider: str, state: dict) -> _Lane2Result:
    """Lane 2 — bounded re-read of one provider's live local packs (refund backstop).

    Reads up to that provider's ``*_reconcile_max_calls_per_run`` live packs ordered
    by ``provider_order_id`` and resuming after the cursor's ``lane2_last_order_id``;
    re-reads each via the provider (``get_order`` / ``get_payment``), and on a missed
    reversal runs ``apply_refund_reversal`` in its own committed session (so a single
    bad order never poisons the batch). On a 429/provider error it backs off and
    stops the lane with the cursor at the last successfully processed id (it advances
    next run) — the error is swallowed here, never raised, so a rate-limited provider
    can never roll back the *other* provider's cursor advance. When the ordered scan
    is exhausted the cursor resets to the start so reconciliation is continuous.
    Never grants — ``apply_refund_reversal`` only revokes, never credits.
    """
    if provider == "razorpay":
        max_calls = settings.razorpay_reconcile_max_calls_per_run
    else:
        max_calls = settings.lemonsqueezy_reconcile_max_calls_per_run
    last_id = str(state.get("lane2_last_order_id") or "")

    async with AsyncSessionLocal() as db:
        candidates = (
            await db.execute(
                select(BillingCreditPack.id, BillingCreditPack.provider_order_id)
                .where(
                    BillingCreditPack.provider == provider,
                    BillingCreditPack.status.in_(_RECONCILE_LIVE_PACK_STATUSES),
                    BillingCreditPack.provider_order_id.isnot(None),
                    BillingCreditPack.provider_order_id > last_id,
                    BillingCreditPack.purchased_at
                    >= _now()
                    - timedelta(days=settings.razorpay_reconcile_lookback_days),
                )
                .order_by(BillingCreditPack.provider_order_id.asc())
                .limit(max_calls)
            )
        ).all()

    calls = 0
    reversals = 0
    new_last = last_id
    stopped_early = False
    for pack_id, order_id in candidates:
        try:
            reread = await _reconcile_reread(provider, order_id)
        except (LemonSqueezyRateLimitError, RazorpayRateLimitError) as exc:
            logger.warning(
                "billing.reconcile.lane2_rate_limited",
                provider=provider,
                retry_after=round(exc.retry_after, 1),
                order_id=order_id,
            )
            stopped_early = True
            break
        except (LemonSqueezyError, RazorpayError):
            logger.warning(
                "billing.reconcile.lane2_provider_error",
                provider=provider,
                order_id=order_id,
            )
            stopped_early = True
            break
        calls += 1

        if reread.has_reversal:
            if await _reconcile_apply_reversal(
                provider=provider,
                pack_id=pack_id,
                refunded_amount_cents=reread.refunded_amount_cents,
                full_or_fraud=reread.full_or_fraud,
                reason_label=reread.reason_label,
                order_id=order_id,
            ):
                reversals += 1

        # Advance only after a successful re-read (+ any reversal commit).
        new_last = order_id

    # A full, uninterrupted scan that returned fewer than the cap means we reached
    # the end of the ordered set — reset so the next run sweeps from the start.
    if not stopped_early and len(candidates) < max_calls:
        new_last = ""

    state["lane2_last_order_id"] = new_last
    return _Lane2Result(state=state, calls=calls, reversals=reversals)


async def _reconcile_apply_reversal(
    *,
    provider: str,
    pack_id: UUID,
    refunded_amount_cents: int,
    full_or_fraud: bool,
    reason_label: str,
    order_id: str | None,
) -> bool:
    """Run the T-300 reversal helper for one re-read pack in its own session.

    Provider-neutral: the caller (lane 2) has already resolved the reversal decision
    from that provider's re-read. Returns True if a revocation was actually applied
    (a webhook the path missed).
    """
    async with AsyncSessionLocal() as db:
        if provider == "razorpay":
            from services.razorpay_settlement import lock_payment, settle_pack

            await lock_payment(db, order_id)
        pack = await db.scalar(
            select(BillingCreditPack).where(BillingCreditPack.id == pack_id)
        )
        if pack is None:  # deleted between the candidate read and now — skip
            return False
        user_id = pack.user_id
        if provider == "razorpay":
            outcome = await settle_pack(db, pack, refunded_amount_cents)
            await db.commit()
            await credit_service.invalidate(user_id)
            outcome.record(reconciled=True)
            return bool(outcome.revoked or outcome.released)
        outcome = await credit_service.apply_refund_reversal(
            db,
            source_pack=pack,
            provider_refunded_amount_cents=refunded_amount_cents,
            full_or_fraud=full_or_fraud,
            reason_label=reason_label,
        )
        await db.commit()

    await credit_service.invalidate(user_id)
    if outcome.credits_revoked > 0:
        BILLING_CREDITS_REVOKED.labels(provider=provider, reason=reason_label).inc(
            outcome.credits_revoked
        )
    if outcome.debt_created > 0:
        BILLING_CREDIT_DEBT_CREATED.labels(provider=provider, reason=reason_label).inc(
            outcome.debt_created
        )
    if outcome.applied:
        BILLING_RECONCILE_MISMATCH.labels(provider=provider).inc()
        logger.warning(
            "billing.reconcile.mismatch",
            provider=provider,
            order_id=order_id,
            reason=reason_label,
            credits_revoked=outcome.credits_revoked,
            debt_created=outcome.debt_created,
        )
    return outcome.applied


async def _reconcile_lane3() -> int:
    """Lane 3 — expire local checkout attempts past their TTL; alert on stale buildup.

    Returns the number of attempts expired this run.
    """
    now = _now()
    async with AsyncSessionLocal() as db:
        # Select the provider alongside the id — an expired-attempt batch can span
        # both providers, and the metric is labelled by each attempt's own provider.
        rows = (
            await db.execute(
                select(BillingCheckoutAttempt.id, BillingCheckoutAttempt.provider)
                .where(
                    BillingCheckoutAttempt.status.in_(("created", "provider_created")),
                    BillingCheckoutAttempt.expires_at < now,
                )
                .order_by(BillingCheckoutAttempt.expires_at)
                .limit(_RECONCILE_ATTEMPT_BATCH)
                .with_for_update(skip_locked=True)
            )
        ).all()
        ids = [row.id for row in rows]
        if ids:
            await db.execute(
                update(BillingCheckoutAttempt)
                .where(BillingCheckoutAttempt.id.in_(ids))
                .values(status="expired")
            )

        # Aggregate stale/pending alert: attempts still open after expiry would be a
        # webhook-delivery or checkout-flow regression worth an operator's attention.
        still_open = await db.scalar(
            select(func.count())
            .select_from(BillingCheckoutAttempt)
            .where(
                BillingCheckoutAttempt.status.in_(("created", "provider_created")),
                BillingCheckoutAttempt.expires_at < now,
            )
        )
        await db.commit()

    if ids:
        per_provider: dict[str, int] = {}
        for row in rows:
            per_provider[row.provider] = per_provider.get(row.provider, 0) + 1
        for expired_provider, count in per_provider.items():
            BILLING_CHECKOUT_EXPIRED.labels(provider=expired_provider).inc(count)
        logger.info("billing.reconcile.attempts_expired", count=len(ids))
    if still_open and still_open >= _RECONCILE_STALE_ATTEMPT_ALERT:
        logger.warning(
            "billing.reconcile.stale_attempts_high",
            open_past_ttl=still_open,
            threshold=_RECONCILE_STALE_ATTEMPT_ALERT,
        )
    return len(ids)


async def _persist_reconcile_error(providers: list[str]) -> None:
    """Persist the failed run's ``last_error`` on every claimed cursor row.

    Runs after the run session rolled back (releasing the locks), in a fresh
    session; best-effort — a concurrent tick may re-run before this lands.
    """
    try:
        async with AsyncSessionLocal() as db:
            cursors = (
                (
                    await db.execute(
                        select(BillingReconciliationCursor).where(
                            BillingReconciliationCursor.provider.in_(providers)
                        )
                    )
                )
                .scalars()
                .all()
            )
            now = _now()
            for cursor in cursors:
                cursor.last_error = (
                    "reconcile run failed; see billing.reconcile.failed log"
                )
                cursor.last_run_started_at = now
                cursor.updated_at = now
            await db.commit()
    except Exception:  # pragma: no cover - best-effort error annotation
        logger.exception("billing.reconcile.error_persist_failed")


async def _claim_pending_ids(
    *, reclaim_stale: bool, exclude_deferred_retries: bool = False
) -> list[UUID]:
    """Reclaim stale ``processing`` rows, then return pending ids to re-enqueue.

    One transaction: lock + reclaim abandoned ``processing`` rows, then select the
    pending set (``received`` ∪ retryable ``failed``, which now includes the
    just-reclaimed rows). ``FOR UPDATE SKIP LOCKED`` skips rows a live worker holds.

    ``exclude_deferred_retries`` (the 60s sweep passes True) drops ``received`` rows
    tagged ``_PACK_NOT_YET_GRANTED`` — a signed refund waiting on its out-of-order
    grant. Those ride the 15-minute reconcile lane 1 instead (which passes the
    default False), so the sweep does not re-run them every 60s for the whole 24h
    give-up horizon. A first-delivery refund (``last_error`` NULL) is never deferred.
    """
    async with AsyncSessionLocal() as db:
        if reclaim_stale:
            cutoff = _now() - timedelta(minutes=_STALE_PROCESSING_MINUTES)
            stale = (
                (
                    await db.execute(
                        select(BillingWebhookEvent.id)
                        .where(
                            BillingWebhookEvent.status == "processing",
                            BillingWebhookEvent.received_at < cutoff,
                        )
                        .order_by(BillingWebhookEvent.received_at)
                        .limit(_SWEEP_BATCH)
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            if stale:
                await db.execute(
                    update(BillingWebhookEvent)
                    .where(BillingWebhookEvent.id.in_(stale))
                    .values(
                        status="failed",
                        retry_count=BillingWebhookEvent.retry_count + 1,
                        last_error=(
                            "reclaimed: processing exceeded "
                            f"{_STALE_PROCESSING_MINUTES} minutes"
                        ),
                    )
                )

        received_clause = BillingWebhookEvent.status == "received"
        if exclude_deferred_retries:
            # Skip refunds parked waiting on their out-of-order grant; the 15-min
            # reconcile lane re-drives those. NULL last_error (a fresh delivery) is
            # always included.
            received_clause = received_clause & (
                (BillingWebhookEvent.last_error.is_(None))
                | (BillingWebhookEvent.last_error != _PACK_NOT_YET_GRANTED)
            )
        pending = (
            (
                await db.execute(
                    select(BillingWebhookEvent.id)
                    .where(
                        received_clause
                        | (
                            (BillingWebhookEvent.status == "failed")
                            & (BillingWebhookEvent.retry_count < JOB_MAX_TRIES)
                        )
                    )
                    .order_by(BillingWebhookEvent.received_at)
                    .limit(_SWEEP_BATCH)
                    .with_for_update(skip_locked=True)
                )
            )
            .scalars()
            .all()
        )
        await db.commit()
    return list(pending)


async def _enqueue_ids(ctx: dict, ids: list[UUID]) -> None:
    """Enqueue each id under its deterministic dedup job id (best-effort)."""
    pool = ctx.get("redis")
    for wid in ids:
        try:
            await enqueue(
                _PROCESS_JOB,
                str(wid),
                job_id=f"{_JOB_ID_PREFIX}{wid}",
                pool=pool,
            )
        except QueueUnavailableError:
            # Redis is unreachable — stop this run; the next tick retries the
            # whole pending set (rows stay 'received'/'failed').
            logger.warning("billing.sweep.queue_unavailable")
            return


# ---------------------------------------------------------------------------
# Retention purge
# ---------------------------------------------------------------------------


async def purge_billing_events(ctx: dict) -> None:
    """Daily: bound the inbox + terminal checkout-attempt growth (cron; catch+log).

    Deletes terminal ``processed`` ``billing_webhook_events`` (by ``processed_at``)
    and terminal (``expired``/``failed``/``completed``) ``billing_checkout_attempts``
    (by ``created_at``) older than the retention window, in bounded batches.
    """
    try:
        cutoff = _now() - timedelta(days=_RETENTION_DAYS)
        async with AsyncSessionLocal() as db:
            webhook_ids = (
                (
                    await db.execute(
                        select(BillingWebhookEvent.id)
                        .where(
                            BillingWebhookEvent.status == "processed",
                            BillingWebhookEvent.processed_at < cutoff,
                            # A refund with no grant is durable reversal evidence.
                            ~(
                                (BillingWebhookEvent.provider == "razorpay")
                                & (BillingWebhookEvent.event_name == "refund.processed")
                                & ~select(BillingCreditPack.id)
                                .where(
                                    BillingCreditPack.provider == "razorpay",
                                    BillingCreditPack.provider_order_id
                                    == BillingWebhookEvent.provider_object_id,
                                )
                                .exists()
                            ),
                        )
                        .limit(_PURGE_BATCH)
                    )
                )
                .scalars()
                .all()
            )
            if webhook_ids:
                await db.execute(
                    delete(BillingWebhookEvent).where(
                        BillingWebhookEvent.id.in_(webhook_ids)
                    )
                )

            attempt_ids = (
                (
                    await db.execute(
                        select(BillingCheckoutAttempt.id)
                        .where(
                            BillingCheckoutAttempt.status.in_(
                                ("expired", "failed", "completed")
                            ),
                            BillingCheckoutAttempt.created_at < cutoff,
                            # Unresolved Razorpay attempts are payment evidence,
                            # retained for provider recovery and support.
                            ~(
                                (BillingCheckoutAttempt.provider == "razorpay")
                                & (BillingCheckoutAttempt.status != "completed")
                                & BillingCheckoutAttempt.provider_checkout_id.isnot(
                                    None
                                )
                            ),
                        )
                        .limit(_PURGE_BATCH)
                    )
                )
                .scalars()
                .all()
            )
            if attempt_ids:
                await db.execute(
                    delete(BillingCheckoutAttempt).where(
                        BillingCheckoutAttempt.id.in_(attempt_ids)
                    )
                )
            await db.commit()
        logger.info(
            "billing.purge.done",
            webhook_events=len(webhook_ids),
            checkout_attempts=len(attempt_ids),
        )
    except Exception:  # pragma: no cover - best-effort; the next daily tick retries
        logger.exception("billing.purge.failed")


# ---------------------------------------------------------------------------
# order_created — grant credits for a verified, proof-matched paid order (T-299)
# ---------------------------------------------------------------------------
#
# Validation and the grant are anchored to the checkout-attempt SNAPSHOT
# (``attempt.credits`` / ``attempt.price_cents`` / ``attempt.currency`` /
# ``attempt.validity_days``), never live ``LEMONSQUEEZY_*`` config (Plan §25 DC6) —
# a config change between checkout creation and the webhook must not break or
# mis-price an in-flight purchase. Ownership is proven ONLY by ``checkout_ref`` +
# the stored nonce hash; a checkout id is never inferred from the order id, receipt,
# relationships, or order number. The signed-but-informational custom
# ``credits``/``price_cents`` echoed by Lemon are deliberately ignored.

# A checkout attempt is still grant-eligible in any of these statuses (a redelivered
# webhook may arrive after the attempt is already ``completed``; idempotency below
# still guards against a second grant).
_GRANTABLE_ATTEMPT_STATUSES = frozenset({"created", "provider_created", "completed"})


def _coerce_int(value: object) -> int | None:
    """Best-effort coercion of a JSON scalar to ``int`` (Lemon sends ints)."""
    if isinstance(value, bool):  # bool is an int subclass — reject it explicitly
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value)
    return None


def _parse_order_timestamp(value: object) -> datetime:
    """Parse Lemon's ISO order ``created_at`` (anchor for expiry); fall back to now."""
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
    return _now()


def _order_created_rejection(
    payload: dict, custom: dict, attempt: BillingCheckoutAttempt | None
) -> str | None:
    """Run the T-299 validation checklist; return a rejection reason, or None to grant.

    Every term in the checklist is terminal on failure (no retry, no reconcile
    first-grant) — a returned reason marks the webhook ``processed`` with a sanitised
    warning so a config error (wrong store/variant) is alertable rather than silent.
    """
    if attempt is None:
        return "attempt_not_found"
    # Ownership is proven by checkout_ref (already used to load the attempt) + the
    # nonce hash + the sanitised custom user_id — never inferred from the order id.
    raw_user_id = custom.get("user_id")
    try:
        custom_user_id = UUID(str(raw_user_id))
    except (ValueError, TypeError):
        return "user_id_invalid"
    if custom_user_id != attempt.user_id:
        return "user_id_mismatch"
    if custom.get("checkout_nonce_hash_from_webhook") != attempt.checkout_nonce_hash:
        return "nonce_mismatch"
    if attempt.status not in _GRANTABLE_ATTEMPT_STATUSES:
        return "attempt_status_invalid"
    # Provider-side invariants come from live config (the store/variant/test_mode the
    # platform is wired to); economics come from the attempt snapshot.
    if str(payload.get("store_id")) != settings.lemonsqueezy_store_id:
        return "store_mismatch"
    if str(payload.get("variant_id")) != settings.lemonsqueezy_variant_id:
        return "variant_mismatch"
    if payload.get("test_mode") != settings.lemonsqueezy_test_mode:
        return "test_mode_mismatch"
    if payload.get("status") != "paid":
        return "status_not_paid"
    order_currency = str(payload.get("currency") or "")
    if order_currency.upper() != attempt.currency.upper():
        return "currency_mismatch"
    if _coerce_int(payload.get("discount_total_cents")) != 0:
        return "discount_present"
    # Validate the ITEM price against the attempt snapshot, never the order
    # total/subtotal — Lemon may add tax on top of the item price.
    if _coerce_int(payload.get("item_price_cents")) != attempt.price_cents:
        return "price_mismatch"
    return None


async def _find_existing_pack(
    db,
    *,
    order_id: str | None,
    provider_checkout_id: str | None,
    provider: str,
) -> BillingCreditPack | None:
    """Reload an already-granted pack for this order/checkout (the idempotency key).

    Matches on ``(provider, provider_order_id)`` OR ``(provider, provider_checkout_id)``
    — guarding against a NULL key matching another NULL-keyed pack. ``provider`` is a
    required argument now that Razorpay joined Lemon as a runtime provider (issue #44).
    """
    conditions = []
    if order_id is not None:
        conditions.append(BillingCreditPack.provider_order_id == order_id)
    if provider_checkout_id is not None:
        conditions.append(
            BillingCreditPack.provider_checkout_id == provider_checkout_id
        )
    if not conditions:
        return None
    return await db.scalar(
        select(BillingCreditPack)
        .where(BillingCreditPack.provider == provider)
        .where(or_(*conditions))
        .limit(1)
    )


async def _ack_order_processed(wid: UUID) -> None:
    """Mark the webhook row ``processed`` in a fresh session (post-rollback ack).

    Used after an idempotency conflict poisons the grant transaction: the pack was
    already granted by a prior delivery, so we reload nothing here — just durably
    acknowledge this redelivery so the sweep stops re-driving it. No second ledger row.
    """
    await _mark_processed(wid)


async def handle_order_created(ctx: dict, webhook_event_id: str) -> None:
    """Grant credits for a verified, proof-matched paid ``order_created`` (T-299).

    Idempotent: a duplicate ``order_created`` (or a redelivered inbox row, even with a
    different ``payload_hash``) reloads the existing pack and writes no second grant —
    enforced by the ``(provider, provider_order_id)`` / ``(provider,
    provider_checkout_id)`` pack uniqueness and the ``billing_purchase:`` ledger index.
    """
    wid = UUID(webhook_event_id)

    async with AsyncSessionLocal() as db:
        webhook = await db.scalar(
            select(BillingWebhookEvent)
            .where(BillingWebhookEvent.id == wid)
            .with_for_update()
        )
        if webhook is None or webhook.status == "processed":
            return
        payload = webhook.normalized_payload or {}
        custom = payload.get("custom") or {}
        order_id = payload.get("order_id")
        checkout_ref = custom.get("checkout_ref")

        # Locate the attempt by checkout_ref (the ownership key) and lock it.
        attempt: BillingCheckoutAttempt | None = None
        if checkout_ref:
            attempt = await db.scalar(
                select(BillingCheckoutAttempt)
                .where(BillingCheckoutAttempt.checkout_ref == checkout_ref)
                .with_for_update()
            )

        reason = _order_created_rejection(payload, custom, attempt)
        if reason is not None:
            webhook.status = "processed"
            webhook.processed_at = _now()
            await db.commit()
            # A rejected order that the provider reports as *paid* is money we took
            # but could not turn into credits — the unprovable-paid-checkout path
            # that needs an admin correction (T-302). Count it so it is alertable;
            # a non-paid rejection (e.g. status_not_paid) is benign and excluded.
            if payload.get("status") == "paid":
                BILLING_UNRECOVERABLE_CHECKOUT.labels(provider="lemonsqueezy").inc()
            logger.warning(
                "billing.order_created.rejected",
                reason=reason,
                order_id=order_id,
                checkout_ref=checkout_ref,
                webhook_event_id=str(wid),
            )
            return

        # attempt is non-None and proof-matched past this point.
        user_id = attempt.user_id
        credits = attempt.credits
        price_cents = attempt.price_cents
        currency = attempt.currency
        validity_days = attempt.validity_days
        provider_checkout_id = attempt.provider_checkout_id

        # Lock the user row (canonical user→pack order shared with deduct/expire).
        user = await db.scalar(select(User).where(User.id == user_id).with_for_update())
        if user is None:  # FK guarantees this never happens; fail loud if it does.
            raise RuntimeError(f"user {user_id} missing for granted attempt")

        # Idempotency pre-check under the lock: a prior delivery already granted.
        existing = await _find_existing_pack(
            db,
            order_id=order_id,
            provider_checkout_id=provider_checkout_id,
            provider="lemonsqueezy",
        )
        if existing is not None:
            attempt.status = "completed"
            if attempt.completed_at is None:
                attempt.completed_at = _now()
            if attempt.provider_order_id is None:
                attempt.provider_order_id = order_id
            webhook.status = "processed"
            webhook.processed_at = _now()
            await db.commit()
            logger.info(
                "billing.order_created.duplicate",
                order_id=order_id,
                pack_id=str(existing.id),
                webhook_event_id=str(wid),
            )
            return

        # Create the pack from the ATTEMPT SNAPSHOT (not live config — DC6).
        purchased_at = _parse_order_timestamp(payload.get("created_at"))
        customer_id = payload.get("customer_id")
        variant_id = payload.get("variant_id")
        pack = BillingCreditPack(
            user_id=user_id,
            provider="lemonsqueezy",
            provider_checkout_id=provider_checkout_id,
            provider_order_id=order_id,
            provider_customer_id=None if customer_id is None else str(customer_id),
            provider_variant_id=None if variant_id is None else str(variant_id),
            credits_purchased=credits,
            credits_remaining=credits,
            price_cents=price_cents,
            currency=currency,
            paid_item_amount_cents=price_cents,
            provider_order_total_cents=_coerce_int(payload.get("order_total_cents")),
            status="active",
            purchased_at=purchased_at,
            expires_at=purchased_at + timedelta(days=validity_days),
        )
        db.add(pack)

        try:
            # flush surfaces a (provider, provider_order_id|checkout_id) uniqueness
            # conflict (a racing duplicate that committed first) before we grant.
            await db.flush()
            granted = await credit_service.grant_credits_with_debt_recovery(
                db,
                user_id=user_id,
                pack=pack,
                granted_credits=credits,
                ledger_reason=f"billing_purchase:lemonsqueezy:{order_id}",
            )
        except IntegrityError:
            # A concurrent delivery won the insert; roll back this poisoned tx and
            # acknowledge the redelivery in a fresh session. No second grant.
            await db.rollback()
            await _ack_order_processed(wid)
            logger.info(
                "billing.order_created.duplicate_conflict",
                order_id=order_id,
                webhook_event_id=str(wid),
            )
            return

        if granted is None:
            # The ledger-reason index rejected a duplicate grant; grant()'s SAVEPOINT
            # rolled back its own rows but our pre-grant pack flush is still pending —
            # never commit it. Roll back and ack the duplicate.
            await db.rollback()
            await _ack_order_processed(wid)
            logger.info(
                "billing.order_created.duplicate_ledger",
                order_id=order_id,
                webhook_event_id=str(wid),
            )
            return

        attempt.status = "completed"
        attempt.completed_at = _now()
        # T-296 contract: GET /billing/status resolves the granted pack through
        # (attempt.provider, attempt.provider_order_id) and 404s while the order
        # id is NULL — without this stamp a completed Lemon purchase polls as
        # pending forever (the Razorpay handler has stamped it from day one).
        attempt.provider_order_id = order_id
        webhook.status = "processed"
        webhook.processed_at = _now()
        await db.commit()

    # Post-commit: evict the credit-balance cache, then record provider-labelled
    # telemetry (T-304); provider context also rides in the structured log.
    await credit_service.invalidate(user_id)
    BILLING_CHECKOUT_COMPLETED.labels(provider="lemonsqueezy").inc()
    BILLING_CREDITS_GRANTED.labels(provider="lemonsqueezy").inc(credits)
    BILLING_PURCHASE_REVENUE_CENTS.labels(provider="lemonsqueezy").inc(price_cents)
    # Debt repaid out of this grant (granted − surplus). Emitted post-commit so a
    # failed outer commit can never leave an over-counted recovery metric.
    recovered = credits - granted.amount
    if recovered > 0:
        BILLING_CREDIT_DEBT_RECOVERED.labels(provider="lemonsqueezy").inc(recovered)
    logger.info(
        "billing.order_created.granted",
        provider="lemonsqueezy",
        order_id=order_id,
        user_id=str(user_id),
        pack_id=str(pack.id),
        credits_granted=credits,
        paid_item_cents=price_cents,
    )


# ---------------------------------------------------------------------------
# order_refunded / fraud — proportional revocation + recoverable debt (T-300)
# ---------------------------------------------------------------------------
#
# `order_refunded` covers full and partial refunds; Lemon `fraudulent` (a chargeback
# the Merchant of Record absorbs) is a full-revocation input on the same event. The
# pack is found ONLY by `(provider, provider_order_id)` — never inferred from custom
# data. The money math (tax-normalised floor, monotonic processed level, single-
# ordered pack lock, recoverable debt) lives in `credit_service.apply_refund_reversal`
# so it is co-located with `deduct`/`grant`; this handler owns event routing, the
# no-pack proof branches, and the inbox-row state machine.


def _custom_user_id(custom: dict) -> UUID | None:
    """Parse the sanitised custom ``user_id`` to a UUID, or None if absent/malformed."""
    try:
        return UUID(str(custom.get("user_id")))
    except (ValueError, TypeError):
        return None


async def handle_order_refunded(ctx: dict, webhook_event_id: str) -> None:
    """Apply a refund / fraud reversal exactly once, or route the no-pack branches.

    Idempotent: the monotonic ``refunded_item_amount_cents_processed`` gate plus the
    two-component ``refund:billing:{pack_id}:{cents}`` ledger reason make a replay of
    the same or a lower refund level a no-op (durable processed state only).
    """
    wid = UUID(webhook_event_id)

    async with AsyncSessionLocal() as db:
        webhook = await db.scalar(
            select(BillingWebhookEvent)
            .where(BillingWebhookEvent.id == wid)
            .with_for_update()
        )
        if webhook is None or webhook.status == "processed":
            return
        payload = webhook.normalized_payload or {}
        custom = payload.get("custom") or {}
        order_id = payload.get("order_id")
        order_status = payload.get("status")
        refunded_amount = _coerce_int(payload.get("refunded_amount_cents")) or 0
        full_or_fraud, reason_label = _reversal_decision(order_status)

        # The pack is found ONLY by provider order id (a signed refund with no custom
        # data still revokes). No lock here — apply_refund_reversal re-locks under the
        # canonical order.
        source_pack = await db.scalar(
            select(BillingCreditPack)
            .where(
                BillingCreditPack.provider == "lemonsqueezy",
                BillingCreditPack.provider_order_id == order_id,
            )
            .limit(1)
        )

        if source_pack is None:
            await _refund_without_pack(db, webhook, custom, order_id)
            return

        user_id = source_pack.user_id
        outcome = await credit_service.apply_refund_reversal(
            db,
            source_pack=source_pack,
            provider_refunded_amount_cents=refunded_amount,
            full_or_fraud=full_or_fraud,
            reason_label=reason_label,
        )
        webhook.status = "processed"
        webhook.processed_at = _now()
        await db.commit()

    # Post-commit: evict the credit-balance cache, then record telemetry.
    await credit_service.invalidate(user_id)
    if outcome.credits_revoked > 0:
        BILLING_CREDITS_REVOKED.labels(
            provider="lemonsqueezy", reason=reason_label
        ).inc(outcome.credits_revoked)
    if outcome.debt_created > 0:
        BILLING_CREDIT_DEBT_CREATED.labels(
            provider="lemonsqueezy", reason=reason_label
        ).inc(outcome.debt_created)
    logger.info(
        "billing.order_refunded.processed",
        provider="lemonsqueezy",
        order_id=order_id,
        user_id=str(user_id),
        reason=reason_label,
        new_refunded_item_cents=outcome.new_refunded_item_cents,
        credits_revoked=outcome.credits_revoked,
        immediate_revoke=outcome.immediate_revoke,
        debt_created=outcome.debt_created,
        applied=outcome.applied,
    )


async def _refund_without_pack(
    db, webhook: BillingWebhookEvent, custom: dict, order_id: str | None
) -> None:
    """Resolve a refund whose pack does not (yet) exist — never revoke other credits.

    (a) valid ``checkout_ref`` + nonce proof and not yet given up → re-queue the row as
        ``received`` (off the dead-letter budget) so a delayed ``order_created`` grants
        the pack first and the next attempt revokes it. **Refinement of the spec's
        literal "keep failed":** ``failed`` rows stop being swept once ``retry_count``
        reaches ``JOB_MAX_TRIES`` (5), which is minutes — far short of the 24h horizon;
        ``received`` rows are re-enqueued unconditionally by the 60s sweep, so resetting
        to ``received`` is the only channel that actually retries for 24h.
    (b) the proven attempt has expired and 24h have elapsed since first receipt → give
        up as a ``processed`` audited no-op (no balance change).
    (c) no ownership proof → ``processed`` audited no-op ("could not link"); a forged or
        unrelated order can never revoke Thought2Build credits.
    """
    checkout_ref = custom.get("checkout_ref")
    nonce_hash = custom.get("checkout_nonce_hash_from_webhook")
    custom_user_id = _custom_user_id(custom)

    attempt = None
    if checkout_ref:
        attempt = await db.scalar(
            select(BillingCheckoutAttempt).where(
                BillingCheckoutAttempt.checkout_ref == checkout_ref
            )
        )
    proof_valid = (
        attempt is not None
        and nonce_hash is not None
        and attempt.checkout_nonce_hash == nonce_hash
        and custom_user_id is not None
        and custom_user_id == attempt.user_id
    )

    if not proof_valid:
        # (c) no proof — audited no-op, never touch unrelated credits.
        webhook.status = "processed"
        webhook.processed_at = _now()
        webhook.last_error = "refund_could_not_link_to_thought2build"
        await db.commit()
        logger.warning(
            "billing.order_refunded.unlinked",
            order_id=order_id,
            checkout_ref=checkout_ref,
            webhook_event_id=str(webhook.id),
        )
        return

    now = _now()
    attempt_expired = attempt.status == "expired" or attempt.expires_at < now
    elapsed = now - webhook.received_at
    if attempt_expired and elapsed > timedelta(hours=_REFUND_GIVE_UP_HOURS):
        # (b) the order_created will never arrive — give up as an audited no-op.
        webhook.status = "processed"
        webhook.processed_at = now
        webhook.last_error = "refund_pack_never_granted_attempt_expired"
        await db.commit()
        logger.warning(
            "billing.order_refunded.pack_never_granted",
            order_id=order_id,
            checkout_ref=checkout_ref,
            webhook_event_id=str(webhook.id),
        )
        return

    # (a) the grant is plausibly just delayed — re-queue for a later attempt. The row
    # stays 'received' but is tagged so the 60s sweep skips it; the 15-minute reconcile
    # lane 1 re-drives it until the grant lands or the 24h give-up fires above.
    webhook.status = "received"
    webhook.last_error = _PACK_NOT_YET_GRANTED
    await db.commit()
    logger.info(
        "billing.order_refunded.pack_not_yet_granted",
        order_id=order_id,
        checkout_ref=checkout_ref,
        webhook_event_id=str(webhook.id),
    )


# ---------------------------------------------------------------------------
# Razorpay payment_link.paid — grant credits for a verified paid link (issue #44)
# ---------------------------------------------------------------------------
#
# The Razorpay analogue of handle_order_created. Validation and the grant are
# anchored to the checkout-attempt SNAPSHOT (never live RAZORPAY_* config — DC6):
# an in-flight price change must not break or mis-price a purchase. Ownership is
# proven ONLY by notes.checkout_ref + the stored nonce hash + notes.user_id — never
# inferred from the payment id. The money authority is the PAYMENT entity (D10): the
# link's amount is a value Thought2Build set at creation, so it is corroboration only.
# The normalized inbox payload uses the ``notes`` block (not Lemon's ``custom``) and
# a top-level ``payment_id`` (the pay_… id → provider_order_id, D7).


def _razorpay_link_paid_rejection(
    payload: dict, notes: dict, attempt: BillingCheckoutAttempt | None
) -> str | None:
    """Run the payment_link.paid validation checklist; reason to reject, or None.

    Every term is terminal on failure (no retry) — the webhook is marked processed
    with a sanitised warning so a config/proof error is alertable rather than silent.
    """
    if attempt is None:
        return "attempt_not_found"
    if attempt.provider != "razorpay":
        return "provider_mismatch"
    # Ownership: notes.checkout_ref already loaded the attempt; corroborate the
    # nonce hash + user id. Never inferred from the payment id.
    raw_user_id = notes.get("user_id")
    try:
        notes_user_id = UUID(str(raw_user_id))
    except (ValueError, TypeError):
        return "user_id_invalid"
    if notes_user_id != attempt.user_id:
        return "user_id_mismatch"
    if notes.get("checkout_nonce_hash_from_webhook") != attempt.checkout_nonce_hash:
        return "nonce_mismatch"
    # Local checkout expiry stops new checkout initiation, not settlement of a
    # captured payment. Expired/failed attempts still require all the proof below.
    if attempt.status not in _GRANTABLE_ATTEMPT_STATUSES | {"expired", "failed"}:
        return "attempt_status_invalid"
    # Environment (test/live). Razorpay events carry no test_mode flag — the
    # round-tripped notes.environment is the guard (re-checked against live config
    # here, defence-in-depth beyond the webhook receiver's ingestion check).
    if notes.get("environment") != razorpay_environment():
        return "environment_mismatch"
    # Payment-entity anchor (D10) — the money object is the authority.
    payment = payload.get("payment") or {}
    if _coerce_int(payment.get("amount")) != attempt.price_cents:
        return "payment_amount_mismatch"
    payment_currency = str(payment.get("currency") or "")
    if payment_currency.upper() != attempt.currency.upper():
        return "payment_currency_mismatch"
    if payment.get("status") not in ("captured", "refunded"):
        return "payment_not_captured"
    if payment.get("status") == "refunded" and (
        (_coerce_int(payment.get("amount_refunded")) or 0) < attempt.price_cents
    ):
        return "refunded_payment_missing_total"
    # Link corroboration — accept_partial is off, so the full amount must match and
    # the link must be paid (the status_not_paid analogue).
    link = payload.get("payment_link") or {}
    if link.get("reference_id") != str(attempt.id):
        return "link_reference_mismatch"
    if not link.get("payment_link_id"):
        return "link_id_missing"
    if (
        attempt.provider_checkout_id
        and link.get("payment_link_id") != attempt.provider_checkout_id
    ):
        return "link_id_mismatch"
    if link.get("order_id") and payment.get("order_id") != link["order_id"]:
        return "payment_order_mismatch"
    if _coerce_int(link.get("amount")) != attempt.price_cents:
        return "link_amount_mismatch"
    if link.get("status") != "paid":
        return "link_status_not_paid"
    return None


async def handle_razorpay_link_paid(ctx: dict, webhook_event_id: str) -> None:
    """Grant credits for a verified, proof-matched ``payment_link.paid`` (issue #44).

    Idempotent: a duplicate delivery (or a redelivered inbox row with a different
    ``payload_hash``) reloads the existing pack and writes no second grant — enforced
    by the ``(provider, provider_order_id)`` / ``(provider, provider_checkout_id)``
    pack uniqueness and the ``billing_purchase:razorpay:{payment_id}`` ledger index.
    """
    wid = UUID(webhook_event_id)

    async with AsyncSessionLocal() as db:
        webhook = await db.scalar(
            select(BillingWebhookEvent)
            .where(BillingWebhookEvent.id == wid)
            .with_for_update()
        )
        if webhook is None or webhook.status == "processed":
            return
        payload = webhook.normalized_payload or {}
        notes = payload.get("notes") or {}
        link = payload.get("payment_link") or {}
        payment_id = payload.get("payment_id")
        checkout_ref = notes.get("checkout_ref")

        from services.razorpay_settlement import (
            known_refund_cents,
            lock_payment,
            settle_pack,
        )

        await lock_payment(db, payment_id)

        # Locate the attempt by checkout_ref (the ownership key) and lock it.
        attempt: BillingCheckoutAttempt | None = None
        if checkout_ref:
            attempt = await db.scalar(
                select(BillingCheckoutAttempt)
                .where(BillingCheckoutAttempt.checkout_ref == checkout_ref)
                .with_for_update()
            )

        reason = _razorpay_link_paid_rejection(payload, notes, attempt)
        if reason is not None:
            webhook.status = "processed"
            webhook.last_error = reason
            webhook.processed_at = _now()
            await db.commit()
            # A rejected link the provider reports as *paid* is money we took but
            # could not turn into credits — the unprovable-paid-checkout path that
            # needs an admin correction (T-302). The "provider says paid" predicate
            # is payment_link.status == "paid" (D10 analogue of Lemon's status).
            if link.get("status") == "paid":
                BILLING_UNRECOVERABLE_CHECKOUT.labels(provider="razorpay").inc()
            logger.warning(
                "billing.razorpay_link_paid.rejected",
                reason=reason,
                payment_id=payment_id,
                checkout_ref=checkout_ref,
                webhook_event_id=str(wid),
            )
            return

        # attempt is non-None and proof-matched past this point.
        user_id = attempt.user_id
        credits = attempt.credits
        # Anchor money fields on the attempt snapshot (== the validated payment
        # amount, D10). provider_order_total_cents is populated explicitly: the INR
        # price is tax-inclusive, so item and order total coincide (no Lemon-style
        # tax on top) — apply_refund_reversal normalises the refund through it.
        price_cents = attempt.price_cents
        currency = attempt.currency
        validity_days = attempt.validity_days
        provider_checkout_id = attempt.provider_checkout_id or link["payment_link_id"]
        attempt.provider_checkout_id = provider_checkout_id

        # Lock the user row (canonical user→pack order shared with deduct/expire).
        user = await db.scalar(select(User).where(User.id == user_id).with_for_update())
        if user is None:  # FK guarantees this never happens; fail loud if it does.
            raise RuntimeError(f"user {user_id} missing for granted attempt")

        # Idempotency pre-check under the lock: a prior delivery already granted.
        existing = await _find_existing_pack(
            db,
            order_id=payment_id,
            provider_checkout_id=provider_checkout_id,
            provider="razorpay",
        )
        if existing is not None:
            attempt.status = "completed"
            if attempt.completed_at is None:
                attempt.completed_at = _now()
            if attempt.provider_order_id is None:
                attempt.provider_order_id = payment_id
            webhook.status = "processed"
            webhook.processed_at = _now()
            if existing.user_id != user_id or existing.provider_order_id != payment_id:
                raise ValueError("Existing payment pack identity mismatch")
            settlement = await settle_pack(
                db,
                existing,
                max(
                    _coerce_int((payload.get("payment") or {}).get("amount_refunded"))
                    or 0,
                    await known_refund_cents(db, payment_id),
                ),
            )
            await db.commit()
            settlement.record()
            await credit_service.invalidate(user_id)
            logger.info(
                "billing.razorpay_link_paid.duplicate",
                payment_id=payment_id,
                pack_id=str(existing.id),
                webhook_event_id=str(wid),
            )
            return

        # Create the pack from the ATTEMPT SNAPSHOT (not live config — DC6). The
        # payment timestamp anchors expiry even after a delayed webhook/recovery.
        payment_created_at = _coerce_int(
            (payload.get("payment") or {}).get("created_at")
        )
        purchased_at = (
            datetime.fromtimestamp(payment_created_at, tz=UTC)
            if payment_created_at
            else _now()
        )
        pack = BillingCreditPack(
            user_id=user_id,
            provider="razorpay",
            provider_checkout_id=provider_checkout_id,
            provider_order_id=payment_id,
            provider_customer_id=None,
            provider_variant_id=None,
            credits_purchased=credits,
            credits_remaining=credits,
            price_cents=price_cents,
            currency=currency,
            paid_item_amount_cents=price_cents,
            provider_order_total_cents=price_cents,
            status="active",
            purchased_at=purchased_at,
            expires_at=purchased_at + timedelta(days=validity_days),
        )
        db.add(pack)

        try:
            # flush surfaces a (provider, provider_order_id|checkout_id) uniqueness
            # conflict (a racing duplicate that committed first) before we grant.
            await db.flush()
            granted = await credit_service.grant_credits_with_debt_recovery(
                db,
                user_id=user_id,
                pack=pack,
                granted_credits=credits,
                ledger_reason=f"billing_purchase:razorpay:{payment_id}",
            )
        except IntegrityError:
            # A concurrent delivery won the insert; roll back this poisoned tx and
            # acknowledge the redelivery in a fresh session. No second grant.
            await db.rollback()
            await _ack_order_processed(wid)
            logger.info(
                "billing.razorpay_link_paid.duplicate_conflict",
                payment_id=payment_id,
                webhook_event_id=str(wid),
            )
            return

        if granted is None:
            # The ledger-reason index rejected a duplicate grant; grant()'s SAVEPOINT
            # rolled back its own rows but our pre-grant pack flush is still pending —
            # never commit it. Roll back and ack the duplicate.
            await db.rollback()
            await _ack_order_processed(wid)
            logger.info(
                "billing.razorpay_link_paid.duplicate_ledger",
                payment_id=payment_id,
                webhook_event_id=str(wid),
            )
            return

        attempt.status = "completed"
        attempt.completed_at = _now()
        attempt.provider_order_id = payment_id
        webhook.status = "processed"
        webhook.processed_at = _now()
        refund_cents = max(
            _coerce_int((payload.get("payment") or {}).get("amount_refunded")) or 0,
            await known_refund_cents(db, payment_id),
        )
        settlement = await settle_pack(db, pack, refund_cents)
        await db.commit()
        settlement.record()

    # Post-commit: evict the credit-balance cache, then record provider-labelled
    # telemetry; provider context also rides in the structured log.
    await credit_service.invalidate(user_id)
    BILLING_CHECKOUT_COMPLETED.labels(provider="razorpay").inc()
    BILLING_CREDITS_GRANTED.labels(provider="razorpay").inc(credits)
    BILLING_PURCHASE_REVENUE_CENTS.labels(provider="razorpay").inc(price_cents)
    # Debt repaid out of this grant (granted − surplus). Emitted post-commit so a
    # failed outer commit can never leave an over-counted recovery metric.
    recovered = credits - granted.amount
    if recovered > 0:
        BILLING_CREDIT_DEBT_RECOVERED.labels(provider="razorpay").inc(recovered)
    logger.info(
        "billing.razorpay_link_paid.granted",
        provider="razorpay",
        payment_id=payment_id,
        user_id=str(user_id),
        pack_id=str(pack.id),
        credits_granted=credits,
        paid_item_cents=price_cents,
    )


# ---------------------------------------------------------------------------
# Razorpay refund.processed — proportional revocation + recoverable debt (issue #44)
# ---------------------------------------------------------------------------
#
# The Razorpay analogue of handle_order_refunded. The pack is found ONLY by
# `(provider='razorpay', provider_order_id=payment_id)` — never inferred from notes.
# The cumulative `amount_refunded` on the PAYMENT entity drives the same proportional
# reversal as Lemon's `refunded_amount`, so partial + full refunds settle correctly
# and re-deliveries are no-ops. Razorpay payments carry NO fraud/chargeback status
# (disputes are separate entities — Plan §11), so reason is always "refund" and lane
# 2 reads refunds; the dedicated dispute handler settles chargebacks.


async def handle_razorpay_refund(ctx: dict, webhook_event_id: str) -> None:
    """Settle cumulative refunds and retain unmatched signed reversal evidence.

    The payment lock serializes grant/refund/dispute transactions; settlement's
    cumulative gate and the durable inbox make redelivery idempotent.
    """
    wid = UUID(webhook_event_id)

    async with AsyncSessionLocal() as db:
        webhook = await db.scalar(
            select(BillingWebhookEvent)
            .where(BillingWebhookEvent.id == wid)
            .with_for_update()
        )
        if webhook is None or webhook.status == "processed":
            return
        payload = webhook.normalized_payload or {}
        payment_id = payload.get("payment_id")
        payment = payload.get("payment") or {}
        from services.razorpay_settlement import lock_payment, settle_pack

        await lock_payment(db, payment_id)
        # Cumulative refunded total on the payment entity (same semantics as Lemon's
        # refunded_amount). Coerced defensively: a refund arriving without a full
        # payment entity degrades to a monotonic no-op (0), and lane 2's get_payment
        # re-read is the backstop that catches the real cumulative later — never a
        # spurious over-revocation.
        amount_refunded = _coerce_int(payment.get("amount_refunded")) or 0
        payment_amount = _coerce_int(payment.get("amount")) or 0
        refund_status = str(payment.get("refund_status") or "")
        full_or_fraud = refund_status == "full" or (
            payment_amount > 0 and amount_refunded >= payment_amount
        )
        reason_label = "refund"  # Razorpay has no fraud/chargeback status on payments

        # The pack is found ONLY by provider order id (the payment id, D7) — a signed
        # refund with no notes still revokes. No lock here — apply_refund_reversal
        # re-locks under the canonical order.
        source_pack = await db.scalar(
            select(BillingCreditPack)
            .where(
                BillingCreditPack.provider == "razorpay",
                BillingCreditPack.provider_order_id == payment_id,
            )
            .limit(1)
        )

        if source_pack is None:
            # Keep proof even without link notes. A future grant consults these
            # inbox rows while holding the same payment lock before committing.
            webhook.status = "processed"
            webhook.processed_at = _now()
            webhook.last_error = "refund_waiting_for_payment_grant"
            await db.commit()
            return

        user_id = source_pack.user_id
        settlement = await settle_pack(
            db, source_pack, payment_amount if full_or_fraud else amount_refunded
        )
        webhook.status = "processed"
        webhook.processed_at = _now()
        await db.commit()
        settlement.record()

    # Post-commit: evict the credit-balance cache, then record telemetry.
    await credit_service.invalidate(user_id)
    logger.info(
        "billing.razorpay_refund.processed",
        provider="razorpay",
        payment_id=payment_id,
        user_id=str(user_id),
        reason=reason_label,
        refunded_cents=amount_refunded,
    )


async def handle_razorpay_dispute(ctx: dict, webhook_event_id: str) -> None:
    from models import BillingDispute
    from services.razorpay_settlement import lock_payment, settle_pack

    async with AsyncSessionLocal() as db:
        webhook = await db.scalar(
            select(BillingWebhookEvent)
            .where(
                BillingWebhookEvent.id == UUID(webhook_event_id),
            )
            .with_for_update()
        )
        if webhook is None or webhook.status == "processed":
            return
        payload = webhook.normalized_payload
        payment_id = payload["payment_id"]
        await lock_payment(db, payment_id)
        dispute = payload["dispute"]
        row = await db.get(BillingDispute, dispute["dispute_id"])
        timestamp = payload["event_created_at"]
        state = dispute["status"]
        if state == "closed" and dispute["amount_deducted"] > 0:
            state = "closed_loss"
        if row and (
            row.provider_payment_id != payment_id or row.currency != dispute["currency"]
        ):
            raise ValueError("Dispute payment or currency changed")
        terminal = {"lost", "won", "closed", "closed_loss"}
        stale = row and (
            timestamp < row.event_created_at
            or (row.status in terminal and state not in terminal)
            or (
                timestamp == row.event_created_at
                and row.status in terminal
                and row.status == state
            )
        )
        if (
            row
            and not stale
            and timestamp == row.event_created_at
            and row.status in terminal
            and state in terminal
        ):
            current = await razorpay_service.get_dispute(dispute["dispute_id"])
            if (
                current.get("payment_id") != payment_id
                or current.get("currency") != dispute["currency"]
            ):
                raise ValueError("Dispute API identity mismatch")
            state = current["status"]
            dispute = {
                **dispute,
                "amount": int(current["amount"]),
                "amount_deducted": int(current.get("amount_deducted") or 0),
            }
            if state == "closed" and dispute["amount_deducted"] > 0:
                state = "closed_loss"
        settlement = None
        if not stale:
            if row is None:
                row = BillingDispute(
                    provider_dispute_id=dispute["dispute_id"],
                    provider_payment_id=payment_id,
                )
                db.add(row)
            row.amount_cents = (
                dispute["amount_deducted"]
                if state == "closed_loss"
                else dispute["amount"]
            )
            row.currency = dispute["currency"]
            row.status = state
            row.event_created_at = timestamp
            await db.flush()
            pack = await db.scalar(
                select(BillingCreditPack).where(
                    BillingCreditPack.provider == "razorpay",
                    BillingCreditPack.provider_order_id == payment_id,
                )
            )
            if pack is not None:
                if pack.currency != row.currency or row.amount_cents > pack.price_cents:
                    raise ValueError("Dispute economics do not match payment")
                settlement = await settle_pack(
                    db,
                    pack,
                    int((payload.get("payment") or {}).get("amount_refunded") or 0),
                )
        webhook.status = "processed"
        webhook.processed_at = _now()
        await db.commit()
    if settlement is not None:
        settlement.record()
        await credit_service.invalidate(pack.user_id)


# Wire the handlers at import so they are present whenever the worker (or the dispatch
# path) loads this module. Handlers dispatch by (provider, event_name) through the
# neutral pipeline: Lemon order events (T-299/T-300) + Razorpay money events (issue
# #44). The late-Stripe grace handlers were removed with the Stripe runtime
# decommission (T-308).
register_event_handler("lemonsqueezy", "order_created", handle_order_created)
register_event_handler("lemonsqueezy", "order_refunded", handle_order_refunded)
register_event_handler("razorpay", "payment_link.paid", handle_razorpay_link_paid)
register_event_handler("razorpay", "refund.processed", handle_razorpay_refund)
for _dispute_state in (
    "created",
    "under_review",
    "action_required",
    "lost",
    "won",
    "closed",
):
    register_event_handler(
        "razorpay", f"payment.dispute.{_dispute_state}", handle_razorpay_dispute
    )
