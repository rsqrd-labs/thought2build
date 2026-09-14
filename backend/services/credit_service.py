from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_shared_redis
from models import (
    BillingCreditDebt,
    BillingCreditPack,
    CreditLedger,
    CreditPackAllocation,
    User,
)
from services.observability import (
    BILLING_CREDITS_CONSUMED,
    BILLING_CREDITS_EXPIRED,
)

logger = logging.getLogger(__name__)

_CACHE_PREFIX = "credits:"
CREDIT_COSTS = {
    "generate": 10,
    "refine": 3,
    "regenerate": 10,
    "chat": 2,
    "export": 0,
}


class InsufficientCreditsError(Exception):
    pass


@dataclass(frozen=True)
class RefundOutcome:
    """Result of :meth:`CreditService.apply_refund_reversal` (Phase 22 — T-300).

    ``applied`` is True only when a revocation ledger row was written (i.e.
    ``credits_revoked > 0``); the monotonic-replay and zero-effective branches
    advance the pack's processed fields and return ``applied=False`` with all
    credit counters at 0. The caller commits and emits the metrics.
    """

    new_refunded_item_cents: int
    credits_revoked: int
    immediate_revoke: int
    debt_created: int
    applied: bool


class CreditService:
    def __init__(self, redis_client: Redis | None = None) -> None:
        self._redis: Redis | None = redis_client

    def _redis_key(self, user_id: UUID) -> str:
        return f"{_CACHE_PREFIX}{user_id}"

    async def _get_redis(self) -> Redis:
        # Use the constructor-injected client (for tests) or the shared pool.
        # H-1 — T-177.
        if self._redis is None:
            self._redis = get_shared_redis()
        return self._redis

    async def get_balance(self, db: AsyncSession, user_id: UUID) -> int:
        # Sweep expired packs before reading balance.  This keeps the DB balance
        # authoritative and ensures the Redis cache reflects post-expiry state.
        # T-229 — lazy expiry.
        await self._expire_user_packs(db, user_id)
        # The expiry sweep already locks and reads the authoritative user row.
        # Never publish uncommitted balances to Redis (the caller can roll back).
        user = await self._get_user(db, user_id)
        balance = int(user.credit_balance) if user is not None else 0
        return balance

    async def _expire_user_packs(self, db: AsyncSession, user_id: UUID) -> None:
        """Sweep active packs whose expires_at has passed.

        Runs inside the caller's transaction — do NOT call db.commit() here.
        Uses SELECT FOR UPDATE on both the user row and the pack rows to prevent
        concurrent get_balance() calls from racing each other.

        The user row lock is acquired FIRST (same order as deduct()) to avoid
        deadlocks with concurrent deduct() calls.
        """
        now = datetime.now(timezone.utc)
        # Lock user row first (consistent lock ordering with deduct()).
        user = await self._get_user(db, user_id, lock=True)
        if user is None:
            return
        # Lock and fetch expired active packs.
        result = await db.execute(
            select(BillingCreditPack)
            .where(
                BillingCreditPack.user_id == user_id,
                BillingCreditPack.status == "active",
                BillingCreditPack.expires_at <= now,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        expired_packs = result.scalars().all()
        if not expired_packs:
            return
        total_expired = sum(p.credits_remaining for p in expired_packs)
        for pack in expired_packs:
            # Move the lapsed remainder into credits_expired BEFORE zeroing it so
            # the conservation invariant (remaining + consumed + expired +
            # debt_recovered == purchased) holds across the lifecycle (T-294).
            pack.credits_expired += pack.credits_remaining
            pack.status = "expired"
            pack.credits_remaining = 0
        # Deduct expired credits from user balance (floor at 0 — defensive).
        user.credit_balance = max(0, int(user.credit_balance or 0) - total_expired)
        await db.flush()
        await self._invalidate(user_id)
        BILLING_CREDITS_EXPIRED.inc(total_expired)

    async def _drain_packs(
        self,
        db: AsyncSession,
        user_id: UUID,
        amount: int,
        ledger_entry: CreditLedger | None = None,
    ) -> None:
        """Drain credits_remaining from active packs in FIFO order.

        FIFO = soonest-expiring pack first (ORDER BY expires_at ASC).

        Called by deduct() AFTER recording the CreditLedger entry and updating
        user.credit_balance.  Pack rows must already be locked by
        _expire_user_packs() having acquired FOR UPDATE on the user row —
        use the same transaction.

        FIFO = ORDER BY expires_at ASC.  Never ORDER BY expires_at DESC.
        """
        if amount <= 0:
            return
        result = await db.execute(
            select(BillingCreditPack)
            .where(
                BillingCreditPack.user_id == user_id,
                BillingCreditPack.status == "active",
            )
            .order_by(
                BillingCreditPack.expires_at.asc()
            )  # FIFO: soonest-expiring first
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        packs = result.scalars().all()
        remaining = amount
        for pack in packs:
            if remaining <= 0:
                break
            drain = min(pack.credits_remaining, remaining)
            pack.credits_remaining -= drain
            # Track lifetime consumption so the conservation invariant holds and
            # refunds can compute the still-revocable amount (T-294).
            pack.credits_consumed += drain
            if drain and ledger_entry is not None:
                db.add(
                    CreditPackAllocation(
                        ledger_entry_id=ledger_entry.id,
                        pack_id=pack.id,
                        amount=drain,
                    )
                )
            remaining -= drain
            if pack.credits_remaining == 0:
                pack.status = "consumed"
        if ledger_entry is not None:
            ledger_entry.metadata_ = {
                "allocation_version": 1,
                "unallocated_credits": remaining,
            }
        await db.flush()
        BILLING_CREDITS_CONSUMED.inc(amount - remaining)

    async def _get_user(
        self,
        db: AsyncSession,
        user_id: UUID,
        *,
        lock: bool = False,
    ) -> User | None:
        statement = select(User).where(User.id == user_id)
        if lock:
            # populate_existing so a locked read refreshes an already-identity-mapped
            # row to committed DB state — a FOR UPDATE that returned a stale cached
            # instance would silently lose a concurrent writer's update (T-306).
            statement = statement.with_for_update().execution_options(
                populate_existing=True
            )
        result = await db.execute(statement)
        return result.scalar_one_or_none()

    async def credit(
        self,
        db: AsyncSession,
        user_id: UUID,
        amount: int,
        reason: str,
        metadata: dict | None = None,
    ) -> CreditLedger:
        user = await self._get_user(db, user_id, lock=True)
        if user is None:
            raise ValueError(f"User {user_id} not found")

        user.credit_balance = int(user.credit_balance or 0) + amount
        entry = CreditLedger(
            user_id=user_id, amount=amount, reason=reason, metadata_=metadata
        )
        db.add(entry)
        await db.flush()
        await self._invalidate(user_id)
        return entry

    async def grant_credits_with_debt_recovery(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        pack: BillingCreditPack,
        granted_credits: int,
        ledger_reason: str,
    ) -> CreditLedger | None:
        """Apply a positive billing grant, repaying pending debt before usable balance.

        The single grant path for every positive billing credit (purchase — T-299,
        admin correction — T-302). ``pack`` is the freshly-created, in-session
        ``BillingCreditPack`` for this order (``credits_remaining == granted_credits``);
        ``ledger_reason`` is the order's idempotency reason
        (``billing_purchase:…`` / ``admin_billing_correction:…``).

        Debt-first (Plan §25 DC5): any pending ``billing_credit_debts`` (a shortfall
        from an earlier reversal the user had already spent) is settled oldest-first
        out of the new pack BEFORE the user sees usable credits. Each settled slice
        moves ``take`` credits from ``pack.credits_remaining`` into
        ``pack.credits_debt_recovered`` and ``debt.credits_recovered`` (so the
        conservation invariant ``remaining + consumed + expired + debt_recovered ==
        purchased`` is preserved) and writes a zero-amount audit ledger row
        ``debt_recovery:billing:{debt_id}:{pack_id}``. Only the surplus
        (``granted_credits - total_recovered``) increases ``user.credit_balance`` and
        is recorded by the single ``ledger_reason`` row (amount may be 0).

        Lock order is the canonical user → (debts) so it never deadlocks with
        deduct/expire (which lock user → packs). The whole mutation set is wrapped in
        a SAVEPOINT: a duplicate grant (same order) collides on the ledger-reason
        unique index and is rolled back to an idempotent no-op, leaving the caller's
        outer transaction intact (mirrors ``refund()``).

        Returns the surplus ``CreditLedger`` row, or ``None`` if the grant was a
        duplicate (already applied). The caller commits and then calls
        ``invalidate(user_id)``.
        """
        # Lock the user row first (canonical order shared with deduct/expire).
        user = await self._get_user(db, user_id, lock=True)
        if user is None:
            raise ValueError(f"User {user_id} not found")

        # Lock pending debts oldest-first so concurrent grants settle deterministically.
        debts_result = await db.execute(
            select(BillingCreditDebt)
            .where(
                BillingCreditDebt.user_id == user_id,
                BillingCreditDebt.status == "pending",
            )
            .order_by(BillingCreditDebt.created_at.asc())
            .with_for_update()
        )
        pending_debts = debts_result.scalars().all()

        surplus_entry = CreditLedger(
            user_id=user_id,
            amount=0,  # finalised below once total_recovered is known
            reason=ledger_reason,
        )
        try:
            async with db.begin_nested():
                grant_left = granted_credits
                total_recovered = 0
                for debt in pending_debts:
                    if grant_left <= 0:
                        break
                    outstanding = debt.credits_owed - debt.credits_recovered
                    take = min(grant_left, outstanding)
                    if take <= 0:
                        continue
                    pack.credits_remaining -= take
                    pack.credits_debt_recovered += take
                    debt.credits_recovered += take
                    if debt.credits_recovered >= debt.credits_owed:
                        debt.status = "recovered"
                    debt.updated_at = datetime.now(timezone.utc)
                    recovery_entry = CreditLedger(
                        id=uuid4(),
                        user_id=user_id,
                        amount=0,
                        reason=f"debt_recovery:billing:{debt.id}:{pack.id}",
                    )
                    db.add(recovery_entry)
                    await db.flush()
                    db.add(
                        CreditPackAllocation(
                            ledger_entry_id=recovery_entry.id,
                            pack_id=pack.id,
                            recovery_for_pack_id=debt.source_pack_id,
                            amount=take,
                        )
                    )
                    grant_left -= take
                    total_recovered += take

                surplus = granted_credits - total_recovered
                surplus_entry.amount = surplus
                user.credit_balance = int(user.credit_balance or 0) + surplus
                db.add(surplus_entry)
                await db.flush()
        except IntegrityError:
            # The ledger-reason unique index rejected a duplicate grant for this
            # order; the SAVEPOINT rolled back every mutation above (pack/debt/
            # balance + ledger rows), leaving the outer transaction intact. Treat
            # as idempotent — the credits were already granted.
            logger.warning(
                "credit.grant.duplicate user_id=%s reason=%s",
                user_id,
                ledger_reason,
            )
            return None

        # NOTE: the BILLING_CREDIT_DEBT_RECOVERED metric is emitted by the CALLER
        # after its outer commit (the recovered amount = granted_credits − surplus),
        # not here — this method does not commit, so emitting mid-transaction would
        # over-count if the caller's commit later fails.
        await self._invalidate(user_id)
        return surplus_entry

    async def apply_refund_reversal(
        self,
        db: AsyncSession,
        *,
        source_pack: BillingCreditPack,
        provider_refunded_amount_cents: int,
        full_or_fraud: bool,
        reason_label: str,
        ledger_reason: str | None = None,
    ) -> RefundOutcome:
        """Apply a tax-normalised, proportional, exactly-once refund/fraud reversal.

        The shared credit-mutation half of the T-300 money path (the billing worker
        owns event routing + idempotency). ``source_pack`` is the pack found by
        ``(provider, provider_order_id)``; ``provider_refunded_amount_cents`` is the
        provider's cumulative refunded amount (may include tax — Lemon
        ``refunded_amount`` / Razorpay ``amount_refunded``, issue #44);
        ``full_or_fraud`` is the worker's amount-primary full-refund decision;
        ``reason_label`` is the
        metric/debt reason (``refund`` / ``fraud``).

        Normalisation + credit math are verbatim from Plan §25.6 T-300. Locking is
        byte-for-byte identical to :meth:`deduct` (expire sweep → lock user → lock all
        active packs ``ORDER BY expires_at ASC FOR UPDATE``, source pack included) so a
        concurrent ``deduct`` can never deadlock. The revocation drains the source
        pack's ``credits_remaining`` first, then other active packs FIFO (the drained
        slices land in their ``credits_debt_recovered`` so the conservation invariant
        holds), decrements ``users.credit_balance`` by the immediately drained amount
        only (never below zero), and turns any still-unrecoverable remainder into a
        recoverable ``billing_credit_debts`` row on the source pack. **Expired credits
        are never converted to debt** (``refundable_value = purchased − expired``).

        Idempotency is the caller's ledger-reason barrier plus this method's monotonic
        ``refunded_item_amount_cents_processed`` gate: a replay of the same or a lower
        refund level only advances the seen-total and returns ``applied=False``. Does
        not commit; the caller commits and invalidates the credit cache.
        """
        user_id = source_pack.user_id

        # Canonical lock order (identical to deduct): sweep lapsed expiry first so
        # credits_expired is current, then lock the user, then every active pack +
        # the source pack in one ORDER BY expires_at ASC FOR UPDATE.
        await self._expire_user_packs(db, user_id)
        user = await self._get_user(db, user_id, lock=True)
        locked = (
            (
                await db.execute(
                    select(BillingCreditPack)
                    .where(
                        BillingCreditPack.user_id == user_id,
                        or_(
                            BillingCreditPack.status == "active",
                            BillingCreditPack.id == source_pack.id,
                        ),
                    )
                    .order_by(BillingCreditPack.expires_at.asc())
                    .with_for_update()
                    # Authoritative locked read: refresh any pre-loaded pack (the
                    # caller passes an already-fetched source_pack) to committed state
                    # so a concurrent deduct's drain is never lost (T-306).
                    .execution_options(populate_existing=True)
                )
            )
            .scalars()
            .all()
        )
        source = next((p for p in locked if p.id == source_pack.id), None)
        if source is None:  # the source row vanished between lookup and lock
            raise ValueError(
                f"refund source pack {source_pack.id} not found under lock"
            )

        # 1. Normalise refund money through the order total (tax-inclusive safe).
        paid_item_cents = source.paid_item_amount_cents
        order_total = source.provider_order_total_cents
        provider_total_cents = (
            max(order_total, paid_item_cents)
            if order_total is not None
            else paid_item_cents
        )
        # clamp(refunded, 0, provider_total)
        provider_refunded_total_cents = min(
            max(provider_refunded_amount_cents, 0), provider_total_cents
        )
        if full_or_fraud or provider_refunded_total_cents >= provider_total_cents:
            new_refunded_item_cents = paid_item_cents
        else:
            new_refunded_item_cents = (
                paid_item_cents * provider_refunded_total_cents
            ) // provider_total_cents

        old_refunded_item_cents = source.refunded_item_amount_cents_processed

        # 2. Monotonic replay gate — same/lower refund level is a no-op.
        if new_refunded_item_cents <= old_refunded_item_cents:
            source.provider_refunded_total_cents_seen = max(
                source.provider_refunded_total_cents_seen, provider_refunded_total_cents
            )
            return RefundOutcome(new_refunded_item_cents, 0, 0, 0, applied=False)

        # 3. Credit math. Expired value is never reclaimed.
        refundable_value_credits = source.credits_purchased - source.credits_expired
        if new_refunded_item_cents == paid_item_cents:
            target_revoked_credits = refundable_value_credits
        else:
            target_revoked_credits = (
                refundable_value_credits * new_refunded_item_cents
            ) // paid_item_cents
        credits_to_revoke = target_revoked_credits - source.credits_revoked
        if credits_to_revoke <= 0:
            # Zero-effective refund: advance the processed fields, no balance change.
            source.refunded_item_amount_cents_processed = max(
                old_refunded_item_cents, new_refunded_item_cents
            )
            source.provider_refunded_total_cents_seen = max(
                source.provider_refunded_total_cents_seen, provider_refunded_total_cents
            )
            return RefundOutcome(new_refunded_item_cents, 0, 0, 0, applied=False)

        # 4. Revoke: drain the source pack's remaining first, then other active
        #    packs FIFO (their drained slices recorded as credits_debt_recovered).
        immediate_revoke = 0
        need = credits_to_revoke
        donor_allocations = []
        take_source = min(source.credits_remaining, need)
        source.credits_remaining -= take_source
        immediate_revoke += take_source
        need -= take_source
        if need > 0:
            for pack in locked:  # already ORDER BY expires_at ASC (FIFO)
                if need <= 0:
                    break
                if pack.id == source.id or pack.status != "active":
                    continue
                take = min(pack.credits_remaining, need)
                if take <= 0:
                    continue
                pack.credits_remaining -= take
                pack.credits_debt_recovered += take
                donor_allocations.append((pack.id, take))
                if pack.credits_remaining == 0:
                    pack.status = "consumed"
                immediate_revoke += take
                need -= take

        # 5. Any still-unrecoverable remainder becomes recoverable debt on the source.
        debt_created = need
        if debt_created > 0:
            await self._upsert_refund_debt(db, source, debt_created, reason_label)

        source.credits_revoked += credits_to_revoke
        source.provider_refunded_total_cents_seen = max(
            source.provider_refunded_total_cents_seen, provider_refunded_total_cents
        )
        source.refunded_item_amount_cents_processed = max(
            old_refunded_item_cents, new_refunded_item_cents
        )

        # Balance drops by the immediately drained amount only; never below zero.
        if user is not None:
            user.credit_balance = max(
                0, int(user.credit_balance or 0) - immediate_revoke
            )

        # Two-component reason: refund:billing:<pack_id>:<new_refunded_item_cents> —
        # the cents suffix is the per-level idempotency barrier (a one-component reason
        # would collide on the second partial refund and silently skip it).
        reversal_entry = CreditLedger(
            id=uuid4(),
            user_id=user_id,
            amount=-immediate_revoke,
            reason=ledger_reason
            or f"refund:billing:{source.id}:{new_refunded_item_cents}",
        )
        db.add(reversal_entry)
        await db.flush()
        for donor_id, take in donor_allocations:
            db.add(
                CreditPackAllocation(
                    ledger_entry_id=reversal_entry.id,
                    pack_id=donor_id,
                    recovery_for_pack_id=source.id,
                    amount=take,
                )
            )

        # 6. Source pack status transition.
        if new_refunded_item_cents == paid_item_cents:
            source.status = "refunded"
        elif source.credits_remaining > 0:
            if source.status != "expired":
                source.status = "active"
        else:
            if source.status != "expired":
                source.status = "consumed"

        await db.flush()
        return RefundOutcome(
            new_refunded_item_cents,
            credits_to_revoke,
            immediate_revoke,
            debt_created,
            applied=True,
        )

    async def release_reversal(
        self,
        db: AsyncSession,
        source: BillingCreditPack,
        target_credits_revoked: int,
        *,
        ledger_reason: str,
    ) -> None:
        """Return a resolved dispute's withheld value with original provenance.

        Caller holds the payment and user locks. Cancel outstanding debt first,
        return collected donor credits next, then restore unused source credits.
        Expired value is never resurrected. Cash-refunded value is excluded by
        the caller when it computes the new settlement target.
        """
        release = max(0, source.credits_revoked - target_credits_revoked)
        if not release:
            return
        gap = source.credits_purchased - (
            source.credits_remaining
            + source.credits_consumed
            + source.credits_expired
            + source.credits_debt_recovered
        )
        collected_elsewhere = min(release, max(0, source.credits_revoked - gap))
        usable = (
            await self._return_reversal_recovery(db, source, collected_elsewhere)
            if collected_elsewhere
            else 0
        )
        direct = release - collected_elsewhere
        source.credits_revoked -= release
        if source.expires_at <= datetime.now(timezone.utc):
            source.credits_expired += direct
            source.status = "expired"
        else:
            source.credits_remaining += direct
            usable += direct
            source.status = "active" if source.credits_remaining else "consumed"
        await db.flush()
        user = await self._get_user(db, source.user_id, lock=True)
        user.credit_balance += usable
        db.add(
            CreditLedger(user_id=source.user_id, amount=usable, reason=ledger_reason)
        )
        await db.flush()

    async def _upsert_refund_debt(
        self,
        db: AsyncSession,
        source_pack: BillingCreditPack,
        amount: int,
        reason_label: str,
    ) -> None:
        """Create or extend the source pack's recoverable debt row (T-300).

        One debt per source pack (``source_pack_id`` is UNIQUE). A later purchase's
        ``grant_credits_with_debt_recovery`` repays it oldest-first. If a prior debt
        was already ``recovered`` and a new refund extends it, it flips back to
        ``pending``.
        """
        debt = await db.scalar(
            select(BillingCreditDebt)
            .where(BillingCreditDebt.source_pack_id == source_pack.id)
            .with_for_update()
        )
        if debt is None:
            db.add(
                BillingCreditDebt(
                    user_id=source_pack.user_id,
                    source_pack_id=source_pack.id,
                    provider=source_pack.provider,
                    provider_order_id=source_pack.provider_order_id,
                    credits_owed=amount,
                    credits_recovered=0,
                    status="pending",
                    reason=f"refund_exceeded_remaining:{reason_label}",
                )
            )
        else:
            debt.credits_owed += amount
            debt.updated_at = datetime.now(timezone.utc)
            if debt.credits_recovered < debt.credits_owed:
                debt.status = "pending"
        await db.flush()

    async def deduct(
        self,
        db: AsyncSession,
        user_id: UUID,
        amount: int,
        reason: str,
    ) -> CreditLedger:
        await self._expire_user_packs(db, user_id)  # Step 1: sweep expired packs
        user = await self._get_user(db, user_id, lock=True)
        balance = int(user.credit_balance or 0) if user is not None else 0
        if user is None or balance < amount:
            raise InsufficientCreditsError(
                f"Balance {balance} is less than required {amount}"
            )
        user.credit_balance = balance - amount
        entry = CreditLedger(user_id=user_id, amount=-amount, reason=reason)
        db.add(entry)
        await db.flush()
        await self._drain_packs(db, user_id, amount, entry)
        await self._invalidate(user_id)
        return entry

    async def refund(
        self,
        db: AsyncSession,
        ledger_entry_id: UUID,
        user_id: UUID | None = None,
    ) -> int:
        """Idempotently reverse a deduction; return the credits this call refunded.

        The return value is the amount actually credited back by THIS call —
        ``abs(original.amount)`` on success, and ``0`` on every no-op branch
        (missing/positive/mismatched entry, or an already-applied refund). Callers
        that log a refunded figure (e.g. the recovery sweep) must use this real
        amount rather than a hardcoded cost constant (audit finding #8).
        """
        original = await self._get_ledger_entry(db, ledger_entry_id)
        if original is None:
            logger.error(
                "credit.refund.missing_entry ledger_entry_id=%s user_id=%s",
                ledger_entry_id,
                user_id,
            )
            return 0
        if original.amount >= 0:
            logger.error(
                "credit.refund.not_a_deduction ledger_entry_id=%s amount=%d user_id=%s",
                ledger_entry_id,
                original.amount,
                original.user_id,
            )
            return 0
        if user_id is not None and original.user_id != user_id:
            logger.error(
                "credit.refund.user_mismatch ledger_entry_id=%s "
                "expected_user_id=%s actual_user_id=%s",
                ledger_entry_id,
                user_id,
                original.user_id,
            )
            return 0

        refund_reason = f"refund:{ledger_entry_id}"
        existing_refund = await self._get_refund_entry(
            db,
            original.user_id,
            refund_reason,
        )
        if existing_refund is not None:
            return 0

        user = await self._get_user(db, original.user_id, lock=True)
        if user is None:
            logger.error(
                "credit.refund.user_missing ledger_entry_id=%s user_id=%s",
                ledger_entry_id,
                original.user_id,
            )
            return 0

        refund_amount = abs(original.amount)
        refund_entry = CreditLedger(
            user_id=original.user_id,
            amount=refund_amount,
            reason=refund_reason,
        )
        try:
            async with db.begin_nested():
                # Scope the INSERT to a SAVEPOINT so that a concurrent duplicate
                # refund (IntegrityError on the unique reason constraint) only
                # rolls back this savepoint — the outer transaction (stage status
                # updates, content writes, etc.) remains intact.  MF-3 — T-207.
                db.add(refund_entry)
                await db.flush()
                if (original.metadata_ or {}).get("allocation_version") == 1:
                    await self._expire_user_packs(db, original.user_id)
                    refund_amount = await self._restore_deduction(db, original)
                    refund_entry.amount = refund_amount
                elif original.created_at is not None:
                    # Old non-pack starter credits remain refundable. An old
                    # paid deduction has no reliable provenance: never guess a
                    # source or turn it into non-expiring balance.
                    historical_pack = (
                        await db.execute(
                            select(BillingCreditPack.id)
                            .where(
                                BillingCreditPack.user_id == original.user_id,
                                BillingCreditPack.purchased_at <= original.created_at,
                            )
                            .limit(1)
                        )
                    ).scalar_one_or_none()
                    if historical_pack is not None:
                        raise ValueError(
                            "Legacy paid deduction needs allocation repair"
                        )
        except IntegrityError:
            # A concurrent refund for the same deduction already committed.
            # The SAVEPOINT was automatically rolled back; the outer transaction
            # is intact.  Treat as idempotent — the credits were already refunded.
            logger.warning(
                "credit.refund.duplicate_entry ledger_entry_id=%s user_id=%s",
                ledger_entry_id,
                original.user_id,
            )
            return 0
        user.credit_balance = int(user.credit_balance or 0) + refund_amount
        await db.flush()
        await self._invalidate(original.user_id)
        return refund_amount

    async def _restore_deduction(self, db: AsyncSession, original: CreditLedger) -> int:
        """Undo source allocations under the already-held user lock.

        Only usable value is returned. Lapsed value retains its original expiry;
        value already reclaimed by a cash reversal offsets that reversal's debt
        or restores the donor packs from which the debt was collected.
        """
        allocations = (
            (
                await db.execute(
                    select(CreditPackAllocation)
                    .where(
                        CreditPackAllocation.ledger_entry_id == original.id,
                        CreditPackAllocation.recovery_for_pack_id.is_(None),
                    )
                    .order_by(CreditPackAllocation.pack_id)
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        unallocated = int((original.metadata_ or {}).get("unallocated_credits", 0))
        if unallocated + sum(a.amount for a in allocations) != abs(original.amount):
            raise ValueError("Incomplete credit allocation history")
        restored = unallocated
        for allocation in allocations:
            amount = allocation.amount - allocation.returned_amount
            allocation.returned_amount += amount
            restored += await self._restore_pack_value(
                db,
                allocation.pack_id,
                amount,
                "credits_consumed",
                original.user_id,
            )
        return restored

    async def _restore_pack_value(
        self,
        db: AsyncSession,
        pack_id: UUID,
        amount: int,
        counter: str,
        user_id: UUID,
    ) -> int:
        if amount <= 0:
            return 0
        pack = (
            await db.execute(
                select(BillingCreditPack)
                .where(
                    BillingCreditPack.id == pack_id,
                    BillingCreditPack.user_id == user_id,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one()
        if getattr(pack, counter) < amount:
            raise ValueError("Credit allocation exceeds source accounting")
        # Accounting's gap is the value revoked directly from this pack.
        # The rest of credits_revoked was collected from other packs or debt.
        directly_revoked = pack.credits_purchased - (
            pack.credits_remaining
            + pack.credits_consumed
            + pack.credits_expired
            + pack.credits_debt_recovered
        )
        absorbed = min(amount, max(0, pack.credits_revoked - directly_revoked))
        setattr(pack, counter, getattr(pack, counter) - amount)
        normal = amount - absorbed
        usable = 0
        if pack.expires_at <= datetime.now(timezone.utc):
            pack.credits_expired += normal
            if pack.status not in ("refunded", "disputed"):
                pack.status = "expired"
        elif normal:
            pack.credits_remaining += normal
            pack.status = "active"
            usable += normal
        await db.flush()
        if absorbed:
            usable += await self._return_reversal_recovery(db, pack, absorbed)
        return usable

    async def _return_reversal_recovery(
        self,
        db: AsyncSession,
        source: BillingCreditPack,
        amount: int,
    ) -> int:
        debt = (
            await db.execute(
                select(BillingCreditDebt)
                .where(
                    BillingCreditDebt.source_pack_id == source.id,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if debt is not None:
            take = min(amount, debt.credits_owed - debt.credits_recovered)
            debt.credits_recovered += take
            amount -= take
            if debt.credits_recovered == debt.credits_owed:
                debt.status = "recovered"
            debt.updated_at = datetime.now(timezone.utc)
        recoveries = (
            (
                await db.execute(
                    select(CreditPackAllocation)
                    .where(
                        CreditPackAllocation.recovery_for_pack_id == source.id,
                        CreditPackAllocation.returned_amount
                        < CreditPackAllocation.amount,
                    )
                    .order_by(CreditPackAllocation.id)
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        usable = 0
        for recovery in recoveries:
            take = min(amount, recovery.amount - recovery.returned_amount)
            if take <= 0:
                break
            recovery.returned_amount += take
            amount -= take
            await db.flush()
            usable += await self._restore_pack_value(
                db,
                recovery.pack_id,
                take,
                "credits_debt_recovered",
                source.user_id,
            )
        if amount:
            raise ValueError("Payment reversal needs legacy recovery allocation repair")
        return usable

    async def _get_ledger_entry(
        self,
        db: AsyncSession,
        ledger_entry_id: UUID,
    ) -> CreditLedger | None:
        result = await db.execute(
            select(CreditLedger).where(CreditLedger.id == ledger_entry_id)
        )
        return result.scalar_one_or_none()

    async def _get_refund_entry(
        self,
        db: AsyncSession,
        user_id: UUID,
        refund_reason: str,
    ) -> CreditLedger | None:
        result = await db.execute(
            select(CreditLedger).where(
                CreditLedger.user_id == user_id,
                CreditLedger.reason == refund_reason,
            )
        )
        return result.scalar_one_or_none()

    async def _invalidate(self, user_id: UUID) -> None:
        redis = await self._get_redis()
        try:
            await redis.delete(self._redis_key(user_id))
        except RedisError:
            logger.warning(
                "credit.cache_invalidation_failed user_id=%s",
                user_id,
                exc_info=True,
            )
        # Also flush the shared auth middleware cache so the next request — on
        # ANY worker — reflects the updated credit_balance immediately rather
        # than serving a stale entry. The cache is Redis-backed, so this
        # invalidation propagates cross-worker (LF-1, was H-4 — T-180).
        # Lazy import to avoid a circular dependency:
        #   auth_service → credit_service → middleware.auth → auth_service.
        from middleware.auth import invalidate_user_cache  # noqa: PLC0415

        await invalidate_user_cache(user_id)

    async def invalidate(self, user_id: UUID) -> None:
        """Public alias for post-commit cache invalidation.

        Call this immediately after ``db.commit()`` in any code path that
        first called ``deduct()``, ``credit()``, or ``refund()``.  The first
        invalidation (inside those methods, after flush) clears the cache
        eagerly.  This second call ensures any concurrent ``get_balance()``
        that re-populated the cache during the flush→commit window is
        immediately evicted after the true balance is committed.  H-2 — T-219.
        """
        await self._invalidate(user_id)


credit_service = CreditService()
