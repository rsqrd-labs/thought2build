# Razorpay payment launch verification

New purchases use Razorpay only. Keep `PAYMENTS_ENABLED=false` until the deployment
and provider checks below are complete. This document supersedes historical
provider-switch and webhook-only assumptions in the integration plan.

## Deploy the accounting fixes

1. Back up the database and apply `alembic upgrade head` through **0046** before
   starting the updated API and workers. This creates credit allocation and
   dispute history. Downgrade intentionally refuses to discard nonempty history.
2. Quiesce credit-consuming jobs during this rollout. New deductions record their
   exact paid-pack and starter-credit sources. Pre-migration paid deductions have
   no reliable provenance: automatic operation refunds fail closed with
   `Legacy paid deduction needs allocation repair` rather than mint non-expiring
   credit. Inspect any existing paid deduction awaiting refund and repair its
   source allocation from verified history before resuming it. Likewise, old
   donor recoveries require allocation repair before a dispute win can return them.
   For the first paid launch there should be no historical paid deductions.
3. Confirm both API and fast/default workers run the same release. Verify the
   15-minute reconciliation and pending-inbox sweeps run. Monitor pending age,
   dead-letter entries, unrecoverable paid checkout alerts, reversal/debt metrics,
   and `billing.recovery.attempt_failed` logs.
4. Set `PAYMENT_PROVIDER=razorpay`. Confirm price, currency, credits and validity
   via `/billing/package`. The example is INR 154900 paise, 200 credits, 30 days;
   configuration is the source of truth. Razorpay secrets must match the selected
   mode; production requires `rzp_live_` keys, HTTPS return URL, and checkout TTL
   of at least 16 minutes. Confirm auto-capture in the provider dashboard.
5. Configure the matching-mode webhook at `/billing/webhook/razorpay` for
   `payment_link.paid`, `refund.processed`, and `payment.dispute.created`,
   `.under_review`, `.action_required`, `.lost`, `.won`, `.closed`.

## Exercise real provider test mode in staging

- Create checkout through the authenticated application; pay via the actual
  hosted Payment Link. Confirm one completed attempt, one pack, one purchase
  ledger row, the correct user's usable balance, and the receipt. Re-deliver the
  webhook and verify there is no second grant. A redirect alone grants nothing.
- Delay delivery until after the local attempt expiry. The captured payment must
  still settle once. Suppress the first webhook and run reconciliation: it must
  recover using the stored link and its authenticated payment membership.
- Spend credits, fail an operation, and verify restoration to the same pack.
  Expire that pack and verify the restored credits expire too. Repeat with a
  cash refund before the operation restoration; debt/donor recovery must balance.
- Issue partial and full refunds from Razorpay. Test unused, consumed and expired
  packs, duplicate delivery, refund-before-grant, and missed webhook recovery.
  Verify available balance never becomes negative and only spent, unexpired
  credit value becomes debt. A later purchase recovers that debt first.
- Exercise dispute delivery where the provider supports it; verify open/lost/won
  behavior and stale delivery handling. Track provider dashboard disputes even
  if a webhook never arrives: payment reads alone cannot discover new disputes.
- Confirm receipt states for refunded, expired, disputed, partial refund and
  debt recovery. Confirm cross-user status requests remain inaccessible.

After staging results and live configuration are verified, enable checkout in a
controlled release and perform one approved live purchase/refund smoke test.
Repository tests mock provider HTTP and do not certify live keys, capture settings,
webhook subscriptions, tax configuration, or provider settlement.

## Recovery boundaries and support

Automatic recovery scans unresolved recorded links and existing packs purchased
within `RAZORPAY_RECONCILE_LOOKBACK_DAYS` (180 by default), in bounded pages.
Unresolved attempts and signed refunds without a pack remain stored; for older
payments use explicit verified support recovery or signed-event redelivery.
Never infer ownership from an email or payment amount. A Razorpay admin correction
requires the original `checkout_ref`; the endpoint verifies the link/payment,
uses original economics and expiry, and completes that checkout atomically.

Cash refunds and lost disputes use the greater cumulative reversed amount,
capped at the purchase. This treats them as overlapping claims on one entitlement.
Open disputes do not freeze credits. Lost disputes revoke credits; a newer win
returns only value not still covered by a cash refund, preserving original expiry.

Provider references: [Payment Link webhooks](https://razorpay.com/docs/webhooks/payment-links/)
and [dispute webhooks](https://razorpay.com/docs/webhooks/disputes/).
