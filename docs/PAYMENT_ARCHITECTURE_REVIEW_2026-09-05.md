# Razorpay payment and credit architecture review

Reviewed: 5 September 2026 (IST). Repository revision: `efc3ea6`.

## Remediation verified — 6 September 2026

Implemented fixes for all six code findings below. New checkout is Razorpay-only;
legacy settlement remains available for historical purchases. Payments have not
been enabled or deployed as part of this work.

| Finding | Implemented behavior |
| --- | --- |
| Delayed or missing payment webhook | Expired/failed attempts can settle verified captures. Bounded reconciliation recovers a first grant through the recorded Payment Link and authenticated payment membership, retaining all ownership and economics checks. |
| Failed-operation credit restoration | Each deduction records pack/starter allocations. Restoration preserves original expiry and offsets reversal debt or returns donor credits where appropriate. |
| Missed refunds on consumed/expired packs | Reconciliation includes consumed, expired, refunded and disputed packs within the configurable lookback. Signed refunds arriving before their grant remain durable settlement evidence. |
| Disputes discarded | All six dispute events enter the durable inbox. Losses reverse credits; wins restore eligible value. Duplicate/stale events are handled, conflicting terminal timestamps trigger an authenticated dispute read, and overlapping refund/dispute value is not charged twice. |
| Expiry rolled back on reads | Balance, history and receipt reads commit the expiry sweep; balance reads use PostgreSQL and do not publish uncommitted cache values. |
| Misleading receipt/admin correction | Receipts expose current usable credits, settlement state and debt recovery. Razorpay corrections verify the original checkout and complete it atomically with the grant. |

Verification on isolated PostgreSQL 16 and Redis 7, with outbound provider HTTP
mocked: **335 payment/credit backend tests passed**, including **33 new regression
cases** for recovery, proof rejection, operation restoration, expiry, refunds,
disputes, concurrent settlement and receipts. Another **13 legacy configuration
tests passed**. **56 frontend billing, credit and legal-page tests passed**.
TypeScript checking, backend lint/format checks and frontend application lint pass;
the OpenAPI client types were regenerated.

Deployment requires **migration 0046** before the updated API/workers. Historical
paid deductions or donor recoveries without source allocations need verified
allocation repair before automatic restoration; the code fails closed instead of
inventing credits. Automatic provider recovery has a configurable 180-day default
lookback. Payment reads cannot discover entirely missing dispute events; dashboard
monitoring and signed-event redelivery remain required. No real provider payment,
refund, live credentials or dashboard configuration was verified in these tests.
See [payment launch verification](PAYMENT_LAUNCH_VERIFICATION.md) for the concrete
rollout and staging checks.

## Original review snapshot

The findings and line references below describe the pre-fix revision, retained as
the evidence for this remediation.

**Recommendation: keep payments disabled until the four high-priority findings below are resolved.** The normal payment-to-credit flow works in local integration testing. Payment recovery and credit lifecycle accounting still have defects that make live payment acceptance premature.

The public [billing package endpoint](https://api.thought2build.com/billing/package) returned HTTP 200 during this review with `provider=razorpay`, `enabled=false`, `currency=INR`, `price_cents=154900`, `credits=200`, and `validity_days=30`. This establishes the advertised configuration, not the deployed revision, validity of credentials, or health of the payment worker.

## Scope and evidence

Reviewed checkout creation, provider HTTP calls, webhook verification and normalization, inbox persistence, Redis/arq routing, worker processing, grant idempotency, refunds, debt recovery, consumption, failed-generation credit restoration, expiry, customer polling, admin corrections, migrations, configuration and rollout documentation.

Validation completed:

- Applied the full Alembic migration chain through `0045` to isolated local PostgreSQL 16; used isolated Redis 7.
- **303 backend tests passed**, covering the selected Razorpay, billing, credit and checkout URL suites, including the billing migration tests. The initial run without a test database passed 177 and skipped 126; the database-backed run removed those skips.
- **42 frontend tests passed** across Billing, useCredits, CreditSystem and CreditMeter.
- Exercised checkout HTTP → provider service with a mocked Razorpay HTTP response → signed webhook HTTP → real database inbox → real Redis/arq worker → credit pack/ledger/balance → checkout-status HTTP. A ₹1,549 checkout granted **200 credits in one pack**, and duplicate webhook delivery did not grant again.
- Ran additional probes against the real local database to reproduce the findings below. These probes intentionally expose gaps that the existing passing tests do not cover.
- Compared the webhook assumptions with official Razorpay documentation.

No real Razorpay payment or cash refund was initiated. The hosted checkout UI, actual Razorpay event delivery, account activation, production credentials, capture settings, worker deployment and alert delivery were not verified. Authentication was injected for the custom local HTTP flow; existing router/middleware tests cover its authorization and request guards. The provider was simulated only at the outbound HTTP boundary.

## Findings, ordered by priority

### 1. High — a valid payment can permanently miss its credit grant after local checkout expiry

Locations: [attempt expiry](../backend/services/billing_worker.py#L718), [grant eligibility](../backend/services/billing_worker.py#L1492), [terminal rejection](../backend/services/billing_worker.py#L1555).

The reconciliation job changes open checkout attempts to `expired` after their TTL. The grant handler accepts only `created`, `provider_created` and `completed`. Consequently, a payment completed successfully at Razorpay can be rejected if its signed event arrives, or is processed, after the local expiry sweep. The handler marks the webhook `processed`, so normal replay will not retry it.

**Reproduced:** a valid captured-payment payload with matching ownership proof and economics, processed after the expiry sweep, left the user with **0 credits, 0 packs and a processed webhook**. The logged reason was `attempt_status_invalid`.

This is a realistic delivery case: Razorpay documents asynchronous delivery, retries for up to 24 hours and potentially out-of-order events. [Razorpay webhook best practices](https://razorpay.com/docs/webhooks/best-practices/?preferred-country=IN).

There is also no automatic recovery of a first paid event that never reaches the inbox. Reconciliation replays existing inbox rows and re-reads payments for existing packs; it does not query unresolved Payment Links. A missing first event therefore requires manual intervention. This is an explicit architectural limitation, separate from the reproduced late-event defect.

**Required change:** separate the deadline for starting a payment from settlement of an already successful payment. Retain the payment timestamps and binding identifiers needed to validate delayed settlement. Recover unresolved attempts by fetching their server-recorded Razorpay Payment Link and associated captured payment, checking ownership, amount, currency and refund state, then invoking the same idempotent grant logic. Do not infer payment from an email, browser redirect or an amount/time match. Razorpay exposes a [Payment Link fetch API with captured payment details](https://razorpay.com/docs/api/payments/payment-links/fetch-id-standard/?preferred-country=US).

### 2. High — failed-generation refunds lose the purchased credits' pack accounting

Locations: [pack consumption](../backend/services/credit_service.py#L127), [deduction](../backend/services/credit_service.py#L549), [credit refund](../backend/services/credit_service.py#L571), particularly [balance-only restoration](../backend/services/credit_service.py#L653).

Deducting credits decreases pack remaining credits and increases pack consumed credits. Refunding that deduction creates a positive ledger entry and increases the user balance, but does not reverse those pack allocations. The restored credits become balance with no corresponding remaining purchased-pack value.

**Reproduced:** buy 200 credits, deduct 10 for a failed generation, then restore the 10. The user balance returned to **200**, but the pack still showed **190 remaining and 10 consumed**.

Two further consequences were reproduced independently:

- Expiring the original pack removed 190 and left **10 spendable credits** beyond the pack's expiry.
- Fully refunding the cash payment removed 190 and left **10 spendable credits plus 10 credits of billing debt**, despite the failed generation having been restored.

This affects the shared credit refund helper used by generation recovery and other paid operations.

**Required change:** record how each deduction allocates across source packs, and reverse those allocations idempotently when an operation fails. Define explicitly how restoration interacts with pack expiry and cash refunds already in progress. A deliberate replacement-credit policy needs its own tracked pack/expiry; it should not emerge accidentally as untracked balance.

### 3. High — refund reconciliation excludes consumed and expired packs

Locations: [eligible statuses](../backend/services/billing_worker.py#L166), [candidate query](../backend/services/billing_worker.py#L599).

The provider re-read lane selects only `active` and `refunded` packs. A fully consumed pack is excluded, as is an expired pack that still has historical consumption. These are precisely cases where a cash reversal may require debt recovery instead of removal of unused credits.

**Reproduced:** seeded a Razorpay pack with 200 consumed credits and `status=consumed`, ran the real reconciliation candidate query and instrumented the provider lookup. **That payment was never queried.**

If the refund webhook is missed, or a refund arriving before its pack was granted is acknowledged as unlinked, this backstop cannot recover the reversal after the pack has been consumed. A fully consumed pack can escape refund debt indefinitely.

**Required change:** select reconciliation candidates based on outstanding financial exposure and a defined payment/refund lookback, not only availability of unused credits. Include consumed packs and expired packs with revocable consumption. Add missed-webhook and out-of-order tests that spend credits before reconciliation.

### 4. High — lost disputes are discarded, and the documented manual remedy cannot revoke credits

Locations: [ignored event branch](../backend/routers/billing.py#L1020), [admin request schema](../backend/schemas/billing.py#L111), [admin correction implementation](../backend/routers/billing.py#L480), [runbook remedy](RUNBOOK.md#L1096).

Only `payment_link.paid` and `refund.processed` are actionable Razorpay events. `payment.dispute.*` events are acknowledged and discarded without a durable inbox record. The payment re-read logic does not query disputes.

**Reproduced:** a correctly signed `payment.dispute.lost` event returned `{"status":"ignored"}` and left the purchased **200 credits** untouched. Razorpay documents this as a distinct [dispute webhook event](https://razorpay.com/docs/webhooks/disputes/?preferred-country=IN).

The runbook advises using the admin-correction path for a dispute loss, but that endpoint only accepts positive credits and grants new packs. For an existing payment pack it returns a no-op. It cannot revoke the existing pack or create the corresponding dispute debt. The proposed operational mitigation therefore does not implement the required settlement.

**Required change:** persist dispute identities and states, implement an audited and idempotent reversal for the appropriate lost/accepted outcomes, and support restoration for won outcomes if credits were withheld earlier. Alternatively, provide a working audited manual reversal path with durable dispute tracking before accepting this operational model. Ensure a dispute reversal and a later refund cannot revoke the same value twice.

### 5. Medium — expiry reported by the balance endpoint is rolled back

Locations: [balance endpoint](../backend/routers/credits.py#L17), [expiry mutation](../backend/services/credit_service.py#L84), [database dependency](../backend/database.py#L166), [pack history](../backend/routers/billing.py#L454).

`get_balance()` mutates expired packs and the user balance, but `/credits/balance` never commits that transaction. `get_db()` simply yields and closes the session, so the expiry changes roll back at request completion. The response/cache may contain the expired balance while persistent balance and pack history retain the old state.

**Reproduced:** an expired 200-credit pack produced API balance **0**, while a fresh database read still showed **200 balance, 200 remaining credits and an active pack**.

This is an accounting and customer-history inconsistency; it does not by itself prove expired credits can be spent, because `deduct()` performs its own expiry sweep.

**Required change:** give expiry an explicit committed transaction boundary, invalidate caches after commit, and make the balance and history views agree. Verify with a second independent database session after the HTTP request finishes.

### 6. Medium — checkout success can disagree with the settled credit state

Locations: [status eligibility and response](../backend/routers/billing.py#L421), [gross credits response](../backend/routers/billing.py#L447), [customer success message](../frontend/src/pages/Billing.tsx#L183), [admin correction](../backend/routers/billing.py#L480).

The status endpoint reports a completed checkout and `pack.credits_purchased` whenever it finds the original completed attempt and pack. It does not communicate subsequent refunds, expiry or the part of a purchase used to recover debt. The frontend presents this as credits added and a balance ready to use.

**Reproduced:** after a full cash reversal, the endpoint still reported **completed / 200 credits added** while actual balance was **0**.

The reverse inconsistency also exists. Admin correction grants a pack but does not complete or bind the original checkout attempt. **Reproduced:** correcting the delayed payment awarded **200 credits**, but polling its original checkout reference still returned **404**.

**Required change:** expose purchase completion separately from current settlement state and usable credits, including debt recovered and reversals. Bind a manual correction to its proven checkout attempt atomically so the customer's original polling flow resolves correctly.

## What is working

- Server-created attempts capture credits, price, currency and validity before contacting Razorpay. Client input does not choose the amount or credit quantity.
- Hosted Payment Links use the attempt economics, disallow partial payments and use a bounded provider HTTP client.
- Raw-body HMAC verification precedes JSON parsing. Current and previous webhook secrets support rotation; missing secrets fail closed.
- Successful-payment validation checks ownership proof, captured status, amount, currency and paid-link status. The browser return has no credit-grant authority.
- Inbox persistence occurs before enqueueing. A queue failure after persistence can be recovered by the pending-event sweep.
- Pack/ledger uniqueness and transactional updates protect grants and repeated refund levels. Existing tests exercise these safeguards; the custom flow also verified duplicate delivery.
- Spending uses a locked database balance and pack draining. Refund reversals and subsequent purchases have a debt-recovery implementation, subject to the lifecycle gaps above.
- Checkout status and history are scoped to the authenticated user. Admin grants require an allowlisted admin and an evidence URL.

These controls are worth retaining. The findings primarily concern settlement recovery and consistency between balance, pack, debt and customer-facing status.

## Razorpay-only readiness

The public package endpoint already advertises Razorpay, but the repository still implements two-provider runtime selection. [Config defaults](../backend/config.py#L750), [.env.example](../backend/.env.example#L338), admin request defaults and [customer terms](../frontend/src/pages/LegalTerms.tsx#L98) still reference Lemon Squeezy; the terms describe provider selection by region, which the global selector does not implement.

For a Razorpay-only launch, make Razorpay the explicit default and supported checkout provider, remove obsolete provider claims from customer copy, and decommission unused credentials/routes after checking for any historical transactions. Retaining historical audit tables is compatible with Razorpay-only checkout. Confirm the Razorpay settings are consistent across the API and fast worker, since both independently interpret webhook environment and processing configuration.

## Release acceptance checks

Before enabling payments, the fixes need regression coverage for:

1. Captured payment delivered after local TTL, received before TTL but processed later, and a missing first webhook recovered by a provider-bound lookup.
2. Failed-generation credit restoration across multiple packs, original pack expiry, and an overlapping cash refund.
3. Missed refunds after full consumption and expiry; refund-before-grant ordering; multiple partial refunds followed by a full refund.
4. Lost/won disputes and repeated/out-of-order dispute and refund events.
5. Committed expiry after a balance request, and truthful checkout status after debt recovery, refund and manual correction.

The operational sign-off still needs an actual Razorpay test-mode checkout and refund through the deployed stack, followed by the controlled live smoke purchase in the rollout process. Confirm the exact webhook route and event subscriptions, the active `FastWorkerSettings` consumer, recovery sweeps, and alert delivery. A webhook HTTP 200 is only receipt/acknowledgment; verify the matching payment, completed attempt, pack, ledger and final balance. Keep `PAYMENTS_ENABLED=false` until these checks and the high-priority fixes are complete.

## Reproduction artifacts

Review-only local artifacts were saved under `/tmp/specforge-payment-review/`:

- `probes.py`: additional HTTP/database/queue probes, with provider HTTP mocked and assertions limiting database/Redis access to the isolated local review ports.
- `probe-results.json`: the observed values quoted above.
- `backend-db.log`: 303 passing backend tests.
- `frontend.log` and `frontend-billing.log`: 42 passing frontend tests.
- `migrations.log`: full migration application.

Application code and production payment configuration were not changed by this review.
