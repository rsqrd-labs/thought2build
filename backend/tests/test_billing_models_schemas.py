"""Unit tests for the Phase 22 (T-291) provider-neutral billing ORM models + schemas.

No database required — these assert that the six neutral models import and register on
the shared metadata, that ``BillingCreditPack`` maps the full lifetime accounting set,
that the Stripe models are retained, and that the reworked ``schemas/billing.py``
validates correctly (rejects an invalid pack status, accepts the five valid ones; the
admin-correction body validates its fields).
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from schemas.billing import (
    AdminCorrectionRequest,
    CheckoutResponse,
    PackageResponse,
    PackHistoryItem,
)


def test_neutral_models_import_and_register() -> None:
    """All six neutral models import from models/ and register on Base.metadata."""
    from models import (
        Base,
        BillingAdminCorrection,
        BillingCheckoutAttempt,
        BillingCreditDebt,
        BillingCreditPack,
        BillingReconciliationCursor,
        BillingWebhookEvent,
    )

    expected = {
        "billing_checkout_attempts": BillingCheckoutAttempt,
        "billing_credit_packs": BillingCreditPack,
        "billing_credit_debts": BillingCreditDebt,
        "billing_admin_corrections": BillingAdminCorrection,
        "billing_webhook_events": BillingWebhookEvent,
        "billing_reconciliation_cursors": BillingReconciliationCursor,
    }
    for table, model in expected.items():
        assert model.__tablename__ == table
        assert (
            table in Base.metadata.tables
        ), f"{table} must be registered on Base.metadata (import side effect)."


def test_stripe_models_retained() -> None:
    """The Stripe models must still import (audit + 7-day webhook grace path)."""
    from models import StripeCreditPack, StripeWebhookEvent

    assert StripeCreditPack.__tablename__ == "stripe_credit_packs"
    assert StripeWebhookEvent.__tablename__ == "stripe_webhook_events"


def test_billing_credit_pack_maps_accounting_columns() -> None:
    """BillingCreditPack mirrors the full lifetime accounting set + ceilings."""
    from models import BillingCreditPack

    columns = set(BillingCreditPack.__table__.columns.keys())
    for col in (
        "credits_purchased",
        "credits_remaining",
        "credits_consumed",
        "credits_expired",
        "credits_debt_recovered",
        "credits_revoked",
        "paid_item_amount_cents",
        "provider_order_total_cents",
        "provider_refunded_total_cents_seen",
        "refunded_item_amount_cents_processed",
        "provider_order_id",
        "provider_checkout_id",
    ):
        assert col in columns, f"BillingCreditPack must map column '{col}'."


def test_reconciliation_cursor_is_provider_pk() -> None:
    """The reconcile cursor keys on provider (one lemonsqueezy row)."""
    from models import BillingReconciliationCursor

    pk = [c.name for c in BillingReconciliationCursor.__table__.primary_key.columns]
    assert pk == ["provider"]


def test_credit_debt_source_pack_restrict_fk() -> None:
    """billing_credit_debts.source_pack_id is ON DELETE RESTRICT (debt never lost)."""
    from models import BillingCreditDebt

    fks = list(BillingCreditDebt.__table__.c.source_pack_id.foreign_keys)
    assert fks, "source_pack_id must declare a foreign key."
    assert fks[0].ondelete == "RESTRICT"


@pytest.mark.parametrize(
    "status", ["active", "consumed", "expired", "refunded", "disputed"]
)
def test_pack_history_accepts_valid_statuses(status: str) -> None:
    """PackHistoryItem accepts each of the five valid neutral statuses."""
    item = PackHistoryItem(
        id=uuid4(),
        credits_purchased=200,
        credits_remaining=150,
        price_cents=900,
        currency="USD",
        status=status,
        purchased_at=datetime.now(UTC),
        expires_at=datetime.now(UTC),
    )
    assert item.status == status


def test_pack_history_rejects_invalid_status() -> None:
    """PackHistoryItem rejects a status outside the union (defence at the edge)."""
    with pytest.raises(ValidationError):
        PackHistoryItem(
            id=uuid4(),
            credits_purchased=200,
            credits_remaining=150,
            price_cents=900,
            currency="USD",
            status="frozen",
            purchased_at=datetime.now(UTC),
            expires_at=datetime.now(UTC),
        )


def test_checkout_response_checkout_ref_optional_then_set() -> None:
    """checkout_ref defaults to None (Stripe grace path) and can be populated."""
    assert CheckoutResponse(checkout_url="https://x/y").checkout_ref is None
    resp = CheckoutResponse(checkout_url="https://x/y", checkout_ref="cr_abc")
    assert resp.checkout_ref == "cr_abc"


def test_package_response_carries_currency() -> None:
    """PackageResponse exposes currency (defaults INR)."""
    assert PackageResponse(credits=200, price_cents=900, validity_days=30).currency == (
        "INR"
    )


def test_admin_correction_request_valid() -> None:
    """A well-formed admin correction validates and defaults provider=razorpay."""
    req = AdminCorrectionRequest(
        provider_order_id="ord_123",
        target_user_id=uuid4(),
        credits=200,
        price_cents=900,
        currency="USD",
        reason="Webhook never arrived; verified paid in Lemon dashboard.",
        evidence_url="https://app.lemonsqueezy.com/orders/123",
    )
    assert req.provider == "razorpay"
    assert req.credits == 200


@pytest.mark.parametrize(
    "overrides",
    [
        {"credits": 0},  # non-positive credits
        {"price_cents": -1},  # negative price
        {"evidence_url": "not-a-url"},  # invalid evidence URL
        {"reason": ""},  # empty justification
    ],
)
def test_admin_correction_request_rejects_bad_input(overrides: dict) -> None:
    """The admin correction body rejects unsafe / incomplete input."""
    base = {
        "provider_order_id": "ord_123",
        "target_user_id": uuid4(),
        "credits": 200,
        "price_cents": 900,
        "currency": "USD",
        "reason": "valid reason",
        "evidence_url": "https://app.lemonsqueezy.com/orders/123",
    }
    base.update(overrides)
    with pytest.raises(ValidationError):
        AdminCorrectionRequest(**base)
