"""Pydantic schemas for the Billing API.

Phase 18 — T-230 (Stripe). Reworked in Phase 22 — T-291 for the provider-neutral
Lemon Squeezy billing flow (Plan §25.6). The schemas never echo the checkout nonce,
the webhook signature, or any raw provider payload (Plan §25 SR4).

Schema inventory
----------------
PackageResponse        GET  /billing/package           (+currency)
CheckoutResponse       POST /billing/checkout          (+checkout_ref)
BillingStatusResponse  GET  /billing/status
PackHistoryItem        GET  /billing/history           (sourced from BillingCreditPack)
AdminCorrectionRequest POST /billing/admin/correction  (T-302)
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class PackageResponse(BaseModel):
    """Current credit package configuration — returned by GET /billing/package.

    Sourced from config.Settings at request time so changes to env vars are
    reflected immediately without redeployment.  No auth required.

    Issue #44: economics come from the **active** ``payment_provider``;
    ``enabled`` mirrors ``settings.billing_checkout_enabled`` so the frontend can
    gate the Buy button instead of discovering a 503 on click. When checkout is
    off the configured numbers are still returned (the pricing card renders) with
    ``enabled=false``.
    """

    credits: int = Field(ge=1, description="Credits granted per purchase")
    price_cents: int = Field(
        ge=1, description="Price in minor units (1500 = $15.00, 154900 = ₹1549.00)"
    )
    validity_days: int = Field(
        ge=1, description="Days until the purchased pack expires"
    )
    currency: str = Field(default="INR", description="ISO 4217 currency code")
    enabled: bool = Field(
        default=True,
        description="True when POST /billing/checkout is available (issue #44)",
    )
    provider: str = Field(
        default="razorpay",
        description="The active payment provider these economics belong to",
    )


class CheckoutResponse(BaseModel):
    """Checkout hand-off — returned by POST /billing/checkout (T-296).

    ``checkout_url`` is the provider-hosted checkout page; ``checkout_ref`` is the
    high-entropy local key the frontend polls /billing/status by. ``checkout_ref`` is
    optional here only so the retained Stripe grace-path router (rewritten in T-296)
    keeps validating — the Lemon flow always populates it.
    """

    checkout_url: str = Field(description="Provider-hosted checkout page URL")
    checkout_ref: str | None = Field(
        default=None, description="Local polling key for GET /billing/status"
    )


class BillingStatusResponse(BaseModel):
    """Polling response for a checkout — GET /billing/status (T-296).

    ``status`` is 'pending' until the signed provider webhook grants the pack, and
    'completed' once the pack is active in the database. ``credits_added`` is 0 while
    pending; ``expires_at`` is None while pending, populated once the pack is active.
    """

    status: Literal["pending", "completed"]
    credits_added: int = Field(
        ge=0, description="Currently usable credits from this purchase"
    )
    credits_purchased: int = Field(default=0, ge=0)
    debt_recovered: int = Field(default=0, ge=0)
    credits_revoked: int = Field(default=0, ge=0)
    settlement_status: Literal[
        "active", "consumed", "expired", "refunded", "partially_refunded", "disputed"
    ] = "active"
    expires_at: datetime | None = Field(
        default=None, description="UTC expiry of the purchased pack"
    )

    model_config = ConfigDict(from_attributes=True)


class PackHistoryItem(BaseModel):
    """One row in the user's purchase history — GET /billing/history.

    Sourced from a ``BillingCreditPack`` ORM row (owner-scoped at the router). The
    status union covers the neutral pack lifecycle; the retained Stripe statuses are a
    subset, so legacy ``StripeCreditPack`` rows validate during the grace window too.
    """

    id: UUID
    credits_purchased: int = Field(ge=0)
    credits_remaining: int = Field(ge=0)
    price_cents: int = Field(ge=0)
    currency: str = Field(
        min_length=3,
        max_length=3,
        description="ISO 4217 currency code captured when the pack was purchased",
    )
    status: Literal["active", "consumed", "expired", "refunded", "disputed"]
    purchased_at: datetime
    expires_at: datetime

    model_config = ConfigDict(from_attributes=True)


class AdminCorrectionRequest(BaseModel):
    """Body for the exceptional admin credit-correction support path — POST
    /billing/admin/correction (T-302).

    An evidence-backed manual grant for an order the automatic webhook pipeline could
    not settle. Authorisation is by the ``admin_emails`` allowlist at the router; the
    write is append-only and unique on ``(provider, provider_order_id)`` so it cannot
    be applied twice for the same order.
    """

    provider: Literal["lemonsqueezy", "stripe", "razorpay"] = Field(
        default="razorpay", description="Billing provider of the corrected order"
    )
    provider_order_id: str = Field(
        min_length=1, description="Provider order id the correction settles"
    )
    target_user_id: UUID = Field(description="User to credit")
    checkout_ref: str | None = Field(
        default=None, description="Original checkout to settle; required for Razorpay"
    )
    credits: int = Field(ge=1, description="Credits to grant")
    price_cents: int = Field(ge=1, description="Recorded order price in cents")
    currency: str = Field(min_length=1, description="ISO 4217 currency code")
    reason: str = Field(min_length=1, description="Support justification (audited)")
    evidence_url: HttpUrl = Field(
        description="Link to the supporting evidence (ticket, provider dashboard)"
    )

    model_config = ConfigDict(str_strip_whitespace=True)


class AdminCorrectionResponse(BaseModel):
    """Result of POST /billing/admin/correction (T-302).

    ``applied`` is True when this call performed the grant; False when an existing
    pack or correction for the same ``(provider, provider_order_id)`` made it an
    idempotent no-op (never a second grant). ``credits_granted`` is the corrected
    credit amount on the applied path, 0 on the no-op path.
    """

    applied: bool = Field(description="True if this call granted; False on duplicate")
    provider: str = Field(description="Billing provider of the corrected order")
    provider_order_id: str = Field(
        description="Provider order id the correction settles"
    )
    credits_granted: int = Field(ge=0, description="Credits granted by this call")
