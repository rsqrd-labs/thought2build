"""Payment feature-flag + Razorpay config and production-guard tests (issue #44).

Covers the derived properties (``razorpay_enabled`` /
``razorpay_webhook_secrets`` / ``billing_checkout_enabled``) and the production
startup guards:

- the provider selector must name a known gateway (a typo must fail startup
  loudly, not silently disable billing);
- ``payments_enabled=true`` with an unconfigured active provider fails startup
  (checkout would 503 at runtime while payments read as enabled);
- a CONFIGURED Razorpay (active or not — its webhook route always processes)
  must carry a complete LIVE config: webhook secret, HTTPS success URL,
  positive economics, non-empty currency, a live ``rzp_live_`` key (Razorpay
  has no test-mode flag on events; the key prefix is the environment), and a
  checkout TTL of at least 16 minutes (Razorpay rejects payment-link expiries
  under ~15 minutes).

Structure mirrors ``test_lemonsqueezy_config.py`` (per-branch failures assert
the error message names the offending field).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

import config

# A syntactically valid PEM head so the JWT-key production check passes; the
# value is never used to sign anything in these tests.
_FAKE_PEM = "-----BEGIN RSA PRIVATE KEY-----\nMIIEow...\n-----END RSA PRIVATE KEY-----"


def _valid_production(**overrides: object):
    """Patch ``config.settings`` to an OTHERWISE-VALID production config.

    Every unrelated production check (metrics token, HTTPS frontend, real JWT
    PEM, non-CI encryption key, Langfuse disabled, GitHub App off) is
    satisfied, a complete LIVE Lemon config is set, AND a complete LIVE
    Razorpay config is set — so a raised error can only come from the branch
    under test. Payments ship disabled by default (the flag matrix tests flip
    them explicitly).
    """
    base: dict[str, object] = {
        "environment": "production",
        "allowed_hosts": "app.example.com",
        "metrics_token": "metrics-token",
        "frontend_url": "https://app.thought2build.com",
        "jwt_private_key": _FAKE_PEM,
        "encryption_master_key": "a-real-non-ci-encryption-key",
        "langfuse_secret_key": "",
        # GitHub App fully disabled.
        "github_app_id": "",
        "github_app_slug": "",
        "github_app_private_key": "",
        "github_app_webhook_secret": "",
        "github_app_webhook_secret_prev": "",
        # Complete LIVE Lemon config (enabled + passes the production guard).
        "lemonsqueezy_api_key": "lsq-live-key",
        "lemonsqueezy_webhook_secret": "lsq-secret",
        "lemonsqueezy_webhook_secret_prev": "",
        "lemonsqueezy_store_id": "store_1",
        "lemonsqueezy_variant_id": "variant_1",
        "lemonsqueezy_price_cents": 900,
        "lemonsqueezy_currency": "USD",
        "lemonsqueezy_credits_per_purchase": 200,
        "lemonsqueezy_credit_validity_days": 30,
        "lemonsqueezy_success_url": "https://app.thought2build.com/billing",
        "lemonsqueezy_test_mode": False,
        # Payment flags at their shipping defaults.
        "payments_enabled": False,
        "payment_provider": "razorpay",
        # Complete LIVE Razorpay config (configured + passes the guard).
        "razorpay_key_id": "rzp_live_abc123",
        "razorpay_key_secret": "rzp-secret",
        "razorpay_webhook_secret": "rzp-webhook-secret",
        "razorpay_webhook_secret_prev": "",
        "razorpay_price_cents": 79900,
        "razorpay_currency": "INR",
        "razorpay_credits_per_purchase": 200,
        "razorpay_credit_validity_days": 30,
        "razorpay_success_url": "https://app.thought2build.com/billing",
        "razorpay_checkout_ttl_minutes": 30,
    }
    base.update(overrides)
    return [patch.object(config.settings, key, value) for key, value in base.items()]


def _apply(patches: list) -> None:
    for p in patches:
        p.start()


def _undo(patches: list) -> None:
    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# Derived properties
# ---------------------------------------------------------------------------


def test_razorpay_enabled_requires_key_id_and_secret() -> None:
    with (
        patch.object(config.settings, "razorpay_key_id", "rzp_test_x"),
        patch.object(config.settings, "razorpay_key_secret", "s"),
    ):
        assert config.settings.razorpay_enabled is True
    # Missing either half of the Basic-auth pair disables it.
    for missing in ("razorpay_key_id", "razorpay_key_secret"):
        with (
            patch.object(config.settings, "razorpay_key_id", "rzp_test_x"),
            patch.object(config.settings, "razorpay_key_secret", "s"),
            patch.object(config.settings, missing, ""),
        ):
            assert config.settings.razorpay_enabled is False


def test_razorpay_webhook_secrets_current_first_non_empty() -> None:
    with (
        patch.object(config.settings, "razorpay_webhook_secret", "cur"),
        patch.object(config.settings, "razorpay_webhook_secret_prev", "prev"),
    ):
        assert config.settings.razorpay_webhook_secrets == ("cur", "prev")
    with (
        patch.object(config.settings, "razorpay_webhook_secret", "cur"),
        patch.object(config.settings, "razorpay_webhook_secret_prev", ""),
    ):
        assert config.settings.razorpay_webhook_secrets == ("cur",)
    with (
        patch.object(config.settings, "razorpay_webhook_secret", ""),
        patch.object(config.settings, "razorpay_webhook_secret_prev", ""),
    ):
        assert config.settings.razorpay_webhook_secrets == ()


# ---------------------------------------------------------------------------
# billing_checkout_enabled — the master gate matrix
# ---------------------------------------------------------------------------


def _both_providers_configured(**overrides: object):
    base: dict[str, object] = {
        "lemonsqueezy_api_key": "k",
        "lemonsqueezy_store_id": "s",
        "lemonsqueezy_variant_id": "v",
        "razorpay_key_id": "rzp_test_x",
        "razorpay_key_secret": "sec",
        "payments_enabled": True,
        "payment_provider": "razorpay",
    }
    base.update(overrides)
    return [patch.object(config.settings, key, value) for key, value in base.items()]


def test_checkout_disabled_when_payments_flag_off() -> None:
    """The master kill switch wins even with both providers configured."""
    patches = _both_providers_configured(payments_enabled=False)
    _apply(patches)
    try:
        assert config.settings.billing_checkout_enabled is False
    finally:
        _undo(patches)


def test_checkout_disabled_for_legacy_lemon() -> None:
    patches = _both_providers_configured(payment_provider="lemonsqueezy")
    _apply(patches)
    try:
        assert config.settings.billing_checkout_enabled is False
    finally:
        _undo(patches)


def test_checkout_enabled_for_active_configured_razorpay() -> None:
    patches = _both_providers_configured(payment_provider="razorpay")
    _apply(patches)
    try:
        assert config.settings.billing_checkout_enabled is True
    finally:
        _undo(patches)


def test_checkout_disabled_when_active_provider_unconfigured() -> None:
    """The gate reads the ACTIVE provider's config, not the other one's."""
    # Razorpay active but unconfigured — a fully configured Lemon must not count.
    patches = _both_providers_configured(
        payment_provider="razorpay", razorpay_key_id="", razorpay_key_secret=""
    )
    _apply(patches)
    try:
        assert config.settings.billing_checkout_enabled is False
    finally:
        _undo(patches)
    # Lemon active but unconfigured — a fully configured Razorpay must not count.
    patches = _both_providers_configured(
        payment_provider="lemonsqueezy", lemonsqueezy_api_key=""
    )
    _apply(patches)
    try:
        assert config.settings.billing_checkout_enabled is False
    finally:
        _undo(patches)


def test_checkout_fails_closed_on_unknown_provider() -> None:
    """A typo'd provider never resolves to some other provider's checkout."""
    patches = _both_providers_configured(payment_provider="stripe")
    _apply(patches)
    try:
        assert config.settings.billing_checkout_enabled is False
    finally:
        _undo(patches)


# ---------------------------------------------------------------------------
# Production guard — happy paths
# ---------------------------------------------------------------------------


def test_prod_guard_passes_with_complete_live_configs_payments_off() -> None:
    """Both providers live-configured, payments off — the shipping default boots."""
    patches = _valid_production()
    _apply(patches)
    try:
        config.validate_production_settings()  # must not raise
    finally:
        _undo(patches)


@pytest.mark.parametrize("provider", ["razorpay"])
def test_prod_guard_passes_with_payments_on_and_active_configured(
    provider: str,
) -> None:
    patches = _valid_production(payments_enabled=True, payment_provider=provider)
    _apply(patches)
    try:
        config.validate_production_settings()  # must not raise
    finally:
        _undo(patches)


def test_prod_guard_ignores_unconfigured_razorpay() -> None:
    """Blank Razorpay keys mean 'disabled' — no Razorpay branch fires."""
    patches = _valid_production(
        razorpay_key_id="",
        razorpay_key_secret="",
        razorpay_webhook_secret="",
        razorpay_success_url="",
    )
    _apply(patches)
    try:
        config.validate_production_settings()  # must not raise
    finally:
        _undo(patches)


def test_dev_environment_ignores_payment_guards() -> None:
    """Outside production the guards are a no-op (typo'd provider, test key, …)."""
    patches = _valid_production(
        environment="development",
        payment_provider="typo",
        payments_enabled=True,
        razorpay_key_id="rzp_test_abc",
        razorpay_webhook_secret="",
        razorpay_checkout_ttl_minutes=5,
    )
    _apply(patches)
    try:
        config.validate_production_settings()  # must not raise in development
    finally:
        _undo(patches)


# ---------------------------------------------------------------------------
# Production guard — feature-flag failures
# ---------------------------------------------------------------------------


def test_prod_guard_rejects_unknown_provider() -> None:
    patches = _valid_production(payment_provider="stripe")
    _apply(patches)
    try:
        with pytest.raises(RuntimeError) as exc:
            config.validate_production_settings()
        assert "PAYMENT_PROVIDER" in str(exc.value)
    finally:
        _undo(patches)


@pytest.mark.parametrize(
    ("provider", "unconfigure"),
    [
        ("razorpay", {"razorpay_key_id": "", "razorpay_key_secret": ""}),
        (
            "lemonsqueezy",
            {
                "lemonsqueezy_api_key": "",
                "lemonsqueezy_store_id": "",
                "lemonsqueezy_variant_id": "",
            },
        ),
    ],
)
def test_prod_guard_rejects_payments_on_with_unconfigured_active_provider(
    provider: str, unconfigure: dict[str, object]
) -> None:
    patches = _valid_production(
        payments_enabled=True, payment_provider=provider, **unconfigure
    )
    _apply(patches)
    try:
        with pytest.raises(RuntimeError) as exc:
            config.validate_production_settings()
        assert (
            "PAYMENTS_ENABLED" if provider == "razorpay" else "PAYMENT_PROVIDER"
        ) in str(exc.value)
        assert provider in str(exc.value)
    finally:
        _undo(patches)


# ---------------------------------------------------------------------------
# Production guard — Razorpay per-field failures (configured, active or not)
# ---------------------------------------------------------------------------


def test_prod_guard_rejects_test_key_in_production() -> None:
    patches = _valid_production(razorpay_key_id="rzp_test_abc123")
    _apply(patches)
    try:
        with pytest.raises(RuntimeError) as exc:
            config.validate_production_settings()
        assert "RAZORPAY_KEY_ID" in str(exc.value)
    finally:
        _undo(patches)


def test_prod_guard_rejects_blank_razorpay_webhook_secret() -> None:
    patches = _valid_production(razorpay_webhook_secret="")
    _apply(patches)
    try:
        with pytest.raises(RuntimeError) as exc:
            config.validate_production_settings()
        assert "RAZORPAY_WEBHOOK_SECRET" in str(exc.value)
    finally:
        _undo(patches)


def test_prod_guard_rejects_non_https_razorpay_success_url() -> None:
    patches = _valid_production(
        razorpay_success_url="http://app.thought2build.com/billing"
    )
    _apply(patches)
    try:
        with pytest.raises(RuntimeError) as exc:
            config.validate_production_settings()
        assert "RAZORPAY_SUCCESS_URL" in str(exc.value)
    finally:
        _undo(patches)


@pytest.mark.parametrize(
    ("field", "token"),
    [
        ("razorpay_price_cents", "RAZORPAY_PRICE_CENTS"),
        ("razorpay_credits_per_purchase", "RAZORPAY_CREDITS_PER_PURCHASE"),
        ("razorpay_credit_validity_days", "RAZORPAY_CREDIT_VALIDITY_DAYS"),
    ],
)
def test_prod_guard_rejects_non_positive_razorpay_economics(
    field: str, token: str
) -> None:
    patches = _valid_production(**{field: 0})
    _apply(patches)
    try:
        with pytest.raises(RuntimeError) as exc:
            config.validate_production_settings()
        assert token in str(exc.value)
    finally:
        _undo(patches)


def test_prod_guard_rejects_empty_razorpay_currency() -> None:
    patches = _valid_production(razorpay_currency="  ")
    _apply(patches)
    try:
        with pytest.raises(RuntimeError) as exc:
            config.validate_production_settings()
        assert "RAZORPAY_CURRENCY" in str(exc.value)
    finally:
        _undo(patches)


def test_prod_guard_rejects_checkout_ttl_under_16_minutes() -> None:
    patches = _valid_production(razorpay_checkout_ttl_minutes=15)
    _apply(patches)
    try:
        with pytest.raises(RuntimeError) as exc:
            config.validate_production_settings()
        assert "RAZORPAY_CHECKOUT_TTL_MINUTES" in str(exc.value)
    finally:
        _undo(patches)


def test_prod_guard_accepts_checkout_ttl_of_exactly_16_minutes() -> None:
    patches = _valid_production(razorpay_checkout_ttl_minutes=16)
    _apply(patches)
    try:
        config.validate_production_settings()  # boundary value must pass
    finally:
        _undo(patches)


def test_prod_guard_applies_to_configured_but_inactive_razorpay() -> None:
    """The Razorpay guard keys on CONFIGURED, not active (webhooks always run).

    With Lemon active and payments off, a configured Razorpay with a blank
    webhook secret must still fail startup — its webhook route processes
    regardless of the active-provider flag.
    """
    patches = _valid_production(
        payments_enabled=False,
        payment_provider="razorpay",
        razorpay_webhook_secret="",
    )
    _apply(patches)
    try:
        with pytest.raises(RuntimeError) as exc:
            config.validate_production_settings()
        assert "RAZORPAY_WEBHOOK_SECRET" in str(exc.value)
    finally:
        _undo(patches)
