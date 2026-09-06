"""Integration tests for the billing router (Phase 22 — T-296; issue #44).

App-level tests over the attempt-first checkout flow and ``checkout_ref`` polling.
They stub the DB session (no real Postgres) and monkeypatch the
``lemonsqueezy_service`` / ``razorpay_service`` singletons, mirroring the
in-process style of ``test_stripe_payments.py``.

Coverage:
  * GET /package returns the ACTIVE provider's economics (incl. currency) plus
    the issue-#44 ``enabled``/``provider`` fields; the kill switch flips
    ``enabled`` false while the numbers still render.
  * POST /checkout: 503 when disabled (kill switch off, or the active provider
    unconfigured); commits ``created`` BEFORE the provider call and
    ``provider_created`` AFTER; returns ``checkout_ref`` (never the raw nonce);
    provider failure marks the attempt ``failed`` → 502; a failed post-provider
    commit is an orphaned 502 that never leaks the checkout URL; rate-limited at
    6/hour; increments the checkout-created metric; the ``payment_provider``
    flag dispatches to Razorpay and snapshots the Razorpay economics.
  * GET /status: 200 only when completed + pack exists; 404 for pending; IDOR
    (cross-user) → 404 on the ``checkout_ref`` path; the pack lookup keys on the
    ATTEMPT's provider (issue #44), not a hardcoded one; legacy ``session_id``
    404s post-decommission.
  * GET /history reads ``billing_credit_packs``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from config import settings
from database import get_db
from main import create_app
from middleware.auth import get_current_user
from models import BillingCheckoutAttempt, BillingCreditPack, User
from services.credit_service import credit_service
from services.lemonsqueezy_service import lemonsqueezy_service
from services.razorpay_service import RazorpayError, razorpay_service


@pytest.fixture(autouse=True)
def mock_balance_sweep(monkeypatch):
    # Real persistence/expiry is exercised in test_payment_settlement_regressions.
    monkeypatch.setattr(credit_service, "get_balance", AsyncMock(return_value=200))
    monkeypatch.setattr(credit_service, "invalidate", AsyncMock())


_USER_ID = uuid4()
_USER = User(
    id=_USER_ID,
    email="buyer@example.com",
    google_id="google-buyer",
    name="Buyer",
    avatar_url=None,
    credit_balance=0,
    created_at=datetime.now(timezone.utc),
)


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class _FakeRedisPipeline:
    def zremrangebyscore(self, *args: Any) -> "_FakeRedisPipeline":
        return self

    def zadd(self, *args: Any) -> "_FakeRedisPipeline":
        return self

    def zcard(self, *args: Any) -> "_FakeRedisPipeline":
        return self

    def expire(self, *args: Any) -> "_FakeRedisPipeline":
        return self

    async def execute(self) -> list:
        return [0, 1, 1, 1]


class _NoopRedis:
    """Redis stub that always allows every rate-limit check."""

    async def eval(self, *args: Any, **kwargs: Any) -> int:
        return 1

    def pipeline(self) -> _FakeRedisPipeline:
        return _FakeRedisPipeline()


class _CountingRedis:
    """Redis stub enforcing the sliding-window rate-limit semantics in-process."""

    def __init__(self) -> None:
        self._sets: dict[str, dict[str, float]] = {}

    async def eval(self, script: str, num_keys: int, *args: Any) -> int:
        key = args[0]
        now_score = float(args[1])
        window_start = float(args[2])
        limit = int(args[3])
        member = args[4]
        ss = self._sets.setdefault(key, {})
        for m in [m for m, s in ss.items() if s <= window_start]:
            del ss[m]
        if len(ss) >= limit:
            return 0
        ss[member] = now_score
        return 1

    def pipeline(self) -> _FakeRedisPipeline:
        return _FakeRedisPipeline()


class _FakeSession:
    """Async session stub: ordered ``scalar`` results + commit snapshots.

    ``scalars_seq`` feeds successive ``db.scalar(...)`` calls; ``history`` backs
    the ``GET /history`` ``execute().scalars().all()`` read. Each ``commit()``
    records the tracked attempt's status so a test can assert the
    ``created`` → ``provider_created`` lifecycle. ``fail_commit_on`` (1-based)
    forces one commit to raise, simulating the orphaned-checkout path.
    """

    def __init__(
        self,
        *,
        scalars_seq: list[Any] | None = None,
        history: list[Any] | None = None,
        fail_commit_on: int | None = None,
    ) -> None:
        self._scalars = list(scalars_seq or [])
        self._history = list(history or [])
        self.added: list[Any] = []
        self.statements: list[Any] = []
        self.commit_statuses: list[str | None] = []
        self.commit_count = 0
        self.rollback_count = 0
        self._fail_commit_on = fail_commit_on
        self._attempt: BillingCheckoutAttempt | None = None

    def add(self, obj: Any) -> None:
        self.added.append(obj)
        if isinstance(obj, BillingCheckoutAttempt):
            self._attempt = obj

    async def commit(self) -> None:
        self.commit_count += 1
        if self._fail_commit_on == self.commit_count:
            from sqlalchemy.exc import OperationalError

            raise OperationalError("commit failed", None, Exception("boom"))
        self.commit_statuses.append(getattr(self._attempt, "status", None))

    async def refresh(self, obj: Any) -> None:
        return None

    async def rollback(self) -> None:
        self.rollback_count += 1

    async def scalar(self, statement: Any) -> Any:
        self.statements.append(statement)
        return self._scalars.pop(0) if self._scalars else None

    async def execute(self, statement: Any) -> Any:
        class _Result:
            def __init__(self, rows: list[Any]) -> None:
                self._rows = rows

            def scalars(self) -> "_Result":
                return self

            def all(self) -> list[Any]:
                return self._rows

        return _Result(self._history)


def _make_app(session: _FakeSession, *, redis: Any | None = None):
    app = create_app(redis_client=redis or _NoopRedis())

    async def _fake_user() -> User:
        return _USER

    async def _fake_db():
        yield session

    app.dependency_overrides[get_current_user] = _fake_user
    app.dependency_overrides[get_db] = _fake_db
    return app


def _enable_lemon():
    """Patch settings so Lemon is the active, enabled provider (issue #44).

    ``payments_enabled`` defaults False now, so checkout tests must switch the
    master flag on alongside the provider config (plan §2 behavioural note).
    """
    return [
        patch.object(settings, "payments_enabled", True),
        patch.object(settings, "payment_provider", "lemonsqueezy"),
        patch.object(settings, "lemonsqueezy_api_key", "lemon_key"),
        patch.object(settings, "lemonsqueezy_store_id", "store_1"),
        patch.object(settings, "lemonsqueezy_variant_id", "variant_1"),
        patch.object(settings, "lemonsqueezy_credits_per_purchase", 200),
        patch.object(settings, "lemonsqueezy_price_cents", 900),
        patch.object(settings, "lemonsqueezy_currency", "USD"),
        patch.object(settings, "lemonsqueezy_credit_validity_days", 30),
        patch.object(settings, "lemonsqueezy_checkout_ttl_minutes", 30),
    ]


def _enable_razorpay():
    """Patch settings so Razorpay is the active, enabled provider (issue #44).

    Economics deliberately differ from the Lemon block (150 credits / paise /
    45 days / 20-minute TTL) so a snapshot test can tell which provider's
    numbers were captured.
    """
    return [
        patch.object(settings, "payments_enabled", True),
        patch.object(settings, "payment_provider", "razorpay"),
        patch.object(settings, "razorpay_key_id", "rzp_test_abc"),
        patch.object(settings, "razorpay_key_secret", "rzp_secret"),
        patch.object(settings, "razorpay_credits_per_purchase", 150),
        patch.object(settings, "razorpay_price_cents", 79900),
        patch.object(settings, "razorpay_currency", "INR"),
        patch.object(settings, "razorpay_credit_validity_days", 45),
        patch.object(settings, "razorpay_checkout_ttl_minutes", 20),
    ]


class _Patches:
    """Apply a list of patch objects as one context manager."""

    def __init__(self, patches: list[Any]) -> None:
        self._patches = patches

    def __enter__(self) -> None:
        for p in self._patches:
            p.start()

    def __exit__(self, *exc: Any) -> None:
        for p in self._patches:
            p.stop()


def _completed_attempt() -> BillingCheckoutAttempt:
    return BillingCheckoutAttempt(
        checkout_ref="ref_done",
        user_id=_USER_ID,
        provider="lemonsqueezy",
        checkout_nonce_hash="hash",
        credits=200,
        price_cents=900,
        currency="USD",
        validity_days=30,
        status="completed",
        provider_order_id="ord_1",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=30),
    )


def _active_pack() -> BillingCreditPack:
    now = datetime.now(timezone.utc)
    return BillingCreditPack(
        id=uuid4(),
        user_id=_USER_ID,
        provider="lemonsqueezy",
        provider_order_id="ord_1",
        credits_purchased=200,
        credits_remaining=200,
        price_cents=900,
        currency="USD",
        paid_item_amount_cents=900,
        credits_revoked=0,
        credits_debt_recovered=0,
        refunded_item_amount_cents_processed=0,
        status="active",
        purchased_at=now,
        expires_at=now + timedelta(days=30),
    )


# ---------------------------------------------------------------------------
# GET /package
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_package_returns_lemon_config() -> None:
    app = _make_app(_FakeSession())
    with _Patches(_enable_lemon()):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/billing/package")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {
        "credits": 200,
        "price_cents": 900,
        "validity_days": 30,
        "currency": "USD",
        "enabled": False,
        "provider": "lemonsqueezy",
    }


@pytest.mark.asyncio
async def test_package_kill_switch_returns_numbers_with_enabled_false() -> None:
    # PAYMENTS_ENABLED=false (issue #44 AC #1): the package card still renders
    # the configured numbers, but the frontend gates the Buy button on enabled.
    app = _make_app(_FakeSession())
    patches = _enable_lemon()
    patches[0] = patch.object(settings, "payments_enabled", False)
    with _Patches(patches):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/billing/package")
    assert resp.status_code == 200
    body = resp.json()
    assert body["enabled"] is False
    assert body["provider"] == "lemonsqueezy"
    assert body["price_cents"] == 900


@pytest.mark.asyncio
async def test_package_razorpay_active_returns_razorpay_economics() -> None:
    app = _make_app(_FakeSession())
    with _Patches(_enable_razorpay()):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/billing/package")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {
        "credits": 150,
        "price_cents": 79900,
        "validity_days": 45,
        "currency": "INR",
        "enabled": True,
        "provider": "razorpay",
    }


@pytest.mark.asyncio
async def test_package_active_provider_unconfigured_enabled_false() -> None:
    # payments on, provider selected, but Razorpay has no key pair → fails closed.
    app = _make_app(_FakeSession())
    patches = _enable_razorpay()
    patches[2] = patch.object(settings, "razorpay_key_id", "")
    with _Patches(patches):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/billing/package")
    assert resp.status_code == 200
    assert resp.json()["enabled"] is False


# ---------------------------------------------------------------------------
# POST /checkout
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_checkout_disabled_returns_503() -> None:
    session = _FakeSession()
    app = _make_app(session)
    # Payments on but the active provider unconfigured (empty api key) → 503,
    # and no attempt is ever committed.
    patches = _enable_lemon()
    patches[2] = patch.object(settings, "lemonsqueezy_api_key", "")
    with _Patches(patches):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/billing/checkout")
    assert resp.status_code == 503
    assert session.commit_count == 0


@pytest.mark.asyncio
async def test_checkout_kill_switch_returns_503() -> None:
    session = _FakeSession()
    app = _make_app(session)
    # Provider fully configured but PAYMENTS_ENABLED=false → 503 (issue #44 AC #1).
    patches = _enable_lemon()
    patches[0] = patch.object(settings, "payments_enabled", False)
    with _Patches(patches):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/billing/checkout")
    assert resp.status_code == 503
    assert session.commit_count == 0


@pytest.mark.asyncio
async def test_checkout_unknown_provider_returns_503() -> None:
    session = _FakeSession()
    app = _make_app(session)
    # An unknown selector must fail closed, never fall through to some gateway.
    patches = _enable_lemon()
    patches[1] = patch.object(settings, "payment_provider", "paypal")
    with _Patches(patches):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/billing/checkout")
    assert resp.status_code == 503
    assert session.commit_count == 0


@pytest.mark.asyncio
async def test_checkout_attempt_lifecycle_created_then_provider_created() -> None:
    session = _FakeSession()
    app = _make_app(session)

    captured: dict[str, Any] = {}

    async def _fake_create_checkout(attempt, user, *, checkout_nonce):  # type: ignore[no-untyped-def]
        # The attempt must already be committed as 'created' before Lemon is called.
        captured["status_at_call"] = attempt.status
        captured["commits_before_lemon"] = session.commit_count
        captured["nonce"] = checkout_nonce
        captured["nonce_hash"] = attempt.checkout_nonce_hash
        captured["checkout_ref"] = attempt.checkout_ref
        return "co_123", "https://rzp.io/i/abc"

    with _Patches(_enable_razorpay()):
        with patch.object(
            razorpay_service, "create_payment_link", _fake_create_checkout
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                resp = await client.post("/billing/checkout")

    assert resp.status_code == 200
    body = resp.json()
    assert body["checkout_url"] == "https://rzp.io/i/abc"
    checkout_ref = body["checkout_ref"]
    assert checkout_ref and checkout_ref == captured["checkout_ref"]

    # Committed 'created' before Lemon, 'provider_created' after.
    assert captured["status_at_call"] == "created"
    assert captured["commits_before_lemon"] == 1
    assert session.commit_statuses == ["created", "provider_created"]

    # Only the sha256 hash is persisted — the raw nonce is never returned.
    import hashlib

    assert (
        captured["nonce_hash"]
        == hashlib.sha256(captured["nonce"].encode("utf-8")).hexdigest()
    )
    assert captured["nonce"] not in resp.text
    assert captured["nonce_hash"] not in resp.text

    attempt = session.added[0]
    assert isinstance(attempt, BillingCheckoutAttempt)
    assert attempt.provider == "razorpay"
    assert attempt.provider_checkout_id == "co_123"
    assert attempt.status == "provider_created"


@pytest.mark.asyncio
async def test_checkout_provider_failure_marks_attempt_failed_502() -> None:
    session = _FakeSession()
    app = _make_app(session)

    async def _boom(attempt, user, *, checkout_nonce):  # type: ignore[no-untyped-def]
        raise RazorpayError("provider down")

    with _Patches(_enable_razorpay()):
        with patch.object(razorpay_service, "create_payment_link", _boom):
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                resp = await client.post("/billing/checkout")

    assert resp.status_code == 502
    # Committed 'created', then re-committed 'failed' after the provider error.
    assert session.commit_statuses == ["created", "failed"]
    assert session.added[0].status == "failed"


@pytest.mark.asyncio
async def test_checkout_orphaned_commit_failure_502_no_url() -> None:
    # Fail the SECOND commit (the provider_created transition) — Lemon already
    # minted the checkout, but Thought2Build cannot record it.
    session = _FakeSession(fail_commit_on=2)
    app = _make_app(session)

    async def _ok(attempt, user, *, checkout_nonce):  # type: ignore[no-untyped-def]
        return "co_orphan", "https://rzp.io/i/secret-url"

    with _Patches(_enable_razorpay()):
        with patch.object(razorpay_service, "create_payment_link", _ok):
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                resp = await client.post("/billing/checkout")

    assert resp.status_code == 502
    # The orphaned path must NEVER expose the checkout URL.
    assert "secret-url" not in resp.text
    assert session.rollback_count == 1


@pytest.mark.asyncio
async def test_checkout_rate_limit_sixth_returns_429() -> None:
    session = _FakeSession()
    app = _make_app(session, redis=_CountingRedis())

    async def _ok(attempt, user, *, checkout_nonce):  # type: ignore[no-untyped-def]
        return "co_x", "https://rzp.io/i/x"

    # The rate-limit middleware needs decodable claims to scope per-user; the
    # CSRF middleware uses its own (unpatched) decoder, so it still sees no
    # session for the fake token and lets the request through.
    with _Patches(_enable_razorpay()):
        with (
            patch(
                "middleware.rate_limit.decode_access_token_claims",
                return_value={"sub": str(_USER_ID)},
            ),
            patch.object(razorpay_service, "create_payment_link", _ok),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                statuses = []
                for _ in range(6):
                    resp = await client.post(
                        "/billing/checkout",
                        headers={"Authorization": "Bearer fake-token"},
                    )
                    statuses.append(resp.status_code)

    assert statuses[:5] == [200] * 5, statuses
    assert statuses[5] == 429


@pytest.mark.asyncio
async def test_checkout_created_metric_increments() -> None:
    from services.observability import BILLING_CHECKOUT_CREATED

    session = _FakeSession()
    app = _make_app(session)

    async def _ok(attempt, user, *, checkout_nonce):  # type: ignore[no-untyped-def]
        return "co_x", "https://rzp.io/i/x"

    created = BILLING_CHECKOUT_CREATED.labels(provider="razorpay")
    before = created._value.get()
    with _Patches(_enable_razorpay()):
        with patch.object(razorpay_service, "create_payment_link", _ok):
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                resp = await client.post("/billing/checkout")

    assert resp.status_code == 200
    assert created._value.get() == before + 1


# ---------------------------------------------------------------------------
# POST /checkout — Razorpay provider dispatch (issue #44)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_checkout_razorpay_dispatches_and_snapshots_razorpay_economics() -> None:
    session = _FakeSession()
    app = _make_app(session)

    captured: dict[str, Any] = {}

    async def _fake_create_payment_link(attempt, user, *, checkout_nonce):  # type: ignore[no-untyped-def]
        captured["status_at_call"] = attempt.status
        captured["commits_before_provider"] = session.commit_count
        return "plink_123", "https://rzp.io/i/abc"

    async def _lemon_must_not_be_called(attempt, user, *, checkout_nonce):  # type: ignore[no-untyped-def]
        raise AssertionError("lemonsqueezy_service must not be dispatched")

    before = datetime.now(timezone.utc)
    with _Patches(_enable_razorpay()):
        with (
            patch.object(
                razorpay_service, "create_payment_link", _fake_create_payment_link
            ),
            patch.object(
                lemonsqueezy_service, "create_checkout", _lemon_must_not_be_called
            ),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                resp = await client.post("/billing/checkout")

    assert resp.status_code == 200
    assert resp.json()["checkout_url"] == "https://rzp.io/i/abc"

    # Attempt-first shape holds under dispatch: committed 'created' before the
    # provider call, 'provider_created' after.
    assert captured["status_at_call"] == "created"
    assert captured["commits_before_provider"] == 1
    assert session.commit_statuses == ["created", "provider_created"]

    # The attempt snapshots the RAZORPAY economics (distinct fixture values) and
    # the razorpay TTL, and records the plink id.
    attempt = session.added[0]
    assert isinstance(attempt, BillingCheckoutAttempt)
    assert attempt.provider == "razorpay"
    assert attempt.credits == 150
    assert attempt.price_cents == 79900
    assert attempt.currency == "INR"
    assert attempt.validity_days == 45
    assert attempt.provider_checkout_id == "plink_123"
    ttl = attempt.expires_at - before
    assert timedelta(minutes=19) < ttl < timedelta(minutes=21)


@pytest.mark.asyncio
async def test_checkout_razorpay_failure_marks_attempt_failed_502() -> None:
    from services.observability import BILLING_CHECKOUT_API_ERROR

    session = _FakeSession()
    app = _make_app(session)

    async def _boom(attempt, user, *, checkout_nonce):  # type: ignore[no-untyped-def]
        raise RazorpayError("provider down")

    errors = BILLING_CHECKOUT_API_ERROR.labels(
        provider="razorpay", error_type="provider_error"
    )
    before = errors._value.get()
    with _Patches(_enable_razorpay()):
        with patch.object(razorpay_service, "create_payment_link", _boom):
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                resp = await client.post("/billing/checkout")

    assert resp.status_code == 502
    assert session.commit_statuses == ["created", "failed"]
    assert session.added[0].status == "failed"
    assert errors._value.get() == before + 1


@pytest.mark.asyncio
async def test_checkout_razorpay_created_metric_uses_provider_label() -> None:
    from services.observability import BILLING_CHECKOUT_CREATED

    session = _FakeSession()
    app = _make_app(session)

    async def _ok(attempt, user, *, checkout_nonce):  # type: ignore[no-untyped-def]
        return "plink_x", "https://rzp.io/i/x"

    created = BILLING_CHECKOUT_CREATED.labels(provider="razorpay")
    before = created._value.get()
    with _Patches(_enable_razorpay()):
        with patch.object(razorpay_service, "create_payment_link", _ok):
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                resp = await client.post("/billing/checkout")

    assert resp.status_code == 200
    assert created._value.get() == before + 1


# ---------------------------------------------------------------------------
# GET /status — checkout_ref path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_checkout_ref_completed_returns_200() -> None:
    attempt = _completed_attempt()
    pack = _active_pack()
    session = _FakeSession(scalars_seq=[attempt, pack])
    app = _make_app(session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/billing/status", params={"checkout_ref": "ref_done"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["credits_added"] == 200
    # First-touch telemetry was stamped + committed.
    assert attempt.success_redirect_seen_at is not None
    assert session.commit_count == 2


@pytest.mark.asyncio
async def test_status_pack_lookup_uses_attempt_provider() -> None:
    # Issue #44: the pack query keys on the ATTEMPT's provider, not a hardcoded
    # 'lemonsqueezy' — a Razorpay attempt must resolve its Razorpay pack.
    attempt = _completed_attempt()
    attempt.provider = "razorpay"
    attempt.provider_order_id = "pay_123"
    pack = _active_pack()
    pack.provider = "razorpay"
    pack.provider_order_id = "pay_123"
    session = _FakeSession(scalars_seq=[attempt, pack])
    app = _make_app(session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/billing/status", params={"checkout_ref": "ref_done"})

    assert resp.status_code == 200
    # The second scalar() call is the pack lookup; its bound parameters must
    # carry the attempt's provider.
    pack_query_params = session.statements[1].compile().params
    assert "razorpay" in pack_query_params.values()
    assert "lemonsqueezy" not in pack_query_params.values()


@pytest.mark.asyncio
async def test_status_checkout_ref_pending_returns_404() -> None:
    attempt = _completed_attempt()
    attempt.status = "provider_created"  # not yet granted
    attempt.provider_order_id = None
    session = _FakeSession(scalars_seq=[attempt])
    app = _make_app(session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/billing/status", params={"checkout_ref": "ref_done"})

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_status_checkout_ref_cross_user_returns_404_idor() -> None:
    # The user-scoped query yields nothing for someone else's ref → 404 (not 403).
    session = _FakeSession(scalars_seq=[None])
    app = _make_app(session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/billing/status", params={"checkout_ref": "someone_elses_ref"}
        )

    assert resp.status_code == 404
    assert session.commit_count == 0  # nothing to stamp for a non-matching ref


# ---------------------------------------------------------------------------
# GET /status — legacy session_id grace path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_legacy_session_id_ignored_after_decommission() -> None:
    # T-308: the legacy Stripe ``session_id`` polling path is gone. A request with
    # only session_id has no usable identifier and is answered 404 (the StripeCreditPack
    # table is never queried — a pack present in the session is not returned).
    session = _FakeSession(scalars_seq=[_active_pack()])
    app = _make_app(session)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/billing/status", params={"session_id": "cs_legacy"})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_status_no_identifier_returns_404() -> None:
    session = _FakeSession()
    app = _make_app(session)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/billing/status")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /history
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_history_reads_billing_credit_packs() -> None:
    pack = _active_pack()
    session = _FakeSession(history=[pack])
    app = _make_app(session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/billing/history")

    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) == 1
    assert rows[0]["credits_purchased"] == 200
    assert rows[0]["currency"] == "USD"
    assert rows[0]["status"] == "active"


# ---------------------------------------------------------------------------
# POST /admin/correction — wired require_admin authorization (T-302)
# ---------------------------------------------------------------------------
#
# These drive the real Depends(require_admin) chain through the ASGI app to prove
# the endpoint is wired and CSRF lets an authenticated request reach authz. The
# grant/idempotency/debt money-path correctness is covered against a real Postgres
# in test_billing_admin_correction.py.

_ADMIN_CORRECTION_BODY = {
    "provider": "lemonsqueezy",
    "provider_order_id": "ord_admin_http",
    "target_user_id": str(uuid4()),
    "credits": 200,
    "price_cents": 900,
    "currency": "USD",
    "reason": "paid order, webhook never arrived",
    "evidence_url": "https://support.thought2build.com/tickets/1",
}


@pytest.mark.asyncio
async def test_admin_correction_forbidden_for_non_admin() -> None:
    session = _FakeSession()
    app = _make_app(session)  # _USER (buyer@example.com) is the authenticated user
    with _Patches(
        [patch.object(settings, "admin_user_emails", "admin@thought2build.com")]
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/billing/admin/correction", json=_ADMIN_CORRECTION_BODY
            )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_admin_correction_forbidden_when_allowlist_empty() -> None:
    session = _FakeSession()
    app = _make_app(session)
    # Empty allowlist authorises no one — not even an otherwise-plausible admin.
    with _Patches([patch.object(settings, "admin_user_emails", "")]):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/billing/admin/correction", json=_ADMIN_CORRECTION_BODY
            )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Provider-aware helpers (issue #44)
# ---------------------------------------------------------------------------


def test_admin_correction_request_accepts_razorpay_provider() -> None:
    from schemas.billing import AdminCorrectionRequest

    body = AdminCorrectionRequest(
        provider="razorpay",
        provider_order_id="pay_admin_1",
        target_user_id=uuid4(),
        credits=150,
        price_cents=79900,
        currency="INR",
        reason="paid order, webhook never arrived",
        evidence_url="https://support.thought2build.com/tickets/2",
    )
    assert body.provider == "razorpay"


def test_credit_validity_days_for_is_provider_aware() -> None:
    from routers.billing import _credit_validity_days_for

    with _Patches(
        [
            patch.object(settings, "razorpay_credit_validity_days", 45),
            patch.object(settings, "lemonsqueezy_credit_validity_days", 30),
        ]
    ):
        assert _credit_validity_days_for("razorpay") == 45
        assert _credit_validity_days_for("lemonsqueezy") == 30
        # The retained stripe audit provider falls back to the Lemon window.
        assert _credit_validity_days_for("stripe") == 30
