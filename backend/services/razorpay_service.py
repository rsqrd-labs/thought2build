"""Razorpay integration service for Thought2Build credit purchases.

Razorpay is the gateway for new credit purchases. Hosted Payment Links use the
committed checkout attempt's economics and proof. Authenticated payment reads
recover missing refunds; authenticated reads of a recorded Payment Link and its
payment can recover a missing first grant through the shared settlement worker.

Security contract (same as the Lemon mirror):
  * The key pair and the hosted link URL are **never logged**. Failure logs
    carry only structured fields (checkout_ref, attempt id, HTTP status,
    attempt number) — never response bodies, headers, or the raw nonce.
  * The raw ``checkout_nonce`` is passed in by the caller (the attempt row
    stores only its sha256); it is embedded in the link ``notes`` so the signed
    webhook can prove it back, and is never logged.
  * ``notes`` scalars are sent as strings — the webhook round-trip delivers
    them back verbatim and the worker reads them as strings, matching Lemon's
    stringified custom data.
  * ``reference_id`` is ``str(attempt.id)`` (36 chars), not ``checkout_ref``
    (43 chars) — Razorpay caps ``reference_id`` at 40 (D9). Correlation
    authority is ``notes.checkout_ref`` anyway, same as Lemon custom data.

The service is pure (no DB session): on retry exhaustion it raises and the
caller marks the attempt failed → 502.
"""

from __future__ import annotations

import asyncio
import logging
import random
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, AsyncIterator
from urllib.parse import urlencode, urlparse

import httpx

from config import settings

if TYPE_CHECKING:  # pragma: no cover - typing only
    from models import User
    from models.billing_checkout_attempt import BillingCheckoutAttempt

logger = logging.getLogger(__name__)

# Per-operation timeouts (same policy as the Lemon service): connect 10s /
# read 30s / write 10s / pool 5s. A slow provider must not pin a request
# worker indefinitely.
_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0)

# Bounded connection pool — the billing path is low-QPS; cap so a provider stall
# cannot exhaust file descriptors.
_LIMITS = httpx.Limits(max_connections=10, max_keepalive_connections=5)

# Retry policy for create_payment_link: at most two retries (three attempts
# total) on transient failures only. 4xx (other than 429) is a contract error —
# never retried. Backoff is exponential with full jitter to avoid a thundering
# herd.
_MAX_RETRIES = 2
_BACKOFF_BASE_SECONDS = 0.5
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class RazorpayError(Exception):
    """A Razorpay API call failed (non-2xx after retries, or a parse error)."""


# Hosted Payment-Link hosts we will hand to the browser. The short_url comes
# from the authenticated Razorpay API, but we still refuse anything off-domain
# so a compromised/misconfigured response cannot become an open redirect
# (F6 — issue #42). The frontend also validates (safeCheckoutUrl).
_RAZORPAY_CHECKOUT_HOST_SUFFIXES = (".razorpay.com", ".rzp.io")


def _assert_allowed_checkout_url(url: str) -> None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    allowed = host.endswith(_RAZORPAY_CHECKOUT_HOST_SUFFIXES) or host in (
        "razorpay.com",
        "rzp.io",
    )
    if parsed.scheme != "https" or not allowed:
        raise RazorpayError(
            "Razorpay returned a payment-link URL off the expected domain"
        )


class RazorpayRateLimitError(RazorpayError):
    """Razorpay returned 429. Carries ``retry_after`` so the caller backs off.

    Raised by :meth:`RazorpayService.get_payment` (reconcile lane 2): the
    re-read lane surfaces the rate limit rather than retrying inline.
    """

    def __init__(self, retry_after: float) -> None:
        super().__init__(f"Razorpay rate limited; retry after {retry_after:.1f}s")
        # Always defer at least a second so a tight loop cannot spin.
        self.retry_after = max(1.0, float(retry_after))


@dataclass(frozen=True)
class RazorpayPayment:
    """Normalised view of a Razorpay payment (reconcile lane 2).

    ``status`` is the provider status string (``captured`` / ``refunded`` /
    ``authorized`` / ``failed`` / …). ``refund_status`` is ``""`` (never
    refunded), ``"partial"``, or ``"full"`` — Razorpay sends ``null`` for the
    never-refunded case, normalised here to the empty string. Amounts are
    integer minor units (paise for INR — D8).
    """

    payment_id: str
    status: str
    amount_cents: int
    amount_refunded_cents: int
    refund_status: str


def razorpay_environment() -> str:
    """The ``notes.environment`` marker the webhook receiver validates against.

    Razorpay events carry no test-mode flag — the key prefix IS the
    environment (production startup requires ``rzp_live_``): ``"live"`` iff the
    configured key id is a live key, ``"test"`` otherwise. Public because it is
    the single source of truth for both sides of the round-trip: link creation
    stamps it into ``notes`` here, and the webhook receiver
    (``routers/billing.py``) enforces it as the test/live guard (issue #44).
    """
    return "live" if settings.razorpay_key_id.startswith("rzp_live_") else "test"


def _retry_after_seconds(response: httpx.Response) -> float:
    """Best-effort backoff from the ``Retry-After`` header (defaults to 1s)."""
    raw = response.headers.get("retry-after")
    if raw is not None:
        try:
            return max(1.0, float(raw))
        except (TypeError, ValueError):
            pass
    return 1.0


class RazorpayService:
    """All Razorpay API interactions for Thought2Build credit purchases.

    Follows the service-singleton pattern: one instance is created at module
    level and imported by name in the billing router and the reconcile worker::

        from services.razorpay_service import razorpay_service

    Both public methods accept an optional ``client`` so tests can inject an
    ``httpx.AsyncClient`` backed by a ``MockTransport``; production passes
    ``None`` and a configured client is built (and closed) per call.
    """

    # ------------------------------------------------------------------
    # Client / auth
    # ------------------------------------------------------------------

    @staticmethod
    def _auth() -> httpx.BasicAuth:
        """HTTP Basic auth built per-call (reads the key pair at call time).

        Building auth lazily keeps the module-level singleton importable when
        billing is disabled (empty key pair) — the credentials are only
        materialised when an enabled caller actually mints a link.
        """
        return httpx.BasicAuth(
            username=settings.razorpay_key_id,
            password=settings.razorpay_key_secret,
        )

    @asynccontextmanager
    async def _client_ctx(
        self, client: httpx.AsyncClient | None
    ) -> AsyncIterator[httpx.AsyncClient]:
        """Yield the injected client, or build+close a configured one."""
        if client is not None:
            yield client
            return
        async with httpx.AsyncClient(
            base_url=settings.razorpay_api_base,
            timeout=_TIMEOUT,
            limits=_LIMITS,
        ) as owned:
            yield owned

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def create_payment_link(
        self,
        attempt: "BillingCheckoutAttempt",
        user: "User",
        *,
        checkout_nonce: str,
        client: httpx.AsyncClient | None = None,
    ) -> tuple[str, str]:
        """Create a hosted Payment Link for ``attempt`` and return ``(id, url)``.

        ``checkout_nonce`` is the **raw** nonce (the attempt row stores only its
        sha256); it is embedded in the link ``notes`` so the signed
        ``payment_link.paid`` webhook can prove it back. The charged amount is
        the attempt's ``price_cents`` snapshot (integer paise), partial payment
        is off, and the callback carries ``?checkout_ref=…`` so the browser
        return can be correlated to the attempt (Razorpay appends its own
        ``razorpay_*`` params with ``&``; the frontend reads only
        ``checkout_ref`` — the browser return is telemetry-only, the webhook is
        the authority).

        Returns ``(provider_checkout_id, checkout_url)`` — the ``plink_…`` id
        and the hosted ``short_url`` (D7: the payment id ``pay_…`` becomes
        ``provider_order_id`` later, at grant time).

        Raises:
            RazorpayError: a 4xx contract error, or transient failures that
                outlast the bounded retries (caller marks the attempt failed →
                502).
        """
        payload = self._build_payment_link_payload(attempt, user, checkout_nonce)

        async with self._client_ctx(client) as http:
            for attempt_no in range(_MAX_RETRIES + 1):
                try:
                    response = await http.post(
                        "/v1/payment_links",
                        json=payload,
                        auth=self._auth(),
                    )
                except httpx.RequestError as exc:
                    # Network/timeout — transient. Retry unless exhausted.
                    if attempt_no >= _MAX_RETRIES:
                        logger.error(
                            "billing.razorpay_link_network_failed "
                            "checkout_ref=%s attempt_id=%s try=%d",
                            attempt.checkout_ref,
                            str(attempt.id),
                            attempt_no,
                        )
                        raise RazorpayError(
                            "Razorpay payment link creation failed (network)"
                        ) from exc
                    await self._sleep_backoff(attempt_no)
                    continue

                if response.status_code in (200, 201):
                    return self._parse_payment_link_response(response, attempt)

                # 4xx (other than 429) is a contract error — never retried.
                if response.status_code not in _RETRYABLE_STATUS:
                    logger.error(
                        "billing.razorpay_link_rejected "
                        "checkout_ref=%s attempt_id=%s status=%d",
                        attempt.checkout_ref,
                        str(attempt.id),
                        response.status_code,
                    )
                    raise RazorpayError(
                        f"Razorpay rejected the payment link (HTTP "
                        f"{response.status_code})"
                    )

                # Transient (429/5xx) — retry unless exhausted.
                if attempt_no >= _MAX_RETRIES:
                    logger.error(
                        "billing.razorpay_link_transient_exhausted "
                        "checkout_ref=%s attempt_id=%s status=%d try=%d",
                        attempt.checkout_ref,
                        str(attempt.id),
                        response.status_code,
                        attempt_no,
                    )
                    raise RazorpayError(
                        f"Razorpay payment link creation failed after retries "
                        f"(HTTP {response.status_code})"
                    )
                await self._sleep_backoff(attempt_no)

        # Unreachable: the loop either returns or raises on the final attempt.
        raise RazorpayError("Razorpay payment link creation failed")

    async def get_payment(
        self,
        payment_id: str,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> RazorpayPayment:
        """Re-read one payment Thought2Build already has by id (reconcile lane 2).

        This is a single GET — it is **never** used to discover or prove a
        payment (the signed webhook is the grant authority). A 429 is surfaced
        as :class:`RazorpayRateLimitError` (carrying ``Retry-After``) so lane 2
        backs off; any other non-2xx raises :class:`RazorpayError`.
        """
        async with self._client_ctx(client) as http:
            try:
                response = await http.get(
                    f"/v1/payments/{payment_id}",
                    auth=self._auth(),
                )
            except httpx.RequestError as exc:
                raise RazorpayError(
                    "Razorpay payment re-read failed (network)"
                ) from exc

        if response.status_code == 429:
            # Surface the rate limit so lane 2 backs off (does NOT retry inline).
            raise RazorpayRateLimitError(_retry_after_seconds(response))

        if response.status_code != 200:
            logger.error(
                "billing.razorpay_payment_read_failed payment_id=%s status=%d",
                payment_id,
                response.status_code,
            )
            raise RazorpayError(
                f"Razorpay payment re-read failed (HTTP {response.status_code})"
            )

        return self._parse_payment_response(response, payment_id)

    async def _get_entity(
        self,
        path: str,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> dict:
        """Authenticated read; callers persist only allow-listed fields."""
        async with self._client_ctx(client) as http:
            try:
                response = await http.get(path, auth=self._auth())
            except httpx.RequestError as exc:
                raise RazorpayError("Razorpay settlement read failed") from exc
        if response.status_code == 429:
            raise RazorpayRateLimitError(_retry_after_seconds(response))
        if response.status_code != 200:
            raise RazorpayError(
                f"Razorpay settlement read failed (HTTP {response.status_code})"
            )
        try:
            data = response.json()
        except ValueError as exc:
            raise RazorpayError("Malformed Razorpay settlement response") from exc
        if not isinstance(data, dict):
            raise RazorpayError("Malformed Razorpay settlement response")
        return data

    async def get_dispute(self, dispute_id: str) -> dict:
        data = await self._get_entity(f"/v1/disputes/{dispute_id}")
        if data.get("id") != dispute_id or data.get("status") not in {
            "open",
            "under_review",
            "action_required",
            "lost",
            "won",
            "closed",
        }:
            raise RazorpayError("Dispute identity or status mismatch")
        return data

    async def get_paid_checkout(
        self,
        attempt: "BillingCheckoutAttempt",
        *,
        client: httpx.AsyncClient | None = None,
    ) -> dict | None:
        """Recover payment ONLY through this attempt's recorded Payment Link.

        The authenticated link provides captured payment membership, and its
        notes prove the original nonce/owner. A fresh payment read supplies the
        current cumulative refund state, so recovery cannot spend refunded money.
        The shared grant validator checks the economics and ownership again.
        """
        link_id = attempt.provider_checkout_id
        if not link_id:
            return None
        link = await self._get_entity(f"/v1/payment_links/{link_id}", client=client)
        if link.get("id") != link_id or link.get("reference_id") != str(attempt.id):
            raise RazorpayError("Payment Link identity mismatch")
        if link.get("status") != "paid":
            return None
        payments = link.get("payments")
        if not isinstance(payments, list) or len(payments) != 1:
            raise RazorpayError("Expected one full Payment Link payment")
        member = payments[0]
        payment_id = member.get("payment_id") if isinstance(member, dict) else None
        if not isinstance(payment_id, str) or not payment_id.startswith("pay_"):
            raise RazorpayError("Missing Payment Link payment identity")
        payment = await self._get_entity(f"/v1/payments/{payment_id}", client=client)
        if payment.get("id") != payment_id:
            raise RazorpayError("Payment identity mismatch")
        if link.get("order_id") and payment.get("order_id") != link["order_id"]:
            raise RazorpayError("Payment does not belong to the Payment Link order")
        if payment.get("status") not in ("captured", "refunded"):
            raise RazorpayError("Payment has not been captured")
        # Same normalization/proof checks as signed webhook ingress. Local import
        # avoids a router/service import cycle; raw notes never reach the inbox.
        from routers.billing import _normalize_razorpay_link_paid

        return _normalize_razorpay_link_paid(
            {
                "payment_link": {"entity": link},
                "payment": {"entity": payment},
            }
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _build_payment_link_payload(
        attempt: "BillingCheckoutAttempt",
        user: "User",
        checkout_nonce: str,
    ) -> dict:
        """Assemble the ``payment_links`` create body for ``attempt``."""
        # ?checkout_ref=… on the callback so the browser return correlates.
        # Razorpay appends its own razorpay_* params to this URL with '&'.
        callback_url = (
            f"{settings.razorpay_success_url}"
            f"?{urlencode({'checkout_ref': attempt.checkout_ref})}"
        )
        return {
            # Attempt-snapshot economics (integer paise), immune to config
            # changes and provider tampering.
            "amount": attempt.price_cents,
            "currency": attempt.currency,
            # Full payment only — grant validation compares the full amount.
            "accept_partial": False,
            # str(attempt.id) is 36 chars; Razorpay caps reference_id at 40
            # (checkout_ref is 43 — D9). notes.checkout_ref is the authority.
            "reference_id": str(attempt.id),
            "description": f"Thought2Build — {attempt.credits} credits",
            # Pre-fills the hosted form for UX only — never trusted for user
            # resolution (that is the signed notes block below).
            "customer": {"email": user.email},
            # Thought2Build owns all communication with the buyer.
            "notify": {"sms": False, "email": False},
            "reminder_enable": False,
            # Attempt TTL — the hosted page expires with the attempt.
            "expire_by": int(attempt.expires_at.timestamp()),
            "callback_url": callback_url,
            "callback_method": "get",
            "notes": {
                # Strings: the webhook round-trip delivers notes back verbatim
                # and the worker reads them as strings (Lemon parity).
                "user_id": str(attempt.user_id),
                "checkout_ref": attempt.checkout_ref,
                "checkout_nonce": checkout_nonce,
                "environment": razorpay_environment(),
                "credits": str(attempt.credits),
                "price_cents": str(attempt.price_cents),
                "currency": attempt.currency,
            },
        }

    @staticmethod
    def _parse_payment_link_response(
        response: httpx.Response,
        attempt: "BillingCheckoutAttempt",
    ) -> tuple[str, str]:
        """Extract ``(provider_checkout_id, checkout_url)`` from a 2xx response."""
        try:
            data = response.json()
            provider_checkout_id = str(data["id"])
            checkout_url = str(data["short_url"])
        except (KeyError, TypeError, ValueError) as exc:
            logger.error(
                "billing.razorpay_link_malformed checkout_ref=%s attempt_id=%s",
                attempt.checkout_ref,
                str(attempt.id),
            )
            raise RazorpayError(
                "Razorpay returned a malformed payment link response"
            ) from exc
        _assert_allowed_checkout_url(checkout_url)
        return provider_checkout_id, checkout_url

    @staticmethod
    def _parse_payment_response(
        response: httpx.Response,
        payment_id: str,
    ) -> RazorpayPayment:
        """Normalise a 2xx payment response (status + refund totals)."""
        try:
            data = response.json()
            parsed_id = str(data["id"])
            if parsed_id != payment_id:
                raise ValueError("Payment identity mismatch")
        except (KeyError, TypeError, ValueError) as exc:
            logger.error("billing.razorpay_payment_malformed payment_id=%s", payment_id)
            raise RazorpayError(
                "Razorpay returned a malformed payment response"
            ) from exc
        return RazorpayPayment(
            payment_id=parsed_id,
            status=str(data.get("status") or ""),
            amount_cents=int(data.get("amount") or 0),
            amount_refunded_cents=int(data.get("amount_refunded") or 0),
            # null (never refunded) normalises to "" — lane 2's reversal
            # predicate checks membership in ("partial", "full").
            refund_status=str(data.get("refund_status") or ""),
        )

    @staticmethod
    async def _sleep_backoff(attempt_no: int) -> None:
        """Sleep exponential-backoff-with-full-jitter before the next retry."""
        ceiling = _BACKOFF_BASE_SECONDS * (2**attempt_no)
        delay = random.uniform(0.0, ceiling)  # nosec B311 — jitter, not security
        await asyncio.sleep(delay)


# Module-level singleton — imported by name in the billing router (Step 4) and
# the reconcile worker (Step 5):
#   from services.razorpay_service import razorpay_service
razorpay_service = RazorpayService()
