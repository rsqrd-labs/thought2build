"""SQLAlchemy ORM model for github_webhook_events.

Idempotency / dedup record for inbound GitHub webhook deliveries — mirrors
``StripeWebhookEvent``. Every delivery gets an INSERT here before reconciliation
work is enqueued. The UNIQUE constraint on ``delivery_id`` (the
``X-GitHub-Delivery`` header) serialises concurrent duplicate deliveries: the
first INSERT wins; the second raises ``IntegrityError``, which the webhook
handler catches to skip re-processing. This makes every webhook handler
idempotent under GitHub's at-least-once delivery + retry semantics.

INSERT order matters (mirrors the Stripe receiver): record the delivery first,
return early on conflict, only then enqueue the reconcile job. Reversing the
steps creates a crash window where a GitHub retry could double-process.

Retention:
    Grows at one row per delivery. Bounded by the daily retention purge
    (``worker.purge_webhook_events`` → ``services.maintenance``), which deletes
    rows older than ``WEBHOOK_EVENT_RETENTION_DAYS`` (30d) — well beyond GitHub's
    redelivery window, so dedup protection is never lost for an in-flight retry
    (spec §12).

Phase 21 — T-265 / T-266 (spec §10).
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID as PythonUUID

from sqlalchemy import Integer, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column

from models import Base


class GitHubWebhookEvent(Base):
    """Idempotency record for a processed GitHub webhook delivery.

    One row per ``delivery_id``. The UNIQUE constraint on ``delivery_id`` is the
    write-serialising lock that prevents duplicate event processing.
    """

    __tablename__ = "github_webhook_events"

    id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    # The X-GitHub-Delivery header. UNIQUE enforced at DB level (dedup lock).
    delivery_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    # e.g. issues | pull_request | installation — stored for audit queries.
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    # Wall-clock time this row was inserted (server clock).
    received_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    # Set when the worker finishes reconciliation for this delivery. A row that
    # still has NULL here well after ``received_at`` was recorded but never
    # applied — see ``github_reconcile.replay_unprocessed_deliveries``.
    processed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    # The verified delivery body, kept so an unprocessed row can be REPLAYED.
    # The dedup row is committed on receipt, so without this a delivery that was
    # recorded but never processed is answered "duplicate" on GitHub's
    # redelivery and lost silently. Nullable: rows written before this column
    # existed (and any row whose payload could not be stored) simply cannot be
    # replayed, and the sweep skips them rather than failing.
    # ``none_as_null`` so an absent payload is SQL NULL rather than the JSON
    # value ``null`` — the replay sweep filters on ``payload IS NOT NULL``, and
    # JSON ``null`` would sail straight through it.
    payload: Mapped[dict | None] = mapped_column(
        JSONB(none_as_null=True), nullable=True
    )
    # How many times the inbox sweep has re-dispatched this delivery. Bounds the
    # replay: a delivery that can never succeed (a handler bug, an installation
    # GitHub 404s) would otherwise be re-enqueued every tick forever, and because
    # the sweep is batched and oldest-first, a pile of such rows would fill the
    # batch and starve the newer deliveries the sweep exists to rescue.
    replay_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
