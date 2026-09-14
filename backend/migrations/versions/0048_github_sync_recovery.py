"""Make GitHub sync recoverable: backfill watermark, re-adoptable detached
pushes, a webhook-inbox payload, and the missing first-export uniqueness guard.

Revision ID: 0048
Revises: 0047

Four additive changes, all nullable/backwards-compatible except the dedupe the
new partial index requires (see below).

1. ``integration_pushes.last_full_backfill_at`` — the push-level watermark
   ``backfill_repo`` uses as its ``since`` cursor. It previously derived the
   cursor from ``max(task.synced_at)``, which meant an issue whose update
   predated the most recently reconciled task was excluded from every future
   sweep — the exact class of missed event backfill exists to recover.

2. ``integration_pushes.detached_installation_id`` — GitHub's numeric
   installation id, recorded when an uninstall detaches a push. Inbound
   reconcile INNER JOINs ``github_installations`` on ``installation_id``, so a
   NULLed column made the push permanently unreachable by webhook, backfill and
   resync — even after reinstalling on the same repo. This lets
   ``upsert_installation`` re-adopt its own former pushes.

3. ``uq_integration_push_workspace_provider_active`` — migration 0016 dropped
   the ``(workspace_id, provider)`` unique constraint in favour of a partial
   index on ``(workspace_id, repo_id)``. A first-time export inserts with
   ``repo_id IS NULL``, and Postgres treats NULLs as distinct in a unique index,
   so two concurrent submits could both insert. Every later lookup then raised
   ``MultipleResultsFound`` and the workspace could never be exported again.
   Pre-existing duplicates are collapsed first (newest live row wins; the rest
   are marked ``failed``, which is the terminal state those rows would have
   reached anyway and which the partial predicate excludes).

4. ``github_webhook_events.payload`` / ``replay_count`` — the verified delivery
   body plus its replay attempt count. The ingress commits a dedup row on
   *receipt*, so a delivery that is recorded but never processed is answered
   "duplicate" on GitHub's redelivery and lost with no record. Storing the
   payload lets the reconcile sweep replay it, mirroring the billing inbox.
   ``replay_count`` bounds that: a delivery that cannot succeed (a handler bug, a
   404'd installation) would otherwise be re-enqueued every tick forever and —
   because the sweep is batched and ordered oldest-first — would eventually fill
   the batch and starve the newer, recoverable deliveries the mechanism exists
   to rescue. The payload is also cleared once a delivery is processed, so the
   column holds only the handful of rows that are actually stuck rather than
   every delivery for the 30-day retention window.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0048"
down_revision = "0047"
branch_labels = None
depends_on = None


_WORKSPACE_PROVIDER_INDEX = "uq_integration_push_workspace_provider_active"

# Collapse any pre-existing (workspace_id, provider) duplicates among rows the
# new partial index will cover, keeping the newest and failing the rest, so
# CREATE UNIQUE INDEX cannot abort the deploy.
_DEDUPE_SQL = """
UPDATE integration_pushes AS p
SET status = 'failed'
WHERE p.repo_id IS NULL
  AND p.status <> 'failed'
  AND EXISTS (
      SELECT 1
      FROM integration_pushes AS newer
      WHERE newer.workspace_id = p.workspace_id
        AND newer.provider = p.provider
        AND newer.repo_id IS NULL
        AND newer.status <> 'failed'
        AND (newer.created_at, newer.id) > (p.created_at, p.id)
  )
"""


def upgrade() -> None:
    op.add_column(
        "integration_pushes",
        sa.Column("last_full_backfill_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.add_column(
        "integration_pushes",
        sa.Column("detached_installation_id", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "github_webhook_events",
        sa.Column("payload", JSONB(), nullable=True),
    )
    op.add_column(
        "github_webhook_events",
        sa.Column(
            "replay_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )

    op.execute(_DEDUPE_SQL)
    op.create_index(
        _WORKSPACE_PROVIDER_INDEX,
        "integration_pushes",
        ["workspace_id", "provider"],
        unique=True,
        postgresql_where=sa.text("repo_id IS NULL AND status <> 'failed'"),
    )


def downgrade() -> None:
    op.drop_index(_WORKSPACE_PROVIDER_INDEX, table_name="integration_pushes")
    op.drop_column("github_webhook_events", "replay_count")
    op.drop_column("github_webhook_events", "payload")
    op.drop_column("integration_pushes", "detached_installation_id")
    op.drop_column("integration_pushes", "last_full_backfill_at")
