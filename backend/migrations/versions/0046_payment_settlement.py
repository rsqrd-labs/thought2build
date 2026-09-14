"""Track credit provenance and durable disputes; preserve unresolved checkouts.

Revision ID: 0046
Revises: 0045
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0046"
down_revision = "0045"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "credit_pack_allocations",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "ledger_entry_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("credit_ledger.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "pack_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("billing_credit_packs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "recovery_for_pack_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("billing_credit_packs.id", ondelete="CASCADE"),
        ),
        sa.Column("amount", sa.Integer(), nullable=False),
        sa.Column("returned_amount", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint("ledger_entry_id", "pack_id", name="uq_cpa_ledger_pack"),
        sa.CheckConstraint("amount > 0", name="ck_cpa_amount_positive"),
        sa.CheckConstraint(
            "returned_amount >= 0 AND returned_amount <= amount",
            name="ck_cpa_returned_bounds",
        ),
    )
    for column in ("ledger_entry_id", "pack_id", "recovery_for_pack_id"):
        op.create_index(
            f"ix_credit_pack_allocations_{column}", "credit_pack_allocations", [column]
        )
    op.create_table(
        "billing_disputes",
        sa.Column("provider_dispute_id", sa.Text(), primary_key=True),
        sa.Column("provider_payment_id", sa.Text(), nullable=False),
        sa.Column("amount_cents", sa.Integer(), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("event_created_at", sa.BigInteger(), nullable=False),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint("amount_cents >= 0", name="ck_bd_amount_nonneg"),
    )
    op.create_index(
        "ix_billing_disputes_provider_payment_id",
        "billing_disputes",
        ["provider_payment_id"],
    )


def downgrade() -> None:
    # Provenance is needed to undo a charged operation safely. Do not silently
    # destroy it during a rollback after the new code has accepted work.
    bind = op.get_bind()
    for table in ("credit_pack_allocations", "billing_disputes"):
        if bind.execute(sa.text(f"SELECT EXISTS (SELECT 1 FROM {table})")).scalar():
            raise RuntimeError("Cannot downgrade 0046 while settlement history exists")
    op.drop_table("billing_disputes")
    op.drop_table("credit_pack_allocations")
