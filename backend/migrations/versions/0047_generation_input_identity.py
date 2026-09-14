"""Pin generation inputs and preserve source lineage.

Revision ID: 0047
Revises: 0046
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0047"
down_revision = "0046"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for name in ("input_snapshot", "input_identity", "prepared_prompt"):
        op.add_column("stage_generation_runs", sa.Column(name, JSONB(), nullable=True))
    op.add_column(
        "stage_versions", sa.Column("source_identity", JSONB(), nullable=True)
    )
    op.add_column("stages", sa.Column("source_identity", JSONB(), nullable=True))


def downgrade() -> None:
    op.drop_column("stage_versions", "source_identity")
    op.drop_column("stages", "source_identity")
    for name in ("prepared_prompt", "input_identity", "input_snapshot"):
        op.drop_column("stage_generation_runs", name)
