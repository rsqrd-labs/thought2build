"""Provenance for consumed credits and credits used to settle another pack's debt."""

from uuid import UUID as PythonUUID

from sqlalchemy import CheckConstraint, ForeignKey, Integer, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from models import Base


class CreditPackAllocation(Base):
    __tablename__ = "credit_pack_allocations"
    __table_args__ = (
        UniqueConstraint("ledger_entry_id", "pack_id", name="uq_cpa_ledger_pack"),
        CheckConstraint("amount > 0", name="ck_cpa_amount_positive"),
        CheckConstraint(
            "returned_amount >= 0 AND returned_amount <= amount",
            name="ck_cpa_returned_bounds",
        ),
    )

    id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    ledger_entry_id: Mapped[PythonUUID] = mapped_column(
        ForeignKey("credit_ledger.id", ondelete="CASCADE"), index=True
    )
    pack_id: Mapped[PythonUUID] = mapped_column(
        ForeignKey("billing_credit_packs.id", ondelete="CASCADE"), index=True
    )
    # NULL means consumption by an operation. Otherwise this pack supplied
    # credits to cover the named pack's payment reversal.
    recovery_for_pack_id: Mapped[PythonUUID | None] = mapped_column(
        ForeignKey("billing_credit_packs.id", ondelete="CASCADE"), index=True
    )
    amount: Mapped[int] = mapped_column(Integer)
    returned_amount: Mapped[int] = mapped_column(
        Integer, server_default=text("0"), default=0
    )
