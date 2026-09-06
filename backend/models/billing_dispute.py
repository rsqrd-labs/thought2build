"""Durable Razorpay dispute state, including events received before a grant."""

from datetime import datetime

from sqlalchemy import BigInteger, CheckConstraint, Integer, Text, func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from models import Base


class BillingDispute(Base):
    __tablename__ = "billing_disputes"
    __table_args__ = (CheckConstraint("amount_cents >= 0", name="ck_bd_amount_nonneg"),)

    provider_dispute_id: Mapped[str] = mapped_column(Text, primary_key=True)
    provider_payment_id: Mapped[str] = mapped_column(Text, index=True)
    amount_cents: Mapped[int] = mapped_column(Integer)
    currency: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)
    event_created_at: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now()
    )
