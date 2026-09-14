from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from middleware.auth import get_current_user
from models import CreditLedger, User
from models.billing_credit_debt import BillingCreditDebt
from schemas.credits import CreditBalance, CreditLedgerEntry
from services.credit_service import CREDIT_COSTS, credit_service

router = APIRouter(prefix="/credits", tags=["credits"])


@router.get("/balance", response_model=CreditBalance)
async def get_balance(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CreditBalance:
    balance = await credit_service.get_balance(db, user.id)
    await db.commit()
    await credit_service.invalidate(user.id)
    # Unrecovered payment-reversal debt (T-305): sum the still-owed amount across the
    # user's pending reversal debts. coalesce → 0 when there are no pending rows so
    # the response is always a non-negative int, never NULL.
    debt_credits = await db.scalar(
        select(
            func.coalesce(
                func.sum(
                    BillingCreditDebt.credits_owed - BillingCreditDebt.credits_recovered
                ),
                0,
            )
        ).where(
            BillingCreditDebt.user_id == user.id,
            BillingCreditDebt.status == "pending",
        )
    )
    return CreditBalance(
        balance=balance,
        generation_cost=CREDIT_COSTS["generate"],
        billing_debt_credits=int(debt_credits or 0),
    )


@router.get("/history", response_model=list[CreditLedgerEntry])
async def get_history(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[CreditLedgerEntry]:
    result = await db.execute(
        select(CreditLedger)
        .where(CreditLedger.user_id == user.id)
        .order_by(desc(CreditLedger.created_at))
        .limit(limit)
        .offset(offset)
    )
    return [CreditLedgerEntry.model_validate(entry) for entry in result.scalars()]
