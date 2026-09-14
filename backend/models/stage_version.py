from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID as PythonUUID

from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    Integer,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models import Base

if TYPE_CHECKING:
    from models.eval_result import EvalResult
    from models.stage import Stage


class StageVersion(Base):
    __tablename__ = "stage_versions"
    __table_args__ = (
        CheckConstraint(
            "created_by IN ('user', 'ai')",
            name="ck_stage_versions_created_by",
        ),
        UniqueConstraint(
            "stage_id",
            "version",
            name="uq_stage_versions_stage_id_version",
        ),
    )

    id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    stage_id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("stages.id", ondelete="CASCADE"),
        nullable=False,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    source_identity: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    # Brave web-research provenance (issue #12, Phase 4). Both NULL unless this
    # version was generated with grounding actually injected — so a populated
    # research_context is the authoritative "this generation used web research"
    # signal (more accurate than the workspace opt-in flag, which only says it
    # *may*). research_context is the exact, sanitised block fed to the model
    # (reproducible/diffable); research_sources is its provenance list of
    # ``{"url": ..., "title": ...}`` (http/https only, sanitised upstream).
    research_context: Mapped[str | None] = mapped_column(Text, nullable=True)
    research_sources: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    stage: Mapped["Stage"] = relationship(back_populates="versions")
    eval_results: Mapped[list["EvalResult"]] = relationship(
        back_populates="stage_version",
        cascade="all, delete-orphan",
    )
