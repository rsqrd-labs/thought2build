from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID as PythonUUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models import Base

if TYPE_CHECKING:
    from models.credit_ledger import CreditLedger
    from models.stage import Stage
    from models.user import User
    from models.workspace import Workspace


RUNNING_GENERATION_STATUSES = frozenset({"running"})
TERMINAL_GENERATION_STATUSES = frozenset(
    {"succeeded", "blocked", "cancelled", "timed_out", "failed"}
)


class StageGenerationRun(Base):
    """Durable source of truth for one stage-generation attempt.

    A stage's ``in_progress`` flag remains a convenient presentation field, but
    concurrency, cancellation, recovery, billing, and terminal outcomes are all
    owned by this row.  The partial unique index below is the final protection
    against two API workers starting work for the same stage.
    """

    __tablename__ = "stage_generation_runs"
    __table_args__ = (
        CheckConstraint(
            "action IN ('generate', 'regenerate', 'resume')",
            name="ck_stage_generation_runs_action",
        ),
        CheckConstraint(
            "status IN ('running', 'succeeded', 'blocked', 'cancelled', "
            "'timed_out', 'failed')",
            name="ck_stage_generation_runs_status",
        ),
        CheckConstraint(
            "phase IN ('preparing', 'drafting', 'assembling', 'validating', "
            "'saving', 'stopping', 'complete')",
            name="ck_stage_generation_runs_phase",
        ),
        CheckConstraint(
            "previous_status IN ('draft', 'stale')",
            name="ck_stage_generation_runs_previous_status",
        ),
        CheckConstraint(
            "completed_parts >= 0 AND total_parts >= 0 "
            "AND completed_parts <= total_parts",
            name="ck_stage_generation_runs_progress",
        ),
        CheckConstraint(
            "refunded_credits >= 0",
            name="ck_stage_generation_runs_refunded_credits",
        ),
        Index(
            "uq_stage_generation_runs_active_stage",
            "stage_id",
            unique=True,
            postgresql_where=text("status = 'running'"),
        ),
        Index(
            "ix_stage_generation_runs_active_deadline",
            "deadline_at",
            postgresql_where=text("status = 'running'"),
        ),
        Index(
            "ix_stage_generation_runs_active_heartbeat",
            "heartbeat_at",
            postgresql_where=text("status = 'running'"),
        ),
        Index(
            "ix_stage_generation_runs_stage_started_at",
            "stage_id",
            "started_at",
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
    workspace_id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("workspaces.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    deduction_ledger_id: Mapped[PythonUUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("credit_ledger.id", ondelete="SET NULL"),
        nullable=True,
    )
    input_snapshot: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    input_identity: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    prepared_prompt: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    action: Mapped[str] = mapped_column(String, nullable=False)
    # The ordered chunk keys this run set out to produce. Stored rather than
    # recomputed so a later resume can tell "which sections are missing" without
    # re-deriving the chunk plan from the workspace mode — and so that a resume
    # whose plan no longer matches (chunking changed under it) is detectable and
    # falls back to a full regenerate instead of stitching incompatible halves.
    chunk_plan: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    # The terminal run whose completed checkpoints seeded this one. Set only on
    # ``action='resume'``; the audit trail for a partially-charged artifact.
    resume_source_run_id: Mapped[PythonUUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("stage_generation_runs.id", ondelete="SET NULL"),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        String, nullable=False, server_default=text("'running'")
    )
    phase: Mapped[str] = mapped_column(
        String, nullable=False, server_default=text("'preparing'")
    )
    previous_status: Mapped[str] = mapped_column(String, nullable=False)
    previous_version: Mapped[int] = mapped_column(Integer, nullable=False)
    completed_parts: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    total_parts: Mapped[int] = mapped_column(Integer, nullable=False)
    result_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String, nullable=True)
    partial_saved: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    refunded_credits: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    started_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    deadline_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    heartbeat_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    cancel_requested_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    stage: Mapped["Stage"] = relationship(back_populates="generation_runs")
    workspace: Mapped["Workspace"] = relationship()
    user: Mapped["User"] = relationship()
    deduction_ledger: Mapped["CreditLedger | None"] = relationship()
    chunks: Mapped[list["StageGenerationChunk"]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
        order_by="StageGenerationChunk.ordinal",
    )

    @property
    def credit_was_deducted(self) -> bool:
        return self.deduction_ledger_id is not None


class StageGenerationChunk(Base):
    """A completed, canonical chunk checkpoint for crash-safe recovery."""

    __tablename__ = "stage_generation_chunks"
    __table_args__ = (
        UniqueConstraint(
            "generation_run_id",
            "chunk_key",
            name="uq_stage_generation_chunks_run_key",
        ),
        UniqueConstraint(
            "generation_run_id",
            "ordinal",
            name="uq_stage_generation_chunks_run_ordinal",
        ),
        CheckConstraint(
            "ordinal >= 0",
            name="ck_stage_generation_chunks_ordinal",
        ),
    )

    id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    generation_run_id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("stage_generation_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    chunk_key: Mapped[str] = mapped_column(String, nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    provider: Mapped[str | None] = mapped_column(String, nullable=True)
    model: Mapped[str | None] = mapped_column(String, nullable=True)
    retry_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    run: Mapped[StageGenerationRun] = relationship(back_populates="chunks")
