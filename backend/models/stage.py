from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID as PythonUUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models import Base

if TYPE_CHECKING:
    from models.stage_generation import StageGenerationRun
    from models.stage_version import StageVersion
    from models.workspace import Workspace


# Quality-gate kinds whose block can only be cleared by regenerating — an
# override is refused for them. Empty since issue #34: every blocking gate is now
# overridable (the user owns the artifact), and the LLM critic no longer blocks
# at all — it is advisory. Kept as a (now empty) set so the lockstep guard test
# and StageManager.override_quality_gate keep one source of truth; if a future
# kind must be non-overridable again, add it here. Literals (not imported from
# services) so the models layer never depends upward on services.
NON_OVERRIDABLE_GATE_KINDS: frozenset[str] = frozenset()

# Credits charged for a (re)generation. Mirrors require_credits(10) on the
# generate/regenerate routes; surfaced in the recovery contract so the UI can
# tell the user exactly what a retry costs.
GENERATION_CREDIT_COST = 10


def _recovery_message(
    kind: str | None,
    *,
    overridable: bool,
    refunded: bool,
    resumable: bool = False,
    completed_sections: int = 0,
    total_sections: int = 0,
) -> str:
    """Build the kind-specific, billing-honest recovery sentence shown to users.

    Every blocking kind is overridable since issue #34, so each message offers
    the same two clear choices — regenerate for a fresh version, or finalise this
    one as-is — phrased so a non-technical user knows exactly what each does.
    """
    refund_clause = " Your credit for this attempt was refunded." if refunded else ""
    if resumable:
        # The resumable case is deliberately NOT phrased as a failure to redo.
        # The sections that succeeded are saved and already paid for; the only
        # outstanding work is the gap, and collecting it costs nothing further.
        remaining = max(0, total_sections - completed_sections)
        return (
            f"{completed_sections} of {total_sections} sections were generated "
            f"and saved. Finish the remaining "
            f"{remaining} {'section' if remaining == 1 else 'sections'} at no "
            "extra credit cost, or finalise this draft as-is."
        )
    if kind == "incomplete_output":
        return (
            "This draft looks cut off before it finished. Regenerate for a "
            "complete version, or finalise this one as-is if it already covers "
            "what you need." + refund_clause
        )
    if kind == "technology_safety":
        return (
            "This draft suggests a technology that's out of date or unsupported. "
            "Regenerate to get an up-to-date version, or finalise this one as-is "
            "if the choice is intentional." + refund_clause
        )
    if kind == "missing_sections":
        return (
            "This draft is missing a few expected sections. Regenerate to fill "
            "them in, or finalise this one as-is if you don't need them."
            + refund_clause
        )
    if kind == "critic_findings":
        # Advisory now (issue #34): findings are suggestions on a finalisable
        # draft, never a block. Kept for forward-compat / legacy blocked rows.
        return (
            "The quality review left some suggestions on this draft. Regenerate "
            "to address them, or finalise this one as-is." + refund_clause
        )
    # Unknown / forward-compatible kind: stay honest about override availability.
    action = (
        "Regenerate for a fresh version, or finalise this one as-is."
        if overridable
        else "Regenerate to continue."
    )
    return "The quality review flagged this draft. " + action + refund_clause


def derive_quality_gate_recovery(
    kind: str | None,
    *,
    refunded_prior_attempt: bool,
    resumable: bool = False,
    completed_sections: int = 0,
    total_sections: int = 0,
) -> dict:
    """Derive the user-facing recovery contract for a blocked quality gate.

    A pure function of the gate ``kind``, whether the blocking attempt was
    refunded, and whether the attempt banked sections that can be completed
    without re-generating (and re-charging for) the whole artifact. No DB state —
    the result is attached to the ``Stage.quality_gate`` property (so it ships in
    every stage GET and survives refresh) and to the structured finalise 409, so
    the frontend renders one authoritative recovery message from a single source
    of truth.

    ``resumable`` changes both the offered action and its price: ``resume`` costs
    **0** credits because the artifact's credit was charged and deliberately not
    refunded, precisely so the banked sections stay paid for. A regenerate is
    still available to the user, it is simply no longer the *recommended* action.
    """
    overridable = kind not in NON_OVERRIDABLE_GATE_KINDS
    return {
        "action": "resume" if resumable else "regenerate",
        "overridable": overridable,
        "credit_required": 0 if resumable else GENERATION_CREDIT_COST,
        "refunded_prior_attempt": refunded_prior_attempt,
        "message": _recovery_message(
            kind,
            overridable=overridable,
            refunded=refunded_prior_attempt,
            resumable=resumable,
            completed_sections=completed_sections,
            total_sections=total_sections,
        ),
    }


class Stage(Base):
    __tablename__ = "stages"
    __table_args__ = (
        CheckConstraint(
            "type IN ('spec', 'plan', 'harness', 'tasks')",
            name="ck_stages_type",
        ),
        CheckConstraint(
            "status IN ('locked', 'draft', 'in_progress', 'finalised', 'stale')",
            name="ck_stages_status",
        ),
        CheckConstraint(
            "quality_gate_status IN "
            "('clear', 'checking', 'blocked', 'overridden', 'advisory')",
            name="ck_stages_quality_gate_status",
        ),
    )

    id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    workspace_id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("workspaces.id", ondelete="CASCADE"),
        nullable=False,
    )
    type: Mapped[str] = mapped_column(String, nullable=False)
    source_identity: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    content: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String, nullable=False)
    current_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    finalised_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    review_gate_acknowledged: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
    )
    gap_patch_used: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    quality_gate_status: Mapped[str] = mapped_column(
        String,
        nullable=False,
        default="clear",
        server_default=text("'clear'"),
    )
    quality_gate_kind: Mapped[str | None] = mapped_column(String, nullable=True)
    quality_gate_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    quality_gate_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quality_gate_failed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    # Write-once generation start instant, stamped at the in_progress transition
    # in StageManager.generate() and NEVER bumped by _stage_db_heartbeat (unlike
    # updated_at). This is the honest elapsed baseline the streaming overlay pins
    # after a page refresh, so the timer no longer sawtooths back to ~0 on every
    # reconnect poll (RC-1). Nullable + additive: old rows / cache-hit drafts are
    # NULL and the frontend falls back to updated_at.
    generation_started_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    # The action that started the in-flight generation ("generate"/"regenerate"),
    # so a reconnect overlay after refresh shows the correct operation label
    # instead of always "generate" (A6). NULL when not generating.
    generation_action: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    deduction_ledger_id: Mapped[PythonUUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("credit_ledger.id", ondelete="SET NULL"),
        nullable=True,
    )

    workspace: Mapped["Workspace"] = relationship(back_populates="stages")
    versions: Mapped[list["StageVersion"]] = relationship(
        back_populates="stage",
        cascade="all, delete-orphan",
    )
    generation_runs: Mapped[list["StageGenerationRun"]] = relationship(
        back_populates="stage",
        cascade="all, delete-orphan",
    )

    @property
    def quality_gate(self) -> dict | None:
        status = self.quality_gate_status or "clear"
        if status == "clear":
            return None

        payload = dict(self.quality_gate_payload or {})
        payload.setdefault("stage", self.type)
        payload.setdefault("kind", self.quality_gate_kind)
        payload["status"] = status
        payload["version"] = self.quality_gate_version
        payload["failed_at"] = (
            self.quality_gate_failed_at.isoformat()
            if self.quality_gate_failed_at is not None
            else None
        )
        # A blocked version carries a derived recovery contract so the frontend
        # has one authoritative source for "is this overridable / what does a
        # retry cost / was I refunded / what do I tell the user." An overridden
        # version is not awaiting recovery, so it carries none.
        if status == "blocked":
            completed = list(payload.get("completed_sections") or [])
            missing = list(payload.get("missing_sections") or [])
            payload["recovery"] = derive_quality_gate_recovery(
                self.quality_gate_kind,
                refunded_prior_attempt=bool(
                    payload.get("refunded_prior_attempt", False)
                ),
                resumable=bool(payload.get("resumable", False)),
                completed_sections=len(completed),
                total_sections=len(completed) + len(missing),
            )
        return payload
