from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Literal
from uuid import UUID as PythonUUID

from sqlalchemy import BigInteger, ForeignKey, Index, Integer, Text, func, text
from sqlalchemy.dialects.postgresql import TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models import Base

if TYPE_CHECKING:
    from models.github_installation import GitHubInstallation
    from models.integration_push_task import IntegrationPushTask
    from models.stage_version import StageVersion
    from models.user import User
    from models.workspace import Workspace


class IntegrationPush(Base):
    """A GitHub push for a workspace (Phase 21 living system of record, spec §10).

    Rekeyed from the Phase-13 ``(workspace_id, provider)`` onto the immutable
    ``repo_id`` (repositories get renamed and transferred, so ``repo_full_name``
    is display-only). Re-export reuses the single live push for a repo rather
    than creating a new one.

    Constraints mirror migration ``0016_github_living_integration.py``
    one-for-one to keep the ORM and schema from drifting: the legacy
    ``uq_integration_push_workspace_provider`` unique constraint is gone, and a
    partial unique index enforces **one live push per repo**. The status enum is
    exactly ``pending`` / ``completed`` / ``failed`` / ``stale`` — there is no
    ``'active'`` literal; a "live" push is the single non-``failed`` row, which
    is what the index predicate keys on. ``repo_id`` is nullable so legacy
    Phase-13 rows (no repo id, backfilled lazily on the next App export) survive
    under the partial index (Postgres treats nulls as distinct).
    """

    __tablename__ = "integration_pushes"
    __table_args__ = (
        Index(
            "uq_integration_push_workspace_repo_active",
            "workspace_id",
            "repo_id",
            unique=True,
            postgresql_where=text("status <> 'failed'"),
        ),
        Index("ix_integration_pushes_repo_id", "repo_id"),
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
        index=True,
    )
    user_id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    # v2 App identity. Nullable: legacy v1 OAuth pushes have no installation.
    installation_id: Mapped[PythonUUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("github_installations.id"),
        nullable=True,
    )
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    # GitHub's immutable numeric repository id — the reconciliation key. Nullable
    # for legacy rows; backfilled on the next App export (T-269).
    repo_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    repo_full_name: Mapped[str | None] = mapped_column(Text)
    repo_url: Mapped[str | None] = mapped_column(Text)
    export_mode: Mapped[Literal["files_to_default", "pr_with_tests"]] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'files_to_default'"),
    )
    # Set in pr_with_tests mode, e.g. thought2build/inc-1.
    branch_name: Mapped[str | None] = mapped_column(Text)
    pr_number: Mapped[int | None] = mapped_column(Integer)
    # The Tasks stage version that produced this push (drift detection).
    source_stage_version_id: Mapped[PythonUUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("stage_versions.id"),
        nullable=True,
    )
    # The increment this push belongs to. FK target (increments.id) wired in
    # migration 0017 (T-278); SET NULL so a push survives an increment delete.
    increment_id: Mapped[PythonUUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("increments.id", ondelete="SET NULL"),
        nullable=True,
    )
    status: Mapped[Literal["pending", "completed", "failed", "stale"]] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
    )
    pushed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
    )
    # Wall-clock completion marker for inbound GitHub reconciliation. Unlike a
    # task's ``synced_at`` (an event-time ordering cursor), this is deliberately
    # updated even when a manual refresh finds no state changes. The frontend
    # uses it to keep "Check GitHub" pending until the worker has actually
    # completed instead of treating the HTTP 202 enqueue acknowledgement as
    # success.
    last_inbound_sync_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
    )
    # Machine-readable result of the last inbound check. Null means success;
    # values are a deliberately small product contract, never raw GitHub text.
    last_inbound_sync_error: Mapped[str | None] = mapped_column(Text)
    # Watermark for backfill's ``since`` cursor: the instant the last SUCCESSFUL
    # full issue sweep started. Deliberately not ``last_inbound_sync_at`` — that
    # is stamped by a single-issue webhook reconcile, and using it would skip
    # every issue whose update predates that one event, which is precisely the
    # class of miss backfill exists to recover.
    last_full_backfill_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
    )
    # GitHub's numeric installation id, retained when an uninstall detaches this
    # push (``installation_id`` is NULLed because the install row is deleted).
    # Inbound reconcile resolves pushes by joining ``github_installations``, so
    # without this the push is unreachable forever; re-installing the App on the
    # same account re-adopts its own former pushes through this column.
    detached_installation_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    workspace: Mapped["Workspace"] = relationship()
    user: Mapped["User"] = relationship()
    installation: Mapped["GitHubInstallation | None"] = relationship()
    source_stage_version: Mapped["StageVersion | None"] = relationship()
    tasks: Mapped[list["IntegrationPushTask"]] = relationship(
        back_populates="push",
        cascade="all, delete-orphan",
    )

    @property
    def issue_count(self) -> int:
        """Transient count populated by export/status service queries."""
        count = getattr(self, "_issue_count", None)
        if count is not None:
            return int(count)
        tasks = getattr(self, "tasks", None)
        return len(tasks or [])

    @issue_count.setter
    def issue_count(self, value: int) -> None:
        self._issue_count = int(value or 0)
