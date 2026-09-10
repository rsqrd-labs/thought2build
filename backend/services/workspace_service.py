from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from config import settings
from models import Stage, User, Workspace
from schemas.workspace import TargetAgent, WorkspaceCreate
from services.llm.routing import (
    LLMRoutingError,
    platform_provider_priority,
    resolve_llm_route,
)
from services.security.problem_statement_gate import (
    ProblemStatementValidationError,
    assert_valid_problem_statement_async,
)
from services.security.sanitizer import sanitize_text, sanitize_text_async

_STAGE_ORDER = ["spec", "plan", "harness", "tasks"]


class WorkspaceService:
    async def create(
        self, user_id: UUID, payload: WorkspaceCreate, db: AsyncSession
    ) -> Workspace:
        await self._assert_problem_statement_is_valid(payload.problem_statement)
        initial_route = self._server_default_route()
        mode, target_agent, time_budget_minutes, restricted_environment = (
            self._resolve_mode(payload)
        )

        # Lock the user row so concurrent create requests are serialized and
        # the quota check + insert are atomic within this transaction.
        await db.execute(select(User).where(User.id == user_id).with_for_update())
        active_count = await self._active_workspace_count(user_id, db)
        if active_count >= settings.max_active_workspaces_per_user:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail={
                    "code": "workspace_quota_exceeded",
                    "limit": settings.max_active_workspaces_per_user,
                },
            )

        workspace = Workspace(
            user_id=user_id,
            name=sanitize_text(payload.name),
            # Problem statements run to 50K chars; the bleach pass is offloaded
            # off the event loop (F7 — scalability audit P2). The name (<=200
            # chars) stays inline — the offload gate would keep it there anyway.
            problem_statement=await sanitize_text_async(payload.problem_statement),
            # Expand-phase compatibility for workers still reading these
            # columns. New routing code never treats them as user preference.
            provider=initial_route.provider,
            model=initial_route.model,
            status="active",
            mode=mode,
            target_agent=target_agent,
            time_budget_minutes=time_budget_minutes,
            restricted_environment=restricted_environment,
        )
        db.add(workspace)
        await db.flush()

        for stage_type in _STAGE_ORDER:
            db.add(
                Stage(
                    workspace_id=workspace.id,
                    type=stage_type,
                    status="draft" if stage_type == "spec" else "locked",
                    content=None,
                    current_version=0,
                    review_gate_acknowledged=False,
                )
            )

        await db.commit()
        result = await db.execute(
            select(Workspace)
            .where(Workspace.id == workspace.id)
            .options(selectinload(Workspace.stages))
        )
        return result.scalar_one()

    async def list_for_user(self, user_id: UUID, db: AsyncSession) -> list[Workspace]:
        result = await db.execute(
            select(Workspace)
            .where(Workspace.user_id == user_id, Workspace.status == "active")
            .options(selectinload(Workspace.stages))
            .order_by(Workspace.created_at.desc())
        )
        return list(result.scalars())

    async def get(
        self, workspace_id: UUID, user_id: UUID, db: AsyncSession, *, lock: bool = False
    ) -> Workspace:
        stmt = (
            select(Workspace)
            .where(Workspace.id == workspace_id, Workspace.user_id == user_id)
            .options(selectinload(Workspace.stages))
        )
        if lock:
            stmt = stmt.with_for_update().execution_options(populate_existing=True)
        result = await db.execute(stmt)
        workspace = result.scalar_one_or_none()
        if workspace is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Workspace not found"
            )
        return workspace

    async def update(
        self,
        workspace_id: UUID,
        user_id: UUID,
        name: str | None,
        db: AsyncSession,
        problem_statement: str | None = None,
        target_agent: TargetAgent | None = None,
    ) -> Workspace:
        workspace = await self.get(workspace_id, user_id, db, lock=True)
        if name is not None:
            workspace.name = sanitize_text(name)
        if problem_statement is not None:
            await self._assert_problem_statement_is_valid(problem_statement)
            workspace.problem_statement = await sanitize_text_async(problem_statement)
            self._mark_problem_statement_dependents_stale(workspace)
        if target_agent is not None:
            workspace.target_agent = target_agent
        await db.commit()
        await db.refresh(workspace)
        return workspace

    def _mark_problem_statement_dependents_stale(self, workspace: Workspace) -> None:
        for stage in workspace.stages:
            if stage.type == "spec" and stage.content and stage.status == "finalised":
                stage.status = "stale"
            elif stage.type != "spec" and stage.status == "finalised":
                stage.status = "stale"

    async def archive(
        self,
        workspace_id: UUID,
        user_id: UUID,
        db: AsyncSession,
        ack_version: str | None = None,
    ) -> None:
        """Move a workspace to the trash (issue #43).

        Sets ``status='archived'`` and stamps the purge clock ``archived_at=now()``
        plus the ``retention_ack_version`` the delete dialog displayed (when the
        client supplied it — legacy/stale-SPA deletes leave it NULL and fall into
        the conservative legacy window). Re-archiving a restored workspace restarts
        the clock (matches "re-archiving restarts the ladder").
        """
        workspace = await self.get(workspace_id, user_id, db)
        workspace.status = "archived"
        workspace.archived_at = datetime.now(UTC)
        workspace.retention_ack_version = ack_version
        await db.commit()

    async def list_trashed(self, user_id: UUID, db: AsyncSession) -> list[Workspace]:
        """Return the user's trashed (archived) workspaces, newest-deleted first."""
        result = await db.execute(
            select(Workspace)
            .where(Workspace.user_id == user_id, Workspace.status == "archived")
            .order_by(Workspace.archived_at.desc().nullslast())
        )
        return list(result.scalars())

    async def restore(
        self, workspace_id: UUID, user_id: UUID, db: AsyncSession
    ) -> Workspace:
        """Restore a trashed workspace: archived → active, clear the purge clock.

        Allowed even when the user is at ``max_active_workspaces_per_user`` — this
        is the user's own data returning, not a new workspace, so restore is never
        gated on quota (a new create still is). Idempotent for an already-active
        workspace. 404 when not owned by the caller (via ``get``).
        """
        workspace = await self.get(workspace_id, user_id, db)
        if workspace.status == "archived":
            workspace.status = "active"
            workspace.archived_at = None
            workspace.retention_ack_version = None
            await db.commit()
        result = await db.execute(
            select(Workspace)
            .where(Workspace.id == workspace.id)
            .options(selectinload(Workspace.stages))
        )
        return result.scalar_one()

    async def _active_workspace_count(self, user_id: UUID, db: AsyncSession) -> int:
        result = await db.execute(
            select(func.count())
            .select_from(Workspace)
            .where(Workspace.user_id == user_id, Workspace.status == "active")
        )
        return int(result.scalar() or 0)

    async def _assert_problem_statement_is_valid(self, problem_statement: str) -> None:
        try:
            await assert_valid_problem_statement_async(problem_statement)
        except ProblemStatementValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code": exc.result.code,
                    "message": exc.result.message,
                    "hints": exc.result.hints,
                },
            ) from exc

    def _resolve_mode(
        self, payload: WorkspaceCreate
    ) -> tuple[str, str | None, int | None, bool]:
        """Resolve the persisted (mode, target_agent, time_budget_minutes,
        restricted_environment).

        The ``demo_day_mode_enabled`` config flag is the master server-side gate
        (plan §4/§0): when it is off, ``mode`` is forced to ``"standard"`` and the
        Demo-Day-only time budget and environment constraint are dropped, so a
        client cannot opt a workspace into Demo Day before the feature is
        enabled. Agent instructions apply to both modes and therefore survive
        the standard-mode fallback.
        """
        if not settings.demo_day_mode_enabled or payload.mode != "demo_day":
            return "standard", payload.target_agent, None, False
        return (
            "demo_day",
            payload.target_agent,
            payload.time_budget_minutes,
            payload.restricted_environment,
        )

    def _server_default_route(self):
        try:
            return resolve_llm_route(
                operation="spec.generate",
                preferred_provider=platform_provider_priority()[0],
                requested_tier="mid",
                fallback_tier=None,
                latency_class="interactive",
            )
        except LLMRoutingError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code": "generation_unavailable",
                    "message": "AI generation is temporarily unavailable.",
                },
            ) from exc


workspace_service = WorkspaceService()
