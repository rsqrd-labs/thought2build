from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from models import Stage, StageGenerationChunk, StageGenerationRun, StageVersion
from models.stage_generation import TERMINAL_GENERATION_STATUSES
from services.credit_service import credit_service
from services.pipeline.chunk_labels import chunk_labels
from services.pipeline.input_manifest import restore_workspace, source_identity
from services.security.output_validator import validate_async

logger = logging.getLogger(__name__)

_CANCEL_KEY_PREFIX = "stage-generation:cancel:"
_MAX_STAGE_CONTENT_CHARS = 500_000
_LOCAL_CONTROLS: dict[UUID, "GenerationControl"] = {}


class GenerationStoppedError(RuntimeError):
    """Base class for an intentional run stop, carrying safe received bytes."""

    code = "generation_stopped"

    def __init__(self, partial_content: str = "") -> None:
        self.partial_content = partial_content
        super().__init__(self.code)


class GenerationCancelledError(GenerationStoppedError):
    code = "generation_cancelled"


class GenerationDeadlineExceeded(GenerationStoppedError):
    code = "generation_deadline_exceeded"


def cancel_key(run_id: UUID) -> str:
    return f"{_CANCEL_KEY_PREFIX}{run_id}"


@dataclass
class GenerationControl:
    """In-process deadline/cancellation handle backed by durable state.

    The event gives same-process cancellation sub-millisecond delivery.  A
    monitor polls the Redis cancellation key for cross-worker requests and the
    database as the Redis-outage fallback.  The monotonic deadline is local and
    cannot be extended by wall-clock changes or liveness heartbeats.
    """

    run_id: UUID
    stage_id: UUID
    redis: Redis
    deadline_at: datetime
    duration_seconds: float
    event: asyncio.Event = field(default_factory=asyncio.Event)
    stop_code: str | None = None
    _monitor_task: asyncio.Task | None = None
    _monotonic_deadline: float = 0.0

    def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._monotonic_deadline = loop.time() + max(0.0, self.duration_seconds)
        _LOCAL_CONTROLS[self.run_id] = self
        self._monitor_task = asyncio.create_task(
            self._monitor(), name=f"generation-control:{self.run_id}"
        )

    @property
    def remaining_seconds(self) -> float:
        if self._monotonic_deadline <= 0:
            return max(0.0, self.duration_seconds)
        return max(0.0, self._monotonic_deadline - asyncio.get_running_loop().time())

    @property
    def provider_seconds_remaining(self) -> float:
        return max(
            0.0,
            self.remaining_seconds
            - float(settings.stage_generation_finalise_reserve_seconds),
        )

    def request_cancel(self) -> None:
        if self.stop_code is None:
            self.stop_code = GenerationCancelledError.code
        self.event.set()

    def request_deadline(self) -> None:
        if self.stop_code is None:
            self.stop_code = GenerationDeadlineExceeded.code
        self.event.set()

    def raise_if_stopped(self, partial_content: str = "") -> None:
        if self.remaining_seconds <= 0:
            self.request_deadline()
        if not self.event.is_set():
            return
        if self.stop_code == GenerationCancelledError.code:
            raise GenerationCancelledError(partial_content)
        raise GenerationDeadlineExceeded(partial_content)

    async def close(self) -> None:
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            await asyncio.gather(self._monitor_task, return_exceptions=True)
        _LOCAL_CONTROLS.pop(self.run_id, None)
        with contextlib.suppress(RedisError):
            await self.redis.delete(cancel_key(self.run_id))

    async def _monitor(self) -> None:
        from database import AsyncSessionLocal  # noqa: PLC0415

        redis_poll = max(0.25, float(settings.stage_generation_cancel_poll_seconds))
        db_poll = max(
            redis_poll, float(settings.stage_generation_cancel_db_poll_seconds)
        )
        last_db_poll = 0.0
        loop = asyncio.get_running_loop()
        try:
            while not self.event.is_set():
                remaining = self.remaining_seconds
                if remaining <= 0:
                    self.request_deadline()
                    return
                await asyncio.sleep(min(redis_poll, remaining))
                if self.remaining_seconds <= 0:
                    self.request_deadline()
                    return
                try:
                    if await self.redis.get(cancel_key(self.run_id)) is not None:
                        self.request_cancel()
                        return
                except RedisError:
                    logger.warning(
                        "stage.generation_cancel_redis_unavailable",
                        extra={"generation_id": str(self.run_id)},
                    )
                now = loop.time()
                if now - last_db_poll < db_poll:
                    continue
                last_db_poll = now
                try:
                    async with AsyncSessionLocal() as db:
                        row = (
                            await db.execute(
                                select(
                                    StageGenerationRun.status,
                                    StageGenerationRun.cancel_requested_at,
                                ).where(StageGenerationRun.id == self.run_id)
                            )
                        ).one_or_none()
                    if row is None or row.status != "running":
                        self.request_cancel()
                        return
                    if row.cancel_requested_at is not None:
                        self.request_cancel()
                        return
                except Exception:
                    logger.warning(
                        "stage.generation_cancel_db_poll_failed",
                        extra={"generation_id": str(self.run_id)},
                        exc_info=True,
                    )
        except asyncio.CancelledError:
            return


def signal_local_cancellation(run_id: UUID) -> None:
    control = _LOCAL_CONTROLS.get(run_id)
    if control is not None:
        control.request_cancel()


async def create_generation_run(
    db: AsyncSession,
    *,
    stage: Stage,
    user_id: UUID,
    action: str,
    deduction_ledger_id: UUID | None,
    total_parts: int,
    now: datetime | None = None,
    chunk_plan: list[str] | None = None,
    resume_source_run_id: UUID | None = None,
    input_snapshot: dict | None = None,
    input_identity: dict | None = None,
) -> StageGenerationRun:
    now = now or datetime.now(UTC)
    run = StageGenerationRun(
        id=uuid4(),
        stage_id=stage.id,
        workspace_id=stage.workspace_id,
        user_id=user_id,
        deduction_ledger_id=deduction_ledger_id,
        action=action,
        input_snapshot=input_snapshot,
        input_identity=input_identity,
        chunk_plan=list(chunk_plan) if chunk_plan else None,
        resume_source_run_id=resume_source_run_id,
        status="running",
        phase="preparing",
        previous_status=stage.status,
        previous_version=stage.current_version,
        completed_parts=0,
        total_parts=total_parts,
        started_at=now,
        deadline_at=now + timedelta(seconds=settings.stage_generation_deadline_seconds),
        heartbeat_at=now,
    )
    db.add(run)
    await db.flush()
    return run


async def set_run_phase(
    db: AsyncSession,
    run_id: UUID,
    phase: str,
    *,
    commit: bool = True,
) -> None:
    now = datetime.now(UTC)
    await db.execute(
        update(StageGenerationRun)
        .where(
            StageGenerationRun.id == run_id,
            StageGenerationRun.status == "running",
        )
        .values(phase=phase, heartbeat_at=now, updated_at=now)
    )
    if commit:
        await db.commit()


async def checkpoint_chunk(
    db: AsyncSession,
    *,
    run_id: UUID,
    chunk_key: str,
    ordinal: int,
    content: str,
    provider: str,
    model: str,
    retry_count: int,
) -> int:
    """Persist one completed chunk and return the authoritative completed count.

    ``ON CONFLICT DO NOTHING`` makes a replay idempotent and preserves first-wins
    semantics. A completed chunk can never be overwritten by a late retry.
    """
    if not content.strip():
        raise ValueError("Cannot checkpoint an empty generation chunk")
    if len(content) > _MAX_STAGE_CONTENT_CHARS:
        raise ValueError("Generation chunk exceeds the stage content limit")
    statement = (
        insert(StageGenerationChunk)
        .values(
            generation_run_id=run_id,
            chunk_key=chunk_key,
            ordinal=ordinal,
            content=content,
            provider=provider,
            model=model,
            retry_count=retry_count,
        )
        .on_conflict_do_nothing(index_elements=["generation_run_id", "chunk_key"])
    )
    await db.execute(statement)
    completed = int(
        (
            await db.execute(
                select(func.count(StageGenerationChunk.id)).where(
                    StageGenerationChunk.generation_run_id == run_id
                )
            )
        ).scalar_one()
    )
    now = datetime.now(UTC)
    await db.execute(
        update(StageGenerationRun)
        .where(
            StageGenerationRun.id == run_id,
            StageGenerationRun.status == "running",
        )
        .values(completed_parts=completed, heartbeat_at=now, updated_at=now)
    )
    await db.commit()
    return completed


async def load_checkpoint_content(db: AsyncSession, run_id: UUID) -> str:
    chunks = (
        (
            await db.execute(
                select(StageGenerationChunk)
                .where(StageGenerationChunk.generation_run_id == run_id)
                .order_by(StageGenerationChunk.ordinal)
            )
        )
        .scalars()
        .all()
    )
    return "\n\n".join(chunk.content for chunk in chunks if chunk.content.strip())


async def load_checkpoint_map(db: AsyncSession, run_id: UUID) -> dict[str, str]:
    """Return completed chunks keyed for same-run crash/retry continuation."""
    chunks = (
        (
            await db.execute(
                select(StageGenerationChunk)
                .where(StageGenerationChunk.generation_run_id == run_id)
                .order_by(StageGenerationChunk.ordinal)
            )
        )
        .scalars()
        .all()
    )
    return {
        chunk.chunk_key: chunk.content
        for chunk in chunks
        if chunk.content and chunk.content.strip()
    }


async def load_resume_seed(
    db: AsyncSession, *, stage: Stage
) -> tuple[UUID, dict[str, str], list[str]] | None:
    """Return ``(source_run_id, {chunk_key: content}, planned_keys)`` to resume.

    The stage's own quality gate is the authority on whether a resume is offered,
    not a "most recent failed run" scan: ``quality_gate_version`` already pins the
    payload to a specific stage version, so a stage that has since been
    regenerated, edited, or finalised stops advertising the resume and this
    returns None. That keeps completed chunks from one attempt from being
    stitched onto a draft they were never part of.

    Returns None (caller falls back to a normal, charged regenerate) whenever the
    seed cannot be trusted: no gate, a non-resumable gate, a gate pinned to an
    older version, a vanished source run, or a source run with no surviving
    checkpoints.
    """
    payload = stage.quality_gate_payload or {}
    if not payload.get("resumable"):
        return None
    if stage.quality_gate_version != stage.current_version:
        return None
    raw_run_id = payload.get("resume_source_run_id")
    if not raw_run_id:
        return None
    try:
        source_run_id = UUID(str(raw_run_id))
    except (ValueError, AttributeError, TypeError):
        return None
    source = (
        await db.execute(
            select(StageGenerationRun).where(
                StageGenerationRun.id == source_run_id,
                StageGenerationRun.stage_id == stage.id,
            )
        )
    ).scalar_one_or_none()
    if source is None:
        return None
    chunks = (
        (
            await db.execute(
                select(StageGenerationChunk)
                .where(StageGenerationChunk.generation_run_id == source_run_id)
                .order_by(StageGenerationChunk.ordinal)
            )
        )
        .scalars()
        .all()
    )
    seed = {
        chunk.chunk_key: chunk.content
        for chunk in chunks
        if chunk.content and chunk.content.strip()
    }
    if not seed:
        return None
    return source_run_id, seed, [str(key) for key in (source.chunk_plan or [])]


async def lock_stage_for_run(db: AsyncSession, run: StageGenerationRun) -> Stage:
    """Lock the run's stage after the run lock, preserving lock order globally."""
    return (
        await db.execute(
            select(Stage)
            .where(Stage.id == run.stage_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one()


async def lock_running_run(db: AsyncSession, run_id: UUID) -> StageGenerationRun:
    run = (
        await db.execute(
            select(StageGenerationRun)
            .where(StageGenerationRun.id == run_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    if run.status != "running":
        raise GenerationCancelledError()
    if run.cancel_requested_at is not None:
        raise GenerationCancelledError()
    if datetime.now(UTC) >= run.deadline_at:
        raise GenerationDeadlineExceeded()
    return run


async def mark_run_terminal(
    db: AsyncSession,
    run: StageGenerationRun,
    *,
    status: str,
    result_version: int | None = None,
    error_code: str | None = None,
    partial_saved: bool = False,
    refunded_credits: int = 0,
) -> None:
    if status not in TERMINAL_GENERATION_STATUSES:
        raise ValueError(f"Invalid terminal generation status: {status}")
    now = datetime.now(UTC)
    run.status = status
    run.phase = "complete"
    run.result_version = result_version
    run.error_code = error_code
    run.partial_saved = partial_saved
    run.refunded_credits = refunded_credits
    run.finished_at = now
    run.heartbeat_at = now
    run.updated_at = now


async def terminalize_interrupted_run(
    db: AsyncSession,
    *,
    run_id: UUID,
    status: str,
    error_code: str,
    partial_content: str = "",
    partial_ordinal: int | None = None,
    discard_content: bool = False,
) -> StageGenerationRun:
    """Idempotently refund and settle an interrupted run.

    Completed checkpoints are the crash-safe base. A caller-provided current
    partial is appended only when it is non-empty and the resulting content is
    within the stage limit; callers must security-validate it before passing it.
    """
    if status not in TERMINAL_GENERATION_STATUSES:
        raise ValueError(f"Invalid terminal generation status: {status}")
    run = (
        await db.execute(
            select(StageGenerationRun)
            .where(StageGenerationRun.id == run_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    if run.status != "running":
        return run
    stage = await lock_stage_for_run(db, run)
    checkpoint_rows = []
    if not discard_content:
        checkpoint_rows = list(
            (
                (
                    await db.execute(
                        select(StageGenerationChunk)
                        .where(StageGenerationChunk.generation_run_id == run.id)
                        .order_by(StageGenerationChunk.ordinal)
                    )
                )
                .scalars()
                .all()
            )
        )
    ordered_pieces = [
        (chunk.ordinal, chunk.content.strip())
        for chunk in checkpoint_rows
        if chunk.content.strip()
    ]
    if partial_content.strip() and not discard_content:
        ordinal = partial_ordinal
        if ordinal is None:
            ordinal = max((item[0] for item in ordered_pieces), default=-1) + 1
        if ordinal not in {item[0] for item in ordered_pieces}:
            ordered_pieces.append((ordinal, partial_content.strip()))
    ordered_pieces.sort(key=lambda item: item[0])
    combined = "\n\n".join(piece for _, piece in ordered_pieces).strip()
    # The central terminal transaction is the final safety boundary. Checkpoints
    # are durable but still model-produced, so recovery must not assume they are
    # safe merely because the worker died before whole-document validation.
    partial_was_discarded = discard_content or bool(
        ordered_pieces and len(combined) > _MAX_STAGE_CONTENT_CHARS
    )
    if combined and len(combined) <= _MAX_STAGE_CONTENT_CHARS:
        try:
            async with asyncio.timeout(5):
                validation = await validate_async(combined)
            if not validation.is_safe:
                combined = ""
                partial_was_discarded = True
        except Exception:
            logger.warning(
                "stage.generation_partial_validation_failed",
                extra={"generation_id": str(run.id)},
                exc_info=True,
            )
            combined = ""
            partial_was_discarded = True
    else:
        combined = ""

    # Completed safe checkpoints are durable evidence and remain available for
    # audit/recovery after terminalisation.  Unsafe or oversized model output
    # must not survive in either StageVersion or the checkpoint table.
    if partial_was_discarded and (checkpoint_rows or discard_content):
        await db.execute(
            delete(StageGenerationChunk).where(
                StageGenerationChunk.generation_run_id == run.id
            )
        )

    # Resumability is decided BEFORE the refund, because it decides the refund.
    # A run that banked at least one chunk and is missing at least one other has
    # durable, paid-for work sitting in stage_generation_chunks; throwing that
    # away and handing the credit back means the next attempt re-generates every
    # chunk — including the ones that already succeeded — and re-enters the same
    # deadline that just killed it. Keeping the charge and completing only the
    # gap is both cheaper and far likelier to succeed.
    surviving = [] if partial_was_discarded else checkpoint_rows
    completed_keys = [chunk.chunk_key for chunk in surviving]
    planned_keys = [str(key) for key in (run.chunk_plan or [])]
    missing_keys = [key for key in planned_keys if key not in set(completed_keys)]
    # ``combined`` is the gate on suppressing the refund: it is the content that
    # actually reaches the user's draft. Without it there is nothing to resume
    # onto, and withholding the credit would charge for nothing.
    resumable = (
        bool(completed_keys)
        and bool(missing_keys)
        and bool(combined)
        and bool(run.input_snapshot)
        and bool(run.input_identity)
        and bool(run.prepared_prompt)
    )

    refunded = 0
    if run.deduction_ledger_id is not None and not resumable:
        refunded = await credit_service.refund(
            db, run.deduction_ledger_id, user_id=run.user_id
        )

    result_version: int | None = None
    if combined:
        result_version = stage.current_version + 1
        now = datetime.now(UTC)
        stage.source_identity = (
            source_identity(restore_workspace(run.input_snapshot), stage.type)
            if run.input_snapshot
            else None
        )
        stage.content = combined
        stage.current_version = result_version
        stage.status = "draft"
        stage.quality_gate_status = "blocked"
        stage.quality_gate_kind = "incomplete_output"
        stage.quality_gate_payload = {
            "stage": stage.type,
            "kind": "incomplete_output",
            "reasons": [
                {
                    "code": error_code,
                    "detail": (
                        (
                            f"Generation stopped after {len(completed_keys)} of "
                            f"{len(planned_keys)} sections. The completed sections "
                            "were saved — the remaining ones can be generated "
                            "without spending another credit."
                        )
                        if resumable
                        else (
                            "Generation stopped before every section completed. "
                            "The safely received portion was saved."
                        )
                    ),
                    "reference": None,
                }
            ],
            "repair_attempted": False,
            "refunded_prior_attempt": (
                run.deduction_ledger_id is not None and not resumable
            ),
            # Consumed by the UI to offer "complete the rest" instead of a full,
            # re-charged regenerate. resume_source_run_id is what the resume run
            # seeds its completed_content from.
            "resumable": resumable,
            "resume_source_run_id": str(run.id) if resumable else None,
            # Rendered names, not routing keys: every stage describes its
            # progress in the same voice. The resume itself joins on the chunk
            # keys held in stage_generation_chunks, never on these strings.
            "completed_sections": chunk_labels(stage.type, completed_keys),
            "missing_sections": chunk_labels(stage.type, missing_keys),
        }
        stage.quality_gate_version = result_version
        stage.quality_gate_failed_at = now
        db.add(
            StageVersion(
                stage_id=stage.id,
                version=result_version,
                content=combined,
                source_identity=stage.source_identity,
                created_by="ai",
            )
        )
    else:
        stage.status = run.previous_status
    stage.generation_started_at = None
    stage.generation_action = None
    stage.updated_at = datetime.now(UTC)
    await mark_run_terminal(
        db,
        run,
        status=status,
        result_version=result_version,
        error_code=error_code,
        partial_saved=bool(combined),
        refunded_credits=refunded,
    )
    await db.commit()
    if run.deduction_ledger_id is not None:
        await credit_service.invalidate(run.user_id)
    return run


def run_to_dict(run: StageGenerationRun) -> dict:
    return {
        "id": str(run.id),
        "stage_id": str(run.stage_id),
        "action": run.action,
        "status": run.status,
        "phase": run.phase,
        "completed_parts": run.completed_parts,
        "total_parts": run.total_parts,
        "started_at": run.started_at,
        "deadline_at": run.deadline_at,
        "heartbeat_at": run.heartbeat_at,
        "cancel_requested_at": run.cancel_requested_at,
        "finished_at": run.finished_at,
        "result_version": run.result_version,
        "error_code": run.error_code,
        "partial_saved": run.partial_saved,
        "refunded_credits": run.refunded_credits,
        "credit_was_deducted": run.credit_was_deducted,
        "resume_source_run_id": (
            str(run.resume_source_run_id) if run.resume_source_run_id else None
        ),
    }
