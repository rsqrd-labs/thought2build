from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import exists, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database import get_shared_redis
from models import Stage, StageGenerationRun
from services.credit_service import credit_service
from services.pipeline.generation_runs import terminalize_interrupted_run
from services.pipeline.stage_manager import (
    _POLL_INTERVAL_SECONDS,
    _RECOVERY_LOCK_KEY,
    _RECOVERY_LOCK_TTL,
    run_recovery_cycle,
)

logger = logging.getLogger(__name__)

# Durable generation jobs may wait briefly for a worker slot or be re-delivered
# after a rolling deploy. A 30-second heartbeat reap was correct for API-local
# tasks but can now race a healthy queued arq job and refund it before execution.
# The absolute run deadline remains the hard settlement bound; stale-heartbeat
# recovery is therefore no shorter than that deadline.
_RUN_HEARTBEAT_GRACE_SECONDS = max(120, settings.stage_generation_deadline_seconds)
# _POLL_INTERVAL_SECONDS, _RECOVERY_LOCK_KEY, and _RECOVERY_LOCK_TTL are
# canonical in stage_manager.py and imported above.  H-3 — T-179.


async def recover_stuck_stages(db: AsyncSession) -> int:
    """Settle expired/dead runs plus legacy stages that have no run row."""
    now = datetime.now(UTC)
    heartbeat_cutoff = now - timedelta(seconds=_RUN_HEARTBEAT_GRACE_SECONDS)

    active_runs = list(
        (
            await db.execute(
                select(StageGenerationRun)
                .where(
                    StageGenerationRun.status == "running",
                    or_(
                        StageGenerationRun.deadline_at <= now,
                        StageGenerationRun.heartbeat_at < heartbeat_cutoff,
                    ),
                )
                .order_by(StageGenerationRun.deadline_at)
            )
        )
        .scalars()
        .all()
    )

    recovered = 0
    for run in active_runs:
        deadline_expired = run.deadline_at <= now
        target_status = "timed_out" if deadline_expired else "failed"
        settled = await terminalize_interrupted_run(
            db,
            run_id=run.id,
            status=target_status,
            error_code=(
                "generation_deadline_exceeded"
                if deadline_expired
                else "generation_worker_lost"
            ),
        )
        if settled.status == target_status:
            recovered += 1
            logger.warning(
                "stage.generation_run_recovered generation_id=%s "
                "stage_id=%s status=%s partial_saved=%s refunded=%d",
                run.id,
                run.stage_id,
                settled.status,
                settled.partial_saved,
                settled.refunded_credits,
            )

    result = await db.execute(
        select(Stage).where(
            Stage.status == "in_progress",
            ~exists().where(
                StageGenerationRun.stage_id == Stage.id,
                StageGenerationRun.status == "running",
            ),
        )
    )
    legacy_stages = list(result.scalars())

    for stage in legacy_stages:
        credits_refunded = 0
        if stage.deduction_ledger_id is not None:
            # Log the amount the refund ACTUALLY reversed (abs of the real ledger
            # row), not a hardcoded generate cost — a stage whose deduction was a
            # different size (e.g. a future cost change) would otherwise log a
            # misleading figure into dashboards/alerts (audit finding #8).
            credits_refunded = await credit_service.refund(
                db, stage.deduction_ledger_id
            )

        stage.status = "draft"
        stage.generation_started_at = None
        stage.generation_action = None
        stage.updated_at = datetime.now(UTC)

        logger.warning(
            "stage.recovery stage_id=%s stage_type=%s credits_refunded=%d",
            stage.id,
            stage.type,
            credits_refunded,
        )
        recovered += 1

    checking_cutoff = now - timedelta(
        seconds=max(1, settings.stage_technology_check_stale_seconds)
    )
    checking_stages = list(
        (await db.execute(select(Stage).where(Stage.quality_gate_status == "checking")))
        .scalars()
        .all()
    )
    for stage in checking_stages:
        payload = dict(stage.quality_gate_payload or {})
        started_at = stage.updated_at
        raw_started_at = payload.get("checking_started_at")
        if isinstance(raw_started_at, str):
            try:
                started_at = datetime.fromisoformat(raw_started_at)
                if started_at.tzinfo is None:
                    started_at = started_at.replace(tzinfo=UTC)
            except ValueError:
                pass
        if started_at >= checking_cutoff:
            continue
        findings = list(payload.get("findings") or [])
        findings.append(
            {
                "kind": "technology_safety_unverified",
                "code": "technology_safety_unverified",
                "severity": "unknown",
                "source": "recovery",
                "detail": (
                    "Technology verification did not finish within its bounded "
                    "window. Review versions before deployment."
                ),
            }
        )
        from services.pipeline.tech_safety import (
            analyze_local_technology_safety,
            is_blocking_finding,
        )

        local = analyze_local_technology_safety(stage.type, stage.content or "", {})
        findings.extend(item.to_payload() for item in local)
        local_blocked = any(is_blocking_finding(item) for item in local)
        stage.quality_gate_status = "blocked" if local_blocked else "advisory"
        stage.quality_gate_kind = (
            "technology_safety" if local_blocked else "critic_findings"
        )
        stage.quality_gate_payload = {
            "stage": stage.type,
            "kind": stage.quality_gate_kind,
            "findings": findings,
        }
        stage.quality_gate_failed_at = now
        stage.updated_at = now
        recovered += 1

    if recovered > 0:
        await db.commit()

    # Storyboard generations stuck in 'generating' past their (longer) threshold
    # are recovered in the same leader-locked cycle.  recover_stuck_storyboards
    # commits its own changes independently, so it is safe to run after the stage
    # commit above whether or not any stage was recovered.  Lazy import keeps the
    # pipeline import graph acyclic.  T-254 (Phase 20).
    from services.pipeline.storyboard_service import (  # noqa: PLC0415
        recover_stuck_storyboards,
    )

    recovered += await recover_stuck_storyboards(db)

    # Increment generations stranded in 'generating' (charge-then-generate after
    # audit finding #6) are refunded + reset in the same cycle. Mirrors the
    # storyboard lane: its own commit, lazy import to keep the graph acyclic.
    from services.pipeline.increment_service import (  # noqa: PLC0415
        recover_stuck_increments,
    )

    recovered += await recover_stuck_increments(db)

    return recovered


async def run_recovery_loop() -> None:
    """Background task: polls every 10 seconds and recovers stuck stages.

    Uses a Redis NX lock so only one gunicorn worker runs recovery per cycle.
    Workers that don't acquire the lock skip the cycle silently.
    """
    from database import AsyncSessionLocal

    # Use the shared connection pool — never create a per-loop client.
    # H-1 — T-177.
    redis = get_shared_redis()
    while True:
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)
        try:
            acquired = await redis.set(
                _RECOVERY_LOCK_KEY, "1", nx=True, ex=_RECOVERY_LOCK_TTL
            )
            if not acquired:
                continue
            # Continuous heartbeat keeps the lock alive for the full cycle.
            # HF-3 — T-200.
            async with AsyncSessionLocal() as db:
                count = await run_recovery_cycle(redis, db)
                if count > 0:
                    logger.info("stage.recovery.complete recovered=%d", count)
        except Exception:
            logger.exception("stage.recovery.error")
