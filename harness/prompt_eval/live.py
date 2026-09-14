"""Generate candidate artifacts through the production prompt/chunk pipeline.

Run this file in a fresh process for each code revision. It uses synthetic
fixtures, no product DB, no credits, and an explicit provider-call ceiling.
Provider calls are billed; --allow-live is mandatory.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
import time
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4


class MemoryCache:
    def __init__(self):
        self.values = {}

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, **kwargs):
        self.values[key] = value


class CallBudget:
    def __init__(self, limit: int):
        from services.llm.gateway import get_llm

        self.get_llm = get_llm
        self.limit = limit
        self.calls: list[dict] = []

    def adapter(self, route):
        delegate = self.get_llm(route.provider, route.model, operation=route.operation)
        budget = self

        class RecordedAdapter:
            def __getattr__(self, name):
                return getattr(delegate, name)

            async def stream(self, system, user, **kwargs):
                if len(budget.calls) >= budget.limit:
                    raise RuntimeError("Live evaluation provider-call budget exhausted")
                record = {
                    "route": {
                        "provider": route.provider,
                        "model": route.model,
                        "operation": route.operation,
                    },
                    "usage": None,
                    "system_sha256": hashlib.sha256(system.encode()).hexdigest(),
                    "user_sha256": hashlib.sha256(user.encode()).hexdigest(),
                }
                budget.calls.append(record)
                started = time.monotonic()
                try:
                    async for token in delegate.stream(system, user, **kwargs):
                        yield token
                finally:
                    info = getattr(delegate, "last_completion", None)
                    record["usage"] = getattr(info, "usage", None)
                    from services.llm.usage import (
                        normalize_provider_usage,
                        estimate_cost_usd,
                    )

                    estimated = estimate_cost_usd(
                        route.provider,
                        route.model,
                        normalize_provider_usage(route.provider, record["usage"]),
                    )
                    record["estimated_cost_usd"] = (
                        float(estimated) if estimated is not None else None
                    )
                    record["finish_reason"] = getattr(info, "finish_reason", None)
                    record["seconds"] = time.monotonic() - started

            async def complete(self, system, user, **kwargs):
                if len(budget.calls) >= budget.limit:
                    raise RuntimeError("Live evaluation provider-call budget exhausted")
                record = {
                    "route": {
                        "provider": route.provider,
                        "model": route.model,
                        "operation": route.operation,
                    },
                    "usage": None,
                    "system_sha256": hashlib.sha256(system.encode()).hexdigest(),
                    "user_sha256": hashlib.sha256(user.encode()).hexdigest(),
                }
                budget.calls.append(record)
                started = time.monotonic()
                try:
                    return await delegate.complete(system, user, **kwargs)
                finally:
                    info = getattr(delegate, "last_completion", None)
                    record["usage"] = getattr(info, "usage", None)
                    from services.llm.usage import (
                        normalize_provider_usage,
                        estimate_cost_usd,
                    )

                    estimated = estimate_cost_usd(
                        route.provider,
                        route.model,
                        normalize_provider_usage(route.provider, record["usage"]),
                    )
                    record["estimated_cost_usd"] = (
                        float(estimated) if estimated is not None else None
                    )
                    record["finish_reason"] = getattr(info, "finish_reason", None)
                    record["seconds"] = time.monotonic() - started

        return RecordedAdapter()


async def generate(args) -> dict:
    sys.path.insert(0, str(args.code_root.resolve()))
    from config import settings
    from prompts.base import stage_prompt_version
    from services.pipeline.generation_runs import GenerationControl
    from services.pipeline.prompt_builder import build_prompt
    from services.pipeline.stage_manager import (
        StageManager,
        _PhaseTracker,
        _build_complexity_signals,
        _route_for_stage_generation,
        _workspace_stage_deps,
    )

    # This runner measures generation. Advisory DB persistence is deliberately
    # outside scope; actual resolved prompts and every provider attempt are saved.
    settings.llm_cost_ledger_enabled = False
    budget = CallBudget(args.max_provider_calls)
    from services.llm import gateway

    # Include compressor/judge preflight calls in the same explicit ceiling.
    gateway.get_llm = lambda provider, model, **kwargs: budget.adapter(
        SimpleNamespace(
            provider=provider, model=model, operation=kwargs.get("operation")
        )
    )
    result = {
        "schema": 1,
        "cases": {},
        "calls": budget.calls,
        "corpus_sha256": None,
        "code_sha256": None,
    }
    code_files = [*args.code_root.rglob("*.py")]
    code_files = [p for p in code_files if ".venv" not in p.parts]
    result["code_sha256"] = hashlib.sha256(
        b"".join(
            str(p.relative_to(args.code_root)).encode() + p.read_bytes()
            for p in sorted(code_files)
        )
    ).hexdigest()
    case_inputs = []
    roots = sorted(p for p in args.corpus.iterdir() if p.is_dir())
    for root in roots:
        problem = (root / "problem_statement.md").read_text()
        qa = json.loads((root / "clarification_qa.json").read_text())
        for mode in args.modes.split(","):
            if mode not in {"standard", "demo_day"}:
                raise ValueError(f"Unsupported mode: {mode}")
            case_inputs.append([root.name, problem, qa, mode])
            workspace = SimpleNamespace(
                id=uuid4(),
                user_id=uuid4(),
                problem_statement=problem,
                clarification_qa=qa,
                mode=mode,
                time_budget_minutes=300,
                restricted_environment=(mode == "demo_day"),
                template_slug=None,
                disable_critic=True,
                brave_research_enabled=False,
                stages=[],
            )
            case = {"input": case_inputs[-1], "stages": {}}
            result["cases"][f"{root.name}/{mode}"] = case
            redis = MemoryCache()
            for stage_type in ("spec", "plan", "harness", "tasks"):
                stage = SimpleNamespace(
                    id=uuid4(), type=stage_type, quality_gate_status="clear"
                )
                route = _route_for_stage_generation(
                    stage_type,
                    workspace,
                    signals=_build_complexity_signals(stage, workspace),
                )
                started = time.monotonic()
                record = {
                    "prompt_version": stage_prompt_version(stage_type, mode),
                    "route": asdict(route),
                    "content": "",
                    "error": None,
                }
                case["stages"][stage_type] = record
                try:
                    system, user, rung = await build_prompt(
                        stage_type,
                        workspace,
                        None,
                        redis,
                        provider=route.provider,
                        model=route.model,
                    )
                    record.update(
                        system_prompt=system, user_prompt=user, compression_rung=rung
                    )
                    seconds = settings.stage_generation_deadline_seconds
                    control = GenerationControl(
                        run_id=uuid4(),
                        stage_id=stage.id,
                        redis=redis,
                        deadline_at=datetime.now(UTC) + timedelta(seconds=seconds),
                        duration_seconds=seconds,
                    )
                    # No DB cancellation monitor in this isolated evaluator.
                    control._monotonic_deadline = (
                        asyncio.get_running_loop().time() + seconds
                    )
                    parts = set()

                    async def checkpoint(chunk, ordinal, content, used_route, retries):
                        parts.add(chunk.key)
                        return len(parts)

                    async def phase_change(phase):
                        return None

                    artifact = await StageManager(
                        redis_client=redis
                    )._generate_durable_artifact(
                        route=route,
                        adapter_factory=budget.adapter,
                        system_prompt=system,
                        user_prompt=user,
                        stage_type=stage_type,
                        deps=_workspace_stage_deps(workspace, stage_type),
                        mode=mode,
                        emit=None,
                        phase=_PhaseTracker(),
                        control=control,
                        checkpoint=checkpoint,
                        phase_change=phase_change,
                    )
                    record["content"] = artifact.content
                    workspace.stages.append(
                        SimpleNamespace(
                            type=stage_type, content=artifact.content, current_version=1
                        )
                    )
                except Exception as exc:
                    record["error"] = f"{type(exc).__name__}: {exc}"
                    break
                finally:
                    record["seconds"] = time.monotonic() - started
                    args.output.write_text(json.dumps(result, indent=2, default=str))
    result["corpus_sha256"] = hashlib.sha256(
        json.dumps(case_inputs, sort_keys=True).encode()
    ).hexdigest()
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-live", action="store_true")
    parser.add_argument("--code-root", type=Path, required=True)
    parser.add_argument(
        "--corpus", type=Path, default=Path(__file__).parent / "golden_workspaces"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--modes", default="standard,demo_day")
    parser.add_argument("--max-provider-calls", type=int, default=100)
    args = parser.parse_args(argv)
    if not args.allow_live or args.max_provider_calls < 1:
        parser.error("Pass --allow-live and a positive --max-provider-calls")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = asyncio.run(generate(args))
    args.output.write_text(json.dumps(result, indent=2, default=str))
    return (
        1
        if any(
            len(case["stages"]) != 4
            or any(stage["error"] for stage in case["stages"].values())
            for case in result["cases"].values()
        )
        else 0
    )


if __name__ == "__main__":
    raise SystemExit(main())
