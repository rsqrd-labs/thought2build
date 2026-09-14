"""Phase 19 offline prompt eval runner.

Usage:
    uv run python -m prompt_eval.run --version asdd-v1.9.0 --baseline asdd-v1.8.0

Grades committed historical fixtures, NOT outputs from candidate prompts.
Cost is zero because no generation happens. See live.py for candidate evaluation. Exit code 1 means at least one grader
regressed against the committed baseline.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from prompt_eval.anonymize import assert_no_pii
from prompt_eval.graders import ALL_GRADERS, GraderResult

GOLDEN_ROOT = Path(__file__).parent / "golden_workspaces"
STAGE_FILES = {
    "spec": "spec.md",
    "plan": "plan.md",
    "harness": "harness.md",
    "tasks": "tasks.md",
}
BASELINE_FILE = "baseline_scores.json"
EPSILON = 1e-9


@dataclass(frozen=True)
class WorkspaceSnapshot:
    name: str
    root: Path
    problem_statement: str
    clarification_qa: str
    artifacts: dict[str, str]
    baseline_scores: dict[str, float]


@dataclass(frozen=True)
class StageRun:
    workspace: str
    stage_type: str
    latency_ms: float
    cost_usd: float
    results: tuple[GraderResult, ...]


@dataclass(frozen=True)
class WorkspaceRun:
    snapshot: WorkspaceSnapshot
    stages: tuple[StageRun, ...]
    aggregate_scores: dict[str, float]


def _read_required(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Missing required prompt eval file: {path}")
    return path.read_text(encoding="utf-8")


def _load_baseline(path: Path) -> dict[str, float]:
    payload = json.loads(_read_required(path))
    scores = payload.get("scores")
    if not isinstance(scores, dict):
        raise ValueError(f"{path} must contain a top-level 'scores' object.")
    return {str(name): float(value) for name, value in scores.items()}


def load_snapshots(root: Path = GOLDEN_ROOT) -> tuple[WorkspaceSnapshot, ...]:
    snapshots: list[WorkspaceSnapshot] = []
    for workspace_root in sorted(path for path in root.iterdir() if path.is_dir()):
        artifacts = {
            stage: _read_required(workspace_root / filename)
            for stage, filename in STAGE_FILES.items()
        }
        for filename in [
            "problem_statement.md",
            "clarification_qa.json",
            *STAGE_FILES.values(),
        ]:
            path = workspace_root / filename
            assert_no_pii(_read_required(path), path=path)
        snapshots.append(
            WorkspaceSnapshot(
                name=workspace_root.name,
                root=workspace_root,
                problem_statement=_read_required(
                    workspace_root / "problem_statement.md"
                ),
                clarification_qa=_read_required(
                    workspace_root / "clarification_qa.json"
                ),
                artifacts=artifacts,
                baseline_scores=_load_baseline(workspace_root / BASELINE_FILE),
            )
        )
    if not snapshots:
        raise ValueError(f"No golden workspaces found under {root}.")
    return tuple(snapshots)


def _deps_for_stage(snapshot: WorkspaceSnapshot, stage_type: str) -> dict[str, str]:
    deps = {
        "problem_statement": snapshot.problem_statement,
        "clarification_qa": snapshot.clarification_qa,
    }
    if stage_type in {"plan", "harness", "tasks"}:
        deps["spec"] = snapshot.artifacts["spec"]
    if stage_type in {"harness", "tasks"}:
        deps["plan"] = snapshot.artifacts["plan"]
    if stage_type == "tasks":
        deps["harness"] = snapshot.artifacts["harness"]
    return deps


def run_snapshot(snapshot: WorkspaceSnapshot) -> WorkspaceRun:
    stages: list[StageRun] = []
    for stage_type, artifact_md in snapshot.artifacts.items():
        deps = _deps_for_stage(snapshot, stage_type)
        start = time.perf_counter()
        results = tuple(grader(stage_type, artifact_md, deps) for grader in ALL_GRADERS)
        latency_ms = (time.perf_counter() - start) * 1000
        for result in results:
            print(
                f"{snapshot.name} {stage_type} {result.name}={result.score:.3f}",
                flush=True,
            )
        stages.append(
            StageRun(
                workspace=snapshot.name,
                stage_type=stage_type,
                latency_ms=latency_ms,
                cost_usd=0.0,
                results=results,
            )
        )

    aggregate_scores: dict[str, float] = {}
    for grader in ALL_GRADERS:
        name = grader.__name__
        values = [
            result.score
            for stage in stages
            for result in stage.results
            if result.name == name
        ]
        aggregate_scores[name] = statistics.fmean(values)
    return WorkspaceRun(
        snapshot=snapshot,
        stages=tuple(stages),
        aggregate_scores=aggregate_scores,
    )


def _collect_regressions(runs: tuple[WorkspaceRun, ...]) -> list[str]:
    regressions: list[str] = []
    expected_names = {grader.__name__ for grader in ALL_GRADERS}
    for run in runs:
        missing_baselines = sorted(expected_names - set(run.snapshot.baseline_scores))
        if missing_baselines:
            regressions.append(
                f"{run.snapshot.name}: baseline missing graders "
                f"{', '.join(missing_baselines)}"
            )
            continue
        for name, current in sorted(run.aggregate_scores.items()):
            baseline = run.snapshot.baseline_scores[name]
            if current + EPSILON < baseline:
                regressions.append(
                    f"{run.snapshot.name} {name}: {current:.3f} < {baseline:.3f}"
                )
    return regressions


def _axis_for_grader(name: str, sample: WorkspaceRun) -> str:
    for stage in sample.stages:
        for result in stage.results:
            if result.name == name:
                return result.axis
    return "unknown"


def write_report(
    *,
    version: str,
    baseline: str,
    report_path: Path,
    runs: tuple[WorkspaceRun, ...],
    regressions: list[str],
) -> None:
    lines: list[str] = [
        "# Committed-fixture grader regression report",
        "",
        "This report does not evaluate changed prompts. Live candidate evidence is required separately.",
        "",
        f"- Version: `{version}`",
        f"- Baseline: `{baseline}`",
        f"- Result: {'FAIL' if regressions else 'PASS'}",
        f"- Golden workspaces: {len(runs)}",
        "",
    ]
    if regressions:
        lines.extend(["## Regressions", ""])
        lines.extend(f"- {item}" for item in regressions)
        lines.append("")

    for run in runs:
        lines.extend([f"## Workspace: {run.snapshot.name}", ""])
        lines.append("| Axis | Grader | Current | Baseline | Delta |")
        lines.append("|---|---|---:|---:|---:|")
        for name, current in sorted(run.aggregate_scores.items()):
            baseline_score = run.snapshot.baseline_scores.get(name, 0.0)
            axis = _axis_for_grader(name, run)
            lines.append(
                f"| {axis} | `{name}` | {current:.3f} | "
                f"{baseline_score:.3f} | {current - baseline_score:+.3f} |"
            )
        lines.append("")

    lines.extend(["## Stage Cost And Latency", ""])
    lines.append("| Workspace | Stage | Cost USD | Grading latency ms |")
    lines.append("|---|---|---:|---:|")
    for run in runs:
        for stage in run.stages:
            lines.append(
                f"| {stage.workspace} | {stage.stage_type} | "
                f"{stage.cost_usd:.4f} | {stage.latency_ms:.2f} |"
            )
    lines.append("")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Phase 19 prompt eval suite.")
    parser.add_argument("--version", required=True, help="Prompt version to test")
    parser.add_argument("--baseline", required=True, help="Baseline prompt version")
    parser.add_argument(
        "--report",
        default="prompt_eval_report.md",
        help="Output markdown report path",
    )
    parser.add_argument(
        "--golden-root",
        type=Path,
        default=GOLDEN_ROOT,
        help="Root directory containing golden workspace snapshots",
    )
    args = parser.parse_args(argv)

    snapshots = load_snapshots(args.golden_root)
    for snapshot in snapshots:
        declared = json.loads((snapshot.root / BASELINE_FILE).read_text()).get(
            "version"
        )
        if declared != args.baseline:
            raise ValueError(
                f"{snapshot.name}: requested baseline {args.baseline} != fixture {declared}"
            )
    runs = tuple(run_snapshot(snapshot) for snapshot in snapshots)
    regressions = _collect_regressions(runs)
    write_report(
        version=args.version,
        baseline=args.baseline,
        report_path=Path(args.report),
        runs=runs,
        regressions=regressions,
    )
    if regressions:
        for regression in regressions:
            print(f"REGRESSION: {regression}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
