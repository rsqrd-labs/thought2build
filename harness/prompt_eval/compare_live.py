"""Gate newly generated packages against the candidate's production validators."""

import argparse
import json
import sys
from pathlib import Path


def compare(candidate: dict, baseline: dict) -> list[str]:
    from services.pipeline.artifact_validator import validate_readiness
    from services.pipeline.tech_safety import (
        analyze_local_technology_safety,
        is_blocking_finding,
    )
    from prompt_eval.graders import ALL_GRADERS

    failures = []
    if (
        not candidate.get("corpus_sha256")
        or candidate["corpus_sha256"] != baseline.get("corpus_sha256")
        or not candidate.get("cases")
        or candidate["cases"].keys() != baseline.get("cases", {}).keys()
    ):
        return ["Candidate and baseline must cover the same nonempty corpus."]
    for name, case in candidate["cases"].items():
        original = baseline["cases"][name]
        mode = case["input"][3]
        deps = {
            "problem_statement": case["input"][1],
            "clarification_qa": json.dumps(case["input"][2]),
        }
        base_deps = dict(deps)
        for stage_type in ("spec", "plan", "harness", "tasks"):
            stage = case["stages"].get(stage_type, {})
            base = original["stages"].get(stage_type, {})
            content, prior = stage.get("content", ""), base.get("content", "")
            if not content or stage.get("error"):
                failures.append(
                    f"{name}/{stage_type}: candidate generation missing/failed"
                )
                continue
            if not prior or base.get("error"):
                failures.append(
                    f"{name}/{stage_type}: baseline generation missing/failed"
                )
                continue
            try:
                validate_readiness(stage_type, content, deps, mode)
            except Exception as exc:
                failures.append(f"{name}/{stage_type}: {exc}")
            for finding in analyze_local_technology_safety(stage_type, content, deps):
                if is_blocking_finding(finding):
                    failures.append(f"{name}/{stage_type}: {finding.code}")
            # Existing golden graders use the standard document contract.
            # Demo Day is checked against its own production readiness schema.
            if mode == "standard":
                for grader in ALL_GRADERS:
                    if (
                        grader(stage_type, content, deps).score + 1e-9
                        < grader(stage_type, prior, base_deps).score
                    ):
                        failures.append(
                            f"{name}/{stage_type}: {grader.__name__} regressed"
                        )
            deps[stage_type], base_deps[stage_type] = content, prior
    return failures


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    args = parser.parse_args(argv)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "backend"))
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    failures = compare(
        json.loads(args.candidate.read_text()), json.loads(args.baseline.read_text())
    )
    print(
        "\n".join(failures)
        if failures
        else "PASS: generated candidate packages meet the release gate"
    )
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
