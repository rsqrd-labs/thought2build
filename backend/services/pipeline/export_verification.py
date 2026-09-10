"""Portable evidence for an exported package, including explicit overrides."""

import hashlib

from services.pipeline.artifact_validator import validate_readiness
from services.pipeline.input_manifest import DEPENDENCIES, source_identity
from services.pipeline.tech_safety import (
    analyze_local_technology_safety,
    is_blocking_finding,
)


def verification_manifest(workspace, stages: dict) -> dict:
    result = {"schema": 1, "stages": {}, "ready": True}
    for kind in DEPENDENCIES:
        stage = stages[kind]
        content = stage.content or ""
        findings = []
        try:
            validate_readiness(
                kind,
                content,
                {dep: stages[dep].content or "" for dep in DEPENDENCIES[kind]},
                getattr(workspace, "mode", "standard") or "standard",
            )
        except ValueError as exc:
            findings.append(str(exc))
        except RuntimeError as exc:
            findings.append(str(exc))
        local = analyze_local_technology_safety(
            kind,
            content,
            {dep: stages[dep].content or "" for dep in DEPENDENCIES[kind]},
        )
        origin = getattr(stage, "source_identity", None)
        current = source_identity(workspace, kind, stages=stages.values())
        if origin is None:
            findings.append("Source provenance unavailable for this legacy artifact.")
        elif origin != current:
            findings.append("Source inputs have changed since generation.")
        overridden = (
            getattr(stage, "quality_gate_status", None) == "overridden"
            and getattr(stage, "quality_gate_version", None) == stage.current_version
        )
        result["stages"][kind] = {
            "version": stage.current_version,
            "content_sha256": hashlib.sha256(content.encode()).hexdigest(),
            "source_identity": origin,
            "quality_gate_status": getattr(stage, "quality_gate_status", None),
            "owner_override": overridden,
            "structural_findings": findings,
            "local_technology_findings": [item.to_payload() for item in local],
        }
        if (
            findings
            or any(is_blocking_finding(item) for item in local)
            or overridden
            or getattr(stage, "quality_gate_status", None) in {"blocked", "checking"}
        ):
            result["ready"] = False
    return result
