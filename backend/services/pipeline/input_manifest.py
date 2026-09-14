"""Immutable, private input identity shared by caches and generation runs."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

from config import settings

DEPENDENCIES = {
    "spec": (),
    "plan": ("spec",),
    "harness": ("spec", "plan"),
    "tasks": ("spec", "plan", "harness"),
}
INPUT_FIELDS = (
    "problem_statement",
    "clarification_qa",
    "mode",
    "time_budget_minutes",
    "restricted_environment",
    "template_slug",
    "disable_critic",
    "brave_research_enabled",
    "target_agent",
)


def digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def snapshot_workspace(workspace, stage_type: str, *, stages=None) -> dict:
    return deepcopy(
        {
            "id": str(workspace.id),
            "user_id": str(getattr(workspace, "user_id", "")),
            **{key: getattr(workspace, key, None) for key in INPUT_FIELDS},
            "stages": [
                {
                    "type": stage.type,
                    "content": stage.content or "",
                    "current_version": stage.current_version,
                }
                for stage in sorted(
                    workspace.stages if stages is None else stages,
                    key=lambda item: item.type,
                )
                if stage.type in DEPENDENCIES[stage_type]
            ],
        }
    )


def restore_workspace(snapshot: dict):
    data = dict(snapshot)
    data["id"] = UUID(data["id"])
    data["stages"] = [SimpleNamespace(**stage) for stage in data["stages"]]
    return SimpleNamespace(**data)


def source_identity(workspace, stage_type: str, *, stages=None) -> dict:
    snapshot = snapshot_workspace(workspace, stage_type, stages=stages)
    # Advisory switches do not invalidate an accepted product artifact.
    snapshot.pop("disable_critic", None)
    return {
        "schema": 1,
        "source_hash": digest(snapshot),
        "upstream_versions": {
            stage["type"]: stage["current_version"] for stage in snapshot["stages"]
        },
    }


@lru_cache(maxsize=1)
def generation_bundle_hash() -> str:
    """Hash shipped generation code, not an operator-maintained version label."""
    root = Path(__file__).resolve().parents[2]
    paths = [
        *root.joinpath("prompts").glob("*.py"),
        *root.joinpath("services/pipeline").glob("*.py"),
        *root.joinpath("services/llm").glob("*.py"),
        *root.joinpath("services/pipeline").glob("*.json"),
        *root.joinpath("services/llm").glob("*.json"),
        root / "config.py",
    ]
    return digest(
        {
            str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(paths)
        }
    )


def cache_identity(workspace, stage_type: str) -> dict:
    return {
        **source_identity(workspace, stage_type),
        "workspace_id": str(workspace.id),
        "prompt_bundle": generation_bundle_hash(),
        "remote_pins": deepcopy(settings.langfuse_prompt_pins),
        "compression": [
            settings.problem_statement_compression,
            settings.problem_statement_abstractive,
            settings.problem_statement_budget_tokens,
        ],
    }
