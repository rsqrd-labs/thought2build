from __future__ import annotations

import io
import json
import logging
import re
import zipfile
from pathlib import PurePosixPath
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Stage, Workspace
from services.pipeline import agent_manual_service, construction_verdict_service
from services.pipeline.export_verification import verification_manifest
from services.security.downstream_command_guard import redact_unsafe_lines

logger = logging.getLogger(__name__)

_STAGE_FILES = {
    "spec": "SPEC.md",
    "plan": "PLAN.md",
    "tasks": "TASKS.md",
}
_HARNESS_FALLBACK = "harness/HARNESS.md"
_FILE_HEADING_RE = re.compile(r"^#{2,3}\s+File:\s+(?P<filename>.+)$")
_FENCE_OPEN_RE = re.compile(r"^(?P<fence>`{3,})[a-zA-Z0-9_-]*$")
_FENCE_FILE_RE = re.compile(r"^(?P<fence>`{3,})[a-zA-Z0-9_-]+\s+(?P<filename>.+?)\s*$")
_WINDOWS_RESERVED_NAMES = frozenset(
    {"CON", "PRN", "AUX", "NUL"}
    | {f"COM{i}" for i in range(1, 10)}
    | {f"LPT{i}" for i in range(1, 10)}
)

# Audit-event name written whenever an exported doc/harness-fallback body has an
# unsafe line stripped (mirrors AUDIT_EVENT_CRITIC_DISABLED in critic.py: a
# module-owned constant next to the check it names, not the DB — there is no
# AuditEvent table). Kept local rather than in observability.py's
# GITHUB_AUDIT_* vocabulary because this fires for the plain ZIP export too,
# not just the GitHub push path.
AUDIT_EVENT_UNSAFE_CONTENT_REDACTED = "export.unsafe_content_redacted"


class ExportNotReadyError(Exception):
    pass


def _redact_stage_content(
    content: str, *, workspace_id: UUID | str, file_path: str
) -> str:
    """Strip unsafe shell-injection lines from a raw stage-derived file body
    before it is written into an exported artifact (ZIP or GitHub push).

    ``redact_unsafe_lines`` is already applied to the small harvested snippets
    folded into AGENTS.md/CLAUDE.md (``_named_section``/``_validation_commands``
    in ``agents_md_builder.py``); this closes the gap for the raw SPEC.md /
    PLAN.md / TASKS.md bodies, which are the actual files a downstream coding
    agent reads directly and a shared repo commits. Deliberately NOT applied to
    harness files extracted from labelled ``## File:`` blocks — those are real,
    executable scripts/tests/config where the same patterns (bare ``sudo``,
    ``curl``, ``rm -rf``) are ordinary and legitimate; see ``parse_harness_files``.
    """
    cleaned = redact_unsafe_lines(content)
    if cleaned != content:
        removed = len(content.splitlines()) - len(cleaned.splitlines())
        logger.info(
            AUDIT_EVENT_UNSAFE_CONTENT_REDACTED,
            extra={
                "audit_event": AUDIT_EVENT_UNSAFE_CONTENT_REDACTED,
                "workspace_id": str(workspace_id),
                "file_path": file_path,
                "redacted_lines": removed,
            },
        )
    return cleaned


def _safe_harness_path(filename: str) -> str | None:
    normalized = filename.strip().replace("\\", "/")
    if not normalized:
        return None
    if any(ord(char) < 32 for char in normalized):
        return None

    # Strip a leading "harness/" prefix the LLM includes in headings — we
    # always re-add it below via PurePosixPath("harness", path).
    if normalized.startswith("harness/"):
        normalized = normalized[len("harness/") :]
    if not normalized:
        return None

    path = PurePosixPath(normalized)
    if path.is_absolute() or ".." in path.parts:
        return None
    if not path.parts or any(":" in part for part in path.parts):
        return None
    for part in path.parts:
        stem = part.split(".", 1)[0].upper()
        if stem in _WINDOWS_RESERVED_NAMES:
            return None

    safe_path = PurePosixPath("harness", path)
    if len(safe_path.parts) <= 1:
        return None
    return safe_path.as_posix()


def _read_fenced_block(
    lines: list[str],
    fence_index: int,
    fence: str,
) -> tuple[str, int] | None:
    content_start = fence_index + 1
    k = content_start
    while k < len(lines) and lines[k].rstrip() != fence:
        k += 1
    if k >= len(lines):
        return None
    return "\n".join(lines[content_start:k]) + "\n", k


def _parse_labelled_harness_files(harness_content: str) -> tuple[dict[str, str], bool]:
    """State-machine parser: finds ## / ### File: headings and captures the
    immediately-following fenced code block as the file's content.

    Uses the fence string itself (backtick count) as the block delimiter so
    files whose content contains ``` (e.g. README.md) are handled correctly.
    """
    files: dict[str, str] = {}
    found_labelled_block = False
    lines = harness_content.split("\n")
    i = 0
    while i < len(lines):
        heading_match = _FILE_HEADING_RE.match(lines[i])
        if heading_match:
            filename = heading_match.group("filename").strip()
            # Skip blank lines between heading and fence opener
            j = i + 1
            while j < len(lines) and lines[j].strip() == "":
                j += 1
            if j < len(lines):
                fence_match = _FENCE_OPEN_RE.match(lines[j].rstrip())
                if fence_match:
                    fence = fence_match.group("fence")  # e.g. "```"
                    fenced_block = _read_fenced_block(lines, j, fence)
                    if fenced_block is not None:
                        content, k = fenced_block
                        found_labelled_block = True
                        path = _safe_harness_path(filename)
                        if path:
                            files[path] = content
                        i = k + 1
                        continue
        fence_file_match = _FENCE_FILE_RE.match(lines[i].rstrip())
        if fence_file_match:
            fence = fence_file_match.group("fence")
            fenced_block = _read_fenced_block(lines, i, fence)
            if fenced_block is not None:
                content, k = fenced_block
                found_labelled_block = True
                path = _safe_harness_path(fence_file_match.group("filename"))
                if path:
                    files[path] = content
                i = k + 1
                continue
        i += 1
    return files, found_labelled_block


def parse_harness_files(
    harness_content: str, *, workspace_id: UUID | str | None = None
) -> dict[str, str]:
    files, found_labelled_block = _parse_labelled_harness_files(harness_content)
    if files or found_labelled_block:
        # Real, executable code extracted from labelled ## File: blocks — never
        # redacted (see _redact_stage_content's docstring).
        return files
    if workspace_id is not None:
        fallback_body = _redact_stage_content(
            harness_content, workspace_id=workspace_id, file_path=_HARNESS_FALLBACK
        )
    else:
        fallback_body = redact_unsafe_lines(harness_content)
    return {_HARNESS_FALLBACK: fallback_body}


async def build_export(workspace_id: UUID, user_id: UUID, db: AsyncSession) -> bytes:
    ws_result = await db.execute(select(Workspace).where(Workspace.id == workspace_id))
    workspace = ws_result.scalar_one_or_none()
    if workspace is None or workspace.user_id != user_id:
        raise ExportNotReadyError("Workspace not found")

    stages_result = await db.execute(
        select(Stage).where(Stage.workspace_id == workspace_id)
    )
    stages = {s.type: s for s in stages_result.scalars()}

    for stage_type in ("spec", "plan", "harness", "tasks"):
        stage = stages.get(stage_type)
        if stage is None or stage.status != "finalised":
            raise ExportNotReadyError(
                f"Stage {stage_type!r} is not finalised — export unavailable"
            )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for stage_type, filename in _STAGE_FILES.items():
            content = _redact_stage_content(
                stages[stage_type].content or "",
                workspace_id=workspace_id,
                file_path=filename,
            )
            zf.writestr(filename, content)

        harness_content = stages["harness"].content or ""
        harness_files = parse_harness_files(harness_content, workspace_id=workspace_id)
        if harness_files.keys() == {_HARNESS_FALLBACK}:
            logger.warning(
                "harness parse yielded no files for workspace_id=%s, using fallback",
                workspace_id,
            )

        for path, content in harness_files.items():
            zf.writestr(path, content)

        zf.writestr(
            "VERIFICATION.json",
            json.dumps(verification_manifest(workspace, stages), indent=2),
        )

        stage_md = {kind: (stage.content or "") for kind, stage in stages.items()}
        for (
            instruction_path,
            instruction_body,
        ) in agent_manual_service.build_instruction_files(workspace, stage_md).items():
            zf.writestr(instruction_path, instruction_body)

        # CONSTRUCTION_REPORT.md (plan §7.3/§8.2), BOTH modes: the verifier now
        # runs for standard workspaces too, so gating the report on demo_day left
        # standard users with a badge reading "N gaps" and no surface anywhere
        # that names them — and, worse, no way to refresh a stale verdict at all,
        # because this is the only place `ensure_fresh_verdict` is called. Refresh
        # if a stage was refined after the verdict was computed (zero-LLM, cheap),
        # then render. ensure_fresh_verdict is fail-open, so a hiccup just ships
        # the last-known verdict (or omits the report) rather than failing the
        # download.
        verdict = await construction_verdict_service.ensure_fresh_verdict(
            db, workspace, stages
        )
        if verdict:
            report = agent_manual_service.build_construction_report(verdict)
            zf.writestr(agent_manual_service.CONSTRUCTION_REPORT_FILENAME, report)

    return buf.getvalue()


async def build_agent_instruction_export(
    workspace_id: UUID,
    user_id: UUID,
    db: AsyncSession,
    target: str,
) -> tuple[bytes, str, str]:
    """Build one Markdown instruction file or a two-file ZIP download."""
    if target not in {"codex", "claude_code", "both"}:
        raise ValueError("unsupported agent-instruction target")
    ws_result = await db.execute(select(Workspace).where(Workspace.id == workspace_id))
    workspace = ws_result.scalar_one_or_none()
    if workspace is None or workspace.user_id != user_id:
        raise ExportNotReadyError("Workspace not found")
    stages_result = await db.execute(
        select(Stage).where(Stage.workspace_id == workspace_id)
    )
    stages = {stage.type: stage for stage in stages_result.scalars()}
    for stage_type in ("spec", "plan", "harness", "tasks"):
        stage = stages.get(stage_type)
        if stage is None or stage.status != "finalised":
            raise ExportNotReadyError(
                f"Stage {stage_type!r} is not finalised — export unavailable"
            )
    stage_md = {kind: (stage.content or "") for kind, stage in stages.items()}
    files = agent_manual_service.build_instruction_files(
        workspace, stage_md, target_agent=target
    )
    if target != "both":
        filename, content = next(iter(files.items()))
        return content.encode("utf-8"), filename, "text/markdown; charset=utf-8"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for filename in ("AGENTS.md", "CLAUDE.md"):
            zf.writestr(filename, files[filename])
    return buf.getvalue(), "thought2build-agent-instructions.zip", "application/zip"
