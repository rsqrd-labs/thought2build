"""Phase 19 zero-LLM artifact validator.

The validator runs BEFORE the critic (T-247) because section-presence is
the cheapest possible gate — regex/substring match, no LLM call, < 5 ms on a
200K-char artifact.  Section-aware: SECTION_CONTRACTS encodes the required
headings per stage; the conditional sentinels trigger the Frontend
Architecture section only when an upstream artifact mentions a browser-facing
surface.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field

from services.pipeline.markdown_structure import headings, matches_heading

# Required section headings per stage.  Order is the order they appear in the
# system prompt for each stage; the validator does NOT enforce order (just
# presence), but keeping the list in canonical order makes it easier to audit
# against the system prompt.
SECTION_CONTRACTS: dict[str, list[str]] = {
    # Density initiative (2026-08-02): trimmed from 25 to 17 sections. Merges
    # (old -> new home): User Problems, Users and Personas -> Overview;
    # Success Metrics -> Product Goals; User Journeys, User Flow Diagrams ->
    # User Flows (new); High-Level System Context, Feature Interaction
    # Overview, Integrations and External Touchpoints -> System Context (new);
    # Permissions and Access Expectations -> Security, Privacy, and Abuse
    # Expectations; Error Handling and Recovery -> Edge Cases. No topic was
    # dropped, only the standalone heading + its own preamble overhead.
    "spec": [
        "## Overview",
        "## Product Goals",
        "## In-Scope (MVP)",
        "## Non-Goals",
        "## User Stories",
        "## User Flows",
        "## Functional Requirements",
        "## Non-Functional Requirements",
        "## Conceptual Domain Model",
        "## System Context",
        "## Security, Privacy, and Abuse Expectations",
        "## Acceptance Criteria",
        "## Edge Cases",
        "## Constraints",
        "## Risks",
        "## Assumptions and Open Questions",
        "## Out of Scope",
    ],
    # Density initiative (2026-08-02): trimmed from 27(+1) to 20(+1) sections.
    # Merges (old -> new home): Assumptions and Open Questions -> Planning
    # Summary; Scalability and Performance -> Capacity Model; Risks and
    # Mitigations -> Failure Mode and Effects Analysis; Directory and File
    # Structure + Module Boundaries and Interfaces -> Codebase Structure
    # (new); Privacy and Data Handling -> Security Architecture; Testing
    # Strategy + Rollout and Migration Plan -> Deployment and Operations.
    # ## Prompt and AI Safety Controls is deleted entirely (it was never in
    # this contract, never validated, never graded — pure prompt cleanup).
    # Order matches the four chunk groupings in stage_manager.py's
    # _chunk_specs_for_stage("plan") contiguously (4/5/6/5) so tests and
    # fixtures can slice this list directly instead of re-deriving the split.
    # Architecture Decision Records sits at the END of the third group (not
    # grouped with Planning Summary/Tech Stack) specifically so its position
    # relative to Data Model/API Design/Security Architecture matches
    # prompt_eval/graders/format.py's _EXPECTED_H2["plan"] canonical order
    # (unchanged, pre-existing) — moving it earlier would flag every
    # already-committed golden-corpus fixture as heading-order-violating.
    "plan": [
        "## Planning Summary",
        "## Architecture Overview",
        "## Requirement Traceability Matrix",
        "## Technology Stack and Rationale",
        "## Architecture Anti-Patterns",  # T-239
        "## Multi-tenancy Stance",  # T-239
        "## Capacity Model",  # T-240
        "## Threat Model",  # T-240 (STRIDE)
        # Stored as the FULL heading (not the truncated "## Architecture Quality
        # Attribute") so it matches BOTH consumers of this list: the substring
        # check in validate_sections AND the line-anchored regex in _section_body.
        # A truncated entry passes the substring gate but makes _section_body
        # extract an empty body, firing a false `shallow_required_section`
        # advisory on every plan (audit finding #1). Keep contract headings
        # verbatim with the prompt's real heading.
        "## Architecture Quality Attribute Matrix",  # T-240
        "## Codebase Structure",
        "## Data Model and Persistence",
        "## API Design",
        "## Authentication and Authorization",
        "## Security Architecture",
        "## Architecture Decision Records",  # T-239
        "## Failure Mode and Effects Analysis",  # T-240
        "## SLOs and Error Budgets",  # T-240
        "## Error Handling and Recovery",
        "## Observability and Audit Logging",
        "## Deployment and Operations",
    ],
    "harness": [
        "## Harness Overview",
        "## Requirement-to-Test Matrix",
        "## Coverage Plan",
        "## File Tree",
        "## Files",
    ],
    "tasks": [
        "## Effort Summary",
        "## Execution Overview",
        "## Traceability Overview",
        "## Dependency Graph",
        "## Task Sizing Legend",
    ],
}

# Demo Day mode contracts (docs/DEMO_DAY_MODE_IMPLEMENTATION_PLAN.md §6). A
# parallel, mode-keyed structure: leaner than the standard contract and
# re-pointed for a ≤5-hour build, plus the three rubric sections (AI Usage /
# Security Posture / Scalability Story) so the demo-day questions are always
# answered. Selected when ``workspace.mode == "demo_day"``. Standard contracts
# above are untouched (the §4 byte-identical regression-pin contract).
DEMO_DAY_SECTION_CONTRACTS: dict[str, list[str]] = {
    "spec": [
        "## Overview",
        "## Target User and Core Problem",
        "## Demo Day Scope",
        "## Out of Scope",
        "## Functional Requirements",
        "## Acceptance Criteria",
        "## Success Demo",
        "## AI Usage",
        "## Security Posture",
        "## Scalability Story",
        "## Risks and Assumptions",
    ],
    "plan": [
        "## Architecture Overview",
        "## Technology Stack",
        "## Requirement Traceability Matrix",
        "## Interface Contracts",
        "## Data Model and Persistence",
        "## External Integrations and Secrets",
        "## Build Sequence",
        "## Environment and Bootstrap",
        "## Architecture Decision Records",
        "## Scalability and Performance",
        "## Security Architecture",
        # Same heading string as the standard contract — see _CONDITIONAL_SECTIONS:
        # Demo Day's copy is unconditionally listed here (Demo Day has no
        # sentinel-gated conditional-section mechanism) but still honours the
        # shared "Not applicable because <reason>" escape for a non-browser-facing
        # build, since that exemption keys off the heading string, not the mode.
        "## Frontend Architecture",
        "## Risks and Mitigations",
    ],
    "harness": [
        "## Harness Overview",
        "## Frozen Interface Contracts",
        "## Requirement-to-Test Matrix",
        "## End-to-End Smoke Test",
        "## File Tree",
        "## Files",
    ],
    "tasks": [
        "## Effort Summary",
        "## Build Order",
        "## Traceability Overview",
        "## Tasks",
    ],
}


def section_contract(stage_type: str, mode: str = "standard") -> list[str]:
    """The required section headings for a stage, selected by workspace mode.

    ``mode="demo_day"`` returns the lean rubric-aware Demo Day contract; any
    other value (the default) returns the unchanged standard contract.
    """
    if mode == "demo_day":
        return DEMO_DAY_SECTION_CONTRACTS.get(stage_type, [])
    return SECTION_CONTRACTS.get(stage_type, [])


# Conditional sections — keyed by stage, value is a list of (sentinel_regex,
# section_heading) pairs.  When any sentinel matches an upstream artifact, the
# section must appear in the current artifact.
_CONDITIONAL_SECTIONS: dict[str, list[tuple[re.Pattern[str], str]]] = {
    "plan": [
        (
            re.compile(
                r"\b(UI|web|app|page|screen|dashboard|console)\b",
                re.IGNORECASE,
            ),
            "## Frontend Architecture",  # T-242
        ),
    ],
}

COMPLETION_CONTRACT_VERSION = "v2"
FINAL_COMPLETION_SENTINEL_TEMPLATE = "<!-- THOUGHT2BUILD_COMPLETE:{stage}:v2 -->"
CHUNK_COMPLETION_SENTINEL_TEMPLATE = (
    "<!-- THOUGHT2BUILD_CHUNK_COMPLETE:{stage}:{chunk}:v2 -->"
)
_REQUIREMENT_ID_RE = re.compile(r"\b(?:FR|NFR|SEC)-\d{3}\b")
_AC_ID_RE = re.compile(r"\bAC-\d{3}\b")
_TASK_HEADER_RE = re.compile(r"^###\s+T-\d{3}:", re.MULTILINE)
_TASK_DEP_RE = re.compile(r"\bT-(\d{3})\b")
_TASK_FIELD_RE = re.compile(r"^\*\*(?P<field>[^*]+):\*\*\s*(?P<value>.*)$")
# NOTE: the old ``_FILE_BLOCK_RE`` ("heading immediately followed by a complete
# fenced block") is deliberately gone. "Was this promised file actually emitted"
# now has exactly ONE implementation — ``harness_test_index(...).files_with_body``
# via :func:`_emitted_file_index` — because the second, weaker definition was
# reachable only from ``_harness_issues`` (standard mode) and left Demo Day with
# no file-body check at all.
_INCOMPLETE_TRAILING_RE = re.compile(r"(:|,\s*|\|\s*)$")


def _ends_with_complete_table_row(final_line: str) -> bool:
    """True when the artifact's last line is a complete markdown table row.

    A well-formed table row is pipe-delimited and *legitimately* ends every row
    with ``|`` (``| a | b |``), so a document that closes on a table — e.g. a
    plan's "Open Questions" matrix — naturally has a trailing pipe.  The
    ``_INCOMPLETE_TRAILING_RE`` ``|`` clause exists to catch a row truncated
    mid-cell, but such a cut ends *inside* the cell text (no trailing ``|``), so
    a row that ends with ``|`` and carries an interior separator (``count >= 2``)
    is complete, not dangling.  Genuine mid-table truncation that happens to land
    on a cell boundary is still caught by the provider ``stopped_by_limit`` and
    ``missing_completion_sentinel`` checks; this only removes the false positive
    that flagged every table-closing artifact as truncated.
    """
    return final_line.endswith("|") and final_line.count("|") >= 2


# The ONLY two signals that mean the platform produced genuinely unusable output
# and must refund: the provider hard-stopped on its output-token cap
# (`provider_stopped_by_limit`) or nothing was produced at all (`empty_artifact`).
# These are objective, provider-reported facts — not heuristics — so they alone
# cost the platform a refund (and earn the single budget-doubling repair).
#
# EVERY other completeness code — a missing internal completion sentinel, an
# unbalanced code fence, an incomplete harness file block, a dangling trailing
# line, plus all depth/quality opinions (shallow section, thin requirement count,
# traceability gap) — is delivered as a NON-blocking advisory finding and NEVER
# refunded or re-run.  The artifact is usable and the user owns it (same stance as
# the critic / missing_sections, issue #34).  This is the fix for the false-refund
# + rerun bleed: a model that ends its turn naturally (no `stopped_by_limit`) has
# *finished* — if it merely omitted our magic-comment marker or emitted a stray
# ``` , that is a formatting heuristic, not truncation, and must not burn the
# user's credit or trigger a regenerate cascade.  Genuine structural loss (a
# dropped required section) is still caught independently by `validate_sections`,
# which blocks terminally **without** refunding and is user-overridable — strictly
# better than a refund.  Keeping this discriminator exhaustive over the codes the
# validator emits is the single source of truth for "does this cost a refund".
REFUNDABLE_INCOMPLETE_CODES: frozenset[str] = frozenset(
    {
        "empty_artifact",
        "provider_stopped_by_limit",
    }
)


@dataclass(frozen=True)
class CompletenessIssue:
    code: str
    detail: str
    reference: str | None = None

    @property
    def is_refundable(self) -> bool:
        """True when this issue means genuinely truncated/corrupt output."""
        return self.code in REFUNDABLE_INCOMPLETE_CODES


class IncompleteArtifactError(RuntimeError):
    """Raised when an artifact has the right broad shape but is not complete."""

    def __init__(
        self,
        stage_type: str,
        issues: list[CompletenessIssue],
        *,
        partial_content: str = "",
        repair_attempted: bool = False,
    ) -> None:
        self.stage_type = stage_type
        self.issues = issues
        self.partial_content = partial_content
        self.repair_attempted = repair_attempted
        joined = "; ".join(issue.detail for issue in issues)
        super().__init__(f"Stage {stage_type} artifact is incomplete: {joined}")

    @property
    def truncation_issues(self) -> list[CompletenessIssue]:
        """The refundable (truncated/corrupt) subset of issues."""
        return [issue for issue in self.issues if issue.is_refundable]

    @property
    def depth_issues(self) -> list[CompletenessIssue]:
        """The non-refundable depth/quality subset, attached as advisory."""
        return [issue for issue in self.issues if not issue.is_refundable]


class MissingSectionError(RuntimeError):
    """Raised when a generated artifact omits one or more required headings."""

    def __init__(self, stage_type: str, missing: list[str]) -> None:
        self.stage_type = stage_type
        self.missing = missing
        super().__init__(
            f"Stage {stage_type} is missing required sections: {', '.join(missing)}"
        )


def validate_sections(
    stage_type: str,
    artifact_md: str,
    deps: dict[str, str] | None = None,
    mode: str = "standard",
) -> None:
    """Assert every required section heading appears in artifact_md.

    The contract is selected by ``mode`` (standard vs demo_day). Conditional
    sections (T-242 Frontend Architecture) are a standard-mode concept enforced
    only when their sentinel matches in the upstream deps; the lean Demo Day
    contract has no *sentinel-gated* conditional sections — its own Frontend
    Architecture entry is unconditionally required instead, but still honours
    the same "Not applicable because <reason>" escape (see
    ``_conditional_headings_for_stage``, which is not mode-gated) for a
    non-browser-facing build.

    Raises MissingSectionError listing all absent headings (does NOT
    short-circuit at the first miss — returning the full list improves UX).
    """
    required = list(section_contract(stage_type, mode))
    deps = deps or {}
    upstream = " ".join(deps.values())
    if mode != "demo_day":
        for sentinel, heading in _CONDITIONAL_SECTIONS.get(stage_type, []):
            if sentinel.search(upstream):
                required.append(heading)

    actual = {heading for heading, _, _ in headings(artifact_md)}
    missing = [
        heading
        for heading in required
        if not any(matches_heading(item, heading) for item in actual)
    ]
    if missing:
        raise MissingSectionError(stage_type, missing)


# Readiness and billing are independent. These failures block ordinary use but
# do not trigger refunds or automatic full regeneration.
STRUCTURAL_BLOCK_CODES = frozenset(
    {
        "unbalanced_code_fence",
        "harness_file_tree_missing_block",
        "harness_matrix_missing_file",
        "harness_matrix_missing_test",
        "missing_harness_file_blocks",
        "incomplete_harness_file_block",
        "missing_task_blocks",
        "incomplete_task_block",
        "incomplete_task_fields",
        "invalid_task_dependency_order",
        "task_harness_ref_not_found",
        "rtm_missing_upstream_id",
        "insufficient_upstream_traceability",
        "harness_requirement_not_test_mapped",
    }
)


class StructuralReadinessError(MissingSectionError):
    kind = "structural_validity"


def validate_readiness(
    stage_type: str,
    artifact_md: str,
    deps: dict[str, str] | None = None,
    mode: str = "standard",
) -> None:
    validate_sections(stage_type, artifact_md, deps, mode)
    try:
        validate_artifact_completeness(stage_type, artifact_md, deps, mode)
    except IncompleteArtifactError as exc:
        blockers = [
            issue
            for issue in exc.issues
            if issue.code in STRUCTURAL_BLOCK_CODES or issue.is_refundable
        ]
        if blockers:
            raise StructuralReadinessError(
                stage_type, [f"{issue.code}: {issue.detail}" for issue in blockers]
            ) from exc


async def validate_readiness_async(
    stage_type: str,
    artifact_md: str,
    deps: dict[str, str] | None = None,
    mode: str = "standard",
) -> None:
    from services.cpu_offload import run_cpu_bound

    await run_cpu_bound(
        artifact_md, validate_readiness, stage_type, artifact_md, deps, mode
    )


async def validate_sections_async(
    stage_type: str,
    artifact_md: str,
    deps: dict[str, str] | None = None,
    mode: str = "standard",
) -> None:
    """Async ``validate_sections``: offloads the section scan off the loop (F7).

    Raises ``MissingSectionError`` identically to ``validate_sections`` (the
    exception propagates through the executor). Large artifacts are dispatched to
    the dedicated CPU pool; small ones run inline.
    """
    from services.cpu_offload import run_cpu_bound

    await run_cpu_bound(
        artifact_md, validate_sections, stage_type, artifact_md, deps, mode
    )


def final_completion_sentinel(stage_type: str) -> str:
    return FINAL_COMPLETION_SENTINEL_TEMPLATE.format(stage=stage_type)


def chunk_completion_sentinel(stage_type: str, chunk_key: str) -> str:
    safe_key = re.sub(r"[^a-z0-9_-]+", "-", chunk_key.lower()).strip("-") or "chunk"
    return CHUNK_COMPLETION_SENTINEL_TEMPLATE.format(
        stage=stage_type,
        chunk=safe_key,
    )


def completion_instruction(stage_type: str, *, chunk_key: str | None = None) -> str:
    sentinel = (
        final_completion_sentinel(stage_type)
        if chunk_key is None
        else chunk_completion_sentinel(stage_type, chunk_key)
    )
    final_line = (
        "- End the response with this exact sentinel on its own final line: "
        f"{sentinel}\n"
    )
    return (
        "\n\nCompletion contract:\n"
        f"{final_line}"
        "- Do not put any content after the sentinel.\n"
        "- Every requested heading must have substantive body content; do not emit "
        "placeholder, TODO, TBD, or summary-only sections.\n"
    )


def strip_completion_sentinel(
    stage_type: str,
    artifact_md: str,
    *,
    chunk_key: str | None = None,
) -> str:
    sentinel = (
        final_completion_sentinel(stage_type)
        if chunk_key is None
        else chunk_completion_sentinel(stage_type, chunk_key)
    )
    lines = artifact_md.rstrip().splitlines()
    if lines and lines[-1].strip() == sentinel:
        return "\n".join(lines[:-1]).strip()
    return artifact_md.strip()


def validate_completion_sentinel(
    stage_type: str,
    artifact_md: str,
    *,
    chunk_key: str | None = None,
) -> None:
    sentinel = (
        final_completion_sentinel(stage_type)
        if chunk_key is None
        else chunk_completion_sentinel(stage_type, chunk_key)
    )
    lines = [line.strip() for line in artifact_md.strip().splitlines() if line.strip()]
    if not lines or lines[-1] != sentinel:
        raise IncompleteArtifactError(
            stage_type,
            [
                CompletenessIssue(
                    code="missing_completion_sentinel",
                    detail="The model did not emit the required completion sentinel.",
                    reference=chunk_key,
                )
            ],
            partial_content=artifact_md,
        )


def validate_artifact_completeness(
    stage_type: str,
    artifact_md: str,
    deps: dict[str, str] | None = None,
    mode: str = "standard",
) -> None:
    deps = deps or {}
    issues: list[CompletenessIssue] = []
    stripped = artifact_md.strip()
    if not stripped:
        issues.append(
            CompletenessIssue(
                code="empty_artifact",
                detail="The generated artifact is empty.",
            )
        )
    issues.extend(_section_body_issues(stage_type, stripped, deps, mode))
    issues.extend(_markdown_shape_issues(stripped))
    if mode == "demo_day":
        # Lean Demo-Day-appropriate floors (§6.5). NOT the standard 16-field /
        # ≥5-FR rigor — a ≤5-hour build is deliberately smaller.
        if stage_type == "spec":
            issues.extend(_demo_day_spec_issues(stripped))
        if stage_type == "harness":
            issues.extend(_demo_day_harness_issues(stripped))
        if stage_type == "tasks":
            issues.extend(_demo_day_task_issues(stripped))
    else:
        if stage_type == "spec":
            issues.extend(_spec_issues(stripped))
        if stage_type == "plan":
            issues.extend(_plan_issues(stripped, deps))
        if stage_type == "harness":
            issues.extend(_harness_issues(stripped, deps))
        if stage_type == "tasks":
            issues.extend(_task_issues(stripped, deps))
    if stage_type in {"plan", "harness", "tasks"}:
        # Cross-stage ID preservation (every upstream FR/NFR/SEC/AC present) is
        # mode-agnostic and load-bearing for traceability in both modes.
        issues.extend(_traceability_issues(stripped, deps))
    if issues:
        raise IncompleteArtifactError(
            stage_type,
            issues,
            partial_content=artifact_md,
        )


async def validate_artifact_completeness_async(
    stage_type: str,
    artifact_md: str,
    deps: dict[str, str] | None = None,
    mode: str = "standard",
) -> None:
    """Async ``validate_artifact_completeness``: offloads the scan off the loop (F7).

    The completeness pass runs many per-section / per-task regex loops over the
    full artifact and is the heaviest validator on the hot generation path. Raises
    ``IncompleteArtifactError`` identically (the exception propagates through the
    executor). Large artifacts are dispatched to the dedicated CPU pool; small ones
    run inline.
    """
    from services.cpu_offload import run_cpu_bound

    await run_cpu_bound(
        artifact_md,
        validate_artifact_completeness,
        stage_type,
        artifact_md,
        deps,
        mode,
    )


def _required_headings(
    stage_type: str, deps: dict[str, str], mode: str = "standard"
) -> list[str]:
    required = list(section_contract(stage_type, mode))
    if mode == "demo_day":
        return required
    upstream = " ".join(deps.values())
    for sentinel, heading in _CONDITIONAL_SECTIONS.get(stage_type, []):
        if sentinel.search(upstream):
            required.append(heading)
    return required


def _conditional_headings_for_stage(stage_type: str) -> frozenset[str]:
    """The set of headings that are CONDITIONAL (sentinel-gated) for a stage.

    A conditional section may legitimately be answered with the prompt-blessed
    one-line ``Not applicable because <reason>`` declaration when its surface is
    out of scope (e.g. Frontend Architecture on a backend/CLI plan). The depth
    floor must exempt that blessed body for these headings only — never for the
    unconditional sections, which always require substantive content.
    """
    return frozenset(
        heading for _, heading in _CONDITIONAL_SECTIONS.get(stage_type, [])
    )


# Matches the prompt-blessed "Not applicable because <reason>" body a conditional
# section is explicitly authorised to carry when its surface is out of scope
# (prompts/plan.py — Frontend Architecture on a backend-only project). Anchored
# to the start of the *raw* section body so a section that merely mentions the
# phrase deeper in real prose is not mistaken for an out-of-scope declaration.
_NOT_APPLICABLE_RE = re.compile(r"^\s*not\s+applicable\b", re.IGNORECASE)


def _is_not_applicable_body(body: str) -> bool:
    return bool(_NOT_APPLICABLE_RE.match(body))


# Sections whose contract is a FIXED FORMAT — a manifest of paths, a lookup
# table, a dependency graph — not prose. Grading them with the prose depth floor
# is category-wrong and fires a guaranteed false `shallow_required_section` on
# exactly the artifacts that are RIGHT, because the correct rendering of each is
# short by construction:
#
#   * a correct 3-file Demo Day tree normalises to ~71 chars against a 90 floor;
#   * the canonical 3-row Task Sizing Legend
#     (`| Size | Effort |` / `| S | < 2h |` / …) normalises to 33 chars against
#     a 50 floor — and adding a "Meaning" column pushes it to 124, so the
#     advisory fired on the TIGHTER artifact;
#   * a small Dependency Graph is a handful of `T-001 --> T-002` edges.
#
# Each grader measures the structure the section is actually for. A section that
# carries NO structure of the expected kind falls through to the prose floor
# rather than passing, so this can never widen into an escape hatch — that is
# the whole reason these are graders and not an exemption list.
_ENUMERATED_ITEM_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+\S")
_TABLE_SEPARATOR_RE = re.compile(r"^[\s|:\-]+$")


def _table_data_rows(body: str) -> int:
    """Markdown table rows that carry data (header and rule excluded)."""
    pipe_rows = 0
    separators = 0
    for raw in body.splitlines():
        line = raw.strip()
        if not line.startswith("|"):
            continue
        pipe_rows += 1
        if _TABLE_SEPARATOR_RE.match(line):
            separators += 1
    if not pipe_rows:
        return 0
    # One header row per separator rule; a table with no rule is all data.
    return max(0, pipe_rows - separators - (1 if separators else 0))


def _enumerated_rows(body: str) -> int:
    """Rows of a fixed-format enumeration, however it is rendered.

    A legend is equally correct as a markdown table or as a bullet list, so the
    measure is the larger of the two counts. Prose scores zero and falls through
    to the depth floor.
    """
    bullets = sum(1 for raw in body.splitlines() if _ENUMERATED_ITEM_RE.match(raw))
    return max(_table_data_rows(body), bullets)


# Matches only the OPENING fence, not a closing ``` too, so a chunk killed by
# the 240s provider-call watchdog mid-diagram (partial output) still reads as
# "has a diagram" rather than "doesn't" -- the same reasoning that keeps the
# harness Files-chunk gates from punishing a truncated-but-real generation.
_MERMAID_FENCE_OPEN_RE = re.compile(r"```mermaid\b", re.IGNORECASE)
_FENCED_BLOCK_RE = re.compile(r"```.*?```", re.DOTALL)
_ASCII_DIAGRAM_CHAR_RE = re.compile(r"[│─├└┃┏┗┣┓┛┫╰╯╭╮]")
# Deliberately only the two-character arrow (`-->`/`<--`), not a bare `->`/`<-`:
# a single hyphen-arrow appears in ordinary prose ("client -> API -> DB") and
# would false-pass a section that never drew a diagram at all. A false PASS is
# worse than a false gap here (this grader has no prose_fallback escape hatch).
_STRICT_ARROW_RE = re.compile(r"-->|<--")


def _architecture_diagram_signal(body: str) -> int:
    """1 if ``body`` contains a plausible Mermaid or ASCII diagram, else 0.

    A Mermaid fence is unambiguous evidence on its own. Otherwise, box-drawing
    glyphs are a strong enough signal alone (>=4 of them); a plain-ASCII
    diagram with no box-drawing glyphs is only credited when its arrows sit
    inside a fenced code block, or are corroborated by box-drawing glyphs
    across >=2 distinct lines -- arrows outside a fence with no box-drawing
    support are exactly the ordinary-prose case this must not false-pass.
    """
    if _MERMAID_FENCE_OPEN_RE.search(body):
        return 1
    box_drawing_hits = len(_ASCII_DIAGRAM_CHAR_RE.findall(body))
    if box_drawing_hits >= 4:
        return 1
    for fence in _FENCED_BLOCK_RE.finditer(body):
        if len(_STRICT_ARROW_RE.findall(fence.group(0))) >= 2:
            return 1
    if box_drawing_hits >= 1:
        arrow_lines = {
            i
            for i, line in enumerate(body.splitlines())
            if _STRICT_ARROW_RE.search(line)
        }
        if len(arrow_lines) >= 2:
            return 1
    return 0


@dataclass(frozen=True)
class _StructuralGrader:
    measure: Callable[[str], int]
    minimum: int
    detail: str
    # Whether a section carrying none of the expected structure may still pass on
    # prose length. False only where the structure IS the contract: a File Tree
    # that names no paths is definitionally broken, however well it reads.
    prose_fallback: bool


_STRUCTURAL_SECTIONS: dict[str, _StructuralGrader] = {
    "## File Tree": _StructuralGrader(
        measure=lambda body: len(_file_tree_paths(body)),
        minimum=1,
        detail="does not name any file paths",
        prose_fallback=False,
    ),
    # Heading is byte-identical in SECTION_CONTRACTS["plan"] and
    # DEMO_DAY_SECTION_CONTRACTS["plan"], so this one entry covers both modes.
    # prose_fallback=False mirrors File Tree: both prompts now explicitly
    # mandate the Mermaid/ASCII format, so the structure IS the contract here.
    "## Architecture Overview": _StructuralGrader(
        measure=_architecture_diagram_signal,
        minimum=1,
        detail="does not contain a Mermaid or ASCII architecture diagram",
        prose_fallback=False,
    ),
    "## Task Sizing Legend": _StructuralGrader(
        measure=_enumerated_rows,
        minimum=3,
        detail="does not define at least three task sizes",
        prose_fallback=True,
    ),
    "## Dependency Graph": _StructuralGrader(
        measure=lambda body: len(set(_TASK_DEP_RE.findall(body))),
        minimum=2,
        detail="does not name at least two task identifiers",
        prose_fallback=True,
    ),
}


def _structural_section_issue(
    heading: str, body: str, stage_type: str, mode: str
) -> CompletenessIssue | None:
    """Grade a fixed-format section by its structure, never by prose length."""
    grader = _STRUCTURAL_SECTIONS[heading]
    if grader.measure(body) >= grader.minimum:
        return None
    if grader.prose_fallback and len(
        _normalise_body_for_depth(body)
    ) >= _min_body_chars(stage_type, mode):
        return None
    return CompletenessIssue(
        code="shallow_required_section",
        detail=f"{heading} {grader.detail}.",
        reference=heading,
    )


def _section_body_issues(
    stage_type: str,
    artifact_md: str,
    deps: dict[str, str],
    mode: str = "standard",
) -> list[CompletenessIssue]:
    issues: list[CompletenessIssue] = []
    conditional = _conditional_headings_for_stage(stage_type)
    for heading in _required_headings(stage_type, deps, mode):
        body = _section_body(artifact_md, heading)
        if heading in _STRUCTURAL_SECTIONS:
            structural_issue = _structural_section_issue(
                heading, body, stage_type, mode
            )
            if structural_issue is not None:
                issues.append(structural_issue)
            continue
        # A conditional section answered with the blessed "Not applicable …"
        # one-liner is valid even though it is well under the depth floor — the
        # prompt explicitly authorises it for an out-of-scope surface. Honour
        # that contract instead of firing a false shallow advisory (audit
        # finding #2). The exemption is scoped to conditional headings so an
        # unconditional section can never escape the floor by declaring itself
        # not applicable.
        if heading in conditional and _is_not_applicable_body(body):
            continue
        body_text = _normalise_body_for_depth(body)
        if len(body_text) < _min_body_chars(stage_type, mode):
            issues.append(
                CompletenessIssue(
                    code="shallow_required_section",
                    detail=f"{heading} does not contain substantive content.",
                    reference=heading,
                )
            )
    return issues


def _section_body(artifact_md: str, heading: str) -> str:
    """Body text under ``heading``, up to the next same-or-higher-level heading.

    Tolerant of the heading being emitted at any level from two to six hashes:
    ``validate_sections`` gates on a plain substring, so a model that renders
    ``### Requirement-to-Test Matrix`` (three hashes) — or even ``####`` — passes
    the section gate, but the old exact-``##`` body regex then returned ``""``,
    silently disabling ``uncovered_requirements`` and reporting a fully-covered
    harness. Note that ``## X`` is a substring of ``#### X`` too, so an H4 heading
    passed the gate while a ``#{2,3}`` body match still missed it — the same
    silent-disable one level deeper. Matching at ``#{2,6}`` and terminating at the
    next same-or-shallower heading closes that false-negative for every level
    without swallowing the ``### File:`` subsections of a legitimate ``## Files``
    section. When the same title appears at multiple levels, the SHALLOWEST
    (then earliest) is chosen — the real section, not an incidental deeper echo.

    The terminator deliberately starts at TWO hashes (``#{2,level}``), never one:
    a single ``#`` at column 0 is overwhelmingly a code comment (``# Tests:
    FR-001``) inside a ``## Files`` code block, not an H1 heading, and this
    function is fence-unaware. Stopping at it would truncate the Files body mid
    code block — so for the common H2 section this is byte-identical to the old
    "next ``##``" behaviour, and a deeper section additionally stops at the next
    same-or-shallower heading.

    A trailing PARENTHETICAL is tolerated as a second pass. Several prompts name
    their own sections with one — ``## Threat Model (STRIDE)``, ``## Failure Mode
    and Effects Analysis (FMEA-lite)``, ``## Architecture Anti-Patterns
    (explicitly avoid)``, ``## Frontend Architecture (if applicable)`` — while
    the contract entry is the bare title. ``validate_sections`` gates on a plain
    substring so both spellings pass presence, but the exact-line regex above
    then returned ``""`` for the parenthesised form, which reads as an empty
    section and fires a false ``shallow_required_section`` advisory on every plan
    that copies the prompt's own heading (the same defect audit finding #1 fixed
    for the Quality Attribute Matrix, four headings later). The fallback is
    deliberately narrow — a single balanced ``(...)`` and nothing else — so it
    cannot swallow a genuinely different section whose title merely starts with
    the same words.
    """
    title = re.escape(heading.lstrip("#").strip())
    start = re.compile(rf"^(#{{2,6}})[ \t]+{title}[ \t]*$", re.MULTILINE)
    matches = list(start.finditer(artifact_md))
    if not matches:
        parenthesised = re.compile(
            rf"^(#{{2,6}})[ \t]+{title}[ \t]*\([^)\n]*\)[ \t]*$", re.MULTILINE
        )
        matches = list(parenthesised.finditer(artifact_md))
    if not matches:
        return ""
    match = min(matches, key=lambda m: (len(m.group(1)), m.start()))
    level = len(match.group(1))
    rest = artifact_md[match.end() :]
    terminator = re.compile(rf"^#{{2,{level}}}[ \t]+\S", re.MULTILINE)
    end = terminator.search(rest)
    body = rest[: end.start()] if end else rest
    return body.strip()


def _normalise_body_for_depth(body: str) -> str:
    # Strip only the fence *markers* (``` and any language tag) and keep the
    # fenced body — a Mermaid/ASCII diagram or code block is real, measurable
    # substance.  Sections like "## User Flows" and "## System Context" are
    # prompted to include a diagram in a fenced block; discarding the whole
    # block made every such section read as empty and trip a spurious shallow
    # finding (the refund bleed this fix targets).
    body = re.sub(r"(?m)^[ \t]*```[^\n]*$", " ", body)
    # Drop markdown table rules/pipes but keep cell text.
    body = re.sub(r"\|?-+\|[-|\s]*", " ", body)
    body = re.sub(r"[*_`>#\[\]()|-]+", " ", body)
    body = re.sub(r"\b(?:TODO|TBD|placeholder|lorem ipsum)\b", " ", body, flags=re.I)
    return " ".join(body.split())


def _min_body_chars(stage_type: str, mode: str = "standard") -> int:
    """Minimum normalised body characters for a required section to count as
    substantive.

    These floors are deliberately well above "a heading restated as one short
    clause" — the depth failure mode where a token-squeezed model emits every
    required heading with a single throwaway sentence under each.  They stay
    below the size of a genuinely minimal-but-real section so terse legitimate
    artifacts (e.g. a focused Out of Scope list) still pass.

    Demo Day uses a HIGHER floor (not the same as standard, as the v1 plan §6.5
    assumed): the Demo Day section set is lean on breadth, so each retained
    section must carry implementation-grade DEPTH or it gives the coding agent no
    direction. The v1 "shared, low" floor is exactly what let cheap-tier Demo Day
    generations pass with one-liner sections; a mode-scoped floor surfaces a thin
    section as an advisory (non-blocking — issue #34 stance) without touching the
    standard contract (the §4 regression pin).
    """
    if mode == "demo_day":
        return {
            "spec": 160,
            "plan": 180,
            "harness": 90,
            "tasks": 80,
        }.get(stage_type, 80)
    return {
        "spec": 120,
        "plan": 150,
        "harness": 60,
        "tasks": 50,
    }.get(stage_type, 50)


def _markdown_shape_issues(artifact_md: str) -> list[CompletenessIssue]:
    issues: list[CompletenessIssue] = []
    if artifact_md.count("```") % 2 != 0:
        issues.append(
            CompletenessIssue(
                code="unbalanced_code_fence",
                detail="Markdown code fences are not balanced.",
            )
        )
    final_line = next(
        (line.strip() for line in reversed(artifact_md.splitlines()) if line.strip()),
        "",
    )
    if (
        final_line
        and _INCOMPLETE_TRAILING_RE.search(final_line)
        and not _ends_with_complete_table_row(final_line)
    ):
        issues.append(
            CompletenessIssue(
                code="dangling_trailing_line",
                detail=(
                    "The artifact appears to end mid-table, mid-list, or " "mid-clause."
                ),
            )
        )
    return issues


# Minimum distinct requirement/acceptance IDs a spec must carry.  Degenerate
# floors only: any real product spec clears these easily, but a token-starved
# generation that compresses requirements into prose (no stable IDs, or two
# token FRs) is caught and routed into the repair pass instead of flowing
# downstream where plan/harness/tasks traceability would silently degrade.
_SPEC_MIN_ID_FLOORS: tuple[tuple[str, int], ...] = (
    ("FR", 5),
    ("NFR", 3),
    ("AC", 3),
)


def _spec_issues(artifact_md: str) -> list[CompletenessIssue]:
    issues: list[CompletenessIssue] = []
    for heading in [
        "## Functional Requirements",
        "## Non-Functional Requirements",
        "## Security, Privacy, and Abuse Expectations",
        "## Acceptance Criteria",
    ]:
        body = _section_body(artifact_md, heading)
        if not _table_or_block_has_evidence(body):
            issues.append(
                CompletenessIssue(
                    code="missing_evidence_contract",
                    detail=(
                        f"{heading} must include an Evidence column or explicit "
                        "evidence lines so generated outputs are objectively "
                        "verifiable."
                    ),
                    reference=heading,
                )
            )
    for prefix, minimum in _SPEC_MIN_ID_FLOORS:
        distinct = set(re.findall(rf"\b{prefix}-\d{{3}}\b", artifact_md))
        if len(distinct) < minimum:
            issues.append(
                CompletenessIssue(
                    code="insufficient_requirement_ids",
                    detail=(
                        f"SPEC must define at least {minimum} distinct "
                        f"{prefix}-NNN identifiers; found {len(distinct)}. "
                        "Shallow or compressed requirement coverage breaks "
                        "downstream traceability."
                    ),
                    reference=prefix,
                )
            )
    return issues


def _table_or_block_has_evidence(body: str) -> bool:
    lower = body.lower()
    if "| evidence" in lower or "| verification" in lower:
        return True
    return bool(re.search(r"(?im)^\s*[-*]?\s*(evidence|verification):\s+\S", body))


def _plan_issues(artifact_md: str, deps: dict[str, str]) -> list[CompletenessIssue]:
    issues: list[CompletenessIssue] = []
    upstream_ids = _upstream_requirement_ids(deps) | _upstream_acceptance_ids(deps)
    if upstream_ids:
        rtm = _section_body(artifact_md, "## Requirement Traceability Matrix")
        present = set(_REQUIREMENT_ID_RE.findall(rtm))
        present.update(_AC_ID_RE.findall(rtm))
        missing = sorted(upstream_ids - present)
        if missing:
            issues.append(
                CompletenessIssue(
                    code="rtm_missing_upstream_id",
                    detail=(
                        "PLAN Requirement Traceability Matrix is missing upstream "
                        f"requirement or acceptance IDs: {', '.join(missing[:10])}."
                    ),
                    reference=", ".join(missing[:10]),
                )
            )
    return issues


# A ``### File:`` heading — tolerant of the two- or three-hash form. The core
# harness emits ``### File:``; a merged gap-patch historically emitted ``## File:``
# (two hashes, see prompts/harness_patch.py). Reading both here means a patched
# workspace's coverage heals with no content migration (a patched file is finally
# counted as emitted), and the three file-heading parsers across the codebase
# (this module, online_eval, stage_manager._merge_harness_patch) agree on shape.
_FILE_HEADING_RE = re.compile(r"^#{2,3}\s+File:\s+(.+?)\s*$", re.MULTILINE)
# A backticked matrix cell that names a test file: a path with a directory
# component and a file extension whose stem/path reads as a test (``test``,
# ``spec``, or a ``tests/`` directory). Language-agnostic on purpose — the
# previous matrix check keyed on the Python ``test_`` prefix / ``def test_`` and
# silently no-opped on TS/Vitest, Go, Ruby, etc. harnesses. An optional
# ``::test``/``::Class::method`` suffix is allowed before the closing backtick so
# a file+test cell (`` `x_test.py::test_foo` ``) and the file token of a mixed
# cell both decompose to the file path (`_canonical_test_path` drops the suffix)
# instead of failing the match entirely and dropping the file.
_MATRIX_TEST_FILE_RE = re.compile(r"`([^`]+?\.[A-Za-z0-9]+)(?:::[^`]+)?`")


def _canonical_test_path(token: str) -> str:
    """Canonical form of a harness file path, for MATCHING only (never display).

    The single source of truth for "do these two strings name the same harness
    file". Shared by :func:`uncovered_requirements`, :func:`_harness_issues`, and
    the online-eval ref matchers so a Matrix cell, a ``### File:`` heading, a
    File-Tree entry, and a TASKS ``Harness refs`` token that all mean the same
    file compare equal — the divergent per-call-site normalisations were a
    standing false-gap factory (a case difference, a ``./`` prefix, or a
    ``file::test`` cell each manufactured a phantom "missing coverage").

    Takes the path part before any ``::`` test/class suffix, normalises
    backslashes, strips a leading ``./`` / ``/`` / ``harness/``, drops
    surrounding backticks and whitespace, and casefolds. Case folding is safe:
    two harness files differing only in case cannot coexist on a
    case-insensitive filesystem, and every user-facing display string keeps its
    original casing (this form is used solely for set membership).
    """
    cleaned = token.strip().strip("`").strip().replace("\\", "/")
    cleaned = cleaned.split("::", 1)[0].strip()
    # Casefold BEFORE stripping prefixes so a capitalised ``Harness/`` / ``./``
    # is stripped identically to its lowercase form (the strips below are
    # literal, case-sensitive comparisons).
    cleaned = cleaned.casefold()
    while cleaned.startswith("./"):
        cleaned = cleaned[2:]
    cleaned = cleaned.lstrip("/")
    if cleaned.startswith("harness/"):
        cleaned = cleaned[len("harness/") :]
    return cleaned


def _normalise_harness_path(path: str) -> str:
    """Strip a leading ``harness/`` segment and surrounding whitespace."""
    cleaned = path.strip().replace("\\", "/")
    if cleaned.startswith("harness/"):
        cleaned = cleaned[len("harness/") :]
    return cleaned


# ---------------------------------------------------------------------------
# Multi-language harness test index.
#
# ONE scanner, every consumer: the online-eval structural task validator, the
# construction verifiers (``standard_plan_linter``), and the completeness gate's
# ``_task_harness_ref_issues``. It used to live privately in
# ``services/evals/online_eval.py`` while the verifier joined on a Python-only
# ``_harness_test_refs`` — so a Vitest/Go/RSpec harness parsed as ZERO tests and
# C2 ``test_coverage`` hard-failed every non-Python package. Moving it here (a
# pure, dependency-free module every caller can import) is what keeps the join
# keys from drifting again; ``online_eval`` imports these names and keeps its
# private aliases, so its behaviour is byte-identical. The last Python-only
# holdout (``_task_harness_ref_issues``) was migrated with it and the duplicate
# scanner/matcher pair deleted.
# ---------------------------------------------------------------------------
_HARNESS_FILE_HEADING_RE = re.compile(r"^#{2,3}\s+File:\s+(.+)$")
# H2 section heading (## Overview, ## Files, …). Ends the current ### File:
# block's scope in the ref scanner. `## File:` is a file heading, matched by
# _HARNESS_FILE_HEADING_RE first, so it never reaches this.
_SECTION_H2_RE = re.compile(r"^##\s+\S")
# CommonMark-ish fenced-code opener: >=3 backticks after <=3 leading spaces, then
# an info string that (per spec) may not itself contain a backtick. The previous
# `[a-zA-Z0-9]*$` rejected every real info string beyond a bare lang tag —
# ```ts title="x.ts"```, ```python {.line-numbers}```, ```c++```, ```objective-c```
# — so that file's tests went entirely unparsed and every TASKS ref into it
# became a phantom GENUINE_GAP (issue: false task coverage gaps).
# A fence opener is indented at most 3 SPACES (CommonMark). Spaces only, not
# ``\s``: a leading tab is 4 columns (tab stop), so a tab-indented run of
# backticks is content, never a fence delimiter — mirrors the tab guard on the
# close path in `_harness_ref_index` (Fable verify #5).
# TILDE fences (``~~~``) are accepted alongside backticks: CommonMark treats them
# as equivalent, and this scanner's ``files_with_body`` is now the single
# definition of "this promised file was actually emitted" (see
# ``_emitted_file_index``). A tilde-fenced file read as BODYLESS would therefore
# manufacture a coverage gap on a correct harness — a false alarm on exactly the
# artifacts that are right. The info string keeps the conservative ``[^`]*``
# bound for both delimiters.
_HARNESS_FENCE_OPEN_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})([^`]*)$")
_CLASS_DEF_RE = re.compile(r"^\s*class\s+(Test\w+)")
# Python test functions (sync or async).
_TEST_FUNC_DEF_RE = re.compile(r"^\s*(?:async\s+)?def\s+(test_\w+)")
# Go: `func TestXxx(t *testing.T)` — the harness prompt's stack table promises a
# Go/`go test` layout, but the extractor had no Go support, so a Go harness was
# ~100% false GENUINE_GAPs.
_GO_TEST_FUNC_RE = re.compile(r"^\s*func\s+(Test[A-Za-z0-9_]*)\s*\(")
# Go testify suites: `func (s *AuthSuite) TestRefresh(...)`. The receiver group
# sits between `func` and the method name, so the plain `_GO_TEST_FUNC_RE` above
# (which needs `func` immediately followed by `Test`) never matched it — a
# testify-only Go file parsed as zero tests, so every ref into it read as a gap.
_GO_RECEIVER_TEST_RE = re.compile(r"^\s*func\s+\([^)]*\)\s+(Test[A-Za-z0-9_]*)\s*\(")
# TypeScript / JavaScript test runners (Vitest / Jest / Mocha). The harness
# generator emits `it(...)` / `test(...)` blocks for any plan that declares a
# frontend, and TASKS references those names — so they must be extracted as
# matchable refs or every TS-test reference false-positives as a coverage gap.
# `describe(...)` is the class analog used for `file::describe::test` refs.
_TS_TEST_DEF_RE = re.compile(
    r"""^\s*(?:it|test)(?:\.\w+)?\s*\(\s*['"`]([^'"`]+)['"`]"""
)
# Parametrised `it.each([...])("name", …)` / `test.each(...)("name")` — the data
# array sits between two paren groups, so the plain runner regex above never
# matched the name.
_TS_EACH_RE = re.compile(r"""^\s*(?:it|test)\.each\b.*?\)\s*\(\s*['"`]([^'"`]+)['"`]""")
_TS_DESCRIBE_DEF_RE = re.compile(
    r"""^\s*describe(?:\.\w+)?\s*\(\s*['"`]([^'"`]+)['"`]"""
)
# RSpec / Cucumber-style `it "name" do` / `scenario "name" do` (no parentheses),
# and the `describe "X" do` / `context "X" do` grouping analog. Also promised by
# the harness prompt's stack table (Ruby / RSpec) but previously unmatched.
_RSPEC_TEST_RE = re.compile(
    r"""^\s*(?:it|specify|scenario|example)\s+['"]([^'"]+)['"]"""
)
_RSPEC_DESCRIBE_RE = re.compile(
    r"""^\s*(?:describe|context|feature)\s+['"]([^'"]+)['"]"""
)

# Extensions whose test shapes we parse COMPLETELY, so "file exists, zero tests
# parsed" is positive evidence of absence (a genuine gap), not parser blindness.
# Python: `def test_*` / `async def test_*` / `class Test*` cover pytest AND
# unittest, so a .py file with none of them genuinely has no test. Other
# ecosystems (Go testify variants, RSpec vs Minitest, TS custom wrappers) have
# common shapes we may miss, so a bodied non-.py file stays UNVERIFIED.
RELIABLY_PARSED_TEST_EXTS: tuple[str, ...] = (".py",)


@dataclass
class HarnessTestIndex:
    """Everything the ref scanner learns from a harness, in one pass.

    * ``known_refs`` — matchable identifiers a TASKS ``Harness refs`` token can
      resolve against: bare test names, ``file::test``, ``Class::method``,
      ``file::Class::method``, and every ``### File:`` path.
    * ``known_files`` — canonical path of every ``### File:`` heading. Authoritative
      proof the file exists, independent of whether we could parse its tests.
    * ``files_with_tests`` — canonical path of every file we extracted >=1 test
      identifier from. The gap between ``known_files`` and this set is where the
      parser is blind, so a ref into that gap is parser-uncertainty, not a hole.
    * ``files_with_body`` — canonical path of every file that opened at least one
      fenced code block. A ``### File:`` heading NOT in this set was promised but
      left completely empty (no code at all) — the strongest, language-agnostic
      evidence of a genuine gap, so it is never demoted to the quiet "unverified"
      class.
    """

    known_refs: set[str] = field(default_factory=set)
    known_files: set[str] = field(default_factory=set)
    files_with_tests: set[str] = field(default_factory=set)
    files_with_body: set[str] = field(default_factory=set)
    test_names: set[str] = field(default_factory=set)
    tests_in_file: dict[str, set[str]] = field(default_factory=dict)

    def unparsed_test_files(self) -> set[str]:
        """Files that carry real code but yielded no test we could name.

        A file with a body and zero parsed tests is EITHER genuinely test-free
        OR written in a shape this scanner does not model. Only ``.py`` is
        parsed completely enough to call the first (``RELIABLY_PARSED_TEST_EXTS``),
        so every other extension lands here and callers must degrade to
        "unverified" rather than assert a gap.
        """
        return {
            path
            for path in self.files_with_body - self.files_with_tests
            if not path.endswith(RELIABLY_PARSED_TEST_EXTS)
        }


def harness_test_index(harness_content: str) -> HarnessTestIndex:
    """Scan a harness once into a :class:`HarnessTestIndex` (span-based).

    Robust to the two markdown realities the old cursor-based scanner tripped on:
      1. Any prose/comment line between a ``### File:`` heading and its fence
         (the fence no longer has to *immediately* follow the heading), and
      2. fenced code opened with an info string (```ts title="x"```), which the
         old open-fence regex rejected.
    Both made a whole file's tests unparsed, turning every TASKS ref into it into
    a phantom GENUINE_GAP. Here a file's scope runs from its ``### File:`` heading
    to the next file heading or ``## `` section; every fenced region inside that
    scope is scanned, in Python/pytest, Go, TS/JS (Vitest/Jest/Mocha, incl.
    ``.each``), and RSpec conventions.
    """
    index = HarnessTestIndex()
    file_norm: str | None = None
    file_canon: str | None = None
    in_fence = False
    fence_len = 0
    fence_char = "`"
    current_class: str | None = None
    current_describe: str | None = None

    def _register(
        name: str, scope: str | None, scope_qualified: bool, *, is_test: bool = True
    ) -> None:
        index.known_refs.add(name)
        # ``is_test`` separates real test CASES from the grouping constructs
        # (a ``class TestX`` / ``describe("…")`` name is registered as a
        # matchable ref, but it is not itself a test). Only the former feed
        # ``test_names``/``tests_in_file``, which the construction verifier
        # uses as its coverage denominator — counting a describe() block as a
        # test would demand a task cite the group AND each of its cases.
        if is_test:
            index.test_names.add(name)
            if file_canon:
                index.tests_in_file.setdefault(file_canon, set()).add(name)
        if file_norm:
            index.known_refs.add(f"{file_norm}::{name}")
        if scope and scope_qualified:
            index.known_refs.add(f"{scope}::{name}")
            if file_norm:
                index.known_refs.add(f"{file_norm}::{scope}::{name}")
        if file_canon:
            index.files_with_tests.add(file_canon)

    for raw in harness_content.split("\n"):
        line = raw.rstrip("\r")
        heading_m = _HARNESS_FILE_HEADING_RE.match(line)
        if heading_m:
            # A ### File: heading at column 0 always starts a new file and closes
            # any dangling (e.g. truncated, unbalanced) fence — this bounds a
            # runaway scan to a single file instead of the rest of the document.
            in_fence = False
            fence_len = 0
            file_norm = _normalise_harness_ref(heading_m.group(1))
            file_canon = _canonical_test_path(file_norm) if file_norm else None
            if file_norm:
                index.known_refs.add(file_norm)
            if file_canon:
                index.known_files.add(file_canon)
                # Register the CANONICAL path too so a file-only TASKS ref that
                # differs only by a ./ prefix, a leading /, case, or a harness/
                # prefix still resolves (via _ref_matches_harness's canonical
                # fallback) instead of manufacturing a phantom GENUINE_GAP.
                index.known_refs.add(file_canon)
            current_class = None
            current_describe = None
            continue
        if not in_fence:
            if _SECTION_H2_RE.match(line):
                file_norm = file_canon = None
                current_class = current_describe = None
                continue
            fence_m = _HARNESS_FENCE_OPEN_RE.match(line)
            if fence_m:
                in_fence = True
                fence_len = len(fence_m.group(1))
                # A fence closes only on its OWN delimiter (CommonMark): a ``` run
                # inside a ~~~ block is content.
                fence_char = fence_m.group(1)[0]
                # NOTE: files_with_body is recorded on the first non-blank line
                # INSIDE the fence (below), not here at the opener. An empty
                # fenced block (```lang immediately followed by the closing ```)
                # is positive evidence the promised file has no body — exactly
                # the strongest gap signal `_classify_unmatched_ref` keys on — so
                # it must stay OUT of files_with_body and classify GENUINE_GAP,
                # not the reassuring UNVERIFIED (Fable verify #2).
            continue
        # Inside a fenced block. Close on a line of only backticks at least as
        # long as the opener (CommonMark), not an exact-length match — AND
        # indented no more than 3 spaces, the same bound `_HARNESS_FENCE_OPEN_RE`
        # puts on the opener. A more-indented run of backticks is fence CONTENT,
        # not a close (e.g. a ``` inside a triple-quoted string a test file
        # embeds to assert markdown rendering): closing on it truncated the scan
        # and turned every test defined after it into a phantom GENUINE_GAP.
        lstripped = line.lstrip()
        leading = len(line) - len(lstripped)
        stripped = lstripped.rstrip()
        is_close = (
            leading <= 3
            # A tab in the leading whitespace is 4 columns (CommonMark tab stop),
            # so a tab-indented run of backticks is ≥4 cols of indent — fence
            # CONTENT, never a closer. `leading` counts a tab as one char, so
            # without this a tab-indented ``` (Go's convention inside a raw
            # string embedding markdown) falsely closed the block and turned
            # every test defined after it into a phantom GENUINE_GAP (Fable
            # verify #5).
            and "\t" not in line[:leading]
            and bool(stripped)
            and stripped.count(fence_char) == len(stripped)
            and len(stripped) >= fence_len
        )
        if is_close:
            in_fence = False
            fence_len = 0
            continue
        # First non-blank line inside the fence proves the promised file has a
        # body (see the opener note above). Idempotent set add.
        if file_canon and stripped:
            index.files_with_body.add(file_canon)
        if file_norm is None:
            # A fence outside any ### File: block (e.g. the File Tree code block).
            continue
        indented = line[:1].isspace()
        cls_m = _CLASS_DEF_RE.match(line)
        if cls_m:
            current_class = cls_m.group(1)
            # Register the class name itself — a TASKS ref may point at the whole
            # test class (`file::TestLogin`), not one method. Without this that
            # ref would read as a phantom GENUINE_GAP even though the class exists.
            _register(current_class, None, False, is_test=False)
        elif line and not indented:
            current_class = None
        func_m = _TEST_FUNC_DEF_RE.match(line)
        if func_m:
            _register(func_m.group(1), current_class, bool(current_class) and indented)
        go_m = _GO_TEST_FUNC_RE.match(line) or _GO_RECEIVER_TEST_RE.match(line)
        if go_m:
            _register(go_m.group(1), None, False)
        # describe()/context "…" is the grouping (class) analog; individual cases
        # are it()/test()/scenario. Brace/`do` nesting is not tracked, so describe
        # attribution is best-effort — the bare and file-qualified names always
        # match regardless.
        desc_m = _TS_DESCRIBE_DEF_RE.match(line) or _RSPEC_DESCRIBE_RE.match(line)
        if desc_m:
            current_describe = desc_m.group(1)
            # Register the group name too — a ref to the describe/context block
            # itself (`file::AuthFlow`) must resolve, not read as a gap.
            _register(current_describe, None, False, is_test=False)
        ts_m = (
            _TS_TEST_DEF_RE.match(line)
            or _TS_EACH_RE.match(line)
            or _RSPEC_TEST_RE.match(line)
        )
        if ts_m:
            _register(ts_m.group(1), current_describe, bool(current_describe))

    return index


def _normalise_harness_ref(ref: str) -> str:
    """Canonicalise a task/harness reference without weakening path identity."""
    normalized = ref.strip().strip("`").replace("\\", "/")
    if normalized.startswith("harness/"):
        normalized = normalized[len("harness/") :]
    # Generated traceability tables often render ``path :: test`` while task
    # fields use ``path::test``. Whitespace around the structural delimiter has
    # no semantic meaning and must not manufacture a gap.
    return "::".join(part.strip() for part in normalized.split("::"))


def ref_matches_harness(ref: str, known_refs: set[str]) -> bool:
    """Match file-, test-, class-, and bare-name harness references."""
    normalized = _normalise_harness_ref(ref)
    if normalized in known_refs:
        return True
    parts = normalized.split("::")
    if len(parts) >= 2 and "::".join(parts[-2:]) in known_refs:
        return True
    if bool(parts) and parts[-1] in known_refs:
        return True
    # File-ONLY canonical fallback: a whole-file ref (no `::`) that differs from
    # its `### File:` heading only by a ./ prefix, a leading /, case, or a
    # harness/ prefix still names a known file (the heading's canonical path was
    # registered in known_refs). Guarded to the no-`::` case on purpose — a
    # `file::test` ref must keep matching on the TEST identifier, never be waved
    # through just because its file exists (that would hide a genuinely missing
    # test inside a present file — the exact over-suppression we must avoid).
    if "::" not in normalized:
        canon = _canonical_test_path(normalized)
        if canon and canon in known_refs:
            return True
    return False


# Real source/test file extensions. A BARE token's extension must be one of
# these to count as a test file. Without this allowlist the ``/``-free widening
# (Fable #5/#8) admitted matrix prose tokens that merely CONTAIN ``test`` —
# ``pytest.mark.slow`` (ext ``slow``), ``latest.md`` (ext ``md``, "la-test"),
# ``pytest==7.4.0`` (ext ``0``) — each of which then armed the 10-credit patch on
# a phantom and fed the completeness gate a garbage filename
# (Fable verify #4). Generous over real languages; excludes docs/config/version
# fragments. A false negative (an exotic-extension test dropped) is the safe,
# quiet direction; a false positive is a paid alarm + junk repair.
_CODE_TEST_EXTS: frozenset[str] = frozenset(
    {
        "py",
        "ts",
        "tsx",
        "js",
        "jsx",
        "mjs",
        "cjs",
        "go",
        "rb",
        "rs",
        "java",
        "kt",
        "kts",
        "cs",
        "ex",
        "exs",
        "php",
        "swift",
        "scala",
        "sc",
        "cpp",
        "cc",
        "cxx",
        "c",
        "h",
        "hh",
        "hpp",
        "m",
        "mm",
        "dart",
        "clj",
        "cljs",
        "cljc",
        "groovy",
        "vue",
        "svelte",
        "lua",
        "jl",
        "fs",
        "fsx",
        "pl",
        "pm",
        "ml",
        "mli",
        "erl",
        "hs",
        "elm",
    }
)


def _looks_like_test_file_path(token: str) -> bool:
    """True when *token* reads as a test-file path (or bare test filename).

    Accepts both a directory-qualified path (``tests/unit/x_test.py``) and a
    BARE test-convention filename (``x_test.py``). Cheap-tier harnesses sometimes
    render bare filenames in the matrix or a nested File Tree; requiring a ``/``
    dropped those, so a genuine coverage hole showed nowhere (Fable #5/#8). A
    bare name is disambiguated
    against emitted files by BASENAME (`_file_is_emitted`), so accepting it never
    manufactures a phantom "missing" for a file that exists under a directory.

    Guards against non-file matrix prose: an ``=`` (version pin / assignment)
    disqualifies, and the file's extension must be a known code extension so
    ``pytest.mark.slow`` / ``latest.md`` / ``pytest==7.4.0`` no longer read as
    test files (Fable verify #4). A trailing ``::test`` / ``::Class::method``
    suffix is stripped before the extension check so a ``file::test`` cell still
    resolves to its file.
    """
    cleaned = token.strip().strip("`").strip()
    if not cleaned or " " in cleaned or "\t" in cleaned or "=" in cleaned:
        return False
    file_part = cleaned.split("::", 1)[0]
    last = file_part.rsplit("/", 1)[-1]
    if "." not in last:
        return False
    if last.rsplit(".", 1)[-1].lower() not in _CODE_TEST_EXTS:
        return False
    lowered = file_part.lower()
    return "test" in lowered or "spec" in lowered


def _canonical_basename(canon: str) -> str:
    """Last path segment of a canonical path — for directory-insensitive match."""
    return canon.rsplit("/", 1)[-1]


def _emitted_file_index(
    artifact_md: str, index: HarnessTestIndex | None = None
) -> tuple[set[str], set[str], set[str]]:
    """Emitted-file identity sets, for basename-safe presence checks.

    "Emitted" means the ``### File:`` heading opened a fenced code block **with
    content** — ``harness_test_index(...).files_with_body`` — never the bare
    heading. This is the single definition of the predicate, shared by
    :func:`uncovered_requirements`, :func:`harness_coverage_ratio`,
    :func:`missing_harness_files`, and :func:`_harness_issues`.

    It used to be ``_FILE_HEADING_RE`` — headings only — which is how a harness
    that listed every promised test file as a bare ``### File: tests/x.py`` with
    ZERO code passed every gate and reported 100% coverage. The block-aware
    check existed (``_FILE_BLOCK_RE``) but was reachable only from
    ``_harness_issues``, i.e. the ``else`` branch of
    :func:`validate_artifact_completeness` — so Demo Day, the guarantee-bearing
    mode, ran no file-body check at all. Keying every consumer on one
    body-aware scan closes that for both modes at once, and closes it for the
    coverage NUMBER too (a bodyless file no longer counts as covering
    anything), which the old ``_harness_issues``-only advisory never did.

    ``files_with_body`` is the deliberately weakest, most language-agnostic
    signal the scanner produces (any non-blank line inside any fence, in any
    language) — not ``files_with_tests`` — so a file whose test shape we cannot
    parse is still "emitted". A false gap here would be a paid alarm; a false
    pass is a silent hole. This picks the conservative side of each.

    Returns ``(canonical_paths, all_basenames, bare_heading_basenames)``. See
    ``_file_is_emitted`` for how the three combine to match a bare tree/matrix
    entry to a directory-qualified heading (and vice versa) without masking a
    real gap. Pass a precomputed *index* to avoid re-scanning a large harness.
    """
    scanned = index if index is not None else harness_test_index(artifact_md)
    canon = {c for c in scanned.files_with_body if c}
    all_bases = {_canonical_basename(c) for c in canon}
    bare_bases = {c for c in canon if "/" not in c}
    return canon, all_bases, bare_bases


def _file_is_emitted(canon: str, emitted: tuple[set[str], set[str], set[str]]) -> bool:
    """Whether a promised file *canon* was emitted as a ``### File:`` block.

    Matches on the full canonical path first. Falls back to a BASENAME match only
    in the safe, unambiguous directions, so a same-basename-different-directory
    collision can never mask a genuine gap:

      * a BARE promised name (no ``/``) matches any emitted file of that basename
        — the harness declined to disambiguate, so the basename is the identity;
      * a directory-qualified promised path matches only a BARE emitted heading of
        that basename (the mirror case).

    A directory-qualified promise is never matched to a different
    directory-qualified emission by basename alone — that is the collision case,
    where preserving the gap is the safe (primary-directive) choice.

    KNOWN residual (Fable verify #6, accepted): this predicate is stateless, so
    if the tree promises two same-basename files in different directories
    (``tests/unit/auth_test.py`` AND ``tests/e2e/auth_test.py``) and the harness
    emits a single BARE ``### File: auth_test.py`` heading, both promises read as
    emitted — one genuine gap is masked. It requires a double coincidence (two
    identically-named test files across directories, emitted as one un-qualified
    heading); count-aware assignment would need a stateful pass across the whole
    promised set. A well-formed harness emits directory-qualified headings, so
    the exact-path branch matches and this fallback never triggers.
    """
    canon_set, all_bases, bare_bases = emitted
    if canon in canon_set:
        return True
    base = _canonical_basename(canon)
    if "/" not in canon:
        return base in all_bases
    return base in bare_bases


def dedupe_file_blocks(artifact_md: str) -> tuple[str, int]:
    """Drop duplicate ``### File: <path>`` blocks, keeping the first of each.

    Language-agnostic self-heal for a harness whose ``## Files`` section was
    emitted more than once — the cheap-tier model looping, or a chunk merge
    concatenating overlapping output (observed: a 122 KB harness that was an
    exact doubling of a 61 KB one). Only the ``## Files`` region is touched, so
    the Overview / Matrix / Coverage Plan / File Tree are never altered. Returns
    the deduped artifact and the number of duplicate blocks removed (0 when the
    artifact has no ``## Files`` section or no duplicates — a safe no-op for
    every non-harness stage).
    """
    files_idx = artifact_md.find("\n## Files")
    if files_idx == -1:
        return artifact_md, 0
    head = artifact_md[:files_idx]
    files_region = artifact_md[files_idx:]
    # Split at each File heading; segment[0] is the "## Files" preamble.
    segments = re.split(r"(?m)(?=^#{2,3}\s+File:\s+)", files_region)
    seen: set[str] = set()
    kept: list[str] = [segments[0]]
    removed = 0
    for segment in segments[1:]:
        match = _FILE_HEADING_RE.match(segment)
        path = _normalise_harness_path(match.group(1)) if match else ""
        if path and path in seen:
            removed += 1
            continue
        if path:
            seen.add(path)
        kept.append(segment)
    if removed == 0:
        return artifact_md, 0
    return head + "".join(kept), removed


# --- Assembly-time duplicate-section guard (prompt-quality audit H1) ---------

# Per-stage headings the duplicate-section guard treats as contract sections
# even though ``validate_sections`` does not require them: they are conditional
# in the system prompt, so two chunks could still both emit them.
#
# Density initiative (2026-08-02): plan.py's "## Prompt and AI Safety
# Controls" — the section this dict used to carry — was deleted entirely (it
# was never in SECTION_CONTRACTS, never validated, never graded), so this
# dict is currently empty. Left in place as the extension point for any
# future conditional-but-not-formally-conditional plan heading.
_DEDUPE_EXTRA_HEADINGS: dict[str, list[str]] = {}

# ``## Files`` is additive and heterogeneous (### File: blocks) and has its own
# first-wins self-heal (``dedupe_file_blocks``). Dropping a second ``## Files``
# region wholesale would delete files that exist only there, so the section
# guard never touches it.
_DEDUPE_SKIP_HEADINGS = frozenset({"## Files"})

_H2_HEADING_LINE_RE = re.compile(r"^##\s+\S")
# H1 or H2 ends a section span; H3+ (### File:, ### T-NNN:) belongs to its
# section and must travel with it when a duplicate is dropped.
_SECTION_TERMINATOR_RE = re.compile(r"^#{1,2}\s+\S")
_DEDUPE_FENCE_LINE_RE = re.compile(r"^ {0,3}`{3,}")


def dedupe_contract_sections(
    stage_type: str,
    artifact_md: str,
    mode: str = "standard",
) -> tuple[str, int]:
    """Drop duplicate contract-section bodies, keeping the first of each (H1).

    Belt-and-braces backstop behind the disjoint chunk scopes: parallel chunks
    have no cross-visibility, so a scope regression (or a model ignoring its
    scope) can emit the same mandatory section twice — two *conflicting* bodies
    in one document that ``validate_sections`` (a substring check) passes
    silently. This generalises the harness ``dedupe_file_blocks`` self-heal to
    every stage's H2 section contract: when a contract heading opens a second
    time, that occurrence's span (up to the next H1/H2 heading) is dropped.
    First-wins, deterministic, zero-LLM — the same idempotent, no-regression
    semantics as the file-block dedup.

    Only contract headings participate: non-contract H2s (``## Phase N``), H3s
    (``### File:``, ``### T-NNN:``), and prose are never touched. A heading is
    matched when the line *starts with* the contract heading (mirroring the
    substring semantics of ``validate_sections``), so a decorated emission like
    ``## Threat Model (STRIDE)`` still deduplicates against ``## Threat Model``.
    Fenced code is tracked so a ``## `` line inside a code block is never read
    as a heading boundary.

    Returns ``(deduped_markdown, removed_section_count)``.
    """
    candidates = {
        heading
        for heading in (
            *section_contract(stage_type, mode),
            *(heading for _, heading in _CONDITIONAL_SECTIONS.get(stage_type, [])),
            *_DEDUPE_EXTRA_HEADINGS.get(stage_type, []),
        )
        if heading not in _DEDUPE_SKIP_HEADINGS
    }
    if not candidates:
        return artifact_md, 0
    # Longest-first so an emitted heading resolves to the most specific
    # contract entry when one contract heading is a prefix of another.
    ordered = sorted(candidates, key=len, reverse=True)

    def _contract_heading(line: str) -> str | None:
        if not _H2_HEADING_LINE_RE.match(line):
            return None
        stripped = line.rstrip()
        for heading in ordered:
            if stripped == heading or stripped.startswith(
                (f"{heading} ", f"{heading}:")
            ):
                return heading
        return None

    kept: list[str] = []
    seen: set[str] = set()
    removed = 0
    in_fence = False
    dropping = False
    for line in artifact_md.split("\n"):
        if _DEDUPE_FENCE_LINE_RE.match(line):
            # Fence state is tracked even inside a dropped span so a heading
            # inside a dropped section's code block can never end the drop.
            in_fence = not in_fence
            if not dropping:
                kept.append(line)
            continue
        if not in_fence and _SECTION_TERMINATOR_RE.match(line):
            heading = _contract_heading(line)
            if heading is not None and heading in seen:
                dropping = True
                removed += 1
                continue
            if heading is not None:
                seen.add(heading)
            dropping = False
            kept.append(line)
            continue
        if not dropping:
            kept.append(line)
    if removed == 0:
        return artifact_md, 0
    return "\n".join(kept), removed


# --- Assembly-time Effort Summary reconciliation (prompt-quality audit M6) ---

_EFFORT_TASK_HEADING_RE = re.compile(r"^###\s+T-\d+:")
_EFFORT_PRIORITY_RE = re.compile(
    r"^[\s\-*]*\*\*\s*Priority\s*:?\s*\*\*\s*:?\s*(.+?)\s*$", re.IGNORECASE
)
_EFFORT_ESTIMATE_RE = re.compile(
    r"^[\s\-*]*\*\*\s*Estimate\s*:?\s*\*\*\s*:?\s*(.+?)\s*$", re.IGNORECASE
)
_EFFORT_PRIORITY_BUCKETS = ("MUST", "SHOULD", "COULD")
# Estimate buckets in the decreasing-size order the Sizes line mandates.
_EFFORT_SIZE_BUCKETS = ("XL", "L", "M", "S")
# The two countable Effort Summary lines. Anchored on their distinctive shapes
# (`Tasks: N total …` / `Sizes: …`) so an unrelated line is never rewritten;
# prefix/suffix keep the list marker and optional backtick formatting intact.
_EFFORT_TASKS_LINE_RE = re.compile(
    r"^(?P<prefix>\s*(?:[-*]\s+)?`?)Tasks:\s*\d+\s*total\b[^`\n]*(?P<suffix>`?\s*)$"
)
_EFFORT_SIZES_LINE_RE = re.compile(
    r"^(?P<prefix>\s*(?:[-*]\s+)?`?)Sizes:[^`\n]*(?P<suffix>`?\s*)$"
)


def _effort_field_value(line: str, regex: re.Pattern[str]) -> str | None:
    match = regex.match(line.rstrip())
    if not match:
        return None
    value = match.group(1).strip()
    paren = value.find("(")
    if paren > 0:
        value = value[:paren].strip()
    return value.strip("`*_ .,;").upper() or None


def reconcile_effort_summary(artifact_md: str) -> tuple[str, bool]:
    """Recompute the Effort Summary counts from the emitted task blocks (M6).

    The overview chunk emits the Effort Summary *before any task block exists*
    — and, on the parallel path, with no visibility into the block chunks at
    all — so its ``Tasks:`` and ``Sizes:`` counts are a forecast the block
    chunks never see. Rather than ask the model to satisfy an unsatisfiable
    "counts match emitted blocks exactly" contract, the counts are reconciled
    here deterministically at assembly: count the ``### T-NNN:`` blocks and
    their per-task Priority/Estimate fields, then rewrite the two count lines
    in place. The judgment lines (``Estimate range:``, ``Minimum cut:``) are
    calendar estimates, not countable facts, and are left untouched.

    Conservative by design: a count line is rewritten only when at least one
    task block parsed a valid value for it (a fully unparsable list keeps the
    model's own forecast rather than degrading it to zeros), and nothing
    outside the ``## Effort Summary`` section is ever modified. Demo Day's
    Effort Summary has neither line, so this is a natural no-op there.

    Returns ``(reconciled_markdown, changed)``.
    """
    lines = artifact_md.split("\n")

    # Locate the ## Effort Summary span (to the next H1/H2 heading).
    section_start: int | None = None
    for i, line in enumerate(lines):
        if line.rstrip() == "## Effort Summary" or line.rstrip().startswith(
            "## Effort Summary "
        ):
            section_start = i
            break
    if section_start is None:
        return artifact_md, False
    section_end = len(lines)
    for i in range(section_start + 1, len(lines)):
        if _SECTION_TERMINATOR_RE.match(lines[i]):
            section_end = i
            break

    # Count task blocks and their Priority/Estimate fields.
    heading_indices = [
        i
        for i, line in enumerate(lines)
        if _EFFORT_TASK_HEADING_RE.match(line.rstrip())
    ]
    total = len(heading_indices)
    if total == 0:
        return artifact_md, False
    priority_counts = dict.fromkeys(_EFFORT_PRIORITY_BUCKETS, 0)
    estimate_counts = dict.fromkeys(_EFFORT_SIZE_BUCKETS, 0)
    for idx, start in enumerate(heading_indices):
        end = heading_indices[idx + 1] if idx + 1 < len(heading_indices) else len(lines)
        priority: str | None = None
        estimate: str | None = None
        for line in lines[start:end]:
            if priority is None:
                priority = _effort_field_value(line, _EFFORT_PRIORITY_RE)
            if estimate is None:
                estimate = _effort_field_value(line, _EFFORT_ESTIMATE_RE)
            if priority is not None and estimate is not None:
                break
        if priority in priority_counts:
            priority_counts[priority] += 1
        if estimate in estimate_counts:
            estimate_counts[estimate] += 1

    tasks_line_body: str | None = None
    if any(priority_counts.values()):
        tasks_line_body = (
            f"Tasks: {total} total · {priority_counts['MUST']} MUST · "
            f"{priority_counts['SHOULD']} SHOULD · {priority_counts['COULD']} COULD"
        )
    sizes_line_body: str | None = None
    if any(estimate_counts.values()):
        sizes_line_body = "Sizes: " + " · ".join(
            f"{estimate_counts[bucket]}x{bucket}"
            for bucket in _EFFORT_SIZE_BUCKETS
            if estimate_counts[bucket]
        )

    changed = False
    for i in range(section_start + 1, section_end):
        if tasks_line_body is not None:
            match = _EFFORT_TASKS_LINE_RE.match(lines[i])
            if match:
                replacement = (
                    f"{match.group('prefix')}{tasks_line_body}{match.group('suffix')}"
                )
                if replacement != lines[i]:
                    lines[i] = replacement
                    changed = True
                continue
        if sizes_line_body is not None:
            match = _EFFORT_SIZES_LINE_RE.match(lines[i])
            if match:
                replacement = (
                    f"{match.group('prefix')}{sizes_line_body}{match.group('suffix')}"
                )
                if replacement != lines[i]:
                    lines[i] = replacement
                    changed = True

    if not changed:
        return artifact_md, False
    return "\n".join(lines), True


_MATRIX_REQ_ID_RE = re.compile(r"^(?:FR|NFR|SEC|AC)-\d+(?:\.\d+)*$")
# A ``# Tests: FR-001`` / ``// Tests: AC-002, FR-003`` traceability tag on the
# line above a test. The harness prompt mandates it, ``_test_has_traceability_
# comment`` already checks for it, and ``prompts/harness_patch.py`` emits it —
# the paid patch adds tagged FILES and no matrix row, so a matrix-only coverage
# computation could never register the coverage the user just paid for.
# The comment marker is REQUIRED, not optional. This is the one path that can
# INFLATE coverage, and the segment scanned here spans a file's whole
# ``### File:`` block — prose between the heading and the fence included — so a
# bare narrative line reading ``Tests: FR-001, FR-002, FR-003`` would credit
# three requirements without a line of code existing. Requiring a comment marker
# and a following identifier matches both the harness prompt's mandated
# ``# Tests: <req-id>`` form and ``_test_has_traceability_comment``.
_TESTS_TAG_RE = re.compile(
    r"^[ \t]*(?:[#*]|//|--|/\*)[ \t]*Tests:[ \t]*((?:FR|NFR|SEC|AC)-\d{3}.*)$",
    re.MULTILINE,
)


def upstream_requirement_ids(*sources: str) -> set[str]:
    """Distinct FR/NFR/SEC identifiers named across *sources*.

    The public denominator builder for :func:`harness_coverage_ratio` — callers
    pass the upstream SPEC body. Acceptance IDs are deliberately excluded: an AC
    is verified through the requirement it belongs to, and mixing the two
    inflates the denominator with rows the matrix is not asked to carry.
    """
    ids: set[str] = set()
    for source in sources:
        ids.update(_REQUIREMENT_ID_RE.findall(source or ""))
    return ids


def _matrix_requirement_files(harness_content: str) -> dict[str, set[str]]:
    """``{requirement id: canonical test-file paths}`` from the RTM, in row order.

    Conservative by construction: a row naming no parseable test file is skipped
    entirely rather than invented as a gap.
    """
    matrix = _section_body(harness_content, "## Requirement-to-Test Matrix")
    req_files: dict[str, set[str]] = {}
    if not matrix:
        return req_files
    for line in matrix.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = stripped.strip("|").split("|")
        if not cells:
            continue
        req = cells[0].strip().strip("`").strip().upper()
        if not _MATRIX_REQ_ID_RE.match(req):
            continue
        files = _matrix_cell_test_files(cells[1:])
        if not files:
            continue
        req_files.setdefault(req, set())
        req_files[req] |= files
    return req_files


def _tagged_requirement_ids(artifact_md: str, emitted_paths: set[str]) -> set[str]:
    """Requirement IDs carried by ``Tests:`` tags inside EMITTED file bodies.

    Scoped to files that actually have a body (``emitted_paths``) so a tag in a
    heading-only stub can never claim coverage. This is the second of the two
    ways a requirement can be covered — see :func:`covered_requirement_ids`.
    """
    files_idx = artifact_md.find("\n## Files")
    if files_idx == -1:
        return set()
    tagged: set[str] = set()
    segments = re.split(r"(?m)(?=^#{2,3}\s+File:\s+)", artifact_md[files_idx:])
    for segment in segments[1:]:
        heading = _FILE_HEADING_RE.match(segment)
        if heading is None:
            continue
        if _canonical_test_path(heading.group(1)) not in emitted_paths:
            continue
        for tag_body in _TESTS_TAG_RE.findall(segment):
            tagged.update(_REQUIREMENT_ID_RE.findall(tag_body))
    return tagged


def covered_requirement_ids(
    harness_content: str, index: HarnessTestIndex | None = None
) -> set[str]:
    """Requirement IDs this harness demonstrably tests.

    A requirement is covered when it has at least one **emitted-with-body** test
    file, established by EITHER a Requirement-to-Test Matrix row mapping it to
    that file, OR a ``# Tests: <id>`` traceability tag inside that file. The two
    paths matter independently: the matrix is what the generator writes, the tag
    is what the paid gap patch writes (patch files carry no matrix row).

    Single predicate behind both :func:`harness_coverage_ratio` and
    :func:`uncovered_requirements`, so the coverage chip, the CoveragePanel gap
    list, and the paid patch cannot contradict each other.
    """
    scanned = index if index is not None else harness_test_index(harness_content)
    emitted = _emitted_file_index(harness_content, scanned)
    covered = {
        req
        for req, files in _matrix_requirement_files(harness_content).items()
        if any(_file_is_emitted(path, emitted) for path in files)
    }
    covered |= _tagged_requirement_ids(harness_content, emitted[0])
    return covered


def uncovered_requirements(
    harness_content: str, *, upstream_ids: set[str] | None = None
) -> list[str]:
    """Requirement IDs with no emitted test file.

    With *upstream_ids* (the SPEC's FR/NFR/SEC set) this is the exact complement
    of :func:`harness_coverage_ratio` — a requirement the matrix never mentioned
    is a gap, not an absence of evidence. Without it, the answer is scoped to
    what the matrix itself claims, which is all a caller holding only the harness
    can honestly say.

    The returned list is the input to both the CoveragePanel and the paid harness
    patch, so it is conservative in the matrix-only mode: a requirement whose row
    names no parseable test file is never invented as a gap.
    """
    covered = covered_requirement_ids(harness_content)
    if upstream_ids:
        return sorted({req.upper() for req in upstream_ids} - covered)
    return [
        req for req in _matrix_requirement_files(harness_content) if req not in covered
    ]


def harness_coverage_ratio(
    harness_content: str, *, upstream_ids: set[str] | None = None
) -> tuple[int, int]:
    """Deterministic ``(covered, total)`` requirement coverage for a harness.

    ``total`` is the number of distinct **upstream** FR/NFR/SEC identifiers
    (``upstream_ids``, built from the SPEC by :func:`upstream_requirement_ids`);
    ``covered`` is how many of those :func:`covered_requirement_ids` proves the
    harness tests. Exactly the complement of :func:`uncovered_requirements` when
    given the same ``upstream_ids``, so the coverage number, the CoveragePanel
    gap list, and the paid patch are three views of one computation.

    The denominator used to be "requirement rows present in the matrix", which
    made the number structurally incapable of reporting the failure it existed to
    catch: when the contract chunk runs out of budget the matrix loses rows, and
    the denominator shrinks along with the numerator — a harness covering 12 of
    20 requirements reported **100%**, with an empty gap list and a paid patch
    that had nothing to patch, while the real gap surfaced only as a non-blocking
    ``insufficient_upstream_traceability`` advisory contradicting the chip beside
    it.

    Returns ``(0, 0)`` when no upstream identifiers are supplied — callers must
    treat a zero total as "unknown" and render nothing, never as 0%. There is
    deliberately NO fallback to identifiers scraped from the harness itself: a
    budget-truncated harness drops those requirements from its whole body, not
    just from the matrix, so that fallback would reproduce the exact 100% lie.

    This also replaces the judge's ``coverage_percent``, which is derived from a
    harness compacted to ~20K chars (10K on the compact retry) while a real
    harness runs 60–120KB — the same truncation poisoning that got the judge's
    ``uncovered_reqs`` pulled from the UI and excluded from the paid patch.
    """
    total_ids = {req.upper() for req in (upstream_ids or set())}
    if not total_ids:
        return 0, 0
    covered = covered_requirement_ids(harness_content)
    return len(total_ids & covered), len(total_ids)


def _matrix_cell_test_files(cells: list[str]) -> set[str]:
    """Canonical test-file paths named across a matrix row's non-ID cells.

    Scrapes every backticked token per cell (``_MATRIX_TEST_FILE_RE``) so a
    multi-file cell (`` `a_test.py`, `b_test.py` ``) and a file+test cell
    (`` `x_test.py::test_foo` ``) both decompose into their component file paths
    instead of one unmatchable blob (the whole-cell ``strip('`')`` did the
    latter). Falls back to whitespace tokens for a matrix that renders paths
    without backticks. Only tokens that read as a test file path
    (``_looks_like_test_file_path``) survive, so behaviour/type/status columns
    never contribute phantom files.
    """
    files: set[str] = set()
    for cell in cells:
        for token in _MATRIX_TEST_FILE_RE.findall(cell):
            if _looks_like_test_file_path(token):
                files.add(_canonical_test_path(token))
    if files:
        return files
    for cell in cells:
        for raw in cell.split():
            # An unbackticked path in a plain-text cell keeps its trailing list
            # punctuation (``a.py,`` ``b.py``); strip it before matching or the
            # canonical form (``a.py,``) never equals the emitted ``a.py`` and a
            # covered requirement reads as uncovered.
            token = raw.strip("`*_,;()[]")
            if _looks_like_test_file_path(token):
                files.add(_canonical_test_path(token))
    return files


def _promised_harness_files(artifact_md: str) -> dict[str, str]:
    """Every file the harness's File Tree and Matrix promise, canonical→display.

    The union of the ``## File Tree`` entries (all files: tests, fixtures,
    factories, schemas, README) and the ``## Requirement-to-Test Matrix`` test
    files. Keyed by :func:`_canonical_test_path` for matching; the value is the
    original-cased display path (first seen wins). This is the single promised-set
    definition shared by prompt construction and the completeness gate, so they
    never disagree about what is missing.
    """
    promised: dict[str, str] = {}
    for path in _file_tree_paths(_section_body(artifact_md, "## File Tree")):
        promised.setdefault(_canonical_test_path(path), path.strip())
    matrix = _section_body(artifact_md, "## Requirement-to-Test Matrix")
    if matrix:
        for token in _MATRIX_TEST_FILE_RE.findall(matrix):
            if _looks_like_test_file_path(token):
                promised.setdefault(_canonical_test_path(token), token.strip())
    return promised


def harness_file_tree_paths(artifact_md: str) -> list[str]:
    """Sorted File-Tree paths a harness's ``## Files`` section is meant to emit.

    The deterministic checklist is injected into the Files chunk's prompt so the
    model cannot silently drop a file it just enumerated in the File Tree. This
    attacks the two-chunk divergence at its source. Display casing is preserved.
    """
    return sorted(_file_tree_paths(_section_body(artifact_md, "## File Tree")))


def missing_harness_files(artifact_md: str) -> tuple[list[str], int]:
    """Files the tree/matrix promise but the ``## Files`` section never emitted.

    Returns ``(missing_display_paths, total_promised)`` for deterministic
    completeness reporting. Deliberately independent of ``_harness_issues``' internal
    guards — in particular it reports missing files even when the Files section
    emitted **zero** ``### File:`` headings — because that "the whole Files chunk
    fell over" case is exactly when a repair matters most.  Matching is canonical
    (case/prefix/``::``-insensitive); the returned paths keep original casing for
    the regenerate prompt.
    """
    emitted = _emitted_file_index(artifact_md)
    promised = _promised_harness_files(artifact_md)
    missing = [
        disp for canon, disp in promised.items() if not _file_is_emitted(canon, emitted)
    ]
    return sorted(missing), len(promised)


def _harness_issues(artifact_md: str, deps: dict[str, str]) -> list[CompletenessIssue]:
    issues: list[CompletenessIssue] = []
    index = harness_test_index(artifact_md)
    emitted = _emitted_file_index(artifact_md, index)
    tree_paths = _file_tree_paths(_section_body(artifact_md, "## File Tree"))
    if tree_paths:
        # Compare canonically (case/`./`/`harness/`-insensitive) AND by basename
        # (`_file_is_emitted`) so neither a normalisation difference nor a bare
        # tree leaf vs a directory-qualified heading manufactures a phantom
        # missing block. Display keeps the tree's original casing.
        missing_blocks = sorted(
            path
            for path in tree_paths
            if not _file_is_emitted(_canonical_test_path(path), emitted)
        )
        if missing_blocks:
            issues.append(
                CompletenessIssue(
                    code="harness_file_tree_missing_block",
                    detail=(
                        "HARNESS File Tree lists files that are missing from "
                        f"the ## Files section: {', '.join(missing_blocks[:10])}."
                    ),
                    reference=", ".join(missing_blocks[:10]),
                )
            )
    matrix = _section_body(artifact_md, "## Requirement-to-Test Matrix")
    # Language-agnostic matrix integrity: every test FILE the matrix promises a
    # requirement must exist as a `### File:` block. Catches the silent coverage
    # hole where the matrix maps NFR-001 to `tests/performance/perf.test.ts` but
    # that file is never emitted (and is absent from the File Tree too, so the
    # tree→files check above also misses it). Works on any test framework,
    # unlike the `test_`-prefixed name check below which is pytest-shaped.
    if matrix:
        matrix_files: dict[str, str] = {}
        for token in _MATRIX_TEST_FILE_RE.findall(matrix):
            if _looks_like_test_file_path(token):
                matrix_files.setdefault(_canonical_test_path(token), token.strip())
        missing_files = sorted(
            disp
            for canon, disp in matrix_files.items()
            if not _file_is_emitted(canon, emitted)
        )
        if missing_files:
            issues.append(
                CompletenessIssue(
                    code="harness_matrix_missing_file",
                    detail=(
                        "HARNESS Requirement-to-Test Matrix maps requirements to "
                        "test files that were never emitted in the ## Files "
                        f"section: {', '.join(missing_files[:10])}."
                    ),
                    reference=", ".join(missing_files[:10]),
                )
            )
    file_body = _section_body(artifact_md, "## Files")
    # A requirement id that survives traceability (it appears SOMEWHERE in the
    # harness) but is mapped to no test — present only in prose, e.g. an
    # "FR-007..010 deferred to a follow-up" note — is the most genuine kind of
    # missing coverage, and before this check it showed on ZERO deterministic
    # surfaces: `uncovered_requirements`/`harness_matrix_missing_file` reason only
    # about matrix rows that EXIST, and `_traceability_issues` counts the bare id
    # string as covered. Advisory (non-blocking, CoverageGap) — mirrors the
    # plan's `rtm_missing_upstream_id`. Fires only when BOTH the matrix and Files
    # sections parsed (else a mis-titled section would false-positive every id),
    # and an id counts as test-mapped if it appears in the matrix OR a File
    # block's traceability comment — so a real test that merely lacks a matrix row
    # is never falsely flagged (Fable verify #1).
    if matrix and file_body:
        upstream_req_ids = _upstream_requirement_ids(deps)
        if upstream_req_ids:
            mapped = set(_REQUIREMENT_ID_RE.findall(matrix))
            mapped.update(_REQUIREMENT_ID_RE.findall(file_body))
            present = set(_REQUIREMENT_ID_RE.findall(artifact_md))
            untested = sorted((upstream_req_ids & present) - mapped)
            if untested:
                issues.append(
                    CompletenessIssue(
                        code="harness_requirement_not_test_mapped",
                        detail=(
                            "HARNESS names these requirements but maps none of "
                            "them to a test in the Requirement-to-Test Matrix or a "
                            f"test file: {', '.join(untested[:10])}."
                        ),
                        reference=", ".join(untested[:10]),
                    )
                )
    test_names = set(re.findall(r"\btest_[A-Za-z0-9_]+", file_body))
    matrix_tests = set(re.findall(r"\btest_[A-Za-z0-9_]+", matrix))
    missing_tests = sorted(matrix_tests - test_names)
    if missing_tests:
        issues.append(
            CompletenessIssue(
                code="harness_matrix_missing_test",
                detail=(
                    "HARNESS Requirement-to-Test Matrix references tests absent "
                    f"from file contents: {', '.join(missing_tests[:10])}."
                ),
                reference=", ".join(missing_tests[:10]),
            )
        )
    for test_name in sorted(test_names):
        if not _test_has_traceability_comment(file_body, test_name):
            issues.append(
                CompletenessIssue(
                    code="missing_test_traceability_comment",
                    detail=(
                        f"HARNESS test {test_name} lacks an immediate Tests: "
                        "traceability comment."
                    ),
                    reference=test_name,
                )
            )
            break
    issues.extend(_harness_file_body_issues(artifact_md, index))
    return issues


def _harness_file_body_issues(
    artifact_md: str, index: HarnessTestIndex
) -> list[CompletenessIssue]:
    """``### File:`` headings that were promised but carry no code — both modes.

    Shared by standard and Demo Day (called from ``validate_artifact_completeness``
    for the latter): "the Files section exists but nothing in it has a body" is a
    structural fact about the artifact, not a mode-specific rigor level, and Demo
    Day is the mode whose whole contract is that the package builds.

    Reads the same body-aware scan as :func:`_emitted_file_index` rather than
    re-matching fences with a second regex — the previous ``_FILE_BLOCK_RE``
    required the fence to follow the heading IMMEDIATELY, so a file with one line
    of prose before its code block was reported as lacking a block at all.
    """
    if "## Files" not in artifact_md:
        return []
    issues: list[CompletenessIssue] = []
    if not index.files_with_body:
        issues.append(
            CompletenessIssue(
                code="missing_harness_file_blocks",
                detail=(
                    "HARNESS must include complete fenced code blocks under "
                    "File headings."
                ),
                reference="## Files",
            )
        )
    # ``known_files``/``files_with_body`` are canonical (casefolded, prefix
    # stripped) — matching identity, never display. Recover each heading's
    # original spelling for the message the user reads.
    display: dict[str, str] = {}
    for heading in _FILE_HEADING_RE.findall(artifact_md):
        display.setdefault(_canonical_test_path(heading), heading.strip())
    for path in sorted(index.known_files - index.files_with_body):
        issues.append(
            CompletenessIssue(
                code="incomplete_harness_file_block",
                detail=(
                    f"Harness file {display.get(path, path)} lacks a complete "
                    "fenced code block."
                ),
                reference=display.get(path, path),
            )
        )
    return issues


def _test_has_traceability_comment(file_body: str, test_name: str) -> bool:
    lines = file_body.splitlines()
    for index, line in enumerate(lines):
        if not re.search(rf"\b{re.escape(test_name)}\b", line):
            continue
        previous = [candidate.strip() for candidate in lines[max(0, index - 3) : index]]
        return any(
            re.match(r"^(#|//)\s*Tests:\s*(FR|NFR|SEC|AC)-\d{3}", candidate)
            for candidate in previous
        )
    return False


def _file_tree_paths(body: str) -> set[str]:
    paths: set[str] = set()
    for raw in body.splitlines():
        line = raw.strip()
        if not line or line.startswith("```"):
            continue
        # Strip the tree-drawing prefix — Unicode box-drawing (├ └ │ ─ and the
        # heavy/curved variants) and the ASCII fallbacks (`|`, `+`, `-`) both —
        # then any trailing `# annotation`. The horizontal ─ (U+2500) MUST be in
        # the class or a nested leaf keeps a ``── name`` prefix (with a space) and
        # is then rejected as prose (the exact reason nested-tree leaves were
        # dropped — Fable #8).
        line = re.sub(r"^[\s`│─├└┃┏┗┣┓┛┫╰╯╭╮|+-]+", "", line).strip()
        line = line.split("#", 1)[0].strip()
        if not line or line.endswith("/") or line.endswith(":"):
            continue
        # A real tree entry is a single whitespace-free token. Reject prose lines
        # (legends, notes) a model sometimes drops into the tree block — they
        # would otherwise be mistaken for a "path" with spaces in it.
        if " " in line or "\t" in line:
            continue
        # Accept a directory-qualified path OR a bare leaf filename (a nested
        # tree renders leaves as bare names once the branch glyphs are stripped —
        # Fable #8). Bare names are basename-matched against emitted headings, so
        # this never manufactures a phantom "missing".
        if "." in line.rsplit("/", 1)[-1]:
            paths.add(line)
    return paths


# Degenerate floor for the task inventory: even a trivial product decomposes
# into more than a handful of implementation tasks, so fewer than this many
# blocks signals a compressed, shallow generation rather than a small project.
_MIN_TASK_BLOCKS = 6


def _task_issues(artifact_md: str, deps: dict[str, str]) -> list[CompletenessIssue]:
    task_headers = list(_TASK_HEADER_RE.finditer(artifact_md))
    if not task_headers:
        return [
            CompletenessIssue(
                code="missing_task_blocks",
                detail="TASKS.md must include at least one ### T-NNN task block.",
                reference="tasks",
            )
        ]
    issues_floor: list[CompletenessIssue] = []
    if len(task_headers) < _MIN_TASK_BLOCKS:
        issues_floor.append(
            CompletenessIssue(
                code="insufficient_task_count",
                detail=(
                    f"TASKS.md contains only {len(task_headers)} task blocks; "
                    f"at least {_MIN_TASK_BLOCKS} are required for a "
                    "non-degenerate implementation breakdown."
                ),
                reference="tasks",
            )
        )
    # Density initiative (2026-08-02): trimmed from 16 to 9 fields. Cut header
    # fields (Estimated size, Risk, Owner) and cut body blocks (Description,
    # Inputs, Outputs, Rollback / Recovery) were presence-checked only here —
    # no downstream join (not a GitHub assignee/label, not read by
    # pr_export_builder.py/github_projects.py/standard_plan_linter.py). Steps,
    # Acceptance Criteria, and Dependencies are kept: all three are hard
    # construction-verifier joins (standard_plan_linter.py's C1/C2/C3).
    required_fields = [
        "**Phase:**",
        "**Spec refs:**",
        "**Plan refs:**",
        "**Harness refs:**",
        "**Priority:**",
        "**Estimate:**",
        "**Steps**",
        "**Acceptance Criteria**",
        "**Dependencies**",
    ]
    issues: list[CompletenessIssue] = issues_floor
    tasks: list[tuple[int, str]] = []
    # task_num -> the task numbers it declares a dependency on. Built here and
    # checked for *cycles* (not numeric ordering) after the loop — a model may
    # legitimately number tasks by feature area, so a forward reference like
    # T-003 -> T-008 is valid as long as the dependency graph stays acyclic
    # (audit finding #5).
    dep_adjacency: dict[int, set[int]] = {}
    for index, match in enumerate(task_headers):
        end = (
            task_headers[index + 1].start()
            if index + 1 < len(task_headers)
            else len(artifact_md)
        )
        block = artifact_md[match.start() : end]
        task_num = int(match.group(0).split("-")[1].split(":")[0])
        tasks.append((task_num, block))
        missing = [field for field in required_fields if field not in block]
        if missing:
            detail = (
                f"{match.group(0).rstrip(':')} is missing required fields: "
                f"{', '.join(missing[:4])}."
            )
            issues.append(
                CompletenessIssue(
                    code="incomplete_task_block",
                    detail=detail,
                    reference=match.group(0).rstrip(":"),
                )
            )
        deps_value = _task_field_value(block, "Dependencies")
        dep_adjacency[task_num] = {
            int(dep) for dep in _TASK_DEP_RE.findall(deps_value or "")
        }
    issues.extend(_task_dependency_cycle_issues(dep_adjacency))
    issues.extend(_effort_summary_issues(artifact_md, tasks))
    issues.extend(_task_harness_ref_issues(artifact_md, deps))
    return issues


def _task_dependency_cycle_issues(
    dep_adjacency: dict[int, set[int]],
) -> list[CompletenessIssue]:
    """Flag any *circular* task dependency (a genuinely unresolvable order).

    Replaces the old ``dep_num >= task_num`` heuristic, which assumed strict
    topological numbering and so falsely flagged legitimate forward references
    (audit finding #5). Acyclicity is decided by a Kahn's-algorithm topological
    peel over the subgraph of *defined* tasks (edges to undefined task numbers
    cannot close a cycle and are ignored; a self-dependency is a one-node cycle).
    The peel is iterative, so a pathologically deep dependency chain can never
    blow the recursion limit. Emits a single advisory issue naming the tasks that
    participate in a cycle, or none when the graph is acyclic.
    """
    defined = set(dep_adjacency)
    # Restrict edges to defined tasks; a self-loop is kept so it is caught.
    edges: dict[int, set[int]] = {
        num: {dep for dep in deps if dep in defined}
        for num, deps in dep_adjacency.items()
    }
    indegree: dict[int, int] = {num: 0 for num in defined}
    for deps in edges.values():
        for dep in deps:
            indegree[dep] += 1
    queue = [num for num in defined if indegree[num] == 0]
    removed = 0
    while queue:
        num = queue.pop()
        removed += 1
        for dep in edges[num]:
            indegree[dep] -= 1
            if indegree[dep] == 0:
                queue.append(dep)
    if removed == len(defined):
        return []
    cyclic = sorted(num for num in defined if indegree[num] > 0)
    return [
        CompletenessIssue(
            code="invalid_task_dependency_order",
            detail=(
                "TASKS.md has a circular task dependency among: "
                f"{', '.join(f'T-{n:03d}' for n in cyclic[:10])}."
            ),
            reference=", ".join(f"T-{n:03d}" for n in cyclic[:10]),
        )
    ]


def _task_field_value(block: str, field_name: str) -> str | None:
    for line in block.splitlines():
        match = _TASK_FIELD_RE.match(line.strip())
        if match and match.group("field").strip().lower() == field_name.lower():
            return match.group("value").strip()
    return None


def _effort_summary_issues(
    artifact_md: str,
    tasks: list[tuple[int, str]],
) -> list[CompletenessIssue]:
    if not tasks:
        return []
    summary = _section_body(artifact_md, "## Effort Summary")
    task_count_match = re.search(r"Tasks:\s*(\d+)\s+total", summary)
    actual = len(tasks)
    issues: list[CompletenessIssue] = []
    if task_count_match and int(task_count_match.group(1)) != actual:
        issues.append(
            CompletenessIssue(
                code="effort_summary_task_count_mismatch",
                detail=(
                    f"Effort Summary says {task_count_match.group(1)} tasks but "
                    f"the artifact contains {actual} task blocks."
                ),
                reference="## Effort Summary",
            )
        )
    priority_counts = {name: 0 for name in ["MUST", "SHOULD", "COULD"]}
    estimate_counts = {name: 0 for name in ["XL", "L", "M", "S"]}
    for _, block in tasks:
        priority = _first_token(_task_field_value(block, "Priority"))
        estimate = _first_token(_task_field_value(block, "Estimate"))
        if priority in priority_counts:
            priority_counts[priority] += 1
        if estimate in estimate_counts:
            estimate_counts[estimate] += 1
    for name, count in priority_counts.items():
        match = re.search(rf"(\d+)\s+{name}\b", summary)
        if match and int(match.group(1)) != count:
            issues.append(
                CompletenessIssue(
                    code="effort_summary_priority_mismatch",
                    detail=(
                        f"Effort Summary says {match.group(1)} {name} tasks but "
                        f"task blocks contain {count}."
                    ),
                    reference=name,
                )
            )
            break
    for name, count in estimate_counts.items():
        match = re.search(rf"(\d+)x{name}\b", summary)
        if match and int(match.group(1)) != count:
            issues.append(
                CompletenessIssue(
                    code="effort_summary_estimate_mismatch",
                    detail=(
                        f"Effort Summary says {match.group(1)}x{name} but task "
                        f"blocks contain {count}."
                    ),
                    reference=name,
                )
            )
            break
    return issues


def _first_token(value: str | None) -> str:
    parts = (value or "").split()
    return parts[0].upper() if parts else ""


def _ref_token(ref: str) -> str:
    """The harness reference inside a backticked token that may be a COMMAND.

    Task acceptance criteria routinely cite a test as a runnable command —
    ``pytest tests/test_auth.py::test_login -q`` — and the ``test_``-bearing
    backtick scan picks the whole string up. Matching that verbatim always fails
    (``parts[-1]`` is ``test_login -q``, not a test name), so every task that
    wrote its acceptance criterion as a command produced a phantom
    ``task_harness_ref_not_found`` on an artifact whose test exists.

    Normalises first (so ``path :: test`` is one token, not three), then takes
    the whitespace-delimited component that actually names a test — preferring a
    ``::``-qualified one, else a path. A plain reference is returned unchanged.
    """
    normalized = _normalise_harness_ref(ref)
    if " " not in normalized and "\t" not in normalized:
        return normalized
    tokens = normalized.split()
    for token in tokens:
        if "::" in token:
            return token
    for token in tokens:
        if "/" in token:
            return token
    return normalized


def _task_harness_ref_issues(
    artifact_md: str,
    deps: dict[str, str],
) -> list[CompletenessIssue]:
    harness = deps.get("harness", "")
    if not harness:
        return []
    # The SHARED multi-language index, not the retired Python-only scanner this
    # call site was left behind on. That scanner keyed on `def test_` / `class
    # Test`, so a Vitest/Go/RSpec harness parsed as ZERO refs and the check
    # silently disabled itself (`if not known: return []`) — while its matcher
    # skipped `_normalise_harness_ref`, manufacturing false gaps out of `path ::
    # test` spacing, a `./` prefix, or a case difference. `online_eval` and
    # `standard_plan_linter` already read this index; this was the last holdout.
    known = harness_test_index(harness).known_refs
    if not known:
        return []
    task_refs = {
        ref
        for ref in re.findall(r"`([^`]*test_[^`]*)`", artifact_md)
        if "::" in ref or "/" in ref
    }
    missing_refs = sorted(
        ref for ref in task_refs if not ref_matches_harness(_ref_token(ref), known)
    )
    if missing_refs:
        return [
            CompletenessIssue(
                code="task_harness_ref_not_found",
                detail=(
                    "TASKS.md references harness tests that are absent from the "
                    f"HARNESS artifact: {', '.join(missing_refs[:10])}."
                ),
                reference=", ".join(missing_refs[:10]),
            )
        ]
    return []


# ---------------------------------------------------------------------------
# Demo Day mode floors (docs/DEMO_DAY_MODE_IMPLEMENTATION_PLAN.md §6.5). These
# replace the standard spec/harness/tasks floors for demo_day workspaces: a
# ≤5-hour build is deliberately smaller, and the construction verifier (§7) does
# the heavier structural verification. Every code emitted here is non-refundable
# (not in REFUNDABLE_INCOMPLETE_CODES), so it surfaces as a non-blocking advisory
# finding — the user owns the artifact (issue #34 stance), exactly as the
# standard ``insufficient_task_count`` / ``insufficient_requirement_ids`` floors.
# ---------------------------------------------------------------------------

_DEMO_DAY_MIN_FR = 3
_DEMO_DAY_MIN_AC = 3
_DEMO_DAY_MIN_TASK_BLOCKS = 4
# Per-task fields a Demo Day task block must carry (§6.4). Mirrors the standard
# bold-field style and adds the two Demo-Day fields (Estimated minutes,
# Precondition) the construction verifier joins on (C1/C5).
_DEMO_DAY_TASK_FIELDS: tuple[str, ...] = (
    "**Spec refs:**",
    "**Plan refs:**",
    "**Harness refs:**",
    "**Priority:**",
    "**Estimate:**",
    "**Estimated minutes:**",
    "**Precondition:**",
    "**Steps**",
    "**Acceptance Criteria**",
)


def _demo_day_spec_issues(artifact_md: str) -> list[CompletenessIssue]:
    issues: list[CompletenessIssue] = []
    distinct_fr = set(re.findall(r"\bFR-\d{3}\b", artifact_md))
    if len(distinct_fr) < _DEMO_DAY_MIN_FR:
        issues.append(
            CompletenessIssue(
                code="insufficient_requirement_ids",
                detail=(
                    f"Demo Day SPEC must define at least {_DEMO_DAY_MIN_FR} "
                    f"distinct FR-NNN identifiers; found {len(distinct_fr)}."
                ),
                reference="FR",
            )
        )
    # The AC ids must live in the ## Acceptance Criteria section so the verifier's
    # C3 (AC → harness RTM → ≥1 task) can join on them (§7.1.1).
    ac_section = _section_body(artifact_md, "## Acceptance Criteria")
    distinct_ac = set(_AC_ID_RE.findall(ac_section))
    if len(distinct_ac) < _DEMO_DAY_MIN_AC:
        issues.append(
            CompletenessIssue(
                code="insufficient_requirement_ids",
                detail=(
                    f"Demo Day SPEC must define at least {_DEMO_DAY_MIN_AC} "
                    "distinct AC-NNN identifiers in the Acceptance Criteria "
                    f"section; found {len(distinct_ac)}."
                ),
                reference="AC",
            )
        )
    return issues


def _e2e_names_a_test(body: str) -> bool:
    """True when the End-to-End Smoke Test section names a concrete test.

    The guarantee-bearing e2e must be cited verbatim by the final task (verifier
    C4), so the section has to name a backticked test file/path or test name —
    not just prose. Conservative: an empty or prose-only section reads as a miss.
    """
    if not body.strip():
        return False
    tokens = re.findall(r"`([^`]+)`", body)
    return any(
        _looks_like_test_file_path(token)
        or "test" in token.lower()
        or "e2e" in token.lower()
        or "smoke" in token.lower()
        for token in tokens
    )


def _demo_day_harness_issues(artifact_md: str) -> list[CompletenessIssue]:
    issues: list[CompletenessIssue] = []
    index = harness_test_index(artifact_md)
    e2e_body = _section_body(artifact_md, "## End-to-End Smoke Test")
    if not _e2e_names_a_test(e2e_body):
        issues.append(
            CompletenessIssue(
                code="missing_e2e_smoke_test",
                detail=(
                    "Demo Day HARNESS must define at least one End-to-End Smoke "
                    "Test naming the guarantee-bearing test file/path (the "
                    "unmockable test that must be green from the first slice)."
                ),
                reference="## End-to-End Smoke Test",
            )
        )
    promised_issues = _promised_file_issues(artifact_md)
    issues.extend(promised_issues)
    # Backstop for the one file-emission hole ``_promised_file_issues`` cannot
    # see: it reports only files the File Tree / Matrix promised in a PARSEABLE
    # form, so a Files section with no bodies and no parseable promise is silent.
    # Demo Day deliberately surfaces ONE consolidated file finding rather than
    # standard mode's triad, so this fires only when that one is absent.
    if not promised_issues and "## Files" in artifact_md and not index.files_with_body:
        issues.append(
            CompletenessIssue(
                code="missing_harness_file_blocks",
                detail=(
                    "Demo Day HARNESS ## Files section contains no complete "
                    "fenced code block."
                ),
                reference="## Files",
            )
        )
    return issues


def _promised_file_issues(artifact_md: str) -> list[CompletenessIssue]:
    """Files the Demo Day harness promised but never emitted (ONE consolidated gap).

    Demo Day previously ran NO file-emission check at all: ``_harness_issues`` —
    which carries the standard mode's ``harness_file_tree_missing_block`` /
    ``harness_matrix_missing_file`` / ``missing_harness_file_blocks`` triad — is
    on the ``else`` branch of :func:`validate_artifact_completeness`. A Demo Day
    harness whose File Tree promised N test files and whose ``## Files`` section
    emitted zero produced only a generic "shallow section" advisory, even though
    Demo Day is the *guarantee-bearing* mode whose whole contract is that the
    package builds. The construction verifier does not cover it either: its
    ``_harness_file_paths`` unions ``### File:`` headings WITH ``## File Tree``
    leaves, so C2 ``task_to_test`` accepts a task citing a file that only ever
    existed in the tree.

    Reuses :func:`missing_harness_files` — until now referenced only by tests —
    so "promised vs emitted" has exactly one definition. Matching there is
    canonical (case / ``./`` / ``harness/`` / ``::``-insensitive, with a basename
    fallback), which is what keeps this from manufacturing phantom gaps out of
    path-spelling differences.

    Deliberately ONE finding listing every missing file rather than the standard
    mode's three overlapping codes: the lean mode should surface one actionable
    gap, not three restatements of it. The code is ``harness_file_tree_missing_block``
    — already non-refundable and already mapped to ``CoverageGap`` in
    stage_manager's ``_COMPLETENESS_ADVISORY_KIND`` — so this is delivered,
    finalisable, never refunded, and never triggers a regenerate cascade.
    """
    missing, total = missing_harness_files(artifact_md)
    if not missing or not total:
        return []
    return [
        CompletenessIssue(
            code="harness_file_tree_missing_block",
            detail=(
                f"Demo Day HARNESS promises {total} file(s) in its File Tree / "
                f"Requirement-to-Test Matrix but the ## Files section never "
                f"emitted {len(missing)} of them: {', '.join(missing[:10])}."
            ),
            reference=", ".join(missing[:10]),
        )
    ]


def _demo_day_task_issues(artifact_md: str) -> list[CompletenessIssue]:
    task_headers = list(_TASK_HEADER_RE.finditer(artifact_md))
    if not task_headers:
        return [
            CompletenessIssue(
                code="missing_task_blocks",
                detail="Demo Day TASKS.md must include at least one ### T-NNN block.",
                reference="tasks",
            )
        ]
    issues: list[CompletenessIssue] = []
    if len(task_headers) < _DEMO_DAY_MIN_TASK_BLOCKS:
        issues.append(
            CompletenessIssue(
                code="insufficient_task_count",
                detail=(
                    f"Demo Day TASKS.md contains only {len(task_headers)} task "
                    f"blocks; at least {_DEMO_DAY_MIN_TASK_BLOCKS} are required "
                    "for a verifiable walking-skeleton build."
                ),
                reference="tasks",
            )
        )
    for index, match in enumerate(task_headers):
        end = (
            task_headers[index + 1].start()
            if index + 1 < len(task_headers)
            else len(artifact_md)
        )
        block = artifact_md[match.start() : end]
        missing = [field for field in _DEMO_DAY_TASK_FIELDS if field not in block]
        if missing:
            issues.append(
                CompletenessIssue(
                    code="incomplete_task_fields",
                    detail=(
                        f"{match.group(0).rstrip(':')} is missing required Demo Day "
                        f"task fields: {', '.join(missing[:4])}."
                    ),
                    reference=match.group(0).rstrip(":"),
                )
            )
    return issues


def _upstream_requirement_ids(deps: dict[str, str]) -> set[str]:
    upstream_ids: set[str] = set()
    for content in deps.values():
        upstream_ids.update(_REQUIREMENT_ID_RE.findall(content))
    return upstream_ids


def _upstream_acceptance_ids(deps: dict[str, str]) -> set[str]:
    upstream_ids: set[str] = set()
    for content in deps.values():
        upstream_ids.update(_AC_ID_RE.findall(content))
    return upstream_ids


def _traceability_issues(
    artifact_md: str,
    deps: dict[str, str],
) -> list[CompletenessIssue]:
    upstream_ids = _upstream_requirement_ids(deps)
    if any(key in deps for key in {"spec", "plan", "harness"}):
        upstream_ids.update(_upstream_acceptance_ids(deps))
    if not upstream_ids:
        return []
    present = set(_REQUIREMENT_ID_RE.findall(artifact_md))
    present.update(_AC_ID_RE.findall(artifact_md))
    missing = sorted(upstream_ids - present)
    if not missing:
        return []
    return [
        CompletenessIssue(
            code="insufficient_upstream_traceability",
            detail=(
                "The artifact does not preserve all upstream FR/NFR/SEC/AC IDs "
                f"({len(upstream_ids) - len(missing)}/{len(upstream_ids)} present)."
            ),
            reference=", ".join(missing[:10]),
        )
    ]
