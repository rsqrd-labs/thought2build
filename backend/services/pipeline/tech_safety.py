from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from redis.asyncio import Redis
from redis.exceptions import RedisError

from config import settings
from services.observability import PIPELINE_TECH_SAFETY_LOOKUP_FAILURES

logger = logging.getLogger(__name__)

TECH_SAFETY_GATE_KIND = "technology_safety"
POLICY_PATH = Path(__file__).with_name("tech_safety_policy.json")
# Standard mode's plan heading is tried first; Demo Day's leaner heading second.
# A plain `.find()` on a single hardcoded heading previously made every Demo Day
# plan's Technology Stack table invisible to this parser (Demo Day never emits
# "and Rationale") — the same heading-mismatch failure class already fixed once
# in agents_md_builder._named_section for the same two headings.
_TECH_STACK_HEADINGS: tuple[str, ...] = (
    "## Technology Stack and Rationale",  # standard
    "## Technology Stack",  # demo_day
)
_NEXT_H2_RE = re.compile(r"(?m)^##\s+")
_VERSION_RE = re.compile(r"\b(\d+(?:\.\d+){0,2})\b")
_PACKAGE_PIN_RE = re.compile(
    r"(?P<name>@?[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)?)"
    r"\s*(?:==|=|@)\s*(?P<version>\d+(?:\.\d+){1,3}[A-Za-z0-9_.-]*)"
)
_REJECTED_CONTEXT_RE = re.compile(
    r"\b(?:rejected|avoid|do not use|not chosen|why not|alternative|deprecated "
    r"\(do not use\)|eol \(do not use\))\b",
    re.IGNORECASE,
)
_LAYER_LIFECYCLE_WORDS = {
    "language",
    "runtime",
    "framework",
    "database",
    "cache",
    "queue",
    "orm",
    "llm provider",
}


@dataclass(frozen=True)
class TechnologyChoice:
    layer: str
    choice: str
    version: str | None
    support_status: str | None
    eol_date: str | None
    source: str


@dataclass(frozen=True)
class TechSafetyFinding:
    code: str
    severity: str
    technology: str
    version: str | None
    source: str
    evidence: str
    remediation: str

    @property
    def kind(self) -> str:
        return self.code

    @property
    def detail(self) -> str:
        version = f" {self.version}" if self.version else ""
        return f"{self.technology}{version}: {self.evidence}. {self.remediation}"

    @property
    def reference(self) -> str:
        return self.technology

    def to_payload(self) -> dict[str, str | None]:
        return {
            "kind": self.code,
            "code": self.code,
            "severity": self.severity,
            "technology": self.technology,
            "version": self.version,
            "source": self.source,
            "evidence": self.evidence,
            "detail": self.detail,
            "reference": self.reference,
            "remediation": self.remediation,
        }


class TechSafetyError(RuntimeError):
    def __init__(
        self,
        findings: list[TechSafetyFinding],
        *,
        stage_type: str,
        partial_content: str,
        repair_attempted: bool = False,
    ) -> None:
        self.findings = findings
        self.stage_type = stage_type
        self.partial_content = partial_content
        self.repair_attempted = repair_attempted
        super().__init__(
            f"Stage {stage_type} failed technology safety gate: "
            f"{len(findings)} finding(s)."
        )


@lru_cache(maxsize=1)
def load_policy() -> dict[str, Any]:
    return json.loads(POLICY_PATH.read_text(encoding="utf-8"))


def policy_version() -> str:
    return str(load_policy().get("policy_version") or "unknown")


def policy_sources() -> list[str]:
    return ["local_policy", "osv", "endoflife.date"]


def render_hard_denylist_prose() -> str:
    """The named technologies/families this policy's ``hard_denylists`` blocks.

    Single source of truth for finding #7: ``plan.py``'s prompt used to
    hand-maintain its own prose copy of this list (with its own review date),
    separate from this JSON policy's structured, regex-enforced entries (with
    ITS own review date) — two facts, two clocks, drifting independently. The
    prompt now renders this list from ``load_policy()`` at import time instead
    of hardcoding it, so the two can no longer silently disagree.

    The version *threshold* a human reads (e.g. "Python ≤ 3.10") is not
    reverse-engineered from the enforcement regex — that would be a fragile
    regex-of-a-regex. Instead each entry optionally carries its own
    ``denied_versions`` string, authored by whoever edits the JSON right next
    to the ``pattern`` it describes, so the model-facing threshold and the
    machine-enforced one are declared together in the one place edits happen.
    Entries with no version threshold (e.g. a whole deprecated model family)
    render as just the technology name.
    """
    policy = load_policy()
    clauses: list[str] = []
    for entry in policy.get("hard_denylists", []):
        technology = str(entry.get("technology") or "").strip()
        if not technology:
            continue
        denied_versions = str(entry.get("denied_versions") or "").strip()
        clause = f"{technology} {denied_versions}" if denied_versions else technology
        clauses.append(clause)
    return ", ".join(clauses)


def _blocked_severities(policy: dict[str, Any]) -> set[str]:
    configured = {
        severity.strip().lower()
        for severity in settings.tech_safety_blocked_severities.split(",")
        if severity.strip()
    }
    if configured:
        return configured
    return {
        str(severity).lower()
        for severity in policy.get(
            "blocked_severities", ["critical", "high", "unknown"]
        )
    }


def _policy_reviewed_at(policy: dict[str, Any]) -> date | None:
    try:
        return date.fromisoformat(str(policy.get("last_reviewed")))
    except (TypeError, ValueError):
        return None


def _freshness_findings(policy: dict[str, Any], today: date) -> list[TechSafetyFinding]:
    reviewed = _policy_reviewed_at(policy)
    max_age = int(
        policy.get("max_age_days") or settings.tech_safety_policy_max_age_days
    )
    if reviewed is None:
        return [
            TechSafetyFinding(
                code="technology_policy_unverified",
                severity="critical",
                technology="technology safety policy",
                version=None,
                source="local_policy",
                evidence="Policy last_reviewed is missing or invalid",
                remediation="Review and republish the technology safety policy.",
            )
        ]
    if today - reviewed > timedelta(days=max_age):
        return [
            TechSafetyFinding(
                code="technology_policy_stale",
                severity="critical",
                technology="technology safety policy",
                version=reviewed.isoformat(),
                source="local_policy",
                evidence=f"Policy is older than {max_age} days",
                remediation="Re-review the policy before accepting generated stacks.",
            )
        ]
    return []


def _section_body(artifact_md: str, heading: str) -> str:
    start = artifact_md.find(heading)
    if start < 0:
        return ""
    rest = artifact_md[start + len(heading) :]
    end = _NEXT_H2_RE.search(rest)
    return rest[: end.start()] if end else rest


def _first_matching_section_body(
    artifact_md: str, headings: tuple[str, ...]
) -> tuple[str, str]:
    """Try each heading in order; return (matched_heading, body) for the first hit.

    Mirrors the ordered-candidate pattern already used by
    agents_md_builder._named_section for this same standard-vs-demo_day heading
    pair, scoped locally here rather than sharing that helper: the two callers'
    fallback semantics differ (a textual default vs. an empty choice list).
    """
    for heading in headings:
        body = _section_body(artifact_md, heading)
        if body:
            return heading, body
    return "", ""


def parse_technology_stack(artifact_md: str) -> list[TechnologyChoice]:
    heading, body = _first_matching_section_body(artifact_md, _TECH_STACK_HEADINGS)
    if not body:
        return []
    rows = [line.strip() for line in body.splitlines() if line.strip().startswith("|")]
    if len(rows) < 2:
        return []
    header_index = next(
        (index for index, row in enumerate(rows) if "Layer" in row and "Choice" in row),
        -1,
    )
    if header_index < 0:
        return []
    headers = [_normalize_header(cell) for cell in _split_table_row(rows[header_index])]
    choices: list[TechnologyChoice] = []
    for row in rows[header_index + 1 :]:
        cells = _split_table_row(row)
        if len(cells) != len(headers):
            continue
        values = dict(zip(headers, cells, strict=True))
        layer = values.get("layer", "").strip()
        choice = values.get("choice", "").strip()
        if not layer or not choice or set(choice) <= {"-", ":"}:
            continue
        choices.append(
            TechnologyChoice(
                layer=layer,
                choice=choice,
                version=_extract_version(values.get("version", "")),
                support_status=values.get("support_status", "").strip() or None,
                eol_date=values.get("eol_date", "").strip() or None,
                source=f"{heading}:{layer}",
            )
        )
    return choices


def _split_table_row(row: str) -> list[str]:
    return [cell.strip() for cell in row.strip().strip("|").split("|")]


def _normalize_header(header: str) -> str:
    lower = re.sub(r"\s+", " ", header.strip().lower())
    if lower.startswith("version"):
        return "version"
    if lower.startswith("support status"):
        return "support_status"
    if lower.startswith("eol"):
        return "eol_date"
    if lower.startswith("layer"):
        return "layer"
    if lower.startswith("choice"):
        return "choice"
    return lower.replace(" ", "_")


def _extract_version(text: str | None) -> str | None:
    if not text:
        return None
    match = _VERSION_RE.search(text)
    return match.group(1) if match else None


def _is_rejected_context(text: str, start: int) -> bool:
    context = text[max(0, start - 80) : start + 80]
    return bool(_REJECTED_CONTEXT_RE.search(context))


def _hard_denylist_findings(
    text: str,
    *,
    source: str,
    policy: dict[str, Any],
) -> list[TechSafetyFinding]:
    findings: list[TechSafetyFinding] = []
    for entry in policy.get("hard_denylists", []):
        pattern = str(entry.get("pattern") or "")
        if not pattern:
            continue
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            if _is_rejected_context(text, match.start()):
                continue
            findings.append(
                TechSafetyFinding(
                    code=str(entry.get("code") or "hard_denylist_match"),
                    severity=str(entry.get("severity") or "high"),
                    technology=str(entry.get("technology") or match.group(0)),
                    version=_extract_version(match.group(0)),
                    source=source,
                    evidence=f"Selected output contains `{match.group(0)}`",
                    remediation=str(
                        entry.get("remediation")
                        or "Choose a maintained, supported alternative."
                    ),
                )
            )
    return findings


def _choice_findings(
    choices: list[TechnologyChoice],
    *,
    policy: dict[str, Any],
    today: date,
) -> list[TechSafetyFinding]:
    findings: list[TechSafetyFinding] = []
    for choice in choices:
        haystack = f"{choice.layer} {choice.choice} {choice.version or ''}"
        findings.extend(
            _hard_denylist_findings(haystack, source=choice.source, policy=policy)
        )
        status = (choice.support_status or "").lower()
        if "deprecated" in status or status == "eol" or "eol" in status:
            findings.append(
                TechSafetyFinding(
                    code="support_status_blocked",
                    severity="high",
                    technology=choice.choice,
                    version=choice.version,
                    source=choice.source,
                    evidence=f"Support status is `{choice.support_status}`",
                    remediation="Replace with an Active supported technology.",
                )
            )
        eol = _parse_eol_date(choice.eol_date)
        if eol is not None:
            days_until_eol = (eol - today).days
            near_eol_days = int(policy.get("near_eol_block_days") or 730)
            if days_until_eol < 0:
                findings.append(
                    TechSafetyFinding(
                        code="technology_eol",
                        severity="critical",
                        technology=choice.choice,
                        version=choice.version,
                        source=choice.source,
                        evidence=f"EOL date {eol.isoformat()} is in the past",
                        remediation="Choose an actively supported release.",
                    )
                )
            elif (
                _requires_near_eol_block(choice.layer)
                and days_until_eol <= near_eol_days
            ):
                findings.append(
                    TechSafetyFinding(
                        code="technology_near_eol",
                        severity="high",
                        technology=choice.choice,
                        version=choice.version,
                        source=choice.source,
                        evidence=(
                            f"EOL date {eol.isoformat()} is within "
                            f"{near_eol_days} days"
                        ),
                        remediation="Choose a release with a longer support window.",
                    )
                )
    return findings


def _parse_eol_date(value: str | None) -> date | None:
    if not value:
        return None
    match = re.search(r"\d{4}-\d{2}-\d{2}", value)
    if not match:
        return None
    try:
        return date.fromisoformat(match.group(0))
    except ValueError:
        return None


def _requires_near_eol_block(layer: str) -> bool:
    normalized = layer.lower()
    return any(word in normalized for word in _LAYER_LIFECYCLE_WORDS)


def _package_candidates(text: str, policy: dict[str, Any]) -> list[TechnologyChoice]:
    ecosystems = policy.get("package_ecosystems", {})
    candidates: list[TechnologyChoice] = []
    for match in _PACKAGE_PIN_RE.finditer(text):
        name = match.group("name")
        ecosystem = ecosystems.get(name.lower())
        if ecosystem is None:
            continue
        candidates.append(
            TechnologyChoice(
                layer="dependency",
                choice=name,
                version=match.group("version"),
                support_status=None,
                eol_date=None,
                source=f"{ecosystem}:{name}",
            )
        )
    return candidates


async def validate_technology_safety(
    stage_type: str,
    artifact_md: str,
    deps: dict[str, str] | None = None,
    *,
    redis: Redis | None = None,
    http_client: httpx.AsyncClient | None = None,
    today: date | None = None,
) -> None:
    findings = await analyze_technology_safety(
        stage_type,
        artifact_md,
        deps or {},
        redis=redis,
        http_client=http_client,
        today=today,
    )
    blocking = [finding for finding in findings if is_blocking_finding(finding)]
    if blocking:
        raise TechSafetyError(
            blocking,
            stage_type=stage_type,
            partial_content=artifact_md,
        )


def analyze_local_technology_safety(
    stage_type: str,
    artifact_md: str,
    deps: dict[str, str] | None = None,
    *,
    today: date | None = None,
) -> list[TechSafetyFinding]:
    """Deterministic evidence survives independently of external lookup health."""
    today = today or datetime.now(UTC).date()
    policy = load_policy()
    scan_text = _selected_technology_text(stage_type, artifact_md, deps or {})
    choices = parse_technology_stack(artifact_md) if stage_type == "plan" else []
    if stage_type in {"harness", "tasks"}:
        choices.extend(_package_candidates(scan_text, policy))
    return [
        *_freshness_findings(policy, today),
        *_hard_denylist_findings(scan_text, source=stage_type, policy=policy),
        *_choice_findings(choices, policy=policy, today=today),
    ]


async def analyze_technology_safety(
    stage_type: str,
    artifact_md: str,
    deps: dict[str, str] | None = None,
    *,
    redis: Redis | None = None,
    http_client: httpx.AsyncClient | None = None,
    today: date | None = None,
) -> list[TechSafetyFinding]:
    today = today or datetime.now(UTC).date()
    policy = load_policy()
    findings = _freshness_findings(policy, today)

    scan_text = _selected_technology_text(stage_type, artifact_md, deps or {})
    choices = parse_technology_stack(artifact_md) if stage_type == "plan" else []
    if stage_type in {"harness", "tasks"}:
        choices.extend(_package_candidates(scan_text, policy))

    findings.extend(
        _hard_denylist_findings(scan_text, source=stage_type, policy=policy)
    )
    findings.extend(_choice_findings(choices, policy=policy, today=today))
    if choices and redis is not None:
        findings.extend(
            await _lifecycle_findings(
                choices,
                policy=policy,
                redis=redis,
                http_client=http_client,
                today=today,
            )
        )

    pinned = [choice for choice in choices if choice.version]
    if pinned and redis is not None:
        findings.extend(
            await _advisory_findings(
                pinned,
                policy=policy,
                redis=redis,
                http_client=http_client,
            )
        )
    if pinned and redis is None:
        findings.append(
            TechSafetyFinding(
                code="technology_safety_unverified",
                severity="unknown",
                technology="advisory lookup",
                version=None,
                source="osv",
                evidence="No Redis cache was available for bounded advisory lookup",
                remediation="Run validation with Redis-backed advisory caching.",
            )
        )
    return _dedupe_findings(findings)


def _selected_technology_text(
    stage_type: str,
    artifact_md: str,
    deps: dict[str, str],
) -> str:
    if stage_type in {"plan", "harness", "tasks"}:
        # The hard-denylist regex scan deliberately covers the WHOLE artifact
        # for plan, not just the Technology Stack table: a denylisted runtime
        # can appear elsewhere (e.g. a Demo Day restricted-environment
        # ## Environment and Bootstrap install command names an EOL'd
        # interpreter without ever entering the stack table). Narrowing this
        # to the resolved Technology Stack section would have been a coverage
        # regression traded for the heading-mismatch fix below — the
        # structured, table-driven checks already get the precisely resolved
        # section body via parse_technology_stack()/_first_matching_section_body;
        # this text is only for the regex scan, which stays broad for every
        # stage that can introduce its own dependencies in its own bytes.
        return artifact_md
    # SPEC is product-level: only scan explicit implementation constraints and
    # risks/open questions, avoiding false positives on conceptual context.
    parts = [
        _section_body(artifact_md, "## Constraints"),
        _section_body(artifact_md, "## Risks"),
        _section_body(artifact_md, "## Assumptions and Open Questions"),
    ]
    return "\n\n".join(part for part in parts if part)


async def _advisory_findings(
    choices: list[TechnologyChoice],
    *,
    policy: dict[str, Any],
    redis: Redis,
    http_client: httpx.AsyncClient | None,
) -> list[TechSafetyFinding]:
    package_choices: list[TechnologyChoice] = []
    packages: list[dict[str, Any]] = []
    for choice in choices:
        ecosystem = _ecosystem_for(choice, policy)
        if ecosystem is None or choice.version is None:
            continue
        package_choices.append(choice)
        packages.append(
            {
                "package": {
                    "ecosystem": ecosystem,
                    "name": choice.choice,
                },
                "version": choice.version,
            }
        )
    if not packages:
        return []
    cache_key = "tech-safety:osv:" + json.dumps(packages, sort_keys=True)
    cached = await _redis_get_json(redis, cache_key)
    if cached is None:
        try:
            cached = await _query_osv(packages, http_client=http_client)
        except Exception:
            PIPELINE_TECH_SAFETY_LOOKUP_FAILURES.labels(
                source="osv",
                reason="request_failed",
            ).inc()
            logger.warning("tech_safety.osv_lookup_failed", exc_info=True)
            return [
                TechSafetyFinding(
                    code="technology_safety_unverified",
                    severity="unknown",
                    technology="OSV advisory lookup",
                    version=None,
                    source="osv",
                    evidence="Advisory lookup failed and no fresh cache exists",
                    remediation=(
                        "Retry after advisory service recovery or refresh cache."
                    ),
                )
            ]
        await _redis_set_json(
            redis,
            cache_key,
            cached,
            ttl_seconds=settings.tech_safety_osv_cache_ttl_seconds,
        )
    findings: list[TechSafetyFinding] = []
    for choice, result in zip(package_choices, cached.get("results", []), strict=False):
        vulns = result.get("vulns") or []
        for vuln in vulns:
            severity = _osv_severity(vuln)
            findings.append(
                TechSafetyFinding(
                    code="vulnerable_dependency",
                    severity=severity,
                    technology=choice.choice,
                    version=choice.version,
                    source="osv",
                    evidence=f"{vuln.get('id', 'OSV advisory')} affects this version",
                    remediation="Choose a non-vulnerable supported version.",
                )
            )
    return findings


def is_blocking_finding(finding: TechSafetyFinding) -> bool:
    """Apply the product policy without treating lookup uncertainty as danger."""
    denylist_codes = {
        str(entry.get("code") or "hard_denylist_match")
        for entry in load_policy().get("hard_denylists", [])
    }
    if finding.code in denylist_codes:
        return True
    if finding.code in {"technology_eol", "support_status_blocked"}:
        return True
    return (
        finding.code == "vulnerable_dependency"
        and finding.severity.lower() == "critical"
    )


def _ecosystem_for(choice: TechnologyChoice, policy: dict[str, Any]) -> str | None:
    ecosystems = policy.get("package_ecosystems", {})
    return ecosystems.get(choice.choice.lower())


async def _lifecycle_findings(
    choices: list[TechnologyChoice],
    *,
    policy: dict[str, Any],
    redis: Redis,
    http_client: httpx.AsyncClient | None,
    today: date,
) -> list[TechSafetyFinding]:
    findings: list[TechSafetyFinding] = []
    for choice in choices:
        if _parse_eol_date(choice.eol_date) is not None or choice.version is None:
            continue
        product = _lifecycle_product_for(choice, policy)
        if product is None:
            continue
        release = _lifecycle_release_cycle(product, choice.version)
        cache_key = f"tech-safety:eol:{product}:{release}"
        cached = await _redis_get_json(redis, cache_key)
        if cached is None:
            try:
                cached = await _query_eol_release(
                    product,
                    release,
                    http_client=http_client,
                )
            except Exception:
                PIPELINE_TECH_SAFETY_LOOKUP_FAILURES.labels(
                    source="endoflife.date",
                    reason="request_failed",
                ).inc()
                logger.warning(
                    "tech_safety.eol_lookup_failed",
                    extra={
                        "product": product,
                        "release": release,
                    },
                    exc_info=True,
                )
                findings.append(
                    TechSafetyFinding(
                        code="technology_safety_unverified",
                        severity="unknown",
                        technology=choice.choice,
                        version=choice.version,
                        source="endoflife.date",
                        evidence=(
                            "Lifecycle lookup failed and no fresh cache exists for "
                            f"{product} {release}"
                        ),
                        remediation="Retry after lifecycle data is available.",
                    )
                )
                continue
            await _redis_set_json(
                redis,
                cache_key,
                cached,
                ttl_seconds=settings.tech_safety_eol_cache_ttl_seconds,
            )
        findings.extend(_findings_from_eol_release(choice, cached, policy, today))
    return findings


def _lifecycle_product_for(
    choice: TechnologyChoice,
    policy: dict[str, Any],
) -> str | None:
    products = policy.get("lifecycle_products", {})
    normalized_choice = choice.choice.lower().strip()
    normalized_layer = choice.layer.lower().strip()
    if normalized_choice in products:
        return str(products[normalized_choice])
    first_word = normalized_choice.split(maxsplit=1)[0]
    if first_word in products:
        return str(products[first_word])
    if normalized_layer in products:
        return str(products[normalized_layer])
    return None


def _lifecycle_release_cycle(product: str, version: str) -> str:
    parts = version.split(".")
    if product in {"java", "nodejs", "postgresql"}:
        return parts[0]
    if product in {"python", "redis"} and len(parts) >= 2:
        return ".".join(parts[:2])
    return version


async def _query_eol_release(
    product: str,
    release: str,
    *,
    http_client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    close_client = False
    client = http_client
    if client is None:
        close_client = True
        client = httpx.AsyncClient(
            timeout=settings.tech_safety_advisory_timeout_seconds,
            follow_redirects=True,
        )
    try:
        response = await client.get(
            f"{settings.tech_safety_eol_api_base.rstrip('/')}"
            f"/products/{quote(product)}/releases/{quote(release)}",
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get("result") if isinstance(payload, dict) else None
        if isinstance(result, dict):
            return result
        return payload if isinstance(payload, dict) else {}
    finally:
        if close_client:
            await client.aclose()


def _findings_from_eol_release(
    choice: TechnologyChoice,
    release: dict[str, Any],
    policy: dict[str, Any],
    today: date,
) -> list[TechSafetyFinding]:
    if not release or (
        "isEol" not in release
        and "isMaintained" not in release
        and "eolFrom" not in release
    ):
        return [
            TechSafetyFinding(
                code="technology_safety_unverified",
                severity="unknown",
                technology=choice.choice,
                version=choice.version,
                source="endoflife.date",
                evidence="Lifecycle source returned no verifiable support fields",
                remediation=(
                    "Refresh lifecycle policy data before accepting this choice."
                ),
            )
        ]
    eol = _parse_eol_date(str(release.get("eolFrom") or ""))
    is_eol = release.get("isEol") is True
    is_maintained = release.get("isMaintained")
    if is_eol or (eol is not None and eol < today):
        return [
            TechSafetyFinding(
                code="technology_eol",
                severity="critical",
                technology=choice.choice,
                version=choice.version,
                source="endoflife.date",
                evidence="Lifecycle source marks this release as end-of-life",
                remediation="Choose an actively supported release.",
            )
        ]
    if is_maintained is False and eol is None:
        return [
            TechSafetyFinding(
                code="technology_unmaintained",
                severity="high",
                technology=choice.choice,
                version=choice.version,
                source="endoflife.date",
                evidence="Lifecycle source reports this release is not maintained",
                remediation="Choose a maintained release.",
            )
        ]
    if eol is None:
        return []
    near_eol_days = int(policy.get("near_eol_block_days") or 730)
    days_until_eol = (eol - today).days
    if _requires_near_eol_block(choice.layer) and days_until_eol <= near_eol_days:
        return [
            TechSafetyFinding(
                code="technology_near_eol",
                severity="high",
                technology=choice.choice,
                version=choice.version,
                source="endoflife.date",
                evidence=f"EOL date {eol.isoformat()} is within {near_eol_days} days",
                remediation="Choose a release with a longer support window.",
            )
        ]
    return []


async def _query_osv(
    packages: list[dict[str, Any]],
    *,
    http_client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    payload = {"queries": packages}
    close_client = False
    client = http_client
    if client is None:
        close_client = True
        client = httpx.AsyncClient(
            timeout=settings.tech_safety_advisory_timeout_seconds
        )
    try:
        response = await client.post(
            f"{settings.tech_safety_osv_api_base.rstrip('/')}/v1/querybatch",
            json=payload,
        )
        response.raise_for_status()
        return response.json()
    finally:
        if close_client:
            await client.aclose()


def _osv_severity(vuln: dict[str, Any]) -> str:
    severities = vuln.get("severity") or []
    for entry in severities:
        score = str(entry.get("score") or "")
        if score.startswith("CVSS:"):
            parsed = _cvss_score(score)
            if parsed is None:
                continue
            if parsed >= 9.0:
                return "critical"
            if parsed >= 7.0:
                return "high"
            if parsed >= 4.0:
                return "medium"
            return "low"
    database_specific = vuln.get("database_specific") or {}
    severity = str(database_specific.get("severity") or "").lower()
    return severity if severity in {"critical", "high", "medium", "low"} else "unknown"


def _cvss_score(vector: str) -> float | None:
    match = re.search(r"/(?:BS|S):?([0-9]+(?:\.[0-9]+)?)", vector)
    if match:
        return float(match.group(1))
    # OSV usually returns vectors, not base score, so unknown is safer.
    return None


async def _redis_get_json(redis: Redis, key: str) -> dict[str, Any] | None:
    try:
        raw = await redis.get(key)
    except RedisError:
        return None
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


async def _redis_set_json(
    redis: Redis,
    key: str,
    value: dict[str, Any],
    *,
    ttl_seconds: int,
) -> None:
    try:
        await redis.set(key, json.dumps(value), ex=ttl_seconds)
    except RedisError:
        logger.warning("tech_safety.cache_write_failed", exc_info=True)


def _dedupe_findings(
    findings: list[TechSafetyFinding],
) -> list[TechSafetyFinding]:
    seen: set[tuple[str, str, str | None, str]] = set()
    unique: list[TechSafetyFinding] = []
    for finding in findings:
        key = (
            finding.code,
            finding.technology.lower(),
            finding.version,
            finding.source,
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(finding)
    return unique
