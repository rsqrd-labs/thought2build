"""T-247 — critic quality-gate tests.

Two layers:
- Security/contract unit tests on the critic module itself (schema cannot carry
  artifact bytes; prompt template is held in code; fail-open on judge failure).
- Behavioral tests that drive StageManager.generate() with a stubbed
  critic_review to exercise the regenerate loop, the persisted blocked-draft
  cleanup, the quality_gate_failed SSE event, and the disable_critic escape
  hatch.  The harness-level contract tests are pure
  greps; these are where the loop's real correctness is checked.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import artifact_fixtures
import pytest
from prometheus_client import REGISTRY
from pydantic import ValidationError

from models import (
    CreditLedger,
    EvalResult,
    Stage,
    StageGenerationChunk,
    StageGenerationRun,
    StageVersion,
    Workspace,
)
from services.pipeline import critic as critic_module
from services.pipeline.artifact_validator import (
    MissingSectionError,
)
from services.pipeline.critic import (
    AUDIT_EVENT_CRITIC_DISABLED,
    CriticFinding,
    StageCriticResult,
    critic_review,
)
from services.pipeline.stage_manager import StageManager

# A spec artifact containing every required section heading and the v1.9
# evidence fields so the deterministic validator passes before the critic is
# reached.  Also well past the critic's 500-char gradable floor so the direct
# critic_review unit tests do real work.
_LONG_ARTIFACT = artifact_fixtures.VALID_SPEC


# ---------------------------------------------------------------------------
# Fakes (mirror of the test_stage_manager harness, kept self-contained).
# ---------------------------------------------------------------------------
class _FakePipeline:
    def zremrangebyscore(self, *a, **kw) -> "_FakePipeline":
        return self

    def zadd(self, *a, **kw) -> "_FakePipeline":
        return self

    def zcard(self, *a, **kw) -> "_FakePipeline":
        return self

    def expire(self, *a, **kw) -> "_FakePipeline":
        return self

    async def execute(self) -> list:
        return [0, 1, 1, 1]


class _FakeRedis:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    async def eval(self, *args, **kwargs) -> int:
        return 1

    def pipeline(self) -> _FakePipeline:
        return _FakePipeline()

    async def get(self, key: str) -> str | None:
        return self._store.get(key)

    async def set(self, key: str, value: str, ex: int = 0) -> None:
        self._store[key] = value

    async def delete(self, *keys: str) -> None:
        for k in keys:
            self._store.pop(k, None)

    async def zrem(self, *_args: Any) -> int:
        return 1


class _FakeResult:
    def __init__(self, value: Any = None, many: list | None = None) -> None:
        self._value = value
        self._many = many or []

    def scalar_one_or_none(self) -> Any:
        return self._value

    def scalar_one(self) -> Any:
        return self._value

    def scalars(self) -> "_FakeResult":
        return self

    def all(self) -> list:
        return self._many

    def __iter__(self):
        yield from self._many


def _is_eval_result_select(statement: Any) -> bool:
    """True when a SQLAlchemy statement is a SELECT against EvalResult."""
    try:
        return any(
            cd.get("entity") is EvalResult for cd in statement.column_descriptions
        )
    except Exception:
        return False


def _select_entity(statement: Any) -> Any:
    """The primary ORM entity of a SELECT, or None for non-ORM statements."""
    try:
        for cd in statement.column_descriptions:
            entity = cd.get("entity")
            if entity is not None:
                return entity
    except Exception:
        return None
    return None


def _is_by_id_select(statement: Any, table: str) -> bool:
    """True for a single-row ``WHERE <table>.id = :id`` SELECT."""
    return f"{table}.id =" in str(statement)


# The fake DB the active generate()-driving test is exercising; the autouse
# ``_patch_pipeline_session`` fixture points ``database.AsyncSessionLocal`` at
# it so the pipeline's own-session stage/workspace re-load (and the detached
# critic) transparently see the seeded rows (docs/REFRESH_DURING_GENERATION
# _PLAN.md).
_ACTIVE_MULTI_QUERY_DB: "_MultiQueryDB | None" = None


class _MultiQueryDB:
    def __init__(self, responses: list[Any]) -> None:
        self._responses = iter(responses)
        self.added: list[Any] = []
        self._committed = False
        # First-seen Stage/Workspace rows, replayed for later by-id reads so the
        # pipeline's re-load on its own session returns the same seeded object a
        # real DB's identity map would.
        self._captured_stage: Stage | None = None
        self._captured_workspace: Workspace | None = None
        self._generation_runs: list[StageGenerationRun] = []
        self._generation_chunks: list[StageGenerationChunk] = []
        global _ACTIVE_MULTI_QUERY_DB
        _ACTIVE_MULTI_QUERY_DB = self

    async def __aenter__(self) -> "_MultiQueryDB":
        return self

    async def __aexit__(self, *exc: Any) -> bool:
        return False

    async def close(self) -> None:
        pass

    async def execute(self, statement: Any) -> _FakeResult:
        table_name = getattr(getattr(statement, "table", None), "name", None)
        entity = _select_entity(statement)
        rendered = str(statement)
        params = statement.compile().params
        if table_name == "stage_generation_chunks":
            if statement.__class__.__name__ == "Insert":
                run_id = params["generation_run_id"]
                chunk_key = params["chunk_key"]
                exists = any(
                    row.generation_run_id == run_id and row.chunk_key == chunk_key
                    for row in self._generation_chunks
                )
                if not exists:
                    self._generation_chunks.append(
                        StageGenerationChunk(
                            id=uuid4(),
                            generation_run_id=run_id,
                            chunk_key=chunk_key,
                            ordinal=params["ordinal"],
                            content=params["content"],
                            provider=params["provider"],
                            model=params["model"],
                            retry_count=params["retry_count"],
                            created_at=datetime.now(UTC),
                        )
                    )
                return _FakeResult()
            if statement.__class__.__name__ == "Delete":
                self._generation_chunks.clear()
                return _FakeResult()
        if (
            table_name == "stage_generation_runs"
            and statement.__class__.__name__ == "Update"
        ):
            if self._generation_runs:
                run = self._generation_runs[-1]
                for column, bind in statement._values.items():
                    setattr(run, column.name, bind.value)
            return _FakeResult()
        if entity is StageGenerationRun:
            return _FakeResult(
                self._generation_runs[-1] if self._generation_runs else None
            )
        if entity is StageGenerationChunk:
            if "count(stage_generation_chunks.id)" in rendered:
                return _FakeResult(len(self._generation_chunks))
            rows = sorted(self._generation_chunks, key=lambda row: row.ordinal)
            return _FakeResult(many=rows)
        # The inline structural eval (issue #27 Phase 1) looks up the version's
        # existing EvalResult.  Model an empty eval_results table without
        # consuming a seeded response, so the ordered generate-flow responses
        # below are unaffected.
        if _is_eval_result_select(statement):
            return _FakeResult(None)
        # A by-id re-load of a previously-seen Stage/Workspace replays that row
        # without consuming a response, mirroring a real DB across sessions.
        if (
            self._captured_stage is not None
            and _select_entity(statement) is Stage
            and _is_by_id_select(statement, "stages")
        ):
            return _FakeResult(self._captured_stage)
        if (
            self._captured_workspace is not None
            and _select_entity(statement) is Workspace
            and _is_by_id_select(statement, "workspaces")
        ):
            return _FakeResult(self._captured_workspace)
        try:
            val = next(self._responses)
        except StopIteration:
            val = None
        if isinstance(val, Stage) and self._captured_stage is None:
            self._captured_stage = val
        elif isinstance(val, Workspace) and self._captured_workspace is None:
            self._captured_workspace = val
        if isinstance(val, list):
            return _FakeResult(many=val)
        return _FakeResult(val)

    def add(self, instance: Any) -> None:
        if isinstance(instance, (StageVersion, CreditLedger)) and (
            getattr(instance, "id", None) is None
        ):
            instance.id = uuid4()
        if isinstance(instance, StageGenerationRun):
            self._generation_runs.append(instance)
        self.added.append(instance)

    async def flush(self) -> None:
        pass

    async def commit(self) -> None:
        self._committed = True

    async def rollback(self) -> None:
        pass

    async def refresh(self, instance: Any) -> None:
        # Mirror the DB server defaults a real refresh populates so a freshly
        # inserted EvalResult is serialisable by _eval_to_dict.
        if isinstance(instance, EvalResult):
            if getattr(instance, "id", None) is None:
                instance.id = uuid4()
            if getattr(instance, "created_at", None) is None:
                instance.created_at = datetime.now(UTC)
            if getattr(instance, "flagged", None) is None:
                instance.flagged = False


def install_pipeline_session_patch(monkeypatch):
    """Point ``database.AsyncSessionLocal`` at the active seeded ``_MultiQueryDB``.

    generate() now runs its pipeline on a session it opens itself (so a client
    disconnect can no longer kill an in-flight generation) and re-loads the
    stage/workspace on it (docs/REFRESH_DURING_GENERATION_PLAN.md).  In tests the
    pipeline-owned session must be the seeded ``_MultiQueryDB``.  Reused by
    test_research_wiring, which drives generate() through this module's
    ``_build_generate_env``.  A test that needs a different session installs its
    own narrower ``patch("database.AsyncSessionLocal")``, which wins inside its
    block.
    """
    import database
    from services.pipeline import stage_manager as stage_manager_module

    global _ACTIVE_MULTI_QUERY_DB
    _ACTIVE_MULTI_QUERY_DB = None

    def _factory():
        if _ACTIVE_MULTI_QUERY_DB is None:
            raise RuntimeError(
                "AsyncSessionLocal() called with no active _MultiQueryDB; "
                "patch database.AsyncSessionLocal explicitly in this test."
            )
        return _ACTIVE_MULTI_QUERY_DB

    monkeypatch.setattr(database, "AsyncSessionLocal", _factory)

    async def _execute_enqueued_inline(self, payload, _run_id):
        # Unit tests use the seeded in-memory DB/Redis doubles. Exercise the
        # durable worker entrypoint synchronously without requiring local arq.
        await self.execute_queued_generation(payload)

    monkeypatch.setattr(
        StageManager, "_enqueue_generation_job", _execute_enqueued_inline
    )
    monkeypatch.setattr(stage_manager_module, "_GENERATION_OBSERVER_POLL_SECONDS", 0)


@pytest.fixture(autouse=True)
def _patch_pipeline_session(monkeypatch):
    from services.llm import provider_status

    global _ACTIVE_MULTI_QUERY_DB
    install_pipeline_session_patch(monkeypatch)
    monkeypatch.setattr(provider_status, "is_provider_configured", lambda _p: True)
    monkeypatch.setattr(provider_status, "can_route", lambda _p: True)
    yield
    _ACTIVE_MULTI_QUERY_DB = None


def _make_stage(workspace_id, stage_type="spec", status="draft") -> Stage:
    return Stage(
        id=uuid4(),
        workspace_id=workspace_id,
        type=stage_type,
        status=status,
        content=None,
        current_version=0,
        review_gate_acknowledged=False,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )


def _make_workspace(stages: list[Stage], *, disable_critic: bool = False) -> Workspace:
    w = Workspace(
        id=uuid4(),
        user_id=uuid4(),
        name="WS",
        problem_statement=(
            "I want to build a todo app for teams to create tasks, assign owners, "
            "track project status, and authenticate users."
        ),
        provider="anthropic",
        model="claude-sonnet-4-6",
        status="active",
        public_share_enabled=False,
        disable_critic=disable_critic,
        brave_research_enabled=False,
        restricted_environment=False,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    w.stages = stages
    return w


def _make_user():
    user = MagicMock()
    user.id = uuid4()
    return user


class _FakeAdvisoryResult:
    def __init__(self, obj) -> None:
        self._obj = obj

    def scalar_one_or_none(self):
        return self._obj


def _fake_session_local_for(stage):
    """A patchable ``database.AsyncSessionLocal`` that yields *stage* on lookup.

    Returns (session_local_factory, session) so a test can both inject the
    factory and assert ``commit`` on the session the detached critic used.
    """
    session = MagicMock()
    session.execute = AsyncMock(return_value=_FakeAdvisoryResult(stage))
    session.commit = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=session), session


async def _fake_stream(
    system, user, max_tokens=0, **kwargs
) -> AsyncGenerator[str, None]:
    # Answer each chunk prompt with its own section group and sentinel,
    # split in two so it streams like a real generation.
    payload = artifact_fixtures.spec_stream_payload(user)
    half = len(payload) // 2
    yield payload[:half]
    yield payload[half:]


def _build_generate_env(*, disable_critic: bool = False):
    workspace_id = uuid4()
    spec_stage = _make_stage(workspace_id, "spec")
    workspace = _make_workspace([spec_stage], disable_critic=disable_critic)
    user = _make_user()
    deduction = CreditLedger(id=uuid4(), user_id=user.id, amount=-10, reason="generate")
    db = _MultiQueryDB([spec_stage, workspace, [], deduction])
    svc = StageManager(redis_client=_FakeRedis())
    return svc, spec_stage, workspace, user, deduction, db


# ---------------------------------------------------------------------------
# Security / contract unit tests on the critic module.
# ---------------------------------------------------------------------------
def test_critic_result_schema_excludes_artifact_bytes() -> None:
    """The critic verdict can carry only a pass flag + structured findings."""
    assert set(StageCriticResult.model_fields) == {"passed", "findings"}
    # extra="forbid": a malicious judge response cannot smuggle a rewrite back
    # through an unexpected key.
    for forbidden in ("artifact_md", "artifact_content", "rewritten", "new_artifact"):
        with pytest.raises(ValidationError):
            StageCriticResult.model_validate({"passed": True, forbidden: "x" * 1000})


def test_critic_prompt_template_held_in_code() -> None:
    """The critic prompt must be an in-code constant, never load_prompt()."""
    src = Path(critic_module.__file__).read_text()
    assert "load_prompt(" not in src
    assert isinstance(critic_module._CRITIC_SYSTEM_PROMPT, str)
    assert len(critic_module._CRITIC_SYSTEM_PROMPT) > 200


def test_build_critic_user_prompt_bounds_maximal_harness_and_deps() -> None:
    """Finding #4: unlike online_eval.py, the critic previously sent the full,
    untruncated artifact and every dependency on every generation, risking
    unbounded judge cost/context-limit errors (silently swallowed as
    passed=True by the fail-open handler exactly when the artifact is largest).
    A synthetic maximal-size harness fixture (50 files x 200 lines, per the
    remediation plan's own suggested fixture shape) plus three large
    dependencies must render a bounded prompt, not one that scales unboundedly
    with input size.
    """
    huge_file_block = "\n".join(
        f"def test_case_{i}():\n    assert True  # padding line to reach ~200 lines\n"
        * 40
        for i in range(50)
    )
    huge_dep = "x" * 50_000

    prompt = critic_module._build_critic_user_prompt(
        "harness",
        huge_file_block,
        {"spec": huge_dep, "plan": huge_dep},
    )

    # Bounded: artifact capped at _ARTIFACT_LIMITS["harness"], each dep capped
    # at _DEP_LIMIT, plus a small constant for headings/labels/omission notes —
    # nowhere near the ~150K+ chars an unbounded render of these inputs would
    # produce.
    assert len(prompt) < 60_000
    assert "characters omitted for eval budget" in prompt


def test_build_critic_user_prompt_small_inputs_are_untouched() -> None:
    """Happy path: inputs well under the bound must render byte-identical
    (compact_text is a no-op below the limit) — the fix must not truncate
    ordinary, well-within-budget generations."""
    small_artifact = "## Harness Overview\nSmall harness body.\n"
    small_dep = "## Spec\nSmall spec body.\n"

    prompt = critic_module._build_critic_user_prompt(
        "harness", small_artifact, {"spec": small_dep}
    )

    assert small_artifact in prompt
    assert small_dep in prompt
    assert "characters omitted" not in prompt


@pytest.mark.asyncio
async def test_critic_review_fail_open_on_judge_error() -> None:
    """A judge call that raises must not brick generation — return passed=True."""
    with patch(
        "services.pipeline.critic.call_judge_model",
        new_callable=AsyncMock,
        side_effect=RuntimeError("judge down"),
    ):
        result = await critic_review("plan", _LONG_ARTIFACT, {})
    assert result.passed is True
    assert result.findings == []


@pytest.mark.asyncio
async def test_critic_review_fail_open_on_unparseable_verdict() -> None:
    """A non-JSON judge response must fail open rather than raise."""
    with patch(
        "services.pipeline.critic.call_judge_model",
        new_callable=AsyncMock,
        return_value="I think this looks fine, honestly.",
    ):
        result = await critic_review("plan", _LONG_ARTIFACT, {})
    assert result.passed is True


@pytest.mark.asyncio
async def test_critic_review_parses_fenced_findings() -> None:
    """A fenced JSON verdict with findings is parsed into the result model."""
    verdict = (
        "```json\n"
        '{"passed": false, "findings": [{"kind": "MissingSection", '
        '"detail": "No Security Architecture section", "reference": "Security"}]}\n'
        "```"
    )
    with patch(
        "services.pipeline.critic.call_judge_model",
        new_callable=AsyncMock,
        return_value=verdict,
    ):
        result = await critic_review("plan", _LONG_ARTIFACT, {})
    assert result.passed is False
    assert result.findings[0].kind == "MissingSection"


@pytest.mark.asyncio
async def test_critic_review_short_artifact_skips_judge() -> None:
    """Artifacts under the gradable threshold pass without a judge call."""
    judge = AsyncMock()
    with patch("services.pipeline.critic.call_judge_model", judge):
        result = await critic_review("spec", "too short", {})
    assert result.passed is True
    judge.assert_not_called()


# ---------------------------------------------------------------------------
# Audit M12: verdict salvage — one malformed finding must not void the verdict.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_critic_review_salvages_verdict_with_one_invented_kind() -> None:
    """M12 regression: a failing verdict where the judge invented one unlisted
    finding kind used to fail strict validation entirely and fall open to a
    silent passed=True — defeating the gate exactly when the judge found
    defects. Salvage must keep the well-formed findings and the failing
    verdict, dropping only the malformed finding."""
    verdict = (
        '{"passed": false, "findings": ['
        '{"kind": "MissingSection", "detail": "No API Design section", '
        '"reference": "API Design"}, '
        '{"kind": "InventedKindTheSchemaForbids", "detail": "made up", '
        '"reference": null}, '
        '{"kind": "CoverageGap", "detail": "FR-003 unaddressed", '
        '"reference": "FR-003"}]}'
    )
    with patch(
        "services.pipeline.critic.call_judge_model",
        new_callable=AsyncMock,
        return_value=verdict,
    ):
        result = await critic_review("plan", _LONG_ARTIFACT, {})
    assert result.passed is False
    assert [f.kind for f in result.findings] == ["MissingSection", "CoverageGap"]


@pytest.mark.asyncio
async def test_critic_review_salvages_verdict_with_extra_top_level_key() -> None:
    """A judge that adds an unrequested top-level field (e.g. confidence) fails
    strict extra="forbid" validation; salvage recovers the verdict from the
    fields that do conform."""
    verdict = (
        '{"passed": false, "confidence": 0.9, "findings": ['
        '{"kind": "ShallowSection", "detail": "Data Model is one line", '
        '"reference": "Data Model"}]}'
    )
    with patch(
        "services.pipeline.critic.call_judge_model",
        new_callable=AsyncMock,
        return_value=verdict,
    ):
        result = await critic_review("plan", _LONG_ARTIFACT, {})
    assert result.passed is False
    assert result.findings[0].kind == "ShallowSection"


@pytest.mark.asyncio
async def test_critic_review_failing_verdict_with_no_salvageable_findings() -> None:
    """A failing verdict whose findings ALL drop carries nothing the
    regenerate/advisory paths can act on — the existing fail-open applies."""
    verdict = (
        '{"passed": false, "findings": ['
        '{"kind": "NotAKind", "detail": "x", "reference": null}]}'
    )
    with patch(
        "services.pipeline.critic.call_judge_model",
        new_callable=AsyncMock,
        return_value=verdict,
    ):
        result = await critic_review("plan", _LONG_ARTIFACT, {})
    assert result.passed is True
    assert result.findings == []


def test_salvage_verdict_rejects_non_dict_and_bad_passed() -> None:
    """Salvage never invents a verdict: non-object JSON, a missing/non-boolean
    passed field, or unparseable text all return None (caller falls open)."""
    assert critic_module._salvage_verdict("[1, 2]", "spec") is None
    assert critic_module._salvage_verdict('{"findings": []}', "spec") is None
    assert critic_module._salvage_verdict('{"passed": "yes"}', "spec") is None
    assert critic_module._salvage_verdict("not json at all", "spec") is None


def test_salvage_verdict_cannot_carry_artifact_bytes() -> None:
    """Security posture preserved: salvage validates each finding against the
    strict CriticFinding schema, so a finding smuggling extra fields (e.g. a
    rewritten artifact) is dropped, not partially accepted."""
    salvaged = critic_module._salvage_verdict(
        '{"passed": false, "findings": ['
        '{"kind": "CoverageGap", "detail": "d", "reference": "FR-001", '
        '"artifact_md": "REWRITTEN CONTENT"}, '
        '{"kind": "CoverageGap", "detail": "clean", "reference": "FR-002"}]}',
        "spec",
    )
    assert salvaged is not None
    assert len(salvaged.findings) == 1
    assert salvaged.findings[0].detail == "clean"


# ---------------------------------------------------------------------------
# Audit M12 (schema side) + L17 (dep filtering) + elision awareness.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_new_finding_kinds_validate_strictly() -> None:
    """ImplementationLeak and DependencyCycle — the kinds _per_stage_focus asks
    for — must parse through the strict (non-salvage) path."""
    verdict = (
        '{"passed": false, "findings": ['
        '{"kind": "ImplementationLeak", "detail": "Spec names a Postgres '
        'schema", "reference": "users table DDL"}, '
        '{"kind": "DependencyCycle", "detail": "T-004 depends on T-009", '
        '"reference": "T-004"}]}'
    )
    with patch(
        "services.pipeline.critic.call_judge_model",
        new_callable=AsyncMock,
        return_value=verdict,
    ) as judge:
        result = await critic_review("tasks", _LONG_ARTIFACT, {})
    assert result.passed is False
    assert {f.kind for f in result.findings} == {
        "ImplementationLeak",
        "DependencyCycle",
    }
    judge.assert_awaited_once()


def test_per_stage_focus_only_asks_for_expressible_kinds() -> None:
    """M12 root cause guard: every kind name a focus string tells the judge to
    flag must exist in the CriticFindingKind literal — an inexpressible ask
    tempts the judge to invent a kind. Covers both the standard and Demo Day
    focus text, since each mode has its own branch."""
    import re
    from typing import get_args

    valid_kinds = set(get_args(critic_module.CriticFindingKind))
    for mode in ("standard", "demo_day"):
        for stage in ("spec", "plan", "harness", "tasks", "unknown"):
            focus = critic_module._per_stage_focus(stage, mode)
            for kind in re.findall(r"flag (?:them as )?([A-Z][A-Za-z]+)", focus):
                assert (
                    kind in valid_kinds
                ), f"{mode}/{stage} focus asks for unknown kind {kind}"


def test_per_stage_focus_demo_day_does_not_name_standard_only_sections() -> None:
    """Gap fix: the critic used to grade every mode against the standard
    section names regardless of workspace.mode, producing confusing findings
    on Demo Day artifacts (e.g. "missing Security, Privacy, and Abuse
    Expectations" on a spec that correctly has "Security Posture" instead).
    The demo_day focus must reference Demo Day's own section names, not the
    standard-only ones that never appear in a Demo Day artifact."""
    standard_only_headings = (
        "Security, Privacy, and Abuse Expectations",
        "In-Scope (MVP)",
        "User Stories",
        "Architecture Anti-Patterns",
        "Multi-tenancy Stance",
        "API Design",
    )
    for stage in ("spec", "plan", "harness", "tasks"):
        focus = critic_module._per_stage_focus(stage, "demo_day")
        for heading in standard_only_headings:
            assert heading not in focus, f"demo_day/{stage} focus names {heading!r}"


def test_per_stage_focus_defaults_to_standard() -> None:
    """Backward compatibility: omitting mode must be byte-identical to the
    pre-fix standard-only behavior, since every existing caller (and the
    default critic_review/_build_critic_user_prompt signature) relies on it."""
    for stage in ("spec", "plan", "harness", "tasks", "unknown"):
        assert critic_module._per_stage_focus(stage) == critic_module._per_stage_focus(
            stage, "standard"
        )


def test_build_critic_user_prompt_drops_advisory_deps() -> None:
    """L17: research_context and clarification_qa are advisory generation
    context, not upstream contracts — presenting them as 'Upstream dependency'
    makes the judge grade the artifact against non-contract content. Only
    _GRADABLE_DEP_KEYS may appear."""
    prompt = critic_module._build_critic_user_prompt(
        "tasks",
        "## Task Breakdown\nA gradable artifact body.",
        {
            "spec": "## Spec\nReal upstream contract.",
            "problem_statement": "Build a thing.",
            "research_context": "SEARCH SNIPPET: unrelated blog post",
            "clarification_qa": "Q: colour? A: blue",
        },
    )
    assert "Upstream dependency — spec:" in prompt
    assert "Upstream dependency — problem_statement:" in prompt
    assert "research_context" not in prompt
    assert "SEARCH SNIPPET" not in prompt
    assert "clarification_qa" not in prompt
    assert "Q: colour?" not in prompt


def test_critic_system_prompt_is_elision_aware() -> None:
    """H4 companion: bounded inputs carry a literal elision marker; the system
    prompt must tell the judge that absence from visible text is not absence
    from the artifact, keyed on the exact marker phrase compact_text emits."""
    from services.text_compaction import ELISION_MARKER_PHRASE

    prompt = critic_module._CRITIC_SYSTEM_PROMPT
    assert ELISION_MARKER_PHRASE in prompt
    assert "Absence from the visible text is NOT" in prompt


# ---------------------------------------------------------------------------
# Behavioral tests driving StageManager.generate() with a stubbed critic.
# ---------------------------------------------------------------------------
def _generate_patches(svc: StageManager):
    """Common patch set for generate(): credits, prompt, validate, adapter."""
    deduct = patch(
        "services.pipeline.stage_manager.credit_service.deduct",
        new_callable=AsyncMock,
    )
    refund = patch(
        "services.pipeline.stage_manager.credit_service.refund",
        new_callable=AsyncMock,
    )
    invalidate = patch(
        "services.pipeline.stage_manager.credit_service.invalidate",
        new_callable=AsyncMock,
    )
    build = patch(
        "services.pipeline.stage_manager.build_prompt",
        new_callable=AsyncMock,
        return_value=("sys", "user", "0"),
    )
    validate = patch(
        "services.pipeline.stage_manager.validate_async",
        new_callable=AsyncMock,
        return_value=MagicMock(is_safe=True, reason=""),
    )
    set_cache = patch(
        "services.pipeline.stage_manager.set_cached_generation",
        new_callable=AsyncMock,
    )
    adapter = MagicMock()
    adapter.stream = _fake_stream
    adapter.last_completion = None
    get_llm = patch("services.pipeline.stage_manager.get_llm", return_value=adapter)
    return deduct, refund, invalidate, build, validate, set_cache, get_llm


@pytest.mark.asyncio
async def test_missing_section_gate_persists_blocked_draft() -> None:
    """Zero-LLM section gate failures follow the same blocked-draft contract."""
    svc, stage, workspace, user, deduction, db = _build_generate_env()
    deduct, refund, invalidate, build, validate, set_cache, get_llm = _generate_patches(
        svc
    )
    missing = ["## Acceptance Criteria"]

    with (
        deduct as md,
        refund as mr,
        invalidate,
        build,
        validate,
        set_cache as mc,
        get_llm,
        patch(
            "services.pipeline.stage_manager.validate_readiness_async",
            new_callable=AsyncMock,
            side_effect=MissingSectionError("spec", missing),
        ),
        patch(
            "services.pipeline.stage_manager.critic_review",
            new_callable=AsyncMock,
        ) as mock_critic,
    ):
        md.return_value = deduction
        tokens = [t async for t in svc.generate(stage.id, user, db)]

    mock_critic.assert_not_awaited()
    mr.assert_not_awaited()
    mc.assert_not_awaited()
    assert stage.status == "draft"
    assert stage.quality_gate_status == "blocked"
    assert stage.quality_gate_kind == "missing_sections"
    assert stage.quality_gate_payload["missing"] == missing
    # This gate path does not refund; the contract must report that honestly.
    assert stage.quality_gate_payload["refunded_prior_attempt"] is False
    assert stage.quality_gate["recovery"]["refunded_prior_attempt"] is False
    assert any(isinstance(a, StageVersion) for a in db.added)
    assert any("quality_gate_failed" in t for t in tokens)


@pytest.mark.asyncio
async def test_disable_critic_skips_gate() -> None:
    """disable_critic=True bypasses the critic entirely."""
    svc, stage, workspace, user, deduction, db = _build_generate_env(
        disable_critic=True
    )
    deduct, refund, invalidate, build, validate, set_cache, get_llm = _generate_patches(
        svc
    )
    with (
        deduct as md,
        refund,
        invalidate,
        build,
        validate,
        set_cache,
        get_llm,
        patch(
            "services.pipeline.stage_manager.critic_review",
            new_callable=AsyncMock,
        ) as mock_critic,
    ):
        md.return_value = deduction
        tokens = [t async for t in svc.generate(stage.id, user, db)]

    mock_critic.assert_not_awaited()
    assert any("done" in t for t in tokens)
    assert stage.content == _LONG_ARTIFACT.strip()


# ---------------------------------------------------------------------------
# Audit-event test for the owner-only disable_critic toggle.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_disable_critic_writes_audit_event(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from routers.workspace import set_workspace_critic
    from schemas.workspace import WorkspaceCriticToggle

    workspace = _make_workspace([], disable_critic=False)
    user = _make_user()
    db = _MultiQueryDB([])

    with (
        patch(
            "routers.workspace.workspace_service.get",
            new_callable=AsyncMock,
            return_value=workspace,
        ),
        patch(
            "routers.workspace.derive_coverage_summary",
            new_callable=AsyncMock,
            return_value=None,
        ),
        caplog.at_level(logging.INFO, logger="routers.workspace"),
    ):
        await set_workspace_critic(
            workspace.id,
            WorkspaceCriticToggle(disable_critic=True),
            user=user,
            db=db,
        )

    assert workspace.disable_critic is True
    audit_records = [
        r for r in caplog.records if r.getMessage() == AUDIT_EVENT_CRITIC_DISABLED
    ]
    assert len(audit_records) == 1
    record = audit_records[0]
    assert record.actor_id == str(user.id)
    assert record.workspace_id == str(workspace.id)
    assert record.disable_critic is True


@pytest.mark.asyncio
async def test_disable_critic_no_change_skips_audit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A no-op toggle (same value) writes no audit row."""
    from routers.workspace import set_workspace_critic
    from schemas.workspace import WorkspaceCriticToggle

    workspace = _make_workspace([], disable_critic=False)
    user = _make_user()
    db = _MultiQueryDB([])

    with (
        patch(
            "routers.workspace.workspace_service.get",
            new_callable=AsyncMock,
            return_value=workspace,
        ),
        patch(
            "routers.workspace.derive_coverage_summary",
            new_callable=AsyncMock,
            return_value=None,
        ),
        caplog.at_level(logging.INFO, logger="routers.workspace"),
    ):
        await set_workspace_critic(
            workspace.id,
            WorkspaceCriticToggle(disable_critic=False),
            user=user,
            db=db,
        )

    assert not [
        r for r in caplog.records if r.getMessage() == AUDIT_EVENT_CRITIC_DISABLED
    ]


@pytest.mark.asyncio
async def test_condensed_problem_statement_surfaces_advisory_notice() -> None:
    """Phase D: a lossily-condensed problem statement is surfaced as a non-blocking
    advisory notice on the delivered draft, even when the critic is clean."""
    svc, stage, workspace, user, deduction, db = _build_generate_env()
    deduct, refund, invalidate, build, validate, set_cache, get_llm = _generate_patches(
        svc
    )
    with (
        deduct as md,
        refund as mr,
        invalidate,
        build as mock_build,
        validate,
        set_cache,
        get_llm,
        patch(
            "services.pipeline.stage_manager.critic_review",
            new_callable=AsyncMock,
            return_value=StageCriticResult(passed=True),
        ),
    ):
        md.return_value = deduction
        mock_build.return_value = ("sys", "user", "3")  # deterministic clamp
        tokens = [t async for t in svc.generate(stage.id, user, db)]

    assert any("done" in t for t in tokens)
    assert stage.status == "draft"  # finalisable — never blocked
    assert stage.quality_gate_status == "advisory"
    kinds = [f["kind"] for f in stage.quality_gate_payload["findings"]]
    assert kinds == ["ProblemStatementCondensed"]
    mr.assert_not_awaited()  # informational notice never refunds
    # The generation completes normally — no blocking event.
    assert not any("quality_gate_failed" in t for t in tokens)


@pytest.mark.asyncio
async def test_uncondensed_clean_generation_stays_clear() -> None:
    """Phase D regression: rung "0" (no condensation) + clean critic ⇒ no advisory."""
    svc, stage, workspace, user, deduction, db = _build_generate_env()
    deduct, refund, invalidate, build, validate, set_cache, get_llm = _generate_patches(
        svc
    )
    with (
        deduct as md,
        refund,
        invalidate,
        build,  # _generate_patches default return rung "0"
        validate,
        set_cache,
        get_llm,
        patch(
            "services.pipeline.stage_manager.critic_review",
            new_callable=AsyncMock,
            return_value=StageCriticResult(passed=True),
        ),
    ):
        md.return_value = deduction
        [t async for t in svc.generate(stage.id, user, db)]

    assert stage.quality_gate_status == "clear"


# ---------------------------------------------------------------------------
# Async-advisory critic (docs/CRITIC_ASYNC_ADVISORY_PLAN.md) — the default path:
# the judge runs OFF the critical path after `done`.
# ---------------------------------------------------------------------------
_ADVISORY_METRIC = "thought2build_pipeline_critic_advisory_findings_total"


@pytest.mark.asyncio
async def test_async_advisory_done_before_judge_and_schedules_critic() -> None:
    """Default path: `done` is emitted WITHOUT awaiting the judge inline, and the
    critic is scheduled as a detached background task instead."""
    svc, stage, workspace, user, deduction, db = _build_generate_env()
    deduct, refund, invalidate, build, validate, set_cache, get_llm = _generate_patches(
        svc
    )
    with (
        deduct as md,
        refund as mr,
        invalidate,
        build,
        validate,
        set_cache as mc,
        get_llm,
        patch(
            "services.pipeline.stage_manager.critic_review",
            new_callable=AsyncMock,
        ) as mock_critic,
        patch.object(svc, "_schedule_critic_review") as mock_schedule,
    ):
        md.return_value = deduction
        tokens = [t async for t in svc.generate(stage.id, user, db)]

    # The judge never runs on the critical path — only the detached scheduler is
    # invoked, once, pinned to the just-persisted version.
    mock_critic.assert_not_awaited()
    mock_schedule.assert_called_once()
    kwargs = mock_schedule.call_args.kwargs
    assert kwargs["stage_id"] == stage.id
    assert kwargs["version"] == stage.current_version
    assert kwargs["stage_type"] == "spec"
    assert kwargs["content"] == _LONG_ARTIFACT.strip()
    # The detached critic is pinned to the provider the artifact was generated
    # on, which is the platform primary (Anthropic) — so the judge resolves to
    # that provider's CHEAP judge entry via JUDGE_MODELS (Haiku 4.5), never to
    # the frontier model the artifact itself was generated with.
    assert kwargs["provider"] == "anthropic"
    # The usable draft is delivered, persisted clean, cached; nothing refunded.
    assert any("done" in t for t in tokens)
    assert not any("quality_gate_failed" in t for t in tokens)
    assert stage.status == "draft"
    assert stage.content == _LONG_ARTIFACT.strip()
    assert stage.quality_gate_status == "clear"
    mc.assert_awaited_once()
    mr.assert_not_awaited()


@pytest.mark.asyncio
async def test_async_advisory_section_gate_still_blocks_inline() -> None:
    """The zero-LLM section gate stays terminal on the critical path: a miss
    blocks the draft and the background critic is never scheduled."""
    svc, stage, workspace, user, deduction, db = _build_generate_env()
    deduct, refund, invalidate, build, validate, set_cache, get_llm = _generate_patches(
        svc
    )
    missing = ["## Acceptance Criteria"]
    with (
        deduct as md,
        refund,
        invalidate,
        build,
        validate,
        set_cache,
        get_llm,
        patch(
            "services.pipeline.stage_manager.validate_readiness_async",
            new_callable=AsyncMock,
            side_effect=MissingSectionError("spec", missing),
        ),
        patch.object(svc, "_schedule_critic_review") as mock_schedule,
    ):
        md.return_value = deduction
        tokens = [t async for t in svc.generate(stage.id, user, db)]

    mock_schedule.assert_not_called()
    assert stage.quality_gate_status == "blocked"
    assert stage.quality_gate_kind == "missing_sections"
    assert any("quality_gate_failed" in t for t in tokens)


@pytest.mark.asyncio
async def test_async_advisory_disable_critic_skips_schedule() -> None:
    """disable_critic bypasses the whole gate, including the background critic."""
    svc, stage, workspace, user, deduction, db = _build_generate_env(
        disable_critic=True
    )
    deduct, refund, invalidate, build, validate, set_cache, get_llm = _generate_patches(
        svc
    )
    with (
        deduct as md,
        refund,
        invalidate,
        build,
        validate,
        set_cache,
        get_llm,
        patch.object(svc, "_schedule_critic_review") as mock_schedule,
    ):
        md.return_value = deduction
        tokens = [t async for t in svc.generate(stage.id, user, db)]

    mock_schedule.assert_not_called()
    assert any("done" in t for t in tokens)
    assert stage.content == _LONG_ARTIFACT.strip()


@pytest.mark.asyncio
async def test_dispatch_critic_review_failing_marks_advisory() -> None:
    """A failing background verdict attaches advisory findings, bumps the counter,
    and records the critic_advisory cost outcome — all off the critical path."""
    workspace_id = uuid4()
    stage = _make_stage(workspace_id, "spec")
    stage.current_version = 3
    svc = StageManager(redis_client=_FakeRedis())
    session_local, session = _fake_session_local_for(stage)
    fail = StageCriticResult(
        passed=False,
        findings=[CriticFinding(kind="MissingSection", detail="missing ADR")],
    )
    before = REGISTRY.get_sample_value(_ADVISORY_METRIC, {"stage": "spec"}) or 0.0

    with (
        patch(
            "services.pipeline.stage_manager.critic_review",
            new_callable=AsyncMock,
            return_value=fail,
        ),
        patch("database.AsyncSessionLocal", session_local),
        patch(
            "services.pipeline.stage_manager.update_cost_event_quality_outcome",
            new_callable=AsyncMock,
        ) as mock_cost,
    ):
        await svc._dispatch_critic_review(
            stage_id=stage.id,
            version=3,
            stage_type="spec",
            content=_LONG_ARTIFACT,
            critic_deps={},
            provider="anthropic",
            content_generation_id="gen-1",
        )

    session.commit.assert_awaited_once()
    assert stage.quality_gate_status == "advisory"
    assert stage.quality_gate_kind == "critic_findings"
    assert stage.quality_gate_version == 3
    kinds = [f["kind"] for f in stage.quality_gate_payload["findings"]]
    assert kinds == ["MissingSection"]
    after = REGISTRY.get_sample_value(_ADVISORY_METRIC, {"stage": "spec"}) or 0.0
    assert after - before == 1.0
    mock_cost.assert_awaited_once_with("gen-1", "critic_advisory")


@pytest.mark.asyncio
async def test_dispatch_critic_review_forwards_demo_day_mode() -> None:
    """Gap fix wiring: _dispatch_critic_review must forward the caller's mode
    to critic_review so a Demo Day artifact is graded by the Demo Day focus
    text, not the standard one. Regression guard for the mode plumbing added
    end to end from _schedule_critic_review's two call sites in
    stage_manager.py through to critic._per_stage_focus."""
    workspace_id = uuid4()
    stage = _make_stage(workspace_id, "spec")
    stage.current_version = 1
    svc = StageManager(redis_client=_FakeRedis())
    session_local, _ = _fake_session_local_for(stage)
    with (
        patch(
            "services.pipeline.stage_manager.critic_review",
            new_callable=AsyncMock,
            return_value=StageCriticResult(passed=True),
        ) as mock_review,
        patch("database.AsyncSessionLocal", session_local),
        patch(
            "services.pipeline.stage_manager.update_cost_event_quality_outcome",
            new_callable=AsyncMock,
        ),
    ):
        await svc._dispatch_critic_review(
            stage_id=stage.id,
            version=1,
            stage_type="spec",
            content=_LONG_ARTIFACT,
            critic_deps={},
            provider="anthropic",
            content_generation_id="gen-1",
            mode="demo_day",
        )

    mock_review.assert_awaited_once_with(
        "spec", _LONG_ARTIFACT, {}, provider="anthropic", mode="demo_day"
    )


@pytest.mark.asyncio
async def test_dispatch_critic_review_merges_with_condensed_notice() -> None:
    """The background critic preserves an advisory notice already attached at
    persist (the Phase-D condensed-statement notice) rather than overwriting."""
    workspace_id = uuid4()
    stage = _make_stage(workspace_id, "spec")
    stage.current_version = 2
    # Persist already attached the condensed-statement notice.
    stage.quality_gate_status = "advisory"
    stage.quality_gate_kind = "critic_findings"
    stage.quality_gate_payload = {
        "stage": "spec",
        "kind": "critic_findings",
        "findings": [{"kind": "ProblemStatementCondensed", "detail": "x"}],
    }
    svc = StageManager(redis_client=_FakeRedis())
    session_local, _ = _fake_session_local_for(stage)
    fail = StageCriticResult(
        passed=False,
        findings=[CriticFinding(kind="MissingSection", detail="missing ADR")],
    )
    with (
        patch(
            "services.pipeline.stage_manager.critic_review",
            new_callable=AsyncMock,
            return_value=fail,
        ),
        patch("database.AsyncSessionLocal", session_local),
        patch(
            "services.pipeline.stage_manager.update_cost_event_quality_outcome",
            new_callable=AsyncMock,
        ),
    ):
        await svc._dispatch_critic_review(
            stage_id=stage.id,
            version=2,
            stage_type="spec",
            content=_LONG_ARTIFACT,
            critic_deps={},
            provider="anthropic",
            content_generation_id="gen-1",
        )

    kinds = {f["kind"] for f in stage.quality_gate_payload["findings"]}
    assert kinds == {"ProblemStatementCondensed", "MissingSection"}


@pytest.mark.asyncio
async def test_dispatch_critic_review_passing_makes_no_change() -> None:
    """A clean background verdict never opens a session or writes anything."""
    workspace_id = uuid4()
    stage = _make_stage(workspace_id, "spec")
    stage.current_version = 1
    svc = StageManager(redis_client=_FakeRedis())
    session_local, session = _fake_session_local_for(stage)
    with (
        patch(
            "services.pipeline.stage_manager.critic_review",
            new_callable=AsyncMock,
            return_value=StageCriticResult(passed=True),
        ),
        patch("database.AsyncSessionLocal", session_local),
        patch(
            "services.pipeline.stage_manager.update_cost_event_quality_outcome",
            new_callable=AsyncMock,
        ) as mock_cost,
    ):
        await svc._dispatch_critic_review(
            stage_id=stage.id,
            version=1,
            stage_type="spec",
            content=_LONG_ARTIFACT,
            critic_deps={},
            provider="anthropic",
            content_generation_id="gen-1",
        )

    session_local.assert_not_called()  # no session opened on a passing verdict
    session.commit.assert_not_awaited()
    mock_cost.assert_not_awaited()
    assert stage.quality_gate_status is None or stage.quality_gate_status != "advisory"


@pytest.mark.asyncio
async def test_dispatch_critic_review_version_mismatch_no_write() -> None:
    """Staleness guard: a version bump since scheduling means a newer draft
    superseded the one judged — findings must not be stamped on the wrong one."""
    workspace_id = uuid4()
    stage = _make_stage(workspace_id, "spec")
    stage.current_version = 5  # newer than the scheduled version below
    svc = StageManager(redis_client=_FakeRedis())
    session_local, session = _fake_session_local_for(stage)
    fail = StageCriticResult(
        passed=False,
        findings=[CriticFinding(kind="MissingSection", detail="missing ADR")],
    )
    with (
        patch(
            "services.pipeline.stage_manager.critic_review",
            new_callable=AsyncMock,
            return_value=fail,
        ),
        patch("database.AsyncSessionLocal", session_local),
        patch(
            "services.pipeline.stage_manager.update_cost_event_quality_outcome",
            new_callable=AsyncMock,
        ) as mock_cost,
    ):
        await svc._dispatch_critic_review(
            stage_id=stage.id,
            version=3,
            stage_type="spec",
            content=_LONG_ARTIFACT,
            critic_deps={},
            provider="anthropic",
            content_generation_id="gen-1",
        )

    session.commit.assert_not_awaited()
    mock_cost.assert_not_awaited()
    assert stage.quality_gate_status != "advisory"


@pytest.mark.asyncio
async def test_dispatch_critic_review_judge_error_swallowed() -> None:
    """Fail-open: a judge error never raises out of the detached task and never
    touches the already-delivered draft."""
    workspace_id = uuid4()
    stage = _make_stage(workspace_id, "spec")
    stage.current_version = 1
    svc = StageManager(redis_client=_FakeRedis())
    session_local, session = _fake_session_local_for(stage)
    with (
        patch(
            "services.pipeline.stage_manager.critic_review",
            new_callable=AsyncMock,
            side_effect=RuntimeError("judge down"),
        ),
        patch("database.AsyncSessionLocal", session_local),
    ):
        # Must not raise.
        await svc._dispatch_critic_review(
            stage_id=stage.id,
            version=1,
            stage_type="spec",
            content=_LONG_ARTIFACT,
            critic_deps={},
            provider="anthropic",
            content_generation_id="gen-1",
        )

    session_local.assert_not_called()
    session.commit.assert_not_awaited()
    assert stage.quality_gate_status != "advisory"
