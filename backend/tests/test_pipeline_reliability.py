from __future__ import annotations

import hashlib
import json
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from prompts import base, demo_day
from services.pipeline.artifact_validator import (
    CompletenessIssue,
    IncompleteArtifactError,
    MissingSectionError,
    StructuralReadinessError,
    section_contract,
    validate_readiness,
    validate_sections,
)
from services.pipeline.context_budget import ContextBudgetError, assert_context_fits
from services.pipeline.input_manifest import (
    cache_identity,
    restore_workspace,
    snapshot_workspace,
    source_identity,
)
from services.pipeline.markdown_structure import headings
from services.pipeline.prompt_builder import _section_aware_injection, _split_by_h2


def workspace():
    return SimpleNamespace(
        id=uuid4(),
        user_id=uuid4(),
        problem_statement="A private team app",
        clarification_qa=[{"question": "Audience?", "answer": "Account A"}],
        mode="demo_day",
        time_budget_minutes=300,
        restricted_environment=False,
        stages=[
            SimpleNamespace(type="spec", content="FR-001: private", current_version=2)
        ],
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", uuid4()),
        ("user_id", uuid4()),
        ("clarification_qa", [{"question": "Audience?", "answer": "Account B"}]),
        ("mode", "standard"),
        ("time_budget_minutes", 600),
        ("restricted_environment", True),
        ("problem_statement", "Different scope"),
    ],
)
def test_private_semantic_inputs_change_cache_identity(field, value):
    item = workspace()
    before = cache_identity(item, "spec")
    setattr(item, field, value)
    assert before != cache_identity(item, "spec")


def test_snapshot_survives_nested_input_edits_and_json_roundtrip():
    item = workspace()
    frozen = snapshot_workspace(item, "plan")
    identity = source_identity(item, "plan")
    item.clarification_qa[0]["answer"] = "changed"
    item.stages[0].content = "FR-999: changed"
    restored = restore_workspace(json.loads(json.dumps(frozen)))
    assert source_identity(restored, "plan") == identity
    assert source_identity(item, "plan") != identity
    assert restored.stages[0].content == "FR-001: private"


def test_source_order_does_not_change_identity():
    item = workspace()
    item.stages.append(
        SimpleNamespace(type="plan", content="design", current_version=3)
    )
    identity = source_identity(item, "tasks")
    item.stages.reverse()
    assert identity == source_identity(item, "tasks")


@pytest.mark.parametrize(
    "wrap",
    [
        lambda text: f"```md\n{text}\n```",
        lambda text: f"~~~~md\n{text}\n~~~~",
        lambda text: f"<!--\n{text}\n-->",
        lambda text: "\n".join("> " + line for line in text.splitlines()),
        lambda text: "\n".join("    " + line for line in text.splitlines()),
    ],
)
def test_fake_headings_never_satisfy_gate(wrap):
    text = "\n".join(section_contract("spec", "standard"))
    with pytest.raises(MissingSectionError):
        validate_sections("spec", wrap(text))


def test_short_fence_does_not_end_long_fenced_block():
    text = "````md\n```\n## Fake\n````\n## Actual\nbody"
    assert [h for h, _, _ in headings(text)] == ["## Actual"]


def test_context_split_preserves_headings_inside_code():
    text = "## File Tree\n```md\n## Not a real section\n```\n## Files\nbody"
    assert [h for h, _ in _split_by_h2(text)] == ["## File Tree", "## Files"]


def test_oversized_requirement_section_fails_explicitly():
    with pytest.raises(ContextBudgetError, match="Functional Requirements"):
        _section_aware_injection(
            "spec", "## Functional Requirements\nFR-999 must survive\n" + "x" * 200_001
        )


def test_total_context_accounts_for_reserved_output_and_multibyte_input():
    with patch(
        "services.pipeline.context_budget.model_entry",
        return_value=SimpleNamespace(max_context_tokens=100),
    ):
        assert_context_fits("anthropic", "model", "ok", "ok", 20)
        with pytest.raises(ContextBudgetError):
            assert_context_fits("anthropic", "model", "ok", "漢" * 100, 20)


@pytest.mark.parametrize(
    "code",
    [
        "unbalanced_code_fence",
        "incomplete_harness_file_block",
        "invalid_task_dependency_order",
        "task_harness_ref_not_found",
    ],
)
def test_structural_failure_is_blocking_but_not_refundable(code):
    issue = CompletenessIssue(code=code, detail="broken contract")
    assert issue.is_refundable is False
    content = "\n".join(section_contract("spec", "standard"))
    with patch(
        "services.pipeline.artifact_validator.validate_artifact_completeness",
        side_effect=IncompleteArtifactError("spec", [issue]),
    ):
        with pytest.raises(StructuralReadinessError):
            validate_readiness("spec", content)


@pytest.mark.asyncio
async def test_unpinned_remote_name_never_fetches_latest(monkeypatch):
    monkeypatch.setattr(base.settings, "langfuse_prompt_pins", {})
    client = SimpleNamespace(get_prompt=AsyncMock(return_value="different body"))
    with patch.object(
        base.langfuse_service, "get_langfuse_client", return_value=client
    ):
        assert await base.load_prompt("stage", "local") == "local"
    client.get_prompt.assert_not_called()


@pytest.mark.asyncio
async def test_pinned_body_cannot_silently_fallback_or_change(monkeypatch):
    monkeypatch.setattr(
        base.settings,
        "langfuse_prompt_pins",
        {"stage": {"version": 7, "sha256": hashlib.sha256(b"reviewed").hexdigest()}},
    )
    client = SimpleNamespace(get_prompt=AsyncMock(return_value="unexpected"))
    with patch.object(
        base.langfuse_service, "get_langfuse_client", return_value=client
    ):
        with pytest.raises(ValueError, match="digest mismatch"):
            await base.load_prompt("stage", "local")
    client.get_prompt.assert_awaited_once_with("stage", version=7)


@pytest.mark.asyncio
async def test_remote_demo_prompt_keeps_environment_and_time_constraints():
    with patch.object(demo_day, "load_prompt", AsyncMock(return_value="Remote body")):
        prompt = await demo_day.get_system_prompt("spec", 300, True)
    assert demo_day._demo_day_directive(300, True) in prompt


def test_policy_warning_precedes_expiry_and_rejects_future_dates():
    from scripts.check_policy_freshness import check

    policy = {"last_reviewed": "2026-09-06", "max_age_days": 30}
    assert check(policy, date(2026, 9, 8)) is None
    assert "7 days" in check(policy, date(2026, 9, 29))
    assert "-1 days" in check(policy, date(2026, 10, 7))
    assert "future" in check(policy, date(2026, 9, 5))


def test_live_gate_rejects_missing_candidate_artifacts():
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "harness"))
    from prompt_eval.compare_live import compare

    candidate = {
        "corpus_sha256": "same",
        "cases": {"case": {"input": ["case", "problem", [], "standard"], "stages": {}}},
    }
    assert any("missing/failed" in issue for issue in compare(candidate, candidate))


def test_live_gate_rejects_wrong_corpus():
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "harness"))
    from prompt_eval.compare_live import compare

    assert compare(
        {"corpus_sha256": "a", "cases": {}}, {"corpus_sha256": "b", "cases": {}}
    )


@pytest.mark.asyncio
async def test_live_runner_uses_production_chunks_and_records_failed_attempts(
    monkeypatch, tmp_path
):
    import artifact_fixtures

    from services.llm import gateway
    from services.llm.completion import LLMCompletionInfo

    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "harness"))
    from prompt_eval import live

    class Adapter:
        last_completion = None

        async def stream(self, system, user, **kwargs):
            self.last_completion = LLMCompletionInfo(
                provider="anthropic",
                model="test",
                max_tokens=100,
                finish_reason="end_turn",
                usage={"input_tokens": 10, "output_tokens": 20},
            )
            yield artifact_fixtures.spec_stream_payload(user)

    monkeypatch.setattr(gateway, "get_llm", lambda *a, **k: Adapter())
    corpus = tmp_path / "corpus"
    case = corpus / "sample"
    case.mkdir(parents=True)
    (case / "problem_statement.md").write_text(
        "Build a web app for teams to track inventory."
    )
    (case / "clarification_qa.json").write_text("[]")
    output = tmp_path / "evidence.json"
    result = await live.generate(
        SimpleNamespace(
            code_root=Path(__file__).resolve().parents[1],
            corpus=corpus,
            modes="standard",
            max_provider_calls=4,
            output=output,
        )
    )
    stages = result["cases"]["sample/standard"]["stages"]
    assert stages["spec"]["content"]
    assert stages["spec"]["error"] is None
    assert stages["plan"]["error"]
    assert len(result["calls"]) == 4
    assert all(call["usage"]["output_tokens"] == 20 for call in result["calls"])
    assert all(call["system_sha256"] for call in result["calls"])
    assert result["corpus_sha256"] and result["code_sha256"]
    assert json.loads(output.read_text())["cases"]
