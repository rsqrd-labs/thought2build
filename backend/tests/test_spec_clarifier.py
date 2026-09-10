"""Unit tests for the Spec Clarification service (Phase 14 / T-162).

Covers:
  - Best-effort timeout behaviour (5-second wrap)
  - JSON-parse fallback to [] on malformed model output
  - Prompt-injection sanitisation on persisted answers
  - Round-key validation in persist_answers
  - That clarification is free (no credit deduction imports)
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from services.pipeline import spec_clarifier
from services.pipeline.spec_clarifier import (
    ClarificationValidationError,
    _parse_questions,
    persist_answers,
    request_clarifying_questions,
)


def _workspace(
    problem_statement: str = "Build a thing for a person.",
    clarification_qa: list[dict[str, str]] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        provider="anthropic",
        problem_statement=problem_statement,
        clarification_qa=clarification_qa,
    )


class _FakeRedis:
    """Minimal in-memory stand-in for the bits of Redis the clarifier touches."""

    def __init__(self) -> None:
        self.store: dict[str, tuple[int, str]] = {}

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self.store[key] = (ttl, value)

    async def get(self, key: str) -> str | None:
        entry = self.store.get(key)
        return entry[1] if entry else None

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)


def test_parse_questions_accepts_clean_json() -> None:
    raw = json.dumps(
        [
            {"question": "Who is the primary user?", "why_it_matters": "Drives FRs."},
            {"question": "What is the hard constraint?", "why_it_matters": "Scope."},
        ]
    )
    result = _parse_questions(raw)
    assert len(result) == 2
    assert result[0]["question"] == "Who is the primary user?"


def test_parse_questions_strips_markdown_fences() -> None:
    raw = (
        "Here are the questions:\n"
        '```json\n[{"question":"Q1","why_it_matters":"W1"}]\n```'
    )
    result = _parse_questions(raw)
    assert result == [{"question": "Q1", "why_it_matters": "W1"}]


def test_parse_questions_returns_empty_on_garbled_output() -> None:
    assert _parse_questions("") == []
    assert _parse_questions("not json at all") == []
    assert _parse_questions("[{not valid json}]") == []
    assert _parse_questions("[]") == []  # empty array, below min


def test_parse_questions_drops_malformed_entries() -> None:
    raw = json.dumps(
        [
            {"question": "ok", "why_it_matters": "ok"},
            {"question": ""},  # empty question — drop
            "not a dict",
            {"why_it_matters": "missing question"},  # missing field — drop
        ]
    )
    result = _parse_questions(raw)
    assert len(result) == 1
    assert result[0]["question"] == "ok"


def test_parse_questions_caps_at_five() -> None:
    raw = json.dumps(
        [{"question": f"Q{i}", "why_it_matters": f"W{i}"} for i in range(20)]
    )
    assert len(_parse_questions(raw)) == 5


@pytest.mark.asyncio
async def test_request_clarifying_questions_returns_empty_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An LLM call that exceeds the 5s timeout must fail-soft to []."""

    async def slow_complete(*args, **kwargs):
        await asyncio.sleep(0.5)
        return "ignored"

    # Pin the timeout extremely short so the test stays fast.
    monkeypatch.setattr(spec_clarifier, "_JUDGE_TIMEOUT_SECONDS", 0.01)
    fake_adapter = SimpleNamespace(complete=slow_complete)
    monkeypatch.setattr(spec_clarifier, "get_llm", lambda *a, **k: fake_adapter)

    redis = _FakeRedis()
    result = await request_clarifying_questions(_workspace(), redis)
    assert result == []
    # Nothing should have been cached.
    assert not redis.store


@pytest.mark.asyncio
async def test_request_clarifying_questions_swallows_provider_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def boom(*args, **kwargs):
        raise RuntimeError("provider exploded")

    fake_adapter = SimpleNamespace(complete=boom)
    monkeypatch.setattr(spec_clarifier, "get_llm", lambda *a, **k: fake_adapter)

    redis = _FakeRedis()
    result = await request_clarifying_questions(_workspace(), redis)
    assert result == []


@pytest.mark.asyncio
async def test_clarifier_compresses_long_statement_when_flag_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.llm.usage import estimate_tokens

    huge = "Background prose with no requirement. " * 5000  # ~25K tokens
    workspace = _workspace(huge)

    captured: dict[str, str] = {}

    async def capturing_complete(system_prompt: str, user_prompt: str, **kwargs):
        captured["user_prompt"] = user_prompt
        return json.dumps([{"question": "Who is the primary user?"}])

    fake_adapter = SimpleNamespace(complete=capturing_complete)
    monkeypatch.setattr(spec_clarifier, "get_llm", lambda *a, **k: fake_adapter)
    monkeypatch.setattr(spec_clarifier.settings, "problem_statement_compression", True)
    monkeypatch.setattr(spec_clarifier.settings, "problem_statement_budget_tokens", 500)

    class _Redis:
        store: dict = {}

        async def get(self, key):
            return None

        async def set(self, key, value, ex=0):
            return None

        async def setex(self, key, ttl, value):
            return None

    await request_clarifying_questions(workspace, _Redis())

    # The judge never sees the raw 25K-token paste; the statement is condensed.
    assert huge not in captured["user_prompt"]
    assert (estimate_tokens("anthropic", "x", captured["user_prompt"]) or 0) < 2000


@pytest.mark.asyncio
async def test_request_clarifying_questions_caches_questions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = json.dumps([{"question": "Who?", "why_it_matters": "Persona drives FRs."}])
    fake_adapter = SimpleNamespace(complete=AsyncMock(return_value=raw))
    monkeypatch.setattr(spec_clarifier, "get_llm", lambda *a, **k: fake_adapter)

    redis = _FakeRedis()
    workspace = _workspace()
    questions = await request_clarifying_questions(workspace, redis)
    assert len(questions) == 1
    cached_key = f"clarify_round:{workspace.id}"
    assert cached_key in redis.store
    ttl, payload = redis.store[cached_key]
    assert ttl == 900
    assert json.loads(payload) == ["Who?"]


@pytest.mark.asyncio
async def test_request_clarifying_questions_records_clarifier_prompt_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding #10 (process): the clarifier's InstrumentedAdapter call must
    thread SPEC_CLARIFICATION_PROMPT_VERSION, or telemetry records the adapter
    default prompt_version="local" and edits to spec_clarification.py stay
    invisible to the cost ledger — the exact defect the audit called out."""
    from prompts.spec_clarification import SPEC_CLARIFICATION_PROMPT_VERSION

    raw = json.dumps([{"question": "Who?", "why_it_matters": "Persona drives FRs."}])
    inner = SimpleNamespace(complete=AsyncMock(return_value=raw))
    monkeypatch.setattr(spec_clarifier, "get_llm", lambda *a, **k: inner)

    captured: dict[str, object] = {}

    def fake_instrumented(adapter, **kwargs):
        captured.update(kwargs)
        return inner

    monkeypatch.setattr(spec_clarifier, "InstrumentedAdapter", fake_instrumented)

    questions = await request_clarifying_questions(_workspace(), _FakeRedis())
    assert len(questions) == 1
    assert captured["prompt_version"] == SPEC_CLARIFICATION_PROMPT_VERSION
    assert captured["prompt_version"] != "local"


@pytest.mark.asyncio
async def test_persist_answers_rejects_unknown_question() -> None:
    workspace_id = uuid4()
    redis = _FakeRedis()
    await redis.setex(
        f"clarify_round:{workspace_id}", 900, json.dumps(["legitimate question"])
    )
    db = MagicMock()
    with pytest.raises(ClarificationValidationError):
        await persist_answers(
            workspace_id,
            [{"question": "smuggled", "answer": "payload"}],
            db,
            redis,
        )
    # Nothing should have been written to the database.
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_persist_answers_sanitises_html_in_answers() -> None:
    workspace_id = uuid4()
    redis = _FakeRedis()
    question = "What is the constraint?"
    await redis.setex(f"clarify_round:{workspace_id}", 900, json.dumps([question]))

    captured: dict = {}

    class _CapturingDB:
        async def execute(self, statement):
            captured.setdefault("statement", statement)
            captured.setdefault("statements", []).append(statement)

        async def commit(self):
            captured["committed"] = True

    db = _CapturingDB()
    await persist_answers(
        workspace_id,
        [
            {
                "question": question,
                "answer": "Latency under <script>alert(1)</script> 200ms.",
            }
        ],
        db,
        redis,
    )
    # The committed update statement has the values bound. Pull them out
    # via the SQLAlchemy parameters mapping rather than re-running it.
    compiled = captured["statement"].compile()
    params = compiled.params
    assert "clarification_qa" in params
    assert captured["statements"][1].compile().params["status"] == "stale"
    stored = params["clarification_qa"]
    assert isinstance(stored, list) and len(stored) == 1
    assert "<script>" not in stored[0]["answer"]
    assert "Latency under" in stored[0]["answer"]
    # And the round key is dropped after persistence.
    assert f"clarify_round:{workspace_id}" not in redis.store


@pytest.mark.asyncio
async def test_persist_existing_answers_uses_saved_questions_without_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_id = uuid4()
    question = "Who is the primary user?"
    workspace = SimpleNamespace(
        id=workspace_id,
        clarification_qa=[{"question": question, "answer": "Managers."}],
    )
    redis = _FakeRedis()
    captured: dict = {}

    async def fail_get_llm(*args, **kwargs):
        raise AssertionError("existing-mode persistence must not call the LLM")

    monkeypatch.setattr(spec_clarifier, "get_llm", fail_get_llm)

    class _CapturingDB:
        async def execute(self, statement):
            captured.setdefault("statement", statement)
            captured.setdefault("statements", []).append(statement)

        async def commit(self):
            captured["committed"] = True

    await persist_answers(
        workspace_id,
        [{"question": question, "answer": "Ops managers reviewing incidents."}],
        _CapturingDB(),
        redis,
        mode="existing",
        workspace=workspace,
    )

    params = captured["statement"].compile().params
    assert params["clarification_qa"] == [
        {
            "question": question,
            "answer": "Ops managers reviewing incidents.",
        }
    ]
    assert captured["committed"] is True
    assert redis.store == {}


@pytest.mark.asyncio
async def test_persist_existing_answers_rejects_unknown_question() -> None:
    workspace_id = uuid4()
    workspace = SimpleNamespace(
        id=workspace_id,
        clarification_qa=[{"question": "Known question?", "answer": "Known answer."}],
    )
    redis = _FakeRedis()
    db = MagicMock()

    with pytest.raises(ClarificationValidationError) as exc:
        await persist_answers(
            workspace_id,
            [{"question": "Smuggled question?", "answer": "payload"}],
            db,
            redis,
            mode="existing",
            workspace=workspace,
        )

    assert str(exc.value) == "question_not_in_existing_answers"
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_persist_existing_answers_requires_saved_answers() -> None:
    workspace_id = uuid4()
    workspace = SimpleNamespace(id=workspace_id, clarification_qa=[])
    redis = _FakeRedis()
    db = MagicMock()

    with pytest.raises(ClarificationValidationError) as exc:
        await persist_answers(
            workspace_id,
            [{"question": "Known question?", "answer": "Known answer."}],
            db,
            redis,
            mode="existing",
            workspace=workspace,
        )

    assert str(exc.value) == "clarification_answers_missing"
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_persist_existing_answers_sanitises_html_in_answers() -> None:
    workspace_id = uuid4()
    question = "What is the constraint?"
    workspace = SimpleNamespace(
        id=workspace_id,
        clarification_qa=[{"question": question, "answer": "Old answer."}],
    )
    redis = _FakeRedis()
    captured: dict = {}

    class _CapturingDB:
        async def execute(self, statement):
            captured.setdefault("statement", statement)
            captured.setdefault("statements", []).append(statement)

        async def commit(self):
            captured["committed"] = True

    await persist_answers(
        workspace_id,
        [
            {
                "question": question,
                "answer": "Keep latency under <script>alert(1)</script> 200ms.",
            }
        ],
        _CapturingDB(),
        redis,
        mode="existing",
        workspace=workspace,
    )

    stored = captured["statement"].compile().params["clarification_qa"]
    assert "<script>" not in stored[0]["answer"]
    assert "Keep latency under" in stored[0]["answer"]


@pytest.mark.asyncio
async def test_persist_answers_raises_when_round_expired() -> None:
    workspace_id = uuid4()
    redis = _FakeRedis()  # Empty — no round cached.
    db = MagicMock()
    with pytest.raises(ClarificationValidationError):
        await persist_answers(
            workspace_id,
            [{"question": "anything", "answer": "anything"}],
            db,
            redis,
        )


@pytest.mark.asyncio
async def test_persist_spec_clarification_passes_existing_mode_to_service() -> None:
    from fastapi import Response

    from routers.workspace import persist_spec_clarification
    from schemas.workspace import ClarifySubmitRequest

    workspace_id = uuid4()
    user = SimpleNamespace(id=uuid4())
    workspace = SimpleNamespace(
        id=workspace_id,
        user_id=user.id,
        clarification_qa=[{"question": "Known question?", "answer": "Old answer."}],
    )
    db = MagicMock()
    redis = _FakeRedis()

    with (pytest.MonkeyPatch.context() as monkeypatch,):
        get_mock = AsyncMock(return_value=workspace)
        persist_mock = AsyncMock()
        monkeypatch.setattr(
            "routers.workspace.workspace_service.get",
            get_mock,
        )
        monkeypatch.setattr(
            "routers.workspace.spec_clarifier.persist_answers",
            persist_mock,
        )

        response = await persist_spec_clarification(
            workspace_id,
            ClarifySubmitRequest(
                answers=[{"question": "Known question?", "answer": "Updated answer."}],
                mode="existing",
            ),
            user=user,
            db=db,
            redis=redis,
        )

    assert isinstance(response, Response)
    assert response.status_code == 204
    get_mock.assert_awaited_once_with(workspace_id, user.id, db)
    persist_mock.assert_awaited_once_with(
        workspace_id=workspace_id,
        answers=[{"question": "Known question?", "answer": "Updated answer."}],
        db=db,
        redis=redis,
        mode="existing",
        workspace=workspace,
    )


def test_spec_clarifier_module_has_no_credit_charges() -> None:
    """Hard contract: clarification is FREE; the file must not import or call any credit-deduction API."""  # noqa: E501
    from pathlib import Path

    source = (
        Path(__file__).resolve().parent.parent
        / "services"
        / "pipeline"
        / "spec_clarifier.py"
    ).read_text(encoding="utf-8")
    for forbidden in ("credit_service.deduct", "deduct_credits", "spend_credits"):
        assert (
            forbidden not in source
        ), f"spec_clarifier.py must NEVER call '{forbidden}'."
