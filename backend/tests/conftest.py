import fnmatch
import sys
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]

if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


class FakeRedis:
    """Minimal in-memory async Redis double for unit tests.

    Implements the small surface the auth user-cache uses (get/set/delete/
    scan_iter) so cache-hit behaviour can be asserted deterministically without
    a live Redis. ``ex`` is accepted and ignored — tests do not exercise TTL
    expiry, only hit/miss/invalidate semantics.
    """

    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        return self._store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self._store[key] = value

    async def delete(self, *keys: str) -> int:
        removed = 0
        for key in keys:
            if key in self._store:
                del self._store[key]
                removed += 1
        return removed

    async def scan_iter(self, match: str | None = None):
        for key in list(self._store):
            if match is None or fnmatch.fnmatch(key, match):
                yield key


@pytest.fixture(autouse=True)
def _disable_llm_cost_ledger(monkeypatch):
    """Disable the Phase-0 cost-ledger DB write in unit tests.

    The write is fire-and-forget and fully exception-swallowed, but with no
    Postgres in the unit-test environment it would still attempt (and slowly
    time out) a real connection on every instrumented LLM call. Off by default
    here; ``test_llm_cost_ledger.py`` re-enables it against a stubbed session.
    """
    from config import settings

    monkeypatch.setattr(settings, "llm_cost_ledger_enabled", False, raising=False)


@pytest.fixture(autouse=True)
def _reset_prompt_cache():
    """Isolate the Langfuse prompt machinery between tests.

    Two process-globals leak a remote prompt body from one test into the next,
    making ``test_stage_system_prompt_has_stable_static_prefix`` pass alone but
    fail when ordered after a Langfuse-using test:

    1. ``prompts.base._PROMPT_CACHE`` — the app-level per-name TTL cache. A test
       that fetches a remote body seeds it, and the entry outlives the test.
    2. ``langfuse_service._INSTANCE`` — the process-wide client singleton. A
       test that configures Langfuse (e.g. ``test_langfuse_service``) builds a
       client whose own SDK TTL cache then serves bodies to later tests; it is
       only reset on the *next* such test's setup, not on teardown.

    Clearing the cache and dropping the singleton before and after each test
    means every test rebuilds the client from its own settings and falls back
    to the local prompt unless it explicitly stubs Langfuse.
    """
    from prompts import base as prompt_base
    from services import langfuse_service

    prompt_base._PROMPT_CACHE.clear()
    langfuse_service.reset_langfuse_client()
    yield
    prompt_base._PROMPT_CACHE.clear()
    langfuse_service.reset_langfuse_client()


@pytest.fixture(autouse=True)
def _reset_shared_redis():
    """Reset the module-level + singleton-cached Redis clients between tests.

    Two leaks across pytest-asyncio's per-test event loops
    (``asyncio_default_test_loop_scope=function``) must be cleared so a Redis
    client created on one test's loop is never reused on the next:

    1. ``database._shared_redis`` — the global injected by ``create_app`` /
       the lifespan (some tests inject a ``_NoopRedis`` lacking ``delete``).
    2. ``credit_service._redis`` — the ``CreditService`` singleton caches its
       own copy from the first ``get_shared_redis()`` call and never refreshes
       it; left set, a later test's ``credit_service.invalidate()`` ->
       ``redis.delete()`` runs against a connection bound to a now-closed loop
       and raises ``RuntimeError: Event loop is closed``.

    Both are reset to ``None`` after each test so the next test lazily rebuilds
    a fresh client on its own loop. H-2 — T-219 / Phase 21.
    """
    import database
    from services.credit_service import credit_service

    yield
    database._shared_redis = None
    credit_service._redis = None


@pytest.fixture(autouse=True)
def _pin_technology_policy_clock(monkeypatch):
    """Behavior tests use a fixed clock; scheduled freshness checks use real time."""
    from datetime import UTC, datetime

    from services.pipeline import tech_safety

    class ReviewDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 9, 6, tzinfo=UTC)
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)

    monkeypatch.setattr(tech_safety, "datetime", ReviewDateTime)
