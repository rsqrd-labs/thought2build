"""Contract tests for migration 0031 (scalability audit P2 — partial index).

The unit suite builds its schema with ``Base.metadata.create_all`` and never
drives Alembic against Postgres, so these pin the cheap-but-load-bearing facts
statically: the revision chain stays linear, the
index is PARTIAL on exactly the recovery sweep's predicate
(``status = 'in_progress'`` keyed by ``updated_at`` — recovery_service filters
``Stage.status == "in_progress", Stage.updated_at < cutoff``), it is
Postgres-only, and it is additive (``ix_stages_status`` is not dropped).
"""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path

_VERSIONS_DIR = Path(__file__).resolve().parents[1] / "migrations" / "versions"


def _load(module_name: str):
    return importlib.import_module(f"migrations.versions.{module_name}")


def _all_version_modules() -> list:
    modules = []
    for info in pkgutil.iter_modules([str(_VERSIONS_DIR)]):
        modules.append(_load(info.name))
    return modules


def test_0031_revises_0030_and_history_is_linear() -> None:
    modules = _all_version_modules()
    by_revision = {m.revision: m for m in modules}
    assert "0031" in by_revision, "migration 0031 must exist"
    assert by_revision["0031"].down_revision == "0030"

    revised = {m.down_revision for m in modules if m.down_revision}
    heads = [m.revision for m in modules if m.revision not in revised]
    # The single head advances as migrations are added (0045 adds the
    # eval_results judge-provenance columns). The guard is that there
    # is exactly ONE head — a branched history breaks `alembic upgrade head` on
    # deploy.
    assert heads == ["0048"], (
        f"Expected a single migration head (0048), got {heads!r} — a branched "
        "history breaks `alembic upgrade head` on deploy."
    )


def test_0031_builds_a_postgres_only_partial_index_on_the_sweep_predicate() -> None:
    source = (_VERSIONS_DIR / "0031_stages_in_progress_partial_index.py").read_text()
    # Partial on exactly the sweep's filter, keyed by its range column.
    assert "postgresql_where" in source
    assert "status = 'in_progress'" in source
    assert '["updated_at"]' in source
    # Non-postgres test backends must skip cleanly (both directions).
    assert source.count('!= "postgresql"') == 2
    # Additive: the full status index still serves the other status filters.
    assert 'drop_index("ix_stages_status"' not in source


def test_0031_upgrade_and_downgrade_are_noops_off_postgres(monkeypatch) -> None:
    """The sqlite/unit path must return before touching alembic.op."""
    migration = _load("0031_stages_in_progress_partial_index")

    class _SqliteDialect:
        name = "sqlite"

    class _SqliteBind:
        dialect = _SqliteDialect()

    monkeypatch.setattr(migration.op, "get_bind", lambda: _SqliteBind())

    def _explode(*a, **kw):  # pragma: no cover - failure path only
        raise AssertionError("op.create_index/drop_index must not run off postgres")

    monkeypatch.setattr(migration.op, "create_index", _explode, raising=False)
    monkeypatch.setattr(migration.op, "drop_index", _explode, raising=False)

    migration.upgrade()
    migration.downgrade()
