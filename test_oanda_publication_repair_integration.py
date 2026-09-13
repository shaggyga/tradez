"""Verify the integration distinguishes frozen records and disabled collection."""
import ast
from pathlib import Path

import oanda_project_integrity_audit as audit
from oanda_source_publication_successor_integrity import check_successor_readiness


def test_retired_inventory_uses_its_own_clock_but_has_no_live_authority(monkeypatch, tmp_path):
    calls = {}
    def reconcile(state, **kwargs):
        calls.update(kwargs)
        return {"ok": True, "total_rows": 903, "rank_eligible_rows": 0}
    monkeypatch.setattr(audit, "source_conditioned_rank_v7_inventory_integrity", reconcile)
    state = {"generated_utc": "2026-09-05T01:00:00+00:00"}
    result = audit.retired_source_rank_v7_diagnostics(state, source_database=tmp_path / "frozen.sqlite")
    assert calls["cutoff_epoch"] == audit.parse_epoch(state["generated_utc"])
    assert result["ok"] is True
    assert result["operationally_ready"] is False
    assert result["live_freshness_assessed"] is False
    assert result["prospective_performance_verified"] is False


def test_disabled_successor_readiness_opens_no_database(monkeypatch):
    import oanda_source_publication_successor_integrity as checker
    def forbidden(*args, **kwargs):
        raise AssertionError("disabled successors must not touch runtime DBs")
    monkeypatch.setattr(checker.sqlite3, "connect", forbidden)
    result = check_successor_readiness()
    assert result["ok"] is True
    assert result["status"] == "inactive"
    assert result["operationally_ready"] is False
    assert result["prospective_performance_verified"] is False


def test_main_audit_wires_successor_disposition_separately_from_frozen_counts():
    source = Path(audit.__file__).read_text(encoding="utf-8")
    parsed = ast.parse(source)
    run = next(node for node in parsed.body if isinstance(node, ast.FunctionDef) and node.name == "run")
    text = ast.get_source_segment(source, run)
    assert '"source_publication_successors_disposition_valid_and_inert"' in text
    assert '"retired_source_rank_v7_inventory_preserved_and_inert"' in text
    assert 'check_successor_readiness(now_utc=generated)' in text
