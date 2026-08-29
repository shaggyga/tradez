from __future__ import annotations

import ast
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

import oanda_source_conditioned_currency_rank_v4 as v4
import oanda_source_conditioned_currency_rank_v5 as subject


UTC = timezone.utc


def test_v5_adapter_is_separate_v6_only_contract_and_manifest():
    manifest = json.loads(subject.DEFAULT_MANIFEST.read_text(encoding="utf-8"))
    assert manifest["contract_id"] == subject.CONTRACT_ID
    assert manifest["base_cohort_id"] == subject.BASE_COHORT_ID
    assert manifest["source"]["input_mode"] == subject.INPUT_MODE
    assert manifest["source"]["required_contract_id"] == (
        subject.REQUIRED_SOURCE_CONTRACT_ID
    )
    assert manifest["source"]["sealed_v1_v2_v3_v4_v5_fallback_allowed"] is False
    assert manifest["source"]["historical_rows_imported"] is False
    assert manifest["parent_adapter_contract_id"] == v4.CONTRACT_ID
    assert subject.PARENT_CONTRACT_ID == v4.CONTRACT_ID
    assert subject.DEFAULT_LEDGER != v4.DEFAULT_LEDGER
    assert subject.DEFAULT_STATE != v4.DEFAULT_STATE


def test_default_source_is_v6_only_and_missing_never_falls_back(tmp_path, monkeypatch):
    missing_v6 = tmp_path / "missing_v6.sqlite"
    old_v5 = tmp_path / "causal_source_factor_response_map_v5.sqlite"
    old_v5.touch()
    monkeypatch.setattr(subject, "SOURCE_V6", missing_v6)
    assert subject.resolve_source_database() == missing_v6.resolve()
    assert subject.resolve_source_database().exists() is False
    assert subject.resolve_source_database(old_v5) == old_v5.resolve()


def test_default_missing_v6_cycle_fails_closed(tmp_path, monkeypatch):
    missing_v6 = tmp_path / "missing_v6.sqlite"
    monkeypatch.setattr(subject, "SOURCE_V6", missing_v6)
    snapshot = subject.run_cycle(
        ticker_path=tmp_path / "ticker.json",
        quotes_path=tmp_path / "quotes.json",
        quote_bars_database=tmp_path / "bars.sqlite",
        ledger_path=tmp_path / "rank_v5.sqlite",
        state_path=tmp_path / "rank_v5.json",
        report_path=tmp_path / "rank_v5.md",
        observed_utc=datetime(2026, 8, 28, 17, 31, tzinfo=UTC),
    )
    assert snapshot["source_database"] == str(missing_v6.resolve())
    assert snapshot["source_input_status"] == "missing_required_v6_fail_closed"
    assert snapshot["source_forecast_rows"] == 0
    assert snapshot["new_decisions"] == 0
    assert snapshot["policy"]["v1_v2_v3_v4_v5_source_fallback"] is False
    assert snapshot["supported_execution_decision"] == "no_trade"


def test_default_v6_contract_contamination_fails_closed(tmp_path, monkeypatch):
    source = tmp_path / "v6.sqlite"
    source.touch()
    monkeypatch.setattr(subject, "SOURCE_V6", source)
    monkeypatch.setattr(
        subject,
        "_V1_LOAD_SOURCE_FORECASTS",
        lambda path: [{"source_contract_id": "sealed_v5_contract"}],
    )
    with pytest.raises(ValueError, match="contract contamination"):
        subject.load_source_forecasts(source)


def test_supervisor_starts_rank_v5_and_retires_rank_v4():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert '-Name "source_conditioned_currency_rank_v5"' in supervisor
    assert '-Needle "oanda_source_conditioned_currency_rank_v5.py"' in supervisor
    assert "source_conditioned_currency_rank_v5.json" in supervisor
    assert '-Name "source_conditioned_currency_rank_v4_preserved"' in supervisor
    assert '-Needle "oanda_source_conditioned_currency_rank_v4.py"' in supervisor
    assert "v6_source_input_adapter_cutover" in supervisor


def test_v5_adapter_has_no_execution_or_broker_import_surface():
    tree = ast.parse(Path(subject.__file__).read_text(encoding="utf-8"))
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    forbidden = ("executor", "execution", "broker", "signal_feed", "lifecycle", "authorization")
    assert not any(any(word in name for word in forbidden) for name in imported)
