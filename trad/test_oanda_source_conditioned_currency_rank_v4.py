from __future__ import annotations

import ast
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

import oanda_source_conditioned_currency_rank_v1 as v1
import oanda_source_conditioned_currency_rank_v3 as v3
import oanda_source_conditioned_currency_rank_v4 as subject


UTC = timezone.utc


def test_v4_adapter_is_separate_v5_only_contract_and_manifest():
    manifest = json.loads(subject.DEFAULT_MANIFEST.read_text(encoding="utf-8"))
    assert manifest["contract_id"] == subject.CONTRACT_ID
    assert manifest["base_cohort_id"] == subject.BASE_COHORT_ID
    assert manifest["source"]["input_mode"] == subject.INPUT_MODE
    assert manifest["source"]["required_contract_id"] == (
        subject.REQUIRED_SOURCE_CONTRACT_ID
    )
    assert manifest["source"]["sealed_v1_v2_v3_v4_fallback_allowed"] is False
    assert manifest["source"]["historical_rows_imported"] is False
    assert manifest["parent_adapter_contract_id"] == v3.CONTRACT_ID
    assert subject.PARENT_CONTRACT_ID == v3.CONTRACT_ID
    assert subject.DEFAULT_LEDGER != v1.DEFAULT_LEDGER
    assert subject.DEFAULT_STATE != v1.DEFAULT_STATE
    assert subject.DEFAULT_REPORT != v1.DEFAULT_REPORT


def test_default_source_is_v5_only_and_missing_v5_never_falls_back(
    tmp_path, monkeypatch
):
    missing_v5 = tmp_path / "missing_v5.sqlite"
    old_v4 = tmp_path / "causal_source_factor_response_map_v4.sqlite"
    old_v4.touch()
    monkeypatch.setattr(subject, "SOURCE_V5", missing_v5)
    assert subject.resolve_source_database() == missing_v5.resolve()
    assert subject.resolve_source_database().exists() is False
    assert subject.resolve_source_database(old_v4) == old_v4.resolve()


def test_default_missing_v5_cycle_fails_closed(tmp_path, monkeypatch):
    missing_v5 = tmp_path / "missing_v5.sqlite"
    monkeypatch.setattr(subject, "SOURCE_V5", missing_v5)
    snapshot = subject.run_cycle(
        ticker_path=tmp_path / "ticker.json",
        quotes_path=tmp_path / "quotes.json",
        quote_bars_database=tmp_path / "bars.sqlite",
        ledger_path=tmp_path / "rank_v4.sqlite",
        state_path=tmp_path / "rank_v4.json",
        report_path=tmp_path / "rank_v4.md",
        observed_utc=datetime(2026, 8, 28, 17, 1, tzinfo=UTC),
    )
    assert snapshot["contract_id"] == subject.CONTRACT_ID
    assert snapshot["source_database"] == str(missing_v5.resolve())
    assert snapshot["source_database_mode"] == "required_v5_default"
    assert snapshot["source_input_status"] == "missing_required_v5_fail_closed"
    assert snapshot["source_forecast_rows"] == 0
    assert snapshot["new_decisions"] == 0
    assert snapshot["policy"]["v1_v2_v3_v4_source_fallback"] is False
    assert snapshot["supported_execution_decision"] == "no_trade"


def test_default_v5_contract_contamination_fails_closed(tmp_path, monkeypatch):
    source = tmp_path / "v5.sqlite"
    source.touch()
    monkeypatch.setattr(subject, "SOURCE_V5", source)
    monkeypatch.setattr(
        subject,
        "_V1_LOAD_SOURCE_FORECASTS",
        lambda path: [{"source_contract_id": "sealed_v4_contract"}],
    )
    with pytest.raises(ValueError, match="contract contamination"):
        subject.load_source_forecasts(source)


def test_supervisor_preserves_rank_v4_and_starts_rank_v7():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert '-Name "source_conditioned_currency_rank_v4_preserved"' in supervisor
    assert '-Needle "oanda_source_conditioned_currency_rank_v4.py"' in supervisor
    assert '-Name "source_conditioned_currency_rank_v7"' in supervisor
    assert "source_conditioned_currency_rank_v7.json" in supervisor
    assert '-Name "source_conditioned_currency_rank_v3_preserved"' in supervisor
    assert '-Needle "oanda_source_conditioned_currency_rank_v3.py"' in supervisor
    assert "v6_source_input_adapter_cutover" in supervisor


def test_v4_adapter_has_no_execution_or_broker_import_surface():
    tree = ast.parse(Path(subject.__file__).read_text(encoding="utf-8"))
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    forbidden = (
        "executor",
        "execution",
        "broker",
        "signal_feed",
        "lifecycle",
        "authorization",
    )
    assert not any(any(word in name for word in forbidden) for name in imported)
