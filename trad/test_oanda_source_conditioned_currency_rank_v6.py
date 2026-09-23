from __future__ import annotations

import ast
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

import pytest

import oanda_source_conditioned_currency_rank_v5 as v5
import oanda_source_conditioned_currency_rank_v6 as subject


UTC = timezone.utc


def _inventory_database(path: Path, rows: list[dict]) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE source_factor_forecast(
          forecast_id TEXT PRIMARY KEY,
          factor_observation_id TEXT NOT NULL,
          canonical_event_id TEXT NOT NULL,
          currency TEXT NOT NULL,
          horizon_min INTEGER NOT NULL,
          issued_utc TEXT NOT NULL,
          forecast_state TEXT NOT NULL,
          abstain_reason TEXT NOT NULL,
          prospective_proof_eligible INTEGER NOT NULL,
          probability_strengthening REAL,
          predicted_currency_factor_bps REAL,
          predicted_absolute_factor_bps REAL,
          contract_id TEXT NOT NULL
        )
        """
    )
    for index, row in enumerate(rows):
        connection.execute(
            """
            INSERT INTO source_factor_forecast VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                row.get("forecast_id", f"forecast_{index}"),
                row.get("factor_observation_id", f"factor_{index}"),
                row.get("canonical_event_id", f"event_{index}"),
                row.get("currency", "JPY"),
                row.get("horizon_min", 60),
                row["issued_utc"],
                row.get("forecast_state", "abstain"),
                row.get("abstain_reason", "low_effective_n:1<8"),
                int(row.get("prospective_proof_eligible", 0)),
                row.get("probability_strengthening"),
                row.get("predicted_currency_factor_bps"),
                row.get("predicted_absolute_factor_bps"),
                row.get("contract_id", subject.REQUIRED_SOURCE_CONTRACT_ID),
            ),
        )
    connection.commit()
    connection.close()


def test_v6_adapter_is_separate_v7_only_contract_and_manifest():
    manifest = json.loads(subject.DEFAULT_MANIFEST.read_text(encoding="utf-8"))
    assert manifest["contract_id"] == subject.CONTRACT_ID
    assert manifest["base_cohort_id"] == subject.BASE_COHORT_ID
    assert manifest["source"]["input_mode"] == subject.INPUT_MODE
    assert manifest["source"]["required_contract_id"] == (
        subject.REQUIRED_SOURCE_CONTRACT_ID
    )
    assert manifest["source"]["sealed_v1_v2_v3_v4_v5_v6_fallback_allowed"] is False
    assert manifest["source"]["historical_rows_imported"] is False
    assert manifest["parent_adapter_contract_id"] == v5.CONTRACT_ID
    assert manifest["comparison_arms"] == list(subject.ARMS)
    assert manifest["no_trade_baseline"]["contract_id"] == (
        subject.NO_TRADE_BASELINE_CONTRACT_ID
    )
    assert subject.PARENT_CONTRACT_ID == v5.CONTRACT_ID
    assert subject.DEFAULT_LEDGER != v5.DEFAULT_LEDGER
    assert subject.DEFAULT_STATE != v5.DEFAULT_STATE


def test_default_source_is_v7_only_and_missing_never_falls_back(tmp_path, monkeypatch):
    missing_v7 = tmp_path / "missing_v7.sqlite"
    old_v6 = tmp_path / "causal_source_factor_response_map_v6.sqlite"
    old_v6.touch()
    monkeypatch.setattr(subject, "SOURCE_V7", missing_v7)
    assert subject.resolve_source_database() == missing_v7.resolve()
    assert subject.resolve_source_database().exists() is False
    assert subject.resolve_source_database(old_v6) == old_v6.resolve()


def test_default_missing_v7_cycle_fails_closed(tmp_path, monkeypatch):
    missing_v7 = tmp_path / "missing_v7.sqlite"
    monkeypatch.setattr(subject, "SOURCE_V7", missing_v7)
    snapshot = subject.run_cycle(
        ticker_path=tmp_path / "ticker.json",
        quotes_path=tmp_path / "quotes.json",
        quote_bars_database=tmp_path / "bars.sqlite",
        ledger_path=tmp_path / "rank_v6.sqlite",
        state_path=tmp_path / "rank_v6.json",
        report_path=tmp_path / "rank_v6.md",
        observed_utc=datetime(2026, 9, 1, 2, 31, tzinfo=UTC),
    )
    assert snapshot["source_database"] == str(missing_v7.resolve())
    assert snapshot["source_input_status"] == "missing_required_v7_fail_closed"
    assert snapshot["source_forecast_rows"] == 0
    assert snapshot["source_forecast_inventory"]["status"] == "missing_database"
    assert snapshot["source_forecast_inventory"]["total_rows"] == 0
    assert snapshot["new_decisions"] == 0
    assert snapshot["policy"]["v1_v2_v3_v4_v5_v6_source_fallback"] is False
    assert snapshot["comparison_arms"] == list(subject.ARMS)
    assert snapshot["supported_execution_decision"] == "no_trade"


def test_default_v7_contract_contamination_fails_closed(tmp_path, monkeypatch):
    source = tmp_path / "v7.sqlite"
    source.touch()
    monkeypatch.setattr(subject, "SOURCE_V7", source)
    monkeypatch.setattr(
        subject,
        "_BASE_LOAD_SOURCE_FORECASTS",
        lambda path: [{"source_contract_id": "sealed_v6_contract"}],
    )
    with pytest.raises(ValueError, match="contract contamination"):
        subject.load_source_forecasts(source)


def test_inventory_separates_produced_abstentions_from_rank_eligible_rows(
    tmp_path: Path,
) -> None:
    source = tmp_path / "v7.sqlite"
    _inventory_database(
        source,
        [
            {
                "issued_utc": "2026-09-01T02:29:00+00:00",
                "forecast_state": "abstain",
                "abstain_reason": "low_effective_n:1<8",
            },
            {
                "issued_utc": "2026-09-01T02:30:00+00:00",
                "currency": "NOK",
                "forecast_state": "forecast",
                "abstain_reason": "",
                "prospective_proof_eligible": 1,
                "probability_strengthening": 0.7,
                "predicted_currency_factor_bps": 2.0,
                "predicted_absolute_factor_bps": 2.0,
            },
            {
                "issued_utc": "2026-09-01T02:33:00+00:00",
                "forecast_state": "abstain",
                "abstain_reason": "future_row",
            },
        ],
    )
    inventory = subject.source_forecast_inventory(
        source, cutoff_utc=datetime(2026, 9, 1, 2, 31, tzinfo=UTC)
    )
    assert inventory["count_basis"] == (
        "issued_utc_at_or_before_adapter_generated_utc"
    )
    assert inventory["total_rows"] == 2
    assert inventory["rank_eligible_rows"] == 1
    assert inventory["abstain_rows"] == 1
    assert inventory["excluded_from_rank_rows"] == 1
    assert inventory["distinct_event_count"] == 2
    assert inventory["currency_counts"] == {"JPY": 1, "NOK": 1}
    assert inventory["abstain_reason_counts"] == {"low_effective_n:1<8": 1}
    assert inventory["status"] == "rank_eligible_rows_available"
    assert inventory["database_integrity"] == "ok"


def test_live_inventory_checks_all_rows_for_contract_contamination(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "v7.sqlite"
    _inventory_database(
        source,
        [
            {
                "issued_utc": "2026-09-01T02:29:00+00:00",
                "contract_id": "foreign_abstain_contract",
            }
        ],
    )
    monkeypatch.setattr(subject, "SOURCE_V7", source)
    with pytest.raises(ValueError, match="inventory contamination"):
        subject.source_forecast_inventory(
            source, cutoff_utc=datetime(2026, 9, 1, 2, 31, tzinfo=UTC)
        )


def test_supervisor_runs_rank_v6_baseline_and_rank_v7_overlay():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert '-Name "source_conditioned_currency_rank_v7"' in supervisor
    assert '-Needle "oanda_source_conditioned_currency_rank_v7.py"' in supervisor
    assert "source_conditioned_currency_rank_v7.json" in supervisor
    assert '-Name "source_conditioned_currency_rank_v6"' in supervisor
    assert '-Needle "oanda_source_conditioned_currency_rank_v6.py"' in supervisor
    assert '-Name "source_conditioned_currency_rank_v5_preserved"' in supervisor
    assert "frozen baseline while V7/V8 measures" in supervisor


def test_v6_adapter_has_no_execution_or_broker_import_surface():
    tree = ast.parse(Path(subject.__file__).read_text(encoding="utf-8"))
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    forbidden = (
        "executor", "execution", "broker", "signal_feed", "lifecycle", "authorization"
    )
    assert not any(any(word in name for word in forbidden) for name in imported)
