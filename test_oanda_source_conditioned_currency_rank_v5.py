from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3

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
    assert manifest["comparison_arms"] == list(subject.ARMS)
    assert manifest["no_trade_baseline"]["contract_id"] == (
        subject.NO_TRADE_BASELINE_CONTRACT_ID
    )
    assert manifest["no_trade_baseline"]["quote_required"] is False
    assert manifest["no_trade_baseline"]["order_submitted"] is False
    assert manifest["no_trade_baseline"]["execution_eligible"] is False
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
    assert snapshot["comparison_arms"] == list(subject.ARMS)
    assert snapshot["no_trade_baseline_contract_id"] == (
        subject.NO_TRADE_BASELINE_CONTRACT_ID
    )
    assert snapshot["no_trade_baseline"]["value_pips"] == 0.0
    assert snapshot["no_trade_baseline"]["quote_required"] is False
    assert snapshot["no_trade_baseline"]["order_submitted"] is False
    assert snapshot["no_trade_baseline"]["can_place_orders"] is False
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


def test_supervisor_preserves_rank_v5_and_starts_rank_v7():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert '-Name "source_conditioned_currency_rank_v5_preserved"' in supervisor
    assert '-Needle "oanda_source_conditioned_currency_rank_v5.py"' in supervisor
    assert '-Name "source_conditioned_currency_rank_v7"' in supervisor
    assert "source_conditioned_currency_rank_v7.json" in supervisor
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


def test_no_trade_arm_is_append_only_zero_value_and_never_an_order(tmp_path):
    ledger = tmp_path / "rank_v5.sqlite"
    cutoff = datetime(2026, 8, 29, 13, 46, tzinfo=UTC)
    maturity = cutoff + timedelta(minutes=15)
    connection = subject.open_ledger(ledger)
    try:
        connection.execute(
            """
            INSERT INTO rank_decision (
              decision_id,market_episode_id,horizon_min,decision_cutoff_utc,
              source_database,source_contract_ids_json,source_cohort_ids_json,
              adapter_cohort_id,ticker_sha256,quotes_sha256,
              source_payload_json,decision_payload_json,research_only,
              execution_eligible,can_authorize,can_promote,contract_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?, ?,1,0,0,0,?)
            """,
            (
                "decision_no_trade_test",
                "episode_no_trade_test",
                15,
                cutoff.isoformat(),
                "source.sqlite",
                "[]",
                "[]",
                "rank_v5_test_cohort",
                "ticker_sha",
                "quotes_sha",
                "{}",
                "{}",
                subject.CONTRACT_ID,
            ),
        )
        connection.commit()

        first = subject.persist_no_trade_baselines(
            connection, observed_utc=cutoff
        )
        assert first == {"inserted_forecasts": 1, "inserted_outcomes": 0}
        forecast = connection.execute(
            """
            SELECT arm,predicted_gross_pips,predicted_after_cost_pips,action,
                   research_only,execution_eligible,can_place_orders,
                   can_authorize,can_promote,contract_id,payload_json
            FROM rank_v5_no_trade_forecast
            """
        ).fetchone()
        assert tuple(forecast[:10]) == (
            "no_trade",
            0.0,
            0.0,
            "no_trade",
            1,
            0,
            0,
            0,
            0,
            subject.NO_TRADE_BASELINE_CONTRACT_ID,
        )
        forecast_payload = json.loads(forecast[10])
        assert forecast_payload["quote_required"] is False
        assert forecast_payload["order_submitted"] is False

        evaluation = maturity + timedelta(seconds=30)
        second = subject.persist_no_trade_baselines(
            connection, observed_utc=evaluation
        )
        assert second == {"inserted_forecasts": 0, "inserted_outcomes": 1}
        outcome = connection.execute(
            """
            SELECT gross_pips,executable_after_cost_pips,realized_cost_pips,
                   order_submitted,action,research_only,execution_eligible,
                   can_place_orders,can_authorize,can_promote,contract_id,
                   payload_json
            FROM rank_v5_no_trade_outcome
            """
        ).fetchone()
        assert tuple(outcome[:11]) == (
            0.0,
            0.0,
            0.0,
            0,
            "no_trade",
            1,
            0,
            0,
            0,
            0,
            subject.NO_TRADE_BASELINE_CONTRACT_ID,
        )
        assert json.loads(outcome[11])["order_submitted"] is False
        assert connection.execute(
            "SELECT evaluated_utc FROM rank_v5_no_trade_outcome"
        ).fetchone()[0] == subject.v1.iso(evaluation)

        summary = subject.add_no_trade_summary(
            connection, subject.v1.ledger_summary(connection)
        )
        arm = summary["cohorts"][0]["arms"]
        arm = next(row for row in arm if row["arm"] == "no_trade")
        assert arm["forecasts"] == 1
        assert arm["outcomes"] == 1
        assert arm["mean_after_cost_pips"] == 0.0
        assert arm["zero_value_outcomes"] == 1

        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE rank_v5_no_trade_outcome SET order_submitted=1"
            )
    finally:
        connection.close()
