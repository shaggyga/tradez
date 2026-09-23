from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path

import pytest

from trad import oanda_news_outcome_improvement_audit as audit


UTC = dt.timezone.utc


def event_row(
    instrument: str,
    *,
    topic: str = "topic",
    category: str = "inflation_context",
    source: str = "TradingView",
    spread: float = 2.0,
    executable: float = -1.0,
    hit: bool = True,
    beat: bool = False,
    result: str = "right_too_small_for_spread",
    direction: str = "short",
) -> dict:
    return {
        "decision_id": f"{topic}-{instrument}",
        "decided_utc": "2026-08-26T01:00:00+00:00",
        "outcome_utc": "2026-08-26T02:00:00+00:00",
        "topic_id": topic,
        "instrument": instrument,
        "direction": direction,
        "horizon_min": 60,
        "headline": "UK inflation expectations rise after recent falls",
        "category": category,
        "source_name": source,
        "entry_spread_pips": spread,
        "status": "matured",
        "executable_pips": executable,
        "direction_hit": int(hit),
        "beat_spread": int(beat),
        "result_class": result,
        "relationship": "neutral",
    }


def test_correlated_event_fanout_is_one_thesis_and_cost_aware() -> None:
    instruments = [
        "GBP_USD",
        "GBP_JPY",
        "GBP_CHF",
        "GBP_CAD",
        "GBP_AUD",
        "GBP_SGD",
        "GBP_NZD",
        "GBP_HKD",
        "GBP_PLN",
        "GBP_ZAR",
    ]
    rows = []
    for index, instrument in enumerate(instruments):
        rows.append(
            event_row(
                instrument,
                spread=1.7 + index if index < 5 else 18.0 + index,
                executable=-3.0 - index,
                hit=index < 5,
            )
        )
    diagnosis = audit.event_diagnosis(rows, stream="accepted_news_decision")
    assert diagnosis["raw_pair_n"] == 10
    assert diagnosis["effective_thesis_n"] == 1
    assert diagnosis["verdict"] == "direction_right_magnitude_below_cost"
    assert "economic_sign_ambiguous" in diagnosis["reason_codes"]
    assert diagnosis["primary_after_cost_win_rate"] == 0.0
    assert diagnosis["representative"]["instrument"] == "GBP_USD"


def test_jpy_event_win_is_not_ten_independent_wins() -> None:
    instruments = [
        "USD_JPY",
        "EUR_JPY",
        "GBP_JPY",
        "CHF_JPY",
        "CAD_JPY",
        "NZD_JPY",
        "SGD_JPY",
        "AUD_JPY",
        "ZAR_JPY",
        "HKD_JPY",
    ]
    rows = []
    for index, instrument in enumerate(instruments):
        won = index not in {7, 8}
        rows.append(
            event_row(
                instrument,
                topic="jpy-topic",
                category="inflation_release",
                source="Investing.com",
                spread=1.5 + 0.3 * index if index < 9 else 24.5,
                executable=22.0 if won else -2.0,
                hit=index != 7,
                beat=won,
                result="captured_after_cost" if won else "wrong_direction",
            )
        )
    diagnosis = audit.event_diagnosis(rows, stream="accepted_news_decision")
    assert diagnosis["verdict"] == "event_after_cost_win"
    assert diagnosis["raw_pair_n"] == 10
    assert diagnosis["effective_thesis_n"] == 1
    assert diagnosis["dominant_signed_currency_factor"] == "JPY+"


def test_profitable_rejected_direction_becomes_shadow_mapping_gap() -> None:
    rows = []
    for instrument in ("AUD_USD", "AUD_JPY", "EUR_AUD"):
        row = event_row(
            instrument,
            topic="rejected-aud",
            category="inflation_release",
            spread=2.0,
            executable=5.0,
            beat=True,
            result="missed_after_cost",
            direction="long",
        )
        row.update(
            {
                "rejection_reason": "no_forward_pair_direction",
                "decision_kind": "prospective_gate_counterfactual",
            }
        )
        rows.append(row)
    diagnosis = audit.rejected_diagnosis(rows)
    assert diagnosis["verdict"] == "profitable_rejected_shadow"
    assert diagnosis["prospective_miss_counted"] is True
    assert diagnosis["reason_codes"] == ["directional_mapping_gap"]


def test_retrospective_rejection_never_counts_as_prospective_miss() -> None:
    row = event_row(
        "USD_JPY", executable=10.0, beat=True, result="post_recap_continuation"
    )
    row.update(
        {
            "rejection_reason": "retrospective_market_move",
            "decision_kind": "retrospective_continuation_counterfactual",
        }
    )
    diagnosis = audit.rejected_diagnosis([row])
    assert diagnosis["verdict"] == "retrospective_followthrough_diagnostic"
    assert diagnosis["prospective_miss_counted"] is False
    assert diagnosis["reason_codes"] == []


def test_mover_giveback_and_stale_context_create_decay_diagnosis() -> None:
    case = {
        "factor_episode_id": "factor-one",
        "factor_representative": True,
        "observation_utc": "2026-08-26T03:00:00+00:00",
        "pre_move_event_count": 1,
        "explanation_state": "research_context_aligned_not_publishable",
        "recent_context_stories": [
            {"effective_from_utc": "2026-08-26T01:00:00+00:00"}
        ],
    }
    rows = [
        {
            "outcome_id": "broad",
            "case_id": "case",
            "arm": "broad_context_direction",
            "horizon_min": 60,
            "after_cost_pips": -5.0,
            "mfe_pips": 2.0,
            "mae_pips": -8.0,
            "factor_episode_id": "factor-one",
            "observation_utc": case["observation_utc"],
            "instrument": "USD_HUF",
            "case": case,
        },
        {
            "outcome_id": "technical",
            "case_id": "case",
            "arm": "technical_continuation",
            "horizon_min": 60,
            "after_cost_pips": -3.0,
            "mfe_pips": 4.0,
            "mae_pips": -7.0,
            "factor_episode_id": "factor-one",
            "observation_utc": case["observation_utc"],
            "instrument": "USD_HUF",
            "case": case,
        },
    ]
    diagnosis = audit.mover_diagnosis(rows)
    assert diagnosis["verdict"] == "mover_followthrough_miss_or_gap"
    assert "stale_context_after_reversal" in diagnosis["reason_codes"]
    assert "giveback_or_reversal" in diagnosis["reason_codes"]
    assert "strict_signal_absence" in diagnosis["reason_codes"]


def create_input_databases(signal_path, mover_path) -> None:
    with sqlite3.connect(signal_path) as connection:
        connection.executescript(
            """
            CREATE TABLE independent_news_decisions (
              decision_id TEXT PRIMARY KEY, decided_utc TEXT, topic_id TEXT,
              instrument TEXT, direction TEXT, confidence REAL,
              horizon_min INTEGER, headline TEXT, category TEXT,
              source_name TEXT, source_first_seen_utc TEXT,
              availability_lag_minutes REAL, forward_signal_timely INTEGER,
              reports_prior_market_move INTEGER, entry_bid REAL, entry_ask REAL,
              entry_mid REAL, pip REAL, entry_spread_pips REAL, status TEXT,
              outcome_utc TEXT, gross_pips REAL, executable_pips REAL,
              direction_hit INTEGER, beat_spread INTEGER, result_class TEXT,
              payload_json TEXT
            );
            CREATE TABLE rejected_news_shadows (
              decision_id TEXT PRIMARY KEY, decided_utc TEXT, topic_id TEXT,
              instrument TEXT, direction TEXT, confidence REAL,
              horizon_min INTEGER, headline TEXT, category TEXT,
              source_name TEXT, source_first_seen_utc TEXT,
              rejection_reason TEXT, decision_kind TEXT, entry_bid REAL,
              entry_ask REAL, entry_mid REAL, pip REAL,
              entry_spread_pips REAL, status TEXT, outcome_utc TEXT,
              gross_pips REAL, executable_pips REAL, direction_hit INTEGER,
              beat_spread INTEGER, result_class TEXT, payload_json TEXT
            );
            CREATE TABLE observations (
              observed_utc TEXT, instrument TEXT, relationship TEXT,
              signal_direction TEXT, signal_confidence REAL,
              signal_expected_net_pips REAL, signal_eligible INTEGER
            );
            """
        )
        connection.execute(
            """
            INSERT INTO independent_news_decisions VALUES (
              'd1','2026-08-26T01:00:00+00:00','topic','GBP_USD','short',0.6,
              60,'Inflation expectations rise','inflation_context','TradingView',
              '2026-08-26T00:58:00+00:00',2,1,0,1.3,1.3002,1.3001,0.0001,
              2,'matured','2026-08-26T02:00:00+00:00',1,-3,1,0,
              'right_too_small_for_spread','{}'
            )
            """
        )
        connection.commit()
    with sqlite3.connect(mover_path) as connection:
        connection.executescript(
            """
            CREATE TABLE mover_cases (
              case_id TEXT PRIMARY KEY, first_recorded_utc TEXT,
              instrument TEXT, start_utc TEXT, end_utc TEXT, case_json TEXT
            );
            CREATE TABLE mover_case_outcomes (
              outcome_id TEXT PRIMARY KEY, matured_utc TEXT, case_id TEXT,
              case_contract_id TEXT, arm TEXT, horizon_min INTEGER, side INTEGER,
              target_utc TEXT, candle_utc TEXT, after_cost_pips REAL,
              outcome_json TEXT
            );
            """
        )
        connection.commit()


def test_run_is_idempotent_read_only_and_append_only(tmp_path) -> None:
    signal = tmp_path / "signal.sqlite"
    mover = tmp_path / "mover.sqlite"
    output_db = tmp_path / "audit.sqlite"
    create_input_databases(signal, mover)
    paths = {
        "database": output_db,
        "output": tmp_path / "state.json",
        "queue_path": tmp_path / "queue.json",
        "report": tmp_path / "report.md",
        "log_path": tmp_path / "audit.jsonl",
    }
    before = sqlite3.connect(signal).execute(
        "SELECT COUNT(*) FROM independent_news_decisions"
    ).fetchone()[0]
    now = dt.datetime(2026, 8, 26, 3, 0, tzinfo=UTC)
    first = audit.run_once(
        signal_database=signal, mover_database=mover, now=now, **paths
    )
    second = audit.run_once(
        signal_database=signal,
        mover_database=mover,
        now=now + dt.timedelta(hours=1),
        **paths,
    )
    after = sqlite3.connect(signal).execute(
        "SELECT COUNT(*) FROM independent_news_decisions"
    ).fetchone()[0]
    assert before == after == 1
    assert first["summary"]["inserted_diagnoses"] == 1
    assert second["summary"]["inserted_diagnoses"] == 0
    assert second["summary"]["inserted_snapshot"] == 0
    assert first["recorded_utc"] == second["recorded_utc"]
    assert first["evaluated_utc"] != second["evaluated_utc"]
    assert second["refresh_mode"] == "on_demand_append_only"
    assert second["recurring_polling"] is False
    assert second["research_only"] is True
    assert second["can_modify_execution_policy"] is False
    with sqlite3.connect(output_db) as connection:
        diagnosis_id = connection.execute(
            "SELECT diagnosis_id FROM diagnoses"
        ).fetchone()[0]
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute(
                "UPDATE diagnoses SET verdict='forged' WHERE diagnosis_id=?",
                (diagnosis_id,),
            )
        snapshot_id = connection.execute(
            "SELECT source_fingerprint FROM audit_snapshots"
        ).fetchone()[0]
        assert connection.execute("SELECT COUNT(*) FROM audit_snapshots").fetchone()[0] == 1
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute(
                "UPDATE audit_snapshots SET queue_count=99 WHERE source_fingerprint=?",
                (snapshot_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute(
                "DELETE FROM audit_snapshots WHERE source_fingerprint=?",
                (snapshot_id,),
            )


def test_validate_existing_rejects_unsafe_state(tmp_path) -> None:
    signal = tmp_path / "signal.sqlite"
    mover = tmp_path / "mover.sqlite"
    create_input_databases(signal, mover)
    state = tmp_path / "state.json"
    database = tmp_path / "audit.sqlite"
    payload = audit.run_once(
        signal_database=signal,
        mover_database=mover,
        database=database,
        output=state,
        queue_path=tmp_path / "queue.json",
        report=tmp_path / "report.md",
        log_path=tmp_path / "log.jsonl",
        now=audit.utc_now(),
    )
    valid, errors = audit.validate_existing(state, database)
    assert valid is True
    assert errors == []
    payload["generated_utc"] = "2000-01-01T00:00:00+00:00"
    state.write_text(json.dumps(payload), encoding="utf-8")
    valid, errors = audit.validate_existing(state, database)
    assert valid is True
    assert errors == []
    payload["generated_utc"] = audit.iso_utc(
        audit.utc_now() + dt.timedelta(hours=1)
    )
    state.write_text(json.dumps(payload), encoding="utf-8")
    valid, errors = audit.validate_existing(state, database)
    assert valid is False
    assert "record_timestamp_in_future" in errors
    payload["can_place_orders"] = True
    payload["generated_utc"] = audit.iso_utc(audit.utc_now())
    state.write_text(json.dumps(payload), encoding="utf-8")
    valid, errors = audit.validate_existing(state, database)
    assert valid is False
    assert "unsafe_or_missing:can_place_orders" in errors


def test_audit_is_on_demand_not_an_always_on_worker() -> None:
    supervisor = (
        Path(__file__).resolve().parent / "oanda_always_on_supervisor.ps1"
    ).read_text(encoding="utf-8")
    assert '-Name "news_outcome_improvement_audit"' not in supervisor
    assert "oanda_news_outcome_improvement_audit.py" not in supervisor


def test_cli_has_no_recurring_polling_mode() -> None:
    with pytest.raises(SystemExit):
        audit.main(["--interval-sec", "300"])
