from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

import pytest

import oanda_causal_source_factor_response_map_v1 as v1
import oanda_causal_source_factor_response_map_v2 as subject


UTC = timezone.utc
PAIRS = ("EUR_USD", "GBP_USD", "USD_JPY", "EUR_GBP", "EUR_JPY", "GBP_JPY")


def _event(event_id: str, at: datetime, *, eligible: bool = False) -> dict:
    evidence = "prospective_v1" if eligible else "preactivation_diagnostic"
    return {
        "canonical_event_id": event_id,
        "market_episode_id": f"episode_{event_id}",
        "currency": "USD",
        "first_known_utc": subject.iso(at),
        "evidence_class": evidence,
        "prospective_proof_eligible": eligible,
        "headline": event_id,
        "source_url": "",
        "authority": "official_test_authority",
        "category": "monetary_policy",
        "event_series_id": "",
        "topic_signature": "",
        "source_role": "primary_policy_release",
        "source_facts": {"topic_action": "hold"},
        "transport_count": 0,
        "transport_observations": [],
        "factors": [
            {
                "factor_key": "action:hold",
                "factor_type": "action",
                "factor_value": "hold",
                "factor_known_utc": subject.iso(at),
            }
        ],
    }


def _ten_minute_panel(at: datetime) -> dict[str, list[v1.CandlePoint]]:
    panel: dict[str, list[v1.CandlePoint]] = {}
    for pair_index, instrument in enumerate(PAIRS):
        pip = v1.pip_size(instrument)
        middle = 150.0 if instrument.endswith("_JPY") else 1.10 + pair_index * 0.01
        rows = []
        for minute in range(10):
            start = middle + minute * pip
            close = middle + (minute + 1) * pip
            rows.append(
                v1.CandlePoint(
                    timestamp=at + timedelta(minutes=minute),
                    bid_open=start - pip,
                    ask_open=start + pip,
                    bid_close=close - pip,
                    ask_close=close + pip,
                    bid_high=close + 2 * pip,
                    bid_low=start - 2 * pip,
                    ask_high=close + 3 * pip,
                    ask_low=start - pip,
                )
            )
        panel[instrument] = rows
    return panel


def _exact_entry(event: dict, panel: dict[str, list[v1.CandlePoint]]) -> dict:
    event_time = v1.parse_time(event["first_known_utc"])
    assert event_time is not None
    return {
        "entry_snapshot_id": "entry_exact_10m",
        "canonical_event_id": event["canonical_event_id"],
        "currency": event["currency"],
        "event_first_known_utc": subject.iso(event_time),
        "captured_utc": subject.iso(event_time + timedelta(seconds=5)),
        "capture_latency_seconds": 5.0,
        "timing_quality": "prospective_exact_live_quote",
        "invalid_reason": "",
        "quote_count": len(panel),
        "quotes": {
            instrument: {
                "bid": rows[0].bid_open,
                "ask": rows[0].ask_open,
                "pip": v1.pip_size(instrument),
                "quote_time_utc": subject.iso(event_time + timedelta(seconds=5)),
                "age_seconds": 0.0,
                "event_offset_seconds": 5.0,
            }
            for instrument, rows in panel.items()
        },
        "technical_snapshot": {},
        "research_only": True,
        "execution_eligible": False,
    }


def _response(event_id: str, at: datetime, value: float) -> dict:
    return {
        "response_id": f"response_{event_id}_10",
        "canonical_event_id": event_id,
        "currency": "USD",
        "horizon_min": 10,
        "event_clock_utc": subject.iso(at),
        "maturity_utc": subject.iso(at + timedelta(minutes=10)),
        "maturity_state": "valid_canonical_ls_factor_and_bid_ask_paths",
        "currency_factor_bps": value,
        "absolute_currency_factor_bps": abs(value),
        "currency_rank": 1 if value > 0 else 4,
        "factor_currency_count": 4,
        "usable_pair_count": 6,
        "currency_pair_path_count": 3,
        "entry_method": "preactivation_m1_approximation",
        "response_timing_quality": "diagnostic_approximate_not_proof",
        "early_currency_factor_bps": value / 2.0,
        "response_shape": "immediate",
        "solver_status": "ok",
        "solver_condition_number": 2.0,
        "solver_weighted_observation_equivalent": 5.0,
        "solver_diagnostics": {"status": "ok", "observation_count": 6},
        "both_executable_paths": [],
        "selected_side": None,
    }


def _factor(connection, event_id: str) -> dict:
    row = connection.execute(
        """
        SELECT sf.factor_observation_id,sf.canonical_event_id,sf.currency,
               sf.factor_key,sf.factor_type,sf.factor_known_utc,
               sf.evidence_class,sf.prospective_proof_eligible,e.authority,
               ep.market_episode_id
        FROM source_factor_observation sf
        JOIN source_event_observation e
          ON e.canonical_event_id=sf.canonical_event_id
        JOIN source_event_episode ep
          ON ep.canonical_event_id=sf.canonical_event_id
        WHERE sf.canonical_event_id=?
        """,
        (event_id,),
    ).fetchone()
    return {
        "factor_observation_id": row[0],
        "canonical_event_id": row[1],
        "currency": row[2],
        "factor_key": row[3],
        "factor_type": row[4],
        "factor_known_utc": row[5],
        "evidence_class": row[6],
        "prospective_proof_eligible": bool(row[7]),
        "authority": row[8],
        "market_episode_id": row[9],
    }


def test_exact_ten_minute_response_uses_declared_maturity_and_both_paths():
    assert subject.HORIZONS_MIN == (1, 5, 10, 15, 30, 60, 120)
    at = subject.ACTIVATED_UTC + timedelta(minutes=5)
    event = _event("prospective_ten", at, eligible=True)
    panel = _ten_minute_panel(at)
    response = subject.compute_event_response(
        event,
        panel,
        10,
        entry_snapshot=_exact_entry(event, panel),
    )
    assert response is not None
    assert response["horizon_min"] == 10
    assert response["maturity_utc"] == subject.iso(at + timedelta(minutes=10))
    assert response["entry_method"] == "prospective_exact_live_quote"
    assert response["response_timing_quality"] == (
        "prospective_exact_entry_completed_m1_exit"
    )
    assert response["selected_side"] is None
    assert response["both_executable_paths"]
    assert all(
        row["exit_utc"] == subject.iso(at + timedelta(minutes=10))
        and row["exit_target_delay_seconds"] == pytest.approx(0.0)
        and row["currency_strengthening"]["terminal_executable_pips"] is not None
        and row["currency_weakening"]["terminal_executable_pips"] is not None
        for row in response["both_executable_paths"]
    )


def test_ten_minute_prequential_mapping_excludes_not_yet_matured_response(tmp_path):
    connection = subject.open_output_database(tmp_path / "v2.sqlite")
    try:
        base = datetime(2026, 8, 20, 12, tzinfo=UTC)
        events = [
            _event("past", base),
            _event("not_matured", base + timedelta(minutes=55)),
            _event("target", base + timedelta(minutes=60)),
        ]
        subject.insert_events(connection, events)
        subject.insert_responses(
            connection,
            [
                _response("past", base, 2.0),
                _response("not_matured", base + timedelta(minutes=55), 100.0),
            ],
        )
        forecast = subject.build_prequential_forecast(
            connection,
            _factor(connection, "target"),
            10,
            minimum_n=1,
        )
        assert forecast["forecast_state"] == "forecast"
        assert forecast["effective_event_n"] == 1
        assert forecast["predicted_currency_factor_bps"] == pytest.approx(2.0)
        assert forecast["training_latest_maturity_utc"] <= _factor(
            connection, "target"
        )["factor_known_utc"]
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        connection.close()


def test_v2_schema_is_distinct_and_v1_contract_remains_unchanged(tmp_path):
    connection = subject.open_output_database(tmp_path / "v2.sqlite")
    try:
        response_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' "
            "AND name='source_event_response'"
        ).fetchone()[0]
        assert "1,5,10,15,30,60,120" in response_sql.replace(" ", "")
    finally:
        connection.close()
    assert subject.OUTPUT_DATABASE != v1.OUTPUT_DATABASE
    assert subject.CONTRACT_ID != v1.CONTRACT_ID
    assert subject.COHORT_ID != v1.COHORT_ID
    assert v1.HORIZONS_MIN == (1, 5, 15, 30, 60, 120)
    assert subject.POLICY["broker_access"] is False
    assert subject.POLICY["can_place_orders"] is False
    assert subject.POLICY["can_authorize"] is False
    assert subject.POLICY["can_promote"] is False


def test_hidden_supervisor_preserves_v2_v3_and_adopts_v4_successor():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert '-Needle "oanda_causal_source_factor_response_map_v2.py"' in supervisor
    assert '-Needle "oanda_causal_source_factor_response_map_v1.py"' in supervisor
    assert '-Needle "oanda_causal_source_factor_response_map_v3.py"' in supervisor
    assert '-Needle "oanda_causal_source_factor_response_map_v4.py"' in supervisor
    assert "causal_source_factor_response_map_latest_v4.json" in supervisor
    assert "v4_v149_relevance_exclusion_gate_cutover" in supervisor
