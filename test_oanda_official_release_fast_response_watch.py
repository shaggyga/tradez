from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import oanda_official_release_fast_response_watch as watch


T0 = datetime(2026, 8, 25, 3, 30, 0, tzinfo=timezone.utc)


def mapping(*, candidate: int = 1, prospective: int = 1) -> dict:
    return {
        "mapping_id": "mapping-1",
        "source_id": "rba_minutes_direct_v1",
        "first_seen_utc": (T0 - timedelta(seconds=10)).isoformat(),
        "input_prospective_observation": prospective,
        "classification_version": watch.REQUIRED_CLASSIFICATION_VERSION,
        "forward_shadow_candidate": candidate,
        "mapping_payload": {
            "headline": "Monetary policy decision",
            "currency_scores": {"AUD": 0.8},
            "research_currency_scores": {},
        },
        "mapped_utc": (T0 - timedelta(seconds=5)).isoformat(),
        "mapper_contract_id": watch.REQUIRED_MAPPER_CONTRACT,
        "mapper_cohort_id": "official_release_fast_mapping_v2_20260824",
    }


def quotes(at: datetime = T0) -> dict:
    return {
        "generated_utc": at.isoformat(),
        "quotes": {
            "AUD_USD": {
                "bid": 0.7000,
                "ask": 0.7002,
                "pip": 0.0001,
                "time": at.isoformat(),
            },
            "EUR_AUD": {
                "bid": 1.6500,
                "ask": 1.6504,
                "pip": 0.0001,
                "time": at.isoformat(),
            },
            "EUR_USD": {
                "bid": 1.1000,
                "ask": 1.1002,
                "pip": 0.0001,
                "time": at.isoformat(),
            },
        },
    }


def technical() -> dict:
    return {
        "updated_at": (T0 - timedelta(seconds=2)).isoformat(),
        "top_signals": [
            {
                "instrument": "AUD_USD",
                "direction": "buy",
                "signal_confidence": 0.6,
                "signal_eligible": False,
                "horizon_breakdown": [
                    {
                        "horizon_sec": 60,
                        "direction_state": "buy",
                        "signal_confidence": 0.61,
                        "signal_eligible": False,
                        "projected_net_pips": 0.4,
                    },
                    {
                        "horizon_sec": 300,
                        "direction_state": "sell",
                        "signal_confidence": 0.57,
                        "signal_eligible": False,
                        "projected_net_pips": -0.2,
                    },
                ],
            }
        ],
    }


def test_currency_side_projection_uses_pair_leg() -> None:
    assert watch.side_for_currency("AUD_USD", "AUD", 1.0) == "buy"
    assert watch.side_for_currency("EUR_AUD", "AUD", 1.0) == "sell"
    assert watch.side_for_currency("AUD_USD", "AUD", -1.0) == "sell"
    assert watch.side_for_currency("EUR_USD", "AUD", 1.0) is None


def test_nonprospective_or_non_candidate_never_opens_watch() -> None:
    assert watch.build_watch_rows(mapping(prospective=0), quotes(), technical(), T0) == []
    assert watch.build_watch_rows(mapping(candidate=0), quotes(), technical(), T0) == []


def test_builds_all_pair_expressions_and_separate_technical_relations() -> None:
    assert watch.HORIZONS_MIN == (1, 5, 15, 30, 60, 120)
    rows = watch.build_watch_rows(mapping(), quotes(), technical(), T0)
    assert len(rows) == 12
    assert {row["instrument"] for row in rows} == {"AUD_USD", "EUR_AUD"}
    assert len({row["event_factor_id"] for row in rows}) == 1
    lookup = {(row["instrument"], row["horizon_min"]): row for row in rows}
    assert lookup[("AUD_USD", 1)]["side"] == "buy"
    assert lookup[("EUR_AUD", 1)]["side"] == "sell"
    assert lookup[("AUD_USD", 1)]["technical_relation"] == "aligned"
    assert lookup[("AUD_USD", 5)]["technical_relation"] == "conflicted"
    assert lookup[("EUR_AUD", 15)]["technical_relation"] == "neutral_or_unavailable"
    assert all(row["research_only"] and not row["execution_eligible"] for row in rows)


def test_stale_or_pre_event_quote_is_rejected() -> None:
    stale = quotes(T0 - timedelta(seconds=31))
    assert watch.build_watch_rows(mapping(), stale, technical(), T0) == []
    pre_event = quotes(T0 - timedelta(seconds=11))
    assert watch.build_watch_rows(mapping(), pre_event, technical(), T0) == []


def test_executable_outcome_uses_ask_entry_bid_exit_for_buy() -> None:
    row = next(
        row
        for row in watch.build_watch_rows(mapping(), quotes(), technical(), T0)
        if row["instrument"] == "AUD_USD" and row["horizon_min"] == 1
    )
    exit_quote = {
        "bid": 0.7005,
        "ask": 0.7007,
        "time": (T0 + timedelta(minutes=1, seconds=5)).isoformat(),
    }
    outcome = watch.build_outcome(row, exit_quote, T0 + timedelta(minutes=1, seconds=5))
    assert outcome is not None
    assert outcome["maturity_state"] == "valid_exact_executable_quote"
    assert round(outcome["signed_mid_move_pips"], 6) == 5.0
    assert round(outcome["executable_net_pips"], 6) == 3.0
    assert round(outcome["realized_cost_drag_pips"], 6) == 2.0


def test_append_only_tables_and_factor_deduplicated_result(tmp_path) -> None:
    connection = watch.connect(tmp_path / "watch.sqlite")
    rows = watch.build_watch_rows(mapping(), quotes(), technical(), T0)
    assert watch.insert_watches(connection, rows) == 12
    assert watch.insert_watches(connection, rows) == 0
    later_quotes = quotes(T0 + timedelta(minutes=20))
    # Use a target-close quote per watch by evaluating each declared horizon.
    for horizon in watch.HORIZONS_MIN:
        at = T0 + timedelta(minutes=horizon, seconds=5)
        payload = quotes(at)
        payload["quotes"]["AUD_USD"].update(bid=0.7005, ask=0.7007)
        payload["quotes"]["EUR_AUD"].update(bid=1.6495, ask=1.6499)
        watch.mature_outcomes(connection, payload, at)
    connection.commit()
    census = watch.counts(connection)
    assert census["watches"] == 12
    assert census["valid_outcomes"] == 12
    results = watch.factor_results(connection)
    assert [row["effective_factor_n"] for row in results] == [1, 1, 1, 1, 1, 1]
    assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    connection.close()


def test_snapshot_policy_is_exactly_inert() -> None:
    assert watch.POLICY == {
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "broker_access": False,
        "canonical_watchlist_mutation": False,
        "supported_decision": "shadow_observation_only",
    }


def test_every_normal_heartbeat_state_proves_classifier_contract(
    tmp_path, monkeypatch
) -> None:
    input_database = tmp_path / "mapping.sqlite"
    connection = sqlite3.connect(input_database)
    connection.execute(
        """
        CREATE TABLE official_release_mapping (
            mapping_id TEXT,
            source_id TEXT,
            first_seen_utc TEXT,
            input_prospective_observation INTEGER,
            classification_version TEXT,
            forward_shadow_candidate INTEGER,
            mapping_payload_json TEXT,
            mapped_utc TEXT,
            mapper_contract_id TEXT,
            mapper_cohort_id TEXT
        )
        """
    )
    connection.commit()
    connection.close()

    heartbeat_path = tmp_path / "heartbeat.json"
    published: list[tuple[object, dict]] = []
    monkeypatch.setattr(watch, "read_json", lambda _path: {})
    monkeypatch.setattr(
        watch,
        "atomic_write_json",
        lambda path, payload: published.append((path, dict(payload))),
    )

    watch.cycle(
        input_database=input_database,
        output_database=tmp_path / "response.sqlite",
        quote_path=tmp_path / "quotes.json",
        technical_path=tmp_path / "technical.json",
        snapshot_path=tmp_path / "snapshot.json",
        heartbeat_path=heartbeat_path,
        observed_utc=T0,
    )

    heartbeats = [payload for path, payload in published if path == heartbeat_path]
    assert [payload["phase"] for payload in heartbeats] == [
        "loading_inputs",
        "cycle_complete",
    ]
    assert all(
        payload["required_classification_version"]
        == watch.REQUIRED_CLASSIFICATION_VERSION
        for payload in heartbeats
    )


def test_error_heartbeat_proves_classifier_contract(monkeypatch) -> None:
    published: list[dict] = []

    def fail_cycle() -> None:
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr(watch, "cycle", fail_cycle)
    monkeypatch.setattr(
        watch,
        "atomic_write_json",
        lambda _path, payload: published.append(dict(payload)),
    )
    monkeypatch.setattr(sys, "argv", ["oanda_official_release_fast_response_watch.py"])

    assert watch.main() == 0
    assert len(published) == 1
    assert published[0]["phase"] == "error"
    assert (
        published[0]["required_classification_version"]
        == watch.REQUIRED_CLASSIFICATION_VERSION
    )


def test_supervisor_tracks_v3_heartbeat() -> None:
    supervisor = (watch.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert "official_release_fast_response_watch_heartbeat_v3.json" in supervisor
    assert "official_release_fast_response_watch_heartbeat_v2.json" not in supervisor
    response_watch_block = supervisor.split(
        '-Name "official_release_fast_response_watch"', 1
    )[1].split('-Name "causal_source_factor_response_map_v4"', 1)[0]
    assert 'ExpectedJsonField = "required_classification_version"' in response_watch_block
    assert watch.REQUIRED_CLASSIFICATION_VERSION in response_watch_block
