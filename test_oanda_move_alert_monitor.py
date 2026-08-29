"""Focused tests for the causal live move-alert ledger."""

from __future__ import annotations

import json
from pathlib import Path

import oanda_move_alert_monitor as monitor


def signal_snapshot(
    epoch: float,
    *,
    instrument: str = "EUR_USD",
    raw_probability_up: float = 0.56,
    raw_signed_pips: float = 12.0,
    eligible: bool = False,
) -> dict:
    direction = "buy" if raw_signed_pips >= 0.0 else "sell"
    return {
        "updated_at": monitor.utc_iso(epoch),
        "top_signals": [
            {
                "instrument": instrument,
                "direction_state": direction if eligible else "neutral",
                "direction_conflict": not eligible,
                "component_count": 20,
                "component_models": ["example_model"],
                "component_families": ["example_family"],
                "signal_blocked_by": [] if eligible else ["unvalidated_signal"],
                "horizon_breakdown": [
                    {
                        "horizon_sec": 3600,
                        "raw_probability_up": raw_probability_up,
                        "raw_ensemble_signed_net_pips": raw_signed_pips,
                        "direction": direction,
                        "signal_eligible": eligible,
                        "paper_consensus_eligible": eligible,
                        "component_count": 20,
                        "eligible_component_count": 2 if eligible else 0,
                        "signal_blocked_by": (
                            [] if eligible else ["unvalidated_signal"]
                        ),
                        "best_model_id": "example_model",
                        "best_family": "example_family",
                        "best_input_timeframe": "M1",
                    }
                ],
            }
        ],
    }


def move_profile(
    instrument: str,
    direction: str,
    start: float,
    end: float,
    *,
    severity: float = 2.0,
) -> dict:
    return {
        "instrument": instrument,
        "direction": direction,
        "start_epoch": start,
        "end_epoch": end,
        "horizon_minutes": 60,
        "entry_bid": 1.0,
        "entry_ask": 1.0002,
        "exit_bid": 1.0032,
        "exit_ask": 1.0034,
        "pip": 0.0001,
        "gross_mid_pips": 32.0 if direction == "buy" else -32.0,
        "executable_net_pips": 30.0,
        "threshold_pips": 15.0,
        "percentage_move": 0.32,
        "spread_cost_pips": 2.0,
        "severity": severity,
        "volatility_z": 4.0,
        "acceleration": 0.2,
        "velocity_pips_per_minute": 0.5,
    }


def test_stale_quotes_are_rejected() -> None:
    now = 10_000.0
    payload = {
        "generated_utc": monitor.utc_iso(now),
        "quotes": {
            "EUR_USD": {
                "bid": 1.1,
                "ask": 1.1002,
                "pip": 0.0001,
                "time": monitor.utc_iso(now - 120),
            },
            "USD_JPY": {
                "bid": 150.0,
                "ask": 150.02,
                "pip": 0.01,
                "time": monitor.utc_iso(now - 1),
            },
        },
    }
    rows = monitor.quote_rows(payload, now=now, max_quote_age_sec=30)
    assert [row["instrument"] for row in rows] == ["USD_JPY"]


def test_gated_aligned_signal_is_a_miss_not_a_catch() -> None:
    payload = monitor.summarize_signal_snapshot(signal_snapshot(1000.0))[0]
    result = monitor.classify_forecast(
        payload,
        move_direction="buy",
        horizon_minutes=60,
        move_net_pips=30.0,
        threshold_pips=15.0,
    )
    assert result["error_code"] == "GATED_ALIGNED_PREMOVE_SIGNAL"
    assert result["missed"] is True


def test_event_summary_separates_coverage_from_gate_what_if() -> None:
    events = [
        {
            "missed": True,
            "status": "resolved",
            "error_code": "NO_ARCHIVED_PREMOVE_FORECAST",
        },
        {
            "missed": True,
            "status": "resolved",
            "horizon_minutes": 60,
            "error_code": "GATED_ALIGNED_PREMOVE_SIGNAL",
            "forecast": {
                "evaluated_horizon_sec": 3600,
                "blockers": ["unvalidated_signal", "signal_confidence"],
            },
        },
        {
            "missed": True,
            "status": "resolved",
            "error_code": "WEAK_ALIGNED_PREMOVE_SIGNAL",
        },
        {
            "missed": False,
            "caught": True,
            "status": "resolved",
            "error_code": "CAUGHT_ELIGIBLE_PREMOVE",
        },
    ]

    summary = monitor.event_summary(events)

    assert summary["forecast_coverage"] == {
        "covered_events": 3,
        "coverage_gap_events": 1,
        "coverage_rate": 0.75,
    }
    assert summary["capture_rates"]["all_events"] == 0.25
    assert summary["capture_rates"]["covered_events"] == 1 / 3
    assert summary["gate_what_if"]["strong_aligned_but_gated"] == 1
    assert summary["gate_what_if"]["strong_aligned_caught_or_gated"] == 2
    assert summary["gate_what_if"]["weak_aligned"] == 1
    assert summary["gate_what_if"]["gated_horizon_counts"] == {"3600": 1}
    assert summary["gate_what_if"]["gated_blocker_counts"] == {
        "signal_confidence": 1,
        "unvalidated_signal": 1,
    }


def test_ledger_uses_only_premove_snapshot_and_marks_late_alignment(
    tmp_path: Path,
) -> None:
    ledger = monitor.MoveAlertLedger(tmp_path / "moves.sqlite")
    start = 10_000.0
    detected = start + 3600.0
    ledger.archive_forecasts(
        signal_snapshot(
            start - 10,
            raw_probability_up=0.40,
            raw_signed_pips=-10.0,
        )
    )
    ledger.archive_forecasts(
        signal_snapshot(
            start + 120,
            raw_probability_up=0.58,
            raw_signed_pips=15.0,
        )
    )
    touched = ledger.record_moves(
        [move_profile("EUR_USD", "buy", start, detected)],
        detected,
    )
    assert len(touched) == 1
    event = ledger.recent_events(detected)[0]
    assert event["error_code"] == "LATE_ALIGNED_SIGNAL"
    assert event["forecast"]["raw_direction"] == "sell"
    assert event["forecast_snapshot_epoch"] == start - 10
    ledger.close()


def test_common_currency_moves_receive_one_cluster(tmp_path: Path) -> None:
    ledger = monitor.MoveAlertLedger(tmp_path / "moves.sqlite")
    start = 20_000.0
    detected = start + 3600.0
    profiles = [
        move_profile("USD_ZAR", "sell", start, detected, severity=4.0),
        move_profile("EUR_ZAR", "sell", start + 30, detected, severity=3.0),
        move_profile("GBP_ZAR", "sell", start + 60, detected, severity=5.0),
    ]
    ledger.record_moves(profiles, detected)
    events = ledger.recent_events(detected)
    assert len(events) == 3
    assert {row["cluster_currency"] for row in events} == {"ZAR"}
    assert {row["cluster_direction"] for row in events} == {"strengthening"}
    assert len({row["cluster_id"] for row in events}) == 1
    ledger.close()


def test_executable_profile_counts_entry_and_exit_spread(tmp_path: Path) -> None:
    ledger = monitor.MoveAlertLedger(tmp_path / "moves.sqlite")
    start = 100_000.0
    rows = []
    for minute in range(61):
        mid = 1.1000 + minute * 0.00005
        rows.append(
            {
                "instrument": "EUR_USD",
                "observed_epoch": start + minute * 60,
                "bid": mid - 0.0001,
                "ask": mid + 0.0001,
                "pip": 0.0001,
            }
        )
    ledger.ingest_quotes(rows)
    profiles = ledger.current_profiles(start + 3600)
    hour = next(row for row in profiles if row["horizon_minutes"] == 60)
    assert round(hour["gross_mid_pips"], 6) == 30.0
    assert round(hour["executable_net_pips"], 6) == 28.0
    assert round(hour["spread_cost_pips"], 6) == 2.0
    ledger.close()


def test_one_minute_shock_profile_is_enabled_and_spread_aware(
    tmp_path: Path,
) -> None:
    ledger = monitor.MoveAlertLedger(tmp_path / "moves.sqlite")
    start = 200_000.0
    rows = []
    for minute in range(11):
        mid = 1.1000 if minute < 10 else 1.1010
        rows.append(
            {
                "instrument": "EUR_USD",
                "observed_epoch": start + minute * 60,
                "bid": mid - 0.0001,
                "ask": mid + 0.0001,
                "pip": 0.0001,
            }
        )
    ledger.ingest_quotes(rows)
    profile = next(
        row
        for row in ledger.current_profiles(start + 600)
        if row["horizon_minutes"] == 1
    )

    assert round(profile["executable_net_pips"], 6) == 8.0
    assert profile["threshold_pips"] >= 3.0 * profile["spread_cost_pips"]
    assert profile["severity"] > 1.0
    assert profile["acceleration"] == 0.0
    ledger.close()


def test_move_state_terms_cannot_be_account_eligible() -> None:
    profile = move_profile("EUR_USD", "buy", 1_000.0, 4_600.0)
    terms = monitor.build_causal_terms([profile])
    quotes = {
        "quotes": {
            "EUR_USD": {
                "bid": 1.1,
                "ask": 1.1002,
                "pip": 0.0001,
            }
        }
    }
    forecasts = monitor.build_term_forecasts(terms, quotes, 4_600.0)
    assert {row["model_id"] for row in forecasts} == set(
        monitor.TERM_MODEL_IDS
    )
    assert all(row["account_eligible"] is False for row in forecasts)
    assert all(
        point["account_eligible"] is False
        for row in forecasts
        for point in row["forecast_curve"]
    )
    assert all(
        row["producer_metadata"]["hindsight_labels_used"] is False
        for row in forecasts
    )


def test_closed_market_cycle_defers_databases_and_writes_heartbeat(
    tmp_path: Path,
) -> None:
    quotes = tmp_path / "quotes.json"
    signals = tmp_path / "signals.json"
    heartbeat = tmp_path / "heartbeat.json"
    database = tmp_path / "move_alerts.sqlite"
    signal_feed = tmp_path / "signal_feed.sqlite"
    quotes.write_text(
        json.dumps({"generated_utc": monitor.utc_iso(), "quotes": {}}),
        encoding="utf-8",
    )
    signals.write_text("{}", encoding="utf-8")
    args = monitor.parse_args(
        [
            "--quotes",
            str(quotes),
            "--signals",
            str(signals),
            "--database",
            str(database),
            "--signal-feed",
            str(signal_feed),
            "--heartbeat",
            str(heartbeat),
            "--process-lock",
            str(tmp_path / "monitor.lock"),
            "--alert-json",
            str(tmp_path / "alert.json"),
            "--alert-md",
            str(tmp_path / "alert.md"),
            "--missed-json",
            str(tmp_path / "missed.json"),
            "--missed-md",
            str(tmp_path / "missed.md"),
            "--terms-json",
            str(tmp_path / "terms.json"),
            "--publish-terms",
            "--once",
        ]
    )

    assert monitor.run(args) == 0
    payload = json.loads(heartbeat.read_text(encoding="utf-8"))
    assert payload["status"] == "ok"
    assert payload["last_publish"]["reason"] == "no_fresh_quotes"
    assert payload["publication_error"] == "deferred_no_fresh_quotes"
    assert not database.exists()
    assert not signal_feed.exists()
