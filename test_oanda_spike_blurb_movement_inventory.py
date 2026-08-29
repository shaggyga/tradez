import csv
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from oanda_spike_blurb_movement_inventory import (
    BUILD_CONTRACT_ID,
    build_legacy_reconciliation,
    build_pair_horizon,
    open_database,
)


def _price() -> dict:
    timestamps = pd.date_range("2026-08-01T00:00:00Z", periods=9, freq="1min")
    bids = [1.0000, 1.0001, 1.0002, 1.0015, 1.0016, 1.0017, 1.0008, 1.0007, 1.0006]
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            # Pandas 3 may store this index at microsecond rather than
            # nanosecond resolution, so an int64 divisor is not portable.
            "epoch": [int(value.timestamp()) for value in timestamps],
            "instrument": ["EUR_USD"] * len(timestamps),
            "bid_high": [value + 0.0001 for value in bids],
            "bid_low": [value - 0.0001 for value in bids],
            "bid_close": bids,
            "ask_high": [value + 0.0003 for value in bids],
            "ask_low": [value + 0.0001 for value in bids],
            "ask_close": [value + 0.0002 for value in bids],
            "mid": [value + 0.0001 for value in bids],
            "spread_pips": [2.0] * len(timestamps),
        }
    )
    return {
        "frame": frame,
        "pip": 0.0001,
        "sha256": "a" * 64,
        "first_epoch": int(frame["epoch"].iloc[0]),
        "last_epoch": int(frame["epoch"].iloc[-1]),
    }


def test_pair_horizon_uses_executable_sides_and_marks_hindsight_only():
    movements, census, stats = build_pair_horizon(
        price=_price(),
        instrument="EUR_USD",
        horizon_min=3,
        selection_quantiles=[0.5, 0.9],
        minimum_net_cost_multiple=1.0,
        build_contract_id=BUILD_CONTRACT_ID,
        created_utc="2026-08-19T00:00:00Z",
        modeled_slippage_pips=0.0,
    )
    assert stats["valid"] == 6
    assert stats["cost_clear"] > 0
    assert movements
    assert all(row["after_cost_pips"] > 0 for row in movements)
    assert all(row["outcome_selected"] == 1 for row in movements)
    assert all(row["forecast_proof_eligible"] == 0 for row in movements)
    assert all(row["research_only"] == 1 for row in movements)
    assert all(row["selected_side"] in {"long", "short"} for row in movements)
    assert sum(row["raw_cost_clear_count"] for row in census) == stats["cost_clear"]


def test_nonoverlap_prevents_correlated_starts_in_same_horizon():
    movements, _, _ = build_pair_horizon(
        price=_price(),
        instrument="EUR_USD",
        horizon_min=3,
        selection_quantiles=[0.0, 0.5],
        minimum_net_cost_multiple=1.0,
        build_contract_id=BUILD_CONTRACT_ID,
        created_utc="2026-08-19T00:00:00Z",
        modeled_slippage_pips=0.0,
    )
    epochs = sorted(row["entry_epoch"] for row in movements)
    assert all(right - left >= 180 for left, right in zip(epochs, epochs[1:]))


def test_legacy_rows_are_classified_without_synthesizing_history(tmp_path):
    path = tmp_path / "legacy.csv"
    fields = [
        "move_id", "instrument", "start_utc", "end_utc", "news_match_status",
        "primary_expected_pair_direction", "primary_causal_relation", "primary_event_id",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(
            [
                {
                    "move_id": "inside", "instrument": "EUR_USD",
                    "start_utc": "2026-08-01T00:00:00Z", "end_utc": "2026-08-01T00:03:00Z",
                    "news_match_status": "matched", "primary_expected_pair_direction": "LONG",
                    "primary_causal_relation": "PRE_MOVE", "primary_event_id": "event_1",
                },
                {
                    "move_id": "outside", "instrument": "EUR_USD",
                    "start_utc": "2025-08-01T00:00:00Z", "end_utc": "2025-08-01T00:03:00Z",
                    "news_match_status": "unmatched", "primary_expected_pair_direction": "",
                    "primary_causal_relation": "", "primary_event_id": "",
                },
            ]
        )
    first = int(pd.Timestamp("2026-08-01T00:00:00Z").timestamp())
    last = int(pd.Timestamp("2026-08-01T00:08:00Z").timestamp())
    rows, states = build_legacy_reconciliation(
        path=path,
        coverage={"EUR_USD": (first, last)},
        build_contract_id=BUILD_CONTRACT_ID,
    )
    assert len(rows) == 2
    assert states["within_current_executable_archive"] == 1
    assert states["historical_executable_archive_unavailable"] == 1
    outside = next(row for row in rows if row["move_id"] == "outside")
    assert outside["retained_direction"] is None
    assert "reacquire" in outside["required_action"]
    assert outside["forecast_proof_eligible"] == 0


def test_ledger_rows_are_immutable(tmp_path):
    database = open_database(tmp_path / "ledger.sqlite")
    database.execute(
        "INSERT INTO reconstruction_contracts VALUES(?,?,?,?,?,?,?,?)",
        (
            BUILD_CONTRACT_ID, "parent", "cohort", "2026-08-19T00:00:00Z",
            "a" * 64, "b" * 64, "c" * 64, "{}",
        ),
    )
    database.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        database.execute(
            "UPDATE reconstruction_contracts SET cohort_id='forged' WHERE build_contract_id=?",
            (BUILD_CONTRACT_ID,),
        )
    database.close()
