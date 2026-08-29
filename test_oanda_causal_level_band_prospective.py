from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from oanda_causal_level_band_prospective import (
    HORIZONS_SEC,
    RETAIN_M1_ROWS,
    RETAIN_M5_ROWS,
    append_entry,
    append_outcome,
    collection_identity,
    compatible_forecasts_for_maturation,
    insert_forecast,
    load_m1_tail,
    mature_payload,
    next_m5_boundary,
    open_ledger,
    quote_row,
    register_cohort,
    trusted_observation_time,
)
from oanda_level_band_contract_v2 import MarketBar


BASE = datetime(2026, 8, 27, 12, 2, 30, tzinfo=timezone.utc)


def bar(
    offset: int,
    *,
    mid: float = 1.1000,
    high: float | None = None,
    low: float | None = None,
    spread: float = 0.0002,
) -> MarketBar:
    high = mid + 0.0001 if high is None else high
    low = mid - 0.0001 if low is None else low
    half = spread / 2.0
    start = datetime(2026, 8, 27, 12, 5, tzinfo=timezone.utc) + timedelta(minutes=5 * offset)
    return MarketBar(
        timestamp=start,
        minutes=5,
        mid_open=mid,
        mid_high=high,
        mid_low=low,
        mid_close=mid,
        bid_open=mid - half,
        bid_high=high - half,
        bid_low=low - half,
        bid_close=mid - half,
        ask_open=mid + half,
        ask_high=high + half,
        ask_low=low + half,
        ask_close=mid + half,
    )


def payload(identity: dict[str, str], quote_time: str = "2026-08-27T12:02:29Z") -> dict:
    return {
        "cohort_id": identity["cohort_id"],
        "contract_id": identity["contract_id"],
        "geometry_contract_id": identity["geometry_contract_id"],
        "issued_at_utc": "2026-08-27T12:02:30Z",
        "data_cutoff_utc": "2026-08-27T12:01:00Z",
        "source_observed_utc": "2026-08-27T12:02:30Z",
        "instrument": "EUR_USD",
        "decision_bar_utc": "2026-08-27T11:55:00Z",
        "planned_entry_utc": "2026-08-27T12:05:00Z",
        "band_id": "band1",
        "band_version_id": "bandv1",
        "level_source": "confirmed_pivot_cluster",
        "level_name": "swing_cluster",
        "physical_role": "resistance",
        "approach_side": 1,
        "band_lower": 1.1000,
        "band_center": 1.1005,
        "band_upper": 1.1010,
        "decision_bid": 1.0998,
        "decision_ask": 1.1000,
        "pip": 0.0001,
        "spread_pips": 2.0,
        "frozen_break_price": 1.1020,
        "frozen_reject_price": 1.0990,
        "horizons_sec": list(HORIZONS_SEC),
        "features": {"selected_side": None},
        "dependencies": {"quote_time_utc": quote_time},
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "selected_side": None,
    }


def forecast_row(values: dict | None = None) -> dict:
    row = {
        "forecast_id": "f1",
        "cohort_id": "c1",
        "issued_at_utc": "2026-08-27T12:02:30Z",
        "planned_entry_utc": "2026-08-27T12:05:00Z",
        "approach_side": 1,
        "band_lower": 1.1000,
        "band_upper": 1.1010,
        "frozen_break_price": 1.1020,
        "frozen_reject_price": 1.0990,
        "pip": 0.0001,
    }
    row.update(values or {})
    return row


def entry_row() -> dict:
    return {"bid_open": 1.0999, "ask_open": 1.1001}


def test_next_entry_is_strictly_future_m5_boundary() -> None:
    assert next_m5_boundary(BASE) == datetime(2026, 8, 27, 12, 5, tzinfo=timezone.utc)
    exact = datetime(2026, 8, 27, 12, 5, tzinfo=timezone.utc)
    assert next_m5_boundary(exact) == datetime(2026, 8, 27, 12, 10, tzinfo=timezone.utc)


def test_retained_context_covers_descriptors_and_all_outcome_horizons() -> None:
    assert RETAIN_M1_ROWS >= 13
    assert RETAIN_M5_ROWS * 300 >= max(HORIZONS_SEC) + 1_800


def test_completed_report_cutoff_ignores_newer_sequential_refresh_rows(tmp_path) -> None:
    path = tmp_path / "EUR_USD_M1.csv"
    header = (
        "datetime,instrument,granularity,open,high,low,close,"
        "bid_open,bid_high,bid_low,bid_close,"
        "ask_open,ask_high,ask_low,ask_close"
    )
    start = datetime(2026, 8, 25, 0, 0, tzinfo=timezone.utc)
    rows = [header]
    for offset in range(2_002):
        timestamp = start + timedelta(minutes=offset)
        rows.append(
            f"{timestamp.isoformat().replace('+00:00', 'Z')},EUR_USD,M1,"
            "1.1000,1.1002,1.0998,1.1001,"
            "1.0999,1.1001,1.0997,1.1000,"
            "1.1001,1.1003,1.0999,1.1002"
        )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    cutoff = start + timedelta(minutes=2_000)

    bars, source = load_m1_tail(path, "EUR_USD", cutoff=cutoff)

    assert bars[-1].timestamp == cutoff
    assert source["report_cutoff_utc"] == cutoff.isoformat().replace("+00:00", "Z")
    assert source["ignored_rows_after_report_cutoff"] == 1
    assert source["file_last_utc"] != source["last_utc"]


def test_clock_state_must_be_fresh_and_trusted(tmp_path) -> None:
    path = tmp_path / "clock.json"
    path.write_text(json.dumps({
        "generated_utc": "2026-08-27T12:02:20Z",
        "status": "ok",
        "timestamp_normalization_trusted": True,
        "host_clock_synchronized": True,
    }))
    _, state = trusted_observation_time(BASE, path)
    assert state["trusted_for_prospective_evidence"] is True
    _, stale = trusted_observation_time(BASE + timedelta(minutes=3), path)
    assert stale["trusted_for_prospective_evidence"] is False


def test_live_quote_contract_requires_and_preserves_venue_pip() -> None:
    observed = datetime(2026, 8, 27, 12, 2, 30, tzinfo=timezone.utc)
    quotes = {"quotes": {"USD_HUF": {
        "bid": 312.63,
        "ask": 313.10,
        "pip": 0.01,
        "time": "2026-08-27T12:02:29Z",
        "source": "stream",
    }}}
    row, reason = quote_row(quotes, "USD_HUF", observed)
    assert reason == ""
    assert row is not None
    assert row["pip"] == pytest.approx(0.01)
    missing, reason = quote_row(
        {"quotes": {"USD_HUF": {**quotes["quotes"]["USD_HUF"], "pip": None}}},
        "USD_HUF", observed,
    )
    assert missing is None
    assert reason == "invalid_quote_number"


def test_forecasts_are_append_only_and_exact_retries_are_idempotent(tmp_path) -> None:
    db = open_ledger(tmp_path / "ledger.sqlite")
    identity = collection_identity()
    register_cohort(db, identity, BASE)
    first_id, inserted = insert_forecast(db, payload(identity))
    db.commit()
    same_id, repeated = insert_forecast(db, payload(identity))
    assert first_id == same_id
    assert inserted is True and repeated is False
    with pytest.raises(sqlite3.DatabaseError, match="append_only"):
        db.execute("UPDATE band_forecasts SET spread_pips=3 WHERE forecast_id=?", (first_id,))
    db.close()


def test_prior_source_hash_cohort_still_matures_when_contracts_match(tmp_path) -> None:
    db = open_ledger(tmp_path / "ledger.sqlite")
    current = collection_identity()
    register_cohort(db, current, BASE)
    prior = dict(current)
    prior["cohort_id"] = "level_band_prospective_prior_source_hash"
    prior["definition_sha256"] = "f" * 64
    register_cohort(db, prior, BASE - timedelta(hours=1))
    prior_id, _ = insert_forecast(db, payload(prior))
    incompatible = dict(current)
    incompatible["cohort_id"] = "level_band_prospective_other_geometry"
    incompatible["geometry_contract_id"] = "other_geometry_contract"
    incompatible["definition_sha256"] = "e" * 64
    register_cohort(db, incompatible, BASE - timedelta(hours=1))
    incompatible_id, _ = insert_forecast(db, payload(incompatible))

    compatible_ids = {
        row["forecast_id"]
        for row in compatible_forecasts_for_maturation(db, current)
    }

    assert prior_id in compatible_ids
    assert incompatible_id not in compatible_ids
    db.close()


def test_same_content_id_with_different_payload_raises_collision(tmp_path) -> None:
    db = open_ledger(tmp_path / "ledger.sqlite")
    identity = collection_identity()
    register_cohort(db, identity, BASE)
    original = payload(identity)
    insert_forecast(db, original)
    changed = payload(identity)
    changed["spread_pips"] = 9.0
    with pytest.raises(RuntimeError, match="forecast_idempotency_collision"):
        insert_forecast(db, changed)
    db.close()


def test_entry_cannot_be_written_before_future_bar_is_complete(tmp_path) -> None:
    db = open_ledger(tmp_path / "ledger.sqlite")
    identity = collection_identity()
    register_cohort(db, identity, BASE)
    forecast_id, _ = insert_forecast(db, payload(identity))
    db.commit()
    forecast = db.execute(
        "SELECT * FROM band_forecasts WHERE forecast_id=?", (forecast_id,)
    ).fetchone()
    path = [bar(0)]
    assert append_entry(db, forecast, path, datetime(2026, 8, 27, 12, 9, 59, tzinfo=timezone.utc)) is False
    assert append_entry(db, forecast, path, datetime(2026, 8, 27, 12, 10, 0, tzinfo=timezone.utc)) is True
    db.close()


def test_due_outcome_appends_to_the_frozen_schema(tmp_path) -> None:
    db = open_ledger(tmp_path / "ledger.sqlite")
    identity = collection_identity()
    register_cohort(db, identity, BASE)
    forecast_id, _ = insert_forecast(db, payload(identity))
    forecast = db.execute(
        "SELECT * FROM band_forecasts WHERE forecast_id=?", (forecast_id,)
    ).fetchone()
    path = [bar(0), bar(1), bar(2)]
    assert append_entry(
        db, forecast, path, datetime(2026, 8, 27, 12, 10, tzinfo=timezone.utc)
    ) is True
    entry = db.execute(
        "SELECT * FROM band_entries WHERE forecast_id=?", (forecast_id,)
    ).fetchone()

    inserted, reason = append_outcome(
        db, forecast, entry, path, 900,
        datetime(2026, 8, 27, 12, 20, tzinfo=timezone.utc),
    )

    assert inserted is True
    assert reason == "matured"
    stored = db.execute(
        "SELECT label,research_only,execution_eligible FROM band_outcomes"
    ).fetchone()
    assert stored is not None
    assert stored["research_only"] == 1
    assert stored["execution_eligible"] == 0
    db.close()


def test_rejection_requires_actual_contact() -> None:
    # The low crosses the rejection barrier, but price never touches the band.
    path = [bar(0, mid=1.0988, high=1.0998, low=1.0980)]
    result = mature_payload(forecast_row(), entry_row(), path, 300, BASE + timedelta(minutes=8))
    assert result["label"] == "no_contact"
    assert result["first_contact_bar_offset"] is None


def test_contact_then_reject_is_classified_and_spread_is_charged() -> None:
    path = [bar(0, mid=1.1000, high=1.1002, low=1.0988)]
    result = mature_payload(forecast_row(), entry_row(), path, 300, BASE + timedelta(minutes=8))
    assert result["label"] == "contact_reject"
    assert result["bounce_arm"]["terminal_net_pips"] == pytest.approx(-2.0)
    assert result["break_arm"]["terminal_net_pips"] == pytest.approx(-2.0)


def test_same_bar_break_and_reject_is_never_optimistically_ordered() -> None:
    path = [bar(0, mid=1.1005, high=1.1021, low=1.0989)]
    result = mature_payload(forecast_row(), entry_row(), path, 300, BASE + timedelta(minutes=8))
    assert result["label"] == "ambiguous_intrabar"
    assert result["ambiguous_bar_offset"] == 0


def test_break_then_close_back_inside_is_false_break_reclaim() -> None:
    path = [
        bar(0, mid=1.1021, high=1.1022, low=1.1001),
        bar(1, mid=1.1004, high=1.1010, low=1.1000),
    ]
    result = mature_payload(forecast_row(), entry_row(), path, 600, BASE + timedelta(minutes=13))
    assert result["label"] == "false_break_reclaim"


def test_worker_source_has_no_broker_or_authorization_import() -> None:
    source = __import__("pathlib").Path(
        __import__("oanda_causal_level_band_prospective").__file__
    ).read_text(encoding="utf-8")
    assert "import requests" not in source
    assert "read_credentials" not in source
    assert "SignalContributionFeed" not in source
    assert "PracticeExecutor" not in source
