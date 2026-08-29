#!/usr/bin/env python3
"""Cross-authority replay of the frozen RBNZ response-entry candidate.

This cohort contains every regular FOMC statement from 2024-01-31 through
2026-07-29 and two same-weekday, same-14:00-America/New_York controls per
decision.  The candidate was selected outside this FOMC sample from the RBNZ
schedule cohort and is unchanged:

* observe the first complete M1 bar after the official policy clock;
* require median absolute currency strength >= 1 bp and breadth >= 60%;
* select the cheapest agreeing direct currency leg with spread <= 5 pips;
* follow the observed response for a fixed 15-minute executable hold;
* charge the observed bid/ask spread plus 0.25 pip modeled slippage.

The policy action never supplies trade direction.  Missing detections are
explicit no-trades with zero return.  Entry and the declared exit candle are
mandatory; an interior path gap leaves fixed-horizon P/L intact but marks
MFE/MAE partial.  This is immutable retrospective research, not prospective
proof, and it cannot authorize, promote, or execute.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import random
import sqlite3
import statistics
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from oanda_spike_blurb_event_watch_price_reacquisition_v3 import (
    MAX_WORKERS,
    fetch_job,
    resolve_readonly_oanda_client,
)
from oanda_spike_blurb_rbnz_response_confirmation_v1 import (
    CONTRACT_ID as RBNZ_PARENT_CONTRACT_ID,
    LOCKED_ARM,
    LOCKED_ARM_ID,
    HOLD_MINUTES,
)
from oanda_spike_blurb_rbnz_response_confirmation_v2 import fixed_horizon_outcome
from oanda_spike_blurb_rbnz_schedule_cohort_v1 import (
    DATABASE,
    canonical_json,
    immutable_insert,
    payload_row,
    sha256_bytes,
    sha256_file,
    stable_id,
    utc_now,
)
from oanda_spike_blurb_verified_event_response_replay_v2 import detect_arm


CONTRACT_ID = "spike_blurb_fomc_response_generalization_v1_20260820"
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "fomc_response_generalization_v1"
)
FREEZE_UTC = "2026-08-20T18:00:00+00:00"
SCHEMA_VERSION = 1
EVENT_CURRENCY = "USD"
EASTERN = ZoneInfo("America/New_York")
PRE_CLOCK_MINUTES = 5
POST_CLOCK_MINUTES = 20
CONTROL_DAY_OFFSETS = (-7, 7)
RANDOMIZATION_DRAWS = 200_000
RANDOMIZATION_SEED = 20_260_820
CALENDAR_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"

DIRECT_INSTRUMENTS = (
    "AUD_USD", "EUR_USD", "GBP_USD", "NZD_USD", "USD_CAD",
    "USD_CHF", "USD_CNH", "USD_CZK", "USD_DKK", "USD_HKD",
    "USD_HUF", "USD_JPY", "USD_MXN", "USD_NOK", "USD_PLN",
    "USD_SEK", "USD_SGD", "USD_THB", "USD_TRY", "USD_ZAR",
)

# Date, exact UTC clock for 14:00 America/New_York, upper target bound,
# change in upper target bound, and official Federal Reserve statement.
SCHEDULE = (
    ("2024-01-31", "2024-01-31T19:00:00+00:00", 5.50, 0.00),
    ("2024-03-20", "2024-03-20T18:00:00+00:00", 5.50, 0.00),
    ("2024-05-01", "2024-05-01T18:00:00+00:00", 5.50, 0.00),
    ("2024-06-12", "2024-06-12T18:00:00+00:00", 5.50, 0.00),
    ("2024-07-31", "2024-07-31T18:00:00+00:00", 5.50, 0.00),
    ("2024-09-18", "2024-09-18T18:00:00+00:00", 5.00, -0.50),
    ("2024-11-07", "2024-11-07T19:00:00+00:00", 4.75, -0.25),
    ("2024-12-18", "2024-12-18T19:00:00+00:00", 4.50, -0.25),
    ("2025-01-29", "2025-01-29T19:00:00+00:00", 4.50, 0.00),
    ("2025-03-19", "2025-03-19T18:00:00+00:00", 4.50, 0.00),
    ("2025-05-07", "2025-05-07T18:00:00+00:00", 4.50, 0.00),
    ("2025-06-18", "2025-06-18T18:00:00+00:00", 4.50, 0.00),
    ("2025-07-30", "2025-07-30T18:00:00+00:00", 4.50, 0.00),
    ("2025-09-17", "2025-09-17T18:00:00+00:00", 4.25, -0.25),
    ("2025-10-29", "2025-10-29T18:00:00+00:00", 4.00, -0.25),
    ("2025-12-10", "2025-12-10T19:00:00+00:00", 3.75, -0.25),
    ("2026-01-28", "2026-01-28T19:00:00+00:00", 3.75, 0.00),
    ("2026-03-18", "2026-03-18T18:00:00+00:00", 3.75, 0.00),
    ("2026-04-29", "2026-04-29T18:00:00+00:00", 3.75, 0.00),
    ("2026-06-17", "2026-06-17T18:00:00+00:00", 3.75, 0.00),
    ("2026-07-29", "2026-07-29T18:00:00+00:00", 3.75, 0.00),
)


def statement_url(event_date: str) -> str:
    return (
        "https://www.federalreserve.gov/newsevents/pressreleases/"
        f"monetary{event_date.replace('-', '')}a.htm"
    )


def event_id(event_date: str) -> str:
    return f"fomc_policy_{event_date.replace('-', '')}_generalization_v1"


def validate_schedule() -> list[dict[str, Any]]:
    if len(SCHEDULE) != 21:
        raise RuntimeError("exact_twenty_one_regular_fomc_decisions_required")
    rows: list[dict[str, Any]] = []
    previous_clock: datetime | None = None
    previous_rate: float | None = None
    for event_date, expected_utc, upper_rate, change in SCHEDULE:
        local = datetime.combine(date.fromisoformat(event_date), dt_time(14, 0), tzinfo=EASTERN)
        utc = local.astimezone(timezone.utc)
        if utc.isoformat() != expected_utc:
            raise RuntimeError(f"eastern_clock_conversion_mismatch:{event_date}")
        if previous_clock is not None and utc <= previous_clock:
            raise RuntimeError("schedule_not_strictly_increasing")
        if previous_rate is not None and abs((upper_rate - previous_rate) - change) > 1e-9:
            raise RuntimeError(f"rate_change_inconsistent:{event_date}")
        rows.append({
            "event_id": event_id(event_date),
            "event_date": event_date,
            "event_clock_local": local.isoformat(),
            "event_clock_utc": utc.isoformat(),
            "event_epoch": int(utc.timestamp()),
            "event_currency": EVENT_CURRENCY,
            "official_target_upper_percent": float(upper_rate),
            "official_target_change_percentage_points": float(change),
            "decision_action": "cut" if change < 0 else "raise" if change > 0 else "hold",
            "release_url": statement_url(event_date),
            "calendar_url": CALENDAR_URL,
            "official_release_clock_text": "For release at 2:00 p.m. EST/EDT",
            "source_direction_assigned": 0,
            "causal_consensus_state": "missing",
            "event_time_rate_repricing_state": "missing",
            "source_retrieval_state": "retrospectively_verified_official_release",
            "research_only": 1,
            "execution_eligible": 0,
            "forecast_proof_eligible": 0,
        })
        previous_clock = utc
        previous_rate = upper_rate
    return rows


def build_clocks(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    event_dates = {date.fromisoformat(str(row["event_date"])) for row in events}
    rows: list[dict[str, Any]] = []
    for event in events:
        source_date = date.fromisoformat(str(event["event_date"]))
        for day_offset in (0, *CONTROL_DAY_OFFSETS):
            clock_date = source_date + timedelta(days=day_offset)
            if day_offset and clock_date in event_dates:
                raise RuntimeError(f"control_overlaps_policy_date:{clock_date}")
            local = datetime.combine(clock_date, dt_time(14, 0), tzinfo=EASTERN)
            utc = local.astimezone(timezone.utc)
            rows.append({
                "clock_id": stable_id("fomc_generalization_clock", CONTRACT_ID, event["event_id"], day_offset),
                "contract_id": CONTRACT_ID,
                "source_event_id": event["event_id"],
                "clock_kind": "scheduled_event" if day_offset == 0 else "matched_non_event_control",
                "day_offset": int(day_offset),
                "clock_local": local.isoformat(),
                "clock_utc": utc.isoformat(),
                "clock_epoch": int(utc.timestamp()),
                "selection_rule": (
                    "scheduled_fomc_statement_clock" if day_offset == 0
                    else "same_weekday_same_1400_america_new_york_plus_or_minus_7_days"
                ),
                "research_only": 1,
                "execution_eligible": 0,
                "forecast_proof_eligible": 0,
            })
    if len(rows) != 63 or len({str(row["clock_id"]) for row in rows}) != 63:
        raise RuntimeError("exact_sixty_three_unique_clocks_required")
    return sorted(rows, key=lambda row: (str(row["source_event_id"]), int(row["day_offset"])))


def monte_carlo_within_trio_pvalue(
    triples: list[tuple[float, float, float]],
    *,
    draws: int = RANDOMIZATION_DRAWS,
    seed: int = RANDOMIZATION_SEED,
) -> dict[str, float | int]:
    """Deterministic one-sided matched-trio randomization estimate.

    There are 3**21 possible assignments, so exhaustive enumeration would be
    impractical.  The draw count and seed are frozen in the contract and a
    plus-one correction prevents a zero p-value.
    """
    if not triples or draws <= 0:
        raise ValueError("triples_and_positive_draw_count_required")
    observed = sum(a - (b + c) / 2.0 for a, b, c in triples)
    choices = [
        (a - (b + c) / 2.0, b - (a + c) / 2.0, c - (a + b) / 2.0)
        for a, b, c in triples
    ]
    generator = random.Random(seed)
    exceedances = 0
    for _ in range(draws):
        value = sum(effects[generator.randrange(3)] for effects in choices)
        exceedances += int(value >= observed - 1e-12)
    pvalue = (exceedances + 1.0) / (draws + 1.0)
    standard_error = (pvalue * (1.0 - pvalue) / (draws + 1.0)) ** 0.5
    return {
        "draw_count": draws,
        "seed": seed,
        "exceedance_count": exceedances,
        "pvalue": pvalue,
        "monte_carlo_standard_error": standard_error,
    }


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS fomc_generalization_contracts (
          contract_id TEXT PRIMARY KEY, parent_candidate_contract_id TEXT NOT NULL,
          contract_json TEXT NOT NULL, builder_sha256 TEXT NOT NULL,
          dependency_sha256_json TEXT NOT NULL, frozen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fomc_generalization_events (
          event_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, event_date TEXT NOT NULL,
          event_clock_local TEXT NOT NULL, event_clock_utc TEXT NOT NULL,
          event_currency TEXT NOT NULL, official_target_upper_percent REAL NOT NULL,
          official_target_change_percentage_points REAL NOT NULL, decision_action TEXT NOT NULL,
          release_url TEXT NOT NULL, calendar_url TEXT NOT NULL,
          official_release_clock_text TEXT NOT NULL, source_direction_assigned INTEGER NOT NULL,
          causal_consensus_state TEXT NOT NULL, event_time_rate_repricing_state TEXT NOT NULL,
          source_retrieval_state TEXT NOT NULL, event_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL, UNIQUE(contract_id,event_date)
        );
        CREATE TABLE IF NOT EXISTS fomc_generalization_clocks (
          clock_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, source_event_id TEXT NOT NULL,
          clock_kind TEXT NOT NULL, day_offset INTEGER NOT NULL, clock_local TEXT NOT NULL,
          clock_utc TEXT NOT NULL, selection_rule TEXT NOT NULL, clock_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL, research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL, forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,source_event_id,day_offset)
        );
        CREATE TABLE IF NOT EXISTS fomc_generalization_price_windows (
          window_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, clock_id TEXT NOT NULL,
          instrument TEXT NOT NULL, requested_start_utc TEXT NOT NULL,
          requested_end_utc TEXT NOT NULL, coverage_state TEXT NOT NULL,
          candle_count INTEGER NOT NULL, first_utc TEXT, last_utc TEXT,
          average_spread_pips REAL, maximum_spread_pips REAL,
          http_status INTEGER NOT NULL, latency_ms INTEGER NOT NULL,
          error_text TEXT NOT NULL, payload_sha256 TEXT NOT NULL, payload_gzip BLOB NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL, UNIQUE(contract_id,clock_id,instrument)
        );
        CREATE TABLE IF NOT EXISTS fomc_generalization_decisions (
          decision_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, clock_id TEXT NOT NULL,
          source_event_id TEXT NOT NULL, clock_kind TEXT NOT NULL, day_offset INTEGER NOT NULL,
          arm_id TEXT NOT NULL, hold_minutes INTEGER NOT NULL, detected INTEGER NOT NULL,
          detection_utc TEXT, currency_direction TEXT, currency_strength_bps REAL,
          breadth REAL, selected_instrument TEXT, selected_pair_direction TEXT,
          entry_spread_pips REAL, net_after_cost_pips REAL NOT NULL, mfe_pips REAL,
          mae_pips REAL, cost_cleared INTEGER NOT NULL, path_expected_bars INTEGER NOT NULL,
          path_observed_bars INTEGER NOT NULL, path_complete INTEGER NOT NULL,
          path_quality TEXT NOT NULL, decision_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,clock_id,arm_id,hold_minutes)
        );
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_contract_no_update BEFORE UPDATE ON fomc_generalization_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_contract_no_delete BEFORE DELETE ON fomc_generalization_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_event_no_update BEFORE UPDATE ON fomc_generalization_events BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_event_no_delete BEFORE DELETE ON fomc_generalization_events BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_clock_no_update BEFORE UPDATE ON fomc_generalization_clocks BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_clock_no_delete BEFORE DELETE ON fomc_generalization_clocks BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_price_no_update BEFORE UPDATE ON fomc_generalization_price_windows BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_price_no_delete BEFORE DELETE ON fomc_generalization_price_windows BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_decision_no_update BEFORE UPDATE ON fomc_generalization_decisions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_decision_no_delete BEFORE DELETE ON fomc_generalization_decisions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def freeze_contract(
    connection: sqlite3.Connection,
    events: list[dict[str, Any]],
    clocks: list[dict[str, Any]],
) -> dict[str, Any]:
    dependency_paths = (
        Path(__file__).resolve().parent / "oanda_spike_blurb_event_watch_price_reacquisition_v3.py",
        Path(__file__).resolve().parent / "oanda_spike_blurb_rbnz_response_confirmation_v1.py",
        Path(__file__).resolve().parent / "oanda_spike_blurb_rbnz_response_confirmation_v2.py",
        Path(__file__).resolve().parent / "oanda_spike_blurb_verified_event_response_replay_v2.py",
    )
    dependencies = {path.name: sha256_file(path) for path in dependency_paths}
    contract = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "parent_candidate_contract_id": RBNZ_PARENT_CONTRACT_ID,
        "builder_sha256": sha256_file(Path(__file__).resolve()),
        "dependency_sha256": dependencies,
        "selection_state": "candidate_selected_on_rbnz_and_frozen_before_any_fomc_price_query",
        "temporal_role": "independent_authority_retrospective_generalization_replay",
        "event_population": "every_regular_fomc_statement_20240131_through_20260729",
        "candidate": {
            "event_clock": "official_1400_america_new_york",
            "direction_source": "first_complete_m1_cross_pair_response_only",
            "arm_id": LOCKED_ARM_ID,
            "arm": LOCKED_ARM,
            "hold_minutes": HOLD_MINUTES,
            "instrument_universe": list(DIRECT_INSTRUMENTS),
            "instrument_count": len(DIRECT_INSTRUMENTS),
            "maximum_entry_spread_pips": 5.0,
            "modeled_slippage_pips": 0.25,
            "missing_detection_policy": "explicit_no_trade_zero_return",
        },
        "price_coverage_policy": {
            "all_direct_usd_windows_required": True,
            "entry_candle_required": True,
            "declared_exit_candle_required": True,
            "interior_bar_gaps_preserve_fixed_horizon_net_but_mark_mfe_mae_partial": True,
        },
        "matched_controls": {
            "day_offsets": list(CONTROL_DAY_OFFSETS),
            "clock_rule": "same_weekday_same_1400_america_new_york",
            "predeclared_with_candidate": True,
        },
        "inference": {
            "method": "deterministic_monte_carlo_within_matched_trio_randomization",
            "draw_count": RANDOMIZATION_DRAWS,
            "seed": RANDOMIZATION_SEED,
            "plus_one_correction": True,
        },
        "diagnostic_acceptance": {
            "treatment_average_net_pips_min_exclusive": 0.0,
            "incremental_average_net_pips_min_exclusive": 0.0,
            "treatment_cost_clearance_rate_min": 2.0 / 3.0,
            "minimum_leave_one_out_treatment_average_min_exclusive": 0.0,
            "randomization_pvalue_max": 0.05,
        },
        "event_count": len(events),
        "clock_count": len(clocks),
        "research_only": True,
        "execution_eligible": False,
        "forecast_proof_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    encoded = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json FROM fomc_generalization_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    if existing is not None and str(existing[0]) != encoded:
        raise RuntimeError("immutable_fomc_generalization_contract_conflict")
    if existing is None:
        connection.execute(
            "INSERT INTO fomc_generalization_contracts VALUES (?,?,?,?,?,?)",
            (
                CONTRACT_ID, RBNZ_PARENT_CONTRACT_ID, encoded,
                contract["builder_sha256"], canonical_json(dependencies), FREEZE_UTC,
            ),
        )
    for event in events:
        stored = {key: value for key, value in event.items() if key != "event_epoch"}
        stored["contract_id"] = CONTRACT_ID
        immutable_insert(connection, "fomc_generalization_events", payload_row(stored, "event_json"), "event_id")
    for clock in clocks:
        stored = {key: value for key, value in clock.items() if key != "clock_epoch"}
        immutable_insert(connection, "fomc_generalization_clocks", payload_row(stored, "clock_json"), "clock_id")
    connection.commit()
    return contract


def request_jobs(clocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for clock in clocks:
        event_time = datetime.fromisoformat(str(clock["clock_utc"]))
        start = event_time - timedelta(minutes=PRE_CLOCK_MINUTES)
        end = event_time + timedelta(minutes=POST_CLOCK_MINUTES)
        for instrument in DIRECT_INSTRUMENTS:
            jobs.append({
                "case_id": clock["clock_id"],
                "clock_id": clock["clock_id"],
                "instrument": instrument,
                "start_epoch": int(start.timestamp()),
                "end_epoch": int(end.timestamp()),
                "start_utc": start.isoformat(),
                "end_utc": end.isoformat(),
            })
    return jobs


def store_price(connection: sqlite3.Connection, result: Mapping[str, Any]) -> None:
    payload = {
        "window_id": stable_id("fomc_generalization_price", CONTRACT_ID, result["clock_id"], result["instrument"]),
        "contract_id": CONTRACT_ID,
        "clock_id": result["clock_id"],
        "instrument": result["instrument"],
        "requested_start_utc": result["start_utc"],
        "requested_end_utc": result["end_utc"],
        "coverage_state": result["coverage_state"],
        "candle_count": int(result["candle_count"]),
        "first_utc": result["first_utc"],
        "last_utc": result["last_utc"],
        "average_spread_pips": result["average_spread_pips"],
        "maximum_spread_pips": result["maximum_spread_pips"],
        "http_status": int(result["http_status"]),
        "latency_ms": int(result["latency_ms"]),
        "error_text": result["error_text"],
        "payload_sha256": result["payload_sha256"],
        "payload_gzip": result["payload_gzip"],
        "research_only": 1,
        "execution_eligible": 0,
        "forecast_proof_eligible": 0,
    }
    columns = list(payload)
    connection.execute(
        f"INSERT OR IGNORE INTO fomc_generalization_price_windows ({','.join(columns)}) "
        f"VALUES ({','.join('?' for _ in columns)})",
        [payload[column] for column in columns],
    )
    existing = connection.execute(
        "SELECT payload_sha256 FROM fomc_generalization_price_windows WHERE window_id=?",
        (payload["window_id"],),
    ).fetchone()
    if existing is None or str(existing[0]) != str(payload["payload_sha256"]):
        raise RuntimeError(f"immutable_fomc_price_conflict:{payload['window_id']}")


def load_paths(
    connection: sqlite3.Connection,
    clocks: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], str]:
    output = {
        str(clock["clock_id"]): {
            **clock,
            "event_epoch": int(clock["clock_epoch"]),
            "event_currency": EVENT_CURRENCY,
            "paths": {},
        }
        for clock in clocks
    }
    seen: dict[str, set[str]] = defaultdict(set)
    digest = hashlib.sha256()
    for row in connection.execute(
        "SELECT clock_id,instrument,coverage_state,payload_sha256,payload_gzip "
        "FROM fomc_generalization_price_windows WHERE contract_id=? ORDER BY clock_id,instrument",
        (CONTRACT_ID,),
    ):
        clock_id, instrument, state, payload_hash = map(str, row[:4])
        seen[clock_id].add(instrument)
        digest.update(canonical_json([clock_id, instrument, state, payload_hash]).encode())
        if state != "exact_window":
            raise RuntimeError(f"fomc_requires_exact_direct_path:{clock_id}:{instrument}:{state}")
        decoded = gzip.decompress(row[4])
        if sha256_bytes(decoded) != payload_hash:
            raise RuntimeError(f"fomc_price_hash_mismatch:{clock_id}:{instrument}")
        body = json.loads(decoded)
        candle_map = {int(candle["epoch"]): candle for candle in body["candles"]}
        if len(candle_map) != len(body["candles"]):
            raise RuntimeError(f"duplicate_fomc_price_clock:{clock_id}:{instrument}")
        output[clock_id]["paths"][instrument] = candle_map
    expected = set(DIRECT_INSTRUMENTS)
    for clock_id, item in output.items():
        if seen[clock_id] != expected or set(item["paths"]) != expected:
            raise RuntimeError(f"fomc_direct_universe_incomplete:{clock_id}")
    return output, digest.hexdigest()


def evaluate_clock(clock: Mapping[str, Any]) -> dict[str, Any]:
    detected = detect_arm(clock, LOCKED_ARM_ID, LOCKED_ARM)
    base = {
        "decision_id": stable_id("fomc_generalization_decision", CONTRACT_ID, clock["clock_id"], LOCKED_ARM_ID, HOLD_MINUTES),
        "contract_id": CONTRACT_ID,
        "clock_id": clock["clock_id"],
        "source_event_id": clock["source_event_id"],
        "clock_kind": clock["clock_kind"],
        "day_offset": int(clock["day_offset"]),
        "arm_id": LOCKED_ARM_ID,
        "hold_minutes": HOLD_MINUTES,
        "research_only": 1,
        "execution_eligible": 0,
        "forecast_proof_eligible": 0,
    }
    if detected is None:
        return {
            **base, "detected": 0, "detection_utc": None,
            "currency_direction": None, "currency_strength_bps": None,
            "breadth": None, "selected_instrument": None,
            "selected_pair_direction": None, "entry_spread_pips": None,
            "net_after_cost_pips": 0.0, "mfe_pips": None, "mae_pips": None,
            "cost_cleared": 0, "path_expected_bars": HOLD_MINUTES,
            "path_observed_bars": 0, "path_complete": 0,
            "path_quality": "no_trade_no_detection",
        }
    instrument = str(detected["selected_instrument"])
    result = fixed_horizon_outcome(detected, clock["paths"][instrument])
    return {
        **base,
        "detected": 1,
        "detection_utc": datetime.fromtimestamp(int(detected["detection_epoch"]), timezone.utc).isoformat(),
        "currency_direction": detected["currency_direction"],
        "currency_strength_bps": float(detected["currency_strength_bps"]),
        "breadth": float(detected["breadth"]),
        "selected_instrument": instrument,
        "selected_pair_direction": detected["selected_pair_direction"],
        "entry_spread_pips": float(detected["entry_spread_pips"]),
        **result,
    }


def store_decisions(
    connection: sqlite3.Connection,
    loaded: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in sorted(loaded.values(), key=lambda row: str(row["clock_utc"])):
        row = payload_row(evaluate_clock(item), "decision_json")
        immutable_insert(connection, "fomc_generalization_decisions", row, "decision_id")
        rows.append(row)
    connection.commit()
    return rows


def analyze(events: list[dict[str, Any]], decisions: list[dict[str, Any]]) -> dict[str, Any]:
    by_event: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    for row in decisions:
        by_event[str(row["source_event_id"])][int(row["day_offset"])] = row
    triples: list[tuple[float, float, float]] = []
    event_rows: list[dict[str, Any]] = []
    for event in events:
        event_key = str(event["event_id"])
        if set(by_event[event_key]) != {-7, 0, 7}:
            raise RuntimeError(f"exact_fomc_trio_required:{event_key}")
        treatment, minus, plus = (by_event[event_key][offset] for offset in (0, -7, 7))
        triple = tuple(float(row["net_after_cost_pips"]) for row in (treatment, minus, plus))
        triples.append(triple)  # type: ignore[arg-type]
        event_rows.append({
            "event_id": event_key,
            "event_date": event["event_date"],
            "decision_action": event["decision_action"],
            "rate_change_percentage_points": event["official_target_change_percentage_points"],
            "treatment_detected": int(treatment["detected"]),
            "treatment_direction": treatment["currency_direction"],
            "treatment_instrument": treatment["selected_instrument"],
            "treatment_net_pips": triple[0],
            "control_minus_7d_net_pips": triple[1],
            "control_plus_7d_net_pips": triple[2],
            "incremental_net_pips": triple[0] - (triple[1] + triple[2]) / 2.0,
        })
    treatment_values = [row[0] for row in triples]
    control_values = [value for row in triples for value in row[1:]]
    increments = [row[0] - (row[1] + row[2]) / 2.0 for row in triples]
    leave_one_out = [
        statistics.mean(value for index, value in enumerate(treatment_values) if index != omitted)
        for omitted in range(len(treatment_values))
    ]
    treatment_average = statistics.mean(treatment_values)
    control_average = statistics.mean(control_values)
    incremental_average = statistics.mean(increments)
    treatment_clearance = statistics.mean(
        int(by_event[str(event["event_id"])][0]["cost_cleared"]) for event in events
    )
    randomization = monte_carlo_within_trio_pvalue(triples)
    criteria = {
        "positive_treatment_average": treatment_average > 0.0,
        "positive_incremental_average": incremental_average > 0.0,
        "cost_clearance_at_least_two_thirds": treatment_clearance >= 2.0 / 3.0,
        "positive_minimum_leave_one_out_treatment_average": min(leave_one_out) > 0.0,
        "randomization_pvalue_at_most_0_05": float(randomization["pvalue"]) <= 0.05,
    }
    action_summary: dict[str, dict[str, float | int]] = {}
    for action in ("hold", "cut", "raise"):
        action_rows = [row for row in event_rows if row["decision_action"] == action]
        if action_rows:
            action_summary[action] = {
                "event_count": len(action_rows),
                "average_treatment_net_pips": statistics.mean(float(row["treatment_net_pips"]) for row in action_rows),
                "average_incremental_net_pips": statistics.mean(float(row["incremental_net_pips"]) for row in action_rows),
                "usd_stronger_count": sum(row["treatment_direction"] == "stronger" for row in action_rows),
                "usd_weaker_count": sum(row["treatment_direction"] == "weaker" for row in action_rows),
            }
    return {
        "independent_scheduled_event_count": len(events),
        "matched_control_count": len(events) * 2,
        "treatment_average_net_pips": treatment_average,
        "control_average_net_pips": control_average,
        "incremental_average_net_pips": incremental_average,
        "incremental_median_net_pips": statistics.median(increments),
        "incremental_positive_event_rate": statistics.mean(value > 0 for value in increments),
        "treatment_cost_clearance_rate": treatment_clearance,
        "treatment_no_trade_count": sum(int(by_event[str(event["event_id"])][0]["detected"]) == 0 for event in events),
        "control_no_trade_count": sum(int(row["detected"]) == 0 for row in decisions if int(row["day_offset"]) != 0),
        "minimum_leave_one_event_out_treatment_average_pips": min(leave_one_out),
        "within_trio_randomization": randomization,
        "diagnostic_acceptance_criteria": criteria,
        "diagnostic_generalization_passed": all(criteria.values()),
        "action_summary": action_summary,
        "event_rows": event_rows,
    }


def write_report(result: Mapping[str, Any], report_root: Path) -> None:
    report_root.mkdir(parents=True, exist_ok=True)
    (report_root / "FOMC_RESPONSE_GENERALIZATION_V1.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    analysis = result["analysis"]
    lines = [
        "# Frozen FOMC response generalization V1", "",
        "- The candidate was selected on RBNZ and frozen before any FOMC price query.",
        "- Direction comes only from the observed first complete M1 USD response; the official action is unsigned.",
        "- All regular FOMC statements from 2024-01-31 through 2026-07-29 are included.",
        "- This is cross-authority retrospective generalization, not prospective proof.",
        "- Execution decision: **no_trade**.", "", "## Result", "",
        f"- Scheduled events: **{analysis['independent_scheduled_event_count']}**; controls: **{analysis['matched_control_count']}**.",
        f"- Treatment average: **{analysis['treatment_average_net_pips']:.3f} pips**.",
        f"- Control average: **{analysis['control_average_net_pips']:.3f} pips**.",
        f"- Incremental average: **{analysis['incremental_average_net_pips']:.3f} pips**.",
        f"- Treatment cost-clearance: **{analysis['treatment_cost_clearance_rate']:.1%}**.",
        f"- Minimum leave-one-event-out treatment average: **{analysis['minimum_leave_one_event_out_treatment_average_pips']:.3f} pips**.",
        f"- Matched-trio randomization p: **{analysis['within_trio_randomization']['pvalue']:.6f}** ({analysis['within_trio_randomization']['draw_count']:,} frozen draws).",
        f"- Predeclared diagnostic generalization passed: **{analysis['diagnostic_generalization_passed']}**.",
        f"- Partial detected paths: **{result['partial_detected_path_count']}**.",
        "", "## Event rows", "",
        "| Date | Action | USD response | Pair | Treatment | -7d | +7d | Incremental |",
        "|---|---|---|---|---:|---:|---:|---:|",
    ]
    for row in analysis["event_rows"]:
        lines.append(
            f"| {row['event_date']} | {row['decision_action']} | {row['treatment_direction'] or 'no_trade'} | "
            f"{row['treatment_instrument'] or '-'} | {row['treatment_net_pips']:.3f} | "
            f"{row['control_minus_7d_net_pips']:.3f} | {row['control_plus_7d_net_pips']:.3f} | "
            f"{row['incremental_net_pips']:.3f} |"
        )
    lines.append("")
    (report_root / "FOMC_RESPONSE_GENERALIZATION_V1.md").write_text(
        "\n".join(lines), encoding="utf-8"
    )


def run(
    database: Path = DATABASE,
    report_root: Path = REPORT_ROOT,
    workers: int = MAX_WORKERS,
    *,
    freeze_only: bool = False,
) -> dict[str, Any]:
    events = validate_schedule()
    clocks = build_clocks(events)
    connection = sqlite3.connect(database, timeout=120)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    ensure_schema(connection)
    contract = freeze_contract(connection, events, clocks)
    if freeze_only:
        result = {
            "contract_id": CONTRACT_ID,
            "parent_candidate_contract_id": RBNZ_PARENT_CONTRACT_ID,
            "builder_sha256": contract["builder_sha256"],
            "event_count": len(events),
            "clock_count": len(clocks),
            "instrument_count": len(DIRECT_INSTRUMENTS),
            "selection_state": contract["selection_state"],
            "research_only": True,
            "execution_eligible": False,
            "forecast_proof_eligible": False,
            "supported_execution_decision": "no_trade",
        }
        connection.close()
        return result
    jobs = request_jobs(clocks)
    existing = {
        (str(row[0]), str(row[1]))
        for row in connection.execute(
            "SELECT clock_id,instrument FROM fomc_generalization_price_windows WHERE contract_id=?",
            (CONTRACT_ID,),
        )
    }
    pending = [job for job in jobs if (str(job["clock_id"]), str(job["instrument"])) not in existing]
    if pending:
        client, metadata = resolve_readonly_oanda_client()
        if metadata.get("environment") != "practice" or "api-fxpractice.oanda.com" not in str(metadata.get("base_url", "")):
            raise RuntimeError("practice_readonly_endpoint_required")
        with ThreadPoolExecutor(max_workers=max(1, min(int(workers), MAX_WORKERS))) as executor:
            futures = {executor.submit(fetch_job, client, job): job for job in pending}
            for future in as_completed(futures):
                store_price(connection, future.result())
                connection.commit()
    loaded, price_snapshot = load_paths(connection, clocks)
    decisions = store_decisions(connection, loaded)
    analysis = analyze(events, decisions)
    states = dict(connection.execute(
        "SELECT coverage_state,count(*) FROM fomc_generalization_price_windows WHERE contract_id=? GROUP BY coverage_state",
        (CONTRACT_ID,),
    ).fetchall())
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    execution_sum = sum(
        int(connection.execute(
            f"SELECT coalesce(sum(execution_eligible),0) FROM {table} WHERE contract_id=?",
            (CONTRACT_ID,),
        ).fetchone()[0])
        for table in (
            "fomc_generalization_events", "fomc_generalization_clocks",
            "fomc_generalization_price_windows", "fomc_generalization_decisions",
        )
    )
    partial_count = sum(int(row["detected"]) == 1 and int(row["path_complete"]) == 0 for row in decisions)
    stable = {
        "contract_id": CONTRACT_ID,
        "parent_candidate_contract_id": RBNZ_PARENT_CONTRACT_ID,
        "builder_sha256": contract["builder_sha256"],
        "price_snapshot_sha256": price_snapshot,
        "event_count": len(events), "clock_count": len(clocks),
        "instrument_count": len(DIRECT_INSTRUMENTS),
        "price_window_count": sum(int(value) for value in states.values()),
        "coverage_states": states, "decision_count": len(decisions),
        "partial_detected_path_count": partial_count,
        "analysis": analysis, "sqlite_integrity": integrity,
        "execution_eligible_count": execution_sum,
        "proof_state": "cross_authority_retrospective_generalization_not_prospective_live_proof",
        "research_only": True, "execution_eligible": False,
        "forecast_proof_eligible": False, "supported_execution_decision": "no_trade",
    }
    result = {
        "schema_version": SCHEMA_VERSION, "generated_utc": utc_now(),
        "fetched_this_run": len(pending), **stable,
        "snapshot_sha256": sha256_bytes(canonical_json(stable).encode()),
    }
    connection.close()
    if result["price_window_count"] != len(clocks) * len(DIRECT_INSTRUMENTS):
        raise RuntimeError("fomc_price_coverage_incomplete")
    if len(decisions) != len(clocks):
        raise RuntimeError("fomc_decision_coverage_incomplete")
    if integrity != "ok" or execution_sum != 0:
        raise RuntimeError("fomc_integrity_or_safety_failure")
    write_report(result, report_root)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--freeze-only", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args.database, args.report_root, args.workers, freeze_only=args.freeze_only), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
