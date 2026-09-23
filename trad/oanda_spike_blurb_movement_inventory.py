#!/usr/bin/env python3
"""Build the immutable all-68, 1-minute-to-30-day movement inventory.

This is outcome-selected research used to discover source/factor hypotheses.  It
uses executable bid/ask paths, stores explicit costs, and keeps unavailable
legacy history unavailable.  It is never forecast proof or an execution input.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from oanda_instrument_pips import fallback_pip_size
from oanda_spike_blurb_factor_reconstruction import (
    CANDLES,
    CONFIG,
    LEGACY_TAGS,
    REPORT_ROOT,
    atomic_text,
    canonical_json,
    file_sha256,
    iso_epoch,
    load_contract,
    parse_epoch,
    sha256_bytes,
    utc_now,
)


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
DATABASE = STATE / "spike_blurb_factor_reconstruction_v1.sqlite"
OUTPUT_JSON = REPORT_ROOT / "SPIKE_BLURB_MOVEMENT_INVENTORY_V1.json"
OUTPUT_MD = REPORT_ROOT / "SPIKE_BLURB_MOVEMENT_INVENTORY_V1.md"
BUILD_CONTRACT_ID = "spike_blurb_movement_inventory_v1_20260819"
MODELED_SLIPPAGE_PIPS = 0.25


def stable_id(prefix: str, *parts: Any) -> str:
    material = "|".join(str(part) for part in parts)
    return f"{prefix}_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:24]}"


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    database = sqlite3.connect(path, timeout=120)
    database.execute("PRAGMA journal_mode=WAL")
    database.execute("PRAGMA synchronous=FULL")
    database.execute("PRAGMA foreign_keys=ON")
    database.executescript(
        """
        CREATE TABLE IF NOT EXISTS reconstruction_contracts (
          build_contract_id TEXT PRIMARY KEY,
          parent_contract_id TEXT NOT NULL,
          cohort_id TEXT NOT NULL,
          created_utc TEXT NOT NULL,
          config_sha256 TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL,
          price_manifest_sha256 TEXT NOT NULL,
          contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS movement_candidates (
          candidate_id TEXT PRIMARY KEY,
          build_contract_id TEXT NOT NULL,
          instrument TEXT NOT NULL,
          base_currency TEXT NOT NULL,
          quote_currency TEXT NOT NULL,
          entry_utc TEXT NOT NULL,
          exit_utc TEXT NOT NULL,
          entry_epoch INTEGER NOT NULL,
          exit_epoch INTEGER NOT NULL,
          horizon_min INTEGER NOT NULL,
          selected_side TEXT NOT NULL,
          signed_currency_factor TEXT NOT NULL,
          market_episode_id TEXT NOT NULL,
          selection_tier TEXT NOT NULL,
          selection_quantile REAL NOT NULL,
          selection_threshold_pips REAL NOT NULL,
          entry_bid REAL NOT NULL,
          entry_ask REAL NOT NULL,
          exit_bid REAL NOT NULL,
          exit_ask REAL NOT NULL,
          entry_spread_pips REAL NOT NULL,
          exit_spread_pips REAL NOT NULL,
          modeled_slippage_pips REAL NOT NULL,
          gross_magnitude_pips REAL NOT NULL,
          after_cost_pips REAL NOT NULL,
          actual_cost_pips REAL NOT NULL,
          gross_cost_multiple REAL NOT NULL,
          mfe_pips REAL NOT NULL,
          mae_pips REAL NOT NULL,
          path_efficiency REAL NOT NULL,
          first_cost_clear_sec INTEGER,
          price_file_sha256 TEXT NOT NULL,
          outcome_selected INTEGER NOT NULL CHECK(outcome_selected=1),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          research_only INTEGER NOT NULL CHECK(research_only=1),
          created_utc TEXT NOT NULL,
          FOREIGN KEY(build_contract_id) REFERENCES reconstruction_contracts(build_contract_id)
        );
        CREATE INDEX IF NOT EXISTS movement_candidates_pair_horizon_time
          ON movement_candidates(instrument,horizon_min,entry_epoch);
        CREATE INDEX IF NOT EXISTS movement_candidates_episode
          ON movement_candidates(market_episode_id,signed_currency_factor);
        CREATE TABLE IF NOT EXISTS opportunity_census (
          census_id TEXT PRIMARY KEY,
          build_contract_id TEXT NOT NULL,
          instrument TEXT NOT NULL,
          horizon_min INTEGER NOT NULL,
          start_date_utc TEXT NOT NULL,
          valid_start_count INTEGER NOT NULL,
          raw_cost_clear_count INTEGER NOT NULL,
          chronological_nonoverlap_cost_clear_count INTEGER NOT NULL,
          selected_q95_count INTEGER NOT NULL,
          selected_q99_count INTEGER NOT NULL,
          mean_best_after_cost_pips REAL,
          median_best_after_cost_pips REAL,
          p95_best_after_cost_pips REAL,
          mean_actual_cost_pips REAL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          FOREIGN KEY(build_contract_id) REFERENCES reconstruction_contracts(build_contract_id)
        );
        CREATE INDEX IF NOT EXISTS opportunity_census_horizon_date
          ON opportunity_census(horizon_min,start_date_utc);
        CREATE TABLE IF NOT EXISTS legacy_gap_reconciliation (
          reconciliation_id TEXT PRIMARY KEY,
          build_contract_id TEXT NOT NULL,
          move_id TEXT NOT NULL,
          instrument TEXT NOT NULL,
          start_utc TEXT NOT NULL,
          end_utc TEXT NOT NULL,
          source_mapping_state TEXT NOT NULL,
          retained_direction TEXT,
          current_price_archive_state TEXT NOT NULL,
          required_action TEXT NOT NULL,
          causal_relation TEXT,
          primary_event_id TEXT,
          outcome_selected INTEGER NOT NULL CHECK(outcome_selected=1),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          research_only INTEGER NOT NULL CHECK(research_only=1),
          FOREIGN KEY(build_contract_id) REFERENCES reconstruction_contracts(build_contract_id),
          UNIQUE(build_contract_id,move_id)
        );
        CREATE TRIGGER IF NOT EXISTS reconstruction_contracts_no_update
          BEFORE UPDATE ON reconstruction_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS reconstruction_contracts_no_delete
          BEFORE DELETE ON reconstruction_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS movement_candidates_no_update
          BEFORE UPDATE ON movement_candidates BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS movement_candidates_no_delete
          BEFORE DELETE ON movement_candidates BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS opportunity_census_no_update
          BEFORE UPDATE ON opportunity_census BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS opportunity_census_no_delete
          BEFORE DELETE ON opportunity_census BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS legacy_gap_reconciliation_no_update
          BEFORE UPDATE ON legacy_gap_reconciliation BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS legacy_gap_reconciliation_no_delete
          BEFORE DELETE ON legacy_gap_reconciliation BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )
    return database


def load_price(path: Path, expected_instrument: str) -> dict[str, Any]:
    columns = [
        "time",
        "datetime",
        "instrument",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_high",
        "ask_low",
        "ask_close",
    ]
    frame = pd.read_csv(path, usecols=columns, low_memory=False)
    frame["timestamp"] = pd.to_datetime(
        frame["time"].where(frame["time"].notna(), frame["datetime"]),
        utc=True,
        errors="coerce",
    )
    numeric = [column for column in columns if column.startswith(("bid_", "ask_"))]
    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = (
        frame.dropna(subset=["timestamp", *numeric])
        .sort_values("timestamp")
        .drop_duplicates("timestamp", keep="last")
        .reset_index(drop=True)
    )
    observed = set(frame["instrument"].dropna().astype(str))
    if observed != {expected_instrument}:
        raise ValueError(f"instrument_identity_mismatch:{expected_instrument}:{sorted(observed)}")
    frame["epoch"] = frame["timestamp"].astype("int64") // 1_000_000_000
    frame["mid"] = (frame["bid_close"] + frame["ask_close"]) / 2.0
    pip = fallback_pip_size(expected_instrument)
    frame["spread_pips"] = (frame["ask_close"] - frame["bid_close"]) / pip
    if frame.empty or (frame["spread_pips"] <= 0).any():
        raise ValueError(f"invalid_executable_price_file:{expected_instrument}")
    return {
        "frame": frame,
        "pip": pip,
        "sha256": file_sha256(path),
        "first_epoch": int(frame["epoch"].iloc[0]),
        "last_epoch": int(frame["epoch"].iloc[-1]),
    }


def _nonoverlap_best(epochs: np.ndarray, scores: np.ndarray, minimum_distance_sec: int) -> np.ndarray:
    """Select the best-scored starts with no other selected start inside the horizon."""
    if len(epochs) == 0:
        return np.array([], dtype=np.int64)
    order = np.lexsort((epochs, -scores))
    selected_epochs: list[int] = []
    selected_indices: list[int] = []
    distance = int(minimum_distance_sec)
    for index in order:
        epoch = int(epochs[index])
        location = bisect.bisect_left(selected_epochs, epoch)
        left_ok = location == 0 or epoch - selected_epochs[location - 1] >= distance
        right_ok = location == len(selected_epochs) or selected_epochs[location] - epoch >= distance
        if left_ok and right_ok:
            selected_epochs.insert(location, epoch)
            selected_indices.append(int(index))
    return np.array(sorted(selected_indices, key=lambda index: int(epochs[index])), dtype=np.int64)


def _chronological_nonoverlap_count(epochs: np.ndarray, horizon_sec: int) -> int:
    count = 0
    next_allowed: int | None = None
    for epoch in np.sort(epochs):
        value = int(epoch)
        if next_allowed is None or value >= next_allowed:
            count += 1
            next_allowed = value + int(horizon_sec)
    return count


def _first_cost_clear_sec(
    *,
    frame: pd.DataFrame,
    entry_position: int,
    exit_position: int,
    side: str,
    pip: float,
    slippage: float,
) -> int | None:
    entry = frame.iloc[entry_position]
    path = frame.iloc[entry_position + 1 : exit_position + 1]
    if path.empty:
        return None
    if side == "long":
        executable = (path["bid_close"].to_numpy() - float(entry["ask_close"])) / pip - slippage
    else:
        executable = (float(entry["bid_close"]) - path["ask_close"].to_numpy()) / pip - slippage
    hits = np.flatnonzero(executable > 0.0)
    if not len(hits):
        return None
    hit_epoch = int(path.iloc[int(hits[0])]["epoch"])
    return hit_epoch - int(entry["epoch"])


def build_pair_horizon(
    *,
    price: Mapping[str, Any],
    instrument: str,
    horizon_min: int,
    selection_quantiles: Sequence[float],
    minimum_net_cost_multiple: float,
    build_contract_id: str,
    created_utc: str,
    modeled_slippage_pips: float = MODELED_SLIPPAGE_PIPS,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    frame: pd.DataFrame = price["frame"]
    pip = float(price["pip"])
    epochs = frame["epoch"].to_numpy(dtype=np.int64)
    epoch_index = pd.Index(epochs)
    horizon_sec = int(horizon_min) * 60
    exit_positions = epoch_index.get_indexer(epochs + horizon_sec)
    entry_positions = np.flatnonzero(exit_positions >= 0)
    if not len(entry_positions):
        return [], [], {"valid": 0, "cost_clear": 0, "selected": 0}
    exits = exit_positions[entry_positions]
    entry_bid = frame["bid_close"].to_numpy()[entry_positions]
    entry_ask = frame["ask_close"].to_numpy()[entry_positions]
    entry_mid = frame["mid"].to_numpy()[entry_positions]
    exit_bid = frame["bid_close"].to_numpy()[exits]
    exit_ask = frame["ask_close"].to_numpy()[exits]
    exit_mid = frame["mid"].to_numpy()[exits]
    long_net = (exit_bid - entry_ask) / pip - modeled_slippage_pips
    short_net = (entry_bid - exit_ask) / pip - modeled_slippage_pips
    best = np.maximum(long_net, short_net)
    gross = np.abs(exit_mid - entry_mid) / pip
    actual_cost = gross - best
    cost_multiple = np.divide(gross, actual_cost, out=np.full_like(gross, np.inf), where=actual_cost > 0)
    cost_clear = (best > 0.0) & (cost_multiple >= float(minimum_net_cost_multiple))
    positive_indices = np.flatnonzero(cost_clear)
    if not len(positive_indices):
        return [], _daily_census(
            frame=frame,
            entry_positions=entry_positions,
            best=best,
            actual_cost=actual_cost,
            cost_clear=cost_clear,
            selected_global=np.array([], dtype=np.int64),
            tiers={},
            instrument=instrument,
            horizon_min=horizon_min,
            build_contract_id=build_contract_id,
        ), {"valid": len(entry_positions), "cost_clear": 0, "selected": 0}
    quantiles = sorted(float(value) for value in selection_quantiles)
    thresholds = {value: float(np.quantile(best[positive_indices], value)) for value in quantiles}
    floor_quantile = min(quantiles)
    selection_local = np.flatnonzero(cost_clear & (best >= thresholds[floor_quantile]))
    selected_within = _nonoverlap_best(
        epochs[entry_positions[selection_local]], best[selection_local], horizon_sec
    )
    selected_global = selection_local[selected_within]
    base, quote = instrument.split("_")
    tiers: dict[int, str] = {}
    rows: list[dict[str, Any]] = []
    for local_index in selected_global:
        entry_position = int(entry_positions[local_index])
        exit_position = int(exits[local_index])
        side = "long" if long_net[local_index] >= short_net[local_index] else "short"
        entry = frame.iloc[entry_position]
        exit_row = frame.iloc[exit_position]
        path = frame.iloc[entry_position + 1 : exit_position + 1]
        if side == "long":
            mfe = float((path["bid_high"].max() - float(entry["ask_close"])) / pip - modeled_slippage_pips)
            mae = float((path["bid_low"].min() - float(entry["ask_close"])) / pip - modeled_slippage_pips)
            factor = f"{base}+|{quote}-"
        else:
            mfe = float((float(entry["bid_close"]) - path["ask_low"].min()) / pip - modeled_slippage_pips)
            mae = float((float(entry["bid_close"]) - path["ask_high"].max()) / pip - modeled_slippage_pips)
            factor = f"{base}-|{quote}+"
        tier_quantile = max(value for value in quantiles if best[local_index] >= thresholds[value])
        tier = f"q{int(round(tier_quantile * 100))}"
        tiers[int(local_index)] = tier
        entry_epoch = int(entry["epoch"])
        exit_epoch = int(exit_row["epoch"])
        bucket = entry_epoch // max(300, horizon_sec)
        candidate_id = stable_id(
            "movement_candidate",
            build_contract_id,
            instrument,
            horizon_min,
            entry_epoch,
            exit_epoch,
            side,
            price["sha256"],
        )
        gross_value = float(gross[local_index])
        after_cost = float(best[local_index])
        rows.append(
            {
                "candidate_id": candidate_id,
                "build_contract_id": build_contract_id,
                "instrument": instrument,
                "base_currency": base,
                "quote_currency": quote,
                "entry_utc": pd.Timestamp(entry["timestamp"]).isoformat(),
                "exit_utc": pd.Timestamp(exit_row["timestamp"]).isoformat(),
                "entry_epoch": entry_epoch,
                "exit_epoch": exit_epoch,
                "horizon_min": int(horizon_min),
                "selected_side": side,
                "signed_currency_factor": factor,
                "market_episode_id": f"market_episode_{horizon_min}m_{bucket}",
                "selection_tier": tier,
                "selection_quantile": tier_quantile,
                "selection_threshold_pips": thresholds[tier_quantile],
                "entry_bid": float(entry["bid_close"]),
                "entry_ask": float(entry["ask_close"]),
                "exit_bid": float(exit_row["bid_close"]),
                "exit_ask": float(exit_row["ask_close"]),
                "entry_spread_pips": float(entry["spread_pips"]),
                "exit_spread_pips": float(exit_row["spread_pips"]),
                "modeled_slippage_pips": float(modeled_slippage_pips),
                "gross_magnitude_pips": gross_value,
                "after_cost_pips": after_cost,
                "actual_cost_pips": float(actual_cost[local_index]),
                "gross_cost_multiple": float(cost_multiple[local_index]),
                "mfe_pips": mfe,
                "mae_pips": mae,
                "path_efficiency": after_cost / gross_value if gross_value > 0 else 0.0,
                "first_cost_clear_sec": _first_cost_clear_sec(
                    frame=frame,
                    entry_position=entry_position,
                    exit_position=exit_position,
                    side=side,
                    pip=pip,
                    slippage=modeled_slippage_pips,
                ),
                "price_file_sha256": str(price["sha256"]),
                "outcome_selected": 1,
                "forecast_proof_eligible": 0,
                "research_only": 1,
                "created_utc": created_utc,
            }
        )
    census = _daily_census(
        frame=frame,
        entry_positions=entry_positions,
        best=best,
        actual_cost=actual_cost,
        cost_clear=cost_clear,
        selected_global=selected_global,
        tiers=tiers,
        instrument=instrument,
        horizon_min=horizon_min,
        build_contract_id=build_contract_id,
    )
    return rows, census, {
        "valid": len(entry_positions),
        "cost_clear": len(positive_indices),
        "selected": len(rows),
        "thresholds": {str(key): value for key, value in thresholds.items()},
    }


def _daily_census(
    *,
    frame: pd.DataFrame,
    entry_positions: np.ndarray,
    best: np.ndarray,
    actual_cost: np.ndarray,
    cost_clear: np.ndarray,
    selected_global: np.ndarray,
    tiers: Mapping[int, str],
    instrument: str,
    horizon_min: int,
    build_contract_id: str,
) -> list[dict[str, Any]]:
    dates = frame.iloc[entry_positions]["timestamp"].dt.strftime("%Y-%m-%d").to_numpy()
    selected_set = set(int(value) for value in selected_global)
    rows: list[dict[str, Any]] = []
    for date in sorted(set(dates)):
        indices = np.flatnonzero(dates == date)
        clear_indices = indices[cost_clear[indices]]
        selected_indices = [int(value) for value in indices if int(value) in selected_set]
        clear_epochs = frame.iloc[entry_positions[clear_indices]]["epoch"].to_numpy(dtype=np.int64)
        values = best[indices]
        costs = actual_cost[indices]
        rows.append(
            {
                "census_id": stable_id("opportunity_census", build_contract_id, instrument, horizon_min, date),
                "build_contract_id": build_contract_id,
                "instrument": instrument,
                "horizon_min": int(horizon_min),
                "start_date_utc": str(date),
                "valid_start_count": len(indices),
                "raw_cost_clear_count": len(clear_indices),
                "chronological_nonoverlap_cost_clear_count": _chronological_nonoverlap_count(
                    clear_epochs, horizon_min * 60
                ),
                "selected_q95_count": sum(tiers.get(value) == "q95" for value in selected_indices),
                "selected_q99_count": sum(tiers.get(value) == "q99" for value in selected_indices),
                "mean_best_after_cost_pips": float(np.mean(values)) if len(values) else None,
                "median_best_after_cost_pips": float(np.median(values)) if len(values) else None,
                "p95_best_after_cost_pips": float(np.quantile(values, 0.95)) if len(values) else None,
                "mean_actual_cost_pips": float(np.mean(costs)) if len(costs) else None,
                "research_only": 1,
            }
        )
    return rows


MOVEMENT_COLUMNS = [
    "candidate_id", "build_contract_id", "instrument", "base_currency", "quote_currency",
    "entry_utc", "exit_utc", "entry_epoch", "exit_epoch", "horizon_min", "selected_side",
    "signed_currency_factor", "market_episode_id", "selection_tier", "selection_quantile",
    "selection_threshold_pips", "entry_bid", "entry_ask", "exit_bid", "exit_ask",
    "entry_spread_pips", "exit_spread_pips", "modeled_slippage_pips", "gross_magnitude_pips",
    "after_cost_pips", "actual_cost_pips", "gross_cost_multiple", "mfe_pips", "mae_pips",
    "path_efficiency", "first_cost_clear_sec", "price_file_sha256", "outcome_selected",
    "forecast_proof_eligible", "research_only", "created_utc",
]

CENSUS_COLUMNS = [
    "census_id", "build_contract_id", "instrument", "horizon_min", "start_date_utc",
    "valid_start_count", "raw_cost_clear_count", "chronological_nonoverlap_cost_clear_count",
    "selected_q95_count", "selected_q99_count", "mean_best_after_cost_pips",
    "median_best_after_cost_pips", "p95_best_after_cost_pips", "mean_actual_cost_pips",
    "research_only",
]


def insert_rows(database: sqlite3.Connection, table: str, columns: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    placeholders = ",".join("?" for _ in columns)
    database.executemany(
        f"INSERT INTO {table}({','.join(columns)}) VALUES({placeholders})",
        [tuple(row.get(column) for column in columns) for row in rows],
    )


def build_legacy_reconciliation(
    *,
    path: Path,
    coverage: Mapping[str, tuple[int, int]],
    build_contract_id: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    states: Counter[str] = Counter()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for source in csv.DictReader(handle):
            instrument = str(source.get("instrument") or "")
            start = parse_epoch(source.get("start_utc"))
            end = parse_epoch(source.get("end_utc"))
            if start is None or end is None or end <= start or "_" not in instrument:
                continue
            move_id = str(source.get("move_id") or source.get("movement_key") or stable_id("legacy", instrument, start, end))
            first, last = coverage.get(instrument, (0, 0))
            if first <= start <= end <= last:
                archive_state = "within_current_executable_archive"
                action = "exact_timestamp_replay_and_factor_attribution"
            else:
                archive_state = "historical_executable_archive_unavailable"
                action = "reacquire_timestamp_safe_bid_ask_history_or_retain_unavailable"
            states[archive_state] += 1
            direction = str(source.get("primary_expected_pair_direction") or "")
            rows.append(
                {
                    "reconciliation_id": stable_id("legacy_reconciliation", build_contract_id, move_id),
                    "build_contract_id": build_contract_id,
                    "move_id": move_id,
                    "instrument": instrument,
                    "start_utc": iso_epoch(start),
                    "end_utc": iso_epoch(end),
                    "source_mapping_state": str(source.get("news_match_status") or "unknown"),
                    "retained_direction": direction if direction in {"LONG", "SHORT"} else None,
                    "current_price_archive_state": archive_state,
                    "required_action": action,
                    "causal_relation": str(source.get("primary_causal_relation") or "") or None,
                    "primary_event_id": str(source.get("primary_event_id") or "") or None,
                    "outcome_selected": 1,
                    "forecast_proof_eligible": 0,
                    "research_only": 1,
                }
            )
    return rows, dict(sorted(states.items()))


LEGACY_COLUMNS = [
    "reconciliation_id", "build_contract_id", "move_id", "instrument", "start_utc", "end_utc",
    "source_mapping_state", "retained_direction", "current_price_archive_state", "required_action",
    "causal_relation", "primary_event_id", "outcome_selected", "forecast_proof_eligible",
    "research_only",
]


def build_inventory(
    *,
    config_path: Path = CONFIG,
    candle_root: Path = CANDLES,
    legacy_tags: Path = LEGACY_TAGS,
    database_path: Path = DATABASE,
) -> dict[str, Any]:
    contract = load_contract(config_path)
    created = utc_now()
    price_files = [candle_root / f"{instrument}_M1.csv" for instrument in contract["expected_instruments"]]
    missing = [str(path) for path in price_files if not path.exists()]
    if missing:
        raise FileNotFoundError(f"missing_price_files:{missing}")
    manifest = [
        {"instrument": instrument, "path": str(path.resolve()), "bytes": path.stat().st_size, "sha256": file_sha256(path)}
        for instrument, path in zip(contract["expected_instruments"], price_files)
    ]
    price_manifest_sha256 = sha256_bytes(canonical_json(manifest).encode("utf-8"))
    builder_sha256 = file_sha256(Path(__file__).resolve())
    database = open_database(database_path)
    existing = database.execute(
        "SELECT price_manifest_sha256,builder_sha256 FROM reconstruction_contracts WHERE build_contract_id=?",
        (BUILD_CONTRACT_ID,),
    ).fetchone()
    if existing:
        database.close()
        if tuple(existing) != (price_manifest_sha256, builder_sha256):
            raise RuntimeError("immutable_build_contract_collision")
        return inventory_snapshot(database_path, BUILD_CONTRACT_ID, reused=True)
    contract_record = {
        "build_contract_id": BUILD_CONTRACT_ID,
        "parent_contract_id": contract["contract_id"],
        "cohort_id": contract["cohort_id"],
        "created_utc": created,
        "config_sha256": file_sha256(config_path),
        "builder_sha256": builder_sha256,
        "price_manifest_sha256": price_manifest_sha256,
        "contract_json": canonical_json(
            {
                "parent_contract": contract,
                "movement_builder": {
                    "modeled_slippage_pips": MODELED_SLIPPAGE_PIPS,
                    "selection_quantiles": contract["movement_discovery"]["selection_quantiles"],
                    "minimum_net_cost_multiple": contract["movement_discovery"]["minimum_net_cost_multiple"],
                    "price_manifest": manifest,
                },
            }
        ),
    }
    with database:
        insert_rows(
            database,
            "reconstruction_contracts",
            list(contract_record),
            [contract_record],
        )
    totals = Counter()
    pair_horizon_stats: list[dict[str, Any]] = []
    coverage: dict[str, tuple[int, int]] = {}
    for instrument, path in zip(contract["expected_instruments"], price_files):
        price = load_price(path, instrument)
        coverage[instrument] = (int(price["first_epoch"]), int(price["last_epoch"]))
        for horizon in contract["horizons_minutes"]:
            movement_rows, census_rows, stats = build_pair_horizon(
                price=price,
                instrument=instrument,
                horizon_min=int(horizon),
                selection_quantiles=contract["movement_discovery"]["selection_quantiles"],
                minimum_net_cost_multiple=float(contract["movement_discovery"]["minimum_net_cost_multiple"]),
                build_contract_id=BUILD_CONTRACT_ID,
                created_utc=created,
            )
            with database:
                insert_rows(database, "movement_candidates", MOVEMENT_COLUMNS, movement_rows)
                insert_rows(database, "opportunity_census", CENSUS_COLUMNS, census_rows)
            totals.update({"valid": stats["valid"], "cost_clear": stats["cost_clear"], "selected": stats["selected"]})
            pair_horizon_stats.append(
                {"instrument": instrument, "horizon_min": int(horizon), **stats}
            )
    legacy_rows, legacy_states = build_legacy_reconciliation(
        path=legacy_tags,
        coverage=coverage,
        build_contract_id=BUILD_CONTRACT_ID,
    )
    with database:
        insert_rows(database, "legacy_gap_reconciliation", LEGACY_COLUMNS, legacy_rows)
    integrity = str(database.execute("PRAGMA integrity_check").fetchone()[0])
    database.close()
    return inventory_snapshot(
        database_path,
        BUILD_CONTRACT_ID,
        reused=False,
        runtime_stats={
            "valid_start_count": totals["valid"],
            "raw_cost_clear_count": totals["cost_clear"],
            "selected_movement_count": totals["selected"],
            "legacy_states": legacy_states,
            "pair_horizon_stats": pair_horizon_stats,
            "sqlite_integrity": integrity,
        },
    )


def inventory_snapshot(
    database_path: Path,
    build_contract_id: str,
    *,
    reused: bool,
    runtime_stats: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    database = sqlite3.connect(f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True)
    database.execute("PRAGMA query_only=ON")
    movement_count = int(database.execute(
        "SELECT COUNT(*) FROM movement_candidates WHERE build_contract_id=?", (build_contract_id,)
    ).fetchone()[0])
    qrows = database.execute(
        "SELECT selection_tier,COUNT(*) FROM movement_candidates WHERE build_contract_id=? GROUP BY selection_tier",
        (build_contract_id,),
    ).fetchall()
    episode_count = int(database.execute(
        "SELECT COUNT(DISTINCT market_episode_id) FROM movement_candidates WHERE build_contract_id=?",
        (build_contract_id,),
    ).fetchone()[0])
    instrument_count, horizon_count, first_entry, last_entry = database.execute(
        "SELECT COUNT(DISTINCT instrument),COUNT(DISTINCT horizon_min),MIN(entry_utc),MAX(entry_utc) "
        "FROM movement_candidates WHERE build_contract_id=?",
        (build_contract_id,),
    ).fetchone()
    census = database.execute(
        "SELECT SUM(valid_start_count),SUM(raw_cost_clear_count),"
        "SUM(chronological_nonoverlap_cost_clear_count),COUNT(DISTINCT start_date_utc) "
        "FROM opportunity_census WHERE build_contract_id=?",
        (build_contract_id,),
    ).fetchone()
    legacy = database.execute(
        "SELECT current_price_archive_state,COUNT(*) FROM legacy_gap_reconciliation "
        "WHERE build_contract_id=? GROUP BY current_price_archive_state",
        (build_contract_id,),
    ).fetchall()
    integrity = str(database.execute("PRAGMA integrity_check").fetchone()[0])
    contract_row = database.execute(
        "SELECT parent_contract_id,cohort_id,created_utc,config_sha256,builder_sha256,price_manifest_sha256 "
        "FROM reconstruction_contracts WHERE build_contract_id=?",
        (build_contract_id,),
    ).fetchone()
    database.close()
    snapshot: dict[str, Any] = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "build_contract_id": build_contract_id,
        "parent_contract_id": contract_row[0],
        "cohort_id": contract_row[1],
        "contract_created_utc": contract_row[2],
        "config_sha256": contract_row[3],
        "builder_sha256": contract_row[4],
        "price_manifest_sha256": contract_row[5],
        "database_path": str(database_path.resolve()),
        "database_bytes": database_path.stat().st_size,
        "sqlite_integrity": integrity,
        "reused_existing_immutable_cohort": bool(reused),
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
        "movement_candidates": movement_count,
        "selection_tier_counts": dict(qrows),
        "market_episode_count": episode_count,
        "instrument_count": int(instrument_count or 0),
        "horizon_count": int(horizon_count or 0),
        "first_entry_utc": first_entry,
        "last_entry_utc": last_entry,
        "opportunity_census": {
            "valid_start_count": int(census[0] or 0),
            "raw_cost_clear_count": int(census[1] or 0),
            "chronological_nonoverlap_cost_clear_count": int(census[2] or 0),
            "distinct_start_dates": int(census[3] or 0),
        },
        "legacy_reconciliation": dict(legacy),
    }
    if runtime_stats:
        snapshot["runtime_stats"] = dict(runtime_stats)
    snapshot["snapshot_sha256"] = sha256_bytes(canonical_json(snapshot).encode("utf-8"))
    return snapshot


def render_report(snapshot: Mapping[str, Any]) -> str:
    census = snapshot["opportunity_census"]
    legacy = snapshot["legacy_reconciliation"]
    valid = int(census["valid_start_count"])
    raw = int(census["raw_cost_clear_count"])
    rate = 100.0 * raw / valid if valid else 0.0
    lines = [
        "# Spike/Blurb All-68 Movement Inventory V1",
        "",
        f"- Generated: `{snapshot['generated_utc']}`",
        f"- Build contract: `{snapshot['build_contract_id']}`",
        f"- Snapshot SHA-256: `{snapshot['snapshot_sha256']}`",
        "- Safety: **outcome-selected research / execution-ineligible / no-trade**",
        "",
        "## Retained movement surface",
        "",
        f"- Instruments: **{snapshot['instrument_count']}/68**",
        f"- Horizons: **{snapshot['horizon_count']}/20** (1 minute through 30 days)",
        f"- Selected q95/q99 movement candidates: **{snapshot['movement_candidates']:,}**",
        f"- Factor-time market episodes: **{snapshot['market_episode_count']:,}**",
        f"- Entry span: `{snapshot['first_entry_utc']}` through `{snapshot['last_entry_utc']}`",
        f"- SQLite integrity: `{snapshot['sqlite_integrity']}`",
        "",
        "## Executable opportunity census",
        "",
        f"- Valid pair/horizon starts: **{valid:,}**",
        f"- Raw starts whose hindsight-best side beat executable cost: **{raw:,} ({rate:.2f}%)**",
        f"- Chronologically non-overlapping cost-clearing paths: **{int(census['chronological_nonoverlap_cost_clear_count']):,}**",
        "",
        "These are opportunity counts, not forecast wins: the profitable side is selected after the outcome. Their purpose is to quantify the surface that a causal model would have to identify.",
        "",
        "## Legacy gap ledger",
        "",
    ]
    for state, count in sorted(legacy.items()):
        lines.append(f"- `{state}`: **{int(count):,}**")
    lines.extend(
        [
            "",
            "Every legacy label now has a permanent recovery state. Unavailable bid/ask history remains unavailable until reacquired from a timestamp-safe source.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--candles", type=Path, default=CANDLES)
    parser.add_argument("--legacy-tags", type=Path, default=LEGACY_TAGS)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--output-json", type=Path, default=OUTPUT_JSON)
    parser.add_argument("--output-md", type=Path, default=OUTPUT_MD)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    snapshot = build_inventory(
        config_path=args.config,
        candle_root=args.candles,
        legacy_tags=args.legacy_tags,
        database_path=args.database,
    )
    atomic_text(args.output_json, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_text(args.output_md, render_report(snapshot))
    print(
        json.dumps(
            {
                "snapshot_sha256": snapshot["snapshot_sha256"],
                "movement_candidates": snapshot["movement_candidates"],
                "market_episode_count": snapshot["market_episode_count"],
                "opportunity_census": snapshot["opportunity_census"],
                "legacy_reconciliation": snapshot["legacy_reconciliation"],
                "output_json": str(args.output_json),
                "output_md": str(args.output_md),
                "supported_execution_decision": "no_trade",
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
