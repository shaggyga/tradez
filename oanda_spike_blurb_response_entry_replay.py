#!/usr/bin/env python3
"""Replay event-first, response-detected FX entries across all 68 pairs.

An official source observation opens a watch but does not donate a direction.
Direction comes from synchronized executable pair-leg responses observed after
the source was known.  Pair selection is cost-aware; technical state can time or
veto entry.  Every result is discovery-only and cannot route to an account.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from oanda_instrument_pips import fallback_pip_size
from oanda_spike_blurb_factor_ledger import FACTOR_CONTRACT_ID
from oanda_spike_blurb_factor_reconstruction import (
    CANDLES,
    CONFIG,
    REPORT_ROOT,
    atomic_text,
    canonical_json,
    file_sha256,
    load_contract,
    parse_epoch,
    sha256_bytes,
    utc_now,
)
from oanda_spike_blurb_movement_inventory import DATABASE, stable_id


REPLAY_CONTRACT_ID = "spike_blurb_response_entry_replay_v1_20260819"
OUTPUT_JSON = REPORT_ROOT / "SPIKE_BLURB_RESPONSE_ENTRY_REPLAY_V1.json"
OUTPUT_MD = REPORT_ROOT / "SPIKE_BLURB_RESPONSE_ENTRY_REPLAY_V1.md"
OUTCOME_HORIZONS_MIN = (5, 15, 30, 60, 120)
MODELED_SLIPPAGE_PIPS = 0.25
MINIMUM_LEGS = 3
MINIMUM_BREADTH = 0.60

ARMS = {
    "response_1m_breadth": {
        "start_min": 1,
        "end_min": 15,
        "minimum_strength_bps": 1.0,
        "persistence_min": 1,
        "technical_confirmation": False,
    },
    "response_3m_persistent": {
        "start_min": 3,
        "end_min": 30,
        "minimum_strength_bps": 2.0,
        "persistence_min": 3,
        "technical_confirmation": False,
    },
    "source_plus_technical_confirmation": {
        "start_min": 3,
        "end_min": 30,
        "minimum_strength_bps": 2.0,
        "persistence_min": 3,
        "technical_confirmation": True,
    },
    "delayed_reconfirmation": {
        "start_min": 5,
        "end_min": 45,
        "minimum_strength_bps": 2.5,
        "persistence_min": 5,
        "technical_confirmation": False,
    },
}


@dataclass(frozen=True)
class PairSeries:
    instrument: str
    base_currency: str
    quote_currency: str
    pip: float
    epochs: np.ndarray
    bid_high: np.ndarray
    bid_low: np.ndarray
    bid_close: np.ndarray
    ask_high: np.ndarray
    ask_low: np.ndarray
    ask_close: np.ndarray
    mid: np.ndarray
    price_file_sha256: str

    def exact_index(self, epoch: int) -> int | None:
        location = int(np.searchsorted(self.epochs, int(epoch)))
        if location < len(self.epochs) and int(self.epochs[location]) == int(epoch):
            return location
        return None


def load_market(candle_root: Path, instruments: Iterable[str]) -> dict[str, PairSeries]:
    output: dict[str, PairSeries] = {}
    columns = [
        "time", "datetime", "instrument", "bid_high", "bid_low", "bid_close",
        "ask_high", "ask_low", "ask_close",
    ]
    for instrument in instruments:
        path = candle_root / f"{instrument}_M1.csv"
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
        if observed != {instrument}:
            raise ValueError(f"instrument_identity_mismatch:{instrument}:{sorted(observed)}")
        epochs = (frame["timestamp"].astype("int64") // 1_000_000_000).to_numpy(dtype=np.int64)
        bid = frame["bid_close"].to_numpy(dtype=float)
        ask = frame["ask_close"].to_numpy(dtype=float)
        if len(epochs) == 0 or np.any(ask <= bid):
            raise ValueError(f"invalid_market_series:{instrument}")
        base, quote = instrument.split("_")
        output[instrument] = PairSeries(
            instrument=instrument,
            base_currency=base,
            quote_currency=quote,
            pip=fallback_pip_size(instrument),
            epochs=epochs,
            bid_high=frame["bid_high"].to_numpy(dtype=float),
            bid_low=frame["bid_low"].to_numpy(dtype=float),
            bid_close=bid,
            ask_high=frame["ask_high"].to_numpy(dtype=float),
            ask_low=frame["ask_low"].to_numpy(dtype=float),
            ask_close=ask,
            mid=(bid + ask) / 2.0,
            price_file_sha256=file_sha256(path),
        )
    return output


def currency_snapshot(
    market: Mapping[str, PairSeries], currency: str, baseline_epoch: int, target_epoch: int
) -> dict[str, Any] | None:
    legs: list[dict[str, Any]] = []
    for pair in market.values():
        if currency not in {pair.base_currency, pair.quote_currency}:
            continue
        baseline = pair.exact_index(baseline_epoch)
        target = pair.exact_index(target_epoch)
        if baseline is None or target is None or target <= baseline:
            continue
        raw_return_bps = (float(pair.mid[target]) / float(pair.mid[baseline]) - 1.0) * 10_000.0
        oriented_return_bps = raw_return_bps if pair.base_currency == currency else -raw_return_bps
        spread_pips = (float(pair.ask_close[target]) - float(pair.bid_close[target])) / pair.pip
        spread_bps = (float(pair.ask_close[target]) - float(pair.bid_close[target])) / float(pair.mid[target]) * 10_000.0
        legs.append(
            {
                "instrument": pair.instrument,
                "target_index": target,
                "oriented_return_bps": oriented_return_bps,
                "spread_pips": spread_pips,
                "spread_bps": spread_bps,
                "is_base": pair.base_currency == currency,
            }
        )
    if len(legs) < MINIMUM_LEGS:
        return None
    returns = np.array([leg["oriented_return_bps"] for leg in legs], dtype=float)
    median = float(np.median(returns))
    if median == 0.0:
        return None
    direction = 1 if median > 0 else -1
    agreeing = [leg for leg in legs if int(math.copysign(1, leg["oriented_return_bps"] or direction)) == direction]
    breadth = len(agreeing) / len(legs)
    candidates = [leg for leg in agreeing if abs(float(leg["oriented_return_bps"])) >= abs(median)]
    if not candidates:
        candidates = agreeing
    selected = min(
        candidates,
        key=lambda leg: (
            float(leg["spread_bps"]),
            -abs(float(leg["oriented_return_bps"])),
            str(leg["instrument"]),
        ),
    )
    pair = market[str(selected["instrument"])]
    if direction > 0:
        pair_direction = "long" if pair.base_currency == currency else "short"
    else:
        pair_direction = "short" if pair.base_currency == currency else "long"
    return {
        "currency_strength_bps": median,
        "currency_direction": "stronger" if direction > 0 else "weaker",
        "direction_sign": direction,
        "breadth": breadth,
        "leg_count": len(legs),
        "agreeing_leg_count": len(agreeing),
        "selected_instrument": pair.instrument,
        "selected_pair_direction": pair_direction,
        "selected_pair_return_bps": float(selected["oriented_return_bps"]),
        "selected_spread_pips": float(selected["spread_pips"]),
        "selected_spread_bps": float(selected["spread_bps"]),
        "selected_index": int(selected["target_index"]),
    }


def technical_alignment(pair: PairSeries, index: int, direction: str) -> tuple[bool, dict[str, float | None]]:
    if index < 14:
        return False, {"sma_5": None, "sma_15": None}
    sma_5 = float(np.mean(pair.mid[index - 4 : index + 1]))
    sma_15 = float(np.mean(pair.mid[index - 14 : index + 1]))
    aligned = sma_5 > sma_15 if direction == "long" else sma_5 < sma_15
    return aligned, {"sma_5": sma_5, "sma_15": sma_15}


def detect_arm(
    *,
    market: Mapping[str, PairSeries],
    currency: str,
    known_epoch: int,
    arm_id: str,
    arm: Mapping[str, Any],
) -> dict[str, Any] | None:
    baseline_epoch = ((int(known_epoch) + 59) // 60) * 60
    for elapsed_min in range(int(arm["start_min"]), int(arm["end_min"]) + 1):
        target_epoch = baseline_epoch + elapsed_min * 60
        snapshot = currency_snapshot(market, currency, baseline_epoch, target_epoch)
        if snapshot is None:
            continue
        if snapshot["leg_count"] < MINIMUM_LEGS or snapshot["breadth"] < MINIMUM_BREADTH:
            continue
        if abs(float(snapshot["currency_strength_bps"])) < float(arm["minimum_strength_bps"]):
            continue
        persistence = int(arm["persistence_min"])
        if persistence > 1:
            prior_epoch = target_epoch - persistence * 60
            prior = currency_snapshot(market, currency, baseline_epoch, prior_epoch)
            recent = currency_snapshot(market, currency, prior_epoch, target_epoch)
            if prior is None or recent is None:
                continue
            if prior["direction_sign"] != snapshot["direction_sign"] or recent["direction_sign"] != snapshot["direction_sign"]:
                continue
            if recent["breadth"] < MINIMUM_BREADTH:
                continue
        pair = market[str(snapshot["selected_instrument"])]
        aligned, technical = technical_alignment(
            pair, int(snapshot["selected_index"]), str(snapshot["selected_pair_direction"])
        )
        if bool(arm["technical_confirmation"]) and not aligned:
            continue
        index = int(snapshot["selected_index"])
        return {
            "arm_id": arm_id,
            "baseline_epoch": baseline_epoch,
            "detection_epoch": target_epoch,
            "elapsed_min": elapsed_min,
            **snapshot,
            "technical_aligned": aligned,
            **technical,
            "entry_bid": float(pair.bid_close[index]),
            "entry_ask": float(pair.ask_close[index]),
            "entry_mid": float(pair.mid[index]),
            "price_file_sha256": pair.price_file_sha256,
        }
    return None


def detect_pullback_resumption(
    *, market: Mapping[str, PairSeries], currency: str, known_epoch: int
) -> dict[str, Any] | None:
    base = detect_arm(
        market=market,
        currency=currency,
        known_epoch=known_epoch,
        arm_id="pullback_resumption_base",
        arm=ARMS["response_3m_persistent"],
    )
    if base is None:
        return None
    pair = market[str(base["selected_instrument"])]
    baseline = pair.exact_index(int(base["baseline_epoch"]))
    detection = pair.exact_index(int(base["detection_epoch"]))
    if baseline is None or detection is None:
        return None
    direction = 1 if base["selected_pair_direction"] == "long" else -1
    detected_move = direction * (float(pair.mid[detection]) - float(pair.mid[baseline]))
    if detected_move <= 0:
        return None
    pullback_threshold = 0.25 * detected_move
    pulled_back = False
    for offset in range(1, 11):
        index = pair.exact_index(int(base["detection_epoch"]) + offset * 60)
        prior = pair.exact_index(int(base["detection_epoch"]) + (offset - 1) * 60)
        if index is None or prior is None:
            continue
        from_detection = direction * (float(pair.mid[index]) - float(pair.mid[detection]))
        if from_detection <= -pullback_threshold:
            pulled_back = True
            continue
        one_minute = direction * (float(pair.mid[index]) - float(pair.mid[prior]))
        if pulled_back and one_minute > 0:
            response = currency_snapshot(
                market, currency, int(base["detection_epoch"]), int(pair.epochs[index])
            )
            if response is None or response["direction_sign"] != base["direction_sign"]:
                continue
            result = dict(base)
            result.update(
                {
                    "arm_id": "pullback_resumption",
                    "detection_epoch": int(pair.epochs[index]),
                    "elapsed_min": int(base["elapsed_min"]) + offset,
                    "selected_index": index,
                    "entry_bid": float(pair.bid_close[index]),
                    "entry_ask": float(pair.ask_close[index]),
                    "entry_mid": float(pair.mid[index]),
                    "pullback_fraction": abs(from_detection) / detected_move,
                }
            )
            return result
    return None


def outcome_rows(
    *, detection_id: str, detection: Mapping[str, Any], market: Mapping[str, PairSeries]
) -> list[dict[str, Any]]:
    pair = market[str(detection["selected_instrument"])]
    entry_index = pair.exact_index(int(detection["detection_epoch"]))
    if entry_index is None:
        return []
    side = str(detection["selected_pair_direction"])
    rows: list[dict[str, Any]] = []
    for horizon in OUTCOME_HORIZONS_MIN:
        exit_epoch = int(detection["detection_epoch"]) + horizon * 60
        exit_index = pair.exact_index(exit_epoch)
        if exit_index is None or exit_index <= entry_index:
            continue
        path_slice = slice(entry_index + 1, exit_index + 1)
        if side == "long":
            net = (float(pair.bid_close[exit_index]) - float(detection["entry_ask"])) / pair.pip - MODELED_SLIPPAGE_PIPS
            mfe = (float(np.max(pair.bid_high[path_slice])) - float(detection["entry_ask"])) / pair.pip - MODELED_SLIPPAGE_PIPS
            mae = (float(np.min(pair.bid_low[path_slice])) - float(detection["entry_ask"])) / pair.pip - MODELED_SLIPPAGE_PIPS
        else:
            net = (float(detection["entry_bid"]) - float(pair.ask_close[exit_index])) / pair.pip - MODELED_SLIPPAGE_PIPS
            mfe = (float(detection["entry_bid"]) - float(np.min(pair.ask_low[path_slice]))) / pair.pip - MODELED_SLIPPAGE_PIPS
            mae = (float(detection["entry_bid"]) - float(np.max(pair.ask_high[path_slice]))) / pair.pip - MODELED_SLIPPAGE_PIPS
        rows.append(
            {
                "outcome_id": stable_id("response_outcome", REPLAY_CONTRACT_ID, detection_id, horizon),
                "replay_contract_id": REPLAY_CONTRACT_ID,
                "detection_id": detection_id,
                "horizon_min": horizon,
                "exit_epoch": exit_epoch,
                "exit_utc": dt.datetime.fromtimestamp(exit_epoch, dt.timezone.utc).isoformat(),
                "exit_bid": float(pair.bid_close[exit_index]),
                "exit_ask": float(pair.ask_close[exit_index]),
                "after_cost_pips": float(net),
                "mfe_pips": float(mfe),
                "mae_pips": float(mae),
                "win_after_cost": int(net > 0.0),
                "outcome_selected": 0,
                "forecast_proof_eligible": 0,
                "research_only": 1,
            }
        )
    return rows


def load_watch_events(database: sqlite3.Connection) -> list[dict[str, Any]]:
    query = """
      SELECT f.factor_id,f.currency,f.known_utc,f.factor_type,f.relevance_state,
             f.signed_factor_score,s.source_evidence_id,s.source_id,s.underlying_event_id,
             s.headline,s.event_type,s.source_priority_class,s.causal_state
      FROM factor_observations f
      JOIN source_attributions s ON s.source_evidence_id=f.source_evidence_id
      WHERE f.factor_contract_id=? AND f.causal_state='point_in_time_observed'
      ORDER BY f.currency,f.known_utc,f.factor_id
    """
    dedup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in database.execute(query, (FACTOR_CONTRACT_ID,)):
        (
            factor_id, currency, known_utc, factor_type, relevance, score,
            source_evidence_id, source_id, underlying_event_id, headline, event_type,
            priority, causal_state,
        ) = row
        key = (str(currency), str(underlying_event_id), str(known_utc))
        dedup.setdefault(
            key,
            {
                "factor_id": str(factor_id),
                "currency": str(currency),
                "known_utc": str(known_utc),
                "factor_type": str(factor_type),
                "relevance_state": str(relevance),
                "signed_factor_score": float(score),
                "source_evidence_id": str(source_evidence_id),
                "source_id": str(source_id),
                "underlying_event_id": str(underlying_event_id),
                "headline": str(headline),
                "event_type": str(event_type),
                "source_priority_class": str(priority),
                "causal_state": str(causal_state),
            },
        )
    return list(dedup.values())


def ensure_schema(database: sqlite3.Connection) -> None:
    database.executescript(
        """
        CREATE TABLE IF NOT EXISTS response_replay_contracts (
          replay_contract_id TEXT PRIMARY KEY,
          factor_contract_id TEXT NOT NULL,
          created_utc TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL,
          price_manifest_sha256 TEXT NOT NULL,
          contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS response_detections (
          detection_id TEXT PRIMARY KEY,
          replay_contract_id TEXT NOT NULL,
          factor_id TEXT NOT NULL,
          source_evidence_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          underlying_event_id TEXT NOT NULL,
          factor_type TEXT NOT NULL,
          relevance_state TEXT NOT NULL,
          currency TEXT NOT NULL,
          arm_id TEXT NOT NULL,
          known_utc TEXT NOT NULL,
          baseline_epoch INTEGER NOT NULL,
          detection_epoch INTEGER NOT NULL,
          detection_utc TEXT NOT NULL,
          elapsed_min INTEGER NOT NULL,
          currency_strength_bps REAL NOT NULL,
          currency_direction TEXT NOT NULL,
          breadth REAL NOT NULL,
          leg_count INTEGER NOT NULL,
          agreeing_leg_count INTEGER NOT NULL,
          instrument TEXT NOT NULL,
          pair_direction TEXT NOT NULL,
          selected_pair_return_bps REAL NOT NULL,
          entry_bid REAL NOT NULL,
          entry_ask REAL NOT NULL,
          entry_spread_pips REAL NOT NULL,
          entry_spread_bps REAL NOT NULL,
          technical_aligned INTEGER NOT NULL,
          sma_5 REAL,
          sma_15 REAL,
          pullback_fraction REAL,
          price_file_sha256 TEXT NOT NULL,
          causal_entry INTEGER NOT NULL CHECK(causal_entry=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          research_only INTEGER NOT NULL CHECK(research_only=1),
          UNIQUE(replay_contract_id,factor_id,arm_id)
        );
        CREATE INDEX IF NOT EXISTS response_detections_arm_time
          ON response_detections(arm_id,detection_epoch);
        CREATE TABLE IF NOT EXISTS response_entry_outcomes (
          outcome_id TEXT PRIMARY KEY,
          replay_contract_id TEXT NOT NULL,
          detection_id TEXT NOT NULL,
          horizon_min INTEGER NOT NULL,
          exit_epoch INTEGER NOT NULL,
          exit_utc TEXT NOT NULL,
          exit_bid REAL NOT NULL,
          exit_ask REAL NOT NULL,
          after_cost_pips REAL NOT NULL,
          mfe_pips REAL NOT NULL,
          mae_pips REAL NOT NULL,
          win_after_cost INTEGER NOT NULL,
          outcome_selected INTEGER NOT NULL CHECK(outcome_selected=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          research_only INTEGER NOT NULL CHECK(research_only=1),
          UNIQUE(replay_contract_id,detection_id,horizon_min),
          FOREIGN KEY(detection_id) REFERENCES response_detections(detection_id)
        );
        CREATE TRIGGER IF NOT EXISTS response_replay_contracts_no_update
          BEFORE UPDATE ON response_replay_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS response_replay_contracts_no_delete
          BEFORE DELETE ON response_replay_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS response_detections_no_update
          BEFORE UPDATE ON response_detections BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS response_detections_no_delete
          BEFORE DELETE ON response_detections BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS response_entry_outcomes_no_update
          BEFORE UPDATE ON response_entry_outcomes BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS response_entry_outcomes_no_delete
          BEFORE DELETE ON response_entry_outcomes BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


DETECTION_COLUMNS = [
    "detection_id", "replay_contract_id", "factor_id", "source_evidence_id", "source_id",
    "underlying_event_id", "factor_type", "relevance_state", "currency", "arm_id",
    "known_utc", "baseline_epoch", "detection_epoch", "detection_utc", "elapsed_min",
    "currency_strength_bps", "currency_direction", "breadth", "leg_count",
    "agreeing_leg_count", "instrument", "pair_direction", "selected_pair_return_bps",
    "entry_bid", "entry_ask", "entry_spread_pips", "entry_spread_bps", "technical_aligned",
    "sma_5", "sma_15", "pullback_fraction", "price_file_sha256", "causal_entry",
    "execution_eligible", "research_only",
]
OUTCOME_COLUMNS = [
    "outcome_id", "replay_contract_id", "detection_id", "horizon_min", "exit_epoch",
    "exit_utc", "exit_bid", "exit_ask", "after_cost_pips", "mfe_pips", "mae_pips",
    "win_after_cost", "outcome_selected", "forecast_proof_eligible", "research_only",
]


def insert_rows(database: sqlite3.Connection, table: str, columns: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    database.executemany(
        f"INSERT INTO {table}({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
        [tuple(row.get(column) for column in columns) for row in rows],
    )


def build_replay(
    *, config_path: Path = CONFIG, candle_root: Path = CANDLES, database_path: Path = DATABASE
) -> dict[str, Any]:
    contract = load_contract(config_path)
    market = load_market(candle_root, contract["expected_instruments"])
    manifest = [
        {"instrument": pair.instrument, "sha256": pair.price_file_sha256, "rows": len(pair.epochs)}
        for pair in market.values()
    ]
    price_manifest_sha256 = sha256_bytes(canonical_json(manifest).encode("utf-8"))
    builder_sha = file_sha256(Path(__file__).resolve())
    database = sqlite3.connect(database_path, timeout=120)
    database.execute("PRAGMA journal_mode=WAL")
    database.execute("PRAGMA synchronous=FULL")
    database.execute("PRAGMA foreign_keys=ON")
    ensure_schema(database)
    existing = database.execute(
        "SELECT builder_sha256,price_manifest_sha256 FROM response_replay_contracts WHERE replay_contract_id=?",
        (REPLAY_CONTRACT_ID,),
    ).fetchone()
    if existing:
        database.close()
        if tuple(existing) != (builder_sha, price_manifest_sha256):
            raise RuntimeError("immutable_replay_contract_collision")
        return replay_snapshot(database_path, reused=True)
    watches = load_watch_events(database)
    detections: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    exclusion_counts: Counter[str] = Counter()
    for watch in watches:
        known_epoch = parse_epoch(watch["known_utc"])
        if known_epoch is None:
            exclusion_counts["invalid_known_clock"] += 1
            continue
        watch_detected = False
        for arm_id, arm in ARMS.items():
            detection = detect_arm(
                market=market,
                currency=watch["currency"],
                known_epoch=known_epoch,
                arm_id=arm_id,
                arm=arm,
            )
            if detection is None:
                continue
            watch_detected = True
            row = detection_row(watch, detection)
            detections.append(row)
            outcomes.extend(outcome_rows(detection_id=row["detection_id"], detection=detection, market=market))
        pullback = detect_pullback_resumption(
            market=market, currency=watch["currency"], known_epoch=known_epoch
        )
        if pullback is not None:
            watch_detected = True
            row = detection_row(watch, pullback)
            detections.append(row)
            outcomes.extend(outcome_rows(detection_id=row["detection_id"], detection=pullback, market=market))
        if not watch_detected:
            exclusion_counts["no_cost_clear_synchronized_response"] += 1
    replay_contract = {
        "replay_contract_id": REPLAY_CONTRACT_ID,
        "factor_contract_id": FACTOR_CONTRACT_ID,
        "created_utc": utc_now(),
        "builder_sha256": builder_sha,
        "price_manifest_sha256": price_manifest_sha256,
        "contract_json": canonical_json(
            {
                "arms": ARMS,
                "pullback": {"base_arm": "response_3m_persistent", "minimum_retrace_fraction": 0.25, "maximum_wait_min": 10},
                "outcome_horizons_min": list(OUTCOME_HORIZONS_MIN),
                "minimum_legs": MINIMUM_LEGS,
                "minimum_breadth": MINIMUM_BREADTH,
                "modeled_slippage_pips": MODELED_SLIPPAGE_PIPS,
                "direction_source": "observed_synchronized_currency_response_only",
                "pair_selection": "lowest_spread_among_above_median_agreeing_currency_legs",
                "technical_role": "entry_confirmation_only",
                "source_factor_direction_not_used": True,
                "research_only": True,
                "execution_eligible": False,
            }
        ),
    }
    with database:
        insert_rows(database, "response_replay_contracts", list(replay_contract), [replay_contract])
        insert_rows(database, "response_detections", DETECTION_COLUMNS, detections)
        insert_rows(database, "response_entry_outcomes", OUTCOME_COLUMNS, outcomes)
    integrity = str(database.execute("PRAGMA integrity_check").fetchone()[0])
    database.close()
    return replay_snapshot(
        database_path,
        reused=False,
        integrity=integrity,
        watch_count=len(watches),
        exclusion_counts=dict(exclusion_counts),
    )


def detection_row(watch: Mapping[str, Any], detection: Mapping[str, Any]) -> dict[str, Any]:
    detection_id = stable_id(
        "response_detection",
        REPLAY_CONTRACT_ID,
        watch["factor_id"],
        detection["arm_id"],
        detection["detection_epoch"],
        detection["selected_instrument"],
    )
    return {
        "detection_id": detection_id,
        "replay_contract_id": REPLAY_CONTRACT_ID,
        "factor_id": watch["factor_id"],
        "source_evidence_id": watch["source_evidence_id"],
        "source_id": watch["source_id"],
        "underlying_event_id": watch["underlying_event_id"],
        "factor_type": watch["factor_type"],
        "relevance_state": watch["relevance_state"],
        "currency": watch["currency"],
        "arm_id": detection["arm_id"],
        "known_utc": watch["known_utc"],
        "baseline_epoch": detection["baseline_epoch"],
        "detection_epoch": detection["detection_epoch"],
        "detection_utc": dt.datetime.fromtimestamp(detection["detection_epoch"], dt.timezone.utc).isoformat(),
        "elapsed_min": detection["elapsed_min"],
        "currency_strength_bps": detection["currency_strength_bps"],
        "currency_direction": detection["currency_direction"],
        "breadth": detection["breadth"],
        "leg_count": detection["leg_count"],
        "agreeing_leg_count": detection["agreeing_leg_count"],
        "instrument": detection["selected_instrument"],
        "pair_direction": detection["selected_pair_direction"],
        "selected_pair_return_bps": detection["selected_pair_return_bps"],
        "entry_bid": detection["entry_bid"],
        "entry_ask": detection["entry_ask"],
        "entry_spread_pips": detection["selected_spread_pips"],
        "entry_spread_bps": detection["selected_spread_bps"],
        "technical_aligned": int(bool(detection["technical_aligned"])),
        "sma_5": detection.get("sma_5"),
        "sma_15": detection.get("sma_15"),
        "pullback_fraction": detection.get("pullback_fraction"),
        "price_file_sha256": detection["price_file_sha256"],
        "causal_entry": 1,
        "execution_eligible": 0,
        "research_only": 1,
    }


def replay_snapshot(
    database_path: Path,
    *,
    reused: bool,
    integrity: str | None = None,
    watch_count: int | None = None,
    exclusion_counts: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    database = sqlite3.connect(f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True)
    database.execute("PRAGMA query_only=ON")
    detection_count = int(database.execute(
        "SELECT COUNT(*) FROM response_detections WHERE replay_contract_id=?", (REPLAY_CONTRACT_ID,)
    ).fetchone()[0])
    event_count = int(database.execute(
        "SELECT COUNT(DISTINCT factor_id) FROM response_detections WHERE replay_contract_id=?", (REPLAY_CONTRACT_ID,)
    ).fetchone()[0])
    arms = dict(database.execute(
        "SELECT arm_id,COUNT(*) FROM response_detections WHERE replay_contract_id=? GROUP BY arm_id",
        (REPLAY_CONTRACT_ID,),
    ).fetchall())
    metrics_rows = database.execute(
        """
        SELECT d.arm_id,o.horizon_min,COUNT(*),AVG(o.win_after_cost),AVG(o.after_cost_pips),
               SUM(o.after_cost_pips),AVG(o.mfe_pips),AVG(o.mae_pips)
        FROM response_detections d JOIN response_entry_outcomes o ON o.detection_id=d.detection_id
        WHERE d.replay_contract_id=? GROUP BY d.arm_id,o.horizon_min ORDER BY d.arm_id,o.horizon_min
        """,
        (REPLAY_CONTRACT_ID,),
    ).fetchall()
    metrics = [
        {
            "arm_id": row[0], "horizon_min": int(row[1]), "n": int(row[2]),
            "win_rate": float(row[3]), "average_after_cost_pips": float(row[4]),
            "total_after_cost_pips": float(row[5]), "average_mfe_pips": float(row[6]),
            "average_mae_pips": float(row[7]),
        }
        for row in metrics_rows
    ]
    if integrity is None:
        integrity = str(database.execute("PRAGMA integrity_check").fetchone()[0])
    database.close()
    snapshot: dict[str, Any] = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "replay_contract_id": REPLAY_CONTRACT_ID,
        "factor_contract_id": FACTOR_CONTRACT_ID,
        "database_path": str(database_path.resolve()),
        "database_bytes": database_path.stat().st_size,
        "sqlite_integrity": integrity,
        "reused_existing_immutable_cohort": reused,
        "watch_event_count": watch_count,
        "detected_event_count": event_count,
        "detection_count": detection_count,
        "arm_detection_counts": arms,
        "exclusion_counts": dict(exclusion_counts or {}),
        "metrics": metrics,
        "discovery_only": True,
        "multiplicity_adjusted": False,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    snapshot["snapshot_sha256"] = sha256_bytes(canonical_json(snapshot).encode("utf-8"))
    return snapshot


def render_report(snapshot: Mapping[str, Any]) -> str:
    lines = [
        "# Spike/Blurb Event-First Response Entry Replay V1",
        "",
        f"- Generated: `{snapshot['generated_utc']}`",
        f"- Contract: `{snapshot['replay_contract_id']}`",
        f"- Snapshot SHA-256: `{snapshot['snapshot_sha256']}`",
        "- Safety: **discovery-only / execution-ineligible / no-trade**",
        "",
        "## Coverage",
        "",
        f"- Point-in-time official watch events: **{int(snapshot.get('watch_event_count') or 0):,}**",
        f"- Events with at least one synchronized response detection: **{snapshot['detected_event_count']:,}**",
        f"- Arm detections: **{snapshot['detection_count']:,}**",
        "",
        "## After-cost outcomes",
        "",
        "| Arm | Horizon | N | Win rate | Avg net pips | Total net pips | Avg MFE | Avg MAE |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in snapshot["metrics"]:
        lines.append(
            f"| `{row['arm_id']}` | {row['horizon_min']}m | {row['n']:,} | "
            f"{100.0 * row['win_rate']:.1f}% | {row['average_after_cost_pips']:+.3f} | "
            f"{row['total_after_cost_pips']:+.1f} | {row['average_mfe_pips']:+.2f} | "
            f"{row['average_mae_pips']:+.2f} |"
        )
    lines.extend(
        [
            "",
            "The source opens the watch; observed synchronized currency response supplies direction; the selected pair is the lowest-spread above-median agreeing leg. These results are exploratory, repeatedly inspected, and unadjusted for multiple testing. Any apparent winner requires a new frozen untouched cohort.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--candles", type=Path, default=CANDLES)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--output-json", type=Path, default=OUTPUT_JSON)
    parser.add_argument("--output-md", type=Path, default=OUTPUT_MD)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    snapshot = build_replay(
        config_path=args.config, candle_root=args.candles, database_path=args.database
    )
    atomic_text(args.output_json, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_text(args.output_md, render_report(snapshot))
    print(json.dumps(snapshot, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
