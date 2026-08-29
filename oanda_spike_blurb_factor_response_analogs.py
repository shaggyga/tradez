#!/usr/bin/env python3
"""Build immutable pure-factor to all-leg currency-response analogs.

This is an attribution and hypothesis-discovery ledger.  It measures the
currency response after a timestamp-valid official factor and computes a
strictly prequential sign mapping using only earlier, already-matured analogs.
It does not select an executable pair or authorize trading.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np

from oanda_spike_blurb_factor_ledger import FACTOR_CONTRACT_ID
from oanda_spike_blurb_factor_reconstruction import (
    CANDLES,
    CONFIG,
    REPORT_ROOT,
    atomic_text,
    canonical_json,
    file_sha256,
    load_contract,
    sha256_bytes,
    utc_now,
)
from oanda_spike_blurb_movement_inventory import DATABASE, stable_id
from oanda_spike_blurb_response_entry_replay import PairSeries, load_market
from oanda_spike_blurb_response_entry_replay_v2 import load_fresh_watch_events


CONTRACT_ID = "spike_blurb_factor_response_analogs_v1_20260819"
HORIZONS_MIN = (1, 3, 5, 10, 15, 30, 60, 120)
MINIMUM_PREQUENTIAL_ANALOGS = 3
OUTPUT_JSON = REPORT_ROOT / "SPIKE_BLURB_FACTOR_RESPONSE_ANALOGS_V1.json"
OUTPUT_MD = REPORT_ROOT / "SPIKE_BLURB_FACTOR_RESPONSE_ANALOGS_V1.md"


def sign(value: float) -> int:
    return 1 if value > 0 else (-1 if value < 0 else 0)


def measure_currency_response(
    market: Mapping[str, PairSeries], currency: str, baseline_epoch: int, target_epoch: int
) -> dict[str, Any] | None:
    legs: list[float] = []
    for pair in market.values():
        if currency not in {pair.base_currency, pair.quote_currency}:
            continue
        baseline = pair.exact_index(baseline_epoch)
        target = pair.exact_index(target_epoch)
        if baseline is None or target is None or target <= baseline:
            continue
        raw = (float(pair.mid[target]) / float(pair.mid[baseline]) - 1.0) * 10_000.0
        legs.append(raw if pair.base_currency == currency else -raw)
    if len(legs) < 3:
        return None
    median = float(np.median(np.asarray(legs, dtype=float)))
    direction = sign(median)
    agreeing = sum(1 for value in legs if sign(value) == direction) if direction else 0
    return {
        "currency_response_bps": median,
        "currency_response_sign": direction,
        "response_breadth": agreeing / len(legs),
        "leg_count": len(legs),
        "response_dispersion_bps": float(np.std(np.asarray(legs, dtype=float), ddof=0)),
    }


def analog_key(watch: Mapping[str, Any]) -> str:
    return "|".join(
        [
            str(watch.get("source_id") or "unknown"),
            str(watch.get("factor_type") or "unknown"),
            str(watch.get("relevance_state") or "unknown"),
        ]
    )


def attach_prequential_mapping(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output = [dict(row) for row in rows]
    by_key: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in output:
        by_key[(str(row["analog_key"]), int(row["horizon_min"]))].append(row)
    for group in by_key.values():
        group.sort(key=lambda row: (int(row["baseline_epoch"]), str(row["factor_id"])))
        for current in group:
            alignments = []
            for prior in group:
                if int(prior["target_epoch"]) > int(current["baseline_epoch"]):
                    continue
                factor_sign = sign(float(prior["signed_factor_score"]))
                response_sign = int(prior["currency_response_sign"])
                if factor_sign and response_sign:
                    alignments.append(factor_sign * response_sign)
            current["prior_matured_analog_n"] = len(alignments)
            current["prior_alignment_mean"] = (sum(alignments) / len(alignments)) if alignments else None
            orientation = sign(float(current["prior_alignment_mean"] or 0.0))
            usable = len(alignments) >= MINIMUM_PREQUENTIAL_ANALOGS and orientation != 0
            prediction = orientation * sign(float(current["signed_factor_score"])) if usable else 0
            current["prequential_orientation_sign"] = orientation if usable else 0
            current["prequential_prediction_sign"] = prediction
            current["prequential_correct"] = (
                int(prediction == int(current["currency_response_sign"]))
                if prediction and int(current["currency_response_sign"]) else None
            )
    return output


RESPONSE_COLUMNS = [
    "response_id", "contract_id", "factor_id", "source_evidence_id", "source_id",
    "underlying_event_id", "source_batch_id", "analog_key", "currency", "factor_type",
    "relevance_state", "numeric_measurement_state", "signed_factor_score", "known_utc",
    "published_utc", "publication_lag_sec", "baseline_epoch", "target_epoch", "horizon_min",
    "currency_response_bps", "currency_response_sign", "response_breadth", "leg_count",
    "response_dispersion_bps", "prior_matured_analog_n", "prior_alignment_mean",
    "prequential_orientation_sign", "prequential_prediction_sign", "prequential_correct",
    "research_only", "execution_eligible",
]


def ensure_schema(database: sqlite3.Connection) -> None:
    database.executescript(
        """
        CREATE TABLE IF NOT EXISTS factor_response_analog_contracts (
          contract_id TEXT PRIMARY KEY, factor_contract_id TEXT NOT NULL, created_utc TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL, price_manifest_sha256 TEXT NOT NULL, contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS factor_response_observations (
          response_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, factor_id TEXT NOT NULL,
          source_evidence_id TEXT NOT NULL, source_id TEXT NOT NULL, underlying_event_id TEXT NOT NULL,
          source_batch_id TEXT NOT NULL, analog_key TEXT NOT NULL, currency TEXT NOT NULL,
          factor_type TEXT NOT NULL, relevance_state TEXT NOT NULL, numeric_measurement_state TEXT NOT NULL,
          signed_factor_score REAL NOT NULL, known_utc TEXT NOT NULL, published_utc TEXT NOT NULL,
          publication_lag_sec INTEGER NOT NULL, baseline_epoch INTEGER NOT NULL, target_epoch INTEGER NOT NULL,
          horizon_min INTEGER NOT NULL, currency_response_bps REAL NOT NULL,
          currency_response_sign INTEGER NOT NULL, response_breadth REAL NOT NULL, leg_count INTEGER NOT NULL,
          response_dispersion_bps REAL NOT NULL, prior_matured_analog_n INTEGER NOT NULL,
          prior_alignment_mean REAL, prequential_orientation_sign INTEGER NOT NULL,
          prequential_prediction_sign INTEGER NOT NULL, prequential_correct INTEGER,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          UNIQUE(contract_id,factor_id,horizon_min),
          FOREIGN KEY(contract_id) REFERENCES factor_response_analog_contracts(contract_id)
        );
        CREATE INDEX IF NOT EXISTS factor_response_analog_key
          ON factor_response_observations(contract_id,analog_key,horizon_min,baseline_epoch);
        CREATE TRIGGER IF NOT EXISTS factor_response_contract_no_update
          BEFORE UPDATE ON factor_response_analog_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS factor_response_contract_no_delete
          BEFORE DELETE ON factor_response_analog_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS factor_response_observation_no_update
          BEFORE UPDATE ON factor_response_observations BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS factor_response_observation_no_delete
          BEFORE DELETE ON factor_response_observations BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def build_rows(watches: Iterable[Mapping[str, Any]], market: Mapping[str, PairSeries]) -> list[dict[str, Any]]:
    raw: list[dict[str, Any]] = []
    for watch in watches:
        baseline = ((int(watch["known_epoch"]) + 59) // 60) * 60
        for horizon in HORIZONS_MIN:
            target = baseline + horizon * 60
            response = measure_currency_response(market, str(watch["currency"]), baseline, target)
            if response is None:
                continue
            raw.append(
                {
                    "response_id": stable_id("factor_response", CONTRACT_ID, watch["factor_id"], horizon),
                    "contract_id": CONTRACT_ID,
                    "factor_id": watch["factor_id"], "source_evidence_id": watch["source_evidence_id"],
                    "source_id": watch["source_id"], "underlying_event_id": watch["underlying_event_id"],
                    "source_batch_id": watch["source_batch_id"], "analog_key": analog_key(watch),
                    "currency": watch["currency"], "factor_type": watch["factor_type"],
                    "relevance_state": watch["relevance_state"],
                    "numeric_measurement_state": watch.get("numeric_measurement_state") or "unknown",
                    "signed_factor_score": float(watch["signed_factor_score"]),
                    "known_utc": watch["known_utc"], "published_utc": watch["published_utc"],
                    "publication_lag_sec": int(watch["publication_lag_sec"]),
                    "baseline_epoch": baseline, "target_epoch": target, "horizon_min": horizon,
                    **response,
                    "prior_matured_analog_n": 0, "prior_alignment_mean": None,
                    "prequential_orientation_sign": 0, "prequential_prediction_sign": 0,
                    "prequential_correct": None, "research_only": 1, "execution_eligible": 0,
                }
            )
    return attach_prequential_mapping(raw)


def snapshot(database_path: Path, *, watch_count: int | None, exclusions: Mapping[str, int], reused: bool) -> dict[str, Any]:
    database = sqlite3.connect(f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True)
    total, factors, keys, currencies = database.execute(
        """SELECT COUNT(*),COUNT(DISTINCT factor_id),COUNT(DISTINCT analog_key),COUNT(DISTINCT currency)
             FROM factor_response_observations WHERE contract_id=?""", (CONTRACT_ID,)
    ).fetchone()
    metrics = []
    for row in database.execute(
        """SELECT horizon_min,COUNT(*),AVG(ABS(currency_response_bps)),AVG(response_breadth),
                  SUM(CASE WHEN prequential_correct IS NOT NULL THEN 1 ELSE 0 END),
                  AVG(prequential_correct)
             FROM factor_response_observations WHERE contract_id=? GROUP BY horizon_min ORDER BY horizon_min""",
        (CONTRACT_ID,),
    ):
        metrics.append(
            {
                "horizon_min": int(row[0]), "n": int(row[1]),
                "average_absolute_currency_response_bps": float(row[2]),
                "average_response_breadth": float(row[3]),
                "prequential_prediction_n": int(row[4] or 0),
                "prequential_direction_accuracy": float(row[5]) if row[5] is not None else None,
            }
        )
    coverage = dict(database.execute(
        "SELECT factor_type,COUNT(DISTINCT factor_id) FROM factor_response_observations WHERE contract_id=? GROUP BY factor_type ORDER BY factor_type",
        (CONTRACT_ID,),
    ).fetchall())
    integrity = str(database.execute("PRAGMA integrity_check").fetchone()[0])
    database.close()
    result = {
        "schema_version": 1, "generated_utc": utc_now(), "contract_id": CONTRACT_ID,
        "factor_contract_id": FACTOR_CONTRACT_ID, "fresh_watch_count": watch_count,
        "watch_exclusions": dict(exclusions), "response_row_count": int(total),
        "factor_count": int(factors), "analog_key_count": int(keys), "currency_count": int(currencies),
        "factor_type_coverage": coverage, "horizon_metrics": metrics, "sqlite_integrity": integrity,
        "reused_existing_immutable_cohort": reused, "discovery_only": True,
        "execution_eligible": False, "supported_execution_decision": "no_trade",
    }
    result["snapshot_sha256"] = sha256_bytes(canonical_json(result).encode("utf-8"))
    return result


def build(
    *, config_path: Path = CONFIG, candle_root: Path = CANDLES, database_path: Path = DATABASE,
) -> dict[str, Any]:
    contract = load_contract(config_path)
    market = load_market(candle_root, contract["expected_instruments"])
    manifest = [{"instrument": pair.instrument, "sha256": pair.price_file_sha256} for pair in market.values()]
    price_sha = sha256_bytes(canonical_json(manifest).encode("utf-8"))
    builder_sha = file_sha256(Path(__file__).resolve())
    database = sqlite3.connect(database_path, timeout=120)
    database.execute("PRAGMA journal_mode=WAL")
    database.execute("PRAGMA synchronous=FULL")
    database.execute("PRAGMA foreign_keys=ON")
    ensure_schema(database)
    existing = database.execute(
        "SELECT builder_sha256,price_manifest_sha256 FROM factor_response_analog_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    if existing:
        database.close()
        if tuple(existing) != (builder_sha, price_sha):
            raise RuntimeError("immutable_factor_response_contract_collision")
        return snapshot(database_path, watch_count=None, exclusions={}, reused=True)
    watches, exclusions = load_fresh_watch_events(database)
    # Enrich the frozen watch rows with the factor field not needed by V2.
    factor_states = dict(database.execute(
        "SELECT factor_id,numeric_measurement_state FROM factor_observations WHERE factor_contract_id=?",
        (FACTOR_CONTRACT_ID,),
    ).fetchall())
    for watch in watches:
        watch["numeric_measurement_state"] = factor_states.get(watch["factor_id"], "unknown")
    rows = build_rows(watches, market)
    contract_row = {
        "contract_id": CONTRACT_ID, "factor_contract_id": FACTOR_CONTRACT_ID,
        "created_utc": utc_now(), "builder_sha256": builder_sha, "price_manifest_sha256": price_sha,
        "contract_json": canonical_json(
            {"horizons_min":HORIZONS_MIN,"minimum_prequential_analogs":MINIMUM_PREQUENTIAL_ANALOGS,
             "mapping_key":"source_id|factor_type|relevance_state","matured_prior_only":True,
             "research_only":True,"execution_eligible":False}
        ),
    }
    marks = ",".join("?" for _ in RESPONSE_COLUMNS)
    with database:
        database.execute(
            "INSERT INTO factor_response_analog_contracts VALUES (?,?,?,?,?,?)", tuple(contract_row.values())
        )
        database.executemany(
            f"INSERT INTO factor_response_observations ({','.join(RESPONSE_COLUMNS)}) VALUES ({marks})",
            [[row.get(column) for column in RESPONSE_COLUMNS] for row in rows],
        )
    database.close()
    return snapshot(database_path, watch_count=len(watches), exclusions=exclusions, reused=False)


def render_report(report: Mapping[str, Any]) -> str:
    lines = [
        "# Pure-Source Factor/Response Analog Ledger V1", "",
        f"- Generated: `{report['generated_utc']}`", f"- Contract: `{report['contract_id']}`",
        f"- Snapshot SHA-256: `{report['snapshot_sha256']}`",
        f"- Fresh factors: **{report['factor_count']:,}**; analog identities: **{report['analog_key_count']:,}**; currencies: **{report['currency_count']:,}**",
        "", "| Horizon | N | Mean absolute currency response | Prequential N | Direction accuracy |",
        "|---:|---:|---:|---:|---:|",
    ]
    for row in report["horizon_metrics"]:
        accuracy = "—" if row["prequential_direction_accuracy"] is None else f"{100*row['prequential_direction_accuracy']:.1f}%"
        lines.append(
            f"| {row['horizon_min']}m | {row['n']:,} | {row['average_absolute_currency_response_bps']:.3f} bps | {row['prequential_prediction_n']:,} | {accuracy} |"
        )
    lines.extend([
        "", "The prequential mapping uses only earlier examples whose response horizon had matured before the next factor was known. It remains retrospective discovery because the mapping design was selected after inspecting this project.",
        "", "No executable pair was outcome-selected, no cost-adjusted edge is claimed, and the supported decision remains `no_trade`.", "",
    ])
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
    report = build(config_path=args.config, candle_root=args.candles, database_path=args.database)
    atomic_text(args.output_json, json.dumps(report, indent=2, sort_keys=True) + "\n")
    atomic_text(args.output_md, render_report(report))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
