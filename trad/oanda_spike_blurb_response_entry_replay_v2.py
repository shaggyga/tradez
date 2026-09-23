#!/usr/bin/env python3
"""Causality/cost repaired event-first response replay.

V1 is preserved as a falsification artifact.  V2 bars stale official items,
collapses same-currency ingestion batches before observing outcomes, and refuses
entry when every confirming expression costs more than five pips.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

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
    parse_epoch,
    sha256_bytes,
    utc_now,
)
from oanda_spike_blurb_movement_inventory import DATABASE, stable_id
from oanda_spike_blurb_response_entry_replay import (
    ARMS,
    MINIMUM_BREADTH,
    MINIMUM_LEGS,
    MODELED_SLIPPAGE_PIPS,
    OUTCOME_COLUMNS,
    PairSeries,
    ensure_schema,
    insert_rows,
    load_market,
    outcome_rows,
    technical_alignment,
)


REPLAY_CONTRACT_ID_V2 = "spike_blurb_response_entry_replay_v2_20260819"
OUTPUT_JSON = REPORT_ROOT / "SPIKE_BLURB_RESPONSE_ENTRY_REPLAY_V2.json"
OUTPUT_MD = REPORT_ROOT / "SPIKE_BLURB_RESPONSE_ENTRY_REPLAY_V2.md"
MAXIMUM_PUBLICATION_LAG_SEC = 15 * 60
MINIMUM_PUBLICATION_LAG_SEC = -2 * 60
SOURCE_BATCH_SEC = 5 * 60
MAXIMUM_ENTRY_SPREAD_PIPS = 5.0

RELEVANCE_PRIORITY = {
    "direct_action": 0,
    "direct_release_or_statement": 1,
    "official_context": 2,
    "official_unresolved_relevance": 3,
}


def normalize_fresh_watch_rows(rows: Iterable[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    exclusions: Counter[str] = Counter()
    selected: dict[tuple[str, int], dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        known = parse_epoch(row.get("known_utc"))
        published = parse_epoch(row.get("published_utc"))
        if known is None or published is None:
            exclusions["invalid_source_clock"] += 1
            continue
        lag = known - published
        if lag < MINIMUM_PUBLICATION_LAG_SEC:
            exclusions["future_dated_beyond_clock_tolerance"] += 1
            continue
        if lag > MAXIMUM_PUBLICATION_LAG_SEC:
            exclusions["stale_at_first_observation"] += 1
            continue
        row["known_epoch"] = known
        row["published_epoch"] = published
        row["publication_lag_sec"] = lag
        row["source_batch_id"] = f"source_batch_{row['currency']}_{known // SOURCE_BATCH_SEC}"
        key = (str(row["currency"]), known // SOURCE_BATCH_SEC)
        incumbent = selected.get(key)
        rank = (
            RELEVANCE_PRIORITY.get(str(row.get("relevance_state")), 9),
            abs(lag),
            str(row.get("source_id") or ""),
            str(row.get("factor_id") or ""),
        )
        if incumbent is None or rank < incumbent["_selection_rank"]:
            if incumbent is not None:
                exclusions["same_currency_source_batch_duplicate"] += 1
            row["_selection_rank"] = rank
            selected[key] = row
        else:
            exclusions["same_currency_source_batch_duplicate"] += 1
    output = []
    for row in sorted(selected.values(), key=lambda value: (value["known_epoch"], value["currency"], value["factor_id"])):
        row.pop("_selection_rank", None)
        output.append(row)
    return output, dict(sorted(exclusions.items()))


def load_fresh_watch_events(database: sqlite3.Connection) -> tuple[list[dict[str, Any]], dict[str, int]]:
    query = """
      SELECT f.factor_id,f.currency,f.known_utc,f.published_utc,f.factor_type,f.relevance_state,
             f.signed_factor_score,s.source_evidence_id,s.source_id,s.underlying_event_id,
             s.headline,s.event_type,s.source_priority_class,s.causal_state
      FROM factor_observations f
      JOIN source_attributions s ON s.source_evidence_id=f.source_evidence_id
      WHERE f.factor_contract_id=? AND f.causal_state='point_in_time_observed'
      ORDER BY f.currency,f.known_utc,f.factor_id
    """
    rows = []
    for values in database.execute(query, (FACTOR_CONTRACT_ID,)):
        rows.append(
            dict(
                zip(
                    [
                        "factor_id", "currency", "known_utc", "published_utc", "factor_type",
                        "relevance_state", "signed_factor_score", "source_evidence_id", "source_id",
                        "underlying_event_id", "headline", "event_type", "source_priority_class",
                        "causal_state",
                    ],
                    values,
                )
            )
        )
    return normalize_fresh_watch_rows(rows)


def currency_snapshot_v2(
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
            }
        )
    if len(legs) < MINIMUM_LEGS:
        return None
    returns = np.array([leg["oriented_return_bps"] for leg in legs], dtype=float)
    median = float(np.median(returns))
    if median == 0.0:
        return None
    direction = 1 if median > 0 else -1
    agreeing = [
        leg for leg in legs
        if int(math.copysign(1, leg["oriented_return_bps"] or direction)) == direction
    ]
    breadth = len(agreeing) / len(legs)
    executable = [
        leg for leg in agreeing
        if abs(float(leg["oriented_return_bps"])) >= abs(median)
        and float(leg["spread_pips"]) <= MAXIMUM_ENTRY_SPREAD_PIPS
    ]
    if not executable:
        return None
    selected = min(
        executable,
        key=lambda leg: (
            float(leg["spread_bps"]), -abs(float(leg["oriented_return_bps"])), str(leg["instrument"])
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


def detect_arm_v2(
    *, market: Mapping[str, PairSeries], currency: str, known_epoch: int,
    arm_id: str, arm: Mapping[str, Any]
) -> dict[str, Any] | None:
    baseline_epoch = ((int(known_epoch) + 59) // 60) * 60
    for elapsed_min in range(int(arm["start_min"]), int(arm["end_min"]) + 1):
        target_epoch = baseline_epoch + elapsed_min * 60
        snapshot = currency_snapshot_v2(market, currency, baseline_epoch, target_epoch)
        if snapshot is None:
            continue
        if snapshot["breadth"] < MINIMUM_BREADTH:
            continue
        if abs(float(snapshot["currency_strength_bps"])) < float(arm["minimum_strength_bps"]):
            continue
        persistence = int(arm["persistence_min"])
        if persistence > 1:
            prior_epoch = target_epoch - persistence * 60
            prior = currency_snapshot_v2(market, currency, baseline_epoch, prior_epoch)
            recent = currency_snapshot_v2(market, currency, prior_epoch, target_epoch)
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
GOVERNANCE_COLUMNS = [
    "governance_id", "replay_contract_id", "detection_id", "publication_lag_sec",
    "source_batch_id", "cost_bucket", "evidence_unit_id", "research_only",
]


def ensure_v2_schema(database: sqlite3.Connection) -> None:
    ensure_schema(database)
    database.executescript(
        """
        CREATE TABLE IF NOT EXISTS response_detection_governance_v2 (
          governance_id TEXT PRIMARY KEY,
          replay_contract_id TEXT NOT NULL,
          detection_id TEXT NOT NULL,
          publication_lag_sec INTEGER NOT NULL,
          source_batch_id TEXT NOT NULL,
          cost_bucket TEXT NOT NULL,
          evidence_unit_id TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          UNIQUE(replay_contract_id,detection_id),
          FOREIGN KEY(detection_id) REFERENCES response_detections(detection_id)
        );
        CREATE INDEX IF NOT EXISTS response_detection_governance_v2_unit
          ON response_detection_governance_v2(replay_contract_id,evidence_unit_id);
        CREATE TRIGGER IF NOT EXISTS response_detection_governance_v2_no_update
          BEFORE UPDATE ON response_detection_governance_v2 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS response_detection_governance_v2_no_delete
          BEFORE DELETE ON response_detection_governance_v2 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def detection_and_governance(
    watch: Mapping[str, Any], detection: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    detection_id = stable_id(
        "response_detection", REPLAY_CONTRACT_ID_V2, watch["factor_id"], detection["arm_id"],
        detection["detection_epoch"], detection["selected_instrument"],
    )
    row = {
        "detection_id": detection_id,
        "replay_contract_id": REPLAY_CONTRACT_ID_V2,
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
        "pullback_fraction": None,
        "price_file_sha256": detection["price_file_sha256"],
        "causal_entry": 1,
        "execution_eligible": 0,
        "research_only": 1,
    }
    cost_bucket = "liquid_le2" if detection["selected_spread_pips"] <= 2.0 else "moderate_2_5"
    evidence_unit_id = stable_id(
        "response_evidence_unit",
        REPLAY_CONTRACT_ID_V2,
        detection["arm_id"],
        detection["selected_instrument"],
        detection["selected_pair_direction"],
        int(detection["detection_epoch"]) // SOURCE_BATCH_SEC,
    )
    governance = {
        "governance_id": stable_id("response_governance", REPLAY_CONTRACT_ID_V2, detection_id),
        "replay_contract_id": REPLAY_CONTRACT_ID_V2,
        "detection_id": detection_id,
        "publication_lag_sec": int(watch["publication_lag_sec"]),
        "source_batch_id": watch["source_batch_id"],
        "cost_bucket": cost_bucket,
        "evidence_unit_id": evidence_unit_id,
        "research_only": 1,
    }
    return row, governance


def v2_outcomes(
    *, detection_id: str, detection: Mapping[str, Any], market: Mapping[str, PairSeries]
) -> list[dict[str, Any]]:
    rows = outcome_rows(detection_id=detection_id, detection=detection, market=market)
    for row in rows:
        row["replay_contract_id"] = REPLAY_CONTRACT_ID_V2
        row["outcome_id"] = stable_id(
            "response_outcome", REPLAY_CONTRACT_ID_V2, detection_id, row["horizon_min"]
        )
    return rows


def build_replay_v2(
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
    ensure_v2_schema(database)
    existing = database.execute(
        "SELECT builder_sha256,price_manifest_sha256 FROM response_replay_contracts WHERE replay_contract_id=?",
        (REPLAY_CONTRACT_ID_V2,),
    ).fetchone()
    if existing:
        database.close()
        if tuple(existing) != (builder_sha, price_manifest_sha256):
            raise RuntimeError("immutable_replay_v2_contract_collision")
        return snapshot_v2(database_path, reused=True)
    watches, exclusions = load_fresh_watch_events(database)
    detections: list[dict[str, Any]] = []
    governance: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    no_response = 0
    for watch in watches:
        watch_detected = False
        for arm_id, arm in ARMS.items():
            detection = detect_arm_v2(
                market=market,
                currency=str(watch["currency"]),
                known_epoch=int(watch["known_epoch"]),
                arm_id=arm_id,
                arm=arm,
            )
            if detection is None:
                continue
            watch_detected = True
            row, governance_row = detection_and_governance(watch, detection)
            detections.append(row)
            governance.append(governance_row)
            outcomes.extend(v2_outcomes(detection_id=row["detection_id"], detection=detection, market=market))
        if not watch_detected:
            no_response += 1
    exclusions["no_cost_clear_synchronized_response"] = no_response
    replay_contract = {
        "replay_contract_id": REPLAY_CONTRACT_ID_V2,
        "factor_contract_id": FACTOR_CONTRACT_ID,
        "created_utc": utc_now(),
        "builder_sha256": builder_sha,
        "price_manifest_sha256": price_manifest_sha256,
        "contract_json": canonical_json(
            {
                "supersedes_discovery_contract": "spike_blurb_response_entry_replay_v1_20260819",
                "repair_reason": "bar_stale_publications_deduplicate_source_batches_and_cap_entry_cost",
                "maximum_publication_lag_sec": MAXIMUM_PUBLICATION_LAG_SEC,
                "minimum_publication_lag_sec": MINIMUM_PUBLICATION_LAG_SEC,
                "source_batch_sec": SOURCE_BATCH_SEC,
                "maximum_entry_spread_pips": MAXIMUM_ENTRY_SPREAD_PIPS,
                "arms": ARMS,
                "research_only": True,
                "execution_eligible": False,
            }
        ),
    }
    with database:
        insert_rows(database, "response_replay_contracts", list(replay_contract), [replay_contract])
        insert_rows(database, "response_detections", DETECTION_COLUMNS, detections)
        insert_rows(database, "response_detection_governance_v2", GOVERNANCE_COLUMNS, governance)
        insert_rows(database, "response_entry_outcomes", OUTCOME_COLUMNS, outcomes)
    integrity = str(database.execute("PRAGMA integrity_check").fetchone()[0])
    database.close()
    return snapshot_v2(
        database_path,
        reused=False,
        integrity=integrity,
        watch_count=len(watches),
        exclusion_counts=exclusions,
    )


def snapshot_v2(
    database_path: Path,
    *, reused: bool,
    integrity: str | None = None,
    watch_count: int | None = None,
    exclusion_counts: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    database = sqlite3.connect(f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True)
    database.execute("PRAGMA query_only=ON")
    counts = dict(database.execute(
        "SELECT arm_id,COUNT(*) FROM response_detections WHERE replay_contract_id=? GROUP BY arm_id",
        (REPLAY_CONTRACT_ID_V2,),
    ).fetchall())
    metrics = []
    query = """
      SELECT d.arm_id,o.horizon_min,g.cost_bucket,COUNT(*),AVG(o.win_after_cost),
             AVG(o.after_cost_pips),SUM(o.after_cost_pips),AVG(o.mfe_pips),AVG(o.mae_pips)
      FROM response_detections d
      JOIN response_entry_outcomes o ON o.detection_id=d.detection_id
      JOIN response_detection_governance_v2 g ON g.detection_id=d.detection_id
      WHERE d.replay_contract_id=?
      GROUP BY d.arm_id,o.horizon_min,g.cost_bucket
      ORDER BY d.arm_id,o.horizon_min,g.cost_bucket
    """
    for row in database.execute(query, (REPLAY_CONTRACT_ID_V2,)):
        metrics.append(
            {
                "arm_id": row[0], "horizon_min": int(row[1]), "cost_bucket": row[2],
                "n": int(row[3]), "win_rate": float(row[4]),
                "average_after_cost_pips": float(row[5]), "total_after_cost_pips": float(row[6]),
                "average_mfe_pips": float(row[7]), "average_mae_pips": float(row[8]),
            }
        )
    evidence_units = int(database.execute(
        "SELECT COUNT(DISTINCT evidence_unit_id) FROM response_detection_governance_v2 WHERE replay_contract_id=?",
        (REPLAY_CONTRACT_ID_V2,),
    ).fetchone()[0])
    if integrity is None:
        integrity = str(database.execute("PRAGMA integrity_check").fetchone()[0])
    database.close()
    snapshot: dict[str, Any] = {
        "schema_version": 2,
        "generated_utc": utc_now(),
        "replay_contract_id": REPLAY_CONTRACT_ID_V2,
        "factor_contract_id": FACTOR_CONTRACT_ID,
        "watch_event_count": watch_count,
        "arm_detection_counts": counts,
        "effective_evidence_unit_count": evidence_units,
        "exclusion_counts": dict(exclusion_counts or {}),
        "metrics": metrics,
        "sqlite_integrity": integrity,
        "reused_existing_immutable_cohort": reused,
        "discovery_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    snapshot["snapshot_sha256"] = sha256_bytes(canonical_json(snapshot).encode("utf-8"))
    return snapshot


def render_report(snapshot: Mapping[str, Any]) -> str:
    lines = [
        "# Spike/Blurb Event-First Response Entry Replay V2",
        "",
        f"- Generated: `{snapshot['generated_utc']}`",
        f"- Contract: `{snapshot['replay_contract_id']}`",
        f"- Snapshot SHA-256: `{snapshot['snapshot_sha256']}`",
        "- Safety: **causality/cost-repaired discovery / execution-ineligible / no-trade**",
        "",
        "V2 bars sources first seen more than 15 minutes after publication, collapses one currency's five-minute ingestion batch before outcomes, and refuses entry above five pips of spread.",
        "",
        f"- Fresh deduplicated watch events: **{int(snapshot.get('watch_event_count') or 0):,}**",
        f"- Effective detected market-response units: **{snapshot['effective_evidence_unit_count']:,}**",
        "",
        "| Arm | Horizon | Cost bucket | N | Win rate | Avg net pips | Total net pips |",
        "|---|---:|---|---:|---:|---:|---:|",
    ]
    for row in snapshot["metrics"]:
        lines.append(
            f"| `{row['arm_id']}` | {row['horizon_min']}m | `{row['cost_bucket']}` | "
            f"{row['n']:,} | {100.0*row['win_rate']:.1f}% | "
            f"{row['average_after_cost_pips']:+.3f} | {row['total_after_cost_pips']:+.1f} |"
        )
    lines.extend(
        [
            "",
            "All cells remain exploratory and multiplicity-unadjusted. A positive cell would become a newly frozen hypothesis for untouched prospective confirmation, not a practice entry rule.",
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
    snapshot = build_replay_v2(
        config_path=args.config, candle_root=args.candles, database_path=args.database
    )
    atomic_text(args.output_json, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_text(args.output_md, render_report(snapshot))
    print(json.dumps(snapshot, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
