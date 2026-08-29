#!/usr/bin/env python3
"""Freeze the unmatched legacy-move official-source research queue.

The queue is movement-first and explicitly hindsight-only.  It does not assign
causality.  Each job carries both currency legs and their configured official
authority/source families so later research can look for the purest
timestamp-safe driver without silently treating a search result as evidence.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DATABASE = ROOT / "data/oanda_training_manager/state/spike_blurb_factor_reconstruction_v1.sqlite"
CENTRAL_BANK_MAP = ROOT / "config/official_central_bank_source_map_v1.json"
SOURCE_DEPTH_MAP = ROOT / "config/official_currency_source_depth_v1.json"
REPORT_ROOT = ROOT / "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/unmatched_source_queue_v1"
CONTRACT_ID = "spike_blurb_unmatched_source_queue_v1_20260819"
UPSTREAM_CONTRACT_ID = "spike_blurb_legacy_attribution_ledger_v1_20260819"
SCHEMA_VERSION = 1
CHUNK_SIZE = 50


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def stable_id(prefix: str, *parts: object) -> str:
    payload = "|".join(str(part) for part in parts).encode("utf-8")
    return f"{prefix}_{sha256_bytes(payload)[:24]}"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def liquidity_bucket(spread_pips: float) -> str:
    if spread_pips <= 2.0:
        return "liquid_le_2p"
    if spread_pips <= 5.0:
        return "moderate_2_to_5p"
    if spread_pips <= 15.0:
        return "wide_5_to_15p"
    return "very_wide_gt_15p"


def horizon_bucket(minutes: int) -> str:
    if minutes <= 15:
        return "intrahour_le_15m"
    if minutes <= 60:
        return "intrahour_16_to_60m"
    if minutes <= 240:
        return "multihour_61_to_240m"
    if minutes <= 1440:
        return "daily_241_to_1440m"
    return "multiday_gt_1440m"


def load_currency_sources(central_path: Path, depth_path: Path) -> dict[str, dict[str, Any]]:
    central = json.loads(central_path.read_text(encoding="utf-8"))
    depth = json.loads(depth_path.read_text(encoding="utf-8"))
    by_currency: dict[str, dict[str, Any]] = {}
    for row in central.get("currencies", []):
        currency = str(row["currency"])
        by_currency[currency] = {
            "currency": currency,
            "authority": row.get("authority"),
            "authority_id": row.get("authority_id"),
            "policy_framework": row.get("policy_framework"),
            "schedule_mode": row.get("schedule_mode"),
            "release_source_ids": list(row.get("release_source_ids") or []),
            "communication_source_ids": list(row.get("communication_source_ids") or []),
            "calendar_source_ids": list(row.get("calendar_source_ids") or []),
            "linked_driver_currency": row.get("linked_driver_currency"),
        }
    for row in depth.get("currencies", []):
        currency = str(row.get("currency") or "")
        target = by_currency.setdefault(currency, {"currency": currency})
        target["official_numeric_source_families"] = {
            key: list(value or [])
            for key, value in row.items()
            if key != "currency" and isinstance(value, list)
        }
    return by_currency


def authority_plan(currency: str, sources: dict[str, dict[str, Any]], event_date: str) -> dict[str, Any]:
    row = sources[currency]
    return {
        "currency": currency,
        "authority": row.get("authority"),
        "authority_id": row.get("authority_id"),
        "policy_framework": row.get("policy_framework"),
        "schedule_mode": row.get("schedule_mode"),
        "linked_driver_currency": row.get("linked_driver_currency"),
        "release_source_ids": row.get("release_source_ids", []),
        "communication_source_ids": row.get("communication_source_ids", []),
        "calendar_source_ids": row.get("calendar_source_ids", []),
        "official_numeric_source_families": row.get("official_numeric_source_families", {}),
        "research_query": f"{row.get('authority')} {event_date} policy economic release currency",
        "causality_state": "unresolved_do_not_force",
    }


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS unmatched_source_queue_contracts (
          contract_id TEXT PRIMARY KEY,
          contract_json TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL,
          created_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS unmatched_source_research_jobs (
          job_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          attribution_id TEXT NOT NULL,
          move_id TEXT NOT NULL,
          instrument TEXT NOT NULL,
          start_utc TEXT NOT NULL,
          end_utc TEXT NOT NULL,
          horizon_minutes INTEGER NOT NULL,
          horizon_bucket TEXT NOT NULL,
          price_coverage_state TEXT NOT NULL,
          restored_best_direction TEXT,
          restored_best_net_pips REAL,
          average_spread_pips REAL,
          liquidity_bucket TEXT NOT NULL,
          descriptive_move_to_cost REAL,
          base_currency TEXT NOT NULL,
          quote_currency TEXT NOT NULL,
          hindsight_signed_factor_candidates_json TEXT NOT NULL,
          official_source_plan_json TEXT NOT NULL,
          market_episode_15m TEXT NOT NULL,
          priority_rank INTEGER NOT NULL,
          chunk_id TEXT NOT NULL,
          research_state TEXT NOT NULL,
          causal_driver_state TEXT NOT NULL,
          outcome_selected INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL,
          UNIQUE(contract_id, attribution_id),
          FOREIGN KEY(contract_id) REFERENCES unmatched_source_queue_contracts(contract_id)
        );
        CREATE INDEX IF NOT EXISTS unmatched_source_jobs_chunk
          ON unmatched_source_research_jobs(contract_id, chunk_id, priority_rank);
        CREATE TRIGGER IF NOT EXISTS unmatched_source_contracts_no_update
          BEFORE UPDATE ON unmatched_source_queue_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS unmatched_source_contracts_no_delete
          BEFORE DELETE ON unmatched_source_queue_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS unmatched_source_jobs_no_update
          BEFORE UPDATE ON unmatched_source_research_jobs BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS unmatched_source_jobs_no_delete
          BEFORE DELETE ON unmatched_source_research_jobs BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def build_queue(
    database: Path = DATABASE,
    central_map: Path = CENTRAL_BANK_MAP,
    depth_map: Path = SOURCE_DEPTH_MAP,
    report_root: Path = REPORT_ROOT,
    chunk_size: int = CHUNK_SIZE,
) -> dict[str, Any]:
    if chunk_size < 1:
        raise ValueError("chunk_size_must_be_positive")
    sources = load_currency_sources(central_map, depth_map)
    builder_hash = sha256_file(Path(__file__).resolve())
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    ensure_schema(connection)
    rows = list(
        connection.execute(
            """
            SELECT * FROM legacy_attribution_cases
            WHERE contract_id=? AND causal_use_state='unmatched_or_unknown'
            """,
            (UPSTREAM_CONTRACT_ID,),
        )
    )
    if len(rows) != 5953:
        raise RuntimeError(f"unexpected_unmatched_count:{len(rows)}")

    prepared: list[dict[str, Any]] = []
    for raw in rows:
        instrument = str(raw["instrument"])
        base, quote = instrument.split("_", 1)
        if base not in sources or quote not in sources:
            raise RuntimeError(f"official_source_leg_missing:{instrument}")
        start = parse_utc(str(raw["start_utc"]))
        end = parse_utc(str(raw["end_utc"]))
        minutes = max(1, int(round((end - start).total_seconds() / 60.0)))
        spread = float(raw["average_spread_pips"] or 0.0)
        net = float(raw["restored_best_net_pips"] or 0.0)
        direction = str(raw["restored_best_direction"] or "unknown")
        base_sign = 1 if direction == "long" else -1 if direction == "short" else 0
        factor_candidates = [
            {"currency": base, "sign": base_sign, "role": "base_leg_candidate"},
            {"currency": quote, "sign": -base_sign, "role": "quote_leg_candidate"},
        ]
        event_date = start.date().isoformat()
        episode_minute = (start.minute // 15) * 15
        episode = start.replace(minute=episode_minute, second=0, microsecond=0).isoformat()
        prepared.append(
            {
                "attribution_id": str(raw["attribution_id"]),
                "move_id": str(raw["move_id"]),
                "instrument": instrument,
                "start_utc": start.isoformat(),
                "end_utc": end.isoformat(),
                "horizon_minutes": minutes,
                "horizon_bucket": horizon_bucket(minutes),
                "price_coverage_state": str(raw["price_coverage_state"]),
                "restored_best_direction": direction,
                "restored_best_net_pips": net,
                "average_spread_pips": spread,
                "liquidity_bucket": liquidity_bucket(spread),
                "descriptive_move_to_cost": net / max(spread, 0.1),
                "base_currency": base,
                "quote_currency": quote,
                "hindsight_signed_factor_candidates_json": canonical_json(factor_candidates),
                "official_source_plan_json": canonical_json(
                    [authority_plan(base, sources, event_date), authority_plan(quote, sources, event_date)]
                ),
                "market_episode_15m": episode,
            }
        )

    bucket_order = {"liquid_le_2p": 0, "moderate_2_to_5p": 1, "wide_5_to_15p": 2, "very_wide_gt_15p": 3}
    prepared.sort(
        key=lambda row: (
            0 if row["price_coverage_state"] == "exact_window" else 1,
            bucket_order[row["liquidity_bucket"]],
            -row["descriptive_move_to_cost"],
            -row["restored_best_net_pips"],
            row["start_utc"],
            row["instrument"],
            row["move_id"],
        )
    )
    for index, row in enumerate(prepared, start=1):
        row["priority_rank"] = index
        row["chunk_id"] = f"unmatched-source-{((index - 1) // chunk_size) + 1:04d}"
        row["job_id"] = stable_id("unmatched_source_job", CONTRACT_ID, row["attribution_id"])

    contract = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "upstream_contract_id": UPSTREAM_CONTRACT_ID,
        "builder_sha256": builder_hash,
        "central_bank_map_sha256": sha256_file(central_map),
        "source_depth_map_sha256": sha256_file(depth_map),
        "chunk_size": chunk_size,
        "movement_direction_semantics": "hindsight_best_executable_side_for_research_prioritization_only",
        "causality_policy": "both_currency_legs_unresolved_until_timestamp_safe_source_is_verified",
        "research_only": True,
        "execution_eligible": False,
    }
    existing = connection.execute(
        "SELECT contract_json,builder_sha256 FROM unmatched_source_queue_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    encoded_contract = canonical_json(contract)
    if existing is None:
        connection.execute(
            "INSERT INTO unmatched_source_queue_contracts VALUES (?,?,?,?)",
            (CONTRACT_ID, encoded_contract, builder_hash, utc_now()),
        )
    elif str(existing["contract_json"]) != encoded_contract or str(existing["builder_sha256"]) != builder_hash:
        raise RuntimeError("immutable_unmatched_source_contract_collision")

    sql = """
        INSERT OR IGNORE INTO unmatched_source_research_jobs VALUES
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    for row in prepared:
        values = (
            row["job_id"], CONTRACT_ID, row["attribution_id"], row["move_id"], row["instrument"],
            row["start_utc"], row["end_utc"], row["horizon_minutes"], row["horizon_bucket"],
            row["price_coverage_state"], row["restored_best_direction"], row["restored_best_net_pips"],
            row["average_spread_pips"], row["liquidity_bucket"], row["descriptive_move_to_cost"],
            row["base_currency"], row["quote_currency"], row["hindsight_signed_factor_candidates_json"],
            row["official_source_plan_json"], row["market_episode_15m"], row["priority_rank"], row["chunk_id"],
            "unresearched", "unresolved_do_not_force", 1, 0, 1, 0,
        )
        connection.execute(sql, values)
    connection.commit()

    stored = int(connection.execute(
        "SELECT count(*) FROM unmatched_source_research_jobs WHERE contract_id=?", (CONTRACT_ID,)
    ).fetchone()[0])
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    liquidity_counts = dict(connection.execute(
        "SELECT liquidity_bucket,count(*) FROM unmatched_source_research_jobs WHERE contract_id=? GROUP BY liquidity_bucket",
        (CONTRACT_ID,),
    ).fetchall())
    currency_counts = dict(connection.execute(
        """SELECT currency,count(*) FROM (
             SELECT base_currency AS currency FROM unmatched_source_research_jobs WHERE contract_id=?
             UNION ALL
             SELECT quote_currency FROM unmatched_source_research_jobs WHERE contract_id=?
           ) GROUP BY currency ORDER BY currency""",
        (CONTRACT_ID, CONTRACT_ID),
    ).fetchall())
    connection.close()

    report_root.mkdir(parents=True, exist_ok=True)
    csv_path = report_root / "UNMATCHED_SOURCE_RESEARCH_QUEUE_V1.csv"
    fields = [
        "priority_rank", "chunk_id", "job_id", "move_id", "instrument", "start_utc", "end_utc",
        "horizon_minutes", "horizon_bucket", "price_coverage_state", "restored_best_direction",
        "restored_best_net_pips", "average_spread_pips", "liquidity_bucket",
        "descriptive_move_to_cost", "base_currency", "quote_currency", "market_episode_15m",
        "causal_driver_state", "outcome_selected", "forecast_proof_eligible",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in prepared:
            writer.writerow({**row, "causal_driver_state": "unresolved_do_not_force", "outcome_selected": 1, "forecast_proof_eligible": 0})

    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "generated_utc": utc_now(),
        "job_count": stored,
        "chunk_count": math.ceil(stored / chunk_size),
        "chunk_size": chunk_size,
        "instrument_count": len({row["instrument"] for row in prepared}),
        "currency_count": len(currency_counts),
        "currency_leg_job_counts": currency_counts,
        "liquidity_bucket_counts": liquidity_counts,
        "exact_window_count": sum(row["price_coverage_state"] == "exact_window" for row in prepared),
        "partial_window_count": sum(row["price_coverage_state"] != "exact_window" for row in prepared),
        "official_source_plan_complete": all(row["base_currency"] in sources and row["quote_currency"] in sources for row in prepared),
        "causal_driver_assignments": 0,
        "outcome_selected": True,
        "forecast_proof_eligible": False,
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
        "sqlite_integrity": integrity,
    }
    snapshot["snapshot_sha256"] = sha256_bytes(canonical_json(snapshot).encode("utf-8"))
    json_path = report_root / "UNMATCHED_SOURCE_RESEARCH_QUEUE_V1.json"
    json_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8")
    md_path = report_root / "UNMATCHED_SOURCE_RESEARCH_QUEUE_V1.md"
    top = prepared[:20]
    lines = [
        "# Unmatched official-source research queue V1",
        "",
        f"- Jobs: **{stored:,}** in **{snapshot['chunk_count']}** chunks of at most {chunk_size}.",
        f"- Coverage: **{snapshot['instrument_count']}/68 pairs**, **{snapshot['currency_count']}/21 currencies**.",
        "- Causality: **0 assigned**. Both official currency legs remain unresolved until researched.",
        "- Direction is the hindsight best executable side and is never forecast proof.",
        "- Execution decision: **no_trade**.",
        "",
        "## Highest-priority research jobs",
        "",
        "| Rank | Pair | Start UTC | Horizon | Best side* | Net pips* | Spread | Bucket |",
        "|---:|---|---|---:|---|---:|---:|---|",
    ]
    for row in top:
        lines.append(
            f"| {row['priority_rank']} | {row['instrument']} | {row['start_utc']} | {row['horizon_minutes']}m | "
            f"{row['restored_best_direction']} | {row['restored_best_net_pips']:.2f} | "
            f"{row['average_spread_pips']:.2f} | {row['liquidity_bucket']} |"
        )
    lines.extend(["", "\\* Hindsight outcome used only to prioritize causal source research."])
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(snapshot, indent=2, sort_keys=True))
    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--central-map", type=Path, default=CENTRAL_BANK_MAP)
    parser.add_argument("--depth-map", type=Path, default=SOURCE_DEPTH_MAP)
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)
    args = parser.parse_args()
    build_queue(args.database, args.central_map, args.depth_map, args.report_root, args.chunk_size)


if __name__ == "__main__":
    main()
