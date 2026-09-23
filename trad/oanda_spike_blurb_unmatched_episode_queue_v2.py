#!/usr/bin/env python3
"""Deduplicate liquid/moderate unmatched moves into hindsight research episodes.

This is a discovery queue, not a forecast.  A signed currency factor is used
only to collapse simultaneous correlated pair moves.  It is not asserted to be
the causal driver; both official currency legs remain available to later
timestamp-safe source research.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DATABASE = ROOT / "data/oanda_training_manager/state/spike_blurb_factor_reconstruction_v1.sqlite"
REPORT_ROOT = ROOT / "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/unmatched_episode_queue_v2"
CONTRACT_ID = "spike_blurb_unmatched_episode_queue_v2_20260820"
UPSTREAM_CONTRACT_ID = "spike_blurb_unmatched_source_queue_v1_20260819"
SCHEMA_VERSION = 2
CHUNK_SIZE = 40


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def stable_id(prefix: str, *parts: object) -> str:
    return f"{prefix}_{sha256_bytes('|'.join(map(str, parts)).encode())[:24]}"


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def pip_size(instrument: str) -> float:
    return 0.01 if instrument.endswith("_JPY") else 0.0001


def movement_bps(net_pips: float, instrument: str, entry_mid: float) -> float:
    if entry_mid <= 0:
        raise ValueError("entry_mid_must_be_positive")
    return abs(net_pips) * pip_size(instrument) / entry_mid * 10000.0


def signed_factor_key(candidate: dict[str, Any]) -> str:
    sign = int(candidate.get("sign") or 0)
    label = "positive" if sign > 0 else "negative" if sign < 0 else "flat"
    return f"{candidate.get('currency')}:{label}"


def choose_factor(candidates: list[dict[str, Any]], counts: Counter[str]) -> dict[str, Any]:
    if not candidates:
        raise ValueError("factor_candidates_missing")
    return sorted(candidates, key=lambda row: (-counts[signed_factor_key(row)], signed_factor_key(row)))[0]


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS unmatched_episode_queue_contracts (
          contract_id TEXT PRIMARY KEY,
          contract_json TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL,
          created_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS unmatched_research_episodes (
          episode_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          market_episode_15m TEXT NOT NULL,
          hindsight_factor_currency TEXT NOT NULL,
          hindsight_factor_sign INTEGER NOT NULL,
          hindsight_factor_key TEXT NOT NULL,
          representative_job_id TEXT NOT NULL,
          representative_move_id TEXT NOT NULL,
          representative_instrument TEXT NOT NULL,
          representative_start_utc TEXT NOT NULL,
          representative_end_utc TEXT NOT NULL,
          representative_best_direction TEXT NOT NULL,
          representative_net_pips REAL NOT NULL,
          representative_movement_bps REAL NOT NULL,
          representative_spread_pips REAL NOT NULL,
          liquidity_bucket TEXT NOT NULL,
          member_job_count INTEGER NOT NULL,
          instrument_count INTEGER NOT NULL,
          instruments_json TEXT NOT NULL,
          member_job_ids_json TEXT NOT NULL,
          currency_legs_json TEXT NOT NULL,
          official_source_plans_json TEXT NOT NULL,
          priority_score REAL NOT NULL,
          priority_rank INTEGER NOT NULL,
          chunk_id TEXT NOT NULL,
          causal_driver_state TEXT NOT NULL,
          factor_role TEXT NOT NULL,
          outcome_selected INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL,
          UNIQUE(contract_id, market_episode_15m, hindsight_factor_key),
          FOREIGN KEY(contract_id) REFERENCES unmatched_episode_queue_contracts(contract_id)
        );
        CREATE INDEX IF NOT EXISTS unmatched_episode_queue_chunk
          ON unmatched_research_episodes(contract_id, chunk_id, priority_rank);
        CREATE TRIGGER IF NOT EXISTS unmatched_episode_contracts_no_update
          BEFORE UPDATE ON unmatched_episode_queue_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS unmatched_episode_contracts_no_delete
          BEFORE DELETE ON unmatched_episode_queue_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS unmatched_episodes_no_update
          BEFORE UPDATE ON unmatched_research_episodes BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS unmatched_episodes_no_delete
          BEFORE DELETE ON unmatched_research_episodes BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def build_episode_queue(
    database: Path = DATABASE,
    report_root: Path = REPORT_ROOT,
    chunk_size: int = CHUNK_SIZE,
) -> dict[str, Any]:
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    ensure_schema(connection)
    source_rows = list(
        connection.execute(
            """
            SELECT q.*, p.entry_bid, p.entry_ask
            FROM unmatched_source_research_jobs q
            JOIN legacy_executable_price_windows p ON p.move_id=q.move_id
            WHERE q.contract_id=?
              AND q.price_coverage_state='exact_window'
              AND q.liquidity_bucket IN ('liquid_le_2p','moderate_2_to_5p')
            """,
            (UPSTREAM_CONTRACT_ID,),
        )
    )
    if len(source_rows) != 3135:
        raise RuntimeError(f"unexpected_exact_liquid_moderate_count:{len(source_rows)}")

    by_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in source_rows:
        candidates = json.loads(str(raw["hindsight_signed_factor_candidates_json"]))
        plan = json.loads(str(raw["official_source_plan_json"]))
        mid = (float(raw["entry_bid"]) + float(raw["entry_ask"])) / 2.0
        row = dict(raw)
        row["factor_candidates"] = candidates
        row["source_plan"] = plan
        row["movement_bps"] = movement_bps(
            float(raw["restored_best_net_pips"]), str(raw["instrument"]), mid
        )
        by_bucket[str(raw["market_episode_15m"])].append(row)

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for bucket, rows in by_bucket.items():
        counts: Counter[str] = Counter()
        for row in rows:
            counts.update(signed_factor_key(candidate) for candidate in row["factor_candidates"])
        for row in rows:
            chosen = choose_factor(row["factor_candidates"], counts)
            row["chosen_factor"] = chosen
            grouped[(bucket, signed_factor_key(chosen))].append(row)

    episodes: list[dict[str, Any]] = []
    for (bucket, factor_key), members in grouped.items():
        members.sort(
            key=lambda row: (
                -float(row["movement_bps"]),
                float(row["average_spread_pips"]),
                str(row["instrument"]),
                str(row["job_id"]),
            )
        )
        rep = members[0]
        factor = rep["chosen_factor"]
        instruments = sorted({str(row["instrument"]) for row in members})
        currencies = sorted(
            {str(row["base_currency"]) for row in members}
            | {str(row["quote_currency"]) for row in members}
        )
        plans: dict[str, dict[str, Any]] = {}
        for row in members:
            for plan in row["source_plan"]:
                plans[str(plan["currency"])] = plan
        breadth = len(instruments)
        priority_score = float(rep["movement_bps"]) * (1.0 + math.log1p(breadth))
        episode_id = stable_id("unmatched_episode", CONTRACT_ID, bucket, factor_key)
        episodes.append(
            {
                "episode_id": episode_id,
                "market_episode_15m": bucket,
                "hindsight_factor_currency": str(factor["currency"]),
                "hindsight_factor_sign": int(factor["sign"]),
                "hindsight_factor_key": factor_key,
                "representative_job_id": str(rep["job_id"]),
                "representative_move_id": str(rep["move_id"]),
                "representative_instrument": str(rep["instrument"]),
                "representative_start_utc": str(rep["start_utc"]),
                "representative_end_utc": str(rep["end_utc"]),
                "representative_best_direction": str(rep["restored_best_direction"]),
                "representative_net_pips": float(rep["restored_best_net_pips"]),
                "representative_movement_bps": float(rep["movement_bps"]),
                "representative_spread_pips": float(rep["average_spread_pips"]),
                "liquidity_bucket": str(rep["liquidity_bucket"]),
                "member_job_count": len(members),
                "instrument_count": breadth,
                "instruments_json": canonical_json(instruments),
                "member_job_ids_json": canonical_json(sorted(str(row["job_id"]) for row in members)),
                "currency_legs_json": canonical_json(currencies),
                "official_source_plans_json": canonical_json([plans[c] for c in sorted(plans)]),
                "priority_score": priority_score,
            }
        )

    episodes.sort(
        key=lambda row: (
            -row["priority_score"],
            -row["instrument_count"],
            row["market_episode_15m"],
            row["hindsight_factor_key"],
        )
    )
    for rank, row in enumerate(episodes, start=1):
        row["priority_rank"] = rank
        row["chunk_id"] = f"unmatched-episode-{((rank - 1) // chunk_size) + 1:04d}"

    builder_hash = sha256_file(Path(__file__).resolve())
    contract = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "upstream_contract_id": UPSTREAM_CONTRACT_ID,
        "builder_sha256": builder_hash,
        "source_job_count": len(source_rows),
        "source_filter": "exact_window_and_spread_le_5_pips",
        "episode_clock": "15_minute_floor",
        "factor_dedup": "most_broadly_expressed_hindsight_signed_currency_candidate_with_lexical_tie_break",
        "factor_role": "correlation_dedup_only_not_causal_attribution",
        "chunk_size": chunk_size,
        "research_only": True,
        "execution_eligible": False,
    }
    encoded = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json,builder_sha256 FROM unmatched_episode_queue_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    if existing is None:
        connection.execute(
            "INSERT INTO unmatched_episode_queue_contracts VALUES (?,?,?,?)",
            (CONTRACT_ID, encoded, builder_hash, utc_now()),
        )
    elif str(existing["contract_json"]) != encoded or str(existing["builder_sha256"]) != builder_hash:
        raise RuntimeError("immutable_unmatched_episode_contract_collision")

    statement = "INSERT OR IGNORE INTO unmatched_research_episodes VALUES (" + ",".join("?" for _ in range(31)) + ")"
    for row in episodes:
        connection.execute(
            statement,
            (
                row["episode_id"], CONTRACT_ID, row["market_episode_15m"],
                row["hindsight_factor_currency"], row["hindsight_factor_sign"], row["hindsight_factor_key"],
                row["representative_job_id"], row["representative_move_id"], row["representative_instrument"],
                row["representative_start_utc"], row["representative_end_utc"],
                row["representative_best_direction"], row["representative_net_pips"],
                row["representative_movement_bps"], row["representative_spread_pips"], row["liquidity_bucket"],
                row["member_job_count"], row["instrument_count"], row["instruments_json"],
                row["member_job_ids_json"], row["currency_legs_json"], row["official_source_plans_json"],
                row["priority_score"], row["priority_rank"], row["chunk_id"], "unresolved_do_not_force",
                "correlation_dedup_only_not_causal_attribution", 1, 0, 1, 0,
            ),
        )
    connection.commit()
    stored = int(connection.execute(
        "SELECT count(*) FROM unmatched_research_episodes WHERE contract_id=?", (CONTRACT_ID,)
    ).fetchone()[0])
    integrity = str(connection.execute("pragma integrity_check").fetchone()[0])
    connection.close()

    report_root.mkdir(parents=True, exist_ok=True)
    csv_path = report_root / "UNMATCHED_EPISODE_QUEUE_V2.csv"
    fields = [
        "priority_rank", "chunk_id", "episode_id", "market_episode_15m", "hindsight_factor_key",
        "representative_instrument", "representative_start_utc", "representative_end_utc",
        "representative_best_direction", "representative_net_pips", "representative_movement_bps",
        "representative_spread_pips", "liquidity_bucket", "member_job_count", "instrument_count",
        "priority_score", "causal_driver_state", "factor_role",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in episodes:
            writer.writerow({**row, "causal_driver_state": "unresolved_do_not_force", "factor_role": "correlation_dedup_only_not_causal_attribution"})

    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "generated_utc": utc_now(),
        "source_job_count": len(source_rows),
        "episode_count": stored,
        "deduplication_ratio": stored / len(source_rows),
        "chunk_count": math.ceil(stored / chunk_size),
        "chunk_size": chunk_size,
        "instrument_count": len({json.loads(row["instruments_json"])[i] for row in episodes for i in range(len(json.loads(row["instruments_json"]))) }),
        "currency_count": len({currency for row in episodes for currency in json.loads(row["currency_legs_json"])}),
        "multi_instrument_episode_count": sum(row["instrument_count"] > 1 for row in episodes),
        "maximum_instrument_breadth": max((row["instrument_count"] for row in episodes), default=0),
        "causal_driver_assignments": 0,
        "factor_role": "correlation_dedup_only_not_causal_attribution",
        "outcome_selected": True,
        "forecast_proof_eligible": False,
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
        "sqlite_integrity": integrity,
    }
    snapshot["snapshot_sha256"] = sha256_bytes(canonical_json(snapshot).encode())
    (report_root / "UNMATCHED_EPISODE_QUEUE_V2.json").write_text(
        json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8"
    )
    lines = [
        "# Unmatched liquid/moderate episode queue V2", "",
        f"- Exact liquid/moderate jobs: **{len(source_rows):,}**.",
        f"- Factor/time-deduplicated research episodes: **{stored:,}**.",
        f"- Chunks: **{snapshot['chunk_count']}** of at most {chunk_size}.",
        f"- Multi-instrument episodes: **{snapshot['multi_instrument_episode_count']:,}**; maximum breadth **{snapshot['maximum_instrument_breadth']}**.",
        "- Signed factor is a hindsight correlation-dedup key, not a causal assignment.",
        "- Execution decision: **no_trade**.", "", "## Highest-priority episodes", "",
        "| Rank | Episode | Factor* | Representative | Move bps* | Spread | Breadth |",
        "|---:|---|---|---|---:|---:|---:|",
    ]
    for row in episodes[:30]:
        lines.append(
            f"| {row['priority_rank']} | {row['market_episode_15m']} | {row['hindsight_factor_key']} | "
            f"{row['representative_instrument']} | {row['representative_movement_bps']:.2f} | "
            f"{row['representative_spread_pips']:.2f} | {row['instrument_count']} |"
        )
    lines.extend(["", "\\* Hindsight-only research prioritization and correlation deduplication."])
    (report_root / "UNMATCHED_EPISODE_QUEUE_V2.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(snapshot, indent=2, sort_keys=True))
    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)
    args = parser.parse_args()
    build_episode_queue(args.database, args.report_root, args.chunk_size)


if __name__ == "__main__":
    main()
