#!/usr/bin/env python3
"""Select diverse immutable movement cases and export API-free research chunks."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from oanda_spike_blurb_factor_reconstruction import (
    REPORT_ROOT,
    atomic_text,
    canonical_json,
    file_sha256,
    sha256_bytes,
    utc_now,
)
from oanda_spike_blurb_movement_inventory import DATABASE, stable_id


CONTRACT_ID = "spike_blurb_representative_cases_v1_20260819"
BUILD_CONTRACT_ID = "spike_blurb_movement_inventory_v1_20260819"
TOP_PER_PAIR = 25
MAX_OVERLAP = 0.25
OUTPUT_ROOT = REPORT_ROOT / "representative_spike_blurb_cases_v1"
OUTPUT_JSON = OUTPUT_ROOT / "REPRESENTATIVE_CASES_V1.json"
OUTPUT_CSV = OUTPUT_ROOT / "representative_cases_v1.csv"
OUTPUT_MD = OUTPUT_ROOT / "RESEARCH_CHUNKS_V1.md"


def horizon_class(minutes: int) -> str:
    if minutes <= 15:
        return "micro_1_15m"
    if minutes <= 60:
        return "intrahour_30_60m"
    if minutes <= 360:
        return "session_2_6h"
    if minutes <= 1440:
        return "intraday_12_24h"
    return "multiday_2_30d"


def overlap_ratio(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
    start = max(int(left["entry_epoch"]), int(right["entry_epoch"]))
    end = min(int(left["exit_epoch"]), int(right["exit_epoch"]))
    overlap = max(0, end - start)
    shortest = max(1, min(int(left["exit_epoch"])-int(left["entry_epoch"]), int(right["exit_epoch"])-int(right["entry_epoch"])))
    return overlap / shortest


def candidate_score(row: Mapping[str, Any]) -> float:
    return (
        max(0.0, float(row["after_cost_pips"]))
        * max(0.05, float(row["path_efficiency"]))
        * math.log1p(max(0.0, float(row["gross_cost_multiple"])))
        * (1.15 if row.get("selection_tier") == "q99" else 1.0)
    )


def select_pair_cases(rows: Iterable[Mapping[str, Any]], limit: int = TOP_PER_PAIR) -> list[dict[str, Any]]:
    pool = [dict(row) for row in rows]
    for row in pool:
        row["horizon_class"] = horizon_class(int(row["horizon_min"]))
        row["selection_score"] = candidate_score(row)
    ranked = sorted(pool, key=lambda row: (-row["selection_score"], row["entry_epoch"], row["candidate_id"]))
    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()

    def take(predicate, reason: str) -> None:
        for row in ranked:
            if len(selected) >= limit or not predicate(row) or str(row["candidate_id"]) in selected_ids:
                continue
            if any(overlap_ratio(row, old) > MAX_OVERLAP for old in selected):
                continue
            item = dict(row); item["selection_reason"] = reason; selected.append(item)
            selected_ids.add(str(row["candidate_id"])); return

    for direction in ("long", "short"):
        take(lambda row, d=direction: row["selected_side"] == d, f"direction_seed:{direction}")
    for klass in ("micro_1_15m", "intrahour_30_60m", "session_2_6h", "intraday_12_24h", "multiday_2_30d"):
        take(lambda row, h=klass: row["horizon_class"] == h, f"horizon_seed:{klass}")
    for row in ranked:
        if len(selected) >= limit:
            break
        if str(row["candidate_id"]) in selected_ids or any(overlap_ratio(row, old) > MAX_OVERLAP for old in selected):
            continue
        item = dict(row); item["selection_reason"] = "ranked_low_overlap"; selected.append(item)
        selected_ids.add(str(row["candidate_id"]))
    if len(selected) < limit:
        for row in ranked:
            if len(selected) >= limit:
                break
            if str(row["candidate_id"]) in selected_ids:
                continue
            item = dict(row); item["selection_reason"] = "overlap_relaxed_fill"; selected.append(item)
            selected_ids.add(str(row["candidate_id"]))
    return selected


CASE_COLUMNS = [
    "case_id", "contract_id", "candidate_id", "instrument", "base_currency", "quote_currency",
    "entry_utc", "exit_utc", "entry_epoch", "exit_epoch", "horizon_min", "horizon_class",
    "selected_side", "signed_currency_factor", "market_episode_id", "selection_tier",
    "selection_reason", "selection_rank", "selection_score", "entry_spread_pips", "after_cost_pips",
    "gross_cost_multiple", "mfe_pips", "mae_pips", "path_efficiency", "pre_entry_factor_links",
    "during_move_factor_links", "late_or_ex_post_factor_links", "research_only", "execution_eligible",
]


def ensure_schema(database: sqlite3.Connection) -> None:
    database.executescript(
        """
        CREATE TABLE IF NOT EXISTS representative_case_contracts (
          contract_id TEXT PRIMARY KEY, build_contract_id TEXT NOT NULL, created_utc TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL, contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS representative_cases (
          case_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, candidate_id TEXT NOT NULL,
          instrument TEXT NOT NULL, base_currency TEXT NOT NULL, quote_currency TEXT NOT NULL,
          entry_utc TEXT NOT NULL, exit_utc TEXT NOT NULL, entry_epoch INTEGER NOT NULL, exit_epoch INTEGER NOT NULL,
          horizon_min INTEGER NOT NULL, horizon_class TEXT NOT NULL, selected_side TEXT NOT NULL,
          signed_currency_factor TEXT NOT NULL, market_episode_id TEXT NOT NULL, selection_tier TEXT NOT NULL,
          selection_reason TEXT NOT NULL, selection_rank INTEGER NOT NULL, selection_score REAL NOT NULL,
          entry_spread_pips REAL NOT NULL, after_cost_pips REAL NOT NULL, gross_cost_multiple REAL NOT NULL,
          mfe_pips REAL NOT NULL, mae_pips REAL NOT NULL, path_efficiency REAL NOT NULL,
          pre_entry_factor_links INTEGER NOT NULL, during_move_factor_links INTEGER NOT NULL,
          late_or_ex_post_factor_links INTEGER NOT NULL, research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          UNIQUE(contract_id,candidate_id), FOREIGN KEY(contract_id) REFERENCES representative_case_contracts(contract_id)
        );
        CREATE TRIGGER IF NOT EXISTS representative_contract_no_update
          BEFORE UPDATE ON representative_case_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS representative_contract_no_delete
          BEFORE DELETE ON representative_case_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS representative_case_no_update
          BEFORE UPDATE ON representative_cases BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS representative_case_no_delete
          BEFORE DELETE ON representative_cases BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def load_candidates(database: sqlite3.Connection) -> list[dict[str, Any]]:
    database.row_factory = sqlite3.Row
    links: dict[str, dict[str, int]] = defaultdict(lambda: {"pre":0,"during":0,"late":0})
    for candidate_id, relation, count in database.execute(
        "SELECT candidate_id,relation,COUNT(*) FROM movement_factor_links GROUP BY candidate_id,relation"
    ):
        if relation == "pre_entry_causal": links[candidate_id]["pre"] += int(count)
        elif relation == "during_move_causal": links[candidate_id]["during"] += int(count)
        else: links[candidate_id]["late"] += int(count)
    rows = []
    for record in database.execute(
        "SELECT * FROM movement_candidates WHERE build_contract_id=? ORDER BY instrument,entry_epoch,candidate_id",
        (BUILD_CONTRACT_ID,),
    ):
        row = dict(record); counts = links[row["candidate_id"]]
        row["pre_entry_factor_links"] = counts["pre"]
        row["during_move_factor_links"] = counts["during"]
        row["late_or_ex_post_factor_links"] = counts["late"]
        rows.append(row)
    return rows


def build(database_path: Path = DATABASE) -> dict[str, Any]:
    builder_sha = file_sha256(Path(__file__).resolve())
    database = sqlite3.connect(database_path, timeout=120)
    database.execute("PRAGMA journal_mode=WAL")
    database.execute("PRAGMA synchronous=FULL")
    database.execute("PRAGMA foreign_keys=ON")
    ensure_schema(database)
    existing = database.execute(
        "SELECT builder_sha256 FROM representative_case_contracts WHERE contract_id=?", (CONTRACT_ID,)
    ).fetchone()
    if existing:
        database.close()
        if existing[0] != builder_sha: raise RuntimeError("immutable_representative_contract_collision")
        return snapshot(database_path, reused=True)
    candidates = load_candidates(database)
    by_pair: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidates: by_pair[row["instrument"]].append(row)
    cases = []
    for instrument in sorted(by_pair):
        selected = select_pair_cases(by_pair[instrument])
        for rank, row in enumerate(selected, 1):
            row.update(
                {
                    "case_id": stable_id("representative_case", CONTRACT_ID, row["candidate_id"]),
                    "contract_id": CONTRACT_ID, "selection_rank": rank,
                    "research_only": 1, "execution_eligible": 0,
                }
            )
            cases.append(row)
    contract = {
        "contract_id":CONTRACT_ID,"build_contract_id":BUILD_CONTRACT_ID,"created_utc":utc_now(),
        "builder_sha256":builder_sha,
        "contract_json":canonical_json({"top_per_pair":TOP_PER_PAIR,"maximum_overlap_ratio":MAX_OVERLAP,
                                         "forced_directions":["long","short"],"forced_horizon_classes":5,
                                         "research_only":True,"execution_eligible":False}),
    }
    marks = ",".join("?" for _ in CASE_COLUMNS)
    with database:
        database.execute("INSERT INTO representative_case_contracts VALUES (?,?,?,?,?)", tuple(contract.values()))
        database.executemany(
            f"INSERT INTO representative_cases ({','.join(CASE_COLUMNS)}) VALUES ({marks})",
            [[row.get(column) for column in CASE_COLUMNS] for row in cases],
        )
    database.close()
    return snapshot(database_path, reused=False)


def load_case_rows(database_path: Path) -> list[dict[str, Any]]:
    database = sqlite3.connect(f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True)
    database.row_factory = sqlite3.Row
    rows = [dict(row) for row in database.execute(
        "SELECT * FROM representative_cases WHERE contract_id=? ORDER BY instrument,selection_rank", (CONTRACT_ID,)
    )]
    database.close(); return rows


def snapshot(database_path: Path, *, reused: bool) -> dict[str, Any]:
    rows = load_case_rows(database_path)
    coverage = defaultdict(set)
    linked = 0
    for row in rows:
        coverage[row["instrument"]].add(row["horizon_class"])
        linked += int(row["pre_entry_factor_links"] > 0)
    result = {
        "schema_version":1,"generated_utc":utc_now(),"contract_id":CONTRACT_ID,
        "case_count":len(rows),"instrument_count":len(coverage),
        "all_five_horizon_class_pair_count":sum(len(values)==5 for values in coverage.values()),
        "pre_entry_factor_linked_case_count":linked,"reused_existing_immutable_cohort":reused,
        "evidence_class":"outcome_selected_research_queue_not_forecast_proof",
        "research_only":True,"execution_eligible":False,"supported_execution_decision":"no_trade",
    }
    result["snapshot_sha256"] = sha256_bytes(canonical_json(result).encode("utf-8")); return result


def write_exports(database_path: Path, output_csv: Path, output_md: Path) -> None:
    rows = load_case_rows(database_path)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CASE_COLUMNS); writer.writeheader(); writer.writerows(rows)
    lines = [
        "# API-Free Spike/Blurb Research Chunks V1", "",
        "Each case was selected from price before source research. Results are known and must not be used as entry-time features. Trace the purest official factor, preserve exact knowledge time, and classify unmatched cases explicitly.", "",
    ]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows: grouped[row["instrument"]].append(row)
    for instrument in sorted(grouped):
        lines.extend([f"## {instrument}", "", "| Rank | Candidate | UTC window | Horizon | Side | Net | Cost x | Path | Pre-entry links |", "|---:|---|---|---:|---|---:|---:|---:|---:|"])
        for row in grouped[instrument]:
            lines.append(
                f"| {row['selection_rank']} | `{row['candidate_id']}` | {row['entry_utc']} → {row['exit_utc']} | {row['horizon_min']}m | {row['selected_side']} | {row['after_cost_pips']:+.2f}p | {row['gross_cost_multiple']:.2f} | {row['path_efficiency']:.2f} | {row['pre_entry_factor_links']} |"
            )
        lines.append("")
    atomic_text(output_md, "\n".join(lines))


def parse_args() -> argparse.Namespace:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database",type=Path,default=DATABASE); parser.add_argument("--output-json",type=Path,default=OUTPUT_JSON)
    parser.add_argument("--output-csv",type=Path,default=OUTPUT_CSV); parser.add_argument("--output-md",type=Path,default=OUTPUT_MD)
    return parser.parse_args()


def main() -> int:
    args=parse_args(); report=build(args.database); write_exports(args.database,args.output_csv,args.output_md)
    atomic_text(args.output_json,json.dumps(report,indent=2,sort_keys=True)+"\n"); print(json.dumps(report,indent=2,sort_keys=True)); return 0


if __name__=="__main__": raise SystemExit(main())
