#!/usr/bin/env python3
"""Join restored legacy executable paths to their retained attribution labels."""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from oanda_spike_blurb_factor_reconstruction import (
    LEGACY_TAGS,
    REPORT_ROOT,
    atomic_text,
    canonical_json,
    file_sha256,
    sha256_bytes,
    utc_now,
)
from oanda_spike_blurb_legacy_price_reacquisition import CONTRACT_ID as PRICE_CONTRACT_ID
from oanda_spike_blurb_movement_inventory import DATABASE, stable_id


CONTRACT_ID = "spike_blurb_legacy_attribution_ledger_v1_20260819"
OUTPUT_JSON = REPORT_ROOT / "SPIKE_BLURB_LEGACY_ATTRIBUTION_LEDGER_V1.json"
OUTPUT_MD = REPORT_ROOT / "SPIKE_BLURB_LEGACY_ATTRIBUTION_LEDGER_V1.md"


def truth(value: Any) -> int:
    return int(str(value or "").strip().lower() in {"1", "true", "yes", "y"})


def causal_use_state(relation: str) -> str:
    return {
        "PRE_MOVE": "candidate_pre_entry_legacy_unverified",
        "FIRST_WAVE_CONFIRMATION": "early_move_confirmation_not_entry",
        "POST_HOC_EXPLANATION": "ex_post_attribution_only",
        "STALE_CONTEXT": "stale_context_only",
    }.get(str(relation or "").strip().upper(), "unmatched_or_unknown")


def expected_side(value: Any) -> str | None:
    normalized = str(value or "").strip().upper()
    return normalized.lower() if normalized in {"LONG", "SHORT"} else None


def load_tags(path: Path) -> dict[str, dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return {str(row.get("move_id") or row.get("movement_key")): dict(row) for row in csv.DictReader(handle)}


COLUMNS = [
    "attribution_id", "contract_id", "move_id", "instrument", "start_utc", "end_utc",
    "price_coverage_state", "candle_count", "restored_best_direction", "restored_best_net_pips",
    "average_spread_pips", "news_match_status", "news_tag_count", "primary_event_id",
    "primary_event_utc", "primary_headline", "primary_category", "primary_source_url",
    "primary_source_domain", "primary_source_verified", "primary_pair_relevance", "primary_match_confidence",
    "primary_temporal_relation", "primary_causal_relation", "causal_use_state",
    "event_to_move_lead_minutes", "availability_to_move_lead_minutes", "legacy_predictive_flag",
    "legacy_expected_side", "expected_side_matches_restored", "raw_event_links_json",
    "outcome_selected", "forecast_proof_eligible", "research_only", "execution_eligible",
]


def ensure_schema(database: sqlite3.Connection) -> None:
    database.executescript(
        """
        CREATE TABLE IF NOT EXISTS legacy_attribution_contracts (
          contract_id TEXT PRIMARY KEY, price_contract_id TEXT NOT NULL, created_utc TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL, tag_file_sha256 TEXT NOT NULL, contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS legacy_attribution_cases (
          attribution_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, move_id TEXT NOT NULL,
          instrument TEXT NOT NULL, start_utc TEXT NOT NULL, end_utc TEXT NOT NULL,
          price_coverage_state TEXT NOT NULL, candle_count INTEGER NOT NULL,
          restored_best_direction TEXT, restored_best_net_pips REAL, average_spread_pips REAL,
          news_match_status TEXT NOT NULL, news_tag_count INTEGER NOT NULL, primary_event_id TEXT,
          primary_event_utc TEXT, primary_headline TEXT, primary_category TEXT, primary_source_url TEXT,
          primary_source_domain TEXT, primary_source_verified INTEGER NOT NULL, primary_pair_relevance REAL,
          primary_match_confidence REAL, primary_temporal_relation TEXT, primary_causal_relation TEXT,
          causal_use_state TEXT NOT NULL, event_to_move_lead_minutes REAL,
          availability_to_move_lead_minutes REAL, legacy_predictive_flag INTEGER NOT NULL,
          legacy_expected_side TEXT, expected_side_matches_restored INTEGER, raw_event_links_json TEXT NOT NULL,
          outcome_selected INTEGER NOT NULL CHECK(outcome_selected=1),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          UNIQUE(contract_id,move_id), FOREIGN KEY(contract_id) REFERENCES legacy_attribution_contracts(contract_id)
        );
        CREATE INDEX IF NOT EXISTS legacy_attribution_state
          ON legacy_attribution_cases(contract_id,causal_use_state,price_coverage_state,instrument);
        CREATE TRIGGER IF NOT EXISTS legacy_attribution_contract_no_update
          BEFORE UPDATE ON legacy_attribution_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS legacy_attribution_contract_no_delete
          BEFORE DELETE ON legacy_attribution_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS legacy_attribution_case_no_update
          BEFORE UPDATE ON legacy_attribution_cases BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS legacy_attribution_case_no_delete
          BEFORE DELETE ON legacy_attribution_cases BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def case_row(tag: Mapping[str, Any], price: Mapping[str, Any]) -> dict[str, Any]:
    side = expected_side(tag.get("primary_expected_pair_direction"))
    restored = price.get("best_direction")
    source_url = str(tag.get("primary_source_url") or "")
    try:
        source_domain = urlsplit(source_url).hostname or None
    except ValueError:
        source_domain = None
    return {
        "attribution_id":stable_id("legacy_attribution",CONTRACT_ID,tag["move_id"]),
        "contract_id":CONTRACT_ID,"move_id":tag["move_id"],"instrument":tag["instrument"],
        "start_utc":tag["start_utc"],"end_utc":tag["end_utc"],
        "price_coverage_state":price["coverage_state"],"candle_count":int(price["candle_count"]),
        "restored_best_direction":restored,"restored_best_net_pips":price.get("best_net_pips"),
        "average_spread_pips":price.get("average_spread_pips"),
        "news_match_status":str(tag.get("news_match_status") or "unknown"),
        "news_tag_count":int(float(tag.get("news_tag_count") or 0)),
        "primary_event_id":tag.get("primary_event_id") or None,"primary_event_utc":tag.get("primary_event_utc") or None,
        "primary_headline":tag.get("primary_headline") or None,"primary_category":tag.get("primary_category") or None,
        "primary_source_url":source_url or None,"primary_source_domain":source_domain,
        "primary_source_verified":truth(tag.get("primary_source_verified")),
        "primary_pair_relevance":float(tag["primary_pair_relevance"]) if tag.get("primary_pair_relevance") else None,
        "primary_match_confidence":float(tag["primary_match_confidence"]) if tag.get("primary_match_confidence") else None,
        "primary_temporal_relation":tag.get("primary_temporal_relation") or None,
        "primary_causal_relation":tag.get("primary_causal_relation") or None,
        "causal_use_state":causal_use_state(str(tag.get("primary_causal_relation") or "")),
        "event_to_move_lead_minutes":float(tag["primary_event_to_move_lead_minutes"]) if tag.get("primary_event_to_move_lead_minutes") else None,
        "availability_to_move_lead_minutes":float(tag["primary_availability_to_move_lead_minutes"]) if tag.get("primary_availability_to_move_lead_minutes") else None,
        "legacy_predictive_flag":truth(tag.get("primary_predictive_eligible")),"legacy_expected_side":side,
        "expected_side_matches_restored":int(side==restored) if side and restored else None,
        "raw_event_links_json":str(tag.get("event_links_json") or "[]"),
        "outcome_selected":1,"forecast_proof_eligible":0,"research_only":1,"execution_eligible":0,
    }


def build(tags_path: Path = LEGACY_TAGS, database_path: Path = DATABASE) -> dict[str, Any]:
    builder_sha=file_sha256(Path(__file__).resolve()); tag_sha=file_sha256(tags_path); tags=load_tags(tags_path)
    database=sqlite3.connect(database_path,timeout=120); database.row_factory=sqlite3.Row
    database.execute("PRAGMA journal_mode=WAL"); database.execute("PRAGMA synchronous=FULL"); database.execute("PRAGMA foreign_keys=ON"); ensure_schema(database)
    existing=database.execute("SELECT builder_sha256,tag_file_sha256 FROM legacy_attribution_contracts WHERE contract_id=?",(CONTRACT_ID,)).fetchone()
    if existing:
        database.close()
        if tuple(existing)!=(builder_sha,tag_sha): raise RuntimeError("immutable_legacy_attribution_contract_collision")
        return snapshot(database_path,reused=True)
    prices={row["move_id"]:dict(row) for row in database.execute("SELECT * FROM legacy_executable_price_windows WHERE contract_id=?",(PRICE_CONTRACT_ID,))}
    if len(prices) < len(tags):
        database.close(); raise RuntimeError(f"legacy_price_recovery_incomplete:{len(prices)}:{len(tags)}")
    rows=[case_row({**tag,"move_id":move_id},prices[move_id]) for move_id,tag in sorted(tags.items()) if move_id in prices]
    contract={"contract_id":CONTRACT_ID,"price_contract_id":PRICE_CONTRACT_ID,"created_utc":utc_now(),"builder_sha256":builder_sha,"tag_file_sha256":tag_sha,
              "contract_json":canonical_json({"legacy_rows":len(tags),"causal_states_preserved":True,"outcome_selected":True,"forecast_proof_eligible":False,"execution_eligible":False})}
    marks=",".join("?" for _ in COLUMNS)
    with database:
        database.execute("INSERT INTO legacy_attribution_contracts VALUES (?,?,?,?,?,?)",tuple(contract.values()))
        database.executemany(f"INSERT INTO legacy_attribution_cases ({','.join(COLUMNS)}) VALUES ({marks})",[[row.get(col) for col in COLUMNS] for row in rows])
    database.close(); return snapshot(database_path,reused=False)


def snapshot(database_path: Path, *, reused: bool) -> dict[str, Any]:
    database=sqlite3.connect(f"file:{database_path.resolve().as_posix()}?mode=ro",uri=True)
    states=dict(database.execute("SELECT causal_use_state,COUNT(*) FROM legacy_attribution_cases WHERE contract_id=? GROUP BY causal_use_state",(CONTRACT_ID,)).fetchall())
    coverage=dict(database.execute("SELECT price_coverage_state,COUNT(*) FROM legacy_attribution_cases WHERE contract_id=? GROUP BY price_coverage_state",(CONTRACT_ID,)).fetchall())
    total,matched,directional,correct=database.execute("""SELECT COUNT(*),SUM(news_match_status='matched'),SUM(expected_side_matches_restored IS NOT NULL),SUM(expected_side_matches_restored=1) FROM legacy_attribution_cases WHERE contract_id=?""",(CONTRACT_ID,)).fetchone()
    domains=[{"domain":row[0],"n":int(row[1])} for row in database.execute("""SELECT primary_source_domain,COUNT(*) FROM legacy_attribution_cases WHERE contract_id=? AND primary_source_domain IS NOT NULL GROUP BY primary_source_domain ORDER BY COUNT(*) DESC,primary_source_domain LIMIT 15""",(CONTRACT_ID,))]
    directional_by_state=[{"causal_use_state":row[0],"n":int(row[1]),"direction_alignment_rate":float(row[2]) if row[2] is not None else None} for row in database.execute("""SELECT causal_use_state,COUNT(*),AVG(expected_side_matches_restored) FROM legacy_attribution_cases WHERE contract_id=? AND expected_side_matches_restored IS NOT NULL GROUP BY causal_use_state ORDER BY causal_use_state""",(CONTRACT_ID,))]
    integrity=str(database.execute("PRAGMA integrity_check").fetchone()[0]); database.close()
    result={"schema_version":1,"generated_utc":utc_now(),"contract_id":CONTRACT_ID,"case_count":int(total),"news_matched_count":int(matched or 0),
            "causal_use_states":states,"price_coverage_states":coverage,"legacy_directional_mapping_count":int(directional or 0),
            "legacy_direction_alignment_rate":(int(correct or 0)/int(directional)) if directional else None,"directional_alignment_by_causal_state":directional_by_state,"top_source_domains":domains,"sqlite_integrity":integrity,"reused_existing_immutable_cohort":reused,
            "evidence_class":"outcome_selected_legacy_attribution_not_forecast_proof","research_only":True,"execution_eligible":False,"supported_execution_decision":"no_trade"}
    result["snapshot_sha256"]=sha256_bytes(canonical_json(result).encode("utf-8")); return result


def render_report(report: Mapping[str, Any]) -> str:
    return "\n".join(["# Legacy Spike/Blurb Attribution Ledger V1","",f"- Generated: `{report['generated_utc']}`",f"- Contract: `{report['contract_id']}`",
        f"- Cases: **{report['case_count']:,}**; news matched: **{report['news_matched_count']:,}**",f"- Price coverage: `{canonical_json(report['price_coverage_states'])}`",
        f"- Causal-use states: `{canonical_json(report['causal_use_states'])}`",f"- Retained directional mappings: **{report['legacy_directional_mapping_count']:,}**",
        "","Direction agreement is descriptive only because labels and articles were assembled after selecting the moves. No row is forecast-proof or execution-eligible.",""])


def parse_args()->argparse.Namespace:
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--tags",type=Path,default=LEGACY_TAGS);p.add_argument("--database",type=Path,default=DATABASE);p.add_argument("--output-json",type=Path,default=OUTPUT_JSON);p.add_argument("--output-md",type=Path,default=OUTPUT_MD);return p.parse_args()


def main()->int:
    a=parse_args();r=build(a.tags,a.database);atomic_text(a.output_json,json.dumps(r,indent=2,sort_keys=True)+"\n");atomic_text(a.output_md,render_report(r));print(json.dumps(r,indent=2,sort_keys=True));return 0


if __name__=="__main__":raise SystemExit(main())
