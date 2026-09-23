#!/usr/bin/env python3
"""Append-only prospective move-first case ledger.

The legacy V2 audit is a useful mutable historical diagnostic.  It is not a
proof ledger: rebuilding it can change its rows as source mappings improve.
This worker starts after the V152 semantic cutover and inserts each independent
factor episode exactly once.  A later change to an inserted case is an
append-only violation, not an update.

Research only.  This module cannot promote, authorize, or place an order.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

import oanda_major_move_gap_census as census


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "move_first_news_case_cohort_v3"
)
DEFAULT_CONFIG = ROOT / "config" / "move_first_news_case_cohort_v3_20260901.json"
DEFAULT_CASE_INPUT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "move_first_news_case_audit"
    / "MOVE_FIRST_NEWS_CASE_AUDIT_DETAIL_CURRENT.csv"
)
DEFAULT_MOVE_INPUT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "major_move_gap_census"
    / "MAJOR_MOVE_GAP_CENSUS_DETAIL_CURRENT.csv"
)
DEFAULT_DATABASE = STATE / "move_first_news_case_cohort_v3_20260901.sqlite"
DEFAULT_REPORT = REPORT_ROOT / "MOVE_FIRST_NEWS_CASE_COHORT_CURRENT.json"


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp lacks timezone: {value!r}")
    return parsed.astimezone(dt.timezone.utc)


def load_config(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "schema_version",
        "cohort_id",
        "cohort_start_utc",
        "source_classifier_contract_id",
        "pair_direction_contract_id",
        "factor_episode_contract_id",
    }
    missing = sorted(required - set(value))
    if missing:
        raise ValueError(f"config missing required fields: {missing}")
    parse_utc(str(value["cohort_start_utc"]))
    if int(value.get("historical_rows_imported", -1)) != 0:
        raise ValueError("prospective cohort must import zero historical rows")
    if value.get("execution_eligible") is not False or value.get("can_place_orders") is not False:
        raise ValueError("prospective case cohort must remain nonexecuting")
    return value


def manifest(config: Mapping[str, Any], config_path: Path) -> dict[str, str]:
    return {
        "schema_version": "move_first_news_case_ledger_v1",
        "cohort_id": str(config["cohort_id"]),
        "cohort_start_utc": str(config["cohort_start_utc"]),
        "config_sha256": file_sha256(config_path),
        "source_classifier_contract_id": str(config["source_classifier_contract_id"]),
        "pair_direction_contract_id": str(config["pair_direction_contract_id"]),
        "factor_episode_contract_id": str(config["factor_episode_contract_id"]),
        "historical_rows_imported": "0",
        "execution_eligible": "false",
    }


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS manifest (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS cases (
            case_id TEXT PRIMARY KEY,
            factor_episode_id TEXT NOT NULL UNIQUE,
            representative_move_id TEXT NOT NULL,
            start_utc TEXT NOT NULL,
            case_json TEXT NOT NULL,
            case_sha256 TEXT NOT NULL,
            raw_pair_paths_json TEXT NOT NULL,
            raw_pair_paths_sha256 TEXT NOT NULL,
            inserted_utc TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_cases_start_utc ON cases(start_utc);
        """
    )
    return connection


def bind_manifest(connection: sqlite3.Connection, expected: Mapping[str, str]) -> None:
    observed = {
        str(row["key"]): str(row["value"])
        for row in connection.execute("SELECT key, value FROM manifest")
    }
    if observed and observed != dict(expected):
        raise RuntimeError(
            "cohort manifest mismatch; material changes require a new cohort/database"
        )
    if not observed:
        connection.executemany(
            "INSERT INTO manifest(key, value) VALUES (?, ?)",
            sorted(expected.items()),
        )
        connection.commit()


RAW_PATH_FIELDS = (
    "move_id",
    "factor_episode_id",
    "factor_representative",
    "instrument",
    "start_utc",
    "end_utc",
    "horizon_min",
    "selected_side_label",
    "gross_magnitude_pips",
    "endpoint_after_cost_pips",
    "modeled_cost_pips",
    "liquidity_bucket",
)


def load_raw_pair_paths(path: Path, start: dt.datetime) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    if not path.exists():
        return grouped
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            episode = str(row.get("factor_episode_id") or "").strip()
            clock = str(row.get("start_utc") or "").strip()
            if not episode or not clock:
                continue
            try:
                if parse_utc(clock) < start:
                    continue
            except ValueError:
                continue
            grouped[episode].append({key: str(row.get(key) or "") for key in RAW_PATH_FIELDS})
    for rows in grouped.values():
        rows.sort(key=lambda row: (row["instrument"], row["move_id"]))
    return grouped


def candidate_rows(
    case_input: Path,
    move_input: Path,
    cohort_start: dt.datetime,
) -> Iterable[tuple[dict[str, str], list[dict[str, str]]]]:
    raw_paths = load_raw_pair_paths(move_input, cohort_start)
    if not case_input.exists():
        return []
    output: list[tuple[dict[str, str], list[dict[str, str]]]] = []
    with case_input.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            episode = str(row.get("factor_episode_id") or "").strip()
            move_id = str(row.get("move_id") or "").strip()
            clock = str(row.get("start_utc") or "").strip()
            if not episode or not move_id or not clock:
                continue
            try:
                if parse_utc(clock) < cohort_start:
                    continue
            except ValueError:
                continue
            output.append((dict(row), raw_paths.get(episode, [])))
    output.sort(key=lambda item: (parse_utc(item[0]["start_utc"]), item[0]["factor_episode_id"]))
    return output


def insert_candidates(
    connection: sqlite3.Connection,
    cohort_id: str,
    rows: Iterable[tuple[Mapping[str, Any], list[dict[str, str]]]],
    inserted_utc: str,
) -> int:
    inserted = 0
    connection.execute("BEGIN IMMEDIATE")
    try:
        for row, raw_paths in rows:
            episode = str(row["factor_episode_id"])
            case_id = sha256_text(f"{cohort_id}|{episode}")
            case_json = canonical_json(dict(row))
            paths_json = canonical_json(raw_paths)
            case_hash = sha256_text(case_json)
            paths_hash = sha256_text(paths_json)
            existing = connection.execute(
                "SELECT case_sha256, raw_pair_paths_sha256 FROM cases WHERE factor_episode_id = ?",
                (episode,),
            ).fetchone()
            if existing:
                if (
                    str(existing["case_sha256"]) != case_hash
                    or str(existing["raw_pair_paths_sha256"]) != paths_hash
                ):
                    raise RuntimeError(
                        f"append-only violation for factor episode {episode}; open a new cohort"
                    )
                continue
            connection.execute(
                """
                INSERT INTO cases(
                    case_id, factor_episode_id, representative_move_id, start_utc,
                    case_json, case_sha256, raw_pair_paths_json,
                    raw_pair_paths_sha256, inserted_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    case_id,
                    episode,
                    str(row["move_id"]),
                    str(row["start_utc"]),
                    case_json,
                    case_hash,
                    paths_json,
                    paths_hash,
                    inserted_utc,
                ),
            )
            inserted += 1
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return inserted


def build_report(
    connection: sqlite3.Connection,
    expected_manifest: Mapping[str, str],
    inserted: int,
) -> dict[str, Any]:
    aggregate = connection.execute(
        """
        SELECT COUNT(*) AS case_count, MIN(start_utc) AS first_start_utc,
               MAX(start_utc) AS last_start_utc,
               COALESCE(SUM(json_array_length(raw_pair_paths_json)), 0) AS raw_pair_path_count
        FROM cases
        """
    ).fetchone()
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    return {
        **dict(expected_manifest),
        "schema_version": "move_first_news_case_cohort_report_v1",
        "ledger_schema_version": str(expected_manifest["schema_version"]),
        "generated_utc": utc_now(),
        "case_count": int(aggregate["case_count"]),
        "raw_pair_path_count": int(aggregate["raw_pair_path_count"]),
        "first_start_utc": aggregate["first_start_utc"],
        "last_start_utc": aggregate["last_start_utc"],
        "inserted_this_cycle": int(inserted),
        "sqlite_integrity": integrity,
        "append_only": True,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
    }


def run_once(
    config_path: Path = DEFAULT_CONFIG,
    case_input: Path = DEFAULT_CASE_INPUT,
    move_input: Path = DEFAULT_MOVE_INPUT,
    database: Path = DEFAULT_DATABASE,
    report: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    config = load_config(config_path)
    expected = manifest(config, config_path)
    connection = open_database(database)
    try:
        bind_manifest(connection, expected)
        now = utc_now()
        inserted = insert_candidates(
            connection,
            str(config["cohort_id"]),
            candidate_rows(case_input, move_input, parse_utc(str(config["cohort_start_utc"]))),
            now,
        )
        payload = build_report(connection, expected, inserted)
    finally:
        connection.close()
    census.atomic_text(report, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload


def verify(
    config_path: Path = DEFAULT_CONFIG,
    database: Path = DEFAULT_DATABASE,
    report: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    failures: list[str] = []
    config = load_config(config_path)
    expected = manifest(config, config_path)
    if not database.exists():
        failures.append("database_missing")
        return {"verified": False, "failures": failures}
    connection = sqlite3.connect(f"file:{database.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity != "ok":
            failures.append(f"sqlite_integrity:{integrity}")
        observed = {
            str(row["key"]): str(row["value"])
            for row in connection.execute("SELECT key, value FROM manifest")
        }
        if observed != expected:
            failures.append("manifest_mismatch")
        rows = list(connection.execute("SELECT * FROM cases ORDER BY start_utc, case_id"))
        seen: set[str] = set()
        for row in rows:
            episode = str(row["factor_episode_id"])
            if episode in seen:
                failures.append(f"duplicate_episode:{episode}")
            seen.add(episode)
            if str(row["case_id"]) != sha256_text(f"{config['cohort_id']}|{episode}"):
                failures.append(f"case_id_mismatch:{episode}")
            if str(row["case_sha256"]) != sha256_text(str(row["case_json"])):
                failures.append(f"case_hash_mismatch:{episode}")
            if str(row["raw_pair_paths_sha256"]) != sha256_text(str(row["raw_pair_paths_json"])):
                failures.append(f"path_hash_mismatch:{episode}")
        if report.exists():
            payload = json.loads(report.read_text(encoding="utf-8"))
            if int(payload.get("case_count", -1)) != len(rows):
                failures.append("report_case_count_mismatch")
            if payload.get("execution_decision") != "no_trade":
                failures.append("report_execution_decision_not_no_trade")
        else:
            failures.append("report_missing")
    finally:
        connection.close()
    return {
        "verified": not failures,
        "failures": failures,
        "cohort_id": str(config["cohort_id"]),
        "database": str(database),
        "checked_utc": utc_now(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--case-input", type=Path, default=DEFAULT_CASE_INPUT)
    parser.add_argument("--move-input", type=Path, default=DEFAULT_MOVE_INPUT)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    args = parser.parse_args()
    if args.verify_only:
        payload = verify(args.config, args.database, args.report)
        print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
        return 0 if payload["verified"] else 1
    stop = time.monotonic() + max(0.0, float(args.duration_sec))
    while True:
        payload = run_once(
            args.config, args.case_input, args.move_input, args.database, args.report
        )
        print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
        if float(args.interval_sec) <= 0.0 or time.monotonic() >= stop:
            return 0
        time.sleep(min(float(args.interval_sec), max(0.0, stop - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
