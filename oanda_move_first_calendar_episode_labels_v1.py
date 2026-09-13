#!/usr/bin/env python3
"""Append-only prospective calendar labels for immutable move-first cases.

This is a side ledger.  It never mutates the frozen V3 case cohort and imports
no case whose move began before this label contract was activated.  Calendar
membership is determined solely from the case start clock and a predeclared
weekday calendar; profit, direction, news, and later market behavior are never
inputs.  Research only: this module cannot promote or place an order.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import oanda_major_move_gap_census as census


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORT_ROOT = (
    ROOT / "data" / "oanda_training_manager" / "reports"
    / "move_first_calendar_episode_labels_v1"
)
DEFAULT_CONFIG = ROOT / "config" / "move_first_calendar_episode_labels_v1_20260901.json"
DEFAULT_SOURCE_DATABASE = STATE / "move_first_news_case_cohort_v3_20260901.sqlite"
DEFAULT_DATABASE = STATE / "move_first_calendar_episode_labels_v1_20260901.sqlite"
DEFAULT_REPORT = REPORT_ROOT / "MOVE_FIRST_CALENDAR_EPISODE_LABELS_CURRENT.json"
UTC = dt.timezone.utc


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
    return dt.datetime.now(UTC).isoformat().replace("+00:00", "Z")


def parse_utc(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp lacks timezone: {value!r}")
    return parsed.astimezone(UTC)


def load_config(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "cohort_id", "cohort_start_utc", "source_case_cohort_id",
        "calendar_timezone", "calendar_contract_id",
        "month_end_definition", "quarter_end_definition",
    }
    missing = sorted(required - set(value))
    if missing:
        raise ValueError(f"calendar label config missing fields: {missing}")
    parse_utc(str(value["cohort_start_utc"]))
    ZoneInfo(str(value["calendar_timezone"]))
    if int(value.get("historical_rows_imported", -1)) != 0:
        raise ValueError("calendar label cohort must import zero historical rows")
    if value.get("execution_eligible") is not False or value.get("can_place_orders") is not False:
        raise ValueError("calendar label cohort must remain nonexecuting")
    if value["month_end_definition"] != "last_weekday_of_calendar_month":
        raise ValueError("unsupported month-end definition")
    if value["quarter_end_definition"] != "last_weekday_of_quarter_end_month":
        raise ValueError("unsupported quarter-end definition")
    return value


def expected_manifest(config: Mapping[str, Any], config_path: Path) -> dict[str, str]:
    return {
        "schema_version": "move_first_calendar_episode_label_ledger_v1",
        "cohort_id": str(config["cohort_id"]),
        "cohort_start_utc": str(config["cohort_start_utc"]),
        "source_case_cohort_id": str(config["source_case_cohort_id"]),
        "calendar_timezone": str(config["calendar_timezone"]),
        "calendar_contract_id": str(config["calendar_contract_id"]),
        "month_end_definition": str(config["month_end_definition"]),
        "quarter_end_definition": str(config["quarter_end_definition"]),
        "config_sha256": file_sha256(config_path),
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
        CREATE TABLE IF NOT EXISTS labels (
            label_id TEXT PRIMARY KEY,
            source_case_id TEXT NOT NULL UNIQUE,
            factor_episode_id TEXT NOT NULL UNIQUE,
            start_utc TEXT NOT NULL,
            label_json TEXT NOT NULL,
            label_sha256 TEXT NOT NULL,
            inserted_utc TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_calendar_labels_start ON labels(start_utc);
        """
    )
    return connection


def bind_manifest(connection: sqlite3.Connection, expected: Mapping[str, str]) -> None:
    observed = {
        str(row["key"]): str(row["value"])
        for row in connection.execute("SELECT key, value FROM manifest")
    }
    if observed and observed != dict(expected):
        raise RuntimeError("calendar label manifest mismatch; open a new cohort")
    if not observed:
        connection.executemany(
            "INSERT INTO manifest(key, value) VALUES (?, ?)", sorted(expected.items())
        )
        connection.commit()


def last_weekday(year: int, month: int) -> dt.date:
    next_month = (
        dt.date(year + 1, 1, 1)
        if month == 12
        else dt.date(year, month + 1, 1)
    )
    result = next_month - dt.timedelta(days=1)
    while result.weekday() >= 5:
        result -= dt.timedelta(days=1)
    return result


def build_label(start_utc: str, config: Mapping[str, Any]) -> dict[str, Any]:
    clock = parse_utc(start_utc)
    local = clock.astimezone(ZoneInfo(str(config["calendar_timezone"])))
    month_end_date = last_weekday(local.year, local.month)
    is_month_end = local.date() == month_end_date
    is_quarter_end = is_month_end and local.month in {3, 6, 9, 12}
    labels = ["ordinary"]
    if is_month_end:
        labels.append("month_end")
    if is_quarter_end:
        labels.append("quarter_end")
    return {
        "calendar_episode_labels": labels,
        "calendar_label_known_before_move": True,
        "calendar_contract_id": str(config["calendar_contract_id"]),
        "calendar_timezone": str(config["calendar_timezone"]),
        "calendar_local_date": local.date().isoformat(),
        "calendar_local_clock": local.isoformat(),
        "month_end_date": month_end_date.isoformat(),
        "is_month_end": is_month_end,
        "is_quarter_end": is_quarter_end,
        "holiday_adjustment": "weekday_only_no_holiday_inference",
    }


def source_cases(
    source_database: Path,
    config: Mapping[str, Any],
) -> list[sqlite3.Row]:
    if not source_database.exists():
        raise FileNotFoundError(f"source case database missing: {source_database}")
    source = sqlite3.connect(
        f"file:{source_database.resolve().as_posix()}?mode=ro", uri=True
    )
    source.row_factory = sqlite3.Row
    try:
        manifest = {
            str(row["key"]): str(row["value"])
            for row in source.execute("SELECT key, value FROM manifest")
        }
        if manifest.get("cohort_id") != str(config["source_case_cohort_id"]):
            raise RuntimeError("source case cohort identity mismatch")
        start = str(config["cohort_start_utc"])
        return list(
            source.execute(
                "SELECT case_id, factor_episode_id, start_utc FROM cases "
                "WHERE start_utc >= ? ORDER BY start_utc, case_id",
                (start,),
            )
        )
    finally:
        source.close()


def insert_labels(
    connection: sqlite3.Connection,
    rows: list[sqlite3.Row],
    config: Mapping[str, Any],
    inserted_utc: str,
) -> int:
    inserted = 0
    connection.execute("BEGIN IMMEDIATE")
    try:
        for row in rows:
            case_id = str(row["case_id"])
            episode = str(row["factor_episode_id"])
            label = build_label(str(row["start_utc"]), config)
            label_json = canonical_json(label)
            label_hash = sha256_text(label_json)
            existing = connection.execute(
                "SELECT label_sha256 FROM labels WHERE source_case_id = ?",
                (case_id,),
            ).fetchone()
            if existing:
                if str(existing["label_sha256"]) != label_hash:
                    raise RuntimeError(
                        f"append-only calendar-label violation for {case_id}"
                    )
                continue
            label_id = sha256_text(f"{config['cohort_id']}|{case_id}")
            connection.execute(
                "INSERT INTO labels(label_id, source_case_id, factor_episode_id, "
                "start_utc, label_json, label_sha256, inserted_utc) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    label_id, case_id, episode, str(row["start_utc"]),
                    label_json, label_hash, inserted_utc,
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
    manifest: Mapping[str, str],
    inserted: int,
) -> dict[str, Any]:
    rows = list(connection.execute("SELECT label_json FROM labels"))
    labels = [json.loads(str(row["label_json"])) for row in rows]
    return {
        **dict(manifest),
        "schema_version": "move_first_calendar_episode_label_report_v1",
        "ledger_schema_version": str(manifest["schema_version"]),
        "generated_utc": utc_now(),
        "label_count": len(labels),
        "month_end_count": sum(bool(row["is_month_end"]) for row in labels),
        "quarter_end_count": sum(bool(row["is_quarter_end"]) for row in labels),
        "inserted_this_cycle": inserted,
        "sqlite_integrity": str(connection.execute("PRAGMA integrity_check").fetchone()[0]),
        "append_only": True,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
    }


def run_once(
    config_path: Path = DEFAULT_CONFIG,
    source_database: Path = DEFAULT_SOURCE_DATABASE,
    database: Path = DEFAULT_DATABASE,
    report: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    config = load_config(config_path)
    manifest = expected_manifest(config, config_path)
    connection = open_database(database)
    try:
        bind_manifest(connection, manifest)
        inserted = insert_labels(
            connection, source_cases(source_database, config), config, utc_now()
        )
        payload = build_report(connection, manifest, inserted)
    finally:
        connection.close()
    census.atomic_text(report, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload


def verify(
    config_path: Path = DEFAULT_CONFIG,
    source_database: Path = DEFAULT_SOURCE_DATABASE,
    database: Path = DEFAULT_DATABASE,
    report: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    failures: list[str] = []
    config = load_config(config_path)
    manifest = expected_manifest(config, config_path)
    if not database.exists():
        return {"verified": False, "failures": ["database_missing"]}
    connection = sqlite3.connect(
        f"file:{database.resolve().as_posix()}?mode=ro", uri=True
    )
    connection.row_factory = sqlite3.Row
    try:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity != "ok":
            failures.append(f"sqlite_integrity:{integrity}")
        observed = {
            str(row["key"]): str(row["value"])
            for row in connection.execute("SELECT key, value FROM manifest")
        }
        if observed != manifest:
            failures.append("manifest_mismatch")
        rows = list(connection.execute("SELECT * FROM labels ORDER BY start_utc"))
        for row in rows:
            if parse_utc(str(row["start_utc"])) < parse_utc(str(config["cohort_start_utc"])):
                failures.append(f"pre_activation_label:{row['source_case_id']}")
            label = build_label(str(row["start_utc"]), config)
            label_json = canonical_json(label)
            if str(row["label_sha256"]) != sha256_text(label_json):
                failures.append(f"label_hash_mismatch:{row['source_case_id']}")
        if report.exists():
            payload = json.loads(report.read_text(encoding="utf-8"))
            if int(payload.get("label_count", -1)) != len(rows):
                failures.append("report_label_count_mismatch")
            if payload.get("execution_decision") != "no_trade":
                failures.append("report_execution_decision_not_no_trade")
        else:
            failures.append("report_missing")
        # Re-read the source identity even when no prospective case exists.
        source_cases(source_database, config)
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
    parser.add_argument("--source-database", type=Path, default=DEFAULT_SOURCE_DATABASE)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    args = parser.parse_args()
    if args.verify_only:
        payload = verify(args.config, args.source_database, args.database, args.report)
        print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
        return 0 if payload["verified"] else 1
    stop = time.monotonic() + max(0.0, float(args.duration_sec))
    while True:
        payload = run_once(args.config, args.source_database, args.database, args.report)
        print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
        if float(args.interval_sec) <= 0.0 or time.monotonic() >= stop:
            return 0
        time.sleep(min(float(args.interval_sec), max(0.0, stop - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
