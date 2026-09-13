"""Seal live move/news cases into a direct append-only prospective cohort.

V3 reads a mutable six-hour historical audit.  That remains a useful frozen
diagnostic but is too far from the source to guarantee near-live capture.  V4
copies immutable rows directly from the append-only V7R3 mover database and
also records the factor membership and root-merge events needed for honest
effective-episode counts.

Research only.  This module cannot promote, authorize, or place an order.
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

import oanda_major_move_gap_census as census


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "move_first_live_case_capture_v4"
)
DEFAULT_CONFIG = ROOT / "config" / "move_first_live_case_capture_v4_20260901.json"
DEFAULT_SOURCE_DATABASE = STATE / "live_move_news_cases_v7r3.sqlite"
DEFAULT_DATABASE = STATE / "move_first_live_case_capture_v4_20260901.sqlite"
DEFAULT_REPORT = REPORT_ROOT / "MOVE_FIRST_LIVE_CASE_CAPTURE_CURRENT.json"


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
        "source_database_contract_id",
        "factor_episode_contract_id",
        "historical_rows_imported",
    }
    missing = sorted(required - set(value))
    if missing:
        raise ValueError(f"config missing required fields: {missing}")
    parse_utc(str(value["cohort_start_utc"]))
    if int(value["historical_rows_imported"]) != 0:
        raise ValueError("prospective cohort must import zero historical rows")
    if value.get("execution_eligible") is not False or value.get("can_place_orders") is not False:
        raise ValueError("prospective capture cohort must remain nonexecuting")
    return value


def manifest(config: Mapping[str, Any], config_path: Path) -> dict[str, str]:
    return {
        "schema_version": "move_first_live_case_capture_ledger_v1",
        "cohort_id": str(config["cohort_id"]),
        "cohort_start_utc": str(config["cohort_start_utc"]),
        "config_sha256": file_sha256(config_path),
        "source_database_contract_id": str(config["source_database_contract_id"]),
        "factor_episode_contract_id": str(config["factor_episode_contract_id"]),
        "historical_rows_imported": "0",
        "execution_eligible": "false",
    }


def open_destination(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS manifest (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS cases (
            source_case_id TEXT PRIMARY KEY,
            first_recorded_utc TEXT NOT NULL,
            instrument TEXT NOT NULL,
            start_utc TEXT NOT NULL,
            end_utc TEXT NOT NULL,
            case_json TEXT NOT NULL,
            case_sha256 TEXT NOT NULL,
            inserted_utc TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_capture_cases_first_recorded
            ON cases(first_recorded_utc, source_case_id);
        CREATE TABLE IF NOT EXISTS factor_memberships (
            source_case_id TEXT PRIMARY KEY REFERENCES cases(source_case_id),
            factor_episode_contract_id TEXT NOT NULL,
            factor_episode_id TEXT NOT NULL,
            factor_primary_token TEXT NOT NULL,
            registered_utc TEXT NOT NULL,
            membership_json TEXT NOT NULL,
            membership_sha256 TEXT NOT NULL,
            inserted_utc TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_capture_memberships_episode
            ON factor_memberships(factor_episode_id);
        CREATE TABLE IF NOT EXISTS factor_root_merges (
            merge_id TEXT PRIMARY KEY,
            factor_episode_contract_id TEXT NOT NULL,
            from_root_id TEXT NOT NULL,
            into_root_id TEXT NOT NULL,
            factor_primary_token TEXT NOT NULL,
            bridge_case_ids TEXT NOT NULL,
            detected_utc TEXT NOT NULL,
            merge_json TEXT NOT NULL,
            merge_sha256 TEXT NOT NULL,
            inserted_utc TEXT NOT NULL
        );
        """
    )
    return connection


def bind_manifest(connection: sqlite3.Connection, expected: Mapping[str, str]) -> None:
    observed = {
        str(row["key"]): str(row["value"])
        for row in connection.execute("SELECT key, value FROM manifest")
    }
    if observed and observed != dict(expected):
        raise RuntimeError("cohort manifest mismatch; material changes require a new cohort/database")
    if not observed:
        connection.executemany(
            "INSERT INTO manifest(key, value) VALUES (?, ?)", sorted(expected.items())
        )
        connection.commit()


def open_source(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(f"source mover database missing: {path}")
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def verify_source_contract(source: sqlite3.Connection, config: Mapping[str, Any]) -> None:
    integrity = str(source.execute("PRAGMA integrity_check").fetchone()[0])
    if integrity != "ok":
        raise RuntimeError(f"source database integrity failed: {integrity}")
    snapshot = source.execute(
        "SELECT contract_id FROM mover_case_contract_registry WHERE contract_id = ?",
        (str(config["source_database_contract_id"]),),
    ).fetchone()
    if snapshot is None:
        raise RuntimeError("source mover contract mismatch")
    factor = source.execute(
        "SELECT factor_episode_contract_id FROM factor_episode_contract_registry "
        "WHERE factor_episode_contract_id = ?",
        (str(config["factor_episode_contract_id"]),),
    ).fetchone()
    if factor is None:
        raise RuntimeError("source factor-episode contract mismatch")


def _row_payload(row: sqlite3.Row, fields: tuple[str, ...]) -> dict[str, Any]:
    return {field: row[field] for field in fields}


CASE_FIELDS = (
    "case_id",
    "first_recorded_utc",
    "instrument",
    "start_utc",
    "end_utc",
    "case_json",
)
MEMBERSHIP_FIELDS = (
    "case_id",
    "factor_episode_contract_id",
    "factor_episode_id",
    "factor_primary_token",
    "registered_utc",
)
MERGE_FIELDS = (
    "merge_id",
    "factor_episode_contract_id",
    "from_root_id",
    "into_root_id",
    "factor_primary_token",
    "bridge_case_ids",
    "detected_utc",
)


def capture_cases(
    source: sqlite3.Connection,
    destination: sqlite3.Connection,
    cohort_start_utc: str,
    inserted_utc: str,
) -> tuple[int, int]:
    rows = list(
        source.execute(
            """
            SELECT c.case_id, c.first_recorded_utc, c.instrument, c.start_utc,
                   c.end_utc, c.case_json, m.factor_episode_contract_id,
                   m.factor_episode_id, m.factor_primary_token, m.registered_utc
            FROM mover_cases AS c
            LEFT JOIN factor_episode_membership AS m ON m.case_id = c.case_id
            WHERE c.first_recorded_utc >= ?
            ORDER BY c.first_recorded_utc, c.case_id
            """,
            (cohort_start_utc,),
        )
    )
    inserted_cases = 0
    inserted_memberships = 0
    for row in rows:
        if row["factor_episode_id"] is None:
            raise RuntimeError(f"source case lacks factor membership: {row['case_id']}")
        case_hash = sha256_text(str(row["case_json"]))
        existing = destination.execute(
            "SELECT case_sha256 FROM cases WHERE source_case_id = ?", (row["case_id"],)
        ).fetchone()
        if existing is not None:
            if str(existing["case_sha256"]) != case_hash:
                raise RuntimeError(f"append-only case mutation: {row['case_id']}")
        else:
            destination.execute(
                "INSERT INTO cases VALUES(?,?,?,?,?,?,?,?)",
                (
                    row["case_id"],
                    row["first_recorded_utc"],
                    row["instrument"],
                    row["start_utc"],
                    row["end_utc"],
                    row["case_json"],
                    case_hash,
                    inserted_utc,
                ),
            )
            inserted_cases += 1
        membership = _row_payload(row, MEMBERSHIP_FIELDS)
        membership_json = canonical_json(membership)
        membership_hash = sha256_text(membership_json)
        existing_membership = destination.execute(
            "SELECT membership_sha256 FROM factor_memberships WHERE source_case_id = ?",
            (row["case_id"],),
        ).fetchone()
        if existing_membership is not None:
            if str(existing_membership["membership_sha256"]) != membership_hash:
                raise RuntimeError(f"append-only membership mutation: {row['case_id']}")
        else:
            destination.execute(
                "INSERT INTO factor_memberships VALUES(?,?,?,?,?,?,?,?)",
                (
                    row["case_id"],
                    row["factor_episode_contract_id"],
                    row["factor_episode_id"],
                    row["factor_primary_token"],
                    row["registered_utc"],
                    membership_json,
                    membership_hash,
                    inserted_utc,
                ),
            )
            inserted_memberships += 1
    return inserted_cases, inserted_memberships


def capture_merges(
    source: sqlite3.Connection,
    destination: sqlite3.Connection,
    cohort_start_utc: str,
    inserted_utc: str,
) -> int:
    inserted = 0
    rows = source.execute(
        "SELECT * FROM factor_episode_root_merges WHERE detected_utc >= ? "
        "ORDER BY detected_utc, merge_id",
        (cohort_start_utc,),
    )
    for row in rows:
        payload = _row_payload(row, MERGE_FIELDS)
        merge_json = canonical_json(payload)
        merge_hash = sha256_text(merge_json)
        existing = destination.execute(
            "SELECT merge_sha256 FROM factor_root_merges WHERE merge_id = ?",
            (row["merge_id"],),
        ).fetchone()
        if existing is not None:
            if str(existing["merge_sha256"]) != merge_hash:
                raise RuntimeError(f"append-only root-merge mutation: {row['merge_id']}")
            continue
        destination.execute(
            "INSERT INTO factor_root_merges VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                row["merge_id"],
                row["factor_episode_contract_id"],
                row["from_root_id"],
                row["into_root_id"],
                row["factor_primary_token"],
                row["bridge_case_ids"],
                row["detected_utc"],
                merge_json,
                merge_hash,
                inserted_utc,
            ),
        )
        inserted += 1
    return inserted


def resolve_root(value: str, edges: Mapping[str, str]) -> tuple[str, bool]:
    current = value
    seen: set[str] = set()
    while current in edges:
        if current in seen:
            return current, True
        seen.add(current)
        current = edges[current]
    return current, False


def build_report(
    connection: sqlite3.Connection,
    expected_manifest: Mapping[str, str],
    inserted_cases: int,
    inserted_memberships: int,
    inserted_merges: int,
) -> dict[str, Any]:
    aggregate = connection.execute(
        "SELECT COUNT(*) case_count, MIN(first_recorded_utc) first_recorded_utc, "
        "MAX(first_recorded_utc) last_recorded_utc FROM cases"
    ).fetchone()
    membership_rows = list(connection.execute("SELECT factor_episode_id FROM factor_memberships"))
    merge_rows = list(connection.execute("SELECT from_root_id, into_root_id FROM factor_root_merges"))
    edges = {str(row["from_root_id"]): str(row["into_root_id"]) for row in merge_rows}
    roots: set[str] = set()
    graph_cycle = False
    for row in membership_rows:
        root, found_cycle = resolve_root(str(row["factor_episode_id"]), edges)
        roots.add(root)
        graph_cycle = graph_cycle or found_cycle
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    return {
        **dict(expected_manifest),
        "schema_version": "move_first_live_case_capture_report_v1",
        "ledger_schema_version": str(expected_manifest["schema_version"]),
        "generated_utc": utc_now(),
        "case_count": int(aggregate["case_count"]),
        "factor_membership_count": len(membership_rows),
        "resolved_factor_episode_count": len(roots),
        "factor_root_merge_count": len(merge_rows),
        "factor_graph_cycle": graph_cycle,
        "first_recorded_utc": aggregate["first_recorded_utc"],
        "last_recorded_utc": aggregate["last_recorded_utc"],
        "inserted_cases_this_cycle": inserted_cases,
        "inserted_memberships_this_cycle": inserted_memberships,
        "inserted_root_merges_this_cycle": inserted_merges,
        "sqlite_integrity": integrity,
        "append_only": True,
        "source_distance": "direct_append_only_live_mover_database",
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
    expected = manifest(config, config_path)
    source = open_source(source_database)
    destination = open_destination(database)
    try:
        verify_source_contract(source, config)
        bind_manifest(destination, expected)
        inserted_utc = utc_now()
        source_cohort_start = parse_utc(str(config["cohort_start_utc"])).isoformat()
        destination.execute("BEGIN IMMEDIATE")
        try:
            inserted_cases, inserted_memberships = capture_cases(
                source,
                destination,
                source_cohort_start,
                inserted_utc,
            )
            inserted_merges = capture_merges(
                source,
                destination,
                source_cohort_start,
                inserted_utc,
            )
            destination.commit()
        except Exception:
            destination.rollback()
            raise
        payload = build_report(
            destination,
            expected,
            inserted_cases,
            inserted_memberships,
            inserted_merges,
        )
    finally:
        source.close()
        destination.close()
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
        return {"verified": False, "failures": ["database_missing"]}
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
        cases = list(connection.execute("SELECT * FROM cases"))
        memberships = list(connection.execute("SELECT * FROM factor_memberships"))
        if len(cases) != len(memberships):
            failures.append("case_membership_count_mismatch")
        for row in cases:
            if str(row["case_sha256"]) != sha256_text(str(row["case_json"])):
                failures.append(f"case_hash_mismatch:{row['source_case_id']}")
        for row in memberships:
            if str(row["membership_sha256"]) != sha256_text(str(row["membership_json"])):
                failures.append(f"membership_hash_mismatch:{row['source_case_id']}")
        for row in connection.execute("SELECT * FROM factor_root_merges"):
            if str(row["merge_sha256"]) != sha256_text(str(row["merge_json"])):
                failures.append(f"merge_hash_mismatch:{row['merge_id']}")
        if report.exists():
            payload = json.loads(report.read_text(encoding="utf-8"))
            if int(payload.get("case_count", -1)) != len(cases):
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
    parser.add_argument("--source-database", type=Path, default=DEFAULT_SOURCE_DATABASE)
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
        payload = run_once(args.config, args.source_database, args.database, args.report)
        print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
        if float(args.interval_sec) <= 0.0 or time.monotonic() >= stop:
            return 0
        time.sleep(min(float(args.interval_sec), max(0.0, stop - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
