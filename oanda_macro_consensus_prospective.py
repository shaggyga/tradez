#!/usr/bin/env python3
"""Prospectively archive authenticated pre-release macro consensus snapshots.

The initial snapshot is causal only for releases still in the future. Historical
calendar values are never backfilled as proof. The immutable SQLite ledger is
the source of truth; a deterministic JSONL projection feeds the existing macro
surprise ledger. This worker has no promotion, authorization, or broker path.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Mapping

from oanda_local_news_sentiment import normalized_observation_time


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
CONFIG = ROOT / "config" / "macro_consensus_prospective_v1.json"
DB = STATE / "macro_consensus_prospective_v1.sqlite"
OUTPUT = STATE / "macro_consensus_prospective_v1.json"
IMPORT = STATE / "macro_consensus_import_v1.jsonl"
REPORT = DATA / "reports" / "macro_consensus" / "MACRO_CONSENSUS_PROSPECTIVE_CURRENT.md"
ARCHIVE = DATA / "source_archives" / "macro_consensus_prospective_v1"
UTC = dt.timezone.utc


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def parse_time(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    for attempt in range(12):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt == 11:
                temporary.unlink(missing_ok=True)
                raise
            time.sleep(min(0.5, 0.025 * (2**attempt)))


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True))


def atomic_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_bytes(value)
    os.replace(temporary, path)


def credential(config: Mapping[str, Any]) -> str | None:
    names = config.get("credential_environment") or []
    if isinstance(names, str):
        names = [names]
    for name in names:
        value = os.environ.get(str(name), "").strip()
        if value:
            return value
    return None


def cohort_contract(config: Mapping[str, Any]) -> dict[str, Any]:
    config_sha = stable_hash(config)
    collector_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    definition_sha = stable_hash({"config_sha256": config_sha, "collector_sha256": collector_sha})
    return {
        "cohort_id": "macro_consensus_prospective_v1.discovery.20260812." + definition_sha[:16],
        "cohort_definition_sha256": definition_sha,
        "config_sha256": config_sha,
        "collector_sha256": collector_sha,
        "source_contract_id": str(config.get("source_contract_id") or ""),
        "provider": str(config.get("provider") or ""),
        "initial_snapshot_future_release_eligible": True,
        "historical_backfill_eligible": False,
        "material_change_requires_new_cohort": True,
        "research_only": True,
        "execution_eligible": False,
    }


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS consensus_observations (
          observation_id TEXT PRIMARY KEY, cohort_id TEXT NOT NULL,
          source_contract_id TEXT NOT NULL, provider TEXT NOT NULL,
          release_key TEXT NOT NULL, event_series_id TEXT NOT NULL,
          external_id TEXT NOT NULL, event_name TEXT NOT NULL,
          country TEXT NOT NULL, currency TEXT NOT NULL,
          scheduled_utc TEXT NOT NULL, captured_utc TEXT NOT NULL,
          source_timestamp_utc TEXT NOT NULL, consensus_text TEXT NOT NULL,
          consensus_value REAL, importance INTEGER NOT NULL,
          source_id TEXT NOT NULL, source_name TEXT NOT NULL,
          source_url TEXT NOT NULL, source_verified INTEGER NOT NULL,
          causal_valid INTEGER NOT NULL, rejection_reason TEXT NOT NULL,
          substantive_sha256 TEXT NOT NULL, raw_archive_sha256 TEXT NOT NULL,
          source_contract_json TEXT NOT NULL,
          UNIQUE(cohort_id,release_key,substantive_sha256)
        );
        CREATE INDEX IF NOT EXISTS ix_consensus_release_time
          ON consensus_observations(release_key,captured_utc);
        CREATE TABLE IF NOT EXISTS collection_cycles (
          cycle_id TEXT PRIMARY KEY, observed_utc TEXT NOT NULL,
          cohort_id TEXT NOT NULL, status TEXT NOT NULL,
          raw_archive_sha256 TEXT NOT NULL, inspected_rows INTEGER NOT NULL,
          inserted_rows INTEGER NOT NULL, causal_rows INTEGER NOT NULL,
          rejected_rows INTEGER NOT NULL, diagnostic_json TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS consensus_observations_no_update
          BEFORE UPDATE ON consensus_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS consensus_observations_no_delete
          BEFORE DELETE ON consensus_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS consensus_cycles_no_update
          BEFORE UPDATE ON collection_cycles BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS consensus_cycles_no_delete
          BEFORE DELETE ON collection_cycles BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    db.commit()
    return db


def fetch(config: Mapping[str, Any], api_key: str) -> bytes:
    query = dict(config.get("query") or {})
    query["c"] = api_key
    url = str(config["endpoint"]) + "?" + urllib.parse.urlencode(query)
    request = urllib.request.Request(url, headers={"User-Agent": "forex-research/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read()


def event_series_id(row: Mapping[str, Any]) -> str:
    return str(row.get("Symbol") or row.get("Ticker") or row.get("Event") or row.get("Category") or "").strip()


def release_key(row: Mapping[str, Any], scheduled: dt.datetime) -> str:
    identity = {
        "external_id": str(row.get("CalendarId") or row.get("CalendarID") or ""),
        "series": event_series_id(row),
        "country": str(row.get("Country") or ""),
        "scheduled_utc": iso(scheduled),
        "reference": str(row.get("Reference") or row.get("ReferenceDate") or ""),
    }
    return "macro_" + stable_hash(identity)[:32]


def ingest_rows(
    db: sqlite3.Connection,
    *,
    rows: list[Mapping[str, Any]],
    observed: dt.datetime,
    contract: Mapping[str, Any],
    config: Mapping[str, Any],
    raw_sha: str,
) -> dict[str, int]:
    counts = {"inspected": 0, "inserted": 0, "causal": 0, "rejected": 0}
    maximum_future = dt.timedelta(days=float(config.get("maximum_future_days") or 14))
    minimum_importance = int(config.get("minimum_importance") or 0)
    for raw_row in rows:
        if not isinstance(raw_row, Mapping):
            continue
        counts["inspected"] += 1
        scheduled = parse_time(raw_row.get("Date"))
        source_time = parse_time(raw_row.get("LastUpdate")) or observed
        value = finite(raw_row.get("ForecastValue"))
        consensus_text = str(raw_row.get("Forecast") or "")
        importance = int(finite(raw_row.get("Importance")) or 0)
        series = event_series_id(raw_row)
        reason = ""
        if scheduled is None or not series:
            reason = "missing_release_identity_or_schedule"
        elif value is None:
            reason = "missing_numeric_consensus"
        elif importance < minimum_importance:
            reason = "below_minimum_importance"
        elif scheduled <= observed:
            reason = "not_captured_before_release"
        elif scheduled - observed > maximum_future:
            reason = "outside_future_capture_window"
        elif source_time > observed + dt.timedelta(seconds=2):
            reason = "source_timestamp_after_capture"
        causal = not reason
        if scheduled is None:
            scheduled = observed
        key = release_key(raw_row, scheduled)
        material = {
            "release_key": key,
            "event_series_id": series,
            "scheduled_utc": iso(scheduled),
            "captured_utc": iso(observed),
            "source_timestamp_utc": iso(source_time),
            "consensus_text": consensus_text,
            "consensus_value": value,
            "external_id": str(raw_row.get("CalendarId") or raw_row.get("CalendarID") or ""),
            "event_name": str(raw_row.get("Event") or raw_row.get("Category") or ""),
            "country": str(raw_row.get("Country") or ""),
            "currency": str(raw_row.get("Currency") or "").upper(),
            "importance": importance,
            "source_url": str(raw_row.get("SourceURL") or ""),
            "causal_valid": causal,
            "rejection_reason": reason,
        }
        # Polling the same provider snapshot repeatedly is not new evidence.
        # Preserve the first capture of each distinct consensus value/version;
        # a changed value creates a new immutable row.
        substantive = stable_hash(
            {
                "release_key": key, "event_series_id": series,
                "scheduled_utc": material["scheduled_utc"],
                "consensus_text": consensus_text, "consensus_value": value,
                "external_id": material["external_id"], "importance": importance,
                "source_url": material["source_url"], "causal_valid": causal,
                "rejection_reason": reason,
            }
        )
        observation_id = "te_consensus_" + stable_hash(
            (contract["cohort_id"], key, substantive)
        )[:32]
        cursor = db.execute(
            "INSERT OR IGNORE INTO consensus_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                observation_id, contract["cohort_id"], contract["source_contract_id"],
                contract["provider"], key, series, material["external_id"],
                material["event_name"], material["country"], material["currency"],
                material["scheduled_utc"], material["captured_utc"],
                material["source_timestamp_utc"], consensus_text, value, importance,
                "trading_economics_calendar", "Trading Economics",
                material["source_url"], 1, int(causal), reason, substantive, raw_sha,
                canonical_json(contract),
            ),
        )
        if cursor.rowcount > 0:
            counts["inserted"] += 1
            counts["causal" if causal else "rejected"] += 1
    db.commit()
    return counts


def write_import(db: sqlite3.Connection, path: Path) -> int:
    rows = db.execute(
        """SELECT release_key,event_series_id,scheduled_utc,captured_utc,
                  source_timestamp_utc,consensus_text,consensus_value,
                  source_id,source_name,source_url,source_verified
           FROM consensus_observations ORDER BY captured_utc,observation_id"""
    ).fetchall()
    values = []
    for row in rows:
        values.append(
            canonical_json(
                {
                    "release_key": row[0], "event_series_id": row[1],
                    "scheduled_utc": row[2], "captured_utc": row[3],
                    "source_timestamp_utc": row[4], "consensus": row[5],
                    "consensus_value": row[6], "source_id": row[7],
                    "source_name": row[8], "source_url": row[9],
                    "source_verified": bool(row[10]),
                }
            )
        )
    atomic_text(path, "\n".join(values) + ("\n" if values else ""))
    return len(values)


def summary(db: sqlite3.Connection) -> dict[str, Any]:
    row = db.execute(
        """SELECT COUNT(*),SUM(causal_valid),SUM(NOT causal_valid),
                  COUNT(DISTINCT release_key),COUNT(DISTINCT event_series_id),
                  MIN(captured_utc),MAX(captured_utc)
           FROM consensus_observations"""
    ).fetchone()
    reasons = dict(db.execute(
        "SELECT rejection_reason,COUNT(*) FROM consensus_observations WHERE causal_valid=0 GROUP BY 1"
    ).fetchall())
    return {
        "observations": int(row[0] or 0), "causal_observations": int(row[1] or 0),
        "rejected_observations": int(row[2] or 0), "release_count": int(row[3] or 0),
        "series_count": int(row[4] or 0), "first_capture_utc": row[5],
        "last_capture_utc": row[6], "rejection_reasons": reasons,
    }


def run_once(
    *,
    config_path: Path = CONFIG,
    database: Path = DB,
    output: Path = OUTPUT,
    import_path: Path = IMPORT,
    report: Path = REPORT,
    archive: Path = ARCHIVE,
    observed: dt.datetime | None = None,
    raw: bytes | None = None,
) -> dict[str, Any]:
    config = read_json(config_path)
    contract = cohort_contract(config)
    if observed is None:
        observed, clock = normalized_observation_time(dt.datetime.now(UTC))
    else:
        observed = observed.astimezone(UTC)
        clock = {"source": "provided_test_time", "trusted_for_prospective_evidence": True}
    key = credential(config)
    if raw is None and not key:
        payload = {
            "schema_version": 1, "generated_utc": iso(observed),
            "status": "blocked_missing_trading_economics_api_key",
            "credential_present": False, "cohort": contract,
            "research_only": True, "execution_eligible": False,
            "can_place_orders": False, "can_promote": False,
            "supported_execution_decision": "no_trade",
        }
        atomic_json(output, payload)
        atomic_text(report, "# Prospective Macro Consensus\n\nStatus: **blocked_missing_trading_economics_api_key**. No consensus was fabricated or reconstructed after release.\n")
        return payload
    if raw is None:
        try:
            raw = fetch(config, str(key))
        except Exception as exc:
            # Do not let a transient provider/credential failure enter a rapid
            # supervisor restart loop, and never serialize the URL because it
            # contains the credential query parameter.
            payload = {
                "schema_version": 1,
                "generated_utc": iso(observed),
                "status": "degraded_fetch_failed",
                "credential_present": True,
                "provider_error_type": type(exc).__name__,
                "retry_after_sec": int(config.get("poll_interval_sec") or 7200),
                "cohort": contract,
                "research_only": True,
                "execution_eligible": False,
                "can_place_orders": False,
                "can_promote": False,
                "supported_execution_decision": "no_trade",
            }
            atomic_json(output, payload)
            atomic_text(
                report,
                "# Prospective Macro Consensus\n\n"
                "Status: **degraded_fetch_failed**. The credential was present, "
                "but the provider request failed. No URL, credential, consensus, "
                "or post-release reconstruction was recorded. Retry remains on "
                "the quota-safe polling cadence.\n",
            )
            return payload
    raw_sha = hashlib.sha256(raw).hexdigest()
    archive.mkdir(parents=True, exist_ok=True)
    archive_path = archive / f"{raw_sha}.json"
    if not archive_path.exists():
        atomic_bytes(archive_path, raw)
    parsed = json.loads(raw.decode("utf-8-sig"))
    rows = parsed if isinstance(parsed, list) else []
    db = connect(database)
    counts = ingest_rows(
        db, rows=rows, observed=observed, contract=contract, config=config, raw_sha=raw_sha
    )
    cycle_id = "consensus_cycle_" + stable_hash((contract["cohort_id"], iso(observed), raw_sha))[:28]
    db.execute(
        "INSERT OR IGNORE INTO collection_cycles VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            cycle_id, iso(observed), contract["cohort_id"], "ok", raw_sha,
            counts["inspected"], counts["inserted"], counts["causal"], counts["rejected"],
            canonical_json({"clock": clock}),
        ),
    )
    db.commit()
    projected = write_import(db, import_path)
    totals = summary(db)
    integrity = str(db.execute("PRAGMA quick_check").fetchone()[0])
    db.close()
    payload = {
        "schema_version": 1, "generated_utc": iso(observed), "status": "ok",
        "credential_present": bool(key), "research_only": True,
        "execution_eligible": False, "can_place_orders": False, "can_promote": False,
        "cohort": contract, "cycle": counts, "totals": totals,
        "import_projection_rows": projected, "raw_archive_sha256": raw_sha,
        "database_integrity": integrity, "observation_clock": clock,
        "supported_execution_decision": "no_trade",
    }
    atomic_json(output, payload)
    atomic_text(
        report,
        "\n".join(
            [
                "# Prospective Macro Consensus", "", f"Generated: `{payload['generated_utc']}`", "",
                "Research-only. Historical post-release values are never backfilled as causal consensus.", "",
                f"- Causal observations: **{totals['causal_observations']}**",
                f"- Rejected observations: **{totals['rejected_observations']}**",
                f"- Independent releases: **{totals['release_count']}**",
                f"- JSONL projection rows: **{projected}**",
                f"- SQLite integrity: **{integrity}**", "",
                "Missing a verified pre-release snapshot remains unavailable, never neutral.",
            ]
        ) + "\n",
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--database", type=Path, default=DB)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--import-jsonl", type=Path, default=IMPORT)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--archive", type=Path, default=ARCHIVE)
    parser.add_argument("--interval-sec", type=float, default=7200)
    parser.add_argument("--duration-sec", type=float, default=0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    stop = time.monotonic() + args.duration_sec if args.duration_sec > 0 else None
    while True:
        result = run_once(
            config_path=args.config, database=args.database, output=args.output,
            import_path=args.import_jsonl, report=args.report, archive=args.archive,
        )
        if args.once or stop is None or time.monotonic() >= stop:
            print(json.dumps(result, sort_keys=True))
            return 0
        time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
