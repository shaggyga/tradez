#!/usr/bin/env python3
"""Build a read-only research snapshot of ALFRED initial-release rate values.

ALFRED output_type=4 provides the first value released for each observation and
the civil date on which it became available.  Because that date has no
intraday precision, this snapshot conservatively delays availability until the
start of the following provider-local day.  It is historical backfill, never
prospective proof, and has no broker or execution path.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from oanda_alfred_vintage_prospective import (
    atomic_write,
    canonical_json,
    connect,
    read_json,
    stable_hash,
)


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
CONFIG = ROOT / "config" / "alfred_short_rate_initial_release_snapshot_v1.json"
DATABASE = STATE / "alfred_short_rate_initial_release_snapshot_v1.sqlite"
OUTPUT = STATE / "alfred_short_rate_initial_release_snapshot_v1.json"
REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "alfred_vintages"
    / "ALFRED_SHORT_RATE_INITIAL_RELEASE_SNAPSHOT_20260816.md"
)
ENDPOINT = "https://api.stlouisfed.org/fred/series/observations"
UTC = dt.timezone.utc


def resolve_api_key(environment_name: str) -> str:
    value = os.environ.get(environment_name, "").strip()
    if len(value) != 32 or not value.isalnum():
        raise RuntimeError(f"missing or invalid {environment_name}")
    return value


def conservative_available_utc(realtime_start: str, timezone_name: str) -> str:
    release_date = dt.date.fromisoformat(realtime_start)
    local_next_day = dt.datetime.combine(
        release_date + dt.timedelta(days=1),
        dt.time.min,
        tzinfo=ZoneInfo(timezone_name),
    )
    return local_next_day.astimezone(UTC).isoformat()


def fetch_initial_releases(
    series_id: str,
    key: str,
    *,
    realtime_start: str,
    realtime_end: str,
    observation_start: str,
) -> tuple[dict[str, Any], bytes]:
    query = urllib.parse.urlencode(
        {
            "series_id": series_id,
            "api_key": key,
            "file_type": "json",
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
            "observation_start": observation_start,
            "output_type": 4,
            "limit": 100000,
            "sort_order": "asc",
        }
    )
    request = urllib.request.Request(
        ENDPOINT + "?" + query,
        headers={"User-Agent": "forex-research/1.0"},
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        raw = response.read()
    payload = json.loads(raw)
    if not isinstance(payload, dict) or not isinstance(payload.get("observations"), list):
        raise ValueError("invalid FRED initial-release response")
    return payload, raw


def snapshot_contract(config: Mapping[str, Any]) -> dict[str, Any]:
    definition = {
        "config": config,
        "collector_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    digest = stable_hash(definition)
    prefix = str(
        config.get("cohort_prefix") or "alfred_short_rate_initial_release_snapshot_v1"
    )
    return {
        "cohort_id": f"{prefix}.{digest[:16]}",
        "source_contract_id": config.get("source_contract_id"),
        "provider": "Federal Reserve Bank of St. Louis FRED_ALFRED_API",
        "provider_output_type": 4,
        "availability_policy": config.get("provider_date_availability_policy"),
        "historical_backfill_only": True,
        "proof_eligible": False,
        "execution_eligible": False,
        "definition_sha256": digest,
    }


def ingest_payload(
    db: sqlite3.Connection,
    *,
    series: Mapping[str, Any],
    payload: Mapping[str, Any],
    raw_sha256: str,
    config: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> int:
    inserted = 0
    timezone_name = str(config.get("provider_realtime_timezone") or "America/Chicago")
    for row in payload.get("observations") or []:
        try:
            value = float(row.get("value"))
            observation_date = dt.date.fromisoformat(str(row.get("date"))).isoformat()
            realtime_start = dt.date.fromisoformat(str(row.get("realtime_start"))).isoformat()
        except (TypeError, ValueError):
            continue
        if not math.isfinite(value):
            continue
        first_seen = conservative_available_utc(realtime_start, timezone_name)
        substantive = stable_hash(
            {
                "series_id": series["series_id"],
                "observation_date": observation_date,
                "initial_release_value": value,
                "provider_vintage_date": realtime_start,
            }
        )
        observation_id = "alfred_initial_" + stable_hash(
            (contract["cohort_id"], series["series_id"], observation_date, substantive)
        )[:24]
        db.execute(
            "INSERT OR IGNORE INTO vintage_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                observation_id,
                contract["cohort_id"],
                contract["source_contract_id"],
                series["series_id"],
                series["economic_family"],
                series["currency"],
                observation_date,
                value,
                realtime_start,
                row.get("realtime_end"),
                first_seen,
                1,
                None,
                "historical_initial_release",
                0,
                0,
                0,
                "abstain",
                substantive,
                raw_sha256,
                canonical_json(contract),
            ),
        )
        inserted += int(db.execute("SELECT changes()").fetchone()[0] > 0)
    db.commit()
    return inserted


def run(
    config_path: Path = CONFIG,
    database_path: Path = DATABASE,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
) -> dict[str, Any]:
    config = read_json(config_path)
    key = resolve_api_key(str(config.get("api_key_environment") or "FRED_API_KEY"))
    now = dt.datetime.now(UTC)
    provider_timezone = ZoneInfo(
        str(config.get("provider_realtime_timezone") or "America/Chicago")
    )
    realtime_end = now.astimezone(provider_timezone).date().isoformat()
    contract = snapshot_contract(config)
    db = connect(database_path)
    errors: list[dict[str, str]] = []
    inserted = 0
    succeeded = 0
    for series in config.get("series") or []:
        try:
            payload, raw = fetch_initial_releases(
                str(series["series_id"]),
                key,
                realtime_start=str(config.get("realtime_start") or "2022-01-01"),
                realtime_end=realtime_end,
                observation_start=str(config.get("observation_start") or "2022-01-01"),
            )
            inserted += ingest_payload(
                db,
                series=series,
                payload=payload,
                raw_sha256=hashlib.sha256(raw).hexdigest(),
                config=config,
                contract=contract,
            )
            succeeded += 1
        except Exception as exc:
            errors.append(
                {"series_id": str(series.get("series_id")), "error": f"{type(exc).__name__}: {exc}"}
            )
    count, usable_series, currencies, minimum_vintage, maximum_vintage = db.execute(
        """SELECT COUNT(*),COUNT(DISTINCT series_id),COUNT(DISTINCT currency),MIN(provider_realtime_start),
                  MAX(provider_realtime_start)
             FROM vintage_observations WHERE cohort_id=?""",
        (contract["cohort_id"],),
    ).fetchone()
    retained_series = {
        str(row[0])
        for row in db.execute(
            "SELECT DISTINCT series_id FROM vintage_observations WHERE cohort_id=?",
            (contract["cohort_id"],),
        )
    }
    empty_series = sorted(
        str(row.get("series_id"))
        for row in config.get("series") or []
        if str(row.get("series_id")) not in retained_series
    )
    integrity = str(db.execute("PRAGMA quick_check(1)").fetchone()[0])
    db.close()
    result = {
        "schema_version": 1,
        "generated_utc": now.isoformat(),
        "cohort": contract,
        "configured_series": len(config.get("series") or []),
        "series_succeeded": succeeded,
        "usable_series": int(usable_series or 0),
        "empty_series": empty_series,
        "observations": int(count or 0),
        "currencies": int(currencies or 0),
        "inserted_this_run": inserted,
        "minimum_provider_vintage_date": minimum_vintage,
        "maximum_provider_vintage_date": maximum_vintage,
        "database_integrity": integrity,
        "errors": errors,
        "proof_eligible": False,
        "execution_eligible": False,
    }
    atomic_write(output_path, json.dumps(result, indent=2, sort_keys=True) + "\n")
    lines = [
        f"# {config.get('report_title') or 'ALFRED Initial-Release Snapshot'}",
        "",
        f"Generated: `{result['generated_utc']}`",
        "",
        "Historical point-in-time robustness input only; never prospective proof or execution input.",
        "",
        f"- API responses succeeded: **{succeeded}/{result['configured_series']}**",
        f"- Series with usable initial-release rows: **{result['usable_series']}/{result['configured_series']}**",
        f"- Empty initial-release series: **{', '.join(empty_series) or 'none'}**",
        f"- Currencies / observations: **{result['currencies']} / {result['observations']}**",
        f"- Vintage span: **{minimum_vintage or 'n/a'} to {maximum_vintage or 'n/a'}**",
        f"- SQLite integrity: **{integrity}**",
        f"- Errors: **{len(errors)}**",
        "",
        "ALFRED provides a civil vintage date. Availability is conservatively moved to the start of the following America/Chicago day, so the replay cannot act earlier on that date.",
        "",
    ]
    atomic_write(report_path, "\n".join(lines))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args()
    run(args.config, args.database, args.output, args.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
