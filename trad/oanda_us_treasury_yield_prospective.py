#!/usr/bin/env python3
"""Prospectively archive the official U.S. Treasury daily yield curve.

The first download is a current-view bootstrap and is never proof evidence.
Only a later, newly observed yield date can become a prospective source row.
Revisions remain append-only but cannot masquerade as the original release.
This collector predicts no direction and has no broker/execution imports.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

import requests

from oanda_local_news_sentiment import normalized_observation_time
from oanda_us_treasury_yield_discovery import TREASURY_URL, parse_yield_xml


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_CONFIG = ROOT / "config" / "us_treasury_yield_prospective_v1.json"
DEFAULT_DATABASE = DATA / "state" / "us_treasury_yield_prospective_v1.sqlite"
DEFAULT_STATE = DATA / "state" / "us_treasury_yield_prospective_v1.json"
DEFAULT_ARCHIVE = DATA / "source_archives" / "us_treasury_yield_prospective_v1"


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def iso(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat()


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS rate_observations (
            observation_id TEXT PRIMARY KEY,
            cohort_id TEXT NOT NULL,
            observed_utc TEXT NOT NULL,
            yield_date TEXT NOT NULL,
            feed_updated_utc TEXT,
            two_year_pct REAL NOT NULL,
            ten_year_pct REAL NOT NULL,
            curve_2s10s_bps REAL NOT NULL,
            row_sha256 TEXT NOT NULL,
            raw_archive_sha256 TEXT NOT NULL,
            observation_version INTEGER NOT NULL,
            supersedes_observation_id TEXT,
            observation_kind TEXT NOT NULL,
            bootstrap_current_view INTEGER NOT NULL,
            prospective_eligible INTEGER NOT NULL,
            direction_policy TEXT NOT NULL,
            source_contract_json TEXT NOT NULL,
            UNIQUE(cohort_id,yield_date,row_sha256)
        );
        CREATE INDEX IF NOT EXISTS ix_treasury_yield_date_version
            ON rate_observations(cohort_id,yield_date,observation_version DESC);
        CREATE TABLE IF NOT EXISTS collection_cycles (
            cycle_id TEXT PRIMARY KEY,
            observed_utc TEXT NOT NULL,
            cohort_id TEXT NOT NULL,
            raw_archive_sha256 TEXT NOT NULL,
            parsed_rows INTEGER NOT NULL,
            inserted_rows INTEGER NOT NULL,
            prospective_rows INTEGER NOT NULL,
            revised_rows INTEGER NOT NULL,
            status TEXT NOT NULL,
            diagnostics_json TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS treasury_observations_no_update
            BEFORE UPDATE ON rate_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS treasury_observations_no_delete
            BEFORE DELETE ON rate_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS treasury_cycles_no_update
            BEFORE UPDATE ON collection_cycles BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS treasury_cycles_no_delete
            BEFORE DELETE ON collection_cycles BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.commit()
    return connection


def cohort_contract(config: dict[str, Any], collector_sha256: str) -> dict[str, Any]:
    config_sha = stable_hash(config)
    definition_sha = stable_hash({
        "config_sha256": config_sha,
        "collector_sha256": collector_sha256,
    })
    legacy_config_only_id = (
        "us_treasury_yield_prospective_v1.discovery."
        f"{str(config.get('cohort_start_utc') or '')[:10].replace('-', '')}."
        f"{config_sha[:16]}"
    )
    return {
        "cohort_id": (
            "us_treasury_yield_prospective_v1.discovery."
            f"{str(config.get('cohort_start_utc') or '')[:10].replace('-', '')}."
            f"{definition_sha[:16]}"
        ),
        "supersedes_cohort_id": legacy_config_only_id,
        "cohort_definition_sha256": definition_sha,
        "config_sha256": config_sha,
        "collector_sha256": collector_sha256,
        "source": "official_us_treasury_daily_par_yield_curve",
        "first_seen_contract": "application_clock_corrected_at_local_retrieval",
        "bootstrap_current_view_proof_eligible": False,
        "historical_revisions_direction_eligible": False,
        "direction_policy": "abstain",
        "material_change_requires_new_cohort": True,
        "research_only": True,
        "execution_eligible": False,
    }


def substantive_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "yield_date": str(row["yield_date"]),
        "two_year_pct": float(row["two_year_pct"]),
        "ten_year_pct": float(row["ten_year_pct"]),
        "curve_2s10s_bps": float(row["curve_2s10s_bps"]),
    }


def ingest(
    connection: sqlite3.Connection,
    *,
    raw: bytes,
    observed_utc: str,
    contract: dict[str, Any],
    archive_sha256: str,
    clock_diagnostic: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = parse_yield_xml(raw)
    cohort_id = str(contract["cohort_id"])
    prior_count = int(connection.execute(
        "SELECT COUNT(*) FROM rate_observations WHERE cohort_id=?", (cohort_id,)
    ).fetchone()[0])
    maximum_prior_date = connection.execute(
        "SELECT MAX(yield_date) FROM rate_observations WHERE cohort_id=?", (cohort_id,)
    ).fetchone()[0]
    initial_bootstrap = prior_count == 0
    inserted = prospective = revisions = 0
    for row in rows:
        material = substantive_row(row)
        row_sha = stable_hash(material)
        prior = connection.execute(
            """SELECT observation_id,row_sha256,observation_version
               FROM rate_observations WHERE cohort_id=? AND yield_date=?
               ORDER BY observation_version DESC,rowid DESC LIMIT 1""",
            (cohort_id, material["yield_date"]),
        ).fetchone()
        if prior is not None and str(prior[1]) == row_sha:
            continue
        is_new_date = bool(
            not initial_bootstrap
            and maximum_prior_date is not None
            and material["yield_date"] > str(maximum_prior_date)
            and prior is None
        )
        version = int(prior[2]) + 1 if prior is not None else 1
        kind = (
            "bootstrap_current_view"
            if initial_bootstrap
            else "new_yield_date_first_observed"
            if is_new_date
            else "revised_or_late_historical_row"
        )
        observation_id = "treasury_rate_" + stable_hash(
            (cohort_id, material["yield_date"], row_sha, version)
        )[:28]
        connection.execute(
            "INSERT OR IGNORE INTO rate_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                observation_id, cohort_id, observed_utc, material["yield_date"],
                row.get("feed_updated_utc"), material["two_year_pct"],
                material["ten_year_pct"], material["curve_2s10s_bps"],
                row_sha, archive_sha256, version,
                str(prior[0]) if prior is not None else None, kind,
                int(initial_bootstrap), int(is_new_date), "abstain",
                canonical_json(contract),
            ),
        )
        inserted += 1
        prospective += int(is_new_date)
        revisions += int(prior is not None)
    diagnostics = {
        "clock": clock_diagnostic or {},
        "initial_bootstrap": initial_bootstrap,
        "maximum_prior_date": maximum_prior_date,
    }
    cycle_id = "treasury_cycle_" + stable_hash(
        (cohort_id, observed_utc, archive_sha256)
    )[:28]
    connection.execute(
        "INSERT OR IGNORE INTO collection_cycles VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            cycle_id, observed_utc, cohort_id, archive_sha256, len(rows), inserted,
            prospective, revisions, "ok", canonical_json(diagnostics),
        ),
    )
    connection.commit()
    return {
        "parsed_rows": len(rows),
        "inserted_rows": inserted,
        "prospective_rows": prospective,
        "revised_rows": revisions,
        "initial_bootstrap": initial_bootstrap,
    }


def totals(connection: sqlite3.Connection, cohort_id: str) -> dict[str, Any]:
    row = connection.execute(
        """SELECT COUNT(*),COUNT(DISTINCT yield_date),
                  SUM(bootstrap_current_view),SUM(prospective_eligible),
                  SUM(CASE WHEN observation_version>1 THEN 1 ELSE 0 END),
                  MAX(yield_date),MAX(observed_utc)
           FROM rate_observations WHERE cohort_id=?""",
        (cohort_id,),
    ).fetchone()
    return {
        "observations": int(row[0] or 0),
        "yield_dates": int(row[1] or 0),
        "bootstrap_rows": int(row[2] or 0),
        "prospective_eligible_rows": int(row[3] or 0),
        "revision_rows": int(row[4] or 0),
        "latest_yield_date": row[5],
        "last_observed_utc": row[6],
    }


def fetch_current_year(url: str, year: int) -> bytes:
    response = requests.get(
        url,
        params={"data": "daily_treasury_yield_curve", "field_tdr_date_value": str(year)},
        headers={"Accept": "application/xml", "User-Agent": "forex-source-integrity/1.0"},
        timeout=45,
    )
    response.raise_for_status()
    return response.content


def run_once(
    *,
    config_path: Path = DEFAULT_CONFIG,
    database: Path = DEFAULT_DATABASE,
    state: Path = DEFAULT_STATE,
    archive: Path = DEFAULT_ARCHIVE,
) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    collector_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    contract = cohort_contract(config, collector_sha)
    raw_local = dt.datetime.now(dt.timezone.utc)
    observed, clock = normalized_observation_time(raw_local)
    try:
        raw = fetch_current_year(
            str(config.get("source_url") or TREASURY_URL), observed.year
        )
        raw_sha = hashlib.sha256(raw).hexdigest()
        archive.mkdir(parents=True, exist_ok=True)
        target = archive / f"treasury_daily_curve_{observed.year}_{raw_sha}.xml"
        if not target.exists():
            temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
            temporary.write_bytes(raw)
            os.replace(temporary, target)
        connection = connect(database)
        try:
            cycle = ingest(
                connection, raw=raw, observed_utc=iso(observed), contract=contract,
                archive_sha256=raw_sha, clock_diagnostic=clock,
            )
            aggregate = totals(connection, str(contract["cohort_id"]))
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        finally:
            connection.close()
        payload = {
            "schema_version": 1, "generated_utc": iso(observed), "status": "ok",
            "research_only": True, "execution_eligible": False,
            "can_place_orders": False, "can_promote": False,
            "direction_policy": "abstain", "supported_execution_decision": "no_trade",
            "cohort": contract, "cycle": cycle, "totals": aggregate,
            "observation_clock": clock, "database_integrity": integrity,
            "raw_archive_sha256": raw_sha,
            "limitations": [
                "daily Treasury par yields are not intraday OIS or policy futures",
                "bootstrap current-view history is never prospective proof evidence",
                "revisions are retained but cannot be treated as original-date information",
            ],
        }
    except Exception as exc:
        payload = {
            "schema_version": 1, "generated_utc": iso(observed),
            "status": "source_error", "error": f"{type(exc).__name__}: {exc}",
            "research_only": True, "execution_eligible": False,
            "can_place_orders": False, "can_promote": False,
            "direction_policy": "abstain", "supported_execution_decision": "no_trade",
            "cohort": contract, "observation_clock": clock,
        }
    atomic_json(state, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        result = run_once(
            config_path=args.config, database=args.database,
            state=args.state, archive=args.archive,
        )
        if args.interval_sec <= 0.0 or (
            args.duration_sec > 0.0 and time.monotonic() - started >= args.duration_sec
        ):
            print(json.dumps(result, sort_keys=True))
            return 0 if result.get("status") == "ok" else 1
        time.sleep(max(300.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["cohort_contract", "connect", "ingest", "substantive_row", "totals"]
