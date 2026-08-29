#!/usr/bin/env python3
"""Append-only prospective FRED/ALFRED macro-vintage collector.

The first current-view response for each series is an engineering bootstrap and
can never be proof evidence. Later new values and revisions are causal only
from their locally observed timestamp. Provider real-time window dates are not
hashed, so repeated polling cannot manufacture revisions. This worker has no
broker, promotion, authorization, or execution path.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import re
import sqlite3
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from oanda_local_news_sentiment import normalized_observation_time

ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
CONFIG = ROOT / "config" / "alfred_vintage_prospective_v1.json"
DB = STATE / "alfred_vintage_prospective_v1.sqlite"
OUTPUT = STATE / "alfred_vintage_prospective_v1.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "alfred_vintages" / "ALFRED_VINTAGE_CURRENT.md"
ENDPOINT = "https://api.stlouisfed.org/fred/series/observations"
KEY_RE = re.compile(r"^[a-z0-9]{32}$")
PROVIDER_REALTIME_TIMEZONE = "America/Chicago"


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


def atomic_write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS vintage_observations (
          vintage_observation_id TEXT PRIMARY KEY, cohort_id TEXT NOT NULL,
          source_contract_id TEXT NOT NULL, series_id TEXT NOT NULL,
          economic_family TEXT NOT NULL, currency TEXT NOT NULL,
          observation_date TEXT NOT NULL, value REAL NOT NULL,
          provider_realtime_start TEXT, provider_realtime_end TEXT,
          first_seen_utc TEXT NOT NULL, version INTEGER NOT NULL,
          supersedes_observation_id TEXT, observation_kind TEXT NOT NULL,
          bootstrap_current_view INTEGER NOT NULL,
          prospective_eligible INTEGER NOT NULL,
          same_day_intrahour_eligible INTEGER NOT NULL,
          direction_policy TEXT NOT NULL, substantive_sha256 TEXT NOT NULL,
          raw_response_sha256 TEXT NOT NULL, source_contract_json TEXT NOT NULL,
          UNIQUE(cohort_id,series_id,observation_date,substantive_sha256)
        );
        CREATE INDEX IF NOT EXISTS ix_alfred_series_date_version
          ON vintage_observations(cohort_id,series_id,observation_date,version DESC);
        CREATE TABLE IF NOT EXISTS collection_cycles (
          cycle_id TEXT PRIMARY KEY, observed_utc TEXT NOT NULL,
          cohort_id TEXT NOT NULL, status TEXT NOT NULL,
          series_attempted INTEGER NOT NULL, series_succeeded INTEGER NOT NULL,
          inserted_rows INTEGER NOT NULL, prospective_rows INTEGER NOT NULL,
          error_count INTEGER NOT NULL, diagnostic_json TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS alfred_observations_no_update
          BEFORE UPDATE ON vintage_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS alfred_observations_no_delete
          BEFORE DELETE ON vintage_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS alfred_cycles_no_update
          BEFORE UPDATE ON collection_cycles BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS alfred_cycles_no_delete
          BEFORE DELETE ON collection_cycles BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    db.commit()
    return db


def cohort_contract(
    config: Mapping[str, Any], *, collector_sha256: str | None = None
) -> dict[str, Any]:
    config_sha = stable_hash(config)
    collector_sha = collector_sha256 or hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    definition_sha = stable_hash({
        "config_sha256": config_sha,
        "collector_sha256": collector_sha,
    })
    legacy_config_only_id = (
        "alfred_vintage_prospective_v1.discovery.20260808." + config_sha[:16]
    )
    return {
        "cohort_id": (
            "alfred_vintage_prospective_v1.discovery.20260808."
            + definition_sha[:16]
        ),
        "supersedes_cohort_id": str(
            config.get("supersedes_cohort_id") or legacy_config_only_id
        ),
        "cohort_definition_sha256": definition_sha,
        "config_sha256": config_sha,
        "collector_sha256": collector_sha,
        "source_contract_id": str(config.get("source_contract_id")),
        "source": "official_fred_alfred_api",
        "first_seen_contract": "application_clock_corrected_at_retrieval",
        "initial_current_view_proof_eligible": False,
        "same_day_intrahour_replay_eligible": False,
        "direction_policy": "abstain",
        "material_change_requires_new_cohort": True,
        "research_only": True,
        "execution_eligible": False,
    }


def api_key(config: Mapping[str, Any]) -> str | None:
    value = os.environ.get(str(config.get("api_key_environment") or "FRED_API_KEY"), "").strip()
    return value if KEY_RE.fullmatch(value) else None


def provider_realtime_date(observed: dt.datetime) -> str:
    """Return the provider-local civil date accepted by FRED real-time windows."""
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=dt.timezone.utc)
    return observed.astimezone(ZoneInfo(PROVIDER_REALTIME_TIMEZONE)).date().isoformat()


def fetch_series(series_id: str, key: str, observed: dt.datetime) -> tuple[dict[str, Any], bytes]:
    # FRED rejects a UTC date that is already tomorrow in America/Chicago.
    # The evidence first-seen timestamp remains the independently normalized
    # UTC instant; only the provider query window uses the provider civil day.
    day = provider_realtime_date(observed)
    query = urllib.parse.urlencode({
        "series_id": series_id, "api_key": key, "file_type": "json",
        "realtime_start": day, "realtime_end": day,
        "limit": 100000, "sort_order": "asc",
    })
    request = urllib.request.Request(ENDPOINT + "?" + query, headers={"User-Agent": "forex-research/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read()
    value = json.loads(raw)
    if not isinstance(value, dict) or not isinstance(value.get("observations"), list):
        raise ValueError("invalid FRED observations payload")
    return value, raw


def ingest_series(db: sqlite3.Connection, *, series: Mapping[str, Any], payload: Mapping[str, Any],
                  raw_sha: str, observed_utc: str, contract: Mapping[str, Any]) -> tuple[int, int]:
    cohort = str(contract["cohort_id"]); series_id = str(series["series_id"])
    initial = db.execute(
        "SELECT COUNT(*) FROM vintage_observations WHERE cohort_id=? AND series_id=?",
        (cohort, series_id),
    ).fetchone()[0] == 0
    maximum_prior_date = db.execute(
        "SELECT MAX(observation_date) FROM vintage_observations WHERE cohort_id=? AND series_id=?",
        (cohort, series_id),
    ).fetchone()[0]
    inserted = prospective = 0
    for row in payload.get("observations") or []:
        try:
            value = float(row.get("value"))
        except (TypeError, ValueError):
            continue
        if not math.isfinite(value) or not row.get("date"):
            continue
        observation_date = str(row["date"])
        substantive = stable_hash({"series_id": series_id, "observation_date": observation_date, "value": value})
        prior = db.execute(
            """SELECT vintage_observation_id,substantive_sha256,version
               FROM vintage_observations WHERE cohort_id=? AND series_id=? AND observation_date=?
               ORDER BY version DESC,rowid DESC LIMIT 1""",
            (cohort, series_id, observation_date),
        ).fetchone()
        if prior and str(prior[1]) == substantive:
            continue
        is_new = bool(not initial and prior is None and maximum_prior_date and observation_date > str(maximum_prior_date))
        is_revision = bool(not initial and prior is not None)
        eligible = is_new or is_revision
        kind = "bootstrap_current_view" if initial else "new_release_first_seen" if is_new else "revision_first_seen"
        version = int(prior[2]) + 1 if prior else 1
        observation_id = "alfred_" + stable_hash((cohort, series_id, observation_date, substantive, version))[:28]
        db.execute(
            "INSERT OR IGNORE INTO vintage_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (observation_id, cohort, contract["source_contract_id"], series_id,
             series.get("economic_family"), series.get("currency"), observation_date, value,
             row.get("realtime_start"), row.get("realtime_end"), observed_utc, version,
             prior[0] if prior else None, kind, int(initial), int(eligible), 0, "abstain",
             substantive, raw_sha, canonical_json(contract)),
        )
        changed = int(db.execute("SELECT changes()").fetchone()[0] > 0)
        inserted += changed; prospective += changed * int(eligible)
    db.commit()
    return inserted, prospective


def summarize(db: sqlite3.Connection, cohort: str) -> dict[str, Any]:
    row = db.execute(
        """SELECT COUNT(*),COUNT(DISTINCT series_id),SUM(bootstrap_current_view),
                  SUM(prospective_eligible),SUM(CASE WHEN observation_kind='revision_first_seen' THEN 1 ELSE 0 END),
                  MAX(first_seen_utc) FROM vintage_observations WHERE cohort_id=?""", (cohort,),
    ).fetchone()
    return {"observations": int(row[0] or 0), "series": int(row[1] or 0),
            "bootstrap_rows": int(row[2] or 0), "prospective_rows": int(row[3] or 0),
            "revision_rows": int(row[4] or 0), "last_first_seen_utc": row[5]}


def coverage_summary(
    db: sqlite3.Connection,
    cohort: str,
    config: Mapping[str, Any],
    observed: dt.datetime,
) -> dict[str, Any]:
    """Expose scope and staleness without mistaking bootstrap breadth for proof."""

    configured = sorted(
        {str(row.get("currency") or "") for row in config.get("series") or []}
        - {""}
    )
    latest_by_currency = {
        str(currency): str(latest_date)
        for currency, latest_date in db.execute(
            """SELECT currency,MAX(observation_date)
                 FROM vintage_observations
                WHERE cohort_id=?
                GROUP BY currency""",
            (cohort,),
        )
        if currency and latest_date
    }
    provider_day = observed.astimezone(
        ZoneInfo(PROVIDER_REALTIME_TIMEZONE)
    ).date()
    ages = {
        currency: (provider_day - dt.date.fromisoformat(latest)).days
        for currency, latest in latest_by_currency.items()
    }
    prospective_currencies = int(
        db.execute(
            """SELECT COUNT(DISTINCT currency)
                 FROM vintage_observations
                WHERE cohort_id=? AND prospective_eligible=1""",
            (cohort,),
        ).fetchone()[0]
        or 0
    )
    context_only = sorted(
        {
            str(row.get("currency") or "")
            for row in config.get("series") or []
            if row.get("comparison_eligible") is False
        }
        - {""}
    )
    return {
        "configured_currency_count": len(configured),
        "observed_currency_count": len(latest_by_currency),
        "prospective_currency_count": prospective_currencies,
        "current_within_120_days_currency_count": sum(age <= 120 for age in ages.values()),
        "context_only_currency_count": len(context_only),
        "context_only_currencies": context_only,
        "latest_observation_date_by_currency": latest_by_currency,
        "latest_observation_age_days_by_currency": ages,
        "cross_currency_proof_ready": False,
    }


def collect_once(config_path: Path = CONFIG, db_path: Path = DB, output: Path = OUTPUT,
                 report: Path = REPORT, fetcher: Callable[[str, str, dt.datetime], tuple[dict[str, Any], bytes]] = fetch_series) -> dict[str, Any]:
    config = read_json(config_path); contract = cohort_contract(config)
    raw_local = dt.datetime.now(dt.timezone.utc); observed, clock = normalized_observation_time(raw_local)
    key = api_key(config)
    if key is None:
        payload = {"schema_version": 1, "generated_utc": observed.isoformat(),
                   "status": "blocked_missing_fred_api_key", "cohort": contract,
                   "configured_series": len(config.get("series") or []),
                   "credential_environment": config.get("api_key_environment") or "FRED_API_KEY",
                   "credential_present": False, "observation_clock": clock,
                   "research_only": True, "execution_eligible": False,
                   "can_place_orders": False, "can_promote": False,
                   "supported_execution_decision": "no_trade"}
        atomic_write(output, json.dumps(payload, indent=2, sort_keys=True))
        atomic_write(report, "# ALFRED Vintage Source\n\nStatus: **blocked_missing_fred_api_key**. Adapter is ready; no data was fabricated or scraped.\n")
        return payload
    db = connect(db_path); attempted = succeeded = inserted = prospective = 0; errors = []
    for series in config.get("series") or []:
        attempted += 1
        try:
            data, raw = fetcher(str(series["series_id"]), key, observed)
            added, eligible = ingest_series(db, series=series, payload=data,
                                             raw_sha=hashlib.sha256(raw).hexdigest(),
                                             observed_utc=observed.isoformat(), contract=contract)
            succeeded += 1; inserted += added; prospective += eligible
        except Exception as exc:  # per-series failure remains explicit and fail-closed
            errors.append({"series_id": series.get("series_id"), "error": type(exc).__name__ + ": " + str(exc)[:240]})
    status = "ok" if not errors else "partial_error" if succeeded else "error"
    diagnostic = {"errors": errors, "clock": clock}
    cycle_id = "alfred_cycle_" + stable_hash((contract["cohort_id"], observed.isoformat()))[:24]
    db.execute("INSERT INTO collection_cycles VALUES (?,?,?,?,?,?,?,?,?,?)",
               (cycle_id, observed.isoformat(), contract["cohort_id"], status, attempted,
                succeeded, inserted, prospective, len(errors), canonical_json(diagnostic)))
    db.commit(); totals = summarize(db, contract["cohort_id"])
    coverage = coverage_summary(db, contract["cohort_id"], config, observed)
    integrity = db.execute("PRAGMA quick_check").fetchone()[0]; db.close()
    payload = {"schema_version": 1, "generated_utc": observed.isoformat(), "status": status,
               "cohort": contract, "cycle": {"series_attempted": attempted, "series_succeeded": succeeded,
               "inserted_rows": inserted, "prospective_rows": prospective, "errors": errors},
               "totals": totals, "coverage": coverage,
               "database_integrity": integrity, "observation_clock": clock,
               "research_only": True, "execution_eligible": False, "can_place_orders": False,
               "can_promote": False, "supported_execution_decision": "no_trade"}
    atomic_write(output, json.dumps(payload, indent=2, sort_keys=True))
    atomic_write(report, "\n".join(["# ALFRED Vintage Source", "", f"Generated: `{payload['generated_utc']}`", "",
        f"- Status: **{status}**", f"- Series succeeded: **{succeeded}/{attempted}**",
        f"- Immutable observations: **{totals['observations']}**", f"- Prospective rows: **{totals['prospective_rows']}**",
        f"- Currency coverage: **{coverage['observed_currency_count']}/{coverage['configured_currency_count']}**",
        f"- Currencies current within 120 days: **{coverage['current_within_120_days_currency_count']}**",
        f"- Currencies with prospective evidence: **{coverage['prospective_currency_count']}**", "",
        "Bootstrap breadth is historical context, not proof. Cross-frequency comparisons are forbidden; annual HKD/SGD/THB series are context-only. Later values become causal only from first-seen time. Direction abstains.", ""]))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG); parser.add_argument("--database", type=Path, default=DB)
    parser.add_argument("--output", type=Path, default=OUTPUT); parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--interval-sec", type=float, default=21600); parser.add_argument("--duration-sec", type=float, default=604800)
    parser.add_argument("--once", action="store_true"); args = parser.parse_args(); stop = time.monotonic() + args.duration_sec
    while True:
        collect_once(args.config, args.database, args.output, args.report)
        if args.once or time.monotonic() >= stop: return 0
        time.sleep(min(args.interval_sec, max(0.0, stop - time.monotonic())))


if __name__ == "__main__": raise SystemExit(main())
