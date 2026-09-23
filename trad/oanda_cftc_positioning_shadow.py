#!/usr/bin/env python3
"""Collect forward-causal CFTC currency positioning for shadow research only."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import time
from datetime import date, datetime, time as datetime_time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests


API_URL = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
SCHEDULE_PATH = ROOT / "config" / "cftc_cot_release_schedule_2026.json"
DATABASE = STATE / "cftc_currency_positioning_prospective_v2.sqlite"
REPORT = DATA / "reports" / "cftc_positioning" / "CFTC_POSITIONING_PROSPECTIVE_CURRENT.md"
SOURCE_CONTRACT_ID = "cftc_tff_currency_positioning_v2_20260814"
CONTRACTS = {
    "AUD": "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "CAD": "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "CHF": "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE",
    "EUR": "EURO FX - CHICAGO MERCANTILE EXCHANGE",
    "GBP": "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE",
    "JPY": "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE",
    "MXN": "MEXICAN PESO - CHICAGO MERCANTILE EXCHANGE",
    "NZD": "NZ DOLLAR - CHICAGO MERCANTILE EXCHANGE",
}
FIELDS = (
    "report_date_as_yyyy_mm_dd",
    "market_and_exchange_names",
    "open_interest_all",
    "lev_money_positions_long",
    "lev_money_positions_short",
    "asset_mgr_positions_long",
    "asset_mgr_positions_short",
    "dealer_positions_long_all",
    "dealer_positions_short_all",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def source_contract(schedule_path: Path = SCHEDULE_PATH) -> dict[str, Any]:
    schedule = load_release_schedule(schedule_path)
    definition = {
        "source_contract_id": SOURCE_CONTRACT_ID,
        "provider": "U.S. Commodity Futures Trading Commission",
        "dataset": API_URL,
        "report": "Traders in Financial Futures - futures only",
        "contracts": CONTRACTS,
        "fields": FIELDS,
        "release_schedule_sha256": stable_hash(schedule),
        "availability_policy": "first successful local observation; never Tuesday backdating",
        "direction_policy": "abstain",
        "research_only": True,
        "execution_eligible": False,
        "collector_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    definition_sha = stable_hash(definition)
    return {
        **definition,
        "cohort_id": f"{SOURCE_CONTRACT_ID}.discovery.{definition_sha[:16]}",
        "cohort_definition_sha256": definition_sha,
        "bootstrap_current_view_proof_eligible": False,
        "new_report_first_observed_prospective": True,
        "same_report_revision_proof_eligible": False,
        "material_change_requires_new_cohort": True,
    }


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS positioning_observations (
          observation_id TEXT PRIMARY KEY,
          cohort_id TEXT NOT NULL,
          source_contract_id TEXT NOT NULL,
          currency TEXT NOT NULL,
          report_date TEXT NOT NULL,
          first_seen_utc TEXT NOT NULL,
          observation_version INTEGER NOT NULL,
          observation_kind TEXT NOT NULL,
          bootstrap_current_view INTEGER NOT NULL,
          prospective_eligible INTEGER NOT NULL,
          substantive_sha256 TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          source_contract_json TEXT NOT NULL,
          UNIQUE(cohort_id,currency,report_date,substantive_sha256)
        );
        CREATE INDEX IF NOT EXISTS ix_cftc_positioning_latest
          ON positioning_observations(cohort_id,currency,report_date DESC,observation_version DESC);
        CREATE TABLE IF NOT EXISTS collection_cycles (
          cycle_id TEXT PRIMARY KEY,
          observed_utc TEXT NOT NULL,
          cohort_id TEXT NOT NULL,
          status TEXT NOT NULL,
          currency_count INTEGER NOT NULL,
          inserted_rows INTEGER NOT NULL,
          prospective_rows INTEGER NOT NULL,
          errors_json TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS cftc_positioning_no_update
          BEFORE UPDATE ON positioning_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS cftc_positioning_no_delete
          BEFORE DELETE ON positioning_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS cftc_cycles_no_update
          BEFORE UPDATE ON collection_cycles BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS cftc_cycles_no_delete
          BEFORE DELETE ON collection_cycles BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    db.commit()
    return db


def ingest_rows(
    db: sqlite3.Connection,
    *,
    currency: str,
    rows: list[dict[str, Any]],
    observed_utc: str,
    contract: dict[str, Any],
) -> tuple[int, int]:
    cohort_id = str(contract["cohort_id"])
    existing_count = int(
        db.execute(
            "SELECT count(*) FROM positioning_observations WHERE cohort_id=? AND currency=?",
            (cohort_id, currency),
        ).fetchone()[0]
    )
    initializing = existing_count == 0
    inserted = 0
    prospective = 0
    ordered = sorted(
        (row for row in rows if isinstance(row, dict)),
        key=lambda row: str(row.get("report_date_as_yyyy_mm_dd") or ""),
    )
    for row in ordered:
        report_date = str(row.get("report_date_as_yyyy_mm_dd") or "")
        if not report_date:
            continue
        substantive = stable_hash(row)
        duplicate = db.execute(
            """SELECT 1 FROM positioning_observations
                 WHERE cohort_id=? AND currency=? AND report_date=? AND substantive_sha256=?""",
            (cohort_id, currency, report_date, substantive),
        ).fetchone()
        if duplicate:
            continue
        prior_same = db.execute(
            """SELECT max(observation_version) FROM positioning_observations
                 WHERE cohort_id=? AND currency=? AND report_date=?""",
            (cohort_id, currency, report_date),
        ).fetchone()[0]
        newest_date = db.execute(
            """SELECT max(report_date) FROM positioning_observations
                 WHERE cohort_id=? AND currency=?""",
            (cohort_id, currency),
        ).fetchone()[0]
        version = int(prior_same or 0) + 1
        is_revision = prior_same is not None
        is_new_report = not initializing and not is_revision and (
            newest_date is None or report_date > str(newest_date)
        )
        kind = (
            "bootstrap_current_view"
            if initializing
            else "same_report_revision_first_seen"
            if is_revision
            else "new_report_first_observed"
            if is_new_report
            else "late_historical_row"
        )
        eligible = bool(is_new_report)
        observation_id = stable_hash(
            {
                "cohort_id": cohort_id,
                "currency": currency,
                "report_date": report_date,
                "substantive_sha256": substantive,
                "version": version,
            }
        )
        db.execute(
            """INSERT INTO positioning_observations
                 (observation_id,cohort_id,source_contract_id,currency,report_date,
                  first_seen_utc,observation_version,observation_kind,
                  bootstrap_current_view,prospective_eligible,substantive_sha256,
                  raw_json,source_contract_json)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                observation_id,
                cohort_id,
                SOURCE_CONTRACT_ID,
                currency,
                report_date,
                observed_utc,
                version,
                kind,
                int(initializing),
                int(eligible),
                substantive,
                canonical_json(row),
                canonical_json(contract),
            ),
        )
        inserted += 1
        prospective += int(eligible)
    return inserted, prospective


def latest_governance(
    db: sqlite3.Connection, currency: str, cohort_id: str
) -> dict[str, Any]:
    row = db.execute(
        """SELECT observation_id,first_seen_utc,observation_version,observation_kind,
                  bootstrap_current_view,prospective_eligible,report_date
             FROM positioning_observations
            WHERE cohort_id=? AND currency=?
            ORDER BY report_date DESC,observation_version DESC LIMIT 1""",
        (cohort_id, currency),
    ).fetchone()
    if row is None:
        return {}
    return {
        "observation_id": row[0],
        "first_seen_utc": row[1],
        "observation_version": int(row[2]),
        "observation_kind": row[3],
        "bootstrap_current_view": bool(row[4]),
        "prospective_eligible": bool(row[5]),
        "governed_report_date": row[6],
    }


def fetch_contract_rows(
    currency: str,
    market_name: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    client = session or requests.Session()
    response = client.get(
        API_URL,
        params={
            "$select": ",".join(FIELDS),
            "$where": f"market_and_exchange_names='{market_name}'",
            "$order": "report_date_as_yyyy_mm_dd desc",
            "$limit": "2",
        },
        timeout=30,
        headers={"User-Agent": "forex-shadow-research/1.0"},
    )
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list):
        raise ValueError(f"unexpected CFTC response for {currency}")
    return [row for row in rows if isinstance(row, dict)]


def normalized_position(row: dict[str, Any], prefix: str) -> float:
    open_interest = max(1.0, finite(row.get("open_interest_all"), 1.0))
    long_value = row.get(f"{prefix}_positions_long")
    short_value = row.get(f"{prefix}_positions_short")
    if long_value is None:
        long_value = row.get(f"{prefix}_positions_long_all")
    if short_value is None:
        short_value = row.get(f"{prefix}_positions_short_all")
    return (
        finite(long_value)
        - finite(short_value)
    ) / open_interest


def load_release_schedule(path: Path = SCHEDULE_PATH) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def scheduled_release(report_date: str, schedule: dict[str, Any]) -> dict[str, Any]:
    try:
        report = date.fromisoformat(report_date[:10])
    except ValueError:
        return {"release_schedule_covered": False}
    releases: list[date] = []
    for raw in schedule.get("release_dates") or []:
        try:
            releases.append(date.fromisoformat(str(raw)))
        except ValueError:
            continue
    later = next((value for value in sorted(releases) if value >= report), None)
    if later is None or (later - report).days > 7:
        return {"release_schedule_covered": False}
    hour, minute = [
        int(value)
        for value in str(schedule.get("release_time_local") or "15:30").split(":")
    ]
    zone = ZoneInfo(str(schedule.get("release_timezone") or "America/New_York"))
    published = datetime.combine(
        later, datetime_time(hour, minute), tzinfo=zone
    ).astimezone(timezone.utc)
    delayed = later.isoformat() in set(schedule.get("delayed_release_dates") or [])
    return {
        "release_schedule_covered": True,
        "scheduled_release_date": later.isoformat(),
        "scheduled_release_utc": published.isoformat(),
        "holiday_delayed_release": delayed,
        "release_schedule_source": schedule.get("source_url"),
        "release_schedule_tentative": bool(schedule.get("tentative", True)),
    }


def contract_snapshot(
    currency: str,
    rows: list[dict[str, Any]],
    observed_utc: str,
    prior: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not rows:
        raise ValueError(f"no CFTC rows for {currency}")
    current = rows[0]
    previous = rows[1] if len(rows) > 1 else {}
    report_date = str(current.get("report_date_as_yyyy_mm_dd") or "")
    prior = prior or {}
    first_seen = (
        str(prior.get("first_seen_utc"))
        if str(prior.get("report_date") or "") == report_date
        and prior.get("first_seen_utc")
        else observed_utc
    )
    lev = normalized_position(current, "lev_money")
    asset = normalized_position(current, "asset_mgr")
    dealer = normalized_position(current, "dealer")
    previous_lev = normalized_position(previous, "lev_money") if previous else math.nan
    previous_asset = normalized_position(previous, "asset_mgr") if previous else math.nan
    payload = {
        "currency": currency,
        "market_name": str(current.get("market_and_exchange_names") or ""),
        "report_date": report_date,
        "first_seen_utc": first_seen,
        "last_observed_utc": observed_utc,
        "open_interest": finite(current.get("open_interest_all")),
        "leveraged_money_net_pct_oi": round(lev, 8),
        "asset_manager_net_pct_oi": round(asset, 8),
        "dealer_net_pct_oi": round(dealer, 8),
        "leveraged_money_weekly_change": round(lev - previous_lev, 8)
        if math.isfinite(previous_lev)
        else None,
        "asset_manager_weekly_change": round(asset - previous_asset, 8)
        if math.isfinite(previous_asset)
        else None,
        "leveraged_vs_asset_crowding_gap": round(lev - asset, 8),
        "directional_interpretation": "research_context_only",
    }
    payload.update(scheduled_release(report_date, load_release_schedule()))
    return payload


def load_previous(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_report(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# CFTC Positioning Prospective Source",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Official weekly positioning context only. Direction abstains; this source cannot execute or promote.",
        "",
        f"- Source cohort: `{payload['source_cohort_id']}`",
        f"- Currencies: **{payload['currency_count']}**",
        f"- Immutable observations: **{payload['evidence']['immutable_observations']}**",
        f"- Prospective observations: **{payload['evidence']['prospective_observations']}**",
        f"- New rows this cycle: **{payload['evidence']['inserted_this_cycle']}**",
        f"- SQLite integrity: **{payload['evidence']['sqlite_integrity']}**",
        "",
        "| Currency | Report date | Leveraged net/OI | Asset manager net/OI | Dealer net/OI | Prospective |",
        "|---|---|---:|---:|---:|---|",
    ]
    for currency, row in sorted(payload.get("currencies", {}).items()):
        lines.append(
            f"| {currency} | {str(row.get('report_date') or '')[:10]} | "
            f"{finite(row.get('leveraged_money_net_pct_oi')):.3f} | "
            f"{finite(row.get('asset_manager_net_pct_oi')):.3f} | "
            f"{finite(row.get('dealer_net_pct_oi')):.3f} | "
            f"{'yes' if row.get('prospective_eligible') else 'no'} |"
        )
    lines += [
        "",
        "Bootstrap values describe the current official view but are not proof evidence. Only a new report first observed after cohort initialization is prospective. Same-report revisions remain separately versioned and proof-ineligible.",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text("\n".join(lines), encoding="utf-8")
    os.replace(temporary, path)


def collect(
    output: Path,
    *,
    database: Path = DATABASE,
    report: Path = REPORT,
    acquired: dict[str, list[dict[str, Any]]] | None = None,
    observed_utc: str | None = None,
) -> dict[str, Any]:
    observed = observed_utc or utc_now()
    contract = source_contract()
    session = requests.Session()
    currencies: dict[str, Any] = {}
    errors: dict[str, str] = {}
    db = connect(database)
    inserted_total = 0
    prospective_total = 0
    for currency, market_name in CONTRACTS.items():
        if acquired is not None and currency not in acquired:
            continue
        try:
            rows = (
                acquired[currency]
                if acquired is not None
                else fetch_contract_rows(currency, market_name, session=session)
            )
            inserted, prospective = ingest_rows(
                db,
                currency=currency,
                rows=rows,
                observed_utc=observed,
                contract=contract,
            )
            inserted_total += inserted
            prospective_total += prospective
            governance = latest_governance(db, currency, str(contract["cohort_id"]))
            snapshot = contract_snapshot(
                currency,
                rows,
                observed,
                prior={
                    "report_date": governance.get("governed_report_date"),
                    "first_seen_utc": governance.get("first_seen_utc"),
                },
            )
            snapshot.update(governance)
            snapshot["source_contract_id"] = SOURCE_CONTRACT_ID
            snapshot["source_cohort_id"] = contract["cohort_id"]
            snapshot["directional_interpretation"] = "abstain_research_context_only"
            currencies[currency] = snapshot
        except (requests.RequestException, ValueError) as exc:
            errors[currency] = f"{type(exc).__name__}: {exc}"
    status = "ok" if currencies and not errors else "degraded" if currencies else "failed"
    cycle_id = stable_hash(
        {
            "cohort_id": contract["cohort_id"],
            "observed_utc": observed,
            "currencies": sorted(currencies),
            "errors": errors,
        }
    )
    db.execute(
        """INSERT OR IGNORE INTO collection_cycles
             (cycle_id,observed_utc,cohort_id,status,currency_count,inserted_rows,
              prospective_rows,errors_json) VALUES (?,?,?,?,?,?,?,?)""",
        (
            cycle_id,
            observed,
            contract["cohort_id"],
            status,
            len(currencies),
            inserted_total,
            prospective_total,
            canonical_json(errors),
        ),
    )
    db.commit()
    immutable_count = int(
        db.execute(
            "SELECT count(*) FROM positioning_observations WHERE cohort_id=?",
            (contract["cohort_id"],),
        ).fetchone()[0]
    )
    prospective_count = int(
        db.execute(
            """SELECT count(*) FROM positioning_observations
                 WHERE cohort_id=? AND prospective_eligible=1""",
            (contract["cohort_id"],),
        ).fetchone()[0]
    )
    integrity = str(db.execute("PRAGMA quick_check").fetchone()[0])
    db.close()
    payload = {
        "schema_version": 2,
        "generated_utc": observed,
        "source": "CFTC Traders in Financial Futures - futures only",
        "source_url": API_URL,
        "source_contract_id": SOURCE_CONTRACT_ID,
        "source_cohort_id": contract["cohort_id"],
        "source_contract": contract,
        "research_only": True,
        "shadow_only": True,
        "account_eligible": False,
        "execution_adapter": False,
        "availability_policy": (
            "first local successful observation; never backdate to Tuesday report date"
        ),
        "historical_causal_backtest_ready": False,
        "historical_blocker": (
            "the official tentative 2026 release schedule is mapped; older years, "
            "later schedule changes, revisions, and exceptional announcements still "
            "require immutable point-in-time observations"
        ),
        "release_schedule_contract": str(SCHEDULE_PATH.resolve()),
        "currency_count": len(currencies),
        "currencies": currencies,
        "errors": errors,
        "status": status,
        "evidence": {
            "database": str(database.resolve()),
            "immutable_observations": immutable_count,
            "prospective_observations": prospective_count,
            "inserted_this_cycle": inserted_total,
            "prospective_this_cycle": prospective_total,
            "sqlite_integrity": integrity,
        },
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, output)
    write_report(report, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument(
        "--interval-sec",
        type=float,
        default=0.0,
        help="Repeat collection at this cadence; zero performs one collection.",
    )
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    if args.interval_sec < 0.0 or args.duration_sec < 0.0:
        raise SystemExit("interval and duration must be non-negative")
    stop_at = time.monotonic() + args.duration_sec if args.duration_sec else None
    while True:
        payload = collect(args.output, database=args.database, report=args.report)
        print(
            json.dumps(
                {
                    "output": str(args.output.resolve()),
                    "generated_utc": payload["generated_utc"],
                    "currency_count": payload["currency_count"],
                    "errors": payload["errors"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
        if args.interval_sec <= 0.0:
            return 0 if payload["currency_count"] else 1
        if stop_at is not None and time.monotonic() >= stop_at:
            return 0 if payload["currency_count"] else 1
        delay = max(60.0, args.interval_sec)
        if stop_at is not None:
            delay = min(delay, max(0.0, stop_at - time.monotonic()))
            if delay <= 0.0:
                return 0 if payload["currency_count"] else 1
        time.sleep(delay)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CONTRACTS",
    "collect",
    "contract_snapshot",
    "connect",
    "fetch_contract_rows",
    "ingest_rows",
    "latest_governance",
    "normalized_position",
    "scheduled_release",
    "source_contract",
]
