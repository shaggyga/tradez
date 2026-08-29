#!/usr/bin/env python3
"""Prospectively freeze a transparent internal macro expectation baseline.

This is deliberately *not* market consensus.  It issues a point-in-time,
append-only persistence/robust-drift forecast for the next distinct official
release in a currency/series, then matures that forecast only when a later
source-native actual is observed.  The ledger cannot trade, promote, authorize,
or populate the causal-consensus fields used by the macro-surprise contract.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
import statistics
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from oanda_local_news_sentiment import normalized_observation_time


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
SOURCE_DB = STATE / "macro_surprise_v1.sqlite"
CONFIG = ROOT / "config" / "internal_macro_expectation_v1.json"
DATABASE = STATE / "internal_macro_expectation_v1.sqlite"
OUTPUT = STATE / "internal_macro_expectation_v1.json"
REPORT = DATA / "reports" / "macro_expectation" / "INTERNAL_MACRO_EXPECTATION_CURRENT.md"
UTC = dt.timezone.utc


def parse_utc(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
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


def stable_hash(value: Any) -> str:
    material = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def contract(config: Mapping[str, Any], code_path: Path | None = None) -> dict[str, Any]:
    code = (code_path or Path(__file__)).read_bytes()
    definition = {
        "config": dict(config),
        "source_contract": "macro_surprise_v1_exact_known_time_actuals",
        "episode_identity": "currency_series_reference_period_else_release_key",
        "forecast_target": "first_later_distinct_episode",
        "market_consensus_substitution": False,
    }
    definition_hash = stable_hash(definition)
    return {
        "contract_id": str(
            config.get("contract_id")
            or "internal_macro_expectation_persistence_v1_20260816"
        ),
        "cohort_id": "internal_macro_expectation_v1.discovery.20260816."
        + definition_hash[:16],
        "definition_sha256": definition_hash,
        "code_sha256": hashlib.sha256(code).hexdigest(),
        "research_only": True,
        "execution_eligible": False,
        "can_populate_market_consensus": False,
        "material_change_requires_new_cohort": True,
    }


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS cohort_contracts (
          cohort_id TEXT PRIMARY KEY,
          created_utc TEXT NOT NULL,
          definition_sha256 TEXT NOT NULL,
          code_sha256 TEXT NOT NULL,
          contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS expectation_forecasts (
          forecast_id TEXT PRIMARY KEY,
          cohort_id TEXT NOT NULL,
          currency TEXT NOT NULL,
          event_series_id TEXT NOT NULL,
          event_name TEXT NOT NULL,
          unit TEXT NOT NULL,
          issued_utc TEXT NOT NULL,
          expires_utc TEXT NOT NULL,
          target_semantics TEXT NOT NULL,
          expected_value REAL NOT NULL,
          model_name TEXT NOT NULL,
          model_components_json TEXT NOT NULL,
          training_episode_count INTEGER NOT NULL,
          training_episode_ids_json TEXT NOT NULL,
          training_cutoff_utc TEXT NOT NULL,
          training_fingerprint_sha256 TEXT NOT NULL,
          market_consensus INTEGER NOT NULL CHECK(market_consensus=0),
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0)
        );
        CREATE TABLE IF NOT EXISTS expectation_outcomes (
          outcome_id TEXT PRIMARY KEY,
          forecast_id TEXT NOT NULL UNIQUE,
          cohort_id TEXT NOT NULL,
          target_episode_id TEXT NOT NULL,
          target_release_key TEXT NOT NULL,
          target_known_utc TEXT NOT NULL,
          target_reference_period TEXT NOT NULL,
          actual_value REAL NOT NULL,
          expected_value REAL NOT NULL,
          forecast_error REAL NOT NULL,
          absolute_error REAL NOT NULL,
          matured_utc TEXT NOT NULL,
          payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_expectation_scope
          ON expectation_forecasts(cohort_id,currency,event_series_id,issued_utc);
        CREATE TRIGGER IF NOT EXISTS cohort_contracts_no_update
          BEFORE UPDATE ON cohort_contracts
          BEGIN SELECT RAISE(ABORT, 'expectation cohort contracts are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS cohort_contracts_no_delete
          BEFORE DELETE ON cohort_contracts
          BEGIN SELECT RAISE(ABORT, 'expectation cohort contracts are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS expectation_forecasts_no_update
          BEFORE UPDATE ON expectation_forecasts
          BEGIN SELECT RAISE(ABORT, 'internal macro expectations are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS expectation_forecasts_no_delete
          BEFORE DELETE ON expectation_forecasts
          BEGIN SELECT RAISE(ABORT, 'internal macro expectations are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS expectation_outcomes_no_update
          BEFORE UPDATE ON expectation_outcomes
          BEGIN SELECT RAISE(ABORT, 'internal macro expectation outcomes are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS expectation_outcomes_no_delete
          BEFORE DELETE ON expectation_outcomes
          BEGIN SELECT RAISE(ABORT, 'internal macro expectation outcomes are immutable'); END;
        """
    )
    return db


def _episode_identity(
    currency: str, series: str, reference_period: str, release_key: str
) -> str:
    discriminator = reference_period.strip().lower() or release_key
    return "macro_episode_" + stable_hash((currency, series, discriminator))[:32]


def load_episodes(source_path: Path, cutoff: dt.datetime) -> list[dict[str, Any]]:
    if not source_path.exists():
        return []
    db = sqlite3.connect(f"file:{source_path.as_posix()}?mode=ro", uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        """SELECT revision_id,release_key,causal_known_utc,event_series_id,event_name,
                  currencies_json,reference_period,unit,actual_value,previous_value,
                  source_id,source_verified,source_direct,payload_json
             FROM macro_release_revisions
            WHERE actual_value IS NOT NULL AND source_verified=1 AND source_direct=1
              AND causal_known_utc<=?
            ORDER BY causal_known_utc,row_id""",
        (iso(cutoff),),
    ).fetchall()
    db.close()
    episodes: dict[str, dict[str, Any]] = {}
    for row in rows:
        known = parse_utc(row[2])
        actual = finite(row[8])
        try:
            currencies = [str(item).upper() for item in json.loads(row[5] or "[]")]
        except (TypeError, ValueError, json.JSONDecodeError):
            currencies = []
        if known is None or actual is None or len(currencies) != 1:
            continue
        currency = currencies[0]
        series = str(row[3] or "").strip()
        if not series:
            continue
        reference = str(row[6] or "").strip()
        episode_id = _episode_identity(currency, series, reference, str(row[1]))
        candidate = {
            "episode_id": episode_id,
            "revision_id": str(row[0]),
            "release_key": str(row[1]),
            "known_utc": iso(known),
            "currency": currency,
            "event_series_id": series,
            "event_name": str(row[4] or series),
            "reference_period": reference,
            "unit": str(row[7] or ""),
            "actual_value": actual,
            "previous_value": finite(row[9]),
            "source_id": str(row[10] or ""),
        }
        existing = episodes.get(episode_id)
        if existing is None or candidate["known_utc"] < existing["known_utc"]:
            episodes[episode_id] = candidate
    return sorted(episodes.values(), key=lambda row: (row["known_utc"], row["episode_id"]))


def forecast_value(values: Sequence[float]) -> tuple[float, str, dict[str, Any]]:
    last = float(values[-1])
    components: dict[str, Any] = {"last_observation": last}
    candidates = [last]
    model_name = "last_observation"
    if len(values) >= 3:
        deltas = [float(values[index] - values[index - 1]) for index in range(1, len(values))]
        median_delta = statistics.median(deltas)
        damped_last_delta = 0.5 * deltas[-1]
        components.update(
            {
                "median_delta_projection": last + median_delta,
                "damped_last_delta_projection": last + damped_last_delta,
                "median_delta": median_delta,
                "damped_last_delta": damped_last_delta,
            }
        )
        candidates.extend((last + median_delta, last + damped_last_delta))
        model_name = "robust_drift_ensemble"
    expected = float(statistics.median(candidates))
    components["ensemble_median"] = expected
    return expected, model_name, components


def mature_forecasts(
    db: sqlite3.Connection,
    episodes: Sequence[Mapping[str, Any]],
    observed: dt.datetime,
) -> int:
    matured_ids = {row[0] for row in db.execute("SELECT forecast_id FROM expectation_outcomes")}
    rows = db.execute(
        """SELECT forecast_id,cohort_id,currency,event_series_id,issued_utc,
                  expires_utc,expected_value,training_episode_ids_json
             FROM expectation_forecasts ORDER BY issued_utc,forecast_id"""
    ).fetchall()
    inserted = 0
    for row in rows:
        if row[0] in matured_ids:
            continue
        issued = parse_utc(row[4])
        expires = parse_utc(row[5])
        if issued is None or expires is None:
            continue
        training = set(json.loads(row[7] or "[]"))
        target = next(
            (
                episode
                for episode in episodes
                if episode["currency"] == row[2]
                and episode["event_series_id"] == row[3]
                and episode["episode_id"] not in training
                and parse_utc(episode["known_utc"]) is not None
                and issued < parse_utc(episode["known_utc"]) <= expires
            ),
            None,
        )
        if target is None:
            continue
        actual = float(target["actual_value"])
        expected = float(row[6])
        error = actual - expected
        payload = {
            "forecast_id": row[0],
            "target_episode": dict(target),
            "expected_value": expected,
            "forecast_error": error,
            "market_consensus": False,
            "research_only": True,
            "execution_eligible": False,
        }
        outcome_id = "macro_expectation_outcome_" + stable_hash((row[0], target["episode_id"]))[:32]
        db.execute(
            """INSERT OR IGNORE INTO expectation_outcomes VALUES
               (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                outcome_id,
                row[0],
                row[1],
                target["episode_id"],
                target["release_key"],
                target["known_utc"],
                target["reference_period"],
                actual,
                expected,
                error,
                abs(error),
                iso(observed),
                json.dumps(payload, sort_keys=True),
            ),
        )
        inserted += int(db.execute("SELECT changes()").fetchone()[0] > 0)
    db.commit()
    return inserted


def issue_forecasts(
    db: sqlite3.Connection,
    episodes: Sequence[Mapping[str, Any]],
    observed: dt.datetime,
    config: Mapping[str, Any],
    cohort: Mapping[str, Any],
) -> int:
    minimum = max(1, int(config.get("minimum_training_episodes") or 1))
    maximum = max(minimum, int(config.get("maximum_training_episodes") or 24))
    maximum_age = max(1, int(config.get("maximum_forecast_age_days") or 120))
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for episode in episodes:
        grouped[(str(episode["currency"]), str(episode["event_series_id"]))].append(episode)
    inserted = 0
    for (currency, series), group in sorted(grouped.items()):
        group = list(group[-maximum:])
        if len(group) < minimum:
            continue
        pending = db.execute(
            """SELECT count(*) FROM expectation_forecasts f
               LEFT JOIN expectation_outcomes o USING(forecast_id)
               WHERE f.cohort_id=? AND f.currency=? AND f.event_series_id=?
                 AND o.forecast_id IS NULL AND f.expires_utc>?""",
            (cohort["cohort_id"], currency, series, iso(observed)),
        ).fetchone()[0]
        if pending:
            continue
        values = [float(row["actual_value"]) for row in group]
        expected, model_name, components = forecast_value(values)
        training_ids = [str(row["episode_id"]) for row in group]
        training_material = [
            {
                "episode_id": row["episode_id"],
                "known_utc": row["known_utc"],
                "actual_value": row["actual_value"],
            }
            for row in group
        ]
        training_hash = stable_hash(training_material)
        forecast_id = "macro_expectation_" + stable_hash(
            (cohort["cohort_id"], currency, series, iso(observed), training_hash)
        )[:32]
        expires = observed + dt.timedelta(days=maximum_age)
        latest = group[-1]
        db.execute(
            """INSERT OR IGNORE INTO expectation_forecasts VALUES
               (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                forecast_id,
                cohort["cohort_id"],
                currency,
                series,
                latest["event_name"],
                latest["unit"],
                iso(observed),
                iso(expires),
                str(config.get("target") or "next_distinct_official_release_for_currency_and_series"),
                expected,
                model_name,
                json.dumps(components, sort_keys=True),
                len(group),
                json.dumps(training_ids, sort_keys=True),
                iso(observed),
                training_hash,
                0,
                1,
                0,
            ),
        )
        inserted += int(db.execute("SELECT changes()").fetchone()[0] > 0)
    db.commit()
    return inserted


def summarize(db: sqlite3.Connection, cohort_id: str) -> dict[str, Any]:
    forecast_count = int(
        db.execute(
            "SELECT count(*) FROM expectation_forecasts WHERE cohort_id=?", (cohort_id,)
        ).fetchone()[0]
    )
    matured_count = int(
        db.execute(
            "SELECT count(*) FROM expectation_outcomes WHERE cohort_id=?", (cohort_id,)
        ).fetchone()[0]
    )
    pending_count = int(
        db.execute(
            """SELECT count(*) FROM expectation_forecasts f
               LEFT JOIN expectation_outcomes o USING(forecast_id)
               WHERE f.cohort_id=? AND o.forecast_id IS NULL""",
            (cohort_id,),
        ).fetchone()[0]
    )
    rows = []
    for row in db.execute(
        """SELECT currency,count(*),count(DISTINCT event_series_id),
                  min(training_episode_count),max(training_episode_count)
             FROM expectation_forecasts WHERE cohort_id=? GROUP BY currency
             ORDER BY currency""",
        (cohort_id,),
    ):
        rows.append(
            {
                "currency": row[0],
                "forecast_count": int(row[1]),
                "series_count": int(row[2]),
                "minimum_training_episodes": int(row[3]),
                "maximum_training_episodes": int(row[4]),
            }
        )
    errors = db.execute(
        """SELECT count(*),avg(absolute_error),avg(forecast_error)
             FROM expectation_outcomes WHERE cohort_id=?""",
        (cohort_id,),
    ).fetchone()
    return {
        "forecast_count": forecast_count,
        "pending_count": pending_count,
        "matured_count": matured_count,
        "currencies": rows,
        "currency_count": len(rows),
        "mean_absolute_error": float(errors[1]) if errors[1] is not None else None,
        "mean_forecast_error": float(errors[2]) if errors[2] is not None else None,
    }


def build_report(payload: Mapping[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# Internal Macro Expectation Baseline",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Research-only. These are internal persistence/drift baselines, not market consensus.",
        "They cannot populate the causal-consensus ledger, assign an FX side, promote, authorize, or trade.",
        "",
        f"- Source-native episodes known at cutoff: **{payload['source_episode_count']}**",
        f"- Forecasts / pending / matured: **{summary['forecast_count']} / {summary['pending_count']} / {summary['matured_count']}**",
        f"- Currencies with a baseline: **{summary['currency_count']}/21**",
        f"- New forecasts / outcomes this cycle: **{payload['new_forecasts']} / {payload['new_outcomes']}**",
        "- Causally captured market-consensus observations added: **0**",
        "",
        "| Currency | Series | Forecasts | Min training episodes | Max training episodes |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in summary["currencies"]:
        lines.append(
            f"| {row['currency']} | {row['series_count']} | {row['forecast_count']} | "
            f"{row['minimum_training_episodes']} | {row['maximum_training_episodes']} |"
        )
    lines += [
        "",
        "A forecast targets the first later distinct official release for the same currency and series.",
        "Every forecast binds its issue time, training episode IDs, training cutoff, training hash,",
        "algorithm cohort, and expiry. A later actual matures a separate immutable outcome row.",
        "",
    ]
    return "\n".join(lines)


def run_once(
    source_path: Path = SOURCE_DB,
    config_path: Path = CONFIG,
    database_path: Path = DATABASE,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
    observed: dt.datetime | None = None,
    code_path: Path | None = None,
) -> dict[str, Any]:
    if observed is None:
        observed, clock = normalized_observation_time(dt.datetime.now(UTC))
    else:
        observed = observed.astimezone(UTC)
        clock = {
            "source": "provided_replay_time",
            "trusted_for_prospective_evidence": True,
            "normalized": False,
        }
    config = read_json(config_path)
    cohort = contract(config, code_path)
    episodes = load_episodes(source_path, observed)
    db = open_database(database_path)
    db.execute(
        "INSERT OR IGNORE INTO cohort_contracts VALUES (?,?,?,?,?)",
        (
            cohort["cohort_id"],
            iso(observed),
            cohort["definition_sha256"],
            cohort["code_sha256"],
            json.dumps(cohort, sort_keys=True),
        ),
    )
    trusted = bool(clock.get("trusted_for_prospective_evidence"))
    new_outcomes = mature_forecasts(db, episodes, observed) if trusted else 0
    new_forecasts = issue_forecasts(db, episodes, observed, config, cohort) if trusted else 0
    summary = summarize(db, cohort["cohort_id"])
    integrity = db.execute("PRAGMA quick_check").fetchone()[0]
    db.close()
    payload = {
        "schema_version": 1,
        "generated_utc": iso(observed),
        "status": "ok" if trusted and integrity == "ok" else "blocked",
        "cohort": cohort,
        "observation_clock": clock,
        "source_episode_count": len(episodes),
        "new_forecasts": new_forecasts,
        "new_outcomes": new_outcomes,
        "summary": summary,
        "database_integrity": integrity,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "can_populate_market_consensus": False,
        "market_consensus_observations_added": 0,
        "supported_execution_decision": "no_trade",
    }
    atomic_text(output_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    atomic_text(report_path, build_report(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=SOURCE_DB)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--interval-sec", type=float, default=300)
    parser.add_argument("--duration-sec", type=float, default=604800)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    stop = time.monotonic() + args.duration_sec
    while True:
        run_once(args.source, args.config, args.database, args.output, args.report)
        if args.once or time.monotonic() >= stop:
            return 0
        time.sleep(min(args.interval_sec, max(0.0, stop - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
