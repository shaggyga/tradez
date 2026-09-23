#!/usr/bin/env python3
"""Map source-native numeric observations directly to executable FX movement.

News text is retained only as provenance.  This prospective shadow worker uses
official macro values (and causal consensus when it exists), records the exact
knowledge and quote cutoff, and matures signed currency-strength, magnitude,
cost-clearance, MFE/MAE, and timing targets.  It cannot trade or promote.
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
from pathlib import Path
from typing import Any, Mapping

from oanda_direct_source_simple_rules import source_rule
from oanda_local_news_sentiment import (
    COLLECTOR_COHORT_ID,
    COLLECTOR_CONTRACT_ID,
    OBSERVATION_TIME_CONTRACT_ID,
    normalized_observation_time,
    prospective_collector_provenance,
)
from oanda_worker_heartbeat import WorkerHeartbeat

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
MACRO = STATE / "macro_surprise_v1.sqlite"
QUOTES = STATE / "practice_007_market_quotes_v1.json"
RATES = ROOT / "config" / "rates_policy_repricing_v1.json"
TREASURY_DB = STATE / "us_treasury_yield_prospective_v1.sqlite"
DAILY_RATES = STATE / "official_daily_rate_context_v1.json"
INTERNAL_EXPECTATION_DB = STATE / "internal_macro_expectation_v1.sqlite"
OPPORTUNITY_DB = STATE / "executable_opportunity_prospective_v1.sqlite"
OPPORTUNITY_STATE = STATE / "executable_opportunity_prospective_v1.json"
OPPORTUNITY_MODEL_COHORT_ID = "executable_opportunity_ranking_v2_20260814"
RULES = ROOT / "config" / "direct_source_simple_rules_v1.json"
DB = STATE / "direct_source_response_v1.sqlite"
EDGE_DB = STATE / "edge_evidence_v1.sqlite"
OUTPUT = STATE / "direct_source_response_v1.json"
REPORT = DATA / "reports" / "direct_source_response" / "DIRECT_SOURCE_RESPONSE_CURRENT.md"
PROGRESS_HEARTBEAT = STATE / "direct_source_response_progress_heartbeat_v1.json"
PARENT_COHORT = "direct_source_response_v19_clock_v4_20260817"
# V7 preserved numeric zeroes from official releases rather than allowing a
# text-cleaning truthiness shortcut to erase them. V8 begins after the official
# Michigan report-listing regex was corrected. V9 begins with its 15-minute
# normal polling cadence. V10 adds the distinct official live-release page and
# a rule contract that explicitly abstains on its mixed activity/inflation
# channels. V11 gives source-contract repairs an immediate retry. V12 treats an
# empty URL history from a failed parser as unseeded. V13 invalidates stale HTTP
# validators on changed source contracts so repaired parsers receive a body.
COHORT = "direct_source_response_v20_clock_v4_provenance_20260817"
# Direct releases can be digested with a delay: today's retail-sales case was
# initially adverse at M5/M15 and expressed broad USD weakness later.  Preserve
# the original horizons and add M30/H2 prospectively so that response shape is
# measured rather than inferred from whichever horizon happens to look best.
HORIZONS = (60, 300, 900, 1800, 3600, 7200, 14400, 86400)
MAX_SOURCE_AGE_SEC = 300
MAX_QUOTE_AGE_SEC = 180
MAX_PAIR_LEGS = 3
MAX_SPREAD_PIPS = 5.0
DAILY_RATE_CLOCK_BOUND_COHORT_ID = (
    "official_daily_rate_context_v2_clock_v4_20260817"
)
OPPORTUNITY_HORIZONS = (300, 900, 1800)
MAX_OPPORTUNITY_AGE_SEC = 1800
_CENSUS_CACHE: dict[str, Any] | None = None
_CENSUS_REFRESH_MONOTONIC = 0.0
CENSUS_REFRESH_SEC = 3600


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_utc(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def iso(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat()


def finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def load_slow_treasury_context(path: Path) -> dict[str, Any]:
    """Return the latest immutable daily curve without implying intraday confirmation.

    A bootstrap/revision row is useful operational context but is not causal proof.
    Only a first-observed new yield date may become a prospective daily feature, and
    even then it remains distinct from the missing OIS/policy-repricing source.
    """
    unavailable = {
        "state": "source_not_connected", "causal_daily_feature": False,
        "intraday_rate_confirmation": False,
    }
    if not path.exists():
        return unavailable
    try:
        connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5)
        row = connection.execute(
            """SELECT observation_id,cohort_id,observed_utc,yield_date,
                      two_year_pct,ten_year_pct,curve_2s10s_bps,
                      observation_kind,bootstrap_current_view,prospective_eligible,
                      direction_policy
               FROM rate_observations
               ORDER BY yield_date DESC,observation_version DESC,rowid DESC LIMIT 1"""
        ).fetchone()
        connection.close()
    except sqlite3.Error as exc:
        return {**unavailable, "state": "unavailable", "error": str(exc)}
    if row is None:
        return {**unavailable, "state": "empty"}
    prospective = bool(row[9]) and str(row[7]) == "new_yield_date_first_observed"
    return {
        "state": (
            "prospective_daily_context_available" if prospective
            else "bootstrap_or_revision_context_only"
        ),
        "observation_id": row[0], "cohort_id": row[1],
        "observed_utc": row[2], "yield_date": row[3],
        "two_year_pct": float(row[4]), "ten_year_pct": float(row[5]),
        "curve_2s10s_bps": float(row[6]), "observation_kind": row[7],
        "bootstrap_current_view": bool(row[8]),
        "prospective_eligible": bool(row[9]),
        "direction_policy": row[10],
        "causal_daily_feature": prospective,
        "intraday_rate_confirmation": False,
        "contract": "daily_treasury_context_v1_never_intraday_repricing",
    }


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    _replace_with_retry(temporary, path)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    _replace_with_retry(temporary, path)


def _replace_with_retry(temporary: Path, path: Path) -> None:
    for attempt in range(8):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt == 7:
                temporary.unlink(missing_ok=True)
                raise
            time.sleep(min(0.4, 0.025 * (2**attempt)))


def load_quotes(path: Path, observed: dt.datetime) -> dict[str, dict[str, Any]]:
    raw = read_json(path).get("quotes") or {}
    result = {}
    for instrument, quote in raw.items():
        if not isinstance(quote, Mapping):
            continue
        bid, ask = finite(quote.get("bid")), finite(quote.get("ask"))
        stamp = parse_utc(quote.get("time") or quote.get("quote_time_utc"))
        if bid is None or ask is None or bid <= 0 or ask <= bid or stamp is None:
            continue
        pip = finite(quote.get("pip")) or (0.01 if str(instrument).endswith("_JPY") else 0.0001)
        age = (observed - stamp).total_seconds()
        spread = (ask - bid) / pip
        result[str(instrument)] = {
            "bid": bid, "ask": ask, "mid": (bid + ask) / 2, "pip": pip,
            "spread_pips": spread, "time": iso(stamp),
            "fresh": -5 <= age <= MAX_QUOTE_AGE_SEC,
        }
    return result


def open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS source_observations (
          source_observation_id TEXT PRIMARY KEY, cohort_id TEXT NOT NULL,
          source_row_id INTEGER NOT NULL, release_key TEXT NOT NULL,
          source_id TEXT NOT NULL, source_event_id TEXT NOT NULL,
          event_series_id TEXT NOT NULL, currency TEXT NOT NULL,
          known_utc TEXT NOT NULL, observed_utc TEXT NOT NULL,
          actual_value REAL NOT NULL, previous_value REAL, revised_previous_value REAL,
          change_from_previous REAL, consensus_value REAL, surprise_raw REAL,
          standardized_surprise REAL, consensus_causal INTEGER NOT NULL,
          rates_state TEXT NOT NULL, eligibility_state TEXT NOT NULL,
          feature_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS response_targets (
          target_id TEXT PRIMARY KEY, source_observation_id TEXT NOT NULL,
          instrument TEXT NOT NULL, currency_orientation INTEGER NOT NULL,
          horizon_sec INTEGER NOT NULL, entry_time TEXT NOT NULL,
          entry_bid REAL NOT NULL, entry_ask REAL NOT NULL, entry_mid REAL NOT NULL,
          pip REAL NOT NULL, entry_spread_pips REAL NOT NULL,
          status TEXT NOT NULL, path_samples INTEGER NOT NULL,
          max_currency_strength_pips REAL NOT NULL,
          max_currency_weakness_pips REAL NOT NULL,
          first_cost_clear_sec REAL, outcome_quote_time TEXT,
          currency_return_pips REAL, absolute_move_pips REAL,
          strengthening_after_cost_pips REAL, weakening_after_cost_pips REAL,
          best_after_cost_pips REAL, movement_cleared_cost INTEGER,
          payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_response_pending ON response_targets(status,horizon_sec,entry_time);
        """
    )
    return db


def causal_consensus(db: sqlite3.Connection, release_key: str, known: dt.datetime) -> tuple[float | None, bool]:
    row = db.execute(
        """SELECT consensus_value,captured_utc,source_timestamp_utc
           FROM macro_consensus_observations WHERE release_key=?
           ORDER BY captured_utc DESC LIMIT 1""", (release_key,),
    ).fetchone()
    if row is None:
        return None, False
    captured, source_time = parse_utc(row[1]), parse_utc(row[2])
    valid = captured is not None and source_time is not None and captured <= known and source_time <= known
    return (finite(row[0]) if valid else None), valid


def internal_expectation_for_release(
    path: Path | None,
    currency: str,
    event_series_id: str,
    known: dt.datetime,
) -> dict[str, Any]:
    """Return a frozen pre-release internal baseline without calling it consensus."""

    unavailable = {
        "available": False,
        "market_consensus": False,
        "causal_market_consensus": False,
        "direction_policy": "abstain_until_response_is_independently_calibrated",
    }
    if path is None or not path.exists() or not currency or not event_series_id:
        return unavailable
    try:
        db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5)
        row = db.execute(
            """SELECT forecast_id,cohort_id,issued_utc,expires_utc,expected_value,
                      model_name,training_episode_count,training_fingerprint_sha256
                 FROM expectation_forecasts
                WHERE currency=? AND event_series_id=?
                  AND issued_utc<? AND expires_utc>=?
                ORDER BY issued_utc DESC,forecast_id DESC LIMIT 1""",
            (currency, event_series_id, iso(known), iso(known)),
        ).fetchone()
        db.close()
    except sqlite3.Error as exc:
        return {**unavailable, "state": "unavailable", "error": str(exc)}
    if row is None:
        return {**unavailable, "state": "no_pre_release_internal_expectation"}
    issued = parse_utc(row[2])
    expires = parse_utc(row[3])
    expected = finite(row[4])
    if issued is None or expires is None or expected is None or not (issued < known <= expires):
        return {**unavailable, "state": "invalid_time_or_value"}
    return {
        "available": True,
        "state": "pre_release_internal_expectation_available",
        "forecast_id": str(row[0]),
        "cohort_id": str(row[1]),
        "issued_utc": iso(issued),
        "expires_utc": iso(expires),
        "expected_value": expected,
        "model_name": str(row[5]),
        "training_episode_count": int(row[6]),
        "training_fingerprint_sha256": str(row[7]),
        "market_consensus": False,
        "causal_market_consensus": False,
        "direction_policy": "abstain_until_response_is_independently_calibrated",
    }


def select_pairs(currency: str, quotes: Mapping[str, Mapping[str, Any]]) -> list[tuple[str, Mapping[str, Any], int]]:
    rows = []
    for instrument, quote in quotes.items():
        parts = instrument.split("_")
        if len(parts) != 2 or currency not in parts or not quote.get("fresh"):
            continue
        spread = finite(quote.get("spread_pips")) or 999999
        if spread > MAX_SPREAD_PIPS:
            continue
        orientation = 1 if parts[0] == currency else -1
        rows.append((spread, instrument, quote, orientation))
    return [(instrument, quote, orientation) for _, instrument, quote, orientation in sorted(rows)[:MAX_PAIR_LEGS]]


def load_pre_event_opportunities(
    path: Path | None,
    known: dt.datetime,
    required_cohort_id: str,
) -> dict[tuple[str, int], dict[str, Any]]:
    """Load only v2 forecasts that were actually issued before the release.

    The release-time quote may already contain the reaction, so this ranker
    deliberately uses a previously issued forecast rather than computing a new
    post-release feature vector.  Old v1 forecasts and later rows are excluded.
    """
    if path is None or not path.exists() or not required_cohort_id:
        return {}
    try:
        db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """SELECT forecast_id,cohort_id,issued_at_utc,instrument,horizon_sec,
                      predicted_clear_probability,predicted_magnitude_pips,
                      modeled_entry_cost_pips,predicted_direction
                 FROM forecasts
                WHERE cohort_id LIKE ? AND issued_at_utc<=? AND issued_at_utc>=?
                  AND horizon_sec IN (300,900,1800)
                ORDER BY issued_at_utc DESC,forecast_id""",
            (
                required_cohort_id,
                iso(known),
                iso(known - dt.timedelta(seconds=MAX_OPPORTUNITY_AGE_SEC)),
            ),
        ).fetchall()
        db.close()
    except sqlite3.Error:
        return {}
    result: dict[tuple[str, int], dict[str, Any]] = {}
    for row in rows:
        key = (str(row["instrument"]), int(row["horizon_sec"]))
        if key in result:
            continue
        result[key] = dict(row)
    return result


def select_opportunity_pairs(
    currency: str,
    quotes: Mapping[str, Mapping[str, Any]],
    opportunities: Mapping[tuple[str, int], Mapping[str, Any]],
    horizon: int,
) -> list[tuple[str, Mapping[str, Any], int, Mapping[str, Any]]]:
    rows = []
    for instrument, quote in quotes.items():
        parts = instrument.split("_")
        model = opportunities.get((instrument, int(horizon)))
        if len(parts) != 2 or currency not in parts or not quote.get("fresh") or not model:
            continue
        spread = finite(quote.get("spread_pips")) or 999999
        if spread > MAX_SPREAD_PIPS:
            continue
        orientation = 1 if parts[0] == currency else -1
        predicted_cost = max(finite(model.get("modeled_entry_cost_pips")) or spread, spread, 1e-9)
        magnitude = finite(model.get("predicted_magnitude_pips")) or 0.0
        clear = finite(model.get("predicted_clear_probability")) or 0.0
        rows.append((-clear, -(magnitude / predicted_cost), spread, instrument, quote, orientation, model))
    return [
        (instrument, quote, orientation, model)
        for _, _, _, instrument, quote, orientation, model in sorted(rows)[:MAX_PAIR_LEGS]
    ]


def ingest_macro(source: sqlite3.Connection, target: sqlite3.Connection, quotes: Mapping[str, Mapping[str, Any]], observed: dt.datetime, rates_state: str, rule_config: Mapping[str, Any] | None = None, opportunity_path: Path | None = None, opportunity_cohort_id: str = "", internal_expectation_path: Path | None = INTERNAL_EXPECTATION_DB) -> tuple[int, int]:
    seen = {row[0] for row in target.execute("SELECT source_observation_id FROM source_observations")}
    seen_releases = {row[0] for row in target.execute("SELECT DISTINCT release_key FROM source_observations")}
    rows = source.execute(
        """SELECT row_id,revision_id,release_key,source_event_id,causal_known_utc,
                  event_series_id,event_name,currencies_json,reference_period,unit,
                  actual_value,previous_value,revised_previous_value,source_id,
                  source_name,source_url,source_verified,source_direct,payload_sha256,
                  consensus_value,known_before_recorded_timestamp,payload_json
           FROM macro_release_revisions WHERE actual_value IS NOT NULL
           ORDER BY row_id"""
    ).fetchall()
    observations = targets = 0
    for row in rows:
        observation_id = str(row[1])
        if observation_id in seen:
            continue
        known = parse_utc(row[4])
        if known is None:
            continue
        # The macro ledger already performs the causal series+schedule join and
        # records the effective consensus on this exact immutable release
        # revision. Re-querying the import table by release_key alone dropped
        # valid provider snapshots whose provider identity hash differed from
        # the official-release identity hash.
        consensus = finite(row[19])
        consensus_valid = bool(row[20]) and consensus is not None
        actual, previous, revised = finite(row[10]), finite(row[11]), finite(row[12])
        if actual is None:
            continue
        effective_previous = revised if revised is not None else previous
        change = actual - effective_previous if effective_previous is not None else None
        surprise = actual - consensus if consensus_valid and consensus is not None else None
        try:
            currencies = [str(x).upper() for x in json.loads(row[7] or "[]")]
        except (TypeError, ValueError, json.JSONDecodeError):
            currencies = []
        currency = currencies[0] if len(currencies) == 1 else ""
        internal_expectation = internal_expectation_for_release(
            internal_expectation_path,
            currency,
            str(row[5] or ""),
            known,
        )
        internal_error = (
            actual - float(internal_expectation["expected_value"])
            if internal_expectation.get("available")
            else None
        )
        try:
            source_payload = json.loads(row[21] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            source_payload = {}
        source_bootstrap = bool(source_payload.get("source_listing_bootstrap"))
        publication_time_inferred = bool(source_payload.get("published_time_inferred"))
        provenance_bound = prospective_collector_provenance(source_payload)
        source_age = (observed - known).total_seconds()
        duplicate_release = str(row[2]) in seen_releases
        eligible = (
            bool(row[16]) and bool(row[17]) and currency != ""
            and provenance_bound
            and not source_bootstrap and not publication_time_inferred
            and not duplicate_release
            and -5 <= source_age <= MAX_SOURCE_AGE_SEC
            and bool(select_pairs(currency, quotes))
        )
        eligibility = "prospective_entry_ready" if eligible else (
            "unbound_source_clock_provenance" if not provenance_bound else
            "bootstrap_context_not_prospective" if source_bootstrap else
            "inferred_publication_clock_not_prospective" if publication_time_inferred else
            "duplicate_release_revision_no_new_episode" if duplicate_release else
            "stale_at_first_observation" if source_age > MAX_SOURCE_AGE_SEC else "missing_fresh_pair_quote"
        )
        feature = {
            "source_observation_id": observation_id, "source_row_id": row[0],
            "release_key": row[2], "source_event_id": row[3], "known_utc": iso(known),
            "event_series_id": row[5], "event_name": row[6], "currency": currency,
            "reference_period": row[8], "unit": row[9], "actual_value": actual,
            "previous_value": previous, "revised_previous_value": revised,
            "change_from_previous": change, "consensus_value": consensus,
            "surprise_raw": surprise, "consensus_causal": consensus_valid,
            "rates_state": rates_state, "source_id": row[13], "source_name": row[14],
            "source_url": row[15], "payload_sha256": row[18],
            "source_listing_bootstrap": source_bootstrap,
            "published_time_inferred": publication_time_inferred,
            "source_contract_id": source_payload.get("source_contract_id"),
            "source_cohort_id": source_payload.get("source_cohort_id"),
            "collector_contract_id": source_payload.get("collector_contract_id"),
            "collector_cohort_id": source_payload.get("collector_cohort_id"),
            "observation_time_contract_id": source_payload.get(
                "observation_time_contract_id"
            ),
            "observation_clock_trusted": (
                source_payload.get("observation_clock_trusted") is True
            ),
            "source_provenance_bound": provenance_bound,
            "internal_expectation": internal_expectation,
            "internal_expectation_error": internal_error,
            "internal_expectation_is_market_consensus": False,
            "semantic_direction_required": False, "news_is_provenance_only": True,
        }
        feature["simple_direction_rule"] = source_rule(feature, rule_config or {})
        feature["magnitude_opportunity_rule"] = {
            "rule_contract_id": (rule_config or {}).get("rule_contract_id"),
            "eligible_horizon_sec": list((rule_config or {}).get("preferred_horizons_sec") or []),
            "directional_action": "abstain",
            "research_only": True,
        }
        target.execute(
            "INSERT INTO source_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (observation_id, COHORT, row[0], row[2], row[13], row[3], row[5], currency,
             iso(known), iso(observed), actual, previous, revised, change, consensus,
             surprise, None, int(consensus_valid), rates_state, eligibility,
             json.dumps(feature, sort_keys=True)),
        )
        observations += 1
        seen_releases.add(str(row[2]))
        if not eligible:
            continue
        opportunities = load_pre_event_opportunities(
            opportunity_path, known, opportunity_cohort_id
        )
        for instrument, quote, orientation in select_pairs(currency, quotes):
            for horizon in HORIZONS:
                target_id = f"{observation_id}|{instrument}|{horizon}"
                payload = {**feature, "instrument": instrument, "currency_orientation": orientation,
                           "horizon_sec": horizon, "entry_quote": dict(quote),
                           "selection_arm": "lowest_spread_baseline",
                           "selection_cohort_id": COHORT}
                target.execute(
                    """INSERT OR IGNORE INTO response_targets VALUES
                       (?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,?)""",
                    (target_id, observation_id, instrument, orientation, horizon, iso(observed),
                     quote["bid"], quote["ask"], quote["mid"], quote["pip"],
                     quote["spread_pips"], "pending", json.dumps(payload, sort_keys=True)),
                )
                targets += int(target.execute("SELECT changes()").fetchone()[0] > 0)
        for horizon in OPPORTUNITY_HORIZONS:
            for rank, (instrument, quote, orientation, model) in enumerate(
                select_opportunity_pairs(currency, quotes, opportunities, horizon), start=1
            ):
                target_id = f"{observation_id}|opportunity_ranked|{instrument}|{horizon}"
                payload = {
                    **feature,
                    "instrument": instrument,
                    "currency_orientation": orientation,
                    "horizon_sec": horizon,
                    "entry_quote": dict(quote),
                    "selection_arm": "pre_event_opportunity_ranked",
                    "selection_cohort_id": f"direct_source_opportunity_ranked_v1_20260814_h{horizon}",
                    "opportunity_rank": rank,
                    "opportunity_forecast_id": model.get("forecast_id"),
                    "opportunity_cohort_id": model.get("cohort_id"),
                    "opportunity_issued_at_utc": model.get("issued_at_utc"),
                    "opportunity_predicted_clear_probability": model.get("predicted_clear_probability"),
                    "opportunity_predicted_magnitude_pips": model.get("predicted_magnitude_pips"),
                    "opportunity_predicted_direction": model.get("predicted_direction"),
                    "direction_source": "official_source_native_numeric_rule",
                    "ranking_source": "pre_event_cost_clearance_magnitude_model",
                }
                target.execute(
                    """INSERT OR IGNORE INTO response_targets VALUES
                       (?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,?)""",
                    (target_id, observation_id, instrument, orientation, horizon, iso(observed),
                     quote["bid"], quote["ask"], quote["mid"], quote["pip"],
                     quote["spread_pips"], "pending", json.dumps(payload, sort_keys=True)),
                )
                targets += int(target.execute("SELECT changes()").fetchone()[0] > 0)
    target.commit()
    return observations, targets


def invalidate_bootstrap_release_targets(target: sqlite3.Connection) -> tuple[int, int]:
    """Retire any target opened from a first-seen mutable release snapshot.

    Older direct-response cohorts could observe the numeric values before the
    macro ledger preserved the upstream bootstrap flag.  Once a later immutable
    revision proves that release key was bootstrap context, invalidate every
    observation and target for that release key without deleting evidence.
    """

    bootstrap_release_keys: set[str] = set()
    for release_key, feature_text in target.execute(
        "SELECT release_key,feature_json FROM source_observations"
    ):
        try:
            feature = json.loads(feature_text or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if bool(feature.get("source_listing_bootstrap")):
            bootstrap_release_keys.add(str(release_key))
    invalidated_observations = invalidated_targets = 0
    for release_key in sorted(bootstrap_release_keys):
        observation_ids = [
            str(row[0]) for row in target.execute(
                "SELECT source_observation_id FROM source_observations WHERE release_key=?",
                (release_key,),
            )
        ]
        cursor = target.execute(
            """UPDATE source_observations
               SET eligibility_state='bootstrap_context_not_prospective'
               WHERE release_key=?
                 AND eligibility_state<>'bootstrap_context_not_prospective'""",
            (release_key,),
        )
        invalidated_observations += max(0, int(cursor.rowcount))
        if observation_ids:
            placeholders = ",".join("?" for _ in observation_ids)
            cursor = target.execute(
                f"""UPDATE response_targets
                    SET status='invalid_bootstrap_context'
                    WHERE source_observation_id IN ({placeholders})
                      AND status<>'invalid_bootstrap_context'""",
                observation_ids,
            )
            invalidated_targets += max(0, int(cursor.rowcount))
    target.commit()
    return invalidated_observations, invalidated_targets


def invalidate_inferred_publication_targets(target: sqlite3.Connection) -> tuple[int, int]:
    """Retire targets whose only publication clock was inferred at ingestion.

    A first-seen or retrieval timestamp can retain a current snapshot for
    provenance, but it cannot prove when the value became public.  Preserve all
    rows while preventing those snapshots from contributing prospective
    response evidence.
    """

    observation_ids: list[str] = []
    for observation_id, feature_text in target.execute(
        "SELECT source_observation_id,feature_json FROM source_observations"
    ):
        try:
            feature = json.loads(feature_text or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if bool(feature.get("published_time_inferred")):
            observation_ids.append(str(observation_id))
    if not observation_ids:
        return 0, 0
    placeholders = ",".join("?" for _ in observation_ids)
    cursor = target.execute(
        f"""UPDATE source_observations
            SET eligibility_state='inferred_publication_clock_not_prospective'
            WHERE source_observation_id IN ({placeholders})
              AND eligibility_state<>'inferred_publication_clock_not_prospective'""",
        observation_ids,
    )
    invalidated_observations = max(0, int(cursor.rowcount))
    cursor = target.execute(
        f"""UPDATE response_targets
            SET status='invalid_inferred_publication_clock'
            WHERE source_observation_id IN ({placeholders})
              AND status<>'invalid_inferred_publication_clock'""",
        observation_ids,
    )
    invalidated_targets = max(0, int(cursor.rowcount))
    target.commit()
    return invalidated_observations, invalidated_targets


def ingest_daily_rate_context(
    payload: Mapping[str, Any], target: sqlite3.Connection,
    quotes: Mapping[str, Mapping[str, Any]], observed: dt.datetime,
) -> tuple[int, int]:
    """Open H4/H24 shadow targets only from fresh prospective daily rates.

    Bootstrap/revision rows are retained as unavailable provenance. A daily
    yield change is not treated as intraday OIS confirmation. The sign rule is
    an explicit research arm with an exact flipped negative control.
    """
    seen = {row[0] for row in target.execute("SELECT source_observation_id FROM source_observations")}
    observations = targets = 0
    for currency, raw in (payload.get("currencies") or {}).items():
        if not isinstance(raw, Mapping) or not raw.get("observation_id"):
            continue
        observation_id = "daily_rate_context:" + str(raw["observation_id"])
        if observation_id in seen:
            continue
        known = parse_utc(raw.get("observed_utc"))
        change_bps = finite(raw.get("change_bps_1d"))
        actual = finite(raw.get("rate_pct"))
        if known is None or actual is None:
            continue
        age = (observed - known).total_seconds()
        pair_rows = select_pairs(str(currency), quotes)
        prospective = bool(raw.get("prospective_eligible"))
        provenance_bound = bool(
            raw.get("observation_clock_trusted") is True
            and str(raw.get("observation_time_contract_id") or "")
            == OBSERVATION_TIME_CONTRACT_ID
            and str(raw.get("collector_cohort_id") or "")
            == DAILY_RATE_CLOCK_BOUND_COHORT_ID
        )
        ready = (
            prospective and provenance_bound
            and change_bps is not None and abs(change_bps) > 0
            and -5 <= age <= MAX_SOURCE_AGE_SEC and bool(pair_rows)
        )
        eligibility = (
            "prospective_entry_ready" if ready else
            "unbound_daily_rate_clock_provenance" if not provenance_bound else
            "not_prospective_source" if not prospective else
            "missing_rate_change" if change_bps is None else
            "stale_at_first_observation" if age > MAX_SOURCE_AGE_SEC else
            "missing_fresh_pair_quote"
        )
        direction = "strengthen" if change_bps is not None and change_bps > 0 else (
            "weaken" if change_bps is not None and change_bps < 0 else "abstain"
        )
        feature = {
            "source_observation_id": observation_id,
            "source_row_id": -1,
            "release_key": "daily_rate:" + str(raw["observation_id"]),
            "source_event_id": str(raw["observation_id"]),
            "known_utc": iso(known),
            "event_series_id": "official_2y_yield_change",
            "event_name": "official daily 2-year yield change",
            "currency": str(currency), "reference_period": str(raw.get("rate_date") or ""),
            "unit": "percent_per_annum", "actual_value": actual,
            "previous_value": actual - change_bps / 100.0 if change_bps is not None else None,
            "change_from_previous": change_bps / 100.0 if change_bps is not None else None,
            "change_bps_1d": change_bps, "consensus_value": None,
            "surprise_raw": None, "consensus_causal": False,
            "rates_state": "official_daily_context_not_intraday_confirmation",
            "source_id": str(raw.get("source_id") or ""),
            "source_name": str(raw.get("provider") or ""),
            "source_url": "", "payload_sha256": str(raw.get("observation_id")),
            "semantic_direction_required": False, "news_is_provenance_only": True,
            "intraday_rate_confirmation": False,
            "observation_time_contract_id": str(
                raw.get("observation_time_contract_id") or ""
            ),
            "observation_clock_trusted": (
                raw.get("observation_clock_trusted") is True
            ),
            "collector_cohort_id": str(raw.get("collector_cohort_id") or ""),
            "source_provenance_bound": provenance_bound,
            "simple_direction_rule": {
                "direction": direction,
                "basis": "higher_daily_2y_yield_strengthens_currency_shadow_only",
                "research_only": True,
            },
            "magnitude_opportunity_rule": {
                "eligible_horizon_sec": [14400, 86400],
                "directional_action": "abstain", "research_only": True,
            },
        }
        target.execute(
            "INSERT OR IGNORE INTO source_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                observation_id, COHORT, -1, feature["release_key"], feature["source_id"],
                feature["source_event_id"], feature["event_series_id"], str(currency),
                iso(known), iso(observed), actual, feature["previous_value"], None,
                feature["change_from_previous"], None, None, None, 0,
                feature["rates_state"], eligibility, json.dumps(feature, sort_keys=True),
            ),
        )
        observations += int(target.execute("SELECT changes()").fetchone()[0] > 0)
        if not ready:
            continue
        for instrument, quote, orientation in pair_rows:
            for horizon in (14400, 86400):
                target_id = f"{observation_id}|{instrument}|{horizon}"
                target_payload = {
                    **feature, "instrument": instrument, "currency_orientation": orientation,
                    "horizon_sec": horizon, "entry_quote": dict(quote),
                }
                target.execute(
                    """INSERT OR IGNORE INTO response_targets VALUES
                       (?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,?)""",
                    (
                        target_id, observation_id, instrument, orientation, horizon, iso(observed),
                        quote["bid"], quote["ask"], quote["mid"], quote["pip"],
                        quote["spread_pips"], "pending", json.dumps(target_payload, sort_keys=True),
                    ),
                )
                targets += int(target.execute("SELECT changes()").fetchone()[0] > 0)
    target.commit()
    return observations, targets


def update_targets(db: sqlite3.Connection, quotes: Mapping[str, Mapping[str, Any]], observed: dt.datetime) -> tuple[int, int]:
    pending = db.execute(
        """SELECT target_id,instrument,currency_orientation,horizon_sec,entry_time,
                  entry_bid,entry_ask,entry_mid,pip,entry_spread_pips,path_samples,
                  max_currency_strength_pips,max_currency_weakness_pips,first_cost_clear_sec,payload_json
           FROM response_targets WHERE status='pending'"""
    ).fetchall()
    sampled = matured = 0
    for row in pending:
        quote = quotes.get(str(row[1])); entry_time = parse_utc(row[4])
        if not quote or not quote.get("fresh") or entry_time is None:
            continue
        elapsed = (parse_utc(quote["time"]) - entry_time).total_seconds()
        if elapsed < 0:
            continue
        orientation, pip = int(row[2]), float(row[8])
        currency_move = orientation * (float(quote["mid"]) - float(row[7])) / pip
        max_strength = max(float(row[11]), currency_move)
        max_weakness = min(float(row[12]), currency_move)
        strengthening = ((float(quote["bid"]) - float(row[6])) / pip if orientation == 1 else (float(row[5]) - float(quote["ask"])) / pip)
        weakening = ((float(row[5]) - float(quote["ask"])) / pip if orientation == 1 else (float(quote["bid"]) - float(row[6])) / pip)
        first_clear = row[13]
        if first_clear is None and max(strengthening, weakening) > 0:
            first_clear = elapsed
        db.execute(
            """UPDATE response_targets SET path_samples=?,max_currency_strength_pips=?,
               max_currency_weakness_pips=?,first_cost_clear_sec=? WHERE target_id=?""",
            (int(row[10]) + 1, max_strength, max_weakness, first_clear, row[0]),
        )
        sampled += 1
        horizon = int(row[3])
        if elapsed < horizon or abs(elapsed - horizon) > 300:
            continue
        best = max(strengthening, weakening)
        try: payload = json.loads(row[14] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError): payload = {}
        payload["outcome"] = {
            "quote_time": quote["time"], "currency_return_pips": currency_move,
            "absolute_move_pips": abs(currency_move),
            "strengthening_after_cost_pips": strengthening,
            "weakening_after_cost_pips": weakening, "best_after_cost_pips": best,
            "movement_cleared_cost": best > 0, "max_currency_strength_pips": max_strength,
            "max_currency_weakness_pips": max_weakness, "first_cost_clear_sec": first_clear,
        }
        direction = str((payload.get("simple_direction_rule") or {}).get("direction") or "abstain")
        if direction in {"strengthen", "weaken"}:
            rule_net = strengthening if direction == "strengthen" else weakening
            flipped_net = weakening if direction == "strengthen" else strengthening
            payload["outcome"]["simple_rule_after_cost_pips"] = rule_net
            payload["outcome"]["simple_rule_direction_hit"] = currency_move * (1 if direction == "strengthen" else -1) > 0
            payload["outcome"]["flipped_negative_control_pips"] = flipped_net
        db.execute(
            """UPDATE response_targets SET status='matured',outcome_quote_time=?,
               currency_return_pips=?,absolute_move_pips=?,strengthening_after_cost_pips=?,
               weakening_after_cost_pips=?,best_after_cost_pips=?,movement_cleared_cost=?,
               payload_json=? WHERE target_id=?""",
            (quote["time"], currency_move, abs(currency_move), strengthening, weakening,
             best, int(best > 0), json.dumps(payload, sort_keys=True), row[0]),
        )
        matured += 1
    db.commit()
    return sampled, matured


def summarize(
    db: sqlite3.Connection, cohort_id: str = COHORT
) -> dict[str, Any]:
    # Preserve every source revision for provenance but ensure identical
    # collector revisions never become independent market episodes.
    for release_key, observation_id in db.execute(
        """SELECT release_key,source_observation_id FROM source_observations s
           WHERE s.cohort_id=?
             AND source_row_id > (
               SELECT min(source_row_id) FROM source_observations x
               WHERE x.release_key=s.release_key AND x.cohort_id=s.cohort_id
             )""",
        (cohort_id,),
    ).fetchall():
        db.execute(
            """UPDATE source_observations
               SET eligibility_state='duplicate_release_revision_no_new_episode'
               WHERE source_observation_id=?
                 AND eligibility_state NOT IN (
                   'prospective_entry_ready',
                   'bootstrap_context_not_prospective',
                   'inferred_publication_clock_not_prospective'
                 )""",
            (observation_id,),
        )
    db.commit()
    counts = dict(db.execute(
        "SELECT eligibility_state,count(*) FROM source_observations "
        "WHERE cohort_id=? GROUP BY 1", (cohort_id,)
    ).fetchall())
    targets = dict(db.execute(
        "SELECT t.status,count(*) FROM response_targets t "
        "JOIN source_observations s USING(source_observation_id) "
        "WHERE s.cohort_id=? GROUP BY 1", (cohort_id,)
    ).fetchall())
    baselines = []
    for row in db.execute(
        """SELECT s.event_series_id,s.currency,t.horizon_sec,count(*) n,
                  avg(t.currency_return_pips),avg(t.absolute_move_pips),
                  avg(t.best_after_cost_pips),avg(t.movement_cleared_cost)
           FROM response_targets t JOIN source_observations s USING(source_observation_id)
           WHERE t.status='matured' AND s.cohort_id=?
           GROUP BY 1,2,3 ORDER BY n DESC""",
        (cohort_id,),
    ):
        baselines.append({"event_series_id": row[0], "currency": row[1], "horizon_sec": row[2],
                          "n": row[3], "mean_currency_return_pips": row[4],
                          "mean_absolute_move_pips": row[5], "mean_best_after_cost_pips": row[6],
                          "cost_clear_rate": row[7], "proof_eligible": row[3] >= 30})
    episodes = db.execute(
        "SELECT count(DISTINCT release_key) FROM source_observations "
        "WHERE cohort_id=?", (cohort_id,)
    ).fetchone()[0]
    simple_rule = []
    for arm, horizon, n, clears, avg_abs, avg_rule, avg_flip in db.execute(
        """SELECT coalesce(json_extract(t.payload_json,'$.selection_arm'),'lowest_spread_baseline'),
                  horizon_sec,count(*),avg(movement_cleared_cost),avg(absolute_move_pips),
                  avg(json_extract(t.payload_json,'$.outcome.simple_rule_after_cost_pips')),
                  avg(json_extract(t.payload_json,'$.outcome.flipped_negative_control_pips'))
           FROM response_targets t
           JOIN source_observations s USING(source_observation_id)
           WHERE t.status='matured' AND t.horizon_sec IN (300,900,1800)
             AND s.cohort_id=?
           GROUP BY 1,t.horizon_sec ORDER BY 1,t.horizon_sec""",
        (cohort_id,),
    ):
        simple_rule.append({"selection_arm":arm,"horizon_sec":horizon,"matured_n":n,"magnitude_cost_clear_rate":clears,
                            "mean_absolute_move_pips":avg_abs,"mean_direction_rule_after_cost_pips":avg_rule,
                            "mean_flipped_negative_control_pips":avg_flip,"proof_eligible":False})
    return {"source_observation_states": counts, "independent_release_episodes": episodes,
            "target_states": targets, "baselines": baselines,"simple_rule_shadow":simple_rule}


def lineage_census(db: sqlite3.Connection) -> dict[str, Any]:
    return {
        "source_observations_by_cohort": {
            str(cohort): int(count)
            for cohort, count in db.execute(
                "SELECT cohort_id,count(*) FROM source_observations GROUP BY 1"
            )
        },
        "targets_by_cohort": {
            str(cohort): int(count)
            for cohort, count in db.execute(
                """SELECT s.cohort_id,count(*) FROM response_targets t
                   JOIN source_observations s USING(source_observation_id)
                   GROUP BY 1"""
            )
        },
    }


def cohort_progress(db: sqlite3.Connection, cohort_id: str) -> dict[str, int]:
    row = db.execute(
        """SELECT coalesce(sum(t.path_samples),0),
                  coalesce(sum(CASE WHEN t.status='matured' THEN 1 ELSE 0 END),0)
           FROM response_targets t
           JOIN source_observations s USING(source_observation_id)
           WHERE s.cohort_id=?""",
        (cohort_id,),
    ).fetchone()
    invalid = dict(
        db.execute(
            """SELECT eligibility_state,count(*) FROM source_observations
               WHERE cohort_id=? AND eligibility_state IN (
                 'bootstrap_context_not_prospective',
                 'inferred_publication_clock_not_prospective'
               ) GROUP BY eligibility_state""",
            (cohort_id,),
        )
    )
    return {
        "path_samples": int(row[0] or 0),
        "matured_targets": int(row[1] or 0),
        "bootstrap_invalidated_observations": int(
            invalid.get("bootstrap_context_not_prospective", 0)
        ),
        "inferred_invalidated_observations": int(
            invalid.get("inferred_publication_clock_not_prospective", 0)
        ),
    }


def canonical_target_census(path: Path = EDGE_DB) -> dict[str, Any]:
    global _CENSUS_CACHE, _CENSUS_REFRESH_MONOTONIC
    if not path.exists():
        return {"state": "missing", "row_count": 0}
    now = time.monotonic()
    if _CENSUS_CACHE is None:
        previous = read_json(OUTPUT).get("canonical_multi_target_census")
        # Older snapshots contain aggregate counts but no durable high-water.
        # They cannot support an exact incremental refresh, so bootstrap them
        # once below instead of treating them as a valid cache.
        if (
            isinstance(previous, Mapping)
            and previous.get("row_count") is not None
            and previous.get("highwater_rowid") is not None
            and previous.get("highwater_anchor_sha256") is not None
            and previous.get("database_file_id") is not None
        ):
            _CENSUS_CACHE = dict(previous)
            # A persisted cache may predate rows committed while the worker was
            # stopped. Verify/extend it immediately; the check is a max(rowid)
            # lookup, not a history scan.
            _CENSUS_REFRESH_MONOTONIC = 0.0
    if _CENSUS_CACHE is not None and now - _CENSUS_REFRESH_MONOTONIC < CENSUS_REFRESH_SEC:
        return {**_CENSUS_CACHE, "cache_state": "cached", "refresh_interval_sec": CENSUS_REFRESH_SEC}
    try:
        db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5)
        db.execute("PRAGMA query_only=ON")
        stat = path.stat()
        file_id = f"{int(stat.st_dev)}:{int(stat.st_ino)}"
        trigger_names = {
            str(row[0])
            for row in db.execute(
                """SELECT name FROM sqlite_master
                   WHERE type='trigger' AND tbl_name='canonical_economic_outcome_labels'"""
            )
        }
        immutable = {
            "canonical_economic_labels_no_update",
            "canonical_economic_labels_no_delete",
        }.issubset(trigger_names)
        maximum_rowid = int(
            db.execute(
                "SELECT coalesce(max(rowid),0) FROM canonical_economic_outcome_labels"
            ).fetchone()[0]
            or 0
        )

        previous_highwater = int(
            (_CENSUS_CACHE or {}).get("highwater_rowid") or 0
        )
        can_extend = bool(
            immutable
            and _CENSUS_CACHE is not None
            and str(_CENSUS_CACHE.get("database_file_id") or "") == file_id
            and maximum_rowid >= previous_highwater
        )
        if can_extend and previous_highwater:
            anchor = db.execute(
                """SELECT event_id,horizon_sec,label_json
                   FROM canonical_economic_outcome_labels WHERE rowid=?""",
                (previous_highwater,),
            ).fetchone()
            anchor_sha256 = hashlib.sha256(
                json.dumps(list(anchor or ()), separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            can_extend = bool(
                anchor is not None
                and anchor_sha256
                == str(_CENSUS_CACHE.get("highwater_anchor_sha256") or "")
            )

        if can_extend:
            event_ids: set[str] = set()
            horizons = {
                int(value)
                for value in (_CENSUS_CACHE or {}).get("horizon_values", [])
            }
            added_rows = added_mfe = added_mae = added_rotation = 0
            for _, event_id, horizon_sec, label_json in db.execute(
                """SELECT rowid,event_id,horizon_sec,label_json
                   FROM canonical_economic_outcome_labels
                   WHERE rowid>? ORDER BY rowid""",
                (previous_highwater,),
            ):
                added_rows += 1
                event_ids.add(str(event_id))
                horizons.add(int(horizon_sec))
                try:
                    label = json.loads(str(label_json or "{}"))
                except json.JSONDecodeError:
                    label = {}
                added_mfe += int(label.get("max_favorable_pips") is not None)
                added_mae += int(label.get("max_adverse_pips") is not None)
                added_rotation += int(
                    label.get("best_alternative_net_pips") is not None
                )
            prior_event_count = int(_CENSUS_CACHE.get("forecast_count") or 0)
            new_event_count = 0
            for event_id in event_ids:
                existed = db.execute(
                    """SELECT 1 FROM canonical_economic_outcome_labels
                       WHERE event_id=? AND rowid<=? LIMIT 1""",
                    (event_id, previous_highwater),
                ).fetchone()
                new_event_count += int(existed is None)
            census = {
                "state": "available",
                "row_count": int(_CENSUS_CACHE.get("row_count") or 0) + added_rows,
                "forecast_count": prior_event_count + new_event_count,
                "horizon_count": len(horizons),
                "horizon_values": sorted(horizons),
                "mfe_rows": int(_CENSUS_CACHE.get("mfe_rows") or 0) + added_mfe,
                "mae_rows": int(_CENSUS_CACHE.get("mae_rows") or 0) + added_mae,
                "rotation_rows": int(_CENSUS_CACHE.get("rotation_rows") or 0)
                + added_rotation,
                "refresh_mode": "incremental_append_only",
            }
        else:
            row = db.execute(
                """SELECT count(*),count(DISTINCT event_id),
                          count(DISTINCT horizon_sec),
                          group_concat(DISTINCT horizon_sec),
                          sum(CASE WHEN json_extract(label_json,'$.max_favorable_pips') IS NOT NULL THEN 1 ELSE 0 END),
                          sum(CASE WHEN json_extract(label_json,'$.max_adverse_pips') IS NOT NULL THEN 1 ELSE 0 END),
                          sum(CASE WHEN json_extract(label_json,'$.best_alternative_net_pips') IS NOT NULL THEN 1 ELSE 0 END)
                   FROM canonical_economic_outcome_labels"""
            ).fetchone()
            horizons = sorted(
                int(value) for value in str(row[3] or "").split(",") if value
            )
            census = {
                "state": "available",
                "row_count": int(row[0] or 0),
                "forecast_count": int(row[1] or 0),
                "horizon_count": int(row[2] or 0),
                "horizon_values": horizons,
                "mfe_rows": int(row[4] or 0),
                "mae_rows": int(row[5] or 0),
                "rotation_rows": int(row[6] or 0),
                "refresh_mode": "full_bootstrap",
            }

        highwater = maximum_rowid
        anchor = db.execute(
            """SELECT event_id,horizon_sec,label_json
               FROM canonical_economic_outcome_labels WHERE rowid=?""",
            (highwater,),
        ).fetchone() if highwater else ()
        census.update(
            {
                "highwater_rowid": highwater,
                "highwater_anchor_sha256": hashlib.sha256(
                    json.dumps(list(anchor or ()), separators=(",", ":")).encode("utf-8")
                ).hexdigest(),
                "database_file_id": file_id,
                "append_only_triggers_verified": immutable,
            }
        )
        db.close()
        _CENSUS_CACHE = census
        _CENSUS_REFRESH_MONOTONIC = now
        return {
            **_CENSUS_CACHE,
            "cache_state": "refreshed",
            "refresh_interval_sec": CENSUS_REFRESH_SEC,
        }
    except sqlite3.Error as exc:
        return {"state": "unavailable", "row_count": 0, "error": str(exc)}


def run_once(macro_path: Path = MACRO, quotes_path: Path = QUOTES, rates_path: Path = RATES, db_path: Path = DB, output: Path = OUTPUT, report: Path = REPORT, observed: dt.datetime | None = None, rules_path: Path = RULES, treasury_db_path: Path = TREASURY_DB, daily_rates_path: Path = DAILY_RATES, opportunity_path: Path | None = OPPORTUNITY_DB, opportunity_state_path: Path = OPPORTUNITY_STATE, internal_expectation_path: Path | None = INTERNAL_EXPECTATION_DB, heartbeat: WorkerHeartbeat | None = None) -> dict[str, Any]:
    if heartbeat is not None:
        heartbeat.mark_progress(phase="loading_inputs")
    if observed is None:
        observed, observation_clock = normalized_observation_time(utc_now())
    else:
        observed = observed.astimezone(dt.timezone.utc)
        observation_clock = {
            "source": "provided_replay_time",
            "contract_id": "nonprospective_replay_time_v1",
            "trusted_for_prospective_evidence": False,
            "normalized": False,
        }
    quotes = load_quotes(quotes_path, observed)
    rates = read_json(rates_path); rates_state = str(rates.get("state") or "source_not_connected"); rule_config = read_json(rules_path)
    opportunity_state = read_json(opportunity_state_path)
    opportunity_cohort_id = str(opportunity_state.get("cohort_id") or "")
    if (
        str(opportunity_state.get("model_cohort_id") or "") != OPPORTUNITY_MODEL_COHORT_ID
        or not opportunity_cohort_id.startswith(f"{OPPORTUNITY_MODEL_COHORT_ID}.collector.")
    ):
        opportunity_cohort_id = ""
    slow_rates_context = load_slow_treasury_context(treasury_db_path)
    if heartbeat is not None:
        heartbeat.mark_progress(phase="opening_response_database")
    db = open_db(db_path); new_obs = new_targets = 0
    clock_trusted = bool(
        observation_clock.get("trusted_for_prospective_evidence") is True
        and observation_clock.get("contract_id") == OBSERVATION_TIME_CONTRACT_ID
    )
    if macro_path.exists() and clock_trusted:
        if heartbeat is not None:
            heartbeat.mark_progress(phase="ingesting_macro")
        source = sqlite3.connect(f"file:{macro_path.as_posix()}?mode=ro", uri=True)
        new_obs, new_targets = ingest_macro(source, db, quotes, observed, rates_state, rule_config, opportunity_path, opportunity_cohort_id, internal_expectation_path); source.close()
    if heartbeat is not None:
        heartbeat.mark_progress(phase="invalidating_legacy_rows")
    current_progress_before = cohort_progress(db, COHORT)
    all_history_invalidated_bootstrap_observations, all_history_invalidated_bootstrap_targets = (
        invalidate_bootstrap_release_targets(db)
    )
    all_history_invalidated_inferred_observations, all_history_invalidated_inferred_targets = (
        invalidate_inferred_publication_targets(db)
    )
    if heartbeat is not None:
        heartbeat.mark_progress(phase="ingesting_daily_rates")
    daily_obs, daily_targets = ingest_daily_rate_context(
        read_json(daily_rates_path), db, quotes, observed
    ) if clock_trusted and daily_rates_path.exists() else (0, 0)
    new_obs += daily_obs
    new_targets += daily_targets
    if heartbeat is not None:
        heartbeat.mark_progress(phase="maturing_targets")
    sampled_all_history, matured_all_history = (
        update_targets(db, quotes, observed) if clock_trusted else (0, 0)
    )
    current_progress_after = cohort_progress(db, COHORT)
    sampled = (
        current_progress_after["path_samples"]
        - current_progress_before["path_samples"]
    )
    matured = (
        current_progress_after["matured_targets"]
        - current_progress_before["matured_targets"]
    )
    invalidated_bootstrap_observations = (
        current_progress_after["bootstrap_invalidated_observations"]
        - current_progress_before["bootstrap_invalidated_observations"]
    )
    invalidated_inferred_observations = (
        current_progress_after["inferred_invalidated_observations"]
        - current_progress_before["inferred_invalidated_observations"]
    )
    # Target invalidations are historical-maintenance diagnostics until the
    # current cohort has actually opened targets. Keep the current primary
    # counter conservative rather than attributing legacy repairs to V20.
    invalidated_bootstrap_targets = 0
    invalidated_inferred_targets = 0
    if heartbeat is not None:
        heartbeat.mark_progress(phase="summarizing")
    summary = summarize(db, COHORT)
    all_history_lineage_census = lineage_census(db)
    db.close()
    payload = {
        "schema_version": 1, "generated_utc": iso(observed),
        "status": "ok" if clock_trusted else "blocked_clock_integrity",
        "cohort_id": COHORT, "model_cohort_id": PARENT_COHORT,
        "research_only": True, "execution_eligible": False,
        "can_place_orders": False, "can_promote": False,
        "fresh_quote_count": sum(bool(q["fresh"]) for q in quotes.values()),
        "rates_state": rates_state, "slow_rates_context": slow_rates_context,
        "observation_clock": observation_clock,
        "observation_time_contract_id": OBSERVATION_TIME_CONTRACT_ID,
        "new_source_observations": new_obs,
        "new_response_targets": new_targets, "sampled_targets": sampled,
        "matured_targets": matured, "summary": summary,
        "all_history_updates_this_cycle": {
            "sampled_targets": sampled_all_history,
            "matured_targets": matured_all_history,
            "invalidated_bootstrap_observations": (
                all_history_invalidated_bootstrap_observations
            ),
            "invalidated_bootstrap_targets": all_history_invalidated_bootstrap_targets,
            "invalidated_inferred_observations": (
                all_history_invalidated_inferred_observations
            ),
            "invalidated_inferred_targets": all_history_invalidated_inferred_targets,
        },
        "all_history_diagnostic_lineage_census": all_history_lineage_census,
        "invalidated_bootstrap_observations": invalidated_bootstrap_observations,
        "invalidated_bootstrap_targets": invalidated_bootstrap_targets,
        "invalidated_inferred_publication_observations": invalidated_inferred_observations,
        "invalidated_inferred_publication_targets": invalidated_inferred_targets,
        "canonical_multi_target_census": canonical_target_census(),
        "contract": {
            "news_role": "provenance_only", "semantic_direction_required": False,
            "source_to_market_join": "source-native numeric observation to executable quote path",
            "targets": ["currency_return", "magnitude", "cost_clearance", "mfe", "mae", "time_to_cost_clear"],
            "maximum_pair_legs": MAX_PAIR_LEGS, "horizons_sec": list(HORIZONS),
            "missing_consensus_or_rates_is_unavailable": True,
            "slow_rates_contract": "daily Treasury context is separate from intraday OIS/policy repricing",
            "official_daily_rate_contract": "fresh prospective 2y changes create H4/H24 shadow targets only",
            "material_revision": "new cohort rejects source snapshots whose publication clock was inferred from ingestion time",
            "internal_expectation_contract": "pre-release internal baseline is a separate feature; it never populates consensus or assigns direction",
            "simple_rule_contract_id": rule_config.get("rule_contract_id"),
            "primary_simple_rule": "5m/15m magnitude opportunity; abstain direction",
            "aggressive_direction_rule": "shadow-only allowlisted numeric direction with flipped negative control",
            "pre_event_pair_ranking": "exact v2 forecasts issued before the official release; separate lowest-spread baseline retained",
        },
    }
    if heartbeat is not None:
        heartbeat.mark_progress(phase="publishing_cycle")
    atomic_json(output, payload)
    lines = ["# Direct Source → Currency Response", "", f"Generated: `{payload['generated_utc']}`", "",
             "Research-only; no execution or promotion path.", "",
             f"- Fresh pairs: **{payload['fresh_quote_count']}**",
             f"- New source observations / targets / maturities: **{new_obs} / {new_targets} / {matured}**",
             f"- Rates state: **{rates_state}**", "",
             f"- Slow Treasury context: **{slow_rates_context['state']}** (intraday confirmation: **no**)", "",
             "News is provenance only. Direction is learned from source-native numbers and later executable currency response.", "",
             "| Series | Currency | Horizon | N | Mean signed | Mean magnitude | Best after cost | Cost-clear | Proof |",
             "|---|---|---:|---:|---:|---:|---:|---:|---|" ]
    for row in summary["baselines"][:50]:
        lines.append(f"| {row['event_series_id']} | {row['currency']} | {row['horizon_sec']} | {row['n']} | {row['mean_currency_return_pips']:.3f} | {row['mean_absolute_move_pips']:.3f} | {row['mean_best_after_cost_pips']:.3f} | {row['cost_clear_rate']:.1%} | {'yes' if row['proof_eligible'] else 'no'} |")
    atomic_text(report, "\n".join(lines) + "\n")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-sec", type=float, default=30); parser.add_argument("--duration-sec", type=float, default=604800); parser.add_argument("--once", action="store_true")
    parser.add_argument("--macro", type=Path, default=MACRO); parser.add_argument("--quotes", type=Path, default=QUOTES); parser.add_argument("--rates", type=Path, default=RATES); parser.add_argument("--daily-rates", type=Path, default=DAILY_RATES); parser.add_argument("--rules", type=Path, default=RULES); parser.add_argument("--database", type=Path, default=DB); parser.add_argument("--output", type=Path, default=OUTPUT); parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--progress-heartbeat", type=Path, default=PROGRESS_HEARTBEAT)
    args = parser.parse_args(); stop = time.monotonic() + args.duration_sec
    cycles_completed = 0
    with WorkerHeartbeat(
        args.progress_heartbeat,
        worker="direct_source_response",
        role="research_only_progress",
        interval_sec=5.0,
    ) as heartbeat:
        while True:
            heartbeat.mark_progress(
                phase="starting_cycle",
                cycles_completed=cycles_completed,
            )
            run_once(
                args.macro,
                args.quotes,
                args.rates,
                args.database,
                args.output,
                args.report,
                rules_path=args.rules,
                daily_rates_path=args.daily_rates,
                heartbeat=heartbeat,
            )
            cycles_completed += 1
            heartbeat.mark_progress(
                phase="cycle_complete",
                cycles_completed=cycles_completed,
            )
            if args.once or time.monotonic() >= stop:
                return 0
            sleep_seconds = min(args.interval_sec, max(0, stop - time.monotonic()))
            heartbeat.update(phase="sleeping", sleep_seconds=sleep_seconds)
            time.sleep(sleep_seconds)


if __name__ == "__main__": raise SystemExit(main())
