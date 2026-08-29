#!/usr/bin/env python3
"""Prospective news-direction x causal-level-resolution x quote-flow study.

This worker is deliberately research-only.  It joins an already-known news
direction to a previously frozen causal level, waits for contact and a later
completed-minute resolution, and records the next future complete M1 bid/ask
open from the OANDA bid/ask/mid (BAM) archive.
The fixed H15 result is compared with an earlier exit on band invalidation.

It has no broker client and no signal-feed, lifecycle, promotion,
authorization, credential, or execution surface.
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
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from oanda_causal_level_band_prospective import (
    load_m1_tail,
    trusted_observation_time,
)
from oanda_level_band_contract_v2 import MarketBar


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
CONFIG = ROOT / "config" / "news_band_resolution_flow_h15_v1.json"
WATCHLIST = STATE / "news_technical_watchlist_v1.json"
BAND_LEDGER = STATE / "causal_level_band_prospective_v1.sqlite"
QUOTE_LEDGER = STATE / "practice_007_quote_intensity_shadow_v1.sqlite"
UPDATE_REPORT = STATE / "all68_m1_forward_update_v1.json"
CLOCK = STATE / "clock_integrity_v1.json"
LEDGER = STATE / "news_band_resolution_flow_h15_v1.sqlite"
OUTPUT = STATE / "news_band_resolution_flow_h15_v1.json"
HEARTBEAT = STATE / "news_band_resolution_flow_h15_heartbeat_v1.json"
REPORT = (
    DATA
    / "reports"
    / "news_band_resolution_flow"
    / "NEWS_BAND_RESOLUTION_FLOW_H15_CURRENT.md"
)
UTC = dt.timezone.utc
SCHEMA_VERSION = "news_band_resolution_flow_h15_v1"


def utc_now() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_utc(value: Any) -> dt.datetime | None:
    try:
        result = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=UTC)
    return result.astimezone(UTC)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def direction(value: Any) -> str:
    value = str(value or "").strip().lower()
    if value in {"long", "buy", "strengthen"}:
        return "long"
    if value in {"short", "sell", "weaken"}:
        return "short"
    return "neutral"


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
        default=str,
    )


def digest(*values: Any) -> str:
    return hashlib.sha256(
        "\x1f".join(str(value) for value in values).encode("utf-8")
    ).hexdigest()


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def load_contract(path: Path = CONFIG) -> dict[str, Any]:
    value = read_json(path)
    policy = value.get("policy") or {}
    required = {
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "signal_feed_write": False,
        "lifecycle_write": False,
        "broker_access": False,
        "credentials_access": False,
    }
    if not value.get("contract_id") or any(policy.get(k) is not v for k, v in required.items()):
        raise ValueError("unsafe_or_incomplete_contract")
    if int(value.get("horizon_sec") or 0) != 900:
        raise ValueError("horizon_contract_mismatch")
    return value


def collection_identity(
    config_path: Path = CONFIG,
    source_path: Path | None = None,
) -> dict[str, str]:
    config = load_contract(config_path)
    source = source_path or Path(__file__).resolve()
    definition = {
        "contract": config,
        "config_sha256": sha256_file(config_path),
        "worker_sha256": sha256_file(source),
    }
    definition_sha = hashlib.sha256(canonical_json(definition).encode()).hexdigest()
    return {
        "contract_id": str(config["contract_id"]),
        "cohort_id": f"{config['cohort_prefix']}.{definition_sha[:16]}",
        "definition_sha256": definition_sha,
        "definition_json": canonical_json(definition),
    }


def next_minute_boundary(value: dt.datetime) -> dt.datetime:
    epoch = value.timestamp()
    return dt.datetime.fromtimestamp((math.floor(epoch / 60.0) + 1) * 60, UTC)


def open_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30.0)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA busy_timeout=30000")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS cohort_registry (
          cohort_id TEXT PRIMARY KEY,contract_id TEXT NOT NULL,
          created_at_utc TEXT NOT NULL,definition_sha256 TEXT NOT NULL,
          definition_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS timing_candidates (
          candidate_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,
          contract_id TEXT NOT NULL,observed_at_utc TEXT NOT NULL,
          planned_entry_utc TEXT NOT NULL,episode_id TEXT NOT NULL,
          currency TEXT NOT NULL,instrument TEXT NOT NULL,
          direction TEXT NOT NULL CHECK(direction IN ('long','short')),
          resolution_kind TEXT NOT NULL CHECK(resolution_kind IN ('break','reject')),
          news_first_known_utc TEXT NOT NULL,news_source_cohort_id TEXT NOT NULL,
          band_forecast_id TEXT NOT NULL,band_cohort_id TEXT NOT NULL,
          band_version_id TEXT NOT NULL,approach_side INTEGER NOT NULL,
          band_lower REAL NOT NULL,band_upper REAL NOT NULL,
          frozen_break_price REAL NOT NULL,frozen_reject_price REAL NOT NULL,
          pip REAL NOT NULL,contact_minute_utc TEXT NOT NULL,
          resolution_minute_utc TEXT NOT NULL,resolution_known_utc TEXT NOT NULL,
          signed_imbalance_30s REAL NOT NULL,signed_imbalance_120s REAL NOT NULL,
          flow_aligned INTEGER NOT NULL CHECK(flow_aligned IN (0,1)),
          resolution_spread_pips REAL NOT NULL,payload_json TEXT NOT NULL,
          payload_sha256 TEXT NOT NULL,research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          UNIQUE(cohort_id,episode_id,currency),
          FOREIGN KEY(cohort_id) REFERENCES cohort_registry(cohort_id)
        );
        CREATE TABLE IF NOT EXISTS timing_arms (
          arm_id TEXT PRIMARY KEY,candidate_id TEXT NOT NULL,arm_name TEXT NOT NULL,
          matched_control_group_id TEXT NOT NULL,payload_json TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          UNIQUE(candidate_id,arm_name),
          FOREIGN KEY(candidate_id) REFERENCES timing_candidates(candidate_id)
        );
        CREATE TABLE IF NOT EXISTS timing_entries (
          candidate_id TEXT PRIMARY KEY,entry_utc TEXT NOT NULL,
          bid_open REAL NOT NULL,ask_open REAL NOT NULL,source_row_sha256 TEXT NOT NULL,
          observed_at_utc TEXT NOT NULL,payload_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          FOREIGN KEY(candidate_id) REFERENCES timing_candidates(candidate_id)
        );
        CREATE TABLE IF NOT EXISTS timing_outcomes (
          outcome_id TEXT PRIMARY KEY,candidate_id TEXT NOT NULL,horizon_sec INTEGER NOT NULL,
          maturity_utc TEXT NOT NULL,matured_at_utc TEXT NOT NULL,
          fixed_horizon_net_pips REAL NOT NULL,invalidation_net_pips REAL NOT NULL,
          invalidation_triggered INTEGER NOT NULL CHECK(invalidation_triggered IN (0,1)),
          invalidation_utc TEXT,payload_json TEXT NOT NULL,payload_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          UNIQUE(candidate_id,horizon_sec),
          FOREIGN KEY(candidate_id) REFERENCES timing_candidates(candidate_id)
        );
        CREATE TABLE IF NOT EXISTS timing_censors (
          censor_id TEXT PRIMARY KEY,candidate_id TEXT NOT NULL,observed_at_utc TEXT NOT NULL,
          reason TEXT NOT NULL,payload_json TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          UNIQUE(candidate_id,reason),
          FOREIGN KEY(candidate_id) REFERENCES timing_candidates(candidate_id)
        );
        CREATE TRIGGER IF NOT EXISTS candidates_no_update BEFORE UPDATE ON timing_candidates BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS candidates_no_delete BEFORE DELETE ON timing_candidates BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS arms_no_update BEFORE UPDATE ON timing_arms BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS arms_no_delete BEFORE DELETE ON timing_arms BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS entries_no_update BEFORE UPDATE ON timing_entries BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS entries_no_delete BEFORE DELETE ON timing_entries BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS outcomes_no_update BEFORE UPDATE ON timing_outcomes BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS outcomes_no_delete BEFORE DELETE ON timing_outcomes BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS censors_no_update BEFORE UPDATE ON timing_censors BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS censors_no_delete BEFORE DELETE ON timing_censors BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS cohorts_no_update BEFORE UPDATE ON cohort_registry BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS cohorts_no_delete BEFORE DELETE ON cohort_registry BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    db.commit()
    return db


def register_cohort(
    db: sqlite3.Connection,
    identity: Mapping[str, str],
    observed: dt.datetime,
) -> None:
    row = db.execute(
        "SELECT definition_sha256,definition_json FROM cohort_registry WHERE cohort_id=?",
        (identity["cohort_id"],),
    ).fetchone()
    if row is not None:
        if (
            str(row["definition_sha256"]) != identity["definition_sha256"]
            or str(row["definition_json"]) != identity["definition_json"]
        ):
            raise RuntimeError("cohort_identity_collision")
        return
    db.execute(
        "INSERT INTO cohort_registry VALUES (?,?,?,?,?)",
        (
            identity["cohort_id"],
            identity["contract_id"],
            iso(observed),
            identity["definition_sha256"],
            identity["definition_json"],
        ),
    )
    db.commit()


def load_active_news(
    payload: Mapping[str, Any],
    observed: dt.datetime,
    config: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    generated = parse_utc(payload.get("generated_utc"))
    max_age = safe_float(config.get("maximum_watchlist_age_sec"), 180.0)
    if (
        payload.get("status") != "ok"
        or generated is None
        or generated > observed + dt.timedelta(seconds=2)
        or (observed - generated).total_seconds() > max_age
    ):
        return [], "stale_or_invalid_watchlist"
    allowed = set(config.get("allowed_news_arms") or [])
    rows: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for raw in payload.get("watchlist") or []:
        if not isinstance(raw, Mapping) or str(raw.get("arm") or "") not in allowed:
            continue
        if raw.get("research_only") is not True or raw.get("execution_eligible") is not False:
            continue
        if bool(raw.get("uncorroborated_research")):
            continue
        episode = str(raw.get("episode_id") or "")
        currency = str(raw.get("currency") or "")
        instrument = str(raw.get("instrument") or "")
        side = direction(raw.get("news_direction") or raw.get("direction"))
        known = parse_utc(raw.get("news_factor_first_known_utc"))
        expires = parse_utc(raw.get("news_factor_expires_utc"))
        source_cohort_ids = sorted(
            {
                str(value).strip()
                for value in (raw.get("news_factor_source_cohort_ids") or [])
                if str(value).strip()
            }
        )
        source_contract_ids = sorted(
            {
                str(value).strip()
                for value in (raw.get("news_factor_source_contract_ids") or [])
                if str(value).strip()
            }
        )
        if (
            not episode
            or not currency
            or not instrument
            or side == "neutral"
            or known is None
            or not source_cohort_ids
            or not source_contract_ids
        ):
            continue
        if known > observed or (expires is not None and expires < observed):
            continue
        key = (episode, currency, instrument, side)
        candidate = dict(raw)
        candidate["_known"] = known
        candidate["_expires"] = expires
        candidate["_source_cohort_ids"] = source_cohort_ids
        candidate["_source_contract_ids"] = source_contract_ids
        prior = rows.get(key)
        if prior is None or safe_float(candidate.get("news_confidence")) > safe_float(prior.get("news_confidence")):
            rows[key] = candidate
    return list(rows.values()), "fresh"


def resolution_direction(approach_side: int, kind: str) -> str:
    if kind == "break":
        return "long" if approach_side == 1 else "short"
    if kind == "reject":
        return "short" if approach_side == 1 else "long"
    return "neutral"


def detect_resolution(
    forecast: Mapping[str, Any],
    minute_rows: Sequence[Mapping[str, Any]],
    observed: dt.datetime,
) -> dict[str, Any] | None:
    issued = parse_utc(forecast.get("issued_at_utc"))
    data_cutoff = parse_utc(forecast.get("data_cutoff_utc"))
    if issued is None or data_cutoff is None or data_cutoff > issued:
        return None
    approach = int(safe_float(forecast.get("approach_side")))
    if approach not in {-1, 1}:
        return None
    lower = safe_float(forecast.get("band_lower"), math.nan)
    upper = safe_float(forecast.get("band_upper"), math.nan)
    break_price = safe_float(forecast.get("frozen_break_price"), math.nan)
    reject_price = safe_float(forecast.get("frozen_reject_price"), math.nan)
    if not all(math.isfinite(value) and value > 0.0 for value in (lower, upper, break_price, reject_price)):
        return None
    first_complete_start = int(issued.timestamp() // 60) * 60 + 60
    last_complete_start = int(observed.timestamp() // 60) * 60 - 60
    contact: Mapping[str, Any] | None = None
    for raw in sorted(minute_rows, key=lambda row: int(safe_float(row.get("minute_epoch")))):
        epoch = int(safe_float(raw.get("minute_epoch"), -1.0))
        if epoch < first_complete_start or epoch > last_complete_start:
            continue
        mid = safe_float(raw.get("last_mid"), math.nan)
        if not math.isfinite(mid) or mid <= 0.0:
            continue
        touched = mid >= lower if approach == 1 else mid <= upper
        if contact is None:
            if touched:
                contact = raw
            # Contact and resolution on one completed minute are deliberately
            # not ordered optimistically. A second minute must confirm.
            continue
        contact_epoch = int(safe_float(contact.get("minute_epoch")))
        if epoch <= contact_epoch:
            continue
        broke = mid >= break_price if approach == 1 else mid <= break_price
        rejected = mid <= reject_price if approach == 1 else mid >= reject_price
        if broke == rejected:
            continue
        kind = "break" if broke else "reject"
        side = resolution_direction(approach, kind)
        sign = 1.0 if side == "long" else -1.0
        contact_time = dt.datetime.fromtimestamp(contact_epoch, UTC)
        resolution_time = dt.datetime.fromtimestamp(epoch, UTC)
        return {
            "contact_minute_utc": iso(contact_time),
            "contact_known_utc": iso(contact_time + dt.timedelta(minutes=1)),
            "resolution_minute_utc": iso(resolution_time),
            "resolution_known_utc": iso(resolution_time + dt.timedelta(minutes=1)),
            "resolution_kind": kind,
            "direction": side,
            "signed_imbalance_30s": sign * safe_float(raw.get("imbalance_30s")),
            "signed_imbalance_120s": sign * safe_float(raw.get("imbalance_120s")),
            "resolution_spread_pips": safe_float(raw.get("average_spread_pips"), math.inf),
            "resolution_mid": mid,
        }
    return None


def _read_band_forecasts(
    path: Path,
    observed: dt.datetime,
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    minimum = iso(
        observed
        - dt.timedelta(seconds=safe_float(config.get("maximum_band_forecast_age_sec"), 1800.0))
    )
    try:
        db = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=10)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """SELECT * FROM band_forecasts
               WHERE contract_id=? AND issued_at_utc>=? AND issued_at_utc<=?
               ORDER BY issued_at_utc,forecast_id""",
            (config["parent_level_contract_id"], minimum, iso(observed)),
        ).fetchall()
        db.close()
    except (OSError, sqlite3.Error):
        return []
    return [dict(row) for row in rows]


def _read_quote_minutes(
    path: Path,
    instruments: Iterable[str],
    observed: dt.datetime,
    lookback_sec: float,
) -> dict[str, list[dict[str, Any]]]:
    names = sorted(set(str(value) for value in instruments if str(value)))
    if not path.is_file() or not names:
        return {}
    placeholders = ",".join("?" for _ in names)
    start = int(observed.timestamp() - lookback_sec - 120)
    end = int(observed.timestamp() // 60) * 60 - 60
    try:
        db = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=10)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            f"""SELECT minute_epoch,instrument,last_mid,average_spread_pips,
                       imbalance_30s,imbalance_120s,last_broker_time
                  FROM quote_intensity_minutes_v1
                 WHERE instrument IN ({placeholders})
                   AND minute_epoch BETWEEN ? AND ?
                 ORDER BY instrument,minute_epoch""",
            [*names, start, end],
        ).fetchall()
        db.close()
    except (OSError, sqlite3.Error):
        return {}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["instrument"]), []).append(dict(row))
    return grouped


def build_candidate_payloads(
    news_rows: Sequence[Mapping[str, Any]],
    band_forecasts: Sequence[Mapping[str, Any]],
    quote_rows: Mapping[str, Sequence[Mapping[str, Any]]],
    observed: dt.datetime,
    config: Mapping[str, Any],
    existing_factor_keys: set[tuple[str, str]] | None = None,
) -> list[dict[str, Any]]:
    existing = existing_factor_keys or set()
    candidates: list[dict[str, Any]] = []
    for forecast in band_forecasts:
        instrument = str(forecast.get("instrument") or "")
        resolution = detect_resolution(forecast, quote_rows.get(instrument) or [], observed)
        if resolution is None:
            continue
        known = parse_utc(resolution["resolution_known_utc"])
        contact_known = parse_utc(resolution["contact_known_utc"])
        if known is None or contact_known is None:
            continue
        age = (observed - known).total_seconds()
        if age < 0.0 or age > safe_float(config.get("maximum_resolution_age_sec"), 120.0):
            continue
        for news in news_rows:
            if str(news.get("instrument") or "") != instrument:
                continue
            if direction(news.get("news_direction") or news.get("direction")) != resolution["direction"]:
                continue
            first_known = news.get("_known") or parse_utc(news.get("news_factor_first_known_utc"))
            expires = news.get("_expires") or parse_utc(news.get("news_factor_expires_utc"))
            # Direction must already be known before the completed contact
            # minute becomes observable. Equality is not treated as prior.
            if not isinstance(first_known, dt.datetime) or first_known >= contact_known:
                continue
            if isinstance(expires, dt.datetime) and expires < known:
                continue
            episode = str(news.get("episode_id") or "")
            currency = str(news.get("currency") or "")
            source_cohort_ids = sorted(
                set(
                    news.get("_source_cohort_ids")
                    or news.get("news_factor_source_cohort_ids")
                    or []
                )
            )
            source_contract_ids = sorted(
                set(
                    news.get("_source_contract_ids")
                    or news.get("news_factor_source_contract_ids")
                    or []
                )
            )
            if (
                not episode
                or not currency
                or not source_cohort_ids
                or not source_contract_ids
                or (episode, currency) in existing
            ):
                continue
            signed30 = safe_float(resolution["signed_imbalance_30s"])
            signed120 = safe_float(resolution["signed_imbalance_120s"])
            flow_aligned = bool(
                signed30 >= safe_float(config.get("minimum_signed_imbalance_30s"), 0.05)
                and (
                    not bool(config.get("require_nonnegative_signed_imbalance_120s"))
                    or signed120 >= 0.0
                )
            )
            pip = safe_float(forecast.get("pip"), math.nan)
            spread = safe_float(resolution["resolution_spread_pips"], math.inf)
            mid = safe_float(resolution["resolution_mid"], math.nan)
            if not all(math.isfinite(value) and value > 0.0 for value in (pip, spread, mid)):
                continue
            candidates.append(
                {
                    "episode_id": episode,
                    "currency": currency,
                    "instrument": instrument,
                    "direction": resolution["direction"],
                    "resolution_kind": resolution["resolution_kind"],
                    "news_first_known_utc": iso(first_known),
                    "news_source_cohort_ids": source_cohort_ids,
                    "news_source_contract_ids": source_contract_ids,
                    "news_source_ids": sorted(set(news.get("news_factor_source_ids") or [])),
                    "news_confidence": safe_float(news.get("news_confidence")),
                    "band_forecast_id": str(forecast.get("forecast_id") or ""),
                    "band_cohort_id": str(forecast.get("cohort_id") or ""),
                    "band_contract_id": str(forecast.get("contract_id") or ""),
                    "band_version_id": str(forecast.get("band_version_id") or ""),
                    "band_data_cutoff_utc": str(forecast.get("data_cutoff_utc") or ""),
                    "band_issued_at_utc": str(forecast.get("issued_at_utc") or ""),
                    "approach_side": int(safe_float(forecast.get("approach_side"))),
                    "band_lower": safe_float(forecast.get("band_lower")),
                    "band_upper": safe_float(forecast.get("band_upper")),
                    "frozen_break_price": safe_float(forecast.get("frozen_break_price")),
                    "frozen_reject_price": safe_float(forecast.get("frozen_reject_price")),
                    "pip": pip,
                    **resolution,
                    "flow_aligned": flow_aligned,
                    "resolution_cost_bps": spread * pip / mid * 10_000.0,
                    "planned_entry_utc": iso(next_minute_boundary(observed)),
                    "observed_at_utc": iso(observed),
                    "research_only": True,
                    "execution_eligible": False,
                    "can_authorize": False,
                }
            )
    # The pair choice is independent of later outcomes: earliest completed
    # resolution, then lowest executable cost, then stable instrument identity.
    candidates.sort(
        key=lambda row: (
            row["resolution_known_utc"],
            safe_float(row["resolution_cost_bps"], math.inf),
            row["instrument"],
            row["band_forecast_id"],
        )
    )
    selected: list[dict[str, Any]] = []
    used = set(existing)
    for row in candidates:
        key = (row["episode_id"], row["currency"])
        if key in used:
            continue
        used.add(key)
        selected.append(row)
    return selected


def insert_candidate(
    db: sqlite3.Connection,
    identity: Mapping[str, str],
    payload: Mapping[str, Any],
) -> tuple[str, bool]:
    content = {**dict(payload), "cohort_id": identity["cohort_id"], "contract_id": identity["contract_id"]}
    payload_json = canonical_json(content)
    payload_sha = hashlib.sha256(payload_json.encode()).hexdigest()
    candidate_id = "nbf_" + digest(
        identity["cohort_id"], payload["episode_id"], payload["currency"],
        payload["instrument"], payload["band_forecast_id"], payload["resolution_minute_utc"],
    )[:28]
    prior = db.execute(
        "SELECT payload_sha256 FROM timing_candidates WHERE candidate_id=?",
        (candidate_id,),
    ).fetchone()
    if prior is not None:
        if str(prior["payload_sha256"]) != payload_sha:
            raise RuntimeError("candidate_idempotency_collision")
        return candidate_id, False
    values = (
            candidate_id, identity["cohort_id"], identity["contract_id"],
            payload["observed_at_utc"], payload["planned_entry_utc"],
            payload["episode_id"], payload["currency"], payload["instrument"],
            payload["direction"], payload["resolution_kind"],
            payload["news_first_known_utc"],
            canonical_json(
                {
                    "cohort_ids": payload["news_source_cohort_ids"],
                    "contract_ids": payload["news_source_contract_ids"],
                }
            ),
            payload["band_forecast_id"], payload["band_cohort_id"],
            payload["band_version_id"], payload["approach_side"],
            payload["band_lower"], payload["band_upper"],
            payload["frozen_break_price"], payload["frozen_reject_price"],
            payload["pip"], payload["contact_minute_utc"],
            payload["resolution_minute_utc"], payload["resolution_known_utc"],
            payload["signed_imbalance_30s"], payload["signed_imbalance_120s"],
            int(bool(payload["flow_aligned"])), payload["resolution_spread_pips"],
            payload_json, payload_sha,
        )
    db.execute(
        """INSERT INTO timing_candidates (
          candidate_id,cohort_id,contract_id,observed_at_utc,planned_entry_utc,
          episode_id,currency,instrument,direction,resolution_kind,
          news_first_known_utc,news_source_cohort_id,band_forecast_id,
          band_cohort_id,band_version_id,approach_side,band_lower,band_upper,
          frozen_break_price,frozen_reject_price,pip,contact_minute_utc,
          resolution_minute_utc,resolution_known_utc,signed_imbalance_30s,
          signed_imbalance_120s,flow_aligned,resolution_spread_pips,
          payload_json,payload_sha256,research_only,execution_eligible,can_authorize
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,0)""",
        values,
    )
    group_id = "match_" + digest(candidate_id)[:24]
    arms = ["news_band_resolution_no_flow_control"]
    arms.append(
        "news_band_resolution_flow_aligned"
        if payload["flow_aligned"]
        else "news_band_resolution_flow_conflicted_negative_control"
    )
    for arm in arms:
        arm_payload = canonical_json(
            {
                "candidate_id": candidate_id,
                "arm_name": arm,
                "matched_control_group_id": group_id,
                "uses_flow_for_selection": arm != "news_band_resolution_no_flow_control",
                "research_only": True,
                "execution_eligible": False,
            }
        )
        db.execute(
                "INSERT INTO timing_arms VALUES (?,?,?,?,?,1,0)",
            ("arm_" + digest(candidate_id, arm)[:28], candidate_id, arm, group_id, arm_payload),
        )
    return candidate_id, True


def _bar_sha(bar: MarketBar) -> str:
    return hashlib.sha256(canonical_json(asdict(bar)).encode()).hexdigest()


def _net_pips(
    side: str,
    entry_bid: float,
    entry_ask: float,
    exit_bid: float,
    exit_ask: float,
    pip: float,
) -> float:
    if side == "long":
        return (exit_bid - entry_ask) / pip
    return (entry_bid - exit_ask) / pip


def invalidated(candidate: Mapping[str, Any], bar: MarketBar) -> bool:
    side = str(candidate.get("direction") or "")
    kind = str(candidate.get("resolution_kind") or "")
    approach = int(safe_float(candidate.get("approach_side")))
    if kind == "break":
        return (
            bar.mid_close < safe_float(candidate.get("band_upper"))
            if side == "long"
            else bar.mid_close > safe_float(candidate.get("band_lower"))
        )
    # A rejected/bounce thesis is invalid only by a later completed close
    # through the same pre-frozen break barrier.
    break_price = safe_float(candidate.get("frozen_break_price"))
    return bar.mid_close <= break_price if side == "long" else bar.mid_close >= break_price


def mature_payload(
    candidate: Mapping[str, Any],
    entry: Mapping[str, Any],
    bars: Sequence[MarketBar],
    observed: dt.datetime,
    horizon_sec: int = 900,
) -> dict[str, Any]:
    entry_time = parse_utc(entry.get("entry_utc"))
    if entry_time is None or horizon_sec % 60:
        raise ValueError("invalid_entry_or_horizon")
    expected = [entry_time + dt.timedelta(minutes=index) for index in range(horizon_sec // 60)]
    if [bar.timestamp for bar in bars] != expected:
        raise ValueError("noncontiguous_or_inexact_h15_path")
    if bars[-1].known_at > observed:
        raise ValueError("outcome_not_mature")
    entry_bid = safe_float(entry.get("bid_open"))
    entry_ask = safe_float(entry.get("ask_open"))
    pip = safe_float(candidate.get("pip"))
    side = str(candidate.get("direction") or "")
    if entry_bid <= 0.0 or entry_ask <= entry_bid or pip <= 0.0 or side not in {"long", "short"}:
        raise ValueError("invalid_executable_entry")
    fixed = _net_pips(
        side, entry_bid, entry_ask,
        bars[-1].bid_close, bars[-1].ask_close, pip,
    )
    invalidation_index = next(
        (index for index, bar in enumerate(bars) if invalidated(candidate, bar)),
        None,
    )
    invalidation_bar = bars[invalidation_index] if invalidation_index is not None else bars[-1]
    invalidation_net = _net_pips(
        side, entry_bid, entry_ask,
        invalidation_bar.bid_close, invalidation_bar.ask_close, pip,
    )
    return {
        "candidate_id": str(candidate.get("candidate_id") or ""),
        "horizon_sec": horizon_sec,
        "maturity_utc": iso(bars[-1].known_at),
        "matured_at_utc": iso(observed),
        "fixed_horizon_net_pips": fixed,
        "invalidation_net_pips": invalidation_net,
        "invalidation_triggered": invalidation_index is not None,
        "invalidation_utc": (
            iso(invalidation_bar.known_at) if invalidation_index is not None else None
        ),
        "invalidation_bar_offset": invalidation_index,
        "entry_utc": iso(entry_time),
        "entry_bid_open": entry_bid,
        "entry_ask_open": entry_ask,
        "fixed_exit_bid": bars[-1].bid_close,
        "fixed_exit_ask": bars[-1].ask_close,
        "invalidation_exit_bid": invalidation_bar.bid_close,
        "invalidation_exit_ask": invalidation_bar.ask_close,
        "research_only": True,
        "execution_eligible": False,
    }


def _load_bars_for_instruments(
    report_path: Path,
    instruments: Iterable[str],
) -> tuple[dict[str, list[MarketBar]], str]:
    report = read_json(report_path)
    source = report.get("source") or {}
    if source.get("environment") != "practice" or source.get("price") != "BAM":
        return {}, "invalid_bam_update_report"
    wanted = set(instruments)
    rows = {
        str(row.get("instrument") or ""): row
        for row in report.get("pairs") or []
        if isinstance(row, Mapping) and str(row.get("instrument") or "") in wanted
    }
    result: dict[str, list[MarketBar]] = {}
    for instrument in sorted(wanted):
        row = rows.get(instrument)
        if not row or row.get("error"):
            continue
        cutoff = parse_utc(row.get("after_last"))
        path = Path(str(row.get("path") or ""))
        if cutoff is None or not path.is_file():
            continue
        try:
            bars, _ = load_m1_tail(path, instrument, cutoff=cutoff)
        except (OSError, ValueError):
            continue
        result[instrument] = bars
    return result, "ready" if result else "bam_rows_unavailable"


def append_entries_and_outcomes(
    db: sqlite3.Connection,
    observed: dt.datetime,
    bars_by_instrument: Mapping[str, Sequence[MarketBar]],
    horizon_sec: int = 900,
) -> dict[str, int]:
    counts = {"entries": 0, "outcomes": 0, "censors": 0}
    candidates = db.execute(
        """SELECT c.* FROM timing_candidates c
           LEFT JOIN timing_censors z ON z.candidate_id=c.candidate_id
           WHERE z.candidate_id IS NULL ORDER BY c.planned_entry_utc,c.candidate_id"""
    ).fetchall()
    for raw in candidates:
        candidate = dict(raw)
        candidate_id = str(candidate["candidate_id"])
        planned = parse_utc(candidate["planned_entry_utc"])
        if planned is None:
            continue
        bars = list(bars_by_instrument.get(str(candidate["instrument"])) or [])
        entry = db.execute(
            "SELECT * FROM timing_entries WHERE candidate_id=?", (candidate_id,)
        ).fetchone()
        if entry is None:
            bar = next((value for value in bars if value.timestamp == planned), None)
            if bar is not None and bar.known_at <= observed:
                payload = {
                    "candidate_id": candidate_id,
                    "entry_utc": iso(planned),
                    "bid_open": bar.bid_open,
                    "ask_open": bar.ask_open,
                    "source_row_sha256": _bar_sha(bar),
                    "observed_at_utc": iso(observed),
                    "research_only": True,
                    "execution_eligible": False,
                }
                payload_sha = hashlib.sha256(canonical_json(payload).encode()).hexdigest()
                db.execute(
                    "INSERT INTO timing_entries VALUES (?,?,?,?,?,?,?,1,0)",
                    (
                        candidate_id, payload["entry_utc"], payload["bid_open"],
                        payload["ask_open"], payload["source_row_sha256"],
                        payload["observed_at_utc"], payload_sha,
                    ),
                )
                counts["entries"] += 1
                entry = db.execute(
                    "SELECT * FROM timing_entries WHERE candidate_id=?", (candidate_id,)
                ).fetchone()
            elif observed >= planned + dt.timedelta(minutes=30):
                detail = canonical_json(
                    {"candidate_id": candidate_id, "planned_entry_utc": iso(planned), "reason": "missing_exact_future_bam_entry"}
                )
                db.execute(
                    "INSERT OR IGNORE INTO timing_censors VALUES (?,?,?,?,?,1,0)",
                    (
                        "censor_" + digest(candidate_id, "entry")[:28], candidate_id,
                        iso(observed), "missing_exact_future_bam_entry", detail,
                    ),
                )
                counts["censors"] += int(db.execute("SELECT changes()").fetchone()[0] > 0)
                continue
        if entry is None:
            continue
        if db.execute(
            "SELECT 1 FROM timing_outcomes WHERE candidate_id=? AND horizon_sec=?",
            (candidate_id, horizon_sec),
        ).fetchone() is not None:
            continue
        path = [
            bar for bar in bars
            if planned <= bar.timestamp < planned + dt.timedelta(seconds=horizon_sec)
        ]
        if len(path) == horizon_sec // 60 and path[-1].known_at <= observed:
            payload = mature_payload(candidate, dict(entry), path, observed, horizon_sec)
            payload_json = canonical_json(payload)
            payload_sha = hashlib.sha256(payload_json.encode()).hexdigest()
            outcome_id = "outcome_" + digest(candidate_id, horizon_sec)[:28]
            db.execute(
                "INSERT INTO timing_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    outcome_id, candidate_id, horizon_sec, payload["maturity_utc"],
                    payload["matured_at_utc"], payload["fixed_horizon_net_pips"],
                    payload["invalidation_net_pips"], int(payload["invalidation_triggered"]),
                    payload["invalidation_utc"], payload_json, payload_sha,
                ),
            )
            counts["outcomes"] += 1
        elif observed >= planned + dt.timedelta(seconds=horizon_sec + 1800):
            detail = canonical_json(
                {"candidate_id": candidate_id, "planned_entry_utc": iso(planned), "reason": "missing_exact_contiguous_h15_path"}
            )
            db.execute(
                "INSERT OR IGNORE INTO timing_censors VALUES (?,?,?,?,?,1,0)",
                (
                    "censor_" + digest(candidate_id, "outcome")[:28], candidate_id,
                    iso(observed), "missing_exact_contiguous_h15_path", detail,
                ),
            )
            counts["censors"] += int(db.execute("SELECT changes()").fetchone()[0] > 0)
    db.commit()
    return counts


def pending_instruments(db: sqlite3.Connection) -> set[str]:
    """Return every unresolved instrument, including immutable prior cohorts.

    A material contract or source change opens a new cohort, but it must not
    strand already-recorded candidates from the prior cohort before their
    exact future entry or H15 outcome can mature.
    """
    return {
        str(row[0])
        for row in db.execute(
            """SELECT DISTINCT c.instrument FROM timing_candidates c
               LEFT JOIN timing_outcomes o USING(candidate_id)
               LEFT JOIN timing_censors z USING(candidate_id)
               WHERE o.candidate_id IS NULL AND z.candidate_id IS NULL"""
        )
    }


def ledger_summary(db: sqlite3.Connection, cohort_id: str) -> dict[str, Any]:
    counts = {}
    for name, table in (
        ("candidates", "timing_candidates"),
        ("entries", "timing_entries"),
        ("outcomes", "timing_outcomes"),
        ("censors", "timing_censors"),
    ):
        counts[name] = int(
            db.execute(
                f"""SELECT COUNT(*) FROM {table} x
                    JOIN timing_candidates c ON c.candidate_id=x.candidate_id
                    WHERE c.cohort_id=?"""
                if table != "timing_candidates"
                else "SELECT COUNT(*) FROM timing_candidates WHERE cohort_id=?",
                (cohort_id,),
            ).fetchone()[0]
        )
    arms: dict[str, Any] = {}
    for row in db.execute(
        """SELECT a.arm_name,COUNT(*) n,
                  AVG(o.fixed_horizon_net_pips) fixed_mean,
                  AVG(o.fixed_horizon_net_pips>0) fixed_win,
                  AVG(o.invalidation_net_pips) invalidation_mean,
                  AVG(o.invalidation_net_pips>0) invalidation_win,
                  AVG(o.invalidation_triggered) invalidation_rate
             FROM timing_arms a
             JOIN timing_candidates c USING(candidate_id)
             JOIN timing_outcomes o USING(candidate_id)
            WHERE c.cohort_id=? GROUP BY a.arm_name ORDER BY a.arm_name""",
        (cohort_id,),
    ):
        arms[str(row["arm_name"])] = {
            "n": int(row["n"]),
            "fixed_h15_mean_net_pips": safe_float(row["fixed_mean"]),
            "fixed_h15_after_cost_win_rate": safe_float(row["fixed_win"]),
            "invalidation_mean_net_pips": safe_float(row["invalidation_mean"]),
            "invalidation_after_cost_win_rate": safe_float(row["invalidation_win"]),
            "invalidation_trigger_rate": safe_float(row["invalidation_rate"]),
        }
    recent = []
    for row in db.execute(
        """SELECT candidate_id,observed_at_utc,episode_id,currency,instrument,
                  direction,resolution_kind,flow_aligned,planned_entry_utc
             FROM timing_candidates WHERE cohort_id=?
             ORDER BY observed_at_utc DESC,candidate_id LIMIT 10""",
        (cohort_id,),
    ):
        recent.append(dict(row))
    return {
        **counts,
        "pending_entries": max(0, counts["candidates"] - counts["entries"] - counts["censors"]),
        "pending_outcomes": max(0, counts["entries"] - counts["outcomes"] - counts["censors"]),
        "arms": arms,
        "recent_candidates": recent,
    }


def render_report(payload: Mapping[str, Any]) -> str:
    ledger = payload.get("ledger") or {}
    lines = [
        "# News x causal-band resolution x quote-flow H15",
        "",
        f"Generated: `{payload.get('generated_utc')}`",
        "",
        "Research-only. This cohort cannot route, authorize, promote, or place an order.",
        "",
        f"- Status: **{payload.get('status')}**",
        f"- Candidates / entries / outcomes / censors: **{ledger.get('candidates', 0)} / {ledger.get('entries', 0)} / {ledger.get('outcomes', 0)} / {ledger.get('censors', 0)}**",
        f"- New candidates / entries / outcomes this cycle: **{payload.get('inserted_candidates', 0)} / {(payload.get('maturation_this_cycle') or {}).get('entries', 0)} / {(payload.get('maturation_this_cycle') or {}).get('outcomes', 0)}**",
        "",
        "| Arm | N | Fixed H15 mean | Fixed win | Invalidation mean | Invalidation win |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for arm, row in sorted((ledger.get("arms") or {}).items()):
        lines.append(
            f"| {arm} | {row['n']} | {row['fixed_h15_mean_net_pips']:.3f} | "
            f"{100*row['fixed_h15_after_cost_win_rate']:.1f}% | "
            f"{row['invalidation_mean_net_pips']:.3f} | "
            f"{100*row['invalidation_after_cost_win_rate']:.1f}% |"
        )
    return "\n".join(lines) + "\n"


def run_once(
    *,
    config_path: Path = CONFIG,
    watchlist_path: Path = WATCHLIST,
    band_ledger_path: Path = BAND_LEDGER,
    quote_ledger_path: Path = QUOTE_LEDGER,
    update_report_path: Path = UPDATE_REPORT,
    clock_path: Path = CLOCK,
    ledger_path: Path = LEDGER,
    output_path: Path = OUTPUT,
    heartbeat_path: Path = HEARTBEAT,
    report_path: Path = REPORT,
    observed: dt.datetime | None = None,
) -> dict[str, Any]:
    if observed is None:
        observed, clock = trusted_observation_time(utc_now(), clock_path)
    else:
        observed = observed.astimezone(UTC)
        clock = {
            "status": "provided_test_clock",
            "trusted_for_prospective_evidence": True,
            "source": "provided",
        }
    config = load_contract(config_path)
    identity = collection_identity(config_path)
    db = open_ledger(ledger_path)
    register_cohort(db, identity, observed)
    clock_trusted = bool(clock.get("trusted_for_prospective_evidence"))
    watchlist = read_json(watchlist_path)
    news_rows, watchlist_state = load_active_news(watchlist, observed, config)
    forecasts = _read_band_forecasts(band_ledger_path, observed, config)
    quote_rows = _read_quote_minutes(
        quote_ledger_path,
        (row.get("instrument") for row in forecasts),
        observed,
        safe_float(config.get("maximum_band_forecast_age_sec"), 1800.0),
    )
    existing = {
        (str(row[0]), str(row[1]))
        for row in db.execute(
            "SELECT episode_id,currency FROM timing_candidates WHERE cohort_id=?",
            (identity["cohort_id"],),
        )
    }
    candidates = (
        build_candidate_payloads(
            news_rows, forecasts, quote_rows, observed, config, existing
        )
        if clock_trusted and watchlist_state == "fresh"
        else []
    )
    inserted = 0
    for candidate in candidates:
        _, was_inserted = insert_candidate(db, identity, candidate)
        inserted += int(was_inserted)
    db.commit()
    unresolved_instruments = pending_instruments(db)
    bars, bam_state = _load_bars_for_instruments(update_report_path, unresolved_instruments)
    maturation = (
        append_entries_and_outcomes(
            db, observed, bars, int(config["horizon_sec"])
        )
        if clock_trusted
        else {"entries": 0, "outcomes": 0, "censors": 0}
    )
    ledger = ledger_summary(db, identity["cohort_id"])
    db.close()
    blocking = []
    if not clock_trusted:
        blocking.append("clock_untrusted")
    if watchlist_state != "fresh":
        blocking.append(watchlist_state)
    if not band_ledger_path.is_file():
        blocking.append("band_ledger_missing")
    if not quote_ledger_path.is_file():
        blocking.append("quote_intensity_ledger_missing")
    status = "running" if not blocking else "blocked"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": identity["contract_id"],
        "cohort_id": identity["cohort_id"],
        "generated_utc": iso(observed),
        "status": status,
        "blocking_reasons": blocking,
        "observation_clock": clock,
        "watchlist_state": watchlist_state,
        "bam_state": bam_state,
        "active_news_row_count": len(news_rows),
        "recent_band_forecast_count": len(forecasts),
        "quote_instrument_count": len(quote_rows),
        "candidate_count_before_factor_dedup": len(candidates),
        "inserted_candidates": inserted,
        "maturation_this_cycle": maturation,
        "ledger": ledger,
        "policy": config["policy"],
        "methodology": {
            "direction": "point_in_time_news_only",
            "entry_timing": config["resolution_contract"],
            "flow": "completed_resolution_minute_signed_quote_change_intensity",
            "entry": config["entry_contract"],
            "exit": config["exit_contract"],
            "factor_deduplication": config["factor_deduplication"],
            "matched_no_flow_control": True,
            "same_minute_contact_and_resolution_allowed": False,
        },
        "database": str(ledger_path),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
    }
    atomic_json(output_path, payload)
    atomic_json(
        heartbeat_path,
        {
            "schema_version": 1,
            "worker": "news_band_resolution_flow_h15",
            "generated_utc": iso(observed),
            "status": status,
            "cohort_id": identity["cohort_id"],
            "candidate_count": ledger["candidates"],
            "outcome_count": ledger["outcomes"],
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
        },
    )
    atomic_text(report_path, render_report(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--watchlist", type=Path, default=WATCHLIST)
    parser.add_argument("--band-ledger", type=Path, default=BAND_LEDGER)
    parser.add_argument("--quote-ledger", type=Path, default=QUOTE_LEDGER)
    parser.add_argument("--update-report", type=Path, default=UPDATE_REPORT)
    parser.add_argument("--clock", type=Path, default=CLOCK)
    parser.add_argument("--ledger", type=Path, default=LEDGER)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        payload = run_once(
            config_path=args.config,
            watchlist_path=args.watchlist,
            band_ledger_path=args.band_ledger,
            quote_ledger_path=args.quote_ledger,
            update_report_path=args.update_report,
            clock_path=args.clock,
            ledger_path=args.ledger,
            output_path=args.output,
            heartbeat_path=args.heartbeat,
            report_path=args.report,
        )
        if args.once or args.duration_sec <= 0.0:
            print(json.dumps({"status": payload["status"], "cohort_id": payload["cohort_id"], "ledger": payload["ledger"]}, sort_keys=True))
            return 0
        if time.monotonic() - started >= args.duration_sec:
            return 0
        time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
