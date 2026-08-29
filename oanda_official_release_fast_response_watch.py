#!/usr/bin/env python3
"""Prospective official-release price/technical response watch.

This worker is intentionally downstream of the exact V4 raw lane and V3
semantic mapper.  It opens immutable, research-only 1/5/15/30/60/120 minute watches for
genuinely prospective mapper candidates, records executable bid/ask entry
economics, and matures outcomes from later executable quotes.  It has no
broker, lifecycle, authorization, promotion, or canonical-watchlist surface.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from oanda_news_classification_contract import NEWS_CLASSIFICATION_VERSION


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
LOCAL_NEWS = DATA / "local_news_sentiment"
STATE = DATA / "state"

INPUT_DATABASE = LOCAL_NEWS / "official_release_fast_mapping_v3.sqlite"
QUOTE_PATH = STATE / "practice_007_market_quotes_v1.json"
TECHNICAL_PATH = STATE / "practice_007_signal_snapshot_research_v1.json"
OUTPUT_DATABASE = LOCAL_NEWS / "official_release_fast_response_watch_v3.sqlite"
SNAPSHOT_PATH = LOCAL_NEWS / "official_release_fast_response_watch_latest_v3.json"
HEARTBEAT_PATH = LOCAL_NEWS / "official_release_fast_response_watch_heartbeat_v3.json"

SCHEMA_VERSION = "official_release_fast_response_watch_v3"
CONTRACT_ID = "official_release_fast_response_watch_v3_multi_horizon_exact_quote_20260825"
COHORT_ID = "official_release_fast_response_watch_v3_20260825"
REQUIRED_MAPPER_CONTRACT = "official_release_fast_mapping_v3_semantic_vs_publish_gate_20260824"
# Bind to the same frozen classifier contract as the upstream mapper. Keeping
# a duplicated literal here previously left the response worker silently one
# classifier generation behind after a validated semantic repair.
REQUIRED_CLASSIFICATION_VERSION = NEWS_CLASSIFICATION_VERSION
HORIZONS_MIN = (1, 5, 15, 30, 60, 120)
MAX_QUOTE_AGE_SEC = 30.0
MAX_DECISION_LATENCY_SEC = 90.0
MAX_MATURITY_QUOTE_DELAY_SEC = 120.0
INVALID_MATURITY_AFTER_SEC = 300.0

POLICY = {
    "research_only": True,
    "execution_eligible": False,
    "can_place_orders": False,
    "can_authorize": False,
    "can_promote": False,
    "broker_access": False,
    "canonical_watchlist_mutation": False,
    "supported_decision": "shadow_observation_only",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def digest(*parts: Any) -> str:
    raw = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def finite_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
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


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                return
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(min(0.5, 0.01 * (2**attempt)))
    finally:
        temporary.unlink(missing_ok=True)


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS response_watch (
            watch_id TEXT PRIMARY KEY,
            mapping_id TEXT NOT NULL,
            event_factor_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            currency_score REAL NOT NULL,
            instrument TEXT NOT NULL,
            side TEXT NOT NULL CHECK(side IN ('buy','sell')),
            horizon_min INTEGER NOT NULL,
            first_seen_utc TEXT NOT NULL,
            mapped_utc TEXT NOT NULL,
            decision_utc TEXT NOT NULL,
            decision_latency_sec REAL NOT NULL,
            quote_time_utc TEXT NOT NULL,
            bid REAL NOT NULL,
            ask REAL NOT NULL,
            pip REAL NOT NULL,
            entry_spread_pips REAL NOT NULL,
            technical_direction TEXT NOT NULL,
            technical_confidence REAL,
            technical_signal_eligible INTEGER NOT NULL CHECK(technical_signal_eligible IN (0,1)),
            technical_relation TEXT NOT NULL,
            technical_snapshot_utc TEXT,
            watch_payload_json TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
            can_promote INTEGER NOT NULL CHECK(can_promote=0),
            contract_id TEXT NOT NULL,
            cohort_id TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_response_watch_target
            ON response_watch(decision_utc, horizon_min);
        CREATE INDEX IF NOT EXISTS idx_response_watch_factor
            ON response_watch(event_factor_id, horizon_min, entry_spread_pips);
        CREATE TABLE IF NOT EXISTS response_outcome (
            outcome_id TEXT PRIMARY KEY,
            watch_id TEXT NOT NULL UNIQUE,
            target_utc TEXT NOT NULL,
            observed_utc TEXT NOT NULL,
            maturity_state TEXT NOT NULL,
            exit_quote_time_utc TEXT,
            exit_bid REAL,
            exit_ask REAL,
            signed_mid_move_pips REAL,
            executable_net_pips REAL,
            realized_cost_drag_pips REAL,
            outcome_payload_json TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            contract_id TEXT NOT NULL,
            cohort_id TEXT NOT NULL,
            FOREIGN KEY(watch_id) REFERENCES response_watch(watch_id)
        );
        """
    )
    return connection


def currency_scores(payload: Mapping[str, Any]) -> dict[str, float]:
    combined: dict[str, float] = {}
    for key in ("research_currency_scores", "currency_scores"):
        values = payload.get(key)
        if not isinstance(values, Mapping):
            continue
        for currency, raw in values.items():
            score = finite_number(raw)
            code = str(currency or "").upper()
            if len(code) == 3 and score is not None and score != 0.0:
                combined[code] = score
    return combined


def side_for_currency(instrument: str, currency: str, score: float) -> str | None:
    try:
        base, quote = instrument.upper().split("_", 1)
    except ValueError:
        return None
    if currency == base:
        return "buy" if score > 0.0 else "sell"
    if currency == quote:
        return "sell" if score > 0.0 else "buy"
    return None


def technical_at_horizon(
    technical_payload: Mapping[str, Any], instrument: str, horizon_min: int
) -> dict[str, Any]:
    target_sec = int(horizon_min) * 60
    rows = technical_payload.get("top_signals")
    if not isinstance(rows, list):
        rows = []
    signal = next(
        (
            row
            for row in rows
            if isinstance(row, Mapping)
            and str(row.get("instrument") or "").upper() == instrument.upper()
        ),
        None,
    )
    if not isinstance(signal, Mapping):
        return {
            "direction": "unavailable",
            "confidence": None,
            "signal_eligible": False,
            "source": "missing_pair_signal",
        }
    breakdown = signal.get("horizon_breakdown")
    if not isinstance(breakdown, list):
        breakdown = []
    horizon = next(
        (
            row
            for row in breakdown
            if isinstance(row, Mapping)
            and int(finite_number(row.get("horizon_sec")) or -1) == target_sec
        ),
        None,
    )
    source = horizon if isinstance(horizon, Mapping) else signal
    direction = str(
        source.get("direction_state") or source.get("direction") or "neutral"
    ).lower()
    if direction not in {"buy", "sell"}:
        direction = "neutral"
    return {
        "direction": direction,
        "confidence": finite_number(source.get("signal_confidence")),
        "signal_eligible": bool(source.get("signal_eligible", False)),
        "source": "exact_horizon" if horizon is not None else "pair_fallback",
        "projected_net_pips": finite_number(source.get("projected_net_pips")),
        "blocked_by": list(source.get("signal_blocked_by") or []),
    }


def event_factor_id(currency: str, source_id: str, first_seen: datetime) -> str:
    # Conservative five-minute currency/source episode collapse. Multiple
    # documents or pair expressions cannot manufacture independent evidence.
    minute = (first_seen.minute // 5) * 5
    bucket = first_seen.replace(minute=minute, second=0, microsecond=0)
    return "official_fast_factor_" + digest(currency, source_id, iso(bucket))[:24]


def build_watch_rows(
    mapping: Mapping[str, Any],
    quote_payload: Mapping[str, Any],
    technical_payload: Mapping[str, Any],
    decision_utc: datetime,
) -> list[dict[str, Any]]:
    if int(mapping.get("forward_shadow_candidate") or 0) != 1:
        return []
    if int(mapping.get("input_prospective_observation") or 0) != 1:
        return []
    if str(mapping.get("mapper_contract_id") or "") != REQUIRED_MAPPER_CONTRACT:
        return []
    if str(mapping.get("classification_version") or "") != REQUIRED_CLASSIFICATION_VERSION:
        return []
    first_seen = parse_time(mapping.get("first_seen_utc"))
    mapped = parse_time(mapping.get("mapped_utc"))
    if first_seen is None or mapped is None:
        return []
    decision_utc = decision_utc.astimezone(timezone.utc)
    latency = (decision_utc - first_seen).total_seconds()
    if latency < 0.0 or latency > MAX_DECISION_LATENCY_SEC:
        return []
    raw_payload = mapping.get("mapping_payload")
    if not isinstance(raw_payload, Mapping):
        return []
    scores = currency_scores(raw_payload)
    quotes = quote_payload.get("quotes")
    if not isinstance(quotes, Mapping):
        return []
    technical_time = parse_time(technical_payload.get("updated_at"))
    rows: list[dict[str, Any]] = []
    for currency, score in sorted(scores.items()):
        factor_id = event_factor_id(
            currency, str(mapping.get("source_id") or ""), first_seen
        )
        for instrument, raw_quote in sorted(quotes.items()):
            if not isinstance(raw_quote, Mapping):
                continue
            side = side_for_currency(str(instrument), currency, score)
            if side is None:
                continue
            bid = finite_number(raw_quote.get("bid"))
            ask = finite_number(raw_quote.get("ask"))
            pip = finite_number(raw_quote.get("pip"))
            quote_time = parse_time(raw_quote.get("time"))
            if (
                bid is None
                or ask is None
                or pip is None
                or pip <= 0.0
                or ask <= bid
                or quote_time is None
            ):
                continue
            quote_age = (decision_utc - quote_time).total_seconds()
            after_first_seen = (quote_time - first_seen).total_seconds()
            if quote_age < -2.0 or quote_age > MAX_QUOTE_AGE_SEC:
                continue
            if after_first_seen < 0.0 or after_first_seen > MAX_DECISION_LATENCY_SEC:
                continue
            spread_pips = (ask - bid) / pip
            for horizon_min in HORIZONS_MIN:
                technical = technical_at_horizon(
                    technical_payload, str(instrument), horizon_min
                )
                tech_direction = str(technical["direction"])
                relation = (
                    "aligned"
                    if tech_direction == side
                    else "conflicted"
                    if tech_direction in {"buy", "sell"}
                    else "neutral_or_unavailable"
                )
                watch_id = "official_fast_watch_" + digest(
                    mapping.get("mapping_id"), currency, instrument, horizon_min
                )[:32]
                row = {
                    "watch_id": watch_id,
                    "mapping_id": str(mapping.get("mapping_id") or ""),
                    "event_factor_id": factor_id,
                    "source_id": str(mapping.get("source_id") or ""),
                    "currency": currency,
                    "currency_score": score,
                    "instrument": str(instrument),
                    "side": side,
                    "horizon_min": horizon_min,
                    "first_seen_utc": iso(first_seen),
                    "mapped_utc": iso(mapped),
                    "decision_utc": iso(decision_utc),
                    "decision_latency_sec": latency,
                    "quote_time_utc": iso(quote_time),
                    "bid": bid,
                    "ask": ask,
                    "pip": pip,
                    "entry_spread_pips": spread_pips,
                    "technical_direction": tech_direction,
                    "technical_confidence": technical.get("confidence"),
                    "technical_signal_eligible": bool(
                        technical.get("signal_eligible")
                    ),
                    "technical_relation": relation,
                    "technical_snapshot_utc": (
                        iso(technical_time) if technical_time else None
                    ),
                    "technical_diagnostic": technical,
                    "research_only": True,
                    "execution_eligible": False,
                }
                rows.append(row)
    return rows


def insert_watches(connection: sqlite3.Connection, rows: Iterable[Mapping[str, Any]]) -> int:
    inserted = 0
    for row in rows:
        before = connection.total_changes
        connection.execute(
            """
            INSERT OR IGNORE INTO response_watch (
                watch_id,mapping_id,event_factor_id,source_id,currency,currency_score,
                instrument,side,horizon_min,first_seen_utc,mapped_utc,decision_utc,
                decision_latency_sec,quote_time_utc,bid,ask,pip,entry_spread_pips,
                technical_direction,technical_confidence,technical_signal_eligible,
                technical_relation,technical_snapshot_utc,watch_payload_json,
                research_only,execution_eligible,can_authorize,can_promote,
                contract_id,cohort_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,0,0,?,?)
            """,
            (
                row["watch_id"], row["mapping_id"], row["event_factor_id"],
                row["source_id"], row["currency"], row["currency_score"],
                row["instrument"], row["side"], row["horizon_min"],
                row["first_seen_utc"], row["mapped_utc"], row["decision_utc"],
                row["decision_latency_sec"], row["quote_time_utc"], row["bid"],
                row["ask"], row["pip"], row["entry_spread_pips"],
                row["technical_direction"], row.get("technical_confidence"),
                int(bool(row["technical_signal_eligible"])),
                row["technical_relation"], row.get("technical_snapshot_utc"),
                json.dumps(dict(row), sort_keys=True, separators=(",", ":")),
                CONTRACT_ID, COHORT_ID,
            ),
        )
        inserted += connection.total_changes - before
    return inserted


def build_outcome(
    watch: Mapping[str, Any], raw_quote: Mapping[str, Any], observed_utc: datetime
) -> dict[str, Any] | None:
    decision = parse_time(watch.get("decision_utc"))
    if decision is None:
        return None
    target = decision + timedelta(minutes=int(watch["horizon_min"]))
    quote_time = parse_time(raw_quote.get("time"))
    bid = finite_number(raw_quote.get("bid"))
    ask = finite_number(raw_quote.get("ask"))
    if quote_time is None or bid is None or ask is None or ask <= bid:
        return None
    delay = (quote_time - target).total_seconds()
    if delay < 0.0:
        return None
    if delay > MAX_MATURITY_QUOTE_DELAY_SEC:
        return {
            "outcome_id": "official_fast_outcome_" + digest(watch["watch_id"]),
            "watch_id": watch["watch_id"],
            "target_utc": iso(target),
            "observed_utc": iso(observed_utc),
            "maturity_state": "late_quote_invalid",
            "exit_quote_time_utc": iso(quote_time),
            "exit_bid": bid,
            "exit_ask": ask,
            "signed_mid_move_pips": None,
            "executable_net_pips": None,
            "realized_cost_drag_pips": None,
        }
    entry_bid = float(watch["bid"])
    entry_ask = float(watch["ask"])
    pip = float(watch["pip"])
    entry_mid = (entry_bid + entry_ask) / 2.0
    exit_mid = (bid + ask) / 2.0
    sign = 1.0 if str(watch["side"]) == "buy" else -1.0
    signed_mid = sign * (exit_mid - entry_mid) / pip
    executable = (
        (bid - entry_ask) / pip
        if str(watch["side"]) == "buy"
        else (entry_bid - ask) / pip
    )
    return {
        "outcome_id": "official_fast_outcome_" + digest(watch["watch_id"]),
        "watch_id": watch["watch_id"],
        "target_utc": iso(target),
        "observed_utc": iso(observed_utc),
        "maturity_state": "valid_exact_executable_quote",
        "exit_quote_time_utc": iso(quote_time),
        "exit_bid": bid,
        "exit_ask": ask,
        "signed_mid_move_pips": signed_mid,
        "executable_net_pips": executable,
        "realized_cost_drag_pips": signed_mid - executable,
    }


def mature_outcomes(
    connection: sqlite3.Connection,
    quote_payload: Mapping[str, Any],
    observed_utc: datetime,
) -> int:
    quotes = quote_payload.get("quotes")
    if not isinstance(quotes, Mapping):
        quotes = {}
    columns = [row[1] for row in connection.execute("PRAGMA table_info(response_watch)")]
    pending = connection.execute(
        """
        SELECT w.* FROM response_watch w
        LEFT JOIN response_outcome o ON o.watch_id=w.watch_id
        WHERE o.watch_id IS NULL
        """
    ).fetchall()
    inserted = 0
    for values in pending:
        watch = dict(zip(columns, values))
        decision = parse_time(watch["decision_utc"])
        if decision is None:
            continue
        target = decision + timedelta(minutes=int(watch["horizon_min"]))
        if observed_utc < target:
            continue
        raw_quote = quotes.get(watch["instrument"])
        outcome = (
            build_outcome(watch, raw_quote, observed_utc)
            if isinstance(raw_quote, Mapping)
            else None
        )
        if outcome is None and (observed_utc - target).total_seconds() >= INVALID_MATURITY_AFTER_SEC:
            outcome = {
                "outcome_id": "official_fast_outcome_" + digest(watch["watch_id"]),
                "watch_id": watch["watch_id"],
                "target_utc": iso(target),
                "observed_utc": iso(observed_utc),
                "maturity_state": "missing_target_quote_invalid",
                "exit_quote_time_utc": None,
                "exit_bid": None,
                "exit_ask": None,
                "signed_mid_move_pips": None,
                "executable_net_pips": None,
                "realized_cost_drag_pips": None,
            }
        if outcome is None:
            continue
        before = connection.total_changes
        connection.execute(
            """
            INSERT OR IGNORE INTO response_outcome (
                outcome_id,watch_id,target_utc,observed_utc,maturity_state,
                exit_quote_time_utc,exit_bid,exit_ask,signed_mid_move_pips,
                executable_net_pips,realized_cost_drag_pips,outcome_payload_json,
                research_only,execution_eligible,contract_id,cohort_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,0,?,?)
            """,
            (
                outcome["outcome_id"], outcome["watch_id"], outcome["target_utc"],
                outcome["observed_utc"], outcome["maturity_state"],
                outcome.get("exit_quote_time_utc"), outcome.get("exit_bid"),
                outcome.get("exit_ask"), outcome.get("signed_mid_move_pips"),
                outcome.get("executable_net_pips"),
                outcome.get("realized_cost_drag_pips"),
                json.dumps(outcome, sort_keys=True, separators=(",", ":")),
                CONTRACT_ID, COHORT_ID,
            ),
        )
        inserted += connection.total_changes - before
    return inserted


def load_candidates(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT mapping_id,source_id,first_seen_utc,input_prospective_observation,
               classification_version,forward_shadow_candidate,mapping_payload_json,
               mapped_utc,mapper_contract_id,mapper_cohort_id
        FROM official_release_mapping
        WHERE forward_shadow_candidate=1
        ORDER BY first_seen_utc,mapping_id
        """
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        payload = json.loads(row[6])
        result.append(
            {
                "mapping_id": row[0], "source_id": row[1], "first_seen_utc": row[2],
                "input_prospective_observation": row[3],
                "classification_version": row[4], "forward_shadow_candidate": row[5],
                "mapping_payload": payload, "mapped_utc": row[7],
                "mapper_contract_id": row[8], "mapper_cohort_id": row[9],
            }
        )
    return result


def counts(connection: sqlite3.Connection) -> dict[str, Any]:
    watch = connection.execute(
        """SELECT COUNT(*),COUNT(DISTINCT mapping_id),COUNT(DISTINCT event_factor_id),
                  COUNT(DISTINCT instrument),
                  SUM(CASE WHEN technical_relation='aligned' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN technical_relation='conflicted' THEN 1 ELSE 0 END)
           FROM response_watch"""
    ).fetchone()
    outcome = connection.execute(
        """SELECT COUNT(*),
                  SUM(CASE WHEN maturity_state='valid_exact_executable_quote' THEN 1 ELSE 0 END)
           FROM response_outcome"""
    ).fetchone()
    return {
        "watches": int(watch[0] or 0),
        "mapping_events": int(watch[1] or 0),
        "factor_episodes": int(watch[2] or 0),
        "instruments": int(watch[3] or 0),
        "technical_aligned_watches": int(watch[4] or 0),
        "technical_conflicted_watches": int(watch[5] or 0),
        "outcomes": int(outcome[0] or 0),
        "valid_outcomes": int(outcome[1] or 0),
    }


def factor_results(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        WITH ranked AS (
          SELECT w.event_factor_id,w.horizon_min,w.instrument,w.technical_relation,
                 w.entry_spread_pips,o.executable_net_pips,
                 ROW_NUMBER() OVER (
                   PARTITION BY w.event_factor_id,w.horizon_min
                   ORDER BY w.entry_spread_pips,w.instrument
                 ) AS rank_in_factor
          FROM response_watch w JOIN response_outcome o ON o.watch_id=w.watch_id
          WHERE o.maturity_state='valid_exact_executable_quote'
        )
        SELECT horizon_min,COUNT(*),AVG(executable_net_pips),
               AVG(CASE WHEN executable_net_pips>0 THEN 1.0 ELSE 0.0 END)
        FROM ranked WHERE rank_in_factor=1 GROUP BY horizon_min ORDER BY horizon_min
        """
    ).fetchall()
    return [
        {
            "horizon_min": int(row[0]), "effective_factor_n": int(row[1]),
            "average_executable_net_pips": row[2], "after_cost_win_rate": row[3],
        }
        for row in rows
    ]


def cycle(
    *, input_database: Path = INPUT_DATABASE, output_database: Path = OUTPUT_DATABASE,
    quote_path: Path = QUOTE_PATH, technical_path: Path = TECHNICAL_PATH,
    snapshot_path: Path = SNAPSHOT_PATH, heartbeat_path: Path = HEARTBEAT_PATH,
    observed_utc: datetime | None = None,
) -> dict[str, Any]:
    observed = (observed_utc or utc_now()).astimezone(timezone.utc)
    atomic_write_json(
        heartbeat_path,
        {
            "schema_version": SCHEMA_VERSION, "contract_id": CONTRACT_ID,
            "cohort_id": COHORT_ID, "heartbeat_utc": iso(observed),
            "required_classification_version": REQUIRED_CLASSIFICATION_VERSION,
            "cycle_in_progress": True, "phase": "loading_inputs",
            "status": "running_cycle", "policy": POLICY,
        },
    )
    if not input_database.exists():
        raise FileNotFoundError(input_database)
    input_connection = sqlite3.connect(input_database, timeout=30.0)
    try:
        candidates = load_candidates(input_connection)
    finally:
        input_connection.close()
    quotes = read_json(quote_path)
    technical = read_json(technical_path)
    output = connect(output_database)
    try:
        inserted_watches = 0
        for candidate in candidates:
            inserted_watches += insert_watches(
                output, build_watch_rows(candidate, quotes, technical, observed)
            )
        inserted_outcomes = mature_outcomes(output, quotes, observed)
        output.commit()
        integrity = str(output.execute("PRAGMA quick_check").fetchone()[0])
        census = counts(output)
        results = factor_results(output)
    finally:
        output.close()
    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "generated_utc": iso(observed),
        "required_mapper_contract_id": REQUIRED_MAPPER_CONTRACT,
        "required_classification_version": REQUIRED_CLASSIFICATION_VERSION,
        "horizons_min": list(HORIZONS_MIN),
        "candidate_mappings_seen": len(candidates),
        "inserted_watches": inserted_watches,
        "inserted_outcomes": inserted_outcomes,
        "counts": census,
        "factor_deduplicated_results": results,
        "sqlite_integrity": integrity,
        "policy": POLICY,
    }
    atomic_write_json(snapshot_path, snapshot)
    atomic_write_json(
        heartbeat_path,
        {
            "schema_version": SCHEMA_VERSION, "contract_id": CONTRACT_ID,
            "cohort_id": COHORT_ID, "heartbeat_utc": iso(utc_now()),
            "required_classification_version": REQUIRED_CLASSIFICATION_VERSION,
            "cycle_in_progress": False, "phase": "cycle_complete",
            "status": "cycle_complete", "details": census, "policy": POLICY,
        },
    )
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval-sec", type=float, default=5.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        try:
            cycle()
        except Exception as exc:  # preserve a fail-closed observable heartbeat
            atomic_write_json(
                HEARTBEAT_PATH,
                {
                    "schema_version": SCHEMA_VERSION, "contract_id": CONTRACT_ID,
                    "cohort_id": COHORT_ID, "heartbeat_utc": iso(utc_now()),
                    "required_classification_version": REQUIRED_CLASSIFICATION_VERSION,
                    "cycle_in_progress": False, "phase": "error",
                    "status": "error",
                    "error": f"{type(exc).__name__}: {exc}", "policy": POLICY,
                },
            )
        if args.duration_sec <= 0.0:
            break
        if time.monotonic() - started >= args.duration_sec:
            break
        time.sleep(max(0.5, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
