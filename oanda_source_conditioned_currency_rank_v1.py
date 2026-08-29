#!/usr/bin/env python3
"""Prospective source-conditioned 21-currency rank comparison (research only).

This adapter deliberately starts *after* source interpretation.  It consumes
only proof-eligible forecasts from the causal source-factor response ledger,
so it does not duplicate article parsing, sentiment scoring, or the
news/technical watchlist.  At each new independent source episode it freezes
three comparable counterfactual arms:

* ``price_only`` -- the existing 21-currency price rank;
* ``source_only`` -- opposing, concurrently available causal currency views;
* ``source_price_timing`` -- source direction with price/spread as a timing
  and rejection layer.

The module has no broker, signal-feed, lifecycle, promotion, authorization, or
execution imports.  Missing source state remains unavailable rather than zero.
Every selected counterfactual uses an exact executable entry quote and matures
against the first completed bid/ask quote bar at its declared horizon.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import statistics
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import oanda_currency_rank_model as price_rank


UTC = timezone.utc
ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
LOCAL_NEWS = DATA / "local_news_sentiment"
STATE = DATA / "state"
REPORT_ROOT = DATA / "reports" / "source_conditioned_currency_rank"

SOURCE_V2 = LOCAL_NEWS / "causal_source_factor_response_map_v2.sqlite"
DEFAULT_TICKER = DATA / "market_sentiment_ticker" / "LATEST.json"
DEFAULT_QUOTES = STATE / "practice_007_market_quotes_v1.json"
DEFAULT_QUOTE_BARS = ROOT / "data" / "significant_moves" / "live_alerts" / "move_alerts_v1.sqlite"
DEFAULT_LEDGER = STATE / "source_conditioned_currency_rank_v1.sqlite"
DEFAULT_STATE = STATE / "source_conditioned_currency_rank_v1.json"
DEFAULT_REPORT = REPORT_ROOT / "SOURCE_CONDITIONED_CURRENCY_RANK_V1.md"
DEFAULT_MANIFEST = ROOT / "config" / "source_conditioned_currency_rank_v1.json"

SCHEMA_VERSION = "source_conditioned_currency_rank_v1"
CONTRACT_ID = "source_conditioned_currency_rank_v1_point_in_time_v1_20260828"
BASE_COHORT_ID = "source_conditioned_currency_rank_v1_prospective_20260828"
ARMS = ("price_only", "source_only", "source_price_timing")

REQUIRED_SOURCE_FORECAST_COLUMNS = {
    "forecast_id", "canonical_event_id", "currency", "horizon_min",
    "issued_utc", "training_cutoff_utc", "forecast_state",
    "effective_event_n", "probability_strengthening",
    "predicted_currency_factor_bps", "predicted_absolute_factor_bps",
    "prospective_proof_eligible", "contract_id", "cohort_id",
}


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def number(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def digest(*parts: Any) -> str:
    material = "\x1f".join(str(part) for part in parts)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def canonical_json_hash(payload: Mapping[str, Any]) -> str:
    material = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def adapter_definition_sha256() -> str:
    return digest(CONTRACT_ID, file_sha256(Path(__file__)), file_sha256(DEFAULT_MANIFEST))


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def resolve_source_database(explicit: Path | None = None) -> Path:
    if explicit is not None:
        return explicit.resolve()
    # V1 is sealed.  Default/live operation must never silently cross back
    # into that contract when V2 is absent.  An explicit path remains
    # available only for isolated tests and point-in-time replay.
    return SOURCE_V2.resolve()


def _read_only_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=15.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=15000")
    return connection


def load_source_forecasts(path: Path) -> list[dict[str, Any]]:
    """Load only causal, prospective, non-abstaining source forecasts."""
    if not path.exists():
        return []
    connection = _read_only_connection(path)
    try:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        required_tables = {
            "source_factor_forecast", "source_event_episode",
            "source_event_observation",
        }
        if not required_tables.issubset(tables):
            raise ValueError("source factor database lacks required causal tables")
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(source_factor_forecast)")
        }
        missing = REQUIRED_SOURCE_FORECAST_COLUMNS - columns
        if missing:
            raise ValueError(f"source forecast schema missing: {sorted(missing)}")
        rows = connection.execute(
            """
            SELECT f.forecast_id,f.canonical_event_id,f.currency,f.factor_key,
                   f.horizon_min,f.issued_utc,f.training_cutoff_utc,
                   f.forecast_state,f.effective_event_n,
                   f.probability_strengthening,
                   f.predicted_currency_factor_bps,
                   f.predicted_absolute_factor_bps,
                   f.contract_id source_contract_id,
                   f.cohort_id source_cohort_id,
                   ep.market_episode_id,e.first_known_utc
            FROM source_factor_forecast f
            JOIN source_event_episode ep
              ON ep.canonical_event_id=f.canonical_event_id
            JOIN source_event_observation e
              ON e.canonical_event_id=f.canonical_event_id
            WHERE f.forecast_state='forecast'
              AND f.prospective_proof_eligible=1
              AND f.probability_strengthening IS NOT NULL
              AND f.predicted_currency_factor_bps IS NOT NULL
              AND f.predicted_absolute_factor_bps IS NOT NULL
            ORDER BY f.issued_utc,f.forecast_id
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def group_source_forecasts(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Collapse correlated factor rows to episode x currency x horizon."""
    grouped: dict[tuple[str, str, int], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        episode = str(row.get("market_episode_id") or "").strip()
        currency = str(row.get("currency") or "").upper().strip()
        horizon = int(number(row.get("horizon_min"), 0.0) or 0)
        if not episode or currency not in price_rank.EXPECTED_CURRENCIES or horizon <= 0:
            continue
        grouped[(episode, currency, horizon)].append(row)

    result: list[dict[str, Any]] = []
    for (episode, currency, horizon), members in sorted(grouped.items()):
        issued_values = [parse_time(row.get("issued_utc")) for row in members]
        issued = max((value for value in issued_values if value is not None), default=None)
        if issued is None:
            continue
        knowledge_values = issued_values + [
            parse_time(row.get("training_cutoff_utc")) for row in members
        ]
        knowledge_cutoff = max(
            (value for value in knowledge_values if value is not None),
            default=issued,
        )
        predicted = [float(row["predicted_currency_factor_bps"]) for row in members]
        magnitudes = [float(row["predicted_absolute_factor_bps"]) for row in members]
        probabilities = [float(row["probability_strengthening"]) for row in members]
        result.append(
            {
                "market_episode_id": episode,
                "currency": currency,
                "horizon_min": horizon,
                "issued_utc": iso(issued),
                "knowledge_cutoff_utc": iso(knowledge_cutoff),
                "predicted_currency_factor_bps": statistics.median(predicted),
                "predicted_absolute_factor_bps": statistics.median(magnitudes),
                "probability_strengthening": statistics.median(probabilities),
                # Conservative support prevents several factors from one event
                # from manufacturing a larger effective N.
                "effective_event_n": min(int(row["effective_event_n"]) for row in members),
                "canonical_event_ids": sorted({str(row["canonical_event_id"]) for row in members}),
                "source_forecast_ids": sorted({str(row["forecast_id"]) for row in members}),
                "factor_keys": sorted({str(row.get("factor_key") or "") for row in members}),
                "source_contract_ids": sorted({str(row["source_contract_id"]) for row in members}),
                "source_cohort_ids": sorted({str(row["source_cohort_id"]) for row in members}),
            }
        )
    return result


def source_direction(row: Mapping[str, Any], probability_floor: float = 0.55) -> str:
    predicted = number(row.get("predicted_currency_factor_bps"))
    probability = number(row.get("probability_strengthening"))
    if predicted is None or probability is None:
        return "unavailable"
    if predicted > 0.0 and probability >= probability_floor:
        return "strengthen"
    if predicted < 0.0 and probability <= 1.0 - probability_floor:
        return "weaken"
    return "conflicted"


def active_currency_source_state(
    groups: Sequence[Mapping[str, Any]],
    *,
    cutoff: datetime,
    horizon_min: int,
    probability_floor: float = 0.55,
) -> dict[str, dict[str, Any]]:
    by_currency: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in groups:
        if int(row["horizon_min"]) != int(horizon_min):
            continue
        issued = parse_time(row.get("issued_utc"))
        if issued is None or issued > cutoff or cutoff > issued + timedelta(minutes=horizon_min):
            continue
        by_currency[str(row["currency"])].append(row)

    result: dict[str, dict[str, Any]] = {}
    for currency, rows in by_currency.items():
        values = [float(row["predicted_currency_factor_bps"]) for row in rows]
        probabilities = [float(row["probability_strengthening"]) for row in rows]
        knowledge_cutoff = trigger_knowledge_cutoff(rows)
        aggregate = {
            "currency": currency,
            "predicted_currency_factor_bps": statistics.median(values),
            "predicted_absolute_factor_bps": statistics.median(
                float(row["predicted_absolute_factor_bps"]) for row in rows
            ),
            "probability_strengthening": statistics.median(probabilities),
            "effective_event_n": len({str(row["market_episode_id"]) for row in rows}),
            "minimum_training_effective_n": min(int(row["effective_event_n"]) for row in rows),
            "knowledge_cutoff_utc": (
                iso(knowledge_cutoff) if knowledge_cutoff is not None else None
            ),
            "market_episode_ids": sorted({str(row["market_episode_id"]) for row in rows}),
            "source_forecast_ids": sorted(
                {item for row in rows for item in row["source_forecast_ids"]}
            ),
            "source_contract_ids": sorted(
                {item for row in rows for item in row["source_contract_ids"]}
            ),
            "source_cohort_ids": sorted(
                {item for row in rows for item in row["source_cohort_ids"]}
            ),
        }
        aggregate["direction_state"] = source_direction(aggregate, probability_floor)
        result[currency] = aggregate
    return result


def trigger_knowledge_cutoff(
    trigger_rows: Sequence[Mapping[str, Any]],
) -> datetime | None:
    """Latest timestamp the adapter must know before freezing an entry.

    Grouped rows carry the latest forecast issue/training cutoff.  The latter
    is normally earlier, but taking the maximum is a fail-closed guard against
    malformed or asynchronously written source records.
    """
    clocks: list[datetime] = []
    for row in trigger_rows:
        for key in ("issued_utc", "knowledge_cutoff_utc", "training_cutoff_utc"):
            parsed = parse_time(row.get(key))
            if parsed is not None:
                clocks.append(parsed)
    return max(clocks) if clocks else None


def quote_for_instrument(
    quotes_payload: Mapping[str, Any],
    instrument: str,
    *,
    cutoff: datetime,
    not_before: datetime,
    max_age_sec: float,
    max_source_capture_lag_sec: float,
) -> tuple[dict[str, Any] | None, str]:
    row = (quotes_payload.get("quotes") or {}).get(instrument)
    if not isinstance(row, Mapping):
        return None, "quote_missing"
    bid = number(row.get("bid"))
    ask = number(row.get("ask"))
    pip = number(row.get("pip"))
    observed = parse_time(row.get("time"))
    if bid is None or ask is None or pip is None or bid <= 0.0 or ask <= bid or pip <= 0.0:
        return None, "quote_invalid"
    if observed is None:
        return None, "quote_clock_missing"
    age = (cutoff - observed).total_seconds()
    if age < 0.0:
        return None, "quote_after_decision_cutoff"
    if age > max_age_sec:
        return None, "quote_stale"
    source_lag = (observed - not_before).total_seconds()
    if source_lag < 0.0:
        return None, "quote_before_source_knowledge"
    if source_lag > max_source_capture_lag_sec:
        return None, "quote_after_source_capture_window"
    mid = (bid + ask) / 2.0
    return {
        "instrument": instrument,
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "pip": pip,
        "quote_utc": iso(observed),
        "quote_age_sec": max(0.0, age),
        "source_to_quote_lag_sec": source_lag,
        "spread_pips": (ask - bid) / pip,
        "spread_bps": (ask - bid) / mid * 10_000.0,
    }, ""


def pair_side_for_currency(instrument: str, currency: str, state: str) -> str:
    base, quote = instrument.split("_", 1)
    if currency not in {base, quote} or state not in {"strengthen", "weaken"}:
        return ""
    currency_should_rise = state == "strengthen"
    pair_should_rise = currency_should_rise if currency == base else not currency_should_rise
    return "buy" if pair_should_rise else "sell"


def directional_pair_move(pair: Mapping[str, Any], side: str, window: str) -> float | None:
    value = number(((pair.get("windows") or {}).get(window) or {}).get("return_bps"))
    if value is None:
        return None
    return value if side == "buy" else -value


def _empty_arm(arm: str, reason: str) -> dict[str, Any]:
    return {
        "arm": arm, "selected": False, "instrument": "", "direction": "",
        "predicted_gross_bps": None, "predicted_after_cost_bps": None,
        "abstain_reason": reason, "research_only": True,
        "execution_eligible": False,
    }


def build_price_only_arm(
    ticker: Mapping[str, Any],
    quotes: Mapping[str, Any],
    *,
    cutoff: datetime,
    entry_not_before: datetime,
    max_quote_age_sec: float,
    max_source_capture_lag_sec: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    result = price_rank.rank_snapshot(ticker, now=cutoff)
    if str(result.get("status")) != "ready":
        return _empty_arm("price_only", "price_rank_unavailable"), result
    for candidate in result.get("selected_pairs") or []:
        instrument = str(candidate.get("instrument") or "")
        quote, reason = quote_for_instrument(
            quotes, instrument, cutoff=cutoff, not_before=entry_not_before,
            max_age_sec=max_quote_age_sec,
            max_source_capture_lag_sec=max_source_capture_lag_sec,
        )
        if quote is None:
            continue
        return {
            "arm": "price_only", "selected": True,
            "instrument": instrument, "direction": candidate["direction"],
            "predicted_gross_bps": float(candidate["gross_movement_proxy_bps"]),
            "predicted_after_cost_bps": float(candidate["after_cost_proxy_bps"]),
            "price_rank_score": float(candidate["rank_score"]),
            "strong_currency": candidate["strong_currency"],
            "weak_currency": candidate["weak_currency"],
            "entry_knowledge_cutoff_utc": iso(entry_not_before),
            "entry_quote": quote, "abstain_reason": "",
            "research_only": True, "execution_eligible": False,
        }, result
    return _empty_arm("price_only", "no_price_rank_candidate_after_cost"), result


def build_source_only_arm(
    source_state: Mapping[str, Mapping[str, Any]],
    quotes: Mapping[str, Any],
    *,
    cutoff: datetime,
    entry_not_before: datetime,
    max_quote_age_sec: float,
    max_source_capture_lag_sec: float,
    spread_stress_multiple: float = 1.20,
) -> dict[str, Any]:
    strong = [row for row in source_state.values() if row["direction_state"] == "strengthen"]
    weak = [row for row in source_state.values() if row["direction_state"] == "weaken"]
    if not strong or not weak:
        return _empty_arm("source_only", "insufficient_opposite_source_currencies")
    candidates: list[dict[str, Any]] = []
    for high in strong:
        for low in weak:
            if high["currency"] == low["currency"]:
                continue
            for instrument in price_rank.EXPECTED_INSTRUMENTS:
                shape = price_rank._pair_shape(instrument, high["currency"], low["currency"])
                if shape is None:
                    continue
                side, _ = shape
                contributing_cutoff = trigger_knowledge_cutoff((high, low))
                pair_not_before = max(
                    entry_not_before,
                    contributing_cutoff or entry_not_before,
                )
                quote, _ = quote_for_instrument(
                    quotes, instrument, cutoff=cutoff, not_before=pair_not_before,
                    max_age_sec=max_quote_age_sec,
                    max_source_capture_lag_sec=max_source_capture_lag_sec,
                )
                if quote is None:
                    continue
                gross = float(high["predicted_currency_factor_bps"]) - float(low["predicted_currency_factor_bps"])
                stressed = float(quote["spread_bps"]) * spread_stress_multiple
                candidates.append(
                    {
                        "instrument": instrument, "direction": side,
                        "strong_currency": high["currency"], "weak_currency": low["currency"],
                        "predicted_gross_bps": gross,
                        "predicted_after_cost_bps": gross - stressed,
                        "stressed_cost_bps": stressed, "entry_quote": quote,
                        "entry_knowledge_cutoff_utc": iso(pair_not_before),
                        "source_forecast_ids": sorted(
                            set(high["source_forecast_ids"]) | set(low["source_forecast_ids"])
                        ),
                        "source_episode_ids": sorted(
                            set(high["market_episode_ids"]) | set(low["market_episode_ids"])
                        ),
                    }
                )
    candidates = [row for row in candidates if row["predicted_after_cost_bps"] > 0.0]
    if not candidates:
        return _empty_arm("source_only", "source_gap_does_not_clear_stressed_cost")
    best = max(candidates, key=lambda row: (row["predicted_after_cost_bps"], row["instrument"]))
    return {
        "arm": "source_only", "selected": True, **best, "abstain_reason": "",
        "research_only": True, "execution_eligible": False,
    }


def build_source_price_timing_arm(
    trigger_rows: Sequence[Mapping[str, Any]],
    source_state: Mapping[str, Mapping[str, Any]],
    ticker: Mapping[str, Any],
    quotes: Mapping[str, Any],
    *,
    cutoff: datetime,
    entry_not_before: datetime,
    max_quote_age_sec: float,
    max_source_capture_lag_sec: float,
    spread_stress_multiple: float = 1.20,
    min_price_confirmation_bps: float = 0.10,
) -> dict[str, Any]:
    pair_moves = ticker.get("pair_moves") or {}
    candidates: list[dict[str, Any]] = []
    trigger_currencies = sorted({str(row["currency"]) for row in trigger_rows})
    for currency in trigger_currencies:
        state = source_state.get(currency)
        if not state or state["direction_state"] not in {"strengthen", "weaken"}:
            continue
        for instrument in price_rank.EXPECTED_INSTRUMENTS:
            if currency not in instrument.split("_"):
                continue
            side = pair_side_for_currency(instrument, currency, state["direction_state"])
            if not side:
                continue
            pair = pair_moves.get(instrument)
            if not isinstance(pair, Mapping):
                continue
            move5 = directional_pair_move(pair, side, "5")
            move15 = directional_pair_move(pair, side, "15")
            if move5 is None or move15 is None or min(move5, move15) < min_price_confirmation_bps:
                continue
            quote, _ = quote_for_instrument(
                quotes, instrument, cutoff=cutoff, not_before=entry_not_before,
                max_age_sec=max_quote_age_sec,
                max_source_capture_lag_sec=max_source_capture_lag_sec,
            )
            if quote is None:
                continue
            base, quoted = instrument.split("_", 1)
            counterpart = quoted if currency == base else base
            counterpart_state = source_state.get(counterpart)
            affected = abs(float(state["predicted_currency_factor_bps"]))
            contributing_states: list[Mapping[str, Any]] = [state]
            if counterpart_state and counterpart_state["direction_state"] in {"strengthen", "weaken"}:
                base_value = float(source_state.get(base, {}).get("predicted_currency_factor_bps", 0.0))
                quote_value = float(source_state.get(quoted, {}).get("predicted_currency_factor_bps", 0.0))
                source_pair = base_value - quote_value
                gross = source_pair if side == "buy" else -source_pair
                counterpart_source_status = counterpart_state["direction_state"]
                contributing_states.append(counterpart_state)
            else:
                # Missing remains explicitly unavailable.  The hybrid arm may
                # use one causal leg, but never inserts a zero counterpart into
                # source-only rankings or evidence counts.
                gross = affected
                counterpart_source_status = "unavailable"
            contributing_cutoff = trigger_knowledge_cutoff(contributing_states)
            pair_not_before = max(
                entry_not_before,
                contributing_cutoff or entry_not_before,
            )
            # Re-evaluate against the complete set of source legs used by the
            # candidate.  The earlier quote check protects the trigger leg;
            # this one also protects a newer active counterpart leg.
            quote, _ = quote_for_instrument(
                quotes, instrument, cutoff=cutoff, not_before=pair_not_before,
                max_age_sec=max_quote_age_sec,
                max_source_capture_lag_sec=max_source_capture_lag_sec,
            )
            if quote is None:
                continue
            stressed = float(quote["spread_bps"]) * spread_stress_multiple
            if gross <= stressed:
                continue
            candidates.append(
                {
                    "instrument": instrument, "direction": side,
                    "source_currency": currency,
                    "source_direction_state": state["direction_state"],
                    "counterpart_currency": counterpart,
                    "counterpart_source_state": counterpart_source_status,
                    "predicted_gross_bps": gross,
                    "predicted_after_cost_bps": gross - stressed,
                    "stressed_cost_bps": stressed,
                    "price_confirmation_5m_bps": move5,
                    "price_confirmation_15m_bps": move15,
                    "entry_knowledge_cutoff_utc": iso(pair_not_before),
                    "entry_quote": quote,
                    "source_forecast_ids": list(state["source_forecast_ids"]),
                    "source_episode_ids": list(state["market_episode_ids"]),
                }
            )
    if not candidates:
        return _empty_arm(
            "source_price_timing",
            "no_source_direction_with_price_confirmation_and_cost_clearance",
        )
    best = max(
        candidates,
        key=lambda row: (
            row["predicted_after_cost_bps"]
            + 0.25 * min(row["price_confirmation_5m_bps"], row["price_confirmation_15m_bps"]),
            row["instrument"],
        ),
    )
    return {
        "arm": "source_price_timing", "selected": True, **best,
        "abstain_reason": "", "research_only": True,
        "execution_eligible": False,
    }


def open_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS rank_decision (
          decision_id TEXT PRIMARY KEY,
          market_episode_id TEXT NOT NULL,
          horizon_min INTEGER NOT NULL,
          decision_cutoff_utc TEXT NOT NULL,
          source_database TEXT NOT NULL,
          source_contract_ids_json TEXT NOT NULL,
          source_cohort_ids_json TEXT NOT NULL,
          adapter_cohort_id TEXT NOT NULL,
          ticker_sha256 TEXT NOT NULL,
          quotes_sha256 TEXT NOT NULL,
          source_payload_json TEXT NOT NULL,
          decision_payload_json TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          can_promote INTEGER NOT NULL CHECK(can_promote=0),
          contract_id TEXT NOT NULL,
          UNIQUE(market_episode_id,horizon_min)
        );
        CREATE TABLE IF NOT EXISTS rank_forecast (
          forecast_id TEXT PRIMARY KEY,
          decision_id TEXT NOT NULL,
          arm TEXT NOT NULL CHECK(arm IN ('price_only','source_only','source_price_timing')),
          horizon_min INTEGER NOT NULL,
          issued_utc TEXT NOT NULL,
          maturity_utc TEXT NOT NULL,
          selected INTEGER NOT NULL CHECK(selected IN (0,1)),
          instrument TEXT NOT NULL,
          direction TEXT NOT NULL,
          entry_quote_utc TEXT,
          entry_bid REAL,
          entry_ask REAL,
          pip REAL,
          predicted_gross_bps REAL,
          predicted_after_cost_bps REAL,
          abstain_reason TEXT NOT NULL,
          forecast_payload_json TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          contract_id TEXT NOT NULL,
          adapter_cohort_id TEXT NOT NULL,
          UNIQUE(decision_id,arm),
          FOREIGN KEY(decision_id) REFERENCES rank_decision(decision_id)
        );
        CREATE TABLE IF NOT EXISTS rank_outcome (
          forecast_id TEXT PRIMARY KEY,
          maturity_utc TEXT NOT NULL,
          exit_quote_utc TEXT NOT NULL,
          exit_alignment_sec REAL NOT NULL,
          exit_bid REAL NOT NULL,
          exit_ask REAL NOT NULL,
          gross_mid_pips REAL NOT NULL,
          executable_after_cost_pips REAL NOT NULL,
          realized_cost_pips REAL NOT NULL,
          after_cost_win INTEGER NOT NULL CHECK(after_cost_win IN (0,1)),
          outcome_payload_json TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          contract_id TEXT NOT NULL,
          FOREIGN KEY(forecast_id) REFERENCES rank_forecast(forecast_id)
        );
        CREATE INDEX IF NOT EXISTS idx_rank_forecast_maturity
          ON rank_forecast(selected,maturity_utc);
        CREATE INDEX IF NOT EXISTS idx_rank_decision_episode
          ON rank_decision(market_episode_id,horizon_min);
        CREATE TRIGGER IF NOT EXISTS rank_decision_no_update
          BEFORE UPDATE ON rank_decision
          BEGIN SELECT RAISE(ABORT,'rank_decision is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS rank_decision_no_delete
          BEFORE DELETE ON rank_decision
          BEGIN SELECT RAISE(ABORT,'rank_decision is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS rank_forecast_no_update
          BEFORE UPDATE ON rank_forecast
          BEGIN SELECT RAISE(ABORT,'rank_forecast is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS rank_forecast_no_delete
          BEFORE DELETE ON rank_forecast
          BEGIN SELECT RAISE(ABORT,'rank_forecast is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS rank_outcome_no_update
          BEFORE UPDATE ON rank_outcome
          BEGIN SELECT RAISE(ABORT,'rank_outcome is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS rank_outcome_no_delete
          BEFORE DELETE ON rank_outcome
          BEGIN SELECT RAISE(ABORT,'rank_outcome is append-only'); END;
        """
    )
    connection.commit()
    return connection


def adapter_cohort_id(trigger_rows: Sequence[Mapping[str, Any]]) -> str:
    contracts = sorted({item for row in trigger_rows for item in row["source_contract_ids"]})
    cohorts = sorted({item for row in trigger_rows for item in row["source_cohort_ids"]})
    return f"{BASE_COHORT_ID}.{digest(adapter_definition_sha256(), *contracts, *cohorts)[:16]}"


def persist_decision(
    connection: sqlite3.Connection,
    *,
    episode_id: str,
    horizon_min: int,
    cutoff: datetime,
    source_database: Path,
    trigger_rows: Sequence[Mapping[str, Any]],
    source_state: Mapping[str, Mapping[str, Any]],
    ticker: Mapping[str, Any],
    quotes: Mapping[str, Any],
    arms: Sequence[Mapping[str, Any]],
    price_result: Mapping[str, Any],
) -> bool:
    decision_id = "source_rank_decision_" + digest(
        episode_id, horizon_min, CONTRACT_ID
    )[:32]
    if connection.execute(
        "SELECT 1 FROM rank_decision WHERE decision_id=?", (decision_id,)
    ).fetchone():
        return False
    contracts = sorted({item for row in trigger_rows for item in row["source_contract_ids"]})
    cohorts = sorted({item for row in trigger_rows for item in row["source_cohort_ids"]})
    cohort = adapter_cohort_id(trigger_rows)
    source_payload = {
        "trigger_rows": list(trigger_rows),
        "active_currency_source_state": dict(source_state),
    }
    decision_payload = {
        "decision_id": decision_id,
        "market_episode_id": episode_id,
        "horizon_min": horizon_min,
        "decision_cutoff_utc": iso(cutoff),
        "trigger_knowledge_cutoff_utc": (
            iso(trigger_knowledge_cutoff(trigger_rows))
            if trigger_knowledge_cutoff(trigger_rows) is not None else None
        ),
        "price_rank_status": price_result.get("status"),
        "price_currency_ranks": price_result.get("currency_ranks") or [],
        "arms": list(arms),
        "missing_source_currencies": sorted(
            set(price_rank.EXPECTED_CURRENCIES) - set(source_state)
        ),
        "research_only": True, "execution_eligible": False,
        "can_authorize": False, "can_promote": False,
        "adapter_definition_sha256": adapter_definition_sha256(),
    }
    connection.execute(
        """
        INSERT INTO rank_decision VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,0,0,0,?)
        """,
        (
            decision_id, episode_id, horizon_min, iso(cutoff),
            str(source_database), json.dumps(contracts), json.dumps(cohorts),
            cohort, canonical_json_hash(ticker), canonical_json_hash(quotes),
            json.dumps(source_payload, sort_keys=True, separators=(",", ":"), default=str),
            json.dumps(decision_payload, sort_keys=True, separators=(",", ":"), default=str),
            CONTRACT_ID,
        ),
    )
    # The INSERT statement has fixed research_only=1 and execution/capability=0.
    for arm in arms:
        forecast_id = "source_rank_forecast_" + digest(decision_id, arm["arm"])[:32]
        entry = arm.get("entry_quote") if isinstance(arm.get("entry_quote"), Mapping) else {}
        maturity = cutoff + timedelta(minutes=horizon_min)
        payload = dict(arm)
        payload.update(
            {
                "forecast_id": forecast_id, "decision_id": decision_id,
                "market_episode_id": episode_id, "horizon_min": horizon_min,
                "issued_utc": iso(cutoff), "maturity_utc": iso(maturity),
                "adapter_cohort_id": cohort, "contract_id": CONTRACT_ID,
            }
        )
        connection.execute(
            """
            INSERT INTO rank_forecast VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,?,?)
            """,
            (
                forecast_id, decision_id, arm["arm"], horizon_min, iso(cutoff),
                iso(maturity), int(bool(arm.get("selected"))),
                str(arm.get("instrument") or ""), str(arm.get("direction") or ""),
                entry.get("quote_utc"), entry.get("bid"), entry.get("ask"),
                entry.get("pip"), arm.get("predicted_gross_bps"),
                arm.get("predicted_after_cost_bps"),
                str(arm.get("abstain_reason") or ""),
                json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
                CONTRACT_ID, cohort,
            ),
        )
    connection.commit()
    return True


def load_exit_quote(
    quote_bars_database: Path,
    instrument: str,
    target: datetime,
    *,
    max_alignment_sec: float,
) -> dict[str, Any] | None:
    if not quote_bars_database.exists():
        return None
    connection = _read_only_connection(quote_bars_database)
    try:
        row = connection.execute(
            """
            SELECT last_epoch,close_bid,close_ask,pip
            FROM quote_bars
            WHERE instrument=? AND last_epoch>=?
            ORDER BY last_epoch
            LIMIT 1
            """,
            (instrument, target.timestamp()),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        return None
    observed = datetime.fromtimestamp(float(row[0]), tz=UTC)
    alignment = (observed - target).total_seconds()
    bid, ask, pip = float(row[1]), float(row[2]), float(row[3])
    if alignment < 0.0 or alignment > max_alignment_sec or bid <= 0.0 or ask <= bid or pip <= 0.0:
        return None
    return {
        "quote_utc": iso(observed), "alignment_sec": alignment,
        "bid": bid, "ask": ask, "pip": pip,
    }


def mature_outcomes(
    connection: sqlite3.Connection,
    quote_bars_database: Path,
    *,
    observed_utc: datetime,
    max_alignment_sec: float,
) -> int:
    rows = connection.execute(
        """
        SELECT f.* FROM rank_forecast f
        LEFT JOIN rank_outcome o ON o.forecast_id=f.forecast_id
        WHERE f.selected=1 AND o.forecast_id IS NULL AND f.maturity_utc<=?
        ORDER BY f.maturity_utc
        """,
        (iso(observed_utc),),
    ).fetchall()
    inserted = 0
    for row in rows:
        target = parse_time(row["maturity_utc"])
        if target is None:
            continue
        exit_quote = load_exit_quote(
            quote_bars_database, str(row["instrument"]), target,
            max_alignment_sec=max_alignment_sec,
        )
        if exit_quote is None:
            continue
        entry_bid, entry_ask, pip = float(row["entry_bid"]), float(row["entry_ask"]), float(row["pip"])
        entry_mid = (entry_bid + entry_ask) / 2.0
        exit_mid = (exit_quote["bid"] + exit_quote["ask"]) / 2.0
        sign = 1.0 if row["direction"] == "buy" else -1.0
        gross = sign * (exit_mid - entry_mid) / pip
        executable = (
            (exit_quote["bid"] - entry_ask) / pip
            if row["direction"] == "buy"
            else (entry_bid - exit_quote["ask"]) / pip
        )
        cost = gross - executable
        payload = {
            "forecast_id": row["forecast_id"], "arm": row["arm"],
            "instrument": row["instrument"], "direction": row["direction"],
            "maturity_utc": row["maturity_utc"],
            "exit_quote_utc": exit_quote["quote_utc"],
            "exit_alignment_sec": exit_quote["alignment_sec"],
            "gross_mid_pips": gross,
            "executable_after_cost_pips": executable,
            "realized_cost_pips": cost,
            "research_only": True, "execution_eligible": False,
        }
        before = connection.total_changes
        connection.execute(
            """
            INSERT OR IGNORE INTO rank_outcome VALUES (?,?,?,?,?,?,?,?,?,?,?,1,0,?)
            """,
            (
                row["forecast_id"], row["maturity_utc"], exit_quote["quote_utc"],
                exit_quote["alignment_sec"], exit_quote["bid"], exit_quote["ask"],
                gross, executable, cost, int(executable > 0.0),
                json.dumps(payload, sort_keys=True, separators=(",", ":")), CONTRACT_ID,
            ),
        )
        inserted += connection.total_changes - before
    connection.commit()
    return inserted


def ledger_summary(connection: sqlite3.Connection) -> dict[str, Any]:
    decisions = connection.execute(
        "SELECT COUNT(*),COUNT(DISTINCT market_episode_id) FROM rank_decision"
    ).fetchone()
    cohort_rows = connection.execute(
        """
        SELECT adapter_cohort_id,COUNT(*),COUNT(DISTINCT market_episode_id)
        FROM rank_decision
        GROUP BY adapter_cohort_id
        ORDER BY adapter_cohort_id
        """
    ).fetchall()
    cohorts: list[dict[str, Any]] = []
    for cohort_row in cohort_rows:
        cohort_id = str(cohort_row[0])
        arm_rows = connection.execute(
            """
            SELECT f.arm,COUNT(*) forecasts,SUM(f.selected) selected,
                   SUM(CASE WHEN f.selected=0 THEN 1 ELSE 0 END) abstained,
                   COUNT(o.forecast_id) outcomes,AVG(o.executable_after_cost_pips),
                   AVG(CASE WHEN o.executable_after_cost_pips>0 THEN 1.0 ELSE 0.0 END)
            FROM rank_forecast f
            LEFT JOIN rank_outcome o ON o.forecast_id=f.forecast_id
            WHERE f.adapter_cohort_id=?
            GROUP BY f.arm ORDER BY f.arm
            """,
            (cohort_id,),
        ).fetchall()
        cohorts.append(
            {
                "adapter_cohort_id": cohort_id,
                "decisions": int(cohort_row[1] or 0),
                "independent_source_episodes": int(cohort_row[2] or 0),
                "arms": [
                    {
                        "arm": row[0], "forecasts": int(row[1]),
                        "selected": int(row[2] or 0),
                        "abstained": int(row[3] or 0),
                        "outcomes": int(row[4] or 0),
                        "mean_after_cost_pips": (
                            None if row[5] is None else float(row[5])
                        ),
                        "after_cost_win_rate": (
                            None if row[6] is None else float(row[6])
                        ),
                    }
                    for row in arm_rows
                ],
            }
        )
    return {
        "decisions": int(decisions[0] or 0),
        "independent_source_episodes": int(decisions[1] or 0),
        "adapter_cohort_count": len(cohorts),
        # Performance metrics deliberately exist only inside immutable cohort
        # blocks.  The top-level values are an inventory census, not evidence.
        "cohorts": cohorts,
    }


def render_report(snapshot: Mapping[str, Any]) -> str:
    lines = [
        "# Source-Conditioned Currency Rank V1",
        "",
        f"Generated: `{snapshot['generated_utc']}`",
        "",
        "Research-only comparison of price rank, causal-source rank, and source direction with price/spread timing.",
        "",
        f"- Source database: `{snapshot['source_database']}`",
        f"- Source forecast rows / episode-currency-horizon groups: **{snapshot['source_forecast_rows']} / {snapshot['source_groups']}**",
        f"- New decisions this cycle: **{snapshot['new_decisions']}**",
        f"- Global episode-ID census (not pooled evidence): **{snapshot['ledger']['independent_source_episodes']}**",
        f"- Immutable adapter cohorts: **{snapshot['ledger']['adapter_cohort_count']}**",
        f"- Source input status: **{snapshot['source_input_status']}**",
        f"- Supported decision: **{snapshot['supported_execution_decision']}**",
    ]
    for cohort in snapshot["ledger"]["cohorts"]:
        lines.extend(
            [
                "", f"## Cohort `{cohort['adapter_cohort_id']}`", "",
                f"Decisions / independent episodes: **{cohort['decisions']} / {cohort['independent_source_episodes']}**",
                "",
                "| Arm | Forecasts | Selected | Abstained | Outcomes | Mean after cost | Win rate |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in cohort["arms"]:
            mean = "n/a" if row["mean_after_cost_pips"] is None else f"{row['mean_after_cost_pips']:+.3f}p"
            win = "n/a" if row["after_cost_win_rate"] is None else f"{row['after_cost_win_rate']:.1%}"
            lines.append(
                f"| {row['arm']} | {row['forecasts']} | {row['selected']} | {row['abstained']} | "
                f"{row['outcomes']} | {mean} | {win} |"
            )
    lines.extend(
        [
            "", "## Boundaries", "",
            "- Only `forecast_state=forecast` and `prospective_proof_eligible=1` source rows are read.",
            "- Factor rows are collapsed to one episode x currency x horizon state before ranking.",
            "- Missing currency source state is unavailable, never zero.",
            "- The adapter does not read article/news-watchlist databases and cannot place, authorize, or promote orders.",
            "- Outcomes use executable bid/ask paths at the declared horizon.",
            "",
        ]
    )
    return "\n".join(lines)


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Windows readers (the dashboard, integrity audit, and supervisor) can
    # briefly hold the destination without delete sharing.  A single
    # ``Path.replace`` therefore turns an otherwise healthy shadow cycle into
    # a process crash.  Keep each publication private to this attempt and
    # tolerate only a short, bounded transient lock; a persistent denial still
    # raises and remains observable/fail-closed.
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(text, encoding="utf-8")
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


def run_cycle(
    *,
    source_database: Path | None = None,
    ticker_path: Path = DEFAULT_TICKER,
    quotes_path: Path = DEFAULT_QUOTES,
    quote_bars_database: Path = DEFAULT_QUOTE_BARS,
    ledger_path: Path = DEFAULT_LEDGER,
    state_path: Path = DEFAULT_STATE,
    report_path: Path = DEFAULT_REPORT,
    observed_utc: datetime | None = None,
    max_source_capture_lag_sec: float = 120.0,
    max_quote_age_sec: float = 90.0,
    max_outcome_alignment_sec: float = 90.0,
) -> dict[str, Any]:
    observed = (observed_utc or utc_now()).astimezone(UTC)
    source_path = resolve_source_database(source_database)
    source_rows = load_source_forecasts(source_path)
    groups = group_source_forecasts(source_rows)
    ticker = read_json(ticker_path)
    quotes = read_json(quotes_path)
    connection = open_ledger(ledger_path)
    new_decisions = 0
    try:
        existing = {
            (str(row[0]), int(row[1]))
            for row in connection.execute(
                "SELECT market_episode_id,horizon_min FROM rank_decision"
            )
        }
        trigger_groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
        for row in groups:
            issued = parse_time(row["issued_utc"])
            if issued is None or issued > observed:
                continue
            age = (observed - issued).total_seconds()
            if age > max_source_capture_lag_sec:
                continue
            key = (str(row["market_episode_id"]), int(row["horizon_min"]))
            if key not in existing:
                trigger_groups[key].append(row)
        for (episode_id, horizon_min), trigger_rows in sorted(trigger_groups.items()):
            entry_not_before = trigger_knowledge_cutoff(trigger_rows)
            if entry_not_before is None or entry_not_before > observed:
                continue
            if (observed - entry_not_before).total_seconds() > max_source_capture_lag_sec:
                continue
            source_state = active_currency_source_state(
                groups, cutoff=observed, horizon_min=horizon_min
            )
            price_arm, price_result = build_price_only_arm(
                ticker, quotes, cutoff=observed,
                entry_not_before=entry_not_before,
                max_quote_age_sec=max_quote_age_sec,
                max_source_capture_lag_sec=max_source_capture_lag_sec,
            )
            source_arm = build_source_only_arm(
                source_state, quotes, cutoff=observed,
                entry_not_before=entry_not_before,
                max_quote_age_sec=max_quote_age_sec,
                max_source_capture_lag_sec=max_source_capture_lag_sec,
            )
            hybrid_arm = build_source_price_timing_arm(
                trigger_rows, source_state, ticker, quotes, cutoff=observed,
                entry_not_before=entry_not_before,
                max_quote_age_sec=max_quote_age_sec,
                max_source_capture_lag_sec=max_source_capture_lag_sec,
            )
            if persist_decision(
                connection, episode_id=episode_id, horizon_min=horizon_min,
                cutoff=observed, source_database=source_path,
                trigger_rows=trigger_rows, source_state=source_state,
                ticker=ticker, quotes=quotes,
                arms=(price_arm, source_arm, hybrid_arm),
                price_result=price_result,
            ):
                new_decisions += 1
        matured = mature_outcomes(
            connection, quote_bars_database, observed_utc=observed,
            max_alignment_sec=max_outcome_alignment_sec,
        )
        integrity = str(connection.execute("PRAGMA quick_check(1)").fetchone()[0])
        summary = ledger_summary(connection)
    finally:
        connection.close()
    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "adapter_definition_sha256": adapter_definition_sha256(),
        "generated_utc": iso(observed),
        "source_database": str(source_path),
        "source_database_mode": (
            "explicit_test_or_replay_override"
            if source_database is not None else "required_v2_default"
        ),
        "source_database_available": source_path.exists(),
        "source_input_status": (
            "ready" if source_path.exists()
            else "missing_required_v2_fail_closed"
        ),
        "source_forecast_rows": len(source_rows),
        "source_groups": len(groups),
        "new_decisions": new_decisions,
        "matured_outcomes": matured,
        "ledger": summary,
        "sqlite_integrity": integrity,
        "policy": {
            "source_filter": "forecast_and_prospective_proof_eligible_only",
            "default_source_contract": "v2_required_no_v1_fallback",
            "deduplication_unit": "market_episode_x_currency_x_horizon",
            "entry_knowledge_clock": "quote_at_or_after_latest_trigger_knowledge_cutoff",
            "missing_source_state": "unavailable_not_zero",
            "news_technical_watchlist_dependency": False,
            "research_only": True, "execution_eligible": False,
            "can_place_orders": False, "can_authorize": False,
            "can_promote": False,
        },
        "research_only": True, "execution_eligible": False,
        "can_place_orders": False, "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }
    atomic_write(state_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_write(report_path, render_report(snapshot))
    return snapshot


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-database", type=Path)
    parser.add_argument("--ticker", type=Path, default=DEFAULT_TICKER)
    parser.add_argument("--quotes", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--quote-bars-database", type=Path, default=DEFAULT_QUOTE_BARS)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--max-source-capture-lag-sec", type=float, default=120.0)
    parser.add_argument("--max-quote-age-sec", type=float, default=90.0)
    parser.add_argument("--max-outcome-alignment-sec", type=float, default=90.0)
    parser.add_argument("--interval-sec", type=float, default=5.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    while True:
        snapshot = run_cycle(
            source_database=args.source_database,
            ticker_path=args.ticker,
            quotes_path=args.quotes,
            quote_bars_database=args.quote_bars_database,
            ledger_path=args.ledger,
            state_path=args.state,
            report_path=args.report,
            max_source_capture_lag_sec=args.max_source_capture_lag_sec,
            max_quote_age_sec=args.max_quote_age_sec,
            max_outcome_alignment_sec=args.max_outcome_alignment_sec,
        )
        print(json.dumps(snapshot, sort_keys=True), flush=True)
        if args.once or (args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec):
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
