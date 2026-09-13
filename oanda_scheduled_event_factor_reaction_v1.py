#!/usr/bin/env python3
"""Prospective scheduled-event, one-minute currency-factor reaction proof.

The official event calendar supplies only the causal activation clock and
driver currency. Direction is locked from the first complete post-event minute
across that currency's tradeable OANDA pairs. One lowest-cost confirming pair
is selected before any later horizon exists, then matured from immutable
executable bid/ask captures. The RBNZ 2026-09-02 event predates this cohort and
is a regression fixture only.

This worker is research-only. It cannot trade, authorize, promote, or mutate
Practice-007 policy.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import statistics
import time
from typing import Any, Mapping, Sequence

from oanda_local_news_sentiment import normalized_observation_time
from oanda_macro_release_breakout_research import atomic_json, atomic_text
from oanda_worker_heartbeat import WorkerHeartbeat


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
NEWS = DATA / "local_news_sentiment"
CONFIG = ROOT / "config" / "scheduled_event_factor_reaction_v1_20260902b.json"
INPUT_DATABASE = NEWS / "scheduled_event_quote_capture_v2.sqlite"
DATABASE = STATE / "scheduled_event_factor_reaction_v1_20260902b.sqlite"
OUTPUT = STATE / "scheduled_event_factor_reaction_v1.json"
HEARTBEAT = STATE / "scheduled_event_factor_reaction_heartbeat_v1.json"
REPORT = (
    DATA
    / "reports"
    / "scheduled_event_factor_reaction"
    / "SCHEDULED_EVENT_FACTOR_REACTION_CURRENT.md"
)

UTC = dt.timezone.utc
CONTRACT_ID = "scheduled_event_factor_reaction_v1_20260902"
COHORT_ID = "scheduled_event_factor_reaction_v1_20260902b"
INPUT_CONTRACT_ID = (
    "scheduled_event_quote_capture_v2_oanda_rest_current_snapshot_20260902"
)
INPUT_COHORT_ID = "scheduled_event_quote_capture_v2_20260902a"
COHORT_START = dt.datetime(2026, 9, 2, 6, 0, tzinfo=UTC)
ENTRY_HORIZON_MIN = 1
OUTCOME_HORIZONS_MIN = (5, 15, 30, 60)
MAXIMUM_DECISION_LAG_SEC = 15.0
MAXIMUM_PROCESSING_LAG_SEC = 30.0
MINIMUM_FACTOR_MOVE_BPS = 2.0
MINIMUM_LEG_MOVE_BPS = 1.0
MINIMUM_CONFIRMING_PAIRS = 2
TOTAL_SLIPPAGE_PIPS = 0.25
VALID_TIMING_QUALITY = "prospective_current_oanda_pricing_snapshot"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_frozen_config(
    config_path: Path = CONFIG,
    *,
    source_path: Path | None = None,
) -> dict[str, Any]:
    source_path = Path(__file__).resolve() if source_path is None else Path(source_path)
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    expected = {
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "cohort_start_utc": iso(COHORT_START),
        "input_contract_id": INPUT_CONTRACT_ID,
        "input_cohort_id": INPUT_COHORT_ID,
        "entry_horizon_min": ENTRY_HORIZON_MIN,
        "outcome_horizons_min": list(OUTCOME_HORIZONS_MIN),
        "maximum_decision_lag_sec": MAXIMUM_DECISION_LAG_SEC,
        "maximum_processing_lag_sec": MAXIMUM_PROCESSING_LAG_SEC,
        "minimum_factor_move_bps": MINIMUM_FACTOR_MOVE_BPS,
        "minimum_leg_move_bps": MINIMUM_LEG_MOVE_BPS,
        "minimum_confirming_pairs": MINIMUM_CONFIRMING_PAIRS,
        "total_slippage_pips": TOTAL_SLIPPAGE_PIPS,
        "selection": "lowest_effective_cost_confirming_pair",
        "direction_source": "post_event_one_minute_multi_pair_factor",
        "research_only": True,
        "execution_eligible": False,
        "can_trade": False,
        "can_authorize": False,
        "can_promote": False,
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise ValueError(f"frozen config mismatch: {key}")
    source_hash = sha256_file(source_path)
    if payload.get("source_sha256") != source_hash:
        raise ValueError("frozen source hash mismatch")
    payload["config_sha256"] = sha256_file(config_path)
    payload["validated_source_sha256"] = source_hash
    return payload


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


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def stable_id(*parts: Any) -> str:
    return hashlib.sha256("|".join(map(str, parts)).encode("utf-8")).hexdigest()[:32]


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _mid(quote: Mapping[str, Any]) -> float | None:
    bid = _finite(quote.get("bid"))
    ask = _finite(quote.get("ask"))
    if bid is None or ask is None or ask <= bid:
        return None
    return (bid + ask) / 2.0


def driver_return_bps(
    instrument: str,
    driver_currency: str,
    start_quote: Mapping[str, Any],
    end_quote: Mapping[str, Any],
) -> float | None:
    """Return positive bps when the declared driver currency strengthens."""

    start = _mid(start_quote)
    end = _mid(end_quote)
    if start is None or end is None or start <= 0.0 or end <= 0.0:
        return None
    try:
        base, quote = instrument.split("_", 1)
    except ValueError:
        return None
    raw = math.log(end / start) * 10_000.0
    if base == driver_currency:
        return raw
    if quote == driver_currency:
        return -raw
    return None


def _rejected(
    *,
    reason: str,
    clock: Mapping[str, Any],
    observed: dt.datetime,
    decision_lag_sec: float | None = None,
    diagnostics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "status": "rejected",
        "reason": reason,
        "clock_id": str(clock.get("clock_id") or ""),
        "event_id": str(clock.get("event_id") or ""),
        "event_series_id": str(clock.get("event_series_id") or ""),
        "scheduled_utc": str(clock.get("scheduled_utc") or ""),
        "driver_currency": str(clock.get("driver_currency") or "").upper(),
        "decision_utc": iso(observed),
        "decision_lag_sec": decision_lag_sec,
        "diagnostics": dict(diagnostics or {}),
        "research_only": True,
        "execution_eligible": False,
        "can_trade": False,
        "can_authorize": False,
        "can_promote": False,
    }


def evaluate_reaction(
    clock: Mapping[str, Any],
    capture_zero: Mapping[str, Any],
    capture_one: Mapping[str, Any],
    *,
    observed: dt.datetime,
) -> dict[str, Any]:
    scheduled = parse_utc(clock.get("scheduled_utc"))
    driver = str(clock.get("driver_currency") or "").upper()
    if scheduled is None or not driver:
        return _rejected(reason="missing_clock_or_driver", clock=clock, observed=observed)
    decision_target = scheduled + dt.timedelta(minutes=ENTRY_HORIZON_MIN)
    capture_decision_time = parse_utc(capture_one.get("retrieval_completed_utc"))
    if capture_decision_time is None:
        return _rejected(
            reason="entry_capture_clock_missing", clock=clock, observed=observed,
        )
    lag = (capture_decision_time - decision_target).total_seconds()
    processing_lag = (observed - capture_decision_time).total_seconds()
    if lag < 0.0:
        return _rejected(
            reason="entry_capture_not_due", clock=clock, observed=observed,
            decision_lag_sec=lag,
        )
    if lag > MAXIMUM_DECISION_LAG_SEC:
        return _rejected(
            reason="decision_clock_too_late", clock=clock, observed=observed,
            decision_lag_sec=lag,
        )
    if processing_lag < -2.0 or processing_lag > MAXIMUM_PROCESSING_LAG_SEC:
        return _rejected(
            reason="decision_processing_clock_too_late", clock=clock,
            observed=observed, decision_lag_sec=lag,
            diagnostics={"processing_lag_sec": processing_lag},
        )
    for horizon, capture in ((0, capture_zero), (1, capture_one)):
        if str(capture.get("contract_id") or "") != INPUT_CONTRACT_ID:
            return _rejected(
                reason=f"input_contract_mismatch_h{horizon}", clock=clock,
                observed=observed, decision_lag_sec=lag,
            )
        if str(capture.get("cohort_id") or "") != INPUT_COHORT_ID:
            return _rejected(
                reason=f"input_cohort_mismatch_h{horizon}", clock=clock,
                observed=observed, decision_lag_sec=lag,
            )
        if (
            str(capture.get("timing_quality") or "") != VALID_TIMING_QUALITY
            or str(capture.get("invalid_reason") or "")
        ):
            return _rejected(
                reason=f"invalid_capture_h{horizon}", clock=clock,
                observed=observed, decision_lag_sec=lag,
            )
        ledger_hash = str(capture.get("_ledger_payload_sha256") or "")
        computed_hash = str(capture.get("_computed_payload_sha256") or "")
        if not ledger_hash or ledger_hash != computed_hash:
            return _rejected(
                reason=f"input_payload_hash_mismatch_h{horizon}", clock=clock,
                observed=observed, decision_lag_sec=lag,
            )
    quotes_zero = capture_zero.get("direct_event_tradeable_quotes")
    quotes_one = capture_one.get("direct_event_tradeable_quotes")
    if not isinstance(quotes_zero, Mapping) or not isinstance(quotes_one, Mapping):
        return _rejected(
            reason="missing_direct_event_quotes", clock=clock, observed=observed,
            decision_lag_sec=lag,
        )
    instruments = sorted(set(quotes_zero).intersection(quotes_one))
    legs: list[dict[str, Any]] = []
    for instrument in instruments:
        q0 = quotes_zero[instrument]
        q1 = quotes_one[instrument]
        if not isinstance(q0, Mapping) or not isinstance(q1, Mapping):
            continue
        factor_bps = driver_return_bps(instrument, driver, q0, q1)
        entry_mid = _mid(q1)
        bid = _finite(q1.get("bid"))
        ask = _finite(q1.get("ask"))
        pip = _finite(q1.get("pip"))
        if None in (factor_bps, entry_mid, bid, ask, pip) or pip <= 0.0:
            continue
        spread_pips = (ask - bid) / pip
        spread_bps = (ask - bid) / entry_mid * 10_000.0
        slippage_bps = TOTAL_SLIPPAGE_PIPS * pip / entry_mid * 10_000.0
        legs.append(
            {
                "instrument": instrument,
                "driver_return_bps": factor_bps,
                "entry_bid": bid,
                "entry_ask": ask,
                "entry_mid": entry_mid,
                "pip": pip,
                "entry_spread_pips": spread_pips,
                "effective_cost_bps": spread_bps + slippage_bps,
                "entry_quote_time_utc": str(
                    q1.get("broker_price_time_utc")
                    or capture_one.get("response_time_utc")
                    or capture_one.get("retrieval_completed_utc")
                    or ""
                ),
            }
        )
    if len(legs) < MINIMUM_CONFIRMING_PAIRS:
        return _rejected(
            reason="insufficient_tradeable_pair_legs", clock=clock,
            observed=observed, decision_lag_sec=lag,
            diagnostics={"available_pair_count": len(legs)},
        )
    factor_bps = statistics.median(row["driver_return_bps"] for row in legs)
    if abs(factor_bps) < MINIMUM_FACTOR_MOVE_BPS:
        return _rejected(
            reason="factor_move_below_threshold", clock=clock, observed=observed,
            decision_lag_sec=lag,
            diagnostics={"factor_bps": factor_bps, "available_pair_count": len(legs)},
        )
    factor_direction = "strengthen" if factor_bps > 0.0 else "weaken"
    sign = 1.0 if factor_bps > 0.0 else -1.0
    confirmations = [
        row for row in legs
        if sign * row["driver_return_bps"] >= MINIMUM_LEG_MOVE_BPS
    ]
    required = max(MINIMUM_CONFIRMING_PAIRS, math.ceil(len(legs) / 2.0))
    if len(confirmations) < required:
        return _rejected(
            reason="insufficient_cross_pair_confirmation", clock=clock,
            observed=observed, decision_lag_sec=lag,
            diagnostics={
                "factor_bps": factor_bps,
                "available_pair_count": len(legs),
                "confirming_pair_count": len(confirmations),
                "required_pair_count": required,
            },
        )
    cost_clearing = [
        row for row in confirmations
        if abs(row["driver_return_bps"]) >= row["effective_cost_bps"]
    ]
    if not cost_clearing:
        return _rejected(
            reason="no_confirmation_leg_cleared_entry_cost", clock=clock,
            observed=observed, decision_lag_sec=lag,
            diagnostics={
                "factor_bps": factor_bps,
                "confirming_pair_count": len(confirmations),
            },
        )
    selected = min(
        cost_clearing,
        key=lambda row: (row["effective_cost_bps"], row["instrument"]),
    )
    base, quote = selected["instrument"].split("_", 1)
    driver_is_base = base == driver
    long_pair = (factor_direction == "strengthen") == driver_is_base
    side = "long" if long_pair else "short"
    return {
        "status": "forecast",
        "reason": "one_minute_factor_and_cost_confirmation",
        "clock_id": str(clock.get("clock_id") or ""),
        "event_id": str(clock.get("event_id") or ""),
        "event_series_id": str(clock.get("event_series_id") or ""),
        "headline": str(clock.get("headline") or ""),
        "scheduled_utc": iso(scheduled),
        "driver_currency": driver,
        "decision_utc": iso(capture_decision_time),
        "processed_utc": iso(observed),
        "decision_lag_sec": round(lag, 6),
        "processing_lag_sec": round(processing_lag, 6),
        "factor_direction": factor_direction,
        "factor_bps": round(factor_bps, 6),
        "available_pair_count": len(legs),
        "confirming_pair_count": len(confirmations),
        "required_pair_count": required,
        "confirming_pairs": sorted(row["instrument"] for row in confirmations),
        "selected": selected,
        "side": side,
        "direction_source": "post_event_one_minute_multi_pair_factor",
        "event_clock_assigns_no_direction": True,
        "input_capture_lineage": [
            {
                "horizon_min": horizon,
                "capture_id": str(capture.get("capture_id") or ""),
                "payload_sha256": str(capture.get("_ledger_payload_sha256") or ""),
                "response_sha256": str(capture.get("response_sha256") or ""),
                "retrieval_completed_utc": str(
                    capture.get("retrieval_completed_utc") or ""
                ),
            }
            for horizon, capture in ((0, capture_zero), (1, capture_one))
        ],
        "one_currency_factor_counted": True,
        "research_only": True,
        "execution_eligible": False,
        "can_trade": False,
        "can_authorize": False,
        "can_promote": False,
    }


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS decisions (
          decision_id TEXT PRIMARY KEY, cohort_id TEXT NOT NULL,
          clock_id TEXT NOT NULL UNIQUE, event_id TEXT NOT NULL,
          event_series_id TEXT NOT NULL, scheduled_utc TEXT NOT NULL,
          driver_currency TEXT NOT NULL, decision_utc TEXT NOT NULL,
          status TEXT NOT NULL, reason TEXT NOT NULL,
          payload_json TEXT NOT NULL, payload_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_trade INTEGER NOT NULL CHECK(can_trade=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          can_promote INTEGER NOT NULL CHECK(can_promote=0)
        );
        CREATE TABLE IF NOT EXISTS forecasts (
          forecast_id TEXT PRIMARY KEY, cohort_id TEXT NOT NULL,
          decision_id TEXT NOT NULL, clock_id TEXT NOT NULL,
          event_id TEXT NOT NULL, event_series_id TEXT NOT NULL,
          scheduled_utc TEXT NOT NULL, driver_currency TEXT NOT NULL,
          decision_utc TEXT NOT NULL, factor_direction TEXT NOT NULL,
          factor_bps REAL NOT NULL, instrument TEXT NOT NULL,
          side TEXT NOT NULL, entry_quote_time_utc TEXT NOT NULL,
          entry_bid REAL NOT NULL, entry_ask REAL NOT NULL,
          pip REAL NOT NULL, entry_spread_pips REAL NOT NULL,
          horizon_min INTEGER NOT NULL, payload_json TEXT NOT NULL,
          payload_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_trade INTEGER NOT NULL CHECK(can_trade=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          can_promote INTEGER NOT NULL CHECK(can_promote=0),
          UNIQUE(clock_id,horizon_min)
        );
        CREATE TABLE IF NOT EXISTS outcomes (
          outcome_id TEXT PRIMARY KEY, cohort_id TEXT NOT NULL,
          forecast_id TEXT NOT NULL UNIQUE, clock_id TEXT NOT NULL,
          horizon_min INTEGER NOT NULL, outcome_utc TEXT NOT NULL,
          exit_quote_time_utc TEXT NOT NULL, exit_bid REAL NOT NULL,
          exit_ask REAL NOT NULL, executable_net_pips REAL NOT NULL,
          total_slippage_pips REAL NOT NULL, payload_json TEXT NOT NULL,
          payload_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_trade INTEGER NOT NULL CHECK(can_trade=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          can_promote INTEGER NOT NULL CHECK(can_promote=0)
        );
        CREATE TRIGGER IF NOT EXISTS trg_event_factor_decisions_no_update
          BEFORE UPDATE ON decisions BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS trg_event_factor_decisions_no_delete
          BEFORE DELETE ON decisions BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS trg_event_factor_forecasts_no_update
          BEFORE UPDATE ON forecasts BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS trg_event_factor_forecasts_no_delete
          BEFORE DELETE ON forecasts BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS trg_event_factor_outcomes_no_update
          BEFORE UPDATE ON outcomes BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS trg_event_factor_outcomes_no_delete
          BEFORE DELETE ON outcomes BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.commit()
    return connection


def load_input_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    clocks = connection.execute(
        "SELECT clock_id,event_id,event_series_id,headline,scheduled_utc,"
        "driver_currency,contract_id,cohort_id FROM scheduled_event_clock_v2 "
        "WHERE scheduled_utc>=? ORDER BY scheduled_utc,clock_id",
        (iso(COHORT_START),),
    ).fetchall()
    output: list[dict[str, Any]] = []
    for row in clocks:
        captures: dict[int, dict[str, Any]] = {}
        for horizon, capture_id, payload_json, payload_sha256 in connection.execute(
            "SELECT horizon_min,capture_id,payload_json,payload_sha256 "
            "FROM scheduled_event_quote_capture_v2 "
            "WHERE clock_id=? ORDER BY horizon_min", (row[0],)
        ):
            try:
                capture = json.loads(payload_json)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if not isinstance(capture, dict):
                continue
            capture["_ledger_capture_id"] = str(capture_id)
            capture["_ledger_payload_sha256"] = str(payload_sha256)
            capture["_computed_payload_sha256"] = hashlib.sha256(
                str(payload_json).encode("utf-8")
            ).hexdigest()
            captures[int(horizon)] = capture
        output.append(
            {
                "clock_id": row[0], "event_id": row[1],
                "event_series_id": row[2], "headline": row[3],
                "scheduled_utc": row[4], "driver_currency": row[5],
                "contract_id": row[6], "cohort_id": row[7],
                "captures": captures,
            }
        )
    connection.close()
    return output


def persist_decision(
    connection: sqlite3.Connection, decision: Mapping[str, Any]
) -> tuple[int, int]:
    decision_id = stable_id(decision["clock_id"], CONTRACT_ID, COHORT_ID)
    payload = {**dict(decision), "decision_id": decision_id}
    payload_json = canonical_json(payload)
    inserted = connection.execute(
        "INSERT OR IGNORE INTO decisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            decision_id, COHORT_ID, decision["clock_id"], decision["event_id"],
            decision["event_series_id"], decision["scheduled_utc"],
            decision["driver_currency"], decision["decision_utc"],
            decision["status"], decision["reason"], payload_json,
            hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
            1, 0, 0, 0, 0,
        ),
    ).rowcount
    forecast_inserted = 0
    if inserted and decision.get("status") == "forecast":
        selected = decision["selected"]
        for horizon in OUTCOME_HORIZONS_MIN:
            forecast_id = stable_id(decision_id, horizon, COHORT_ID)
            forecast_payload = {
                **payload,
                "forecast_id": forecast_id,
                "horizon_min": horizon,
                "outcome_available_at_decision": False,
            }
            forecast_inserted += connection.execute(
                "INSERT INTO forecasts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    forecast_id, COHORT_ID, decision_id, decision["clock_id"],
                    decision["event_id"], decision["event_series_id"],
                    decision["scheduled_utc"], decision["driver_currency"],
                    decision["decision_utc"], decision["factor_direction"],
                    decision["factor_bps"], selected["instrument"],
                    decision["side"], selected["entry_quote_time_utc"],
                    selected["entry_bid"], selected["entry_ask"], selected["pip"],
                    selected["entry_spread_pips"], horizon,
                    canonical_json(forecast_payload),
                    hashlib.sha256(
                        canonical_json(forecast_payload).encode("utf-8")
                    ).hexdigest(),
                    1, 0, 0, 0, 0,
                ),
            ).rowcount
    connection.commit()
    return inserted, forecast_inserted


def mature_outcomes(
    connection: sqlite3.Connection,
    events: Sequence[Mapping[str, Any]],
) -> int:
    captures_by_clock = {
        str(event["clock_id"]): event.get("captures") or {} for event in events
    }
    rows = connection.execute(
        "SELECT f.forecast_id,f.clock_id,f.horizon_min,f.instrument,f.side,"
        "f.entry_bid,f.entry_ask,f.pip FROM forecasts f "
        "LEFT JOIN outcomes o ON o.forecast_id=f.forecast_id "
        "WHERE o.forecast_id IS NULL ORDER BY f.scheduled_utc,f.horizon_min"
    ).fetchall()
    inserted = 0
    for row in rows:
        forecast_id, clock_id, horizon, instrument, side, entry_bid, entry_ask, pip = row
        capture = captures_by_clock.get(str(clock_id), {}).get(int(horizon))
        if not isinstance(capture, Mapping):
            continue
        if (
            str(capture.get("contract_id") or "") != INPUT_CONTRACT_ID
            or str(capture.get("cohort_id") or "") != INPUT_COHORT_ID
            or str(capture.get("timing_quality") or "") != VALID_TIMING_QUALITY
            or str(capture.get("invalid_reason") or "")
        ):
            continue
        quotes = capture.get("tradeable_quotes")
        quote = quotes.get(instrument) if isinstance(quotes, Mapping) else None
        if not isinstance(quote, Mapping):
            continue
        exit_bid = _finite(quote.get("bid"))
        exit_ask = _finite(quote.get("ask"))
        if exit_bid is None or exit_ask is None or exit_ask <= exit_bid:
            continue
        gross = (
            (exit_bid - float(entry_ask)) / float(pip)
            if side == "long"
            else (float(entry_bid) - exit_ask) / float(pip)
        )
        net = gross - TOTAL_SLIPPAGE_PIPS
        outcome_id = stable_id(forecast_id, "outcome", COHORT_ID)
        payload = {
            "outcome_id": outcome_id, "forecast_id": forecast_id,
            "clock_id": clock_id, "horizon_min": int(horizon),
            "instrument": instrument, "side": side,
            "outcome_utc": str(capture.get("retrieval_completed_utc") or ""),
            "exit_quote_time_utc": str(
                quote.get("broker_price_time_utc")
                or capture.get("response_time_utc")
                or capture.get("retrieval_completed_utc")
                or ""
            ),
            "exit_bid": exit_bid, "exit_ask": exit_ask,
            "executable_net_pips": round(net, 6),
            "total_slippage_pips": TOTAL_SLIPPAGE_PIPS,
            "research_only": True, "execution_eligible": False,
            "can_trade": False, "can_authorize": False, "can_promote": False,
        }
        inserted += connection.execute(
            "INSERT OR IGNORE INTO outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                outcome_id, COHORT_ID, forecast_id, clock_id, int(horizon),
                payload["outcome_utc"], payload["exit_quote_time_utc"],
                exit_bid, exit_ask, round(net, 6), TOTAL_SLIPPAGE_PIPS,
                canonical_json(payload),
                hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest(),
                1, 0, 0, 0, 0,
            ),
        ).rowcount
    connection.commit()
    return inserted


def render_report(payload: Mapping[str, Any]) -> str:
    counts = payload.get("counts") or {}
    lines = [
        "# Scheduled-event factor reaction — current",
        "",
        f"Generated: `{payload.get('generated_utc')}`",
        "",
        "- Research-only; cannot trade, authorize or promote.",
        "- Event clocks assign no direction.",
        "- Direction is locked from the first complete post-event currency factor.",
        "- The 2 September RBNZ move is excluded because it predates activation.",
        "",
        "## Counts",
        "",
        f"- Registered prospective clocks: {counts.get('input_event_count', 0)}",
        f"- Immutable decisions: {counts.get('decision_count', 0)}",
        f"- Forecasts: {counts.get('forecast_count', 0)}",
        f"- Matured outcomes: {counts.get('outcome_count', 0)}",
        "",
    ]
    for row in payload.get("recent_decisions") or []:
        lines.append(
            f"- `{row.get('scheduled_utc')}` {row.get('driver_currency')} "
            f"{row.get('status')}: {row.get('reason')}"
        )
    return "\n".join(lines) + "\n"


def run_once(
    *,
    input_database: Path = INPUT_DATABASE,
    database_path: Path = DATABASE,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
    observed: dt.datetime | None = None,
    heartbeat: WorkerHeartbeat | None = None,
    frozen_config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    frozen_config = dict(frozen_config or validate_frozen_config())
    if observed is None:
        observed, clock_state = normalized_observation_time(dt.datetime.now(UTC))
    else:
        observed = observed.astimezone(UTC)
        clock_state = {"source": "provided", "trusted_for_prospective_evidence": True}
    if heartbeat is not None:
        heartbeat.update(phase="loading_scheduled_event_captures")
    events = load_input_events(input_database)
    connection = open_database(database_path)
    decided_clock_ids = {
        str(row[0]) for row in connection.execute("SELECT clock_id FROM decisions")
    }
    inserted_decisions = 0
    inserted_forecasts = 0
    for event in events:
        clock_id = str(event.get("clock_id") or "")
        captures = event.get("captures") or {}
        if clock_id in decided_clock_ids or 0 not in captures or 1 not in captures:
            continue
        scheduled = parse_utc(event.get("scheduled_utc"))
        if scheduled is None or observed < scheduled + dt.timedelta(minutes=1):
            continue
        decision = evaluate_reaction(
            event, captures[0], captures[1], observed=observed
        )
        added_decision, added_forecasts = persist_decision(connection, decision)
        inserted_decisions += added_decision
        inserted_forecasts += added_forecasts
    if heartbeat is not None:
        heartbeat.update(phase="maturing_scheduled_event_outcomes")
    matured = mature_outcomes(connection, events)
    counts = {
        "input_event_count": len(events),
        "decision_count": int(connection.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]),
        "forecast_count": int(connection.execute("SELECT COUNT(*) FROM forecasts").fetchone()[0]),
        "outcome_count": int(connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0]),
    }
    recent = []
    for payload_json, in connection.execute(
        "SELECT payload_json FROM decisions ORDER BY scheduled_utc DESC LIMIT 12"
    ):
        try:
            recent.append(json.loads(payload_json))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    connection.close()
    payload = {
        "schema_version": "scheduled_event_factor_reaction_v1",
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "cohort_start_utc": iso(COHORT_START),
        "input_contract_id": INPUT_CONTRACT_ID,
        "input_cohort_id": INPUT_COHORT_ID,
        "source_sha256": frozen_config["validated_source_sha256"],
        "config_sha256": frozen_config["config_sha256"],
        "generated_utc": iso(observed),
        "observation_clock": clock_state,
        "status": "ok" if integrity == "ok" else "degraded",
        "sqlite_integrity": integrity,
        "counts": counts,
        "inserted_decisions_this_cycle": inserted_decisions,
        "inserted_forecasts_this_cycle": inserted_forecasts,
        "matured_outcomes_this_cycle": matured,
        "recent_decisions": recent,
        "policy": {
            "event_clock_assigns_no_direction": True,
            "entry_horizon_min": ENTRY_HORIZON_MIN,
            "outcome_horizons_min": list(OUTCOME_HORIZONS_MIN),
            "maximum_decision_lag_sec": MAXIMUM_DECISION_LAG_SEC,
            "maximum_processing_lag_sec": MAXIMUM_PROCESSING_LAG_SEC,
            "minimum_factor_move_bps": MINIMUM_FACTOR_MOVE_BPS,
            "minimum_leg_move_bps": MINIMUM_LEG_MOVE_BPS,
            "minimum_confirming_pairs": MINIMUM_CONFIRMING_PAIRS,
            "selection": "lowest_effective_cost_confirming_pair",
            "total_slippage_pips": TOTAL_SLIPPAGE_PIPS,
            "rbnz_20260902_is_regression_only": True,
            "material_change_requires_new_cohort": True,
        },
        "supported_execution_decision": "no_trade",
        "research_only": True,
        "execution_eligible": False,
        "can_trade": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
    }
    atomic_json(output_path, payload)
    atomic_text(report_path, render_report(payload))
    if heartbeat is not None:
        heartbeat.mark_progress(
            phase="cycle_complete",
            input_event_count=len(events),
            decision_count=counts["decision_count"],
            forecast_count=counts["forecast_count"],
            outcome_count=counts["outcome_count"],
        )
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-database", type=Path, default=INPUT_DATABASE)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT)
    parser.add_argument("--interval-sec", type=float, default=2.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    frozen_config = validate_frozen_config(args.config)
    started = time.monotonic()
    with WorkerHeartbeat(
        args.heartbeat,
        worker="scheduled_event_factor_reaction_v1",
        role="research_only",
        interval_sec=5.0,
    ) as heartbeat:
        while True:
            run_once(
                input_database=args.input_database,
                database_path=args.database,
                output_path=args.output,
                report_path=args.report,
                heartbeat=heartbeat,
                frozen_config=frozen_config,
            )
            if args.once or args.duration_sec <= 0.0:
                return 0
            if time.monotonic() - started >= args.duration_sec:
                return 0
            heartbeat.update(phase="sleeping")
            time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
