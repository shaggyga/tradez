#!/usr/bin/env python3
"""Replay near-pass live scout skips against forward M1 candles.

This is read-only research/reporting.  It does not submit, modify, or close
broker orders.  It answers whether scout candidates that narrowly missed live
value/score/move-spread gates later moved favorably enough to justify more
formal backtests.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

import oanda_gpt_training_strategy_manager as manager


ROOT = Path(__file__).resolve().parent
SCOUT_ROOT = ROOT / "data" / "technical_scout_manager"
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports"
DEFAULT_JSON = REPORT_ROOT / "latest_scout_near_pass_replay.json"
DEFAULT_CSV = REPORT_ROOT / "latest_scout_near_pass_replay_rows.csv"
ACCOUNT_AUDITS = {
    "tech": SCOUT_ROOT / "account_live_tech_broad_regime_scout" / "scout_audit_ledger.csv",
    "primary": SCOUT_ROOT / "account_live_primary_challenger_scout" / "scout_audit_ledger.csv",
}
DEFAULT_HORIZONS = [15, 30, 60, 120]
DEFAULT_TRAIL_SPECS = "5:5,10:10,15:15,25:25,10:25,25:50"
DIRECT_USD_QUOTES = {"AUD", "EUR", "GBP", "NZD"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        number = float(value)
        return number if math.isfinite(number) else default
    except Exception:
        return default


def read_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def normalize_instrument(value: Any) -> str:
    return str(value or "").strip().upper().replace("/", "_").replace("-", "_")


def extract_value_gate(text: str) -> tuple[float, float] | None:
    match = re.search(
        r"value\s+gate:\s*expected\s*\$(-?\d+(?:\.\d+)?)\s*<\s*\$(-?\d+(?:\.\d+)?)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return safe_float(match.group(1)), safe_float(match.group(2))


def extract_score_gate(text: str) -> tuple[float, float] | None:
    match = re.search(
        r"score\s+too\s+low\s+(-?\d+(?:\.\d+)?)\s*<\s*(-?\d+(?:\.\d+)?)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return safe_float(match.group(1)), safe_float(match.group(2))


def extract_spread_ratio_gate(text: str) -> tuple[float, float] | None:
    match = re.search(
        r"move/spread\s+(-?\d+(?:\.\d+)?)\s*<\s*(-?\d+(?:\.\d+)?)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return safe_float(match.group(1)), safe_float(match.group(2))


def gate_hits(
    text: str,
    *,
    min_value_gate_ratio: float,
    min_score_ratio: float,
    max_score_gap: float,
    min_spread_ratio: float,
) -> tuple[list[str], dict[str, float]]:
    gates: list[str] = []
    metrics: dict[str, float] = {}
    value_gate = extract_value_gate(text)
    if value_gate:
        expected, threshold = value_gate
        if expected > 0.0 and threshold > 0.0:
            ratio = expected / threshold
            metrics.update({
                "value_expected_usd": expected,
                "value_threshold_usd": threshold,
                "value_gate_ratio": ratio,
            })
            if ratio >= min_value_gate_ratio:
                gates.append("near_value_gate")
    score_gate = extract_score_gate(text)
    if score_gate:
        score, threshold = score_gate
        if threshold > 0.0:
            ratio = score / threshold
            gap = threshold - score
            metrics.update({
                "score": score,
                "score_threshold": threshold,
                "score_ratio": ratio,
                "score_gap": gap,
            })
            if ratio >= min_score_ratio or 0.0 <= gap <= max_score_gap:
                gates.append("near_score_gate")
    spread_gate = extract_spread_ratio_gate(text)
    if spread_gate:
        observed, threshold = spread_gate
        if threshold > 0.0:
            ratio = observed / threshold
            metrics.update({
                "move_spread_ratio": observed,
                "move_spread_threshold": threshold,
                "move_spread_gate_ratio": ratio,
            })
            if ratio >= min_spread_ratio:
                gates.append("near_move_spread_gate")
    return gates, metrics


def load_candidates(
    *,
    accounts: Iterable[str],
    lookback_hours: float,
    min_value_gate_ratio: float,
    min_score_ratio: float,
    max_score_gap: float,
    min_spread_ratio: float,
) -> list[dict[str, Any]]:
    cutoff = utc_now() - timedelta(hours=float(lookback_hours))
    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for account in accounts:
        path = ACCOUNT_AUDITS.get(account)
        if not path:
            continue
        for row in read_csv_rows(path):
            status = str(row.get("status") or "").strip().lower()
            if status not in {"skipped", "rejected", "blocked"}:
                continue
            time_utc = parse_dt(row.get("time_utc") or row.get("time") or row.get("time_ny"))
            if time_utc is None or time_utc < cutoff:
                continue
            instrument = normalize_instrument(row.get("instrument"))
            direction = str(row.get("direction") or "").strip().upper()
            if not instrument or direction not in {"LONG", "SHORT"}:
                continue
            reason = str(row.get("reject_reason") or row.get("reason") or "")
            gates, metrics = gate_hits(
                reason,
                min_value_gate_ratio=min_value_gate_ratio,
                min_score_ratio=min_score_ratio,
                max_score_gap=max_score_gap,
                min_spread_ratio=min_spread_ratio,
            )
            if not gates:
                continue
            key = (
                account,
                time_utc.replace(second=0, microsecond=0).isoformat(),
                instrument,
                direction,
                "|".join(sorted(gates)),
            )
            if key in seen:
                continue
            seen.add(key)
            candidate = {
                "account": account,
                "time_utc": time_utc,
                "instrument": instrument,
                "direction": direction,
                "gates": gates,
                "reject_reason": reason,
                "decision_stage": row.get("decision_stage", ""),
                "pressure_score": safe_float(row.get("pressure_score"), float("nan")),
                "pressure_cluster_instruments": safe_float(row.get("pressure_cluster_instruments"), float("nan")),
                "pressure_cluster_direction_instruments": safe_float(row.get("pressure_cluster_direction_instruments"), float("nan")),
                "net_pips": safe_float(row.get("net_pips"), float("nan")),
            }
            candidate.update(metrics)
            candidates.append(candidate)
    candidates.sort(key=lambda item: item["time_utc"])
    return candidates


def candidate_priority(candidate: dict[str, Any]) -> tuple[float, float, float, float]:
    value = safe_float(candidate.get("value_expected_usd"), 0.0)
    value_ratio = safe_float(candidate.get("value_gate_ratio"), 0.0)
    score_ratio = safe_float(candidate.get("score_ratio"), 0.0)
    spread_ratio = safe_float(candidate.get("move_spread_gate_ratio"), 0.0)
    return (value, max(value_ratio, score_ratio, spread_ratio), score_ratio, spread_ratio)


def select_candidates(
    candidates: list[dict[str, Any]],
    *,
    max_candidates: int,
    max_instruments: int,
) -> list[dict[str, Any]]:
    """Bound API work while preserving the strongest/recent near-pass rows."""
    if max_candidates <= 0 and max_instruments <= 0:
        return candidates
    ranked = sorted(
        candidates,
        key=lambda item: (
            candidate_priority(item),
            item.get("time_utc") or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )
    selected: list[dict[str, Any]] = []
    instruments: set[str] = set()
    for candidate in ranked:
        instrument = str(candidate.get("instrument") or "")
        if max_instruments > 0 and instrument not in instruments and len(instruments) >= max_instruments:
            continue
        if max_candidates > 0 and len(selected) >= max_candidates:
            break
        selected.append(candidate)
        if instrument:
            instruments.add(instrument)
    selected.sort(key=lambda item: item["time_utc"])
    return selected


def candle_rows_from_oanda(candles: list[dict[str, Any]], instrument: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for candle in candles:
        if not candle.get("complete", True):
            continue
        time = parse_dt(candle.get("time"))
        if time is None:
            continue
        mid = candle.get("mid") or {}
        bid = candle.get("bid") or {}
        ask = candle.get("ask") or {}
        rows.append({
            "time": pd.Timestamp(time),
            "instrument": instrument,
            "open": safe_float(mid.get("o"), float("nan")),
            "high": safe_float(mid.get("h"), float("nan")),
            "low": safe_float(mid.get("l"), float("nan")),
            "close": safe_float(mid.get("c"), float("nan")),
            "bid_open": safe_float(bid.get("o"), float("nan")),
            "bid_high": safe_float(bid.get("h"), float("nan")),
            "bid_low": safe_float(bid.get("l"), float("nan")),
            "bid_close": safe_float(bid.get("c"), float("nan")),
            "ask_open": safe_float(ask.get("o"), float("nan")),
            "ask_high": safe_float(ask.get("h"), float("nan")),
            "ask_low": safe_float(ask.get("l"), float("nan")),
            "ask_close": safe_float(ask.get("c"), float("nan")),
        })
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    return frame


def load_local_candles(instrument: str, granularity: str = "M1") -> pd.DataFrame:
    path = manager.DIRS["candles"] / f"{instrument}_{granularity}.csv"
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path)
    time_col = "datetime" if "datetime" in frame.columns else "time"
    if time_col not in frame.columns:
        return pd.DataFrame()
    frame["time"] = pd.to_datetime(frame[time_col], utc=True, errors="coerce")
    frame = frame.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    return frame


def oanda_client() -> manager.OandaClient:
    credentials = manager.load_creds(manager.CREDS_PATH)
    token, base_url, account_id = manager.resolve_oanda_creds(credentials)
    return manager.OandaClient(token, base_url, account_id)


def fetch_recent_candles(instruments: Iterable[str], *, count: int, price: str = "BAM") -> dict[str, pd.DataFrame]:
    client = oanda_client()
    frames: dict[str, pd.DataFrame] = {}
    for instrument in sorted(set(instruments)):
        try:
            data = client.candles(instrument, granularity="M1", count=int(count), price=price)
        except Exception:
            data = {"_error": True, "candles": []}
        frame = candle_rows_from_oanda(data.get("candles", []) or [], instrument)
        prior = load_local_candles(instrument)
        if not prior.empty and not frame.empty:
            frame = (
                pd.concat([prior, frame], ignore_index=True)
                .drop_duplicates(subset=["time"], keep="last")
                .sort_values("time")
                .reset_index(drop=True)
            )
        elif frame.empty:
            frame = prior
        frames[instrument] = frame
    return frames


def price_available(value: Any) -> bool:
    try:
        number = float(value)
        return math.isfinite(number) and number > 0
    except Exception:
        return False


def row_price(row: pd.Series, field: str, fallback: str) -> float:
    value = row.get(field)
    if price_available(value):
        return float(value)
    return safe_float(row.get(fallback), 0.0)


def parse_trail_specs(value: str) -> list[tuple[float, float]]:
    specs: list[tuple[float, float]] = []
    for item in str(value or "").split(","):
        text = item.strip()
        if not text:
            continue
        if ":" in text:
            activation_text, trail_text = text.split(":", 1)
        else:
            activation_text = text
            trail_text = text
        activation = safe_float(activation_text, float("nan"))
        trail = safe_float(trail_text, float("nan"))
        if math.isfinite(activation) and math.isfinite(trail) and activation > 0.0 and trail > 0.0:
            specs.append((activation, trail))
    return specs


def trail_label(activation_pips: float, trail_pips: float) -> str:
    activation = ("%g" % activation_pips).replace(".", "p")
    trail = ("%g" % trail_pips).replace(".", "p")
    return f"a{activation}_t{trail}"


def simulate_trailing_pips(
    *,
    direction: str,
    entry: float,
    frame: pd.DataFrame,
    multiplier: float,
    activation_pips: float,
    trail_pips: float,
    final_pips: float,
) -> tuple[float, bool]:
    """Conservative M1 trailing-stop simulation.

    The replay enters at the first available candle close after the skipped
    scout decision.  To avoid using the pre-entry high/low from that same
    minute, trailing logic starts from the next candle.  If a candle could both
    extend the favorable move and hit the trailing stop, the stop check uses
    only the best favorable move observed before that candle.
    """
    if frame.empty or len(frame) <= 1:
        return final_pips, False
    best_favorable = 0.0
    for _, candle in frame.iloc[1:].iterrows():
        if best_favorable >= activation_pips:
            stop_pips = best_favorable - trail_pips
            if direction == "LONG":
                worst_pips = (row_price(candle, "bid_low", "low") - entry) * multiplier
            else:
                worst_pips = (entry - row_price(candle, "ask_high", "high")) * multiplier
            if worst_pips <= stop_pips:
                return stop_pips, True
        if direction == "LONG":
            favorable = (row_price(candle, "bid_high", "high") - entry) * multiplier
        else:
            favorable = (entry - row_price(candle, "ask_low", "low")) * multiplier
        best_favorable = max(best_favorable, favorable)
    return final_pips, False


def quote_to_usd_map(instruments: Iterable[str]) -> dict[str, float]:
    needed: set[str] = set()
    quotes: set[str] = {"USD"}
    for instrument in instruments:
        if "_" not in instrument:
            continue
        quote = instrument.split("_", 1)[1]
        quotes.add(quote)
        if quote == "USD":
            continue
        if quote in DIRECT_USD_QUOTES:
            needed.add(f"{quote}_USD")
        else:
            needed.add(f"USD_{quote}")
    prices: dict[str, float] = {}
    if needed:
        try:
            data = oanda_client().pricing(sorted(needed))
        except Exception:
            data = {}
        for price in data.get("prices", []) or []:
            instrument = str(price.get("instrument") or "")
            bids = price.get("bids") or []
            asks = price.get("asks") or []
            if not instrument or not bids or not asks:
                continue
            bid = safe_float(bids[0].get("price"), 0.0)
            ask = safe_float(asks[0].get("price"), 0.0)
            if bid > 0.0 and ask > 0.0:
                prices[instrument] = (bid + ask) / 2.0
    out = {"USD": 1.0}
    for quote in quotes:
        if quote == "USD":
            continue
        direct = prices.get(f"{quote}_USD")
        inverse = prices.get(f"USD_{quote}")
        if direct and direct > 0.0:
            out[quote] = direct
        elif inverse and inverse > 0.0:
            out[quote] = 1.0 / inverse
        else:
            out[quote] = 1.0
    return out


def replay_candidate(
    candidate: dict[str, Any],
    frame: pd.DataFrame,
    *,
    horizons: list[int],
    trail_specs: list[tuple[float, float]],
    units: float,
    quote_to_usd: float,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        key: value
        for key, value in candidate.items()
        if key != "time_utc"
    }
    row["time_utc"] = candidate["time_utc"].isoformat()
    row["gates"] = ",".join(candidate.get("gates") or [])
    if frame.empty:
        row["available"] = False
        row["reason"] = "no candle data"
        return row
    event_time = pd.Timestamp(candidate["time_utc"])
    future = frame[frame["time"] >= event_time]
    if future.empty:
        row["available"] = False
        row["reason"] = "no candles after skip time"
        return row
    entry_row = future.iloc[0]
    direction = candidate["direction"]
    instrument = candidate["instrument"]
    multiplier = manager.pips_multiplier(instrument)
    pip_size = 1.0 / multiplier
    if direction == "LONG":
        entry = row_price(entry_row, "ask_close", "close")
    else:
        entry = row_price(entry_row, "bid_close", "close")
    row.update({
        "available": True,
        "entry_time_utc": entry_row["time"].isoformat(),
        "entry_price": entry,
    })
    for horizon in horizons:
        end_time = entry_row["time"] + pd.Timedelta(minutes=int(horizon))
        horizon_frame = frame[(frame["time"] >= entry_row["time"]) & (frame["time"] <= end_time)]
        if horizon_frame.empty:
            continue
        last = horizon_frame.iloc[-1]
        if direction == "LONG":
            final = row_price(last, "bid_close", "close")
            high = max(row_price(candle, "bid_high", "high") for _, candle in horizon_frame.iterrows())
            low = min(row_price(candle, "bid_low", "low") for _, candle in horizon_frame.iterrows())
            final_pips = (final - entry) * multiplier
            mfe_pips = (high - entry) * multiplier
            mae_pips = (low - entry) * multiplier
        else:
            final = row_price(last, "ask_close", "close")
            low = min(row_price(candle, "ask_low", "low") for _, candle in horizon_frame.iterrows())
            high = max(row_price(candle, "ask_high", "high") for _, candle in horizon_frame.iterrows())
            final_pips = (entry - final) * multiplier
            mfe_pips = (entry - low) * multiplier
            mae_pips = (entry - high) * multiplier
        row[f"final_{horizon}m_pips"] = round(final_pips, 4)
        row[f"mfe_{horizon}m_pips"] = round(mfe_pips, 4)
        row[f"mae_{horizon}m_pips"] = round(mae_pips, 4)
        row[f"final_{horizon}m_usd"] = round(final_pips * pip_size * units * quote_to_usd, 6)
        row[f"mfe_{horizon}m_usd"] = round(mfe_pips * pip_size * units * quote_to_usd, 6)
        row[f"mae_{horizon}m_usd"] = round(mae_pips * pip_size * units * quote_to_usd, 6)
        for activation_pips, trail_pips in trail_specs:
            label = trail_label(activation_pips, trail_pips)
            trail_result_pips, trail_hit = simulate_trailing_pips(
                direction=direction,
                entry=entry,
                frame=horizon_frame,
                multiplier=multiplier,
                activation_pips=activation_pips,
                trail_pips=trail_pips,
                final_pips=final_pips,
            )
            row[f"trail_{label}_{horizon}m_pips"] = round(trail_result_pips, 4)
            row[f"trail_{label}_{horizon}m_usd"] = round(
                trail_result_pips * pip_size * units * quote_to_usd,
                6,
            )
            row[f"trail_{label}_{horizon}m_hit"] = bool(trail_hit)
    return row


def summarize_rows(
    rows: list[dict[str, Any]],
    horizons: list[int],
    trail_specs: list[tuple[float, float]],
) -> dict[str, Any]:
    available = [row for row in rows if row.get("available") is True]
    summary: dict[str, Any] = {
        "rows": len(rows),
        "available_rows": len(available),
        "gate_counts": Counter(
            gate
            for row in available
            for gate in str(row.get("gates") or "").split(",")
            if gate
        ).most_common(),
        "account_counts": Counter(str(row.get("account") or "") for row in available).most_common(),
    }
    for horizon in horizons:
        final = [safe_float(row.get(f"final_{horizon}m_usd"), float("nan")) for row in available]
        mfe = [safe_float(row.get(f"mfe_{horizon}m_usd"), float("nan")) for row in available]
        final = [value for value in final if math.isfinite(value)]
        mfe = [value for value in mfe if math.isfinite(value)]
        if not final:
            continue
        summary[f"final_{horizon}m"] = {
            "sum_usd": round(sum(final), 6),
            "mean_usd": round(sum(final) / len(final), 6),
            "positive_rate": round(sum(1 for value in final if value > 0.0) / len(final), 4),
            "rows": len(final),
        }
        if mfe:
            summary[f"mfe_{horizon}m"] = {
                "sum_usd": round(sum(mfe), 6),
                "mean_usd": round(sum(mfe) / len(mfe), 6),
                "positive_rate": round(sum(1 for value in mfe if value > 0.0) / len(mfe), 4),
                "rows": len(mfe),
            }
        trail_summaries: list[dict[str, Any]] = []
        for activation_pips, trail_pips in trail_specs:
            label = trail_label(activation_pips, trail_pips)
            key = f"trail_{label}_{horizon}m_usd"
            hit_key = f"trail_{label}_{horizon}m_hit"
            trail_values = [safe_float(row.get(key), float("nan")) for row in available]
            trail_values = [value for value in trail_values if math.isfinite(value)]
            if not trail_values:
                continue
            hits = [
                row.get(hit_key)
                for row in available
                if math.isfinite(safe_float(row.get(key), float("nan")))
            ]
            trail_summaries.append({
                "config": label,
                "activation_pips": activation_pips,
                "trail_pips": trail_pips,
                "sum_usd": round(sum(trail_values), 6),
                "mean_usd": round(sum(trail_values) / len(trail_values), 6),
                "positive_rate": round(
                    sum(1 for value in trail_values if value > 0.0) / len(trail_values),
                    4,
                ),
                "hit_rate": round(sum(1 for hit in hits if hit is True) / len(hits), 4) if hits else 0.0,
                "rows": len(trail_values),
            })
        if trail_summaries:
            trail_summaries.sort(key=lambda item: item["sum_usd"], reverse=True)
            summary[f"trail_{horizon}m"] = trail_summaries
    by_gate: dict[str, dict[str, Any]] = {}
    for gate in sorted({gate for row in available for gate in str(row.get("gates") or "").split(",") if gate}):
        subset = [row for row in available if gate in str(row.get("gates") or "").split(",")]
        if not subset:
            continue
        gate_summary: dict[str, Any] = {"rows": len(subset)}
        for horizon in horizons:
            values = [safe_float(row.get(f"final_{horizon}m_usd"), float("nan")) for row in subset]
            values = [value for value in values if math.isfinite(value)]
            if values:
                gate_summary[f"final_{horizon}m_sum_usd"] = round(sum(values), 6)
                gate_summary[f"final_{horizon}m_positive_rate"] = round(
                    sum(1 for value in values if value > 0.0) / len(values),
                    4,
                )
            if trail_specs:
                best_gate_trail: dict[str, Any] | None = None
                for activation_pips, trail_pips in trail_specs:
                    label = trail_label(activation_pips, trail_pips)
                    key = f"trail_{label}_{horizon}m_usd"
                    trail_values = [safe_float(row.get(key), float("nan")) for row in subset]
                    trail_values = [value for value in trail_values if math.isfinite(value)]
                    if not trail_values:
                        continue
                    candidate = {
                        "config": label,
                        "sum_usd": round(sum(trail_values), 6),
                        "positive_rate": round(
                            sum(1 for value in trail_values if value > 0.0) / len(trail_values),
                            4,
                        ),
                    }
                    if best_gate_trail is None or candidate["sum_usd"] > best_gate_trail["sum_usd"]:
                        best_gate_trail = candidate
                if best_gate_trail is not None:
                    gate_summary[f"best_trail_{horizon}m"] = best_gate_trail
        by_gate[gate] = gate_summary
    summary["by_gate"] = by_gate
    for horizon in horizons:
        key = f"final_{horizon}m_usd"
        top = sorted(
            available,
            key=lambda row: safe_float(row.get(key), -999999.0),
            reverse=True,
        )[:10]
        summary[f"top_final_{horizon}m"] = [
            {
                "account": row.get("account"),
                "time_utc": row.get("time_utc"),
                "instrument": row.get("instrument"),
                "direction": row.get("direction"),
                "gates": row.get("gates"),
                key: row.get(key),
                f"mfe_{horizon}m_usd": row.get(f"mfe_{horizon}m_usd"),
            }
            for row in top
        ]
        for activation_pips, trail_pips in trail_specs:
            label = trail_label(activation_pips, trail_pips)
            key = f"trail_{label}_{horizon}m_usd"
            top_trail = sorted(
                available,
                key=lambda row: safe_float(row.get(key), -999999.0),
                reverse=True,
            )[:10]
            summary[f"top_trail_{label}_{horizon}m"] = [
                {
                    "account": row.get("account"),
                    "time_utc": row.get("time_utc"),
                    "instrument": row.get("instrument"),
                    "direction": row.get("direction"),
                    "gates": row.get("gates"),
                    key: row.get(key),
                    f"trail_{label}_{horizon}m_hit": row.get(f"trail_{label}_{horizon}m_hit"),
                }
                for row in top_trail
            ]
    return summary


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay near-pass live scout skips against M1 candles.")
    parser.add_argument("--accounts", default="tech,primary", help="Comma-separated account audit keys.")
    parser.add_argument("--lookback-hours", type=float, default=6.0)
    parser.add_argument("--recent-count", type=int, default=1500)
    parser.add_argument("--max-candidates", type=int, default=400)
    parser.add_argument("--max-instruments", type=int, default=12)
    parser.add_argument("--units", type=float, default=10.0, help="Paper/scout unit size for USD estimates.")
    parser.add_argument("--horizons", default="15,30,60,120")
    parser.add_argument(
        "--trail-specs",
        default=DEFAULT_TRAIL_SPECS,
        help="Comma-separated activation:trail pip configs for conservative trailing-exit replay.",
    )
    parser.add_argument("--min-value-gate-ratio", type=float, default=0.75)
    parser.add_argument("--min-score-ratio", type=float, default=0.85)
    parser.add_argument("--max-score-gap", type=float, default=10.0)
    parser.add_argument("--min-spread-ratio", type=float, default=0.65)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV)
    args = parser.parse_args()

    accounts = [item.strip() for item in args.accounts.split(",") if item.strip()]
    horizons = [int(item.strip()) for item in args.horizons.split(",") if item.strip()]
    trail_specs = parse_trail_specs(args.trail_specs)
    candidates = load_candidates(
        accounts=accounts,
        lookback_hours=args.lookback_hours,
        min_value_gate_ratio=args.min_value_gate_ratio,
        min_score_ratio=args.min_score_ratio,
        max_score_gap=args.max_score_gap,
        min_spread_ratio=args.min_spread_ratio,
    )
    raw_candidate_count = len(candidates)
    candidates = select_candidates(
        candidates,
        max_candidates=int(args.max_candidates),
        max_instruments=int(args.max_instruments),
    )
    frames = fetch_recent_candles(
        [str(candidate["instrument"]) for candidate in candidates],
        count=max(int(args.recent_count), max(horizons) + int(args.lookback_hours * 60) + 30),
    )
    conversions = quote_to_usd_map({str(candidate["instrument"]) for candidate in candidates})
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        quote = str(candidate["instrument"]).split("_", 1)[1] if "_" in str(candidate["instrument"]) else "USD"
        rows.append(
            replay_candidate(
                candidate,
                frames.get(str(candidate["instrument"]), pd.DataFrame()),
                horizons=horizons,
                trail_specs=trail_specs,
                units=float(args.units),
                quote_to_usd=float(conversions.get(quote, 1.0)),
            )
        )

    summary = summarize_rows(rows, horizons, trail_specs)
    payload = {
        "generated_utc": utc_now().isoformat(),
        "execution": "read_only_no_broker_writes",
        "accounts": accounts,
        "lookback_hours": args.lookback_hours,
        "units": args.units,
        "horizons": horizons,
        "trail_specs": [
            {"activation_pips": activation, "trail_pips": trail, "label": trail_label(activation, trail)}
            for activation, trail in trail_specs
        ],
        "candidate_rows": len(candidates),
        "raw_candidate_rows": raw_candidate_count,
        "max_candidates": args.max_candidates,
        "max_instruments": args.max_instruments,
        "instruments": sorted({str(candidate["instrument"]) for candidate in candidates}),
        "summary": summary,
    }
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    write_csv(args.csv_out, rows)
    print(json.dumps({
        "status": "ok",
        "candidates": len(candidates),
        "available": summary.get("available_rows", 0),
        "json": str(args.json_out),
        "csv": str(args.csv_out),
        "summary": {
            key: value
            for key, value in summary.items()
            if key.startswith("final_")
            or key.startswith("trail_")
            or key in {"rows", "available_rows", "gate_counts"}
        },
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
