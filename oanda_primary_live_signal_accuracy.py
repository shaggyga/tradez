#!/usr/bin/env python3
"""Read-only signal accuracy check for primary forecast-rotation live trades."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from oanda_live_account_readonly_status import LIVE_BASE_URL, cfg_value, read_creds
from oanda_broker_style_portfolio_replay import pip_size, safe_float, safe_int


ROOT = Path(__file__).resolve().parent
DEFAULT_ACTIONS = (
    ROOT
    / "data"
    / "technical_scout_manager"
    / "account_live_primary_forecast_rotation"
    / "actions.csv"
)
DEFAULT_CREDS = ROOT / "creds"
DEFAULT_OUTPUT = ROOT / "data" / "oanda_training_manager" / "reports" / "primary_live_signal_accuracy_20260707"


def iso_z(value: pd.Timestamp | datetime) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC").isoformat().replace("+00:00", "Z")


def direction_int(text: Any) -> int:
    value = str(text or "").strip().lower()
    if value in {"long", "buy", "1"}:
        return 1
    if value in {"short", "sell", "-1"}:
        return -1
    return 0


def load_actions(path: Path, since: pd.Timestamp) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame["time_utc"] = pd.to_datetime(frame["time_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["time_utc"])
    return frame[frame["time_utc"] >= since].copy()


def live_session(session: requests.Session, creds_text: str) -> tuple[str, str]:
    api_key = cfg_value(creds_text, "OANDA_LIVE_API_KEY", "OANDA_API_KEY_LIVE", "OANDA_API_TOKEN_LIVE")
    account_id = cfg_value(creds_text, "OANDA_ACCOUNT_LIVE_PRIMARY", "OANDA_ACCOUNT_ID_PRIMARY_LIVE")
    if not api_key or not account_id:
        raise RuntimeError("Missing OANDA live primary API credentials")
    session.headers.update({"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"})
    return LIVE_BASE_URL, account_id


def fetch_candles(
    session: requests.Session,
    base_url: str,
    instrument: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    if end <= start:
        return pd.DataFrame()
    params = {
        "granularity": "M1",
        "price": "BA",
        "from": iso_z(start),
        "to": iso_z(end),
    }
    response = session.get(f"{base_url}/v3/instruments/{instrument}/candles", params=params, timeout=30)
    try:
        payload = response.json()
    except Exception as exc:
        raise RuntimeError(f"{instrument} candle response was not JSON: {response.text[:200]}") from exc
    if response.status_code == 400 and "future" in str(payload).lower():
        params.pop("to", None)
        response = session.get(f"{base_url}/v3/instruments/{instrument}/candles", params=params, timeout=30)
        try:
            payload = response.json()
        except Exception as exc:
            raise RuntimeError(f"{instrument} candle response was not JSON: {response.text[:200]}") from exc
    if response.status_code != 200:
        raise RuntimeError(f"{instrument} candle fetch failed {response.status_code}: {payload}")
    rows: List[Dict[str, Any]] = []
    for candle in payload.get("candles") or []:
        if not candle.get("complete", True):
            continue
        bid = candle.get("bid") or {}
        ask = candle.get("ask") or {}
        rows.append(
            {
                "time": pd.to_datetime(candle.get("time"), utc=True, errors="coerce"),
                "bid_o": safe_float(bid.get("o")),
                "bid_h": safe_float(bid.get("h")),
                "bid_l": safe_float(bid.get("l")),
                "bid_c": safe_float(bid.get("c")),
                "ask_o": safe_float(ask.get("o")),
                "ask_h": safe_float(ask.get("h")),
                "ask_l": safe_float(ask.get("l")),
                "ask_c": safe_float(ask.get("c")),
            }
        )
    frame = pd.DataFrame(rows).dropna(subset=["time"]) if rows else pd.DataFrame()
    if frame.empty:
        return frame
    frame = frame.sort_values("time").drop_duplicates("time").reset_index(drop=True)
    return frame[frame["time"] <= end].reset_index(drop=True)


def close_rows_by_trade(actions: pd.DataFrame) -> Dict[str, pd.Series]:
    closes = actions[actions["action"].astype(str).str.lower() == "close"].copy()
    out: Dict[str, pd.Series] = {}
    for _, row in closes.sort_values("time_utc").iterrows():
        trade_id = str(safe_int(row.get("broker_trade_id"), 0))
        if trade_id != "0" and trade_id not in out:
            out[trade_id] = row
    return out


def price_pips(direction: int, entry: float, price: float, instrument: str) -> float:
    pip = pip_size(instrument)
    return (price - entry) / pip * direction


def evaluate_trade(row: pd.Series, close: Optional[pd.Series], candles: pd.DataFrame, now_utc: pd.Timestamp) -> Dict[str, Any]:
    inst = str(row.get("instrument") or "")
    direction = direction_int(row.get("direction"))
    trade_id = str(safe_int(row.get("broker_trade_id"), 0))
    open_time = pd.Timestamp(row["time_utc"])
    horizon_minutes = max(1, safe_int(row.get("horizon_minutes"), 120))
    planned_exit = open_time + pd.Timedelta(minutes=horizon_minutes)
    eval_end = min(planned_exit, now_utc)
    entry = safe_float(row.get("entry_ref"), 0.0)
    pip = pip_size(inst)
    active = candles[(candles["time"] >= open_time) & (candles["time"] <= eval_end)].copy()
    after_close = pd.DataFrame()
    close_time = None
    if close is not None:
        close_time = pd.Timestamp(close["time_utc"])
        after_close = candles[(candles["time"] > close_time) & (candles["time"] <= eval_end)].copy()
    if active.empty or entry <= 0 or direction == 0:
        horizon_pips = mfe_pips = mae_pips = after_close_best_pips = after_close_end_pips = None
    else:
        if direction > 0:
            end_price = safe_float(active.iloc[-1]["bid_c"])
            best_price = safe_float(active["bid_h"].max())
            worst_price = safe_float(active["bid_l"].min())
        else:
            end_price = safe_float(active.iloc[-1]["ask_c"])
            best_price = safe_float(active["ask_l"].min())
            worst_price = safe_float(active["ask_h"].max())
        horizon_pips = price_pips(direction, entry, end_price, inst)
        mfe_pips = price_pips(direction, entry, best_price, inst)
        mae_pips = price_pips(direction, entry, worst_price, inst)
        if after_close.empty:
            after_close_best_pips = None
            after_close_end_pips = None
        elif direction > 0:
            after_close_best_pips = price_pips(direction, entry, safe_float(after_close["bid_h"].max()), inst)
            after_close_end_pips = price_pips(direction, entry, safe_float(after_close.iloc[-1]["bid_c"]), inst)
        else:
            after_close_best_pips = price_pips(direction, entry, safe_float(after_close["ask_l"].min()), inst)
            after_close_end_pips = price_pips(direction, entry, safe_float(after_close.iloc[-1]["ask_c"]), inst)
    realized_pips = None if close is None else safe_float(close.get("realized_pips_estimate"), 0.0)
    realized_pl = None if close is None else safe_float(close.get("realized_pl"), 0.0)
    close_profitable = None if close is None else realized_pl > 0
    close_reason = "" if close is None else str(close.get("reason") or "")
    broker_close_reason = "" if close is None else str(close.get("broker_close_reason") or "")
    held_minutes = None if close_time is None else max(0.0, (close_time - open_time).total_seconds() / 60.0)
    closed_before_horizon = bool(close_time is not None and close_time < planned_exit)
    return {
        "broker_trade_id": trade_id,
        "forecast_id": str(row.get("forecast_id") or ""),
        "instrument": inst,
        "direction": str(row.get("direction") or ""),
        "open_time_utc": open_time.isoformat(),
        "planned_exit_utc": planned_exit.isoformat(),
        "eval_end_utc": eval_end.isoformat(),
        "horizon_complete": bool(eval_end >= planned_exit),
        "close_time_utc": "" if close_time is None else close_time.isoformat(),
        "held_minutes": held_minutes,
        "closed_before_horizon": closed_before_horizon,
        "open_reason": str(row.get("reason") or ""),
        "close_reason": close_reason,
        "broker_close_reason": broker_close_reason,
        "rank_score": safe_float(row.get("rank_score")),
        "probability": safe_float(row.get("probability")),
        "edge_pips": safe_float(row.get("edge_pips")),
        "risk_pips": safe_float(row.get("risk_pips")),
        "entry_ref": entry,
        "realized_pips": realized_pips,
        "realized_pl": realized_pl,
        "close_profitable": close_profitable,
        "eval_end_pips": horizon_pips,
        "mfe_pips": mfe_pips,
        "mae_pips": mae_pips,
        "after_close_best_pips": after_close_best_pips,
        "after_close_end_pips": after_close_end_pips,
        "forecast_profitable_at_eval_end": None if horizon_pips is None else horizon_pips > 0,
        "forecast_profitable_at_any_point": None if mfe_pips is None else mfe_pips > 0,
        "closed_then_became_profitable": bool(
            close is not None
            and safe_float(realized_pl, 0.0) <= 0
            and after_close_best_pips is not None
            and after_close_best_pips > 0
        ),
        "lost_or_flat_then_horizon_profitable": bool(
            close is not None
            and safe_float(realized_pl, 0.0) <= 0
            and horizon_pips is not None
            and horizon_pips > 0
        ),
        "candles_used": int(len(active)),
    }


def summarize(results: pd.DataFrame) -> Dict[str, Any]:
    closed = results[results["close_time_utc"].astype(str) != ""].copy()
    completed = results[results["horizon_complete"] == True].copy()
    closed_losers = closed[pd.to_numeric(closed["realized_pl"], errors="coerce").fillna(0.0) <= 0]
    return {
        "trades_evaluated": int(len(results)),
        "closed_trades": int(len(closed)),
        "completed_horizon_trades": int(len(completed)),
        "closed_before_horizon": int((closed["closed_before_horizon"] == True).sum()) if not closed.empty else 0,
        "median_held_minutes": float(pd.to_numeric(closed["held_minutes"], errors="coerce").median()) if not closed.empty else 0.0,
        "losing_or_flat_closed_trades": int(len(closed_losers)),
        "losing_or_flat_later_profitable": int((closed_losers["closed_then_became_profitable"] == True).sum()) if not closed_losers.empty else 0,
        "losing_or_flat_profitable_at_eval_end": int((closed_losers["lost_or_flat_then_horizon_profitable"] == True).sum()) if not closed_losers.empty else 0,
        "forecast_profitable_at_eval_end_rate": float((completed["forecast_profitable_at_eval_end"] == True).mean()) if not completed.empty else 0.0,
        "forecast_profitable_anytime_rate": float((results["forecast_profitable_at_any_point"] == True).mean()) if not results.empty else 0.0,
        "mean_realized_pips_closed": float(pd.to_numeric(closed["realized_pips"], errors="coerce").mean()) if not closed.empty else 0.0,
        "mean_eval_end_pips_completed": float(pd.to_numeric(completed["eval_end_pips"], errors="coerce").mean()) if not completed.empty else 0.0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--actions", type=Path, default=DEFAULT_ACTIONS)
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    parser.add_argument("--since", default="2026-07-07T04:40:57Z")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    since = pd.Timestamp(args.since).tz_convert("UTC")
    actions = load_actions(args.actions, since)
    opens = actions[(actions["action"].astype(str).str.lower() == "open") & (actions["status"].astype(str) == "filled")].copy()
    closes = close_rows_by_trade(actions)
    now_utc = pd.Timestamp(datetime.now(timezone.utc))
    session = requests.Session()
    base_url, _account_id = live_session(session, read_creds(args.creds))

    results: List[Dict[str, Any]] = []
    candle_cache: Dict[str, pd.DataFrame] = {}
    for inst in sorted(opens["instrument"].astype(str).unique()):
        inst_opens = opens[opens["instrument"].astype(str) == inst]
        start = pd.to_datetime(inst_opens["time_utc"], utc=True).min() - pd.Timedelta(minutes=5)
        end = min(
            pd.to_datetime(inst_opens["time_utc"], utc=True).max() + pd.Timedelta(minutes=180),
            now_utc,
        )
        candle_cache[inst] = fetch_candles(session, base_url, inst, start, end)

    for _, row in opens.sort_values("time_utc").iterrows():
        trade_id = str(safe_int(row.get("broker_trade_id"), 0))
        results.append(evaluate_trade(row, closes.get(trade_id), candle_cache.get(str(row.get("instrument")), pd.DataFrame()), now_utc))

    result_frame = pd.DataFrame(results)
    summary = summarize(result_frame)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    result_frame.to_csv(args.output_dir / "trade_signal_accuracy.csv", index=False)
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True, default=str), flush=True)
    if not result_frame.empty:
        cols = [
            "broker_trade_id",
            "instrument",
            "direction",
            "open_reason",
            "close_reason",
            "broker_close_reason",
            "held_minutes",
            "realized_pips",
            "eval_end_pips",
            "mfe_pips",
            "mae_pips",
            "after_close_best_pips",
            "closed_then_became_profitable",
            "lost_or_flat_then_horizon_profitable",
            "horizon_complete",
        ]
        print(result_frame[cols].to_string(index=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
