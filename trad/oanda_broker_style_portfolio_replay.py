#!/usr/bin/env python3
"""OANDA-style broker replay for H1/H4 model streams.

This replay is stricter than the ATR proxy portfolio replay:

- joins historical OANDA M1 candle cache for entry/exit prices;
- sizes integer units from stop distance, pip value, NAV, and margin;
- applies per-instrument OANDA margin rates captured from the live account;
- uses bid/ask side fills for long/short entries and exits;
- simulates stop-loss, take-profit, and trailing-stop checks on M1 bars;
- limits new entries per timestamp like the live bot loop.

It is still a backtest: minute bars cannot prove intraminute order, and account
financing/commission remain zero for OANDA spot FX in this local model.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from oanda_h1_h4_guard_recalibration import PROFILES, SCORE_GATES, STREAMS


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
CANDLE_ROOT = DATA_ROOT / "candles"
REPORTS = DATA_ROOT / "reports"
DEFAULT_OUTPUT = REPORTS / "h1_h4_oanda_broker_replay"
PRIMARY_STATE = (
    ROOT
    / "data"
    / "technical_scout_manager"
    / "account_live_primary_forecast_rotation"
    / "state.json"
)
PRIMARY_CONFIG = ROOT / "config" / "primary_forecast_rotation_bot.json"


DEFAULT_MARGIN_RATES = {
    "EUR_USD": 0.02,
    "USD_CAD": 0.02,
    "USD_DKK": 0.02,
    "AUD_CAD": 0.03,
    "AUD_CHF": 0.03,
    "AUD_NZD": 0.03,
    "AUD_USD": 0.03,
    "CAD_CHF": 0.03,
    "EUR_AUD": 0.03,
    "EUR_CHF": 0.03,
    "EUR_NZD": 0.03,
    "EUR_SEK": 0.03,
    "NZD_CAD": 0.03,
    "NZD_CHF": 0.03,
    "NZD_USD": 0.03,
    "AUD_JPY": 0.05,
    "AUD_SGD": 0.05,
    "CAD_JPY": 0.05,
    "CAD_SGD": 0.05,
    "CHF_JPY": 0.05,
    "EUR_CAD": 0.05,
    "EUR_CZK": 0.05,
    "EUR_GBP": 0.05,
    "EUR_HUF": 0.05,
    "EUR_SGD": 0.05,
    "GBP_AUD": 0.05,
    "GBP_CAD": 0.05,
    "GBP_CHF": 0.05,
    "GBP_JPY": 0.05,
    "GBP_NZD": 0.05,
    "GBP_PLN": 0.05,
    "GBP_SGD": 0.05,
    "GBP_USD": 0.05,
    "SGD_CHF": 0.05,
    "SGD_JPY": 0.05,
    "USD_CNH": 0.05,
    "USD_CZK": 0.05,
    "USD_HUF": 0.05,
    "USD_PLN": 0.05,
    "USD_SGD": 0.05,
    "USD_THB": 0.05,
    "CHF_ZAR": 0.07,
    "EUR_NOK": 0.07,
    "EUR_ZAR": 0.07,
    "GBP_ZAR": 0.07,
    "USD_NOK": 0.07,
    "USD_ZAR": 0.07,
    "ZAR_JPY": 0.07,
    "AUD_HKD": 0.10,
    "CAD_HKD": 0.10,
    "CHF_HKD": 0.10,
    "EUR_DKK": 0.10,
    "EUR_HKD": 0.10,
    "GBP_HKD": 0.10,
    "HKD_JPY": 0.10,
    "NZD_HKD": 0.10,
    "USD_HKD": 0.10,
    "USD_MXN": 0.10,
    "EUR_TRY": 0.25,
    "TRY_JPY": 0.25,
    "USD_TRY": 0.25,
}


@dataclass
class BrokerPosition:
    trade_id: int
    instrument: str
    direction: int
    campaign: str
    open_time: pd.Timestamp
    planned_exit_time: pd.Timestamp
    last_check_time: pd.Timestamp
    units: int
    entry_price: float
    entry_mid: float
    stop_loss: float
    take_profit: float
    trailing_distance: float
    trailing_stop: float
    stop_pips: float
    take_profit_pips: float
    risk_pct: float
    risk_usd_estimate: float
    margin_required_open: float
    pip_value_per_unit_usd: float
    rank_score: float
    score_percentile: float
    probability: float
    expected_move_atr: float
    atr240_pips: float
    spread_pips: float
    open_nav: float
    model_type: str
    feature_set: str
    target: str
    outcome: str
    experiment_id: str
    fold: int
    week_start: str
    source_stream: str
    opened_score: float = 0.0
    last_score: float = 0.0
    max_unrealized_pips: float = 0.0
    min_unrealized_pips: float = 0.0
    max_unrealized_usd: float = 0.0
    min_unrealized_usd: float = 0.0
    max_favorable_pips: float = 0.0
    max_adverse_pips: float = 0.0
    max_favorable_usd: float = 0.0
    max_adverse_usd: float = 0.0
    first_profitable_time: str = ""
    first_deep_adverse_time: str = ""


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except Exception:
        return default
    if not math.isfinite(result):
        return default
    return result


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def utc_timestamp(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def pip_size(instrument: str) -> float:
    return 0.01 if instrument.endswith("_JPY") or instrument.endswith("JPY") else 0.0001


def instrument_parts(instrument: str) -> Tuple[str, str]:
    parts = str(instrument).split("_", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    text = str(instrument)
    return text[:3], text[3:6]


def pair_bucket(instrument: str) -> str:
    base, quote = instrument_parts(instrument)
    exotics = {"TRY", "HUF", "ZAR", "MXN", "NOK", "SEK", "PLN", "CZK", "HKD", "CNH", "THB"}
    if base in exotics or quote in exotics:
        return "exotic"
    if base == "USD" or quote == "USD":
        return "major"
    return "cross"


def max_spread_for(cfg: Dict[str, Any], instrument: str) -> float:
    bucket = pair_bucket(instrument)
    if bucket == "exotic":
        return safe_float(cfg.get("max_spread_pips_exotic"), 180.0)
    if bucket == "cross":
        return safe_float(cfg.get("max_spread_pips_cross"), 8.0)
    return safe_float(cfg.get("max_spread_pips_major"), 4.0)


def slippage_for(cfg: Dict[str, Any], instrument: str) -> float:
    if pair_bucket(instrument) == "exotic":
        return safe_float(cfg.get("slippage_pips_exotic"), 2.0)
    return safe_float(cfg.get("slippage_pips_default"), 0.2)


def min_trailing_stop_for(cfg: Dict[str, Any], instrument: str, spread_pips: float) -> float:
    bucket = pair_bucket(instrument)
    if bucket == "exotic":
        floor = safe_float(cfg.get("min_trailing_stop_pips_exotic"), 25.0)
    elif bucket == "cross":
        floor = safe_float(cfg.get("min_trailing_stop_pips_cross"), 8.0)
    else:
        floor = safe_float(cfg.get("min_trailing_stop_pips_major"), 5.0)
    spread_floor = spread_pips * safe_float(cfg.get("min_trailing_spread_multiple"), 2.0)
    return max(floor, spread_floor, 0.1)


def direction_int(value: Any) -> int:
    text = str(value or "").strip().upper()
    return -1 if text.startswith("S") else 1


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def load_candidates(path: Path, min_score: float) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame["time_utc"] = pd.to_datetime(frame["time_utc"], utc=True)
    frame["planned_exit_time"] = pd.to_datetime(frame["planned_exit_time"], utc=True)
    if "score_percentile" not in frame:
        frame["score_percentile"] = frame["rank_score"].rank(method="average", pct=True)
    frame = frame[pd.to_numeric(frame["rank_score"], errors="coerce") >= min_score].copy()
    return frame.sort_values(["time_utc", "rank_score"], ascending=[True, False]).reset_index(drop=True)


class CandleStore:
    def __init__(self, root: Path, timestamps: Iterable[pd.Timestamp], end_time: pd.Timestamp):
        self.root = root
        self.timestamps = pd.DatetimeIndex(sorted({utc_timestamp(ts) for ts in timestamps}))
        self.start = self.timestamps.min() - pd.Timedelta(minutes=2)
        self.end = utc_timestamp(end_time) + pd.Timedelta(minutes=2)
        self.frames: Dict[str, pd.DataFrame] = {}
        self.price_lookup: Dict[pd.Timestamp, Dict[str, float]] = {}

    def load_instrument(self, instrument: str) -> pd.DataFrame:
        instrument = str(instrument)
        cached = self.frames.get(instrument)
        if cached is not None:
            return cached
        path = self.root / f"{instrument}_M1.csv"
        if not path.exists():
            self.frames[instrument] = pd.DataFrame()
            return self.frames[instrument]
        columns = [
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "bid_high",
            "bid_low",
            "bid_close",
            "ask_high",
            "ask_low",
            "ask_close",
            "spread_pips",
        ]
        frame = pd.read_csv(path, usecols=lambda col: col in columns)
        frame["datetime"] = pd.to_datetime(frame["datetime"], utc=True, errors="coerce")
        frame = frame[(frame["datetime"] >= self.start) & (frame["datetime"] <= self.end)].copy()
        if frame.empty:
            self.frames[instrument] = frame
            return frame
        for col in columns:
            if col != "datetime" and col not in frame:
                frame[col] = np.nan
        for col in columns:
            if col != "datetime":
                frame[col] = pd.to_numeric(frame[col], errors="coerce")
        spread = frame["spread_pips"].copy()
        missing_spread = ~np.isfinite(spread)
        if missing_spread.any():
            pip = pip_size(instrument)
            bid_close = frame["bid_close"]
            ask_close = frame["ask_close"]
            derived = (ask_close - bid_close).abs() / pip
            spread = spread.where(np.isfinite(spread), derived)
        spread = spread.fillna(0.0).clip(lower=0.0)
        half = spread * pip_size(instrument) / 2.0
        close = frame["close"].ffill().bfill()
        for side, sign in [("bid", -1.0), ("ask", 1.0)]:
            close_col = f"{side}_close"
            high_col = f"{side}_high"
            low_col = f"{side}_low"
            frame[close_col] = frame[close_col].where(np.isfinite(frame[close_col]), close + sign * half)
            frame[high_col] = frame[high_col].where(np.isfinite(frame[high_col]), frame["high"] + sign * half)
            frame[low_col] = frame[low_col].where(np.isfinite(frame[low_col]), frame["low"] + sign * half)
        frame["close"] = close
        frame["spread_pips"] = spread
        frame = frame.dropna(subset=["datetime", "close"]).sort_values("datetime")
        frame = frame.drop_duplicates("datetime", keep="last").set_index("datetime")
        self.frames[instrument] = frame
        hits = frame.reindex(self.timestamps.intersection(frame.index))
        for ts, row in hits.iterrows():
            self.price_lookup.setdefault(ts, {})[instrument] = safe_float(row.get("close"), 0.0)
        return frame

    def load_all(self, instruments: Iterable[str]) -> None:
        for instrument in sorted(set(map(str, instruments))):
            self.load_instrument(instrument)

    def snapshot(self, instrument: str, time_utc: pd.Timestamp) -> Optional[Dict[str, float]]:
        frame = self.load_instrument(instrument)
        if frame.empty:
            return None
        ts = utc_timestamp(time_utc)
        loc = frame.index.searchsorted(ts, side="right") - 1
        if loc < 0:
            return None
        row = frame.iloc[loc]
        return {
            "time_utc": frame.index[loc],
            "mid": safe_float(row.get("close"), 0.0),
            "bid": safe_float(row.get("bid_close"), 0.0),
            "ask": safe_float(row.get("ask_close"), 0.0),
            "spread_pips": safe_float(row.get("spread_pips"), 0.0),
        }

    def path(self, instrument: str, start_exclusive: pd.Timestamp, end_inclusive: pd.Timestamp) -> pd.DataFrame:
        frame = self.load_instrument(instrument)
        if frame.empty:
            return frame
        start = utc_timestamp(start_exclusive)
        end = utc_timestamp(end_inclusive)
        return frame[(frame.index > start) & (frame.index <= end)]

    def quote_to_usd_rate(self, quote: str, time_utc: pd.Timestamp) -> float:
        quote = str(quote).upper()
        if quote == "USD":
            return 1.0
        ts = utc_timestamp(time_utc)
        bucket = self.price_lookup.get(ts, {})
        direct = bucket.get(f"{quote}_USD")
        if direct and direct > 0:
            return direct
        inverse = bucket.get(f"USD_{quote}")
        if inverse and inverse > 0:
            return 1.0 / inverse
        # Fall back to the nearest row from either cross if it exists.
        for inst, invert in [(f"{quote}_USD", False), (f"USD_{quote}", True)]:
            snap = self.snapshot(inst, ts)
            if snap and snap["mid"] > 0:
                return (1.0 / snap["mid"]) if invert else snap["mid"]
        return 1.0


class BrokerReplay:
    def __init__(
        self,
        profile: Dict[str, Any],
        candles: CandleStore,
        *,
        margin_rates: Dict[str, float],
        live_cfg: Dict[str, Any],
        max_new_positions_per_timestamp: int = 2,
    ):
        self.profile = dict(profile)
        self.candles = candles
        self.margin_rates = margin_rates
        self.live_cfg = dict(live_cfg or {})
        self.live_exit_checks_enabled = bool(self.live_cfg.get("simulate_live_exit_checks", False))
        self.conservative_intrabar_order = bool(self.live_cfg.get("conservative_intrabar_order", False))
        self.deep_adverse_r_threshold = max(
            0.0,
            safe_float(self.live_cfg.get("deep_adverse_r_threshold"), 0.5),
        )
        self.max_new_positions_per_timestamp = max(1, int(max_new_positions_per_timestamp))
        self.balance = safe_float(self.profile.get("start_equity"), 100000.0)
        self.equity_peak = self.balance
        self.open_positions: List[BrokerPosition] = []
        self.closed_rows: List[Dict[str, Any]] = []
        self.blocked_rows: List[Dict[str, Any]] = []
        self.curve_rows: List[Dict[str, Any]] = []
        self.daily_pnl: Dict[str, float] = {}
        self.pair_daily_pnl: Dict[Tuple[str, str], float] = {}
        self.pair_total_pnl: Dict[str, float] = {}
        self.pair_loss_streak: Dict[str, int] = {}
        self.pair_cooldown_until: Dict[str, pd.Timestamp] = {}
        self.last_entry_by_pair: Dict[str, pd.Timestamp] = {}
        self.last_entry_by_campaign: Dict[str, pd.Timestamp] = {}
        self.last_close_by_pair: Dict[str, pd.Timestamp] = {}
        self.rotations_by_time: Dict[str, int] = {}
        self.replacement_times: List[pd.Timestamp] = []
        self.exit_fade_counts: Dict[int, int] = {}
        self.trade_id = 0

    def margin_rate(self, instrument: str) -> float:
        return safe_float(self.margin_rates.get(instrument), safe_float(DEFAULT_MARGIN_RATES.get(instrument), 0.0333))

    def pip_value_per_unit_usd(self, instrument: str, mid: float, time_utc: pd.Timestamp) -> float:
        _base, quote = instrument_parts(instrument)
        return pip_size(instrument) * self.candles.quote_to_usd_rate(quote, time_utc)

    def margin_mid_usd(self, instrument: str, mid: float, time_utc: pd.Timestamp) -> float:
        _base, quote = instrument_parts(instrument)
        return max(0.0, mid * self.candles.quote_to_usd_rate(quote, time_utc))

    def position_margin_required(self, position: BrokerPosition, time_utc: pd.Timestamp) -> float:
        snap = self.candles.snapshot(position.instrument, time_utc)
        mid = safe_float(snap.get("mid") if snap else position.entry_mid, position.entry_mid)
        return abs(position.units) * self.margin_mid_usd(position.instrument, mid, time_utc) * self.margin_rate(position.instrument)

    def position_unrealized(self, position: BrokerPosition, time_utc: pd.Timestamp) -> float:
        snap = self.candles.snapshot(position.instrument, time_utc)
        if not snap:
            return 0.0
        pip = pip_size(position.instrument)
        if position.direction > 0:
            pips = (snap["bid"] - position.entry_price) / pip
        else:
            pips = (position.entry_price - snap["ask"]) / pip
        return pips * position.pip_value_per_unit_usd * abs(position.units)

    def nav(self, time_utc: pd.Timestamp) -> float:
        return self.balance + sum(self.position_unrealized(pos, time_utc) for pos in self.open_positions)

    def margin_used(self, time_utc: pd.Timestamp) -> float:
        return sum(self.position_margin_required(pos, time_utc) for pos in self.open_positions)

    def open_risk_usd(self) -> float:
        return sum(max(0.0, pos.risk_usd_estimate) for pos in self.open_positions)

    def currency_risk_usd(self, currency: str) -> float:
        total = 0.0
        for pos in self.open_positions:
            if currency in set(instrument_parts(pos.instrument)):
                total += max(0.0, pos.risk_usd_estimate)
        return total

    def current_drawdown_pct(self, time_utc: pd.Timestamp) -> float:
        current = self.nav(time_utc)
        peak = max(self.equity_peak, 1e-9)
        return max(0.0, (1.0 - current / peak) * 100.0)

    def drawdown_risk_multiplier(self, time_utc: pd.Timestamp) -> float:
        drawdown = self.current_drawdown_pct(time_utc)
        start = max(0.0, safe_float(self.profile.get("drawdown_risk_throttle_start_pct"), 0.0))
        full = max(start + 0.01, safe_float(self.profile.get("drawdown_risk_throttle_full_pct"), start + 0.01))
        floor = min(1.0, max(0.0, safe_float(self.profile.get("drawdown_risk_min_multiplier"), 1.0)))
        if drawdown <= start:
            return 1.0
        if drawdown >= full:
            return floor
        progress = (drawdown - start) / (full - start)
        return 1.0 - progress * (1.0 - floor)

    def effective_open_risk_pct(self, key: str, time_utc: pd.Timestamp) -> float:
        configured = safe_float(self.profile.get(key), 0.0)
        if configured <= 0:
            return 1e9
        floor = min(1.0, max(0.0, safe_float(self.profile.get("drawdown_open_risk_min_multiplier"), 1.0)))
        return configured * max(floor, self.drawdown_risk_multiplier(time_utc))

    def risk_for_candidate(self, candidate: pd.Series, time_utc: pd.Timestamp) -> float:
        percentile = safe_float(candidate.get("score_percentile"))
        probability = safe_float(candidate.get("probability"))
        if percentile >= 0.97:
            base = safe_float(self.profile.get("max_risk_pct"), 0.65)
        elif percentile >= 0.90:
            base = min(safe_float(self.profile.get("max_risk_pct"), 0.65), safe_float(self.profile.get("high_risk_pct"), 0.52))
        elif percentile >= 0.75:
            base = min(safe_float(self.profile.get("high_risk_pct"), 0.52), safe_float(self.profile.get("medium_risk_pct"), 0.40))
        elif percentile >= 0.50:
            base = min(safe_float(self.profile.get("medium_risk_pct"), 0.40), safe_float(self.profile.get("low_risk_pct"), 0.28))
        else:
            base = safe_float(self.profile.get("min_risk_pct"), 0.18)
        confidence_scale = min(1.2, max(0.6, probability / 0.65))
        risk = min(safe_float(self.profile.get("max_risk_pct"), 0.65), base * confidence_scale)
        risk *= self.drawdown_risk_multiplier(time_utc)
        floor = (
            safe_float(self.profile.get("min_risk_pct"), 0.18)
            if self.drawdown_risk_multiplier(time_utc) >= 0.999
            else safe_float(self.profile.get("min_throttled_risk_pct"), 0.05)
        )
        return max(floor, min(safe_float(self.profile.get("max_risk_pct"), 0.65), risk))

    def date_key(self, time_utc: pd.Timestamp) -> str:
        return utc_timestamp(time_utc).date().isoformat()

    def block(self, candidate: pd.Series, reason: str, detail: str = "") -> None:
        self.blocked_rows.append({
            "time_utc": utc_timestamp(candidate["time_utc"]).isoformat(),
            "instrument": str(candidate["instrument"]),
            "direction": str(candidate["direction"]),
            "reason": reason,
            "detail": detail,
            "rank_score": safe_float(candidate.get("rank_score")),
            "probability": safe_float(candidate.get("probability")),
            "nav": self.nav(utc_timestamp(candidate["time_utc"])),
            "margin_used_pct": self.margin_used(utc_timestamp(candidate["time_utc"])) / max(self.nav(utc_timestamp(candidate["time_utc"])), 1e-9) * 100.0,
            "open_positions": len(self.open_positions),
        })

    def close_position(self, position: BrokerPosition, close_time: pd.Timestamp, reason: str, exit_price: Optional[float] = None) -> None:
        close_time = utc_timestamp(close_time)
        snap = self.candles.snapshot(position.instrument, close_time)
        if exit_price is None:
            if snap:
                exit_price = snap["bid"] if position.direction > 0 else snap["ask"]
            else:
                exit_price = position.entry_price
        pip = pip_size(position.instrument)
        if position.direction > 0:
            realized_pips = (exit_price - position.entry_price) / pip
        else:
            realized_pips = (position.entry_price - exit_price) / pip
        pnl = realized_pips * position.pip_value_per_unit_usd * abs(position.units)
        self.balance += pnl
        self.equity_peak = max(self.equity_peak, self.balance)
        day = self.date_key(close_time)
        self.daily_pnl[day] = self.daily_pnl.get(day, 0.0) + pnl
        pair_day = (day, position.instrument)
        self.pair_daily_pnl[pair_day] = self.pair_daily_pnl.get(pair_day, 0.0) + pnl
        self.pair_total_pnl[position.instrument] = self.pair_total_pnl.get(position.instrument, 0.0) + pnl
        if pnl < 0.0:
            self.pair_loss_streak[position.instrument] = self.pair_loss_streak.get(position.instrument, 0) + 1
        else:
            self.pair_loss_streak[position.instrument] = 0
        if self.live_exit_checks_enabled:
            self.apply_close_cooldowns(position, close_time, reason, pnl)
        stop_risk_pips = max(abs(position.stop_pips), 1e-9)
        mae_r = max(0.0, position.max_adverse_pips / stop_risk_pips)
        mfe_r = max(0.0, position.max_favorable_pips / stop_risk_pips)
        deep_adverse = (
            self.deep_adverse_r_threshold > 0.0
            and mae_r >= self.deep_adverse_r_threshold
        )
        path_adjusted_pnl = float(pnl)
        if deep_adverse:
            path_adjusted_pnl = min(
                path_adjusted_pnl,
                -abs(position.risk_usd_estimate) * min(max(mae_r, self.deep_adverse_r_threshold), 1.5),
            )
        row = asdict(position)
        row.update({
            "close_time": close_time.isoformat(),
            "close_reason": reason,
            "exit_price": float(exit_price),
            "realized_pips": float(realized_pips),
            "pnl": float(pnl),
            "path_adjusted_pnl": float(path_adjusted_pnl),
            "balance_after_close": float(self.balance),
            "nav_after_close": float(self.nav(close_time)),
            "held_minutes": max(0.0, (close_time - position.open_time).total_seconds() / 60.0),
            "exit_fade_count": int(self.exit_fade_counts.get(position.trade_id, 0)),
            "mae_r": float(mae_r),
            "mfe_r": float(mfe_r),
            "deep_adverse": bool(deep_adverse),
            "rescued_winner": bool(pnl > 0.0 and deep_adverse),
            "clean_winner": bool(pnl > 0.0 and not deep_adverse),
        })
        self.closed_rows.append(row)
        self.last_close_by_pair[position.instrument] = close_time
        self.exit_fade_counts.pop(position.trade_id, None)
        self.open_positions = [pos for pos in self.open_positions if pos.trade_id != position.trade_id]

    def set_pair_cooldown(self, instrument: str, from_time: pd.Timestamp, minutes: float) -> None:
        minutes = safe_float(minutes, 0.0)
        if minutes <= 0:
            return
        until = utc_timestamp(from_time) + pd.Timedelta(minutes=minutes)
        current = self.pair_cooldown_until.get(str(instrument))
        if current is None or until > current:
            self.pair_cooldown_until[str(instrument)] = until

    def apply_close_cooldowns(
        self,
        position: BrokerPosition,
        close_time: pd.Timestamp,
        reason: str,
        pnl: float,
    ) -> None:
        reason = str(reason)
        if "rotation" in reason:
            self.set_pair_cooldown(
                position.instrument,
                close_time,
                safe_float(self.live_cfg.get("pair_cooldown_minutes_after_rotation"), 0.0),
            )
        if reason in {"take_profit", "stop_or_trailing_stop", "paper_stop_hit", "paper_take_profit_hit"}:
            self.set_pair_cooldown(
                position.instrument,
                close_time,
                safe_float(self.live_cfg.get("pair_cooldown_minutes_after_broker_side_close"), 0.0),
            )
        if pnl >= 0.0:
            return
        nav = max(self.nav(close_time), 1e-9)
        loss_pct = -pnl / nav * 100.0
        cooldown_minutes = safe_float(self.live_cfg.get("pair_loss_cooldown_minutes"), 0.0)
        streak_limit = safe_int(self.live_cfg.get("pair_loss_streak_cooldown_trades"), 0)
        if streak_limit > 0 and self.pair_loss_streak.get(position.instrument, 0) >= streak_limit:
            self.set_pair_cooldown(position.instrument, close_time, cooldown_minutes)
        large_loss_pct = safe_float(self.live_cfg.get("pair_large_loss_cooldown_pct"), 0.0)
        if large_loss_pct > 0 and loss_pct >= large_loss_pct:
            self.set_pair_cooldown(position.instrument, close_time, cooldown_minutes)
        day_loss_pct = max(
            0.0,
            -self.pair_daily_pnl.get((self.date_key(close_time), position.instrument), 0.0) / nav * 100.0,
        )
        max_pair_daily = safe_float(self.live_cfg.get("max_pair_daily_loss_pct"), 0.0)
        if max_pair_daily > 0 and day_loss_pct >= max_pair_daily:
            self.set_pair_cooldown(position.instrument, close_time, cooldown_minutes)
        total_loss_pct = max(0.0, -self.pair_total_pnl.get(position.instrument, 0.0) / nav * 100.0)
        max_pair_total = safe_float(self.live_cfg.get("max_pair_total_loss_pct"), 0.0)
        if max_pair_total > 0 and total_loss_pct >= max_pair_total:
            self.set_pair_cooldown(position.instrument, close_time, cooldown_minutes)

    def process_protective_orders(self, current_time: pd.Timestamp) -> None:
        current_time = utc_timestamp(current_time)
        for position in list(self.open_positions):
            end = current_time if self.live_exit_checks_enabled else min(current_time, position.planned_exit_time)
            if end <= position.last_check_time:
                continue
            rows = self.candles.path(position.instrument, position.last_check_time, end)
            if rows.empty:
                position.last_check_time = end
                continue
            pip = pip_size(position.instrument)
            closed = False
            for ts, row in rows.iterrows():
                self.update_position_excursion_from_row(position, ts, row)
                if position.direction > 0:
                    high = safe_float(row.get("bid_high"), safe_float(row.get("high"), position.entry_price))
                    low = safe_float(row.get("bid_low"), safe_float(row.get("low"), position.entry_price))
                    if self.conservative_intrabar_order:
                        active_stop = max(position.stop_loss, position.trailing_stop)
                        if low <= active_stop:
                            self.close_position(position, ts, "stop_or_trailing_stop", active_stop)
                            closed = True
                            break
                        if high >= position.take_profit:
                            self.close_position(position, ts, "take_profit", position.take_profit)
                            closed = True
                            break
                        position.trailing_stop = max(position.trailing_stop, high - position.trailing_distance * pip)
                    else:
                        position.trailing_stop = max(position.trailing_stop, high - position.trailing_distance * pip)
                        active_stop = max(position.stop_loss, position.trailing_stop)
                        if low <= active_stop:
                            self.close_position(position, ts, "stop_or_trailing_stop", active_stop)
                            closed = True
                            break
                        if high >= position.take_profit:
                            self.close_position(position, ts, "take_profit", position.take_profit)
                            closed = True
                            break
                else:
                    high = safe_float(row.get("ask_high"), safe_float(row.get("high"), position.entry_price))
                    low = safe_float(row.get("ask_low"), safe_float(row.get("low"), position.entry_price))
                    if self.conservative_intrabar_order:
                        active_stop = min(position.stop_loss, position.trailing_stop)
                        if high >= active_stop:
                            self.close_position(position, ts, "stop_or_trailing_stop", active_stop)
                            closed = True
                            break
                        if low <= position.take_profit:
                            self.close_position(position, ts, "take_profit", position.take_profit)
                            closed = True
                            break
                        position.trailing_stop = min(position.trailing_stop, low + position.trailing_distance * pip)
                    else:
                        position.trailing_stop = min(position.trailing_stop, low + position.trailing_distance * pip)
                        active_stop = min(position.stop_loss, position.trailing_stop)
                        if high >= active_stop:
                            self.close_position(position, ts, "stop_or_trailing_stop", active_stop)
                            closed = True
                            break
                        if low <= position.take_profit:
                            self.close_position(position, ts, "take_profit", position.take_profit)
                            closed = True
                            break
            if not closed:
                position.last_check_time = end
        if not self.live_exit_checks_enabled:
            due = [pos for pos in self.open_positions if pos.planned_exit_time <= current_time]
            for position in sorted(due, key=lambda item: item.planned_exit_time):
                self.close_position(position, position.planned_exit_time, "scheduled_exit")

    def candidate_trade_terms(self, candidate: pd.Series, time_utc: pd.Timestamp) -> Optional[Dict[str, Any]]:
        inst = str(candidate["instrument"])
        snap = self.candles.snapshot(inst, time_utc)
        if not snap or snap["mid"] <= 0 or snap["bid"] <= 0 or snap["ask"] <= 0:
            return None
        direction = direction_int(candidate.get("direction"))
        pip = pip_size(inst)
        spread_pips = max(safe_float(candidate.get("spread_pips"), snap["spread_pips"]), snap["spread_pips"], 0.0)
        atr_pips = max(0.1, safe_float(candidate.get("atr240_pips"), 0.1))
        edge_pips = max(
            0.0,
            safe_float(candidate.get("expected_move_atr")) * atr_pips
            - spread_pips
            - slippage_for(self.live_cfg, inst),
        )
        stop_pips = max(
            atr_pips * safe_float(self.live_cfg.get("atr_stop_multiplier"), 0.85),
            spread_pips * safe_float(self.live_cfg.get("min_stop_spread_multiple"), 2.0),
            0.1,
        )
        take_profit_pips = max(
            edge_pips * safe_float(self.live_cfg.get("take_profit_edge_capture"), 0.45),
            stop_pips * safe_float(self.live_cfg.get("take_profit_min_r_multiple"), 0.35),
        )
        trailing_stop_pips = max(
            stop_pips * safe_float(self.live_cfg.get("trailing_stop_r_multiple"), 0.35),
            min_trailing_stop_for(self.live_cfg, inst, spread_pips),
        )
        entry = snap["ask"] if direction > 0 else snap["bid"]
        stop_loss = entry - stop_pips * pip if direction > 0 else entry + stop_pips * pip
        take_profit = entry + take_profit_pips * pip if direction > 0 else entry - take_profit_pips * pip
        trailing_stop = entry - trailing_stop_pips * pip if direction > 0 else entry + trailing_stop_pips * pip
        return {
            "direction": direction,
            "entry": entry,
            "mid": snap["mid"],
            "spread_pips": spread_pips,
            "atr_pips": atr_pips,
            "edge_pips": edge_pips,
            "stop_pips": stop_pips,
            "take_profit_pips": take_profit_pips,
            "trailing_stop_pips": trailing_stop_pips,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_stop": trailing_stop,
        }

    def position_unrealized_pips(self, position: BrokerPosition, time_utc: pd.Timestamp) -> float:
        snap = self.candles.snapshot(position.instrument, time_utc)
        if not snap:
            return 0.0
        pip = pip_size(position.instrument)
        if position.direction > 0:
            return (snap["bid"] - position.entry_price) / pip
        return (position.entry_price - snap["ask"]) / pip

    def update_position_excursion(
        self,
        position: BrokerPosition,
        time_utc: pd.Timestamp,
        *,
        favorable_pips: float,
        adverse_pips: float,
    ) -> None:
        favorable_pips = max(0.0, safe_float(favorable_pips))
        adverse_pips = max(0.0, safe_float(adverse_pips))
        if favorable_pips > position.max_favorable_pips:
            position.max_favorable_pips = float(favorable_pips)
            position.max_unrealized_pips = float(favorable_pips)
            position.max_unrealized_usd = float(
                favorable_pips * position.pip_value_per_unit_usd * abs(position.units)
            )
        if adverse_pips > position.max_adverse_pips:
            position.max_adverse_pips = float(adverse_pips)
            position.min_unrealized_pips = float(-adverse_pips)
            position.min_unrealized_usd = float(
                -adverse_pips * position.pip_value_per_unit_usd * abs(position.units)
            )
            stop_pips = max(abs(position.stop_pips), 1e-9)
            if (
                not position.first_deep_adverse_time
                and self.deep_adverse_r_threshold > 0
                and adverse_pips / stop_pips >= self.deep_adverse_r_threshold
            ):
                position.first_deep_adverse_time = utc_timestamp(time_utc).isoformat()
        if favorable_pips > 0.0 and not position.first_profitable_time:
            position.first_profitable_time = utc_timestamp(time_utc).isoformat()

    def update_position_excursion_from_row(
        self,
        position: BrokerPosition,
        time_utc: pd.Timestamp,
        row: pd.Series,
    ) -> None:
        pip = pip_size(position.instrument)
        if position.direction > 0:
            high = safe_float(row.get("bid_high"), safe_float(row.get("high"), position.entry_price))
            low = safe_float(row.get("bid_low"), safe_float(row.get("low"), position.entry_price))
            favorable = (high - position.entry_price) / pip
            adverse = (position.entry_price - low) / pip
        else:
            high = safe_float(row.get("ask_high"), safe_float(row.get("high"), position.entry_price))
            low = safe_float(row.get("ask_low"), safe_float(row.get("low"), position.entry_price))
            favorable = (position.entry_price - low) / pip
            adverse = (high - position.entry_price) / pip
        self.update_position_excursion(
            position,
            time_utc,
            favorable_pips=favorable,
            adverse_pips=adverse,
        )

    def current_candidate_state(
        self,
        candidate: Optional[pd.Series],
        time_utc: pd.Timestamp,
        units: int = 0,
    ) -> Tuple[bool, bool, float, str]:
        if candidate is None:
            return False, True, 0.0, "missing_current_forecast"
        inst = str(candidate["instrument"])
        if safe_float(candidate.get("expected_move_atr")) <= 0.0:
            return False, True, 0.0, "non_positive_expected_edge"
        if safe_float(candidate.get("rank_score")) < safe_float(self.profile.get("min_rank_score"), 0.0):
            return False, True, 0.0, "below_min_rank_score"
        if safe_float(candidate.get("score_percentile")) < safe_float(self.profile.get("min_score_percentile"), 0.0):
            return False, True, 0.0, "below_min_score_percentile"
        if safe_float(candidate.get("probability")) < safe_float(self.profile.get("min_probability"), 0.0):
            return False, True, 0.0, "below_min_probability"
        terms = self.candidate_trade_terms(candidate, time_utc)
        if not terms:
            return False, True, 0.0, "missing_price"
        if terms["spread_pips"] > max_spread_for(self.live_cfg, inst):
            return False, True, terms["edge_pips"], "spread_above_gate"
        min_edge = safe_float(self.live_cfg.get("min_edge_pips"), 0.0)
        if terms["edge_pips"] < min_edge or terms["edge_pips"] <= 0.0:
            return False, True, terms["edge_pips"], "edge_below_gate"
        max_spread_to_edge = safe_float(self.live_cfg.get("max_spread_to_edge_ratio"), 0.0)
        if max_spread_to_edge > 0 and terms["spread_pips"] / max(terms["edge_pips"], 0.01) > max_spread_to_edge:
            return False, True, terms["edge_pips"], "spread_edge_ratio_above_gate"
        economics_ok = True
        if units:
            abs_units = abs(int(units))
            pip_value = self.pip_value_per_unit_usd(inst, terms["mid"], time_utc)
            expected_profit = terms["edge_pips"] * pip_value * abs_units
            take_profit_usd = terms["take_profit_pips"] * pip_value * abs_units
            margin_required = self.margin_mid_usd(inst, terms["mid"], time_utc) * self.margin_rate(inst) * abs_units
            if expected_profit < safe_float(self.live_cfg.get("min_expected_profit_usd"), 0.0):
                economics_ok = False
            elif take_profit_usd < safe_float(self.live_cfg.get("min_take_profit_usd"), 0.0):
                economics_ok = False
            elif margin_required > 0:
                profit_per_margin = expected_profit / max(margin_required, 1e-9)
                if profit_per_margin < safe_float(self.live_cfg.get("min_profit_per_margin"), 0.0):
                    economics_ok = False
        return True, economics_ok, terms["edge_pips"], ""

    def best_current_candidates(self, group: pd.DataFrame) -> Tuple[Dict[Tuple[str, int], pd.Series], Dict[Tuple[str, int], pd.Series]]:
        best_same: Dict[Tuple[str, int], pd.Series] = {}
        best_opposite: Dict[Tuple[str, int], pd.Series] = {}
        if group.empty:
            return best_same, best_opposite
        ordered = group.sort_values(["rank_score", "probability"], ascending=[False, False])
        for _, candidate in ordered.iterrows():
            inst = str(candidate["instrument"])
            direction = direction_int(candidate.get("direction"))
            same_key = (inst, direction)
            if same_key not in best_same:
                best_same[same_key] = candidate
            opposite_key = (inst, -direction)
            if opposite_key not in best_opposite:
                best_opposite[opposite_key] = candidate
        return best_same, best_opposite

    def process_live_exit_checks(self, current_time: pd.Timestamp, group: pd.DataFrame) -> None:
        if not self.live_exit_checks_enabled or not self.open_positions:
            return
        current = utc_timestamp(current_time)
        best_same, best_opposite = self.best_current_candidates(group)
        min_hold = safe_float(self.live_cfg.get("min_hold_minutes"), 8.0)
        hold_floor = safe_float(self.live_cfg.get("hold_score_floor_fraction"), 0.45)
        fade_confirm_cycles = max(1, safe_int(self.live_cfg.get("exit_fade_confirm_cycles"), 1))
        signal_gone_loser_policy = str(
            self.live_cfg.get("signal_gone_loser_policy") or "allow"
        ).strip().lower()
        signal_gone_loser_buffer = max(
            0.0,
            safe_float(self.live_cfg.get("signal_gone_loser_buffer_pips"), 0.0),
        )
        for pos in list(self.open_positions):
            age_minutes = max(0.0, (current - pos.open_time).total_seconds() / 60.0)
            current_candidate = best_same.get((pos.instrument, pos.direction))
            opposite_candidate = best_opposite.get((pos.instrument, pos.direction))
            current_tradable, current_economics_ok, _edge, _reject = self.current_candidate_state(
                current_candidate,
                current,
                pos.units,
            )
            current_score = safe_float(current_candidate.get("rank_score"), 0.0) if current_candidate is not None else 0.0
            opened_score = pos.opened_score or pos.rank_score
            pos.opened_score = opened_score
            pos.last_score = current_score if current_candidate is not None else 0.0
            score_faded = (not current_tradable) or current_score < opened_score * hold_floor
            opposite_stronger = False
            if opposite_candidate is not None:
                opposite_tradable, _opposite_economics_ok, _opposite_edge, _opposite_reject = self.current_candidate_state(
                    opposite_candidate,
                    current,
                    0,
                )
                reference_score = max(pos.last_score, opened_score)
                threshold = max(
                    reference_score + safe_float(self.live_cfg.get("replacement_min_score_improvement"), 0.35),
                    reference_score * safe_float(self.live_cfg.get("opposite_min_score_multiplier"), 1.0),
                )
                opposite_stronger = opposite_tradable and safe_float(opposite_candidate.get("rank_score")) > threshold
            fade_basis = score_faded or opposite_stronger
            if fade_basis:
                fade_count = self.exit_fade_counts.get(pos.trade_id, 0) + 1
            else:
                fade_count = 0
            self.exit_fade_counts[pos.trade_id] = fade_count
            fade_confirmed = fade_count >= fade_confirm_cycles
            unrealized_pips = self.position_unrealized_pips(pos, current)
            take_profit_pips = abs(pos.take_profit - pos.entry_price) / pip_size(pos.instrument)
            profit_lock_age = safe_float(self.live_cfg.get("profit_lock_min_age_minutes", min_hold), min_hold)
            profit_lock_pips = max(
                safe_float(self.live_cfg.get("profit_lock_min_pips"), 0.0),
                abs(pos.stop_pips) * safe_float(self.live_cfg.get("profit_lock_r_multiple"), 0.0),
            )
            profit_lock_hit = (
                age_minutes >= profit_lock_age
                and profit_lock_pips > 0
                and unrealized_pips >= profit_lock_pips
                and fade_confirmed
            )
            signal_gone_exit_age = safe_float(self.live_cfg.get("signal_gone_exit_minutes", min_hold), min_hold)
            signal_gone_exit_age = max(
                signal_gone_exit_age,
                max(1, safe_float((pos.planned_exit_time - pos.open_time).total_seconds() / 60.0, 1.0))
                * safe_float(self.live_cfg.get("signal_gone_min_horizon_fraction", 0.0), 0.0),
            )
            horizon_minutes = max(1.0, (pos.planned_exit_time - pos.open_time).total_seconds() / 60.0)
            economics_exit_age = max(
                min_hold,
                horizon_minutes * safe_float(self.live_cfg.get("economics_exit_min_horizon_fraction", 0.0), 0.0),
            )
            soft_loss_exit_age = max(
                min_hold,
                horizon_minutes * safe_float(self.live_cfg.get("soft_loss_exit_min_horizon_fraction", 0.0), 0.0),
            )
            reason = ""
            if unrealized_pips <= -abs(pos.stop_pips):
                reason = "paper_stop_hit"
            elif unrealized_pips >= take_profit_pips:
                reason = "paper_take_profit_hit"
            elif profit_lock_hit:
                reason = "profit_lock_churn"
            elif age_minutes >= signal_gone_exit_age and not current_tradable:
                losing_signal_gone = unrealized_pips < -signal_gone_loser_buffer
                defer_loser = (
                    signal_gone_loser_policy in {
                        "defer_losers",
                        "defer_losers_unless_opposite",
                        "defer_losers_until_stop",
                    }
                    and losing_signal_gone
                )
                allow_opposite_override = (
                    signal_gone_loser_policy == "defer_losers_unless_opposite"
                    and opposite_stronger
                )
                if fade_confirmed and (not defer_loser or allow_opposite_override):
                    reason = "signal_no_longer_positive"
            elif age_minutes >= economics_exit_age and current_tradable and not current_economics_ok:
                reason = "economics_below_floor"
            elif (
                age_minutes >= soft_loss_exit_age
                and unrealized_pips <= -abs(pos.stop_pips) * safe_float(self.live_cfg.get("soft_loss_r_multiple"), 1.0)
                and fade_confirmed
            ):
                reason = "soft_loss_signal_fade"
            elif age_minutes >= horizon_minutes:
                hold_same_signal = (
                    bool(self.live_cfg.get("hold_same_signal_past_horizon", False))
                    and current_tradable
                    and current_economics_ok
                    and not score_faded
                    and not opposite_stronger
                )
                if not hold_same_signal:
                    reason = "horizon_expired"
            elif age_minutes >= min_hold:
                stale_age = age_minutes >= horizon_minutes * safe_float(self.live_cfg.get("stale_horizon_fraction"), 0.80)
                if stale_age and score_faded and fade_confirmed:
                    reason = "stale_forecast_decay"
                elif opposite_stronger and fade_confirmed:
                    reason = "opposite_forecast_stronger"
            if reason:
                self.close_position(pos, current, reason)

    def size_units(self, candidate: pd.Series, terms: Dict[str, Any], time_utc: pd.Timestamp, released_margin: float = 0.0) -> Tuple[int, Dict[str, float]]:
        inst = str(candidate["instrument"])
        nav = max(self.nav(time_utc), 0.0)
        if nav <= 0:
            return 0, {}
        risk_pct = self.risk_for_candidate(candidate, time_utc)
        risk_usd = nav * risk_pct / 100.0
        pip_value = self.pip_value_per_unit_usd(inst, terms["mid"], time_utc)
        units = int(risk_usd / max(terms["stop_pips"] * pip_value, 1e-9))
        if bool(self.live_cfg.get("size_to_min_profit_usd", True)):
            min_units = 0
            min_expected = safe_float(self.live_cfg.get("min_expected_profit_usd"), 0.0)
            min_take_profit = safe_float(self.live_cfg.get("min_take_profit_usd"), 0.0)
            if min_expected > 0 and terms["edge_pips"] > 0:
                min_units = max(min_units, math.ceil(min_expected / max(terms["edge_pips"] * pip_value, 1e-9)))
            if min_take_profit > 0 and terms["take_profit_pips"] > 0:
                min_units = max(min_units, math.ceil(min_take_profit / max(terms["take_profit_pips"] * pip_value, 1e-9)))
            if min_units > 0:
                max_trade_risk_usd = nav * safe_float(self.profile.get("max_risk_pct"), 0.65) / 100.0
                max_risk_units = int(max_trade_risk_usd / max(terms["stop_pips"] * pip_value, 1e-9))
                units = min(max(units, min_units), max_risk_units)
        margin_per_unit = self.margin_mid_usd(inst, terms["mid"], time_utc) * self.margin_rate(inst)
        hard_margin_total = nav * safe_float(self.profile.get("hard_margin_pct"), 74.0) / 100.0
        remaining_margin = max(0.0, hard_margin_total - self.margin_used(time_utc) + released_margin)
        margin_units = int(remaining_margin / max(margin_per_unit, 1e-9))
        units = min(units, max(0, margin_units))
        if units < 1:
            return 0, {}
        signed = units if terms["direction"] > 0 else -units
        risk_usd_estimate = terms["stop_pips"] * pip_value * units
        margin_required = margin_per_unit * units
        return signed, {
            "risk_pct": risk_usd_estimate / max(nav, 1e-9) * 100.0,
            "risk_usd_estimate": risk_usd_estimate,
            "margin_required": margin_required,
            "pip_value": pip_value,
        }

    def can_pass_portfolio_caps(self, candidate: pd.Series, risk_usd: float, margin_required: float, time_utc: pd.Timestamp) -> Tuple[bool, str]:
        nav = max(self.nav(time_utc), 1e-9)
        open_risk_after_pct = (self.open_risk_usd() + risk_usd) / nav * 100.0
        if open_risk_after_pct > self.effective_open_risk_pct("target_open_risk_pct", time_utc):
            if safe_float(candidate.get("score_percentile")) < safe_float(self.profile.get("above_target_min_score_percentile"), 0.8):
                return False, "below_quality_for_above_target_risk"
        if open_risk_after_pct > self.effective_open_risk_pct("max_open_risk_pct", time_utc):
            return False, "open_risk_cap"
        margin_after_pct = (self.margin_used(time_utc) + margin_required) / nav * 100.0
        if margin_after_pct > safe_float(self.profile.get("target_margin_pct"), 62.0):
            if safe_float(candidate.get("score_percentile")) < safe_float(self.profile.get("above_target_min_score_percentile"), 0.8):
                return False, "below_quality_for_above_target_margin"
        if margin_after_pct > safe_float(self.profile.get("hard_margin_pct"), 74.0):
            return False, "hard_margin_cap"
        if margin_after_pct > safe_float(self.profile.get("emergency_margin_pct"), 88.0):
            return False, "emergency_margin_cap"
        max_currency_pct = safe_float(self.profile.get("max_currency_risk_pct"), 0.0)
        if max_currency_pct > 0:
            for currency in instrument_parts(str(candidate["instrument"])):
                after = (self.currency_risk_usd(currency) + risk_usd) / nav * 100.0
                if after > max_currency_pct:
                    return False, f"currency_risk_cap:{currency}:{after:.2f}>{max_currency_pct:.2f}"
        return True, ""

    def replacement_slots_available(self, time_utc: pd.Timestamp) -> int:
        limit = safe_int(
            self.profile.get(
                "max_replacements_per_hour",
                self.live_cfg.get("max_replacements_per_hour", self.profile.get("max_rotations_per_timestamp", 2)),
            ),
            0,
        )
        if limit <= 0:
            self.replacement_times = []
            return 0
        current = utc_timestamp(time_utc)
        cutoff = current - pd.Timedelta(hours=1)
        self.replacement_times = [ts for ts in self.replacement_times if ts >= cutoff]
        return max(0, limit - len(self.replacement_times))

    def maybe_rotate(self, candidate: pd.Series, risk_usd: float, margin_required: float, time_utc: pd.Timestamp) -> None:
        key = utc_timestamp(time_utc).isoformat()
        rotations = self.rotations_by_time.get(key, 0)
        nav = max(self.nav(time_utc), 1e-9)
        margin_pressure = (self.margin_used(time_utc) + margin_required) / nav * 100.0 > safe_float(self.profile.get("target_margin_pct"), 62.0)
        risk_pressure = (self.open_risk_usd() + risk_usd) / nav * 100.0 > self.effective_open_risk_pct("target_open_risk_pct", time_utc)
        capacity_pressure = len(self.open_positions) >= safe_int(self.profile.get("max_open_positions"), 18)
        quality_rotation = (
            safe_float(candidate.get("score_percentile")) >= safe_float(self.profile.get("stale_rotation_min_score_percentile"), 0.90)
            and len(self.open_positions) >= max(1, int(safe_int(self.profile.get("max_open_positions"), 18) * safe_float(self.profile.get("stale_rotation_open_share"), 0.70)))
        )
        if not (margin_pressure or risk_pressure or capacity_pressure or quality_rotation):
            return
        replacement_slots = self.replacement_slots_available(time_utc)
        if replacement_slots <= 0:
            return
        per_timestamp_limit = max(1, safe_int(self.profile.get("max_rotations_per_timestamp"), replacement_slots))
        replacement_min_age = safe_float(
            self.live_cfg.get(
                "replacement_min_age_minutes",
                self.profile.get("min_rotation_age_minutes", self.live_cfg.get("min_hold_minutes", 8.0)),
            ),
            8.0,
        )
        replacement_min_improvement = safe_float(
            self.live_cfg.get("replacement_min_score_improvement", self.profile.get("replacement_min_score_improvement", 0.35)),
            0.35,
        )
        replacement_multiplier = safe_float(
            self.live_cfg.get("replacement_min_score_multiplier", self.profile.get("rotation_score_multiplier", 1.25)),
            1.25,
        )
        eligible: List[BrokerPosition] = []
        current = utc_timestamp(time_utc)
        candidate_score = safe_float(candidate.get("rank_score"))
        for pos in self.open_positions:
            age = max(0.0, (current - pos.open_time).total_seconds() / 60.0)
            stale = age >= safe_float(self.profile.get("stale_minutes"), 90.0)
            if age < replacement_min_age:
                continue
            if not stale and not (margin_pressure or risk_pressure or capacity_pressure):
                continue
            if not stale and pos.instrument == str(candidate["instrument"]):
                continue
            opened_score = pos.opened_score or pos.rank_score
            reference_score = max(pos.last_score, opened_score * 0.50)
            required_score = max(reference_score + replacement_min_improvement, reference_score * replacement_multiplier)
            if candidate_score - reference_score < replacement_min_improvement or candidate_score < required_score:
                continue
            eligible.append(pos)
        eligible.sort(
            key=lambda pos: (
                pos.last_score,
                -max(0.0, (current - pos.open_time).total_seconds() / 60.0),
                self.position_unrealized_pips(pos, current),
            )
        )
        for pos in eligible:
            if rotations >= per_timestamp_limit or replacement_slots <= 0:
                break
            self.close_position(pos, current, "replacement_rotation")
            rotations += 1
            replacement_slots -= 1
            self.replacement_times.append(current)
            nav = max(self.nav(time_utc), 1e-9)
            if (
                (self.margin_used(time_utc) + margin_required) / nav * 100.0 <= safe_float(self.profile.get("target_margin_pct"), 62.0)
                and (self.open_risk_usd() + risk_usd) / nav * 100.0 <= self.effective_open_risk_pct("target_open_risk_pct", time_utc)
                and len(self.open_positions) < safe_int(self.profile.get("max_open_positions"), 18)
            ):
                break
        self.rotations_by_time[key] = rotations

    def open_candidate(self, candidate: pd.Series, time_utc: pd.Timestamp) -> bool:
        inst = str(candidate["instrument"])
        if safe_float(candidate.get("expected_move_atr")) <= 0.0:
            self.block(candidate, "non_positive_expected_edge")
            return False
        if safe_float(candidate.get("rank_score")) < safe_float(self.profile.get("min_rank_score"), 0.0):
            self.block(candidate, "below_min_rank_score")
            return False
        if safe_float(candidate.get("score_percentile")) < safe_float(self.profile.get("min_score_percentile"), 0.0):
            self.block(candidate, "below_min_score_percentile")
            return False
        if safe_float(candidate.get("probability")) < safe_float(self.profile.get("min_probability"), 0.0):
            self.block(candidate, "below_min_probability")
            return False
        cooldown = self.pair_cooldown_until.get(inst)
        if cooldown is not None and time_utc < cooldown:
            self.block(candidate, "pair_loss_cooldown", cooldown.isoformat())
            return False
        if self.live_exit_checks_enabled:
            nav_for_pair_guards = max(self.nav(time_utc), 1e-9)
            pair_daily_loss_pct = max(
                0.0,
                -self.pair_daily_pnl.get((self.date_key(time_utc), inst), 0.0) / nav_for_pair_guards * 100.0,
            )
            max_pair_daily = safe_float(self.live_cfg.get("max_pair_daily_loss_pct"), 0.0)
            if max_pair_daily > 0 and pair_daily_loss_pct >= max_pair_daily:
                self.block(candidate, "pair_daily_loss_stop", f"{pair_daily_loss_pct:.2f}%")
                return False
            pair_total_loss_pct = max(0.0, -self.pair_total_pnl.get(inst, 0.0) / nav_for_pair_guards * 100.0)
            max_pair_total = safe_float(self.live_cfg.get("max_pair_total_loss_pct"), 0.0)
            if max_pair_total > 0 and pair_total_loss_pct >= max_pair_total:
                self.block(candidate, "pair_total_loss_stop", f"{pair_total_loss_pct:.2f}%")
                return False
            streak_limit = safe_int(self.live_cfg.get("pair_loss_streak_cooldown_trades"), 0)
            if streak_limit > 0 and self.pair_loss_streak.get(inst, 0) >= streak_limit:
                self.block(candidate, "pair_loss_streak_stop", str(self.pair_loss_streak.get(inst, 0)))
                return False
        same_pair = [pos for pos in self.open_positions if pos.instrument == inst]
        same_direction = [pos for pos in same_pair if pos.direction == direction_int(candidate.get("direction"))]
        opposite = [pos for pos in same_pair if pos.direction != direction_int(candidate.get("direction"))]
        if len(same_direction) >= safe_int(self.profile.get("max_same_campaign_positions"), 1):
            self.block(candidate, "duplicate_campaign_open")
            return False
        if len(same_pair) >= safe_int(self.profile.get("max_same_pair_positions"), 1) and not opposite:
            self.block(candidate, "same_pair_cap")
            return False
        if opposite:
            best_opposite = max(pos.rank_score for pos in opposite)
            oldest_age = max((time_utc - pos.open_time).total_seconds() / 60.0 for pos in opposite)
            if (
                safe_float(candidate.get("rank_score")) < best_opposite * safe_float(self.profile.get("opposite_rotation_multiplier"), 1.15)
                or oldest_age < safe_float(self.profile.get("min_opposite_rotation_age_minutes"), 30.0)
            ):
                self.block(candidate, "opposite_position_open")
                return False
            for pos in list(opposite):
                self.close_position(pos, time_utc, "opposite_rotation")
        last_pair = self.last_entry_by_pair.get(inst)
        if last_pair is not None:
            minutes = (time_utc - last_pair).total_seconds() / 60.0
            if minutes < safe_float(self.profile.get("min_pair_entry_gap_minutes"), 20.0):
                self.block(candidate, "pair_entry_cooldown", f"{minutes:.1f}")
                return False
        last_close = self.last_close_by_pair.get(inst)
        if last_close is not None:
            minutes = (time_utc - last_close).total_seconds() / 60.0
            if minutes < safe_float(self.profile.get("min_after_close_gap_minutes"), 10.0):
                self.block(candidate, "after_close_cooldown", f"{minutes:.1f}")
                return False
        if len(self.open_positions) >= safe_int(self.profile.get("max_open_positions"), 18):
            self.block(candidate, "max_open_positions")
            return False
        terms = self.candidate_trade_terms(candidate, time_utc)
        if not terms:
            self.block(candidate, "missing_price")
            return False
        if terms["spread_pips"] > max_spread_for(self.live_cfg, inst):
            self.block(candidate, "spread_above_gate", f"{terms['spread_pips']:.2f}>{max_spread_for(self.live_cfg, inst):.2f}")
            return False
        min_edge = safe_float(self.live_cfg.get("min_edge_pips"), 0.0)
        if terms["edge_pips"] < min_edge:
            self.block(candidate, "edge_below_gate", f"{terms['edge_pips']:.2f}<{min_edge:.2f}")
            return False
        max_spread_to_edge = safe_float(self.live_cfg.get("max_spread_to_edge_ratio"), 0.0)
        if max_spread_to_edge > 0 and terms["spread_pips"] / max(terms["edge_pips"], 0.01) > max_spread_to_edge:
            self.block(
                candidate,
                "spread_edge_ratio_above_gate",
                f"{terms['spread_pips'] / max(terms['edge_pips'], 0.01):.3f}>{max_spread_to_edge:.3f}",
            )
            return False
        units, economics = self.size_units(candidate, terms, time_utc)
        if units == 0:
            self.block(candidate, "zero_units_or_margin")
            return False
        self.maybe_rotate(candidate, economics["risk_usd_estimate"], economics["margin_required"], time_utc)
        if len(self.open_positions) >= safe_int(self.profile.get("max_open_positions"), 18):
            self.block(candidate, "max_open_positions")
            return False
        allowed, reason = self.can_pass_portfolio_caps(candidate, economics["risk_usd_estimate"], economics["margin_required"], time_utc)
        if not allowed:
            self.block(candidate, reason)
            return False
        nav = max(self.nav(time_utc), 1e-9)
        expected_profit = terms["edge_pips"] * economics["pip_value"] * abs(units)
        take_profit_usd = terms["take_profit_pips"] * economics["pip_value"] * abs(units)
        if expected_profit < safe_float(self.live_cfg.get("min_expected_profit_usd"), 0.0):
            self.block(candidate, "expected_profit_usd_floor", f"{expected_profit:.6f}")
            return False
        if take_profit_usd < safe_float(self.live_cfg.get("min_take_profit_usd"), 0.0):
            self.block(candidate, "take_profit_usd_floor", f"{take_profit_usd:.6f}")
            return False
        profit_per_margin = expected_profit / max(economics["margin_required"], 1e-9)
        if profit_per_margin < safe_float(self.live_cfg.get("min_profit_per_margin"), 0.0):
            self.block(candidate, "profit_per_margin_floor", f"{profit_per_margin:.6f}")
            return False
        self.trade_id += 1
        pos = BrokerPosition(
            trade_id=self.trade_id,
            instrument=inst,
            direction=terms["direction"],
            campaign=str(candidate.get("campaign") or f"{inst}:{candidate.get('direction')}"),
            open_time=time_utc,
            planned_exit_time=utc_timestamp(candidate["planned_exit_time"]),
            last_check_time=time_utc,
            units=units,
            entry_price=terms["entry"],
            entry_mid=terms["mid"],
            stop_loss=terms["stop_loss"],
            take_profit=terms["take_profit"],
            trailing_distance=terms["trailing_stop_pips"],
            trailing_stop=terms["trailing_stop"],
            stop_pips=terms["stop_pips"],
            take_profit_pips=terms["take_profit_pips"],
            risk_pct=economics["risk_usd_estimate"] / nav * 100.0,
            risk_usd_estimate=economics["risk_usd_estimate"],
            margin_required_open=economics["margin_required"],
            pip_value_per_unit_usd=economics["pip_value"],
            rank_score=safe_float(candidate.get("rank_score")),
            score_percentile=safe_float(candidate.get("score_percentile")),
            probability=safe_float(candidate.get("probability")),
            expected_move_atr=safe_float(candidate.get("expected_move_atr")),
            atr240_pips=safe_float(candidate.get("atr240_pips")),
            spread_pips=terms["spread_pips"],
            open_nav=nav,
            model_type=str(candidate.get("model_type") or ""),
            feature_set=str(candidate.get("feature_set") or ""),
            target=str(candidate.get("target") or ""),
            outcome=str(candidate.get("outcome") or ""),
            experiment_id=str(candidate.get("experiment_id") or ""),
            fold=safe_int(candidate.get("fold")),
            week_start=str(candidate.get("week_start") or ""),
            source_stream=str(candidate.get("source_stream") or ""),
            opened_score=safe_float(candidate.get("rank_score")),
            last_score=safe_float(candidate.get("rank_score")),
        )
        self.open_positions.append(pos)
        self.last_entry_by_pair[inst] = time_utc
        self.last_entry_by_campaign[pos.campaign] = time_utc
        return True

    def record_curve(self, time_utc: pd.Timestamp) -> None:
        current_nav = self.nav(time_utc)
        self.equity_peak = max(self.equity_peak, current_nav)
        margin = self.margin_used(time_utc)
        self.curve_rows.append({
            "time_utc": utc_timestamp(time_utc).isoformat(),
            "balance": float(self.balance),
            "nav": float(current_nav),
            "margin_used": float(margin),
            "margin_used_pct": margin / max(current_nav, 1e-9) * 100.0,
            "margin_available": float(current_nav - margin),
            "open_risk_usd": float(self.open_risk_usd()),
            "open_risk_pct": self.open_risk_usd() / max(current_nav, 1e-9) * 100.0,
            "drawdown_pct": self.current_drawdown_pct(time_utc),
            "open_positions": len(self.open_positions),
        })

    def run(self, candidates: pd.DataFrame) -> Dict[str, Any]:
        if candidates.empty:
            return self.summary(candidates)
        for time_utc, group in candidates.groupby("time_utc", sort=True):
            current = utc_timestamp(time_utc)
            self.process_protective_orders(current)
            self.process_live_exit_checks(current, group)
            opened = 0
            ordered = group.sort_values(["rank_score", "probability"], ascending=[False, False])
            for _, candidate in ordered.iterrows():
                if opened >= self.max_new_positions_per_timestamp:
                    self.block(candidate, "live_new_entry_cycle_cap")
                    continue
                before = len(self.open_positions)
                if self.open_candidate(candidate, current):
                    opened += max(0, len(self.open_positions) - before)
            self.record_curve(current)
        final_time = max(pd.to_datetime(candidates["planned_exit_time"], utc=True).max(), pd.to_datetime(candidates["time_utc"], utc=True).max())
        if self.live_exit_checks_enabled:
            self.process_protective_orders(final_time)
            self.process_live_exit_checks(final_time, candidates.iloc[0:0])
            self.record_curve(final_time)
            return self.summary(candidates)
        while self.open_positions:
            next_exit = min(pos.planned_exit_time for pos in self.open_positions)
            self.process_protective_orders(next_exit)
            self.record_curve(next_exit)
            if next_exit > final_time + pd.Timedelta(days=5):
                break
        return self.summary(candidates)

    def summary(self, candidates: pd.DataFrame) -> Dict[str, Any]:
        trades = pd.DataFrame(self.closed_rows)
        blocked = pd.DataFrame(self.blocked_rows)
        curve = pd.DataFrame(self.curve_rows)
        if not curve.empty:
            max_drawdown_pct = float(curve["drawdown_pct"].max())
            avg_margin_pct = float(curve["margin_used_pct"].mean())
            max_margin_pct = float(curve["margin_used_pct"].max())
            avg_open_risk_pct = float(curve["open_risk_pct"].mean())
            max_open_risk_pct = float(curve["open_risk_pct"].max())
            min_margin_available = float(curve["margin_available"].min())
            margin_call_rows = int((curve["margin_available"] < 0).sum())
        else:
            max_drawdown_pct = avg_margin_pct = max_margin_pct = avg_open_risk_pct = max_open_risk_pct = 0.0
            min_margin_available = 0.0
            margin_call_rows = 0
        if not trades.empty:
            pnl = pd.to_numeric(trades["pnl"], errors="coerce").fillna(0.0)
            path_adjusted_pnl = pd.to_numeric(
                trades.get("path_adjusted_pnl", trades["pnl"]),
                errors="coerce",
            ).fillna(0.0)
            wins = pnl[pnl > 0]
            losses = pnl[pnl < 0]
            gross_profit = float(wins.sum())
            gross_loss = abs(float(losses.sum()))
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else (99.0 if gross_profit > 0 else 0.0)
            win_rate = float((pnl > 0).mean())
            avg_pips = float(pd.to_numeric(trades["realized_pips"], errors="coerce").mean())
            path_wins = path_adjusted_pnl[path_adjusted_pnl > 0]
            path_losses = path_adjusted_pnl[path_adjusted_pnl < 0]
            path_gross_profit = float(path_wins.sum())
            path_gross_loss = abs(float(path_losses.sum()))
            path_profit_factor = (
                path_gross_profit / path_gross_loss
                if path_gross_loss > 0
                else (99.0 if path_gross_profit > 0 else 0.0)
            )
            path_win_rate = float((path_adjusted_pnl > 0.0).mean())
            path_adjusted_ending_balance = safe_float(self.profile.get("start_equity"), 100000.0) + float(path_adjusted_pnl.sum())
            mae_r = pd.to_numeric(trades.get("mae_r", 0.0), errors="coerce").fillna(0.0)
            mfe_r = pd.to_numeric(trades.get("mfe_r", 0.0), errors="coerce").fillna(0.0)
            mae_pips = pd.to_numeric(trades.get("max_adverse_pips", 0.0), errors="coerce").fillna(0.0)
            mfe_pips = pd.to_numeric(trades.get("max_favorable_pips", 0.0), errors="coerce").fillna(0.0)
            deep_adverse = trades.get("deep_adverse", pd.Series(False, index=trades.index)).astype(bool)
            rescued_winner = trades.get("rescued_winner", pd.Series(False, index=trades.index)).astype(bool)
            clean_winner = trades.get("clean_winner", pd.Series(False, index=trades.index)).astype(bool)
        else:
            profit_factor = win_rate = avg_pips = 0.0
            path_adjusted_pnl = pd.Series(dtype=float)
            path_profit_factor = path_win_rate = 0.0
            path_adjusted_ending_balance = safe_float(self.profile.get("start_equity"), 100000.0)
            mae_r = mfe_r = mae_pips = mfe_pips = pd.Series(dtype=float)
            deep_adverse = rescued_winner = clean_winner = pd.Series(dtype=bool)
        start = safe_float(self.profile.get("start_equity"), 100000.0)
        return {
            "candidate_rows": int(len(candidates)),
            "opened_trades": int(len(trades)),
            "blocked_candidates": int(len(blocked)),
            "ending_balance": float(self.balance),
            "return_pct": (self.balance / max(start, 1e-9) - 1.0) * 100.0,
            "profit_factor": float(profit_factor),
            "win_rate": float(win_rate),
            "path_adjusted_ending_balance": float(path_adjusted_ending_balance),
            "path_adjusted_return_pct": (path_adjusted_ending_balance / max(start, 1e-9) - 1.0) * 100.0,
            "path_adjusted_profit_factor": float(path_profit_factor),
            "path_adjusted_win_rate": float(path_win_rate),
            "deep_adverse_r_threshold": float(self.deep_adverse_r_threshold),
            "deep_adverse_trades": int(deep_adverse.sum()) if not trades.empty else 0,
            "deep_adverse_trade_share": float(deep_adverse.mean()) if not trades.empty else 0.0,
            "rescued_winners": int(rescued_winner.sum()) if not trades.empty else 0,
            "rescued_winner_share": float(rescued_winner.mean()) if not trades.empty else 0.0,
            "clean_winners": int(clean_winner.sum()) if not trades.empty else 0,
            "clean_win_rate": float(clean_winner.mean()) if not trades.empty else 0.0,
            "avg_mae_r": float(mae_r.mean()) if not mae_r.empty else 0.0,
            "p95_mae_r": float(mae_r.quantile(0.95)) if not mae_r.empty else 0.0,
            "max_mae_r": float(mae_r.max()) if not mae_r.empty else 0.0,
            "avg_mfe_r": float(mfe_r.mean()) if not mfe_r.empty else 0.0,
            "p95_mfe_r": float(mfe_r.quantile(0.95)) if not mfe_r.empty else 0.0,
            "avg_mae_pips": float(mae_pips.mean()) if not mae_pips.empty else 0.0,
            "p95_mae_pips": float(mae_pips.quantile(0.95)) if not mae_pips.empty else 0.0,
            "max_mae_pips": float(mae_pips.max()) if not mae_pips.empty else 0.0,
            "avg_mfe_pips": float(mfe_pips.mean()) if not mfe_pips.empty else 0.0,
            "p95_mfe_pips": float(mfe_pips.quantile(0.95)) if not mfe_pips.empty else 0.0,
            "max_drawdown_pct": float(max_drawdown_pct),
            "avg_margin_used_pct": float(avg_margin_pct),
            "max_margin_used_pct": float(max_margin_pct),
            "avg_open_risk_pct": float(avg_open_risk_pct),
            "max_open_risk_pct": float(max_open_risk_pct),
            "min_margin_available": float(min_margin_available),
            "margin_call_rows": int(margin_call_rows),
            "avg_realized_pips": float(avg_pips),
            "stop_or_trailing_exits": int((trades.get("close_reason") == "stop_or_trailing_stop").sum()) if not trades.empty else 0,
            "take_profit_exits": int((trades.get("close_reason") == "take_profit").sum()) if not trades.empty else 0,
            "scheduled_exits": int((trades.get("close_reason") == "scheduled_exit").sum()) if not trades.empty else 0,
            "stale_rotations": int((trades.get("close_reason") == "stale_rotation").sum()) if not trades.empty else 0,
            "replacement_rotations": int((trades.get("close_reason") == "replacement_rotation").sum()) if not trades.empty else 0,
            "opposite_rotations": int((trades.get("close_reason") == "opposite_rotation").sum()) if not trades.empty else 0,
            "profit_lock_exits": int((trades.get("close_reason") == "profit_lock_churn").sum()) if not trades.empty else 0,
            "signal_gone_exits": int((trades.get("close_reason") == "signal_no_longer_positive").sum()) if not trades.empty else 0,
            "economics_floor_exits": int((trades.get("close_reason") == "economics_below_floor").sum()) if not trades.empty else 0,
            "soft_loss_fade_exits": int((trades.get("close_reason") == "soft_loss_signal_fade").sum()) if not trades.empty else 0,
            "horizon_expired_exits": int((trades.get("close_reason") == "horizon_expired").sum()) if not trades.empty else 0,
            "stale_forecast_decay_exits": int((trades.get("close_reason") == "stale_forecast_decay").sum()) if not trades.empty else 0,
            "opposite_forecast_stronger_exits": int((trades.get("close_reason") == "opposite_forecast_stronger").sum()) if not trades.empty else 0,
            "max_open_positions_seen": int(curve["open_positions"].max()) if not curve.empty else 0,
        }

    def write_outputs(self, output_dir: Path, summary: Dict[str, Any], candidates: pd.DataFrame) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=str), encoding="utf-8")
        pd.DataFrame(self.closed_rows).to_csv(output_dir / "trades.csv", index=False)
        pd.DataFrame(self.blocked_rows).to_csv(output_dir / "blocked_candidates.csv", index=False)
        pd.DataFrame(self.curve_rows).to_csv(output_dir / "equity_curve.csv", index=False)
        candidates.to_csv(output_dir / "candidate_rows.csv", index=False)


def load_margin_rates() -> Dict[str, float]:
    state = read_json(PRIMARY_STATE, {})
    rates = state.get("broker_margin_rates") if isinstance(state, dict) else {}
    out = dict(DEFAULT_MARGIN_RATES)
    if isinstance(rates, dict):
        for key, value in rates.items():
            rate = safe_float(value, 0.0)
            if rate > 0:
                out[str(key)] = rate
    return out


def objective(summary: Dict[str, Any]) -> float:
    trades = safe_float(summary.get("opened_trades"), 0.0)
    if trades < 100 or safe_float(summary.get("margin_call_rows"), 0.0) > 0:
        return -9999.0
    ret = safe_float(summary.get("return_pct"), 0.0)
    dd = safe_float(summary.get("max_drawdown_pct"), 0.0)
    pf = safe_float(summary.get("profit_factor"), 0.0)
    return (ret / max(1.0, dd)) * max(0.0, min(3.0, pf))


def scenario_filter(name: str) -> List[Tuple[str, str, str]]:
    if name == "top":
        return [
            ("h1", "all", "h_tf_growth"),
            ("h4", "all", "h_tf_growth"),
            ("h1", "score_ge_012", "h_tf_growth"),
        ]
    rows: List[Tuple[str, str, str]] = []
    for stream in STREAMS:
        for gate in SCORE_GATES:
            for profile in PROFILES:
                rows.append((stream, gate, profile))
    return rows


def run(output_root: Path, scenario: str) -> pd.DataFrame:
    live_cfg = read_json(PRIMARY_CONFIG, {})
    selected = scenario_filter(scenario)
    loaded_candidates: Dict[Tuple[str, str], pd.DataFrame] = {}
    all_times: List[pd.Timestamp] = []
    all_instruments: set[str] = set()
    end_time = pd.Timestamp.min.tz_localize("UTC")
    for stream, gate, _profile in selected:
        key = (stream, gate)
        if key not in loaded_candidates:
            candidates = load_candidates(STREAMS[stream], SCORE_GATES[gate])
            loaded_candidates[key] = candidates
            all_times.extend(pd.to_datetime(candidates["time_utc"], utc=True).tolist())
            all_instruments.update(candidates["instrument"].astype(str).unique())
            end_time = max(end_time, pd.to_datetime(candidates["planned_exit_time"], utc=True).max())
    # Include USD crosses needed for quote conversion.
    for inst in list(CANDLE_ROOT.glob("*_M1.csv")):
        name = inst.name.removesuffix("_M1.csv")
        base, quote = instrument_parts(name)
        if base == "USD" or quote == "USD":
            all_instruments.add(name)
    candles = CandleStore(CANDLE_ROOT, all_times, end_time)
    candles.load_all(all_instruments)
    margin_rates = load_margin_rates()
    output_root.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, Any]] = []
    for stream, gate, profile_name in selected:
        candidates = loaded_candidates[(stream, gate)]
        profile = {**PROFILES[profile_name], "start_equity": 100000.0}
        replay = BrokerReplay(
            profile,
            candles,
            margin_rates=margin_rates,
            live_cfg=live_cfg,
            max_new_positions_per_timestamp=safe_int(live_cfg.get("max_new_positions_per_cycle"), 2),
        )
        out_dir = output_root / f"{stream}_{gate}_{profile_name}"
        summary = replay.run(candidates)
        summary_row = {
            "stream": stream,
            "score_gate": gate,
            "profile": profile_name,
            "objective": objective(summary),
            **summary,
            "guard_target_margin_pct": profile.get("target_margin_pct"),
            "guard_hard_margin_pct": profile.get("hard_margin_pct"),
            "guard_emergency_margin_pct": profile.get("emergency_margin_pct"),
            "guard_target_open_risk_pct": profile.get("target_open_risk_pct"),
            "guard_max_open_risk_pct": profile.get("max_open_risk_pct"),
            "max_new_positions_per_timestamp": safe_int(live_cfg.get("max_new_positions_per_cycle"), 2),
            "output_dir": str(out_dir),
        }
        replay.write_outputs(out_dir, summary_row, candidates)
        rows.append(summary_row)
        print(json.dumps(summary_row, sort_keys=True, default=str), flush=True)
    summary_frame = pd.DataFrame(rows).sort_values("objective", ascending=False)
    summary_frame.to_csv(output_root / "oanda_broker_replay_summary.csv", index=False)
    (output_root / "oanda_broker_replay_summary.json").write_text(
        json.dumps(rows, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    return summary_frame


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--scenario", choices=["top", "all"], default="top")
    args = parser.parse_args()
    summary = run(args.output_root, args.scenario)
    print(summary.to_string(index=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
