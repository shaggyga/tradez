from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
ENGINE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ENGINE_ROOT / "config.json"


MAJOR_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD"}


@dataclass(frozen=True)
class PairMeta:
    instrument: str
    base: str
    quote: str
    pip_size: float
    tier: str
    quote_to_usd: float = 1.0


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    cfg_path = Path(path) if path else DEFAULT_CONFIG
    with cfg_path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg["_config_path"] = str(cfg_path)
    return cfg


def utc_now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def parse_date(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def pair_parts(instrument: str) -> tuple[str, str]:
    base, quote = instrument.split("_", 1)
    return base, quote


def pip_size_for(instrument: str) -> float:
    _, quote = pair_parts(instrument)
    return 0.01 if quote == "JPY" else 0.0001


def tier_for(instrument: str, cfg: dict[str, Any]) -> str:
    if instrument in set(cfg["tier1_pairs"]):
        return "tier1"
    if instrument in set(cfg["tier2_pairs"]):
        return "tier2"
    return "tier3"


def available_m1_pairs(cfg: dict[str, Any]) -> list[str]:
    candles_dir = ROOT / cfg["candles_dir"]
    return sorted(p.name.removesuffix("_M1.csv") for p in candles_dir.glob("*_M1.csv"))


def select_pairs(cfg: dict[str, Any], tier: str, explicit_pairs: list[str] | None = None) -> list[str]:
    available = set(available_m1_pairs(cfg))
    if explicit_pairs:
        pairs = [p for p in explicit_pairs if p in available]
    elif tier == "tier1":
        pairs = [p for p in cfg["tier1_pairs"] if p in available]
    elif tier == "tier2":
        wanted = set(cfg["tier1_pairs"]) | set(cfg["tier2_pairs"])
        pairs = [p for p in sorted(wanted) if p in available]
    elif tier == "all":
        pairs = sorted(available)
    else:
        raise ValueError(f"unsupported tier: {tier}")
    if not pairs:
        raise ValueError("no pairs selected")
    return pairs


def pair_meta(instrument: str, cfg: dict[str, Any]) -> PairMeta:
    base, quote = pair_parts(instrument)
    return PairMeta(
        instrument=instrument,
        base=base,
        quote=quote,
        pip_size=pip_size_for(instrument),
        tier=tier_for(instrument, cfg),
    )


def fallback_spread_pips(meta: PairMeta, cfg: dict[str, Any]) -> float:
    return float(cfg["costs"][f"fallback_spread_pips_{meta.tier}"])


def slippage_pips(meta: PairMeta, cfg: dict[str, Any]) -> float:
    return float(cfg["costs"][f"slippage_pips_{meta.tier}"])


def quote_to_usd_from_prices(instrument: str, row_close: float, close_by_pair: dict[str, float]) -> float:
    _, quote = pair_parts(instrument)
    if quote == "USD":
        return 1.0
    direct = f"{quote}_USD"
    inverse = f"USD_{quote}"
    if direct in close_by_pair and close_by_pair[direct] > 0:
        return float(close_by_pair[direct])
    if inverse in close_by_pair and close_by_pair[inverse] > 0:
        return 1.0 / float(close_by_pair[inverse])
    # Crosses without a contemporaneous conversion get a conservative fallback.
    if quote == "JPY":
        usd_jpy = close_by_pair.get("USD_JPY")
        return 1.0 / float(usd_jpy) if usd_jpy else 0.0068
    return 1.0


def pip_value_usd_per_unit(instrument: str, quote_to_usd: float | pd.Series) -> float | pd.Series:
    return pip_size_for(instrument) * quote_to_usd


def read_candles(
    instrument: str,
    cfg: dict[str, Any],
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
    max_rows: int | None = None,
) -> pd.DataFrame:
    path = ROOT / cfg["candles_dir"] / f"{instrument}_M1.csv"
    if not path.exists():
        raise FileNotFoundError(path)
    usecols = [
        "time",
        "datetime",
        "instrument",
        "granularity",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",
        "spread_pips",
    ]
    df = pd.read_csv(path, usecols=lambda c: c in usecols)
    df["decision_time_utc"] = pd.to_datetime(df["datetime"], utc=True)
    if start is not None:
        df = df[df["decision_time_utc"] >= start]
    if end is not None:
        df = df[df["decision_time_utc"] <= end]
    if max_rows and len(df) > max_rows:
        df = df.tail(max_rows)
    df = df.sort_values("decision_time_utc").drop_duplicates("decision_time_utc")
    df["instrument"] = instrument
    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",
        "spread_pips",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.reset_index(drop=True)


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    t = df["decision_time_utc"]
    hour = t.dt.hour + t.dt.minute / 60.0
    minute = t.dt.minute
    dow = t.dt.dayofweek
    df["hour_sin"] = np.sin(2 * np.pi * hour / 24.0)
    df["hour_cos"] = np.cos(2 * np.pi * hour / 24.0)
    df["minute_sin"] = np.sin(2 * np.pi * minute / 60.0)
    df["minute_cos"] = np.cos(2 * np.pi * minute / 60.0)
    df["weekday_sin"] = np.sin(2 * np.pi * dow / 7.0)
    df["weekday_cos"] = np.cos(2 * np.pi * dow / 7.0)
    df["is_asia"] = ((t.dt.hour >= 0) & (t.dt.hour < 7)).astype(int)
    df["is_london"] = ((t.dt.hour >= 7) & (t.dt.hour < 16)).astype(int)
    df["is_new_york"] = ((t.dt.hour >= 12) & (t.dt.hour < 21)).astype(int)
    df["is_london_ny_overlap"] = ((t.dt.hour >= 12) & (t.dt.hour < 16)).astype(int)
    df["is_london_open"] = ((t.dt.hour == 7) & (t.dt.minute < 60)).astype(int)
    df["is_new_york_open"] = ((t.dt.hour == 12) | (t.dt.hour == 13)).astype(int)
    df["is_rollover_risk"] = ((t.dt.hour == 21) | (t.dt.hour == 22)).astype(int)
    df["is_friday_late"] = ((t.dt.dayofweek == 4) & (t.dt.hour >= 16)).astype(int)
    return df


def safe_div(a: pd.Series | np.ndarray, b: pd.Series | np.ndarray | float) -> pd.Series | np.ndarray:
    return np.asarray(a) / np.where(np.abs(np.asarray(b)) < 1e-12, np.nan, np.asarray(b))


def max_drawdown(equity: list[float]) -> float:
    if not equity:
        return 0.0
    arr = np.asarray(equity, dtype=float)
    peaks = np.maximum.accumulate(arr)
    dd = (arr - peaks) / np.where(peaks == 0, np.nan, peaks)
    return float(np.nanmin(dd) * 100.0)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
