#!/usr/bin/env python3
r"""
Local surface label / execution-economics probe for the fresh M1 intrahour engine.

This is read-only. It does not import or start any OANDA bot and does not enable execution.

Purpose:
- Inspect the real M1 candle data used by the fresh engine.
- Recompute short-horizon TP/SL outcomes with conservative bid/ask-aware execution.
- Quantify whether there is any oracle extractable opportunity after costs.
- Compare against simple non-ML baselines before spending time on another model sweep.

Recommended first run from project root:

  .\data\oanda_training_manager\.research_py313\Scripts\python.exe .\local_surface_label_economics_probe.py --start 2026-07-03 --end 2026-07-07 --pairs EUR_USD GBP_USD USD_JPY

Then upload the generated zip from fresh_m1_intrahour\reports\local_surface_label_probe_<timestamp>.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


DEFAULT_HORIZONS = [1, 2, 3, 5, 8, 13, 21, 30]
DEFAULT_POLICIES = [
    {"name": "tp2_sl3", "tp_pips": 2.0, "sl_pips": 3.0},
    {"name": "tp3_sl5", "tp_pips": 3.0, "sl_pips": 5.0},
    {"name": "tp5_sl8", "tp_pips": 5.0, "sl_pips": 8.0},
    {"name": "tp8_sl12", "tp_pips": 8.0, "sl_pips": 12.0},
]
DEFAULT_TIER1 = [
    "EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF", "USD_CAD", "AUD_USD", "NZD_USD",
    "EUR_JPY", "GBP_JPY", "AUD_JPY", "EUR_GBP", "EUR_AUD", "EUR_CAD", "GBP_AUD", "GBP_CAD",
]
FALLBACK_SPREAD = {"tier1": 1.8, "tier2": 3.5, "tier3": 8.0}
SLIPPAGE = {"tier1": 0.2, "tier2": 0.4, "tier3": 1.0}


@dataclass(frozen=True)
class Policy:
    name: str
    tp_pips: float
    sl_pips: float


@dataclass(frozen=True)
class PairMeta:
    instrument: str
    base: str
    quote: str
    tier: str
    pip_size: float


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True, default=str)


def pip_size_for(instrument: str) -> float:
    quote = instrument.split("_", 1)[1]
    return 0.01 if quote == "JPY" else 0.0001


def pair_meta(instrument: str, tier1: set[str], tier2: set[str] | None = None) -> PairMeta:
    tier2 = tier2 or set()
    base, quote = instrument.split("_", 1)
    tier = "tier1" if instrument in tier1 else ("tier2" if instrument in tier2 else "tier3")
    return PairMeta(instrument, base, quote, tier, pip_size_for(instrument))


def parse_ts(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def load_config(root: Path) -> dict[str, Any]:
    cfg_path = root / "fresh_m1_intrahour" / "config.json"
    if cfg_path.exists():
        cfg = read_json(cfg_path)
    else:
        cfg = {}
    cfg.setdefault("horizons_minutes", DEFAULT_HORIZONS)
    cfg.setdefault("policy_templates", DEFAULT_POLICIES)
    cfg.setdefault("tier1_pairs", DEFAULT_TIER1)
    cfg.setdefault("tier2_pairs", [])
    cfg.setdefault("costs", {})
    for k, v in FALLBACK_SPREAD.items():
        cfg["costs"].setdefault(f"fallback_spread_pips_{k}", v)
    for k, v in SLIPPAGE.items():
        cfg["costs"].setdefault(f"slippage_pips_{k}", v)
    cfg.setdefault("candles_dir", "data/oanda_training_manager/candles")
    return cfg


def resolve_pairs(args: argparse.Namespace, cfg: dict[str, Any], candles_dir: Path) -> list[str]:
    available = sorted(p.name.removesuffix("_M1.csv") for p in candles_dir.glob("*_M1.csv"))
    if not available:
        raise FileNotFoundError(f"No *_M1.csv files found in {candles_dir}")
    if args.tier == "tier1" and not args.pairs:
        wanted = cfg.get("tier1_pairs", DEFAULT_TIER1)
    elif args.tier == "all" and not args.pairs:
        wanted = available
    elif args.pairs:
        wanted = args.pairs
    else:
        wanted = cfg.get("tier1_pairs", DEFAULT_TIER1)
    pairs = [p for p in wanted if p in available]
    missing = sorted(set(wanted) - set(pairs))
    if missing:
        print(f"[warn] missing candle files for: {missing[:20]}{'...' if len(missing) > 20 else ''}")
    if not pairs:
        raise ValueError("No selected pairs have matching candle files.")
    return pairs


def read_candle_file(path: Path, start: pd.Timestamp | None, end: pd.Timestamp | None) -> pd.DataFrame:
    wanted = {
        "time", "datetime", "instrument", "granularity", "open", "high", "low", "close", "volume",
        "bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low", "ask_close", "spread_pips",
    }
    df = pd.read_csv(path, usecols=lambda c: c in wanted)
    time_col = "datetime" if "datetime" in df.columns else "time"
    df["decision_time_utc"] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    df = df.dropna(subset=["decision_time_utc"])
    if start is not None:
        df = df[df["decision_time_utc"] >= start]
    if end is not None:
        df = df[df["decision_time_utc"] <= end]
    df = df.sort_values("decision_time_utc").drop_duplicates("decision_time_utc").reset_index(drop=True)
    for col in [c for c in df.columns if c not in {"time", "datetime", "instrument", "granularity", "decision_time_utc"}]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def enrich_bid_ask(df: pd.DataFrame, meta: PairMeta, cfg: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    df = df.copy()
    pip = meta.pip_size
    fallback_spread = float(cfg["costs"].get(f"fallback_spread_pips_{meta.tier}", FALLBACK_SPREAD.get(meta.tier, 8.0)))
    # Prefer native spread. If absent, derive close spread from ask/bid. If still absent, fallback by tier.
    spread = pd.to_numeric(df.get("spread_pips"), errors="coerce") if "spread_pips" in df.columns else pd.Series(np.nan, index=df.index)
    if "ask_close" in df.columns and "bid_close" in df.columns:
        derived = (df["ask_close"] - df["bid_close"]) / pip
        spread = spread.where(spread.notna(), derived)
    spread = spread.replace([np.inf, -np.inf], np.nan)
    # Rolling median preserves local spread regimes when partial values exist.
    spread = spread.fillna(spread.rolling(240, min_periods=10).median()).fillna(fallback_spread).clip(lower=0.0)
    df["_spread_pips_used"] = spread

    for base_col, mid_col, side in [
        ("bid_open", "open", -0.5), ("bid_high", "high", -0.5), ("bid_low", "low", -0.5), ("bid_close", "close", -0.5),
        ("ask_open", "open", 0.5), ("ask_high", "high", 0.5), ("ask_low", "low", 0.5), ("ask_close", "close", 0.5),
    ]:
        if base_col not in df.columns:
            df[base_col] = np.nan
        native = pd.to_numeric(df[base_col], errors="coerce")
        approx = pd.to_numeric(df[mid_col], errors="coerce") + (side * spread * pip)
        df[f"_{base_col}_used"] = native.where(native.notna(), approx)

    native_bidask_cols = ["bid_high", "bid_low", "bid_close", "ask_high", "ask_low", "ask_close"]
    native_bidask_present = float(df[native_bidask_cols].notna().all(axis=1).mean()) if all(c in df.columns for c in native_bidask_cols) and len(df) else 0.0
    native_spread_present = float(pd.to_numeric(df.get("spread_pips"), errors="coerce").notna().mean()) if "spread_pips" in df.columns and len(df) else 0.0
    health = {
        "pair": meta.instrument,
        "rows": int(len(df)),
        "start": str(df["decision_time_utc"].min()) if len(df) else None,
        "end": str(df["decision_time_utc"].max()) if len(df) else None,
        "native_bidask_complete_rate": native_bidask_present,
        "native_spread_present_rate": native_spread_present,
        "median_spread_pips_used": float(df["_spread_pips_used"].median()) if len(df) else None,
        "p95_spread_pips_used": float(df["_spread_pips_used"].quantile(0.95)) if len(df) else None,
        "fallback_spread_pips": fallback_spread,
    }
    return df, health


def build_quote_to_usd_series(pair: str, df: pd.DataFrame, all_closes: dict[str, pd.Series]) -> pd.Series:
    base, quote = pair.split("_", 1)
    idx = df["decision_time_utc"]
    if quote == "USD":
        return pd.Series(1.0, index=df.index)
    direct = f"{quote}_USD"
    inverse = f"USD_{quote}"
    if direct in all_closes:
        s = all_closes[direct].reindex(idx).ffill().bfill()
        return pd.Series(s.to_numpy(dtype=float), index=df.index)
    if inverse in all_closes:
        s = all_closes[inverse].reindex(idx).ffill().bfill()
        return pd.Series(1.0 / s.to_numpy(dtype=float), index=df.index)
    # Conservative static fallbacks for common FX quotes.
    fallback = {
        "JPY": 0.0068,
        "CHF": 1.12,
        "CAD": 0.73,
        "AUD": 0.66,
        "NZD": 0.61,
        "GBP": 1.28,
        "EUR": 1.08,
    }.get(quote, 1.0)
    return pd.Series(fallback, index=df.index)


def first_hit_exec(
    bid_high: np.ndarray,
    bid_low: np.ndarray,
    bid_close: np.ndarray,
    ask_high: np.ndarray,
    ask_low: np.ndarray,
    ask_close: np.ndarray,
    i: int,
    side: str,
    tp_pips: float,
    sl_pips: float,
    horizon: int,
    pip: float,
    slippage_pips: float,
) -> tuple[str, int, float, float, float, float, int]:
    """Conservative executable-side first touch.

    Long enters at ask_close[i] and exits on bid prices.
    Short enters at bid_close[i] and exits on ask prices.
    If TP and SL touch in the same M1 bar, count adverse first.

    Returns: outcome, minutes, net_pips_after_slippage, mfe_pips, mae_pips, endpoint_pips_after_slippage, ambiguous_flag
    """
    end_i = min(len(bid_close) - 1, i + int(horizon))
    slip = 2.0 * float(slippage_pips)
    mfe = 0.0
    mae = 0.0
    ambiguous = 0
    if side == "long":
        entry = ask_close[i]
        for j in range(i + 1, end_i + 1):
            fav = (bid_high[j] - entry) / pip
            adv = (entry - bid_low[j]) / pip
            mfe = max(mfe, float(fav))
            mae = max(mae, float(adv))
            tp = fav >= tp_pips
            sl = adv >= sl_pips
            if tp and sl:
                ambiguous = 1
                return "ambiguous_adverse_first", j - i, -sl_pips - slip, mfe, mae, ((bid_close[j] - entry) / pip) - slip, ambiguous
            if tp:
                return "tp_before_sl", j - i, tp_pips - slip, mfe, mae, ((bid_close[j] - entry) / pip) - slip, ambiguous
            if sl:
                return "sl_before_tp", j - i, -sl_pips - slip, mfe, mae, ((bid_close[j] - entry) / pip) - slip, ambiguous
        endpoint = ((bid_close[end_i] - entry) / pip) - slip
        return "timeout", end_i - i, float(endpoint), mfe, mae, float(endpoint), ambiguous
    else:
        entry = bid_close[i]
        for j in range(i + 1, end_i + 1):
            fav = (entry - ask_low[j]) / pip
            adv = (ask_high[j] - entry) / pip
            mfe = max(mfe, float(fav))
            mae = max(mae, float(adv))
            tp = fav >= tp_pips
            sl = adv >= sl_pips
            if tp and sl:
                ambiguous = 1
                return "ambiguous_adverse_first", j - i, -sl_pips - slip, mfe, mae, ((entry - ask_close[j]) / pip) - slip, ambiguous
            if tp:
                return "tp_before_sl", j - i, tp_pips - slip, mfe, mae, ((entry - ask_close[j]) / pip) - slip, ambiguous
            if sl:
                return "sl_before_tp", j - i, -sl_pips - slip, mfe, mae, ((entry - ask_close[j]) / pip) - slip, ambiguous
        endpoint = ((entry - ask_close[end_i]) / pip) - slip
        return "timeout", end_i - i, float(endpoint), mfe, mae, float(endpoint), ambiguous


def compute_momentum_features(df: pd.DataFrame, meta: PairMeta) -> pd.DataFrame:
    out = df[["decision_time_utc", "close", "_spread_pips_used"]].copy()
    pip = meta.pip_size
    for n in [1, 3, 5, 15]:
        out[f"return_{n}m_pips"] = (df["close"] - df["close"].shift(n)) / pip
    tr = pd.concat([
        (df["high"] - df["low"]).abs(),
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs(),
    ], axis=1).max(axis=1) / pip
    out["atr_15m_pips"] = tr.rolling(15, min_periods=5).mean()
    out["spread_to_atr_15m"] = out["_spread_pips_used"] / out["atr_15m_pips"].replace(0, np.nan)
    return out


def update_topn(store: dict[pd.Timestamp, list[float]], ts: pd.Timestamp, value: float, n: int = 5) -> None:
    arr = store.setdefault(ts, [])
    arr.append(float(value))
    arr.sort(reverse=True)
    if len(arr) > n:
        del arr[n:]


def summarize_topn(store: dict[pd.Timestamp, list[float]], k: int) -> dict[str, Any]:
    vals = []
    for arr in store.values():
        if arr:
            vals.append(float(np.mean(arr[:k])))
    if not vals:
        return {"timestamps": 0, "mean_ev": None, "median_ev": None, "positive_rate": None, "sum_mean_ev": None}
    s = pd.Series(vals)
    return {
        "timestamps": int(len(vals)),
        "mean_ev": float(s.mean()),
        "median_ev": float(s.median()),
        "positive_rate": float((s > 0).mean()),
        "sum_mean_ev": float(s.sum()),
        "p05_ev": float(s.quantile(0.05)),
        "p95_ev": float(s.quantile(0.95)),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Local M1 label/execution economics probe")
    ap.add_argument("--root", default=".", help="Project root, default current directory")
    ap.add_argument("--start", required=True, help="UTC start date/time, e.g. 2026-07-03")
    ap.add_argument("--end", required=True, help="UTC end date/time, e.g. 2026-07-07")
    ap.add_argument("--pairs", nargs="*", default=None, help="Explicit pair list. Defaults to tier1 unless --tier all.")
    ap.add_argument("--tier", choices=["tier1", "all"], default="tier1")
    ap.add_argument("--candles-dir", default=None, help="Override candle directory")
    ap.add_argument("--output-dir", default=None, help="Override output directory")
    ap.add_argument("--max-rows-per-pair", type=int, default=0, help="Optional tail row cap per pair after date filter")
    ap.add_argument("--max-horizon", type=int, default=30, help="Maximum horizon to evaluate")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    cfg = load_config(root)
    candles_dir = Path(args.candles_dir) if args.candles_dir else (root / cfg.get("candles_dir", "data/oanda_training_manager/candles"))
    if not candles_dir.is_absolute():
        candles_dir = (root / candles_dir).resolve()
    start = parse_ts(args.start)
    end = parse_ts(args.end)
    horizons = [int(h) for h in cfg.get("horizons_minutes", DEFAULT_HORIZONS) if int(h) <= int(args.max_horizon)]
    policies = [Policy(str(p["name"]), float(p["tp_pips"]), float(p["sl_pips"])) for p in cfg.get("policy_templates", DEFAULT_POLICIES)]
    tier1 = set(cfg.get("tier1_pairs", DEFAULT_TIER1))
    tier2 = set(cfg.get("tier2_pairs", []))
    pairs = resolve_pairs(args, cfg, candles_dir)
    stamp = utc_stamp()
    out_dir = Path(args.output_dir) if args.output_dir else root / "fresh_m1_intrahour" / "reports" / f"local_surface_label_probe_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[info] root={root}")
    print(f"[info] candles_dir={candles_dir}")
    print(f"[info] pairs={pairs}")
    print(f"[info] horizons={horizons}, policies={[p.name for p in policies]}")

    # Load and enrich candles first so cross-pair quote conversions can use available close series.
    pair_frames: dict[str, tuple[pd.DataFrame, PairMeta, dict[str, Any]]] = {}
    close_lookup: dict[str, pd.Series] = {}
    data_health = []
    for pair in pairs:
        meta = pair_meta(pair, tier1, tier2)
        path = candles_dir / f"{pair}_M1.csv"
        if not path.exists():
            print(f"[warn] missing {path}")
            continue
        raw = read_candle_file(path, start, end)
        if args.max_rows_per_pair and len(raw) > args.max_rows_per_pair:
            raw = raw.tail(args.max_rows_per_pair).reset_index(drop=True)
        if len(raw) <= max(horizons) + 20:
            print(f"[warn] too few rows for {pair}: {len(raw)}")
            continue
        raw, health = enrich_bid_ask(raw, meta, cfg)
        pair_frames[pair] = (raw, meta, health)
        close_lookup[pair] = pd.Series(raw["close"].to_numpy(dtype=float), index=raw["decision_time_utc"])
        data_health.append(health)

    if not pair_frames:
        raise RuntimeError("No usable candle frames loaded.")

    oracle_topn: dict[pd.Timestamp, list[float]] = {}
    baseline_mom1: dict[pd.Timestamp, tuple[float, float]] = {}  # score, ev
    baseline_mom5: dict[pd.Timestamp, tuple[float, float]] = {}
    baseline_lowcost_mom5: dict[pd.Timestamp, tuple[float, float]] = {}
    summary_acc: dict[tuple[str, int], dict[str, float]] = defaultdict(lambda: defaultdict(float))
    pair_acc: dict[tuple[str, str, int], dict[str, float]] = defaultdict(lambda: defaultdict(float))
    samples: list[dict[str, Any]] = []

    for pair, (df, meta, health) in pair_frames.items():
        print(f"[info] evaluating {pair}: {len(df):,} rows")
        quote_to_usd = build_quote_to_usd_series(pair, df, close_lookup).replace([np.inf, -np.inf], np.nan).ffill().bfill().fillna(1.0)
        pip_value = meta.pip_size * quote_to_usd.to_numpy(dtype=float)
        slippage = float(cfg["costs"].get(f"slippage_pips_{meta.tier}", SLIPPAGE.get(meta.tier, 1.0)))
        feats = compute_momentum_features(df, meta)
        times = df["decision_time_utc"].to_numpy()
        close = df["close"].to_numpy(dtype=float)
        bid_high = df["_bid_high_used"].to_numpy(dtype=float)
        bid_low = df["_bid_low_used"].to_numpy(dtype=float)
        bid_close = df["_bid_close_used"].to_numpy(dtype=float)
        ask_high = df["_ask_high_used"].to_numpy(dtype=float)
        ask_low = df["_ask_low_used"].to_numpy(dtype=float)
        ask_close = df["_ask_close_used"].to_numpy(dtype=float)
        max_h = max(horizons)
        last_i = len(df) - max_h - 1
        if last_i <= 0:
            continue
        for side in ["long", "short"]:
            sign = 1.0 if side == "long" else -1.0
            side_mom1 = feats["return_1m_pips"].to_numpy(dtype=float) * sign
            side_mom5 = feats["return_5m_pips"].to_numpy(dtype=float) * sign
            spread_to_atr = feats["spread_to_atr_15m"].to_numpy(dtype=float)
            for policy in policies:
                for h in horizons:
                    key = (policy.name, h)
                    pkey = (pair, policy.name, h)
                    for i in range(30, last_i):  # skip warmup for features
                        if not np.isfinite(close[i]) or not np.isfinite(pip_value[i]):
                            continue
                        outcome, tt, net_pips, mfe, mae, endpoint, ambiguous = first_hit_exec(
                            bid_high, bid_low, bid_close, ask_high, ask_low, ask_close,
                            i, side, policy.tp_pips, policy.sl_pips, h, meta.pip_size, slippage,
                        )
                        ev = float(net_pips * pip_value[i] * 1000.0)
                        ts = pd.Timestamp(times[i])
                        update_topn(oracle_topn, ts, ev, 5)
                        acc = summary_acc[key]
                        acc["rows"] += 1
                        acc["ev_sum"] += ev
                        acc["net_pips_sum"] += float(net_pips)
                        acc["positive"] += 1 if ev > 0 else 0
                        acc["tp"] += 1 if outcome == "tp_before_sl" else 0
                        acc["sl"] += 1 if outcome == "sl_before_tp" or outcome == "ambiguous_adverse_first" else 0
                        acc["timeout"] += 1 if outcome == "timeout" else 0
                        acc["ambiguous"] += ambiguous
                        acc["mfe_sum"] += float(mfe)
                        acc["mae_sum"] += float(mae)
                        acc["duration_sum"] += float(tt)
                        pacc = pair_acc[pkey]
                        pacc["rows"] += 1
                        pacc["ev_sum"] += ev
                        pacc["positive"] += 1 if ev > 0 else 0
                        # Baseline candidates: choose one contract/side at each timestamp by simple known-time signal.
                        # Use all contracts to give the baseline its own best known-time contract after the same signal rank.
                        if np.isfinite(side_mom1[i]):
                            old = baseline_mom1.get(ts)
                            score = float(side_mom1[i])
                            if old is None or score > old[0]:
                                baseline_mom1[ts] = (score, ev)
                        if np.isfinite(side_mom5[i]):
                            old = baseline_mom5.get(ts)
                            score = float(side_mom5[i])
                            if old is None or score > old[0]:
                                baseline_mom5[ts] = (score, ev)
                            old2 = baseline_lowcost_mom5.get(ts)
                            cost_penalized_score = float(side_mom5[i] - 2.0 * np.nan_to_num(spread_to_atr[i], nan=10.0))
                            if old2 is None or cost_penalized_score > old2[0]:
                                baseline_lowcost_mom5[ts] = (cost_penalized_score, ev)
                        if len(samples) < 500 and (ev > 0 or ambiguous):
                            samples.append({
                                "timestamp": str(ts), "pair": pair, "side": side, "policy": policy.name, "horizon_min": h,
                                "outcome": outcome, "time_to_exit": tt, "net_pips_after_slippage": net_pips,
                                "ev_usd_per_1k_units": ev, "mfe_pips": mfe, "mae_pips": mae,
                                "spread_pips_used": float(df["_spread_pips_used"].iloc[i]),
                                "spread_to_atr_15m": float(spread_to_atr[i]) if np.isfinite(spread_to_atr[i]) else None,
                                "side_mom1_pips": float(side_mom1[i]) if np.isfinite(side_mom1[i]) else None,
                                "side_mom5_pips": float(side_mom5[i]) if np.isfinite(side_mom5[i]) else None,
                                "ambiguous_same_bar": int(ambiguous),
                            })

    def acc_rows(acc_map: dict[tuple[Any, ...], dict[str, float]], cols: list[str]) -> list[dict[str, Any]]:
        rows = []
        for key, acc in sorted(acc_map.items()):
            n = max(float(acc.get("rows", 0.0)), 1.0)
            row = {col: val for col, val in zip(cols, key)}
            row.update({
                "rows": int(acc.get("rows", 0.0)),
                "mean_ev_usd_per_1k_units": float(acc.get("ev_sum", 0.0) / n),
                "sum_ev_usd_per_1k_units": float(acc.get("ev_sum", 0.0)),
                "mean_net_pips": float(acc.get("net_pips_sum", 0.0) / n) if "net_pips_sum" in acc else None,
                "positive_rate": float(acc.get("positive", 0.0) / n),
                "tp_rate": float(acc.get("tp", 0.0) / n) if "tp" in acc else None,
                "sl_rate": float(acc.get("sl", 0.0) / n) if "sl" in acc else None,
                "timeout_rate": float(acc.get("timeout", 0.0) / n) if "timeout" in acc else None,
                "ambiguous_rate": float(acc.get("ambiguous", 0.0) / n) if "ambiguous" in acc else None,
                "mean_mfe_pips": float(acc.get("mfe_sum", 0.0) / n) if "mfe_sum" in acc else None,
                "mean_mae_pips": float(acc.get("mae_sum", 0.0) / n) if "mae_sum" in acc else None,
                "mean_duration_min": float(acc.get("duration_sum", 0.0) / n) if "duration_sum" in acc else None,
            })
            rows.append(row)
        return rows

    policy_horizon_rows = acc_rows(summary_acc, ["policy_name", "horizon_min"])
    pair_policy_horizon_rows = acc_rows(pair_acc, ["pair", "policy_name", "horizon_min"])

    def baseline_summary(store: dict[pd.Timestamp, tuple[float, float]]) -> dict[str, Any]:
        vals = [ev for _, ev in store.values()]
        if not vals:
            return {"timestamps": 0}
        s = pd.Series(vals)
        return {
            "timestamps": int(len(vals)),
            "mean_ev": float(s.mean()),
            "median_ev": float(s.median()),
            "sum_ev": float(s.sum()),
            "positive_rate": float((s > 0).mean()),
            "p05_ev": float(s.quantile(0.05)),
            "p95_ev": float(s.quantile(0.95)),
        }

    oracle_report = {
        "top1": summarize_topn(oracle_topn, 1),
        "top3_mean": summarize_topn(oracle_topn, 3),
        "top5_mean": summarize_topn(oracle_topn, 5),
        "note": "Oracle uses realized future P/L and is diagnostic only; never use it for model selection or live allocation.",
    }
    baseline_report = {
        "top_side_momentum_1m": baseline_summary(baseline_mom1),
        "top_side_momentum_5m": baseline_summary(baseline_mom5),
        "top_side_momentum_5m_minus_cost_pressure": baseline_summary(baseline_lowcost_mom5),
    }
    data_health_df = pd.DataFrame(data_health)
    policy_df = pd.DataFrame(policy_horizon_rows)
    pair_df = pd.DataFrame(pair_policy_horizon_rows)
    sample_df = pd.DataFrame(samples)

    data_health_df.to_csv(out_dir / "data_health.csv", index=False)
    policy_df.to_csv(out_dir / "policy_horizon_economics.csv", index=False)
    pair_df.to_csv(out_dir / "pair_policy_horizon_economics.csv", index=False)
    sample_df.to_csv(out_dir / "positive_or_ambiguous_samples.csv", index=False)
    write_json(out_dir / "oracle_opportunity_report.json", oracle_report)
    write_json(out_dir / "baseline_comparison_report.json", baseline_report)

    # Compact markdown summary.
    best_policy = None
    if not policy_df.empty:
        best_policy = policy_df.sort_values("mean_ev_usd_per_1k_units", ascending=False).head(10).to_dict("records")
    summary = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "root": str(root),
        "candles_dir": str(candles_dir),
        "pairs": list(pair_frames.keys()),
        "start": str(start),
        "end": str(end),
        "horizons": horizons,
        "policies": [p.__dict__ for p in policies],
        "data_health": data_health,
        "oracle_opportunity": oracle_report,
        "baselines": baseline_report,
        "top_policy_horizon_rows": best_policy,
        "live_execution_changed": False,
        "labeling": {
            "execution_model": "bid/ask-aware where native columns exist; mid+spread approximation otherwise",
            "entry": "long uses ask_close; short uses bid_close",
            "exit": "long exits on bid prices; short exits on ask prices",
            "same_bar_tp_sl_rule": "adverse-first",
            "slippage": "subtracts round-trip slippage from all outcomes",
        },
    }
    write_json(out_dir / "LOCAL_SURFACE_LABEL_PROBE.json", summary)
    md = [
        "# Local Surface Label / Economics Probe",
        "",
        f"Generated UTC: `{summary['generated_utc']}`",
        f"Root: `{root}`",
        f"Candles: `{candles_dir}`",
        f"Pairs: `{', '.join(pair_frames.keys())}`",
        f"Date range: `{start}` to `{end}`",
        "",
        "## Data health",
        data_health_df.to_markdown(index=False) if not data_health_df.empty else "No data health rows.",
        "",
        "## Oracle opportunity after costs",
        "Diagnostic only; this is hindsight and must not be used as a trading selector.",
        "```json",
        json.dumps(oracle_report, indent=2),
        "```",
        "",
        "## Simple baseline comparison",
        "```json",
        json.dumps(baseline_report, indent=2),
        "```",
        "",
        "## Top policy/horizon economics",
        policy_df.sort_values("mean_ev_usd_per_1k_units", ascending=False).head(20).to_markdown(index=False) if not policy_df.empty else "No policy rows.",
        "",
        "## Notes",
        "- Read-only script; live execution was not touched.",
        "- Labels use bid/ask-aware execution when native bid/ask columns exist; otherwise they approximate from mid + spread.",
        "- Same-bar TP/SL ambiguity is treated adverse-first.",
        "- Upload the zip from this folder back to chat for analysis.",
    ]
    md_path = out_dir / "LOCAL_SURFACE_LABEL_PROBE.md"
    md_path.write_text("\n".join(md), encoding="utf-8")

    zip_path = out_dir / f"local_surface_label_probe_{stamp}.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in [
            out_dir / "LOCAL_SURFACE_LABEL_PROBE.md",
            out_dir / "LOCAL_SURFACE_LABEL_PROBE.json",
            out_dir / "data_health.csv",
            out_dir / "policy_horizon_economics.csv",
            out_dir / "pair_policy_horizon_economics.csv",
            out_dir / "positive_or_ambiguous_samples.csv",
            out_dir / "oracle_opportunity_report.json",
            out_dir / "baseline_comparison_report.json",
        ]:
            if p.exists():
                z.write(p, arcname=p.name)

    print("\n[done] Wrote:")
    print(f"  {md_path}")
    print(f"  {out_dir / 'LOCAL_SURFACE_LABEL_PROBE.json'}")
    print(f"  {zip_path}")
    print("\nQuick read:")
    print(json.dumps({"oracle": oracle_report, "baselines": baseline_report}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
