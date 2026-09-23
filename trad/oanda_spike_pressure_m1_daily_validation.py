#!/usr/bin/env python3
"""M1-candle validation for the live pre-spike pressure gate.

This is the closer historical analogue to the live pressure scanner:
it reconstructs the m3/m5/m10/m15 pressure score from M1 candle CSVs, then
labels 60-minute same-direction outcomes and applies the score/value/cluster
gate. Research-only; no account or process mutation.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import math
import os
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Iterator
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from oanda_spike_cluster_quick_test import ACCOUNT_NAV_USD, build_value_table
from oanda_spike_pressure_daily_validation import (
    DEFAULT_DATASET,
    ROOT,
    VOLATILE_SCOUT_PAIRS,
    event_threshold_pips,
    markdown_table,
    safe_float,
)


NY = ZoneInfo("America/New_York")
REPORT_ROOT = ROOT / "data" / "technical_scout_manager" / "reports"
CANDLE_ROOT = ROOT / "data" / "oanda_training_manager" / "candles"
RUN_LOCK_STALE_HOURS = 12.0


def clamp(values: pd.Series, lo: float, hi: float) -> pd.Series:
    return values.clip(lower=lo, upper=hi)


def parse_day(value: str) -> date:
    return date.fromisoformat(value)


def ny_day_bounds(days: list[date], lookback_minutes: int = 120, forward_minutes: int = 90) -> tuple[pd.Timestamp, pd.Timestamp]:
    start_day = min(days)
    end_day = max(days)
    start = datetime.combine(start_day, time.min, NY).astimezone(timezone.utc) - pd.Timedelta(minutes=lookback_minutes)
    end = datetime.combine(end_day, time.max, NY).astimezone(timezone.utc) + pd.Timedelta(minutes=forward_minutes)
    return pd.Timestamp(start), pd.Timestamp(end)


def available_candle_instruments() -> list[str]:
    return sorted(p.name.removesuffix("_M1.csv") for p in CANDLE_ROOT.glob("*_M1.csv"))


def infer_recent_days(count: int, instruments: list[str]) -> list[date]:
    # Use the first available target file to infer the latest rows. This avoids
    # scanning every large CSV just to choose dates.
    for inst in instruments:
        path = CANDLE_ROOT / f"{inst}_M1.csv"
        if path.exists():
            df = pd.read_csv(path, usecols=["datetime"], parse_dates=["datetime"])
            df["date_ny"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(NY).dt.date
            by_day = df.groupby("date_ny", observed=True).size()
            by_day = by_day[by_day >= 120]
            return list(by_day.sort_index().tail(count).index)
    return []


def spread_estimates(instruments: list[str]) -> dict[str, float]:
    """Historical spread estimate from feature cache; p75 is safer than median."""
    try:
        df = pd.read_parquet(DEFAULT_DATASET, columns=["instrument", "spread_pips"])
        df = df[df["instrument"].astype(str).isin(set(instruments))].copy()
        df["spread_pips"] = pd.to_numeric(df["spread_pips"], errors="coerce")
        df = df[df["spread_pips"].notna() & (df["spread_pips"] > 0)]
        if df.empty:
            return {}
        return df.groupby("instrument", observed=True)["spread_pips"].quantile(0.75).to_dict()
    except Exception:
        return {}


def load_pair_candles(inst: str, start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> pd.DataFrame:
    path = CANDLE_ROOT / f"{inst}_M1.csv"
    if not path.exists():
        return pd.DataFrame()
    usecols = ["datetime", "instrument", "open", "high", "low", "close", "volume"]
    df = pd.read_csv(path, usecols=usecols, parse_dates=["datetime"])
    df["time_utc"] = pd.to_datetime(df["datetime"], utc=True)
    df = df[(df["time_utc"] >= start_utc) & (df["time_utc"] <= end_utc)].copy()
    if df.empty:
        return df
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["close", "high", "low"]).sort_values("time_utc")
    df["instrument"] = inst
    return df.reset_index(drop=True)


def add_pair_pressure_and_outcomes(df: pd.DataFrame, pip_size: float, spread_pips: float) -> pd.DataFrame:
    out = df.copy()
    close = out["close"].astype(float)
    high = out["high"].astype(float)
    low = out["low"].astype(float)
    pip = max(float(pip_size), 1e-12)
    spread = max(float(spread_pips), 0.1)

    def move(bars: int) -> pd.Series:
        return (close - close.shift(bars)) / pip

    m3 = move(3)
    m5 = move(5)
    m8 = move(8)
    m10 = move(10)
    m15 = move(15)
    weighted = m3 * 0.40 + m5 * 0.30 + m10 * 0.20 + m15 * 0.10
    abs_weighted = weighted.abs()

    range_pips = (high - low).abs() / pip
    recent_range = range_pips.rolling(5, min_periods=5).mean()
    prior_range = range_pips.shift(10).rolling(25, min_periods=20).mean()
    longer_range = range_pips.shift(35).rolling(45, min_periods=30).mean()
    compression_ratio = prior_range / longer_range.clip(lower=0.1)
    expansion_ratio = recent_range / prior_range.clip(lower=0.1)
    accel = (m3.abs() - (m8 - m5).abs()).clip(lower=0.0)
    threshold = event_threshold_pips(str(out["instrument"].iloc[0]))
    ratio = abs_weighted / spread

    move_score = clamp(abs_weighted / max(threshold, 1.0) * 45.0, 0.0, 45.0)
    accel_score = clamp(accel / max(threshold * 0.25, 0.5) * 20.0, 0.0, 20.0)
    compression_score = clamp((1.10 - compression_ratio) * 30.0, 0.0, 15.0)
    expansion_score = clamp((expansion_ratio - 1.0) * 14.0, 0.0, 15.0)
    spread_score = clamp((ratio - 0.50) * 12.0, 0.0, 10.0)
    score = clamp(move_score + accel_score + compression_score + expansion_score + spread_score, 0.0, 100.0)

    future_close = close.shift(-60)
    long_fixed = (future_close - close) / pip - spread
    short_fixed = (close - future_close) / pip - spread
    high_next60 = high.shift(-1).iloc[::-1].rolling(60, min_periods=10).max().iloc[::-1]
    low_next60 = low.shift(-1).iloc[::-1].rolling(60, min_periods=10).min().iloc[::-1]
    long_best = (high_next60 - close) / pip - spread
    short_best = (close - low_next60) / pip - spread

    out["pressure_direction"] = np.where(weighted >= 0.0, "LONG", "SHORT")
    out["pressure_score"] = score
    out["pressure_signal_pips"] = abs_weighted
    out["pressure_mid_move_pips"] = weighted
    out["pressure_signal_ratio"] = ratio
    out["pressure_spread_pips"] = spread
    out["pressure_threshold_pips"] = threshold
    out["long_fixed_net_pips_60"] = long_fixed
    out["short_fixed_net_pips_60"] = short_fixed
    out["long_best_net_pips_60"] = long_best
    out["short_best_net_pips_60"] = short_best
    return out


def build_episodes(pressure: pd.DataFrame, cooldown_minutes: int) -> pd.DataFrame:
    if pressure.empty:
        return pressure
    cluster = (
        pressure.groupby("bucket30", observed=True)
        .agg(
            pre_rows=("instrument", "size"),
            pre_instruments=("instrument", "nunique"),
            pre_pairs_dirs=("pressure_direction", "size"),
        )
        .reset_index()
    )
    direction_cluster = (
        pressure.groupby(["bucket30", "pressure_direction"], observed=True)
        .agg(pre_direction_instruments=("instrument", "nunique"))
        .reset_index()
    )
    pressure = pressure.merge(cluster, on="bucket30", how="left")
    pressure = pressure.merge(direction_cluster, on=["bucket30", "pressure_direction"], how="left")
    pressure = pressure.sort_values(["instrument", "pressure_direction", "time_utc"]).reset_index(drop=True)
    gap = pd.Timedelta(minutes=max(1, cooldown_minutes))
    episode_no: dict[tuple[str, str], int] = {}
    last_key: tuple[str, str] | None = None
    last_time: pd.Timestamp | None = None
    ids: list[str] = []
    for row in pressure.itertuples(index=False):
        key = (str(row.instrument), str(row.pressure_direction))
        if key != last_key or last_time is None or row.time_utc - last_time > gap:
            episode_no[key] = episode_no.get(key, -1) + 1
        ids.append(f"{key[0]}:{key[1]}:{episode_no[key]}")
        last_key = key
        last_time = row.time_utc
    pressure["episode_id"] = ids
    return (
        pressure.groupby("episode_id", observed=True)
        .agg(
            first_time=("time_utc", "min"),
            last_time=("time_utc", "max"),
            instrument=("instrument", "first"),
            direction=("pressure_direction", "first"),
            rows=("instrument", "size"),
            max_score=("pressure_score", "max"),
            first_score=("pressure_score", "first"),
            max_signal_pips=("pressure_signal_pips", "max"),
            max_signal_ratio=("pressure_signal_ratio", "max"),
            max_signal_usd=("pressure_signal_usd", "max"),
            first_signal_usd=("pressure_signal_usd", "first"),
            bucket30=("bucket30", "first"),
            pre_rows=("pre_rows", "max"),
            pre_instruments=("pre_instruments", "max"),
            pre_pairs_dirs=("pre_pairs_dirs", "max"),
            pre_direction_instruments=("pre_direction_instruments", "max"),
            first_fixed_pips_60m=("future_fixed_pips_60", "first"),
            first_fixed_usd_60m=("future_fixed_usd_60", "first"),
            best_fixed_pips_60m=("future_fixed_pips_60", "max"),
            best_fixed_usd_60m=("future_fixed_usd_60", "max"),
            best_favorable_pips_60m=("future_best_pips_60", "max"),
            best_favorable_usd_60m=("future_best_usd_60", "max"),
            tp_value_major_fixed_60m=("tp_value_major_fixed_60m", "max"),
            tp_value_major_favorable_60m=("tp_value_major_favorable_60m", "max"),
            tp_raw_major_fixed_60m=("tp_raw_major_fixed_60m", "max"),
            tp_raw_major_favorable_60m=("tp_raw_major_favorable_60m", "max"),
        )
        .reset_index()
    )


def selection_summary(episodes: pd.DataFrame, mask: pd.Series, label: str) -> dict[str, Any]:
    sel = episodes[mask].copy()
    if sel.empty:
        return {
            "label": label,
            "episodes": 0,
            "tp_value_fixed": 0,
            "fp_value_fixed": 0,
            "precision_value_fixed": 0.0,
            "tp_value_favorable": 0,
            "fp_value_favorable": 0,
            "precision_value_favorable": 0.0,
            "sum_first_fixed_usd_60m": 0.0,
            "sum_best_fixed_usd_60m": 0.0,
            "sum_best_favorable_usd_60m": 0.0,
            "win_rate_first_fixed": 0.0,
        }
    fixed = sel["tp_value_major_fixed_60m"].astype(bool)
    fav = sel["tp_value_major_favorable_60m"].astype(bool)
    first_fixed_usd = pd.to_numeric(sel["first_fixed_usd_60m"], errors="coerce").fillna(0.0)
    best_fixed_usd = pd.to_numeric(sel["best_fixed_usd_60m"], errors="coerce").fillna(0.0)
    best_fav_usd = pd.to_numeric(sel["best_favorable_usd_60m"], errors="coerce").fillna(0.0)
    return {
        "label": label,
        "episodes": int(len(sel)),
        "tp_value_fixed": int(fixed.sum()),
        "fp_value_fixed": int((~fixed).sum()),
        "precision_value_fixed": float(fixed.mean()),
        "tp_value_favorable": int(fav.sum()),
        "fp_value_favorable": int((~fav).sum()),
        "precision_value_favorable": float(fav.mean()),
        "sum_first_fixed_usd_60m": float(first_fixed_usd.sum()),
        "sum_best_fixed_usd_60m": float(best_fixed_usd.sum()),
        "sum_best_favorable_usd_60m": float(best_fav_usd.sum()),
        "win_rate_first_fixed": float((first_fixed_usd > 0.0).mean()),
    }


def label_number(value: float | int) -> str:
    text = f"{float(value):g}"
    return text.replace(".", "p").replace("-", "m")


def evaluate(
    *,
    days: list[date],
    instruments: list[str],
    watch_score: float,
    gate_score: float,
    gate_ratio: float,
    gate_signal_usd: float,
    gate_cluster_instruments: int,
    gate_direction_cluster_instruments: int,
    cooldown_minutes: int,
) -> tuple[dict[str, Any], pd.DataFrame]:
    start_utc, end_utc = ny_day_bounds(days)
    value_table, value_source = build_value_table(instruments)
    value_map = value_table.set_index("instrument").to_dict("index") if not value_table.empty else {}
    spread_map = spread_estimates(instruments)
    wanted_days = {d.isoformat() for d in days}
    pressure_frames: list[pd.DataFrame] = []
    instrument_stats = []
    selection_labels = {
        "all_pressure": "all_pressure_watch_score",
        "loose": "looser_live_like_score78_ratio0p8_value",
        "no_cluster": (
            f"score{label_number(gate_score)}_ratio{label_number(gate_ratio)}"
            f"_value{label_number(gate_signal_usd)}_no_cluster"
        ),
        "gate": (
            f"score{label_number(gate_score)}_ratio{label_number(gate_ratio)}"
            f"_value{label_number(gate_signal_usd)}_cluster{gate_cluster_instruments}"
            + (
                f"_dircluster{gate_direction_cluster_instruments}"
                if gate_direction_cluster_instruments > 0
                else ""
            )
        ),
    }

    for inst in instruments:
        info = value_map.get(inst, {})
        pip_size = safe_float(info.get("pip_size"), 0.01 if inst.endswith("_JPY") else 0.0001)
        usd_per_pip = safe_float(info.get("usd_per_pip_at_budget"), 0.0)
        spread_pips = safe_float(spread_map.get(inst), 1.0)
        candles = load_pair_candles(inst, start_utc, end_utc)
        if candles.empty or len(candles) < 120:
            instrument_stats.append({"instrument": inst, "rows": int(len(candles)), "pressure_rows": 0})
            continue
        enriched = add_pair_pressure_and_outcomes(candles, pip_size, spread_pips)
        enriched["date_ny"] = enriched["time_utc"].dt.tz_convert(NY).dt.date.astype(str)
        enriched = enriched[enriched["date_ny"].isin(wanted_days)].copy()
        enriched["usd_per_pip_at_budget"] = usd_per_pip
        enriched["pressure_signal_usd"] = enriched["pressure_signal_pips"] * usd_per_pip
        future_fixed = np.where(
            enriched["pressure_direction"].eq("LONG"),
            enriched["long_fixed_net_pips_60"],
            enriched["short_fixed_net_pips_60"],
        )
        future_best = np.where(
            enriched["pressure_direction"].eq("LONG"),
            enriched["long_best_net_pips_60"],
            enriched["short_best_net_pips_60"],
        )
        enriched["future_fixed_pips_60"] = future_fixed
        enriched["future_fixed_usd_60"] = enriched["future_fixed_pips_60"] * usd_per_pip
        enriched["future_best_pips_60"] = future_best
        enriched["future_best_usd_60"] = enriched["future_best_pips_60"] * usd_per_pip
        enriched["tp_value_major_fixed_60m"] = enriched["future_fixed_usd_60"] >= 0.05
        enriched["tp_value_major_favorable_60m"] = enriched["future_best_usd_60"] >= 0.05
        enriched["tp_raw_major_fixed_60m"] = enriched["future_fixed_pips_60"] >= 50.0
        enriched["tp_raw_major_favorable_60m"] = enriched["future_best_pips_60"] >= 50.0
        enriched["bucket30"] = enriched["time_utc"].dt.floor("30min")
        pressure = enriched[enriched["pressure_score"] >= watch_score].copy()
        pressure_frames.append(
            pressure[
                [
                    "time_utc",
                    "date_ny",
                    "instrument",
                    "pressure_direction",
                    "pressure_score",
                    "pressure_signal_pips",
                    "pressure_signal_ratio",
                    "pressure_signal_usd",
                    "bucket30",
                    "future_fixed_pips_60",
                    "future_fixed_usd_60",
                    "future_best_pips_60",
                    "future_best_usd_60",
                    "tp_value_major_fixed_60m",
                    "tp_value_major_favorable_60m",
                    "tp_raw_major_fixed_60m",
                    "tp_raw_major_favorable_60m",
                ]
            ]
        )
        instrument_stats.append({"instrument": inst, "rows": int(len(enriched)), "pressure_rows": int(len(pressure))})

    pressure_all = pd.concat(pressure_frames, ignore_index=True) if pressure_frames else pd.DataFrame()
    day_summaries: list[dict[str, Any]] = []
    episode_frames: list[pd.DataFrame] = []
    for d in sorted(wanted_days):
        day_pressure = pressure_all[pressure_all["date_ny"].eq(d)].copy() if not pressure_all.empty else pd.DataFrame()
        episodes = build_episodes(day_pressure, cooldown_minutes)
        if not episodes.empty:
            episodes["date_ny"] = d
            episode_frames.append(episodes)
            gate = (
                (pd.to_numeric(episodes["max_score"], errors="coerce").fillna(0.0) >= gate_score)
                & (pd.to_numeric(episodes["max_signal_ratio"], errors="coerce").fillna(0.0) >= gate_ratio)
                & (pd.to_numeric(episodes["max_signal_usd"], errors="coerce").fillna(0.0) >= gate_signal_usd)
                & (pd.to_numeric(episodes["pre_instruments"], errors="coerce").fillna(0.0) >= gate_cluster_instruments)
            )
            if gate_direction_cluster_instruments > 0:
                gate = gate & (
                    pd.to_numeric(episodes["pre_direction_instruments"], errors="coerce").fillna(0.0)
                    >= gate_direction_cluster_instruments
                )
            no_cluster = (
                (pd.to_numeric(episodes["max_score"], errors="coerce").fillna(0.0) >= gate_score)
                & (pd.to_numeric(episodes["max_signal_ratio"], errors="coerce").fillna(0.0) >= gate_ratio)
                & (pd.to_numeric(episodes["max_signal_usd"], errors="coerce").fillna(0.0) >= gate_signal_usd)
            )
            loose = (
                (pd.to_numeric(episodes["max_score"], errors="coerce").fillna(0.0) >= 78.0)
                & (pd.to_numeric(episodes["max_signal_ratio"], errors="coerce").fillna(0.0) >= 0.80)
                & (pd.to_numeric(episodes["max_signal_usd"], errors="coerce").fillna(0.0) >= gate_signal_usd)
            )
            all_mask = pd.Series(True, index=episodes.index)
            selections = [
                selection_summary(episodes, all_mask, selection_labels["all_pressure"]),
                selection_summary(episodes, loose, selection_labels["loose"]),
                selection_summary(episodes, no_cluster, selection_labels["no_cluster"]),
                selection_summary(episodes, gate, selection_labels["gate"]),
            ]
        else:
            selections = [
                selection_summary(pd.DataFrame(), pd.Series(dtype=bool), selection_labels["all_pressure"]),
                selection_summary(pd.DataFrame(), pd.Series(dtype=bool), selection_labels["loose"]),
                selection_summary(pd.DataFrame(), pd.Series(dtype=bool), selection_labels["no_cluster"]),
                selection_summary(pd.DataFrame(), pd.Series(dtype=bool), selection_labels["gate"]),
            ]
        day_summaries.append(
            {
                "date_ny": d,
                "pressure_rows": int(len(day_pressure)),
                "pressure_episodes": int(len(episodes)) if not episodes.empty else 0,
                "instruments_with_pressure": int(day_pressure["instrument"].nunique()) if not day_pressure.empty else 0,
                "selections": selections,
            }
        )

    all_episodes = pd.concat(episode_frames, ignore_index=True) if episode_frames else pd.DataFrame()
    report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "days_requested": sorted(wanted_days),
        "instruments": instruments,
        "instrument_count": len(instruments),
        "value_source": value_source,
        "spread_source": "feature_cache_p75_spread_pips",
        "gates": {
            "watch_score": watch_score,
            "gate_score": gate_score,
            "gate_ratio": gate_ratio,
            "gate_signal_usd": gate_signal_usd,
            "gate_cluster_instruments": gate_cluster_instruments,
            "gate_direction_cluster_instruments": gate_direction_cluster_instruments,
            "cooldown_minutes": cooldown_minutes,
            "account_nav_usd": ACCOUNT_NAV_USD,
            "margin_fraction": 0.70,
        },
        "selection_labels": selection_labels,
        "assumptions": {
            "score": "direct M1 reconstruction of live technical_pressure_signal m3/m5/m10/m15 formula",
            "spread": "historical feature-cache p75 spread per pair; candle CSVs do not contain spread values",
            "fixed_outcome": "entry at signal close and exit at close +60m, less estimated spread",
            "favorable_outcome": "best favorable high/low in next 60m, less estimated spread; measures catchability, not full execution P/L",
            "scope": "research only; no live account mutation",
        },
        "instrument_stats": instrument_stats,
        "days": day_summaries,
        "aggregate": aggregate(day_summaries),
    }
    return report, all_episodes


def aggregate(days: list[dict[str, Any]]) -> dict[str, Any]:
    labels = []
    for day in days:
        for selection in day.get("selections", []):
            label = selection.get("label")
            if label and label not in labels:
                labels.append(label)
    out: dict[str, Any] = {"days": len(days)}
    for label in labels:
        rows = []
        for day in days:
            rows.extend([s for s in day.get("selections", []) if s.get("label") == label])
        episodes = sum(int(r.get("episodes", 0)) for r in rows)
        tp_fixed = sum(int(r.get("tp_value_fixed", 0)) for r in rows)
        fp_fixed = sum(int(r.get("fp_value_fixed", 0)) for r in rows)
        tp_fav = sum(int(r.get("tp_value_favorable", 0)) for r in rows)
        fp_fav = sum(int(r.get("fp_value_favorable", 0)) for r in rows)
        out[label] = {
            "episodes": episodes,
            "precision_value_fixed": float(tp_fixed / max(1, tp_fixed + fp_fixed)),
            "precision_value_favorable": float(tp_fav / max(1, tp_fav + fp_fav)),
            "tp_value_fixed": tp_fixed,
            "fp_value_fixed": fp_fixed,
            "tp_value_favorable": tp_fav,
            "fp_value_favorable": fp_fav,
            "sum_first_fixed_usd_60m": float(sum(safe_float(r.get("sum_first_fixed_usd_60m")) for r in rows)),
            "sum_best_fixed_usd_60m": float(sum(safe_float(r.get("sum_best_fixed_usd_60m")) for r in rows)),
            "sum_best_favorable_usd_60m": float(sum(safe_float(r.get("sum_best_favorable_usd_60m")) for r in rows)),
            "positive_days_first_fixed_usd": int(sum(1 for r in rows if safe_float(r.get("sum_first_fixed_usd_60m")) > 0.0)),
        }
    return out


def write_report(report: dict[str, Any], episodes: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    if not episodes.empty:
        episodes.to_csv(out_dir / "episodes.csv", index=False)
    labels = report.get("selection_labels") if isinstance(report.get("selection_labels"), dict) else {}
    gate_label = labels.get("gate", "")
    loose_label = labels.get("loose", "looser_live_like_score78_ratio0p8_value")
    gates = report.get("gates") if isinstance(report.get("gates"), dict) else {}
    rows = []
    direction_cluster_gate = int(safe_float(gates.get("gate_direction_cluster_instruments"), 0))
    for day in report["days"]:
        sel = {s["label"]: s for s in day.get("selections", [])}
        gate = sel.get(gate_label, {})
        loose = sel.get(loose_label, {})
        rows.append(
            {
                "date": day["date_ny"],
                "pressure_rows": day["pressure_rows"],
                "episodes": day["pressure_episodes"],
                "gate_trades": gate.get("episodes", 0),
                "gate_fixed_precision": gate.get("precision_value_fixed", 0.0),
                "gate_favorable_precision": gate.get("precision_value_favorable", 0.0),
                "gate_first_fixed_usd": gate.get("sum_first_fixed_usd_60m", 0.0),
                "gate_best_favorable_usd": gate.get("sum_best_favorable_usd_60m", 0.0),
                "loose_trades": loose.get("episodes", 0),
                "loose_fixed_precision": loose.get("precision_value_fixed", 0.0),
            }
        )
    md = [
        "# M1 spike pressure daily validation",
        "",
        f"Generated UTC: {report['generated_utc']}",
        f"Instruments: {report['instrument_count']}",
        f"Value source: {report['value_source']}",
        f"Spread source: {report['spread_source']}",
        "",
        (
            "Gate: "
            f"score >= {safe_float(gates.get('gate_score'), 85.0):g}, "
            f"signal/spread >= {safe_float(gates.get('gate_ratio'), 1.0):g}, "
            f"signal value >= ${safe_float(gates.get('gate_signal_usd'), 0.05):g} "
            f"on ${safe_float(gates.get('account_nav_usd'), ACCOUNT_NAV_USD):g} "
            f"@ {safe_float(gates.get('margin_fraction'), 0.70) * 100:g}% margin, "
            f"and >={int(safe_float(gates.get('gate_cluster_instruments'), 20))} "
            "pressure instruments in the 30m bucket."
            + (
                f" Same-direction cluster must also be >={direction_cluster_gate} instruments."
                if direction_cluster_gate > 0
                else ""
            )
        ),
        f"Strict gate label: `{gate_label}`.",
        "",
        "## Day summary",
        "",
        markdown_table(
            rows,
            [
                "date",
                "pressure_rows",
                "episodes",
                "gate_trades",
                "gate_fixed_precision",
                "gate_favorable_precision",
                "gate_first_fixed_usd",
                "gate_best_favorable_usd",
                "loose_trades",
                "loose_fixed_precision",
            ],
        ),
        "",
        "## Aggregate",
        "",
        json.dumps(report["aggregate"], indent=2, default=str),
        "",
        "Notes:",
        "- Fixed outcome is stricter: exit exactly 60m later.",
        "- Favorable outcome is catchability: best favorable high/low during the next 60m.",
        "- Candle CSVs do not include historical spread, so spread is estimated with p75 feature-cache spread by pair.",
    ]
    text = "\n".join(md)
    (out_dir / "summary.md").write_text(text, encoding="utf-8")
    (REPORT_ROOT / "latest_spike_pressure_m1_daily_validation.md").write_text(text, encoding="utf-8")
    (REPORT_ROOT / "latest_spike_pressure_m1_daily_validation.json").write_text(
        json.dumps(report, indent=2, default=str),
        encoding="utf-8",
    )


@contextmanager
def output_run_guard(out_dir: Path) -> Iterator[None]:
    """Prevent duplicate long validations writing the same output directory."""
    out_dir.mkdir(parents=True, exist_ok=True)
    lock_path = out_dir / ".run_lock.json"
    now = datetime.now(timezone.utc)
    if lock_path.exists():
        try:
            existing = json.loads(lock_path.read_text(encoding="utf-8"))
            started = pd.Timestamp(existing.get("started_utc"))
            age_hours = (pd.Timestamp(now) - started).total_seconds() / 3600.0
        except Exception:
            existing = {}
            age_hours = 0.0
        if age_hours < RUN_LOCK_STALE_HOURS and not existing.get("completed_utc"):
            pid = existing.get("pid", "unknown")
            started_text = existing.get("started_utc", "unknown")
            raise SystemExit(
                f"Output directory already has an active pressure-validation lock "
                f"(pid={pid}, started_utc={started_text}). "
                f"Use a different --out-dir or remove {lock_path} after verifying the old run is stopped."
            )

    payload = {
        "pid": os.getpid(),
        "started_utc": now.isoformat(),
        "script": Path(__file__).name,
        "out_dir": str(out_dir),
    }
    lock_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    try:
        yield
        payload["completed_utc"] = datetime.now(timezone.utc).isoformat()
        (out_dir / ".last_run.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    finally:
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", nargs="+", default=[])
    parser.add_argument("--recent-days", type=int, default=8)
    parser.add_argument("--all-candles", action="store_true", help="Use all candle instruments instead of volatile scout list.")
    parser.add_argument("--watch-score", type=float, default=62.0)
    parser.add_argument("--gate-score", type=float, default=85.0)
    parser.add_argument("--gate-ratio", type=float, default=1.0)
    parser.add_argument("--gate-signal-usd", type=float, default=0.05)
    parser.add_argument("--gate-cluster-instruments", type=int, default=20)
    parser.add_argument(
        "--gate-direction-cluster-instruments",
        type=int,
        default=0,
        help="Optional same-direction instrument cluster threshold; 0 preserves legacy total-cluster-only validation.",
    )
    parser.add_argument("--cooldown-minutes", type=int, default=30)
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args()

    available = set(available_candle_instruments())
    instruments = sorted(available if args.all_candles else (available & VOLATILE_SCOUT_PAIRS))
    if args.days:
        days = [parse_day(d) for d in args.days]
    else:
        days = infer_recent_days(args.recent_days, instruments)
    if not instruments:
        raise SystemExit("No candle instruments selected.")
    if not days:
        raise SystemExit("No days selected.")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_dir or (REPORT_ROOT / f"spike_pressure_m1_daily_validation_{stamp}")
    with output_run_guard(out_dir):
        report, episodes = evaluate(
            days=days,
            instruments=instruments,
            watch_score=args.watch_score,
            gate_score=args.gate_score,
            gate_ratio=args.gate_ratio,
            gate_signal_usd=args.gate_signal_usd,
            gate_cluster_instruments=args.gate_cluster_instruments,
            gate_direction_cluster_instruments=args.gate_direction_cluster_instruments,
            cooldown_minutes=args.cooldown_minutes,
        )
        write_report(report, episodes, out_dir)
    print(
        json.dumps(
            {
                "report": str(out_dir / "summary.md"),
                "days": report["days_requested"],
                "aggregate": report["aggregate"],
            },
            indent=2,
            default=str,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
