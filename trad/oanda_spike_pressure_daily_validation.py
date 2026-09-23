#!/usr/bin/env python3
"""Validate the pre-spike pressure/value gate on historical NY trading days.

Research-only. This does not place orders, change account state, or touch live
manager processes. It uses the historical feature cache plus the same OANDA
read-only value-weight helper used by the cluster quick test.
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from oanda_spike_cluster_quick_test import ACCOUNT_NAV_USD, build_value_table


ROOT = Path(__file__).resolve().parent
DEFAULT_DATASET = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "continuous_research"
    / "spike_cluster_architecture_features_60m.parquet"
)
REPORT_ROOT = ROOT / "data" / "technical_scout_manager" / "reports"
NY = ZoneInfo("America/New_York")

MAJOR_CURRENCIES = {"USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "NZD"}
VOLATILE_SCOUT_PAIRS = {
    "CHF_ZAR",
    "EUR_ZAR",
    "GBP_ZAR",
    "USD_ZAR",
    "EUR_TRY",
    "USD_TRY",
    "USD_HUF",
    "EUR_HUF",
    "USD_THB",
    "USD_CZK",
    "EUR_CZK",
    "USD_PLN",
    "EUR_PLN",
    "GBP_PLN",
    "USD_NOK",
    "EUR_NOK",
    "USD_SEK",
    "EUR_SEK",
    "USD_DKK",
    "EUR_DKK",
    "USD_CNH",
    "USD_MXN",
    "EUR_MXN",
    "HKD_JPY",
    "CHF_HKD",
    "GBP_HKD",
    "EUR_HKD",
    "CAD_HKD",
    "AUD_HKD",
    "USD_HKD",
}

BASE_COLUMNS = [
    "time_utc",
    "instrument",
    "pair_taxonomy_primary",
    "regime_primary",
    "momentum_5_atr",
    "momentum_15_atr",
    "momentum_30_atr",
    "momentum_60_atr",
    "momentum_acceleration_pressure",
    "compression_30",
    "realized_vol_ratio_30_240",
    "spread_pips",
    "atr240_pips",
    "instrument_q995_abs_60_pips",
    "is_volatile_or_exotic_pair",
    "major_event_60",
    "major_direction_up_60",
    "profitable_long_move_60",
    "profitable_short_move_60",
    "profitable_any_move_60",
    "long_trailing_stop075_net_pips_60",
    "short_trailing_stop075_net_pips_60",
    "long_curve_net_pips_60",
    "short_curve_net_pips_60",
    "market_pair_count",
    "market_abs_momentum15_mean",
    "market_abs_momentum60_mean",
    "market_vol_expansion_mean",
    "base_ccy_pressure_15_abs",
    "quote_ccy_pressure_15_abs",
    "pair_ccy_pressure_15_abs",
    "base_ccy_pressure_60_abs",
    "quote_ccy_pressure_60_abs",
    "pair_ccy_pressure_60_abs",
]


def clamp_series(values: pd.Series, lo: float, hi: float) -> pd.Series:
    return values.clip(lower=lo, upper=hi)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        number = float(value)
        return number if math.isfinite(number) else default
    except Exception:
        return default


def parse_day(value: str) -> date:
    return date.fromisoformat(value)


def ny_day_bounds(days: list[date]) -> tuple[pd.Timestamp, pd.Timestamp]:
    start_day = min(days)
    end_day = max(days)
    start = datetime.combine(start_day, time.min, NY).astimezone(timezone.utc)
    end = datetime.combine(end_day, time.max, NY).astimezone(timezone.utc)
    return pd.Timestamp(start), pd.Timestamp(end)


def pair_kind(instrument: str) -> str:
    inst = str(instrument).upper()
    if inst in VOLATILE_SCOUT_PAIRS:
        return "volatile"
    if "_" not in inst:
        return "cross"
    base, quote = inst.split("_", 1)
    if base in MAJOR_CURRENCIES and quote in MAJOR_CURRENCIES:
        return "major"
    if base in MAJOR_CURRENCIES or quote in MAJOR_CURRENCIES:
        return "cross"
    return "exotic"


def event_threshold_pips(instrument: str) -> float:
    """Primary challenger style thresholds from current live config."""
    kind = pair_kind(instrument)
    if kind == "volatile":
        return 35.0
    if kind == "major":
        return 4.0
    if kind == "cross":
        return 6.0
    return 30.0


def available_columns(path: Path) -> list[str]:
    import pyarrow.parquet as pq

    return pq.ParquetFile(path).schema.names


def load_rows(path: Path, days: list[date], volatile_only: bool) -> tuple[pd.DataFrame, str]:
    existing = set(available_columns(path))
    cols = [col for col in BASE_COLUMNS if col in existing]
    start_utc, end_utc = ny_day_bounds(days)
    df = pd.read_parquet(path, columns=cols)
    df["time_utc"] = pd.to_datetime(df["time_utc"], utc=True)
    df = df[(df["time_utc"] >= start_utc) & (df["time_utc"] <= end_utc)].copy()
    df["date_ny"] = df["time_utc"].dt.tz_convert(NY).dt.date.astype(str)
    wanted = {d.isoformat() for d in days}
    df = df[df["date_ny"].isin(wanted)].copy()
    if volatile_only and "is_volatile_or_exotic_pair" in df.columns:
        df = df[pd.to_numeric(df["is_volatile_or_exotic_pair"], errors="coerce").fillna(0.0) > 0].copy()
    instruments = sorted(df["instrument"].dropna().astype(str).unique().tolist())
    value_table, value_source = build_value_table(instruments)
    if not value_table.empty:
        df = df.merge(
            value_table[["instrument", "usd_per_pip_at_budget", "estimated_units_10usd_70pct_margin", "margin_rate"]],
            on="instrument",
            how="left",
            copy=False,
        )
    else:
        df["usd_per_pip_at_budget"] = 0.0
        df["estimated_units_10usd_70pct_margin"] = 0.0
        df["margin_rate"] = 0.0
    df["usd_per_pip_at_budget"] = pd.to_numeric(df["usd_per_pip_at_budget"], errors="coerce").fillna(0.0)
    return df.sort_values(["time_utc", "instrument"]).reset_index(drop=True), value_source


def add_pressure_proxy(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in [
        "momentum_5_atr",
        "momentum_15_atr",
        "momentum_30_atr",
        "momentum_60_atr",
        "momentum_acceleration_pressure",
        "compression_30",
        "realized_vol_ratio_30_240",
        "spread_pips",
        "atr240_pips",
    ]:
        if col not in out:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)

    atr = out["atr240_pips"].clip(lower=0.1)
    m5 = out["momentum_5_atr"] * atr
    m15 = out["momentum_15_atr"] * atr
    m30 = out["momentum_30_atr"] * atr
    # Historical cache does not include the live m3/m10 legs, so this is the
    # closest causal approximation using existing 5/15/30m momentum.
    weighted = m5 * 0.50 + m15 * 0.35 + m30 * 0.15
    abs_weighted = weighted.abs()
    spread = out["spread_pips"].abs().clip(lower=0.1)
    threshold = out["instrument"].astype(str).map(event_threshold_pips).astype(float).clip(lower=0.1)
    accel_pips = (out["momentum_acceleration_pressure"].abs() * atr).clip(lower=0.0)
    compression_ratio = out["compression_30"].clip(lower=0.0)
    expansion_ratio = out["realized_vol_ratio_30_240"].clip(lower=0.0)
    ratio = abs_weighted / spread

    move_score = clamp_series(abs_weighted / threshold * 45.0, 0.0, 45.0)
    accel_score = clamp_series(accel_pips / (threshold * 0.25).clip(lower=0.5) * 20.0, 0.0, 20.0)
    compression_score = clamp_series((1.10 - compression_ratio) * 30.0, 0.0, 15.0)
    expansion_score = clamp_series((expansion_ratio - 1.0) * 14.0, 0.0, 15.0)
    spread_score = clamp_series((ratio - 0.50) * 12.0, 0.0, 10.0)
    score = clamp_series(move_score + accel_score + compression_score + expansion_score + spread_score, 0.0, 100.0)

    out["pressure_direction"] = np.where(weighted >= 0, "LONG", "SHORT")
    out["pressure_signal_pips"] = abs_weighted
    out["pressure_mid_move_pips"] = weighted
    out["pressure_signal_ratio"] = ratio
    out["pressure_threshold_pips"] = threshold
    out["pressure_score_proxy"] = score
    out["pressure_signal_usd"] = out["pressure_signal_pips"] * out["usd_per_pip_at_budget"]
    out["pressure_bucket30"] = out["time_utc"].dt.floor("30min")
    out["pressure_bucket60"] = out["time_utc"].dt.floor("60min")

    long_net = pd.to_numeric(
        out.get("long_trailing_stop075_net_pips_60", out.get("long_curve_net_pips_60", 0.0)),
        errors="coerce",
    ).fillna(0.0)
    short_net = pd.to_numeric(
        out.get("short_trailing_stop075_net_pips_60", out.get("short_curve_net_pips_60", 0.0)),
        errors="coerce",
    ).fillna(0.0)
    if "long_trailing_stop075_net_pips_60" not in out and "long_curve_net_pips_60" in out:
        long_net = pd.to_numeric(out["long_curve_net_pips_60"], errors="coerce").fillna(0.0)
    if "short_trailing_stop075_net_pips_60" not in out and "short_curve_net_pips_60" in out:
        short_net = pd.to_numeric(out["short_curve_net_pips_60"], errors="coerce").fillna(0.0)
    out["pressure_future_net_pips_60"] = np.where(out["pressure_direction"].eq("LONG"), long_net, short_net)
    out["pressure_future_usd_60"] = out["pressure_future_net_pips_60"] * out["usd_per_pip_at_budget"]
    out["tp_value_major_60m"] = out["pressure_future_usd_60"] >= 0.05
    out["tp_raw_major_60m"] = out["pressure_future_net_pips_60"] >= 50.0
    out["tp_any_positive_60m"] = out["pressure_future_net_pips_60"] > 0.0
    return out


def build_episodes(day_rows: pd.DataFrame, min_watch_score: float, cooldown_minutes: int) -> pd.DataFrame:
    pressure = day_rows[day_rows["pressure_score_proxy"] >= min_watch_score].copy()
    if pressure.empty:
        return pressure
    cluster = (
        pressure.groupby("pressure_bucket30", observed=True)
        .agg(
            pre_rows=("instrument", "size"),
            pre_instruments=("instrument", "nunique"),
            pre_pairs_dirs=("pressure_direction", "size"),
        )
        .reset_index()
    )
    pressure = pressure.merge(cluster, on="pressure_bucket30", how="left")
    pressure = pressure.sort_values(["instrument", "pressure_direction", "time_utc"]).reset_index(drop=True)

    episode_ids: list[str] = []
    last_key: tuple[str, str] | None = None
    last_time: pd.Timestamp | None = None
    episode_no: dict[tuple[str, str], int] = {}
    gap = pd.Timedelta(minutes=max(1, cooldown_minutes))
    for row in pressure.itertuples(index=False):
        key = (str(row.instrument), str(row.pressure_direction))
        current_time = row.time_utc
        if key != last_key or last_time is None or current_time - last_time > gap:
            episode_no[key] = episode_no.get(key, -1) + 1
        episode_ids.append(f"{key[0]}:{key[1]}:{episode_no[key]}")
        last_key = key
        last_time = current_time
    pressure["episode_id"] = episode_ids

    agg = (
        pressure.groupby("episode_id", observed=True)
        .agg(
            first_time=("time_utc", "min"),
            last_time=("time_utc", "max"),
            instrument=("instrument", "first"),
            direction=("pressure_direction", "first"),
            rows=("instrument", "size"),
            max_score=("pressure_score_proxy", "max"),
            first_score=("pressure_score_proxy", "first"),
            max_signal_pips=("pressure_signal_pips", "max"),
            max_signal_ratio=("pressure_signal_ratio", "max"),
            max_signal_usd=("pressure_signal_usd", "max"),
            first_signal_usd=("pressure_signal_usd", "first"),
            bucket30=("pressure_bucket30", "first"),
            pre_rows=("pre_rows", "max"),
            pre_instruments=("pre_instruments", "max"),
            pre_pairs_dirs=("pre_pairs_dirs", "max"),
            tp_raw_major_60m=("tp_raw_major_60m", "max"),
            tp_value_major_60m=("tp_value_major_60m", "max"),
            tp_any_positive_60m=("tp_any_positive_60m", "max"),
            best_future_pips_60m=("pressure_future_net_pips_60", "max"),
            best_future_usd_60m=("pressure_future_usd_60", "max"),
            first_future_pips_60m=("pressure_future_net_pips_60", "first"),
            first_future_usd_60m=("pressure_future_usd_60", "first"),
        )
        .reset_index()
    )
    return agg


def summarize_selection(episodes: pd.DataFrame, mask: pd.Series, label: str) -> dict[str, Any]:
    selected = episodes[mask].copy()
    if selected.empty:
        return {
            "label": label,
            "episodes": 0,
            "true_positives_value": 0,
            "false_positives_value": 0,
            "precision_value": 0.0,
            "true_positives_raw": 0,
            "false_positives_raw": 0,
            "precision_raw": 0.0,
            "sum_best_future_usd_60m": 0.0,
            "sum_first_future_usd_60m": 0.0,
            "sum_best_future_pips_60m": 0.0,
            "sum_first_future_pips_60m": 0.0,
            "win_rate_first": 0.0,
        }
    tp_value = selected["tp_value_major_60m"].astype(bool)
    tp_raw = selected["tp_raw_major_60m"].astype(bool)
    first_usd = pd.to_numeric(selected["first_future_usd_60m"], errors="coerce").fillna(0.0)
    first_pips = pd.to_numeric(selected["first_future_pips_60m"], errors="coerce").fillna(0.0)
    best_usd = pd.to_numeric(selected["best_future_usd_60m"], errors="coerce").fillna(0.0)
    best_pips = pd.to_numeric(selected["best_future_pips_60m"], errors="coerce").fillna(0.0)
    return {
        "label": label,
        "episodes": int(len(selected)),
        "true_positives_value": int(tp_value.sum()),
        "false_positives_value": int((~tp_value).sum()),
        "precision_value": float(tp_value.mean()),
        "true_positives_raw": int(tp_raw.sum()),
        "false_positives_raw": int((~tp_raw).sum()),
        "precision_raw": float(tp_raw.mean()),
        "sum_best_future_usd_60m": float(best_usd.sum()),
        "sum_first_future_usd_60m": float(first_usd.sum()),
        "sum_best_future_pips_60m": float(best_pips.sum()),
        "sum_first_future_pips_60m": float(first_pips.sum()),
        "win_rate_first": float((first_usd > 0.0).mean()),
    }


def label_number(value: float | int) -> str:
    text = f"{float(value):g}"
    return text.replace(".", "p").replace("-", "m")


def selection_label_map(gate_score: float, gate_ratio: float, gate_signal_usd: float, gate_cluster_instruments: int) -> dict[str, str]:
    return {
        "all_pressure": "all_pressure_watch_score",
        "loose": "looser_live_like_score78_ratio0p8_value",
        "no_cluster": (
            f"score{label_number(gate_score)}_ratio{label_number(gate_ratio)}"
            f"_value{label_number(gate_signal_usd)}_no_cluster"
        ),
        "gate": (
            f"score{label_number(gate_score)}_ratio{label_number(gate_ratio)}"
            f"_value{label_number(gate_signal_usd)}_cluster{gate_cluster_instruments}"
        ),
    }


def opportunity_summary(day_rows: pd.DataFrame) -> dict[str, Any]:
    base = day_rows[["time_utc", "instrument", "usd_per_pip_at_budget"]].copy()
    long_net = pd.to_numeric(
        day_rows.get("long_trailing_stop075_net_pips_60", day_rows.get("long_curve_net_pips_60", 0.0)),
        errors="coerce",
    ).fillna(0.0)
    short_net = pd.to_numeric(
        day_rows.get("short_trailing_stop075_net_pips_60", day_rows.get("short_curve_net_pips_60", 0.0)),
        errors="coerce",
    ).fillna(0.0)
    long = base.copy()
    long["direction"] = "LONG"
    long["future_pips_60m"] = long_net
    short = base.copy()
    short["direction"] = "SHORT"
    short["future_pips_60m"] = short_net
    opp = pd.concat([long, short], ignore_index=True)
    opp["future_usd_60m"] = opp["future_pips_60m"] * opp["usd_per_pip_at_budget"]
    opp["bucket60"] = opp["time_utc"].dt.floor("60min")
    value_major = opp[opp["future_usd_60m"] >= 0.05].copy()
    raw_major = opp[opp["future_pips_60m"] >= 50.0].copy()

    def clustered(frame: pd.DataFrame, value_col: str) -> pd.DataFrame:
        if frame.empty:
            return frame
        return (
            frame.sort_values(["bucket60", "instrument", "direction", value_col], ascending=[True, True, True, False])
            .drop_duplicates(["bucket60", "instrument", "direction"], keep="first")
            .copy()
        )

    vcl = clustered(value_major, "future_usd_60m")
    rcl = clustered(raw_major, "future_pips_60m")
    return {
        "value_major_rows": int(len(value_major)),
        "value_major_clustered_events": int(len(vcl)),
        "value_major_clustered_usd": float(vcl["future_usd_60m"].sum()) if not vcl.empty else 0.0,
        "raw_major_rows": int(len(raw_major)),
        "raw_major_clustered_events": int(len(rcl)),
        "raw_major_clustered_pips": float(rcl["future_pips_60m"].sum()) if not rcl.empty else 0.0,
        "top_value_opportunities": vcl.sort_values("future_usd_60m", ascending=False)
        .head(10)[["time_utc", "instrument", "direction", "future_pips_60m", "future_usd_60m"]]
        .to_dict("records")
        if not vcl.empty
        else [],
    }


def evaluate_day(
    df: pd.DataFrame,
    day: str,
    *,
    watch_score: float,
    gate_score: float,
    gate_ratio: float,
    gate_signal_usd: float,
    gate_cluster_instruments: int,
    cooldown_minutes: int,
) -> tuple[dict[str, Any], pd.DataFrame]:
    day_rows = df[df["date_ny"].eq(day)].copy()
    episodes = build_episodes(day_rows, watch_score, cooldown_minutes)
    if episodes.empty:
        summary = {
            "date_ny": day,
            "rows": int(len(day_rows)),
            "instruments": int(day_rows["instrument"].nunique()) if "instrument" in day_rows else 0,
            "pressure_episodes": 0,
            "opportunities": opportunity_summary(day_rows),
            "selections": [],
        }
        return summary, episodes

    all_mask = pd.Series(True, index=episodes.index)
    gate_mask = (
        (pd.to_numeric(episodes["max_score"], errors="coerce").fillna(0.0) >= gate_score)
        & (pd.to_numeric(episodes["max_signal_ratio"], errors="coerce").fillna(0.0) >= gate_ratio)
        & (pd.to_numeric(episodes["max_signal_usd"], errors="coerce").fillna(0.0) >= gate_signal_usd)
        & (pd.to_numeric(episodes["pre_instruments"], errors="coerce").fillna(0.0) >= gate_cluster_instruments)
    )
    no_cluster_mask = (
        (pd.to_numeric(episodes["max_score"], errors="coerce").fillna(0.0) >= gate_score)
        & (pd.to_numeric(episodes["max_signal_ratio"], errors="coerce").fillna(0.0) >= gate_ratio)
        & (pd.to_numeric(episodes["max_signal_usd"], errors="coerce").fillna(0.0) >= gate_signal_usd)
    )
    looser_mask = (
        (pd.to_numeric(episodes["max_score"], errors="coerce").fillna(0.0) >= 78.0)
        & (pd.to_numeric(episodes["max_signal_ratio"], errors="coerce").fillna(0.0) >= 0.80)
        & (pd.to_numeric(episodes["max_signal_usd"], errors="coerce").fillna(0.0) >= gate_signal_usd)
    )
    labels = selection_label_map(gate_score, gate_ratio, gate_signal_usd, gate_cluster_instruments)
    summary = {
        "date_ny": day,
        "rows": int(len(day_rows)),
        "instruments": int(day_rows["instrument"].nunique()) if "instrument" in day_rows else 0,
        "pressure_episodes": int(len(episodes)),
        "opportunities": opportunity_summary(day_rows),
        "selections": [
            summarize_selection(episodes, all_mask, labels["all_pressure"]),
            summarize_selection(episodes, looser_mask, labels["loose"]),
            summarize_selection(episodes, no_cluster_mask, labels["no_cluster"]),
            summarize_selection(episodes, gate_mask, labels["gate"]),
        ],
        "top_selected": episodes[gate_mask]
        .sort_values(["best_future_usd_60m", "max_score"], ascending=[False, False])
        .head(20)
        .to_dict("records"),
    }
    episodes[f"selected_{labels['gate']}"] = gate_mask
    return summary, episodes


def markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    if not rows:
        return "_none_"
    out = ["|" + "|".join(columns) + "|", "|" + "|".join(["---"] * len(columns)) + "|"]
    for row in rows:
        vals = []
        for col in columns:
            value = row.get(col, "")
            if isinstance(value, float):
                value = f"{value:.4f}"
            vals.append(str(value))
        out.append("|" + "|".join(vals) + "|")
    return "\n".join(out)


def write_report(report: dict[str, Any], episodes: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    if not episodes.empty:
        episodes.to_csv(out_dir / "episodes.csv", index=False)

    labels = report.get("selection_labels") if isinstance(report.get("selection_labels"), dict) else {}
    gates = report.get("gates") if isinstance(report.get("gates"), dict) else {}
    gate_label = labels.get("gate", "")
    loose_label = labels.get("loose", "looser_live_like_score78_ratio0p8_value")
    gate_rows = []
    for day in report["days"]:
        sel = {row["label"]: row for row in day.get("selections", [])}
        gate = sel.get(gate_label, {})
        loose = sel.get(loose_label, {})
        gate_rows.append(
            {
                "date": day["date_ny"],
                "episodes": day["pressure_episodes"],
                "gate_trades": gate.get("episodes", 0),
                "gate_precision": gate.get("precision_value", 0.0),
                "gate_fp": gate.get("false_positives_value", 0),
                "gate_first_usd": gate.get("sum_first_future_usd_60m", 0.0),
                "gate_best_usd": gate.get("sum_best_future_usd_60m", 0.0),
                "loose_trades": loose.get("episodes", 0),
                "loose_precision": loose.get("precision_value", 0.0),
                "value_events": (day.get("opportunities") or {}).get("value_major_clustered_events", 0),
                "value_usd": (day.get("opportunities") or {}).get("value_major_clustered_usd", 0.0),
            }
        )

    md = [
        "# Spike pressure daily validation",
        "",
        f"Generated UTC: {report['generated_utc']}",
        f"Dataset: `{report['dataset']}`",
        f"Value source: {report['value_source']}",
        "",
        (
            "Gate under test: "
            f"score >= {safe_float(gates.get('gate_score'), 85.0):g}, "
            f"signal/spread >= {safe_float(gates.get('gate_ratio'), 1.0):g}, "
            f"signal value >= ${safe_float(gates.get('gate_signal_usd'), 0.05):g} "
            f"on ${safe_float(gates.get('account_nav_usd'), ACCOUNT_NAV_USD):g} "
            f"@ {safe_float(gates.get('margin_fraction'), 0.70) * 100:g}% margin, "
            f"and >={int(safe_float(gates.get('gate_cluster_instruments'), 20))} "
            "pressure instruments in the 30m bucket."
        ),
        f"Strict gate label: `{gate_label}`.",
        "",
        "## Day summary",
        "",
        markdown_table(
            gate_rows,
            [
                "date",
                "episodes",
                "gate_trades",
                "gate_precision",
                "gate_fp",
                "gate_first_usd",
                "gate_best_usd",
                "loose_trades",
                "loose_precision",
                "value_events",
                "value_usd",
            ],
        ),
        "",
        "## Aggregate",
        "",
        json.dumps(report["aggregate"], indent=2, default=str),
        "",
        "Notes:",
        "- `first_usd` is the estimated value if entered on the first row of each episode.",
        "- `best_usd` is the best same-episode 60m outcome, useful for measuring whether the model saw the setup but timing may still need work.",
        "- This uses a historical proxy for the live pressure score because the cache has 5/15/30m momentum features, not the live m3/m10 candle legs.",
    ]
    (out_dir / "summary.md").write_text("\n".join(md), encoding="utf-8")
    (REPORT_ROOT / "latest_spike_pressure_daily_validation.md").write_text("\n".join(md), encoding="utf-8")
    (REPORT_ROOT / "latest_spike_pressure_daily_validation.json").write_text(
        json.dumps(report, indent=2, default=str),
        encoding="utf-8",
    )


def aggregate(day_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    labels = []
    for day in day_summaries:
        for selection in day.get("selections", []):
            label = selection.get("label")
            if label and label not in labels:
                labels.append(label)
    out: dict[str, Any] = {"days": len(day_summaries)}
    for label in labels:
        rows = []
        for day in day_summaries:
            for sel in day.get("selections", []):
                if sel.get("label") == label:
                    rows.append(sel)
        episodes = sum(int(r.get("episodes", 0)) for r in rows)
        tp = sum(int(r.get("true_positives_value", 0)) for r in rows)
        fp = sum(int(r.get("false_positives_value", 0)) for r in rows)
        out[label] = {
            "episodes": episodes,
            "true_positives_value": tp,
            "false_positives_value": fp,
            "precision_value": float(tp / max(1, tp + fp)),
            "sum_first_future_usd_60m": float(sum(safe_float(r.get("sum_first_future_usd_60m")) for r in rows)),
            "sum_best_future_usd_60m": float(sum(safe_float(r.get("sum_best_future_usd_60m")) for r in rows)),
            "positive_days_first_usd": int(sum(1 for r in rows if safe_float(r.get("sum_first_future_usd_60m")) > 0.0)),
        }
    out["value_major_clustered_events"] = int(
        sum(int((d.get("opportunities") or {}).get("value_major_clustered_events", 0)) for d in day_summaries)
    )
    out["value_major_clustered_usd"] = float(
        sum(safe_float((d.get("opportunities") or {}).get("value_major_clustered_usd", 0.0)) for d in day_summaries)
    )
    return out


def infer_recent_days(path: Path, count: int, volatile_only: bool) -> list[date]:
    cols = ["time_utc", "instrument"]
    if volatile_only:
        cols.append("is_volatile_or_exotic_pair")
    existing = set(available_columns(path))
    cols = [c for c in cols if c in existing]
    df = pd.read_parquet(path, columns=cols)
    df["time_utc"] = pd.to_datetime(df["time_utc"], utc=True)
    if volatile_only and "is_volatile_or_exotic_pair" in df:
        df = df[pd.to_numeric(df["is_volatile_or_exotic_pair"], errors="coerce").fillna(0.0) > 0.0]
    df["date_ny"] = df["time_utc"].dt.tz_convert(NY).dt.date
    by_day = df.groupby("date_ny", observed=True).agg(rows=("instrument", "size"), instruments=("instrument", "nunique"))
    by_day = by_day[(by_day["rows"] >= 1000) & (by_day["instruments"] >= 10)].copy()
    return list(by_day.sort_index().tail(count).index)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--days", nargs="+", default=[])
    parser.add_argument("--recent-days", type=int, default=8)
    parser.add_argument("--all-pairs", action="store_true", help="Do not restrict to volatile/exotic rows.")
    parser.add_argument("--watch-score", type=float, default=62.0)
    parser.add_argument("--gate-score", type=float, default=85.0)
    parser.add_argument("--gate-ratio", type=float, default=1.0)
    parser.add_argument("--gate-signal-usd", type=float, default=0.05)
    parser.add_argument("--gate-cluster-instruments", type=int, default=20)
    parser.add_argument("--cooldown-minutes", type=int, default=30)
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args()

    volatile_only = not args.all_pairs
    if args.days:
        days = [parse_day(d) for d in args.days]
    else:
        days = infer_recent_days(args.dataset, args.recent_days, volatile_only)
    if not days:
        raise SystemExit("No days selected.")

    df, value_source = load_rows(args.dataset, days, volatile_only)
    df = add_pressure_proxy(df)
    day_summaries = []
    episode_frames = []
    for day in sorted({d.isoformat() for d in days}):
        summary, episodes = evaluate_day(
            df,
            day,
            watch_score=args.watch_score,
            gate_score=args.gate_score,
            gate_ratio=args.gate_ratio,
            gate_signal_usd=args.gate_signal_usd,
            gate_cluster_instruments=args.gate_cluster_instruments,
            cooldown_minutes=args.cooldown_minutes,
        )
        day_summaries.append(summary)
        if not episodes.empty:
            episodes = episodes.copy()
            episodes["date_ny"] = day
            episode_frames.append(episodes)

    report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "dataset": str(args.dataset),
        "date_min": min(d.isoformat() for d in days),
        "date_max": max(d.isoformat() for d in days),
        "volatile_only": volatile_only,
        "value_source": value_source,
        "gates": {
            "watch_score": args.watch_score,
            "gate_score": args.gate_score,
            "gate_ratio": args.gate_ratio,
            "gate_signal_usd": args.gate_signal_usd,
            "gate_cluster_instruments": args.gate_cluster_instruments,
            "cooldown_minutes": args.cooldown_minutes,
            "account_nav_usd": ACCOUNT_NAV_USD,
            "margin_fraction": 0.70,
        },
        "selection_labels": selection_label_map(
            args.gate_score,
            args.gate_ratio,
            args.gate_signal_usd,
            args.gate_cluster_instruments,
        ),
        "assumptions": {
            "scope": "research only; no live account state changes",
            "score": "historical proxy for live technical_pressure_signal using 5/15/30m momentum, compression, realized-vol expansion, spread, and primary-style thresholds",
            "success_value": "episode is value-TP when any same-episode 60m direction outcome is >= $0.05 on $10 @ 70% margin",
            "entry_value": "first_usd approximates entering on the first pressure row; best_usd measures timing slack inside the same episode",
        },
        "days": day_summaries,
        "aggregate": aggregate(day_summaries),
    }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_dir or (REPORT_ROOT / f"spike_pressure_daily_validation_{stamp}")
    all_episodes = pd.concat(episode_frames, ignore_index=True) if episode_frames else pd.DataFrame()
    write_report(report, all_episodes, out_dir)
    print(
        json.dumps(
            {
                "report": str(out_dir / "summary.md"),
                "days": [d.isoformat() for d in days],
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
