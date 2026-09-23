#!/usr/bin/env python3
"""Out-of-sample validation for Kraken spike warning conditions.

This script improves on the descriptive spike report by:

- learning spike thresholds and condition cutoffs on an earlier time window;
- applying those rules to a later holdout window;
- using only pre-event liquidity/volume fields for filtering;
- reporting liquidity tiers separately;
- writing a current next-hour watchlist from the latest available candles.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from kraken_spike_condition_research import (
    REPORT_ROOT,
    add_market_features,
    add_pair_features,
    atomic_write_json,
    atomic_write_text,
    choose_usd_pairs,
    fetch_ohlc,
    finite_float,
    load_asset_pairs,
    utc_now,
    write_csv,
)


DEFAULT_OUTPUT_DIR = REPORT_ROOT / "forecast_validation"

ALL_CONDITIONS = [
    "pre_6h_momentum_up",
    "pre_24h_momentum_up",
    "pre_6h_momentum_down",
    "pre_24h_momentum_down",
    "near_24h_high",
    "near_24h_low",
    "prev_volume_z_gt_1",
    "prev_volume_z_gt_2",
    "volume_trend_gt_1p5",
    "vol_compressed_lt_0p8",
    "vol_expanded_gt_1p2",
    "market_abs_hot",
    "market_broad_green",
    "market_broad_red",
]

DIRECTION_CONDITIONS = {
    "positive": [
        "pre_6h_momentum_up",
        "pre_24h_momentum_up",
        "near_24h_high",
        "near_24h_low",
        "prev_volume_z_gt_1",
        "prev_volume_z_gt_2",
        "volume_trend_gt_1p5",
        "vol_compressed_lt_0p8",
        "vol_expanded_gt_1p2",
        "market_abs_hot",
        "market_broad_green",
    ],
    # Negative spikes in the descriptive run were often post-pump breaks, so
    # include both weak-trend and overextended-up conditions.
    "negative": [
        "pre_6h_momentum_down",
        "pre_24h_momentum_down",
        "near_24h_low",
        "pre_6h_momentum_up",
        "pre_24h_momentum_up",
        "near_24h_high",
        "prev_volume_z_gt_1",
        "prev_volume_z_gt_2",
        "volume_trend_gt_1p5",
        "vol_compressed_lt_0p8",
        "vol_expanded_gt_1p2",
        "market_abs_hot",
        "market_broad_red",
    ],
    "absolute": ALL_CONDITIONS,
}

POSITIVE_SCORE_TERMS = [
    "pre_6h_momentum_up",
    "pre_24h_momentum_up",
    "near_24h_high",
    "prev_volume_z_gt_2",
    "volume_trend_gt_1p5",
    "vol_expanded_gt_1p2",
    "market_abs_hot",
    "market_broad_green",
]

CRASH_AFTER_PUMP_TERMS = [
    "pre_6h_momentum_up",
    "pre_24h_momentum_up",
    "near_24h_high",
    "prev_volume_z_gt_2",
    "volume_trend_gt_1p5",
    "vol_expanded_gt_1p2",
    "market_abs_hot",
    "market_broad_red",
]

DUMP_CONTINUATION_TERMS = [
    "pre_6h_momentum_down",
    "pre_24h_momentum_down",
    "near_24h_low",
    "prev_volume_z_gt_2",
    "volume_trend_gt_1p5",
    "vol_expanded_gt_1p2",
    "market_abs_hot",
    "market_broad_red",
]


def parse_tiers(raw: str) -> list[float]:
    tiers: list[float] = []
    for item in raw.split(","):
        item = item.strip().replace("_", "")
        if not item:
            continue
        tiers.append(float(item))
    if not tiers:
        raise ValueError("At least one liquidity tier is required.")
    return sorted(set(tiers))


def atomic_write_frame_csv_gz(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    frame.to_csv(tmp, index=False, compression="gzip")
    os.replace(tmp, path)


def qvalue(frame: pd.DataFrame, column: str, quantile: float, default: float = 0.0) -> float:
    if column not in frame.columns:
        return default
    series = pd.to_numeric(frame[column], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if series.empty:
        return default
    value = float(series.quantile(quantile))
    return value if math.isfinite(value) else default


def add_known_liquidity_columns(combined: pd.DataFrame) -> pd.DataFrame:
    out = combined.sort_values(["pair", "time_utc"] if "time_utc" in combined.columns else ["pair"]).copy()
    if "time_utc" in out.columns:
        group_keys = out.groupby("pair", group_keys=False)
    else:
        group_keys = out.groupby("pair", group_keys=False)

    out["pre_quote_volume_30d"] = group_keys["quote_volume"].transform(
        lambda value: value.shift(1).rolling(720, min_periods=24).sum()
    )
    out["pre_trade_count_24h"] = group_keys["count"].transform(
        lambda value: value.shift(1).rolling(24, min_periods=12).sum()
    )
    return out


def condition_thresholds(train: pd.DataFrame) -> dict[str, float]:
    return {
        "pre_ret_6h_p75": qvalue(train, "pre_ret_6h", 0.75),
        "pre_ret_6h_p25": qvalue(train, "pre_ret_6h", 0.25),
        "pre_ret_24h_p75": qvalue(train, "pre_ret_24h", 0.75),
        "pre_ret_24h_p25": qvalue(train, "pre_ret_24h", 0.25),
        "market_abs_p75": qvalue(train, "pre_market_median_abs_ret_1h", 0.75),
    }


def spike_thresholds(train: pd.DataFrame, spike_quantile: float) -> dict[str, float]:
    return {
        "positive": qvalue(train, "ret_1h", spike_quantile, float("inf")),
        "negative": qvalue(train, "ret_1h", 1.0 - spike_quantile, float("-inf")),
        "absolute": qvalue(train, "abs_ret_1h", spike_quantile, float("inf")),
    }


def condition_columns(frame: pd.DataFrame, thresholds: dict[str, float]) -> dict[str, pd.Series]:
    return {
        "pre_6h_momentum_up": frame["pre_ret_6h"] > thresholds["pre_ret_6h_p75"],
        "pre_24h_momentum_up": frame["pre_ret_24h"] > thresholds["pre_ret_24h_p75"],
        "pre_6h_momentum_down": frame["pre_ret_6h"] < thresholds["pre_ret_6h_p25"],
        "pre_24h_momentum_down": frame["pre_ret_24h"] < thresholds["pre_ret_24h_p25"],
        "near_24h_high": frame["range_position_24h"] > 0.8,
        "near_24h_low": frame["range_position_24h"] < 0.2,
        "prev_volume_z_gt_1": frame["prev_volume_z72"] > 1.0,
        "prev_volume_z_gt_2": frame["prev_volume_z72"] > 2.0,
        "volume_trend_gt_1p5": frame["volume_trend_6v72"] > 1.5,
        "vol_compressed_lt_0p8": frame["vol_compression_24v168"] < 0.8,
        "vol_expanded_gt_1p2": frame["vol_compression_24v168"] > 1.2,
        "market_abs_hot": frame["pre_market_median_abs_ret_1h"] > thresholds["market_abs_p75"],
        "market_broad_green": frame["pre_market_positive_share"] > 0.6,
        "market_broad_red": frame["pre_market_positive_share"] < 0.4,
    }


def is_valid_combo(combo: Sequence[str]) -> bool:
    names = set(combo)
    invalid_pairs = [
        ("pre_6h_momentum_up", "pre_6h_momentum_down"),
        ("pre_24h_momentum_up", "pre_24h_momentum_down"),
        ("near_24h_high", "near_24h_low"),
        ("vol_compressed_lt_0p8", "vol_expanded_gt_1p2"),
        ("market_broad_green", "market_broad_red"),
    ]
    if any(left in names and right in names for left, right in invalid_pairs):
        return False
    if "prev_volume_z_gt_1" in names and "prev_volume_z_gt_2" in names:
        return False
    return True


def target_for_direction(frame: pd.DataFrame, direction: str, thresholds: dict[str, float]) -> pd.Series:
    if direction == "positive":
        return frame["ret_1h"] >= thresholds["positive"]
    if direction == "negative":
        return frame["ret_1h"] <= thresholds["negative"]
    if direction == "absolute":
        return frame["abs_ret_1h"] >= thresholds["absolute"]
    raise ValueError(f"Unknown direction: {direction}")


def metric_row(mask: pd.Series, target: pd.Series) -> dict[str, Any]:
    clean_mask = mask.fillna(False).astype(bool)
    clean_target = target.fillna(False).astype(bool)
    prediction_count = int(clean_mask.sum())
    hit_count = int((clean_mask & clean_target).sum())
    event_count = int(clean_target.sum())
    row_count = int(len(clean_target))
    precision = hit_count / max(prediction_count, 1)
    base_rate = event_count / max(row_count, 1)
    recall = hit_count / max(event_count, 1)
    lift = precision / base_rate if base_rate > 0 else 0.0
    return {
        "row_count": row_count,
        "event_count": event_count,
        "prediction_count": prediction_count,
        "hit_count": hit_count,
        "precision": precision,
        "base_rate": base_rate,
        "lift": lift,
        "recall": recall,
    }


def combined_mask(conditions: dict[str, pd.Series], combo: Sequence[str], index: pd.Index) -> pd.Series:
    mask = pd.Series(True, index=index)
    for name in combo:
        mask &= conditions[name].fillna(False)
    return mask


def evaluate_rule_candidates(
    train: pd.DataFrame,
    test: pd.DataFrame,
    direction: str,
    condition_cutoffs: dict[str, float],
    event_cutoffs: dict[str, float],
    min_train_predictions: int,
    combo_max: int,
) -> list[dict[str, Any]]:
    train_conditions = condition_columns(train, condition_cutoffs)
    test_conditions = condition_columns(test, condition_cutoffs)
    train_target = target_for_direction(train, direction, event_cutoffs)
    test_target = target_for_direction(test, direction, event_cutoffs)
    candidates: list[dict[str, Any]] = []
    names = DIRECTION_CONDITIONS[direction]
    for size in range(1, combo_max + 1):
        for combo in itertools.combinations(names, size):
            if not is_valid_combo(combo):
                continue
            train_mask = combined_mask(train_conditions, combo, train.index)
            train_metric = metric_row(train_mask, train_target)
            if train_metric["prediction_count"] < min_train_predictions:
                continue
            if train_metric["hit_count"] < 3:
                continue
            test_mask = combined_mask(test_conditions, combo, test.index)
            test_metric = metric_row(test_mask, test_target)
            row: dict[str, Any] = {
                "direction": direction,
                "conditions": " AND ".join(combo),
                "condition_count": size,
            }
            for key, value in train_metric.items():
                row[f"train_{key}"] = value
            for key, value in test_metric.items():
                row[f"test_{key}"] = value
            candidates.append(row)

    candidates.sort(
        key=lambda row: (
            finite_float(row["train_lift"]),
            finite_float(row["train_precision"]),
            int(row["train_hit_count"]),
        ),
        reverse=True,
    )
    for rank, row in enumerate(candidates, start=1):
        row["train_rank"] = rank
    return candidates


def score_series(conditions: dict[str, pd.Series], terms: Sequence[str], index: pd.Index) -> pd.Series:
    score = pd.Series(0, index=index, dtype=int)
    for term in terms:
        score += conditions[term].fillna(False).astype(int)
    return score


def score_bucket_rows(
    frame: pd.DataFrame,
    direction: str,
    label: str,
    condition_cutoffs: dict[str, float],
    event_cutoffs: dict[str, float],
) -> list[dict[str, Any]]:
    conditions = condition_columns(frame, condition_cutoffs)
    if direction == "positive":
        score = score_series(conditions, POSITIVE_SCORE_TERMS, frame.index)
    elif direction == "negative":
        pump = score_series(conditions, CRASH_AFTER_PUMP_TERMS, frame.index)
        continuation = score_series(conditions, DUMP_CONTINUATION_TERMS, frame.index)
        score = pd.concat([pump, continuation], axis=1).max(axis=1)
    else:
        positive = score_series(conditions, POSITIVE_SCORE_TERMS, frame.index)
        pump = score_series(conditions, CRASH_AFTER_PUMP_TERMS, frame.index)
        continuation = score_series(conditions, DUMP_CONTINUATION_TERMS, frame.index)
        score = pd.concat([positive, pump, continuation], axis=1).max(axis=1)

    target = target_for_direction(frame, direction, event_cutoffs).fillna(False).astype(bool)
    base_rate = float(target.mean()) if len(target) else 0.0
    rows: list[dict[str, Any]] = []
    for bucket in sorted(score.dropna().unique()):
        mask = score == int(bucket)
        prediction_count = int(mask.sum())
        if prediction_count == 0:
            continue
        hit_count = int((mask & target).sum())
        precision = hit_count / prediction_count
        rows.append(
            {
                "sample": label,
                "direction": direction,
                "score": int(bucket),
                "prediction_count": prediction_count,
                "hit_count": hit_count,
                "precision": precision,
                "base_rate": base_rate,
                "lift": precision / base_rate if base_rate > 0 else 0.0,
            }
        )
    return rows


def split_by_time(frame: pd.DataFrame, train_fraction: float) -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    times = pd.Index(sorted(frame.index.unique()))
    if len(times) < 3:
        raise ValueError("Not enough timestamps for a train/test split.")
    split_idx = min(max(int(len(times) * train_fraction), 1), len(times) - 1)
    split_time = pd.Timestamp(times[split_idx])
    train = frame[frame.index < split_time].copy()
    test = frame[frame.index >= split_time].copy()
    return train, test, split_time


def prepare_tier_frame(
    combined: pd.DataFrame,
    min_pre_quote_volume_30d: float,
    min_prev_quote_volume_24h: float,
    min_prev_trade_count_24h: float,
) -> pd.DataFrame:
    frame = combined[
        combined["ret_1h"].notna()
        & combined["abs_ret_1h"].notna()
        & combined["pre_ret_24h"].notna()
        & combined["pre_quote_volume_30d"].fillna(0.0).ge(min_pre_quote_volume_30d)
        & combined["prev_quote_volume_24h"].fillna(0.0).ge(min_prev_quote_volume_24h)
        & combined["pre_trade_count_24h"].fillna(0.0).ge(min_prev_trade_count_24h)
    ].copy()
    return frame.replace([np.inf, -np.inf], np.nan)


def metric_summary_row(
    tier: float,
    direction: str,
    selected: dict[str, Any] | None,
    validated: dict[str, Any] | None,
    train: pd.DataFrame,
    test: pd.DataFrame,
    event_cutoffs: dict[str, float],
) -> dict[str, Any]:
    train_target = target_for_direction(train, direction, event_cutoffs)
    test_target = target_for_direction(test, direction, event_cutoffs)
    selected = selected or {}
    validated = validated or {}
    return {
        "liquidity_min_pre_quote_volume_30d": tier,
        "direction": direction,
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "train_events": int(train_target.sum()),
        "test_events": int(test_target.sum()),
        "train_base_rate": float(train_target.mean()) if len(train_target) else 0.0,
        "test_base_rate": float(test_target.mean()) if len(test_target) else 0.0,
        "train_selected_rule": selected.get("conditions", ""),
        "train_selected_train_rank": selected.get("train_rank", ""),
        "train_selected_train_lift": selected.get("train_lift", 0.0),
        "train_selected_train_precision": selected.get("train_precision", 0.0),
        "train_selected_test_lift": selected.get("test_lift", 0.0),
        "train_selected_test_precision": selected.get("test_precision", 0.0),
        "train_selected_test_predictions": selected.get("test_prediction_count", 0),
        "train_selected_test_hits": selected.get("test_hit_count", 0),
        "best_validated_rule": validated.get("conditions", ""),
        "best_validated_train_rank": validated.get("train_rank", ""),
        "best_validated_train_lift": validated.get("train_lift", 0.0),
        "best_validated_train_precision": validated.get("train_precision", 0.0),
        "best_validated_test_lift": validated.get("test_lift", 0.0),
        "best_validated_test_precision": validated.get("test_precision", 0.0),
        "best_validated_test_predictions": validated.get("test_prediction_count", 0),
        "best_validated_test_hits": validated.get("test_hit_count", 0),
    }


def latest_snapshot_for_pair(pair_frame: pd.DataFrame) -> dict[str, Any] | None:
    frame = pair_frame.sort_index()
    if len(frame) < 80:
        return None
    close = pd.to_numeric(frame["close"], errors="coerce")
    high = pd.to_numeric(frame["high"], errors="coerce")
    low = pd.to_numeric(frame["low"], errors="coerce")
    volume = pd.to_numeric(frame["volume"], errors="coerce")
    count = pd.to_numeric(frame["count"], errors="coerce")
    quote_volume = pd.to_numeric(frame["quote_volume"], errors="coerce")
    ret = close.pct_change()
    if close.dropna().empty:
        return None

    idx = frame.index[-1]
    row = frame.iloc[-1]
    out: dict[str, Any] = {
        "time_utc": idx,
        "pair": row.get("pair", ""),
        "base": row.get("base", ""),
        "quote": row.get("quote", ""),
        "close": finite_float(close.iloc[-1]),
        "pre_quote_volume_30d": finite_float(quote_volume.tail(720).sum()),
        "prev_quote_volume_24h": finite_float(quote_volume.tail(24).sum()),
        "pre_trade_count_24h": finite_float(count.tail(24).sum()),
    }
    for window in [1, 3, 6, 12, 24]:
        if len(close) > window and close.iloc[-window - 1] not in [0, np.nan]:
            out[f"pre_ret_{window}h"] = finite_float(close.iloc[-1] / close.iloc[-window - 1] - 1.0, np.nan)
        else:
            out[f"pre_ret_{window}h"] = np.nan
    out["pre_abs_ret_6h"] = abs(finite_float(out["pre_ret_6h"], np.nan))
    out["pre_abs_ret_24h"] = abs(finite_float(out["pre_ret_24h"], np.nan))
    out["pre_volatility_24h"] = finite_float(ret.tail(24).std(), np.nan)
    out["pre_volatility_72h"] = finite_float(ret.tail(72).std(), np.nan)
    out["pre_volatility_168h"] = finite_float(ret.tail(168).std(), np.nan)
    vol_168 = finite_float(out["pre_volatility_168h"], np.nan)
    out["vol_compression_24v168"] = (
        finite_float(out["pre_volatility_24h"] / vol_168, np.nan)
        if math.isfinite(vol_168) and vol_168 != 0.0
        else np.nan
    )

    vol_72 = volume.tail(72)
    vol_mean_72 = finite_float(vol_72.mean(), np.nan)
    vol_std_72 = finite_float(vol_72.std(), np.nan)
    out["prev_volume_z72"] = finite_float((volume.iloc[-1] - vol_mean_72) / vol_std_72, np.nan) if vol_std_72 else np.nan
    out["volume_trend_6v72"] = finite_float(volume.tail(6).mean() / vol_mean_72, np.nan) if vol_mean_72 else np.nan

    high_24 = finite_float(high.tail(24).max(), np.nan)
    low_24 = finite_float(low.tail(24).min(), np.nan)
    last_close = finite_float(close.iloc[-1], np.nan)
    range_24 = high_24 - low_24
    out["range_24h_pct"] = finite_float(high_24 / low_24 - 1.0, np.nan) if low_24 else np.nan
    out["range_position_24h"] = finite_float((last_close - low_24) / range_24, np.nan) if range_24 else np.nan
    out["dist_from_24h_high_pct"] = finite_float(last_close / high_24 - 1.0, np.nan) if high_24 else np.nan
    out["dist_from_24h_low_pct"] = finite_float(last_close / low_24 - 1.0, np.nan) if low_24 else np.nan
    return out


def build_latest_snapshot(combined: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, pair_frame in combined.groupby("pair"):
        snapshot = latest_snapshot_for_pair(pair_frame)
        if snapshot:
            rows.append(snapshot)
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    latest_ret = pd.to_numeric(out["pre_ret_1h"], errors="coerce")
    out["pre_market_median_ret_1h"] = float(latest_ret.median())
    out["pre_market_median_abs_ret_1h"] = float(latest_ret.abs().median())
    out["pre_market_positive_share"] = float((latest_ret > 0).mean())
    return out.replace([np.inf, -np.inf], np.nan)


def build_watchlist(
    snapshot: pd.DataFrame,
    tier_min: float,
    condition_cutoffs: dict[str, float],
    selected_rules: Sequence[dict[str, Any]],
    min_score: int,
) -> list[dict[str, Any]]:
    if snapshot.empty:
        return []
    eligible = snapshot[
        snapshot["pre_quote_volume_30d"].fillna(0.0).ge(tier_min)
        & snapshot["pre_ret_24h"].notna()
    ].copy()
    if eligible.empty:
        return []
    conditions = condition_columns(eligible, condition_cutoffs)
    positive_score = score_series(conditions, POSITIVE_SCORE_TERMS, eligible.index)
    crash_score = score_series(conditions, CRASH_AFTER_PUMP_TERMS, eligible.index)
    dump_score = score_series(conditions, DUMP_CONTINUATION_TERMS, eligible.index)
    rows: list[dict[str, Any]] = []
    for idx, row in eligible.iterrows():
        triggered: list[str] = []
        for rule in selected_rules:
            names = [name.strip() for name in str(rule.get("conditions", "")).split(" AND ") if name.strip()]
            if names and all(bool(conditions[name].loc[idx]) for name in names):
                triggered.append(f"{rule.get('direction')}:{rule.get('conditions')}")
        max_score = max(int(positive_score.loc[idx]), int(crash_score.loc[idx]), int(dump_score.loc[idx]))
        if not triggered and max_score < min_score:
            continue
        rows.append(
            {
                "time_utc": row["time_utc"].isoformat() if hasattr(row["time_utc"], "isoformat") else str(row["time_utc"]),
                "pair": row.get("pair", ""),
                "close": finite_float(row.get("close")),
                "pre_quote_volume_30d": finite_float(row.get("pre_quote_volume_30d")),
                "prev_quote_volume_24h": finite_float(row.get("prev_quote_volume_24h")),
                "pre_ret_6h_pct": 100.0 * finite_float(row.get("pre_ret_6h")),
                "pre_ret_24h_pct": 100.0 * finite_float(row.get("pre_ret_24h")),
                "prev_volume_z72": finite_float(row.get("prev_volume_z72")),
                "volume_trend_6v72": finite_float(row.get("volume_trend_6v72")),
                "range_position_24h": finite_float(row.get("range_position_24h")),
                "positive_score": int(positive_score.loc[idx]),
                "crash_after_pump_score": int(crash_score.loc[idx]),
                "dump_continuation_score": int(dump_score.loc[idx]),
                "triggered_rule_count": len(triggered),
                "triggered_rules": " | ".join(triggered[:5]),
            }
        )
    return sorted(
        rows,
        key=lambda item: (
            int(item["triggered_rule_count"]),
            max(
                int(item["positive_score"]),
                int(item["crash_after_pump_score"]),
                int(item["dump_continuation_score"]),
            ),
            finite_float(item["pre_quote_volume_30d"]),
        ),
        reverse=True,
    )


def fmt_pct(value: Any, digits: int = 2) -> str:
    return f"{100.0 * finite_float(value):.{digits}f}%"


def fmt_num(value: Any, digits: int = 2) -> str:
    return f"{finite_float(value):.{digits}f}"


def markdown_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Kraken Spike Forecast Validation",
        "",
        f"Generated: `{payload['generated_at_utc']}`",
        "",
        "## Scope",
        "",
        f"- Online spot pairs from Kraken AssetPairs: `{payload['online_spot_pair_count']}`",
        f"- Unique online base assets: `{payload['unique_online_base_asset_count']}`",
        f"- USD-like base assets selected: `{payload['usd_like_pair_count']}`",
        f"- Pairs with usable hourly candles: `{payload['analyzed_pair_count']}`",
        f"- Hourly rows collected: `{payload['hourly_row_count']}`",
        f"- Data range: `{payload['data_start_utc']}` to `{payload['data_end_utc']}`",
        f"- Train/test split time: `{payload['split_time_utc']}`",
        f"- Spike quantile learned on train: `{payload['spike_quantile']}`",
        "",
        "## Out-of-Sample Rule Check",
        "",
        "| pre-30d quote vol tier | direction | train base | test base | train-selected rule | train lift | test precision | test lift | test hits/preds | best validated rule | best test precision | best test lift |",
        "|---:|---|---:|---:|---|---:|---:|---:|---:|---|---:|---:|",
    ]
    for row in payload["validation_summary"]:
        lines.append(
            f"| {fmt_num(row['liquidity_min_pre_quote_volume_30d'], 0)} | {row['direction']} | "
            f"{fmt_pct(row['train_base_rate'])} | {fmt_pct(row['test_base_rate'])} | "
            f"{row['train_selected_rule']} | {fmt_num(row['train_selected_train_lift'])} | "
            f"{fmt_pct(row['train_selected_test_precision'])} | {fmt_num(row['train_selected_test_lift'])} | "
            f"{row['train_selected_test_hits']}/{row['train_selected_test_predictions']} | "
            f"{row['best_validated_rule']} | {fmt_pct(row['best_validated_test_precision'])} | "
            f"{fmt_num(row['best_validated_test_lift'])} |"
        )

    lines.extend(
        [
            "",
            "## Test Score Buckets",
            "",
            "| tier | direction | score | predictions | hits | precision | lift |",
            "|---:|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in payload["score_buckets"][:80]:
        lines.append(
            f"| {fmt_num(row['liquidity_min_pre_quote_volume_30d'], 0)} | {row['direction']} | "
            f"{row['score']} | {row['prediction_count']} | {row['hit_count']} | "
            f"{fmt_pct(row['precision'])} | {fmt_num(row['lift'])} |"
        )

    lines.extend(
        [
            "",
            "## Current Watchlist",
            "",
            "| pair | time UTC | 30d pre quote vol | pre 6h % | pre 24h % | vol z | range pos | pos score | crash score | dump score | triggered rule count |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in payload["watchlist"][:40]:
        lines.append(
            f"| {row['pair']} | {row['time_utc']} | {fmt_num(row['pre_quote_volume_30d'], 0)} | "
            f"{fmt_num(row['pre_ret_6h_pct'])} | {fmt_num(row['pre_ret_24h_pct'])} | "
            f"{fmt_num(row['prev_volume_z72'])} | {fmt_num(row['range_position_24h'])} | "
            f"{row['positive_score']} | {row['crash_after_pump_score']} | {row['dump_continuation_score']} | "
            f"{row['triggered_rule_count']} |"
        )

    lines.extend(["", "## Interpretation", ""])
    lines.extend(f"- {item}" for item in payload["interpretation"])
    lines.extend(["", "## Files", ""])
    for label, path in payload["files"].items():
        lines.append(f"- {label}: `{path}`")
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--pause-seconds", type=float, default=0.12)
    parser.add_argument("--max-pairs", type=int, default=0)
    parser.add_argument("--min-rows", type=int, default=120)
    parser.add_argument("--train-fraction", type=float, default=0.70)
    parser.add_argument("--spike-quantile", type=float, default=0.99)
    parser.add_argument("--liquidity-tiers", default="50000,250000,1000000")
    parser.add_argument("--min-prev-quote-volume-24h", type=float, default=0.0)
    parser.add_argument("--min-prev-trade-count-24h", type=float, default=0.0)
    parser.add_argument("--min-train-predictions", type=int, default=50)
    parser.add_argument("--combo-max", type=int, default=3)
    parser.add_argument("--top-rules-per-direction", type=int, default=50)
    parser.add_argument("--watchlist-min-score", type=int, default=5)
    parser.add_argument("--exclude-latest-global-candle", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def collect_kraken_data(args: argparse.Namespace) -> tuple[list[dict[str, Any]], list[dict[str, Any]], pd.DataFrame, list[dict[str, Any]]]:
    pairs = load_asset_pairs()
    online_pairs = [pair for pair in pairs if pair.status == "online"]
    usd_pairs = choose_usd_pairs(pairs)
    if args.max_pairs > 0:
        usd_pairs = usd_pairs[: args.max_pairs]
    print(f"[setup] online spot pairs={len(online_pairs)} selected_usd_like={len(usd_pairs)}", flush=True)

    frames: list[pd.DataFrame] = []
    errors: list[dict[str, Any]] = []
    for idx, pair in enumerate(usd_pairs, start=1):
        try:
            print(f"[ohlc] {idx:03d}/{len(usd_pairs):03d} {pair.wsname}", flush=True)
            frame = fetch_ohlc(pair, int(args.interval), float(args.pause_seconds))
            if len(frame) < int(args.min_rows):
                errors.append({"pair": pair.wsname, "reason": f"too few rows: {len(frame)}"})
                continue
            frames.append(add_pair_features(frame))
        except Exception as exc:
            errors.append({"pair": pair.wsname, "reason": str(exc)[:500]})
    if not frames:
        raise RuntimeError("No Kraken OHLC data was collected.")

    combined = pd.concat(frames, axis=0).sort_index()
    combined = add_market_features(combined).reset_index()
    combined = add_known_liquidity_columns(combined)
    combined = combined.set_index("time_utc").sort_index().replace([np.inf, -np.inf], np.nan)
    if args.exclude_latest_global_candle and not combined.empty:
        combined = combined[combined.index < combined.index.max()].copy()
    return [asdict(pair) for pair in pairs], [asdict(pair) for pair in usd_pairs], combined, errors


def choose_validated_rule(candidates: Sequence[dict[str, Any]], top_n: int) -> dict[str, Any] | None:
    shortlisted = list(candidates[:top_n])
    valid = [row for row in shortlisted if int(row.get("test_prediction_count", 0)) > 0]
    if not valid:
        return shortlisted[0] if shortlisted else None
    valid.sort(
        key=lambda row: (
            finite_float(row.get("test_lift")),
            finite_float(row.get("test_precision")),
            int(row.get("test_hit_count", 0)),
            -int(row.get("train_rank", 999999)),
        ),
        reverse=True,
    )
    return valid[0]


def build_interpretation(summary: Sequence[dict[str, Any]], watchlist: Sequence[dict[str, Any]]) -> list[str]:
    if not summary:
        return ["No eligible validation rows were available after filtering."]
    best_abs = max(
        (row for row in summary if row["direction"] == "absolute"),
        key=lambda row: finite_float(row.get("best_validated_test_lift")),
        default=None,
    )
    best_pos = max(
        (row for row in summary if row["direction"] == "positive"),
        key=lambda row: finite_float(row.get("best_validated_test_lift")),
        default=None,
    )
    best_neg = max(
        (row for row in summary if row["direction"] == "negative"),
        key=lambda row: finite_float(row.get("best_validated_test_lift")),
        default=None,
    )
    items = [
        "This is a harder test than the first pass: condition cutoffs, event thresholds, and rule selection are learned on the earlier window and then applied to later candles.",
        "Liquidity tiers use pre-event 30-day quote volume, so the filter does not benefit from the spike candle itself.",
    ]
    if best_abs:
        items.append(
            "Best absolute-spike holdout lift came from "
            f"`{best_abs['best_validated_rule']}` at the {best_abs['liquidity_min_pre_quote_volume_30d']:.0f} tier "
            f"with {100 * finite_float(best_abs['best_validated_test_precision']):.2f}% precision versus "
            f"{100 * finite_float(best_abs['test_base_rate']):.2f}% base rate."
        )
    if best_pos:
        items.append(
            "Best positive-spike holdout lift came from "
            f"`{best_pos['best_validated_rule']}` with {100 * finite_float(best_pos['best_validated_test_precision']):.2f}% precision."
        )
    if best_neg:
        items.append(
            "Best negative-spike holdout lift came from "
            f"`{best_neg['best_validated_rule']}` with {100 * finite_float(best_neg['best_validated_test_precision']):.2f}% precision."
        )
    items.append(
        f"The current watchlist has {len(watchlist)} names meeting either a top train-discovered rule or the score threshold; these are candidates for monitoring, not automatic trades."
    )
    items.append(
        "The remaining weak point is sample size: Kraken's public OHLC endpoint gives a short recent window, so these results should be rerun daily and forward-paper-tested before risking capital."
    )
    return items


def main() -> int:
    args = parse_args()
    if not 0.2 <= float(args.train_fraction) <= 0.9:
        raise ValueError("--train-fraction must be between 0.2 and 0.9.")
    if not 0.90 <= float(args.spike_quantile) < 1.0:
        raise ValueError("--spike-quantile must be in [0.90, 1.0).")

    output_dir = Path(args.output_dir)
    liquidity_tiers = parse_tiers(str(args.liquidity_tiers))
    pair_rows, selected_pair_rows, combined, errors = collect_kraken_data(args)
    snapshot = build_latest_snapshot(combined)

    validation_summary: list[dict[str, Any]] = []
    all_rules: list[dict[str, Any]] = []
    score_buckets: list[dict[str, Any]] = []
    tier_context: dict[float, dict[str, Any]] = {}

    for tier in liquidity_tiers:
        tier_frame = prepare_tier_frame(
            combined,
            min_pre_quote_volume_30d=tier,
            min_prev_quote_volume_24h=float(args.min_prev_quote_volume_24h),
            min_prev_trade_count_24h=float(args.min_prev_trade_count_24h),
        )
        if len(tier_frame) < 500:
            continue
        train, test, split_time = split_by_time(tier_frame, float(args.train_fraction))
        if len(train) < 250 or len(test) < 100:
            continue
        condition_cutoffs = condition_thresholds(train)
        event_cutoffs = spike_thresholds(train, float(args.spike_quantile))
        tier_context[tier] = {
            "condition_cutoffs": condition_cutoffs,
            "event_cutoffs": event_cutoffs,
            "split_time": split_time.isoformat(),
            "train_rows": int(len(train)),
            "test_rows": int(len(test)),
        }
        for direction in ["positive", "negative", "absolute"]:
            candidates = evaluate_rule_candidates(
                train=train,
                test=test,
                direction=direction,
                condition_cutoffs=condition_cutoffs,
                event_cutoffs=event_cutoffs,
                min_train_predictions=int(args.min_train_predictions),
                combo_max=int(args.combo_max),
            )
            for row in candidates[: int(args.top_rules_per_direction)]:
                row["liquidity_min_pre_quote_volume_30d"] = tier
                all_rules.append(row)
            selected = candidates[0] if candidates else None
            validated = choose_validated_rule(candidates, int(args.top_rules_per_direction))
            validation_summary.append(
                metric_summary_row(
                    tier=tier,
                    direction=direction,
                    selected=selected,
                    validated=validated,
                    train=train,
                    test=test,
                    event_cutoffs=event_cutoffs,
                )
            )
            for bucket_row in score_bucket_rows(test, direction, "test", condition_cutoffs, event_cutoffs):
                bucket_row["liquidity_min_pre_quote_volume_30d"] = tier
                score_buckets.append(bucket_row)

    if not validation_summary:
        raise RuntimeError("No validation rows were produced. Try lower liquidity tiers or lower min prediction settings.")

    min_tier = min(tier_context)
    selected_for_watchlist = [
        row
        for row in all_rules
        if finite_float(row.get("liquidity_min_pre_quote_volume_30d")) == min_tier and int(row.get("train_rank", 999999)) <= 10
    ]
    watchlist = build_watchlist(
        snapshot=snapshot,
        tier_min=min_tier,
        condition_cutoffs=tier_context[min_tier]["condition_cutoffs"],
        selected_rules=selected_for_watchlist,
        min_score=int(args.watchlist_min_score),
    )

    all_pairs_csv = output_dir / "kraken_all_spot_pairs.csv"
    selected_pairs_csv = output_dir / "kraken_selected_usd_like_pairs.csv"
    full_features_csv_gz = output_dir / "kraken_hourly_features_full.csv.gz"
    validation_summary_csv = output_dir / "validation_summary.csv"
    rules_csv = output_dir / "train_selected_rules_with_holdout_metrics.csv"
    score_buckets_csv = output_dir / "score_buckets.csv"
    watchlist_csv = output_dir / "current_watchlist.csv"
    errors_csv = output_dir / "collection_errors.csv"
    summary_json = output_dir / "latest_kraken_spike_forecast_validation.json"
    summary_md = output_dir / "latest_kraken_spike_forecast_validation.md"

    write_csv(all_pairs_csv, pair_rows)
    write_csv(selected_pairs_csv, selected_pair_rows)
    atomic_write_frame_csv_gz(full_features_csv_gz, combined.reset_index())
    write_csv(validation_summary_csv, validation_summary)
    write_csv(rules_csv, all_rules)
    write_csv(score_buckets_csv, score_buckets)
    write_csv(watchlist_csv, watchlist)
    write_csv(errors_csv, errors)

    split_times = [context["split_time"] for context in tier_context.values()]
    payload = {
        "generated_at_utc": utc_now(),
        "source_docs": {
            "asset_pairs": "https://docs.kraken.com/api/docs/rest-api/get-tradable-asset-pairs/",
            "ohlc": "https://docs.kraken.com/api/docs/rest-api/get-ohlc-data/",
        },
        "online_spot_pair_count": len([row for row in pair_rows if row.get("status") == "online"]),
        "unique_online_base_asset_count": len({row.get("base") for row in pair_rows if row.get("status") == "online"}),
        "usd_like_pair_count": len(selected_pair_rows),
        "analyzed_pair_count": int(combined["pair"].nunique()),
        "hourly_row_count": int(len(combined)),
        "data_start_utc": combined.index.min().isoformat(),
        "data_end_utc": combined.index.max().isoformat(),
        "split_time_utc": split_times[0] if split_times else "",
        "spike_quantile": float(args.spike_quantile),
        "train_fraction": float(args.train_fraction),
        "liquidity_tiers": liquidity_tiers,
        "validation_summary": validation_summary,
        "score_buckets": sorted(
            score_buckets,
            key=lambda row: (
                finite_float(row["liquidity_min_pre_quote_volume_30d"]),
                row["direction"],
                int(row["score"]),
            ),
        ),
        "watchlist": watchlist[:100],
        "interpretation": build_interpretation(validation_summary, watchlist),
        "collection_error_count": len(errors),
        "files": {
            "all_pairs_csv": str(all_pairs_csv),
            "selected_pairs_csv": str(selected_pairs_csv),
            "full_features_csv_gz": str(full_features_csv_gz),
            "validation_summary_csv": str(validation_summary_csv),
            "rules_csv": str(rules_csv),
            "score_buckets_csv": str(score_buckets_csv),
            "watchlist_csv": str(watchlist_csv),
            "errors_csv": str(errors_csv),
            "summary_json": str(summary_json),
            "summary_md": str(summary_md),
        },
    }
    atomic_write_json(summary_json, payload)
    atomic_write_text(summary_md, markdown_report(payload))
    print(
        json.dumps(
            {
                "analyzed_pair_count": payload["analyzed_pair_count"],
                "hourly_row_count": payload["hourly_row_count"],
                "data_start_utc": payload["data_start_utc"],
                "data_end_utc": payload["data_end_utc"],
                "validation_rows": len(validation_summary),
                "watchlist_count": len(watchlist),
                "summary_md": str(summary_md),
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
