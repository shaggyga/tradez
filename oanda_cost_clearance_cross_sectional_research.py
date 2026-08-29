#!/usr/bin/env python3
"""Research cost clearance before direction across the 68-pair BAM archive.

The first stage predicts whether either executable side can clear bid/ask plus
modeled slippage.  Only then are magnitude and conditional direction used to
rank pairs.  Outcomes use recorded bid/ask closes.  This script is archive
discovery only: it cannot authorize, promote, or place an order.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, mean_absolute_error, roc_auc_score


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "cost_clearance_cross_sectional_research_v1.json"
SOURCE = ROOT / "data" / "oanda_training_manager" / "candles"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "reports" / "cost_clearance_cross_sectional" / "COST_CLEARANCE_CROSS_SECTIONAL_20260809.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "cost_clearance_cross_sectional" / "COST_CLEARANCE_CROSS_SECTIONAL_20260809.md"

FEATURES = [
    "spread_pips", "entry_cost_pips", "log_volume",
    "return_1m_pips", "return_5m_pips", "return_15m_pips",
    "return_30m_pips", "return_60m_pips",
    "abs_return_1m_pips", "abs_return_5m_pips", "abs_return_15m_pips",
    "volatility_5m_pips", "volatility_15m_pips", "volatility_60m_pips",
    "ema_5_20_gap_pips", "ema_20_slope_5m_pips", "volume_ratio_15m",
    "momentum_cost_ratio", "spread_rank", "movement_rank", "volume_rank",
    "currency_factor_5m_pips", "pair_residual_5m_pips",
    "hour_sin", "hour_cos",
]


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected object in {path}")
    return value


def infer_pip_size(instrument: str, frame: pd.DataFrame) -> float:
    declared = pd.to_numeric(frame["spread_pips"], errors="coerce")
    raw = pd.to_numeric(frame["ask_close"], errors="coerce") - pd.to_numeric(frame["bid_close"], errors="coerce")
    valid = declared.gt(0) & raw.gt(0) & np.isfinite(declared) & np.isfinite(raw)
    if int(valid.sum()) >= 10:
        observed = float((raw[valid] / declared[valid]).median())
        candidates = np.asarray([0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1])
        selected = float(candidates[int(np.argmin(np.abs(np.log10(candidates) - math.log10(observed))))])
        if 0.5 <= observed / selected <= 2.0:
            return selected
    return 0.01 if instrument.endswith("_JPY") else 0.0001


def _exact_shift(frame: pd.DataFrame, column: str, minutes: int) -> pd.Series:
    shifted = frame[column].shift(minutes)
    valid = frame["epoch"].sub(frame["epoch"].shift(minutes)).eq(minutes * 60)
    return shifted.where(valid)


def load_instrument(path: Path) -> pd.DataFrame:
    use = [
        "datetime", "instrument", "close", "bid_close", "ask_close",
        "spread_pips", "volume",
    ]
    frame = pd.read_csv(path, usecols=use)
    instrument = str(frame["instrument"].dropna().iloc[0])
    time = pd.to_datetime(frame["datetime"], utc=True, errors="coerce")
    frame = frame.assign(timestamp=time).dropna(subset=["timestamp"]).sort_values("timestamp")
    frame = frame.drop_duplicates("timestamp", keep="last").reset_index(drop=True)
    frame["epoch"] = frame["timestamp"].astype("int64") // 1_000_000_000
    pip = infer_pip_size(instrument, frame)
    frame["pip_size"] = pip
    frame["base"] = instrument.split("_")[0]
    frame["quote"] = instrument.split("_")[1]
    frame["mid"] = (frame["bid_close"] + frame["ask_close"]) / 2.0
    for minutes in (1, 5, 15, 30, 60):
        frame[f"return_{minutes}m_pips"] = (frame["mid"] - _exact_shift(frame, "mid", minutes)) / pip
    for minutes in (1, 5, 15):
        frame[f"abs_return_{minutes}m_pips"] = frame[f"return_{minutes}m_pips"].abs()
    one = frame["return_1m_pips"]
    for minutes in (5, 15, 60):
        frame[f"volatility_{minutes}m_pips"] = one.rolling(minutes, min_periods=max(3, minutes // 2)).std(ddof=0)
    ema5 = frame["mid"].ewm(span=5, adjust=False).mean()
    ema20 = frame["mid"].ewm(span=20, adjust=False).mean()
    frame["ema_5_20_gap_pips"] = (ema5 - ema20) / pip
    frame["ema_20_slope_5m_pips"] = (ema20 - ema20.shift(5)).where(
        frame["epoch"].sub(frame["epoch"].shift(5)).eq(300)
    ) / pip
    volume_mean = frame["volume"].rolling(15, min_periods=8).mean()
    frame["log_volume"] = np.log1p(frame["volume"].clip(lower=0))
    frame["volume_ratio_15m"] = frame["volume"] / volume_mean.replace(0, np.nan)
    frame["entry_cost_pips"] = frame["spread_pips"]
    frame["momentum_cost_ratio"] = (
        0.65 * frame["return_5m_pips"].abs() + 0.35 * frame["return_15m_pips"].abs()
    ) / frame["entry_cost_pips"].replace(0, np.nan)
    hour = frame["timestamp"].dt.hour + frame["timestamp"].dt.minute / 60.0
    frame["hour_sin"] = np.sin(2 * np.pi * hour / 24.0)
    frame["hour_cos"] = np.cos(2 * np.pi * hour / 24.0)
    frame["instrument"] = instrument
    return frame


def add_cross_sectional_features(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["spread_rank"] = frame.groupby("epoch")["spread_pips"].rank(pct=True, ascending=True)
    frame["movement_rank"] = frame.groupby("epoch")["momentum_cost_ratio"].rank(pct=True, ascending=True)
    frame["volume_rank"] = frame.groupby("epoch")["log_volume"].rank(pct=True, ascending=True)
    base = frame[["epoch", "base", "return_5m_pips"]].rename(columns={"base": "currency", "return_5m_pips": "leg"})
    quote = frame[["epoch", "quote", "return_5m_pips"]].rename(columns={"quote": "currency", "return_5m_pips": "leg"})
    quote["leg"] = -quote["leg"]
    strengths = pd.concat([base, quote], ignore_index=True).groupby(["epoch", "currency"], as_index=False)["leg"].mean()
    left = strengths.rename(columns={"currency": "base", "leg": "base_strength"})
    right = strengths.rename(columns={"currency": "quote", "leg": "quote_strength"})
    frame = frame.merge(left, on=["epoch", "base"], how="left").merge(right, on=["epoch", "quote"], how="left")
    frame["currency_factor_5m_pips"] = frame["base_strength"] - frame["quote_strength"]
    frame["pair_residual_5m_pips"] = frame["return_5m_pips"] - frame["currency_factor_5m_pips"]
    frame["pair_coverage"] = frame.groupby("epoch")["instrument"].transform("nunique")
    return frame


def attach_outcomes(frame: pd.DataFrame, horizon_min: int, slippage: float) -> pd.DataFrame:
    result = frame.copy()
    future = result[["epoch", "bid_close", "ask_close", "mid", "spread_pips"]].copy()
    future["epoch"] = future["epoch"] - horizon_min * 60
    future = future.rename(columns={
        "bid_close": "future_bid", "ask_close": "future_ask", "mid": "future_mid",
        "spread_pips": "future_spread_pips",
    })
    delayed = result[["epoch", "bid_close", "ask_close"]].copy()
    delayed["epoch"] = delayed["epoch"] - 60
    delayed = delayed.rename(columns={"bid_close": "delayed_bid", "ask_close": "delayed_ask"})
    result = result.merge(future, on="epoch", how="left", validate="one_to_one").merge(
        delayed, on="epoch", how="left", validate="one_to_one"
    )
    result["future_move_pips"] = (result["future_mid"] - result["mid"]) / result["pip_size"]
    result["long_net_pips"] = (result["future_bid"] - result["ask_close"]) / result["pip_size"] - slippage
    result["short_net_pips"] = (result["bid_close"] - result["future_ask"]) / result["pip_size"] - slippage
    result["delayed_long_net_pips"] = (result["future_bid"] - result["delayed_ask"]) / result["pip_size"] - slippage
    result["delayed_short_net_pips"] = (result["delayed_bid"] - result["future_ask"]) / result["pip_size"] - slippage
    result["best_after_cost_pips"] = result[["long_net_pips", "short_net_pips"]].max(axis=1)
    result["future_magnitude_pips"] = result["future_move_pips"].abs()
    result["actual_cost_pips"] = (result["spread_pips"] + result["future_spread_pips"]) / 2.0 + slippage
    result["cost_clear"] = result["best_after_cost_pips"].gt(0).astype(int)
    result["future_up"] = result["future_move_pips"].gt(0).astype(int)
    return result


def calibration(model: Any, x: pd.DataFrame, y: pd.Series) -> LogisticRegression | None:
    if len(x) < 50 or y.nunique() < 2:
        return None
    raw = np.clip(model.predict_proba(x)[:, 1], 1e-6, 1 - 1e-6)
    logit = np.log(raw / (1 - raw)).reshape(-1, 1)
    fitted = LogisticRegression(C=1.0, max_iter=300, random_state=20260809)
    fitted.fit(logit, y)
    return fitted


def probabilities(model: Any, calibrator: LogisticRegression | None, x: pd.DataFrame) -> np.ndarray:
    raw = np.clip(model.predict_proba(x)[:, 1], 1e-6, 1 - 1e-6)
    if calibrator is None:
        return raw
    return calibrator.predict_proba(np.log(raw / (1 - raw)).reshape(-1, 1))[:, 1]


def _model_parameters(config: Mapping[str, Any]) -> dict[str, Any]:
    raw = config.get("model") or {}
    return {
        "learning_rate": float(raw.get("learning_rate", 0.05)),
        "max_iter": int(raw.get("max_iter", 100)),
        "max_leaf_nodes": int(raw.get("max_leaf_nodes", 15)),
        "min_samples_leaf": int(raw.get("min_samples_leaf", 50)),
        "l2_regularization": float(raw.get("l2_regularization", 1.0)),
        "random_state": int(raw.get("random_state", 20260809)),
    }


def metric(values: Iterable[float]) -> dict[str, Any]:
    rows = [float(value) for value in values if np.isfinite(value)]
    wins = [value for value in rows if value > 0]
    losses = [-value for value in rows if value < 0]
    best = max(rows) if rows else None
    return {
        "n": len(rows),
        "win_rate": len(wins) / len(rows) if rows else None,
        "average_net_pips": float(np.mean(rows)) if rows else None,
        "total_net_pips": float(np.sum(rows)) if rows else 0.0,
        "profit_factor": sum(wins) / sum(losses) if losses else None,
        "average_without_best_pips": (sum(rows) - best) / (len(rows) - 1) if len(rows) > 1 else None,
        "minimum_net_pips": min(rows) if rows else None,
        "maximum_net_pips": best,
        "proof_eligible": False,
    }


def select_currency_disjoint(group: pd.DataFrame, count: int = 3) -> pd.DataFrame:
    selected: list[int] = []
    currencies: set[str] = set()
    for index, row in group.sort_values(["predicted_ev_pips", "instrument"], ascending=[False, True]).iterrows():
        legs = {str(row["base"]), str(row["quote"])}
        if currencies.isdisjoint(legs):
            selected.append(index)
            currencies.update(legs)
        if len(selected) == count:
            break
    return group.loc[selected] if len(selected) == count else group.iloc[0:0]


def side_result(row: pd.Series, flipped: bool = False) -> float:
    up = bool(float(row["predicted_up_probability"]) >= 0.5)
    if flipped:
        up = not up
    return float(row["long_net_pips"] if up else row["short_net_pips"])


def delayed_side_result(row: pd.Series) -> float:
    up = bool(float(row["predicted_up_probability"]) >= 0.5)
    return float(row["delayed_long_net_pips"] if up else row["delayed_short_net_pips"])


def selection_diagnostics(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        return {
            "pair_counts": {}, "day_counts": {}, "direction_counts": {},
            "early": metric([]), "late": metric([]), "one_minute_delayed": metric([]),
            "cost_stress": {"plus_0_5_pips": metric([]), "plus_1_pip": metric([]), "plus_2_pips": metric([])},
            "without_best_pair": metric([]), "without_best_day": metric([]),
        }
    ordered = sorted(records, key=lambda row: (row["epoch"], row["instrument"]))
    midpoint = ordered[len(ordered) // 2]["epoch"]
    pair_counts: dict[str, int] = {}
    day_counts: dict[str, int] = {}
    direction_counts = {"long": 0, "short": 0}
    for row in ordered:
        pair_counts[row["instrument"]] = pair_counts.get(row["instrument"], 0) + 1
        day_counts[row["day"]] = day_counts.get(row["day"], 0) + 1
        direction_counts[row["direction"]] += 1
    best_pair = max(pair_counts, key=lambda value: (pair_counts[value], value))
    best_day = max(day_counts, key=lambda value: (day_counts[value], value))
    values = [row["net"] for row in ordered]
    delayed = [row["delayed_net"] for row in ordered]
    return {
        "pair_counts": dict(sorted(pair_counts.items(), key=lambda item: (-item[1], item[0]))),
        "day_counts": dict(sorted(day_counts.items(), key=lambda item: (-item[1], item[0]))),
        "direction_counts": direction_counts,
        "early": metric(row["net"] for row in ordered if row["epoch"] < midpoint),
        "late": metric(row["net"] for row in ordered if row["epoch"] >= midpoint),
        "one_minute_delayed": metric(delayed),
        "cost_stress": {
            "plus_0_5_pips": metric(value - 0.5 for value in values),
            "plus_1_pip": metric(value - 1.0 for value in values),
            "plus_2_pips": metric(value - 2.0 for value in values),
        },
        "most_frequent_pair": best_pair,
        "most_frequent_pair_share": pair_counts[best_pair] / len(ordered),
        "without_best_pair": metric(row["net"] for row in ordered if row["instrument"] != best_pair),
        "most_frequent_day": best_day,
        "most_frequent_day_share": day_counts[best_day] / len(ordered),
        "without_best_day": metric(row["net"] for row in ordered if row["day"] != best_day),
    }


def allocate(valid: pd.DataFrame, config: Mapping[str, Any]) -> dict[str, Any]:
    selection = config.get("selection") or {}
    eligible = valid[
        (valid["predicted_clear_probability"] >= float(selection.get("minimum_clear_probability", 0.55)))
        & (valid["predicted_direction_confidence"] >= float(selection.get("minimum_direction_confidence", 0.1)))
        & (valid["predicted_magnitude_pips"] >= float(selection.get("minimum_predicted_magnitude_cost_ratio", 1.5)) * valid["entry_cost_pips"])
        & (valid["predicted_ev_pips"] > float(selection.get("minimum_predicted_ev_pips", 0.0)))
    ].copy()
    top: list[float] = []
    flipped: list[float] = []
    baskets: list[float] = []
    basket_flips: list[float] = []
    random_same_set: list[float] = []
    top_records: list[dict[str, Any]] = []
    for epoch, group in eligible.groupby("epoch"):
        ordered = group.sort_values(["predicted_ev_pips", "instrument"], ascending=[False, True])
        chosen = ordered.iloc[0]
        top.append(side_result(chosen))
        flipped.append(side_result(chosen, True))
        is_up = bool(float(chosen["predicted_up_probability"]) >= 0.5)
        top_records.append({
            "epoch": int(epoch),
            "day": dt.datetime.fromtimestamp(int(epoch), dt.timezone.utc).date().isoformat(),
            "instrument": str(chosen["instrument"]),
            "direction": "long" if is_up else "short",
            "net": side_result(chosen),
            "delayed_net": delayed_side_result(chosen),
        })
        random_index = int(hashlib.sha256(str(int(epoch)).encode()).hexdigest()[:8], 16) % len(group)
        random_same_set.append(side_result(group.sort_values("instrument").iloc[random_index]))
        disjoint = select_currency_disjoint(group)
        if len(disjoint) == 3:
            baskets.append(float(np.mean([side_result(row) for _, row in disjoint.iterrows()])))
            basket_flips.append(float(np.mean([side_result(row, True) for _, row in disjoint.iterrows()])))
    momentum: list[float] = []
    lowest_cost: list[float] = []
    oracle: list[float] = []
    for _, group in valid.groupby("epoch"):
        mom = group.sort_values(["momentum_cost_ratio", "instrument"], ascending=[False, True]).iloc[0]
        momentum.append(float(mom["long_net_pips"] if mom["return_5m_pips"] >= 0 else mom["short_net_pips"]))
        cheap = group.sort_values(["entry_cost_pips", "instrument"]).iloc[0]
        lowest_cost.append(float(cheap["long_net_pips"] if cheap["return_5m_pips"] >= 0 else cheap["short_net_pips"]))
        oracle.append(float(group["best_after_cost_pips"].max()))
    return {
        "eligible_pair_rows": int(len(eligible)),
        "eligible_decision_timestamps": int(eligible["epoch"].nunique()),
        "all_holdout_timestamps": int(valid["epoch"].nunique()),
        "learned_top_one": metric(top),
        "learned_top_one_diagnostics": selection_diagnostics(top_records),
        "learned_top_one_flipped": metric(flipped),
        "exactly_three_currency_disjoint": metric(baskets),
        "exactly_three_currency_disjoint_flipped": metric(basket_flips),
        "deterministic_random_same_eligible_set": metric(random_same_set),
        "momentum_top_one_baseline": metric(momentum),
        "lowest_cost_momentum_baseline": metric(lowest_cost),
        "oracle_top_one_upper_bound": metric(oracle),
        "no_trade_average_net_pips": 0.0,
    }


def candidate_lock_gate(allocation: Mapping[str, Any]) -> dict[str, Any]:
    top = allocation.get("learned_top_one") or {}
    diagnostics = allocation.get("learned_top_one_diagnostics") or {}
    reasons: list[str] = []
    if int(top.get("n") or 0) < 30:
        reasons.append("fewer_than_30_selected_holdout_decisions")
    if float(top.get("average_net_pips") or -1e9) <= 0:
        reasons.append("nonpositive_holdout_ev")
    if float((diagnostics.get("early") or {}).get("average_net_pips") or -1e9) <= 0:
        reasons.append("nonpositive_early_holdout_ev")
    if float((diagnostics.get("late") or {}).get("average_net_pips") or -1e9) <= 0:
        reasons.append("nonpositive_late_holdout_ev")
    if float((diagnostics.get("one_minute_delayed") or {}).get("average_net_pips") or -1e9) <= 0:
        reasons.append("nonpositive_one_minute_delayed_ev")
    if float(((diagnostics.get("cost_stress") or {}).get("plus_1_pip") or {}).get("average_net_pips") or -1e9) <= 0:
        reasons.append("nonpositive_plus_one_pip_cost_stress")
    if float((diagnostics.get("without_best_pair") or {}).get("average_net_pips") or -1e9) <= 0:
        reasons.append("nonpositive_without_most_frequent_pair")
    if float(diagnostics.get("most_frequent_pair_share") or 1.0) > 0.5:
        reasons.append("more_than_half_of_selections_from_one_pair")
    if len(diagnostics.get("day_counts") or {}) < 3:
        reasons.append("fewer_than_three_holdout_days")
    if float(diagnostics.get("most_frequent_day_share") or 1.0) > 0.5:
        reasons.append("more_than_half_of_selections_from_one_day")
    return {"eligible": not reasons, "reasons": reasons, "opens_untouched_cohort": False}


def fit_cell(frame: pd.DataFrame, horizon_min: int, bucket: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    minimum = float(bucket["minimum_spread_pips"])
    maximum = float(bucket["maximum_spread_pips"])
    cell = frame[(frame["spread_pips"] > minimum) & (frame["spread_pips"] <= maximum)].copy()
    if bool(config.get("aligned_non_overlapping_decisions", True)):
        cell = cell[cell["epoch"].mod(horizon_min * 60).eq(0)]
    cell = cell.replace([np.inf, -np.inf], np.nan).dropna(subset=FEATURES + [
        "future_move_pips", "long_net_pips", "short_net_pips", "delayed_long_net_pips",
        "delayed_short_net_pips", "actual_cost_pips"
    ])
    epochs = np.asarray(sorted(cell["epoch"].unique()))
    if len(epochs) < 20:
        return {"horizon_min": horizon_min, "cost_bucket": bucket["id"], "status": "insufficient_epochs", "proof_eligible": False}
    train_fraction = float(config.get("train_fraction", 0.5))
    cal_fraction = float(config.get("calibration_fraction", 0.25))
    split1 = epochs[max(1, int(len(epochs) * train_fraction)) - 1]
    split2 = epochs[max(2, int(len(epochs) * (train_fraction + cal_fraction))) - 1]
    purge = horizon_min * 60
    train = cell[cell["epoch"] < split1 - purge].copy()
    cal = cell[(cell["epoch"] >= split1) & (cell["epoch"] < split2 - purge)].copy()
    valid = cell[cell["epoch"] >= split2].copy()
    if min(len(train), len(cal), len(valid)) < 100 or train["cost_clear"].nunique() < 2:
        return {"horizon_min": horizon_min, "cost_bucket": bucket["id"], "status": "insufficient_rows", "proof_eligible": False}
    parameters = _model_parameters(config)
    clear_model = HistGradientBoostingClassifier(**parameters).fit(train[FEATURES], train["cost_clear"])
    clear_cal = calibration(clear_model, cal[FEATURES], cal["cost_clear"])
    directional_train = train[train["cost_clear"] == 1]
    directional_cal = cal[cal["cost_clear"] == 1]
    if directional_train["future_up"].nunique() < 2:
        return {"horizon_min": horizon_min, "cost_bucket": bucket["id"], "status": "insufficient_direction_classes", "proof_eligible": False}
    direction_model = HistGradientBoostingClassifier(**parameters).fit(directional_train[FEATURES], directional_train["future_up"])
    direction_cal = calibration(direction_model, directional_cal[FEATURES], directional_cal["future_up"])
    magnitude_model = HistGradientBoostingRegressor(loss="absolute_error", **parameters).fit(train[FEATURES], train["future_magnitude_pips"])
    valid["predicted_clear_probability"] = probabilities(clear_model, clear_cal, valid[FEATURES])
    valid["predicted_up_probability"] = probabilities(direction_model, direction_cal, valid[FEATURES])
    valid["predicted_direction_confidence"] = (2 * valid["predicted_up_probability"] - 1).abs()
    valid["predicted_magnitude_pips"] = np.maximum(0.0, magnitude_model.predict(valid[FEATURES]))
    valid["predicted_ev_pips"] = (
        valid["predicted_clear_probability"] * valid["predicted_magnitude_pips"]
        * valid["predicted_direction_confidence"] - valid["entry_cost_pips"]
    )
    clears = valid[valid["cost_clear"] == 1]
    direction_accuracy = float(((clears["predicted_up_probability"] >= 0.5).astype(int) == clears["future_up"]).mean()) if len(clears) else None
    allocation = allocate(valid, config)
    return {
        "horizon_min": horizon_min,
        "cost_bucket": bucket["id"],
        "status": "ok",
        "rows": {"train": len(train), "calibration": len(cal), "holdout": len(valid), "holdout_clear": int(valid["cost_clear"].sum())},
        "epochs": {"train": int(train["epoch"].nunique()), "calibration": int(cal["epoch"].nunique()), "holdout": int(valid["epoch"].nunique())},
        "cutoffs": {"train_end_epoch": int(split1), "holdout_start_epoch": int(split2), "purge_sec": purge},
        "clearance": {
            "base_rate": float(valid["cost_clear"].mean()),
            "roc_auc": float(roc_auc_score(valid["cost_clear"], valid["predicted_clear_probability"])) if valid["cost_clear"].nunique() > 1 else None,
            "brier": float(brier_score_loss(valid["cost_clear"], valid["predicted_clear_probability"])),
        },
        "conditional_direction_accuracy": direction_accuracy,
        "magnitude_mae_pips": float(mean_absolute_error(valid["future_magnitude_pips"], valid["predicted_magnitude_pips"])),
        "allocation": allocation,
        "candidate_lock_gate": candidate_lock_gate(allocation),
        "proof_eligible": False,
    }


def run(config_path: Path = CONFIG, source_dir: Path = SOURCE, output: Path = OUTPUT, report: Path = REPORT) -> dict[str, Any]:
    config = read_json(config_path)
    paths = sorted(source_dir.glob(str(config.get("source_glob") or "*_M1.csv")))
    if not paths:
        raise FileNotFoundError(f"No M1 CSV inputs in {source_dir}")
    frames = [load_instrument(path) for path in paths]
    panel = add_cross_sectional_features(pd.concat(frames, ignore_index=True))
    panel = panel[panel["pair_coverage"] >= int(config.get("minimum_pairs_per_timestamp", 8))]
    slippage = float(config.get("modeled_slippage_pips", 0.25))
    panel["entry_cost_pips"] = panel["spread_pips"] + slippage
    panel["momentum_cost_ratio"] = (
        0.65 * panel["return_5m_pips"].abs() + 0.35 * panel["return_15m_pips"].abs()
    ) / panel["entry_cost_pips"].replace(0, np.nan)
    panel["movement_rank"] = panel.groupby("epoch")["momentum_cost_ratio"].rank(pct=True, ascending=True)
    results: list[dict[str, Any]] = []
    for horizon in config.get("horizons_min") or []:
        labeled = pd.concat(
            [attach_outcomes(group.sort_values("epoch").reset_index(drop=True), int(horizon), slippage) for _, group in panel.groupby("instrument")],
            ignore_index=True,
        )
        for bucket in config.get("cost_buckets") or []:
            results.append(fit_cell(labeled, int(horizon), bucket, config))
    source_manifest = [
        {"name": path.name, "size_bytes": path.stat().st_size, "sha256": digest(path)} for path in paths
    ]
    manifest_hash = hashlib.sha256(json.dumps(source_manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    generated = dt.datetime.now(dt.timezone.utc).isoformat()
    payload = {
        "schema_version": 1,
        "research_id": config["research_id"],
        "generated_utc": generated,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "evidence_class": "already_observed_archive_discovery",
        "instrument_count": len(paths),
        "panel_rows": int(len(panel)),
        "panel_start_utc": panel["timestamp"].min().isoformat(),
        "panel_end_utc": panel["timestamp"].max().isoformat(),
        "config_sha256": digest(config_path),
        "source_code_sha256": digest(Path(__file__)),
        "source_manifest_sha256": manifest_hash,
        "features": FEATURES,
        "results": results,
        "limitations": [
            "short and previously inspected archive",
            "correlated rows are diagnostics rather than independent evidence",
            "selection thresholds were frozen before this run but the same archive cannot confirm a candidate",
            "any selected hypothesis requires a new immutable prospective cohort",
        ],
    }
    atomic_text(output, json.dumps(payload, indent=2, sort_keys=True))
    lines = [
        "# Cost-clearance cross-sectional research",
        "",
        "Archive discovery only. Recorded bid/ask outcomes are used; no row can authorize or promote.",
        "",
        f"- Instruments: {len(paths)}",
        f"- Panel rows after coverage gate: {len(panel):,}",
        f"- Source window: {payload['panel_start_utc']} to {payload['panel_end_utc']}",
        "",
        "| Horizon | Cost bucket | Holdout rows | Clear rate | Clear AUC | Direction on clears | Top-one N | Top-one win | Top-one EV | Basket N | Basket EV | Momentum EV |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    def fmt(value: Any, spec: str = ".3f") -> str:
        return "n/a" if value is None else format(float(value), spec)
    for result in results:
        if result.get("status") != "ok":
            lines.append(f"| {result['horizon_min']}m | {result['cost_bucket']} | {result['status']} | | | | | | | | | |")
            continue
        allocation = result["allocation"]
        top = allocation["learned_top_one"]
        basket = allocation["exactly_three_currency_disjoint"]
        momentum = allocation["momentum_top_one_baseline"]
        lines.append(
            f"| {result['horizon_min']}m | {result['cost_bucket']} | {result['rows']['holdout']:,} | "
            f"{result['clearance']['base_rate']:.1%} | {fmt(result['clearance']['roc_auc'])} | "
            f"{fmt(result['conditional_direction_accuracy'], '.1%')} | {top['n']} | {fmt(top['win_rate'], '.1%')} | "
            f"{fmt(top['average_net_pips'])} | {basket['n']} | {fmt(basket['average_net_pips'])} | "
            f"{fmt(momentum['average_net_pips'])} |"
        )
    lines += ["", "## Candidate-lock gates", ""]
    for result in results:
        if result.get("status") != "ok":
            continue
        gate = result.get("candidate_lock_gate") or {}
        reason = ", ".join(gate.get("reasons") or []) or "all archive diagnostics passed; still requires untouched evidence"
        lines.append(f"- {result['horizon_min']}m / `{result['cost_bucket']}`: eligible=`{str(bool(gate.get('eligible'))).lower()}` — {reason}.")
    lines += [
        "",
        "A positive row is a discovery hypothesis only. It must be frozen and evaluated after the archive high-water mark; a negative row is retained as a control.",
        "",
    ]
    atomic_text(report, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--source-dir", type=Path, default=SOURCE)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args()
    run(args.config, args.source_dir, args.output, args.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
