#!/usr/bin/env python3
"""
Live-style per-pair portfolio replay for OANDA technical research models.

This is a portfolio allocator backtest on top of the existing purged rolling
research folds.  It does not re-score all passing signals equally.  For each
pair it selects the best threshold on the calibration slice, ranks test-time
signals by expected ATR edge times model confidence, and then simulates a
single account that spends margin on the best current opportunities while
limiting duplicate, stale, and churn-heavy rotations.

The P/L is an account proxy because the research labels are ATR-normalized
outcomes, not broker fill paths.  A trade with risk_pct=0.35 and
realized_outcome_atr=1.0 contributes about +0.35% before compounding.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import timedelta
import json
import math
import os
from pathlib import Path
import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from oanda_gpt_training_strategy_manager import (
    CONTINUOUS_RESEARCH_PURGE_MINUTES,
    DIRS,
    ContinuousResearchEngine,
)


SUPPORTED_MODEL_TYPES = {
    "random_forest",
    "extra_trees",
    "hist_gradient_boosting",
    "gradient_boosting",
}


@dataclass
class OpenPosition:
    trade_id: int
    instrument: str
    direction: str
    campaign: str
    open_time: pd.Timestamp
    planned_exit_time: pd.Timestamp
    horizon_minutes: int
    probability: float
    threshold: float
    confidence: float
    expected_move_atr: float
    realized_outcome_atr: float
    rank_score: float
    score_percentile: float
    risk_pct: float
    margin_pct: float
    open_equity: float
    model_type: str
    feature_set: str
    target: str
    outcome: str
    experiment_id: str
    fold: int
    week_start: str
    pair_threshold_rank_score: float


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except Exception:
        return default
    return result if math.isfinite(result) else default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def json_default(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return str(value)


def optional_utc_timestamp(value: str) -> Optional[pd.Timestamp]:
    text = str(value or "").strip()
    if not text:
        return None
    ts = pd.Timestamp(text)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=json_default),
        encoding="utf-8",
    )


def parse_thresholds(text: str) -> List[float]:
    values: List[float] = []
    for part in re.split(r"[, ]+", text.strip()):
        if not part:
            continue
        if ":" in part:
            start_s, stop_s, step_s = part.split(":", 2)
            start = safe_float(start_s)
            stop = safe_float(stop_s)
            step = safe_float(step_s)
            if step <= 0:
                continue
            current = start
            while current <= stop + 1e-9:
                values.append(round(float(current), 6))
                current += step
        else:
            values.append(round(safe_float(part), 6))
    values = sorted({v for v in values if 0.0 < v < 1.0})
    if not values:
        raise ValueError("No valid thresholds supplied")
    return values


def horizon_minutes_from_name(*names: str) -> int:
    for name in names:
        match = re.search(r"_(\d+)$", str(name))
        if match:
            return max(1, int(match.group(1)))
    return 120


def utc_timestamp(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def instrument_direction(row: pd.Series, target: str, outcome: str) -> str:
    name = f"{target} {outcome}".lower()
    if "short" in name and "long" not in name.split("short", 1)[0]:
        return "SHORT"
    if "long" in name:
        return "LONG"
    momentum = safe_float(row.get("momentum_30_atr"), 0.0)
    if "continuation" in name:
        return "LONG" if momentum >= 0.0 else "SHORT"
    if "reversal" in name:
        return "SHORT" if momentum >= 0.0 else "LONG"
    return "LONG" if momentum >= 0.0 else "SHORT"


def instrument_currencies(instrument: str) -> Tuple[str, str]:
    parts = str(instrument).split("_", 1)
    if len(parts) != 2:
        return str(instrument), ""
    return parts[0], parts[1]


def outcome_summary(values: Sequence[float]) -> Dict[str, float]:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    trades = int(len(arr))
    gross_win = float(arr[arr > 0].sum()) if trades else 0.0
    gross_loss = float(abs(arr[arr < 0].sum())) if trades else 0.0
    return {
        "trades": float(trades),
        "mean": float(arr.mean()) if trades else 0.0,
        "median": float(np.median(arr)) if trades else 0.0,
        "win_rate": float((arr > 0).mean()) if trades else 0.0,
        "profit_factor": (
            gross_win / gross_loss
            if gross_loss > 0
            else (gross_win if gross_win > 0 else 0.0)
        ),
        "total": float(arr.sum()) if trades else 0.0,
    }


def choose_pair_threshold(
    engine: ContinuousResearchEngine,
    frame: pd.DataFrame,
    probability: np.ndarray,
    outcome: np.ndarray,
    thresholds: Sequence[float],
    *,
    min_trades: int,
    episode_aware: bool,
) -> Tuple[float, Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for threshold in thresholds:
        selected = probability >= threshold
        positions = (
            engine.episode_positions(frame, probability, selected)
            if episode_aware
            else np.flatnonzero(selected)
        )
        summary = outcome_summary(outcome[positions])
        sample_weight = min(
            1.0,
            math.sqrt(summary["trades"] / max(float(min_trades), 1.0)),
        )
        robust_mean = summary["mean"] * sample_weight
        pf_bonus = min(max(summary["profit_factor"] - 1.0, 0.0), 2.0) * 0.02
        rows.append({
            "threshold": float(threshold),
            "threshold_rank_score": float(robust_mean + pf_bonus),
            **summary,
        })
    ranked = pd.DataFrame(rows)
    eligible = ranked[
        (ranked["trades"] >= max(1, min_trades))
        & (ranked["mean"] > 0.0)
        & (ranked["profit_factor"] >= 1.0)
    ]
    if eligible.empty:
        eligible = ranked[ranked["trades"] >= max(1, min_trades)]
    if eligible.empty:
        eligible = ranked
    best = eligible.sort_values(
        ["threshold_rank_score", "mean", "profit_factor", "trades"],
        ascending=[False, False, False, False],
    ).iloc[0]
    return float(best["threshold"]), {
        key: safe_float(value)
        for key, value in best.to_dict().items()
    }


def load_settings(path: Path, max_pairs: int) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pd.read_csv(path)
    required = {"instrument", "experiment_id", "model_type", "target", "outcome"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Settings CSV missing columns: {sorted(missing)}")
    for column in [
        "rank_score",
        "pair_net_per_trade",
        "pair_total_net_per_validation_week",
    ]:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    sort_columns = [
        column
        for column in ["rank_score", "pair_total_net_per_validation_week"]
        if column in frame.columns
    ]
    if sort_columns:
        frame = frame.sort_values(
            sort_columns,
            ascending=[False] * len(sort_columns),
            na_position="last",
        ).reset_index(drop=True)
    if max_pairs > 0:
        frame = frame.head(max_pairs).copy()
    return frame


def load_experiment_spec(experiment_id: str, row: pd.Series) -> Dict[str, Any]:
    path = DIRS["research_experiments"] / f"{experiment_id}.json"
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        spec = dict(payload.get("spec") or {})
        if spec:
            for key in [
                "evaluation_stage",
                "instrument_subset",
                "validation_weeks",
                "max_train_rows",
            ]:
                value = row.get(key, "")
                if pd.notna(value) and str(value).strip():
                    if key in {"validation_weeks", "max_train_rows"}:
                        spec[key] = safe_int(value, safe_int(spec.get(key), 0))
                    else:
                        spec[key] = str(value)
            return spec
    params: Dict[str, Any] = {}
    raw_params = row.get("parameters", "")
    if isinstance(raw_params, str) and raw_params.strip():
        try:
            params = json.loads(raw_params)
        except Exception:
            params = {}
    return {
        "dataset_kind": "technical_spike",
        "evaluation_stage": "validation",
        "feature_set": str(row.get("feature_set") or "technical_full"),
        "instrument_subset": str(row.get("instrument_subset") or "all"),
        "max_train_rows": safe_int(row.get("max_train_rows"), 250000),
        "model_type": str(row.get("model_type") or "random_forest"),
        "outcome": str(row.get("outcome")),
        "parameters": params,
        "research_role": str(row.get("research_role") or "return_curve_candidate"),
        "target": str(row.get("target")),
        "validation_weeks": safe_int(row.get("validation_weeks"), 8),
    }


def candidate_aux_columns(frame: pd.DataFrame) -> List[str]:
    columns = [
        "momentum_30_atr",
        "spread_pips",
        "atr240_pips",
        "pair_taxonomy_primary",
        "regime_primary",
        "is_asia_session",
        "is_london_session",
        "is_new_york_session",
        "is_london_ny_overlap",
    ]
    return [column for column in columns if column in frame.columns]


def prepare_research_frame(
    frame: pd.DataFrame,
    features: Sequence[str],
    target: str,
    outcome: str,
) -> pd.DataFrame:
    needed = list(dict.fromkeys(["time_utc", "instrument", *features, target, outcome]))
    existing = [column for column in needed if column in frame.columns]
    frame = frame[existing].copy()
    frame["time_utc"] = pd.to_datetime(frame["time_utc"], utc=True, errors="coerce")
    numeric_columns = [column for column in [*features, target, outcome] if column in frame]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["time_utc", "instrument", *numeric_columns])
    frame = frame.sort_values("time_utc").reset_index(drop=True)
    frame["week_start"] = (
        frame["time_utc"].dt.normalize()
        - pd.to_timedelta(frame["time_utc"].dt.weekday, unit="D")
    )
    return frame


def generate_candidates(
    settings: pd.DataFrame,
    *,
    thresholds: Sequence[float],
    min_pair_calibration_rows: int,
    min_pair_calibration_trades: int,
    min_expected_move_atr: float,
    episode_aware: bool,
    skip_unsupported: bool,
    min_train_rows: int,
    min_calibration_rows: int,
    min_test_rows: int,
    candidate_start_utc: Optional[pd.Timestamp] = None,
    candidate_end_utc: Optional[pd.Timestamp] = None,
    all_test_weeks: bool = False,
    holdout_weeks_override: int = 0,
    train_lookback_days: int = 0,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, List[Dict[str, Any]]]:
    engine = ContinuousResearchEngine()
    candidate_rows: List[Dict[str, Any]] = []
    threshold_rows: List[Dict[str, Any]] = []
    model_rows: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for experiment_id, group in settings.groupby("experiment_id", sort=False):
        first = group.iloc[0]
        spec = load_experiment_spec(str(experiment_id), first)
        model_type = str(spec.get("model_type") or first.get("model_type") or "")
        pairs = sorted({str(value) for value in group["instrument"].tolist()})
        if skip_unsupported and model_type not in SUPPORTED_MODEL_TYPES:
            skipped.append({
                "experiment_id": experiment_id,
                "pairs": pairs,
                "reason": f"unsupported_model_type:{model_type}",
            })
            continue
        target = str(spec["target"])
        outcome = str(spec["outcome"])
        try:
            raw_frame, dataset_hash, dataset_path, features = engine.load_dataset(spec)
            frame = prepare_research_frame(raw_frame, features, target, outcome)
            evaluation_stage, holdout_weeks, max_train_rows = engine.validation_settings(spec)
            model_rows.append({
                "experiment_id": experiment_id,
                "pairs": ",".join(pairs),
                "model_type": model_type,
                "feature_set": spec.get("feature_set"),
                "target": target,
                "outcome": outcome,
                "dataset_rows": int(len(frame)),
                "dataset_hash": dataset_hash,
                "dataset_path": str(dataset_path),
                "holdout_weeks": int(holdout_weeks),
                "max_train_rows": int(max_train_rows),
                "status": "started",
            })
        except Exception as exc:
            skipped.append({
                "experiment_id": experiment_id,
                "pairs": pairs,
                "reason": f"load_failed:{type(exc).__name__}:{exc}",
            })
            continue

        aux_columns = candidate_aux_columns(frame)
        keep_columns = list(dict.fromkeys([
            "time_utc",
            "week_start",
            "instrument",
            target,
            outcome,
            *aux_columns,
        ]))
        valid_folds = 0
        purge = pd.Timedelta(minutes=CONTINUOUS_RESEARCH_PURGE_MINUTES)
        window_holdout_weeks = int(holdout_weeks)
        if all_test_weeks or candidate_start_utc is not None or candidate_end_utc is not None:
            window_holdout_weeks = max(
                window_holdout_weeks,
                int(frame["week_start"].nunique()),
            )
        if holdout_weeks_override > 0:
            window_holdout_weeks = max(
                window_holdout_weeks,
                int(holdout_weeks_override),
            )
        windows = engine.rolling_windows(frame, window_holdout_weeks)
        train_row_counts: List[int] = []
        for fold_number, (train_end, calibration_start, test_start) in enumerate(windows, 1):
            test_week_end = test_start + pd.Timedelta(days=7)
            if candidate_start_utc is not None and test_week_end <= candidate_start_utc:
                continue
            if candidate_end_utc is not None and test_start >= candidate_end_utc:
                continue
            train = frame[frame["time_utc"] < train_end]
            calibration = frame[
                (frame["time_utc"] >= calibration_start)
                & (frame["time_utc"] < test_start - purge)
            ]
            test = frame[frame["week_start"] == test_start].reset_index(drop=True)
            if train_lookback_days > 0:
                train_start = train_end - pd.Timedelta(days=int(train_lookback_days))
                train = train[train["time_utc"] >= train_start]
            if len(train) > max_train_rows:
                train = train.tail(max_train_rows)
            if (
                len(train) < min_train_rows
                or len(calibration) < min_calibration_rows
                or len(test) < min_test_rows
            ):
                continue
            y_train = train[target].astype(int)
            if y_train.nunique() < 2:
                continue
            train_row_counts.append(int(len(train)))
            try:
                model = engine.estimator(spec)
                model.fit(train[features], y_train)
                calibration_probability = model.predict_proba(calibration[features])[:, 1]
                test_probability = model.predict_proba(test[features])[:, 1]
            except Exception as exc:
                skipped.append({
                    "experiment_id": experiment_id,
                    "pairs": pairs,
                    "fold": fold_number,
                    "reason": f"fit_or_predict_failed:{type(exc).__name__}:{exc}",
                })
                continue

            valid_folds += 1
            calibration_scored = calibration[keep_columns].copy()
            calibration_scored["_probability"] = calibration_probability
            test_scored = test[keep_columns].copy()
            test_scored["_probability"] = test_probability
            for _, pair_row in group.iterrows():
                pair = str(pair_row["instrument"])
                pair_cal = calibration_scored[
                    calibration_scored["instrument"] == pair
                ].reset_index(drop=True)
                pair_test = test_scored[
                    test_scored["instrument"] == pair
                ].reset_index(drop=True)
                if len(pair_cal) < min_pair_calibration_rows or pair_test.empty:
                    threshold_rows.append({
                        "experiment_id": experiment_id,
                        "instrument": pair,
                        "fold": fold_number,
                        "week_start": test_start.isoformat(),
                        "status": "insufficient_pair_rows",
                        "calibration_rows": int(len(pair_cal)),
                        "test_rows": int(len(pair_test)),
                    })
                    continue
                cal_probability = pair_cal["_probability"].to_numpy(dtype=float)
                cal_outcome = pair_cal[outcome].to_numpy(dtype=float)
                threshold, selection = choose_pair_threshold(
                    engine,
                    pair_cal,
                    cal_probability,
                    cal_outcome,
                    thresholds,
                    min_trades=min_pair_calibration_trades,
                    episode_aware=episode_aware,
                )
                expected_move_atr = max(0.0, safe_float(selection.get("mean")))
                threshold_rows.append({
                    "experiment_id": experiment_id,
                    "instrument": pair,
                    "fold": fold_number,
                    "week_start": test_start.isoformat(),
                    "calibration_week": calibration_start.isoformat(),
                    "status": "selected",
                    "threshold": threshold,
                    "expected_move_atr": expected_move_atr,
                    "calibration_rows": int(len(pair_cal)),
                    "test_rows": int(len(pair_test)),
                    **{f"calibration_{key}": value for key, value in selection.items()},
                })
                if expected_move_atr < min_expected_move_atr:
                    continue
                test_probability_pair = pair_test["_probability"].to_numpy(dtype=float)
                selected = test_probability_pair >= threshold
                positions = (
                    engine.episode_positions(pair_test, test_probability_pair, selected)
                    if episode_aware
                    else np.flatnonzero(selected)
                )
                if not len(positions):
                    continue
                horizon = horizon_minutes_from_name(target, outcome)
                for _, row in pair_test.iloc[list(positions)].iterrows():
                    probability = safe_float(row["_probability"])
                    confidence = probability
                    rank_score = expected_move_atr * confidence
                    time_utc = utc_timestamp(row["time_utc"])
                    realized_outcome = safe_float(row[outcome])
                    direction = instrument_direction(row, target, outcome)
                    candidate_rows.append({
                        "time_utc": time_utc.isoformat(),
                        "instrument": pair,
                        "direction": direction,
                        "campaign": f"{pair}:{direction}",
                        "planned_exit_time": (
                            time_utc + pd.Timedelta(minutes=horizon)
                        ).isoformat(),
                        "horizon_minutes": int(horizon),
                        "fold": int(fold_number),
                        "week_start": test_start.isoformat(),
                        "experiment_id": str(experiment_id),
                        "model_type": model_type,
                        "feature_set": str(spec.get("feature_set") or ""),
                        "target": target,
                        "outcome": outcome,
                        "threshold": float(threshold),
                        "probability": probability,
                        "confidence": confidence,
                        "expected_move_atr": expected_move_atr,
                        "realized_outcome_atr": realized_outcome,
                        "rank_score": rank_score,
                        "pair_threshold_rank_score": safe_float(
                            selection.get("threshold_rank_score")
                        ),
                        "pair_report_rank_score": safe_float(pair_row.get("rank_score")),
                        "pair_report_net_per_trade": safe_float(
                            pair_row.get("pair_net_per_trade")
                        ),
                        "momentum_30_atr": safe_float(row.get("momentum_30_atr")),
                        "spread_pips": safe_float(row.get("spread_pips"), math.nan),
                        "atr240_pips": safe_float(row.get("atr240_pips"), math.nan),
                        "pair_taxonomy_primary": str(
                            row.get("pair_taxonomy_primary", "")
                        ),
                        "regime_primary": str(row.get("regime_primary", "")),
                    })
        if model_rows:
            model_rows[-1]["valid_folds"] = int(valid_folds)
            model_rows[-1]["train_lookback_days"] = int(train_lookback_days)
            model_rows[-1]["min_fold_train_rows"] = int(min(train_row_counts)) if train_row_counts else 0
            model_rows[-1]["max_fold_train_rows"] = int(max(train_row_counts)) if train_row_counts else 0
            model_rows[-1]["mean_fold_train_rows"] = (
                float(np.mean(train_row_counts)) if train_row_counts else 0.0
            )
            model_rows[-1]["status"] = "finished"

    candidates = pd.DataFrame(candidate_rows)
    thresholds_frame = pd.DataFrame(threshold_rows)
    models_frame = pd.DataFrame(model_rows)
    if not candidates.empty:
        candidates["time_utc"] = pd.to_datetime(candidates["time_utc"], utc=True)
        candidates["planned_exit_time"] = pd.to_datetime(
            candidates["planned_exit_time"],
            utc=True,
        )
        if candidate_start_utc is not None:
            candidates = candidates[candidates["time_utc"] >= candidate_start_utc].copy()
        if candidate_end_utc is not None:
            candidates = candidates[candidates["time_utc"] < candidate_end_utc].copy()
    if not candidates.empty:
        candidates = candidates.sort_values(
            ["time_utc", "rank_score", "probability"],
            ascending=[True, False, False],
        ).reset_index(drop=True)
        ranks = candidates["rank_score"].rank(method="average", pct=True)
        candidates["score_percentile"] = ranks.astype(float)
    return candidates, thresholds_frame, models_frame, skipped


class PortfolioReplay:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.open_positions: List[OpenPosition] = []
        self.closed_rows: List[Dict[str, Any]] = []
        self.blocked_rows: List[Dict[str, Any]] = []
        self.equity_curve: List[Dict[str, Any]] = []
        self.trade_id = 0
        self.equity = float(args.start_equity)
        self.last_entry_by_campaign: Dict[str, pd.Timestamp] = {}
        self.last_entry_by_pair: Dict[str, pd.Timestamp] = {}
        self.last_close_by_pair: Dict[str, pd.Timestamp] = {}
        self.rotations_by_time: Dict[str, int] = {}
        self.equity_peak = float(args.start_equity)
        self.pair_loss_streak: Dict[str, int] = {}
        self.pair_cooldown_until: Dict[str, pd.Timestamp] = {}
        self.pair_total_pnl: Dict[str, float] = {}
        self.daily_pnl: Dict[str, float] = {}
        self.pair_daily_pnl: Dict[Tuple[str, str], float] = {}

    def margin_used_pct(self) -> float:
        return float(sum(position.margin_pct for position in self.open_positions))

    def open_risk_pct(self) -> float:
        return float(sum(position.risk_pct for position in self.open_positions))

    def current_drawdown_pct(self) -> float:
        peak = max(self.equity_peak, 1e-9)
        return max(0.0, (1.0 - (self.equity / peak)) * 100.0)

    def drawdown_risk_multiplier(self) -> float:
        drawdown = self.current_drawdown_pct()
        start = max(0.0, self.args.drawdown_risk_throttle_start_pct)
        full = max(start + 0.01, self.args.drawdown_risk_throttle_full_pct)
        floor = min(1.0, max(0.0, self.args.drawdown_risk_min_multiplier))
        if drawdown <= start:
            return 1.0
        if drawdown >= full:
            return floor
        progress = (drawdown - start) / (full - start)
        return 1.0 - progress * (1.0 - floor)

    def effective_max_open_risk_pct(self) -> float:
        if self.args.max_open_risk_pct <= 0.0:
            return 1e9
        multiplier = max(
            self.args.drawdown_open_risk_min_multiplier,
            self.drawdown_risk_multiplier(),
        )
        return self.args.max_open_risk_pct * multiplier

    def effective_target_open_risk_pct(self) -> float:
        if self.args.target_open_risk_pct <= 0.0:
            return 1e9
        multiplier = max(
            self.args.drawdown_open_risk_min_multiplier,
            self.drawdown_risk_multiplier(),
        )
        return self.args.target_open_risk_pct * multiplier

    def currency_risk_pct(self, currency: str) -> float:
        if not currency:
            return 0.0
        total = 0.0
        for position in self.open_positions:
            base, quote = instrument_currencies(position.instrument)
            if currency in {base, quote}:
                total += position.risk_pct
        return float(total)

    def currency_risk_violations(
        self,
        instrument: str,
        risk_pct: float,
    ) -> List[str]:
        if self.args.max_currency_risk_pct <= 0.0:
            return []
        violations = []
        for currency in instrument_currencies(instrument):
            if not currency:
                continue
            after = self.currency_risk_pct(currency) + risk_pct
            if after > self.args.max_currency_risk_pct:
                violations.append(f"{currency}:{after:.2f}")
        return violations

    def date_key(self, time_utc: pd.Timestamp) -> str:
        return utc_timestamp(time_utc).date().isoformat()

    def daily_loss_pct(self, time_utc: pd.Timestamp) -> float:
        if self.args.max_daily_loss_pct <= 0.0:
            return 0.0
        pnl = self.daily_pnl.get(self.date_key(time_utc), 0.0)
        return max(0.0, (-pnl / max(self.args.start_equity, 1e-9)) * 100.0)

    def pair_daily_loss_pct(self, pair: str, time_utc: pd.Timestamp) -> float:
        if self.args.max_pair_daily_loss_pct <= 0.0:
            return 0.0
        pnl = self.pair_daily_pnl.get((self.date_key(time_utc), pair), 0.0)
        return max(0.0, (-pnl / max(self.args.start_equity, 1e-9)) * 100.0)

    def pair_total_loss_pct(self, pair: str) -> float:
        if self.args.max_pair_total_loss_pct <= 0.0:
            return 0.0
        pnl = self.pair_total_pnl.get(pair, 0.0)
        return max(0.0, (-pnl / max(self.args.start_equity, 1e-9)) * 100.0)

    def record_curve(self, time_utc: pd.Timestamp) -> None:
        self.equity_peak = max(self.equity_peak, self.equity)
        self.equity_curve.append({
            "time_utc": utc_timestamp(time_utc).isoformat(),
            "equity": float(self.equity),
            "margin_used_pct": self.margin_used_pct(),
            "open_risk_pct": self.open_risk_pct(),
            "drawdown_pct": self.current_drawdown_pct(),
            "risk_multiplier": self.drawdown_risk_multiplier(),
            "open_positions": int(len(self.open_positions)),
        })

    def position_age_minutes(
        self,
        position: OpenPosition,
        current_time: pd.Timestamp,
    ) -> float:
        return max(
            0.0,
            (utc_timestamp(current_time) - position.open_time).total_seconds() / 60.0,
        )

    def risk_for_candidate(self, candidate: pd.Series) -> float:
        percentile = safe_float(candidate.get("score_percentile"))
        probability = safe_float(candidate.get("probability"))
        if percentile >= 0.97:
            base = self.args.max_risk_pct
        elif percentile >= 0.90:
            base = min(self.args.max_risk_pct, self.args.high_risk_pct)
        elif percentile >= 0.75:
            base = min(self.args.high_risk_pct, self.args.medium_risk_pct)
        elif percentile >= 0.50:
            base = min(self.args.medium_risk_pct, self.args.low_risk_pct)
        else:
            base = self.args.min_risk_pct
        confidence_scale = min(1.2, max(0.6, probability / 0.65))
        risk = min(self.args.max_risk_pct, base * confidence_scale)
        risk *= self.drawdown_risk_multiplier()
        floor = (
            self.args.min_risk_pct
            if self.drawdown_risk_multiplier() >= 0.999
            else self.args.min_throttled_risk_pct
        )
        return max(floor, min(self.args.max_risk_pct, risk))

    def close_position(
        self,
        position: OpenPosition,
        close_time: pd.Timestamp,
        reason: str,
    ) -> None:
        close_time = utc_timestamp(close_time)
        total_seconds = max(
            60.0,
            (position.planned_exit_time - position.open_time).total_seconds(),
        )
        held_fraction = min(
            1.0,
            max(
                0.0,
                (close_time - position.open_time).total_seconds() / total_seconds,
            ),
        )
        outcome_fraction = 1.0 if reason == "scheduled_exit" else held_fraction
        realized_atr = position.realized_outcome_atr * outcome_fraction
        pnl = position.open_equity * (position.risk_pct / 100.0) * realized_atr
        self.equity += pnl
        self.equity_peak = max(self.equity_peak, self.equity)
        day = self.date_key(close_time)
        self.daily_pnl[day] = self.daily_pnl.get(day, 0.0) + pnl
        pair_day_key = (day, position.instrument)
        self.pair_daily_pnl[pair_day_key] = (
            self.pair_daily_pnl.get(pair_day_key, 0.0) + pnl
        )
        self.pair_total_pnl[position.instrument] = (
            self.pair_total_pnl.get(position.instrument, 0.0) + pnl
        )
        pnl_pct = (pnl / position.open_equity) * 100.0
        if pnl < 0.0:
            streak = self.pair_loss_streak.get(position.instrument, 0) + 1
            self.pair_loss_streak[position.instrument] = streak
            if (
                self.args.pair_loss_streak_cooldown_trades > 0
                and streak >= self.args.pair_loss_streak_cooldown_trades
            ):
                self.pair_cooldown_until[position.instrument] = max(
                    self.pair_cooldown_until.get(
                        position.instrument,
                        pd.Timestamp.min.tz_localize("UTC"),
                    ),
                    close_time
                    + pd.Timedelta(minutes=self.args.pair_loss_cooldown_minutes),
                )
        else:
            self.pair_loss_streak[position.instrument] = 0
        if (
            self.args.pair_large_loss_cooldown_pct > 0.0
            and pnl_pct <= -abs(self.args.pair_large_loss_cooldown_pct)
        ):
            self.pair_cooldown_until[position.instrument] = max(
                self.pair_cooldown_until.get(
                    position.instrument,
                    pd.Timestamp.min.tz_localize("UTC"),
                ),
                close_time + pd.Timedelta(minutes=self.args.pair_loss_cooldown_minutes),
            )
        row = asdict(position)
        row.update({
            "close_time": close_time.isoformat(),
            "close_reason": reason,
            "held_minutes": float(
                max(0.0, (close_time - position.open_time).total_seconds() / 60.0)
            ),
            "held_fraction": float(held_fraction),
            "outcome_fraction": float(outcome_fraction),
            "realized_atr_at_close": float(realized_atr),
            "pnl": float(pnl),
            "pnl_pct_of_open_equity": float(pnl_pct),
            "equity_after_close": float(self.equity),
            "drawdown_after_close_pct": float(self.current_drawdown_pct()),
        })
        self.closed_rows.append(row)
        self.last_close_by_pair[position.instrument] = close_time
        self.open_positions = [
            open_position
            for open_position in self.open_positions
            if open_position.trade_id != position.trade_id
        ]

    def close_due_positions(self, current_time: pd.Timestamp) -> None:
        due = [
            position
            for position in self.open_positions
            if position.planned_exit_time <= utc_timestamp(current_time)
        ]
        for position in sorted(due, key=lambda item: item.planned_exit_time):
            self.close_position(position, position.planned_exit_time, "scheduled_exit")

    def block(self, candidate: pd.Series, reason: str, detail: str = "") -> None:
        self.blocked_rows.append({
            "time_utc": utc_timestamp(candidate["time_utc"]).isoformat(),
            "instrument": str(candidate["instrument"]),
            "direction": str(candidate["direction"]),
            "reason": reason,
            "detail": detail,
            "rank_score": safe_float(candidate.get("rank_score")),
            "probability": safe_float(candidate.get("probability")),
            "expected_move_atr": safe_float(candidate.get("expected_move_atr")),
            "margin_used_pct": self.margin_used_pct(),
            "open_risk_pct": self.open_risk_pct(),
            "drawdown_pct": self.current_drawdown_pct(),
            "daily_loss_pct": self.daily_loss_pct(candidate["time_utc"]),
            "open_positions": int(len(self.open_positions)),
        })

    def maybe_rotate_for_candidate(
        self,
        candidate: pd.Series,
        risk_pct: float,
        margin_pct: float,
    ) -> None:
        current_time = utc_timestamp(candidate["time_utc"])
        key = current_time.isoformat()
        rotations = self.rotations_by_time.get(key, 0)
        required_margin = self.margin_used_pct() + margin_pct
        required_open_risk = self.open_risk_pct() + risk_pct
        candidate_score = safe_float(candidate.get("rank_score"))
        candidate_percentile = safe_float(candidate.get("score_percentile"))
        margin_pressure = required_margin > self.args.target_margin_pct
        risk_pressure = required_open_risk > self.effective_target_open_risk_pct()
        capacity_pressure = len(self.open_positions) >= self.args.max_open_positions
        quality_rotation = (
            candidate_percentile >= self.args.stale_rotation_min_score_percentile
            and len(self.open_positions)
            >= max(1, int(self.args.max_open_positions * self.args.stale_rotation_open_share))
        )
        if (
            required_margin <= self.args.hard_margin_pct
            and required_open_risk <= self.effective_max_open_risk_pct()
            and len(self.open_positions) < self.args.max_open_positions
            and not margin_pressure
            and not risk_pressure
            and not quality_rotation
        ):
            return
        eligible: List[OpenPosition] = []
        for position in self.open_positions:
            age = self.position_age_minutes(position, current_time)
            stale = age >= self.args.stale_minutes
            if age < self.args.min_rotation_age_minutes:
                continue
            if not stale and not (margin_pressure or risk_pressure or capacity_pressure):
                continue
            if (
                not stale
                and age < self.args.stale_minutes
                and position.instrument == candidate["instrument"]
            ):
                continue
            if candidate_score < position.rank_score * self.args.rotation_score_multiplier:
                continue
            eligible.append(position)
        eligible.sort(key=lambda item: (item.rank_score, -self.position_age_minutes(item, current_time)))
        for position in eligible:
            if rotations >= self.args.max_rotations_per_timestamp:
                break
            self.close_position(position, current_time, "stale_rotation")
            rotations += 1
            required_margin = self.margin_used_pct() + margin_pct
            if (
                required_margin <= self.args.target_margin_pct
                and self.open_risk_pct() + risk_pct <= self.effective_target_open_risk_pct()
                and len(self.open_positions) < self.args.max_open_positions
                and not capacity_pressure
            ):
                break
            if quality_rotation and not margin_pressure and not capacity_pressure:
                break
        self.rotations_by_time[key] = rotations

    def open_candidate(self, candidate: pd.Series) -> None:
        current_time = utc_timestamp(candidate["time_utc"])
        self.close_due_positions(current_time)
        if safe_float(candidate.get("expected_move_atr")) <= 0.0:
            self.block(candidate, "non_positive_expected_edge")
            return
        rank_score = safe_float(candidate.get("rank_score"))
        score_percentile = safe_float(candidate.get("score_percentile"))
        probability = safe_float(candidate.get("probability"))
        if rank_score < self.args.min_rank_score:
            self.block(candidate, "below_min_rank_score", f"{rank_score:.4f}")
            return
        if score_percentile < self.args.min_score_percentile:
            self.block(
                candidate,
                "below_min_score_percentile",
                f"{score_percentile:.4f}",
            )
            return
        if probability < self.args.min_probability:
            self.block(candidate, "below_min_probability", f"{probability:.4f}")
            return
        campaign = str(candidate["campaign"])
        pair = str(candidate["instrument"])
        direction = str(candidate["direction"])
        pair_cooldown = self.pair_cooldown_until.get(pair)
        if pair_cooldown is not None and current_time < pair_cooldown:
            self.block(
                candidate,
                "pair_loss_cooldown",
                pair_cooldown.isoformat(),
            )
            return
        if (
            self.args.max_daily_loss_pct > 0.0
            and self.daily_loss_pct(current_time) >= self.args.max_daily_loss_pct
            and score_percentile < self.args.daily_loss_bypass_score_percentile
        ):
            self.block(
                candidate,
                "account_daily_loss_stop",
                f"{self.daily_loss_pct(current_time):.2f}%",
            )
            return
        if (
            self.args.max_pair_daily_loss_pct > 0.0
            and self.pair_daily_loss_pct(pair, current_time)
            >= self.args.max_pair_daily_loss_pct
        ):
            self.block(
                candidate,
                "pair_daily_loss_stop",
                f"{self.pair_daily_loss_pct(pair, current_time):.2f}%",
            )
            return
        if (
            self.args.max_pair_total_loss_pct > 0.0
            and self.pair_total_loss_pct(pair) >= self.args.max_pair_total_loss_pct
        ):
            self.block(
                candidate,
                "pair_total_loss_stop",
                f"{self.pair_total_loss_pct(pair):.2f}%",
            )
            return
        drawdown = self.current_drawdown_pct()
        if (
            self.args.drawdown_entry_halt_pct > 0.0
            and drawdown >= self.args.drawdown_entry_halt_pct
            and score_percentile < self.args.drawdown_halt_bypass_score_percentile
        ):
            self.block(candidate, "drawdown_entry_halt", f"{drawdown:.2f}%")
            return
        last_campaign = self.last_entry_by_campaign.get(campaign)
        if last_campaign is not None:
            minutes = (current_time - last_campaign).total_seconds() / 60.0
            if minutes < self.args.min_campaign_entry_gap_minutes:
                self.block(candidate, "campaign_cooldown", f"{minutes:.1f} minutes")
                return
        last_pair = self.last_entry_by_pair.get(pair)
        if last_pair is not None:
            minutes = (current_time - last_pair).total_seconds() / 60.0
            if minutes < self.args.min_pair_entry_gap_minutes:
                self.block(candidate, "pair_entry_cooldown", f"{minutes:.1f} minutes")
                return
        last_close = self.last_close_by_pair.get(pair)
        if last_close is not None:
            minutes = (current_time - last_close).total_seconds() / 60.0
            if minutes < self.args.min_after_close_gap_minutes:
                self.block(candidate, "after_close_cooldown", f"{minutes:.1f} minutes")
                return
        same_pair = [
            position for position in self.open_positions if position.instrument == pair
        ]
        same_campaign = [
            position for position in same_pair if position.direction == direction
        ]
        opposite = [
            position for position in same_pair if position.direction != direction
        ]
        if len(same_campaign) >= self.args.max_same_campaign_positions:
            self.block(candidate, "duplicate_campaign_open")
            return
        if len(same_pair) >= self.args.max_same_pair_positions and not opposite:
            self.block(candidate, "same_pair_cap")
            return
        if opposite:
            candidate_score = safe_float(candidate.get("rank_score"))
            best_opposite_score = max(position.rank_score for position in opposite)
            oldest_opposite_age = max(
                self.position_age_minutes(position, current_time)
                for position in opposite
            )
            if (
                candidate_score < best_opposite_score * self.args.opposite_rotation_multiplier
                or oldest_opposite_age < self.args.min_opposite_rotation_age_minutes
            ):
                self.block(
                    candidate,
                    "opposite_position_open",
                    f"candidate_score={candidate_score:.4f} opposite_score={best_opposite_score:.4f} age={oldest_opposite_age:.1f}",
                )
                return
            for position in list(opposite):
                self.close_position(position, current_time, "opposite_rotation")
        risk_pct = self.risk_for_candidate(candidate)
        margin_pct = risk_pct * self.args.margin_pct_per_risk_pct
        if self.open_risk_pct() + risk_pct > self.effective_target_open_risk_pct():
            if score_percentile < self.args.above_target_min_score_percentile:
                self.block(candidate, "below_quality_for_above_target_risk")
                return
        if self.margin_used_pct() + margin_pct > self.args.target_margin_pct:
            if score_percentile < self.args.above_target_min_score_percentile:
                self.block(candidate, "below_quality_for_above_target_margin")
                return
        self.maybe_rotate_for_candidate(candidate, risk_pct, margin_pct)
        if len(self.open_positions) >= self.args.max_open_positions:
            self.block(candidate, "max_open_positions")
            return
        if self.open_risk_pct() + risk_pct > self.effective_max_open_risk_pct():
            self.block(
                candidate,
                "open_risk_cap",
                f"{self.open_risk_pct() + risk_pct:.2f}>{self.effective_max_open_risk_pct():.2f}",
            )
            return
        currency_violations = self.currency_risk_violations(pair, risk_pct)
        if currency_violations:
            self.block(candidate, "currency_risk_cap", ",".join(currency_violations))
            return
        if self.margin_used_pct() + margin_pct > self.args.hard_margin_pct:
            self.block(candidate, "hard_margin_cap")
            return
        if self.margin_used_pct() + margin_pct > self.args.emergency_margin_pct:
            self.block(candidate, "emergency_margin_cap")
            return
        self.trade_id += 1
        position = OpenPosition(
            trade_id=self.trade_id,
            instrument=pair,
            direction=direction,
            campaign=campaign,
            open_time=current_time,
            planned_exit_time=utc_timestamp(candidate["planned_exit_time"]),
            horizon_minutes=safe_int(candidate.get("horizon_minutes"), 120),
            probability=safe_float(candidate.get("probability")),
            threshold=safe_float(candidate.get("threshold")),
            confidence=safe_float(candidate.get("confidence")),
            expected_move_atr=safe_float(candidate.get("expected_move_atr")),
            realized_outcome_atr=safe_float(candidate.get("realized_outcome_atr")),
            rank_score=safe_float(candidate.get("rank_score")),
            score_percentile=safe_float(candidate.get("score_percentile")),
            risk_pct=risk_pct,
            margin_pct=margin_pct,
            open_equity=float(self.equity),
            model_type=str(candidate.get("model_type")),
            feature_set=str(candidate.get("feature_set")),
            target=str(candidate.get("target")),
            outcome=str(candidate.get("outcome")),
            experiment_id=str(candidate.get("experiment_id")),
            fold=safe_int(candidate.get("fold")),
            week_start=str(candidate.get("week_start")),
            pair_threshold_rank_score=safe_float(
                candidate.get("pair_threshold_rank_score")
            ),
        )
        self.open_positions.append(position)
        self.last_entry_by_campaign[campaign] = current_time
        self.last_entry_by_pair[pair] = current_time

    def run(self, candidates: pd.DataFrame) -> Dict[str, Any]:
        if candidates.empty:
            return self.summary(candidates)
        for time_utc, group in candidates.groupby("time_utc", sort=True):
            current_time = utc_timestamp(time_utc)
            self.close_due_positions(current_time)
            ordered = group.sort_values(
                ["rank_score", "probability"],
                ascending=[False, False],
            )
            for _, candidate in ordered.iterrows():
                self.open_candidate(candidate)
            self.record_curve(current_time)
        for position in sorted(
            list(self.open_positions),
            key=lambda item: item.planned_exit_time,
        ):
            self.close_position(position, position.planned_exit_time, "scheduled_exit")
            self.record_curve(position.planned_exit_time)
        return self.summary(candidates)

    def summary(self, candidates: pd.DataFrame) -> Dict[str, Any]:
        trades = pd.DataFrame(self.closed_rows)
        blocked = pd.DataFrame(self.blocked_rows)
        curve = pd.DataFrame(self.equity_curve)
        if not curve.empty:
            curve["running_max"] = curve["equity"].cummax()
            curve["drawdown_pct"] = (
                (curve["equity"] / curve["running_max"].clip(lower=1e-9)) - 1.0
            ) * 100.0
            max_drawdown_pct = float(abs(curve["drawdown_pct"].min()))
            avg_margin_pct = float(curve["margin_used_pct"].mean())
            max_margin_pct = float(curve["margin_used_pct"].max())
            avg_open_risk_pct = float(curve["open_risk_pct"].mean())
            max_open_risk_pct = float(curve["open_risk_pct"].max())
            min_risk_multiplier = float(curve["risk_multiplier"].min())
            samples_above_target = float(
                (curve["margin_used_pct"] >= self.args.target_margin_pct).mean()
            )
        else:
            max_drawdown_pct = 0.0
            avg_margin_pct = 0.0
            max_margin_pct = 0.0
            avg_open_risk_pct = 0.0
            max_open_risk_pct = 0.0
            min_risk_multiplier = 1.0
            samples_above_target = 0.0
        if trades.empty:
            gross_profit = 0.0
            gross_loss = 0.0
            profit_factor = 0.0
            win_rate = 0.0
            avg_pnl_pct = 0.0
            avg_risk_pct = 0.0
            avg_realized_atr = 0.0
        else:
            gross_profit = float(trades.loc[trades["pnl"] > 0, "pnl"].sum())
            gross_loss = float(abs(trades.loc[trades["pnl"] < 0, "pnl"].sum()))
            profit_factor = (
                gross_profit / gross_loss
                if gross_loss > 0.0
                else (gross_profit if gross_profit > 0.0 else 0.0)
            )
            win_rate = float((trades["pnl"] > 0.0).mean())
            avg_pnl_pct = float(trades["pnl_pct_of_open_equity"].mean())
            avg_risk_pct = float(trades["risk_pct"].mean())
            avg_realized_atr = float(trades["realized_atr_at_close"].mean())
        candidate_start = (
            candidates["time_utc"].min().isoformat()
            if not candidates.empty
            else ""
        )
        candidate_end = (
            candidates["time_utc"].max().isoformat()
            if not candidates.empty
            else ""
        )
        return {
            "candidate_rows": int(len(candidates)),
            "candidate_start_utc": candidate_start,
            "candidate_end_utc": candidate_end,
            "opened_trades": int(len(trades)),
            "blocked_candidates": int(len(blocked)),
            "unique_pairs_traded": int(trades["instrument"].nunique()) if not trades.empty else 0,
            "start_equity": float(self.args.start_equity),
            "final_equity": float(self.equity),
            "return_pct": float((self.equity / self.args.start_equity - 1.0) * 100.0),
            "max_drawdown_pct": max_drawdown_pct,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "avg_pnl_pct_of_open_equity": avg_pnl_pct,
            "avg_risk_pct": avg_risk_pct,
            "avg_realized_atr_at_close": avg_realized_atr,
            "avg_margin_used_pct": avg_margin_pct,
            "max_margin_used_pct": max_margin_pct,
            "avg_open_risk_pct": avg_open_risk_pct,
            "max_open_risk_pct": max_open_risk_pct,
            "min_drawdown_risk_multiplier": min_risk_multiplier,
            "curve_samples_at_or_above_target_margin_share": samples_above_target,
            "scheduled_exits": int(
                (trades["close_reason"] == "scheduled_exit").sum()
            ) if not trades.empty else 0,
            "opposite_rotations": int(
                (trades["close_reason"] == "opposite_rotation").sum()
            ) if not trades.empty else 0,
            "stale_rotations": int(
                (trades["close_reason"] == "stale_rotation").sum()
            ) if not trades.empty else 0,
            "blocked_by_reason": (
                blocked["reason"].value_counts().to_dict()
                if not blocked.empty
                else {}
            ),
            "assumption": (
                "ATR-normalized account proxy: pnl = open_equity * risk_pct * "
                "realized_outcome_atr; early rotations use linear pro-rata outcome."
            ),
        }


def write_outputs(
    output_dir: Path,
    summary: Dict[str, Any],
    candidates: pd.DataFrame,
    thresholds: pd.DataFrame,
    model_runs: pd.DataFrame,
    skipped: List[Dict[str, Any]],
    replay: PortfolioReplay,
    args: argparse.Namespace,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "summary.json", {
        **summary,
        "config": vars(args),
        "skipped": skipped,
    })
    candidates.to_csv(output_dir / "candidate_rows.csv", index=False)
    thresholds.to_csv(output_dir / "pair_thresholds.csv", index=False)
    model_runs.to_csv(output_dir / "model_runs.csv", index=False)
    pd.DataFrame(skipped).to_csv(output_dir / "skipped.csv", index=False)
    trades = pd.DataFrame(replay.closed_rows)
    blocked = pd.DataFrame(replay.blocked_rows)
    curve = pd.DataFrame(replay.equity_curve)
    trades.to_csv(output_dir / "trades.csv", index=False)
    blocked.to_csv(output_dir / "blocked_candidates.csv", index=False)
    curve.to_csv(output_dir / "equity_curve.csv", index=False)
    if not trades.empty:
        pair_rows = []
        for pair, group in trades.groupby("instrument"):
            pnl = group["pnl"].to_numpy(dtype=float)
            gross_profit = float(pnl[pnl > 0].sum())
            gross_loss = float(abs(pnl[pnl < 0].sum()))
            pair_rows.append({
                "instrument": pair,
                "trades": int(len(group)),
                "pnl": float(group["pnl"].sum()),
                "return_pct_contribution_on_start_equity": float(
                    group["pnl"].sum() / args.start_equity * 100.0
                ),
                "win_rate": float((group["pnl"] > 0.0).mean()),
                "profit_factor": (
                    gross_profit / gross_loss
                    if gross_loss > 0.0
                    else (gross_profit if gross_profit > 0.0 else 0.0)
                ),
                "avg_risk_pct": float(group["risk_pct"].mean()),
                "avg_rank_score": float(group["rank_score"].mean()),
                "avg_expected_move_atr": float(group["expected_move_atr"].mean()),
                "avg_realized_atr": float(group["realized_atr_at_close"].mean()),
            })
        pair_summary = pd.DataFrame(pair_rows).sort_values(
            "return_pct_contribution_on_start_equity",
            ascending=False,
        )
        pair_summary.to_csv(output_dir / "pair_summary.csv", index=False)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--settings-csv",
        type=Path,
        default=DIRS["reports"] / "best_directional_model_settings_by_pair_timeboxed.csv",
    )
    parser.add_argument(
        "--candidates-csv",
        type=Path,
        default=None,
        help="Reuse previously generated candidate_rows.csv and only rerun portfolio allocation.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DIRS["reports"] / "live_style_pair_portfolio_backtest",
    )
    parser.add_argument("--max-pairs", type=int, default=0)
    parser.add_argument(
        "--thresholds",
        default="0.50:0.95:0.025",
        help="Comma-separated thresholds or start:stop:step.",
    )
    parser.add_argument("--skip-unsupported", action="store_true", default=True)
    parser.add_argument("--include-unsupported", dest="skip_unsupported", action="store_false")
    parser.add_argument("--train-row-cap", type=int, default=30000)
    parser.add_argument(
        "--train-lookback-days",
        type=int,
        default=0,
        help="If >0, train each fold only on rows within this many days before the train end.",
    )
    parser.add_argument("--n-jobs", type=int, default=4)
    parser.add_argument("--min-train-rows", type=int, default=5000)
    parser.add_argument("--min-calibration-rows", type=int, default=250)
    parser.add_argument("--min-test-rows", type=int, default=250)
    parser.add_argument("--min-pair-calibration-rows", type=int, default=250)
    parser.add_argument("--min-pair-calibration-trades", type=int, default=8)
    parser.add_argument("--min-expected-move-atr", type=float, default=0.0)
    parser.add_argument("--episode-aware", action="store_true", default=True)
    parser.add_argument("--no-episode-aware", dest="episode_aware", action="store_false")
    parser.add_argument(
        "--candidate-start-utc",
        default="",
        help="Optional inclusive UTC timestamp for generated or reused candidate rows.",
    )
    parser.add_argument(
        "--candidate-end-utc",
        default="",
        help="Optional exclusive UTC timestamp for generated or reused candidate rows.",
    )
    parser.add_argument(
        "--all-test-weeks",
        action="store_true",
        help="Evaluate every valid historical test week instead of only the spec holdout tail.",
    )
    parser.add_argument(
        "--holdout-weeks-override",
        type=int,
        default=0,
        help="Minimum number of trailing walk-forward test weeks to evaluate.",
    )

    parser.add_argument("--start-equity", type=float, default=100000.0)
    parser.add_argument("--min-risk-pct", type=float, default=0.18)
    parser.add_argument("--low-risk-pct", type=float, default=0.28)
    parser.add_argument("--medium-risk-pct", type=float, default=0.40)
    parser.add_argument("--high-risk-pct", type=float, default=0.52)
    parser.add_argument("--max-risk-pct", type=float, default=0.65)
    parser.add_argument("--min-throttled-risk-pct", type=float, default=0.05)
    parser.add_argument("--min-rank-score", type=float, default=0.0)
    parser.add_argument("--min-score-percentile", type=float, default=0.0)
    parser.add_argument("--min-probability", type=float, default=0.0)
    parser.add_argument("--margin-pct-per-risk-pct", type=float, default=8.0)
    parser.add_argument("--target-margin-pct", type=float, default=62.0)
    parser.add_argument("--hard-margin-pct", type=float, default=74.0)
    parser.add_argument("--emergency-margin-pct", type=float, default=88.0)
    parser.add_argument("--target-open-risk-pct", type=float, default=0.0)
    parser.add_argument("--max-open-risk-pct", type=float, default=0.0)
    parser.add_argument("--max-currency-risk-pct", type=float, default=0.0)
    parser.add_argument("--above-target-min-score-percentile", type=float, default=0.80)
    parser.add_argument("--drawdown-risk-throttle-start-pct", type=float, default=10.0)
    parser.add_argument("--drawdown-risk-throttle-full-pct", type=float, default=25.0)
    parser.add_argument("--drawdown-risk-min-multiplier", type=float, default=0.25)
    parser.add_argument("--drawdown-open-risk-min-multiplier", type=float, default=0.45)
    parser.add_argument("--drawdown-entry-halt-pct", type=float, default=0.0)
    parser.add_argument("--drawdown-halt-bypass-score-percentile", type=float, default=0.98)
    parser.add_argument("--max-daily-loss-pct", type=float, default=0.0)
    parser.add_argument("--daily-loss-bypass-score-percentile", type=float, default=0.99)
    parser.add_argument("--max-pair-daily-loss-pct", type=float, default=0.0)
    parser.add_argument("--max-pair-total-loss-pct", type=float, default=0.0)
    parser.add_argument("--pair-loss-streak-cooldown-trades", type=int, default=0)
    parser.add_argument("--pair-loss-cooldown-minutes", type=float, default=240.0)
    parser.add_argument("--pair-large-loss-cooldown-pct", type=float, default=0.0)
    parser.add_argument("--max-open-positions", type=int, default=18)
    parser.add_argument("--max-same-pair-positions", type=int, default=2)
    parser.add_argument("--max-same-campaign-positions", type=int, default=1)
    parser.add_argument("--min-campaign-entry-gap-minutes", type=float, default=45.0)
    parser.add_argument("--min-pair-entry-gap-minutes", type=float, default=20.0)
    parser.add_argument("--min-after-close-gap-minutes", type=float, default=10.0)
    parser.add_argument("--stale-minutes", type=float, default=90.0)
    parser.add_argument("--stale-rotation-min-score-percentile", type=float, default=0.90)
    parser.add_argument("--stale-rotation-open-share", type=float, default=0.70)
    parser.add_argument("--min-rotation-age-minutes", type=float, default=45.0)
    parser.add_argument("--min-opposite-rotation-age-minutes", type=float, default=30.0)
    parser.add_argument("--rotation-score-multiplier", type=float, default=1.25)
    parser.add_argument("--opposite-rotation-multiplier", type=float, default=1.15)
    parser.add_argument("--max-rotations-per-timestamp", type=int, default=2)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    os.environ["OANDA_CONTINUOUS_RESEARCH_VALIDATION_MAX_TRAIN_ROWS_CAP"] = str(
        max(10000, args.train_row_cap)
    )
    os.environ["OANDA_CONTINUOUS_RESEARCH_SCREEN_MAX_TRAIN_ROWS_CAP"] = str(
        max(10000, min(args.train_row_cap, 30000))
    )
    os.environ["OANDA_CONTINUOUS_RESEARCH_N_JOBS"] = str(max(1, args.n_jobs))

    thresholds = parse_thresholds(args.thresholds)
    candidate_start_utc = optional_utc_timestamp(args.candidate_start_utc)
    candidate_end_utc = optional_utc_timestamp(args.candidate_end_utc)
    if (
        candidate_start_utc is not None
        and candidate_end_utc is not None
        and candidate_start_utc >= candidate_end_utc
    ):
        raise SystemExit("--candidate-start-utc must be before --candidate-end-utc")
    if args.candidates_csv:
        candidates = pd.read_csv(args.candidates_csv)
        if candidates.empty:
            pair_thresholds = pd.DataFrame()
            model_runs = pd.DataFrame()
            skipped: List[Dict[str, Any]] = []
        else:
            candidates["time_utc"] = pd.to_datetime(candidates["time_utc"], utc=True)
            candidates["planned_exit_time"] = pd.to_datetime(
                candidates["planned_exit_time"],
                utc=True,
            )
            if "score_percentile" not in candidates:
                candidates["score_percentile"] = candidates["rank_score"].rank(
                    method="average",
                    pct=True,
                )
            if candidate_start_utc is not None:
                candidates = candidates[candidates["time_utc"] >= candidate_start_utc].copy()
            if candidate_end_utc is not None:
                candidates = candidates[candidates["time_utc"] < candidate_end_utc].copy()
            pair_thresholds = pd.DataFrame()
            model_runs = pd.DataFrame()
            skipped = []
    else:
        settings = load_settings(args.settings_csv, args.max_pairs)
        candidates, pair_thresholds, model_runs, skipped = generate_candidates(
            settings,
            thresholds=thresholds,
            min_pair_calibration_rows=args.min_pair_calibration_rows,
            min_pair_calibration_trades=args.min_pair_calibration_trades,
            min_expected_move_atr=args.min_expected_move_atr,
            episode_aware=args.episode_aware,
            skip_unsupported=args.skip_unsupported,
            min_train_rows=args.min_train_rows,
            min_calibration_rows=args.min_calibration_rows,
            min_test_rows=args.min_test_rows,
            candidate_start_utc=candidate_start_utc,
            candidate_end_utc=candidate_end_utc,
            all_test_weeks=args.all_test_weeks,
            holdout_weeks_override=args.holdout_weeks_override,
            train_lookback_days=args.train_lookback_days,
        )
    replay = PortfolioReplay(args)
    summary = replay.run(candidates)
    write_outputs(
        args.output_dir,
        summary,
        candidates,
        pair_thresholds,
        model_runs,
        skipped,
        replay,
        args,
    )
    print(json.dumps({
        "output_dir": str(args.output_dir),
        **summary,
        "skipped_count": len(skipped),
    }, indent=2, default=json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
