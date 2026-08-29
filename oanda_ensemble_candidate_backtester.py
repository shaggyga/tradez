#!/usr/bin/env python3
"""Build and backtest a multi-model shadow ensemble for the OANDA FX stack.

This script is intentionally research/shadow only.  It uses already validated
continuous-research candidates as members, adds a direction model for
``profitable_any_move_*`` opportunity detectors, compares against the existing
ARIMA baseline report, and writes an ensemble manifest that can be reviewed or
wired to a dummy/canary account later.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import joblib
import numpy as np
import pandas as pd

warnings.filterwarnings(
    "ignore",
    message=r"`sklearn\.utils\.parallel\.delayed` should be used with `sklearn\.utils\.parallel\.Parallel`.*",
    category=UserWarning,
)

from oanda_gpt_training_strategy_manager import (
    CONTINUOUS_RESEARCH_CALIBRATION_WEEKS,
    CONTINUOUS_RESEARCH_PURGE_MINUTES,
    DIRS,
    INSTRUMENT_METADATA_FEATURES,
    TECHNICAL_CORE_FEATURES,
    TECHNICAL_MODEL_FEATURES,
    ContinuousResearchEngine,
    apply_instrument_subset,
    build_technical_spike_research_dataset,
    iso_utc,
    load_json,
    safe_float,
    safe_int,
    save_json,
)


PROJECT_ROOT = Path(__file__).resolve().parent
TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
ENSEMBLE_ROOT = TRAINING_ROOT / "ensemble_research"
PROMOTIONS_ROOT = TRAINING_ROOT / "promotions"
ARIMA_ROOT = TRAINING_ROOT / "arima_baselines"


def progress(message: str) -> None:
    print(f"[{iso_utc()}] {message}", file=sys.stderr, flush=True)


@dataclass(frozen=True)
class MemberSpec:
    experiment_id: str
    candidate_id: str
    score: float
    mean_auc: float
    selected_trades: float
    selected_mean_net: float
    spec: dict[str, Any]

    @property
    def key(self) -> str:
        return f"{self.experiment_id}:{self.spec.get('target', '')}"

    @property
    def target(self) -> str:
        return str(self.spec.get("target", ""))

    @property
    def outcome(self) -> str:
        return str(self.spec.get("outcome", ""))

    @property
    def feature_set(self) -> str:
        return str(self.spec.get("feature_set", ""))

    @property
    def instrument_subset(self) -> str:
        return str(self.spec.get("instrument_subset") or "all")


def json_safe(value: Any) -> Any:
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def clean_text(value: Any) -> str:
    try:
        if value is None or pd.isna(value):
            return ""
    except Exception:
        if value is None:
            return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=json_safe),
        encoding="utf-8",
    )


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def read_ledger() -> pd.DataFrame:
    path = TRAINING_ROOT / "continuous_research" / "experiment_ledger.csv"
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def detail_path_for(row: pd.Series) -> Path:
    raw = str(row.get("detail_path") or "")
    if raw:
        path = Path(raw)
        if path.exists():
            return path
    return (
        TRAINING_ROOT
        / "continuous_research"
        / "experiments"
        / f"{row.get('experiment_id')}.json"
    )


def validated_members(
    *,
    max_members: int,
    core_only: bool,
    deployment_lane: str = "",
    account_focus: str = "",
) -> list[MemberSpec]:
    ledger = read_ledger()
    filtered = ledger[
        ledger.get("status", "").astype(str).eq("successful")
        & ledger.get("evaluation_stage", "").astype(str).eq("validation")
        & ledger.get("dataset_kind", "").astype(str).isin(
            ["technical_spike", "profitable_move_precursor"]
        )
    ].copy()
    if filtered.empty:
        return []
    for column in [
        "score",
        "mean_auc",
        "trades",
        "mean_net_pips",
        "median_profit_factor",
    ]:
        filtered[column] = pd.to_numeric(filtered.get(column), errors="coerce")
    filtered = filtered.sort_values(
        ["score", "mean_auc", "trades"],
        ascending=False,
    )

    members: list[MemberSpec] = []
    seen_roles: set[tuple[str, str, str]] = set()
    for _, row in filtered.iterrows():
        path = detail_path_for(row)
        if not path.exists():
            continue
        payload = load_json(path, {})
        spec = dict(payload.get("spec") or {})
        if not spec:
            continue
        resolved_deployment_lane = (
            clean_text(row.get("deployment_lane"))
            or clean_text(spec.get("deployment_lane"))
        )
        resolved_account_focus = (
            clean_text(row.get("account_focus"))
            or clean_text(spec.get("account_focus"))
        )
        if deployment_lane and resolved_deployment_lane != deployment_lane:
            continue
        if account_focus and resolved_account_focus != account_focus:
            continue
        if core_only and str(spec.get("feature_set")) != "technical_core":
            continue
        target = str(spec.get("target", ""))
        subset = str(spec.get("instrument_subset") or "all")
        model_type = str(spec.get("model_type") or "")
        role_key = (target, subset, model_type)
        if role_key in seen_roles:
            continue
        seen_roles.add(role_key)
        members.append(
            MemberSpec(
                experiment_id=str(row.get("experiment_id", "")),
                candidate_id=str(row.get("candidate_id", "")),
                score=safe_float(row.get("score")),
                mean_auc=safe_float(row.get("mean_auc")),
                selected_trades=safe_float(row.get("trades")),
                selected_mean_net=safe_float(row.get("mean_net_pips")),
                spec=spec,
            )
        )
        if len(members) >= max_members:
            break
    return members


def horizon_from_target(target: str, default: int = 120) -> int:
    try:
        return int(str(target).rsplit("_", 1)[-1])
    except Exception:
        return default


def feature_columns_for(spec: dict[str, Any]) -> list[str]:
    return (
        list(TECHNICAL_CORE_FEATURES)
        if str(spec.get("feature_set")) == "technical_core"
        else list(TECHNICAL_MODEL_FEATURES)
    )


def required_columns(members: Sequence[MemberSpec], outcome_horizon: int) -> list[str]:
    columns: list[str] = [
        "time_utc",
        "instrument",
        "pair_taxonomy_primary",
        "regime_primary",
        "is_asia_session",
        "is_london_session",
        "is_new_york_session",
        "is_london_ny_overlap",
        *INSTRUMENT_METADATA_FEATURES,
        f"long_net_atr_{outcome_horizon}",
        f"short_net_atr_{outcome_horizon}",
        f"best_net_atr_{outcome_horizon}",
    ]
    for member in members:
        spec = member.spec
        horizon = horizon_from_target(str(spec.get("target", "")))
        columns.extend(feature_columns_for(spec))
        columns.append(str(spec.get("target", "")))
        if spec.get("outcome"):
            columns.append(str(spec["outcome"]))
        if str(spec.get("target", "")).startswith("profitable_any_move_"):
            columns.extend(
                [
                    f"best_direction_up_{horizon}",
                    f"long_net_atr_{horizon}",
                    f"short_net_atr_{horizon}",
                    f"best_net_atr_{horizon}",
                ]
            )
    return list(dict.fromkeys([c for c in columns if c]))


def load_research_frame(
    members: Sequence[MemberSpec],
    *,
    outcome_horizon: int,
) -> tuple[pd.DataFrame, Path]:
    path = build_technical_spike_research_dataset()
    columns = required_columns(members, outcome_horizon)
    frame = pd.read_parquet(path, columns=columns)
    frame["time_utc"] = pd.to_datetime(
        frame["time_utc"],
        errors="coerce",
        utc=True,
    )
    frame = frame.dropna(subset=["time_utc"]).sort_values("time_utc")
    frame["week_start"] = (
        frame["time_utc"].dt.normalize()
        - pd.to_timedelta(frame["time_utc"].dt.weekday, unit="D")
    )
    return frame.reset_index(drop=True), path


def member_weight(member: MemberSpec) -> float:
    # Keep scores useful without letting the current champion swamp every vote.
    return float(np.clip(math.log1p(max(member.score, 1.0)) / 4.0, 0.75, 2.0))


def target_direction(
    target: str,
    rows: pd.DataFrame,
    direction_probability: np.ndarray | None,
) -> np.ndarray:
    target = target.lower()
    if target.startswith("long_"):
        return np.ones(len(rows), dtype=float)
    if target.startswith("short_"):
        return np.zeros(len(rows), dtype=float)
    if target.startswith("continuation_"):
        return (pd.to_numeric(rows.get("momentum_30_atr"), errors="coerce").fillna(0) >= 0).to_numpy(dtype=float)
    if target.startswith("reversal_"):
        return (pd.to_numeric(rows.get("momentum_30_atr"), errors="coerce").fillna(0) < 0).to_numpy(dtype=float)
    if direction_probability is not None:
        return np.asarray(direction_probability, dtype=float)
    return np.full(len(rows), 0.5, dtype=float)


def score_member_fold(
    engine: ContinuousResearchEngine,
    member: MemberSpec,
    train: pd.DataFrame,
    calibration: pd.DataFrame,
    test: pd.DataFrame,
    *,
    max_train_rows: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    spec = dict(member.spec)
    features = feature_columns_for(spec)
    target = str(spec["target"])
    subset = str(spec.get("instrument_subset") or "all")
    needed = list(dict.fromkeys([*features, target]))
    cal_probability = np.full(len(calibration), np.nan, dtype=float)
    test_probability = np.full(len(test), np.nan, dtype=float)
    cal_direction = np.full(len(calibration), np.nan, dtype=float)
    test_direction = np.full(len(test), np.nan, dtype=float)

    train_subset = apply_instrument_subset(train, subset).copy()
    calibration_subset = apply_instrument_subset(calibration, subset).copy()
    test_subset = apply_instrument_subset(test, subset).copy()
    if len(train_subset) > max_train_rows:
        train_subset = train_subset.tail(max_train_rows)
    for column in needed:
        train_subset[column] = pd.to_numeric(train_subset[column], errors="coerce")
        calibration_subset[column] = pd.to_numeric(
            calibration_subset[column],
            errors="coerce",
        )
        test_subset[column] = pd.to_numeric(test_subset[column], errors="coerce")
    train_subset = train_subset.dropna(subset=needed)
    calibration_subset = calibration_subset.dropna(subset=needed)
    test_subset = test_subset.dropna(subset=needed)
    if (
        len(train_subset) < 5_000
        or len(calibration_subset) < 250
        or len(test_subset) < 250
        or train_subset[target].nunique() < 2
    ):
        return cal_probability, test_probability, cal_direction, test_direction

    model = engine.estimator(spec)
    model.fit(train_subset[features], train_subset[target].astype(int))
    raw_calibration = model.predict_proba(calibration_subset[features])[:, 1]
    calibrator = engine.fit_probability_calibrator(
        raw_calibration,
        calibration_subset[target].astype(int).to_numpy(),
    )
    calibrated_calibration = engine.apply_probability_calibrator(
        calibrator,
        raw_calibration,
    )
    calibrated_test = engine.apply_probability_calibrator(
        calibrator,
        model.predict_proba(test_subset[features])[:, 1],
    )
    cal_probability[calibration_subset.index.to_numpy()] = calibrated_calibration
    test_probability[test_subset.index.to_numpy()] = calibrated_test

    direction_target = str(spec.get("direction_target") or "")
    if target.startswith("profitable_any_move_") and direction_target:
        direction_columns = [*features, target, direction_target]
        direction_train = train_subset.copy()
        for column in direction_columns:
            direction_train[column] = pd.to_numeric(
                direction_train[column],
                errors="coerce",
            )
        direction_train = direction_train[
            direction_train[target].astype(int).eq(1)
        ].dropna(subset=direction_columns)
        if (
            len(direction_train) >= 50
            and direction_train[direction_target].nunique() >= 2
        ):
            direction_model = engine.estimator(spec)
            direction_model.fit(
                direction_train[features],
                direction_train[direction_target].astype(int),
            )
            cal_dir = direction_model.predict_proba(
                calibration_subset[features]
            )[:, 1]
            test_dir = direction_model.predict_proba(test_subset[features])[:, 1]
            cal_direction[calibration_subset.index.to_numpy()] = cal_dir
            test_direction[test_subset.index.to_numpy()] = test_dir
    else:
        cal_direction[calibration_subset.index.to_numpy()] = target_direction(
            target,
            calibration_subset,
            None,
        )
        test_direction[test_subset.index.to_numpy()] = target_direction(
            target,
            test_subset,
            None,
        )
    return cal_probability, test_probability, cal_direction, test_direction


def combine_member_predictions(
    frame: pd.DataFrame,
    member_outputs: Sequence[tuple[MemberSpec, np.ndarray, np.ndarray]],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    weighted_probability = np.zeros(len(frame), dtype=float)
    total_weight = np.zeros(len(frame), dtype=float)
    long_weight = np.zeros(len(frame), dtype=float)
    short_weight = np.zeros(len(frame), dtype=float)
    member_count = np.zeros(len(frame), dtype=float)

    for member, probability, direction_probability in member_outputs:
        probability = np.asarray(probability, dtype=float)
        direction_probability = np.asarray(direction_probability, dtype=float)
        mask = np.isfinite(probability)
        if not mask.any():
            continue
        weight = member_weight(member)
        direction = np.where(
            np.isfinite(direction_probability),
            direction_probability,
            0.5,
        )
        weighted_probability[mask] += probability[mask] * weight
        total_weight[mask] += weight
        long_weight[mask] += probability[mask] * direction[mask] * weight
        short_weight[mask] += probability[mask] * (1.0 - direction[mask]) * weight
        member_count[mask] += 1.0

    ensemble_probability = np.divide(
        weighted_probability,
        total_weight,
        out=np.full(len(frame), np.nan, dtype=float),
        where=total_weight > 0,
    )
    directional_total = long_weight + short_weight
    long_share = np.divide(
        long_weight,
        directional_total,
        out=np.full(len(frame), 0.5, dtype=float),
        where=directional_total > 0,
    )
    agreement = np.divide(
        np.maximum(long_weight, short_weight),
        directional_total,
        out=np.zeros(len(frame), dtype=float),
        where=directional_total > 0,
    )
    return ensemble_probability, long_share, agreement, member_count


def outcome_for_direction(
    frame: pd.DataFrame,
    long_share: np.ndarray,
    *,
    outcome_horizon: int,
    opportunity_mode: bool = False,
) -> np.ndarray:
    if opportunity_mode:
        return pd.to_numeric(
            frame[f"best_net_atr_{outcome_horizon}"],
            errors="coerce",
        ).to_numpy(dtype=float)
    long_values = pd.to_numeric(
        frame[f"long_net_atr_{outcome_horizon}"],
        errors="coerce",
    ).to_numpy(dtype=float)
    short_values = pd.to_numeric(
        frame[f"short_net_atr_{outcome_horizon}"],
        errors="coerce",
    ).to_numpy(dtype=float)
    return np.where(long_share >= 0.5, long_values, short_values)


def choose_ensemble_threshold(
    engine: ContinuousResearchEngine,
    frame: pd.DataFrame,
    probability: np.ndarray,
    long_share: np.ndarray,
    agreement: np.ndarray,
    member_count: np.ndarray,
    *,
    outcome_horizon: int,
    opportunity_mode: bool,
    min_members: int,
    min_trades: int,
) -> tuple[dict[str, float], np.ndarray]:
    outcome = outcome_for_direction(
        frame,
        long_share,
        outcome_horizon=outcome_horizon,
        opportunity_mode=opportunity_mode,
    )
    best: dict[str, float] = {
        "probability_threshold": 1.0,
        "agreement_threshold": 1.0,
        "trades": 0.0,
        "mean_net_pips": 0.0,
        "profit_factor": 0.0,
        "total_net_pips": 0.0,
        "win_rate": 0.0,
        "score": -1e18,
    }
    best_mask = np.zeros(len(frame), dtype=bool)
    valid_probability = probability[
        np.isfinite(probability) & (member_count >= min_members)
    ]
    dynamic_thresholds: list[float] = []
    if len(valid_probability):
        dynamic_thresholds = [
            float(np.quantile(valid_probability, q))
            for q in [0.50, 0.65, 0.75, 0.85, 0.90, 0.95, 0.975, 0.99]
        ]
    probability_thresholds = sorted(
        {
            *[0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75],
            *dynamic_thresholds,
        }
    )
    agreement_thresholds = (
        [0.0]
        if opportunity_mode
        else [0.50, 0.525, 0.55, 0.60, 0.65, 0.70]
    )
    for probability_threshold in probability_thresholds:
        for agreement_threshold in agreement_thresholds:
            mask = (
                np.isfinite(probability)
                & (probability >= probability_threshold)
                & (agreement >= agreement_threshold)
                & (member_count >= min_members)
            )
            summary = engine.outcome_summary(outcome[mask])
            if safe_float(summary.get("trades")) < min_trades:
                continue
            score = (
                safe_float(summary.get("total_net_pips"))
                + min(10.0, safe_float(summary.get("profit_factor"))) * 4.0
                + safe_float(summary.get("mean_net_pips")) * 15.0
            )
            if score > best["score"]:
                best = {
                    "probability_threshold": float(probability_threshold),
                    "agreement_threshold": float(agreement_threshold),
                    **summary,
                    "score": float(score),
                }
                best_mask = mask
    return best, best_mask


def session_for_row(row: pd.Series) -> str:
    if safe_float(row.get("is_london_ny_overlap")) >= 0.5:
        return "london_ny_overlap"
    if safe_float(row.get("is_new_york_session")) >= 0.5:
        return "new_york"
    if safe_float(row.get("is_london_session")) >= 0.5:
        return "london"
    if safe_float(row.get("is_asia_session")) >= 0.5:
        return "asia"
    return "off_session"


def selected_records(
    frame: pd.DataFrame,
    positions: Sequence[int],
    outcome: np.ndarray,
    probability: np.ndarray,
    long_share: np.ndarray,
    agreement: np.ndarray,
    member_count: np.ndarray,
    opportunity_mode: bool,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not len(positions):
        return rows
    selected = frame.iloc[list(positions)]
    for position, (_, row) in zip(positions, selected.iterrows()):
        rows.append(
            {
                "time_utc": row.get("time_utc"),
                "instrument": str(row.get("instrument", "")),
                "direction": (
                    "OPPORTUNITY"
                    if opportunity_mode
                    else ("LONG" if long_share[position] >= 0.5 else "SHORT")
                ),
                "outcome": safe_float(outcome[position]),
                "probability": safe_float(probability[position]),
                "long_share": safe_float(long_share[position]),
                "agreement": safe_float(agreement[position]),
                "member_count": safe_float(member_count[position]),
                "pair_taxonomy_primary": str(
                    row.get("pair_taxonomy_primary", "")
                ),
                "regime_primary": str(row.get("regime_primary", "")),
                "session": session_for_row(row),
            }
        )
    return rows


def arima_baseline_reference() -> dict[str, Any]:
    summaries = sorted(ARIMA_ROOT.glob("*_summary.json"), key=lambda p: p.stat().st_mtime)
    if not summaries:
        return {
            "available": False,
            "reason": "no ARIMA summary files found",
        }
    summary_path = summaries[-1]
    summary = load_json(summary_path, {})
    return {
        "available": True,
        "summary_path": str(summary_path),
        "pipeline_version": summary.get("pipeline_version", ""),
        "promotion_ready_count": safe_int(summary.get("promotion_ready_count"), 0),
        "pairs": summary.get("pairs", []),
        "horizons": summary.get("horizons", []),
        "top_rows": summary.get("top_rows", [])[:10],
        "note": (
            "ARIMA is currently an aggregate baseline. It is not used as a "
            "row-level ensemble member until stable rolling rows pass promotion."
        ),
    }


def backtest_ensemble(
    members: Sequence[MemberSpec],
    *,
    holdout_weeks: int,
    max_train_rows: int,
    outcome_horizon: int,
    min_members: int,
    min_calibration_trades: int,
    opportunity_mode: bool,
) -> dict[str, Any]:
    if not members:
        raise RuntimeError("No validated members selected for ensemble")
    progress(
        "ensemble backtest start "
        f"members={len(members)} holdout_weeks={holdout_weeks} "
        f"horizon={outcome_horizon} max_train_rows={max_train_rows}"
    )
    engine = ContinuousResearchEngine()
    frame, dataset_path = load_research_frame(
        members,
        outcome_horizon=outcome_horizon,
    )
    windows = engine.rolling_windows(frame, holdout_weeks)
    progress(
        f"research frame loaded rows={len(frame)} windows={len(windows)} "
        f"dataset={dataset_path}"
    )
    folds: list[dict[str, Any]] = []
    threshold_rows: list[dict[str, Any]] = []
    selected_all: list[dict[str, Any]] = []
    selected_values_all: list[float] = []
    purge = pd.Timedelta(minutes=CONTINUOUS_RESEARCH_PURGE_MINUTES)

    for fold_number, (train_end, calibration_start, test_start) in enumerate(windows, 1):
        train = frame[frame["time_utc"] < train_end].copy()
        calibration = frame[
            (frame["time_utc"] >= calibration_start)
            & (frame["time_utc"] < test_start - purge)
        ].copy()
        test = frame[frame["week_start"] == test_start].copy()
        if len(train) < 5_000 or len(calibration) < 250 or len(test) < 250:
            progress(
                f"fold {fold_number}/{len(windows)} skipped "
                f"train={len(train)} calibration={len(calibration)} test={len(test)}"
            )
            continue
        progress(
            f"fold {fold_number}/{len(windows)} scoring "
            f"train={len(train)} calibration={len(calibration)} test={len(test)}"
        )
        train = train.reset_index(drop=True)
        calibration = calibration.reset_index(drop=True)
        test = test.reset_index(drop=True)

        cal_outputs: list[tuple[MemberSpec, np.ndarray, np.ndarray]] = []
        test_outputs: list[tuple[MemberSpec, np.ndarray, np.ndarray]] = []
        for member in members:
            (
                cal_probability,
                test_probability,
                cal_direction,
                test_direction,
            ) = score_member_fold(
                engine,
                member,
                train,
                calibration,
                test,
                max_train_rows=max_train_rows,
            )
            cal_outputs.append((member, cal_probability, cal_direction))
            test_outputs.append((member, test_probability, test_direction))

        cal_probability, cal_long_share, cal_agreement, cal_member_count = (
            combine_member_predictions(calibration, cal_outputs)
        )
        test_probability, test_long_share, test_agreement, test_member_count = (
            combine_member_predictions(test, test_outputs)
        )
        threshold, _ = choose_ensemble_threshold(
            engine,
            calibration,
            cal_probability,
            cal_long_share,
            cal_agreement,
            cal_member_count,
            outcome_horizon=outcome_horizon,
            opportunity_mode=opportunity_mode,
            min_members=min_members,
            min_trades=min_calibration_trades,
        )
        selected = (
            np.isfinite(test_probability)
            & (test_probability >= threshold["probability_threshold"])
            & (test_agreement >= threshold["agreement_threshold"])
            & (test_member_count >= min_members)
        )
        positions = engine.episode_positions(test, test_probability, selected)
        test_outcome = outcome_for_direction(
            test,
            test_long_share,
            outcome_horizon=outcome_horizon,
            opportunity_mode=opportunity_mode,
        )
        selected_values = test_outcome[positions]
        selected_values_all.extend(
            [float(value) for value in selected_values if math.isfinite(float(value))]
        )
        selected_records_fold = selected_records(
            test,
            positions,
            test_outcome,
            test_probability,
            test_long_share,
            test_agreement,
            test_member_count,
            opportunity_mode,
        )
        selected_all.extend(selected_records_fold)
        summary = engine.outcome_summary(selected_values)
        progress(
            f"fold {fold_number}/{len(windows)} complete "
            f"selected={len(selected_records_fold)} "
            f"mean_net={summary.get('mean_net', '')} "
            f"profit_factor={summary.get('profit_factor', '')}"
        )
        folds.append(
            {
                "fold": fold_number,
                "week_start": test_start.isoformat(),
                "calibration_week": calibration_start.isoformat(),
                "purge_minutes": CONTINUOUS_RESEARCH_PURGE_MINUTES,
                "train_rows": int(len(train)),
                "calibration_rows": int(len(calibration)),
                "test_rows": int(len(test)),
                "probability_threshold": threshold["probability_threshold"],
                "agreement_threshold": threshold["agreement_threshold"],
                "available_member_count_median": float(
                    np.nanmedian(test_member_count)
                ),
                **summary,
            }
        )
        threshold_rows.append(
            {
                "week_start": test_start.isoformat(),
                "probability_threshold": threshold["probability_threshold"],
                "agreement_threshold": threshold["agreement_threshold"],
                **summary,
            }
        )

    if not folds:
        raise RuntimeError("No valid ensemble folds")
    fold_frame = pd.DataFrame(folds)
    total_trades = int(pd.to_numeric(fold_frame["trades"], errors="coerce").sum())
    total_net = float(pd.to_numeric(fold_frame["total_net_pips"], errors="coerce").sum())
    selected = {
        "weeks": float(len(fold_frame)),
        "positive_weeks": float((fold_frame["mean_net_pips"] > 0).sum()),
        "trades": float(total_trades),
        "mean_net_pips": total_net / max(total_trades, 1),
        "median_week_mean_net_pips": float(fold_frame["mean_net_pips"].median()),
        "median_profit_factor": float(fold_frame["profit_factor"].median()),
        "total_net_pips": total_net,
        "probability_threshold": float(
            fold_frame["probability_threshold"].median()
        ),
        "agreement_threshold": float(fold_frame["agreement_threshold"].median()),
        "bootstrap_lower_mean_net_pips": engine.bootstrap_lower_mean(
            selected_values_all,
            seed=104729,
        ),
    }
    selected.update(engine.concentration_summary(selected_all))
    scorecards = engine.segment_scorecards(selected_all)
    allowed_segments = engine.allowed_segments_from_scorecards(scorecards)
    economic_gate = engine.economic_gate(
        selected,
        stage="validation",
        fold_count=len(fold_frame),
        bootstrap_lower_mean=selected["bootstrap_lower_mean_net_pips"],
        top_pair_trade_share=selected["top_pair_trade_share"],
    )
    gate = {
        **economic_gate,
        "passed": all(economic_gate.values()),
    }
    score = (
        selected["total_net_pips"]
        + min(50.0, selected["median_profit_factor"]) * 8.0
        + selected["mean_net_pips"] * 40.0
        + selected["positive_weeks"] / max(selected["weeks"], 1.0) * 100.0
        - selected["top_pair_trade_share"] * 50.0
    )
    return {
        "generated_utc": iso_utc(),
        "pipeline_version": "ensemble_shadow_v1",
        "dataset_path": str(dataset_path),
        "dataset_rows": int(len(frame)),
        "holdout_weeks": int(holdout_weeks),
        "calibration_weeks": CONTINUOUS_RESEARCH_CALIBRATION_WEEKS,
        "max_train_rows": int(max_train_rows),
        "outcome_horizon": int(outcome_horizon),
        "opportunity_mode": bool(opportunity_mode),
        "min_members": int(min_members),
        "members": [
            {
                "experiment_id": m.experiment_id,
                "candidate_id": m.candidate_id,
                "score": m.score,
                "mean_auc": m.mean_auc,
                "selected_trades": m.selected_trades,
                "selected_mean_net": m.selected_mean_net,
                "weight": member_weight(m),
                "spec": m.spec,
            }
            for m in members
        ],
        "folds": folds,
        "threshold_summary": threshold_rows,
        "selected_threshold": selected,
        "gate": gate,
        "scorecards": scorecards,
        "allowed_segments": allowed_segments,
        "research_score": float(score),
        "arima_baseline": arima_baseline_reference(),
        "selected_records": selected_all,
    }


def train_final_bundle(
    members: Sequence[MemberSpec],
    *,
    frame: pd.DataFrame,
    max_train_rows: int,
    outcome_horizon: int,
    selected_threshold: dict[str, Any],
) -> dict[str, Any]:
    engine = ContinuousResearchEngine()
    bundles: list[dict[str, Any]] = []
    for member in members:
        spec = dict(member.spec)
        features = feature_columns_for(spec)
        target = str(spec["target"])
        subset = str(spec.get("instrument_subset") or "all")
        train = apply_instrument_subset(frame, subset).copy()
        if len(train) > max_train_rows:
            train = train.tail(max_train_rows)
        needed = [*features, target]
        for column in needed:
            train[column] = pd.to_numeric(train[column], errors="coerce")
        train = train.dropna(subset=needed)
        if len(train) < 5_000 or train[target].nunique() < 2:
            continue
        model = engine.estimator(spec)
        model.fit(train[features], train[target].astype(int))
        direction_model = None
        direction_target = str(spec.get("direction_target") or "")
        if target.startswith("profitable_any_move_") and direction_target:
            dtrain = train.copy()
            dtrain[direction_target] = pd.to_numeric(
                dtrain[direction_target],
                errors="coerce",
            )
            dtrain = dtrain[dtrain[target].astype(int).eq(1)].dropna(
                subset=[*features, direction_target]
            )
            if len(dtrain) >= 50 and dtrain[direction_target].nunique() >= 2:
                direction_model = engine.estimator(spec)
                direction_model.fit(
                    dtrain[features],
                    dtrain[direction_target].astype(int),
                )
        bundles.append(
            {
                "experiment_id": member.experiment_id,
                "candidate_id": member.candidate_id,
                "spec": spec,
                "features": features,
                "weight": member_weight(member),
                "model": model,
                "direction_model": direction_model,
            }
        )
    return {
        "pipeline_version": "ensemble_shadow_v1",
        "trained_utc": iso_utc(),
        "outcome_horizon": outcome_horizon,
        "selected_threshold": selected_threshold,
        "members": bundles,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backtest a validated multi-model FX ensemble candidate"
    )
    parser.add_argument("--max-members", type=int, default=6)
    parser.add_argument("--holdout-weeks", type=int, default=8)
    parser.add_argument("--max-train-rows", type=int, default=150_000)
    parser.add_argument("--outcome-horizon", type=int, default=120)
    parser.add_argument("--min-members", type=int, default=2)
    parser.add_argument("--min-calibration-trades", type=int, default=25)
    parser.add_argument(
        "--opportunity-mode",
        action="store_true",
        help=(
            "Evaluate as a profitable-move opportunity gate using best_net_atr. "
            "This matches the current technical champion's gate role; it is not "
            "a standalone directional-entry proof."
        ),
    )
    parser.add_argument(
        "--include-full",
        action="store_true",
        help="Allow technical_full validated models; default keeps core models only.",
    )
    parser.add_argument(
        "--write-final-bundle",
        action="store_true",
        help="Train and save a final full-window ensemble bundle for shadow loading.",
    )
    parser.add_argument(
        "--deployment-lane",
        default="",
        help="Optional filter for validated members, e.g. live_tech_champion_candidate.",
    )
    parser.add_argument(
        "--account-focus",
        default="",
        help="Optional filter for validated members by account_focus.",
    )
    args = parser.parse_args()

    members = validated_members(
        max_members=args.max_members,
        core_only=not args.include_full,
        deployment_lane=args.deployment_lane,
        account_focus=args.account_focus,
    )
    if not members:
        raise RuntimeError("No validated members available for ensemble")
    progress(
        "validated members selected "
        f"count={len(members)} opportunity_mode={bool(args.opportunity_mode)} "
        f"include_full={bool(args.include_full)}"
    )
    result = backtest_ensemble(
        members,
        holdout_weeks=args.holdout_weeks,
        max_train_rows=args.max_train_rows,
        outcome_horizon=args.outcome_horizon,
        min_members=args.min_members,
        min_calibration_trades=args.min_calibration_trades,
        opportunity_mode=args.opportunity_mode,
    )

    run_id = iso_utc().replace(":", "").replace("+", "_").replace(".", "_")
    digest = sha256_text(
        json.dumps(
            {
                "members": [m.key for m in members],
                "args": vars(args),
                "generated_utc": result["generated_utc"],
            },
            sort_keys=True,
        )
    )[:10]
    output_prefix = f"ensemble_{run_id}_{digest}"
    detail_path = ENSEMBLE_ROOT / f"{output_prefix}.json"
    selected_path = ENSEMBLE_ROOT / f"{output_prefix}_selected.csv"
    summary_path = ENSEMBLE_ROOT / "latest_ensemble_summary.json"
    ENSEMBLE_ROOT.mkdir(parents=True, exist_ok=True)
    selected_records_payload = result.pop("selected_records")
    pd.DataFrame(selected_records_payload).to_csv(selected_path, index=False)
    result["selected_records_path"] = str(selected_path)
    result["selected_record_count"] = len(selected_records_payload)
    write_json(detail_path, result)
    write_json(summary_path, result)
    progress(
        f"ensemble report written detail={detail_path} "
        f"selected_records={len(selected_records_payload)} summary={summary_path}"
    )

    artifact_path = ""
    if args.write_final_bundle:
        progress("training final ensemble bundle")
        frame, _ = load_research_frame(
            members,
            outcome_horizon=args.outcome_horizon,
        )
        bundle = train_final_bundle(
            members,
            frame=frame,
            max_train_rows=args.max_train_rows,
            outcome_horizon=args.outcome_horizon,
            selected_threshold=result["selected_threshold"],
        )
        artifact = TRAINING_ROOT / "models" / f"{output_prefix}.joblib"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(bundle, artifact)
        artifact_path = str(artifact)
        progress(f"final ensemble bundle written artifact={artifact_path}")

    current_production = load_json(PROMOTIONS_ROOT / "technical_production.json", {})
    manifest = {
        "stage": "ensemble_shadow",
        "activation_effective": False,
        "technical_account_activation": False,
        "updated_utc": iso_utc(),
        "deployment_lane": args.deployment_lane,
        "account_focus": args.account_focus,
        "pipeline_version": result["pipeline_version"],
        "experiment_id": output_prefix,
        "candidate_id": sha256_text(output_prefix),
        "research_score": result["research_score"],
        "model_type": "ensemble",
        "dataset_kind": "technical_spike_plus_profitable_precursor",
        "target": (
            f"ensemble_profitable_any_move_{args.outcome_horizon}"
            if args.opportunity_mode
            else f"ensemble_directional_net_atr_{args.outcome_horizon}"
        ),
        "outcome": (
            f"best_net_atr_{args.outcome_horizon}"
            if args.opportunity_mode
            else f"directional_net_atr_{args.outcome_horizon}"
        ),
        "ensemble_mode": (
            "opportunity_gate_not_directional"
            if args.opportunity_mode
            else "directional_entry"
        ),
        "feature_set": "technical_core" if not args.include_full else "mixed",
        "instrument_subset": "member_specific",
        "members": result["members"],
        "validation": {
            "fold_count": len(result["folds"]),
            "selected_threshold": result["selected_threshold"],
            "gate": result["gate"],
            "threshold_summary": result["threshold_summary"],
            "outcome_horizon": args.outcome_horizon,
            "min_members": args.min_members,
            "opportunity_mode": bool(args.opportunity_mode),
        },
        "scorecards": result["scorecards"],
        "allowed_segments": result["allowed_segments"],
        "model_artifact_path": artifact_path,
        "detail_path": str(detail_path),
        "selected_records_path": str(selected_path),
        "arima_baseline": result["arima_baseline"],
        "current_technical_production": {
            "experiment_id": current_production.get("experiment_id", ""),
            "candidate_id": current_production.get("candidate_id", ""),
            "model_type": current_production.get("model_type", ""),
            "target": current_production.get("target", ""),
            "research_score": current_production.get("research_score", ""),
        },
        "promotion_note": (
            "Shadow only. Promote only after the ensemble beats the active "
            "technical production candidate under comparable validation."
        ),
    }
    manifest_name = "ensemble_shadow_candidate.json"
    if args.deployment_lane or args.account_focus:
        scope = "_".join(
            part
            for part in [
                str(args.deployment_lane or "").replace("-", "_"),
                str(args.account_focus or "").replace("-", "_"),
            ]
            if part
        )
        safe_scope = "".join(
            char if char.isalnum() or char == "_" else "_"
            for char in scope
        ).strip("_")
        manifest_name = f"ensemble_shadow_candidate_{safe_scope}.json"
    manifest_path = PROMOTIONS_ROOT / manifest_name
    save_json(manifest_path, manifest)
    progress(f"ensemble manifest written manifest={manifest_path}")
    print(json.dumps({
        "detail_path": str(detail_path),
        "selected_records_path": str(selected_path),
        "manifest_path": str(manifest_path),
        "artifact_path": artifact_path,
        "members": len(members),
        "gate_passed": result["gate"].get("passed"),
        "research_score": result["research_score"],
        "selected_threshold": result["selected_threshold"],
        "arima_promotion_ready_count": result["arima_baseline"].get(
            "promotion_ready_count"
        ),
    }, indent=2, default=json_safe))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
