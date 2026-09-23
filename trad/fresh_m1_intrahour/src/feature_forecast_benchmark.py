from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import ExtraTreesClassifier, HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    balanced_accuracy_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from .common import ENGINE_ROOT, ROOT, utc_now_stamp
from .forecast_transformers import StableFSelector
from .htf_corrected_rebuild import SOURCE_DATASET, corrected_feature_families


REPORT_VERSION = "forex_feature_forecast_benchmark_v4"
PRIMARY_TARGET = "two_hour_mid_direction_up"
OPPORTUNITY_TARGET = "two_hour_cost_survival"
DEFAULT_LABELS_PATH = (
    ENGINE_ROOT
    / "reports"
    / "htf_corrected_arima_features_tier1_2026_20260712"
    / "corrected_h1_labels.parquet"
)
DEFAULT_CONFIG_PATH = ENGINE_ROOT / "config.json"
PAIR_FEATURE_PREFIX = "pair_identity__"
INTERACTION_PREFIX = "causal_interaction__"
CALENDAR_PREFIX = "calendar__"
CURRENCY_BASE_PREFIX = "currency_base__"
CURRENCY_QUOTE_PREFIX = "currency_quote__"
TWO_STAGE_OPPORTUNITY_THRESHOLDS = tuple(
    round(value, 3) for value in np.arange(0.50, 0.901, 0.025)
)
TWO_STAGE_DIRECTION_CONFIDENCES = (0.0, 0.005, 0.01, 0.015, 0.02, 0.03, 0.04, 0.05, 0.075, 0.10)
TWO_STAGE_MINIMUM_ROWS = 5_000
TWO_STAGE_MINIMUM_ROWS_PER_FOLD = 500


def safety_fields() -> dict[str, Any]:
    return {
        "research_only": True,
        "offline_only": True,
        "network_accessed": False,
        "credentials_accessed": False,
        "broker_api_used": False,
        "account_data_accessed": False,
        "order_placement_used": False,
        "live_execution_enabled": False,
        "demo_execution_enabled": False,
        "oanda_execution_enabled": False,
        "mt5_execution_enabled": False,
        "production_ready": False,
    }


@dataclass(frozen=True)
class TemporalFold:
    fold_id: str
    test_start: str
    test_end: str

    @property
    def start(self) -> pd.Timestamp:
        return pd.Timestamp(self.test_start)

    @property
    def end(self) -> pd.Timestamp:
        return pd.Timestamp(self.test_end)


@dataclass(frozen=True)
class BenchmarkConfig:
    max_train_rows: int = 100_000
    minimum_train_rows: int = 20_000
    minimum_test_rows: int = 2_000
    embargo_minutes: int = 60
    random_state: int = 42
    n_jobs: int = 2
    evaluation_start: str = "2026-01-01T00:00:00Z"
    evaluation_end: str = "2026-07-07T00:00:00Z"

    def __post_init__(self) -> None:
        if self.max_train_rows < self.minimum_train_rows:
            raise ValueError("max_train_rows must be at least minimum_train_rows")
        if self.minimum_test_rows < 100:
            raise ValueError("minimum_test_rows must be at least 100")
        if self.embargo_minutes < 0:
            raise ValueError("embargo_minutes cannot be negative")
        if self.n_jobs < 1:
            raise ValueError("n_jobs must be positive")


@dataclass(frozen=True)
class ModelSpec:
    model_id: str
    model_family: str
    feature_family: str
    selector_k: int | None = None


TARGET_DEFINITIONS: dict[str, dict[str, Any]] = {
    PRIMARY_TARGET: {
        "description": "Whether the executable-path two-hour mid-price move is upward.",
        "kind": "direction",
        "required_columns": ["long_gross_endpoint_pips"],
    },
    OPPORTUNITY_TARGET: {
        "description": "Whether either side has positive endpoint pips after observed spread and static slippage.",
        "kind": "opportunity",
        "required_columns": ["long_endpoint_net_pips", "short_endpoint_net_pips"],
    },
}


INTERACTION_INPUTS: tuple[tuple[str, str, str], ...] = (
    ("momentum_30_atr", "trend_consistency_60", "momentum30_x_trend_consistency"),
    ("momentum_60_atr", "regime_trend_strength", "momentum60_x_regime_trend"),
    ("momentum_30_atr", "strength_gap_60", "momentum30_x_strength_gap60"),
    ("momentum_15_atr", "regime_acceleration_score", "momentum15_x_regime_acceleration"),
    ("macd_hist_atr", "atr15_to_atr240", "macd_hist_x_volatility_ratio"),
    ("donchian_60_breakout_atr", "volume_z_30", "breakout_x_volume"),
    ("compression_30", "realized_vol_ratio_30_240", "compression_x_volatility_ratio"),
    ("range_position_60_centered", "trend_consistency_60", "range_position_x_trend"),
    ("strength_gap_15", "strength_gap_60", "strength15_x_strength60"),
    ("spread_to_atr240", "regime_spread_cost_score", "spread_ratio_x_spread_regime"),
    ("liquidity_quality_score", "volatility_expansion_pressure", "liquidity_x_volatility_expansion"),
    ("risk_on_long_pressure", "carry_trend_alignment", "risk_on_x_carry_alignment"),
)


def development_folds() -> tuple[TemporalFold, ...]:
    return (
        TemporalFold("2025Q1", "2025-01-01T00:00:00Z", "2025-04-01T00:00:00Z"),
        TemporalFold("2025Q2", "2025-04-01T00:00:00Z", "2025-07-01T00:00:00Z"),
        TemporalFold("2025Q3", "2025-07-01T00:00:00Z", "2025-10-01T00:00:00Z"),
        TemporalFold("2025Q4", "2025-10-01T00:00:00Z", "2026-01-01T00:00:00Z"),
    )


def candidate_specs(*, quick: bool = False) -> tuple[ModelSpec, ...]:
    specs = (
        ModelSpec("logistic_l2_k40", "logistic_l2", "safe_plus_interactions", 40),
        ModelSpec("logistic_l2_k100", "logistic_l2", "safe_plus_interactions", 100),
        ModelSpec("sgd_elastic_net_full", "sgd_elastic_net", "safe_plus_interactions"),
        ModelSpec("hist_gradient_boosting_motion", "hist_gradient_boosting", "motion_plus_interactions"),
        ModelSpec("hist_gradient_boosting_full", "hist_gradient_boosting", "safe_plus_interactions"),
        ModelSpec("extra_trees_k100", "extra_trees", "safe_plus_interactions", 100),
        ModelSpec("random_forest_k100", "random_forest", "safe_plus_interactions", 100),
    )
    return specs[:3] if quick else specs


def ensemble_definitions(*, quick: bool = False) -> dict[str, tuple[str, ...]]:
    if quick:
        return {
            "blend_logistic_sparse": ("logistic_l2_k40", "logistic_l2_k100"),
        }
    return {
        "blend_logistic_hgb": ("logistic_l2_k100", "hist_gradient_boosting_full"),
        "blend_tree_models": ("hist_gradient_boosting_full", "extra_trees_k100"),
        "blend_forest_models": ("extra_trees_k100", "random_forest_k100"),
        "blend_linear_and_trees": (
            "logistic_l2_k100",
            "hist_gradient_boosting_full",
            "extra_trees_k100",
            "random_forest_k100",
        ),
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_fingerprint(value: Any) -> str:
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def assert_offline_config(path: Path = DEFAULT_CONFIG_PATH) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    execution = payload.get("execution") or {}
    unsafe = [
        key
        for key in ("live_execution_enabled", "demo_execution_enabled")
        if bool(execution.get(key, False))
    ]
    if unsafe:
        raise RuntimeError(f"Offline benchmark refused unsafe config flags: {unsafe}")


def target_values(frame: pd.DataFrame, target_id: str) -> pd.Series:
    if target_id == PRIMARY_TARGET:
        return (pd.to_numeric(frame["long_gross_endpoint_pips"], errors="coerce") > 0.0).astype(int)
    if target_id == "two_hour_cost_survival":
        long_net = pd.to_numeric(frame["long_endpoint_net_pips"], errors="coerce")
        short_net = pd.to_numeric(frame["short_endpoint_net_pips"], errors="coerce")
        return pd.Series(
            (np.maximum(long_net.to_numpy(), short_net.to_numpy()) > 0.0).astype(int),
            index=frame.index,
            name=target_id,
        )
    raise KeyError(f"Unknown target: {target_id}")


def add_causal_interactions(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    output = frame.copy()
    columns: list[str] = []
    for left, right, name in INTERACTION_INPUTS:
        if left not in output or right not in output:
            continue
        column = f"{INTERACTION_PREFIX}{name}"
        output[column] = (
            pd.to_numeric(output[left], errors="coerce")
            * pd.to_numeric(output[right], errors="coerce")
        ).astype("float32")
        columns.append(column)
    return output, columns


def add_calendar_features(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    output = frame.copy()
    decision_time = pd.to_datetime(output["decision_time_utc"], utc=True, errors="coerce")
    hour = decision_time.dt.hour.astype(float)
    day_of_week = decision_time.dt.dayofweek.astype(float)
    definitions = {
        "hour_sin": np.sin(2.0 * np.pi * hour / 24.0),
        "hour_cos": np.cos(2.0 * np.pi * hour / 24.0),
        "day_of_week_sin": np.sin(2.0 * np.pi * day_of_week / 7.0),
        "day_of_week_cos": np.cos(2.0 * np.pi * day_of_week / 7.0),
    }
    columns: list[str] = []
    for name, values in definitions.items():
        column = f"{CALENDAR_PREFIX}{name}"
        output[column] = values.astype("float32")
        columns.append(column)
    return output, columns


def add_currency_identity_features(
    frame: pd.DataFrame,
    pairs: Sequence[str],
) -> tuple[pd.DataFrame, list[str]]:
    output = frame.copy()
    parsed_pairs = [str(pair).split("_", 1) for pair in pairs]
    currencies = sorted(
        {part for parsed in parsed_pairs for part in parsed if part}
    )
    instruments = output["instrument"].astype(str)
    base = instruments.str.split("_", n=1).str[0]
    quote = instruments.str.split("_", n=1).str[1]
    columns: list[str] = []
    for currency in currencies:
        base_column = f"{CURRENCY_BASE_PREFIX}{currency}"
        quote_column = f"{CURRENCY_QUOTE_PREFIX}{currency}"
        output[base_column] = (base == currency).astype("float32")
        output[quote_column] = (quote == currency).astype("float32")
        columns.extend([base_column, quote_column])
    return output, columns


def _read_source_columns(
    source_path: Path,
    columns: Sequence[str],
    pairs: Sequence[str],
) -> pd.DataFrame:
    try:
        return pd.read_parquet(
            source_path,
            columns=list(columns),
            filters=[("instrument", "in", list(pairs))],
        )
    except Exception:
        return pd.read_parquet(source_path, columns=list(columns))


def load_forecast_dataset(
    *,
    source_path: Path = SOURCE_DATASET,
    labels_path: Path = DEFAULT_LABELS_PATH,
    pairs: Sequence[str] | None = None,
    max_rows_per_pair: int | None = None,
) -> tuple[pd.DataFrame, dict[str, list[str]], dict[str, Any]]:
    families = corrected_feature_families()
    safe_features = list(families["safe_full_220"])
    motion_features = list(families["motion_cost_117"])
    label_columns = [
        "instrument",
        "raw_bar_time_utc",
        "decision_time_utc",
        "entry_time_utc",
        "label_end_time_utc",
        "long_gross_endpoint_pips",
        "short_gross_endpoint_pips",
        "long_endpoint_net_pips",
        "short_endpoint_net_pips",
        "pip_value_usd_per_unit",
        "fixed_units",
        "slippage_pips_round_trip",
    ]
    labels = pd.read_parquet(labels_path, columns=label_columns)
    labels["instrument"] = labels["instrument"].astype(str)
    selected_pairs = sorted(set(pairs or labels["instrument"].unique().tolist()))
    labels = labels[labels["instrument"].isin(selected_pairs)].copy()
    source = _read_source_columns(
        source_path,
        ["time_utc", "instrument", *safe_features],
        selected_pairs,
    )
    source["instrument"] = source["instrument"].astype(str)
    source = source[source["instrument"].isin(selected_pairs)].copy()
    source["raw_bar_time_utc"] = pd.to_datetime(
        source.pop("time_utc"), utc=True, errors="coerce"
    )
    labels["raw_bar_time_utc"] = pd.to_datetime(
        labels["raw_bar_time_utc"], utc=True, errors="coerce"
    )
    labels["decision_time_utc"] = pd.to_datetime(
        labels["decision_time_utc"], utc=True, errors="coerce"
    )
    labels["label_end_time_utc"] = pd.to_datetime(
        labels["label_end_time_utc"], utc=True, errors="coerce"
    )
    duplicate_source = int(source.duplicated(["instrument", "raw_bar_time_utc"]).sum())
    duplicate_labels = int(labels.duplicated(["instrument", "raw_bar_time_utc"]).sum())
    if duplicate_source or duplicate_labels:
        raise ValueError(
            f"Non-unique source keys: features={duplicate_source}, labels={duplicate_labels}"
        )
    dataset = source.merge(
        labels,
        on=["instrument", "raw_bar_time_utc"],
        how="inner",
        validate="one_to_one",
    )
    for feature in safe_features:
        dataset[feature] = pd.to_numeric(dataset[feature], errors="coerce").astype("float32")
    dataset = dataset.replace([np.inf, -np.inf], np.nan)
    dataset, interaction_columns = add_causal_interactions(dataset)
    dataset, calendar_columns = add_calendar_features(dataset)
    dataset, currency_columns = add_currency_identity_features(dataset, selected_pairs)
    pair_columns: list[str] = []
    for pair in selected_pairs:
        column = f"{PAIR_FEATURE_PREFIX}{pair}"
        dataset[column] = (dataset["instrument"] == pair).astype("float32")
        pair_columns.append(column)
    if max_rows_per_pair:
        dataset = (
            dataset.sort_values("decision_time_utc")
            .groupby("instrument", group_keys=False)
            .tail(int(max_rows_per_pair))
        )
    dataset = dataset.sort_values(["decision_time_utc", "instrument"]).reset_index(drop=True)
    dataset["forecast_row_id"] = np.arange(len(dataset), dtype=np.int64)
    if not (dataset["label_end_time_utc"] > dataset["decision_time_utc"]).all():
        raise ValueError("Forward label boundary is not strictly after decision time")
    engineered_columns = [
        *interaction_columns,
        *calendar_columns,
        *currency_columns,
        *pair_columns,
    ]
    feature_families = {
        "motion_plus_interactions": [*motion_features, *engineered_columns],
        "safe_plus_interactions": [*safe_features, *engineered_columns],
    }
    audit = {
        "source_path": str(source_path),
        "labels_path": str(labels_path),
        "row_count": int(len(dataset)),
        "pair_count": int(dataset["instrument"].nunique()),
        "pairs": selected_pairs,
        "decision_start": dataset["decision_time_utc"].min(),
        "decision_end": dataset["decision_time_utc"].max(),
        "label_end_max": dataset["label_end_time_utc"].max(),
        "safe_feature_count": len(safe_features),
        "motion_feature_count": len(motion_features),
        "causal_interaction_count": len(interaction_columns),
        "calendar_feature_count": len(calendar_columns),
        "currency_identity_feature_count": len(currency_columns),
        "pair_identity_feature_count": len(pair_columns),
        "engineered_feature_count": len(engineered_columns),
        "feature_family_counts": {
            key: len(value) for key, value in feature_families.items()
        },
        "unsafe_global_features_excluded": True,
        "forward_label_columns_excluded": True,
        "label_end_strictly_after_decision": True,
        "duplicate_source_key_count": duplicate_source,
        "duplicate_label_key_count": duplicate_labels,
    }
    return dataset, feature_families, audit


def split_temporal_fold(
    dataset: pd.DataFrame,
    fold: TemporalFold,
    *,
    embargo_minutes: int,
    max_train_rows: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    cutoff = fold.start - pd.Timedelta(minutes=embargo_minutes)
    train = dataset[dataset["label_end_time_utc"] < cutoff].copy()
    test = dataset[
        (dataset["decision_time_utc"] >= fold.start)
        & (dataset["decision_time_utc"] < fold.end)
    ].copy()
    if max_train_rows and len(train) > max_train_rows:
        train = train.sort_values("decision_time_utc").tail(int(max_train_rows)).copy()
    audit = {
        "fold_id": fold.fold_id,
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "train_start": train["decision_time_utc"].min() if not train.empty else None,
        "train_decision_end": train["decision_time_utc"].max() if not train.empty else None,
        "train_label_end_max": train["label_end_time_utc"].max() if not train.empty else None,
        "test_start": fold.start,
        "test_end": fold.end,
        "embargo_minutes": int(embargo_minutes),
        "purge_passed": bool(
            train.empty or train["label_end_time_utc"].max() < cutoff
        ),
    }
    return train, test, audit


def expected_calibration_error(
    actual: np.ndarray,
    probability: np.ndarray,
    *,
    bins: int = 10,
) -> float:
    actual = np.asarray(actual, dtype=int)
    probability = np.asarray(probability, dtype=float)
    edges = np.linspace(0.0, 1.0, bins + 1)
    total = max(1, len(actual))
    error = 0.0
    for index in range(bins):
        lower, upper = edges[index], edges[index + 1]
        mask = (probability >= lower) & (
            probability <= upper if index + 1 == bins else probability < upper
        )
        if not np.any(mask):
            continue
        error += float(mask.mean()) * abs(
            float(actual[mask].mean()) - float(probability[mask].mean())
        )
    return float(error * total / max(1, len(actual)))


def _safe_metric(metric: Callable[..., float], *args: Any, **kwargs: Any) -> float | None:
    try:
        value = float(metric(*args, **kwargs))
        return value if math.isfinite(value) else None
    except Exception:
        return None


def classification_metrics(
    actual: Sequence[int],
    probability: Sequence[float],
    *,
    baseline_probability: float,
) -> dict[str, Any]:
    y = np.asarray(actual, dtype=int)
    p = np.clip(np.asarray(probability, dtype=float), 1e-6, 1.0 - 1e-6)
    predicted = (p >= 0.5).astype(int)
    baseline = np.full(len(y), float(baseline_probability), dtype=float)
    model_brier = _safe_metric(brier_score_loss, y, p)
    baseline_brier = _safe_metric(brier_score_loss, y, baseline)
    return {
        "rows": int(len(y)),
        "positive_rate": float(y.mean()) if len(y) else None,
        "predicted_positive_rate": float(predicted.mean()) if len(y) else None,
        "accuracy": _safe_metric(accuracy_score, y, predicted),
        "balanced_accuracy": _safe_metric(balanced_accuracy_score, y, predicted),
        "roc_auc": _safe_metric(roc_auc_score, y, p),
        "average_precision": _safe_metric(average_precision_score, y, p),
        "brier_score": model_brier,
        "baseline_brier_score": baseline_brier,
        "brier_skill_score": (
            1.0 - model_brier / baseline_brier
            if model_brier is not None
            and baseline_brier is not None
            and baseline_brier > 0.0
            else None
        ),
        "log_loss": _safe_metric(log_loss, y, p, labels=[0, 1]),
        "expected_calibration_error": expected_calibration_error(y, p),
    }


def _direction_economics(rows: pd.DataFrame, probability: np.ndarray) -> dict[str, Any]:
    use_long = np.asarray(probability) >= 0.5
    long_net = pd.to_numeric(rows["long_endpoint_net_pips"], errors="coerce").to_numpy(float)
    short_net = pd.to_numeric(rows["short_endpoint_net_pips"], errors="coerce").to_numpy(float)
    selected = np.where(use_long, long_net, short_net)
    confidence = np.abs(np.asarray(probability, dtype=float) - 0.5)

    def summarize(mask: np.ndarray) -> dict[str, Any]:
        values = selected[mask]
        return {
            "rows": int(mask.sum()),
            "mean_net_pips": float(np.nanmean(values)) if len(values) else None,
            "total_net_pips": float(np.nansum(values)) if len(values) else 0.0,
            "positive_net_rate": float(np.nanmean(values > 0.0)) if len(values) else None,
        }

    return {
        "all_rows": summarize(np.ones(len(rows), dtype=bool)),
        "confidence_at_least_0p05": summarize(confidence >= 0.05),
        "confidence_at_least_0p10": summarize(confidence >= 0.10),
        "interpretation": "descriptive endpoint economics only; no deployable threshold selected",
        "static_slippage_assumption": True,
        "live_cost_verified": False,
    }


def _pair_metrics(rows: pd.DataFrame, probability: np.ndarray, target_id: str) -> list[dict[str, Any]]:
    work = rows[["instrument"]].copy()
    work["actual"] = target_values(rows, target_id).to_numpy()
    work["probability"] = probability
    output: list[dict[str, Any]] = []
    for instrument, group in work.groupby("instrument", sort=True):
        metrics = classification_metrics(
            group["actual"],
            group["probability"],
            baseline_probability=float(group["actual"].mean()),
        )
        output.append({"instrument": str(instrument), **metrics})
    return output


def build_estimator(spec: ModelSpec, config: BenchmarkConfig) -> Pipeline:
    steps: list[tuple[str, Any]] = [("imputer", SimpleImputer(strategy="median"))]
    if spec.selector_k:
        steps.append(("selector", StableFSelector(spec.selector_k)))
    if spec.model_family == "logistic_l2":
        steps.extend(
            [
                ("scaler", StandardScaler()),
                (
                    "model",
                    LogisticRegression(
                        C=0.1,
                        max_iter=1_000,
                        random_state=config.random_state,
                        solver="lbfgs",
                    ),
                ),
            ]
        )
    elif spec.model_family == "sgd_elastic_net":
        steps.extend(
            [
                ("scaler", StandardScaler()),
                (
                    "model",
                    SGDClassifier(
                        loss="log_loss",
                        penalty="elasticnet",
                        alpha=0.0001,
                        l1_ratio=0.15,
                        max_iter=2_000,
                        tol=1e-4,
                        average=True,
                        random_state=config.random_state,
                    ),
                ),
            ]
        )
    elif spec.model_family == "hist_gradient_boosting":
        steps.append(
            (
                "model",
                HistGradientBoostingClassifier(
                    learning_rate=0.05,
                    max_iter=180,
                    max_leaf_nodes=31,
                    min_samples_leaf=60,
                    l2_regularization=0.10,
                    early_stopping=True,
                    validation_fraction=0.10,
                    n_iter_no_change=12,
                    random_state=config.random_state,
                ),
            )
        )
    elif spec.model_family == "extra_trees":
        steps.append(
            (
                "model",
                ExtraTreesClassifier(
                    n_estimators=240,
                    max_depth=14,
                    min_samples_leaf=24,
                    max_features="sqrt",
                    class_weight="balanced",
                    n_jobs=config.n_jobs,
                    random_state=config.random_state,
                ),
            )
        )
    elif spec.model_family == "random_forest":
        steps.append(
            (
                "model",
                RandomForestClassifier(
                    n_estimators=260,
                    max_depth=12,
                    min_samples_leaf=40,
                    max_features="sqrt",
                    class_weight="balanced_subsample",
                    n_jobs=config.n_jobs,
                    random_state=config.random_state,
                ),
            )
        )
    else:
        raise KeyError(f"Unsupported model family: {spec.model_family}")
    return Pipeline(steps)


def _selected_features(model: Pipeline, features: Sequence[str]) -> list[str]:
    selector = model.named_steps.get("selector")
    if selector is None:
        return list(features)
    support = selector.get_support()
    return [feature for feature, selected in zip(features, support) if selected]


def _fold_prediction_frame(
    rows: pd.DataFrame,
    probability: np.ndarray,
    *,
    fold_id: str,
    target_id: str,
    candidate_id: str,
    train_prior: float,
) -> pd.DataFrame:
    keep = [
        "forecast_row_id",
        "instrument",
        "decision_time_utc",
        "label_end_time_utc",
        "momentum_30_atr",
        "long_gross_endpoint_pips",
        "short_gross_endpoint_pips",
        "long_endpoint_net_pips",
        "short_endpoint_net_pips",
    ]
    output = rows[keep].copy()
    output["fold_id"] = fold_id
    output["target_id"] = target_id
    output["candidate_id"] = candidate_id
    output["actual"] = target_values(rows, target_id).to_numpy()
    output["probability"] = np.asarray(probability, dtype=float)
    output["train_prior"] = float(train_prior)
    return output


def _aggregate_report(
    *,
    candidate_id: str,
    model_family: str,
    feature_family: str,
    target_id: str,
    folds: list[dict[str, Any]],
    predictions: pd.DataFrame,
    selected_features: Iterable[str] = (),
    ensemble_members: Sequence[str] = (),
) -> dict[str, Any]:
    metric_names = (
        "accuracy",
        "balanced_accuracy",
        "roc_auc",
        "average_precision",
        "brier_skill_score",
        "log_loss",
        "expected_calibration_error",
    )
    aggregate: dict[str, Any] = {}
    for metric in metric_names:
        values = [
            float(row["metrics"][metric])
            for row in folds
            if row["metrics"].get(metric) is not None
        ]
        aggregate[f"mean_{metric}"] = float(np.mean(values)) if values else None
        aggregate[f"minimum_{metric}"] = float(np.min(values)) if values else None
        aggregate[f"maximum_{metric}"] = float(np.max(values)) if values else None
    aucs = [row["metrics"].get("roc_auc") for row in folds]
    aggregate["positive_auc_fold_count"] = int(
        sum(value is not None and float(value) > 0.5 for value in aucs)
    )
    pair_rows = _pair_metrics(predictions, predictions["probability"].to_numpy(), target_id)
    eligible_pair_aucs = [
        float(row["roc_auc"])
        for row in pair_rows
        if row.get("roc_auc") is not None
    ]
    aggregate["pair_auc_above_0p5_count"] = int(sum(value > 0.5 for value in eligible_pair_aucs))
    aggregate["pair_auc_count"] = len(eligible_pair_aucs)
    aggregate["pair_auc_above_0p5_fraction"] = (
        float(np.mean(np.asarray(eligible_pair_aucs) > 0.5))
        if eligible_pair_aucs
        else None
    )
    aggregate["mean_pair_auc"] = float(np.mean(eligible_pair_aucs)) if eligible_pair_aucs else None
    feature_counts = Counter(selected_features)
    return {
        **safety_fields(),
        "candidate_id": candidate_id,
        "model_family": model_family,
        "feature_family": feature_family,
        "target_id": target_id,
        "is_baseline": False,
        "ensemble_members": list(ensemble_members),
        "folds": folds,
        "fold_count": len(folds),
        "aggregate_metrics": aggregate,
        "pair_metrics": pair_rows,
        "selected_feature_frequency": [
            {"feature": feature, "fold_count": count}
            for feature, count in feature_counts.most_common()
        ],
    }


def evaluate_candidate(
    dataset: pd.DataFrame,
    feature_families: Mapping[str, list[str]],
    spec: ModelSpec,
    *,
    target_id: str,
    config: BenchmarkConfig,
) -> tuple[dict[str, Any], pd.DataFrame]:
    features = feature_families[spec.feature_family]
    fold_reports: list[dict[str, Any]] = []
    prediction_parts: list[pd.DataFrame] = []
    selected_features: list[str] = []
    for fold in development_folds():
        train, test, split_audit = split_temporal_fold(
            dataset,
            fold,
            embargo_minutes=config.embargo_minutes,
            max_train_rows=config.max_train_rows,
        )
        if len(train) < config.minimum_train_rows or len(test) < config.minimum_test_rows:
            raise ValueError(
                f"Insufficient fold rows {fold.fold_id}: train={len(train)} test={len(test)}"
            )
        y_train = target_values(train, target_id)
        y_test = target_values(test, target_id)
        if y_train.nunique() < 2 or y_test.nunique() < 2:
            raise ValueError(f"Single-class target in fold {fold.fold_id}")
        model = build_estimator(spec, config)
        model.fit(train[features], y_train)
        probability = model.predict_proba(test[features])[:, 1]
        prior = float(y_train.mean())
        metrics = classification_metrics(y_test, probability, baseline_probability=prior)
        selected_features.extend(_selected_features(model, features))
        fold_report = {
            **split_audit,
            "train_positive_rate": prior,
            "test_positive_rate": float(y_test.mean()),
            "metrics": metrics,
            "pair_metrics": _pair_metrics(test, probability, target_id),
        }
        if TARGET_DEFINITIONS[target_id]["kind"] == "direction":
            fold_report["cost_diagnostics"] = _direction_economics(test, probability)
        fold_reports.append(fold_report)
        prediction_parts.append(
            _fold_prediction_frame(
                test,
                probability,
                fold_id=fold.fold_id,
                target_id=target_id,
                candidate_id=spec.model_id,
                train_prior=prior,
            )
        )
    predictions = pd.concat(prediction_parts, ignore_index=True)
    return (
        _aggregate_report(
            candidate_id=spec.model_id,
            model_family=spec.model_family,
            feature_family=spec.feature_family,
            target_id=target_id,
            folds=fold_reports,
            predictions=predictions,
            selected_features=selected_features,
        ),
        predictions,
    )


def _reports_from_predictions(
    predictions: pd.DataFrame,
    *,
    candidate_id: str,
    model_family: str,
    feature_family: str,
    target_id: str,
    ensemble_members: Sequence[str] = (),
) -> dict[str, Any]:
    fold_reports: list[dict[str, Any]] = []
    for fold_id, rows in predictions.groupby("fold_id", sort=True):
        metrics = classification_metrics(
            rows["actual"],
            rows["probability"],
            baseline_probability=float(rows["train_prior"].iloc[0]),
        )
        report = {
            "fold_id": str(fold_id),
            "train_positive_rate": float(rows["train_prior"].iloc[0]),
            "test_positive_rate": float(rows["actual"].mean()),
            "test_rows": int(len(rows)),
            "metrics": metrics,
            "pair_metrics": _pair_metrics(rows, rows["probability"].to_numpy(), target_id),
        }
        if TARGET_DEFINITIONS[target_id]["kind"] == "direction":
            report["cost_diagnostics"] = _direction_economics(
                rows, rows["probability"].to_numpy()
            )
        fold_reports.append(report)
    return _aggregate_report(
        candidate_id=candidate_id,
        model_family=model_family,
        feature_family=feature_family,
        target_id=target_id,
        folds=fold_reports,
        predictions=predictions,
        ensemble_members=ensemble_members,
    )


def build_ensemble_predictions(
    component_predictions: Mapping[str, pd.DataFrame],
    *,
    ensemble_id: str,
    members: Sequence[str],
) -> pd.DataFrame:
    missing = [member for member in members if member not in component_predictions]
    if missing:
        raise KeyError(f"Missing ensemble components: {missing}")
    ordered = [
        component_predictions[member].sort_values("forecast_row_id").reset_index(drop=True)
        for member in members
    ]
    reference = ordered[0].copy()
    for member, frame in zip(members[1:], ordered[1:]):
        if not reference["forecast_row_id"].equals(frame["forecast_row_id"]):
            raise ValueError(f"Ensemble row mismatch for {member}")
    reference["probability"] = np.mean(
        np.column_stack([frame["probability"].to_numpy(float) for frame in ordered]),
        axis=1,
    )
    reference["candidate_id"] = ensemble_id
    return reference


def development_gate(report: Mapping[str, Any]) -> dict[str, Any]:
    metrics = report["aggregate_metrics"]
    checks = {
        "four_temporal_folds": int(report["fold_count"]) == 4,
        "mean_auc_at_least_0p55": float(metrics.get("mean_roc_auc") or 0.0) >= 0.55,
        "minimum_fold_auc_above_0p50": float(metrics.get("minimum_roc_auc") or 0.0) > 0.50,
        "all_fold_aucs_above_0p50": int(metrics.get("positive_auc_fold_count") or 0) == 4,
        "mean_balanced_accuracy_at_least_0p52": float(
            metrics.get("mean_balanced_accuracy") or 0.0
        )
        >= 0.52,
        "positive_mean_brier_skill": float(metrics.get("mean_brier_skill_score") or -1.0) > 0.0,
        "pair_auc_breadth_at_least_60pct": float(
            metrics.get("pair_auc_above_0p5_fraction") or 0.0
        )
        >= 0.60,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "passed_check_count": int(sum(checks.values())),
        "total_check_count": len(checks),
    }


def _ranking_key(report: Mapping[str, Any]) -> tuple[Any, ...]:
    metrics = report["aggregate_metrics"]
    gate = report["development_gate"]
    return (
        bool(gate["passed"]),
        int(gate["passed_check_count"]),
        float(metrics.get("minimum_roc_auc") or -1.0),
        float(metrics.get("mean_roc_auc") or -1.0),
        float(metrics.get("mean_brier_skill_score") or -1.0),
        -float(metrics.get("mean_expected_calibration_error") or 1.0),
        str(report["candidate_id"]),
    )


def rank_reports(reports: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = [dict(report) for report in reports]
    for row in rows:
        row["development_gate"] = development_gate(row)
    return sorted(rows, key=_ranking_key, reverse=True)


def _fit_components(
    train: pd.DataFrame,
    test: pd.DataFrame,
    *,
    target_id: str,
    selected_id: str,
    specs_by_id: Mapping[str, ModelSpec],
    ensembles: Mapping[str, tuple[str, ...]],
    feature_families: Mapping[str, list[str]],
    config: BenchmarkConfig,
) -> tuple[np.ndarray, list[dict[str, Any]]]:
    component_ids = ensembles.get(selected_id, (selected_id,))
    y_train = target_values(train, target_id)
    probabilities: list[np.ndarray] = []
    fitted: list[dict[str, Any]] = []
    for component_id in component_ids:
        spec = specs_by_id[component_id]
        features = feature_families[spec.feature_family]
        model = build_estimator(spec, config)
        model.fit(train[features], y_train)
        probabilities.append(model.predict_proba(test[features])[:, 1])
        fitted.append(
            {
                "model_id": component_id,
                "spec": asdict(spec),
                "features": list(features),
                "selected_features": _selected_features(model, features),
                "estimator": model,
            }
        )
    return np.mean(np.column_stack(probabilities), axis=1), fitted


def frozen_diagnostic_evaluation(
    dataset: pd.DataFrame,
    *,
    target_id: str,
    selected_report: Mapping[str, Any],
    specs_by_id: Mapping[str, ModelSpec],
    ensembles: Mapping[str, tuple[str, ...]],
    feature_families: Mapping[str, list[str]],
    config: BenchmarkConfig,
) -> tuple[dict[str, Any], pd.DataFrame, list[dict[str, Any]]]:
    fold = TemporalFold("previously_inspected_2026", config.evaluation_start, config.evaluation_end)
    train, test, split_audit = split_temporal_fold(
        dataset,
        fold,
        embargo_minutes=config.embargo_minutes,
        max_train_rows=config.max_train_rows,
    )
    probability, fitted = _fit_components(
        train,
        test,
        target_id=target_id,
        selected_id=str(selected_report["candidate_id"]),
        specs_by_id=specs_by_id,
        ensembles=ensembles,
        feature_families=feature_families,
        config=config,
    )
    prior = float(target_values(train, target_id).mean())
    metrics = classification_metrics(
        target_values(test, target_id),
        probability,
        baseline_probability=prior,
    )
    output = {
        **safety_fields(),
        **split_audit,
        "candidate_id": selected_report["candidate_id"],
        "target_id": target_id,
        "evidence_status": "previously_inspected_diagnostic_not_fresh_holdout",
        "fresh_holdout": False,
        "metrics": metrics,
        "pair_metrics": _pair_metrics(test, probability, target_id),
        "cost_diagnostics": (
            _direction_economics(test, probability)
            if TARGET_DEFINITIONS[target_id]["kind"] == "direction"
            else None
        ),
    }
    predictions = _fold_prediction_frame(
        test,
        probability,
        fold_id=fold.fold_id,
        target_id=target_id,
        candidate_id=str(selected_report["candidate_id"]),
        train_prior=prior,
    )
    return output, predictions, fitted


def _future_research_bundle(
    dataset: pd.DataFrame,
    *,
    target_id: str,
    selected_report: Mapping[str, Any],
    specs_by_id: Mapping[str, ModelSpec],
    ensembles: Mapping[str, tuple[str, ...]],
    feature_families: Mapping[str, list[str]],
    config: BenchmarkConfig,
) -> dict[str, Any]:
    train = dataset.sort_values("decision_time_utc")
    if len(train) > config.max_train_rows:
        train = train.tail(config.max_train_rows).copy()
    probability, fitted = _fit_components(
        train,
        train.iloc[:1].copy(),
        target_id=target_id,
        selected_id=str(selected_report["candidate_id"]),
        specs_by_id=specs_by_id,
        ensembles=ensembles,
        feature_families=feature_families,
        config=config,
    )
    del probability
    return {
        **safety_fields(),
        "bundle_version": REPORT_VERSION,
        "artifact_role": "offline_research_candidate_only",
        "candidate_id": selected_report["candidate_id"],
        "target_id": target_id,
        "target_definition": TARGET_DEFINITIONS[target_id],
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "training_rows": int(len(train)),
        "training_decision_start": train["decision_time_utc"].min(),
        "training_decision_end": train["decision_time_utc"].max(),
        "training_label_end": train["label_end_time_utc"].max(),
        "development_gate": selected_report["development_gate"],
        "confirmed_accurate_forecast": False,
        "fresh_holdout_required": True,
        "components": fitted,
    }


def _baseline_reports(dataset: pd.DataFrame, target_id: str, config: BenchmarkConfig) -> list[dict[str, Any]]:
    constant_folds: list[dict[str, Any]] = []
    momentum_folds: list[dict[str, Any]] = []
    constant_predictions: list[pd.DataFrame] = []
    momentum_predictions: list[pd.DataFrame] = []
    for fold in development_folds():
        train, test, split_audit = split_temporal_fold(
            dataset,
            fold,
            embargo_minutes=config.embargo_minutes,
            max_train_rows=config.max_train_rows,
        )
        prior = float(target_values(train, target_id).mean())
        actual = target_values(test, target_id)
        constant = np.full(len(test), prior, dtype=float)
        momentum = np.where(
            pd.to_numeric(test["momentum_30_atr"], errors="coerce").fillna(0.0).to_numpy() >= 0.0,
            0.999,
            0.001,
        )
        constant_folds.append(
            {**split_audit, "metrics": classification_metrics(actual, constant, baseline_probability=prior)}
        )
        momentum_folds.append(
            {**split_audit, "metrics": classification_metrics(actual, momentum, baseline_probability=prior)}
        )
        constant_predictions.append(
            _fold_prediction_frame(
                test,
                constant,
                fold_id=fold.fold_id,
                target_id=target_id,
                candidate_id="constant_training_prior",
                train_prior=prior,
            )
        )
        momentum_predictions.append(
            _fold_prediction_frame(
                test,
                momentum,
                fold_id=fold.fold_id,
                target_id=target_id,
                candidate_id="momentum_30_sign",
                train_prior=prior,
            )
        )
    reports = []
    for candidate_id, family, folds, parts in (
        ("constant_training_prior", "constant_baseline", constant_folds, constant_predictions),
        ("momentum_30_sign", "momentum_baseline", momentum_folds, momentum_predictions),
    ):
        predictions = pd.concat(parts, ignore_index=True)
        report = _aggregate_report(
            candidate_id=candidate_id,
            model_family=family,
            feature_family="baseline",
            target_id=target_id,
            folds=folds,
            predictions=predictions,
        )
        report["is_baseline"] = True
        report["development_gate"] = {"passed": False, "reason": "baseline_not_promotable"}
        reports.append(report)
    return reports


def _candidate_summary(report: Mapping[str, Any]) -> dict[str, Any]:
    metrics = report["aggregate_metrics"]
    return {
        "candidate_id": report["candidate_id"],
        "model_family": report["model_family"],
        "feature_family": report["feature_family"],
        "ensemble_members": report.get("ensemble_members") or [],
        "development_gate_passed": bool(report["development_gate"].get("passed", False)),
        "development_gate_checks_passed": report["development_gate"].get("passed_check_count"),
        "mean_auc": metrics.get("mean_roc_auc"),
        "minimum_fold_auc": metrics.get("minimum_roc_auc"),
        "mean_balanced_accuracy": metrics.get("mean_balanced_accuracy"),
        "mean_brier_skill": metrics.get("mean_brier_skill_score"),
        "mean_calibration_error": metrics.get("mean_expected_calibration_error"),
        "positive_auc_folds": metrics.get("positive_auc_fold_count"),
        "pair_auc_breadth": metrics.get("pair_auc_above_0p5_fraction"),
    }


def _merge_two_stage_predictions(
    direction: pd.DataFrame,
    opportunity: pd.DataFrame,
) -> pd.DataFrame:
    if direction["forecast_row_id"].duplicated().any():
        raise ValueError("Direction predictions contain duplicate forecast_row_id values")
    if opportunity["forecast_row_id"].duplicated().any():
        raise ValueError("Opportunity predictions contain duplicate forecast_row_id values")
    opportunity_columns = opportunity[
        ["forecast_row_id", "actual", "probability", "candidate_id"]
    ].rename(
        columns={
            "actual": "opportunity_actual",
            "probability": "opportunity_probability",
            "candidate_id": "opportunity_candidate_id",
        }
    )
    work = direction.rename(
        columns={
            "probability": "direction_probability",
            "candidate_id": "direction_candidate_id",
        }
    ).merge(opportunity_columns, on="forecast_row_id", how="inner", validate="one_to_one")
    if len(work) != len(direction) or len(work) != len(opportunity):
        raise ValueError("Direction and opportunity predictions do not cover identical rows")
    work["direction_prediction"] = (work["direction_probability"] >= 0.5).astype(int)
    work["direction_confidence"] = (work["direction_probability"] - 0.5).abs()
    return work


def _two_stage_subset_metrics(frame: pd.DataFrame, selected: pd.Series) -> dict[str, Any] | None:
    subset = frame.loc[selected].copy()
    if subset.empty or subset["actual"].nunique() < 2:
        return None
    metrics = classification_metrics(
        subset["actual"],
        subset["direction_probability"],
        baseline_probability=float(subset["actual"].mean()),
    )
    choose_long = subset["direction_prediction"].to_numpy(dtype=bool)
    selected_net_pips = np.where(
        choose_long,
        pd.to_numeric(subset["long_endpoint_net_pips"], errors="coerce").to_numpy(float),
        pd.to_numeric(subset["short_endpoint_net_pips"], errors="coerce").to_numpy(float),
    )
    return {
        **metrics,
        "coverage": float(len(subset) / len(frame)),
        "descriptive_selected_side_mean_net_pips": float(np.nanmean(selected_net_pips)),
        "descriptive_selected_side_positive_net_rate": float(np.nanmean(selected_net_pips > 0.0)),
        "overlapping_endpoint_labels": True,
        "profitability_metric": False,
        "static_cost_assumption": True,
        "live_cost_verified": False,
    }


def _two_stage_candidate(
    frame: pd.DataFrame,
    *,
    opportunity_threshold: float,
    direction_confidence: float,
    enforce_minimums: bool,
) -> dict[str, Any] | None:
    selected = (frame["opportunity_probability"] >= opportunity_threshold) & (
        frame["direction_confidence"] >= direction_confidence
    )
    aggregate = _two_stage_subset_metrics(frame, selected)
    if aggregate is None:
        return None
    folds: list[dict[str, Any]] = []
    for fold_id, fold in frame.groupby("fold_id", sort=True):
        fold_selected = selected.loc[fold.index]
        metrics = _two_stage_subset_metrics(fold, fold_selected)
        if metrics is None:
            return None
        if enforce_minimums and metrics["rows"] < TWO_STAGE_MINIMUM_ROWS_PER_FOLD:
            return None
        folds.append({"fold_id": str(fold_id), **metrics})
    if enforce_minimums and aggregate["rows"] < TWO_STAGE_MINIMUM_ROWS:
        return None

    def finite_or(value: Any, default: float = -1.0) -> float:
        return float(value) if value is not None and math.isfinite(float(value)) else default

    return {
        "opportunity_threshold": float(opportunity_threshold),
        "direction_confidence": float(direction_confidence),
        "aggregate_metrics": aggregate,
        "fold_metrics": folds,
        "minimum_fold_accuracy": min(finite_or(row["accuracy"]) for row in folds),
        "mean_fold_accuracy": float(np.mean([finite_or(row["accuracy"]) for row in folds])),
        "minimum_fold_balanced_accuracy": min(
            finite_or(row["balanced_accuracy"]) for row in folds
        ),
        "mean_fold_balanced_accuracy": float(
            np.mean([finite_or(row["balanced_accuracy"]) for row in folds])
        ),
        "minimum_fold_auc": min(finite_or(row["roc_auc"]) for row in folds),
        "mean_fold_auc": float(np.mean([finite_or(row["roc_auc"]) for row in folds])),
    }


def _two_stage_ranking_key(candidate: Mapping[str, Any]) -> tuple[Any, ...]:
    metrics = candidate["aggregate_metrics"]
    return (
        float(candidate["minimum_fold_balanced_accuracy"]),
        float(candidate["mean_fold_balanced_accuracy"]),
        float(candidate["minimum_fold_auc"]),
        float(candidate["mean_fold_auc"]),
        int(metrics["rows"]),
        -float(candidate["opportunity_threshold"]),
        -float(candidate["direction_confidence"]),
    )


def evaluate_two_stage_forecast(
    development_direction: pd.DataFrame,
    development_opportunity: pd.DataFrame,
    diagnostic_direction: pd.DataFrame,
    diagnostic_opportunity: pd.DataFrame,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    development = _merge_two_stage_predictions(
        development_direction,
        development_opportunity,
    )
    diagnostic = _merge_two_stage_predictions(
        diagnostic_direction,
        diagnostic_opportunity,
    )
    candidates: list[dict[str, Any]] = []
    for opportunity_threshold in TWO_STAGE_OPPORTUNITY_THRESHOLDS:
        for direction_confidence in TWO_STAGE_DIRECTION_CONFIDENCES:
            candidate = _two_stage_candidate(
                development,
                opportunity_threshold=opportunity_threshold,
                direction_confidence=direction_confidence,
                enforce_minimums=True,
            )
            if candidate is not None:
                candidates.append(candidate)
    if not candidates:
        raise ValueError("No two-stage threshold candidate met the declared sample minimums")
    ranked = sorted(candidates, key=_two_stage_ranking_key, reverse=True)
    selected = ranked[0]
    diagnostic_result = _two_stage_candidate(
        diagnostic,
        opportunity_threshold=float(selected["opportunity_threshold"]),
        direction_confidence=float(selected["direction_confidence"]),
        enforce_minimums=False,
    )
    if diagnostic_result is None:
        raise ValueError("Selected two-stage threshold produced no diagnostic observations")
    minimum_balanced = float(selected["minimum_fold_balanced_accuracy"])
    mean_balanced = float(selected["mean_fold_balanced_accuracy"])
    gate_checks = {
        "minimum_fold_balanced_accuracy_at_least_0p55": minimum_balanced >= 0.55,
        "mean_fold_balanced_accuracy_at_least_0p55": mean_balanced >= 0.55,
        "every_fold_accuracy_above_0p50": all(
            float(row["accuracy"] or 0.0) > 0.50 for row in selected["fold_metrics"]
        ),
        "development_rows_at_least_5000": (
            int(selected["aggregate_metrics"]["rows"]) >= TWO_STAGE_MINIMUM_ROWS
        ),
        "development_coverage_at_least_0p10": (
            float(selected["aggregate_metrics"]["coverage"]) >= 0.10
        ),
    }
    development["selected_by_two_stage_policy"] = (
        development["opportunity_probability"] >= selected["opportunity_threshold"]
    ) & (development["direction_confidence"] >= selected["direction_confidence"])
    diagnostic["selected_by_two_stage_policy"] = (
        diagnostic["opportunity_probability"] >= selected["opportunity_threshold"]
    ) & (diagnostic["direction_confidence"] >= selected["direction_confidence"])
    report = {
        **safety_fields(),
        "artifact_role": "offline_research_two_stage_direction_filter",
        "selection_data": "2025 purged out_of_sample predictions only",
        "diagnostic_data": "previously inspected 2026 data; not used for selection",
        "threshold_grid_declared_before_diagnostic": True,
        "opportunity_thresholds": list(TWO_STAGE_OPPORTUNITY_THRESHOLDS),
        "direction_confidences": list(TWO_STAGE_DIRECTION_CONFIDENCES),
        "minimum_rows": TWO_STAGE_MINIMUM_ROWS,
        "minimum_rows_per_fold": TWO_STAGE_MINIMUM_ROWS_PER_FOLD,
        "eligible_candidate_count": len(ranked),
        "selection_ranking": [
            "minimum_fold_balanced_accuracy",
            "mean_fold_balanced_accuracy",
            "minimum_fold_auc",
            "mean_fold_auc",
            "row_count",
        ],
        "selected_development_policy": selected,
        "development_gate": {
            "passed": all(gate_checks.values()),
            "checks": gate_checks,
            "passed_check_count": int(sum(gate_checks.values())),
            "total_check_count": len(gate_checks),
        },
        "leaderboard": ranked,
        "diagnostic_2026": diagnostic_result,
        "confirmed_accurate_directional_forecast": False,
        "fresh_holdout": False,
        "profitability_claimed": False,
        "interpretation": (
            "The opportunity model can filter for larger cost-surviving moves, but the remaining "
            "directional accuracy must independently clear the directional development gate."
        ),
    }
    return report, development, diagnostic


def _render_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Forex Feature Forecast Benchmark",
        "",
        "Offline research only. No broker API, credentials, account data, order placement, demo execution, or live execution was used.",
        "",
        "## Result",
        "",
        f"- Confirmed accurate forecast: `{str(report['confirmed_accurate_forecast']).lower()}`",
        f"- Fresh untouched holdout available: `{str(report['fresh_untouched_holdout_available']).lower()}`",
        f"- Primary target: `{report['primary_target']}`",
        f"- Dataset rows: {report['data_audit']['row_count']:,}",
        f"- Safe source features: {report['data_audit']['safe_feature_count']}",
        f"- Causal interactions: {report['data_audit']['causal_interaction_count']}",
        f"- Pair identity features: {report['data_audit']['pair_identity_feature_count']}",
        "",
    ]
    for target_id, target in report["targets"].items():
        selected = target["selected_development_candidate"]
        diagnostic = target["diagnostic_evaluation"]
        lines.extend(
            [
                f"## {target_id}",
                "",
                f"Selected on 2025 purged folds: `{selected['candidate_id']}`.",
                "",
                "| Candidate | Mean AUC | Min AUC | Balanced accuracy | Brier skill | Pair breadth | Gate |",
                "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for row in target["leaderboard"]:
            lines.append(
                "| `{candidate_id}` | {mean_auc:.4f} | {minimum_fold_auc:.4f} | "
                "{mean_balanced_accuracy:.4f} | {mean_brier_skill:.4f} | {pair_auc_breadth:.1%} | `{gate}` |".format(
                    candidate_id=row["candidate_id"],
                    mean_auc=float(row["mean_auc"] or 0.0),
                    minimum_fold_auc=float(row["minimum_fold_auc"] or 0.0),
                    mean_balanced_accuracy=float(row["mean_balanced_accuracy"] or 0.0),
                    mean_brier_skill=float(row["mean_brier_skill"] or 0.0),
                    pair_auc_breadth=float(row["pair_auc_breadth"] or 0.0),
                    gate=str(row["development_gate_passed"]).lower(),
                )
            )
        metrics = diagnostic["metrics"]
        lines.extend(
            [
                "",
                "### Previously Inspected 2026 Diagnostic",
                "",
                f"- AUC: `{metrics.get('roc_auc')}`",
                f"- Balanced accuracy: `{metrics.get('balanced_accuracy')}`",
                f"- Brier skill: `{metrics.get('brier_skill_score')}`",
                f"- Expected calibration error: `{metrics.get('expected_calibration_error')}`",
                "- This period is diagnostic evidence, not a fresh holdout and not confirmation.",
                "",
            ]
        )
    two_stage = report.get("two_stage_forecast")
    if two_stage:
        selected = two_stage["selected_development_policy"]
        development = selected["aggregate_metrics"]
        diagnostic = two_stage["diagnostic_2026"]["aggregate_metrics"]
        lines.extend(
            [
                "## Two-Stage Direction Filter",
                "",
                f"- Opportunity threshold: `{selected['opportunity_threshold']}`",
                f"- Direction confidence: `{selected['direction_confidence']}`",
                f"- 2025 OOS coverage: `{development['coverage']:.2%}`",
                f"- 2025 OOS accuracy: `{development['accuracy']:.4f}`",
                f"- 2025 OOS balanced accuracy: `{development['balanced_accuracy']:.4f}`",
                f"- Minimum fold balanced accuracy: `{selected['minimum_fold_balanced_accuracy']:.4f}`",
                f"- Directional development gate: `{str(two_stage['development_gate']['passed']).lower()}`",
                f"- 2026 diagnostic coverage: `{diagnostic['coverage']:.2%}`",
                f"- 2026 diagnostic accuracy: `{diagnostic['accuracy']:.4f}`",
                f"- 2026 diagnostic balanced accuracy: `{diagnostic['balanced_accuracy']:.4f}`",
                "- The 2026 period was not used for threshold selection, but it was previously inspected and is not a fresh holdout.",
                "",
            ]
        )
    lines.extend(
        [
            "## Interpretation",
            "",
            report["interpretation"],
            "",
            "The serialized target bundles are offline research artifacts. They are not imported by any OANDA, MT5, practice, demo, or live process.",
        ]
    )
    return "\n".join(lines) + "\n"


def run_feature_forecast_benchmark(
    *,
    output_dir: Path,
    source_path: Path = SOURCE_DATASET,
    labels_path: Path = DEFAULT_LABELS_PATH,
    pairs: Sequence[str] | None = None,
    max_rows_per_pair: int | None = None,
    targets: Sequence[str] = (PRIMARY_TARGET, OPPORTUNITY_TARGET),
    config: BenchmarkConfig = BenchmarkConfig(),
    quick: bool = False,
) -> dict[str, Any]:
    assert_offline_config()
    unknown_targets = sorted(set(targets) - set(TARGET_DEFINITIONS))
    if unknown_targets:
        raise ValueError(f"Unknown targets: {unknown_targets}")
    output_dir.mkdir(parents=True, exist_ok=False)
    dataset, feature_families, data_audit = load_forecast_dataset(
        source_path=source_path,
        labels_path=labels_path,
        pairs=pairs,
        max_rows_per_pair=max_rows_per_pair,
    )
    specs = candidate_specs(quick=quick)
    specs_by_id = {spec.model_id: spec for spec in specs}
    ensembles = ensemble_definitions(quick=quick)
    target_reports: dict[str, Any] = {}
    target_bundles: dict[str, dict[str, Any]] = {}
    development_predictions: dict[str, pd.DataFrame] = {}
    diagnostic_prediction_frames: dict[str, pd.DataFrame] = {}
    for target_id in targets:
        model_reports: list[dict[str, Any]] = []
        prediction_by_id: dict[str, pd.DataFrame] = {}
        for spec in specs:
            candidate_report, predictions = evaluate_candidate(
                dataset,
                feature_families,
                spec,
                target_id=target_id,
                config=config,
            )
            model_reports.append(candidate_report)
            prediction_by_id[spec.model_id] = predictions
        for ensemble_id, members in ensembles.items():
            predictions = build_ensemble_predictions(
                prediction_by_id,
                ensemble_id=ensemble_id,
                members=members,
            )
            prediction_by_id[ensemble_id] = predictions
            model_reports.append(
                _reports_from_predictions(
                    predictions,
                    candidate_id=ensemble_id,
                    model_family="probability_average_ensemble",
                    feature_family="component_defined",
                    target_id=target_id,
                    ensemble_members=members,
                )
            )
        ranked = rank_reports(model_reports)
        selected = ranked[0]
        diagnostic, diagnostic_predictions, _ = frozen_diagnostic_evaluation(
            dataset,
            target_id=target_id,
            selected_report=selected,
            specs_by_id=specs_by_id,
            ensembles=ensembles,
            feature_families=feature_families,
            config=config,
        )
        selected_predictions = prediction_by_id[str(selected["candidate_id"])]
        selected_predictions.to_parquet(
            output_dir / f"{target_id}_development_oos_predictions.parquet",
            index=False,
        )
        diagnostic_predictions.to_parquet(
            output_dir / f"{target_id}_2026_diagnostic_predictions.parquet",
            index=False,
        )
        development_predictions[target_id] = selected_predictions
        diagnostic_prediction_frames[target_id] = diagnostic_predictions
        baseline_reports = _baseline_reports(dataset, target_id, config)
        target_reports[target_id] = {
            "target_definition": TARGET_DEFINITIONS[target_id],
            "candidate_grid_declared_before_evaluation": True,
            "candidate_count": len(ranked),
            "selected_development_candidate": selected,
            "leaderboard": [_candidate_summary(row) for row in ranked],
            "candidate_reports": ranked,
            "baselines": [_candidate_summary(row) for row in baseline_reports],
            "baseline_reports": baseline_reports,
            "diagnostic_evaluation": diagnostic,
            "confirmed_accurate_forecast": False,
        }
        target_bundles[target_id] = _future_research_bundle(
            dataset,
            target_id=target_id,
            selected_report=selected,
            specs_by_id=specs_by_id,
            ensembles=ensembles,
            feature_families=feature_families,
            config=config,
        )
    if PRIMARY_TARGET not in target_bundles:
        raise ValueError(f"Primary target {PRIMARY_TARGET} was not evaluated")
    bundle_paths: dict[str, Path] = {}
    bundle_validation: dict[str, dict[str, Any]] = {}
    for target_id, bundle in target_bundles.items():
        bundle_path = output_dir / f"{target_id}_offline_research_forecast_bundle.joblib"
        joblib.dump(bundle, bundle_path, compress=3)
        restored = joblib.load(bundle_path)
        if restored.get("target_id") != target_id:
            raise ValueError(f"Bundle round-trip target mismatch for {target_id}")
        if restored.get("candidate_id") != bundle.get("candidate_id"):
            raise ValueError(f"Bundle round-trip candidate mismatch for {target_id}")
        bundle_paths[target_id] = bundle_path
        bundle_validation[target_id] = {
            "joblib_round_trip_passed": True,
            "component_count": len(restored.get("components") or []),
            "sha256": sha256_file(bundle_path),
        }

    two_stage_report: dict[str, Any] | None = None
    if {PRIMARY_TARGET, OPPORTUNITY_TARGET}.issubset(development_predictions):
        two_stage_report, two_stage_development, two_stage_diagnostic = (
            evaluate_two_stage_forecast(
                development_predictions[PRIMARY_TARGET],
                development_predictions[OPPORTUNITY_TARGET],
                diagnostic_prediction_frames[PRIMARY_TARGET],
                diagnostic_prediction_frames[OPPORTUNITY_TARGET],
            )
        )
        two_stage_development.to_parquet(
            output_dir / "two_stage_direction_development_oos_predictions.parquet",
            index=False,
        )
        two_stage_diagnostic.to_parquet(
            output_dir / "two_stage_direction_2026_diagnostic_predictions.parquet",
            index=False,
        )
        atomic_write_json(output_dir / "TWO_STAGE_FORECAST_REPORT.json", two_stage_report)

    bundle_index = {
        **safety_fields(),
        "bundle_version": REPORT_VERSION,
        "artifact_role": "offline_research_forecast_bundle_index",
        "target_bundles": {
            target_id: {
                "path": path.name,
                "sha256": bundle_validation[target_id]["sha256"],
                "joblib_round_trip_passed": bundle_validation[target_id][
                    "joblib_round_trip_passed"
                ],
                "candidate_id": target_bundles[target_id]["candidate_id"],
                "target_definition": target_bundles[target_id]["target_definition"],
            }
            for target_id, path in sorted(bundle_paths.items())
        },
        "two_stage_policy": (
            {
                "opportunity_threshold": two_stage_report["selected_development_policy"][
                    "opportunity_threshold"
                ],
                "direction_confidence": two_stage_report["selected_development_policy"][
                    "direction_confidence"
                ],
                "development_gate_passed": two_stage_report["development_gate"]["passed"],
                "confirmed_accurate_directional_forecast": False,
            }
            if two_stage_report is not None
            else None
        ),
        "fresh_holdout_required": True,
        "production_ready": False,
    }
    atomic_write_json(output_dir / "offline_research_forecast_bundle_index.json", bundle_index)
    generated_at = datetime.now(timezone.utc).isoformat()
    report = {
        **safety_fields(),
        "report_version": REPORT_VERSION,
        "generated_at_utc": generated_at,
        "objective": "objective_leakage_safe_forex_feature_forecast_comparison",
        "primary_target": PRIMARY_TARGET,
        "config": asdict(config),
        "quick_mode": bool(quick),
        "data_audit": data_audit,
        "data_snapshot": {
            "source_path": str(source_path),
            "source_size_bytes": source_path.stat().st_size,
            "source_sha256": sha256_file(source_path),
            "labels_path": str(labels_path),
            "labels_size_bytes": labels_path.stat().st_size,
            "labels_sha256": sha256_file(labels_path),
            "feature_family_sha256": stable_fingerprint(feature_families),
        },
        "development_protocol": {
            "folds": [asdict(fold) for fold in development_folds()],
            "expanding_windows": True,
            "purge_rule": "training label_end_time is strictly before fold start minus embargo",
            "embargo_minutes": config.embargo_minutes,
            "selection_period": "2025 only",
            "evaluation_period": "2026 previously inspected diagnostic only",
            "candidate_grid_declared_before_evaluation": True,
            "evaluation_does_not_change_selection": True,
        },
        "targets": target_reports,
        "two_stage_forecast": two_stage_report,
        "confirmed_accurate_forecast": False,
        "fresh_untouched_holdout_available": False,
        "profitability_claimed": False,
        "interpretation": (
            "A development gate may identify repeatable forecast structure, but no model can be "
            "called confirmed accurate because all available data through 2026-07-06 has already "
            "been inspected. Confirmation requires a frozen model and genuinely later data."
        ),
        "next_required_evidence": (
            "Freeze this exact primary bundle and evaluate once on a materially sized period "
            "strictly after 2026-07-06 without changing features, target, model, or threshold."
        ),
        "artifact_paths": {
            "model_bundles": {
                target_id: str(path) for target_id, path in sorted(bundle_paths.items())
            },
            "model_bundle_index": str(
                output_dir / "offline_research_forecast_bundle_index.json"
            ),
            "two_stage_report": (
                str(output_dir / "TWO_STAGE_FORECAST_REPORT.json")
                if two_stage_report is not None
                else None
            ),
            "report_json": str(output_dir / "FEATURE_FORECAST_REPORT.json"),
            "report_markdown": str(output_dir / "FEATURE_FORECAST_REPORT.md"),
        },
    }
    atomic_write_json(output_dir / "FEATURE_FORECAST_REPORT.json", report)
    (output_dir / "FEATURE_FORECAST_REPORT.md").write_text(
        _render_markdown(report), encoding="utf-8"
    )
    manifest = {
        **safety_fields(),
        "run_id": output_dir.name,
        "generated_at_utc": generated_at,
        "report_version": REPORT_VERSION,
        "candidate_specs": [asdict(spec) for spec in specs],
        "ensemble_definitions": ensembles,
        "targets": list(targets),
        "output_files": sorted(path.name for path in output_dir.iterdir() if path.is_file()),
        "confirmed_accurate_forecast": False,
    }
    atomic_write_json(output_dir / "run_manifest.json", manifest)
    return report


def _csv_list(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the offline leakage-safe Forex feature forecast benchmark."
    )
    parser.add_argument("--source", type=Path, default=SOURCE_DATASET)
    parser.add_argument("--labels", type=Path, default=DEFAULT_LABELS_PATH)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--pairs", default="")
    parser.add_argument("--targets", default=f"{PRIMARY_TARGET},{OPPORTUNITY_TARGET}")
    parser.add_argument("--max-rows-per-pair", type=int)
    parser.add_argument("--max-train-rows", type=int, default=100_000)
    parser.add_argument("--n-jobs", type=int, default=2)
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args()
    output_dir = args.output_dir or (
        ENGINE_ROOT / "reports" / f"feature_forecast_benchmark_{utc_now_stamp()}"
    )
    report = run_feature_forecast_benchmark(
        output_dir=output_dir,
        source_path=args.source,
        labels_path=args.labels,
        pairs=_csv_list(args.pairs) or None,
        max_rows_per_pair=args.max_rows_per_pair,
        targets=_csv_list(args.targets),
        config=BenchmarkConfig(
            max_train_rows=args.max_train_rows,
            n_jobs=args.n_jobs,
        ),
        quick=args.quick,
    )
    primary = report["targets"][PRIMARY_TARGET]
    selected = primary["selected_development_candidate"]
    print(
        json.dumps(
            {
                "output_dir": str(output_dir),
                "selected_candidate": selected["candidate_id"],
                "development_gate_passed": selected["development_gate"]["passed"],
                "mean_auc": selected["aggregate_metrics"]["mean_roc_auc"],
                "diagnostic_auc": primary["diagnostic_evaluation"]["metrics"]["roc_auc"],
                "confirmed_accurate_forecast": False,
                **safety_fields(),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
