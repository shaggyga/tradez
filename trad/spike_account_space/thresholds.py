"""Auditable train-only empirical threshold fitting."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class TrainOnlyThresholdModel:
    """Frozen threshold metadata fitted from one explicitly supplied train set."""

    value_col: str
    group_col: str
    quantile: float
    minimum_group_samples: int
    fixed_floor: float
    global_threshold: float
    group_thresholds: dict[str, float]
    group_sample_counts: dict[str, int]
    train_row_count: int
    train_min_timestamp: str
    train_max_timestamp: str
    train_fingerprint: str
    fit_scope: str = "train_only"

    def threshold_for(self, group: Any) -> tuple[float, str]:
        key = str(group)
        if key in self.group_thresholds:
            return self.group_thresholds[key], "instrument_train_quantile"
        return self.global_threshold, "global_train_fallback"


def _fingerprint_train_rows(frame: pd.DataFrame) -> str:
    if "decision_id" in frame:
        values = sorted(frame["decision_id"].dropna().astype(str).tolist())
    else:
        values = [str(value) for value in frame.index.tolist()]
    return hashlib.sha256("|".join(values).encode("utf-8")).hexdigest()


def fit_train_only_thresholds(
    train: pd.DataFrame,
    *,
    value_col: str = "forward_abs_pips",
    group_col: str = "instrument",
    quantile: float = 0.995,
    minimum_group_samples: int = 100,
    fixed_floor: float = 0.0,
    valid_col: str | None = "label_is_valid",
    timestamp_col: str = "timestamp",
) -> TrainOnlyThresholdModel:
    """Fit global and instrument thresholds using only the supplied train rows.

    Groups below ``minimum_group_samples`` fall back to the global train
    quantile.  Validation/test rows are never accepted by this function and
    therefore cannot affect any fitted value.
    """

    required = {value_col, group_col}
    missing = sorted(required.difference(train.columns))
    if missing:
        raise ValueError(f"training frame missing threshold columns: {missing}")
    if not 0.0 < float(quantile) < 1.0:
        raise ValueError("quantile must be strictly between zero and one")
    if int(minimum_group_samples) <= 0:
        raise ValueError("minimum_group_samples must be positive")
    floor = float(fixed_floor)
    if not np.isfinite(floor) or floor < 0:
        raise ValueError("fixed_floor must be a finite non-negative number")

    usable = train.copy()
    if valid_col is not None and valid_col in usable:
        usable = usable.loc[usable[valid_col].fillna(False).astype(bool)]
    usable["_threshold_value"] = pd.to_numeric(usable[value_col], errors="coerce")
    usable = usable.loc[
        np.isfinite(usable["_threshold_value"]) & (usable["_threshold_value"] >= 0)
    ]
    if usable.empty:
        raise ValueError("training frame has no finite, valid threshold values")

    global_threshold = max(
        floor, float(usable["_threshold_value"].quantile(float(quantile)))
    )
    thresholds: dict[str, float] = {}
    counts: dict[str, int] = {}
    for group, part in usable.groupby(group_col, sort=True, observed=True):
        key = str(group)
        counts[key] = int(len(part))
        if len(part) >= int(minimum_group_samples):
            thresholds[key] = max(
                floor, float(part["_threshold_value"].quantile(float(quantile)))
            )

    minimum_timestamp = ""
    maximum_timestamp = ""
    if timestamp_col in usable:
        timestamp = pd.to_datetime(usable[timestamp_col], errors="coerce", utc=True)
        if timestamp.notna().any():
            minimum_timestamp = timestamp.min().isoformat()
            maximum_timestamp = timestamp.max().isoformat()
    return TrainOnlyThresholdModel(
        value_col=value_col,
        group_col=group_col,
        quantile=float(quantile),
        minimum_group_samples=int(minimum_group_samples),
        fixed_floor=floor,
        global_threshold=global_threshold,
        group_thresholds=thresholds,
        group_sample_counts=counts,
        train_row_count=int(len(usable)),
        train_min_timestamp=minimum_timestamp,
        train_max_timestamp=maximum_timestamp,
        train_fingerprint=_fingerprint_train_rows(usable),
    )


def apply_train_only_thresholds(
    frame: pd.DataFrame,
    model: TrainOnlyThresholdModel,
    *,
    threshold_col: str = "significance_threshold",
    output_col: str = "is_significant",
) -> pd.DataFrame:
    """Apply a previously frozen train-only model without refitting."""

    required = {model.value_col, model.group_col}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"application frame missing threshold columns: {missing}")
    output = frame.copy()
    groups = output[model.group_col].astype(str)
    mapped = groups.map(model.group_thresholds)
    output[threshold_col] = mapped.fillna(model.global_threshold).astype(float)
    output["threshold_source"] = np.where(
        mapped.notna(), "instrument_train_quantile", "global_train_fallback"
    )
    values = pd.to_numeric(output[model.value_col], errors="coerce")
    output[output_col] = values.notna() & (values >= output[threshold_col])
    if "label_is_valid" in output:
        output[output_col] &= output["label_is_valid"].fillna(False).astype(bool)
    output["threshold_fit_scope"] = model.fit_scope
    output["threshold_train_fingerprint"] = model.train_fingerprint
    return output
