"""Leakage-audited causal movement gates for the two-hour OCO study.

The existing all-68 feature cache is useful, but it also contains forward
labels.  This module reads an explicit causal allow-list only.  Its source
five-minute bars are left-labelled, so their timestamps are shifted to the
first decision time at which the completed bar is knowable.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import numpy as np
import pandas as pd


DEFAULT_VAULT_ROOT = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "all68_weekly_move_study"
    / "features"
)

VAULT_CAUSAL_FEATURES = (
    "momentum_5_atr",
    "momentum_15_atr",
    "momentum_30_atr",
    "momentum_60_atr",
    "acceleration_15_atr",
    "sma7_minus_8_atr",
    "sma30_slope_5_atr",
    "rsi14_centered",
    "atr15_to_atr240",
    "compression_30",
    "range_position_60_centered",
    "range_position_240_centered",
    "spread_ratio_60",
    "volume_z_30",
    "strength_gap_15",
    "strength_gap_60",
    "strength_gap_rank_15",
    "strength_gap_rank_60",
)

_SOURCE_CONTEXT = ("atr240_pips", "spread_pips")
_FORBIDDEN_TOKENS = (
    "future",
    "forward",
    "label",
    "target",
    "oracle",
    "outcome",
    "mfe",
    "mae",
    "realized_pnl",
)


def reject_hindsight_feature_names(columns: Iterable[str]) -> None:
    rejected = [
        str(column)
        for column in columns
        if any(token in str(column).lower() for token in _FORBIDDEN_TOKENS)
    ]
    if rejected:
        raise ValueError(f"hindsight-derived feature names rejected: {sorted(rejected)}")


def audit_vault_columns(
    columns: Iterable[str],
    *,
    allowlist: Sequence[str] = VAULT_CAUSAL_FEATURES,
) -> pd.DataFrame:
    """Return an explicit include/exclude ledger for a vault schema."""

    allowed = set(map(str, allowlist))
    rows: list[dict[str, Any]] = []
    for raw in columns:
        column = str(raw)
        lowered = column.lower()
        if column in allowed:
            status, reason = "included", "explicit_causal_allowlist"
        elif any(token in lowered for token in _FORBIDDEN_TOKENS):
            status, reason = "excluded", "hindsight_or_label_name"
        elif column in {*_SOURCE_CONTEXT, "time_utc", "instrument"}:
            status, reason = "context", "execution_or_provenance_context"
        else:
            status, reason = "excluded", "not_explicitly_causality_approved"
        rows.append({"column": column, "status": status, "reason": reason})
    return pd.DataFrame(rows).sort_values(["status", "column"], kind="stable")


def _utc(value: Any | None) -> pd.Timestamp | None:
    if value is None or value == "":
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _instrument_files(
    root: Path, instruments: Sequence[str] | None
) -> list[tuple[str, Path]]:
    available = {path.stem.upper(): path for path in root.glob("*.parquet")}
    if not available:
        raise FileNotFoundError(f"no vault feature Parquets found under {root}")
    if instruments is None:
        names = sorted(available)
    else:
        names = sorted({str(value).upper().replace("/", "_") for value in instruments})
        missing = sorted(set(names).difference(available))
        if missing:
            raise FileNotFoundError(f"vault features missing for instruments: {missing}")
    return [(name, available[name]) for name in names]


def _read_one_vault_file(
    instrument: str,
    path: Path,
    *,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
    decision_offset_minutes: int,
    decision_stride_minutes: int,
    expected_horizon_minutes: int,
    assumed_round_trip_slippage_pips: float,
) -> pd.DataFrame:
    import pyarrow.parquet as pq

    schema = pq.ParquetFile(path).schema.names
    audit = audit_vault_columns(schema)
    included = audit.loc[audit["status"].eq("included"), "column"].tolist()
    missing = sorted(set(VAULT_CAUSAL_FEATURES).difference(included))
    if missing:
        raise ValueError(f"{path} is missing approved causal features: {missing}")
    wanted = list(VAULT_CAUSAL_FEATURES) + list(_SOURCE_CONTEXT)
    frame = pd.read_parquet(path, columns=wanted).reset_index()
    if "time_utc" not in frame:
        raise ValueError(f"{path} has no time_utc index/column")
    source_time = pd.to_datetime(frame.pop("time_utc"), utc=True, errors="raise")
    # The legacy vault resampler used left labels.  At least one full bar must
    # elapse before any feature using that bar's close/high/low is tradable.
    frame.insert(
        0,
        "timestamp",
        source_time + pd.Timedelta(minutes=int(decision_offset_minutes)),
    )
    frame.insert(1, "instrument", instrument)
    if start is not None:
        frame = frame.loc[frame["timestamp"] >= start]
    if end is not None:
        frame = frame.loc[frame["timestamp"] <= end]
    if int(decision_stride_minutes) <= 0 or int(decision_stride_minutes) % 5:
        raise ValueError("decision_stride_minutes must be a positive multiple of five")
    stride = f"{int(decision_stride_minutes)}min"
    aligned = frame["timestamp"].dt.floor(stride).eq(frame["timestamp"])
    frame = frame.loc[aligned].copy()

    for column in wanted:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    reject_hindsight_feature_names(VAULT_CAUSAL_FEATURES)
    impulse = frame[
        ["momentum_5_atr", "momentum_15_atr", "momentum_30_atr", "momentum_60_atr"]
    ].abs().max(axis=1)
    expansion = (frame["atr15_to_atr240"] - 1.0).clip(lower=0.0)
    acceleration = frame["acceleration_15_atr"].abs()
    strength = frame[["strength_gap_rank_15", "strength_gap_rank_60"]].abs().max(axis=1)
    # A deterministic magnitude/opportunity score.  It intentionally contains
    # no direction and is later thresholded using train rows only.
    frame["movement_score_rule"] = (
        impulse + 0.50 * expansion + 0.25 * acceleration + 0.15 * strength
    )
    base_minutes = 5.0
    frame["atr_pips"] = frame["atr240_pips"] * np.sqrt(
        float(expected_horizon_minutes) / base_minutes
    )
    frame["expected_net_edge_pips"] = (
        frame["atr_pips"]
        - 2.0 * frame["spread_pips"].clip(lower=0.0)
        - float(assumed_round_trip_slippage_pips)
    ).clip(lower=0.0)
    required = list(VAULT_CAUSAL_FEATURES) + [
        "movement_score_rule",
        "atr_pips",
        "spread_pips",
        "expected_net_edge_pips",
    ]
    frame["feature_ready"] = frame[required].replace(
        [np.inf, -np.inf], np.nan
    ).notna().all(axis=1)
    frame["vault_source_timestamp"] = source_time.loc[frame.index]
    frame["vault_decision_offset_minutes"] = int(decision_offset_minutes)
    frame["features_are_causal"] = True
    frame["feature_schema"] = "all68_weekly_move_study_causal_allowlist_v1"
    return frame.reset_index(drop=True)


def iter_vault_features(
    feature_root: str | Path = DEFAULT_VAULT_ROOT,
    *,
    instruments: Sequence[str] | None = None,
    start: Any | None = None,
    end: Any | None = None,
    decision_offset_minutes: int = 5,
    decision_stride_minutes: int = 15,
    expected_horizon_minutes: int = 120,
    assumed_round_trip_slippage_pips: float = 0.4,
) -> Iterator[tuple[str, pd.DataFrame]]:
    root = Path(feature_root)
    start_utc, end_utc = _utc(start), _utc(end)
    if start_utc is not None and end_utc is not None and end_utc < start_utc:
        raise ValueError("end precedes start")
    for instrument, path in _instrument_files(root, instruments):
        yield instrument, _read_one_vault_file(
            instrument,
            path,
            start=start_utc,
            end=end_utc,
            decision_offset_minutes=int(decision_offset_minutes),
            decision_stride_minutes=int(decision_stride_minutes),
            expected_horizon_minutes=int(expected_horizon_minutes),
            assumed_round_trip_slippage_pips=float(assumed_round_trip_slippage_pips),
        )


def load_vault_features(feature_root: str | Path = DEFAULT_VAULT_ROOT, **kwargs: Any) -> pd.DataFrame:
    frames = [frame for _, frame in iter_vault_features(feature_root, **kwargs)]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values(
        ["timestamp", "instrument"], kind="stable", ignore_index=True
    )


def _fingerprint(frame: pd.DataFrame, id_col: str) -> str:
    values = frame[id_col].astype(str).sort_values(kind="stable").tolist()
    return hashlib.sha256("|".join(values).encode("utf-8")).hexdigest()


@dataclass
class MovementGateModel:
    estimator: Any
    feature_columns: tuple[str, ...]
    fill_values: dict[str, float]
    train_fingerprint: str
    train_rows: int
    positive_rows: int
    model_kind: str = "hist_gradient_boosting_classifier"
    fit_scope: str = "explicit_train_rows_only"


def fit_movement_classifier(
    train: pd.DataFrame,
    *,
    label_col: str = "is_significant",
    id_col: str = "decision_id",
    feature_columns: Sequence[str] = VAULT_CAUSAL_FEATURES,
    maximum_train_rows: int = 500_000,
    random_state: int = 20260713,
    max_iter: int = 120,
) -> MovementGateModel:
    """Fit a magnitude classifier on one explicitly supplied train slice."""

    from sklearn.ensemble import HistGradientBoostingClassifier

    features = tuple(map(str, feature_columns))
    reject_hindsight_feature_names(features)
    missing = sorted({label_col, id_col, *features}.difference(train.columns))
    if missing:
        raise ValueError(f"training frame is missing gate columns: {missing}")
    usable = train.loc[train[label_col].notna()].copy()
    y = usable[label_col].astype(bool)
    if y.nunique() < 2:
        raise ValueError("movement classifier needs both positive and negative train labels")
    if len(usable) > int(maximum_train_rows):
        positives = usable.loc[y]
        negative_budget = max(0, int(maximum_train_rows) - len(positives))
        negatives = usable.loc[~y]
        if len(positives) >= int(maximum_train_rows):
            positives = positives.sample(
                n=int(maximum_train_rows), random_state=int(random_state)
            )
            usable = positives
        else:
            usable = pd.concat(
                [
                    positives,
                    negatives.sample(
                        n=min(negative_budget, len(negatives)),
                        random_state=int(random_state),
                    ),
                ],
                ignore_index=True,
            )
        y = usable[label_col].astype(bool)
    numeric = usable.loc[:, features].apply(pd.to_numeric, errors="coerce")
    fill = numeric.median(axis=0).fillna(0.0)
    x = numeric.fillna(fill).replace([np.inf, -np.inf], 0.0)
    positive_count = int(y.sum())
    negative_count = int((~y).sum())
    weights = np.where(
        y.to_numpy(),
        len(y) / (2.0 * max(positive_count, 1)),
        len(y) / (2.0 * max(negative_count, 1)),
    )
    estimator = HistGradientBoostingClassifier(
        learning_rate=0.06,
        max_iter=int(max_iter),
        max_leaf_nodes=31,
        l2_regularization=1.0,
        early_stopping=True,
        random_state=int(random_state),
    )
    estimator.fit(x, y.to_numpy(dtype=np.int8), sample_weight=weights)
    return MovementGateModel(
        estimator=estimator,
        feature_columns=features,
        fill_values={column: float(fill[column]) for column in features},
        train_fingerprint=_fingerprint(usable, id_col),
        train_rows=int(len(usable)),
        positive_rows=positive_count,
    )


def score_movement_classifier(frame: pd.DataFrame, model: MovementGateModel) -> pd.Series:
    reject_hindsight_feature_names(model.feature_columns)
    missing = sorted(set(model.feature_columns).difference(frame.columns))
    if missing:
        raise ValueError(f"score frame is missing gate features: {missing}")
    x = frame.loc[:, model.feature_columns].apply(pd.to_numeric, errors="coerce")
    x = x.fillna(pd.Series(model.fill_values)).replace([np.inf, -np.inf], 0.0)
    return pd.Series(
        model.estimator.predict_proba(x)[:, 1], index=frame.index, name="movement_score"
    )


def apply_cross_sectional_top_n(
    frame: pd.DataFrame,
    *,
    score_col: str = "movement_score",
    threshold_col: str = "movement_threshold",
    timestamp_col: str = "timestamp",
    top_n: int = 3,
) -> pd.DataFrame:
    """Apply a frozen threshold, then causally rank pairs at each timestamp."""

    if int(top_n) < 1:
        raise ValueError("top_n must be positive")
    required = {score_col, threshold_col, timestamp_col, "instrument"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"gate application frame is missing columns: {missing}")
    output = frame.copy()
    output["cross_sectional_rank"] = output.groupby(
        timestamp_col, sort=False, observed=True
    )[score_col].rank(method="first", ascending=False)
    output["movement_gate_passed"] = (
        pd.to_numeric(output[score_col], errors="coerce")
        >= pd.to_numeric(output[threshold_col], errors="coerce")
    ) & output["cross_sectional_rank"].le(int(top_n))
    output["cross_sectional_top_n"] = int(top_n)
    return output
