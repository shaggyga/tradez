"""Chronological nested walk-forward splits with purge, embargo, and event groups."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any

import numpy as np
import pandas as pd

from .dataset import _strict_utc_series


@dataclass(frozen=True)
class PurgedFold:
    """Positional row indices and audit metadata for one walk-forward fold."""

    name: str
    train_positions: np.ndarray
    test_positions: np.ndarray
    omitted_positions: np.ndarray
    test_start: pd.Timestamp
    test_end_exclusive: pd.Timestamp | None
    purge: pd.Timedelta
    embargo: pd.Timedelta

    def take_train(self, frame: pd.DataFrame) -> pd.DataFrame:
        return frame.iloc[self.train_positions]

    def take_test(self, frame: pd.DataFrame) -> pd.DataFrame:
        return frame.iloc[self.test_positions]


@dataclass(frozen=True)
class NestedPurgedFold:
    """One outer test fold and its inner tuning folds."""

    outer: PurgedFold
    inner: tuple[PurgedFold, ...]


def _effective_group_ids(
    frame: pd.DataFrame,
    *,
    group_col: str,
    decision_id_col: str,
) -> pd.Series:
    if decision_id_col in frame:
        fallback = "decision:" + frame[decision_id_col].astype(str)
    else:
        fallback = pd.Series(
            [f"row:{position}" for position in range(len(frame))], index=frame.index
        )
    if group_col not in frame:
        return fallback
    group = frame[group_col].astype("string")
    usable = group.notna() & group.str.strip().ne("")
    return group.where(usable, fallback).astype(str)


def _time_edges(
    timestamps: pd.Series,
    *,
    n_splits: int,
    initial_train_fraction: float,
) -> list[tuple[pd.Timestamp, pd.Timestamp | None]]:
    unique = pd.DatetimeIndex(timestamps.dropna().unique()).sort_values()
    if len(unique) < n_splits + 2:
        raise ValueError("not enough unique timestamps for requested chronological splits")
    first_test = max(1, int(math.ceil(len(unique) * initial_train_fraction)))
    if first_test >= len(unique):
        raise ValueError("initial_train_fraction leaves no timestamps for testing")
    remaining = len(unique) - first_test
    if remaining < n_splits:
        raise ValueError("too few post-training timestamps for requested splits")
    edges = np.linspace(first_test, len(unique), n_splits + 1, dtype=int)
    if len(np.unique(edges)) != len(edges):
        raise ValueError("chronological split boundaries collapsed; reduce n_splits")
    result: list[tuple[pd.Timestamp, pd.Timestamp | None]] = []
    for index in range(n_splits):
        start = pd.Timestamp(unique[edges[index]])
        end = pd.Timestamp(unique[edges[index + 1]]) if edges[index + 1] < len(unique) else None
        result.append((start, end))
    return result


def _make_folds(
    frame: pd.DataFrame,
    positions: np.ndarray,
    *,
    n_splits: int,
    initial_train_fraction: float,
    purge: pd.Timedelta,
    embargo: pd.Timedelta,
    time_col: str,
    label_end_col: str,
    group_col: str,
    decision_id_col: str,
    prefix: str,
) -> tuple[PurgedFold, ...]:
    subset = frame.iloc[positions].copy()
    subset["_position"] = positions
    subset["_time"] = frame[time_col].iloc[positions].to_numpy()
    subset["_label_end"] = frame[label_end_col].iloc[positions].to_numpy()
    subset["_group"] = frame["_effective_split_group"].iloc[positions].to_numpy()
    edges = _time_edges(
        subset["_time"], n_splits=n_splits, initial_train_fraction=initial_train_fraction
    )

    units = (
        subset.groupby("_group", sort=False, observed=True)
        .agg(
            min_time=("_time", "min"),
            max_time=("_time", "max"),
            min_label_end=("_label_end", "min"),
            max_label_end=("_label_end", "max"),
        )
        .reset_index()
    )
    positions_by_group = subset.groupby("_group", sort=False)["_position"].apply(
        lambda values: np.sort(values.to_numpy(dtype=np.int64))
    )

    result: list[PurgedFold] = []
    all_positions = set(int(value) for value in positions)
    for fold_index, (test_start, test_end) in enumerate(edges):
        if embargo == pd.Timedelta(0):
            train_time_ok = units["max_time"] < test_start
        else:
            train_time_ok = units["max_time"] <= test_start - embargo
        train_outcome_ok = units["max_label_end"] <= test_start - purge
        train_groups = set(units.loc[train_time_ok & train_outcome_ok, "_group"])

        test_ok = units["min_time"] >= test_start
        if test_end is not None:
            # Drop whole event groups that straddle a decision or outcome fold boundary.
            test_ok &= (units["max_time"] < test_end) & (units["max_label_end"] <= test_end)
        test_groups = set(units.loc[test_ok, "_group"])
        if train_groups.intersection(test_groups):
            raise RuntimeError("split group appeared in both train and test")

        train_positions = np.sort(
            np.concatenate([positions_by_group[group] for group in train_groups])
            if train_groups
            else np.array([], dtype=np.int64)
        )
        test_positions = np.sort(
            np.concatenate([positions_by_group[group] for group in test_groups])
            if test_groups
            else np.array([], dtype=np.int64)
        )
        if not len(train_positions) or not len(test_positions):
            raise ValueError(
                f"{prefix}_{fold_index} is empty after purge/embargo/group-boundary rules"
            )
        used = set(int(value) for value in np.concatenate([train_positions, test_positions]))
        omitted = np.array(sorted(all_positions.difference(used)), dtype=np.int64)
        result.append(
            PurgedFold(
                name=f"{prefix}_{fold_index}",
                train_positions=train_positions,
                test_positions=test_positions,
                omitted_positions=omitted,
                test_start=test_start,
                test_end_exclusive=test_end,
                purge=purge,
                embargo=embargo,
            )
        )
    return tuple(result)


def make_nested_purged_splits(
    frame: pd.DataFrame,
    *,
    n_outer_splits: int = 3,
    n_inner_splits: int = 3,
    outer_initial_train_fraction: float = 0.5,
    inner_initial_train_fraction: float = 0.5,
    purge: Any = "120min",
    embargo: Any = "120min",
    time_col: str = "timestamp",
    label_end_col: str = "outcome_timestamp",
    group_col: str = "event_cluster_id",
    decision_id_col: str = "decision_id",
) -> tuple[NestedPurgedFold, ...]:
    """Create nested expanding-window splits without label or event leakage.

    Purging constrains the latest *label endpoint* in training.  Embargo
    constrains the latest *decision timestamp*.  A non-empty event cluster is
    an indivisible unit; rows without a cluster use their decision ID as a
    singleton group.  Groups crossing a fold boundary are omitted wholesale.

    Returned arrays are positional and are suitable for ``frame.iloc[...]``.
    """

    required = {time_col, label_end_col}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"split frame missing required columns: {missing}")
    if int(n_outer_splits) <= 0 or int(n_inner_splits) <= 0:
        raise ValueError("outer and inner split counts must be positive")
    for name, value in [
        ("outer_initial_train_fraction", outer_initial_train_fraction),
        ("inner_initial_train_fraction", inner_initial_train_fraction),
    ]:
        if not 0.0 < float(value) < 1.0:
            raise ValueError(f"{name} must be strictly between zero and one")
    purge_delta = pd.Timedelta(purge)
    embargo_delta = pd.Timedelta(embargo)
    if purge_delta < pd.Timedelta(0) or embargo_delta < pd.Timedelta(0):
        raise ValueError("purge and embargo must be non-negative")
    if frame.empty:
        raise ValueError("cannot split an empty frame")

    prepared = frame.copy().reset_index(drop=True)
    prepared[time_col] = _strict_utc_series(prepared[time_col], name=time_col)
    prepared[label_end_col] = _strict_utc_series(
        prepared[label_end_col], name=label_end_col
    )
    if bool((prepared[label_end_col] <= prepared[time_col]).any()):
        raise ValueError("label endpoints must be later than decision timestamps")
    prepared["_effective_split_group"] = _effective_group_ids(
        prepared, group_col=group_col, decision_id_col=decision_id_col
    )
    all_positions = np.arange(len(prepared), dtype=np.int64)
    outer = _make_folds(
        prepared,
        all_positions,
        n_splits=int(n_outer_splits),
        initial_train_fraction=float(outer_initial_train_fraction),
        purge=purge_delta,
        embargo=embargo_delta,
        time_col=time_col,
        label_end_col=label_end_col,
        group_col=group_col,
        decision_id_col=decision_id_col,
        prefix="outer",
    )
    nested: list[NestedPurgedFold] = []
    for outer_fold in outer:
        inner = _make_folds(
            prepared,
            outer_fold.train_positions,
            n_splits=int(n_inner_splits),
            initial_train_fraction=float(inner_initial_train_fraction),
            purge=purge_delta,
            embargo=embargo_delta,
            time_col=time_col,
            label_end_col=label_end_col,
            group_col=group_col,
            decision_id_col=decision_id_col,
            prefix=f"{outer_fold.name}_inner",
        )
        nested.append(NestedPurgedFold(outer=outer_fold, inner=inner))
    return tuple(nested)
