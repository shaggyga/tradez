#!/usr/bin/env python3
"""Leakage-safe cross-pair graph construction for the StemGNN shadow lane."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class GraphEdge:
    source: str
    target: str
    weight: float
    relationship: str
    observations: int


@dataclass(frozen=True)
class LaggedGraphSnapshot:
    as_of_utc: str
    lookback_rows: int
    instruments: tuple[str, ...]
    edges: tuple[GraphEdge, ...]
    source_max_utc: str
    construction: str = "strictly_lagged_return_correlation_and_currency_exposure_v1"

    def to_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["edge_count"] = len(self.edges)
        return value


def split_instrument(instrument: str) -> tuple[str, str]:
    parts = instrument.upper().split("_")
    if len(parts) != 2 or any(len(part) != 3 for part in parts):
        raise ValueError(f"invalid OANDA instrument: {instrument}")
    return parts[0], parts[1]


def currency_exposure(instrument: str, currency: str) -> int:
    base, quote = split_instrument(instrument)
    if base == currency:
        return 1
    if quote == currency:
        return -1
    return 0


def _currency_relationship(left: str, right: str) -> tuple[str, float] | None:
    left_base, left_quote = split_instrument(left)
    right_base, right_quote = split_instrument(right)
    shared = sorted({left_base, left_quote} & {right_base, right_quote})
    if not shared:
        return None
    currency = shared[0]
    sign = float(currency_exposure(left, currency) * currency_exposure(right, currency))
    return f"shared_{currency}", sign


def build_lagged_graph(
    returns: pd.DataFrame,
    as_of: pd.Timestamp,
    instruments: Iterable[str] | None = None,
    lookback_rows: int = 256,
    min_observations: int = 24,
    min_abs_correlation: float = 0.15,
) -> LaggedGraphSnapshot:
    required = {"timestamp_utc", "instrument", "return_pips"}
    missing = sorted(required - set(returns.columns))
    if missing:
        raise ValueError(f"returns are missing: {', '.join(missing)}")
    if lookback_rows < min_observations:
        raise ValueError("lookback_rows must be at least min_observations")
    cutoff = pd.Timestamp(as_of)
    if cutoff.tzinfo is None:
        cutoff = cutoff.tz_localize("UTC")
    else:
        cutoff = cutoff.tz_convert("UTC")
    frame = returns.copy()
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True)
    frame = frame[frame["timestamp_utc"] < cutoff]
    if instruments is None:
        nodes = tuple(sorted(frame["instrument"].astype(str).unique()))
    else:
        nodes = tuple(dict.fromkeys(str(value) for value in instruments))
    if not nodes:
        raise ValueError("graph has no instruments")
    for instrument in nodes:
        split_instrument(instrument)
    frame = frame[frame["instrument"].isin(nodes)]
    frame = (
        frame.sort_values("timestamp_utc")
        .groupby("instrument", observed=True, group_keys=False)
        .tail(lookback_rows)
    )
    if frame.empty:
        raise ValueError("no strictly historical observations exist before as_of")
    if not (frame["timestamp_utc"] < cutoff).all():
        raise AssertionError("graph construction exposed a contemporaneous or future observation")
    pivot = frame.pivot_table(
        index="timestamp_utc",
        columns="instrument",
        values="return_pips",
        aggfunc="last",
    )
    edges: list[GraphEdge] = []
    for source_index, source in enumerate(nodes):
        source_values = pivot[source] if source in pivot else pd.Series(dtype=float)
        for target in nodes[source_index + 1 :]:
            target_values = pivot[target] if target in pivot else pd.Series(dtype=float)
            paired = pd.concat([source_values, target_values], axis=1).dropna()
            relationship = _currency_relationship(source, target)
            correlation = float(paired.iloc[:, 0].corr(paired.iloc[:, 1])) if len(paired) >= min_observations else math_nan()
            candidates: list[tuple[str, float]] = []
            if np.isfinite(correlation) and abs(correlation) >= min_abs_correlation:
                candidates.append(("lagged_return_correlation", correlation))
            if relationship is not None:
                label, exposure_sign = relationship
                candidates.append((label, exposure_sign))
            for label, weight in candidates:
                edges.append(GraphEdge(source, target, float(weight), label, len(paired)))
                edges.append(GraphEdge(target, source, float(weight), label, len(paired)))
    return LaggedGraphSnapshot(
        as_of_utc=cutoff.isoformat(),
        lookback_rows=lookback_rows,
        instruments=nodes,
        edges=tuple(edges),
        source_max_utc=frame["timestamp_utc"].max().isoformat(),
    )


def math_nan() -> float:
    return float("nan")


def adjacency_matrix(snapshot: LaggedGraphSnapshot) -> np.ndarray:
    positions = {instrument: index for index, instrument in enumerate(snapshot.instruments)}
    matrix = np.eye(len(positions), dtype=np.float32)
    counts = np.ones_like(matrix)
    for edge in snapshot.edges:
        row = positions[edge.source]
        column = positions[edge.target]
        matrix[row, column] += edge.weight
        counts[row, column] += 1.0
    matrix /= counts
    scale = np.max(np.abs(matrix), axis=1, keepdims=True)
    scale[scale == 0.0] = 1.0
    return matrix / scale


def make_stemgnn_model(
    instruments: Iterable[str],
    horizon: int,
    input_size: int,
    max_steps: int = 1,
):
    from oanda_neuralforecast_family_adapters import make_model

    nodes = tuple(instruments)
    if len(nodes) < 2:
        raise ValueError("cross-pair StemGNN requires at least two instruments")
    return make_model("stemgnn", horizon, input_size, max_steps, n_series=len(nodes))
