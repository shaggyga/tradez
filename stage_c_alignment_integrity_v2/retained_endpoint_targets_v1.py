"""Endpoint-only historical targets, deliberately separate from path labels."""
from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np


def endpoint_outcomes(data: Mapping[str, Sequence[float]], horizon_minutes: int) -> dict[str, np.ndarray]:
    """Return exact-endpoint labels without claiming uninterrupted path support.

    `endpoint_available` establishes only that origin and target closes exist.
    It is suitable for midpoint movement research, never for a fill/path claim.
    """
    if horizon_minutes <= 0:
        raise ValueError("positive_horizon_required")
    time = np.asarray(data["time"], dtype=np.int64)
    close = np.asarray(data["close"], dtype=float)
    bid = np.asarray(data["bid_close"], dtype=float)
    ask = np.asarray(data["ask_close"], dtype=float)
    if not (len(time) == len(close) == len(bid) == len(ask)) or np.any(np.diff(time) <= 0):
        raise ValueError("sorted_equal_length_arrays_required")
    target = time + horizon_minutes * 60
    lookup = {int(value): index for index, value in enumerate(time)}
    index = np.array([lookup.get(int(value), -1) for value in target])
    present = index >= 0
    safe = np.maximum(index, 0)
    valid = present & np.isfinite(close) & (close > 0) & np.isfinite(close[safe]) & (close[safe] > 0)
    quote_valid = valid & np.isfinite(bid) & np.isfinite(ask) & np.isfinite(bid[safe]) & np.isfinite(ask[safe]) & (ask >= bid) & (ask[safe] >= bid[safe])
    state = np.where(valid, "endpoint_available", "missing_target").astype(object)
    with np.errstate(divide="ignore", invalid="ignore"):
        midpoint_return_bps = (close[safe] / close - 1.0) * 10000.0
        long_endpoint_net_bps = (bid[safe] - ask) / close * 10000.0
        short_endpoint_net_bps = (bid - ask[safe]) / close * 10000.0
    midpoint_return_bps[~valid] = np.nan
    long_endpoint_net_bps[~quote_valid] = np.nan
    short_endpoint_net_bps[~quote_valid] = np.nan
    return {
        "target_bar_start_epoch": target,
        "assumed_available_epoch": target + 60,
        "state": state,
        "midpoint_return_bps": midpoint_return_bps,
        "long_endpoint_net_bps": long_endpoint_net_bps,
        "short_endpoint_net_bps": short_endpoint_net_bps,
        "endpoint_quote_valid": quote_valid,
    }
