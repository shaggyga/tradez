"""UTC daily-close and weekday trading-day endpoint target helpers.

These are endpoint-only midpoint diagnostic labels.  They intentionally do not
claim an executable path through intervening missing M1 bars.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np


def _next_close_after(decision_epoch: int) -> int:
    decision = datetime.fromtimestamp(int(decision_epoch), tz=timezone.utc)
    candidate = datetime(decision.year, decision.month, decision.day, tzinfo=timezone.utc) + timedelta(days=1)
    return int(candidate.timestamp())


def utc_daily_close_targets(decision_time: np.ndarray, trading_days: int = 1) -> np.ndarray:
    """Return the next UTC close, plus weekday closes for requested trading days.

    `trading_days=1` means the next eligible Monday-Friday close strictly after
    the decision. Weekends are skipped. The source's original session-close
    convention is unavailable, so UTC is a documented research default.
    """
    if trading_days < 1:
        raise ValueError("trading_days must be positive")
    targets = np.empty(len(decision_time), dtype=np.int64)
    for index, value in enumerate(decision_time):
        candidate = datetime.fromtimestamp(_next_close_after(int(value)), tz=timezone.utc)
        found = 0
        while found < trading_days:
            if candidate.weekday() < 5:
                found += 1
                if found == trading_days:
                    break
            candidate += timedelta(days=1)
        targets[index] = int(candidate.timestamp())
    return targets


def endpoint_for_calendar_targets(data: dict[str, np.ndarray], decision_time: np.ndarray, trading_days: int) -> dict[str, np.ndarray]:
    """Return midpoint endpoint outcomes with an explicit target-readiness clock."""
    targets = utc_daily_close_targets(decision_time, trading_days)
    lookup = {int(stamp): index for index, stamp in enumerate(data["time"])}
    state = np.full(len(targets), "missing_target", dtype=object)
    value = np.full(len(targets), np.nan, dtype=float)
    for index, target_decision in enumerate(targets):
        # A raw bar stamped T is usable only at T + 60 seconds. Target close's
        # midpoint is raw T - 60 seconds when T denotes the close boundary.
        target_raw = int(target_decision) - 60
        origin_raw = int(decision_time[index]) - 60
        target_index, origin_index = lookup.get(target_raw), lookup.get(origin_raw)
        if target_index is None or origin_index is None:
            continue
        origin, endpoint = float(data["close"][origin_index]), float(data["close"][target_index])
        if not (np.isfinite(origin) and np.isfinite(endpoint) and origin > 0 and endpoint > 0):
            continue
        state[index] = "endpoint_available"
        value[index] = (endpoint / origin - 1.0) * 1e4
    return {"target_decision_time": targets, "state": state, "midpoint_return_bps": value}
