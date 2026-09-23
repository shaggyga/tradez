#!/usr/bin/env python3
"""Reusable feature contract for frozen executable-opportunity models.

This module never fits, promotes, authorizes, or trades.  It reconstructs the
pre-entry features used by ``executable_opportunity_ranking_v1`` and can attach
labels for research diagnostics without changing the frozen model source.
"""

from __future__ import annotations

import datetime as dt
import math
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

import oanda_68_pair_opportunity_census as source


def feature_frame(
    by_pair: Mapping[str, Mapping[int, Mapping[str, float]]],
    epochs: Iterable[int],
    slippage: float,
    maximum_spread: float,
) -> pd.DataFrame:
    wanted = frozenset(int(value) for value in epochs)
    rows: list[dict[str, Any]] = []
    for instrument, timeline in by_pair.items():
        pip = source.pip_size(instrument)
        for epoch in wanted:
            current = timeline.get(epoch)
            if current is None:
                continue
            past = {seconds: timeline.get(epoch - seconds) for seconds in (60, 300, 900, 1800)}
            if any(value is None for value in past.values()):
                continue
            spread = float(current["spread"])
            if spread <= 0 or spread > maximum_spread:
                continue
            returns = {
                seconds: (float(current["mid"]) - float(value["mid"])) / pip
                for seconds, value in past.items()
            }
            moment = dt.datetime.fromtimestamp(epoch, dt.timezone.utc)
            hour = moment.hour + moment.minute / 60
            entry_cost = spread + slippage
            rows.append(
                {
                    "epoch": epoch,
                    "instrument": instrument,
                    "base": instrument.split("_")[0],
                    "quote": instrument.split("_")[1],
                    "spread_pips": spread,
                    "entry_cost_pips": entry_cost,
                    "log_updates": math.log1p(float(current["updates"])),
                    "imbalance_5s": float(current["imbalance_5s"]),
                    "imbalance_30s": float(current["imbalance_30s"]),
                    "imbalance_120s": float(current["imbalance_120s"]),
                    "return_1m_pips": returns[60],
                    "return_5m_pips": returns[300],
                    "return_15m_pips": returns[900],
                    "return_30m_pips": returns[1800],
                    "abs_return_1m_pips": abs(returns[60]),
                    "abs_return_5m_pips": abs(returns[300]),
                    "abs_return_15m_pips": abs(returns[900]),
                    "abs_return_30m_pips": abs(returns[1800]),
                    "momentum_cost_ratio": (0.65 * abs(returns[300]) + 0.35 * abs(returns[900])) / entry_cost,
                    "hour_sin": math.sin(2 * math.pi * hour / 24),
                    "hour_cos": math.cos(2 * math.pi * hour / 24),
                    "entry_mid": float(current["mid"]),
                }
            )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame = frame.sort_values(["epoch", "instrument"]).reset_index(drop=True)
    frame["spread_rank"] = frame.groupby("epoch")["spread_pips"].rank(pct=True, ascending=True)
    frame["movement_rank"] = frame.groupby("epoch")["momentum_cost_ratio"].rank(pct=True, ascending=True)
    frame["updates_rank"] = frame.groupby("epoch")["log_updates"].rank(pct=True, ascending=True)
    factor = np.zeros(len(frame), dtype=float)
    residual = np.zeros(len(frame), dtype=float)
    for _, indexes in frame.groupby("epoch").groups.items():
        indices = list(indexes)
        sums: dict[str, float] = {}
        counts: dict[str, int] = {}
        for index in indices:
            row = frame.loc[index]
            move = float(row["return_5m_pips"])
            for currency, value in ((row["base"], move), (row["quote"], -move)):
                sums[currency] = sums.get(currency, 0.0) + value
                counts[currency] = counts.get(currency, 0) + 1
        strengths = {currency: sums[currency] / counts[currency] for currency in sums}
        for index in indices:
            row = frame.loc[index]
            expected = strengths[row["base"]] - strengths[row["quote"]]
            factor[index] = expected
            residual[index] = float(row["return_5m_pips"]) - expected
    frame["currency_factor_5m_pips"] = factor
    frame["pair_residual_5m_pips"] = residual
    return frame


def labeled_frame(
    by_pair: Mapping[str, Mapping[int, Mapping[str, float]]],
    horizon: int,
    slippage: float,
    maximum_spread: float,
    aligned_only: bool = True,
) -> pd.DataFrame:
    epochs = {
        epoch
        for timeline in by_pair.values()
        for epoch in timeline
        if not aligned_only or epoch % horizon == 0
    }
    frame = feature_frame(by_pair, epochs, slippage, maximum_spread)
    if frame.empty:
        return frame
    labels: list[dict[str, Any] | None] = []
    for row in frame.itertuples(index=False):
        current = by_pair[str(row.instrument)].get(int(row.epoch))
        future = by_pair[str(row.instrument)].get(int(row.epoch) + horizon)
        if current is None or future is None:
            labels.append(None)
            continue
        window = source.modeled_window(str(row.instrument), current, future, slippage)
        labels.append(
            {
                "future_move_pips": float(window["signed_move_pips"]),
                "future_magnitude_pips": float(window["absolute_move_pips"]),
                "actual_cost_pips": float(window["modeled_cost_pips"]),
                "cost_clear": int(bool(window["movement_cleared_cost"])),
                "future_up": int(float(window["signed_move_pips"]) > 0),
            }
        )
    keep = [index for index, label in enumerate(labels) if label is not None]
    frame = frame.iloc[keep].reset_index(drop=True)
    attached = [labels[index] for index in keep]
    for key in ("future_move_pips", "future_magnitude_pips", "actual_cost_pips", "cost_clear", "future_up"):
        frame[key] = [label[key] for label in attached if label is not None]
    return frame
