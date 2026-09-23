"""Pure, versioned directional research features; no source or broker I/O.

Input epochs are completed M1 bar START labels; their close becomes available
at epoch+60. That clock is an economic bar-close convention, not proof of
historical receipt. The caller must enforce capture/first-seen availability.

The 24-field technical layout adapts oanda_pair_local_models_v2._features:
native-pip rates become start-price-relative bps/min; a directional window
requires every real minute rather than using a shorter/gapped rate. RMS uses
one-minute bps returns in the latest contiguous run. Session age resets at any
missing minute. Explicit coverage accompanies zero feature sentinels.

Peer means/breadth/dispersion reuse the currency-leg definitions in the unified
and repaired cross-window work, but exclude the target pair from BOTH currency
legs. They are observed covariates, never forecasts. Frozen lineage hashes are
in reuse_review/DIRECTION_FEATURE_REUSE_20260911.json; no old weights are used.
"""
from __future__ import annotations

from collections import deque
from typing import Mapping, Sequence
import re

import numpy as np
import pandas as pd

VERSION = "direction_features_v1_20260911"
WINDOWS_MINUTES = (1, 5, 15, 30, 60)
TECHNICAL_COLUMNS = tuple(
    f"tech_{name}_{window}m"
    for window in WINDOWS_MINUTES
    for name in ("rate_bps_per_min", "span_fraction", "missing_fraction", "unavailable")
) + ("tech_rms_bps", "tech_max_gap_minutes", "tech_observed_fraction_60m", "tech_session_age_hours")
_PEER_NAMES = (
    "base_mean_bps", "quote_mean_bps", "strength_gap_bps",
    "base_up_fraction", "quote_up_fraction", "breadth_gap",
    "base_std_bps", "quote_std_bps", "base_count", "quote_count",
    "base_missing", "quote_missing", "base_std_missing", "quote_std_missing",
    "strength_gap_missing",
)
PEER_COLUMNS = tuple(f"peer_{name}_{window}m" for window in WINDOWS_MINUTES for name in _PEER_NAMES)
FEATURE_COLUMNS = TECHNICAL_COLUMNS + PEER_COLUMNS
_PAIR = re.compile(r"[A-Z]{3}_[A-Z]{3}\Z")


def metadata() -> dict:
    return {
        "version": VERSION, "technical_count": len(TECHNICAL_COLUMNS),
        "peer_count": len(PEER_COLUMNS), "windows_minutes": list(WINDOWS_MINUTES),
        "technical_columns": list(TECHNICAL_COLUMNS), "peer_columns": list(PEER_COLUMNS),
        "epoch_semantics": "UTC_minute_start_close_available_at_epoch_plus_60",
        "historical_receipt_proven": False, "price_fill": False,
        "window_policy": "exact_endpoints_and_every_intervening_M1_close",
        "session_policy": "reset_at_every_missing_minute",
        "peer_policy": "exclude_target_instrument_from_base_and_quote_currency_legs",
        "return_units": "10000_times_end_over_start_minus_one",
        "research_only": True, "can_place_orders": False,
    }


def _inputs(candles_by_pair: Mapping[str, pd.DataFrame], pip_map: Mapping[str, float],
            cutoff_epoch: int | None) -> dict[str, pd.DataFrame]:
    if not isinstance(candles_by_pair, Mapping) or not isinstance(pip_map, Mapping):
        raise ValueError("price_and_pip_mappings_required")
    if cutoff_epoch is not None and (isinstance(cutoff_epoch, bool) or not isinstance(cutoff_epoch, (int, np.integer)) or cutoff_epoch < 0 or cutoff_epoch % 60):
        raise ValueError("cutoff_must_be_minute_start")
    result = {}
    for pair in sorted(candles_by_pair):
        if not isinstance(pair, str) or _PAIR.fullmatch(pair) is None or pair[:3] == pair[4:]:
            raise ValueError("invalid_instrument")
        pip = pip_map.get(pair)
        if isinstance(pip, bool) or not isinstance(pip, (int, float, np.number)) or not np.isfinite(pip) or not 1e-8 <= pip <= .1:
            raise ValueError("explicit_valid_pip_required")
        frame = candles_by_pair[pair]
        if not isinstance(frame, pd.DataFrame) or not {"epoch", "mid"}.issubset(frame.columns):
            raise ValueError("epoch_and_mid_required")
        if frame.columns.duplicated().any():
            raise ValueError("duplicate_columns")
        raw_epoch = frame["epoch"].to_numpy()
        if raw_epoch.dtype.kind not in "iu" or np.any(raw_epoch < 0) or np.any(raw_epoch % 60):
            raise ValueError("epoch_must_be_integer_minute_start")
        if raw_epoch.size and np.any(raw_epoch > np.iinfo(np.int64).max - 86400):
            raise ValueError("epoch_out_of_range")
        if frame["epoch"].duplicated().any():
            raise ValueError("duplicate_epoch")
        columns = ["epoch", "mid"]
        if ("bid" in frame) != ("ask" in frame):
            raise ValueError("bid_and_ask_required_together")
        if "bid" in frame:
            columns += ["bid", "ask"]
        selected = frame.loc[:, columns].sort_values("epoch", kind="stable").copy()
        if cutoff_epoch is not None:
            selected = selected.loc[selected.epoch <= cutoff_epoch].copy()
        for column in columns[1:]:
            values = selected[column].to_numpy()
            missing_cost = column in ("bid", "ask")
            if values.dtype.kind not in "fiu" or np.any(np.isinf(values)) or (not missing_cost and np.any(np.isnan(values))) or np.any(values <= 0):
                raise ValueError("finite_positive_prices_required")
            selected[column] = values.astype(float)
        if "bid" in selected:
            if np.any(np.isnan(selected.bid) != np.isnan(selected.ask)):
                raise ValueError("bid_and_ask_missingness_must_match")
            if np.any(selected.ask < selected.bid) or np.any(selected.mid < selected.bid) or np.any(selected.mid > selected.ask):
                raise ValueError("invalid_bid_ask_or_mid")
        # External observed/ingested timestamps, if present, remain caller-owned;
        # accepting them here must not silently convert bar labels into receipts.
        selected["epoch"] = selected["epoch"].astype("int64")
        result[pair] = selected.reset_index(drop=True)
    return result


def _technical(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[int, np.ndarray]]:
    epochs = frame.epoch.to_numpy(dtype=np.int64)
    mid = frame.mid.to_numpy(dtype=float)
    size = len(frame)
    positions = np.arange(size)
    if not size:
        return pd.DataFrame(columns=TECHNICAL_COLUMNS), {w: np.empty(0) for w in WINDOWS_MINUTES}
    gaps = np.r_[0., np.diff(epochs) / 60.]
    run_start = np.maximum.accumulate(np.where(np.r_[True, np.diff(epochs) != 60], positions, 0))
    output, returns = {}, {}
    for window in WINDOWS_MINUTES:
        start = np.searchsorted(epochs, epochs - window * 60, side="left")
        complete = positions - run_start >= window
        previous = np.maximum(0, positions - window)
        change = np.full(size, np.nan)
        change[complete] = (mid[complete] / mid[previous[complete]] - 1.) * 10000.
        returns[window] = change
        output[f"tech_rate_bps_per_min_{window}m"] = np.where(complete, change / window, 0.)
        output[f"tech_span_fraction_{window}m"] = (epochs - epochs[start]) / (60. * window)
        output[f"tech_missing_fraction_{window}m"] = 1. - (positions - start) / window
        output[f"tech_unavailable_{window}m"] = (~complete).astype(float)
    contiguous_start = np.maximum(run_start, positions - 60)
    changes = np.r_[0., (mid[1:] / mid[:-1] - 1.) * 10000.]
    changes[gaps != 1] = 0.
    cumulative = np.cumsum(changes * changes)
    count = positions - contiguous_start
    variance = np.divide(cumulative - cumulative[contiguous_start], count, out=np.zeros(size), where=count > 0)
    output["tech_rms_bps"] = np.sqrt(np.maximum(variance, 0.))
    full_start = np.searchsorted(epochs, epochs - 3600, side="left")
    max_gaps = np.zeros(size)
    pending = deque()
    for position in range(size):
        while pending and pending[0] <= full_start[position]:
            pending.popleft()
        if position > full_start[position]:
            while pending and gaps[pending[-1]] <= gaps[position]:
                pending.pop()
            pending.append(position)
        if pending:
            max_gaps[position] = gaps[pending[0]]
    output["tech_max_gap_minutes"] = max_gaps
    output["tech_observed_fraction_60m"] = (positions - full_start + 1) / 61.
    output["tech_session_age_hours"] = (epochs - epochs[run_start]) / 3600.
    return pd.DataFrame(output, columns=TECHNICAL_COLUMNS), returns


def build_features(candles_by_pair: Mapping[str, pd.DataFrame], *, pip_map: Mapping[str, float],
                   cutoff_epoch: int | None = None) -> pd.DataFrame:
    """Return all observed pair/minute rows, including explicitly missing features.

    The result index is (instrument, epoch); available_epoch is epoch+60.
    No label or future availability determines which feature rows are retained.
    Peer returns use only the exact same epoch, with no as-of price carry.
    """
    inputs = _inputs(candles_by_pair, pip_map, cutoff_epoch)
    parts, return_parts = [], {window: [] for window in WINDOWS_MINUTES}
    for pair, frame in inputs.items():
        technical, returns = _technical(frame)
        technical["instrument"] = pair
        technical["epoch"] = frame.epoch.to_numpy()
        technical["available_epoch"] = frame.epoch.to_numpy() + 60
        parts.append(technical)
        for window in WINDOWS_MINUTES:
            return_parts[window].append(returns[window])
    if not parts or not sum(len(part) for part in parts):
        empty = pd.DataFrame(columns=["instrument", "epoch", "available_epoch", *FEATURE_COLUMNS])
        return empty.set_index(["instrument", "epoch"])
    output = pd.concat(parts, ignore_index=True)
    epochs = output.epoch.to_numpy()
    names = output.instrument.to_numpy()
    bases = np.asarray([name[:3] for name in names])
    quotes = np.asarray([name[4:] for name in names])
    all_peer_fields = {}
    for window in WINDOWS_MINUTES:
        changes = np.concatenate(return_parts[window])
        available = np.isfinite(changes)
        leg_epochs = np.r_[epochs[available], epochs[available]]
        leg_currencies = np.r_[bases[available], quotes[available]]
        values = np.r_[changes[available], -changes[available]]
        legs = pd.DataFrame({"epoch": leg_epochs, "currency": leg_currencies,
                             "value": values, "square": values ** 2, "up": (values > 0).astype(float)})
        stats = legs.groupby(["epoch", "currency"], sort=False).agg(
            count=("value", "count"), total=("value", "sum"), square=("square", "sum"), positive=("up", "sum"))
        peer_values = {}
        for leg, currency, own in (("base", bases, changes), ("quote", quotes, -changes)):
            joined = stats.reindex(pd.MultiIndex.from_arrays([epochs, currency])).fillna(0.)
            count = joined["count"].to_numpy() - available.astype(float)
            own_finite = np.where(available, own, 0.)
            total = joined["total"].to_numpy() - own_finite
            square = joined["square"].to_numpy() - own_finite ** 2
            up = joined["positive"].to_numpy() - ((own > 0) & available).astype(float)
            mean = np.divide(total, count, out=np.zeros(len(output)), where=count > 0)
            breadth = np.divide(up, count, out=np.zeros(len(output)), where=count > 0)
            centered = square - np.divide(total ** 2, count, out=np.zeros(len(output)), where=count > 0)
            variance = np.divide(np.maximum(centered, 0.), count - 1., out=np.zeros(len(output)), where=count > 1)
            peer_values[leg] = (mean, breadth, count)
            for field, array in (("mean_bps", mean), ("up_fraction", breadth), ("std_bps", np.sqrt(variance)),
                                 ("count", count), ("missing", (count == 0).astype(float)), ("std_missing", (count < 2).astype(float))):
                all_peer_fields[f"peer_{leg}_{field}_{window}m"] = array
        base_mean, base_breadth, base_count = peer_values["base"]
        quote_mean, quote_breadth, quote_count = peer_values["quote"]
        both = (base_count > 0) & (quote_count > 0)
        all_peer_fields[f"peer_strength_gap_bps_{window}m"] = np.where(both, base_mean - quote_mean, 0.)
        all_peer_fields[f"peer_breadth_gap_{window}m"] = np.where(both, base_breadth - quote_breadth, 0.)
        all_peer_fields[f"peer_strength_gap_missing_{window}m"] = (~both).astype(float)
    output = pd.concat([output, pd.DataFrame(all_peer_fields, index=output.index)], axis=1)
    output = output.set_index(["instrument", "epoch"])
    return output.loc[:, ["available_epoch", *FEATURE_COLUMNS]].sort_index()


def build_labels(candles_by_pair: Mapping[str, pd.DataFrame], *, pip_map: Mapping[str, float],
                 horizons_minutes: Sequence[int] = (5, 15, 30, 60),
                 cutoff_epoch: int | None = None) -> pd.DataFrame:
    """Separate endpoint labels, requiring every intervening real minute.

    Output index: (instrument, epoch, horizon_minutes). Missing/cross-gap labels
    are absent. Economics are bid/ask endpoint returns per starting mid notional;
    spread is included, commissions, financing and slippage are NOT simulated.
    Unmeasured bid/ask produces NaN net labels, never assumed zero-cost labels.
    """
    if not horizons_minutes or any(isinstance(h, bool) or not isinstance(h, (int, np.integer)) or h <= 0 or h > 1440 for h in horizons_minutes) or len(set(horizons_minutes)) != len(horizons_minutes):
        raise ValueError("unique_positive_integer_horizons_required")
    inputs = _inputs(candles_by_pair, pip_map, cutoff_epoch)
    columns = ["instrument", "epoch", "horizon_minutes", "target_epoch", "label_available_epoch", "return_bps", "long_net_bps", "short_net_bps"]
    parts = []
    for pair, frame in inputs.items():
        epochs = frame.epoch.to_numpy(dtype=np.int64)
        mid = frame.mid.to_numpy(dtype=float)
        for horizon in horizons_minutes:
            if len(frame) <= horizon:
                continue
            origin = np.arange(len(frame) - horizon)
            target = origin + horizon
            complete = epochs[target] - epochs[origin] == horizon * 60
            origin, target = origin[complete], target[complete]
            count = len(origin)
            long_net, short_net = np.full(count, np.nan), np.full(count, np.nan)
            if "bid" in frame:
                bid, ask = frame.bid.to_numpy(dtype=float), frame.ask.to_numpy(dtype=float)
                long_net = (bid[target] - ask[origin]) / mid[origin] * 10000.
                short_net = (bid[origin] - ask[target]) / mid[origin] * 10000.
            parts.append(pd.DataFrame({"instrument": pair, "epoch": epochs[origin], "horizon_minutes": horizon,
                "target_epoch": epochs[target], "label_available_epoch": epochs[target] + 60,
                "return_bps": (mid[target] / mid[origin] - 1.) * 10000.,
                "long_net_bps": long_net, "short_net_bps": short_net}))
    output = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=columns)
    return output.set_index(["instrument", "epoch", "horizon_minutes"]).sort_index()
