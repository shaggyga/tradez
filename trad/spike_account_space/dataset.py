"""Exact wall-clock bar loading and forward-label construction.

The stable significant-move cache contains a complete 5-minute wall-clock
lattice per instrument.  Closed-market and missing-source intervals are kept
as rows with missing prices.  This is important: removing those rows and then
using ``shift(24)`` would turn 24 observations into an alleged two-hour label.
This module instead joins the endpoint at ``timestamp + 120 minutes``.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


FIVE_MINUTES = pd.Timedelta(minutes=5)
DEFAULT_BAR_CACHE_ROOT = (
    Path(__file__).resolve().parents[1] / "data" / "significant_moves" / "cache" / "bars"
)

# Copied from the validated OANDA metadata inventory used by the permanent
# significant-move subsystem.  HKD_JPY intentionally is not in this set.
PIP_LOCATION_MINUS2 = frozenset(
    {
        "AUD_JPY",
        "CAD_JPY",
        "CHF_JPY",
        "EUR_HUF",
        "EUR_JPY",
        "GBP_JPY",
        "NZD_JPY",
        "SGD_JPY",
        "TRY_JPY",
        "USD_HUF",
        "USD_JPY",
        "USD_THB",
        "ZAR_JPY",
    }
)

BAR_COLUMNS = (
    "timestamp",
    "instrument",
    "mid_open",
    "mid_high",
    "mid_low",
    "mid_close",
    "bid_open",
    "bid_high",
    "bid_low",
    "bid_close",
    "ask_open",
    "ask_high",
    "ask_low",
    "ask_close",
    "spread_pips",
    "pip_size",
    "bar_observed",
    "source_m1_observations",
    "spread_estimated_fraction",
    "bid_ask_open_high_low_is_derived",
    "bid_ask_close_is_estimated",
)

DECISION_COLUMNS = (
    "decision_id",
    "timestamp",
    "outcome_timestamp",
    "instrument",
    "horizon_minutes",
    "start_mid",
    "end_mid",
    "forward_signed_pips",
    "forward_abs_pips",
    "forward_signed_return",
    "forward_abs_return",
    "forward_long_net_pips",
    "forward_short_net_pips",
    "forward_observation_count",
    "forward_expected_observation_count",
    "forward_observation_fraction",
    "label_is_valid",
    "label_quality_flags",
    "event_cluster_id",
)

_CACHE_REQUIRED = {"time_utc", "open", "high", "low", "close", "spread_pips"}


def normalize_instrument(value: str) -> str:
    """Return an uppercase ``AAA_BBB`` instrument name or raise."""

    normalized = str(value or "").strip().upper().replace("/", "_")
    parts = normalized.split("_")
    if len(parts) != 2 or any(len(part) != 3 or not part.isalpha() for part in parts):
        raise ValueError(f"invalid FX instrument name: {value!r}")
    return normalized


def pip_size(instrument: str) -> float:
    """Return the project-validated pip size for an OANDA FX instrument."""

    return 0.01 if normalize_instrument(instrument) in PIP_LOCATION_MINUS2 else 0.0001


def _strict_utc_series(values: pd.Series, *, name: str) -> pd.Series:
    """Parse timezone-aware UTC values without silently localizing naive data."""

    if pd.api.types.is_datetime64_dtype(values.dtype) and not isinstance(
        values.dtype, pd.DatetimeTZDtype
    ):
        raise ValueError(f"{name} must be timezone-aware UTC; naive datetimes are not accepted")
    parsed = pd.to_datetime(values, errors="raise")
    if not isinstance(parsed.dtype, pd.DatetimeTZDtype):
        raise ValueError(f"{name} must be timezone-aware UTC; naive datetimes are not accepted")
    if str(parsed.dt.tz).upper() not in {"UTC", "ETC/UTC"}:
        raise ValueError(f"{name} must already be UTC, found timezone {parsed.dt.tz!s}")
    return parsed.dt.tz_convert("UTC")


def _utc_boundary(value: Any | None) -> pd.Timestamp | None:
    if value is None:
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def validate_utc_grid(
    bars: pd.DataFrame,
    *,
    frequency_minutes: int = 5,
    require_complete_grid: bool = True,
) -> None:
    """Validate unique, ordered, timezone-aware UTC bars on a wall-clock grid.

    With ``require_complete_grid=True`` every instrument must include every grid
    timestamp from its first row to its last.  Missing market observations must
    therefore be represented by an explicit row with missing prices.
    """

    required = {"timestamp", "instrument"}
    missing = sorted(required.difference(bars.columns))
    if missing:
        raise ValueError(f"bar frame missing required columns: {missing}")
    if bars.empty:
        return
    timestamps = _strict_utc_series(bars["timestamp"], name="timestamp")
    if timestamps.isna().any():
        raise ValueError("timestamp contains missing values")
    if bars["instrument"].isna().any():
        raise ValueError("instrument contains missing values")
    if bars.assign(timestamp=timestamps).duplicated(["instrument", "timestamp"]).any():
        raise ValueError("duplicate instrument/timestamp bars are not allowed")

    step = pd.Timedelta(minutes=int(frequency_minutes))
    if step <= pd.Timedelta(0):
        raise ValueError("frequency_minutes must be positive")
    if bool((timestamps != timestamps.dt.floor(step)).any()):
        raise ValueError(f"timestamps are not aligned to the {frequency_minutes}-minute UTC grid")

    check = pd.DataFrame(
        {"instrument": bars["instrument"].astype(str).to_numpy(), "timestamp": timestamps}
    ).sort_values(["instrument", "timestamp"])
    differences = check.groupby("instrument", sort=False)["timestamp"].diff().dropna()
    if require_complete_grid:
        invalid = differences != step
        description = "exactly one grid step"
    else:
        invalid = (differences <= pd.Timedelta(0)) | (
            differences % step != pd.Timedelta(0)
        )
        description = "a positive whole number of grid steps"
    if bool(invalid.any()):
        first = differences[invalid].iloc[0]
        raise ValueError(f"UTC grid gap must be {description}; found {first}")


def normalize_cached_bars(
    frame: pd.DataFrame,
    instrument: str,
    *,
    require_complete_grid: bool = True,
) -> pd.DataFrame:
    """Normalize one stable significant-move bar-cache frame.

    The cache stores observed bid/ask closes but only mid OHLC.  Bid/ask open,
    high, and low are transparent spread-based reconstructions and are flagged
    as derived.  Observed endpoint bid/ask values are retained when present.
    """

    instrument = normalize_instrument(instrument)
    missing = sorted(_CACHE_REQUIRED.difference(frame.columns))
    if missing:
        raise ValueError(f"cache frame for {instrument} missing required columns: {missing}")

    timestamp = _strict_utc_series(frame["time_utc"], name="time_utc")
    size = pip_size(instrument)
    numeric = {
        column: pd.to_numeric(frame[column], errors="coerce")
        for column in [
            "open",
            "high",
            "low",
            "close",
            "bid_close",
            "ask_close",
            "spread_pips",
            "source_m1_observations",
            "spread_estimated_fraction",
        ]
        if column in frame.columns
    }
    spread = numeric["spread_pips"]
    half_spread_price = spread * size / 2.0
    mid_close = numeric["close"]
    derived_bid_close = mid_close - half_spread_price
    derived_ask_close = mid_close + half_spread_price
    source_bid_close = numeric.get("bid_close", pd.Series(np.nan, index=frame.index))
    source_ask_close = numeric.get("ask_close", pd.Series(np.nan, index=frame.index))
    observed_close = (
        source_bid_close.notna()
        & source_ask_close.notna()
        & (source_ask_close >= source_bid_close)
    )
    bid_close = source_bid_close.where(observed_close, derived_bid_close)
    ask_close = source_ask_close.where(observed_close, derived_ask_close)
    bid_open = numeric["open"] - half_spread_price
    ask_open = numeric["open"] + half_spread_price
    bid_high = pd.concat(
        [numeric["high"] - half_spread_price, bid_open, bid_close], axis=1
    ).max(axis=1, skipna=True)
    bid_low = pd.concat(
        [numeric["low"] - half_spread_price, bid_open, bid_close], axis=1
    ).min(axis=1, skipna=True)
    ask_high = pd.concat(
        [numeric["high"] + half_spread_price, ask_open, ask_close], axis=1
    ).max(axis=1, skipna=True)
    ask_low = pd.concat(
        [numeric["low"] + half_spread_price, ask_open, ask_close], axis=1
    ).min(axis=1, skipna=True)

    out = pd.DataFrame(
        {
            "timestamp": timestamp,
            "instrument": instrument,
            "mid_open": numeric["open"],
            "mid_high": numeric["high"],
            "mid_low": numeric["low"],
            "mid_close": mid_close,
            "bid_open": bid_open,
            "bid_high": bid_high,
            "bid_low": bid_low,
            "bid_close": bid_close,
            "ask_open": ask_open,
            "ask_high": ask_high,
            "ask_low": ask_low,
            "ask_close": ask_close,
            "spread_pips": spread,
            "pip_size": size,
            "bar_observed": mid_close.notna(),
            "source_m1_observations": numeric.get(
                "source_m1_observations", pd.Series(np.nan, index=frame.index)
            ),
            "spread_estimated_fraction": numeric.get(
                "spread_estimated_fraction", pd.Series(np.nan, index=frame.index)
            ),
            "bid_ask_open_high_low_is_derived": True,
            "bid_ask_close_is_estimated": ~observed_close & mid_close.notna(),
        }
    )
    out = out.sort_values("timestamp", kind="stable").reset_index(drop=True)

    observed = out["bar_observed"]
    invalid_mid = observed & (
        (out["mid_close"] <= 0)
        | (out["mid_high"] < out[["mid_open", "mid_close"]].max(axis=1))
        | (out["mid_low"] > out[["mid_open", "mid_close"]].min(axis=1))
        | (out["mid_high"] < out["mid_low"])
    )
    if bool(invalid_mid.any()):
        raise ValueError(f"cache frame for {instrument} contains invalid observed mid OHLC")
    invalid_spread = observed & (
        out["bid_close"].notna()
        & out["ask_close"].notna()
        & (out["ask_close"] < out["bid_close"])
    )
    if bool(invalid_spread.any()):
        raise ValueError(f"cache frame for {instrument} contains crossed bid/ask closes")
    validate_utc_grid(out, require_complete_grid=require_complete_grid)
    return out.loc[:, list(BAR_COLUMNS)]


def _resolve_cache_root(path: str | Path | None) -> Path:
    root = DEFAULT_BAR_CACHE_ROOT if path is None else Path(path)
    candidates = [root, root / "cache" / "bars"]
    for candidate in candidates:
        if candidate.is_dir() and any(candidate.glob("*.parquet")):
            return candidate.resolve()
    raise FileNotFoundError(f"no instrument Parquet files found under {root}")


def iter_normalized_bars(
    cache_root: str | Path | None = None,
    *,
    instruments: Sequence[str] | None = None,
    start: Any | None = None,
    end: Any | None = None,
    require_complete_grid: bool = True,
    drop_empty_bars: bool = False,
) -> Iterator[tuple[str, pd.DataFrame]]:
    """Yield normalized bars one instrument at a time to bound memory use."""

    root = _resolve_cache_root(cache_root)
    requested = (
        None if instruments is None else {normalize_instrument(value) for value in instruments}
    )
    files = sorted(root.glob("*.parquet"))
    available = {normalize_instrument(path.stem): path for path in files}
    if requested is not None:
        missing = sorted(requested.difference(available))
        if missing:
            raise FileNotFoundError(f"bar caches not found for instruments: {missing}")
        selected = [(name, available[name]) for name in sorted(requested)]
    else:
        selected = sorted(available.items())

    start_utc = _utc_boundary(start)
    end_utc = _utc_boundary(end)
    if start_utc is not None and end_utc is not None and end_utc < start_utc:
        raise ValueError("end must not be earlier than start")

    for instrument, path in selected:
        raw = pd.read_parquet(path)
        bars = normalize_cached_bars(
            raw, instrument, require_complete_grid=require_complete_grid
        )
        if start_utc is not None:
            bars = bars.loc[bars["timestamp"] >= start_utc]
        if end_utc is not None:
            bars = bars.loc[bars["timestamp"] <= end_utc]
        bars = bars.reset_index(drop=True)
        if drop_empty_bars:
            bars = bars.loc[bars["bar_observed"]].reset_index(drop=True)
        yield instrument, bars


def load_normalized_bars(
    cache_root: str | Path | None = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """Load and concatenate normalized caches.

    For the full 68-pair universe, prefer :func:`iter_normalized_bars` unless a
    single in-memory table is explicitly needed.
    """

    frames = [frame for _, frame in iter_normalized_bars(cache_root, **kwargs)]
    if not frames:
        return pd.DataFrame(columns=BAR_COLUMNS)
    return pd.concat(frames, ignore_index=True).sort_values(
        ["timestamp", "instrument"], kind="stable", ignore_index=True
    )


def _decision_ids(instrument: pd.Series, timestamp: pd.Series, horizon: int) -> pd.Series:
    stamp = timestamp.dt.strftime("%Y%m%dT%H%M%SZ")
    return "decision|" + instrument.astype(str) + "|" + stamp + f"|{horizon}m"


def build_forward_labels(
    bars: pd.DataFrame,
    *,
    horizon_minutes: int = 120,
    minimum_observation_fraction: float = 0.0,
    drop_invalid: bool = True,
    require_complete_grid: bool = True,
) -> pd.DataFrame:
    """Build exact wall-clock forward labels from normalized 5-minute bars.

    Endpoint values are matched by ``timestamp + horizon`` in a one-to-one
    timestamp join.  No row-offset shift and no price forward-fill is used.
    Returns are decimal returns, not percentages.
    """

    horizon = int(horizon_minutes)
    if horizon <= 0 or horizon % 5:
        raise ValueError("horizon_minutes must be a positive multiple of five")
    if not 0.0 <= float(minimum_observation_fraction) <= 1.0:
        raise ValueError("minimum_observation_fraction must be between zero and one")
    needed = {
        "timestamp",
        "instrument",
        "mid_close",
        "bid_close",
        "ask_close",
        "pip_size",
        "bar_observed",
    }
    missing = sorted(needed.difference(bars.columns))
    if missing:
        raise ValueError(f"normalized bars missing required columns: {missing}")
    validate_utc_grid(bars, require_complete_grid=require_complete_grid)
    if bars.empty:
        return pd.DataFrame(columns=DECISION_COLUMNS)

    expected = horizon // 5
    delta = pd.Timedelta(minutes=horizon)
    parts: list[pd.DataFrame] = []
    for instrument, source in bars.groupby("instrument", sort=True, observed=True):
        source = source.sort_values("timestamp", kind="stable").reset_index(drop=True).copy()
        source["_observed_cumulative"] = source["bar_observed"].fillna(False).astype(int).cumsum()
        starts = source[
            [
                "timestamp",
                "instrument",
                "mid_close",
                "bid_close",
                "ask_close",
                "pip_size",
                "bar_observed",
                "_observed_cumulative",
            ]
        ].rename(
            columns={
                "mid_close": "start_mid",
                "bid_close": "start_bid",
                "ask_close": "start_ask",
                "bar_observed": "start_observed",
                "_observed_cumulative": "start_observed_cumulative",
            }
        )
        endpoints = source[
            [
                "timestamp",
                "mid_close",
                "bid_close",
                "ask_close",
                "bar_observed",
                "_observed_cumulative",
            ]
        ].rename(
            columns={
                "timestamp": "outcome_timestamp",
                "mid_close": "end_mid",
                "bid_close": "end_bid",
                "ask_close": "end_ask",
                "bar_observed": "end_observed",
                "_observed_cumulative": "end_observed_cumulative",
            }
        )
        # This key is the defining anti-row-offset operation.
        endpoints["timestamp"] = endpoints["outcome_timestamp"] - delta
        merged = starts.merge(
            endpoints,
            on="timestamp",
            how="left",
            validate="one_to_one",
            sort=False,
        )
        merged["outcome_timestamp"] = merged["timestamp"] + delta
        count = merged["end_observed_cumulative"] - merged["start_observed_cumulative"]
        merged["forward_observation_count"] = count.fillna(0).astype(int)
        merged["forward_expected_observation_count"] = expected
        merged["forward_observation_fraction"] = merged["forward_observation_count"] / expected

        signed_price = merged["end_mid"] - merged["start_mid"]
        merged["forward_signed_pips"] = signed_price / merged["pip_size"]
        merged["forward_abs_pips"] = merged["forward_signed_pips"].abs()
        merged["forward_signed_return"] = merged["end_mid"] / merged["start_mid"] - 1.0
        merged["forward_abs_return"] = merged["forward_signed_return"].abs()
        merged["forward_long_net_pips"] = (
            merged["end_bid"] - merged["start_ask"]
        ) / merged["pip_size"]
        merged["forward_short_net_pips"] = (
            merged["start_bid"] - merged["end_ask"]
        ) / merged["pip_size"]
        merged["horizon_minutes"] = horizon
        merged["label_is_valid"] = (
            merged["start_observed"].fillna(False)
            & merged["end_observed"].fillna(False)
            & merged["start_mid"].notna()
            & merged["end_mid"].notna()
            & (merged["start_mid"] > 0)
            & (merged["forward_observation_fraction"] >= minimum_observation_fraction)
        )
        no_endpoint = merged["end_mid"].isna()
        sparse = merged["forward_observation_fraction"] < minimum_observation_fraction
        flags = np.where(no_endpoint, f"missing_exact_{horizon}m_endpoint", "")
        flags = np.where(
            sparse,
            np.where(flags == "", "sparse_forward_window", flags + "|sparse_forward_window"),
            flags,
        )
        merged["label_quality_flags"] = flags
        merged["decision_id"] = _decision_ids(
            merged["instrument"], merged["timestamp"], horizon
        )
        merged["event_cluster_id"] = pd.Series(pd.NA, index=merged.index, dtype="string")
        parts.append(merged)

    output = pd.concat(parts, ignore_index=True)
    if drop_invalid:
        output = output.loc[output["label_is_valid"]]
    output = output.sort_values(["timestamp", "instrument"], kind="stable").reset_index(drop=True)
    return output.loc[:, list(DECISION_COLUMNS)]
