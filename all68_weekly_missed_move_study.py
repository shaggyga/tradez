"""Weekly major-move and reusable-condition study for all 68 OANDA FX instruments.

Discovery data ends before the requested week. Candidate feature ranges are then
validated on the requested week, so this report does not derive and validate a
condition on the same moves.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DATA_ROOT = Path("data") / "oanda_training_manager" / "candles"
ROOT = Path("data") / "all68_weekly_move_study"
FEATURE_ROOT = ROOT / "features"
REPORT_ROOT = ROOT / "reports"
JOURNAL_PATH = Path("data") / "forex" / "logging" / "full_event_journal.csv"
ORDER_PATH = Path("data") / "forex" / "logging" / "order_result_ledger.csv"
LIFECYCLE_PATH = Path("data") / "forex" / "logging" / "trade_lifecycle_ledger.csv"
EVENT_WINDOWS_PATH = (
    Path("data")
    / "technical_scout_manager"
    / "account_technical_scout_all_pairs"
    / "event_windows.csv"
)

HORIZONS = [15, 30, 60, 120, 240]
CONDITION_HORIZONS = [30, 60, 120]
EXOTIC_CURRENCIES = {
    "CNH",
    "CZK",
    "DKK",
    "HKD",
    "HUF",
    "MXN",
    "NOK",
    "PLN",
    "SEK",
    "SGD",
    "THB",
    "TRY",
    "ZAR",
}
LIVE_START = pd.Timestamp("2026-06-19T03:07:00Z")
FRIDAY_BLOCK_START = pd.Timestamp("2026-06-19T19:00:00Z")
PIPELINE_VERSION = "all68_weekly_move_study_v1"
FEATURE_TIMEFRAME_ALIASES = {
    "M5": "5min",
    "M15": "15min",
    "M30": "30min",
    "H1": "1h",
    "H4": "4h",
    "D1": "1D",
}
FEATURE_COLUMNS = [
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
]
PIP_LOCATION_MINUS2 = {
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


def pip_multiplier(pair: str) -> float:
    return 100.0 if pair in PIP_LOCATION_MINUS2 else 10_000.0


def pair_parts(pair: str) -> tuple[str, str]:
    base, quote = pair.split("_", 1)
    return base, quote


def pair_group(pair: str) -> str:
    base, quote = pair_parts(pair)
    if "USD" in (base, quote):
        return "usd_related"
    if base in EXOTIC_CURRENCIES or quote in EXOTIC_CURRENCIES:
        return "non_usd_exotic"
    return "non_usd_cross"


def normalize_feature_timeframe(value: str) -> str:
    text = str(value or "5min").strip()
    if not text:
        return "5min"
    return FEATURE_TIMEFRAME_ALIASES.get(text.upper(), text)


def feature_timeframe_slug(value: str) -> str:
    text = normalize_feature_timeframe(value).lower()
    return (
        text.replace(" ", "")
        .replace("/", "")
        .replace("min", "m")
        .replace("h", "h")
        .replace("d", "d")
    )


def feature_root_for_timeframe(value: str) -> Path:
    slug = feature_timeframe_slug(value)
    return FEATURE_ROOT if slug == "5m" else ROOT / f"features_{slug}"


def report_root_for_timeframe(value: str) -> Path:
    slug = feature_timeframe_slug(value)
    return REPORT_ROOT if slug == "5m" else ROOT / f"reports_{slug}"


def timeframe_minutes(value: str) -> int:
    delta = pd.Timedelta(normalize_feature_timeframe(value))
    minutes = int(delta.total_seconds() // 60)
    if minutes <= 0:
        raise ValueError(f"Unsupported feature timeframe: {value}")
    return minutes


def bars_for_minutes(minutes: int, base_minutes: int) -> int:
    return max(1, int(round(float(minutes) / float(max(base_minutes, 1)))))


def exact_future(series: pd.Series, minutes: int) -> np.ndarray:
    indexed = pd.Series(series.to_numpy(), index=series.index)
    return indexed.reindex(series.index + pd.Timedelta(minutes=minutes)).to_numpy()


def future_values_at(
    series: pd.Series,
    index: pd.Index,
    minutes: int,
    *,
    base_minutes: int,
) -> np.ndarray:
    indexed = pd.Series(series.to_numpy(), index=series.index)
    label_offset = pd.Timedelta(minutes=max(0, base_minutes - 1))
    return indexed.reindex(index + label_offset + pd.Timedelta(minutes=minutes)).to_numpy()


def rsi(close: pd.Series, window: int = 14) -> pd.Series:
    change = close.diff()
    gain = change.clip(lower=0).rolling(window).mean()
    loss = (-change.clip(upper=0)).rolling(window).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100.0 - 100.0 / (1.0 + rs)


def load_spread_estimates(pairs: list[str]) -> dict[str, float]:
    observed: dict[str, float] = {}
    if EVENT_WINDOWS_PATH.exists():
        frame = pd.read_csv(
            EVENT_WINDOWS_PATH,
            usecols=["instrument", "spread_avg_pips"],
        )
        frame["spread_avg_pips"] = pd.to_numeric(
            frame["spread_avg_pips"], errors="coerce"
        )
        observed = (
            frame.dropna()
            .groupby("instrument")["spread_avg_pips"]
            .median()
            .to_dict()
        )
    known_values = list(observed.values())
    fallback = float(np.median(known_values)) if known_values else 5.0
    estimates: dict[str, float] = {}
    for pair in pairs:
        if pair in observed:
            estimates[pair] = max(0.1, float(observed[pair]))
            continue
        base, quote = pair_parts(pair)
        neighbors = [
            value
            for other, value in observed.items()
            if base in pair_parts(other) or quote in pair_parts(other)
        ]
        estimates[pair] = max(
            0.1,
            float(np.median(neighbors)) if neighbors else fallback,
        )
    return estimates


def load_pair(
    pair: str,
    estimated_spread_pips: float,
    feature_timeframe: str = "5min",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    path = DATA_ROOT / f"{pair}_M1.csv"
    frame = pd.read_csv(
        path,
        usecols=lambda column: column
        in {
            "time",
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "bid_close",
            "ask_close",
            "spread_pips",
            "volume",
        },
    )
    frame["time_utc"] = pd.to_datetime(
        frame.get("datetime", frame.get("time")), errors="coerce", utc=True
    )
    for column in [
        "open",
        "high",
        "low",
        "close",
        "bid_close",
        "ask_close",
        "spread_pips",
        "volume",
    ]:
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    frame = (
        frame.dropna(subset=["time_utc", "open", "high", "low", "close"])
        .sort_values("time_utc")
        .drop_duplicates("time_utc", keep="last")
        .set_index("time_utc")
    )
    frame["spread_pips"] = frame["spread_pips"].fillna(estimated_spread_pips)
    half_spread = estimated_spread_pips / pip_multiplier(pair) / 2.0
    frame["bid_close"] = frame["bid_close"].fillna(frame["close"] - half_spread)
    frame["ask_close"] = frame["ask_close"].fillna(frame["close"] + half_spread)
    rule = normalize_feature_timeframe(feature_timeframe)
    bars = pd.DataFrame(
        {
            "open": frame["open"].resample(rule).first(),
            "high": frame["high"].resample(rule).max(),
            "low": frame["low"].resample(rule).min(),
            "close": frame["close"].resample(rule).last(),
            "bid_close": frame["bid_close"].resample(rule).last(),
            "ask_close": frame["ask_close"].resample(rule).last(),
            "spread_pips": frame["spread_pips"].resample(rule).median(),
            "volume": frame["volume"].resample(rule).sum(min_count=1),
        }
    ).dropna(subset=["open", "high", "low", "close", "spread_pips"])
    return bars, frame


def build_features(
    pair: str,
    frame: pd.DataFrame,
    raw_frame: pd.DataFrame | None = None,
    *,
    base_minutes: int = 5,
) -> pd.DataFrame:
    multiplier = pip_multiplier(pair)
    close = frame["close"].astype(float)
    previous = close.shift()
    true_range = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - previous).abs(),
            (frame["low"] - previous).abs(),
        ],
        axis=1,
    ).max(axis=1) * multiplier
    atr15 = true_range.rolling(bars_for_minutes(15, base_minutes)).mean()
    atr240 = true_range.rolling(bars_for_minutes(240, base_minutes)).mean()
    scale = atr240.replace(0, np.nan)
    output = frame.copy()
    output["instrument"] = pair
    for minutes in [5, 15, 30, 60]:
        bars = bars_for_minutes(minutes, base_minutes)
        output[f"momentum_{minutes}_pips"] = close.diff(bars) * multiplier
        output[f"momentum_{minutes}_atr"] = output[f"momentum_{minutes}_pips"] / scale
    output["acceleration_15_atr"] = (
        output["momentum_15_pips"]
        - output["momentum_15_pips"].shift(bars_for_minutes(15, base_minutes))
    ) / scale
    sma7 = close.rolling(7).mean()
    sma8 = close.rolling(8).mean()
    sma30 = close.rolling(30).mean()
    output["sma7_minus_8_atr"] = (sma7 - sma8) * multiplier / scale
    output["sma30_slope_5_atr"] = (
        sma30.diff(bars_for_minutes(5, base_minutes)) * multiplier / scale
    )
    output["rsi14_centered"] = (rsi(close, 14) - 50.0) / 50.0
    output["atr15_to_atr240"] = atr15 / scale
    output["compression_30"] = (
        (close.rolling(6).max() - close.rolling(6).min()) * multiplier
    ) / (scale * math.sqrt(6))
    for minutes in [60, 240]:
        bars = max(2, bars_for_minutes(minutes, base_minutes))
        low = close.rolling(bars).min()
        high = close.rolling(bars).max()
        output[f"range_position_{minutes}_centered"] = (
            (close - low) / (high - low).replace(0, np.nan) - 0.5
        ) * 2.0
    output["spread_ratio_60"] = output["spread_pips"] / output["spread_pips"].rolling(
        bars_for_minutes(60, base_minutes)
    ).median()
    volume_window = max(2, bars_for_minutes(30, base_minutes))
    volume_mean = output["volume"].rolling(volume_window).mean()
    volume_std = output["volume"].rolling(volume_window).std()
    output["volume_z_30"] = (output["volume"] - volume_mean) / volume_std.replace(0, np.nan)
    output["atr240_pips"] = atr240
    label_frame = raw_frame if raw_frame is not None else frame
    for horizon in HORIZONS:
        future_mid = future_values_at(
            label_frame["close"].astype(float),
            output.index,
            horizon,
            base_minutes=base_minutes,
        )
        future_bid = future_values_at(
            label_frame["bid_close"].astype(float),
            output.index,
            horizon,
            base_minutes=base_minutes,
        )
        future_ask = future_values_at(
            label_frame["ask_close"].astype(float),
            output.index,
            horizon,
            base_minutes=base_minutes,
        )
        output[f"future_move_pips_{horizon}"] = (future_mid - close.to_numpy()) * multiplier
        output[f"future_long_net_pips_{horizon}"] = (
            future_bid - output["ask_close"].to_numpy()
        ) * multiplier
        output[f"future_short_net_pips_{horizon}"] = (
            output["bid_close"].to_numpy() - future_ask
        ) * multiplier
    output = output.replace([np.inf, -np.inf], np.nan)
    return output


def add_currency_strength(
    feature_paths: dict[str, Path],
    *,
    base_minutes: int = 5,
) -> dict[str, Path]:
    closes: dict[str, pd.Series] = {}
    for pair, path in feature_paths.items():
        frame = pd.read_parquet(path, columns=["close"])
        closes[pair] = frame["close"]
    wide = pd.concat(closes, axis=1).sort_index()
    strength_by_horizon: dict[int, pd.DataFrame] = {}
    rank_by_horizon: dict[int, pd.DataFrame] = {}
    currencies = sorted({currency for pair in feature_paths for currency in pair_parts(pair)})
    for minutes in [15, 60]:
        pair_returns = np.log(wide).diff(bars_for_minutes(minutes, base_minutes))
        strength = pd.DataFrame(index=wide.index)
        for currency in currencies:
            contributions = []
            for pair in feature_paths:
                base, quote = pair_parts(pair)
                if base == currency:
                    contributions.append(pair_returns[pair])
                elif quote == currency:
                    contributions.append(-pair_returns[pair])
            strength[currency] = pd.concat(contributions, axis=1).median(axis=1)
        strength_by_horizon[minutes] = strength
        rank_by_horizon[minutes] = strength.rank(axis=1, pct=True)
    updated: dict[str, Path] = {}
    for pair, path in feature_paths.items():
        frame = pd.read_parquet(path)
        base, quote = pair_parts(pair)
        for minutes in [15, 60]:
            strength = strength_by_horizon[minutes]
            ranks = rank_by_horizon[minutes]
            frame[f"strength_gap_{minutes}"] = (
                strength[base] - strength[quote]
            ).reindex(frame.index)
            frame[f"strength_gap_rank_{minutes}"] = (
                ranks[base] - ranks[quote]
            ).reindex(frame.index)
        frame.to_parquet(path, compression="zstd")
        updated[pair] = path
    return updated


def build_all_features(
    *,
    feature_root: Path = FEATURE_ROOT,
    feature_timeframe: str = "5min",
) -> tuple[dict[str, Path], pd.DataFrame]:
    feature_timeframe = normalize_feature_timeframe(feature_timeframe)
    base_minutes = timeframe_minutes(feature_timeframe)
    feature_root.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    quality: list[dict[str, Any]] = []
    files = sorted(DATA_ROOT.glob("*_M1.csv"))
    pairs = [source.stem.removesuffix("_M1") for source in files]
    spread_estimates = load_spread_estimates(pairs)
    observed_spread_pairs: set[str] = set()
    if EVENT_WINDOWS_PATH.exists():
        observed_spread_pairs = set(
            pd.read_csv(EVENT_WINDOWS_PATH, usecols=["instrument"])["instrument"]
            .dropna()
            .astype(str)
        )
    for number, source in enumerate(files, 1):
        pair = source.stem.removesuffix("_M1")
        print(f"[features] {number}/{len(files)} {pair}", flush=True)
        bars, raw = load_pair(pair, spread_estimates[pair], feature_timeframe)
        features = build_features(pair, bars, raw, base_minutes=base_minutes)
        path = feature_root / f"{pair}.parquet"
        features.to_parquet(path, compression="zstd")
        paths[pair] = path
        quality.append(
            {
                "instrument": pair,
                "group": pair_group(pair),
                "feature_timeframe": feature_timeframe,
                "base_minutes": base_minutes,
                "rows": len(features),
                "start": features.index.min(),
                "end": features.index.max(),
                "week_rows": int(
                    (
                        (features.index >= pd.Timestamp("2026-06-15T00:00:00Z"))
                        & (features.index < pd.Timestamp("2026-06-20T00:00:00Z"))
                    ).sum()
                ),
                "estimated_spread_pips": spread_estimates[pair],
                "spread_source": (
                    "observed_june19_event_windows"
                    if pair in observed_spread_pairs
                    else "currency_neighbor_estimate"
                ),
            }
        )
    add_currency_strength(paths, base_minutes=base_minutes)
    return paths, pd.DataFrame(quality)


def historical_thresholds(
    frame: pd.DataFrame, discovery_end: pd.Timestamp
) -> dict[int, dict[str, float]]:
    discovery = frame[frame.index < discovery_end]
    thresholds: dict[int, dict[str, float]] = {}
    for horizon in HORIZONS:
        move = discovery[f"future_move_pips_{horizon}"].abs().dropna()
        thresholds[horizon] = {
            "q50": float(move.quantile(0.50)),
            "q99": float(move.quantile(0.99)),
            "q995": float(move.quantile(0.995)),
        }
    return thresholds


def candidate_events(
    paths: dict[str, Path], week_start: pd.Timestamp, week_end: pd.Timestamp
) -> tuple[pd.DataFrame, pd.DataFrame]:
    candidates: list[dict[str, Any]] = []
    volatility_rows: list[dict[str, Any]] = []
    for pair, path in paths.items():
        frame = pd.read_parquet(path)
        frame.index = pd.to_datetime(frame.index, utc=True)
        thresholds = historical_thresholds(frame, week_start)
        discovery = frame[frame.index < week_start]
        volatility_rows.append(
            {
                "instrument": pair,
                "median_abs_60_pips": float(
                    discovery["future_move_pips_60"].abs().median()
                ),
                "q995_abs_60_pips": thresholds[60]["q995"],
                "median_spread_pips": float(discovery["spread_pips"].median()),
                "q995_move_to_spread": thresholds[60]["q995"]
                / max(0.1, float(discovery["spread_pips"].median())),
            }
        )
        week = frame[(frame.index >= week_start) & (frame.index < week_end)]
        for horizon in HORIZONS:
            move = week[f"future_move_pips_{horizon}"]
            expected = week["atr240_pips"] * math.sqrt(horizon / 5)
            severity = move.abs() / max(thresholds[horizon]["q995"], 0.1)
            move_atr = move.abs() / expected.replace(0, np.nan)
            move_to_spread = move.abs() / week["spread_pips"].clip(lower=0.1)
            mask = (
                (move.abs() >= thresholds[horizon]["q995"])
                & (move_atr >= 1.5)
                & (move_to_spread >= 3.0)
            )
            for timestamp in week.index[mask]:
                row = week.loc[timestamp]
                signed_move = float(row[f"future_move_pips_{horizon}"])
                record: dict[str, Any] = {
                    "instrument": pair,
                    "group": pair_group(pair),
                    "start_utc": timestamp,
                    "end_utc": timestamp + pd.Timedelta(minutes=horizon),
                    "horizon_minutes": horizon,
                    "direction": "LONG" if signed_move > 0 else "SHORT",
                    "move_pips": signed_move,
                    "abs_move_pips": abs(signed_move),
                    "historical_q995_pips": thresholds[horizon]["q995"],
                    "severity_vs_q995": float(severity.loc[timestamp]),
                    "move_atr_units": float(move_atr.loc[timestamp]),
                    "move_to_spread": float(move_to_spread.loc[timestamp]),
                    "start_price": float(row["close"]),
                    "end_price": float(row["close"])
                    + signed_move / pip_multiplier(pair),
                    "spread_pips": float(row["spread_pips"]),
                }
                for feature in FEATURE_COLUMNS:
                    record[feature] = float(row.get(feature, np.nan))
                candidates.append(record)
    volatility = pd.DataFrame(volatility_rows)
    non_usd = volatility[~volatility["instrument"].str.contains("USD")].copy()
    non_usd["volatility_percentile"] = non_usd["q995_move_to_spread"].rank(pct=True)
    volatility = volatility.merge(
        non_usd[["instrument", "volatility_percentile"]],
        on="instrument",
        how="left",
    )
    return pd.DataFrame(candidates), volatility


def deduplicate_events(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty or "instrument" not in candidates.columns:
        return pd.DataFrame()
    selected: list[pd.Series] = []
    for pair, group in candidates.groupby("instrument"):
        accepted: list[pd.Series] = []
        for _, row in group.sort_values(
            ["severity_vs_q995", "move_atr_units", "abs_move_pips"],
            ascending=False,
        ).iterrows():
            overlaps = False
            for existing in accepted:
                overlap_start = max(row.start_utc, existing.start_utc)
                overlap_end = min(row.end_utc, existing.end_utc)
                if overlap_end > overlap_start:
                    overlap = (overlap_end - overlap_start).total_seconds()
                    shortest = min(
                        (row.end_utc - row.start_utc).total_seconds(),
                        (existing.end_utc - existing.start_utc).total_seconds(),
                    )
                    if overlap / shortest >= 0.30:
                        overlaps = True
                        break
            if not overlaps:
                accepted.append(row)
            if len(accepted) >= 8:
                break
        selected.extend(accepted)
    events = pd.DataFrame(selected)
    if events.empty:
        return events
    return events.sort_values(
        ["group", "severity_vs_q995", "abs_move_pips"],
        ascending=[True, False, False],
    ).reset_index(drop=True)


def load_trade_intervals() -> pd.DataFrame:
    if not ORDER_PATH.exists():
        return pd.DataFrame()
    orders = pd.read_csv(
        ORDER_PATH,
        usecols=lambda column: column
        in {
            "time_utc",
            "account_lane",
            "source",
            "status",
            "instrument",
            "direction",
            "trade_id",
            "units",
            "reason",
        },
    )
    orders["time_utc"] = pd.to_datetime(orders["time_utc"], errors="coerce", utc=True)
    orders["trade_id"] = orders["trade_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    entries = orders[
        orders["status"].eq("accepted")
        & orders["source"].isin(["open", "event_scout"])
        & orders["trade_id"].notna()
    ].copy()
    closes = orders[
        orders["status"].eq("accepted")
        & orders["source"].isin(["close", "partial_close", "flip_close"])
        & orders["trade_id"].notna()
    ].copy()
    records = []
    for _, entry in entries.iterrows():
        matching = closes[
            (closes["trade_id"] == entry["trade_id"])
            & (closes["time_utc"] >= entry["time_utc"])
        ]
        close_time = matching["time_utc"].min() if not matching.empty else pd.NaT
        records.append(
            {
                "account_lane": entry["account_lane"],
                "instrument": entry["instrument"],
                "direction": entry["direction"],
                "trade_id": entry["trade_id"],
                "entry_utc": entry["time_utc"],
                "exit_utc": close_time,
                "entry_reason": entry["reason"],
            }
        )
    intervals = pd.DataFrame(records)
    if not intervals.empty:
        intervals["entry_utc"] = pd.to_datetime(
            intervals["entry_utc"], errors="coerce", utc=True
        )
        intervals["exit_utc"] = pd.to_datetime(
            intervals["exit_utc"], errors="coerce", utc=True
        )
    return intervals


def load_missed_journal() -> pd.DataFrame:
    if not JOURNAL_PATH.exists():
        return pd.DataFrame()
    columns = [
        "time_utc",
        "event_type",
        "event_status",
        "instrument",
        "direction",
        "reason",
    ]
    frame = pd.read_csv(
        JOURNAL_PATH, usecols=lambda column: column in columns, low_memory=False
    )
    frame["time_utc"] = pd.to_datetime(frame["time_utc"], errors="coerce", utc=True)
    return frame[frame["event_type"].eq("movement_missed")].copy()


def classify_capture(
    events: pd.DataFrame, trades: pd.DataFrame, missed: pd.DataFrame
) -> pd.DataFrame:
    output = events.copy()
    labels = []
    details = []
    for _, event in output.iterrows():
        if event.start_utc < LIVE_START:
            labels.append("historical_opportunity_pre_runtime")
            details.append("Managers did not have comparable execution journals yet.")
            continue
        matching = trades[
            (trades["instrument"] == event.instrument)
            & (trades["direction"] == event.direction)
            & (trades["entry_utc"] <= event.end_utc)
            & (
                trades["exit_utc"].isna()
                | (trades["exit_utc"] >= event.start_utc)
            )
        ]
        if not matching.empty:
            first = matching.sort_values("entry_utc").iloc[0]
            halfway = event.start_utc + (
                event.end_utc - event.start_utc
            ) / 2
            label = (
                "captured"
                if first.entry_utc <= halfway
                else "captured_late"
            )
            labels.append(label)
            details.append(
                f"{first.account_lane} trade {first.trade_id} entered {first.entry_utc.isoformat()}"
            )
            continue
        opposite = trades[
            (trades["instrument"] == event.instrument)
            & (trades["direction"] != event.direction)
            & (trades["entry_utc"] <= event.end_utc)
            & (
                trades["exit_utc"].isna()
                | (trades["exit_utc"] >= event.start_utc)
            )
        ]
        nearby_miss = missed[
            (missed["instrument"] == event.instrument)
            & (missed["direction"] == event.direction)
            & (
                missed["time_utc"].between(
                    event.start_utc - pd.Timedelta(minutes=30),
                    event.end_utc + pd.Timedelta(minutes=30),
                )
            )
        ]
        if event.start_utc >= FRIDAY_BLOCK_START:
            labels.append("weekend_policy_window")
            details.append("Technical lane was intentionally blocking/flattening before Friday close.")
        elif not opposite.empty:
            labels.append("live_missed_wrong_way_exposure")
            details.append(
                f"Opposite-direction exposure existed: {opposite.iloc[0].trade_id}"
            )
        elif not nearby_miss.empty:
            labels.append("live_missed_logged")
            miss = nearby_miss.sort_values("time_utc").iloc[0]
            details.append(f"{miss.event_status}: {str(miss.reason)[:300]}")
        else:
            labels.append("live_missed_no_matching_trade")
            details.append("No matching trade interval or explicit missed-move record.")
    output["capture_status"] = labels
    output["capture_detail"] = details
    return output


def build_episodes(events: pd.DataFrame) -> pd.DataFrame:
    priority = events[events["group"].ne("usd_related")].sort_values("start_utc")
    episodes: list[list[pd.Series]] = []
    for _, event in priority.iterrows():
        placed = False
        for episode in episodes:
            center = min(item.start_utc for item in episode)
            if abs((event.start_utc - center).total_seconds()) <= 30 * 60:
                episode.append(event)
                placed = True
                break
        if not placed:
            episodes.append([event])
    rows = []
    for number, episode in enumerate(episodes, 1):
        rows.append(
            {
                "episode_id": number,
                "start_utc": min(item.start_utc for item in episode),
                "end_utc": max(item.end_utc for item in episode),
                "pair_count": len({item.instrument for item in episode}),
                "pairs": ",".join(sorted({item.instrument for item in episode})),
                "groups": ",".join(sorted({item.group for item in episode})),
                "maximum_severity": max(item.severity_vs_q995 for item in episode),
                "maximum_atr_units": max(item.move_atr_units for item in episode),
                "captured_count": sum(
                    str(item.capture_status).startswith("captured")
                    for item in episode
                ),
                "live_missed_count": sum(
                    str(item.capture_status).startswith("live_missed")
                    for item in episode
                ),
                "historical_count": sum(
                    str(item.capture_status).startswith("historical")
                    for item in episode
                ),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["pair_count", "maximum_severity"], ascending=False
    )


def condition_dataset(
    paths: dict[str, Path],
    week_start: pd.Timestamp,
    week_end: pd.Timestamp,
    horizon: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    discovery_rows = []
    validation_rows = []
    for pair, path in paths.items():
        if pair_group(pair) == "usd_related":
            continue
        frame = pd.read_parquet(path)
        frame.index = pd.to_datetime(frame.index, utc=True)
        threshold = float(
            frame.loc[frame.index < week_start, f"future_move_pips_{horizon}"]
            .abs()
            .quantile(0.99)
        )
        keep = FEATURE_COLUMNS + [f"future_move_pips_{horizon}"]
        discovery = frame.loc[frame.index < week_start, keep].dropna()
        validation = frame.loc[
            (frame.index >= week_start) & (frame.index < week_end), keep
        ].dropna()
        for sample, destination in [
            (discovery, discovery_rows),
            (validation, validation_rows),
        ]:
            sample = sample.copy()
            sample["instrument"] = pair
            sample["group"] = pair_group(pair)
            sample["threshold"] = threshold
            sample["spike"] = (
                sample[f"future_move_pips_{horizon}"].abs() >= threshold
            )
            sample["target_direction"] = np.sign(
                sample[f"future_move_pips_{horizon}"]
            )
            destination.append(sample.reset_index(names="time_utc"))
    discovery = pd.concat(discovery_rows, ignore_index=True)
    validation = pd.concat(validation_rows, ignore_index=True)
    target = f"future_move_pips_{horizon}"
    for sample, prefix in [(discovery, "train"), (validation, "week")]:
        sample["spike_cluster_id"] = ""
        spike_rows = sample[sample["spike"]].sort_values(
            ["instrument", "time_utc"]
        )
        for pair, group in spike_rows.groupby("instrument"):
            cluster_number = 0
            prior_time: pd.Timestamp | None = None
            for index, row in group.iterrows():
                if (
                    prior_time is None
                    or row["time_utc"] - prior_time
                    > pd.Timedelta(minutes=horizon)
                ):
                    cluster_number += 1
                sample.at[
                    index, "spike_cluster_id"
                ] = f"{prefix}:{pair}:{cluster_number}"
                prior_time = row["time_utc"]
        sample["abs_target"] = sample[target].abs()
    return discovery, validation


def independent_spikes(
    sample: pd.DataFrame, mask: pd.Series
) -> pd.DataFrame:
    spikes = sample.loc[mask & sample["spike"]].copy()
    if spikes.empty:
        return spikes
    return (
        spikes.sort_values("abs_target", ascending=False)
        .drop_duplicates("spike_cluster_id", keep="first")
        .sort_values("time_utc")
    )


def episode_count(spikes: pd.DataFrame, gap_minutes: int = 30) -> int:
    if spikes.empty:
        return 0
    times = sorted(pd.to_datetime(spikes["time_utc"], utc=True).tolist())
    count = 1
    anchor = times[0]
    for timestamp in times[1:]:
        if timestamp - anchor > pd.Timedelta(minutes=gap_minutes):
            count += 1
            anchor = timestamp
    return count


def discover_conditions(
    paths: dict[str, Path], week_start: pd.Timestamp, week_end: pd.Timestamp
) -> pd.DataFrame:
    condition_rows: list[dict[str, Any]] = []
    for horizon in CONDITION_HORIZONS:
        print(f"[conditions] horizon={horizon}", flush=True)
        discovery, validation = condition_dataset(
            paths, week_start, week_end, horizon
        )
        base_train_rate = float(discovery["spike"].mean())
        base_validation_rate = float(validation["spike"].mean())
        for feature in FEATURE_COLUMNS:
            values = discovery[feature].replace([np.inf, -np.inf], np.nan).dropna()
            edges = np.unique(
                values.quantile(
                    [0.0, 0.10, 0.25, 0.50, 0.75, 0.90, 1.0]
                ).to_numpy()
            )
            if len(edges) < 3:
                continue
            for lower, upper in zip(edges[:-1], edges[1:]):
                train_mask = discovery[feature].ge(lower) & discovery[feature].le(upper)
                valid_mask = validation[feature].ge(lower) & validation[feature].le(upper)
                train_rows = int(train_mask.sum())
                valid_rows = int(valid_mask.sum())
                if train_rows < 500 or valid_rows < 25:
                    continue
                train_spikes = independent_spikes(discovery, train_mask)
                valid_spikes = independent_spikes(validation, valid_mask)
                train_rate = float(discovery.loc[train_mask, "spike"].mean())
                valid_rate = float(validation.loc[valid_mask, "spike"].mean())
                if len(train_spikes):
                    dominant_direction = int(
                        np.sign(train_spikes["target_direction"].mean())
                    )
                    train_direction_accuracy = float(
                        (
                            train_spikes["target_direction"]
                            == dominant_direction
                        ).mean()
                    )
                else:
                    dominant_direction = 0
                    train_direction_accuracy = 0.0
                valid_direction_accuracy = (
                    float(
                        (
                            valid_spikes["target_direction"]
                            == dominant_direction
                        ).mean()
                    )
                    if len(valid_spikes) and dominant_direction
                    else 0.0
                )
                condition_rows.append(
                    {
                        "horizon_minutes": horizon,
                        "feature": feature,
                        "lower": float(lower),
                        "upper": float(upper),
                        "train_rows": train_rows,
                        "train_spikes": len(train_spikes),
                        "train_spike_rate": train_rate,
                        "train_lift": train_rate / max(base_train_rate, 1e-12),
                        "validation_rows": valid_rows,
                        "validation_spikes": len(valid_spikes),
                        "validation_spike_pairs": int(
                            valid_spikes["instrument"].nunique()
                        ),
                        "validation_spike_episodes": episode_count(valid_spikes),
                        "validation_spike_rate": valid_rate,
                        "validation_lift": valid_rate
                        / max(base_validation_rate, 1e-12),
                        "dominant_direction": (
                            "LONG"
                            if dominant_direction > 0
                            else "SHORT"
                            if dominant_direction < 0
                            else "NONE"
                        ),
                        "train_direction_accuracy": train_direction_accuracy,
                        "validation_direction_accuracy": valid_direction_accuracy,
                    }
                )
    conditions = pd.DataFrame(condition_rows)
    if conditions.empty:
        return conditions
    conditions["validated"] = (
        (conditions["train_spikes"] >= 20)
        & (conditions["validation_spikes"] >= 5)
        & (conditions["validation_spike_pairs"] >= 3)
        & (conditions["validation_spike_episodes"] >= 2)
        & (conditions["train_lift"] >= 1.25)
        & (conditions["validation_lift"] >= 1.10)
        & (conditions["train_direction_accuracy"] >= 0.55)
        & (conditions["validation_direction_accuracy"] >= 0.50)
    )
    return conditions.sort_values(
        ["validated", "validation_lift", "train_lift", "validation_spikes"],
        ascending=[False, False, False, False],
    )


def discover_conjunctions(
    paths: dict[str, Path], week_start: pd.Timestamp, week_end: pd.Timestamp
) -> pd.DataFrame:
    directional_features = [
        "momentum_5_atr",
        "momentum_15_atr",
        "momentum_30_atr",
        "strength_gap_15",
        "range_position_240_centered",
    ]
    rows: list[dict[str, Any]] = []
    for horizon in CONDITION_HORIZONS:
        discovery, validation = condition_dataset(
            paths, week_start, week_end, horizon
        )
        base_train_rate = float(discovery["spike"].mean())
        base_validation_rate = float(validation["spike"].mean())
        bounds = {
            feature: (
                float(discovery[feature].quantile(0.10)),
                float(discovery[feature].quantile(0.90)),
            )
            for feature in directional_features
        }
        for left, right in itertools.combinations(directional_features, 2):
            for tail in ["lower", "upper"]:
                if tail == "upper":
                    train_mask = (discovery[left] >= bounds[left][1]) & (
                        discovery[right] >= bounds[right][1]
                    )
                    valid_mask = (validation[left] >= bounds[left][1]) & (
                        validation[right] >= bounds[right][1]
                    )
                    left_bound, right_bound = bounds[left][1], bounds[right][1]
                    operator = ">="
                else:
                    train_mask = (discovery[left] <= bounds[left][0]) & (
                        discovery[right] <= bounds[right][0]
                    )
                    valid_mask = (validation[left] <= bounds[left][0]) & (
                        validation[right] <= bounds[right][0]
                    )
                    left_bound, right_bound = bounds[left][0], bounds[right][0]
                    operator = "<="
                if train_mask.sum() < 500 or valid_mask.sum() < 25:
                    continue
                train_spikes = independent_spikes(discovery, train_mask)
                valid_spikes = independent_spikes(validation, valid_mask)
                if train_spikes.empty:
                    continue
                direction = int(np.sign(train_spikes["target_direction"].mean()))
                train_direction_accuracy = float(
                    (train_spikes["target_direction"] == direction).mean()
                )
                validation_direction_accuracy = (
                    float(
                        (
                            valid_spikes["target_direction"]
                            == direction
                        ).mean()
                    )
                    if not valid_spikes.empty and direction
                    else 0.0
                )
                train_rate = float(discovery.loc[train_mask, "spike"].mean())
                validation_rate = float(
                    validation.loc[valid_mask, "spike"].mean()
                )
                rows.append(
                    {
                        "horizon_minutes": horizon,
                        "rule": (
                            f"{left} {operator} {left_bound:.6g} AND "
                            f"{right} {operator} {right_bound:.6g}"
                        ),
                        "tail": tail,
                        "train_rows": int(train_mask.sum()),
                        "train_spikes": len(train_spikes),
                        "train_lift": train_rate
                        / max(base_train_rate, 1e-12),
                        "validation_rows": int(valid_mask.sum()),
                        "validation_spikes": len(valid_spikes),
                        "validation_spike_pairs": int(
                            valid_spikes["instrument"].nunique()
                        ),
                        "validation_spike_episodes": episode_count(
                            valid_spikes
                        ),
                        "validation_lift": validation_rate
                        / max(base_validation_rate, 1e-12),
                        "dominant_direction": (
                            "LONG"
                            if direction > 0
                            else "SHORT"
                            if direction < 0
                            else "NONE"
                        ),
                        "train_direction_accuracy": train_direction_accuracy,
                        "validation_direction_accuracy": validation_direction_accuracy,
                    }
                )
    conjunctions = pd.DataFrame(rows)
    if conjunctions.empty:
        return conjunctions
    conjunctions["validated"] = (
        (conjunctions["train_spikes"] >= 20)
        & (conjunctions["validation_spikes"] >= 5)
        & (conjunctions["validation_spike_pairs"] >= 3)
        & (conjunctions["validation_spike_episodes"] >= 2)
        & (conjunctions["train_lift"] >= 1.25)
        & (conjunctions["validation_lift"] >= 1.10)
        & (conjunctions["train_direction_accuracy"] >= 0.60)
        & (conjunctions["validation_direction_accuracy"] >= 0.65)
    )
    return conjunctions.sort_values(
        [
            "validated",
            "validation_lift",
            "train_lift",
            "validation_direction_accuracy",
        ],
        ascending=[False, False, False, False],
    )


def event_feature_ranges(events: pd.DataFrame) -> pd.DataFrame:
    priority = events[events["group"].ne("usd_related")]
    rows = []
    for feature in FEATURE_COLUMNS:
        values = pd.to_numeric(priority[feature], errors="coerce").dropna()
        if values.empty:
            continue
        rows.append(
            {
                "feature": feature,
                "event_count": len(values),
                "minimum": values.min(),
                "q10": values.quantile(0.10),
                "q25": values.quantile(0.25),
                "median": values.median(),
                "q75": values.quantile(0.75),
                "q90": values.quantile(0.90),
                "maximum": values.max(),
            }
        )
    return pd.DataFrame(rows)


def markdown_report(
    events: pd.DataFrame,
    episodes: pd.DataFrame,
    conditions: pd.DataFrame,
    conjunctions: pd.DataFrame,
    volatility: pd.DataFrame,
    week_start: pd.Timestamp,
    week_end: pd.Timestamp,
) -> str:
    priority = events[events["group"].ne("usd_related")].copy()
    live = priority[priority["start_utc"] >= LIVE_START]
    if "validated" not in conditions:
        conditions = conditions.copy()
        conditions["validated"] = False
    if "validated" not in conjunctions:
        conjunctions = conjunctions.copy()
        conjunctions["validated"] = False
    validated = conditions[conditions["validated"]].head(15)
    validated_conjunctions = conjunctions[conjunctions["validated"]].head(12)
    volatile = volatility[
        (~volatility["instrument"].str.contains("USD"))
        & (volatility["volatility_percentile"] >= 0.75)
    ].sort_values("volatility_percentile", ascending=False)
    lines = [
        "# All-68 Weekly Missed-Move Study",
        "",
        f"Week: `{week_start.isoformat()}` to `{week_end.isoformat()}`",
        "",
        "## Scope",
        f"- Instruments analyzed: `68`",
        f"- Priority non-USD events: `{len(priority)}`",
        f"- Live-window priority events: `{len(live)}`",
        f"- Captured live events: `{int(live.capture_status.str.startswith('captured').sum())}`",
        f"- Live misses: `{int(live.capture_status.str.startswith('live_missed').sum())}`",
        f"- Pre-runtime historical opportunities: `{int(priority.capture_status.str.startswith('historical').sum())}`",
        "",
        "## Largest Priority Events",
        "",
        "| Start UTC | Pair | Group | Direction | Horizon | Move pips | Q99.5 multiple | ATR units | Status |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for _, row in priority.sort_values(
        ["severity_vs_q995", "move_atr_units"], ascending=False
    ).head(30).iterrows():
        lines.append(
            f"| {row.start_utc.isoformat()} | {row.instrument} | {row.group} | "
            f"{row.direction} | {int(row.horizon_minutes)}m | {row.move_pips:.1f} | "
            f"{row.severity_vs_q995:.2f} | {row.move_atr_units:.2f} | {row.capture_status} |"
        )
    lines += [
        "",
        "## Cross-Pair Episodes",
        "",
        "| Start UTC | Pairs | Count | Max Q99.5 multiple | Live misses |",
        "|---|---|---:|---:|---:|",
    ]
    for _, row in episodes.head(20).iterrows():
        lines.append(
            f"| {row.start_utc.isoformat()} | {row.pairs} | {int(row.pair_count)} | "
            f"{row.maximum_severity:.2f} | {int(row.live_missed_count)} |"
        )
    lines += [
        "",
        "## Validated Candidate Feature Ranges",
        "",
        "These ranges were discovered before June 15 and then checked on June 15–19.",
        "",
        "| Horizon | Feature range | Train lift | Week lift | Independent spikes | Pairs | Episodes | Direction | Week direction accuracy |",
        "|---:|---|---:|---:|---:|---:|---:|---|---:|",
    ]
    if validated.empty:
        lines.append("| - | No range met the validation requirements | - | - | - | - | - | - | - |")
    else:
        for _, row in validated.iterrows():
            lines.append(
                f"| {int(row.horizon_minutes)}m | {row.feature} in [{row.lower:.3f}, {row.upper:.3f}] | "
                f"{row.train_lift:.2f} | {row.validation_lift:.2f} | {int(row.validation_spikes)} | "
                f"{int(row.validation_spike_pairs)} | {int(row.validation_spike_episodes)} | "
                f"{row.dominant_direction} | {row.validation_direction_accuracy:.1%} |"
            )
    lines += [
        "",
        "## Validated Multi-Feature Conditions",
        "",
        "| Horizon | Rule | Train lift | Week lift | Spikes | Pairs | Episodes | Direction | Train/Week direction |",
        "|---:|---|---:|---:|---:|---:|---:|---|---:|",
    ]
    if validated_conjunctions.empty:
        lines.append("| - | No conjunction passed | - | - | - | - | - | - | - |")
    else:
        for _, row in validated_conjunctions.iterrows():
            lines.append(
                f"| {int(row.horizon_minutes)}m | {row.rule} | "
                f"{row.train_lift:.2f} | {row.validation_lift:.2f} | "
                f"{int(row.validation_spikes)} | {int(row.validation_spike_pairs)} | "
                f"{int(row.validation_spike_episodes)} | {row.dominant_direction} | "
                f"{row.train_direction_accuracy:.1%}/{row.validation_direction_accuracy:.1%} |"
            )
    lines += [
        "",
        "## Highest-Volatility Non-USD Instruments",
        "",
        "| Pair | Group | Volatility percentile | Q99.5 60m pips | Move/spread |",
        "|---|---|---:|---:|---:|",
    ]
    for _, row in volatile.head(25).iterrows():
        lines.append(
            f"| {row.instrument} | {pair_group(row.instrument)} | "
            f"{row.volatility_percentile:.1%} | {row.q995_abs_60_pips:.1f} | "
            f"{row.q995_move_to_spread:.1f} |"
        )
    lines += [
        "",
        "## Interpretation",
        "",
        "- A validated range is a research candidate, not a deployment rule.",
        "- Pair-specific historical percentiles normalize exotic-pair pip scales.",
        "- Events before the live journals began are not labeled as operational misses.",
        "- USD-related pairs remain in the complete CSV outputs but are excluded from the priority tables.",
    ]
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--week-start", default="2026-06-15")
    parser.add_argument("--week-end", default="2026-06-20")
    parser.add_argument("--reuse-features", action="store_true")
    parser.add_argument(
        "--feature-timeframe",
        default="5min",
        help="Feature bar base, e.g. M5, M30, H1, H4.",
    )
    parser.add_argument("--feature-root", type=Path, default=None)
    parser.add_argument("--report-root", type=Path, default=None)
    parser.add_argument(
        "--build-features-only",
        action="store_true",
        help="Only build/reuse per-pair feature parquets and write data quality.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    week_start = pd.Timestamp(args.week_start, tz="UTC")
    week_end = pd.Timestamp(args.week_end, tz="UTC")
    feature_timeframe = normalize_feature_timeframe(args.feature_timeframe)
    feature_root = args.feature_root or feature_root_for_timeframe(feature_timeframe)
    report_root = args.report_root or report_root_for_timeframe(feature_timeframe)
    report_root.mkdir(parents=True, exist_ok=True)
    existing = sorted(feature_root.glob("*.parquet"))
    if args.reuse_features and len(existing) == 68:
        paths = {path.stem: path for path in existing}
        quality = pd.DataFrame(
            [
                {
                    "instrument": pair,
                    "group": pair_group(pair),
                    "feature_timeframe": feature_timeframe,
                    "base_minutes": timeframe_minutes(feature_timeframe),
                }
                for pair in paths
            ]
        )
    else:
        paths, quality = build_all_features(
            feature_root=feature_root,
            feature_timeframe=feature_timeframe,
        )
    if args.build_features_only:
        quality.to_csv(report_root / "data_quality.csv", index=False)
        summary = {
            "pipeline_version": PIPELINE_VERSION,
            "feature_timeframe": feature_timeframe,
            "feature_root": feature_root,
            "report_root": report_root,
            "instruments": len(paths),
            "feature_files": len(paths),
            "build_features_only": True,
        }
        (report_root / "latest_summary.json").write_text(
            json.dumps(summary, indent=2, default=str),
            encoding="utf-8",
        )
        print(json.dumps(summary, indent=2, default=str), flush=True)
        return 0
    candidates, volatility = candidate_events(paths, week_start, week_end)
    events = deduplicate_events(candidates)
    trades = load_trade_intervals()
    missed = load_missed_journal()
    events = classify_capture(events, trades, missed)
    events = events.merge(
        volatility[["instrument", "volatility_percentile"]],
        on="instrument",
        how="left",
    )
    episodes = build_episodes(events)
    conditions = discover_conditions(paths, week_start, week_end)
    conjunctions = discover_conjunctions(paths, week_start, week_end)
    ranges = event_feature_ranges(events)

    quality.to_csv(report_root / "data_quality.csv", index=False)
    volatility.to_csv(report_root / "instrument_volatility.csv", index=False)
    candidates.to_csv(report_root / "all_candidate_events.csv", index=False)
    events.to_csv(report_root / "clustered_weekly_events.csv", index=False)
    events[events["group"].ne("usd_related")].to_csv(
        report_root / "priority_non_usd_events.csv", index=False
    )
    episodes.to_csv(report_root / "cross_pair_episodes.csv", index=False)
    conditions.to_csv(report_root / "validated_feature_conditions.csv", index=False)
    conjunctions.to_csv(
        report_root / "validated_conjunction_conditions.csv", index=False
    )
    ranges.to_csv(report_root / "priority_event_feature_ranges.csv", index=False)
    report = markdown_report(
        events,
        episodes,
        conditions,
        conjunctions,
        volatility,
        week_start,
        week_end,
    )
    (report_root / "latest_report.md").write_text(report, encoding="utf-8")
    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "feature_timeframe": feature_timeframe,
        "feature_root": feature_root,
        "report_root": report_root,
        "week_start": week_start,
        "week_end": week_end,
        "instruments": len(paths),
        "candidate_events": len(candidates),
        "clustered_events": len(events),
        "priority_non_usd_events": int(events["group"].ne("usd_related").sum()),
        "live_priority_events": int(
            (
                events["group"].ne("usd_related")
                & (events["start_utc"] >= LIVE_START)
            ).sum()
        ),
        "captured_live_priority_events": int(
            (
                events["group"].ne("usd_related")
                & (events["start_utc"] >= LIVE_START)
                & events["capture_status"].str.startswith("captured")
            ).sum()
        ),
        "validated_conditions": int(conditions["validated"].sum()),
        "validated_conjunctions": int(conjunctions["validated"].sum()),
        "report": report_root / "latest_report.md",
    }
    (report_root / "latest_summary.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
