from __future__ import annotations

import hashlib
import itertools
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from .common import ROOT, fallback_spread_pips, pair_meta, pip_size_for, tier_for, write_json
from .replay_metrics import rank_ic, write_report_pair


TIMEFRAME_MINUTES = {"m30": 30, "h1": 60, "h4": 240}
TIMEFRAME_SETS = [
    ("m30",),
    ("h1",),
    ("h4",),
    ("m30", "h1"),
    ("m30", "h4"),
    ("h1", "h4"),
    ("m30", "h1", "h4"),
]
MODEL_SCORE_COLUMNS = ("probability", "rank_score")
SCORE_QUANTILES = (0.50, 0.75, 0.90, 0.95, 0.98, 0.99)
NEW_TRADE_QUOTAS = (1, 2, 3)
MAX_OPEN_POSITIONS = 3
INITIAL_EQUITY = 1000.0
DEFAULT_FIXED_UNITS = 10_000
DEFAULT_DEVELOPMENT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "fresh_fullhist_m30_h1_h4_2025_validation_maxnew8_20260707"
)
DEFAULT_EVALUATION_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "fresh_rebuild_m30_h1_h4_fullhist_20260707"
)
EXPERIMENT_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "continuous_research"
    / "experiments"
    / "exp_20260702_022306_e09b53ec8d.json"
)


def _utc(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _exclusive_end(value: str) -> pd.Timestamp:
    ts = _utc(value)
    if len(str(value).strip()) == 10:
        ts += pd.Timedelta(days=1)
    return ts


def _timestamp_ns(values: Any) -> np.ndarray:
    index = pd.DatetimeIndex(pd.to_datetime(values, utc=True, errors="coerce"))
    if hasattr(index, "as_unit"):
        index = index.as_unit("ns")
    else:
        index = index.astype("datetime64[ns]")
    return index.asi8


def decision_time_from_bar_start(value: Any, timeframe: str) -> pd.Timestamp:
    return _utc(value) + pd.Timedelta(minutes=TIMEFRAME_MINUTES[timeframe])


def curve_path_window_minutes(horizon_minutes: int, timeframe: str) -> int:
    # The archived label builder hard-coded horizon / 5 rows.
    rows = max(1, int(math.ceil(float(horizon_minutes) / 5.0)))
    return rows * TIMEFRAME_MINUTES[timeframe]


def _candidate_columns() -> list[str]:
    return [
        "time_utc",
        "instrument",
        "direction",
        "horizon_minutes",
        "fold",
        "week_start",
        "experiment_id",
        "model_type",
        "feature_set",
        "target",
        "outcome",
        "threshold",
        "probability",
        "expected_move_atr",
        "realized_outcome_atr",
        "rank_score",
        "score_percentile",
        "momentum_30_atr",
        "spread_pips",
        "atr240_pips",
    ]


def _load_candidate_root(
    report_root: Path,
    cfg: dict[str, Any],
    tier: str,
    period_name: str,
    *,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    allowed = {
        pair
        for pair in cfg.get("tier1_pairs", []) + cfg.get("tier2_pairs", [])
        if tier == "all"
        or (tier == "tier1" and tier_for(pair, cfg) == "tier1")
        or (tier == "tier2" and tier_for(pair, cfg) in {"tier1", "tier2"})
    }
    frames: list[pd.DataFrame] = []
    coverage: dict[str, Any] = {}
    for timeframe, minutes in TIMEFRAME_MINUTES.items():
        stream_root = report_root / "streams" / timeframe
        path = stream_root / "candidate_rows.csv"
        if not path.exists():
            raise FileNotFoundError(path)
        frame = pd.read_csv(path, usecols=lambda col: col in set(_candidate_columns()))
        frame = frame[frame["instrument"].astype(str).isin(allowed)].copy()
        frame["raw_bar_time_utc"] = pd.to_datetime(frame["time_utc"], utc=True, errors="coerce")
        frame["decision_time_utc"] = frame["raw_bar_time_utc"] + pd.Timedelta(minutes=minutes)
        frame["intended_exit_time_utc"] = frame["decision_time_utc"] + pd.to_timedelta(
            pd.to_numeric(frame.get("horizon_minutes", 120), errors="coerce").fillna(120),
            unit="min",
        )
        frame["timeframe"] = timeframe
        frame["source_period"] = period_name
        frame["candidate_uid"] = (
            timeframe
            + "|"
            + frame["instrument"].astype(str)
            + "|"
            + frame["raw_bar_time_utc"].astype(str)
        )
        if start is not None:
            frame = frame[frame["decision_time_utc"] >= start]
        if end is not None:
            frame = frame[frame["decision_time_utc"] < end]
        frame = frame.dropna(subset=["raw_bar_time_utc", "decision_time_utc"])

        predicted_rows = None
        threshold_path = stream_root / "pair_thresholds.csv"
        if threshold_path.exists():
            threshold_rows = pd.read_csv(
                threshold_path,
                usecols=lambda col: col in {"instrument", "status", "test_rows"},
            )
            threshold_rows = threshold_rows[threshold_rows["instrument"].astype(str).isin(allowed)]
            threshold_rows = threshold_rows[threshold_rows["status"].astype(str) == "selected"]
            predicted_rows = int(
                pd.to_numeric(threshold_rows.get("test_rows", 0), errors="coerce").fillna(0).sum()
            )
        coverage[timeframe] = {
            "archived_candidates": int(len(frame)),
            "candidate_start": frame["decision_time_utc"].min(),
            "candidate_end": frame["decision_time_utc"].max(),
            "pair_fold_test_rows_with_predictions": predicted_rows,
            "archive_is_thresholded": True,
        }
        frames.append(frame)
    combined = pd.concat(frames, ignore_index=True, copy=False)
    for column in [
        "probability",
        "rank_score",
        "momentum_30_atr",
        "spread_pips",
        "atr240_pips",
        "horizon_minutes",
    ]:
        combined[column] = pd.to_numeric(combined.get(column), errors="coerce")
    combined["horizon_minutes"] = combined["horizon_minutes"].fillna(120).astype(int)
    return combined.sort_values(["decision_time_utc", "rank_score"], ascending=[True, False]), coverage


class _OpenPriceCache:
    def __init__(self, candle_root: Path, start: pd.Timestamp, end: pd.Timestamp) -> None:
        self.candle_root = candle_root
        self.start = start - pd.Timedelta(minutes=5)
        self.end = end + pd.Timedelta(minutes=5)
        self._cache: dict[str, tuple[np.ndarray, np.ndarray]] = {}

    def load(self, instrument: str) -> tuple[np.ndarray, np.ndarray]:
        cached = self._cache.get(instrument)
        if cached is not None:
            return cached
        path = self.candle_root / f"{instrument}_M1.csv"
        if not path.exists():
            result = (np.asarray([], dtype="int64"), np.asarray([], dtype=float))
            self._cache[instrument] = result
            return result
        frame = pd.read_csv(path, usecols=lambda col: col in {"datetime", "open", "close"})
        frame["datetime"] = pd.to_datetime(frame["datetime"], utc=True, errors="coerce")
        frame = frame[(frame["datetime"] >= self.start) & (frame["datetime"] <= self.end)].copy()
        price = pd.to_numeric(frame.get("open"), errors="coerce")
        if "close" in frame:
            price = price.fillna(pd.to_numeric(frame["close"], errors="coerce"))
        frame["price"] = price
        frame = frame.dropna(subset=["datetime", "price"]).sort_values("datetime")
        frame = frame.drop_duplicates("datetime", keep="last")
        result = (
            _timestamp_ns(frame["datetime"]),
            frame["price"].to_numpy(dtype=float),
        )
        self._cache[instrument] = result
        return result

    def aligned(self, instrument: str, timestamps: pd.Series) -> np.ndarray:
        times, prices = self.load(instrument)
        result = np.full(len(timestamps), np.nan, dtype=float)
        if not len(times):
            return result
        targets = _timestamp_ns(timestamps)
        positions = np.searchsorted(times, targets, side="left")
        valid = positions < len(times)
        result[valid] = prices[positions[valid]]
        return result


def _quote_to_usd_rates(
    instrument: str,
    exit_times: pd.Series,
    exit_mid: np.ndarray,
    cache: _OpenPriceCache,
) -> np.ndarray:
    base, quote = instrument.split("_", 1)
    if quote == "USD":
        return np.ones(len(exit_times), dtype=float)
    if base == "USD":
        return np.divide(1.0, exit_mid, out=np.full(len(exit_mid), np.nan), where=exit_mid > 0)
    direct = f"{quote}_USD"
    inverse = f"USD_{quote}"
    if (cache.candle_root / f"{direct}_M1.csv").exists():
        return cache.aligned(direct, exit_times)
    inverse_price = cache.aligned(inverse, exit_times)
    return np.divide(
        1.0,
        inverse_price,
        out=np.full(len(inverse_price), np.nan),
        where=inverse_price > 0,
    )


def _aligned_positions(times: np.ndarray, targets: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    positions = np.searchsorted(times, targets, side="left")
    valid = positions < len(times)
    safe_positions = np.minimum(positions, max(0, len(times) - 1))
    delays = np.full(len(targets), np.iinfo(np.int64).max, dtype=np.int64)
    if len(times):
        delays[valid] = times[safe_positions[valid]] - targets[valid]
    valid &= delays <= pd.Timedelta(minutes=10).value
    return safe_positions, valid


def _score_pair_executions(
    frame: pd.DataFrame,
    instrument: str,
    cfg: dict[str, Any],
    fixed_units: int,
    cache: _OpenPriceCache,
) -> pd.DataFrame:
    path = cache.candle_root / f"{instrument}_M1.csv"
    if not path.exists():
        return pd.DataFrame({"_row_id": frame["_row_id"], "valid_execution": False})
    columns = {
        "datetime",
        "open",
        "close",
        "bid_open",
        "ask_open",
        "spread_pips",
    }
    candles = pd.read_csv(path, usecols=lambda col: col in columns)
    candles["datetime"] = pd.to_datetime(candles["datetime"], utc=True, errors="coerce")
    candles = candles[
        (candles["datetime"] >= cache.start) & (candles["datetime"] <= cache.end)
    ].copy()
    mid = pd.to_numeric(candles.get("open"), errors="coerce")
    if "close" in candles:
        mid = mid.fillna(pd.to_numeric(candles["close"], errors="coerce"))
    candles["mid_open"] = mid
    for column in ["bid_open", "ask_open", "spread_pips"]:
        candles[column] = pd.to_numeric(candles.get(column), errors="coerce")
    candles = candles.dropna(subset=["datetime", "mid_open"]).sort_values("datetime")
    candles = candles.drop_duplicates("datetime", keep="last")
    if candles.empty:
        return pd.DataFrame({"_row_id": frame["_row_id"], "valid_execution": False})
    times = _timestamp_ns(candles["datetime"])
    mids = candles["mid_open"].to_numpy(dtype=float)
    bid_open = candles["bid_open"].to_numpy(dtype=float)
    ask_open = candles["ask_open"].to_numpy(dtype=float)
    observed_spread = candles["spread_pips"].to_numpy(dtype=float)

    decision_ns = _timestamp_ns(frame["decision_time_utc"])
    entry_pos, entry_valid = _aligned_positions(times, decision_ns)
    entry_time_ns = times[entry_pos]
    horizons_ns = pd.to_numeric(frame["horizon_minutes"], errors="coerce").fillna(120).to_numpy(dtype="int64")
    exit_targets = entry_time_ns + horizons_ns * pd.Timedelta(minutes=1).value
    exit_pos, exit_valid = _aligned_positions(times, exit_targets)
    valid = entry_valid & exit_valid

    pip = pip_size_for(instrument)
    fallback = fallback_spread_pips(pair_meta(instrument, cfg), cfg)
    candidate_spread = pd.to_numeric(frame["spread_pips"], errors="coerce").fillna(fallback).to_numpy(dtype=float)
    entry_observed = observed_spread[entry_pos]
    exit_observed = observed_spread[exit_pos]
    entry_spread = np.fmax(candidate_spread, np.where(np.isfinite(entry_observed), entry_observed, 0.0))
    exit_spread = np.fmax(candidate_spread, np.where(np.isfinite(exit_observed), exit_observed, 0.0))

    entry_mid = mids[entry_pos]
    exit_mid = mids[exit_pos]
    entry_bid = np.where(np.isfinite(bid_open[entry_pos]) & (bid_open[entry_pos] > 0), bid_open[entry_pos], entry_mid - entry_spread * pip / 2.0)
    entry_ask = np.where(np.isfinite(ask_open[entry_pos]) & (ask_open[entry_pos] > 0), ask_open[entry_pos], entry_mid + entry_spread * pip / 2.0)
    exit_bid = np.where(np.isfinite(bid_open[exit_pos]) & (bid_open[exit_pos] > 0), bid_open[exit_pos], exit_mid - exit_spread * pip / 2.0)
    exit_ask = np.where(np.isfinite(ask_open[exit_pos]) & (ask_open[exit_pos] > 0), ask_open[exit_pos], exit_mid + exit_spread * pip / 2.0)

    gross_long = (exit_mid - entry_mid) / pip
    gross_short = -gross_long
    executable_long = (exit_bid - entry_ask) / pip
    executable_short = (entry_bid - exit_ask) / pip
    spread_long = np.maximum(0.0, gross_long - executable_long)
    spread_short = np.maximum(0.0, gross_short - executable_short)
    target_long = frame["direction"].astype(str).str.upper().eq("LONG").to_numpy()
    gross_target = np.where(target_long, gross_long, gross_short)
    spread_target = np.where(target_long, spread_long, spread_short)
    gross_inverted = np.where(target_long, gross_short, gross_long)
    spread_inverted = np.where(target_long, spread_short, spread_long)

    tier = tier_for(instrument, cfg)
    slippage = float(cfg["costs"][f"slippage_pips_{tier}"])
    net_target = gross_target - spread_target - slippage
    net_inverted = gross_inverted - spread_inverted - slippage
    exit_times = pd.to_datetime(exit_time_ns := times[exit_pos], unit="ns", utc=True)
    rates = _quote_to_usd_rates(instrument, pd.Series(exit_times), exit_mid, cache)
    rates = np.where(np.isfinite(rates) & (rates > 0), rates, np.nan)
    pip_value = pip * rates

    result = pd.DataFrame({
        "_row_id": frame["_row_id"].to_numpy(),
        "valid_execution": valid & np.isfinite(pip_value),
        "entry_time_utc": pd.to_datetime(entry_time_ns, unit="ns", utc=True),
        "exit_time_utc": pd.to_datetime(exit_time_ns, unit="ns", utc=True),
        "entry_delay_minutes": (entry_time_ns - decision_ns) / pd.Timedelta(minutes=1).value,
        "entry_mid": entry_mid,
        "exit_mid": exit_mid,
        "pip_value_usd_per_unit": pip_value,
        "fixed_units": int(fixed_units),
        "slippage_pips_round_trip": slippage,
        "gross_target_pips": gross_target,
        "spread_drag_target_pips": spread_target,
        "net_target_pips": net_target,
        "pnl_target_usd": net_target * pip_value * fixed_units,
        "gross_inverted_pips": gross_inverted,
        "spread_drag_inverted_pips": spread_inverted,
        "net_inverted_pips": net_inverted,
        "pnl_inverted_usd": net_inverted * pip_value * fixed_units,
        "absolute_move_pips": np.abs(gross_long),
    })
    numeric = [column for column in result.columns if column not in {"_row_id", "valid_execution", "entry_time_utc", "exit_time_utc"}]
    result.loc[~result["valid_execution"], numeric] = np.nan
    return result


def score_executable_outcomes(
    frame: pd.DataFrame,
    cfg: dict[str, Any],
    fixed_units: int = DEFAULT_FIXED_UNITS,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    work = frame.reset_index(drop=True).copy()
    work["_row_id"] = np.arange(len(work), dtype=int)
    candle_root = ROOT / cfg["candles_dir"]
    start = work["decision_time_utc"].min()
    end = work["intended_exit_time_utc"].max()
    cache = _OpenPriceCache(candle_root, start, end)
    scored = []
    for instrument, pair_frame in work.groupby("instrument", sort=True):
        scored.append(_score_pair_executions(pair_frame, str(instrument), cfg, fixed_units, cache))
    outcomes = pd.concat(scored, ignore_index=True, copy=False) if scored else pd.DataFrame()
    work = work.merge(outcomes, on="_row_id", how="left")
    valid = work.get("valid_execution", pd.Series(False, index=work.index)).fillna(False)
    audit = {
        "candidate_rows": int(len(work)),
        "valid_executable_rows": int(valid.sum()),
        "invalid_or_missing_price_rows": int((~valid).sum()),
        "native_or_synthetic_bid_ask_entry": True,
        "entry_contract": "first M1 open at or after completed HTF candle",
        "exit_contract": "first M1 open at or after entry plus archived horizon",
        "fixed_units": int(fixed_units),
        "compounding": False,
    }
    return work[valid].copy(), audit


def _policy_columns(policy: str) -> dict[str, str]:
    suffix = "target" if policy == "target_semantics" else "inverted"
    return {
        "gross": f"gross_{suffix}_pips",
        "spread": f"spread_drag_{suffix}_pips",
        "net": f"net_{suffix}_pips",
        "pnl": f"pnl_{suffix}_usd",
    }


def _prepare_policy_values(frame: pd.DataFrame, policy: str, score_column: str) -> pd.DataFrame:
    columns = _policy_columns(policy)
    out = frame.copy()
    out["selected_score"] = pd.to_numeric(out[score_column], errors="coerce")
    out["selected_gross_pips"] = pd.to_numeric(out[columns["gross"]], errors="coerce")
    out["selected_spread_drag_pips"] = pd.to_numeric(out[columns["spread"]], errors="coerce")
    out["selected_net_pips"] = pd.to_numeric(out[columns["net"]], errors="coerce")
    out["selected_pnl_usd"] = pd.to_numeric(out[columns["pnl"]], errors="coerce")
    out["selected_direction"] = np.where(
        policy == "target_semantics",
        out["direction"].astype(str).str.upper(),
        np.where(out["direction"].astype(str).str.upper().eq("LONG"), "SHORT", "LONG"),
    )
    return out.replace([np.inf, -np.inf], np.nan).dropna(
        subset=["selected_score", "selected_net_pips", "selected_pnl_usd"]
    )


def allocate_fixed_exposure(
    eligible: pd.DataFrame,
    *,
    max_new_per_timestamp: int,
    max_open_positions: int = MAX_OPEN_POSITIONS,
) -> tuple[pd.DataFrame, dict[str, int]]:
    if eligible.empty:
        return eligible.copy(), {
            "eligible_after_score": 0,
            "rejected_by_timestamp_quota": 0,
            "rejected_by_pair_overlap": 0,
            "rejected_by_exposure_cap": 0,
            "selected_trades": 0,
        }
    ordered = eligible.sort_values(
        ["entry_time_utc", "selected_score", "instrument", "timeframe"],
        ascending=[True, False, True, True],
    )
    active_by_pair: dict[str, pd.Timestamp] = {}
    kept_indices: list[int] = []
    rejected_quota = 0
    rejected_pair = 0
    rejected_exposure = 0
    current_timestamp: pd.Timestamp | None = None
    opened_at_timestamp = 0
    for row in ordered.itertuples():
        entry_time = _utc(row.entry_time_utc)
        if current_timestamp is None or entry_time != current_timestamp:
            current_timestamp = entry_time
            opened_at_timestamp = 0
        expired = [pair for pair, exit_time in active_by_pair.items() if exit_time <= entry_time]
        for pair in expired:
            active_by_pair.pop(pair, None)
        if opened_at_timestamp >= max_new_per_timestamp:
            rejected_quota += 1
            continue
        instrument = str(row.instrument)
        if instrument in active_by_pair:
            rejected_pair += 1
            continue
        if len(active_by_pair) >= max_open_positions:
            rejected_exposure += 1
            continue
        kept_indices.append(int(row.Index))
        active_by_pair[instrument] = _utc(row.exit_time_utc)
        opened_at_timestamp += 1
    selected = ordered.loc[kept_indices].sort_values(["entry_time_utc", "selected_score"], ascending=[True, False])
    return selected, {
        "eligible_after_score": int(len(eligible)),
        "rejected_by_timestamp_quota": int(rejected_quota),
        "rejected_by_pair_overlap": int(rejected_pair),
        "rejected_by_exposure_cap": int(rejected_exposure),
        "selected_trades": int(len(selected)),
    }


def _apply_config(
    frame: pd.DataFrame,
    config: dict[str, Any],
    *,
    threshold: float,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    timeframes = tuple(config["timeframes"])
    source = frame[frame["timeframe"].isin(timeframes)].copy()
    prepared = _prepare_policy_values(source, config["direction_policy"], config["score_column"])
    eligible = prepared[prepared["selected_score"] >= float(threshold)].copy()
    selected, allocator = allocate_fixed_exposure(
        eligible,
        max_new_per_timestamp=int(config["max_new_per_timestamp"]),
        max_open_positions=MAX_OPEN_POSITIONS,
    )
    attribution = {
        "source_candidates": int(len(frame)),
        "rejected_by_timeframe": int(len(frame) - len(source)),
        "candidates_in_selected_timeframes": int(len(source)),
        "candidates_with_finite_score_and_outcome": int(len(prepared)),
        "rejected_by_score_threshold": int(len(prepared) - len(eligible)),
        **allocator,
    }
    return eligible, selected, attribution


def _max_drawdown_pct(pnl: pd.Series) -> float:
    if pnl.empty:
        return 0.0
    equity = INITIAL_EQUITY + pnl.cumsum().to_numpy(dtype=float)
    equity = np.concatenate([[INITIAL_EQUITY], equity])
    peaks = np.maximum.accumulate(equity)
    drawdowns = (equity - peaks) / np.where(peaks == 0.0, np.nan, peaks) * 100.0
    return float(abs(np.nanmin(drawdowns)))


def _profit_factor(values: pd.Series) -> float | None:
    wins = float(values[values > 0.0].sum())
    losses = float(abs(values[values < 0.0].sum()))
    return wins / losses if losses > 0.0 else (None if wins <= 0.0 else math.inf)


def _breakdown(trades: pd.DataFrame, column: str) -> list[dict[str, Any]]:
    if trades.empty or column not in trades:
        return []
    rows = []
    for value, group in trades.groupby(column, dropna=False, sort=True):
        rows.append({
            column: str(value),
            "trades": int(len(group)),
            "pnl_usd": float(group["selected_pnl_usd"].sum()),
            "mean_net_pips": float(group["selected_net_pips"].mean()),
            "win_rate": float((group["selected_pnl_usd"] > 0.0).mean()),
        })
    return rows


def _quarter_labels(timestamps: pd.Series) -> pd.Series:
    values = pd.to_datetime(timestamps, utc=True)
    quarter = ((values.dt.month - 1) // 3 + 1).astype(str)
    return values.dt.year.astype(str) + "Q" + quarter


def _top_k_metrics(eligible: pd.DataFrame, k: int) -> dict[str, Any]:
    if eligible.empty:
        return {"k": int(k), "rows": 0, "pnl_usd": 0.0, "mean_net_pips": None, "precision": None}
    top = (
        eligible.sort_values(["entry_time_utc", "selected_score"], ascending=[True, False])
        .groupby("entry_time_utc", sort=False)
        .head(k)
    )
    return {
        "k": int(k),
        "rows": int(len(top)),
        "timestamps": int(top["entry_time_utc"].nunique()),
        "pnl_usd": float(top["selected_pnl_usd"].sum()),
        "mean_net_pips": float(top["selected_net_pips"].mean()),
        "precision": float((top["selected_pnl_usd"] > 0.0).mean()),
    }


def _rich_metrics(trades: pd.DataFrame, eligible: pd.DataFrame) -> dict[str, Any]:
    if trades.empty:
        return {
            "pnl_usd": 0.0,
            "return_pct": 0.0,
            "trades": 0,
            "win_rate": None,
            "profit_factor": None,
            "max_drawdown_pct": 0.0,
            "mean_net_pips": None,
            "top_score_decile_actual_ev_usd": None,
            "rank_ic": None,
            "precision_at_1": None,
            "precision_at_3": None,
            "precision_at_5": None,
        }
    realized = trades.sort_values(["exit_time_utc", "entry_time_utc"])
    pnl = realized["selected_pnl_usd"]
    net_pips = realized["selected_net_pips"]
    gross_pips = realized["selected_gross_pips"]
    spread_pips = realized["selected_spread_drag_pips"]
    top_cut = float(eligible["selected_score"].quantile(0.90)) if not eligible.empty else math.inf
    top_bucket = eligible[eligible["selected_score"] >= top_cut]
    top_metrics = {k: _top_k_metrics(eligible, k) for k in (1, 3, 5)}
    pair_counts = realized["instrument"].value_counts()
    return {
        "pnl_usd": float(pnl.sum()),
        "return_pct": float(pnl.sum() / INITIAL_EQUITY * 100.0),
        "trades": int(len(realized)),
        "win_rate": float((pnl > 0.0).mean()),
        "profit_factor": _profit_factor(pnl),
        "max_drawdown_pct": _max_drawdown_pct(pnl),
        "mean_net_pips": float(net_pips.mean()),
        "total_net_pips": float(net_pips.sum()),
        "mean_gross_pips": float(gross_pips.mean()),
        "total_gross_pips": float(gross_pips.sum()),
        "spread_drag_pips": float(spread_pips.sum()),
        "slippage_drag_pips": float(realized["slippage_pips_round_trip"].sum()),
        "top_score_decile_actual_ev_usd": float(top_bucket["selected_pnl_usd"].mean()) if len(top_bucket) else None,
        "top_score_decile_mean_net_pips": float(top_bucket["selected_net_pips"].mean()) if len(top_bucket) else None,
        "rank_ic": rank_ic(eligible, "selected_score", "selected_pnl_usd"),
        "precision_at_1": top_metrics[1]["precision"],
        "precision_at_3": top_metrics[3]["precision"],
        "precision_at_5": top_metrics[5]["precision"],
        "top_k": top_metrics,
        "top_pair_trade_share": float(pair_counts.iloc[0] / len(realized)) if len(pair_counts) else None,
        "pair_breakdown": _breakdown(realized, "instrument"),
        "timeframe_breakdown": _breakdown(realized, "timeframe"),
        "quarter_breakdown": _breakdown(
            realized.assign(quarter=_quarter_labels(realized["entry_time_utc"])),
            "quarter",
        ),
    }


def _quick_metrics(trades: pd.DataFrame) -> dict[str, Any]:
    if trades.empty:
        return {"pnl_usd": 0.0, "trades": 0, "mean_net_pips": None, "profit_factor": None}
    pnl = trades["selected_pnl_usd"]
    return {
        "pnl_usd": float(pnl.sum()),
        "trades": int(len(trades)),
        "mean_net_pips": float(trades["selected_net_pips"].mean()),
        "profit_factor": _profit_factor(pnl),
    }


def _fold_metrics(trades: pd.DataFrame) -> list[dict[str, Any]]:
    if trades.empty:
        return []
    work = trades.copy()
    work["quarter"] = _quarter_labels(work["entry_time_utc"])
    rows = []
    for quarter, group in work.groupby("quarter", sort=True):
        rows.append({"quarter": quarter, **_quick_metrics(group)})
    return rows


def _viability(metrics: dict[str, Any], folds: list[dict[str, Any]]) -> dict[str, Any]:
    nonempty = [fold for fold in folds if int(fold.get("trades", 0)) > 0]
    positive = sum(float(fold.get("pnl_usd", 0.0)) > 0.0 for fold in nonempty)
    required = int(math.ceil(len(nonempty) / 2.0)) if nonempty else 1
    viable = (
        int(metrics.get("trades", 0)) >= 100
        and float(metrics.get("pnl_usd", 0.0)) > 0.0
        and float(metrics.get("mean_net_pips") or -math.inf) > 0.0
        and positive >= required
    )
    return {
        "viable": bool(viable),
        "positive_folds": int(positive),
        "nonempty_folds": int(len(nonempty)),
        "required_positive_folds": int(required),
    }


def _config_name(config: dict[str, Any]) -> str:
    return "|".join([
        "+".join(config["timeframes"]),
        str(config["direction_policy"]),
        str(config["score_column"]),
        f"q{float(config['score_quantile']):.2f}",
        f"new{int(config['max_new_per_timestamp'])}",
    ])


def _model_configs() -> Iterable[dict[str, Any]]:
    for timeframes, direction, score, quantile, quota in itertools.product(
        TIMEFRAME_SETS,
        ("target_semantics", "invert_target_semantics"),
        MODEL_SCORE_COLUMNS,
        SCORE_QUANTILES,
        NEW_TRADE_QUOTAS,
    ):
        yield {
            "family": "archived_hgb_227_feature",
            "timeframes": list(timeframes),
            "direction_policy": direction,
            "score_column": score,
            "score_quantile": float(quantile),
            "max_new_per_timestamp": int(quota),
        }


def _select_grid(development: pd.DataFrame, configs: Iterable[dict[str, Any]]) -> dict[str, Any]:
    reports: list[dict[str, Any]] = []
    threshold_cache: dict[tuple[tuple[str, ...], str, float], float] = {}
    for config in configs:
        timeframes = tuple(config["timeframes"])
        score_column = str(config["score_column"])
        quantile = float(config["score_quantile"])
        cache_key = (timeframes, score_column, quantile)
        threshold = threshold_cache.get(cache_key)
        if threshold is None:
            values = pd.to_numeric(
                development.loc[development["timeframe"].isin(timeframes), score_column],
                errors="coerce",
            ).dropna()
            threshold = float(values.quantile(quantile)) if len(values) else math.inf
            threshold_cache[cache_key] = threshold
        _eligible, selected, attribution = _apply_config(development, config, threshold=threshold)
        metrics = _quick_metrics(selected)
        folds = _fold_metrics(selected)
        viability = _viability(metrics, folds)
        reports.append({
            "config_name": _config_name(config),
            "config": config,
            "frozen_score_threshold": threshold,
            "development": metrics,
            "folds": folds,
            "viability": viability,
            "gate_attribution": attribution,
        })
    viable = [report for report in reports if report["viability"]["viable"]]
    ranked = viable or reports
    best = max(
        ranked,
        key=lambda report: (
            report["viability"]["viable"],
            report["viability"]["positive_folds"],
            float(report["development"].get("pnl_usd", -math.inf)),
            float(report["development"].get("mean_net_pips") or -math.inf),
        ),
    )
    return {
        "best": best,
        "reports": reports,
        "declared_configs": int(len(reports)),
        "viable_configs": int(len(viable)),
    }


def _simple_configs() -> Iterable[dict[str, Any]]:
    for timeframes, direction, quantile, quota in itertools.product(
        TIMEFRAME_SETS,
        ("target_semantics", "invert_target_semantics"),
        SCORE_QUANTILES,
        NEW_TRADE_QUOTAS,
    ):
        yield {
            "family": "candidate_surface_abs_momentum_baseline",
            "timeframes": list(timeframes),
            "direction_policy": direction,
            "score_column": "simple_abs_momentum_score",
            "score_quantile": float(quantile),
            "max_new_per_timestamp": int(quota),
        }


def _random_direction_baseline(trades: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if trades.empty:
        return trades.copy(), _rich_metrics(trades, trades)
    out = trades.copy()
    choose_target = out["candidate_uid"].astype(str).map(
        lambda value: hashlib.sha256(value.encode("utf-8")).digest()[0] % 2 == 0
    ).to_numpy()
    out["selected_gross_pips"] = np.where(
        choose_target,
        out["gross_target_pips"],
        out["gross_inverted_pips"],
    )
    out["selected_spread_drag_pips"] = np.where(
        choose_target,
        out["spread_drag_target_pips"],
        out["spread_drag_inverted_pips"],
    )
    out["selected_net_pips"] = np.where(
        choose_target,
        out["net_target_pips"],
        out["net_inverted_pips"],
    )
    out["selected_pnl_usd"] = np.where(
        choose_target,
        out["pnl_target_usd"],
        out["pnl_inverted_usd"],
    )
    out["selected_direction"] = np.where(
        choose_target,
        out["direction"].astype(str).str.upper(),
        np.where(out["direction"].astype(str).str.upper().eq("LONG"), "SHORT", "LONG"),
    )
    return out, _rich_metrics(out, out)


def _cost_stress(trades: pd.DataFrame) -> dict[str, Any]:
    if trades.empty:
        return {
            "trades": 0,
            "base_pnl_usd": 0.0,
            "stress": [],
            "breakeven_spread_multiplier": None,
            "breakeven_additional_slippage_pips": None,
            "edge_survives_mild_stress": False,
            "edge_exists_only_under_fantasy_costs": False,
        }
    usd_per_pip = trades["pip_value_usd_per_unit"] * trades["fixed_units"]
    gross_usd = float((trades["selected_gross_pips"] * usd_per_pip).sum())
    spread_usd = float((trades["selected_spread_drag_pips"] * usd_per_pip).sum())
    slippage_usd = float((trades["slippage_pips_round_trip"] * usd_per_pip).sum())
    base = gross_usd - spread_usd - slippage_usd
    rows = []
    for name, multiplier in [
        ("zero_cost_fantasy", 0.0),
        ("spread_0p5x", 0.5),
        ("base_spread", 1.0),
        ("spread_1p25x", 1.25),
        ("spread_1p5x", 1.5),
        ("spread_2x", 2.0),
    ]:
        pnl = gross_usd - spread_usd * multiplier - (0.0 if multiplier == 0.0 else slippage_usd)
        rows.append({
            "stress": name,
            "pnl_usd": float(pnl),
            "return_pct": float(pnl / INITIAL_EQUITY * 100.0),
            "survives": bool(pnl > 0.0),
        })
    per_pip_usd = float(usd_per_pip.sum())
    for name, additional_pips in [("slippage_plus_0p1", 0.1), ("slippage_plus_0p3", 0.3)]:
        pnl = base - additional_pips * per_pip_usd
        rows.append({
            "stress": name,
            "pnl_usd": float(pnl),
            "return_pct": float(pnl / INITIAL_EQUITY * 100.0),
            "survives": bool(pnl > 0.0),
        })
    by_name = {row["stress"]: row for row in rows}
    mild = all(by_name[name]["survives"] for name in ["base_spread", "spread_1p25x", "slippage_plus_0p1"])
    return {
        "trades": int(len(trades)),
        "gross_pnl_usd": gross_usd,
        "spread_drag_usd": spread_usd,
        "slippage_drag_usd": slippage_usd,
        "base_pnl_usd": float(base),
        "stress": rows,
        "breakeven_spread_multiplier": (
            float((gross_usd - slippage_usd) / spread_usd) if spread_usd > 0.0 else None
        ),
        "breakeven_additional_slippage_pips": (
            float(base / per_pip_usd) if per_pip_usd > 0.0 else None
        ),
        "edge_survives_mild_stress": bool(mild),
        "edge_exists_only_under_fantasy_costs": bool(
            by_name["zero_cost_fantasy"]["survives"] and base <= 0.0
        ),
    }


def _feature_timing_audit() -> dict[str, Any]:
    payload = json.loads(EXPERIMENT_PATH.read_text(encoding="utf-8"))
    result = payload.get("result") or {}
    spec = payload.get("spec") or {}
    features = [str(value) for value in result.get("features") or []]
    forbidden_tokens = (
        "future_",
        "_target",
        "target_",
        "_outcome",
        "outcome_",
        "_curve_",
        "_profit_",
        "_mfe_",
        "_mae_",
        "giveback",
    )
    forbidden_features = [
        feature for feature in features if any(token in feature.lower() for token in forbidden_tokens)
    ]
    path_windows = {
        timeframe: {
            "declared_horizon_minutes": 120,
            "hard_coded_path_rows": 24,
            "actual_path_window_minutes": curve_path_window_minutes(120, timeframe),
            "actual_path_window_hours": curve_path_window_minutes(120, timeframe) / 60.0,
        }
        for timeframe in TIMEFRAME_MINUTES
    }
    return {
        "experiment_id": payload.get("experiment_id"),
        "model_type": spec.get("model_type"),
        "target": spec.get("target"),
        "outcome": spec.get("outcome"),
        "declared_feature_count": int(len(features)),
        "reported_feature_count": result.get("feature_count", len(features)),
        "feature_name_forbidden_columns": forbidden_features,
        "feature_name_allowlist_passed": not forbidden_features,
        "bar_timestamp_contract": {
            "source_resample_label": "left edge",
            "archived_candidate_timestamp": "left edge",
            "feature_available_delay_minutes": TIMEFRAME_MINUTES,
            "original_exact_replay_entry_timestamp": "unshifted left edge",
            "original_replay_bar_completion_lookahead": True,
            "corrected_replay_entry_timestamp": "first M1 open after HTF bar completion",
        },
        "endpoint_label_contract": {
            "offset_uses_base_minutes_minus_one": True,
            "endpoint_measured_after_completed_bar": True,
        },
        "path_label_contract": {
            "best_path_weight": 0.55,
            "endpoint_weight": 0.25,
            "early_weight": 0.20,
            "path_rows_calculated_as_horizon_divided_by_5": True,
            "timeframe_specific_window_bug": True,
            "windows": path_windows,
        },
        "global_static_feature_risk": {
            "features": [
                "instrument_volatility_percentile",
                "instrument_median_abs_60_pips",
                "instrument_q995_abs_60_pips",
                "instrument_q995_move_to_spread",
            ],
            "computed_from_full_volatility_table": True,
            "holdout_safe_as_of_join": False,
        },
        "archived_score_percentile_risk": {
            "computed_over_complete_archive": True,
            "used_by_corrected_replay": False,
        },
        "full_timing_audit_passed": False,
        "original_portfolio_claim_valid": False,
        "corrected_replay_can_test_salvage_signal": True,
    }


def _latest_m1_reference(output_dir: Path) -> dict[str, Any] | None:
    candidates = sorted(
        output_dir.parent.glob("validation_replay_leakfree_walkforward_*/VALIDATION_REPLAY_FINAL_SUMMARY.json"),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )
    for path in candidates:
        if (path.parent / "RUN_INVALID.md").exists() or (path.parent / "RUN_SUPERSEDED.md").exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        return {
            "path": str(path),
            "verdict": payload.get("verdict"),
            "best_arima_ar_baseline": payload.get("best_arima_ar_baseline"),
            "selected_ml_config": payload.get("selected_ml_config"),
            "comparison_note": "Reference only: M1 intrahour and HTF 120-minute replays are not directly comparable portfolios.",
        }
    return None


def _top_scorecards(grid: dict[str, Any], limit: int = 40) -> list[dict[str, Any]]:
    return sorted(
        grid["reports"],
        key=lambda report: (
            report["viability"]["viable"],
            report["viability"]["positive_folds"],
            float(report["development"].get("pnl_usd", -math.inf)),
        ),
        reverse=True,
    )[:limit]


def run_htf_validation_replay(
    cfg: dict[str, Any],
    output_dir: Path,
    *,
    start: str,
    end: str,
    tier: str,
    development_root: Path | None = None,
    evaluation_root: Path | None = None,
    fixed_units: int = DEFAULT_FIXED_UNITS,
) -> Path:
    if tier == "all":
        raise ValueError("HTF validation replay requires an explicit bounded tier; all-tier is not automatic")
    output_dir.mkdir(parents=True, exist_ok=True)
    development_root = development_root or DEFAULT_DEVELOPMENT_ROOT
    evaluation_root = evaluation_root or DEFAULT_EVALUATION_ROOT
    evaluation_start = _utc(start)
    evaluation_end = _exclusive_end(end)

    development_raw, development_coverage = _load_candidate_root(
        development_root,
        cfg,
        tier,
        "development_2025",
    )
    evaluation_raw, evaluation_coverage = _load_candidate_root(
        evaluation_root,
        cfg,
        tier,
        "diagnostic_evaluation_2026",
        start=evaluation_start,
        end=evaluation_end,
    )
    raw = pd.concat([development_raw, evaluation_raw], ignore_index=True, copy=False)
    scored, execution_audit = score_executable_outcomes(raw, cfg, fixed_units=fixed_units)
    scored["simple_abs_momentum_score"] = pd.to_numeric(
        scored["momentum_30_atr"], errors="coerce"
    ).abs()
    development = scored[scored["source_period"] == "development_2025"].copy()
    evaluation = scored[scored["source_period"] == "diagnostic_evaluation_2026"].copy()

    model_grid = _select_grid(development, _model_configs())
    model_best = model_grid["best"]
    model_config = model_best["config"]
    model_threshold = float(model_best["frozen_score_threshold"])
    model_dev_eligible, model_dev_trades, model_dev_gates = _apply_config(
        development,
        model_config,
        threshold=model_threshold,
    )
    model_eval_eligible, model_eval_trades, model_eval_gates = _apply_config(
        evaluation,
        model_config,
        threshold=model_threshold,
    )
    model_viable = bool(model_best["viability"]["viable"])
    selected_model_eval_trades = model_eval_trades if model_viable else model_eval_trades.iloc[0:0].copy()
    selected_model_dev_trades = model_dev_trades if model_viable else model_dev_trades.iloc[0:0].copy()
    model_development_metrics = _rich_metrics(selected_model_dev_trades, model_dev_eligible)
    model_evaluation_metrics = _rich_metrics(selected_model_eval_trades, model_eval_eligible)
    best_research_evaluation_metrics = _rich_metrics(model_eval_trades, model_eval_eligible)

    simple_grid = _select_grid(development, _simple_configs())
    simple_best = simple_grid["best"]
    simple_config = simple_best["config"]
    simple_threshold = float(simple_best["frozen_score_threshold"])
    simple_dev_eligible, simple_dev_trades, _simple_dev_gates = _apply_config(
        development,
        simple_config,
        threshold=simple_threshold,
    )
    simple_eval_eligible, simple_eval_trades, simple_eval_gates = _apply_config(
        evaluation,
        simple_config,
        threshold=simple_threshold,
    )
    simple_viable = bool(simple_best["viability"]["viable"])
    selected_simple_eval = simple_eval_trades if simple_viable else simple_eval_trades.iloc[0:0].copy()
    simple_development_metrics = _rich_metrics(
        simple_dev_trades if simple_viable else simple_dev_trades.iloc[0:0].copy(),
        simple_dev_eligible,
    )
    simple_evaluation_metrics = _rich_metrics(selected_simple_eval, simple_eval_eligible)

    random_trades, random_metrics = _random_direction_baseline(selected_model_eval_trades)
    cost_stress = _cost_stress(selected_model_eval_trades)
    timing_audit = _feature_timing_audit()
    raw_selected_timeframes = evaluation[evaluation["timeframe"].isin(model_config["timeframes"])]
    raw_policy_surface = _prepare_policy_values(
        raw_selected_timeframes,
        model_config["direction_policy"],
        model_config["score_column"],
    )
    raw_top_k = {str(k): _top_k_metrics(raw_policy_surface, k) for k in (1, 3, 5)}

    selected_model_pnl = float(model_evaluation_metrics["pnl_usd"])
    simple_pnl = float(simple_evaluation_metrics["pnl_usd"])
    random_pnl = float(random_metrics["pnl_usd"])
    signal_checks = {
        "development_config_viable": model_viable,
        "positive_diagnostic_evaluation_pnl_after_costs": selected_model_pnl > 0.0,
        "beats_no_trade": selected_model_pnl > 0.0,
        "beats_selected_simple_baseline": selected_model_pnl > simple_pnl,
        "beats_random_direction_baseline": selected_model_pnl > random_pnl,
        "top_score_decile_actual_ev_positive": (
            model_evaluation_metrics.get("top_score_decile_actual_ev_usd") or -math.inf
        ) > 0.0,
        "mild_cost_stress_survives": bool(cost_stress["edge_survives_mild_stress"]),
    }
    salvage_verdict = "PASS" if all(signal_checks.values()) else "FAIL"
    production_checks = {
        "corrected_target_dataset_rebuilt": False,
        "fresh_untouched_evaluation": False,
        "full_feature_timing_audit_passed": bool(timing_audit["full_timing_audit_passed"]),
        "live_execution_disabled": True,
    }

    selected_model_eval_trades.to_csv(output_dir / "htf_selected_evaluation_trades.csv", index=False)
    selected_model_dev_trades.to_csv(output_dir / "htf_selected_development_trades.csv", index=False)
    random_trades.to_csv(output_dir / "htf_random_baseline_trades.csv", index=False)

    report = {
        "run_id": output_dir.name,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "htf-validation-replay",
        "tier": tier,
        "evaluation_start": evaluation_start,
        "evaluation_end_exclusive": evaluation_end,
        "source": {
            "development_root": str(development_root),
            "evaluation_root": str(evaluation_root),
            "development_coverage": development_coverage,
            "evaluation_coverage": evaluation_coverage,
            "experiment_path": str(EXPERIMENT_PATH),
        },
        "selection_protocol": {
            "archived_predictions_are_weekly_rolling_out_of_sample": True,
            "configuration_selected_only_on_2025_development": True,
            "frozen_before_2026_scoring": True,
            "evaluation_period_previously_inspected": True,
            "evidence_status": "corrected_execution_diagnostic_not_fresh_holdout",
            "fixed_units": int(fixed_units),
            "compounding": False,
            "max_open_positions": MAX_OPEN_POSITIONS,
        },
        "execution_audit": execution_audit,
        "feature_timing_audit": timing_audit,
        "model_grid": {
            "declared_configs": model_grid["declared_configs"],
            "viable_configs": model_grid["viable_configs"],
            "top_development_scorecards": _top_scorecards(model_grid),
        },
        "selected_model": {
            "selected_for_evaluation": model_viable,
            "config_name": model_best["config_name"],
            "config": model_config,
            "frozen_score_threshold": model_threshold,
            "walk_forward_quarter_viability": model_best["viability"],
            "development_folds": model_best["folds"],
            "development": model_development_metrics,
            "diagnostic_evaluation": model_evaluation_metrics,
            "best_research_config_evaluation_if_nonviable": (
                best_research_evaluation_metrics if not model_viable else None
            ),
        },
        "baselines": {
            "no_trade": {"pnl_usd": 0.0, "return_pct": 0.0, "trades": 0},
            "random_direction_on_selected_opportunities": random_metrics,
            "candidate_surface_abs_momentum": {
                "selected_for_evaluation": simple_viable,
                "config_name": simple_best["config_name"],
                "config": simple_config,
                "frozen_score_threshold": simple_threshold,
                "development": simple_development_metrics,
                "diagnostic_evaluation": simple_evaluation_metrics,
                "gate_attribution": simple_eval_gates,
            },
            "m1_arima_reference": _latest_m1_reference(output_dir),
        },
        "raw_top_k_before_selected_threshold_and_allocator": raw_top_k,
        "signal_checks": signal_checks,
        "diagnostic_salvage_signal_verdict": salvage_verdict,
        "production_readiness_verdict": "FAIL",
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "demo_execution_enabled": False,
    }
    write_report_pair(output_dir, "HTF_VALIDATION_REPLAY_REPORT", report)
    write_report_pair(output_dir, "HTF_FEATURE_TIMING_AUDIT", timing_audit)

    gate_report = {
        "development_archive_coverage": development_coverage,
        "evaluation_archive_coverage": evaluation_coverage,
        "selected_model_development": model_dev_gates,
        "selected_model_evaluation": model_eval_gates,
        "archive_limit": "Rows below the historical pair-calibrated probability threshold were not materialized, so this replay cannot recover the full raw probability surface.",
        "score_percentile_used": False,
        "rank_score_warning": "rank_score includes expected_move_atr calibrated against the flawed curve-path label; probability is the cleaner archived score candidate.",
    }
    write_report_pair(output_dir, "HTF_GATE_ATTRIBUTION", gate_report)
    write_report_pair(output_dir, "HTF_COST_STRESS", cost_stress)

    failure_reasons = []
    if salvage_verdict == "FAIL":
        failure_reasons.extend([name for name, passed in signal_checks.items() if not passed])
    failure_reasons.extend([
        "archived curve target uses timeframe-mismatched path windows",
        "2026 evaluation period was previously inspected",
        "global instrument volatility metadata was not built as-of each fold",
    ])
    final_summary = {
        "verdict": "FAIL",
        "diagnostic_salvage_signal_verdict": salvage_verdict,
        "signal_checks": signal_checks,
        "production_checks": production_checks,
        "failure_reasons": failure_reasons,
        "best_validation_selected_htf_config": model_config if model_viable else {"family": "no_trade"},
        "best_research_config": model_config,
        "model_development_result": model_development_metrics,
        "model_diagnostic_evaluation_result": model_evaluation_metrics,
        "best_simple_baseline_result": simple_evaluation_metrics,
        "random_baseline_result": random_metrics,
        "no_trade_result": {"pnl_usd": 0.0, "return_pct": 0.0, "trades": 0},
        "positive_signal_survived_corrected_entry_replay": bool(
            model_viable and selected_model_pnl > 0.0
        ),
        "cost_stress_survived": bool(cost_stress["edge_survives_mild_stress"]),
        "original_93pct_win_rate_claim_accepted": False,
        "biggest_remaining_blocker": (
            "rebuild M30/H1/H4 targets with timeframe-aware path windows and validate on post-2026-07-07 candles"
        ),
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "demo_execution_enabled": False,
        "live_execution_confirmation": "Research/backtest only; no OANDA bot, broker call, order placement, credential edit, or account-file modification was invoked.",
    }
    write_report_pair(output_dir, "HTF_FINAL_SUMMARY", final_summary)
    write_json(output_dir / "run_manifest.json", {
        "mode": "htf-validation-replay",
        "run_dir": str(output_dir),
        "reports": [
            "HTF_VALIDATION_REPLAY_REPORT.json",
            "HTF_FEATURE_TIMING_AUDIT.json",
            "HTF_GATE_ATTRIBUTION.json",
            "HTF_COST_STRESS.json",
            "HTF_FINAL_SUMMARY.json",
        ],
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "demo_execution_enabled": False,
    })
    return output_dir / "HTF_FINAL_SUMMARY.json"
