#!/usr/bin/env python3
"""Build the leakage-audited panel shared by modern model challengers.

The panel is deliberately side-specific.  Every market event has one LONG and
one SHORT row for each requested horizon, and the realized target is the
executable bid/ask net result for that side.  This lets classifiers estimate
side profitability while the evaluator can consolidate both sides to one
action per event.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

try:
    from oanda_s5_timeframe_strategy_replay import (
        TimeframeHistory,
        augment_cross_sectional_features,
        build_features,
        candle_time,
        load_history,
        load_history_with_fallback,
        parse_horizons,
        parse_instruments,
        parse_timeframe,
        seconds_label,
        select_times,
        setup_outcomes,
    )
    from oanda_timeframe_horizon_sweep import discover_instruments
except ModuleNotFoundError:
    from trad.oanda_s5_timeframe_strategy_replay import (
        TimeframeHistory,
        augment_cross_sectional_features,
        build_features,
        candle_time,
        load_history,
        load_history_with_fallback,
        parse_horizons,
        parse_instruments,
        parse_timeframe,
        seconds_label,
        select_times,
        setup_outcomes,
    )
    from trad.oanda_timeframe_horizon_sweep import discover_instruments


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
DEFAULT_M1_DIR = DATA_ROOT / "candles"
DEFAULT_M1_CACHE_DIR = DATA_ROOT / "candles_m1_parquet"
DEFAULT_S5_DIR = DATA_ROOT / "candles_s5_bam"
DEFAULT_PANEL_DIR = DATA_ROOT / "training_sets" / "shared_timeframe_horizon_panel"

SCHEMA_VERSION = 1
FEATURE_VERSION = "strategy_lab_scalar_cross_pair_v1"
CANONICAL_TIMEFRAMES = (
    "S5",
    "S10",
    "S15",
    "S30",
    "M1",
    "M5",
    "M10",
    "M15",
    "M30",
    "H1",
    "H2",
    "H3",
    "H4",
)
CANONICAL_HORIZONS_SEC = (
    30,
    60,
    120,
    180,
    300,
    600,
    900,
    1800,
    3600,
    7200,
    10800,
    14400,
    21600,
    28800,
    43200,
    86400,
)
REQUIRED_LONG_HORIZONS_SEC = (3600, 14400, 43200, 86400)
# An outcome horizon must not mature inside its input bar.
REQUIRED_LONG_CELLS = (
    ("H1", 3600),
    ("H1", 14400),
    ("H1", 43200),
    ("H1", 86400),
    ("H4", 14400),
    ("H4", 43200),
    ("H4", 86400),
)

BASE_SCALAR_FEATURES = (
    "volume_ratio_12",
    "volume_ratio_30",
    "current_volume",
    "current_candle_spread_pips",
    "median_spread_12_pips",
    "spread_ratio_12",
    "spread_drop_3_pips",
    "last",
    "r1_pips",
    "r3_pips",
    "r5_pips",
    "m5_r1_pips",
    "m5_r3_pips",
    "pos20",
    "m1_atr14_pips",
    "m5_atr14_pips",
)
CROSS_PAIR_FEATURES = (
    "cross_sample_count",
    "cross_strength_r1",
    "cross_strength_r3",
    "cross_strength_r5",
    "cross_breadth_r1",
    "cross_breadth_r3",
    "pair_norm_r1",
    "pair_norm_r3",
    "pair_norm_r5",
    "cross_pair_count",
    "pair_rank_r3",
    "pair_rank_r5",
    "relative_residual_r3",
    "relative_residual_r5",
)
OPTIONAL_MICROSTRUCTURE_FEATURES = (
    "live_spread_pips",
    "bid_top_liquidity",
    "ask_top_liquidity",
    "bid_total_liquidity",
    "ask_total_liquidity",
    "depth_imbalance",
    "bid_levels",
    "ask_levels",
    "depth_total_liquidity",
    "depth_log_total_liquidity",
    "depth_top_imbalance",
    "microprice_offset_pips",
    "quote_receive_age_sec",
    "order_book_near_5_imbalance",
    "order_book_near_10_imbalance",
    "order_book_near_25_imbalance",
    "order_book_near_50_imbalance",
    "order_book_above_25_net",
    "order_book_below_25_net",
    "order_book_concentration",
    "position_book_near_5_imbalance",
    "position_book_near_10_imbalance",
    "position_book_near_25_imbalance",
    "position_book_near_50_imbalance",
    "position_book_above_25_net",
    "position_book_below_25_net",
    "position_book_concentration",
    "return_5_pips",
    "return_10_pips",
    "return_30_pips",
    "return_60_pips",
    "acceleration_5_30",
    "volatility_30_pips",
    "volatility_60_pips",
    "range_30_pips",
    "spread_pips",
    "spread_ratio_60",
    "spread_delta_1",
    "spread_delta_5",
    "spread_volatility_30",
    "activity_5",
    "activity_30",
    "activity_ratio_30_60",
    "activity_acceleration_5_30",
    "tick_imbalance_5",
    "tick_imbalance_30",
    "price_efficiency_30",
    "depth_imbalance_delta_5",
    "depth_imbalance_delta_30",
    "depth_log_total_liquidity_delta_5",
    "top_liquidity_share",
    "level_imbalance",
    "microprice_offset_mean_5",
    "microprice_offset_mean_30",
    "liquidity_ratio",
    "liquidity_ratio_log",
    "liquidity_ratio_delta_5",
    "quote_flow_imbalance_1",
    "quote_flow_imbalance_5",
    "quote_flow_imbalance_30",
    "cross_quote_flow_sample_count",
    "cross_quote_flow_strength_1",
    "cross_quote_flow_strength_5",
    "cross_quote_flow_strength_30",
    "cross_quote_flow_breadth_5",
    "quote_flow_rank_5",
    "quote_flow_rank_30",
    "quote_flow_residual_5",
    "quote_flow_residual_30",
)
MODEL_NUMERIC_FEATURES = (
    "input_timeframe_seconds",
    "horizon_sec",
    "side_sign",
    "entry_spread_pips",
    "hour_sin",
    "hour_cos",
    "weekday_sin",
    "weekday_cos",
    *BASE_SCALAR_FEATURES,
    *CROSS_PAIR_FEATURES,
    *OPTIONAL_MICROSTRUCTURE_FEATURES,
)
REQUIRED_MODEL_NUMERIC_FEATURES = tuple(
    name
    for name in MODEL_NUMERIC_FEATURES
    if name not in OPTIONAL_MICROSTRUCTURE_FEATURES
)
MODEL_CATEGORICAL_FEATURES = (
    "instrument",
    "input_timeframe",
    "direction",
    "source",
    "spread_mode",
)
TARGET_COLUMN = "target_profitable"

REQUIRED_COLUMNS = (
    "event_id",
    "side_id",
    "decision_candle_utc",
    "prediction_time_utc",
    "maturity_time_utc",
    "max_feature_origin_utc",
    "instrument",
    "input_timeframe",
    "input_timeframe_seconds",
    "horizon_sec",
    "direction",
    "side_sign",
    "source",
    "spread_mode",
    "feature_version",
    "entry_spread_pips",
    "realized_net_pips",
    "opposite_net_pips",
    "realized_edge_pips",
    "market_mid_move_pips",
    "realized_directional_move_pips",
    TARGET_COLUMN,
    "target_best_side",
    *REQUIRED_MODEL_NUMERIC_FEATURES,
    *MODEL_CATEGORICAL_FEATURES,
)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _finite_or_nan(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    return number if math.isfinite(number) else math.nan


def _feature_origin(features: dict[str, Any], fallback: pd.Timestamp) -> pd.Timestamp:
    origins = features.get("series_origins") or {}
    values = pd.to_datetime(list(origins.values()), utc=True, errors="coerce")
    valid = values[~pd.isna(values)]
    return pd.Timestamp(valid.max()) if len(valid) else fallback


def _calendar_features(timestamp: pd.Timestamp) -> dict[str, float]:
    hour = timestamp.hour + timestamp.minute / 60.0
    weekday = timestamp.dayofweek
    return {
        "hour_sin": math.sin(2.0 * math.pi * hour / 24.0),
        "hour_cos": math.cos(2.0 * math.pi * hour / 24.0),
        "weekday_sin": math.sin(2.0 * math.pi * weekday / 7.0),
        "weekday_cos": math.cos(2.0 * math.pi * weekday / 7.0),
    }


def _scalar_features(features: dict[str, Any]) -> dict[str, float]:
    return {
        name: _finite_or_nan(features.get(name))
        for name in (
            *BASE_SCALAR_FEATURES,
            *CROSS_PAIR_FEATURES,
            *OPTIONAL_MICROSTRUCTURE_FEATURES,
        )
    }


def _event_rows(
    history: TimeframeHistory,
    decision_time: pd.Timestamp,
    entry_position: int,
    timeframe: str,
    source: str,
    horizons: Iterable[int],
    features: dict[str, Any],
) -> list[dict[str, Any]]:
    long_outcomes = setup_outcomes(history, entry_position, "buy", horizons)
    short_outcomes = setup_outcomes(history, entry_position, "sell", horizons)
    entry = history.execution.iloc[entry_position]
    prediction_time = pd.Timestamp(history.execution_times[entry_position])
    if prediction_time.tzinfo is None:
        prediction_time = prediction_time.tz_localize("UTC")
    else:
        prediction_time = prediction_time.tz_convert("UTC")
    feature_origin = _feature_origin(features, decision_time)
    entry_spread = (
        float(entry.ask_open) - float(entry.bid_open)
    ) / history.pip
    scalar = _scalar_features(features)
    calendar = _calendar_features(prediction_time)
    rows: list[dict[str, Any]] = []
    for horizon in sorted(set(long_outcomes) & set(short_outcomes)):
        exit_position = history.exit_position(entry_position, horizon)
        if exit_position is None:
            continue
        exit_row = history.execution.iloc[exit_position]
        entry_mid = 0.5 * (float(entry.ask_open) + float(entry.bid_open))
        exit_mid = 0.5 * (float(exit_row.ask_close) + float(exit_row.bid_close))
        mid_move = (exit_mid - entry_mid) / history.pip
        long_pips = float(long_outcomes[horizon])
        short_pips = float(short_outcomes[horizon])
        event_id = (
            f"{FEATURE_VERSION}|{timeframe}|{prediction_time.isoformat()}|"
            f"{history.instrument}|{horizon}"
        )
        common = {
            "event_id": event_id,
            "decision_candle_utc": decision_time,
            "prediction_time_utc": prediction_time,
            "maturity_time_utc": prediction_time + timedelta(seconds=int(horizon)),
            "max_feature_origin_utc": feature_origin,
            "instrument": history.instrument,
            "input_timeframe": timeframe,
            "input_timeframe_seconds": int(history.primary_seconds),
            "horizon_sec": int(horizon),
            "source": source,
            "spread_mode": history.spread_mode,
            "feature_version": FEATURE_VERSION,
            "entry_spread_pips": float(entry_spread),
            "market_mid_move_pips": float(mid_move),
            **calendar,
            **scalar,
        }
        for direction, side_sign, own, opposite, is_best in (
            ("LONG", 1.0, long_pips, short_pips, long_pips >= short_pips),
            ("SHORT", -1.0, short_pips, long_pips, short_pips > long_pips),
        ):
            rows.append(
                {
                    **common,
                    "side_id": f"{event_id}|{direction}",
                    "direction": direction,
                    "side_sign": side_sign,
                    "realized_net_pips": own,
                    "opposite_net_pips": opposite,
                    "realized_edge_pips": own - opposite,
                    "realized_directional_move_pips": side_sign * mid_move,
                    TARGET_COLUMN: int(own > 0.0),
                    "target_best_side": int(is_best),
                }
            )
    return rows


def build_panel_rows(
    histories: dict[str, TimeframeHistory],
    decision_times: list[pd.Timestamp],
    timeframe: str,
    source: str,
    horizons: Iterable[int],
    progress_every: int = 0,
) -> pd.DataFrame:
    """Build rows using only features observable by each prediction time."""
    output: list[dict[str, Any]] = []
    for cycle, decision_time in enumerate(decision_times, start=1):
        feature_cache: dict[str, dict[str, Any]] = {}
        entries: dict[str, int] = {}
        for instrument, history in histories.items():
            windows = history.windows(decision_time)
            entry = history.entry_position(decision_time)
            if windows is None or entry is None:
                continue
            features, _ = build_features(instrument, windows, history.pip)
            if features is None:
                continue
            feature_cache[instrument] = features
            entries[instrument] = entry
        augment_cross_sectional_features(feature_cache)
        for instrument, features in feature_cache.items():
            output.extend(
                _event_rows(
                    histories[instrument],
                    decision_time,
                    entries[instrument],
                    timeframe,
                    source,
                    horizons,
                    features,
                )
            )
        if progress_every and (cycle % progress_every == 0 or cycle == len(decision_times)):
            print(f"[shared-panel] {cycle:,}/{len(decision_times):,} cycles", flush=True)
    frame = pd.DataFrame(output)
    if not frame.empty:
        frame = frame.sort_values(
            ["prediction_time_utc", "instrument", "horizon_sec", "direction"]
        ).reset_index(drop=True)
    return frame


def validate_panel(frame: pd.DataFrame) -> dict[str, Any]:
    missing = sorted(set(REQUIRED_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"shared panel missing columns: {', '.join(missing)}")
    if frame.empty:
        raise ValueError("shared panel is empty")
    if frame["side_id"].duplicated().any():
        raise ValueError("shared panel has duplicate side_id values")
    side_counts = frame.groupby("event_id", observed=True)["direction"].nunique()
    if not side_counts.eq(2).all():
        raise ValueError("every shared-panel event must have LONG and SHORT rows")
    best_counts = frame.groupby("event_id", observed=True)["target_best_side"].sum()
    if not best_counts.eq(1).all():
        raise ValueError("every shared-panel event must have exactly one best side")
    for column in (
        "decision_candle_utc",
        "prediction_time_utc",
        "maturity_time_utc",
        "max_feature_origin_utc",
    ):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="raise")
    if not (frame["maturity_time_utc"] > frame["prediction_time_utc"]).all():
        raise ValueError("maturity must be after prediction time")
    if not (frame["max_feature_origin_utc"] <= frame["prediction_time_utc"]).all():
        raise ValueError("feature provenance extends beyond prediction time")
    if not frame[TARGET_COLUMN].isin([0, 1]).all():
        raise ValueError("target_profitable must be binary")
    if (frame["entry_spread_pips"] < 0.0).any():
        raise ValueError("entry spread cannot be negative")
    return {
        "rows": int(len(frame)),
        "events": int(frame["event_id"].nunique()),
        "instruments": sorted(frame["instrument"].unique().tolist()),
        "timeframes": sorted(frame["input_timeframe"].unique().tolist()),
        "horizons_sec": sorted(int(value) for value in frame["horizon_sec"].unique()),
        "start_utc": frame["prediction_time_utc"].min().isoformat(),
        "end_utc": frame["prediction_time_utc"].max().isoformat(),
        "maturity_end_utc": frame["maturity_time_utc"].max().isoformat(),
        "positive_rate": float(frame[TARGET_COLUMN].mean()),
    }


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_panel_atomic(
    frame: pd.DataFrame,
    output: Path,
    provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = validate_panel(frame)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + f".{os.getpid()}.tmp")
    try:
        frame.to_parquet(temporary, index=False, compression="zstd")
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "feature_version": FEATURE_VERSION,
        "generated_utc": utc_iso(),
        "execution_policy": "research_shadow_only",
        "account_wired": False,
        "panel": {
            "path": str(output.resolve()),
            "bytes": output.stat().st_size,
            "sha256": sha256_file(output),
            **summary,
        },
        "model_contract": {
            "numeric_features": list(MODEL_NUMERIC_FEATURES),
            "categorical_features": list(MODEL_CATEGORICAL_FEATURES),
            "target": TARGET_COLUMN,
            "event_consolidation": "maximum side probability per event_id",
            "purge_rule": "training maturity_time_utc must precede test prediction_time_utc",
            "cost_semantics": "realized_net_pips uses observed bid/ask entry and exit prices",
        },
        "provenance": provenance or {},
    }
    manifest_path = output.with_suffix(output.suffix + ".manifest.json")
    write_json_atomic(manifest_path, manifest)
    manifest["manifest_path"] = str(manifest_path.resolve())
    return manifest


def _default_output(timeframe: str, source: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return DEFAULT_PANEL_DIR / f"panel_{timeframe.lower()}_{source}_{stamp}.parquet"


def load_panel_history(
    *,
    source: str,
    m1_dir: Path,
    m1_cache_dir: Path,
    s5_dir: Path,
    instrument: str,
    timeframe_seconds: int,
    tail_rows: int,
) -> tuple[TimeframeHistory, str, str]:
    if source != "hybrid":
        return load_history_with_fallback(
            source=source,
            m1_dir=m1_dir,
            m1_cache_dir=m1_cache_dir,
            s5_dir=s5_dir,
            instrument=instrument,
            timeframe_seconds=timeframe_seconds,
            tail_rows=tail_rows,
            compact=True,
        )

    history, actual_source, warning = load_history_with_fallback(
        source="m1",
        m1_dir=m1_dir,
        m1_cache_dir=m1_cache_dir,
        s5_dir=s5_dir,
        instrument=instrument,
        timeframe_seconds=timeframe_seconds,
        tail_rows=tail_rows,
        compact=True,
    )
    primary_start = pd.Timestamp(history.primary.index.min())
    primary_end = pd.Timestamp(history.primary.index.max())
    primary_span_seconds = max(
        0.0,
        (primary_end - primary_start).total_seconds(),
    )
    # The S5 supplement only supplies execution prices. Keep one day of end-time
    # skew padding, while still allowing long input windows to request the full file.
    observed_tail_rows = max(
        120,
        math.ceil(primary_span_seconds / 5.0) + math.ceil(timedelta(days=1).total_seconds() / 5.0),
    )
    observed = load_history(
        s5_dir,
        instrument,
        5,
        observed_tail_rows,
        compact=True,
    )
    history.execution = observed.execution
    history.execution_seconds = observed.execution_seconds
    history.execution_times = observed.execution_times
    history.execution_positions = observed.execution_positions
    history.spread_mode = f"deep_features_{actual_source}|observed_s5_execution"
    return history, f"{actual_source}_features_s5_execution", warning


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeframe", type=parse_timeframe, default=parse_timeframe("M1"))
    parser.add_argument("--horizons-sec", type=parse_horizons, default=list(CANONICAL_HORIZONS_SEC))
    parser.add_argument("--source", choices=("auto", "m1", "s5", "hybrid"), default="auto")
    parser.add_argument("--instruments", default="")
    parser.add_argument("--m1-dir", type=Path, default=DEFAULT_M1_DIR)
    parser.add_argument("--m1-cache-dir", type=Path, default=DEFAULT_M1_CACHE_DIR)
    parser.add_argument("--s5-dir", type=Path, default=DEFAULT_S5_DIR)
    parser.add_argument("--max-cycles", type=int, default=500)
    parser.add_argument("--sampling", choices=("uniform", "latest"), default="uniform")
    parser.add_argument("--step-seconds", type=int, default=0)
    parser.add_argument("--tail-rows", type=int, default=0)
    parser.add_argument("--max-exit-delay-sec", type=int, default=0)
    parser.add_argument("--progress-every", type=int, default=50)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.max_exit_delay_sec < 0:
        raise SystemExit("max exit delay must be non-negative")
    timeframe = seconds_label(args.timeframe)
    if timeframe not in CANONICAL_TIMEFRAMES:
        raise SystemExit(f"unsupported canonical timeframe: {timeframe}")
    source = args.source if args.source != "auto" else ("s5" if args.timeframe < 60 else "m1")
    horizons = sorted(
        horizon
        for horizon in args.horizons_sec
        if horizon in CANONICAL_HORIZONS_SEC
        and (source in {"s5", "hybrid"} or horizon >= 60)
    )
    if not horizons:
        raise SystemExit("no executable canonical horizons remain for the selected source")
    instruments = (
        parse_instruments(args.instruments)
        if args.instruments
        else discover_instruments(args.m1_dir, args.s5_dir)
    )
    histories: dict[str, TimeframeHistory] = {}
    history_sources: dict[str, str] = {}
    warnings: dict[str, str] = {}
    for instrument in instruments:
        try:
            history, actual_source, warning = load_panel_history(
                source=source,
                m1_dir=args.m1_dir,
                m1_cache_dir=args.m1_cache_dir,
                s5_dir=args.s5_dir,
                instrument=instrument,
                timeframe_seconds=args.timeframe,
                tail_rows=args.tail_rows,
            )
        except (OSError, ValueError, EOFError, pd.errors.ParserError) as error:
            warnings[instrument] = f"load_failed:{type(error).__name__}:{error}"
            continue
        histories[instrument] = history
        history.max_exit_delay_seconds = args.max_exit_delay_sec
        if args.max_exit_delay_sec:
            history.spread_mode = (
                f"{history.spread_mode}|bounded_next_quote_exit<="
                f"{args.max_exit_delay_sec}s"
            )
        history_sources[instrument] = actual_source
        if warning:
            warnings[instrument] = warning
    if not histories:
        raise SystemExit("no instrument histories could be loaded")
    step = args.step_seconds or max(300, args.timeframe)
    times = select_times(
        histories,
        step_seconds=step,
        max_cycles=args.max_cycles,
        max_horizon_sec=max(horizons),
        sampling=args.sampling,
    )
    frame = build_panel_rows(
        histories,
        times,
        timeframe=timeframe,
        source=source,
        horizons=horizons,
        progress_every=args.progress_every,
    )
    output = (args.output or _default_output(timeframe, source)).resolve()
    manifest = write_panel_atomic(
        frame,
        output,
        provenance={
            "requested_instruments": instruments,
            "loaded_instruments": sorted(histories),
            "history_sources": history_sources,
            "history_load_warnings": warnings,
            "sampling_mode": args.sampling,
            "max_cycles": args.max_cycles,
            "step_seconds": step,
            "tail_rows": args.tail_rows,
            "max_exit_delay_sec": args.max_exit_delay_sec,
            "requested_horizons_sec": args.horizons_sec,
        },
    )
    print(json.dumps({"panel": manifest["panel"], "manifest": manifest["manifest_path"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
