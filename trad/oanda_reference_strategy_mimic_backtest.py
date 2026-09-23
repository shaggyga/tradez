#!/usr/bin/env python3
"""Mimic/test previously reported FX strategy profiles on local OANDA data.

The pasted reference IDs (for example ``M5_EV_091`` and ``M15_ATR_032``) are
not currently present as executable strategy definitions in this workspace.
This script therefore treats them as strategy *families* and tests whether
similar rule shapes can reproduce comparable cost-aware behavior.

Outputs:
- calibration-selected out-of-sample test metrics;
- full-period best-fit mimic metrics;
- deltas versus the pasted reference rows.

No broker calls are made.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent
FEATURE_ROOT = PROJECT_ROOT / "data" / "all68_weekly_move_study" / "features"
REPORT_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager" / "reports"
PIPELINE_VERSION = "reference_strategy_mimic_v1"


@dataclass(frozen=True)
class ReferenceStrategy:
    pair: str
    strategy_id: str
    timeframe: str
    equity: float
    trades: int
    win_rate: float
    net_pips: float
    cost_pips: float


REFERENCE_STRATEGIES: list[ReferenceStrategy] = [
    ReferenceStrategy("EUR_USD", "M5_EV_091", "M5", 121.33, 75, 0.89, 213.0, 30.0),
    ReferenceStrategy("GBP_USD", "M5_EV_091", "M5", 128.03, 64, 0.88, 280.0, 25.0),
    ReferenceStrategy("USD_JPY", "M15_ATR_032", "M15", 114.31, 37, 0.73, 143.0, 15.0),
    ReferenceStrategy("AUD_USD", "EV_091", "H1", 106.13, 11, 0.91, 61.0, 6.0),
    ReferenceStrategy("USD_CAD", "EV_091", "H1", 111.65, 18, 0.89, 117.0, 6.0),
    ReferenceStrategy("USD_CHF", "EV_091", "H1", 104.69, 9, 0.78, 47.0, 8.0),
    ReferenceStrategy("NZD_USD", "EV_091", "H1", 102.86, 9, 0.78, 29.0, 7.0),
]


def json_safe(value: Any) -> Any:
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=json_safe),
        encoding="utf-8",
    )


def resolve_output_paths(output_prefix: str) -> tuple[Path, Path]:
    """Resolve output-prefix as either a report name or an explicit path.

    Historically this script treated every prefix as relative to REPORT_ROOT.
    That is fine for ``--output-prefix latest_reference_strategy_mimic`` but it
    double-prepends the reports directory when callers pass a path-like prefix
    such as ``data/oanda_training_manager/reports/run_...``.  Support both
    forms so background research launches can use timestamped report paths
    without failing at the final write step.
    """
    raw = Path(str(output_prefix or "latest_reference_strategy_mimic"))
    if raw.is_absolute():
        base = raw
    elif raw.parent != Path("."):
        base = PROJECT_ROOT / raw
    else:
        base = REPORT_ROOT / raw
    if base.suffix.lower() in {".csv", ".json"}:
        base = base.with_suffix("")
    base.parent.mkdir(parents=True, exist_ok=True)
    return base.with_suffix(".csv"), base.with_suffix(".json")


def timeframe_minutes(timeframe: str) -> int:
    text = timeframe.upper().strip()
    if text.startswith("M"):
        return int(text[1:])
    if text.startswith("H"):
        return int(text[1:]) * 60
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def load_pair_frame(pair: str, timeframe: str, *, since: str = "") -> pd.DataFrame:
    path = FEATURE_ROOT / f"{pair}.parquet"
    columns = [
        "close",
        "spread_pips",
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
        "atr240_pips",
        "future_move_pips_15",
        "future_long_net_pips_15",
        "future_short_net_pips_15",
        "future_move_pips_30",
        "future_long_net_pips_30",
        "future_short_net_pips_30",
        "future_move_pips_60",
        "future_long_net_pips_60",
        "future_short_net_pips_60",
        "future_move_pips_120",
        "future_long_net_pips_120",
        "future_short_net_pips_120",
        "future_move_pips_240",
        "future_long_net_pips_240",
        "future_short_net_pips_240",
        "strength_gap_15",
        "strength_gap_60",
    ]
    frame = pd.read_parquet(path, columns=columns).copy()
    frame.index = pd.to_datetime(frame.index, errors="coerce", utc=True)
    frame = frame.sort_index()
    frame["time_utc"] = frame.index
    minutes = timeframe_minutes(timeframe)
    if minutes > 5:
        frame = frame[
            (frame["time_utc"].dt.minute % minutes == 0)
            if minutes < 60
            else (frame["time_utc"].dt.minute == 0)
        ].copy()
    if since:
        frame = frame[frame["time_utc"] >= pd.Timestamp(since, tz="UTC")]
    numeric = [column for column in frame.columns if column != "time_utc"]
    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["time_utc", "close", "spread_pips", "atr240_pips"]).reset_index(drop=True)


def non_overlapping_positions(times: pd.Series, mask: np.ndarray, cooldown_minutes: int) -> np.ndarray:
    chosen: list[int] = []
    next_allowed: pd.Timestamp | None = None
    for position in np.flatnonzero(mask):
        timestamp = pd.Timestamp(times.iloc[position])
        if next_allowed is None or timestamp >= next_allowed:
            chosen.append(int(position))
            next_allowed = timestamp + pd.Timedelta(minutes=cooldown_minutes)
    return np.asarray(chosen, dtype=int)


def ev_score(frame: pd.DataFrame, timeframe: str, horizon: int) -> tuple[np.ndarray, np.ndarray]:
    m5 = frame["momentum_5_atr"].to_numpy(dtype=float)
    m15 = frame["momentum_15_atr"].to_numpy(dtype=float)
    m30 = frame["momentum_30_atr"].to_numpy(dtype=float)
    m60 = frame["momentum_60_atr"].to_numpy(dtype=float)
    slope = frame["sma30_slope_5_atr"].to_numpy(dtype=float)
    stack = frame["sma7_minus_8_atr"].to_numpy(dtype=float)
    strength = (
        frame.get("strength_gap_15", pd.Series(0.0, index=frame.index)).to_numpy(dtype=float)
        * 10_000.0
    )
    if timeframe.upper() == "M5":
        raw = 0.42 * m5 + 0.34 * m15 + 0.18 * m30 + 0.06 * slope + 0.03 * strength
    elif timeframe.upper() == "M15":
        raw = 0.20 * m5 + 0.35 * m15 + 0.28 * m30 + 0.12 * m60 + 0.05 * slope
    else:
        raw = 0.18 * m15 + 0.34 * m30 + 0.34 * m60 + 0.10 * slope + 0.04 * stack
    atr = frame["atr240_pips"].to_numpy(dtype=float)
    spread = np.maximum(frame["spread_pips"].to_numpy(dtype=float), 0.1)
    expected_pips = np.abs(raw) * atr * math.sqrt(max(horizon, 5) / 60.0)
    score = expected_pips / spread
    direction = np.sign(raw)
    return direction, score


def atr_score(frame: pd.DataFrame, timeframe: str, horizon: int) -> tuple[np.ndarray, np.ndarray]:
    m15 = frame["momentum_15_atr"].to_numpy(dtype=float)
    m30 = frame["momentum_30_atr"].to_numpy(dtype=float)
    m60 = frame["momentum_60_atr"].to_numpy(dtype=float)
    accel = frame["acceleration_15_atr"].to_numpy(dtype=float)
    if timeframe.upper() == "M15":
        raw = 0.55 * m15 + 0.30 * m30 + 0.15 * accel
    else:
        raw = 0.35 * m15 + 0.35 * m30 + 0.30 * m60
    direction = np.sign(raw)
    score = np.abs(raw)
    return direction, score


def evaluate_rule(
    frame: pd.DataFrame,
    *,
    strategy_family: str,
    timeframe: str,
    horizon: int,
    score_threshold: float,
    max_spread_ratio: float,
    min_alignment: int,
    cooldown_minutes: int,
    direction_polarity: int = 1,
) -> dict[str, Any]:
    if "ATR" in strategy_family.upper():
        direction, score = atr_score(frame, timeframe, horizon)
    else:
        direction, score = ev_score(frame, timeframe, horizon)
    direction = direction * int(direction_polarity)
    m5 = frame["momentum_5_atr"].to_numpy(dtype=float)
    m15 = frame["momentum_15_atr"].to_numpy(dtype=float)
    m30 = frame["momentum_30_atr"].to_numpy(dtype=float)
    m60 = frame["momentum_60_atr"].to_numpy(dtype=float)
    alignment = (
        (np.sign(m5) == direction).astype(int)
        + (np.sign(m15) == direction).astype(int)
        + (np.sign(m30) == direction).astype(int)
        + (np.sign(m60) == direction).astype(int)
    )
    spread_ratio = frame["spread_ratio_60"].to_numpy(dtype=float)
    mask = (
        np.isfinite(score)
        & (direction != 0)
        & (score >= score_threshold)
        & (spread_ratio <= max_spread_ratio)
        & (alignment >= min_alignment)
    )
    positions = non_overlapping_positions(
        frame["time_utc"],
        mask,
        cooldown_minutes=cooldown_minutes,
    )
    if len(positions) == 0:
        return metrics([], [], horizon, params={
            "strategy_family": strategy_family,
            "timeframe": timeframe,
            "horizon": horizon,
            "score_threshold": score_threshold,
            "max_spread_ratio": max_spread_ratio,
            "min_alignment": min_alignment,
            "cooldown_minutes": cooldown_minutes,
            "direction_polarity": int(direction_polarity),
        })
    selected_direction = direction[positions]
    long_net = frame[f"future_long_net_pips_{horizon}"].to_numpy(dtype=float)[positions]
    short_net = frame[f"future_short_net_pips_{horizon}"].to_numpy(dtype=float)[positions]
    net = np.where(selected_direction > 0, long_net, short_net)
    costs = frame["spread_pips"].to_numpy(dtype=float)[positions]
    valid = np.isfinite(net) & np.isfinite(costs)
    return metrics(net[valid], costs[valid], horizon, params={
        "strategy_family": strategy_family,
        "timeframe": timeframe,
        "horizon": horizon,
        "score_threshold": score_threshold,
        "max_spread_ratio": max_spread_ratio,
        "min_alignment": min_alignment,
        "cooldown_minutes": cooldown_minutes,
        "direction_polarity": int(direction_polarity),
    })


def metrics(net_values: Sequence[float], costs: Sequence[float], horizon: int, *, params: dict[str, Any]) -> dict[str, Any]:
    net = np.asarray(net_values, dtype=float)
    cost = np.asarray(costs, dtype=float)
    if len(net) == 0:
        return {
            **params,
            "trades": 0,
            "equity": 100.0,
            "net_pips": 0.0,
            "avg_net_per_trade": 0.0,
            "cost_pips": 0.0,
            "cost_ratio": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_pips": 0.0,
        }
    wins = net[net > 0].sum()
    losses = -net[net < 0].sum()
    curve = np.cumsum(net)
    drawdown = np.maximum.accumulate(np.r_[0.0, curve])[1:] - curve
    total_net = float(net.sum())
    total_cost = float(cost.sum())
    return {
        **params,
        "trades": int(len(net)),
        "equity": 100.0 + total_net / 10.0,
        "net_pips": total_net,
        "avg_net_per_trade": float(net.mean()),
        "cost_pips": total_cost,
        "cost_ratio": total_cost / max(abs(total_net) + total_cost, 1e-9),
        "win_rate": float((net > 0).mean()),
        "profit_factor": float(wins / losses) if losses > 0 else (float("inf") if wins > 0 else 0.0),
        "max_drawdown_pips": float(drawdown.max()) if len(drawdown) else 0.0,
    }


def grid_for(reference: ReferenceStrategy, *, fast: bool = False) -> Iterable[dict[str, Any]]:
    timeframe = reference.timeframe.upper()
    if fast and timeframe == "M5":
        horizons = [30, 60]
        thresholds = [0.91, 1.35, 2.00, 3.00, 4.50, 6.00]
        spread_ratios = [1.25, 1.50]
        alignments = [4]
        cooldown_mults = [2, 4, 8]
    elif fast and timeframe == "M15":
        horizons = [60, 120]
        thresholds = [0.32, 0.60, 1.00, 1.50, 2.25, 3.00]
        spread_ratios = [1.25, 1.50]
        alignments = [4]
        cooldown_mults = [2, 4, 8]
    elif fast:
        horizons = [120, 240]
        thresholds = [0.91, 1.35, 2.00, 3.00, 4.50, 6.00]
        spread_ratios = [1.25, 1.50]
        alignments = [4]
        cooldown_mults = [2, 4, 8]
    elif timeframe == "M5":
        horizons = [15, 30, 60, 120]
        thresholds = [0.65, 0.80, 0.91, 1.10, 1.35, 1.65, 2.00, 2.50, 3.00, 4.50, 6.00, 8.00]
        spread_ratios = [1.25, 1.50, 1.80, 2.25, 3.00]
        alignments = [2, 3, 4]
        cooldown_mults = [1, 2, 4]
    elif timeframe == "M15":
        horizons = [30, 60, 120, 240]
        thresholds = [0.32, 0.45, 0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50, 3.00, 4.00]
        spread_ratios = [1.25, 1.50, 1.80, 2.25, 3.00]
        alignments = [2, 3, 4]
        cooldown_mults = [1, 2, 4]
    else:
        horizons = [60, 120, 240]
        thresholds = [0.55, 0.70, 0.91, 1.10, 1.35, 1.65, 2.00, 2.50, 3.50, 5.00, 7.00]
        spread_ratios = [1.25, 1.50, 1.80, 2.25, 3.00]
        alignments = [2, 3, 4]
        cooldown_mults = [1, 2, 4]
    family = reference.strategy_id
    for horizon, threshold, spread_ratio, min_alignment, cooldown_mult, direction_polarity in itertools.product(
        horizons,
        thresholds,
        spread_ratios,
        alignments,
        cooldown_mults,
        [1, -1],
    ):
        yield {
            "strategy_family": family,
            "timeframe": timeframe,
            "horizon": horizon,
            "score_threshold": threshold,
            "max_spread_ratio": spread_ratio,
            "min_alignment": min_alignment,
            "cooldown_minutes": horizon * cooldown_mult,
            "direction_polarity": direction_polarity,
        }


def score_for_selection(result: dict[str, Any], reference: ReferenceStrategy | None = None) -> float:
    trades = float(result.get("trades", 0))
    if trades < 3:
        return -1e18
    pf = min(float(result.get("profit_factor", 0.0)), 5.0)
    win = float(result.get("win_rate", 0.0))
    net = float(result.get("net_pips", 0.0))
    avg = float(result.get("avg_net_per_trade", 0.0))
    score = net + 25.0 * pf + 50.0 * win + 10.0 * avg
    if reference is not None:
        target_avg = max(abs(reference.net_pips) / max(reference.trades, 1), 1.0)
        trade_penalty = abs(trades - reference.trades) * target_avg
        overtrade_penalty = max(trades - reference.trades * 1.75, 0.0) * target_avg * 2.0
        undertrade_penalty = max(reference.trades * 0.35 - trades, 0.0) * target_avg * 2.0
        score -= trade_penalty + overtrade_penalty + undertrade_penalty
    return score


def delta_metrics(result: dict[str, Any], reference: ReferenceStrategy) -> dict[str, float]:
    return {
        "delta_equity": float(result.get("equity", 100.0)) - reference.equity,
        "delta_trades": float(result.get("trades", 0)) - reference.trades,
        "delta_win_rate": float(result.get("win_rate", 0.0)) - reference.win_rate,
        "delta_net_pips": float(result.get("net_pips", 0.0)) - reference.net_pips,
        "delta_cost_pips": float(result.get("cost_pips", 0.0)) - reference.cost_pips,
    }


def evaluate_reference(reference: ReferenceStrategy, *, since: str = "", fast: bool = False) -> dict[str, Any]:
    frame = load_pair_frame(reference.pair, reference.timeframe, since=since)
    if frame.empty:
        raise RuntimeError(f"No frame rows for {reference}")
    split_time = frame["time_utc"].quantile(0.70)
    calibration = frame[frame["time_utc"] <= split_time].reset_index(drop=True)
    test = frame[frame["time_utc"] > split_time].reset_index(drop=True)
    grid = list(grid_for(reference, fast=fast))
    calibration_rows = []
    full_rows = []
    for params in grid:
        calibration_result = evaluate_rule(calibration, **params)
        calibration_rows.append(calibration_result)
        full_rows.append(evaluate_rule(frame, **params))
    best_calibration = max(calibration_rows, key=lambda row: score_for_selection(row, reference))
    test_result = evaluate_rule(test, **{
        key: best_calibration[key]
        for key in [
            "strategy_family",
            "timeframe",
            "horizon",
            "score_threshold",
            "max_spread_ratio",
            "min_alignment",
            "cooldown_minutes",
            "direction_polarity",
        ]
    })
    best_full = max(full_rows, key=lambda row: score_for_selection(row, reference))
    test_result = {
        **test_result,
        **{f"reference_{key}": value for key, value in asdict(reference).items()},
        **{f"calibration_{key}": best_calibration.get(key) for key in [
            "trades",
            "equity",
            "net_pips",
            "win_rate",
            "cost_pips",
            "profit_factor",
        ]},
        **delta_metrics(test_result, reference),
        "selection_mode": "calibration_then_test",
        "period_start": frame["time_utc"].min(),
        "period_end": frame["time_utc"].max(),
        "split_time": split_time,
    }
    best_full = {
        **best_full,
        **{f"reference_{key}": value for key, value in asdict(reference).items()},
        **delta_metrics(best_full, reference),
        "selection_mode": "full_window_best_fit",
        "period_start": frame["time_utc"].min(),
        "period_end": frame["time_utc"].max(),
        "split_time": split_time,
    }
    return {
        "reference": asdict(reference),
        "frame_rows": len(frame),
        "calibration_rows": len(calibration),
        "test_rows": len(test),
        "calibration_selected_test": test_result,
        "full_window_best_fit": best_full,
    }


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    test_rows = [item["calibration_selected_test"] for item in results]
    full_rows = [item["full_window_best_fit"] for item in results]

    def aggregate(rows: list[dict[str, Any]]) -> dict[str, Any]:
        trades = sum(float(row.get("trades", 0)) for row in rows)
        net = sum(float(row.get("net_pips", 0)) for row in rows)
        cost = sum(float(row.get("cost_pips", 0)) for row in rows)
        wins = sum(float(row.get("trades", 0)) * float(row.get("win_rate", 0)) for row in rows)
        return {
            "trades": trades,
            "net_pips": net,
            "equity": 100.0 + net / 10.0,
            "avg_net_per_trade": net / max(trades, 1.0),
            "cost_pips": cost,
            "cost_ratio": cost / max(abs(net) + cost, 1e-9),
            "weighted_win_rate": wins / max(trades, 1.0),
        }

    reference_trades = sum(item.trades for item in REFERENCE_STRATEGIES)
    reference_net = sum(item.net_pips for item in REFERENCE_STRATEGIES)
    reference_cost = sum(item.cost_pips for item in REFERENCE_STRATEGIES)
    return {
        "pipeline_version": PIPELINE_VERSION,
        "reference_summary": {
            "listed_trades": reference_trades,
            "listed_net_pips": reference_net,
            "listed_equity": 100.0 + reference_net / 10.0,
            "listed_cost_pips": reference_cost,
            "listed_avg_net_per_trade": reference_net / max(reference_trades, 1),
            "note": (
                "The pasted Global row has more trades than the seven listed "
                "rows, so listed totals are only for the rows provided here."
            ),
        },
        "calibration_selected_test_summary": aggregate(test_rows),
        "full_window_best_fit_summary": aggregate(full_rows),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", default="", help="Optional UTC start timestamp, e.g. 2025-01-01")
    parser.add_argument("--fast", action="store_true", help="Use a smaller mimic grid for quick feedback.")
    parser.add_argument("--output-prefix", default="latest_reference_strategy_mimic")
    args = parser.parse_args()

    results = []
    rows = []
    for reference in REFERENCE_STRATEGIES:
        print(
            f"[mimic] {reference.pair} {reference.strategy_id} {reference.timeframe}",
            flush=True,
        )
        item = evaluate_reference(reference, since=args.since, fast=args.fast)
        results.append(item)
        rows.append(item["calibration_selected_test"])
        rows.append(item["full_window_best_fit"])

    csv_path, json_path = resolve_output_paths(args.output_prefix)
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    payload = {
        "generated_utc": pd.Timestamp.utcnow(),
        "since": args.since,
        "fast": args.fast,
        "csv": str(csv_path),
        "summary": summarize(results),
        "results": results,
        "method": {
            "exact_rule_available": False,
            "description": (
                "Mimic grid over EV-style and ATR-style causal rule families. "
                "Use original rule definitions/export to reproduce exact IDs."
            ),
        },
    }
    write_json(json_path, payload)
    print(json.dumps(payload["summary"], indent=2, default=json_safe), flush=True)
    print(f"json={json_path}", flush=True)
    print(f"csv={csv_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
