#!/usr/bin/env python3
"""Mine support/resistance trendline trade logs for more profitable filters.

This optimizer does not rerun candle-level backtests. It loads the per-trade
CSV outputs produced by ``oanda_sr_trendline_backtest.py`` across timeframes
and searches entry-known filters:

- timeframe, pair, pair group, direction, weekday, and UTC hour subsets;
- trendline quality and timing thresholds;
- train-selected pair/timeframe baskets.

Every candidate is evaluated on the original chronological train/test split
stored in each timeframe JSON. The intent is to find defensible candidates for
further candle-level reruns, not to declare a production strategy.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
SWEEP_ROOT = ROOT / "data" / "oanda_training_manager" / "reports" / "sr_trendline_timeframe_sweep"
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports" / "sr_trendline_optimizer"
TIMEFRAMES = ("1min", "5min", "15min", "30min", "1h", "4h", "1d")


@dataclass(frozen=True)
class Candidate:
    name: str
    family: str
    description: str
    mask: np.ndarray
    metadata: dict[str, Any]


@dataclass(frozen=True)
class EvalContext:
    gross_return: np.ndarray
    risk_return: np.ndarray
    is_test: np.ndarray


def finite_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    return number if math.isfinite(number) else default


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=json_safe), encoding="utf-8")
    os.replace(tmp, path)


def write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row})
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


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


def load_timeframe(timeframe: str, sweep_root: Path) -> pd.DataFrame:
    folder = sweep_root / timeframe
    trades_path = folder / "latest_sr_trendline_trades.csv"
    json_path = folder / "latest_sr_trendline_backtest.json"
    if not trades_path.exists() or not json_path.exists():
        raise FileNotFoundError(f"Missing sweep outputs for {timeframe}: {folder}")
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    split_time = pd.Timestamp(payload["split_time_utc"])
    frame = pd.read_csv(trades_path)
    frame["timeframe"] = timeframe
    frame["entry_time"] = pd.to_datetime(frame["entry_time"], utc=True, errors="coerce")
    frame["exit_time"] = pd.to_datetime(frame["exit_time"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["entry_time"])
    frame["is_test"] = frame["entry_time"] > split_time
    frame["hour_utc"] = frame["entry_time"].dt.hour.astype("int16")
    frame["weekday"] = frame["entry_time"].dt.dayofweek.astype("int16")
    frame["month"] = frame["entry_time"].dt.strftime("%Y-%m")
    frame["risk_return"] = pd.to_numeric(frame["risk_price"], errors="coerce") / pd.to_numeric(
        frame["entry_price"], errors="coerce"
    )
    frame["risk_atr"] = pd.to_numeric(frame["risk_price"], errors="coerce") / pd.to_numeric(
        frame["atr"], errors="coerce"
    )
    frame["level_entry_dist_atr"] = (
        pd.to_numeric(frame["entry_price"], errors="coerce")
        - pd.to_numeric(frame["support_resistance_level"], errors="coerce")
    ).abs() / pd.to_numeric(frame["atr"], errors="coerce")
    frame["gross_return"] = pd.to_numeric(frame["gross_return_bps"], errors="coerce") / 10_000.0
    frame["base_net_r"] = pd.to_numeric(frame["net_r"], errors="coerce")
    return frame


def load_trades(sweep_root: Path, timeframes: Sequence[str]) -> pd.DataFrame:
    frames = []
    for timeframe in timeframes:
        print(f"[load] {timeframe}")
        frames.append(load_timeframe(timeframe, sweep_root))
    out = pd.concat(frames, ignore_index=True)
    numeric_columns = [
        "base_net_r",
        "gross_return",
        "risk_return",
        "risk_atr",
        "level_entry_dist_atr",
        "line_touches",
        "line_touch_error_atr",
        "trendline_to_level_bars",
        "level_break_to_entry_bars",
        "hold_bars",
        "net_return_bps",
        "net_pips",
    ]
    for column in numeric_columns:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    out = out.replace([np.inf, -np.inf], np.nan).dropna(
        subset=["base_net_r", "gross_return", "risk_return", "risk_atr"]
    )
    return out.reset_index(drop=True)


def net_r_for_cost(frame: pd.DataFrame, cost_bps: float) -> np.ndarray:
    gross = frame["gross_return"].to_numpy(dtype=float)
    risk = frame["risk_return"].to_numpy(dtype=float)
    return (gross - float(cost_bps) / 10_000.0) / risk


def build_eval_context(frame: pd.DataFrame) -> EvalContext:
    return EvalContext(
        gross_return=frame["gross_return"].to_numpy(dtype=float),
        risk_return=frame["risk_return"].to_numpy(dtype=float),
        is_test=frame["is_test"].to_numpy(dtype=bool),
    )


def summarize_values(values: np.ndarray) -> dict[str, Any]:
    values = values[np.isfinite(values)]
    if len(values) == 0:
        return {
            "trades": 0,
            "total_r": 0.0,
            "avg_r": 0.0,
            "median_r": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_r": 0.0,
        }
    wins = values[values > 0.0]
    losses = -values[values < 0.0]
    equity = np.cumsum(values)
    peak = np.maximum.accumulate(np.insert(equity, 0, 0.0))[1:]
    dd = peak - equity
    return {
        "trades": int(len(values)),
        "total_r": float(values.sum()),
        "avg_r": float(values.mean()),
        "median_r": float(np.median(values)),
        "win_rate": float((values > 0.0).mean()),
        "profit_factor": float(wins.sum() / losses.sum()) if losses.sum() > 0.0 else float("inf"),
        "max_drawdown_r": float(dd.max()) if len(dd) else 0.0,
    }


def summarize_mask(context: EvalContext, mask: np.ndarray, *, cost_bps: float = 0.0) -> dict[str, Any]:
    if not np.any(mask):
        return {
            "full": summarize_values(np.array([], dtype=float)),
            "train": summarize_values(np.array([], dtype=float)),
            "test": summarize_values(np.array([], dtype=float)),
        }
    values = (context.gross_return[mask] - float(cost_bps) / 10_000.0) / context.risk_return[mask]
    is_test = context.is_test[mask]
    return {
        "full": summarize_values(values),
        "train": summarize_values(values[~is_test]),
        "test": summarize_values(values[is_test]),
    }


def flatten_summary(prefix: str, summary: dict[str, Any]) -> dict[str, Any]:
    return {f"{prefix}_{key}": value for key, value in summary.items()}


def candidate_row(context: EvalContext, candidate: Candidate, *, cost_bps: float = 0.0) -> dict[str, Any]:
    summaries = summarize_mask(context, candidate.mask, cost_bps=cost_bps)
    row: dict[str, Any] = {
        "name": candidate.name,
        "family": candidate.family,
        "description": candidate.description,
        "cost_bps": cost_bps,
        **candidate.metadata,
    }
    for split in ["full", "train", "test"]:
        row.update(flatten_summary(split, summaries[split]))
    return row


def selected_token_series(frame: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    if len(columns) == 1:
        return frame[columns[0]].astype(str)
    return frame[list(columns)].astype(str).agg("|".join, axis=1)


def group_selection_candidates(
    frame: pd.DataFrame,
    columns: Sequence[str],
    *,
    min_train_trades: int,
    thresholds: Sequence[float],
    top_ks: Sequence[int],
) -> list[Candidate]:
    token = selected_token_series(frame, columns)
    train = frame.loc[~frame["is_test"]].copy()
    train_token = token.loc[~frame["is_test"]]
    grouped = train.groupby(train_token)["base_net_r"].agg(["count", "sum", "mean"])
    candidates: list[Candidate] = []
    for threshold in thresholds:
        selected = grouped[(grouped["count"] >= min_train_trades) & (grouped["mean"] >= threshold)]
        if selected.empty:
            continue
        values = set(selected.index.astype(str))
        mask = token.isin(values).to_numpy()
        name = f"group_{'_'.join(columns)}_mean_ge_{threshold:g}"
        candidates.append(
            Candidate(
                name=name,
                family="train_selected_groups",
                description=f"Train-selected {columns} with train avg R >= {threshold:g}",
                mask=mask,
                metadata={
                    "selector_columns": "|".join(columns),
                    "selector_mode": "mean_threshold",
                    "selector_threshold": threshold,
                    "selected_groups": int(len(values)),
                    "selected_values": ";".join(sorted(values)[:120]),
                },
            )
        )
    ranked = grouped[grouped["count"] >= min_train_trades].sort_values(["mean", "sum"], ascending=False)
    for top_k in top_ks:
        selected = ranked.head(top_k)
        if selected.empty:
            continue
        values = set(selected.index.astype(str))
        mask = token.isin(values).to_numpy()
        name = f"group_{'_'.join(columns)}_top_{top_k}"
        candidates.append(
            Candidate(
                name=name,
                family="train_selected_groups",
                description=f"Top {top_k} train {columns} groups by avg R",
                mask=mask,
                metadata={
                    "selector_columns": "|".join(columns),
                    "selector_mode": "top_k_avg",
                    "selector_threshold": "",
                    "selected_groups": int(len(values)),
                    "selected_values": ";".join(selected.index.astype(str).tolist()),
                },
            )
        )
    return candidates


def numeric_filter_candidates(frame: pd.DataFrame, *, preset: str) -> Iterable[Candidate]:
    if preset == "none":
        return
    tf_values = ["ALL", *TIMEFRAMES]
    if preset == "wide":
        directions = ["ALL", "LONG", "SHORT"]
        groups = ["ALL", "major_cross", "usd_major", "exotic_or_regional"]
        line_touches_min = [3, 4, 5, 6]
        line_error_max = [0.20, 0.30, 0.40, 0.60, 0.90]
        trend_to_level_max = [1, 2, 4, 8, 12, 24]
        break_to_entry_max = [1, 2, 4, 8, 12, 24]
        risk_atr_max = [0.50, 0.75, 1.00, 1.50, 2.00, 2.75]
        dist_atr_max = [0.20, 0.35, 0.50, 0.75, 1.00]
    else:
        directions = ["ALL", "LONG", "SHORT"]
        groups = ["ALL"]
        line_touches_min = [3, 4, 5]
        line_error_max = [0.30, 0.60, 0.90]
        trend_to_level_max = [2, 4, 8, 24]
        break_to_entry_max = [1, 2, 4, 8, 24]
        risk_atr_max = [0.75, 1.50, 2.75]
        dist_atr_max = [0.35, 0.75, 1.00]

    tf_col = frame["timeframe"].astype(str).to_numpy()
    dir_col = frame["direction"].astype(str).to_numpy()
    group_col = frame["group"].astype(str).to_numpy()
    touches = frame["line_touches"].to_numpy(dtype=float)
    line_error = frame["line_touch_error_atr"].to_numpy(dtype=float)
    trend_to_level = frame["trendline_to_level_bars"].to_numpy(dtype=float)
    break_to_entry = frame["level_break_to_entry_bars"].to_numpy(dtype=float)
    risk_atr = frame["risk_atr"].to_numpy(dtype=float)
    dist_atr = frame["level_entry_dist_atr"].to_numpy(dtype=float)

    for timeframe in tf_values:
        tf_mask = np.ones(len(frame), dtype=bool) if timeframe == "ALL" else tf_col == timeframe
        for direction in directions:
            dir_mask = np.ones(len(frame), dtype=bool) if direction == "ALL" else dir_col == direction
            for group in groups:
                group_mask = np.ones(len(frame), dtype=bool) if group == "ALL" else group_col == group
                base = tf_mask & dir_mask & group_mask
                if int(base.sum()) < 50:
                    continue
                for touch_min in line_touches_min:
                    touch_mask = base & (touches >= touch_min)
                    if int(touch_mask.sum()) < 50:
                        continue
                    for error_max in line_error_max:
                        error_mask = touch_mask & (line_error <= error_max)
                        if int(error_mask.sum()) < 50:
                            continue
                        for trend_max in trend_to_level_max:
                            trend_mask = error_mask & (trend_to_level <= trend_max)
                            if int(trend_mask.sum()) < 50:
                                continue
                            for entry_max in break_to_entry_max:
                                entry_mask = trend_mask & (break_to_entry <= entry_max)
                                if int(entry_mask.sum()) < 50:
                                    continue
                                for risk_max in risk_atr_max:
                                    risk_mask = entry_mask & (risk_atr <= risk_max)
                                    if int(risk_mask.sum()) < 50:
                                        continue
                                    for dist_max in dist_atr_max:
                                        mask = risk_mask & (dist_atr <= dist_max)
                                        if int(mask.sum()) < 50:
                                            continue
                                        name = (
                                            f"grid_tf={timeframe}_dir={direction}_group={group}_"
                                            f"touch>={touch_min}_err<={error_max}_tl<={trend_max}_"
                                            f"pull<={entry_max}_risk<={risk_max}_dist<={dist_max}"
                                        )
                                        yield Candidate(
                                            name=name,
                                            family="numeric_grid",
                                            description="Entry-known numeric quality/timing filter",
                                            mask=mask,
                                            metadata={
                                                "timeframe_filter": timeframe,
                                                "direction_filter": direction,
                                                "group_filter": group,
                                                "line_touches_min": touch_min,
                                                "line_error_max": error_max,
                                                "trendline_to_level_max": trend_max,
                                                "level_break_to_entry_max": entry_max,
                                                "risk_atr_max": risk_max,
                                                "level_entry_dist_atr_max": dist_max,
                                            },
                                        )


def session_candidates(frame: pd.DataFrame) -> Iterable[Candidate]:
    tf_col = frame["timeframe"].astype(str)
    hour = frame["hour_utc"].astype(int)
    weekday = frame["weekday"].astype(int)
    direction = frame["direction"].astype(str)
    sessions = {
        "asia_utc_00_07": set(range(0, 8)),
        "london_utc_07_15": set(range(7, 16)),
        "ny_utc_12_20": set(range(12, 21)),
        "overlap_utc_12_15": set(range(12, 16)),
        "not_friday": set(range(0, 4)),
        "tue_wed_thu": {1, 2, 3},
    }
    for timeframe in ["ALL", *TIMEFRAMES]:
        tf_mask = np.ones(len(frame), dtype=bool) if timeframe == "ALL" else (tf_col == timeframe).to_numpy()
        for side in ["ALL", "LONG", "SHORT"]:
            side_mask = np.ones(len(frame), dtype=bool) if side == "ALL" else (direction == side).to_numpy()
            for session, allowed in sessions.items():
                if session.startswith("not_") or session == "tue_wed_thu":
                    sess_mask = weekday.isin(allowed).to_numpy()
                else:
                    sess_mask = hour.isin(allowed).to_numpy()
                mask = tf_mask & side_mask & sess_mask
                yield Candidate(
                    name=f"session_tf={timeframe}_dir={side}_{session}",
                    family="session",
                    description=f"{timeframe} {side} {session}",
                    mask=mask,
                    metadata={"timeframe_filter": timeframe, "direction_filter": side, "session_filter": session},
                )


def keep_candidate(row: dict[str, Any], *, min_train: int, min_test: int) -> bool:
    if int(row.get("train_trades", 0)) < min_train:
        return False
    if int(row.get("test_trades", 0)) < min_test:
        return False
    return True


def rank_key(row: dict[str, Any]) -> tuple[float, float, float, float]:
    test_total = finite_float(row.get("test_total_r"))
    test_pf = finite_float(row.get("test_profit_factor"))
    full_total = finite_float(row.get("full_total_r"))
    train_total = finite_float(row.get("train_total_r"))
    return test_total, test_pf, full_total, train_total


def evaluate_candidates(
    context: EvalContext,
    candidates: Iterable[Candidate],
    *,
    min_train: int,
    min_test: int,
    cost_bps: float,
    max_rows: int = 250_000,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, candidate in enumerate(candidates, start=1):
        row = candidate_row(context, candidate, cost_bps=cost_bps)
        if keep_candidate(row, min_train=min_train, min_test=min_test):
            rows.append(row)
        if idx % 25_000 == 0:
            rows = sorted(rows, key=rank_key, reverse=True)[:max_rows]
            print(f"[eval] {idx:,} candidates, kept={len(rows):,}")
    return sorted(rows, key=rank_key, reverse=True)[:max_rows]


def best_cost_sensitivity(frame: pd.DataFrame, rows: Sequence[dict[str, Any]], output_dir: Path, limit: int) -> list[dict[str, Any]]:
    context = build_eval_context(frame)
    sensitivity: list[dict[str, Any]] = []
    row_by_name = {row["name"]: row for row in rows}
    candidates: list[Candidate] = []

    # Rebuild masks for the small final set from saved candidate definitions.
    numeric_rows = [row for row in rows[:limit] if row.get("family") == "numeric_grid"]
    for row in numeric_rows:
        mask = np.ones(len(frame), dtype=bool)
        if row.get("timeframe_filter") and row["timeframe_filter"] != "ALL":
            mask &= frame["timeframe"].astype(str).to_numpy() == str(row["timeframe_filter"])
        if row.get("direction_filter") and row["direction_filter"] != "ALL":
            mask &= frame["direction"].astype(str).to_numpy() == str(row["direction_filter"])
        if row.get("group_filter") and row["group_filter"] != "ALL":
            mask &= frame["group"].astype(str).to_numpy() == str(row["group_filter"])
        mask &= frame["line_touches"].to_numpy(dtype=float) >= finite_float(row.get("line_touches_min"), 3.0)
        mask &= frame["line_touch_error_atr"].to_numpy(dtype=float) <= finite_float(row.get("line_error_max"), 99.0)
        mask &= frame["trendline_to_level_bars"].to_numpy(dtype=float) <= finite_float(
            row.get("trendline_to_level_max"), 99.0
        )
        mask &= frame["level_break_to_entry_bars"].to_numpy(dtype=float) <= finite_float(
            row.get("level_break_to_entry_max"), 99.0
        )
        mask &= frame["risk_atr"].to_numpy(dtype=float) <= finite_float(row.get("risk_atr_max"), 99.0)
        mask &= frame["level_entry_dist_atr"].to_numpy(dtype=float) <= finite_float(
            row.get("level_entry_dist_atr_max"), 99.0
        )
        candidates.append(
            Candidate(
                name=str(row["name"]),
                family=str(row["family"]),
                description=str(row.get("description", "")),
                mask=mask,
                metadata={key: row[key] for key in row.keys() if key.endswith("_filter") or key.endswith("_max") or key.endswith("_min")},
            )
        )

    for candidate in candidates:
        for cost_bps in [0.0, 0.5, 1.0, 2.0]:
            row = candidate_row(context, candidate, cost_bps=cost_bps)
            sensitivity.append(row)
    write_csv(output_dir / "optimizer_cost_sensitivity.csv", sensitivity)
    return sensitivity


def markdown_report(rows: Sequence[dict[str, Any]], output_dir: Path) -> str:
    lines = [
        "# Support/Resistance Trendline Optimizer",
        "",
        "Candidates are selected on the chronological train split and ranked by test total R.",
        "",
        "| rank | family | name | train trades | train R | test trades | test R | test PF | full R | full PF |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for rank, row in enumerate(rows[:40], start=1):
        lines.append(
            f"| {rank} | {row.get('family', '')} | {row.get('name', '')[:90]} | "
            f"{int(row.get('train_trades', 0))} | {finite_float(row.get('train_total_r')):.2f} | "
            f"{int(row.get('test_trades', 0))} | {finite_float(row.get('test_total_r')):.2f} | "
            f"{finite_float(row.get('test_profit_factor')):.2f} | {finite_float(row.get('full_total_r')):.2f} | "
            f"{finite_float(row.get('full_profit_factor')):.2f} |"
        )
    lines.extend(
        [
            "",
            "## Files",
            "",
            f"- Candidate CSV: `{output_dir / 'optimizer_candidates.csv'}`",
            f"- Cost sensitivity CSV: `{output_dir / 'optimizer_cost_sensitivity.csv'}`",
            f"- JSON: `{output_dir / 'optimizer_summary.json'}`",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sweep-root", type=Path, default=SWEEP_ROOT)
    parser.add_argument("--output-dir", type=Path, default=REPORT_ROOT)
    parser.add_argument("--timeframes", nargs="*", default=list(TIMEFRAMES))
    parser.add_argument("--min-train", type=int, default=50)
    parser.add_argument("--min-test", type=int, default=25)
    parser.add_argument("--max-candidates", type=int, default=250_000)
    parser.add_argument("--grid-preset", choices=["none", "quick", "wide"], default="quick")
    parser.add_argument("--selection-cost-bps", type=float, default=0.0)
    parser.add_argument("--eval-cost-bps", type=float, default=0.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    frame = load_trades(args.sweep_root, args.timeframes)
    if float(args.selection_cost_bps) != 0.0:
        frame["base_net_r"] = (
            frame["gross_return"].to_numpy(dtype=float) - float(args.selection_cost_bps) / 10_000.0
        ) / frame["risk_return"].to_numpy(dtype=float)
    context = build_eval_context(frame)
    print(f"[load] combined trades={len(frame):,}")

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    candidates: list[Candidate] = []
    candidates.extend(session_candidates(frame))
    group_specs = [
        (["timeframe", "instrument"], 20),
        (["timeframe", "instrument", "direction"], 15),
        (["timeframe", "group"], 50),
        (["timeframe", "group", "direction"], 30),
        (["timeframe", "hour_utc"], 30),
        (["timeframe", "weekday"], 30),
        (["timeframe", "direction"], 50),
        (["instrument"], 50),
        (["instrument", "direction"], 30),
    ]
    for columns, min_train in group_specs:
        candidates.extend(
            group_selection_candidates(
                frame,
                columns,
                min_train_trades=min_train,
                thresholds=[0.02, 0.05, 0.10, 0.15, 0.20],
                top_ks=[1, 3, 5, 10, 20, 40],
            )
        )

    rows = evaluate_candidates(
        context,
        candidates,
        min_train=args.min_train,
        min_test=args.min_test,
        cost_bps=float(args.eval_cost_bps),
        max_rows=args.max_candidates,
    )
    print(f"[eval] non-grid kept={len(rows):,}")

    grid_rows: list[dict[str, Any]] = []
    if args.grid_preset != "none":
        grid_rows = evaluate_candidates(
            context,
            numeric_filter_candidates(frame, preset=args.grid_preset),
            min_train=args.min_train,
            min_test=args.min_test,
            cost_bps=float(args.eval_cost_bps),
            max_rows=args.max_candidates,
        )
        print(f"[eval] grid kept={len(grid_rows):,}")

    combined = sorted([*rows, *grid_rows], key=rank_key, reverse=True)[: args.max_candidates]
    write_csv(output_dir / "optimizer_candidates.csv", combined)
    sensitivity = best_cost_sensitivity(frame, combined, output_dir, limit=100)
    payload = {
        "trade_count": int(len(frame)),
        "timeframes": list(args.timeframes),
        "selection_cost_bps": float(args.selection_cost_bps),
        "eval_cost_bps": float(args.eval_cost_bps),
        "candidate_count": int(len(combined)),
        "top_candidates": combined[:100],
        "cost_sensitivity_rows": len(sensitivity),
        "candidate_csv": str(output_dir / "optimizer_candidates.csv"),
        "cost_sensitivity_csv": str(output_dir / "optimizer_cost_sensitivity.csv"),
        "summary_md": str(output_dir / "optimizer_summary.md"),
    }
    atomic_write_json(output_dir / "optimizer_summary.json", payload)
    (output_dir / "optimizer_summary.md").write_text(markdown_report(combined, output_dir), encoding="utf-8")
    print(json.dumps(payload, indent=2, sort_keys=True, default=json_safe))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
