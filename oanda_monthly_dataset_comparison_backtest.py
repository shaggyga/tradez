#!/usr/bin/env python3
"""Compare primary forecast model behavior across historical dataset months."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import pandas as pd

from oanda_broker_env_sweep import transform_candidates
from oanda_broker_style_portfolio_replay import PRIMARY_CONFIG, objective
from oanda_live_style_pair_portfolio_backtest import (
    generate_candidates,
    load_settings,
    parse_thresholds,
)
from oanda_primary_live_exact_backtest import run as run_exact_backtest


ROOT = Path(__file__).resolve().parent
REPORTS = ROOT / "data" / "oanda_training_manager" / "reports"
DEFAULT_SETTINGS = REPORTS / "all68_latest_hgb_reversal_pool_settings_technical_full_only.csv"
DEFAULT_OUTPUT = (
    REPORTS
    / f"monthly_dataset_comparison_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
)

TIMEFRAMES: Dict[str, Dict[str, str]] = {
    "m1": {
        "suffix": "",
        "feature_root": str(ROOT / "data" / "all68_weekly_move_study" / "features"),
    },
    "m30": {
        "suffix": "30m_step1",
        "feature_root": str(ROOT / "data" / "all68_weekly_move_study" / "features_30m"),
    },
    "h1": {
        "suffix": "1h_step1",
        "feature_root": str(ROOT / "data" / "all68_weekly_move_study" / "features_1h"),
    },
    "h4": {
        "suffix": "4h_step1",
        "feature_root": str(ROOT / "data" / "all68_weekly_move_study" / "features_4h"),
    },
}


def json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=json_default),
        encoding="utf-8",
    )


def month_bounds(month: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    text = month.strip()
    try:
        start = pd.Timestamp(f"{text}-01", tz="UTC")
    except Exception as exc:
        raise ValueError(f"Month must be YYYY-MM, got {month!r}") from exc
    end = start + pd.offsets.MonthBegin(1)
    return start, pd.Timestamp(end).tz_convert("UTC")


def parse_months(text: str) -> List[str]:
    raw = str(text or "").strip()
    if not raw:
        raw = "2026-01,2026-02,2026-03,2026-04,2026-05,2026-06"
    months = []
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        month_bounds(part)
        months.append(part)
    if not months:
        raise ValueError("At least one month is required")
    return sorted(dict.fromkeys(months))


def period_bounds(months: Sequence[str]) -> Tuple[pd.Timestamp, pd.Timestamp]:
    starts_ends = [month_bounds(month) for month in months]
    return min(start for start, _end in starts_ends), max(end for _start, end in starts_ends)


def parse_timeframes(text: str) -> List[str]:
    values = [part.strip().lower() for part in str(text or "").split(",") if part.strip()]
    if not values:
        values = ["m30", "h1", "h4"]
    unknown = sorted(set(values) - set(TIMEFRAMES))
    if unknown:
        raise ValueError(f"Unknown timeframe(s): {unknown}")
    return list(dict.fromkeys(values))


def set_timeframe_environment(timeframe: str, train_row_cap: int, n_jobs: int) -> None:
    cfg = TIMEFRAMES[timeframe]
    os.environ["OANDA_TECHNICAL_RESEARCH_DATASET_SUFFIX"] = cfg["suffix"]
    os.environ["OANDA_TECHNICAL_FEATURE_ROOT"] = cfg["feature_root"]
    os.environ["OANDA_TECHNICAL_RESEARCH_SAMPLE_STEP"] = "1"
    os.environ["OANDA_CONTINUOUS_RESEARCH_VALIDATION_MAX_TRAIN_ROWS_CAP"] = str(
        max(10_000, int(train_row_cap))
    )
    os.environ["OANDA_CONTINUOUS_RESEARCH_SCREEN_MAX_TRAIN_ROWS_CAP"] = str(
        max(10_000, min(int(train_row_cap), 30_000))
    )
    os.environ["OANDA_CONTINUOUS_RESEARCH_N_JOBS"] = str(max(1, int(n_jobs)))


def candidate_time_range(candidates: pd.DataFrame) -> Dict[str, str]:
    if candidates.empty:
        return {"candidate_start_utc": "", "candidate_end_utc": ""}
    return {
        "candidate_start_utc": pd.to_datetime(candidates["time_utc"], utc=True).min().isoformat(),
        "candidate_end_utc": pd.to_datetime(candidates["time_utc"], utc=True).max().isoformat(),
    }


def write_candidate_bundle(
    output_dir: Path,
    candidates: pd.DataFrame,
    thresholds: pd.DataFrame,
    model_runs: pd.DataFrame,
    skipped: List[Dict[str, Any]],
    metadata: Dict[str, Any],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    candidates.to_csv(output_dir / "candidate_rows.csv", index=False)
    thresholds.to_csv(output_dir / "pair_thresholds.csv", index=False)
    model_runs.to_csv(output_dir / "model_runs.csv", index=False)
    pd.DataFrame(skipped).to_csv(output_dir / "skipped.csv", index=False)
    write_json(output_dir / "metadata.json", metadata)


def sort_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    out = candidates.copy()
    out["time_utc"] = pd.to_datetime(out["time_utc"], utc=True)
    out["planned_exit_time"] = pd.to_datetime(out["planned_exit_time"], utc=True)
    return out.sort_values(
        ["time_utc", "rank_score", "probability"],
        ascending=[True, False, False],
    ).reset_index(drop=True)


def merge_streams(frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame()
    merged = pd.concat(non_empty, ignore_index=True)
    return sort_candidates(merged)


def run_month_replay(
    *,
    month: str,
    candidates: pd.DataFrame,
    output_dir: Path,
    config_path: Path,
    profile_name: str,
    start_equity: float,
) -> Dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = output_dir / "candidate_rows.csv"
    candidates.to_csv(candidate_path, index=False)
    if candidates.empty:
        row = {
            "month": month,
            "candidate_rows": 0,
            "opened_trades": 0,
            "return_pct": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_pct": 0.0,
            "margin_call_rows": 0,
            "output_dir": str(output_dir),
        }
        write_json(output_dir / "summary.json", row)
        return row
    summary = run_exact_backtest(
        output_dir,
        config_path=config_path,
        candidate_rows_csv=candidate_path,
        profile_name=profile_name,
        start_equity=start_equity,
    )
    return {
        "month": month,
        "objective": objective(summary),
        "output_dir": str(output_dir),
        **summary,
    }


def run(args: argparse.Namespace) -> pd.DataFrame:
    months = parse_months(args.months)
    timeframes = parse_timeframes(args.timeframes)
    period_start, period_end = period_bounds(months)
    output_root = args.output_root
    output_root.mkdir(parents=True, exist_ok=True)

    settings = load_settings(args.settings_csv, args.max_pairs)
    thresholds = parse_thresholds(args.thresholds)
    stream_frames: Dict[str, pd.DataFrame] = {}
    stream_rows: List[Dict[str, Any]] = []

    for timeframe in timeframes:
        set_timeframe_environment(timeframe, args.train_row_cap, args.n_jobs)
        candidates, pair_thresholds, model_runs, skipped = generate_candidates(
            settings,
            thresholds=thresholds,
            min_pair_calibration_rows=args.min_pair_calibration_rows,
            min_pair_calibration_trades=args.min_pair_calibration_trades,
            min_expected_move_atr=args.min_expected_move_atr,
            episode_aware=not args.no_episode_aware,
            skip_unsupported=not args.include_unsupported,
            min_train_rows=args.min_train_rows,
            min_calibration_rows=args.min_calibration_rows,
            min_test_rows=args.min_test_rows,
            candidate_start_utc=period_start,
            candidate_end_utc=period_end,
            all_test_weeks=True,
            holdout_weeks_override=args.holdout_weeks_override,
            train_lookback_days=args.train_lookback_days,
        )
        candidates = sort_candidates(candidates)
        if not candidates.empty:
            candidates["source_stream"] = timeframe
        stream_frames[timeframe] = candidates
        stream_dir = output_root / "streams" / timeframe
        metadata = {
            "timeframe": timeframe,
            "dataset_suffix": TIMEFRAMES[timeframe]["suffix"],
            "feature_root": TIMEFRAMES[timeframe]["feature_root"],
            "period_start_utc": period_start.isoformat(),
            "period_end_utc": period_end.isoformat(),
            "candidate_rows": int(len(candidates)),
            "pair_threshold_rows": int(len(pair_thresholds)),
            "model_run_rows": int(len(model_runs)),
            "skipped_count": int(len(skipped)),
            "train_row_cap": int(args.train_row_cap),
            "train_lookback_days": int(args.train_lookback_days),
            **candidate_time_range(candidates),
        }
        write_candidate_bundle(
            stream_dir,
            candidates,
            pair_thresholds,
            model_runs,
            skipped,
            metadata,
        )
        stream_rows.append(metadata)
        print(json.dumps(metadata, sort_keys=True, default=json_default), flush=True)

    write_json(output_root / "stream_summary.json", stream_rows)
    pd.DataFrame(stream_rows).to_csv(output_root / "stream_summary.csv", index=False)

    rows: List[Dict[str, Any]] = []
    period_candidates = transform_candidates(
        merge_streams(stream_frames.values()),
        "inverted" if args.direction_mode == "inverted" else "normal",
    )
    period_candidates = sort_candidates(period_candidates)
    if not args.no_period_replay:
        row = run_month_replay(
            month="period",
            candidates=period_candidates,
            output_dir=output_root / "period_exact_oanda_replay",
            config_path=args.config,
            profile_name=args.profile_name,
            start_equity=args.start_equity,
        )
        row.update({
            "period_start_utc": period_start.isoformat(),
            "period_end_utc": period_end.isoformat(),
            "direction_mode": args.direction_mode,
            "timeframes": ",".join(timeframes),
        })
        rows.append(row)
        print(json.dumps(row, sort_keys=True, default=json_default), flush=True)

    for month in months:
        start, end = month_bounds(month)
        monthly_frames = []
        stream_counts: Dict[str, int] = {}
        for timeframe, frame in stream_frames.items():
            if frame.empty:
                monthly = frame
            else:
                times = pd.to_datetime(frame["time_utc"], utc=True)
                monthly = frame[(times >= start) & (times < end)].copy()
            stream_counts[f"{timeframe}_candidate_rows"] = int(len(monthly))
            monthly_frames.append(monthly)
        monthly_candidates = transform_candidates(
            merge_streams(monthly_frames),
            "inverted" if args.direction_mode == "inverted" else "normal",
        )
        monthly_candidates = sort_candidates(monthly_candidates)
        row = run_month_replay(
            month=month,
            candidates=monthly_candidates,
            output_dir=output_root / "months" / month,
            config_path=args.config,
            profile_name=args.profile_name,
            start_equity=args.start_equity,
        )
        row.update({
            "period_start_utc": start.isoformat(),
            "period_end_utc": end.isoformat(),
            "direction_mode": args.direction_mode,
            "timeframes": ",".join(timeframes),
            **stream_counts,
        })
        rows.append(row)
        print(json.dumps(row, sort_keys=True, default=json_default), flush=True)

    summary = pd.DataFrame(rows)
    summary.to_csv(output_root / "monthly_dataset_comparison_summary.csv", index=False)
    write_json(output_root / "monthly_dataset_comparison_summary.json", rows)
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--months", default="")
    parser.add_argument("--timeframes", default="m30,h1,h4")
    parser.add_argument("--settings-csv", type=Path, default=DEFAULT_SETTINGS)
    parser.add_argument("--config", type=Path, default=PRIMARY_CONFIG)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--thresholds", default="0.50:0.95:0.025")
    parser.add_argument("--max-pairs", type=int, default=0)
    parser.add_argument("--train-row-cap", type=int, default=30000)
    parser.add_argument("--train-lookback-days", type=int, default=0)
    parser.add_argument("--n-jobs", type=int, default=4)
    parser.add_argument("--min-train-rows", type=int, default=5000)
    parser.add_argument("--min-calibration-rows", type=int, default=250)
    parser.add_argument("--min-test-rows", type=int, default=250)
    parser.add_argument("--min-pair-calibration-rows", type=int, default=20)
    parser.add_argument("--min-pair-calibration-trades", type=int, default=3)
    parser.add_argument("--min-expected-move-atr", type=float, default=0.0)
    parser.add_argument("--include-unsupported", action="store_true")
    parser.add_argument("--no-episode-aware", action="store_true")
    parser.add_argument("--holdout-weeks-override", type=int, default=0)
    parser.add_argument("--direction-mode", choices=["normal", "inverted"], default="inverted")
    parser.add_argument("--profile-name", default="h_tf_growth")
    parser.add_argument("--start-equity", type=float, default=100000.0)
    parser.add_argument("--no-period-replay", action="store_true")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    summary = run(args)
    columns = [
        column
        for column in [
            "month",
            "candidate_rows",
            "opened_trades",
            "return_pct",
            "profit_factor",
            "max_drawdown_pct",
            "max_margin_used_pct",
            "margin_call_rows",
            "m1_candidate_rows",
            "m30_candidate_rows",
            "h1_candidate_rows",
            "h4_candidate_rows",
            "output_dir",
        ]
        if column in summary.columns
    ]
    print(summary[columns].to_string(index=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
