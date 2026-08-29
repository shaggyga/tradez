#!/usr/bin/env python3
"""Cost-aware ARIMA baseline grid for the current OANDA all-pair research data.

This is deliberately separate from the always-on trainer.  It writes comparable
screen/validation metrics that the trainer can use as a baseline before any
technical-account promotion is considered.
"""

from __future__ import annotations

import argparse
import json
import math
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA


PROJECT_ROOT = Path(__file__).resolve().parent
FEATURE_ROOT = PROJECT_ROOT / "data" / "all68_weekly_move_study" / "features"
REPORT_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager" / "arima_baselines"
PIPELINE_VERSION = "all68_arima_baseline_v1"

SEVEN_MAJOR_PAIRS = [
    "EUR_USD",
    "GBP_USD",
    "USD_JPY",
    "USD_CHF",
    "USD_CAD",
    "AUD_USD",
    "NZD_USD",
]

ARIMA_SPECS: list[tuple[tuple[int, int, int], str]] = [
    ((0, 1, 0), "n"),
    ((0, 1, 0), "t"),
    ((1, 1, 0), "n"),
    ((1, 1, 0), "t"),
    ((2, 1, 0), "n"),
    ((3, 1, 0), "n"),
    ((5, 1, 0), "n"),
    ((8, 1, 0), "n"),
    ((0, 1, 1), "n"),
    ((0, 1, 2), "n"),
    ((1, 1, 1), "n"),
    ((2, 1, 1), "n"),
    ((1, 1, 2), "n"),
    ((2, 1, 2), "n"),
    ((3, 1, 1), "n"),
]


@dataclass(frozen=True)
class Window:
    window_id: int
    train_start: str
    train_end: str
    calibration_end: str
    test_end: str
    train_slice: slice
    calibration_slice: slice
    test_slice: slice


def json_safe(value: Any) -> Any:
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
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


def profit_factor(values: np.ndarray) -> float:
    wins = values[values > 0].sum()
    losses = -values[values < 0].sum()
    if losses <= 0:
        return float(wins) if wins > 0 else 0.0
    return float(wins / losses)


def load_pair(pair: str) -> pd.DataFrame:
    path = FEATURE_ROOT / f"{pair}.parquet"
    frame = pd.read_parquet(
        path,
        columns=[
            "close",
            "future_move_pips_15",
            "future_move_pips_30",
            "future_move_pips_60",
            "future_move_pips_120",
            "future_long_net_pips_15",
            "future_long_net_pips_30",
            "future_long_net_pips_60",
            "future_long_net_pips_120",
            "future_short_net_pips_15",
            "future_short_net_pips_30",
            "future_short_net_pips_60",
            "future_short_net_pips_120",
        ],
    ).copy()
    frame.index = pd.to_datetime(frame.index, errors="coerce", utc=True)
    frame = frame.sort_index()
    gap_minutes = frame.index.to_series().diff().dt.total_seconds().div(60)
    frame = frame[gap_minutes.fillna(5).le(10)].copy()
    frame["time_utc"] = frame.index
    return frame.dropna().reset_index(drop=True)


def windows_for(
    frame: pd.DataFrame,
    *,
    windows: int,
    train_rows: int,
    calibration_rows: int,
    test_rows: int,
) -> list[Window]:
    out: list[Window] = []
    stride = test_rows
    n = len(frame)
    for idx in range(windows):
        test_end = n - idx * stride
        test_start = test_end - test_rows
        calibration_start = test_start - calibration_rows
        train_start = calibration_start - train_rows
        if train_start < 0:
            break
        train = frame.iloc[train_start:calibration_start]
        calibration = frame.iloc[calibration_start:test_start]
        test = frame.iloc[test_start:test_end]
        out.append(
            Window(
                window_id=idx + 1,
                train_start=str(train["time_utc"].iloc[0]),
                train_end=str(train["time_utc"].iloc[-1]),
                calibration_end=str(calibration["time_utc"].iloc[-1]),
                test_end=str(test["time_utc"].iloc[-1]),
                train_slice=slice(train_start, calibration_start),
                calibration_slice=slice(calibration_start, test_start),
                test_slice=slice(test_start, test_end),
            )
        )
    return out


def arima_predictions(
    train: pd.DataFrame,
    calibration: pd.DataFrame,
    test: pd.DataFrame,
    *,
    order: tuple[int, int, int],
    trend: str,
    horizons: Sequence[int],
    predict_every: int,
    pip_multiplier: float,
) -> tuple[np.ndarray, np.ndarray]:
    train_series = np.log(train["close"].to_numpy(dtype=float))
    continuation = np.log(
        pd.concat(
            [calibration["close"], test["close"]],
            ignore_index=True,
        ).to_numpy(dtype=float)
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fitted = ARIMA(
            train_series,
            order=order,
            trend=trend,
            enforce_stationarity=False,
            enforce_invertibility=False,
        ).fit(method_kwargs={"maxiter": 60})
        applied = fitted.append(continuation, refit=False)

    train_length = len(train_series)
    calibration_length = len(calibration)
    max_steps = max(horizons) // 5

    def predict_segment(start: int, length: int) -> np.ndarray:
        output = np.full((length, len(horizons)), np.nan, dtype=float)
        for local_position in range(0, length, predict_every):
            absolute_origin = train_length + start + local_position - 1
            if absolute_origin < 0:
                continue
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                predicted = applied.get_prediction(
                    start=absolute_origin + 1,
                    end=absolute_origin + max_steps,
                    dynamic=True,
                ).predicted_mean
            endog = np.asarray(applied.model.endog, dtype=float).reshape(-1)
            current_log = float(endog[absolute_origin])
            current = math.exp(current_log)
            predicted_log = np.asarray(predicted, dtype=float)
            valid = np.isfinite(predicted_log) & (
                np.abs(predicted_log - current_log) <= 0.05
            )
            predicted_price = np.full_like(predicted_log, np.nan, dtype=float)
            predicted_price[valid] = np.exp(predicted_log[valid])
            for column, horizon in enumerate(horizons):
                step = horizon // 5
                if step <= len(predicted_price):
                    output[local_position, column] = (
                        predicted_price[step - 1] - current
                    ) * pip_multiplier
        return output

    return (
        predict_segment(0, calibration_length),
        predict_segment(calibration_length, len(test)),
    )


def trading_values(
    frame: pd.DataFrame,
    predictions: np.ndarray,
    horizon: int,
    threshold: float,
) -> np.ndarray:
    pred = np.asarray(predictions, dtype=float)
    long_mask = pred >= threshold
    short_mask = pred <= -threshold
    long_net = frame[f"future_long_net_pips_{horizon}"].to_numpy(dtype=float)
    short_net = frame[f"future_short_net_pips_{horizon}"].to_numpy(dtype=float)
    values = np.full(len(frame), np.nan, dtype=float)
    values[long_mask] = long_net[long_mask]
    values[short_mask] = short_net[short_mask]
    return values[np.isfinite(values)]


def metrics_from_values(values: np.ndarray) -> dict[str, float]:
    values = np.asarray(values, dtype=float)
    if len(values) == 0:
        return {
            "trades": 0.0,
            "mean_net_pips": 0.0,
            "median_net_pips": 0.0,
            "profit_factor": 0.0,
            "win_rate": 0.0,
            "total_net_pips": 0.0,
        }
    return {
        "trades": float(len(values)),
        "mean_net_pips": float(values.mean()),
        "median_net_pips": float(np.median(values)),
        "profit_factor": profit_factor(values),
        "win_rate": float((values > 0).mean()),
        "total_net_pips": float(values.sum()),
    }


def choose_threshold(
    calibration: pd.DataFrame,
    predictions: np.ndarray,
    horizon: int,
    *,
    min_calibration_trades: int,
) -> tuple[float, dict[str, float]]:
    finite_abs = np.abs(predictions[np.isfinite(predictions)])
    if len(finite_abs) == 0:
        return float("inf"), metrics_from_values(np.array([], dtype=float))
    quantiles = [0.0, 0.50, 0.60, 0.70, 0.80, 0.90, 0.95]
    candidates = sorted({float(np.quantile(finite_abs, q)) for q in quantiles})
    best_threshold = candidates[0]
    best_metrics = metrics_from_values(np.array([], dtype=float))
    best_score = -1e18
    for threshold in candidates:
        values = trading_values(calibration, predictions, horizon, threshold)
        metrics = metrics_from_values(values)
        if metrics["trades"] < min_calibration_trades:
            continue
        score = (
            metrics["total_net_pips"]
            + 20.0 * min(metrics["profit_factor"], 3.0)
            + 10.0 * metrics["mean_net_pips"]
        )
        if score > best_score:
            best_score = score
            best_threshold = threshold
            best_metrics = metrics
    return best_threshold, best_metrics


def forecast_metrics(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float]:
    mask = np.isfinite(actual) & np.isfinite(predicted)
    if not mask.any():
        return {
            "mae_pips": np.nan,
            "rmse_pips": np.nan,
            "direction_accuracy": np.nan,
            "correlation": np.nan,
        }
    actual = actual[mask]
    predicted = predicted[mask]
    correlation = (
        float(np.corrcoef(actual, predicted)[0, 1])
        if len(actual) > 2 and np.std(actual) > 0 and np.std(predicted) > 0
        else np.nan
    )
    return {
        "mae_pips": float(np.mean(np.abs(actual - predicted))),
        "rmse_pips": float(np.sqrt(np.mean((actual - predicted) ** 2))),
        "direction_accuracy": float((np.sign(actual) == np.sign(predicted)).mean()),
        "correlation": correlation,
    }


def evaluate_pair(
    pair: str,
    *,
    horizons: Sequence[int],
    windows: int,
    train_rows: int,
    calibration_rows: int,
    test_rows: int,
    predict_every: int,
    min_calibration_trades: int,
) -> list[dict[str, Any]]:
    frame = load_pair(pair)
    pair_windows = windows_for(
        frame,
        windows=windows,
        train_rows=train_rows,
        calibration_rows=calibration_rows,
        test_rows=test_rows,
    )
    multiplier = 100.0 if pair.endswith("JPY") else 10_000.0
    results: list[dict[str, Any]] = []
    for window in pair_windows:
        train = frame.iloc[window.train_slice].reset_index(drop=True)
        calibration = frame.iloc[window.calibration_slice].reset_index(drop=True)
        test = frame.iloc[window.test_slice].reset_index(drop=True)
        for spec_number, (order, trend) in enumerate(ARIMA_SPECS, 1):
            model = f"ARIMA{order}_trend_{trend}"
            try:
                cal_pred, test_pred = arima_predictions(
                    train,
                    calibration,
                    test,
                    order=order,
                    trend=trend,
                    horizons=horizons,
                    predict_every=predict_every,
                    pip_multiplier=multiplier,
                )
            except Exception as exc:
                results.append({
                    "family": "ARIMA",
                    "model": model,
                    "pair": pair,
                    "window_id": window.window_id,
                    "error": repr(exc),
                })
                continue
            for column, horizon in enumerate(horizons):
                valid_cal = np.isfinite(cal_pred[:, column])
                valid_test = np.isfinite(test_pred[:, column])
                cal_subset = calibration.loc[valid_cal].reset_index(drop=True)
                test_subset = test.loc[valid_test].reset_index(drop=True)
                cal_values = cal_pred[valid_cal, column]
                test_values = test_pred[valid_test, column]
                threshold, cal_metrics = choose_threshold(
                    cal_subset,
                    cal_values,
                    horizon,
                    min_calibration_trades=min_calibration_trades,
                )
                test_trade_values = trading_values(
                    test_subset,
                    test_values,
                    horizon,
                    threshold,
                )
                test_metrics = metrics_from_values(test_trade_values)
                results.append({
                    "pipeline_version": PIPELINE_VERSION,
                    "family": "ARIMA",
                    "model": model,
                    "pair": pair,
                    "window_id": window.window_id,
                    "train_start": window.train_start,
                    "train_end": window.train_end,
                    "calibration_end": window.calibration_end,
                    "test_end": window.test_end,
                    "horizon_minutes": horizon,
                    "train_rows": len(train),
                    "calibration_rows": int(valid_cal.sum()),
                    "test_rows": int(valid_test.sum()),
                    "spec_number": spec_number,
                    "threshold_pips": threshold,
                    **{f"calibration_{k}": v for k, v in cal_metrics.items()},
                    **forecast_metrics(
                        test_subset[f"future_move_pips_{horizon}"].to_numpy(dtype=float),
                        test_values,
                    ),
                    **test_metrics,
                    "error": "",
                })
    return results


def summarize(results: pd.DataFrame) -> dict[str, Any]:
    if results.empty:
        return {"pipeline_version": PIPELINE_VERSION, "rows": 0}
    clean = results[results["error"].fillna("").eq("")].copy()
    stable_rows = []
    if not clean.empty:
        for keys, group in clean.groupby(["model", "pair", "horizon_minutes"], dropna=False):
            values = pd.to_numeric(group["mean_net_pips"], errors="coerce").fillna(0.0)
            trades = pd.to_numeric(group["trades"], errors="coerce").fillna(0.0)
            pfs = pd.to_numeric(group["profit_factor"], errors="coerce").fillna(0.0)
            stable_rows.append({
                "model": keys[0],
                "pair": keys[1],
                "horizon_minutes": int(keys[2]),
                "windows": int(len(group)),
                "profitable_windows": int((values > 0).sum()),
                "min_mean_net_pips": float(values.min()) if len(values) else 0.0,
                "avg_mean_net_pips": float(values.mean()) if len(values) else 0.0,
                "avg_profit_factor": float(pfs.mean()) if len(pfs) else 0.0,
                "avg_trades": float(trades.mean()) if len(trades) else 0.0,
                "total_net_pips": float(pd.to_numeric(group["total_net_pips"], errors="coerce").fillna(0.0).sum()),
            })
    stable = pd.DataFrame(stable_rows)
    promotion_ready = pd.DataFrame()
    if not stable.empty:
        promotion_ready = stable[
            stable["windows"].ge(2)
            & stable["profitable_windows"].eq(stable["windows"])
            & stable["min_mean_net_pips"].gt(0.0)
            & stable["avg_profit_factor"].ge(1.20)
            & stable["avg_trades"].ge(10.0)
        ].sort_values(["avg_mean_net_pips", "avg_profit_factor"], ascending=False)
    return {
        "pipeline_version": PIPELINE_VERSION,
        "rows": int(len(results)),
        "successful_rows": int(len(clean)),
        "pairs": sorted(clean["pair"].dropna().unique().tolist()) if not clean.empty else [],
        "horizons": sorted(clean["horizon_minutes"].dropna().astype(int).unique().tolist()) if not clean.empty else [],
        "promotion_ready_count": int(len(promotion_ready)),
        "promotion_ready": promotion_ready.head(25).to_dict("records"),
        "top_rows": clean.sort_values(
            ["total_net_pips", "mean_net_pips"],
            ascending=False,
        ).head(25).to_dict("records") if not clean.empty else [],
    }


def parse_pairs(raw: str) -> list[str]:
    raw = raw.strip()
    if raw.lower() == "majors":
        return SEVEN_MAJOR_PAIRS
    if raw.lower() == "all":
        return sorted(path.stem for path in FEATURE_ROOT.glob("*.parquet"))
    return [item.strip().upper().replace("/", "_") for item in raw.split(",") if item.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run cost-aware ARIMA baselines on OANDA all-pair features")
    parser.add_argument("--pairs", default="majors", help="'majors', 'all', or comma-separated instruments")
    parser.add_argument("--horizons", default="30,60,120", help="Comma-separated horizons in minutes")
    parser.add_argument("--windows", type=int, default=4)
    parser.add_argument("--train-rows", type=int, default=12_000)
    parser.add_argument("--calibration-rows", type=int, default=2_000)
    parser.add_argument("--test-rows", type=int, default=2_000)
    parser.add_argument("--predict-every", type=int, default=12, help="Rows between forecast origins; 12 = hourly on M5 data")
    parser.add_argument("--min-calibration-trades", type=int, default=10)
    parser.add_argument("--output-prefix", default="latest_arima_grid")
    args = parser.parse_args()

    pairs = parse_pairs(args.pairs)
    horizons = [int(x.strip()) for x in args.horizons.split(",") if x.strip()]
    all_results: list[dict[str, Any]] = []
    for pair_number, pair in enumerate(pairs, 1):
        print(f"[arima-grid] {pair_number}/{len(pairs)} {pair}", flush=True)
        try:
            all_results.extend(
                evaluate_pair(
                    pair,
                    horizons=horizons,
                    windows=args.windows,
                    train_rows=args.train_rows,
                    calibration_rows=args.calibration_rows,
                    test_rows=args.test_rows,
                    predict_every=args.predict_every,
                    min_calibration_trades=args.min_calibration_trades,
                )
            )
        except Exception as exc:
            all_results.append({
                "pipeline_version": PIPELINE_VERSION,
                "family": "ARIMA",
                "pair": pair,
                "error": repr(exc),
            })

    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    results = pd.DataFrame(all_results)
    csv_path = REPORT_ROOT / f"{args.output_prefix}_results.csv"
    summary_path = REPORT_ROOT / f"{args.output_prefix}_summary.json"
    results.to_csv(csv_path, index=False)
    summary = summarize(results)
    summary.update({
        "results_csv": str(csv_path),
        "pairs_requested": pairs,
        "horizons_requested": horizons,
        "windows_requested": args.windows,
        "train_rows": args.train_rows,
        "calibration_rows": args.calibration_rows,
        "test_rows": args.test_rows,
        "predict_every": args.predict_every,
    })
    write_json(summary_path, summary)
    print(json.dumps(summary, indent=2, default=json_safe)[:4000], flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
