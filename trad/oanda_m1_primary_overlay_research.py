#!/usr/bin/env python3
"""Train/test an M1 helper overlay for the primary forecast-rotation stream.

The overlay is not a standalone entry model. It learns whether the current
M30/H1/H4 primary candidate has M1 context that supports a 120-minute hold,
then tests filtering thresholds through the same broker-style portfolio replay.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import joblib
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score

from oanda_broker_style_portfolio_replay import BrokerReplay, REPORTS, load_margin_rates, objective, read_json, safe_float, safe_int
from oanda_gpt_training_strategy_manager import TECHNICAL_MODEL_FEATURES
from oanda_primary_live_exact_backtest import PRIMARY_CONFIG, selected_profile
from oanda_primary_live_exit_rotation_sweep import load_replay_inputs


ROOT = Path(__file__).resolve().parent
DEFAULT_M1_DATASET = ROOT / "data" / "oanda_training_manager" / "continuous_research" / "technical_spike_research.parquet"
CANARY_CONFIG = ROOT / "config" / "canary_primary_forecast_rotation_bot.json"
DEFAULT_CONFIG = CANARY_CONFIG if CANARY_CONFIG.exists() else PRIMARY_CONFIG
DEFAULT_OUTPUT = REPORTS / f"m1_primary_overlay_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

OUTCOME_COLUMNS = [
    "long_curve_endpoint_net_pips_120",
    "short_curve_endpoint_net_pips_120",
    "long_curve_best_net_pips_30",
    "short_curve_best_net_pips_30",
    "long_curve_mae_pips_30",
    "short_curve_mae_pips_30",
    "long_curve_giveback_pips_30",
    "short_curve_giveback_pips_30",
    "long_curve_efficiency_30",
    "short_curve_efficiency_30",
]

CANDIDATE_FEATURES = [
    "probability",
    "confidence",
    "expected_move_atr",
    "rank_score",
    "score_percentile",
    "threshold",
    "spread_pips",
    "atr240_pips",
    "momentum_30_atr",
    "pair_threshold_rank_score",
    "pair_report_rank_score",
]

M1_FEATURE_PREFIX = "m1ctx__"


@dataclass
class FoldSpec:
    name: str
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=json_default), encoding="utf-8")


def utc_ns(values: Any) -> pd.Series:
    """Normalize mixed pyarrow/pandas timestamp units for merge_asof."""
    converted = pd.to_datetime(values, utc=True)
    return converted.astype("datetime64[ns, UTC]")


def direction_sign(value: Any) -> int:
    text = str(value or "").strip().upper()
    if text in {"LONG", "BUY", "1"}:
        return 1
    if text in {"SHORT", "SELL", "-1"}:
        return -1
    return 0


def available_m1_columns(path: Path, candidate_start: pd.Timestamp, candidate_end: pd.Timestamp) -> List[str]:
    names = set(pq.ParquetFile(path).schema.names)
    feature_cols = [column for column in TECHNICAL_MODEL_FEATURES if column in names]
    columns = ["time_utc", "instrument", *feature_cols, *[column for column in OUTCOME_COLUMNS if column in names]]
    return list(dict.fromkeys(columns))


def load_m1_slice(path: Path, candidate_start: pd.Timestamp, candidate_end: pd.Timestamp) -> pd.DataFrame:
    columns = available_m1_columns(path, candidate_start, candidate_end)
    table = pq.read_table(
        path,
        columns=columns,
        filters=[
            ("time_utc", ">=", candidate_start.to_pydatetime()),
            ("time_utc", "<=", candidate_end.to_pydatetime()),
        ],
    )
    frame = table.to_pandas()
    frame["time_utc"] = utc_ns(frame["time_utc"])
    frame["instrument"] = frame["instrument"].astype(str)
    rename_map = {
        column: f"{M1_FEATURE_PREFIX}{column}"
        for column in TECHNICAL_MODEL_FEATURES
        if column in frame.columns
    }
    frame = frame.rename(columns=rename_map)
    return frame.sort_values(["instrument", "time_utc"]).reset_index(drop=True)


def merge_m1_context(candidates: pd.DataFrame, m1: pd.DataFrame) -> pd.DataFrame:
    candidates = candidates.copy()
    candidates["time_utc"] = utc_ns(candidates["time_utc"])
    candidates["instrument"] = candidates["instrument"].astype(str)
    m1 = m1.copy()
    m1["time_utc"] = utc_ns(m1["time_utc"])
    merged_parts: List[pd.DataFrame] = []
    for instrument, candidate_group in candidates.groupby("instrument", sort=False):
        m1_group = m1[m1["instrument"] == instrument].sort_values("time_utc")
        if m1_group.empty:
            continue
        part = pd.merge_asof(
            candidate_group.sort_values("time_utc"),
            m1_group.drop(columns=["instrument"]).sort_values("time_utc"),
            on="time_utc",
            direction="backward",
            tolerance=pd.Timedelta(minutes=10),
            suffixes=("", "_m1"),
        )
        merged_parts.append(part)
    if not merged_parts:
        return pd.DataFrame()
    merged = pd.concat(merged_parts, ignore_index=True)
    merged = merged.dropna(subset=["long_curve_endpoint_net_pips_120", "short_curve_endpoint_net_pips_120"])
    sign = merged["direction"].map(direction_sign)
    merged["direction_sign"] = sign
    merged["m1_endpoint_pips_120"] = np.where(
        sign >= 0,
        pd.to_numeric(merged["long_curve_endpoint_net_pips_120"], errors="coerce"),
        pd.to_numeric(merged["short_curve_endpoint_net_pips_120"], errors="coerce"),
    )
    merged["m1_best_pips_30"] = np.where(
        sign >= 0,
        pd.to_numeric(merged["long_curve_best_net_pips_30"], errors="coerce"),
        pd.to_numeric(merged["short_curve_best_net_pips_30"], errors="coerce"),
    )
    merged["m1_mae_pips_30"] = np.where(
        sign >= 0,
        pd.to_numeric(merged["long_curve_mae_pips_30"], errors="coerce"),
        pd.to_numeric(merged["short_curve_mae_pips_30"], errors="coerce"),
    )
    merged["m1_giveback_pips_30"] = np.where(
        sign >= 0,
        pd.to_numeric(merged["long_curve_giveback_pips_30"], errors="coerce"),
        pd.to_numeric(merged["short_curve_giveback_pips_30"], errors="coerce"),
    )
    merged["m1_efficiency_30"] = np.where(
        sign >= 0,
        pd.to_numeric(merged["long_curve_efficiency_30"], errors="coerce"),
        pd.to_numeric(merged["short_curve_efficiency_30"], errors="coerce"),
    )
    merged["m1_hold_success_120"] = (merged["m1_endpoint_pips_120"] > 0).astype(int)
    if "edge_pips" in merged:
        edge_series = pd.to_numeric(merged["edge_pips"], errors="coerce").fillna(0.0)
    else:
        edge_series = pd.Series(0.0, index=merged.index)
    quick_floor = np.maximum(1.0, edge_series * 0.25)
    merged["m1_quick_success_30"] = (merged["m1_best_pips_30"] >= quick_floor).astype(int)
    return merged.sort_values(["time_utc", "rank_score", "probability"], ascending=[True, False, False]).reset_index(drop=True)


def build_feature_frame(frame: pd.DataFrame, feature_columns: Sequence[str] | None = None) -> tuple[pd.DataFrame, List[str]]:
    data = pd.DataFrame(index=frame.index)
    for column in CANDIDATE_FEATURES:
        if column in frame:
            data[f"candidate__{column}"] = pd.to_numeric(frame[column], errors="coerce")
    for column in TECHNICAL_MODEL_FEATURES:
        m1_column = f"{M1_FEATURE_PREFIX}{column}"
        if m1_column in frame:
            data[f"m1__{column}"] = pd.to_numeric(frame[m1_column], errors="coerce")
    data["candidate__direction_sign"] = pd.to_numeric(frame["direction_sign"], errors="coerce")
    for column in ["source_stream", "direction"]:
        if column in frame:
            dummies = pd.get_dummies(frame[column].astype(str), prefix=f"candidate__{column}")
            data = pd.concat([data, dummies.astype(float)], axis=1)
    data = data.replace([np.inf, -np.inf], np.nan)
    if feature_columns is None:
        features = list(data.columns)
    else:
        features = list(feature_columns)
        for column in features:
            if column not in data:
                data[column] = 0.0
        data = data[features]
    return data, features


def make_model() -> HistGradientBoostingClassifier:
    return HistGradientBoostingClassifier(
        learning_rate=0.05,
        max_iter=240,
        max_leaf_nodes=31,
        min_samples_leaf=40,
        l2_regularization=0.05,
        random_state=42,
    )


def fold_specs(frame: pd.DataFrame, folds: int) -> List[FoldSpec]:
    times = utc_ns(frame["time_utc"])
    start = times.min()
    end = times.max()
    edges = pd.date_range(start=start, end=end, periods=max(3, folds + 2))
    specs: List[FoldSpec] = []
    for idx in range(1, len(edges) - 1):
        train_start = start
        train_end = edges[idx]
        test_start = edges[idx]
        test_end = edges[idx + 1]
        if test_end <= test_start:
            continue
        specs.append(FoldSpec(f"fold_{idx:02d}", train_start, train_end, test_start, test_end))
    return specs


def score_out_of_sample(frame: pd.DataFrame, folds: int) -> tuple[pd.DataFrame, List[Dict[str, Any]], List[str]]:
    scored = frame.copy()
    scored["m1_hold_prob"] = np.nan
    scored["m1_quick_prob"] = np.nan
    rows: List[Dict[str, Any]] = []
    feature_columns: List[str] = []
    for spec in fold_specs(frame, folds):
        times = utc_ns(frame["time_utc"])
        train_mask = (times >= spec.train_start) & (times < spec.train_end)
        test_mask = (times >= spec.test_start) & (times < spec.test_end)
        train = frame[train_mask].copy()
        test = frame[test_mask].copy()
        if len(train) < 500 or len(test) < 100 or train["m1_hold_success_120"].nunique() < 2:
            continue
        x_train, features = build_feature_frame(train, feature_columns if feature_columns else None)
        if not feature_columns:
            feature_columns = features
        x_test, _ = build_feature_frame(test, feature_columns)
        hold_model = make_model()
        hold_model.fit(x_train, train["m1_hold_success_120"].astype(int))
        hold_prob = hold_model.predict_proba(x_test)[:, 1]
        scored.loc[test.index, "m1_hold_prob"] = hold_prob
        if train["m1_quick_success_30"].nunique() >= 2:
            quick_model = make_model()
            quick_model.fit(x_train, train["m1_quick_success_30"].astype(int))
            quick_prob = quick_model.predict_proba(x_test)[:, 1]
        else:
            quick_prob = np.full(len(test), float(train["m1_quick_success_30"].mean()))
        scored.loc[test.index, "m1_quick_prob"] = quick_prob
        y = test["m1_hold_success_120"].astype(int)
        rows.append(
            {
                "fold": spec.name,
                "train_start": spec.train_start,
                "train_end": spec.train_end,
                "test_start": spec.test_start,
                "test_end": spec.test_end,
                "train_rows": int(len(train)),
                "test_rows": int(len(test)),
                "hold_positive_rate": float(y.mean()),
                "hold_auc": float(roc_auc_score(y, hold_prob)) if y.nunique() == 2 else 0.0,
                "hold_average_precision": float(average_precision_score(y, hold_prob)) if y.nunique() == 2 else 0.0,
                "hold_brier": float(brier_score_loss(y, hold_prob)),
                "quick_positive_rate": float(test["m1_quick_success_30"].mean()),
            }
        )
    return scored.dropna(subset=["m1_hold_prob"]).copy(), rows, feature_columns


def replay_summary(
    candidates: pd.DataFrame,
    *,
    live_cfg: Dict[str, Any],
    base_profile: Dict[str, Any],
    candles: Any,
    margin_rates: Dict[str, float],
) -> Dict[str, Any]:
    replay = BrokerReplay(
        dict(base_profile),
        candles,
        margin_rates=margin_rates,
        live_cfg={**live_cfg, "simulate_live_exit_checks": True},
        max_new_positions_per_timestamp=safe_int(live_cfg.get("max_new_positions_per_cycle"), 2),
    )
    return replay.run(candidates)


def run_replays(
    scored: pd.DataFrame,
    *,
    live_cfg: Dict[str, Any],
    base_profile: Dict[str, Any],
    candles: Any,
    margin_rates: Dict[str, float],
    hold_thresholds: Sequence[float],
    quick_thresholds: Sequence[float],
    progress_csv: Path | None = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    def persist_progress() -> None:
        if progress_csv is None:
            return
        progress_csv.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_csv(progress_csv, index=False)

    def log_progress(row: Dict[str, Any]) -> None:
        print(
            "[m1-overlay-replay] "
            f"scenario={row.get('scenario')} "
            f"candidates={row.get('candidate_rows_after_filter')} "
            f"return_pct={safe_float(row.get('return_pct'), 0.0):.3f} "
            f"pf={safe_float(row.get('profit_factor'), 0.0):.3f} "
            f"win={safe_float(row.get('win_rate'), 0.0):.3f} "
            f"objective={safe_float(row.get('objective'), 0.0):.3f}",
            flush=True,
        )

    baseline = replay_summary(scored, live_cfg=live_cfg, base_profile=base_profile, candles=candles, margin_rates=margin_rates)
    baseline_row = {
        "scenario": "baseline_scored_window",
        "m1_hold_threshold": 0.0,
        "m1_quick_threshold": 0.0,
        "candidate_rows_after_filter": int(len(scored)),
        "objective": objective(baseline),
        **baseline,
    }
    rows.append(baseline_row)
    log_progress(baseline_row)
    persist_progress()

    def add_filtered_replay(scenario: str, filtered: pd.DataFrame, hold_threshold: float, quick_threshold: float) -> None:
        if filtered.empty:
            empty_row = {
                "scenario": scenario,
                "m1_hold_threshold": hold_threshold,
                "m1_quick_threshold": quick_threshold,
                "candidate_rows_after_filter": 0,
            }
            rows.append(empty_row)
            log_progress(empty_row)
            persist_progress()
            return
        summary = replay_summary(filtered, live_cfg=live_cfg, base_profile=base_profile, candles=candles, margin_rates=margin_rates)
        row = {
            "scenario": scenario,
            "m1_hold_threshold": hold_threshold,
            "m1_quick_threshold": quick_threshold,
            "candidate_rows_after_filter": int(len(filtered)),
            "mean_m1_hold_prob": float(filtered["m1_hold_prob"].mean()),
            "mean_m1_quick_prob": float(filtered["m1_quick_prob"].mean()),
            "actual_hold_success_rate": float(filtered["m1_hold_success_120"].mean()),
            "actual_quick_success_rate": float(filtered["m1_quick_success_30"].mean()),
            "objective": objective(summary),
            **summary,
        }
        rows.append(row)
        log_progress(row)
        persist_progress()

    for threshold in hold_thresholds:
        filtered = scored[scored["m1_hold_prob"] >= threshold].copy()
        add_filtered_replay(f"m1_hold_ge_{threshold:.2f}", filtered, threshold, 0.0)

    for threshold in quick_thresholds:
        filtered = scored[scored["m1_quick_prob"] >= threshold].copy()
        add_filtered_replay(f"m1_quick_ge_{threshold:.2f}", filtered, 0.0, threshold)

    for hold_threshold in hold_thresholds:
        for quick_threshold in quick_thresholds:
            filtered = scored[
                (scored["m1_hold_prob"] >= hold_threshold)
                & (scored["m1_quick_prob"] >= quick_threshold)
            ].copy()
            add_filtered_replay(
                f"m1_hold_ge_{hold_threshold:.2f}__quick_ge_{quick_threshold:.2f}",
                filtered,
                hold_threshold,
                quick_threshold,
            )
    return rows


def train_final_model(frame: pd.DataFrame, feature_columns: Sequence[str]) -> Dict[str, Any]:
    x, _ = build_feature_frame(frame, feature_columns)
    hold_model = make_model()
    hold_model.fit(x, frame["m1_hold_success_120"].astype(int))
    quick_model: HistGradientBoostingClassifier | None = None
    if frame["m1_quick_success_30"].nunique() >= 2:
        quick_model = make_model()
        quick_model.fit(x, frame["m1_quick_success_30"].astype(int))
    return {
        "hold_model": hold_model,
        "quick_model": quick_model,
        "features": list(feature_columns),
        "candidate_features": CANDIDATE_FEATURES,
        "m1_features": [column for column in TECHNICAL_MODEL_FEATURES if f"{M1_FEATURE_PREFIX}{column}" in frame.columns],
        "trained_rows": int(len(frame)),
        "trained_start_utc": utc_ns(frame["time_utc"]).min().isoformat(),
        "trained_end_utc": utc_ns(frame["time_utc"]).max().isoformat(),
        "hold_positive_rate": float(frame["m1_hold_success_120"].mean()),
        "quick_positive_rate": float(frame["m1_quick_success_30"].mean()),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--candidate-rows-csv", type=Path, default=None)
    parser.add_argument("--m1-dataset", type=Path, default=DEFAULT_M1_DATASET)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--profile-name", default="h_tf_growth")
    parser.add_argument("--start-equity", type=float, default=1000.0)
    parser.add_argument("--folds", type=int, default=4)
    parser.add_argument("--thresholds", default="0.45,0.50,0.55,0.60,0.65,0.70")
    parser.add_argument("--quick-thresholds", default="0.50,0.55,0.60,0.65,0.70,0.75")
    args = parser.parse_args()

    args.output_root.mkdir(parents=True, exist_ok=True)
    live_cfg, candidates, candles = load_replay_inputs(args.config, args.candidate_rows_csv)
    candidates["time_utc"] = utc_ns(candidates["time_utc"])
    candidate_start = candidates["time_utc"].min() - pd.Timedelta(minutes=10)
    candidate_end = candidates["time_utc"].max()
    print(f"[m1-overlay] loaded candidates rows={len(candidates)} start={candidate_start} end={candidate_end}", flush=True)
    m1 = load_m1_slice(args.m1_dataset, candidate_start, candidate_end)
    print(f"[m1-overlay] loaded m1 context rows={len(m1)} columns={len(m1.columns)}", flush=True)
    merged = merge_m1_context(candidates, m1)
    if merged.empty:
        raise RuntimeError("No primary candidates could be matched to M1 context")
    print(f"[m1-overlay] matched candidates rows={len(merged)}", flush=True)
    scored, fold_rows, features = score_out_of_sample(merged, args.folds)
    if scored.empty:
        raise RuntimeError("No out-of-sample M1 overlay scores were produced")
    print(f"[m1-overlay] scored oos rows={len(scored)} folds={len(fold_rows)} features={len(features)}", flush=True)
    base_profile = selected_profile(live_cfg, args.profile_name, args.start_equity)
    thresholds = [float(item.strip()) for item in str(args.thresholds).split(",") if item.strip()]
    quick_thresholds = [float(item.strip()) for item in str(args.quick_thresholds).split(",") if item.strip()]
    replay_rows = run_replays(
        scored,
        live_cfg=live_cfg,
        base_profile=base_profile,
        candles=candles,
        margin_rates=load_margin_rates(),
        hold_thresholds=thresholds,
        quick_thresholds=quick_thresholds,
        progress_csv=args.output_root / "overlay_replay_progress.csv",
    )
    print(f"[m1-overlay] completed replays scenarios={len(replay_rows)}", flush=True)
    final_bundle = train_final_model(merged, features)
    model_path = args.output_root / "m1_primary_overlay_model.joblib"
    joblib.dump(final_bundle, model_path)
    metadata = {
        "config": str(args.config),
        "candidate_rows": int(len(candidates)),
        "matched_rows": int(len(merged)),
        "scored_oos_rows": int(len(scored)),
        "candidate_start_utc": candidate_start.isoformat(),
        "candidate_end_utc": candidate_end.isoformat(),
        "m1_dataset": str(args.m1_dataset),
        "feature_count": int(len(features)),
        "model_path": str(model_path),
    }
    write_json(args.output_root / "metadata.json", metadata)
    pd.DataFrame(fold_rows).to_csv(args.output_root / "fold_metrics.csv", index=False)
    write_json(args.output_root / "fold_metrics.json", fold_rows)
    pd.DataFrame(replay_rows).sort_values("objective", ascending=False).to_csv(args.output_root / "overlay_replay_summary.csv", index=False)
    write_json(args.output_root / "overlay_replay_summary.json", replay_rows)
    scored.to_csv(args.output_root / "scored_candidate_rows.csv", index=False)
    merged.to_csv(args.output_root / "matched_candidate_rows.csv", index=False)
    print(json.dumps({"metadata": metadata, "fold_metrics": fold_rows, "replay_rows": replay_rows}, indent=2, sort_keys=True, default=json_default), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
