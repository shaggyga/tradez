#!/usr/bin/env python3
"""Read-only live forecast sensitivity audit for OANDA candle warmup count."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

from oanda_primary_forecast_rotation_bot import (
    CONFIG_PATH,
    Forecast,
    ForecastEngine,
    OandaGateway,
    load_config,
    load_creds,
    resolve_oanda,
    safe_float,
    safe_int,
)


ROOT = Path(__file__).resolve().parent
REPORTS = ROOT / "data" / "oanda_training_manager" / "reports"


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def forecast_key(item: Forecast) -> str:
    return "|".join(
        [
            item.instrument,
            item.event,
            item.source_stream,
            item.model_feature_timeframe,
            "long" if item.direction > 0 else "short",
        ]
    )


def forecast_row(count: int, item: Forecast) -> Dict[str, Any]:
    return {
        "candle_count": count,
        "key": forecast_key(item),
        "instrument": item.instrument,
        "event": item.event,
        "source_stream": item.source_stream,
        "feature_timeframe": item.model_feature_timeframe,
        "feature_time_utc": item.model_feature_time_utc,
        "direction": item.direction_name(),
        "probability": item.probability,
        "rank_score": item.rank_score,
        "edge_pips": item.edge_pips,
        "risk_pips": item.risk_pips,
        "spread_pips": item.spread_pips,
        "reject_reason": item.reject_reason,
    }


def run_one(base_cfg: Dict[str, Any], gateway: OandaGateway, count: int, max_pairs: int) -> Tuple[List[Forecast], Dict[str, Any]]:
    cfg = dict(base_cfg)
    cfg["oanda_candle_count"] = int(count)
    # ForecastEngine is read-only; force execution flags off for audit clarity.
    cfg["live_execution_enabled"] = False
    cfg["demo_execution_enabled"] = False
    cfg["live_new_entries_enabled"] = False
    start = time.perf_counter()
    engine = ForecastEngine(cfg, gateway=gateway)
    forecasts, meta = engine.forecast(source="oanda", max_pairs=max_pairs)
    elapsed = time.perf_counter() - start
    meta = dict(meta)
    meta["elapsed_seconds"] = round(elapsed, 3)
    meta["oanda_candle_count"] = int(count)
    meta["eligible_forecast_count"] = sum(1 for item in forecasts if not item.reject_reason)
    return forecasts, meta


def top_keys(rows: Iterable[Forecast], n: int, *, eligible_only: bool = False) -> List[str]:
    items = [item for item in rows if (not eligible_only or not item.reject_reason)]
    items.sort(key=lambda item: item.rank_score, reverse=True)
    return [forecast_key(item) for item in items[:n]]


def summarize_against_baseline(
    baseline_count: int,
    baseline: List[Forecast],
    count: int,
    forecasts: List[Forecast],
) -> Dict[str, Any]:
    base_map = {forecast_key(item): item for item in baseline}
    cur_map = {forecast_key(item): item for item in forecasts}
    common = sorted(set(base_map) & set(cur_map))
    missing = sorted(set(base_map) - set(cur_map))
    added = sorted(set(cur_map) - set(base_map))
    probability_diffs: List[float] = []
    rank_diffs: List[float] = []
    edge_diffs: List[float] = []
    direction_changes = 0
    reject_changes = 0
    feature_time_changes = 0
    for key in common:
        base = base_map[key]
        cur = cur_map[key]
        probability_diffs.append(abs(safe_float(cur.probability) - safe_float(base.probability)))
        rank_diffs.append(abs(safe_float(cur.rank_score) - safe_float(base.rank_score)))
        edge_diffs.append(abs(safe_float(cur.edge_pips) - safe_float(base.edge_pips)))
        direction_changes += int(cur.direction != base.direction)
        reject_changes += int(str(cur.reject_reason or "") != str(base.reject_reason or ""))
        feature_time_changes += int(str(cur.model_feature_time_utc or "") != str(base.model_feature_time_utc or ""))

    def max_or_zero(values: List[float]) -> float:
        return round(max(values), 8) if values else 0.0

    def mean_or_zero(values: List[float]) -> float:
        return round(float(sum(values) / len(values)), 8) if values else 0.0

    base_top50 = set(top_keys(baseline, 50))
    cur_top50 = set(top_keys(forecasts, 50))
    base_top20_eligible = set(top_keys(baseline, 20, eligible_only=True))
    cur_top20_eligible = set(top_keys(forecasts, 20, eligible_only=True))
    return {
        "baseline_candle_count": baseline_count,
        "candle_count": count,
        "baseline_forecasts": len(baseline),
        "forecast_count": len(forecasts),
        "common_forecasts": len(common),
        "missing_vs_baseline": len(missing),
        "added_vs_baseline": len(added),
        "direction_changes": direction_changes,
        "reject_reason_changes": reject_changes,
        "feature_time_changes": feature_time_changes,
        "max_probability_abs_diff": max_or_zero(probability_diffs),
        "mean_probability_abs_diff": mean_or_zero(probability_diffs),
        "max_rank_score_abs_diff": max_or_zero(rank_diffs),
        "mean_rank_score_abs_diff": mean_or_zero(rank_diffs),
        "max_edge_pips_abs_diff": max_or_zero(edge_diffs),
        "mean_edge_pips_abs_diff": mean_or_zero(edge_diffs),
        "top50_overlap": len(base_top50 & cur_top50),
        "top50_overlap_pct": round((len(base_top50 & cur_top50) / max(1, len(base_top50))) * 100.0, 3),
        "eligible_top20_overlap": len(base_top20_eligible & cur_top20_eligible),
        "eligible_top20_overlap_pct": round(
            (len(base_top20_eligible & cur_top20_eligible) / max(1, len(base_top20_eligible))) * 100.0,
            3,
        ),
        "missing_keys_preview": missing[:20],
        "added_keys_preview": added[:20],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--counts", default="650,1000,1500,5000")
    parser.add_argument("--baseline", type=int, default=650)
    parser.add_argument("--max-pairs", type=int, default=0)
    parser.add_argument("--output-root", type=Path, default=None)
    args = parser.parse_args()

    counts = [safe_int(item, 0) for item in str(args.counts).split(",") if str(item).strip()]
    counts = [item for item in counts if item > 0]
    if args.baseline not in counts:
        counts.insert(0, args.baseline)

    base_cfg = load_config(args.config)
    creds = load_creds()
    resolved = resolve_oanda(base_cfg, creds, "live")
    gateway = OandaGateway(
        resolved["token"],
        resolved["base_url"],
        resolved["account_id"],
        timeout=safe_float(base_cfg.get("oanda_request_timeout_seconds"), 20.0),
        request_retries=safe_int(base_cfg.get("oanda_request_retries"), 3),
        retry_backoff_seconds=safe_float(base_cfg.get("oanda_request_backoff_seconds"), 1.25),
    )

    output_root = args.output_root or REPORTS / f"candle_count_sensitivity_{utc_stamp()}"
    output_root.mkdir(parents=True, exist_ok=True)
    all_forecasts: Dict[int, List[Forecast]] = {}
    meta_rows: List[Dict[str, Any]] = []
    forecast_rows: List[Dict[str, Any]] = []

    for count in counts:
        forecasts, meta = run_one(base_cfg, gateway, count, args.max_pairs)
        all_forecasts[count] = forecasts
        meta_rows.append(meta)
        forecast_rows.extend(forecast_row(count, item) for item in forecasts)
        print(json.dumps({"count": count, **meta}, sort_keys=True, default=str), flush=True)

    baseline = all_forecasts[args.baseline]
    comparisons = [
        summarize_against_baseline(args.baseline, baseline, count, all_forecasts[count])
        for count in counts
    ]

    pd.DataFrame(meta_rows).to_csv(output_root / "run_meta.csv", index=False)
    pd.DataFrame(forecast_rows).to_csv(output_root / "forecasts.csv", index=False)
    pd.DataFrame(comparisons).to_csv(output_root / "comparison_vs_baseline.csv", index=False)
    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "config": str(args.config),
        "counts": counts,
        "baseline": args.baseline,
        "meta": meta_rows,
        "comparison_vs_baseline": comparisons,
    }
    (output_root / "summary.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    print(json.dumps({"output_root": str(output_root)}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
