#!/usr/bin/env python3
"""Build robust-candidate and cost-sensitivity reports for SR trendline mining."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

import oanda_sr_trendline_filter_optimizer as opt


ROOT = Path(__file__).resolve().parent
OPTIMIZER_ROOT = ROOT / "data" / "oanda_training_manager" / "reports" / "sr_trendline_optimizer"


def finite_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    return number if math.isfinite(number) else default


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


def token_series(frame: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    if len(columns) == 1:
        return frame[columns[0]].astype(str)
    return frame[list(columns)].astype(str).agg("|".join, axis=1)


def mask_from_candidate(frame: pd.DataFrame, row: pd.Series) -> np.ndarray:
    family = str(row.get("family", ""))
    if family == "train_selected_groups":
        columns = str(row.get("selector_columns", "")).split("|")
        values = {
            token
            for token in str(row.get("selected_values", "")).split(";")
            if token and token.lower() != "nan"
        }
        if not columns or not values:
            return np.zeros(len(frame), dtype=bool)
        return token_series(frame, columns).isin(values).to_numpy()
    if family == "numeric_grid":
        mask = np.ones(len(frame), dtype=bool)
        if str(row.get("timeframe_filter", "ALL")) != "ALL":
            mask &= frame["timeframe"].astype(str).to_numpy() == str(row["timeframe_filter"])
        if str(row.get("direction_filter", "ALL")) != "ALL":
            mask &= frame["direction"].astype(str).to_numpy() == str(row["direction_filter"])
        if str(row.get("group_filter", "ALL")) != "ALL":
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
        return mask
    return np.zeros(len(frame), dtype=bool)


def flatten(split: str, summary: dict[str, Any]) -> dict[str, Any]:
    return {f"{split}_{key}": value for key, value in summary.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--optimizer-dir", type=Path, default=OPTIMIZER_ROOT)
    parser.add_argument("--sweep-root", type=Path, default=opt.SWEEP_ROOT)
    parser.add_argument("--limit", type=int, default=40)
    args = parser.parse_args()

    candidates_path = args.optimizer_dir / "optimizer_candidates.csv"
    candidates = pd.read_csv(candidates_path)
    robust = candidates[
        (candidates["train_total_r"] > 0.0)
        & (candidates["test_total_r"] > 0.0)
        & (candidates["full_total_r"] > 0.0)
        & (candidates["train_profit_factor"] > 1.0)
        & (candidates["test_profit_factor"] > 1.0)
        & (candidates["train_trades"] >= 50)
        & (candidates["test_trades"] >= 25)
    ].copy()
    robust = robust.sort_values(
        ["test_total_r", "test_profit_factor", "full_total_r"],
        ascending=False,
    ).head(args.limit)

    frame = opt.load_trades(args.sweep_root, opt.TIMEFRAMES)
    context = opt.build_eval_context(frame)
    cost_rows: list[dict[str, Any]] = []
    for rank, (_, row) in enumerate(robust.iterrows(), start=1):
        mask = mask_from_candidate(frame, row)
        for cost_bps in [0.0, 0.25, 0.50, 1.0, 2.0, 5.0]:
            summaries = opt.summarize_mask(context, mask, cost_bps=cost_bps)
            out = {
                "rank": rank,
                "name": row["name"],
                "family": row["family"],
                "selected_values": row.get("selected_values", ""),
                "cost_bps": cost_bps,
            }
            for split in ["full", "train", "test"]:
                out.update(flatten(split, summaries[split]))
            cost_rows.append(out)

    robust_csv = args.optimizer_dir / "robust_candidates.csv"
    cost_csv = args.optimizer_dir / "robust_candidate_cost_sensitivity.csv"
    robust.to_csv(robust_csv, index=False)
    write_csv(cost_csv, cost_rows)
    lines = [
        "# Robust Candidate Cost Sensitivity",
        "",
        "| rank | candidate | cost bps | train R | test R | test PF | full R | full PF |",
        "|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in cost_rows:
        if float(row["cost_bps"]) not in {0.0, 0.5, 1.0, 2.0}:
            continue
        lines.append(
            f"| {row['rank']} | {str(row['name'])[:80]} | {row['cost_bps']:.2f} | "
            f"{finite_float(row.get('train_total_r')):.2f} | {finite_float(row.get('test_total_r')):.2f} | "
            f"{finite_float(row.get('test_profit_factor')):.2f} | {finite_float(row.get('full_total_r')):.2f} | "
            f"{finite_float(row.get('full_profit_factor')):.2f} |"
        )
    md_path = args.optimizer_dir / "robust_candidate_cost_sensitivity.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")
    payload = {
        "robust_count": int(len(robust)),
        "robust_csv": str(robust_csv),
        "cost_csv": str(cost_csv),
        "cost_md": str(md_path),
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
