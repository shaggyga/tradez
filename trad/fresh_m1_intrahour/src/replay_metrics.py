from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .common import max_drawdown, write_json
from .economics import execution_settings_from_config
from .research_suite import _calibration, _select_threshold, _top_by_timestamp, _trade_metrics


def json_safe(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (np.ndarray,)):
        return [json_safe(x) for x in value.tolist()]
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, pd.Series):
        return [json_safe(x) for x in value.tolist()]
    if isinstance(value, pd.DataFrame):
        return {"rows": int(len(value)), "columns": list(value.columns)}
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items() if k != "selected_trades"}
    if isinstance(value, (list, tuple)):
        return [json_safe(x) for x in value]
    return value


def write_report_pair(output_dir: Path, stem: str, payload: dict[str, Any], max_md_chars: int = 180000) -> None:
    clean = json_safe(payload)
    write_json(output_dir / f"{stem}.json", clean)
    body = json.dumps(clean, indent=2, sort_keys=True)
    (output_dir / f"{stem}.md").write_text(
        f"# {stem.replace('_', ' ').title()}\n\n```json\n{body[:max_md_chars]}\n```\n",
        encoding="utf-8",
    )


def rank_ic(frame: pd.DataFrame, score_col: str, actual_col: str, max_rows: int = 100000) -> float | None:
    tmp = frame[[score_col, actual_col]].replace([np.inf, -np.inf], np.nan).dropna()
    if len(tmp) < 5 or tmp[score_col].nunique() < 2 or tmp[actual_col].nunique() < 2:
        return None
    if len(tmp) > max_rows:
        tmp = tmp.sample(max_rows, random_state=42)
    return float(tmp[score_col].rank(pct=True).corr(tmp[actual_col].rank(pct=True)))


def _ensure_score(df: pd.DataFrame, score_col: str, actual_col: str) -> pd.DataFrame:
    return df.replace([np.inf, -np.inf], np.nan).dropna(subset=[score_col, actual_col]).copy()


def _apply_current_allocator(
    frame: pd.DataFrame,
    score_col: str,
    actual_col: str,
    threshold: float,
    cfg: dict[str, Any],
) -> tuple[pd.DataFrame, int]:
    if math.isinf(float(threshold)):
        return frame.iloc[0:0].copy(), 0
    allocator = cfg.get("allocator", {})
    min_ev = float(allocator.get("min_ev_usd", 0.01))
    uncertainty = float(allocator.get("uncertainty_buffer_usd", 0.01))
    anti_churn_minutes = int(allocator.get("anti_churn_minutes", 3))
    min_score = max(float(threshold), min_ev + uncertainty)
    tmp = _ensure_score(frame, score_col, actual_col)
    if tmp.empty:
        return tmp, 0
    spread_ok = pd.to_numeric(tmp.get("spread_to_atr_15m", 0.0), errors="coerce").fillna(999.0) <= 0.35
    vol_ok = pd.to_numeric(tmp.get("atr_15m_pips", 0.0), errors="coerce").fillna(0.0) > 0.0
    tmp = tmp[(tmp[score_col] >= min_score) & spread_ok & vol_ok].copy()
    if tmp.empty:
        return tmp, 0
    top = _top_by_timestamp(tmp, score_col, 1).sort_values("decision_time_utc")
    kept = []
    last_key: tuple[str, str] | None = None
    last_time: pd.Timestamp | None = None
    rejected = 0
    for _, row in top.iterrows():
        key = (str(row.get("pair")), str(row.get("side")))
        ts = pd.Timestamp(row["decision_time_utc"])
        if last_key == key and last_time is not None and ts - last_time < pd.Timedelta(minutes=anti_churn_minutes):
            rejected += 1
            continue
        kept.append(row)
        last_key = key
        last_time = ts
    if not kept:
        return top.iloc[0:0].copy(), rejected
    return pd.DataFrame(kept).reset_index(drop=True), rejected


def select_trades_for_policy(
    frame: pd.DataFrame,
    score_col: str,
    actual_col: str,
    policy: str,
    threshold: float,
    rmse: float,
    cfg: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    tmp = _ensure_score(frame, score_col, actual_col)
    if tmp.empty:
        return tmp, {"anti_churn_rejections": 0}
    if policy == "raw_top1":
        return _top_by_timestamp(tmp, score_col, 1), {"anti_churn_rejections": 0}
    if policy == "raw_top3":
        return _top_by_timestamp(tmp, score_col, 3), {"anti_churn_rejections": 0}
    if policy == "raw_top5":
        return _top_by_timestamp(tmp, score_col, 5), {"anti_churn_rejections": 0}
    if policy == "fixed_trade_quota_top1":
        return _top_by_timestamp(tmp, score_col, 1), {"anti_churn_rejections": 0}
    if policy == "positive_predicted_ev":
        return _top_by_timestamp(tmp[tmp[score_col] > 0.0], score_col, 1), {"anti_churn_rejections": 0}
    if policy == "calibrated_lower_bound":
        return _top_by_timestamp(tmp[(tmp[score_col] - float(rmse)) > 0.0], score_col, 1), {"anti_churn_rejections": 0}
    if policy == "no_edge_gated":
        if math.isinf(float(threshold)):
            return tmp.iloc[0:0].copy(), {"anti_churn_rejections": 0}
        return _top_by_timestamp(tmp[tmp[score_col] >= float(threshold)], score_col, 1), {"anti_churn_rejections": 0}
    if policy == "current_allocator_gated":
        trades, rejected = _apply_current_allocator(tmp, score_col, actual_col, threshold, cfg)
        return trades, {"anti_churn_rejections": int(rejected)}
    raise ValueError(f"unknown replay policy: {policy}")


def evaluate_score_policies(
    validation: pd.DataFrame,
    final: pd.DataFrame,
    score_col: str,
    actual_col: str,
    cfg: dict[str, Any],
    branch_name: str,
) -> dict[str, Any]:
    threshold = _select_threshold(validation, score_col, actual_col)
    calibration = _calibration(validation, score_col, actual_col)
    rmse = float(calibration.get("rmse") or 0.0)
    policies = [
        "raw_top1",
        "raw_top3",
        "raw_top5",
        "fixed_trade_quota_top1",
        "positive_predicted_ev",
        "calibrated_lower_bound",
        "no_edge_gated",
        "current_allocator_gated",
    ]
    validation_reports: dict[str, Any] = {}
    final_reports: dict[str, Any] = {}
    selected_frames: dict[str, pd.DataFrame] = {}
    policy_meta: dict[str, Any] = {}
    for policy in policies:
        val_trades, val_meta = select_trades_for_policy(validation, score_col, actual_col, policy, threshold, rmse, cfg)
        fin_trades, fin_meta = select_trades_for_policy(final, score_col, actual_col, policy, threshold, rmse, cfg)
        validation_reports[policy] = _trade_metrics(val_trades, validation, score_col, actual_col, f"{branch_name}|{policy}")
        final_reports[policy] = _trade_metrics(fin_trades, final, score_col, actual_col, f"{branch_name}|{policy}")
        selected_frames[policy] = fin_trades
        policy_meta[policy] = {"validation": val_meta, "final": fin_meta}
    best_policy = max(
        policies,
        key=lambda p: (
            validation_reports[p].get("pnl_account", 0.0),
            validation_reports[p].get("trade_count", 0),
        ),
    )
    return {
        "threshold": threshold,
        "validation_calibration": calibration,
        "validation_rmse": rmse,
        "policies": policies,
        "best_policy": best_policy,
        "validation": validation_reports,
        "final": final_reports,
        "selected_final_trades": selected_frames[best_policy],
        "policy_meta": policy_meta,
        "final_rank_ic": rank_ic(final, score_col, actual_col),
    }


def quick_trade_metrics(trades: pd.DataFrame, actual_col: str, initial_equity: float = 1000.0) -> dict[str, Any]:
    pnl = pd.to_numeric(trades.get(actual_col, pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    wins = pnl[pnl > 0.0]
    losses = pnl[pnl < 0.0]
    equity = (initial_equity + pnl.cumsum()).tolist() if len(pnl) else [initial_equity]
    return {
        "pnl_account": float(pnl.sum()) if len(pnl) else 0.0,
        "return_pct": float(pnl.sum() / initial_equity * 100.0) if len(pnl) else 0.0,
        "trade_count": int(len(pnl)),
        "win_rate": float((pnl > 0.0).mean()) if len(pnl) else None,
        "profit_factor": float(wins.sum() / abs(losses.sum())) if len(losses) and abs(losses.sum()) > 0 else None,
        "max_drawdown_pct": max_drawdown(equity),
    }


def quick_evaluate_validation_policies(
    validation: pd.DataFrame,
    score_col: str,
    actual_col: str,
    cfg: dict[str, Any],
) -> dict[str, Any]:
    threshold = _select_threshold(validation, score_col, actual_col)
    calibration = _calibration(validation, score_col, actual_col)
    rmse = float(calibration.get("rmse") or 0.0)
    policies = [
        "raw_top1",
        "raw_top3",
        "raw_top5",
        "fixed_trade_quota_top1",
        "positive_predicted_ev",
        "calibrated_lower_bound",
        "no_edge_gated",
        "current_allocator_gated",
    ]
    validation_reports: dict[str, Any] = {}
    for policy in policies:
        trades, _ = select_trades_for_policy(validation, score_col, actual_col, policy, threshold, rmse, cfg)
        validation_reports[policy] = quick_trade_metrics(trades, actual_col)
    best_policy = max(
        policies,
        key=lambda p: (
            validation_reports[p].get("pnl_account", 0.0),
            validation_reports[p].get("trade_count", 0),
        ),
    )
    return {
        "threshold": threshold,
        "validation_calibration": calibration,
        "validation_rmse": rmse,
        "policies": policies,
        "best_policy": best_policy,
        "validation": validation_reports,
    }


def gate_attribution(
    validation: pd.DataFrame,
    final: pd.DataFrame,
    score_col: str,
    actual_col: str,
    cfg: dict[str, Any],
    selected_policy: str,
    threshold: float,
    rmse: float,
    final_trades: pd.DataFrame,
) -> dict[str, Any]:
    finite = final[score_col].replace([np.inf, -np.inf], np.nan).notna()
    positive = finite & (final[score_col] > 0.0)
    lower_bound = finite & ((final[score_col] - float(rmse)) > 0.0)
    policy_eligible = final[actual_col].replace([np.inf, -np.inf], np.nan).notna()
    spread_ok = pd.to_numeric(final.get("spread_to_atr_15m", 0.0), errors="coerce").fillna(999.0) <= 0.35
    volatility_ok = pd.to_numeric(final.get("atr_15m_pips", 0.0), errors="coerce").fillna(0.0) > 0.0
    _, allocator_meta = select_trades_for_policy(final, score_col, actual_col, "current_allocator_gated", threshold, rmse, cfg)
    traded_ts = set(pd.to_datetime(final_trades.get("decision_time_utc", pd.Series(dtype=str)), utc=True)) if not final_trades.empty else set()
    all_ts = set(pd.to_datetime(final["decision_time_utc"], utc=True).dropna().unique())
    return {
        "total_candidates": int(len(final)),
        "candidates_with_predictions": int(finite.sum()),
        "candidates_positive_predicted_ev": int(positive.sum()),
        "rejected_by_no_edge_gate": int((finite & ~positive).sum()),
        "rejected_by_calibration_lower_bound": int((positive & ~lower_bound).sum()),
        "rejected_by_policy_eligibility": int((finite & ~policy_eligible).sum()),
        "rejected_by_spread_cost_gate": int((finite & ~spread_ok).sum()),
        "rejected_by_volatility_gate": int((finite & ~volatility_ok).sum()),
        "rejected_by_margin_exposure_gate": 0,
        "rejected_by_anti_churn": int(allocator_meta.get("anti_churn_rejections", 0)),
        "final_traded_candidates": int(len(final_trades)),
        "final_no_trade_timestamps": int(len(all_ts - traded_ts)),
        "validation_threshold": None if math.isinf(float(threshold)) else float(threshold),
        "validation_threshold_is_infinite": bool(math.isinf(float(threshold))),
        "validation_rmse": float(rmse),
        "selected_policy": selected_policy,
    }


def _slippage_pips_for_rows(trades: pd.DataFrame, cfg: dict[str, Any]) -> pd.Series:
    if trades.empty:
        return pd.Series(dtype=float)
    tiers = trades.get("tier", pd.Series("tier1", index=trades.index)).fillna("tier1").astype(str)
    cache: dict[str, float] = {}
    for tier in tiers.unique():
        cache[tier] = float(execution_settings_from_config(cfg, tier).slippage_pips_round_trip)
    return tiers.map(cache).astype(float)


def cost_stress_for_trades(trades: pd.DataFrame, actual_col: str, cfg: dict[str, Any], label: str) -> dict[str, Any]:
    if trades.empty or actual_col not in trades.columns:
        return {
            "label": label,
            "base_pnl_account": 0.0,
            "trade_count": 0,
            "stress": [],
            "breakeven_spread_multiplier": None,
            "breakeven_slippage_pips": None,
            "edge_survives_mild_stress": False,
            "edge_exists_only_under_fantasy_costs": False,
        }
    pnl = pd.to_numeric(trades[actual_col], errors="coerce").fillna(0.0)
    pip_value = pd.to_numeric(trades.get("pip_value_usd_per_unit", 0.0), errors="coerce").fillna(0.0) * 1000.0
    spread_cost = pd.to_numeric(trades.get("spread_pips_used", 0.0), errors="coerce").fillna(0.0) * pip_value
    slippage_cost = _slippage_pips_for_rows(trades, cfg) * pip_value
    base = float(pnl.sum())
    spread_sum = float(spread_cost.sum())
    slip_unit_sum = float(pip_value.sum())
    base_slip_sum = float(slippage_cost.sum())
    rows = []
    for name, multiplier in [
        ("zero_cost_fantasy", None),
        ("spread_0p5x", 0.5),
        ("base_spread", 1.0),
        ("spread_1p25x", 1.25),
        ("spread_1p5x", 1.5),
        ("spread_2x", 2.0),
    ]:
        if multiplier is None:
            stressed = base + spread_sum + base_slip_sum
        else:
            stressed = base - ((float(multiplier) - 1.0) * spread_sum)
        rows.append({"stress": name, "pnl_account": stressed, "return_pct": stressed / 1000.0 * 100.0, "survives": stressed > 0.0})
    for name, add_pips in [("slippage_plus_0p1", 0.1), ("slippage_plus_0p3", 0.3)]:
        stressed = base - (float(add_pips) * slip_unit_sum)
        rows.append({"stress": name, "pnl_account": stressed, "return_pct": stressed / 1000.0 * 100.0, "survives": stressed > 0.0})
    breakeven_spread = 1.0 + (base / spread_sum) if spread_sum > 0 else None
    breakeven_slip = base / slip_unit_sum if slip_unit_sum > 0 else None
    mild = all(
        next((r["survives"] for r in rows if r["stress"] == name), False)
        for name in ["base_spread", "spread_1p25x", "slippage_plus_0p1"]
    )
    fantasy = bool(next((r["survives"] for r in rows if r["stress"] == "zero_cost_fantasy"), False)) and base <= 0
    return {
        "label": label,
        "base_pnl_account": base,
        "trade_count": int(len(trades)),
        "stress": rows,
        "breakeven_spread_multiplier": breakeven_spread,
        "breakeven_slippage_pips": breakeven_slip,
        "edge_survives_mild_stress": bool(mild),
        "edge_exists_only_under_fantasy_costs": bool(fantasy),
    }


def topk_result(frame: pd.DataFrame, score_col: str, actual_col: str, gross_col: str | None = None, k: int = 1) -> dict[str, Any]:
    needed = [score_col, actual_col] + ([gross_col] if gross_col else [])
    tmp = frame.replace([np.inf, -np.inf], np.nan).dropna(subset=needed)
    top = _top_by_timestamp(tmp, score_col, k)
    if top.empty:
        return {"rows": 0, "timestamps": 0, "actual_ev_mean": None, "pnl_account": 0.0, "precision": None, "gross_ev_mean": None}
    return {
        "rows": int(len(top)),
        "timestamps": int(top["decision_time_utc"].nunique()),
        "actual_ev_mean": float(top[actual_col].mean()),
        "pnl_account": float(top[actual_col].sum()),
        "precision": float((top[actual_col] > 0.0).mean()),
        "gross_ev_mean": float(top[gross_col].mean()) if gross_col and gross_col in top.columns else None,
    }
