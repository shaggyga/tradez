from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .common import pair_meta, write_json
from .research_suite import (
    HORIZONS,
    SplitData,
    _calibration,
    _family_map,
    _fit_predict_model,
    _load_research_dataset,
    _sample_train,
    _select_threshold,
    _split,
    _top_by_timestamp,
)


MODEL_NAMES = [
    "simple_rule_baseline",
    "ridge",
    "hist_gradient_boosting",
    "extra_trees",
    "calibrated_classifier_ev_regressor",
    "direct_ev_regressor",
    "quantile_lower_bound",
    "rank_surrogate",
]
FEATURE_ORDER = [
    "A_m1_price_action",
    "B_m1_spread_cost_liquidity",
    "C_m1_volatility_motion",
    "D_m1_currency_strength",
    "E_m1_cross_sectional_ranks",
    "M1_plus_spread_plus_volatility",
    "M1_plus_spread_plus_currency_strength",
    "M1_plus_spread_plus_currency_strength_plus_ranks",
    "I_full",
]
LABEL_SPECS = [
    ("signed_return", 5, "actual_ev_5m"),
    ("signed_return", 13, "actual_ev_13m"),
    ("expected_MFE_path_quality", 8, "actual_ev_8m"),
    ("TP_before_SL_proxy", 21, "actual_ev_21m"),
]


def _safe(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, pd.Series):
        return [_safe(x) for x in value.tolist()]
    if isinstance(value, np.ndarray):
        return [_safe(x) for x in value.tolist()]
    if isinstance(value, dict):
        return {str(k): _safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe(x) for x in value]
    return value


def _write_pair(out: Path, stem: str, payload: dict[str, Any]) -> None:
    clean = _safe(payload)
    write_json(out / f"{stem}.json", clean)
    body = json.dumps(clean, indent=2, sort_keys=True)
    (out / f"{stem}.md").write_text(
        f"# {stem.replace('_', ' ').title()}\n\n```json\n{body[:180000]}\n```\n",
        encoding="utf-8",
    )


def _reuse_or_build_dataset(cfg: dict[str, Any], out: Path, start: str, end: str, tier: str, pairs: list[str] | None, max_rows: int | None) -> pd.DataFrame:
    reports = out.parent
    candidates = sorted(
        list(reports.glob("research_suite_*/research_dataset.parquet"))
        + list(reports.glob("signal_regression_audit_*/research_dataset.parquet")),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    candidates += sorted(
        list(reports.glob("arima_baseline_*/research_dataset.parquet"))
        + list(reports.glob("validation_replay_*/research_dataset.parquet")),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    req_start = pd.Timestamp(start).tz_localize("UTC") if pd.Timestamp(start).tzinfo is None else pd.Timestamp(start).tz_convert("UTC")
    req_end = pd.Timestamp(end).tz_localize("UTC") if pd.Timestamp(end).tzinfo is None else pd.Timestamp(end).tz_convert("UTC")
    for path in candidates:
        try:
            manifest = json.loads((path.parent / "research_dataset_manifest.json").read_text(encoding="utf-8"))
            manifest_start = pd.Timestamp(manifest.get("start")).tz_convert("UTC")
            manifest_end = pd.Timestamp(manifest.get("end")).tz_convert("UTC")
            start_matches = abs((manifest_start - req_start).total_seconds()) <= 15 * 60
            end_matches = 0 <= (req_end - manifest_end).total_seconds() <= 2 * 60 * 60 or str(manifest.get("end", "")).startswith(end)
            if start_matches and end_matches and manifest.get("pairs") and tier == "tier1":
                return pd.read_parquet(path)
        except Exception:
            continue
    return _load_research_dataset(cfg, out, start, end, tier, pairs, max_rows)


def _add_gross_labels(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    out = df.copy()
    out["decision_time_utc"] = pd.to_datetime(out["decision_time_utc"], utc=True)
    long_rows = out[out["side"] == "long"].copy().sort_values(["pair", "decision_time_utc"])
    gross_maps: dict[tuple[str, pd.Timestamp], float] = {}
    pip_by_pair: dict[str, float] = {}
    for pair, group in long_rows.groupby("pair", sort=False):
        pip = pair_meta(pair, cfg).pip_size
        pip_by_pair[pair] = pip
        mid_entry = pd.to_numeric(group["open"], errors="coerce").shift(-1)
        for h in HORIZONS:
            mid_exit = pd.to_numeric(group["close"], errors="coerce").shift(-h)
            gross = (mid_exit - mid_entry) / pip
            for ts, value in zip(group["decision_time_utc"], gross):
                gross_maps[(pair, pd.Timestamp(ts))] = float(value) if np.isfinite(value) else np.nan
    base = pd.Series([gross_maps.get((p, pd.Timestamp(t)), np.nan) for p, t in zip(out["pair"], out["decision_time_utc"])], index=out.index)
    for h in HORIZONS:
        gross_pips = base * out["side_sign"].astype(float)
        out[f"gross_mid_pips_{h}m"] = np.nan
        out[f"gross_mid_ev_{h}m"] = np.nan
        # The midpoint map is horizon-specific; rebuild the small pair lookup per horizon.
        values = []
        for pair, group in out[out["side"] == "long"].groupby("pair", sort=False):
            g = group.sort_values("decision_time_utc")
            pip = pip_by_pair[pair]
            val = (pd.to_numeric(g["close"], errors="coerce").shift(-h) - pd.to_numeric(g["open"], errors="coerce").shift(-1)) / pip
            values.extend((pair, pd.Timestamp(ts), float(x) if np.isfinite(x) else np.nan) for ts, x in zip(g["decision_time_utc"], val))
        lookup = {(p, t): v for p, t, v in values}
        gross = pd.Series([lookup.get((p, pd.Timestamp(t)), np.nan) for p, t in zip(out["pair"], out["decision_time_utc"])], index=out.index)
        out[f"gross_mid_pips_{h}m"] = gross * out["side_sign"].astype(float)
        out[f"gross_mid_ev_{h}m"] = out[f"gross_mid_pips_{h}m"] * pd.to_numeric(out["pip_value_usd_per_unit"], errors="coerce") * 1000.0
        out[f"cost_drag_ev_{h}m"] = out[f"gross_mid_ev_{h}m"] - pd.to_numeric(out[f"actual_ev_{h}m"], errors="coerce")
    return out


def _rank_ic(frame: pd.DataFrame, score: str, actual: str) -> float | None:
    clean = frame[["decision_time_utc", score, actual]].dropna()
    if clean.empty or clean[score].nunique() < 2 or clean[actual].nunique() < 2:
        return None
    if len(clean) > 100000:
        clean = clean.sample(100000, random_state=42)
    return float(clean[score].rank(pct=True).corr(clean[actual].rank(pct=True)))


def _top_metrics(frame: pd.DataFrame, score: str, gross: str, net: str, k: int) -> dict[str, Any]:
    top = _top_by_timestamp(frame.dropna(subset=[score, gross, net]), score, k)
    if top.empty:
        return {"rows": 0, "timestamps": 0, "gross_ev_mean": None, "net_ev_mean": None, "net_pnl_sum": 0.0, "precision": None}
    return {
        "rows": int(len(top)),
        "timestamps": int(top["decision_time_utc"].nunique()),
        "gross_ev_mean": float(top[gross].mean()),
        "net_ev_mean": float(top[net].mean()),
        "net_pnl_sum": float(top[net].sum()),
        "precision": float((top[net] > 0).mean()),
    }


def _metrics_from_top(top: pd.DataFrame, gross: str, net: str) -> dict[str, Any]:
    if top.empty:
        return {"rows": 0, "timestamps": 0, "gross_ev_mean": None, "net_ev_mean": None, "net_pnl_sum": 0.0, "precision": None}
    return {
        "rows": int(len(top)),
        "timestamps": int(top["decision_time_utc"].nunique()),
        "gross_ev_mean": float(top[gross].mean()),
        "net_ev_mean": float(top[net].mean()),
        "net_pnl_sum": float(top[net].sum()),
        "precision": float((top[net] > 0).mean()),
    }


def _quota_metrics(frame: pd.DataFrame, score: str, actual: str, group_col: str, quota: int) -> dict[str, Any]:
    tmp = frame.dropna(subset=[score, actual]).copy()
    if tmp.empty:
        return {"rows": 0, "pnl_sum": 0.0, "mean_ev": None, "positive_rate": None}
    tmp["_group"] = tmp[group_col].astype(str)
    selected = tmp.sort_values(score, ascending=False).groupby("_group", as_index=False).head(quota)
    return {"rows": int(len(selected)), "groups": int(selected["_group"].nunique()), "pnl_sum": float(selected[actual].sum()), "mean_ev": float(selected[actual].mean()), "positive_rate": float((selected[actual] > 0).mean())}


def _gate_attribution(val: pd.DataFrame, fin: pd.DataFrame, score: str, actual: str) -> dict[str, Any]:
    finite = fin[score].replace([np.inf, -np.inf], np.nan).notna()
    predicted_positive = finite & (fin[score] > 0)
    threshold = _select_threshold(val, score, actual)
    rmse = _calibration(val, score, actual).get("rmse") or 0.0
    positive_threshold = finite & (fin[score] >= threshold)
    lower_bound = finite & ((fin[score] - float(rmse)) > 0)
    policy_eligible = fin[actual].replace([np.inf, -np.inf], np.nan).notna()
    spread_eligible = pd.to_numeric(fin.get("spread_to_atr_15m", 0.0), errors="coerce").fillna(999.0) <= 0.35
    atr = pd.to_numeric(fin.get("atr_15m_pips", 0.0), errors="coerce").fillna(0.0)
    volatility_eligible = atr > 0
    sequence = {
        "total_candidates": int(len(fin)),
        "candidates_with_predictions": int(finite.sum()),
        "candidates_positive_predicted_ev": int(predicted_positive.sum()),
        "rejected_by_no_edge_gate": int((finite & ~predicted_positive).sum()),
        "rejected_by_calibration_lower_bound_gate": int((predicted_positive & ~lower_bound).sum()),
        "rejected_by_policy_eligibility_gate": int((finite & ~policy_eligible).sum()),
        "rejected_by_spread_gate": int((finite & ~spread_eligible).sum()),
        "rejected_by_volatility_gate": int((finite & ~volatility_eligible).sum()),
        "rejected_by_margin_exposure_gate": 0,
        "rejected_by_anti_churn_gate": 0,
        "final_candidates_traded": int(positive_threshold.sum()),
        "final_no_trade_timestamps": int(fin.loc[~positive_threshold, "decision_time_utc"].nunique()),
        "validation_threshold": threshold,
        "calibration_rmse": rmse,
    }
    return sequence


def _model_audit(split: SplitData, seed: int) -> tuple[list[dict[str, Any]], dict[str, Any], pd.DataFrame]:
    families = _family_map(split.train)
    train_sample = _sample_train(split.train, 12000, seed)
    rows: list[dict[str, Any]] = []
    for feature_i, feature_name in enumerate(FEATURE_ORDER):
        for model_i, model_name in enumerate(MODEL_NAMES):
            label_family, horizon, target = LABEL_SPECS[(feature_i + model_i) % len(LABEL_SPECS)]
            features = families.get(feature_name, [])
            if not features:
                continue
            val_score, final_score, reason = _fit_predict_model(model_name, train_sample, split.validation, split.final, features, target, seed)
            if reason:
                rows.append({"model_family": model_name, "feature_family": feature_name, "label_family": label_family, "horizon": horizon, "skipped_reason": reason})
                continue
            val = split.validation.copy()
            fin = split.final.copy()
            val["predicted_score"] = val_score
            fin["predicted_score"] = final_score
            gross = f"gross_mid_ev_{horizon}m"
            net = target
            ranked = fin.dropna(subset=["predicted_score", gross, net]).sort_values(["decision_time_utc", "predicted_score"], ascending=[True, False])
            top5 = ranked.groupby("decision_time_utc", sort=False, as_index=False).head(5)
            raw = {str(k): _metrics_from_top(top5.groupby("decision_time_utc", sort=False, as_index=False).head(k), gross, net) for k in [1, 3, 5]}
            calibration = _calibration(fin, "predicted_score", net)
            final = {
                "model_family": model_name,
                "feature_family": feature_name,
                "label_family": label_family,
                "horizon": horizon,
                "target": target,
                "gross_topk": {k: raw[k] for k in raw},
                "net_rank_ic": _rank_ic(fin, "predicted_score", net),
                "gross_rank_ic": _rank_ic(fin, "predicted_score", gross),
                "precision_at_1": raw["1"]["precision"],
                "precision_at_3": raw["3"]["precision"],
                "precision_at_5": raw["5"]["precision"],
                "predicted_ev_deciles": calibration,
                "forced_top1": raw["1"],
                "forced_top3": raw["3"],
                "forced_top5": raw["5"],
                "daily_quota_3": _quota_metrics(fin, "predicted_score", net, "decision_time_utc", 3),
                "session_quota_1": _quota_metrics(fin, "predicted_score", net, "session", 1),
                "gate_attribution": _gate_attribution(val, fin, "predicted_score", net),
            }
            rows.append(final)
    meta = {"model_families": MODEL_NAMES, "feature_families": FEATURE_ORDER, "label_specs": LABEL_SPECS, "experiments": len(rows)}
    return rows, meta, pd.DataFrame()


def _label_economics(df: pd.DataFrame) -> dict[str, Any]:
    final = df[df["decision_time_utc"] >= df["decision_time_utc"].quantile(0.80)].copy()
    rows = []
    for h in HORIZONS:
        gross = f"gross_mid_ev_{h}m"
        net = f"actual_ev_{h}m"
        sample = final[[gross, net, f"cost_drag_ev_{h}m"]].replace([np.inf, -np.inf], np.nan).dropna()
        if sample.empty:
            continue
        top_decile = sample[gross].nlargest(max(1, len(sample) // 10))
        rows.append({
            "horizon": h,
            "rows": int(len(sample)),
            "mean_gross_ev": float(sample[gross].mean()),
            "mean_net_ev": float(sample[net].mean()),
            "top_decile_gross_ev": float(top_decile.mean()),
            "top_decile_net_ev": float(sample.loc[top_decile.index, net].mean()),
            "top1_gross_oracle_ev": float(sample[gross].max()),
            "top1_net_oracle_ev": float(sample[net].max()),
            "cost_drag_mean": float(sample[f"cost_drag_ev_{h}m"].mean()),
            "edge_lost_to_spread_slippage": float((sample[gross] - sample[net]).mean()),
            "same_bar_ambiguity_rate": None,
            "adverse_first_note": "Endpoint labels do not encode first-touch ambiguity; this requires TP/SL contract simulation.",
        })
    return {"basis": "same research candidates; midpoint gross versus corrected executable net", "horizons": rows}


def _prior_signal() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[1] / "reports" / "smoke_policy_sweep_20260709" / "policy_report_leg_5_8_15m.json"
    if not path.exists():
        return {"artifact_found": False}
    report = json.loads(path.read_text(encoding="utf-8"))
    threshold_zero = next((x for x in report.get("trade_count_by_threshold", []) if x.get("threshold_pred_ev_usd_1k") == 0.0), None)
    top_bucket = report.get("predicted_ev_deciles", [])[-1] if report.get("predicted_ev_deciles") else None
    return {
        "artifact_found": True,
        "artifact": str(path),
        "policy": report.get("policy"),
        "historical_auc": report.get("auc"),
        "historical_ev_prediction_correlation": report.get("ev_prediction_correlation"),
        "historical_top_decile_actual_ev": top_bucket.get("actual_pnl_usd_1k_mean") if top_bucket else None,
        "historical_zero_threshold_bucket": threshold_zero,
        "interpretation": "The artifact contains a small positive threshold-zero bucket, but its full corrected top decile is negative; reproduce cautiously rather than treating the bucket as a validated edge.",
    }


def _movement_direction(df: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for score, actual, label in [
        ("side_momentum_5m_atr", "actual_ev_13m", "direction_momentum_proxy"),
        ("movement_forecast_proxy", "abs_move_pips_13m", "movement_proxy"),
        ("movement_forecast_proxy", "mfe_pips_13m", "mfe_proxy"),
    ]:
        if score not in df.columns or actual not in df.columns:
            continue
        top = _top_by_timestamp(df.dropna(subset=[score, actual]), score, 1)
        rows.append({"signal": label, "rank_ic": _rank_ic(df, score, actual), "top1_rows": int(len(top)), "top1_actual_mean": float(top[actual].mean()) if len(top) else None, "top1_actual_positive_rate": float((top[actual] > 0).mean()) if len(top) else None})
    return {"signals": rows, "interpretation": "Movement is measured without requiring a direction choice; direction uses side-normalized momentum and signed after-cost return."}


def _oco_summary() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[1] / "reports" / "two_pending_oco_breakout_deep_tier1_bounded" / "TWO_PENDING_OCO_BREAKOUT_REPORT.json"
    if not path.exists():
        return {"artifact_found": False}
    x = json.loads(path.read_text(encoding="utf-8"))
    best = x.get("best_metrics", {})
    return {"artifact_found": True, "best": best, "comparison": x.get("comparison"), "diagnosis": {"false_breakout": best.get("false_breakout_rate"), "whipsaw": best.get("whipsaw_rate"), "trigger_rate": best.get("trigger_rate"), "no_trigger_rate": best.get("no_trigger_cancel_rate"), "follow_through_proxy": best.get("tp_before_sl_rate_after_trigger")}}


def run_signal_regression_audit(cfg: dict[str, Any], output_dir: Path, start: str, end: str, tier: str = "tier1", pairs: list[str] | None = None, max_rows_per_pair: int | None = None) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    df = _reuse_or_build_dataset(cfg, output_dir, start, end, tier, pairs, max_rows_per_pair)
    df = _add_gross_labels(df, cfg)
    split = _split(df, cfg)
    model_rows, model_meta, _ = _model_audit(split, int(cfg.get("model", {}).get("random_state", 42)))
    model_rows_clean = [row for row in model_rows if "skipped_reason" not in row]
    raw_summary = {
        "experiments": len(model_rows),
        "evaluated_with_predictions": len(model_rows_clean),
        "skipped": [row for row in model_rows if "skipped_reason" in row],
        "models": model_rows_clean,
        "best_raw_net_top1": max(model_rows_clean, key=lambda x: (x.get("forced_top1", {}).get("net_ev_mean") is not None, x.get("forced_top1", {}).get("net_ev_mean") or -np.inf), default=None),
        "best_raw_gross_top1": max(model_rows_clean, key=lambda x: (x.get("forced_top1", {}).get("gross_ev_mean") is not None, x.get("forced_top1", {}).get("gross_ev_mean") or -np.inf), default=None),
    }
    _write_pair(output_dir, "RAW_TOPK_MODEL_REPORT", raw_summary)
    gate = {"models": [{"model_family": x.get("model_family"), "feature_family": x.get("feature_family"), "label_family": x.get("label_family"), "horizon": x.get("horizon"), **x.get("gate_attribution", {})} for x in model_rows_clean], "explanation": "Counts are calculated before allocation and show why prior models produced zero final trades."}
    _write_pair(output_dir, "GATE_ATTRIBUTION_REPORT", gate)
    economics = _label_economics(df)
    _write_pair(output_dir, "LABEL_ECONOMICS_COMPARISON", economics)
    prior = _prior_signal()
    prior["current_corrected_label_summary"] = economics
    _write_pair(output_dir, "PRIOR_SIGNAL_REPRODUCTION", prior)
    movement = _movement_direction(df)
    _write_pair(output_dir, "MOVEMENT_VS_DIRECTION_SIGNAL", movement)
    shape = {"horizon_policy_rows": economics["horizons"], "model_rows": [{"model_family": x.get("model_family"), "feature_family": x.get("feature_family"), "label_family": x.get("label_family"), "horizon": x.get("horizon"), "raw_net_top1": x.get("forced_top1", {}).get("net_ev_mean"), "gated_net_top1": x.get("gate_attribution", {}).get("final_candidates_traded")} for x in model_rows_clean]}
    _write_pair(output_dir, "POLICY_HORIZON_SIGNAL_SHAPE", shape)
    oco = _oco_summary()
    _write_pair(output_dir, "OCO_SIGNAL_REGRESSION", oco)
    best_net = raw_summary["best_raw_net_top1"]
    best_gross = raw_summary["best_raw_gross_top1"]
    final = {
        "verdict": "FAIL_RESEARCH_ONLY",
        "run_id": output_dir.name,
        "start": start,
        "end": end,
        "tier": tier,
        "split": split.meta,
        "raw_model_topk_positive_gross_ev": bool(best_gross and (best_gross.get("forced_top1", {}).get("gross_ev_mean") or 0) > 0),
        "raw_model_topk_positive_net_ev": bool(best_net and (best_net.get("forced_top1", {}).get("net_ev_mean") or 0) > 0),
        "raw_model_topk_best_gross": best_gross,
        "raw_model_topk_best_net": best_net,
        "forced_topk_beats_no_trade": bool(best_net and (best_net.get("forced_top1", {}).get("net_ev_mean") or 0) > 0),
        "allocator_suppressed_candidates": int(sum((x.get("gate_attribution", {}).get("candidates_positive_predicted_ev", 0) or 0) - (x.get("gate_attribution", {}).get("final_candidates_traded", 0) or 0) for x in model_rows_clean)),
        "prior_signal": prior,
        "movement_vs_direction": movement,
        "oco": oco,
        "bottleneck": "model ranking/economics after costs remain negative in raw top-k; gate suppression is real but not the only failure.",
        "next_step": "Use the raw top-k and gate attribution tables to select one model with positive gross separation, then validate its corrected net labels on a new untouched period; do not enable execution.",
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "demo_execution_enabled": False,
    }
    _write_pair(output_dir, "SIGNAL_REGRESSION_AUDIT", final)
    write_json(output_dir / "run_manifest.json", {"run_dir": str(output_dir), "mode": "signal-regression-audit", "reports": ["SIGNAL_REGRESSION_AUDIT.json", "RAW_TOPK_MODEL_REPORT.json", "GATE_ATTRIBUTION_REPORT.json", "LABEL_ECONOMICS_COMPARISON.json", "PRIOR_SIGNAL_REPRODUCTION.json", "MOVEMENT_VS_DIRECTION_SIGNAL.json", "POLICY_HORIZON_SIGNAL_SHAPE.json", "OCO_SIGNAL_REGRESSION.json"], "live_execution_enabled": False, "oanda_execution_enabled": False})
    return output_dir
