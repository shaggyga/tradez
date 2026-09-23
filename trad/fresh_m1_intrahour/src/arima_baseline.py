from __future__ import annotations

import importlib.util
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .common import write_json
from .economics import execution_settings_from_config
from .replay_metrics import (
    cost_stress_for_trades,
    evaluate_score_policies,
    gate_attribution,
    json_safe,
    quick_evaluate_validation_policies,
    rank_ic,
    topk_result,
    write_report_pair,
)
from .research_suite import SplitData, _score_rule_branches, _split, _trade_metrics
from .signal_regression_audit import _add_gross_labels, _reuse_or_build_dataset

try:
    from sklearn.linear_model import Ridge
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import make_pipeline
except Exception:  # pragma: no cover - captured in runtime reports
    Ridge = None
    StandardScaler = None
    make_pipeline = None


AR_P_VALUES = [1, 2, 3, 5, 8, 13]
ARIMA_P_VALUES = [1, 2, 3, 5]
ARIMA_Q_VALUES = [0, 1]
DRIFT_WINDOWS = [15, 30, 60, 120]
FORECAST_HORIZONS = [1, 2, 3, 5, 8, 13, 21, 30]
VOL_WINDOWS = [5, 15, 30, 60]


def statsmodels_available() -> bool:
    return importlib.util.find_spec("statsmodels") is not None


def _slippage_map(cfg: dict[str, Any], tiers: pd.Series) -> pd.Series:
    cache: dict[str, float] = {}
    out = []
    for tier in tiers.fillna("tier1").astype(str):
        if tier not in cache:
            cache[tier] = float(execution_settings_from_config(cfg, tier).slippage_pips_round_trip)
        out.append(cache[tier])
    return pd.Series(out, index=tiers.index, dtype=float)


def _cost_pips(frame: pd.DataFrame, cfg: dict[str, Any]) -> pd.Series:
    tiers = frame.get("tier", pd.Series("tier1", index=frame.index))
    spread = pd.to_numeric(frame.get("spread_pips_used", 0.0), errors="coerce").fillna(0.0)
    return spread + _slippage_map(cfg, tiers)


def _cost_ev(frame: pd.DataFrame, cfg: dict[str, Any]) -> pd.Series:
    return _cost_pips(frame, cfg) * pd.to_numeric(frame.get("pip_value_usd_per_unit", 0.0), errors="coerce").fillna(0.0) * 1000.0


def _base_pair_features(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    base = df[df["side"].eq("long")].copy()
    base = base.sort_values(["pair", "decision_time_utc"]).reset_index(drop=True)
    base["decision_time_utc"] = pd.to_datetime(base["decision_time_utc"], utc=True)
    base["ret_1m_pips"] = pd.to_numeric(base.get("return_1m_pips"), errors="coerce")
    base["abs_ret_1m_pips"] = base["ret_1m_pips"].abs()
    base["cost_pips"] = _cost_pips(base, cfg)
    for w in DRIFT_WINDOWS:
        base[f"drift_mean_{w}m"] = base.groupby("pair")["ret_1m_pips"].transform(lambda s: s.rolling(w, min_periods=max(3, w // 4)).mean())
    for w in VOL_WINDOWS:
        if f"rv_{w}m_pips" not in base.columns:
            base[f"rv_{w}m_pips"] = base.groupby("pair")["ret_1m_pips"].transform(lambda s: s.rolling(w, min_periods=max(3, w // 4)).std())
        if f"range_{w}m_pips" not in base.columns:
            high = pd.to_numeric(base.get("high"), errors="coerce")
            low = pd.to_numeric(base.get("low"), errors="coerce")
            # This fallback is rarely used because pair features usually provide ranges.
            base[f"range_{w}m_pips"] = (high - low).groupby(base["pair"]).transform(lambda s: s.rolling(w, min_periods=max(3, w // 4)).mean())
        base[f"abs_ret_mean_{w}m"] = base.groupby("pair")["abs_ret_1m_pips"].transform(lambda s: s.rolling(w, min_periods=max(3, w // 4)).mean())
    for p in AR_P_VALUES:
        for lag in range(1, p + 1):
            base[f"ar_lag_{lag}"] = base.groupby("pair")["ret_1m_pips"].shift(lag - 1)
    for h in FORECAST_HORIZONS:
        actual_pips = pd.to_numeric(base.get(f"actual_pips_{h}m"), errors="coerce")
        base[f"gross_future_pips_{h}m"] = actual_pips + base["cost_pips"]
        base[f"abs_future_pips_{h}m"] = base[f"gross_future_pips_{h}m"].abs()
    keep = ["pair", "decision_time_utc", "cost_pips"]
    keep += [c for c in base.columns if c.startswith(("drift_mean_", "rv_", "range_", "abs_ret_mean_", "ar_lag_", "gross_future_pips_", "abs_future_pips_"))]
    return base[keep]


def _merge_pair_features(part: pd.DataFrame, base_features: pd.DataFrame) -> pd.DataFrame:
    out = part.copy()
    out["decision_time_utc"] = pd.to_datetime(out["decision_time_utc"], utc=True)
    return out.merge(base_features, on=["pair", "decision_time_utc"], how="left", suffixes=("", "_pairbase"))


def _fit_ar_forecasts(base_features: pd.DataFrame, split: SplitData, skipped: list[dict[str, Any]]) -> dict[tuple[int, int], str]:
    forecast_cols: dict[tuple[int, int], str] = {}
    if Ridge is None or StandardScaler is None or make_pipeline is None:
        for p in AR_P_VALUES:
            for h in FORECAST_HORIZONS:
                skipped.append({"variant": f"ar_p{p}_{h}m", "reason": "sklearn Ridge/StandardScaler unavailable"})
        return forecast_cols
    train_times = set(pd.to_datetime(split.train["decision_time_utc"], utc=True).unique())
    train_base = base_features[base_features["decision_time_utc"].isin(train_times)].copy()
    for p in AR_P_VALUES:
        lag_cols = [f"ar_lag_{lag}" for lag in range(1, p + 1)]
        for h in FORECAST_HORIZONS:
            target = f"gross_future_pips_{h}m"
            col = f"ar_p{p}_forecast_pips_{h}m"
            fit = train_base[lag_cols + [target]].replace([np.inf, -np.inf], np.nan).dropna()
            if len(fit) < max(200, p * 50):
                skipped.append({"variant": col, "reason": f"too few train rows ({len(fit)})"})
                continue
            try:
                model = make_pipeline(StandardScaler(), Ridge(alpha=2.0))
                model.fit(fit[lag_cols], fit[target].astype(float))
                pred_frame = base_features[lag_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)
                base_features[col] = model.predict(pred_frame)
                forecast_cols[(p, h)] = col
            except Exception as exc:
                skipped.append({"variant": col, "reason": str(exc)})
    return forecast_cols


def _variant_scores(frame: pd.DataFrame, cfg: dict[str, Any], variant: dict[str, Any]) -> pd.Series:
    h = int(variant["horizon"])
    pip_value = pd.to_numeric(frame.get("pip_value_usd_per_unit", 0.0), errors="coerce").fillna(0.0) * 1000.0
    cost_pips = _cost_pips(frame, cfg)
    side_sign = pd.to_numeric(frame.get("side_sign", 1.0), errors="coerce").fillna(1.0)
    kind = variant["kind"]
    if kind == "signed_forecast":
        forecast = pd.to_numeric(frame.get(str(variant["forecast_col"])), errors="coerce")
        return ((forecast * side_sign) - cost_pips) * pip_value
    if kind == "movement_forecast":
        movement = pd.to_numeric(frame.get(str(variant["forecast_col"])), errors="coerce")
        return (movement - cost_pips) * pip_value
    if kind == "random_walk_no_change":
        return -cost_pips * pip_value
    raise ValueError(f"unknown AR baseline kind: {kind}")


def _build_variants(base_features: pd.DataFrame, split: SplitData, skipped: list[dict[str, Any]]) -> list[dict[str, Any]]:
    variants: list[dict[str, Any]] = []
    for h in FORECAST_HORIZONS:
        variants.append({"name": f"random_walk_no_change_{h}m", "kind": "random_walk_no_change", "horizon": h})
        for w in DRIFT_WINDOWS:
            variants.append({"name": f"drift_mean_{w}m_x_{h}m", "kind": "signed_forecast", "horizon": h, "forecast_col": f"drift_mean_{w}m"})
        for w in VOL_WINDOWS:
            for source in [f"rv_{w}m_pips", f"range_{w}m_pips", f"abs_ret_mean_{w}m"]:
                if source in base_features.columns:
                    variants.append({"name": f"movement_{source}_x_{h}m", "kind": "movement_forecast", "horizon": h, "forecast_col": source})
    ar_cols = _fit_ar_forecasts(base_features, split, skipped)
    for (p, h), col in ar_cols.items():
        variants.append({"name": f"ar_p{p}_returns_{h}m", "kind": "signed_forecast", "horizon": h, "forecast_col": col})
    return variants


def _best_simple_baseline(split: SplitData) -> dict[str, Any]:
    branches = _score_rule_branches(split)
    baselines = {"no_trade": {"validation": {"pnl_account": 0.0}, "final": {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0}}}
    for name in [
        "random_candidate",
        "top_1m_momentum_continuation",
        "top_5m_momentum_continuation",
        "top_5m_snapback_reversal",
        "top_volatility_compression_breakout",
        "strongest_vs_weakest_currency",
        "lowest_spread_top_momentum",
    ]:
        if branches.get(name):
            baselines[name] = branches[name]
    best_name, best_report = max(baselines.items(), key=lambda kv: float(kv[1].get("validation", {}).get("pnl_account", 0.0)))
    return {"name": best_name, "report": best_report, "all": baselines}


def run_arima_baseline_analysis(
    cfg: dict[str, Any],
    output_dir: Path,
    start: str,
    end: str,
    tier: str = "tier1",
    pairs: list[str] | None = None,
    max_rows_per_pair: int | None = None,
    split: SplitData | None = None,
    dataset: pd.DataFrame | None = None,
    write_reports: bool = True,
    ml_winner: dict[str, Any] | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stats_ok = statsmodels_available()
    skipped: list[dict[str, Any]] = []
    if not stats_ok:
        for p in ARIMA_P_VALUES:
            for q in ARIMA_Q_VALUES:
                skipped.append({"variant": f"statsmodels_arima_p{p}_q{q}", "reason": "statsmodels not installed"})
        skipped.append({"variant": "sarimax_exogenous", "reason": "statsmodels not installed"})

    if split is None:
        df = dataset if dataset is not None else _reuse_or_build_dataset(cfg, output_dir, start, end, tier, pairs, max_rows_per_pair)
        df = _add_gross_labels(df, cfg)
        split = _split(df, cfg)
    base_all = pd.concat([split.train, split.validation, split.final], ignore_index=True)
    base_features = _base_pair_features(base_all, cfg)
    variants = _build_variants(base_features, split, skipped)
    train_aug = _merge_pair_features(split.train, base_features)
    val_aug = _merge_pair_features(split.validation, base_features)
    final_aug = _merge_pair_features(split.final, base_features)

    reports: list[dict[str, Any]] = []
    for variant in variants:
        h = int(variant["horizon"])
        actual_col = f"actual_ev_{h}m"
        if actual_col not in val_aug.columns or actual_col not in final_aug.columns:
            skipped.append({"variant": variant["name"], "reason": f"missing {actual_col}"})
            continue
        val = val_aug.copy()
        try:
            val["predicted_ev"] = _variant_scores(val, cfg, variant)
        except Exception as exc:
            skipped.append({"variant": variant["name"], "reason": str(exc)})
            continue
        if val["predicted_ev"].replace([np.inf, -np.inf], np.nan).notna().sum() < 100:
            skipped.append({"variant": variant["name"], "reason": "too few finite predictions"})
            continue
        evaluation = quick_evaluate_validation_policies(val, "predicted_ev", actual_col, cfg)
        best_policy = evaluation["best_policy"]
        report = {
            "variant": variant["name"],
            "kind": variant["kind"],
            "horizon": h,
            "forecast_col": variant.get("forecast_col"),
            "selected_policy": best_policy,
            "threshold": evaluation["threshold"],
            "validation": evaluation["validation"][best_policy],
            "validation_policy_results": evaluation["validation"],
            "validation_calibration": evaluation["validation_calibration"],
            "variant_spec": variant,
        }
        reports.append(report)

    if reports:
        best_screen = max(
            reports,
            key=lambda r: (
                float(r["validation"].get("pnl_account", 0.0)),
                float(r["validation"].get("trade_count", 0.0) or 0.0),
            ),
        )
        variant = best_screen["variant_spec"]
        h = int(variant["horizon"])
        actual_col = f"actual_ev_{h}m"
        gross_col = f"gross_mid_ev_{h}m"
        val = val_aug.copy()
        fin = final_aug.copy()
        val["predicted_ev"] = _variant_scores(val, cfg, variant)
        fin["predicted_ev"] = _variant_scores(fin, cfg, variant)
        evaluation = evaluate_score_policies(val, fin, "predicted_ev", actual_col, cfg, variant["name"])
        best_policy = evaluation["best_policy"]
        final_trades = evaluation["selected_final_trades"]
        best = {
            **{k: v for k, v in best_screen.items() if k != "variant_spec"},
            "selected_policy": best_policy,
            "threshold": evaluation["threshold"],
            "validation": evaluation["validation"][best_policy],
            "final": evaluation["final"][best_policy],
            "validation_policy_results": evaluation["validation"],
            "final_policy_results": evaluation["final"],
            "rank_ic": rank_ic(fin, "predicted_ev", actual_col),
            "movement_rank_ic": rank_ic(fin, "predicted_ev", f"abs_move_pips_{h}m") if f"abs_move_pips_{h}m" in fin.columns else None,
            "mfe_rank_ic": rank_ic(fin, "predicted_ev", f"mfe_pips_{h}m") if f"mfe_pips_{h}m" in fin.columns else None,
            "raw_topk": {str(k): topk_result(fin, "predicted_ev", actual_col, gross_col if gross_col in fin.columns else None, k) for k in [1, 3, 5]},
            "gate_attribution": gate_attribution(
                val,
                fin,
                "predicted_ev",
                actual_col,
                cfg,
                best_policy,
                evaluation["threshold"],
                evaluation["validation_rmse"],
                final_trades,
            ),
            "selected_final_trades": final_trades,
        }
    else:
        best = {
            "variant": None,
            "selected_policy": None,
            "validation": {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0},
            "final": {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0},
            "selected_final_trades": pd.DataFrame(),
            "horizon": None,
        }
    simple = _best_simple_baseline(split)
    no_trade = {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0}
    random_walk = next((r for r in reports if str(r.get("variant", "")).startswith("random_walk_no_change")), None)
    random_walk_final = None
    if random_walk:
        rw_variant = random_walk["variant_spec"]
        rw_h = int(rw_variant["horizon"])
        rw_actual = f"actual_ev_{rw_h}m"
        rw_val = val_aug.copy()
        rw_fin = final_aug.copy()
        rw_val["predicted_ev"] = _variant_scores(rw_val, cfg, rw_variant)
        rw_fin["predicted_ev"] = _variant_scores(rw_fin, cfg, rw_variant)
        rw_eval = evaluate_score_policies(rw_val, rw_fin, "predicted_ev", rw_actual, cfg, str(rw_variant["name"]))
        random_walk_final = rw_eval["final"][rw_eval["best_policy"]]
    actual_col = f"actual_ev_{best.get('horizon')}m" if best.get("horizon") else "actual_ev_1m"
    stress = cost_stress_for_trades(best.get("selected_final_trades", pd.DataFrame()), actual_col, cfg, f"arima_baseline|{best.get('variant')}")
    comparison = {
        "no_trade": no_trade,
        "random_walk": random_walk_final,
        "best_simple_baseline": simple["report"].get("final", {}),
        "validation_replay_ml_winner": ml_winner or {"available": False, "reason": "not available at arima-baseline command time"},
    }
    summary = {
        "run_id": output_dir.name,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "command_mode": "arima-baseline",
        "start": start,
        "end": end,
        "tier": tier,
        "statsmodels_available": stats_ok,
        "variants_attempted": [r["variant"] for r in reports],
        "variants_skipped": skipped,
        "best_validation_selected_variant": {k: v for k, v in best.items() if k not in {"selected_final_trades", "variant_spec"}},
        "validation_result": best.get("validation"),
        "untouched_final_result": best.get("final"),
        "comparison": comparison,
        "cost_stress": stress,
        "split": split.meta,
        "hyperparameters_selected_on_validation_only": True,
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "demo_execution_enabled": False,
    }
    if write_reports:
        write_report_pair(output_dir, "ARIMA_BASELINE_REPORT", summary)
        write_json(output_dir / "run_manifest.json", {
            "run_dir": str(output_dir),
            "mode": "arima-baseline",
            "report_json": str(output_dir / "ARIMA_BASELINE_REPORT.json"),
            "statsmodels_available": stats_ok,
            "live_execution_enabled": False,
            "oanda_execution_enabled": False,
        })
    return {
        "summary": summary,
        "reports": reports,
        "best": best,
        "best_simple_baseline": simple,
        "split": split,
        "statsmodels_available": stats_ok,
        "variants_skipped": skipped,
        "output_dir": output_dir,
    }


def run_arima_baseline(
    cfg: dict[str, Any],
    output_dir: Path,
    start: str,
    end: str,
    tier: str = "tier1",
    pairs: list[str] | None = None,
    max_rows_per_pair: int | None = None,
) -> Path:
    result = run_arima_baseline_analysis(
        cfg,
        output_dir,
        start=start,
        end=end,
        tier=tier,
        pairs=pairs,
        max_rows_per_pair=max_rows_per_pair,
        write_reports=True,
    )
    return output_dir / "ARIMA_BASELINE_REPORT.json"
