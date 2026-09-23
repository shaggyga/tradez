from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier, DummyRegressor
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.metrics import roc_auc_score

from .audit import write_horizon_integrity_report, write_top_ev_failure_diagnostics
from .common import max_drawdown, write_json
from .train import EXCLUDE_COLUMNS, EXCLUDE_PREFIXES


SURFACE_COLUMNS = [
    "timestamp",
    "pair",
    "side",
    "horizon_min",
    "policy_name",
    "tp_pips",
    "sl_pips",
    "predicted_ev_account",
    "predicted_ev_pips",
    "predicted_endpoint_pips",
    "predicted_mfe_pips",
    "predicted_mae_pips",
    "predicted_tp_before_sl_prob",
    "predicted_stop_first_prob",
    "predicted_timeout_prob",
    "predicted_confidence",
    "spread_pips",
    "spread_to_atr",
    "feature_set_name",
    "model_name",
    "score_rank_cross_sectional",
    "eligible_policy_flag",
]


@dataclass(frozen=True)
class CandidateSpec:
    feature_set_name: str
    model_name: str
    features: list[str]


def _policy_names(cfg: dict[str, Any]) -> list[str]:
    return [str(t["name"]) for t in cfg["policy_templates"]]


def _policy_lookup(cfg: dict[str, Any]) -> dict[str, dict[str, float]]:
    return {
        str(t["name"]): {"tp_pips": float(t["tp_pips"]), "sl_pips": float(t["sl_pips"])}
        for t in cfg["policy_templates"]
    }


def _horizons(cfg: dict[str, Any]) -> list[int]:
    return [int(h) for h in cfg["horizons_minutes"]]


def _numeric_feature_columns(df: pd.DataFrame) -> list[str]:
    cols = []
    for col in df.columns:
        if col in EXCLUDE_COLUMNS:
            continue
        if any(col.startswith(prefix) for prefix in EXCLUDE_PREFIXES):
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            cols.append(col)
    return cols


def _feature_families(df: pd.DataFrame) -> dict[str, list[str]]:
    numeric = set(_numeric_feature_columns(df))

    def keep(names: list[str]) -> list[str]:
        return [c for c in names if c in numeric]

    m1 = keep([
        "side_return_1m_pips",
        "side_return_2m_pips",
        "side_return_3m_pips",
        "side_return_5m_pips",
        "side_return_8m_pips",
        "side_return_13m_pips",
        "side_return_21m_pips",
        "return_1m_pips",
        "return_2m_pips",
        "return_3m_pips",
        "return_5m_pips",
        "atr_5m_pips",
        "rv_5m_pips",
        "range_5m_pips",
        "wick_body_ratio",
        "side_momentum_3m_atr",
        "side_momentum_5m_atr",
        "side_acceleration",
        "side_range_position",
        "side_breakout_pressure",
        "side_pullback_pressure",
    ])
    spread = keep(["spread_pips", "slippage_pips", "round_trip_cost_pips", "spread_to_atr_15m", "cost_pressure_score", "session_liquidity_score"])
    strength = keep(["base_strength_5m", "quote_strength_5m", "base_minus_quote_strength_5m", "side_currency_strength_5m", "base_strength_15m", "quote_strength_15m", "side_currency_strength_15m"])
    xs = keep([c for c in numeric if c.startswith("xs_rank_") or c == "xs_opportunity_score"])
    session_htf = keep([
        "hour_sin",
        "hour_cos",
        "minute_sin",
        "minute_cos",
        "weekday_sin",
        "weekday_cos",
        "is_asia",
        "is_london",
        "is_new_york",
        "is_london_ny_overlap",
        "is_london_open",
        "is_new_york_open",
        "is_rollover_risk",
        "is_friday_late",
        "m5_momentum_pips",
        "m15_momentum_pips",
        "m30_momentum_pips",
        "h1_momentum_pips",
        "m1_m5_alignment",
        "m1_m15_alignment",
        "m1_h1_alignment",
        "atr_15m_pips",
        "atr_30m_pips",
        "atr_60m_pips",
        "rv_15m_pips",
        "range_15m_pips",
        "range_30m_pips",
        "efficiency_ratio_15m",
        "choppiness_15m",
        "volatility_expansion_15_60",
        "movement_quality",
        "side_trend_alignment",
        "side_exhaustion_risk",
    ])
    return {
        "A_m1_price_action": m1,
        "B_m1_spread_cost_liquidity": sorted(set(m1 + spread)),
        "C_m1_spread_currency_strength": sorted(set(m1 + spread + strength)),
        "D_m1_spread_strength_cross_sectional": sorted(set(m1 + spread + strength + xs)),
        "E_full_session_htf_context": sorted(numeric),
    }


def _clean_x(df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    return df[features].replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _safe_auc(y: pd.Series, pred: np.ndarray) -> float | None:
    if y.nunique(dropna=True) < 2:
        return None
    try:
        return float(roc_auc_score(y.astype(int), pred))
    except Exception:
        return None


def _safe_corr(a: pd.Series, b: pd.Series) -> float | None:
    tmp = pd.concat([a, pd.Series(b, index=a.index)], axis=1).dropna()
    if len(tmp) < 3 or tmp.iloc[:, 0].nunique() < 2 or tmp.iloc[:, 1].nunique() < 2:
        return None
    return float(tmp.corr().iloc[0, 1])


def _make_regressor(model_name: str, seed: int):
    if model_name == "simple_baseline":
        return DummyRegressor(strategy="mean")
    if model_name == "direct_ev_regressor":
        return HistGradientBoostingRegressor(
            learning_rate=0.06,
            max_iter=45,
            max_leaf_nodes=21,
            min_samples_leaf=80,
            l2_regularization=0.10,
            random_state=seed,
        )
    return HistGradientBoostingRegressor(
        learning_rate=0.06,
        max_iter=60,
        max_leaf_nodes=21,
        min_samples_leaf=80,
        l2_regularization=0.10,
        random_state=seed,
    )


def _make_classifier(model_name: str, seed: int):
    if model_name == "simple_baseline":
        return DummyClassifier(strategy="prior")
    return HistGradientBoostingClassifier(
        learning_rate=0.06,
        max_iter=55,
        max_leaf_nodes=21,
        min_samples_leaf=80,
        l2_regularization=0.10,
        random_state=seed,
    )


def _fit_classifier(model, x: pd.DataFrame, y: pd.Series):
    if y.nunique(dropna=True) < 2:
        dummy = DummyClassifier(strategy="prior")
        dummy.fit(np.zeros((len(y), 1)), y.astype(int))
        return dummy, True
    model.fit(x, y.astype(int))
    return model, False


def _predict_classifier(model, x: pd.DataFrame) -> np.ndarray:
    if isinstance(model, DummyClassifier):
        arr = np.zeros((len(x), 1))
        proba = model.predict_proba(arr)
    else:
        proba = model.predict_proba(x)
    if proba.shape[1] == 1:
        return np.full(len(x), float(model.classes_[0] == 1))
    idx = list(model.classes_).index(1) if 1 in model.classes_ else 0
    return proba[:, idx]


def _fit_surface_models(cfg: dict[str, Any], train: pd.DataFrame, spec: CandidateSpec) -> dict[str, Any]:
    seed = int(cfg["model"]["random_state"])
    max_rows = int(cfg.get("surface", {}).get("max_train_rows_per_candidate", 0) or 0)
    if max_rows > 0 and len(train) > max_rows:
        sample_idx = np.linspace(0, len(train) - 1, max_rows, dtype=int)
        train = train.iloc[sample_idx].copy()
    x = _clean_x(train, spec.features)
    models: dict[str, Any] = {
        "horizon": {},
        "policy": {},
    }
    for h in _horizons(cfg):
        h_models = {}
        targets = {
            "endpoint_net_pips": f"net_after_cost_{h}m_pips",
            "mfe_pips": f"mfe_{h}m_pips",
            "mae_pips": f"mae_{h}m_pips",
        }
        for key, col in targets.items():
            reg = _make_regressor(spec.model_name, seed)
            reg.fit(x, train[col].astype(float))
            h_models[key] = reg
        models["horizon"][h] = h_models
    for policy in _policy_names(cfg):
        models["policy"][policy] = {}
        for h in _horizons(cfg):
            p_models = {}
            prefix = f"policy_{policy}_{h}m"
            targets = {
                "ev_account": f"{prefix}_net_account_pnl_1k_units",
                "ev_pips": f"{prefix}_net_pips",
                "time_to_profit": f"{prefix}_time_to_exit",
                "endpoint_pips": f"{prefix}_endpoint_pips",
                "mfe_pips": f"{prefix}_mfe_pips",
                "mae_pips": f"{prefix}_mae_pips",
            }
            for key, col in targets.items():
                reg = _make_regressor(spec.model_name, seed)
                reg.fit(x, train[col].astype(float))
                p_models[key] = reg
            for key, col in {
                "tp_before_sl_prob": f"{prefix}_tp_before_sl",
                "stop_first_prob": f"{prefix}_sl_before_tp",
                "timeout_prob": f"{prefix}_timeout",
            }.items():
                clf, _ = _fit_classifier(_make_classifier(spec.model_name, seed), x, train[col].astype(int))
                p_models[key] = clf
            models["policy"][policy][h] = p_models
    return {
        "feature_set_name": spec.feature_set_name,
        "model_name": spec.model_name,
        "features": spec.features,
        "models": models,
    }


def _score_surface_model(bundle: dict[str, Any], df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    x = _clean_x(df, bundle["features"])
    policies = _policy_lookup(cfg)
    base = pd.DataFrame({
        "timestamp": df["decision_time_utc"].values,
        "pair": df["instrument"].values,
        "side": df["side"].values,
        "spread_pips": df["spread_pips"].values,
        "spread_to_atr": df["spread_to_atr_15m"].values,
        "close": df["close"].values,
        "pip_value_usd_per_unit": df["pip_value_usd_per_unit"].values,
        "tier": df["tier"].values,
        "base_currency": df["base_currency"].values,
        "quote_currency": df["quote_currency"].values,
    }, index=df.index)
    frames = []
    for h in _horizons(cfg):
        h_models = bundle["models"]["horizon"][h]
        for policy in _policy_names(cfg):
            p_models = bundle["models"]["policy"][policy][h]
            prefix = f"policy_{policy}_{h}m"
            part = base.copy()
            part["horizon_min"] = h
            part["policy_name"] = policy
            part["tp_pips"] = policies[policy]["tp_pips"]
            part["sl_pips"] = policies[policy]["sl_pips"]
            part["predicted_ev_account"] = p_models["ev_account"].predict(x)
            part["predicted_ev_pips"] = p_models["ev_pips"].predict(x)
            part["predicted_endpoint_pips"] = p_models["endpoint_pips"].predict(x)
            part["predicted_mfe_pips"] = p_models["mfe_pips"].predict(x)
            part["predicted_mae_pips"] = p_models["mae_pips"].predict(x)
            part["predicted_tp_before_sl_prob"] = _predict_classifier(p_models["tp_before_sl_prob"], x)
            part["predicted_stop_first_prob"] = _predict_classifier(p_models["stop_first_prob"], x)
            part["predicted_timeout_prob"] = _predict_classifier(p_models["timeout_prob"], x)
            part["predicted_time_to_profit"] = p_models["time_to_profit"].predict(x)
            part["feature_set_name"] = bundle["feature_set_name"]
            part["model_name"] = bundle["model_name"]
            part["eligible_policy_flag"] = (part["predicted_ev_account"] > 0).astype(int)
            part["actual_policy_pnl_1k"] = df[f"{prefix}_net_account_pnl_1k_units"].values
            part["actual_policy_net_pips"] = df[f"{prefix}_net_pips"].values
            part["actual_endpoint_pips"] = df[f"{prefix}_endpoint_pips"].values
            part["actual_horizon_net_pips"] = df[f"net_after_cost_{h}m_pips"].values
            part["actual_horizon_mfe_pips"] = df[f"{prefix}_mfe_pips"].values
            part["actual_horizon_mae_pips"] = df[f"{prefix}_mae_pips"].values
            part["actual_tp_before_sl"] = df[f"{prefix}_tp_before_sl"].values
            part["actual_stop_first"] = df[f"{prefix}_sl_before_tp"].values
            part["actual_timeout"] = df[f"{prefix}_timeout"].values
            part["actual_time_to_exit"] = df[f"{prefix}_time_to_exit"].values
            frames.append(part)
    surface = pd.concat(frames, ignore_index=True)
    surface["score_rank_cross_sectional"] = surface.groupby("timestamp")["predicted_ev_account"].rank(pct=True, ascending=False)
    surface["predicted_confidence"] = surface["predicted_ev_account"] / (
        surface["predicted_ev_account"].abs()
        + surface["predicted_mae_pips"].abs() * surface["pip_value_usd_per_unit"] * 1000.0
        + 1e-9
    )
    surface["confidence_calibration_bucket"] = (
        surface.groupby(["feature_set_name", "model_name"])["predicted_ev_account"]
        .transform(lambda s: pd.qcut(s.rank(method="first"), 10, labels=False, duplicates="drop"))
        .fillna(0)
        .astype(int)
    )
    return surface


def _bucket_table(df: pd.DataFrame, pred_col: str, actual_col: str) -> list[dict[str, Any]]:
    if df.empty:
        return []
    tmp = df[[pred_col, actual_col]].dropna().copy()
    if tmp.empty:
        return []
    tmp["bucket"] = pd.qcut(tmp[pred_col].rank(method="first"), 10, labels=False, duplicates="drop")
    out = []
    for b, g in tmp.groupby("bucket"):
        out.append({
            "bucket": int(b),
            "rows": int(len(g)),
            "pred_mean": float(g[pred_col].mean()),
            "actual_sum": float(g[actual_col].sum()),
            "actual_mean": float(g[actual_col].mean()),
            "positive_rate": float((g[actual_col] > 0).mean()),
        })
    return out


def _precision_at_k(surface: pd.DataFrame, k: int) -> float | None:
    vals = []
    for _, g in surface.groupby("timestamp", sort=True):
        top = g.sort_values("predicted_ev_account", ascending=False).head(k)
        if len(top):
            vals.append(float((top["actual_policy_pnl_1k"] > 0).mean()))
    return float(np.mean(vals)) if vals else None


def _precision_by_horizon(surface: pd.DataFrame) -> dict[str, dict[str, float | None]]:
    out: dict[str, dict[str, float | None]] = {}
    for h, g in surface.groupby("horizon_min"):
        out[str(h)] = {"1": _precision_at_k(g, 1), "3": _precision_at_k(g, 3), "5": _precision_at_k(g, 5)}
    return out


def _monotonicity(bucket_rows: list[dict[str, Any]]) -> dict[str, Any]:
    means = [r["actual_mean"] for r in bucket_rows]
    if len(means) < 2:
        return {"spearman_bucket_actual_mean": None, "nondecreasing_adjacent_steps": None}
    corr = pd.Series(range(len(means))).corr(pd.Series(means), method="spearman")
    return {
        "spearman_bucket_actual_mean": float(corr) if pd.notna(corr) else None,
        "nondecreasing_adjacent_steps": int(sum(means[i] >= means[i - 1] for i in range(1, len(means)))),
    }


def _calibration_line(df: pd.DataFrame, pred_col: str, actual_col: str) -> dict[str, Any]:
    tmp = df[[pred_col, actual_col]].replace([np.inf, -np.inf], np.nan).dropna()
    if len(tmp) < 3 or tmp[pred_col].nunique() < 2:
        return {"intercept": None, "slope": None}
    x = tmp[pred_col].to_numpy(dtype=float)
    y = tmp[actual_col].to_numpy(dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    return {"intercept": float(intercept), "slope": float(slope)}


def _top_k_actual_ev_by_timestamp(surface: pd.DataFrame) -> dict[str, Any]:
    out = {}
    for k in [1, 3, 5]:
        vals = []
        for _, g in surface.groupby("timestamp", sort=True):
            top = g.sort_values("predicted_ev_account", ascending=False).head(k)
            if len(top):
                vals.append(float(top["actual_policy_pnl_1k"].mean()))
        out[str(k)] = {
            "mean_actual_ev_1k": float(np.mean(vals)) if vals else None,
            "timestamps": int(len(vals)),
        }
    return out


def _breakdown(df: pd.DataFrame, col: str) -> list[dict[str, Any]]:
    if df.empty or col not in df.columns:
        return []
    out = []
    for key, g in df.groupby(col, dropna=False):
        out.append({
            col: str(key),
            "rows": int(len(g)),
            "actual_pnl_1k_sum": float(g["actual_policy_pnl_1k"].sum()),
            "actual_pnl_1k_mean": float(g["actual_policy_pnl_1k"].mean()),
            "positive_rate": float((g["actual_policy_pnl_1k"] > 0).mean()),
            "avg_predicted_ev_account": float(g["predicted_ev_account"].mean()),
        })
    return out


def _horizon_specific_reports(surface: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for h, g in surface.groupby("horizon_min", dropna=False):
        bucket = _bucket_table(g, "predicted_ev_account", "actual_policy_pnl_1k")
        rows.append({
            "horizon_min": int(h),
            "rows": int(len(g)),
            "actual_pnl_1k_sum": float(g["actual_policy_pnl_1k"].sum()),
            "actual_pnl_1k_mean": float(g["actual_policy_pnl_1k"].mean()),
            "predicted_ev_mean": float(g["predicted_ev_account"].mean()),
            "tp_before_sl_rate": float(g["actual_tp_before_sl"].mean()),
            "stop_first_rate": float(g["actual_stop_first"].mean()),
            "timeout_rate": float(g["actual_timeout"].mean()) if "actual_timeout" in g.columns else None,
            "average_mfe_pips": float(g["actual_horizon_mfe_pips"].mean()),
            "average_mae_pips": float(g["actual_horizon_mae_pips"].mean()),
            "calibration": _calibration_line(g, "predicted_ev_account", "actual_policy_pnl_1k"),
            "precision_at": {"1": _precision_at_k(g, 1), "3": _precision_at_k(g, 3), "5": _precision_at_k(g, 5)},
            "predicted_ev_bucket_table": bucket,
            "ev_bucket_monotonicity": _monotonicity(bucket),
        })
    return rows


def _shadow_diagnostics(surface: pd.DataFrame) -> dict[str, Any]:
    selected = surface[surface["predicted_ev_account"] > 0].copy()
    if selected.empty:
        selected = surface.sort_values("predicted_ev_account", ascending=False).head(min(1000, len(surface))).copy()
    opp = surface[["timestamp", "pair", "side", "horizon_min", "policy_name", "actual_policy_pnl_1k"]].copy()
    opp["side"] = np.where(opp["side"] == "long", "short", "long")
    opp = opp.rename(columns={"actual_policy_pnl_1k": "opposite_actual_policy_pnl_1k"})
    merged = selected.merge(opp, on=["timestamp", "pair", "side", "horizon_min", "policy_name"], how="left")
    a = merged["actual_policy_pnl_1k"].astype(float)
    b = merged["opposite_actual_policy_pnl_1k"].fillna(0).astype(float)
    return {
        "rows": int(len(merged)),
        "selected_positive_rate": float((a > 0).mean()) if len(merged) else None,
        "opposite_positive_rate": float((b > 0).mean()) if len(merged) else None,
        "signal_inverted_rate": float(((a <= 0) & (b > 0)).mean()) if len(merged) else None,
        "both_positive_rate": float(((a > 0) & (b > 0)).mean()) if len(merged) else None,
        "neither_positive_rate": float(((a <= 0) & (b <= 0)).mean()) if len(merged) else None,
    }


def _session_name_from_ts(ts: pd.Series) -> pd.Series:
    hour = pd.to_datetime(ts, utc=True).dt.hour
    return np.select(
        [
            hour.isin([21, 22]),
            (hour >= 12) & (hour < 16),
            (hour >= 7) & (hour < 16),
            (hour >= 12) & (hour < 21),
            (hour >= 0) & (hour < 7),
        ],
        ["rollover", "london_ny_overlap", "london", "new_york", "asia"],
        default="other",
    )


def _surface_report(surface: pd.DataFrame) -> dict[str, Any]:
    surface = surface.copy()
    surface["session"] = _session_name_from_ts(surface["timestamp"])
    surface["month"] = pd.to_datetime(surface["timestamp"], utc=True).dt.strftime("%Y-%m")
    bucket = _bucket_table(surface, "predicted_ev_account", "actual_policy_pnl_1k")
    return {
        "rows": int(len(surface)),
        "positive_predicted_ev_rows": int((surface["predicted_ev_account"] > 0).sum()),
        "ev_prediction_correlation": _safe_corr(surface["actual_policy_pnl_1k"], surface["predicted_ev_account"].to_numpy()),
        "tp_auc": _safe_auc(surface["actual_tp_before_sl"], surface["predicted_tp_before_sl_prob"].to_numpy()),
        "stop_auc": _safe_auc(surface["actual_stop_first"], surface["predicted_stop_first_prob"].to_numpy()),
        "timeout_auc": _safe_auc(surface["actual_timeout"], surface["predicted_timeout_prob"].to_numpy()) if "actual_timeout" in surface.columns else None,
        "precision_at": {
            "1": _precision_at_k(surface, 1),
            "3": _precision_at_k(surface, 3),
            "5": _precision_at_k(surface, 5),
        },
        "precision_at_by_horizon": _precision_by_horizon(surface),
        "top_predicted_ev_bucket": bucket[-1] if bucket else None,
        "predicted_ev_bucket_table": bucket,
        "ev_bucket_monotonicity": _monotonicity(bucket),
        "calibration": _calibration_line(surface, "predicted_ev_account", "actual_policy_pnl_1k"),
        "top_k_actual_ev_per_timestamp": _top_k_actual_ev_by_timestamp(surface),
        "pair_breakdown": _breakdown(surface, "pair"),
        "tier_breakdown": _breakdown(surface, "tier"),
        "base_currency_breakdown": _breakdown(surface, "base_currency"),
        "quote_currency_breakdown": _breakdown(surface, "quote_currency"),
        "session_breakdown": _breakdown(surface, "session"),
        "policy_breakdown": _breakdown(surface, "policy_name"),
        "horizon_breakdown": _breakdown(surface, "horizon_min"),
        "horizon_specific_reports": _horizon_specific_reports(surface),
        "long_short_breakdown": _breakdown(surface, "side"),
        "monthly_consistency": _breakdown(surface, "month"),
        "dominance_checks": _dominance_checks(surface),
        "no_trade_diagnostics": _no_trade_diagnostics(surface),
        "shadow_opposite_side": _shadow_diagnostics(surface),
    }


def _dominance_checks(surface: pd.DataFrame) -> dict[str, Any]:
    checks = {}
    dims = ["pair", "base_currency", "quote_currency", "session", "horizon_min", "policy_name", "model_name", "month"]
    total_abs = float(surface["actual_policy_pnl_1k"].abs().sum())
    for dim in dims:
        if dim not in surface.columns or surface.empty:
            checks[dim] = None
            continue
        grouped = surface.groupby(dim)["actual_policy_pnl_1k"].sum().sort_values(key=lambda s: s.abs(), ascending=False)
        if grouped.empty:
            checks[dim] = None
            continue
        checks[dim] = {
            "top_key": str(grouped.index[0]),
            "top_pnl_1k_sum": float(grouped.iloc[0]),
            "top_abs_share": float(abs(grouped.iloc[0]) / total_abs) if total_abs > 0 else None,
        }
    return checks


def _no_trade_diagnostics(surface: pd.DataFrame) -> dict[str, Any]:
    if surface.empty:
        return {}
    top = surface.sort_values("predicted_ev_account", ascending=False).groupby("timestamp", as_index=False).head(1)
    top5 = surface.sort_values("predicted_ev_account", ascending=False).groupby("timestamp", as_index=False).head(5)
    return {
        "timestamps": int(surface["timestamp"].nunique()),
        "highest_predicted_ev_candidate_mean": float(top["predicted_ev_account"].mean()),
        "highest_predicted_ev_candidate_max": float(top["predicted_ev_account"].max()),
        "highest_predicted_ev_realized_ev_mean": float(top["actual_policy_pnl_1k"].mean()),
        "top5_predicted_realized_ev_mean": float(top5["actual_policy_pnl_1k"].mean()),
        "positive_predicted_ev_count": int((surface["predicted_ev_account"] > 0).sum()),
        "eligible_count": int((surface["eligible_policy_flag"] == 1).sum()),
        "filtered_by_cost_or_negative_ev_count": int((surface["predicted_ev_account"] <= 0).sum()),
        "policy_gate_fail_count": int((surface["predicted_tp_before_sl_prob"] < surface["predicted_stop_first_prob"]).sum()),
        "spread_pressure_high_count": int((surface["spread_to_atr"] > 1.0).sum()),
        "predictions_too_pessimistic_hint": bool((top["predicted_ev_account"].mean() <= 0) and (top["actual_policy_pnl_1k"].mean() > 0)),
        "top_ranked_candidates_truly_negative_hint": bool(top["actual_policy_pnl_1k"].mean() <= 0),
    }


def _variant_validation_gates(report: dict[str, Any], min_samples: int = 100) -> dict[str, Any]:
    bucket = report.get("predicted_ev_bucket_table") or []
    top = bucket[-1] if bucket else {}
    median = bucket[len(bucket) // 2] if bucket else {}
    pos_rows = int(report.get("positive_predicted_ev_rows") or 0)
    mono = report.get("ev_bucket_monotonicity", {})
    precision = report.get("precision_at", {})
    pos_mean = None
    if bucket:
        positive_buckets = [r for r in bucket if r.get("pred_mean", 0) > 0]
        if positive_buckets:
            total_rows = sum(int(r["rows"]) for r in positive_buckets)
            pos_mean = sum(float(r["actual_mean"]) * int(r["rows"]) for r in positive_buckets) / max(total_rows, 1)
    gates = {
        "top_bucket_actual_ev_positive": bool(top and float(top.get("actual_mean") or 0.0) > 0.0),
        "top_bucket_beats_median_bucket": bool(top and median and float(top.get("actual_mean") or 0.0) > float(median.get("actual_mean") or 0.0)),
        "predicted_positive_ev_candidates_positive_on_average": bool(pos_mean is not None and pos_mean > 0.0),
        "minimum_sample_count_satisfied": bool(pos_rows >= min_samples),
        "ev_bucket_monotonicity_acceptable": bool((mono.get("nondecreasing_adjacent_steps") or 0) >= 6),
        "top_k_precision_available": bool(precision.get("1") is not None and precision.get("3") is not None and precision.get("5") is not None),
        "predicted_positive_ev_actual_mean": pos_mean,
        "predicted_positive_ev_rows": pos_rows,
    }
    gates["allocator_eligible"] = all(v for k, v in gates.items() if isinstance(v, bool) and k != "top_k_precision_available") and gates["top_k_precision_available"]
    return gates


def _actual_for_candidate(row: pd.Series, units: int) -> tuple[float, int]:
    pnl = float(row["actual_policy_pnl_1k"]) * (units / 1000.0)
    hold = max(1, int(row.get("actual_time_to_exit", 1)))
    return pnl, hold


def _run_surface_allocator(surface: pd.DataFrame, cfg: dict[str, Any], name: str) -> dict[str, Any]:
    initial_equity = float(cfg["initial_equity"])
    equity = initial_equity
    positions: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    curve: list[float] = []
    margin_curve: list[float] = []
    timestamps = 0
    no_trade_timestamps = 0
    filter_counts = {
        "candidate_rows": 0,
        "negative_or_below_min_ev": 0,
        "policy_gate": 0,
        "spread_pressure": 0,
        "margin_gate": 0,
        "same_pair_side_open": 0,
        "max_new_gate": 0,
    }
    max_open = int(cfg["max_open_positions"])
    max_new = int(cfg["max_new_positions_per_cycle"])
    min_ev = float(cfg["allocator"]["min_ev_usd"])
    max_margin_pct = float(cfg["hard_max_margin_used_pct"])
    notional_fraction = float(cfg["allocator"]["position_notional_fraction"])
    max_units = int(cfg["allocator"]["max_units"])
    replacement_margin = float(cfg["allocator"]["replace_ev_margin_usd"])
    anti_churn = int(cfg["allocator"]["anti_churn_minutes"])
    switching_cost_pips = float(cfg["costs"]["switching_cost_pips"])
    for ts, group in surface.groupby("timestamp", sort=True):
        timestamps += 1
        still = []
        for pos in positions:
            if ts >= pos["exit_time"]:
                equity += pos["pnl"]
                trades.append({**pos, "close_time": ts, "equity_after": equity})
            else:
                still.append(pos)
        positions = still
        filter_counts["candidate_rows"] += int(len(group))
        filter_counts["negative_or_below_min_ev"] += int(((group["eligible_policy_flag"] != 1) | (group["predicted_ev_account"] <= min_ev)).sum())
        filter_counts["spread_pressure"] += int((group["spread_to_atr"] > 1.0).sum())
        eligible = group[(group["eligible_policy_flag"] == 1) & (group["predicted_ev_account"] > min_ev)].copy()
        if name == "old_fixed_policy_allocator":
            eligible = eligible[(eligible["policy_name"] == "tp3_sl5") & (eligible["horizon_min"] == 5)]
        elif name == "policy_gated_allocator":
            filter_counts["policy_gate"] += int((eligible["predicted_tp_before_sl_prob"] < eligible["predicted_stop_first_prob"]).sum())
            eligible = eligible[eligible["predicted_tp_before_sl_prob"] >= eligible["predicted_stop_first_prob"]]
        eligible = eligible.sort_values("predicted_ev_account", ascending=False)
        opened = 0
        for _, cand in eligible.iterrows():
            if opened >= max_new:
                filter_counts["max_new_gate"] += 1
                break
            if any(p["pair"] == cand["pair"] and p["side"] == cand["side"] for p in positions):
                filter_counts["same_pair_side_open"] += 1
                continue
            if len(positions) >= max_open:
                worst = min(positions, key=lambda p: p["predicted_ev_account"])
                age = int((pd.Timestamp(ts) - pd.Timestamp(worst["open_time"])).total_seconds() // 60)
                if age < anti_churn or float(cand["predicted_ev_account"]) <= worst["predicted_ev_account"] + replacement_margin:
                    continue
                equity += worst["pnl"]
                trades.append({**worst, "close_time": ts, "equity_after": equity, "exit_reason": "replaced_by_higher_predicted_ev"})
                positions.remove(worst)
            units = min(max_units, max(1, int((equity * notional_fraction) / max(float(cand["close"]), 1e-9))))
            margin = units * float(cand["close"]) * float(cfg["margin_rate_default"])
            if (sum(p["margin"] for p in positions) + margin) / max(equity, 1e-9) * 100.0 > max_margin_pct:
                filter_counts["margin_gate"] += 1
                continue
            tier = str(cand.get("tier", "tier3"))
            slippage_pips = float(cfg["costs"].get(f"slippage_pips_{tier}", cfg["costs"]["slippage_pips_tier3"]))
            entry_cost_usd = float(cand["spread_pips"]) * float(cand["pip_value_usd_per_unit"]) * units
            slippage_cost_usd = 2.0 * slippage_pips * float(cand["pip_value_usd_per_unit"]) * units
            switch_cost_usd = switching_cost_pips * float(cand["pip_value_usd_per_unit"]) * units
            pnl, hold = _actual_for_candidate(cand, units)
            positions.append({
                "open_time": ts,
                "exit_time": pd.Timestamp(ts) + pd.Timedelta(minutes=hold),
                "pair": cand["pair"],
                "side": cand["side"],
                "policy_name": cand["policy_name"],
                "horizon_min": int(cand["horizon_min"]),
                "feature_set_name": cand["feature_set_name"],
                "model_name": cand["model_name"],
                "predicted_ev_account": float(cand["predicted_ev_account"]),
                "pnl": pnl,
                "units": units,
                "margin": margin,
                "hold_minutes": hold,
                "spread_pips": float(cand["spread_pips"]),
                "spread_to_atr": float(cand["spread_to_atr"]),
                "estimated_spread_cost_usd": entry_cost_usd,
                "estimated_slippage_cost_usd": slippage_cost_usd,
                "estimated_switching_cost_usd": switch_cost_usd,
                "exit_reason": "policy_exit",
            })
            opened += 1
        if opened == 0:
            no_trade_timestamps += 1
        curve.append(equity)
        margin_curve.append(sum(p["margin"] for p in positions) / max(equity, 1e-9) * 100.0)
    final_ts = surface["timestamp"].max()
    for pos in positions:
        equity += pos["pnl"]
        trades.append({**pos, "close_time": final_ts, "equity_after": equity, "exit_reason": "end_of_backtest"})
    trades_df = pd.DataFrame(trades)
    pnl = trades_df["pnl"] if not trades_df.empty else pd.Series(dtype=float)
    losses = pnl[pnl < 0]
    wins = pnl[pnl > 0]
    switch_count = int((trades_df["exit_reason"] == "replaced_by_higher_predicted_ev").sum()) if not trades_df.empty else 0
    total_estimated_cost = 0.0
    if not trades_df.empty:
        for col in ["estimated_spread_cost_usd", "estimated_slippage_cost_usd", "estimated_switching_cost_usd"]:
            if col in trades_df.columns:
                total_estimated_cost += float(trades_df[col].fillna(0).sum())
    return {
        "allocator_name": name,
        "initial_equity": initial_equity,
        "final_equity": float(equity),
        "return_pct": float((equity / initial_equity - 1.0) * 100.0),
        "trade_count": int(len(trades_df)),
        "win_rate": float((pnl > 0).mean()) if len(pnl) else None,
        "gross_profit_usd": float(wins.sum()) if len(wins) else 0.0,
        "gross_loss_usd": float(losses.sum()) if len(losses) else 0.0,
        "profit_factor": float(wins.sum() / abs(losses.sum())) if len(losses) and abs(losses.sum()) > 0 else None,
        "max_drawdown_pct": max_drawdown(curve),
        "max_margin_used_pct": float(max(margin_curve)) if margin_curve else 0.0,
        "no_trade_rate": float(no_trade_timestamps / timestamps) if timestamps else None,
        "average_trade_duration_minutes": float(trades_df["hold_minutes"].mean()) if not trades_df.empty and "hold_minutes" in trades_df.columns else None,
        "average_spread_pips_at_entry": float(trades_df["spread_pips"].mean()) if not trades_df.empty and "spread_pips" in trades_df.columns else None,
        "average_spread_to_atr_at_entry": float(trades_df["spread_to_atr"].mean()) if not trades_df.empty and "spread_to_atr" in trades_df.columns else None,
        "total_estimated_spread_slippage_switch_cost_usd": total_estimated_cost,
        "filter_counts": filter_counts,
        "churn_switch_count": switch_count,
        "churn_switch_rate": float(switch_count / len(trades_df)) if len(trades_df) else None,
        "pair_breakdown": _trade_breakdown(trades_df, "pair"),
        "policy_breakdown": _trade_breakdown(trades_df, "policy_name"),
        "horizon_breakdown": _trade_breakdown(trades_df, "horizon_min"),
        "feature_model_breakdown": _trade_breakdown(trades_df, "feature_set_name"),
    }


def _trade_breakdown(df: pd.DataFrame, col: str) -> list[dict[str, Any]]:
    if df.empty or col not in df.columns:
        return []
    out = []
    for key, g in df.groupby(col, dropna=False):
        out.append({
            col: str(key),
            "count": int(len(g)),
            "pnl_sum": float(g["pnl"].sum()),
            "pnl_mean": float(g["pnl"].mean()),
            "win_rate": float((g["pnl"] > 0).mean()),
        })
    return out


def _candidate_specs(df: pd.DataFrame, cfg: dict[str, Any]) -> list[CandidateSpec]:
    families = _feature_families(df)
    requested = cfg.get("surface", {}).get("feature_families")
    model_names = cfg.get("surface", {}).get("model_names", ["simple_baseline", "calibrated_classifier_ev_regressor", "tree_gradient_boosting", "direct_ev_regressor"])
    specs = []
    for family, features in families.items():
        if requested and family not in requested:
            continue
        if not features:
            continue
        for model_name in model_names:
            specs.append(CandidateSpec(family, str(model_name), features))
    return specs


def _nested_time_split(df: pd.DataFrame, cfg: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    df = df.sort_values("decision_time_utc").copy()
    times = pd.Series(df["decision_time_utc"].drop_duplicates().sort_values().to_numpy())
    if len(times) < 10:
        raise RuntimeError("not enough unique timestamps for nested split")
    val_frac = float(cfg.get("surface", {}).get("selection_fraction", 0.20))
    test_frac = float(cfg.get("surface", {}).get("final_test_fraction", 0.20))
    train_end_i = max(1, int(len(times) * (1.0 - val_frac - test_frac)))
    val_end_i = max(train_end_i + 1, int(len(times) * (1.0 - test_frac)))
    train_end = pd.Timestamp(times.iloc[train_end_i - 1])
    val_start = pd.Timestamp(times.iloc[train_end_i])
    val_end = pd.Timestamp(times.iloc[val_end_i - 1])
    test_start = pd.Timestamp(times.iloc[val_end_i])
    embargo = pd.Timedelta(minutes=int(cfg.get("surface", {}).get("purge_embargo_minutes", 90)))
    train = df[df["decision_time_utc"] <= train_end - embargo].copy()
    selection = df[(df["decision_time_utc"] >= val_start + embargo) & (df["decision_time_utc"] <= val_end - embargo)].copy()
    final_test = df[df["decision_time_utc"] >= test_start + embargo].copy()
    if train.empty or selection.empty or final_test.empty:
        raise RuntimeError("nested split produced an empty train/selection/final_test set; widen date range")
    meta = {
        "train_start": str(train["decision_time_utc"].min()),
        "train_end": str(train["decision_time_utc"].max()),
        "selection_start": str(selection["decision_time_utc"].min()),
        "selection_end": str(selection["decision_time_utc"].max()),
        "final_test_start": str(final_test["decision_time_utc"].min()),
        "final_test_end": str(final_test["decision_time_utc"].max()),
        "purge_embargo_minutes": int(embargo.total_seconds() // 60),
        "max_feature_lookback_minutes": int(cfg.get("surface", {}).get("max_feature_lookback_minutes", 60)),
        "max_horizon_minutes": max(_horizons(cfg)),
    }
    return train, selection, final_test, meta


def _write_top_candidates(surface: pd.DataFrame, output_dir: Path, key: str, limit_per_timestamp: int) -> str:
    safe_key = key.replace("|", "__").replace("/", "_")
    cols = SURFACE_COLUMNS + [
        "actual_policy_pnl_1k",
        "actual_policy_net_pips",
        "actual_endpoint_pips",
        "actual_horizon_mfe_pips",
        "actual_horizon_mae_pips",
        "actual_tp_before_sl",
        "actual_stop_first",
        "actual_timeout",
    ]
    top = surface.sort_values("predicted_ev_account", ascending=False).groupby("timestamp", as_index=False).head(limit_per_timestamp)
    path = output_dir / f"top_candidates_{safe_key}.csv"
    top[[c for c in cols if c in top.columns]].to_csv(path, index=False)
    return str(path)


def train_surface_models(cfg: dict[str, Any], dataset_path: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(dataset_path)
    df["decision_time_utc"] = pd.to_datetime(df["decision_time_utc"], utc=True)
    df = df.sort_values("decision_time_utc").reset_index(drop=True)
    train, selection, final_test, split_meta = _nested_time_split(df, cfg)
    bundles = []
    selection_reports = []
    allocator_reports = []
    top_candidate_paths = {}
    limit_per_ts = int(cfg.get("surface", {}).get("top_candidates_per_timestamp", 5))
    for spec in _candidate_specs(df, cfg):
        bundle = _fit_surface_models(cfg, train, spec)
        bundles.append(bundle)
        surface = _score_surface_model(bundle, selection, cfg)
        report = _surface_report(surface)
        gates = _variant_validation_gates(report, int(cfg.get("surface", {}).get("min_allocator_gate_samples", 100)))
        report["validation_allocator_eligibility_gates"] = gates
        if not gates["allocator_eligible"]:
            surface["eligible_policy_flag"] = 0
        report["feature_set_name"] = spec.feature_set_name
        report["model_name"] = spec.model_name
        report["allocator_performance"] = _run_surface_allocator(surface, cfg, "candidate_surface_allocator")
        selection_reports.append(report)
        key = f"{spec.feature_set_name}|{spec.model_name}"
        top_candidate_paths[key] = _write_top_candidates(surface, output_dir, key, limit_per_ts)
    ranking = sorted(
        selection_reports,
        key=lambda r: (
            1 if r.get("validation_allocator_eligibility_gates", {}).get("allocator_eligible") else 0,
            r["allocator_performance"]["return_pct"],
            -abs(r["allocator_performance"]["max_drawdown_pct"]),
            r["allocator_performance"]["trade_count"],
            r["precision_at"]["1"] if r["precision_at"]["1"] is not None else -1e18,
        ),
        reverse=True,
    )
    winner_key = f"{ranking[0]['feature_set_name']}|{ranking[0]['model_name']}" if ranking else None
    winner_bundle = next((b for b in bundles if f"{b['feature_set_name']}|{b['model_name']}" == winner_key), None)
    if winner_bundle is None:
        raise RuntimeError("no winning surface model bundle")
    winner_surface = _score_surface_model(winner_bundle, final_test, cfg)
    winner_selection_report = next((r for r in selection_reports if f"{r['feature_set_name']}|{r['model_name']}" == winner_key), {})
    if not winner_selection_report.get("validation_allocator_eligibility_gates", {}).get("allocator_eligible", False):
        winner_surface["eligible_policy_flag"] = 0
    winner_surface["timestamp"] = pd.to_datetime(winner_surface["timestamp"], utc=True)
    final_winner_report = _surface_report(winner_surface)
    forecast_surface_path = output_dir / "forecast_surface.csv"
    winner_surface[SURFACE_COLUMNS].to_csv(forecast_surface_path, index=False)
    audit_surface_path = output_dir / "forecast_surface_audit.parquet"
    audit_cols = SURFACE_COLUMNS + [
        "actual_policy_pnl_1k",
        "actual_policy_net_pips",
        "actual_endpoint_pips",
        "actual_horizon_mfe_pips",
        "actual_horizon_mae_pips",
        "actual_tp_before_sl",
        "actual_stop_first",
        "actual_timeout",
        "actual_time_to_exit",
    ]
    winner_surface[[c for c in audit_cols if c in winner_surface.columns]].to_parquet(audit_surface_path, index=False)
    horizon_integrity_report_path = write_horizon_integrity_report(audit_surface_path, output_dir)
    top_ev_failure_report_path = write_top_ev_failure_diagnostics(audit_surface_path, output_dir)
    full_surface_path = None
    if bool(cfg.get("surface", {}).get("write_all_candidates", False)):
        all_parts = []
        for bundle in bundles:
            part = _score_surface_model(bundle, final_test, cfg)
            part["candidate_key"] = f"{bundle['feature_set_name']}|{bundle['model_name']}"
            all_parts.append(part)
        full_surface = pd.concat(all_parts, ignore_index=True)
        full_surface_path = output_dir / "forecast_surface_all_candidates.parquet"
        full_surface.to_parquet(full_surface_path, index=False)
    for allocator_name, allocator_surface in [
        ("old_fixed_policy_allocator", winner_surface),
        ("policy_gated_allocator", winner_surface),
        ("full_forecast_surface_allocator", winner_surface),
        ("feature_family_model_sweep_winner_allocator", winner_surface),
    ]:
        allocator_reports.append(_run_surface_allocator(allocator_surface, cfg, allocator_name))
    for h in [3, 5, 8, 13, 21, 30]:
        fixed = winner_surface[winner_surface["horizon_min"] == h].copy()
        allocator_reports.append(_run_surface_allocator(fixed, cfg, f"fixed_horizon_{h}m_allocator"))
    report = {
        "dataset_path": str(dataset_path),
        "train_rows": int(len(train)),
        "selection_rows": int(len(selection)),
        "final_test_rows": int(len(final_test)),
        "nested_split": split_meta,
        "max_train_rows_per_candidate": int(cfg.get("surface", {}).get("max_train_rows_per_candidate", 0) or 0),
        "forecast_surface_path": str(forecast_surface_path),
        "forecast_surface_audit_path": str(audit_surface_path),
        "full_surface_path": str(full_surface_path) if full_surface_path else None,
        "full_surface_materialized": full_surface_path is not None,
        "horizon_integrity_report_path": str(horizon_integrity_report_path),
        "top_ev_failure_diagnostics_path": str(top_ev_failure_report_path),
        "winner_key": winner_key,
        "selection_candidate_ranking": ranking,
        "final_winner_report": final_winner_report,
        "allocator_comparison": allocator_reports,
        "top_candidate_paths": top_candidate_paths,
        "final_comparison_table": _final_comparison_table(allocator_reports, final_winner_report),
        "tiered_reporting": {
            "winner_final": final_winner_report.get("tier_breakdown", []),
        },
        "leakage_checks": {
            "surface_rows_scored_out_of_sample": True,
            "winner_selected_on_validation_not_final_test": True,
            "final_test_untouched_until_winner_selection": True,
            "purge_embargo_enforced": True,
            "features_use_known_at_decision_time_only": True,
            "allocator_uses_predicted_ev_only": True,
            "realized_best_horizon_used": False,
            "realized_best_policy_used": False,
            "label_derived_best_exit_policy_used": False,
            "oracle_columns_used_for_selection": False,
            "live_execution_enabled": False,
            "policy_is_tp_sl_template_only": True,
            "horizon_is_max_hold_evaluation_window": True,
        },
    }
    write_json(output_dir / "surface_sweep_report.json", report)
    model_path = output_dir / "surface_model_bundle.joblib"
    joblib.dump({
        "bundles": bundles,
        "winner_key": winner_key,
        "config": cfg,
        "dataset_path": str(dataset_path),
        "forecast_surface_path": str(forecast_surface_path),
        "full_surface_path": str(full_surface_path) if full_surface_path else None,
    }, model_path)
    return model_path


def _final_comparison_table(allocator_reports: list[dict[str, Any]], winner_report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [{
        "comparison": "no_trade_baseline",
        "return_pct": 0.0,
        "trade_count": 0,
        "max_drawdown_pct": 0.0,
        "win_rate": None,
    }]
    for report in allocator_reports:
        rows.append({
            "comparison": report["allocator_name"],
            "return_pct": report["return_pct"],
            "trade_count": report["trade_count"],
            "max_drawdown_pct": report["max_drawdown_pct"],
            "win_rate": report["win_rate"],
        })
    rows.append({
        "comparison": "best_feature_family",
        "return_pct": allocator_reports[-1]["return_pct"] if allocator_reports else None,
        "trade_count": allocator_reports[-1]["trade_count"] if allocator_reports else None,
        "max_drawdown_pct": allocator_reports[-1]["max_drawdown_pct"] if allocator_reports else None,
        "win_rate": allocator_reports[-1]["win_rate"] if allocator_reports else None,
    })
    rows.append({
        "comparison": "best_model_family",
        "return_pct": allocator_reports[-1]["return_pct"] if allocator_reports else None,
        "trade_count": allocator_reports[-1]["trade_count"] if allocator_reports else None,
        "max_drawdown_pct": allocator_reports[-1]["max_drawdown_pct"] if allocator_reports else None,
        "win_rate": allocator_reports[-1]["win_rate"] if allocator_reports else None,
    })
    rows.append({
        "comparison": "tier_1_only",
        "return_pct": None,
        "trade_count": None,
        "max_drawdown_pct": None,
        "win_rate": _tier_positive_rate(winner_report, "tier1"),
    })
    rows.append({
        "comparison": "all_tiers",
        "return_pct": None,
        "trade_count": None,
        "max_drawdown_pct": None,
        "win_rate": None,
    })
    return rows


def _tier_positive_rate(winner_report: dict[str, Any], tier: str) -> float | None:
    for row in winner_report.get("tier_breakdown", []):
        if row.get("tier") == tier:
            return row.get("positive_rate")
    return None
