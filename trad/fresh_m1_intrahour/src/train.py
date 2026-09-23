from __future__ import annotations

from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.metrics import accuracy_score, precision_score, roc_auc_score

from .common import write_json


EXCLUDE_PREFIXES = (
    "long_endpoint_",
    "short_endpoint_",
    "long_mfe_",
    "short_mfe_",
    "long_mae_",
    "short_mae_",
    "endpoint_",
    "mfe_",
    "mae_",
    "net_after_cost_",
    "net_account_pnl_",
    "policy_",
)
EXCLUDE_COLUMNS = {
    "decision_time_utc",
    "datetime",
    "instrument",
    "side",
    "side_sign",
    "target_ev_usd_1k_units",
    "target_positive_ev",
    "target_policy",
    "best_exit_policy",
    "best_horizon",
    "best_policy_net_account_pnl_1k_units",
    "best_horizon_net_account_pnl_1k_units",
    "raw_best_horizon_any_side",
    "base_currency",
    "quote_currency",
    "tier",
    "entry_delay_bars",
    "same_bar_ambiguity_mode",
    "execution_cost_mode",
}


def policy_names(cfg: dict[str, Any]) -> list[str]:
    return [str(t["name"]) for t in cfg["policy_templates"]]


def feature_columns(df: pd.DataFrame) -> list[str]:
    cols = []
    for col in df.columns:
        if col in EXCLUDE_COLUMNS:
            continue
        if any(col.startswith(prefix) for prefix in EXCLUDE_PREFIXES):
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            cols.append(col)
    return cols


def _clean_x(df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    return df[features].replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _safe_auc(y_true: pd.Series, y_score: pd.Series) -> float | None:
    if y_true.nunique(dropna=True) < 2:
        return None
    try:
        return float(roc_auc_score(y_true.astype(int), y_score))
    except Exception:
        return None


def _safe_corr(a: pd.Series, b: pd.Series) -> float | None:
    tmp = pd.concat([a, b], axis=1).dropna()
    if len(tmp) < 2 or tmp.iloc[:, 0].nunique() < 2 or tmp.iloc[:, 1].nunique() < 2:
        return None
    return float(tmp.corr().iloc[0, 1])


def _session_name(row: pd.Series) -> str:
    if int(row.get("is_rollover_risk", 0)) == 1:
        return "rollover"
    if int(row.get("is_london_ny_overlap", 0)) == 1:
        return "london_ny_overlap"
    if int(row.get("is_london", 0)) == 1:
        return "london"
    if int(row.get("is_new_york", 0)) == 1:
        return "new_york"
    if int(row.get("is_asia", 0)) == 1:
        return "asia"
    return "other"


def _group_stats(df: pd.DataFrame, group_col: str, target_col: str) -> list[dict[str, Any]]:
    if df.empty or group_col not in df.columns:
        return []
    out = []
    for key, group in df.groupby(group_col, dropna=False):
        vals = group[target_col].astype(float)
        out.append({
            group_col: str(key),
            "rows": int(len(group)),
            "actual_pnl_usd_1k_sum": float(vals.sum()),
            "actual_pnl_usd_1k_mean": float(vals.mean()),
            "positive_rate": float((vals > 0).mean()),
            "avg_pred_ev_usd_1k": float(group["pred_ev_usd_1k_units"].mean()),
        })
    return out


def _bucket_table(df: pd.DataFrame, score_col: str, target_col: str, bins: int = 10) -> list[dict[str, Any]]:
    tmp = df[[score_col, target_col]].dropna().copy()
    if tmp.empty:
        return []
    tmp["bucket"] = pd.qcut(tmp[score_col].rank(method="first"), bins, labels=False, duplicates="drop")
    out = []
    for bucket, group in tmp.groupby("bucket"):
        vals = group[target_col].astype(float)
        out.append({
            "bucket": int(bucket),
            "rows": int(len(group)),
            "pred_ev_min": float(group[score_col].min()),
            "pred_ev_max": float(group[score_col].max()),
            "pred_ev_mean": float(group[score_col].mean()),
            "actual_pnl_usd_1k_sum": float(vals.sum()),
            "actual_pnl_usd_1k_mean": float(vals.mean()),
            "positive_rate": float((vals > 0).mean()),
        })
    return out


def _threshold_counts(df: pd.DataFrame, pred_col: str, target_col: str) -> list[dict[str, Any]]:
    thresholds = [-0.25, -0.10, -0.05, 0.0, 0.01, 0.03, 0.05, 0.10, 0.20, 0.50, 1.00]
    out = []
    for threshold in thresholds:
        take = df[df[pred_col] >= threshold]
        vals = take[target_col].astype(float)
        out.append({
            "threshold_pred_ev_usd_1k": float(threshold),
            "candidate_count": int(len(take)),
            "actual_pnl_usd_1k_sum": float(vals.sum()) if len(vals) else 0.0,
            "actual_pnl_usd_1k_mean": float(vals.mean()) if len(vals) else None,
            "positive_rate": float((vals > 0).mean()) if len(vals) else None,
        })
    return out


def _precision_at_k_by_minute(df: pd.DataFrame, score_col: str, target_col: str, k: int) -> dict[str, Any]:
    minute_scores = []
    for ts, group in df.groupby("decision_time_utc", sort=True):
        top = group.sort_values(score_col, ascending=False).head(k)
        if top.empty:
            continue
        minute_scores.append({
            "decision_time_utc": ts,
            "precision": float((top[target_col] > 0).mean()),
            "actual_pnl_usd_1k_sum": float(top[target_col].sum()),
        })
    if not minute_scores:
        return {"mean_precision": None, "minutes": 0, "by_minute_path": None}
    vals = [x["precision"] for x in minute_scores]
    return {
        "mean_precision": float(np.mean(vals)),
        "minutes": int(len(minute_scores)),
        "positive_minutes": int(sum(v > 0 for v in vals)),
    }


def _spread_atr_breakdown(df: pd.DataFrame, target_col: str) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for col in ["spread_pips", "atr_15m_pips", "spread_to_atr_15m"]:
        if col not in df.columns:
            out[col] = []
            continue
        tmp = df[[col, target_col, "pred_ev_usd_1k_units"]].dropna().copy()
        if tmp.empty:
            out[col] = []
            continue
        tmp[f"{col}_bucket"] = pd.qcut(tmp[col].rank(method="first"), 5, labels=False, duplicates="drop")
        out[col] = _group_stats(tmp, f"{col}_bucket", target_col)
    return out


def _opposite_shadow(df: pd.DataFrame, policy: str, target_col: str) -> dict[str, Any]:
    opposite = df[["decision_time_utc", "instrument", "side", target_col]].copy()
    opposite["side"] = np.where(opposite["side"] == "long", "short", "long")
    opposite = opposite.rename(columns={target_col: "opposite_actual_pnl_usd_1k"})
    merged = df[["decision_time_utc", "instrument", "side", target_col, "pred_ev_usd_1k_units"]].merge(
        opposite,
        on=["decision_time_utc", "instrument", "side"],
        how="left",
    )
    high = merged[merged["pred_ev_usd_1k_units"] > 0].copy()
    if high.empty:
        high = merged.sort_values("pred_ev_usd_1k_units", ascending=False).head(max(1, min(1000, len(merged))))
    selected = high[target_col].astype(float)
    opp = high["opposite_actual_pnl_usd_1k"].astype(float)
    return {
        "policy": policy,
        "rows": int(len(high)),
        "selected_positive_rate": float((selected > 0).mean()) if len(high) else None,
        "opposite_positive_rate": float((opp > 0).mean()) if len(high) else None,
        "selected_actual_pnl_usd_1k_sum": float(selected.sum()) if len(high) else 0.0,
        "opposite_actual_pnl_usd_1k_sum": float(opp.sum()) if len(high) else 0.0,
        "signal_inverted_rate": float(((selected <= 0) & (opp > 0)).mean()) if len(high) else None,
        "both_positive_rate": float(((selected > 0) & (opp > 0)).mean()) if len(high) else None,
        "neither_positive_rate": float(((selected <= 0) & (opp <= 0)).mean()) if len(high) else None,
    }


def _fit_policy_models(cfg: dict[str, Any], train: pd.DataFrame, features: list[str], policy: str) -> dict[str, Any]:
    target_col = f"policy_{policy}_net_account_pnl_1k_units"
    y_ev = train[target_col].astype(float)
    y_pos = (y_ev > float(cfg["model"]["positive_ev_usd_threshold"])).astype(int)
    X_train = _clean_x(train, features)
    clf = HistGradientBoostingClassifier(
        learning_rate=0.05,
        max_iter=180,
        max_leaf_nodes=31,
        min_samples_leaf=40,
        l2_regularization=0.05,
        random_state=int(cfg["model"]["random_state"]),
    )
    reg = HistGradientBoostingRegressor(
        learning_rate=0.05,
        max_iter=220,
        max_leaf_nodes=31,
        min_samples_leaf=40,
        l2_regularization=0.05,
        random_state=int(cfg["model"]["random_state"]),
    )
    clf.fit(X_train, y_pos)
    reg.fit(X_train, y_ev)
    return {"classifier": clf, "regressor": reg}


def _score_policy(bundle: dict[str, Any], df: pd.DataFrame, features: list[str], policy: str) -> pd.DataFrame:
    scored = df.copy()
    X = _clean_x(scored, features)
    scored["pred_positive_probability"] = bundle["classifier"].predict_proba(X)[:, 1]
    scored["pred_ev_usd_1k_units"] = bundle["regressor"].predict(X)
    scored["rank_score"] = scored["pred_ev_usd_1k_units"] * scored["pred_positive_probability"]
    scored["predicted_policy"] = policy
    return scored


def _policy_report(policy: str, scored: pd.DataFrame, output_dir: Path) -> dict[str, Any]:
    target_col = f"policy_{policy}_net_account_pnl_1k_units"
    y_pos = (scored[target_col] > 0).astype(int)
    scored = scored.copy()
    scored["session"] = scored.apply(_session_name, axis=1)
    report = {
        "policy": policy,
        "rows": int(len(scored)),
        "auc": _safe_auc(y_pos, scored["pred_positive_probability"]),
        "ev_prediction_correlation": _safe_corr(scored["pred_ev_usd_1k_units"], scored[target_col]),
        "classification": {
            "accuracy_at_0p5": float(accuracy_score(y_pos, scored["pred_positive_probability"] >= 0.5)),
            "precision_at_0p5": float(precision_score(y_pos, scored["pred_positive_probability"] >= 0.5, zero_division=0)),
            "positive_rate": float(y_pos.mean()),
        },
        "regression": {
            "mean_actual_ev_usd_1k": float(scored[target_col].mean()),
            "mean_pred_ev_usd_1k": float(scored["pred_ev_usd_1k_units"].mean()),
        },
        "predicted_ev_deciles": _bucket_table(scored, "pred_ev_usd_1k_units", target_col, bins=10),
        "actual_account_pnl_by_predicted_ev_bucket": _bucket_table(scored, "pred_ev_usd_1k_units", target_col, bins=10),
        "trade_count_by_threshold": _threshold_counts(scored, "pred_ev_usd_1k_units", target_col),
        "precision_at_by_minute": {
            "1": _precision_at_k_by_minute(scored, "rank_score", target_col, 1),
            "3": _precision_at_k_by_minute(scored, "rank_score", target_col, 3),
            "5": _precision_at_k_by_minute(scored, "rank_score", target_col, 5),
        },
        "pair_breakdown": _group_stats(scored, "instrument", target_col),
        "session_breakdown": _group_stats(scored, "session", target_col),
        "spread_atr_breakdown": _spread_atr_breakdown(scored, target_col),
        "long_short_breakdown": _group_stats(scored, "side", target_col),
        "predicted_side_vs_opposite_side_shadow": _opposite_shadow(scored, policy, target_col),
    }
    write_json(output_dir / f"policy_report_{policy}.json", report)
    return report


def _monthly_walk_forward(
    cfg: dict[str, Any],
    df: pd.DataFrame,
    features: list[str],
    policies: list[str],
    output_dir: Path,
) -> dict[str, list[dict[str, Any]]]:
    min_train = int(cfg["model"].get("monthly_walk_forward_min_train_rows", 5000))
    min_test = int(cfg["model"].get("monthly_walk_forward_min_test_rows", 500))
    df = df.sort_values("decision_time_utc").copy()
    df["_walk_forward_month"] = df["decision_time_utc"].dt.strftime("%Y-%m")
    months = sorted(df["_walk_forward_month"].unique())
    results: dict[str, list[dict[str, Any]]] = {policy: [] for policy in policies}
    for month in months:
        month_start = pd.Timestamp(f"{month}-01", tz="UTC")
        train = df[df["decision_time_utc"] < month_start]
        test = df[df["_walk_forward_month"] == month]
        if len(train) < min_train or len(test) < min_test:
            continue
        for policy in policies:
            target_col = f"policy_{policy}_net_account_pnl_1k_units"
            models = _fit_policy_models(cfg, train, features, policy)
            scored = _score_policy(models, test, features, policy)
            high = scored[scored["pred_ev_usd_1k_units"] > 0]
            y_pos = (scored[target_col] > 0).astype(int)
            results[policy].append({
                "month": month,
                "train_rows": int(len(train)),
                "test_rows": int(len(test)),
                "auc": _safe_auc(y_pos, scored["pred_positive_probability"]),
                "ev_prediction_correlation": _safe_corr(scored["pred_ev_usd_1k_units"], scored[target_col]),
                "positive_predicted_ev_candidates": int(len(high)),
                "positive_predicted_ev_actual_pnl_usd_1k_sum": float(high[target_col].sum()) if len(high) else 0.0,
                "positive_predicted_ev_actual_pnl_usd_1k_mean": float(high[target_col].mean()) if len(high) else None,
                "positive_predicted_ev_win_rate": float((high[target_col] > 0).mean()) if len(high) else None,
            })
    write_json(output_dir / "monthly_walk_forward.json", results)
    return results


def train_models(cfg: dict[str, Any], dataset_path: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(dataset_path)
    df["decision_time_utc"] = pd.to_datetime(df["decision_time_utc"], utc=True)
    df = df.sort_values("decision_time_utc").reset_index(drop=True)
    features = feature_columns(df)
    policies = policy_names(cfg)
    if len(df) < int(cfg["model"]["min_train_rows"]):
        raise RuntimeError(f"not enough rows to train: {len(df)}")
    split_idx = int(len(df) * (1.0 - float(cfg["model"]["test_fraction"])))
    train = df.iloc[:split_idx].copy()
    test = df.iloc[split_idx:].copy()

    policy_models: dict[str, Any] = {}
    policy_reports: dict[str, Any] = {}
    scored_frames = []
    for policy in policies:
        models = _fit_policy_models(cfg, train, features, policy)
        policy_models[policy] = models
        scored = _score_policy(models, test, features, policy)
        policy_reports[policy] = _policy_report(policy, scored, output_dir)
        scored_frames.append(scored)

    scored_long = pd.concat(scored_frames, ignore_index=True)
    pred_path = output_dir / "holdout_policy_predictions.parquet"
    keep_base = [
        "decision_time_utc",
        "instrument",
        "side",
        "predicted_policy",
        "pred_positive_probability",
        "pred_ev_usd_1k_units",
        "rank_score",
        "close",
        "pip_value_usd_per_unit",
        "spread_pips",
        "atr_15m_pips",
        "spread_to_atr_15m",
        "is_asia",
        "is_london",
        "is_new_york",
        "is_london_ny_overlap",
        "is_rollover_risk",
    ]
    policy_cols = []
    for policy in policies:
        policy_cols.extend([
            f"policy_{policy}_net_account_pnl_1k_units",
            f"policy_{policy}_time_to_exit",
            f"policy_{policy}_outcome",
        ])
    scored_long[[c for c in keep_base + policy_cols if c in scored_long.columns]].to_parquet(pred_path, index=False)

    walk_forward = _monthly_walk_forward(cfg, df, features, policies, output_dir)
    bundle = {
        "policy_models": policy_models,
        "policies": policies,
        "features": features,
        "config": cfg,
        "dataset_path": str(dataset_path),
        "prediction_path": str(pred_path),
    }
    model_path = output_dir / "policy_model_bundle.joblib"
    joblib.dump(bundle, model_path)
    report = {
        "dataset_path": str(dataset_path),
        "model_path": str(model_path),
        "holdout_predictions_path": str(pred_path),
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "features": features,
        "policies": policies,
        "policy_reports": policy_reports,
        "monthly_walk_forward": walk_forward,
        "time_split": {
            "train_start": str(train["decision_time_utc"].min()),
            "train_end": str(train["decision_time_utc"].max()),
            "test_start": str(test["decision_time_utc"].min()),
            "test_end": str(test["decision_time_utc"].max()),
        },
        "leakage_checks": {
            "feature_columns_exclude_future_label_prefixes": True,
            "time_ordered_holdout": True,
            "old_reversal_target_used": False,
            "old_execution_side_policy_used": False,
            "label_derived_best_policy_feature_excluded": "raw_best_horizon_any_side" not in features,
            "label_derived_best_exit_policy_used_for_selection": False,
            "each_exit_policy_trained_independently": True,
        },
    }
    write_json(output_dir / "model_report.json", report)
    return model_path


def predict_dataset(model_path: Path, dataset_path: Path) -> pd.DataFrame:
    bundle = joblib.load(model_path)
    df = pd.read_parquet(dataset_path)
    df["decision_time_utc"] = pd.to_datetime(df["decision_time_utc"], utc=True)
    features = bundle["features"]
    frames = []
    for policy in bundle["policies"]:
        scored = _score_policy(bundle["policy_models"][policy], df, features, policy)
        frames.append(scored)
    return pd.concat(frames, ignore_index=True)
