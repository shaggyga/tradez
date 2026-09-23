from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .arima_baseline import run_arima_baseline_analysis
from .common import write_json
from .economics import execution_settings_from_config
from .features import assert_decision_time_feature_columns, build_feature_family_report
from .movement_first import add_movement_targets, fit_predict_movement_first, neutral_movement_features
from .replay_metrics import (
    cost_stress_for_trades,
    evaluate_score_policies,
    gate_attribution,
    quick_evaluate_validation_policies,
    quick_trade_metrics,
    rank_ic,
    select_trades_for_policy,
    topk_result,
    write_report_pair,
)
from .research_suite import (
    SplitData,
    _calibration,
    _family_map,
    _fit_predict_model,
    _sample_train,
    _score_rule_branches,
    _select_threshold,
    _split,
    _trade_metrics,
)
from .signal_regression_audit import _add_gross_labels, _reuse_or_build_dataset


ML_FEATURE_FAMILIES = [
    "I_full",
    "M1+spread+currency+ranks",
    "M1+volatility+motion",
    "full_minus_pair_identity",
]
ML_LABEL_FAMILIES = ["TP_before_SL_proxy", "expected_MFE", "signed_return", "expected_account_EV"]
ML_HORIZONS = [13, 21, 30]
ML_MODEL_FAMILIES = [
    ("Extra Trees", "extra_trees"),
    ("HistGradientBoosting", "hist_gradient_boosting"),
    ("calibrated classifier + EV regressor", "calibrated_classifier_ev_regressor"),
    ("direct EV regressor", "direct_ev_regressor"),
]
MOVEMENT_MARGINS_PIPS = [1.0, 3.0]
WALK_FORWARD_FOLDS = 3
SELECTION_POLICIES = [
    "raw_top1",
    "positive_predicted_ev",
    "calibrated_lower_bound",
    "no_edge_gated",
    "current_allocator_gated",
]


def _slippage_round_trip(frame: pd.DataFrame, cfg: dict[str, Any]) -> pd.Series:
    tiers = frame.get("tier", pd.Series("tier1", index=frame.index)).fillna("tier1").astype(str)
    cache: dict[str, float] = {}
    values = []
    for tier in tiers:
        if tier not in cache:
            cache[tier] = float(execution_settings_from_config(cfg, tier).slippage_pips_round_trip)
        values.append(cache[tier])
    return pd.Series(values, index=frame.index, dtype=float)


def _add_validation_targets(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    out = df.copy()
    pip_value = pd.to_numeric(out.get("pip_value_usd_per_unit", 0.0), errors="coerce").fillna(0.0) * 1000.0
    slip = _slippage_round_trip(out, cfg)
    for h in ML_HORIZONS:
        actual = pd.to_numeric(out.get(f"actual_ev_{h}m"), errors="coerce")
        actual_pips = pd.to_numeric(out.get(f"actual_pips_{h}m"), errors="coerce")
        mfe = pd.to_numeric(out.get(f"mfe_pips_{h}m"), errors="coerce")
        mae = pd.to_numeric(out.get(f"mae_pips_{h}m"), errors="coerce")
        tp = 3.0
        sl = 5.0
        proxy_pips = np.select(
            [
                (mfe >= tp) & (mae < sl),
                mae >= sl,
            ],
            [
                tp - slip,
                -sl - slip,
            ],
            default=actual_pips,
        )
        out[f"target_signed_return_{h}m"] = actual
        out[f"target_expected_account_EV_{h}m"] = actual
        out[f"target_expected_MFE_{h}m"] = (mfe - slip).clip(lower=-sl) * pip_value
        out[f"target_TP_before_SL_proxy_{h}m"] = pd.Series(proxy_pips, index=out.index, dtype=float) * pip_value
    return add_movement_targets(out, ML_HORIZONS, MOVEMENT_MARGINS_PIPS)


def _feature_sets(train: pd.DataFrame) -> dict[str, list[str]]:
    fam = _family_map(train)
    full = fam.get("I_full", [])
    spread_currency_ranks = fam.get("M1_plus_spread_plus_currency_strength_plus_ranks", [])
    vol_motion = sorted(set(fam.get("M1_plus_spread_plus_volatility", []) + fam.get("H_movement_quality_choppiness", [])))
    full_minus_pair_identity = [
        c for c in full
        if not any(token in c.lower() for token in ["pip_value", "quote_to_usd"])
    ]
    feature_sets = {
        "I_full": full,
        "M1+spread+currency+ranks": spread_currency_ranks,
        "M1+volatility+motion": vol_motion or fam.get("M1_plus_spread_plus_volatility", []),
        "full_minus_pair_identity": full_minus_pair_identity or full,
    }
    feature_sets["movement_neutral"] = neutral_movement_features(feature_sets["M1+volatility+motion"])
    for columns in feature_sets.values():
        assert_decision_time_feature_columns(columns)
    return feature_sets


def _target_col(label_family: str, horizon: int) -> str:
    return f"target_{label_family}_{horizon}m"


def _candidate_name(model_label: str, feature_family: str, label_family: str, horizon: int) -> str:
    return f"{model_label}|{feature_family}|{label_family}|{horizon}m"


def _candidate_specs() -> list[dict[str, Any]]:
    direct_specs = [
        ("Extra Trees", "extra_trees", "I_full", "TP_before_SL_proxy", 13),
        ("Extra Trees", "extra_trees", "I_full", "TP_before_SL_proxy", 21),
        ("Extra Trees", "extra_trees", "I_full", "TP_before_SL_proxy", 30),
        ("Extra Trees", "extra_trees", "I_full", "expected_MFE", 21),
        ("HistGradientBoosting", "hist_gradient_boosting", "I_full", "signed_return", 13),
        ("HistGradientBoosting", "hist_gradient_boosting", "I_full", "signed_return", 21),
        ("HistGradientBoosting", "hist_gradient_boosting", "I_full", "signed_return", 30),
        ("calibrated classifier + EV regressor", "calibrated_classifier_ev_regressor", "I_full", "signed_return", 21),
        ("direct EV regressor", "direct_ev_regressor", "full_minus_pair_identity", "signed_return", 21),
        ("HistGradientBoosting", "hist_gradient_boosting", "M1+spread+currency+ranks", "signed_return", 13),
        ("HistGradientBoosting", "hist_gradient_boosting", "M1+spread+currency+ranks", "signed_return", 21),
        ("HistGradientBoosting", "hist_gradient_boosting", "M1+volatility+motion", "expected_MFE", 13),
        ("HistGradientBoosting", "hist_gradient_boosting", "M1+volatility+motion", "expected_MFE", 21),
    ]
    specs = [
        {
            "kind": "direct",
            "candidate": _candidate_name(model_label, feature_family, label_family, horizon),
            "model_family": model_label,
            "model_key": model_key,
            "feature_family": feature_family,
            "label_family": label_family,
            "horizon": horizon,
            "target": _target_col(label_family, horizon),
            "actual_col": f"actual_ev_{horizon}m",
        }
        for model_label, model_key, feature_family, label_family, horizon in direct_specs
    ]
    for horizon in [13, 21]:
        for margin_pips in MOVEMENT_MARGINS_PIPS:
            for direction_family in ["I_full", "M1+spread+currency+ranks"]:
                for direction_model in ["conditional_ev_regressor", "conditional_positive_classifier"]:
                    candidate = (
                        f"movement_first|HGB_move|{direction_model}|{direction_family}|"
                        f"cost_plus_{margin_pips:g}pips|{horizon}m"
                    )
                    specs.append({
                        "kind": "movement_first",
                        "candidate": candidate,
                        "model_family": "movement-first conditional direction",
                        "model_key": direction_model,
                        "feature_family": direction_family,
                        "movement_feature_family": "movement_neutral",
                        "label_family": "movement_then_account_EV",
                        "horizon": horizon,
                        "movement_margin_pips": margin_pips,
                        "target": f"actual_ev_{horizon}m",
                        "actual_col": f"actual_ev_{horizon}m",
                    })
    return specs


def _development_walk_forward_splits(
    split: SplitData,
    cfg: dict[str, Any],
    folds: int = WALK_FORWARD_FOLDS,
) -> tuple[pd.DataFrame, list[SplitData], list[dict[str, Any]]]:
    development = pd.concat([split.train, split.validation], ignore_index=True)
    development["decision_time_utc"] = pd.to_datetime(development["decision_time_utc"], utc=True)
    development = development.sort_values(["decision_time_utc", "pair", "side"]).reset_index(drop=True)
    times = pd.Series(development["decision_time_utc"].drop_duplicates().sort_values().to_numpy())
    if len(times) < 100:
        raise RuntimeError("not enough development timestamps for walk-forward folds")
    initial = max(20, int(len(times) * 0.55))
    boundaries = np.linspace(initial, len(times), folds + 1, dtype=int)
    embargo_minutes = int(cfg.get("surface", {}).get("purge_embargo_minutes", 270))
    embargo = pd.Timedelta(minutes=embargo_minutes)
    result: list[SplitData] = []
    meta: list[dict[str, Any]] = []
    for fold_index in range(folds):
        train_stop = int(boundaries[fold_index])
        validation_stop = int(boundaries[fold_index + 1])
        if train_stop <= 0 or validation_stop <= train_stop:
            continue
        train_boundary = pd.Timestamp(times.iloc[train_stop - 1])
        validation_boundary = pd.Timestamp(times.iloc[train_stop])
        validation_end = pd.Timestamp(times.iloc[validation_stop - 1])
        fold_train = development[development["decision_time_utc"] <= train_boundary - embargo].copy()
        fold_validation = development[
            (development["decision_time_utc"] >= validation_boundary + embargo)
            & (development["decision_time_utc"] <= validation_end)
        ].copy()
        if fold_train.empty or fold_validation.empty:
            continue
        fold_meta = {
            "fold": int(fold_index + 1),
            "train_start": str(fold_train["decision_time_utc"].min()),
            "train_end": str(fold_train["decision_time_utc"].max()),
            "validation_start": str(fold_validation["decision_time_utc"].min()),
            "validation_end": str(fold_validation["decision_time_utc"].max()),
            "train_rows": int(len(fold_train)),
            "validation_rows": int(len(fold_validation)),
            "purge_embargo_minutes": embargo_minutes,
        }
        result.append(SplitData(fold_train, fold_validation, fold_validation.iloc[0:0].copy(), fold_meta))
        meta.append(fold_meta)
    if len(result) < 2:
        raise RuntimeError("walk-forward construction produced fewer than two usable folds")
    return development, result, meta


def _fit_candidate_scores(
    spec: dict[str, Any],
    train: pd.DataFrame,
    validation: pd.DataFrame,
    final: pd.DataFrame,
    feature_sets: dict[str, list[str]],
    cfg: dict[str, Any],
    seed: int,
) -> tuple[pd.Series, pd.Series, dict[str, Any], str | None]:
    train_cap = min(int(cfg.get("surface", {}).get("max_train_rows_per_candidate", 30000)), 16000)
    if spec["kind"] == "direct":
        features = feature_sets.get(spec["feature_family"], [])
        if not features:
            return pd.Series(dtype=float), pd.Series(dtype=float), {}, "no usable direct feature columns"
        assert_decision_time_feature_columns(features)
        finite_train = train.replace([np.inf, -np.inf], np.nan).dropna(subset=[spec["target"]])
        if len(finite_train) < 1000:
            return pd.Series(dtype=float), pd.Series(dtype=float), {}, "too few finite direct training rows"
        train_sample = _sample_train(finite_train, train_cap, seed)
        validation_score, final_score, reason = _fit_predict_model(
            spec["model_key"],
            train_sample,
            validation,
            final,
            features,
            spec["target"],
            seed,
        )
        return validation_score, final_score, {
            "train_rows": int(len(train_sample)),
            "feature_count": int(len(features)),
        }, reason

    movement_features = feature_sets.get(spec["movement_feature_family"], [])
    direction_features = feature_sets.get(spec["feature_family"], [])
    if not movement_features or not direction_features:
        return pd.Series(dtype=float), pd.Series(dtype=float), {}, "no usable movement-first feature columns"
    movement_train = _sample_train(train, min(train_cap * 2, 30000), seed)
    return fit_predict_movement_first(
        movement_train,
        validation,
        final,
        movement_features,
        direction_features,
        int(spec["horizon"]),
        float(spec["movement_margin_pips"]),
        spec["model_key"],
        seed,
    )


def _policy_stability(
    oof: pd.DataFrame,
    score_col: str,
    actual_col: str,
    cfg: dict[str, Any],
) -> dict[str, Any]:
    threshold = _select_threshold(oof, score_col, actual_col)
    calibration = _calibration(oof, score_col, actual_col)
    rmse = float(calibration.get("rmse") or 0.0)
    all_policies = [
        "raw_top1",
        "raw_top3",
        "raw_top5",
        "fixed_trade_quota_top1",
        "positive_predicted_ev",
        "calibrated_lower_bound",
        "no_edge_gated",
        "current_allocator_gated",
    ]
    policy_results: dict[str, Any] = {}
    for policy in all_policies:
        selected, _ = select_trades_for_policy(oof, score_col, actual_col, policy, threshold, rmse, cfg)
        aggregate = quick_trade_metrics(selected, actual_col)
        fold_results = []
        for fold_id, fold_frame in oof.groupby("_walk_forward_fold", sort=True):
            fold_trades, _ = select_trades_for_policy(
                fold_frame,
                score_col,
                actual_col,
                policy,
                threshold,
                rmse,
                cfg,
            )
            metrics = quick_trade_metrics(fold_trades, actual_col)
            metrics["fold"] = int(fold_id)
            fold_results.append(metrics)
        fold_pnl = np.asarray([float(row.get("pnl_account", 0.0)) for row in fold_results], dtype=float)
        median_pnl = float(np.median(fold_pnl)) if len(fold_pnl) else 0.0
        std_pnl = float(np.std(fold_pnl)) if len(fold_pnl) else 0.0
        positive_folds = int((fold_pnl > 0.0).sum())
        stability_score = median_pnl + (0.25 * float(aggregate["pnl_account"])) - (0.50 * std_pnl)
        policy_results[policy] = {
            "aggregate": aggregate,
            "folds": fold_results,
            "positive_fold_count": positive_folds,
            "median_fold_pnl": median_pnl,
            "fold_pnl_std": std_pnl,
            "stability_score": float(stability_score),
        }
    selected_policy = max(
        SELECTION_POLICIES,
        key=lambda policy: (
            float(policy_results[policy]["stability_score"]),
            int(policy_results[policy]["positive_fold_count"]),
            float(policy_results[policy]["aggregate"]["pnl_account"]),
        ),
    )
    selected = policy_results[selected_policy]
    active_policy_names = [
        policy for policy in SELECTION_POLICIES
        if int(policy_results[policy]["aggregate"]["trade_count"]) >= 30
    ]
    best_active_policy = max(
        active_policy_names,
        key=lambda policy: (
            float(policy_results[policy]["stability_score"]),
            int(policy_results[policy]["positive_fold_count"]),
            float(policy_results[policy]["aggregate"]["pnl_account"]),
        ),
        default=None,
    )
    best_active = policy_results[best_active_policy] if best_active_policy else None
    viable = bool(
        selected["positive_fold_count"] >= math.ceil(oof["_walk_forward_fold"].nunique() / 2)
        and selected["median_fold_pnl"] > 0.0
        and float(selected["aggregate"]["pnl_account"]) > 0.0
        and int(selected["aggregate"]["trade_count"]) >= 30
    )
    return {
        "threshold": threshold,
        "validation_calibration": calibration,
        "validation_rmse": rmse,
        "selected_policy": selected_policy,
        "selected": selected,
        "best_active_policy": best_active_policy,
        "best_active": best_active,
        "policy_results": policy_results,
        "viable": viable,
    }


def _fit_walk_forward_grid(split: SplitData, cfg: dict[str, Any]) -> dict[str, Any]:
    seed = int(cfg.get("model", {}).get("random_state", 42))
    development, folds, fold_meta = _development_walk_forward_splits(split, cfg)
    feature_sets = _feature_sets(development)
    reports: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    oof_by_candidate: dict[str, pd.DataFrame] = {}
    specs_by_candidate = {spec["candidate"]: spec for spec in _candidate_specs()}

    for spec in specs_by_candidate.values():
        fold_predictions = []
        fit_metadata = []
        reason: str | None = None
        for fold_index, fold in enumerate(folds):
            validation_score, _, metadata, reason = _fit_candidate_scores(
                spec,
                fold.train,
                fold.validation,
                fold.validation,
                feature_sets,
                cfg,
                seed + fold_index,
            )
            if reason:
                break
            scored = fold.validation.copy()
            scored["predicted_ev"] = validation_score
            scored["_walk_forward_fold"] = int(fold_index + 1)
            fold_predictions.append(scored)
            fit_metadata.append({"fold": int(fold_index + 1), **metadata})
        if reason or len(fold_predictions) != len(folds):
            skipped.append({"candidate": spec["candidate"], "reason": reason or "incomplete fold predictions"})
            continue
        oof = pd.concat(fold_predictions, ignore_index=True)
        if oof["predicted_ev"].replace([np.inf, -np.inf], np.nan).notna().sum() < 100:
            skipped.append({"candidate": spec["candidate"], "reason": "too few finite walk-forward predictions"})
            continue
        stability = _policy_stability(oof, "predicted_ev", spec["actual_col"], cfg)
        selected = stability["selected"]
        report = {
            **spec,
            "policy": stability["selected_policy"],
            "threshold": stability["threshold"],
            "validation": selected["aggregate"],
            "walk_forward": {
                "viable": stability["viable"],
                "positive_fold_count": selected["positive_fold_count"],
                "median_fold_pnl": selected["median_fold_pnl"],
                "fold_pnl_std": selected["fold_pnl_std"],
                "stability_score": selected["stability_score"],
                "folds": selected["folds"],
                "policy_results": stability["policy_results"],
            },
            "validation_calibration": stability["validation_calibration"],
            "validation_rmse": stability["validation_rmse"],
            "active_diagnostic": {
                "policy": stability["best_active_policy"],
                **(stability["best_active"] or {
                    "aggregate": {"pnl_account": 0.0, "trade_count": 0},
                    "positive_fold_count": 0,
                    "median_fold_pnl": 0.0,
                    "fold_pnl_std": 0.0,
                    "stability_score": -math.inf,
                    "folds": [],
                }),
            },
            "fit_metadata": fit_metadata,
        }
        reports.append(report)
        oof_by_candidate[spec["candidate"]] = oof

    viable_reports = [report for report in reports if report["walk_forward"]["viable"]]
    best_research = max(
        reports,
        key=lambda report: (
            float(report["active_diagnostic"]["stability_score"]),
            float(report["active_diagnostic"]["aggregate"].get("pnl_account", 0.0)),
        ),
        default=None,
    )
    best_screen = max(
        viable_reports,
        key=lambda report: (
            float(report["walk_forward"]["stability_score"]),
            int(report["walk_forward"]["positive_fold_count"]),
            float(report["validation"].get("pnl_account", 0.0)),
        ),
        default=None,
    )

    if best_screen is None:
        best = {
            "candidate": None,
            "kind": "no_trade",
            "model_family": "no_trade",
            "feature_family": None,
            "label_family": None,
            "horizon": 13,
            "actual_col": "actual_ev_13m",
            "policy": "no_trade",
            "threshold": None,
            "validation": {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0},
            "final": {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0, "max_drawdown_pct": 0.0},
            "selected_final_trades": pd.DataFrame(),
            "raw_topk": {},
            "final_policy_results": {},
            "gate_attribution": {},
            "walk_forward": {"viable": False},
        }
    else:
        spec = specs_by_candidate[best_screen["candidate"]]
        oof = oof_by_candidate[best_screen["candidate"]]
        _, final_score, final_fit_metadata, reason = _fit_candidate_scores(
            spec,
            development,
            split.final,
            split.final,
            feature_sets,
            cfg,
            seed + 10000,
        )
        if reason:
            raise RuntimeError(f"selected walk-forward candidate failed final refit: {reason}")
        final = split.final.copy()
        final["predicted_ev"] = final_score
        threshold = float(best_screen["threshold"]) if best_screen["threshold"] is not None else math.inf
        rmse = float(best_screen["validation_rmse"] or 0.0)
        selected_policy = best_screen["policy"]
        validation_policy_results: dict[str, Any] = {}
        final_policy_results: dict[str, Any] = {}
        selected_final_trades = final.iloc[0:0].copy()
        for policy in best_screen["walk_forward"]["policy_results"]:
            validation_trades, _ = select_trades_for_policy(oof, "predicted_ev", spec["actual_col"], policy, threshold, rmse, cfg)
            final_trades, _ = select_trades_for_policy(final, "predicted_ev", spec["actual_col"], policy, threshold, rmse, cfg)
            validation_policy_results[policy] = _trade_metrics(
                validation_trades,
                oof,
                "predicted_ev",
                spec["actual_col"],
                f"{spec['candidate']}|walk_forward|{policy}",
            )
            final_policy_results[policy] = _trade_metrics(
                final_trades,
                final,
                "predicted_ev",
                spec["actual_col"],
                f"{spec['candidate']}|diagnostic_final|{policy}",
            )
            if policy == selected_policy:
                selected_final_trades = final_trades
        gross_col = f"gross_mid_ev_{spec['horizon']}m"
        best = {
            **best_screen,
            "validation": validation_policy_results[selected_policy],
            "final": final_policy_results[selected_policy],
            "validation_policy_results": validation_policy_results,
            "final_policy_results": final_policy_results,
            "final_fit_metadata": final_fit_metadata,
            "final_rank_ic": rank_ic(final, "predicted_ev", spec["actual_col"]),
            "raw_topk": {
                str(k): topk_result(final, "predicted_ev", spec["actual_col"], gross_col if gross_col in final.columns else None, k)
                for k in [1, 3, 5]
            },
            "gate_attribution": gate_attribution(
                oof,
                final,
                "predicted_ev",
                spec["actual_col"],
                cfg,
                selected_policy,
                threshold,
                rmse,
                selected_final_trades,
            ),
            "selected_final_trades": selected_final_trades,
        }

    discovered = next(
        (
            report for report in reports
            if report["model_key"] == "extra_trees"
            and report["feature_family"] == "I_full"
            and report["label_family"] == "TP_before_SL_proxy"
            and int(report["horizon"]) == 21
        ),
        None,
    )
    scorecard = [
        {
            "candidate": report["candidate"],
            "kind": report["kind"],
            "feature_family": report["feature_family"],
            "label_family": report["label_family"],
            "model_family": report["model_family"],
            "horizon": report["horizon"],
            "selected_policy": report["policy"],
            "validation_pnl": report["validation"].get("pnl_account"),
            "validation_trades": report["validation"].get("trade_count"),
            "positive_fold_count": report["walk_forward"]["positive_fold_count"],
            "median_fold_pnl": report["walk_forward"]["median_fold_pnl"],
            "stability_score": report["walk_forward"]["stability_score"],
            "walk_forward_viable": report["walk_forward"]["viable"],
            "best_active_policy": report["active_diagnostic"]["policy"],
            "best_active_pnl": report["active_diagnostic"]["aggregate"].get("pnl_account"),
            "best_active_trades": report["active_diagnostic"]["aggregate"].get("trade_count"),
            "best_active_positive_fold_count": report["active_diagnostic"]["positive_fold_count"],
            "best_active_median_fold_pnl": report["active_diagnostic"]["median_fold_pnl"],
            "best_active_stability_score": report["active_diagnostic"]["stability_score"],
            "top_decile_actual_ev": report["validation_calibration"].get("top_decile_actual_ev"),
        }
        for report in reports
    ]
    return {
        "reports": reports,
        "best": best,
        "best_research_candidate": best_research,
        "skipped": skipped,
        "scorecard": scorecard,
        "discovered_candidate": discovered,
        "feature_column_counts": {key: len(value) for key, value in feature_sets.items()},
        "feature_sets": feature_sets,
        "walk_forward_folds": fold_meta,
        "declared_candidate_count": int(len(specs_by_candidate)),
        "viable_candidate_count": int(len(viable_reports)),
    }


def _fit_ml_grid(split: SplitData, cfg: dict[str, Any]) -> dict[str, Any]:
    seed = int(cfg.get("model", {}).get("random_state", 42))
    train_cap = min(int(cfg.get("surface", {}).get("max_train_rows_per_candidate", 25000)), 12000)
    feature_sets = _feature_sets(split.train)
    reports: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for feature_family in ML_FEATURE_FAMILIES:
        features = feature_sets.get(feature_family, [])
        if not features:
            skipped.append({"feature_family": feature_family, "reason": "no usable columns"})
            continue
        for label_family in ML_LABEL_FAMILIES:
            for horizon in ML_HORIZONS:
                target = _target_col(label_family, horizon)
                actual_col = f"actual_ev_{horizon}m"
                if target not in split.train.columns or actual_col not in split.final.columns:
                    skipped.append({"feature_family": feature_family, "label_family": label_family, "horizon": horizon, "reason": "missing target or actual column"})
                    continue
                finite_train = split.train.replace([np.inf, -np.inf], np.nan).dropna(subset=[target])
                if len(finite_train) < 1000:
                    skipped.append({"feature_family": feature_family, "label_family": label_family, "horizon": horizon, "reason": f"too few finite train rows ({len(finite_train)})"})
                    continue
                train_sample = _sample_train(finite_train, train_cap, seed)
                for model_label, model_name in ML_MODEL_FAMILIES:
                    val_score, _unused_final_score, reason = _fit_predict_model(
                        model_name,
                        train_sample,
                        split.validation,
                        split.validation,
                        features,
                        target,
                        seed,
                    )
                    name = _candidate_name(model_label, feature_family, label_family, horizon)
                    if reason:
                        skipped.append({
                            "candidate": name,
                            "feature_family": feature_family,
                            "label_family": label_family,
                            "horizon": horizon,
                            "model_family": model_label,
                            "reason": reason,
                        })
                        continue
                    val = split.validation.copy()
                    val["predicted_ev"] = val_score
                    if val["predicted_ev"].replace([np.inf, -np.inf], np.nan).notna().sum() < 100:
                        skipped.append({"candidate": name, "reason": "too few finite validation predictions"})
                        continue
                    evaluation = quick_evaluate_validation_policies(val, "predicted_ev", actual_col, cfg)
                    best_policy = evaluation["best_policy"]
                    report = {
                        "candidate": name,
                        "feature_family": feature_family,
                        "label_family": label_family,
                        "model_family": model_label,
                        "model_key": model_name,
                        "horizon": horizon,
                        "target": target,
                        "actual_col": actual_col,
                        "policy": best_policy,
                        "threshold": evaluation["threshold"],
                        "validation": evaluation["validation"][best_policy],
                        "validation_policy_results": evaluation["validation"],
                        "validation_calibration": evaluation["validation_calibration"],
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
        features = feature_sets.get(best_screen["feature_family"], [])
        finite_train = split.train.replace([np.inf, -np.inf], np.nan).dropna(subset=[best_screen["target"]])
        train_sample = _sample_train(finite_train, train_cap, seed)
        val_score, final_score, reason = _fit_predict_model(
            best_screen["model_key"],
            train_sample,
            split.validation,
            split.final,
            features,
            best_screen["target"],
            seed,
        )
        if reason:
            skipped.append({"candidate": best_screen["candidate"], "stage": "final_refit", "reason": reason})
            best = {**best_screen, "final": {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0}, "selected_final_trades": pd.DataFrame()}
        else:
            val = split.validation.copy()
            fin = split.final.copy()
            val["predicted_ev"] = val_score
            fin["predicted_ev"] = final_score
            evaluation = evaluate_score_policies(val, fin, "predicted_ev", best_screen["actual_col"], cfg, best_screen["candidate"])
            best_policy = evaluation["best_policy"]
            gross_col = f"gross_mid_ev_{best_screen['horizon']}m"
            final_trades = evaluation["selected_final_trades"]
            best = {
                **best_screen,
                "policy": best_policy,
                "threshold": evaluation["threshold"],
                "validation": evaluation["validation"][best_policy],
                "final": evaluation["final"][best_policy],
                "validation_policy_results": evaluation["validation"],
                "final_policy_results": evaluation["final"],
                "validation_calibration": evaluation["validation_calibration"],
                "final_rank_ic": evaluation["final_rank_ic"],
                "raw_topk": {str(k): topk_result(fin, "predicted_ev", best_screen["actual_col"], gross_col if gross_col in fin.columns else None, k) for k in [1, 3, 5]},
                "gate_attribution": gate_attribution(
                    val,
                    fin,
                    "predicted_ev",
                    best_screen["actual_col"],
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
            "candidate": None,
            "policy": None,
            "validation": {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0},
            "final": {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0},
            "selected_final_trades": pd.DataFrame(),
            "actual_col": "actual_ev_13m",
        }
    discovered = next(
        (
            r for r in reports
            if r["model_key"] == "extra_trees"
            and r["feature_family"] == "I_full"
            and r["label_family"] == "TP_before_SL_proxy"
            and int(r["horizon"]) == 21
        ),
        None,
    )
    scorecard = [
        {
            "candidate": r["candidate"],
            "feature_family": r["feature_family"],
            "label_family": r["label_family"],
            "model_family": r["model_family"],
            "horizon": r["horizon"],
            "selected_policy": r["policy"],
            "validation_pnl": r["validation"].get("pnl_account"),
            "validation_trades": r["validation"].get("trade_count"),
        }
        for r in reports
    ]
    return {
        "reports": reports,
        "best": best,
        "skipped": skipped,
        "scorecard": scorecard,
        "discovered_candidate": discovered,
        "feature_column_counts": {k: len(v) for k, v in feature_sets.items()},
    }


def _best_simple_baseline(split: SplitData) -> dict[str, Any]:
    branches = _score_rule_branches(split)
    baselines = {
        "no_trade": {
            "validation": {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0},
            "final": {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0, "max_drawdown_pct": 0.0},
        }
    }
    for name in [
        "random_candidate",
        "top_1m_momentum_continuation",
        "top_5m_momentum_continuation",
        "top_5m_snapback_reversal",
        "top_volatility_compression_breakout",
        "strongest_vs_weakest_currency",
        "lowest_spread_top_momentum",
        "movement_only_branch",
        "currency_strength_rotation_branch",
    ]:
        if branches.get(name):
            baselines[name] = branches[name]
    best_name, best_report = max(baselines.items(), key=lambda kv: float(kv[1].get("validation", {}).get("pnl_account", 0.0)))
    return {"name": best_name, "report": best_report, "all": baselines}


def _topk_value_result(
    frame: pd.DataFrame,
    score_col: str,
    actual_col: str,
    unit: str,
    k: int,
) -> dict[str, Any]:
    tmp = frame[["decision_time_utc", score_col, actual_col]].replace([np.inf, -np.inf], np.nan).dropna()
    top = tmp.sort_values(score_col, ascending=False).groupby("decision_time_utc", as_index=False).head(k)
    if top.empty:
        return {
            "rows": 0,
            "timestamps": 0,
            "actual_value_mean": None,
            "actual_value_sum": 0.0,
            "positive_rate": None,
            "unit": unit,
        }
    actual = pd.to_numeric(top[actual_col], errors="coerce").fillna(0.0)
    return {
        "rows": int(len(top)),
        "timestamps": int(top["decision_time_utc"].nunique()),
        "actual_value_mean": float(actual.mean()),
        "actual_value_sum": float(actual.sum()),
        "positive_rate": float((actual > 0.0).mean()),
        "unit": unit,
    }


def _signal_metric(
    frame: pd.DataFrame,
    score_col: str,
    actual_col: str,
    label: str,
    unit: str = "account_currency",
) -> dict[str, Any]:
    topk = (
        lambda k: topk_result(frame, score_col, actual_col, None, k)
        if unit == "account_currency"
        else _topk_value_result(frame, score_col, actual_col, unit, k)
    )
    return {
        "signal": label,
        "target": actual_col,
        "unit": unit,
        "rank_ic": rank_ic(frame, score_col, actual_col),
        "top1": topk(1),
        "top3": topk(3),
        "top5": topk(5),
    }


def _movement_direction_summary(split: SplitData, selected: dict[str, Any], reports: list[dict[str, Any]]) -> dict[str, Any]:
    horizon = int(selected.get("horizon") or 13)
    actual_col = f"actual_ev_{horizon}m"
    final = split.final.copy()
    signed_score = "side_momentum_5m_atr"
    movement_score = "movement_forecast_proxy"
    direction = _signal_metric(final, signed_score, actual_col, "direction_only")
    movement_actual = f"future_range_pips_{horizon}m"
    movement = _signal_metric(
        final,
        movement_score,
        movement_actual if movement_actual in final.columns else actual_col,
        "movement_only",
        "pips" if movement_actual in final.columns else "account_currency",
    )
    mfe_actual = f"mfe_pips_{horizon}m"
    mfe = _signal_metric(
        final,
        movement_score,
        mfe_actual if mfe_actual in final.columns else actual_col,
        "MFE_proxy",
        "pips" if mfe_actual in final.columns else "account_currency",
    )
    z_dir = (pd.to_numeric(final[signed_score], errors="coerce") - pd.to_numeric(split.train[signed_score], errors="coerce").mean()) / (pd.to_numeric(split.train[signed_score], errors="coerce").std() or 1.0)
    z_mov = (pd.to_numeric(final[movement_score], errors="coerce") - pd.to_numeric(split.train[movement_score], errors="coerce").mean()) / (pd.to_numeric(split.train[movement_score], errors="coerce").std() or 1.0)
    final["direction_movement_combined_score"] = z_dir.fillna(0.0) + z_mov.fillna(0.0)
    combined = _signal_metric(final, "direction_movement_combined_score", actual_col, "direction_plus_movement")
    path_candidates = [
        r for r in reports
        if r.get("label_family") in {"TP_before_SL_proxy", "expected_MFE"} and int(r.get("horizon", -1)) == horizon
    ]
    best_path = max(path_candidates, key=lambda r: float(r["validation"].get("pnl_account", 0.0)), default=None)
    selected_driver = (
        "movement-first conditional direction"
        if selected.get("kind") == "movement_first"
        else "movement/path-driven"
        if selected.get("label_family") in {"TP_before_SL_proxy", "expected_MFE"}
        else "no-trade"
        if selected.get("kind") == "no_trade"
        else "direction-driven"
    )
    return {
        "horizon": horizon,
        "direction_only_raw_topk_result": direction,
        "movement_only_raw_topk_result": movement,
        "mfe_tp_before_sl_raw_topk_result": {
            "candidate": best_path.get("candidate") if best_path else None,
            "raw_topk": best_path.get("raw_topk") if best_path else None,
            "final": best_path.get("final") if best_path else None,
        },
        "direction_plus_movement_combined_result": combined,
        "signed_return_rank_ic": direction.get("rank_ic"),
        "movement_rank_ic": rank_ic(final, movement_score, movement_actual) if movement_actual in final.columns else None,
        "MFE_rank_ic": rank_ic(final, movement_score, mfe_actual) if mfe_actual in final.columns else None,
        "selected_winner_driver": selected_driver,
        "interpretation": "Direction uses account-currency EV. Movement and MFE diagnostics are explicitly reported in pips and are not account P/L.",
    }


def _comparison_flags(ml_best: dict[str, Any], ar_best: dict[str, Any], simple_best: dict[str, Any]) -> dict[str, Any]:
    ml_final = float(ml_best.get("final", {}).get("pnl_account", 0.0) or 0.0)
    ar_final = float(ar_best.get("final", {}).get("pnl_account", 0.0) or 0.0)
    simple_final = float(simple_best.get("report", {}).get("final", {}).get("pnl_account", 0.0) or 0.0)
    top_bucket = ml_best.get("final", {}).get("top_predicted_ev_bucket_actual_ev")
    return {
        "ml_positive_final_pnl": ml_final > 0.0,
        "ml_beats_no_trade": ml_final > 0.0,
        "ml_beats_best_simple_baseline": ml_final > simple_final,
        "ml_beats_arima_ar_baseline": ml_final > ar_final,
        "ml_top_predicted_ev_bucket_positive": (top_bucket or -1e18) > 0.0,
        "arima_ar_final_pnl": ar_final,
        "best_simple_final_pnl": simple_final,
    }


def _feature_leakage_audit(
    frame: pd.DataFrame,
    feature_sets: dict[str, list[str]],
) -> dict[str, Any]:
    family_report = build_feature_family_report(frame)
    selected_columns = sorted({column for columns in feature_sets.values() for column in columns})
    assert_decision_time_feature_columns(selected_columns)
    forbidden_selected = family_report.get("forbidden_selected_feature_columns", [])
    return {
        "decision_time_allowlist_enforced": True,
        "selected_feature_set_counts": {name: int(len(columns)) for name, columns in feature_sets.items()},
        "selected_feature_columns": selected_columns,
        "forbidden_selected_feature_columns": forbidden_selected,
        "excluded_label_like_columns": family_report.get("excluded_label_like_columns", []),
        "excluded_future_like_columns": family_report.get("excluded_future_like_columns", []),
        "rejected_unapproved_numeric_columns": family_report.get("rejected_unapproved_numeric_columns", []),
        "residual_abs_move_feature_columns": [column for column in selected_columns if column.startswith("abs_move_pips_")],
        "no_feature_leakage": bool(family_report.get("no_feature_leakage")) and not forbidden_selected,
        "failure_mode": "fail_closed_before_model_fit",
    }


def _prior_final_was_inspected(output_dir: Path, start: str, end: str, tier: str) -> bool:
    for path in output_dir.parent.glob("validation_replay_*/VALIDATION_REPLAY_REPORT.json"):
        if output_dir in path.parents:
            continue
        try:
            prior = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if prior.get("start") == start and prior.get("end") == end and prior.get("tier") == tier:
            return True
    return False


def _reuse_latest_arima_report(
    output_dir: Path,
    start: str,
    end: str,
    tier: str,
    ml_winner: dict[str, Any],
) -> dict[str, Any] | None:
    candidates = sorted(
        output_dir.parent.glob("arima_baseline_*/ARIMA_BASELINE_REPORT.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for path in candidates:
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if report.get("start") != start or report.get("end") != end or report.get("tier") != tier:
            continue
        report = dict(report)
        report["reused_from"] = str(path)
        report.setdefault("comparison", {})
        report["comparison"]["validation_replay_ml_winner"] = ml_winner
        write_report_pair(output_dir, "ARIMA_BASELINE_REPORT", report)
        best = report.get("best_validation_selected_variant") or {
            "variant": None,
            "selected_policy": None,
            "validation": report.get("validation_result"),
            "final": report.get("untouched_final_result"),
        }
        return {
            "summary": report,
            "best": best,
            "reports": [],
            "variants_skipped": report.get("variants_skipped", []),
            "statsmodels_available": report.get("statsmodels_available"),
            "output_dir": output_dir,
            "reused_from": str(path),
        }
    return None


def run_validation_replay(
    cfg: dict[str, Any],
    output_dir: Path,
    start: str,
    end: str,
    tier: str = "tier1",
    pairs: list[str] | None = None,
    max_rows_per_pair: int | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    df = _reuse_or_build_dataset(cfg, output_dir, start, end, tier, pairs, max_rows_per_pair)
    df = _add_gross_labels(df, cfg)
    df = _add_validation_targets(df, cfg)
    split = _split(df, cfg)
    development_for_audit = pd.concat([split.train, split.validation], ignore_index=True)
    leakage_audit = _feature_leakage_audit(development_for_audit, _feature_sets(development_for_audit))
    write_report_pair(output_dir, "VALIDATION_REPLAY_LEAKAGE_AUDIT", leakage_audit)
    prior_final_inspected = _prior_final_was_inspected(output_dir, start, end, tier)

    ml = _fit_walk_forward_grid(split, cfg)
    ml_best = ml["best"]
    simple = _best_simple_baseline(split)
    ml_winner_for_ar = {
        "candidate": ml_best.get("candidate"),
        "validation": ml_best.get("validation"),
        "final": ml_best.get("final"),
    }
    ar = _reuse_latest_arima_report(output_dir, start, end, tier, ml_winner_for_ar)
    if ar is None:
        ar = run_arima_baseline_analysis(
            cfg,
            output_dir,
            start=start,
            end=end,
            tier=tier,
            pairs=pairs,
            max_rows_per_pair=max_rows_per_pair,
            split=split,
            dataset=df,
            write_reports=True,
            ml_winner=ml_winner_for_ar,
        )
    ar_best = ar["best"]
    ar_final = float(ar_best.get("final", {}).get("pnl_account", 0.0) or 0.0)
    ml_final = float(ml_best.get("final", {}).get("pnl_account", 0.0) or 0.0)
    overall_winner = "ML" if float(ml_best.get("validation", {}).get("pnl_account", 0.0) or 0.0) >= float(ar_best.get("validation", {}).get("pnl_account", 0.0) or 0.0) else "ARIMA_AR"
    overall = ml_best if overall_winner == "ML" else ar_best
    overall_final = float(overall.get("final", {}).get("pnl_account", 0.0) or 0.0)

    actual_col = ml_best.get("actual_col", f"actual_ev_{ml_best.get('horizon', 13)}m")
    ml_stress = cost_stress_for_trades(ml_best.get("selected_final_trades", pd.DataFrame()), actual_col, cfg, f"ML|{ml_best.get('candidate')}")
    ar_actual_col = f"actual_ev_{ar_best.get('horizon')}m" if ar_best.get("horizon") else "actual_ev_1m"
    ar_stress = ar["summary"].get("cost_stress") or cost_stress_for_trades(ar_best.get("selected_final_trades", pd.DataFrame()), ar_actual_col, cfg, f"ARIMA_AR|{ar_best.get('variant')}")
    signal_summary = _movement_direction_summary(split, ml_best, ml["reports"])
    flags = _comparison_flags(ml_best, ar_best, simple)
    discovered = ml.get("discovered_candidate")

    selected_gate = ml_best.get("gate_attribution", {})
    raw_top1_pnl = float(ml_best.get("raw_topk", {}).get("1", {}).get("pnl_account", 0.0) or 0.0)
    current_allocator_pnl = float(ml_best.get("final_policy_results", {}).get("current_allocator_gated", {}).get("pnl_account", 0.0) or 0.0)
    gates_suppressed_positive = raw_top1_pnl > 0.0 and current_allocator_pnl <= 0.0

    report = {
        "run_id": output_dir.name,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "command_mode": "validation-replay",
        "start": start,
        "end": end,
        "tier": tier,
        "split": split.meta,
        "selection_protocol": {
            "candidate_grid_declared_before_final_scoring": True,
            "model_config_threshold_policy_selected_on_development_walk_forward_only": True,
            "walk_forward_fold_count": int(len(ml["walk_forward_folds"])),
            "minimum_positive_folds_to_reach_final": int(math.ceil(len(ml["walk_forward_folds"]) / 2)),
            "final_test_used_once_after_candidate_freeze": True,
            "final_period_previously_inspected": bool(prior_final_inspected),
            "evidence_status": "correctness_diagnostic_not_fresh_holdout" if prior_final_inspected else "fresh_holdout",
            "discovered_promising_family_included_but_not_hard_coded_as_winner": True,
        },
        "selected_ml_config": {
            "candidate": ml_best.get("candidate"),
            "kind": ml_best.get("kind"),
            "feature_family": ml_best.get("feature_family"),
            "label_family": ml_best.get("label_family"),
            "model_family": ml_best.get("model_family"),
            "horizon": ml_best.get("horizon"),
            "policy": ml_best.get("policy"),
            "threshold": ml_best.get("threshold"),
            "allocator_variant": ml_best.get("policy"),
            "walk_forward": ml_best.get("walk_forward"),
        },
        "validation_metrics": ml_best.get("validation"),
        "untouched_final_metrics": ml_best.get("final"),
        "validation_edge_carried_forward": bool(float(ml_best.get("validation", {}).get("pnl_account", 0.0) or 0.0) > 0.0 and ml_final > 0.0),
        "top_ev_bucket_remained_positive": bool((ml_best.get("final", {}).get("top_predicted_ev_bucket_actual_ev") or -1e18) > 0.0),
        "comparisons": {
            "no_trade": {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0},
            "best_simple_baseline": {"name": simple["name"], "final": simple["report"].get("final", {})},
            "best_arima_ar_baseline": {
                "variant": ar_best.get("variant"),
                "policy": ar_best.get("selected_policy"),
                "validation": ar_best.get("validation"),
                "final": ar_best.get("final"),
            },
            "flags": flags,
        },
        "overall_validation_winner": {
            "family": overall_winner,
            "validation": overall.get("validation"),
            "final": overall.get("final"),
        },
        "cost_stress_survives": bool(ml_stress.get("edge_survives_mild_stress")),
        "candidate_scorecard_top_validation": sorted(
            ml["scorecard"],
            key=lambda row: float(row["validation_pnl"]) if row.get("validation_pnl") is not None else -1e18,
            reverse=True,
        )[:40],
        "candidate_scorecard_top_final": "not computed for non-selected candidates; final was evaluated only for the frozen walk-forward-selected ML winner",
        "discovered_promising_candidate": {
            "available": discovered is not None,
            "candidate": discovered.get("candidate") if discovered else "Extra Trees|I_full|TP_before_SL_proxy|21m",
            "validation": discovered.get("validation") if discovered else None,
            "final": ml_best.get("final") if discovered is not None and discovered.get("candidate") == ml_best.get("candidate") else None,
            "selected_policy": discovered.get("policy") if discovered else None,
            "raw_topk": ml_best.get("raw_topk") if discovered is not None and discovered.get("candidate") == ml_best.get("candidate") else None,
        },
        "experiments": {
            "ml_attempted": int(len(ml["reports"])),
            "ml_declared": ml["declared_candidate_count"],
            "ml_walk_forward_viable": ml["viable_candidate_count"],
            "ml_skipped": ml["skipped"],
            "feature_column_counts": ml["feature_column_counts"],
            "arima_ar_attempted": ar["summary"].get("variants_attempted"),
            "arima_ar_skipped": ar["variants_skipped"],
        },
        "leakage_audit": leakage_audit,
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "demo_execution_enabled": False,
    }
    write_report_pair(output_dir, "VALIDATION_REPLAY_REPORT", report)

    walk_forward_report = {
        "folds": ml["walk_forward_folds"],
        "declared_candidate_count": ml["declared_candidate_count"],
        "evaluated_candidate_count": int(len(ml["reports"])),
        "viable_candidate_count": ml["viable_candidate_count"],
        "selection_policies": SELECTION_POLICIES,
        "selected_candidate": ml_best.get("candidate"),
        "selected_candidate_walk_forward": ml_best.get("walk_forward"),
        "best_research_candidate": ml.get("best_research_candidate"),
        "scorecard": sorted(
            ml["scorecard"],
            key=lambda row: float(row["best_active_stability_score"])
            if row.get("best_active_stability_score") is not None
            else -1e18,
            reverse=True,
        ),
        "final_period_previously_inspected": bool(prior_final_inspected),
    }
    write_report_pair(output_dir, "VALIDATION_REPLAY_WALK_FORWARD", walk_forward_report)

    movement_first_reports = [report_row for report_row in ml["reports"] if report_row.get("kind") == "movement_first"]
    movement_first_report = {
        "architecture": "side-neutral cost-clearing movement classifier followed by movement-conditioned long/short EV",
        "candidate_count": int(len(movement_first_reports)),
        "best_development_candidate": max(
            movement_first_reports,
            key=lambda row: float(row.get("active_diagnostic", {}).get("stability_score", -1e18)),
            default=None,
        ),
        "selected_as_overall_ml_candidate": ml_best.get("kind") == "movement_first",
        "selected_candidate": ml_best.get("candidate") if ml_best.get("kind") == "movement_first" else None,
        "final_result": ml_best.get("final") if ml_best.get("kind") == "movement_first" else None,
        "scorecard": [row for row in ml["scorecard"] if row.get("kind") == "movement_first"],
    }
    write_report_pair(output_dir, "VALIDATION_REPLAY_MOVEMENT_FIRST", movement_first_report)

    gate_report = {
        "selected_candidate": ml_best.get("candidate"),
        "selected_policy": ml_best.get("policy"),
        "raw_top1_top3_top5_before_gates": {
            "top1": ml_best.get("raw_topk", {}).get("1"),
            "top3": ml_best.get("raw_topk", {}).get("3"),
            "top5": ml_best.get("raw_topk", {}).get("5"),
        },
        "fixed_trade_quota_result": ml_best.get("final_policy_results", {}).get("fixed_trade_quota_top1"),
        "positive_predicted_EV_threshold_result": ml_best.get("final_policy_results", {}).get("positive_predicted_ev"),
        "calibrated_lower_bound_result": ml_best.get("final_policy_results", {}).get("calibrated_lower_bound"),
        "no_edge_gated_result": ml_best.get("final_policy_results", {}).get("no_edge_gated"),
        "current_allocator_gated_result": ml_best.get("final_policy_results", {}).get("current_allocator_gated"),
        "selected_policy_result": ml_best.get("final"),
        "gate_attribution": selected_gate,
        "gates_suppressed_positive_candidates": bool(gates_suppressed_positive),
        "interpretation": "Gate counts are final-period counts for the development walk-forward-selected ML candidate. Raw top-k rows are reported before gates so a positive ranking surface is not hidden by allocator filters.",
    }
    write_report_pair(output_dir, "VALIDATION_REPLAY_GATE_ATTRIBUTION", gate_report)

    stress_report = {
        "validation_selected_ml_winner": ml_stress,
        "best_validation_selected_arima_ar_baseline": ar_stress,
        "edge_survives_mild_stress": {
            "ML": bool(ml_stress.get("edge_survives_mild_stress")),
            "ARIMA_AR": bool(ar_stress.get("edge_survives_mild_stress")),
        },
        "edge_exists_only_under_fantasy_costs": {
            "ML": bool(ml_stress.get("edge_exists_only_under_fantasy_costs")),
            "ARIMA_AR": bool(ar_stress.get("edge_exists_only_under_fantasy_costs")),
        },
    }
    write_report_pair(output_dir, "VALIDATION_REPLAY_COST_STRESS", stress_report)
    write_report_pair(output_dir, "VALIDATION_REPLAY_SIGNAL_SUMMARY", signal_summary)

    best_simple_final = float(simple["report"].get("final", {}).get("pnl_account", 0.0) or 0.0)
    overall_top_bucket = (
        ml_best.get("final", {}).get("top_predicted_ev_bucket_actual_ev")
        if overall_winner == "ML"
        else ar_best.get("final", {}).get("top_predicted_ev_bucket_actual_ev")
    )
    pass_checks = {
        "development_walk_forward_only_selection": True,
        "fresh_untouched_final_period": not prior_final_inspected,
        "positive_untouched_final_account_pnl_after_realistic_costs": overall_final > 0.0,
        "beats_no_trade": overall_final > 0.0,
        "beats_best_simple_baseline": overall_final > best_simple_final,
        "beats_arima_ar_baseline_unless_ar_is_winner": bool(overall_winner == "ARIMA_AR" or ml_final > ar_final),
        "top_predicted_ev_bucket_actual_ev_positive": (overall_top_bucket or -1e18) > 0.0,
        "no_leakage": bool(leakage_audit.get("no_feature_leakage")),
        "live_execution_disabled": True,
    }
    verdict = "PASS" if all(pass_checks.values()) else "FAIL"
    failure_reasons = []
    if verdict == "FAIL":
        if ml_final <= 0.0 and raw_top1_pnl > 0.0:
            failure_reasons.append("final-selected discovery signal did not survive validation selection")
        if overall_final <= 0.0:
            failure_reasons.append("no-trade beat all or costs erased edge")
        if gates_suppressed_positive:
            failure_reasons.append("gates suppressed positive raw signal")
        if ar_final >= ml_final and overall_winner != "ML":
            failure_reasons.append("ARIMA/AR baseline beat ML on validation selection")
        if signal_summary.get("signed_return_rank_ic") is not None and abs(float(signal_summary["signed_return_rank_ic"])) < 0.05:
            failure_reasons.append("direction weak")
        if raw_top1_pnl > 0.0 and ml_final <= 0.0:
            failure_reasons.append("movement/path signal present but not tradable by selected gates")
        if int(ml_best.get("final", {}).get("trade_count", 0) or 0) < 20:
            failure_reasons.append("too few trades")
        if prior_final_inspected:
            failure_reasons.append("final period was previously inspected; this run is diagnostic, not fresh evidence")
        if ml_best.get("kind") == "no_trade":
            failure_reasons.append("no model candidate passed walk-forward viability")
        if not leakage_audit.get("no_feature_leakage"):
            failure_reasons.append("feature leakage audit failed")
    final_summary = {
        "verdict": verdict,
        "pass_checks": pass_checks,
        "failure_reasons": failure_reasons or ([] if verdict == "PASS" else ["implementation incomplete or edge failed unspecified pass check"]),
        "positive_raw_topk_signal_survived_validation_selected_replay": bool(raw_top1_pnl > 0.0 and ml_final > 0.0),
        "gates_suppressed_positive_candidates": bool(gates_suppressed_positive),
        "arima_ar_baseline_was_beaten": bool(ml_final > ar_final),
        "cost_stress_survived": bool(ml_stress.get("edge_survives_mild_stress") if overall_winner == "ML" else ar_stress.get("edge_survives_mild_stress")),
        "biggest_remaining_blocker": (
            "no model candidate passed development walk-forward viability"
            if ml_best.get("kind") == "no_trade" else
            "validation-selected edge did not produce positive final P/L after costs"
            if overall_final <= 0.0 else
            "mild cost stress did not survive"
            if not (ml_stress.get("edge_survives_mild_stress") if overall_winner == "ML" else ar_stress.get("edge_survives_mild_stress")) else
            "validate on a later untouched period"
        ),
        "selected_ml_config": report["selected_ml_config"],
        "ml_validation_result": ml_best.get("validation"),
        "ml_final_result": ml_best.get("final"),
        "best_arima_ar_baseline": {
            "variant": ar_best.get("variant"),
            "policy": ar_best.get("selected_policy"),
            "validation": ar_best.get("validation"),
            "final": ar_best.get("final"),
        },
        "no_trade_result": {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0},
        "best_simple_baseline_result": {"name": simple["name"], "final": simple["report"].get("final", {})},
        "overall_validation_winner": overall_winner,
        "evidence_status": "correctness_diagnostic_not_fresh_holdout" if prior_final_inspected else "fresh_holdout",
        "fresh_untouched_final_available": not prior_final_inspected,
        "leakage_audit_passed": bool(leakage_audit.get("no_feature_leakage")),
        "walk_forward_viable_candidate_count": ml["viable_candidate_count"],
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "demo_execution_enabled": False,
        "live_execution_confirmation": "Research/backtest only; no OANDA bot, order placement, credential edit, or account-file modification was invoked.",
    }
    write_report_pair(output_dir, "VALIDATION_REPLAY_FINAL_SUMMARY", final_summary)
    write_json(output_dir / "run_manifest.json", {
        "run_dir": str(output_dir),
        "mode": "validation-replay",
        "reports": [
            "VALIDATION_REPLAY_REPORT.json",
            "VALIDATION_REPLAY_GATE_ATTRIBUTION.json",
            "ARIMA_BASELINE_REPORT.json",
            "VALIDATION_REPLAY_COST_STRESS.json",
            "VALIDATION_REPLAY_SIGNAL_SUMMARY.json",
            "VALIDATION_REPLAY_FINAL_SUMMARY.json",
            "VALIDATION_REPLAY_LEAKAGE_AUDIT.json",
            "VALIDATION_REPLAY_WALK_FORWARD.json",
            "VALIDATION_REPLAY_MOVEMENT_FIRST.json",
        ],
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "demo_execution_enabled": False,
    })
    return output_dir / "VALIDATION_REPLAY_FINAL_SUMMARY.json"
