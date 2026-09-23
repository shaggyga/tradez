#!/usr/bin/env python3
"""Seed a broad, auditable model-search agenda for the FX research trainer.

The always-on trainer is strongest as a walk-forward validator for tabular
candidate models.  This script keeps that loop intact and feeds it a wider,
structured queue while separately documenting model families that need their
own baseline evaluator instead of being forced into the tabular classifier
interface.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from oanda_model_lifecycle import ModelLifecycleRegistry, experiment_spec_hash


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = Path(os.environ.get("TRAD_PROJECT_ROOT", str(DEFAULT_PROJECT_ROOT)))
TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
CONFIG_DIR = PROJECT_ROOT / "config"

REGISTRY_ROOT = TRAINING_ROOT / "model_lifecycle"
PROMOTIONS_ROOT = TRAINING_ROOT / "promotions"
RESEARCH_LEDGER_PATH = TRAINING_ROOT / "continuous_research" / "experiment_ledger.csv"
MODEL_SPACE_ROOT = TRAINING_ROOT / "model_space"
AGENDA_CONFIG_PATH = CONFIG_DIR / "model_space_agenda.json"
AGENDA_LATEST_PATH = MODEL_SPACE_ROOT / "model_space_agenda_latest.json"

SCREEN_HOLDOUT_WEEKS = 2
SCREEN_MAX_TRAIN_ROWS = 75_000

HORIZONS = [30, 60, 120]
TECHNICAL_TARGET_MODES = ["continuation", "reversal", "long", "short"]
PRIMARY_SUBSETS = ["volatile", "non_usd_volatile", "volatile_exotic", "exotic", "majors", "all"]
PRIMARY_FEATURE_SETS = ["technical_full", "technical_core"]
PAIR_FAMILY_SUBSETS = [
    "jpy_risk",
    "exotic_high_spread",
    "volatile_non_usd",
    "chf_safe_haven",
    "commodity",
    "eur_gbp_cross",
    "usd_other",
]
SCOUT_PRIORITY_SUBSETS = [
    "volatile",
    "non_usd_volatile",
    "exotic",
    "volatile_exotic",
    "jpy_risk",
    "exotic_high_spread",
    "volatile_non_usd",
]
SCOUT_PRIORITY_MODEL_TYPES = [
    "random_forest",
    "extra_trees",
    "gradient_boosting",
    "hist_gradient_boosting",
]
MAJOR_EVENT_LEAD_COMBOS = [
    (30, 60),
    (15, 60),
    (30, 120),
    (15, 120),
    (60, 120),
]
LIVE_TECH_CHAMPION_SUBSETS = [
    "exotic_high_spread",
    "jpy_risk",
]
LIVE_PRIMARY_CHALLENGER_SUBSETS = [
    "exotic_high_spread",
    "volatile_non_usd",
    "jpy_risk",
    "commodity",
    "chf_safe_haven",
]
SCOUT_STOPCAP_POLICIES = [
    "trailing_stop010",
    "trailing_stop012",
    "trailing_stop015",
    "trailing_stop020",
    "trailing_stop025",
    "trailing_stop035",
    "trailing_stop050",
    "trailing_stop075",
    "trailing_stop100",
]


EXTERNAL_BASELINE_LANES = [
    {
        "lane": "arima_sarimax",
        "status": "baseline_evaluator_exists_for_major_pairs",
        "reason": "Time-series benchmark; useful only if it survives net-of-cost walk-forward gates.",
        "next": "Extend baseline comparison output into promotion reports, not direct live execution.",
    },
    {
        "lane": "var_vecm_cross_pair",
        "status": "not_in_current_trainer",
        "reason": "Needs multi-instrument synchronized matrices and cointegration tests.",
        "next": "Build separate evaluator for EUR/GBP/JPY/USD clusters and compare to ARIMA and tabular gates.",
    },
    {
        "lane": "garch_volatility",
        "status": "not_in_current_trainer",
        "reason": "Forecasts volatility/risk, not direct direction. Better as position sizing and stop-distance input.",
        "next": "Add ATR/GARCH volatility forecast columns to feature store, then validate as risk-control factor.",
    },
    {
        "lane": "hmm_markov_regime",
        "status": "not_in_current_trainer",
        "reason": "Regime model should label trend/range/risk-off/liquidity states before candidate models trade.",
        "next": "Train unsupervised regimes by pair family/session and add regime IDs/probabilities as features.",
    },
    {
        "lane": "kalman_state_space",
        "status": "not_in_current_trainer",
        "reason": "Useful for dynamic beta/spread relationships and smoothing, not plain row classification.",
        "next": "Prototype on pair baskets and feed residual/z-score features to tabular trainer.",
    },
    {
        "lane": "deep_sequence",
        "status": "deferred",
        "reason": "LSTM/TCN/Transformer models need GPU-aware training, strict leakage controls, and larger data windows.",
        "next": "Only add after tabular/regime baselines are stable; compare by net pips and drawdown, not raw accuracy.",
    },
    {
        "lane": "macro_news_nlp",
        "status": "separate_context_layer",
        "reason": "Macro/GPT should veto, bias, or reduce risk. It should not be allowed as an unbacktested trigger.",
        "next": "Persist pair-level macro bias snapshots and join lagged bias to training rows.",
    },
    {
        "lane": "portfolio_meta_allocator",
        "status": "shadow_ensemble_evaluator_exists",
        "reason": "Best-model-per-pair needs portfolio exposure, pair-correlation, margin, and concentration control before it can drive execution.",
        "next": "Refresh the shadow ensemble manifest from validated members and promote only after it beats active production under comparable validation.",
    },
    {
        "lane": "online_learning_bandits",
        "status": "not_in_current_trainer",
        "reason": "Useful for promotion/demotion and threshold selection, but dangerous without drawdown guardrails.",
        "next": "Use paper-only challenger allocation after validated candidates have enough shadow trades.",
    },
]


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str), encoding="utf-8")
    os.replace(tmp, path)


def completed_hashes() -> set[str]:
    if not RESEARCH_LEDGER_PATH.exists():
        return set()
    out: set[str] = set()
    try:
        with RESEARCH_LEDGER_PATH.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                value = str(row.get("spec_hash") or "").strip()
                if value:
                    out.add(value)
    except Exception:
        return out
    return out


def installed_model_types(include_new_estimators: bool = False) -> List[str]:
    model_types = ["random_forest", "gradient_boosting"]
    try:
        import lightgbm  # noqa: F401
        model_types.append("lightgbm")
    except Exception:
        pass
    try:
        import xgboost  # noqa: F401
        model_types.append("xgboost")
    except Exception:
        pass
    try:
        import catboost  # noqa: F401
        model_types.append("catboost")
    except Exception:
        pass
    try:
        import ngboost  # noqa: F401
        model_types.append("ngboost")
    except Exception:
        pass
    if include_new_estimators:
        try:
            from sklearn.ensemble import ExtraTreesClassifier, HistGradientBoostingClassifier  # noqa: F401
            model_types.extend(["extra_trees", "hist_gradient_boosting"])
        except Exception:
            pass
    return model_types


def parameter_sets(model_type: str) -> List[Dict[str, Any]]:
    if model_type == "random_forest":
        return [
            {"n_estimators": 180, "max_depth": 7, "min_samples_leaf": 10, "max_features": "sqrt", "class_weight": "balanced_subsample"},
            {"n_estimators": 240, "max_depth": 9, "min_samples_leaf": 20, "max_features": 0.7, "class_weight": "balanced"},
            {"n_estimators": 320, "max_depth": None, "min_samples_leaf": 40, "max_features": "log2", "class_weight": "balanced_subsample"},
        ]
    if model_type == "extra_trees":
        return [
            {"n_estimators": 240, "max_depth": 9, "min_samples_leaf": 10, "max_features": "sqrt", "class_weight": "balanced_subsample"},
            {"n_estimators": 360, "max_depth": 12, "min_samples_leaf": 20, "max_features": 0.7, "class_weight": "balanced"},
            {"n_estimators": 500, "max_depth": None, "min_samples_leaf": 40, "max_features": "log2", "class_weight": "balanced_subsample"},
        ]
    if model_type == "hist_gradient_boosting":
        return [
            {"max_iter": 160, "learning_rate": 0.05, "max_leaf_nodes": 31, "min_samples_leaf": 40, "l2_regularization": 0.01},
            {"max_iter": 240, "learning_rate": 0.025, "max_leaf_nodes": 63, "min_samples_leaf": 80, "l2_regularization": 0.05},
            {"max_iter": 320, "learning_rate": 0.075, "max_leaf_nodes": 15, "min_samples_leaf": 20, "l2_regularization": 0.10},
        ]
    if model_type == "gradient_boosting":
        return [
            {"n_estimators": 100, "learning_rate": 0.05, "max_depth": 3, "min_samples_leaf": 20, "subsample": 0.8},
            {"n_estimators": 160, "learning_rate": 0.025, "max_depth": 4, "min_samples_leaf": 40, "subsample": 0.65},
        ]
    if model_type == "lightgbm":
        return [
            {"n_estimators": 180, "learning_rate": 0.05, "num_leaves": 31, "min_child_samples": 40, "subsample": 0.85, "colsample_bytree": 0.85},
            {"n_estimators": 320, "learning_rate": 0.025, "num_leaves": 63, "min_child_samples": 80, "subsample": 0.70, "colsample_bytree": 0.70},
        ]
    if model_type == "xgboost":
        return [
            {"n_estimators": 180, "learning_rate": 0.05, "max_depth": 4, "subsample": 0.85, "colsample_bytree": 0.85},
            {"n_estimators": 240, "learning_rate": 0.025, "max_depth": 3, "subsample": 0.70, "colsample_bytree": 0.70},
        ]
    if model_type == "catboost":
        return [
            {"iterations": 240, "learning_rate": 0.05, "depth": 6, "l2_leaf_reg": 3.0, "random_strength": 1.0},
            {"iterations": 320, "learning_rate": 0.025, "depth": 8, "l2_leaf_reg": 5.0, "random_strength": 0.5},
        ]
    if model_type == "ngboost":
        return [
            {"n_estimators": 240, "learning_rate": 0.025, "minibatch_frac": 0.8, "col_sample": 0.8},
            {"n_estimators": 320, "learning_rate": 0.01, "minibatch_frac": 0.65, "col_sample": 0.65},
        ]
    raise ValueError(f"unsupported model_type {model_type!r}")


def make_spec(
    *,
    dataset_kind: str,
    model_type: str,
    target: str,
    outcome: str,
    feature_set: str,
    instrument_subset: str,
    research_role: str,
    parameters: Dict[str, Any],
    direction_target: str = "",
    execution_policy: str = "",
    validation_weeks: int = SCREEN_HOLDOUT_WEEKS,
    max_train_rows: int = SCREEN_MAX_TRAIN_ROWS,
    agenda_reason: str = "",
    deployment_lane: str = "",
    account_focus: str = "",
) -> Dict[str, Any]:
    spec = {
        "source": "model_space_agenda",
        "evaluation_stage": "screen",
        "validation_weeks": validation_weeks,
        "max_train_rows": max_train_rows,
        "dataset_kind": dataset_kind,
        "model_type": model_type,
        "target": target,
        "outcome": outcome,
        "feature_set": feature_set,
        "instrument_subset": instrument_subset,
        "research_role": research_role,
        "direction_target": direction_target,
        "execution_policy": execution_policy,
        "parameters": parameters,
        "agenda_reason": agenda_reason,
    }
    if deployment_lane:
        spec["deployment_lane"] = deployment_lane
    if account_focus:
        spec["account_focus"] = account_focus
    return spec


def scout_priority_model_types(model_types: Sequence[str]) -> List[str]:
    preferred = [
        model_type
        for model_type in SCOUT_PRIORITY_MODEL_TYPES
        if model_type in set(model_types)
    ]
    return preferred or list(model_types[:2])


def production_scout_priority_specs(model_types: Sequence[str]) -> List[Dict[str, Any]]:
    """Focused queue lane for the live technical scout research goal.

    The generic agenda is intentionally broad.  This lane is intentionally
    opinionated: lead-time major-move targets, return-curve/path-quality
    targets, and volatile/pair-family subsets using the tree families that have
    produced the most stable local evidence so far.
    """
    specs: List[Dict[str, Any]] = []
    priority_models = scout_priority_model_types(model_types)
    for lead_minutes, horizon in MAJOR_EVENT_LEAD_COMBOS:
        for policy in [*SCOUT_STOPCAP_POLICIES, "trailing", "fixed"]:
            for subset in SCOUT_PRIORITY_SUBSETS:
                for feature_set in PRIMARY_FEATURE_SETS:
                    for model_type in priority_models:
                        for params in parameter_sets(model_type)[:2]:
                            specs.append(make_spec(
                                dataset_kind="technical_spike",
                                model_type=model_type,
                                target=f"major_event_lead_{lead_minutes}_{horizon}",
                                outcome=f"two_stage_{policy}_net_atr_{horizon}",
                                feature_set=feature_set,
                                instrument_subset=subset,
                                research_role="major_move_factor",
                                direction_target=(
                                    f"major_direction_up_lead_{lead_minutes}_{horizon}"
                                ),
                                execution_policy=policy,
                                parameters=params,
                                agenda_reason=(
                                    "production scout priority: lead-time "
                                    "major-move prediction for volatile and "
                                    "pair-family specialist subsets"
                                ),
                            ))
    for horizon in [120, 60]:
        for mode in TECHNICAL_TARGET_MODES:
            for subset in SCOUT_PRIORITY_SUBSETS:
                for feature_set in PRIMARY_FEATURE_SETS:
                    for model_type in priority_models:
                        for params in parameter_sets(model_type)[:2]:
                            specs.append(make_spec(
                                dataset_kind="technical_spike",
                                model_type=model_type,
                                target=f"{mode}_curve_profit_{horizon}",
                                outcome=f"{mode}_curve_net_atr_{horizon}",
                                feature_set=feature_set,
                                instrument_subset=subset,
                                research_role="return_curve_candidate",
                                execution_policy="curve",
                                parameters=params,
                                agenda_reason=(
                                    "production scout priority: path-aware "
                                    "return curve model for entry/exit quality"
                                ),
                            ))
    for horizon in [120, 60]:
        for subset in SCOUT_PRIORITY_SUBSETS:
            for feature_set in PRIMARY_FEATURE_SETS:
                for model_type in priority_models:
                    for params in parameter_sets(model_type)[:2]:
                        specs.append(make_spec(
                            dataset_kind="profitable_move_precursor",
                            model_type=model_type,
                            target=f"profitable_any_move_{horizon}",
                            outcome=f"best_net_atr_{horizon}",
                            feature_set=feature_set,
                            instrument_subset=subset,
                            research_role="profitable_move_precursor",
                            direction_target=f"best_direction_up_{horizon}",
                            parameters=params,
                            agenda_reason=(
                                "production scout priority: broad profitable "
                                "move opportunity gate for volatile/pair-family "
                                "specialists"
                            ),
                        ))
    return specs


def live_account_split_priority_specs(model_types: Sequence[str]) -> List[Dict[str, Any]]:
    """Focused candidates for the current live-account split.

    GPT production is intentionally excluded.  The technical live account should
    receive stricter, champion-grade specialists.  The primary live account is
    better used as a broader challenger lane for scout variants that look
    promising but need account-specific evidence before promotion.
    """
    specs: List[Dict[str, Any]] = []
    priority_models = scout_priority_model_types(model_types)

    def selected_parameter_sets(model_type: str) -> List[Dict[str, Any]]:
        return parameter_sets(model_type)[:2]

    tech_reason = (
        "live split: strict tech-champion candidate; pair-family specialist, "
        "walk-forward scout evidence required before technical production"
    )
    primary_reason = (
        "live split: primary-challenger scout candidate; broader exploration "
        "for volatile/pair-family moves before any tech promotion"
    )

    for subset in LIVE_TECH_CHAMPION_SUBSETS:
        for feature_set in PRIMARY_FEATURE_SETS:
            for model_type in priority_models:
                for params in selected_parameter_sets(model_type):
                    for horizon in [120, 60]:
                        specs.append(make_spec(
                            dataset_kind="profitable_move_precursor",
                            model_type=model_type,
                            target=f"profitable_any_move_{horizon}",
                            outcome=f"best_net_atr_{horizon}",
                            feature_set=feature_set,
                            instrument_subset=subset,
                            research_role="profitable_move_precursor",
                            direction_target=f"best_direction_up_{horizon}",
                            parameters=params,
                            validation_weeks=6,
                            max_train_rows=150_000,
                            agenda_reason=tech_reason,
                            deployment_lane="live_tech_champion_candidate",
                            account_focus="technical_live_001-001-21715580-003",
                        ))
                    for target, outcome in [
                        ("reversal_curve_profit_120", "reversal_curve_net_atr_120"),
                        ("short_curve_profit_120", "short_curve_net_atr_120"),
                        ("continuation_curve_profit_60", "continuation_curve_net_atr_60"),
                        ("long_curve_profit_60", "long_curve_net_atr_60"),
                    ]:
                        specs.append(make_spec(
                            dataset_kind="technical_spike",
                            model_type=model_type,
                            target=target,
                            outcome=outcome,
                            feature_set=feature_set,
                            instrument_subset=subset,
                            research_role="return_curve_candidate",
                            execution_policy="curve",
                            parameters=params,
                            validation_weeks=6,
                            max_train_rows=150_000,
                            agenda_reason=tech_reason,
                            deployment_lane="live_tech_champion_candidate",
                            account_focus="technical_live_001-001-21715580-003",
                        ))
                    for lead_minutes, horizon in [(30, 60), (30, 120)]:
                        for policy in SCOUT_STOPCAP_POLICIES:
                            specs.append(make_spec(
                                dataset_kind="technical_spike",
                                model_type=model_type,
                                target=f"major_event_lead_{lead_minutes}_{horizon}",
                                outcome=f"two_stage_{policy}_net_atr_{horizon}",
                                feature_set=feature_set,
                                instrument_subset=subset,
                                research_role="major_move_factor",
                                direction_target=f"major_direction_up_lead_{lead_minutes}_{horizon}",
                                execution_policy=policy,
                                parameters=params,
                                validation_weeks=6,
                                max_train_rows=150_000,
                                agenda_reason=tech_reason,
                                deployment_lane="live_tech_champion_candidate",
                                account_focus="technical_live_001-001-21715580-003",
                            ))

    for subset in LIVE_PRIMARY_CHALLENGER_SUBSETS:
        for feature_set in PRIMARY_FEATURE_SETS:
            for model_type in priority_models:
                for params in selected_parameter_sets(model_type):
                    for horizon in [60, 120]:
                        specs.append(make_spec(
                            dataset_kind="profitable_move_precursor",
                            model_type=model_type,
                            target=f"profitable_any_move_{horizon}",
                            outcome=f"best_net_atr_{horizon}",
                            feature_set=feature_set,
                            instrument_subset=subset,
                            research_role="profitable_move_precursor",
                            direction_target=f"best_direction_up_{horizon}",
                            parameters=params,
                            validation_weeks=6,
                            max_train_rows=150_000,
                            agenda_reason=primary_reason,
                            deployment_lane="live_primary_challenger_candidate",
                            account_focus="primary_live_001-001-21715580-002",
                        ))
                    for target, outcome in [
                        ("reversal_curve_profit_60", "reversal_curve_net_atr_60"),
                        ("short_curve_profit_60", "short_curve_net_atr_60"),
                        ("continuation_curve_profit_60", "continuation_curve_net_atr_60"),
                    ]:
                        specs.append(make_spec(
                            dataset_kind="technical_spike",
                            model_type=model_type,
                            target=target,
                            outcome=outcome,
                            feature_set=feature_set,
                            instrument_subset=subset,
                            research_role="return_curve_candidate",
                            execution_policy="curve",
                            parameters=params,
                            validation_weeks=6,
                            max_train_rows=150_000,
                            agenda_reason=primary_reason,
                            deployment_lane="live_primary_challenger_candidate",
                            account_focus="primary_live_001-001-21715580-002",
                        ))
                    for lead_minutes, horizon in [(15, 60), (30, 60)]:
                        for policy in SCOUT_STOPCAP_POLICIES:
                            specs.append(make_spec(
                                dataset_kind="technical_spike",
                                model_type=model_type,
                                target=f"major_event_lead_{lead_minutes}_{horizon}",
                                outcome=f"two_stage_{policy}_net_atr_{horizon}",
                                feature_set=feature_set,
                                instrument_subset=subset,
                                research_role="major_move_factor",
                                direction_target=f"major_direction_up_lead_{lead_minutes}_{horizon}",
                                execution_policy=policy,
                                parameters=params,
                                validation_weeks=6,
                                max_train_rows=150_000,
                                agenda_reason=primary_reason,
                                deployment_lane="live_primary_challenger_candidate",
                                account_focus="primary_live_001-001-21715580-002",
                            ))
    return specs


def opportunity_gate_specs(model_types: Sequence[str]) -> List[Dict[str, Any]]:
    specs: List[Dict[str, Any]] = []
    targets = [
        ("any", "best_net_atr", "best_direction_up"),
        ("long", "long_net_atr", ""),
        ("short", "short_net_atr", ""),
    ]
    for horizon in HORIZONS:
        for direction, outcome_prefix, direction_prefix in targets:
            for subset in ["volatile", "non_usd_volatile", "volatile_exotic", "all", "majors"]:
                for feature_set in PRIMARY_FEATURE_SETS:
                    for model_type in model_types:
                        for params in parameter_sets(model_type):
                            specs.append(make_spec(
                                dataset_kind="profitable_move_precursor",
                                model_type=model_type,
                                target=f"profitable_{direction}_move_{horizon}",
                                outcome=f"{outcome_prefix}_{horizon}",
                                feature_set=feature_set,
                                instrument_subset=subset,
                                research_role="profitable_move_precursor",
                                direction_target=f"{direction_prefix}_{horizon}" if direction_prefix else "",
                                parameters=params,
                                agenda_reason="large profitable move precursor/opportunity gate",
                            ))
    return specs


def technical_direction_specs(model_types: Sequence[str]) -> List[Dict[str, Any]]:
    specs: List[Dict[str, Any]] = []
    for horizon in [60, 120, 30]:
        for mode in TECHNICAL_TARGET_MODES:
            for subset in PRIMARY_SUBSETS:
                for feature_set in PRIMARY_FEATURE_SETS:
                    for model_type in model_types:
                        for params in parameter_sets(model_type):
                            specs.append(make_spec(
                                dataset_kind="technical_spike",
                                model_type=model_type,
                                target=f"{mode}_profit_{horizon}",
                                outcome=f"{mode}_net_atr_{horizon}",
                                feature_set=feature_set,
                                instrument_subset=subset,
                                research_role="standalone_candidate",
                                parameters=params,
                                agenda_reason="directional profit model for technical account candidates",
                            ))
    return specs


def technical_return_curve_specs(model_types: Sequence[str]) -> List[Dict[str, Any]]:
    """Models that learn tradable forward path quality, not only endpoint P/L."""
    specs: List[Dict[str, Any]] = []
    for horizon in [120, 60, 30]:
        for mode in TECHNICAL_TARGET_MODES:
            for subset in ["volatile", "non_usd_volatile", "volatile_exotic", "exotic", "majors", "all"]:
                for feature_set in PRIMARY_FEATURE_SETS:
                    for model_type in model_types:
                        for params in parameter_sets(model_type):
                            specs.append(make_spec(
                                dataset_kind="technical_spike",
                                model_type=model_type,
                                target=f"{mode}_curve_profit_{horizon}",
                                outcome=f"{mode}_curve_net_atr_{horizon}",
                                feature_set=feature_set,
                                instrument_subset=subset,
                                research_role="return_curve_candidate",
                                execution_policy="curve",
                                parameters=params,
                                agenda_reason=(
                                    "return-curve model: entry decisions are scored by best in-horizon "
                                    "exit opportunity, early follow-through, endpoint retention, and "
                                    "adverse excursion instead of stagnant endpoint-only horizon labels"
                                ),
                            ))
    return specs


def major_event_factor_specs(model_types: Sequence[str]) -> List[Dict[str, Any]]:
    specs: List[Dict[str, Any]] = []
    for horizon in [60, 120, 30]:
        for policy in [*SCOUT_STOPCAP_POLICIES, "trailing", "fixed"]:
            for subset in ["volatile", "non_usd_volatile", "volatile_exotic", "all"]:
                for feature_set in PRIMARY_FEATURE_SETS:
                    for model_type in model_types:
                        for params in parameter_sets(model_type)[:2]:
                            specs.append(make_spec(
                                dataset_kind="technical_spike",
                                model_type=model_type,
                                target=f"major_event_{horizon}",
                                outcome=f"two_stage_{policy}_net_atr_{horizon}",
                                feature_set=feature_set,
                                instrument_subset=subset,
                                research_role="major_move_factor",
                                direction_target=f"major_direction_up_{horizon}",
                                execution_policy=policy,
                                parameters=params,
                                agenda_reason="major spike factor search for volatile-pair scout/technical account",
                            ))
    return specs


def base_quality_specs(model_types: Sequence[str]) -> List[Dict[str, Any]]:
    specs: List[Dict[str, Any]] = []
    for model_type in model_types:
        for params in parameter_sets(model_type)[:2]:
            specs.append(make_spec(
                dataset_kind="base_trade_quality",
                model_type=model_type,
                target="would_profit_30m",
                outcome="net_vol_units_30",
                feature_set="base",
                instrument_subset="all",
                research_role="standalone_candidate",
                parameters=params,
                agenda_reason="legacy account-manager signal quality baseline",
            ))
    return specs


def build_agenda(model_types: Sequence[str]) -> List[Dict[str, Any]]:
    """Return ordered specs. Earlier specs are more relevant to current goals."""
    specs: List[Dict[str, Any]] = []
    specs.extend(live_account_split_priority_specs(model_types))
    specs.extend(production_scout_priority_specs(model_types))
    specs.extend(opportunity_gate_specs(model_types))
    specs.extend(technical_return_curve_specs(model_types))
    specs.extend(technical_direction_specs(model_types))
    specs.extend(major_event_factor_specs(model_types))
    specs.extend(base_quality_specs(model_types))
    return specs


def hash_counts(specs: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for spec in specs:
        model_type = str(spec.get("model_type") or "")
        counts[model_type] = counts.get(model_type, 0) + 1
    return counts


def seed_queue(
    *,
    max_queue: int,
    include_new_estimators: bool,
    only_model_type: Sequence[str],
    dry_run: bool,
) -> Dict[str, Any]:
    registry = ModelLifecycleRegistry(REGISTRY_ROOT, PROMOTIONS_ROOT)
    model_types = installed_model_types(include_new_estimators)
    if only_model_type:
        allowed = set(only_model_type)
        model_types = [model for model in model_types if model in allowed]
    if not model_types:
        raise SystemExit("No supported model types are available for this environment")

    specs = build_agenda(model_types)
    existing_queue_hashes = {experiment_spec_hash(spec) for spec in registry.queued_specs()}
    done_hashes = completed_hashes()
    seen_hashes = set(existing_queue_hashes) | set(done_hashes)

    seeded: List[Dict[str, Any]] = []
    skipped_duplicate = 0
    for spec in specs:
        spec_hash = experiment_spec_hash(spec)
        if spec_hash in seen_hashes:
            skipped_duplicate += 1
            continue
        seen_hashes.add(spec_hash)
        seeded.append(spec)
        if len(seeded) >= max_queue:
            break

    if not dry_run:
        for spec in seeded:
            registry.enqueue(spec, reason=str(spec.get("agenda_reason") or "model-space agenda"))

    agenda = {
        "updated_utc": utc_iso(),
        "purpose": "Structured model-space coverage for the always-on OANDA FX trainer",
        "trainer_lane": {
            "description": "Tabular walk-forward classifier validation. Only this lane writes model lifecycle candidates.",
            "model_types": model_types,
            "dataset_kinds": ["profitable_move_precursor", "technical_spike", "base_trade_quality"],
            "feature_sets": ["technical_full", "technical_core", "base"],
            "instrument_subsets": sorted(set(PRIMARY_SUBSETS + PAIR_FAMILY_SUBSETS)),
            "horizons_minutes": HORIZONS,
            "seeded_this_run": len(seeded),
            "dry_run": dry_run,
        },
        "external_baseline_lanes": EXTERNAL_BASELINE_LANES,
        "coverage_totals_if_fully_enumerated": {
            "specs": len(specs),
            "by_model_type": hash_counts(specs),
        },
        "dedupe": {
            "existing_queue_hashes": len(existing_queue_hashes),
            "completed_hashes": len(done_hashes),
            "skipped_duplicate": skipped_duplicate,
        },
        "seeded_specs": [
            {
                "spec_hash": experiment_spec_hash(spec),
                "dataset_kind": spec["dataset_kind"],
                "model_type": spec["model_type"],
                "target": spec["target"],
                "outcome": spec["outcome"],
                "feature_set": spec["feature_set"],
                "instrument_subset": spec["instrument_subset"],
                "research_role": spec["research_role"],
                "agenda_reason": spec.get("agenda_reason", ""),
            }
            for spec in seeded
        ],
    }
    if not dry_run:
        atomic_write_json(AGENDA_CONFIG_PATH, agenda)
        atomic_write_json(AGENDA_LATEST_PATH, agenda)
    return agenda


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed the always-on FX trainer with a structured model-space agenda")
    parser.add_argument("--max-queue", type=int, default=80, help="Maximum new specs to append this run")
    parser.add_argument(
        "--include-new-estimators",
        action="store_true",
        help="Queue ExtraTrees/HistGradient specs too. Use only after trainer process is restarted onto current code.",
    )
    parser.add_argument(
        "--only-model-type",
        action="append",
        default=[],
        help="Restrict seeding to one model type. Can be repeated.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Build agenda without appending to the trainer queue")
    args = parser.parse_args()

    agenda = seed_queue(
        max_queue=max(0, args.max_queue),
        include_new_estimators=bool(args.include_new_estimators),
        only_model_type=args.only_model_type,
        dry_run=bool(args.dry_run),
    )
    print(json.dumps({
        "updated_utc": agenda["updated_utc"],
        "seeded_this_run": agenda["trainer_lane"]["seeded_this_run"],
        "model_types": agenda["trainer_lane"]["model_types"],
        "external_baseline_lanes": [lane["lane"] for lane in EXTERNAL_BASELINE_LANES],
        "dry_run": agenda["trainer_lane"]["dry_run"],
        "agenda_config": str(AGENDA_CONFIG_PATH),
        "agenda_latest": str(AGENDA_LATEST_PATH),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
