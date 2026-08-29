#!/usr/bin/env python3
"""Seed weekend account-specific model-improvement research specs.

This is offline/research-only.  It does not call OANDA, inspect live managers,
place trades, close trades, or publish production manifests.  The goal is to
turn the latest validated account-lane evidence into reproducible follow-up
screens that the always-on trainer can validate.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import oanda_gpt_training_strategy_manager as manager
from oanda_model_lifecycle import (
    ModelLifecycleRegistry,
    candidate_id,
    experiment_spec_hash,
)


TRAINING_ROOT = manager.TRAINING_ROOT
REPORTS_ROOT = manager.DIRS["reports"]
RESEARCH_ROOT = manager.DIRS["research"]
LEDGER_PATH = manager.LOG_FILES["research_experiments"]
MISSED_SPIKE_SUBSET_REPORT = REPORTS_ROOT / "latest_missed_spike_subset_report.json"
MISSED_SPIKE_CAPTURE_GAP_REPORT = REPORTS_ROOT / "latest_missed_spike_capture_gap_report.json"
TRAINER_REPORTING_EXTENSIONS_PATH = REPORTS_ROOT / "latest_trainer_reporting_extensions.json"
PROMOTION_READINESS_CSV = REPORTS_ROOT / "trainer_promotion_readiness.csv"
SEGMENT_LEADERBOARD_CSV = REPORTS_ROOT / "trainer_segment_leaderboard.csv"
TECHNICAL_SCOUT_ROOT = manager.PROJECT_ROOT / "data" / "technical_scout_manager"
PRIMARY_SCOUT_AUDIT_PATH = (
    TECHNICAL_SCOUT_ROOT
    / "account_live_primary_challenger_scout"
    / "scout_audit_ledger.csv"
)
PRIMARY_EVENT_SCAN_PATH = (
    TECHNICAL_SCOUT_ROOT
    / "account_live_primary_challenger_scout"
    / "event_scan_summary.csv"
)
LOCALIZED_CLUSTER_CURRENCIES = {
    "ZAR",
    "TRY",
    "MXN",
    "NOK",
    "SEK",
    "CZK",
    "HKD",
    "THB",
    "CNH",
    "PLN",
    "HUF",
}
DAILY_MOVE_COMPARISON_ROOT = (
    manager.PROJECT_ROOT
    / "data"
    / "forex"
    / "reports"
    / "daily_move_comparison"
)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_ledger_rows() -> List[Dict[str, str]]:
    if not LEDGER_PATH.exists():
        return []
    with LEDGER_PATH.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def queued_specs(registry: ModelLifecycleRegistry) -> List[Dict[str, Any]]:
    return registry.queued_specs()


def existing_hashes(registry: ModelLifecycleRegistry) -> set[str]:
    hashes: set[str] = set()
    for spec in queued_specs(registry):
        normalized = normalized_spec(spec)
        hashes.add(experiment_spec_hash(normalized))
        hashes.add(semantic_spec_key(normalized))
    state = manager.load_json(RESEARCH_ROOT / "research_state.json", {})
    hashes.update(str(item) for item in state.get("completed_spec_hashes", []))
    for row in read_ledger_rows():
        if row.get("spec_hash"):
            hashes.add(str(row["spec_hash"]))
        semantic_key = semantic_spec_key_from_ledger_row(row)
        if semantic_key:
            hashes.add(semantic_key)
    return hashes


def normalized_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(spec)
    whitelist = manager.normalized_instrument_whitelist(
        out.get("instrument_whitelist"),
    )
    if whitelist:
        out["instrument_whitelist"] = whitelist
    else:
        out.pop("instrument_whitelist", None)
    filters = manager.normalized_segment_filters(out.get("segment_filters"))
    if filters:
        out["segment_filters"] = filters
    else:
        out.pop("segment_filters", None)
    return out


SEMANTIC_SPEC_KEY_FIELDS = [
    "source",
    "deployment_lane",
    "account_focus",
    "evaluation_stage",
    "validation_weeks",
    "max_train_rows",
    "dataset_kind",
    "model_type",
    "target",
    "outcome",
    "feature_set",
    "instrument_subset",
    "instrument_whitelist",
    "segment_filters",
    "research_role",
    "direction_target",
    "execution_policy",
    "parameters",
]


def _json_or_raw(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    raw = value.strip()
    if not raw:
        return ""
    try:
        return json.loads(raw)
    except Exception:
        return raw


def semantic_spec_key(spec: Dict[str, Any]) -> str:
    """Return a duplicate guard key for materially identical train/eval specs.

    The lifecycle hash intentionally includes ``specialist_profile`` so audit
    labels remain traceable.  For queue seeding, however, two specs that only
    differ by profile wording train the same model over the same validation
    slice, so they should not both consume trainer time.
    """
    normalized = normalized_spec(spec)
    identity = {
        key: normalized.get(key)
        for key in SEMANTIC_SPEC_KEY_FIELDS
        if key in normalized
    }
    return "semantic:" + hashlib.sha256(
        json.dumps(identity, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def semantic_spec_key_from_ledger_row(row: Dict[str, str]) -> str:
    spec: Dict[str, Any] = {}
    for key in SEMANTIC_SPEC_KEY_FIELDS:
        if key == "parameters":
            raw_value = row.get("parameters_json")
        else:
            raw_value = row.get(key)
        if raw_value in (None, ""):
            continue
        value = _json_or_raw(raw_value)
        if value in (None, ""):
            continue
        spec[key] = value
    return semantic_spec_key(spec) if spec else ""


def enqueue_unique(
    registry: ModelLifecycleRegistry,
    spec: Dict[str, Any],
    *,
    reason: str,
    seen_hashes: set[str],
) -> bool:
    spec = normalized_spec(spec)
    spec_hash = experiment_spec_hash(spec)
    semantic_key = semantic_spec_key(spec)
    if spec_hash in seen_hashes or semantic_key in seen_hashes:
        return False
    line = {
        "time_utc": utc_iso(),
        "reason": reason,
        "spec": spec,
        "candidate_id": candidate_id(spec),
        "spec_hash": spec_hash,
    }
    registry.queue_path.parent.mkdir(parents=True, exist_ok=True)
    with registry.queue_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(line, sort_keys=True, default=str) + "\n")
    seen_hashes.add(spec_hash)
    seen_hashes.add(semantic_key)
    return True


def hgb_parameter_variants() -> List[Dict[str, Any]]:
    return [
        {
            "max_iter": 160,
            "learning_rate": 0.05,
            "max_leaf_nodes": 31,
            "min_samples_leaf": 40,
            "l2_regularization": 0.01,
        },
        {
            "max_iter": 240,
            "learning_rate": 0.05,
            "max_leaf_nodes": 31,
            "min_samples_leaf": 40,
            "l2_regularization": 0.05,
        },
        {
            "max_iter": 320,
            "learning_rate": 0.035,
            "max_leaf_nodes": 31,
            "min_samples_leaf": 40,
            "l2_regularization": 0.05,
        },
        {
            "max_iter": 240,
            "learning_rate": 0.075,
            "max_leaf_nodes": 15,
            "min_samples_leaf": 80,
            "l2_regularization": 0.10,
        },
        {
            "max_iter": 360,
            "learning_rate": 0.025,
            "max_leaf_nodes": 63,
            "min_samples_leaf": 40,
            "l2_regularization": 0.05,
        },
    ]


def tree_parameter_variants(model_type: str) -> List[Dict[str, Any]]:
    if model_type == "extra_trees":
        return [
            {
                "n_estimators": 360,
                "max_depth": 12,
                "min_samples_leaf": 20,
                "max_features": 0.7,
                "class_weight": "balanced",
            },
            {
                "n_estimators": 500,
                "max_depth": None,
                "min_samples_leaf": 20,
                "max_features": "sqrt",
                "class_weight": "balanced_subsample",
            },
        ]
    return [
        {
            "n_estimators": 240,
            "max_depth": 9,
            "min_samples_leaf": 20,
            "max_features": 0.7,
            "class_weight": "balanced_subsample",
        }
    ]


def confirmation_tree_parameter_variants(model_type: str) -> List[Dict[str, Any]]:
    """Moderate tree configs for cross-family confirmation sweeps.

    These are intentionally lighter than the broad tree variants.  The goal is
    not to exhaust every forest depth overnight; it is to see whether an edge
    survives a different learner family without starving the queue.
    """
    if model_type == "extra_trees":
        return [
            {
                "n_estimators": 220,
                "max_depth": 10,
                "min_samples_leaf": 25,
                "max_features": "sqrt",
                "class_weight": "balanced_subsample",
            }
        ]
    return [
        {
            "n_estimators": 180,
            "max_depth": 8,
            "min_samples_leaf": 25,
            "max_features": "sqrt",
            "class_weight": "balanced_subsample",
        }
    ]


def calibration_model_parameter_variants(model_type: str) -> List[Dict[str, Any]]:
    """Compact probability-quality variants for candidates failing Brier gates."""
    if model_type == "hist_gradient_boosting":
        return [
            {
                "max_iter": 220,
                "learning_rate": 0.035,
                "max_leaf_nodes": 15,
                "min_samples_leaf": 60,
                "l2_regularization": 0.10,
            },
            {
                "max_iter": 280,
                "learning_rate": 0.025,
                "max_leaf_nodes": 31,
                "min_samples_leaf": 80,
                "l2_regularization": 0.20,
            },
        ]
    if model_type == "gradient_boosting":
        return [
            {
                "n_estimators": 120,
                "learning_rate": 0.035,
                "max_depth": 2,
                "min_samples_leaf": 40,
                "subsample": 0.80,
            },
            {
                "n_estimators": 180,
                "learning_rate": 0.025,
                "max_depth": 2,
                "min_samples_leaf": 60,
                "subsample": 0.70,
            },
        ]
    return []


def gpt_support_tree_parameter_variants(model_type: str) -> List[Dict[str, Any]]:
    """Faster tree configs for broad GPT-support sweeps.

    GPT-support precursor sweeps cover larger pair families than the account
    specialist baskets.  Lightweight trees get more hypotheses screened during
    weekend research; heavy variants can still run later if the light pass has
    evidence.
    """
    if model_type == "extra_trees":
        return [
            {
                "n_estimators": 160,
                "max_depth": 9,
                "min_samples_leaf": 30,
                "max_features": "sqrt",
                "class_weight": "balanced_subsample",
            }
        ]
    return [
        {
            "n_estimators": 140,
            "max_depth": 8,
            "min_samples_leaf": 30,
            "max_features": "sqrt",
            "class_weight": "balanced_subsample",
        }
    ]


def technical_curve_spec(
    *,
    source: str,
    deployment_lane: str,
    account_focus: str,
    model_type: str,
    target: str,
    outcome: str,
    instrument_subset: str,
    instrument_whitelist: Iterable[str],
    segment_filters: Dict[str, Any],
    specialist_profile: str,
    specialist_parent_experiment_id: str,
    parameters: Dict[str, Any],
    feature_set: str = "technical_full",
    validation_weeks: int = 6,
    max_train_rows: int = 180_000,
    evaluation_stage: str = "screen",
    validation_profile: str = "",
) -> Dict[str, Any]:
    spec = {
        "source": source,
        "deployment_lane": deployment_lane,
        "account_focus": account_focus,
        "evaluation_stage": evaluation_stage,
        "validation_weeks": validation_weeks,
        "max_train_rows": max_train_rows,
        "dataset_kind": "technical_spike",
        "model_type": model_type,
        "target": target,
        "outcome": outcome,
        "feature_set": feature_set,
        "instrument_subset": instrument_subset,
        "instrument_whitelist": list(instrument_whitelist),
        "segment_filters": segment_filters,
        "research_role": "return_curve_candidate",
        "direction_target": "",
        "execution_policy": "curve",
        "specialist_profile": specialist_profile,
        "specialist_parent_experiment_id": specialist_parent_experiment_id,
        "parameters": parameters,
    }
    if validation_profile:
        spec["validation_profile"] = validation_profile
    return spec


def precursor_spec(
    *,
    source: str,
    deployment_lane: str,
    account_focus: str,
    model_type: str,
    target: str,
    outcome: str,
    direction_target: str,
    instrument_subset: str,
    segment_filters: Dict[str, Any],
    specialist_profile: str,
    parameters: Dict[str, Any],
    validation_weeks: int = 6,
    max_train_rows: int = 180_000,
    evaluation_stage: str = "screen",
    validation_profile: str = "",
) -> Dict[str, Any]:
    spec = {
        "source": source,
        "deployment_lane": deployment_lane,
        "account_focus": account_focus,
        "evaluation_stage": evaluation_stage,
        "validation_weeks": validation_weeks,
        "max_train_rows": max_train_rows,
        "dataset_kind": "profitable_move_precursor",
        "model_type": model_type,
        "target": target,
        "outcome": outcome,
        "feature_set": "technical_full",
        "instrument_subset": instrument_subset,
        "segment_filters": segment_filters,
        "research_role": "profitable_move_precursor",
        "direction_target": direction_target,
        "execution_policy": "",
        "specialist_profile": specialist_profile,
        "parameters": parameters,
    }
    if validation_profile:
        spec["validation_profile"] = validation_profile
    return spec


def major_lead_spec(
    *,
    source: str,
    deployment_lane: str,
    account_focus: str,
    model_type: str,
    lead_minutes: int,
    horizon_minutes: int,
    instrument_subset: str,
    segment_filters: Dict[str, Any],
    specialist_profile: str,
    parameters: Dict[str, Any],
    instrument_whitelist: Iterable[str] = (),
    validation_weeks: int = 8,
    max_train_rows: int = 220_000,
    execution_policy: str = "trailing_stop075",
    evaluation_stage: str = "validation",
    validation_profile: str = "scout_value_lead_validation_v1",
) -> Dict[str, Any]:
    spec = {
        "source": source,
        "deployment_lane": deployment_lane,
        "account_focus": account_focus,
        "evaluation_stage": evaluation_stage,
        "validation_weeks": validation_weeks,
        "max_train_rows": max_train_rows,
        "dataset_kind": "technical_spike",
        "model_type": model_type,
        "target": f"major_event_lead_{int(lead_minutes)}_{int(horizon_minutes)}",
        "outcome": f"two_stage_{execution_policy}_net_atr_{int(horizon_minutes)}",
        "feature_set": "technical_full",
        "instrument_subset": instrument_subset,
        "instrument_whitelist": list(instrument_whitelist),
        "segment_filters": segment_filters,
        "research_role": "major_move_factor",
        "direction_target": (
            f"major_direction_up_lead_{int(lead_minutes)}_{int(horizon_minutes)}"
        ),
        "execution_policy": execution_policy,
        "specialist_profile": specialist_profile,
        "parameters": parameters,
        "validation_profile": validation_profile,
    }
    return spec


def seed_primary_variants(
    registry: ModelLifecycleRegistry,
    seen: set[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    source = "operator_weekend_primary_improvement_v1"
    lane = "live_primary_challenger_candidate"
    account_focus = "primary_live_001-001-21715580-002"
    parent = "exp_20260627_214432_52fd65d317"
    full_basket = ["SGD_JPY", "AUD_JPY", "GBP_JPY", "EUR_JPY", "CAD_JPY"]
    top3_basket = ["SGD_JPY", "AUD_JPY", "GBP_JPY"]
    filter_variants = [
        (
            "spread_offny",
            {
                "regimes": ["spread_impaired"],
                "sessions": ["off_session", "new_york"],
            },
        ),
        ("spread_only", {"regimes": ["spread_impaired"]}),
        (
            "offny_only",
            {"sessions": ["off_session", "new_york"]},
        ),
        (
            "spread_offny_asia_guard",
            {
                "regimes": ["spread_impaired"],
                "sessions": ["off_session", "new_york", "asia"],
                "exclude_sessions": ["rollover"],
            },
        ),
    ]
    targets = [
        ("reversal_curve_profit_60", "reversal_curve_net_atr_60"),
        ("reversal_curve_profit_120", "reversal_curve_net_atr_120"),
    ]
    seeded: List[Dict[str, Any]] = []
    count = 0
    for basket_name, basket in [
        ("top3", top3_basket),
        ("full5", full_basket),
    ]:
        for filter_name, filters in filter_variants:
            for target, outcome in targets:
                for index, params in enumerate(hgb_parameter_variants(), 1):
                    spec = technical_curve_spec(
                        source=source,
                        deployment_lane=lane,
                        account_focus=account_focus,
                        model_type="hist_gradient_boosting",
                        target=target,
                        outcome=outcome,
                        instrument_subset="jpy_risk",
                        instrument_whitelist=basket,
                        segment_filters=filters,
                        specialist_profile=(
                            f"primary_jpy_{basket_name}_{filter_name}_hgb_v{index}"
                        ),
                        specialist_parent_experiment_id=parent,
                        parameters=params,
                        validation_weeks=6,
                        max_train_rows=180_000,
                    )
                    if enqueue_unique(
                        registry,
                        spec,
                        reason="weekend_primary_hgb_scorecard_refinement",
                        seen_hashes=seen,
                    ):
                        count += 1
                        seeded.append(spec)
    # Keep a small tree comparison around the best HGB lane without letting it
    # dominate the weekend queue.
    for model_type in ["extra_trees", "random_forest"]:
        for params in tree_parameter_variants(model_type):
            spec = technical_curve_spec(
                source=source,
                deployment_lane=lane,
                account_focus=account_focus,
                model_type=model_type,
                target="reversal_curve_profit_60",
                outcome="reversal_curve_net_atr_60",
                instrument_subset="jpy_risk",
                instrument_whitelist=top3_basket,
                segment_filters={
                    "regimes": ["spread_impaired"],
                    "sessions": ["off_session", "new_york"],
                },
                specialist_profile=f"primary_jpy_top3_spread_offny_{model_type}",
                specialist_parent_experiment_id=parent,
                parameters=params,
                validation_weeks=6,
                max_train_rows=160_000,
            )
            if enqueue_unique(
                registry,
                spec,
                reason="weekend_primary_tree_scorecard_comparison",
                seen_hashes=seen,
            ):
                count += 1
                seeded.append(spec)
    return count, seeded


def seed_tech_variants(
    registry: ModelLifecycleRegistry,
    seen: set[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    source = "operator_weekend_tech_improvement_v1"
    lane = "live_tech_champion_candidate"
    account_focus = "tech_live_001-001-21715580-003"
    parent = "exp_20260627_221846_5d66ec165a"
    full_basket = ["EUR_CZK", "EUR_SEK", "USD_DKK", "EUR_HUF", "USD_HUF"]
    top_total_basket = ["EUR_CZK", "EUR_SEK", "USD_DKK"]
    top_mean_basket = ["EUR_CZK", "EUR_HUF", "EUR_SEK"]
    filter_variants = [
        (
            "highvol_spread",
            {"regimes": ["high_vol_event", "spread_impaired"]},
        ),
        ("spread_only", {"regimes": ["spread_impaired"]}),
        (
            "highvol_spread_nonrollover",
            {
                "regimes": ["high_vol_event", "spread_impaired"],
                "sessions": [
                    "asia",
                    "london",
                    "new_york",
                    "london_ny_overlap",
                    "off_session",
                ],
                "exclude_sessions": ["rollover"],
            },
        ),
    ]
    targets = [
        ("short_curve_profit_120", "short_curve_net_atr_120"),
        ("short_curve_profit_60", "short_curve_net_atr_60"),
    ]
    seeded: List[Dict[str, Any]] = []
    count = 0
    for basket_name, basket in [
        ("top_total3", top_total_basket),
        ("top_mean3", top_mean_basket),
        ("full5", full_basket),
    ]:
        for filter_name, filters in filter_variants:
            for target, outcome in targets:
                for index, params in enumerate(hgb_parameter_variants(), 1):
                    spec = technical_curve_spec(
                        source=source,
                        deployment_lane=lane,
                        account_focus=account_focus,
                        model_type="hist_gradient_boosting",
                        target=target,
                        outcome=outcome,
                        instrument_subset="exotic_high_spread",
                        instrument_whitelist=basket,
                        segment_filters=filters,
                        specialist_profile=(
                            f"tech_short_{basket_name}_{filter_name}_hgb_v{index}"
                        ),
                        specialist_parent_experiment_id=parent,
                        parameters=params,
                        validation_weeks=6,
                        max_train_rows=180_000,
                    )
                    if enqueue_unique(
                        registry,
                        spec,
                        reason="weekend_tech_short_scorecard_refinement",
                        seen_hashes=seen,
                    ):
                        count += 1
                        seeded.append(spec)
    # Keep a small long-side challenger set because the existing long specialists
    # were positive but materially weaker than the short book.
    long_basket = ["USD_HUF", "USD_DKK", "EUR_SEK", "EUR_CZK", "GBP_PLN"]
    for target, outcome in [
        ("long_curve_profit_60", "long_curve_net_atr_60"),
        ("long_curve_profit_120", "long_curve_net_atr_120"),
    ]:
        for index, params in enumerate(hgb_parameter_variants()[:3], 1):
            spec = technical_curve_spec(
                source=source,
                deployment_lane=lane,
                account_focus=account_focus,
                model_type="hist_gradient_boosting",
                target=target,
                outcome=outcome,
                instrument_subset="exotic_high_spread",
                instrument_whitelist=long_basket,
                segment_filters={
                    "regimes": ["high_vol_event", "spread_impaired"],
                    "exclude_sessions": ["rollover"],
                },
                specialist_profile=f"tech_long_highvol_spread_hgb_v{index}",
                specialist_parent_experiment_id="exp_20260627_222941_61a46e50ac",
                parameters=params,
                validation_weeks=6,
                max_train_rows=180_000,
            )
            if enqueue_unique(
                registry,
                spec,
                reason="weekend_tech_long_challenger_refinement",
                seen_hashes=seen,
            ):
                count += 1
                seeded.append(spec)
    return count, seeded


def seed_scout_value_capture_variants(
    registry: ModelLifecycleRegistry,
    seen: set[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    """Seed focused scout lead-event validations using the new value features."""
    seeded: List[Dict[str, Any]] = []
    count = 0
    lanes = [
        (
            "operator_scout_value_capture_v1",
            "live_primary_challenger_candidate",
            "primary_live_001-001-21715580-002",
            "primary_scout_value",
        ),
        (
            "operator_scout_value_capture_v1",
            "live_tech_champion_candidate",
            "tech_live_001-001-21715580-003",
            "tech_scout_value",
        ),
    ]
    filter_variants = [
        (
            "highvol_spread_nonrollover",
            {
                "regimes": ["high_vol_event", "spread_impaired"],
                "sessions": ["asia", "london", "new_york", "off_session"],
                "exclude_sessions": ["rollover"],
            },
        ),
        (
            "highvol_spread_asia_london_off",
            {
                "regimes": ["high_vol_event", "spread_impaired"],
                "sessions": ["asia", "london", "off_session"],
            },
        ),
        ("highvol_only_nonrollover", {
            "regimes": ["high_vol_event"],
            "sessions": ["asia", "london", "new_york", "off_session"],
            "exclude_sessions": ["rollover"],
        }),
    ]
    for source, lane, focus, prefix in lanes:
        for subset in ["volatile_exotic", "exotic_high_spread", "volatile"]:
            for filter_name, filters in filter_variants:
                for lead in [15, 30]:
                    for execution_policy in [
                        "trailing_stop010",
                        "trailing_stop012",
                        "trailing_stop015",
                        "trailing_stop020",
                        "trailing_stop025",
                        "trailing_stop035",
                        "trailing_stop050",
                        "trailing_stop075",
                    ]:
                        for model_type in ["random_forest", "extra_trees"]:
                            for params in confirmation_tree_parameter_variants(model_type):
                                spec = major_lead_spec(
                                    source=source,
                                    deployment_lane=lane,
                                    account_focus=focus,
                                    model_type=model_type,
                                    lead_minutes=lead,
                                    horizon_minutes=120,
                                    instrument_subset=subset,
                                    segment_filters=filters,
                                    specialist_profile=(
                                        f"{prefix}_{subset}_{filter_name}_lead{lead}_"
                                        f"event120_{execution_policy}_{model_type}"
                                    ),
                                    parameters=params,
                                    validation_weeks=8,
                                    max_train_rows=220_000,
                                    execution_policy=execution_policy,
                                )
                                if enqueue_unique(
                                    registry,
                                    spec,
                                    reason="scout_value_capture_lead_event_validation",
                                    seen_hashes=seen,
                                ):
                                    count += 1
                                    seeded.append(spec)
    return count, seeded


def seed_gpt_support_variants(
    registry: ModelLifecycleRegistry,
    seen: set[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    source = "operator_weekend_gpt_support_v1"
    lane = "gpt_live_model_support"
    account_focus = "gpt_live_001-001-21715580-001"
    filter_variants = [
        (
            "highvol_spread_asia_london_off",
            {
                "regimes": ["high_vol_event", "spread_impaired"],
                "sessions": ["asia", "london", "off_session"],
            },
        ),
        (
            "highvol_spread_all_sessions",
            {"regimes": ["high_vol_event", "spread_impaired"]},
        ),
        ("highvol_only", {"regimes": ["high_vol_event"]}),
        (
            "liquid_sessions_only",
            {"sessions": ["asia", "london", "new_york", "off_session"]},
        ),
        ("unfiltered", {}),
    ]
    seeded: List[Dict[str, Any]] = []
    count = 0
    for subset in [
        "volatile",
        "volatile_exotic",
        "exotic_high_spread",
        "volatile_non_usd",
    ]:
        for target, outcome, direction_target in [
            ("profitable_any_move_60", "best_net_atr_60", "best_direction_up_60"),
            ("profitable_any_move_120", "best_net_atr_120", "best_direction_up_120"),
            ("profitable_long_move_60", "long_net_atr_60", ""),
            ("profitable_short_move_60", "short_net_atr_60", ""),
        ]:
            for filter_name, filters in filter_variants:
                for model_type, params in [
                    ("hist_gradient_boosting", hgb_parameter_variants()[1]),
                    ("extra_trees", gpt_support_tree_parameter_variants("extra_trees")[0]),
                    ("random_forest", gpt_support_tree_parameter_variants("random_forest")[0]),
                ]:
                    spec = precursor_spec(
                        source=source,
                        deployment_lane=lane,
                        account_focus=account_focus,
                        model_type=model_type,
                        target=target,
                        outcome=outcome,
                        direction_target=direction_target,
                        instrument_subset=subset,
                        segment_filters=filters,
                        specialist_profile=(
                            f"gpt_support_{subset}_{target}_{filter_name}_{model_type}"
                        ),
                        parameters=params,
                        validation_weeks=6,
                        max_train_rows=180_000,
                    )
                    if enqueue_unique(
                        registry,
                        spec,
                        reason="weekend_gpt_support_precursor_refinement",
                        seen_hashes=seen,
                    ):
                        count += 1
                        seeded.append(spec)
    return count, seeded


def seed_targeted_confirmation_variants(
    registry: ModelLifecycleRegistry,
    seen: set[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    """Seed compact follow-ups around the best current account-lane evidence."""
    seeded: List[Dict[str, Any]] = []
    count = 0

    primary_source = "operator_weekend_primary_improvement_v1"
    primary_lane = "live_primary_challenger_candidate"
    primary_focus = "primary_live_001-001-21715580-002"
    primary_parent = "exp_20260628_003125_d43f13a892"
    primary_baskets = [
        ("top3", ["SGD_JPY", "AUD_JPY", "GBP_JPY"]),
        ("full5", ["SGD_JPY", "AUD_JPY", "GBP_JPY", "EUR_JPY", "CAD_JPY"]),
    ]
    primary_filters = [
        (
            "spread_offny",
            {
                "regimes": ["spread_impaired"],
                "sessions": ["off_session", "new_york"],
            },
        ),
        (
            "spread_offny_asia_guard",
            {
                "regimes": ["spread_impaired"],
                "sessions": ["off_session", "new_york", "asia"],
                "exclude_sessions": ["rollover"],
            },
        ),
    ]
    for basket_name, basket in primary_baskets:
        for filter_name, filters in primary_filters:
            for target, outcome in [
                ("reversal_curve_profit_60", "reversal_curve_net_atr_60"),
                ("reversal_curve_profit_120", "reversal_curve_net_atr_120"),
            ]:
                for model_type in ["extra_trees", "random_forest"]:
                    for params in confirmation_tree_parameter_variants(model_type):
                        spec = technical_curve_spec(
                            source=primary_source,
                            deployment_lane=primary_lane,
                            account_focus=primary_focus,
                            model_type=model_type,
                            target=target,
                            outcome=outcome,
                            instrument_subset="jpy_risk",
                            instrument_whitelist=basket,
                            segment_filters=filters,
                            specialist_profile=(
                                "primary_confirm_"
                                f"{basket_name}_{filter_name}_{target}_{model_type}"
                            ),
                            specialist_parent_experiment_id=primary_parent,
                            parameters=params,
                            validation_weeks=8,
                            max_train_rows=170_000,
                        )
                        if enqueue_unique(
                            registry,
                            spec,
                            reason="weekend_primary_cross_family_confirmation",
                            seen_hashes=seen,
                        ):
                            count += 1
                            seeded.append(spec)

    tech_source = "operator_weekend_tech_improvement_v1"
    tech_lane = "live_tech_champion_candidate"
    tech_focus = "tech_live_001-001-21715580-003"
    tech_parent = "exp_20260627_221846_5d66ec165a"
    tech_baskets = [
        ("top_total3", ["EUR_CZK", "EUR_SEK", "USD_DKK"]),
        ("full5", ["EUR_CZK", "EUR_SEK", "USD_DKK", "EUR_HUF", "USD_HUF"]),
    ]
    tech_filters = [
        ("highvol_spread", {"regimes": ["high_vol_event", "spread_impaired"]}),
        (
            "highvol_spread_nonrollover",
            {
                "regimes": ["high_vol_event", "spread_impaired"],
                "sessions": [
                    "asia",
                    "london",
                    "new_york",
                    "london_ny_overlap",
                    "off_session",
                ],
                "exclude_sessions": ["rollover"],
            },
        ),
    ]
    for basket_name, basket in tech_baskets:
        for filter_name, filters in tech_filters:
            for target, outcome in [
                ("short_curve_profit_120", "short_curve_net_atr_120"),
                ("short_curve_profit_60", "short_curve_net_atr_60"),
            ]:
                for model_type in ["extra_trees", "random_forest"]:
                    for params in confirmation_tree_parameter_variants(model_type):
                        spec = technical_curve_spec(
                            source=tech_source,
                            deployment_lane=tech_lane,
                            account_focus=tech_focus,
                            model_type=model_type,
                            target=target,
                            outcome=outcome,
                            instrument_subset="exotic_high_spread",
                            instrument_whitelist=basket,
                            segment_filters=filters,
                            specialist_profile=(
                                "tech_confirm_"
                                f"{basket_name}_{filter_name}_{target}_{model_type}"
                            ),
                            specialist_parent_experiment_id=tech_parent,
                            parameters=params,
                            validation_weeks=8,
                            max_train_rows=170_000,
                        )
                        if enqueue_unique(
                            registry,
                            spec,
                            reason="weekend_tech_cross_family_concentration_check",
                            seen_hashes=seen,
                        ):
                            count += 1
                            seeded.append(spec)

    gpt_source = "operator_weekend_gpt_support_v1"
    gpt_lane = "gpt_live_model_support"
    gpt_focus = "gpt_live_001-001-21715580-001"
    gpt_filters = [
        ("highvol_spread_all", {"regimes": ["high_vol_event", "spread_impaired"]}),
        (
            "highvol_spread_asia_london_off",
            {
                "regimes": ["high_vol_event", "spread_impaired"],
                "sessions": ["asia", "london", "off_session"],
            },
        ),
    ]
    for subset in ["volatile", "volatile_non_usd"]:
        for filter_name, filters in gpt_filters:
            for model_type in ["extra_trees", "random_forest"]:
                for params in gpt_support_tree_parameter_variants(model_type):
                    spec = precursor_spec(
                        source=gpt_source,
                        deployment_lane=gpt_lane,
                        account_focus=gpt_focus,
                        model_type=model_type,
                        target="profitable_any_move_60",
                        outcome="best_net_atr_60",
                        direction_target="best_direction_up_60",
                        instrument_subset=subset,
                        segment_filters=filters,
                        specialist_profile=(
                            "gpt_support_confirm_"
                            f"{subset}_{filter_name}_{model_type}_12w"
                        ),
                        parameters=params,
                        validation_weeks=12,
                        max_train_rows=220_000,
                        evaluation_stage="validation",
                        validation_profile="account_specific_extended_12w_v1",
                    )
                    if enqueue_unique(
                        registry,
                        spec,
                        reason="weekend_gpt_support_extended_confirmation",
                        seen_hashes=seen,
                    ):
                        count += 1
                        seeded.append(spec)

    # Transfer the cleanest GPT-support precursor structure into the primary
    # and tech research lanes.  This is a candidate discovery bridge only: no
    # production routing or promotion is changed here.
    transfer_filters = [
        ("highvol_spread_all", {"regimes": ["high_vol_event", "spread_impaired"]}),
        (
            "highvol_spread_asia_london_off",
            {
                "regimes": ["high_vol_event", "spread_impaired"],
                "sessions": ["asia", "london", "off_session"],
            },
        ),
    ]
    transfer_lanes = [
        (
            "operator_weekend_primary_improvement_v1",
            "live_primary_challenger_candidate",
            "primary_live_001-001-21715580-002",
            "primary_transfer",
        ),
        (
            "operator_weekend_tech_improvement_v1",
            "live_tech_champion_candidate",
            "tech_live_001-001-21715580-003",
            "tech_transfer",
        ),
    ]
    for source, lane, focus, profile_prefix in transfer_lanes:
        for subset in ["volatile", "volatile_exotic"]:
            for filter_name, filters in transfer_filters:
                for model_type in ["extra_trees", "random_forest"]:
                    for params in gpt_support_tree_parameter_variants(model_type):
                        spec = precursor_spec(
                            source=source,
                            deployment_lane=lane,
                            account_focus=focus,
                            model_type=model_type,
                            target="profitable_any_move_60",
                            outcome="best_net_atr_60",
                            direction_target="best_direction_up_60",
                            instrument_subset=subset,
                            segment_filters=filters,
                            specialist_profile=(
                                f"{profile_prefix}_{subset}_{filter_name}_{model_type}_12w"
                            ),
                            parameters=params,
                            validation_weeks=12,
                            max_train_rows=220_000,
                            evaluation_stage="validation",
                            validation_profile="gpt_precursor_transfer_12w_v1",
                        )
                        if enqueue_unique(
                            registry,
                            spec,
                            reason="weekend_gpt_precursor_transfer_to_account_lanes",
                            seen_hashes=seen,
                        ):
                            count += 1
                            seeded.append(spec)

    transfer_120_lanes = [
        (
            "operator_weekend_gpt_support_v1",
            "gpt_live_model_support",
            "gpt_live_001-001-21715580-001",
            "gpt_support_transfer120",
        ),
        *transfer_lanes,
    ]
    for source, lane, focus, profile_prefix in transfer_120_lanes:
        for filter_name, filters in transfer_filters:
            for model_type in ["extra_trees", "random_forest"]:
                for params in gpt_support_tree_parameter_variants(model_type):
                    spec = precursor_spec(
                        source=source,
                        deployment_lane=lane,
                        account_focus=focus,
                        model_type=model_type,
                        target="profitable_any_move_120",
                        outcome="best_net_atr_120",
                        direction_target="best_direction_up_120",
                        instrument_subset="volatile",
                        segment_filters=filters,
                        specialist_profile=(
                            f"{profile_prefix}_volatile_{filter_name}_{model_type}_12w"
                        ),
                        parameters=params,
                        validation_weeks=12,
                        max_train_rows=220_000,
                        evaluation_stage="validation",
                        validation_profile="gpt_precursor_transfer_120_12w_v1",
                    )
                    if enqueue_unique(
                        registry,
                        spec,
                        reason="weekend_gpt_precursor_120m_transfer_to_account_lanes",
                        seen_hashes=seen,
                    ):
                        count += 1
                        seeded.append(spec)

    return count, seeded


def latest_daily_move_dir() -> Path | None:
    if not DAILY_MOVE_COMPARISON_ROOT.exists():
        return None
    dated = [
        path
        for path in DAILY_MOVE_COMPARISON_ROOT.iterdir()
        if path.is_dir()
        and len(path.name) == 10
        and path.name[4] == "-"
        and path.name[7] == "-"
    ]
    return max(dated, key=lambda path: path.name) if dated else None


def missed_detail_value_usd(detail: Any) -> float | None:
    text = str(detail or "")
    if not text:
        return None
    match = re.search(r"(?:value=|expected\s*)\$(-?\d+(?:\.\d+)?)", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"\$(-?\d+(?:\.\d+)?)", text)
    if not match:
        return None
    try:
        return float(match.group(1))
    except Exception:
        return None


def current_value_miss_baskets(limit: int = 3) -> Tuple[List[str], List[Tuple[str, float]]]:
    """Return current top missed-value instruments from the daily advisor report.

    This is only used to seed offline/research specs. It does not read live
    broker state or change any account behavior.
    """
    day_dir = latest_daily_move_dir()
    if not day_dir:
        return [], []
    missed_path = day_dir / "missed_moves.csv"
    if not missed_path.exists():
        return [], []
    by_instrument: Dict[str, float] = {}
    seen_rows: set[Tuple[str, str, str, str, str]] = set()
    with missed_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            instrument = (
                str(row.get("instrument") or "")
                .strip()
                .upper()
                .replace("/", "_")
                .replace("-", "_")
            )
            if not instrument:
                continue
            key = (
                str(row.get("time_ny") or "")[:19],
                instrument,
                str(row.get("direction") or "").strip().upper(),
                str(row.get("miss_reason") or "").strip().lower(),
                str(row.get("detail") or "").strip(),
            )
            if key in seen_rows:
                continue
            seen_rows.add(key)
            value = missed_detail_value_usd(row.get("detail"))
            if value is None or value <= 0.0:
                continue
            by_instrument[instrument] = by_instrument.get(instrument, 0.0) + value
    ranked = sorted(by_instrument.items(), key=lambda item: (-item[1], item[0]))
    top = [instrument for instrument, _ in ranked[:limit]]
    return top, ranked[:limit]


def missed_detail_value_gate_pair(detail: Any) -> Tuple[float, float] | None:
    text = str(detail or "")
    if not text:
        return None
    match = re.search(
        r"value\s+gate:\s*expected\s*\$(-?\d+(?:\.\d+)?)\s*<\s*\$(-?\d+(?:\.\d+)?)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    try:
        return float(match.group(1)), float(match.group(2))
    except Exception:
        return None


def missed_detail_move_spread_pair(detail: Any) -> Tuple[float, float] | None:
    text = str(detail or "")
    if not text:
        return None
    match = re.search(
        r"move/spread\s+(-?\d+(?:\.\d+)?)\s*<\s*(-?\d+(?:\.\d+)?)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    try:
        return float(match.group(1)), float(match.group(2))
    except Exception:
        return None


def current_near_value_gate_baskets(
    *,
    limit: int = 3,
    min_threshold_ratio: float = 0.50,
) -> Tuple[List[str], List[Tuple[str, float, int, float]]]:
    """Return instruments that repeatedly missed just below the value gate.

    This is distinct from the broader value-miss basket.  A pair can have low
    total missed dollars but consistently sit just below the configured live
    minimum expected value.  These are research-only candidates for testing
    whether the threshold/model gate should ever be adjusted.
    """
    day_dir = latest_daily_move_dir()
    if not day_dir:
        return [], []
    missed_path = day_dir / "missed_moves.csv"
    if not missed_path.exists():
        return [], []
    by_instrument: Dict[str, Dict[str, float]] = {}
    seen_rows: set[Tuple[str, str, str, str, str]] = set()
    with missed_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            instrument = (
                str(row.get("instrument") or "")
                .strip()
                .upper()
                .replace("/", "_")
                .replace("-", "_")
            )
            if not instrument:
                continue
            detail = str(row.get("detail") or "")
            parsed = missed_detail_value_gate_pair(detail)
            if not parsed:
                continue
            expected, threshold = parsed
            if threshold <= 0.0 or expected <= 0.0:
                continue
            ratio = expected / threshold
            if ratio < min_threshold_ratio:
                continue
            key = (
                str(row.get("time_ny") or "")[:19],
                instrument,
                str(row.get("direction") or "").strip().upper(),
                str(row.get("miss_reason") or "").strip().lower(),
                detail.strip(),
            )
            if key in seen_rows:
                continue
            seen_rows.add(key)
            stats = by_instrument.setdefault(
                instrument,
                {"expected": 0.0, "count": 0.0, "max_ratio": 0.0},
            )
            stats["expected"] += expected
            stats["count"] += 1.0
            stats["max_ratio"] = max(stats["max_ratio"], ratio)
    ranked = sorted(
        (
            (
                instrument,
                float(stats.get("expected") or 0.0),
                int(stats.get("count") or 0),
                float(stats.get("max_ratio") or 0.0),
            )
            for instrument, stats in by_instrument.items()
        ),
        key=lambda item: (-item[1], -item[2], -item[3], item[0]),
    )[:limit]
    return [instrument for instrument, _expected, _count, _ratio in ranked], ranked


def current_unknown_profile_miss_baskets(
    *,
    count_limit: int = 4,
    near_limit: int = 4,
    min_threshold_ratio: float = 0.70,
) -> Tuple[List[Tuple[str, List[str]]], List[Tuple[str, int, int, float, float, float]]]:
    """Return compact research baskets for unknown-profile scout skips.

    Unknown-profile misses mean the live scout saw a move but only had the
    generic move/spread gate available.  This seeder does not loosen that gate;
    it asks the offline trainer whether repeated/near-threshold instruments
    have enough reusable lead signal to justify a future profile.
    """
    day_dir = latest_daily_move_dir()
    if not day_dir:
        return [], []
    missed_path = day_dir / "missed_moves.csv"
    if not missed_path.exists():
        return [], []

    by_instrument: Dict[str, Dict[str, float]] = {}
    seen_rows: set[Tuple[str, str, str, str, str]] = set()
    with missed_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            detail = str(row.get("detail") or "")
            if "unknown-profile" not in detail.lower():
                continue
            parsed = missed_detail_move_spread_pair(detail)
            if not parsed:
                continue
            ratio, threshold = parsed
            if threshold <= 0.0:
                continue
            instrument = (
                str(row.get("instrument") or "")
                .strip()
                .upper()
                .replace("/", "_")
                .replace("-", "_")
            )
            if not instrument:
                continue
            key = (
                str(row.get("time_ny") or "")[:19],
                instrument,
                str(row.get("direction") or "").strip().upper(),
                str(row.get("miss_reason") or "").strip().lower(),
                detail.strip(),
            )
            if key in seen_rows:
                continue
            seen_rows.add(key)
            threshold_ratio = ratio / threshold
            stats = by_instrument.setdefault(
                instrument,
                {
                    "rows": 0.0,
                    "near_rows": 0.0,
                    "ratio_sum": 0.0,
                    "threshold_ratio_sum": 0.0,
                    "max_ratio": 0.0,
                    "max_threshold_ratio": 0.0,
                },
            )
            stats["rows"] += 1.0
            stats["ratio_sum"] += ratio
            stats["threshold_ratio_sum"] += threshold_ratio
            stats["max_ratio"] = max(stats["max_ratio"], ratio)
            stats["max_threshold_ratio"] = max(stats["max_threshold_ratio"], threshold_ratio)
            if threshold_ratio >= min_threshold_ratio:
                stats["near_rows"] += 1.0

    ranked: List[Tuple[str, int, int, float, float, float]] = []
    for instrument, stats in by_instrument.items():
        rows = int(stats.get("rows") or 0)
        if rows <= 0:
            continue
        near_rows = int(stats.get("near_rows") or 0)
        avg_ratio = float(stats.get("ratio_sum") or 0.0) / rows
        avg_threshold_ratio = float(stats.get("threshold_ratio_sum") or 0.0) / rows
        max_ratio = float(stats.get("max_ratio") or 0.0)
        # Count keeps repeated misses visible; near-threshold rows pull forward
        # instruments that may only need a pair-specific profile.
        score = rows * (0.5 + avg_threshold_ratio) + near_rows * 2.0
        ranked.append((instrument, rows, near_rows, avg_ratio, max_ratio, score))
    ranked.sort(key=lambda item: (-item[5], -item[2], -item[1], item[0]))

    count_ranked = sorted(ranked, key=lambda item: (-item[1], -item[5], item[0]))[:count_limit]
    near_ranked = [row for row in ranked if row[2] > 0]
    near_ranked.sort(key=lambda item: (-item[2], -item[5], item[0]))
    near_ranked = near_ranked[:near_limit]

    baskets: List[Tuple[str, List[str]]] = []
    basket_keys: set[Tuple[str, ...]] = set()

    def add_basket(name: str, instruments: Iterable[str]) -> None:
        basket = [
            str(instrument or "").strip().upper().replace("/", "_").replace("-", "_")
            for instrument in instruments
        ]
        basket = [instrument for instrument in basket if instrument]
        if not basket:
            return
        key = tuple(sorted(dict.fromkeys(basket)))
        if key in basket_keys:
            return
        basket_keys.add(key)
        baskets.append((name, list(dict.fromkeys(basket))))

    add_basket("current_unknown_profile_count_top", [row[0] for row in count_ranked])
    add_basket("current_unknown_profile_near_threshold", [row[0] for row in near_ranked])
    for instrument, _rows, near_rows, _avg_ratio, _max_ratio, _score in ranked[:2]:
        add_basket(f"current_unknown_profile_{instrument.lower()}", [instrument])
    for instrument, _rows, near_rows, _avg_ratio, _max_ratio, _score in near_ranked[:1]:
        if near_rows:
            add_basket(f"current_unknown_profile_near_{instrument.lower()}", [instrument])

    return baskets, ranked[: max(count_limit, near_limit, 6)]


def current_count_miss_baskets(limit: int = 3) -> List[Tuple[str, int]]:
    """Return repeated missed instruments from the latest daily report.

    Value-ranked misses identify the most economically meaningful candidates,
    but clustered spike failures often show up first as repeated skipped rows
    across one currency theme, for example several ZAR crosses.  This count
    basket is research-only and lets the trainer validate those grouped misses
    without loosening any live execution gates.
    """
    day_dir = latest_daily_move_dir()
    if not day_dir:
        return []
    missed_path = day_dir / "missed_moves.csv"
    if not missed_path.exists():
        return []
    counts: Dict[str, int] = {}
    seen_rows: set[Tuple[str, str, str, str, str]] = set()
    with missed_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            instrument = (
                str(row.get("instrument") or "")
                .strip()
                .upper()
                .replace("/", "_")
                .replace("-", "_")
            )
            if not instrument:
                continue
            key = (
                str(row.get("time_ny") or "")[:19],
                instrument,
                str(row.get("direction") or "").strip().upper(),
                str(row.get("miss_reason") or "").strip().lower(),
                str(row.get("detail") or "").strip(),
            )
            if key in seen_rows:
                continue
            seen_rows.add(key)
            counts[instrument] = counts.get(instrument, 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]


def current_currency_miss_baskets(
    *,
    limit_currencies: int = 2,
    instruments_per_currency: int = 6,
) -> List[Tuple[str, List[str]]]:
    """Build research baskets around missed-move currency legs.

    Live missed spikes often cluster by currency, not by a single pair.  A
    single-pair follow-up can be too sparse for rolling folds, so this groups
    the latest missed rows into compact baskets like AUD-weakness or
    ZAR-weakness candidates.  It remains research-only and only uses local
    daily-move report rows.
    """
    day_dir = latest_daily_move_dir()
    if not day_dir:
        return []
    missed_path = day_dir / "missed_moves.csv"
    if not missed_path.exists():
        return []
    currency_score: Dict[str, float] = {}
    currency_instruments: Dict[str, Dict[str, Dict[str, float]]] = {}
    seen_rows: set[Tuple[str, str, str, str, str]] = set()
    with missed_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            instrument = (
                str(row.get("instrument") or "")
                .strip()
                .upper()
                .replace("/", "_")
                .replace("-", "_")
            )
            if "_" not in instrument:
                continue
            key = (
                str(row.get("time_ny") or "")[:19],
                instrument,
                str(row.get("direction") or "").strip().upper(),
                str(row.get("miss_reason") or "").strip().lower(),
                str(row.get("detail") or "").strip(),
            )
            if key in seen_rows:
                continue
            seen_rows.add(key)
            parsed_value = missed_detail_value_usd(row.get("detail"))
            value = max(float(parsed_value or 0.0), 0.0)
            # Count-only rows still matter for finding clustered themes, but
            # missed dollar value should dominate the ranking.
            score = value + 0.001
            base, quote = instrument.split("_", 1)
            for currency in [base, quote]:
                if not currency:
                    continue
                currency_score[currency] = currency_score.get(currency, 0.0) + score
                stats = currency_instruments.setdefault(currency, {}).setdefault(
                    instrument,
                    {"value": 0.0, "count": 0.0},
                )
                stats["value"] += value
                stats["count"] += 1.0

    baskets: List[Tuple[str, List[str]]] = []
    for currency, _score in sorted(currency_score.items(), key=lambda item: (-item[1], item[0]))[
        :limit_currencies
    ]:
        inst_stats = currency_instruments.get(currency) or {}
        ranked_instruments = sorted(
            inst_stats.items(),
            key=lambda item: (-(item[1].get("value") or 0.0), -(item[1].get("count") or 0.0), item[0]),
        )
        instruments = [instrument for instrument, _stats in ranked_instruments[:instruments_per_currency]]
        if len(instruments) >= 2:
            baskets.append((f"current_currency_{currency.lower()}", instruments))
    return baskets


def current_ev_near_miss_baskets(
    *,
    min_score_ratio: float = 0.80,
    limit: int = 4,
) -> Tuple[List[str], List[Tuple[str, float, int]]]:
    """Return instruments repeatedly rejected just below the scout EV gate.

    The first live-week misses are not only generic value misses.  A separate
    pattern is high-value scout candidates blocked by a model score just under
    the current EV threshold, for example ``scout EV score too low 87 < 90``.
    These should not loosen live gates directly; they should seed compact
    offline validations that can prove whether a calibrated challenger is
    warranted for those instruments.
    """
    day_dir = latest_daily_move_dir()
    if not day_dir:
        return [], []
    missed_path = day_dir / "missed_moves.csv"
    if not missed_path.exists():
        return [], []
    by_instrument: Dict[str, Dict[str, float]] = {}
    seen_rows: set[Tuple[str, str, str, str, str]] = set()
    pattern = re.compile(
        r"scout\s+EV\s+score\s+too\s+low\s+(-?\d+(?:\.\d+)?)\s*<\s*(-?\d+(?:\.\d+)?)",
        flags=re.IGNORECASE,
    )
    with missed_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            instrument = (
                str(row.get("instrument") or "")
                .strip()
                .upper()
                .replace("/", "_")
                .replace("-", "_")
            )
            detail = str(row.get("detail") or "")
            if not instrument or not detail:
                continue
            match = pattern.search(detail)
            if not match:
                continue
            score = float(match.group(1))
            threshold = float(match.group(2))
            if threshold <= 0.0 or score / threshold < min_score_ratio:
                continue
            key = (
                str(row.get("time_ny") or "")[:19],
                instrument,
                str(row.get("direction") or "").strip().upper(),
                str(row.get("miss_reason") or "").strip().lower(),
                detail.strip(),
            )
            if key in seen_rows:
                continue
            seen_rows.add(key)
            value = max(float(missed_detail_value_usd(detail) or 0.0), 0.0)
            # Value dominates; score/count break ties so repeated near misses
            # with small values still get a small signal.
            stats = by_instrument.setdefault(
                instrument,
                {"value": 0.0, "score_gap": 0.0, "count": 0.0},
            )
            stats["value"] += value
            stats["score_gap"] += max(0.0, threshold - score)
            stats["count"] += 1.0
    ranked_rows = sorted(
        (
            (
                instrument,
                float(stats.get("value") or 0.0),
                int(stats.get("count") or 0),
            )
            for instrument, stats in by_instrument.items()
        ),
        key=lambda item: (-item[1], -item[2], item[0]),
    )[:limit]
    return [instrument for instrument, _value, _count in ranked_rows], ranked_rows


def current_scan_cost_candidate_baskets(
    *,
    limit: int = 6,
    max_rows: int = 160,
    min_cost_adjusted_net_pips: float = 12.0,
    min_ratio: float = 0.95,
) -> Tuple[List[str], List[Tuple[str, float, int, float, float]]]:
    """Return primary-scan candidates with positive cost-adjusted movement.

    Daily missed-move rows capture final missed opportunities, but the primary
    scout scan log also records raw candidates that never reach an order
    attempt because broad pressure/scout gates fail.  This research-only basket
    promotes the *question* to the trainer: do instruments repeatedly showing
    positive movement after spread have reusable pre-move signal?

    It does not change any live threshold or execution gate.
    """
    if not PRIMARY_EVENT_SCAN_PATH.exists():
        return [], []

    rows: deque[Dict[str, str]] = deque(maxlen=max_rows)
    try:
        with PRIMARY_EVENT_SCAN_PATH.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                rows.append(row)
    except Exception:
        return [], []

    by_instrument: Dict[str, Dict[str, float]] = {}
    seen: set[Tuple[str, str, str, str]] = set()
    for row in rows:
        raw_text = str(row.get("raw_json") or "")
        if not raw_text:
            continue
        try:
            raw = json.loads(raw_text)
        except Exception:
            continue
        if not isinstance(raw, dict):
            continue
        candidates: List[Dict[str, Any]] = []
        for key in ("top_candidates", "rejected"):
            value = raw.get(key)
            if isinstance(value, list):
                candidates.extend(item for item in value if isinstance(item, dict))
        for candidate in candidates:
            instrument = (
                str(candidate.get("instrument") or "")
                .strip()
                .upper()
                .replace("/", "_")
                .replace("-", "_")
            )
            direction = str(candidate.get("direction") or "").strip().upper()
            window = str(candidate.get("window_minutes") or "")
            if not instrument or direction not in {"LONG", "SHORT"}:
                continue
            key = (str(row.get("time_utc") or "")[:19], instrument, direction, window)
            if key in seen:
                continue
            seen.add(key)
            try:
                cost_net = float(candidate.get("cost_adjusted_net_pips") or 0.0)
                ratio = float(candidate.get("move_to_spread_ratio") or 0.0)
                raw_net = abs(float(candidate.get("net_pips") or 0.0))
            except Exception:
                continue
            if cost_net < min_cost_adjusted_net_pips or ratio < min_ratio:
                continue
            stats = by_instrument.setdefault(
                instrument,
                {
                    "score": 0.0,
                    "rows": 0.0,
                    "cost_net": 0.0,
                    "max_cost_net": 0.0,
                    "max_ratio": 0.0,
                    "raw_net": 0.0,
                },
            )
            # Cost-adjusted pips dominate.  Ratio and repeated appearances
            # break ties so the trainer prioritizes tradable follow-through,
            # not just large raw moves with high spread.
            stats["score"] += cost_net * max(0.25, min(ratio, 5.0))
            stats["rows"] += 1.0
            stats["cost_net"] += cost_net
            stats["max_cost_net"] = max(stats["max_cost_net"], cost_net)
            stats["max_ratio"] = max(stats["max_ratio"], ratio)
            stats["raw_net"] += raw_net

    ranked = sorted(
        (
            (
                instrument,
                float(stats.get("score") or 0.0),
                int(stats.get("rows") or 0),
                float(stats.get("max_cost_net") or 0.0),
                float(stats.get("max_ratio") or 0.0),
            )
            for instrument, stats in by_instrument.items()
        ),
        key=lambda item: (-item[1], -item[2], item[0]),
    )[:limit]
    return [instrument for instrument, _score, _rows, _net, _ratio in ranked], ranked


def current_regime_lock_miss_baskets(
    *,
    limit: int = 6,
) -> Tuple[List[str], List[Tuple[str, int, float, str]]]:
    """Return instruments repeatedly blocked by the scout regime lock.

    These are not automatic live relaxations.  The intent is to ask the
    research trainer whether the anti-flip/counter-regime guard is filtering
    reusable reversal opportunities for specific instruments or baskets.
    """
    day_dir = latest_daily_move_dir()
    if not day_dir:
        return [], []
    missed_path = day_dir / "missed_moves.csv"
    if not missed_path.exists():
        return [], []
    pattern = re.compile(
        r"counter-regime scout signal;\s*([^:]+):\s*([A-Z]{3}_[A-Z]{3})\s+"
        r"(LONG|SHORT)\s+edge=([-+]?\d+(?:\.\d+)?)",
        flags=re.IGNORECASE,
    )
    by_instrument: Dict[str, Dict[str, Any]] = {}
    seen_rows: set[Tuple[str, str, str, str]] = set()
    with missed_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            detail = str(row.get("detail") or "")
            if "counter-regime" not in detail.lower():
                continue
            match = pattern.search(detail)
            instrument = (
                str(row.get("instrument") or "")
                .strip()
                .upper()
                .replace("/", "_")
                .replace("-", "_")
            )
            direction = str(row.get("direction") or "").strip().upper()
            label = "unknown_lock"
            edge = 0.0
            if match:
                label = match.group(1).strip()
                instrument = match.group(2).upper().replace("/", "_").replace("-", "_")
                direction = match.group(3).upper()
                edge = abs(float(match.group(4)))
            if not instrument:
                continue
            key = (str(row.get("time_ny") or "")[:19], instrument, direction, detail.strip())
            if key in seen_rows:
                continue
            seen_rows.add(key)
            stats = by_instrument.setdefault(
                instrument,
                {"count": 0, "edge_sum": 0.0, "labels": Counter()},
            )
            stats["count"] = int(stats.get("count") or 0) + 1
            stats["edge_sum"] = float(stats.get("edge_sum") or 0.0) + edge
            labels = stats.get("labels")
            if isinstance(labels, Counter):
                labels[label] += 1

    ranked: List[Tuple[str, int, float, str]] = []
    for instrument, stats in by_instrument.items():
        count = int(stats.get("count") or 0)
        if count <= 0:
            continue
        avg_edge = float(stats.get("edge_sum") or 0.0) / max(1, count)
        labels = stats.get("labels")
        top_label = "unknown_lock"
        if isinstance(labels, Counter) and labels:
            top_label = labels.most_common(1)[0][0]
        ranked.append((instrument, count, avg_edge, top_label))
    ranked.sort(key=lambda item: (-item[1], -item[2], item[0]))
    ranked = ranked[:limit]
    return [instrument for instrument, _count, _edge, _label in ranked], ranked


def seed_live_value_miss_followup_variants(
    registry: ModelLifecycleRegistry,
    seen: set[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    """Seed compact follow-ups for the current value-weighted live misses.

    This makes the first-24h advisor loop actionable without loosening live
    gates: if a pair shows repeated missed economic value, the trainer gets a
    small whitelist experiment set to validate whether that pair/basket has a
    reusable pre-spike signal.
    """
    top_instruments, ranked = current_value_miss_baskets(limit=3)
    count_ranked = current_count_miss_baskets(limit=3)
    currency_baskets = current_currency_miss_baskets(limit_currencies=2, instruments_per_currency=6)
    near_value_instruments, near_value_ranked = current_near_value_gate_baskets(limit=6)
    scan_cost_instruments, scan_cost_ranked = current_scan_cost_candidate_baskets(limit=6)
    regime_lock_instruments, regime_lock_ranked = current_regime_lock_miss_baskets(limit=6)
    if (
        not top_instruments
        and not count_ranked
        and not currency_baskets
        and not near_value_instruments
        and not scan_cost_instruments
        and not regime_lock_instruments
    ):
        return 0, []
    baskets: List[Tuple[str, List[str]]] = []
    basket_keys: set[Tuple[str, ...]] = set()

    def add_basket(name: str, instruments: Iterable[str]) -> None:
        basket = [
            str(instrument or "").strip().upper().replace("/", "_").replace("-", "_")
            for instrument in instruments
        ]
        basket = [instrument for instrument in basket if instrument]
        if not basket:
            return
        key = tuple(sorted(dict.fromkeys(basket)))
        if key in basket_keys:
            return
        basket_keys.add(key)
        baskets.append((name, list(dict.fromkeys(basket))))

    add_basket("current_value_top", top_instruments)
    add_basket("current_count_top", [instrument for instrument, _count in count_ranked])
    add_basket("current_near_value_gate_top", near_value_instruments[:3])
    if len(near_value_instruments) >= 4:
        add_basket("current_near_value_gate_broad", near_value_instruments)
    add_basket("current_scan_cost_top", scan_cost_instruments[:3])
    if len(scan_cost_instruments) >= 4:
        add_basket("current_scan_cost_broad", scan_cost_instruments)
    add_basket("current_regime_lock_top", regime_lock_instruments[:3])
    if len(regime_lock_instruments) >= 4:
        add_basket("current_regime_lock_broad", regime_lock_instruments)
    for basket_name, instruments in currency_baskets:
        add_basket(basket_name, instruments)
    for instrument, _value in ranked[:2]:
        add_basket(f"current_value_{instrument.lower()}", [instrument])
    for instrument, _expected, _count, _ratio in near_value_ranked[:1]:
        add_basket(f"current_near_value_gate_{instrument.lower()}", [instrument])
    for instrument, _score, _rows, _net, _ratio in scan_cost_ranked[:2]:
        add_basket(f"current_scan_cost_{instrument.lower()}", [instrument])

    lanes = [
        (
            "live_primary_challenger_candidate",
            "primary_live_001-001-21715580-002",
            "primary_live_value_miss",
        ),
        (
            "live_tech_champion_candidate",
            "tech_live_001-001-21715580-003",
            "tech_live_value_miss",
        ),
    ]
    filters = {
        "regimes": ["high_vol_event", "spread_impaired"],
        "sessions": ["asia", "london", "new_york", "off_session"],
        "exclude_sessions": ["rollover"],
    }
    scan_cost_contexts: List[Tuple[str, Dict[str, Any], str]] = [
        ("event", filters, ""),
        ("liquidity", {"exclude_sessions": ["rollover"]}, "_liquidity"),
        ("unfiltered", {}, "_unfiltered"),
    ]
    seeded: List[Dict[str, Any]] = []
    count = 0
    source = "operator_live_value_miss_followup_v1"
    for lane, focus, prefix in lanes:
        for basket_name, basket in baskets:
            pair_level_followup = len(basket) == 1
            context_options = (
                scan_cost_contexts
                if "current_scan_cost" in basket_name
                else scan_cost_contexts[:2]
                if "current_regime_lock" in basket_name
                else [("event", filters, "")]
            )
            validation_options = (
                [
                    (4, "live_value_miss_pair_followup_4w_v1", "_pair4w"),
                    (8, "live_value_miss_pair_followup_8w_v1", "_pair8w"),
                ]
                if pair_level_followup
                else [(8, "live_value_miss_followup_8w_v1", "")]
            )
            for context_label, segment_filters, context_suffix in context_options:
                for validation_weeks, validation_profile, profile_suffix in validation_options:
                    profile_suffix_full = f"{profile_suffix}{context_suffix}"
                    validation_profile_full = (
                        f"{validation_profile}_{context_label}"
                        if context_label != "event"
                        else validation_profile
                    )
                    for lead in [15, 30]:
                        for execution_policy in ["trailing_stop012", "trailing_stop015"]:
                            for model_type in ["extra_trees", "random_forest"]:
                                params = confirmation_tree_parameter_variants(model_type)[0]
                                spec = major_lead_spec(
                                    source=source,
                                    deployment_lane=lane,
                                    account_focus=focus,
                                    model_type=model_type,
                                    lead_minutes=lead,
                                    horizon_minutes=120,
                                    # The current value-miss basket can mix crosses
                                    # (for example EUR_AUD) with exotics (USD_ZAR,
                                    # USD_MXN).  Use the whitelist as the true pair
                                    # selector here; otherwise a broad subset such as
                                    # volatile_exotic can silently filter out valid
                                    # live misses before training.
                                    instrument_subset="all",
                                    instrument_whitelist=basket,
                                    segment_filters=segment_filters,
                                    specialist_profile=(
                                        f"{prefix}_{basket_name}_lead{lead}_event120_"
                                        f"{execution_policy}_{model_type}{profile_suffix_full}"
                                    ),
                                    parameters=params,
                                    validation_weeks=validation_weeks,
                                    max_train_rows=260_000 if validation_weeks >= 8 else 220_000,
                                    execution_policy=execution_policy,
                                    evaluation_stage="validation",
                                    validation_profile=validation_profile_full,
                                )
                                if "current_regime_lock" in basket_name:
                                    spec["live_value_miss_context"] = {
                                        "context_label": context_label,
                                        "regime_lock_ranked": [
                                            {
                                                "instrument": inst,
                                                "count": count_rows,
                                                "avg_abs_edge": round(avg_edge, 4),
                                                "top_label": label,
                                            }
                                            for inst, count_rows, avg_edge, label in regime_lock_ranked
                                            if inst in basket
                                        ],
                                        "rationale": (
                                            "Research-only test for scout regime-lock "
                                            "misses: validate whether instruments "
                                            "repeatedly blocked by the anti-flip/"
                                            "counter-regime guard have reusable "
                                            "lead-event reversal signal. This does not "
                                            "loosen live regime locks."
                                        ),
                                    }
                                else:
                                    spec["live_value_miss_context"] = {
                                        "context_label": context_label,
                                        "rationale": (
                                            "Current scan-cost singleton specs are too "
                                            "sparse under the strict event filter, while "
                                            "the broad basket can form folds but has weak "
                                            "classification quality.  Fallback contexts "
                                            "test whether the signal is reusable under "
                                            "liquidity-only or unfiltered validation "
                                            "before rejecting this scan-cost lane."
                                        ),
                                    }
                                if enqueue_unique(
                                    registry,
                                    spec,
                                    reason="live_value_weighted_miss_followup",
                                    seen_hashes=seen,
                                ):
                                    count += 1
                            seeded.append(spec)
    return count, seeded


def _float_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except Exception:
        return None
    return number if number == number else None


def current_scan_cost_segment_focus_baskets(
    *,
    limit: int = 3,
) -> List[Dict[str, Any]]:
    """Build focused baskets from near-miss broad scan-cost experiments.

    This is intentionally downstream of validation results.  It does not
    promote a model; it asks the next narrower research question when a broad
    scan-cost basket forms folds and is economically positive but fails global
    detection/concentration gates.
    """
    candidates: List[Dict[str, Any]] = []
    for row in reversed(read_ledger_rows()):
        profile = str(row.get("specialist_profile") or "")
        if "current_scan_cost_broad" not in profile:
            continue
        mean_auc = _float_or_none(row.get("mean_auc"))
        minimum_auc = _float_or_none(row.get("minimum_week_auc"))
        mean_net = _float_or_none(row.get("mean_net_pips"))
        folds = _float_or_none(row.get("fold_count"))
        if (
            mean_auc is None
            or minimum_auc is None
            or mean_net is None
            or folds is None
            or mean_auc < 0.55
            or minimum_auc < 0.52
            or mean_net <= 0
            or folds < 2
        ):
            continue
        detail_path = Path(str(row.get("detail_path") or ""))
        if not detail_path.exists():
            continue
        try:
            detail = json.loads(detail_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        result = detail.get("result") if isinstance(detail.get("result"), dict) else {}
        scorecards = result.get("scorecards") if isinstance(result.get("scorecards"), dict) else {}
        instrument_rows = scorecards.get("instrument") if isinstance(scorecards.get("instrument"), list) else []
        positive_rows = []
        for item in instrument_rows:
            if not isinstance(item, dict):
                continue
            instrument = (
                str(item.get("segment") or "")
                .strip()
                .upper()
                .replace("/", "_")
                .replace("-", "_")
            )
            if not instrument:
                continue
            trades = _float_or_none(item.get("trades")) or 0.0
            mean_net_segment = _float_or_none(item.get("mean_net")) or 0.0
            profit_factor = _float_or_none(item.get("profit_factor")) or 0.0
            total_net = _float_or_none(item.get("total_net")) or 0.0
            if trades >= 5 and mean_net_segment > 0 and profit_factor >= 1.20 and total_net > 0:
                positive_rows.append({
                    "instrument": instrument,
                    "trades": trades,
                    "mean_net": mean_net_segment,
                    "profit_factor": profit_factor,
                    "total_net": total_net,
                })
        positive_rows.sort(
            key=lambda item: (
                -float(item.get("total_net") or 0.0),
                -float(item.get("trades") or 0.0),
                str(item.get("instrument") or ""),
            )
        )
        basket = list(dict.fromkeys(str(item["instrument"]) for item in positive_rows[:5]))
        if len(basket) < 2:
            continue
        allowed = result.get("allowed_segments") if isinstance(result.get("allowed_segments"), dict) else {}
        candidates.append({
            "basket": basket,
            "source_experiment_id": row.get("experiment_id", ""),
            "source_profile": profile,
            "source_metrics": {
                "mean_auc": mean_auc,
                "minimum_week_auc": minimum_auc,
                "mean_net_pips": mean_net,
                "fold_count": folds,
                "top_pair_trade_share": _float_or_none(row.get("top_pair_trade_share")),
                "trades": _float_or_none(row.get("trades")),
            },
            "positive_segments": positive_rows[:8],
            "allowed_regimes": [
                str(item)
                for item in (allowed.get("allowed_regimes") or [])
                if str(item).strip()
            ],
            "allowed_sessions": [
                str(item)
                for item in (allowed.get("allowed_sessions") or [])
                if str(item).strip()
            ],
        })
        if len(candidates) >= limit:
            break
    return candidates


def seed_scan_cost_segment_focus_variants(
    registry: ModelLifecycleRegistry,
    seen: set[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    """Seed focused scan-cost follow-ups from positive broad-basket segments."""
    candidates = current_scan_cost_segment_focus_baskets()
    if not candidates:
        return 0, []

    source = "operator_live_value_miss_followup_v1"
    lane = "live_primary_challenger_candidate"
    focus = "primary_live_001-001-21715580-002"
    seeded: List[Dict[str, Any]] = []
    count = 0
    for candidate in candidates:
        basket = list(candidate.get("basket") or [])
        slug = "_".join(instrument.lower() for instrument in basket[:5])
        contexts: List[Tuple[str, Dict[str, Any]]] = [
            ("liquidity", {"exclude_sessions": ["rollover"]}),
        ]
        regimes = list(candidate.get("allowed_regimes") or [])
        sessions = list(candidate.get("allowed_sessions") or [])
        if regimes or sessions:
            context_filters: Dict[str, Any] = {"exclude_sessions": ["rollover"]}
            if regimes:
                context_filters["regimes"] = regimes
            if sessions:
                context_filters["sessions"] = sessions
            contexts.append(("scorecard_segments", context_filters))
        for context_label, segment_filters in contexts:
            for lead in [15, 30]:
                for execution_policy in ["trailing_stop012", "trailing_stop015"]:
                    for model_type in ["extra_trees", "random_forest"]:
                        params = confirmation_tree_parameter_variants(model_type)[0]
                        spec = major_lead_spec(
                            source=source,
                            deployment_lane=lane,
                            account_focus=focus,
                            model_type=model_type,
                            lead_minutes=lead,
                            horizon_minutes=120,
                            instrument_subset="all",
                            instrument_whitelist=basket,
                            segment_filters=segment_filters,
                            specialist_profile=(
                                f"primary_live_value_miss_current_scan_cost_segment_focus_"
                                f"{slug}_{context_label}_lead{lead}_event120_"
                                f"{execution_policy}_{model_type}"
                            ),
                            parameters=params,
                            validation_weeks=8,
                            max_train_rows=260_000,
                            execution_policy=execution_policy,
                            evaluation_stage="validation",
                            validation_profile=(
                                f"live_scan_cost_segment_focus_8w_v1_{context_label}"
                            ),
                        )
                        spec["live_value_miss_context"] = {
                            "context_label": context_label,
                            "source_experiment_id": candidate.get("source_experiment_id", ""),
                            "source_profile": candidate.get("source_profile", ""),
                            "source_metrics": candidate.get("source_metrics", {}),
                            "positive_segments": candidate.get("positive_segments", []),
                            "rationale": (
                                "A broad scan-cost basket formed folds and was "
                                "economically positive but failed global detection "
                                "and concentration gates.  This follow-up removes "
                                "negative/weak instruments and validates the "
                                "positive segment basket before any live use."
                            ),
                        }
                        if enqueue_unique(
                            registry,
                            spec,
                            reason="live_scan_cost_segment_focus_followup",
                            seen_hashes=seen,
                        ):
                            count += 1
                            seeded.append(spec)
            if context_label == "liquidity":
                for model_type in ["hist_gradient_boosting", "gradient_boosting"]:
                    for param_index, params in enumerate(
                        calibration_model_parameter_variants(model_type),
                        1,
                    ):
                        spec = major_lead_spec(
                            source=source,
                            deployment_lane=lane,
                            account_focus=focus,
                            model_type=model_type,
                            lead_minutes=15,
                            horizon_minutes=120,
                            instrument_subset="all",
                            instrument_whitelist=basket,
                            segment_filters=segment_filters,
                            specialist_profile=(
                                f"primary_live_value_miss_current_scan_cost_segment_focus_"
                                f"{slug}_{context_label}_lead15_event120_"
                                f"trailing_stop015_{model_type}_calibration{param_index}"
                            ),
                            parameters=params,
                            validation_weeks=8,
                            max_train_rows=260_000,
                            execution_policy="trailing_stop015",
                            evaluation_stage="validation",
                            validation_profile=(
                                f"live_scan_cost_segment_focus_8w_v1_"
                                f"{context_label}_calibration"
                            ),
                        )
                        spec["live_value_miss_context"] = {
                            "context_label": f"{context_label}_calibration",
                            "source_experiment_id": candidate.get("source_experiment_id", ""),
                            "source_profile": candidate.get("source_profile", ""),
                            "source_metrics": candidate.get("source_metrics", {}),
                            "positive_segments": candidate.get("positive_segments", []),
                            "rationale": (
                                "Segment-focused scan-cost tree models preserved "
                                "positive net pips but failed probability/detection "
                                "quality gates.  This compact follow-up tests "
                                "calibration-oriented boosting families on the "
                                "same positive segment basket."
                            ),
                        }
                        if enqueue_unique(
                            registry,
                            spec,
                            reason="live_scan_cost_segment_focus_calibration_followup",
                            seen_hashes=seen,
                        ):
                            count += 1
                            seeded.append(spec)
    return count, seeded


def current_scan_cost_broad_calibration_candidates(
    *,
    limit: int = 2,
) -> List[Dict[str, Any]]:
    """Return broad scan-cost near-misses worth calibration-family follow-up."""
    candidates: List[Dict[str, Any]] = []
    seen_keys: set[Tuple[str, ...]] = set()
    for row in reversed(read_ledger_rows()):
        profile = str(row.get("specialist_profile") or "")
        if "current_scan_cost_broad" not in profile or "calibration" in profile:
            continue
        if "_liquidity" not in profile:
            # The liquidity broad basket is the only scan-cost context that has
            # both fold coverage and near-threshold detection evidence so far.
            continue
        mean_auc = _float_or_none(row.get("mean_auc"))
        minimum_auc = _float_or_none(row.get("minimum_week_auc"))
        mean_net = _float_or_none(row.get("mean_net_pips"))
        folds = _float_or_none(row.get("fold_count"))
        trades = _float_or_none(row.get("trades"))
        if (
            mean_auc is None
            or minimum_auc is None
            or mean_net is None
            or folds is None
            or trades is None
            or mean_auc < 0.55
            or minimum_auc < 0.50
            or mean_net <= 0
            or folds < 3
            or trades < 75
        ):
            continue
        basket = manager.normalized_instrument_whitelist(row.get("instrument_whitelist"))
        if len(basket) < 4:
            continue
        key = tuple(basket)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        try:
            segment_filters = json.loads(str(row.get("segment_filters") or "{}"))
        except Exception:
            segment_filters = {"exclude_sessions": ["rollover"]}
        if not isinstance(segment_filters, dict):
            segment_filters = {"exclude_sessions": ["rollover"]}
        candidates.append({
            "basket": basket,
            "segment_filters": segment_filters,
            "source_experiment_id": row.get("experiment_id", ""),
            "source_profile": profile,
            "source_metrics": {
                "mean_auc": mean_auc,
                "minimum_week_auc": minimum_auc,
                "mean_net_pips": mean_net,
                "fold_count": folds,
                "trades": trades,
                "top_pair_trade_share": _float_or_none(row.get("top_pair_trade_share")),
            },
        })
        if len(candidates) >= limit:
            break
    return candidates


def seed_scan_cost_broad_calibration_variants(
    registry: ModelLifecycleRegistry,
    seen: set[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    """Seed compact calibration follow-ups for broad scan-cost near-misses."""
    candidates = current_scan_cost_broad_calibration_candidates()
    if not candidates:
        return 0, []

    source = "operator_live_value_miss_followup_v1"
    lane = "live_primary_challenger_candidate"
    focus = "primary_live_001-001-21715580-002"
    seeded: List[Dict[str, Any]] = []
    count = 0
    for candidate in candidates:
        basket = list(candidate.get("basket") or [])
        slug = "_".join(instrument.lower() for instrument in basket[:6])
        segment_filters = (
            candidate.get("segment_filters")
            if isinstance(candidate.get("segment_filters"), dict)
            else {"exclude_sessions": ["rollover"]}
        )
        for model_type in ["hist_gradient_boosting", "gradient_boosting"]:
            for param_index, params in enumerate(
                calibration_model_parameter_variants(model_type),
                1,
            ):
                spec = major_lead_spec(
                    source=source,
                    deployment_lane=lane,
                    account_focus=focus,
                    model_type=model_type,
                    lead_minutes=15,
                    horizon_minutes=120,
                    instrument_subset="all",
                    instrument_whitelist=basket,
                    segment_filters=segment_filters,
                    specialist_profile=(
                        f"primary_live_value_miss_current_scan_cost_broad_calibration_"
                        f"{slug}_liquidity_lead15_event120_trailing_stop015_"
                        f"{model_type}_calibration{param_index}"
                    ),
                    parameters=params,
                    validation_weeks=8,
                    max_train_rows=260_000,
                    execution_policy="trailing_stop015",
                    evaluation_stage="validation",
                    validation_profile="live_scan_cost_broad_calibration_8w_v1_liquidity",
                )
                spec["live_value_miss_context"] = {
                    "context_label": "broad_liquidity_calibration",
                    "source_experiment_id": candidate.get("source_experiment_id", ""),
                    "source_profile": candidate.get("source_profile", ""),
                    "source_metrics": candidate.get("source_metrics", {}),
                    "rationale": (
                        "Segment-focus calibration improved AUC but lacked "
                        "trade count.  The broad liquidity basket already has "
                        "enough validation trades and positive economics, so "
                        "test the same calibration-oriented boosting families "
                        "on the broader scan-cost basket."
                    ),
                }
                if enqueue_unique(
                    registry,
                    spec,
                    reason="live_scan_cost_broad_calibration_followup",
                    seen_hashes=seen,
                ):
                    count += 1
                    seeded.append(spec)
    return count, seeded


def truthy_text(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def current_calibration_detection_followup_candidates(
    *,
    limit_per_lane: int = 3,
    min_readiness_score: float = 5.0,
) -> List[Dict[str, Any]]:
    """Return economic-positive validation candidates blocked by Brier/detection.

    Promotion readiness now separates candidates that made money across the
    selected validation windows but failed probability-quality gates.  These
    are not live candidates.  They are good research inputs for compact
    calibration-oriented boosting families, especially when the blocker is
    `mean_brier_skill_positive` rather than net/trade/coverage failure.
    """
    rows = read_tail_csv_rows(PROMOTION_READINESS_CSV, max_rows=1_000)
    if not rows:
        return []

    lane_buckets: Dict[str, List[Dict[str, Any]]] = {}
    seen_keys: set[Tuple[str, str, str, Tuple[str, ...]]] = set()
    for row in rows:
        if not truthy_text(row.get("calibration_detection_followup")):
            continue
        if str(row.get("source") or "") != "operator_live_value_miss_followup_v1":
            continue
        readiness = row_float(row, "readiness_score")
        if readiness < min_readiness_score:
            continue
        failed_reasons = str(row.get("gate_failed_reasons") or "")
        if "mean_brier_skill_positive" not in failed_reasons:
            continue
        target = str(row.get("target") or "")
        match = re.search(r"major_event_lead_(\d+)_(\d+)", target)
        if not match:
            continue
        lead_minutes = int(match.group(1))
        horizon_minutes = int(match.group(2))
        execution_policy = str(row.get("execution_policy") or "").strip()
        if execution_policy not in {"trailing_stop012", "trailing_stop015"}:
            continue
        basket = manager.normalized_instrument_whitelist(row.get("instrument_whitelist"))
        if len(basket) < 2:
            continue
        deployment_lane = str(row.get("deployment_lane") or "")
        if deployment_lane not in {
            "live_primary_challenger_candidate",
            "live_tech_champion_candidate",
        }:
            continue
        try:
            segment_filters = json.loads(str(row.get("segment_filters") or "{}"))
        except Exception:
            segment_filters = {}
        if not isinstance(segment_filters, dict):
            segment_filters = {}
        if not segment_filters:
            segment_filters = {
                "regimes": ["high_vol_event", "spread_impaired"],
                "sessions": ["asia", "london", "new_york", "off_session"],
                "exclude_sessions": ["rollover"],
            }
        key = (deployment_lane, target, execution_policy, tuple(basket))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        lane_buckets.setdefault(deployment_lane, []).append({
            "account_focus": str(row.get("account_focus") or ""),
            "basket": basket,
            "deployment_lane": deployment_lane,
            "execution_policy": execution_policy,
            "failed_reasons": failed_reasons,
            "horizon_minutes": horizon_minutes,
            "lead_minutes": lead_minutes,
            "readiness_score": readiness,
            "segment_filters": segment_filters,
            "source_experiment_id": str(row.get("experiment_id") or ""),
            "source_model_type": str(row.get("model_type") or ""),
            "source_profile": str(row.get("specialist_profile") or ""),
            "source_metrics": {
                "mean_auc": row_float(row, "mean_auc"),
                "minimum_week_auc": row_float(row, "minimum_week_auc"),
                "mean_brier_skill": row_float(row, "mean_brier_skill"),
                "mean_net_pips": row_float(row, "mean_net_pips"),
                "trades": row_float(row, "trades"),
                "positive_week_rate": row_float(row, "positive_week_rate"),
            },
            "target": target,
        })

    candidates: List[Dict[str, Any]] = []
    for lane, lane_rows in lane_buckets.items():
        lane_rows.sort(
            key=lambda item: (
                -float(item.get("readiness_score") or 0.0),
                -float((item.get("source_metrics") or {}).get("mean_net_pips") or 0.0),
                str(item.get("target") or ""),
            )
        )
        selected_baskets: set[Tuple[str, ...]] = set()
        for item in lane_rows:
            basket_key = tuple(item.get("basket") or [])
            if basket_key in selected_baskets:
                continue
            selected_baskets.add(basket_key)
            candidates.append(item)
            if len(selected_baskets) >= limit_per_lane:
                break
    return candidates


def seed_calibration_detection_followup_variants(
    registry: ModelLifecycleRegistry,
    seen: set[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    """Seed compact boosting follow-ups for positive economics / weak Brier."""
    candidates = current_calibration_detection_followup_candidates()
    if not candidates:
        return 0, []

    source = "operator_live_value_miss_followup_v1"
    seeded: List[Dict[str, Any]] = []
    count = 0
    for candidate in candidates:
        basket = list(candidate.get("basket") or [])
        slug = "_".join(instrument.lower() for instrument in basket[:6])
        lane = str(candidate.get("deployment_lane") or "")
        focus = str(candidate.get("account_focus") or "")
        if not focus:
            focus = (
                "primary_live_001-001-21715580-002"
                if lane == "live_primary_challenger_candidate"
                else "tech_live_001-001-21715580-003"
            )
        prefix = (
            "primary_live_value_miss"
            if lane == "live_primary_challenger_candidate"
            else "tech_live_value_miss"
        )
        for model_type in ["hist_gradient_boosting", "gradient_boosting"]:
            for variant_index, params in enumerate(
                calibration_model_parameter_variants(model_type),
                1,
            ):
                spec = major_lead_spec(
                    source=source,
                    deployment_lane=lane,
                    account_focus=focus,
                    model_type=model_type,
                    lead_minutes=int(candidate.get("lead_minutes") or 15),
                    horizon_minutes=int(candidate.get("horizon_minutes") or 120),
                    instrument_subset="all",
                    instrument_whitelist=basket,
                    segment_filters=(
                        candidate.get("segment_filters")
                        if isinstance(candidate.get("segment_filters"), dict)
                        else {}
                    ),
                    specialist_profile=(
                        f"{prefix}_calibration_detection_{slug}_"
                        f"lead{int(candidate.get('lead_minutes') or 15)}_"
                        f"event{int(candidate.get('horizon_minutes') or 120)}_"
                        f"{candidate.get('execution_policy')}_{model_type}_v{variant_index}"
                    ),
                    parameters=params,
                    validation_weeks=8,
                    max_train_rows=260_000,
                    execution_policy=str(candidate.get("execution_policy") or "trailing_stop015"),
                    evaluation_stage="validation",
                    validation_profile="live_value_miss_calibration_detection_8w_v1",
                )
                spec["live_value_miss_context"] = {
                    "context_label": "calibration_detection_followup",
                    "source_experiment_id": candidate.get("source_experiment_id", ""),
                    "source_model_type": candidate.get("source_model_type", ""),
                    "source_profile": candidate.get("source_profile", ""),
                    "source_failed_reasons": candidate.get("failed_reasons", ""),
                    "source_metrics": candidate.get("source_metrics", {}),
                    "rationale": (
                        "The source value-miss candidate passed economic "
                        "criteria but failed detection/probability quality "
                        "through Brier skill.  This follow-up tests compact "
                        "boosting families with stronger regularization before "
                        "considering any live routing."
                    ),
                }
                if enqueue_unique(
                    registry,
                    spec,
                    reason="live_value_miss_calibration_detection_followup",
                    seen_hashes=seen,
                ):
                    count += 1
                    seeded.append(spec)
    return count, seeded


def compact_slug(value: Any, *, max_len: int = 90) -> str:
    slug = re.sub(
        r"[^a-z0-9]+",
        "_",
        str(value or "").strip().lower(),
    ).strip("_")
    return slug[:max_len].strip("_") or "na"


def segment_focus_context(
    row: Dict[str, Any],
    *,
    source_basket: List[str],
    source_filters: Dict[str, Any],
) -> Tuple[str, List[str], Dict[str, Any]] | None:
    """Translate a scorecard row into a compact validation context.

    Scorecard rows are descriptive evidence, not deployment instructions.  This
    helper turns the strongest segment into one research-only filter while
    preserving the source experiment's liquidity/regime guardrails where doing
    so will not make the slice meaningless.
    """
    scorecard = str(row.get("scorecard") or "").strip().lower()
    segment_raw = str(row.get("segment") or "").strip()
    if not scorecard or not segment_raw:
        return None

    filters = manager.normalized_segment_filters(source_filters)
    filters.setdefault("exclude_sessions", ["rollover"])
    whitelist = list(source_basket)
    segment_norm = compact_slug(segment_raw)

    if scorecard == "session":
        if segment_norm not in {
            "asia",
            "london",
            "new_york",
            "london_ny_overlap",
            "overlap",
            "off_session",
        }:
            return None
        filters["sessions"] = [segment_norm]
        label = f"session_{segment_norm}"
    elif scorecard == "regime_primary":
        filters["regimes"] = [segment_norm]
        label = f"regime_{segment_norm}"
    elif scorecard == "pair_taxonomy_primary":
        filters["pair_families"] = [segment_norm]
        label = f"family_{segment_norm}"
    elif scorecard == "instrument":
        instrument = manager.normalized_instrument_whitelist([segment_raw])
        if not instrument:
            return None
        instrument_name = instrument[0]
        if source_basket and instrument_name not in source_basket:
            return None
        whitelist = [instrument_name]
        label = f"instrument_{instrument_name.lower()}"
    else:
        return None

    return label, whitelist, filters


def current_calibration_detection_segment_focus_candidates(
    *,
    candidate_limit: int = 4,
    segments_per_candidate: int = 2,
    min_readiness_score: float = 8.0,
    min_segment_trades: float = 40.0,
    min_segment_mean_net: float = 0.18,
    min_segment_profit_factor: float = 5.0,
) -> List[Dict[str, Any]]:
    """Find strong scorecard slices inside unpromotable value-miss candidates.

    The common failure mode in first-24h research has been positive economics
    with weak global detection/calibration.  Before widening the model space,
    test whether the edge is localized to a session, regime, pair family, or
    instrument.  These remain research-only specs and are not promotion
    evidence until they pass validation on their own.
    """
    readiness_rows = read_tail_csv_rows(PROMOTION_READINESS_CSV, max_rows=2_000)
    segment_rows = read_tail_csv_rows(SEGMENT_LEADERBOARD_CSV, max_rows=5_000)
    if not readiness_rows or not segment_rows:
        return []

    segments_by_experiment: Dict[str, List[Dict[str, Any]]] = {}
    for row in segment_rows:
        experiment_id = str(row.get("experiment_id") or "").strip()
        if not experiment_id:
            continue
        trades = row_float(row, "trades")
        mean_net = row_float(row, "mean_net")
        total_net = row_float(row, "total_net")
        profit_factor = row_float(row, "profit_factor")
        if (
            trades < min_segment_trades
            or mean_net < min_segment_mean_net
            or total_net <= 0
            or profit_factor < min_segment_profit_factor
        ):
            continue
        scorecard = str(row.get("scorecard") or "").strip().lower()
        if scorecard == "instrument" and trades < 60:
            # Single-pair slices are the most prone to sparse fold artefacts.
            continue
        if scorecard not in {
            "session",
            "regime_primary",
            "pair_taxonomy_primary",
            "instrument",
        }:
            continue
        segments_by_experiment.setdefault(experiment_id, []).append(row)

    candidates: List[Dict[str, Any]] = []
    seen_keys: set[Tuple[str, str, str, Tuple[str, ...], str]] = set()
    for row in sorted(
        readiness_rows,
        key=lambda item: (
            -row_float(item, "readiness_score"),
            -row_float(item, "mean_net_pips"),
            str(item.get("experiment_id") or ""),
        ),
    ):
        if str(row.get("source") or "") != "operator_live_value_miss_followup_v1":
            continue
        if truthy_text(row.get("gate_passed")):
            continue
        if not truthy_text(row.get("economic_passed")):
            continue
        failed_reasons = str(row.get("gate_failed_reasons") or "")
        if "mean_brier_skill_positive" not in failed_reasons and "detection" not in failed_reasons:
            continue
        readiness = row_float(row, "readiness_score")
        if readiness < min_readiness_score:
            continue
        experiment_id = str(row.get("experiment_id") or "").strip()
        if not experiment_id or experiment_id not in segments_by_experiment:
            continue
        target = str(row.get("target") or "")
        match = re.search(r"major_event_lead_(\d+)_(\d+)", target)
        if not match:
            continue
        execution_policy = str(row.get("execution_policy") or "").strip()
        if execution_policy not in {"trailing_stop012", "trailing_stop015"}:
            continue
        basket = manager.normalized_instrument_whitelist(row.get("instrument_whitelist"))
        if not basket:
            continue
        deployment_lane = str(row.get("deployment_lane") or "")
        if deployment_lane not in {
            "live_primary_challenger_candidate",
            "live_tech_champion_candidate",
        }:
            continue
        try:
            source_filters = json.loads(str(row.get("segment_filters") or "{}"))
        except Exception:
            source_filters = {}
        if not isinstance(source_filters, dict):
            source_filters = {}

        scored_segments = sorted(
            segments_by_experiment[experiment_id],
            key=lambda item: (
                -row_float(item, "leader_score"),
                -row_float(item, "total_net"),
                -row_float(item, "trades"),
                str(item.get("scorecard") or ""),
                str(item.get("segment") or ""),
            ),
        )
        selected_contexts: List[Dict[str, Any]] = []
        context_keys: set[Tuple[str, Tuple[str, ...]]] = set()
        for segment_row in scored_segments:
            context = segment_focus_context(
                segment_row,
                source_basket=basket,
                source_filters=source_filters,
            )
            if context is None:
                continue
            label, whitelist, filters = context
            key = (label, tuple(whitelist))
            if key in context_keys:
                continue
            context_keys.add(key)
            selected_contexts.append({
                "label": label,
                "whitelist": whitelist,
                "filters": filters,
                "scorecard": str(segment_row.get("scorecard") or ""),
                "segment": str(segment_row.get("segment") or ""),
                "segment_metrics": {
                    "trades": row_float(segment_row, "trades"),
                    "mean_net": row_float(segment_row, "mean_net"),
                    "total_net": row_float(segment_row, "total_net"),
                    "profit_factor": row_float(segment_row, "profit_factor"),
                    "leader_score": row_float(segment_row, "leader_score"),
                },
            })
            if len(selected_contexts) >= segments_per_candidate:
                break
        if not selected_contexts:
            continue

        candidate_key = (
            deployment_lane,
            target,
            execution_policy,
            tuple(basket),
            "|".join(item["label"] for item in selected_contexts),
        )
        if candidate_key in seen_keys:
            continue
        seen_keys.add(candidate_key)
        candidates.append({
            "account_focus": str(row.get("account_focus") or ""),
            "basket": basket,
            "contexts": selected_contexts,
            "deployment_lane": deployment_lane,
            "execution_policy": execution_policy,
            "failed_reasons": failed_reasons,
            "horizon_minutes": int(match.group(2)),
            "lead_minutes": int(match.group(1)),
            "readiness_score": readiness,
            "source_experiment_id": experiment_id,
            "source_model_type": str(row.get("model_type") or ""),
            "source_profile": str(row.get("specialist_profile") or ""),
            "source_metrics": {
                "mean_auc": row_float(row, "mean_auc"),
                "minimum_week_auc": row_float(row, "minimum_week_auc"),
                "mean_brier_skill": row_float(row, "mean_brier_skill"),
                "mean_net_pips": row_float(row, "mean_net_pips"),
                "trades": row_float(row, "trades"),
                "positive_week_rate": row_float(row, "positive_week_rate"),
            },
        })
        if len(candidates) >= candidate_limit:
            break
    return candidates


def seed_calibration_detection_segment_focus_variants(
    registry: ModelLifecycleRegistry,
    seen: set[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    """Seed narrow segment-focus tests for economic-positive calibration misses."""
    candidates = current_calibration_detection_segment_focus_candidates()
    if not candidates:
        return 0, []

    source = "operator_live_value_miss_followup_v1"
    seeded: List[Dict[str, Any]] = []
    count = 0
    for candidate in candidates:
        lane = str(candidate.get("deployment_lane") or "")
        focus = str(candidate.get("account_focus") or "")
        if not focus:
            focus = (
                "primary_live_001-001-21715580-002"
                if lane == "live_primary_challenger_candidate"
                else "tech_live_001-001-21715580-003"
            )
        prefix = (
            "primary_live_value_miss"
            if lane == "live_primary_challenger_candidate"
            else "tech_live_value_miss"
        )
        source_model_type = str(candidate.get("source_model_type") or "").lower()
        for context in candidate.get("contexts") or []:
            label = str(context.get("label") or "segment")
            whitelist = list(context.get("whitelist") or [])
            if not whitelist:
                continue
            slug = compact_slug("_".join(whitelist[:4]), max_len=60)
            model_variants: List[Tuple[str, int, Dict[str, Any], str]] = []
            if source_model_type in {"extra_trees", "random_forest"}:
                params = confirmation_tree_parameter_variants(source_model_type)[0]
                model_variants.append((source_model_type, 1, params, "tree_confirm"))
                calibration_params = calibration_model_parameter_variants("gradient_boosting")
                if calibration_params:
                    model_variants.append((
                        "gradient_boosting",
                        1,
                        calibration_params[0],
                        "calibration",
                    ))
            elif source_model_type in {"gradient_boosting", "hist_gradient_boosting"}:
                for variant_index, params in enumerate(
                    calibration_model_parameter_variants(source_model_type)[:2],
                    1,
                ):
                    model_variants.append((
                        source_model_type,
                        variant_index,
                        params,
                        "calibration",
                    ))
            else:
                continue

            for model_type, variant_index, params, model_context in model_variants:
                validation_weeks = 6 if len(whitelist) == 1 else 8
                validation_profile = (
                    "live_value_miss_calibration_detection_segment_pair6w_v1"
                    if len(whitelist) == 1
                    else "live_value_miss_calibration_detection_segment_8w_v1"
                )
                spec = major_lead_spec(
                    source=source,
                    deployment_lane=lane,
                    account_focus=focus,
                    model_type=model_type,
                    lead_minutes=int(candidate.get("lead_minutes") or 15),
                    horizon_minutes=int(candidate.get("horizon_minutes") or 120),
                    instrument_subset="all",
                    instrument_whitelist=whitelist,
                    segment_filters=(
                        context.get("filters")
                        if isinstance(context.get("filters"), dict)
                        else {}
                    ),
                    specialist_profile=(
                        f"{prefix}_calibration_detection_segment_focus_"
                        f"{slug}_{compact_slug(label, max_len=50)}_"
                        f"lead{int(candidate.get('lead_minutes') or 15)}_"
                        f"event{int(candidate.get('horizon_minutes') or 120)}_"
                        f"{candidate.get('execution_policy')}_{model_type}_"
                        f"{model_context}{variant_index}"
                    ),
                    parameters=params,
                    validation_weeks=validation_weeks,
                    max_train_rows=260_000,
                    execution_policy=str(candidate.get("execution_policy") or "trailing_stop015"),
                    evaluation_stage="validation",
                    validation_profile=validation_profile,
                )
                spec["live_value_miss_context"] = {
                    "context_label": "calibration_detection_segment_focus",
                    "segment_focus": context,
                    "source_experiment_id": candidate.get("source_experiment_id", ""),
                    "source_model_type": candidate.get("source_model_type", ""),
                    "source_profile": candidate.get("source_profile", ""),
                    "source_failed_reasons": candidate.get("failed_reasons", ""),
                    "source_metrics": candidate.get("source_metrics", {}),
                    "rationale": (
                        "The source value-miss candidate passed economic gates "
                        "but failed global detection/calibration. Its scorecard "
                        "showed a strong localized segment, so this research-only "
                        "follow-up validates the narrower session/regime/family/"
                        "instrument context before any live routing decision."
                    ),
                }
                if enqueue_unique(
                    registry,
                    spec,
                    reason="live_value_miss_calibration_detection_segment_focus",
                    seen_hashes=seen,
                ):
                    count += 1
                    seeded.append(spec)
    return count, seeded


def seed_unknown_profile_followup_variants(
    registry: ModelLifecycleRegistry,
    seen: set[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    """Seed compact tech-lane research for repeated unknown-profile skips.

    Current live evidence shows the unknown-profile bucket is mostly a tech
    scout issue.  Keep this lane broker-free and whitelisted so the trainer can
    answer one narrow question: which skipped instruments deserve a future
    profile or stricter/no-trade confirmation rule?
    """
    baskets, ranked = current_unknown_profile_miss_baskets()
    if not baskets:
        return 0, []

    lane = "live_tech_champion_candidate"
    focus = "tech_live_001-001-21715580-003"
    source = "operator_live_unknown_profile_followup_v1"
    filters = {
        "regimes": ["high_vol_event", "spread_impaired"],
        "sessions": ["asia", "london", "new_york", "off_session"],
        "exclude_sessions": ["rollover"],
    }
    seeded: List[Dict[str, Any]] = []
    count = 0
    for basket_name, basket in baskets:
        pair_level_followup = len(basket) == 1
        validation_weeks = 4 if pair_level_followup else 8
        validation_profile = (
            "live_unknown_profile_pair_followup_4w_v1"
            if pair_level_followup
            else "live_unknown_profile_followup_8w_v1"
        )
        profile_suffix = "_pair4w" if pair_level_followup else ""
        for lead in [15, 30]:
            for execution_policy in ["trailing_stop012", "trailing_stop015"]:
                for model_type in ["extra_trees", "random_forest"]:
                    params = confirmation_tree_parameter_variants(model_type)[0]
                    spec = major_lead_spec(
                        source=source,
                        deployment_lane=lane,
                        account_focus=focus,
                        model_type=model_type,
                        lead_minutes=lead,
                        horizon_minutes=120,
                        instrument_subset="all",
                        instrument_whitelist=basket,
                        segment_filters=filters,
                        specialist_profile=(
                            f"tech_live_unknown_profile_{basket_name}_lead{lead}_"
                            f"event120_{execution_policy}_{model_type}{profile_suffix}"
                        ),
                        parameters=params,
                        validation_weeks=validation_weeks,
                        max_train_rows=200_000,
                        execution_policy=execution_policy,
                        evaluation_stage="validation",
                        validation_profile=validation_profile,
                    )
                    if enqueue_unique(
                        registry,
                        spec,
                        reason="live_unknown_profile_followup",
                        seen_hashes=seen,
                    ):
                        count += 1
                        seeded.append(spec)
    return count, seeded


def seed_ev_near_miss_followup_variants(
    registry: ModelLifecycleRegistry,
    seen: set[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    """Seed compact offline tests for high-value EV-gate near misses."""
    top_instruments, ranked = current_ev_near_miss_baskets(limit=4)
    if not top_instruments:
        return 0, []

    baskets: List[Tuple[str, List[str]]] = [
        ("current_ev_near_top", top_instruments),
    ]
    for instrument, _value, _count in ranked[:2]:
        baskets.append((f"current_ev_near_{instrument.lower()}", [instrument]))

    source = "operator_live_ev_near_miss_followup_v1"
    lane = "live_tech_champion_candidate"
    focus = "tech_live_001-001-21715580-003"
    filters = {
        "regimes": ["high_vol_event", "spread_impaired"],
        "sessions": ["asia", "london", "new_york", "off_session"],
        "exclude_sessions": ["rollover"],
    }
    seeded: List[Dict[str, Any]] = []
    count = 0
    for basket_name, basket in baskets:
        pair_level_followup = len(basket) == 1
        validation_weeks = 4 if pair_level_followup else 8
        validation_profile = (
            "live_ev_near_miss_pair_followup_4w_v1"
            if pair_level_followup
            else "live_ev_near_miss_followup_8w_v1"
        )
        for lead in [15, 30]:
            for model_type in ["extra_trees", "random_forest"]:
                params = confirmation_tree_parameter_variants(model_type)[0]
                spec = major_lead_spec(
                    source=source,
                    deployment_lane=lane,
                    account_focus=focus,
                    model_type=model_type,
                    lead_minutes=lead,
                    horizon_minutes=120,
                    instrument_subset="all",
                    instrument_whitelist=basket,
                    segment_filters=filters,
                    specialist_profile=(
                        f"tech_live_ev_near_miss_{basket_name}_lead{lead}_"
                        f"event120_trailing_stop012_{model_type}"
                    ),
                    parameters=params,
                    validation_weeks=validation_weeks,
                    max_train_rows=180_000,
                    execution_policy="trailing_stop012",
                    evaluation_stage="validation",
                    validation_profile=validation_profile,
                )
                if enqueue_unique(
                    registry,
                    spec,
                    reason="live_ev_near_miss_followup",
                    seen_hashes=seen,
                ):
                    count += 1
                    seeded.append(spec)
        if basket_name == "current_ev_near_top" and not pair_level_followup:
            for lead in [15, 30]:
                for model_type in ["hist_gradient_boosting", "gradient_boosting"]:
                    for variant_index, params in enumerate(
                        calibration_model_parameter_variants(model_type),
                        1,
                    ):
                        spec = major_lead_spec(
                            source=source,
                            deployment_lane=lane,
                            account_focus=focus,
                            model_type=model_type,
                            lead_minutes=lead,
                            horizon_minutes=120,
                            instrument_subset="all",
                            instrument_whitelist=basket,
                            segment_filters=filters,
                            specialist_profile=(
                                f"tech_live_ev_near_miss_calibration_{basket_name}_"
                                f"lead{lead}_event120_trailing_stop012_{model_type}_v{variant_index}"
                            ),
                            parameters=params,
                            validation_weeks=8,
                            max_train_rows=180_000,
                            execution_policy="trailing_stop012",
                            evaluation_stage="validation",
                            validation_profile="live_ev_near_miss_calibration_8w_v1",
                        )
                        if enqueue_unique(
                            registry,
                            spec,
                            reason="live_ev_near_miss_calibration_followup",
                            seen_hashes=seen,
                        ):
                            count += 1
                            seeded.append(spec)
    return count, seeded


def load_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists() or path.stat().st_size <= 0:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def read_tail_csv_rows(path: Path, max_rows: int = 50_000) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    rows: deque[Dict[str, str]] = deque(maxlen=max_rows)
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                rows.append(row)
    except Exception:
        return []
    return list(rows)


def row_float(row: Dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        value = row.get(key)
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def lead_horizon_from_target(target: str) -> Tuple[int, int] | None:
    match = re.search(r"major_event_lead_(\d+)_(\d+)", str(target or ""))
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def execution_policy_from_outcome(outcome: str) -> str:
    text = str(outcome or "").lower()
    if "trailing_stop012" in text:
        return "trailing_stop012"
    if "trailing_stop015" in text:
        return "trailing_stop015"
    if "trailing_stop020" in text:
        return "trailing_stop020"
    if "trailing_stop025" in text:
        return "trailing_stop025"
    if "fixed" in text:
        return "fixed"
    return ""


def parse_segment_filters_text(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return manager.normalized_segment_filters(value)
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return manager.normalized_segment_filters(parsed)


def current_lead_only_gate_winners(
    *,
    limit: int = 6,
    min_trades: float = 75.0,
) -> List[Dict[str, Any]]:
    """Return live validation winners that need longer/broader holdout.

    These are not promotion candidates.  They already passed the research gate
    but have too few folds/weeks to treat as robust.  The follow-up seeder below
    expands them into longer liquidity/unfiltered validations.
    """
    live_sources = {
        "operator_live_value_miss_followup_v1",
        "operator_live_unknown_profile_followup_v1",
        "operator_live_missed_spike_subset_followup_v1",
        "operator_live_ev_near_miss_followup_v1",
    }
    candidates: Dict[Tuple[str, str, str, str, Tuple[str, ...], str], Dict[str, Any]] = {}
    for row in read_tail_csv_rows(LEDGER_PATH, max_rows=5_000):
        source = str(row.get("source") or "")
        if source not in live_sources:
            continue
        if str(row.get("evaluation_stage") or "").lower() != "validation":
            continue
        if not truthy_text(row.get("gate_passed")):
            continue
        if str(row.get("status") or "").lower() != "successful":
            continue
        lead_horizon = lead_horizon_from_target(str(row.get("target") or ""))
        if not lead_horizon:
            continue
        model_type = str(row.get("model_type") or "").lower()
        if model_type not in {"extra_trees", "random_forest"}:
            continue
        whitelist = manager.normalized_instrument_whitelist(
            row.get("instrument_whitelist"),
        )
        if not whitelist:
            continue
        trades = row_float(row, "trades")
        fold_count = row_float(row, "fold_count")
        positive_weeks = row_float(row, "positive_weeks")
        mean_net = row_float(row, "mean_net_pips")
        mean_auc = row_float(row, "mean_auc")
        min_auc = row_float(row, "minimum_week_auc")
        if trades < min_trades or mean_net <= 0 or mean_auc < 0.58 or min_auc < 0.52:
            continue
        if fold_count >= 4 and positive_weeks >= 4:
            continue
        execution_policy = execution_policy_from_outcome(row.get("outcome"))
        if execution_policy not in {"trailing_stop012", "trailing_stop015", "fixed"}:
            continue
        deployment_lane = str(row.get("deployment_lane") or "")
        if deployment_lane not in {
            "live_primary_challenger_candidate",
            "live_tech_champion_candidate",
        }:
            continue
        account_focus = str(row.get("account_focus") or "")
        key = (
            source,
            deployment_lane,
            model_type,
            str(row.get("target") or ""),
            tuple(whitelist),
            execution_policy,
        )
        candidate = {
            "source": source,
            "deployment_lane": deployment_lane,
            "account_focus": account_focus,
            "model_type": model_type,
            "target": str(row.get("target") or ""),
            "lead_minutes": lead_horizon[0],
            "horizon_minutes": lead_horizon[1],
            "execution_policy": execution_policy,
            "instrument_subset": str(row.get("instrument_subset") or "all") or "all",
            "instrument_whitelist": whitelist,
            "segment_filters": parse_segment_filters_text(row.get("segment_filters")),
            "specialist_profile": str(row.get("specialist_profile") or ""),
            "experiment_id": str(row.get("experiment_id") or ""),
            "score": row_float(row, "score"),
            "trades": trades,
            "fold_count": fold_count,
            "positive_weeks": positive_weeks,
            "mean_net_pips": mean_net,
            "mean_auc": mean_auc,
            "minimum_week_auc": min_auc,
        }
        old = candidates.get(key)
        if old is None or (
            candidate["score"],
            candidate["trades"],
            candidate["mean_net_pips"],
        ) > (
            float(old.get("score") or 0.0),
            float(old.get("trades") or 0.0),
            float(old.get("mean_net_pips") or 0.0),
        ):
            candidates[key] = candidate
    ranked = sorted(
        candidates.values(),
        key=lambda row: (
            -float(row.get("score") or 0.0),
            -float(row.get("minimum_week_auc") or 0.0),
            -float(row.get("trades") or 0.0),
            str(row.get("source") or ""),
        ),
    )
    return ranked[:limit]


def seed_lead_only_robust_followup_variants(
    registry: ModelLifecycleRegistry,
    seen: set[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    """Seed longer validation for live winners blocked by sparse folds."""
    winners = current_lead_only_gate_winners()
    if not winners:
        return 0, []

    seeded: List[Dict[str, Any]] = []
    count = 0
    for winner in winners:
        original_filters = manager.normalized_segment_filters(
            winner.get("segment_filters"),
        )
        context_options: List[Tuple[str, Dict[str, Any]]] = []
        for label, filters in [
            ("original", original_filters),
            ("liquidity", {"exclude_sessions": ["rollover"]}),
            ("unfiltered", {}),
        ]:
            normalized = manager.normalized_segment_filters(filters)
            if any(
                normalized == existing_filters
                for _existing_label, existing_filters in context_options
            ):
                continue
            context_options.append((label, normalized))

        model_types = [str(winner.get("model_type") or "random_forest")]
        if model_types[0] == "random_forest":
            model_types.append("extra_trees")
        elif model_types[0] == "extra_trees":
            model_types.append("random_forest")
        model_types = [model for model in dict.fromkeys(model_types) if model in {"extra_trees", "random_forest"}]

        basket = list(winner.get("instrument_whitelist") or [])
        basket_slug = compact_slug("_".join(basket[:4]), max_len=48)
        target_slug = compact_slug(winner.get("target"), max_len=36)
        for model_type in model_types:
            params = confirmation_tree_parameter_variants(model_type)[0]
            for context_label, segment_filters in context_options:
                spec = major_lead_spec(
                    source=str(winner.get("source") or ""),
                    deployment_lane=str(winner.get("deployment_lane") or ""),
                    account_focus=str(winner.get("account_focus") or ""),
                    model_type=model_type,
                    lead_minutes=int(winner.get("lead_minutes") or 30),
                    horizon_minutes=int(winner.get("horizon_minutes") or 120),
                    instrument_subset=str(winner.get("instrument_subset") or "all"),
                    instrument_whitelist=basket,
                    segment_filters=segment_filters,
                    specialist_profile=(
                        "lead_only_robust_"
                        f"{basket_slug}_{target_slug}_"
                        f"{winner.get('execution_policy')}_{context_label}_{model_type}"
                    ),
                    parameters=params,
                    validation_weeks=12,
                    max_train_rows=340_000,
                    execution_policy=str(winner.get("execution_policy") or "trailing_stop015"),
                    evaluation_stage="validation",
                    validation_profile=f"live_lead_only_robust_12w_v1_{context_label}",
                )
                spec["lead_only_robust_followup"] = {
                    "source_experiment_id": winner.get("experiment_id", ""),
                    "source_profile": winner.get("specialist_profile", ""),
                    "source_score": winner.get("score", 0.0),
                    "source_fold_count": winner.get("fold_count", 0.0),
                    "source_positive_weeks": winner.get("positive_weeks", 0.0),
                    "source_trades": winner.get("trades", 0.0),
                    "source_mean_net_pips": winner.get("mean_net_pips", 0.0),
                    "source_mean_auc": winner.get("mean_auc", 0.0),
                    "source_minimum_week_auc": winner.get("minimum_week_auc", 0.0),
                    "context_label": context_label,
                    "rationale": (
                        "The source live follow-up passed the research gate but "
                        "had fewer than four validation folds/positive weeks. "
                        "This follow-up tests the same basket under a 12-week "
                        "holdout and broader liquidity/unfiltered contexts before "
                        "any promotion or live-routing decision."
                    ),
                }
                if enqueue_unique(
                    registry,
                    spec,
                    reason="live_lead_only_robust_followup",
                    seen_hashes=seen,
                ):
                    count += 1
                    seeded.append(spec)
    return count, seeded


def metric_execution_policies(metric: str) -> List[str]:
    """Map replay exit-proxy names to supported trainer execution policies."""
    text = str(metric or "").lower()
    if "_final_" in text:
        return ["fixed", "trailing_stop012"]
    if "_trail_" in text:
        return ["trailing_stop012", "trailing_stop015"]
    return ["trailing_stop012"]


def current_missed_spike_subset_baskets(
    *,
    limit: int = 16,
    min_rows: int = 10,
    min_exit_usd: float = 0.0,
    min_positive_rate: float = 0.45,
) -> List[Dict[str, Any]]:
    """Return compact pair/direction subset leaders from the replay report.

    Multiple entry-delay replay files are alternate scenarios, not additive
    trades.  Prefer delay-specific leaders so the follow-up queue validates a
    concrete entry timing/profile instead of double-counted aggregates.
    """
    report = load_json_file(MISSED_SPIKE_SUBSET_REPORT)
    rows = report.get("top_delay_specific_positive_subsets")
    if not isinstance(rows, list) or not rows:
        rows = report.get("top_positive_subsets")
    if not isinstance(rows, list):
        return []
    leaders: List[Dict[str, Any]] = []
    seen_keys: set[Tuple[str, str, str]] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        if int(float(row.get("rows") or 0)) < min_rows:
            continue
        if float(row.get("best_exit_usd_sum") or 0.0) <= min_exit_usd:
            continue
        if float(row.get("best_exit_pos_rate") or 0.0) < min_positive_rate:
            continue
        key_text = str(row.get("key") or "")
        parts = key_text.split("|")
        delay = ""
        instrument = ""
        direction = ""
        if len(parts) >= 3 and parts[0].isdigit():
            delay, instrument, direction = parts[0], parts[1], parts[2]
        elif len(parts) >= 2:
            instrument, direction = parts[0], parts[1]
        else:
            continue
        instrument = instrument.strip().upper().replace("/", "_").replace("-", "_")
        direction = direction.strip().upper()
        if not instrument or direction not in {"LONG", "SHORT"}:
            continue
        leader_key = (delay or "any", instrument, direction)
        if leader_key in seen_keys:
            continue
        seen_keys.add(leader_key)
        leaders.append({
            "delay": delay,
            "instrument": instrument,
            "direction": direction,
            "rows": int(float(row.get("rows") or 0)),
            "best_exit_metric": str(row.get("best_exit_metric") or ""),
            "best_exit_usd_sum": float(row.get("best_exit_usd_sum") or 0.0),
            "best_exit_pos_rate": float(row.get("best_exit_pos_rate") or 0.0),
            "fresh_mfe_120m_usd_sum": float(row.get("fresh_mfe_120m_usd_sum") or 0.0),
            "move_to_spread_ratio_mean": float(row.get("move_to_spread_ratio_mean") or 0.0),
        })
        if len(leaders) >= limit:
            break
    return leaders


def current_missed_spike_capture_gap_baskets(
    *,
    limit: int = 6,
    min_rows: int = 100,
    min_instruments: int = 2,
    min_exit_usd: float = 2.0,
    min_positive_rate: float = 0.60,
) -> List[Dict[str, Any]]:
    """Return broad replay-capture groups with enough rows for folds.

    Pair/direction replay leaders are useful, but the latest live-week replay
    showed that singletons such as CHF_ZAR can still fail purged weekly fold
    construction.  The capture-gap report has broader direction/quote/family
    groups that preserve the useful scout lesson while supplying more
    cross-pair samples for the trainer.
    """
    report = load_json_file(MISSED_SPIKE_CAPTURE_GAP_REPORT)
    rows: List[Dict[str, Any]] = []
    for key in ["top_positive_capture_groups", "top_capture_gap_groups"]:
        value = report.get(key)
        if isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, dict))
    if not rows:
        return []

    leaders: List[Dict[str, Any]] = []
    seen: set[Tuple[str, Tuple[str, ...], str, str]] = set()
    for row in rows:
        row_count = int(float(row.get("count") or 0))
        instrument_count = int(float(row.get("instrument_count") or 0))
        best_exit_usd = float(row.get("best_exit_usd_sum") or 0.0)
        pos_rate = float(row.get("best_exit_pos_rate") or 0.0)
        if (
            row_count < min_rows
            or instrument_count < min_instruments
            or best_exit_usd <= min_exit_usd
            or pos_rate < min_positive_rate
        ):
            continue
        sample_text = str(row.get("sample_instruments") or "")
        instruments = [
            item.strip().upper().replace("/", "_").replace("-", "_")
            for item in sample_text.split(",")
            if item.strip()
        ][:8]
        instruments = list(dict.fromkeys(instruments))
        if len(instruments) < min_instruments:
            continue

        group = str(row.get("group") or "capture").strip().lower()
        key_text = str(row.get("key") or "").strip()
        key_parts = [part.strip().upper() for part in key_text.split("|") if part.strip()]
        direction = "MIXED"
        delay = "mixed"
        if key_parts and key_parts[0].isdigit():
            delay = key_parts[0]
        for part in key_parts:
            if part in {"LONG", "SHORT"}:
                direction = part
                break
        if group == "direction" and key_text.upper() in {"LONG", "SHORT"}:
            direction = key_text.upper()

        dedupe_key = (group, tuple(sorted(instruments)), direction, delay)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        leaders.append({
            "group": group,
            "key": key_text,
            "delay": delay,
            "direction": direction,
            "instruments": instruments,
            "rows": row_count,
            "instrument_count": instrument_count,
            "best_exit_metric": str(row.get("top_exit_key") or "best_exit"),
            "best_exit_usd_sum": best_exit_usd,
            "best_exit_pos_rate": pos_rate,
            "fresh_mfe_60m_usd_sum": float(row.get("fresh_mfe_60m_usd_sum") or 0.0),
            "capture_gap_60m_usd_sum": float(row.get("capture_gap_60m_usd_sum") or 0.0),
            "best_exit_to_mfe60_ratio": float(row.get("best_exit_to_mfe60_ratio") or 0.0),
        })
        if len(leaders) >= limit:
            break
    return leaders


def current_localized_cluster_baskets(
    *,
    limit: int = 3,
    min_cluster: int = 4,
    min_direction_cluster: int = 4,
    min_ratio: float = 1.35,
    min_score: float = 80.0,
) -> List[Dict[str, Any]]:
    """Return compact same-currency cluster rejects for offline validation.

    These are advisor/training candidates only.  The live primary scout still
    requires the broader strict cluster gate; this seeder asks the trainer
    whether localized baskets such as ZAR/TRY/MXN have reusable lead signal.
    """
    rows = read_tail_csv_rows(PRIMARY_SCOUT_AUDIT_PATH)
    if not rows:
        return []

    groups: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        stamp = str(row.get("time_utc") or "")[:19]
        if stamp:
            groups.setdefault(stamp, []).append(row)

    candidates: List[Dict[str, Any]] = []
    for stamp, group_rows in groups.items():
        currency_instruments: Dict[str, set[str]] = {}
        currency_direction_instruments: Dict[Tuple[str, str], set[str]] = {}
        for row in group_rows:
            instrument = str(row.get("instrument") or "").upper().replace("/", "_")
            if "_" not in instrument:
                continue
            direction = str(row.get("direction") or "").upper()
            base, quote = instrument.split("_", 1)
            for currency in {base, quote} & LOCALIZED_CLUSTER_CURRENCIES:
                currency_instruments.setdefault(currency, set()).add(instrument)
                if direction in {"LONG", "SHORT"}:
                    currency_direction_instruments.setdefault((currency, direction), set()).add(instrument)

        for row in group_rows:
            if str(row.get("decision_stage") or "").strip() != "strict_pressure_gate":
                continue
            if str(row.get("status") or "").strip().lower() not in {"skipped", "rejected", "blocked"}:
                continue
            reason = str(row.get("reject_reason") or row.get("reason") or "")
            if "cluster instruments" not in reason:
                continue
            instrument = str(row.get("instrument") or "").upper().replace("/", "_")
            direction = str(row.get("direction") or "").upper()
            if "_" not in instrument or direction not in {"LONG", "SHORT"}:
                continue
            ratio = row_float(row, "move_to_spread_ratio")
            score = row_float(row, "pressure_score")
            if ratio < min_ratio or score < min_score:
                continue
            base, quote = instrument.split("_", 1)
            best_currency = ""
            best_cluster = 0
            best_direction_cluster = 0
            for currency in {base, quote} & LOCALIZED_CLUSTER_CURRENCIES:
                cluster_count = len(currency_instruments.get(currency, set()))
                direction_count = len(currency_direction_instruments.get((currency, direction), set()))
                if (direction_count, cluster_count) > (best_direction_cluster, best_cluster):
                    best_currency = currency
                    best_cluster = cluster_count
                    best_direction_cluster = direction_count
            if (
                best_currency
                and best_cluster >= min_cluster
                and best_direction_cluster >= min_direction_cluster
            ):
                candidates.append({
                    "time_utc": row.get("time_utc") or stamp,
                    "instrument": instrument,
                    "direction": direction,
                    "currency": best_currency,
                    "cluster": best_cluster,
                    "direction_cluster": best_direction_cluster,
                    "net_pips": row_float(row, "net_pips"),
                    "ratio": ratio,
                    "score": score,
                })

    aggregated: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for candidate in candidates:
        key = (str(candidate["currency"]), str(candidate["direction"]))
        bucket = aggregated.setdefault(
            key,
            {
                "currency": candidate["currency"],
                "direction": candidate["direction"],
                "rows": 0,
                "instrument_counts": {},
                "total_abs_net_pips": 0.0,
                "max_abs_net_pips": 0.0,
                "ratio_sum": 0.0,
                "max_score": 0.0,
                "latest_time_utc": "",
                "examples": [],
            },
        )
        instrument = str(candidate["instrument"])
        bucket["rows"] += 1
        bucket["instrument_counts"][instrument] = bucket["instrument_counts"].get(instrument, 0) + 1
        abs_net = abs(float(candidate.get("net_pips") or 0.0))
        bucket["total_abs_net_pips"] += abs_net
        bucket["max_abs_net_pips"] = max(float(bucket["max_abs_net_pips"]), abs_net)
        bucket["ratio_sum"] += float(candidate.get("ratio") or 0.0)
        bucket["max_score"] = max(float(bucket["max_score"]), float(candidate.get("score") or 0.0))
        bucket["latest_time_utc"] = max(str(bucket.get("latest_time_utc") or ""), str(candidate.get("time_utc") or ""))
        if len(bucket["examples"]) < 8:
            bucket["examples"].append(candidate)

    leaders: List[Dict[str, Any]] = []
    for bucket in aggregated.values():
        instruments = sorted(
            bucket["instrument_counts"],
            key=lambda inst: (-int(bucket["instrument_counts"][inst]), inst),
        )
        if len(instruments) < min_direction_cluster:
            continue
        rows_count = int(bucket.get("rows") or 0)
        leaders.append({
            "currency": bucket["currency"],
            "direction": bucket["direction"],
            "instruments": instruments[:8],
            "rows": rows_count,
            "total_abs_net_pips": float(bucket.get("total_abs_net_pips") or 0.0),
            "max_abs_net_pips": float(bucket.get("max_abs_net_pips") or 0.0),
            "avg_ratio": (
                float(bucket.get("ratio_sum") or 0.0) / rows_count
                if rows_count > 0
                else 0.0
            ),
            "max_score": float(bucket.get("max_score") or 0.0),
            "latest_time_utc": bucket.get("latest_time_utc") or "",
            "examples": bucket.get("examples") or [],
        })
    return sorted(
        leaders,
        key=lambda row: (
            -float(row.get("total_abs_net_pips") or 0.0),
            -float(row.get("avg_ratio") or 0.0),
            str(row.get("currency") or ""),
        ),
    )[:limit]


def current_strict_pressure_pass_baskets(
    *,
    limit: int = 5,
    min_score: float = 80.0,
    min_ratio: float = 2.0,
    min_expected_usd: float = 0.075,
    max_age_hours: float = 36.0,
) -> List[Dict[str, Any]]:
    """Return current strict pre-spike pressure passes for offline validation.

    Primary scout can mark a strict pressure signal as passed while later
    execution gates still block order attempts.  These are exactly the cases
    worth validating offline before changing live thresholds: the broad move
    context is strong, but we need held-out evidence that those entries are
    worth taking.
    """
    rows = read_tail_csv_rows(PRIMARY_SCOUT_AUDIT_PATH)
    cutoff_iso = (
        datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    ).isoformat()
    passed: List[Dict[str, Any]] = []
    for row in rows:
        if str(row.get("time_utc") or "") < cutoff_iso:
            continue
        if str(row.get("decision_stage") or "").strip() != "strict_pressure_gate":
            continue
        if str(row.get("status") or "").strip().lower() != "passed":
            continue
        instrument = str(row.get("instrument") or "").upper().replace("/", "_").replace("-", "_")
        direction = str(row.get("direction") or "").upper().strip()
        if "_" not in instrument or direction not in {"LONG", "SHORT"}:
            continue
        score = row_float(row, "pressure_score")
        ratio = row_float(row, "move_to_spread_ratio")
        expected_usd = row_float(row, "value_expected_usd")
        if score < min_score or ratio < min_ratio or expected_usd < min_expected_usd:
            continue
        passed.append({
            "time_utc": row.get("time_utc") or "",
            "instrument": instrument,
            "direction": direction,
            "net_pips": row_float(row, "net_pips"),
            "ratio": ratio,
            "score": score,
            "expected_usd": expected_usd,
            "return_pct": row_float(row, "value_return_pct"),
            "net_after_spread_pips": row_float(row, "value_net_after_spread_pips"),
            "cluster": row_float(row, "pressure_cluster_instruments"),
            "direction_cluster": row_float(row, "pressure_cluster_direction_instruments"),
        })
    if not passed:
        return []

    leaders: List[Dict[str, Any]] = []
    by_direction: Dict[str, Dict[str, Any]] = {}
    by_instrument_direction: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in passed:
        direction = str(row["direction"])
        instrument = str(row["instrument"])
        for key, bucket in [
            (
                direction,
                by_direction.setdefault(
                    direction,
                    {
                        "basket_kind": "direction_basket",
                        "direction": direction,
                        "instrument_counts": {},
                        "rows": 0,
                        "expected_usd": 0.0,
                        "net_pips": 0.0,
                        "ratio_sum": 0.0,
                        "max_score": 0.0,
                        "latest_time_utc": "",
                        "examples": [],
                    },
                ),
            ),
            (
                f"{instrument}:{direction}",
                by_instrument_direction.setdefault(
                    (instrument, direction),
                    {
                        "basket_kind": "single_pair",
                        "direction": direction,
                        "instrument_counts": {instrument: 0},
                        "rows": 0,
                        "expected_usd": 0.0,
                        "net_pips": 0.0,
                        "ratio_sum": 0.0,
                        "max_score": 0.0,
                        "latest_time_utc": "",
                        "examples": [],
                    },
                ),
            ),
        ]:
            _ = key
            bucket["rows"] += 1
            bucket["instrument_counts"][instrument] = bucket["instrument_counts"].get(instrument, 0) + 1
            bucket["expected_usd"] += float(row.get("expected_usd") or 0.0)
            bucket["net_pips"] += abs(float(row.get("net_pips") or 0.0))
            bucket["ratio_sum"] += float(row.get("ratio") or 0.0)
            bucket["max_score"] = max(float(bucket.get("max_score") or 0.0), float(row.get("score") or 0.0))
            bucket["latest_time_utc"] = max(str(bucket.get("latest_time_utc") or ""), str(row.get("time_utc") or ""))
            if len(bucket["examples"]) < 8:
                bucket["examples"].append(row)

    for bucket in list(by_direction.values()) + list(by_instrument_direction.values()):
        instruments = sorted(
            bucket["instrument_counts"],
            key=lambda inst: (-int(bucket["instrument_counts"][inst]), inst),
        )
        rows_count = int(bucket.get("rows") or 0)
        if bucket.get("basket_kind") == "direction_basket" and len(instruments) < 2:
            continue
        if bucket.get("basket_kind") == "single_pair" and rows_count < 3:
            # Initial current-window single-pair strict-pressure validations
            # are already sparse/unstable.  Keep the research focused on
            # basket-level and directional precursor checks unless a pair
            # repeats enough times to be worth a standalone test.
            continue
        leaders.append({
            "basket_kind": bucket.get("basket_kind"),
            "direction": bucket.get("direction"),
            "instruments": instruments[:6],
            "rows": rows_count,
            "expected_usd": float(bucket.get("expected_usd") or 0.0),
            "total_abs_net_pips": float(bucket.get("net_pips") or 0.0),
            "avg_ratio": (
                float(bucket.get("ratio_sum") or 0.0) / rows_count
                if rows_count > 0
                else 0.0
            ),
            "max_score": float(bucket.get("max_score") or 0.0),
            "latest_time_utc": bucket.get("latest_time_utc") or "",
            "examples": bucket.get("examples") or [],
        })
    return sorted(
        leaders,
        key=lambda row: (
            -float(row.get("expected_usd") or 0.0),
            -float(row.get("total_abs_net_pips") or 0.0),
            str(row.get("basket_kind") or ""),
        ),
    )[:limit]


def current_successful_missed_spike_segment_leaders(
    *,
    limit: int = 4,
    min_trades: int = 8,
    min_profit_factor: float = 1.2,
    min_mean_net: float = 0.05,
) -> List[Dict[str, Any]]:
    """Return successful missed-spike segment leaders for focused follow-up.

    These are not promotion instructions.  They seed additional research-only
    validation around profiles that already passed a segment gate, especially
    when stricter replay/robust slices are too sparse for rolling folds.
    """
    report = load_json_file(TRAINER_REPORTING_EXTENSIONS_PATH)
    segment_leaders = report.get("segment_leaders") if isinstance(report.get("segment_leaders"), dict) else {}
    top_by_scorecard = (
        segment_leaders.get("top_by_scorecard")
        if isinstance(segment_leaders.get("top_by_scorecard"), dict)
        else {}
    )
    best_by_profile: Dict[str, Dict[str, Any]] = {}
    for rows in top_by_scorecard.values():
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            if row.get("source") != "operator_live_missed_spike_subset_followup_v1":
                continue
            if str(row.get("status") or "").lower() != "successful" and row.get("gate_passed") is not True:
                continue
            if row_float(row, "trades") < min_trades:
                continue
            if row_float(row, "profit_factor") < min_profit_factor:
                continue
            if row_float(row, "mean_net") < min_mean_net:
                continue
            whitelist_text = str(row.get("instrument_whitelist") or "").strip()
            if not whitelist_text:
                continue
            instruments = [
                item.strip().upper().replace("/", "_").replace("-", "_")
                for item in whitelist_text.split(",")
                if item.strip()
            ]
            if not instruments:
                continue
            profile = str(row.get("specialist_profile") or row.get("experiment_id") or "")
            if not profile:
                continue
            direction = "mixed"
            profile_lower = profile.lower()
            if "_short_" in profile_lower or "short_" in profile_lower:
                direction = "short"
            elif "_long_" in profile_lower or "long_" in profile_lower:
                direction = "long"
            compact = dict(row)
            compact["instruments"] = instruments[:6]
            compact["direction"] = direction
            current = best_by_profile.get(profile)
            score = row_float(compact, "leader_score")
            current_score = row_float(current, "leader_score") if current else -1.0
            if current is None or score > current_score:
                best_by_profile[profile] = compact

    return sorted(
        best_by_profile.values(),
        key=lambda row: (
            -row_float(row, "leader_score"),
            -row_float(row, "total_net"),
            -row_float(row, "mean_auc"),
        ),
    )[:limit]


def repeated_insufficient_fold_singletons(
    *,
    source: str,
    min_failures: int = 4,
) -> set[str]:
    """Return single-pair subset specs that repeatedly cannot form folds.

    Missed-spike subsets can look attractive in replay but still be too sparse
    for purged weekly validation.  Once a standalone pair has repeatedly failed
    fold construction and has no standalone success, keep it in combo baskets
    but stop reseeding the same single-pair research specs.
    """
    failures: Dict[str, int] = {}
    successes: set[str] = set()
    for row in read_ledger_rows():
        if str(row.get("source") or "") != source:
            continue
        whitelist_text = str(row.get("instrument_whitelist") or "").strip()
        if not whitelist_text:
            continue
        if whitelist_text.startswith("["):
            try:
                whitelist = json.loads(whitelist_text)
            except json.JSONDecodeError:
                whitelist = [whitelist_text]
        else:
            whitelist = [part for part in whitelist_text.split(",") if part]
        pairs = [
            str(pair).strip().upper().replace("/", "_").replace("-", "_")
            for pair in whitelist
            if str(pair).strip()
        ]
        if len(pairs) != 1:
            continue
        pair = pairs[0]
        if str(row.get("status") or "") == "successful":
            successes.add(pair)
            continue
        error_text = str(row.get("error") or "")
        if (
            "InsufficientRollingFolds" in error_text
            or "No valid purged rolling weekly folds" in error_text
            or "No valid purged two-stage rolling folds" in error_text
        ):
            failures[pair] = failures.get(pair, 0) + 1
    return {
        pair
        for pair, count in failures.items()
        if count >= min_failures and pair not in successes
    }


def seed_missed_spike_subset_followup_variants(
    registry: ModelLifecycleRegistry,
    seen: set[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    """Seed held-out validation specs for positive missed-spike replay subsets."""
    leaders = current_missed_spike_subset_baskets()
    if not leaders:
        return 0, []

    source = "operator_live_missed_spike_subset_followup_v1"
    lane = "live_primary_challenger_candidate"
    focus = "primary_live_001-001-21715580-002"
    fold_blocked_singletons = repeated_insufficient_fold_singletons(
        source=source,
    )
    event_filters = {
        "regimes": ["high_vol_event", "spread_impaired"],
        "sessions": ["asia", "london", "new_york", "off_session"],
        "exclude_sessions": ["rollover"],
    }
    validation_contexts: List[Tuple[str, Dict[str, Any]]] = [
        ("event", event_filters),
        ("liquidity", {"exclude_sessions": ["rollover"]}),
        ("unfiltered", {}),
    ]
    seeded: List[Dict[str, Any]] = []
    count = 0

    def enqueue_subset_spec(
        *,
        basket_name: str,
        basket: List[str],
        direction: str,
        delay: str,
        metric_slug: str,
        lead: int,
        execution_policy: str,
        model_type: str,
        evidence: Dict[str, Any],
        validation_weeks: int,
        validation_profile: str,
        filter_label: str,
        segment_filters: Dict[str, Any],
    ) -> None:
        nonlocal count
        params = confirmation_tree_parameter_variants(model_type)[0]
        spec = major_lead_spec(
            source=source,
            deployment_lane=lane,
            account_focus=focus,
            model_type=model_type,
            lead_minutes=lead,
            horizon_minutes=120,
            instrument_subset="all",
            instrument_whitelist=basket,
            segment_filters=segment_filters,
            specialist_profile=(
                f"primary_missed_spike_subset_{basket_name}_{direction}_"
                f"delay{delay}_{metric_slug}_{filter_label}_lead{lead}_event120_"
                f"{execution_policy}_{model_type}"
            ),
            parameters=params,
            validation_weeks=validation_weeks,
            max_train_rows=220_000 if len(basket) > 1 else 200_000,
            execution_policy=execution_policy,
            evaluation_stage="validation",
            validation_profile=f"{validation_profile}_{filter_label}",
        )
        spec["subset_evidence"] = dict(evidence)
        spec["subset_validation_context"] = {
            "filter_label": filter_label,
            "segment_filters": segment_filters,
            "rationale": (
                "Missed-spike replay positives are sparse under strict event "
                "filters.  Run parallel held-out validations with the original "
                "event filter, rollover-only liquidity filter, and an unfiltered "
                "context before considering any live promotion."
            ),
        }
        if enqueue_unique(
            registry,
            spec,
            reason="live_missed_spike_subset_followup",
            seen_hashes=seen,
        ):
            count += 1
            seeded.append(spec)

    for leader in leaders:
        instrument = str(leader["instrument"])
        direction = str(leader["direction"]).lower()
        if int(leader.get("rows") or 0) < 50:
            # Low-row pair-level positives are useful as ingredients for a
            # broader basket, but they routinely fail purged rolling-fold
            # validation on their own.  Avoid adding more sparse single-pair
            # specs; validate them inside combined baskets below.
            continue
        if instrument in fold_blocked_singletons:
            # This pair had enough replay rows to be tempting but repeatedly
            # failed standalone weekly fold construction.  Keep it in combined
            # baskets below where cross-pair samples can form proper folds.
            continue
        delay = str(leader.get("delay") or "any")
        metric_slug = re.sub(r"[^a-z0-9]+", "_", str(leader.get("best_exit_metric") or "exit").lower()).strip("_")
        policies = metric_execution_policies(str(leader.get("best_exit_metric") or ""))
        for lead in [15, 30]:
            for execution_policy in policies:
                for model_type in ["extra_trees", "random_forest"]:
                    for filter_label, segment_filters in validation_contexts:
                        enqueue_subset_spec(
                            basket_name=instrument.lower(),
                            basket=[instrument],
                            direction=direction,
                            delay=delay,
                            metric_slug=metric_slug,
                            lead=lead,
                            execution_policy=execution_policy,
                            model_type=model_type,
                            evidence=leader,
                            validation_weeks=6,
                            validation_profile="missed_spike_subset_pair_followup_6w_v1",
                            filter_label=filter_label,
                            segment_filters=segment_filters,
                        )
    for leader in current_successful_missed_spike_segment_leaders():
        instruments = [
            str(item).upper().replace("/", "_").replace("-", "_")
            for item in leader.get("instruments", [])
            if str(item).strip()
        ][:6]
        if not instruments:
            continue
        direction = str(leader.get("direction") or "mixed").lower()
        scorecard = re.sub(
            r"[^a-z0-9]+",
            "_",
            str(leader.get("scorecard") or "segment").lower(),
        ).strip("_") or "segment"
        segment = re.sub(
            r"[^a-z0-9]+",
            "_",
            str(leader.get("segment") or "leader").lower(),
        ).strip("_") or "leader"
        basket_name = "_".join(instrument.lower() for instrument in instruments)
        execution_policy = str(leader.get("execution_policy") or "trailing_stop015")
        if execution_policy not in {"fixed", "trailing_stop012", "trailing_stop015"}:
            execution_policy = "trailing_stop015"
        model_types = list(dict.fromkeys([
            str(leader.get("model_type") or "random_forest"),
            "random_forest",
        ]))
        evidence = {
            "successful_segment_leader": leader,
            "rationale": (
                "A missed-spike segment leader already passed validation, while "
                "stricter robust slices are sparse. Re-test the same profile in "
                "lightweight liquidity/unfiltered contexts before considering "
                "promotion or live-gate changes."
            ),
        }
        for model_type in model_types:
            if model_type not in {"extra_trees", "random_forest"}:
                continue
            for filter_label, segment_filters in [
                ("successful_segment_liquidity", {"exclude_sessions": ["rollover"]}),
                ("successful_segment_unfiltered", {}),
            ]:
                enqueue_subset_spec(
                    basket_name=f"successful_segment_{basket_name}_{scorecard}_{segment}",
                    basket=instruments,
                    direction=direction,
                    delay="leader",
                    metric_slug="validated_segment",
                    lead=30,
                    execution_policy=execution_policy,
                    model_type=model_type,
                    evidence=evidence,
                    validation_weeks=6,
                    validation_profile="missed_spike_successful_segment_followup_6w_v1",
                    filter_label=filter_label,
                    segment_filters=segment_filters,
                )
    unique_positive = list(dict.fromkeys(str(row["instrument"]) for row in leaders))
    combined_baskets: list[tuple[str, list[str]]] = []
    core = unique_positive[:4]
    broad = unique_positive[:6]
    if len(core) >= 2:
        combined_baskets.append(("core", core))
    if len(broad) > len(core):
        combined_baskets.append(("broad", broad))

    for combo_label, combined in combined_baskets:
        evidence = {
            "leaders": [
                row
                for row in leaders
                if str(row.get("instrument") or "") in set(combined)
            ][:16],
            "instrument": "+".join(combined),
            "direction": "MIXED",
            "delay": "mixed",
            "rows": sum(
                int(row.get("rows") or 0)
                for row in leaders
                if str(row.get("instrument") or "") in set(combined)
            ),
            "best_exit_usd_sum": sum(
                float(row.get("best_exit_usd_sum") or 0.0)
                for row in leaders
                if str(row.get("instrument") or "") in set(combined)
            ),
            "fresh_mfe_120m_usd_sum": sum(
                float(row.get("fresh_mfe_120m_usd_sum") or 0.0)
                for row in leaders
                if str(row.get("instrument") or "") in set(combined)
            ),
        }
        basket_name = "_".join(instrument.lower() for instrument in combined[:6])
        for lead in [15, 30]:
            for execution_policy in ["fixed", "trailing_stop012", "trailing_stop015"]:
                for model_type in ["extra_trees", "random_forest"]:
                    for filter_label, segment_filters in validation_contexts:
                        enqueue_subset_spec(
                            basket_name=f"combo_{combo_label}_{basket_name}",
                            basket=combined,
                            direction="mixed",
                            delay="mixed",
                            metric_slug="top_subset_combo",
                            lead=lead,
                            execution_policy=execution_policy,
                            model_type=model_type,
                            evidence=evidence,
                            validation_weeks=8,
                            validation_profile="missed_spike_subset_combo_followup_8w_v1",
                            filter_label=filter_label,
                            segment_filters=segment_filters,
                        )

    for leader in current_missed_spike_capture_gap_baskets():
        instruments = [
            str(instrument).upper().replace("/", "_").replace("-", "_")
            for instrument in leader.get("instruments", [])
            if str(instrument).strip()
        ][:8]
        if len(instruments) < 2:
            continue
        direction = str(leader.get("direction") or "mixed").lower()
        metric_slug = re.sub(
            r"[^a-z0-9]+",
            "_",
            str(leader.get("best_exit_metric") or "capture_gap").lower(),
        ).strip("_") or "capture_gap"
        group_slug = re.sub(
            r"[^a-z0-9]+",
            "_",
            str(leader.get("group") or "capture").lower(),
        ).strip("_") or "capture"
        key_slug = re.sub(
            r"[^a-z0-9]+",
            "_",
            str(leader.get("key") or "broad").lower(),
        ).strip("_") or "broad"
        basket_name = "_".join(instrument.lower() for instrument in instruments[:6])
        evidence = {
            "capture_gap_leader": leader,
            "rationale": (
                "Fresh missed-spike replay showed single-pair validation can "
                "be too sparse, while broad LONG/ZAR/volatile capture groups "
                "retain positive diagnostic exits. Validate these broader "
                "capture-gap baskets before any live scout gate change."
            ),
        }
        policies = metric_execution_policies(str(leader.get("best_exit_metric") or ""))
        for lead in [15, 30]:
            for execution_policy in policies:
                for model_type in ["extra_trees", "random_forest"]:
                    for filter_label, segment_filters in validation_contexts:
                        enqueue_subset_spec(
                            basket_name=f"capture_gap_{group_slug}_{key_slug}_{basket_name}",
                            basket=instruments,
                            direction=direction,
                            delay=str(leader.get("delay") or "mixed"),
                            metric_slug=metric_slug,
                            lead=lead,
                            execution_policy=execution_policy,
                            model_type=model_type,
                            evidence=evidence,
                            validation_weeks=8,
                            validation_profile="missed_spike_capture_gap_broad_followup_8w_v1",
                            filter_label=filter_label,
                            segment_filters=segment_filters,
                        )
        target_side = "long" if direction == "long" else "short" if direction == "short" else ""
        if not target_side:
            continue
        for horizon in [60, 120]:
            for model_type in ["extra_trees", "random_forest"]:
                for filter_label, segment_filters in validation_contexts:
                    outcome_variants = [
                        ("fixed", f"{target_side}_net_atr_{horizon}", ""),
                        ("curve", f"{target_side}_curve_net_atr_{horizon}", "curve"),
                    ]
                    for outcome_label, outcome, execution_policy in outcome_variants:
                        profile_outcome_suffix = "" if outcome_label == "fixed" else "_curve"
                        spec = precursor_spec(
                            source=source,
                            deployment_lane=lane,
                            account_focus=focus,
                            model_type=model_type,
                            target=f"profitable_{target_side}_move_{horizon}",
                            outcome=outcome,
                            direction_target="",
                            instrument_subset="all",
                            segment_filters=segment_filters,
                            specialist_profile=(
                                f"primary_missed_spike_subset_capture_gap_"
                                f"directional_precursor_{group_slug}_{key_slug}_{basket_name}_"
                                f"{target_side}_{filter_label}_{horizon}m"
                                f"{profile_outcome_suffix}_{model_type}"
                            ),
                            parameters=confirmation_tree_parameter_variants(model_type)[0],
                            validation_weeks=8,
                            max_train_rows=220_000,
                            evaluation_stage="validation",
                            validation_profile=(
                                "missed_spike_capture_gap_directional_precursor_"
                                f"8w_v1_{filter_label}_{outcome_label}"
                            ),
                        )
                        spec["execution_policy"] = execution_policy
                        spec["instrument_whitelist"] = instruments
                        spec["subset_evidence"] = evidence
                        spec["subset_validation_context"] = {
                            "filter_label": filter_label,
                            "segment_filters": segment_filters,
                            "directional_precursor": True,
                            "target_side": target_side,
                            "outcome_label": outcome_label,
                            "rationale": (
                                "Broad capture-gap replay groups carry an explicit "
                                "LONG/SHORT side. Generic major-event tests can fail "
                                "direction accuracy even when the side-specific "
                                "continuation has value, so validate the same basket "
                                "against profitable-move precursor labels. Curve "
                                "outcomes are included because the fresh replay "
                                "showed positive MFE/capture gap despite weak fixed "
                                "hold follow-through."
                            ),
                        }
                        if enqueue_unique(
                            registry,
                            spec,
                            reason=(
                                "live_missed_spike_capture_gap_directional_"
                                f"precursor_{outcome_label}"
                            ),
                            seen_hashes=seen,
                        ):
                            count += 1
                            seeded.append(spec)

    localized_contexts: List[Tuple[str, Dict[str, Any]]] = [
        ("localized_liquidity", {"exclude_sessions": ["rollover"]}),
        (
            "localized_event",
            {
                "regimes": ["high_vol_event", "spread_impaired"],
                "exclude_sessions": ["rollover"],
            },
        ),
    ]
    for leader in current_localized_cluster_baskets():
        instruments = [
            str(instrument).upper().replace("/", "_").replace("-", "_")
            for instrument in leader.get("instruments", [])
            if str(instrument).strip()
        ][:6]
        if len(instruments) < 4:
            continue
        direction = str(leader.get("direction") or "mixed").lower()
        currency = str(leader.get("currency") or "local").lower()
        basket_name = "_".join(instrument.lower() for instrument in instruments)
        evidence = {
            "localized_cluster": leader,
            "rationale": (
                "Primary scout strict broad-cluster gate rejected a same-currency "
                "localized basket. Validate the basket offline before any live "
                "gate or execution change."
            ),
        }
        for lead in [15, 30]:
            for execution_policy in ["trailing_stop012", "trailing_stop015"]:
                for model_type in ["extra_trees", "random_forest"]:
                    for filter_label, segment_filters in localized_contexts:
                        enqueue_subset_spec(
                            basket_name=f"localized_{currency}_{basket_name}",
                            basket=instruments,
                            direction=direction,
                            delay="live",
                            metric_slug="localized_cluster",
                            lead=lead,
                            execution_policy=execution_policy,
                            model_type=model_type,
                            evidence=evidence,
                            validation_weeks=8,
                            validation_profile="missed_spike_localized_cluster_followup_8w_v1",
                            filter_label=filter_label,
                            segment_filters=segment_filters,
                        )
        target_side = "long" if direction == "long" else "short" if direction == "short" else ""
        if not target_side:
            continue
        for horizon in [60, 120]:
            for model_type in ["extra_trees", "random_forest"]:
                for filter_label, segment_filters in localized_contexts:
                    spec = precursor_spec(
                        source=source,
                        deployment_lane=lane,
                        account_focus=focus,
                        model_type=model_type,
                        target=f"profitable_{target_side}_move_{horizon}",
                        outcome=f"{target_side}_net_atr_{horizon}",
                        direction_target="",
                        instrument_subset="all",
                        segment_filters=segment_filters,
                        specialist_profile=(
                            f"primary_missed_spike_subset_localized_cluster_"
                            f"directional_precursor_{currency}_{basket_name}_"
                            f"{target_side}_{filter_label}_{horizon}m_{model_type}"
                        ),
                        parameters=confirmation_tree_parameter_variants(model_type)[0],
                        validation_weeks=8,
                        max_train_rows=220_000,
                        evaluation_stage="validation",
                        validation_profile=(
                            "missed_spike_localized_cluster_directional_"
                            f"precursor_8w_v1_{filter_label}"
                        ),
                    )
                    spec["instrument_whitelist"] = instruments
                    spec["subset_evidence"] = evidence
                    spec["subset_validation_context"] = {
                        "filter_label": filter_label,
                        "segment_filters": segment_filters,
                        "directional_precursor": True,
                        "target_side": target_side,
                        "rationale": (
                            "The generic localized-cluster major-event tests "
                            "do not encode the observed LONG/SHORT basket "
                            "direction. This compact follow-up validates the "
                            "same basket against existing direction-specific "
                            "profitable-move precursor labels."
                        ),
                    }
                    if enqueue_unique(
                        registry,
                        spec,
                        reason="live_missed_spike_localized_directional_precursor",
                        seen_hashes=seen,
                    ):
                        count += 1
                        seeded.append(spec)

    strict_pressure_contexts: List[Tuple[str, Dict[str, Any]]] = [
        ("strict_pressure_liquidity", {"exclude_sessions": ["rollover"]}),
        (
            "strict_pressure_event",
            {
                "regimes": ["high_vol_event", "spread_impaired"],
                "exclude_sessions": ["rollover"],
            },
        ),
    ]
    for leader in current_strict_pressure_pass_baskets():
        instruments = [
            str(instrument).upper().replace("/", "_").replace("-", "_")
            for instrument in leader.get("instruments", [])
            if str(instrument).strip()
        ][:6]
        if not instruments:
            continue
        direction = str(leader.get("direction") or "mixed").lower()
        target_side = "long" if direction == "long" else "short" if direction == "short" else ""
        basket_kind = re.sub(
            r"[^a-z0-9]+",
            "_",
            str(leader.get("basket_kind") or "pressure").lower(),
        ).strip("_") or "pressure"
        basket_name = "_".join(instrument.lower() for instrument in instruments)
        evidence = {
            "strict_pressure_pass": leader,
            "rationale": (
                "Primary scout strict pressure gate passed, but later execution "
                "gates did not place a trade. Validate the setup offline before "
                "any live threshold or routing change."
            ),
        }
        for lead in [15, 30]:
            for execution_policy in ["trailing_stop012", "trailing_stop015"]:
                for model_type in ["random_forest", "extra_trees"]:
                    for filter_label, segment_filters in strict_pressure_contexts:
                        enqueue_subset_spec(
                            basket_name=f"strict_pressure_pass_{basket_kind}_{basket_name}",
                            basket=instruments,
                            direction=direction,
                            delay="live",
                            metric_slug="strict_pressure_pass",
                            lead=lead,
                            execution_policy=execution_policy,
                            model_type=model_type,
                            evidence=evidence,
                            validation_weeks=6 if len(instruments) == 1 else 8,
                            validation_profile="missed_spike_strict_pressure_pass_followup_v1",
                            filter_label=filter_label,
                            segment_filters=segment_filters,
                        )
        if not target_side:
            continue
        for horizon in [60, 120]:
            for model_type in ["random_forest", "extra_trees"]:
                for filter_label, segment_filters in strict_pressure_contexts:
                    spec = precursor_spec(
                        source=source,
                        deployment_lane=lane,
                        account_focus=focus,
                        model_type=model_type,
                        target=f"profitable_{target_side}_move_{horizon}",
                        outcome=f"{target_side}_net_atr_{horizon}",
                        direction_target="",
                        instrument_subset="all",
                        segment_filters=segment_filters,
                        specialist_profile=(
                            f"primary_missed_spike_subset_strict_pressure_pass_"
                            f"directional_precursor_{basket_kind}_{basket_name}_"
                            f"{target_side}_{filter_label}_{horizon}m_{model_type}"
                        ),
                        parameters=confirmation_tree_parameter_variants(model_type)[0],
                        validation_weeks=6 if len(instruments) == 1 else 8,
                        max_train_rows=220_000,
                        evaluation_stage="validation",
                        validation_profile=(
                            "missed_spike_strict_pressure_pass_directional_"
                            f"precursor_v1_{filter_label}"
                        ),
                    )
                    spec["instrument_whitelist"] = instruments
                    spec["subset_evidence"] = evidence
                    spec["subset_validation_context"] = {
                        "filter_label": filter_label,
                        "segment_filters": segment_filters,
                        "directional_precursor": True,
                        "target_side": target_side,
                        "rationale": (
                            "The live strict pressure pass records a concrete "
                            "direction. This follow-up uses existing "
                            "direction-specific precursor labels to test "
                            "whether that side has a reusable edge."
                        ),
                    }
                    if enqueue_unique(
                        registry,
                        spec,
                        reason="live_missed_spike_strict_pressure_pass_directional_precursor",
                        seen_hashes=seen,
                    ):
                        count += 1
                        seeded.append(spec)
    return count, seeded


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Seed research-only account/model improvement specs from validated "
            "lane evidence and current value-miss reports. With no arguments, "
            "the seeder runs and writes queue/report artifacts. It never places "
            "or closes broker trades."
        )
    )
    return parser.parse_args()


def main() -> int:
    parse_args()
    registry = ModelLifecycleRegistry(
        manager.DIRS["registry"],
        manager.DIRS["promotions"],
    )
    seen = existing_hashes(registry)
    before = len(seen)
    seeded_by_lane: Dict[str, int] = {}
    examples: List[Dict[str, Any]] = []
    for lane_name, seeder in [
        ("primary_live_challenger", seed_primary_variants),
        ("tech_live_champion", seed_tech_variants),
        ("gpt_live_model_support", seed_gpt_support_variants),
        ("targeted_confirmation", seed_targeted_confirmation_variants),
        ("scout_value_capture", seed_scout_value_capture_variants),
        ("live_value_miss_followup", seed_live_value_miss_followup_variants),
        ("live_scan_cost_segment_focus", seed_scan_cost_segment_focus_variants),
        ("live_scan_cost_broad_calibration", seed_scan_cost_broad_calibration_variants),
        ("live_value_miss_calibration_detection", seed_calibration_detection_followup_variants),
        ("live_value_miss_segment_focus", seed_calibration_detection_segment_focus_variants),
        ("live_unknown_profile_followup", seed_unknown_profile_followup_variants),
        ("live_ev_near_miss_followup", seed_ev_near_miss_followup_variants),
        ("live_missed_spike_subset_followup", seed_missed_spike_subset_followup_variants),
        ("live_lead_only_robust_followup", seed_lead_only_robust_followup_variants),
    ]:
        count, specs = seeder(registry, seen)
        seeded_by_lane[lane_name] = count
        examples.extend(specs[:5])
    seeded_total = sum(seeded_by_lane.values())
    payload = {
        "generated_utc": utc_iso(),
        "execution": "research_only_no_broker_no_live_monitoring",
        "seeded_by_lane": seeded_by_lane,
        "seeded_total": seeded_total,
        "no_op": seeded_total == 0,
        "existing_hashes_before": before,
        "existing_hashes_after": len(seen),
        "reason": (
            "Weekend account-specific model improvement around validated "
            "scorecard segments: primary JPY spread/off-NY, tech exotic short "
            "high-vol/spread, GPT support volatile precursor coverage, and "
            "scout value-capture lead-event validation. During live-week "
            "monitoring it also seeds compact whitelist follow-ups for current "
            "value-weighted misses, unknown-profile scout skips, and "
            "EV-threshold near misses. It also seeds scan-cost candidates "
            "from primary scout scans where movement was positive after spread "
            "but still failed before order attempts. Current value-miss "
            "candidates that pass economic gates but fail Brier/detection "
            "quality are queued as compact calibration-oriented boosting "
            "follow-ups, plus narrow segment-focus retests when scorecards "
            "show the edge is localized to a session/regime/pair family/"
            "instrument. Positive missed-spike replay subsets and localized "
            "same-currency cluster rejects are also queued as primary scout "
            "validation candidates, not live promotion evidence. Gate-passed "
            "live follow-ups with too few folds are queued as 12-week robust "
            "validation checks across original, liquidity-only, and unfiltered "
            "contexts before any promotion or live-routing decision."
        ),
        "examples": [
            {
                "source": spec.get("source"),
                "deployment_lane": spec.get("deployment_lane"),
                "model_type": spec.get("model_type"),
                "target": spec.get("target"),
                "instrument_whitelist": spec.get("instrument_whitelist", []),
                "segment_filters": spec.get("segment_filters", {}),
                "specialist_profile": spec.get("specialist_profile", ""),
            }
            for spec in examples
        ],
    }
    REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    stamped = (
        REPORTS_ROOT
        / f"weekend_account_improvement_seed_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    )
    manager.save_json(stamped, payload)
    if seeded_total > 0:
        manager.save_json(
            REPORTS_ROOT / "latest_weekend_account_improvement_seed.json",
            payload,
        )
    else:
        manager.save_json(
            REPORTS_ROOT / "latest_weekend_account_improvement_seed_noop.json",
            payload,
        )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
