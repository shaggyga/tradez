from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable

import numpy as np
import pandas as pd

from .oco import evaluate_oco_candidates
from .portfolio import ReplayResult, no_trade_baseline, replay_account
from .schemas import (
    AccountConfig,
    EconomicsProvider,
    ObjectiveConfig,
    OCOConfig,
    config_fingerprint,
)


@dataclass
class SearchResult:
    leaderboard: pd.DataFrame
    runs: dict[str, list[ReplayResult]]
    oco_outcomes: dict[str, pd.DataFrame] | None = None


def objective_for_summary(summary: dict[str, Any], config: ObjectiveConfig) -> float:
    """Score one fold relative to the explicit no-trade value of zero."""

    if summary.get("strategy") == "no_trade":
        return 0.0
    if (
        bool(summary.get("account_ruin"))
        or int(summary.get("margin_closeout_risk_events", 0)) > 0
        or int(summary.get("mae_stress_closeout_events", 0)) > 0
    ):
        return float(config.hard_failure_score)
    start = float(summary.get("starting_balance", 0.0))
    end = float(summary.get("ending_balance", 0.0))
    if start <= 0 or end <= 0:
        return float(config.hard_failure_score)
    growth = float(np.log(end / start))
    drawdown = max(0.0, float(summary.get("max_drawdown_fraction", 0.0)))
    tail_loss = max(0.0, -float(summary.get("cvar_5_trade_return_fraction", 0.0)))
    turnover = max(0.0, float(summary.get("risk_turnover_fraction", 0.0)))
    concentration = max(0.0, float(summary.get("theme_concentration_hhi", 0.0)))
    trades = int(summary.get("opened_trades", 0))
    shortfall = max(0.0, (int(config.minimum_trades_per_fold) - trades) / max(1, int(config.minimum_trades_per_fold)))
    return float(
        growth
        - float(config.drawdown_penalty) * drawdown
        - float(config.cvar_penalty) * tail_loss
        - float(config.turnover_penalty) * turnover
        - float(config.concentration_penalty) * concentration
        - float(config.trade_shortfall_penalty) * shortfall
    )


def stable_fold_objective(scores: Iterable[float], config: ObjectiveConfig) -> float:
    values = np.asarray(list(scores), dtype=float)
    if len(values) == 0 or not np.isfinite(values).all():
        return float(config.hard_failure_score)
    if (values <= float(config.hard_failure_score)).any():
        return float(config.hard_failure_score)
    median = float(np.median(values))
    mad = float(np.median(np.abs(values - median)))
    negative_share = float((values < 0.0).mean())
    return float(
        median
        - float(config.dispersion_penalty) * mad
        - float(config.negative_fold_penalty) * negative_share
    )


def search_account_space(
    outcomes: pd.DataFrame,
    account_configs: Iterable[AccountConfig],
    objective_config: ObjectiveConfig = ObjectiveConfig(),
    *,
    fold_column: str | None = None,
    economics_provider: EconomicsProvider | None = None,
) -> SearchResult:
    """Evaluate account configurations, always including a no-trade baseline."""

    configs = list(account_configs)
    if not configs:
        raise ValueError("account_configs cannot be empty")
    if fold_column is not None and fold_column not in outcomes.columns:
        raise ValueError(f"fold column is missing: {fold_column}")
    folds: list[tuple[str, pd.DataFrame]]
    if fold_column is None:
        folds = [("all", outcomes)]
    else:
        folds = [
            (str(key), group.copy())
            for key, group in outcomes.groupby(fold_column, sort=True, dropna=False)
        ]
        if not folds:
            folds = [("empty", outcomes)]

    rows: list[dict[str, Any]] = [{
        "strategy": "no_trade",
        "account_config_id": "no_trade",
        "stable_objective": 0.0,
        "fold_count": len(folds),
        "minimum_fold_objective": 0.0,
        "median_fold_objective": 0.0,
        "median_return_fraction": 0.0,
        "median_max_drawdown_fraction": 0.0,
        "total_opened_trades": 0,
        "beats_no_trade": False,
        "research_only": True,
    }]
    runs: dict[str, list[ReplayResult]] = {
        "no_trade": [no_trade_baseline(configs[0].starting_balance)]
    }

    for account_config in configs:
        account_id = config_fingerprint(account_config, "account")
        fold_runs: list[ReplayResult] = []
        fold_scores: list[float] = []
        for fold_name, fold_frame in folds:
            result = replay_account(fold_frame, account_config, economics_provider)
            score = objective_for_summary(result.summary, objective_config)
            result.summary["fold"] = fold_name
            result.summary["objective"] = score
            fold_runs.append(result)
            fold_scores.append(score)
        stable = stable_fold_objective(fold_scores, objective_config)
        runs[account_id] = fold_runs
        rows.append({
            "strategy": "movement_gated_oco",
            "account_config_id": account_id,
            "stable_objective": stable,
            "fold_count": len(fold_runs),
            "minimum_fold_objective": float(min(fold_scores)),
            "median_fold_objective": float(np.median(fold_scores)),
            "median_return_fraction": float(np.median([r.summary["return_fraction"] for r in fold_runs])),
            "median_max_drawdown_fraction": float(np.median([r.summary["max_drawdown_fraction"] for r in fold_runs])),
            "total_opened_trades": int(sum(r.summary["opened_trades"] for r in fold_runs)),
            "beats_no_trade": bool(stable > 0.0),
            "account_config": asdict(account_config),
            "research_only": True,
        })
    leaderboard = pd.DataFrame(rows).sort_values(
        ["stable_objective", "account_config_id"], ascending=[False, True], kind="mergesort"
    ).reset_index(drop=True)
    leaderboard["rank"] = np.arange(1, len(leaderboard) + 1)
    return SearchResult(leaderboard, runs)


def search_joint_space(
    decisions: pd.DataFrame,
    bars: pd.DataFrame,
    oco_configs: Iterable[OCOConfig],
    account_configs: Iterable[AccountConfig],
    objective_config: ObjectiveConfig = ObjectiveConfig(),
    *,
    fold_column: str | None = None,
    economics_provider: EconomicsProvider | None = None,
) -> SearchResult:
    """Evaluate OCO execution parameters and account constraints together."""

    oco_configs = list(oco_configs)
    account_configs = list(account_configs)
    if not oco_configs or not account_configs:
        raise ValueError("oco_configs and account_configs cannot be empty")
    rows: list[pd.DataFrame] = []
    all_runs: dict[str, list[ReplayResult]] = {}
    all_outcomes: dict[str, pd.DataFrame] = {}
    for oco_config in oco_configs:
        oco_id = config_fingerprint(oco_config, "oco")
        outcomes = evaluate_oco_candidates(decisions, bars, oco_config)
        all_outcomes[oco_id] = outcomes
        account_search = search_account_space(
            outcomes,
            account_configs,
            objective_config,
            fold_column=fold_column,
            economics_provider=economics_provider,
        )
        frame = account_search.leaderboard[account_search.leaderboard["strategy"] != "no_trade"].copy()
        frame["oco_config_id"] = oco_id
        frame["joint_config_id"] = frame["account_config_id"].map(lambda account_id: f"{oco_id}__{account_id}")
        frame["oco_config"] = [asdict(oco_config)] * len(frame)
        rows.append(frame)
        for account_id, run_list in account_search.runs.items():
            if account_id != "no_trade":
                all_runs[f"{oco_id}__{account_id}"] = run_list

    baseline = pd.DataFrame([{
        "strategy": "no_trade",
        "account_config_id": "no_trade",
        "oco_config_id": "none",
        "joint_config_id": "no_trade",
        "stable_objective": 0.0,
        "fold_count": 1 if fold_column is None else int(decisions[fold_column].nunique(dropna=False)),
        "minimum_fold_objective": 0.0,
        "median_fold_objective": 0.0,
        "median_return_fraction": 0.0,
        "median_max_drawdown_fraction": 0.0,
        "total_opened_trades": 0,
        "beats_no_trade": False,
        "research_only": True,
    }])
    leaderboard = pd.concat([baseline, *rows], ignore_index=True, sort=False).sort_values(
        ["stable_objective", "joint_config_id"], ascending=[False, True], kind="mergesort"
    ).reset_index(drop=True)
    leaderboard["rank"] = np.arange(1, len(leaderboard) + 1)
    all_runs["no_trade"] = [no_trade_baseline(account_configs[0].starting_balance)]
    return SearchResult(leaderboard, all_runs, all_outcomes)
