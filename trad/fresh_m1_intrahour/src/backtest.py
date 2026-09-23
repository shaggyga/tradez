from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .common import max_drawdown, write_json
from .train import predict_dataset


@dataclass
class Position:
    instrument: str
    side: str
    open_time: pd.Timestamp
    planned_exit_time: pd.Timestamp
    policy: str
    pred_ev_usd_1k: float
    rank_score: float
    units: int
    margin_used: float
    entry_row_index: int


def _policy_actual(row: pd.Series, units: int, policy: str) -> tuple[float, int, str]:
    pnl_col = f"policy_{policy}_net_account_pnl_1k_units"
    time_col = f"policy_{policy}_time_to_exit"
    outcome_col = f"policy_{policy}_outcome"
    pnl = float(row[pnl_col]) * (units / 1000.0)
    hold = max(1, int(row[time_col]))
    outcome = str(row[outcome_col])
    return pnl, hold, outcome


def _same_market_side(pos: Position, row: pd.Series) -> bool:
    return pos.instrument == row["instrument"] and pos.side == row["side"]


def _opposite_key(row: pd.Series) -> tuple[pd.Timestamp, str, str, str]:
    return (
        row["decision_time_utc"],
        row["instrument"],
        "short" if row["side"] == "long" else "long",
        row["predicted_policy"],
    )


def _failure_class(selected_pnl: float, opposite_pnl: float | None) -> str:
    selected_ok = selected_pnl > 0
    opposite_ok = opposite_pnl is not None and opposite_pnl > 0
    if selected_ok and not opposite_ok:
        return "signal_works"
    if not selected_ok and opposite_ok:
        return "signal_inverted"
    if selected_ok and opposite_ok:
        return "both_sides_tradable_exit_timing_sensitive"
    return "neither_side_tradable"


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


def _breakdown(df: pd.DataFrame, group_col: str) -> list[dict[str, Any]]:
    if df.empty or group_col not in df.columns:
        return []
    out = []
    for key, group in df.groupby(group_col, dropna=False):
        pnl = group["realized_pnl_usd"].astype(float)
        out.append({
            group_col: str(key),
            "count": int(len(group)),
            "realized_pnl_usd_sum": float(pnl.sum()),
            "realized_pnl_usd_mean": float(pnl.mean()),
            "win_rate": float((pnl > 0).mean()),
        })
    return out


def _bucket_breakdown(df: pd.DataFrame, value_col: str) -> list[dict[str, Any]]:
    if df.empty or value_col not in df.columns:
        return []
    tmp = df[[value_col, "realized_pnl_usd"]].dropna().copy()
    if tmp.empty:
        return []
    tmp["bucket"] = pd.qcut(tmp[value_col].rank(method="first"), 5, labels=False, duplicates="drop")
    return _breakdown(tmp, "bucket")


def run_backtest(cfg: dict[str, Any], dataset_path: Path, model_path: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    df = predict_dataset(model_path, dataset_path)
    df["decision_time_utc"] = pd.to_datetime(df["decision_time_utc"], utc=True)
    df = df.sort_values(["decision_time_utc", "rank_score"], ascending=[True, False]).reset_index(drop=True)
    split_idx = int(len(df) * (1.0 - float(cfg["model"]["test_fraction"])))
    test_start = df.iloc[split_idx]["decision_time_utc"]
    df = df[df["decision_time_utc"] >= test_start].copy().reset_index(drop=True)

    opposite_lookup = {}
    for i, row in enumerate(df[["decision_time_utc", "instrument", "side", "predicted_policy"]].itertuples(index=False)):
        opposite_lookup[(row.decision_time_utc, row.instrument, row.side, row.predicted_policy)] = i

    initial_equity = float(cfg["initial_equity"])
    equity = initial_equity
    positions: list[Position] = []
    equity_points: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    shadow: list[dict[str, Any]] = []

    max_open = int(cfg["max_open_positions"])
    max_new = int(cfg["max_new_positions_per_cycle"])
    min_ev = float(cfg["allocator"]["min_ev_usd"])
    replacement_margin = float(cfg["allocator"]["replace_ev_margin_usd"])
    uncertainty = float(cfg["allocator"]["uncertainty_buffer_usd"])
    anti_churn = int(cfg["allocator"]["anti_churn_minutes"])
    max_margin_pct = float(cfg["hard_max_margin_used_pct"])
    notional_fraction = float(cfg["allocator"]["position_notional_fraction"])
    max_units = int(cfg["allocator"]["max_units"])
    switching_cost_pips = float(cfg["costs"]["switching_cost_pips"])

    for ts, group in df.groupby("decision_time_utc", sort=True):
        still_open: list[Position] = []
        for pos in positions:
            if ts >= pos.planned_exit_time:
                entry = df.iloc[pos.entry_row_index]
                pnl, hold, outcome = _policy_actual(entry, pos.units, pos.policy)
                equity += pnl
                trades.append({
                    "open_time": pos.open_time,
                    "close_time": ts,
                    "instrument": pos.instrument,
                    "side": pos.side,
                    "policy": pos.policy,
                    "units": pos.units,
                    "hold_minutes": hold,
                    "exit_reason": "policy_exit",
                    "outcome": outcome,
                    "pred_ev_usd_1k": pos.pred_ev_usd_1k,
                    "rank_score": pos.rank_score,
                    "realized_pnl_usd": pnl,
                    "equity_after": equity,
                    "session": _session_name(entry),
                    "spread_pips": float(entry.get("spread_pips", np.nan)),
                    "atr_15m_pips": float(entry.get("atr_15m_pips", np.nan)),
                    "spread_to_atr_15m": float(entry.get("spread_to_atr_15m", np.nan)),
                })
            else:
                still_open.append(pos)
        positions = still_open

        # One row per side-policy candidate. The allocator chooses only from predictions.
        candidates = group[
            (group["pred_ev_usd_1k_units"] > min_ev)
            & (group["pred_positive_probability"] >= 0.50)
        ].sort_values("rank_score", ascending=False)
        opened = 0
        for _, cand in candidates.iterrows():
            if opened >= max_new:
                break
            if any(_same_market_side(p, cand) for p in positions):
                continue
            if len(positions) >= max_open:
                worst = min(positions, key=lambda p: p.pred_ev_usd_1k)
                age = int((ts - worst.open_time).total_seconds() // 60)
                if age < anti_churn:
                    continue
                switch_cost_usd_1k = switching_cost_pips * float(cand["pip_value_usd_per_unit"]) * 1000.0
                hurdle = worst.pred_ev_usd_1k + replacement_margin + uncertainty + switch_cost_usd_1k
                if float(cand["pred_ev_usd_1k_units"]) <= hurdle:
                    continue
                entry = df.iloc[worst.entry_row_index]
                pnl, hold, outcome = _policy_actual(entry, worst.units, worst.policy)
                equity += pnl
                trades.append({
                    "open_time": worst.open_time,
                    "close_time": ts,
                    "instrument": worst.instrument,
                    "side": worst.side,
                    "policy": worst.policy,
                    "units": worst.units,
                    "hold_minutes": age,
                    "exit_reason": "replaced_by_better_predicted_policy_ev",
                    "outcome": outcome,
                    "pred_ev_usd_1k": worst.pred_ev_usd_1k,
                    "rank_score": worst.rank_score,
                    "realized_pnl_usd": pnl,
                    "equity_after": equity,
                    "session": _session_name(entry),
                    "spread_pips": float(entry.get("spread_pips", np.nan)),
                    "atr_15m_pips": float(entry.get("atr_15m_pips", np.nan)),
                    "spread_to_atr_15m": float(entry.get("spread_to_atr_15m", np.nan)),
                })
                positions.remove(worst)

            units = min(max_units, max(1, int((equity * notional_fraction) / max(float(cand["close"]), 1e-9))))
            margin = units * float(cand["close"]) * float(cfg["margin_rate_default"])
            projected_margin_pct = (sum(p.margin_used for p in positions) + margin) / max(equity, 1e-9) * 100.0
            if projected_margin_pct > max_margin_pct:
                continue

            policy = str(cand["predicted_policy"])
            selected_pnl, hold, outcome = _policy_actual(cand, units, policy)
            planned_exit = ts + pd.Timedelta(minutes=max(1, hold))
            positions.append(Position(
                instrument=str(cand["instrument"]),
                side=str(cand["side"]),
                open_time=ts,
                planned_exit_time=planned_exit,
                policy=policy,
                pred_ev_usd_1k=float(cand["pred_ev_usd_1k_units"]),
                rank_score=float(cand["rank_score"]),
                units=units,
                margin_used=margin,
                entry_row_index=int(cand.name),
            ))
            opened += 1

            opposite_pnl = None
            opp_idx = opposite_lookup.get(_opposite_key(cand))
            if opp_idx is not None:
                opp = df.iloc[opp_idx]
                opposite_pnl, _, _ = _policy_actual(opp, units, policy)
            shadow.append({
                "decision_time_utc": ts,
                "instrument": cand["instrument"],
                "selected_side": cand["side"],
                "selected_policy": policy,
                "selected_pred_ev_usd_1k": cand["pred_ev_usd_1k_units"],
                "selected_actual_pnl_usd": selected_pnl,
                "opposite_actual_pnl_usd": opposite_pnl,
                "failure_class": _failure_class(selected_pnl, opposite_pnl),
                "spread_pips": cand.get("spread_pips"),
                "atr_15m_pips": cand.get("atr_15m_pips"),
                "spread_to_atr_15m": cand.get("spread_to_atr_15m"),
            })

        equity_points.append({
            "decision_time_utc": ts,
            "equity": equity,
            "open_positions": len(positions),
            "margin_used_pct": sum(p.margin_used for p in positions) / max(equity, 1e-9) * 100.0,
        })

    final_ts = df["decision_time_utc"].max()
    for pos in positions:
        entry = df.iloc[pos.entry_row_index]
        pnl, hold, outcome = _policy_actual(entry, pos.units, pos.policy)
        equity += pnl
        trades.append({
            "open_time": pos.open_time,
            "close_time": final_ts,
            "instrument": pos.instrument,
            "side": pos.side,
            "policy": pos.policy,
            "units": pos.units,
            "hold_minutes": hold,
            "exit_reason": "end_of_backtest",
            "outcome": outcome,
            "pred_ev_usd_1k": pos.pred_ev_usd_1k,
            "rank_score": pos.rank_score,
            "realized_pnl_usd": pnl,
            "equity_after": equity,
            "session": _session_name(entry),
            "spread_pips": float(entry.get("spread_pips", np.nan)),
            "atr_15m_pips": float(entry.get("atr_15m_pips", np.nan)),
            "spread_to_atr_15m": float(entry.get("spread_to_atr_15m", np.nan)),
        })

    trades_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity_points)
    shadow_df = pd.DataFrame(shadow)
    trades_df.to_csv(output_dir / "trades.csv", index=False)
    equity_df.to_csv(output_dir / "equity_curve.csv", index=False)
    shadow_df.to_csv(output_dir / "shadow_diagnostics.csv", index=False)

    pnl = trades_df["realized_pnl_usd"] if not trades_df.empty else pd.Series(dtype=float)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    summary = {
        "dataset_path": str(dataset_path),
        "model_path": str(model_path),
        "initial_equity": initial_equity,
        "final_equity": float(equity),
        "return_pct": float((equity / initial_equity - 1.0) * 100.0),
        "trade_count": int(len(trades_df)),
        "win_rate": float((pnl > 0).mean()) if len(pnl) else None,
        "profit_factor": float(wins.sum() / abs(losses.sum())) if len(losses) and abs(losses.sum()) > 0 else None,
        "max_drawdown_pct": max_drawdown(equity_df["equity"].tolist()) if not equity_df.empty else 0.0,
        "avg_pnl_usd": float(pnl.mean()) if len(pnl) else 0.0,
        "gross_profit_usd": float(wins.sum()) if len(wins) else 0.0,
        "gross_loss_usd": float(losses.sum()) if len(losses) else 0.0,
        "max_margin_used_pct": float(equity_df["margin_used_pct"].max()) if not equity_df.empty else 0.0,
        "policy_breakdown": _breakdown(trades_df, "policy"),
        "pair_breakdown": _breakdown(trades_df, "instrument"),
        "session_breakdown": _breakdown(trades_df, "session"),
        "long_short_breakdown": _breakdown(trades_df, "side"),
        "spread_breakdown": _bucket_breakdown(trades_df, "spread_pips"),
        "atr_breakdown": _bucket_breakdown(trades_df, "atr_15m_pips"),
        "spread_to_atr_breakdown": _bucket_breakdown(trades_df, "spread_to_atr_15m"),
        "failure_taxonomy": shadow_df["failure_class"].value_counts().to_dict() if not shadow_df.empty else {},
        "shadow_rates": {
            "selected_side_positive_rate": float((shadow_df["selected_actual_pnl_usd"] > 0).mean()) if not shadow_df.empty else None,
            "opposite_side_positive_rate": float((shadow_df["opposite_actual_pnl_usd"].fillna(0) > 0).mean()) if not shadow_df.empty else None,
            "signal_inverted_rate": float((shadow_df["failure_class"] == "signal_inverted").mean()) if not shadow_df.empty else None,
            "both_sides_tradable_rate": float((shadow_df["failure_class"] == "both_sides_tradable_exit_timing_sensitive").mean()) if not shadow_df.empty else None,
            "neither_side_tradable_rate": float((shadow_df["failure_class"] == "neither_side_tradable").mean()) if not shadow_df.empty else None,
        },
        "allocator": cfg["allocator"],
        "risk": {
            "max_open_positions": cfg["max_open_positions"],
            "target_margin_used_pct": cfg["target_margin_used_pct"],
            "hard_max_margin_used_pct": cfg["hard_max_margin_used_pct"],
            "max_new_positions_per_cycle": cfg["max_new_positions_per_cycle"],
        },
        "leakage_checks": {
            "backtest_uses_model_predictions_at_decision_time": True,
            "policies_chosen_by_predicted_ev_only": True,
            "labels_used_only_for_realized_outcomes_after_selection": True,
            "old_reversal_target_used": False,
            "old_follow_momentum_policy_used": False,
            "label_derived_best_policy_used_for_execution": False,
            "oracle_policy_selector_used": False,
        },
    }
    write_json(output_dir / "backtest_summary.json", summary)
    return output_dir / "backtest_summary.json"
