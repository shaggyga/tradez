from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

import numpy as np
import pandas as pd

from .schemas import (
    AccountConfig,
    EconomicsProvider,
    TradeEconomics,
    assert_causal_selection_column,
    config_fingerprint,
    require_columns,
)


@dataclass
class ReplayResult:
    summary: dict[str, Any]
    trades: pd.DataFrame
    blocked: pd.DataFrame
    equity_curve: pd.DataFrame


def _finite(value: Any) -> bool:
    try:
        return bool(np.isfinite(float(value)))
    except (TypeError, ValueError):
        return False


def _row_economics(
    row: Mapping[str, Any],
    config: AccountConfig,
    provider: EconomicsProvider | None,
) -> TradeEconomics:
    if provider is not None:
        return provider.resolve(row, config.account_currency)

    risk_value = row.get("risk_per_pip_account_per_unit")
    pnl_value = row.get("pnl_per_pip_account_per_unit")
    exact_pnl = row.get("realized_pnl_account_per_unit")
    explicit = row.get("pip_value_account_per_unit")
    usd = row.get("pip_value_usd_per_unit")
    if not _finite(risk_value):
        risk_value = explicit
    if not _finite(pnl_value):
        pnl_value = explicit
    if str(config.account_currency).upper() == "USD":
        if not _finite(risk_value):
            risk_value = usd
        if not _finite(pnl_value):
            pnl_value = usd

    margin = row.get("margin_per_unit_account")
    if not _finite(margin) and config.fallback_margin_rate is not None:
        notional = row.get("notional_value_account_per_unit")
        if _finite(notional):
            margin = float(notional) * float(config.fallback_margin_rate)

    if not (_finite(risk_value) and float(risk_value) > 0):
        raise ValueError("missing positive risk_per_pip_account_per_unit")
    if not (_finite(exact_pnl) or (_finite(pnl_value) and float(pnl_value) > 0)):
        raise ValueError("missing positive pnl_per_pip_account_per_unit")
    if not (_finite(margin) and float(margin) > 0):
        raise ValueError("missing positive margin_per_unit_account")
    financing = row.get("financing_per_unit_account", 0.0)
    other_cost = row.get("other_cost_per_unit_account", 0.0)
    return TradeEconomics(
        risk_per_pip_account_per_unit=float(risk_value),
        margin_per_unit_account=float(margin),
        pnl_per_pip_account_per_unit=float(pnl_value) if _finite(pnl_value) else None,
        realized_pnl_account_per_unit=float(exact_pnl) if _finite(exact_pnl) else None,
        financing_per_unit_account=float(financing) if _finite(financing) else 0.0,
        other_cost_per_unit_account=float(other_cost) if _finite(other_cost) else 0.0,
    )


def _empty_summary(config: AccountConfig, candidate_rows: int = 0) -> dict[str, Any]:
    return {
        "strategy": "movement_gated_oco",
        "account_config_id": config_fingerprint(config, "account"),
        "candidate_rows": int(candidate_rows),
        "opened_trades": 0,
        "blocked_candidates": 0,
        "starting_balance": float(config.starting_balance),
        "ending_balance": float(config.starting_balance),
        "net_profit_account": 0.0,
        "return_fraction": 0.0,
        "max_drawdown_fraction": 0.0,
        "profit_factor": 0.0,
        "win_rate": 0.0,
        "cvar_5_trade_return_fraction": 0.0,
        "risk_turnover_fraction": 0.0,
        "theme_concentration_hhi": 0.0,
        "max_margin_used_fraction": 0.0,
        "max_open_risk_fraction": 0.0,
        "max_concurrent_positions_seen": 0,
        "margin_closeout_risk_events": 0,
        "mae_stress_closeout_events": 0,
        "account_ruin": False,
        "daily_stop_blocks": 0,
        "drawdown_halt_blocks": 0,
        "research_only": True,
    }


def no_trade_baseline(starting_balance: float = 10_000.0) -> ReplayResult:
    """Return the explicit zero-exposure baseline used by every search."""

    cfg = AccountConfig(starting_balance=starting_balance)
    summary = _empty_summary(cfg)
    summary.update({
        "strategy": "no_trade",
        "account_config_id": "no_trade",
        "objective": 0.0,
    })
    curve = pd.DataFrame([{
        "timestamp": pd.NaT,
        "balance": float(starting_balance),
        "margin_used": 0.0,
        "open_risk": 0.0,
        "open_positions": 0,
        "drawdown_fraction": 0.0,
    }])
    return ReplayResult(summary, pd.DataFrame(), pd.DataFrame(), curve)


class _Replay:
    def __init__(
        self,
        config: AccountConfig,
        provider: EconomicsProvider | None,
        candidate_rows: int,
    ) -> None:
        self.config = config
        self.provider = provider
        self.candidate_rows = candidate_rows
        self.balance = float(config.starting_balance)
        self.peak_balance = self.balance
        self.open_positions: list[dict[str, Any]] = []
        self.closed: list[dict[str, Any]] = []
        self.blocked: list[dict[str, Any]] = []
        self.curve: list[dict[str, Any]] = []
        self.day: Any = None
        self.day_start_balance = self.balance
        self.day_realized = 0.0
        self.margin_closeout_risk_events = 0
        self.mae_stress_closeout_events = 0
        self.global_halt = False
        self.max_margin_fraction = 0.0
        self.max_risk_fraction = 0.0
        self.max_positions = 0

    @property
    def margin_used(self) -> float:
        return float(sum(float(p["margin_account"]) for p in self.open_positions))

    @property
    def open_risk(self) -> float:
        return float(sum(float(p["risk_account"]) for p in self.open_positions))

    @property
    def open_stress_loss(self) -> float:
        """Conservative simultaneous-MAE stress; MAE timing is unavailable."""
        return float(sum(float(p["stress_loss_account"]) for p in self.open_positions))

    def _ensure_day(self, timestamp: pd.Timestamp) -> None:
        day = pd.Timestamp(timestamp).date()
        if day != self.day:
            self.day = day
            self.day_start_balance = self.balance
            self.day_realized = 0.0

    def _drawdown(self) -> float:
        return max(0.0, (self.peak_balance - self.balance) / max(self.peak_balance, 1e-12))

    def record_curve(self, timestamp: pd.Timestamp) -> None:
        balance = max(self.balance, 1e-12)
        margin_fraction = self.margin_used / balance
        risk_fraction = self.open_risk / balance
        self.max_margin_fraction = max(self.max_margin_fraction, margin_fraction)
        self.max_risk_fraction = max(self.max_risk_fraction, risk_fraction)
        self.max_positions = max(self.max_positions, len(self.open_positions))
        self.curve.append({
            "timestamp": pd.Timestamp(timestamp),
            "balance": float(self.balance),
            "margin_used": self.margin_used,
            "open_risk": self.open_risk,
            "open_stress_loss": self.open_stress_loss,
            "open_positions": len(self.open_positions),
            "drawdown_fraction": self._drawdown(),
        })

    def block(self, row: Mapping[str, Any], reason: str, timestamp: pd.Timestamp) -> None:
        self.blocked.append({
            "decision_id": row.get("decision_id"),
            "timestamp": pd.Timestamp(timestamp),
            "instrument": row.get("instrument"),
            "currency_theme_cluster_id": row.get("currency_theme_cluster_id", "unclassified"),
            "reason": reason,
            "balance": float(self.balance),
            "margin_used": self.margin_used,
            "open_risk": self.open_risk,
        })

    def close_due(self, timestamp: pd.Timestamp) -> None:
        due = sorted(
            (p for p in self.open_positions if pd.Timestamp(p["exit_timestamp"]) <= timestamp),
            key=lambda p: (pd.Timestamp(p["exit_timestamp"]), str(p["decision_id"])),
        )
        for position in due:
            self.open_positions.remove(position)
            close_time = pd.Timestamp(position["exit_timestamp"])
            self._ensure_day(close_time)
            exact_per_unit = position.get("realized_pnl_account_per_unit")
            if exact_per_unit is not None and _finite(exact_per_unit):
                price_pnl = float(exact_per_unit) * int(position["units"])
            else:
                price_pnl = (
                    float(position["realized_pips"])
                    * float(position["pnl_per_pip_account_per_unit"])
                    * int(position["units"])
                )
            pnl = (
                price_pnl
                + float(position["financing_per_unit_account"]) * int(position["units"])
                - float(position["other_cost_per_unit_account"]) * int(position["units"])
            )
            self.balance += pnl
            self.day_realized += pnl
            self.peak_balance = max(self.peak_balance, self.balance)
            closed = {**position, "pnl_account": float(pnl), "balance_after_close": float(self.balance)}
            self.closed.append(closed)
            if self.balance <= 0:
                self.global_halt = True
            # This is causal: current realized balance and configured
            # protective risk, not future realized MAE.
            stressed_nav = self.balance - self.open_risk
            if self.open_positions and stressed_nav <= self.margin_used * float(self.config.margin_closeout_buffer):
                self.margin_closeout_risk_events += 1
                self.global_halt = True
            self.record_curve(close_time)

    def _positions_matching(self, key: str, value: Any) -> list[dict[str, Any]]:
        return [p for p in self.open_positions if p.get(key) == value]

    def open(self, row: Mapping[str, Any], timestamp: pd.Timestamp) -> bool:
        self._ensure_day(timestamp)
        if self.global_halt:
            self.block(row, "account_halted", timestamp)
            return False
        if self.day_realized <= -float(self.config.daily_loss_stop_fraction) * self.day_start_balance:
            self.block(row, "daily_loss_stop", timestamp)
            return False
        if self._drawdown() >= float(self.config.drawdown_halt_fraction):
            self.block(row, "drawdown_halt", timestamp)
            return False
        if len(self.open_positions) >= int(self.config.max_concurrent_positions):
            self.block(row, "concurrent_position_cap", timestamp)
            return False

        instrument = str(row.get("instrument"))
        theme = str(row.get("currency_theme_cluster_id", "unclassified"))
        if len(self._positions_matching("instrument", instrument)) >= int(self.config.max_positions_per_instrument):
            self.block(row, "instrument_position_cap", timestamp)
            return False
        if len(self._positions_matching("currency_theme_cluster_id", theme)) >= int(self.config.max_positions_per_theme):
            self.block(row, "currency_theme_position_cap", timestamp)
            return False
        edge = row.get("expected_net_edge_pips")
        if not _finite(edge) or float(edge) < float(self.config.minimum_expected_edge_pips):
            self.block(row, "expected_edge_floor", timestamp)
            return False
        risk_pips = row.get("risk_pips_per_unit")
        if not _finite(risk_pips) or float(risk_pips) <= 0:
            self.block(row, "undefined_protective_risk", timestamp)
            return False
        try:
            economics = _row_economics(row, self.config, self.provider)
        except (ValueError, TypeError) as exc:
            self.block(row, f"missing_economics:{exc}", timestamp)
            return False

        unit_risk = float(risk_pips) * economics.risk_per_pip_account_per_unit
        reserved_legs = max(1, int(row.get("reserved_legs", row.get("filled_legs", 1))))
        unit_margin = economics.margin_per_unit_account * reserved_legs
        trade_budget = self.balance * float(self.config.risk_per_trade_fraction)
        remaining_risk = self.balance * float(self.config.max_total_open_risk_fraction) - self.open_risk
        theme_risk = sum(float(p["risk_account"]) for p in self._positions_matching("currency_theme_cluster_id", theme))
        remaining_theme_risk = self.balance * float(self.config.max_theme_open_risk_fraction) - theme_risk
        remaining_margin = self.balance * float(self.config.max_margin_used_fraction) - self.margin_used
        unit_caps = [trade_budget / unit_risk, remaining_risk / unit_risk, remaining_theme_risk / unit_risk]
        unit_caps.append(remaining_margin / unit_margin)
        units = int(np.floor(max(0.0, min(unit_caps)) / int(self.config.unit_step))) * int(self.config.unit_step)
        if self.config.max_units is not None:
            units = min(units, int(self.config.max_units))
        if units < int(self.config.min_units):
            self.block(row, "risk_or_margin_capacity", timestamp)
            return False
        risk_account = units * unit_risk
        margin_account = units * unit_margin
        mae_pips = float(row.get("mae_pips", 0.0)) if _finite(row.get("mae_pips", 0.0)) else 0.0
        stress_pips = max(float(risk_pips), mae_pips)
        stress_loss_account = units * stress_pips * economics.risk_per_pip_account_per_unit
        # Opening capacity must be causal.  Reserve configured stop risk; do
        # not use the candidate's future MAE to decide whether it gets opened.
        stressed_nav = self.balance - (self.open_risk + risk_account)
        stressed_margin = self.margin_used + margin_account
        if stressed_nav <= stressed_margin * float(self.config.margin_closeout_buffer):
            self.block(row, "stressed_margin_closeout_buffer", timestamp)
            return False

        position = {
            **dict(row),
            "entry_timestamp": pd.Timestamp(row["entry_timestamp"]),
            "exit_timestamp": pd.Timestamp(row["exit_timestamp"]),
            "units": int(units),
            "reserved_legs": reserved_legs,
            "risk_account": float(risk_account),
            "margin_account": float(margin_account),
            "stress_loss_account": float(stress_loss_account),
            "risk_per_pip_account_per_unit": economics.risk_per_pip_account_per_unit,
            "pnl_per_pip_account_per_unit": economics.pnl_per_pip_account_per_unit,
            "realized_pnl_account_per_unit": economics.realized_pnl_account_per_unit,
            "margin_per_unit_account": economics.margin_per_unit_account,
            "financing_per_unit_account": economics.financing_per_unit_account,
            "other_cost_per_unit_account": economics.other_cost_per_unit_account,
            "balance_at_open": float(self.balance),
        }
        self.open_positions.append(position)
        # Retrospective stress diagnostic only.  Since exact MAE timestamps are
        # not in the neutral outcome schema, assume all concurrent positions
        # experience their individual MAE together.  This can invalidate a
        # configuration's objective, but never changes candidate selection.
        if self.balance - self.open_stress_loss <= self.margin_used * float(
            self.config.margin_closeout_buffer
        ):
            self.mae_stress_closeout_events += 1
        return True

    def finish(self) -> ReplayResult:
        if self.open_positions:
            final = max(pd.Timestamp(p["exit_timestamp"]) for p in self.open_positions)
            self.close_due(final)
        trades = pd.DataFrame(self.closed)
        blocked = pd.DataFrame(self.blocked)
        curve = pd.DataFrame(self.curve)
        summary = _empty_summary(self.config, self.candidate_rows)
        summary["blocked_candidates"] = len(blocked)
        summary["opened_trades"] = len(trades)
        summary["ending_balance"] = float(self.balance)
        summary["net_profit_account"] = float(self.balance - self.config.starting_balance)
        summary["return_fraction"] = float(self.balance / self.config.starting_balance - 1.0)
        summary["max_margin_used_fraction"] = float(self.max_margin_fraction)
        summary["max_open_risk_fraction"] = float(self.max_risk_fraction)
        summary["max_concurrent_positions_seen"] = int(self.max_positions)
        summary["margin_closeout_risk_events"] = int(self.margin_closeout_risk_events)
        summary["mae_stress_closeout_events"] = int(self.mae_stress_closeout_events)
        summary["account_ruin"] = bool(self.balance <= 0)
        if not curve.empty:
            summary["max_drawdown_fraction"] = float(curve["drawdown_fraction"].max())
        if not blocked.empty:
            summary["daily_stop_blocks"] = int((blocked["reason"] == "daily_loss_stop").sum())
            summary["drawdown_halt_blocks"] = int((blocked["reason"] == "drawdown_halt").sum())
        if not trades.empty:
            pnl = pd.to_numeric(trades["pnl_account"], errors="coerce").fillna(0.0)
            wins, losses = pnl[pnl > 0], pnl[pnl < 0]
            summary["win_rate"] = float((pnl > 0).mean())
            summary["profit_factor"] = (
                float(wins.sum() / abs(losses.sum())) if abs(losses.sum()) > 0
                else (float("inf") if wins.sum() > 0 else 0.0)
            )
            count = max(1, int(np.ceil(len(pnl) * 0.05)))
            worst = pnl.nsmallest(count) / float(self.config.starting_balance)
            summary["cvar_5_trade_return_fraction"] = float(worst.mean())
            summary["risk_turnover_fraction"] = float(
                pd.to_numeric(trades["risk_account"], errors="coerce").sum() / self.config.starting_balance
            )
            theme_weight = trades.groupby("currency_theme_cluster_id")["risk_account"].sum()
            if theme_weight.sum() > 0:
                weights = theme_weight / theme_weight.sum()
                summary["theme_concentration_hhi"] = float((weights * weights).sum())
        summary["account_config"] = asdict(self.config)
        return ReplayResult(summary, trades, blocked, curve)


def replay_account(
    outcomes: pd.DataFrame,
    config: AccountConfig,
    economics_provider: EconomicsProvider | None = None,
) -> ReplayResult:
    """Replay precomputed OCO outcomes under conservative account constraints.

    Candidate ordering may use only ``selection_score_column`` and
    ``expected_net_edge_pips``.  Realized path columns never participate in
    selection.  Drawdown and daily stops use realized balance; per-trade
    protective risk is additionally reserved in the stressed-margin guard.
    """

    required = (
        "decision_id", "instrument", "entry_timestamp", "exit_timestamp",
        "realized_pips", "risk_pips_per_unit", "triggered", "is_executable_candidate",
        "expected_net_edge_pips",
    )
    require_columns(outcomes, required, "OCO outcomes")
    assert_causal_selection_column(config.selection_score_column)
    require_columns(outcomes, (config.selection_score_column,), "OCO outcomes")
    if outcomes.empty:
        return ReplayResult(_empty_summary(config), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    work = outcomes.copy()
    work["entry_timestamp"] = pd.to_datetime(work["entry_timestamp"], utc=True, errors="coerce")
    work["exit_timestamp"] = pd.to_datetime(work["exit_timestamp"], utc=True, errors="coerce")
    eligible = work[
        work["triggered"].astype(bool)
        & work["is_executable_candidate"].astype(bool)
        & work["entry_timestamp"].notna()
        & work["exit_timestamp"].notna()
    ].copy()
    eligible["_selection_score"] = pd.to_numeric(
        eligible[config.selection_score_column], errors="coerce"
    )
    eligible = eligible[np.isfinite(eligible["_selection_score"])]
    eligible = eligible.sort_values(
        ["entry_timestamp", "_selection_score", "expected_net_edge_pips", "decision_id"],
        ascending=[True, False, False, True],
        kind="mergesort",
    )

    replay = _Replay(config, economics_provider, len(outcomes))
    for timestamp, group in eligible.groupby("entry_timestamp", sort=True):
        timestamp = pd.Timestamp(timestamp)
        replay.close_due(timestamp)
        opened = 0
        for row in group.to_dict("records"):
            if opened >= int(config.max_new_positions_per_timestamp):
                replay.block(row, "new_position_timestamp_cap", timestamp)
                continue
            if replay.open(row, timestamp):
                opened += 1
        replay.record_curve(timestamp)
    return replay.finish()
