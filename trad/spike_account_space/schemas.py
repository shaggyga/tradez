from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping, Protocol

import numpy as np
import pandas as pd


DECISION_REQUIRED_COLUMNS = (
    "decision_id",
    "timestamp",
    "instrument",
    "movement_score",
    "movement_threshold",
    "atr_pips",
    "spread_pips",
    "expected_net_edge_pips",
)

BAR_REQUIRED_COLUMNS = (
    "timestamp",
    "instrument",
    "bid_open",
    "bid_high",
    "bid_low",
    "bid_close",
    "ask_open",
    "ask_high",
    "ask_low",
    "ask_close",
    "pip_size",
)


def _positive(name: str, value: float, *, allow_zero: bool = False) -> None:
    valid = value >= 0 if allow_zero else value > 0
    if not np.isfinite(value) or not valid:
        comparator = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{name} must be finite and {comparator}; got {value!r}")


@dataclass(frozen=True)
class BufferConfig:
    """Causal stop-entry buffer components, expressed in pips.

    With ``combine='max'`` the widest enabled component wins.  ``sum`` adds
    the components.  This makes spread-, ATR-, and fixed-pip grids possible
    without embedding broker logic in the evaluator.
    """

    fixed_pips: float = 0.0
    spread_multiple: float = 1.0
    atr_multiple: float = 0.0
    combine: str = "max"

    def __post_init__(self) -> None:
        for name in ("fixed_pips", "spread_multiple", "atr_multiple"):
            _positive(name, float(getattr(self, name)), allow_zero=True)
        if self.combine not in {"max", "sum"}:
            raise ValueError("BufferConfig.combine must be 'max' or 'sum'")

    def pips(self, spread_pips: float, atr_pips: float) -> float:
        pieces = (
            float(self.fixed_pips),
            float(self.spread_multiple) * float(spread_pips),
            float(self.atr_multiple) * float(atr_pips),
        )
        return float(max(pieces) if self.combine == "max" else sum(pieces))


@dataclass(frozen=True)
class ExitConfig:
    """Protective exits for each filled OCO leg."""

    stop_loss_pips: float | None = 12.0
    take_profit_pips: float | None = 18.0
    stop_loss_atr_multiple: float | None = None
    take_profit_atr_multiple: float | None = None
    fixed_atr_combine: str = "max"
    trailing_start_pips: float | None = None
    trailing_distance_pips: float | None = None
    max_hold_minutes: int = 120

    def __post_init__(self) -> None:
        for name in (
            "stop_loss_pips",
            "take_profit_pips",
            "stop_loss_atr_multiple",
            "take_profit_atr_multiple",
            "trailing_start_pips",
            "trailing_distance_pips",
        ):
            value = getattr(self, name)
            if value is not None:
                _positive(name, float(value))
        if (self.trailing_start_pips is None) != (self.trailing_distance_pips is None):
            raise ValueError("trailing_start_pips and trailing_distance_pips must be set together")
        if self.fixed_atr_combine not in {"max", "min", "fixed", "atr"}:
            raise ValueError("fixed_atr_combine must be max, min, fixed, or atr")
        if self.stop_loss_pips is None and self.stop_loss_atr_multiple is None:
            # Execution evaluation can run without a stop, but account sizing
            # deliberately rejects the resulting undefined risk.
            pass
        if int(self.max_hold_minutes) < 1:
            raise ValueError("max_hold_minutes must be at least 1")

    def _resolve(self, fixed: float | None, atr_multiple: float | None, atr_pips: float) -> float | None:
        atr_value = None if atr_multiple is None else float(atr_multiple) * float(atr_pips)
        if fixed is None:
            return atr_value
        if atr_value is None:
            return float(fixed)
        if self.fixed_atr_combine == "max":
            return max(float(fixed), atr_value)
        if self.fixed_atr_combine == "min":
            return min(float(fixed), atr_value)
        return float(fixed) if self.fixed_atr_combine == "fixed" else atr_value

    def resolve(self, atr_pips: float) -> tuple[float | None, float | None]:
        _positive("atr_pips", float(atr_pips))
        return (
            self._resolve(self.stop_loss_pips, self.stop_loss_atr_multiple, atr_pips),
            self._resolve(self.take_profit_pips, self.take_profit_atr_multiple, atr_pips),
        )


@dataclass(frozen=True)
class OCOConfig:
    """Configuration for a movement-gated two-pending-order experiment."""

    range_lookback_minutes: int = 30
    minimum_history_bars: int = 2
    trigger_timeout_minutes: int = 30
    cancel_latency_minutes: int = 0
    entry_slippage_pips: float = 0.0
    exit_slippage_pips: float = 0.0
    minimum_expected_edge_pips: float | None = None
    same_bar_entry_policy: str = "adverse_first"
    buffer: BufferConfig = BufferConfig()
    exit: ExitConfig = ExitConfig()

    def __post_init__(self) -> None:
        if int(self.range_lookback_minutes) < 1:
            raise ValueError("range_lookback_minutes must be at least 1")
        if int(self.minimum_history_bars) < 1:
            raise ValueError("minimum_history_bars must be at least 1")
        if int(self.trigger_timeout_minutes) < 1:
            raise ValueError("trigger_timeout_minutes must be at least 1")
        if int(self.cancel_latency_minutes) < 0:
            raise ValueError("cancel_latency_minutes cannot be negative")
        _positive("entry_slippage_pips", float(self.entry_slippage_pips), allow_zero=True)
        _positive("exit_slippage_pips", float(self.exit_slippage_pips), allow_zero=True)
        if self.same_bar_entry_policy not in {"adverse_first", "double_fill", "skip"}:
            raise ValueError("same_bar_entry_policy must be adverse_first, double_fill, or skip")


@dataclass(frozen=True)
class AccountConfig:
    """Portfolio constraints expressed as fractions of current balance/NAV."""

    starting_balance: float = 10_000.0
    account_currency: str = "USD"
    risk_per_trade_fraction: float = 0.0025
    max_total_open_risk_fraction: float = 0.02
    max_theme_open_risk_fraction: float = 0.01
    max_margin_used_fraction: float = 0.20
    margin_closeout_buffer: float = 1.25
    max_concurrent_positions: int = 5
    max_positions_per_theme: int = 2
    max_positions_per_instrument: int = 1
    max_new_positions_per_timestamp: int = 2
    daily_loss_stop_fraction: float = 0.02
    drawdown_halt_fraction: float = 0.08
    minimum_expected_edge_pips: float = 0.0
    selection_score_column: str = "expected_net_edge_pips"
    min_units: int = 1
    unit_step: int = 1
    max_units: int | None = None
    fallback_margin_rate: float | None = None

    def __post_init__(self) -> None:
        _positive("starting_balance", float(self.starting_balance))
        for name in (
            "risk_per_trade_fraction",
            "max_total_open_risk_fraction",
            "max_theme_open_risk_fraction",
            "max_margin_used_fraction",
            "daily_loss_stop_fraction",
            "drawdown_halt_fraction",
        ):
            value = float(getattr(self, name))
            _positive(name, value, allow_zero=True)
            if value > 1:
                raise ValueError(f"{name} is a fraction and cannot exceed 1")
        _positive("margin_closeout_buffer", float(self.margin_closeout_buffer))
        for name in (
            "max_concurrent_positions",
            "max_positions_per_theme",
            "max_positions_per_instrument",
            "max_new_positions_per_timestamp",
            "min_units",
            "unit_step",
        ):
            if int(getattr(self, name)) < 1:
                raise ValueError(f"{name} must be at least 1")
        if self.max_units is not None and int(self.max_units) < int(self.min_units):
            raise ValueError("max_units must be at least min_units")
        if self.fallback_margin_rate is not None:
            _positive("fallback_margin_rate", float(self.fallback_margin_rate))
        assert_causal_selection_column(self.selection_score_column)


@dataclass(frozen=True)
class ObjectiveConfig:
    """Stable, no-trade-anchored research objective.

    The score is based on log growth, with explicit penalties for drawdown,
    tail loss, turnover/risk consumption, concentration, and fold instability.
    A no-trade run is always exactly zero.
    """

    minimum_trades_per_fold: int = 20
    drawdown_penalty: float = 1.0
    cvar_penalty: float = 1.0
    turnover_penalty: float = 0.002
    concentration_penalty: float = 0.02
    trade_shortfall_penalty: float = 0.05
    dispersion_penalty: float = 0.5
    negative_fold_penalty: float = 0.05
    hard_failure_score: float = -1_000_000.0

    def __post_init__(self) -> None:
        if int(self.minimum_trades_per_fold) < 0:
            raise ValueError("minimum_trades_per_fold cannot be negative")
        for name in (
            "drawdown_penalty",
            "cvar_penalty",
            "turnover_penalty",
            "concentration_penalty",
            "trade_shortfall_penalty",
            "dispersion_penalty",
            "negative_fold_penalty",
        ):
            _positive(name, float(getattr(self, name)), allow_zero=True)


@dataclass(frozen=True)
class TradeEconomics:
    """Account-currency economics resolved at the candidate timestamps."""

    risk_per_pip_account_per_unit: float
    # Per one filled leg.  Replay multiplies this by reserved_legs.
    margin_per_unit_account: float
    pnl_per_pip_account_per_unit: float | None = None
    realized_pnl_account_per_unit: float | None = None
    financing_per_unit_account: float = 0.0
    other_cost_per_unit_account: float = 0.0

    def __post_init__(self) -> None:
        _positive("risk_per_pip_account_per_unit", float(self.risk_per_pip_account_per_unit))
        _positive("margin_per_unit_account", float(self.margin_per_unit_account))
        if self.pnl_per_pip_account_per_unit is not None:
            _positive("pnl_per_pip_account_per_unit", float(self.pnl_per_pip_account_per_unit))
        if self.realized_pnl_account_per_unit is not None and not np.isfinite(
            float(self.realized_pnl_account_per_unit)
        ):
            raise ValueError("realized_pnl_account_per_unit must be finite")
        if self.pnl_per_pip_account_per_unit is None and self.realized_pnl_account_per_unit is None:
            raise ValueError("either pnl_per_pip or exact realized PnL per unit is required")
        for name in ("financing_per_unit_account", "other_cost_per_unit_account"):
            if not np.isfinite(float(getattr(self, name))):
                raise ValueError(f"{name} must be finite")


class EconomicsProvider(Protocol):
    """Optional bridge to a broker/account-specific conversion engine."""

    def resolve(self, trade: Mapping[str, Any], account_currency: str) -> TradeEconomics:
        ...


_FORBIDDEN_SELECTION_TOKENS = (
    "actual",
    "forward",
    "future",
    "significant",
    "realized",
    "pnl",
    "mfe",
    "mae",
    "exit",
    "outcome",
    "label",
    "target",
)


def assert_causal_selection_column(column: str) -> None:
    lowered = str(column).lower()
    if any(token in lowered for token in _FORBIDDEN_SELECTION_TOKENS):
        raise ValueError(f"selection column looks hindsight-derived: {column!r}")


def require_columns(frame: pd.DataFrame, columns: Iterable[str], label: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def normalize_decisions(decisions: pd.DataFrame) -> pd.DataFrame:
    require_columns(decisions, DECISION_REQUIRED_COLUMNS, "decisions")
    out = decisions.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="raise")
    if out["decision_id"].isna().any() or out["decision_id"].astype(str).duplicated().any():
        raise ValueError("decision_id must be non-null and unique")
    out["instrument"] = out["instrument"].astype(str)
    for col in (
        "movement_score",
        "movement_threshold",
        "atr_pips",
        "spread_pips",
        "expected_net_edge_pips",
    ):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    invalid = (
        ~np.isfinite(out["movement_score"])
        | ~np.isfinite(out["movement_threshold"])
        | ~np.isfinite(out["atr_pips"])
        | (out["atr_pips"] <= 0)
        | ~np.isfinite(out["spread_pips"])
        | (out["spread_pips"] < 0)
    )
    if invalid.any():
        ids = out.loc[invalid, "decision_id"].astype(str).head(5).tolist()
        raise ValueError(f"decisions contain invalid causal numeric values; examples: {ids}")
    if "currency_theme_cluster_id" not in out:
        out["currency_theme_cluster_id"] = "unclassified"
    out["currency_theme_cluster_id"] = out["currency_theme_cluster_id"].fillna("unclassified").astype(str)
    return out.sort_values(["timestamp", "instrument", "decision_id"], kind="mergesort").reset_index(drop=True)


def normalize_bars(bars: pd.DataFrame) -> pd.DataFrame:
    require_columns(bars, BAR_REQUIRED_COLUMNS, "bars")
    out = bars.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="raise")
    out["instrument"] = out["instrument"].astype(str)
    price_cols = [c for c in BAR_REQUIRED_COLUMNS if c not in {"timestamp", "instrument"}]
    for col in price_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    if out[list(price_cols)].isna().any(axis=None) or not np.isfinite(out[list(price_cols)].to_numpy()).all():
        raise ValueError("bars contain missing or non-finite prices")
    if (out["pip_size"] <= 0).any():
        raise ValueError("pip_size must be positive")
    bad_ohlc = (
        (out["bid_low"] > out[["bid_open", "bid_close"]].min(axis=1))
        | (out["bid_high"] < out[["bid_open", "bid_close"]].max(axis=1))
        | (out["ask_low"] > out[["ask_open", "ask_close"]].min(axis=1))
        | (out["ask_high"] < out[["ask_open", "ask_close"]].max(axis=1))
        | (out["bid_high"] < out["bid_low"])
        | (out["ask_high"] < out["ask_low"])
    )
    if bad_ohlc.any():
        raise ValueError("bars contain invalid OHLC ranges")
    for suffix in ("open", "high", "low", "close"):
        if (out[f"ask_{suffix}"] < out[f"bid_{suffix}"]).any():
            raise ValueError(f"ask_{suffix} cannot be below bid_{suffix}")
    if out.duplicated(["instrument", "timestamp"]).any():
        raise ValueError("bars must be unique by instrument and timestamp")
    return out.sort_values(["instrument", "timestamp"], kind="mergesort").reset_index(drop=True)


def config_fingerprint(value: Any, prefix: str) -> str:
    payload = asdict(value) if hasattr(value, "__dataclass_fields__") else value
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(encoded).hexdigest()[:12]}"
