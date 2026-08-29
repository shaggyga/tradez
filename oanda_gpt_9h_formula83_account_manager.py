#!/usr/bin/env python3
"""Practice-only GPT manager for nine-hour outlooks and the formula-83 scan.

The lane is isolated on OANDA practice account 013.  It reuses the existing
all-pairs GPT/news/technical manager, adds a deterministic M1 momentum score to
every market snapshot, explicitly flags USD strength/weakness, and asks GPT for
a nine-hour indexed outlook before it may submit a practice order.

Broker execution is opt-in with --execute.  Live routing and any account other
than the configured practice account suffix are rejected.
"""

from __future__ import annotations

import copy
import math
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import oanda_advisor_account_manager_auto as advisor
import oanda_gpt_exp_account_manager as gpt_exp
import oanda_gpt_prod_live_account_manager as live_prod


ACCOUNT_ID = "101-001-37981792-013"
ACCOUNT_SUFFIX = "013"
ACCOUNT_ALIAS = "OANDA_ACCOUNT_ID_GPT_9H"
LANE = "gpt_9h_formula83"
DATA_DIR = (
    advisor.SCRIPT_DIR
    / "data"
    / "forex_gpt_manager"
    / "account_gpt_9h_formula83_practice_013"
)

FORECAST_HORIZON_HOURS = 9
FORMULA_MOMENTUM_BASELINE_PIPS = 6.0
FORMULA_FLAG_THRESHOLD = 83.0
FORMULA_AGGRESSIVE_REVIEW_THRESHOLD = 78.0
FORMULA_CANDLE_FETCH_COUNT = 45
FORMULA_DETAILED_SIGNAL_LIMIT = 24
AGGRESSIVE_GPT_CALL_TIMES_NY = [
    f"{hour:02d}:{minute:02d}"
    for hour in range(24)
    for minute in (15, 45)
]

LANE_RUNTIME_OVERRIDES = {
    # During the short aggressive demo/babysit window, do not wait a full hour
    # to re-read the account and run the local profit/loss guard on open trades.
    # Keep this lane-local so the shared GPT manager's calmer cadence is not
    # changed for other accounts.
    "FOREX_LOCAL_MONITOR_INTERVAL_MINUTES": 5,
    "FOREX_LOOP_SLEEP_SECONDS": 30,
    # The demo calls GPT on :15/:45. Keep the lane throttle below 30 minutes so
    # a slow/quality-retried scan does not accidentally skip the next scheduled
    # aggressive slot, while still preventing tight retry loops.
    "FOREX_MIN_MINUTES_BETWEEN_GPT_SCANS": 12,
    # This short demo lane is intentionally aggressive, but after the local
    # monitor closes a loser early, immediately re-opening the same USD thesis
    # is not useful exploration. One same-USD-thesis loss is enough to force
    # fresh evidence or opposite/flat review until the next NY trading date.
    "FOREX_LIVE_FAILED_THESIS_MAX_LOSSES": 1,
    "FOREX_LIVE_MAX_SAME_THESIS_LOSSES": 1,
    "FOREX_LIVE_SAME_PAIR_LOSS_CAP_MAX_LOSING_CLOSES": 1,
    "FOREX_LIVE_DECISION_QUALITY_MAX_RETRIES": 2,
    # The shared live profit guard is pip-based.  During this intentionally
    # aggressive practice window, cross sizes can turn a small pip loss into a
    # materially larger account-currency drawdown before the next monitor pass.
    # Add lane-local account-currency caps so one fast adverse move is cut
    # before the broker stop becomes the only real guardrail.
    "FOREX_GPT9H_MAX_OPEN_LOSS_ACCOUNT_CCY": 150,
    "FOREX_GPT9H_MAX_TOTAL_OPEN_LOSS_ACCOUNT_CCY": 275,
    "FOREX_GPT9H_MAX_LOSS_GUARD_CLOSES_PER_PASS": 1,
}


def apply_lane_runtime_overrides() -> None:
    advisor.CONFIG.update(LANE_RUNTIME_OVERRIDES)
    for key, value in LANE_RUNTIME_OVERRIDES.items():
        if advisor.os.environ.get(key) in (None, ""):
            advisor.CREDS[key] = str(value)


SYSTEM_PROMPT_APPEND = """

Nine-hour indexed-outlook and formula-83 practice mandate:
- This is the GPT_9H_FORMULA83 lane on OANDA practice account 013. It can never
  route to the OANDA live endpoint or to a different account suffix.
- On every call, make a fresh, explicit nine-hour forward assessment. Return a
  nine_hour_forecast with the exact target time, a USD/index outlook, per-currency
  strength-index forecasts, and the strongest pair forecasts. If a current DXY
  level is not verified by supplied or web evidence, leave it null and use the
  supplied cross-pair USD proxy; never invent a market-index level.
- The packet includes a deterministic formula_83 score for every pair with enough
  M1 data. It is the historical transparent score that produced 83.2915609897 for
  SGD_JPY: a 50-point base plus capped move/spread, M5 momentum, acceleration,
  and compression components, followed by liquidity penalties and a 0-100 clamp.
- A score of 83 is a technical flag, not an 83 percent win probability and not
  permission to trade. Require the nine-hour macro/news outlook, current technical
  direction, stop geometry, expected R, non-exhaustion, and portfolio exposure to
  agree before OPEN or SCALE_IN. Contradiction means WATCH.
- Do not describe any formula_83 flag as "high probability", "probable", or
  statistically predictive unless supplied backtest evidence proves it. Use
  language like "elevated technical momentum flag" or "candidate for review."
- This lane is allowed to be aggressive on practice money: scores from 78.0 up to
  82.999 are aggressive_review candidates. They are not formula flags, but GPT
  must either justify a reduced-risk probe with current confirmation or state the
  concrete no-trade blocker. Do not leave strong near-threshold candidates vague.
- Explicitly review usd_flag. State whether USD is flashing RALLY, SELLOFF,
  CONFLICT, or NEUTRAL, name the pairs creating that flag, and prevent redundant
  same-USD-thesis stacking.
- Keep direction logic internally consistent. A USD_RALLY thesis supports long
  USD-base pairs or short USD-quote pairs; a USD_SELLOFF thesis supports the
  inverse. If you intentionally trade against usd_flag because another currency
  is stronger, say that explicitly in why_now and reduce risk.
- Under USD_RALLY, AUD_USD LONG, NZD_USD LONG, EUR_USD LONG, and GBP_USD LONG
  are USD-short trades. Do not OPEN them while already holding a USD-long
  position such as USD_CHF LONG unless usd_flag is no longer RALLY or the output
  documents a deliberate reduced-risk hedge.
- Under USD_RALLY, do not describe AUD_USD LONG, NZD_USD LONG, EUR_USD LONG, or
  GBP_USD LONG as "aligned with USD rally." They are opposite-USD expressions.
  If they look technically attractive but conflict with the portfolio USD thesis,
  put them in WATCH or REJECT with the blocker "contradicts USD_RALLY/USD_LONG
  portfolio thesis" and instead search for USD-long expressions such as USD_CHF
  LONG, USD_CAD LONG, USD_PLN LONG, USD_THB LONG, or non-USD crosses that do not
  add USD-short exposure.
- Event permissions must match the declared portfolio USD thesis. If
  portfolio_bias/portfolio_mode/summary says USD_BULLISH or usd_flag is RALLY,
  event_permissions may only include USD-long themes/directions unless a named
  hedge is explicitly reduced-risk and marked as hedge. Do not emit USD_SELLOFF
  permissions or JPY_WEAKNESS permissions whose allowed_directions imply
  USD_SHORT while the portfolio thesis is USD_LONG.
- If live_failed_thesis blocks both USD_LONG and USD_SHORT for the current NY
  date, do not let the portfolio become USD-only paralysis. Treat USD pairs as
  blocked unless the local guard says otherwise, then aggressively search
  non-USD crosses. Promote at least the best three non-USD cross setups from
  pair_bucket_coverage_review into new_trade_candidates when available. Each
  promoted cross must include direction, expected_R, stop_loss, take_profit,
  technical_confirmation, and either an executable OPEN/SCALE_IN order or a
  concrete_no_trade_blocker. If fewer than three cross candidates are available,
  state the exact spread/stop/confirmation blocker for each missing slot.
- Execution contract: if a new_trade_candidate action is OPEN or SCALE_IN and
  passes the required stop/expected_R/confirmation checks, include the matching
  order in orders_to_execute. If you are not including an executable order, the
  candidate action must be WATCH or REJECT with a concrete_no_trade_blocker.
- Compare every supplied OANDA currency pair. Manage every open trade first. Use
  only supplied instruments and exact broker trade IDs. Every executable order
  requires a stop, expected_R >= 1.0, and a concrete invalidation.
- News can update the nine-hour thesis and prioritize a watch, but current
  technical confirmation controls entry. Do not convert a headline directly into
  an order.
"""


def formula_83_components(
    *,
    move_spread_ratio: float,
    momentum_5_pips: float,
    acceleration_pips: float,
    compression_score: float,
    volatility_30_pips: float,
    spread_pips: float,
    instrument: str,
    momentum_baseline_pips: float = FORMULA_MOMENTUM_BASELINE_PIPS,
) -> Dict[str, float]:
    """Return the exact transparent score components used by the 83.29156 row."""
    base = 50.0
    move_spread = min(25.0, max(0.0, float(move_spread_ratio)) * 5.0)
    momentum = min(
        15.0,
        max(0.0, abs(float(momentum_5_pips)) / max(momentum_baseline_pips, 0.1))
        * 5.0,
    )
    acceleration = min(10.0, max(0.0, abs(float(acceleration_pips))) * 1.5)
    compression = min(8.0, max(0.0, float(compression_score) - 1.0) * 3.0)
    low_volatility_penalty = -5.0 if float(volatility_30_pips) < 0.2 else 0.0
    wide_spread_penalty = 0.0
    if float(spread_pips) > 4.0 and "JPY" not in str(instrument).upper():
        wide_spread_penalty -= min(15.0, float(spread_pips))
    extreme_spread_penalty = 0.0
    if float(spread_pips) > 12.0:
        extreme_spread_penalty -= min(20.0, float(spread_pips) / 2.0)
    raw = (
        base
        + move_spread
        + momentum
        + acceleration
        + compression
        + low_volatility_penalty
        + wide_spread_penalty
        + extreme_spread_penalty
    )
    return {
        "base": base,
        "move_spread_component": move_spread,
        "momentum_5_component": momentum,
        "acceleration_component": acceleration,
        "compression_component": compression,
        "low_volatility_penalty": low_volatility_penalty,
        "wide_spread_penalty": wide_spread_penalty,
        "extreme_spread_penalty": extreme_spread_penalty,
        "raw_score": raw,
        "score": max(0.0, min(100.0, raw)),
    }


def _population_std(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / len(values))


def _signal_theme(instrument: str, direction: str) -> str:
    inst = advisor.normalize_instrument(instrument)
    theme = "MOMENTUM"
    if "JPY" in inst:
        theme = (
            "JPY_STRENGTH"
            if direction == "SHORT" and inst.endswith("JPY")
            else "JPY_WEAKNESS"
        )
    if inst.startswith("USD") or inst.endswith("USD"):
        usd_rally = (inst.startswith("USD") and direction == "LONG") or (
            inst.endswith("USD") and direction == "SHORT"
        )
        theme = "USD_RALLY" if usd_rally else "USD_SELLOFF"
    base, quote = advisor.split_instrument(inst)
    if base in {"AUD", "NZD", "CAD"} or quote in {"AUD", "NZD", "CAD"}:
        theme = "COMMODITY_FX_" + (
            "STRENGTH" if direction == "LONG" else "WEAKNESS"
        )
    return theme


def formula_83_signal(
    instrument: str,
    rows: Sequence[Dict[str, float]],
    spread_pips: float,
) -> Dict[str, Any]:
    """Calculate the historical M1 formula from at least 40 completed candles."""
    closes = [advisor.safe_float(row.get("c"), float("nan")) for row in rows]
    closes = [value for value in closes if math.isfinite(value)]
    if len(closes) < 40:
        return {
            "available": False,
            "reason": f"requires 40 M1 closes; received {len(closes)}",
        }
    pip_size = 0.01 if "JPY" in advisor.normalize_instrument(instrument) else 0.0001
    multiplier = 1.0 / pip_size

    def momentum(bars: int) -> float:
        return (closes[-1] - closes[-1 - bars]) * multiplier

    m5 = momentum(5)
    m15 = momentum(15)
    m30 = momentum(30)
    acceleration = m5 - (m15 / 3.0)
    diffs = [
        (closes[index] - closes[index - 1]) * multiplier
        for index in range(len(closes) - 30, len(closes))
    ]
    volatility = _population_std(diffs)
    recent = closes[-15:]
    previous = closes[-40:-15]
    recent_range = (max(recent) - min(recent)) * multiplier
    previous_range = (max(previous) - min(previous)) * multiplier
    compression = previous_range / max(recent_range, 0.1) if recent_range > 0 else 1.0
    dominant = m15 if abs(m15) >= abs(m30) * 0.55 else m30
    direction = "LONG" if dominant > 0 else "SHORT"
    ratio = abs(dominant) / max(float(spread_pips), 0.01)
    components = formula_83_components(
        move_spread_ratio=ratio,
        momentum_5_pips=m5,
        acceleration_pips=acceleration,
        compression_score=compression,
        volatility_30_pips=volatility,
        spread_pips=spread_pips,
        instrument=instrument,
    )
    score = components["score"]
    return {
        "available": True,
        "instrument": advisor.normalize_instrument(instrument),
        "direction": direction,
        "theme": _signal_theme(instrument, direction),
        "momentum_5_pips": m5,
        "momentum_15_pips": m15,
        "momentum_30_pips": m30,
        "acceleration_pips": acceleration,
        "volatility_30_pips": volatility,
        "compression_score": compression,
        "move_spread_ratio": ratio,
        "spread_pips": float(spread_pips),
        "score": score,
        "flag_threshold": FORMULA_FLAG_THRESHOLD,
        "flagged": score >= FORMULA_FLAG_THRESHOLD,
        "components": components,
    }


def currency_flags(signals: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Translate flagged pair directions into per-currency and explicit USD flags."""
    events: Dict[str, List[Dict[str, Any]]] = {}
    for signal in signals:
        if not signal.get("available") or not signal.get("flagged"):
            continue
        instrument = advisor.normalize_instrument(signal.get("instrument"))
        base, quote = advisor.split_instrument(instrument)
        direction = str(signal.get("direction") or "").upper()
        score = advisor.safe_float(signal.get("score"), 0.0)
        if not base or not quote or direction not in {"LONG", "SHORT"}:
            continue
        base_bias = "STRENGTH" if direction == "LONG" else "WEAKNESS"
        quote_bias = "WEAKNESS" if direction == "LONG" else "STRENGTH"
        for currency, bias in ((base, base_bias), (quote, quote_bias)):
            events.setdefault(currency, []).append(
                {
                    "instrument": instrument,
                    "pair_direction": direction,
                    "currency_bias": bias,
                    "score": score,
                    "theme": signal.get("theme"),
                }
            )

    summaries: Dict[str, Any] = {}
    for currency, rows in sorted(events.items()):
        strength = [row for row in rows if row["currency_bias"] == "STRENGTH"]
        weakness = [row for row in rows if row["currency_bias"] == "WEAKNESS"]
        strength_score = max([row["score"] for row in strength] + [0.0])
        weakness_score = max([row["score"] for row in weakness] + [0.0])
        if strength and weakness:
            state = "CONFLICT"
        elif strength:
            state = "STRENGTH_FLASH"
        elif weakness:
            state = "WEAKNESS_FLASH"
        else:
            state = "NEUTRAL"
        summaries[currency] = {
            "state": state,
            "strength_score": strength_score,
            "weakness_score": weakness_score,
            "events": sorted(rows, key=lambda row: row["score"], reverse=True),
        }

    usd = copy.deepcopy(summaries.get("USD", {}))
    state = usd.get("state", "NEUTRAL")
    usd["state"] = {
        "STRENGTH_FLASH": "RALLY",
        "WEAKNESS_FLASH": "SELLOFF",
        "CONFLICT": "CONFLICT",
    }.get(state, "NEUTRAL")
    usd.setdefault("strength_score", 0.0)
    usd.setdefault("weakness_score", 0.0)
    usd.setdefault("events", [])
    return {"currencies": summaries, "usd_flag": usd}


def compact_signal_tape(signals: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep all-pair coverage in the prompt without sending every component."""
    tape: List[Dict[str, Any]] = []
    for signal in signals:
        score = advisor.safe_float(signal.get("score"), 0.0)
        tape.append(
            {
                "instrument": signal.get("instrument"),
                "direction": signal.get("direction"),
                "theme": signal.get("theme"),
                "score": round(score, 3),
                "flagged": bool(signal.get("flagged")),
                "aggressive_review": score >= FORMULA_AGGRESSIVE_REVIEW_THRESHOLD,
                "spread_pips": round(advisor.safe_float(signal.get("spread_pips"), 0.0), 3),
                "m5_pips": round(advisor.safe_float(signal.get("momentum_5_pips"), 0.0), 2),
                "m15_pips": round(advisor.safe_float(signal.get("momentum_15_pips"), 0.0), 2),
            }
        )
    return tape


def apply_decision_schema_extensions() -> None:
    """Describe the nine-hour response contract to the Responses API."""
    properties = advisor.DECISION_SCHEMA.setdefault("properties", {})
    properties["nine_hour_forecast"] = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "horizon_hours": {"type": "number"},
            "target_time_utc": {"type": "string"},
            "market_index_name": {"type": "string"},
            "market_index_now": {"type": ["number", "null"]},
            "market_index_9h": {"type": ["number", "null"]},
            "market_index_source_status": {"type": "string"},
            "usd_direction": {
                "type": "string",
                "enum": ["RALLY", "SELLOFF", "CONFLICT", "NEUTRAL"],
            },
            "confidence": {"type": "number"},
            "currency_index_forecasts": {"type": "array", "items": {"type": "object"}},
            "pair_forecasts": {"type": "array", "items": {"type": "object"}},
            "reason": {"type": "string"},
            "invalidation": {"type": "string"},
        },
    }
    properties["usd_flag_review"] = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "state": {
                "type": "string",
                "enum": ["RALLY", "SELLOFF", "CONFLICT", "NEUTRAL"],
            },
            "flagged_pairs": {"type": "array", "items": {"type": "string"}},
            "interpretation": {"type": "string"},
        },
    }


def _account_id_from_env_or_default() -> str:
    return gpt_exp._cred_or_env(ACCOUNT_ALIAS, default=ACCOUNT_ID)


def build_config(
    base_cfg: advisor.BotConfig,
    *,
    execute_requested: bool,
) -> advisor.BotConfig:
    account_id = _account_id_from_env_or_default()
    if account_id != ACCOUNT_ID or not account_id.endswith(f"-{ACCOUNT_SUFFIX}"):
        raise RuntimeError(
            f"{LANE} is pinned to OANDA practice account {ACCOUNT_SUFFIX}; "
            f"resolved account was {account_id!r}"
        )
    inherited = gpt_exp.build_exp_config(
        base_cfg,
        execute_requested=bool(execute_requested),
    )
    cfg = replace(
        inherited,
        oanda_env="practice",
        oanda_account_id=account_id,
        account_lane=LANE,
        account_display_name=f"{ACCOUNT_ALIAS} / GPT_9H_FORMULA83_DEMO",
        instrument_filter_mode="all",
        gpt_reliable_pairs=[],
        technical_scout_pairs=[],
        execute_trades=bool(execute_requested),
        allow_live=False,
        include_candles=False,
        candle_granularity="M1",
        candle_count=FORMULA_CANDLE_FETCH_COUNT,
        max_open_trades=8,
        max_new_trades_per_scan=5,
        max_total_new_risk_pct_per_scan=8.0,
        max_risk_pct_per_trade=2.0,
        target_margin_used_pct=55.0,
        max_margin_used_pct=78.0,
        emergency_margin_used_pct=82.0,
        max_one_currency_net_units_pct=45.0,
        min_minutes_between_gpt_scans=12,
        call_times_ny=list(AGGRESSIVE_GPT_CALL_TIMES_NY),
        friday_call_times_ny=list(AGGRESSIVE_GPT_CALL_TIMES_NY),
        event_scout_trades_enabled=False,
        event_require_gpt_permission=True,
    )
    return advisor.with_data_dir(cfg, DATA_DIR)


class NineHourFormula83Manager(gpt_exp.BroadNewsForexManager):
    """All-pairs practice manager with deterministic formula and USD flags."""

    def __init__(self, cfg: advisor.BotConfig):
        if cfg.oanda_env != "practice" or cfg.allow_live:
            raise RuntimeError("Nine-hour formula-83 manager is practice-only.")
        if cfg.oanda_account_id != ACCOUNT_ID:
            raise RuntimeError("Nine-hour formula-83 manager account pin failed.")
        super().__init__(cfg)
        if "api-fxpractice.oanda.com" not in self.oanda.base_url.lower():
            raise RuntimeError("Nine-hour formula-83 manager refused a non-practice endpoint.")

    def live_expectancy_context(self) -> Dict[str, Any]:
        context = super().live_expectancy_context()
        weak_buckets = context.setdefault("weak_score_buckets", [])
        if isinstance(weak_buckets, list) and not any(
            isinstance(item, dict) and item.get("bucket") == "70_79"
            for item in weak_buckets
        ):
            weak_buckets.append(
                {
                    "bucket": "70_79",
                    "closed": 0,
                    "loss_rate": 0.0,
                    "realized_pl": 0.0,
                    "source": "gpt_9h_formula83_aggressive_review_cap",
                }
            )
        return context

    def should_execute(self) -> bool:
        base_url = str(getattr(self.oanda, "base_url", "") or "").lower()
        if (
            self.cfg.oanda_env != "practice"
            or self.cfg.allow_live
            or self.cfg.oanda_account_id != ACCOUNT_ID
            or "api-fxpractice.oanda.com" not in base_url
        ):
            return False
        return super().should_execute()

    @staticmethod
    def _lane_float_setting(key: str, default: float) -> float:
        return advisor.safe_float(
            advisor.os.environ.get(key) or advisor.CREDS.get(key),
            default,
        )

    @staticmethod
    def _lane_int_setting(key: str, default: int) -> int:
        return int(
            max(
                0,
                advisor.safe_float(
                    advisor.os.environ.get(key) or advisor.CREDS.get(key),
                    float(default),
                ),
            )
        )

    def run_formula83_aggressive_loss_guard(
        self,
        *,
        reason: str = "gpt_9h_formula83_aggressive_loss_guard",
        open_trades: List[Dict[str, Any]] | None = None,
    ) -> int:
        if not self.should_execute():
            return 0
        per_trade_cap = abs(
            self._lane_float_setting("FOREX_GPT9H_MAX_OPEN_LOSS_ACCOUNT_CCY", 150.0)
        )
        total_cap = abs(
            self._lane_float_setting("FOREX_GPT9H_MAX_TOTAL_OPEN_LOSS_ACCOUNT_CCY", 275.0)
        )
        max_closes = self._lane_int_setting(
            "FOREX_GPT9H_MAX_LOSS_GUARD_CLOSES_PER_PASS",
            1,
        )
        if max_closes <= 0 or (per_trade_cap <= 0 and total_cap <= 0):
            return 0
        if open_trades is None:
            try:
                open_trades = self.oanda.get_open_trades()
            except Exception as exc:
                self.log_error("formula83 aggressive loss guard open trades", exc)
                return 0
        losing = [
            trade
            for trade in (open_trades or [])
            if advisor.safe_float(trade.get("unrealizedPL"), 0.0) < 0
            and str(trade.get("id") or "").strip()
        ]
        if not losing:
            return 0
        total_open_loss = sum(
            abs(advisor.safe_float(trade.get("unrealizedPL"), 0.0))
            for trade in losing
        )
        if total_open_loss < total_cap and all(
            abs(advisor.safe_float(trade.get("unrealizedPL"), 0.0)) < per_trade_cap
            for trade in losing
        ):
            return 0

        closed = 0
        for trade in sorted(losing, key=lambda item: advisor.safe_float(item.get("unrealizedPL"), 0.0)):
            if closed >= max_closes:
                break
            unrealized_pl = advisor.safe_float(trade.get("unrealizedPL"), 0.0)
            loss_abs = abs(unrealized_pl)
            if loss_abs < per_trade_cap and total_open_loss < total_cap:
                continue
            action = {
                "instrument": trade.get("instrument"),
                "trade_id": str(trade.get("id") or "").strip(),
                "action": "CLOSE",
                "direction": advisor.trade_direction_from_units(trade.get("currentUnits")),
                "outlook_confidence": 0,
                "risk_pct": 0,
                "reason": (
                    f"{reason}: close worst loser; unrealizedPL={unrealized_pl:.2f}, "
                    f"open_loss={total_open_loss:.2f}, per_trade_cap={per_trade_cap:.2f}, "
                    f"total_cap={total_cap:.2f}"
                ),
            }
            try:
                status = self.close_trade(trade, action)
            except Exception as exc:
                self.log_error("formula83 aggressive loss guard close", exc)
                continue
            if status in {"accepted", "dry_run", "accepted_after_recheck"}:
                closed += 1
                total_open_loss = max(0.0, total_open_loss - loss_abs)
        if closed:
            advisor.log(f"{self.lane_prefix()}Aggressive loss guard closed {closed} trade(s).")
        return closed

    def run_local_monitor(self) -> None:
        try:
            self.run_formula83_aggressive_loss_guard(reason="pre_local_monitor")
        except Exception as exc:
            self.log_error("formula83 aggressive loss guard from local monitor", exc)
        super().run_local_monitor()

    def apply_live_stop_policy(self, decision: Dict[str, Any]) -> None:
        super().apply_live_stop_policy(decision)
        min_fraction = 0.50
        for section in ("orders_to_execute", "open_position_actions", "new_trade_candidates"):
            for obj in decision.get(section) or []:
                if not isinstance(obj, dict):
                    continue
                action = str(obj.get("action") or "").upper().strip()
                if section in {"orders_to_execute", "new_trade_candidates"} and action not in {"OPEN", "SCALE_IN"}:
                    continue
                if section == "open_position_actions" and action not in {"TIGHTEN", "FLIP"}:
                    continue
                trailing = advisor.safe_float(obj.get("trailing_stop_pips"), 0.0)
                if trailing <= 0:
                    continue
                critic = obj.get("expectancy_critic")
                if not isinstance(critic, dict):
                    critic = {}
                stop_pips = advisor.safe_float(
                    critic.get("recommended_stop_pips")
                    or critic.get("invalidation_pips")
                    or obj.get("recommended_stop_pips")
                    or obj.get("invalidation_pips"),
                    0.0,
                )
                if stop_pips <= 0:
                    continue
                minimum = stop_pips * min_fraction
                if trailing >= minimum:
                    continue
                note = (
                    f"gpt_9h_formula83 removed undersized trailing_stop_pips {trailing:g}; "
                    f"minimum is {minimum:g}p from recommended stop {stop_pips:g}p"
                )
                obj["trailing_stop_pips"] = None
                obj["lane_trailing_stop_policy_adjustment"] = note
                obj["reason"] = (str(obj.get("reason") or "") + " " + note).strip()

    @staticmethod
    def _instrument_has_usd(instrument: Any) -> bool:
        inst = advisor.normalize_instrument(instrument)
        parts = inst.split("_")
        return len(parts) == 2 and "USD" in parts

    def _non_usd_cross_candidate_count(self, decision: Dict[str, Any]) -> int:
        count = 0
        seen: set[str] = set()
        for candidate in decision.get("new_trade_candidates") or []:
            if not isinstance(candidate, dict):
                continue
            inst = advisor.normalize_instrument(candidate.get("instrument", ""))
            if not inst or inst in seen or self._instrument_has_usd(inst):
                continue
            action = str(candidate.get("action") or "").upper().strip()
            if action not in {"OPEN", "SCALE_IN", "WATCH"}:
                continue
            seen.add(inst)
            count += 1
        return count

    def _managed_non_usd_cross_position_count(self, decision: Dict[str, Any]) -> int:
        count = 0
        seen: set[str] = set()
        for action_item in decision.get("open_position_actions") or []:
            if not isinstance(action_item, dict):
                continue
            inst = advisor.normalize_instrument(action_item.get("instrument", ""))
            if not inst or inst in seen or self._instrument_has_usd(inst):
                continue
            action = str(action_item.get("action") or "").upper().strip()
            if action not in {"HOLD", "TIGHTEN", "PARTIAL_CLOSE", "CLOSE"}:
                continue
            if not str(action_item.get("trade_id") or "").strip():
                continue
            seen.add(inst)
            count += 1
        return count

    def _reviewed_non_usd_cross_count(self, decision: Dict[str, Any]) -> int:
        seen: set[str] = set()
        for review in decision.get("pair_bucket_coverage_review") or []:
            if not isinstance(review, dict):
                continue
            instruments = list(review.get("reviewed_instruments") or [])
            best = review.get("best_instrument")
            if best:
                instruments.append(best)
            for instrument in instruments:
                inst = advisor.normalize_instrument(instrument)
                if inst and not self._instrument_has_usd(inst):
                    seen.add(inst)
        return len(seen)

    def normalize_decision(
        self,
        decision: Dict[str, Any],
        open_trades: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        decision = super().normalize_decision(decision, open_trades)
        self.sanitize_blocked_candidate_event_permissions(decision)
        self.sanitize_correlated_open_trade_event_permissions(decision, open_trades)
        open_instruments = {
            advisor.normalize_instrument(trade.get("instrument", ""))
            for trade in open_trades
            if isinstance(trade, dict)
            and advisor.safe_float(trade.get("currentUnits"), 0.0) != 0.0
        }
        open_instruments.discard("")

        cleanup: List[Dict[str, Any]] = []
        if open_instruments:
            for candidate in decision.get("new_trade_candidates") or []:
                if not isinstance(candidate, dict):
                    continue
                inst = advisor.normalize_instrument(candidate.get("instrument", ""))
                action = str(candidate.get("action") or "").upper().strip()
                if action != "OPEN" or inst not in open_instruments:
                    continue
                blocker = (
                    "Position already open; manage this instrument via open_position_actions "
                    "or submit an explicit SCALE_IN order with fresh risk justification."
                )
                original_reason = str(candidate.get("reason") or "").strip()
                candidate["action"] = "HOLD"
                candidate["concrete_no_trade_blocker"] = blocker
                candidate["reason"] = (
                    f"{blocker} Original OPEN candidate reason: {original_reason}"
                    if original_reason
                    else blocker
                )
                candidate["duplicate_open_candidate_blocked"] = blocker
                cleanup.append(
                    {
                        "instrument": inst,
                        "original_action": action,
                        "new_action": "HOLD",
                        "reason": blocker,
                    }
                )
        if cleanup:
            decision["duplicate_open_candidate_cleanup"] = cleanup
        self.sanitize_unmatched_open_candidates(decision)
        self.dedupe_blocked_watch_candidates(decision)
        self.prioritize_non_usd_candidates_when_usd_theses_blocked(decision)
        return decision

    def sanitize_unmatched_open_candidates(self, decision: Dict[str, Any]) -> None:
        executable_keys = {
            (
                advisor.normalize_instrument(order.get("instrument", "")),
                str(order.get("direction") or "").upper().strip(),
            )
            for order in (decision.get("orders_to_execute") or [])
            if isinstance(order, dict)
            and str(order.get("action") or "").upper().strip() in {"OPEN", "SCALE_IN"}
        }
        adjustments: List[Dict[str, Any]] = []
        for candidate in decision.get("new_trade_candidates") or []:
            if not isinstance(candidate, dict):
                continue
            action = str(candidate.get("action") or "").upper().strip()
            if action not in {"OPEN", "SCALE_IN"}:
                continue
            inst = advisor.normalize_instrument(candidate.get("instrument", ""))
            direction = str(candidate.get("direction") or "").upper().strip()
            if (inst, direction) in executable_keys:
                continue
            blocker = (
                "No matching executable order survived local risk cleanup; "
                "candidate is watch-only until a fresh order passes all guards."
            )
            original_reason = str(candidate.get("reason") or "").strip()
            candidate["action"] = "WATCH"
            candidate["concrete_no_trade_blocker"] = blocker
            candidate["unmatched_open_candidate_blocked"] = blocker
            candidate["reason"] = (
                f"{blocker} Original candidate reason: {original_reason}"
                if original_reason
                else blocker
            )
            adjustments.append(
                {
                    "instrument": inst,
                    "direction": direction,
                    "original_action": action,
                    "new_action": "WATCH",
                    "reason": blocker,
                }
            )
        if adjustments:
            decision["unmatched_open_candidate_cleanup"] = adjustments

    def dedupe_blocked_watch_candidates(self, decision: Dict[str, Any]) -> None:
        candidates = decision.get("new_trade_candidates")
        if not isinstance(candidates, list) or len(candidates) < 2:
            return
        seen: set[tuple[str, str, str, str]] = set()
        kept: List[Any] = []
        removed: List[Dict[str, Any]] = []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                kept.append(candidate)
                continue
            inst = advisor.normalize_instrument(candidate.get("instrument", ""))
            direction = str(candidate.get("direction") or "").upper().strip()
            action = str(candidate.get("action") or "").upper().strip()
            blocker = str(
                candidate.get("same_pair_loss_cap_blocked")
                or candidate.get("concrete_no_trade_blocker")
                or ""
            ).strip()
            key = (inst, direction, action, blocker)
            if action in {"WATCH", "REJECT"} and blocker and key in seen:
                removed.append(
                    {
                        "instrument": inst,
                        "direction": direction,
                        "action": action,
                        "reason": blocker,
                    }
                )
                continue
            seen.add(key)
            kept.append(candidate)
        if removed:
            decision["new_trade_candidates"] = kept
            decision["duplicate_blocked_candidate_cleanup"] = removed

    def prioritize_non_usd_candidates_when_usd_theses_blocked(
        self,
        decision: Dict[str, Any],
    ) -> None:
        """Keep USD-blocked flat reviews focused on actual non-USD alternatives."""
        failed = decision.get("live_failed_thesis")
        blocked = set()
        if isinstance(failed, dict) and failed.get("active"):
            blocked = {str(key).upper() for key in (failed.get("blocked_keys") or [])}
        if not {"USD_LONG", "USD_SHORT"}.issubset(blocked):
            return
        if decision.get("orders_to_execute"):
            return
        candidates = decision.get("new_trade_candidates")
        if not isinstance(candidates, list) or len(candidates) < 2:
            return

        indexed = [
            (idx, candidate)
            for idx, candidate in enumerate(candidates)
            if isinstance(candidate, dict)
        ]
        if len(indexed) != len(candidates):
            return
        reordered = sorted(
            indexed,
            key=lambda item: (
                self._instrument_has_usd(item[1].get("instrument", "")),
                item[0],
            ),
        )
        if [idx for idx, _candidate in reordered] == list(range(len(candidates))):
            return
        decision["new_trade_candidates"] = [candidate for _idx, candidate in reordered]
        decision["non_usd_candidate_priority_adjustment"] = {
            "reason": (
                "USD_LONG and USD_SHORT are blocked; true non-USD cross candidates "
                "were moved ahead of USD-base/USD-quote WATCH items."
            ),
            "blocked_usd_keys": sorted(blocked),
            "original_order": [
                advisor.normalize_instrument(candidate.get("instrument", ""))
                for _idx, candidate in indexed
            ],
            "new_order": [
                advisor.normalize_instrument(candidate.get("instrument", ""))
                for _idx, candidate in reordered
            ],
        }

    def sanitize_blocked_candidate_event_permissions(self, decision: Dict[str, Any]) -> None:
        blocked_by_pair: Dict[str, str] = {}
        for candidate in decision.get("new_trade_candidates") or []:
            if not isinstance(candidate, dict):
                continue
            action = str(candidate.get("action") or "").upper().strip()
            if action not in {"WATCH", "REJECT"}:
                continue
            inst = advisor.normalize_instrument(candidate.get("instrument", ""))
            blocker = str(candidate.get("concrete_no_trade_blocker") or "").strip()
            if not blocker and action in {"WATCH", "REJECT"}:
                blocker = str(candidate.get("reason") or "").strip()
            if inst and blocker:
                blocked_by_pair[inst] = blocker
        if not blocked_by_pair:
            return

        kept_permissions: List[Dict[str, Any]] = []
        removed_permissions: List[Dict[str, Any]] = []
        for permission in decision.get("event_permissions") or []:
            if not isinstance(permission, dict):
                kept_permissions.append(permission)
                continue
            allowed_pairs = [
                advisor.normalize_instrument(pair)
                for pair in (permission.get("allowed_pairs") or [])
            ]
            allowed_pairs = [pair for pair in allowed_pairs if pair]
            allowed_directions = permission.get("allowed_directions")
            if not isinstance(allowed_directions, dict):
                allowed_directions = {}
            removed_pairs = [pair for pair in allowed_pairs if pair in blocked_by_pair]
            if not removed_pairs:
                kept_permissions.append(permission)
                continue

            next_permission = dict(permission)
            next_permission["allowed_pairs"] = [
                pair for pair in allowed_pairs if pair not in blocked_by_pair
            ]
            next_permission["allowed_directions"] = {
                advisor.normalize_instrument(pair): direction
                for pair, direction in allowed_directions.items()
                if advisor.normalize_instrument(pair) not in blocked_by_pair
            }
            removal_note = {
                **permission,
                "blocked_candidate_event_permission_removed": (
                    "event permission pair removed because the same decision marked "
                    "the pair WATCH/REJECT with a concrete no-trade blocker"
                ),
                "removed_pairs": [
                    {
                        "instrument": pair,
                        "concrete_no_trade_blocker": blocked_by_pair[pair],
                    }
                    for pair in removed_pairs
                ],
            }
            removed_permissions.append(removal_note)
            if next_permission["allowed_pairs"]:
                kept_permissions.append(next_permission)

        if removed_permissions:
            decision["event_permissions"] = kept_permissions
            decision.setdefault("blocked_event_permissions", []).extend(removed_permissions)

    def sanitize_correlated_open_trade_event_permissions(
        self,
        decision: Dict[str, Any],
        open_trades: List[Dict[str, Any]],
    ) -> None:
        permissions = decision.get("event_permissions")
        if not isinstance(permissions, list) or not permissions:
            return

        active_keys: set[str] = set()
        for trade in open_trades or []:
            if not isinstance(trade, dict):
                continue
            units = advisor.safe_float(trade.get("currentUnits"), 0.0)
            if units == 0:
                continue
            inst = advisor.normalize_instrument(trade.get("instrument", ""))
            direction = advisor.trade_direction_from_units(units)
            active_keys.update(self.currency_direction_keys(inst, direction))
        if not active_keys:
            return

        kept_permissions: List[Dict[str, Any]] = []
        removed_permissions: List[Dict[str, Any]] = []
        adjustments: List[Dict[str, Any]] = []
        for permission in permissions:
            if not isinstance(permission, dict):
                kept_permissions.append(permission)
                continue
            allowed_directions = permission.get("allowed_directions")
            if not isinstance(allowed_directions, dict):
                kept_permissions.append(permission)
                continue

            kept_dirs: Dict[str, str] = {}
            removed: List[Dict[str, Any]] = []
            for pair_raw, direction_raw in allowed_directions.items():
                inst = advisor.normalize_instrument(pair_raw)
                direction = str(direction_raw or "").upper().strip()
                if not inst or direction not in {"LONG", "SHORT"}:
                    continue
                overlap = sorted(
                    key
                    for key in self.currency_direction_keys(inst, direction)
                    if key in active_keys
                )
                if overlap:
                    removed.append(
                        {
                            "instrument": inst,
                            "direction": direction,
                            "overlapping_active_thesis_keys": overlap,
                        }
                    )
                    continue
                kept_dirs[inst] = direction

            if not removed:
                kept_permissions.append(permission)
                continue

            note = (
                "Local lane guard removed event-permission pairs that would add "
                "correlated exposure to an already-open directional currency thesis."
            )
            adjusted = dict(permission)
            adjusted["allowed_directions"] = kept_dirs
            adjusted["allowed_pairs"] = [
                pair
                for pair in (permission.get("allowed_pairs") or [])
                if advisor.normalize_instrument(pair) in kept_dirs
            ]
            adjusted["reason"] = (str(adjusted.get("reason") or "") + " " + note).strip()
            adjusted["correlated_open_trade_event_permission_sanitizer"] = {
                "active_thesis_keys": sorted(active_keys),
                "removed": removed,
                "kept_allowed_directions": kept_dirs,
            }
            adjustments.append(
                {
                    "theme": permission.get("theme", ""),
                    "removed": removed,
                    "kept_allowed_directions": kept_dirs,
                }
            )
            if kept_dirs:
                kept_permissions.append(adjusted)
            else:
                blocked_item = dict(permission)
                blocked_item["correlated_open_trade_event_permission_blocked"] = note
                blocked_item["active_thesis_keys"] = sorted(active_keys)
                blocked_item["removed"] = removed
                removed_permissions.append(blocked_item)

        if not adjustments:
            return
        decision["event_permissions"] = kept_permissions
        if removed_permissions:
            decision.setdefault("blocked_event_permissions", []).extend(removed_permissions)
        decision["correlated_open_trade_event_permission_adjustments"] = adjustments

    def annotate_live_decision_quality(
        self,
        decision: Dict[str, Any],
        prices: Dict[str, Dict[str, Any]],
        open_trades: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        review = super().annotate_live_decision_quality(decision, prices, open_trades)
        failed = decision.get("live_failed_thesis")
        blocked = set()
        if isinstance(failed, dict):
            blocked = {str(key).upper() for key in (failed.get("blocked_keys") or [])}
        if not {"USD_LONG", "USD_SHORT"}.issubset(blocked):
            return review
        if decision.get("orders_to_execute"):
            return review
        non_usd_candidates = self._non_usd_cross_candidate_count(decision)
        managed_non_usd_positions = self._managed_non_usd_cross_position_count(decision)
        reviewed_crosses = self._reviewed_non_usd_cross_count(decision)
        target = 1 if managed_non_usd_positions > 0 else min(3, reviewed_crosses)
        accounted_for = non_usd_candidates + managed_non_usd_positions
        if target <= 0 or accounted_for >= target:
            return review
        issue = (
            "Both USD_LONG and USD_SHORT are blocked by live_failed_thesis, "
            f"but only {accounted_for} non-USD cross candidate/managed-position item(s) were promoted "
            f"from {reviewed_crosses} reviewed cross instrument(s); promote the best "
            "available non-USD crosses or state exact blockers."
        )
        issues = review.setdefault("issues", [])
        if isinstance(issues, list) and issue not in issues:
            issues.append(issue)
        review["issue_count"] = len(review.get("issues") or [])
        review["local_verdict"] = "retry_decision_quality"
        review["non_usd_fallback_accountability"] = {
            "required": True,
            "blocked_usd_keys": sorted(blocked),
            "non_usd_candidates": non_usd_candidates,
            "managed_non_usd_positions": managed_non_usd_positions,
            "accounted_for": accounted_for,
            "reviewed_cross_instruments": reviewed_crosses,
            "target_non_usd_candidates": target,
        }
        decision["live_decision_quality_review"] = review
        return review

    def build_market_packet(
        self,
    ) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
        packet, prices, open_trades, raw_context = super().build_market_packet()
        snapshots = packet.get("market_snapshots", [])
        signals: List[Dict[str, Any]] = []
        failures: List[Dict[str, str]] = []
        for snapshot in snapshots if isinstance(snapshots, list) else []:
            if not isinstance(snapshot, dict):
                continue
            instrument = advisor.normalize_instrument(snapshot.get("instrument"))
            meta = self.instrument_meta.get(instrument)
            if not instrument or meta is None:
                continue
            try:
                # OANDA includes the current incomplete candle in the requested
                # count and this client deliberately removes it. Fetch a buffer
                # so the formula still receives at least 40 completed M1 bars.
                candles = self.oanda.get_candles(
                    instrument,
                    "M1",
                    FORMULA_CANDLE_FETCH_COUNT,
                )
                rows = advisor.candles_to_rows(candles)
                spread = advisor.safe_float(snapshot.get("spread_pips"), 0.0)
                signal = formula_83_signal(instrument, rows, spread)
                snapshot["recent_bars"] = advisor.candle_snapshot(rows, meta)
                snapshot["formula_83"] = signal
                signals.append(signal)
            except Exception as exc:
                signal = {"available": False, "instrument": instrument, "reason": str(exc)[:200]}
                snapshot["formula_83"] = signal
                signals.append(signal)
                failures.append({"instrument": instrument, "error": str(exc)[:200]})

        ranked = sorted(
            [signal for signal in signals if signal.get("available")],
            key=lambda signal: advisor.safe_float(signal.get("score"), 0.0),
            reverse=True,
        )
        flags = currency_flags(ranked)
        aggressive_review = [
            signal
            for signal in ranked
            if advisor.safe_float(signal.get("score"), 0.0) >= FORMULA_AGGRESSIVE_REVIEW_THRESHOLD
        ]
        target = advisor.utc_now() + advisor.dt.timedelta(hours=FORECAST_HORIZON_HOURS)
        advisor.log(
            f"{self.lane_prefix()}Formula-83 scan: scored={len(ranked)} "
            f"flagged={sum(1 for signal in ranked if signal.get('flagged'))} "
            f"aggressive_review={len(aggressive_review)} failures={len(failures)} "
            f"usd_flag={flags['usd_flag']['state']}"
        )
        packet["forecast_horizon"] = {
            "hours": FORECAST_HORIZON_HOURS,
            "target_time_utc": target.isoformat(),
            "instruction": "Forecast the supplied currency/index and pair outlook exactly to this target time.",
        }
        packet["formula_83_technical_scan"] = {
            "formula_name": "historical_m1_transparent_score_v1",
            "momentum_baseline_pips": FORMULA_MOMENTUM_BASELINE_PIPS,
            "flag_threshold": FORMULA_FLAG_THRESHOLD,
            "aggressive_review_threshold": FORMULA_AGGRESSIVE_REVIEW_THRESHOLD,
            "meaning": "Technical prioritization flag only; not probability and not trade permission.",
            "historical_reconstruction": {
                "instrument": "SGD_JPY",
                "score": 83.29156098971292,
                "components": [50.0, 13.85435168738623, 6.500000000000247, 9.10000000000082, 3.837209302325627],
            },
            "scored_pair_count": len(ranked),
            "flagged_pair_count": sum(1 for signal in ranked if signal.get("flagged")),
            "aggressive_review_pair_count": len(aggressive_review),
            "aggressive_review_signals": aggressive_review[:16],
            "ranked_signals": ranked[:FORMULA_DETAILED_SIGNAL_LIMIT],
            "ranked_signal_detail_limit": FORMULA_DETAILED_SIGNAL_LIMIT,
            "all_pair_score_tape": compact_signal_tape(ranked),
            "fetch_failures": failures,
        }
        packet["currency_formula_flags"] = flags["currencies"]
        packet["usd_flag"] = flags["usd_flag"]
        packet["all68_bucket_shortlist"] = self.build_all68_bucket_shortlist(snapshots)

        instructions = packet.setdefault("instructions", {})
        instructions["experimental_lane"] = (
            "GPT_9H_FORMULA83 on isolated OANDA practice account 013. "
            "Nine-hour outlook plus current formula-83 confirmation controls entries."
        )
        instructions["nine_hour_forecast_required"] = (
            "Return nine_hour_forecast for the exact forecast_horizon target. Use a verified DXY level "
            "when available; otherwise identify the market index as CROSS_PAIR_USD_PROXY and use usd_flag."
        )
        instructions["usd_flag_required"] = (
            "Review usd_flag explicitly and return usd_flag_review. Conflicting USD flashes block "
            "same-thesis USD stacking unless a concrete pair-specific exception is documented."
        )
        instructions["usd_direction_consistency"] = (
            "For USD_RALLY, prefer USD-base LONG or USD-quote SHORT. For USD_SELLOFF, prefer "
            "USD-base SHORT or USD-quote LONG. Any exception must name the stronger non-USD "
            "currency evidence and use reduced risk."
        )
        instructions["formula_83_gate"] = (
            "Use formula_83 to prioritize current technical flow. Score >= 83 is a flag only; "
            "OPEN still requires aligned nine-hour thesis, confirmation, expected R, stop, and exposure fit."
        )
        instructions["aggressive_review_gate"] = (
            "For formula_83 scores >= 78, either propose a reduced-risk practice probe with exact current "
            "confirmation and invalidation, or name the concrete no-trade blocker. Do not leave these as "
            "generic WATCH candidates."
        )
        instructions["candidate_order_consistency"] = (
            "If new_trade_candidates contains action OPEN or SCALE_IN, orders_to_execute must contain the "
            "matching executable broker action. If no order is included, the candidate action must be WATCH "
            "or REJECT and must state the blocker."
        )
        guardrails = packet.setdefault("config_guardrails", {})
        live_profile = guardrails.setdefault("live_money_profile", {})
        if isinstance(live_profile, dict):
            live_profile.update(
                {
                    "mode": "practice_demo_explicit_execute_only",
                    "execute_trades": self.cfg.execute_trades,
                    "allow_live": False,
                    "account_suffix": ACCOUNT_SUFFIX,
                    "operator_note": "No live-money authorization exists in this lane.",
                }
            )
        guardrails["route_policy"] = (
            "Practice account 013 only; GPT orders require the nine-hour plus formula-83 gate."
        )
        packet["experimental_lane_profile"] = {
            "lane": LANE,
            "name": self.cfg.account_display_name,
            "account_suffix": ACCOUNT_SUFFIX,
            "primary_goal": "Nine-hour indexed outlook with all-pair formula-83 and explicit USD flagging.",
        }
        output = packet.setdefault("output_contract", {})
        output["nine_hour_forecast"] = (
            "Mandatory exact-target forecast with verified market-index level or named cross-pair proxy, "
            "per-currency index forecasts, and strongest pair forecasts."
        )
        output["usd_flag_review"] = (
            "Mandatory RALLY/SELLOFF/CONFLICT/NEUTRAL review naming the formula-flagged pairs."
        )
        return packet, prices, open_trades, raw_context


def build_arg_parser() -> Any:
    parser = gpt_exp.build_arg_parser()
    parser.description = (
        "Practice-only all-pairs GPT manager with a nine-hour outlook, formula-83 scan, and USD flags."
    )
    return parser


def main() -> int:
    live_prod.apply_live_runtime_overrides()
    apply_lane_runtime_overrides()
    live_prod.apply_live_decision_schema_extensions()
    apply_decision_schema_extensions()
    if SYSTEM_PROMPT_APPEND not in advisor.SYSTEM_PROMPT:
        advisor.SYSTEM_PROMPT = advisor.SYSTEM_PROMPT + SYSTEM_PROMPT_APPEND

    args = build_arg_parser().parse_args()
    if args.dry_run and args.execute:
        raise SystemExit("Choose either --dry-run or --execute, not both.")
    base_cfg = advisor.BotConfig.load()
    execute_requested = bool(args.execute and not args.dry_run)
    base_cfg.execute_trades = execute_requested
    base_cfg.scan_on_launch = not args.no_scan_on_launch
    if args.normal_mode:
        base_cfg.shutdown_mode = "off"
    if args.shutdown_fade:
        base_cfg.shutdown_mode = "fade"
        base_cfg.shutdown_block_new_trades = True
    if args.shutdown_close_now:
        base_cfg.shutdown_mode = "close_now"
        base_cfg.shutdown_block_new_trades = True
        base_cfg.shutdown_skip_gpt = True

    bot = NineHourFormula83Manager(
        build_config(base_cfg, execute_requested=execute_requested)
    )
    explicit_flags = (
        args.scan_now
        or args.monitor_now
        or args.write_recaps_now
        or args.daily_report_now
        or args.shutdown_pass_now
        or args.event_scan_now
        or args.weekend_summary_now
        or args.sunday_reopen_now
        or args.news_watch_now
    )
    process_lock: live_prod.LiveAccountProcessLock | None = None
    if not args.print_config or explicit_flags:
        process_lock = live_prod.LiveAccountProcessLock(
            bot.cfg.data_dir / "account_process.lock",
            f"{LANE} practice-{ACCOUNT_SUFFIX}",
        )
        process_lock.acquire()
        advisor.log(f"{bot.lane_prefix()}Acquired account process lock: {process_lock.path}")

    try:
        if args.print_config:
            advisor.log(
                f"Resolved config for lane={bot.cfg.account_lane} "
                f"account_name={bot.cfg.account_display_name}"
            )
            advisor.log(f"practice_execution_enabled={bot.should_execute()}")
            bot.print_config()

        if not args.print_config or explicit_flags:
            bot.validate_config()

        did_explicit = False
        if args.write_recaps_now:
            bot.write_daily_recaps_around_now()
            did_explicit = True
        if args.daily_report_now:
            advisor.maybe_auto_daily_move_report(
                account_lane=bot.cfg.account_lane,
                writer=False,
                force=True,
            )
            did_explicit = True
        if args.monitor_now:
            advisor.ensure_bot_ready(bot)
            bot.run_local_monitor()
            did_explicit = True
        if args.news_watch_now:
            advisor.ensure_bot_ready(bot)
            bot.run_news_watch_scan(reason="manual_9h_formula83", force=True)
            did_explicit = True
        if args.scan_now:
            advisor.ensure_bot_ready(bot)
            bot.run_gpt_scan(reason="manual_9h_formula83")
            did_explicit = True
        if args.event_scan_now:
            advisor.ensure_bot_ready(bot)
            bot.run_event_scan(reason="manual_9h_formula83")
            did_explicit = True
        if args.weekend_summary_now:
            advisor.ensure_bot_ready(bot)
            bot.run_weekend_summary_scan(reason="manual_weekend_summary")
            did_explicit = True
        if args.sunday_reopen_now:
            advisor.ensure_bot_ready(bot)
            account = bot.oanda.get_account_summary()
            open_trades = bot.oanda.get_open_trades()
            bot.update_trade_profit_memory(account, open_trades, reason="manual_sunday_reopen")
            bot.run_sunday_reopen_hard_protection(account=account, open_trades=open_trades)
            bot.run_gpt_scan(reason="sunday_reopen_manual")
            did_explicit = True
        if args.shutdown_pass_now:
            advisor.ensure_bot_ready(bot)
            if not bot.shutdown_mode_active():
                bot.cfg.shutdown_mode = "fade"
            bot.run_shutdown_fade_pass(reason="manual_shutdown_pass", force=True)
            did_explicit = True

        if args.once and (did_explicit or args.print_config):
            return 0
        bot.loop()
        return 0
    finally:
        if process_lock is not None:
            process_lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
