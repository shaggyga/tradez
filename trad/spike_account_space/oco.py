from __future__ import annotations

from dataclasses import asdict
from typing import Any, Mapping

import numpy as np
import pandas as pd

from .schemas import OCOConfig, config_fingerprint, normalize_bars, normalize_decisions


def _spread_pips(bar: pd.Series, pip_size: float, field: str = "open") -> float:
    return float((float(bar[f"ask_{field}"]) - float(bar[f"bid_{field}"])) / pip_size)


def _entry_price(bar: pd.Series, side: str, stop_price: float, pip_size: float, slippage_pips: float) -> float:
    if side == "long":
        return float(max(stop_price, float(bar["ask_open"])) + slippage_pips * pip_size)
    return float(min(stop_price, float(bar["bid_open"])) - slippage_pips * pip_size)


def _simulate_leg_exit(
    instrument_bars: pd.DataFrame,
    entry_index: int,
    side: str,
    entry_price: float,
    pip_size: float,
    config: OCOConfig,
    resolved_stop_pips: float | None,
    resolved_target_pips: float | None,
) -> dict[str, Any]:
    """Simulate one filled leg using executable opposite-side prices.

    OHLC cannot establish intrabar ordering.  A stop wins any bar containing
    both the stop and profit target.  A newly activated trailing stop becomes
    executable on the following bar, avoiding a favorable-high-first
    assumption on its activation bar.
    """

    entry_time = pd.Timestamp(instrument_bars.at[entry_index, "timestamp"])
    deadline = entry_time + pd.Timedelta(minutes=int(config.exit.max_hold_minutes))
    timestamps_ns = instrument_bars["timestamp"].to_numpy(dtype="datetime64[ns]").astype(np.int64)
    end_exclusive = int(np.searchsorted(timestamps_ns, deadline.value, side="right"))
    end_exclusive = max(entry_index + 1, min(end_exclusive, len(instrument_bars)))
    if entry_index >= len(instrument_bars):
        return {
            "path_complete": False,
            "exit_timestamp": pd.NaT,
            "realized_pips": np.nan,
            "gross_pips": np.nan,
            "mfe_pips": np.nan,
            "mae_pips": np.nan,
            "exit_reason": "missing_exit_path",
            "same_bar_exit_ambiguous": False,
        }

    fixed_stop = None
    fixed_target = None
    if resolved_stop_pips is not None:
        direction = -1.0 if side == "long" else 1.0
        fixed_stop = entry_price + direction * float(resolved_stop_pips) * pip_size
    if resolved_target_pips is not None:
        direction = 1.0 if side == "long" else -1.0
        fixed_target = entry_price + direction * float(resolved_target_pips) * pip_size

    mfe = 0.0
    mae = 0.0
    active_trail: float | None = None
    exit_price: float | None = None
    exit_index: int | None = None
    exit_reason = "time_exit"
    same_bar_ambiguous = False
    exit_slippage = float(config.exit_slippage_pips) * pip_size

    for j in range(entry_index, end_exclusive):
        bar = instrument_bars.iloc[j]
        if side == "long":
            favorable = (float(bar["bid_high"]) - entry_price) / pip_size
            adverse = (entry_price - float(bar["bid_low"])) / pip_size
            hit_stop = fixed_stop is not None and float(bar["bid_low"]) <= fixed_stop
            hit_target = fixed_target is not None and float(bar["bid_high"]) >= fixed_target
            hit_trail = active_trail is not None and float(bar["bid_low"]) <= active_trail
        else:
            favorable = (entry_price - float(bar["ask_low"])) / pip_size
            adverse = (float(bar["ask_high"]) - entry_price) / pip_size
            hit_stop = fixed_stop is not None and float(bar["ask_high"]) >= fixed_stop
            hit_target = fixed_target is not None and float(bar["ask_low"]) <= fixed_target
            hit_trail = active_trail is not None and float(bar["ask_high"]) >= active_trail

        # The trigger bar contains pre-entry extremes.  Never credit its
        # favorable high/low or target; do allow the adverse stop, which is the
        # conservative adverse-first interpretation of unknown tick ordering.
        is_entry_bar = j == entry_index
        if not is_entry_bar:
            mfe = max(mfe, float(favorable), 0.0)
        mae = max(mae, float(adverse), 0.0)
        if is_entry_bar:
            same_bar_ambiguous = bool(hit_stop and hit_target)
            hit_target = False
            hit_trail = False

        if hit_stop or hit_trail or hit_target:
            competing_profit_exit = bool(hit_target and (hit_stop or hit_trail))
            same_bar_ambiguous = bool(same_bar_ambiguous or competing_profit_exit)
            if hit_stop:
                raw_exit = float(fixed_stop)
                exit_reason = "stop_loss"
            elif hit_trail:
                raw_exit = float(active_trail)
                exit_reason = "trailing_stop"
            else:
                raw_exit = float(fixed_target)
                exit_reason = "take_profit"
            exit_price = raw_exit - exit_slippage if side == "long" else raw_exit + exit_slippage
            exit_index = int(j)
            break

        # A trail based on this bar's extreme is active only from the next bar.
        if config.exit.trailing_start_pips is not None and mfe >= float(config.exit.trailing_start_pips):
            if side == "long":
                proposed = entry_price + (mfe - float(config.exit.trailing_distance_pips)) * pip_size
                active_trail = proposed if active_trail is None else max(active_trail, proposed)
            else:
                proposed = entry_price - (mfe - float(config.exit.trailing_distance_pips)) * pip_size
                active_trail = proposed if active_trail is None else min(active_trail, proposed)

    full_path_available = pd.Timestamp(instrument_bars["timestamp"].max()) >= deadline
    if exit_index is None:
        final_index = int(end_exclusive - 1)
        final_bar = instrument_bars.loc[final_index]
        raw_exit = float(final_bar["bid_close"] if side == "long" else final_bar["ask_close"])
        exit_price = raw_exit - exit_slippage if side == "long" else raw_exit + exit_slippage
        exit_index = final_index
        exit_reason = "time_exit" if full_path_available else "truncated_time_exit"

    realized = (
        (float(exit_price) - entry_price) / pip_size
        if side == "long"
        else (entry_price - float(exit_price)) / pip_size
    )
    entry_bar = instrument_bars.loc[entry_index]
    exit_bar = instrument_bars.loc[exit_index]
    estimated_spread_cost = 0.5 * (
        _spread_pips(entry_bar, pip_size, "open") + _spread_pips(exit_bar, pip_size, "close")
    )
    estimated_slippage_cost = float(config.entry_slippage_pips) + float(config.exit_slippage_pips)
    estimated_cost = estimated_spread_cost + estimated_slippage_cost
    return {
        "path_complete": bool(exit_reason != "truncated_time_exit"),
        "exit_timestamp": pd.Timestamp(exit_bar["timestamp"]),
        "exit_price": float(exit_price),
        "realized_pips": float(realized),
        "gross_pips": float(realized + estimated_cost),
        "estimated_spread_cost_pips": float(estimated_spread_cost),
        "estimated_slippage_cost_pips": float(estimated_slippage_cost),
        "estimated_total_cost_pips": float(estimated_cost),
        "mfe_pips": float(mfe),
        "mae_pips": float(mae),
        "exit_reason": exit_reason,
        "same_bar_exit_ambiguous": bool(same_bar_ambiguous),
        "entry_bar_favorable_suppressed": True,
        "hold_minutes": float((pd.Timestamp(exit_bar["timestamp"]) - entry_time).total_seconds() / 60.0),
    }


def _blank_result(base: dict[str, Any], outcome: str, *, path_complete: bool = True) -> dict[str, Any]:
    return {
        **base,
        "movement_gate_passed": outcome not in {"movement_gate_rejected", "expected_edge_gate_rejected"},
        "triggered": False,
        "is_executable_candidate": False,
        "trigger_outcome": outcome,
        "side": None,
        "sibling_side": None,
        "filled_legs": 0,
        "double_trigger": False,
        "same_bar_entry_ambiguous": False,
        "ambiguity_resolution": None,
        "entry_timestamp": pd.NaT,
        "exit_timestamp": pd.NaT,
        "entry_price": np.nan,
        "exit_price": np.nan,
        "buy_stop": np.nan,
        "sell_stop": np.nan,
        "buffer_pips": np.nan,
        "realized_pips": 0.0,
        "gross_pips": 0.0,
        "estimated_spread_cost_pips": 0.0,
        "estimated_slippage_cost_pips": 0.0,
        "estimated_total_cost_pips": 0.0,
        "mfe_pips": 0.0,
        "mae_pips": 0.0,
        "risk_pips_per_unit": np.nan,
        "exit_reason": None,
        "same_bar_exit_ambiguous": False,
        "path_complete": bool(path_complete),
        "hold_minutes": 0.0,
    }


def _base_row(decision: Mapping[str, Any], config: OCOConfig, config_id: str) -> dict[str, Any]:
    base = dict(decision)
    base["decision_timestamp"] = base.pop("timestamp")
    base["oco_config_id"] = config_id
    base["oco_cancel_latency_minutes"] = int(config.cancel_latency_minutes)
    base["oco_same_bar_entry_policy"] = config.same_bar_entry_policy
    base["reserved_legs"] = 2 if (
        int(config.cancel_latency_minutes) > 0 or config.same_bar_entry_policy == "double_fill"
    ) else 1
    inherited_oracle = [
        str(column) for column in decision
        if any(token in str(column).lower() for token in ("actual", "forward", "future", "label", "target", "significant"))
    ]
    base["hindsight_fields"] = "|".join([
        *inherited_oracle,
        "realized_pips", "gross_pips", "mfe_pips", "mae_pips", "exit_timestamp", "exit_reason",
    ])
    base["research_only"] = True
    return base


def _combine_legs(
    base: dict[str, Any],
    primary_side: str,
    primary_entry_time: pd.Timestamp,
    primary_entry_price: float,
    primary: dict[str, Any],
    sibling_side: str | None,
    sibling_entry_time: pd.Timestamp | None,
    sibling_entry_price: float | None,
    sibling: dict[str, Any] | None,
    buy_stop: float,
    sell_stop: float,
    buffer_pips: float,
    same_bar_entry_ambiguous: bool,
    ambiguity_resolution: str | None,
    config: OCOConfig,
    resolved_stop_pips: float | None,
    resolved_target_pips: float | None,
) -> dict[str, Any]:
    legs = [(primary_side, primary_entry_time, primary_entry_price, primary)]
    if sibling is not None and sibling_side is not None and sibling_entry_time is not None and sibling_entry_price is not None:
        legs.append((sibling_side, sibling_entry_time, sibling_entry_price, sibling))
    path_complete = all(bool(leg[3]["path_complete"]) for leg in legs)
    exit_times = [pd.Timestamp(leg[3]["exit_timestamp"]) for leg in legs if pd.notna(leg[3]["exit_timestamp"])]
    total = lambda key: float(sum(float(leg[3].get(key, 0.0)) for leg in legs))
    reserved_legs = int(base["reserved_legs"])
    risk_pips = float(resolved_stop_pips) * reserved_legs if resolved_stop_pips is not None else np.nan
    exit_reasons = "+".join(str(leg[3]["exit_reason"]) for leg in legs)
    out = {
        **base,
        "movement_gate_passed": True,
        "triggered": True,
        "is_executable_candidate": bool(path_complete and np.isfinite(total("realized_pips"))),
        "trigger_outcome": "double_trigger" if len(legs) == 2 else "single_trigger",
        "side": primary_side,
        "sibling_side": sibling_side,
        "filled_legs": len(legs),
        "double_trigger": len(legs) == 2,
        "same_bar_entry_ambiguous": bool(same_bar_entry_ambiguous),
        "ambiguity_resolution": ambiguity_resolution,
        "entry_timestamp": pd.Timestamp(primary_entry_time),
        "sibling_entry_timestamp": pd.Timestamp(sibling_entry_time) if sibling_entry_time is not None else pd.NaT,
        "exit_timestamp": max(exit_times) if exit_times else pd.NaT,
        "entry_price": float(primary_entry_price),
        "sibling_entry_price": float(sibling_entry_price) if sibling_entry_price is not None else np.nan,
        "exit_price": float(primary.get("exit_price", np.nan)),
        "sibling_exit_price": float(sibling.get("exit_price", np.nan)) if sibling is not None else np.nan,
        "buy_stop": float(buy_stop),
        "sell_stop": float(sell_stop),
        "buffer_pips": float(buffer_pips),
        "realized_pips": total("realized_pips"),
        "gross_pips": total("gross_pips"),
        "estimated_spread_cost_pips": total("estimated_spread_cost_pips"),
        "estimated_slippage_cost_pips": total("estimated_slippage_cost_pips"),
        "estimated_total_cost_pips": total("estimated_total_cost_pips"),
        "mfe_pips": total("mfe_pips"),
        "mae_pips": total("mae_pips"),
        "risk_pips_per_unit": risk_pips,
        "resolved_stop_loss_pips": float(resolved_stop_pips) if resolved_stop_pips is not None else np.nan,
        "resolved_take_profit_pips": float(resolved_target_pips) if resolved_target_pips is not None else np.nan,
        "exit_reason": exit_reasons,
        "primary_exit_reason": primary["exit_reason"],
        "sibling_exit_reason": sibling["exit_reason"] if sibling is not None else None,
        "primary_exit_timestamp": pd.Timestamp(primary["exit_timestamp"]),
        "sibling_exit_timestamp": pd.Timestamp(sibling["exit_timestamp"]) if sibling is not None else pd.NaT,
        "primary_realized_pips": float(primary["realized_pips"]),
        "sibling_realized_pips": float(sibling["realized_pips"]) if sibling is not None else 0.0,
        "same_bar_exit_ambiguous": any(bool(leg[3]["same_bar_exit_ambiguous"]) for leg in legs),
        "path_complete": bool(path_complete),
        # Aggregate opportunity remains open until all filled legs have exited.
        "hold_minutes": float(
            (max(exit_times) - pd.Timestamp(primary_entry_time)).total_seconds() / 60.0
        ) if exit_times else np.nan,
    }
    return out


def evaluate_oco_candidates(
    decisions: pd.DataFrame,
    bars: pd.DataFrame,
    config: OCOConfig,
) -> pd.DataFrame:
    """Evaluate movement-gated two-pending OCO decisions against bid/ask bars.

    Decision rows are evaluated strictly after ``timestamp``.  Candidate
    ranking inputs are copied through, while all realized path fields are
    explicitly marked hindsight-derived.  The function does not size trades or
    assume an account currency.
    """

    decisions_n = normalize_decisions(decisions)
    bars_n = normalize_bars(bars)
    grouped: dict[str, tuple[pd.DataFrame, np.ndarray]] = {}
    for instrument, frame in bars_n.groupby("instrument", sort=False):
        frame = frame.reset_index(drop=True)
        grouped[str(instrument)] = (
            frame,
            frame["timestamp"].to_numpy(dtype="datetime64[ns]").astype(np.int64),
        )
    output: list[dict[str, Any]] = []
    oco_config_id = config_fingerprint(config, "oco")

    for decision in decisions_n.to_dict("records"):
        base = _base_row(decision, config, oco_config_id)
        resolved_stop_pips, resolved_target_pips = config.exit.resolve(float(decision["atr_pips"]))
        base["resolved_stop_loss_pips"] = (
            float(resolved_stop_pips) if resolved_stop_pips is not None else np.nan
        )
        base["resolved_take_profit_pips"] = (
            float(resolved_target_pips) if resolved_target_pips is not None else np.nan
        )
        instrument = str(decision["instrument"])
        decision_time = pd.Timestamp(decision["timestamp"])
        if float(decision["movement_score"]) < float(decision["movement_threshold"]):
            output.append(_blank_result(base, "movement_gate_rejected"))
            continue
        if config.minimum_expected_edge_pips is not None and (
            not np.isfinite(float(decision["expected_net_edge_pips"]))
            or float(decision["expected_net_edge_pips"]) < float(config.minimum_expected_edge_pips)
        ):
            output.append(_blank_result(base, "expected_edge_gate_rejected"))
            continue
        bundle = grouped.get(instrument)
        if bundle is None:
            output.append(_blank_result(base, "missing_instrument_bars", path_complete=False))
            continue
        instrument_bars, timestamps_ns = bundle

        history_start = decision_time - pd.Timedelta(minutes=int(config.range_lookback_minutes))
        history_left = int(np.searchsorted(timestamps_ns, history_start.value, side="left"))
        history_right = int(np.searchsorted(timestamps_ns, decision_time.value, side="right"))
        history = instrument_bars.iloc[history_left:history_right]
        if len(history) < int(config.minimum_history_bars):
            output.append(_blank_result(base, "insufficient_history", path_complete=False))
            continue
        pip_sizes = history["pip_size"].unique()
        if len(pip_sizes) != 1:
            raise ValueError(f"pip_size changes inside {instrument} history window")
        pip_size = float(pip_sizes[0])
        buffer_pips = config.buffer.pips(float(decision["spread_pips"]), float(decision["atr_pips"]))
        buy_stop = float(history["ask_high"].max() + buffer_pips * pip_size)
        sell_stop = float(history["bid_low"].min() - buffer_pips * pip_size)

        trigger_deadline = decision_time + pd.Timedelta(minutes=int(config.trigger_timeout_minutes))
        future_left = int(np.searchsorted(timestamps_ns, decision_time.value, side="right"))
        future_right = int(np.searchsorted(timestamps_ns, trigger_deadline.value, side="right"))
        if future_left >= future_right:
            complete = bool(timestamps_ns[-1] >= trigger_deadline.value)
            blank = _blank_result(base, "no_trigger" if complete else "trigger_path_truncated", path_complete=complete)
            blank.update({"buy_stop": buy_stop, "sell_stop": sell_stop, "buffer_pips": buffer_pips})
            output.append(blank)
            continue

        trigger_index: int | None = None
        buy_hit = sell_hit = False
        for j in range(future_left, future_right):
            bar = instrument_bars.iloc[j]
            buy_hit = float(bar["ask_high"]) >= buy_stop
            sell_hit = float(bar["bid_low"]) <= sell_stop
            if buy_hit or sell_hit:
                trigger_index = int(j)
                break
        if trigger_index is None:
            complete = bool(timestamps_ns[-1] >= trigger_deadline.value)
            blank = _blank_result(base, "no_trigger" if complete else "trigger_path_truncated", path_complete=complete)
            blank.update({"buy_stop": buy_stop, "sell_stop": sell_stop, "buffer_pips": buffer_pips})
            output.append(blank)
            continue

        trigger_bar = instrument_bars.loc[trigger_index]
        trigger_time = pd.Timestamp(trigger_bar["timestamp"])
        simultaneous = bool(buy_hit and sell_hit)
        if simultaneous and config.same_bar_entry_policy == "skip":
            blank = _blank_result(base, "ambiguous_entry_skipped")
            blank.update({
                "buy_stop": buy_stop,
                "sell_stop": sell_stop,
                "buffer_pips": buffer_pips,
                "same_bar_entry_ambiguous": True,
                "ambiguity_resolution": "skip",
            })
            output.append(blank)
            continue

        if simultaneous:
            long_entry = _entry_price(trigger_bar, "long", buy_stop, pip_size, float(config.entry_slippage_pips))
            short_entry = _entry_price(trigger_bar, "short", sell_stop, pip_size, float(config.entry_slippage_pips))
            long_leg = _simulate_leg_exit(
                instrument_bars, trigger_index, "long", long_entry, pip_size, config,
                resolved_stop_pips, resolved_target_pips,
            )
            short_leg = _simulate_leg_exit(
                instrument_bars, trigger_index, "short", short_entry, pip_size, config,
                resolved_stop_pips, resolved_target_pips,
            )
            double_fill = config.same_bar_entry_policy == "double_fill" or int(config.cancel_latency_minutes) > 0
            if double_fill:
                output.append(_combine_legs(
                    base, "long", trigger_time, long_entry, long_leg,
                    "short", trigger_time, short_entry, short_leg,
                    buy_stop, sell_stop, buffer_pips, True,
                    "same_bar_double_fill", config, resolved_stop_pips, resolved_target_pips,
                ))
            else:
                # The path-consistent ordering is unknowable from OHLC.  Use
                # the worse of the two possible first fills, never the winner.
                if float(long_leg["realized_pips"]) <= float(short_leg["realized_pips"]):
                    side, entry, leg = "long", long_entry, long_leg
                else:
                    side, entry, leg = "short", short_entry, short_leg
                output.append(_combine_legs(
                    base, side, trigger_time, entry, leg,
                    None, None, None, None,
                    buy_stop, sell_stop, buffer_pips, True,
                    "adverse_first_worst_single_fill", config, resolved_stop_pips, resolved_target_pips,
                ))
            continue

        primary_side = "long" if buy_hit else "short"
        primary_stop = buy_stop if primary_side == "long" else sell_stop
        primary_entry = _entry_price(
            trigger_bar, primary_side, primary_stop, pip_size, float(config.entry_slippage_pips)
        )
        primary_leg = _simulate_leg_exit(
            instrument_bars, trigger_index, primary_side, primary_entry, pip_size, config,
            resolved_stop_pips, resolved_target_pips,
        )

        sibling_side: str | None = None
        sibling_time: pd.Timestamp | None = None
        sibling_entry: float | None = None
        sibling_leg: dict[str, Any] | None = None
        if int(config.cancel_latency_minutes) > 0:
            cancel_deadline = trigger_time + pd.Timedelta(minutes=int(config.cancel_latency_minutes))
            latency_left = int(np.searchsorted(timestamps_ns, trigger_time.value, side="right"))
            latency_right = int(np.searchsorted(timestamps_ns, cancel_deadline.value, side="right"))
            for k in range(latency_left, latency_right):
                latency_bar = instrument_bars.iloc[k]
                opposite_hit = (
                    float(latency_bar["bid_low"]) <= sell_stop
                    if primary_side == "long"
                    else float(latency_bar["ask_high"]) >= buy_stop
                )
                if opposite_hit:
                    sibling_side = "short" if primary_side == "long" else "long"
                    sibling_stop = sell_stop if sibling_side == "short" else buy_stop
                    sibling_time = pd.Timestamp(latency_bar["timestamp"])
                    sibling_entry = _entry_price(
                        latency_bar, sibling_side, sibling_stop, pip_size, float(config.entry_slippage_pips)
                    )
                    sibling_leg = _simulate_leg_exit(
                        instrument_bars, int(k), sibling_side, sibling_entry, pip_size, config,
                        resolved_stop_pips, resolved_target_pips,
                    )
                    break

        output.append(_combine_legs(
            base, primary_side, trigger_time, primary_entry, primary_leg,
            sibling_side, sibling_time, sibling_entry, sibling_leg,
            buy_stop, sell_stop, buffer_pips, False,
            "cancel_latency_double_fill" if sibling_leg is not None else None,
            config, resolved_stop_pips, resolved_target_pips,
        ))

    result = pd.DataFrame(output)
    if not result.empty:
        result = result.sort_values(
            ["decision_timestamp", "instrument", "decision_id"], kind="mergesort"
        ).reset_index(drop=True)
    result.attrs["oco_config"] = asdict(config)
    result.attrs["research_only"] = True
    result.attrs["oracle_fields"] = [
        "realized_pips", "gross_pips", "mfe_pips", "mae_pips", "exit_timestamp", "exit_reason"
    ]
    return result
