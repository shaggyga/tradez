#!/usr/bin/env python3
"""Generate a reproducible final analysis for the read-only -007 monitor."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PRIMARY_MAJORS = (
    "EUR_USD",
    "GBP_USD",
    "USD_JPY",
    "USD_CHF",
    "AUD_USD",
    "NZD_USD",
    "USD_CAD",
    "USD_SGD",
)
EVENT_INSTRUMENTS = (
    "EUR_USD",
    "GBP_USD",
    "USD_JPY",
    "USD_CHF",
    "AUD_USD",
    "NZD_USD",
    "USD_CAD",
)
EVENT_TIMEFRAMES = ("M1", "M5", "M30", "H1", "H4")


def finite(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def mean(values: Iterable[float | None]) -> float | None:
    clean = [value for value in values if value is not None and math.isfinite(value)]
    return statistics.fmean(clean) if clean else None


def median(values: Iterable[float | None]) -> float | None:
    clean = [value for value in values if value is not None and math.isfinite(value)]
    return statistics.median(clean) if clean else None


def pearson(left: list[float], right: list[float]) -> float | None:
    if len(left) < 3 or len(left) != len(right):
        return None
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    numerator = sum(
        (a - left_mean) * (b - right_mean) for a, b in zip(left, right)
    )
    left_scale = math.sqrt(sum((a - left_mean) ** 2 for a in left))
    right_scale = math.sqrt(sum((b - right_mean) ** 2 for b in right))
    if left_scale == 0.0 or right_scale == 0.0:
        return None
    return numerator / (left_scale * right_scale)


def forecast_metrics(rows: list[sqlite3.Row]) -> dict[str, Any]:
    if not rows:
        return {"observations": 0}
    projected_pairs = [
        (finite(row["projected_net_pips"]), finite(row["executable_net_pips"]))
        for row in rows
    ]
    projected_pairs = [
        (predicted, actual)
        for predicted, actual in projected_pairs
        if predicted is not None and actual is not None
    ]
    gross_pairs: list[tuple[float, float]] = []
    direct_gross_count = 0
    implied_gross_count = 0
    for row in rows:
        actual_move = finite(row["signed_mid_move_pips"])
        if actual_move is None:
            continue
        projected_gross = finite(row["projected_gross_movement_pips"])
        if projected_gross is not None:
            direct_gross_count += 1
        else:
            gross_to_spread = finite(row["gross_to_spread"])
            entry_spread = finite(row["entry_spread_pips"])
            if gross_to_spread is not None and entry_spread is not None:
                projected_gross = gross_to_spread * entry_spread
                implied_gross_count += 1
        if projected_gross is not None:
            gross_pairs.append((projected_gross, abs(actual_move)))
    brier: list[float] = []
    for row in rows:
        probability_up = finite(row["filtered_probability_up"])
        if probability_up is None:
            probability_up = finite(row["raw_probability_up"])
        signed_move = finite(row["signed_mid_move_pips"])
        if probability_up is None or signed_move is None:
            continue
        actual_up = signed_move > 0 if row["direction"] == "buy" else signed_move < 0
        brier.append((probability_up - float(actual_up)) ** 2)
    false_positives = sum(
        1
        for row in rows
        if (finite(row["projected_net_pips"]) or 0.0) > 0.0
        and (finite(row["executable_net_pips"]) or 0.0) <= 0.0
    )
    return {
        "observations": len(rows),
        "unique_origins": len({row["captured_epoch"] for row in rows}),
        "instruments": len({row["instrument"] for row in rows}),
        "direction_accuracy": mean(
            [float(row["direction_correct"]) for row in rows]
        ),
        "net_win_rate": mean([float(row["net_positive"]) for row in rows]),
        "mean_executable_net_pips": mean(
            [finite(row["executable_net_pips"]) for row in rows]
        ),
        "median_executable_net_pips": median(
            [finite(row["executable_net_pips"]) for row in rows]
        ),
        "sum_executable_net_pips": sum(
            finite(row["executable_net_pips"]) or 0.0 for row in rows
        ),
        "mean_signed_mid_move_pips": mean(
            [finite(row["signed_mid_move_pips"]) for row in rows]
        ),
        "mean_absolute_mid_move_pips": mean(
            [abs(finite(row["signed_mid_move_pips"]) or 0.0) for row in rows]
        ),
        "mean_entry_spread_pips": mean(
            [finite(row["entry_spread_pips"]) for row in rows]
        ),
        "mean_mfe_pips": mean([finite(row["mfe_pips"]) for row in rows]),
        "mean_mae_pips": mean([finite(row["mae_pips"]) for row in rows]),
        "direction_brier": mean(brier),
        "projected_net_actual_correlation": pearson(
            [item[0] for item in projected_pairs],
            [item[1] for item in projected_pairs],
        ),
        "projected_net_mae_pips": mean(
            [abs(predicted - actual) for predicted, actual in projected_pairs]
        ),
        "projected_net_bias_pips": mean(
            [predicted - actual for predicted, actual in projected_pairs]
        ),
        "projected_gross_actual_abs_correlation": pearson(
            [item[0] for item in gross_pairs],
            [item[1] for item in gross_pairs],
        ),
        "projected_gross_direct_observations": direct_gross_count,
        "projected_gross_implied_observations": implied_gross_count,
        "positive_projection_false_positives": false_positives,
        "positive_projection_false_positive_rate": (
            false_positives / len(rows) if rows else None
        ),
        "mean_exit_delay_sec": mean(
            [finite(row["exit_delay_sec"]) for row in rows]
        ),
    }


def grouped_metrics(
    rows: list[sqlite3.Row], field: str, minimum_rows: int = 1
) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        groups[str(row[field])].append(row)
    return {
        key: forecast_metrics(group)
        for key, group in sorted(groups.items())
        if len(group) >= minimum_rows
    }


def grouped_metrics_multi(
    rows: list[sqlite3.Row],
    fields: tuple[str, ...],
    minimum_rows: int = 1,
) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        key = "::".join(str(row[field]) for field in fields)
        groups[key].append(row)
    return {
        key: forecast_metrics(group)
        for key, group in sorted(groups.items())
        if len(group) >= minimum_rows
    }


def greedy_nonoverlap(rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
    """Keep one observation per pair/horizon after the prior label has elapsed."""
    groups: dict[tuple[str, int], list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        groups[(row["instrument"], int(row["horizon_sec"]))].append(row)
    selected: list[sqlite3.Row] = []
    for group in groups.values():
        next_allowed = -math.inf
        for row in sorted(group, key=lambda item: item["captured_epoch"]):
            if row["captured_epoch"] < next_allowed:
                continue
            selected.append(row)
            next_allowed = row["due_epoch"]
    return sorted(selected, key=lambda item: item["captured_epoch"])


def nearest_quote(
    connection: sqlite3.Connection,
    instrument: str,
    target_epoch: float,
    maximum_delay_sec: float = 45.0,
) -> sqlite3.Row | None:
    row = connection.execute(
        """
        SELECT * FROM quote_samples
        WHERE instrument = ?
          AND captured_epoch BETWEEN ? AND ?
        ORDER BY ABS(captured_epoch - ?) ASC LIMIT 1
        """,
        (
            instrument,
            target_epoch - maximum_delay_sec,
            target_epoch + maximum_delay_sec,
            target_epoch,
        ),
    ).fetchone()
    return row


def event_analysis(
    connection: sqlite3.Connection, events: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for event in events:
        release_epoch = float(event["release_epoch"])
        instruments = tuple(event.get("instruments", EVENT_INSTRUMENTS))
        instrument_placeholders = ",".join("?" for _ in instruments)
        timeframe_placeholders = ",".join("?" for _ in EVENT_TIMEFRAMES)
        technical_candidates = connection.execute(
            f"""
            SELECT * FROM technical_samples
            WHERE captured_epoch BETWEEN ? AND ?
              AND instrument IN ({instrument_placeholders})
              AND timeframe IN ({timeframe_placeholders})
            ORDER BY captured_epoch DESC
            """,
            (
                release_epoch - 1800.0,
                release_epoch,
                *instruments,
                *EVENT_TIMEFRAMES,
            ),
        ).fetchall()
        latest_technical = {}
        for row in technical_candidates:
            latest_technical.setdefault(
                (row["instrument"], row["timeframe"]), row
            )
        signal_epoch_row = connection.execute(
            """
            SELECT MAX(captured_epoch)
            FROM signal_rows
            WHERE captured_epoch <= ?
            """,
            (release_epoch,),
        ).fetchone()
        signal_epoch = signal_epoch_row[0] if signal_epoch_row else None
        release_result: dict[str, Any] = {
            **event,
            "frozen_signal_epoch": signal_epoch,
            "frozen_signal_utc": (
                datetime.fromtimestamp(signal_epoch, tz=timezone.utc).isoformat()
                if signal_epoch is not None
                else None
            ),
            "instruments": {},
        }
        for instrument in instruments:
            signal = connection.execute(
                """
                SELECT * FROM signal_rows
                WHERE captured_epoch <= ? AND instrument = ?
                ORDER BY captured_epoch DESC, signal_rank ASC LIMIT 1
                """,
                (release_epoch, instrument),
            ).fetchone()
            entry_override = (event.get("entry_quotes") or {}).get(instrument)
            if entry_override:
                entry = {
                    "quote_utc": entry_override.get("time"),
                    "quote_epoch": release_epoch,
                    "bid": finite(entry_override.get("bid_open")),
                    "ask": finite(entry_override.get("ask_open")),
                    "pip": finite(entry_override.get("pip")),
                }
                entry_quote_source = str(
                    entry_override.get("source") or "event_entry_override"
                )
            else:
                entry = nearest_quote(connection, instrument, release_epoch)
                entry_quote_source = (
                    "independent_quote_ledger" if entry is not None else None
                )
            technical_pre: dict[str, Any] = {}
            for timeframe in EVENT_TIMEFRAMES:
                technical = latest_technical.get((instrument, timeframe))
                if technical is None:
                    continue
                technical_pre[timeframe] = {
                    "candle_utc": technical["candle_utc"],
                    "r1_pips": finite(technical["r1_pips"]),
                    "r3_pips": finite(technical["r3_pips"]),
                    "r5_pips": finite(technical["r5_pips"]),
                    "pos20": finite(technical["pos20"]),
                    "m1_atr14_pips": finite(technical["m1_atr14_pips"]),
                    "m5_atr14_pips": finite(technical["m5_atr14_pips"]),
                    "pair_norm_r3": finite(technical["pair_norm_r3"]),
                    "pair_rank_r3": finite(technical["pair_rank_r3"]),
                    "relative_residual_r3": finite(
                        technical["relative_residual_r3"]
                    ),
                    "live_spread_pips": finite(technical["live_spread_pips"]),
                }
            item: dict[str, Any] = {
                "signal_captured_utc": signal["signal_snapshot_utc"]
                if signal
                else None,
                "direction": signal["direction"] if signal else None,
                "confidence": finite(signal["signal_confidence"]) if signal else None,
                "eligible": bool(signal["signal_eligible"]) if signal else False,
                "blockers": json.loads(signal["blockers_json"] or "[]")
                if signal
                else [],
                "entry_quote_utc": entry["quote_utc"] if entry else None,
                "entry_quote_source": entry_quote_source,
                "technical_pre_release": technical_pre,
                "horizons": {},
            }
            for minutes in event.get("windows_minutes", (5, 15, 30)):
                exit_override = (
                    (event.get("exit_quotes") or {})
                    .get(str(minutes), {})
                    .get(instrument)
                )
                if exit_override:
                    exit_quote = {
                        "quote_utc": exit_override.get("time"),
                        "bid": finite(exit_override.get("bid_open")),
                        "ask": finite(exit_override.get("ask_open")),
                    }
                    exit_quote_source = str(
                        exit_override.get("source")
                        or "read_only_oanda_s5_bid_ask_candle"
                    )
                else:
                    exit_quote = nearest_quote(
                        connection, instrument, release_epoch + 60.0 * minutes
                    )
                    exit_quote_source = (
                        "independent_quote_ledger"
                        if exit_quote is not None
                        else None
                    )
                if entry is None or exit_quote is None:
                    item["horizons"][str(minutes)] = {"available": False}
                    continue
                pip = finite(entry["pip"]) or 0.0001
                direction = item["direction"]
                entry_mid = (entry["bid"] + entry["ask"]) / 2.0
                exit_mid = (exit_quote["bid"] + exit_quote["ask"]) / 2.0
                raw_move = (exit_mid - entry_mid) / pip
                if direction == "buy":
                    executable = (exit_quote["bid"] - entry["ask"]) / pip
                    correct = raw_move > 0.0
                elif direction == "sell":
                    executable = (entry["bid"] - exit_quote["ask"]) / pip
                    correct = raw_move < 0.0
                else:
                    executable = None
                    correct = None
                item["horizons"][str(minutes)] = {
                    "available": True,
                    "exit_quote_utc": exit_quote["quote_utc"],
                    "exit_quote_source": exit_quote_source,
                    "raw_up_move_pips": raw_move,
                    "forecast_executable_net_pips": executable,
                    "direction_correct": correct,
                }
            release_result["instruments"][instrument] = item
        output.append(release_result)
    return output


def largest_observed_moves(
    connection: sqlite3.Connection,
    start_epoch: float,
    end_epoch: float | None,
    *,
    horizons_sec: tuple[int, ...] = (300, 900, 1800, 3600),
    limit: int = 20,
    maximum_quote_delay_sec: float = 90.0,
    maximum_forecast_age_sec: float = 900.0,
) -> dict[str, Any]:
    """Rank unique hindsight moves and attach the frozen preceding forecast."""

    if end_epoch is None:
        row = connection.execute(
            "SELECT MAX(quote_epoch) FROM quote_samples"
        ).fetchone()
        end_epoch = finite(row[0]) if row else None
    if end_epoch is None or end_epoch <= start_epoch:
        return {"observations": 0, "moves": []}

    maximum_horizon = max(horizons_sec)
    quote_rows = connection.execute(
        """
        SELECT quote_epoch, instrument, bid, ask, pip
        FROM quote_samples
        WHERE quote_epoch BETWEEN ? AND ?
        ORDER BY instrument, quote_epoch
        """,
        (start_epoch, end_epoch + maximum_horizon + maximum_quote_delay_sec),
    ).fetchall()
    quotes: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in quote_rows:
        quotes[str(row["instrument"])].append(row)

    forecast_rows = connection.execute(
        """
        SELECT captured_epoch, instrument, horizon_sec, direction,
               signal_confidence, projected_gross_movement_pips,
               projected_net_pips, signal_eligible, blockers_json,
               best_family, best_model_id, qualified_signal_count
        FROM forecasts
        WHERE stable_generation = 1
          AND captured_epoch BETWEEN ? AND ?
          AND horizon_sec IN (300, 900, 1800, 3600)
        ORDER BY instrument, horizon_sec, captured_epoch
        """,
        (start_epoch - maximum_forecast_age_sec, end_epoch),
    ).fetchall()
    forecasts: dict[tuple[str, int], list[sqlite3.Row]] = defaultdict(list)
    for row in forecast_rows:
        forecasts[(str(row["instrument"]), int(row["horizon_sec"]))].append(
            row
        )
    forecast_times = {
        key: [float(row["captured_epoch"]) for row in rows]
        for key, rows in forecasts.items()
    }

    candidates: list[dict[str, Any]] = []
    for instrument, instrument_quotes in quotes.items():
        quote_times = [float(row["quote_epoch"]) for row in instrument_quotes]
        for start_index, entry in enumerate(instrument_quotes):
            move_start = float(entry["quote_epoch"])
            if move_start < start_epoch:
                continue
            entry_mid = (float(entry["bid"]) + float(entry["ask"])) / 2.0
            pip = float(entry["pip"])
            if pip <= 0.0:
                continue
            for horizon in horizons_sec:
                target = move_start + horizon
                if target > end_epoch:
                    continue
                exit_index = bisect_left(quote_times, target, lo=start_index + 1)
                if exit_index >= len(instrument_quotes):
                    continue
                exit_quote = instrument_quotes[exit_index]
                exit_delay = float(exit_quote["quote_epoch"]) - target
                if exit_delay > maximum_quote_delay_sec:
                    continue
                exit_mid = (
                    float(exit_quote["bid"]) + float(exit_quote["ask"])
                ) / 2.0
                signed_mid = (exit_mid - entry_mid) / pip
                if signed_mid == 0.0:
                    continue
                actual_direction = "buy" if signed_mid > 0.0 else "sell"
                actual_executable = (
                    (float(exit_quote["bid"]) - float(entry["ask"])) / pip
                    if actual_direction == "buy"
                    else (float(entry["bid"]) - float(exit_quote["ask"])) / pip
                )
                if actual_executable <= 0.0:
                    continue

                forecast = None
                forecast_key = (instrument, horizon)
                rows = forecasts.get(forecast_key, [])
                if rows:
                    forecast_index = (
                        bisect_right(
                            forecast_times[forecast_key],
                            move_start,
                        )
                        - 1
                    )
                    if forecast_index >= 0:
                        candidate = rows[forecast_index]
                        if (
                            move_start - float(candidate["captured_epoch"])
                            <= maximum_forecast_age_sec
                        ):
                            forecast = candidate

                forecast_direction = (
                    str(forecast["direction"]) if forecast else None
                )
                forecast_executable = None
                if forecast_direction == "buy":
                    forecast_executable = (
                        float(exit_quote["bid"]) - float(entry["ask"])
                    ) / pip
                elif forecast_direction == "sell":
                    forecast_executable = (
                        float(entry["bid"]) - float(exit_quote["ask"])
                    ) / pip
                candidates.append(
                    {
                        "instrument": instrument,
                        "start_epoch": move_start,
                        "start_utc": datetime.fromtimestamp(
                            move_start, tz=timezone.utc
                        ).isoformat(),
                        "end_epoch": float(exit_quote["quote_epoch"]),
                        "end_utc": datetime.fromtimestamp(
                            float(exit_quote["quote_epoch"]), tz=timezone.utc
                        ).isoformat(),
                        "horizon_sec": horizon,
                        "exit_delay_sec": exit_delay,
                        "actual_direction": actual_direction,
                        "absolute_mid_move_pips": abs(signed_mid),
                        "actual_post_spread_pips": actual_executable,
                        "forecast_available": forecast is not None,
                        "forecast_age_sec": (
                            move_start - float(forecast["captured_epoch"])
                            if forecast
                            else None
                        ),
                        "forecast_direction": forecast_direction,
                        "forecast_direction_correct": (
                            forecast_direction == actual_direction
                            if forecast_direction
                            else None
                        ),
                        "forecast_post_spread_pips": forecast_executable,
                        "forecast_net_positive": (
                            forecast_executable > 0.0
                            if forecast_executable is not None
                            else None
                        ),
                        "signal_confidence": (
                            finite(forecast["signal_confidence"])
                            if forecast
                            else None
                        ),
                        "projected_gross_movement_pips": (
                            finite(
                                forecast[
                                    "projected_gross_movement_pips"
                                ]
                            )
                            if forecast
                            else None
                        ),
                        "projected_net_pips": (
                            finite(forecast["projected_net_pips"])
                            if forecast
                            else None
                        ),
                        "signal_eligible": (
                            bool(forecast["signal_eligible"])
                            if forecast
                            else False
                        ),
                        "qualified_signal_count": (
                            int(forecast["qualified_signal_count"])
                            if forecast
                            else 0
                        ),
                        "best_family": (
                            str(forecast["best_family"] or "")
                            if forecast
                            else ""
                        ),
                        "best_model_id": (
                            str(forecast["best_model_id"] or "")
                            if forecast
                            else ""
                        ),
                        "blockers": (
                            json.loads(forecast["blockers_json"] or "[]")
                            if forecast
                            else []
                        ),
                    }
                )

    selected: list[dict[str, Any]] = []
    for candidate in sorted(
        candidates,
        key=lambda row: (
            float(row["actual_post_spread_pips"]),
            float(row["absolute_mid_move_pips"]),
        ),
        reverse=True,
    ):
        if any(
            row["instrument"] == candidate["instrument"]
            and candidate["start_epoch"] < row["end_epoch"]
            and row["start_epoch"] < candidate["end_epoch"]
            for row in selected
        ):
            continue
        selected.append(candidate)
        if len(selected) >= max(1, int(limit)):
            break

    available = [row for row in selected if row["forecast_available"]]
    direction_caught = [
        row for row in available if row["forecast_direction_correct"]
    ]
    post_spread_caught = [
        row for row in available if row["forecast_net_positive"]
    ]
    return {
        "observations": len(selected),
        "forecast_available": len(available),
        "direction_caught": len(direction_caught),
        "direction_catch_rate": (
            len(direction_caught) / len(available) if available else None
        ),
        "post_spread_caught": len(post_spread_caught),
        "post_spread_catch_rate": (
            len(post_spread_caught) / len(available) if available else None
        ),
        "eligible_forecasts": sum(
            bool(row["signal_eligible"]) for row in available
        ),
        "qualified_forecast_snapshots": sum(
            int(row["qualified_signal_count"]) > 0 for row in available
        ),
        "moves": selected,
    }


def account_summary(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """
        SELECT * FROM monitor_samples
        WHERE account_balance IS NOT NULL
        ORDER BY captured_epoch
        """
    ).fetchall()
    if not rows:
        return {}
    first, last = rows[0], rows[-1]
    return {
        "first_snapshot_utc": first["captured_utc"],
        "last_snapshot_utc": last["captured_utc"],
        "starting_balance": finite(first["account_balance"]),
        "ending_balance": finite(last["account_balance"]),
        "starting_nav": finite(first["account_nav"]),
        "ending_nav": finite(last["account_nav"]),
        "starting_realized_pl": finite(first["account_pl"]),
        "ending_realized_pl": finite(last["account_pl"]),
        "maximum_open_trades": max(int(row["open_trade_count"] or 0) for row in rows),
        "maximum_pending_orders": max(
            int(row["pending_order_count"] or 0) for row in rows
        ),
        "snapshots": len(rows),
    }


def execution_log_summary(
    path: Path | None,
    start_epoch: float,
    end_epoch: float | None,
) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"available": False}
    event_counts: Counter[str] = Counter()
    skip_reasons: Counter[str] = Counter()
    compact_events: list[dict[str, Any]] = []
    qualifying_cycles = 0
    maximum_qualified = 0
    relevant_events = {
        "execution_selection_summary",
        "execution_skipped",
        "execution_selected",
        "practice_order_filled",
        "practice_order_not_filled",
        "practice_order_error",
        "practice_execution_disabled",
    }
    relevant_event_markers = tuple(
        f'"{event_name}"'.encode("ascii") for event_name in relevant_events
    )

    def reverse_lines() -> Iterable[bytes]:
        block_size = 1024 * 1024
        with path.open("rb") as handle:
            handle.seek(0, 2)
            position = handle.tell()
            remainder = b""
            while position > 0:
                read_size = min(block_size, position)
                position -= read_size
                handle.seek(position)
                parts = (handle.read(read_size) + remainder).split(b"\n")
                remainder = parts[0]
                for raw_line in reversed(parts[1:]):
                    if raw_line:
                        yield raw_line
            if remainder:
                yield remainder

    for line in reverse_lines():
        marker_position = line.find(b'"event"')
        if marker_position < 0:
            continue
        event_fragment = line[marker_position : marker_position + 128]
        if not any(marker in event_fragment for marker in relevant_event_markers):
            continue
        try:
            row = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        event = str(row.get("event") or "")
        if event not in relevant_events:
            continue
        event_epoch = epoch_from_iso(row.get("time"))
        if event_epoch is None:
            continue
        if event_epoch < start_epoch:
            break
        if end_epoch is not None and event_epoch > end_epoch:
            continue
        event_counts[event] += 1
        if event == "execution_selection_summary":
            qualified = int(row.get("qualified_candidates", 0) or 0)
            maximum_qualified = max(maximum_qualified, qualified)
            qualifying_cycles += qualified > 0
            continue
        if event == "execution_skipped":
            skip_reasons[str(row.get("reason") or "unknown")] += 1
        compact_events.append(
            {
                key: row.get(key)
                for key in (
                    "time",
                    "event",
                    "reason",
                    "selected_id",
                    "instrument",
                    "direction",
                    "execution_horizon_sec",
                    "projected_net_pips",
                    "signal_confidence",
                    "trade_id",
                    "units",
                    "price",
                    "status",
                    "execution_disabled",
                    "outcome_uncertain",
                    "second_curve",
                )
                if key in row
            }
        )
    compact_events.reverse()
    return {
        "available": True,
        "path": str(path),
        "event_counts": dict(event_counts),
        "skip_reasons": dict(skip_reasons),
        "selection_cycles_with_qualified_candidates": qualifying_cycles,
        "maximum_qualified_candidates": maximum_qualified,
        "events": compact_events,
    }


def gate_summary(rows: list[sqlite3.Row]) -> dict[str, Any]:
    blockers: Counter[str] = Counter()
    for row in rows:
        blockers.update(json.loads(row["blockers_json"] or "[]"))
    positive = [
        row for row in rows if (finite(row["projected_net_pips"]) or 0.0) > 0.0
    ]
    return {
        "signal_rows": len(rows),
        "positive_projected_net_rows": len(positive),
        "eligible_rows": sum(bool(row["signal_eligible"]) for row in rows),
        "selected_rows": sum(bool(row["selected"]) for row in rows),
        "direction_conflict_rows": sum(bool(row["direction_conflict"]) for row in rows),
        "blocker_counts": dict(blockers.most_common()),
    }


def coverage_summary(
    connection: sqlite3.Connection,
    exact_start_epoch: float,
    exact_end_epoch: float | None,
) -> dict[str, Any]:
    rows = connection.execute(
        """
        SELECT * FROM coverage_samples
        WHERE captured_epoch >= ?
        ORDER BY captured_epoch
        """,
        (exact_start_epoch,),
    ).fetchall()
    source_totals: Counter[str] = Counter()
    for row in rows:
        active_sources = json.loads(row["active_sources_json"] or "[]")
        if isinstance(active_sources, dict):
            source_totals.update(active_sources)
            continue
        for source in active_sources:
            if isinstance(source, dict):
                source_totals[str(source.get("source", "unknown"))] += int(
                    source.get("candidates", 0) or 0
                )
    intervals = [
        float(current["captured_epoch"]) - float(previous["captured_epoch"])
        for previous, current in zip(rows, rows[1:])
        if float(current["captured_epoch"]) > float(previous["captured_epoch"])
    ]
    nominal_interval = median(intervals)
    coverage_end = (
        exact_end_epoch
        if exact_end_epoch is not None
        else (float(rows[-1]["captured_epoch"]) if rows else exact_start_epoch)
    )
    expected_samples = (
        int(math.floor((coverage_end - exact_start_epoch) / nominal_interval)) + 1
        if nominal_interval is not None and nominal_interval > 0.0
        else None
    )
    return {
        "samples": len(rows),
        "first_sample_utc": datetime.fromtimestamp(
            float(rows[0]["captured_epoch"]), tz=timezone.utc
        ).isoformat()
        if rows
        else None,
        "last_sample_utc": datetime.fromtimestamp(
            float(rows[-1]["captured_epoch"]), tz=timezone.utc
        ).isoformat()
        if rows
        else None,
        "median_interval_sec": nominal_interval,
        "expected_samples_at_median_interval": expected_samples,
        "sample_coverage_ratio": (
            len(rows) / expected_samples if expected_samples else None
        ),
        "mean_feed_candidates": mean(
            [finite(row["feed_candidate_count"]) for row in rows]
        ),
        "mean_fresh_contributors": mean(
            [finite(row["fresh_contributors"]) for row in rows]
        ),
        "mean_contributor_coverage_ratio": mean(
            [finite(row["contributor_coverage_ratio"]) for row in rows]
        ),
        "mean_source_candidates": {
            key: value / len(rows) for key, value in source_totals.items()
        }
        if rows
        else {},
    }


def technical_freshness_summary(
    connection: sqlite3.Connection,
    exact_start_epoch: float,
    exact_end_epoch: float | None,
) -> dict[str, Any]:
    timeframe_seconds = {
        "M1": 60,
        "M5": 300,
        "M15": 900,
        "M30": 1800,
        "H1": 3600,
        "H4": 14400,
    }
    parameters: list[Any] = [exact_start_epoch]
    end_clause = ""
    if exact_end_epoch is not None:
        end_clause = " AND captured_epoch <= ?"
        parameters.append(exact_end_epoch)
    rows = connection.execute(
        f"""
        SELECT captured_epoch, candle_utc, timeframe
        FROM technical_samples
        WHERE captured_epoch >= ?
          {end_clause}
        ORDER BY captured_epoch
        """,
        parameters,
    ).fetchall()
    grouped: dict[str, list[int]] = defaultdict(list)
    invalid_rows = 0
    for row in rows:
        timeframe = str(row["timeframe"])
        seconds = timeframe_seconds.get(timeframe)
        candle_epoch = epoch_from_iso(row["candle_utc"])
        captured_epoch = finite(row["captured_epoch"])
        if seconds is None or candle_epoch is None or captured_epoch is None:
            invalid_rows += 1
            continue
        expected_latest_start = (
            math.floor(captured_epoch / seconds) - 1
        ) * seconds
        bars_behind = max(
            0,
            int(
                math.floor(
                    (expected_latest_start - candle_epoch) / seconds + 1e-9
                )
            ),
        )
        grouped[timeframe].append(bars_behind)

    by_timeframe: dict[str, dict[str, Any]] = {}
    for timeframe, values in sorted(
        grouped.items(), key=lambda item: timeframe_seconds[item[0]]
    ):
        ordered = sorted(values)
        p95_index = max(
            0,
            min(len(ordered) - 1, math.ceil(0.95 * len(ordered)) - 1),
        )
        by_timeframe[timeframe] = {
            "observations": len(ordered),
            "median_completed_bars_behind": statistics.median(ordered),
            "p95_completed_bars_behind": ordered[p95_index],
            "maximum_completed_bars_behind": ordered[-1],
            "rows_at_least_one_bar_behind": sum(
                value >= 1 for value in ordered
            ),
            "rows_at_least_two_bars_behind": sum(
                value >= 2 for value in ordered
            ),
        }
    return {
        "by_timeframe": by_timeframe,
        "invalid_rows": invalid_rows,
        "definition": (
            "Expected latest completed bar start minus observed candle start, "
            "divided by timeframe duration."
        ),
    }


def surface_summary(rows: list[sqlite3.Row]) -> dict[str, Any]:
    directions = Counter(row["direction"] for row in rows)
    families = Counter(row["best_family"] or "unknown" for row in rows)
    total = len(rows)
    return {
        "observations": total,
        "direction_counts": dict(directions),
        "direction_dominance": max(directions.values()) / total if total else None,
        "best_family_counts": dict(families.most_common()),
        "best_family_dominance": max(families.values()) / total if total else None,
        "zero_eligible_component_rows": sum(
            int(row["eligible_component_count"] or 0) == 0 for row in rows
        ),
        "direction_conflict_rows": sum(bool(row["direction_conflict"]) for row in rows),
    }


def format_number(value: Any, digits: int = 4) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return "n/a"
        return f"{value:.{digits}f}"
    return str(value)


def metric_line(label: str, block: dict[str, Any]) -> str:
    return (
        f"| {label} | {block.get('observations', 0)} | "
        f"{format_number(block.get('direction_accuracy'))} | "
        f"{format_number(block.get('net_win_rate'))} | "
        f"{format_number(block.get('mean_executable_net_pips'))} | "
        f"{format_number(block.get('projected_net_actual_correlation'))} |"
    )


def build_report(
    connection: sqlite3.Connection,
    exact_start_epoch: float,
    exact_end_epoch: float | None,
    events: list[dict[str, Any]],
    strategy_log: Path | None,
) -> dict[str, Any]:
    parameters: list[Any] = [exact_start_epoch]
    end_clause = ""
    if exact_end_epoch is not None:
        end_clause = " AND captured_epoch <= ?"
        parameters.append(exact_end_epoch)
    all_rows = connection.execute(
        f"""
        SELECT * FROM forecasts
        WHERE matured_epoch IS NOT NULL
          AND captured_epoch >= ?
          {end_clause}
          AND exit_delay_sec <= 45
        ORDER BY captured_epoch
        """,
        parameters,
    ).fetchall()
    major_rows = [row for row in all_rows if row["instrument"] in PRIMARY_MAJORS]
    signal_parameters: list[Any] = [exact_start_epoch]
    signal_end = ""
    if exact_end_epoch is not None:
        signal_end = " AND captured_epoch <= ?"
        signal_parameters.append(exact_end_epoch)
    signal_rows = connection.execute(
        f"""
        SELECT * FROM signal_rows
        WHERE captured_epoch >= ? {signal_end}
        ORDER BY captured_epoch
        """,
        signal_parameters,
    ).fetchall()
    preferred_keys = {
        (
            signal["signal_snapshot_utc"],
            signal["feature_snapshot_utc"],
            signal["instrument"],
            int(signal["preferred_horizon_sec"]),
        )
        for signal in signal_rows
    }
    rank_one_keys = {
        (
            signal["signal_snapshot_utc"],
            signal["feature_snapshot_utc"],
            signal["instrument"],
            int(signal["preferred_horizon_sec"]),
        )
        for signal in signal_rows
        if int(signal["signal_rank"]) == 1
    }
    preferred_rows = [
        row
        for row in all_rows
        if (
            row["signal_snapshot_utc"],
            row["feature_snapshot_utc"],
            row["instrument"],
            int(row["horizon_sec"]),
        )
        in preferred_keys
    ]
    rank_one_preferred = [
        row
        for row in preferred_rows
        if (
            row["signal_snapshot_utc"],
            row["feature_snapshot_utc"],
            row["instrument"],
            int(row["horizon_sec"]),
        )
        in rank_one_keys
    ]
    nonoverlap = greedy_nonoverlap(
        [
            row
            for row in major_rows
            if int(row["horizon_sec"]) in (1800, 3600)
        ]
    )
    pair_horizon_metrics = grouped_metrics_multi(
        major_rows, ("instrument", "horizon_sec"), minimum_rows=20
    )
    within_cell_magnitude_correlations = [
        finite(block.get("projected_gross_actual_abs_correlation"))
        for block in pair_horizon_metrics.values()
    ]
    within_cell_magnitude_correlations = [
        value for value in within_cell_magnitude_correlations if value is not None
    ]
    latest_sample = connection.execute(
        "SELECT * FROM monitor_samples ORDER BY captured_epoch DESC LIMIT 1"
    ).fetchone()
    return {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "method": {
            "exact_start_utc": datetime.fromtimestamp(
                exact_start_epoch, tz=timezone.utc
            ).isoformat(),
            "exact_end_utc": datetime.fromtimestamp(
                exact_end_epoch, tz=timezone.utc
            ).isoformat()
            if exact_end_epoch is not None
            else None,
            "maximum_exit_quote_delay_sec": 45,
            "outcome": "buy ask-to-bid; sell bid-to-ask",
            "overlap_policy": "greedy per pair and horizon for the purged cohort",
            "read_only": True,
        },
        "account": account_summary(connection),
        "latest_monitor_sample": dict(latest_sample) if latest_sample else {},
        "coverage": coverage_summary(
            connection, exact_start_epoch, exact_end_epoch
        ),
        "technical_freshness": technical_freshness_summary(
            connection, exact_start_epoch, exact_end_epoch
        ),
        "execution_log": execution_log_summary(
            strategy_log, exact_start_epoch, exact_end_epoch
        ),
        "gates": gate_summary(signal_rows),
        "surface": surface_summary(preferred_rows),
        "metrics": {
            "all_exact": forecast_metrics(all_rows),
            "primary_majors": forecast_metrics(major_rows),
            "preferred_horizons_all_pairs": forecast_metrics(preferred_rows),
            "rank_one_preferred_all_pairs": forecast_metrics(rank_one_preferred),
            "primary_majors_nonoverlap_30m_60m": forecast_metrics(nonoverlap),
        },
        "by_horizon_primary_majors": grouped_metrics(major_rows, "horizon_sec"),
        "by_pair_primary_majors": grouped_metrics(major_rows, "instrument"),
        "by_pair_horizon_primary_majors": pair_horizon_metrics,
        "magnitude_diagnostic": {
            "pooled_implied_gross_actual_abs_correlation": forecast_metrics(
                major_rows
            ).get("projected_gross_actual_abs_correlation"),
            "median_within_pair_horizon_correlation": median(
                within_cell_magnitude_correlations
            ),
            "mean_within_pair_horizon_correlation": mean(
                within_cell_magnitude_correlations
            ),
            "pair_horizon_cells": len(pair_horizon_metrics),
            "cells_with_positive_correlation": sum(
                value > 0.0 for value in within_cell_magnitude_correlations
            ),
            "direct_magnitude_observations": forecast_metrics(major_rows).get(
                "projected_gross_direct_observations"
            ),
            "implied_magnitude_observations": forecast_metrics(major_rows).get(
                "projected_gross_implied_observations"
            ),
        },
        "nonoverlap_by_horizon_primary_majors": grouped_metrics(
            nonoverlap, "horizon_sec"
        ),
        "nonoverlap_by_pair_primary_majors": grouped_metrics(
            nonoverlap, "instrument"
        ),
        "largest_observed_moves": largest_observed_moves(
            connection,
            exact_start_epoch,
            exact_end_epoch,
        ),
        "events": event_analysis(connection, events),
    }


def render_markdown(report: dict[str, Any]) -> str:
    account = report["account"]
    gates = report["gates"]
    surface = report["surface"]
    magnitude = report["magnitude_diagnostic"]
    execution_log = report["execution_log"]
    coverage = report["coverage"]
    technical_freshness = report["technical_freshness"]
    largest_moves = report["largest_observed_moves"]
    lines = [
        "# Practice -007 Forecast Monitor Final Analysis",
        "",
        "## Protocol",
        "",
        f"- Exact quote phase: {report['method']['exact_start_utc']} to "
        f"{report['method']['exact_end_utc'] or 'latest available'}",
        "- Outcomes use executable bid/ask prices with no account simulation.",
        "- The 30/60-minute purged cohort greedily removes overlapping labels per pair and horizon.",
        "- This process was read-only and had no capability to submit an OANDA order.",
        f"- Observed / nominal exact-phase samples: {coverage.get('samples', 0)} / "
        f"{coverage.get('expected_samples_at_median_interval', 'n/a')} "
        f"({format_number(coverage.get('sample_coverage_ratio'))} coverage).",
        "",
        "## Account",
        "",
        f"- Balance: {format_number(account.get('starting_balance'))} to "
        f"{format_number(account.get('ending_balance'))}",
        f"- Realized P/L: {format_number(account.get('starting_realized_pl'))} to "
        f"{format_number(account.get('ending_realized_pl'))}",
        f"- Maximum open trades / pending orders: "
        f"{account.get('maximum_open_trades', 0)} / "
        f"{account.get('maximum_pending_orders', 0)}",
        "",
        "## Forecast Results",
        "",
        "| Cohort | N | Direction accuracy | Net win rate | Mean executable pips | Projected/actual corr |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for label, block in report["metrics"].items():
        lines.append(metric_line(label, block))
    lines.extend(
        [
            "",
            "## Primary Majors By Horizon",
            "",
            "| Horizon sec | N | Direction accuracy | Net win rate | Mean executable pips | Projected/actual corr |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for label, block in report["by_horizon_primary_majors"].items():
        lines.append(metric_line(label, block))
    lines.extend(
        [
            "",
            "## Non-Overlapping 30/60-Minute Cohort",
            "",
            "| Horizon sec | N | Direction accuracy | Net win rate | Mean executable pips | Projected/actual corr |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for label, block in report["nonoverlap_by_horizon_primary_majors"].items():
        lines.append(metric_line(label, block))
    lines.extend(
        [
            "",
            "## Technical Input Freshness",
            "",
            "| Timeframe | N | Median bars behind | P95 | Maximum | >=1 bar | >=2 bars |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for timeframe, block in technical_freshness["by_timeframe"].items():
        lines.append(
            f"| {timeframe} | {block['observations']} | "
            f"{format_number(block['median_completed_bars_behind'])} | "
            f"{block['p95_completed_bars_behind']} | "
            f"{block['maximum_completed_bars_behind']} | "
            f"{block['rows_at_least_one_bar_behind']} | "
            f"{block['rows_at_least_two_bars_behind']} |"
        )
    lines.extend(
        [
            "",
            "## Gates And Surface",
            "",
            f"- Signal rows: {gates['signal_rows']}",
            f"- Positive projected-net rows: {gates['positive_projected_net_rows']}",
            f"- Eligible / selected rows: {gates['eligible_rows']} / {gates['selected_rows']}",
            f"- Direction-conflict rows: {gates['direction_conflict_rows']}",
            f"- Direction counts: {json.dumps(surface['direction_counts'], sort_keys=True)}",
            f"- Best-family dominance: {format_number(surface['best_family_dominance'])}",
            f"- Zero eligible-component rows: {surface['zero_eligible_component_rows']}",
            f"- Blockers: {json.dumps(gates['blocker_counts'], sort_keys=True)}",
            f"- Direct / implied gross-magnitude observations: "
            f"{report['metrics']['all_exact'].get('projected_gross_direct_observations', 0)} / "
            f"{report['metrics']['all_exact'].get('projected_gross_implied_observations', 0)}",
            f"- Pooled / median within-pair-horizon implied magnitude correlation: "
            f"{format_number(magnitude.get('pooled_implied_gross_actual_abs_correlation'))} / "
            f"{format_number(magnitude.get('median_within_pair_horizon_correlation'))}",
            f"- Execution skip reasons: "
            f"{json.dumps(execution_log.get('skip_reasons', {}), sort_keys=True)}",
            f"- Orders selected / filled / errored: "
            f"{execution_log.get('event_counts', {}).get('execution_selected', 0)} / "
            f"{execution_log.get('event_counts', {}).get('practice_order_filled', 0)} / "
            f"{execution_log.get('event_counts', {}).get('practice_order_error', 0)}",
            "",
            "## Largest Observed Moves",
            "",
            f"- Unique moves / forecasts available: "
            f"{largest_moves.get('observations', 0)} / "
            f"{largest_moves.get('forecast_available', 0)}",
            f"- Direction caught / post-spread caught: "
            f"{largest_moves.get('direction_caught', 0)} / "
            f"{largest_moves.get('post_spread_caught', 0)}",
            f"- Eligible / qualified at decision time: "
            f"{largest_moves.get('eligible_forecasts', 0)} / "
            f"{largest_moves.get('qualified_forecast_snapshots', 0)}",
            "",
            "| Pair | Start UTC | Horizon | Actual side | Post-spread pips | Forecast side | Caught | Eligible |",
            "|---|---|---:|---|---:|---|---:|---:|",
        ]
    )
    for move in largest_moves.get("moves", [])[:10]:
        lines.append(
            f"| {move['instrument']} | {move['start_utc']} | "
            f"{move['horizon_sec']} | {move['actual_direction']} | "
            f"{format_number(move['actual_post_spread_pips'])} | "
            f"{move.get('forecast_direction') or 'n/a'} | "
            f"{format_number(move.get('forecast_net_positive'))} | "
            f"{format_number(move.get('signal_eligible'))} |"
        )
    lines.extend(
        [
            "",
            "## Event Windows",
            "",
        ]
    )
    if not report["events"]:
        lines.append("- No event definitions were supplied.")
    for event in report["events"]:
        lines.append(
            f"### {event['name']} ({event.get('release_utc', event['release_epoch'])})"
        )
        lines.append("")
        if event.get("actual_headline_mom_pct") is not None:
            lines.append(
                f"- Headline actual / consensus / previous: "
                f"{format_number(event.get('actual_headline_mom_pct'))}% / "
                f"{format_number(event.get('forecast_headline_mom_pct'))}% / "
                f"{format_number(event.get('previous_headline_mom_pct'))}%"
            )
        lines.extend(
            [
                "",
                "| Window | Available | Direction correct | Net positive | Mean executable pips |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for minutes in event.get("windows_minutes", (5, 15, 30)):
            horizon_rows = [
                item["horizons"].get(str(minutes), {})
                for item in event["instruments"].values()
            ]
            available = [row for row in horizon_rows if row.get("available")]
            direction_correct = sum(
                row.get("direction_correct") is True for row in available
            )
            positive = sum(
                (finite(row.get("forecast_executable_net_pips")) or 0.0) > 0.0
                for row in available
            )
            mean_net = mean(
                [
                    finite(row.get("forecast_executable_net_pips"))
                    for row in available
                ]
            )
            lines.append(
                f"| {minutes}m | {len(available)} | {direction_correct} | "
                f"{positive} | {format_number(mean_net)} |"
            )
        lines.append("")
        for instrument, item in event["instruments"].items():
            horizon = item["horizons"].get("30", {})
            lines.append(
                f"- {instrument}: {item['direction']} "
                f"({format_number(item['confidence'])}); 30m executable "
                f"{format_number(horizon.get('forecast_executable_net_pips'))} pips"
            )
        lines.append(
            "- All frozen signals retained their original gate state. Event "
            "endpoints use the synchronized read-only bid/ask source recorded "
            "in the JSON report."
        )
        lines.append("")
    lines.extend(
        [
            "## Interpretation",
            "",
            "- Direction accuracy must be read alongside executable outcomes, overlap, pair concentration, and family concentration.",
            "- A positive direction percentage with negative executable pips is not a tradable edge.",
            "- When direct gross magnitude is absent, the report labels and uses the existing gross-to-spread ratio times entry spread as an implied magnitude only.",
            "- Gate suppression is protective when positive projected-net rows subsequently fail to clear spread.",
            "- Any research promotion should use multiple days of purged chronological validation; this monitor is an intraday diagnostic, not promotion evidence.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--exact-start-utc", required=True)
    parser.add_argument("--exact-end-utc")
    parser.add_argument("--events-json", type=Path)
    parser.add_argument("--strategy-log", type=Path)
    return parser.parse_args()


def epoch_from_iso(value: str | None) -> float | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    events: list[dict[str, Any]] = []
    if args.events_json and args.events_json.exists():
        events = json.loads(args.events_json.read_text(encoding="utf-8"))
    connection = sqlite3.connect(args.database)
    connection.row_factory = sqlite3.Row
    try:
        report = build_report(
            connection,
            epoch_from_iso(args.exact_start_utc) or 0.0,
            epoch_from_iso(args.exact_end_utc),
            events,
            args.strategy_log,
        )
    finally:
        connection.close()
    json_path = args.output_dir / "PRACTICE_007_MONITOR_FINAL_REPORT.json"
    markdown_path = args.output_dir / "PRACTICE_007_MONITOR_FINAL_REPORT.md"
    json_path.write_text(
        json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
    )
    markdown_path.write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(markdown_path)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
