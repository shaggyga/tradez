#!/usr/bin/env python3
"""Scan all tradeable OANDA FX pairs and trade the best current setup."""

from __future__ import annotations

import argparse
import copy
import json
import math
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import oanda_practice_eurusd_micro_scalper as scalper_module
    from oanda_practice_pair_rotation_scalper import (
        oanda_get,
        read_credentials,
        tradeable_currency_instruments,
    )
    from oanda_practice_eurusd_micro_scalper import (
        DEFAULT_CREDS,
        DEFAULT_LOG_DIR,
        FatalSessionError,
        OandaApiError,
        PracticeScalper,
        infer_pip_size,
        normalize_instrument,
        parse_args as parse_scalper_args,
        quote_from_price_payload,
        safe_float,
    )
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad import oanda_practice_eurusd_micro_scalper as scalper_module
    from trad.oanda_practice_pair_rotation_scalper import (
        oanda_get,
        read_credentials,
        tradeable_currency_instruments,
    )
    from trad.oanda_practice_eurusd_micro_scalper import (
        DEFAULT_CREDS,
        DEFAULT_LOG_DIR,
        FatalSessionError,
        OandaApiError,
        PracticeScalper,
        infer_pip_size,
        normalize_instrument,
        parse_args as parse_scalper_args,
        quote_from_price_payload,
        safe_float,
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_instrument_list(value: str) -> list[str]:
    if not value.strip():
        return []
    return [normalize_instrument(item) for item in re.split(r"[\s,]+", value.strip()) if item]


def log_line(path: Path, event: str, **fields: Any) -> None:
    payload = {"time": utc_now(), "event": event, **fields}
    line = json.dumps(payload, sort_keys=True, default=str)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")
    print(line, flush=True)


def scalper_namespace(args: argparse.Namespace, instrument: str) -> argparse.Namespace:
    argv = [
        "--creds",
        str(args.creds),
        "--account-key",
        args.account_key,
        "--account-id",
        args.account_id,
        "--log-dir",
        str(args.child_log_dir),
        "--instrument",
        instrument,
        "--duration-sec",
        str(args.duration_sec),
        "--strategy",
        args.strategy,
        "--shadow-strategies",
        args.shadow_strategies,
        "--risk-per-trade-pct",
        str(args.risk_per_trade_pct),
        "--max-session-loss-pct",
        str(args.max_session_loss_pct),
        "--max-units",
        str(args.max_units),
        "--max-spread-pips",
        str(args.max_spread_pips),
        "--max-trades",
        str(args.max_trades),
        "--max-spread-atr-fraction",
        str(args.max_spread_atr_fraction),
        "--min-reward-to-spread",
        str(args.min_reward_to_spread),
        "--min-signal-pips",
        str(args.min_signal_pips),
        "--min-signal-to-spread",
        str(args.min_signal_to_spread),
        "--min-r1-pips",
        str(args.min_r1_pips),
        "--min-m5-signal-pips",
        str(args.min_m5_signal_pips),
        "--long-pos20-min",
        str(args.long_pos20_min),
        "--short-pos20-max",
        str(args.short_pos20_max),
        "--rsi-oversold",
        str(args.rsi_oversold),
        "--rsi-overbought",
        str(args.rsi_overbought),
        "--stop-loss-pips",
        str(args.stop_loss_pips),
        "--take-profit-r",
        str(args.take_profit_r),
        "--max-hold-sec",
        str(args.max_hold_sec),
        "--cooldown-sec",
        str(args.cooldown_sec),
        "--loss-cooldown-sec",
        str(args.loss_cooldown_sec),
        "--no-price-stream",
    ]
    scalper_args = parse_scalper_args(argv)
    scalper_args.dry_run = args.dry_run
    return scalper_args


def setup_score(direction: str, meta: dict[str, Any]) -> float:
    spread = max(0.1, safe_float(meta.get("spread_pips"), 0.1))
    required = max(0.1, safe_float(meta.get("required_signal_pips"), 0.1))
    r1 = safe_float(meta.get("r1_pips"))
    r3 = safe_float(meta.get("r3_pips"))
    r5 = safe_float(meta.get("r5_pips"))
    m5_r3 = safe_float(meta.get("m5_r3_pips"))
    atr = safe_float(meta.get("m5_atr14_pips"))
    signed = 1.0 if direction == "buy" else -1.0
    impulse = max(0.0, signed * r3 / required) + max(0.0, signed * r5 / required)
    confirmation = max(0.0, signed * r1) + max(0.0, signed * m5_r3 / required)
    volatility = min(2.0, atr / spread) if spread > 0.0 else 0.0
    return impulse * 10.0 + confirmation * 2.0 + volatility


def infer_shadow_direction(meta: dict[str, Any]) -> str | None:
    r3 = safe_float(meta.get("r3_pips"))
    r5 = safe_float(meta.get("r5_pips"))
    m5_r3 = safe_float(meta.get("m5_r3_pips"))
    pos20 = safe_float(meta.get("pos20"), 0.5)
    if r3 > 0.0 and r5 >= 0.0 and m5_r3 >= 0.0 and pos20 >= 0.55:
        return "buy"
    if r3 < 0.0 and r5 <= 0.0 and m5_r3 <= 0.0 and pos20 <= 0.45:
        return "sell"
    return None


def shadow_score(direction: str, meta: dict[str, Any]) -> float:
    spread = max(0.1, safe_float(meta.get("spread_pips"), 0.1))
    required = max(0.1, safe_float(meta.get("required_signal_pips"), 0.1))
    atr = safe_float(meta.get("m5_atr14_pips"))
    signed = 1.0 if direction == "buy" else -1.0
    r1 = signed * safe_float(meta.get("r1_pips"))
    r3 = signed * safe_float(meta.get("r3_pips"))
    r5 = signed * safe_float(meta.get("r5_pips"))
    m5_r3 = signed * safe_float(meta.get("m5_r3_pips"))
    spread_penalty = spread / max(0.1, atr) if atr > 0.0 else spread
    return (r1 + r3 + r5 + m5_r3) / required - spread_penalty


def theoretical_pips(direction: str, entry_bid: float, entry_ask: float, now_bid: float, now_ask: float) -> float:
    if direction == "buy":
        return (now_bid - entry_ask) / scalper_module.PIP
    return (entry_bid - now_ask) / scalper_module.PIP


def selected_instruments(args: argparse.Namespace, token: str, account_id: str) -> list[str]:
    instruments = parse_instrument_list(args.instruments)
    if not instruments:
        instruments = tradeable_currency_instruments(token, account_id)
    if args.exclude_regex:
        exclude = re.compile(args.exclude_regex)
        instruments = [instrument for instrument in instruments if not exclude.search(instrument)]
    if not instruments:
        raise SystemExit("No instruments selected.")
    return sorted(dict.fromkeys(instruments))


def process_pending_near_misses(
    token: str,
    account_id: str,
    log_path: Path,
    pending_misses: list[dict[str, Any]],
) -> None:
    due: list[tuple[dict[str, Any], float]] = []
    now = time.monotonic()
    for miss in list(pending_misses):
        checks = list(miss.get("checks") or [])
        remaining: list[float] = []
        for horizon in checks:
            if now - safe_float(miss.get("created_monotonic")) >= horizon:
                due.append((miss, horizon))
            else:
                remaining.append(horizon)
        miss["checks"] = remaining
        if not remaining:
            pending_misses.remove(miss)
    if not due:
        return
    instruments = sorted({miss["instrument"] for miss, _horizon in due})
    prices = pricing_snapshot(token, account_id, instruments)
    for miss, horizon in due:
        instrument = miss["instrument"]
        quote = prices.get(instrument)
        if quote is None:
            log_line(log_path, "near_miss_outcome", id=miss["id"], instrument=instrument, horizon_sec=horizon, error="missing_price")
            continue
        scalper_module.PIP = infer_pip_size(instrument)
        pips = theoretical_pips(
            str(miss["direction"]),
            safe_float(miss.get("entry_bid")),
            safe_float(miss.get("entry_ask")),
            quote.bid,
            quote.ask,
        )
        log_line(
            log_path,
            "near_miss_outcome",
            id=miss["id"],
            instrument=instrument,
            direction=miss["direction"],
            blocked_reason=miss.get("blocked_reason"),
            horizon_sec=horizon,
            theoretical_pips=round(pips, 3),
            entry_bid=miss.get("entry_bid"),
            entry_ask=miss.get("entry_ask"),
            exit_bid=quote.bid,
            exit_ask=quote.ask,
            exit_time=quote.time,
        )


def pricing_snapshot(token: str, account_id: str, instruments: list[str]) -> dict[str, Any]:
    prices: dict[str, Any] = {}
    chunk_size = 50
    for index in range(0, len(instruments), chunk_size):
        chunk = instruments[index : index + chunk_size]
        payload = oanda_get(
            token,
            f"/v3/accounts/{account_id}/pricing",
            params={"instruments": ",".join(chunk), "includeUnitsAvailable": "false"},
        )
        for price in payload.get("prices") or []:
            instrument = str(price.get("instrument") or "")
            if instrument:
                prices[instrument] = quote_from_price_payload(price, "scan_pricing")
    return prices


def complete_candles(token: str, instrument: str, granularity: str, count: int) -> list[dict[str, Any]]:
    payload = oanda_get(
        token,
        f"/v3/instruments/{instrument}/candles",
        params={"price": "MBA", "granularity": granularity, "count": str(count)},
    )
    candles = payload.get("candles") or []
    return [candle for candle in candles if isinstance(candle, dict) and candle.get("complete")]


def candle_snapshot(token: str, instruments: list[str], workers: int) -> dict[str, dict[str, list[dict[str, Any]]]]:
    output: dict[str, dict[str, list[dict[str, Any]]]] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {}
        for instrument in instruments:
            futures[executor.submit(complete_candles, token, instrument, "M1", 90)] = (instrument, "M1")
            futures[executor.submit(complete_candles, token, instrument, "M5", 50)] = (instrument, "M5")
        for future in as_completed(futures):
            instrument, granularity = futures[future]
            output.setdefault(instrument, {})[granularity] = future.result()
    return output


def scan_once(
    args: argparse.Namespace,
    instruments: list[str],
    log_path: Path,
    pending_misses: list[dict[str, Any]],
) -> tuple[PracticeScalper | None, str | None, dict[str, Any] | None]:
    best: tuple[float, str, str, dict[str, Any]] | None = None
    near_misses: list[tuple[float, dict[str, Any]]] = []
    reason_counts: dict[str, int] = {}
    token, account_id = read_credentials(args.creds, args.account_key, args.account_id)
    prices = pricing_snapshot(token, account_id, instruments)
    price_candidates: list[tuple[str, Any]] = []
    scanned = 0
    for instrument in instruments:
        scanned += 1
        quote = prices.get(instrument)
        if quote is None:
            reason_counts["missing_price"] = reason_counts.get("missing_price", 0) + 1
            continue
        if not quote.tradeable:
            reason_counts["quote_not_tradeable"] = reason_counts.get("quote_not_tradeable", 0) + 1
            continue
        if quote.spread_pips > args.max_spread_pips:
            reason_counts["skip_spread"] = reason_counts.get("skip_spread", 0) + 1
            continue
        price_candidates.append((instrument, quote))

    candle_sets_by_instrument = candle_snapshot(
        token,
        [instrument for instrument, _quote in price_candidates],
        args.candle_workers,
    )
    eligible = 0
    for instrument, quote in price_candidates:
        try:
            scalper_module.INSTRUMENT = instrument
            scalper_module.PIP = infer_pip_size(instrument)
            bot = PracticeScalper.__new__(PracticeScalper)
            bot.args = copy.deepcopy(scalper_namespace(args, instrument))
            candle_sets = candle_sets_by_instrument.get(instrument) or {}
            common, rejection = bot.common_signal_meta(quote, candle_sets)
            if rejection:
                meta = {**common, "strategy": bot.args.strategy, "reason": rejection}
                direction = None
            else:
                direction, meta = bot.evaluate_strategy(bot.args.strategy, quote, candle_sets, common)
            reason = str(meta.get("reason") or "")
            if not direction:
                shadow_direction = infer_shadow_direction(meta)
                if shadow_direction:
                    score = shadow_score(shadow_direction, meta)
                    near_misses.append(
                        (
                            score,
                            {
                                "id": f"{int(time.time() * 1000)}-{instrument}-{len(pending_misses) + len(near_misses)}",
                                "instrument": instrument,
                                "direction": shadow_direction,
                                "blocked_reason": reason or "no_direction",
                                "score": round(score, 4),
                                "entry_bid": quote.bid,
                                "entry_ask": quote.ask,
                                "entry_mid": quote.mid,
                                "entry_time": quote.time,
                                "created_monotonic": time.monotonic(),
                                "checks": [60.0, 180.0, 300.0],
                                "signal": meta,
                            },
                        )
                    )
                reason_counts[reason or "no_direction"] = reason_counts.get(reason or "no_direction", 0) + 1
                continue
            eligible += 1
            score = setup_score(direction, meta)
            log_line(
                log_path,
                "candidate",
                instrument=instrument,
                direction=direction,
                score=round(score, 4),
                spread_pips=round(quote.spread_pips, 3),
                signal=meta,
            )
            if best is None or score > best[0]:
                best = (score, instrument, direction, meta)
        except (FatalSessionError, OandaApiError, SystemExit) as exc:
            reason_counts[type(exc).__name__] = reason_counts.get(type(exc).__name__, 0) + 1
            log_line(log_path, "scan_instrument_error", instrument=instrument, error_type=type(exc).__name__, error=str(exc)[:500])
        except Exception as exc:
            reason_counts[type(exc).__name__] = reason_counts.get(type(exc).__name__, 0) + 1
            log_line(log_path, "scan_instrument_error", instrument=instrument, error_type=type(exc).__name__, error=str(exc)[:500])

    log_line(
        log_path,
        "scan_summary",
        scanned=scanned,
        price_candidates=len(price_candidates),
        eligible=eligible,
        reason_counts=reason_counts,
        selected=None if best is None else best[1],
        selected_score=None if best is None else round(best[0], 4),
    )
    for _score, miss in sorted(near_misses, key=lambda item: item[0], reverse=True)[: args.near_miss_limit]:
        pending_misses.append(miss)
        log_line(
            log_path,
            "near_miss",
            id=miss["id"],
            instrument=miss["instrument"],
            direction=miss["direction"],
            blocked_reason=miss["blocked_reason"],
            score=miss["score"],
            entry_bid=miss["entry_bid"],
            entry_ask=miss["entry_ask"],
            entry_mid=miss["entry_mid"],
            entry_time=miss["entry_time"],
            signal=miss["signal"],
        )
    if best is None:
        return None, None, None
    return PracticeScalper(copy.deepcopy(scalper_namespace(args, best[1]))), best[2], best[3]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    parser.add_argument("--account-key", default="OANDA_ACCOUNT_ID_DUM3")
    parser.add_argument("--account-id", default="")
    parser.add_argument("--run-label", default="", help="Human-readable strategy variant label for logs/dashboard.")
    parser.add_argument("--duration-sec", type=int, default=3600)
    parser.add_argument("--scan-pause-sec", type=float, default=10.0)
    parser.add_argument("--max-spread-pips", type=float, default=1.8)
    parser.add_argument("--candle-workers", type=int, default=16)
    parser.add_argument("--near-miss-limit", type=int, default=5)
    parser.add_argument("--instruments", default="")
    parser.add_argument("--exclude-regex", default="")
    parser.add_argument("--strategy", choices=("momentum", "pullback", "macd_rsi_reversal"), default="pullback")
    parser.add_argument("--shadow-strategies", default="", help="Keep empty for faster 68-pair scans.")
    parser.add_argument("--risk-per-trade-pct", type=float, default=0.25)
    parser.add_argument("--max-session-loss-pct", type=float, default=1.0)
    parser.add_argument("--max-units", type=int, default=250)
    parser.add_argument("--max-trades", type=int, default=5)
    parser.add_argument("--max-spread-atr-fraction", type=float, default=0.55)
    parser.add_argument("--min-reward-to-spread", type=float, default=4.0)
    parser.add_argument("--min-signal-pips", type=float, default=1.0)
    parser.add_argument("--min-signal-to-spread", type=float, default=2.5)
    parser.add_argument("--min-r1-pips", type=float, default=0.1)
    parser.add_argument("--min-m5-signal-pips", type=float, default=0.3)
    parser.add_argument("--long-pos20-min", type=float, default=0.65)
    parser.add_argument("--short-pos20-max", type=float, default=0.35)
    parser.add_argument("--rsi-oversold", type=float, default=35.0)
    parser.add_argument("--rsi-overbought", type=float, default=65.0)
    parser.add_argument("--stop-loss-pips", type=float, default=5.0)
    parser.add_argument("--take-profit-r", type=float, default=1.6)
    parser.add_argument("--max-hold-sec", type=float, default=180.0)
    parser.add_argument("--cooldown-sec", type=float, default=120.0)
    parser.add_argument("--loss-cooldown-sec", type=float, default=300.0)
    parser.add_argument("--child-log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    args.log_dir.mkdir(parents=True, exist_ok=True)
    args.child_log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    selector = re.sub(r"[^a-z0-9]+", "_", str(args.account_key or args.account_id or "default").lower()).strip("_")
    label_slug = re.sub(r"[^a-z0-9]+", "_", str(args.run_label or args.strategy).lower()).strip("_")
    log_path = args.log_dir / f"practice_all_pairs_opportunity_{selector}_{label_slug}_{stamp}.jsonl"
    token, account_id = read_credentials(args.creds, args.account_key, args.account_id)
    instruments = selected_instruments(args, token, account_id)

    log_line(
        log_path,
        "scanner_start",
        account_suffix=account_id[-4:],
        instrument_count=len(instruments),
        instruments=instruments,
        strategy=args.strategy,
        run_label=args.run_label or args.strategy,
        parameters={
            "max_spread_pips": args.max_spread_pips,
            "max_spread_atr_fraction": args.max_spread_atr_fraction,
            "min_reward_to_spread": args.min_reward_to_spread,
            "min_signal_pips": args.min_signal_pips,
            "min_signal_to_spread": args.min_signal_to_spread,
            "min_r1_pips": args.min_r1_pips,
            "min_m5_signal_pips": args.min_m5_signal_pips,
            "long_pos20_min": args.long_pos20_min,
            "short_pos20_max": args.short_pos20_max,
            "rsi_oversold": args.rsi_oversold,
            "rsi_overbought": args.rsi_overbought,
            "stop_loss_pips": args.stop_loss_pips,
            "take_profit_r": args.take_profit_r,
            "max_hold_sec": args.max_hold_sec,
            "cooldown_sec": args.cooldown_sec,
            "loss_cooldown_sec": args.loss_cooldown_sec,
        },
        max_trades=args.max_trades,
        dry_run=args.dry_run,
    )
    stop_at = time.monotonic() + args.duration_sec
    trades = 0
    consecutive_losses = 0
    pending_misses: list[dict[str, Any]] = []
    while time.monotonic() < stop_at and trades < args.max_trades:
        process_pending_near_misses(token, account_id, log_path, pending_misses)
        bot, direction, meta = scan_once(args, instruments, log_path, pending_misses)
        if bot is None or direction is None or meta is None:
            time.sleep(min(args.scan_pause_sec, max(0.0, stop_at - time.monotonic())))
            continue
        log_line(log_path, "selected_setup", instrument=bot.args.instrument, direction=direction, signal=meta)
        context = bot.place_order(direction, bot.quote(), meta)
        if context is None:
            time.sleep(min(args.scan_pause_sec, max(0.0, stop_at - time.monotonic())))
            continue
        trades += 1
        outcome = bot.monitor_trade(context)
        log_line(
            log_path,
            "selected_trade_result",
            instrument=bot.args.instrument,
            realized_pl=round(outcome.realized_pl, 6),
            realized_r=round(outcome.realized_r, 4),
            reason=outcome.reason,
        )
        if outcome.realized_r < -abs(bot.args.scratch_r):
            consecutive_losses += 1
        elif outcome.realized_r > abs(bot.args.scratch_r):
            consecutive_losses = 0
        if consecutive_losses >= bot.args.max_consecutive_losses:
            log_line(log_path, "consecutive_loss_cap_hit", consecutive_losses=consecutive_losses)
            break
        time.sleep(min(args.scan_pause_sec, max(0.0, stop_at - time.monotonic())))

    process_pending_near_misses(token, account_id, log_path, pending_misses)
    log_line(log_path, "scanner_end", trades=trades, consecutive_losses=consecutive_losses, pending_near_misses=len(pending_misses))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
