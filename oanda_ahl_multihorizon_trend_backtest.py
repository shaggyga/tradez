#!/usr/bin/env python3
"""Causal all-pair backtest for the public Man AHL-style trend example.

Signals use completed New York 17:00 FX sessions and execute at the next
session open.  The report compares the four-lookback blend against each
single-lookback variant after a configurable round-trip cost.  This is a
research diagnostic only; it never connects to an account or submits orders.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo


NY_TZ = ZoneInfo("America/New_York")
DEFAULT_CANDLE_DIR = Path(__file__).resolve().parent / "data" / "oanda_training_manager" / "candles"
DEFAULT_REPORT = (
    Path(__file__).resolve().parent
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "ahl_multihorizon_trend_backtest_latest.json"
)
LOOKBACKS = (5, 10, 21, 42)


@dataclass(frozen=True)
class DailyBar:
    session: date
    open: float
    close: float


def safe_float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if math.isfinite(result) else 0.0


def parse_utc(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def fx_session_date(timestamp: datetime) -> date:
    return (timestamp.astimezone(NY_TZ) - timedelta(hours=17)).date()


def fx_session_date_from_utc_text(
    value: str,
    cache: dict[tuple[str, bool], date],
    boundary_cache: dict[str, int],
) -> date | None:
    """Fast M1 row bucketing with one timezone conversion per UTC date."""

    text = str(value or "")
    if len(text) < 13:
        return None
    day_text = text[:10]
    try:
        hour = int(text[11:13])
    except ValueError:
        return None
    boundary_hour = boundary_cache.get(day_text)
    if boundary_hour is None:
        try:
            utc_day = date.fromisoformat(day_text)
        except ValueError:
            return None
        noon_utc = datetime(
            utc_day.year,
            utc_day.month,
            utc_day.day,
            12,
            tzinfo=timezone.utc,
        )
        offset_hours = int(
            noon_utc.astimezone(NY_TZ).utcoffset().total_seconds() / 3600
        )
        boundary_hour = 17 - offset_hours
        boundary_cache[day_text] = boundary_hour
    before_close = hour < boundary_hour
    key = (day_text, before_close)
    session = cache.get(key)
    if session is None:
        utc_day = date.fromisoformat(day_text)
        session = utc_day - timedelta(days=1) if before_close else utc_day
        cache[key] = session
    return session


def load_daily_bars(path: Path) -> list[DailyBar]:
    sessions: dict[date, list[float]] = {}
    session_cache: dict[tuple[str, bool], date] = {}
    boundary_cache: dict[str, int] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        header = next(handle, "").rstrip("\r\n").split(",")
        indexes = {name: offset for offset, name in enumerate(header)}
        time_index = indexes.get("datetime", indexes.get("time", 0))
        open_index = indexes.get("open", 4)
        close_index = indexes.get("close", 7)
        required_index = max(time_index, open_index, close_index)
        for line in handle:
            fields = line.rstrip("\r\n").split(",", required_index + 1)
            if len(fields) <= required_index:
                continue
            session = fx_session_date_from_utc_text(
                fields[time_index],
                session_cache,
                boundary_cache,
            )
            row_open = safe_float(fields[open_index])
            row_close = safe_float(fields[close_index])
            if session is None or row_open <= 0.0 or row_close <= 0.0:
                continue
            existing = sessions.get(session)
            if existing is None:
                sessions[session] = [row_open, row_close]
            else:
                existing[1] = row_close
    return [
        DailyBar(session=session, open=values[0], close=values[1])
        for session, values in sorted(sessions.items())
    ]


def trailing_annualized_volatility(closes: list[float], index: int, lookback: int) -> float:
    start = max(1, index - lookback + 1)
    returns = [
        math.log(closes[offset] / closes[offset - 1])
        for offset in range(start, index + 1)
        if closes[offset] > 0.0 and closes[offset - 1] > 0.0
    ]
    if len(returns) < 2:
        return 0.0
    return statistics.pstdev(returns) * math.sqrt(252.0)


def score_variants(closes: list[float], index: int) -> dict[str, float]:
    votes = {
        lookback: 1.0 if closes[index] > closes[index - lookback] else -1.0
        for lookback in LOOKBACKS
    }
    return {
        "multi_5_10_21_42": sum(votes.values()) / len(LOOKBACKS),
        **{f"lookback_{lookback}": vote for lookback, vote in votes.items()},
    }


def pip_size(instrument: str) -> float:
    return 0.01 if instrument.endswith("_JPY") else 0.0001


def backtest_pair(
    instrument: str,
    bars: list[DailyBar],
    *,
    target_annualized_volatility: float,
    volatility_lookback: int,
    max_volatility_scalar: float,
    round_trip_cost_pips: float,
) -> tuple[dict[str, Any], dict[str, list[tuple[date, float]]]]:
    closes = [bar.close for bar in bars]
    series: dict[str, list[tuple[date, float]]] = defaultdict(list)
    previous_weights: dict[str, float] = defaultdict(float)
    multi_rows: list[dict[str, Any]] = []
    start = max(max(LOOKBACKS), volatility_lookback)
    for index in range(start, len(bars) - 2):
        annualized_volatility = trailing_annualized_volatility(
            closes,
            index,
            volatility_lookback,
        )
        if annualized_volatility <= 1e-12:
            continue
        volatility_scalar = min(
            max_volatility_scalar,
            target_annualized_volatility / annualized_volatility,
        )
        entry = bars[index + 1].open
        exit_price = bars[index + 2].open
        if entry <= 0.0 or exit_price <= 0.0:
            continue
        underlying_return = exit_price / entry - 1.0
        for variant, conviction in score_variants(closes, index).items():
            weight = conviction * volatility_scalar
            turnover = abs(weight - previous_weights[variant])
            cost_return = (
                turnover * round_trip_cost_pips * pip_size(instrument) / entry
            )
            net_return = weight * underlying_return - cost_return
            series[variant].append((bars[index + 1].session, net_return))
            previous_weights[variant] = weight
            if variant == "multi_5_10_21_42":
                multi_rows.append(
                    {
                        "date": bars[index + 1].session,
                        "net_return": net_return,
                        "underlying_return": underlying_return,
                        "weight": weight,
                        "turnover": turnover,
                    }
                )
    multi_returns = [row["net_return"] for row in multi_rows]
    correct = sum(
        1
        for row in multi_rows
        if row["weight"] != 0.0
        and math.copysign(1.0, row["weight"])
        == math.copysign(1.0, row["underlying_return"])
    )
    active = sum(1 for row in multi_rows if row["weight"] != 0.0)
    metrics = summarize_returns(multi_returns)
    metrics.update(
        {
            "instrument": instrument,
            "daily_bars": len(bars),
            "decisions": len(multi_rows),
            "active_directional_days": active,
            "direction_hit_rate_pct": round(100.0 * correct / active, 3) if active else None,
            "turnover": round(sum(row["turnover"] for row in multi_rows), 4),
            "first_session": bars[0].session.isoformat() if bars else None,
            "last_session": bars[-1].session.isoformat() if bars else None,
        }
    )
    return metrics, series


def backtest_path(
    path: Path,
    *,
    target_annualized_volatility: float,
    volatility_lookback: int,
    max_volatility_scalar: float,
    round_trip_cost_pips: float,
) -> tuple[str, dict[str, Any], dict[str, list[tuple[date, float]]]]:
    instrument = path.stem.removesuffix("_M1")
    bars = load_daily_bars(path)
    metrics, series = backtest_pair(
        instrument,
        bars,
        target_annualized_volatility=target_annualized_volatility,
        volatility_lookback=volatility_lookback,
        max_volatility_scalar=max_volatility_scalar,
        round_trip_cost_pips=round_trip_cost_pips,
    )
    return instrument, metrics, series


def summarize_returns(returns: Iterable[float]) -> dict[str, Any]:
    values = [safe_float(value) for value in returns]
    if not values:
        return {
            "observations": 0,
            "total_return_pct": None,
            "annualized_return_pct": None,
            "annualized_volatility_pct": None,
            "sharpe": None,
            "max_drawdown_pct": None,
            "positive_day_rate_pct": None,
        }
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    positive = 0
    for value in values:
        equity *= max(1e-9, 1.0 + value)
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1.0)
        positive += int(value > 0.0)
    mean = statistics.fmean(values)
    volatility = statistics.pstdev(values) if len(values) >= 2 else 0.0
    years = len(values) / 252.0
    annualized_return = equity ** (1.0 / years) - 1.0 if years > 0.0 else 0.0
    return {
        "observations": len(values),
        "total_return_pct": round((equity - 1.0) * 100.0, 4),
        "annualized_return_pct": round(annualized_return * 100.0, 4),
        "annualized_volatility_pct": round(volatility * math.sqrt(252.0) * 100.0, 4),
        "sharpe": round(mean / volatility * math.sqrt(252.0), 4) if volatility > 1e-12 else None,
        "max_drawdown_pct": round(max_drawdown * 100.0, 4),
        "positive_day_rate_pct": round(positive / len(values) * 100.0, 3),
    }


def aggregate_portfolio(
    pair_series: dict[str, dict[str, list[tuple[date, float]]]],
) -> dict[str, dict[date, float]]:
    totals: dict[str, dict[date, list[float]]] = defaultdict(lambda: defaultdict(list))
    for variants in pair_series.values():
        for variant, rows in variants.items():
            for session, value in rows:
                totals[variant][session].append(value)
    return {
        variant: {
            session: statistics.fmean(values)
            for session, values in sorted(days.items())
            if values
        }
        for variant, days in totals.items()
    }


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candle-dir", type=Path, default=DEFAULT_CANDLE_DIR)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--instruments", default="")
    parser.add_argument("--max-files", type=int, default=0)
    parser.add_argument("--target-annualized-volatility", type=float, default=0.10)
    parser.add_argument("--volatility-lookback", type=int, default=20)
    parser.add_argument("--max-volatility-scalar", type=float, default=2.0)
    parser.add_argument("--round-trip-cost-pips", type=float, default=1.2)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    selected = {
        value.strip().upper()
        for value in args.instruments.replace(",", " ").split()
        if value.strip()
    }
    paths = sorted(args.candle_dir.glob("*_M1.csv"))
    if selected:
        paths = [path for path in paths if path.stem.removesuffix("_M1") in selected]
    if args.max_files > 0:
        paths = paths[: args.max_files]
    if not paths:
        raise SystemExit("No matching M1 candle files found.")

    pair_metrics: list[dict[str, Any]] = []
    pair_series: dict[str, dict[str, list[tuple[date, float]]]] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {
            pool.submit(
                backtest_path,
                path,
                target_annualized_volatility=args.target_annualized_volatility,
                volatility_lookback=args.volatility_lookback,
                max_volatility_scalar=args.max_volatility_scalar,
                round_trip_cost_pips=args.round_trip_cost_pips,
            ): path.stem.removesuffix("_M1")
            for path in paths
        }
        for future in as_completed(futures):
            instrument = futures[future]
            try:
                _, metrics, series = future.result()
                pair_metrics.append(metrics)
                pair_series[instrument] = series
            except Exception as exc:  # retain all-pair diagnostics instead of aborting
                errors[instrument] = f"{type(exc).__name__}: {exc}"

    portfolio_days = aggregate_portfolio(pair_series)
    all_dates = sorted({day for days in portfolio_days.values() for day in days})
    holdout_start = (
        all_dates[max(0, int(len(all_dates) * 0.80))]
        if all_dates
        else None
    )
    variants: dict[str, Any] = {}
    for variant, days in portfolio_days.items():
        ordered = [value for _, value in sorted(days.items())]
        holdout = [
            value
            for day, value in sorted(days.items())
            if holdout_start is not None and day >= holdout_start
        ]
        variants[variant] = {
            "full": summarize_returns(ordered),
            "holdout": summarize_returns(holdout),
        }
    ranked = sorted(
        variants,
        key=lambda name: safe_float(variants[name]["holdout"].get("sharpe")),
        reverse=True,
    )
    payload = {
        "schema_version": 1,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "research_only": True,
        "causal_policy": "signal_at_completed_17NY_session_close_fill_next_session_open",
        "lookbacks_days": list(LOOKBACKS),
        "cost_policy": {
            "round_trip_cost_pips": args.round_trip_cost_pips,
            "charged_on_absolute_target_weight_turnover": True,
        },
        "risk_policy": {
            "target_annualized_volatility": args.target_annualized_volatility,
            "volatility_lookback_days": args.volatility_lookback,
            "max_volatility_scalar": args.max_volatility_scalar,
        },
        "files_requested": len(paths),
        "pairs_completed": len(pair_metrics),
        "errors": errors,
        "holdout_start": holdout_start.isoformat() if holdout_start else None,
        "variant_rank_by_holdout_sharpe": ranked,
        "portfolio_variants": variants,
        "pair_metrics_multi": sorted(
            pair_metrics,
            key=lambda row: safe_float(row.get("sharpe")),
            reverse=True,
        ),
        "limitations": [
            "equal-weight pair aggregation does not net shared currency exposure",
            "fixed pip cost is conservative but not a historical tick-spread reconstruction",
            "daily bars are reconstructed from stored M1 mid candles",
        ],
    }
    atomic_json(args.report, payload)
    print(json.dumps({
        "report": str(args.report.resolve()),
        "pairs_completed": len(pair_metrics),
        "errors": len(errors),
        "holdout_start": payload["holdout_start"],
        "variant_rank_by_holdout_sharpe": ranked,
        "portfolio_variants": variants,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
