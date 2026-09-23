"""Read-only health check for the two independent practice quote transports."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import statistics
import time
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo


UTC = dt.timezone.utc
NEW_YORK = ZoneInfo("America/New_York")
EXPECTED_CANONICAL_PRODUCER = "practice_007_fast_executor_price_stream"


def utc_now() -> dt.datetime:
    return dt.datetime.now(UTC)


def parse_utc(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def read_snapshot(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("quotes"), dict):
        raise ValueError(f"invalid quote snapshot: {path}")
    return payload


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1))
    return ordered[index]


def snapshot_age_seconds(payload: Mapping[str, Any], now: dt.datetime) -> float | None:
    generated = parse_utc(payload.get("generated_utc"))
    if generated is None:
        return None
    return max(0.0, (now - generated).total_seconds())


def retained_last_known_instruments(payload: Mapping[str, Any]) -> set[str]:
    """Return rows explicitly retained for research, never current transport."""
    coverage = payload.get("coverage") or {}
    if not isinstance(coverage, Mapping):
        return set()
    raw = coverage.get("retained_last_known_instruments") or []
    if not isinstance(raw, (list, tuple, set)):
        return set()
    return {str(instrument) for instrument in raw if str(instrument).strip()}


def forex_market_state(now: dt.datetime) -> str:
    """Classify the regular weekend closure using the New York FX clock.

    The prior UTC-hour shortcut shifted the boundary by one hour during US
    daylight-saving time. OANDA's regular FX week closes Friday at 17:00 New
    York and reopens Sunday at 17:00 New York.
    """

    now_new_york = now.astimezone(NEW_YORK)
    if now_new_york.weekday() == 5:
        return "weekend_closed"
    if now_new_york.weekday() == 4 and now_new_york.time() >= dt.time(17, 0):
        return "weekend_closed"
    if now_new_york.weekday() == 6 and now_new_york.time() < dt.time(17, 0):
        return "weekend_closed"
    return "open_or_transition"


def compare_snapshots(
    canonical: Mapping[str, Any],
    comparison: Mapping[str, Any],
    *,
    now: dt.datetime,
    max_snapshot_age_sec: float = 20.0,
    minimum_shared_ratio: float = 0.95,
    maximum_p95_divergence_to_spread: float = 1.5,
    maximum_p95_quote_time_skew_sec: float = 20.0,
) -> dict[str, Any]:
    canonical_quotes = canonical.get("quotes") or {}
    comparison_quotes = comparison.get("quotes") or {}
    canonical_retained = retained_last_known_instruments(canonical)
    comparison_retained = retained_last_known_instruments(comparison)
    canonical_current = set(canonical_quotes).difference(canonical_retained)
    comparison_current = set(comparison_quotes).difference(comparison_retained)
    instruments = sorted(set(canonical_quotes) | set(comparison_quotes))
    current_instruments = sorted(canonical_current | comparison_current)
    shared = sorted(canonical_current & comparison_current)
    divergence_pips: list[float] = []
    divergence_to_spread: list[float] = []
    quote_time_skew_sec: list[float] = []
    worst: list[dict[str, Any]] = []

    for instrument in shared:
        left = canonical_quotes[instrument]
        right = comparison_quotes[instrument]
        pip = float(left.get("pip") or right.get("pip") or 0.0)
        if pip <= 0:
            continue
        left_bid = float(left["bid"])
        left_ask = float(left["ask"])
        right_bid = float(right["bid"])
        right_ask = float(right["ask"])
        left_mid = 0.5 * (left_bid + left_ask)
        right_mid = 0.5 * (right_bid + right_ask)
        delta = abs(left_mid - right_mid) / pip
        average_spread = max(
            1.0,
            0.5 * (((left_ask - left_bid) / pip) + ((right_ask - right_bid) / pip)),
        )
        ratio = delta / average_spread
        divergence_pips.append(delta)
        divergence_to_spread.append(ratio)
        left_time = parse_utc(left.get("time"))
        right_time = parse_utc(right.get("time"))
        skew = None
        if left_time is not None and right_time is not None:
            skew = abs((left_time - right_time).total_seconds())
            quote_time_skew_sec.append(skew)
        worst.append(
            {
                "instrument": instrument,
                "mid_divergence_pips": round(delta, 6),
                "divergence_to_average_spread": round(ratio, 6),
                "quote_time_skew_sec": round(skew, 6) if skew is not None else None,
            }
        )

    total = max(1, len(instruments))
    shared_ratio = len(shared) / total
    canonical_age = snapshot_age_seconds(canonical, now)
    comparison_age = snapshot_age_seconds(comparison, now)
    p95_ratio = percentile(divergence_to_spread, 0.95)
    p95_skew = percentile(quote_time_skew_sec, 0.95)
    reasons: list[str] = []
    informational_reasons: list[str] = []
    market_state = forex_market_state(now)
    if canonical.get("producer") != EXPECTED_CANONICAL_PRODUCER:
        reasons.append("unexpected_canonical_producer")
    if canonical_age is None or canonical_age > max_snapshot_age_sec:
        if market_state == "weekend_closed":
            informational_reasons.append("canonical_snapshot_stale_market_closed")
        else:
            reasons.append("canonical_snapshot_stale")
    if comparison_age is None or comparison_age > max_snapshot_age_sec:
        if market_state == "weekend_closed":
            informational_reasons.append("comparison_snapshot_stale_market_closed")
        else:
            reasons.append("comparison_snapshot_stale")
    if shared_ratio < minimum_shared_ratio:
        if market_state == "weekend_closed":
            informational_reasons.append(
                "insufficient_shared_instruments_market_closed"
            )
        else:
            reasons.append("insufficient_shared_instruments")
    if p95_ratio is None or p95_ratio > maximum_p95_divergence_to_spread:
        if market_state == "weekend_closed" and p95_ratio is None:
            informational_reasons.append(
                "transport_price_comparison_unavailable_market_closed"
            )
        else:
            reasons.append("transport_price_divergence")
    if p95_skew is None or p95_skew > maximum_p95_quote_time_skew_sec:
        if market_state == "weekend_closed":
            informational_reasons.append("transport_quote_time_skew_market_closed")
        else:
            reasons.append("transport_quote_time_skew")

    worst.sort(
        key=lambda row: (
            float(row["divergence_to_average_spread"]),
            float(row["mid_divergence_pips"]),
        ),
        reverse=True,
    )
    return {
        "schema_version": 1,
        "generated_utc": now.isoformat(),
        "status": "healthy" if not reasons else "degraded",
        "market_state": market_state,
        "research_only": True,
        "can_place_orders": False,
        "reasons": reasons,
        "informational_reasons": informational_reasons,
        "canonical": {
            "producer": canonical.get("producer"),
            "generated_utc": canonical.get("generated_utc"),
            "age_sec": round(canonical_age, 6) if canonical_age is not None else None,
            "quote_count": len(canonical_quotes),
            "current_quote_count": len(canonical_current),
            "retained_last_known_count": len(
                set(canonical_quotes) & canonical_retained
            ),
            "retained_last_known_instruments": sorted(
                set(canonical_quotes) & canonical_retained
            ),
        },
        "comparison": {
            "producer": comparison.get("producer"),
            "generated_utc": comparison.get("generated_utc"),
            "age_sec": round(comparison_age, 6) if comparison_age is not None else None,
            "quote_count": len(comparison_quotes),
            "current_quote_count": len(comparison_current),
            "retained_last_known_count": len(
                set(comparison_quotes) & comparison_retained
            ),
            "retained_last_known_instruments": sorted(
                set(comparison_quotes) & comparison_retained
            ),
        },
        "metrics": {
            "instrument_union_count": len(instruments),
            "current_instrument_union_count": len(current_instruments),
            "shared_instrument_count": len(shared),
            "shared_instrument_ratio": round(shared_ratio, 6),
            "median_mid_divergence_pips": round(statistics.median(divergence_pips), 6)
            if divergence_pips
            else None,
            "p95_mid_divergence_pips": round(percentile(divergence_pips, 0.95), 6)
            if divergence_pips
            else None,
            "maximum_mid_divergence_pips": round(max(divergence_pips), 6)
            if divergence_pips
            else None,
            "median_divergence_to_average_spread": round(
                statistics.median(divergence_to_spread), 6
            )
            if divergence_to_spread
            else None,
            "p95_divergence_to_average_spread": round(p95_ratio, 6)
            if p95_ratio is not None
            else None,
            "p95_quote_time_skew_sec": round(p95_skew, 6)
            if p95_skew is not None
            else None,
        },
        "worst_relative_divergences": worst[:10],
        "thresholds": {
            "max_snapshot_age_sec": max_snapshot_age_sec,
            "minimum_shared_ratio": minimum_shared_ratio,
            "maximum_p95_divergence_to_spread": maximum_p95_divergence_to_spread,
            "maximum_p95_quote_time_skew_sec": maximum_p95_quote_time_skew_sec,
        },
    }


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    now = utc_now()
    canonical = read_snapshot(args.canonical)
    comparison = read_snapshot(args.comparison)
    result = compare_snapshots(
        canonical,
        comparison,
        now=now,
        max_snapshot_age_sec=args.max_snapshot_age_sec,
        minimum_shared_ratio=args.minimum_shared_ratio,
        maximum_p95_divergence_to_spread=(
            args.maximum_p95_divergence_to_spread
        ),
        maximum_p95_quote_time_skew_sec=args.maximum_p95_quote_time_skew_sec,
    )
    result["canonical"]["path"] = str(args.canonical.resolve())
    result["comparison"]["path"] = str(args.comparison.resolve())
    atomic_write_json(args.state, result)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--canonical", type=Path, required=True)
    parser.add_argument("--comparison", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument("--max-snapshot-age-sec", type=float, default=20.0)
    parser.add_argument("--minimum-shared-ratio", type=float, default=0.95)
    parser.add_argument(
        "--maximum-p95-divergence-to-spread", type=float, default=1.5
    )
    parser.add_argument("--maximum-p95-quote-time-skew-sec", type=float, default=20.0)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    started = time.monotonic()
    while time.monotonic() - started < max(0.0, args.duration_sec):
        try:
            run_once(args)
        except Exception as exc:  # diagnostic worker must fail visibly, not silently
            atomic_write_json(
                args.state,
                {
                    "schema_version": 1,
                    "generated_utc": utc_now().isoformat(),
                    "status": "error",
                    "research_only": True,
                    "can_place_orders": False,
                    "error": f"{type(exc).__name__}: {exc}"[:500],
                },
            )
        time.sleep(max(1.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
