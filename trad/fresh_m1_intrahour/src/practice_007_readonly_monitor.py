from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    for attempt in range(3):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, json.JSONDecodeError):
            if attempt < 2:
                time.sleep(0.2)
    return {}


def finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def preferred_horizon(signal: dict[str, Any]) -> dict[str, Any]:
    preferred = int(signal.get("preferred_horizon_sec") or 0)
    rows = signal.get("horizon_breakdown") or []
    selected = next(
        (
            row
            for row in rows
            if int(row.get("horizon_sec") or 0) == preferred
        ),
        rows[0] if rows else {},
    )
    return {
        "horizon_sec": int(selected.get("horizon_sec") or preferred),
        "best_family": selected.get("best_family"),
        "best_model_id": selected.get("best_model_id"),
        "raw_probability_up": finite_float(selected.get("raw_probability_up")),
        "filtered_probability_up": finite_float(
            selected.get("filtered_probability_up")
        ),
        "execution_probability_up": finite_float(
            selected.get("execution_probability_up")
        ),
        "projected_gross_movement_pips": finite_float(
            selected.get("projected_gross_movement_pips")
        ),
        "projected_net_pips": finite_float(selected.get("projected_net_pips")),
        "instant_projected_net_pips": finite_float(
            selected.get("instant_projected_net_pips")
        ),
        "gross_to_spread": finite_float(selected.get("gross_to_spread")),
        "aligned_weight_pct": finite_float(
            selected.get("ensemble_aligned_weight_pct")
        ),
        "family_count": int(selected.get("family_count") or 0),
        "eligible_component_count": int(
            selected.get("eligible_component_count") or 0
        ),
        "signal_eligible": bool(selected.get("signal_eligible")),
        "blocked_by": list(selected.get("signal_blocked_by") or []),
    }


def compact_signal(signal: dict[str, Any]) -> dict[str, Any]:
    return {
        "instrument": signal.get("instrument"),
        "direction": signal.get("direction"),
        "direction_conflict": bool(signal.get("direction_conflict")),
        "confidence": finite_float(signal.get("signal_confidence")),
        "projected_net_pips": finite_float(signal.get("projected_net_pips")),
        "instant_projected_net_pips": finite_float(
            signal.get("instant_projected_net_pips")
        ),
        "historical_expected_net_pips": finite_float(
            signal.get("historical_expected_net_pips")
        ),
        "historical_reliability": finite_float(
            signal.get("historical_reliability")
        ),
        "agreement_family_count": int(
            signal.get("agreement_family_count") or 0
        ),
        "opposing_family_count": int(signal.get("opposing_family_count") or 0),
        "blocked_by": list(signal.get("signal_blocked_by") or []),
        "component_families": list(signal.get("component_families") or []),
        "preferred": preferred_horizon(signal),
    }


def currency_bias(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    state: dict[str, dict[str, float]] = defaultdict(
        lambda: {"votes": 0.0, "confidence_weighted": 0.0}
    )
    for signal in signals:
        instrument = str(signal.get("instrument") or "")
        if "_" not in instrument:
            continue
        base, quote = instrument.split("_", 1)
        side = 1.0 if signal.get("direction") == "buy" else -1.0
        confidence = finite_float(signal.get("signal_confidence")) or 0.5
        weight = max(0.0, abs(confidence - 0.5) * 2.0)
        state[base]["votes"] += side
        state[quote]["votes"] -= side
        state[base]["confidence_weighted"] += side * weight
        state[quote]["confidence_weighted"] -= side * weight
    return [
        {
            "currency": currency,
            "votes": round(values["votes"], 6),
            "confidence_weighted": round(
                values["confidence_weighted"], 6
            ),
        }
        for currency, values in sorted(
            state.items(),
            key=lambda item: abs(item[1]["confidence_weighted"]),
            reverse=True,
        )
    ]


def account_summary(payload: dict[str, Any]) -> dict[str, Any]:
    accounts = payload.get("accounts") or []
    account = next(
        (
            row
            for row in accounts
            if str(row.get("account_id") or "").endswith("-007")
        ),
        accounts[0] if accounts else {},
    )
    return {
        "snapshot_utc": payload.get("time"),
        "ok": bool(account.get("ok")),
        "account_id_suffix": "-007" if account else None,
        "balance": finite_float(account.get("balance")),
        "nav": finite_float(account.get("NAV")),
        "realized_pl": finite_float(account.get("pl")),
        "unrealized_pl": finite_float(account.get("unrealizedPL")),
        "margin_used": finite_float(account.get("marginUsed")),
        "open_trade_count": int(account.get("openTradeCount") or 0),
        "pending_order_count": int(account.get("pendingOrderCount") or 0),
        "trades": [
            {
                "instrument": row.get("instrument"),
                "current_units": finite_float(row.get("currentUnits")),
                "unrealized_pl": finite_float(row.get("unrealizedPL")),
                "price": finite_float(row.get("price")),
            }
            for row in (account.get("trades") or [])
        ],
    }


def capture(signal_path: Path, account_path: Path) -> dict[str, Any]:
    payload = read_json(signal_path)
    top_signals = list(payload.get("top_signals") or [])
    compact = [compact_signal(row) for row in top_signals]
    blockers = Counter(
        reason
        for row in top_signals
        for reason in (row.get("signal_blocked_by") or [])
    )
    families = Counter(
        family
        for row in top_signals
        for family in (row.get("component_families") or [])
    )
    feed = payload.get("contribution_feed") or {}
    return {
        "captured_utc": utc_now(),
        "signal_snapshot_utc": payload.get("updated_at"),
        "producer": payload.get("producer"),
        "selection_mode": payload.get("selection_mode"),
        "raw_candidate_count": int(payload.get("raw_candidate_count") or 0),
        "feed_candidate_count": int(payload.get("feed_candidate_count") or 0),
        "consolidated_signal_count": int(
            payload.get("consolidated_signal_count") or 0
        ),
        "qualified_signal_count": int(
            payload.get("qualified_signal_count") or 0
        ),
        "selected": payload.get("selected"),
        "feed_coverage": {
            "fresh_contributors": int(feed.get("fresh_contributors") or 0),
            "registered_contributors": int(
                feed.get("registered_contributors") or 0
            ),
            "expected_contributors": int(feed.get("expected_contributors") or 0),
            "account_eligible_contributors": int(
                feed.get("account_eligible_contributors") or 0
            ),
            "active_sources": list(feed.get("active_feed_sources") or []),
            "model_gap": dict(feed.get("model_gap") or {}),
        },
        "blocker_counts": dict(blockers.most_common()),
        "component_family_counts": dict(families.most_common()),
        "currency_bias": currency_bias(top_signals),
        "top_signals": compact,
        "account": account_summary(read_json(account_path)),
        "read_only": True,
        "oanda_api_called": False,
        "orders_supported": False,
    }


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only sampler for practice account -007 signal snapshots."
    )
    parser.add_argument("--signal-snapshot", type=Path, required=True)
    parser.add_argument("--account-snapshot", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--duration-sec", type=int, default=7200)
    parser.add_argument("--interval-sec", type=float, default=60.0)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    samples_path = args.output_dir / "PRACTICE_007_MONITOR_SAMPLES.jsonl"
    latest_path = args.output_dir / "PRACTICE_007_MONITOR_LATEST.json"
    state_path = args.output_dir / "PRACTICE_007_MONITOR_STATE.json"
    started = time.monotonic()
    sample_count = 0
    last_signal_timestamp: str | None = None

    while True:
        snapshot = capture(args.signal_snapshot, args.account_snapshot)
        with samples_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(snapshot, sort_keys=True) + "\n")
        atomic_json(latest_path, snapshot)
        sample_count += 1
        last_signal_timestamp = snapshot.get("signal_snapshot_utc")
        elapsed = time.monotonic() - started
        atomic_json(
            state_path,
            {
                "status": (
                    "complete" if elapsed >= args.duration_sec else "running"
                ),
                "started_utc": datetime.fromtimestamp(
                    time.time() - elapsed, tz=timezone.utc
                ).isoformat(),
                "updated_utc": utc_now(),
                "elapsed_sec": round(elapsed, 3),
                "duration_sec": args.duration_sec,
                "interval_sec": args.interval_sec,
                "sample_count": sample_count,
                "last_signal_snapshot_utc": last_signal_timestamp,
                "read_only": True,
                "oanda_api_called": False,
                "orders_supported": False,
            },
        )
        if elapsed >= args.duration_sec:
            break
        time.sleep(min(args.interval_sec, args.duration_sec - elapsed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
