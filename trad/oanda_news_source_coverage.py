#!/usr/bin/env python3
"""Audit source provenance for every currency leg and configured FX pair."""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_local_news_sentiment as news
import oanda_news_event_tagger as event_tagger


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "news_sources_v1.json"
DEFAULT_STATE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "collector_state_v1.json"
)
DEFAULT_OUTPUT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "source_coverage_latest.json"
)
SCHEMA_VERSION = "fx_news_source_coverage_v1"


def source_recent_success_state(
    source: Mapping[str, Any],
    state: Mapping[str, Any],
    *,
    as_of: dt.datetime,
) -> tuple[bool, float | None]:
    """Return whether a parsed success remains usable during a short outage.

    This is deliberately narrower than source health.  It only preserves
    minimum feed *capability* for a bounded window after a transport/server
    error when the same frozen parser previously emitted at least one item.
    Parse errors never receive grace, and the outage remains visible as
    degraded rather than healthy.
    """

    success = news.parse_datetime(state.get("last_success_utc"))
    if success is None:
        return False, None
    age_sec = max(0.0, (as_of - success).total_seconds())
    try:
        parsed_items = int(state.get("parsed_items") or 0)
        consecutive_errors = int(state.get("consecutive_errors") or 0)
        status = int(state.get("last_status") or 0)
    except (TypeError, ValueError):
        return False, age_sec
    error = str(state.get("last_error") or "").strip().lower()
    if parsed_items <= 0 or not error or consecutive_errors <= 0:
        return False, age_sec
    if any(token in error for token in ("parse_error", "decode", "malformed", "schema")):
        return False, age_sec
    if status not in {403, 408, 409, 425, 429, 500, 502, 503, 504}:
        return False, age_sec
    interval = max(60.0, news.safe_float(source.get("poll_interval_sec"), 180.0))
    grace_sec = max(1800.0, interval * 4.0)
    return age_sec <= grace_sec, age_sec


def source_is_healthy(
    source: Mapping[str, Any],
    state: Mapping[str, Any],
    *,
    as_of: dt.datetime,
) -> bool:
    # A successful HTTP response is not sufficient evidence that the source is
    # usable.  Parsers historically recorded ``last_success_utc`` before a
    # downstream decode failure, so fail closed whenever the latest source
    # state still carries an error or a consecutive-error count.
    if str(state.get("last_error") or "").strip():
        return False
    try:
        consecutive_errors = int(state.get("consecutive_errors") or 0)
    except (TypeError, ValueError):
        return False
    if consecutive_errors > 0:
        return False
    success = news.parse_datetime(state.get("last_success_utc"))
    if success is None or int(state.get("last_status") or 0) not in {200, 304}:
        return False
    interval = max(
        60.0,
        news.safe_float(
            source.get("poll_interval_sec"),
            21600.0
            if str(source.get("kind") or "").lower() == "gdelt"
            else 180.0,
        ),
    )
    return (as_of - success).total_seconds() <= max(1800.0, interval * 4.0)


def build_coverage(
    *,
    config: Mapping[str, Any],
    state: Mapping[str, Any],
    instruments: Sequence[str],
    as_of: dt.datetime,
) -> dict[str, Any]:
    currencies = sorted(
        {
            currency
            for instrument in instruments
            for currency in news.split_instrument(instrument)
            if currency
        }
    )
    source_states = state.get("sources")
    if not isinstance(source_states, Mapping):
        source_states = {}
    currency_rows: dict[str, Any] = {}
    for currency in currencies:
        configured: list[dict[str, Any]] = []
        for source in config.get("sources") or ():
            if not isinstance(source, Mapping) or currency not in (
                source.get("currencies") or ()
            ):
                continue
            source_id = str(source.get("source_id") or "")
            status = source_states.get(source_id)
            if not isinstance(status, Mapping):
                status = {}
            runtime_status = news.source_runtime_status(source)
            operational = runtime_status in {"enabled", "external_adapter"}
            healthy = operational and source_is_healthy(
                source,
                status,
                as_of=as_of,
            )
            recent_usable, recent_success_age_sec = source_recent_success_state(
                source,
                status,
                as_of=as_of,
            )
            recent_usable = operational and not healthy and recent_usable
            configured.append(
                {
                    "source_id": source_id,
                    "name": source.get("name"),
                    "kind": source.get("kind"),
                    "direct": bool(source.get("direct", True)),
                    "retrieval_via": source.get("retrieval_via") or "direct",
                    "configured_verified": bool(source.get("verified")),
                    "source_role": source.get("source_role")
                    or news.source_role(source),
                    "runtime_status": runtime_status,
                    "operational": operational,
                    "healthy": healthy,
                    "usable_recent_success": recent_usable,
                    "health_state": (
                        "healthy"
                        if healthy
                        else "degraded_recent_success"
                        if recent_usable
                        else "degraded"
                        if operational
                        else "inactive"
                    ),
                    "recent_success_age_sec": (
                        round(recent_success_age_sec, 3)
                        if recent_success_age_sec is not None
                        else None
                    ),
                    "last_success_utc": status.get("last_success_utc"),
                    "last_status": status.get("last_status"),
                    "last_error": status.get("last_error") or "",
                }
            )
        operational_rows = [row for row in configured if row["operational"]]
        live_direct = [row for row in configured if row["direct"] and row["healthy"]]
        degraded_recent_direct = [
            row
            for row in configured
            if row["direct"] and row["usable_recent_success"]
        ]
        live_proxy = [
            row for row in configured if not row["direct"] and row["healthy"]
        ]
        tier = (
            "LIVE_DIRECT"
            if live_direct
            else "DEGRADED_RECENT_DIRECT"
            if degraded_recent_direct
            else "LIVE_OFFICIAL_PUBLISHER_PROXY"
            if live_proxy
            else "CONFIGURED_NOT_HEALTHY"
            if operational_rows
            else "CONFIGURED_INACTIVE"
            if configured
            else "NO_SOURCE"
        )
        currency_rows[currency] = {
            "currency": currency,
            "coverage_tier": tier,
            "configured_source_count": len(configured),
            "operational_source_count": len(operational_rows),
            "healthy_direct_source_count": len(live_direct),
            "degraded_recent_direct_source_count": len(degraded_recent_direct),
            "healthy_proxy_source_count": len(live_proxy),
            "sources": configured,
        }

    pair_rows: dict[str, Any] = {}
    pair_tier_counts: dict[str, int] = {}
    for instrument in instruments:
        base, quote = news.split_instrument(instrument)
        base_tier = str(currency_rows.get(base, {}).get("coverage_tier") or "NO_SOURCE")
        quote_tier = str(currency_rows.get(quote, {}).get("coverage_tier") or "NO_SOURCE")
        if base_tier == quote_tier == "LIVE_DIRECT":
            tier = "TWO_LEG_LIVE_DIRECT"
        elif base_tier.startswith("LIVE_") and quote_tier.startswith("LIVE_"):
            tier = "TWO_LEG_LIVE_MIXED"
        elif base_tier.startswith("LIVE_") or quote_tier.startswith("LIVE_"):
            tier = "ONE_LEG_LIVE"
        else:
            tier = "NO_LIVE_LEG"
        pair_tier_counts[tier] = pair_tier_counts.get(tier, 0) + 1
        pair_rows[instrument] = {
            "instrument": instrument,
            "coverage_tier": tier,
            "base": {"currency": base, "source_tier": base_tier},
            "quote": {"currency": quote, "source_tier": quote_tier},
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": news.iso_utc(),
        "as_of_utc": news.iso_utc(as_of),
        "policy": {
            "research_only": True,
            "execution_eligible": False,
            "matrix_weight": 0.0,
        },
        "currency_count": len(currency_rows),
        "pair_count": len(pair_rows),
        "all_currencies_configured": all(
            row["configured_source_count"] > 0 for row in currency_rows.values()
        ),
        "all_pairs_emitted": len(pair_rows) == len(instruments),
        "pair_coverage_tier_counts": dict(sorted(pair_tier_counts.items())),
        "currencies": currency_rows,
        "pairs": pair_rows,
    }


def run(
    *,
    config_path: Path = DEFAULT_CONFIG,
    state_path: Path = DEFAULT_STATE,
    output_path: Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    config = news.load_json(config_path, {})
    state = news.load_json(state_path, {})
    report = build_coverage(
        config=config if isinstance(config, Mapping) else {},
        state=state if isinstance(state, Mapping) else {},
        instruments=event_tagger.discover_instruments(),
        as_of=news.utc_now(),
    )
    news.atomic_write_json(output_path, report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(
        json.dumps(
            run(
                config_path=args.config,
                state_path=args.state,
                output_path=args.output,
            ),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
