#!/usr/bin/env python3
"""Audit minimum official economic-feed readiness for all priced currencies.

This audit deliberately distinguishes a configured URL from a usable feed.
Minimum readiness requires an operational direct policy publisher, an
operational direct statistical-release publisher, a frozen numeric parser,
and a future event clock.  Consensus, rates and observed actuals are reported
as later evidence layers; they do not let a listing URL self-certify.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_CONFIG = ROOT / "config" / "news_sources_v1.json"
DEFAULT_RUNTIME = DATA / "local_news_sentiment" / "source_coverage_latest.json"
DEFAULT_ECONOMIC_DEPTH = (
    DATA
    / "reports"
    / "currency_event_technical_coverage"
    / "CURRENCY_EVENT_TECHNICAL_COVERAGE_CURRENT.json"
)
DEFAULT_ALFRED_STATE = DATA / "state" / "alfred_vintage_prospective_v1.json"
DEFAULT_EXPECTATION_STATE = DATA / "state" / "internal_macro_expectation_v1.json"
DEFAULT_CONSENSUS_ACCESS = DATA / "state" / "macro_consensus_access_audit_v1.json"
CURRENT_DIRECT_REPLAY = (
    DATA
    / "reports"
    / "direct_source_response"
    / "DIRECT_SOURCE_HISTORICAL_REPLAY_CURRENT.json"
)
LEGACY_DIRECT_REPLAY = (
    DATA
    / "reports"
    / "direct_source_response"
    / "DIRECT_SOURCE_HISTORICAL_REPLAY_20260816.json"
)
DEFAULT_DIRECT_REPLAY = (
    CURRENT_DIRECT_REPLAY if CURRENT_DIRECT_REPLAY.exists() else LEGACY_DIRECT_REPLAY
)
REPORT_ROOT = DATA / "reports" / "economic_feed_completeness"
DEFAULT_JSON = REPORT_ROOT / "ECONOMIC_FEED_COMPLETENESS_CURRENT.json"
DEFAULT_MD = REPORT_ROOT / "ECONOMIC_FEED_COMPLETENESS_CURRENT.md"
SCHEMA_VERSION = "economic_feed_completeness_v1"


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    return dict(value) if isinstance(value, Mapping) else {}


def configured_sources(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("sources") or []
    if isinstance(rows, Mapping):
        rows = rows.values()
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def role(source: Mapping[str, Any]) -> str:
    return str(source.get("source_role") or "").strip().lower()


def is_enabled(source: Mapping[str, Any]) -> bool:
    return bool(source.get("enabled", True) and source.get("runtime_supported", True))


def is_direct(source: Mapping[str, Any]) -> bool:
    direct = source.get("direct")
    if direct is None:
        # Legacy first-party definitions predate the explicit ``direct``
        # field.  They are direct only when verified and their configured
        # transport is not an aggregator/search/vendor route.
        retrieval = str(source.get("retrieval_via") or "").lower()
        kind = str(source.get("kind") or "").lower()
        url = str(source.get("url") or "").lower()
        direct = bool(
            source.get("verified")
            and "google_news" not in retrieval
            and "news.google." not in url
            and kind not in {"gdelt", "economic_calendar"}
        )
    return bool(source.get("verified") and direct and is_enabled(source))


def source_currencies(source: Mapping[str, Any]) -> set[str]:
    return {str(value).upper() for value in source.get("currencies") or [] if value}


def runtime_sources(payload: Mapping[str, Any], currency: str) -> dict[str, dict[str, Any]]:
    currencies = payload.get("currencies") or {}
    row = currencies.get(currency) if isinstance(currencies, Mapping) else None
    sources = row.get("sources") if isinstance(row, Mapping) else []
    return {
        str(source.get("source_id") or ""): dict(source)
        for source in sources or []
        if isinstance(source, Mapping) and source.get("source_id")
    }


def source_operational(source: Mapping[str, Any], runtime: Mapping[str, Any]) -> bool:
    row = runtime.get(str(source.get("source_id") or ""))
    return bool(
        row
        and row.get("operational")
        and (row.get("healthy") or row.get("usable_recent_success"))
    )


def source_degraded_recent(source: Mapping[str, Any], runtime: Mapping[str, Any]) -> bool:
    row = runtime.get(str(source.get("source_id") or ""))
    return bool(row and row.get("operational") and row.get("usable_recent_success"))


def economic_depth_rows(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("currency") or "").upper(): dict(row)
        for row in payload.get("currencies") or []
        if isinstance(row, Mapping) and row.get("currency")
    }


def build_currency_rows(
    sources: Sequence[Mapping[str, Any]],
    runtime_payload: Mapping[str, Any],
    depth_payload: Mapping[str, Any],
    alfred_payload: Mapping[str, Any] | None = None,
    expectation_payload: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    depth = economic_depth_rows(depth_payload)
    alfred_coverage = (alfred_payload or {}).get("coverage") or {}
    alfred_dates = alfred_coverage.get("latest_observation_date_by_currency") or {}
    alfred_ages = alfred_coverage.get("latest_observation_age_days_by_currency") or {}
    expectation_by_currency = {
        str(row.get("currency") or "").upper(): dict(row)
        for row in ((expectation_payload or {}).get("summary") or {}).get("currencies") or []
        if isinstance(row, Mapping) and row.get("currency")
    }
    currencies = sorted(
        set(depth)
        or {
            currency
            for source in sources
            for currency in source_currencies(source)
        }
    )
    result: list[dict[str, Any]] = []
    for currency in currencies:
        configured = [
            source
            for source in sources
            if currency in source_currencies(source) and is_direct(source)
        ]
        runtime = runtime_sources(runtime_payload, currency)
        evidence = depth.get(currency) or {}
        expectation = expectation_by_currency.get(currency) or {}
        vintage_age = alfred_ages.get(currency)
        observed_policy_ids = {
            str(value) for value in evidence.get("official_policy_sources") or []
        }
        policy = [
            source
            for source in configured
            if (
                "calendar" not in role(source)
                and (
                    "policy" in role(source)
                    or str(source.get("source_id")) in observed_policy_ids
                )
            )
        ]
        macro = [
            source
            for source in configured
            if (
                "calendar" not in role(source)
                and "statistical" in role(source)
            )
        ]
        macro_calendars = [
            source
            for source in configured
            if "statistical" in role(source) and "calendar" in role(source)
        ]
        policy_live = [source for source in policy if source_operational(source, runtime)]
        macro_live = [source for source in macro if source_operational(source, runtime)]
        calendar_live = [
            source for source in macro_calendars if source_operational(source, runtime)
        ]
        numeric_parser = bool(evidence.get("structured_numeric_parser"))
        degraded_policy = [
            source for source in policy_live if source_degraded_recent(source, runtime)
        ]
        degraded_macro = [
            source for source in macro_live if source_degraded_recent(source, runtime)
        ]
        future_clock = bool(evidence.get("future_relevant_events"))
        gaps: list[str] = []
        if not policy_live:
            gaps.append("no_live_direct_policy_release")
        if not macro_live:
            gaps.append("no_live_direct_statistical_release")
        if not numeric_parser:
            gaps.append("no_frozen_structured_numeric_parser")
        if not future_clock:
            gaps.append("no_future_economic_event_clock")
        minimum_ready = not gaps
        actual_count = int(evidence.get("actual_value_observations") or 0)
        prospective_actual_count = int(
            evidence.get("prospective_actual_value_observations") or 0
        )
        consensus_count = int(evidence.get("causal_pre_release_consensus") or 0)
        expectation_count = int(expectation.get("forecast_count") or 0)
        research_gaps = list(gaps)
        if not actual_count:
            research_gaps.append("no_retained_numeric_actual")
        if not prospective_actual_count:
            research_gaps.append("no_prospectively_observed_numeric_actual")
        if not consensus_count:
            research_gaps.append("no_causal_pre_release_consensus")
        if vintage_age is None:
            research_gaps.append("no_historical_vintage_context")
        elif int(vintage_age) > 120:
            research_gaps.append("historical_vintage_stale_over_120d")
        if not expectation_count:
            research_gaps.append("no_internal_pre_release_baseline")
        result.append(
            {
                "currency": currency,
                "pair_leg_count": int(evidence.get("pair_leg_count") or 0),
                "configured_policy_sources": sorted(
                    str(source.get("source_id")) for source in policy
                ),
                "live_policy_sources": sorted(
                    str(source.get("source_id")) for source in policy_live
                ),
                "degraded_recent_policy_sources": sorted(
                    str(source.get("source_id")) for source in degraded_policy
                ),
                "configured_macro_sources": sorted(
                    str(source.get("source_id")) for source in macro
                ),
                "live_macro_sources": sorted(
                    str(source.get("source_id")) for source in macro_live
                ),
                "degraded_recent_macro_sources": sorted(
                    str(source.get("source_id")) for source in degraded_macro
                ),
                "configured_macro_calendars": sorted(
                    str(source.get("source_id")) for source in macro_calendars
                ),
                "live_macro_calendars": sorted(
                    str(source.get("source_id")) for source in calendar_live
                ),
                "structured_numeric_parser": numeric_parser,
                "actual_value_observations": actual_count,
                "prospective_actual_value_observations": prospective_actual_count,
                "future_event_clock": future_clock,
                "causal_pre_release_consensus": consensus_count,
                "daily_rate_context": bool(evidence.get("daily_rate_context")),
                "historical_vintage_context": currency in alfred_dates,
                "historical_vintage_latest_date": str(alfred_dates.get(currency) or ""),
                "historical_vintage_age_days": (
                    int(vintage_age) if vintage_age is not None else None
                ),
                "historical_vintage_current_120d": bool(
                    vintage_age is not None and int(vintage_age) <= 120
                ),
                "internal_expectation_forecasts": expectation_count,
                "minimum_feed_ready": minimum_ready,
                "prospective_surprise_ready": bool(
                    minimum_ready
                    and evidence.get("prospective_actual_value_observations")
                    and evidence.get("causal_pre_release_consensus")
                ),
                "gaps": gaps,
                "research_gaps": research_gaps,
            }
        )
    return result


def build_report(
    config_payload: Mapping[str, Any],
    runtime_payload: Mapping[str, Any],
    depth_payload: Mapping[str, Any],
    alfred_payload: Mapping[str, Any] | None = None,
    expectation_payload: Mapping[str, Any] | None = None,
    consensus_access_payload: Mapping[str, Any] | None = None,
    direct_replay_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rows = build_currency_rows(
        configured_sources(config_payload), runtime_payload, depth_payload,
        alfred_payload, expectation_payload,
    )
    total_pair_legs = sum(int(row["pair_leg_count"]) for row in rows)
    ready_pair_legs = sum(
        int(row["pair_leg_count"])
        for row in rows
        if row["minimum_feed_ready"]
    )
    blocker_priority = sorted(
        (
            {
                "currency": row["currency"],
                "pair_leg_count": row["pair_leg_count"],
                "gaps": list(row["gaps"]),
            }
            for row in rows
            if not row["minimum_feed_ready"]
        ),
        key=lambda row: (-int(row["pair_leg_count"]), str(row["currency"])),
    )
    surprise_blocker_priority = sorted(
        (
            {
                "currency": row["currency"],
                "pair_leg_count": row["pair_leg_count"],
                "research_gaps": list(row["research_gaps"]),
            }
            for row in rows
            if not row["prospective_surprise_ready"]
        ),
        key=lambda row: (-int(row["pair_leg_count"]), str(row["currency"])),
    )
    currency_count = len(rows)
    percentage = lambda count: round(100.0 * count / currency_count, 3) if currency_count else 0.0
    replay_currencies = sorted(
        {
            str(value).upper()
            for value in (direct_replay_payload or {}).get("mapped_source_currencies") or []
            if str(value).strip()
        }
    )
    replay_pair_legs = sum(
        int(row["pair_leg_count"])
        for row in rows
        if row["currency"] in replay_currencies
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "research_only": True,
        "execution_eligible": False,
        "definition": {
            "minimum_feed_ready": [
                "healthy operational direct official policy release source, or bounded recent parsed success during a visible transport/server outage",
                "healthy operational direct official statistical release source, or bounded recent parsed success during a visible transport/server outage",
                "frozen structured numeric parser",
                "future relevant economic event clock",
            ],
            "prospective_surprise_ready": [
                "minimum_feed_ready",
                "at least one prospectively observed normalized actual",
                "causally captured pre-release consensus",
            ],
        },
        "currency_count": currency_count,
        "minimum_feed_ready_count": sum(
            bool(row["minimum_feed_ready"]) for row in rows
        ),
        "minimum_feed_degraded_recent_count": sum(
            bool(row["degraded_recent_policy_sources"] or row["degraded_recent_macro_sources"])
            for row in rows
            if row["minimum_feed_ready"]
        ),
        "minimum_feed_ready_pct": percentage(sum(bool(row["minimum_feed_ready"]) for row in rows)),
        "structured_numeric_parser_count": sum(
            bool(row["structured_numeric_parser"]) for row in rows
        ),
        "structured_numeric_parser_pct": percentage(sum(bool(row["structured_numeric_parser"]) for row in rows)),
        "future_event_clock_count": sum(
            bool(row["future_event_clock"]) for row in rows
        ),
        "future_event_clock_pct": percentage(sum(bool(row["future_event_clock"]) for row in rows)),
        "actual_observation_currency_count": sum(
            int(row["actual_value_observations"]) > 0 for row in rows
        ),
        "actual_observation_currency_pct": percentage(sum(int(row["actual_value_observations"]) > 0 for row in rows)),
        "prospective_actual_observation_currency_count": sum(
            int(row["prospective_actual_value_observations"]) > 0 for row in rows
        ),
        "prospective_actual_observation_currency_pct": percentage(
            sum(int(row["prospective_actual_value_observations"]) > 0 for row in rows)
        ),
        "historical_vintage_context_currency_count": sum(
            bool(row["historical_vintage_context"]) for row in rows
        ),
        "historical_vintage_current_120d_currency_count": sum(
            bool(row["historical_vintage_current_120d"]) for row in rows
        ),
        "internal_expectation_currency_count": sum(
            int(row["internal_expectation_forecasts"]) > 0 for row in rows
        ),
        "causal_consensus_currency_count": sum(
            int(row["causal_pre_release_consensus"]) > 0 for row in rows
        ),
        "causal_consensus_currency_pct": percentage(sum(int(row["causal_pre_release_consensus"]) > 0 for row in rows)),
        "total_pair_legs": total_pair_legs,
        "minimum_feed_ready_pair_legs": ready_pair_legs,
        "minimum_feed_ready_pair_leg_pct": (
            round(100.0 * ready_pair_legs / total_pair_legs, 3)
            if total_pair_legs
            else 0.0
        ),
        "prospective_surprise_ready_count": sum(
            bool(row["prospective_surprise_ready"]) for row in rows
        ),
        "prospective_surprise_ready_pct": percentage(sum(bool(row["prospective_surprise_ready"]) for row in rows)),
        "direct_historical_replay": {
            "timestamp_valid_currency_count": len(replay_currencies),
            "timestamp_valid_currency_pct": percentage(len(replay_currencies)),
            "timestamp_valid_pair_leg_count": replay_pair_legs,
            "timestamp_valid_pair_leg_pct": (
                round(100.0 * replay_pair_legs / total_pair_legs, 3)
                if total_pair_legs else 0.0
            ),
            "currencies": replay_currencies,
            "source_event_count": int(
                (direct_replay_payload or {}).get("source_event_count") or 0
            ),
            "independent_source_time_count": int(
                (direct_replay_payload or {}).get("independent_source_time_count") or 0
            ),
            "excluded_source_event_count": len(
                (direct_replay_payload or {}).get("excluded_source_events") or []
            ),
            "evidence_class": str(
                (direct_replay_payload or {}).get("evidence_class") or "unavailable"
            ),
        },
        "consensus_provider_access": {
            "status": str((consensus_access_payload or {}).get("status") or "not_audited"),
            "accessible": bool(
                (consensus_access_payload or {}).get(
                    "causal_consensus_provider_accessible"
                )
            ),
            "unlock_requirement": str(
                (consensus_access_payload or {}).get("unlock_requirement") or ""
            ),
            "providers": list((consensus_access_payload or {}).get("providers") or []),
        },
        "blocker_priority": blocker_priority,
        "surprise_blocker_priority": surprise_blocker_priority,
        "currencies": rows,
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Economic Feed Completeness",
        "",
        f"Generated: `{report['generated_utc']}`",
        "",
        "Research-only. A configured URL is not counted as a working feed.",
        "",
        f"- Currencies: **{report['currency_count']}**",
        f"- Minimum feed ready: **{report['minimum_feed_ready_count']}/{report['currency_count']} ({report['minimum_feed_ready_pct']:.1f}%)**",
        f"- Ready through bounded recent-success grace (still visibly degraded): **{report['minimum_feed_degraded_recent_count']}**",
        f"- Frozen numeric parsers: **{report['structured_numeric_parser_count']}/{report['currency_count']} ({report['structured_numeric_parser_pct']:.1f}%)**",
        f"- Future economic clocks: **{report['future_event_clock_count']}/{report['currency_count']} ({report['future_event_clock_pct']:.1f}%)**",
        f"- Currencies with retained actuals: **{report['actual_observation_currency_count']}/{report['currency_count']} ({report['actual_observation_currency_pct']:.1f}%)**",
        f"- Currencies with prospectively observed actuals: **{report['prospective_actual_observation_currency_count']}/{report['currency_count']} ({report['prospective_actual_observation_currency_pct']:.1f}%)**",
        f"- Historical vintage context: **{report['historical_vintage_context_currency_count']}** (current within 120 days: **{report['historical_vintage_current_120d_currency_count']}**)",
        f"- Internal expectation baselines: **{report['internal_expectation_currency_count']}** (not market consensus)",
        f"- Currencies with causal consensus: **{report['causal_consensus_currency_count']}/{report['currency_count']} ({report['causal_consensus_currency_pct']:.1f}%)**",
        f"- Consensus provider access: **{report['consensus_provider_access']['status']}**",
        "- Ready pair legs: **{}/{} ({:.1f}%)**".format(
            report["minimum_feed_ready_pair_legs"],
            report["total_pair_legs"],
            report["minimum_feed_ready_pair_leg_pct"],
        ),
        f"- Prospective surprise ready: **{report['prospective_surprise_ready_count']}/{report['currency_count']} ({report['prospective_surprise_ready_pct']:.1f}%)**",
        "- Timestamp-valid direct historical replay: **{}/{} ({:.1f}%)** currencies and **{}/{} ({:.1f}%)** pair legs; **{}** source events / **{}** independent currency-factor clocks; **{}** excluded events".format(
            report["direct_historical_replay"]["timestamp_valid_currency_count"],
            report["currency_count"],
            report["direct_historical_replay"]["timestamp_valid_currency_pct"],
            report["direct_historical_replay"]["timestamp_valid_pair_leg_count"],
            report["total_pair_legs"],
            report["direct_historical_replay"]["timestamp_valid_pair_leg_pct"],
            report["direct_historical_replay"]["source_event_count"],
            report["direct_historical_replay"]["independent_source_time_count"],
            report["direct_historical_replay"]["excluded_source_event_count"],
        ),
        "",
        "| Currency | Pair legs | Policy live | Macro live | Numeric parser | Retained actuals | Prospective actuals | Vintage / age days | Internal expectations | Future clock | Causal consensus | Minimum ready | Minimum gaps | Research gaps |",
        "|---|---:|---:|---:|---|---:|---:|---|---:|---|---:|---|---|---|",
    ]
    for row in report["currencies"]:
        lines.append(
            "| {currency} | {pair_leg_count} | {policy} | {macro} | {parser} | {actuals} | {prospective_actuals} | {vintage} | {expectations} | {clock} | {consensus} | {ready} | {gaps} | {research_gaps} |".format(
                currency=row["currency"],
                pair_leg_count=row["pair_leg_count"],
                policy=len(row["live_policy_sources"]),
                macro=len(row["live_macro_sources"]),
                parser="yes" if row["structured_numeric_parser"] else "no",
                actuals=row["actual_value_observations"],
                prospective_actuals=row["prospective_actual_value_observations"],
                vintage=(
                    "no"
                    if not row["historical_vintage_context"]
                    else "{} / {}".format(
                        row["historical_vintage_latest_date"],
                        row["historical_vintage_age_days"],
                    )
                ),
                expectations=row["internal_expectation_forecasts"],
                clock="yes" if row["future_event_clock"] else "no",
                consensus=row["causal_pre_release_consensus"],
                ready="yes" if row["minimum_feed_ready"] else "no",
                gaps=", ".join(row["gaps"]) or "none",
                research_gaps=", ".join(row["research_gaps"]) or "none",
            )
        )
    lines.extend(
        [
            "",
            "## Blocker priority by pair-leg impact",
            "",
        ]
    )
    for row in report["blocker_priority"]:
        lines.append(
            "- **{}** ({} pair legs): {}".format(
                row["currency"],
                row["pair_leg_count"],
                ", ".join(row["gaps"]),
            )
        )
    lines.extend(
        [
            "",
            "## Surprise-research blockers by pair-leg impact",
            "",
        ]
    )
    for row in report["surprise_blocker_priority"]:
        lines.append(
            "- **{}** ({} pair legs): {}".format(
                row["currency"],
                row["pair_leg_count"],
                ", ".join(row["research_gaps"]),
            )
        )
    lines.extend(
        [
            "",
            "## Consensus provider access",
            "",
            report["consensus_provider_access"].get("unlock_requirement")
            or "No access audit has been recorded.",
            "",
            "| Provider | Credential | Role | Access state |",
            "|---|---|---|---|",
        ]
    )
    for row in report["consensus_provider_access"].get("providers") or []:
        lines.append(
            f"| {row.get('provider')} | {'present' if row.get('credential_present') else 'missing'} | "
            f"{row.get('role')} | {row.get('access_state')} |"
        )
    lines.extend(
        [
            "",
            "Feed readiness does not imply predictive edge, promotion eligibility, or permission to trade.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--runtime", type=Path, default=DEFAULT_RUNTIME)
    parser.add_argument("--economic-depth", type=Path, default=DEFAULT_ECONOMIC_DEPTH)
    parser.add_argument("--alfred-state", type=Path, default=DEFAULT_ALFRED_STATE)
    parser.add_argument(
        "--expectation-state", type=Path, default=DEFAULT_EXPECTATION_STATE
    )
    parser.add_argument(
        "--consensus-access", type=Path, default=DEFAULT_CONSENSUS_ACCESS
    )
    parser.add_argument("--direct-replay", type=Path, default=DEFAULT_DIRECT_REPLAY)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_MD)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(
        read_json(args.config), read_json(args.runtime), read_json(args.economic_depth),
        read_json(args.alfred_state), read_json(args.expectation_state),
        read_json(args.consensus_access), read_json(args.direct_replay),
    )
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    args.output_md.write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps({
        "currency_count": report["currency_count"],
        "minimum_feed_ready_count": report["minimum_feed_ready_count"],
        "prospective_surprise_ready_count": report["prospective_surprise_ready_count"],
        "report": str(args.output_md),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
