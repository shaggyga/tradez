#!/usr/bin/env python3
"""Audit exact domestic release-clock gaps without creating a trade signal.

The report deliberately separates domestic statistical clocks, domestic
policy clocks, date windows, and linked ECB/FOMC dependencies.  It is a
read-only research diagnostic: an exact clock carries no actual, consensus,
surprise, direction, confirmation, promotion, authorization, or execution.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_local_news_sentiment as news


ROOT = Path(__file__).resolve().parent
REGISTRY = ROOT / "config" / "official_exact_event_clock_gap_registry_v1.json"
SOURCE_CONFIG = ROOT / "config" / "news_sources_v1.json"
POLICY_DEPENDENCY_REGISTRY = (
    ROOT / "config" / "currency_policy_dependency_registry_v1.json"
)
LINKED_POLICY_REGISTRY = ROOT / "config" / "linked_currency_policy_drivers_v1.json"
REPORT_ROOT = (
    ROOT / "data" / "oanda_training_manager" / "reports"
    / "official_exact_event_clock_gap"
)
DEFAULT_JSON = REPORT_ROOT / "OFFICIAL_EXACT_EVENT_CLOCK_GAP_CURRENT.json"
DEFAULT_MD = REPORT_ROOT / "OFFICIAL_EXACT_EVENT_CLOCK_GAP_CURRENT.md"


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _is_direction_free(row: Mapping[str, Any]) -> bool:
    return bool(
        row.get("actual") is None
        and row.get("actual_value") is None
        and row.get("consensus") is None
        and row.get("consensus_value") is None
        and row.get("direction") is None
        and not row.get("currency_scores")
        and row.get("directional_research_only") is True
        and row.get("research_only") is True
        and row.get("execution_eligible") is False
        and row.get("can_place_orders") is False
        and row.get("independent_domestic_event") is True
        and row.get("linked_policy_factor") is False
        and str(row.get("timing_precision") or "") == "minute"
    )


def build_report(
    registry: Mapping[str, Any],
    source_observations: Mapping[str, Mapping[str, Any]],
    *,
    as_of: dt.datetime,
    policy_dependency_registry: Mapping[str, Any] | None = None,
    linked_policy_registry: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    as_of = as_of.astimezone(dt.timezone.utc)
    policy_dependency_registry = policy_dependency_registry or {}
    linked_policy_registry = linked_policy_registry or {}
    dependency_by_currency = {
        str(row.get("dependent_currency") or "").upper(): row
        for row in policy_dependency_registry.get("dependencies") or []
        if isinstance(row, Mapping) and row.get("dependent_currency")
    }
    linked_by_currency = {
        str(row.get("currency") or "").upper(): row
        for row in linked_policy_registry.get("relationships") or []
        if isinstance(row, Mapping) and row.get("currency")
    }
    currency_rows: list[dict[str, Any]] = []
    exact_pairs: set[str] = set()
    scoped_pairs: set[str] = set()
    for configured in registry.get("currencies") or []:
        if not isinstance(configured, Mapping):
            continue
        currency = str(configured.get("currency") or "").upper()
        priced_pairs = sorted(str(value) for value in configured.get("priced_pairs") or [])
        scoped_pairs.update(priced_pairs)
        statistics = configured.get("domestic_statistics") or {}
        policy = configured.get("domestic_policy") or {}
        source_id = str(statistics.get("source_id") or "")
        observation = source_observations.get(source_id) or {}
        future_rows = []
        guard_violations = []
        for raw in observation.get("rows") or []:
            if not isinstance(raw, Mapping):
                continue
            scheduled = news.parse_datetime(raw.get("scheduled_utc"))
            if scheduled is None or scheduled < as_of:
                continue
            future_rows.append(dict(raw))
            if not _is_direction_free(raw):
                guard_violations.append(str(raw.get("external_id") or "unknown"))
        configured_exact = str(statistics.get("status") or "") == "exact_minute_available"
        linked = configured.get("linked_driver")
        linked_safety_violations: list[str] = []
        if isinstance(linked, Mapping):
            expected_driver = str(linked.get("driver_currency") or "").upper()
            dependency = dependency_by_currency.get(currency)
            relationship = linked_by_currency.get(currency)
            if dependency is None:
                linked_safety_violations.append("missing_policy_dependency_registry_row")
            else:
                if str(dependency.get("driver_currency") or "").upper() != expected_driver:
                    linked_safety_violations.append("policy_dependency_driver_mismatch")
                if dependency.get("assign_direction") is not False:
                    linked_safety_violations.append("policy_dependency_can_assign_direction")
                if str(dependency.get("timing_policy") or "") != "driver_clock_preflight_only":
                    linked_safety_violations.append("policy_dependency_timing_not_preflight_only")
            if relationship is None:
                linked_safety_violations.append("missing_linked_policy_registry_row")
            else:
                if str(relationship.get("driver_currency") or "").upper() != expected_driver:
                    linked_safety_violations.append("linked_policy_driver_mismatch")
                if relationship.get("can_assign_direction") is not False:
                    linked_safety_violations.append("linked_policy_can_assign_direction")
                if relationship.get("can_confirm") is not False:
                    linked_safety_violations.append("linked_policy_can_confirm")
        pagination_guard_violations: list[str] = []
        if source_id == "denmark_statistics_release_calendar_exact_v1":
            if int(observation.get("pagination_horizon_days") or 0) < 45:
                pagination_guard_violations.append("pagination_horizon_under_45_days")
            if str(observation.get("pagination_stop_reason") or "") != "horizon_covered":
                pagination_guard_violations.append("pagination_horizon_not_covered")
            if int(observation.get("pagination_pages_fetched") or 0) < 2:
                pagination_guard_violations.append("pagination_did_not_advance_beyond_page_1")
        guard_violations.extend(pagination_guard_violations)
        source_live_ready = bool(
            configured_exact
            and int(observation.get("http_status") or 0) == 200
            and future_rows
            and not guard_violations
        )
        if source_live_ready:
            exact_pairs.update(priced_pairs)
        currency_rows.append(
            {
                "currency": currency,
                "priced_pairs": priced_pairs,
                "domestic_statistics_status": str(statistics.get("status") or ""),
                "domestic_statistics_source_id": source_id,
                "domestic_statistics_scope": str(statistics.get("scope") or ""),
                "source_http_status": int(observation.get("http_status") or 0),
                "source_error": str(observation.get("error") or ""),
                "parsed_rows": int(observation.get("parsed_rows") or 0),
                "future_exact_rows": len(future_rows),
                "pagination_pages_fetched": int(
                    observation.get("pagination_pages_fetched") or 0
                ),
                "pagination_horizon_days": int(
                    observation.get("pagination_horizon_days") or 0
                ),
                "pagination_horizon_end_date": str(
                    observation.get("pagination_horizon_end_date") or ""
                ),
                "next_exact_events": [
                    {
                        "scheduled_utc": str(row.get("scheduled_utc") or ""),
                        "event_series_id": str(row.get("event_series_id") or ""),
                        "event_name": str(row.get("event_name") or ""),
                    }
                    for row in future_rows[:5]
                ],
                "direction_or_execution_guard_violations": guard_violations,
                "domestic_exact_statistics_clock_live_ready": source_live_ready,
                "domestic_policy_status": str(policy.get("status") or ""),
                "domestic_policy_timing_precision": str(
                    policy.get("timing_precision") or ""
                ),
                "linked_driver_currency": (
                    str(linked.get("driver_currency") or "")
                    if isinstance(linked, Mapping)
                    else ""
                ),
                "linked_driver_counted_as_domestic_or_independent": bool(
                    linked_safety_violations
                ),
                "linked_driver_safety_validated": not linked_safety_violations,
                "linked_driver_safety_violations": linked_safety_violations,
                "research_only": True,
                "execution_eligible": False,
            }
        )
    exact_currency_count = sum(
        bool(row["domestic_exact_statistics_clock_live_ready"])
        for row in currency_rows
    )
    all_source_guards_pass = not any(
        row["direction_or_execution_guard_violations"] for row in currency_rows
    )
    all_linked_guards_pass = not any(
        row["linked_driver_safety_violations"] for row in currency_rows
    )
    return {
        "schema_version": 1,
        "contract_id": "official_exact_event_clock_gap_audit_v3_huf_stats_policy_split_20260817",
        "registry_contract_id": str(registry.get("contract_id") or ""),
        "generated_utc": as_of.isoformat(),
        "research_only": True,
        "execution_eligible": False,
        "direction_policy": "abstain",
        "execution_policy_changed": False,
        "summary": {
            "target_currency_count": len(currency_rows),
            "baseline_exact_domestic_currency_count": 0,
            "live_exact_domestic_currency_count": exact_currency_count,
            "live_exact_domestic_currency_pct": round(
                100.0 * exact_currency_count / max(1, len(currency_rows)), 3
            ),
            "target_pair_count": len(scoped_pairs),
            "pairs_with_live_exact_domestic_clock": len(exact_pairs),
            "target_pair_coverage_pct": round(
                100.0 * len(exact_pairs) / max(1, len(scoped_pairs)), 3
            ),
            "huf_policy_exact_clock": False,
            "huf_policy_clock_state": "official_date_window_only",
            "huf_cpi_statistics_exact_clock": any(
                row.get("currency") == "HUF"
                and row.get("domestic_statistics_scope") == "headline_cpi_only"
                and row.get("domestic_exact_statistics_clock_live_ready")
                for row in currency_rows
            ),
            "source_abstention_and_pagination_guards_pass": all_source_guards_pass,
            "linked_driver_safety_pass": all_linked_guards_pass,
            "audit_guard_pass": all_source_guards_pass and all_linked_guards_pass,
        },
        "policy_dependency_contract_id": str(
            policy_dependency_registry.get("contract_id") or ""
        ),
        "linked_policy_contract_id": str(linked_policy_registry.get("contract_id") or ""),
        "currencies": currency_rows,
        "guards": list(registry.get("guards") or []),
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    summary = report.get("summary") or {}
    lines = [
        "# Official Exact Event Clock Gap Audit",
        "",
        f"Generated: `{report.get('generated_utc')}`",
        "",
        "This is a direction-free, research-only timing audit. Exact clocks do not imply a surprise, trade direction, confirmation, authorization, or execution.",
        "",
        "## Coverage change",
        "",
        f"- Exact domestic currencies in this three-currency gap set: **{summary.get('baseline_exact_domestic_currency_count')} -> {summary.get('live_exact_domestic_currency_count')}/{summary.get('target_currency_count')} ({summary.get('live_exact_domestic_currency_pct')}%)**.",
        f"- Targeted priced pairs with at least one new exact domestic clock: **{summary.get('pairs_with_live_exact_domestic_clock')}/{summary.get('target_pair_count')} ({summary.get('target_pair_coverage_pct')}%)**.",
        f"- Source abstention/pagination guards: **{'PASS' if summary.get('source_abstention_and_pagination_guards_pass') else 'FAIL'}**; linked-driver registry guards: **{'PASS' if summary.get('linked_driver_safety_pass') else 'FAIL'}**.",
        "- HUF/KSH headline CPI now has an exact, content-addressed public-release clock. This is one statistical series, not broad HUF source completeness.",
        "- HUF/MNB policy remains an official **date window**, not an exact decision minute. The documented 14:00 time is for later abridged minutes and was not relabeled.",
        "- ECB/FOMC clocks remain linked readiness dependencies for DKK/HKD and do not count as independent domestic events.",
        "",
        "## Currency detail",
        "",
        "| Currency | Domestic stats | Live exact | Future exact rows | Pages / horizon | Domestic policy | Linked driver safe | Guard violations |",
        "|---|---|---:|---:|---|---|---:|---:|",
    ]
    for row in report.get("currencies") or []:
        lines.append(
            "| {currency} | {stats} | {ready} | {future} | {pages} / {horizon}d | {policy} | {linked} | {violations} |".format(
                currency=row.get("currency"),
                stats=row.get("domestic_statistics_status") or "none",
                ready="yes" if row.get("domestic_exact_statistics_clock_live_ready") else "no",
                future=row.get("future_exact_rows"),
                pages=row.get("pagination_pages_fetched"),
                horizon=row.get("pagination_horizon_days"),
                policy=row.get("domestic_policy_status") or "none",
                linked=(
                    "yes"
                    if row.get("linked_driver_currency")
                    and row.get("linked_driver_safety_validated")
                    else "FAIL"
                    if row.get("linked_driver_currency")
                    else "n/a"
                ),
                violations=(
                    len(row.get("direction_or_execution_guard_violations") or [])
                    + len(row.get("linked_driver_safety_violations") or [])
                ),
            )
        )
    lines.extend(["", "## Next exact rows", ""])
    for row in report.get("currencies") or []:
        lines.append(f"### {row.get('currency')}")
        lines.append("")
        events = row.get("next_exact_events") or []
        if not events:
            lines.append("No verified prospective exact domestic row in the current snapshot.")
        else:
            for event in events:
                lines.append(
                    f"- `{event.get('scheduled_utc')}` - {event.get('event_name')} (`{event.get('event_series_id')}`)."
                )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def collect_source_observations(
    registry: Mapping[str, Any],
    source_config: Mapping[str, Any],
    *,
    as_of: dt.datetime,
    timeout_sec: float,
) -> dict[str, dict[str, Any]]:
    by_id = {
        str(row.get("source_id") or ""): row
        for row in source_config.get("sources") or []
        if isinstance(row, Mapping)
    }
    observations: dict[str, dict[str, Any]] = {}
    source_ids = {
        str((row.get("domestic_statistics") or {}).get("source_id") or "")
        for row in registry.get("currencies") or []
        if isinstance(row, Mapping)
        and str((row.get("domestic_statistics") or {}).get("status") or "")
        == "exact_minute_available"
    }
    for source_id in sorted(source_ids):
        source = by_id.get(source_id)
        if source is None:
            observations[source_id] = {
                "http_status": 0,
                "error": "source_missing_from_news_config",
                "parsed_rows": 0,
                "rows": [],
            }
            continue
        rows, state = news.fetch_source(
            source,
            {},
            timeout_sec=timeout_sec,
            maximum_bytes=10_000_000,
            now=as_of,
        )
        observations[source_id] = {
            "http_status": int(state.get("last_status") or 0),
            "error": str(state.get("last_error") or ""),
            "parsed_rows": len(rows),
            "rows": rows,
            "pagination_pages_fetched": int(
                state.get("pagination_pages_fetched") or 0
            ),
            "pagination_horizon_days": int(
                state.get("pagination_horizon_days") or 0
            ),
            "pagination_horizon_end_date": str(
                state.get("pagination_horizon_end_date") or ""
            ),
            "pagination_stop_reason": str(
                state.get("pagination_stop_reason") or ""
            ),
        }
    return observations


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--source-config", type=Path, default=SOURCE_CONFIG)
    parser.add_argument(
        "--policy-dependency-registry",
        type=Path,
        default=POLICY_DEPENDENCY_REGISTRY,
    )
    parser.add_argument(
        "--linked-policy-registry",
        type=Path,
        default=LINKED_POLICY_REGISTRY,
    )
    parser.add_argument("--output-json", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_MD)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    args = parser.parse_args()
    as_of = dt.datetime.now(dt.timezone.utc)
    registry = read_json(args.registry)
    observations = collect_source_observations(
        registry,
        read_json(args.source_config),
        as_of=as_of,
        timeout_sec=max(1.0, args.timeout_sec),
    )
    report = build_report(
        registry,
        observations,
        as_of=as_of,
        policy_dependency_registry=read_json(args.policy_dependency_registry),
        linked_policy_registry=read_json(args.linked_policy_registry),
    )
    news.atomic_write_json(args.output_json, report)
    news.atomic_write_text(args.output_md, render_markdown(report))
    print(json.dumps(report["summary"], sort_keys=True))
    return 0 if report["summary"]["audit_guard_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
