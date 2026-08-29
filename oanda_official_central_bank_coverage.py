#!/usr/bin/env python3
"""Validate and report the official central-bank map for all 21 FX currencies.

This contract is deliberately narrower than the broad news-source audit.  A
currency passes only when a named, verified, first-party monetary authority
policy source is mapped to it.  Official statistical fast-lane transports are
validated and reported in a separate source class and cannot satisfy or degrade
the central-bank policy coverage decision.  Configuration, operational
transport, fresh parser health and scheduling are reported independently.  The
report is research only and cannot confirm, promote, authorize or execute a
trade.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_local_news_sentiment as news
import oanda_news_event_tagger as event_tagger
import oanda_news_source_coverage as source_coverage


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_MAP = ROOT / "config" / "official_central_bank_source_map_v1.json"
DEFAULT_SOURCES = ROOT / "config" / "news_sources_v1.json"
DEFAULT_LINKS = ROOT / "config" / "linked_currency_policy_drivers_v1.json"
DEFAULT_COLLECTOR_STATE = DATA / "local_news_sentiment" / "collector_state_v1.json"
DEFAULT_REPORT_ROOT = DATA / "reports" / "official_central_bank_coverage"
DEFAULT_JSON = DEFAULT_REPORT_ROOT / "OFFICIAL_CENTRAL_BANK_COVERAGE_CURRENT.json"
DEFAULT_MD = DEFAULT_REPORT_ROOT / "OFFICIAL_CENTRAL_BANK_COVERAGE_CURRENT.md"

EXPECTED_CURRENCIES = (
    "AUD", "CAD", "CHF", "CNH", "CZK", "DKK", "EUR", "GBP", "HKD",
    "HUF", "JPY", "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "THB",
    "TRY", "USD", "ZAR",
)
ALLOWED_SCHEDULE_MODES = {
    "domestic_explicit",
    "linked_driver",
    "authority_publication_schedule",
}
POLICY_RELEASE_ROLES = {
    "primary_policy_release",
}
STATISTICAL_RELEASE_ROLES = {
    "primary_central_bank_statistical_release",
    "primary_statistical_release",
}
POLICY_COMMUNICATION_ROLES = {
    "primary_policy_release",
    "primary_policy_communication",
    "primary_policy_commentary",
    "primary_policy_and_event_communication",
}
POLICY_CALENDAR_ROLES = {
    "primary_policy_calendar",
    "primary_policy_communication_calendar",
}


def _source_index(config: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for row in config.get("sources") or ():
        if not isinstance(row, Mapping):
            continue
        source_id = str(row.get("source_id") or "").strip()
        if source_id:
            result[source_id] = row
    return result


def _state_index(state: Mapping[str, Any]) -> Mapping[str, Any]:
    rows = state.get("sources")
    return rows if isinstance(rows, Mapping) else {}


def _linked_index(config: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(row.get("currency") or "").upper(): row
        for row in config.get("relationships") or ()
        if isinstance(row, Mapping) and row.get("currency")
    }


def _source_status(
    source_id: str,
    *,
    source_index: Mapping[str, Mapping[str, Any]],
    state_index: Mapping[str, Any],
    as_of: dt.datetime,
) -> dict[str, Any]:
    source = source_index.get(source_id)
    if source is None:
        return {
            "source_id": source_id,
            "configured": False,
            "verified_first_party": False,
            "operational": False,
            "healthy": False,
            "runtime_status": "missing",
            "last_success_utc": None,
            "last_status": None,
            "last_error": "source_not_registered",
        }
    state = state_index.get(source_id)
    if not isinstance(state, Mapping):
        state = {}
    runtime_status = news.source_runtime_status(source)
    operational = runtime_status in {"enabled", "external_adapter"}
    healthy = operational and source_coverage.source_is_healthy(
        source, state, as_of=as_of
    )
    return {
        "source_id": source_id,
        "configured": True,
        "name": source.get("name"),
        "source_role": source.get("source_role") or news.source_role(source),
        "verified_first_party": bool(source.get("verified"))
        and bool(source.get("direct", True)),
        "runtime_status": runtime_status,
        "operational": operational,
        "healthy": healthy,
        "last_success_utc": state.get("last_success_utc"),
        "last_status": state.get("last_status"),
        "last_error": str(state.get("last_error") or ""),
    }


def _validate_source_binding(
    source_id: str,
    currency: str,
    *,
    source_index: Mapping[str, Mapping[str, Any]],
    allowed_roles: set[str],
) -> list[str]:
    blockers: list[str] = []
    source = source_index.get(source_id)
    if source is None:
        return [f"missing_source:{source_id}"]
    if source.get("verified") is not True or not bool(source.get("direct", True)):
        blockers.append(f"not_verified_first_party:{source_id}")
    currencies = {str(value).upper() for value in source.get("currencies") or ()}
    if currency not in currencies:
        blockers.append(f"currency_binding_mismatch:{source_id}")
    role = str(source.get("source_role") or news.source_role(source))
    if role not in allowed_roles:
        blockers.append(f"source_role_mismatch:{source_id}:{role}")
    return blockers


def build_report(
    *,
    mapping: Mapping[str, Any],
    source_config: Mapping[str, Any],
    collector_state: Mapping[str, Any],
    linked_config: Mapping[str, Any],
    instruments: Sequence[str],
    as_of: dt.datetime,
) -> dict[str, Any]:
    source_index = _source_index(source_config)
    state_index = _state_index(collector_state)
    linked_index = _linked_index(linked_config)
    expected = set(EXPECTED_CURRENCIES)
    frozen = {str(value).upper() for value in mapping.get("frozen_currency_universe") or ()}
    configured_rows = [
        row for row in mapping.get("currencies") or () if isinstance(row, Mapping)
    ]
    configured_codes = [str(row.get("currency") or "").upper() for row in configured_rows]
    duplicate_codes = sorted({code for code in configured_codes if configured_codes.count(code) > 1})
    row_index = {str(row.get("currency") or "").upper(): row for row in configured_rows}

    global_blockers: list[str] = []
    if frozen != expected:
        global_blockers.append("frozen_currency_universe_mismatch")
    if set(configured_codes) != expected:
        global_blockers.append("configured_currency_rows_mismatch")
    if duplicate_codes:
        global_blockers.append("duplicate_currency_rows:" + ",".join(duplicate_codes))
    if mapping.get("expected_currency_count") != 21:
        global_blockers.append("expected_currency_count_contract_mismatch")
    if mapping.get("expected_pair_count") != 68:
        global_blockers.append("expected_pair_count_contract_mismatch")
    if mapping.get("schema_version") != "official_central_bank_source_map_v2":
        global_blockers.append("source_map_schema_version_mismatch")

    currency_rows: dict[str, Any] = {}
    for currency in EXPECTED_CURRENCIES:
        contract = row_index.get(currency, {})
        release_ids = [str(value) for value in contract.get("release_source_ids") or ()]
        statistical_ids = [
            str(value)
            for value in contract.get("statistical_release_source_ids") or ()
        ]
        communication_ids = [
            str(value) for value in contract.get("communication_source_ids") or ()
        ]
        calendar_ids = [str(value) for value in contract.get("calendar_source_ids") or ()]
        policy_calendar_ids = [
            source_id
            for source_id in calendar_ids
            if str(
                source_index.get(source_id, {}).get("source_role")
                or news.source_role(source_index.get(source_id, {}))
            )
            == "primary_policy_calendar"
        ]
        schedule_mode = str(contract.get("schedule_mode") or "")
        blockers: list[str] = []
        statistical_blockers: list[str] = []
        if not contract:
            blockers.append("missing_currency_contract")
        if "statistical_release_source_ids" not in contract:
            statistical_blockers.append("missing_statistical_release_source_ids")
        if not str(contract.get("authority") or "").strip():
            blockers.append("missing_authority")
        if not str(contract.get("policy_framework") or "").strip():
            blockers.append("missing_policy_framework")
        if not release_ids:
            blockers.append("missing_direct_policy_release_source")
        overlap = sorted(set(release_ids) & set(statistical_ids))
        if overlap:
            statistical_blockers.append(
                "source_class_overlap:" + ",".join(overlap)
            )
        if schedule_mode not in ALLOWED_SCHEDULE_MODES:
            blockers.append("invalid_schedule_mode")
        for source_id in release_ids:
            blockers.extend(
                _validate_source_binding(
                    source_id,
                    currency,
                    source_index=source_index,
                    allowed_roles=POLICY_RELEASE_ROLES,
                )
            )
        for source_id in statistical_ids:
            statistical_blockers.extend(
                _validate_source_binding(
                    source_id,
                    currency,
                    source_index=source_index,
                    allowed_roles=STATISTICAL_RELEASE_ROLES,
                )
            )
        for source_id in communication_ids:
            blockers.extend(
                _validate_source_binding(
                    source_id,
                    currency,
                    source_index=source_index,
                    allowed_roles=POLICY_COMMUNICATION_ROLES,
                )
            )
        for source_id in calendar_ids:
            blockers.extend(
                _validate_source_binding(
                    source_id,
                    currency,
                    source_index=source_index,
                    allowed_roles=POLICY_CALENDAR_ROLES,
                )
            )

        linked_driver = str(contract.get("linked_driver_currency") or "").upper()
        linked_relationship = str(contract.get("linked_driver_relationship") or "")
        if schedule_mode == "domestic_explicit" and not policy_calendar_ids:
            blockers.append("missing_domestic_policy_calendar")
        if schedule_mode == "linked_driver":
            linked = linked_index.get(currency)
            if not linked_driver or linked_driver not in expected:
                blockers.append("invalid_linked_driver_currency")
            if not linked or str(linked.get("driver_currency") or "").upper() != linked_driver:
                blockers.append("linked_driver_contract_mismatch")
            if not linked or str(linked.get("relationship") or "") != linked_relationship:
                blockers.append("linked_relationship_contract_mismatch")
            if linked and (linked.get("can_assign_direction") is not False or linked.get("can_confirm") is not False):
                blockers.append("linked_driver_not_fail_closed")
        elif linked_driver or linked_relationship:
            blockers.append("unexpected_linked_driver")

        release_status = [
            _source_status(
                source_id,
                source_index=source_index,
                state_index=state_index,
                as_of=as_of,
            )
            for source_id in release_ids
        ]
        statistical_status = [
            _source_status(
                source_id,
                source_index=source_index,
                state_index=state_index,
                as_of=as_of,
            )
            for source_id in statistical_ids
        ]
        communication_status = [
            _source_status(
                source_id,
                source_index=source_index,
                state_index=state_index,
                as_of=as_of,
            )
            for source_id in communication_ids
        ]
        calendar_status = [
            _source_status(
                source_id,
                source_index=source_index,
                state_index=state_index,
                as_of=as_of,
            )
            for source_id in calendar_ids
        ]
        policy_calendar_status = [
            row
            for row in calendar_status
            if row.get("source_role") == "primary_policy_calendar"
        ]
        communication_calendar_status = [
            row
            for row in calendar_status
            if row.get("source_role") == "primary_policy_communication_calendar"
        ]
        release_operational = any(row["operational"] for row in release_status)
        release_healthy = any(row["healthy"] for row in release_status)
        if schedule_mode == "linked_driver":
            driver_contract = row_index.get(linked_driver, {})
            driver_calendar_ids = [
                str(value) for value in driver_contract.get("calendar_source_ids") or ()
                if str(
                    source_index.get(str(value), {}).get("source_role")
                    or news.source_role(source_index.get(str(value), {}))
                )
                == "primary_policy_calendar"
            ]
            driver_calendar_status = [
                _source_status(
                    source_id,
                    source_index=source_index,
                    state_index=state_index,
                    as_of=as_of,
                )
                for source_id in driver_calendar_ids
            ]
            schedule_operational = any(row["operational"] for row in driver_calendar_status)
            schedule_healthy = any(row["healthy"] for row in driver_calendar_status)
        elif schedule_mode == "authority_publication_schedule":
            driver_calendar_status = []
            schedule_operational = release_operational
            schedule_healthy = release_healthy
        else:
            driver_calendar_status = []
            schedule_operational = any(
                row["operational"] for row in policy_calendar_status
            )
            schedule_healthy = any(row["healthy"] for row in policy_calendar_status)

        configured_complete = not blockers
        runtime_state = (
            "healthy"
            if configured_complete and release_healthy and schedule_healthy
            else "operational_degraded"
            if configured_complete and release_operational and schedule_operational
            else "not_operational"
        )
        currency_rows[currency] = {
            "currency": currency,
            "authority": contract.get("authority"),
            "authority_id": contract.get("authority_id"),
            "policy_framework": contract.get("policy_framework"),
            "schedule_mode": schedule_mode,
            "linked_driver_currency": linked_driver or None,
            "configured_complete": configured_complete,
            "release_operational": release_operational,
            "release_healthy": release_healthy,
            "schedule_operational": schedule_operational,
            "schedule_healthy": schedule_healthy,
            "runtime_state": runtime_state,
            "blockers": blockers,
            "statistical_release_configured_complete": not statistical_blockers,
            "statistical_release_operational": sum(
                bool(row["operational"]) for row in statistical_status
            ),
            "statistical_release_healthy": sum(
                bool(row["healthy"]) for row in statistical_status
            ),
            "statistical_release_blockers": statistical_blockers,
            "release_sources": release_status,
            "statistical_release_sources": statistical_status,
            "communication_sources": communication_status,
            "calendar_sources": calendar_status,
            "policy_decision_calendar_sources": policy_calendar_status,
            "communication_calendar_sources": communication_calendar_status,
            "linked_driver_calendar_sources": driver_calendar_status,
        }

    instrument_set = sorted({str(value).upper() for value in instruments if value})
    discovered_currencies = {
        leg for instrument in instrument_set for leg in instrument.split("_") if leg
    }
    if len(instrument_set) != 68:
        global_blockers.append(f"pair_universe_count:{len(instrument_set)}")
    if discovered_currencies != expected:
        global_blockers.append("pair_currency_universe_mismatch")
    pair_rows: dict[str, Any] = {}
    for instrument in instrument_set:
        parts = instrument.split("_")
        if len(parts) != 2:
            global_blockers.append(f"invalid_instrument:{instrument}")
            continue
        base, quote = parts
        base_row = currency_rows.get(base, {})
        quote_row = currency_rows.get(quote, {})
        pair_rows[instrument] = {
            "instrument": instrument,
            "base_currency": base,
            "quote_currency": quote,
            "both_legs_configured": bool(base_row.get("configured_complete"))
            and bool(quote_row.get("configured_complete")),
            "both_legs_operational": bool(base_row.get("release_operational"))
            and bool(quote_row.get("release_operational")),
            "both_legs_healthy": bool(base_row.get("release_healthy"))
            and bool(quote_row.get("release_healthy")),
            "mapping_rule": "validated_base_strength_minus_validated_quote_strength",
        }

    configured_count = sum(row["configured_complete"] for row in currency_rows.values())
    operational_count = sum(
        row["configured_complete"] and row["release_operational"]
        for row in currency_rows.values()
    )
    healthy_count = sum(
        row["configured_complete"] and row["release_healthy"]
        for row in currency_rows.values()
    )
    schedule_operational_count = sum(
        row["configured_complete"] and row["schedule_operational"]
        for row in currency_rows.values()
    )
    configured_pair_count = sum(row["both_legs_configured"] for row in pair_rows.values())
    operational_pair_count = sum(row["both_legs_operational"] for row in pair_rows.values())
    healthy_pair_count = sum(row["both_legs_healthy"] for row in pair_rows.values())
    statistical_source_count = sum(
        len(row["statistical_release_sources"]) for row in currency_rows.values()
    )
    statistical_operational_count = sum(
        row["statistical_release_operational"] for row in currency_rows.values()
    )
    statistical_healthy_count = sum(
        row["statistical_release_healthy"] for row in currency_rows.values()
    )
    statistical_blocker_count = sum(
        len(row["statistical_release_blockers"]) for row in currency_rows.values()
    )
    policy = mapping.get("policy") if isinstance(mapping.get("policy"), Mapping) else {}
    return {
        "schema_version": "official_central_bank_coverage_v2",
        "contract_id": mapping.get("contract_id"),
        "generated_utc": news.iso_utc(),
        "as_of_utc": news.iso_utc(as_of),
        "policy": {
            "research_only": True,
            "execution_eligible": False,
            "can_confirm": False,
            "can_promote": False,
            "can_authorize": False,
            "matrix_weight": 0.0,
            "configured_policy_matches": policy.get("research_only") is True
            and policy.get("execution_eligible") is False
            and policy.get("can_confirm") is False
            and policy.get("can_promote") is False
            and policy.get("can_authorize") is False,
        },
        "global_blockers": global_blockers,
        "currency_summary": {
            "expected": 21,
            "configured_complete": configured_count,
            "release_operational": operational_count,
            "release_healthy": healthy_count,
            "schedule_operational": schedule_operational_count,
        },
        "pair_summary": {
            "expected": 68,
            "emitted": len(pair_rows),
            "both_legs_configured": configured_pair_count,
            "both_legs_operational": operational_pair_count,
            "both_legs_healthy": healthy_pair_count,
        },
        "statistical_release_summary": {
            "mapped_sources": statistical_source_count,
            "operational": statistical_operational_count,
            "healthy": statistical_healthy_count,
            "blockers": statistical_blocker_count,
        },
        "statistical_release_contract_complete": statistical_blocker_count == 0,
        "contract_complete": not global_blockers and configured_count == 21,
        "minimum_operational_complete": not global_blockers
        and operational_count == 21
        and schedule_operational_count == 21
        and operational_pair_count == 68,
        "fully_healthy": not global_blockers
        and healthy_count == 21
        and healthy_pair_count == 68,
        "currencies": currency_rows,
        "pairs": pair_rows,
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    currencies = report.get("currencies") or {}
    lines = [
        "# Official Central-Bank Coverage",
        "",
        f"Generated: `{report.get('generated_utc')}`",
        "",
        "This is the canonical 21-currency monetary-authority map. It is research-only and has zero execution weight.",
        "",
        "## Summary",
        "",
        f"- Contract complete: **{report.get('contract_complete')}**",
        f"- Minimum operational complete: **{report.get('minimum_operational_complete')}**",
        f"- Fully healthy: **{report.get('fully_healthy')}**",
        f"- Currencies: `{json.dumps(report.get('currency_summary'), sort_keys=True)}`",
        f"- Pairs: `{json.dumps(report.get('pair_summary'), sort_keys=True)}`",
        f"- Statistical fast lane: `{json.dumps(report.get('statistical_release_summary'), sort_keys=True)}`",
        "",
        "## Currency authorities",
        "",
        "| Currency | Authority | Framework | Schedule | Policy release | Policy health | Statistical fast lane |",
        "|---|---|---|---|---:|---:|---:|",
    ]
    for currency in EXPECTED_CURRENCIES:
        row = currencies.get(currency, {})
        lines.append(
            "| {currency} | {authority} | {framework} | {schedule} | {operational} | {healthy} | {statistical} |".format(
                currency=currency,
                authority=row.get("authority") or "missing",
                framework=row.get("policy_framework") or "missing",
                schedule=row.get("schedule_mode") or "missing",
                operational="yes" if row.get("release_operational") else "no",
                healthy="yes" if row.get("release_healthy") else "no",
                statistical=len(row.get("statistical_release_sources") or ()),
            )
        )
        if row.get("blockers"):
            lines.append(f"|  | blockers | `{', '.join(row['blockers'])}` |  |  |  |  |")
        if row.get("statistical_release_blockers"):
            lines.append(
                "|  | statistical blockers | "
                f"`{', '.join(row['statistical_release_blockers'])}` |  |  |  |  |"
            )
    lines.extend(
        [
            "",
            "## Frozen mapping rule",
            "",
            "Official events map first to currency direction, strength, magnitude and horizon. Pair state is base minus quote. Technical and executable-price evidence may time or veto the setup, but cannot reverse an unvalidated event interpretation or make this source map executable.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    *,
    map_path: Path = DEFAULT_MAP,
    sources_path: Path = DEFAULT_SOURCES,
    linked_path: Path = DEFAULT_LINKS,
    state_path: Path = DEFAULT_COLLECTOR_STATE,
    json_path: Path = DEFAULT_JSON,
    md_path: Path = DEFAULT_MD,
) -> dict[str, Any]:
    report = build_report(
        mapping=news.load_json(map_path, {}),
        source_config=news.load_json(sources_path, {}),
        collector_state=news.load_json(state_path, {}),
        linked_config=news.load_json(linked_path, {}),
        instruments=event_tagger.discover_instruments(),
        as_of=news.utc_now(),
    )
    news.atomic_write_json(json_path, report)
    news.atomic_write_text(md_path, render_markdown(report))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--map", type=Path, default=DEFAULT_MAP)
    parser.add_argument("--sources", type=Path, default=DEFAULT_SOURCES)
    parser.add_argument("--links", type=Path, default=DEFAULT_LINKS)
    parser.add_argument("--state", type=Path, default=DEFAULT_COLLECTOR_STATE)
    parser.add_argument("--json", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MD)
    args = parser.parse_args()
    report = run(
        map_path=args.map,
        sources_path=args.sources,
        linked_path=args.links,
        state_path=args.state,
        json_path=args.json,
        md_path=args.markdown,
    )
    print(json.dumps({
        "contract_complete": report["contract_complete"],
        "minimum_operational_complete": report["minimum_operational_complete"],
        "fully_healthy": report["fully_healthy"],
        "currency_summary": report["currency_summary"],
        "pair_summary": report["pair_summary"],
        "global_blockers": report["global_blockers"],
    }, sort_keys=True))
    return 0 if report["minimum_operational_complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
