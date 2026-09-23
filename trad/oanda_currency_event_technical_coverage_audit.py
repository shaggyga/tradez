#!/usr/bin/env python3
"""Report how official event inputs join technical evidence across all pairs.

This is a read-only, research-only coverage audit.  It deliberately separates
pair coverage from causal data depth: a currency can be present in every
technical calculation while still lacking structured actuals, pre-release
consensus, a rate-market context, or a frozen statement-delta baseline.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_local_news_sentiment as news


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
REPORT_ROOT = DATA / "reports" / "currency_event_technical_coverage"
DEFAULT_SOURCE_AUDIT = (
    DATA
    / "reports"
    / "official_source_direction_coverage"
    / "OFFICIAL_SOURCE_DIRECTION_COVERAGE_CURRENT.json"
)
DEFAULT_QUOTES = STATE / "practice_007_market_quotes_v1.json"
DEFAULT_WATCHLIST = STATE / "news_technical_watchlist_v1.json"
DEFAULT_RATES = STATE / "official_daily_rate_context_v1.json"
DEFAULT_CONSENSUS = STATE / "macro_consensus_prospective_v1.json"
DEFAULT_MACRO_COHORT = STATE / "macro_release_breakout_research_v1.json"
DEFAULT_CURRENCY_MACRO_COHORT = STATE / "currency_macro_release_breakout_research_v1.json"
DEFAULT_POLICY_COHORT = STATE / "policy_statement_breakout_research_v1.json"
DEFAULT_POLICY_BASELINES = ROOT / "config" / "official_policy_statement_baselines_v2_20260816.json"
DEFAULT_SOURCE_CONFIG = ROOT / "config" / "news_sources_v1.json"
DEFAULT_EVENT_CATALOG = DATA / "news_event_tags" / "events_latest.json"
DEFAULT_POLICY_DRIVERS = ROOT / "config" / "linked_currency_policy_drivers_v1.json"
DEFAULT_JSON = REPORT_ROOT / "CURRENCY_EVENT_TECHNICAL_COVERAGE_CURRENT.json"
DEFAULT_MD = REPORT_ROOT / "CURRENCY_EVENT_TECHNICAL_COVERAGE_CURRENT.md"


def read_json(path: Path) -> dict[str, Any]:
    return news.load_json(path, {})


def universe_from_quotes(payload: Mapping[str, Any]) -> list[str]:
    quotes = payload.get("quotes") or {}
    if isinstance(quotes, Mapping):
        values = quotes.keys()
    else:
        values = (
            row.get("instrument")
            for row in quotes
            if isinstance(row, Mapping)
        )
    return sorted(
        {
            str(value).upper()
            for value in values
            if value and len(str(value).split("_")) == 2
        }
    )


def pair_currencies(instruments: Sequence[str]) -> list[str]:
    return sorted({leg for instrument in instruments for leg in instrument.split("_")})


def source_rows_for_currency(
    rows: Sequence[Mapping[str, Any]], currency: str, population: str
) -> list[Mapping[str, Any]]:
    return [
        row
        for row in rows
        if str(row.get("source_population") or "") == population
        and currency in {str(item).upper() for item in row.get("configured_currencies") or []}
    ]


def future_schedule_counts(
    events: Sequence[Mapping[str, Any]], as_of: dt.datetime
) -> tuple[dict[str, int], dict[str, int]]:
    """Count causally scheduled future events and explicit policy decisions."""

    all_events: dict[str, int] = {}
    policy_events: dict[str, int] = {}
    for event in events:
        scheduled = news.parse_datetime(event.get("scheduled_utc"))
        if scheduled is None or scheduled < as_of:
            continue
        currencies = {
            str(item).upper() for item in event.get("currencies") or [] if item
        }
        category = str(event.get("category") or "").lower()
        headline = str(event.get("headline") or "").lower()
        policy = bool(
            "monetary_policy" in category
            or "policy decision" in headline
            or "interest rate decision" in headline
            or "bank rate decision" in headline
        )
        for currency in currencies:
            all_events[currency] = all_events.get(currency, 0) + 1
            if policy:
                policy_events[currency] = policy_events.get(currency, 0) + 1
    return all_events, policy_events


def build_currency_rows(
    instruments: Sequence[str],
    source_rows: Sequence[Mapping[str, Any]],
    rate_currencies: set[str],
    policy_baseline_currencies: set[str],
    macro_breakout_currencies: set[str],
    technical_contract_active: bool,
    scheduled_event_counts: Mapping[str, int] | None = None,
    scheduled_policy_counts: Mapping[str, int] | None = None,
    numeric_adapter_currencies: set[str] | None = None,
    rate_context_by_currency: Mapping[str, Mapping[str, Any]] | None = None,
    as_of_date: dt.date | None = None,
    policy_driver_by_currency: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    scheduled_event_counts = scheduled_event_counts or {}
    scheduled_policy_counts = scheduled_policy_counts or {}
    rate_context_by_currency = rate_context_by_currency or {}
    policy_driver_by_currency = policy_driver_by_currency or {}
    as_of_date = as_of_date or dt.datetime.now(dt.timezone.utc).date()
    if numeric_adapter_currencies is None:
        numeric_adapter_currencies = {
            currency
            for currency in pair_currencies(instruments)
            if any(
                currency
                in {
                    str(item).upper()
                    for item in row.get("configured_currencies") or []
                }
                and int(row.get("actual") or 0) > 0
                for row in source_rows
            )
        }
    result: list[dict[str, Any]] = []
    for currency in pair_currencies(instruments):
        legs = sorted(item for item in instruments if currency in item.split("_"))
        policy = source_rows_for_currency(
            source_rows, currency, "official_policy_publisher"
        )
        macro = source_rows_for_currency(
            source_rows, currency, "official_macro_publisher"
        )
        rates = source_rows_for_currency(source_rows, currency, "official_rates_curve")
        rate_context = rate_context_by_currency.get(currency) or {}
        rate_date = news.parse_datetime(rate_context.get("rate_date"))
        if rate_date is None:
            try:
                parsed_rate_date = dt.date.fromisoformat(
                    str(rate_context.get("rate_date") or "")
                )
            except ValueError:
                parsed_rate_date = None
        else:
            parsed_rate_date = rate_date.date()
        rate_age_days = (
            (as_of_date - parsed_rate_date).days
            if parsed_rate_date is not None
            else None
        )
        # Daily/weekday official curves normally arrive with a one- to
        # several-business-day lag.  Keep slower contexts visible, but do not
        # let an old month-end observation look current merely because its
        # collector ran today.
        fresh_daily_rate = bool(
            currency in rate_currencies
            and (
                not rate_context
                or (rate_age_days is not None and 0 <= rate_age_days <= 7)
            )
        )
        relevant = policy + macro + rates
        actual = sum(int(row.get("actual") or 0) for row in relevant)
        prospective_actual = sum(
            int(row.get("prospective_actual") or 0) for row in relevant
        )
        structured = sum(int(row.get("structured") or 0) for row in relevant)
        timely = sum(int(row.get("timely_directional") or 0) for row in relevant)
        raw_consensus = sum(int(row.get("consensus") or 0) for row in relevant)
        causal_consensus = sum(
            int(row.get("causal_consensus") or 0) for row in relevant
        )
        technical_join = bool(technical_contract_active and legs)
        policy_driver = policy_driver_by_currency.get(currency) or {}
        driver_currency = str(policy_driver.get("driver_currency") or "").upper()
        inherited_policy_events = int(
            scheduled_policy_counts.get(driver_currency) or 0
        ) if driver_currency else 0
        domestic_events = int(scheduled_event_counts.get(currency) or 0)
        domestic_policy_events = int(
            scheduled_policy_counts.get(currency) or 0
        )
        relevant_events = domestic_events + inherited_policy_events
        relevant_policy_events = domestic_policy_events + inherited_policy_events
        blockers: list[str] = []
        if not policy:
            blockers.append("no_observed_official_policy_source")
        if not macro:
            blockers.append("no_observed_official_macro_source")
        if not actual:
            blockers.append("no_structured_actual_value")
        if currency not in numeric_adapter_currencies:
            blockers.append("no_structured_numeric_parser")
        if not causal_consensus:
            blockers.append("no_causal_pre_release_consensus")
        if currency not in rate_currencies:
            blockers.append("no_daily_rate_context")
        elif not fresh_daily_rate:
            blockers.append("stale_daily_rate_context")
        if currency not in policy_baseline_currencies:
            blockers.append("no_frozen_policy_delta_baseline")
        if currency not in macro_breakout_currencies:
            blockers.append("no_prospective_macro_breakout_adapter")
        if not relevant_events:
            blockers.append("no_future_relevant_event_clock")
        if not relevant_policy_events:
            blockers.append("no_future_relevant_policy_decision_clock")
        if not technical_join:
            blockers.append("technical_join_inactive")
        full_causal_stack = not blockers
        result.append(
            {
                "currency": currency,
                "pair_leg_count": len(legs),
                "pairs": legs,
                "official_policy_sources": sorted(
                    {str(row.get("source_id")) for row in policy}
                ),
                "official_macro_sources": sorted(
                    {str(row.get("source_id")) for row in macro}
                ),
                "official_rate_sources": sorted(
                    {
                        *{str(row.get("source_id")) for row in rates},
                        *(
                            {str(rate_context.get("source_id"))}
                            if rate_context.get("source_id")
                            else set()
                        ),
                    }
                ),
                "structured_observations": structured,
                "actual_value_observations": actual,
                "prospective_actual_value_observations": prospective_actual,
                "structured_numeric_parser": currency in numeric_adapter_currencies,
                "raw_consensus_fields": raw_consensus,
                "causal_pre_release_consensus": causal_consensus,
                "timely_directional_mappings": timely,
                "daily_rate_context": currency in rate_currencies,
                "fresh_daily_rate_context": fresh_daily_rate,
                "daily_rate_source_id": rate_context.get("source_id"),
                "daily_rate_provider": rate_context.get("provider"),
                "daily_rate_date": rate_context.get("rate_date"),
                "daily_rate_age_calendar_days": rate_age_days,
                "daily_rate_tenor": rate_context.get("tenor_label"),
                "daily_rate_measure": rate_context.get("rate_measure"),
                "daily_rate_comparison_group": rate_context.get(
                    "comparison_group"
                ),
                "daily_rate_comparable_2y": bool(
                    rate_context.get("comparison_group")
                    == "two_year_market_rate_context"
                ),
                "daily_rate_prospective": bool(
                    rate_context.get("prospective_eligible")
                ),
                "policy_delta_baseline": currency in policy_baseline_currencies,
                "prospective_macro_breakout_adapter": currency
                in macro_breakout_currencies,
                "future_scheduled_events": domestic_events,
                "future_scheduled_policy_decisions": domestic_policy_events,
                "inherited_policy_driver_currency": driver_currency or None,
                "inherited_policy_relationship": policy_driver.get(
                    "relationship"
                ),
                "inherited_policy_factor_independence": policy_driver.get(
                    "factor_independence"
                ),
                "future_inherited_policy_decisions": inherited_policy_events,
                "future_relevant_events": relevant_events,
                "future_relevant_policy_decisions": relevant_policy_events,
                "inherited_policy_clock_can_confirm": bool(
                    policy_driver.get("can_confirm")
                ),
                "technical_join": technical_join,
                "full_causal_stack": full_causal_stack,
                "blockers": blockers,
            }
        )
    return result


def build_pair_rows(
    instruments: Sequence[str], currency_rows: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """Expose economic depth for each priced pair without hiding weak legs.

    A pair is technically joinable when its price feature contract is active,
    but that says nothing about the economic depth of either currency.  These
    booleans make the one-leg/both-leg distinction explicit for all 68 pairs.
    """

    by_currency = {
        str(row.get("currency") or "").upper(): row for row in currency_rows
    }
    result: list[dict[str, Any]] = []
    for instrument in sorted(instruments):
        base, quote = instrument.split("_", 1)
        legs = [by_currency.get(base, {}), by_currency.get(quote, {})]

        def leg_flags(field: str) -> list[bool]:
            return [bool(row.get(field)) for row in legs]

        policy = [bool(row.get("official_policy_sources")) for row in legs]
        macro = [bool(row.get("official_macro_sources")) for row in legs]
        parser = leg_flags("structured_numeric_parser")
        actual = [bool(row.get("actual_value_observations")) for row in legs]
        prospective_actual = [
            bool(row.get("prospective_actual_value_observations")) for row in legs
        ]
        rates = leg_flags("daily_rate_context")
        fresh_rates = leg_flags("fresh_daily_rate_context")
        comparable_rates = leg_flags("daily_rate_comparable_2y")
        consensus = [bool(row.get("causal_pre_release_consensus")) for row in legs]
        clocks = [
            bool(
                row.get(
                    "future_relevant_events", row.get("future_scheduled_events")
                )
            )
            for row in legs
        ]
        technical = all(leg_flags("technical_join"))
        full = all(leg_flags("full_causal_stack"))
        result.append(
            {
                "instrument": instrument,
                "base_currency": base,
                "quote_currency": quote,
                "technical_join": technical,
                "official_policy_any_leg": any(policy),
                "official_policy_both_legs": all(policy),
                "official_macro_any_leg": any(macro),
                "official_macro_both_legs": all(macro),
                "numeric_parser_any_leg": any(parser),
                "numeric_parser_both_legs": all(parser),
                "actual_value_any_leg": any(actual),
                "actual_value_both_legs": all(actual),
                "prospective_actual_any_leg": any(prospective_actual),
                "prospective_actual_both_legs": all(prospective_actual),
                "daily_rate_any_leg": any(rates),
                "daily_rate_both_legs": all(rates),
                "fresh_daily_rate_any_leg": any(fresh_rates),
                "fresh_daily_rate_both_legs": all(fresh_rates),
                "comparable_2y_rate_any_leg": any(comparable_rates),
                "comparable_2y_rate_both_legs": all(comparable_rates),
                "causal_consensus_any_leg": any(consensus),
                "causal_consensus_both_legs": all(consensus),
                "future_event_clock_any_leg": any(clocks),
                "future_event_clock_both_legs": all(clocks),
                "full_causal_stack_both_legs": full,
            }
        )
    return result


def pair_leg_grade(any_leg: bool, both_legs: bool) -> str:
    if both_legs:
        return "both"
    return "one" if any_leg else "none"


def render_markdown(payload: Mapping[str, Any]) -> str:
    totals = payload["totals"]
    lines = [
        "# Currency Event + Technical Coverage",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Research-only. Pair coverage is not treated as proof of equal economic-source depth.",
        "",
        f"- OANDA priced pairs: **{totals['pairs']}**",
        f"- Distinct currencies: **{totals['currencies']}**",
        f"- Pairs covered by the technical join contract: **{totals['technical_pairs']}**",
        f"- Currencies with observed official policy sources: **{totals['currencies_with_policy']}**",
        f"- Currencies with observed official macro sources: **{totals['currencies_with_macro']}**",
        f"- Currencies with a configured structured numeric parser: **{totals['currencies_with_numeric_parser']}**",
        f"- Currencies with a prospectively observed actual: **{totals['currencies_with_prospective_actuals']}**",
        f"- Currencies with causally certified pre-release consensus: **{totals['currencies_with_causal_consensus']}**",
        f"- Currencies with daily official rate context: **{totals['currencies_with_daily_rates']}**",
        f"- Currencies with fresh daily official rate context (<=7 calendar days): **{totals['currencies_with_fresh_daily_rates']}**",
        f"- Currencies with comparable two-year rate context: **{totals['currencies_with_comparable_2y_rates']}**",
        f"- Currencies with a frozen policy-delta baseline: **{totals['currencies_with_policy_baseline']}**",
        f"- Currencies with a prospective macro-breakout adapter: **{totals['currencies_with_macro_breakout']}**",
        f"- Currencies with at least one future official event clock: **{totals['currencies_with_future_event_clock']}**",
        f"- Currencies with an explicit future policy-decision clock: **{totals['currencies_with_future_policy_clock']}**",
        f"- Currencies with a relevant domestic or structurally inherited policy clock: **{totals['currencies_with_future_relevant_policy_clock']}**",
        f"- Currencies with the complete causal stack: **{totals['currencies_full_causal_stack']}**",
        f"- Pairs with a numeric release parser on one/both legs: **{totals['pairs_with_numeric_parser_any_leg']} / {totals['pairs_with_numeric_parser_both_legs']}**",
        f"- Pairs with retained structured actual values on one/both legs: **{totals['pairs_with_actual_values_any_leg']} / {totals['pairs_with_actual_values_both_legs']}**",
        f"- Pairs with prospectively observed actual values on one/both legs: **{totals['pairs_with_prospective_actuals_any_leg']} / {totals['pairs_with_prospective_actuals_both_legs']}**",
        f"- Pairs with daily rate context on one/both legs: **{totals['pairs_with_daily_rates_any_leg']} / {totals['pairs_with_daily_rates_both_legs']}**",
        f"- Pairs with fresh daily rate context on one/both legs: **{totals['pairs_with_fresh_daily_rates_any_leg']} / {totals['pairs_with_fresh_daily_rates_both_legs']}**",
        f"- Pairs with comparable two-year rate context on one/both legs: **{totals['pairs_with_comparable_2y_rates_any_leg']} / {totals['pairs_with_comparable_2y_rates_both_legs']}**",
        f"- Pairs with causal consensus on one/both legs: **{totals['pairs_with_causal_consensus_any_leg']} / {totals['pairs_with_causal_consensus_both_legs']}**",
        "",
        "| Currency | Pair legs | Policy | Macro | Numeric parser | Retained actuals | Prospective actuals | Timely direction | Causal consensus | Domestic events/policy | Inherited policy driver/count | Daily rate source | Rate date/age | Fresh | Comparable 2Y | Policy delta | Macro breakout | Technical join | Main blockers |",
        "|---|---:|---:|---:|---|---:|---:|---:|---:|---|---|---|---|---|---|---|---|---|---|",
    ]
    for row in payload["currencies"]:
        lines.append(
            f"| {row['currency']} | {row['pair_leg_count']} | "
            f"{len(row['official_policy_sources'])} | {len(row['official_macro_sources'])} | "
            f"{'yes' if row['structured_numeric_parser'] else 'no'} | "
            f"{row['actual_value_observations']} | {row['prospective_actual_value_observations']} | "
            f"{row['timely_directional_mappings']} | "
            f"{row['causal_pre_release_consensus']} | "
            f"{row['future_scheduled_events']} / {row['future_scheduled_policy_decisions']} | "
            f"{row['inherited_policy_driver_currency'] or 'none'} / {row['future_inherited_policy_decisions']} | "
            f"{row['daily_rate_source_id'] or ('unattributed' if row['daily_rate_context'] else 'none')} | "
            f"{row['daily_rate_date'] or 'n/a'} / {row['daily_rate_age_calendar_days'] if row['daily_rate_age_calendar_days'] is not None else 'n/a'}d | "
            f"{'yes' if row['fresh_daily_rate_context'] else 'no'} | "
            f"{'yes' if row['daily_rate_comparable_2y'] else 'no'} | "
            f"{'yes' if row['policy_delta_baseline'] else 'no'} | "
            f"{'yes' if row['prospective_macro_breakout_adapter'] else 'no'} | "
            f"{'yes' if row['technical_join'] else 'no'} | "
            f"{', '.join(row['blockers']) or 'none'} |"
        )
    lines.extend(
        [
            "",
            "## Pair-leg economic depth",
            "",
            "`both`, `one`, and `none` refer to the two currency legs; technical coverage does not upgrade missing economic inputs.",
            "",
            "| Pair | Policy | Macro | Numeric parser | Retained actuals | Prospective actuals | Future clock | Daily rates | Causal consensus | Technical |",
            "|---|---|---|---|---|---|---|---|---|---|",
        ]
    )
    for row in payload["pairs"]:
        lines.append(
            f"| {row['instrument']} | "
            f"{pair_leg_grade(row['official_policy_any_leg'], row['official_policy_both_legs'])} | "
            f"{pair_leg_grade(row['official_macro_any_leg'], row['official_macro_both_legs'])} | "
            f"{pair_leg_grade(row['numeric_parser_any_leg'], row['numeric_parser_both_legs'])} | "
            f"{pair_leg_grade(row['actual_value_any_leg'], row['actual_value_both_legs'])} | "
            f"{pair_leg_grade(row['prospective_actual_any_leg'], row['prospective_actual_both_legs'])} | "
            f"{pair_leg_grade(row['future_event_clock_any_leg'], row['future_event_clock_both_legs'])} | "
            f"{pair_leg_grade(row['daily_rate_any_leg'], row['daily_rate_both_legs'])} | "
            f"{pair_leg_grade(row['causal_consensus_any_leg'], row['causal_consensus_both_legs'])} | "
            f"{'yes' if row['technical_join'] else 'no'} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- The technical layer can evaluate every priced pair leg, but the weekend market correctly produces zero fresh technical signals and zero entries.",
            "- Numeric macro releases without certified pre-release consensus are watched in both directions and require post-release factor confirmation; they are not assigned a side from semantics alone.",
            "- Policy statements require a frozen prior-statement baseline and a deterministic statement delta before technical breakout confirmation is considered.",
            "- Daily government yields are conditioning data, not intraday rate repricing. Missing OIS/futures confirmation remains missing.",
            "",
            "Execution decision remains `no_trade`.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    source_audit_path: Path = DEFAULT_SOURCE_AUDIT,
    quotes_path: Path = DEFAULT_QUOTES,
    watchlist_path: Path = DEFAULT_WATCHLIST,
    rates_path: Path = DEFAULT_RATES,
    consensus_path: Path = DEFAULT_CONSENSUS,
    macro_cohort_path: Path = DEFAULT_MACRO_COHORT,
    currency_macro_cohort_path: Path = DEFAULT_CURRENCY_MACRO_COHORT,
    policy_cohort_path: Path = DEFAULT_POLICY_COHORT,
    policy_baselines_path: Path = DEFAULT_POLICY_BASELINES,
    event_catalog_path: Path = DEFAULT_EVENT_CATALOG,
    policy_drivers_path: Path = DEFAULT_POLICY_DRIVERS,
    output_json: Path = DEFAULT_JSON,
    output_md: Path = DEFAULT_MD,
    source_config_path: Path = DEFAULT_SOURCE_CONFIG,
) -> dict[str, Any]:
    source_audit = read_json(source_audit_path)
    quotes = read_json(quotes_path)
    watchlist = read_json(watchlist_path)
    rates = read_json(rates_path)
    consensus = read_json(consensus_path)
    macro_cohort = read_json(macro_cohort_path)
    currency_macro_cohort = read_json(currency_macro_cohort_path)
    policy_cohort = read_json(policy_cohort_path)
    policy_baselines = read_json(policy_baselines_path)
    source_config = read_json(source_config_path)
    event_catalog_value = news.load_json(event_catalog_path, [])
    policy_drivers = read_json(policy_drivers_path)
    event_catalog = (
        event_catalog_value
        if isinstance(event_catalog_value, list)
        else event_catalog_value.get("events") or []
        if isinstance(event_catalog_value, Mapping)
        else []
    )
    as_of = dt.datetime.now(dt.timezone.utc)
    scheduled_counts, scheduled_policy_counts = future_schedule_counts(
        event_catalog, as_of
    )
    policy_driver_by_currency = {
        str(row.get("currency") or "").upper(): row
        for row in policy_drivers.get("relationships") or []
        if isinstance(row, Mapping) and row.get("currency")
    }
    instruments = universe_from_quotes(quotes)
    rate_currencies = {str(item).upper() for item in (rates.get("currencies") or {})}
    baseline_currencies = {
        str(row.get("currency") or "").upper()
        for row in policy_baselines.get("baselines") or []
        if row.get("currency")
    }
    macro_breakout_currencies = {
        leg
        for instrument in macro_cohort.get("eligible_universe") or []
        for leg in str(instrument).split("_")
        if leg == "USD"
    }
    macro_breakout_currencies.update(
        str(currency).upper()
        for currency in currency_macro_cohort.get("supported_currencies") or []
    )
    numeric_adapter_currencies = {
        str(currency).upper()
        for source in source_config.get("sources") or []
        if isinstance(source, Mapping)
        and source.get("numeric_extraction_contract_id")
        and source.get("enabled", True) is not False
        for currency in source.get("currencies") or []
        if currency
    }
    technical_contract_active = bool(
        watchlist.get("research_only")
        and watchlist.get("cohort_id")
        and len(instruments) == int(quotes.get("quote_count") or len(instruments))
    )
    rows = build_currency_rows(
        instruments,
        source_audit.get("sources") or [],
        rate_currencies,
        baseline_currencies,
        macro_breakout_currencies,
        technical_contract_active,
        scheduled_counts,
        scheduled_policy_counts,
        numeric_adapter_currencies,
        rates.get("currencies") or {},
        as_of.date(),
        policy_driver_by_currency,
    )
    pair_rows = build_pair_rows(instruments, rows)
    totals = {
        "pairs": len(instruments),
        "currencies": len(rows),
        "technical_pairs": len(instruments) if technical_contract_active else 0,
        "currencies_with_policy": sum(bool(row["official_policy_sources"]) for row in rows),
        "currencies_with_macro": sum(bool(row["official_macro_sources"]) for row in rows),
        "currencies_with_numeric_parser": sum(bool(row["structured_numeric_parser"]) for row in rows),
        "currencies_with_prospective_actuals": sum(
            bool(row["prospective_actual_value_observations"]) for row in rows
        ),
        "currencies_with_causal_consensus": sum(bool(row["causal_pre_release_consensus"]) for row in rows),
        "currencies_with_daily_rates": sum(bool(row["daily_rate_context"]) for row in rows),
        "currencies_with_fresh_daily_rates": sum(bool(row["fresh_daily_rate_context"]) for row in rows),
        "currencies_with_comparable_2y_rates": sum(bool(row["daily_rate_comparable_2y"]) for row in rows),
        "currencies_with_policy_baseline": sum(bool(row["policy_delta_baseline"]) for row in rows),
        "currencies_with_macro_breakout": sum(bool(row["prospective_macro_breakout_adapter"]) for row in rows),
        "currencies_with_future_event_clock": sum(bool(row["future_scheduled_events"]) for row in rows),
        "currencies_with_future_policy_clock": sum(bool(row["future_scheduled_policy_decisions"]) for row in rows),
        "currencies_with_inherited_policy_clock": sum(bool(row["future_inherited_policy_decisions"]) for row in rows),
        "currencies_with_future_relevant_policy_clock": sum(bool(row["future_relevant_policy_decisions"]) for row in rows),
        "currencies_full_causal_stack": sum(bool(row["full_causal_stack"]) for row in rows),
        "pairs_with_numeric_parser_any_leg": sum(bool(row["numeric_parser_any_leg"]) for row in pair_rows),
        "pairs_with_numeric_parser_both_legs": sum(bool(row["numeric_parser_both_legs"]) for row in pair_rows),
        "pairs_with_actual_values_any_leg": sum(bool(row["actual_value_any_leg"]) for row in pair_rows),
        "pairs_with_actual_values_both_legs": sum(bool(row["actual_value_both_legs"]) for row in pair_rows),
        "pairs_with_prospective_actuals_any_leg": sum(bool(row["prospective_actual_any_leg"]) for row in pair_rows),
        "pairs_with_prospective_actuals_both_legs": sum(bool(row["prospective_actual_both_legs"]) for row in pair_rows),
        "pairs_with_daily_rates_any_leg": sum(bool(row["daily_rate_any_leg"]) for row in pair_rows),
        "pairs_with_daily_rates_both_legs": sum(bool(row["daily_rate_both_legs"]) for row in pair_rows),
        "pairs_with_fresh_daily_rates_any_leg": sum(bool(row["fresh_daily_rate_any_leg"]) for row in pair_rows),
        "pairs_with_fresh_daily_rates_both_legs": sum(bool(row["fresh_daily_rate_both_legs"]) for row in pair_rows),
        "pairs_with_comparable_2y_rates_any_leg": sum(bool(row["comparable_2y_rate_any_leg"]) for row in pair_rows),
        "pairs_with_comparable_2y_rates_both_legs": sum(bool(row["comparable_2y_rate_both_legs"]) for row in pair_rows),
        "pairs_with_causal_consensus_any_leg": sum(bool(row["causal_consensus_any_leg"]) for row in pair_rows),
        "pairs_with_causal_consensus_both_legs": sum(bool(row["causal_consensus_both_legs"]) for row in pair_rows),
        "pairs_with_future_event_clock_any_leg": sum(bool(row["future_event_clock_any_leg"]) for row in pair_rows),
        "pairs_with_future_event_clock_both_legs": sum(bool(row["future_event_clock_both_legs"]) for row in pair_rows),
        "pairs_full_causal_stack_both_legs": sum(bool(row["full_causal_stack_both_legs"]) for row in pair_rows),
    }
    payload = {
        "schema_version": 2,
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "research_only": True,
        "execution_eligible": False,
        "execution_decision": "no_trade",
        "totals": totals,
        "source_states": {
            "official_source_audit_generated_utc": source_audit.get("generated_utc"),
            "watchlist_cohort_id": watchlist.get("cohort_id"),
            "technical_signal_count": int(watchlist.get("technical_signal_count") or 0),
            "watchlist_entry_count": int(watchlist.get("watchlist_entry_count") or 0),
            "macro_consensus_status": consensus.get("status"),
            "macro_breakout_contract_id": macro_cohort.get("contract_id"),
            "currency_macro_breakout_contract_id": currency_macro_cohort.get("contract_id"),
            "policy_breakout_contract_id": policy_cohort.get("contract_id"),
            "linked_policy_driver_contract_id": policy_drivers.get("contract_id"),
        },
        "currencies": rows,
        "pairs": pair_rows,
    }
    news.atomic_write_json(output_json, payload)
    news.atomic_write_text(output_md, render_markdown(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-audit", type=Path, default=DEFAULT_SOURCE_AUDIT)
    parser.add_argument("--quotes", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--watchlist", type=Path, default=DEFAULT_WATCHLIST)
    parser.add_argument("--rates", type=Path, default=DEFAULT_RATES)
    parser.add_argument("--consensus", type=Path, default=DEFAULT_CONSENSUS)
    parser.add_argument("--macro-cohort", type=Path, default=DEFAULT_MACRO_COHORT)
    parser.add_argument(
        "--currency-macro-cohort",
        type=Path,
        default=DEFAULT_CURRENCY_MACRO_COHORT,
    )
    parser.add_argument("--policy-cohort", type=Path, default=DEFAULT_POLICY_COHORT)
    parser.add_argument("--policy-baselines", type=Path, default=DEFAULT_POLICY_BASELINES)
    parser.add_argument("--event-catalog", type=Path, default=DEFAULT_EVENT_CATALOG)
    parser.add_argument("--source-config", type=Path, default=DEFAULT_SOURCE_CONFIG)
    parser.add_argument("--policy-drivers", type=Path, default=DEFAULT_POLICY_DRIVERS)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_MD)
    args = parser.parse_args()
    payload = run(
        source_audit_path=args.source_audit,
        quotes_path=args.quotes,
        watchlist_path=args.watchlist,
        rates_path=args.rates,
        consensus_path=args.consensus,
        macro_cohort_path=args.macro_cohort,
        currency_macro_cohort_path=args.currency_macro_cohort,
        policy_cohort_path=args.policy_cohort,
        policy_baselines_path=args.policy_baselines,
        event_catalog_path=args.event_catalog,
        policy_drivers_path=args.policy_drivers,
        output_json=args.output_json,
        output_md=args.output_md,
        source_config_path=args.source_config,
    )
    print(json.dumps({"generated_utc": payload["generated_utc"], "totals": payload["totals"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
