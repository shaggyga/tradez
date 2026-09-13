#!/usr/bin/env python3
"""Build a neutral upcoming-event-to-technical preflight board.

Scheduled events are clocks, not forecasts.  This read-only research artifact
maps causally known official release clocks to every affected priced pair and
reports whether executable quotes and the independent technical snapshot are
ready.  It never assigns a direction, authorizes execution, or promotes a
hypothesis.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sqlite3
import statistics
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_local_news_sentiment as news


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
EVENTS = DATA / "news_event_tags" / "events_latest.json"
CONTEXT_ARTICLES = (
    DATA / "local_news_sentiment" / "context_articles_latest.json"
)
REFERENCE_DATABASE = (
    DATA / "local_news_sentiment" / "local_news_sentiment_v1.sqlite"
)
QUOTES = STATE / "practice_007_market_quotes_v1.json"
SIGNALS = STATE / "practice_007_signal_snapshot_research_v1.json"
COVERAGE = (
    DATA
    / "reports"
    / "currency_event_technical_coverage"
    / "CURRENCY_EVENT_TECHNICAL_COVERAGE_CURRENT.json"
)
DEPENDENCIES = ROOT / "config" / "currency_policy_dependency_registry_v1.json"
OFFICIAL_CENTRAL_BANK_SOURCES = (
    ROOT / "config" / "official_central_bank_source_map_v1.json"
)
SOURCE_COVERAGE = DATA / "local_news_sentiment" / "source_coverage_latest.json"
OUTPUT = STATE / "event_technical_preflight_v1.json"
REPORT = (
    DATA
    / "reports"
    / "currency_event_technical_coverage"
    / "EVENT_TECHNICAL_PREFLIGHT_CURRENT.md"
)
HISTORICAL_REPLAY = (
    DATA
    / "reports"
    / "direct_source_response"
    / "DIRECT_SOURCE_HISTORICAL_REPLAY_CURRENT.json"
)
RBNZ_SCHEDULE_HISTORY = (
    DATA
    / "reports"
    / "spike_blurb_factor_reconstruction"
    / "rbnz_schedule_cohort_v1"
    / "RBNZ_SCHEDULE_COHORT_V1.json"
)
UTC = dt.timezone.utc
SCHEMA_VERSION = "event_technical_preflight_v1"
CONTRACT_ID = (
    "neutral_event_clock_to_technical_preflight_v6_source_transport_readiness_20260901"
)
MOVEMENT_HORIZONS = (60, 300, 900, 3600)


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return default


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    atomic_text(path, json.dumps(payload, indent=2, sort_keys=True))


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def is_policy_event(event: Mapping[str, Any]) -> bool:
    text = " ".join(
        (
            str(event.get("category") or ""),
            str(event.get("headline") or ""),
            str(event.get("event_id") or ""),
        )
    ).lower()
    return any(
        token in text
        for token in (
            "monetary_policy",
            "monetary policy",
            "policy decision",
            "policy rate",
            "interest rate decision",
            "loan prime rate",
        )
    )


def scheduled_rows(
    events: Sequence[Mapping[str, Any]],
    observed: dt.datetime,
    lookahead_days: int,
) -> list[Mapping[str, Any]]:
    horizon = observed + dt.timedelta(days=max(1, lookahead_days))
    result: list[Mapping[str, Any]] = []
    for event in events:
        scheduled = news.parse_datetime(event.get("scheduled_utc"))
        if scheduled is None or scheduled < observed or scheduled > horizon:
            continue
        result.append(event)
    return sorted(
        result,
        key=lambda row: (
            str(row.get("scheduled_utc") or ""),
            str(row.get("event_id") or ""),
        ),
    )


def _currency_rows(payload: Mapping[str, Any] | None) -> dict[str, Mapping[str, Any]]:
    """Normalize list- or mapping-shaped currency inventories."""

    payload = payload or {}
    values = payload.get("currencies") or {}
    if isinstance(values, Mapping):
        return {
            str(currency).upper(): row
            for currency, row in values.items()
            if isinstance(row, Mapping)
        }
    return {
        str(row.get("currency") or "").upper(): row
        for row in values
        if isinstance(row, Mapping) and str(row.get("currency") or "").strip()
    }


def policy_release_transport_readiness(
    currency: str,
    official_sources: Mapping[str, Any] | None,
    source_coverage: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Describe primary/fallback policy-release transport without inventing readiness.

    Central-bank calendars prove when a release is due, not that its decision body
    can be observed at release time. Publisher-filtered discovery is retained as a
    separately labelled fallback and must never be represented as a direct source.
    """

    currency = str(currency or "").upper()
    official_by_currency = _currency_rows(official_sources)
    coverage_by_currency = _currency_rows(source_coverage)
    official = official_by_currency.get(currency) or {}
    coverage = coverage_by_currency.get(currency) or {}
    authority_id = str(official.get("authority_id") or "").lower()
    release_source_ids = sorted(
        {str(value) for value in official.get("release_source_ids") or [] if value}
    )
    calendar_source_ids = sorted(
        {str(value) for value in official.get("calendar_source_ids") or [] if value}
    )
    observed_sources = {
        str(row.get("source_id") or ""): row
        for row in coverage.get("sources") or []
        if isinstance(row, Mapping) and str(row.get("source_id") or "")
    }

    def operational(source_id: str) -> bool:
        row = observed_sources.get(source_id) or {}
        return bool(row.get("operational")) and bool(row.get("healthy"))

    direct_ready_ids = [source_id for source_id in release_source_ids if operational(source_id)]
    calendar_ready_ids = [source_id for source_id in calendar_source_ids if operational(source_id)]
    fallback_ids = sorted(
        source_id
        for source_id, row in observed_sources.items()
        if (
            str(row.get("source_role") or "") == "news_aggregator"
            and bool(row.get("operational"))
            and bool(row.get("healthy"))
            and authority_id
            and authority_id in source_id.lower()
        )
    )
    blockers = sorted(
        {
            f"{source_id}:{str((observed_sources.get(source_id) or {}).get('runtime_status') or 'not_observed')}"
            for source_id in release_source_ids
            if source_id not in direct_ready_ids
        }
    )
    if direct_ready_ids:
        state = "direct_release_ready"
    elif fallback_ids:
        state = "fallback_only_direct_release_unavailable"
    elif release_source_ids:
        state = "direct_release_unavailable_no_operational_fallback"
    else:
        state = "no_registered_direct_release_source"
    return {
        "policy_release_transport_state": state,
        "policy_direct_release_transport_available": bool(direct_ready_ids),
        "policy_fallback_transport_available": bool(fallback_ids),
        "policy_calendar_transport_available": bool(calendar_ready_ids),
        "policy_release_source_ids": release_source_ids,
        "policy_direct_release_ready_source_ids": direct_ready_ids,
        "policy_fallback_source_ids": fallback_ids,
        "policy_calendar_ready_source_ids": calendar_ready_ids,
        "policy_source_blockers": blockers,
    }


def event_direct_currencies(
    event: Mapping[str, Any],
    universe: Mapping[str, Any],
) -> list[str]:
    """Return authoritative direct event legs when lineage marks them known.

    Event tagging intentionally expands ``currencies`` with non-directional
    policy dependencies (for example, USD policy can affect linked currencies).
    Those legs are useful to the readiness board but are not direct event
    currencies.  Older event rows without the explicit knowledge flag retain
    the legacy ``currencies`` fallback; a known-empty direct set stays empty.
    """

    raw = event.get("raw") if isinstance(event.get("raw"), Mapping) else {}
    direct_known = event.get("direct_currencies_known") is True or str(
        raw.get("direct_currencies_known") or ""
    ).strip().lower() in {"1", "true"}
    values: Any = (
        event.get("direct_currencies")
        if direct_known
        else event.get("currencies")
    )
    if direct_known and values is None:
        values = raw.get("direct_currencies")
    if isinstance(values, str):
        values = re.split(r"[,;|\s]+", values)
    return sorted(
        {
            str(value).upper()
            for value in values or []
            if str(value).upper() in universe
        }
    )


def preserve_reference_periods(
    events: Sequence[Mapping[str, Any]],
    context_articles: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Restore calendar reference periods dropped by the event-tag projection.

    This is a read-only diagnostic join on frozen series identity plus exact
    scheduled clock. Conflicting source values fail closed to an empty period.
    """

    periods: dict[tuple[str, str], str] = {}
    conflicts: set[tuple[str, str]] = set()
    for article in context_articles:
        if not isinstance(article, Mapping):
            continue
        series_id = str(article.get("event_series_id") or "").strip()
        scheduled = news.parse_datetime(article.get("scheduled_utc"))
        period = str(article.get("reference_period") or "").strip()
        if not series_id or scheduled is None or not period:
            continue
        key = (series_id, iso(scheduled))
        prior = periods.get(key)
        if prior is not None and prior != period:
            conflicts.add(key)
            continue
        periods[key] = period
    output: list[dict[str, Any]] = []
    for event in events:
        item = dict(event)
        raw = item.get("raw") or {}
        series_id = str(
            item.get("event_series_id")
            or (raw.get("event_series_id") if isinstance(raw, Mapping) else "")
            or ""
        ).strip()
        scheduled = news.parse_datetime(item.get("scheduled_utc"))
        current = str(
            item.get("reference_period")
            or (raw.get("reference_period") if isinstance(raw, Mapping) else "")
            or ""
        ).strip()
        key = (series_id, iso(scheduled)) if series_id and scheduled else None
        if not current:
            item["reference_period"] = (
                periods.get(key, "")
                if key is not None and key not in conflicts
                else ""
            )
        output.append(item)
    return output


def canonical_calendar_articles(database_path: Path) -> list[dict[str, Any]]:
    """Read immutable calendar payloads that may age out of latest snapshots."""

    try:
        connection = sqlite3.connect(
            f"file:{database_path.as_posix()}?mode=ro", uri=True, timeout=5.0
        )
        rows = connection.execute(
            """
            SELECT payload_json
              FROM articles
             WHERE source_kind IN ('recurring_release_calendar',
                                   'census_release_calendar')
            """
        ).fetchall()
    except (OSError, sqlite3.Error):
        return []
    finally:
        if "connection" in locals():
            connection.close()
    output: list[dict[str, Any]] = []
    for (payload_text,) in rows:
        try:
            payload = json.loads(str(payload_text))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            output.append(payload)
    return output


def pair_universe(quotes: Mapping[str, Any]) -> dict[str, list[str]]:
    output: dict[str, list[str]] = {}
    for instrument in (quotes.get("quotes") or {}):
        legs = str(instrument).upper().split("_")
        if len(legs) != 2:
            continue
        for currency in legs:
            output.setdefault(currency, []).append(str(instrument).upper())
    return {key: sorted(set(values)) for key, values in output.items()}


def technical_index(signals: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(row.get("instrument") or "").upper(): row
        for row in signals.get("top_signals") or []
        if isinstance(row, Mapping) and row.get("instrument")
    }


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _movement_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    absolute = [
        value
        for row in rows
        if (value := _finite(row.get("median_absolute_currency_bps"))) is not None
    ]
    cost_clear = [
        value
        for row in rows
        if (value := _finite(row.get("cost_clear_pair_fraction"))) is not None
    ]
    return {
        "independent_episode_n": len(absolute),
        "mean_absolute_currency_bps": (
            sum(absolute) / len(absolute) if absolute else None
        ),
        "median_absolute_currency_bps": (
            statistics.median(absolute) if absolute else None
        ),
        "mean_cost_clear_pair_fraction": (
            sum(cost_clear) / len(cost_clear) if cost_clear else None
        ),
    }


def event_class(
    event_series_id: str, event_name: str, configured_category: str = ""
) -> str:
    """Return a coarse, predeclared economic class without reading outcomes."""

    text = " ".join((event_series_id, event_name, configured_category)).lower()
    # Labor must precede inflation because wage releases can be named price indices.
    if any(token in text for token in (
        "employment", "unemployment", "payroll", "jolts", "job opening",
        "labor", "labour", "wage",
    )):
        return "labor"
    if any(token in text for token in (
        "monetary_policy", "monetary policy", "cash rate", "policy decision",
        "policy rate", "interest rate", "loan prime rate", "_mpc", "ocr",
    )):
        return "policy"
    if any(token in text for token in (
        "cpi", "hicp", "cpif", "consumer price", "producer price", "ppi",
        "inflation", "price index",
    )):
        return "inflation"
    if any(token in text for token in (
        "housing", "home sales", "residential sales", "construction",
    )):
        return "housing"
    if any(token in text for token in (
        "sentiment", "pmi", "business activity", "consumer confidence", "ism",
    )):
        return "activity_sentiment"
    if any(token in text for token in (
        "trade", "export", "import", "retail", "wholesale", "manufacturing",
        "gdp", "sales", "orders", "production",
    )):
        return "growth_trade"
    return "other"


def canonical_series_key(event_series_id: str, event_name: str = "") -> str:
    """Normalize known provider labels for the same release series."""

    text = f"{event_series_id} {event_name}".lower()
    aliases = (
        (("new residential sales", "new home sales", "home_sales"), "home_sales"),
        (("new residential construction", "housing_starts"), "housing_starts"),
        (("rbnz_policy_decision", "new_zealand_official_cash_rate"), "rbnz_policy"),
    )
    for needles, replacement in aliases:
        if any(needle in text for needle in needles):
            return replacement
    return re.sub(r"[^a-z0-9]+", "_", event_series_id.lower()).strip("_")


def _prior_record(
    event_rows: Sequence[Mapping[str, Any]],
    control_rows: Sequence[Mapping[str, Any]],
    scope: str,
    event_class_name: str,
) -> dict[str, Any]:
    event_summary = _movement_summary(event_rows)
    control_summary = _movement_summary(control_rows)
    event_mean = _finite(event_summary["mean_absolute_currency_bps"])
    control_mean = _finite(control_summary["mean_absolute_currency_bps"])
    return {
        "scope": scope,
        "event_class": event_class_name,
        "direction_policy": "abstain",
        "historical_availability_only": True,
        "event": event_summary,
        "matched_control": control_summary,
        "event_minus_control_mean_absolute_bps": (
            event_mean - control_mean
            if event_mean is not None and control_mean is not None
            else None
        ),
    }


def _rbnz_schedule_magnitude_priors(
    payload: Mapping[str, Any],
    minimum_n: int = 2,
) -> tuple[dict[tuple[str, str, int], dict[str, Any]], dict[str, Any]]:
    """Adapt the frozen RBNZ schedule cohort into directionless magnitude priors.

    The schedule cohort deliberately selected every RBNZ decision in its
    closed interval, so its response trajectories are useful as a raw
    movement-risk baseline for the next RBNZ decision.  It does not contain a
    causal expectation/surprise variable.  This adapter therefore exposes
    absolute magnitude only, labels matched-control magnitude unavailable,
    and cannot assign a direction or execution authority.
    """

    expected_contract = "spike_blurb_rbnz_schedule_cohort_v1_20260820"
    expected_selection = (
        "every_scheduled_rbnz_policy_decision_20240228_through_20250820"
    )
    valid = bool(
        payload.get("contract_id") == expected_contract
        and payload.get("selection_rule") == expected_selection
        and payload.get("supported_execution_decision") == "no_trade"
        and int(payload.get("execution_eligible_count") or 0) == 0
        and int(payload.get("event_count") or 0) >= minimum_n
        and isinstance(payload.get("trajectory_rows"), list)
    )
    metadata = {
        "source_contract_id": str(payload.get("contract_id") or ""),
        "selection_rule": str(payload.get("selection_rule") or ""),
        "source_event_count": int(payload.get("event_count") or 0),
        "integrity_state": "accepted" if valid else "unavailable_or_invalid",
        "direction_policy": "abstain",
        "matched_control_magnitude_available": False,
        "research_only": True,
        "execution_eligible": False,
    }
    if not valid:
        return {}, metadata

    rows_by_horizon: dict[int, dict[str, Mapping[str, Any]]] = {
        horizon: {} for horizon in MOVEMENT_HORIZONS
    }
    for row in payload.get("trajectory_rows") or []:
        if not isinstance(row, Mapping):
            continue
        horizon = int(row.get("horizon_minutes") or 0) * 60
        event_id = str(row.get("event_id") or "")
        strength = _finite(row.get("currency_strength_bps"))
        if horizon not in rows_by_horizon or not event_id or strength is None:
            continue
        # Duplicate event/horizon rows cannot inflate independent N.
        rows_by_horizon[horizon].setdefault(
            event_id,
            {
                "median_absolute_currency_bps": abs(strength),
                "cost_clear_pair_fraction": None,
            },
        )

    priors: dict[tuple[str, str, int], dict[str, Any]] = {}
    for horizon, event_rows in rows_by_horizon.items():
        if len(event_rows) < minimum_n:
            continue
        event_summary = _movement_summary(list(event_rows.values()))
        priors[("NZD", "rbnz_policy", horizon)] = {
            "scope": "release_series_specialized_schedule_archive",
            "event_class": "policy",
            "direction_policy": "abstain",
            "historical_availability_only": True,
            "event": event_summary,
            "matched_control": _movement_summary([]),
            "matched_control_state": (
                "not_carried_into_raw_magnitude_prior; use the separately "
                "frozen placebo/inference report for strategy comparisons"
            ),
            "event_minus_control_mean_absolute_bps": None,
            "source_contract_id": expected_contract,
        }
    metadata["usable_horizon_count"] = len(priors)
    metadata["independent_event_n_by_horizon"] = {
        str(horizon): len(rows)
        for horizon, rows in rows_by_horizon.items()
        if len(rows) >= minimum_n
    }
    return priors, metadata


def movement_prior_index(
    historical: Mapping[str, Any],
    specialized_histories: Sequence[Mapping[str, Any]] = (),
    minimum_series_n: int = 2,
    minimum_class_currency_n: int = 2,
    minimum_class_global_n: int = 4,
) -> tuple[dict[str, dict[Any, dict[str, Any]]], dict[str, Any]]:
    """Build comparable-event magnitude priors, never direction priors.

    The old implementation fell back from sparse currencies to every official
    event.  That made a minor statistical release inherit a policy-decision
    prior.  This index instead keeps exact release-series and economic-class
    evidence separate and exposes no generic per-event fallback.
    """

    episode_rows = [
        row for row in historical.get("episode_horizon_rows") or []
        if isinstance(row, Mapping)
    ]
    details = [
        row for row in historical.get("details") or []
        if isinstance(row, Mapping)
    ]
    control_rows = [
        row for row in historical.get("independent_control_episode_horizon_rows") or []
        if isinstance(row, Mapping)
    ]
    currency_by_release: dict[str, str] = {}
    for row in details:
        release_key = str(row.get("release_key") or "")
        currency = str(row.get("currency") or "").upper()
        if release_key and currency:
            currency_by_release[release_key] = currency

    enriched: list[dict[str, Any]] = []
    for row in episode_rows:
        release_key = str(row.get("release_key") or "")
        currency = currency_by_release.get(release_key, "")
        series_id = str(row.get("event_series_id") or "")
        event_name = str(row.get("event_name") or "")
        if not release_key or not currency or not series_id:
            continue
        item = dict(row)
        item["currency"] = currency
        item["canonical_series_key"] = canonical_series_key(series_id, event_name)
        item["event_class"] = event_class(series_id, event_name)
        enriched.append(item)

    def controls_for(selected: Sequence[Mapping[str, Any]], horizon: int) -> list[Mapping[str, Any]]:
        release_keys = {str(row.get("release_key") or "") for row in selected}
        currencies = {str(row.get("currency") or "") for row in selected}
        return [
            row for row in control_rows
            if int(row.get("horizon_sec") or 0) == horizon
            and str(row.get("currency") or "") in currencies
            and any(
                str(key).removeprefix("control|") in release_keys
                for key in row.get("release_keys") or []
            )
        ]

    index: dict[str, dict[Any, dict[str, Any]]] = {
        "series": {}, "class_currency": {}, "class_global": {},
    }
    for horizon in MOVEMENT_HORIZONS:
        horizon_rows = [
            row for row in enriched if int(row.get("horizon_sec") or 0) == horizon
        ]
        series_keys = sorted({
            (str(row["currency"]), str(row["canonical_series_key"]))
            for row in horizon_rows
        })
        for currency, series_key in series_keys:
            selected = [
                row for row in horizon_rows
                if row["currency"] == currency
                and row["canonical_series_key"] == series_key
            ]
            if len(selected) >= minimum_series_n:
                class_name = str(selected[0]["event_class"])
                index["series"][(currency, series_key, horizon)] = _prior_record(
                    selected, controls_for(selected, horizon),
                    "release_series", class_name,
                )

        class_currency_keys = sorted({
            (str(row["currency"]), str(row["event_class"])) for row in horizon_rows
        })
        for currency, class_name in class_currency_keys:
            selected = [
                row for row in horizon_rows
                if row["currency"] == currency and row["event_class"] == class_name
            ]
            if len(selected) >= minimum_class_currency_n:
                index["class_currency"][(currency, class_name, horizon)] = _prior_record(
                    selected, controls_for(selected, horizon),
                    "event_class_currency", class_name,
                )

        for class_name in sorted({str(row["event_class"]) for row in horizon_rows}):
            selected = [row for row in horizon_rows if row["event_class"] == class_name]
            if len(selected) >= minimum_class_global_n:
                index["class_global"][(class_name, horizon)] = _prior_record(
                    selected, controls_for(selected, horizon),
                    "event_class_global", class_name,
                )

    global_baseline = {
        str(horizon): _prior_record(
            [row for row in enriched if int(row.get("horizon_sec") or 0) == horizon],
            [row for row in control_rows if int(row.get("horizon_sec") or 0) == horizon],
            "all_official_events_context_only",
            "all",
        )
        for horizon in MOVEMENT_HORIZONS
    }
    supplemental_metadata: list[dict[str, Any]] = []
    for specialized in specialized_histories:
        priors, supplemental = _rbnz_schedule_magnitude_priors(specialized)
        supplemental_metadata.append(supplemental)
        for key, prior in priors.items():
            # The strict direct-source replay remains preferred whenever it
            # already supplies the exact same series/horizon evidence.
            index["series"].setdefault(key, prior)
    metadata = {
        "source_contract": str(historical.get("evidence_class") or ""),
        "source_generated_utc": str(historical.get("generated_utc") or ""),
        "source_event_count": int(historical.get("source_event_count") or 0),
        "independent_source_time_count": int(
            historical.get("independent_source_time_count") or 0
        ),
        "direction_policy": "abstain",
        "selection_hierarchy": [
            "release_series_n_ge_2",
            "material_event_class_currency_n_ge_2",
            "material_event_class_global_n_ge_4",
            "otherwise_unavailable",
        ],
        "all_official_event_baseline_context_only": global_baseline,
        "generic_event_fallback_allowed": False,
        "specialized_history_adapters": supplemental_metadata,
        "research_only": True,
        "execution_eligible": False,
    }
    return index, metadata


def select_movement_priors(
    index: Mapping[str, Mapping[Any, dict[str, Any]]],
    *,
    currency: str,
    event_series_id: str,
    headline: str,
    category: str,
    severity: float | None,
    policy_event: bool,
) -> tuple[dict[str, dict[str, Any]], str, str]:
    """Select only comparable, sufficiently sampled magnitude evidence."""

    series_key = canonical_series_key(event_series_id, headline)
    class_name = event_class(event_series_id, headline, category)
    material_for_class_fallback = bool(
        policy_event or (severity is not None and severity >= 60.0)
    )
    selected: dict[str, dict[str, Any]] = {}
    for horizon in MOVEMENT_HORIZONS:
        prior = (index.get("series") or {}).get((currency, series_key, horizon))
        if prior is None and material_for_class_fallback:
            prior = (index.get("class_currency") or {}).get(
                (currency, class_name, horizon)
            )
        if prior is None and material_for_class_fallback:
            prior = (index.get("class_global") or {}).get((class_name, horizon))
        if prior is not None:
            selected[str(horizon)] = prior
    if selected:
        state = "comparable_historical_magnitude_prior_available_direction_abstains"
    elif not event_series_id:
        state = "missing_event_series_identity"
    elif not material_for_class_fallback:
        state = "no_exact_series_prior_and_event_below_class_fallback_threshold"
    else:
        state = "insufficient_comparable_historical_event_episodes"
    return selected, state, class_name


def build(
    events: Sequence[Mapping[str, Any]],
    quotes: Mapping[str, Any],
    signals: Mapping[str, Any],
    coverage: Mapping[str, Any],
    observed: dt.datetime,
    lookahead_days: int = 45,
    dependencies: Mapping[str, Any] | None = None,
    historical_magnitude: Mapping[str, Any] | None = None,
    specialized_histories: Sequence[Mapping[str, Any]] = (),
    official_sources: Mapping[str, Any] | None = None,
    source_coverage: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    universe = pair_universe(quotes)
    technical = technical_index(signals)
    quote_generated = news.parse_datetime(quotes.get("generated_utc"))
    signal_generated = news.parse_datetime(signals.get("updated_at"))
    quote_fresh = bool(
        quote_generated and (observed - quote_generated).total_seconds() <= 30
    )
    technical_fresh = bool(
        signal_generated and (observed - signal_generated).total_seconds() <= 360
    )
    coverage_by_currency = {
        str(row.get("currency") or ""): row
        for row in coverage.get("currencies") or []
        if isinstance(row, Mapping)
    }
    dependencies = dependencies or {}
    magnitude_index, magnitude_metadata = movement_prior_index(
        historical_magnitude or {}, specialized_histories
    )
    dependency_rows = [
        row
        for row in dependencies.get("dependencies") or []
        if isinstance(row, Mapping) and not bool(row.get("assign_direction"))
    ]
    rows: list[dict[str, Any]] = []
    for event in scheduled_rows(events, observed, lookahead_days):
        scheduled = news.parse_datetime(event.get("scheduled_utc"))
        if scheduled is None:
            continue
        direct_currencies = event_direct_currencies(event, universe)
        currencies: list[tuple[str, Mapping[str, Any] | None]] = [
            (currency, None) for currency in direct_currencies
        ]
        if is_policy_event(event):
            for dependency in dependency_rows:
                if str(dependency.get("driver_currency") or "").upper() not in direct_currencies:
                    continue
                dependent = str(dependency.get("dependent_currency") or "").upper()
                if dependent in universe and dependent not in direct_currencies:
                    currencies.append((dependent, dependency))
        for currency, dependency in currencies:
            pairs = universe.get(currency, [])
            technical_pairs = sorted(pair for pair in pairs if pair in technical)
            currency_coverage = coverage_by_currency.get(currency) or {}
            raw_event = event.get("raw") or {}
            event_series_id = str(
                event.get("event_series_id")
                or raw_event.get("event_series_id")
                or ""
            )
            policy_event = is_policy_event(event)
            severity = _finite(event.get("severity"))
            driver_currency = (
                currency
                if dependency is None
                else str(dependency.get("driver_currency") or "").upper()
            )
            movement_priors, movement_state, magnitude_event_class = (
                select_movement_priors(
                    magnitude_index,
                    currency=driver_currency,
                    event_series_id=event_series_id,
                    headline=str(event.get("headline") or ""),
                    category=str(event.get("category") or ""),
                    severity=severity,
                    policy_event=policy_event,
                )
            )
            source_readiness = (
                policy_release_transport_readiness(
                    driver_currency,
                    official_sources,
                    source_coverage,
                )
                if policy_event
                else {
                    "policy_release_transport_state": "not_policy_event",
                    "policy_direct_release_transport_available": False,
                    "policy_fallback_transport_available": False,
                    "policy_calendar_transport_available": False,
                    "policy_release_source_ids": [],
                    "policy_direct_release_ready_source_ids": [],
                    "policy_fallback_source_ids": [],
                    "policy_calendar_ready_source_ids": [],
                    "policy_source_blockers": [],
                }
            )
            rows.append(
                {
                    "event_id": str(event.get("event_id") or ""),
                    "event_series_id": event_series_id,
                    "reference_period": str(
                        event.get("reference_period")
                        or raw_event.get("reference_period")
                        or ""
                    ),
                    "headline": str(event.get("headline") or ""),
                    "category": str(event.get("category") or ""),
                    "severity": severity,
                    "movement_potential": str(event.get("movement_potential") or ""),
                    "currency": currency,
                    "direct_event_currency": dependency is None,
                    "driver_currency": driver_currency,
                    "policy_dependency": bool(dependency),
                    "policy_dependency_mechanism": str(
                        (dependency or {}).get("mechanism") or ""
                    ),
                    "policy_dependency_reference": str(
                        (dependency or {}).get("official_reference") or ""
                    ),
                    "scheduled_utc": iso(scheduled),
                    "minutes_until_event": round(
                        (scheduled - observed).total_seconds() / 60.0, 3
                    ),
                    "timing_precision": str(
                        event.get("timing_precision")
                        or (event.get("raw") or {}).get("timing_precision")
                        # Missing precision is not evidence of an exact clock.
                        # Older catalog rows can predate preservation of this
                        # field, so fail closed as unknown until recollected.
                        or "unknown"
                    ),
                    "schedule_window_end_utc": str(
                        event.get("schedule_window_end_utc")
                        or (event.get("raw") or {}).get("schedule_window_end_utc")
                        or ""
                    ),
                    "policy_event": policy_event,
                    "pair_count": len(pairs),
                    "pairs": pairs,
                    "technical_pair_count": len(technical_pairs),
                    "technical_pairs": technical_pairs,
                    "quote_snapshot_fresh": quote_fresh,
                    "technical_snapshot_fresh": technical_fresh,
                    "causal_consensus_available": bool(
                        currency_coverage.get("causal_pre_release_consensus")
                    ),
                    "daily_rate_context_available": bool(
                        currency_coverage.get("daily_rate_context")
                    ),
                    "readiness_state": (
                        "ready_for_neutral_post_release_verification"
                        if quote_fresh and technical_fresh and technical_pairs
                        else "await_market_open"
                        if not quote_fresh
                        else "await_fresh_technical_snapshot"
                    ),
                    "direction": "unknown_until_causal_release_evidence",
                    "movement_risk_state": movement_state,
                    "movement_risk_event_class": magnitude_event_class,
                    "movement_risk_driver_currency": driver_currency,
                    "movement_risk_priors": movement_priors,
                    **source_readiness,
                    "research_only": True,
                    "execution_eligible": False,
                }
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "generated_utc": iso(observed),
        "lookahead_days": lookahead_days,
        "priced_pair_count": sum(1 for _ in (quotes.get("quotes") or {})),
        "priced_currency_count": len(universe),
        "quote_snapshot_fresh": quote_fresh,
        "technical_snapshot_fresh": technical_fresh,
        "scheduled_currency_event_rows": len(rows),
        "scheduled_policy_currency_rows": sum(
            1 for row in rows if row["policy_event"]
        ),
        "derived_policy_dependency_rows": sum(
            1 for row in rows if row["policy_dependency"]
        ),
        "policy_direct_release_ready_rows": sum(
            1
            for row in rows
            if row["policy_event"]
            and row["policy_direct_release_transport_available"]
        ),
        "policy_fallback_only_rows": sum(
            1
            for row in rows
            if row["policy_event"]
            and row["policy_release_transport_state"]
            == "fallback_only_direct_release_unavailable"
        ),
        "policy_release_transport_blocked_rows": sum(
            1
            for row in rows
            if row["policy_event"]
            and row["policy_release_transport_state"]
            in {
                "direct_release_unavailable_no_operational_fallback",
                "no_registered_direct_release_source",
            }
        ),
        "events": rows,
        "historical_magnitude_prior": magnitude_metadata,
        "policy": {
            "calendar_is_not_direction": True,
            "no_consensus_is_not_neutral_consensus": True,
            "technical_confirmation_cannot_self_authorize": True,
            "can_place_orders": False,
            "can_promote": False,
            "dependency_clock_is_not_independent_decision": True,
            "dependency_clock_assigns_no_direction": True,
            "historical_magnitude_prior_assigns_no_direction": True,
            "historical_magnitude_prior_cannot_self_authorize": True,
            "calendar_readiness_is_not_release_transport_readiness": True,
            "publisher_search_fallback_is_not_direct_release_transport": True,
        },
        "research_only": True,
        "execution_eligible": False,
    }


def render(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Event + Technical Preflight",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Scheduled events are neutral clocks, not directional signals. This board cannot trade or promote.",
        "",
        f"- Pair/currency universe: **{payload['priced_pair_count']} / {payload['priced_currency_count']}**",
        f"- Scheduled event/policy currency rows: **{payload['scheduled_currency_event_rows']} / {payload['scheduled_policy_currency_rows']}**",
        f"- Derived policy-dependency rows: **{payload['derived_policy_dependency_rows']}**",
        f"- Policy direct-ready / fallback-only / blocked rows: "
        f"**{payload['policy_direct_release_ready_rows']} / "
        f"{payload['policy_fallback_only_rows']} / "
        f"{payload['policy_release_transport_blocked_rows']}**",
        f"- Quote/technical snapshots fresh: **{payload['quote_snapshot_fresh']} / {payload['technical_snapshot_fresh']}**",
        "",
        "| Scheduled UTC | Currency | Event | Policy | Dependency | Precision | Pair legs | Technical legs | H1 risk prior | Consensus | Rates | Source transport | Market readiness |",
        "|---|---|---|---|---|---|---:|---:|---|---|---|---|---|",
    ]
    for row in payload["events"]:
        lines.append(
            f"| {row['scheduled_utc']} | {row['currency']} | {row['headline']} | "
            f"{'yes' if row['policy_event'] else 'no'} | "
            f"{'yes' if row['policy_dependency'] else 'no'} | {row['timing_precision']} | "
            f"{row['pair_count']} | {row['technical_pair_count']} | "
            f"{((row.get('movement_risk_priors') or {}).get('3600') or {}).get('event_minus_control_mean_absolute_bps')} | "
            f"{'yes' if row['causal_consensus_available'] else 'no'} | "
            f"{'yes' if row['daily_rate_context_available'] else 'no'} | "
            f"{row['policy_release_transport_state']} | "
            f"{row['readiness_state']} |"
        )
    lines.extend(
        [
            "",
            "When the market is open, the board may become ready for neutral post-release verification. Direction still requires causal release evidence or observed post-release price confirmation under a frozen shadow cohort.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    events_path: Path = EVENTS,
    quotes_path: Path = QUOTES,
    signals_path: Path = SIGNALS,
    coverage_path: Path = COVERAGE,
    dependencies_path: Path = DEPENDENCIES,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
    lookahead_days: int = 45,
    observed: dt.datetime | None = None,
    historical_path: Path = HISTORICAL_REPLAY,
    rbnz_schedule_history_path: Path = RBNZ_SCHEDULE_HISTORY,
    context_articles_path: Path = CONTEXT_ARTICLES,
    reference_database_path: Path = REFERENCE_DATABASE,
    official_sources_path: Path = OFFICIAL_CENTRAL_BANK_SOURCES,
    source_coverage_path: Path = SOURCE_COVERAGE,
) -> dict[str, Any]:
    observed = (observed or dt.datetime.now(UTC)).astimezone(UTC)
    events = read_json(events_path, [])
    if isinstance(events, Mapping):
        events = events.get("events") or []
    context = read_json(context_articles_path, {})
    if isinstance(context, Mapping):
        context = context.get("articles") or []
    events = preserve_reference_periods(
        events,
        [*context, *canonical_calendar_articles(reference_database_path)],
    )
    payload = build(
        events,
        read_json(quotes_path, {}),
        read_json(signals_path, {}),
        read_json(coverage_path, {}),
        observed,
        lookahead_days,
        read_json(dependencies_path, {}),
        read_json(historical_path, {}),
        [read_json(rbnz_schedule_history_path, {})],
        read_json(official_sources_path, {}),
        read_json(source_coverage_path, {}),
    )
    atomic_json(output_path, payload)
    atomic_text(report_path, render(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, default=EVENTS)
    parser.add_argument("--quotes", type=Path, default=QUOTES)
    parser.add_argument("--signals", type=Path, default=SIGNALS)
    parser.add_argument("--coverage", type=Path, default=COVERAGE)
    parser.add_argument("--dependencies", type=Path, default=DEPENDENCIES)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--lookahead-days", type=int, default=45)
    parser.add_argument("--historical-replay", type=Path, default=HISTORICAL_REPLAY)
    parser.add_argument(
        "--rbnz-schedule-history", type=Path, default=RBNZ_SCHEDULE_HISTORY
    )
    parser.add_argument("--context-articles", type=Path, default=CONTEXT_ARTICLES)
    parser.add_argument(
        "--reference-database", type=Path, default=REFERENCE_DATABASE
    )
    parser.add_argument(
        "--official-central-bank-sources",
        type=Path,
        default=OFFICIAL_CENTRAL_BANK_SOURCES,
    )
    parser.add_argument(
        "--source-coverage", type=Path, default=SOURCE_COVERAGE
    )
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        payload = run(
            args.events,
            args.quotes,
            args.signals,
            args.coverage,
            args.dependencies,
            args.output,
            args.report,
            args.lookahead_days,
            historical_path=args.historical_replay,
            rbnz_schedule_history_path=args.rbnz_schedule_history,
            context_articles_path=args.context_articles,
            reference_database_path=args.reference_database,
            official_sources_path=args.official_central_bank_sources,
            source_coverage_path=args.source_coverage,
        )
        if args.interval_sec <= 0 or (
            args.duration_sec > 0
            and time.monotonic() - started >= args.duration_sec
        ):
            break
        time.sleep(max(5.0, args.interval_sec))
    print(json.dumps({key: payload[key] for key in (
        "generated_utc", "priced_pair_count", "priced_currency_count",
        "scheduled_currency_event_rows", "scheduled_policy_currency_rows",
        "policy_direct_release_ready_rows", "policy_fallback_only_rows",
        "policy_release_transport_blocked_rows",
        "quote_snapshot_fresh", "technical_snapshot_fresh",
    )}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
