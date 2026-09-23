#!/usr/bin/env python3
"""Build a conservative official-source readiness census.

V1 answers whether a free official transport is configured.  This V2 report
keeps that useful inventory, but deliberately separates it from the much
smaller set of source/event-family cells that have a structured parser, have
actually emitted an exact numeric value, have a prospective causal record,
have a matured market-response outcome, or have emitted semantic direction.

The artifact is diagnostic and research-only.  It cannot trade, authorize,
promote, or contribute currency strength.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping


ROOT = Path(__file__).resolve().parent
DEPTH_CONFIG = ROOT / "config" / "official_currency_source_depth_v1.json"
CENTRAL_BANK_CONFIG = ROOT / "config" / "official_central_bank_source_map_v1.json"
NEWS_CONFIG = ROOT / "config" / "news_sources_v1.json"
RATE_CONFIG = ROOT / "config" / "official_daily_rate_context_v1.json"
SOURCE_COVERAGE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "source_coverage_latest.json"
)
FAST_MAPPING_SNAPSHOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "official_release_fast_mapping_latest_v3.json"
)
FAST_MAPPING_DATABASE = FAST_MAPPING_SNAPSHOT.with_name(
    "official_release_fast_mapping_v3.sqlite"
)
RESPONSE_DATABASE = FAST_MAPPING_SNAPSHOT.with_name(
    "official_release_fast_response_watch_v3.sqlite"
)
CAUSAL_RESPONSE_MAP_DATABASE = FAST_MAPPING_SNAPSHOT.with_name(
    "causal_source_factor_response_map_v2.sqlite"
)
RATE_STATE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "official_daily_rate_context_v1.json"
)
RATE_DATABASE = RATE_STATE.with_suffix(".sqlite")
REPORT_DIR = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "official_currency_source_depth_readiness_v2"
)
JSON_REPORT = REPORT_DIR / "OFFICIAL_CURRENCY_SOURCE_DEPTH_READINESS_V2_CURRENT.json"
MD_REPORT = REPORT_DIR / "OFFICIAL_CURRENCY_SOURCE_DEPTH_READINESS_V2_CURRENT.md"

EVENT_FAMILIES = (
    "policy",
    "inflation",
    "labour",
    "growth",
    "trade",
    "intervention_reserves",
    "market_rates",
    "fiscal_debt",
)

# This is intentionally an allowlist of implemented numeric parser scope.  A
# source being listed under four V1 transport categories does not mean its one
# narrow parser understands all four categories.
PARSER_FAMILIES_BY_SOURCE: dict[str, tuple[str, ...]] = {
    "abs_latest_releases": ("inflation", "labour", "growth"),
    "statcan_daily_releases": ("labour", "growth"),
    "swiss_fso_releases": ("inflation",),
    "china_nbs_latest_releases_direct_v1": ("inflation", "growth"),
    "czech_cnb_inflation_snapshot_direct_v1": ("inflation",),
    "denmark_statbank_cpi_yoy_table_direct_v1": ("inflation",),
    "eurostat_economy_finance": ("inflation", "growth"),
    "ons_published_releases": ("inflation", "labour", "growth"),
    "hong_kong_censtatd_cpi_yoy_table_direct_v1": ("inflation",),
    "hungary_ksh_cpi_snapshot_direct_v1": ("inflation",),
    "japan_cpi_national_yoy_csv": ("inflation",),
    "japan_cpi_current_summary_direct_v1": ("inflation",),
    "mexico_banxico_inflation_snapshot_direct_v1": ("inflation",),
    "norway_ssb_cpi_yoy_table_direct_v1": ("inflation",),
    "stats_nz_releases": ("inflation", "labour"),
    "new_zealand_rbnz_ocr_snapshot_direct_v1": ("policy",),
    "poland_gus_economic_releases_direct_v1": ("inflation", "growth"),
    "sweden_scb_cpif_snapshot_direct_v1": ("inflation",),
    "singstat_cpi_yoy_table_direct_v1": ("inflation",),
    "thailand_bot_sdds_cpi_index_direct_v1": ("inflation",),
    "turkey_tuik_cpi_indicator_direct_v1": ("inflation",),
    "bls_ppi_current_release_v1": ("inflation",),
    "dol_eta_ui_claims_scheduled_direct_pdf_v1": ("labour",),
    "census_economic_indicators": ("growth", "trade"),
    "south_africa_sarb_cpi_direct_v1": ("inflation",),
    "stats_sa_ppi_scheduled_direct_pdf_v1": ("inflation",),
    "south_africa_sarb_policy_rate_direct_v1": ("policy",),
}

CATEGORY_TO_FAMILY = {
    "monetary_policy": "policy",
    "policy_rate_release": "policy",
    "inflation_release": "inflation",
    "labor_release": "labour",
    "labour_release": "labour",
    "growth_release": "growth",
    "manufacturing_release": "growth",
    "trade_balance_release": "trade",
    "fx_intervention": "intervention_reserves",
    "reserves_release": "intervention_reserves",
    "market_rate_release": "market_rates",
    "fiscal_release": "fiscal_debt",
}


def read_json(path: Path, *, required: bool = True) -> dict[str, Any]:
    if not path.exists():
        if required:
            raise FileNotFoundError(path)
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError(f"expected object: {path}")
    return dict(value)


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _open_readonly(database_path: Path) -> sqlite3.Connection:
    uri = f"file:{database_path.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=10.0)
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=10000")
    return connection


def event_family(payload: Mapping[str, Any]) -> str | None:
    category = str(payload.get("category") or "").strip().lower()
    family = CATEGORY_TO_FAMILY.get(category)
    if family:
        return family
    series = str(payload.get("event_series_id") or "").strip().lower()
    if any(token in series for token in ("cpi", "ppi", "inflation", "price_index")):
        return "inflation"
    if any(
        token in series
        for token in (
            "employment",
            "unemployment",
            "labour",
            "labor",
            "payroll",
            "claims",
            "wage",
        )
    ):
        return "labour"
    if any(token in series for token in ("trade", "import", "export", "current_account")):
        return "trade"
    if any(
        token in series
        for token in (
            "gdp",
            "retail",
            "wholesale",
            "manufactur",
            "orders",
            "sales",
            "housing",
        )
    ):
        return "growth"
    if any(token in series for token in ("policy_rate", "ocr", "repo_rate")):
        return "policy"
    return None


def source_health(coverage: Mapping[str, Any]) -> dict[str, dict[str, bool]]:
    result: dict[str, dict[str, bool]] = {}
    currencies = coverage.get("currencies")
    if not isinstance(currencies, Mapping):
        return result
    for currency in currencies.values():
        if not isinstance(currency, Mapping):
            continue
        for row in currency.get("sources") or []:
            if not isinstance(row, Mapping) or not row.get("source_id"):
                continue
            source_id = str(row["source_id"])
            current = result.setdefault(source_id, {"operational": False, "healthy": False})
            current["operational"] = current["operational"] or bool(row.get("operational"))
            current["healthy"] = current["healthy"] or bool(row.get("healthy"))
    return result


def rate_health(state: Mapping[str, Any]) -> dict[str, dict[str, bool]]:
    errors = ((state.get("cycle") or {}).get("errors") or {})
    currencies = state.get("currencies") or {}
    result: dict[str, dict[str, bool]] = {}
    if not isinstance(currencies, Mapping):
        return result
    for row in currencies.values():
        if not isinstance(row, Mapping) or not row.get("source_id"):
            continue
        source_id = str(row["source_id"])
        operational = bool(row.get("observation_id") and _finite(row.get("rate_pct")))
        result[source_id] = {
            "operational": operational,
            "healthy": bool(operational and source_id not in errors),
        }
    return result


def load_current_mapping_rows(
    snapshot_path: Path = FAST_MAPPING_SNAPSHOT,
    database_path: Path = FAST_MAPPING_DATABASE,
) -> list[dict[str, Any]]:
    if not snapshot_path.exists() or not database_path.exists():
        return []
    snapshot = read_json(snapshot_path)
    version = str(snapshot.get("required_classification_version") or "")
    contract = str(snapshot.get("mapper_contract_id") or "")
    if not version or not contract:
        return []
    connection = _open_readonly(database_path)
    try:
        rows = connection.execute(
            """
            SELECT mapping_id,observation_id,source_id,first_seen_utc,
                   input_prospective_observation,input_listing_bootstrap,
                   input_publisher_time_eligible,semantic_direction_available,
                   mapping_payload_json
            FROM official_release_mapping
            WHERE classification_version=? AND mapper_contract_id=?
            ORDER BY first_seen_utc,mapping_id
            """,
            (version, contract),
        ).fetchall()
    finally:
        connection.close()
    output: list[dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(str(row[8]))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(payload, Mapping):
            continue
        output.append(
            {
                "mapping_id": str(row[0]),
                "observation_id": str(row[1]),
                "source_id": str(row[2]),
                "first_seen_utc": str(row[3]),
                "prospective": bool(row[4]),
                "listing_bootstrap": bool(row[5]),
                "publisher_time_eligible": bool(row[6]),
                "semantic_direction": bool(row[7]),
                "payload": dict(payload),
            }
        )
    return output


def load_rate_counts(
    database_path: Path = RATE_DATABASE,
    state_path: Path = RATE_STATE,
) -> dict[str, dict[str, Any]]:
    if not database_path.exists():
        return {}
    state = read_json(state_path, required=False)
    active_cohorts = sorted(
        {
            str(row.get("cohort_id"))
            for row in (state.get("currencies") or {}).values()
            if isinstance(row, Mapping) and row.get("cohort_id")
        }
    )
    if not active_cohorts:
        return {}
    placeholders = ",".join("?" for _ in active_cohorts)
    connection = _open_readonly(database_path)
    try:
        rows = connection.execute(
            f"""
            SELECT source_id,currency,COUNT(*),
                   COALESCE(SUM(prospective_eligible),0)
            FROM daily_rate_observations
            WHERE cohort_id IN ({placeholders})
            GROUP BY source_id,currency
            """,
            active_cohorts,
        ).fetchall()
    finally:
        connection.close()
    return {
        str(row[0]): {
            "currency": str(row[1]),
            "exact_numeric_observations": int(row[2]),
            "prospective_causal_observations": int(row[3]),
        }
        for row in rows
    }


def load_valid_outcome_mapping_ids(database_path: Path = RESPONSE_DATABASE) -> list[str]:
    if not database_path.exists():
        return []
    connection = _open_readonly(database_path)
    try:
        rows = connection.execute(
            """
            SELECT w.mapping_id
            FROM response_outcome o
            JOIN response_watch w ON w.watch_id=o.watch_id
            WHERE o.maturity_state='valid_exact_executable_quote'
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        connection.close()
    return [str(row[0]) for row in rows]


def load_causal_response_event_counts(
    database_path: Path = CAUSAL_RESPONSE_MAP_DATABASE,
) -> tuple[bool, dict[tuple[str, str], int]]:
    """Return distinct proof-event counts from the causal response-map ledger.

    The EXISTS clause deliberately prevents the response horizons from
    multiplying one canonical event.  Preactivation/diagnostic events and
    events without an exact prospective entry are excluded fail-closed.
    """

    if not database_path.exists():
        return False, {}
    connection = _open_readonly(database_path)
    try:
        rows = connection.execute(
            """
            SELECT e.canonical_event_id,e.currency,e.category,e.event_series_id
            FROM source_event_observation e
            WHERE e.evidence_class='prospective_v1'
              AND e.prospective_proof_eligible=1
              AND EXISTS (
                  SELECT 1
                  FROM source_event_response r
                  WHERE r.canonical_event_id=e.canonical_event_id
                    AND r.maturity_state='valid_canonical_ls_factor_and_bid_ask_paths'
                    AND r.response_timing_quality='prospective_exact_entry_completed_m1_exit'
              )
            ORDER BY e.canonical_event_id
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return True, {}
    finally:
        connection.close()
    event_ids: dict[tuple[str, str], set[str]] = defaultdict(set)
    for canonical_event_id, currency, category, series in rows:
        family = event_family(
            {"category": category, "event_series_id": series}
        )
        if family in EVENT_FAMILIES:
            event_ids[(str(currency), str(family))].add(str(canonical_event_id))
    return True, {key: len(values) for key, values in event_ids.items()}


def _currency_for_mapping(
    mapping: Mapping[str, Any], news_sources: Mapping[str, Mapping[str, Any]]
) -> str | None:
    source = news_sources.get(str(mapping.get("source_id") or "")) or {}
    currencies = [str(value) for value in source.get("currencies") or []]
    if len(currencies) == 1:
        return currencies[0]
    payload = mapping.get("payload") or {}
    candidates = [str(value) for value in payload.get("source_currencies") or []]
    return candidates[0] if len(candidates) == 1 else None


def build_report(
    *,
    depth: Mapping[str, Any] | None = None,
    central: Mapping[str, Any] | None = None,
    news: Mapping[str, Any] | None = None,
    rates: Mapping[str, Any] | None = None,
    coverage: Mapping[str, Any] | None = None,
    rate_state_payload: Mapping[str, Any] | None = None,
    mappings: Iterable[Mapping[str, Any]] | None = None,
    rate_counts: Mapping[str, Mapping[str, Any]] | None = None,
    valid_outcome_mapping_ids: Iterable[str] | None = None,
    causal_outcome_counts: Mapping[tuple[str, str], int] | None = None,
    causal_outcome_database_present: bool | None = None,
) -> dict[str, Any]:
    depth = dict(depth or read_json(DEPTH_CONFIG))
    central = dict(central or read_json(CENTRAL_BANK_CONFIG))
    news = dict(news or read_json(NEWS_CONFIG))
    rates = dict(rates or read_json(RATE_CONFIG))
    coverage = dict(coverage or read_json(SOURCE_COVERAGE, required=False))
    rate_state_payload = dict(rate_state_payload or read_json(RATE_STATE, required=False))
    mapping_rows = list(mappings) if mappings is not None else load_current_mapping_rows()
    rate_count_rows = dict(rate_counts) if rate_counts is not None else load_rate_counts()
    if causal_outcome_counts is None:
        causal_present, loaded_causal_counts = load_causal_response_event_counts()
    else:
        causal_present = (
            True
            if causal_outcome_database_present is None
            else bool(causal_outcome_database_present)
        )
        loaded_causal_counts = {
            (str(key[0]), str(key[1])): int(value)
            for key, value in causal_outcome_counts.items()
        }
    if causal_outcome_database_present is not None:
        causal_present = bool(causal_outcome_database_present)
    outcome_ids = set()
    if not causal_present:
        outcome_ids = set(
            valid_outcome_mapping_ids
            if valid_outcome_mapping_ids is not None
            else load_valid_outcome_mapping_ids()
        )
    outcome_source = (
        "causal_source_factor_response_map_v2_distinct_prospective_proof_events"
        if causal_present
        else "official_release_fast_response_watch_v3_distinct_mapping_fallback"
    )

    expected = [str(value) for value in central.get("frozen_currency_universe") or []]
    if len(expected) != 21:
        raise ValueError("expected_exact_21_currency_universe")
    central_rows = {
        str(row.get("currency")): dict(row)
        for row in central.get("currencies") or []
        if isinstance(row, Mapping) and row.get("currency")
    }
    depth_rows = {
        str(row.get("currency")): dict(row)
        for row in depth.get("currencies") or []
        if isinstance(row, Mapping) and row.get("currency")
    }
    if set(expected) != set(central_rows) or set(expected) != set(depth_rows):
        raise ValueError("currency_universe_mismatch")
    news_sources = {
        str(row.get("source_id")): dict(row)
        for row in news.get("sources") or []
        if isinstance(row, Mapping) and row.get("source_id")
    }
    rate_sources = {
        str(row.get("source_id")): dict(row)
        for row in rates.get("sources") or []
        if isinstance(row, Mapping) and row.get("source_id")
    }
    health = source_health(coverage)
    health.update(rate_health(rate_state_payload))

    cell_sources: dict[tuple[str, str], set[str]] = defaultdict(set)
    for currency in expected:
        cell_sources[(currency, "policy")].update(
            str(value) for value in central_rows[currency].get("release_source_ids") or []
        )
        for family in EVENT_FAMILIES[1:]:
            cell_sources[(currency, family)].update(
                str(value) for value in depth_rows[currency].get(family) or []
            )
        related = {
            str(value)
            for key in ("release_source_ids", "statistical_release_source_ids")
            for value in central_rows[currency].get(key) or []
        }
        for source_id in related:
            for family in PARSER_FAMILIES_BY_SOURCE.get(source_id, ()):
                cell_sources[(currency, family)].add(source_id)

    evidence: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "exact_numeric_observations": 0,
            "exact_numeric_source_ids": set(),
            "prospective_causal_observations": 0,
            "prospective_causal_outcomes": 0,
            "semantic_direction_observations": 0,
            "prospective_semantic_direction_observations": 0,
        }
    )
    mapping_index: dict[str, tuple[str, str]] = {}
    excluded_unmapped = 0
    for mapping in mapping_rows:
        payload = mapping.get("payload") or {}
        if not isinstance(payload, Mapping):
            continue
        family = event_family(payload)
        currency = _currency_for_mapping(mapping, news_sources)
        if family not in EVENT_FAMILIES or currency not in expected:
            excluded_unmapped += 1
            continue
        key = (str(currency), str(family))
        mapping_index[str(mapping.get("mapping_id") or "")] = key
        row = evidence[key]
        exact = bool(
            payload.get("structured_event")
            and _finite(payload.get("actual_value"))
            and payload.get("numeric_causal_known_utc")
        )
        if exact:
            row["exact_numeric_observations"] += 1
            row["exact_numeric_source_ids"].add(str(mapping.get("source_id") or ""))
        prospective = bool(
            mapping.get("prospective")
            and not mapping.get("listing_bootstrap")
            and mapping.get("publisher_time_eligible")
        )
        if prospective:
            row["prospective_causal_observations"] += 1
        if mapping.get("semantic_direction"):
            row["semantic_direction_observations"] += 1
            if prospective:
                row["prospective_semantic_direction_observations"] += 1

    if causal_present:
        for key, count in loaded_causal_counts.items():
            if key[0] in expected and key[1] in EVENT_FAMILIES:
                evidence[key]["prospective_causal_outcomes"] += max(0, int(count))
    else:
        for mapping_id in outcome_ids:
            key = mapping_index.get(str(mapping_id))
            if key:
                evidence[key]["prospective_causal_outcomes"] += 1

    for source_id, counts in rate_count_rows.items():
        currency = str(counts.get("currency") or "")
        if currency not in expected:
            continue
        key = (currency, "market_rates")
        evidence[key]["exact_numeric_observations"] += int(
            counts.get("exact_numeric_observations") or 0
        )
        if int(counts.get("exact_numeric_observations") or 0) > 0:
            evidence[key]["exact_numeric_source_ids"].add(str(source_id))
        evidence[key]["prospective_causal_observations"] += int(
            counts.get("prospective_causal_observations") or 0
        )

    cells: list[dict[str, Any]] = []
    known_sources = set(news_sources) | set(rate_sources)
    for currency in expected:
        for family in EVENT_FAMILIES:
            source_ids = sorted(cell_sources[(currency, family)])
            unknown = sorted(set(source_ids) - known_sources)
            if unknown:
                raise ValueError(f"unknown_source_ids:{currency}:{family}:{unknown}")
            configured_ids: list[str] = []
            parser_ids: list[str] = []
            for source_id in source_ids:
                source = news_sources.get(source_id)
                if source is not None:
                    source_currencies = {str(value) for value in source.get("currencies") or []}
                    configured = bool(
                        source.get("verified") is True
                        and currency in source_currencies
                    )
                    parser = bool(
                        configured
                        and source.get("numeric_extraction_contract_id")
                        and family in PARSER_FAMILIES_BY_SOURCE.get(source_id, ())
                    )
                else:
                    source = rate_sources[source_id]
                    configured = bool(str(source.get("currency") or "") == currency)
                    parser = bool(configured and family == "market_rates")
                if configured:
                    configured_ids.append(source_id)
                if parser:
                    parser_ids.append(source_id)
            facts = evidence[(currency, family)]
            exact_source_ids = sorted(facts["exact_numeric_source_ids"])
            cells.append(
                {
                    "currency": currency,
                    "event_family": family,
                    "source_ids": source_ids,
                    "configured_transport_source_ids": configured_ids,
                    "operational_source_ids": [
                        value for value in configured_ids if health.get(value, {}).get("operational")
                    ],
                    "healthy_source_ids": [
                        value for value in configured_ids if health.get(value, {}).get("healthy")
                    ],
                    "structured_parser_source_ids": sorted(parser_ids),
                    "exact_numeric_source_ids": exact_source_ids,
                    "exact_numeric_observations": int(facts["exact_numeric_observations"]),
                    "prospective_causal_observations": int(
                        facts["prospective_causal_observations"]
                    ),
                    "prospective_causal_outcomes": int(
                        facts["prospective_causal_outcomes"]
                    ),
                    "semantic_direction_observations": int(
                        facts["semantic_direction_observations"]
                    ),
                    "prospective_semantic_direction_observations": int(
                        facts["prospective_semantic_direction_observations"]
                    ),
                }
            )

    def cells_with(field: str) -> int:
        if field.endswith("_ids"):
            return sum(bool(row[field]) for row in cells)
        return sum(int(row[field]) > 0 for row in cells)

    stage_cells = {
        "configured_transport": cells_with("configured_transport_source_ids"),
        "operational": cells_with("operational_source_ids"),
        "healthy": cells_with("healthy_source_ids"),
        "structured_parser": cells_with("structured_parser_source_ids"),
        "exact_numeric_extraction": cells_with("exact_numeric_observations"),
        "prospective_causal_observation": cells_with("prospective_causal_observations"),
        "prospective_causal_outcome": cells_with("prospective_causal_outcomes"),
        "semantic_direction": cells_with("semantic_direction_observations"),
        "prospective_semantic_direction": cells_with(
            "prospective_semantic_direction_observations"
        ),
    }
    family_summary: dict[str, dict[str, int]] = {}
    for family in EVENT_FAMILIES:
        rows = [row for row in cells if row["event_family"] == family]
        family_summary[family] = {
            "currencies": len(rows),
            "configured_transport": sum(bool(row["configured_transport_source_ids"]) for row in rows),
            "operational": sum(bool(row["operational_source_ids"]) for row in rows),
            "healthy": sum(bool(row["healthy_source_ids"]) for row in rows),
            "structured_parser": sum(bool(row["structured_parser_source_ids"]) for row in rows),
            "exact_numeric_extraction": sum(row["exact_numeric_observations"] > 0 for row in rows),
            "prospective_causal_observation": sum(row["prospective_causal_observations"] > 0 for row in rows),
            "prospective_causal_outcome": sum(row["prospective_causal_outcomes"] > 0 for row in rows),
            "semantic_direction": sum(row["semantic_direction_observations"] > 0 for row in rows),
        }

    observed_pairs = int(coverage.get("pair_count") or 0)
    expected_pairs = int(central.get("expected_pair_count") or 0)
    return {
        "schema_version": "official_currency_source_depth_readiness_v2",
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "currency_strength_weight": 0.0,
        "supported_decision": "diagnostic_only",
        "prospective_causal_outcome_source": outcome_source,
        "currency_count": len(expected),
        "event_family_count": len(EVENT_FAMILIES),
        "currency_event_cell_count": len(cells),
        "expected_pair_count": expected_pairs,
        "observed_pair_count": observed_pairs,
        "all_pairs_emitted": bool(coverage.get("all_pairs_emitted")),
        "stage_cell_counts": stage_cells,
        "family_summary": family_summary,
        "observation_totals": {
            "current_mapper_rows": len(mapping_rows),
            "mapper_rows_without_supported_event_family": excluded_unmapped,
            "exact_numeric_observations": sum(row["exact_numeric_observations"] for row in cells),
            "prospective_causal_observations": sum(
                row["prospective_causal_observations"] for row in cells
            ),
            "prospective_causal_outcomes": sum(
                row["prospective_causal_outcomes"] for row in cells
            ),
            "semantic_direction_observations": sum(
                row["semantic_direction_observations"] for row in cells
            ),
            "prospective_semantic_direction_observations": sum(
                row["prospective_semantic_direction_observations"] for row in cells
            ),
        },
        "definitions": {
            "configured_transport": "verified official source registered for this exact currency/event-family cell; direct runtime transport and enabled/live state are intentionally measured separately",
            "operational": "configured source currently reports an operational transport or retained numeric rate observation",
            "healthy": "configured source currently reports healthy and is not the current failed daily-rate source",
            "structured_parser": "an explicit allowlisted numeric parser is implemented for this exact source/event-family pair",
            "exact_numeric_extraction": "the current evidence ledgers contain at least one finite source-native numeric observation with a causal-known clock",
            "prospective_causal_observation": "observed after cohort activation, not a listing bootstrap, and publisher-time eligible; daily rates use prospective_eligible",
            "prospective_causal_outcome": "distinct prospective/proof-eligible canonical events with at least one valid exact-entry causal response; horizons never multiply an event; legacy V3 distinct-mapping evidence is used only while the causal response-map database is absent",
            "semantic_direction": "the current classifier contract emitted a nonzero currency direction; historical/nonprospective direction remains diagnostic only",
        },
        "cells": cells,
        "limitations": [
            "transport does not imply parsing, numeric extraction, causal timing, direction, or edge",
            "exact actuals without causally captured pre-release consensus cannot establish surprise",
            "semantic counts are raw observations and are not independent market episodes",
            "daily market-rate values are context and have no response outcome in this ledger",
            "no stage in this report can trade, authorize, promote, or change currency strength",
        ],
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    stages = report["stage_cell_counts"]
    lines = [
        "# Official Currency Source Depth / Readiness V2",
        "",
        f"Generated: `{report['generated_utc']}`",
        "",
        "Research-only diagnostic. It has zero execution, authorization, promotion, or currency-strength weight.",
        "",
        f"- Universe: **{report['currency_count']} currencies**, **{report['expected_pair_count']} expected pairs**, **{report['currency_event_cell_count']} currency × event-family cells**.",
        f"- Pair coverage artifact: **{report['observed_pair_count']}/{report['expected_pair_count']}**, all emitted: **{str(report['all_pairs_emitted']).lower()}**.",
        "- Readiness cells: " + ", ".join(f"{key} **{value}/{report['currency_event_cell_count']}**" for key, value in stages.items()),
        "",
        "| Event family | Transport | Operational | Healthy | Parser | Numeric output | Prospective input | Matured outcome | Semantic direction |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for family, row in report["family_summary"].items():
        lines.append(
            f"| {family} | {row['configured_transport']}/21 | {row['operational']}/21 | {row['healthy']}/21 | {row['structured_parser']}/21 | {row['exact_numeric_extraction']}/21 | {row['prospective_causal_observation']}/21 | {row['prospective_causal_outcome']}/21 | {row['semantic_direction']}/21 |"
        )
    lines.extend(
        [
            "",
            "## Definitions",
            "",
            *[f"- `{key}`: {value}" for key, value in report["definitions"].items()],
            "",
            "## Currency × event-family detail",
            "",
            "| Currency | Family | T | L | H | P | X obs | C obs | O | D obs |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in report["cells"]:
        lines.append(
            f"| {row['currency']} | {row['event_family']} | {len(row['configured_transport_source_ids'])} | {len(row['operational_source_ids'])} | {len(row['healthy_source_ids'])} | {len(row['structured_parser_source_ids'])} | {row['exact_numeric_observations']} | {row['prospective_causal_observations']} | {row['prospective_causal_outcomes']} | {row['semantic_direction_observations']} |"
        )
    lines.extend(["", "T=transport, L=live/operational, H=healthy, P=structured parser, X=exact numeric, C=prospective causal input, O=matured response outcome, D=semantic direction.", ""])
    return "\n".join(lines)


def write_report() -> dict[str, Any]:
    report = build_report()
    atomic_write(JSON_REPORT, json.dumps(report, indent=2, sort_keys=True) + "\n")
    atomic_write(MD_REPORT, render_markdown(report))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        report = build_report() if args.no_write else write_report()
        print(
            json.dumps(
                {
                    "currency_count": report["currency_count"],
                    "expected_pair_count": report["expected_pair_count"],
                    "currency_event_cell_count": report["currency_event_cell_count"],
                    "stage_cell_counts": report["stage_cell_counts"],
                    "observation_totals": report["observation_totals"],
                },
                indent=2,
            )
        )
        if args.interval_sec <= 0.0 or args.duration_sec <= 0.0:
            break
        if time.monotonic() - started >= args.duration_sec:
            break
        time.sleep(max(1.0, min(args.interval_sec, args.duration_sec)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
