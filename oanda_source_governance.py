#!/usr/bin/env python3
"""Build an append-only, point-in-time source registry for FX research.

The registry normalizes existing news and positioning observations without
changing their source databases.  It is research-only and deliberately has no
broker or execution imports.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from oanda_news_collector_contract import (
    NEWS_COLLECTOR_COHORT_ID,
    NEWS_COLLECTOR_CONTRACT_ID,
)
from oanda_official_release_fast_lane_contract import (
    OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC,
    OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
    OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID,
    OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID,
    OFFICIAL_RELEASE_FAST_LANE_COHORT_ID,
    OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
)


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_CONFIG = ROOT / "config" / "news_sources_v1.json"
DEFAULT_COVERAGE = DATA / "local_news_sentiment" / "source_coverage_latest.json"
DEFAULT_NEWS = DATA / "local_news_sentiment" / "local_news_sentiment_v1.sqlite"
DEFAULT_CFTC = DATA / "state" / "cftc_currency_positioning_shadow_v1.json"
DEFAULT_TREASURY = DATA / "state" / "us_treasury_yield_prospective_v1.sqlite"
DEFAULT_OFFICIAL_FAST_LANE = (
    DATA / "local_news_sentiment" / "official_release_fast_lane_v4.sqlite"
)
DEFAULT_OFFICIAL_CENTRAL_BANK_MAP = (
    ROOT / "config" / "official_central_bank_source_map_v1.json"
)
DEFAULT_DATABASE = DATA / "state" / "source_governance_v1.sqlite"
DEFAULT_STATE = DATA / "state" / "source_governance_v1.json"
DEFAULT_REPORT = DATA / "reports" / "source_governance" / "SOURCE_GOVERNANCE_CURRENT.md"
STORY_CLUSTER_CONTRACT_ID = "story_cluster_v2_event_lineage_or_headline_20260808"
OBSERVATION_TIME_CONTRACT_ID = (
    "observation_time_v4_fail_closed_consistent_integrity_sources_20260817"
)
LOCAL_NEWS_COLLECTOR_COHORT_ID = NEWS_COLLECTOR_COHORT_ID
LOCAL_NEWS_COLLECTOR_CONTRACT_ID = NEWS_COLLECTOR_CONTRACT_ID
ARTICLE_IDENTITY_CONTRACT_ID = (
    "substantive_article_identity_v6_collector_provenance_projection_20260816"
)
SOURCE_EVENT_REPAIR_CONTRACT_ID = (
    "source_event_noncausal_revision_quarantine_v1_20260816"
)
# Replay migrations appended these rows while operational collector or nested
# routing fields still participated in source-event identity.  The immutable
# rows remain preserved, but they are excluded from causal replay.  Genuine
# numeric enrichments at 837887, 837888, 837891 and 837894 are deliberately
# outside the ranges and remain causal at their true knowledge timestamps.
SOURCE_EVENT_REPAIR_ROWID_RANGES = (
    (837862, 837886),
    (837889, 837890),
    (837891, 837893),
    (837894, 837910),
    (838153, 838171),
    (838171, 838173),
    (838174, 838175),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def latency_ms(published: Any, first_seen: Any) -> int | None:
    left, right = parse_time(published), parse_time(first_seen)
    if left is None or right is None:
        return None
    return max(0, int(round((right - left).total_seconds() * 1000.0)))


def finite_or_none(value: Any) -> float | None:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return None
    return output if math.isfinite(output) else None


def cftc_substantive_payload(value: dict[str, Any]) -> dict[str, Any]:
    """Return the immutable vendor observation without polling bookkeeping.

    ``last_observed_utc`` changes every time the forward collector polls CFTC.
    Treating that field as source data created a new superseding event version
    even when the published report and every positioning value were identical.
    Observation time belongs on the registry row, not in the content identity.
    """
    return {
        key: item
        for key, item in value.items()
        if key not in {"first_seen_utc", "last_observed_utc"}
    }


def canonical_story_cluster(row: dict[str, Any], raw_payload: dict[str, Any]) -> tuple[str, str]:
    """Return a story identity, never a broad topic/category identity.

    ``topic_signature`` describes an archetype (for example every generic
    market-news article), not a syndicated story.  It previously collapsed
    thousands of unrelated articles into a handful of false clusters.
    """
    for key, method in (
        ("event_lineage_id", "source_event_lineage"),
        ("story_cluster_id", "source_story_cluster"),
        ("external_story_id", "source_external_story"),
    ):
        value = str(raw_payload.get(key) or "").strip()
        if value:
            return value, method
    headline = " ".join(str(row.get("headline") or "").casefold().split())
    summary = " ".join(str(row.get("summary") or "").casefold().split())
    return "story_" + stable_hash((headline, summary))[:24], "normalized_headline_summary"


def source_population(source: dict[str, Any]) -> str:
    role = str(source.get("source_role") or "").lower()
    kind = str(source.get("kind") or "").lower()
    if "policy" in role:
        return "official_policy_publisher"
    if "statistical" in role or "calendar" in role:
        return "official_macro_publisher"
    if "position" in role or "position" in kind:
        return "institutional_futures_positioning"
    if "rates" in role or "yield" in role or "rate_curve" in kind:
        return "official_rates_curve"
    if "option" in role or "option" in kind:
        return "listed_fx_options_market"
    if "aggregator" in role or kind in {"gdelt", "news_search", "news_sentiment"}:
        return "media_aggregator"
    return "public_market_context"


def license_class(source: dict[str, Any]) -> str:
    explicit = str(source.get("license_class") or "").strip()
    if explicit:
        return explicit
    kind = str(source.get("kind") or "").lower()
    role = str(source.get("source_role") or "").lower()
    if "credential" in str(source.get("retrieval_via") or "").lower():
        return "credentialed_vendor_terms_required"
    if "primary" in role or kind in {
        "bls_timeseries", "census_release_calendar", "vintage_backfill",
        "positioning_snapshot", "recurring_release_calendar",
    }:
        return "public_official_research_use"
    return "public_discovery_terms_review_required"


def contract_for(source: dict[str, Any]) -> dict[str, Any]:
    contract = {
        "source_id": str(source.get("source_id") or ""),
        "provider": str(source.get("name") or source.get("source_id") or ""),
        "access_method": str(source.get("kind") or "unknown"),
        "retrieval_via": str(source.get("retrieval_via") or "configured_collector"),
        "information_population": source_population(source),
        "currencies": sorted(str(item) for item in source.get("currencies") or []),
        "timestamp_semantics": (
            "first_seen_is_earliest_local_knowledge; published_is_provenance; "
            "revisions_create_new immutable source-event versions"
        ),
        "observation_time_contract_id": OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": LOCAL_NEWS_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": LOCAL_NEWS_COLLECTOR_COHORT_ID,
        "parser_version": str(source.get("parser_version") or "local_fx_news_sources_v1"),
        "missing_data_rule": "missing remains missing; never zero-fill unavailable information",
        "raw_payload_retention": "retain existing legally permitted payload and SHA-256",
        "license_class": license_class(source),
        "execution_eligible": False,
        "research_only": True,
    }
    if source.get("upstream_source_cohort_id"):
        contract["upstream_source_cohort_id"] = str(
            source["upstream_source_cohort_id"]
        )
    if "runtime_supported" in source:
        contract["runtime_supported"] = bool(source.get("runtime_supported"))
    if source.get("runtime_adapter"):
        contract["runtime_adapter"] = str(source.get("runtime_adapter"))
    if source.get("credential_env"):
        contract["credential_environment"] = str(source.get("credential_env"))
    contract["source_contract_id"] = "source_contract_" + stable_hash(contract)[:24]
    contract["source_cohort_id"] = (
        f"{contract['source_id']}.source.{contract['source_contract_id'][-16:]}"
    )
    return contract


def connect_registry(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS source_contracts (
            source_contract_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            source_cohort_id TEXT NOT NULL,
            created_utc TEXT NOT NULL,
            contract_sha256 TEXT NOT NULL,
            contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS source_events (
            source_event_id TEXT PRIMARY KEY,
            provider_event_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_family TEXT NOT NULL,
            source_population TEXT NOT NULL,
            provider TEXT NOT NULL,
            instrument TEXT,
            base_currency TEXT,
            quote_currency TEXT,
            country TEXT,
            event_type TEXT NOT NULL,
            article_id TEXT,
            story_cluster_id TEXT,
            event_id TEXT,
            market_episode_id TEXT,
            published_at_utc TEXT,
            first_seen_at_utc TEXT NOT NULL,
            retrieved_at_utc TEXT NOT NULL,
            effective_from_utc TEXT NOT NULL,
            valid_until_utc TEXT,
            revised_at_utc TEXT,
            superseded_at_utc TEXT,
            decision_cutoff_utc TEXT NOT NULL,
            source_clock_uncertainty_ms INTEGER,
            raw_value REAL,
            normalized_value REAL,
            coverage_count INTEGER NOT NULL,
            source_dispersion REAL,
            quality_state TEXT NOT NULL,
            latency_ms INTEGER,
            parser_version TEXT NOT NULL,
            vendor_model_name TEXT,
            vendor_model_version TEXT,
            raw_payload_sha256 TEXT NOT NULL,
            license_class TEXT NOT NULL,
            retention_policy TEXT NOT NULL,
            source_contract_id TEXT NOT NULL,
            source_cohort_id TEXT NOT NULL,
            event_version INTEGER NOT NULL,
            supersedes_source_event_id TEXT,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_source_events_knowledge
            ON source_events(source_id,effective_from_utc,source_event_id);
        CREATE INDEX IF NOT EXISTS ix_source_events_effective
            ON source_events(effective_from_utc,source_event_id);
        CREATE INDEX IF NOT EXISTS ix_source_events_provider_version
            ON source_events(source_id,provider_event_id,event_version DESC);
        CREATE INDEX IF NOT EXISTS ix_source_events_payload_replay
            ON source_events(source_id,provider_event_id,raw_payload_sha256);
        CREATE INDEX IF NOT EXISTS ix_source_events_story
            ON source_events(story_cluster_id,first_seen_at_utc);
        CREATE TABLE IF NOT EXISTS source_event_substantive_identities (
            source_id TEXT NOT NULL,
            provider_event_id TEXT NOT NULL,
            substantive_sha256 TEXT NOT NULL,
            raw_payload_sha256 TEXT NOT NULL,
            source_event_id TEXT NOT NULL,
            indexed_utc TEXT NOT NULL,
            PRIMARY KEY(source_id,provider_event_id,substantive_sha256)
        );
        CREATE TABLE IF NOT EXISTS source_event_quarantines (
            quarantine_id TEXT PRIMARY KEY,
            source_event_id TEXT NOT NULL UNIQUE,
            detected_utc TEXT NOT NULL,
            reason TEXT NOT NULL,
            repair_contract_id TEXT NOT NULL,
            evidence_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS source_supersession_events (
            supersession_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            provider_event_id TEXT NOT NULL,
            prior_source_event_id TEXT NOT NULL,
            next_source_event_id TEXT NOT NULL,
            observed_utc TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_source_supersession_prior_observed
            ON source_supersession_events(prior_source_event_id,observed_utc);
        CREATE TABLE IF NOT EXISTS source_card_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            observed_utc TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_contract_id TEXT NOT NULL,
            card_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS source_story_cluster_assignments (
            assignment_id TEXT PRIMARY KEY,
            source_event_id TEXT NOT NULL,
            clustering_contract_id TEXT NOT NULL,
            story_cluster_id TEXT NOT NULL,
            assigned_utc TEXT NOT NULL,
            method TEXT NOT NULL,
            assignment_json TEXT NOT NULL,
            UNIQUE(source_event_id,clustering_contract_id)
        );
        CREATE INDEX IF NOT EXISTS ix_story_assignments_cluster
            ON source_story_cluster_assignments(clustering_contract_id,story_cluster_id);
        CREATE TABLE IF NOT EXISTS official_fast_lane_governance_imports (
            receipt_id TEXT PRIMARY KEY,
            observation_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            provider_event_id TEXT NOT NULL,
            material_kind TEXT NOT NULL,
            material_sha256 TEXT NOT NULL,
            source_event_id TEXT NOT NULL,
            input_first_seen_utc TEXT NOT NULL,
            effective_from_utc TEXT NOT NULL,
            imported_utc TEXT NOT NULL,
            adapter_contract_id TEXT NOT NULL,
            adapter_activated_utc TEXT NOT NULL,
            upstream_collector_contract_id TEXT NOT NULL,
            upstream_collector_cohort_id TEXT NOT NULL,
            upstream_source_contract_id TEXT NOT NULL,
            upstream_source_cohort_id TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
            UNIQUE(observation_id,material_kind,material_sha256)
        );
        CREATE INDEX IF NOT EXISTS ix_fast_lane_import_effective
            ON official_fast_lane_governance_imports(effective_from_utc,receipt_id);
        CREATE TRIGGER IF NOT EXISTS source_contracts_no_update
            BEFORE UPDATE ON source_contracts BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS source_contracts_no_delete
            BEFORE DELETE ON source_contracts BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS source_events_no_update
            BEFORE UPDATE ON source_events BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS source_events_no_delete
            BEFORE DELETE ON source_events BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS source_supersession_no_update
            BEFORE UPDATE ON source_supersession_events BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS source_supersession_no_delete
            BEFORE DELETE ON source_supersession_events BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS source_event_identity_no_update
            BEFORE UPDATE ON source_event_substantive_identities BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS source_event_identity_no_delete
            BEFORE DELETE ON source_event_substantive_identities BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS source_event_quarantines_no_update
            BEFORE UPDATE ON source_event_quarantines BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS source_event_quarantines_no_delete
            BEFORE DELETE ON source_event_quarantines BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS source_cards_no_update
            BEFORE UPDATE ON source_card_snapshots BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS source_cards_no_delete
            BEFORE DELETE ON source_card_snapshots BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS story_assignments_no_update
            BEFORE UPDATE ON source_story_cluster_assignments BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS story_assignments_no_delete
            BEFORE DELETE ON source_story_cluster_assignments BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS fast_lane_imports_no_update
            BEFORE UPDATE ON official_fast_lane_governance_imports BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS fast_lane_imports_no_delete
            BEFORE DELETE ON official_fast_lane_governance_imports BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.execute(
        """CREATE VIEW IF NOT EXISTS source_events_causal_v1 AS
           SELECT event.* FROM source_events AS event
           WHERE NOT EXISTS (
               SELECT 1 FROM source_event_quarantines AS quarantine
               WHERE quarantine.source_event_id=event.source_event_id
           )"""
    )
    connection.execute(
        """CREATE VIEW IF NOT EXISTS source_supersession_events_causal_v1 AS
           SELECT supersession.* FROM source_supersession_events AS supersession
           WHERE NOT EXISTS (
               SELECT 1 FROM source_event_quarantines AS quarantine
               WHERE quarantine.source_event_id=supersession.prior_source_event_id
                  OR quarantine.source_event_id=supersession.next_source_event_id
           )"""
    )
    connection.commit()
    return connection


def configured_sources(config: dict[str, Any]) -> list[dict[str, Any]]:
    output = []
    for source in config.get("sources") or []:
        if isinstance(source, dict) and source.get("source_id"):
            output.append(dict(source))
    return output


def runtime_sources(coverage: dict[str, Any]) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for currency, value in (coverage.get("currencies") or {}).items():
        for source in (value or {}).get("sources") or []:
            if not isinstance(source, dict) or not source.get("source_id"):
                continue
            row = output.setdefault(str(source["source_id"]), dict(source))
            currencies = set(row.get("currencies") or [])
            currencies.add(str(currency))
            row["currencies"] = sorted(currencies)
    return output


def merge_collector_runtime(
    runtime: dict[str, dict[str, Any]], collector_state: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    """Merge source states that do not naturally map to a currency row.

    Discovery, vendor, and cross-market adapters can be live even when the
    currency coverage document has no per-currency entry for them.  The
    collector heartbeat is authoritative for their operational state, while
    the coverage document remains authoritative for currency membership.
    """

    healthy_states = {"ok", "not_due", "external_adapter"}
    blocked_states = {"credential_missing", "disabled", "unsupported"}
    observation_clock = collector_state.get("observation_clock") or {}
    collector_clock_trusted = bool(
        isinstance(observation_clock, Mapping)
        and observation_clock.get("trusted_for_prospective_evidence") is True
        and observation_clock.get("contract_id") == OBSERVATION_TIME_CONTRACT_ID
    )
    for source in collector_state.get("sources") or []:
        if not isinstance(source, Mapping) or not source.get("source_id"):
            continue
        source_id = str(source["source_id"])
        state = str(source.get("status") or "configured_not_observed")
        try:
            last_status = int(source.get("http_status") or source.get("last_status") or 0)
        except (TypeError, ValueError):
            last_status = 0
        error = str(source.get("error") or "")
        row = runtime.setdefault(source_id, {"source_id": source_id})
        row["runtime_status"] = state
        row["operational"] = state not in blocked_states
        row["healthy"] = bool(
            state in healthy_states and (not last_status or last_status < 400) and not error
        )
        row["causal_clock_trusted"] = collector_clock_trusted
        row["observation_time_contract_id"] = str(
            observation_clock.get("contract_id") or ""
        )
        row["last_error"] = error
    return runtime


def insert_contracts(
    connection: sqlite3.Connection, sources: Iterable[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    contracts: dict[str, dict[str, Any]] = {}
    for source in sources:
        contract = contract_for(source)
        contracts[str(source["source_id"])] = contract
        connection.execute(
            "INSERT OR IGNORE INTO source_contracts VALUES (?,?,?,?,?,?)",
            (
                contract["source_contract_id"], contract["source_id"],
                contract["source_cohort_id"], utc_now(), stable_hash(contract),
                canonical_json(contract),
            ),
        )
    connection.commit()
    return contracts


def article_events(
    news_database: Path, *, changed_after_utc: str | None = None
) -> Iterable[dict[str, Any]]:
    if not news_database.is_file():
        return []
    connection = sqlite3.connect(
        f"file:{news_database.resolve().as_posix()}?mode=ro", uri=True, timeout=10.0
    )
    connection.row_factory = sqlite3.Row
    try:
        query = """
            SELECT event_id,source_id,source_name,source_kind,source_quality,
                   source_verified,published_utc,first_seen_utc,last_seen_utc,
                   headline,summary,category,scope,currencies_json,
                   generic_sentiment_score,directional_confidence,severity,
                   movement_potential,duplicate_count,payload_json
            FROM articles
        """
        parameters: tuple[Any, ...] = ()
        if changed_after_utc:
            # A bounded offline classifier migration intentionally preserves
            # the publisher's original first/last observation clocks.  Always
            # reconsider verified structured/detail rows so newly extracted
            # numeric facts or document text reach the immutable registry
            # without falsely refreshing their market-knowledge timestamp.
            query += """ WHERE last_seen_utc>?
                OR (
                    source_verified=1 AND (
                        json_extract(payload_json,'$.structured_event')=1
                        OR json_extract(payload_json,'$.detail_enriched')=1
                    )
                )"""
            parameters = (changed_after_utc,)
        query += " ORDER BY first_seen_utc,event_id"
        rows = connection.execute(query, parameters).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


NON_SUBSTANTIVE_ARTICLE_PAYLOAD_FIELDS = frozenset(
    {
        # Observation refreshes and classifier deployment metadata are not new
        # publisher facts.  Semantic fields remain in the payload, so an
        # actual classification or source-content change still versions the
        # immutable source event.
        "classification_version",
        "collector_cohort_id",
        "collector_contract_id",
        "observation_time_contract_id",
        "observation_clock_trusted",
        "observation_clock_source",
        "availability_lag_minutes",
        "causal_known_utc",
        "detail_archive_path",
        "detail_context_archive_only",
        # Availability metadata is operational provenance, not a publisher
        # fact.  The actual consensus value/timestamp fields remain
        # substantive, so a newly captured pre-release consensus still
        # creates an immutable version.
        "consensus_capture_state",
        "forward_signal_timely",
        "last_seen_utc",
        "numeric_extraction_contract_id",
        "event_lineage_id",
        "material_update_id",
        "numeric_causal_known_utc",
        "post_window_minutes",
        "publication_hold_seconds",
        "published_utc",
        "relevance_window_minutes",
        "retrieval_via",
        "schedule_window_end_utc",
        "semantic_claim_contract",
        "sentiment_schema_version",
        "source_cohort_id",
        "source_contract_id",
        "source_direct",
        "source_id",
        "source_listing_bootstrap",
        "source_name",
        "source_role",
        # Vendor currency lists are a derived mapping.  The vendor's actual
        # sentiment/ticker payload remains substantive; mapping changes must
        # create a new source cohort rather than rewrite the publisher event.
        "vendor_currencies",
    }
)

# These fields describe how the collector reached or routed an article.  They
# are useful provenance in the retained full payload, but are not publisher
# facts and therefore cannot create an immutable publisher-event revision.
NON_SUBSTANTIVE_NESTED_RAW_FIELDS = NON_SUBSTANTIVE_ARTICLE_PAYLOAD_FIELDS | {
    "retrieval_via",
    "source_direct",
    "source_id",
    "source_role",
}


def normalized_publisher_headline(payload: Mapping[str, Any]) -> str:
    headline = " ".join(str(payload.get("headline") or "").split())
    if not headline:
        return headline
    candidates = {
        " ".join(str(payload.get("source_name") or "").split()).casefold(),
        " ".join(str(payload.get("domain") or "").split()).casefold(),
    }
    for candidate in sorted((value for value in candidates if value), key=len, reverse=True):
        suffix = " - " + candidate
        if headline.casefold().endswith(suffix):
            return headline[: -len(suffix)].rstrip()
    return headline


def substantive_article_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the source/semantic state that can create a new event version.

    The full observed payload is still retained in ``payload_json``.  This
    projection only controls immutable source-event identity, preventing poll
    timestamps and classifier labels from manufacturing revisions when the
    source text and derived semantics did not change.
    """

    output = {
        str(key): value
        for key, value in payload.items()
        if str(key) not in NON_SUBSTANTIVE_ARTICLE_PAYLOAD_FIELDS
        and str(key) != "raw_payload"
    }
    if "headline" in output:
        output["headline"] = normalized_publisher_headline(payload)
    # The governance payload wraps the complete collector record under
    # ``raw_payload``.  Apply the same substantive projection to that nested
    # record; otherwise a collector heartbeat, parser deployment, or routing
    # change manufactures a publisher revision even though the normalized
    # headline/summary and numeric facts are unchanged.  Numeric values,
    # consensus, revisions, source-native sentiment and vendor mappings remain
    # present, so real content changes still version the event.
    nested_raw = payload.get("raw_payload")
    if isinstance(nested_raw, Mapping):
        nested_output = {
            str(key): value
            for key, value in nested_raw.items()
            if str(key) not in NON_SUBSTANTIVE_NESTED_RAW_FIELDS
            and str(key) != "raw_payload"
        }
        if "headline" in nested_output:
            nested_output["headline"] = normalized_publisher_headline(nested_raw)
        output["raw_payload"] = nested_output
    return output


def _event_payload(
    row: dict[str, Any], contract: dict[str, Any], *, observed_utc: str
) -> dict[str, Any] | None:
    try:
        raw_payload = json.loads(str(row.get("payload_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        raw_payload = {}
    # Never assign today's governance contract to an older or untrusted
    # first-seen observation.  V38 and earlier did not retain immutable clock
    # provenance and therefore remain diagnostic rather than proof evidence.
    if not (
        raw_payload.get("observation_clock_trusted") is True
        and raw_payload.get("observation_time_contract_id")
        == OBSERVATION_TIME_CONTRACT_ID
        and raw_payload.get("collector_cohort_id")
        == LOCAL_NEWS_COLLECTOR_COHORT_ID
        and raw_payload.get("collector_contract_id")
        == LOCAL_NEWS_COLLECTOR_CONTRACT_ID
    ):
        return None
    payload_hash = stable_hash(substantive_article_payload(raw_payload))
    provider_event_id = str(row.get("event_id") or payload_hash)
    first_seen = str(row.get("first_seen_utc") or observed_utc)
    retrieved = str(row.get("last_seen_utc") or first_seen)
    effective_from = first_seen
    # An enriched official document or a parser-derived numeric observation
    # can become available well after the source listing was first seen.  The
    # immutable event must begin at the latest applicable knowledge boundary,
    # never at the older listing timestamp.
    knowledge_times = [parse_time(first_seen)]
    if bool(raw_payload.get("detail_enriched")):
        knowledge_times.append(
            parse_time(str(raw_payload.get("detail_available_utc") or ""))
        )
    if bool(raw_payload.get("structured_event")) and raw_payload.get(
        "actual_value"
    ) is not None:
        knowledge_times.append(
            parse_time(str(raw_payload.get("numeric_causal_known_utc") or ""))
        )
    known = [value for value in knowledge_times if value is not None]
    if known:
        effective_from = max(known).isoformat()
    currencies = []
    try:
        currencies = json.loads(str(row.get("currencies_json") or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        pass
    currencies = [str(item).upper() for item in currencies if item]
    story_cluster, story_cluster_method = canonical_story_cluster(row, raw_payload)
    source_event_id = "source_event_" + stable_hash(
        (
            contract["source_id"],
            provider_event_id,
            payload_hash,
            first_seen,
            effective_from,
        )
    )[:32]
    return {
        "source_event_id": source_event_id,
        "provider_event_id": provider_event_id,
        "source_id": contract["source_id"],
        "source_family": str(row.get("source_kind") or "news"),
        "source_population": contract["information_population"],
        "provider": str(row.get("source_name") or contract["provider"]),
        "instrument": None,
        "base_currency": currencies[0] if len(currencies) == 1 else None,
        "quote_currency": None,
        "country": None,
        "event_type": str(row.get("category") or "news_context"),
        "article_id": provider_event_id,
        "story_cluster_id": story_cluster,
        "event_id": provider_event_id,
        "market_episode_id": None,
        "published_at_utc": row.get("published_utc"),
        "first_seen_at_utc": first_seen,
        "retrieved_at_utc": retrieved,
        "effective_from_utc": effective_from,
        "valid_until_utc": None,
        "revised_at_utc": None,
        "superseded_at_utc": None,
        "decision_cutoff_utc": effective_from,
        "source_clock_uncertainty_ms": None,
        "raw_value": finite_or_none(row.get("generic_sentiment_score")),
        "normalized_value": finite_or_none(row.get("generic_sentiment_score")),
        "coverage_count": max(1, int(row.get("duplicate_count") or 0) + 1),
        "source_dispersion": None,
        "quality_state": (
            "verified" if int(row.get("source_verified") or 0) else "unverified"
        ),
        "latency_ms": latency_ms(row.get("published_utc"), first_seen),
        "parser_version": str(
            raw_payload.get("classification_version")
            or raw_payload.get("classifier_version")
            or "local_news_v1"
        ),
        "vendor_model_name": raw_payload.get("vendor_model_name"),
        "vendor_model_version": raw_payload.get("vendor_model_version"),
        "raw_payload_sha256": payload_hash,
        "license_class": contract["license_class"],
        "retention_policy": contract["raw_payload_retention"],
        "source_contract_id": contract["source_contract_id"],
        "source_cohort_id": contract["source_cohort_id"],
        "event_version": 1,
        "supersedes_source_event_id": None,
        "payload_json": canonical_json(
            {
                "headline": row.get("headline"), "summary": row.get("summary"),
                "scope": row.get("scope"), "currencies": currencies,
                "directional_confidence": row.get("directional_confidence"),
                "severity": row.get("severity"),
                "movement_potential": row.get("movement_potential"),
                "raw_payload": raw_payload,
                "story_cluster_contract_id": STORY_CLUSTER_CONTRACT_ID,
                "story_cluster_method": story_cluster_method,
            }
        ),
    }


SOURCE_EVENT_COLUMNS = (
    "source_event_id", "provider_event_id", "source_id", "source_family",
    "source_population", "provider", "instrument", "base_currency",
    "quote_currency", "country", "event_type", "article_id",
    "story_cluster_id", "event_id", "market_episode_id", "published_at_utc",
    "first_seen_at_utc", "retrieved_at_utc", "effective_from_utc",
    "valid_until_utc", "revised_at_utc", "superseded_at_utc",
    "decision_cutoff_utc", "source_clock_uncertainty_ms", "raw_value",
    "normalized_value", "coverage_count", "source_dispersion", "quality_state",
    "latency_ms", "parser_version", "vendor_model_name", "vendor_model_version",
    "raw_payload_sha256", "license_class", "retention_policy",
    "source_contract_id", "source_cohort_id", "event_version",
    "supersedes_source_event_id", "payload_json",
)


def source_event_substantive_sha256(event: Mapping[str, Any]) -> str:
    """Return the stable publisher/semantic identity used by the replay index."""

    try:
        wrapper = json.loads(str(event.get("payload_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        wrapper = {}
    raw_payload = wrapper.get("raw_payload")
    if isinstance(raw_payload, Mapping):
        return stable_hash(substantive_article_payload(raw_payload))
    return str(event.get("raw_payload_sha256") or "")


def insert_substantive_identity(
    connection: sqlite3.Connection,
    event: Mapping[str, Any],
    *,
    source_event_id: str,
    substantive_sha256: str,
) -> None:
    connection.execute(
        """INSERT OR IGNORE INTO source_event_substantive_identities
           VALUES (?,?,?,?,?,?)""",
        (
            event["source_id"],
            event["provider_event_id"],
            substantive_sha256,
            event["raw_payload_sha256"],
            source_event_id,
            event["first_seen_at_utc"],
        ),
    )


def quarantine_known_migration_artifacts(
    connection: sqlite3.Connection, *, detected_utc: str
) -> int:
    inserted = 0
    for lower_exclusive, upper_inclusive in SOURCE_EVENT_REPAIR_ROWID_RANGES:
        rows = connection.execute(
            """SELECT rowid,source_event_id,source_id,provider_event_id,
                      event_version,effective_from_utc,raw_payload_sha256
               FROM source_events
               WHERE rowid>? AND rowid<=? ORDER BY rowid""",
            (lower_exclusive, upper_inclusive),
        ).fetchall()
        for row in rows:
            evidence = {
                "rowid": int(row[0]),
                "source_id": str(row[2]),
                "provider_event_id": str(row[3]),
                "event_version": int(row[4]),
                "effective_from_utc": str(row[5]),
                "raw_payload_sha256": str(row[6]),
                "range": [lower_exclusive, upper_inclusive],
            }
            quarantine_id = "source_quarantine_" + stable_hash(
                (SOURCE_EVENT_REPAIR_CONTRACT_ID, str(row[1]))
            )[:28]
            before = connection.total_changes
            connection.execute(
                "INSERT OR IGNORE INTO source_event_quarantines VALUES (?,?,?,?,?,?)",
                (
                    quarantine_id,
                    str(row[1]),
                    detected_utc,
                    "noncausal_revision_or_backdated_parser_enrichment",
                    SOURCE_EVENT_REPAIR_CONTRACT_ID,
                    canonical_json(evidence),
                ),
            )
            inserted += int(connection.total_changes > before)
    connection.commit()
    return inserted


def backfill_article_substantive_identities(
    connection: sqlite3.Connection,
    rows: Iterable[dict[str, Any]],
    contracts: Mapping[str, dict[str, Any]],
    *,
    observed_utc: str,
    batch_size: int = 1000,
    progress_callback: Callable[[dict[str, int]], None] | None = None,
) -> dict[str, int]:
    """Build the compact replay index without changing source-event history.

    Legacy source events predate the substantive identity table.  Reconciling
    all retained news rows can take longer than the supervisor freshness
    window, so this migration commits only derived identity links in bounded
    batches.  A restart resumes from the append-only primary key rather than
    discarding a long transaction.
    """

    materialized = list(rows)
    total = len(materialized)
    processed = 0
    linked = 0
    already_indexed = 0
    batch_size = max(1, int(batch_size))
    for row in materialized:
        processed += 1
        contract = contracts.get(str(row.get("source_id") or ""))
        if contract is not None:
            event = _event_payload(row, contract, observed_utc=observed_utc)
            if event is not None:
                substantive_sha256 = source_event_substantive_sha256(event)
                known = connection.execute(
                """SELECT 1 FROM source_event_substantive_identities AS identity
                   WHERE identity.source_id=? AND identity.provider_event_id=?
                     AND identity.substantive_sha256=?
                     AND NOT EXISTS (
                         SELECT 1 FROM source_event_quarantines AS quarantine
                         WHERE quarantine.source_event_id=identity.source_event_id
                     ) LIMIT 1""",
                (
                    event["source_id"],
                    event["provider_event_id"],
                    substantive_sha256,
                ),
                ).fetchone()
                if known is not None:
                    already_indexed += 1
                else:
                    prior = connection.execute(
                    """SELECT event.source_event_id,event.raw_payload_sha256,event.payload_json
                       FROM source_events AS event
                       WHERE event.source_id=? AND event.provider_event_id=?
                         AND NOT EXISTS (
                             SELECT 1 FROM source_event_quarantines AS quarantine
                             WHERE quarantine.source_event_id=event.source_event_id
                         )
                       ORDER BY event.event_version DESC,event.rowid DESC LIMIT 1""",
                    (event["source_id"], event["provider_event_id"]),
                ).fetchone()
                    matching_source_event_id: str | None = None
                    if prior is not None and str(prior[1]) == str(
                        event["raw_payload_sha256"]
                    ):
                        matching_source_event_id = str(prior[0])
                    elif prior is not None:
                        try:
                            prior_wrapper = json.loads(str(prior[2] or "{}"))
                            current_wrapper = json.loads(
                                str(event.get("payload_json") or "{}")
                            )
                        except (TypeError, ValueError, json.JSONDecodeError):
                            prior_wrapper = {}
                            current_wrapper = {}
                        prior_raw = prior_wrapper.get("raw_payload")
                        current_raw = current_wrapper.get("raw_payload")
                        if (
                            isinstance(prior_raw, Mapping)
                            and isinstance(current_raw, Mapping)
                            and stable_hash(substantive_article_payload(prior_raw))
                            == stable_hash(substantive_article_payload(current_raw))
                        ):
                            matching_source_event_id = str(prior[0])
                        else:
                            replay = connection.execute(
                                """SELECT event.source_event_id FROM source_events AS event
                                   WHERE event.source_id=? AND event.provider_event_id=?
                                     AND event.raw_payload_sha256=?
                                     AND NOT EXISTS (
                                         SELECT 1 FROM source_event_quarantines AS quarantine
                                         WHERE quarantine.source_event_id=event.source_event_id
                                     ) LIMIT 1""",
                                (
                                    event["source_id"],
                                    event["provider_event_id"],
                                    event["raw_payload_sha256"],
                                ),
                            ).fetchone()
                            if replay is not None:
                                matching_source_event_id = str(replay[0])
                    if matching_source_event_id is not None:
                        before = connection.total_changes
                        insert_substantive_identity(
                            connection,
                            event,
                            source_event_id=matching_source_event_id,
                            substantive_sha256=substantive_sha256,
                        )
                        linked += int(connection.total_changes > before)
        if processed % batch_size == 0 or processed == total:
            connection.commit()
            progress = {
                "processed": processed,
                "total": total,
                "linked": linked,
                "already_indexed": already_indexed,
            }
            if progress_callback is not None:
                progress_callback(progress)
    return {
        "processed": processed,
        "total": total,
        "linked": linked,
        "already_indexed": already_indexed,
    }


def insert_source_event(connection: sqlite3.Connection, event: dict[str, Any]) -> bool:
    changes_before = connection.total_changes
    substantive_sha256 = source_event_substantive_sha256(event)
    if substantive_sha256:
        known_identity = connection.execute(
            """SELECT identity.source_event_id
               FROM source_event_substantive_identities AS identity
               WHERE identity.source_id=? AND identity.provider_event_id=?
                 AND identity.substantive_sha256=?
                 AND NOT EXISTS (
                     SELECT 1 FROM source_event_quarantines AS quarantine
                     WHERE quarantine.source_event_id=identity.source_event_id
                 ) LIMIT 1""",
            (event["source_id"], event["provider_event_id"], substantive_sha256),
        ).fetchone()
        if known_identity is not None:
            return False
    prior = connection.execute(
        """
        SELECT event.source_event_id,event.raw_payload_sha256,event.event_version,event.payload_json
        FROM source_events AS event
        WHERE event.source_id=? AND event.provider_event_id=?
          AND NOT EXISTS (
              SELECT 1 FROM source_event_quarantines AS quarantine
              WHERE quarantine.source_event_id=event.source_event_id
          )
        ORDER BY event.event_version DESC, event.rowid DESC LIMIT 1
        """,
        (event["source_id"], event["provider_event_id"]),
    ).fetchone()
    if prior is not None and str(prior[1]) == str(event["raw_payload_sha256"]):
        if substantive_sha256:
            insert_substantive_identity(
                connection,
                event,
                source_event_id=str(prior[0]),
                substantive_sha256=substantive_sha256,
            )
        return False
    if prior is not None:
        # Compatibility with source events written before substantive hashes
        # were introduced.  Their stored digest covered refresh metadata, but
        # their retained payload still lets us prove that the publisher facts
        # and semantic outputs are unchanged without rewriting history.
        try:
            prior_wrapper = json.loads(str(prior[3] or "{}"))
            current_wrapper = json.loads(str(event.get("payload_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            prior_wrapper = {}
            current_wrapper = {}
        prior_raw = prior_wrapper.get("raw_payload")
        current_raw = current_wrapper.get("raw_payload")
        if isinstance(prior_raw, Mapping) and isinstance(current_raw, Mapping):
            if stable_hash(substantive_article_payload(prior_raw)) == stable_hash(
                substantive_article_payload(current_raw)
            ):
                if substantive_sha256:
                    insert_substantive_identity(
                        connection,
                        event,
                        source_event_id=str(prior[0]),
                        substantive_sha256=substantive_sha256,
                    )
                return False
        # Importers can legitimately alternate between two upstream snapshots
        # (for example, two collector cohorts that contain the same
        # observation).  Query history only after the latest-version and
        # compatibility fast paths above have failed; unchanged imports stay
        # at one indexed lookup per event.
        replay = connection.execute(
            """
            SELECT event.source_event_id FROM source_events AS event
            WHERE event.source_id=? AND event.provider_event_id=?
              AND event.raw_payload_sha256=?
              AND NOT EXISTS (
                  SELECT 1 FROM source_event_quarantines AS quarantine
                  WHERE quarantine.source_event_id=event.source_event_id
              )
            LIMIT 1
            """,
            (
                event["source_id"],
                event["provider_event_id"],
                event["raw_payload_sha256"],
            ),
        ).fetchone()
        if replay is not None:
            if substantive_sha256:
                insert_substantive_identity(
                    connection,
                    event,
                    source_event_id=str(replay[0]),
                    substantive_sha256=substantive_sha256,
                )
            return False
    if prior is not None:
        event["event_version"] = int(prior[2]) + 1
        event["supersedes_source_event_id"] = str(prior[0])
        event["source_event_id"] = "source_event_" + stable_hash(
            (event["source_id"], event["provider_event_id"], event["raw_payload_sha256"], event["event_version"])
        )[:32]
    cursor = connection.execute(
        f"INSERT OR IGNORE INTO source_events ({','.join(SOURCE_EVENT_COLUMNS)}) VALUES ({','.join('?' for _ in SOURCE_EVENT_COLUMNS)})",
        tuple(event.get(column) for column in SOURCE_EVENT_COLUMNS),
    )
    event_inserted = int(cursor.rowcount > 0)
    if event_inserted and substantive_sha256:
        insert_substantive_identity(
            connection,
            event,
            source_event_id=str(event["source_event_id"]),
            substantive_sha256=substantive_sha256,
        )
    if event_inserted and prior is not None:
        relation = (
            "supersession_" + stable_hash((prior[0], event["source_event_id"]))[:28],
            event["source_id"], event["provider_event_id"], str(prior[0]),
            event["source_event_id"], event["first_seen_at_utc"],
        )
        connection.execute(
            "INSERT OR IGNORE INTO source_supersession_events VALUES (?,?,?,?,?,?)",
            relation,
        )
    return bool(event_inserted and connection.total_changes > changes_before)


def insert_story_assignment(
    connection: sqlite3.Connection,
    event: dict[str, Any],
    *,
    assigned_utc: str,
) -> bool:
    row = connection.execute(
        """
        SELECT event.source_event_id FROM source_events AS event
        WHERE event.source_id=? AND event.provider_event_id=?
          AND event.raw_payload_sha256=?
          AND NOT EXISTS (
              SELECT 1 FROM source_event_quarantines AS quarantine
              WHERE quarantine.source_event_id=event.source_event_id
          )
        ORDER BY event.event_version DESC,event.rowid DESC LIMIT 1
        """,
        (event["source_id"], event["provider_event_id"], event["raw_payload_sha256"]),
    ).fetchone()
    if row is None:
        substantive_sha256 = source_event_substantive_sha256(event)
        row = connection.execute(
            """SELECT identity.source_event_id
               FROM source_event_substantive_identities AS identity
               WHERE identity.source_id=? AND identity.provider_event_id=?
                 AND identity.substantive_sha256=?
                 AND NOT EXISTS (
                     SELECT 1 FROM source_event_quarantines AS quarantine
                     WHERE quarantine.source_event_id=identity.source_event_id
                 ) LIMIT 1""",
            (event["source_id"], event["provider_event_id"], substantive_sha256),
        ).fetchone()
    if row is None or not event.get("story_cluster_id"):
        return False
    source_event_id = str(row[0])
    try:
        payload = json.loads(str(event.get("payload_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        payload = {}
    method = str(payload.get("story_cluster_method") or "unknown")
    assignment = {
        "source_event_id": source_event_id,
        "clustering_contract_id": STORY_CLUSTER_CONTRACT_ID,
        "story_cluster_id": str(event["story_cluster_id"]),
        "method": method,
    }
    before = connection.total_changes
    connection.execute(
        "INSERT OR IGNORE INTO source_story_cluster_assignments VALUES (?,?,?,?,?,?,?)",
        (
            "story_assignment_" + stable_hash(assignment)[:28],
            source_event_id,
            STORY_CLUSTER_CONTRACT_ID,
            str(event["story_cluster_id"]),
            assigned_utc,
            method,
            canonical_json(assignment),
        ),
    )
    return connection.total_changes > before


OFFICIAL_FAST_LANE_REQUIRED_COLUMNS = frozenset(
    {
        "observation_id",
        "source_id",
        "source_contract_id",
        "source_cohort_id",
        "item_key",
        "material_sha256",
        "first_seen_utc",
        "prospective_observation",
        "listing_bootstrap",
        "identity_preexisting",
        "publisher_time_eligible",
        "observation_clock_trusted",
        "observation_clock_source",
        "raw_payload_json",
        "research_only",
        "execution_eligible",
        "can_authorize",
        "collector_contract_id",
        "collector_cohort_id",
    }
)


def official_fast_lane_source_ids(mapping: Mapping[str, Any]) -> set[str]:
    output: set[str] = set()
    for currency in mapping.get("currencies") or []:
        if not isinstance(currency, Mapping):
            continue
        for field in (
            "release_source_ids",
            "communication_source_ids",
            "statistical_release_source_ids",
        ):
            output.update(
                str(value).strip()
                for value in currency.get(field) or []
                if str(value).strip()
            )
    return output


def official_fast_lane_source_lineages(
    config: Mapping[str, Any], *, allowed_source_ids: set[str]
) -> dict[str, dict[str, Any]]:
    """Return the exact source lineages used by the active fast collector."""

    # Import lazily so ordinary registry inspection remains lightweight.  The
    # fast collector uses this exact function to bind unversioned config rows;
    # duplicating its digest algorithm here would create a second authority.
    from oanda_local_news_sentiment import source_config_lineage

    output: dict[str, dict[str, Any]] = {}
    for raw in config.get("sources") or []:
        if not isinstance(raw, Mapping):
            continue
        source_id = str(raw.get("source_id") or "")
        if source_id not in allowed_source_ids:
            continue
        source = source_config_lineage(raw)
        is_direct = (
            source.get("direct") is True
            if "direct" in source
            else source.get("verified") is True
        )
        if source.get("verified") is not True or not is_direct:
            continue
        output[source_id] = source
    return output


def _valid_sha256(value: Any) -> bool:
    text = str(value or "").lower().strip()
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _fast_lane_material_versions(
    raw_payload: Mapping[str, Any],
    *,
    root_first_seen: datetime,
) -> list[dict[str, Any]]:
    """Split one trusted observation into separately clocked material states."""

    detail_present = bool(
        raw_payload.get("detail_enriched")
        or raw_payload.get("detail_attachment_enriched")
    )
    base_payload = {
        str(key): value
        for key, value in raw_payload.items()
        if not str(key).startswith("detail_")
    }
    if detail_present:
        # Enriched collectors append body/PDF text to ``summary``.  Keeping it
        # in the base observation would backdate later knowledge to the feed's
        # earlier first-seen clock.
        base_payload.pop("summary", None)
        base_payload["summary_omitted_due_to_later_enrichment"] = True
    versions = [
        {
            "kind": "listing_observation",
            "effective": root_first_seen,
            "parser_contract_id": "official_fast_lane_listing_payload_v1",
            "material": base_payload,
        }
    ]

    detail_available = parse_time(raw_payload.get("detail_available_utc"))
    detail_summary = str(raw_payload.get("summary") or "")
    detail_hash = str(raw_payload.get("detail_content_sha256") or "").lower()
    if not _valid_sha256(detail_hash) and detail_summary:
        detail_hash = hashlib.sha256(detail_summary.encode("utf-8")).hexdigest()
    detail_parser = str(
        raw_payload.get("detail_parser_contract_id")
        or raw_payload.get("detail_enrichment_kind")
        or ""
    )
    if (
        raw_payload.get("detail_enriched") is True
        and detail_available is not None
        and _valid_sha256(detail_hash)
        and detail_parser
    ):
        versions.append(
            {
                "kind": "official_body_detail",
                "effective": max(root_first_seen, detail_available),
                "parser_contract_id": detail_parser,
                "material": {
                    **base_payload,
                    "material_version_kind": "official_body_detail",
                    "detail_content_sha256": detail_hash,
                    "detail_content_bytes": raw_payload.get("detail_content_bytes"),
                    "detail_text_characters": raw_payload.get(
                        "detail_text_characters"
                    ),
                    "detail_source_url": raw_payload.get("detail_source_url"),
                    "summary": detail_summary,
                },
            }
        )

    attachments = raw_payload.get("detail_attachments")
    if isinstance(attachments, Mapping):
        attachments = [attachments]
    elif not isinstance(attachments, list):
        attachments = []
    if not attachments and raw_payload.get("detail_attachment_enriched") is True:
        attachments = [
            {
                "available_utc": raw_payload.get(
                    "detail_attachment_available_utc"
                ),
                "content_sha256": raw_payload.get(
                    "detail_attachment_content_sha256"
                ),
                "content_bytes": raw_payload.get(
                    "detail_attachment_content_bytes"
                ),
                "text_characters": raw_payload.get(
                    "detail_attachment_text_characters"
                ),
                "parser_contract_id": raw_payload.get(
                    "detail_attachment_parser_contract_id"
                ),
                "url": raw_payload.get("detail_attachment_urls"),
            }
        ]
    for attachment in attachments:
        if not isinstance(attachment, Mapping):
            continue
        available = parse_time(
            attachment.get("available_utc")
            or raw_payload.get("detail_attachment_available_utc")
        )
        content_hash = str(attachment.get("content_sha256") or "").lower()
        parser_contract = str(
            attachment.get("parser_contract_id")
            or raw_payload.get("detail_attachment_parser_contract_id")
            or ""
        )
        if available is None or not _valid_sha256(content_hash) or not parser_contract:
            continue
        versions.append(
            {
                "kind": "official_pdf_attachment",
                "effective": max(root_first_seen, available),
                "parser_contract_id": parser_contract,
                "material": {
                    **base_payload,
                    "material_version_kind": "official_pdf_attachment",
                    "attachment_content_sha256": content_hash,
                    "attachment_content_bytes": attachment.get("content_bytes"),
                    "attachment_text_characters": attachment.get(
                        "text_characters"
                    ),
                    "attachment_url": attachment.get("url"),
                    "attachment_parser_contract_id": parser_contract,
                },
            }
        )
    versions[1:] = sorted(
        versions[1:],
        key=lambda value: (
            value["effective"],
            value["kind"],
            stable_hash(value["material"]),
        ),
    )
    return versions


def _official_fast_lane_event(
    row: Mapping[str, Any],
    *,
    raw_payload: Mapping[str, Any],
    material_version: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    source_id = str(row["source_id"])
    item_key = str(row["item_key"])
    observation_id = str(row["observation_id"])
    provider_event_id = f"official_fast_lane:{source_id}:{item_key}"
    effective = material_version["effective"].astimezone(timezone.utc).isoformat()
    first_seen = str(row["first_seen_utc"])
    material = dict(material_version["material"])
    material.update(
        {
            "material_version_kind": str(material_version["kind"]),
            "material_effective_from_utc": effective,
            "upstream_observation_id": observation_id,
            "upstream_material_sha256": str(row["material_sha256"]),
            "upstream_source_contract_id": str(row["source_contract_id"]),
            "upstream_source_cohort_id": str(row["source_cohort_id"]),
            "upstream_collector_contract_id": str(row["collector_contract_id"]),
            "upstream_collector_cohort_id": str(row["collector_cohort_id"]),
            "adapter_contract_id": OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
            "adapter_activated_utc": (
                OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC.isoformat()
            ),
            "research_only": True,
            "execution_eligible": False,
            "can_authorize": False,
        }
    )
    payload_hash = stable_hash(material)
    configured_currencies = [
        str(value).upper() for value in contract.get("currencies") or [] if value
    ]
    published = raw_payload.get("published_utc") or raw_payload.get(
        "published_at_utc"
    )
    story_cluster_id = "story_cluster_fast_lane_" + stable_hash(
        (source_id, item_key)
    )[:28]
    return {
        "source_event_id": "source_event_" + stable_hash(
            (source_id, provider_event_id, payload_hash, effective)
        )[:32],
        "provider_event_id": provider_event_id,
        "source_id": source_id,
        "source_family": "official_release_fast_lane",
        "source_population": contract["information_population"],
        "provider": contract["provider"],
        "instrument": None,
        "base_currency": (
            configured_currencies[0] if len(configured_currencies) == 1 else None
        ),
        "quote_currency": None,
        "country": None,
        "event_type": str(material_version["kind"]),
        "article_id": observation_id,
        "story_cluster_id": story_cluster_id,
        "event_id": observation_id,
        "market_episode_id": None,
        "published_at_utc": published,
        "first_seen_at_utc": effective,
        "retrieved_at_utc": effective,
        "effective_from_utc": effective,
        "valid_until_utc": None,
        "revised_at_utc": None,
        "superseded_at_utc": None,
        "decision_cutoff_utc": effective,
        "source_clock_uncertainty_ms": None,
        "raw_value": None,
        "normalized_value": None,
        "coverage_count": 1,
        "source_dispersion": None,
        "quality_state": "prospective_official_fast_lane_research_only",
        "latency_ms": latency_ms(published, effective),
        "parser_version": str(material_version["parser_contract_id"]),
        "vendor_model_name": None,
        "vendor_model_version": None,
        "raw_payload_sha256": payload_hash,
        "license_class": contract["license_class"],
        "retention_policy": contract["raw_payload_retention"],
        "source_contract_id": contract["source_contract_id"],
        "source_cohort_id": contract["source_cohort_id"],
        "event_version": 1,
        "supersedes_source_event_id": None,
        "payload_json": canonical_json(
            {
                "headline": raw_payload.get("headline")
                or raw_payload.get("title"),
                "summary": (
                    raw_payload.get("summary")
                    if material_version["kind"] != "listing_observation"
                    else None
                ),
                "currencies": configured_currencies,
                "raw_payload": material,
                "story_cluster_contract_id": STORY_CLUSTER_CONTRACT_ID,
                "story_cluster_method": "official_fast_lane_source_item",
            }
        ),
    }


def import_official_fast_lane_events(
    connection: sqlite3.Connection,
    *,
    fast_lane_database: Path,
    allowed_source_ids: set[str],
    expected_source_lineages: Mapping[str, Mapping[str, Any]],
    governance_contracts: Mapping[str, Mapping[str, Any]],
    observed_utc: str,
) -> dict[str, Any]:
    """Append only post-activation, inert fast-lane observations.

    There is intentionally no migration switch.  Rows first seen before the
    adapter activation clock are counted and rejected on every run, including
    the audited BOJ observation.
    """

    activated = OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC
    observed = parse_time(observed_utc)
    result: dict[str, Any] = {
        "status": "ok",
        "adapter_contract_id": OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
        "adapter_activated_utc": activated.isoformat(),
        "upstream_collector_contract_id": OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
        "upstream_collector_cohort_id": OFFICIAL_RELEASE_FAST_LANE_COHORT_ID,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "allowed_official_source_count": len(allowed_source_ids),
        "active_source_lineage_count": len(expected_source_lineages),
        "rows_scanned": 0,
        "eligible_root_observations": 0,
        "inserted_versions": 0,
        "inserted_listing_versions": 0,
        "inserted_detail_versions": 0,
        "inserted_attachment_versions": 0,
        "duplicate_receipts": 0,
        "rejections": {},
    }
    if observed is None or observed < activated:
        result["status"] = "not_activated"
        return result
    if not allowed_source_ids:
        result["status"] = "source_map_missing_or_empty"
        return result
    if set(expected_source_lineages) != set(allowed_source_ids):
        result["status"] = "source_lineage_configuration_incomplete"
        result["missing_source_lineages"] = sorted(
            allowed_source_ids - set(expected_source_lineages)
        )
        return result
    if not fast_lane_database.is_file():
        result["status"] = "input_missing"
        return result

    reader = sqlite3.connect(
        f"file:{fast_lane_database.resolve().as_posix()}?mode=ro",
        uri=True,
        timeout=10.0,
    )
    reader.row_factory = sqlite3.Row
    try:
        columns = {
            str(row[1])
            for row in reader.execute(
                "PRAGMA table_info(official_release_observation)"
            ).fetchall()
        }
        missing = sorted(OFFICIAL_FAST_LANE_REQUIRED_COLUMNS - columns)
        if missing:
            result["status"] = "schema_mismatch"
            result["missing_columns"] = missing
            return result
        rows = reader.execute(
            "SELECT * FROM official_release_observation "
            "ORDER BY first_seen_utc,observation_id"
        ).fetchall()
    finally:
        reader.close()

    rejections: dict[str, int] = {}

    def reject(reason: str) -> None:
        rejections[reason] = rejections.get(reason, 0) + 1

    for row in rows:
        result["rows_scanned"] += 1
        first_seen = parse_time(row["first_seen_utc"])
        # This check deliberately precedes every other contract test. The
        # audited BOJ row has incomplete source lineage, but its decisive
        # rejection reason is that it predates adapter activation.
        if first_seen is None or first_seen < activated:
            reject("preactivation_or_invalid_first_seen")
            continue
        if first_seen > observed:
            reject("future_first_seen")
            continue
        if str(row["source_id"]) not in allowed_source_ids:
            reject("source_not_in_official_release_map")
            continue
        expected = expected_source_lineages.get(str(row["source_id"]))
        contract = governance_contracts.get(str(row["source_id"]))
        if expected is None or contract is None:
            reject("source_not_active_or_not_direct")
            continue
        if not str(row["source_contract_id"]) or not str(row["source_cohort_id"]):
            reject("empty_source_contract")
            continue
        if (
            str(row["source_contract_id"])
            != str(expected.get("source_contract_id") or "")
            or str(row["source_cohort_id"])
            != str(expected.get("source_cohort_id") or "")
        ):
            reject("source_contract_mismatch")
            continue
        if (
            str(row["collector_contract_id"])
            != OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID
            or str(row["collector_cohort_id"])
            != OFFICIAL_RELEASE_FAST_LANE_COHORT_ID
        ):
            if (
                str(row["collector_contract_id"])
                == OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID
                and str(row["collector_cohort_id"])
                == OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID
            ):
                reject("retained_prior_collector_contract_diagnostic_only")
            else:
                reject("collector_contract_mismatch")
            continue
        if int(row["prospective_observation"]) != 1:
            reject("not_prospective")
            continue
        if int(row["listing_bootstrap"]) != 0:
            reject("listing_bootstrap")
            continue
        if int(row["identity_preexisting"]) != 0:
            reject("identity_preexisting")
            continue
        if int(row["publisher_time_eligible"]) != 1:
            reject("publisher_clock_untrusted")
            continue
        if (
            int(row["observation_clock_trusted"]) != 1
            or not str(row["observation_clock_source"])
        ):
            reject("observation_clock_untrusted")
            continue
        if (
            int(row["research_only"]) != 1
            or int(row["execution_eligible"]) != 0
            or int(row["can_authorize"]) != 0
        ):
            reject("input_not_inert")
            continue
        try:
            raw_payload = json.loads(str(row["raw_payload_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            raw_payload = None
        if not isinstance(raw_payload, Mapping):
            reject("invalid_raw_payload")
            continue
        canonical_input = json.dumps(
            raw_payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        expected_material_hash = hashlib.sha256(
            canonical_input.encode("utf-8")
        ).hexdigest()
        if expected_material_hash != str(row["material_sha256"]):
            reject("material_hash_mismatch")
            continue
        expected_observation_id = hashlib.sha256(
            (
                f"{row['source_id']}|{row['item_key']}|"
                f"{row['material_sha256']}"
            ).encode("utf-8")
        ).hexdigest()
        if expected_observation_id != str(row["observation_id"]):
            reject("observation_identity_mismatch")
            continue
        published = parse_time(
            raw_payload.get("published_utc")
            or raw_payload.get("published_at_utc")
        )
        if (
            published is None
            or published < activated
            or published > first_seen
        ):
            reject("publisher_clock_outside_adapter_window")
            continue

        result["eligible_root_observations"] += 1
        versions = _fast_lane_material_versions(
            raw_payload,
            root_first_seen=first_seen,
        )
        for version in versions:
            event = _official_fast_lane_event(
                row,
                raw_payload=raw_payload,
                material_version=version,
                contract=contract,
            )
            material_hash = str(event["raw_payload_sha256"])
            receipt_exists = connection.execute(
                """SELECT 1 FROM official_fast_lane_governance_imports
                   WHERE observation_id=? AND material_kind=?
                     AND material_sha256=? LIMIT 1""",
                (
                    str(row["observation_id"]),
                    str(version["kind"]),
                    material_hash,
                ),
            ).fetchone()
            if receipt_exists is not None:
                result["duplicate_receipts"] += 1
                continue
            inserted = insert_source_event(connection, event)
            if not inserted:
                reject("source_event_replay_without_receipt")
                continue
            insert_story_assignment(connection, event, assigned_utc=observed_utc)
            receipt = {
                "observation_id": str(row["observation_id"]),
                "source_id": str(row["source_id"]),
                "provider_event_id": str(event["provider_event_id"]),
                "material_kind": str(version["kind"]),
                "material_sha256": material_hash,
                "source_event_id": str(event["source_event_id"]),
                "input_first_seen_utc": str(row["first_seen_utc"]),
                "effective_from_utc": str(event["effective_from_utc"]),
                "adapter_contract_id": OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
            }
            connection.execute(
                """INSERT INTO official_fast_lane_governance_imports
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    "fast_lane_import_" + stable_hash(receipt)[:28],
                    receipt["observation_id"],
                    receipt["source_id"],
                    receipt["provider_event_id"],
                    receipt["material_kind"],
                    receipt["material_sha256"],
                    receipt["source_event_id"],
                    receipt["input_first_seen_utc"],
                    receipt["effective_from_utc"],
                    observed_utc,
                    OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
                    activated.isoformat(),
                    OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
                    OFFICIAL_RELEASE_FAST_LANE_COHORT_ID,
                    str(row["source_contract_id"]),
                    str(row["source_cohort_id"]),
                    1,
                    0,
                    0,
                ),
            )
            result["inserted_versions"] += 1
            if version["kind"] == "listing_observation":
                result["inserted_listing_versions"] += 1
            elif version["kind"] == "official_body_detail":
                result["inserted_detail_versions"] += 1
            elif version["kind"] == "official_pdf_attachment":
                result["inserted_attachment_versions"] += 1
    result["rejections"] = dict(sorted(rejections.items()))
    return result


def insert_cftc_events(
    connection: sqlite3.Connection,
    cftc: dict[str, Any],
    contract: dict[str, Any],
) -> int:
    generated = str(cftc.get("generated_utc") or utc_now())
    inserted = 0
    for currency, value in sorted((cftc.get("currencies") or {}).items()):
        if not isinstance(value, dict):
            continue
        substantive_payload = cftc_substantive_payload(value)
        payload_hash = stable_hash(substantive_payload)
        provider_event_id = f"{currency}|{value.get('report_date')}"
        event = {
            "source_event_id": "source_event_" + stable_hash((contract["source_id"], provider_event_id, payload_hash, generated))[:32],
            "provider_event_id": provider_event_id,
            "source_id": contract["source_id"],
            "source_family": "positioning_snapshot",
            "source_population": "institutional_futures_positioning",
            "provider": "CFTC Traders in Financial Futures",
            "instrument": None,
            "base_currency": str(currency),
            "quote_currency": None,
            "country": None,
            "event_type": "institutional_positioning",
            "article_id": None,
            "story_cluster_id": None,
            "event_id": provider_event_id,
            "market_episode_id": None,
            "published_at_utc": None,
            "first_seen_at_utc": str(value.get("first_seen_utc") or generated),
            "retrieved_at_utc": generated,
            "effective_from_utc": str(value.get("first_seen_utc") or generated),
            "valid_until_utc": None,
            "revised_at_utc": None,
            "superseded_at_utc": None,
            "decision_cutoff_utc": str(value.get("first_seen_utc") or generated),
            "source_clock_uncertainty_ms": None,
            "raw_value": finite_or_none(value.get("leveraged_money_net_pct_oi")),
            "normalized_value": finite_or_none(value.get("leveraged_money_net_pct_oi")),
            "coverage_count": 1,
            "source_dispersion": None,
            "quality_state": "forward_only_causal_observation",
            "latency_ms": None,
            "parser_version": "cftc_tff_shadow_v1",
            "vendor_model_name": None,
            "vendor_model_version": None,
            "raw_payload_sha256": payload_hash,
            "license_class": contract["license_class"],
            "retention_policy": contract["raw_payload_retention"],
            "source_contract_id": contract["source_contract_id"],
            "source_cohort_id": contract["source_cohort_id"],
            "event_version": 1,
            "supersedes_source_event_id": None,
            "payload_json": canonical_json(value),
        }
        inserted += int(insert_source_event(connection, event))
    return inserted


def treasury_observations(database: Path) -> list[dict[str, Any]]:
    if not database.is_file():
        return []
    connection = sqlite3.connect(
        f"file:{database.resolve().as_posix()}?mode=ro", uri=True, timeout=10.0
    )
    connection.row_factory = sqlite3.Row
    try:
        active = connection.execute(
            """
            SELECT cohort_id
            FROM rate_observations
            GROUP BY cohort_id
            ORDER BY MAX(observed_utc) DESC, cohort_id DESC
            LIMIT 1
            """
        ).fetchone()
        if active is None:
            return []
        return [
            dict(row)
            for row in connection.execute(
                """
                SELECT observation_id,cohort_id,observed_utc,yield_date,
                       feed_updated_utc,two_year_pct,ten_year_pct,
                       curve_2s10s_bps,row_sha256,raw_archive_sha256,
                       observation_version,supersedes_observation_id,
                       observation_kind,bootstrap_current_view,
                       prospective_eligible,direction_policy,source_contract_json
                FROM (
                    SELECT rowid AS source_rowid,*,
                           ROW_NUMBER() OVER (
                               PARTITION BY yield_date
                               ORDER BY observation_version DESC,
                                        observed_utc DESC,rowid DESC
                           ) AS cohort_date_rank
                    FROM rate_observations
                    WHERE cohort_id=?
                )
                WHERE cohort_date_rank=1
                ORDER BY observed_utc,yield_date,observation_version
                """,
                (str(active["cohort_id"]),),
            )
        ]
    except sqlite3.DatabaseError:
        return []
    finally:
        connection.close()


def insert_treasury_events(
    connection: sqlite3.Connection,
    rows: list[dict[str, Any]],
    contract: dict[str, Any],
    *,
    batch_size: int = 1000,
    progress_callback: Callable[[dict[str, int]], None] | None = None,
) -> int:
    inserted = 0
    total = len(rows)
    batch_size = max(1, int(batch_size))
    for processed, value in enumerate(rows, start=1):
        payload = {
            "yield_date": value.get("yield_date"),
            "two_year_pct": value.get("two_year_pct"),
            "ten_year_pct": value.get("ten_year_pct"),
            "curve_2s10s_bps": value.get("curve_2s10s_bps"),
            "observation_kind": value.get("observation_kind"),
            "bootstrap_current_view": bool(value.get("bootstrap_current_view")),
            "prospective_eligible": bool(value.get("prospective_eligible")),
            "direction_policy": "abstain",
            "raw_archive_sha256": value.get("raw_archive_sha256"),
            "upstream_source_contract": value.get("source_contract_json"),
            "upstream_source_cohort_id": value.get("cohort_id"),
            "canonical_source_binding": "canonical_contract_cohort_v2",
        }
        payload_hash = stable_hash(payload)
        provider_event_id = str(value.get("yield_date") or value.get("observation_id"))
        observed = str(value.get("observed_utc") or utc_now())
        event = {
            "source_event_id": "source_event_" + stable_hash(
                (contract["source_id"], provider_event_id, payload_hash, observed)
            )[:32],
            "provider_event_id": provider_event_id,
            "source_id": contract["source_id"],
            "source_family": "rate_curve_snapshot",
            "source_population": "official_rates_curve",
            "provider": "U.S. Department of the Treasury",
            "instrument": None,
            "base_currency": "USD",
            "quote_currency": None,
            "country": "US",
            "event_type": "daily_treasury_yield_curve",
            "article_id": None,
            "story_cluster_id": None,
            "event_id": provider_event_id,
            "market_episode_id": None,
            "published_at_utc": value.get("feed_updated_utc"),
            "first_seen_at_utc": observed,
            "retrieved_at_utc": observed,
            "effective_from_utc": observed,
            "valid_until_utc": None,
            "revised_at_utc": observed if int(value.get("observation_version") or 1) > 1 else None,
            "superseded_at_utc": None,
            "decision_cutoff_utc": observed,
            "source_clock_uncertainty_ms": 1000,
            "raw_value": finite_or_none(value.get("two_year_pct")),
            "normalized_value": finite_or_none(value.get("curve_2s10s_bps")),
            "coverage_count": 1,
            "source_dispersion": None,
            "quality_state": (
                "prospective_first_seen_source_observation"
                if bool(value.get("prospective_eligible"))
                else "bootstrap_or_revision_nonproof"
            ),
            "latency_ms": None,
            "parser_version": "us_treasury_yield_prospective_v1",
            "vendor_model_name": None,
            "vendor_model_version": None,
            "raw_payload_sha256": payload_hash,
            "license_class": contract["license_class"],
            "retention_policy": contract["raw_payload_retention"],
            "source_contract_id": contract["source_contract_id"],
            "source_cohort_id": contract["source_cohort_id"],
            "event_version": 1,
            "supersedes_source_event_id": None,
            "payload_json": canonical_json(payload),
        }
        inserted += int(insert_source_event(connection, event))
        if processed % batch_size == 0 or processed == total:
            # Identity links are a derived append-only acceleration index.
            # Commit them in bounded batches so a supervised restart resumes
            # instead of replaying the whole retained Treasury history.
            connection.commit()
            if progress_callback is not None:
                progress_callback(
                    {
                        "processed": processed,
                        "total": total,
                        "inserted_source_events": inserted,
                    }
                )
    return inserted


def source_card(
    configured: dict[str, Any], runtime: dict[str, Any] | None,
    contract: dict[str, Any], *, observed_utc: str,
) -> dict[str, Any]:
    runtime = runtime or {}
    operational = bool(runtime.get("operational"))
    healthy = bool(runtime.get("healthy"))
    causal_clock_trusted = runtime.get("causal_clock_trusted") is True
    state = str(runtime.get("runtime_status") or "configured_not_observed")
    currencies = sorted(set(configured.get("currencies") or runtime.get("currencies") or []))
    card = {
        "source_id": contract["source_id"],
        "source_contract_id": contract["source_contract_id"],
        "source_cohort_id": contract["source_cohort_id"],
        "information_population": contract["information_population"],
        "causal_timestamp_quality": (
            "first_seen_prospective_clock_bound"
            if operational and causal_clock_trusted
            else "clock_untrusted_or_not_currently_collecting"
        ),
        "historical_backfill_quality": (
            "requires_point_in_time_validation" if "vintage" not in str(configured.get("kind"))
            else "point_in_time_vintage_adapter"
        ),
        "coverage": currencies,
        "missingness_state": "available" if healthy else "degraded_or_missing",
        "latency_distribution": "measured_per_source_event_when published_at exists",
        "revision_frequency": "measured from immutable source-event versions",
        "duplicate_rate": "measured by article/story cluster counts",
        "outage_state": state,
        "last_error": str(runtime.get("last_error") or ""),
        "license": contract["license_class"],
        "retention_rights": contract["raw_payload_retention"],
        "monetary_cost": str(configured.get("monetary_cost") or "unknown_or_free"),
        "storage_cost": "measured_from_registry_payloads",
        "compute_cost": "measured_by_collector",
        "maintenance_cost": "unscored",
        "incremental_predictive_value": "unproven",
        "incremental_allocator_value": "unproven",
        "placebo_result": "pending",
        "ablation_result": "pending",
        "retirement_state": "active" if operational else "blocked_or_dormant",
        "runtime_supported": bool(configured.get("runtime_supported", True)),
        "runtime_adapter": str(configured.get("runtime_adapter") or ""),
        "credential_environment": str(configured.get("credential_env") or ""),
        "observed_utc": observed_utc,
        "research_only": True,
        "execution_eligible": False,
    }
    return card


def source_reliability_statistics(
    connection: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Summarize one last immutable source-card observation per UTC day.

    A card is appended when its state changes, so treating every raw card row
    as an equal availability observation would overweight unstable sources.
    This diagnostic uses only the final observation for each source/day and
    labels short histories as limited-sample. It is not a trading feature.
    """

    latest_by_day: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
    for observed_utc, source_id, card_json in connection.execute(
        """
        SELECT observed_utc, source_id, card_json
        FROM source_card_snapshots
        ORDER BY observed_utc, source_id
        """
    ).fetchall():
        try:
            card = json.loads(str(card_json or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        observed = str(observed_utc or "")
        parsed = parse_time(observed)
        if parsed is None:
            continue
        key = (str(source_id or ""), parsed.date().isoformat())
        prior = latest_by_day.get(key)
        if prior is None or observed >= prior[0]:
            latest_by_day[key] = (observed, card)

    event_stats: dict[str, dict[str, Any]] = {}
    for row in connection.execute(
        """
        SELECT source_id,
               COUNT(*),
               COUNT(DISTINCT COALESCE(story_cluster_id, source_event_id)),
               SUM(CASE WHEN event_version > 1 THEN 1 ELSE 0 END),
               AVG(latency_ms),
               MAX(latency_ms)
        FROM source_events_causal_v1
        GROUP BY source_id
        """
    ).fetchall():
        source_id, events, stories, revisions, mean_latency, maximum_latency = row
        event_stats[str(source_id)] = {
            "causal_event_versions": int(events or 0),
            "independent_story_clusters": int(stories or 0),
            "revision_versions": int(revisions or 0),
            "mean_latency_ms": (
                None if mean_latency is None else round(float(mean_latency), 3)
            ),
            "maximum_latency_ms": (
                None if maximum_latency is None else int(maximum_latency)
            ),
        }

    daily: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    for (source_id, day), value in latest_by_day.items():
        daily.setdefault(source_id, []).append((day, value[1]))
    output: list[dict[str, Any]] = []
    for source_id in sorted(set(daily) | set(event_stats)):
        registered_samples = sorted(
            daily.get(source_id) or [], key=lambda item: item[0]
        )
        first_collecting_index = next(
            (
                index
                for index, (_, card) in enumerate(registered_samples)
                if str(card.get("outage_state") or "")
                != "configured_not_observed"
            ),
            len(registered_samples),
        )
        samples = registered_samples[first_collecting_index:]
        available_days = sum(
            card.get("missingness_state") == "available"
            and not str(card.get("last_error") or "")
            for _, card in samples
        )
        clock_trusted_days = sum(
            card.get("causal_timestamp_quality")
            == "first_seen_prospective_clock_bound"
            for _, card in samples
        )
        last_card = samples[-1][1] if samples else {}
        observation_days = len(samples)
        output.append(
            {
                "source_id": source_id,
                "registered_observation_days": len(registered_samples),
                "observation_days": observation_days,
                "available_days": available_days,
                "degraded_days": max(0, observation_days - available_days),
                "availability_day_rate": (
                    None
                    if observation_days == 0
                    else round(available_days / observation_days, 6)
                ),
                "clock_trusted_days": clock_trusted_days,
                "first_registered_day": (
                    registered_samples[0][0] if registered_samples else None
                ),
                "first_observed_day": samples[0][0] if samples else None,
                "last_observed_day": samples[-1][0] if samples else None,
                "current_outage_state": str(last_card.get("outage_state") or ""),
                "current_last_error": str(last_card.get("last_error") or ""),
                "sample_quality": (
                    "prospective_30d_or_more"
                    if observation_days >= 30
                    else "limited_sample_current_state"
                ),
                **event_stats.get(
                    source_id,
                    {
                        "causal_event_versions": 0,
                        "independent_story_clusters": 0,
                        "revision_versions": 0,
                        "mean_latency_ms": None,
                        "maximum_latency_ms": None,
                    },
                ),
                "research_only": True,
                "execution_eligible": False,
            }
        )
    return output


def governance_progress_payload(
    prior_state: dict[str, Any],
    *,
    phase: str,
    progress: dict[str, int],
    configured_news_source_count: int,
    configured_source_count: int,
    incremental_article_scan: bool,
    article_scan_since_utc: str | None,
) -> dict[str, Any]:
    """Return an honest in-progress state under the current source contract."""

    progress_state = dict(prior_state)
    progress_state.update(
        {
            "schema_version": 1,
            "generated_utc": utc_now(),
            "status": "building_governance",
            "research_only": True,
            "can_place_orders": False,
            "real_money_routing": False,
            "configured_news_source_count": configured_news_source_count,
            "configured_source_count": configured_source_count,
            "article_identity_contract_id": ARTICLE_IDENTITY_CONTRACT_ID,
            "news_collector_contract_id": LOCAL_NEWS_COLLECTOR_CONTRACT_ID,
            "news_collector_cohort_id": LOCAL_NEWS_COLLECTOR_COHORT_ID,
            "official_fast_lane_governance_adapter_contract_id": (
                OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID
            ),
            "official_fast_lane_governance_adapter_activated_utc": (
                OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC.isoformat()
            ),
            "article_scan_mode": (
                "incremental" if incremental_article_scan else "full"
            ),
            "article_scan_since_utc": article_scan_since_utc,
            "build_phase": phase,
            "build_progress": progress,
        }
    )
    return progress_state


def run(
    *, config_path: Path = DEFAULT_CONFIG, coverage_path: Path = DEFAULT_COVERAGE,
    news_database: Path = DEFAULT_NEWS, cftc_path: Path = DEFAULT_CFTC,
    treasury_path: Path = DEFAULT_TREASURY,
    fast_lane_database: Path = DEFAULT_OFFICIAL_FAST_LANE,
    official_central_bank_map_path: Path = DEFAULT_OFFICIAL_CENTRAL_BANK_MAP,
    database_path: Path = DEFAULT_DATABASE, state_path: Path = DEFAULT_STATE,
    report_path: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    observed = utc_now()
    config = read_json(config_path)
    official_central_bank_map = read_json(official_central_bank_map_path)
    coverage = read_json(coverage_path)
    runtime = runtime_sources(coverage)
    runtime = merge_collector_runtime(
        runtime, read_json(coverage_path.with_name("collector_latest_v1.json"))
    )
    # The separately supervised CFTC collector supersedes the dormant embedded
    # collector state without rewriting its original source definition.
    cftc = read_json(cftc_path)
    if cftc.get("currency_count"):
        runtime["cftc_cot_positioning"] = {
            "source_id": "cftc_cot_positioning", "runtime_status": "enabled_separate_forward_collector",
            "operational": True, "healthy": not bool(cftc.get("errors")),
            "currencies": sorted((cftc.get("currencies") or {}).keys()), "last_error": "",
        }
    treasury_rows = treasury_observations(treasury_path)
    treasury_runtime_state = read_json(treasury_path.with_suffix(".json"))
    if treasury_rows:
        runtime["us_treasury_daily_yield_curve"] = {
            "source_id": "us_treasury_daily_yield_curve",
            "runtime_status": "enabled_separate_forward_collector",
            "operational": True,
            "healthy": treasury_runtime_state.get("status") == "ok",
            "currencies": ["USD"],
            "last_error": str(treasury_runtime_state.get("error") or ""),
        }
    configured = []
    for source in configured_sources(config):
        live = runtime.get(str(source.get("source_id") or "")) or {}
        merged = dict(source)
        for key in ("source_role", "retrieval_via", "name", "kind"):
            if live.get(key):
                merged[key] = live[key]
        merged["currencies"] = sorted(
            set(source.get("currencies") or []) | set(live.get("currencies") or [])
        )
        configured.append(merged)
    configured_news_source_count = len(configured)
    if treasury_rows:
        configured.append({
            "source_id": "us_treasury_daily_yield_curve",
            "name": "U.S. Treasury Daily Par Yield Curve",
            "kind": "rate_curve_snapshot",
            "source_role": "primary_rates_curve",
            "retrieval_via": "official_xml_prospective_first_seen",
            "currencies": ["USD"],
            "verified": True,
            "license_class": "public_official_research_use",
            "parser_version": "us_treasury_yield_prospective_v1",
            "monetary_cost": "free",
            "upstream_source_cohort_id": str(
                (treasury_runtime_state.get("cohort") or {}).get("cohort_id") or ""
            ),
        })
    by_id = {str(row["source_id"]): row for row in configured}
    connection = connect_registry(database_path)
    try:
        inserted_quarantines = quarantine_known_migration_artifacts(
            connection, detected_utc=observed
        )
        contracts = insert_contracts(connection, configured)
        prior_state = read_json(state_path)
        incremental_article_scan = bool(
            prior_state.get("status") == "ok"
            and prior_state.get("article_identity_contract_id")
            == ARTICLE_IDENTITY_CONTRACT_ID
            and prior_state.get("generated_utc")
        )
        article_scan_since_utc = (
            str(prior_state.get("generated_utc"))
            if incremental_article_scan
            else None
        )
        article_rows = list(
            article_events(
                news_database, changed_after_utc=article_scan_since_utc
            )
        )

        def publish_build_progress(
            phase: str, progress: dict[str, int]
        ) -> None:
            progress_state = governance_progress_payload(
                prior_state,
                phase=phase,
                progress=progress,
                configured_news_source_count=configured_news_source_count,
                configured_source_count=len(configured),
                incremental_article_scan=incremental_article_scan,
                article_scan_since_utc=article_scan_since_utc,
            )
            atomic_json(state_path, progress_state)

        identity_backfill = backfill_article_substantive_identities(
            connection,
            article_rows,
            contracts,
            observed_utc=observed,
            progress_callback=lambda progress: publish_build_progress(
                "backfilling_substantive_replay_index", progress
            ),
        )
        inserted_articles = 0
        inserted_initial_articles = 0
        inserted_revision_articles = 0
        inserted_story_assignments = 0
        article_total = len(article_rows)
        for article_index, row in enumerate(article_rows, start=1):
            contract = contracts.get(str(row.get("source_id") or ""))
            if contract is not None:
                event = _event_payload(row, contract, observed_utc=observed)
                if event is not None:
                    event_inserted = bool(insert_source_event(connection, event))
                    inserted_articles += int(event_inserted)
                    if event_inserted and int(event.get("event_version") or 1) == 1:
                        inserted_initial_articles += 1
                    elif event_inserted:
                        inserted_revision_articles += 1
                    inserted_story_assignments += int(
                        insert_story_assignment(connection, event, assigned_utc=observed)
                    )
            if article_index % 1000 == 0 or article_index == article_total:
                # Source events and assignments are append-only.  Bounded
                # commits make a long retained-corpus reconciliation
                # resumable, while the progress heartbeat prevents the
                # supervisor from mistaking useful work for a stalled worker.
                connection.commit()
                publish_build_progress(
                    "reconciling_article_source_events",
                    {
                        "processed": article_index,
                        "total": article_total,
                        "inserted_article_versions": inserted_articles,
                        "inserted_initial_article_versions": inserted_initial_articles,
                        "inserted_revision_article_versions": inserted_revision_articles,
                        "inserted_story_cluster_assignments": inserted_story_assignments,
                    },
                )
        inserted_cftc = 0
        allowed_fast_lane_source_ids = official_fast_lane_source_ids(
            official_central_bank_map
        )
        expected_fast_lane_source_lineages = official_fast_lane_source_lineages(
            config,
            allowed_source_ids=allowed_fast_lane_source_ids,
        )
        official_fast_lane_adapter = import_official_fast_lane_events(
            connection,
            fast_lane_database=fast_lane_database,
            allowed_source_ids=allowed_fast_lane_source_ids,
            expected_source_lineages=expected_fast_lane_source_lineages,
            governance_contracts=contracts,
            observed_utc=observed,
        )
        if cftc and "cftc_cot_positioning" in contracts:
            inserted_cftc = insert_cftc_events(
                connection, cftc, contracts["cftc_cot_positioning"]
            )
        inserted_treasury = 0
        if treasury_rows and "us_treasury_daily_yield_curve" in contracts:
            inserted_treasury = insert_treasury_events(
                connection,
                treasury_rows,
                contracts["us_treasury_daily_yield_curve"],
                progress_callback=lambda progress: publish_build_progress(
                    "indexing_treasury_source_events", progress
                ),
            )
        cards = []
        day = observed[:10]
        for source_id, source in sorted(by_id.items()):
            card = source_card(source, runtime.get(source_id), contracts[source_id], observed_utc=observed)
            snapshot_identity = {
                key: value for key, value in card.items() if key != "observed_utc"
            }
            snapshot_id = "source_card_" + stable_hash((day, snapshot_identity))[:28]
            connection.execute(
                "INSERT OR IGNORE INTO source_card_snapshots VALUES (?,?,?,?,?)",
                (snapshot_id, observed, source_id, contracts[source_id]["source_contract_id"], canonical_json(card)),
            )
            cards.append(card)
        connection.commit()
        reliability = source_reliability_statistics(connection)
        counts = {
            "contracts": int(connection.execute("SELECT COUNT(*) FROM source_contracts").fetchone()[0]),
            "events": int(connection.execute("SELECT COUNT(*) FROM source_events").fetchone()[0]),
            "causal_events": int(connection.execute(
                "SELECT COUNT(*) FROM source_events_causal_v1"
            ).fetchone()[0]),
            "quarantined_events": int(connection.execute(
                "SELECT COUNT(*) FROM source_event_quarantines"
            ).fetchone()[0]),
            "story_clusters": int(connection.execute(
                "SELECT COUNT(DISTINCT story_cluster_id) FROM source_story_cluster_assignments WHERE clustering_contract_id=?",
                (STORY_CLUSTER_CONTRACT_ID,),
            ).fetchone()[0]),
            "story_cluster_assignments": int(connection.execute(
                "SELECT COUNT(*) FROM source_story_cluster_assignments WHERE clustering_contract_id=?",
                (STORY_CLUSTER_CONTRACT_ID,),
            ).fetchone()[0]),
            "legacy_embedded_story_clusters": int(connection.execute(
                "SELECT COUNT(DISTINCT story_cluster_id) FROM source_events WHERE story_cluster_id IS NOT NULL"
            ).fetchone()[0]),
            "supersessions": int(connection.execute("SELECT COUNT(*) FROM source_supersession_events").fetchone()[0]),
            "substantive_identities": int(connection.execute(
                "SELECT COUNT(*) FROM source_event_substantive_identities"
            ).fetchone()[0]),
            "source_card_snapshots": int(connection.execute("SELECT COUNT(*) FROM source_card_snapshots").fetchone()[0]),
            "official_fast_lane_import_receipts": int(connection.execute(
                "SELECT COUNT(*) FROM official_fast_lane_governance_imports"
            ).fetchone()[0]),
        }
    finally:
        connection.close()
    runtime_count = len(runtime)
    operational = sum(bool(card["retirement_state"] == "active") for card in cards)
    payload = {
        "schema_version": 1, "generated_utc": observed, "status": "ok",
        "research_only": True, "can_place_orders": False,
        "real_money_routing": False,
        "configured_news_source_count": configured_news_source_count,
        "configured_source_count": len(configured),
        "runtime_observed_source_count": runtime_count,
        "operational_source_count": operational,
        "inserted_article_versions": inserted_articles,
        "inserted_initial_article_versions": inserted_initial_articles,
        "inserted_revision_article_versions": inserted_revision_articles,
        "inserted_quarantines": inserted_quarantines,
        "inserted_story_cluster_assignments": inserted_story_assignments,
        "inserted_cftc_versions": inserted_cftc,
        "inserted_treasury_versions": inserted_treasury,
        "inserted_official_fast_lane_versions": int(
            official_fast_lane_adapter.get("inserted_versions") or 0
        ),
        "official_fast_lane_source_governance_adapter": (
            official_fast_lane_adapter
        ),
        "official_fast_lane_governance_adapter_contract_id": (
            OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID
        ),
        "official_fast_lane_governance_adapter_activated_utc": (
            OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC.isoformat()
        ),
        "identity_backfill": identity_backfill,
        "registry": str(database_path.resolve()), "counts": counts,
        "story_cluster_contract_id": STORY_CLUSTER_CONTRACT_ID,
        "article_identity_contract_id": ARTICLE_IDENTITY_CONTRACT_ID,
        "news_collector_contract_id": LOCAL_NEWS_COLLECTOR_CONTRACT_ID,
        "news_collector_cohort_id": LOCAL_NEWS_COLLECTOR_COHORT_ID,
        "article_scan_mode": "incremental" if incremental_article_scan else "full",
        "article_scan_since_utc": article_scan_since_utc,
        "source_cards": cards,
        "source_reliability": reliability,
        "source_reliability_contract": (
            "last_immutable_source_card_per_source_utc_day_v1"
        ),
        "canonical_timestamp_policy": (
            "effective_from/decision_cutoff use earliest local first_seen, "
            "except detail-derived or structured numeric content uses the later "
            "detail/numeric causal-availability timestamp; quarantined migration "
            "artifacts are excluded from causal replay; "
            "publication is provenance; post-activation official fast-lane "
            "listings begin at exact first_seen and later body/PDF material "
            "begins only at its later observed availability; revisions append"
        ),
    }
    lines = [
        "# FX source governance", "", f"Generated: `{observed}`", "",
        "Research-only; cannot place orders or alter lifecycle state.", "",
        f"- Configured sources: **{len(configured)}**",
        f"- Runtime-observed/operational: **{runtime_count} / {operational}**",
        f"- Immutable raw/causal/quarantined source events: **{counts['events']} / {counts['causal_events']} / {counts['quarantined_events']}**",
        f"- Story clusters: **{counts['story_clusters']}**",
        f"- Immutable supersessions: **{counts['supersessions']}**", "",
        f"- Prospective official fast-lane import receipts: **{counts['official_fast_lane_import_receipts']}**",
        f"- Official fast-lane adapter status: **{official_fast_lane_adapter.get('status')}**",
        "",
        f"- Compact substantive replay identities: **{counts['substantive_identities']}**", "",
        "Source reliability uses the final immutable card per source/UTC day; histories under 30 days are explicitly limited-sample.", "",
        "| Source | Observed days | Available | Causal events | Stories | Mean latency ms | Sample |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in reliability:
        lines.append(
            f"| {row['source_id']} | {row['observation_days']} | "
            f"{row['available_days']} | {row['causal_event_versions']} | "
            f"{row['independent_story_clusters']} | "
            f"{row['mean_latency_ms'] if row['mean_latency_ms'] is not None else ''} | "
            f"{row['sample_quality']} |"
        )
    lines.extend([
        "",
        "| Source | Population | State | Currencies | Timestamp quality | License |",
        "|---|---|---|---|---|---|",
    ])
    for card in cards:
        lines.append(
            f"| {card['source_id']} | {card['information_population']} | {card['outage_state']} | "
            f"{','.join(card['coverage'])} | {card['causal_timestamp_quality']} | {card['license']} |"
        )
    atomic_json(state_path, payload)
    atomic_text(report_path, "\n".join(lines) + "\n")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--coverage", type=Path, default=DEFAULT_COVERAGE)
    parser.add_argument("--news-database", type=Path, default=DEFAULT_NEWS)
    parser.add_argument("--cftc", type=Path, default=DEFAULT_CFTC)
    parser.add_argument("--treasury-database", type=Path, default=DEFAULT_TREASURY)
    parser.add_argument(
        "--official-fast-lane-database", type=Path,
        default=DEFAULT_OFFICIAL_FAST_LANE,
    )
    parser.add_argument(
        "--official-central-bank-map", type=Path,
        default=DEFAULT_OFFICIAL_CENTRAL_BANK_MAP,
    )
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.time()
    while True:
        run(
            config_path=args.config, coverage_path=args.coverage,
            news_database=args.news_database, cftc_path=args.cftc,
            treasury_path=args.treasury_database,
            fast_lane_database=args.official_fast_lane_database,
            official_central_bank_map_path=args.official_central_bank_map,
            database_path=args.database, state_path=args.state, report_path=args.report,
        )
        if args.interval_sec <= 0.0 or (
            args.duration_sec > 0.0 and time.time() - started >= args.duration_sec
        ):
            break
        time.sleep(max(60.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "backfill_article_substantive_identities", "connect_registry", "contract_for",
    "import_official_fast_lane_events", "official_fast_lane_source_ids",
    "official_fast_lane_source_lineages",
    "insert_source_event",
    "insert_treasury_events", "source_reliability_statistics",
    "treasury_observations", "run",
]
