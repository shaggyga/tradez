#!/usr/bin/env python3
"""Prospectively compare mover explanations using receipt-backed map clocks.

The frozen V7R3 movement-first cohort uses the immutable source event's causal
clock, which can precede the time at which a semantic mapping became available
to a downstream consumer.  This sidecar does not rewrite that cohort.  For
newly starting moves only, it seals a second research-only interpretation that
admits an event only when an exact current fast-lane mapping receipt was
available before move onset.

Selection remains conditioned on an already-realized executable move.  The
output is an explanation diagnostic and can never promote, authorize, or place
an order.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import re
import sqlite3
import time
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import oanda_live_move_news_snapshot as snapshot
import oanda_major_move_gap_census as census
import oanda_move_first_news_case_audit as news_audit
from oanda_official_release_fast_lane_contract import (
    OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
)
from oanda_source_governance_news_fast_lane import CONTRACT_ID as NEWS_MAPPING_CONTRACT_ID


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "move_first_operational_mapping_alignment_v1"
)
DEFAULT_CONFIG = (
    ROOT / "config" / "move_first_operational_mapping_alignment_v1_20260901.json"
)
DEFAULT_SOURCE_CASE_DATABASE = STATE / "move_first_live_case_capture_v4_20260901.sqlite"
DEFAULT_SOURCE_GOVERNANCE_DATABASE = STATE / "source_governance_v1.sqlite"
DEFAULT_LEDGER = STATE / "move_first_operational_mapping_alignment_v1_20260901.sqlite"
DEFAULT_JSON_REPORT = REPORT_ROOT / "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json"
DEFAULT_MD_REPORT = REPORT_ROOT / "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.md"

SCHEMA_VERSION = "move_first_operational_mapping_alignment_ledger_v1"
REPORT_SCHEMA_VERSION = "move_first_operational_mapping_alignment_report_v1"
CONTRACT_ID = (
    "move_first_operational_mapping_alignment_v1_receipt_backed_"
    "prospective_20260901T140000Z"
)
DEFAULT_STORY_DEDUPLICATION_RULE = "normalized_full_headline_v1"
PUBLISHER_SUFFIX_STORY_DEDUPLICATION_RULE = (
    "publisher_suffix_stripped_normalized_headline_v2"
)
NARRATIVE_FAMILY_STORY_DEDUPLICATION_RULE = (
    "conservative_time_bounded_narrative_family_v3"
)
SUPPORTED_STORY_DEDUPLICATION_RULES = {
    DEFAULT_STORY_DEDUPLICATION_RULE,
    PUBLISHER_SUFFIX_STORY_DEDUPLICATION_RULE,
    NARRATIVE_FAMILY_STORY_DEDUPLICATION_RULE,
}
NARRATIVE_FAMILY_MAX_SEPARATION_SEC = 2 * 60 * 60
NARRATIVE_FAMILY_MIN_SHARED_TERMS = 3
NARRATIVE_FAMILY_MIN_OVERLAP_COEFFICIENT = 0.5
_HEADLINE_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "before",
        "by",
        "central",
        "currency",
        "economy",
        "for",
        "forex",
        "from",
        "in",
        "is",
        "it",
        "latest",
        "market",
        "markets",
        "moving",
        "near",
        "news",
        "of",
        "on",
        "or",
        "policy",
        "rate",
        "rates",
        "report",
        "said",
        "says",
        "the",
        "to",
        "today",
        "update",
        "what",
        "with",
    }
)
_HEADLINE_TERM_ALIASES = {
    "attacks": "attack",
    "hikes": "hike",
    "hitting": "hit",
    "hits": "hit",
    "projectiles": "projectile",
    "strikes": "hit",
    "struck": "hit",
    "tankers": "tanker",
}


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(value, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _parse_epoch(value: Any) -> float | None:
    """Parse knowledge clocks without discarding sub-second ordering.

    The movement census intentionally uses integer minute boundaries. Mapping
    receipts do not: a receipt at ``11:25:00.757`` was not available at the
    ``11:25:00.000`` move onset.  Reusing the census integer parser erased that
    distinction and could admit information observed just after onset.
    """

    if value in (None, ""):
        return None
    try:
        number = float(value)
        if math.isfinite(number) and number > 1_000_000_000:
            return number
    except (TypeError, ValueError):
        pass
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.timestamp()


def _iso_epoch(value: int | float) -> str:
    return dt.datetime.fromtimestamp(value, tz=dt.timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _direction(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, (int, float)):
        return 1 if value > 0 else -1 if value < 0 else 0
    normalized = str(value or "").strip().lower()
    if normalized in {"up", "long", "buy", "+1", "positive"}:
        return 1
    if normalized in {"down", "short", "sell", "-1", "negative"}:
        return -1
    return 0


def _side(value: int) -> str:
    return "long" if value > 0 else "short" if value < 0 else "neutral"


def _alignment(direction: int, actual: int) -> str:
    if direction == actual and direction:
        return "aligned"
    if direction == -actual and direction:
        return "opposed"
    return "abstained"


def _contract_id(config: Mapping[str, Any]) -> str:
    return str(config.get("contract_id") or CONTRACT_ID)


def _story_deduplication_rule(config: Mapping[str, Any]) -> str:
    return str(
        config.get("story_deduplication_rule")
        or DEFAULT_STORY_DEDUPLICATION_RULE
    )


def _broad_age_decay_half_life_minutes(
    config: Mapping[str, Any],
) -> float | None:
    raw = config.get("broad_age_decay_half_life_minutes")
    if raw is None:
        return None
    return float(raw)


def _publisher_suffix_stripped_headline(event: Mapping[str, Any]) -> str:
    """Return a conservative content key for cross-publisher syndication.

    Upstream story-cluster identifiers are source-local in some discovery
    lanes.  A wire headline repeated as ``headline - Publisher`` can therefore
    arrive under several cluster IDs and look like independent consensus.  The
    V2 sidecar strips only the final publisher-style suffix before exact
    Unicode-normalized comparison; it deliberately does not fuzzy-match or
    merge merely similar stories.
    """

    payload = news_audit.parse_payload(event)
    headline = str(
        payload.get("headline")
        or payload.get("event_name")
        or payload.get("summary")
        or ""
    ).strip()
    if not headline:
        return ""
    headline = unicodedata.normalize("NFKC", headline).replace("\ufffd", " ")
    parts = re.split(r"\s+[-\u2013\u2014]\s+", headline)
    if len(parts) > 1:
        prefix = " - ".join(parts[:-1]).strip()
        suffix = parts[-1].strip()
        if len(prefix.split()) >= 4 and 1 <= len(suffix.split()) <= 12:
            headline = prefix
    return re.sub(r"\W+", " ", headline.lower(), flags=re.UNICODE).strip()


def _operational_story_key(
    event: Mapping[str, Any], rule: str
) -> str:
    if rule == PUBLISHER_SUFFIX_STORY_DEDUPLICATION_RULE:
        headline = _publisher_suffix_stripped_headline(event)
        if headline:
            return f"headline_content:{headline}"
    return news_audit.independent_story_key(event)


def _headline_terms(event: Mapping[str, Any]) -> frozenset[str]:
    headline = _publisher_suffix_stripped_headline(event)
    terms: set[str] = set()
    for raw_term in re.findall(r"[^\W_]+", headline, flags=re.UNICODE):
        term = _HEADLINE_TERM_ALIASES.get(raw_term, raw_term)
        if len(term) < 3 or term in _HEADLINE_STOPWORDS:
            continue
        terms.add(term)
    return frozenset(terms)


def _narrative_similarity_edge(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> bool:
    if str(left.get("event_type") or "") != str(right.get("event_type") or ""):
        return False
    separation = abs(
        float(left.get("effective_epoch") or 0)
        - float(right.get("effective_epoch") or 0)
    )
    if separation > NARRATIVE_FAMILY_MAX_SEPARATION_SEC:
        return False
    left_terms = _headline_terms(left)
    right_terms = _headline_terms(right)
    if not left_terms or not right_terms:
        return False
    shared = len(left_terms & right_terms)
    overlap = shared / min(len(left_terms), len(right_terms))
    return bool(
        shared >= NARRATIVE_FAMILY_MIN_SHARED_TERMS
        and overlap >= NARRATIVE_FAMILY_MIN_OVERLAP_COEFFICIENT
    )


def _dedupe_narrative_families(
    events: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Collapse conservative headline-similarity components to first-seen rows.

    This is intentionally stricter than a topic model.  It only links stories
    of the same classified event type, within the operational lookback, that
    share at least three lexical anchors and at least half of the shorter
    headline's anchors.  The earliest causally available member represents the
    component, so later syndication cannot increase vote weight or choose a
    more favorable publisher confidence.
    """

    exact, _ = _dedupe_operational_events(
        events, PUBLISHER_SUFFIX_STORY_DEDUPLICATION_RULE
    )
    if len(exact) < 2:
        return exact
    parent = list(range(len(exact)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root == right_root:
            return
        if left_root < right_root:
            parent[right_root] = left_root
        else:
            parent[left_root] = right_root

    for left in range(len(exact)):
        for right in range(left + 1, len(exact)):
            if _narrative_similarity_edge(exact[left], exact[right]):
                union(left, right)

    components: dict[int, list[dict[str, Any]]] = {}
    for index, event in enumerate(exact):
        components.setdefault(find(index), []).append(dict(event))
    selected: list[dict[str, Any]] = []
    for members in components.values():
        ordered = sorted(
            members,
            key=lambda event: (
                float(event.get("effective_epoch") or 0),
                str(event.get("source_event_id") or ""),
            ),
        )
        representative = dict(ordered[0])
        family_identity = [
            str(event.get("source_event_id") or "") for event in ordered
        ]
        representative["operational_narrative_family_key"] = (
            "narrative_family:" + sha256_text(canonical_json(family_identity))[:24]
        )
        representative["operational_narrative_family_member_count"] = len(ordered)
        selected.append(representative)
    return sorted(
        selected,
        key=lambda event: (
            float(event.get("effective_epoch") or 0),
            str(event.get("source_event_id") or ""),
        ),
    )


def _dedupe_operational_events(
    events: Sequence[Mapping[str, Any]], rule: str
) -> tuple[list[dict[str, Any]], int]:
    if rule == DEFAULT_STORY_DEDUPLICATION_RULE:
        return [dict(event) for event in events], 0
    if rule == NARRATIVE_FAMILY_STORY_DEDUPLICATION_RULE:
        deduped = _dedupe_narrative_families(events)
        return deduped, max(0, len(events) - len(deduped))
    selected: dict[str, dict[str, Any]] = {}
    for raw in sorted(
        events,
        key=lambda event: (
            float(event.get("effective_epoch") or 0),
            str(event.get("source_event_id") or ""),
        ),
    ):
        event = dict(raw)
        key = _operational_story_key(event, rule)
        if not key:
            key = f"source_event:{event.get('source_event_id') or ''}"
        selected.setdefault(key, event)
    deduped = sorted(
        selected.values(),
        key=lambda event: (
            float(event.get("effective_epoch") or 0),
            str(event.get("source_event_id") or ""),
        ),
    )
    return deduped, max(0, len(events) - len(deduped))


def load_config(path: Path) -> dict[str, Any]:
    value = read_json(path)
    required = {
        "schema_version",
        "cohort_id",
        "cohort_start_utc",
        "source_case_cohort_id",
        "source_case_ledger_schema_version",
        "source_case_database_contract_id",
        "factor_episode_contract_id",
        "news_mapping_contract_id",
        "official_mapping_contract_id",
        "operational_clock_rule",
        "lookback_minutes",
        "deduplication_rule",
    }
    missing = sorted(required - set(value))
    if missing:
        raise ValueError(f"config missing required fields: {missing}")
    if value["schema_version"] != "move_first_operational_mapping_alignment_config_v1":
        raise ValueError("unexpected operational-mapping config schema")
    if _parse_epoch(value["cohort_start_utc"]) is None:
        raise ValueError("invalid cohort start")
    if int(value["lookback_minutes"]) < 1:
        raise ValueError("lookback must be positive")
    contract_id = _contract_id(value)
    if not contract_id or not contract_id.startswith(
        "move_first_operational_mapping_alignment_"
    ):
        raise ValueError("invalid operational-mapping contract id")
    story_deduplication_rule = _story_deduplication_rule(value)
    if story_deduplication_rule not in SUPPORTED_STORY_DEDUPLICATION_RULES:
        raise ValueError("unsupported story deduplication rule")
    broad_half_life = _broad_age_decay_half_life_minutes(value)
    if broad_half_life is not None and broad_half_life <= 0:
        raise ValueError("broad age-decay half-life must be positive")
    if (
        story_deduplication_rule == NARRATIVE_FAMILY_STORY_DEDUPLICATION_RULE
        and broad_half_life is None
    ):
        raise ValueError("narrative-family cohort requires broad age decay")
    if value["news_mapping_contract_id"] != NEWS_MAPPING_CONTRACT_ID:
        raise ValueError("news mapping contract is not the exact current contract")
    if (
        value["official_mapping_contract_id"]
        != OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID
    ):
        raise ValueError("official mapping contract is not the exact current contract")
    allowed_operational_clock_rules = {
        "max_source_effective_detail_available_and_mapping_receipt_available_utc",
        (
            "max_source_effective_detail_available_and_mapping_receipt_"
            "available_utc_subsecond_precision"
        ),
    }
    if value["operational_clock_rule"] not in allowed_operational_clock_rules:
        raise ValueError("unexpected operational clock rule")
    required_false = (
        "predictive_backtest_eligible",
        "promotion_eligible",
        "execution_eligible",
        "can_authorize",
        "can_place_orders",
    )
    if value.get("selection_conditioned_on_realized_executable_move") is not True:
        raise ValueError("realized-move conditioning must remain explicit")
    if value.get("research_only") is not True:
        raise ValueError("cohort must remain research only")
    if any(value.get(key) is not False for key in required_false):
        raise ValueError("cohort must remain nonexecuting and nonpromotional")
    return value


def _open_read_only(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(path)
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.row_factory = sqlite3.Row
    connection.execute("BEGIN")
    return connection


def _manifest(connection: sqlite3.Connection) -> dict[str, str]:
    return {
        str(row["key"]): str(row["value"])
        for row in connection.execute("SELECT key,value FROM manifest")
    }


def _resolve_root(value: str, edges: Mapping[str, str]) -> str:
    current = value
    seen: set[str] = set()
    while current in edges:
        if current in seen:
            raise RuntimeError(f"factor root merge cycle: {current}")
        seen.add(current)
        current = edges[current]
    return current


def load_source_cases(
    path: Path,
    config: Mapping[str, Any],
    *,
    merge_cutoff_utc: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, str], str | None]:
    connection = _open_read_only(path)
    try:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity != "ok":
            raise RuntimeError(f"source-case integrity failed: {integrity}")
        observed = _manifest(connection)
        expected = {
            "schema_version": str(config["source_case_ledger_schema_version"]),
            "cohort_id": str(config["source_case_cohort_id"]),
            "source_database_contract_id": str(
                config["source_case_database_contract_id"]
            ),
            "factor_episode_contract_id": str(config["factor_episode_contract_id"]),
            "historical_rows_imported": "0",
            "execution_eligible": "false",
        }
        failures = [key for key, expected_value in expected.items() if observed.get(key) != expected_value]
        if failures:
            raise RuntimeError(f"source-case manifest mismatch: {failures}")

        memberships: dict[str, sqlite3.Row] = {}
        for row in connection.execute("SELECT * FROM factor_memberships"):
            case_id = str(row["source_case_id"])
            if str(row["membership_sha256"]) != sha256_text(str(row["membership_json"])):
                raise RuntimeError(f"membership hash mismatch: {case_id}")
            memberships[case_id] = row

        edges: dict[str, str] = {}
        merge_cutoff: str | None = None
        merge_query = "SELECT * FROM factor_root_merges"
        merge_parameters: tuple[Any, ...] = ()
        if merge_cutoff_utc is not None:
            merge_query += " WHERE inserted_utc<=?"
            merge_parameters = (merge_cutoff_utc,)
        merge_query += " ORDER BY inserted_utc,merge_id"
        for row in connection.execute(merge_query, merge_parameters):
            if str(row["merge_sha256"]) != sha256_text(str(row["merge_json"])):
                raise RuntimeError(f"root-merge hash mismatch: {row['merge_id']}")
            source = str(row["from_root_id"])
            target = str(row["into_root_id"])
            if source in edges and edges[source] != target:
                raise RuntimeError(f"conflicting root merge: {source}")
            edges[source] = target
            merge_cutoff = str(row["inserted_utc"])
        for root in tuple(edges):
            _resolve_root(root, edges)

        cases: list[dict[str, Any]] = []
        for row in connection.execute(
            "SELECT * FROM cases ORDER BY first_recorded_utc,source_case_id"
        ):
            case_id = str(row["source_case_id"])
            case_json = str(row["case_json"])
            case_hash = str(row["case_sha256"])
            if case_hash != sha256_text(case_json):
                raise RuntimeError(f"source case hash mismatch: {case_id}")
            membership = memberships.get(case_id)
            if membership is None:
                raise RuntimeError(f"case missing factor membership: {case_id}")
            if str(membership["factor_episode_contract_id"]) != str(
                config["factor_episode_contract_id"]
            ):
                raise RuntimeError(f"factor contract mismatch: {case_id}")
            payload = json.loads(case_json)
            if str(payload.get("case_id") or "") != case_id:
                raise RuntimeError(f"case identity mismatch: {case_id}")
            if payload.get("execution_eligible") is not False:
                raise RuntimeError(f"source case is not explicitly inert: {case_id}")
            episode = str(membership["factor_episode_id"])
            cases.append(
                {
                    "source_case_id": case_id,
                    "source_case_sha256": case_hash,
                    "source_case_json": case_json,
                    "first_recorded_utc": str(row["first_recorded_utc"]),
                    "instrument": str(row["instrument"]),
                    "factor_episode_id": episode,
                    "resolved_factor_episode_id": _resolve_root(episode, edges),
                    "factor_primary_token": str(membership["factor_primary_token"]),
                    "payload": payload,
                }
            )
        if len(cases) != len(memberships):
            raise RuntimeError("source case and membership counts differ")
        return cases, edges, merge_cutoff or merge_cutoff_utc
    finally:
        connection.close()


def _table_exists(connection: sqlite3.Connection, name: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name=? AND type IN ('table','view')",
            (name,),
        ).fetchone()
        is not None
    )


def _event_currencies(row: Mapping[str, Any]) -> set[str]:
    values = {
        str(value).upper()
        for value in (row.get("base_currency"), row.get("quote_currency"))
        if value
    }
    values.update(census._payload_currencies(str(row.get("payload_json") or "")))
    return values


def _mapped_rows(
    connection: sqlite3.Connection,
    *,
    receipt_table: str,
    available_column: str,
    mapping_kind: str,
    contract_id: str,
) -> Iterable[sqlite3.Row]:
    return connection.execute(
        f"""SELECT event.source_event_id,event.source_id,event.source_population,
                   event.event_type,event.story_cluster_id,event.effective_from_utc,
                   event.valid_until_utc,event.superseded_at_utc,event.base_currency,
                   event.quote_currency,event.payload_json,event.published_at_utc,
                   event.first_seen_at_utc,event.retrieved_at_utc,event.revised_at_utc,
                   event.event_version,event.supersedes_source_event_id,
                   receipt.receipt_id,
                   receipt.{available_column} AS mapping_available_utc,
                   receipt.adapter_contract_id,receipt.research_only,
                   receipt.execution_eligible,receipt.can_authorize,
                   ? AS mapping_kind
            FROM source_events_causal_v1 AS event
            JOIN {receipt_table} AS receipt
              ON receipt.source_event_id=event.source_event_id
            WHERE receipt.adapter_contract_id=?
            ORDER BY receipt.{available_column},event.source_event_id""",
        (mapping_kind, contract_id),
    )


def load_operational_events(
    path: Path, config: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    connection = _open_read_only(path)
    try:
        # The live governance database is multi-gigabyte.  A full SQLite
        # integrity scan here would turn a minute-level read-only sidecar into
        # a multi-minute competing workload.  The independent project
        # integrity worker owns full-file checks; this hot path verifies the
        # exact schema, contracts, receipt safety flags, joins, and clocks it
        # consumes while holding a read-only snapshot transaction.
        connection.execute("PRAGMA query_only=ON")
        if int(connection.execute("PRAGMA schema_version").fetchone()[0]) <= 0:
            raise RuntimeError("source-governance schema is unavailable")
        required = (
            "source_events_causal_v1",
            "news_fast_lane_import_receipts",
            "official_fast_lane_governance_imports",
        )
        missing = [name for name in required if not _table_exists(connection, name)]
        if missing:
            raise RuntimeError(f"operational mapping inputs missing: {missing}")
        specifications = (
            (
                "news_fast_lane_import_receipts",
                "governance_available_utc",
                "general_news_fast_lane",
                str(config["news_mapping_contract_id"]),
            ),
            (
                "official_fast_lane_governance_imports",
                "imported_utc",
                "official_release_fast_lane",
                str(config["official_mapping_contract_id"]),
            ),
        )
        by_id: dict[str, dict[str, Any]] = {}
        for table, available_column, kind, contract in specifications:
            for raw in _mapped_rows(
                connection,
                receipt_table=table,
                available_column=available_column,
                mapping_kind=kind,
                contract_id=contract,
            ):
                row = dict(raw)
                if not (
                    int(row["research_only"] or 0) == 1
                    and int(row["execution_eligible"] or 0) == 0
                    and int(row["can_authorize"] or 0) == 0
                ):
                    raise RuntimeError(f"unsafe mapping receipt: {row['receipt_id']}")
                recorded_epoch, detail_adjusted = census.causal_effective_epoch(
                    row.get("effective_from_utc"), str(row.get("payload_json") or "{}")
                )
                mapping_epoch = _parse_epoch(row.get("mapping_available_utc"))
                if recorded_epoch is None or mapping_epoch is None:
                    raise RuntimeError(f"invalid mapping clock: {row['receipt_id']}")
                operational_epoch = max(recorded_epoch, mapping_epoch)
                event_id = str(row["source_event_id"])
                event = {
                    "source_event_id": event_id,
                    "source_id": str(row.get("source_id") or ""),
                    "source_population": str(row.get("source_population") or ""),
                    "event_type": str(row.get("event_type") or ""),
                    "story_cluster_id": str(row.get("story_cluster_id") or ""),
                    "effective_from_utc": _iso_epoch(operational_epoch),
                    "recorded_effective_from_utc": str(row.get("effective_from_utc") or ""),
                    "mapping_available_utc": str(row.get("mapping_available_utc") or ""),
                    "operational_effective_from_utc": _iso_epoch(operational_epoch),
                    "effective_epoch": operational_epoch,
                    "valid_until_epoch": _parse_epoch(row.get("valid_until_utc")),
                    "superseded_epoch": _parse_epoch(row.get("superseded_at_utc")),
                    "base_currency": str(row.get("base_currency") or ""),
                    "quote_currency": str(row.get("quote_currency") or ""),
                    "payload_json": str(row.get("payload_json") or "{}"),
                    "published_at_utc": str(row.get("published_at_utc") or ""),
                    "first_seen_at_utc": str(row.get("first_seen_at_utc") or ""),
                    "retrieved_at_utc": str(row.get("retrieved_at_utc") or ""),
                    "revised_at_utc": str(row.get("revised_at_utc") or ""),
                    "event_version": int(row.get("event_version") or 1),
                    "supersedes_source_event_id": str(
                        row.get("supersedes_source_event_id") or ""
                    ),
                    "mapping_receipt_id": str(row.get("receipt_id") or ""),
                    "mapping_contract_id": str(row.get("adapter_contract_id") or ""),
                    "mapping_kind": str(row.get("mapping_kind") or ""),
                    "knowledge_time_adjusted_for_detail": detail_adjusted,
                }
                event["currencies"] = sorted(_event_currencies(event))
                existing = by_id.get(event_id)
                if existing is None or (
                    float(event["effective_epoch"]), event["mapping_receipt_id"]
                ) < (
                    float(existing["effective_epoch"]), existing["mapping_receipt_id"]
                ):
                    by_id[event_id] = event
        events = sorted(
            by_id.values(),
            key=lambda row: (float(row["effective_epoch"]), row["source_event_id"]),
        )
        return events, by_id
    finally:
        connection.close()


def ensure_ledger(path: Path, config: Mapping[str, Any]) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS manifest(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS operational_cases(
            operational_case_id TEXT PRIMARY KEY,
            source_case_id TEXT NOT NULL UNIQUE,
            source_case_sha256 TEXT NOT NULL,
            factor_episode_id TEXT NOT NULL,
            source_case_first_recorded_utc TEXT NOT NULL,
            move_start_utc TEXT NOT NULL,
            mapping_snapshot_utc TEXT NOT NULL,
            case_json TEXT NOT NULL,
            case_sha256 TEXT NOT NULL,
            inserted_utc TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_operational_cases_inserted
            ON operational_cases(inserted_utc,operational_case_id);
        CREATE TRIGGER IF NOT EXISTS operational_cases_no_update
        BEFORE UPDATE ON operational_cases BEGIN
            SELECT RAISE(ABORT,'operational mapping cases are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS operational_cases_no_delete
        BEFORE DELETE ON operational_cases BEGIN
            SELECT RAISE(ABORT,'operational mapping cases are immutable');
        END;
        """
    )
    expected = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": _contract_id(config),
        "cohort_id": str(config["cohort_id"]),
        "cohort_start_utc": str(config["cohort_start_utc"]),
        "source_case_cohort_id": str(config["source_case_cohort_id"]),
        "source_case_database_contract_id": str(
            config["source_case_database_contract_id"]
        ),
        "factor_episode_contract_id": str(config["factor_episode_contract_id"]),
        "news_mapping_contract_id": str(config["news_mapping_contract_id"]),
        "official_mapping_contract_id": str(config["official_mapping_contract_id"]),
        "historical_rows_imported": "0",
        "selection_conditioned_on_realized_executable_move": "true",
        "research_only": "true",
        "execution_eligible": "false",
        "can_authorize": "false",
        "can_place_orders": "false",
    }
    # The already-sealed V1 manifest predates this optional field.  Bind it
    # only for newly declared cohorts that explicitly include the rule, so a
    # code deployment can never mutate or invalidate V1's immutable manifest.
    if "story_deduplication_rule" in config:
        expected["story_deduplication_rule"] = _story_deduplication_rule(config)
    observed = {
        str(row["key"]): str(row["value"])
        for row in connection.execute("SELECT key,value FROM manifest")
    }
    if not observed:
        connection.executemany("INSERT INTO manifest VALUES(?,?)", sorted(expected.items()))
        connection.commit()
    else:
        failures = [key for key, value in expected.items() if observed.get(key) != value]
        if failures:
            connection.close()
            raise RuntimeError(f"operational ledger manifest mismatch: {failures}")
    return connection


def _eligible_events(
    events: Sequence[Mapping[str, Any]],
    *,
    base: str,
    quote: str,
    start_epoch: float,
    lookback_minutes: int,
) -> list[dict[str, Any]]:
    minimum = start_epoch - int(lookback_minutes) * 60
    currencies = {base, quote}
    output: list[dict[str, Any]] = []
    for raw in events:
        event = dict(raw)
        effective = float(event["effective_epoch"])
        if effective < minimum or effective > start_epoch:
            continue
        if not currencies.intersection(event.get("currencies") or []):
            continue
        valid_until = event.get("valid_until_epoch")
        superseded = event.get("superseded_epoch")
        if valid_until is not None and float(valid_until) <= start_epoch:
            continue
        if superseded is not None and float(superseded) <= start_epoch:
            continue
        output.append(event)
    return output


def _story(event: Mapping[str, Any], base: str, quote: str) -> dict[str, Any]:
    value = snapshot._context_story(event, base, quote)
    value.update(
        {
            "mapping_receipt_id": str(event.get("mapping_receipt_id") or ""),
            "mapping_contract_id": str(event.get("mapping_contract_id") or ""),
            "mapping_kind": str(event.get("mapping_kind") or ""),
            "mapping_available_utc": str(event.get("mapping_available_utc") or ""),
            "operational_effective_from_utc": str(
                event.get("operational_effective_from_utc") or ""
            ),
            "recorded_effective_from_utc": str(
                event.get("recorded_effective_from_utc") or ""
            ),
            "operational_narrative_family_key": str(
                event.get("operational_narrative_family_key") or ""
            ),
            "operational_narrative_family_member_count": int(
                event.get("operational_narrative_family_member_count") or 1
            ),
        }
    )
    return value


def _context_stories(
    events: Sequence[Mapping[str, Any]],
    base: str,
    quote: str,
    limit: int = 10,
    *,
    story_deduplication_rule: str = DEFAULT_STORY_DEDUPLICATION_RULE,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for event in reversed(events):
        key = _operational_story_key(event, story_deduplication_rule)
        if not key or key in seen:
            continue
        seen.add(key)
        value = _story(event, base, quote)
        if value.get("headline"):
            output.append(value)
        if len(output) >= limit:
            break
    return output


def build_operational_case(
    source_case: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    events_by_id: Mapping[str, Mapping[str, Any]],
    config: Mapping[str, Any],
    *,
    observed_utc: str,
) -> dict[str, Any]:
    payload = source_case["payload"]
    instrument = str(source_case["instrument"])
    base, quote = instrument.split("_", 1)
    start_epoch = _parse_epoch(payload.get("start_utc"))
    actual = _direction(payload.get("move_direction"))
    if start_epoch is None or not actual:
        raise RuntimeError(f"invalid source mover: {source_case['source_case_id']}")
    admitted = _eligible_events(
        events,
        base=base,
        quote=quote,
        start_epoch=start_epoch,
        lookback_minutes=int(config["lookback_minutes"]),
    )
    story_deduplication_rule = _story_deduplication_rule(config)
    vote_events, syndicated_duplicate_count = _dedupe_operational_events(
        admitted, story_deduplication_rule
    )
    strict = news_audit.directional_vote(
        vote_events, base, quote, start_epoch, strict_forward=True
    )
    broad_half_life = _broad_age_decay_half_life_minutes(config)
    broad = news_audit.directional_vote(
        vote_events,
        base,
        quote,
        start_epoch,
        age_decay_half_life_minutes=broad_half_life,
    )
    strict_direction = int(strict.get("side") or 0)
    broad_direction = int(broad.get("side") or 0)
    frozen_arms = payload.get("forward_shadow_arms") or {}
    legacy_strict = _direction(frozen_arms.get("strict_forward_direction"))
    legacy_broad = _direction(frozen_arms.get("broad_context_direction"))
    technical = _direction(frozen_arms.get("technical_continuation"))

    minimum_epoch = start_epoch - int(config["lookback_minutes"]) * 60
    legacy_story_statuses: list[dict[str, Any]] = []
    exclusion_counts: Counter[str] = Counter()
    for raw_story in payload.get("recent_context_stories") or []:
        story = dict(raw_story)
        event_id = str(story.get("source_event_id") or "")
        mapped = events_by_id.get(event_id)
        if mapped is None:
            status = "no_current_mapping_receipt"
            mapping_available = ""
        else:
            operational_epoch = float(mapped["effective_epoch"])
            mapping_available = str(mapped.get("mapping_available_utc") or "")
            if operational_epoch > start_epoch:
                status = "mapping_available_after_move_start"
            elif operational_epoch < minimum_epoch:
                status = "outside_operational_lookback"
            else:
                status = "admitted"
        exclusion_counts[status] += 1
        legacy_story_statuses.append(
            {
                "source_event_id": event_id,
                "story_cluster_id": str(story.get("story_cluster_id") or ""),
                "headline": str(story.get("headline") or "")[:500],
                "legacy_effective_from_utc": str(story.get("effective_from_utc") or ""),
                "mapping_available_utc": mapping_available,
                "status": status,
            }
        )

    mapping_kinds = Counter(str(event["mapping_kind"]) for event in admitted)
    independent_story_count = len(
        {
            _operational_story_key(event, story_deduplication_rule)
            for event in vote_events
            if _operational_story_key(event, story_deduplication_rule)
        }
    )
    operational_case_id = "operational_mapping_case_" + sha256_text(
        f"{config['cohort_id']}|{source_case['source_case_id']}"
    )[:32]
    return {
        "schema_version": "move_first_operational_mapping_case_v1",
        "contract_id": _contract_id(config),
        "cohort_id": str(config["cohort_id"]),
        "operational_case_id": operational_case_id,
        "source_case_id": str(source_case["source_case_id"]),
        "source_case_sha256": str(source_case["source_case_sha256"]),
        "source_case_first_recorded_utc": str(source_case["first_recorded_utc"]),
        "factor_episode_id": str(source_case["factor_episode_id"]),
        "factor_primary_token": str(source_case["factor_primary_token"]),
        "instrument": instrument,
        "move_start_utc": str(payload.get("start_utc") or ""),
        "move_end_utc": str(payload.get("end_utc") or ""),
        "move_direction": "up" if actual > 0 else "down",
        "executable_net_pips": payload.get("executable_net_pips"),
        "move_bps": payload.get("move_bps"),
        "mapping_snapshot_utc": observed_utc,
        "operational_clock_rule": str(config["operational_clock_rule"]),
        "lookback_minutes": int(config["lookback_minutes"]),
        "legacy_pre_move_event_count": int(payload.get("pre_move_event_count") or 0),
        "operational_pre_move_event_count": len(admitted),
        "operational_independent_story_count": independent_story_count,
        "operational_syndicated_duplicate_count": syndicated_duplicate_count,
        "operational_story_family_duplicate_count": (
            syndicated_duplicate_count
            if story_deduplication_rule
            == NARRATIVE_FAMILY_STORY_DEDUPLICATION_RULE
            else 0
        ),
        "story_deduplication_rule": story_deduplication_rule,
        "broad_age_decay_half_life_minutes": broad_half_life,
        "operational_mapping_kind_counts": dict(sorted(mapping_kinds.items())),
        "legacy_recent_story_mapping_status_counts": dict(
            sorted(exclusion_counts.items())
        ),
        "legacy_recent_story_mapping_statuses": legacy_story_statuses,
        "operational_context_stories": _context_stories(
            vote_events,
            base,
            quote,
            story_deduplication_rule=story_deduplication_rule,
        ),
        "operational_event_receipts": [
            {
                "source_event_id": str(event["source_event_id"]),
                "mapping_receipt_id": str(event["mapping_receipt_id"]),
                "mapping_contract_id": str(event["mapping_contract_id"]),
                "mapping_kind": str(event["mapping_kind"]),
                "mapping_available_utc": str(event["mapping_available_utc"]),
                "operational_effective_from_utc": str(
                    event["operational_effective_from_utc"]
                ),
                "operational_narrative_family_key": str(
                    event.get("operational_narrative_family_key") or ""
                ),
                "operational_narrative_family_member_count": int(
                    event.get("operational_narrative_family_member_count") or 1
                ),
            }
            for event in admitted
        ],
        "directions": {
            "legacy_strict": legacy_strict,
            "legacy_broad": legacy_broad,
            "receipt_backed_strict": strict_direction,
            "receipt_backed_broad": broad_direction,
            "technical_continuation_control": technical,
        },
        "alignments": {
            "legacy_strict": _alignment(legacy_strict, actual),
            "legacy_broad": _alignment(legacy_broad, actual),
            "receipt_backed_strict": _alignment(strict_direction, actual),
            "receipt_backed_broad": _alignment(broad_direction, actual),
            "technical_continuation_control": _alignment(technical, actual),
        },
        "receipt_backed_strict_vote": strict,
        "receipt_backed_broad_vote": broad,
        "source_case_bytes_rewritten": False,
        "selection_conditioned_on_realized_executable_move": True,
        "predictive_backtest_eligible": False,
        "promotion_eligible": False,
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
    }


def append_new_cases(
    connection: sqlite3.Connection,
    source_cases: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
    events_by_id: Mapping[str, Mapping[str, Any]],
    config: Mapping[str, Any],
) -> int:
    activation = _parse_epoch(config["cohort_start_utc"])
    assert activation is not None
    existing = {
        str(row["source_case_id"]): str(row["source_case_sha256"])
        for row in connection.execute(
            "SELECT source_case_id,source_case_sha256 FROM operational_cases"
        )
    }
    inserted = 0
    for source_case in source_cases:
        start = _parse_epoch(source_case["payload"].get("start_utc"))
        if start is None or start < activation:
            continue
        source_id = str(source_case["source_case_id"])
        observed_hash = existing.get(source_id)
        if observed_hash is not None:
            if observed_hash != str(source_case["source_case_sha256"]):
                raise RuntimeError(f"sealed source case mutated: {source_id}")
            continue
        inserted_utc = utc_now()
        payload = build_operational_case(
            source_case,
            events,
            events_by_id,
            config,
            observed_utc=inserted_utc,
        )
        case_json = canonical_json(payload)
        connection.execute(
            """INSERT INTO operational_cases VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (
                payload["operational_case_id"],
                source_id,
                str(source_case["source_case_sha256"]),
                str(source_case["factor_episode_id"]),
                str(source_case["first_recorded_utc"]),
                str(payload["move_start_utc"]),
                inserted_utc,
                case_json,
                sha256_text(case_json),
                inserted_utc,
            ),
        )
        existing[source_id] = str(source_case["source_case_sha256"])
        inserted += 1
    connection.commit()
    return inserted


def _ledger_rows(
    connection: sqlite3.Connection, cutoff_utc: str | None = None
) -> list[dict[str, Any]]:
    query = "SELECT * FROM operational_cases"
    parameters: tuple[Any, ...] = ()
    if cutoff_utc:
        query += " WHERE inserted_utc<=?"
        parameters = (cutoff_utc,)
    query += " ORDER BY inserted_utc,operational_case_id"
    output: list[dict[str, Any]] = []
    for row in connection.execute(query, parameters):
        case_json = str(row["case_json"])
        if str(row["case_sha256"]) != sha256_text(case_json):
            raise RuntimeError(f"operational case hash mismatch: {row['operational_case_id']}")
        payload = json.loads(case_json)
        if payload.get("execution_eligible") is not False:
            raise RuntimeError(f"unsafe operational case: {row['operational_case_id']}")
        output.append({"row": dict(row), "payload": payload})
    return output


def _select_factor_episodes(
    rows: Sequence[Mapping[str, Any]], edges: Mapping[str, str]
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for item in rows:
        root = _resolve_root(str(item["row"]["factor_episode_id"]), edges)
        grouped.setdefault(root, []).append(item)
    selected: list[dict[str, Any]] = []
    for root, members in sorted(grouped.items()):
        chosen = min(
            members,
            key=lambda item: (
                str(item["row"]["source_case_first_recorded_utc"]),
                str(item["row"]["source_case_id"]),
            ),
        )
        value = {"resolved_factor_episode_id": root, **dict(chosen["payload"])}
        value["root_member_count"] = len(members)
        selected.append(value)
    selected.sort(key=lambda row: (row["move_start_utc"], row["source_case_id"]))
    return selected


def _arm_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    arms = (
        "legacy_strict",
        "legacy_broad",
        "receipt_backed_strict",
        "receipt_backed_broad",
        "technical_continuation_control",
    )
    output: dict[str, dict[str, Any]] = {}
    for arm in arms:
        alignments = Counter(str(row["alignments"][arm]) for row in rows)
        signaled = alignments["aligned"] + alignments["opposed"]
        output[arm] = {
            "episode_count": len(rows),
            "signaled_episode_count": signaled,
            "aligned_episode_count": alignments["aligned"],
            "opposed_episode_count": alignments["opposed"],
            "abstained_episode_count": alignments["abstained"],
            "diagnostic_directional_accuracy_pct": (
                round(100.0 * alignments["aligned"] / signaled, 6)
                if signaled
                else None
            ),
        }
    return output


def build_report(
    connection: sqlite3.Connection,
    config: Mapping[str, Any],
    edges: Mapping[str, str],
    *,
    ledger_cutoff_utc: str,
    factor_merge_cutoff_utc: str | None,
) -> dict[str, Any]:
    rows = _ledger_rows(connection, ledger_cutoff_utc)
    episodes = _select_factor_episodes(rows, edges)
    exclusions: Counter[str] = Counter()
    mapping_kinds: Counter[str] = Counter()
    for row in episodes:
        exclusions.update(row.get("legacy_recent_story_mapping_status_counts") or {})
        mapping_kinds.update(row.get("operational_mapping_kind_counts") or {})
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "generated_utc": utc_now(),
        "contract_id": _contract_id(config),
        "cohort_id": str(config["cohort_id"]),
        "story_deduplication_rule": _story_deduplication_rule(config),
        "broad_age_decay_half_life_minutes": _broad_age_decay_half_life_minutes(
            config
        ),
        "operational_syndicated_duplicate_count": sum(
            int(row.get("operational_syndicated_duplicate_count") or 0)
            for row in episodes
        ),
        "operational_story_family_duplicate_count": sum(
            int(row.get("operational_story_family_duplicate_count") or 0)
            for row in episodes
        ),
        "cohort_start_utc": str(config["cohort_start_utc"]),
        "operational_clock_rule": str(config["operational_clock_rule"]),
        "config_sha256": sha256_text(canonical_json(config)),
        "ledger_cutoff_utc": ledger_cutoff_utc,
        "factor_merge_cutoff_utc": factor_merge_cutoff_utc,
        "source_case_count": len(rows),
        "resolved_factor_episode_count": len(episodes),
        "receipt_backed_event_count": sum(
            int(row.get("operational_pre_move_event_count") or 0) for row in episodes
        ),
        "receipt_backed_independent_story_count": sum(
            int(row.get("operational_independent_story_count") or 0) for row in episodes
        ),
        "mapping_kind_counts": dict(sorted(mapping_kinds.items())),
        "legacy_recent_story_mapping_status_counts": dict(sorted(exclusions.items())),
        "arm_metrics": _arm_metrics(episodes),
        "episode_rows": episodes,
        "source_case_bytes_rewritten": False,
        "historical_rows_imported": 0,
        "selection_conditioned_on_realized_executable_move": True,
        "predictive_backtest_eligible": False,
        "promotion_eligible": False,
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
    }


def render_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Receipt-backed operational mapping alignment",
        "",
        f"Generated: `{payload.get('generated_utc')}`",
        "",
        "This prospective sidecar preserves the frozen mover cohort and admits a",
        "semantic source only when an exact current mapping receipt was available",
        "before move onset. It is movement-conditioned and cannot execute.",
        "",
        f"- Sealed source cases: **{payload.get('source_case_count')}**",
        f"- Independent factor episodes: **{payload.get('resolved_factor_episode_count')}**",
        f"- Receipt-backed events: **{payload.get('receipt_backed_event_count')}**",
        f"- Receipt-backed independent stories: **{payload.get('receipt_backed_independent_story_count')}**",
        f"- Story deduplication: **{payload.get('story_deduplication_rule')}**",
        f"- Broad-vote age-decay half-life: **{payload.get('broad_age_decay_half_life_minutes') or 'none'} minutes**",
        f"- Collapsed narrative-family duplicates: **{payload.get('operational_story_family_duplicate_count')}**",
        "",
        "| Arm | Signaled | Aligned | Opposed | Abstained | Diagnostic accuracy |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for arm, metric in (payload.get("arm_metrics") or {}).items():
        accuracy = metric.get("diagnostic_directional_accuracy_pct")
        lines.append(
            f"| `{arm}` | {metric.get('signaled_episode_count')} | "
            f"{metric.get('aligned_episode_count')} | {metric.get('opposed_episode_count')} | "
            f"{metric.get('abstained_episode_count')} | "
            f"{accuracy if accuracy is not None else 'n/a'} |"
        )
    lines.extend(
        [
            "",
            "Accuracy is diagnostic only because cases were selected after an",
            "executable move existed. Execution decision: **no_trade**.",
            "",
        ]
    )
    return "\n".join(lines)


def run_once(
    config_path: Path = DEFAULT_CONFIG,
    source_case_database: Path = DEFAULT_SOURCE_CASE_DATABASE,
    source_governance_database: Path = DEFAULT_SOURCE_GOVERNANCE_DATABASE,
    ledger_path: Path = DEFAULT_LEDGER,
    json_report: Path = DEFAULT_JSON_REPORT,
    md_report: Path = DEFAULT_MD_REPORT,
) -> dict[str, Any]:
    config = load_config(config_path)
    source_cases, edges, merge_cutoff = load_source_cases(source_case_database, config)
    events, events_by_id = load_operational_events(source_governance_database, config)
    connection = ensure_ledger(ledger_path, config)
    try:
        inserted = append_new_cases(
            connection, source_cases, events, events_by_id, config
        )
        cutoff_row = connection.execute(
            "SELECT MAX(inserted_utc) FROM operational_cases"
        ).fetchone()
        ledger_cutoff = str(cutoff_row[0] or config["cohort_start_utc"])
        payload = build_report(
            connection,
            config,
            edges,
            ledger_cutoff_utc=ledger_cutoff,
            factor_merge_cutoff_utc=merge_cutoff or str(config["cohort_start_utc"]),
        )
        payload["inserted_this_run"] = inserted
        payload["source_mapping_event_inventory"] = len(events)
    finally:
        connection.close()
    atomic_text(json_report, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    atomic_text(md_report, render_markdown(payload))
    return payload


def verify(
    config_path: Path = DEFAULT_CONFIG,
    source_case_database: Path = DEFAULT_SOURCE_CASE_DATABASE,
    ledger_path: Path = DEFAULT_LEDGER,
    json_report: Path = DEFAULT_JSON_REPORT,
) -> dict[str, Any]:
    failures: list[str] = []
    if not json_report.exists():
        return {"verified": False, "failures": ["report_missing"]}
    observed = read_json(json_report)
    config = load_config(config_path)
    observed_merge_cutoff = str(
        observed.get("factor_merge_cutoff_utc") or config["cohort_start_utc"]
    )
    _, edges, merge_cutoff = load_source_cases(
        source_case_database,
        config,
        merge_cutoff_utc=observed_merge_cutoff,
    )
    connection = ensure_ledger(ledger_path, config)
    try:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity != "ok":
            failures.append(f"ledger_integrity:{integrity}")
        rebuilt = build_report(
            connection,
            config,
            edges,
            ledger_cutoff_utc=str(observed.get("ledger_cutoff_utc") or ""),
            factor_merge_cutoff_utc=merge_cutoff or observed_merge_cutoff,
        )
    finally:
        connection.close()
    stable_keys = (
        "contract_id",
        "cohort_id",
        "cohort_start_utc",
        "operational_clock_rule",
        "config_sha256",
        "ledger_cutoff_utc",
        "factor_merge_cutoff_utc",
        "source_case_count",
        "resolved_factor_episode_count",
        "receipt_backed_event_count",
        "receipt_backed_independent_story_count",
        "operational_syndicated_duplicate_count",
        "operational_story_family_duplicate_count",
        "story_deduplication_rule",
        "broad_age_decay_half_life_minutes",
        "mapping_kind_counts",
        "legacy_recent_story_mapping_status_counts",
        "arm_metrics",
        "episode_rows",
        "source_case_bytes_rewritten",
        "historical_rows_imported",
        "selection_conditioned_on_realized_executable_move",
        "predictive_backtest_eligible",
        "promotion_eligible",
        "research_only",
        "execution_eligible",
        "can_authorize",
        "can_place_orders",
        "execution_decision",
    )
    for key in stable_keys:
        if observed.get(key) != rebuilt.get(key):
            failures.append(f"report_mismatch:{key}")
    return {
        "verified": not failures,
        "failures": failures,
        "contract_id": _contract_id(config),
        "source_case_count": rebuilt["source_case_count"],
        "resolved_factor_episode_count": rebuilt["resolved_factor_episode_count"],
        "checked_utc": utc_now(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument(
        "--source-case-database", type=Path, default=DEFAULT_SOURCE_CASE_DATABASE
    )
    parser.add_argument(
        "--source-governance-database",
        type=Path,
        default=DEFAULT_SOURCE_GOVERNANCE_DATABASE,
    )
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--json-report", type=Path, default=DEFAULT_JSON_REPORT)
    parser.add_argument("--md-report", type=Path, default=DEFAULT_MD_REPORT)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress recurring report payloads on stdout; report files are unchanged.",
    )
    args = parser.parse_args()
    if args.verify_only:
        result = verify(
            args.config, args.source_case_database, args.ledger, args.json_report
        )
        if not args.quiet:
            print(json.dumps(result, indent=2, sort_keys=True), flush=True)
        return 0 if result["verified"] else 1
    stop = time.monotonic() + max(0.0, float(args.duration_sec))
    while True:
        result = run_once(
            args.config,
            args.source_case_database,
            args.source_governance_database,
            args.ledger,
            args.json_report,
            args.md_report,
        )
        if not args.quiet:
            print(json.dumps(result, indent=2, sort_keys=True), flush=True)
        if float(args.interval_sec) <= 0.0 or time.monotonic() >= stop:
            return 0
        time.sleep(min(float(args.interval_sec), max(0.0, stop - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
