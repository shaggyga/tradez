"""Inert V4 integrity successor for frozen policy/watch provenance V3.

V4 preserves V3 as an exact dependency and adds only four bounded repairs:

* proof-grade policy documents require a nonempty immutable content SHA-256;
* same-key policy variants fail closed with an order-independent conflict ID;
* syndicated source events collapse to one canonical story observation; and
* event, story, and payload mappings must be unambiguous in both directions.

The module is pure and has no file, database, network, process, broker,
registration, promotion, authorization, or runtime surface.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
from collections import Counter, defaultdict
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from . import persistent_policy_watch_provenance_v3 as v3


POLICY_CONTEXT_CONTRACT_ID = (
    "persistent_policy_context_v4_content_conflict_20260817"
)
WATCH_PROVENANCE_CONTRACT_ID = (
    "news_technical_watchlist_v40_story_digest_candidate_20260817"
)
EPISODE_SEMANTICS_ID = (
    "point_in_time_canonical_story_cluster_currency_factor_v2_20260817"
)
EPISODE_SEMANTICS = (
    "One canonical story cluster for one currency factor; syndicated event "
    "copies sharing one unambiguous story/payload mapping count once, exact "
    "duplicates do not increase evidence, and ambiguous event, story, or "
    "payload mappings fail closed."
)

REQUIRED_OBSERVATION_TIME_CONTRACT_ID = v3.REQUIRED_OBSERVATION_TIME_CONTRACT_ID
REQUIRED_COLLECTOR_CONTRACT_ID = v3.REQUIRED_COLLECTOR_CONTRACT_ID
REQUIRED_COLLECTOR_COHORT_ID = v3.REQUIRED_COLLECTOR_COHORT_ID
CANONICAL_CURRENCIES = v3.CANONICAL_CURRENCIES
CANONICAL_INSTRUMENTS = v3.CANONICAL_INSTRUMENTS
CURRENCY_SET = frozenset(CANONICAL_CURRENCIES)
INSTRUMENT_SET = frozenset(CANONICAL_INSTRUMENTS)
ARM_ALLOWLIST = v3.ARM_ALLOWLIST
DIRECTION_ALLOWLIST = v3.DIRECTION_ALLOWLIST
WATCH_COHORT_BY_ARM = MappingProxyType(
    {
        arm: f"{WATCH_PROVENANCE_CONTRACT_ID}.{arm}"
        for arm in sorted(ARM_ALLOWLIST)
    }
)

POLICY_DOCUMENT_FIELDS = frozenset(v3.POLICY_DOCUMENT_FIELDS)
CURRENCY_ROW_FIELDS = frozenset(v3.CURRENCY_ROW_FIELDS)
COVERAGE_FIELDS = frozenset(v3.COVERAGE_FIELDS)
POLICY_OUTPUT_FIELDS = frozenset(v3.POLICY_OUTPUT_FIELDS)
REJECTION_FIELDS = frozenset(
    {"event_id", "source_id", "currencies", "reason", "conflict_version_sha256"}
)
SOURCE_EVIDENCE_FIELDS = frozenset(
    {
        "event_id",
        "story_cluster_id",
        "payload_sha256",
        "observation_clock_trusted",
        "observation_time_contract_id",
        "collector_contract_id",
        "collector_cohort_id",
    }
)
STORY_EVIDENCE_FIELDS = frozenset(
    {
        "story_cluster_id",
        "payload_sha256",
        "event_ids",
        "observation_clock_trusted",
        "observation_time_contract_id",
        "collector_contract_id",
        "collector_cohort_id",
    }
)
FACTOR_OUTPUT_FIELDS = frozenset(
    {
        "episode_id",
        "episode_semantics_id",
        "episode_semantics",
        "currency",
        "score",
        "confidence",
        "horizon_min",
        "event_ids",
        "story_cluster_ids",
        "payload_sha256s",
        "source_story_evidence",
        "source_event_count",
        "source_story_count",
        "source_evidence_digest_sha256",
        "observation_clock_trusted",
        "observation_time_contract_id",
        "collector_contract_id",
        "collector_cohort_id",
        "watch_provenance_contract_id",
        "research_only",
        "execution_eligible",
    }
)
WATCH_OUTPUT_FIELDS = frozenset(
    {
        "entry_id",
        "cohort_id",
        "watch_provenance_contract_id",
        "episode_id",
        "episode_semantics_id",
        "currency",
        "instrument",
        "arm",
        "direction",
        "horizon_min",
        "source_evidence_digest_sha256",
        "source_event_count",
        "source_story_count",
        "factor_score",
        "factor_confidence",
        "event_ids",
        "story_cluster_ids",
        "payload_sha256s",
        "source_story_evidence",
        "observation_clock_trusted",
        "observation_time_contract_id",
        "collector_contract_id",
        "collector_cohort_id",
        "research_only",
        "execution_eligible",
    }
)


class ContractViolation(ValueError):
    """Raised when V4 cannot prove a closed, deterministic research record."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _plain(value: Any, *, path: str = "$") -> Any:
    if value is None or type(value) in {str, int, bool}:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ContractViolation(f"nonfinite_json:{path}")
        return value
    if type(value) is list:
        return [_plain(item, path=f"{path}[]") for item in value]
    if type(value) is dict:
        output: dict[str, Any] = {}
        for key, item in value.items():
            if type(key) is not str or key in output:
                raise ContractViolation(f"invalid_json_key:{path}")
            output[key] = _plain(item, path=f"{path}.{key}")
        return output
    raise ContractViolation(f"non_plain_json:{path}:{type(value).__name__}")


def _exact_fields(value: Any, fields: frozenset[str], *, path: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise ContractViolation(f"closed_schema_mismatch:{path}")
    keys = tuple(value.keys())
    if (
        len(keys) != len(fields)
        or any(type(key) is not str for key in keys)
        or set(keys) != fields
    ):
        raise ContractViolation(f"closed_schema_mismatch:{path}")
    return value


def _identifier(value: Any, *, path: str, optional: bool = False) -> str:
    if optional and (value is None or value == ""):
        return ""
    try:
        return v3._identifier(value, path=path)
    except v3.ContractViolation as exc:
        raise ContractViolation(str(exc)) from exc


def _sha256(value: Any, *, path: str) -> str:
    try:
        return v3._sha256(value, path=path)
    except v3.ContractViolation as exc:
        raise ContractViolation(str(exc)) from exc


def _number(
    value: Any,
    *,
    path: str,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    try:
        return v3._number(value, path=path, minimum=minimum, maximum=maximum)
    except v3.ContractViolation as exc:
        raise ContractViolation(str(exc)) from exc


def _as_of(value: Any) -> dt.datetime:
    try:
        return v3._as_of(value)
    except v3.ContractViolation as exc:
        raise ContractViolation(str(exc)) from exc


def _iso(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _v4_document(document: Mapping[str, Any]) -> dict[str, Any]:
    value = _plain(document, path="policy_document")
    _exact_fields(value, POLICY_DOCUMENT_FIELDS, path="policy_document")
    result = dict(value)
    result["policy_context_contract_id"] = POLICY_CONTEXT_CONTRACT_ID
    result["research_only"] = True
    result["execution_eligible"] = False
    return _exact_fields(result, POLICY_DOCUMENT_FIELDS, path="policy_document_output")


def _policy_conflict_key(document: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(document["currency"]),
        str(document["known_utc"]),
        str(document["event_id"]),
    )


def _policy_variant_digest(document: Mapping[str, Any]) -> str:
    material = {
        key: value
        for key, value in document.items()
        if key != "policy_context_contract_id"
    }
    return _sha256_json(material)


def build_persistent_policy_context_v4(
    articles: Sequence[Mapping[str, Any]],
    *,
    as_of: dt.datetime,
    sources: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build deterministic policy context over individually validated V3 rows."""

    cutoff = _as_of(as_of)
    if type(articles) not in {list, tuple}:
        raise ContractViolation("articles_must_be_exact_list_or_tuple")
    source_map = _plain(sources, path="sources") if type(sources) is dict else {}

    documents: list[dict[str, Any]] = []
    rejections: list[dict[str, Any]] = []
    observed_currencies: Counter[str] = Counter()
    rejection_by_currency: dict[str, Counter[str]] = {
        currency: Counter() for currency in CANONICAL_CURRENCIES
    }

    for raw in articles:
        try:
            frozen = _plain(raw, path="article")
        except ContractViolation:
            rejection = {
                "event_id": "",
                "source_id": "",
                "currencies": [],
                "reason": "non_plain_article",
                "conflict_version_sha256": "",
            }
            rejections.append(rejection)
            continue
        if type(frozen) is not dict:
            rejections.append(
                {
                    "event_id": "",
                    "source_id": "",
                    "currencies": [],
                    "reason": "non_plain_article",
                    "conflict_version_sha256": "",
                }
            )
            continue
        currencies = v3._authority_currencies(frozen, source_map)
        if frozen.get("source_verified") is True and frozen.get("official_policy_release") is True:
            for currency in currencies:
                observed_currencies[currency] += 1
        try:
            single = v3.build_persistent_policy_context_v3(
                [frozen], as_of=cutoff, sources=source_map
            )
        except v3.ContractViolation as exc:
            rejection = {
                "event_id": "",
                "source_id": "",
                "currencies": currencies,
                "reason": f"v3_contract_rejection:{str(exc)}",
                "conflict_version_sha256": "",
            }
            rejections.append(rejection)
            for currency in currencies:
                rejection_by_currency[currency][rejection["reason"]] += 1
            continue
        for rejection_v3 in single["rejections"]:
            rejection = {
                "event_id": rejection_v3["event_id"],
                "source_id": rejection_v3["source_id"],
                "currencies": list(rejection_v3["currencies"]),
                "reason": rejection_v3["reason"],
                "conflict_version_sha256": "",
            }
            rejections.append(rejection)
            for currency in rejection["currencies"]:
                rejection_by_currency[currency][rejection["reason"]] += 1
        for document_v3 in (
            list(single["proof_grade_documents"])
            + list(single["legacy_diagnostics"])
        ):
            document = _v4_document(document_v3)
            if (
                document["grade"] == "proof_grade_current"
                and not document["content_sha256"]
            ):
                reason = "proof_grade_content_hash_required"
                rejection = {
                    "event_id": document["event_id"],
                    "source_id": document["source_id"],
                    "currencies": [document["currency"]],
                    "reason": reason,
                    "conflict_version_sha256": "",
                }
                rejections.append(rejection)
                rejection_by_currency[document["currency"]][reason] += 1
                continue
            documents.append(document)

    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for document in documents:
        grouped[_policy_conflict_key(document)].append(document)

    accepted: list[dict[str, Any]] = []
    for key in sorted(grouped):
        variants: dict[str, dict[str, Any]] = {}
        for document in grouped[key]:
            variants[_policy_variant_digest(document)] = document
        if len(variants) == 1:
            accepted.append(variants[sorted(variants)[0]])
            continue
        variant_ids = sorted(variants)
        conflict_version = _sha256_json(
            {"policy_conflict_key": list(key), "variant_sha256s": variant_ids}
        )
        currency, _known, event_id = key
        source_ids = sorted({document["source_id"] for document in variants.values()})
        reason = "same_key_policy_conflict"
        rejection = {
            "event_id": event_id,
            "source_id": source_ids[0] if len(source_ids) == 1 else "",
            "currencies": [currency],
            "reason": reason,
            "conflict_version_sha256": conflict_version,
        }
        rejections.append(rejection)
        rejection_by_currency[currency][reason] += 1

    accepted.sort(
        key=lambda row: (
            v3._exact_aware_iso(row["known_utc"], path="known_utc"),
            row["event_id"],
            row["content_sha256"],
            row["currency"],
        )
    )
    proof_documents = [row for row in accepted if row["grade"] == "proof_grade_current"]
    legacy_documents = [row for row in accepted if row["grade"] == "legacy_diagnostic_only"]
    proof_latest = {row["currency"]: row for row in proof_documents}
    legacy_latest = {row["currency"]: row for row in legacy_documents}

    currency_rows: dict[str, dict[str, Any]] = {}
    missing_counts: Counter[str] = Counter()
    for currency in CANONICAL_CURRENCIES:
        proof = proof_latest.get(currency)
        legacy = legacy_latest.get(currency)
        if proof is not None:
            status, reason = "proof_grade_current", ""
        elif legacy is not None:
            status, reason = "missing", "current_v40_provenance_unavailable"
        elif observed_currencies[currency] == 0:
            status, reason = "missing", "no_official_policy_candidate"
        elif rejection_by_currency[currency]:
            status = "missing"
            reason = "all_candidates_rejected:" + ",".join(
                sorted(rejection_by_currency[currency])
            )
        else:
            status, reason = "missing", "no_proof_grade_actual_policy_document"
        if reason:
            missing_counts[reason] += 1
        row = {
            "currency": currency,
            "status": status,
            "missing_reason": reason,
            "proof_grade_context": proof,
            "legacy_diagnostic_context": legacy,
            "rejection_counts": dict(sorted(rejection_by_currency[currency].items())),
            "research_only": True,
            "execution_eligible": False,
        }
        currency_rows[currency] = _exact_fields(
            row, CURRENCY_ROW_FIELDS, path=f"currency:{currency}"
        )

    coverage = {
        "expected_currency_count": 21,
        "reported_currency_count": 21,
        "proof_grade_currency_count": len(proof_latest),
        "missing_currency_count": 21 - len(proof_latest),
        "legacy_diagnostic_currency_count": len(legacy_latest),
        "proof_grade_document_count": len(proof_documents),
        "legacy_diagnostic_document_count": len(legacy_documents),
        "rejected_document_count": len(rejections),
        "missing_reason_counts": dict(sorted(missing_counts.items())),
    }
    output = {
        "schema_version": "persistent_policy_context_v4",
        "contract_id": POLICY_CONTEXT_CONTRACT_ID,
        "generated_utc": _iso(cutoff),
        "required_observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "required_collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
        "required_collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
        "research_only": True,
        "execution_eligible": False,
        "coverage": coverage,
        "currencies": currency_rows,
        "proof_grade_documents": proof_documents,
        "legacy_diagnostics": legacy_documents,
        "rejections": sorted(
            rejections,
            key=lambda row: (
                row["reason"],
                row["event_id"],
                row["conflict_version_sha256"],
            ),
        ),
    }
    return _exact_fields(output, POLICY_OUTPUT_FIELDS, path="policy_output")


def _normalize_source_stories(
    source_evidence: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[str], str]:
    if type(source_evidence) not in {list, tuple} or not source_evidence:
        raise ContractViolation("source_evidence_required")
    event_mapping: dict[str, tuple[str, str]] = {}
    payload_mapping: dict[str, str] = {}
    story_mapping: dict[str, str] = {}
    story_events: dict[tuple[str, str], set[str]] = defaultdict(set)

    for index, raw in enumerate(source_evidence):
        value = _plain(raw, path=f"evidence[{index}]")
        value = _exact_fields(
            value, SOURCE_EVIDENCE_FIELDS, path=f"evidence[{index}]"
        )
        event_id = _identifier(value["event_id"], path="event_id")
        payload_hash = _sha256(value["payload_sha256"], path="payload_sha256")
        supplied_story = _identifier(
            value["story_cluster_id"], path="story_cluster_id", optional=True
        )
        story_id = supplied_story or f"payload_{payload_hash}"
        if not v3._collector_provenance(value):
            raise ContractViolation("source_evidence_not_exact_v40_v4")

        event_value = (story_id, payload_hash)
        if event_id in event_mapping and event_mapping[event_id] != event_value:
            raise ContractViolation("ambiguous_event_to_story_payload_mapping")
        event_mapping[event_id] = event_value
        if payload_hash in payload_mapping and payload_mapping[payload_hash] != story_id:
            raise ContractViolation("ambiguous_payload_to_story_mapping")
        payload_mapping[payload_hash] = story_id
        if story_id in story_mapping and story_mapping[story_id] != payload_hash:
            raise ContractViolation("ambiguous_story_to_payload_mapping")
        story_mapping[story_id] = payload_hash
        story_events[(story_id, payload_hash)].add(event_id)

    stories = [
        {
            "story_cluster_id": story_id,
            "payload_sha256": payload_hash,
            "event_ids": sorted(event_ids),
            "observation_clock_trusted": True,
            "observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
            "collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
            "collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
        }
        for (story_id, payload_hash), event_ids in sorted(story_events.items())
    ]
    all_event_ids = sorted(event_mapping)
    digest = _sha256_json(
        {"episode_semantics_id": EPISODE_SEMANTICS_ID, "canonical_stories": stories}
    )
    return stories, all_event_ids, digest


def build_currency_factor_v4(
    *,
    episode_id: str,
    currency: str,
    score: float,
    confidence: float,
    horizon_min: int,
    source_evidence: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    episode = _identifier(episode_id, path="episode_id")
    if type(currency) is not str or currency not in CURRENCY_SET:
        raise ContractViolation("currency_not_canonical")
    numeric_score = _number(score, path="score", minimum=-1.0, maximum=1.0)
    if numeric_score == 0.0:
        raise ContractViolation("score_must_be_directional")
    numeric_confidence = _number(
        confidence, path="confidence", minimum=0.0, maximum=1.0
    )
    if type(horizon_min) is not int or isinstance(horizon_min, bool) or not 1 <= horizon_min <= 1_440:
        raise ContractViolation("horizon_min_invalid")
    stories, event_ids, digest = _normalize_source_stories(source_evidence)
    result = {
        "episode_id": episode,
        "episode_semantics_id": EPISODE_SEMANTICS_ID,
        "episode_semantics": EPISODE_SEMANTICS,
        "currency": currency,
        "score": numeric_score,
        "confidence": numeric_confidence,
        "horizon_min": horizon_min,
        "event_ids": event_ids,
        "story_cluster_ids": [story["story_cluster_id"] for story in stories],
        "payload_sha256s": [story["payload_sha256"] for story in stories],
        "source_story_evidence": stories,
        "source_event_count": len(event_ids),
        "source_story_count": len(stories),
        "source_evidence_digest_sha256": digest,
        "observation_clock_trusted": True,
        "observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
        "watch_provenance_contract_id": WATCH_PROVENANCE_CONTRACT_ID,
        "research_only": True,
        "execution_eligible": False,
    }
    return _exact_fields(result, FACTOR_OUTPUT_FIELDS, path="factor_output")


def _verify_factor(factor: Mapping[str, Any]) -> str:
    value = _plain(factor, path="factor")
    value = _exact_fields(value, FACTOR_OUTPUT_FIELDS, path="factor")
    if value["episode_semantics_id"] != EPISODE_SEMANTICS_ID or value["episode_semantics"] != EPISODE_SEMANTICS:
        raise ContractViolation("episode_semantics_mismatch")
    if value["watch_provenance_contract_id"] != WATCH_PROVENANCE_CONTRACT_ID:
        raise ContractViolation("watch_contract_mismatch")
    if (
        value["observation_clock_trusted"] is not True
        or value["observation_time_contract_id"] != REQUIRED_OBSERVATION_TIME_CONTRACT_ID
        or value["collector_contract_id"] != REQUIRED_COLLECTOR_CONTRACT_ID
        or value["collector_cohort_id"] != REQUIRED_COLLECTOR_COHORT_ID
    ):
        raise ContractViolation("factor_provenance_mismatch")
    events = value["event_ids"]
    stories = value["story_cluster_ids"]
    payloads = value["payload_sha256s"]
    source_stories = value["source_story_evidence"]
    if (
        type(events) is not list
        or type(stories) is not list
        or type(payloads) is not list
        or type(source_stories) is not list
        or type(value["source_event_count"]) is not int
        or type(value["source_story_count"]) is not int
        or value["source_event_count"] != len(events)
        or value["source_story_count"] != len(stories)
        or len(stories) != len(payloads)
        or events != sorted(set(events))
        or stories != sorted(set(stories))
        or payloads != sorted(set(payloads))
        or len(source_stories) != value["source_story_count"]
    ):
        raise ContractViolation("factor_story_evidence_shape_invalid")
    normalized_stories: list[dict[str, Any]] = []
    union_events: set[str] = set()
    event_owners: dict[str, tuple[str, str]] = {}
    for index, story in enumerate(source_stories):
        story = _exact_fields(
            story, STORY_EVIDENCE_FIELDS, path=f"factor.source_story_evidence[{index}]"
        )
        story_id = _identifier(story["story_cluster_id"], path="story_cluster_id")
        payload_hash = _sha256(story["payload_sha256"], path="payload_sha256")
        event_list = story["event_ids"]
        if (
            type(event_list) is not list
            or not event_list
            or event_list != sorted(set(event_list))
            or story["observation_clock_trusted"] is not True
            or story["observation_time_contract_id"] != REQUIRED_OBSERVATION_TIME_CONTRACT_ID
            or story["collector_contract_id"] != REQUIRED_COLLECTOR_CONTRACT_ID
            or story["collector_cohort_id"] != REQUIRED_COLLECTOR_COHORT_ID
        ):
            raise ContractViolation("factor_story_evidence_invalid")
        for event_id in event_list:
            normalized_event_id = _identifier(event_id, path="event_id")
            owner = (story_id, payload_hash)
            if (
                normalized_event_id in event_owners
                and event_owners[normalized_event_id] != owner
            ):
                raise ContractViolation("ambiguous_event_to_story_payload_mapping")
            event_owners[normalized_event_id] = owner
            union_events.add(normalized_event_id)
        normalized_stories.append(
            {
                "story_cluster_id": story_id,
                "payload_sha256": payload_hash,
                "event_ids": list(event_list),
                "observation_clock_trusted": True,
                "observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
                "collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
                "collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
            }
        )
    if normalized_stories != sorted(
        normalized_stories,
        key=lambda row: (row["story_cluster_id"], row["payload_sha256"]),
    ):
        raise ContractViolation("factor_story_evidence_not_canonical")
    if sorted(union_events) != events:
        raise ContractViolation("factor_story_event_union_mismatch")
    if [row["story_cluster_id"] for row in normalized_stories] != stories:
        raise ContractViolation("factor_story_index_mismatch")
    if [row["payload_sha256"] for row in normalized_stories] != payloads:
        raise ContractViolation("factor_payload_index_mismatch")
    recomputed = _sha256_json(
        {
            "episode_semantics_id": EPISODE_SEMANTICS_ID,
            "canonical_stories": normalized_stories,
        }
    )
    if value["source_evidence_digest_sha256"] != recomputed:
        raise ContractViolation("factor_source_evidence_digest_mismatch")
    return recomputed


def bind_watch_candidate_v4(
    factor: Mapping[str, Any],
    *,
    instrument: str,
    arm: str,
    direction: str,
    horizon_min: int,
) -> dict[str, Any]:
    value = _plain(factor, path="factor")
    digest = _verify_factor(value)
    episode = _identifier(value["episode_id"], path="episode_id")
    currency = value["currency"]
    if type(currency) is not str or currency not in CURRENCY_SET:
        raise ContractViolation("currency_not_canonical")
    if type(instrument) is not str or instrument not in INSTRUMENT_SET:
        raise ContractViolation("instrument_not_canonical")
    if type(arm) is not str or arm not in ARM_ALLOWLIST:
        raise ContractViolation("arm_not_allowlisted")
    if type(direction) is not str or direction not in DIRECTION_ALLOWLIST:
        raise ContractViolation("direction_not_allowlisted")
    if type(horizon_min) is not int or isinstance(horizon_min, bool) or horizon_min <= 0:
        raise ContractViolation("horizon_invalid")
    if not v3._arm_horizon_valid(arm, value["horizon_min"], horizon_min):
        raise ContractViolation("arm_horizon_mismatch")
    score = _number(value["score"], path="score", minimum=-1.0, maximum=1.0)
    try:
        expected = v3._expected_pair_direction(currency, instrument, score)
    except v3.ContractViolation as exc:
        raise ContractViolation(str(exc)) from exc
    if direction != expected:
        raise ContractViolation("currency_factor_direction_mismatch")
    confidence = _number(value["confidence"], path="confidence", minimum=0.0, maximum=1.0)
    cohort_id = WATCH_COHORT_BY_ARM[arm]
    identity = {
        "watch_provenance_contract_id": WATCH_PROVENANCE_CONTRACT_ID,
        "cohort_id": cohort_id,
        "episode_id": episode,
        "episode_semantics_id": EPISODE_SEMANTICS_ID,
        "currency": currency,
        "instrument": instrument,
        "arm": arm,
        "direction": direction,
        "horizon_min": horizon_min,
        "source_evidence_digest_sha256": digest,
        "factor_score": score,
        "factor_confidence": confidence,
        "observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
    }
    result = {
        "entry_id": hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()[:32],
        "cohort_id": cohort_id,
        "watch_provenance_contract_id": WATCH_PROVENANCE_CONTRACT_ID,
        "episode_id": episode,
        "episode_semantics_id": EPISODE_SEMANTICS_ID,
        "currency": currency,
        "instrument": instrument,
        "arm": arm,
        "direction": direction,
        "horizon_min": horizon_min,
        "source_evidence_digest_sha256": digest,
        "source_event_count": value["source_event_count"],
        "source_story_count": value["source_story_count"],
        "factor_score": score,
        "factor_confidence": confidence,
        "event_ids": list(value["event_ids"]),
        "story_cluster_ids": list(value["story_cluster_ids"]),
        "payload_sha256s": list(value["payload_sha256s"]),
        "source_story_evidence": [
            {**row, "event_ids": list(row["event_ids"])}
            for row in value["source_story_evidence"]
        ],
        "observation_clock_trusted": True,
        "observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
        "research_only": True,
        "execution_eligible": False,
    }
    return _exact_fields(result, WATCH_OUTPUT_FIELDS, path="watch_output")
