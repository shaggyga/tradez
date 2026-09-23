"""Inert V5 integrity successor for frozen policy/watch provenance V4.

V5 keeps V4 byte-exact and closes its factor-validation gaps with one
canonical validator shared by factor construction and watch binding.  The
module is pure and has no file, database, network, process, broker,
registration, promotion, authorization, execution, or runtime surface.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from . import persistent_policy_watch_provenance_v4 as v4


POLICY_CONTEXT_CONTRACT_ID = "persistent_policy_context_v5_factor_validator_20260817"
WATCH_PROVENANCE_CONTRACT_ID = (
    "news_technical_watchlist_v41_canonical_factor_candidate_20260817"
)
EPISODE_SEMANTICS_ID = (
    "point_in_time_canonical_story_cluster_currency_factor_v3_20260817"
)
EPISODE_SEMANTICS = v4.EPISODE_SEMANTICS

REQUIRED_OBSERVATION_TIME_CONTRACT_ID = v4.REQUIRED_OBSERVATION_TIME_CONTRACT_ID
REQUIRED_COLLECTOR_CONTRACT_ID = v4.REQUIRED_COLLECTOR_CONTRACT_ID
REQUIRED_COLLECTOR_COHORT_ID = v4.REQUIRED_COLLECTOR_COHORT_ID
CANONICAL_CURRENCIES = v4.CANONICAL_CURRENCIES
CANONICAL_INSTRUMENTS = v4.CANONICAL_INSTRUMENTS
CURRENCY_SET = v4.CURRENCY_SET
INSTRUMENT_SET = v4.INSTRUMENT_SET
ARM_ALLOWLIST = v4.ARM_ALLOWLIST
DIRECTION_ALLOWLIST = v4.DIRECTION_ALLOWLIST
WATCH_COHORT_BY_ARM = MappingProxyType(
    {
        arm: f"{WATCH_PROVENANCE_CONTRACT_ID}.{arm}"
        for arm in sorted(ARM_ALLOWLIST)
    }
)

POLICY_DOCUMENT_FIELDS = v4.POLICY_DOCUMENT_FIELDS
CURRENCY_ROW_FIELDS = v4.CURRENCY_ROW_FIELDS
COVERAGE_FIELDS = v4.COVERAGE_FIELDS
POLICY_OUTPUT_FIELDS = v4.POLICY_OUTPUT_FIELDS
REJECTION_FIELDS = v4.REJECTION_FIELDS
SOURCE_EVIDENCE_FIELDS = v4.SOURCE_EVIDENCE_FIELDS
STORY_EVIDENCE_FIELDS = v4.STORY_EVIDENCE_FIELDS
FACTOR_OUTPUT_FIELDS = v4.FACTOR_OUTPUT_FIELDS
WATCH_OUTPUT_FIELDS = v4.WATCH_OUTPUT_FIELDS

_FIXED_HORIZON_ARM = re.compile(r"_h(5|15|30)\Z")


class ContractViolation(ValueError):
    """Raised when V5 cannot prove a closed, deterministic research record."""


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
    keys = tuple(value)
    if len(keys) != len(fields) or set(keys) != fields:
        raise ContractViolation(f"closed_schema_mismatch:{path}")
    return value


def _identifier(value: Any, *, path: str) -> str:
    try:
        return v4._identifier(value, path=path)
    except v4.ContractViolation as exc:
        raise ContractViolation(str(exc)) from exc


def _sha256(value: Any, *, path: str) -> str:
    try:
        return v4._sha256(value, path=path)
    except v4.ContractViolation as exc:
        raise ContractViolation(str(exc)) from exc


def _number(
    value: Any,
    *,
    path: str,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    try:
        return v4._number(value, path=path, minimum=minimum, maximum=maximum)
    except v4.ContractViolation as exc:
        raise ContractViolation(str(exc)) from exc


def _retag_policy_value(value: Any) -> Any:
    if type(value) is list:
        return [_retag_policy_value(item) for item in value]
    if type(value) is dict:
        output = {key: _retag_policy_value(item) for key, item in value.items()}
        if output.get("policy_context_contract_id") == v4.POLICY_CONTEXT_CONTRACT_ID:
            output["policy_context_contract_id"] = POLICY_CONTEXT_CONTRACT_ID
        return output
    return value


def build_persistent_policy_context_v5(
    articles: Sequence[Mapping[str, Any]],
    *,
    as_of: dt.datetime,
    sources: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Retag the fully validated frozen V4 policy result as an inert V5 result."""

    try:
        frozen = v4.build_persistent_policy_context_v4(
            articles, as_of=as_of, sources=sources
        )
    except v4.ContractViolation as exc:
        raise ContractViolation(str(exc)) from exc
    result = _retag_policy_value(_plain(frozen, path="v4_policy"))
    result["schema_version"] = "persistent_policy_context_v5"
    result["contract_id"] = POLICY_CONTEXT_CONTRACT_ID
    return _exact_fields(result, POLICY_OUTPUT_FIELDS, path="policy_output")


def _canonical_source_stories(
    source_evidence: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    try:
        stories, events, _digest = v4._normalize_source_stories(source_evidence)
    except v4.ContractViolation as exc:
        raise ContractViolation(str(exc)) from exc
    return _plain(stories, path="canonical_stories"), list(events)


def _fixed_horizon(arm: str) -> int | None:
    match = _FIXED_HORIZON_ARM.search(arm)
    return int(match.group(1)) if match is not None else None


def _canonical_factor_validator(
    factor: Mapping[str, Any],
    *,
    arm: str | None = None,
    declared_horizon_min: int | None = None,
) -> tuple[dict[str, Any], str, str, str, float, float, int]:
    """Validate and canonicalize every factor invariant used by the binder."""

    value = _exact_fields(_plain(factor, path="factor"), FACTOR_OUTPUT_FIELDS, path="factor")
    if value["research_only"] is not True or value["execution_eligible"] is not False:
        raise ContractViolation("factor_not_inert")
    if (
        value["episode_semantics_id"] != EPISODE_SEMANTICS_ID
        or value["episode_semantics"] != EPISODE_SEMANTICS
    ):
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

    episode = _identifier(value["episode_id"], path="factor.episode_id")
    currency = value["currency"]
    if type(currency) is not str or currency not in CURRENCY_SET:
        raise ContractViolation("currency_not_canonical")
    score = _number(value["score"], path="factor.score", minimum=-1.0, maximum=1.0)
    if score == 0.0:
        raise ContractViolation("score_must_be_directional")
    confidence = _number(
        value["confidence"], path="factor.confidence", minimum=0.0, maximum=1.0
    )
    factor_horizon = value["horizon_min"]
    if (
        type(factor_horizon) is not int
        or isinstance(factor_horizon, bool)
        or not 1 <= factor_horizon <= 1_440
    ):
        raise ContractViolation("factor_horizon_invalid")

    events = value["event_ids"]
    story_ids = value["story_cluster_ids"]
    payloads = value["payload_sha256s"]
    source_stories = value["source_story_evidence"]
    event_count = value["source_event_count"]
    story_count = value["source_story_count"]
    if (
        type(events) is not list
        or type(story_ids) is not list
        or type(payloads) is not list
        or type(source_stories) is not list
        or type(event_count) is not int
        or isinstance(event_count, bool)
        or event_count <= 0
        or type(story_count) is not int
        or isinstance(story_count, bool)
        or story_count <= 0
        or event_count != len(events)
        or story_count != len(source_stories)
        or story_count != len(story_ids)
        or story_count != len(payloads)
        or events != sorted(set(events))
    ):
        raise ContractViolation("factor_story_evidence_shape_invalid")

    normalized_stories: list[dict[str, Any]] = []
    event_owners: dict[str, tuple[str, str]] = {}
    story_to_payload: dict[str, str] = {}
    payload_to_story: dict[str, str] = {}
    for index, raw_story in enumerate(source_stories):
        story = _exact_fields(
            raw_story,
            STORY_EVIDENCE_FIELDS,
            path=f"factor.source_story_evidence[{index}]",
        )
        story_id = _identifier(story["story_cluster_id"], path="story_cluster_id")
        payload = _sha256(story["payload_sha256"], path="payload_sha256")
        story_events = story["event_ids"]
        if (
            type(story_events) is not list
            or not story_events
            or story_events != sorted(set(story_events))
            or story["observation_clock_trusted"] is not True
            or story["observation_time_contract_id"]
            != REQUIRED_OBSERVATION_TIME_CONTRACT_ID
            or story["collector_contract_id"] != REQUIRED_COLLECTOR_CONTRACT_ID
            or story["collector_cohort_id"] != REQUIRED_COLLECTOR_COHORT_ID
        ):
            raise ContractViolation("factor_story_evidence_invalid")
        if story_id in story_to_payload and story_to_payload[story_id] != payload:
            raise ContractViolation("ambiguous_story_to_payload_mapping")
        if payload in payload_to_story and payload_to_story[payload] != story_id:
            raise ContractViolation("ambiguous_payload_to_story_mapping")
        story_to_payload[story_id] = payload
        payload_to_story[payload] = story_id
        normalized_events: list[str] = []
        for event in story_events:
            event_id = _identifier(event, path="event_id")
            owner = (story_id, payload)
            if event_id in event_owners and event_owners[event_id] != owner:
                raise ContractViolation("ambiguous_event_to_story_payload_mapping")
            event_owners[event_id] = owner
            normalized_events.append(event_id)
        normalized_stories.append(
            {
                "story_cluster_id": story_id,
                "payload_sha256": payload,
                "event_ids": normalized_events,
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
    aligned_pairs = [
        (row["story_cluster_id"], row["payload_sha256"])
        for row in normalized_stories
    ]
    if aligned_pairs != sorted(set(aligned_pairs)):
        raise ContractViolation("factor_story_pairs_not_canonical")
    if list(zip(story_ids, payloads)) != aligned_pairs:
        raise ContractViolation("factor_story_payload_index_mismatch")
    if sorted(event_owners) != events:
        raise ContractViolation("factor_story_event_union_mismatch")
    digest = _sha256_json(
        {
            "episode_semantics_id": EPISODE_SEMANTICS_ID,
            "canonical_stories": normalized_stories,
        }
    )
    if value["source_evidence_digest_sha256"] != digest:
        raise ContractViolation("factor_source_evidence_digest_mismatch")

    if arm is not None:
        if type(arm) is not str or arm not in ARM_ALLOWLIST:
            raise ContractViolation("arm_not_allowlisted")
        if (
            type(declared_horizon_min) is not int
            or isinstance(declared_horizon_min, bool)
            or not 1 <= declared_horizon_min <= 1_440
        ):
            raise ContractViolation("declared_horizon_invalid")
        fixed = _fixed_horizon(arm)
        expected_horizon = fixed if fixed is not None else factor_horizon
        if factor_horizon != expected_horizon or declared_horizon_min != expected_horizon:
            raise ContractViolation("arm_horizon_mismatch")
    elif declared_horizon_min is not None:
        raise ContractViolation("arm_required_for_declared_horizon")

    canonical = dict(value)
    canonical["score"] = score
    canonical["confidence"] = confidence
    canonical["source_story_evidence"] = normalized_stories
    return canonical, digest, episode, currency, score, confidence, factor_horizon


def build_currency_factor_v5(
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
    if (
        type(horizon_min) is not int
        or isinstance(horizon_min, bool)
        or not 1 <= horizon_min <= 1_440
    ):
        raise ContractViolation("factor_horizon_invalid")
    stories, events = _canonical_source_stories(source_evidence)
    digest = _sha256_json(
        {"episode_semantics_id": EPISODE_SEMANTICS_ID, "canonical_stories": stories}
    )
    result = {
        "episode_id": episode,
        "episode_semantics_id": EPISODE_SEMANTICS_ID,
        "episode_semantics": EPISODE_SEMANTICS,
        "currency": currency,
        "score": numeric_score,
        "confidence": numeric_confidence,
        "horizon_min": horizon_min,
        "event_ids": events,
        "story_cluster_ids": [row["story_cluster_id"] for row in stories],
        "payload_sha256s": [row["payload_sha256"] for row in stories],
        "source_story_evidence": stories,
        "source_event_count": len(events),
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
    canonical, *_unused = _canonical_factor_validator(result)
    return canonical


def bind_watch_candidate_v5(
    factor: Mapping[str, Any],
    *,
    instrument: str,
    arm: str,
    direction: str,
    horizon_min: int,
) -> dict[str, Any]:
    value, digest, episode, currency, score, confidence, _factor_horizon = (
        _canonical_factor_validator(
            factor,
            arm=arm,
            declared_horizon_min=horizon_min,
        )
    )
    if type(instrument) is not str or instrument not in INSTRUMENT_SET:
        raise ContractViolation("instrument_not_canonical")
    if type(direction) is not str or direction not in DIRECTION_ALLOWLIST:
        raise ContractViolation("direction_not_allowlisted")
    try:
        expected_direction = v4.v3._expected_pair_direction(currency, instrument, score)
    except v4.v3.ContractViolation as exc:
        raise ContractViolation(str(exc)) from exc
    if direction != expected_direction:
        raise ContractViolation("currency_factor_direction_mismatch")

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
