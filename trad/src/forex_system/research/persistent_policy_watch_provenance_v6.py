"""Dependency-closed inert V6 factor/watch provenance candidate.

The frozen V5 policy-context contract remains separate. V6 owns every
validator, source-story normalizer, horizon rule, pair-direction rule, schema,
and universe used by factor construction and watch binding. No V5/V4/V3
module or function is imported or called by the V6 factor/watch path.

This module has no file, database, network, process, broker, registration,
promotion, authorization, execution, or runtime surface.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict
from types import MappingProxyType
from typing import Any, Mapping, Sequence

FROZEN_POLICY_CONTEXT_CONTRACT_ID = "persistent_policy_context_v5_factor_validator_20260817"
WATCH_PROVENANCE_CONTRACT_ID = (
    "news_technical_watchlist_v42_dependency_closed_factor_candidate_20260817"
)
EPISODE_SEMANTICS_ID = (
    "point_in_time_canonical_story_cluster_currency_factor_v4_20260817"
)
EPISODE_SEMANTICS = (
    "One canonical story cluster for one currency factor; syndicated event "
    "copies sharing one unambiguous story/payload mapping count once, exact "
    "duplicates do not increase evidence, and ambiguous event, story, or "
    "payload mappings fail closed."
)

REQUIRED_OBSERVATION_TIME_CONTRACT_ID = (
    "observation_time_v4_fail_closed_consistent_integrity_sources_20260817"
)
REQUIRED_COLLECTOR_CONTRACT_ID = "local_news_incremental_source_commit_v40_20260817"
REQUIRED_COLLECTOR_COHORT_ID = "local_news_incremental_source_commit_v40_20260817"

# Own the exact closed universes and schemas; the factor/watch path has no
# transitive dependency whose runtime mutation can weaken these contracts.
CANONICAL_CURRENCIES = (
    "AUD", "CAD", "CHF", "CNH", "CZK", "DKK", "EUR", "GBP", "HKD", "HUF",
    "JPY", "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "THB", "TRY", "USD",
    "ZAR",
)
CANONICAL_INSTRUMENTS = (
    "AUD_CAD", "AUD_CHF", "AUD_HKD", "AUD_JPY", "AUD_NZD", "AUD_SGD", "AUD_USD",
    "CAD_CHF", "CAD_HKD", "CAD_JPY", "CAD_SGD", "CHF_HKD", "CHF_JPY", "CHF_ZAR",
    "EUR_AUD", "EUR_CAD", "EUR_CHF", "EUR_CZK", "EUR_DKK", "EUR_GBP", "EUR_HKD",
    "EUR_HUF", "EUR_JPY", "EUR_NOK", "EUR_NZD", "EUR_PLN", "EUR_SEK", "EUR_SGD",
    "EUR_TRY", "EUR_USD", "EUR_ZAR", "GBP_AUD", "GBP_CAD", "GBP_CHF", "GBP_HKD",
    "GBP_JPY", "GBP_NZD", "GBP_PLN", "GBP_SGD", "GBP_USD", "GBP_ZAR", "HKD_JPY",
    "NZD_CAD", "NZD_CHF", "NZD_HKD", "NZD_JPY", "NZD_SGD", "NZD_USD", "SGD_CHF",
    "SGD_JPY", "TRY_JPY", "USD_CAD", "USD_CHF", "USD_CNH", "USD_CZK", "USD_DKK",
    "USD_HKD", "USD_HUF", "USD_JPY", "USD_MXN", "USD_NOK", "USD_PLN", "USD_SEK",
    "USD_SGD", "USD_THB", "USD_TRY", "USD_ZAR", "ZAR_JPY",
)
CURRENCY_SET = frozenset(CANONICAL_CURRENCIES)
INSTRUMENT_SET = frozenset(CANONICAL_INSTRUMENTS)
ARM_ALLOWLIST = frozenset(
    {
        "news_only",
        "news_technical_confirmed",
        "news_technical_conflicted",
        "news_technical_reconfirmed_h15",
        "uncorroborated_news_response_h5",
        "uncorroborated_news_response_h15",
        "uncorroborated_news_response_h30",
        "news_magnitude_ranked_h5",
        "news_magnitude_ranked_h15",
        "news_magnitude_ranked_h30",
        "news_magnitude_direction_confirmed_h5",
        "news_magnitude_direction_confirmed_h15",
        "news_magnitude_direction_confirmed_h30",
    }
)
DIRECTION_ALLOWLIST = frozenset({"long", "short"})
SOURCE_EVIDENCE_FIELDS = frozenset(
    {
        "event_id", "story_cluster_id", "payload_sha256",
        "observation_clock_trusted", "observation_time_contract_id",
        "collector_contract_id", "collector_cohort_id",
    }
)
STORY_EVIDENCE_FIELDS = frozenset(
    {
        "story_cluster_id", "payload_sha256", "event_ids",
        "observation_clock_trusted", "observation_time_contract_id",
        "collector_contract_id", "collector_cohort_id",
    }
)
FACTOR_OUTPUT_FIELDS = frozenset(
    {
        "episode_id", "episode_semantics_id", "episode_semantics", "currency",
        "score", "confidence", "horizon_min", "event_ids", "story_cluster_ids",
        "payload_sha256s", "source_story_evidence", "source_event_count",
        "source_story_count", "source_evidence_digest_sha256",
        "observation_clock_trusted", "observation_time_contract_id",
        "collector_contract_id", "collector_cohort_id", "watch_provenance_contract_id",
        "research_only", "execution_eligible",
    }
)
WATCH_OUTPUT_FIELDS = frozenset(
    {
        "entry_id", "cohort_id", "watch_provenance_contract_id", "episode_id",
        "episode_semantics_id", "currency", "instrument", "arm", "direction",
        "horizon_min", "source_evidence_digest_sha256", "source_event_count",
        "source_story_count", "factor_score", "factor_confidence", "event_ids",
        "story_cluster_ids", "payload_sha256s", "source_story_evidence",
        "observation_clock_trusted", "observation_time_contract_id",
        "collector_contract_id", "collector_cohort_id", "research_only",
        "execution_eligible",
    }
)
WATCH_COHORT_BY_ARM = MappingProxyType(
    {
        arm: f"{WATCH_PROVENANCE_CONTRACT_ID}.{arm}"
        for arm in sorted(ARM_ALLOWLIST)
    }
)

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@/+|-]{0,191}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_FIXED_HORIZON_ARM = re.compile(r"_h(5|15|30)\Z")


class ContractViolation(ValueError):
    """Raised when V6 cannot prove a closed, deterministic research record."""


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


def _identifier(value: Any, *, path: str, optional: bool = False) -> str:
    if optional and (value is None or value == ""):
        return ""
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise ContractViolation(f"identifier_invalid:{path}")
    return value


def _sha256(value: Any, *, path: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ContractViolation(f"sha256_invalid:{path}")
    return value


def _number(
    value: Any,
    *,
    path: str,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    if type(value) not in {int, float} or isinstance(value, bool):
        raise ContractViolation(f"number_invalid:{path}")
    result = float(value)
    if not math.isfinite(result):
        raise ContractViolation(f"number_invalid:{path}")
    if minimum is not None and result < minimum:
        raise ContractViolation(f"number_below_minimum:{path}")
    if maximum is not None and result > maximum:
        raise ContractViolation(f"number_above_maximum:{path}")
    return result


def _collector_provenance(value: Mapping[str, Any]) -> bool:
    return (
        value.get("observation_clock_trusted") is True
        and value.get("observation_time_contract_id")
        == REQUIRED_OBSERVATION_TIME_CONTRACT_ID
        and value.get("collector_contract_id") == REQUIRED_COLLECTOR_CONTRACT_ID
        and value.get("collector_cohort_id") == REQUIRED_COLLECTOR_COHORT_ID
    )


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
        value = _exact_fields(
            _plain(raw, path=f"evidence[{index}]"),
            SOURCE_EVIDENCE_FIELDS,
            path=f"evidence[{index}]",
        )
        event_id = _identifier(value["event_id"], path="event_id")
        payload = _sha256(value["payload_sha256"], path="payload_sha256")
        supplied_story = _identifier(
            value["story_cluster_id"], path="story_cluster_id", optional=True
        )
        story_id = supplied_story or f"payload_{payload}"
        if not _collector_provenance(value):
            raise ContractViolation("source_evidence_not_exact_v40_v4")
        owner = (story_id, payload)
        if event_id in event_mapping and event_mapping[event_id] != owner:
            raise ContractViolation("ambiguous_event_to_story_payload_mapping")
        if payload in payload_mapping and payload_mapping[payload] != story_id:
            raise ContractViolation("ambiguous_payload_to_story_mapping")
        if story_id in story_mapping and story_mapping[story_id] != payload:
            raise ContractViolation("ambiguous_story_to_payload_mapping")
        event_mapping[event_id] = owner
        payload_mapping[payload] = story_id
        story_mapping[story_id] = payload
        story_events[owner].add(event_id)

    stories = [
        {
            "story_cluster_id": story_id,
            "payload_sha256": payload,
            "event_ids": sorted(events),
            "observation_clock_trusted": True,
            "observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
            "collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
            "collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
        }
        for (story_id, payload), events in sorted(story_events.items())
    ]
    event_ids = sorted(event_mapping)
    digest = _sha256_json(
        {"episode_semantics_id": EPISODE_SEMANTICS_ID, "canonical_stories": stories}
    )
    return stories, event_ids, digest


def _fixed_horizon(arm: str) -> int | None:
    match = _FIXED_HORIZON_ARM.search(arm)
    return int(match.group(1)) if match is not None else None


def _expected_pair_direction(currency: str, instrument: str, score: float) -> str:
    base, quote = instrument.split("_")
    if currency not in {base, quote}:
        raise ContractViolation("currency_not_in_instrument")
    currency_up = score > 0.0
    long_pair = (currency == base and currency_up) or (
        currency == quote and not currency_up
    )
    return "long" if long_pair else "short"


def _canonical_factor_validator(
    factor: Mapping[str, Any],
    *,
    arm: str | None = None,
    declared_horizon_min: int | None = None,
) -> tuple[dict[str, Any], str, str, str, float, float, int]:
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
    if not _collector_provenance(value):
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
            or not _collector_provenance(story)
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


def build_currency_factor_v6(
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
    stories, events, digest = _normalize_source_stories(source_evidence)
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


def bind_watch_candidate_v6(
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
    expected_direction = _expected_pair_direction(currency, instrument, score)
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
