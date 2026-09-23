"""Fail-closed SARB MPC exact-clock and decision-fact source (V2).

This module is deliberately a pure, shadow/research-only adapter.  It does not
write a database, register with the running news collector, touch an immutable
clock ledger, or expose an execution surface.  Schedule decisions are derived
only after reopening the exact, read-only, content-addressed official archive
and its pinned observation manifest.  The adapter returns only bounded facts,
causal timestamps, URLs, and hashes; it never returns the source HTML.

The historical 23 July 2026 decision is availability-counterfactual context
only because this contract did not observe it prospectively.  The 23 September
and 19 November clocks can support prospective *collection* only when the
official schedule was captured before the event.  A decision is not known at
the schedule clock: its causal knowledge time is max(clock, statement first
seen).  Neither a clock nor a parsed decision creates direction, consensus,
surprise, confirmation, promotion, authorization, or execution eligibility.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import html as html_lib
import json
import math
import os
import re
import stat
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse
from zoneinfo import ZoneInfo


SCHEMA_VERSION = 2
SOURCE_ID = "sarb_mpc_decision_exact_v2"
CONTRACT_ID = "sarb_mpc_decision_exact_v2_20260817"
COHORT_ID = "sarb_mpc_decision_exact_v2_20260817"

TIMEZONE_NAME = "Africa/Johannesburg"
SCHEDULE_URL = (
    "https://www.resbank.co.za/en/home/what-we-do/monetary-policy/"
    "MPC-announcement-webcasts"
)

# Frozen, content-addressed primary-source observation.  The raw response is
# retained in a read-only local archive after two identical observations and a
# later byte-identical recovery fetch.  Any changed response, mirror,
# reconstructed fixture, or caller-supplied hash requires a new source cohort;
# this V2 cohort accepts only this exact observation and archive path.
SCHEDULE_OBSERVATION_ID = "sarb_mpc_schedule_observation_20260817T082716Z"
SCHEDULE_OBSERVED_UTC = "2026-08-17T08:27:16.304041+00:00"
SCHEDULE_OBSERVED_UTC_ORIGINAL_100NS = "2026-08-17T08:27:16.3040411+00:00"
SCHEDULE_OBSERVATION_MANIFEST_PATH = (
    "data/oanda_training_manager/source_archives/"
    "sarb_mpc_decision_exact_v2/schedule_observation_20260817T082716Z.json"
)
SCHEDULE_OBSERVATION_MANIFEST_SHA256 = (
    "f74006c7df2abec925592e61d1a4ce36584da8c39cbb6303b3dd82838deb3108"
)
SCHEDULE_SOURCE_ARCHIVE_PATH = (
    "data/oanda_training_manager/source_archives/"
    "sarb_mpc_decision_exact_v2/sarb_mpc_schedule_20260817T082716Z.html"
)
SCHEDULE_SOURCE_BYTES = 100_939
SCHEDULE_SOURCE_SHA256 = (
    "bd426f66a8360d45ad4c7f4f50b39eb14690b96278937ebbdc58f59b43d4f540"
)
JULY_STATEMENT_URL = (
    "https://www.resbank.co.za/en/home/publications/publication-detail-pages/"
    "statements/monetary-policy-statements/2026/july"
)
STATEMENT_PATH_PREFIX = (
    "/en/home/publications/publication-detail-pages/statements/"
    "monetary-policy-statements/"
)

UTC = dt.timezone.utc
_OFFICIAL_HOSTS = frozenset({"resbank.co.za", "www.resbank.co.za"})
_WORD_NUMBERS = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
}

EXPECTED_SCHEDULE_FACTS: tuple[dict[str, str], ...] = (
    {
        "event_date": "2026-07-23",
        "local_time": "15:00",
        "scheduled_utc": "2026-07-23T13:00:00Z",
        "evidence_boundary": "historical_availability_counterfactual_archive_only",
    },
    {
        "event_date": "2026-09-23",
        "local_time": "15:00",
        "scheduled_utc": "2026-09-23T13:00:00Z",
        "evidence_boundary": "prospective_schedule_if_captured_pre_event",
    },
    {
        "event_date": "2026-11-19",
        "local_time": "15:00",
        "scheduled_utc": "2026-11-19T13:00:00Z",
        "evidence_boundary": "prospective_schedule_if_captured_pre_event",
    },
)

EXPECTED_JULY_DECISION_FACTS: dict[str, Any] = {
    "event_date": "2026-07-23",
    "action": "hold",
    "policy_rate_pct": 7.0,
    "votes": (
        {"count": 4, "preference": "hold", "basis_points": 0},
        {"count": 2, "preference": "hike", "basis_points": 25},
    ),
    "statement_url": JULY_STATEMENT_URL,
}


class SarbMpcDecisionSourceError(RuntimeError):
    """Raised when the exact-clock or decision-fact contract is not proved."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
        default=str,
    )


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


SCHEDULE_FACTS_SHA256 = canonical_sha256(
    {
        "schedule_url": SCHEDULE_URL,
        "timezone": TIMEZONE_NAME,
        "events": EXPECTED_SCHEDULE_FACTS,
    }
)
JULY_DECISION_FACTS_SHA256 = canonical_sha256(EXPECTED_JULY_DECISION_FACTS)

EXPECTED_HARD_GUARDS: dict[str, Any] = {
    "schedule_and_statement_are_one_event": True,
    "same_time_global_event_confounding_required": True,
    "causal_attribution_status": "unresolved",
    "direction": None,
    "currency_bias": None,
    "pair_bias": None,
    "consensus": None,
    "surprise": None,
    "confirmation_eligible": False,
    "promotion_eligible": False,
    "proof_eligible": False,
    "authorization_eligible": False,
    "execution_eligible": False,
    "supported_execution_decision": "no_trade",
}

EXPECTED_EVIDENCE_POLICY: dict[str, Any] = {
    "historical_july": "availability-counterfactual/archive-only; never prospective proof",
    "future_september_november": (
        "prospective collection only when this schedule cohort was captured before the event"
    ),
    "decision_causal_known_utc": "max(scheduled_utc, statement_first_seen_utc)",
    "clock_is_not_statement_availability": True,
    "five_priced_zar_pairs_are_one_currency_factor": True,
    "same_time_global_event_confounding_required": True,
}

EXPECTED_FACT_RETENTION_POLICY: dict[str, Any] = {
    "retain": [
        "date",
        "action",
        "policy_rate_level",
        "vote_split",
        "official_url",
        "source_sha256",
        "facts_sha256",
        "causal_timestamps",
        "official_schedule_source_archive",
    ],
    "do_not_retain": [
        "full_statement_text",
        "search_result_timestamp",
    ],
}

EXPECTED_SEPARATION_CONTRACT: dict[str, Any] = {
    "existing_source_id": "south_africa_sarb_policy_rate_direct_v1",
    "existing_series_code": "MMRD002A",
    "existing_snapshot_semantics": "homepage/API indicator update; not a decision timestamp",
    "rewrite_or_relabel_existing_snapshot": False,
    "touch_immutable_event_clock": False,
    "touch_live_collector": False,
    "touch_executor_or_account": False,
}

EXPECTED_OBSERVATION_MANIFEST: dict[str, Any] = {
    "schema_version": 1,
    "observation_id": SCHEDULE_OBSERVATION_ID,
    "source_id": SOURCE_ID,
    "source_url": SCHEDULE_URL,
    "trusted_domain": "www.resbank.co.za",
    "observed_utc": SCHEDULE_OBSERVED_UTC,
    "observed_utc_original_100ns": SCHEDULE_OBSERVED_UTC_ORIGINAL_100NS,
    "observation_timestamp_precision": "microsecond",
    "discarded_submicrosecond_100ns_ticks": 1,
    "http_status": 200,
    "http_date": "2026-08-17T08:27:15+00:00",
    "content_type": "text/html; charset=utf-8",
    "raw_response_bytes": SCHEDULE_SOURCE_BYTES,
    "raw_response_sha256": SCHEDULE_SOURCE_SHA256,
    "capture_method": "System.Net.Http.HttpClient.ReadAsByteArrayAsync",
    "independent_repeat_observation_same_hash": True,
    "bounded_fact_checks": {
        "contains_2026_september_23": True,
        "contains_2026_november_19": True,
        "contains_15_00_local_time": True,
    },
    "raw_payload_retained": True,
    "raw_payload_archive_path": SCHEDULE_SOURCE_ARCHIVE_PATH,
    "raw_payload_archive_bytes": SCHEDULE_SOURCE_BYTES,
    "raw_payload_archive_sha256": SCHEDULE_SOURCE_SHA256,
    "raw_payload_archived_utc": "2026-08-17T08:52:47.9660253+00:00",
    "archive_recovery_method": (
        "A later direct official HTTP-200 response was byte-identical to both in-memory "
        "observations made at the recorded observation time; the exact public payload was "
        "then atomically retained under the original content hash."
    ),
    "retention_note": (
        "Downstream decision parsing must reopen this immutable archive and verify exact "
        "path, byte count, SHA-256, observation manifest, URL, and observation time; a "
        "caller-supplied mapping or public hash is insufficient."
    ),
    "research_only": True,
    "direction": None,
    "consensus": None,
    "surprise": None,
    "promotion_eligible": False,
    "authorization_eligible": False,
    "execution_eligible": False,
    "supported_execution_decision": "no_trade",
}


def _canonical_event_id(event_date: str) -> str:
    return f"sarb_mpc_decision:{event_date}"


def _canonical_episode_key(event_date: str) -> str:
    return f"ZAR:sarb_mpc_decision:{event_date}"


def _strict_equal(actual: Any, expected: Any) -> bool:
    """Type-sensitive recursive equality; ``0`` must not equal ``False``."""

    if isinstance(expected, Mapping):
        if not isinstance(actual, Mapping) or set(actual) != set(expected):
            return False
        return all(_strict_equal(actual[key], value) for key, value in expected.items())
    if isinstance(expected, (list, tuple)):
        if not isinstance(actual, (list, tuple)) or len(actual) != len(expected):
            return False
        return all(_strict_equal(left, right) for left, right in zip(actual, expected))
    if expected is None:
        return actual is None
    if isinstance(expected, bool):
        return type(actual) is bool and actual is expected
    if isinstance(expected, (int, float)) and not isinstance(expected, bool):
        return type(actual) is type(expected) and actual == expected
    return type(actual) is type(expected) and actual == expected


def _source_sha256(payload: bytes) -> str:
    if not isinstance(payload, bytes) or not payload:
        raise SarbMpcDecisionSourceError("source_payload_missing")
    return hashlib.sha256(payload).hexdigest()


def _validate_frozen_schedule_payload(payload: bytes) -> str:
    """Require the exact twice-observed official schedule response bytes."""

    source_hash = _source_sha256(payload)
    if len(payload) != SCHEDULE_SOURCE_BYTES:
        raise SarbMpcDecisionSourceError(
            "schedule_source_byte_count_mismatch_requires_new_cohort"
        )
    if source_hash != SCHEDULE_SOURCE_SHA256:
        raise SarbMpcDecisionSourceError(
            "schedule_source_sha256_mismatch_requires_new_cohort"
        )
    return source_hash


def _parse_utc(value: str | dt.datetime) -> dt.datetime:
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = dt.datetime.fromisoformat(text)
        except ValueError as exc:
            raise SarbMpcDecisionSourceError("invalid_utc_timestamp") from exc
    if parsed.tzinfo is None:
        raise SarbMpcDecisionSourceError("naive_timestamp_forbidden")
    return parsed.astimezone(UTC)


def _iso_utc(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _official_url(url: str, *, expected_path: str | None = None) -> str:
    parsed = urlparse(str(url or ""))
    host = (parsed.hostname or "").lower()
    if parsed.scheme.lower() != "https" or host not in _OFFICIAL_HOSTS:
        raise SarbMpcDecisionSourceError("non_official_sarb_url")
    if parsed.username or parsed.password or parsed.port not in (None, 443):
        raise SarbMpcDecisionSourceError("unsafe_sarb_url_authority")
    normalized_path = parsed.path.rstrip("/")
    if expected_path is not None and normalized_path != expected_path.rstrip("/"):
        raise SarbMpcDecisionSourceError("unexpected_sarb_url_path")
    if parsed.query or parsed.fragment:
        raise SarbMpcDecisionSourceError("sarb_source_url_query_or_fragment_forbidden")
    return f"https://www.resbank.co.za{normalized_path}"


def _statement_url(url: str, event_date: str) -> str:
    normalized = _official_url(url)
    parsed = urlparse(normalized)
    event = dt.date.fromisoformat(event_date)
    expected_path = f"{STATEMENT_PATH_PREFIX}{event.year}/{event.strftime('%B').lower()}"
    if parsed.path.rstrip("/") != expected_path:
        raise SarbMpcDecisionSourceError("statement_url_does_not_match_event_month")
    return normalized


def _html_to_text(payload: bytes) -> str:
    _source_sha256(payload)
    decoded = payload.decode("utf-8", errors="replace")
    decoded = re.sub(r"(?is)<script\b.*?</script>", " ", decoded)
    decoded = re.sub(r"(?is)<style\b.*?</style>", " ", decoded)
    decoded = re.sub(r"(?s)<[^>]+>", " ", decoded)
    decoded = html_lib.unescape(decoded)
    return re.sub(r"\s+", " ", decoded).strip()


def assert_johannesburg_utc_plus_two_no_dst(year: int = 2026) -> None:
    zone = ZoneInfo(TIMEZONE_NAME)
    offsets: set[dt.timedelta | None] = set()
    dst_values: set[dt.timedelta | None] = set()
    for month in range(1, 13):
        local = dt.datetime(year, month, 1, 12, tzinfo=zone)
        offsets.add(local.utcoffset())
        dst_values.add(local.dst())
    if offsets != {dt.timedelta(hours=2)}:
        raise SarbMpcDecisionSourceError("johannesburg_offset_not_fixed_utc_plus_two")
    if dst_values != {dt.timedelta(0)}:
        raise SarbMpcDecisionSourceError("johannesburg_dst_detected")


def _scheduled_utc(event_date: str, local_time: str) -> dt.datetime:
    assert_johannesburg_utc_plus_two_no_dst(int(event_date[:4]))
    local = dt.datetime.fromisoformat(f"{event_date}T{local_time}:00").replace(
        tzinfo=ZoneInfo(TIMEZONE_NAME)
    )
    if local.utcoffset() != dt.timedelta(hours=2) or local.dst() != dt.timedelta(0):
        raise SarbMpcDecisionSourceError("event_clock_timezone_contract_failed")
    return local.astimezone(UTC)


def _guardrails() -> dict[str, Any]:
    return {
        "research_only": True,
        "shadow_only": True,
        "schedule_and_statement_are_one_event": True,
        "same_time_global_event_confounding_required": True,
        "causal_attribution_status": "unresolved",
        "direction_policy": "abstain",
        "direction": None,
        "currency_bias": None,
        "pair_bias": None,
        "consensus": None,
        "surprise": None,
        "confirmation_eligible": False,
        "promotion_eligible": False,
        "proof_eligible": False,
        "authorization_eligible": False,
        "execution_eligible": False,
        "can_authorize": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }


def _assert_guardrails(record: Mapping[str, Any]) -> None:
    expected = _guardrails()
    for key, value in expected.items():
        if not _strict_equal(record.get(key), value):
            raise SarbMpcDecisionSourceError(f"guardrail_violation:{key}")
    forbidden = {
        "raw_html",
        "raw_text",
        "full_text",
        "article_text",
        "headline_direction",
        "expected_net_pips",
        "forecast_mean_bps",
        "allocator_rank",
    }
    overlap = forbidden.intersection(record)
    if overlap:
        raise SarbMpcDecisionSourceError(
            f"forbidden_output_surface:{','.join(sorted(overlap))}"
        )


_SCHEDULE_ROW = re.compile(
    r"(?P<ymd>2026/\d{2}/\d{2})\s*-\s*(?P<time>\d{2}:\d{2})\s*"
    r"(?P<display>\d{1,2}\s+[A-Za-z]+\s+2026)\s+"
    r"press\s+conference\s+of\s+the\s+Monetary\s+Policy\s+Committee",
    re.IGNORECASE,
)


def _schedule_binding_material(
    *,
    event_date: str,
    scheduled_utc: str,
    schedule_observed_utc: str,
    schedule_source_sha256: str,
) -> dict[str, Any]:
    """Bind one event identity to the exact official page observation."""

    return {
        "source_id": SOURCE_ID,
        "source_contract_id": CONTRACT_ID,
        "source_cohort_id": COHORT_ID,
        "event_id": _canonical_event_id(event_date),
        "independent_episode_key": _canonical_episode_key(event_date),
        "event_date": event_date,
        "currency": "ZAR",
        "currency_factor": "ZAR",
        "scheduled_utc": scheduled_utc,
        "schedule_observed_utc": schedule_observed_utc,
        "schedule_source_url": SCHEDULE_URL,
        "schedule_source_sha256": schedule_source_sha256,
        "schedule_source_bytes": SCHEDULE_SOURCE_BYTES,
        "schedule_observation_id": SCHEDULE_OBSERVATION_ID,
        "schedule_observation_manifest_path": SCHEDULE_OBSERVATION_MANIFEST_PATH,
        "schedule_observation_manifest_sha256": SCHEDULE_OBSERVATION_MANIFEST_SHA256,
        "schedule_facts_sha256": SCHEDULE_FACTS_SHA256,
    }


def _schedule_binding_sha256(**kwargs: str) -> str:
    return canonical_sha256(_schedule_binding_material(**kwargs))


def parse_official_schedule(
    payload: bytes,
    *,
    observed_utc: str | dt.datetime,
    source_url: str = SCHEDULE_URL,
) -> dict[str, Any]:
    """Parse the frozen July/September/November 2026 SARB schedule facts."""

    normalized_url = _official_url(source_url, expected_path=urlparse(SCHEDULE_URL).path)
    if type(observed_utc) is not str or observed_utc != SCHEDULE_OBSERVED_UTC:
        raise SarbMpcDecisionSourceError(
            "schedule_observation_timestamp_mismatch_requires_new_cohort"
        )
    observed = _parse_utc(observed_utc)
    observed_text = SCHEDULE_OBSERVED_UTC
    source_hash = _validate_frozen_schedule_payload(payload)
    text = _html_to_text(payload)
    matches: dict[str, list[tuple[str, str]]] = {}
    for match in _SCHEDULE_ROW.finditer(text):
        date = dt.datetime.strptime(match.group("ymd"), "%Y/%m/%d").date()
        display = dt.datetime.strptime(match.group("display"), "%d %B %Y").date()
        if display != date:
            raise SarbMpcDecisionSourceError("schedule_display_date_mismatch")
        key = date.isoformat()
        matches.setdefault(key, []).append((match.group("time"), match.group(0)))

    rows: list[dict[str, Any]] = []
    for expected in EXPECTED_SCHEDULE_FACTS:
        event_date = expected["event_date"]
        candidates = matches.get(event_date, [])
        if len(candidates) != 1:
            reason = "missing" if not candidates else "duplicate"
            raise SarbMpcDecisionSourceError(
                f"schedule_{reason}_target_row_requires_new_cohort:{event_date}"
            )
        local_time = candidates[0][0]
        if local_time != expected["local_time"]:
            raise SarbMpcDecisionSourceError(
                f"schedule_time_revision_requires_new_cohort:{event_date}"
            )
        scheduled = _scheduled_utc(event_date, local_time)
        if _iso_utc(scheduled) != expected["scheduled_utc"]:
            raise SarbMpcDecisionSourceError("schedule_utc_conversion_mismatch")
        historical = event_date == "2026-07-23"
        captured_pre_event = observed < scheduled
        if historical:
            collection_class = "historical_availability_counterfactual_archive_only"
            prospective_schedule = False
        elif captured_pre_event:
            collection_class = "prospective_schedule_only"
            prospective_schedule = True
        else:
            collection_class = "late_schedule_capture_archive_only"
            prospective_schedule = False
        scheduled_text = _iso_utc(scheduled)
        binding_hash = _schedule_binding_sha256(
            event_date=event_date,
            scheduled_utc=scheduled_text,
            schedule_observed_utc=observed_text,
            schedule_source_sha256=source_hash,
        )
        row = {
            "schema_version": SCHEMA_VERSION,
            "source_id": SOURCE_ID,
            "source_contract_id": CONTRACT_ID,
            "source_cohort_id": COHORT_ID,
            "event_id": _canonical_event_id(event_date),
            "event_series_id": "sarb_mpc_decision",
            "event_date": event_date,
            "currency": "ZAR",
            "currency_factor": "ZAR",
            "independent_episode_key": _canonical_episode_key(event_date),
            "local_timezone": TIMEZONE_NAME,
            "local_time": local_time,
            "scheduled_utc": scheduled_text,
            "schedule_observed_utc": observed_text,
            "schedule_captured_pre_event": captured_pre_event,
            "prospective_schedule": prospective_schedule,
            "collection_class": collection_class,
            "schedule_source_url": normalized_url,
            "schedule_source_sha256": source_hash,
            "schedule_source_bytes": SCHEDULE_SOURCE_BYTES,
            "schedule_observation_id": SCHEDULE_OBSERVATION_ID,
            "schedule_observation_manifest_path": SCHEDULE_OBSERVATION_MANIFEST_PATH,
            "schedule_observation_manifest_sha256": SCHEDULE_OBSERVATION_MANIFEST_SHA256,
            "schedule_facts_sha256": SCHEDULE_FACTS_SHA256,
            "schedule_binding_sha256": binding_hash,
            "decision_causal_known_utc": None,
            "statement_first_seen_utc": None,
            "statement_source_url": None,
            "statement_source_sha256": None,
            "license_full_text_retention_verified": False,
            "full_text_retained": False,
            **_guardrails(),
        }
        _assert_guardrails(row)
        rows.append(row)

    result = {
        "schema_version": SCHEMA_VERSION,
        "source_id": SOURCE_ID,
        "source_contract_id": CONTRACT_ID,
        "source_cohort_id": COHORT_ID,
        "source_url": normalized_url,
        "source_sha256": source_hash,
        "source_bytes": SCHEDULE_SOURCE_BYTES,
        "observation_id": SCHEDULE_OBSERVATION_ID,
        "observation_manifest_path": SCHEDULE_OBSERVATION_MANIFEST_PATH,
        "observation_manifest_sha256": SCHEDULE_OBSERVATION_MANIFEST_SHA256,
        "facts_sha256": SCHEDULE_FACTS_SHA256,
        "observed_utc": observed_text,
        "events": rows,
        "event_count": len(rows),
        "license_full_text_retention_verified": False,
        "full_text_retained": False,
        **_guardrails(),
    }
    _assert_guardrails(result)
    return result


_HOLD_DECISION = re.compile(
    r"committee\s+decided\s+to\s+(?:keep|hold)\s+the\s+policy\s+rate\s+"
    r"(?:unchanged\s*,?\s*)?at\s+(?P<rate>\d+(?:\.\d+)?)\s*%",
    re.IGNORECASE,
)
_MOVE_DECISION = re.compile(
    r"committee\s+decided\s+to\s+(?P<verb>increase|raise|decrease|reduce|cut)\s+"
    r"the\s+policy\s+rate(?:\s+by\s+(?P<bp>\d+)\s+basis\s+points?)?\s+"
    r"(?:to|at)\s+(?P<rate>\d+(?:\.\d+)?)\s*%",
    re.IGNORECASE,
)
_VOTE_SPLIT = re.compile(
    r"(?P<count1>[A-Za-z]+|\d+)\s+members?\s+preferred\s+(?:a|an)\s+"
    r"(?P<preference1>hold|increase|hike|decrease|cut)"
    r"(?:\s+of\s+(?P<bp1>\d+)\s+basis\s+points?)?\s*,?\s*"
    r"(?:while|and)\s+(?P<count2>[A-Za-z]+|\d+)\s+"
    r"(?:members?\s+)?favou?red\s+(?:a|an)\s+"
    r"(?P<preference2>hold|increase|hike|decrease|cut)"
    r"(?:\s+of\s+(?P<bp2>\d+)\s+basis\s+points?)?",
    re.IGNORECASE,
)


def _count(value: str) -> int:
    lowered = value.lower()
    if lowered.isdigit():
        result = int(lowered)
    elif lowered in _WORD_NUMBERS:
        result = _WORD_NUMBERS[lowered]
    else:
        raise SarbMpcDecisionSourceError("unrecognized_vote_count")
    if result < 0 or result > 25:
        raise SarbMpcDecisionSourceError("implausible_vote_count")
    return result


def _preference(value: str) -> str:
    lowered = value.lower()
    if lowered == "hold":
        return "hold"
    if lowered in {"increase", "hike"}:
        return "hike"
    if lowered in {"decrease", "cut"}:
        return "cut"
    raise SarbMpcDecisionSourceError("unrecognized_vote_preference")


def _validate_schedule_event(
    schedule_event: Mapping[str, Any],
) -> tuple[str, dt.datetime, dt.datetime, bool]:
    exact_identity = {
        "schema_version": SCHEMA_VERSION,
        "source_id": SOURCE_ID,
        "source_contract_id": CONTRACT_ID,
        "source_cohort_id": COHORT_ID,
        "event_series_id": "sarb_mpc_decision",
        "currency": "ZAR",
        "currency_factor": "ZAR",
        "local_timezone": TIMEZONE_NAME,
        "schedule_source_url": SCHEDULE_URL,
        "schedule_observed_utc": SCHEDULE_OBSERVED_UTC,
        "schedule_source_sha256": SCHEDULE_SOURCE_SHA256,
        "schedule_source_bytes": SCHEDULE_SOURCE_BYTES,
        "schedule_observation_id": SCHEDULE_OBSERVATION_ID,
        "schedule_observation_manifest_path": SCHEDULE_OBSERVATION_MANIFEST_PATH,
        "schedule_observation_manifest_sha256": SCHEDULE_OBSERVATION_MANIFEST_SHA256,
        "schedule_facts_sha256": SCHEDULE_FACTS_SHA256,
    }
    for key, expected_value in exact_identity.items():
        if not _strict_equal(schedule_event.get(key), expected_value):
            raise SarbMpcDecisionSourceError(f"schedule_identity_mismatch:{key}")

    event_date_value = schedule_event.get("event_date")
    if type(event_date_value) is not str:
        raise SarbMpcDecisionSourceError("schedule_event_date_not_string")
    event_date = event_date_value
    expected = {row["event_date"]: row for row in EXPECTED_SCHEDULE_FACTS}.get(event_date)
    if expected is None:
        raise SarbMpcDecisionSourceError("event_not_in_frozen_schedule_contract")
    if schedule_event.get("event_id") != _canonical_event_id(event_date):
        raise SarbMpcDecisionSourceError("schedule_event_id_mismatch")
    if schedule_event.get("independent_episode_key") != _canonical_episode_key(event_date):
        raise SarbMpcDecisionSourceError("schedule_episode_key_mismatch")
    if schedule_event.get("local_time") != expected["local_time"]:
        raise SarbMpcDecisionSourceError("schedule_event_local_time_mismatch")
    if schedule_event.get("scheduled_utc") != expected["scheduled_utc"]:
        raise SarbMpcDecisionSourceError("schedule_event_clock_mismatch")

    observed_value = schedule_event.get("schedule_observed_utc")
    if type(observed_value) is not str:
        raise SarbMpcDecisionSourceError("schedule_observed_utc_not_string")
    observed = _parse_utc(observed_value)
    scheduled = _parse_utc(expected["scheduled_utc"])
    captured_pre_event = observed < scheduled

    captured_flag = schedule_event.get("schedule_captured_pre_event")
    prospective_flag = schedule_event.get("prospective_schedule")
    if type(captured_flag) is not bool:
        raise SarbMpcDecisionSourceError("schedule_captured_pre_event_not_bool")
    if type(prospective_flag) is not bool:
        raise SarbMpcDecisionSourceError("prospective_schedule_not_bool")
    if captured_flag is not captured_pre_event:
        raise SarbMpcDecisionSourceError("schedule_captured_pre_event_inconsistent")
    expected_prospective = event_date != "2026-07-23" and captured_pre_event
    if prospective_flag is not expected_prospective:
        raise SarbMpcDecisionSourceError("prospective_schedule_inconsistent")
    if event_date == "2026-07-23":
        expected_class = "historical_availability_counterfactual_archive_only"
    elif captured_pre_event:
        expected_class = "prospective_schedule_only"
    else:
        expected_class = "late_schedule_capture_archive_only"
    if schedule_event.get("collection_class") != expected_class:
        raise SarbMpcDecisionSourceError("schedule_collection_class_inconsistent")

    source_hash = schedule_event.get("schedule_source_sha256")
    binding = schedule_event.get("schedule_binding_sha256")
    if type(binding) is not str or re.fullmatch(r"[0-9a-f]{64}", binding) is None:
        raise SarbMpcDecisionSourceError("schedule_binding_sha256_invalid")
    expected_binding = _schedule_binding_sha256(
        event_date=event_date,
        scheduled_utc=expected["scheduled_utc"],
        schedule_observed_utc=observed_value,
        schedule_source_sha256=source_hash,
    )
    if binding != expected_binding:
        raise SarbMpcDecisionSourceError("schedule_source_content_binding_mismatch")
    _assert_guardrails(schedule_event)
    return event_date, scheduled, observed, captured_pre_event


_WORKSPACE_ROOT = Path(__file__).resolve().parents[3]


def _workspace_path(relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise SarbMpcDecisionSourceError("archive_path_not_workspace_relative")
    candidate = (_WORKSPACE_ROOT / relative).absolute()
    try:
        candidate.relative_to(_WORKSPACE_ROOT)
    except ValueError as exc:
        raise SarbMpcDecisionSourceError("archive_path_escaped_workspace") from exc
    return candidate


def _file_identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        int(value.st_dev),
        int(value.st_ino),
        int(value.st_size),
        int(value.st_mtime_ns),
        int(value.st_nlink),
    )


def _read_exact_pinned_file(
    path: str | Path,
    *,
    expected_path: str | Path,
    expected_bytes: int,
    expected_sha256: str,
    require_read_only: bool,
) -> bytes:
    """Read one pinned file while rejecting alias, link, and path-swap attacks."""

    requested = Path(path).absolute()
    expected = Path(expected_path).absolute()
    if os.path.normcase(str(requested)) != os.path.normcase(str(expected)):
        raise SarbMpcDecisionSourceError("pinned_file_path_substitution")
    if requested.is_symlink():
        raise SarbMpcDecisionSourceError("pinned_file_symlink_forbidden")
    try:
        resolved = requested.resolve(strict=True)
        expected_resolved = expected.resolve(strict=True)
    except OSError as exc:
        raise SarbMpcDecisionSourceError("pinned_file_missing") from exc
    if resolved != expected_resolved:
        raise SarbMpcDecisionSourceError("pinned_file_resolved_path_mismatch")

    before = os.lstat(requested)
    if not stat.S_ISREG(before.st_mode):
        raise SarbMpcDecisionSourceError("pinned_file_not_regular")
    if before.st_nlink != 1:
        raise SarbMpcDecisionSourceError("pinned_file_hardlink_forbidden")
    if require_read_only:
        if os.name == "nt":
            readonly_flag = getattr(stat, "FILE_ATTRIBUTE_READONLY", 0x1)
            attributes = int(getattr(before, "st_file_attributes", 0))
            if attributes & readonly_flag == 0:
                raise SarbMpcDecisionSourceError("pinned_archive_not_read_only")
        elif before.st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH):
            raise SarbMpcDecisionSourceError("pinned_archive_not_read_only")

    with requested.open("rb") as handle:
        opened_before = os.fstat(handle.fileno())
        if _file_identity(opened_before) != _file_identity(before):
            raise SarbMpcDecisionSourceError("pinned_file_swapped_before_open")
        raw = handle.read()
        opened_after = os.fstat(handle.fileno())
    after = os.lstat(requested)
    if _file_identity(opened_before) != _file_identity(opened_after):
        raise SarbMpcDecisionSourceError("pinned_file_changed_while_reading")
    if _file_identity(opened_after) != _file_identity(after):
        raise SarbMpcDecisionSourceError("pinned_file_path_swapped_after_read")
    if len(raw) != expected_bytes:
        raise SarbMpcDecisionSourceError("pinned_file_byte_count_mismatch")
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise SarbMpcDecisionSourceError("pinned_file_sha256_mismatch")
    return raw


def _load_frozen_schedule_archive() -> dict[str, Any]:
    observation = load_schedule_observation_manifest()
    if observation["raw_payload_archive_path"] != SCHEDULE_SOURCE_ARCHIVE_PATH:
        raise SarbMpcDecisionSourceError("schedule_archive_path_manifest_mismatch")
    archive_path = _workspace_path(SCHEDULE_SOURCE_ARCHIVE_PATH)
    raw = _read_exact_pinned_file(
        archive_path,
        expected_path=archive_path,
        expected_bytes=SCHEDULE_SOURCE_BYTES,
        expected_sha256=SCHEDULE_SOURCE_SHA256,
        require_read_only=True,
    )
    return parse_official_schedule(
        raw,
        observed_utc=SCHEDULE_OBSERVED_UTC,
        source_url=SCHEDULE_URL,
    )


def _statement_identity(source_url: str) -> tuple[str, str]:
    normalized = _official_url(source_url)
    path = urlparse(normalized).path.rstrip("/")
    for expected in EXPECTED_SCHEDULE_FACTS:
        event_date = expected["event_date"]
        event = dt.date.fromisoformat(event_date)
        expected_path = (
            f"{STATEMENT_PATH_PREFIX}{event.year}/{event.strftime('%B').lower()}"
        )
        if path == expected_path:
            return normalized, event_date
    raise SarbMpcDecisionSourceError("statement_url_not_in_frozen_event_contract")


def parse_official_decision(
    payload: bytes,
    *,
    source_url: str,
    statement_first_seen_utc: str | dt.datetime,
) -> dict[str, Any]:
    """Parse bounded decision facts and apply the causal-knowledge boundary."""

    normalized_url, event_date = _statement_identity(source_url)
    archived_schedule = _load_frozen_schedule_archive()
    candidates = [
        row
        for row in archived_schedule["events"]
        if row.get("event_date") == event_date
    ]
    if len(candidates) != 1:
        raise SarbMpcDecisionSourceError("archived_schedule_event_not_unique")
    schedule_event = candidates[0]
    event_date, scheduled, schedule_observed, schedule_pre_event = (
        _validate_schedule_event(schedule_event)
    )
    first_seen = _parse_utc(statement_first_seen_utc)
    if first_seen < scheduled:
        raise SarbMpcDecisionSourceError(
            "statement_first_seen_before_scheduled_event_quarantined"
        )
    if first_seen < schedule_observed:
        raise SarbMpcDecisionSourceError(
            "statement_first_seen_before_schedule_observation_quarantined"
        )
    text = _html_to_text(payload)
    event = dt.date.fromisoformat(event_date)
    month_year = f"{event.strftime('%B')} {event.year}"
    if not re.search(
        rf"Statement\s+of\s+(?:the\s+Monetary\s+Policy\s+Committee|the\s+MPC)\s+{re.escape(month_year)}",
        text,
        re.IGNORECASE,
    ):
        raise SarbMpcDecisionSourceError("statement_title_or_event_month_missing")

    action_matches: list[tuple[str, re.Match[str]]] = [
        ("hold", match) for match in _HOLD_DECISION.finditer(text)
    ]
    action_matches.extend(("move", match) for match in _MOVE_DECISION.finditer(text))
    action_matches.sort(key=lambda item: item[1].start())
    if len(action_matches) != 1:
        raise SarbMpcDecisionSourceError("decision_sentence_missing_or_ambiguous")
    action_kind, action_match = action_matches[0]
    if action_kind == "hold":
        action = "hold"
        rate = float(action_match.group("rate"))
        decision_basis_points = 0
    else:
        action = (
            "hike"
            if action_match.group("verb").lower() in {"increase", "raise"}
            else "cut"
        )
        rate = float(action_match.group("rate"))
        move_basis_points = action_match.group("bp")
        if move_basis_points is None:
            raise SarbMpcDecisionSourceError(
                "directional_decision_missing_explicit_basis_points"
            )
        decision_basis_points = int(move_basis_points)
        if decision_basis_points <= 0:
            raise SarbMpcDecisionSourceError(
                "directional_decision_nonpositive_basis_points"
            )

    sentences = [
        sentence.strip()
        for sentence in re.split(r"(?<=[.!?])\s+", text)
        if sentence.strip()
    ]
    vote_sentences = [
        sentence
        for sentence in sentences
        if re.search(r"\bmembers?\b", sentence, re.IGNORECASE)
        and re.search(r"\b(?:preferred|favou?red)\b", sentence, re.IGNORECASE)
    ]
    if len(vote_sentences) != 1:
        raise SarbMpcDecisionSourceError("decision_vote_split_missing_or_ambiguous")
    vote_sentence = vote_sentences[0].rstrip(".!?").strip()
    vote = _VOTE_SPLIT.fullmatch(vote_sentence)
    if vote is None:
        raise SarbMpcDecisionSourceError("unsupported_or_complex_vote_split")
    votes = (
        {
            "count": _count(vote.group("count1")),
            "preference": _preference(vote.group("preference1")),
            "basis_points": int(vote.group("bp1") or 0),
        },
        {
            "count": _count(vote.group("count2")),
            "preference": _preference(vote.group("preference2")),
            "basis_points": int(vote.group("bp2") or 0),
        },
    )
    if votes[0]["preference"] == votes[1]["preference"]:
        raise SarbMpcDecisionSourceError("duplicate_vote_preference")
    for option in votes:
        if option["preference"] == "hold" and option["basis_points"] != 0:
            raise SarbMpcDecisionSourceError("hold_vote_has_nonzero_basis_points")
        if option["preference"] != "hold" and option["basis_points"] <= 0:
            raise SarbMpcDecisionSourceError("directional_vote_missing_basis_points")
    if sum(item["count"] for item in votes) <= 0:
        raise SarbMpcDecisionSourceError("empty_vote_split")

    decision_facts = {
        "event_date": event_date,
        "action": action,
        "policy_rate_pct": rate,
        "votes": votes,
        "statement_url": normalized_url,
    }
    decision_facts_hash = canonical_sha256(decision_facts)
    if event_date == "2026-07-23" and decision_facts_hash != JULY_DECISION_FACTS_SHA256:
        raise SarbMpcDecisionSourceError(
            "historical_july_facts_changed_requires_new_contract"
        )

    causal_known = max(scheduled, first_seen)
    historical = event_date == "2026-07-23"
    if historical:
        collection_class = "historical_availability_counterfactual_archive_only"
        prospective_observation = False
    elif schedule_pre_event:
        collection_class = "prospective_causal_decision_observation"
        prospective_observation = True
    else:
        collection_class = "late_schedule_capture_archive_only"
        prospective_observation = False

    record = {
        "schema_version": SCHEMA_VERSION,
        "source_id": SOURCE_ID,
        "source_contract_id": CONTRACT_ID,
        "source_cohort_id": COHORT_ID,
        "event_id": _canonical_event_id(event_date),
        "event_series_id": "sarb_mpc_decision",
        "event_date": event_date,
        "currency": "ZAR",
        "currency_factor": "ZAR",
        "independent_episode_key": _canonical_episode_key(event_date),
        "scheduled_utc": _iso_utc(scheduled),
        "schedule_observed_utc": schedule_event["schedule_observed_utc"],
        "statement_first_seen_utc": _iso_utc(first_seen),
        "decision_causal_known_utc": _iso_utc(causal_known),
        "clock_floor_applied": first_seen < scheduled,
        "schedule_captured_pre_event": schedule_pre_event,
        "prospective_observation": prospective_observation,
        "collection_class": collection_class,
        "action": action,
        "policy_rate_pct": rate,
        "decision_basis_points": decision_basis_points,
        "votes": votes,
        "vote_total_observed": sum(item["count"] for item in votes),
        "statement_source_url": normalized_url,
        "statement_source_sha256": _source_sha256(payload),
        "decision_facts_sha256": decision_facts_hash,
        "schedule_source_url": schedule_event["schedule_source_url"],
        "schedule_source_sha256": schedule_event["schedule_source_sha256"],
        "schedule_source_bytes": schedule_event["schedule_source_bytes"],
        "schedule_observation_id": schedule_event["schedule_observation_id"],
        "schedule_observation_manifest_path": schedule_event[
            "schedule_observation_manifest_path"
        ],
        "schedule_observation_manifest_sha256": schedule_event[
            "schedule_observation_manifest_sha256"
        ],
        "schedule_facts_sha256": SCHEDULE_FACTS_SHA256,
        "schedule_binding_sha256": schedule_event["schedule_binding_sha256"],
        "license_full_text_retention_verified": False,
        "full_text_retained": False,
        **_guardrails(),
    }
    _assert_guardrails(record)
    return record


EXPECTED_CONTRACT_MANIFEST: dict[str, Any] = {
    "schema_version": SCHEMA_VERSION,
    "source_id": SOURCE_ID,
    "source_contract_id": CONTRACT_ID,
    "source_cohort_id": COHORT_ID,
    "registered_with_live_collector": False,
    "enabled": False,
    "runtime_supported": False,
    "research_only": True,
    "shadow_only": True,
    "direction_policy": "abstain",
    "execution_eligible": False,
    "full_text_retention_enabled": False,
    "full_text_license_verified": False,
    "currency": "ZAR",
    "timezone": TIMEZONE_NAME,
    "timezone_contract": "UTC+02:00 year-round; daylight saving time forbidden",
    "schedule_url": SCHEDULE_URL,
    "schedule_observation_id": SCHEDULE_OBSERVATION_ID,
    "schedule_observed_utc": SCHEDULE_OBSERVED_UTC,
    "schedule_observed_utc_original_100ns": SCHEDULE_OBSERVED_UTC_ORIGINAL_100NS,
    "schedule_source_bytes": SCHEDULE_SOURCE_BYTES,
    "schedule_source_sha256": SCHEDULE_SOURCE_SHA256,
    "schedule_source_archive_path": SCHEDULE_SOURCE_ARCHIVE_PATH,
    "schedule_source_archive_read_only": True,
    "schedule_observation_manifest_path": SCHEDULE_OBSERVATION_MANIFEST_PATH,
    "schedule_observation_manifest_sha256": SCHEDULE_OBSERVATION_MANIFEST_SHA256,
    "july_statement_url": JULY_STATEMENT_URL,
    "trusted_domains": ["resbank.co.za", "www.resbank.co.za"],
    "schedule_facts": EXPECTED_SCHEDULE_FACTS,
    "schedule_facts_sha256": SCHEDULE_FACTS_SHA256,
    "july_decision_facts": EXPECTED_JULY_DECISION_FACTS,
    "july_decision_facts_sha256": JULY_DECISION_FACTS_SHA256,
    "evidence_policy": EXPECTED_EVIDENCE_POLICY,
    "fact_retention_policy": EXPECTED_FACT_RETENTION_POLICY,
    "hard_guards": EXPECTED_HARD_GUARDS,
    "separation_contract": EXPECTED_SEPARATION_CONTRACT,
}


def _strict_json(raw: bytes, *, label: str) -> Any:
    def reject_constant(value: str) -> None:
        raise SarbMpcDecisionSourceError(f"{label}_nonfinite_number:{value}")

    def parse_float(value: str) -> float:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise SarbMpcDecisionSourceError(f"{label}_nonfinite_number:{value}")
        return parsed

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise SarbMpcDecisionSourceError(f"{label}_duplicate_key:{key}")
            result[key] = value
        return result

    try:
        text = raw.decode("utf-8")
        return json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
            parse_float=parse_float,
        )
    except SarbMpcDecisionSourceError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise SarbMpcDecisionSourceError(f"{label}_invalid_json") from exc


def load_schedule_observation_manifest() -> dict[str, Any]:
    observation_path = _workspace_path(SCHEDULE_OBSERVATION_MANIFEST_PATH)
    raw = _read_exact_pinned_file(
        observation_path,
        expected_path=observation_path,
        expected_bytes=2_117,
        expected_sha256=SCHEDULE_OBSERVATION_MANIFEST_SHA256,
        require_read_only=False,
    )
    observation = _strict_json(raw, label="schedule_observation_manifest")
    if not _strict_equal(observation, EXPECTED_OBSERVATION_MANIFEST):
        raise SarbMpcDecisionSourceError("schedule_observation_manifest_typed_mismatch")
    return observation


def load_contract_manifest(path: str | Path) -> dict[str, Any]:
    manifest_path = Path(path)
    raw = manifest_path.read_bytes()
    manifest = _strict_json(raw, label="contract_manifest")
    if not _strict_equal(manifest, EXPECTED_CONTRACT_MANIFEST):
        raise SarbMpcDecisionSourceError("contract_manifest_deep_typed_mismatch")
    load_schedule_observation_manifest()
    archive_path = _workspace_path(SCHEDULE_SOURCE_ARCHIVE_PATH)
    _read_exact_pinned_file(
        archive_path,
        expected_path=archive_path,
        expected_bytes=SCHEDULE_SOURCE_BYTES,
        expected_sha256=SCHEDULE_SOURCE_SHA256,
        require_read_only=True,
    )
    return manifest


__all__ = [
    "COHORT_ID",
    "CONTRACT_ID",
    "EXPECTED_JULY_DECISION_FACTS",
    "EXPECTED_SCHEDULE_FACTS",
    "JULY_DECISION_FACTS_SHA256",
    "JULY_STATEMENT_URL",
    "SCHEDULE_FACTS_SHA256",
    "SCHEDULE_OBSERVATION_ID",
    "SCHEDULE_OBSERVATION_MANIFEST_PATH",
    "SCHEDULE_OBSERVATION_MANIFEST_SHA256",
    "SCHEDULE_OBSERVED_UTC",
    "SCHEDULE_OBSERVED_UTC_ORIGINAL_100NS",
    "SCHEDULE_SOURCE_ARCHIVE_PATH",
    "SCHEDULE_SOURCE_BYTES",
    "SCHEDULE_SOURCE_SHA256",
    "SCHEDULE_URL",
    "SCHEMA_VERSION",
    "SOURCE_ID",
    "SarbMpcDecisionSourceError",
    "assert_johannesburg_utc_plus_two_no_dst",
    "canonical_sha256",
    "load_contract_manifest",
    "load_schedule_observation_manifest",
    "parse_official_decision",
    "parse_official_schedule",
]
