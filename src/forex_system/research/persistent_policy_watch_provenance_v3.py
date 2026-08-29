"""Inert V3 policy-context and news-watch provenance candidate.

The module contains pure transformations only: no file, database, network,
process, broker, lifecycle, authorization, promotion, or registration surface.
It intentionally does not import the running collector or watch worker.

Episode semantics
-----------------
An ``episode_id`` names one causally known story/event cluster for one currency
factor.  Syndicated copies and exact duplicate source evidence do not create a
new episode or increase evidence count.  A material revision must have a new
event ID or payload hash and therefore produces a different evidence digest.
The watch identity commits to that digest, the exact V40/V4 provenance, and the
frozen episode-semantics contract.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import re
from collections import Counter
from types import MappingProxyType
from typing import Any, Mapping, Sequence


UTC = dt.timezone.utc

POLICY_CONTEXT_CONTRACT_ID = (
    "persistent_policy_context_v3_v40_clock_v4_20260817"
)
WATCH_PROVENANCE_CONTRACT_ID = (
    "news_technical_watchlist_v39_provenance_digest_candidate_20260817"
)
EPISODE_SEMANTICS_ID = (
    "point_in_time_story_event_cluster_currency_factor_v1_20260817"
)
EPISODE_SEMANTICS = (
    "One causally known story/event cluster for one currency factor; exact "
    "duplicates and syndication do not increase evidence, while a material "
    "revision changes event identity or payload hash."
)

REQUIRED_OBSERVATION_TIME_CONTRACT_ID = (
    "observation_time_v4_fail_closed_consistent_integrity_sources_20260817"
)
REQUIRED_COLLECTOR_CONTRACT_ID = (
    "local_news_incremental_source_commit_v40_20260817"
)
REQUIRED_COLLECTOR_COHORT_ID = REQUIRED_COLLECTOR_CONTRACT_ID

CANONICAL_CURRENCIES = (
    "AUD", "CAD", "CHF", "CNH", "CZK", "DKK", "EUR", "GBP", "HKD", "HUF", "JPY",
    "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "THB", "TRY", "USD", "ZAR",
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

DIRECTION_ALLOWLIST = frozenset({"long", "short"})
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
WATCH_COHORT_BY_ARM = MappingProxyType(
    {
        arm: f"{WATCH_PROVENANCE_CONTRACT_ID}.{arm}"
        for arm in sorted(ARM_ALLOWLIST)
    }
)

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@/+|-]{0,191}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ERROR_PATTERNS = (
    re.compile(r"\bservice is currently unavailable\b", re.IGNORECASE),
    re.compile(r"\btemporarily unavailable\b", re.IGNORECASE),
    re.compile(r"\bscheduled maintenance\b", re.IGNORECASE),
    re.compile(r"\bunder maintenance\b", re.IGNORECASE),
    re.compile(r"\baccess denied\b", re.IGNORECASE),
    re.compile(r"\benable javascript(?: and cookies)? to continue\b", re.IGNORECASE),
    re.compile(r"\bpage (?:was )?not found\b", re.IGNORECASE),
    re.compile(r"\b(?:http )?error\s+(?:403|404|429|500|502|503|504)\b", re.IGNORECASE),
    re.compile(r"\bsomething went wrong\b", re.IGNORECASE),
    re.compile(r"\brequest (?:was )?blocked\b", re.IGNORECASE),
    re.compile(r"\bchecking your browser\b", re.IGNORECASE),
)
_STATUS_FIELDS = (
    "fetch_status",
    "detail_fetch_status",
    "enrichment_status",
    "detail_enrichment_status",
    "source_status",
    "quality_state",
    "http_status",
    "last_status",
)
_BAD_STATUS_TOKENS = frozenset(
    {
        "access_denied",
        "blocked",
        "error",
        "failed",
        "invalid",
        "maintenance",
        "quarantined",
        "stale",
        "timeout",
        "unavailable",
    }
)

POLICY_DOCUMENT_FIELDS = frozenset(
    {
        "currency",
        "grade",
        "state",
        "monetary_impulse",
        "known_utc",
        "published_utc",
        "scheduled_utc",
        "source_id",
        "event_id",
        "headline",
        "summary",
        "policy_document_type",
        "content_sha256",
        "observation_clock_trusted",
        "observation_time_contract_id",
        "collector_contract_id",
        "collector_cohort_id",
        "policy_context_contract_id",
        "research_only",
        "execution_eligible",
    }
)
CURRENCY_ROW_FIELDS = frozenset(
    {
        "currency",
        "status",
        "missing_reason",
        "proof_grade_context",
        "legacy_diagnostic_context",
        "rejection_counts",
        "research_only",
        "execution_eligible",
    }
)
COVERAGE_FIELDS = frozenset(
    {
        "expected_currency_count",
        "reported_currency_count",
        "proof_grade_currency_count",
        "missing_currency_count",
        "legacy_diagnostic_currency_count",
        "proof_grade_document_count",
        "legacy_diagnostic_document_count",
        "rejected_document_count",
        "missing_reason_counts",
    }
)
POLICY_OUTPUT_FIELDS = frozenset(
    {
        "schema_version",
        "contract_id",
        "generated_utc",
        "required_observation_time_contract_id",
        "required_collector_contract_id",
        "required_collector_cohort_id",
        "research_only",
        "execution_eligible",
        "coverage",
        "currencies",
        "proof_grade_documents",
        "legacy_diagnostics",
        "rejections",
    }
)
REJECTION_FIELDS = frozenset({"event_id", "source_id", "currencies", "reason"})
EVIDENCE_FIELDS = frozenset(
    {
        "event_id",
        "payload_sha256",
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
        "payload_sha256s",
        "source_evidence_count",
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
        "source_evidence_count",
        "factor_score",
        "factor_confidence",
        "event_ids",
        "payload_sha256s",
        "observation_clock_trusted",
        "observation_time_contract_id",
        "collector_contract_id",
        "collector_cohort_id",
        "research_only",
        "execution_eligible",
    }
)


class ContractViolation(ValueError):
    """Raised when an inert candidate input violates its frozen contract."""


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


def _strict_plain_json(value: Any, *, path: str = "$") -> Any:
    if value is None or type(value) in {str, int, bool}:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ContractViolation(f"nonfinite_json:{path}")
        return value
    if type(value) is list:
        return [_strict_plain_json(item, path=f"{path}[]") for item in value]
    if type(value) is dict:
        output: dict[str, Any] = {}
        for key, item in value.items():
            if type(key) is not str or key in output:
                raise ContractViolation(f"invalid_json_key:{path}")
            output[key] = _strict_plain_json(item, path=f"{path}.{key}")
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


def _identifier(value: Any, *, path: str) -> str:
    if type(value) is not str or value != value.strip() or not _IDENTIFIER.fullmatch(value):
        raise ContractViolation(f"invalid_identifier:{path}")
    if not value.isascii() or not value.isprintable():
        raise ContractViolation(f"invalid_identifier:{path}")
    return value


def _optional_identifier(value: Any, *, path: str) -> str:
    if value is None or value == "":
        return ""
    return _identifier(value, path=path)


def _optional_normalized_label(value: Any, *, path: str) -> str:
    if value is None or value == "":
        return ""
    if type(value) is not str or value != value.strip() or not value.isprintable():
        raise ContractViolation(f"invalid_identifier:{path}")
    normalized = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return _identifier(normalized, path=path) if normalized else ""


def _display_text(value: Any, *, limit: int) -> str:
    if type(value) is not str:
        return ""
    printable = "".join(char if char.isprintable() else " " for char in value)
    return " ".join(printable.split())[:limit]


def _sha256(value: Any, *, path: str, required: bool = True) -> str:
    if (value is None or value == "") and not required:
        return ""
    if type(value) is not str or not _SHA256.fullmatch(value):
        raise ContractViolation(f"invalid_sha256:{path}")
    return value


def _number(
    value: Any,
    *,
    path: str,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    if type(value) not in {int, float}:
        raise ContractViolation(f"invalid_number:{path}")
    result = float(value)
    if not math.isfinite(result):
        raise ContractViolation(f"invalid_number:{path}")
    if minimum is not None and result < minimum:
        raise ContractViolation(f"invalid_number:{path}")
    if maximum is not None and result > maximum:
        raise ContractViolation(f"invalid_number:{path}")
    return result


def _exact_aware_iso(value: Any, *, path: str) -> dt.datetime:
    """Accept any exact aware ISO-8601 string and normalize it to UTC."""

    if type(value) is not str or not value or value != value.strip():
        raise ContractViolation(f"invalid_aware_iso:{path}")
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise ContractViolation(f"invalid_aware_iso:{path}") from exc
    timezone = parsed.tzinfo
    if timezone is None or type(timezone) is not dt.timezone:
        raise ContractViolation(f"invalid_aware_iso:{path}")
    offset = parsed.utcoffset()
    if offset is None:
        raise ContractViolation(f"invalid_aware_iso:{path}")
    return parsed.astimezone(UTC)


def _as_of(value: Any) -> dt.datetime:
    if type(value) is not dt.datetime:
        raise ContractViolation("as_of_must_be_exact_aware_datetime")
    timezone = value.tzinfo
    if timezone is None or type(timezone) is not dt.timezone:
        raise ContractViolation("as_of_must_be_exact_builtin_timezone")
    if value.utcoffset() is None:
        raise ContractViolation("as_of_must_be_exact_aware_datetime")
    return value.astimezone(UTC)


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _collector_provenance(value: Mapping[str, Any]) -> bool:
    return bool(
        type(value) is dict
        and value.get("observation_clock_trusted") is True
        and type(value.get("observation_time_contract_id")) is str
        and value.get("observation_time_contract_id")
        == REQUIRED_OBSERVATION_TIME_CONTRACT_ID
        and type(value.get("collector_contract_id")) is str
        and value.get("collector_contract_id") == REQUIRED_COLLECTOR_CONTRACT_ID
        and type(value.get("collector_cohort_id")) is str
        and value.get("collector_cohort_id") == REQUIRED_COLLECTOR_COHORT_ID
    )


def _provider_error_reason(article: Mapping[str, Any]) -> str:
    text = " ".join(
        _display_text(article.get(field), limit=8_000)
        for field in ("headline", "summary", "detail_text", "body", "content")
    )
    if any(pattern.search(text) for pattern in _ERROR_PATTERNS):
        return "provider_error_or_maintenance_content"
    for field in _STATUS_FIELDS:
        value = article.get(field)
        if value is None or value == "":
            continue
        if field in {"http_status", "last_status"}:
            if type(value) is int and not isinstance(value, bool):
                if 200 <= value < 400:
                    continue
                return "bad_fetch_or_enrichment_status"
            if type(value) is str and value.isascii() and value.isdigit():
                status = int(value)
                if 200 <= status < 400:
                    continue
                return "bad_fetch_or_enrichment_status"
            if type(value) is str and re.search(r"(?:^|\D)[45]\d\d(?:\D|$)", value):
                return "bad_fetch_or_enrichment_status"
        if type(value) is not str or value != value.strip() or not value.isprintable():
            return "malformed_fetch_or_enrichment_status"
        normalized = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
        tokens = set(normalized.split("_"))
        if normalized in _BAD_STATUS_TOKENS or tokens & _BAD_STATUS_TOKENS:
            return "bad_fetch_or_enrichment_status"
    return ""


def _authority_currencies(
    article: Mapping[str, Any],
    sources: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    source_id = article.get("source_id")
    configured = sources.get(source_id) if type(source_id) is str else None
    configured = configured if type(configured) is dict else {}
    values = (
        configured.get("currencies")
        or article.get("source_currencies")
        or article.get("currencies")
        or []
    )
    if type(values) is not list:
        return []
    result: list[str] = []
    for value in values:
        if type(value) is not str or value not in CURRENCY_SET:
            continue
        result.append(value)
    return sorted(set(result))


def _content_sha256(article: Mapping[str, Any]) -> str:
    for field in (
        "material_content_sha256",
        "detail_content_sha256",
        "raw_payload_hash",
        "payload_sha256",
    ):
        value = article.get(field)
        if value is not None and value != "":
            return _sha256(value, path=field)
    return ""


def _monetary_impulse(article: Mapping[str, Any]) -> tuple[str, float]:
    value = article.get("monetary_impulse")
    if value is None or value == "":
        impulse = 0.0
    else:
        impulse = _number(value, path="monetary_impulse", minimum=-1.0, maximum=1.0)
    state = "HAWKISH" if impulse > 0.05 else "DOVISH" if impulse < -0.05 else "NEUTRAL"
    return state, round(impulse, 6)


def _policy_document(
    article: Mapping[str, Any],
    *,
    currency: str,
    known: dt.datetime,
    scheduled: dt.datetime | None,
    grade: str,
) -> dict[str, Any]:
    state, impulse = _monetary_impulse(article)
    source_id = _identifier(article.get("source_id"), path="source_id")
    event_id = _identifier(article.get("event_id"), path="event_id")
    result = {
        "currency": currency,
        "grade": grade,
        "state": state,
        "monetary_impulse": impulse,
        "known_utc": _iso(known),
        "published_utc": (
            _iso(_exact_aware_iso(article.get("published_utc"), path="published_utc"))
            if article.get("published_utc") is not None
            and article.get("published_utc") != ""
            else ""
        ),
        "scheduled_utc": _iso(scheduled) if scheduled is not None else "",
        "source_id": source_id,
        "event_id": event_id,
        "headline": _display_text(article.get("headline"), limit=500),
        "summary": _display_text(article.get("summary"), limit=2_000),
        "policy_document_type": _optional_normalized_label(
            article.get("policy_document_type"), path="policy_document_type"
        ),
        "content_sha256": _content_sha256(article),
        "observation_clock_trusted": bool(
            article.get("observation_clock_trusted") is True
        ),
        "observation_time_contract_id": (
            _optional_identifier(
                article.get("observation_time_contract_id"),
                path="observation_time_contract_id",
            )
        ),
        "collector_contract_id": (
            _optional_identifier(
                article.get("collector_contract_id"), path="collector_contract_id"
            )
        ),
        "collector_cohort_id": (
            _optional_identifier(
                article.get("collector_cohort_id"), path="collector_cohort_id"
            )
        ),
        "policy_context_contract_id": POLICY_CONTEXT_CONTRACT_ID,
        "research_only": True,
        "execution_eligible": False,
    }
    return _exact_fields(result, POLICY_DOCUMENT_FIELDS, path="policy_document_output")


def build_persistent_policy_context_v3(
    articles: Sequence[Mapping[str, Any]],
    *,
    as_of: dt.datetime,
    sources: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build proof/legacy policy context with explicit 21-currency coverage."""

    cutoff = _as_of(as_of)
    if type(articles) not in {list, tuple}:
        raise ContractViolation("articles_must_be_exact_list_or_tuple")
    source_map = (
        _strict_plain_json(sources, path="sources")
        if type(sources) is dict
        else {}
    )

    proof_documents: list[dict[str, Any]] = []
    legacy_documents: list[dict[str, Any]] = []
    rejections: list[dict[str, Any]] = []
    observed_by_currency: Counter[str] = Counter()
    rejected_by_currency: dict[str, Counter[str]] = {
        currency: Counter() for currency in CANONICAL_CURRENCIES
    }

    for raw in articles:
        try:
            article = _strict_plain_json(raw)
        except ContractViolation:
            rejections.append({"event_id": "", "source_id": "", "currencies": [], "reason": "non_plain_article"})
            continue
        if type(article) is not dict:
            rejections.append({"event_id": "", "source_id": "", "currencies": [], "reason": "non_plain_article"})
            continue
        if article.get("source_verified") is not True or article.get("official_policy_release") is not True:
            continue
        currencies = _authority_currencies(article, source_map)
        for currency in currencies:
            observed_by_currency[currency] += 1
        if not currencies:
            rejections.append(
                {
                    "event_id": "",
                    "source_id": "",
                    "currencies": [],
                    "reason": "no_supported_authority_currency",
                }
            )
            continue

        reason = ""
        try:
            event_id = _identifier(article.get("event_id"), path="event_id")
            source_id = _identifier(article.get("source_id"), path="source_id")
        except ContractViolation:
            event_id = ""
            source_id = ""
            reason = "invalid_policy_identifier"

        known: dt.datetime | None = None
        if not reason:
            raw_known = (
                article.get("causal_known_utc")
                or article.get("first_seen_utc")
                or article.get("published_utc")
            )
            try:
                known = _exact_aware_iso(raw_known, path="known_utc")
            except ContractViolation:
                reason = "missing_or_malformed_known_time"
            if known is not None and known > cutoff:
                reason = "not_known_by_cutoff"

        scheduled: dt.datetime | None = None
        raw_schedule = article.get("scheduled_utc")
        if not reason and raw_schedule is not None and raw_schedule != "":
            try:
                scheduled = _exact_aware_iso(raw_schedule, path="scheduled_utc")
            except ContractViolation:
                reason = "malformed_nonempty_schedule"

        structured_value = article.get("structured_event")
        if (
            not reason
            and structured_value is not None
            and type(structured_value) is not bool
        ):
            reason = "malformed_structured_event_flag"
        structured = structured_value is True
        if not reason and structured and scheduled is not None and scheduled > cutoff:
            reason = "future_scheduled_release"
        if not reason and structured:
            actual = article.get("actual_value")
            if actual is None:
                reason = "structured_release_not_actual"
            else:
                try:
                    _number(actual, path="actual_value")
                except ContractViolation:
                    reason = "invalid_structured_actual_value"

        if not reason:
            reason = _provider_error_reason(article)
        if not reason:
            try:
                _monetary_impulse(article)
                _content_sha256(article)
                for field in (
                    "observation_time_contract_id",
                    "collector_contract_id",
                    "collector_cohort_id",
                ):
                    _optional_identifier(article.get(field), path=field)
                if (
                    article.get("published_utc") is not None
                    and article.get("published_utc") != ""
                ):
                    _exact_aware_iso(article.get("published_utc"), path="published_utc")
            except ContractViolation as exc:
                text = str(exc)
                if "monetary_impulse" in text:
                    reason = "invalid_monetary_impulse"
                elif "published_utc" in text:
                    reason = "malformed_published_time"
                elif any(
                    field in text
                    for field in (
                        "observation_time_contract_id",
                        "collector_contract_id",
                        "collector_cohort_id",
                    )
                ):
                    reason = "invalid_provenance_identifier"
                else:
                    reason = "invalid_content_digest"

        if reason:
            rejection = {
                "event_id": event_id,
                "source_id": source_id,
                "currencies": currencies,
                "reason": reason,
            }
            _exact_fields(rejection, REJECTION_FIELDS, path="rejection_output")
            rejections.append(rejection)
            for currency in currencies:
                rejected_by_currency[currency][reason] += 1
            continue

        assert known is not None
        assert source_id and event_id
        proof_grade = _collector_provenance(article)
        grade = "proof_grade_current" if proof_grade else "legacy_diagnostic_only"
        for currency in currencies:
            document = _policy_document(
                article,
                currency=currency,
                known=known,
                scheduled=scheduled,
                grade=grade,
            )
            if proof_grade:
                proof_documents.append(document)
            else:
                legacy_documents.append(document)

    order = lambda row: (row["known_utc"], row["event_id"], row["currency"])
    proof_documents.sort(key=order)
    legacy_documents.sort(key=order)
    proof_latest = {row["currency"]: row for row in proof_documents}
    legacy_latest = {row["currency"]: row for row in legacy_documents}

    currency_rows: dict[str, dict[str, Any]] = {}
    missing_reason_counts: Counter[str] = Counter()
    for currency in CANONICAL_CURRENCIES:
        proof = proof_latest.get(currency)
        legacy = legacy_latest.get(currency)
        if proof is not None:
            status = "proof_grade_current"
            missing_reason = ""
        elif legacy is not None:
            status = "missing"
            missing_reason = "current_v40_provenance_unavailable"
        elif observed_by_currency[currency] == 0:
            status = "missing"
            missing_reason = "no_official_policy_candidate"
        elif rejected_by_currency[currency]:
            status = "missing"
            missing_reason = "all_candidates_rejected:" + ",".join(
                sorted(rejected_by_currency[currency])
            )
        else:
            status = "missing"
            missing_reason = "no_proof_grade_actual_policy_document"
        if missing_reason:
            missing_reason_counts[missing_reason] += 1
        row = {
            "currency": currency,
            "status": status,
            "missing_reason": missing_reason,
            "proof_grade_context": proof,
            "legacy_diagnostic_context": legacy,
            "rejection_counts": dict(sorted(rejected_by_currency[currency].items())),
            "research_only": True,
            "execution_eligible": False,
        }
        currency_rows[currency] = _exact_fields(
            row, CURRENCY_ROW_FIELDS, path=f"currency_output:{currency}"
        )

    coverage = {
        "expected_currency_count": len(CANONICAL_CURRENCIES),
        "reported_currency_count": len(currency_rows),
        "proof_grade_currency_count": len(proof_latest),
        "missing_currency_count": len(CANONICAL_CURRENCIES) - len(proof_latest),
        "legacy_diagnostic_currency_count": len(legacy_latest),
        "proof_grade_document_count": len(proof_documents),
        "legacy_diagnostic_document_count": len(legacy_documents),
        "rejected_document_count": len(rejections),
        "missing_reason_counts": dict(sorted(missing_reason_counts.items())),
    }
    _exact_fields(coverage, COVERAGE_FIELDS, path="coverage_output")
    output = {
        "schema_version": "persistent_policy_context_v3",
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
            key=lambda row: (row["reason"], row["event_id"], row["source_id"]),
        ),
    }
    for rejection in output["rejections"]:
        _exact_fields(rejection, REJECTION_FIELDS, path="rejection_output")
    return _exact_fields(output, POLICY_OUTPUT_FIELDS, path="policy_output")


def _normalize_source_evidence(
    evidence: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], str]:
    if type(evidence) not in {list, tuple} or not evidence:
        raise ContractViolation("source_evidence_required")
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    event_hashes: dict[str, str] = {}
    for index, raw in enumerate(evidence):
        value = _strict_plain_json(raw, path=f"evidence[{index}]")
        value = _exact_fields(value, EVIDENCE_FIELDS, path=f"evidence[{index}]")
        event_id = _identifier(value["event_id"], path=f"evidence[{index}].event_id")
        payload_hash = _sha256(
            value["payload_sha256"], path=f"evidence[{index}].payload_sha256"
        )
        if not _collector_provenance(value):
            raise ContractViolation("source_evidence_not_exact_v40_v4")
        prior = event_hashes.get(event_id)
        if prior is not None and prior != payload_hash:
            raise ContractViolation("conflicting_payload_hash_for_event_id")
        event_hashes[event_id] = payload_hash
        normalized = {
            "event_id": event_id,
            "payload_sha256": payload_hash,
            "observation_clock_trusted": True,
            "observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
            "collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
            "collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
        }
        unique[(event_id, payload_hash)] = normalized
    ordered = [unique[key] for key in sorted(unique)]
    digest_payload = {
        "episode_semantics_id": EPISODE_SEMANTICS_ID,
        "source_evidence": ordered,
    }
    return ordered, _sha256_json(digest_payload)


def build_currency_factor_v3(
    *,
    episode_id: str,
    currency: str,
    score: float,
    confidence: float,
    horizon_min: int,
    source_evidence: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build one closed-schema currency factor with a canonical evidence digest."""

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
    evidence, digest = _normalize_source_evidence(source_evidence)
    result = {
        "episode_id": episode,
        "episode_semantics_id": EPISODE_SEMANTICS_ID,
        "episode_semantics": EPISODE_SEMANTICS,
        "currency": currency,
        "score": numeric_score,
        "confidence": numeric_confidence,
        "horizon_min": horizon_min,
        "event_ids": [item["event_id"] for item in evidence],
        "payload_sha256s": [item["payload_sha256"] for item in evidence],
        "source_evidence_count": len(evidence),
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


def _arm_horizon_valid(arm: str, factor_horizon: int, horizon: int) -> bool:
    match = re.search(r"_h(5|15|30)\Z", arm)
    if match is not None:
        return horizon == int(match.group(1))
    return horizon == factor_horizon


def _expected_pair_direction(currency: str, instrument: str, score: float) -> str:
    base, quote = instrument.split("_")
    if currency not in {base, quote}:
        raise ContractViolation("currency_not_in_instrument")
    currency_up = score > 0.0
    long_pair = (currency == base and currency_up) or (currency == quote and not currency_up)
    return "long" if long_pair else "short"


def _verify_factor_evidence(factor: Mapping[str, Any]) -> str:
    event_ids = factor.get("event_ids")
    payload_hashes = factor.get("payload_sha256s")
    count = factor.get("source_evidence_count")
    if (
        type(event_ids) is not list
        or type(payload_hashes) is not list
        or type(count) is not int
        or isinstance(count, bool)
        or count <= 0
        or len(event_ids) != count
        or len(payload_hashes) != count
    ):
        raise ContractViolation("factor_source_evidence_shape_invalid")
    evidence = []
    for event_id, payload_hash in zip(event_ids, payload_hashes):
        evidence.append(
            {
                "event_id": event_id,
                "payload_sha256": payload_hash,
                "observation_clock_trusted": True,
                "observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
                "collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
                "collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
            }
        )
    normalized, digest = _normalize_source_evidence(evidence)
    if [item["event_id"] for item in normalized] != event_ids:
        raise ContractViolation("factor_source_evidence_not_canonical")
    if [item["payload_sha256"] for item in normalized] != payload_hashes:
        raise ContractViolation("factor_source_evidence_not_canonical")
    if factor.get("source_evidence_digest_sha256") != digest:
        raise ContractViolation("factor_source_evidence_digest_mismatch")
    return digest


def bind_watch_candidate_v3(
    factor: Mapping[str, Any],
    *,
    instrument: str,
    arm: str,
    direction: str,
    horizon_min: int,
) -> dict[str, Any]:
    """Bind a closed-schema news watch identity to exact source evidence."""

    factor_value = _strict_plain_json(factor, path="factor")
    factor_value = _exact_fields(factor_value, FACTOR_OUTPUT_FIELDS, path="factor")
    if factor_value.get("research_only") is not True or factor_value.get("execution_eligible") is not False:
        raise ContractViolation("factor_not_inert")
    if factor_value.get("episode_semantics_id") != EPISODE_SEMANTICS_ID:
        raise ContractViolation("episode_semantics_mismatch")
    if factor_value.get("episode_semantics") != EPISODE_SEMANTICS:
        raise ContractViolation("episode_semantics_mismatch")
    if factor_value.get("watch_provenance_contract_id") != WATCH_PROVENANCE_CONTRACT_ID:
        raise ContractViolation("watch_contract_mismatch")
    if factor_value.get("observation_clock_trusted") is not True:
        raise ContractViolation("factor_clock_not_trusted")
    if factor_value.get("observation_time_contract_id") != REQUIRED_OBSERVATION_TIME_CONTRACT_ID:
        raise ContractViolation("factor_clock_contract_mismatch")
    if factor_value.get("collector_contract_id") != REQUIRED_COLLECTOR_CONTRACT_ID:
        raise ContractViolation("factor_collector_contract_mismatch")
    if factor_value.get("collector_cohort_id") != REQUIRED_COLLECTOR_COHORT_ID:
        raise ContractViolation("factor_collector_cohort_mismatch")
    digest = _verify_factor_evidence(factor_value)
    episode = _identifier(factor_value.get("episode_id"), path="factor.episode_id")
    currency = factor_value.get("currency")
    if type(currency) is not str or currency not in CURRENCY_SET:
        raise ContractViolation("currency_not_canonical")
    if type(instrument) is not str or instrument not in INSTRUMENT_SET:
        raise ContractViolation("instrument_not_canonical")
    if type(arm) is not str or arm not in ARM_ALLOWLIST:
        raise ContractViolation("arm_not_allowlisted")
    if type(direction) is not str or direction not in DIRECTION_ALLOWLIST:
        raise ContractViolation("direction_not_allowlisted")
    if type(horizon_min) is not int or isinstance(horizon_min, bool) or horizon_min <= 0:
        raise ContractViolation("horizon_min_invalid")
    factor_horizon = factor_value.get("horizon_min")
    if (
        type(factor_horizon) is not int
        or isinstance(factor_horizon, bool)
        or not 1 <= factor_horizon <= 1_440
        or not _arm_horizon_valid(arm, factor_horizon, horizon_min)
    ):
        raise ContractViolation("arm_horizon_mismatch")
    _number(factor_value.get("confidence"), path="factor.confidence", minimum=0.0, maximum=1.0)
    score = _number(
        factor_value.get("score"), path="factor.score", minimum=-1.0, maximum=1.0
    )
    if score == 0.0:
        raise ContractViolation("score_must_be_directional")
    expected_direction = _expected_pair_direction(
        currency, instrument, score
    )
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
        "factor_confidence": factor_value["confidence"],
        "observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
    }
    entry_id = hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()[:32]
    result = {
        "entry_id": entry_id,
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
        "source_evidence_count": factor_value["source_evidence_count"],
        "factor_score": score,
        "factor_confidence": factor_value["confidence"],
        "event_ids": list(factor_value["event_ids"]),
        "payload_sha256s": list(factor_value["payload_sha256s"]),
        "observation_clock_trusted": True,
        "observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
        "research_only": True,
        "execution_eligible": False,
    }
    return _exact_fields(result, WATCH_OUTPUT_FIELDS, path="watch_output")


def assert_closed_output_schemas(value: Mapping[str, Any], *, kind: str) -> None:
    """Public verifier used by review/tests; raises on any output schema drift."""

    if kind == "factor":
        _exact_fields(value, FACTOR_OUTPUT_FIELDS, path="factor_output")
        return
    if kind == "watch":
        _exact_fields(value, WATCH_OUTPUT_FIELDS, path="watch_output")
        return
    if kind == "policy":
        output = _exact_fields(value, POLICY_OUTPUT_FIELDS, path="policy_output")
        _exact_fields(output["coverage"], COVERAGE_FIELDS, path="coverage_output")
        for currency, row in output["currencies"].items():
            if currency not in CURRENCY_SET:
                raise ContractViolation("policy_output_noncanonical_currency")
            _exact_fields(row, CURRENCY_ROW_FIELDS, path=f"currency_output:{currency}")
            for field in ("proof_grade_context", "legacy_diagnostic_context"):
                if row[field] is not None:
                    _exact_fields(
                        row[field], POLICY_DOCUMENT_FIELDS, path=f"{field}:{currency}"
                    )
        for rejection in output["rejections"]:
            _exact_fields(rejection, REJECTION_FIELDS, path="rejection_output")
        return
    raise ContractViolation("unknown_output_schema_kind")
