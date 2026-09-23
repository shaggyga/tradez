"""Fail-closed research contracts for policy context and news-watch lineage.

This module is deliberately inert.  It does not read or write operational
state, start a worker, register a model, promote evidence, authorize an order,
or alter Practice 007.  It is a versioned successor candidate for two derived
research artifacts:

* persistent policy context must distinguish a current policy document from a
  future release-calendar row or a provider error page; and
* a news-derived watch row must expose and hash the exact collector/clock
  provenance that was already required at input.

Legacy documents remain visible as diagnostics, but never become proof-grade
context under this contract.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from collections import Counter
from typing import Any, Mapping, Sequence


UTC = dt.timezone.utc

POLICY_CONTEXT_CONTRACT_ID = (
    "persistent_policy_context_v2_v40_clock_v4_20260817"
)
WATCH_PROVENANCE_CONTRACT_ID = (
    "news_technical_watchlist_v38_provenance_candidate_20260817"
)
WATCH_CANDIDATE_COHORT_ID = WATCH_PROVENANCE_CONTRACT_ID

REQUIRED_OBSERVATION_TIME_CONTRACT_ID = (
    "observation_time_v4_fail_closed_consistent_integrity_sources_20260817"
)
REQUIRED_COLLECTOR_CONTRACT_ID = (
    "local_news_incremental_source_commit_v40_20260817"
)
REQUIRED_COLLECTOR_COHORT_ID = REQUIRED_COLLECTOR_CONTRACT_ID

ALL_CURRENCIES = (
    "AUD",
    "CAD",
    "CHF",
    "CNH",
    "CZK",
    "DKK",
    "EUR",
    "GBP",
    "HKD",
    "HUF",
    "JPY",
    "MXN",
    "NOK",
    "NZD",
    "PLN",
    "SEK",
    "SGD",
    "THB",
    "TRY",
    "USD",
    "ZAR",
)

_PROVIDER_ERROR_PATTERNS = (
    re.compile(r"\bservice is currently unavailable\b", re.IGNORECASE),
    re.compile(r"\btemporarily unavailable\b", re.IGNORECASE),
    re.compile(r"\bscheduled maintenance\b", re.IGNORECASE),
    re.compile(r"\bunder maintenance\b", re.IGNORECASE),
    re.compile(r"\baccess denied\b", re.IGNORECASE),
    re.compile(r"\benable javascript(?: and cookies)? to continue\b", re.IGNORECASE),
    re.compile(r"\bpage (?:was )?not found\b", re.IGNORECASE),
    re.compile(r"\b(?:http )?error\s+(?:403|404|429|500|502|503|504)\b", re.IGNORECASE),
    re.compile(r"\bsomething went wrong\b", re.IGNORECASE),
)


class ContractViolation(ValueError):
    """Raised when a candidate cannot satisfy its frozen research contract."""


def _exact_text(value: Any) -> str:
    return value.strip() if type(value) is str else ""


def _exact_bool(value: Any) -> bool:
    return value is True


def _parse_utc(value: Any) -> dt.datetime | None:
    """Parse only exact ISO strings with an explicit UTC offset."""

    if type(value) is not str:
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != dt.timedelta(0):
        return None
    return parsed.astimezone(UTC)


def _require_as_of(value: Any) -> dt.datetime:
    if type(value) is not dt.datetime or value.tzinfo is not UTC:
        raise ContractViolation("as_of_must_be_exact_builtin_utc_datetime")
    return value


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _plain_json(value: Any, *, path: str = "$") -> Any:
    """Freeze exact JSON values and reject callback-capable subclasses."""

    if value is None or type(value) in {str, int, float, bool}:
        return value
    if type(value) is list:
        return [_plain_json(item, path=f"{path}[]") for item in value]
    if type(value) is dict:
        output: dict[str, Any] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ContractViolation(f"non_string_key:{path}")
            output[key] = _plain_json(item, path=f"{path}.{key}")
        return output
    raise ContractViolation(f"non_plain_json_value:{path}:{type(value).__name__}")


def exact_collector_provenance(value: Any) -> bool:
    """Return true only for the frozen V40/V4 prospective source contract."""

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


def provider_error_reason(article: Mapping[str, Any]) -> str:
    """Classify common provider error/maintenance bodies, not policy text."""

    text = " ".join(
        _exact_text(article.get(field))
        for field in (
            "headline",
            "summary",
            "detail_text",
            "body",
            "content",
        )
    )
    for pattern in _PROVIDER_ERROR_PATTERNS:
        if pattern.search(text):
            return "provider_error_or_maintenance_content"
    return ""


def _authority_currencies(
    article: Mapping[str, Any],
    sources: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    source_id = _exact_text(article.get("source_id"))
    configured = sources.get(source_id) if type(sources) is dict else None
    configured = configured if type(configured) is dict else {}
    raw = (
        configured.get("currencies")
        or article.get("source_currencies")
        or article.get("currencies")
        or []
    )
    if type(raw) is not list:
        return []
    return sorted(
        {
            value.upper()
            for value in raw
            if type(value) is str and value.upper() in ALL_CURRENCIES
        }
    )


def _policy_state(article: Mapping[str, Any]) -> tuple[str, float]:
    value = article.get("monetary_impulse")
    impulse = float(value) if type(value) in {int, float} else 0.0
    impulse = max(-1.0, min(1.0, impulse))
    state = "HAWKISH" if impulse > 0.05 else "DOVISH" if impulse < -0.05 else "NEUTRAL"
    return state, round(impulse, 6)


def _document_payload(
    article: Mapping[str, Any],
    *,
    currency: str,
    known: dt.datetime,
) -> dict[str, Any]:
    state, impulse = _policy_state(article)
    return {
        "currency": currency,
        "state": state,
        "monetary_impulse": impulse,
        "known_utc": _iso(known),
        "published_utc": _exact_text(article.get("published_utc")),
        "scheduled_utc": _exact_text(article.get("scheduled_utc")),
        "source_id": _exact_text(article.get("source_id")),
        "event_id": _exact_text(article.get("event_id")),
        "headline": _exact_text(article.get("headline")),
        "summary": _exact_text(article.get("summary"))[:2_000],
        "source_url": _exact_text(article.get("source_url")),
        "policy_document_type": _exact_text(article.get("policy_document_type")),
        "detail_enriched": _exact_bool(article.get("detail_enriched")),
        "detail_content_sha256": _exact_text(article.get("detail_content_sha256")),
        "observation_clock_trusted": True,
        "observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
        "policy_context_contract_id": POLICY_CONTEXT_CONTRACT_ID,
        "validity": "until_superseded_by_newer_proof_grade_policy_document",
        "research_only": True,
        "execution_eligible": False,
    }


def build_persistent_policy_context_v2(
    articles: Sequence[Mapping[str, Any]],
    *,
    as_of: dt.datetime,
    sources: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build 21-currency proof context while segregating legacy diagnostics."""

    cutoff = _require_as_of(as_of)
    if type(articles) not in {list, tuple}:
        raise ContractViolation("articles_must_be_list_or_tuple")
    source_map: dict[str, Mapping[str, Any]] = (
        sources if type(sources) is dict else {}
    )

    proof_documents: list[dict[str, Any]] = []
    legacy_documents: list[dict[str, Any]] = []
    rejections: list[dict[str, Any]] = []
    observed_by_currency: Counter[str] = Counter()
    rejected_by_currency: dict[str, Counter[str]] = {
        currency: Counter() for currency in ALL_CURRENCIES
    }

    for raw_article in articles:
        if type(raw_article) is not dict:
            rejections.append({"event_id": "", "reason": "non_plain_article"})
            continue
        try:
            article = _plain_json(raw_article)
        except ContractViolation:
            rejections.append({"event_id": "", "reason": "non_plain_article"})
            continue
        if article.get("source_verified") is not True:
            continue
        if article.get("official_policy_release") is not True:
            continue

        currencies = _authority_currencies(article, source_map)
        for currency in currencies:
            observed_by_currency[currency] += 1
        if not currencies:
            rejections.append(
                {
                    "event_id": _exact_text(article.get("event_id")),
                    "reason": "no_supported_authority_currency",
                }
            )
            continue

        known = _parse_utc(
            article.get("causal_known_utc")
            or article.get("first_seen_utc")
            or article.get("published_utc")
        )
        if known is None:
            reason = "missing_or_untrusted_known_time"
        elif known > cutoff:
            reason = "not_known_by_cutoff"
        else:
            reason = ""

        scheduled = _parse_utc(article.get("scheduled_utc"))
        if (
            not reason
            and article.get("structured_event") is True
            and scheduled is not None
            and scheduled > cutoff
        ):
            reason = "future_scheduled_release"
        if not reason:
            reason = provider_error_reason(article)

        if reason:
            rejection = {
                "event_id": _exact_text(article.get("event_id")),
                "source_id": _exact_text(article.get("source_id")),
                "currencies": currencies,
                "reason": reason,
            }
            rejections.append(rejection)
            for currency in currencies:
                rejected_by_currency[currency][reason] += 1
            continue

        assert known is not None
        for currency in currencies:
            document = _document_payload(article, currency=currency, known=known)
            if exact_collector_provenance(article):
                proof_documents.append(document)
            else:
                legacy_documents.append(
                    {
                        **document,
                        "observation_clock_trusted": bool(
                            article.get("observation_clock_trusted") is True
                        ),
                        "observation_time_contract_id": _exact_text(
                            article.get("observation_time_contract_id")
                        ),
                        "collector_contract_id": _exact_text(
                            article.get("collector_contract_id")
                        ),
                        "collector_cohort_id": _exact_text(
                            article.get("collector_cohort_id")
                        ),
                        "policy_context_contract_id": "legacy_diagnostic_only",
                        "validity": "legacy_diagnostic_not_proof_grade",
                        "proof_grade": False,
                        "execution_eligible": False,
                    }
                )

    order = lambda row: (
        row["known_utc"],
        row["event_id"],
        row["currency"],
    )
    proof_documents.sort(key=order)
    legacy_documents.sort(key=order)
    proof_latest = {row["currency"]: row for row in proof_documents}
    legacy_latest = {row["currency"]: row for row in legacy_documents}

    currency_rows: dict[str, dict[str, Any]] = {}
    missing_reason_counts: Counter[str] = Counter()
    for currency in ALL_CURRENCIES:
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
            reasons = sorted(rejected_by_currency[currency])
            missing_reason = "all_candidates_rejected:" + ",".join(reasons)
        else:
            status = "missing"
            missing_reason = "no_proof_grade_policy_document"
        if missing_reason:
            missing_reason_counts[missing_reason] += 1
        currency_rows[currency] = {
            "currency": currency,
            "status": status,
            "missing_reason": missing_reason,
            "proof_grade_context": proof,
            "legacy_diagnostic_context": legacy,
            "rejection_counts": dict(sorted(rejected_by_currency[currency].items())),
            "research_only": True,
            "execution_eligible": False,
        }

    proof_count = len(proof_latest)
    legacy_count = len(legacy_latest)
    return {
        "schema_version": "persistent_policy_context_v2",
        "contract_id": POLICY_CONTEXT_CONTRACT_ID,
        "generated_utc": _iso(cutoff),
        "required_observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "required_collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
        "required_collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
        "research_only": True,
        "execution_eligible": False,
        "can_modify_practice_execution": False,
        "can_authorize": False,
        "activation_state": "inert_candidate",
        "coverage": {
            "expected_currency_count": len(ALL_CURRENCIES),
            "reported_currency_count": len(currency_rows),
            "proof_grade_currency_count": proof_count,
            "missing_currency_count": len(ALL_CURRENCIES) - proof_count,
            "legacy_diagnostic_currency_count": legacy_count,
            "proof_grade_document_count": len(proof_documents),
            "legacy_diagnostic_document_count": len(legacy_documents),
            "rejected_document_count": len(rejections),
            "missing_reason_counts": dict(sorted(missing_reason_counts.items())),
        },
        "currencies": currency_rows,
        "proof_grade_documents": proof_documents,
        "legacy_diagnostics": legacy_documents,
        "rejections": sorted(
            rejections,
            key=lambda row: (
                str(row.get("reason") or ""),
                str(row.get("event_id") or ""),
            ),
        ),
    }


def collector_lineage_fields(source_payload: Mapping[str, Any]) -> dict[str, Any]:
    """Expose exact source lineage after the existing V40 gate passes."""

    if type(source_payload) is not dict or not exact_collector_provenance(source_payload):
        raise ContractViolation("news_source_not_exact_v40_v4_provenance")
    return {
        "news_observation_clock_trusted": True,
        "news_observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "news_collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
        "news_collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
        "news_collector_provenance_bound": True,
        "news_watch_provenance_contract_id": WATCH_PROVENANCE_CONTRACT_ID,
    }


def bind_currency_factor_lineage(
    factor: Mapping[str, Any],
    source_payloads: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Bind all aggregated factor inputs to one exact collector lineage."""

    if type(factor) is not dict:
        raise ContractViolation("factor_must_be_plain_dict")
    if type(source_payloads) not in {list, tuple} or not source_payloads:
        raise ContractViolation("factor_requires_source_payload")
    frozen_factor = _plain_json(factor)
    for payload in source_payloads:
        if type(payload) is not dict:
            raise ContractViolation("source_payload_must_be_plain_dict")
        collector_lineage_fields(payload)
    return {
        **frozen_factor,
        **collector_lineage_fields(source_payloads[0]),
        "news_collector_source_payload_count": len(source_payloads),
        "research_only": True,
        "execution_eligible": False,
    }


def _required_watch_lineage(row: Mapping[str, Any]) -> dict[str, Any]:
    expected = {
        "news_observation_clock_trusted": True,
        "news_observation_time_contract_id": REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "news_collector_contract_id": REQUIRED_COLLECTOR_CONTRACT_ID,
        "news_collector_cohort_id": REQUIRED_COLLECTOR_COHORT_ID,
        "news_collector_provenance_bound": True,
        "news_watch_provenance_contract_id": WATCH_PROVENANCE_CONTRACT_ID,
    }
    for key, value in expected.items():
        if type(value) is str:
            if type(row.get(key)) is not str or row.get(key) != value:
                raise ContractViolation(f"watch_row_lineage_mismatch:{key}")
        elif row.get(key) is not value:
            raise ContractViolation(f"watch_row_lineage_mismatch:{key}")
    return expected


def watch_candidate_identifier(
    row: Mapping[str, Any],
    *,
    cohort_id: str = WATCH_CANDIDATE_COHORT_ID,
) -> str:
    """Hash watch thesis identity and exact collector/clock lineage together."""

    if type(row) is not dict or type(cohort_id) is not str:
        raise ContractViolation("watch_identity_requires_plain_values")
    lineage = _required_watch_lineage(row)
    horizon = row.get("horizon_min")
    if type(horizon) is not int or isinstance(horizon, bool) or horizon <= 0:
        raise ContractViolation("watch_horizon_must_be_positive_exact_int")
    identity = {
        "watch_cohort_id": cohort_id,
        "episode_id": _exact_text(row.get("episode_id")),
        "currency": _exact_text(row.get("currency")),
        "instrument": _exact_text(row.get("instrument")),
        "arm": _exact_text(row.get("arm")),
        "direction": _exact_text(row.get("direction")),
        "horizon_min": horizon,
        **lineage,
    }
    required = ("episode_id", "currency", "instrument", "arm", "direction")
    if any(not identity[key] for key in required):
        raise ContractViolation("watch_identity_field_missing")
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def bind_watch_candidate(
    row: Mapping[str, Any],
    *,
    cohort_id: str = WATCH_CANDIDATE_COHORT_ID,
) -> dict[str, Any]:
    """Return an inert watch payload whose ID commits to source provenance."""

    if type(row) is not dict:
        raise ContractViolation("watch_row_must_be_plain_dict")
    payload = _plain_json(row)
    lineage = _required_watch_lineage(payload)
    payload.update(lineage)
    payload.update(
        {
            "entry_id": watch_candidate_identifier(payload, cohort_id=cohort_id),
            "cohort_id": cohort_id,
            "research_only": True,
            "execution_eligible": False,
            "can_promote": False,
            "can_authorize": False,
            "can_place_orders": False,
            "activation_state": "inert_candidate",
        }
    )
    return payload
