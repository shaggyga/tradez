"""Late-observed Thailand June-2026 CPI briefing/upload diagnostic.

This module reopens two exact official source payloads retained on 17 August
2026.  The Ministry of Commerce page reports a 10:30 Asia/Bangkok *briefing*
start.  It does not prove the earliest time at which the CPI facts were public.
The TPSO PDF has a later server Last-Modified value, which is upload/modify
metadata rather than proof of public availability.  Exact facts therefore use
the later, causally supportable first-seen clock.

The cohort is deliberately disabled, unregistered, archive-only, shadow-only,
and no-trade.  It cannot supply consensus, surprise, direction, proof,
promotion, authorization, or execution.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import html as html_lib
import io
import json
import math
import os
import re
import stat
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse
from zoneinfo import ZoneInfo


SCHEMA_VERSION = 1
SOURCE_ID = "thailand_cpi_june_2026_briefing_upload_v1"
CONTRACT_ID = "thailand_cpi_june_2026_briefing_upload_v1_20260817"
COHORT_ID = "thailand_cpi_june_2026_briefing_upload_v1_20260817"
TIMEZONE_NAME = "Asia/Bangkok"
SUPPORTED_EXECUTION_DECISION = "no_trade"
COLLECTION_CLASS = "historical_availability_counterfactual_archive_only"

BRIEFING_URL = (
    "https://sisaket.moc.go.th/th/content/category/detail/id/161/iid/173746"
)
PDF_URL = (
    "https://uploads.tpso.go.th/editor/pdf/"
    "1783311439_fe22a795f58e35ce0374.pdf"
)
BRIEFING_SOURCE_REPORTED_LOCAL = "2026-07-06T10:30:00+07:00"
BRIEFING_EVENT_TIME_UTC = "2026-07-06T03:30:00Z"
PDF_SERVER_LAST_MODIFIED_UTC = "2026-07-06T04:17:19Z"
PAGE_FIRST_SEEN_UTC = "2026-08-17T09:50:17.5319969Z"
PDF_FIRST_SEEN_UTC = "2026-08-17T09:50:22.1793935Z"
EFFECTIVE_KNOWN_UTC = PDF_FIRST_SEEN_UTC

BRIEFING_TIME_SEMANTICS = (
    "briefing_start_not_earliest_public_data_availability"
)
PDF_LAST_MODIFIED_SEMANTICS = (
    "server_reported_pdf_modification_or_upload_metadata_not_"
    "public_availability_proof"
)
OPEN_DATA_CONTRACT_PRECISION = "date_only_no_exact_minute"

CONTRACT_MANIFEST_PATH = "config/thailand_cpi_june_2026_briefing_upload_v1.json"
CONTRACT_MANIFEST_BYTES = 3_726
CONTRACT_MANIFEST_SHA256 = (
    "9a3d6a89dfa3001a2ac327db031d8a07673e0701f624460349e38a590d77ec9c"
)
OBSERVATION_MANIFEST_PATH = (
    "data/oanda_training_manager/source_archives/"
    "thailand_cpi_june_2026_briefing_upload_v1/"
    "source_observation_manifest_20260817T095017Z.json"
)
OBSERVATION_MANIFEST_BYTES = 4_474
OBSERVATION_MANIFEST_SHA256 = (
    "ceb7b37b90d6273aeb019c740f6b945e17c20d2bd8e84f56fe082ef70a3181f6"
)
BRIEFING_ARCHIVE_PATH = (
    "data/oanda_training_manager/source_archives/"
    "thailand_cpi_june_2026_briefing_upload_v1/"
    "moc_sisaket_cpi_briefing_observed_20260817.html"
)
BRIEFING_ARCHIVE_BYTES = 166_730
BRIEFING_ARCHIVE_SHA256 = (
    "a9bb23d0445032aefcbd037bcf440a434bc9c26a61b51dcd7c130a9fb0ce2094"
)
PDF_ARCHIVE_PATH = (
    "data/oanda_training_manager/source_archives/"
    "thailand_cpi_june_2026_briefing_upload_v1/"
    "tpso_cpi_june_2026_observed_20260817.pdf"
)
PDF_ARCHIVE_BYTES = 414_864
PDF_ARCHIVE_SHA256 = (
    "014472adf420655f9f9374f857cccd8c5efd1f4da041e742e2ae7519ffc235df"
)

EXPECTED_FACTS: dict[str, Any] = {
    "headline_cpi_index": 102.85,
    "headline_month_over_month_pct": -0.34,
    "headline_year_over_year_pct": 2.42,
    "core_year_over_year_pct": 1.23,
}

EXPECTED_GUARDS: dict[str, Any] = {
    "research_only": True,
    "shadow_only": True,
    "enabled": False,
    "registered_with_live_collector": False,
    "runtime_supported": False,
    "prospective_clock": False,
    "prospective_observation": False,
    "exact_prospective_cpi_release_clock_closed": False,
    "direction": None,
    "consensus": None,
    "surprise": None,
    "proof_eligible": False,
    "confirmation_eligible": False,
    "promotion_eligible": False,
    "authorization_eligible": False,
    "execution_eligible": False,
    "can_place_orders": False,
    "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
}

UTC = dt.timezone.utc
_WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
_SHA256 = re.compile(r"[0-9a-f]{64}")


class ThailandCpiJuneBriefingUploadV1Error(RuntimeError):
    """Raised whenever the exact source or chronology contract fails."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_json(value).encode("utf-8"))


def _strict_json(raw: bytes, *, label: str) -> Any:
    def reject_constant(value: str) -> None:
        raise ThailandCpiJuneBriefingUploadV1Error(
            f"{label}_nonfinite_number:{value}"
        )

    def parse_float(value: str) -> float:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ThailandCpiJuneBriefingUploadV1Error(
                f"{label}_nonfinite_number:{value}"
            )
        return parsed

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in pairs:
            if key in output:
                raise ThailandCpiJuneBriefingUploadV1Error(
                    f"{label}_duplicate_key:{key}"
                )
            output[key] = value
        return output

    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
            parse_float=parse_float,
        )
    except ThailandCpiJuneBriefingUploadV1Error:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ThailandCpiJuneBriefingUploadV1Error(
            f"{label}_invalid_json"
        ) from exc


def _typed_equal(left: Any, right: Any) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, Mapping):
        return set(left) == set(right) and all(
            _typed_equal(left[key], right[key]) for key in left
        )
    if isinstance(left, list):
        return len(left) == len(right) and all(
            _typed_equal(a, b) for a, b in zip(left, right)
        )
    return left == right


def _closed_mapping(
    value: Any, *, label: str, fields: set[str] | frozenset[str]
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ThailandCpiJuneBriefingUploadV1Error(f"{label}_not_mapping")
    if set(value) != set(fields):
        raise ThailandCpiJuneBriefingUploadV1Error(
            f"{label}_schema_mismatch:missing={sorted(set(fields)-set(value))}:"
            f"extra={sorted(set(value)-set(fields))}"
        )
    return value


def _integer(value: Any, *, label: str) -> int:
    if type(value) is not int:
        raise ThailandCpiJuneBriefingUploadV1Error(f"{label}_not_exact_int")
    return value


def _parse_time(value: Any, *, label: str) -> dt.datetime:
    if type(value) is not str or not value:
        raise ThailandCpiJuneBriefingUploadV1Error(f"{label}_not_string")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ThailandCpiJuneBriefingUploadV1Error(
            f"{label}_invalid"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ThailandCpiJuneBriefingUploadV1Error(
            f"{label}_timezone_missing"
        )
    return parsed


def _iso_utc(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _official_url(
    value: Any, *, expected: str, domain: str, label: str
) -> str:
    if type(value) is not str or value != expected:
        raise ThailandCpiJuneBriefingUploadV1Error(f"{label}_url_mismatch")
    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or parsed.hostname != domain
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.fragment
    ):
        raise ThailandCpiJuneBriefingUploadV1Error(
            f"{label}_url_identity_invalid"
        )
    return value


def _workspace_path(relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "pinned_path_not_workspace_relative"
        )
    candidate = (_WORKSPACE_ROOT / relative).absolute()
    try:
        candidate.relative_to(_WORKSPACE_ROOT)
    except ValueError as exc:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "pinned_path_escaped_workspace"
        ) from exc
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
    """Reject path substitution, links, mutations, races, and byte drift."""

    requested = Path(path).absolute()
    expected = Path(expected_path).absolute()
    if os.path.normcase(str(requested)) != os.path.normcase(str(expected)):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "pinned_file_path_substitution"
        )
    if requested.is_symlink():
        raise ThailandCpiJuneBriefingUploadV1Error(
            "pinned_file_symlink_forbidden"
        )
    try:
        resolved = requested.resolve(strict=True)
        expected_resolved = expected.resolve(strict=True)
    except OSError as exc:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "pinned_file_missing"
        ) from exc
    if resolved != expected_resolved:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "pinned_file_resolved_path_mismatch"
        )
    before = os.lstat(requested)
    if not stat.S_ISREG(before.st_mode):
        raise ThailandCpiJuneBriefingUploadV1Error("pinned_file_not_regular")
    if before.st_nlink != 1:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "pinned_file_hardlink_forbidden"
        )
    if require_read_only:
        if os.name == "nt":
            readonly_flag = getattr(stat, "FILE_ATTRIBUTE_READONLY", 0x1)
            attributes = int(getattr(before, "st_file_attributes", 0))
            if attributes & readonly_flag == 0:
                raise ThailandCpiJuneBriefingUploadV1Error(
                    "pinned_archive_not_read_only"
                )
        elif before.st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH):
            raise ThailandCpiJuneBriefingUploadV1Error(
                "pinned_archive_not_read_only"
            )
    with requested.open("rb") as handle:
        opened_before = os.fstat(handle.fileno())
        if _file_identity(opened_before) != _file_identity(before):
            raise ThailandCpiJuneBriefingUploadV1Error(
                "pinned_file_swapped_before_open"
            )
        raw = handle.read()
        opened_after = os.fstat(handle.fileno())
    after = os.lstat(requested)
    if _file_identity(opened_before) != _file_identity(opened_after):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "pinned_file_changed_while_reading"
        )
    if _file_identity(opened_after) != _file_identity(after):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "pinned_file_path_swapped_after_read"
        )
    if len(raw) != expected_bytes:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "pinned_file_byte_count_mismatch"
        )
    if _SHA256.fullmatch(expected_sha256) is None:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "pinned_expected_sha256_invalid"
        )
    if _sha256_bytes(raw) != expected_sha256:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "pinned_file_sha256_mismatch"
        )
    return raw


_CONTRACT_FIELDS = frozenset(
    {
        "schema_version", "source_id", "source_contract_id", "source_cohort_id",
        "enabled", "registered_with_live_collector", "runtime_supported",
        "research_only", "shadow_only", "currency", "event_series_id",
        "reference_period", "trusted_domains", "briefing_page_url",
        "publication_pdf_url", "briefing_source_reported_local",
        "briefing_event_time_utc", "briefing_timezone",
        "briefing_time_semantics", "pdf_server_last_modified_utc",
        "pdf_last_modified_semantics", "page_first_seen_utc",
        "pdf_first_seen_utc", "effective_known_utc", "effective_known_rule",
        "open_data_contract_precision", "observation_manifest_path",
        "observation_manifest_bytes", "observation_manifest_sha256",
        "briefing_archive_path", "briefing_archive_bytes",
        "briefing_archive_sha256", "publication_archive_path",
        "publication_archive_bytes", "publication_archive_sha256",
        "historical_collection_class", "prospective_clock",
        "prospective_observation", "parsed_fact_scope", "hard_guards",
        "separation_contract",
    }
)


def load_contract_manifest(
    path: str | Path = CONTRACT_MANIFEST_PATH,
) -> dict[str, Any]:
    expected_path = _workspace_path(CONTRACT_MANIFEST_PATH)
    requested = Path(path) if Path(path).is_absolute() else _workspace_path(str(path))
    raw = _read_exact_pinned_file(
        requested,
        expected_path=expected_path,
        expected_bytes=CONTRACT_MANIFEST_BYTES,
        expected_sha256=CONTRACT_MANIFEST_SHA256,
        require_read_only=False,
    )
    row = _closed_mapping(
        _strict_json(raw, label="contract_manifest"),
        label="contract_manifest",
        fields=_CONTRACT_FIELDS,
    )
    expected_identity = {
        "schema_version": SCHEMA_VERSION,
        "source_id": SOURCE_ID,
        "source_contract_id": CONTRACT_ID,
        "source_cohort_id": COHORT_ID,
        "enabled": False,
        "registered_with_live_collector": False,
        "runtime_supported": False,
        "research_only": True,
        "shadow_only": True,
        "currency": "THB",
        "event_series_id": "thailand_consumer_price_index_headline",
        "reference_period": "2026-06",
        "trusted_domains": ["sisaket.moc.go.th", "uploads.tpso.go.th"],
        "briefing_page_url": BRIEFING_URL,
        "publication_pdf_url": PDF_URL,
        "briefing_source_reported_local": BRIEFING_SOURCE_REPORTED_LOCAL,
        "briefing_event_time_utc": BRIEFING_EVENT_TIME_UTC,
        "briefing_timezone": TIMEZONE_NAME,
        "briefing_time_semantics": BRIEFING_TIME_SEMANTICS,
        "pdf_server_last_modified_utc": PDF_SERVER_LAST_MODIFIED_UTC,
        "pdf_last_modified_semantics": PDF_LAST_MODIFIED_SEMANTICS,
        "page_first_seen_utc": PAGE_FIRST_SEEN_UTC,
        "pdf_first_seen_utc": PDF_FIRST_SEEN_UTC,
        "effective_known_utc": EFFECTIVE_KNOWN_UTC,
        "effective_known_rule": (
            "maximum_causally_supportable_clock_for_combined_exact_facts"
        ),
        "open_data_contract_precision": OPEN_DATA_CONTRACT_PRECISION,
        "observation_manifest_path": OBSERVATION_MANIFEST_PATH,
        "observation_manifest_bytes": OBSERVATION_MANIFEST_BYTES,
        "observation_manifest_sha256": OBSERVATION_MANIFEST_SHA256,
        "briefing_archive_path": BRIEFING_ARCHIVE_PATH,
        "briefing_archive_bytes": BRIEFING_ARCHIVE_BYTES,
        "briefing_archive_sha256": BRIEFING_ARCHIVE_SHA256,
        "publication_archive_path": PDF_ARCHIVE_PATH,
        "publication_archive_bytes": PDF_ARCHIVE_BYTES,
        "publication_archive_sha256": PDF_ARCHIVE_SHA256,
        "historical_collection_class": COLLECTION_CLASS,
        "prospective_clock": False,
        "prospective_observation": False,
    }
    for key, expected in expected_identity.items():
        if not _typed_equal(row[key], expected):
            raise ThailandCpiJuneBriefingUploadV1Error(
                f"contract_manifest_identity_mismatch:{key}"
            )
    if not _typed_equal(row["parsed_fact_scope"], EXPECTED_FACTS):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "contract_manifest_fact_scope_mismatch"
        )
    hard_guards = row["hard_guards"]
    expected_hard_guards = {
        "briefing_time_is_not_release_availability": True,
        "pdf_last_modified_is_not_public_availability_proof": True,
        "open_data_contract_has_date_only_not_exact_minute": True,
        "historical_late_observation_is_never_prospective_proof": True,
        "exact_prospective_cpi_release_clock_closed": False,
        "no_consensus_inference": True,
        "no_surprise_inference": True,
        "no_direction_inference": True,
        "direction": None,
        "consensus": None,
        "surprise": None,
        "proof_eligible": False,
        "confirmation_eligible": False,
        "promotion_eligible": False,
        "authorization_eligible": False,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
    }
    if not _typed_equal(hard_guards, expected_hard_guards):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "contract_manifest_guard_mismatch"
        )
    expected_separation = {
        "edit_existing_news_configuration": False,
        "register_live_collector": False,
        "write_database": False,
        "touch_processes": False,
        "touch_executor_or_account": False,
    }
    if not _typed_equal(row["separation_contract"], expected_separation):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "contract_manifest_separation_mismatch"
        )
    _official_url(
        row["briefing_page_url"],
        expected=BRIEFING_URL,
        domain="sisaket.moc.go.th",
        label="briefing",
    )
    _official_url(
        row["publication_pdf_url"],
        expected=PDF_URL,
        domain="uploads.tpso.go.th",
        label="pdf",
    )
    return dict(row)


_TOP_OBSERVATION_FIELDS = frozenset(
    {
        "schema_version", "observation_id", "source_id", "source_contract_id",
        "source_cohort_id", "currency", "event_series_id", "reference_period",
        "collection_class", "briefing_clock", "upload_clock",
        "effective_known_clock", "raw_payloads", "fact_scope",
        "evidence_boundaries",
    }
)

_PAYLOAD_FIELDS = frozenset(
    {
        "role", "source_url", "trusted_domain", "observed_utc",
        "completed_utc", "repeat_observed_utc", "repeat_completed_utc",
        "http_status", "repeat_http_status", "http_date_utc", "content_type",
        "server_last_modified_utc", "raw_response_bytes",
        "raw_response_sha256", "repeat_response_bytes",
        "repeat_response_sha256", "repeat_byte_identical", "repeat_semantics",
        "raw_payload_archive_path", "raw_payload_archive_read_only",
    }
)


def _load_observation_manifest() -> dict[str, Any]:
    path = _workspace_path(OBSERVATION_MANIFEST_PATH)
    raw = _read_exact_pinned_file(
        path,
        expected_path=path,
        expected_bytes=OBSERVATION_MANIFEST_BYTES,
        expected_sha256=OBSERVATION_MANIFEST_SHA256,
        require_read_only=True,
    )
    row = _closed_mapping(
        _strict_json(raw, label="observation_manifest"),
        label="observation_manifest",
        fields=_TOP_OBSERVATION_FIELDS,
    )
    expected_identity = {
        "schema_version": SCHEMA_VERSION,
        "observation_id": (
            "thailand_cpi_june_2026_briefing_upload_v1_"
            "observation_20260817T095017Z"
        ),
        "source_id": SOURCE_ID,
        "source_contract_id": CONTRACT_ID,
        "source_cohort_id": COHORT_ID,
        "currency": "THB",
        "event_series_id": "thailand_consumer_price_index_headline",
        "reference_period": "2026-06",
        "collection_class": COLLECTION_CLASS,
    }
    for key, expected in expected_identity.items():
        if not _typed_equal(row[key], expected):
            raise ThailandCpiJuneBriefingUploadV1Error(
                f"observation_manifest_identity_mismatch:{key}"
            )
    expected_briefing_clock = {
        "source_reported_local": BRIEFING_SOURCE_REPORTED_LOCAL,
        "event_time_utc": BRIEFING_EVENT_TIME_UTC,
        "timezone": TIMEZONE_NAME,
        "semantics": BRIEFING_TIME_SEMANTICS,
    }
    expected_upload_clock = {
        "server_last_modified_utc": PDF_SERVER_LAST_MODIFIED_UTC,
        "semantics": PDF_LAST_MODIFIED_SEMANTICS,
    }
    expected_effective_clock = {
        "effective_known_utc": EFFECTIVE_KNOWN_UTC,
        "rule": "maximum_causally_supportable_clock_for_combined_exact_facts",
        "basis": "pdf_first_seen_after_briefing_and_server_last_modified",
    }
    if not _typed_equal(row["briefing_clock"], expected_briefing_clock):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "observation_briefing_clock_mismatch"
        )
    if not _typed_equal(row["upload_clock"], expected_upload_clock):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "observation_upload_clock_mismatch"
        )
    if not _typed_equal(row["effective_known_clock"], expected_effective_clock):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "observation_effective_clock_mismatch"
        )
    payloads = row["raw_payloads"]
    if not isinstance(payloads, list) or len(payloads) != 2:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "observation_payload_count_mismatch"
        )
    by_role: dict[str, Mapping[str, Any]] = {}
    for index, value in enumerate(payloads):
        item = _closed_mapping(
            value,
            label=f"observation_payload_{index}",
            fields=_PAYLOAD_FIELDS,
        )
        role = item["role"]
        if type(role) is not str or role in by_role:
            raise ThailandCpiJuneBriefingUploadV1Error(
                "observation_payload_role_invalid"
            )
        by_role[role] = item
        clocks = [
            _parse_time(item["observed_utc"], label=f"{role}_observed"),
            _parse_time(item["completed_utc"], label=f"{role}_completed"),
            _parse_time(
                item["repeat_observed_utc"], label=f"{role}_repeat_observed"
            ),
            _parse_time(
                item["repeat_completed_utc"], label=f"{role}_repeat_completed"
            ),
        ]
        if clocks != sorted(clocks):
            raise ThailandCpiJuneBriefingUploadV1Error(
                "observation_chronology_invalid"
            )
        _parse_time(item["http_date_utc"], label=f"{role}_http_date")
        if _integer(item["http_status"], label=f"{role}_status") != 200:
            raise ThailandCpiJuneBriefingUploadV1Error(
                "observation_http_status_invalid"
            )
        if _integer(item["repeat_http_status"], label=f"{role}_repeat") != 200:
            raise ThailandCpiJuneBriefingUploadV1Error(
                "observation_repeat_status_invalid"
            )
        if item["raw_payload_archive_read_only"] is not True:
            raise ThailandCpiJuneBriefingUploadV1Error(
                "observation_archive_read_only_claim_missing"
            )
    page = by_role.get("official_moc_provincial_briefing_page")
    pdf = by_role.get("official_tpso_cpi_pdf")
    if page is None or pdf is None or len(by_role) != 2:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "observation_payload_roles_mismatch"
        )
    expected_page = {
        "source_url": BRIEFING_URL,
        "trusted_domain": "sisaket.moc.go.th",
        "observed_utc": PAGE_FIRST_SEEN_UTC,
        "content_type": "text/html; charset=UTF-8",
        "server_last_modified_utc": None,
        "raw_response_bytes": BRIEFING_ARCHIVE_BYTES,
        "raw_response_sha256": BRIEFING_ARCHIVE_SHA256,
        "repeat_response_bytes": 166_730,
        "repeat_response_sha256": (
            "d370a3fbdfb859405dbcd9e11cca4802819a026fef434216c3eb831082f82f67"
        ),
        "repeat_byte_identical": False,
        "repeat_semantics": (
            "dynamic_official_page_changed_between_requests_so_only_first_"
            "exact_payload_is_archived"
        ),
        "raw_payload_archive_path": BRIEFING_ARCHIVE_PATH,
    }
    expected_pdf = {
        "source_url": PDF_URL,
        "trusted_domain": "uploads.tpso.go.th",
        "observed_utc": PDF_FIRST_SEEN_UTC,
        "content_type": "application/pdf",
        "server_last_modified_utc": PDF_SERVER_LAST_MODIFIED_UTC,
        "raw_response_bytes": PDF_ARCHIVE_BYTES,
        "raw_response_sha256": PDF_ARCHIVE_SHA256,
        "repeat_response_bytes": PDF_ARCHIVE_BYTES,
        "repeat_response_sha256": PDF_ARCHIVE_SHA256,
        "repeat_byte_identical": True,
        "repeat_semantics": "two_consecutive_observations_were_byte_identical",
        "raw_payload_archive_path": PDF_ARCHIVE_PATH,
    }
    for item, expected, label in (
        (page, expected_page, "page"),
        (pdf, expected_pdf, "pdf"),
    ):
        for key, expected_value in expected.items():
            if not _typed_equal(item[key], expected_value):
                raise ThailandCpiJuneBriefingUploadV1Error(
                    f"observation_{label}_mismatch:{key}"
                )
    _official_url(
        page["source_url"], expected=BRIEFING_URL,
        domain="sisaket.moc.go.th", label="briefing"
    )
    _official_url(
        pdf["source_url"], expected=PDF_URL,
        domain="uploads.tpso.go.th", label="pdf"
    )
    if not _typed_equal(row["fact_scope"], EXPECTED_FACTS):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "observation_fact_scope_mismatch"
        )
    expected_boundaries = {
        "briefing_time_is_not_release_availability": True,
        "pdf_last_modified_is_not_public_availability_proof": True,
        "open_data_contract_has_date_only_not_exact_minute": True,
        "historical_late_observation_is_never_prospective_proof": True,
        "exact_prospective_cpi_release_clock_closed": False,
        "consensus": None,
        "surprise": None,
        "direction": None,
        "proof_eligible": False,
        "promotion_eligible": False,
        "authorization_eligible": False,
        "execution_eligible": False,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
    }
    if not _typed_equal(row["evidence_boundaries"], expected_boundaries):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "observation_evidence_boundaries_mismatch"
        )
    event = _parse_time(BRIEFING_EVENT_TIME_UTC, label="briefing_event")
    upload = _parse_time(PDF_SERVER_LAST_MODIFIED_UTC, label="pdf_upload")
    page_seen = _parse_time(PAGE_FIRST_SEEN_UTC, label="page_first_seen")
    pdf_seen = _parse_time(PDF_FIRST_SEEN_UTC, label="pdf_first_seen")
    effective = _parse_time(EFFECTIVE_KNOWN_UTC, label="effective_known")
    if not event < upload < page_seen < pdf_seen:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "source_clock_chronology_invalid"
        )
    if effective != max(event, upload, page_seen, pdf_seen):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "effective_known_not_causal_maximum"
        )
    return dict(row)


def _html_text(payload: bytes) -> str:
    try:
        raw = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "briefing_page_invalid_utf8"
        ) from exc
    raw = re.sub(r"(?is)<script\b.*?</script>|<style\b.*?</style>", " ", raw)
    return " ".join(
        html_lib.unescape(re.sub(r"(?s)<[^>]+>", " ", raw)).split()
    )


def parse_briefing_page(payload: bytes) -> dict[str, Any]:
    try:
        raw_html = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "briefing_page_invalid_utf8"
        ) from exc
    if "article-detail-173746" not in raw_html:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "briefing_article_identity_missing"
        )
    text = _html_text(payload)
    required = ("ดัชนีราคาผู้บริโภค ประจำเดือนมิถุนายน 2569",)
    for literal in required:
        if literal not in text:
            raise ThailandCpiJuneBriefingUploadV1Error(
                "briefing_required_literal_missing"
            )
    matches = re.findall(
        r"วันที่\s*6\s*กรกฎาคม\s*2569\s*เวลา\s*10[.]30\s*น[.]",
        text,
    )
    if len(matches) != 1:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "briefing_clock_missing_or_ambiguous"
        )
    local = dt.datetime(2026, 7, 6, 10, 30, tzinfo=ZoneInfo(TIMEZONE_NAME))
    if local.isoformat() != BRIEFING_SOURCE_REPORTED_LOCAL:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "briefing_local_timezone_mismatch"
        )
    if _iso_utc(local) != BRIEFING_EVENT_TIME_UTC:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "briefing_utc_conversion_mismatch"
        )
    return {
        "source_reported_local": BRIEFING_SOURCE_REPORTED_LOCAL,
        "event_time_utc": BRIEFING_EVENT_TIME_UTC,
        "timezone": TIMEZONE_NAME,
        "time_semantics": BRIEFING_TIME_SEMANTICS,
    }


def _unique_number(pattern: str, text: str, *, label: str) -> float:
    values = {float(value) for value in re.findall(pattern, text, flags=re.S)}
    if len(values) != 1 or not all(math.isfinite(value) for value in values):
        raise ThailandCpiJuneBriefingUploadV1Error(
            f"{label}_missing_or_ambiguous"
        )
    return values.pop()


def _extract_pdf_facts_from_text(page_one_text: str) -> dict[str, Any]:
    text = " ".join(page_one_text.split())
    required = (
        "ดัชนีราคาผู้บริโภค ประจำเดือนมิถุนายน 2569",
        "(YoY)",
        "(MoM)",
    )
    for literal in required:
        if literal not in text:
            raise ThailandCpiJuneBriefingUploadV1Error(
                "pdf_required_literal_missing"
            )
    index_value = _unique_number(
        r"ดัชนีราคาผู้บริโภคทั่วไป\s*ของไทย\s*เดือนมิถุนายน\s*2569\s*"
        r"เท่ากับ\s*([0-9]+(?:[.][0-9]+)?)",
        text,
        label="headline_cpi_index",
    )
    yoy = _unique_number(
        r"อัตรา\s*เงินเฟ้อทั่วไป\s*สูงขึ้น\s*ร้อยละ\s*"
        r"([0-9]+(?:[.][0-9]+)?)\s*[(]YoY[)]",
        text,
        label="headline_year_over_year",
    )
    core_yoy = _unique_number(
        r"อัตราเงินเฟ้อพื้นฐาน\s*[(]อัตราเงินเฟ้อทั่วไป\s*"
        r"เมื่อหักอาหารสดและพลังงานออก[)]\s*สูงขึ้นร้อยละ\s*"
        r"([0-9]+(?:[.][0-9]+)?)\s*[(]YoY[)]",
        text,
        label="core_year_over_year",
    )
    mom_matches = re.findall(
        r"ดัชนีราคาผู้บริโภค\s*ทั่วไปเดือนมิถุนายน\s*2569\s*"
        r"เมื่อเทียบกับเดือน\s*พฤษภาคม\s*2569\s*(ลดลง|สูงขึ้น)ร้อยละ\s*"
        r"([0-9]+(?:[.][0-9]+)?)\s*[(]MoM[)]",
        text,
    )
    signed_mom = {
        (-float(value) if direction == "ลดลง" else float(value))
        for direction, value in mom_matches
    }
    if len(signed_mom) != 1 or not all(math.isfinite(v) for v in signed_mom):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "headline_month_over_month_missing_or_ambiguous"
        )
    facts = {
        "headline_cpi_index": index_value,
        "headline_month_over_month_pct": signed_mom.pop(),
        "headline_year_over_year_pct": yoy,
        "core_year_over_year_pct": core_yoy,
    }
    if not _typed_equal(facts, EXPECTED_FACTS):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "pdf_exact_facts_changed"
        )
    return facts


def parse_publication_pdf(payload: bytes) -> dict[str, Any]:
    if not payload.startswith(b"%PDF-"):
        raise ThailandCpiJuneBriefingUploadV1Error("publication_not_pdf")
    try:
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(payload))
        if len(reader.pages) != 4:
            raise ThailandCpiJuneBriefingUploadV1Error(
                "publication_page_count_mismatch"
            )
        page_one_text = reader.pages[0].extract_text() or ""
    except ThailandCpiJuneBriefingUploadV1Error:
        raise
    except Exception as exc:
        raise ThailandCpiJuneBriefingUploadV1Error(
            "publication_pdf_parse_failed"
        ) from exc
    return _extract_pdf_facts_from_text(page_one_text)


def build_historical_record() -> dict[str, Any]:
    """Return one immutable, non-proof archive counterfactual record."""

    load_contract_manifest()
    observation = _load_observation_manifest()
    briefing_path = _workspace_path(BRIEFING_ARCHIVE_PATH)
    pdf_path = _workspace_path(PDF_ARCHIVE_PATH)
    briefing_raw = _read_exact_pinned_file(
        briefing_path,
        expected_path=briefing_path,
        expected_bytes=BRIEFING_ARCHIVE_BYTES,
        expected_sha256=BRIEFING_ARCHIVE_SHA256,
        require_read_only=True,
    )
    pdf_raw = _read_exact_pinned_file(
        pdf_path,
        expected_path=pdf_path,
        expected_bytes=PDF_ARCHIVE_BYTES,
        expected_sha256=PDF_ARCHIVE_SHA256,
        require_read_only=True,
    )
    clock = parse_briefing_page(briefing_raw)
    facts = parse_publication_pdf(pdf_raw)
    identity = {
        "source_id": SOURCE_ID,
        "source_contract_id": CONTRACT_ID,
        "source_cohort_id": COHORT_ID,
        "briefing_archive_sha256": BRIEFING_ARCHIVE_SHA256,
        "publication_archive_sha256": PDF_ARCHIVE_SHA256,
        "observation_manifest_sha256": OBSERVATION_MANIFEST_SHA256,
        "effective_known_utc": EFFECTIVE_KNOWN_UTC,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "source_id": SOURCE_ID,
        "source_contract_id": CONTRACT_ID,
        "source_cohort_id": COHORT_ID,
        "record_id": SOURCE_ID + "_" + _sha256_json(identity),
        "currency": "THB",
        "currency_factor": "THB",
        "event_series_id": "thailand_consumer_price_index_headline",
        "reference_period": "2026-06",
        "briefing_source_reported_local": clock["source_reported_local"],
        "briefing_event_time_utc": clock["event_time_utc"],
        "briefing_timezone": clock["timezone"],
        "briefing_time_semantics": clock["time_semantics"],
        "pdf_server_last_modified_utc": PDF_SERVER_LAST_MODIFIED_UTC,
        "pdf_last_modified_semantics": PDF_LAST_MODIFIED_SEMANTICS,
        "page_first_seen_utc": PAGE_FIRST_SEEN_UTC,
        "pdf_first_seen_utc": PDF_FIRST_SEEN_UTC,
        "fact_first_seen_utc": PDF_FIRST_SEEN_UTC,
        "effective_known_utc": EFFECTIVE_KNOWN_UTC,
        "effective_known_rule": (
            "maximum_causally_supportable_clock_for_combined_exact_facts"
        ),
        "open_data_contract_precision": OPEN_DATA_CONTRACT_PRECISION,
        "collection_class": COLLECTION_CLASS,
        **facts,
        "briefing_source_url": BRIEFING_URL,
        "publication_source_url": PDF_URL,
        "briefing_source_bytes": BRIEFING_ARCHIVE_BYTES,
        "briefing_source_sha256": BRIEFING_ARCHIVE_SHA256,
        "publication_source_bytes": PDF_ARCHIVE_BYTES,
        "publication_source_sha256": PDF_ARCHIVE_SHA256,
        "observation_manifest_path": OBSERVATION_MANIFEST_PATH,
        "observation_manifest_sha256": OBSERVATION_MANIFEST_SHA256,
        "observation_id": observation["observation_id"],
        **EXPECTED_GUARDS,
    }


def validate_historical_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """Refuse chronology relabeling or any safety/promotion mutation."""

    expected = build_historical_record()
    if not _typed_equal(record, expected):
        raise ThailandCpiJuneBriefingUploadV1Error(
            "historical_record_typed_content_mismatch"
        )
    return dict(record)


__all__ = [
    "BRIEFING_ARCHIVE_PATH", "BRIEFING_ARCHIVE_SHA256",
    "BRIEFING_EVENT_TIME_UTC", "BRIEFING_SOURCE_REPORTED_LOCAL",
    "BRIEFING_TIME_SEMANTICS", "BRIEFING_URL", "COHORT_ID",
    "COLLECTION_CLASS", "CONTRACT_ID", "CONTRACT_MANIFEST_PATH",
    "EFFECTIVE_KNOWN_UTC", "EXPECTED_FACTS", "OBSERVATION_MANIFEST_PATH",
    "OBSERVATION_MANIFEST_SHA256", "OPEN_DATA_CONTRACT_PRECISION",
    "PAGE_FIRST_SEEN_UTC", "PDF_ARCHIVE_PATH", "PDF_ARCHIVE_SHA256",
    "PDF_FIRST_SEEN_UTC", "PDF_LAST_MODIFIED_SEMANTICS",
    "PDF_SERVER_LAST_MODIFIED_UTC", "PDF_URL", "SCHEMA_VERSION",
    "SOURCE_ID", "SUPPORTED_EXECUTION_DECISION",
    "ThailandCpiJuneBriefingUploadV1Error", "TIMEZONE_NAME",
    "build_historical_record", "load_contract_manifest",
    "parse_briefing_page", "parse_publication_pdf",
    "validate_historical_record",
]
