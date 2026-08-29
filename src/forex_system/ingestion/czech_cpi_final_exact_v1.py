"""Exact, late-observed CZSO July-2026 final-CPI source cohort.

This module is deliberately isolated, disabled, unregistered, shadow-only, and
read-only.  It reopens two exact CZSO responses retained on 17 August 2026:
the public calendar API response that reports the 11 August event at
``09:00:00+02:00`` and the final July CPI publication page.  Because both
responses were first observed after the event, the resulting record is an
availability-counterfactual archive fact only.  It can never be prospective
proof and cannot imply consensus, surprise, direction, promotion,
authorization, or execution.
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
from typing import Any, Mapping
from urllib.parse import urlparse
from zoneinfo import ZoneInfo


SCHEMA_VERSION = 1
SOURCE_ID = "czech_cpi_final_exact_v1"
CONTRACT_ID = "czech_cpi_final_exact_v1_20260817"
COHORT_ID = "czech_cpi_final_exact_v1_20260817"
TIMEZONE_NAME = "Europe/Prague"
SUPPORTED_EXECUTION_DECISION = "no_trade"

CALENDAR_URL = (
    "https://csu.gov.cz/api/web-externi/udalosti?datumOd=2026-08-11&"
    "pocetDni=1&webKod=rychle-informace&kodJazyk=EN&kategorieKod="
)
PUBLICATION_URL = (
    "https://csu.gov.cz/rychle-informace/"
    "consumer-price-indices-inflation-july-2026"
)
EVENT_GUID = "04cce34c-6553-4817-899f-ab353685d990"
CONTENT_GUID = "e5d9d9bb-6f20-4049-a1d0-b7b1edb279c1"
PRODUCT_CODE = "012024-26"
SOURCE_REPORTED_START = "2026-08-11T09:00:00+02:00"
SCHEDULED_UTC = "2026-08-11T07:00:00Z"

CONTRACT_MANIFEST_PATH = "config/czech_cpi_final_exact_v1.json"
CONTRACT_MANIFEST_BYTES = 3_377
CONTRACT_MANIFEST_SHA256 = (
    "82955c918736ec3142078e7c6fdb7f9aa4f4a783536a48870d43a89af324ec20"
)
OBSERVATION_MANIFEST_PATH = (
    "data/oanda_training_manager/source_archives/czech_cpi_final_exact_v1/"
    "source_observation_manifest_20260817T094022Z.json"
)
OBSERVATION_MANIFEST_BYTES = 3_697
OBSERVATION_MANIFEST_SHA256 = (
    "1c569595f394c829bb84f1c1225ac31b5331b5c3692af4a809b2ddbe903de9e5"
)
CALENDAR_ARCHIVE_PATH = (
    "data/oanda_training_manager/source_archives/czech_cpi_final_exact_v1/"
    "czso_calendar_api_observed_20260817.json"
)
CALENDAR_ARCHIVE_BYTES = 1_300
CALENDAR_ARCHIVE_SHA256 = (
    "3cf984548be632f967c413ae7d5a42116767618051abeee00da37ab29823dbd7"
)
PUBLICATION_ARCHIVE_PATH = (
    "data/oanda_training_manager/source_archives/czech_cpi_final_exact_v1/"
    "czso_cpi_final_july_2026_observed_20260817.html"
)
PUBLICATION_ARCHIVE_BYTES = 172_027
PUBLICATION_ARCHIVE_SHA256 = (
    "4193065b668789457273e411b1d7df6b6a949eef7eb20ed575145273294575e8"
)

CALENDAR_OBSERVED_UTC = "2026-08-17T09:40:22.5309820+00:00"
PUBLICATION_OBSERVED_UTC = "2026-08-17T09:40:23.4061684+00:00"
COLLECTION_CLASS = "historical_availability_counterfactual_archive_only"

UTC = dt.timezone.utc
_WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
_SHA256 = re.compile(r"[0-9a-f]{64}")

EXPECTED_FACTS: dict[str, Any] = {
    "publication_date": "2026-08-11",
    "cpi_month_over_month_pct": 0.6,
    "cpi_year_over_year_pct": 1.7,
    "average_twelve_month_inflation_pct": 2.0,
    "hicp_preliminary_excluded": True,
}

EXPECTED_GUARDS: dict[str, Any] = {
    "research_only": True,
    "shadow_only": True,
    "enabled": False,
    "registered_with_live_collector": False,
    "runtime_supported": False,
    "prospective_clock": False,
    "prospective_observation": False,
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


class CzechCpiFinalExactV1Error(RuntimeError):
    """Raised whenever the exact source/chronology contract is not proved."""


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
        raise CzechCpiFinalExactV1Error(f"{label}_nonfinite_number:{value}")

    def parse_float(value: str) -> float:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise CzechCpiFinalExactV1Error(f"{label}_nonfinite_number:{value}")
        return parsed

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in pairs:
            if key in output:
                raise CzechCpiFinalExactV1Error(f"{label}_duplicate_key:{key}")
            output[key] = value
        return output

    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
            parse_float=parse_float,
        )
    except CzechCpiFinalExactV1Error:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise CzechCpiFinalExactV1Error(f"{label}_invalid_json") from exc


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
        raise CzechCpiFinalExactV1Error(f"{label}_not_mapping")
    keys = set(value)
    if keys != set(fields):
        raise CzechCpiFinalExactV1Error(
            f"{label}_schema_mismatch:missing={sorted(set(fields)-keys)}:"
            f"extra={sorted(keys-set(fields))}"
        )
    return value


def _integer(value: Any, *, label: str) -> int:
    if type(value) is not int:
        raise CzechCpiFinalExactV1Error(f"{label}_not_exact_int")
    return value


def _number(value: Any, *, label: str) -> float:
    if type(value) not in {int, float}:
        raise CzechCpiFinalExactV1Error(f"{label}_not_number")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise CzechCpiFinalExactV1Error(f"{label}_not_finite")
    return parsed


def _parse_time(value: Any, *, label: str) -> dt.datetime:
    if type(value) is not str or not value:
        raise CzechCpiFinalExactV1Error(f"{label}_not_string")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CzechCpiFinalExactV1Error(f"{label}_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CzechCpiFinalExactV1Error(f"{label}_timezone_missing")
    return parsed


def _iso_utc(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _official_url(value: Any, *, expected: str, label: str) -> str:
    if type(value) is not str or value != expected:
        raise CzechCpiFinalExactV1Error(f"{label}_url_mismatch")
    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "csu.gov.cz"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.fragment
    ):
        raise CzechCpiFinalExactV1Error(f"{label}_url_identity_invalid")
    return value


def _workspace_path(relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise CzechCpiFinalExactV1Error("pinned_path_not_workspace_relative")
    candidate = (_WORKSPACE_ROOT / relative).absolute()
    try:
        candidate.relative_to(_WORKSPACE_ROOT)
    except ValueError as exc:
        raise CzechCpiFinalExactV1Error("pinned_path_escaped_workspace") from exc
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
    """Reject substitution, links, mutation, races, and content drift."""

    requested = Path(path).absolute()
    expected = Path(expected_path).absolute()
    if os.path.normcase(str(requested)) != os.path.normcase(str(expected)):
        raise CzechCpiFinalExactV1Error("pinned_file_path_substitution")
    if requested.is_symlink():
        raise CzechCpiFinalExactV1Error("pinned_file_symlink_forbidden")
    try:
        resolved = requested.resolve(strict=True)
        expected_resolved = expected.resolve(strict=True)
    except OSError as exc:
        raise CzechCpiFinalExactV1Error("pinned_file_missing") from exc
    if resolved != expected_resolved:
        raise CzechCpiFinalExactV1Error("pinned_file_resolved_path_mismatch")
    before = os.lstat(requested)
    if not stat.S_ISREG(before.st_mode):
        raise CzechCpiFinalExactV1Error("pinned_file_not_regular")
    if before.st_nlink != 1:
        raise CzechCpiFinalExactV1Error("pinned_file_hardlink_forbidden")
    if require_read_only:
        if os.name == "nt":
            readonly_flag = getattr(stat, "FILE_ATTRIBUTE_READONLY", 0x1)
            attributes = int(getattr(before, "st_file_attributes", 0))
            if attributes & readonly_flag == 0:
                raise CzechCpiFinalExactV1Error("pinned_archive_not_read_only")
        elif before.st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH):
            raise CzechCpiFinalExactV1Error("pinned_archive_not_read_only")
    with requested.open("rb") as handle:
        opened_before = os.fstat(handle.fileno())
        if _file_identity(opened_before) != _file_identity(before):
            raise CzechCpiFinalExactV1Error("pinned_file_swapped_before_open")
        raw = handle.read()
        opened_after = os.fstat(handle.fileno())
    after = os.lstat(requested)
    if _file_identity(opened_before) != _file_identity(opened_after):
        raise CzechCpiFinalExactV1Error("pinned_file_changed_while_reading")
    if _file_identity(opened_after) != _file_identity(after):
        raise CzechCpiFinalExactV1Error("pinned_file_path_swapped_after_read")
    if len(raw) != expected_bytes:
        raise CzechCpiFinalExactV1Error("pinned_file_byte_count_mismatch")
    if _SHA256.fullmatch(expected_sha256) is None:
        raise CzechCpiFinalExactV1Error("pinned_expected_sha256_invalid")
    if _sha256_bytes(raw) != expected_sha256:
        raise CzechCpiFinalExactV1Error("pinned_file_sha256_mismatch")
    return raw


_PAYLOAD_FIELDS = frozenset(
    {
        "role",
        "source_url",
        "trusted_domain",
        "observed_utc",
        "completed_utc",
        "repeat_observed_utc",
        "repeat_completed_utc",
        "http_status",
        "repeat_http_status",
        "http_date",
        "content_type",
        "raw_response_bytes",
        "raw_response_sha256",
        "repeat_response_sha256",
        "raw_payload_archive_path",
        "raw_payload_archive_read_only",
    }
)

_OBSERVATION_FIELDS = frozenset(
    {
        "schema_version",
        "observation_id",
        "source_id",
        "source_contract_id",
        "source_cohort_id",
        "currency",
        "event_series_id",
        "reference_period",
        "official_product_code",
        "official_event_guid",
        "official_content_guid",
        "source_reported_start",
        "source_reported_offset",
        "timezone_contract",
        "scheduled_utc",
        "collection_class",
        "prospective_clock",
        "prospective_observation",
        "raw_payloads",
        "fact_scope",
        "evidence_boundaries",
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
    value = _strict_json(raw, label="observation_manifest")
    row = _closed_mapping(value, label="observation_manifest", fields=_OBSERVATION_FIELDS)
    expected_identity = {
        "schema_version": SCHEMA_VERSION,
        "observation_id": "czech_cpi_final_exact_v1_observation_20260817T094022Z",
        "source_id": SOURCE_ID,
        "source_contract_id": CONTRACT_ID,
        "source_cohort_id": COHORT_ID,
        "currency": "CZK",
        "event_series_id": "czso_consumer_price_indices_inflation_final",
        "reference_period": "2026-07",
        "official_product_code": PRODUCT_CODE,
        "official_event_guid": EVENT_GUID,
        "official_content_guid": CONTENT_GUID,
        "source_reported_start": SOURCE_REPORTED_START,
        "source_reported_offset": "+02:00",
        "timezone_contract": TIMEZONE_NAME,
        "scheduled_utc": SCHEDULED_UTC,
        "collection_class": COLLECTION_CLASS,
        "prospective_clock": False,
        "prospective_observation": False,
    }
    for key, expected in expected_identity.items():
        if not _typed_equal(row[key], expected):
            raise CzechCpiFinalExactV1Error(
                f"observation_manifest_identity_mismatch:{key}"
            )
    payloads = row["raw_payloads"]
    if not isinstance(payloads, list) or len(payloads) != 2:
        raise CzechCpiFinalExactV1Error("observation_payload_count_mismatch")
    by_role: dict[str, Mapping[str, Any]] = {}
    for index, payload in enumerate(payloads):
        item = _closed_mapping(
            payload, label=f"observation_payload_{index}", fields=_PAYLOAD_FIELDS
        )
        role = item["role"]
        if type(role) is not str or role in by_role:
            raise CzechCpiFinalExactV1Error("observation_payload_role_invalid")
        by_role[role] = item
        observed = _parse_time(item["observed_utc"], label=f"{role}_observed")
        completed = _parse_time(item["completed_utc"], label=f"{role}_completed")
        repeat_observed = _parse_time(
            item["repeat_observed_utc"], label=f"{role}_repeat_observed"
        )
        repeat_completed = _parse_time(
            item["repeat_completed_utc"], label=f"{role}_repeat_completed"
        )
        if not observed <= completed <= repeat_observed <= repeat_completed:
            raise CzechCpiFinalExactV1Error("observation_chronology_invalid")
        _parse_time(item["http_date"], label=f"{role}_http_date")
        if _integer(item["http_status"], label=f"{role}_http_status") != 200:
            raise CzechCpiFinalExactV1Error("observation_http_status_invalid")
        if (
            _integer(item["repeat_http_status"], label=f"{role}_repeat_status")
            != 200
        ):
            raise CzechCpiFinalExactV1Error("repeat_http_status_invalid")
        if item["raw_response_sha256"] != item["repeat_response_sha256"]:
            raise CzechCpiFinalExactV1Error("repeat_response_hash_mismatch")
        if item["raw_payload_archive_read_only"] is not True:
            raise CzechCpiFinalExactV1Error("archive_read_only_claim_missing")
    expected_payloads = {
        "official_calendar_api_response": {
            "source_url": CALENDAR_URL,
            "observed_utc": CALENDAR_OBSERVED_UTC,
            "content_type": "application/json",
            "bytes": CALENDAR_ARCHIVE_BYTES,
            "sha256": CALENDAR_ARCHIVE_SHA256,
            "path": CALENDAR_ARCHIVE_PATH,
        },
        "official_final_cpi_publication_page": {
            "source_url": PUBLICATION_URL,
            "observed_utc": PUBLICATION_OBSERVED_UTC,
            "content_type": "text/html; charset=utf-8",
            "bytes": PUBLICATION_ARCHIVE_BYTES,
            "sha256": PUBLICATION_ARCHIVE_SHA256,
            "path": PUBLICATION_ARCHIVE_PATH,
        },
    }
    if set(by_role) != set(expected_payloads):
        raise CzechCpiFinalExactV1Error("observation_payload_roles_mismatch")
    scheduled = _parse_time(SCHEDULED_UTC, label="scheduled_utc")
    for role, expected in expected_payloads.items():
        item = by_role[role]
        _official_url(item["source_url"], expected=expected["source_url"], label=role)
        if item["trusted_domain"] != "csu.gov.cz":
            raise CzechCpiFinalExactV1Error("observation_trusted_domain_mismatch")
        if item["observed_utc"] != expected["observed_utc"]:
            raise CzechCpiFinalExactV1Error("observation_timestamp_mismatch")
        if _parse_time(item["observed_utc"], label=role) <= scheduled:
            raise CzechCpiFinalExactV1Error(
                "historical_observation_not_strictly_after_event"
            )
        if item["content_type"] != expected["content_type"]:
            raise CzechCpiFinalExactV1Error("observation_content_type_mismatch")
        if _integer(item["raw_response_bytes"], label=f"{role}_bytes") != expected["bytes"]:
            raise CzechCpiFinalExactV1Error("observation_byte_count_mismatch")
        if item["raw_response_sha256"] != expected["sha256"]:
            raise CzechCpiFinalExactV1Error("observation_sha256_mismatch")
        if item["raw_payload_archive_path"] != expected["path"]:
            raise CzechCpiFinalExactV1Error("observation_archive_path_mismatch")
    if not _typed_equal(row["fact_scope"], EXPECTED_FACTS):
        raise CzechCpiFinalExactV1Error("observation_fact_scope_mismatch")
    boundaries = row["evidence_boundaries"]
    expected_boundaries = {
        "calendar_and_publication_first_observed_after_event": True,
        "historical_row_is_never_prospective_proof": True,
        "future_clock_requires_new_pre_event_observation": True,
        "consensus": None,
        "surprise": None,
        "direction": None,
        "proof_eligible": False,
        "promotion_eligible": False,
        "authorization_eligible": False,
        "execution_eligible": False,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
    }
    if not _typed_equal(boundaries, expected_boundaries):
        raise CzechCpiFinalExactV1Error("observation_evidence_boundaries_mismatch")
    return dict(row)


def load_contract_manifest(
    path: str | Path = CONTRACT_MANIFEST_PATH,
) -> dict[str, Any]:
    expected_path = _workspace_path(CONTRACT_MANIFEST_PATH)
    raw = _read_exact_pinned_file(
        path if Path(path).is_absolute() else _workspace_path(str(path)),
        expected_path=expected_path,
        expected_bytes=CONTRACT_MANIFEST_BYTES,
        expected_sha256=CONTRACT_MANIFEST_SHA256,
        require_read_only=False,
    )
    value = _strict_json(raw, label="contract_manifest")
    if not isinstance(value, Mapping):
        raise CzechCpiFinalExactV1Error("contract_manifest_not_mapping")
    required = {
        "schema_version": SCHEMA_VERSION,
        "source_id": SOURCE_ID,
        "source_contract_id": CONTRACT_ID,
        "source_cohort_id": COHORT_ID,
        "enabled": False,
        "registered_with_live_collector": False,
        "runtime_supported": False,
        "research_only": True,
        "shadow_only": True,
        "currency": "CZK",
        "event_series_id": "czso_consumer_price_indices_inflation_final",
        "reference_period": "2026-07",
        "official_product_code": PRODUCT_CODE,
        "official_event_guid": EVENT_GUID,
        "official_content_guid": CONTENT_GUID,
        "trusted_domains": ["csu.gov.cz"],
        "calendar_api_url": CALENDAR_URL,
        "publication_url": PUBLICATION_URL,
        "source_reported_start": SOURCE_REPORTED_START,
        "source_reported_offset": "+02:00",
        "timezone": TIMEZONE_NAME,
        "scheduled_utc": SCHEDULED_UTC,
        "observation_manifest_path": OBSERVATION_MANIFEST_PATH,
        "observation_manifest_bytes": OBSERVATION_MANIFEST_BYTES,
        "observation_manifest_sha256": OBSERVATION_MANIFEST_SHA256,
        "calendar_archive_path": CALENDAR_ARCHIVE_PATH,
        "calendar_archive_bytes": CALENDAR_ARCHIVE_BYTES,
        "calendar_archive_sha256": CALENDAR_ARCHIVE_SHA256,
        "publication_archive_path": PUBLICATION_ARCHIVE_PATH,
        "publication_archive_bytes": PUBLICATION_ARCHIVE_BYTES,
        "publication_archive_sha256": PUBLICATION_ARCHIVE_SHA256,
        "historical_collection_class": COLLECTION_CLASS,
        "prospective_clock": False,
        "prospective_observation": False,
        "parsed_fact_scope": EXPECTED_FACTS,
    }
    for key, expected in required.items():
        if key not in value or not _typed_equal(value[key], expected):
            raise CzechCpiFinalExactV1Error(f"contract_identity_mismatch:{key}")
    guard = value.get("hard_guards")
    expected_hard_guard = {
        "calendar_and_publication_first_observed_after_event": True,
        "historical_row_is_never_prospective_proof": True,
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
    if not _typed_equal(guard, expected_hard_guard):
        raise CzechCpiFinalExactV1Error("contract_hard_guard_mismatch")
    expected_separation = {
        "edit_existing_cnb_snapshot": False,
        "edit_news_configuration": False,
        "register_live_collector": False,
        "write_database": False,
        "touch_processes": False,
        "touch_executor_or_account": False,
    }
    if not _typed_equal(value.get("separation_contract"), expected_separation):
        raise CzechCpiFinalExactV1Error("contract_separation_mismatch")
    _load_observation_manifest()
    return dict(value)


_EVENT_FIELDS = frozenset(
    {
        "guid",
        "odkaz",
        "celodenni",
        "nazev",
        "zacatek",
        "konec",
        "jazyk",
        "kodProduktu",
        "guidObsah",
        "kategorie",
        "maProdukt",
        "pocetDni",
        "cisloDne",
    }
)
_CATEGORY_FIELDS = frozenset(
    {
        "id",
        "nadrazenaKategorieId",
        "nazev",
        "popis",
        "skupinaKategoriiKod",
        "skupinaKategoriiId",
        "kod",
        "url",
        "webKod",
        "poradi",
    }
)


def parse_calendar_api(payload: bytes) -> dict[str, Any]:
    value = _strict_json(payload, label="calendar_api")
    top = _closed_mapping(
        value, label="calendar_api", fields={"pocetCelkem", "seznam"}
    )
    if _integer(top["pocetCelkem"], label="calendar_total") != 55:
        raise CzechCpiFinalExactV1Error("calendar_total_changed")
    days = top["seznam"]
    if not isinstance(days, list) or len(days) != 1:
        raise CzechCpiFinalExactV1Error("calendar_day_count_mismatch")
    day = _closed_mapping(days[0], label="calendar_day", fields={"datum", "udalosti"})
    if day["datum"] != "2026-08-11" or not isinstance(day["udalosti"], list):
        raise CzechCpiFinalExactV1Error("calendar_day_identity_mismatch")
    matching: list[Mapping[str, Any]] = []
    for index, event in enumerate(day["udalosti"]):
        row = _closed_mapping(event, label=f"calendar_event_{index}", fields=_EVENT_FIELDS)
        if row["guid"] == EVENT_GUID:
            matching.append(row)
    if len(matching) != 1:
        raise CzechCpiFinalExactV1Error("calendar_event_not_unique")
    event = matching[0]
    expected_event = {
        "guid": EVENT_GUID,
        "odkaz": PUBLICATION_URL,
        "celodenni": False,
        "nazev": "Consumer price indices - inflation - July 2026",
        "zacatek": SOURCE_REPORTED_START,
        "konec": None,
        "jazyk": "EN",
        "kodProduktu": PRODUCT_CODE,
        "guidObsah": CONTENT_GUID,
        "maProdukt": True,
        "pocetDni": 1,
        "cisloDne": 1,
    }
    for key, expected in expected_event.items():
        if not _typed_equal(event[key], expected):
            raise CzechCpiFinalExactV1Error(f"calendar_event_mismatch:{key}")
    categories = event["kategorie"]
    if not isinstance(categories, list) or len(categories) != 1:
        raise CzechCpiFinalExactV1Error("calendar_category_count_mismatch")
    category = _closed_mapping(
        categories[0], label="calendar_category", fields=_CATEGORY_FIELDS
    )
    expected_category = {
        "id": 221,
        "nadrazenaKategorieId": None,
        "nazev": "News releases",
        "popis": None,
        "skupinaKategoriiKod": "typ-udalosti",
        "skupinaKategoriiId": 79,
        "kod": "rychle-informace",
        "url": None,
        "webKod": None,
        "poradi": 3,
    }
    if not _typed_equal(category, expected_category):
        raise CzechCpiFinalExactV1Error("calendar_category_mismatch")
    _official_url(event["odkaz"], expected=PUBLICATION_URL, label="calendar_event")
    start = _parse_time(event["zacatek"], label="source_reported_start")
    if start.utcoffset() != dt.timedelta(hours=2):
        raise CzechCpiFinalExactV1Error("source_reported_offset_mismatch")
    prague = dt.datetime(2026, 8, 11, 9, 0, 0, tzinfo=ZoneInfo(TIMEZONE_NAME))
    if prague.utcoffset() != dt.timedelta(hours=2):
        raise CzechCpiFinalExactV1Error("prague_timezone_offset_mismatch")
    if start.astimezone(UTC) != prague.astimezone(UTC):
        raise CzechCpiFinalExactV1Error("source_start_prague_binding_mismatch")
    if _iso_utc(start) != SCHEDULED_UTC:
        raise CzechCpiFinalExactV1Error("scheduled_utc_mismatch")
    return {
        "event_guid": EVENT_GUID,
        "content_guid": CONTENT_GUID,
        "product_code": PRODUCT_CODE,
        "publication_url": PUBLICATION_URL,
        "source_reported_start": SOURCE_REPORTED_START,
        "source_reported_offset": "+02:00",
        "timezone": TIMEZONE_NAME,
        "scheduled_utc": SCHEDULED_UTC,
    }


def _html_text(payload: bytes) -> str:
    try:
        source = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CzechCpiFinalExactV1Error("publication_not_utf8") from exc
    if "\x00" in source:
        raise CzechCpiFinalExactV1Error("publication_contains_nul")
    source = re.sub(r"<(script|style)\b[^>]*>.*?</\1>", " ", source, flags=re.I | re.S)
    source = re.sub(r"<[^>]+>", " ", source)
    return re.sub(r"\s+", " ", html_lib.unescape(source)).strip()


def _unique_numeric_matches(pattern: str, text: str, *, label: str) -> float:
    values = re.findall(pattern, text, flags=re.I)
    parsed = {_number(float(value), label=label) for value in values}
    if len(parsed) != 1:
        raise CzechCpiFinalExactV1Error(f"{label}_missing_or_ambiguous")
    return parsed.pop()


def parse_publication(payload: bytes) -> dict[str, Any]:
    text = _html_text(payload)
    required_literals = (
        "Consumer price indices - inflation - July 2026",
        "Publication Date: 11. 08. 2026",
        f"Product Code: {PRODUCT_CODE}",
        "According to preliminary calculations, the HICP in Czechia",
    )
    for literal in required_literals:
        if literal not in text:
            raise CzechCpiFinalExactV1Error("publication_required_literal_missing")
    mom = _unique_numeric_matches(
        r"Consumer prices in July increased by\s*([0-9]+(?:\.[0-9]+)?)%,\s*month-on-month",
        text,
        label="cpi_month_over_month",
    )
    yoy = _unique_numeric_matches(
        r"year-on-year growth of consumer prices amounted to\s*([0-9]+(?:\.[0-9]+)?)%\s*in July",
        text,
        label="cpi_year_over_year",
    )
    average = _unique_numeric_matches(
        r"Inflation rate, i\.e\..*?twelve months to July 2026.*?amounted to\s*([0-9]+(?:\.[0-9]+)?)%",
        text,
        label="average_twelve_month_inflation",
    )
    facts = {
        "publication_date": "2026-08-11",
        "cpi_month_over_month_pct": mom,
        "cpi_year_over_year_pct": yoy,
        "average_twelve_month_inflation_pct": average,
        "hicp_preliminary_excluded": True,
    }
    if not _typed_equal(facts, EXPECTED_FACTS):
        raise CzechCpiFinalExactV1Error("publication_final_facts_changed")
    return facts


def build_historical_record() -> dict[str, Any]:
    """Reopen pinned bytes and return one non-proof archival fact record."""

    load_contract_manifest()
    observation = _load_observation_manifest()
    calendar_path = _workspace_path(CALENDAR_ARCHIVE_PATH)
    publication_path = _workspace_path(PUBLICATION_ARCHIVE_PATH)
    calendar_raw = _read_exact_pinned_file(
        calendar_path,
        expected_path=calendar_path,
        expected_bytes=CALENDAR_ARCHIVE_BYTES,
        expected_sha256=CALENDAR_ARCHIVE_SHA256,
        require_read_only=True,
    )
    publication_raw = _read_exact_pinned_file(
        publication_path,
        expected_path=publication_path,
        expected_bytes=PUBLICATION_ARCHIVE_BYTES,
        expected_sha256=PUBLICATION_ARCHIVE_SHA256,
        require_read_only=True,
    )
    clock = parse_calendar_api(calendar_raw)
    facts = parse_publication(publication_raw)
    scheduled = _parse_time(SCHEDULED_UTC, label="scheduled")
    calendar_observed = _parse_time(CALENDAR_OBSERVED_UTC, label="calendar_observed")
    publication_observed = _parse_time(
        PUBLICATION_OBSERVED_UTC, label="publication_observed"
    )
    if calendar_observed <= scheduled or publication_observed <= scheduled:
        raise CzechCpiFinalExactV1Error("historical_chronology_not_late_observed")
    if publication_observed < calendar_observed:
        raise CzechCpiFinalExactV1Error("publication_observed_before_calendar")
    identity = {
        "source_id": SOURCE_ID,
        "source_contract_id": CONTRACT_ID,
        "source_cohort_id": COHORT_ID,
        "event_guid": EVENT_GUID,
        "scheduled_utc": SCHEDULED_UTC,
        "calendar_archive_sha256": CALENDAR_ARCHIVE_SHA256,
        "publication_archive_sha256": PUBLICATION_ARCHIVE_SHA256,
        "observation_manifest_sha256": OBSERVATION_MANIFEST_SHA256,
    }
    record = {
        "schema_version": SCHEMA_VERSION,
        "source_id": SOURCE_ID,
        "source_contract_id": CONTRACT_ID,
        "source_cohort_id": COHORT_ID,
        "record_id": "czech_cpi_final_exact_v1_" + _sha256_json(identity),
        "event_series_id": "czso_consumer_price_indices_inflation_final",
        "event_guid": EVENT_GUID,
        "content_guid": CONTENT_GUID,
        "product_code": PRODUCT_CODE,
        "currency": "CZK",
        "currency_factor": "CZK",
        "reference_period": "2026-07",
        "publication_date": facts["publication_date"],
        "source_reported_start": clock["source_reported_start"],
        "source_reported_offset": clock["source_reported_offset"],
        "timezone": clock["timezone"],
        "scheduled_utc": clock["scheduled_utc"],
        "calendar_first_observed_utc": CALENDAR_OBSERVED_UTC,
        "publication_first_observed_utc": PUBLICATION_OBSERVED_UTC,
        "fact_first_observed_utc": PUBLICATION_OBSERVED_UTC,
        "collection_class": COLLECTION_CLASS,
        "cpi_month_over_month_pct": facts["cpi_month_over_month_pct"],
        "cpi_year_over_year_pct": facts["cpi_year_over_year_pct"],
        "average_twelve_month_inflation_pct": facts[
            "average_twelve_month_inflation_pct"
        ],
        "hicp_preliminary_excluded": True,
        "calendar_source_url": CALENDAR_URL,
        "publication_source_url": PUBLICATION_URL,
        "calendar_source_bytes": CALENDAR_ARCHIVE_BYTES,
        "calendar_source_sha256": CALENDAR_ARCHIVE_SHA256,
        "publication_source_bytes": PUBLICATION_ARCHIVE_BYTES,
        "publication_source_sha256": PUBLICATION_ARCHIVE_SHA256,
        "observation_manifest_path": OBSERVATION_MANIFEST_PATH,
        "observation_manifest_sha256": OBSERVATION_MANIFEST_SHA256,
        "observation_id": observation["observation_id"],
        **EXPECTED_GUARDS,
    }
    return record


def validate_historical_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """Refuse any caller mutation, including chronology or safety promotion."""

    expected = build_historical_record()
    if not _typed_equal(record, expected):
        raise CzechCpiFinalExactV1Error("historical_record_typed_content_mismatch")
    return dict(record)


__all__ = [
    "CALENDAR_ARCHIVE_PATH",
    "CALENDAR_ARCHIVE_SHA256",
    "CALENDAR_OBSERVED_UTC",
    "CALENDAR_URL",
    "COHORT_ID",
    "COLLECTION_CLASS",
    "CONTENT_GUID",
    "CONTRACT_ID",
    "CONTRACT_MANIFEST_PATH",
    "CzechCpiFinalExactV1Error",
    "EVENT_GUID",
    "EXPECTED_FACTS",
    "OBSERVATION_MANIFEST_PATH",
    "OBSERVATION_MANIFEST_SHA256",
    "PRODUCT_CODE",
    "PUBLICATION_ARCHIVE_PATH",
    "PUBLICATION_ARCHIVE_SHA256",
    "PUBLICATION_OBSERVED_UTC",
    "PUBLICATION_URL",
    "SCHEMA_VERSION",
    "SCHEDULED_UTC",
    "SOURCE_ID",
    "SOURCE_REPORTED_START",
    "SUPPORTED_EXECUTION_DECISION",
    "TIMEZONE_NAME",
    "build_historical_record",
    "load_contract_manifest",
    "parse_calendar_api",
    "parse_publication",
    "validate_historical_record",
]
