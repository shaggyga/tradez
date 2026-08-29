from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from src.forex_system.ingestion import czech_cpi_final_exact_v1 as source
from src.forex_system.ingestion.czech_cpi_final_exact_v1 import (
    CALENDAR_ARCHIVE_PATH,
    CALENDAR_ARCHIVE_SHA256,
    CALENDAR_URL,
    COLLECTION_CLASS,
    CzechCpiFinalExactV1Error,
    EXPECTED_FACTS,
    PUBLICATION_ARCHIVE_PATH,
    PUBLICATION_ARCHIVE_SHA256,
    PUBLICATION_URL,
    SCHEDULED_UTC,
    SOURCE_REPORTED_START,
    TIMEZONE_NAME,
    build_historical_record,
    load_contract_manifest,
    parse_calendar_api,
    parse_publication,
    validate_historical_record,
)


ROOT = Path(__file__).resolve().parent
CALENDAR_BYTES = (ROOT / CALENDAR_ARCHIVE_PATH).read_bytes()
PUBLICATION_BYTES = (ROOT / PUBLICATION_ARCHIVE_PATH).read_bytes()


def calendar_value() -> dict:
    return json.loads(CALENDAR_BYTES)


def test_exact_archives_parse_as_late_observed_counterfactual_only() -> None:
    contract = load_contract_manifest()
    record = build_historical_record()
    assert contract["enabled"] is False
    assert contract["registered_with_live_collector"] is False
    assert contract["runtime_supported"] is False
    assert record["collection_class"] == COLLECTION_CLASS
    assert record["prospective_clock"] is False
    assert record["prospective_observation"] is False
    assert record["scheduled_utc"] == SCHEDULED_UTC
    assert record["source_reported_start"] == SOURCE_REPORTED_START
    assert record["source_reported_offset"] == "+02:00"
    assert record["timezone"] == TIMEZONE_NAME
    assert record["calendar_first_observed_utc"] > record["scheduled_utc"]
    assert record["publication_first_observed_utc"] > record["scheduled_utc"]
    assert record["cpi_month_over_month_pct"] == 0.6
    assert record["cpi_year_over_year_pct"] == 1.7
    assert record["average_twelve_month_inflation_pct"] == 2.0
    assert record["hicp_preliminary_excluded"] is True
    assert record["calendar_source_sha256"] == CALENDAR_ARCHIVE_SHA256
    assert record["publication_source_sha256"] == PUBLICATION_ARCHIVE_SHA256
    for field in (
        "direction",
        "consensus",
        "surprise",
    ):
        assert record[field] is None
    for field in (
        "proof_eligible",
        "confirmation_eligible",
        "promotion_eligible",
        "authorization_eligible",
        "execution_eligible",
        "can_place_orders",
    ):
        assert record[field] is False
    assert record["supported_execution_decision"] == "no_trade"


def test_calendar_binds_exact_official_event_time_offset_and_timezone() -> None:
    parsed = parse_calendar_api(CALENDAR_BYTES)
    assert parsed == {
        "event_guid": "04cce34c-6553-4817-899f-ab353685d990",
        "content_guid": "e5d9d9bb-6f20-4049-a1d0-b7b1edb279c1",
        "product_code": "012024-26",
        "publication_url": PUBLICATION_URL,
        "source_reported_start": SOURCE_REPORTED_START,
        "source_reported_offset": "+02:00",
        "timezone": TIMEZONE_NAME,
        "scheduled_utc": SCHEDULED_UTC,
    }


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("prospective_clock", True),
        ("prospective_observation", True),
        ("collection_class", "prospective_clock"),
        ("calendar_first_observed_utc", "2026-08-11T06:59:59Z"),
        ("fact_first_observed_utc", "2026-08-11T07:00:00Z"),
        ("proof_eligible", True),
        ("direction", "buy_CZK"),
        ("consensus", 1.6),
        ("surprise", 0.1),
    ],
)
def test_chronology_and_safety_cannot_be_reclassified(
    field: str, replacement: object
) -> None:
    record = build_historical_record()
    record[field] = replacement
    with pytest.raises(CzechCpiFinalExactV1Error, match="typed_content_mismatch"):
        validate_historical_record(record)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("guid", "00000000-0000-0000-0000-000000000000"),
        ("odkaz", "https://example.com/not-official"),
        ("guidObsah", "00000000-0000-0000-0000-000000000000"),
        ("kodProduktu", "forged"),
        ("zacatek", "2026-08-11T09:00:00+01:00"),
    ],
)
def test_calendar_source_identity_substitution_is_rejected(
    field: str, replacement: object
) -> None:
    value = calendar_value()
    event = value["seznam"][0]["udalosti"][0]
    assert event["guid"] == "04cce34c-6553-4817-899f-ab353685d990"
    event[field] = replacement
    with pytest.raises(CzechCpiFinalExactV1Error):
        parse_calendar_api(json.dumps(value, separators=(",", ":")).encode())


def test_duplicate_event_and_duplicate_json_key_are_rejected() -> None:
    value = calendar_value()
    value["seznam"][0]["udalosti"].append(
        dict(value["seznam"][0]["udalosti"][0])
    )
    with pytest.raises(CzechCpiFinalExactV1Error, match="not_unique"):
        parse_calendar_api(json.dumps(value, separators=(",", ":")).encode())
    duplicated_key = CALENDAR_BYTES.replace(
        b'{"pocetCelkem":55,', b'{"pocetCelkem":55,"pocetCelkem":55,', 1
    )
    with pytest.raises(CzechCpiFinalExactV1Error, match="duplicate_key"):
        parse_calendar_api(duplicated_key)


def test_nonfinite_bool_int_confusion_and_extra_fields_are_rejected() -> None:
    nonfinite = CALENDAR_BYTES.replace(b'"pocetCelkem":55', b'"pocetCelkem":NaN', 1)
    with pytest.raises(CzechCpiFinalExactV1Error, match="nonfinite"):
        parse_calendar_api(nonfinite)
    value = calendar_value()
    value["pocetCelkem"] = True
    with pytest.raises(CzechCpiFinalExactV1Error, match="not_exact_int"):
        parse_calendar_api(json.dumps(value, separators=(",", ":")).encode())
    value = calendar_value()
    value["seznam"][0]["udalosti"][0]["forged_safety"] = False
    with pytest.raises(CzechCpiFinalExactV1Error, match="schema_mismatch"):
        parse_calendar_api(json.dumps(value, separators=(",", ":")).encode())


def test_publication_extracts_only_final_cpi_scope_and_rejects_ambiguity() -> None:
    assert parse_publication(PUBLICATION_BYTES) == EXPECTED_FACTS
    forged = PUBLICATION_BYTES.replace(
        b"</body>",
        b"Consumer prices in July increased by 9.9%, month-on-month.</body>",
        1,
    )
    with pytest.raises(CzechCpiFinalExactV1Error, match="missing_or_ambiguous"):
        parse_publication(forged)


def test_contract_and_archive_paths_cannot_be_substituted(tmp_path: Path) -> None:
    copied_contract = tmp_path / "contract.json"
    copied_contract.write_bytes((ROOT / source.CONTRACT_MANIFEST_PATH).read_bytes())
    with pytest.raises(CzechCpiFinalExactV1Error, match="path_substitution"):
        load_contract_manifest(copied_contract)
    copied_archive = tmp_path / "calendar.json"
    copied_archive.write_bytes(CALENDAR_BYTES)
    expected = ROOT / CALENDAR_ARCHIVE_PATH
    with pytest.raises(CzechCpiFinalExactV1Error, match="path_substitution"):
        source._read_exact_pinned_file(
            copied_archive,
            expected_path=expected,
            expected_bytes=len(CALENDAR_BYTES),
            expected_sha256=CALENDAR_ARCHIVE_SHA256,
            require_read_only=False,
        )


def test_symlink_or_hardlink_alias_is_rejected(tmp_path: Path) -> None:
    copied = tmp_path / "copied.json"
    copied.write_bytes(CALENDAR_BYTES)
    hardlink = tmp_path / "hardlink.json"
    os.link(copied, hardlink)
    with pytest.raises(CzechCpiFinalExactV1Error, match="hardlink_forbidden"):
        source._read_exact_pinned_file(
            copied,
            expected_path=copied,
            expected_bytes=len(CALENDAR_BYTES),
            expected_sha256=CALENDAR_ARCHIVE_SHA256,
            require_read_only=False,
        )
    hardlink.unlink()
    symlink = tmp_path / "symlink.json"
    try:
        symlink.symlink_to(copied)
    except OSError:
        return
    with pytest.raises(CzechCpiFinalExactV1Error, match="symlink_forbidden"):
        source._read_exact_pinned_file(
            symlink,
            expected_path=symlink,
            expected_bytes=len(CALENDAR_BYTES),
            expected_sha256=CALENDAR_ARCHIVE_SHA256,
            require_read_only=False,
        )


def test_writeable_archive_and_read_race_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    copied = tmp_path / "copied.json"
    copied.write_bytes(CALENDAR_BYTES)
    with pytest.raises(CzechCpiFinalExactV1Error, match="not_read_only"):
        source._read_exact_pinned_file(
            copied,
            expected_path=copied,
            expected_bytes=len(CALENDAR_BYTES),
            expected_sha256=CALENDAR_ARCHIVE_SHA256,
            require_read_only=True,
        )
    original_identity = source._file_identity
    calls = 0

    def raced_identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
        nonlocal calls
        calls += 1
        actual = original_identity(value)
        if calls >= 4:
            return actual[:3] + (actual[3] + 1,) + actual[4:]
        return actual

    monkeypatch.setattr(source, "_file_identity", raced_identity)
    with pytest.raises(CzechCpiFinalExactV1Error, match="changed_while_reading"):
        source._read_exact_pinned_file(
            copied,
            expected_path=copied,
            expected_bytes=len(CALENDAR_BYTES),
            expected_sha256=CALENDAR_ARCHIVE_SHA256,
            require_read_only=False,
        )


def test_source_urls_are_exact_and_no_live_or_execution_surface_exists() -> None:
    record = build_historical_record()
    assert record["calendar_source_url"] == CALENDAR_URL
    assert record["publication_source_url"] == PUBLICATION_URL
    module_text = Path(source.__file__).read_text(encoding="utf-8")
    for forbidden in (
        "import requests",
        "import sqlite3",
        "create_order",
        "submit_order",
        "FOREX_ALLOW_LIVE",
        "FOREX_LIVE_EXECUTE",
    ):
        assert forbidden not in module_text
