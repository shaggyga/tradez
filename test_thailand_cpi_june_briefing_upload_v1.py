from __future__ import annotations

import io
import json
import os
from pathlib import Path

import pytest
from pypdf import PdfReader

from src.forex_system.ingestion import thailand_cpi_june_briefing_upload_v1 as source
from src.forex_system.ingestion.thailand_cpi_june_briefing_upload_v1 import (
    BRIEFING_ARCHIVE_PATH,
    BRIEFING_ARCHIVE_SHA256,
    BRIEFING_EVENT_TIME_UTC,
    BRIEFING_SOURCE_REPORTED_LOCAL,
    BRIEFING_TIME_SEMANTICS,
    BRIEFING_URL,
    COLLECTION_CLASS,
    EFFECTIVE_KNOWN_UTC,
    EXPECTED_FACTS,
    OPEN_DATA_CONTRACT_PRECISION,
    PDF_ARCHIVE_PATH,
    PDF_ARCHIVE_SHA256,
    PDF_FIRST_SEEN_UTC,
    PDF_LAST_MODIFIED_SEMANTICS,
    PDF_SERVER_LAST_MODIFIED_UTC,
    PDF_URL,
    ThailandCpiJuneBriefingUploadV1Error,
    build_historical_record,
    load_contract_manifest,
    parse_briefing_page,
    parse_publication_pdf,
    validate_historical_record,
)


ROOT = Path(__file__).resolve().parent
BRIEFING_BYTES = (ROOT / BRIEFING_ARCHIVE_PATH).read_bytes()
PDF_BYTES = (ROOT / PDF_ARCHIVE_PATH).read_bytes()


def test_exact_archives_build_late_observed_no_trade_diagnostic() -> None:
    contract = load_contract_manifest()
    record = build_historical_record()
    assert contract["enabled"] is False
    assert contract["registered_with_live_collector"] is False
    assert contract["runtime_supported"] is False
    assert record["collection_class"] == COLLECTION_CLASS
    assert record["briefing_source_reported_local"] == BRIEFING_SOURCE_REPORTED_LOCAL
    assert record["briefing_event_time_utc"] == BRIEFING_EVENT_TIME_UTC
    assert record["briefing_time_semantics"] == BRIEFING_TIME_SEMANTICS
    assert record["pdf_server_last_modified_utc"] == PDF_SERVER_LAST_MODIFIED_UTC
    assert record["pdf_last_modified_semantics"] == PDF_LAST_MODIFIED_SEMANTICS
    assert record["pdf_first_seen_utc"] == PDF_FIRST_SEEN_UTC
    assert record["effective_known_utc"] == EFFECTIVE_KNOWN_UTC
    assert record["open_data_contract_precision"] == OPEN_DATA_CONTRACT_PRECISION
    assert record["briefing_source_sha256"] == BRIEFING_ARCHIVE_SHA256
    assert record["publication_source_sha256"] == PDF_ARCHIVE_SHA256
    for key, value in EXPECTED_FACTS.items():
        assert type(record[key]) is type(value)
        assert record[key] == value
    for field in ("direction", "consensus", "surprise"):
        assert record[field] is None
    for field in (
        "prospective_clock",
        "prospective_observation",
        "exact_prospective_cpi_release_clock_closed",
        "proof_eligible",
        "confirmation_eligible",
        "promotion_eligible",
        "authorization_eligible",
        "execution_eligible",
        "can_place_orders",
    ):
        assert record[field] is False
    assert record["supported_execution_decision"] == "no_trade"


def test_briefing_page_clock_is_explicitly_not_release_availability() -> None:
    assert parse_briefing_page(BRIEFING_BYTES) == {
        "source_reported_local": BRIEFING_SOURCE_REPORTED_LOCAL,
        "event_time_utc": BRIEFING_EVENT_TIME_UTC,
        "timezone": "Asia/Bangkok",
        "time_semantics": BRIEFING_TIME_SEMANTICS,
    }


def test_exact_pdf_facts_are_extracted_from_page_one_narrative() -> None:
    assert parse_publication_pdf(PDF_BYTES) == EXPECTED_FACTS
    reader = PdfReader(io.BytesIO(PDF_BYTES))
    assert len(reader.pages) == 4
    assert source._extract_pdf_facts_from_text(
        reader.pages[0].extract_text() or ""
    ) == EXPECTED_FACTS


def test_clock_order_and_effective_known_use_causal_maximum() -> None:
    record = build_historical_record()
    clocks = [
        source._parse_time(record["briefing_event_time_utc"], label="event"),
        source._parse_time(record["pdf_server_last_modified_utc"], label="upload"),
        source._parse_time(record["page_first_seen_utc"], label="page_seen"),
        source._parse_time(record["pdf_first_seen_utc"], label="pdf_seen"),
    ]
    assert clocks == sorted(clocks)
    assert source._parse_time(record["effective_known_utc"], label="known") == max(
        clocks
    )
    assert record["fact_first_seen_utc"] == record["pdf_first_seen_utc"]


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("briefing_time_semantics", "exact_public_release_time"),
        ("pdf_last_modified_semantics", "proven_public_availability"),
        ("effective_known_utc", "2026-07-06T03:30:00Z"),
        ("fact_first_seen_utc", "2026-07-06T04:17:19Z"),
        ("prospective_clock", True),
        ("prospective_observation", True),
        ("exact_prospective_cpi_release_clock_closed", True),
        ("proof_eligible", True),
        ("promotion_eligible", True),
        ("direction", "buy_THB"),
        ("consensus", 2.3),
        ("surprise", 0.12),
    ],
)
def test_chronology_semantics_and_safety_cannot_be_relabelled(
    field: str, replacement: object
) -> None:
    record = build_historical_record()
    record[field] = replacement
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error, match="typed_content_mismatch"
    ):
        validate_historical_record(record)


def test_duplicate_or_missing_briefing_clock_is_rejected() -> None:
    duplicate = BRIEFING_BYTES.replace(
        b"</body>",
        "วันที่ 6 กรกฎาคม 2569 เวลา 10.30 น.</body>".encode("utf-8"),
        1,
    )
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error,
        match="briefing_clock_missing_or_ambiguous",
    ):
        parse_briefing_page(duplicate)
    missing = BRIEFING_BYTES.replace(b"article-detail-173746", b"article-detail-forged")
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error, match="article_identity_missing"
    ):
        parse_briefing_page(missing)


def test_pdf_fact_substitution_and_ambiguity_are_rejected() -> None:
    text = PdfReader(io.BytesIO(PDF_BYTES)).pages[0].extract_text() or ""
    changed = text.replace("102.85", "999.99", 1)
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error, match="exact_facts_changed"
    ):
        source._extract_pdf_facts_from_text(changed)
    index_position = text.index("102.85")
    headline_context = text[max(0, index_position - 220) : index_position + 20]
    ambiguous = text + "\n" + headline_context.replace("102.85", "103.85", 1)
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error, match="missing_or_ambiguous"
    ):
        source._extract_pdf_facts_from_text(ambiguous)


def test_source_urls_domains_and_identity_are_exact() -> None:
    record = build_historical_record()
    assert record["briefing_source_url"] == BRIEFING_URL
    assert record["publication_source_url"] == PDF_URL
    for value, expected, domain in (
        ("https://example.com/forged", BRIEFING_URL, "sisaket.moc.go.th"),
        (PDF_URL + "#forged", PDF_URL, "uploads.tpso.go.th"),
    ):
        with pytest.raises(ThailandCpiJuneBriefingUploadV1Error):
            source._official_url(
                value, expected=expected, domain=domain, label="source"
            )


def test_duplicate_nonfinite_type_and_schema_confusion_are_rejected() -> None:
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error, match="duplicate_key"
    ):
        source._strict_json(b'{"x":1,"x":2}', label="fixture")
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error, match="nonfinite"
    ):
        source._strict_json(b'{"x":NaN}', label="fixture")
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error, match="not_exact_int"
    ):
        source._integer(True, label="fixture")
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error, match="schema_mismatch"
    ):
        source._closed_mapping(
            {"expected": 1, "forged": 2},
            label="fixture",
            fields={"expected"},
        )
    assert source._typed_equal(1, True) is False


def test_contract_and_archive_paths_cannot_be_substituted(tmp_path: Path) -> None:
    copied_contract = tmp_path / "contract.json"
    copied_contract.write_bytes((ROOT / source.CONTRACT_MANIFEST_PATH).read_bytes())
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error, match="path_substitution"
    ):
        load_contract_manifest(copied_contract)
    copied_archive = tmp_path / "briefing.html"
    copied_archive.write_bytes(BRIEFING_BYTES)
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error, match="path_substitution"
    ):
        source._read_exact_pinned_file(
            copied_archive,
            expected_path=ROOT / BRIEFING_ARCHIVE_PATH,
            expected_bytes=len(BRIEFING_BYTES),
            expected_sha256=BRIEFING_ARCHIVE_SHA256,
            require_read_only=False,
        )


def test_symlink_or_hardlink_alias_is_rejected(tmp_path: Path) -> None:
    copied = tmp_path / "copied.html"
    copied.write_bytes(BRIEFING_BYTES)
    hardlink = tmp_path / "hardlink.html"
    os.link(copied, hardlink)
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error, match="hardlink_forbidden"
    ):
        source._read_exact_pinned_file(
            copied,
            expected_path=copied,
            expected_bytes=len(BRIEFING_BYTES),
            expected_sha256=BRIEFING_ARCHIVE_SHA256,
            require_read_only=False,
        )
    hardlink.unlink()
    symlink = tmp_path / "symlink.html"
    try:
        symlink.symlink_to(copied)
    except OSError:
        return
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error, match="symlink_forbidden"
    ):
        source._read_exact_pinned_file(
            symlink,
            expected_path=symlink,
            expected_bytes=len(BRIEFING_BYTES),
            expected_sha256=BRIEFING_ARCHIVE_SHA256,
            require_read_only=False,
        )


def test_writable_archive_and_read_race_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    copied = tmp_path / "copied.html"
    copied.write_bytes(BRIEFING_BYTES)
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error, match="not_read_only"
    ):
        source._read_exact_pinned_file(
            copied,
            expected_path=copied,
            expected_bytes=len(BRIEFING_BYTES),
            expected_sha256=BRIEFING_ARCHIVE_SHA256,
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
    with pytest.raises(
        ThailandCpiJuneBriefingUploadV1Error, match="changed_while_reading"
    ):
        source._read_exact_pinned_file(
            copied,
            expected_path=copied,
            expected_bytes=len(BRIEFING_BYTES),
            expected_sha256=BRIEFING_ARCHIVE_SHA256,
            require_read_only=False,
        )


def test_manifest_records_dynamic_page_and_stable_pdf_honestly() -> None:
    manifest = json.loads((ROOT / source.OBSERVATION_MANIFEST_PATH).read_bytes())
    by_role = {row["role"]: row for row in manifest["raw_payloads"]}
    page = by_role["official_moc_provincial_briefing_page"]
    pdf = by_role["official_tpso_cpi_pdf"]
    assert page["repeat_byte_identical"] is False
    assert page["raw_response_sha256"] != page["repeat_response_sha256"]
    assert pdf["repeat_byte_identical"] is True
    assert pdf["raw_response_sha256"] == pdf["repeat_response_sha256"]


def test_no_live_database_account_or_execution_surface_exists() -> None:
    module_text = Path(source.__file__).read_text(encoding="utf-8")
    for forbidden in (
        "import requests",
        "import sqlite3",
        "create_order",
        "submit_order",
        "FOREX_ALLOW_LIVE",
        "FOREX_LIVE_EXECUTE",
        "account_id",
    ):
        assert forbidden not in module_text
