from pathlib import Path

import trad.oanda_official_document_sla as sla


def row(first_seen, *, detail="", enriched=False, archive=""):
    return {
        "event_id": first_seen,
        "event_lineage_id": "boj-opinions",
        "source_id": "boj_updates",
        "source_verified": True,
        "official_policy_release": True,
        "policy_document_type": "summary_of_opinions",
        "headline": "Summary of Opinions",
        "source_url": "https://www.boj.or.jp/opinions.pdf#page=1",
        "published_utc": "2026-08-09T23:50:00+00:00",
        "first_seen_utc": first_seen,
        "detail_available_utc": detail,
        "detail_enriched": enriched,
        "detail_archive_path": archive,
    }


def test_versions_are_deduplicated_and_listing_detail_latency_are_separate(tmp_path: Path):
    archive = tmp_path / "document.pdf"
    archive.write_bytes(b"pdf")
    payload = sla.build_document_sla(
        [
            row("2026-08-09T23:50:18+00:00"),
            row(
                "2026-08-10T05:14:58+00:00",
                detail="2026-08-10T14:17:04+00:00",
                enriched=True,
                archive=str(archive),
            ),
        ],
        prospective_start_utc="2026-08-01T00:00:00+00:00",
    )
    assert payload["counts"]["documents"] == 1
    document = payload["documents"][0]
    assert document["listing_latency_sec"] == 18.0
    assert document["listing_sla_met"] is True
    assert document["detail_latency_sec"] == 52024.0
    assert document["detail_sla_met"] is False
    assert document["archive_present"] is True
    assert document["version_rows"] == 2
    assert {alarm["type"] for alarm in payload["alarms"]} == {"detail_sla_miss"}


def test_missing_detail_is_explicit_alarm():
    payload = sla.build_document_sla(
        [row("2026-08-09T23:55:00+00:00")],
        prospective_start_utc="2026-08-01T00:00:00+00:00",
    )
    document = payload["documents"][0]
    assert document["listing_sla_met"] is False
    assert document["detail_enriched"] is False
    assert {alarm["type"] for alarm in payload["alarms"]} == {
        "listing_sla_miss", "detail_missing"
    }
    assert payload["can_place_orders"] is False


def test_pre_governance_back_catalog_is_diagnostic_not_an_sla_alarm():
    payload = sla.build_document_sla(
        [row("2026-08-09T23:55:00+00:00")],
        prospective_start_utc="2026-08-10T00:00:00+00:00",
    )
    assert payload["counts"]["prospective_documents"] == 0
    assert payload["counts"]["historical_diagnostic_documents"] == 1
    assert payload["alarms"] == []


def test_retired_source_document_is_diagnostic_not_current_alarm():
    payload = sla.build_document_sla(
        [row("2026-08-10T00:10:00+00:00")],
        prospective_start_utc="2026-08-01T00:00:00+00:00",
        active_source_ids={"replacement_source"},
    )
    document = payload["documents"][0]
    assert document["source_active"] is False
    assert document["prospective_sla_eligible"] is False
    assert payload["alarms"] == []


def test_future_policy_clock_is_not_a_missing_document_alarm():
    scheduled = row("2026-08-16T05:40:55+00:00")
    scheduled["scheduled_utc"] = "2026-09-17T12:00:00+00:00"
    payload = sla.build_document_sla(
        [scheduled],
        prospective_start_utc="2026-08-01T00:00:00+00:00",
        as_of_utc="2026-08-16T10:00:00+00:00",
    )

    document = payload["documents"][0]
    assert document["future_schedule_placeholder"] is True
    assert document["prospective_sla_eligible"] is False
    assert payload["counts"]["future_schedule_placeholders"] == 1
    assert payload["alarms"] == []
