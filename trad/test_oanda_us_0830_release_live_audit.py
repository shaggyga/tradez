import datetime as dt
import json
import sqlite3

import oanda_us_0830_release_live_audit as audit


DOL_FIXTURE = """
TRANSMISSION OF MATERIALS IN THIS RELEASE IS EMBARGOED UNTIL
8:30 A.M. (Eastern) Thursday, August 27, 2026
UNEMPLOYMENT INSURANCE WEEKLY CLAIMS
SEASONALLY ADJUSTED DATA
In the week ending August 22, the advance figure for seasonally adjusted initial claims was 211,000, an increase of 5,000
from the previous week's revised level. The previous week's level was revised down by 1,000 from 207,000 to 206,000.
The advance number for seasonally adjusted insured unemployment during the week ending August 15 was 1,805,000, an
increase of 6,000 from the previous week's revised level. The previous week's level was revised up 2,000 from 1,797,000
to 1,799,000.
"""


def test_atomic_write_preserves_hashed_lf_bytes_on_windows(tmp_path):
    output = tmp_path / "artifact.json"
    text = "{\n  \"status\": \"ok\"\n}\n"
    audit.atomic_write(output, text)
    assert output.read_bytes() == text.encode("utf-8")


def test_dol_parser_is_narrow_and_audit_only():
    parsed = audit.parse_dol_claims_text(DOL_FIXTURE)
    assert parsed["release_date_text"] == "August 27, 2026"
    assert parsed["initial_claims"] == {
        "week_ending": "August 22",
        "actual": 211000,
        "previous_revised": 206000,
        "previous_unrevised": 207000,
        "change": 5000,
        "revision": -1000,
    }
    assert parsed["continued_claims"]["actual"] == 1805000
    assert parsed["continued_claims"]["revision"] == 2000
    assert parsed["consensus"] is None
    assert parsed["parser_scope"] == "independent_audit_parser_not_shared_with_runtime"


def test_dol_parser_rejects_non_allowlisted_shape():
    assert audit.parse_dol_claims_text("Initial claims were 211,000") == {}


def test_independent_dol_parser_handles_today_optional_by_and_insured_rate():
    today = """
    TRANSMISSION OF MATERIALS IN THIS RELEASE IS EMBARGOED UNTIL
    8:30 A.M. (Eastern) Thursday, August 27, 2026
    UNEMPLOYMENT INSURANCE WEEKLY CLAIMS
    In the week ending August 22, the advance figure for seasonally adjusted initial
    claims was 203,000, a decrease of 4,000 from the previous week's revised level.
    The previous week's level was revised up by 1,000 from 206,000 to 207,000.
    The advance seasonally adjusted insured unemployment rate was 1.2 percent for
    the week ending August 15, unchanged from the previous week's unrevised rate.
    The advance number for seasonally adjusted insured unemployment during the week
    ending August 15 was 1,778,000, a decrease of 18,000 from the previous week's
    revised level. The previous week's level was revised down by 3,000 from
    1,799,000 to 1,796,000.
    """
    parsed = audit.parse_dol_claims_text(today)
    assert parsed["continued_claims"] == {
        "week_ending": "August 15",
        "actual": 1778000,
        "previous_revised": 1796000,
        "previous_unrevised": 1799000,
        "change": -18000,
        "revision": -3000,
    }
    assert parsed["insured_unemployment_rate"] == {
        "week_ending": "August 15",
        "actual": 1.2,
        "previous_unrevised": 1.2,
        "change": 0.0,
    }


def test_census_rss_selects_only_exact_release_clock():
    payload = b"""<?xml version='1.0'?><rss><channel>
    <item><title>Advance Wholesale Inventories</title><link>https://www.census.gov/econ/indicators/</link>
    <description>July 2026: +0.3 % Change</description><guid>adv_mwts</guid>
    <pubDate>Thu, 27 Aug 2026 08:30:00 -0400</pubDate></item>
    <item><title>Old</title><link>https://www.census.gov/</link><description>old</description><guid>old</guid>
    <pubDate>Wed, 26 Aug 2026 08:30:00 -0400</pubDate></item>
    </channel></rss>"""
    rows = audit.parse_census_rss(payload)
    assert len(rows) == 1
    assert rows[0]["guid"] == "adv_mwts"
    assert rows[0]["published_utc"] == "2026-08-27T12:30:00Z"


def test_exact_path_uses_exact_number_of_completed_m1_bars():
    rows = []
    for minute in range(5):
        rows.append(
            {
                "timestamp": audit.EVENT_UTC + dt.timedelta(minutes=minute),
                "bid_open": 1.0 + minute * 0.001,
                "ask_open": 1.0002 + minute * 0.001,
                "bid_close": 1.0008 + minute * 0.001,
                "ask_close": 1.0010 + minute * 0.001,
            }
        )
    one = audit.exact_path(rows, 1)
    five = audit.exact_path(rows, 5)
    assert one is not None and one["end_utc"] == "2026-08-27T12:31:00Z"
    assert five is not None and five["end_utc"] == "2026-08-27T12:35:00Z"
    assert audit.exact_path(rows, 15) is None


def test_quote_endpoint_path_carries_quiet_quote_and_reports_age():
    event = audit.EVENT_UTC.timestamp()
    rows = [
        {
            "last_epoch": event - 10.0,
            "close_bid": 1.0,
            "close_ask": 1.0002,
            "pip": 0.0001,
        },
        {
            "last_epoch": event + 20.0,
            "close_bid": 1.001,
            "close_ask": 1.0012,
            "pip": 0.0001,
        },
    ]
    path = audit.quote_endpoint_path(rows, 1)
    assert path is not None
    assert path["entry_quote_age_sec"] == 10.0
    assert path["exit_quote_age_sec"] == 40.0
    assert path["bid_open"] == 1.0
    assert path["bid_close"] == 1.001


def test_report_contract_cannot_claim_execution():
    assert audit.CONTRACT_ID.endswith("20260827")
    assert audit.CUTOFF_UTC == dt.datetime(2026, 8, 27, 13, 0, tzinfo=dt.timezone.utc)
    generated = dt.datetime(2026, 8, 27, 13, 1, tzinfo=dt.timezone.utc)
    assert audit.horizon_unmatured_reason(30, generated) is None
    assert audit.horizon_unmatured_reason(60, generated) == (
        "60m_unavailable_at_0900_cutoff"
    )


def test_restart_recovers_prior_probe_signatures_without_rewriting_history():
    baseline = {
        "record_kind": "source_probe",
        "source": "dol",
        "status": 200,
        "content_sha256": "old-hash",
        "last_modified": "Thu, 20 Aug 2026 12:29:59 GMT",
        "event_items": None,
        "release": {"initial_claims": {"actual": 206000}},
        "error": "",
        "observed_utc": "2026-08-27T11:12:50Z",
        "material_change": True,
    }
    duplicate_after_restart = {
        **baseline,
        "observed_utc": "2026-08-27T11:50:33Z",
    }
    signatures = audit.recovered_probe_signatures(
        [baseline, duplicate_after_restart]
    )
    assert signatures == {"dol": audit.material_probe_signature(baseline)}
    assert audit.material_probe_signature(duplicate_after_restart) == signatures["dol"]


def test_dol_parser_repair_does_not_create_a_material_source_revision():
    original = {
        "record_kind": "source_probe",
        "source": "dol",
        "status": 200,
        "content_sha256": "same-official-pdf",
        "last_modified": "Thu, 27 Aug 2026 12:29:59 GMT",
        "release": {"initial_claims": {"actual": 203000}},
        "error": "",
    }
    repaired_parser_view = {
        **original,
        "release": {
            "initial_claims": {"actual": 203000},
            "continued_claims": {"actual": 1778000},
            "insured_unemployment_rate": {"actual": 1.2},
        },
    }
    assert audit.material_probe_signature(original) == audit.material_probe_signature(
        repaired_parser_view
    )


def test_knowledge_clock_summary_keeps_surfaces_separate():
    census = [{
        "observed_utc": "2026-08-27T12:30:00.500000Z",
        "event_items": [{"published_utc": "2026-08-27T12:30:00Z"}],
    }]
    dol = [{
        "observed_utc": "2026-08-27T12:30:01.000000Z",
        "content_sha256": "pdf",
        "release": {"release_date_text": "August 27, 2026"},
    }]
    local = [{
        "observation_surface": "official_release_fast_lane_v4",
        "source_id": "dol_eta_ui_claims_scheduled_direct_pdf_v1",
        "detail_available_utc": "2026-08-27T12:30:08Z",
        "first_seen_utc": "2026-08-27T12:30:10Z",
    }]
    summary = audit.knowledge_clock_summary(census, dol, local)
    assert summary["independent_official_probe"]["census"]["latency_sec"] == 0.5
    assert summary["independent_official_probe"]["dol"]["latency_sec"] == 1.0
    assert summary["production"]["dol_fast_lane"]["detail_latency_sec"] == 8.0
    assert summary["production"]["dol_fast_lane"]["first_seen_latency_sec"] == 10.0
    assert summary["production"]["dol_broad_collector"] is None


def test_census_materiality_uses_target_items_not_alternating_feed_bytes():
    first = {
        "record_kind": "source_probe",
        "source": "census",
        "status": 200,
        "content_sha256": "edge-a",
        "event_items": [],
        "error": "",
    }
    alternate = {**first, "content_sha256": "edge-b"}
    released = {
        **alternate,
        "event_items": [{"guid": "adv_mwts", "published_utc": "2026-08-27T12:30:00Z"}],
    }
    assert audit.material_probe_signature(first) == audit.material_probe_signature(alternate)
    assert audit.material_probe_signature(released) != audit.material_probe_signature(first)


def test_local_case_query_includes_new_direct_dol_lane(tmp_path, monkeypatch):
    news = tmp_path / "local_news_sentiment"
    news.mkdir()
    database = news / "local_news_sentiment_v1.sqlite"
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            """
            CREATE TABLE articles (
                event_id TEXT, source_id TEXT, published_utc TEXT,
                first_seen_utc TEXT, last_seen_utc TEXT, headline TEXT,
                summary TEXT, source_url TEXT, payload_json TEXT
            )
            """
        )
        payload = {
            "actual_value": 211000,
            "previous_value": 206000,
            "consensus_value": None,
            "event_series_id": "dol_ui_weekly_claims_bundle",
            "reference_period": "2026-08-22",
            "source_native_components": {
                "initial_claims_sa": {"actual": 211000, "previous_revised": 206000}
            },
            "document_revision_number": 1,
            "is_material_revision": False,
            "directional_publish_eligible": False,
            "execution_eligible": False,
        }
        connection.execute(
            "INSERT INTO articles VALUES (?,?,?,?,?,?,?,?,?)",
            (
                "event", "dol_eta_ui_claims_scheduled_direct_pdf_v1",
                "2026-08-27T12:30:00Z", "2026-08-27T12:30:05Z",
                "2026-08-27T12:30:05Z", "Weekly claims", "summary",
                "https://www.dol.gov/ui/data.pdf", json.dumps(payload),
            ),
        )
        connection.commit()
    finally:
        connection.close()
    monkeypatch.setattr(audit, "NEWS", news)
    rows = audit.local_news_records()
    assert len(rows) == 1
    assert rows[0]["source_id"] == "dol_eta_ui_claims_scheduled_direct_pdf_v1"
    assert rows[0]["actual_value"] == 211000
    assert rows[0]["consensus_value"] is None
    assert rows[0]["reference_period"] == "2026-08-22"
    assert rows[0]["source_native_components"]["initial_claims_sa"]["actual"] == 211000
    assert rows[0]["document_revision_number"] == 1
    assert rows[0]["directional_publish_eligible"] is False


def test_fast_lane_case_query_preserves_exact_observation_clock(tmp_path, monkeypatch):
    news = tmp_path / "local_news_sentiment"
    news.mkdir()
    database = news / "official_release_fast_lane_v4.sqlite"
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            """
            CREATE TABLE official_release_observation (
                observation_id TEXT, source_id TEXT, first_seen_utc TEXT,
                material_sha256 TEXT, raw_payload_json TEXT,
                prospective_observation INTEGER, listing_bootstrap INTEGER,
                observation_clock_trusted INTEGER, collector_contract_id TEXT
            )
            """
        )
        payload = {
            "published_utc": "2026-08-27T12:30:00+00:00",
            "title": "United States Unemployment Insurance Weekly Claims",
            "actual_value": 211000,
            "previous_value": 206000,
            "consensus_value": None,
            "event_series_id": "dol_ui_weekly_claims_bundle",
            "reference_period": "2026-08-22",
            "source_native_components": {
                "initial_claims_sa": {"actual": 211000, "previous_revised": 206000}
            },
            "execution_eligible": False,
        }
        connection.execute(
            "INSERT INTO official_release_observation VALUES (?,?,?,?,?,?,?,?,?)",
            (
                "observation", "dol_eta_ui_claims_scheduled_direct_pdf_v1",
                "2026-08-27T12:30:04.125000+00:00", "hash", json.dumps(payload),
                1, 0, 1, "official_release_fast_lane_v4_test",
            ),
        )
        connection.commit()
    finally:
        connection.close()
    monkeypatch.setattr(audit, "NEWS", news)
    rows = audit.fast_lane_records()
    assert len(rows) == 1
    assert rows[0]["observation_surface"] == "official_release_fast_lane_v4"
    assert rows[0]["first_seen_utc"] == "2026-08-27T12:30:04.125000+00:00"
    assert rows[0]["actual_value"] == 211000
    assert rows[0]["consensus_value"] is None
    assert rows[0]["observation_clock_trusted"] is True
    assert rows[0]["execution_eligible"] is False
