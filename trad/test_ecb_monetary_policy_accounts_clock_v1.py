import datetime as dt
import json
from pathlib import Path

import oanda_local_news_sentiment as news


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "news_sources_v1.json"
SOURCE_ID = "ecb_monetary_policy_accounts_calendar_2026"
UTC = dt.timezone.utc


def _source() -> dict:
    payload = json.loads(CONFIG.read_text(encoding="utf-8"))
    matches = [
        row for row in payload["sources"] if row.get("source_id") == SOURCE_ID
    ]
    assert len(matches) == 1
    return matches[0]


def _configured_source(source_id: str) -> dict:
    payload = json.loads(CONFIG.read_text(encoding="utf-8"))
    matches = [row for row in payload["sources"] if row.get("source_id") == source_id]
    assert len(matches) == 1
    return matches[0]


def test_ecb_accounts_clock_is_exact_official_and_research_only():
    source = _source()
    assert source["url"] == (
        "https://www.ecb.europa.eu/press/calendars/weekly/html/index.en.html"
    )
    assert source["trusted_domains"] == ["ecb.europa.eu"]
    assert source["direct"] is True
    assert source["directional_research_only"] is True

    rows = news.build_recurring_release_calendar(
        source,
        now=dt.datetime(2026, 8, 27, 10, 30, tzinfo=UTC),
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["scheduled_utc"] == "2026-08-27T11:30:00+00:00"
    assert row["event_series_id"] == "ecb_monetary_policy_accounts"
    assert row["actual_value"] is None
    assert row["consensus_value"] is None
    assert row["source_role"] == "primary_policy_calendar"


def test_ecb_accounts_clock_cannot_publish_a_direction_or_execute():
    source = _source()
    row = news.build_recurring_release_calendar(
        source,
        now=dt.datetime(2026, 8, 27, 10, 30, tzinfo=UTC),
    )[0]
    classified = news.classify_article(
        row,
        first_seen=dt.datetime(2026, 8, 27, 10, 30, tzinfo=UTC),
    )
    assert classified["structured_event"] is True
    assert classified["scheduled_utc"] == "2026-08-27T11:30:00+00:00"
    assert classified["actual_value"] is None
    assert classified["consensus_value"] is None
    assert classified["directional_publish_eligible"] is False
    assert classified["execution_eligible"] is False
    assert classified["can_place_orders"] is False


def test_ecb_press_transport_has_bounded_release_detection_cadence():
    source = _configured_source("ecb_press")
    assert source["poll_interval_sec"] == 60
    assert source["detail_enrichment"] == "official_document_text"
    assert "press/accounts/2026" in source["detail_context_url_patterns"]
    assert "ecb\\.mg26" in source["detail_context_url_patterns"]
    assert source["source_contract_id"] == (
        "ecb_press_policy_and_accounts_context_archive_v2_20260827"
    )
    assert source["source_cohort_id"] == source["source_contract_id"]
    assert source["trusted_domains"] == ["ecb.europa.eu"]


def test_ecb_press_exact_event_burst_is_dated_and_cannot_repeat_daily():
    source = _configured_source("ecb_press")
    assert source["burst_poll_interval_sec"] == 15
    assert source["poll_timezone"] == "UTC"
    assert news.burst_poll_active(
        source,
        dt.datetime(2026, 8, 27, 11, 30, tzinfo=UTC),
    )
    assert not news.burst_poll_active(
        source,
        dt.datetime(2026, 8, 28, 11, 30, tzinfo=UTC),
    )


def test_ecb_accounts_live_detail_body_is_retained_from_exact_official_host(
    monkeypatch,
):
    """Prove that today's accounts URL reaches the official-body parser.

    This is a transport/retention assertion only.  The body is not treated as
    a causal surprise, execution signal, or authorization.
    """

    body = (
        b"<html><body><main><h1>Account of the monetary policy meeting</h1>"
        b"<p>Members discussed the inflation outlook, the transmission of "
        b"monetary policy and the balance of risks. The account records the "
        b"discussion held on 22-23 July 2026 without supplying a pre-release "
        b"market consensus or a contemporaneous rates benchmark.</p>"
        b"</main></body></html>"
    )

    class Response:
        headers = {"Content-Type": "text/html; charset=utf-8"}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return (
                "https://www.ecb.europa.eu/press/accounts/2026/html/"
                "ecb.mg260827~fixture.en.html"
            )

        def read(self, _limit):
            return body

    monkeypatch.setattr(
        news.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: Response(),
    )
    source = _configured_source("ecb_press")
    article = {
        "title": "Account of the monetary policy meeting of 22-23 July 2026",
        "summary": "",
        "url": (
            "https://www.ecb.europa.eu/press/accounts/2026/html/"
            "ecb.mg260827~fixture.en.html"
        ),
        "published_utc": "2026-08-27T11:30:00+00:00",
        "source_listing_new_item": True,
        "source_listing_bootstrap": False,
    }
    enriched, first_seen_by_url, error = (
        news.enrich_recent_official_release_details(
            [article],
            source,
            {},
            timeout_sec=5,
            maximum_bytes=1_000_000,
            now=dt.datetime(2026, 8, 27, 11, 30, 15, tzinfo=UTC),
        )
    )
    assert enriched == 1
    assert error == ""
    assert article["detail_enriched"] is True
    assert article["detail_enrichment_kind"] == "official_html_text"
    assert "inflation outlook" in article["summary"]
    assert article["detail_source_url"].startswith(
        "https://www.ecb.europa.eu/press/accounts/2026/"
    )
    assert list(first_seen_by_url.values()) == [
        "2026-08-27T11:30:15+00:00"
    ]
