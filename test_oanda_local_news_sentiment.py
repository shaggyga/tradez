import datetime as dt
import csv
import hashlib
import html
import io
import json
import sqlite3
import threading
import time
import urllib.parse
import zipfile

import pytest

import oanda_local_news_sentiment as news


UTC = dt.timezone.utc


def test_official_document_quality_rejects_maintenance_shell_not_short_release():
    assert news.official_document_quality_issue(
        "Maintenance Sorry, this service is currently unavailable. "
        "Please try accessing the page using a different device or internet browser."
    ) == "maintenance_or_unavailable_page"
    assert news.official_document_quality_issue(
        "The Monetary Policy Committee voted to maintain the policy rate at 3.0 percent."
    ) == ""


def test_classification_quarantines_invalid_detail_body():
    first_seen = news.parse_datetime("2026-08-18T12:00:00Z")
    article = news.classify_article(
        {
            "source_id": "mas_monetary_policy_api_direct_v1",
            "source_name": "Monetary Authority of Singapore",
            "source_kind": "json_api",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["SGD"],
            "title": "MAS Monetary Policy Statement - July 2026",
            "summary": (
                "Maintenance Sorry, this service is currently unavailable. "
                "Try accessing the page using a different device or internet browser."
            ),
            "url": "https://www.mas.gov.sg/news/monetary-policy-statements/example",
            "published_utc": "2026-07-27T04:00:00Z",
            "detail_enriched": True,
            "detail_content_sha256": "a" * 64,
            "detail_content_bytes": 200,
            "detail_text_characters": 150,
        },
        first_seen=first_seen,
    )
    assert article["detail_enriched"] is False
    assert article["detail_quality_state"] == "rejected"
    assert article["detail_quality_reason"] == "maintenance_or_unavailable_page"
    assert article["rejected_detail_content_sha256"] == "a" * 64
    assert article["summary"] == ""
    assert article["currencies"] == ["SGD"]
    assert "TRY" not in article["direct_currencies"]


def test_ons_labour_release_bundle_prioritizes_all_material_bulletins():
    html = """
    <a href='/economy/inflationandpriceindices/bulletins/averageweeklyearningsingreatbritain/august2026'>Wages</a>
    <a href='/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/bulletins/earningsandemploymentfrompayasyouearnrealtimeinformationuk/august2026'>PAYE</a>
    <a href='/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/bulletins/labourmarketoverview/august2026'>Overview</a>
    <a href='/employmentandlabourmarket/peoplenotinwork/unemployment/bulletins/employmentintheuk/august2026'>Employment</a>
    """
    links = news.ons_release_bulletin_links(
        html,
        base_url="https://www.ons.gov.uk/releases/uklabourmarketaugust2026",
        release_title="UK Labour Market: August 2026",
    )
    assert len(links) == 4
    assert "labourmarketoverview" in links[0]
    assert any("earningsandemploymentfrompayasyouearn" in value for value in links)


def test_ons_labour_bundle_is_structured_labor_and_abstains_without_consensus():
    first_seen = dt.datetime(2026, 8, 18, 6, 2, tzinfo=UTC)
    article = news.classify_article(
        {
            "source_id": "ons_published_releases",
            "source_name": "ONS",
            "source_kind": "rss",
            "source_role": "primary_statistical_release",
            "source_verified": True,
            "source_direct": True,
            "source_quality": 1.0,
            "source_currencies": ["GBP"],
            "title": "UK Labour Market: August 2026",
            "summary": (
                "Payrolled employees decreased by 13,000 between June and July. "
                "The ILO unemployment rate was 4.9%. Regular earnings growth was "
                "3.5% and total earnings growth was 4.1%. Vacancies declined by 44,000."
            ),
            "url": "https://www.ons.gov.uk/releases/uklabourmarketaugust2026",
            "published_utc": "2026-08-18T06:00:00Z",
            "detail_enriched": True,
            "detail_enrichment_kind": "ons_release_bundle",
            "detail_available_utc": "2026-08-18T06:02:00Z",
            "numeric_parser_activated_utc": "2026-08-18T06:01:00Z",
        },
        first_seen=first_seen,
    )
    assert article["structured_event"] is True
    assert article["category"] == "labor_release"
    assert article["event_series_id"] == "ons_uk_labour_release_package"
    assert len(article["release_components"]) >= 4
    assert article["directional_publish_eligible"] is False
    assert article["research_currency_scores"]["GBP"] < 0


def test_secondary_cooling_labor_report_cannot_turn_bullish_from_rate_hike_phrase():
    article = news.classify_article(
        {
            "source_id": "finnhub_fx_market_news",
            "source_name": "Finnhub",
            "source_kind": "json",
            "source_verified": False,
            "source_direct": True,
            "source_quality": 0.7,
            "source_currencies": ["GBP"],
            "title": "UK labour market shows further signs of cooling in June",
            "summary": (
                "Payrolls dropped again, vacancies declined and hiring lost momentum. "
                "The report may not put off another Bank of England rate hike."
            ),
            "url": "https://example.com/uk-labour-cooling",
            "published_utc": "2026-08-18T08:22:33Z",
        },
        first_seen=dt.datetime(2026, 8, 18, 8, 31, tzinfo=UTC),
    )
    assert article["category"] == "labor_release"
    assert article["currency_scores"] == {}
    assert article["research_currency_scores"]["GBP"] < 0
    assert article["context_reason"] == "secondary_uncorroborated_semantic_direction"


def test_ongoing_hormuz_status_does_not_rearm_risk_basket():
    article = news.classify_article(
        {
            "source_id": "google_news_systemic_catalyst",
            "source_kind": "rss",
            "source_verified": False,
            "source_quality": 0.65,
            "title": (
                "Strait of Hormuz will remain closed until frozen assets are "
                "released and sanctions are lifted"
            ),
            "summary": "",
            "url": "https://news.google.com/articles/status",
            "published_utc": "2026-08-18T10:05:54Z",
        },
        first_seen=dt.datetime(2026, 8, 18, 10, 13, tzinfo=UTC),
    )
    assert article["category"] != "risk_off_geopolitical_or_financial"
    assert article["currency_scores"] == {}
    assert article["research_currency_scores"] == {}


def test_official_culture_or_cooperation_program_is_not_fx_catalyst():
    article = news.classify_article(
        {
            "source_id": "oman_foreign_ministry",
            "source_kind": "rss",
            "source_verified": True,
            "source_direct": True,
            "source_quality": 1.0,
            "title": "Renewable energy cooperation and cultural festival programme",
            "summary": "Officials discussed a cultural festival and renewable energy cooperation.",
            "url": "https://fm.gov.om/example",
            "published_utc": "2026-08-18T08:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 18, 8, 1, tzinfo=UTC),
    )
    assert article["relevant"] is False
    assert article["currency_scores"] == {}
    assert article["exclusion_reason"] == "official_non_market_program"


def test_hormuz_ship_attack_paraphrases_share_story_cluster_and_clock():
    first = news.classify_article(
        {
            "source_id": "google_news_systemic_catalyst",
            "source_kind": "rss",
            "source_verified": False,
            "source_quality": 0.65,
            "title": "Vessel struck by projectile during Strait of Hormuz transit",
            "url": "https://news.google.com/articles/a",
            "published_utc": "2026-08-18T03:19:19Z",
        },
        first_seen=dt.datetime(2026, 8, 18, 3, 22, tzinfo=UTC),
    )
    second = news.classify_article(
        {
            "source_id": "google_news_systemic_catalyst",
            "source_kind": "rss",
            "source_verified": False,
            "source_quality": 0.65,
            "title": "Ship attacked during Strait of Hormuz transit - Yahoo News Canada",
            "url": "https://news.google.com/articles/b",
            "published_utc": "2026-08-18T11:40:21Z",
        },
        first_seen=dt.datetime(2026, 8, 18, 11, 58, tzinfo=UTC),
    )
    clustered = news.cluster_articles([first, second])
    for article in (first, second):
        assert article["commodity_exporter_direction_state"] == (
            "ambiguous_pending_authoritative_repricing_or_price_reaction"
        )
        assert not ({"CAD", "MXN", "NOK"} & set(article["currency_scores"]))
        assert article["currency_scores"]["JPY"] > 0
    assert len(clustered) == 1
    assert clustered[0]["causal_known_utc"] == first["causal_known_utc"]
    assert clustered[0]["syndicated_article_count"] == 2
    assert clustered[0]["story_cluster_id"].startswith("local_news_")


def test_collector_cycle_heartbeat_is_truthful_and_preserves_prior_summary(tmp_path):
    started = dt.datetime(2026, 8, 17, 12, 0, tzinfo=UTC)
    progress = news.CollectorCycleProgress(started)
    progress.update("collecting_sources", {"completed_sources": 7})
    payload = progress.snapshot()

    assert payload["status"] == "running_cycle"
    assert payload["cycle_in_progress"] is True
    assert payload["phase"] == "collecting_sources"
    assert payload["progress_sequence"] == 1
    assert payload["details"]["completed_sources"] == 7
    assert payload["progress_age_sec"] >= 0
    assert payload["policy"]["research_only"] is True
    assert payload["policy"]["execution_eligible"] is False


def test_publish_collector_cycle_heartbeat_keeps_completed_snapshot_immutable(tmp_path):
    completed = {"generated_utc": "2026-08-17T11:30:00Z", "status": "ok"}
    latest = tmp_path / "collector_latest_v1.json"
    latest.write_text(json.dumps(completed), encoding="utf-8")
    progress = news.CollectorCycleProgress(
        dt.datetime(2026, 8, 17, 12, 0, tzinfo=UTC)
    )
    payload = news.publish_collector_cycle_heartbeat(
        output_root=tmp_path,
        progress=progress,
    )

    assert json.loads((tmp_path / "collector_heartbeat_v1.json").read_text()) == payload
    assert json.loads(latest.read_text()) == completed
    assert payload["status"] == "running_cycle"


def test_stopped_heartbeat_thread_cannot_overwrite_completed_snapshot(tmp_path):
    progress = news.CollectorCycleProgress(
        dt.datetime(2026, 8, 17, 12, 0, tzinfo=UTC)
    )
    stop = threading.Event()
    worker = threading.Thread(
        target=news.run_collector_cycle_heartbeat,
        kwargs={
            "stop_event": stop,
            "output_root": tmp_path,
            "progress": progress,
            "interval_sec": 5.0,
        },
    )
    worker.start()
    deadline = time.monotonic() + 2.0
    while not (tmp_path / "collector_heartbeat_v1.json").exists():
        assert time.monotonic() < deadline
        time.sleep(0.01)
    stop.set()
    worker.join(timeout=2.0)
    assert worker.is_alive() is False

    completed = {"generated_utc": "2026-08-17T12:01:00Z", "status": "ok"}
    (tmp_path / "collector_latest_v1.json").write_text(
        json.dumps(completed), encoding="utf-8"
    )
    time.sleep(0.05)
    assert json.loads((tmp_path / "collector_latest_v1.json").read_text()) == completed


def test_observation_time_never_uses_cached_executor_clock_reference(tmp_path):
    heartbeat = tmp_path / "executor_heartbeat.json"
    heartbeat.write_text(
        json.dumps(
            {
                "updated_at": "2026-08-04T12:16:59+00:00",
                "details": {
                    "price_stream": {
                        "connected": True,
                        "broker_clock_lead_sec": 63.625,
                        "broker_clock_sample_count": 128,
                        "clock_sync_status": "local_clock_behind_broker",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    local = dt.datetime(2026, 8, 4, 12, 17, tzinfo=UTC)
    corrected, diagnostic = news.normalized_observation_time(
        local,
        heartbeat_path=heartbeat,
        clock_integrity_path=tmp_path / "missing_clock_integrity.json",
    )

    assert corrected == local
    assert diagnostic["normalized"] is False
    assert diagnostic["cached_executor_offset_permitted"] is False
    assert diagnostic["trusted_for_prospective_evidence"] is False


def test_observation_time_rejects_stale_broker_clock_reference(tmp_path):
    heartbeat = tmp_path / "executor_heartbeat.json"
    heartbeat.write_text(
        json.dumps(
            {
                "updated_at": "2026-08-04T12:15:00+00:00",
                "details": {
                    "price_stream": {
                        "connected": True,
                        "broker_clock_lead_sec": 63.625,
                        "broker_clock_sample_count": 128,
                        "clock_sync_status": "local_clock_behind_broker",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    local = dt.datetime(2026, 8, 4, 12, 17, tzinfo=UTC)

    corrected, diagnostic = news.normalized_observation_time(
        local,
        heartbeat_path=heartbeat,
        clock_integrity_path=tmp_path / "missing_clock_integrity.json",
    )

    assert corrected == local
    assert diagnostic["normalized"] is False
    assert diagnostic["applied_offset_sec"] == 0.0


def test_observation_time_uses_fresh_external_clock_when_market_stream_is_idle(tmp_path):
    local = dt.datetime(2026, 8, 8, 22, 0, tzinfo=UTC)
    clock = tmp_path / "clock.json"
    clock.write_text(json.dumps({
        "generated_utc": "2026-08-08T21:59:45+00:00",
        "status": "mitigated",
        "timestamp_normalization_trusted": True,
        "host_clock_synchronized": False,
        "external_https_clock": {
            "status": "ok",
            "offset_sec": 59.5,
            "round_trip_ms": 150.0,
            "precision_sec": 1.0,
        },
    }), encoding="utf-8")

    corrected, diagnostic = news.normalized_observation_time(
        local,
        heartbeat_path=tmp_path / "missing_heartbeat.json",
        clock_integrity_path=clock,
    )

    assert corrected == dt.datetime(2026, 8, 8, 22, 0, 59, 500000, tzinfo=UTC)
    assert diagnostic["normalized"] is True
    assert diagnostic["source"] == "clock_integrity_oanda_https_date"
    assert diagnostic["external_clock_usable"] is True
    assert diagnostic["trusted_for_prospective_evidence"] is True


def test_observation_time_rejects_stale_external_clock(tmp_path):
    local = dt.datetime(2026, 8, 8, 22, 0, tzinfo=UTC)
    clock = tmp_path / "clock.json"
    clock.write_text(json.dumps({
        "generated_utc": "2026-08-08T21:58:00+00:00",
        "status": "mitigated",
        "timestamp_normalization_trusted": True,
        "host_clock_synchronized": False,
        "external_https_clock": {
            "status": "ok",
            "offset_sec": 59.5,
            "round_trip_ms": 150.0,
            "precision_sec": 1.0,
        },
    }), encoding="utf-8")

    corrected, diagnostic = news.normalized_observation_time(
        local,
        heartbeat_path=tmp_path / "missing_heartbeat.json",
        clock_integrity_path=clock,
    )

    assert corrected == local
    assert diagnostic["normalized"] is False
    assert diagnostic["external_clock_usable"] is False


def test_observation_time_trusts_fresh_exact_host_attestation(tmp_path):
    local = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    clock = tmp_path / "clock.json"
    clock.write_text(json.dumps({
        "generated_utc": "2026-08-17T09:59:45+00:00",
        "status": "ok",
        "source_fresh": True,
        "broker_clock_lead_sec": -0.25,
        "broker_clock_sample_count": 128,
        "timestamp_normalization_trusted": True,
        "host_clock_synchronized": True,
        "external_https_clock": {
            "status": "ok",
            "offset_sec": -0.4,
            "round_trip_ms": 100.0,
            "precision_sec": 1.0,
        },
    }), encoding="utf-8")

    corrected, diagnostic = news.normalized_observation_time(
        local,
        heartbeat_path=tmp_path / "missing_heartbeat.json",
        clock_integrity_path=clock,
    )

    assert corrected == local
    assert diagnostic["source"] == "clock_integrity_synchronized_host"
    assert diagnostic["host_clock_synchronized"] is True
    assert diagnostic["trusted_for_prospective_evidence"] is True


def test_observation_time_rejects_stale_host_sync_claim(tmp_path):
    local = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    clock = tmp_path / "clock.json"
    clock.write_text(json.dumps({
        "generated_utc": "2026-08-17T09:29:59+00:00",
        "status": "ok",
        "source_fresh": True,
        "broker_clock_lead_sec": -0.25,
        "broker_clock_sample_count": 128,
        "timestamp_normalization_trusted": True,
        "host_clock_synchronized": True,
        "external_https_clock": {
            "status": "ok",
            "offset_sec": -0.4,
            "round_trip_ms": 100.0,
            "precision_sec": 1.0,
        },
    }), encoding="utf-8")

    corrected, diagnostic = news.normalized_observation_time(
        local,
        clock_integrity_path=clock,
    )

    assert corrected == local
    assert diagnostic["integrity_state_fresh"] is False
    assert diagnostic["host_clock_synchronized"] is False
    assert diagnostic["trusted_for_prospective_evidence"] is False


@pytest.mark.parametrize(
    "field,value",
    [("host_clock_synchronized", 1), ("timestamp_normalization_trusted", 1)],
)
def test_observation_time_requires_exact_boolean_attestations(
    tmp_path, field, value
):
    local = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    state = {
        "generated_utc": "2026-08-17T09:59:45+00:00",
        "status": "ok",
        "source_fresh": True,
        "broker_clock_lead_sec": -0.25,
        "broker_clock_sample_count": 128,
        "timestamp_normalization_trusted": True,
        "host_clock_synchronized": True,
        "external_https_clock": {"status": "unavailable"},
    }
    state[field] = value
    clock = tmp_path / "clock.json"
    clock.write_text(json.dumps(state), encoding="utf-8")

    corrected, diagnostic = news.normalized_observation_time(
        local, clock_integrity_path=clock
    )

    assert corrected == local
    assert diagnostic["host_clock_synchronized"] is False
    assert diagnostic["trusted_for_prospective_evidence"] is False


def test_observation_time_rejects_unsubstantiated_host_sync_claim(tmp_path):
    local = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    clock = tmp_path / "clock.json"
    clock.write_text(json.dumps({
        "generated_utc": "2026-08-17T09:59:45+00:00",
        "status": "ok",
        "source_fresh": False,
        "timestamp_normalization_trusted": True,
        "host_clock_synchronized": True,
        "external_https_clock": {"status": "unavailable"},
    }), encoding="utf-8")

    corrected, diagnostic = news.normalized_observation_time(
        local, clock_integrity_path=clock
    )

    assert corrected == local
    assert diagnostic["source"] == "unavailable"
    assert diagnostic["host_clock_synchronized"] is False
    assert diagnostic["trusted_for_prospective_evidence"] is False


def test_observation_time_rejects_future_integrity_state(tmp_path):
    local = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    clock = tmp_path / "clock.json"
    clock.write_text(json.dumps({
        "generated_utc": "2026-08-18T10:00:00+00:00",
        "status": "ok",
        "source_fresh": True,
        "broker_clock_lead_sec": 0.1,
        "broker_clock_sample_count": 128,
        "timestamp_normalization_trusted": True,
        "host_clock_synchronized": True,
        "external_https_clock": {"status": "unavailable"},
    }), encoding="utf-8")

    corrected, diagnostic = news.normalized_observation_time(
        local, clock_integrity_path=clock
    )

    assert corrected == local
    assert diagnostic["integrity_age_sec"] < 0
    assert diagnostic["integrity_state_fresh"] is False
    assert diagnostic["trusted_for_prospective_evidence"] is False


def test_observation_time_rejects_boolean_clock_number(tmp_path):
    local = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    clock = tmp_path / "clock.json"
    clock.write_text(json.dumps({
        "generated_utc": "2026-08-17T09:59:45+00:00",
        "status": "ok",
        "source_fresh": True,
        "broker_clock_lead_sec": True,
        "broker_clock_sample_count": 128,
        "timestamp_normalization_trusted": True,
        "host_clock_synchronized": True,
        "external_https_clock": {"status": "unavailable"},
    }), encoding="utf-8")

    corrected, diagnostic = news.normalized_observation_time(
        local, clock_integrity_path=clock
    )

    assert corrected == local
    assert diagnostic["broker_clock_lead_sec"] is None
    assert diagnostic["trusted_for_prospective_evidence"] is False


def test_observation_time_rejects_conflicting_fresh_sources(tmp_path):
    local = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    clock = tmp_path / "clock.json"
    clock.write_text(json.dumps({
        "generated_utc": "2026-08-17T09:59:45+00:00",
        "status": "mitigated",
        "source_fresh": True,
        "broker_clock_lead_sec": 60.0,
        "broker_clock_sample_count": 128,
        "timestamp_normalization_trusted": True,
        "host_clock_synchronized": False,
        "external_https_clock": {
            "status": "ok", "offset_sec": 290.0,
            "round_trip_ms": 100.0, "precision_sec": 1.0,
        },
    }), encoding="utf-8")

    corrected, diagnostic = news.normalized_observation_time(
        local, clock_integrity_path=clock
    )

    assert corrected == local
    assert diagnostic["clock_sources_consistent"] is False
    assert diagnostic["clock_source_disagreement_sec"] == 230.0
    assert diagnostic["trusted_for_prospective_evidence"] is False


def test_news_cycle_fails_closed_before_database_open_on_untrusted_clock(
    tmp_path, monkeypatch
):
    config = tmp_path / "sources.json"
    config.write_text(json.dumps({"policy": {}, "sources": []}), encoding="utf-8")
    output = tmp_path / "news"
    current = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    monkeypatch.setattr(news, "utc_now", lambda: current)
    monkeypatch.setattr(
        news,
        "normalized_observation_time",
        lambda value: (
            value,
            {
                "contract_id": news.OBSERVATION_TIME_CONTRACT_ID,
                "source": "unavailable",
                "trusted_for_prospective_evidence": False,
            },
        ),
    )

    result = news.run_cycle(
        config_path=config,
        output_root=output,
        ledger_path=tmp_path / "ledger.csv",
        event_root=tmp_path / "events",
        refresh_event_catalog=False,
    )

    assert result["status"] == "blocked_clock_integrity"
    assert result["database_opened"] is False
    assert result["prospective_evidence_written"] is False
    assert not (output / "local_news_sentiment_v1.sqlite").exists()


def test_news_cycle_quarantines_fetch_when_clock_fails_after_response(
    tmp_path, monkeypatch
):
    config = tmp_path / "sources.json"
    config.write_text(
        json.dumps(
            {
                "policy": {"request_timeout_sec": 2},
                "sources": [
                    {
                        "source_id": "official",
                        "name": "Official",
                        "kind": "rss",
                        "url": "https://example.com/feed.xml",
                        "enabled": True,
                        "runtime_supported": True,
                        "verified": True,
                        "direct": True,
                        "currencies": ["USD"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "news"
    current = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    monkeypatch.setattr(news, "utc_now", lambda: current)
    calls = {"value": 0}

    def clock(value):
        calls["value"] += 1
        trusted = calls["value"] <= 2
        return value, {
            "contract_id": news.OBSERVATION_TIME_CONTRACT_ID,
            "source": "test" if trusted else "unavailable",
            "trusted_for_prospective_evidence": trusted,
        }

    monkeypatch.setattr(news, "normalized_observation_time", clock)
    seen_fetch_states = []

    def fetch(source, state, **kwargs):
        seen_fetch_states.append(dict(state))
        return (
            [
                {
                    "source_id": "official",
                    "source_name": "Official",
                    "source_kind": "rss",
                    "source_quality": 1.0,
                    "source_verified": True,
                    "source_direct": True,
                    "source_currencies": ["USD"],
                    "title": "Official release",
                    "summary": "New information",
                    "url": "https://example.com/release",
                    "published_utc": "2026-08-17T09:59:00Z",
                }
            ],
            {
                **state,
                "last_status": 200,
                "last_error": "",
                "etag": "EVENT_ETAG",
                "last_modified": "EVENT_LAST_MODIFIED",
            },
        )

    monkeypatch.setattr(news, "fetch_source", fetch)

    result = news.run_cycle(
        config_path=config,
        output_root=output,
        ledger_path=tmp_path / "ledger.csv",
        event_root=tmp_path / "events",
        refresh_event_catalog=False,
    )

    assert result["status"] == "blocked_clock_integrity_at_completion"
    assert result["inserted_items"] == 0
    persisted = json.loads(
        (output / "collector_state_v1.json").read_text(encoding="utf-8")
    )
    assert "etag" not in (persisted.get("sources") or {}).get("official", {})
    assert "last_modified" not in (
        (persisted.get("sources") or {}).get("official", {})
    )
    with sqlite3.connect(output / "local_news_sentiment_v1.sqlite") as connection:
        assert connection.execute("SELECT COUNT(*) FROM articles").fetchone()[0] == 0

    # The completion-block path closed the cached connection.  A trusted next
    # cycle must open a fresh handle, retry without quarantined validators,
    # and commit the event rather than crashing or receiving an artificial 304.
    monkeypatch.setattr(
        news,
        "normalized_observation_time",
        lambda value: (
            value,
            {
                "contract_id": news.OBSERVATION_TIME_CONTRACT_ID,
                "source": "test",
                "trusted_for_prospective_evidence": True,
            },
        ),
    )
    recovered = news.run_cycle(
        config_path=config,
        output_root=output,
        ledger_path=tmp_path / "ledger.csv",
        event_root=tmp_path / "events",
        refresh_event_catalog=False,
    )
    assert recovered["status"] == "ok"
    assert len(seen_fetch_states) == 2
    assert "etag" not in seen_fetch_states[1]
    with sqlite3.connect(output / "local_news_sentiment_v1.sqlite") as connection:
        assert connection.execute("SELECT COUNT(*) FROM articles").fetchone()[0] == 1


def test_news_cycle_rejects_truthy_attestation_from_wrong_clock_contract(
    tmp_path, monkeypatch
):
    config = tmp_path / "sources.json"
    config.write_text(json.dumps({"policy": {}, "sources": []}), encoding="utf-8")
    output = tmp_path / "news"
    current = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    monkeypatch.setattr(news, "utc_now", lambda: current)
    monkeypatch.setattr(
        news,
        "normalized_observation_time",
        lambda value: (
            value,
            {
                "contract_id": "obsolete_or_forged_clock_contract",
                "source": "test",
                "trusted_for_prospective_evidence": True,
            },
        ),
    )

    result = news.run_cycle(
        config_path=config,
        output_root=output,
        ledger_path=tmp_path / "ledger.csv",
        event_root=tmp_path / "events",
        refresh_event_catalog=False,
    )

    assert result["status"] == "blocked_clock_integrity"
    assert result["database_opened"] is False
    assert not (output / "local_news_sentiment_v1.sqlite").exists()


def test_duplicate_article_cannot_be_relabelled_to_new_clock_contract(tmp_path):
    connection = news.process_database(tmp_path / "news.sqlite")
    first_seen = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    raw = {
        "source_id": "official",
        "source_name": "Official",
        "source_kind": "rss",
        "source_quality": 1.0,
        "source_verified": True,
        "source_direct": True,
        "source_currencies": ["USD"],
        "title": "Official release",
        "summary": "Unchanged facts",
        "url": "https://example.com/release",
        "published_utc": "2026-08-17T09:59:00Z",
        "collector_contract_id": "collector-old",
        "collector_cohort_id": "collector-old",
        "observation_time_contract_id": "clock-old",
        "observation_clock_trusted": False,
        "observation_clock_source": "unavailable",
    }
    first = news.classify_article(raw, first_seen=first_seen)
    assert news.upsert_articles(connection, [first], first_seen) == (1, 0)
    newer = dict(raw)
    newer.update(
        {
            "collector_contract_id": news.COLLECTOR_CONTRACT_ID,
            "collector_cohort_id": news.COLLECTOR_COHORT_ID,
            "observation_time_contract_id": news.OBSERVATION_TIME_CONTRACT_ID,
            "observation_clock_trusted": True,
            "observation_clock_source": "clock_integrity_synchronized_host",
        }
    )
    second = news.classify_article(
        newer, first_seen=first_seen + dt.timedelta(minutes=1)
    )
    news.upsert_articles(connection, [second], first_seen + dt.timedelta(minutes=1))
    payload = json.loads(
        connection.execute("SELECT payload_json FROM articles").fetchone()[0]
    )
    connection.close()
    assert payload["observation_time_contract_id"] == "clock-old"
    assert payload["observation_clock_trusted"] is False


def test_pre_provenance_duplicate_remains_legacy_untrusted(tmp_path):
    connection = news.process_database(tmp_path / "news.sqlite")
    first_seen = dt.datetime(2026, 8, 10, 10, 0, tzinfo=UTC)
    raw = {
        "source_id": "official",
        "source_name": "Official",
        "source_kind": "rss",
        "source_quality": 1.0,
        "source_verified": True,
        "source_direct": True,
        "source_currencies": ["USD"],
        "title": "Legacy official release",
        "summary": "Unchanged facts",
        "url": "https://example.com/legacy-release",
        "published_utc": "2026-08-10T09:59:00Z",
    }
    first = news.classify_article(raw, first_seen=first_seen)
    assert news.upsert_articles(connection, [first], first_seen) == (1, 0)

    payload = json.loads(
        connection.execute("SELECT payload_json FROM articles").fetchone()[0]
    )
    for field in (
        "collector_contract_id",
        "collector_cohort_id",
        "observation_time_contract_id",
        "observation_clock_trusted",
        "observation_clock_source",
    ):
        payload.pop(field, None)
    connection.execute(
        "UPDATE articles SET payload_json=?", (json.dumps(payload, sort_keys=True),)
    )
    connection.commit()

    current = dict(raw)
    current.update(
        {
            "collector_contract_id": news.COLLECTOR_CONTRACT_ID,
            "collector_cohort_id": news.COLLECTOR_COHORT_ID,
            "observation_time_contract_id": news.OBSERVATION_TIME_CONTRACT_ID,
            "observation_clock_trusted": True,
            "observation_clock_source": "clock_integrity_synchronized_host",
        }
    )
    second = news.classify_article(
        current, first_seen=first_seen + dt.timedelta(days=7)
    )
    news.upsert_articles(connection, [second], first_seen + dt.timedelta(days=7))
    stored = json.loads(
        connection.execute(
            "SELECT payload_json FROM articles WHERE source_url=?",
            (raw["url"],),
        ).fetchone()[0]
    )
    connection.close()

    assert stored["collector_contract_id"] == ""
    assert stored["collector_cohort_id"] == ""
    assert stored["observation_time_contract_id"] == ""
    assert stored["observation_clock_trusted"] is False
    assert stored["observation_clock_source"] == "legacy_unattested"
    assert news.prospective_collector_provenance(stored) is False


def test_current_topic_payload_does_not_inherit_legacy_first_known_clock(tmp_path):
    connection = news.process_database(tmp_path / "topics.sqlite")
    legacy_known = dt.datetime(2026, 8, 10, 10, 0, tzinfo=UTC)
    current_known = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    base = {
        "topic_id": "same-semantic-topic",
        "topic_signature": "monetary_policy|JPY|boj|hike",
        "published_utc": news.iso_utc(legacy_known),
        "category": "monetary_policy",
        "direct_currencies": ["JPY"],
        "topic_tags": ["#jpy_hike"],
        "topic_article_count": 1,
        "distinct_source_count": 1,
        "source_quality": 1.0,
        "source_verified": True,
        "event_id": "legacy-event",
        "collector_contract_id": "legacy-v38",
        "collector_cohort_id": "legacy-v38",
        "observation_time_contract_id": "legacy-clock",
        "observation_clock_trusted": False,
    }
    legacy = {
        **base,
        "causal_known_utc": news.iso_utc(legacy_known),
        "first_seen_utc": news.iso_utc(legacy_known),
        "last_seen_utc": news.iso_utc(legacy_known),
    }
    assert news.upsert_topic_events(connection, [legacy]) == (1, 0)
    current = {
        **base,
        "event_id": "current-event",
        "published_utc": news.iso_utc(current_known),
        "causal_known_utc": news.iso_utc(current_known),
        "first_seen_utc": news.iso_utc(current_known),
        "last_seen_utc": news.iso_utc(current_known),
        "collector_contract_id": news.COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": news.COLLECTOR_COHORT_ID,
        "observation_time_contract_id": news.OBSERVATION_TIME_CONTRACT_ID,
        "observation_clock_trusted": True,
        "observation_clock_source": "clock_integrity_synchronized_host",
    }
    assert news.upsert_topic_events(connection, [current]) == (0, 1)
    first_known, payload_text = connection.execute(
        "SELECT first_known_utc,payload_json FROM topic_events"
    ).fetchone()
    payload = json.loads(payload_text)
    connection.close()

    assert first_known == news.iso_utc(legacy_known)
    assert payload["prospective_provenance_known_utc"] == news.iso_utc(
        current_known
    )
    assert news.prospective_collector_provenance(payload) is True


def test_source_health_separates_enabled_from_observed_health(monkeypatch):
    monkeypatch.delenv("TEST_NEWS_API_KEY", raising=False)
    sources = [
        {"source_id": "healthy", "enabled": True},
        {"source_id": "degraded", "enabled": True},
        {"source_id": "new", "enabled": True},
        {"source_id": "off", "enabled": False},
        {
            "source_id": "credentialed",
            "enabled": True,
            "credential_env": "TEST_NEWS_API_KEY",
        },
    ]
    states = {
        "healthy": {
            "last_attempt_utc": "2026-08-04T03:00:00+00:00",
            "consecutive_errors": 0,
        },
        "degraded": {
            "last_attempt_utc": "2026-08-04T03:00:00+00:00",
            "consecutive_errors": 3,
        },
    }
    assert news.summarize_source_health(sources, states) == {
        "configured": 5,
        "enabled": 3,
        "healthy": 1,
        "degraded": 1,
        "uninitialized": 1,
        "credential_missing": 1,
        "external_adapter": 0,
        "disabled": 1,
        "unsupported": 0,
    }


def test_gdelt_error_backoff_scales_from_long_poll_interval():
    now = dt.datetime(2026, 8, 4, 8, 0, tzinfo=UTC)
    source = {"kind": "gdelt", "poll_interval_sec": 900}
    policy = {"gdelt_poll_interval_sec": 600}

    one_error = {
        "last_attempt_utc": (now - dt.timedelta(seconds=1799)).isoformat(),
        "consecutive_errors": 1,
    }
    assert not news.due_for_poll(source, one_error, policy, now)
    one_error["last_attempt_utc"] = (
        now - dt.timedelta(seconds=1801)
    ).isoformat()
    assert news.due_for_poll(source, one_error, policy, now)

    two_errors = {
        "last_attempt_utc": (now - dt.timedelta(seconds=3599)).isoformat(),
        "consecutive_errors": 2,
    }
    assert not news.due_for_poll(source, two_errors, policy, now)
    two_errors["last_attempt_utc"] = (
        now - dt.timedelta(seconds=3601)
    ).isoformat()
    assert news.due_for_poll(source, two_errors, policy, now)


def test_gdelt_429_without_header_gets_six_hour_provider_backoff():
    now = dt.datetime(2026, 8, 16, 5, 45, tzinfo=UTC)
    source = {
        "kind": "gdelt",
        "rate_limit_retry_sec": 21600,
    }
    assert news.provider_http_retry_after(source, 429, 0.0, now) == (
        "2026-08-16T11:45:00+00:00"
    )
    assert news.provider_http_retry_after(source, 429, 120.0, now) == (
        "2026-08-16T05:47:00+00:00"
    )
    assert news.provider_http_retry_after({"kind": "rss"}, 503, 0.0, now) == ""


def test_slow_changing_source_cadences_are_quota_safe_and_versioned():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    sources = {row["source_id"]: row for row in config["sources"]}
    bls = sources["bls_major_timeseries_batch_v1"]
    assert bls["poll_interval_sec"] == 21600
    assert bls["burst_poll_interval_sec"] == 300
    assert bls["source_contract_id"] == (
        "bls_major_timeseries_v4_source_lineage_20260819"
    )
    assert bls["source_cohort_id"] == bls["source_contract_id"]
    gdelt = sources["gdelt_fx_macro_discovery"]
    assert gdelt["rate_limit_retry_sec"] == 21600
    assert gdelt["source_contract_id"] == (
        "gdelt_fx_macro_rate_limit_v2_20260816"
    )
    assert gdelt["source_cohort_id"] == gdelt["source_contract_id"]


def test_unversioned_source_gets_deterministic_exact_config_lineage():
    source = {
        "source_id": "central_bank",
        "kind": "rss",
        "url": "https://example.gov/feed.xml",
        "currencies": ["USD"],
        "verified": True,
    }
    first = news.source_config_lineage(source)
    reordered = news.source_config_lineage(dict(reversed(list(source.items()))))
    assert first["source_contract_id"] == reordered["source_contract_id"]
    assert first["source_cohort_id"] == first["source_contract_id"]
    assert first["source_contract_derived"] is True
    assert len(first["source_config_sha256"]) == 64
    changed = news.source_config_lineage({**source, "url": "https://example.gov/v2.xml"})
    assert changed["source_contract_id"] != first["source_contract_id"]
    explicit = news.source_config_lineage({
        **source,
        "source_contract_id": "explicit-v7",
        "source_cohort_id": "explicit-cohort-v7",
    })
    assert explicit["source_contract_id"] == "explicit-v7"
    assert explicit["source_cohort_id"] == "explicit-cohort-v7"
    assert "source_contract_derived" not in explicit


def test_derived_lineage_state_adoption_preserves_poll_and_validator_state():
    now = dt.datetime(2026, 8, 27, 4, 30, tzinfo=UTC)
    source = news.source_config_lineage({
        "source_id": "central_bank",
        "kind": "rss",
        "url": "https://example.gov/feed.xml",
        "poll_interval_sec": 900,
    })
    prior = {
        "last_attempt_utc": (now - dt.timedelta(seconds=30)).isoformat(),
        "last_success_utc": (now - dt.timedelta(seconds=30)).isoformat(),
        "etag": '"keep-me"',
        "last_modified": "Wed, 26 Aug 2026 20:00:00 GMT",
        "known_release_ids": ["old-release"],
        "detail_first_seen_utc_by_url": {"https://example.gov/a": "known"},
    }
    migrated = news.migrate_derived_source_state(source, prior, now=now)
    assert migrated["source_contract_id"] == source["source_contract_id"]
    assert migrated["source_cohort_id"] == source["source_cohort_id"]
    assert migrated["etag"] == prior["etag"]
    assert migrated["last_modified"] == prior["last_modified"]
    assert migrated["known_release_ids"] == ["old-release"]
    assert migrated["detail_first_seen_utc_by_url"] == prior[
        "detail_first_seen_utc_by_url"
    ]
    assert not news.due_for_poll(source, migrated, {}, now)


def test_changed_source_contract_gets_immediate_retry_after_parser_repair():
    now = dt.datetime(2026, 8, 14, 15, 0, tzinfo=UTC)
    source = {
        "kind": "umich_current_release",
        "poll_interval_sec": 900,
        "source_contract_id": "fixed_v2",
    }
    stale_state = {
        "last_attempt_utc": (now - dt.timedelta(seconds=5)).isoformat(),
        "consecutive_errors": 1,
        "source_contract_id": "broken_v1",
    }
    assert news.due_for_poll(source, stale_state, {}, now)


def test_changed_source_contract_suppresses_stale_conditional_get(monkeypatch):
    captured = {}
    payload = b'''<h1>Preliminary Results for August 2026</h1>
      Index of Consumer Sentiment 51.0 55.2 58.2 -7.6% -12.4%
      Current Economic Conditions 51.8 54.8 61.7
      Index of Consumer Expectations 50.6 55.4 55.9
      Surveys of Consumers Director Joanne Hsu Consumer sentiment fell.
      Copyright 2026'''

    class Response:
        status = 200
        headers = {}
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def read(self, _limit): return payload

    def fake_urlopen(request, **_kwargs):
        captured.update(dict(request.header_items()))
        return Response()

    monkeypatch.setattr(news.urllib.request, "urlopen", fake_urlopen)
    rows, state = news.fetch_source(
        {
            "source_id": "umich",
            "name": "Michigan",
            "kind": "umich_current_release",
            "url": "https://www.sca.isr.umich.edu/",
            "trusted_domains": ["www.sca.isr.umich.edu"],
            "currencies": ["USD"],
            "verified": True,
            "source_contract_id": "fixed_v2",
            "source_cohort_id": "fixed_v2",
        },
        {
            "etag": '"stale"',
            "last_modified": "Fri, 14 Aug 2026 13:58:02 GMT",
            "source_contract_id": "broken_v1",
        },
        timeout_sec=1,
        maximum_bytes=100_000,
        now=dt.datetime(2026, 8, 14, 15, 0, tzinfo=UTC),
    )
    assert len(rows) == 1
    assert "if-none-match" not in {
        key.lower(): value for key, value in captured.items()
    }
    assert state["source_contract_id"] == "fixed_v2"


def test_pending_official_context_archive_forces_listing_body(monkeypatch):
    captured = {}

    class Response:
        status = 200
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, _limit):
            return b'<a href="/release/2026.pdf">Current report</a>'

    def fake_urlopen(request, **_kwargs):
        captured.update(dict(request.header_items()))
        return Response()

    monkeypatch.setattr(news.urllib.request, "urlopen", fake_urlopen)
    rows, _state = news.fetch_source(
        {
            "source_id": "central_bank",
            "kind": "html_links",
            "url": "https://example.gov/listing",
            "link_patterns": ["2026"],
            "trusted_domains": ["example.gov"],
            "detail_context_archive_only": True,
            "detail_context_target_count": 8,
            "source_contract_id": "current_v1",
        },
        {
            "etag": '"listing"',
            "source_contract_id": "current_v1",
            "detail_first_seen_utc_by_url": {
                f"https://example.gov/{index}.pdf": "2026-08-14T15:00:00Z"
                for index in range(4)
            },
        },
        timeout_sec=1,
        maximum_bytes=100_000,
        now=dt.datetime(2026, 8, 14, 16, 0, tzinfo=UTC),
    )
    assert len(rows) == 1
    assert "if-none-match" not in {
        key.lower(): value for key, value in captured.items()
    }

def test_partition_articles_by_retention_skips_known_expired_rows():
    cutoff = dt.datetime(2026, 8, 1, tzinfo=UTC)
    current = {
        "event_id": "new",
        "published_utc": "2026-08-02T00:00:00+00:00",
    }
    old = {
        "event_id": "old",
        "published_utc": "2026-06-01T00:00:00+00:00",
    }
    unknown = {"event_id": "unknown", "published_utc": ""}

    eligible, expired = news.partition_articles_by_retention(
        [current, old, unknown],
        before=cutoff,
    )

    assert [row["event_id"] for row in eligible] == ["new", "unknown"]
    assert [row["event_id"] for row in expired] == ["old"]


def test_verified_official_policy_history_survives_general_retention(tmp_path):
    cutoff = dt.datetime(2026, 8, 1, tzinfo=UTC)
    official = {
        "event_id": "old-policy",
        "published_utc": "2026-06-01T00:00:00+00:00",
        "source_verified": True,
        "official_policy_release": True,
    }
    eligible, expired = news.partition_articles_by_retention(
        [official], before=cutoff
    )
    assert [row["event_id"] for row in eligible] == ["old-policy"]
    assert expired == []

    connection = news.open_database(tmp_path / "news.sqlite")
    placeholders = ",".join("?" for _ in range(29))
    common = (
        "bank", "Bank", "rss", 1.0, 1,
        "2026-06-01T00:00:00+00:00", "2026-06-01T00:00:01+00:00",
        "2026-06-01T00:00:01+00:00", "Decision", "", "https://bank.test/a",
        "bank.test", 1, "monetary_policy", "currency", "[]", "{}", "{}",
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0, "low", 180, 0,
    )
    connection.execute(
        f"INSERT INTO articles VALUES ({placeholders})",
        ("old-policy", *common, json.dumps(official)),
    )
    ordinary = {"event_id": "old-news"}
    connection.execute(
        f"INSERT INTO articles VALUES ({placeholders})",
        ("old-news", *common, json.dumps(ordinary)),
    )
    connection.commit()
    assert news.prune_database(connection, before=cutoff) == 1
    assert connection.execute(
        "SELECT event_id FROM articles ORDER BY event_id"
    ).fetchall() == [("old-policy",)]
    connection.close()


def test_gdelt_noise_filter_keeps_only_audit_worthy_context_rows():
    noise = {
        "source_kind": "gdelt",
        "relevant": False,
        "context_only": False,
        "market_relevance_count": 0,
        "currencies": [],
    }
    incidental_context = {**noise, "context_only": True}
    macro_context = {
        **incidental_context,
        "market_relevance_count": 1,
    }
    currency_context = {**incidental_context, "currencies": ["JPY"]}
    relevant = {**noise, "relevant": True}
    rss_noise = {**noise, "source_kind": "rss"}

    assert news.retain_classified_discovery_article(noise) is False
    assert news.retain_classified_discovery_article(incidental_context) is False
    assert news.retain_classified_discovery_article(macro_context) is True
    assert news.retain_classified_discovery_article(currency_context) is True
    assert news.retain_classified_discovery_article(relevant) is True
    assert news.retain_classified_discovery_article(rss_noise) is True


def test_tariff_lawsuit_is_retained_as_non_directional_policy_context():
    article = news.classify_article(
        {
            "source_id": "google_news_trade_policy",
            "source_name": "Associated Press",
            "source_kind": "rss",
            "source_quality": 0.7,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "25 states sue over new tariffs, calling them a pretext to "
                "replace the old import taxes"
            ),
            "summary": "",
            "url": "https://example.com/tariff-lawsuit",
            "published_utc": "2026-08-03T20:25:13Z",
        },
        first_seen=dt.datetime(2026, 8, 3, 20, 26, tzinfo=UTC),
    )

    assert article["category"] == "trade_policy"
    assert article["context_only"] is True
    assert article["relevant"] is False
    assert article["currency_scores"] == {}
    assert article["execution_eligible"] is False


def test_forced_labour_tariff_lawsuit_is_trade_policy_not_labor_release():
    article = news.classify_article(
        {
            "source_id": "google_news_trade_policy",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "25 states move court against forced-labour tariffs",
            "summary": "",
            "url": "https://example.com/forced-labour-tariff-lawsuit",
            "published_utc": "2026-08-04T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 12, 1, tzinfo=UTC),
    )

    assert article["category"] == "trade_policy"
    assert article["context_only"] is True
    assert article["currency_scores"] == {}


def test_prior_session_equity_close_with_oil_move_is_recap_context():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "US Equity Markets End Higher Monday as Crude Oil Falls "
                "on US-Iran's New Round of Talks"
            ),
            "summary": "",
            "url": "https://example.com/prior-equity-close-oil-recap",
            "published_utc": "2026-08-04T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 12, 1, tzinfo=UTC),
    )

    assert article["category"] == "commodity_shock"
    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]
    assert topic["context_reason"] == "reported_market_move_context"


def test_futures_and_oil_move_headline_is_reaction_not_fresh_catalyst():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "U.S. futures march higher, while oil drops, after Bessent "
                "comments on Iran talks"
            ),
            "summary": "",
            "url": "https://example.com/futures-oil-reaction",
            "published_utc": "2026-08-04T13:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 13, 1, tzinfo=UTC),
    )

    assert article["category"] == "commodity_shock"
    assert article["reports_prior_market_move"] is True
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]
    assert topic["context_reason"] == "reported_market_move_context"


def test_event_catalog_refresh_returns_fresh_manifest_without_spawning(tmp_path):
    (tmp_path / "latest_pair_news_context.json").write_text(
        "{}",
        encoding="utf-8",
    )
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "generated_utc": "2026-07-29T22:00:00+00:00",
                "event_count": 123,
                "pair_event_tag_count": 456,
            }
        ),
        encoding="utf-8",
    )
    result = news.request_event_catalog_refresh(
        output_root=tmp_path,
        ledger_path=tmp_path / "missing-ledger.csv",
    )
    assert result["status"] == "fresh"
    assert result["event_count"] == 123
    assert result["pair_event_tag_count"] == 456


def test_event_catalog_refresh_reports_existing_worker_lock(tmp_path):
    (tmp_path / ".sync.lock").write_text(
        json.dumps({"pid": 9876}),
        encoding="utf-8",
    )
    result = news.request_event_catalog_refresh(
        output_root=tmp_path,
        ledger_path=tmp_path / "missing-ledger.csv",
    )
    assert result["status"] == "refresh_running"
    assert result["pid"] == 9876


def test_event_catalog_refresh_detects_newer_ledger_while_worker_is_locked(
    tmp_path,
):
    context = tmp_path / "latest_pair_news_context.json"
    context.write_text("{}", encoding="utf-8")
    ledger = tmp_path / "news_watch_ledger.csv"
    ledger.write_text("watch_id\n", encoding="utf-8")
    context_time = context.stat().st_mtime
    import os

    os.utime(ledger, (context_time + 2, context_time + 2))
    (tmp_path / ".sync.lock").write_text(
        json.dumps({"pid": 1234}),
        encoding="utf-8",
    )
    result = news.request_event_catalog_refresh(
        output_root=tmp_path,
        ledger_path=ledger,
    )
    assert result["status"] == "refresh_running"
    assert result["ledger_newer_than_context"] is True


def test_event_catalog_refresh_recovers_stale_lock_for_dead_worker(
    tmp_path,
    monkeypatch,
):
    lock = tmp_path / ".sync.lock"
    lock.write_text(json.dumps({"pid": 9876}), encoding="utf-8")
    old_time = time.time() - news.EVENT_CATALOG_LOCK_STALE_SECONDS - 1
    import os

    os.utime(lock, (old_time, old_time))
    monkeypatch.setattr(news, "_process_is_running", lambda _pid: False)

    class DummyProcess:
        pid = 4321

    monkeypatch.setattr(news.subprocess, "Popen", lambda *args, **kwargs: DummyProcess())
    result = news.request_event_catalog_refresh(
        output_root=tmp_path,
        ledger_path=tmp_path / "missing-ledger.csv",
    )

    assert result["status"] == "refresh_started"
    assert result["pid"] == 4321
    assert result["stale_lock_recovered"] is True
    assert not lock.exists()


def test_unchanged_ledger_is_not_replaced(tmp_path):
    path = tmp_path / "ledger.csv"
    article = {
        "event_id": "one",
        "published_utc": "2026-07-29T12:00:00+00:00",
        "first_seen_utc": "2026-07-29T12:01:00+00:00",
        "last_seen_utc": "2026-07-29T12:02:00+00:00",
        "headline": "Test",
        "source_verified": False,
    }
    assert news.write_ledger(path, [article]) is True
    first_modified = path.stat().st_mtime_ns
    time.sleep(0.01)
    assert news.write_ledger(path, [article]) is False
    assert path.stat().st_mtime_ns == first_modified


def test_parse_rss_and_preserve_published_time():
    source = {
        "source_id": "fed",
        "name": "Federal Reserve",
        "quality": 1.0,
        "verified": True,
        "currencies": ["USD"],
    }
    payload = b"""<?xml version="1.0"?>
    <rss version="2.0"><channel><item>
      <title>Federal Reserve raises interest rate</title>
      <description>Inflation remains above forecast.</description>
      <link>https://www.federalreserve.gov/example?utm_source=test</link>
      <pubDate>Mon, 27 Jul 2026 20:00:00 GMT</pubDate>
      <guid>fed-1</guid>
    </item></channel></rss>"""
    rows = news.parse_rss(payload, source)
    assert len(rows) == 1
    assert rows[0]["published_utc"] == "2026-07-27T20:00:00+00:00"
    assert rows[0]["url"] == "https://www.federalreserve.gov/example"


def test_parse_rss_resolves_first_party_relative_release_url():
    rows = news.parse_rss(
        b"""<?xml version="1.0"?>
        <rss version="2.0"><channel><item>
          <title>Statement of the Monetary Policy Committee</title>
          <link>/en/home/publications/publication-detail-pages/statements/mpc</link>
          <pubDate>Thu, 23 Jul 2026 13:00:00 GMT</pubDate>
        </item></channel></rss>""",
        {
            "source_id": "sarb_publications_rss_direct_v1",
            "name": "SARB",
            "url": "https://www.resbank.co.za/bin/sarb/solr/publications/rss",
            "verified": True,
            "direct": True,
            "currencies": ["ZAR"],
        },
    )
    assert rows[0]["url"] == (
        "https://www.resbank.co.za/en/home/publications/"
        "publication-detail-pages/statements/mpc"
    )


def test_recurring_statistical_rss_preserves_each_release_version_and_values():
    source = {
        "source_id": "census_economic_indicators",
        "name": "US Census Bureau economic indicators",
        "kind": "rss",
        "quality": 1.0,
        "verified": True,
        "direct": True,
        "source_role": "primary_statistical_release",
        "source_contract_id": "bls_jolts_v4_source_lineage_20260819",
        "source_cohort_id": "bls_jolts_v4_source_lineage_20260819",
        "recurring_release_feed": True,
        "currencies": ["USD"],
    }
    payload = b"""<?xml version="1.0"?>
    <rss><channel><item>
      <title>Manufacturers' Shipments, Inventories, and Orders</title>
      <description>New orders fell. June 2026: -0.3 % Change May 2026 (r): -1.1 % Change</description>
      <link>https://www.census.gov/manufacturing/m3/</link>
      <pubDate>Tue, 04 Aug 2026 10:00:00 -0400</pubDate>
      <guid>mfg_orders</guid>
    </item></channel></rss>"""

    row = news.parse_rss(payload, source)[0]
    assert row["structured_event"] is True
    assert row["event_series_id"] == "mfg_orders"
    assert row["source_reported_update_utc"] == "2026-08-04T14:00:00+00:00"
    assert row["reference_period"] == "June 2026"
    assert row["actual_value"] == -0.3
    assert row["previous_value"] == -1.1


def test_mutable_official_latest_numbers_are_content_versioned_at_first_seen(tmp_path):
    source = {
        "source_id": "bls_principal_releases",
        "name": "BLS latest numbers",
        "kind": "rss",
        "quality": 1.0,
        "verified": True,
        "direct": True,
        "source_role": "primary_statistical_release",
        "recurring_release_feed": True,
        "mutable_content_versioned": True,
        "currencies": ["USD"],
    }

    def parsed(summary):
        return news.parse_rss(
            f"""<rss><channel><item>
            <title>Major Economic Indicators Latest Numbers</title>
            <description>{summary}</description>
            <link>https://www.bls.gov/bls</link>
            <pubDate>Fri, 31 Jul 2026 12:30:26 GMT</pubDate>
            <guid>bls-latest</guid>
            </item></channel></rss>""".encode(),
            source,
        )[0]

    first_seen = dt.datetime(2026, 8, 12, 12, 32, tzinfo=UTC)
    first_raw = parsed("Consumer Price Index: +0.1% in Jul 2026")
    second_raw = parsed(
        "Consumer Price Index: +0.1% in Jul 2026; "
        "Producer Price Index: unchanged in Jul 2026"
    )
    first = news.classify_article(first_raw, first_seen=first_seen)
    second = news.classify_article(
        second_raw,
        first_seen=first_seen + dt.timedelta(days=1),
    )

    assert first_raw["publisher_container_timestamp_utc"] == (
        "2026-07-31T12:30:26+00:00"
    )
    assert first["published_time_inferred"] is True
    assert first["causal_known_utc"] == "2026-08-12T12:33:00+00:00"
    assert second["causal_known_utc"] == "2026-08-13T12:33:00+00:00"
    assert first["event_lineage_id"] == second["event_lineage_id"]
    assert first["event_id"] != second["event_id"]
    assert first["causal_integrity_state"] == "content_versioned_at_collection"
    assert first["historical_replay_eligible"] is True

    connection = news.open_database(tmp_path / "news.sqlite")
    try:
        indexes = {
            str(row[1])
            for row in connection.execute("PRAGMA index_list(articles)").fetchall()
        }
        assert "idx_articles_source_headline" in indexes
        assert news.upsert_articles(connection, [first], first_seen) == (1, 0)
        assert news.upsert_articles(
            connection,
            [second],
            first_seen + dt.timedelta(days=1),
        ) == (1, 0)
        assert connection.execute("SELECT COUNT(*) FROM articles").fetchone()[0] == 2
    finally:
        connection.close()


def test_legacy_mutable_content_is_quarantined_from_historical_replay():
    article = news.classify_article(
        {
            "source_id": "bls_principal_releases",
            "source_name": "BLS latest numbers",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_role": "primary_statistical_release",
            "source_currencies": ["USD"],
            "title": "Major Economic Indicators Latest Numbers",
            "summary": "Producer Price Index: unchanged in Jul 2026",
            "url": "https://www.bls.gov/bls",
            "published_utc": "2026-07-31T12:30:26Z",
            "mutable_content_source": True,
        },
        first_seen=dt.datetime(2026, 8, 2, 21, 14, tzinfo=UTC),
    )

    assert article["causal_integrity_state"] == (
        "legacy_mutable_content_unversioned"
    )
    assert article["historical_replay_eligible"] is False


def test_parse_bls_timeseries_keeps_arrival_time_and_official_values():
    payload = b'''{"status":"REQUEST_SUCCEEDED","Results":{"series":[{"seriesID":"JTS000000000000000JOL","data":[{"year":"2026","period":"M06","periodName":"June","latest":"true","value":"7359"},{"year":"2026","period":"M05","periodName":"May","value":"7537"}]}]}}'''
    source = {
        "source_id": "bls_jolts_job_openings",
        "name": "US Bureau of Labor Statistics JOLTS job openings",
        "kind": "bls_timeseries",
        "url": "https://api.bls.gov/publicAPI/v2/timeseries/data/JTS000000000000000JOL",
        "event_name": "JOLTS Job Openings",
        "value_scale": 0.001,
        "value_precision": 3,
        "unit": "million",
        "currencies": ["USD"],
        "verified": True,
        "direct": True,
        "source_role": "primary_statistical_release",
        "source_contract_id": "bls_jolts_v4_source_lineage_20260819",
        "source_cohort_id": "bls_jolts_v4_source_lineage_20260819",
    }

    raw = news.parse_bls_timeseries(payload, source)[0]
    assert raw["published_utc"] == ""
    assert raw["reference_period"] == "June 2026"
    assert raw["actual_value"] == 7.359
    assert raw["previous_value"] == 7.537
    assert raw["source_contract_id"] == "bls_jolts_v4_source_lineage_20260819"
    assert raw["source_cohort_id"] == "bls_jolts_v4_source_lineage_20260819"
    classified = news.classify_article(
        raw,
        first_seen=dt.datetime(2026, 8, 4, 14, 8, tzinfo=UTC),
    )
    assert classified["category"] == "labor_release"
    assert classified["published_time_inferred"] is True
    assert classified["causal_known_utc"] == "2026-08-04T14:09:00+00:00"
    assert classified["currency_scores"] == {}
    assert classified["execution_eligible"] is False


def test_parse_bls_batch_expands_series_and_computes_monthly_changes():
    payload = json.dumps(
        {
            "status": "REQUEST_SUCCEEDED",
            "Results": {
                "series": [
                    {
                        "seriesID": "CUSR0000SA0",
                        "data": [
                            {"year": "2026", "period": "M07", "periodName": "July", "value": "324.200"},
                            {"year": "2026", "period": "M06", "periodName": "June", "value": "323.100"},
                            {"year": "2026", "period": "M05", "periodName": "May", "value": "322.500"},
                        ],
                    },
                    {
                        "seriesID": "JTS000000000000000JOL",
                        "data": [
                            {"year": "2026", "period": "M06", "periodName": "June", "value": "7359"},
                            {"year": "2026", "period": "M05", "periodName": "May", "value": "7537"},
                        ],
                    },
                ]
            },
        }
    ).encode()
    source = {
        "source_id": "bls_major_timeseries_batch_v1",
        "name": "BLS major releases",
        "url": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        "currencies": ["USD"],
        "verified": True,
        "direct": True,
        "source_role": "primary_statistical_release",
        "source_contract_id": "bls_major_timeseries_v4_source_lineage_20260819",
        "source_cohort_id": "bls_major_timeseries_v4_source_lineage_20260819",
        "series": [
            {
                "series_id": "CUSR0000SA0",
                "event_name": "Consumer Price Index All Items",
                "value_transform": "percent_change_1",
                "value_precision": 3,
                "unit": "percent change",
                "release_utc_by_reference": {
                    "July 2026": "2026-08-12T12:30:00Z"
                },
            },
            {
                "series_id": "JTS000000000000000JOL",
                "event_name": "JOLTS Job Openings",
                "value_scale": 0.001,
                "value_precision": 3,
                "unit": "million",
                "release_utc_by_reference": {
                    "June 2026": "2026-08-04T14:00:00Z"
                },
            },
        ],
    }

    rows = news.parse_bls_timeseries_batch(payload, source)

    assert [row["event_series_id"] for row in rows] == [
        "CUSR0000SA0",
        "JTS000000000000000JOL",
    ]
    assert {row["source_id"] for row in rows} == {"bls_major_timeseries_batch_v1"}
    assert {row["source_contract_id"] for row in rows} == {
        "bls_major_timeseries_v4_source_lineage_20260819"
    }
    assert {row["source_cohort_id"] for row in rows} == {
        "bls_major_timeseries_v4_source_lineage_20260819"
    }
    assert rows[0]["published_utc"] == "2026-08-12T12:30:00+00:00"
    assert rows[0]["published_time_inferred"] is False
    assert rows[1]["published_utc"] == "2026-08-04T14:00:00+00:00"
    assert rows[1]["published_time_inferred"] is False
    assert rows[0]["actual_value"] == pytest.approx(0.340452, abs=0.000001)
    assert rows[0]["previous_value"] == pytest.approx(0.186047, abs=0.000001)
    assert rows[1]["actual_value"] == 7.359


def test_bls_api_failure_is_not_silently_treated_as_empty_success():
    payload = json.dumps(
        {
            "status": "REQUEST_NOT_PROCESSED",
            "message": ["Request could not be serviced because the daily threshold was reached"],
            "Results": {},
        }
    ).encode()

    with pytest.raises(ValueError, match="daily threshold"):
        news.parse_bls_timeseries_batch(payload, {"series": []})


def test_bls_daily_quota_error_waits_for_next_new_york_day():
    now = dt.datetime(2026, 8, 14, 13, 45, tzinfo=UTC)
    retry = news.provider_parse_retry_after(
        "bls_timeseries_batch",
        ValueError("Request could not be serviced because the daily threshold was reached"),
        now,
    )
    assert retry == "2026-08-15T04:05:00+00:00"
    assert news.due_for_poll(
        {"kind": "bls_timeseries_batch"},
        {"retry_after_utc": retry},
        {},
        dt.datetime(2026, 8, 14, 20, 0, tzinfo=UTC),
    ) is False
    assert news.provider_parse_retry_after("rss", "daily threshold", now) == ""


def test_bls_batch_uses_release_bursts_and_quota_safe_idle_cadence():
    source = {
        "kind": "bls_timeseries_batch",
        "poll_interval_sec": 21600,
        "burst_poll_interval_sec": 300,
        "poll_timezone": "America/New_York",
        "burst_poll_windows": [
            {"start": "08:25", "end": "08:45"},
            {"start": "09:55", "end": "10:10"},
        ],
    }
    burst_now = dt.datetime(2026, 8, 14, 12, 30, tzinfo=UTC)
    assert not news.due_for_poll(
        source,
        {"last_attempt_utc": (burst_now - dt.timedelta(seconds=299)).isoformat()},
        {},
        burst_now,
    )
    assert news.due_for_poll(
        source,
        {"last_attempt_utc": (burst_now - dt.timedelta(seconds=301)).isoformat()},
        {},
        burst_now,
    )
    idle_now = dt.datetime(2026, 8, 14, 15, 0, tzinfo=UTC)
    assert not news.due_for_poll(
        source,
        {"last_attempt_utc": (idle_now - dt.timedelta(hours=5)).isoformat()},
        {},
        idle_now,
    )
    assert news.due_for_poll(
        source,
        {"last_attempt_utc": (idle_now - dt.timedelta(hours=6, seconds=1)).isoformat()},
        {},
        idle_now,
    )


def test_active_release_burst_sources_are_collected_first_stably():
    sources = [
        {"source_id": "ordinary_official"},
        {
            "source_id": "census",
            "poll_timezone": "America/New_York",
            "burst_poll_windows": [{"start": "08:25", "end": "08:45"}],
        },
        {
            "source_id": "bls",
            "poll_timezone": "America/New_York",
            "burst_poll_windows": [{"start": "08:25", "end": "08:45"}],
        },
        {"source_id": "discovery"},
    ]
    release = dt.datetime(2026, 8, 14, 12, 30, tzinfo=UTC)
    assert [row["source_id"] for row in news.collection_order(sources, release)] == [
        "census", "bls", "ordinary_official", "discovery"
    ]
    outside = dt.datetime(2026, 8, 14, 15, 30, tzinfo=UTC)
    assert news.collection_order(sources, outside) == sources


def test_requests_transport_fetches_and_parses_official_atom_feed(monkeypatch):
    payload = b"""<?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
      <title>Statistics Canada: The Daily</title>
      <entry>
        <id>https://www150.statcan.gc.ca/n1/daily-quotidien/260814/example-eng.htm</id>
        <title>Consumer Price Index, July 2026</title>
        <updated>2026-08-14T08:30:00-04:00</updated>
        <link href="https://www150.statcan.gc.ca/n1/daily-quotidien/260814/example-eng.htm" />
        <summary>Consumer prices increased in July.</summary>
      </entry>
    </feed>"""

    class Raw:
        def read(self, limit=-1, decode_content=False):
            return payload[:limit] if limit >= 0 else payload

    class Response:
        status_code = 200
        headers = {"ETag": '"statcan-v1"'}
        raw = Raw()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def iter_content(self, chunk_size=65_536):
            yield payload[:37]
            yield payload[37:]

    calls = []

    class Requests:
        @staticmethod
        def request(method, url, **kwargs):
            calls.append((method, url, kwargs))
            return Response()

    monkeypatch.setattr(news, "requests", Requests)
    rows, state = news.fetch_source(
        {
            "source_id": "statcan_daily_releases",
            "name": "Statistics Canada daily releases",
            "kind": "rss",
            "url": "https://www150.statcan.gc.ca/n1/rss/dai-quo/0-eng.atom",
            "http_transport": "requests",
            "currencies": ["CAD"],
            "verified": True,
            "direct": True,
            "trusted_domains": ["statcan.gc.ca"],
        },
        {},
        timeout_sec=2.0,
        maximum_bytes=10_000,
        now=dt.datetime(2026, 8, 14, 12, 31, tzinfo=UTC),
    )

    assert calls[0][0] == "GET"
    assert calls[0][2]["stream"] is True
    assert len(rows) == 1
    assert rows[0]["title"] == "Consumer Price Index, July 2026"
    assert state["last_status"] == 200
    assert state["response_bytes"] == len(payload)
    assert state["http_transport"] == "requests"
    assert state["etag"] == '"statcan-v1"'


def test_statcan_source_is_governed_requests_transport_with_release_burst():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"] if row.get("source_id") == "statcan_daily_releases"
    )
    assert source["enabled"] is True
    assert source["runtime_supported"] is True
    assert source["http_transport"] == "requests"
    assert source["source_contract_id"] == "statcan_daily_releases_requests_v3_20260814"
    assert source["source_cohort_id"] == "statcan_daily_releases_requests_v3_20260814"
    assert source["numeric_parser_activated_utc"] == "2026-08-14T12:43:00Z"
    burst_now = dt.datetime(2026, 8, 14, 12, 30, tzinfo=UTC)
    assert news.due_for_poll(
        source,
        {"last_attempt_utc": (burst_now - dt.timedelta(seconds=121)).isoformat()},
        {},
        burst_now,
    )


def test_ons_source_follows_current_official_bulletin_prospectively():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"] if row.get("source_id") == "ons_published_releases"
    )
    assert source["detail_enrichment"] == "ons_release_bulletin"
    assert source["source_contract_id"] == (
        "ons_published_releases_numeric_context_v5_labour_bundle_20260818"
    )
    assert source["detail_bundle_max_items"] == 8
    assert source["detail_context_archive_only"] is True
    assert source["detail_context_url_patterns"]
    assert source["detail_max_age_minutes"] == 20


def test_ons_release_listing_enriches_from_linked_official_bulletin(monkeypatch):
    listing_url = "https://www.ons.gov.uk/releases/gdpfirstquarterlyestimate"
    bulletin_url = (
        "https://www.ons.gov.uk/economy/grossdomesticproductgdp/"
        "bulletins/gdpfirstquarterlyestimateuk/apriltojune2026"
    )

    class Response:
        headers = {"Content-Type": "text/html; charset=utf-8"}

        def __init__(self, url, body):
            self.url = url
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def geturl(self):
            return self.url

        def read(self, _maximum):
            return self.body

    def fake_open(request, **_kwargs):
        url = request.full_url
        if url == listing_url:
            return Response(
                listing_url,
                (
                    '<html><a href="/economy/grossdomesticproductgdp/'
                    'bulletins/gdpfirstquarterlyestimateuk/apriltojune2026">'
                    'GDP first quarterly estimate</a></html>'
                ).encode(),
            )
        assert url == bulletin_url
        return Response(
            bulletin_url,
            (
                "<html><main>UK real gross domestic product increased by "
                "0.4% in Quarter 2 2026, following growth of 0.6% in Quarter "
                "1. June GDP grew 0.3% after no growth in May, revised down "
                "from 0.1%.</main></html>"
            ).encode(),
        )

    monkeypatch.setattr(news.urllib.request, "urlopen", fake_open)
    article = {
        "title": "GDP first quarterly estimate, UK: April to June 2026",
        "summary": "First quarterly estimate of GDP.",
        "url": listing_url,
        "published_utc": "2026-08-13T06:00:00Z",
    }
    enriched, first_seen, error = news.enrich_recent_official_release_details(
        [article],
        {
            "source_id": "ons_published_releases",
            "detail_enrichment": "ons_release_bulletin",
            "trusted_domains": ["ons.gov.uk"],
        },
        {},
        timeout_sec=10,
        maximum_bytes=8_000_000,
        now=dt.datetime(2026, 8, 13, 6, 1, 46, tzinfo=UTC),
    )
    assert enriched == 1
    assert error == ""
    assert len(first_seen) == 1
    assert article["detail_enrichment_kind"] == "ons_release_bulletin"
    assert article["detail_enrichment_research_only"] is True
    assert article["detail_source_url"] == bulletin_url
    assert "increased by 0.4%" in article["summary"]


def test_external_adapter_source_is_reported_without_duplicate_collection():
    source = {
        "source_id": "cftc_cot_positioning",
        "enabled": True,
        "runtime_supported": True,
        "externally_managed": True,
        "runtime_adapter": "oanda_cftc_positioning_shadow.py",
    }
    assert news.source_runtime_status(source) == "external_adapter"
    health = news.summarize_source_health([source], {})
    assert health["configured"] == 1
    assert health["external_adapter"] == 1
    assert health["enabled"] == 0


def test_recent_existing_official_item_can_be_enriched_without_backdating(
    monkeypatch,
):
    class Response:
        headers = {"Content-Type": "text/html"}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return "https://www.abs.gov.au/media-centre/release"

        def read(self, _maximum):
            return (
                b"<html><main>Official release body with enough factual "
                b"content to pass document quality validation.</main></html>"
            )

    monkeypatch.setattr(
        news.urllib.request, "urlopen", lambda *_args, **_kwargs: Response()
    )
    now = dt.datetime(2026, 8, 19, 3, 30, tzinfo=UTC)
    article = {
        "title": "Recent official release",
        "url": "https://www.abs.gov.au/media-centre/release",
        "published_utc": "2026-08-19T02:00:00Z",
        "source_listing_bootstrap": True,
    }
    enriched, first_seen, error = news.enrich_recent_official_release_details(
        [article],
        {
            "source_id": "abs_latest_releases",
            "detail_enrichment": "official_document_text",
            "detail_enrich_recent_existing_items": True,
            "detail_max_age_minutes": 180,
            "trusted_domains": ["abs.gov.au"],
        },
        {},
        timeout_sec=10,
        maximum_bytes=1_000_000,
        now=now,
    )
    assert enriched == 1
    assert error == ""
    assert first_seen[article["url"]] == "2026-08-19T03:30:00+00:00"
    assert article["source_listing_bootstrap"] is True
    assert article["detail_existing_item_observed_late"] is True
    assert article["detail_available_utc"] == "2026-08-19T03:30:00+00:00"
    assert article["detail_enrichment_research_only"] is True


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("27/08/2026 11:30am AEST", "2026-08-27T01:30:00+00:00"),
        ("03/12/2026 2.05pm AEDT", "2026-12-03T03:05:00+00:00"),
    ],
)
def test_abs_official_page_release_clock_is_narrow_and_timezone_exact(
    label, expected
):
    parsed = news.parse_abs_official_page_release_clock(
        f"Released today. Release date and time {label} Other text."
    )
    assert news.iso_utc(parsed) == expected
    assert news.parse_abs_official_page_release_clock(
        "Meeting date and time 27/08/2026 11:30am AEST"
    ) is None
    assert news.parse_abs_official_page_release_clock(
        "Release date and time 27/08/2026 11:30am UTC"
    ) is None


def test_abs_late_detail_clock_repairs_event_time_without_backdating_knowledge(
    monkeypatch,
):
    class Response:
        headers = {"Content-Type": "text/html"}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return (
                "https://www.abs.gov.au/media-centre/media-releases/"
                "new-capital-expenditure-down-36-cent-june"
            )

        def read(self, _maximum):
            return (
                b"<html><main>New capital expenditure down 3.6 per cent. "
                b"Release date and time 27/08/2026 11:30am AEST. "
                b"Private new capital expenditure fell in the June quarter."
                b"</main></html>"
            )

    monkeypatch.setattr(
        news.urllib.request, "urlopen", lambda *_args, **_kwargs: Response()
    )
    first_seen = dt.datetime(2026, 8, 27, 3, 58, 40, tzinfo=UTC)
    article = {
        "source_id": "abs_latest_releases",
        "source_name": "Australian Bureau of Statistics latest releases",
        "source_kind": "html_links",
        "source_role": "primary_statistical_release",
        "source_quality": 0.98,
        "source_verified": True,
        "source_direct": True,
        "source_currencies": ["AUD"],
        "title": "Media Release - New capital expenditure down 3.6 per cent in June",
        "url": (
            "https://www.abs.gov.au/media-centre/media-releases/"
            "new-capital-expenditure-down-36-cent-june"
        ),
        "published_utc": "",
        "source_listing_new_item": True,
    }
    enriched, detail_times, error = news.enrich_recent_official_release_details(
        [article],
        {
            "source_id": "abs_latest_releases",
            "detail_enrichment": "official_document_text",
            "detail_max_age_minutes": 180,
            "trusted_domains": ["abs.gov.au"],
        },
        {},
        timeout_sec=10,
        maximum_bytes=1_000_000,
        now=first_seen,
    )
    assert enriched == 1 and error == ""
    assert article["published_utc"] == "2026-08-27T01:30:00+00:00"
    assert article["published_time_inferred"] is False
    assert article["publication_clock_known_utc"] == (
        "2026-08-27T03:58:40+00:00"
    )
    assert detail_times[article["url"]] == "2026-08-27T03:58:40+00:00"

    classified = news.classify_article(article, first_seen=first_seen)
    assert classified["published_utc"] == "2026-08-27T01:30:00+00:00"
    assert classified["first_seen_utc"] == "2026-08-27T03:58:40+00:00"
    assert classified["causal_known_utc"] == "2026-08-27T03:58:40+00:00"
    assert classified["availability_lag_minutes"] == pytest.approx(148.666667)
    assert classified["forward_signal_timely"] is False
    assert classified["directional_publish_eligible"] is False


def test_alfred_vintage_config_projects_the_governed_external_worker():
    config = json.loads(
        (news.ROOT / "config" / "news_sources_v1.json").read_text(
            encoding="utf-8"
        )
    )
    source = next(
        row for row in config["sources"]
        if row["source_id"] == "fred_alfred_vintages"
    )
    assert source["enabled"] is True
    assert source["externally_managed"] is True
    assert source["runtime_adapter"] == "oanda_alfred_vintage_prospective.py"
    assert source["prospective_state"].endswith(
        "alfred_vintage_prospective_v1.json"
    )
    assert news.source_runtime_status(source) == "external_adapter"


def test_transport_change_bypasses_retired_adapter_backoff():
    now = dt.datetime(2026, 8, 14, 12, 30, tzinfo=UTC)
    source = {
        "kind": "rss",
        "http_transport": "requests",
        "poll_interval_sec": 21600,
    }
    state = {
        "last_attempt_utc": now.isoformat(),
        "http_transport": "urllib",
        "consecutive_errors": 7,
    }
    assert news.due_for_poll(source, state, {}, now) is True


def test_collector_code_cohort_change_forces_one_source_retry():
    now = dt.datetime(2026, 8, 19, 3, 40, tzinfo=UTC)
    source = {
        "source_contract_id": "source-v1",
        "poll_interval_sec": 3600,
    }
    state = {
        "source_contract_id": "source-v1",
        "collector_contract_id": "older-collector",
        "last_attempt_utc": now.isoformat(),
    }
    assert news.due_for_poll(source, state, {}, now) is True


def test_curl_transport_fetches_rss_without_exposing_credentials(monkeypatch):
    payload = (
        b'<?xml version="1.0"?><rss><channel><item>'
        b'<title>Policy statement</title>'
        b'<link>https://www.resbank.co.za/example</link>'
        b'<pubDate>Fri, 14 Aug 2026 12:00:00 GMT</pubDate>'
        b'</item></channel></rss>'
    )
    calls = []

    class Completed:
        returncode = 0
        stdout = payload + b"\n__FOREX_HTTP_STATUS__:200"
        stderr = b""

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return Completed()

    monkeypatch.setattr(news.subprocess, "run", fake_run)
    rows, state = news.fetch_source(
        {
            "source_id": "sarb",
            "name": "SARB",
            "kind": "rss",
            "url": "https://www.resbank.co.za/rss",
            "http_transport": "curl",
            "curl_retry_count": 2,
            "curl_fail_http_errors": True,
            "curl_ip_version": 6,
            "curl_ssl_revoke_best_effort": True,
            "headers": {"Connection": "close"},
            "suppress_default_headers": ["Accept-Encoding"],
            "currencies": ["ZAR"],
            "verified": True,
            "direct": True,
            "trusted_domains": ["resbank.co.za"],
        },
        {},
        timeout_sec=2.0,
        maximum_bytes=10_000,
        now=dt.datetime(2026, 8, 14, 12, 1, tzinfo=UTC),
    )

    assert len(rows) == 1
    assert rows[0]["title"] == "Policy statement"
    assert state["last_status"] == 200
    assert state["http_transport"] == "curl"
    assert state["response_bytes"] == len(payload)
    assert calls[0][1]["capture_output"] is True
    assert "--max-filesize" in calls[0][0]
    assert calls[0][0][calls[0][0].index("--retry") + 1] == "2"
    assert "--retry-all-errors" in calls[0][0]
    assert "--fail" in calls[0][0]
    assert "--ipv6" in calls[0][0]
    assert "--ssl-revoke-best-effort" in calls[0][0]
    assert not any(
        "Accept-Encoding" in argument for argument in calls[0][0]
    )


def test_curl_fail_mode_retains_gateway_error_when_all_retries_fail(monkeypatch):
    class Completed:
        returncode = 22
        stdout = b"\n__FOREX_HTTP_STATUS__:502"
        stderr = b"curl: (22) The requested URL returned error: 502"

    monkeypatch.setattr(news.subprocess, "run", lambda *args, **kwargs: Completed())
    rows, state = news.fetch_source(
        {
            "source_id": "official_gateway",
            "name": "Official gateway",
            "kind": "rss",
            "url": "https://example.gov/feed",
            "http_transport": "curl",
            "curl_retry_count": 3,
            "curl_fail_http_errors": True,
            "currencies": ["HKD"],
            "verified": True,
            "direct": True,
        },
        {},
        timeout_sec=2.0,
        maximum_bytes=10_000,
        now=dt.datetime(2026, 8, 16, 12, 0, tzinfo=UTC),
    )

    assert rows == []
    assert state["last_status"] == 502
    assert state["consecutive_errors"] == 1
    assert "502" in state["last_error"]
    assert state["http_transport"] == "curl"


def test_external_adapter_runtime_state_clears_retired_parser_error(tmp_path):
    adapter = tmp_path / "adapter.json"
    adapter.write_text(
        json.dumps(
            {
                "status": "ok",
                "generated_utc": "2026-08-14T12:00:00+00:00",
                "source_contract_id": "contract-v2",
                "source_cohort_id": "cohort-v2",
                "currency_count": 8,
                "evidence": {"immutable_observations": 16},
            }
        ),
        encoding="utf-8",
    )
    state = news.external_adapter_runtime_state(
        {
            "externally_managed": True,
            "runtime_adapter": "adapter.py",
            "prospective_state": str(adapter),
        },
        {"last_error": "parse_error", "consecutive_errors": 1},
    )
    assert state["last_error"] == ""
    assert state["consecutive_errors"] == 0
    assert state["parsed_items"] == 16
    assert state["external_source_cohort_id"] == "cohort-v2"


def test_fresh_statcan_release_extracts_source_native_change_without_direction():
    first_seen = dt.datetime(2026, 8, 14, 12, 32, 47, tzinfo=UTC)
    article = news.classify_article(
        {
            "source_id": "statcan_daily_releases",
            "source_name": "Statistics Canada",
            "source_kind": "rss",
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_statistical_release",
            "source_currencies": ["CAD"],
            "numeric_parser_activated_utc": "2026-08-14T12:43:00Z",
            "title": "Monthly Survey of Manufacturing, June 2026",
            "summary": "Manufacturing sales edged up 0.1% in June.",
            "url": "https://www.statcan.gc.ca/daily-quotidien/260814/a-eng.htm",
            "published_utc": "2026-08-14T12:30:00Z",
        },
        first_seen=first_seen,
    )
    assert article["structured_event"] is True
    assert article["event_series_id"] == "statcan_manufacturing_sales_mom"
    assert article["actual_value"] == 0.1
    assert article["numeric_causal_known_utc"] == "2026-08-14T12:43:00+00:00"
    assert article["numeric_direction_policy"] == "abstain_and_learn_response"
    assert article["currency_scores"] == {}


def test_fresh_statcan_labour_release_preserves_employment_and_unemployment_actuals():
    first_seen = dt.datetime(2026, 8, 7, 12, 30, 21, tzinfo=UTC)
    article = news.classify_article(
        {
            "source_id": "statcan_daily_releases",
            "source_name": "Statistics Canada",
            "source_kind": "rss",
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_statistical_release",
            "source_currencies": ["CAD"],
            "numeric_parser_activated_utc": "2026-08-15T21:30:00Z",
            "title": "Labour Force Survey, July 2026",
            "summary": (
                "Employment increased by 75,000 (+0.4%) in July and the "
                "employment rate rose 0.1 percentage points to 60.9%. The "
                "unemployment rate declined 0.1 percentage points to 6.4%."
            ),
            "url": "https://www.statcan.gc.ca/daily-quotidien/260807/a-eng.htm",
            "published_utc": "2026-08-07T12:30:00Z",
        },
        first_seen=first_seen,
    )
    assert article["structured_event"] is True
    assert article["event_series_id"] == "statcan_employment_change"
    assert article["actual_value"] == 75000.0
    assert article["unit"] == "persons"
    assert article["source_native_components"]["employment_change"][
        "actual_value"
    ] == 75000.0
    assert article["source_native_components"]["unemployment_rate"][
        "actual_value"
    ] == 6.4
    assert article["numeric_causal_known_utc"] == "2026-08-15T21:30:00+00:00"
    assert article["numeric_direction_policy"] == "abstain_and_learn_response"
    assert article["currency_scores"] == {}


def test_old_statcan_listing_row_never_becomes_structured_backfill():
    fields = news.official_numeric_release_fields(
        {
            "source_id": "statcan_daily_releases",
            "source_verified": True,
            "source_direct": True,
            "title": "Wholesale trade, June 2026",
            "summary": "Wholesale sales rose 2.8% in June.",
            "published_utc": "2026-08-01T12:30:00Z",
            "numeric_parser_activated_utc": "2026-08-14T12:43:00Z",
        },
        first_seen=dt.datetime(2026, 8, 14, 12, 32, tzinfo=UTC),
    )
    assert fields == {}


def test_secondary_nz_unemployment_surprise_is_research_only_and_not_backdated():
    first_seen = dt.datetime(2026, 8, 4, 22, 47, 21, tzinfo=UTC)
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "FXStreet",
            "source_kind": "rss",
            "source_verified": False,
            "source_direct": False,
            "source_role": "aggregator_discovery",
            "title": (
                "New Zealand’s Unemployment Rate climbs to 5.6% in Q2 "
                "vs. 5.4% expected - FXStreet"
            ),
            "summary": "",
            "url": "https://news.google.com/example",
            "published_utc": "2026-08-04T22:45:51+00:00",
        },
        first_seen=first_seen,
    )
    assert article["structured_event"] is True
    assert article["event_series_id"] == "stats_nz_unemployment_rate"
    assert article["actual_value"] == 5.6
    assert article["consensus_value"] == 5.4
    assert article["surprise_raw"] == pytest.approx(0.2)
    assert article["consensus_capture_state"] == (
        "post_release_reference_not_pre_release_capture"
    )
    assert article["numeric_direction_policy"] == (
        "research_only_post_release_reference"
    )
    assert article["numeric_causal_known_utc"] == (
        news.SECONDARY_MACRO_HEADLINE_PARSER_ACTIVATED_UTC
    )
    assert article["causal_known_utc"] == (
        news.SECONDARY_MACRO_HEADLINE_PARSER_ACTIVATED_UTC
    )
    assert article["currency_scores"]["NZD"] < 0
    assert article["directional_research_only"] is True
    assert article["directional_publish_eligible"] is False
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["NZD"] < 0
    assert topic["directional_publish_eligible"] is False


def test_secondary_nz_employment_preview_does_not_invent_numeric_surprise():
    fields = news.secondary_macro_headline_numeric_fields(
        {
            "source_verified": False,
            "title": "New Zealand dollar steadies ahead of employment data",
        },
        first_seen=dt.datetime(2026, 8, 4, 22, 30, tzinfo=UTC),
    )
    assert fields == {}


def test_secondary_japan_machinery_orders_preserves_late_surprise_research_only():
    article = news.classify_article(
        {
            "source_id": "finnhub_fx_market_news",
            "source_name": "Forexlive",
            "source_kind": "finnhub_news",
            "source_verified": False,
            "source_direct": False,
            "source_role": "aggregator_discovery",
            "title": "Japan June Machine Orders +16.9% y/y (expected +10.8%)",
            "summary": (
                "Machinery Orders (YoY) (June 2026) +16.9% expected +10.8%, "
                "prior -1.9% Machinery Orders (MoM) +9.7% m/m expected +7.8%, "
                "prior -12.4%"
            ),
            "published_utc": "2026-08-18T23:50:40+00:00",
        },
        first_seen=dt.datetime(2026, 8, 19, 0, 5, 45, tzinfo=UTC),
    )
    assert article["structured_event"] is True
    assert article["event_series_id"] == "japan_machinery_orders_yoy"
    assert article["actual_value"] == 16.9
    assert article["consensus_value"] == 10.8
    assert article["previous_value"] == -1.9
    assert article["surprise_raw"] == pytest.approx(6.1)
    assert article["category"] == "manufacturing_release"
    assert article["currencies"] == ["JPY"]
    assert article["consensus_capture_state"] == (
        "post_release_reference_not_pre_release_capture"
    )
    assert article["forward_signal_timely"] is False
    assert article["directional_research_only"] is True
    assert article["directional_publish_eligible"] is False
    assert article["execution_eligible"] is False


def test_parse_bea_release_blurb_and_preserve_detail_availability():
    page = b"""
    <html><body><main>
      <p>The U.S. Census Bureau and the U.S. Bureau of Economic Analysis
      announced today that the goods and services deficit was $73.3 billion
      in June, down $4.4 billion from $77.6 billion in May, revised.</p>
      <p>June exports were $314.7 billion, $2.9 billion less than May exports.
      June imports were $388.0 billion, $7.3 billion less than May imports.</p>
    </main></body></html>
    """
    blurb = news.parse_bea_release_blurb(page)
    assert blurb.startswith("The goods and services deficit was $73.3 billion")
    assert "June exports were $314.7 billion" in blurb
    assert "June imports were $388.0 billion" in blurb

    article = news.classify_article(
        {
            "source_id": "bea_releases",
            "source_name": "US Bureau of Economic Analysis releases",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_statistical_release",
            "source_currencies": ["USD"],
            "title": "U.S. International Trade in Goods and Services, June 2026",
            "summary": blurb,
            "url": "https://www.bea.gov/news/2026/trade-june-2026",
            "published_utc": "2026-08-04T12:30:00+00:00",
            "detail_enriched": True,
            "detail_enrichment_kind": "bea_release_blurb",
            "detail_available_utc": "2026-08-04T12:35:00+00:00",
        },
        first_seen=dt.datetime(2026, 8, 4, 12, 31, tzinfo=UTC),
    )
    assert article["detail_enriched"] is True
    assert article["causal_known_utc"] == "2026-08-04T12:35:00+00:00"
    assert article["currency_scores"] == {}
    assert article["context_only"] is True

    stored = news.article_storage_payload(article)
    assert "causal_known_utc" not in stored
    assert "event_lineage_id" not in stored
    assert "material_update_id" not in stored
    assert "publication_hold_seconds" not in stored
    assert (
        news.iso_utc(news.causal_known_datetime(stored))
        == "2026-08-04T12:35:00+00:00"
    )


def test_hawkish_source_is_currency_bullish_and_research_only():
    first_seen = dt.datetime(2026, 7, 27, 20, 2, tzinfo=UTC)
    article = news.classify_article(
        {
            "source_id": "fed",
            "source_name": "Federal Reserve",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_currencies": ["USD"],
            "title": "Federal Reserve raises interest rate",
            "summary": "Inflation remains above forecast",
            "url": "https://www.federalreserve.gov/example",
            "published_utc": "2026-07-27T20:00:00Z",
        },
        first_seen=first_seen,
    )
    assert article["currency_scores"]["USD"] > 0
    assert article["directional_bias"]["USD"] == "BULLISH"
    assert article["first_seen_utc"] == "2026-07-27T20:02:00+00:00"
    assert article["published_utc"] == "2026-07-27T20:00:00+00:00"
    assert article["research_only"] is True
    assert article["execution_eligible"] is False


def test_negated_hike_is_not_treated_as_confirmed_hawkish_policy():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "ING Think",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Bank of England some way off a rate hike despite energy price spike",
            "summary": "",
            "url": "https://example.com/boe-preview",
            "published_utc": "2026-07-29T10:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 29, 10, 1, tzinfo=UTC),
    )
    assert article["monetary_impulse"] == 0
    assert "GBP" not in article["currency_scores"]


def test_hike_with_explicitly_negated_currency_support_is_context_only():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "investingLive",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Three reasons why BOJ rate hikes will not save the yen",
            "summary": "",
            "url": "https://example.com/boj-hikes-will-not-save-yen",
            "published_utc": "2026-08-14T10:38:24Z",
        },
        first_seen=dt.datetime(2026, 8, 14, 11, 15, tzinfo=UTC),
    )

    assert article["policy_assertion_status"] == "neutral_or_expected_hold"
    assert article["monetary_impulse"] == 0
    assert article["currency_scores"] == {}
    assert article["context_only"] is True


def test_following_unlikely_phrase_is_not_treated_as_confirmed_hike():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "The Australia Today",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Australian inflation has eased a little, "
                "An August interest rate rise now looks unlikely"
            ),
            "summary": "",
            "url": "https://example.com/australia-rate-preview",
            "published_utc": "2026-07-29T23:21:35Z",
        },
        first_seen=dt.datetime(2026, 7, 29, 23, 23, tzinfo=UTC),
    )
    assert article["policy_assertion_status"] == "unverified_speculation"
    assert article["monetary_impulse"] == 0
    assert article["currency_scores"] == {}
    assert article["context_only"] is True
    assert article["relevant"] is False


def test_unverified_policy_speculation_is_context_not_direction():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Readers.id",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Federal Reserve weighs interest rate hike amid inflation concerns",
            "summary": "",
            "url": "https://example.com/fed-speculation",
            "published_utc": "2026-07-29T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 29, 12, 1, tzinfo=UTC),
    )
    assert article["policy_assertion_status"] == "unverified_speculation"
    assert article["monetary_impulse"] == 0
    assert "USD" not in article["currency_scores"]
    assert article["context_only"] is True
    assert article["relevant"] is False


def test_unverified_rate_hike_expectations_are_not_hawkish_policy_action():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Crypto Briefing",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Emerging-market stocks rise as Federal Reserve eases "
                "rate hike expectations"
            ),
            "summary": "",
            "url": "https://example.com/fed-rate-hike-expectations",
            "published_utc": "2026-08-10T04:47:17Z",
        },
        first_seen=dt.datetime(2026, 8, 10, 4, 51, tzinfo=UTC),
    )
    assert article["policy_assertion_status"] == "unverified_speculation"
    assert article["monetary_impulse"] == 0
    assert "USD" not in article["currency_scores"]
    assert "#usd_hawkish_guidance" not in article["topic_tags"]
    assert article["context_only"] is True


def test_unverified_rate_hike_fears_are_not_hawkish_policy_action():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Whalesbook",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Indian Markets Trade Mixed As US Job Losses Ease Rate Hike Fears",
            "summary": "",
            "url": "https://example.com/rate-hike-fears",
            "published_utc": "2026-08-10T05:36:14Z",
        },
        first_seen=dt.datetime(2026, 8, 10, 6, 1, tzinfo=UTC),
    )
    assert article["policy_assertion_status"] == "unverified_speculation"
    assert article["monetary_impulse"] == 0
    assert article["currency_scores"] == {}
    assert article["context_only"] is True


def test_expected_policy_hold_is_directionally_neutral():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Example News",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Federal Reserve expected to hold interest rates as meeting starts",
            "summary": "Markets debate whether a rate hike could come later.",
            "url": "https://example.com/fed-hold",
            "published_utc": "2026-07-29T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 29, 12, 1, tzinfo=UTC),
    )
    assert article["policy_assertion_status"] == "neutral_or_expected_hold"
    assert article["monetary_impulse"] == 0
    assert "USD" not in article["currency_scores"]


def test_risk_off_maps_havens_against_risk_currencies():
    article = news.classify_article(
        {
            "source_id": "gdelt",
            "source_name": "example.com",
            "source_kind": "gdelt",
            "source_quality": 0.65,
            "source_verified": False,
            "source_currencies": [],
            "title": "Military attack triggers global risk-off market selloff",
            "summary": "Currency and oil markets react to geopolitical tensions",
            "url": "https://example.com/risk",
            "published_utc": "2026-07-27T20:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 27, 20, 1, tzinfo=UTC),
    )
    assert article["category"] == "risk_off_geopolitical_or_financial"
    assert article["currency_scores"]["JPY"] > 0
    assert article["currency_scores"]["CHF"] > 0
    assert article["currency_scores"]["AUD"] < 0
    assert article["scope"] == "all_pairs"
    assert article["context_only"] is False
    assert article["relevant"] is True


def test_company_growth_noise_is_kept_for_context_but_not_active_fx():
    article = news.classify_article(
        {
            "source_id": "gdelt",
            "source_name": "example.com",
            "source_kind": "gdelt",
            "source_quality": 0.65,
            "source_verified": False,
            "source_currencies": [],
            "title": "Allergy Therapeutics reports accelerated revenue growth",
            "summary": "",
            "url": "https://example.com/company-growth",
            "published_utc": "2026-07-29T09:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 29, 9, 1, tzinfo=UTC),
    )
    assert article["category"] == "growth_release"
    assert article["currency_scores"] == {}
    assert article["context_only"] is True
    assert article["context_reason"] == "no_deterministic_fx_impulse"
    assert article["relevant"] is False


def test_primary_spending_release_keeps_absolute_direction_research_only():
    article = news.classify_article(
        {
            "source_id": "abs_latest_releases",
            "source_name": "Australian Bureau of Statistics latest releases",
            "source_kind": "html_links",
            "source_role": "primary_statistical_release",
            "source_quality": 0.98,
            "source_verified": True,
            "source_currencies": ["AUD"],
            "title": "Media Release - Household spending up 0.8% in June",
            "summary": "",
            "url": "https://www.abs.gov.au/example",
        },
        first_seen=dt.datetime(2026, 8, 4, 1, 29, tzinfo=UTC),
    )

    assert article["category"] == "growth_release"
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False
    assert article["execution_eligible"] is False
    assert article["research_currency_scores"] == {"AUD": 0.25}
    assert article["research_directional_basis"] == (
        "absolute_primary_spending_release_without_consensus"
    )
    assert article["context_only"] is True
    assert article["context_reason"] == (
        "primary_release_absolute_direction_research_only"
    )

    pair = news.build_pair_scores(
        [article],
        ["AUD_USD"],
        as_of=dt.datetime(2026, 8, 4, 1, 31, tzinfo=UTC),
    )["pairs"]["AUD_USD"]
    assert pair["direction"] == "NEUTRAL"
    assert pair["directional_event_count"] == 0
    assert pair["context_directional_event_count"] == 1
    assert pair["events"][0]["research_pair_score"] > 0


def test_secondary_household_cost_story_is_not_an_inflation_release_signal():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Nine.com.au",
            "title": (
                "‘Smashed by bills’: Australian families say they’re still "
                "bleeding cash as inflation slows - Nine.com.au"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/example",
            "publisher_url": "https://www.nine.com.au",
            "source_currencies": ["AUD"],
            "source_quality": 0.55,
            "source_verified": False,
        },
        first_seen=dt.datetime(2026, 7, 30, 19, 40, tzinfo=UTC),
    )
    assert article["category"] == "inflation_context"
    assert article["monetary_impulse"] == 0
    assert article["currency_scores"] == {}
    assert article["directional_evidence"] is False
    assert article["context_only"] is True
    assert article["context_reason"] == "secondary_household_cost_context"
    assert article["relevant"] is False


def test_secondary_inflation_opinion_is_context_not_a_statistical_release():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Example Commentary",
            "title": "Fed chair's inflation strategy: leave it to the market",
            "summary": "A columnist discusses the central bank's approach.",
            "url": "https://news.google.com/rss/articles/example-opinion",
            "publisher_url": "https://example.com",
            "source_currencies": ["USD"],
            "source_quality": 0.55,
            "source_verified": False,
        },
        first_seen=dt.datetime(2026, 8, 4, 11, 9, tzinfo=UTC),
    )
    assert article["category"] == "inflation_context"
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False
    assert article["execution_eligible"] is False


def test_secondary_inflation_data_claim_remains_an_inflation_release():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Example Wire",
            "title": "Korea inflation eases in July as core prices hit two-year high",
            "summary": "",
            "url": "https://news.google.com/rss/articles/example-data",
            "publisher_url": "https://example.com",
            "source_currencies": ["KRW"],
            "source_quality": 0.75,
            "source_verified": False,
        },
        first_seen=dt.datetime(2026, 8, 4, 10, 5, tzinfo=UTC),
    )
    assert article["category"] == "inflation_release"
    assert article["execution_eligible"] is False


def test_composite_labor_and_inflation_claim_is_not_flattened_to_direction():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Example Wire",
            "title": "U.S. economy stumbles with job losses and sticky inflation",
            "summary": "Weak payrolls conflict with persistent price pressure.",
            "url": "https://news.google.com/rss/articles/composite-us-report",
            "publisher_url": "https://example.com",
            "source_currencies": ["USD"],
            "source_quality": 0.75,
            "source_verified": False,
        },
        first_seen=dt.datetime(2026, 8, 7, 13, 40, tzinfo=UTC),
    )
    assert article["semantic_claim_conflict"] is True
    assert {
        claim["dimension"] for claim in article["semantic_claims"]
    } == {"labor_weakness", "inflation_persistence"}


def test_rising_inflation_and_weakening_labor_remain_conflicted():
    article = news.classify_article(
        {
            "source_id": "secondary-macro",
            "source_name": "Secondary macro publisher",
            "source_kind": "rss",
            "source_quality": 0.75,
            "source_verified": False,
            "source_currencies": ["USD"],
            "title": (
                "July Inflation Rises, But Weakening Labor Complicates "
                "Fed's Choice"
            ),
            "summary": "",
            "url": "https://example.com/mixed-inflation-labor",
            "published_utc": "2026-08-14T15:26:43Z",
        },
        first_seen=dt.datetime(2026, 8, 14, 15, 34, 49, tzinfo=UTC),
    )
    assert article["semantic_claim_conflict"] is True
    assert {
        claim["dimension"] for claim in article["semantic_claims"]
    } == {"labor_weakness", "inflation_persistence"}
    assert article["currency_scores"] == {}
    assert article["directional_bias"] == {}
    assert article["context_only"] is True
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False
    assert article["execution_eligible"] is False
    assert article["context_reason"] == "multi_claim_semantic_conflict"
    assert (
        article["research_directional_basis"]
        == "multi_claim_components_no_flattened_direction"
    )


def test_price_war_is_not_treated_as_geopolitical_risk_off():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Example News",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "EV price war intensifies as automakers cut prices",
            "summary": "",
            "url": "https://example.com/ev-price-war",
            "published_utc": "2026-07-29T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 29, 12, 1, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0
    assert article["currency_scores"] == {}
    assert article["context_only"] is True
    assert article["relevant"] is False


def test_enters_ai_price_war_does_not_retrigger_direct_conflict_escalation():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Example News",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "OpenAI, Anthropic enter AI price war as cheaper Chinese "
                "rivals gain ground"
            ),
            "summary": "",
            "url": "https://example.com/ai-price-war",
            "published_utc": "2026-08-14T14:33:40Z",
        },
        first_seen=dt.datetime(2026, 8, 14, 15, 5, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0
    assert article["currency_scores"] == {}
    assert article["relevant"] is False


def test_department_of_war_company_award_is_not_a_risk_off_event():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Financial Times",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Department of War Awards Lockheed Martin $58.62B "
                "for Multiyear PAC-3 MSE Production"
            ),
            "summary": "Company announcement",
            "url": "https://example.com/defense-contract",
            "published_utc": "2026-07-29T23:09:00Z",
        },
        first_seen=dt.datetime(2026, 7, 29, 23, 10, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0
    assert article["currency_scores"] == {}
    assert article["context_only"] is True
    assert article["relevant"] is False


def test_wage_gains_outpacing_war_inflation_is_context_not_escalation():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "AOL.com",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Working-class wage gains outpace Iran war inflation, "
                "new data show"
            ),
            "summary": "",
            "url": "https://example.com/wages-war-inflation",
            "published_utc": "2026-07-29T22:17:00Z",
        },
        first_seen=dt.datetime(2026, 7, 29, 22, 18, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0
    assert article["currency_scores"] == {}
    assert article["context_only"] is True
    assert article["relevant"] is False


def test_market_pricing_policy_preview_is_not_confirmed_action():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Example News",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Traders price in Fed rate hike ahead of key decision",
            "summary": "",
            "url": "https://example.com/fed-preview",
            "published_utc": "2026-07-29T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 29, 12, 1, tzinfo=UTC),
    )
    assert article["policy_assertion_status"] == "unverified_speculation"
    assert article["monetary_impulse"] == 0
    assert article["currency_scores"] == {}
    assert article["context_only"] is True
    assert article["relevant"] is False


def test_verified_neutral_policy_release_is_retained_as_active_evidence():
    article = news.classify_article(
        {
            "source_id": "fed_monetary_policy",
            "source_name": "Federal Reserve monetary policy",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_currencies": ["USD"],
            "title": "Federal Reserve issues FOMC statement",
            "summary": "",
            "url": "https://www.federalreserve.gov/example",
            "published_utc": "2026-07-29T18:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 29, 18, 1, tzinfo=UTC),
    )
    assert article["currency_scores"] == {}
    assert article["context_only"] is False
    assert article["relevant"] is True
    assert article["category"] == "monetary_policy"
    assert article["official_policy_release"] is True
    assert article["directional_evidence"] is False
    assert article["context_reason"] == "official_neutral_policy_evidence"


def test_war_on_involution_is_not_geopolitical_risk_off():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Example News",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Beijing's latest salvo in the war on involution",
            "summary": "",
            "url": "https://example.com/involution",
            "published_utc": "2026-07-29T18:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 29, 18, 1, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0
    assert article["currency_scores"] == {}
    assert article["context_only"] is True
    assert article["relevant"] is False


def test_ceasefire_ending_is_escalation_not_risk_on():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Example News",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Iran ends ceasefire as hostilities resume",
            "summary": "",
            "url": "https://example.com/ceasefire-ends",
            "published_utc": "2026-07-29T18:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 29, 18, 1, tzinfo=UTC),
    )
    assert article["risk_on_score"] == 0
    assert article["risk_off_score"] > 0
    assert article["category"] == "risk_off_geopolitical_or_financial"
    assert article["currency_scores"]["JPY"] > 0


def test_ceasefire_deal_over_hormuz_is_deescalation_not_failed_truce():
    article = news.classify_article(
        {
            "source_id": "google_news_systemic_catalyst",
            "source_name": "Example News",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Trump signals imminent US-Iran ceasefire deal over "
                "Strait of Hormuz"
            ),
            "summary": "",
            "url": "https://example.com/ceasefire-deal-over-hormuz",
            "published_utc": "2026-08-05T08:48:50Z",
        },
        first_seen=dt.datetime(2026, 8, 5, 8, 49, tzinfo=UTC),
    )
    assert article["risk_on_score"] > 0
    assert article["risk_off_score"] == 0
    assert article["category"] == "risk_on_deescalation"


def test_incidental_war_reference_is_context_not_global_signal():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Casino News",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "CBRE: Wynn can still open UAE casino in 2027 despite Iran war",
            "summary": "",
            "url": "https://example.com/casino",
            "published_utc": "2026-07-29T18:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 29, 18, 1, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0
    assert article["currency_scores"] == {}
    assert article["context_only"] is True
    assert article["relevant"] is False


def test_market_tug_of_war_is_not_geopolitical_risk_off():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "InteractiveCrypto",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Gold's Tug of War: Fed's Rate Hold Spurs Volatility "
                "Amid Inflation and Geopolitical Risks"
            ),
            "summary": "",
            "url": "https://example.com/tug-of-war",
            "published_utc": "2026-07-30T01:59:44Z",
        },
        first_seen=dt.datetime(2026, 7, 30, 2, 0, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0
    assert article["currency_scores"] == {}
    assert article["context_only"] is True
    assert article["relevant"] is False


def test_warning_does_not_match_war():
    article = news.classify_article(
        {
            "source_id": "gdelt",
            "source_name": "example.com",
            "source_kind": "gdelt",
            "source_quality": 0.65,
            "source_verified": False,
            "source_currencies": [],
            "title": "RBA governor issues wage warning as interest rates rise",
            "summary": "Australian inflation remains above forecast",
            "url": "https://example.com/rba-warning",
            "published_utc": "2026-07-27T20:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 27, 20, 1, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0
    assert article["category"] == "monetary_policy"
    assert article["currency_scores"]["AUD"] > 0


def test_gdelt_syndication_uses_normalized_headline_identity():
    first_seen = dt.datetime(2026, 7, 27, 20, 1, tzinfo=UTC)
    common = {
        "source_id": "gdelt",
        "source_kind": "gdelt",
        "source_quality": 0.65,
        "source_verified": False,
        "source_currencies": [],
        "summary": "",
        "published_utc": "2026-07-27T20:00:00Z",
    }
    first = news.classify_article(
        {
            **common,
            "source_name": "one.example",
            "title": "Oil prices fall as investors assess supply | Publisher One",
            "url": "https://one.example/story",
        },
        first_seen=first_seen,
    )
    second = news.classify_article(
        {
            **common,
            "source_name": "two.example",
            "title": "Oil prices fall as investors assess supply | Publisher Two",
            "url": "https://two.example/story",
        },
        first_seen=first_seen,
    )
    assert first["event_id"] == second["event_id"]


def test_headline_content_preserves_currency_clause_after_separator():
    title = "How a US - Japan pact to hit yen speculators came together"
    assert news.headline_content(title) == title

    article = news.classify_article(
        {
            "source_id": "gdelt",
            "source_name": "example.com",
            "source_kind": "gdelt",
            "source_quality": 0.65,
            "source_verified": False,
            "source_currencies": [],
            "title": title,
            "summary": "",
            "url": "https://example.com/pact",
            "published_utc": "2026-08-03T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 3, 12, 1, tzinfo=UTC),
    )
    assert article["currencies"] == ["JPY"]
    assert article["context_only"] is True
    assert article["relevant"] is False


def test_headline_content_removes_exact_publisher_suffix_even_with_country_name():
    title = "Will Britain change tax policy? - The Spectator Australia"
    assert news.headline_content(
        title,
        publisher_name="The Spectator Australia",
    ) == "Will Britain change tax policy?"

    article = news.classify_article(
        {
            "source_id": "google_news_trade_policy",
            "source_name": "The Spectator Australia",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": title,
            "summary": "",
            "url": "https://news.google.com/rss/articles/publisher-suffix",
            "published_utc": "2026-08-19T09:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 9, 1, tzinfo=UTC),
    )
    assert article["currencies"] == ["GBP"]
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_live_feed_edge_cases_remain_retrospective_context_or_fail_closed():
    first_seen = dt.datetime(2026, 8, 19, 15, 0, tzinfo=UTC)

    def classify(title: str, source_name: str = "Example") -> dict:
        return news.classify_article(
            {
                "source_id": "google_news_fx_macro",
                "source_name": source_name,
                "source_kind": "rss",
                "source_quality": 0.55,
                "source_verified": False,
                "source_currencies": [],
                "title": title,
                "summary": "",
                "url": "https://news.google.com/rss/articles/feed-edge-case",
                "published_utc": "2026-08-19T14:59:00Z",
            },
            first_seen=first_seen,
        )

    hypothetical = classify(
        "Iran weighs strikes on US targets in Europe if Trump escalates war"
    )
    assert hypothetical["currency_scores"] == {}
    assert hypothetical["context_only"] is True
    assert hypothetical["directional_publish_eligible"] is False

    oil_outcome = classify("Kolkata fuel prices stay steady as global oil rises")
    assert oil_outcome["reports_prior_market_move"] is True
    assert oil_outcome["directional_publish_eligible"] is False

    beneficiary_recap = classify(
        "Fuel exporters reap billions from war-induced oil supply disruptions"
    )
    assert beneficiary_recap["non_catalyst_context"] is True
    assert beneficiary_recap["currency_scores"] == {}
    assert beneficiary_recap["context_only"] is True

    equity_watchlist = classify(
        "Greggs stock and other UK consumer shares worth watching as inflation rises"
    )
    assert equity_watchlist["currency_scores"] == {}
    assert equity_watchlist["relevant"] is False
    assert equity_watchlist["exclusion_reason"] == "non_fx_equity_earnings_preview"

    wages = classify("Euro area wage growth eases inflation concerns in 2026")
    assert wages["currency_scores"] == {"EUR": -0.4}
    assert wages["directional_publish_eligible"] is False


def test_official_treasury_long_end_buyback_is_rate_confirmed_research_only():
    article = news.classify_article(
        {
            "source_id": "us_treasury_press",
            "source_name": "U.S. Treasury press releases",
            "source_kind": "html_links",
            "source_role": "primary_policy_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["USD"],
            "title": "Treasury Announces Increased Sizes of Nominal Long-End",
            "summary": (
                "The U.S. Department of the Treasury is increasing, by at least "
                "double, the size of liquidity support buyback operations for "
                "longer-dated nominal coupon securities."
            ),
            "url": "https://home.treasury.gov/news/press-releases/sb0607",
            "published_utc": "2026-08-19T12:31:48Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 12, 31, 48, tzinfo=UTC),
    )
    assert article["category"] == "sovereign_duration_liquidity_policy"
    assert article["currency_scores"] == {}
    assert article["research_currency_scores"] == {"USD": -0.55}
    assert article["research_directional_basis"] == (
        "official_duration_liquidity_policy_requires_rate_and_price_confirmation"
    )
    assert article["context_reason"] == (
        "official_duration_liquidity_policy_research_only"
    )
    assert article["transmission_mechanisms"] == [
        "sovereign_duration_supply",
        "yield_curve_repricing",
    ]
    assert article["forward_signal_timely"] is True
    assert article["directional_publish_eligible"] is False
    assert article["execution_eligible"] is False


def test_contextual_dollar_maps_usd_without_mapping_ambiguous_local_dollar():
    assert news.extract_currencies(
        "Dollar rallies on solid US economic news and soaring crude prices"
    ) == ["USD"]
    assert news.extract_currencies(
        "Havana hotel introduces dollar based pool membership"
    ) == []
    assert news.extract_currencies("Fed cuts rates after inflation cools") == ["USD"]
    assert news.extract_currencies("US Treasury sells 30 year bonds") == ["USD"]
    assert news.extract_currencies("Livestock were fed before sunrise") == []


def test_country_and_demonym_aliases_cover_the_full_currency_universe():
    cases = {
        "AUD": "Australian inflation slows",
        "CAD": "Canadian tariffs are delayed",
        "CHF": "Swiss inflation eases",
        "CNH": "Chinese growth surprises",
        "CZK": "Czech inflation rises",
        "DKK": "Danish inflation holds steady",
        "EUR": "Euro area wages moderate",
        "GBP": "United Kingdom inflation rises",
        "HKD": "Hong Kong growth accelerates",
        "HUF": "Hungarian inflation cools",
        "JPY": "Japanese wages rise",
        "MXN": "Mexican inflation eases",
        "NOK": "Norwegian growth slows",
        "NZD": "New Zealand unemployment rises",
        "PLN": "Polish inflation falls",
        "SEK": "Swedish inflation slows",
        "SGD": "Singapore growth strengthens",
        "THB": "Thai exports rise",
        "TRY": "Türkiye inflation cools",
        "USD": "Federal Reserve cuts rates",
        "ZAR": "South African inflation cools",
    }
    assert set(cases) == set(news.ALL_CURRENCIES)
    for currency, headline in cases.items():
        assert currency in news.extract_currencies(headline)


def test_pair_scores_use_first_seen_and_have_zero_execution_weight():
    article = {
        "event_id": "one",
        "headline": "Fed raises rates",
        "published_utc": "2026-07-27T20:00:00+00:00",
        "first_seen_utc": "2026-07-27T20:05:00+00:00",
        "source_quality": 1.0,
        "directional_confidence": 0.9,
        "currency_scores": {"USD": 1.0},
        "post_window_minutes": 360,
        "category": "monetary_policy",
        "source_name": "Federal Reserve",
        "source_url": "https://example.com/fed",
        "source_verified": True,
    }
    before = news.build_pair_scores(
        [article],
        ["EUR_USD"],
        as_of=dt.datetime(2026, 7, 27, 20, 4, tzinfo=UTC),
    )
    assert before["pairs"]["EUR_USD"]["direction"] == "NEUTRAL"
    after = news.build_pair_scores(
        [article],
        ["EUR_USD"],
        as_of=dt.datetime(2026, 7, 27, 20, 6, tzinfo=UTC),
    )
    assert after["pairs"]["EUR_USD"]["direction"] == "SHORT"
    assert after["pairs"]["EUR_USD"]["matrix_weight"] == 0.0
    assert after["pairs"]["EUR_USD"]["execution_eligible"] is False


def test_pair_scores_stop_forward_direction_at_causal_reaction_horizon():
    article = {
        "event_id": "late-usd-policy",
        "topic_id": "late-usd-policy-topic",
        "topic_clustered": True,
        "headline": "Fed signals higher rates",
        # The publisher timestamp was corrected after the collector first knew
        # the item; it must not reset the causal clock.
        "published_utc": "2026-07-27T12:30:00+00:00",
        "first_seen_utc": "2026-07-27T12:00:00+00:00",
        "source_quality": 1.0,
        "directional_confidence": 0.9,
        "currency_scores": {"USD": 1.0},
        "direct_currencies": ["USD"],
        "post_window_minutes": 360,
        "estimated_reaction_horizon_minutes": 60,
        "forward_signal_timely": True,
        "reports_prior_market_move": False,
        "category": "monetary_policy",
        "source_name": "Federal Reserve",
        "source_verified": True,
    }
    output = news.build_pair_scores(
        [article],
        ["EUR_USD"],
        as_of=dt.datetime(2026, 7, 27, 13, 1, tzinfo=UTC),
    )
    pair = output["pairs"]["EUR_USD"]
    assert pair["direction"] == "NEUTRAL"
    assert pair["evidence_quality"] == "NO_CURRENT_EVIDENCE"
    assert pair["directional_event_count"] == 0
    assert pair["context_directional_event_count"] == 1
    assert pair["events"][0]["pair_score"] == 0.0
    assert pair["events"][0]["research_pair_score"] == -1.0
    assert pair["events"][0]["reaction_phase"] == "continuation_context"


def test_pair_scores_preserve_context_only_research_direction_separately():
    article = {
        "event_id": "oil-context",
        "topic_id": "oil-context-topic",
        "topic_clustered": True,
        "headline": "Oil drops on diplomatic hopes",
        "published_utc": "2026-07-27T12:00:00+00:00",
        "first_seen_utc": "2026-07-27T12:01:00+00:00",
        "source_quality": 0.55,
        "directional_confidence": 0.4,
        "currency_scores": {},
        "research_currency_scores": {"CAD": -0.5, "JPY": 0.2},
        "direct_currencies": [],
        "inferred_currencies": ["CAD", "JPY"],
        "post_window_minutes": 360,
        "estimated_reaction_horizon_minutes": 180,
        "forward_signal_timely": False,
        "reports_prior_market_move": False,
        "category": "commodity_shock",
        "scope": "all_pairs",
        "source_name": "Secondary Publisher",
        "source_verified": False,
    }
    output = news.build_pair_scores(
        [article],
        ["CAD_JPY"],
        as_of=dt.datetime(2026, 7, 27, 12, 2, tzinfo=UTC),
    )
    pair = output["pairs"]["CAD_JPY"]
    assert pair["direction"] == "NEUTRAL"
    assert pair["directional_event_count"] == 0
    assert pair["context_directional_event_count"] == 1
    assert pair["events"][0]["pair_score"] == 0.0
    assert pair["events"][0]["research_pair_score"] == -0.7


def test_pair_scores_cluster_exact_syndication_across_publication_hours():
    template = {
        "headline": "Trump warns Iran after missile attack",
        "first_seen_utc": "2026-07-29T12:05:00+00:00",
        "source_quality": 0.55,
        "directional_confidence": 0.8,
        "currency_scores": {"USD": 0.55, "AUD": -0.65},
        "post_window_minutes": 720,
        "category": "risk_off_geopolitical_or_financial",
        "source_url": "https://example.com/story",
        "source_verified": False,
    }
    output = news.build_pair_scores(
        [
            {
                **template,
                "event_id": "one",
                "published_utc": "2026-07-29T11:00:00+00:00",
                "source_name": "Publisher One",
            },
            {
                **template,
                "event_id": "two",
                "published_utc": "2026-07-29T12:00:00+00:00",
                "source_name": "Publisher Two",
            },
        ],
        ["AUD_USD"],
        as_of=dt.datetime(2026, 7, 29, 12, 10, tzinfo=UTC),
    )
    assert output["active_article_count"] == 1
    assert output["pairs"]["AUD_USD"]["active_event_count"] == 0
    assert output["pairs"]["AUD_USD"]["direction"] == "NEUTRAL"


def test_cluster_counts_distinct_sources_not_repeat_observations():
    template = {
        "headline": "Oil prices surge after supply disruption",
        "published_utc": "2026-07-29T12:00:00+00:00",
        "first_seen_utc": "2026-07-29T12:01:00+00:00",
        "source_quality": 0.55,
        "directional_confidence": 0.8,
        "currency_scores": {"CAD": 0.5},
        "topic_signature": "commodity_shock|CAD|oil|oil_up",
        "duplicate_observation_count": 8,
        "source_verified": False,
    }
    clustered = news.cluster_articles(
        [
            {
                **template,
                "event_id": "one",
                "source_name": "Publisher One",
            },
            {
                **template,
                "event_id": "two",
                "source_name": "Publisher Two",
                "headline": "Oil prices surge sharply after supply disruption",
                "duplicate_observation_count": 3,
            },
        ],
        as_of=dt.datetime(2026, 7, 29, 12, 2, tzinfo=UTC),
    )
    assert len(clustered) == 1
    assert clustered[0]["distinct_source_count"] == 2
    assert clustered[0]["corroboration_count"] == 1
    assert clustered[0]["duplicate_observation_count"] == 11
    assert clustered[0]["syndicated_article_count"] == 2


def test_repeated_structured_calendar_headlines_keep_distinct_scheduled_events():
    template = {
        "source_id": "ecb_policy_calendar",
        "source_name": "ECB",
        "source_url": "https://www.ecb.europa.eu/press/calendars/mgcgc/html/index.en.html",
        "headline": "ECB Monetary Policy Decision",
        "published_utc": "2026-08-16T05:40:00+00:00",
        "first_seen_utc": "2026-08-16T05:40:00+00:00",
        "last_seen_utc": "2026-08-16T05:40:00+00:00",
        "structured_event": True,
        "source_verified": True,
        "relevant": True,
        "category": "monetary_policy",
        "currencies": ["EUR"],
        "direct_currencies": ["EUR"],
    }
    clustered = news.cluster_articles(
        [
            {
                **template,
                "event_id": "ecb-september",
                "scheduled_utc": "2026-09-10T12:15:00+00:00",
            },
            {
                **template,
                "event_id": "ecb-october",
                "scheduled_utc": "2026-10-29T13:15:00+00:00",
            },
        ],
        as_of=dt.datetime(2026, 8, 16, 6, 0, tzinfo=UTC),
    )
    assert len(clustered) == 2
    assert {row["scheduled_utc"] for row in clustered} == {
        "2026-09-10T12:15:00+00:00",
        "2026-10-29T13:15:00+00:00",
    }
    assert len({row["topic_id"] for row in clustered}) == 2


def test_exact_syndicated_headline_is_not_independent_corroboration():
    template = {
        "headline": "Stocks mixed as oil prices rise with eyes on Mideast",
        "published_utc": "2026-08-04T03:00:00+00:00",
        "first_seen_utc": "2026-08-04T03:01:00+00:00",
        "source_quality": 0.55,
        "directional_confidence": 0.435,
        "currency_scores": {"CAD": 0.5, "JPY": -0.2},
        "category": "commodity_shock",
        "topic_signature": "commodity_shock|global|oil|oil_up",
        "forward_signal_timely": True,
        "source_verified": False,
    }
    clustered = news.cluster_articles(
        [
            {
                **template,
                "event_id": "rtl-copy",
                "source_name": "RTL Today",
            },
            {
                **template,
                "event_id": "france24-copy",
                "source_name": "France 24",
            },
        ]
    )

    assert len(clustered) == 1
    assert clustered[0]["publisher_source_count"] == 2
    assert clustered[0]["distinct_source_count"] == 1
    assert clustered[0]["corroboration_count"] == 0
    assert clustered[0]["directional_publish_eligible"] is False
    assert clustered[0]["currency_scores"] == {}
    assert clustered[0]["research_currency_scores"] == {
        "CAD": 0.5,
        "JPY": -0.2,
    }


def test_fresh_rewrite_does_not_reset_old_topics_reaction_clock():
    template = {
        "source_quality": 0.55,
        "directional_confidence": 0.6,
        "currency_scores": {"AUD": -0.5, "JPY": 0.4},
        "category": "risk_off_geopolitical_or_financial",
        "topic_signature": "risk_off_geopolitical_or_financial|global|ship_strike",
        "forward_timeliness_limit_minutes": 30.0,
        "source_verified": False,
        "relevant": True,
    }
    clustered = news.cluster_articles(
        [
            {
                **template,
                "event_id": "late-original",
                "source_name": "Publisher One",
                "headline": "Ship struck in Hormuz as US Iran talks remain uncertain",
                "published_utc": "2026-08-04T12:00:00+00:00",
                "first_seen_utc": "2026-08-04T12:45:00+00:00",
                "causal_known_utc": "2026-08-04T12:45:00+00:00",
                "availability_lag_minutes": 45.0,
                "forward_signal_timely": False,
            },
            {
                **template,
                "event_id": "fresh-rewrite",
                "source_name": "Publisher Two",
                "headline": "US Iran talks remain uncertain after ship struck in Hormuz",
                "published_utc": "2026-08-04T13:00:00+00:00",
                "first_seen_utc": "2026-08-04T13:01:00+00:00",
                "causal_known_utc": "2026-08-04T13:01:00+00:00",
                "availability_lag_minutes": 1.0,
                "forward_signal_timely": True,
            },
        ],
        as_of=dt.datetime(2026, 8, 4, 13, 2, tzinfo=UTC),
    )
    assert len(clustered) == 1
    topic = clustered[0]
    assert topic["distinct_source_count"] == 2
    assert topic["availability_lag_minutes"] == 45.0
    assert topic["forward_signal_timely"] is False
    assert topic["directional_publish_eligible"] is False
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"] == {"AUD": -0.5, "JPY": 0.4}
    assert topic["context_reason"] == "source_late_for_reaction_horizon"


def test_separate_same_day_claims_do_not_share_topic_id():
    template = {
        "published_utc": "2026-08-04T12:00:00+00:00",
        "first_seen_utc": "2026-08-04T12:01:00+00:00",
        "source_quality": 0.55,
        "directional_confidence": 0.6,
        "currency_scores": {"AUD": -0.5, "JPY": 0.4},
        "category": "risk_off_geopolitical_or_financial",
        "topic_signature": "risk_off_geopolitical_or_financial|global|iran",
        "forward_signal_timely": True,
        "forward_timeliness_limit_minutes": 30.0,
        "source_verified": False,
    }
    topics = news.cluster_articles(
        [
            {
                **template,
                "event_id": "ship-claim",
                "source_name": "Publisher One",
                "headline": "Ship struck in Hormuz while talks remain uncertain",
            },
            {
                **template,
                "event_id": "blockade-claim",
                "source_name": "Publisher Two",
                "headline": "Blockade will continue until a deal is reached",
            },
        ],
        as_of=dt.datetime(2026, 8, 4, 12, 2, tzinfo=UTC),
    )
    assert len(topics) == 2
    assert len({topic["topic_id"] for topic in topics}) == 2


def test_database_deduplicates_and_preserves_first_seen(tmp_path):
    database = tmp_path / "news.sqlite"
    connection = news.open_database(database)
    try:
        first_seen = dt.datetime(2026, 7, 27, 20, 1, tzinfo=UTC)
        article = news.classify_article(
            {
                "source_id": "fed",
                "source_name": "Fed",
                "source_kind": "rss",
                "source_quality": 1.0,
                "source_verified": True,
                "source_currencies": ["USD"],
                "title": "Fed rate hike",
                "summary": "Monetary policy tightening",
                "url": "https://example.com/one",
                "published_utc": "2026-07-27T20:00:00Z",
            },
            first_seen=first_seen,
        )
        inserted, duplicates = news.upsert_articles(connection, [article], first_seen)
        assert (inserted, duplicates) == (1, 0)
        inserted, duplicates = news.upsert_articles(
            connection,
            [article],
            first_seen + dt.timedelta(minutes=5),
        )
        assert (inserted, duplicates) == (0, 1)
        unchanged = connection.execute(
            "SELECT duplicate_count FROM articles"
        ).fetchone()[0]
        assert unchanged == 0
        poll_time_only = {
            **article,
            "availability_lag_minutes": article["availability_lag_minutes"] + 5.0,
            "causal_known_utc": "2026-07-27T20:06:00+00:00",
        }
        inserted, duplicates = news.upsert_articles(
            connection,
            [poll_time_only],
            first_seen + dt.timedelta(minutes=5, seconds=30),
        )
        assert (inserted, duplicates) == (0, 1)
        unchanged = connection.execute(
            "SELECT duplicate_count FROM articles"
        ).fetchone()[0]
        assert unchanged == 0
        revised = {**article, "summary": "Monetary policy tightening revised"}
        inserted, duplicates = news.upsert_articles(
            connection,
            [revised],
            first_seen + dt.timedelta(minutes=6),
        )
        assert (inserted, duplicates) == (0, 1)
        row = connection.execute(
            """
            SELECT first_seen_utc, duplicate_count, monetary_impulse,
                   currency_scores_json
            FROM articles
            """
        ).fetchone()
        assert row == (
            "2026-07-27T20:01:00+00:00",
            1,
            1.0,
            '{"USD": 1.0}',
        )
        loaded = news.load_relevant_articles(
            connection,
            since=dt.datetime(2026, 7, 27, 0, 0, tzinfo=UTC),
        )
        assert loaded[0]["duplicate_observation_count"] == 1
        assert loaded[0]["corroboration_count"] == 0

        timestamp_revised = news.classify_article(
            {
                "source_id": "fed",
                "source_name": "Fed",
                "source_kind": "rss",
                "source_quality": 1.0,
                "source_verified": True,
                "source_currencies": ["USD"],
                "title": "Fed rate hike",
                "summary": "Monetary policy tightening revised",
                "url": "https://example.com/one",
                "published_utc": "2026-07-27T21:00:00Z",
            },
            first_seen=first_seen + dt.timedelta(hours=1),
        )
        assert timestamp_revised["event_id"] != article["event_id"]
        inserted, duplicates = news.upsert_articles(
            connection,
            [timestamp_revised],
            first_seen + dt.timedelta(hours=1),
        )
        assert (inserted, duplicates) == (0, 1)
        assert connection.execute("SELECT COUNT(*) FROM articles").fetchone()[0] == 1
        stored = json.loads(
            connection.execute("SELECT payload_json FROM articles").fetchone()[0]
        )
        assert stored["event_id"] == article["event_id"]
        stored_published, stored_payload_json = connection.execute(
            "SELECT published_utc, payload_json FROM articles"
        ).fetchone()
        stored_payload = json.loads(stored_payload_json)
        assert stored_published == "2026-07-27T20:00:00+00:00"
        assert stored_payload["published_utc"] == stored_published
    finally:
        connection.close()


def test_structured_numeric_duplicate_preserves_first_causal_clock(tmp_path):
    database = tmp_path / "news.sqlite"
    connection = news.open_database(database)
    try:
        raw = {
            "source_id": "eurostat_economy_finance",
            "source_name": "Eurostat",
            "source_kind": "rss",
            "source_role": "primary_statistical_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["EUR"],
            "numeric_parser_activated_utc": "2026-08-16T06:35:00Z",
            "title": "GDP up by 0.4% in the euro area and by 0.5% in the EU",
            "summary": (
                "In the second quarter of 2026, seasonally adjusted GDP increased "
                "by 0.4% in the euro area and by 0.5% in the EU, compared with the "
                "previous quarter."
            ),
            "published_utc": "2026-08-16T06:30:00Z",
            "url": "https://ec.europa.eu/eurostat/product?code=proof",
        }
        first_seen = dt.datetime(2026, 8, 16, 6, 35, tzinfo=UTC)
        repeated_seen = first_seen + dt.timedelta(minutes=10)
        first = news.classify_article(raw, first_seen=first_seen)
        repeated = news.classify_article(raw, first_seen=repeated_seen)
        first["collector_contract_id"] = "collector-v1"
        repeated["collector_contract_id"] = "collector-v2"
        first["causal_known_utc"] = news.iso_utc(first_seen)
        repeated["causal_known_utc"] = news.iso_utc(repeated_seen)
        assert first["event_id"] == repeated["event_id"]
        assert first["numeric_causal_known_utc"] == news.iso_utc(first_seen)
        assert repeated["numeric_causal_known_utc"] == news.iso_utc(repeated_seen)
        assert news.upsert_articles(connection, [first], first_seen) == (1, 0)
        assert news.upsert_articles(connection, [repeated], repeated_seen) == (0, 1)
        payload = json.loads(
            connection.execute("SELECT payload_json FROM articles").fetchone()[0]
        )
        assert payload["first_seen_utc"] == news.iso_utc(first_seen)
        assert payload["numeric_causal_known_utc"] == news.iso_utc(first_seen)
    finally:
        connection.close()


def test_reclassification_repairs_payload_clock_from_authoritative_column(tmp_path):
    database = tmp_path / "news.sqlite"
    connection = news.open_database(database)
    try:
        first_seen = dt.datetime(2026, 8, 13, 3, 17, tzinfo=UTC)
        article = news.classify_article(
            {
                "source_id": "rbnz_official_search",
                "source_name": "RBNZ official search",
                "source_kind": "rss",
                "source_quality": 0.8,
                "source_verified": False,
                "source_currencies": ["NZD"],
                "title": "Survey of Expectations – August 2026",
                "summary": "Two-year inflation expectations fall to 2.34%.",
                "url": "https://example.com/rbnz-survey",
                "published_utc": "2026-08-13T03:05:04Z",
            },
            first_seen=first_seen,
        )
        assert news.upsert_articles(connection, [article], first_seen) == (1, 0)
        payload = json.loads(
            connection.execute("SELECT payload_json FROM articles").fetchone()[0]
        )
        payload["classification_version"] = "legacy"
        payload["published_utc"] = "2026-08-13T03:25:34+00:00"
        payload["numeric_causal_known_utc"] = "2026-08-13T03:17:00+00:00"
        payload["numeric_direction_policy"] = "abstain_and_learn_response"
        payload["source_native_components"] = {
            "headline_cpi_yoy": {"actual": 2.5, "previous": 2.7}
        }
        payload["source_native_update_date"] = "2026-08-13"
        payload["consensus_capture_state"] = "not_captured_pre_release"
        payload["vendor_currencies"] = ["NZD", "USD"]
        connection.execute(
            "UPDATE articles SET payload_json = ?",
            (json.dumps(payload, sort_keys=True),),
        )
        connection.commit()

        progress_events = []
        changed = news.reclassify_stored_articles(
            connection,
            sources={
                "rbnz_official_search": {
                    "currencies": ["NZD"],
                    "direct": False,
                    "retrieval_via": "google_news_official_site_search",
                    "source_contract_id": "rbnz_search_contract_v2",
                    "source_cohort_id": "rbnz_search_cohort_v2",
                }
            },
            since=dt.datetime(2026, 8, 13, 0, 0, tzinfo=UTC),
            progress_callback=lambda phase, details: progress_events.append(
                (phase, dict(details))
            ),
        )
        assert changed == 1
        assert progress_events[0] == (
            "postprocessing_evidence",
            {
                "postprocess_step": "reclassifying_retained_articles",
                "candidate_rows": 1,
                "processed_rows": 0,
                "reclassified_rows": 0,
            },
        )
        assert progress_events[-1] == (
            "postprocessing_evidence",
            {
                "postprocess_step": "reclassification_complete",
                "candidate_rows": 1,
                "processed_rows": 1,
                "reclassified_rows": 1,
            },
        )
        stored_published, repaired_json = connection.execute(
            "SELECT published_utc, payload_json FROM articles"
        ).fetchone()
        repaired = json.loads(repaired_json)
        assert stored_published == "2026-08-13T03:05:04+00:00"
        assert repaired["published_utc"] == stored_published
        assert repaired["classification_version"] == news.CLASSIFICATION_VERSION
        assert repaired["numeric_causal_known_utc"] == "2026-08-13T03:17:00+00:00"
        assert repaired["numeric_direction_policy"] == "abstain_and_learn_response"
        assert repaired["source_native_components"] == {
            "headline_cpi_yoy": {"actual": 2.5, "previous": 2.7}
        }
        assert repaired["source_native_update_date"] == "2026-08-13"
        assert repaired["consensus_capture_state"] == "not_captured_pre_release"
        assert repaired["vendor_currencies"] == ["NZD", "USD"]
        assert repaired["source_direct"] is False
        assert repaired["source_contract_id"] == "rbnz_search_contract_v2"
        assert repaired["source_cohort_id"] == "rbnz_search_cohort_v2"
    finally:
        connection.close()


def test_html_listing_poll_time_is_not_a_material_revision(tmp_path):
    database = tmp_path / "news.sqlite"
    connection = news.open_database(database)
    try:
        first_seen = dt.datetime(2026, 8, 3, 20, 0, tzinfo=UTC)
        raw = {
            "source_id": "treasury",
            "source_name": "Treasury",
            "source_kind": "html_links",
            "source_quality": 1.0,
            "source_verified": True,
            "source_currencies": ["USD"],
            "title": "Treasury borrowing estimate",
            "summary": "",
            "url": "https://example.gov/release/one",
            "published_utc": "",
        }
        first = news.classify_article(raw, first_seen=first_seen)
        second = news.classify_article(
            raw,
            first_seen=first_seen + dt.timedelta(hours=1),
        )
        assert first["published_time_inferred"] is True
        assert second["event_id"] != first["event_id"]
        assert news.upsert_articles(connection, [first], first_seen) == (1, 0)
        assert news.upsert_articles(
            connection,
            [second],
            first_seen + dt.timedelta(hours=1),
        ) == (0, 1)
        row = connection.execute(
            "SELECT COUNT(*), duplicate_count, first_seen_utc FROM articles"
        ).fetchone()
        assert row == (1, 0, "2026-08-03T20:00:00+00:00")
    finally:
        connection.close()


def test_database_never_downgrades_observed_official_release_details(tmp_path):
    connection = news.open_database(tmp_path / "news.sqlite")
    try:
        first_seen = dt.datetime(2026, 8, 4, 12, 31, tzinfo=UTC)
        raw = {
            "source_id": "bea_releases",
            "source_name": "BEA",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_statistical_release",
            "source_currencies": ["USD"],
            "title": "U.S. International Trade in Goods and Services, June 2026",
            "url": "https://www.bea.gov/news/2026/trade-june-2026",
            "published_utc": "2026-08-04T12:30:00+00:00",
        }
        enriched = news.classify_article(
            {
                **raw,
                "summary": "The goods and services deficit was $73.3 billion.",
                "detail_enriched": True,
                "detail_enrichment_kind": "bea_release_blurb",
                "detail_available_utc": "2026-08-04T12:35:00+00:00",
            },
            first_seen=first_seen,
        )
        plain = news.classify_article(
            {**raw, "summary": "Full Text ]]>"},
            first_seen=first_seen + dt.timedelta(minutes=31),
        )
        assert enriched["event_id"] == plain["event_id"]
        assert news.upsert_articles(connection, [enriched], first_seen) == (1, 0)
        assert news.upsert_articles(
            connection,
            [plain],
            first_seen + dt.timedelta(minutes=31),
        ) == (0, 1)
        row = connection.execute(
            "SELECT summary, payload_json FROM articles"
        ).fetchone()
        payload = json.loads(row[1])
        assert row[0] == "The goods and services deficit was $73.3 billion."
        assert payload["detail_enriched"] is True
        assert payload["detail_available_utc"] == "2026-08-04T12:35:00+00:00"
        loaded = news.load_context_articles(
            connection,
            since=dt.datetime(2026, 8, 4, tzinfo=UTC),
        )
        assert loaded[0]["causal_known_utc"] == "2026-08-04T12:35:00+00:00"
    finally:
        connection.close()


def test_html_listing_history_seeds_back_catalog_independent_of_poll_state():
    first = [
        {"url": "https://example.gov/release/old", "title": "Old release"},
    ]
    known = news.annotate_html_listing_history(
        first,
        {"last_success_utc": "2026-08-03T20:00:00Z"},
        listing_bootstrap=False,
    )
    assert first[0]["source_listing_bootstrap"] is True
    assert known == ["https://example.gov/release/old"]

    empty_history = [
        {"url": "https://example.gov/release/old", "title": "Old release"},
    ]
    news.annotate_html_listing_history(
        empty_history,
        {"known_item_urls": []},
        listing_bootstrap=False,
    )
    assert empty_history[0]["source_listing_bootstrap"] is True

    second = [
        {"url": "https://example.gov/release/old", "title": "Old release"},
        {"url": "https://example.gov/release/new", "title": "New release"},
    ]
    updated = news.annotate_html_listing_history(
        second,
        {"known_item_urls": known},
        listing_bootstrap=False,
    )
    # A state written before bootstrap_item_urls existed migrates fail closed:
    # already-known URLs remain historical, while a genuinely unseen link is
    # still recognized as new.
    assert second[0]["source_listing_bootstrap"] is True
    assert second[0]["source_listing_new_item"] is False
    assert not second[1].get("source_listing_bootstrap")
    assert second[1]["source_listing_new_item"] is True
    assert updated == [
        "https://example.gov/release/old",
        "https://example.gov/release/new",
    ]

    bootstrap_urls = news.retained_html_listing_bootstrap_urls(
        second,
        {"known_item_urls": known},
    )
    assert bootstrap_urls == ["https://example.gov/release/old"]

    third = [
        {"url": "https://example.gov/release/old", "title": "Old release"},
        {"url": "https://example.gov/release/new", "title": "New release"},
        {"url": "https://example.gov/release/future", "title": "Future release"},
    ]
    news.annotate_html_listing_history(
        third,
        {
            "known_item_urls": updated,
            "bootstrap_item_urls": bootstrap_urls,
        },
        listing_bootstrap=False,
    )
    assert third[0]["source_listing_bootstrap"] is True
    assert third[0]["source_listing_new_item"] is False
    assert not third[1].get("source_listing_bootstrap")
    assert third[1]["source_listing_new_item"] is False
    assert not third[2].get("source_listing_bootstrap")
    assert third[2]["source_listing_new_item"] is True
    assert news.retained_html_listing_bootstrap_urls(
        third,
        {
            "known_item_urls": updated,
            "bootstrap_item_urls": bootstrap_urls,
        },
    ) == ["https://example.gov/release/old"]


def test_fetch_html_listing_persists_bootstrap_quarantine_across_polls(
    monkeypatch,
):
    payloads = iter(
        [
            b'<a href="/release/old">Old release</a>',
            (
                b'<a href="/release/old">Old release</a>'
                b'<a href="/release/new">New release</a>'
            ),
            (
                b'<a href="/release/old">Old release</a>'
                b'<a href="/release/new">New release</a>'
            ),
        ]
    )

    class Response:
        status = 200
        headers = {}

        def __init__(self, payload):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, _maximum):
            return self.payload

    monkeypatch.setattr(
        news.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: Response(next(payloads)),
    )
    source = {
        "source_id": "example_html",
        "name": "Example official releases",
        "kind": "html_links",
        "url": "https://example.gov/releases/",
        "link_patterns": [r"/release/"],
        "trusted_domains": ["example.gov"],
        "currencies": ["USD"],
        "verified": True,
        "direct": True,
        "source_role": "primary_policy_release",
        "source_contract_id": "example_html_v1",
        "source_cohort_id": "example_html_v1",
        "conditional_get": False,
    }
    state = {}
    first, state = news.fetch_source(
        source,
        state,
        timeout_sec=1,
        maximum_bytes=10_000,
        now=dt.datetime(2026, 8, 27, 12, 0, tzinfo=UTC),
    )
    assert first[0]["source_listing_bootstrap"] is True
    assert state["bootstrap_item_urls"] == [
        "https://example.gov/release/old"
    ]

    second, state = news.fetch_source(
        source,
        state,
        timeout_sec=1,
        maximum_bytes=10_000,
        now=dt.datetime(2026, 8, 27, 12, 3, tzinfo=UTC),
    )
    second_by_url = {row["url"]: row for row in second}
    assert second_by_url["https://example.gov/release/old"][
        "source_listing_bootstrap"
    ] is True
    assert second_by_url["https://example.gov/release/new"][
        "source_listing_new_item"
    ] is True
    assert not second_by_url["https://example.gov/release/new"].get(
        "source_listing_bootstrap"
    )
    assert state["bootstrap_item_urls"] == [
        "https://example.gov/release/old"
    ]

    third, state = news.fetch_source(
        source,
        state,
        timeout_sec=1,
        maximum_bytes=10_000,
        now=dt.datetime(2026, 8, 27, 12, 6, tzinfo=UTC),
    )
    third_by_url = {row["url"]: row for row in third}
    assert third_by_url["https://example.gov/release/old"][
        "source_listing_bootstrap"
    ] is True
    assert third_by_url["https://example.gov/release/new"][
        "source_listing_new_item"
    ] is False
    assert state["bootstrap_item_urls"] == [
        "https://example.gov/release/old"
    ]


def test_html_listing_reads_first_party_embedded_page_view_data():
    page_data = {
        "PaginatedBlockPages": [
            {
                "Title": "Labour market statistics: June 2026 quarter",
                "PageLink": (
                    "/information-releases/"
                    "labour-market-statistics-june-2026-quarter/"
                ),
            },
            {
                "Title": "Outside link",
                "PageLink": "https://example.com/not-trusted",
            },
        ]
    }
    encoded = (
        json.dumps(page_data)
        .replace("&", "&amp;")
        .replace('"', "&quot;")
    )
    payload = (
        '<html><div id="pageViewData" data-value="'
        + encoded
        + '"></div></html>'
    ).encode()

    rows = news.parse_html_links(
        payload,
        {
            "source_id": "stats_nz_releases",
            "name": "Stats NZ information releases",
            "kind": "html_links",
            "url": "https://www.stats.govt.nz/information-releases/",
            "link_patterns": ["/information-releases/"],
            "trusted_domains": ["stats.govt.nz"],
            "currencies": ["NZD"],
            "verified": True,
            "direct": True,
            "source_role": "primary_statistical_release",
            "quality": 0.98,
        },
    )

    assert len(rows) == 1
    assert rows[0]["title"] == (
        "Labour market statistics: June 2026 quarter"
    )
    assert rows[0]["url"] == (
        "https://www.stats.govt.nz/information-releases/"
        "labour-market-statistics-june-2026-quarter"
    )
    assert rows[0]["source_currencies"] == ["NZD"]


def test_stats_nz_release_source_fetches_fresh_official_detail_research_only():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row.get("source_id") == "stats_nz_releases"
    )
    assert source["detail_enrichment"] == "stats_nz_release_detail"
    assert source["detail_max_age_minutes"] == 60
    assert source["directional_research_only"] is True
    assert source["source_contract_id"] == "stats_nz_releases_numeric_v3_20260819"


def test_fresh_stats_nz_labour_detail_extracts_unemployment_without_direction():
    article = news.classify_article(
        {
            "source_id": "stats_nz_releases",
            "source_name": "Stats NZ",
            "source_kind": "html_links",
            "source_role": "primary_statistical_release",
            "source_quality": 0.98,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["NZD"],
            "numeric_parser_activated_utc": "2026-08-16T07:00:00Z",
            "detail_enriched": True,
            "detail_enrichment_kind": "stats_nz_release_detail",
            "detail_available_utc": "2026-08-17T22:46:10Z",
            "title": "Labour market statistics: June 2026 quarter",
            "summary": (
                "The unemployment rate was 5.6 percent in the June 2026 quarter, "
                "compared with 5.4 percent in the March 2026 quarter. Employment "
                "was unchanged."
            ),
            "published_utc": "2026-08-17T22:45:00Z",
            "url": "https://www.stats.govt.nz/information-releases/example",
        },
        first_seen=dt.datetime(2026, 8, 17, 22, 46, 10, tzinfo=UTC),
    )
    assert article["event_series_id"] == "stats_nz_unemployment_rate"
    assert article["actual_value"] == 5.6
    assert article["previous_value"] == 5.4
    assert article["reference_period"] == "June 2026 quarter"
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_fresh_stats_nz_business_prices_extracts_components_without_direction():
    article = news.classify_article(
        {
            "source_id": "stats_nz_releases",
            "source_name": "Stats NZ",
            "source_kind": "html_links",
            "source_role": "primary_statistical_release",
            "source_quality": 0.98,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["NZD"],
            "numeric_parser_activated_utc": "2026-08-19T01:10:00Z",
            "numeric_extraction_contract_id": (
                "stats_nz_allowlisted_labour_and_business_prices_v3_20260819"
            ),
            "detail_enriched": True,
            "detail_enrichment_kind": "stats_nz_release_detail",
            "detail_available_utc": "2026-08-18T22:46:55Z",
            "title": "Business price indexes: June 2026 quarter",
            "summary": (
                "The output producers price index (PPI) rose 1.6 percent in the "
                "June 2026 quarter compared with the March 2026 quarter. The input "
                "PPI rose 2.9 percent. The farm expenses price index (FEPI) rose "
                "3.8 percent and the capital goods price index (CGPI) rose 1.8 percent."
            ),
            "published_utc": "2026-08-18T22:45:00Z",
            "url": "https://www.stats.govt.nz/information-releases/example",
        },
        first_seen=dt.datetime(2026, 8, 18, 22, 47, tzinfo=UTC),
    )
    assert article["event_series_id"] == "stats_nz_output_ppi_qoq"
    assert article["actual_value"] == 1.6
    assert article["reference_period"] == "June 2026 quarter"
    assert article["category"] == "inflation_release"
    assert article["source_native_components"]["input_ppi"]["actual_value"] == 2.9
    assert len(article["release_components"]) == 4
    assert article["numeric_causal_known_utc"] == "2026-08-19T01:10:00+00:00"
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False
    assert article["execution_eligible"] is False


@pytest.mark.parametrize(
    ("title", "series_id", "actual", "period"),
    [
        (
            "Media Release - CPI rose 3.8% in the year to June 2026",
            "abs_australia_cpi_yoy",
            3.8,
            "June 2026",
        ),
        (
            "Media Release - Unemployment rate remains at 4.4% in June",
            "abs_australia_unemployment_rate",
            4.4,
            "June",
        ),
        (
            "Media Release - Australian economy grew 0.3% in the March quarter",
            "abs_australia_real_gdp_qoq",
            0.3,
            "March quarter",
        ),
        (
            "Media Release - Annual wage growth of 3.2% in June quarter 2026",
            "abs_australia_wage_price_index_yoy",
            3.2,
            "June quarter 2026",
        ),
    ],
)
def test_abs_allowlisted_numeric_headlines_abstain_on_direction(
    title, series_id, actual, period
):
    article = news.classify_article(
        {
            "source_id": "abs_latest_releases",
            "source_name": "Australian Bureau of Statistics",
            "source_kind": "html_links",
            "source_role": "primary_statistical_release",
            "source_quality": 0.98,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["AUD"],
            "numeric_parser_activated_utc": "2026-08-16T07:10:00Z",
            "title": title,
            "summary": "",
            "url": "https://www.abs.gov.au/statistics/example",
            "published_utc": "",
            "published_time_inferred": True,
        },
        first_seen=dt.datetime(2026, 8, 16, 7, 10, tzinfo=UTC),
    )
    assert article["event_series_id"] == series_id
    assert article["actual_value"] == actual
    assert article["reference_period"] == period
    assert article["numeric_causal_known_utc"] == "2026-08-16T07:10:00+00:00"
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_abs_wage_detail_retains_quarterly_and_annual_components_without_direction():
    article = news.classify_article(
        {
            "source_id": "abs_latest_releases",
            "source_name": "Australian Bureau of Statistics",
            "source_kind": "html_links",
            "source_role": "primary_statistical_release",
            "source_quality": 0.98,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["AUD"],
            "numeric_parser_activated_utc": "2026-08-19T02:01:00Z",
            "title": (
                "Media Release - Annual wage growth of 3.2% in "
                "June quarter 2026"
            ),
            "summary": (
                "The Wage Price Index (WPI) rose 0.8 per cent in the June "
                "quarter 2026 and 3.2 per cent annually. Annual wage growth "
                "of 3.2 per cent is slightly down from 3.4 per cent at the "
                "same time last year."
            ),
            "detail_enriched": True,
            "detail_enrichment_kind": "official_html_text",
            "detail_available_utc": "2026-08-19T02:40:00Z",
            "url": "https://www.abs.gov.au/media-centre/example",
            "published_utc": "2026-08-19T01:30:00Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 1, 31, tzinfo=UTC),
    )
    components = article["source_native_components"]
    assert components["wage_price_index_qoq"]["actual"] == 0.8
    assert components["wage_price_index_yoy"]["actual"] == 3.2
    assert components["wage_price_index_yoy"]["year_ago"] == 3.4
    assert article["numeric_causal_known_utc"] == "2026-08-19T02:40:00+00:00"
    assert article["numeric_extraction_contract_id"] == (
        "abs_wage_detail_components_v1_20260819"
    )
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_abs_generic_wage_release_title_uses_causally_fetched_body():
    article = news.classify_article(
        {
            "source_id": "abs_latest_releases",
            "source_name": "Australian Bureau of Statistics",
            "source_kind": "html_links",
            "source_role": "primary_statistical_release",
            "source_quality": 0.98,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["AUD"],
            "numeric_parser_activated_utc": "2026-08-19T02:01:00Z",
            "title": "Wage Price Index, Australia",
            "summary": (
                "The Wage Price Index (WPI) rose 0.8 per cent in the June "
                "quarter 2026 and 3.2 per cent annually. Annual wage growth "
                "of 3.2 per cent is slightly down from 3.4 per cent at the "
                "same time last year."
            ),
            "detail_enriched": True,
            "detail_enrichment_kind": "official_html_text",
            "detail_available_utc": "2026-08-19T03:30:00Z",
            "url": "https://www.abs.gov.au/statistics/wage-price-index",
            "published_utc": "2026-08-19T01:30:00Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 1, 31, tzinfo=UTC),
    )
    assert article["event_series_id"] == "abs_australia_wage_price_index_yoy"
    assert article["actual_value"] == 3.2
    assert article["source_native_components"]["wage_price_index_qoq"][
        "actual"
    ] == 0.8
    assert article["numeric_causal_known_utc"] == "2026-08-19T03:30:00+00:00"
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_stats_nz_release_detail_reads_embedded_page_view_json():
    page_view = {
        "MetaDescription": (
            "The unemployment rate was 5.6 percent in the June 2026 quarter, "
            "compared with 5.4 percent in the March 2026 quarter."
        ),
        "FeaturedText": "<p>Official New Zealand labour market statistics.</p>",
        "PageBlocks": [
            {"Content": "<h2>Key facts</h2><p>Employment was unchanged.</p>"},
            {"GraphCsvData": "this graph payload must not become prose"},
        ],
    }
    encoded = html.escape(json.dumps(page_view), quote=True)
    payload = (
        '<html><div id="pageViewData" data-value="'
        + encoded
        + '"></div></html>'
    ).encode()
    detail = news.parse_stats_nz_release_detail(payload)
    assert "unemployment rate was 5.6 percent" in detail
    assert "Official New Zealand labour market statistics" in detail
    assert "Employment was unchanged" in detail
    assert "graph payload" not in detail


def test_html_listing_reads_first_party_custom_element_link_json():
    payload = (
        "<html><dnb-tile "
        "link='{&quot;text&quot;:&quot;Interest rate increase&quot;,"
        "&quot;url&quot;:&quot;https://www.nationalbanken.dk/en/news-and-"
        "knowledge/press/archive/2026/interest-rate-increase&quot;}' "
        "meta-data='{&quot;publishTime&quot;:&quot;2026-08-13&quot;}'>"
        "</dnb-tile></html>"
    ).encode()

    rows = news.parse_html_links(
        payload,
        {
            "source_id": "nationalbanken_press_direct_v1",
            "name": "Danmarks Nationalbank press releases",
            "kind": "html_links",
            "url": "https://www.nationalbanken.dk/en/news-and-knowledge/press",
            "link_patterns": ["/en/news-and-knowledge/press/archive/"],
            "trusted_domains": ["nationalbanken.dk"],
            "currencies": ["DKK"],
            "verified": True,
            "direct": True,
        },
    )

    assert len(rows) == 1
    assert rows[0]["title"] == "Interest rate increase"
    assert rows[0]["url"].endswith("/2026/interest-rate-increase")


def test_stats_nz_calendar_converts_local_release_time_and_stays_neutral():
    payload = json.dumps(
        {
            "items": {
                "upcoming": [
                    {
                        "ID": 8307,
                        "DisplayName": (
                            "Labour market statistics: June 2026 quarter"
                        ),
                        "DateString": "5 August 2026",
                        "PublicationDate": "2026-08-05 10:45:00",
                    }
                ]
            }
        }
    ).encode()
    rows = news.parse_stats_nz_calendar(
        payload,
        {
            "source_id": "stats_nz_calendar",
            "name": "Stats NZ official release calendar",
            "url": (
                "https://www.stats.govt.nz/api/v1/"
                "releaseCalendarMonth/"
            ),
            "publisher_url": (
                "https://www.stats.govt.nz/public/release-calendar/"
            ),
            "source_timezone": "Pacific/Auckland",
            "currencies": ["NZD"],
            "verified": True,
            "direct": True,
            "source_role": "primary_statistical_calendar",
            "quality": 0.99,
        },
    )

    assert len(rows) == 1
    assert rows[0]["scheduled_utc"] == "2026-08-04T22:45:00+00:00"
    assert rows[0]["source_currencies"] == ["NZD"]
    assert rows[0]["structured_event"] is True

    article = news.classify_article(
        rows[0],
        first_seen=dt.datetime(2026, 8, 4, 10, 0, tzinfo=UTC),
    )
    assert article["category"] == "labor_release"
    assert article["currencies"] == ["NZD"]
    assert article["currency_scores"] == {}
    assert article["context_only"] is True
    assert article["scheduled_utc"] == "2026-08-04T22:45:00+00:00"
    ledger = news.ledger_row(article)
    assert ledger["event_utc"] == "2026-08-04T22:45:00+00:00"
    assert ledger["scheduled_utc"] == "2026-08-04T22:45:00+00:00"
    assert ledger["pre_window_minutes"] == 60
    assert ledger["directional_bias"] == "{}"


def test_census_release_calendar_uses_eastern_time_and_stays_neutral():
    now_local = news.utc_now().astimezone(news.ZoneInfo("America/New_York"))
    release_local = (now_local + dt.timedelta(hours=2)).replace(
        second=0, microsecond=0
    )
    schedule_key = release_local.strftime("%Y%m%d%H%M")
    payload = f"""
    <table>
      <tr height="20">
        <td><a href="/foreign-trade/">U.S. International Trade in Goods and Services</a></td>
        <td sorttable_customkey="{schedule_key}">{release_local:%B %d, %Y}</td>
        <td>{release_local:%I:%M %p}</td>
        <td>June 2026</td>
      </tr>
    </table>
    """.encode()
    rows = news.parse_census_release_calendar(
        payload,
        {
            "source_id": "census_release_calendar",
            "name": "US Census Bureau economic release calendar",
            "url": "https://www.census.gov/economic-indicators/calendar-listview.html",
            "source_timezone": "America/New_York",
            "currencies": ["USD"],
            "verified": True,
            "direct": True,
            "source_role": "primary_statistical_calendar",
            "trusted_domains": ["census.gov"],
        },
    )

    assert len(rows) == 1
    expected = release_local.astimezone(news.UTC).isoformat()
    assert rows[0]["scheduled_utc"] == expected
    assert rows[0]["source_currencies"] == ["USD"]
    assert rows[0]["structured_event"] is True
    article = news.classify_article(
        rows[0],
        first_seen=news.utc_now(),
    )
    assert article["category"] == "trade_balance_release"
    assert article["currencies"] == ["USD"]
    assert article["currency_scores"] == {}
    assert article["context_only"] is True
    ledger = news.ledger_row(article)
    assert ledger["event_utc"] == expected
    assert ledger["pre_window_minutes"] == 60
    assert ledger["directional_bias"] == "{}"


def test_denmark_exact_release_calendar_keeps_one_confirmed_news_clock():
    payload = """
    <table>
      <tr><th>Date</th><th>Time</th><th>Type</th><th>Title</th>
          <th>Subject</th><th>Reference period</th><th>Confirmed</th></tr>
      <tr><td>20-08-2026</td><td>08:00</td><td>News</td>
          <td>Consumer price index</td><td>Prices</td><td>July 2026</td><td>Yes</td></tr>
      <tr><td>20-08-2026</td><td>08:00</td><td>News</td>
          <td>Consumer price index</td><td>Prices</td><td>July 2026</td><td>Yes</td></tr>
      <tr><td>20-08-2026</td><td>08:00</td><td>Table</td>
          <td>Consumer price index</td><td>Prices</td><td>July 2026</td><td>Yes</td></tr>
      <tr><td>21-08-2026</td><td>08:00</td><td>News</td>
          <td>Consumer price index</td><td>Prices</td><td>August 2026</td><td>No</td></tr>
    </table>
    """.encode()
    source = {
        "source_id": "denmark_statistics_release_calendar_exact_v1",
        "name": "Statistics Denmark exact official release calendar",
        "kind": "denmark_statistics_release_calendar",
        "publisher_url": "https://www.dst.dk/en/Statistik/planlagte",
        "currencies": ["DKK"],
        "verified": True,
        "direct": True,
        "source_role": "primary_statistical_calendar",
        "event_country": "Denmark",
        "source_timezone": "Europe/Copenhagen",
        "require_confirmed": True,
        "event_mappings": [
            {
                "title_pattern": "^Consumer price index$",
                "event_series_id": "denmark_cpi_release",
                "event_name": "Denmark Consumer Price Index",
            }
        ],
        "source_contract_id": (
            "denmark_statistics_release_calendar_exact_v2_paged_45d_20260817"
        ),
        "source_cohort_id": (
            "denmark_statistics_release_calendar_exact_v2_paged_45d_20260817"
        ),
    }

    rows = news.parse_denmark_statistics_release_calendar(payload, source)

    assert len(rows) == 1
    assert rows[0]["scheduled_utc"] == "2026-08-20T06:00:00+00:00"
    assert rows[0]["timing_precision"] == "minute"
    assert rows[0]["clock_semantics"] == "domestic_official_statistical_release"
    assert rows[0]["independent_domestic_event"] is True
    assert rows[0]["linked_policy_factor"] is False
    assert rows[0]["actual_value"] is None
    assert rows[0]["consensus_value"] is None
    assert rows[0]["directional_research_only"] is True
    assert rows[0]["research_only"] is True
    assert rows[0]["execution_eligible"] is False
    assert rows[0]["can_place_orders"] is False

    first_seen = dt.datetime(2026, 8, 19, 12, 0, tzinfo=UTC)
    article = news.classify_article(rows[0], first_seen=first_seen)
    assert article["causal_known_utc"] == first_seen.isoformat()
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False
    assert article["execution_eligible"] is False
    assert article["clock_semantics"] == "domestic_official_statistical_release"
    ledger = news.ledger_row(article)
    assert ledger["timing_precision"] == "minute"
    assert ledger["event_time_basis"] == "scheduled_release"
    assert ledger["clock_semantics"] == "domestic_official_statistical_release"
    assert ledger["independent_domestic_event"] == 1
    assert ledger["linked_policy_factor"] == 0
    assert ledger["source_contract_id"] == source["source_contract_id"]
    assert ledger["source_cohort_id"] == source["source_cohort_id"]
    assert ledger["directional_research_only"] == 1
    assert ledger["execution_eligible"] == 0
    assert ledger["can_place_orders"] == 0

    normalized = news.event_tagger.normalize_event(
        ledger,
        source_type="live_news_watch",
    )
    assert normalized is not None
    assert normalized["timing_precision"] == "minute"
    assert normalized["clock_semantics"] == (
        "domestic_official_statistical_release"
    )
    assert normalized["independent_domestic_event"] is True
    assert normalized["linked_policy_factor"] is False
    assert normalized["source_contract_id"] == source["source_contract_id"]
    assert normalized["source_cohort_id"] == source["source_cohort_id"]
    assert normalized["directional_research_only"] is True
    assert normalized["execution_eligible"] is False
    assert normalized["can_place_orders"] is False


def test_denmark_calendar_paginates_to_45_days_maps_live_titles_and_deduplicates(
    monkeypatch,
):
    def page(*rows):
        return (
            "<table><tr><th>Date</th><th>Time</th><th>Type</th><th>Title</th>"
            "<th>Subject</th><th>Period</th><th>Confirmed</th></tr>"
            + "".join(
                "<tr>" + "".join(f"<td>{value}</td>" for value in row) + "</tr>"
                for row in rows
            )
            + "</table>"
        ).encode()

    first_payload = page(
        (
            "25‑08‑2026",
            "08:00",
            "News",
            "Index of retail sales",
            "Retail trade index",
            "July 2026",
            "Yes",
        )
    )
    second_payload = page(
        # Repeated row at a page boundary must not multiply an event.
        (
            "25‑08‑2026",
            "08:00",
            "News",
            "Index of retail sales",
            "Retail trade index",
            "July 2026",
            "Yes",
        ),
        (
            "02‑10‑2026",
            "08:00",
            "News",
            "Balance of payments and external trade",
            "Current account of the balance of payments",
            "August 2026",
            "Yes",
        ),
    )

    class Response:
        status = 200

        def __init__(self, payload):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, _maximum):
            return self.payload

    requested = []

    def urlopen(request, **_kwargs):
        requested.append(request.full_url)
        return Response(second_payload)

    monkeypatch.setattr(news.urllib.request, "urlopen", urlopen)
    source = {
        "source_id": "denmark_statistics_release_calendar_exact_v1",
        "name": "Statistics Denmark exact official release calendar",
        "kind": "denmark_statistics_release_calendar",
        "publisher_url": "https://www.dst.dk/en/Statistik/planlagte",
        "pagination_url_template": (
            "https://www.dst.dk/en/Statistik/planlagte?days=e&page={page}"
        ),
        "pagination_start_page": 1,
        "pagination_max_pages": 2,
        "pagination_maximum_bytes_total": 100_000,
        "prospective_horizon_days": 45,
        "currencies": ["DKK"],
        "verified": True,
        "direct": True,
        "source_role": "primary_statistical_calendar",
        "event_country": "Denmark",
        "source_timezone": "Europe/Copenhagen",
        "require_confirmed": True,
        "event_mappings": [
            {
                "title_pattern": "^(?:Retail sales|Index of retail sales)$",
                "event_series_id": "denmark_retail_sales_release",
                "event_name": "Denmark Index of Retail Sales",
            },
            {
                "title_pattern": (
                    "^(?:External trade in goods|Balance of payments and external trade)$"
                ),
                "event_series_id": "denmark_external_trade_release",
                "event_name": "Denmark Balance of Payments and External Trade",
            },
        ],
        "directional_research_only": True,
        "trusted_domains": ["dst.dk", "www.dst.dk"],
    }

    rows, state = news.fetch_denmark_statistics_release_calendar_pages(
        first_payload,
        source,
        headers={"User-Agent": "test"},
        timeout_sec=1,
        maximum_bytes=100_000,
        now=dt.datetime(2026, 8, 17, tzinfo=UTC),
    )

    assert requested == [
        "https://www.dst.dk/en/Statistik/planlagte?days=e&page=2"
    ]
    assert state["pagination_pages_fetched"] == 2
    assert state["pagination_horizon_days"] == 45
    assert state["pagination_horizon_end_date"] == "2026-10-02"
    assert state["pagination_stop_reason"] == "horizon_covered"
    assert len(rows) == 2
    assert {row["event_series_id"] for row in rows} == {
        "denmark_retail_sales_release",
        "denmark_external_trade_release",
    }
    assert len({row["external_id"] for row in rows}) == len(rows)


def test_denmark_calendar_fails_closed_when_page_cap_misses_horizon(monkeypatch):
    first_payload = (
        "<table><tr><td>17-08-2026</td><td>08:00</td><td>News</td>"
        "<td>National accounts</td><td>GDP</td><td>Q2</td><td>Yes</td></tr></table>"
    ).encode()
    second_payload = (
        "<table><tr><td>25-08-2026</td><td>08:00</td><td>News</td>"
        "<td>National accounts</td><td>GDP</td><td>Q2</td><td>Yes</td></tr></table>"
    ).encode()

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, _maximum):
            return second_payload

    monkeypatch.setattr(news.urllib.request, "urlopen", lambda *_a, **_k: Response())
    source = {
        "pagination_url_template": "https://www.dst.dk/en/Statistik/planlagte?page={page}",
        "pagination_max_pages": 2,
        "prospective_horizon_days": 45,
        "trusted_domains": ["dst.dk", "www.dst.dk"],
        "event_mappings": [],
    }

    with pytest.raises(ValueError, match="did not reach configured horizon"):
        news.fetch_denmark_statistics_release_calendar_pages(
            first_payload,
            source,
            headers={},
            timeout_sec=1,
            maximum_bytes=100_000,
            now=dt.datetime(2026, 8, 17, tzinfo=UTC),
        )


def test_hong_kong_exact_release_calendar_xlsx_is_causal_and_neutral():
    workbook_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
      xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
      <sheets>
        <sheet name="Cover" sheetId="1" r:id="rId1"/>
        <sheet name="Release Schedule" sheetId="2" r:id="rId2"/>
      </sheets>
    </workbook>"""
    relationships_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
      <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet7.xml"/>
      <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet3.xml"/>
    </Relationships>"""
    sheet_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
      <sheetData>
        <row r="1">
          <c r="A1"><v>46254</v></c>
          <c r="B1" t="inlineStr"><is><t>Category</t></is></c>
          <c r="C1" t="inlineStr"><is><t>Subject</t></is></c>
          <c r="D1" t="inlineStr"><is><t>Consumer Price Index</t></is></c>
          <c r="E1" t="inlineStr"><is><t>Consumer Price Index (July 2026)</t></is></c>
        </row>
      </sheetData>
    </worksheet>"""
    cover_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
      <sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>Cover</t></is></c></row></sheetData>
    </worksheet>"""
    payload_buffer = io.BytesIO()
    with zipfile.ZipFile(payload_buffer, "w") as workbook:
        workbook.writestr("xl/workbook.xml", workbook_xml)
        workbook.writestr("xl/_rels/workbook.xml.rels", relationships_xml)
        workbook.writestr("xl/worksheets/sheet7.xml", cover_xml)
        workbook.writestr("xl/worksheets/sheet3.xml", sheet_xml)
    source = {
        "source_id": "hong_kong_censtatd_release_calendar_exact_v1",
        "name": "Hong Kong C&SD exact regular-release calendar",
        "kind": "hong_kong_censtatd_release_calendar_xlsx",
        "publisher_url": "https://www.censtatd.gov.hk/en/press_release.html",
        "currencies": ["HKD"],
        "verified": True,
        "direct": True,
        "source_role": "primary_statistical_calendar",
        "event_country": "Hong Kong",
        "source_timezone": "Asia/Hong_Kong",
        "official_release_time_local": "16:30",
        "worksheet_name": "Release Schedule",
        "event_mappings": [
            {
                "series_title": "Consumer Price Index",
                "event_series_id": "hong_kong_cpi_release",
                "event_name": "Hong Kong Consumer Price Index",
            }
        ],
        "source_contract_id": (
            "hong_kong_censtatd_release_calendar_exact_v2_workbook_relationships_20260817"
        ),
        "source_cohort_id": (
            "hong_kong_censtatd_release_calendar_exact_v2_workbook_relationships_20260817"
        ),
    }

    rows = news.parse_hong_kong_censtatd_release_calendar_xlsx(
        payload_buffer.getvalue(), source
    )

    assert len(rows) == 1
    assert rows[0]["scheduled_utc"] == "2026-08-20T08:30:00+00:00"
    assert rows[0]["timing_precision"] == "minute"
    assert rows[0]["independent_domestic_event"] is True
    assert rows[0]["linked_policy_factor"] is False
    assert rows[0]["actual_value"] is None
    assert rows[0]["consensus_value"] is None
    assert rows[0]["directional_research_only"] is True
    assert rows[0]["research_only"] is True
    assert rows[0]["execution_eligible"] is False
    assert rows[0]["can_place_orders"] is False

    # A first ingestion after the release must not backdate local knowledge to
    # the official schedule.  It is diagnostic/provenance only for this row.
    first_seen = dt.datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    article = news.classify_article(rows[0], first_seen=first_seen)
    assert article["scheduled_utc"] == "2026-08-20T08:30:00+00:00"
    assert article["causal_known_utc"] == first_seen.isoformat()
    assert article["first_seen_utc"] == first_seen.isoformat()
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False
    assert article["execution_eligible"] is False


def test_dkk_hkd_exact_clock_source_contracts_are_new_and_official():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    sources = {row["source_id"]: row for row in config["sources"]}
    denmark = sources["denmark_statistics_release_calendar_exact_v1"]
    hong_kong = sources["hong_kong_censtatd_release_calendar_exact_v1"]

    assert denmark["source_contract_id"] == (
        "denmark_statistics_release_calendar_exact_v2_paged_45d_20260817"
    )
    assert denmark["source_cohort_id"] == denmark["source_contract_id"]
    assert denmark["conditional_get"] is False
    assert denmark["prospective_horizon_days"] >= 45
    assert denmark["pagination_max_pages"] >= 2
    patterns = " ".join(
        row["title_pattern"] for row in denmark["event_mappings"]
    )
    assert "Index of retail sales" in patterns
    assert "Balance of payments and external trade" in patterns

    assert hong_kong["source_contract_id"] == (
        "hong_kong_censtatd_release_calendar_exact_v2_workbook_relationships_20260817"
    )
    assert hong_kong["source_cohort_id"] == hong_kong["source_contract_id"]
    assert hong_kong["release_time_rule_url"] == (
        "https://www.censtatd.gov.hk/en/press_release.html"
    )
    assert "page_39" not in json.dumps(hong_kong)


def test_repo_rate_preview_is_monetary_policy_context_not_inflation_release():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Example publisher",
            "source_kind": "rss",
            "source_quality": 0.7,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Why RBI may keep repo rate unchanged, adopt hawkish tone "
                "on inflation"
            ),
            "summary": "",
            "url": "https://example.com/rbi-preview",
            "published_utc": "2026-08-04T10:24:53Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 10, 27, tzinfo=UTC),
    )

    assert article["category"] == "monetary_policy"
    assert article["context_only"] is True
    assert article["currency_scores"] == {}
    assert article["execution_eligible"] is False


def test_rss_can_filter_author_and_treat_date_only_as_inferred():
    payload = b"""<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"><channel>
      <item><title>Swiss CPI falls</title><link>https://www.admin.ch/cpi</link>
        <description>Consumer prices fell 0.1%.</description>
        <pubDate>2026-08-04</pubDate><author>Bundesamt f\xc3\xbcr Statistik</author>
      </item>
      <item><title>Swiss labour briefing</title><link>https://www.admin.ch/jobs</link>
        <description>Briefing invitation.</description>
        <pubDate>2026-08-04</pubDate><author>Staatssekretariat f\xc3\xbcr Wirtschaft</author>
      </item>
    </channel></rss>"""
    rows = news.parse_rss(
        payload,
        {
            "source_id": "swiss_fso_releases",
            "name": "Swiss Federal Statistical Office releases",
            "kind": "rss",
            "author_patterns": [r"^Bundesamt f\u00fcr Statistik$"],
            "date_only_publication_is_inferred": True,
            "currencies": ["CHF"],
            "verified": True,
            "source_role": "primary_statistical_release",
        },
    )
    assert len(rows) == 1
    assert rows[0]["title"] == "Swiss CPI falls"
    assert rows[0]["source_name"] == "Bundesamt f\u00fcr Statistik"
    assert rows[0]["published_utc"] == ""


def test_primary_swiss_german_cpi_release_is_inflation_context_not_direction():
    article = news.classify_article(
        {
            "source_id": "swiss_fso_releases",
            "source_name": "Bundesamt f\u00fcr Statistik",
            "source_kind": "rss",
            "source_role": "primary_statistical_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["CHF"],
            "numeric_parser_activated_utc": "2026-08-16T06:40:00Z",
            "title": "Die Konsumentenpreise sind im Juli um 0,1% gefallen",
            "summary": (
                "Der Landesindex der Konsumentenpreise sank im Juli 2026 "
                "im Vergleich zum Vormonat um 0,1%. Die Teuerung betrug +0,4%."
            ),
            "url": "https://www.admin.ch/de/newnsb/example",
        },
        first_seen=dt.datetime(2026, 8, 4, 6, 13, tzinfo=UTC),
    )
    assert article["category"] == "inflation_release"
    assert article["currencies"] == ["CHF"]
    assert article["currency_scores"] == {}
    assert article["context_only"] is True
    assert article["directional_publish_eligible"] is False
    assert article["actual_value"] == -0.1
    assert article["event_series_id"] == "swiss_fso_cpi_mom"
    assert article["numeric_causal_known_utc"] == "2026-08-16T06:40:00+00:00"


def test_primary_swiss_producer_price_release_extracts_one_allowlisted_change():
    article = news.classify_article(
        {
            "source_id": "swiss_fso_releases",
            "source_name": "Bundesamt für Statistik",
            "source_kind": "rss",
            "source_role": "primary_statistical_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["CHF"],
            "numeric_parser_activated_utc": "2026-08-16T06:40:00Z",
            "title": "Produzenten- und Importpreisindex sinkt im Juli um 0,1%",
            "summary": (
                "Der Gesamtindex der Produzenten- und Importpreise sank im Juli 2026 "
                "gegenüber dem Vormonat um 0,1% und erreichte 99,6 Punkte."
            ),
            "url": "https://www.admin.ch/de/newnsb/example-ppi",
            "published_utc": "2026-08-17T06:30:00Z",
        },
        first_seen=dt.datetime(2026, 8, 17, 6, 30, 20, tzinfo=UTC),
    )
    assert article["actual_value"] == -0.1
    assert article["event_series_id"] == "swiss_fso_producer_import_price_mom"
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_primary_eurostat_gdp_release_extracts_euro_area_qoq_without_direction():
    article = news.classify_article(
        {
            "source_id": "eurostat_economy_finance",
            "source_name": "Eurostat",
            "source_kind": "rss",
            "source_role": "primary_statistical_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["EUR"],
            "numeric_parser_activated_utc": "2026-08-16T06:35:00Z",
            "title": "GDP up by 0.4% in the euro area and by 0.5% in the EU",
            "summary": (
                "In the second quarter of 2026, seasonally adjusted GDP increased "
                "by 0.4% in the euro area and by 0.5% in the EU, compared with the "
                "previous quarter, according to a preliminary flash estimate."
            ),
            "published_utc": "2026-08-14T09:00:00Z",
            "url": "https://ec.europa.eu/eurostat/product?code=2-14082026-ap",
        },
        first_seen=dt.datetime(2026, 8, 14, 9, 0, 20, tzinfo=UTC),
    )
    assert article["event_series_id"] == "eurostat_euro_area_gdp_qoq"
    assert article["actual_value"] == 0.4
    assert article["reference_period"] == "second quarter of 2026"
    assert article["numeric_causal_known_utc"] == "2026-08-16T06:35:00+00:00"
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_fresh_ons_quarterly_gdp_detail_extracts_actual_without_direction():
    article = news.classify_article(
        {
            "source_id": "ons_published_releases",
            "source_name": "Office for National Statistics",
            "source_kind": "rss",
            "source_role": "primary_statistical_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["GBP"],
            "numeric_parser_activated_utc": "2026-08-16T06:50:00Z",
            "detail_enriched": True,
            "detail_enrichment_kind": "ons_release_bulletin",
            "detail_available_utc": "2026-08-17T06:01:46Z",
            "title": "GDP first quarterly estimate, UK: April to June 2026",
            "summary": (
                "UK real gross domestic product increased by 0.4% in Quarter 2 "
                "2026, following growth of 0.6% in Quarter 1."
            ),
            "published_utc": "2026-08-17T06:00:00Z",
            "url": "https://www.ons.gov.uk/example-gdp",
        },
        first_seen=dt.datetime(2026, 8, 17, 6, 1, 46, tzinfo=UTC),
    )
    assert article["event_series_id"] == "ons_uk_real_gdp_qoq"
    assert article["actual_value"] == 0.4
    assert article["reference_period"] == "Quarter 2 2026"
    assert article["numeric_causal_known_utc"] == "2026-08-17T06:01:46+00:00"
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_fresh_ons_cpi_detail_preserves_previous_and_abstains_on_direction():
    article = news.classify_article(
        {
            "source_id": "ons_published_releases",
            "source_name": "Office for National Statistics",
            "source_kind": "rss",
            "source_role": "primary_statistical_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["GBP"],
            "numeric_parser_activated_utc": "2026-08-16T06:50:00Z",
            "detail_enriched": True,
            "detail_enrichment_kind": "ons_release_bulletin",
            "detail_available_utc": "2026-08-19T06:01:10Z",
            "title": "Consumer price inflation, UK: July 2026",
            "summary": (
                "The Consumer Prices Index (CPI) rose by 3.8% in the 12 months "
                "to July 2026, up from 3.6% in the 12 months to June 2026."
            ),
            "published_utc": "2026-08-19T06:00:00Z",
            "url": "https://www.ons.gov.uk/example-cpi",
        },
        first_seen=dt.datetime(2026, 8, 19, 6, 1, 10, tzinfo=UTC),
    )
    assert article["event_series_id"] == "ons_uk_cpi_yoy"
    assert article["actual_value"] == 3.8
    assert article["previous_value"] == 3.6
    assert article["reference_period"] == "July 2026"
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_primary_eurostat_flash_inflation_extracts_level_and_previous_only():
    article = news.classify_article(
        {
            "source_id": "eurostat_economy_finance",
            "source_name": "Eurostat",
            "source_kind": "rss",
            "source_role": "primary_statistical_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["EUR"],
            "numeric_parser_activated_utc": "2026-08-16T06:35:00Z",
            "title": "Euro area annual inflation up to 2.9%",
            "summary": (
                "Euro area annual inflation is expected to be 2.9% in July 2026, "
                "up from 2.8% in June according to a flash estimate from Eurostat."
            ),
            "published_utc": "2026-07-31T09:00:00Z",
            "url": "https://ec.europa.eu/eurostat/product?code=2-31072026-ap",
        },
        first_seen=dt.datetime(2026, 8, 2, 21, 14, tzinfo=UTC),
    )
    assert article["event_series_id"] == "eurostat_euro_area_hicp_flash_yoy"
    assert article["actual_value"] == 2.9
    assert article["previous_value"] == 2.8
    assert article["reference_period"] == "July 2026"
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_administrative_sanctions_removals_are_not_risk_off():
    article = news.classify_article(
        {
            "source_id": "us_treasury_press",
            "source_name": "U.S. Treasury press releases",
            "source_kind": "html_links",
            "source_role": "primary_policy_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_currencies": ["USD"],
            "title": (
                "Treasury Announces Second Round of Sanctions Removals, "
                "Updates in Modernization Initiative"
            ),
            "summary": "",
            "url": "https://home.treasury.gov/news/press-releases/example",
            "published_utc": "2026-07-27T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 0, 0, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0.0
    assert article["risk_on_score"] == 0.0
    assert article["currency_scores"] == {}
    assert article["category"] == "market_news"


def test_official_sanctions_relief_is_not_sanctions_escalation():
    article = news.classify_article(
        {
            "source_id": "us_treasury_press",
            "source_name": "U.S. Treasury press releases",
            "source_kind": "html_links",
            "source_role": "primary_policy_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["USD"],
            "title": (
                "Treasury and State Departments Deliver Additional "
                "Sanctions Relief on Syria"
            ),
            "summary": "The United States issued additional sanctions relief.",
            "url": "https://home.treasury.gov/news/press-releases/example",
            "published_utc": "2026-08-24T17:42:18Z",
        },
        first_seen=dt.datetime(2026, 8, 24, 17, 42, 19, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0.0
    assert article["risk_on_score"] == 0.0
    assert article["currency_scores"] == {}
    assert article["research_currency_scores"] == {}
    assert article["category"] != "risk_off_geopolitical_or_financial"
    assert article["directional_evidence"] is False


def test_secondary_mixed_growth_and_inflation_headline_abstains():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Briefs Finance",
            "source_kind": "rss",
            "source_role": "news_aggregator",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": ["MXN"],
            "title": "Mexico GDP Misses, Inflation Rises, Banxico Holds",
            "summary": "",
            "url": "https://example.com/mixed-mexico-macro",
            "published_utc": "2026-08-24T17:46:22Z",
        },
        first_seen=dt.datetime(2026, 8, 24, 17, 53, 29, tzinfo=UTC),
    )
    assert article["activity_release_direction"] == -1
    assert article["monetary_impulse"] > 0
    assert article["secondary_mixed_macro_direction_conflict"] is True
    assert article["currency_scores"] == {}
    assert article["research_currency_scores"] == {}
    assert article["directional_evidence"] is False
    assert article["directional_publish_eligible"] is False


@pytest.mark.parametrize(
    "headline",
    [
        (
            "Treasury Launches Unprecedented Campaign Against Iranian "
            "Regime on Economic D-Day"
        ),
        (
            "Remarks from Secretary of the Treasury Scott Bessent on "
            "Operation Economic Outcast against Iran"
        ),
    ],
)
def test_official_treasury_iran_sanctions_campaign_maps_risk_off(headline):
    article = news.classify_article(
        {
            "source_id": "us_treasury_press",
            "source_name": "U.S. Treasury press releases",
            "source_kind": "html_links",
            "source_role": "primary_policy_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["USD"],
            "title": headline,
            "summary": (
                "Today Treasury has begun Operation Economic Outcast, an "
                "economic campaign against Iran and its enablers. OFAC is "
                "sanctioning nearly 60 entities, individuals, and vessels."
            ),
            "url": "https://home.treasury.gov/news/press-releases/example",
            "published_utc": "2026-08-24T17:42:18Z",
        },
        first_seen=dt.datetime(2026, 8, 24, 17, 42, 19, tzinfo=UTC),
    )
    assert article["official_sanctions_escalation"] is True
    assert article["category"] == "risk_off_geopolitical_or_financial"
    assert article["risk_off_score"] > 0
    assert article["directional_evidence"] is True
    assert article["currency_scores"]["USD"] > 0
    assert article["currency_scores"]["AUD"] < 0


def test_event_identity_is_scoped_to_source():
    common = {
        "source_name": "Publisher",
        "source_kind": "rss",
        "source_quality": 0.55,
        "source_verified": False,
        "source_currencies": [],
        "title": "Dollar steady as markets await central bank decision",
        "summary": "",
        "url": "",
        "published_utc": "2026-08-03T20:00:00Z",
    }
    first = news.classify_article(
        {**common, "source_id": "discovery_one"},
        first_seen=dt.datetime(2026, 8, 3, 20, 1, tzinfo=UTC),
    )
    second = news.classify_article(
        {**common, "source_id": "discovery_two"},
        first_seen=dt.datetime(2026, 8, 3, 20, 1, tzinfo=UTC),
    )
    assert first["event_id"] != second["event_id"]


def test_google_news_identity_is_shared_across_discovery_queries():
    common = {
        "source_name": "Publisher",
        "source_kind": "rss",
        "source_quality": 0.55,
        "source_verified": False,
        "source_currencies": [],
        "title": "Australia household spending rises",
        "summary": "",
        "url": "https://news.google.com/rss/articles/shared?oc=5",
        "external_id": "shared-google-news-item",
        "published_utc": "2026-08-04T01:30:00Z",
    }
    first = news.classify_article(
        {**common, "source_id": "google_news_fx_macro"},
        first_seen=dt.datetime(2026, 8, 4, 1, 31, tzinfo=UTC),
    )
    second = news.classify_article(
        {**common, "source_id": "google_news_market_ticker"},
        first_seen=dt.datetime(2026, 8, 4, 1, 32, tzinfo=UTC),
    )
    assert first["event_id"] == second["event_id"]


def test_future_reported_publication_holds_forward_causality():
    first_seen = dt.datetime(2026, 8, 4, 1, 29, 24, tzinfo=UTC)
    article = news.classify_article(
        {
            "source_id": "official_central_bank",
            "source_name": "Official central bank",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_currencies": ["AUD"],
            "title": "Central bank raises interest rate",
            "summary": "Official monetary policy tightening",
            "url": "https://example.gov/policy/decision",
            "published_utc": "2026-08-04T01:30:00Z",
        },
        first_seen=first_seen,
    )
    assert article["first_seen_utc"] == "2026-08-04T01:29:24+00:00"
    assert article["causal_known_utc"] == "2026-08-04T01:30:00+00:00"
    assert article["publication_hold_seconds"] == 36.0
    before = news.build_pair_scores(
        [article],
        ["AUD_USD"],
        as_of=dt.datetime(2026, 8, 4, 1, 29, 59, tzinfo=UTC),
    )
    after = news.build_pair_scores(
        [article],
        ["AUD_USD"],
        as_of=dt.datetime(2026, 8, 4, 1, 30, 1, tzinfo=UTC),
    )
    assert before["active_article_count"] == 0
    assert after["active_article_count"] == 1


def test_inferred_primary_release_gets_conservative_one_minute_hold():
    first_seen = dt.datetime(2026, 8, 4, 1, 29, 23, tzinfo=UTC)
    article = news.classify_article(
        {
            "source_id": "abs_latest_releases",
            "source_name": "Australian Bureau of Statistics latest releases",
            "source_kind": "html_links",
            "source_role": "primary_statistical_release",
            "source_quality": 0.98,
            "source_verified": True,
            "source_currencies": ["AUD"],
            "title": "Media Release - Household spending up 0.8% in June",
            "summary": "",
            "url": "https://www.abs.gov.au/example",
        },
        first_seen=first_seen,
    )
    assert article["published_time_inferred"] is True
    assert article["causal_known_utc"] == "2026-08-04T01:30:23+00:00"
    assert article["publication_hold_seconds"] == 60.0
    assert article["forward_signal_timely"] is False


def test_all_primary_statistical_inferred_clocks_fail_closed_but_exact_is_timely():
    common = {
        "source_id": "unrelated_official_statistics",
        "source_name": "Unrelated official statistics office",
        "source_kind": "html_links",
        "source_role": "primary_statistical_release",
        "source_quality": 1.0,
        "source_verified": True,
        "source_direct": True,
        "source_currencies": ["CAD"],
        "title": "Retail spending increased in July",
        "summary": "",
        "url": "https://statistics.example/release",
    }
    first_seen = dt.datetime(2026, 8, 27, 12, 1, tzinfo=UTC)
    inferred = news.classify_article(common, first_seen=first_seen)
    exact = news.classify_article(
        {**common, "published_utc": "2026-08-27T12:00:00Z"},
        first_seen=first_seen,
    )
    assert inferred["published_time_inferred"] is True
    assert inferred["forward_signal_timely"] is False
    assert exact["published_time_inferred"] is False
    assert exact["availability_lag_minutes"] == 1.0
    assert exact["forward_signal_timely"] is True


def test_cuban_parallel_market_quote_is_not_global_eur_direction():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "CiberCuba",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Informal currency market closes with surprises: dollar "
                "stagnant, euro declining, and MLC on the rise - CiberCuba"
            ),
            "summary": "Cuba's parallel exchange market update.",
            "url": "https://example.com/cuba-parallel-market",
            "published_utc": "2026-08-04T01:32:00Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 2, 12, tzinfo=UTC),
    )
    assert article["localized_parallel_currency_market"] is True
    assert article["currency_scores"] == {}
    assert article["relevant"] is False
    assert article["context_only"] is False
    assert article["directional_publish_eligible"] is False
    assert article["exclusion_reason"] == "localized_parallel_currency_market"


def test_sp_global_recurring_calendar_is_timing_only_and_holiday_aware():
    source = {
        "source_id": "sp_global_uk_pmi_calendar",
        "name": "S&P Global UK PMI official recurring calendar",
        "kind": "recurring_release_calendar",
        "publisher_url": (
            "https://pmi.spglobal.com/Public/Release/ReleaseDates?language=en&os=0"
        ),
        "calendar_lookback_months": 0,
        "calendar_lookahead_months": 0,
        "event_country": "United Kingdom",
        "excluded_working_dates": ["2026-01-01"],
        "rules": [
            {
                "event_name": "S&P Global UK Services and Composite PMI",
                "event_series_id": "sp_global_uk_services_composite_pmi",
                "working_day": 3,
                "utc_time": "08:30",
            }
        ],
        "currencies": ["GBP"],
        "verified": True,
        "direct": True,
        "source_role": "primary_statistical_calendar",
        "quality": 0.98,
        "source_contract_id": "sp_global_uk_pmi_calendar_v1",
        "source_cohort_id": "sp_global_uk_pmi_calendar_v1",
    }
    now = dt.datetime(2026, 1, 2, 12, 0, tzinfo=UTC)
    assert news.due_for_poll(
        source,
        {
            "last_attempt_utc": "2026-01-02T11:59:00+00:00",
            "last_error": "legacy HTTP 403",
            "consecutive_errors": 1,
        },
        {},
        now,
    ) is True
    assert news.due_for_poll(
        source,
        {
            "last_attempt_utc": "2026-01-02T11:59:00+00:00",
            "last_success_utc": "2026-01-02T11:59:00+00:00",
            "last_error": "",
            "parsed_items": 0,
        },
        {},
        now,
    ) is True
    rows, state = news.fetch_source(
        source,
        {},
        timeout_sec=1.0,
        maximum_bytes=1024,
        now=now,
    )

    assert state["last_status"] == 200
    assert state["schedule_materialization"] == "local_official_rule"
    assert state["source_contract_id"] == "sp_global_uk_pmi_calendar_v1"
    assert state["source_cohort_id"] == "sp_global_uk_pmi_calendar_v1"
    assert state["source_lineage_adopted_utc"] == now.isoformat()
    assert len(rows) == 1
    assert rows[0]["source_contract_id"] == "sp_global_uk_pmi_calendar_v1"
    assert rows[0]["source_cohort_id"] == "sp_global_uk_pmi_calendar_v1"
    assert rows[0]["scheduled_utc"] == "2026-01-06T08:30:00+00:00"
    assert rows[0]["actual_value"] is None
    assert rows[0]["consensus_value"] is None
    assert news.due_for_poll(source, state, {}, now) is False

    article = news.classify_article(
        rows[0],
        first_seen=dt.datetime(2026, 1, 2, 12, 0, tzinfo=UTC),
    )
    assert article["category"] == "business_activity_release"
    assert article["surprise_sign"] == "UNKNOWN"
    assert article["directional_publish_eligible"] is False


def test_sp_global_calendar_contract_includes_official_2027_uk_holidays():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row
        for row in config["sources"]
        if row.get("source_id") == "sp_global_uk_pmi_calendar"
    )
    expected = {
        "2027-01-01",
        "2027-03-26",
        "2027-03-29",
        "2027-05-03",
        "2027-05-31",
        "2027-08-30",
        "2027-12-27",
        "2027-12-28",
    }

    assert expected.issubset(set(source["excluded_working_dates"]))


def test_ism_calendar_uses_eastern_time_and_documented_2026_exceptions():
    source = {
        "source_id": "ism_us_pmi_calendar",
        "name": "Institute for Supply Management PMI official recurring calendar",
        "kind": "recurring_release_calendar",
        "publisher_url": (
            "https://www.ismworld.org/supply-management-news-and-reports/"
            "reports/rob-report-calendar/"
        ),
        "calendar_lookback_months": 0,
        "calendar_lookahead_months": 0,
        "event_country": "United States",
        "source_timezone": "America/New_York",
        "excluded_working_dates": [
            "2026-01-01",
            "2026-01-02",
            "2026-04-03",
            "2026-07-03",
        ],
        "rules": [
            {
                "event_name": "United States ISM Manufacturing PMI",
                "event_series_id": "ism_us_manufacturing_pmi",
                "working_day": 1,
                "local_time": "10:00",
            },
            {
                "event_name": "United States ISM Services PMI",
                "event_series_id": "ism_us_services_pmi",
                "working_day": 3,
                "local_time": "10:00",
            },
        ],
        "currencies": ["USD"],
        "verified": True,
        "direct": True,
        "source_role": "primary_statistical_calendar",
        "quality": 1.0,
    }
    july = news.build_recurring_release_calendar(
        source,
        now=dt.datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
    )
    assert [row["scheduled_utc"] for row in july] == [
        "2026-07-01T14:00:00+00:00",
        "2026-07-06T14:00:00+00:00",
    ]
    august = news.build_recurring_release_calendar(
        source,
        now=dt.datetime(2026, 8, 5, 12, 0, tzinfo=UTC),
    )
    assert [row["scheduled_utc"] for row in august] == [
        "2026-08-03T14:00:00+00:00",
        "2026-08-05T14:00:00+00:00",
    ]
    article = news.classify_article(
        august[1],
        first_seen=dt.datetime(2026, 8, 1, 12, 0, tzinfo=UTC),
    )
    assert article["category"] == "business_activity_release"
    assert article["currencies"] == ["USD"]
    assert article["directional_publish_eligible"] is False
    assert article["surprise_sign"] == "UNKNOWN"


def test_explicit_official_schedule_uses_exact_date_without_guessing_recurrence():
    source = {
        "source_id": "umich_consumer_sentiment_calendar",
        "name": "University of Michigan Surveys of Consumers official calendar",
        "kind": "recurring_release_calendar",
        "publisher_url": "https://data.sca.isr.umich.edu/",
        "calendar_lookback_months": 0,
        "calendar_lookahead_months": 1,
        "event_country": "United States",
        "source_timezone": "America/New_York",
        "explicit_schedule": [
            {
                "event_name": "United States University of Michigan Consumer Sentiment Preliminary",
                "event_series_id": "umich_consumer_sentiment_preliminary",
                "local_date": "2026-08-14",
                "local_time": "10:00",
            },
            {
                "event_name": "United States University of Michigan Consumer Sentiment Final",
                "event_series_id": "umich_consumer_sentiment_final",
                "local_date": "2026-08-28",
                "local_time": "10:00",
            },
        ],
        "currencies": ["USD"],
        "verified": True,
        "direct": True,
        "source_role": "primary_statistical_calendar",
    }
    rows = news.build_recurring_release_calendar(
        source,
        now=dt.datetime(2026, 8, 14, 14, 15, tzinfo=UTC),
    )
    assert [row["scheduled_utc"] for row in rows] == [
        "2026-08-14T14:00:00+00:00",
        "2026-08-28T14:00:00+00:00",
    ]
    article = news.classify_article(
        rows[0], first_seen=dt.datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
    )
    assert article["category"] == "business_activity_release"
    assert article["directional_publish_eligible"] is False
    assert article["consensus_value"] is None


def test_date_window_policy_schedule_preserves_uncertain_release_clock():
    source = {
        "source_id": "boj_policy_decision_calendar_2026",
        "name": "Bank of Japan official MPM schedule",
        "kind": "recurring_release_calendar",
        "publisher_url": "https://www.boj.or.jp/en/mopo/mpmsche_minu/",
        "calendar_lookback_months": 0,
        "calendar_lookahead_months": 2,
        "event_country": "Japan",
        "source_timezone": "Asia/Tokyo",
        "explicit_schedule": [
            {
                "event_name": "Bank of Japan Monetary Policy Decision",
                "event_series_id": "boj_monetary_policy_decision",
                "local_date": "2026-09-18",
                "local_time": "09:00",
                "release_window_end_local": "15:00",
                "timing_precision": "date_window",
            }
        ],
        "currencies": ["JPY"],
        "verified": True,
        "direct": True,
        "source_role": "primary_policy_calendar",
    }
    rows = news.build_recurring_release_calendar(
        source,
        now=dt.datetime(2026, 8, 16, 6, 0, tzinfo=UTC),
    )
    assert rows[0]["scheduled_utc"] == "2026-09-18T00:00:00+00:00"
    assert rows[0]["schedule_window_end_utc"] == "2026-09-18T06:00:00+00:00"
    assert rows[0]["timing_precision"] == "date_window"
    article = news.classify_article(
        rows[0], first_seen=dt.datetime(2026, 8, 16, 6, 0, tzinfo=UTC)
    )
    assert article["schedule_window_end_utc"] == "2026-09-18T06:00:00+00:00"
    assert article["directional_publish_eligible"] is False


def test_g18_policy_calendars_are_official_versioned_and_non_directional():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    sources = {row["source_id"]: row for row in config["sources"]}
    expected = {
        "fomc_policy_decision_calendar_2026": "USD",
        "ecb_policy_decision_calendar_2026": "EUR",
        "boe_policy_decision_calendar_2026": "GBP",
        "boc_policy_decision_calendar_2026": "CAD",
        "rba_policy_decision_calendar_2026": "AUD",
        "rbnz_policy_decision_calendar_2026": "NZD",
        "snb_policy_decision_calendar_2026": "CHF",
        "boj_policy_decision_calendar_2026": "JPY",
        "norges_policy_decision_calendar_2026": "NOK",
        "riksbank_policy_decision_calendar_2026": "SEK",
        "cnb_policy_decision_calendar_2026": "CZK",
        "nbp_policy_decision_calendar_2026": "PLN",
        "mnb_policy_decision_calendar_2026": "HUF",
        "banxico_policy_decision_calendar_2026": "MXN",
        "sarb_policy_decision_calendar_2026": "ZAR",
        "tcmb_policy_decision_calendar_2026": "TRY",
        "bot_policy_decision_calendar_2026": "THB",
        "pboc_lpr_fixing_calendar_2026": "CNH",
    }
    now = dt.datetime(2026, 8, 16, 6, 0, tzinfo=UTC)
    for source_id, currency in expected.items():
        source = sources[source_id]
        assert source["kind"] == "recurring_release_calendar"
        assert source["retrieval_via"] in {
            "official_published_fixed_schedule",
            "official_published_recurring_rule",
        }
        assert source["source_role"] == "primary_policy_calendar"
        assert source["source_contract_id"].endswith("_v1_20260816")
        assert source["source_cohort_id"] == source["source_contract_id"]
        assert source["currencies"] == [currency]
        assert source["directional_research_only"] is True
        rows = news.build_recurring_release_calendar(source, now=now)
        assert rows
        article = news.classify_article(rows[0], first_seen=now)
        assert article["category"] == "monetary_policy"
        assert article["directional_publish_eligible"] is False
        assert article["actual_value"] is None
        assert article["consensus_value"] is None


def test_stats_sa_ppi_calendar_arms_exact_clock_without_direction():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row
        for row in config["sources"]
        if row.get("source_id") == "stats_sa_ppi_release_calendar_2026_v1"
    )
    now = dt.datetime(2026, 8, 27, 8, 50, tzinfo=UTC)
    rows = news.build_recurring_release_calendar(source, now=now)
    august_clock = next(
        row for row in rows if row["scheduled_utc"] == "2026-08-27T09:30:00+00:00"
    )
    assert august_clock["source_contract_id"].endswith(
        "causal_activation_20260827T0850Z"
    )
    assert august_clock["source_currencies"] == ["ZAR"]
    article = news.classify_article(august_clock, first_seen=now)
    assert article["category"] == "inflation_release"
    assert article["scheduled_utc"] == "2026-08-27T09:30:00+00:00"
    assert article["consensus_value"] is None
    assert article["directional_publish_eligible"] is False
    assert article["execution_eligible"] is False


def test_mnb_policy_meeting_clock_remains_date_window_not_minutes_clock():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row
        for row in config["sources"]
        if row.get("source_id") == "mnb_policy_decision_calendar_2026"
    )

    assert source["source_contract_id"] == (
        "mnb_policy_decision_calendar_2026_v1_20260816"
    )
    assert all(
        row.get("timing_precision") == "date_window"
        and row.get("local_time") == "00:00"
        and row.get("release_window_end_local") == "23:59"
        for row in source["explicit_schedule"]
    )
    rows = news.build_recurring_release_calendar(
        source,
        now=dt.datetime(2026, 8, 17, 4, 0, tzinfo=UTC),
    )
    assert rows
    assert all(row["timing_precision"] == "date_window" for row in rows)
    assert all(row["schedule_window_end_utc"] for row in rows)


def test_numeric_zero_survives_classification_and_reclassification_shape():
    assert news.optional_float(0) == 0.0
    assert news.optional_float(0.0) == 0.0
    article = news.classify_article(
        {
            "source_id": "census_economic_indicators",
            "source_name": "US Census Bureau economic indicators",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_statistical_release",
            "source_currencies": ["USD"],
            "title": "Manufacturing and Trade Inventories and Sales",
            "summary": "June inventories were unchanged.",
            "url": "https://www.census.gov/mtis/index.html",
            "published_utc": "2026-08-14T14:00:00Z",
            "structured_event": True,
            "event_series_id": "business_sales",
            "event_name": "Manufacturing and Trade Inventories and Sales",
            "actual": "0.0",
            # This is the shape left by the earlier zero-loss bug. The raw
            # publisher text remains sufficient for deterministic recovery.
            "actual_value": None,
            "previous": "+0.4",
            "previous_value": 0.4,
            "unit": "percent change",
        },
        first_seen=dt.datetime(2026, 8, 14, 14, 0, 16, tzinfo=UTC),
    )
    assert article["actual"] == "0.0"
    assert article["actual_value"] == 0.0
    assert article["previous_value"] == 0.4


def test_umich_sources_are_official_and_prospective_only():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    by_id = {row.get("source_id"): row for row in config["sources"]}
    calendar = by_id["umich_consumer_sentiment_calendar"]
    reports = by_id["umich_consumer_sentiment_reports"]
    assert calendar["retrieval_via"] == "official_published_exact_schedule"
    assert reports["kind"] == "html_links"
    assert reports["link_patterns"] == [r"fetchdoc\.php\?docid="]
    assert reports["detail_enrichment"] == "official_document_text"
    assert set(calendar["trusted_domains"]) == {"data.sca.isr.umich.edu"}
    assert set(reports["trusted_domains"]) == {"data.sca.isr.umich.edu"}


def test_umich_report_listing_regex_matches_official_document_links():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row.get("source_id") == "umich_consumer_sentiment_reports"
    )
    payload = b'''<table><tr><td class="date">August 14, 2026</td>
        <td><a data-docname="August Preliminary Results"
        href="fetchdoc.php?docid=81625">August Preliminary Results</a></td>
        </tr></table>'''
    rows = news.parse_html_links(payload, source)
    assert len(rows) == 1
    assert rows[0]["title"] == "August Preliminary Results"
    assert rows[0]["url"] == (
        "https://data.sca.isr.umich.edu/fetchdoc.php?docid=81625"
    )


def test_umich_live_release_parser_preserves_activity_and_inflation_components():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row.get("source_id") == "umich_consumer_sentiment_current_release"
    )
    payload = b'''<h1>Preliminary Results for August 2026</h1>
      <table><tr><td>Index of Consumer Sentiment</td><td>51.0</td><td>55.2</td><td>58.2</td><td>-7.6%</td><td>-12.4%</td></tr>
      <tr><td>Current Economic Conditions</td><td>51.8</td><td>54.8</td><td>61.7</td></tr>
      <tr><td>Index of Consumer Expectations</td><td>50.6</td><td>55.4</td><td>55.9</td></tr></table>
      <h2>Surveys of Consumers Director Joanne Hsu</h2>
      <p>Consumer sentiment fell about 8% this August. Year-ahead inflation expectations ticked up from 4.2% in July to 4.3% this month. Long-run inflation expectations held steady at 3.3% for the third consecutive month.</p>
      <footer>Copyright 2026</footer>'''
    rows = news.parse_umich_current_release(payload, source)
    assert len(rows) == 1
    row = rows[0]
    assert row["structured_event"] is True
    assert row["event_series_id"] == "umich_consumer_sentiment_preliminary"
    assert row["actual_value"] == 51.0
    assert row["previous_value"] == 55.2
    assert row["published_utc"] == "2026-08-14T14:00:00+00:00"
    assert row["published_time_inferred"] is False
    assert row["timing_precision"] == "official_exact_schedule"
    unclocked_source = dict(source)
    unclocked_source.pop("release_utc_by_identity")
    unclocked = news.parse_umich_current_release(payload, unclocked_source)[0]
    assert unclocked["published_time_inferred"] is True
    assert unclocked["external_id"] != row["external_id"]
    assert row["numeric_extraction_contract_id"] == (
        "umich_live_release_components_numeric_v1_20260816"
    )
    assert row["source_native_components"]["consumer_expectations"] == {
        "actual": 50.6, "previous": 55.4, "year_ago": 55.9
    }
    assert row["source_native_components"][
        "year_ahead_inflation_expectations"
    ] == {"actual": 4.3, "previous": 4.2}
    assert row["source_native_components"][
        "long_run_inflation_expectations"
    ] == {"actual": 3.3}
    assert "Consumer sentiment fell" in row["summary"]


def test_umich_live_release_first_snapshot_is_bootstrap_and_components_survive():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row.get("source_id") == "umich_consumer_sentiment_current_release"
    )
    payload = b'''<h1>Preliminary Results for August 2026</h1>
      Index of Consumer Sentiment 51.0 55.2 58.2 -7.6% -12.4%
      Current Economic Conditions 51.8 54.8 61.7
      Index of Consumer Expectations 50.6 55.4 55.9
      Surveys of Consumers Director Joanne Hsu Consumer sentiment fell.
      Year-ahead inflation expectations ticked up from 4.2% in July to 4.3% this month.
      Copyright 2026'''
    rows = news.parse_umich_current_release(payload, source)
    known = news.annotate_singleton_release_history(rows, {})
    assert rows[0]["source_listing_bootstrap"] is True
    article = news.classify_article(
        rows[0],
        first_seen=dt.datetime(2026, 8, 14, 15, 0, tzinfo=UTC),
    )
    assert article["source_listing_bootstrap"] is True
    assert article["forward_signal_timely"] is False
    assert article["numeric_causal_known_utc"] == "2026-08-16T07:15:00+00:00"
    assert article["actual_value"] == 51.0
    assert article["source_native_components"][
        "year_ahead_inflation_expectations"
    ]["actual"] == 4.3
    component_change = article["structured_component_change"]
    assert component_change["cross_channel_conflict"] is True
    by_name = {
        row["component"]: row for row in component_change["components"]
    }
    assert by_name["consumer_sentiment"]["delta_previous"] == -4.2
    assert by_name["consumer_sentiment"]["rate_channel_sign"] == -1
    assert by_name["year_ahead_inflation_expectations"][
        "delta_previous"
    ] == 0.1
    assert by_name["year_ahead_inflation_expectations"][
        "rate_channel_sign"
    ] == 1
    assert component_change["consensus_component_count"] == 0
    assert component_change["directional_use"] == (
        "research_only_pending_consensus_and_rate_repricing"
    )
    next_rows = news.parse_umich_current_release(payload, source)
    news.annotate_singleton_release_history(
        next_rows, {"known_release_ids": known}
    )
    assert not next_rows[0].get("source_listing_bootstrap")


def test_bls_ppi_current_release_parser_preserves_embargo_and_components():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row.get("source_id") == "bls_ppi_current_release_v1"
    )
    payload = b'''<html><body>
      <h1>PRODUCER PRICE INDEXES - JULY 2026</h1>
      <p>EMBARGOED UNTIL 8:30 a.m. (ET) Thursday, August 13, 2026</p>
      <p>The Producer Price Index for final demand was unchanged in July. Final
      demand prices edged down 0.1 percent in June. On an unadjusted basis, the
      index for final demand advanced 4.7 percent for the 12 months ended in July.</p>
      <p>Prices for final demand less foods, energy, and trade services advanced
      0.4 percent in July after inching up 0.1 percent in June and advanced 4.7 percent for the
      12 months ended in July.</p>
      <h2>Product Detail</h2>
    </body></html>'''
    rows = news.parse_bls_current_release(payload, source)
    assert len(rows) == 2
    assert {row["source_id"] for row in rows} == {"bls_ppi_current_release_v1"}
    by_series = {row["event_series_id"]: row for row in rows}
    headline = by_series["WPSFD4"]
    underlying = by_series["WPSFD49116"]
    assert headline["published_utc"] == "2026-08-13T12:30:00+00:00"
    assert headline["timing_precision"] == "official_embargo_timestamp"
    assert headline["reference_period"] == "2026-07"
    assert headline["actual_value"] == 0.0
    assert headline["previous_value"] == -0.1
    assert headline["numeric_extraction_contract_id"] == (
        "bls_ppi_current_release_allowlisted_components_v1_20260816"
    )
    assert headline["source_native_components"]["year_over_year"]["actual"] == 4.7
    assert underlying["actual_value"] == 0.4
    assert underlying["previous_value"] == 0.1
    assert underlying["consensus_value"] is None
    assert underlying["directional_research_only"] is True


def test_bls_ppi_current_release_first_snapshot_is_noncausal_bootstrap():
    source = {
        "source_id": "bls_ppi_current_release_v1",
        "name": "BLS PPI",
        "kind": "bls_current_release",
        "url": "https://www.bls.gov/news.release/ppi.nr0.htm",
        "currencies": ["USD"],
        "verified": True,
        "direct": True,
        "source_role": "primary_statistical_release",
    }
    payload = b'''<html><body>
      PRODUCER PRICE INDEXES - JULY 2026
      EMBARGOED UNTIL 8:30 a.m. (ET) Thursday, August 13, 2026
      The Producer Price Index for final demand was unchanged in July. Final
      demand prices edged down 0.1 percent in June. The index for final demand
      advanced 4.7 percent for the 12 months ended in July.
      Prices for final demand less foods, energy, and trade services advanced
      0.4 percent in July. The index rose 0.1 percent in June and advanced 4.7
      percent for the 12 months ended in July. Product Detail
    </body></html>'''
    rows = news.parse_bls_current_release(payload, source)
    known = news.annotate_singleton_release_history(rows, {})
    assert len(known) == 2
    assert all(row["source_listing_bootstrap"] is True for row in rows)
    changed = payload.replace(b"0.4 percent in July", b"0.5 percent in July")
    changed_rows = news.parse_bls_current_release(changed, source)
    news.annotate_singleton_release_history(
        changed_rows, {"known_release_ids": known}
    )
    assert not any(row.get("source_listing_bootstrap") for row in changed_rows)
    assert changed_rows[1]["external_id"] != rows[1]["external_id"]


def test_unverified_us_ism_headline_is_usd_context_without_forced_direction():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Forex Factory",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "US Services PMI at 54.1%; July 2026 ISM Services PMI Report "
                "- Forex Factory"
            ),
            "summary": "",
            "url": "https://example.com/ism-services-pmi",
            "published_utc": "2026-08-05T14:09:00Z",
        },
        first_seen=dt.datetime(2026, 8, 5, 14, 9, tzinfo=UTC),
    )
    assert article["category"] == "business_activity_release"
    assert article["currencies"] == ["USD"]
    assert article["context_only"] is True
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_loaded_articles_collapse_legacy_exact_url_copies(tmp_path):
    database = tmp_path / "news.sqlite"
    connection = news.open_database(database)
    try:
        first_seen = dt.datetime(2026, 8, 4, 1, 31, tzinfo=UTC)
        common = {
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_currencies": ["AUD"],
            "title": "RBA raises interest rate",
            "summary": "Official monetary policy tightening",
            "url": "https://example.com/exact-story",
            "published_utc": "2026-08-04T01:30:00Z",
        }
        first = news.classify_article(
            {**common, "source_id": "legacy_query_one"},
            first_seen=first_seen,
        )
        second = news.classify_article(
            {**common, "source_id": "legacy_query_two"},
            first_seen=first_seen + dt.timedelta(minutes=1),
        )
        assert first["event_id"] != second["event_id"]
        assert news.upsert_articles(connection, [first], first_seen) == (1, 0)
        assert news.upsert_articles(
            connection,
            [second],
            first_seen + dt.timedelta(minutes=1),
        ) == (1, 0)
        loaded = news.load_relevant_articles(
            connection,
            since=dt.datetime(2026, 8, 4, 0, 0, tzinfo=UTC),
        )
        assert len(loaded) == 1
        assert loaded[0]["cross_query_duplicate_count"] == 1
        assert loaded[0]["duplicate_observation_count"] == 1
        assert loaded[0]["discovery_source_ids"] == [
            "legacy_query_one",
            "legacy_query_two",
        ]
        assert loaded[0]["first_seen_utc"] == "2026-08-04T01:31:00+00:00"
    finally:
        connection.close()


def test_exact_url_collapse_keeps_enriched_body_at_its_later_causal_time():
    rows = news.collapse_exact_source_url_duplicates(
        [
            {
                "event_id": "early",
                "source_id": "boj_updates",
                "source_url": "http://www.boj.or.jp/example.pdf",
                "first_seen_utc": "2026-08-09T23:50:18+00:00",
                "causal_known_utc": "2026-08-09T23:50:18+00:00",
                "last_seen_utc": "2026-08-10T14:00:00+00:00",
                "source_quality": 1.0,
                "summary": "",
                "detail_enriched": False,
            },
            {
                "event_id": "detail",
                "source_id": "boj_updates",
                "source_url": "https://www.boj.or.jp/example.pdf",
                "first_seen_utc": "2026-08-10T14:17:04+00:00",
                "causal_known_utc": "2026-08-10T14:17:04+00:00",
                "detail_available_utc": "2026-08-10T14:17:04+00:00",
                "last_seen_utc": "2026-08-10T14:17:04+00:00",
                "source_quality": 1.0,
                "summary": "Faster rate hikes may be appropriate.",
                "detail_enriched": True,
            },
        ]
    )
    assert len(rows) == 1
    assert rows[0]["event_id"] == "detail"
    assert rows[0]["detail_enriched"] is True
    assert rows[0]["first_seen_utc"] == "2026-08-09T23:50:18+00:00"
    assert rows[0]["causal_known_utc"] == "2026-08-10T14:17:04+00:00"


def test_incremental_topic_reload_preserves_original_duplicate_clock(tmp_path):
    connection = news.open_database(tmp_path / "news.sqlite")
    try:
        original_seen = dt.datetime(2026, 8, 14, 15, 21, tzinfo=UTC)
        repeated_seen = original_seen + dt.timedelta(minutes=16)
        raw = {
            "source_id": "google_news_systemic_catalyst",
            "source_name": "Example publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "title": "Drone attack targets oil tanker exiting Strait of Hormuz",
            "summary": "",
            "url": "https://example.com/tanker",
            "published_utc": "2026-08-14T15:16:25Z",
        }
        original = news.classify_article(raw, first_seen=original_seen)
        repeated = news.classify_article(raw, first_seen=repeated_seen)
        assert news.upsert_articles(connection, [original], original_seen) == (1, 0)
        assert news.upsert_articles(connection, [repeated], repeated_seen) == (0, 1)
        rows = news.load_canonical_articles_by_event_ids(
            connection, [repeated["event_id"]]
        )
        assert len(rows) == 1
        assert rows[0]["first_seen_utc"] == news.iso_utc(original_seen)
        assert rows[0]["causal_known_utc"] == news.iso_utc(original_seen)
        topics = news.cluster_articles(rows, as_of=repeated_seen)
        assert topics[0]["causal_known_utc"] == news.iso_utc(original_seen)
    finally:
        connection.close()


def test_incremental_topic_clusters_new_url_against_retained_story(tmp_path):
    connection = news.open_database(tmp_path / "news.sqlite")
    try:
        first_seen = dt.datetime(2026, 8, 14, 8, 58, tzinfo=UTC)
        repeated_seen = dt.datetime(2026, 8, 14, 15, 21, tzinfo=UTC)
        first = news.classify_article(
            {
                "source_id": "google_news_systemic_catalyst",
                "source_name": "Publisher A",
                "source_kind": "rss",
                "source_quality": 0.55,
                "source_verified": False,
                "title": "Drone attack damages oil tanker in Strait of Hormuz",
                "summary": "",
                "url": "https://a.example/tanker",
                "published_utc": "2026-08-14T08:53:00Z",
            },
            first_seen=first_seen,
        )
        assert news.upsert_articles(connection, [first], first_seen) == (1, 0)
        initial_topics = news.cluster_articles([first], as_of=first_seen)
        assert news.upsert_topic_events(connection, initial_topics) == (1, 0)
        repeated = news.classify_article(
            {
                "source_id": "google_news_systemic_catalyst",
                "source_name": "Publisher B",
                "source_kind": "rss",
                "source_quality": 0.55,
                "source_verified": False,
                "title": "Drone attack targets oil tanker exiting the Strait of Hormuz",
                "summary": "",
                "url": "https://b.example/tanker-rewrite",
                "published_utc": "2026-08-14T15:16:00Z",
            },
            first_seen=repeated_seen,
        )
        assert news.upsert_articles(connection, [repeated], repeated_seen) == (1, 0)
        topics = news.cluster_incremental_topics_with_history(
            connection, [repeated], as_of=repeated_seen
        )
        assert len(topics) == 1
        assert topics[0]["causal_known_utc"] == news.iso_utc(first_seen)
        assert set(topics[0]["article_event_ids"]) == {
            first["event_id"], repeated["event_id"]
        }
    finally:
        connection.close()


def test_retained_legacy_rows_are_reclassified_without_changing_first_seen(tmp_path):
    database = tmp_path / "news.sqlite"
    connection = news.open_database(database)
    try:
        first_seen = dt.datetime(2026, 7, 29, 12, 1, tzinfo=UTC)
        article = news.classify_article(
            {
                "source_id": "google_news_fx_macro",
                "source_name": "Readers.id",
                "source_kind": "rss",
                "source_quality": 0.55,
                "source_verified": False,
                "source_currencies": [],
                "title": "Federal Reserve weighs interest rate hike",
                "summary": "Inflation remains a concern.",
                "url": "https://example.com/speculation",
                "published_utc": "2026-07-29T12:00:00Z",
            },
            first_seen=first_seen,
        )
        article["classification_version"] = "legacy"
        article["currency_scores"] = {"USD": 0.9}
        article["directional_bias"] = {"USD": "BULLISH"}
        article["monetary_impulse"] = 0.9
        news.upsert_articles(connection, [article], first_seen)

        changed = news.reclassify_stored_articles(
            connection,
            sources={"google_news_fx_macro": {"currencies": []}},
            since=dt.datetime(2026, 7, 29, 0, 0, tzinfo=UTC),
        )
        assert changed == 1
        row = connection.execute(
            """
            SELECT first_seen_utc, monetary_impulse, currency_scores_json,
                   payload_json
            FROM articles
            """
        ).fetchone()
        assert row[0] == "2026-07-29T12:01:00+00:00"
        assert row[1] == 0.0
        assert row[2] == "{}"
        assert json.loads(row[3])["classification_version"] == (
            news.CLASSIFICATION_VERSION
        )
    finally:
        connection.close()


def test_reclassification_prioritizes_newest_release(tmp_path, monkeypatch):
    database = tmp_path / "news.sqlite"
    connection = news.open_database(database)
    try:
        observed_at = dt.datetime(2026, 8, 14, 12, 31, tzinfo=UTC)
        for title, published in (
            ("older release", "2026-08-13T12:30:00Z"),
            ("fresh release", "2026-08-14T12:30:00Z"),
        ):
            article = news.classify_article(
                {
                    "source_id": "test_official",
                    "source_name": "Test Official",
                    "source_kind": "rss",
                    "source_quality": 1.0,
                    "source_verified": True,
                    "source_currencies": ["USD"],
                    "title": title,
                    "summary": "Official release.",
                    "url": f"https://example.com/{title.replace(' ', '-')}",
                    "published_utc": published,
                },
                first_seen=observed_at,
            )
            article["classification_version"] = "legacy"
            news.upsert_articles(connection, [article], observed_at)

        call_order = []
        original = news.classify_article

        def recording_classifier(raw, *, first_seen):
            call_order.append(raw["title"])
            return original(raw, first_seen=first_seen)

        monkeypatch.setattr(news, "classify_article", recording_classifier)
        changed = news.reclassify_stored_articles(
            connection,
            sources={"test_official": {"currencies": ["USD"], "direct": True}},
            since=dt.datetime(2026, 8, 13, 0, 0, tzinfo=UTC),
        )
        assert changed == 2
        assert call_order == ["fresh release", "older release"]
    finally:
        connection.close()


def test_reclassification_prioritizes_verified_official_before_newer_discovery(
    tmp_path, monkeypatch
):
    connection = news.open_database(tmp_path / "news.sqlite")
    try:
        observed_at = dt.datetime(2026, 8, 14, 12, 31, tzinfo=UTC)
        for source_id, verified, title, published in (
            (
                "test_discovery",
                False,
                "newer discovery",
                "2026-08-14T12:30:00Z",
            ),
            (
                "test_official",
                True,
                "older official release",
                "2026-08-13T12:30:00Z",
            ),
        ):
            article = news.classify_article(
                {
                    "source_id": source_id,
                    "source_name": title,
                    "source_kind": "rss",
                    "source_quality": 1.0 if verified else 0.5,
                    "source_verified": verified,
                    "source_currencies": ["USD"],
                    "title": title,
                    "summary": "Release text.",
                    "url": f"https://example.com/{source_id}",
                    "published_utc": published,
                },
                first_seen=observed_at,
            )
            article["classification_version"] = "legacy"
            news.upsert_articles(connection, [article], observed_at)

        call_order = []
        original = news.classify_article

        def recording_classifier(raw, *, first_seen):
            call_order.append(raw["title"])
            return original(raw, first_seen=first_seen)

        monkeypatch.setattr(news, "classify_article", recording_classifier)
        changed = news.reclassify_stored_articles(
            connection,
            sources={
                "test_official": {"currencies": ["USD"], "direct": True},
                "test_discovery": {"currencies": ["USD"], "direct": False},
            },
            since=dt.datetime(2026, 8, 13, 0, tzinfo=UTC),
            maximum_rows=1,
        )
        assert changed == 1
        assert call_order == ["older official release"]
    finally:
        connection.close()


def test_reclassification_is_bounded_and_resumes_oldest_stale_rows(tmp_path):
    connection = news.open_database(tmp_path / "news.sqlite")
    try:
        first_seen = dt.datetime(2026,8,14,13,tzinfo=UTC)
        for index in range(3):
            article = news.classify_article(
                {
                    "source_id":"test_official","source_name":"Test Official",
                    "source_kind":"rss","source_quality":1.0,"source_verified":True,
                    "source_currencies":["USD"],"title":f"Release {index}",
                    "summary":"Official release.","url":f"https://example.test/{index}",
                    "published_utc":f"2026-08-14T1{index}:00:00Z",
                },
                first_seen=first_seen,
            )
            article["classification_version"]="legacy"
            news.upsert_articles(connection,[article],first_seen)
        changed = news.reclassify_stored_articles(
            connection,
            sources={"test_official":{"currencies":["USD"],"direct":True}},
            since=dt.datetime(2026,8,14,0,tzinfo=UTC),
            maximum_rows=2,
        )
        assert changed == 2
        remaining = connection.execute(
            "SELECT count(*) FROM articles WHERE json_extract(payload_json,'$.classification_version')<>?",
            (news.CLASSIFICATION_VERSION,),
        ).fetchone()[0]
        assert remaining == 1
        assert news.reclassify_stored_articles(
            connection,
            sources={"test_official":{"currencies":["USD"],"direct":True}},
            since=dt.datetime(2026,8,14,0,tzinfo=UTC),
            maximum_rows=2,
        ) == 1
    finally:
        connection.close()


def test_recent_reclassification_republishes_topic_before_network_poll(tmp_path):
    observed = dt.datetime(2026, 8, 14, 13, 4, 7, tzinfo=UTC)
    connection = news.open_database(tmp_path / "news.sqlite")
    raw = {
        "source_id": "google_news_fx_macro",
        "source_name": "TradingKey",
        "source_kind": "rss",
        "source_quality": 0.7,
        "source_verified": False,
        "source_direct": False,
        "source_currencies": ["USD"],
        "title": (
            "US July Retail Sales Unexpectedly Fall 0.6% as Spending Cools, "
            "Hitting Fed Rate-Hike Expectations Again"
        ),
        "url": "https://example.test/retail-sales-refresh",
        "published_utc": "2026-08-14T13:03:00Z",
    }
    stale = news.classify_article(raw, first_seen=observed)
    stale["classification_version"] = "obsolete_rules"
    stale["currency_scores"] = {"USD": 0.9}
    stale["directional_bias"] = {"USD": "BULLISH"}
    stale["category"] = "market_news"
    stale["directional_publish_eligible"] = True
    news.upsert_articles(connection, [stale], observed)
    news.upsert_topic_events(
        connection,
        news.cluster_articles([stale], as_of=observed),
    )
    result = news.refresh_recent_topic_contract(
        connection,
        sources={
            "google_news_fx_macro": {
                "currencies": ["USD"],
                "direct": False,
                "retrieval_via": "search_discovery",
            }
        },
        since=observed - dt.timedelta(hours=1),
        as_of=observed,
    )
    assert result["reclassified"] == 1
    rows = connection.execute("SELECT payload_json FROM topic_events").fetchall()
    payloads = [json.loads(row[0]) for row in rows]
    current = [
        row
        for row in payloads
        if row.get("classification_version") == news.CLASSIFICATION_VERSION
    ]
    assert current
    assert all(
        row.get("classification_version") == news.CLASSIFICATION_VERSION
        for row in payloads
    )
    assert any(row.get("activity_release_direction") == -1 for row in current)
    assert all(row.get("currency_scores", {}).get("USD", 0) <= 0 for row in current)

    # A crash/restart may leave the article on the current contract but its
    # derived topic on the prior one.  That mismatch must also be repaired
    # even though no article itself requires reclassification.
    topic_id, topic_payload_json = connection.execute(
        "SELECT topic_id,payload_json FROM topic_events LIMIT 1"
    ).fetchone()
    topic_payload = json.loads(topic_payload_json)
    topic_payload["classification_version"] = "obsolete_topic_rules"
    topic_payload["currency_scores"] = {"USD": 0.9}
    connection.execute(
        "UPDATE topic_events SET payload_json=? WHERE topic_id=?",
        (json.dumps(topic_payload), topic_id),
    )
    connection.commit()
    repaired = news.refresh_recent_topic_contract(
        connection,
        sources={
            "google_news_fx_macro": {
                "currencies": ["USD"],
                "direct": False,
                "retrieval_via": "search_discovery",
            }
        },
        since=observed - dt.timedelta(hours=1),
        as_of=observed,
    )
    assert repaired["reclassified"] == 0
    assert repaired["stale_topics"] == 1
    repaired_payload = json.loads(
        connection.execute(
            "SELECT payload_json FROM topic_events WHERE topic_id=?", (topic_id,)
        ).fetchone()[0]
    )
    assert repaired_payload["classification_version"] == news.CLASSIFICATION_VERSION
    connection.close()


def test_run_cycle_commits_each_source_at_its_actual_completion_time(
    tmp_path, monkeypatch
):
    output_root = tmp_path / "news"
    config_path = tmp_path / "sources.json"
    config_path.write_text(
        json.dumps(
            {
                "policy": {"retention_days": 45, "request_timeout_sec": 2},
                "sources": [
                    {
                        "source_id": "source_one",
                        "name": "Source One",
                        "kind": "rss",
                        "url": "https://example.com/one.xml",
                        "enabled": True,
                        "runtime_supported": True,
                        "verified": True,
                        "direct": True,
                        "currencies": ["USD"],
                    },
                    {
                        "source_id": "source_two",
                        "name": "Source Two",
                        "kind": "rss",
                        "url": "https://example.com/two.xml",
                        "enabled": True,
                        "runtime_supported": True,
                        "verified": True,
                        "direct": True,
                        "currencies": ["JPY"],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    clock = [
        dt.datetime(2026, 8, 14, 12, minute, tzinfo=UTC)
        for minute in range(6)
    ]
    clock_index = {"value": 0}

    def test_clock():
        index = min(clock_index["value"], len(clock) - 1)
        clock_index["value"] += 1
        return clock[index]

    monkeypatch.setattr(news, "utc_now", test_clock)
    monkeypatch.setattr(
        news,
        "normalized_observation_time",
        lambda value: (
            value,
            {
                "source": "test",
                "normalized_utc": news.iso_utc(value),
                "status": "aligned",
                "contract_id": news.OBSERVATION_TIME_CONTRACT_ID,
                "trusted_for_prospective_evidence": True,
            },
        ),
    )
    observed_during_second_fetch = {}

    def fake_fetch(source, source_state, *, timeout_sec, maximum_bytes, now):
        if source["source_id"] == "source_two":
            database = output_root / "local_news_sentiment_v1.sqlite"
            with sqlite3.connect(database) as reader:
                observed_during_second_fetch["row"] = reader.execute(
                    "SELECT source_id, first_seen_utc FROM articles"
                ).fetchone()
                observed_during_second_fetch["topic_count"] = reader.execute(
                    "SELECT COUNT(*) FROM topic_events"
                ).fetchone()[0]
        raw = {
            "source_id": source["source_id"],
            "source_name": source["name"],
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": source["currencies"],
            "title": f"{source['name']} central bank raises interest rate",
            "summary": "The central bank announced an interest rate hike.",
            "url": source["url"],
            "published_utc": "2026-08-14T12:00:00Z",
        }
        return [raw], {
            **source_state,
            "last_attempt_utc": news.iso_utc(now),
            "last_success_utc": news.iso_utc(now),
            "last_status": 200,
            "last_error": "",
            "parsed_items": 1,
        }

    monkeypatch.setattr(news, "fetch_source", fake_fetch)
    monkeypatch.setattr(news.event_tagger, "discover_instruments", lambda: ["USD_JPY"])
    result = news.run_cycle(
        config_path=config_path,
        output_root=output_root,
        ledger_path=tmp_path / "ledger.csv",
        event_root=tmp_path / "events",
        refresh_event_catalog=False,
    )
    assert observed_during_second_fetch["row"] == (
        "source_one",
        "2026-08-14T12:02:00+00:00",
    )
    assert observed_during_second_fetch["topic_count"] == 1
    assert result["inserted_items"] == 2
    assert result["generated_utc"] == "2026-08-14T12:05:00+00:00"


def test_reclassification_preserves_html_listing_bootstrap_guard(tmp_path):
    database = tmp_path / "news.sqlite"
    connection = news.open_database(database)
    try:
        first_seen = dt.datetime(2026, 8, 3, 18, 30, tzinfo=UTC)
        article = news.classify_article(
            {
                "source_id": "japan_mof_international_policy",
                "source_name": "Japan Ministry of Finance",
                "source_kind": "html_links",
                "source_role": "primary_policy_release",
                "source_quality": 1.0,
                "source_verified": True,
                "source_currencies": ["JPY"],
                "source_listing_bootstrap": True,
                "title": "Japan intervenes in foreign exchange market to support yen",
                "summary": "",
                "url": "https://www.mof.go.jp/english/example",
                "published_utc": "",
            },
            first_seen=first_seen,
        )
        article["classification_version"] = "legacy"
        assert news.upsert_articles(connection, [article], first_seen) == (1, 0)

        changed = news.reclassify_stored_articles(
            connection,
            sources={
                "japan_mof_international_policy": {
                    "currencies": ["JPY"],
                    "direct": True,
                }
            },
            since=dt.datetime(2026, 8, 3, 0, 0, tzinfo=UTC),
        )
        assert changed == 1
        stored = json.loads(
            connection.execute("SELECT payload_json FROM articles").fetchone()[0]
        )
        assert stored["source_listing_bootstrap"] is True
        assert stored["forward_signal_timely"] is False
    finally:
        connection.close()


def test_boe_maintained_bank_rate_is_an_official_neutral_topic():
    article = news.classify_article(
        {
            "source_id": "boe_news",
            "source_name": "Bank of England",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["GBP"],
            "title": (
                "Bank Rate maintained at 3.75% - July 2026 "
                "Monetary Policy Summary and Minutes"
            ),
            "summary": "The Committee voted 6-3 to maintain Bank Rate.",
            "url": "https://www.bankofengland.co.uk/example",
            "published_utc": "2026-07-30T11:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 30, 11, 1, tzinfo=UTC),
    )
    assert article["official_policy_release"] is True
    assert article["relevant"] is True
    assert article["currency_scores"] == {}
    assert article["direct_currencies"] == ["GBP"]
    assert article["topic_action"] == "hold"
    assert article["policy_vote_split"] == "6-3"
    assert "#gbp_hold" in article["topic_tags"]


def test_boj_hedonic_review_pdf_is_not_fx_intervention_or_policy_vote():
    article = news.classify_article(
        {
            "source_id": "boj_updates",
            "source_name": "Bank of Japan updates",
            "source_kind": "rss",
            "source_role": "primary_policy_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["JPY"],
            "title": (
                "(BOJ Review) Expanding and Revising the Application of "
                "Hedonic Quality Adjustment in the Corporate Goods Price Index"
            ),
            "summary": (
                "This Research and Statistics Department methodology paper "
                "estimates quality-adjusted passenger-car prices. A driver "
                "monitoring system can stop a vehicle without driver "
                "intervention. Chart 8 uses an axis labelled -40 -20 0 20 40. "
                "The appendix compares products in Canada and the United "
                "Kingdom and discusses import pricing associated with tariffs."
            ),
            "url": (
                "https://www.boj.or.jp/en/research/wps_rev/rev_2026/"
                "rev26e11.htm"
            ),
            "published_utc": "2026-08-28T05:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 28, 5, 2, tzinfo=UTC),
    )
    assert article["official_non_market_research"] is True
    assert article["category"] == "market_news"
    assert article["relevant"] is False
    assert article["exclusion_reason"] == "official_non_market_research"
    assert article["currencies"] == ["JPY"]
    assert article["direct_currencies"] == ["JPY"]
    assert article["mentioned_currency_entities"] == ["JPY"]
    assert article["source_native_currency_bound"] is True
    assert article["intervention_status"] == ""
    assert article["policy_vote_split"] == ""
    assert article["topic_entities"] == []
    assert "#fx_intervention" not in article["topic_tags"]
    assert article["currency_scores"] == {}


def test_cbrt_student_paper_contest_is_non_market_administrative_context():
    article = news.classify_article(
        {
            "source_id": "tcmb_press",
            "source_name": "Central Bank of the Republic of Turkiye press releases",
            "source_kind": "rss",
            "source_role": "primary_policy_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["TRY"],
            "title": (
                "Press Release on Results of CBRT Paper Contest for "
                "University Students (2026-37)"
            ),
            "summary": (
                "The evaluation process for the CBRT Paper Contest for "
                "University Students has been completed. Students interested "
                "in economics and monetary policy will present their papers."
            ),
            "url": (
                "https://www.tcmb.gov.tr/wps/wcm/connect/en/tcmb+en/"
                "main+menu/announcements/press+releases/2026/ano2026-37"
            ),
            "published_utc": "2026-08-28T10:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 28, 11, 27, 43, tzinfo=UTC),
    )
    assert article["official_non_market_administrative"] is True
    assert article["category"] == "official_non_market_administrative"
    assert article["relevant"] is False
    assert article["exclusion_reason"] == "official_non_market_administrative"
    assert article["currencies"] == ["TRY"]
    assert article["direct_currencies"] == ["TRY"]
    assert article["currency_scores"] == {}
    assert article["monetary_impulse"] == 0.0
    assert article["official_policy_release"] is False


def test_cbrt_genuine_policy_decision_remains_market_policy_evidence():
    article = news.classify_article(
        {
            "source_id": "tcmb_press",
            "source_name": "Central Bank of the Republic of Turkiye press releases",
            "source_kind": "rss",
            "source_role": "primary_policy_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["TRY"],
            "title": "Press Release on Interest Rates (2026-34)",
            "summary": (
                "The Monetary Policy Committee decided to reduce the policy "
                "rate by 100 basis points to 42 percent."
            ),
            "url": (
                "https://www.tcmb.gov.tr/wps/wcm/connect/en/tcmb+en/"
                "main+menu/announcements/press+releases/2026/ano2026-34"
            ),
            "published_utc": "2026-07-23T11:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 23, 11, 0, 5, tzinfo=UTC),
    )
    assert article["official_non_market_administrative"] is False
    assert article["official_policy_release"] is True
    assert article["category"] == "monetary_policy"
    assert article["relevant"] is True
    assert article["exclusion_reason"] == ""
    assert article["currencies"] == ["TRY"]
    assert article["currency_scores"] == {"TRY": -1.0}
    assert article["topic_action"] == "cut"


def test_mnb_explicit_cut_overrides_foreign_policy_recap_and_stays_huf_native():
    article = news.classify_article(
        {
            "source_id": "mnb_policy_decisions_direct_v1",
            "source_name": "Magyar Nemzeti Bank Monetary Council releases (direct)",
            "source_kind": "html_links",
            "source_role": "primary_policy_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["HUF"],
            "title": "Press release on the Monetary Council meeting of 25 August 2026",
            "summary": (
                "In July, the European Central Bank and the Federal Reserve kept "
                "policy rates unchanged. Looking ahead, the Bank of Japan is "
                "expected to raise the policy rate. The Monetary Council reduced "
                "the base rate by 25 basis points to 5.50 percent at today's "
                "meeting. The Council will decide on the future path based on "
                "the September Inflation Report."
            ),
            "url": "https://www.mnb.hu/en/monetary-policy/example",
            "published_utc": "2026-08-25T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 25, 12, 1, tzinfo=UTC),
    )
    assert article["official_policy_release"] is True
    assert article["monetary_impulse"] == -1.0
    assert article["currency_scores"] == {"HUF": -1.0}
    assert article["direct_currencies"] == ["HUF"]
    assert article["source_native_currency_bound"] is True
    assert article["topic_action"] == "cut"
    assert "#huf_cut" in article["topic_tags"]


def test_mnb_native_language_cut_is_direct_huf_policy_evidence():
    article = news.classify_article(
        {
            "source_id": "mnb_policy_decisions_hu_direct_v1",
            "source_name": "Magyar Nemzeti Bank Monetary Council releases (Hungarian direct)",
            "source_kind": "html_links",
            "source_role": "primary_policy_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["HUF"],
            "title": "Közlemény a Monetáris Tanács 2026. augusztus 25-i üléséről",
            "summary": (
                "A Monetáris Tanács mai ülésén az alapkamatot 25 "
                "bázisponttal, 5,50 százalékra mérsékelte."
            ),
            "url": "https://www.mnb.hu/monetaris-politika/example",
            "published_utc": "2026-08-25T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 25, 12, 1, tzinfo=UTC),
    )
    assert article["official_policy_release"] is True
    assert article["monetary_impulse"] == -1.0
    assert article["currency_scores"] == {"HUF": -1.0}
    assert article["direct_currencies"] == ["HUF"]
    assert article["topic_action"] == "cut"


def test_official_policy_hold_does_not_rebroadcast_risk_words_from_summary():
    article = news.classify_article(
        {
            "source_id": "riksbank_press",
            "source_name": "Sveriges Riksbank",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_currencies": ["SEK"],
            "title": "Policy rate unchanged at 1.75 per cent",
            "summary": (
                "Supply disruptions from the war in the Middle East have "
                "raised oil prices. "
                "The policy rate is unchanged."
            ),
            "url": "https://www.riksbank.se/example",
            "published_utc": "2026-07-30T07:30:00Z",
        },
        first_seen=dt.datetime(2026, 7, 30, 7, 31, tzinfo=UTC),
    )
    assert article["official_policy_release"] is True
    assert article["category"] == "monetary_policy"
    assert article["risk_off_score"] == 0.0
    assert article["currency_scores"] == {}
    assert "#sek_hold" in article["topic_tags"]
    assert article["topic_entities"] == []
    assert "#middle_east_hold" not in article["topic_tags"]


def test_bounded_checkpoint_exits_cleanly_without_venv_launcher_orphan(tmp_path):
    database = tmp_path / "checkpoint.sqlite"
    connection = news.open_database(database)
    connection.execute("CREATE TABLE checkpoint_test (value INTEGER)")
    connection.execute("INSERT INTO checkpoint_test VALUES (1)")
    connection.commit()
    result = news.bounded_wal_checkpoint(
        database,
        minimum_bytes=0,
        timeout_sec=5.0,
    )
    assert result["status"] in {"ok", "busy"}
    assert result["status"] != "timeout"
    connection.close()


def test_bounded_checkpoint_is_passive_and_nonblocking(tmp_path, monkeypatch):
    database = tmp_path / "checkpoint.sqlite"
    connection = news.open_database(database)
    connection.execute("CREATE TABLE checkpoint_test (value INTEGER)")
    connection.execute("INSERT INTO checkpoint_test VALUES (1)")
    connection.commit()
    captured = {}
    real_run = news.subprocess.run

    def capture_run(command, **kwargs):
        captured["script"] = command[2]
        return real_run(command, **kwargs)

    monkeypatch.setattr(news.subprocess, "run", capture_run)
    result = news.bounded_wal_checkpoint(
        database,
        minimum_bytes=0,
        timeout_sec=5.0,
    )
    assert result["status"] in {"ok", "busy"}
    assert "wal_checkpoint(PASSIVE)" in captured["script"]
    assert "wal_checkpoint(TRUNCATE)" not in captured["script"]
    connection.close()


def test_market_commentary_about_existing_war_is_not_a_new_global_event():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Euro yields firm as Middle East war and Fed ambiguity "
                "stoke inflation fears"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/example",
            "published_utc": "2026-07-30T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 30, 12, 1, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0.0
    assert article["currency_scores"] == {}
    assert article["relevant"] is False
    assert article["context_only"] is True


def test_reported_attack_and_entry_into_war_remain_escalation():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Saudi Arabia joins U.S. war after oil sites come under attack",
            "summary": "",
            "url": "https://news.google.com/rss/articles/example-two",
            "published_utc": "2026-07-30T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 30, 12, 1, tzinfo=UTC),
    )
    assert article["risk_off_score"] > 0
    assert article["category"] == "risk_off_geopolitical_or_financial"
    assert article["currency_scores"]


def test_hormuz_reopening_deal_is_a_fresh_deescalation_catalyst():
    article = news.classify_article(
        {
            "source_id": "google_news_systemic_catalyst",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Iran and Oman make progress on a deal to reopen the "
                "Strait of Hormuz, officials say"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/hormuz-reopening",
            "published_utc": "2026-08-04T16:17:23Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 16, 18, tzinfo=UTC),
    )
    assert article["category"] == "risk_on_deescalation"
    assert article["risk_on_score"] > 0
    assert article["relevant"] is True
    assert article["context_only"] is False
    assert article["forward_signal_timely"] is True
    assert article["currency_scores"]


def test_possible_hormuz_deal_is_a_modest_risk_on_setup():
    article = news.classify_article(
        {
            "source_id": "google_news_systemic_catalyst",
            "source_name": "Associated Press syndication",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Possible Strait of Hormuz deal, a large attack in Yemen "
                "and other Mideast news"
            ),
            "summary": "",
            "url": "https://example.com/hormuz-possible",
            "published_utc": "2026-08-06T08:34:41Z",
        },
        first_seen=dt.datetime(2026, 8, 6, 8, 38, tzinfo=UTC),
    )
    assert article["classification_version"] == news.CLASSIFICATION_VERSION
    assert article["category"] == "risk_on_deescalation"
    assert article["risk_on_score"] == 0.4
    assert article["risk_off_score"] == 0
    assert article["currency_scores"]["CHF"] < 0
    assert article["currency_scores"]["AUD"] > 0


def test_failed_hormuz_deal_is_not_a_risk_on_setup():
    article = news.classify_article(
        {
            "source_id": "google_news_systemic_catalyst",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Strait of Hormuz deal is not feasible, shippers say",
            "summary": "",
            "url": "https://example.com/hormuz-not-feasible",
            "published_utc": "2026-08-06T14:50:40Z",
        },
        first_seen=dt.datetime(2026, 8, 6, 15, 8, tzinfo=UTC),
    )
    assert article["risk_on_score"] == 0
    assert article["category"] != "risk_on_deescalation"


def test_oman_foreign_ministry_direct_source_is_bootstrap_safe():
    config = json.loads(
        (news.ROOT / "config" / "news_sources_v1.json").read_text(
            encoding="utf-8"
        )
    )
    source = next(
        row
        for row in config["sources"]
        if row.get("source_id") == "oman_foreign_ministry_news_direct_v1"
    )
    assert source["kind"] == "rss"
    assert source["verified"] is True
    assert source["direct"] is True
    assert source["bootstrap_existing_items"] is True
    assert source["directional_research_only"] is True
    assert source["trusted_domains"] == ["fm.gov.om"]


def test_oman_routine_diplomacy_ignores_related_story_war_boilerplate():
    article = news.classify_article(
        {
            "source_id": "oman_foreign_ministry_news_direct_v1",
            "source_name": "Oman Foreign Ministry",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_foreign_policy_and_hormuz_release",
            "source_currencies": [],
            "title": "Egypt: Ambassador explores health cooperation",
            "summary": (
                "The ambassador visited the Egyptian Health Council. "
                "Discussions focused on cooperation in medical education. "
                "Previous Related stories: Oman condemns drone attacks. "
                "Recent Speeches: Minister proposes a path away from war."
            ),
            "url": "https://www.fm.gov.om/en/routine-health-cooperation",
            "published_utc": "2026-08-18T18:45:28Z",
        },
        first_seen=dt.datetime(2026, 8, 18, 18, 46, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0.0
    assert article["risk_on_score"] == 0.0
    assert article["currency_scores"] == {}
    assert article["category"] != "risk_off_geopolitical_or_financial"


def test_oman_direct_attack_headline_remains_risk_off_context():
    article = news.classify_article(
        {
            "source_id": "oman_foreign_ministry_news_direct_v1",
            "source_name": "Oman Foreign Ministry",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_foreign_policy_and_hormuz_release",
            "source_currencies": [],
            "title": "Oman condemns drone attacks on Saudi oil facilities",
            "summary": "Oman condemned the new attacks and called for restraint.",
            "url": "https://www.fm.gov.om/en/drone-attacks",
            "published_utc": "2026-08-18T18:45:28Z",
        },
        first_seen=dt.datetime(2026, 8, 18, 18, 46, tzinfo=UTC),
    )
    assert article["risk_off_score"] > 0.0
    assert article["category"] == "risk_off_geopolitical_or_financial"


@pytest.mark.parametrize(
    "headline",
    [
        "Oil Slips Ahead of US Announcement of New Sanctions on Iran",
        (
            "Scott Bessent Launches Operation Economic Outcasts Sanctions "
            "Targeting Third-Party Countries Supporting Iran"
        ),
        "Bessent to detail 'economic D-Day' sanctions push against Iran",
    ],
)
def test_fresh_sanctions_grammar_remains_research_risk_off(headline):
    article = news.classify_article(
        {
            "source_id": "secondary-risk",
            "source_name": "Secondary risk publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": headline,
            "summary": "",
            "url": "https://example.com/fresh-sanctions",
            "published_utc": "2026-08-24T17:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 24, 17, 1, tzinfo=UTC),
    )
    assert article["category"] == "risk_off_geopolitical_or_financial"
    assert article["risk_off_score"] > 0.0
    assert article["directional_evidence"] is True
    # Classification is not execution authority.  Secondary discovery still
    # needs independent corroboration before it may publish direction.
    assert article["directional_publish_eligible"] is False


def test_treasury_related_buyback_footer_does_not_relabel_sanctions_release():
    article = news.classify_article(
        {
            "source_id": "us_treasury_press",
            "source_name": "U.S. Treasury",
            "source_kind": "html_links",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_policy_release",
            "source_currencies": ["USD"],
            "title": "Treasury increases sanctions on illicit finance network",
            "summary": (
                "Treasury designated ten members of an illicit-finance network. "
                + ("Current sanctions release detail. " * 150)
                + "Related releases: Treasury doubles long-end liquidity support "
                "buyback operations for 10-year to 20-year securities."
            ),
            "url": "https://home.treasury.gov/news/press-releases/sanctions",
            "published_utc": "2026-08-20T17:30:00Z",
        },
        first_seen=dt.datetime(2026, 8, 20, 17, 31, tzinfo=UTC),
    )
    assert article["category"] != "sovereign_duration_liquidity_policy"
    assert article["research_currency_scores"] != {"USD": -0.55}


def test_treasury_current_buyback_lead_remains_duration_policy():
    article = news.classify_article(
        {
            "source_id": "us_treasury_press",
            "source_name": "U.S. Treasury",
            "source_kind": "html_links",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_policy_release",
            "source_currencies": ["USD"],
            "title": "Treasury announces increased sizes of nominal long-end buybacks",
            "summary": (
                "Treasury is increasing by at least double the size of liquidity "
                "support buyback operations for longer-dated nominal securities "
                "in the 10-year to 20-year sector."
            ),
            "url": "https://home.treasury.gov/news/press-releases/buybacks",
            "published_utc": "2026-08-19T12:30:00Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 12, 31, tzinfo=UTC),
    )
    assert article["category"] == "sovereign_duration_liquidity_policy"
    assert article["research_currency_scores"] == {"USD": -0.55}
    assert article["directional_publish_eligible"] is False


def test_current_classification_contract_versions_pair_technical_recap_guard():
    assert (
        news.CLASSIFICATION_VERSION
        == (
            "local_fx_news_rules_20260828_v151_"
            "pair_breakout_recap_boundary"
        )
    )


def test_official_date_title_may_is_not_a_conditional_modal():
    assert news.policy_assertion_status(
        "5 May 2026", source_verified=True
    ) == ("asserted", 1.0)
    assert news.policy_assertion_status(
        "RBA may raise rates again", source_verified=True
    ) == ("official_conditional", 0.35)


def test_hormuz_escort_success_and_barrels_breaking_blockade_is_risk_relief():
    article = news.classify_article(
        {
            "source_id": "google_news_systemic_catalyst",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.75,
            "source_verified": False,
            "source_direct": False,
            "title": (
                "U.S. Naval Escorts Show Early Results in Strait of Hormuz as "
                "Over 660 Million Barrels of Crude Break Through Iran's Blockade"
            ),
            "summary": "Oil shipments passed through the strait under escort.",
            "url": "https://example.test/hormuz-escort-results",
            "published_utc": "2026-08-21T14:44:00Z",
        },
        first_seen=dt.datetime(2026, 8, 21, 14, 45, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0.0
    assert article["risk_on_score"] >= 0.6
    assert article["category"] == "risk_on_deescalation"
    assert article["currency_scores"]["AUD"] > 0
    assert article["currency_scores"]["JPY"] < 0
    assert article["directional_publish_eligible"] is False


def test_blockade_consequence_story_is_not_a_fresh_escalation():
    article = news.classify_article(
        {
            "source_id": "google_news_systemic_catalyst",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.75,
            "source_verified": False,
            "source_direct": False,
            "title": "Less oil for China due to US naval blockade of Iran",
            "summary": "The article discusses consequences of the existing blockade.",
            "url": "https://example.test/blockade-consequence",
            "published_utc": "2026-08-21T16:12:00Z",
        },
        first_seen=dt.datetime(2026, 8, 21, 16, 13, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0.0
    assert article["risk_on_score"] == 0.0
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_ongoing_geopolitical_tensions_market_commentary_is_context_only():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Brent holds above $80 as geopolitical tensions continue "
                "to drive inflation concerns"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/example-three",
            "published_utc": "2026-07-30T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 30, 12, 1, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0.0
    assert article["currency_scores"] == {}
    assert article["context_only"] is True


def test_context_only_article_exports_context_flag_to_ledger():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Damietta drone strike underscores precarity of Egypt's "
                "regional energy hub ambitions"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/damietta-analysis",
            "published_utc": "2026-08-04T15:59:48Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 16, 25, tzinfo=UTC),
    )

    # Corroboration/clustering is the stage that demotes an unverified fresh
    # conflict claim to context-only. The ledger must preserve that decision.
    article["context_only"] = True
    article["currency_scores"] = {}
    assert news.ledger_row(article)["context_only"] == 1


def test_named_central_bank_impulse_is_not_applied_to_other_pair_leg():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "British Pound falls against Euro despite hawkish "
                "Bank of England vote"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/example-four",
            "published_utc": "2026-07-30T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 30, 12, 1, tzinfo=UTC),
    )
    assert article["currency_scores"].get("GBP", 0) < 0
    assert article["currency_scores"].get("EUR", 0) > 0
    assert article["reports_prior_market_move"] is True


def test_generic_dollar_falls_against_yen_inverts_pair_sides():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Dollar falls against Yen following U.S.-Japan currency "
                "intervention"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/dollar-yen-move",
            "published_utc": "2026-08-03T23:03:01Z",
        },
        first_seen=dt.datetime(2026, 8, 3, 23, 41, tzinfo=UTC),
    )
    assert article["reports_prior_market_move"] is True
    assert article["currency_scores"]["USD"] < 0
    assert article["currency_scores"]["JPY"] > 0
    assert article["directional_bias"]["JPY"] == "BULLISH"
    assert article["forward_signal_timely"] is False


def test_reported_central_bank_spending_to_support_yen_is_intervention_context():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Bank of Japan spends 87 billion US dollars to support yen "
                "amid currency weakness"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/boj-spending-recap",
            "published_utc": "2026-08-04T05:45:56Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 6, 37, 10, tzinfo=UTC),
    )
    assert article["category"] == "fx_intervention"
    assert article["intervention_status"] == "reported"
    assert article["currency_scores"]["JPY"] > 0
    assert article["forward_signal_timely"] is False
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["JPY"] > 0
    assert topic["context_reason"] == "source_late_for_reaction_horizon"


def test_pair_symbol_plunge_inverts_base_and_quote_sides():
    article = news.classify_article(
        {
            "source_id": "google_news_market_ticker",
            "source_name": "FOREX.com",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Japanese Yen Outlook: USD/JPY Plunge Loses Steam, "
                "but Risks Remain"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/pair-move",
            "published_utc": "2026-08-03T23:02:42Z",
        },
        first_seen=dt.datetime(2026, 8, 3, 23, 52, tzinfo=UTC),
    )
    assert article["reports_prior_market_move"] is True
    assert article["currency_scores"]["USD"] < 0
    assert article["currency_scores"]["JPY"] > 0
    assert article["directional_bias"]["JPY"] == "BULLISH"
    assert article["forward_signal_timely"] is False


def test_pair_symbol_pressured_is_retrospective_market_move():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Action Forex",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "AUD/NZD Pressured as Inflation Backs More RBNZ Tightening, "
                "Jobs Need Only Confirm It"
            ),
            "summary": "",
            "url": "https://example.com/aud-nzd-pressured",
            "published_utc": "2026-08-04T05:18:26Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 5, 21, 22, tzinfo=UTC),
    )

    assert article["reports_prior_market_move"] is True
    assert article["currency_scores"]["AUD"] < 0
    assert article["currency_scores"]["NZD"] > 0
    assert article["forward_signal_timely"] is False
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["AUD"] < 0
    assert topic["research_currency_scores"]["NZD"] > 0
    assert topic["context_reason"] == "reported_market_move_context"


def test_named_currency_cracks_is_retrospective_market_move():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "FOREX.com",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "US Dollar Index Holds Its Uptrend While the Yen Cracks First",
            "summary": "",
            "url": "https://news.google.com/rss/articles/yen-cracks",
            "published_utc": "2026-08-04T11:34:55Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 11, 38, 25, tzinfo=UTC),
    )

    assert article["reports_prior_market_move"] is True
    assert article["currency_scores"]["JPY"] < 0
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["JPY"] < 0
    assert topic["context_reason"] == "reported_market_move_context"


def test_official_verbal_currency_stability_support_is_research_only():
    first_seen = dt.datetime(2026, 8, 4, 12, 7, 6, tzinfo=UTC)
    headlines = (
        "Treasury Sec. Bessent: A stable yen is important not only for the U.S., but for the entire region",
        "Bessent Backs Japan's Yen-Stabilization Efforts, Says Further Slide Could Weigh on Other Currencies",
    )
    for headline in headlines:
        article = news.classify_article(
            {
                "source_id": "google_news_fx_policy_ticker",
                "source_name": "CNBC",
                "source_kind": "rss",
                "source_quality": 0.55,
                "source_verified": False,
                "source_direct": False,
                "source_currencies": [],
                "title": headline,
                "summary": "",
                "url": "https://news.google.com/rss/articles/verbal-stability",
                "published_utc": "2026-08-04T11:49:11Z",
            },
            first_seen=first_seen,
        )

        assert article["currency_scores"] == {}
        assert article["research_currency_scores"]["JPY"] > 0
        assert article["research_directional_basis"] == (
            "verbal_currency_stability_support"
        )
        assert article["context_only"] is True
        assert article["execution_eligible"] is False


def test_possessive_fed_rate_policy_comment_is_neutral_usd_policy_context():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Secondary Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Fed’s Paulson keeps ‘open mind’ on rate policy outlook amid "
                "high inflation"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/fed-paulson",
            "published_utc": "2026-08-04T12:35:00Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 12, 40, tzinfo=UTC),
    )

    assert article["category"] == "monetary_policy"
    assert article["currencies"] == ["USD"]
    assert article["currency_scores"] == {}
    assert article["context_only"] is True
    assert article["execution_eligible"] is False


def test_forex_signals_equity_earnings_preview_is_not_fx_context():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "FXLeaders",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Forex Signals August 3: Palantir Technologies, SpaceX, AMD, "
                "HSBC, Oklo, MUFG Earnings Preview"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/equity-preview",
            "published_utc": "2026-08-04T09:11:15Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 9, 17, 21, tzinfo=UTC),
    )

    assert article["relevant"] is False
    assert article["context_only"] is False
    assert article["currencies"] == []
    assert article["currency_scores"] == {}
    assert article["exclusion_reason"] == "non_fx_equity_earnings_preview"


def test_sports_cost_inflation_is_not_fx_context():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "The Tomkins Times",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "The Premier League Is Now Financially Fairer: All Team Costs "
                "Analysed and Adjusted for Football Inflation"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/football-inflation",
            "published_utc": "2026-08-04T09:22:14Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 9, 24, 29, tzinfo=UTC),
    )

    assert article["relevant"] is False
    assert article["context_only"] is False
    assert article["exclusion_reason"] == "non_fx_false_term_context"


def test_corporate_profit_recap_does_not_become_commodity_fx_shock():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Secondary Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Aramco's Q2 Profit Surges 33% as Oil Prices Rise Amid "
                "Middle East Tensions"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/aramco-profit",
            "published_utc": "2026-08-04T12:36:00Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 12, 40, 40, tzinfo=UTC),
    )

    assert article["relevant"] is False
    assert article["context_only"] is False
    assert article["currency_scores"] == {}
    assert article["category"] == "market_news"
    assert article["exclusion_reason"] == "non_fx_corporate_commodity_results"


def test_fake_currency_crime_is_not_fx_context():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Local Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Businessman Alleges Fraud in Fake Currency Deal, Police Probe Network",
            "summary": "",
            "url": "https://news.google.com/rss/articles/fake-currency-fraud",
            "published_utc": "2026-08-04T09:05:28Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 9, 13, 21, tzinfo=UTC),
    )

    assert article["relevant"] is False
    assert article["context_only"] is False
    assert article["exclusion_reason"] == "non_fx_false_term_context"


def test_context_artifact_clusters_syndicated_claims_without_dropping_raw_rows():
    first_seen = dt.datetime(2026, 8, 4, 2, 0, tzinfo=UTC)
    raws = [
        {
            "source_id": "google_news_trade_policy",
            "source_name": source,
            "source_kind": "rss",
            "source_quality": 0.6,
            "source_verified": False,
            "source_currencies": [],
            "title": headline,
            "summary": "",
            "url": f"https://news.google.com/rss/articles/{index}",
            "published_utc": f"2026-08-04T02:0{index}:00Z",
        }
        for index, (source, headline) in enumerate(
            (
                ("Source A", "25 states sue Trump administration over new tariffs"),
                ("Source B", "25 US states file lawsuit challenging Trump's latest tariffs"),
                ("Source C", "Euro holds ground after regional growth data"),
            )
        )
    ]
    articles = [
        news.classify_article(raw, first_seen=first_seen)
        for raw in raws
    ]
    self_contained_raw_count = len(articles)

    published = news.cluster_context_articles(
        articles,
        as_of=dt.datetime(2026, 8, 4, 3, 0, tzinfo=UTC),
    )

    assert self_contained_raw_count == 3
    assert len(articles) == 3
    assert len(published) == 2
    tariff = next(row for row in published if row["category"] == "trade_policy")
    assert tariff["topic_article_count"] == 2
    assert tariff["semantic_context_duplicate_count"] == 1
    assert tariff["distinct_source_count"] == 2
    assert tariff["execution_eligible"] is False


def test_context_cluster_does_not_backdate_enriched_release_figures():
    template = {
        "category": "trade_balance_release",
        "context_only": True,
        "relevant": False,
        "execution_eligible": False,
        "currency_scores": {},
        "directional_bias": {},
        "topic_signature": "trade_balance_release|USD|united_states|neutral",
        "published_utc": "2026-08-04T12:30:00+00:00",
        "source_quality": 1.0,
        "source_verified": True,
        "source_direct": True,
        "direct_currencies": ["USD"],
    }
    calendar = {
        **template,
        "event_id": "calendar",
        "source_id": "census_calendar",
        "source_name": "Census calendar",
        "headline": "U.S. International Trade in Goods and Services",
        "summary": "Scheduled release",
        "first_seen_utc": "2026-08-02T20:00:00+00:00",
        "causal_known_utc": "2026-08-04T12:30:00+00:00",
    }
    enriched = {
        **template,
        "event_id": "release",
        "source_id": "bea_releases",
        "source_name": "BEA",
        "headline": "U.S. International Trade in Goods and Services, June 2026",
        "summary": "The goods and services deficit was $73.3 billion.",
        "first_seen_utc": "2026-08-04T12:31:00+00:00",
        "causal_known_utc": "2026-08-04T12:42:29+00:00",
        "detail_enriched": True,
        "detail_enrichment_kind": "bea_release_blurb",
        "detail_available_utc": "2026-08-04T12:42:29+00:00",
    }
    clustered = news.cluster_context_articles(
        [calendar, enriched],
        as_of=dt.datetime(2026, 8, 4, 12, 45, tzinfo=UTC),
    )
    assert len(clustered) == 1
    assert clustered[0]["summary"].startswith("The goods and services deficit")
    assert clustered[0]["causal_known_utc"] == "2026-08-04T12:42:29+00:00"


def test_banknote_artwork_vote_is_not_fx_context():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Beethoven or birds? Vote on the future look of euro banknotes",
            "summary": "",
            "url": "https://news.google.com/rss/articles/euro-banknote-artwork",
            "published_utc": "2026-08-04T09:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 9, 32, 16, tzinfo=UTC),
    )

    assert article["relevant"] is False
    assert article["context_only"] is False
    assert article["currencies"] == ["EUR"]
    assert article["exclusion_reason"] == "non_fx_banknote_design"


def test_reported_currency_move_is_retrospective_not_forward_direction():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Reuters",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Sterling slips as oil slides, US rate hike bets grow",
            "summary": "",
            "url": "https://news.google.com/rss/articles/sterling-move",
            "published_utc": "2026-07-28T11:07:59Z",
        },
        first_seen=dt.datetime(2026, 7, 28, 11, 8, tzinfo=UTC),
    )
    assert article["policy_assertion_status"] == "unverified_speculation"
    assert article["currency_scores"]["GBP"] < 0
    assert article["reports_prior_market_move"] is True

    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["GBP"] < 0
    assert topic["directional_publish_eligible"] is False
    assert topic["context_reason"] == "reported_market_move_context"


def test_reported_currency_adjective_is_not_reinterpreted_as_policy_direction():
    article = news.classify_article(
        {
            "source_id": "google_news_market_ticker",
            "source_name": "Reuters via publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Yen stronger as traders pare bets on Fed rate hike",
            "summary": "",
            "url": "https://news.google.com/rss/articles/yen-stronger",
            "published_utc": "2026-08-17T06:26:19Z",
        },
        first_seen=dt.datetime(2026, 8, 17, 6, 54, 19, tzinfo=UTC),
    )
    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False
    assert article["currency_scores"]["JPY"] > 0
    assert article["monetary_impulse"] < 0

    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["JPY"] > 0
    assert topic["context_reason"] == "reported_market_move_context"


def test_intervention_what_to_know_is_retrospective_context():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Secondary Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "US Intervenes to Support Japanese Yen: Here's What to Know",
            "summary": "",
            "url": "https://news.google.com/rss/articles/yen-explainer",
            "published_utc": "2026-08-03T20:22:38Z",
        },
        first_seen=dt.datetime(2026, 8, 3, 20, 24, tzinfo=UTC),
    )
    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False
    assert article["directional_publish_eligible"] is False

    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["JPY"] > 0
    assert topic["context_reason"] == "reported_market_move_context"


def test_compact_oil_drop_headline_maps_to_research_currency_impulse():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Stocks Rally as Oil Drops on Diplomatic Hopes",
            "summary": "",
            "url": "https://news.google.com/rss/articles/oil-drops",
            "published_utc": "2026-08-03T20:26:00Z",
        },
        first_seen=dt.datetime(2026, 8, 3, 20, 30, tzinfo=UTC),
    )
    assert article["category"] == "commodity_shock"
    assert article["currency_scores"]["CAD"] < 0
    assert article["currency_scores"]["NOK"] < 0
    assert article["currency_scores"]["JPY"] > 0
    assert article["reports_prior_market_move"] is True

    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["CAD"] < 0
    assert topic["context_reason"] == "reported_market_move_context"


def test_stocks_mixed_oil_prices_rise_is_market_recap_context():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "France 24",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Stocks mixed as Seoul stabilises, oil prices rise with "
                "eyes on Mideast"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/oil-rises-recap",
            "published_utc": "2026-08-04T02:57:10Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 3, 8, 35, tzinfo=UTC),
    )

    assert article["category"] == "commodity_shock"
    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["CAD"] > 0
    assert topic["context_reason"] == "reported_market_move_context"


def test_australian_shares_decline_oil_rise_is_recap_not_fx_signal():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": ["AUD"],
            "title": (
                "Australian shares decline as oil prices rise and inflation "
                "fears resurface"
            ),
            "summary": "",
            "url": "https://example.com/australian-shares-oil-recap",
            "published_utc": "2026-08-21T16:05:00Z",
        },
        first_seen=dt.datetime(2026, 8, 21, 16, 9, 30, tzinfo=UTC),
    )

    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False
    assert article["directional_corroboration_required"] is True
    assert article["directional_publish_eligible"] is False
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["context_reason"] == "reported_market_move_context"


def test_causal_clause_followed_by_oil_move_is_retrospective_context():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "CNBC",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Trump warns Iran talks are 'last chance' to end war — "
                "oil prices rise as Tehran denies negotiations"
            ),
            "summary": "",
            "url": "https://example.com/mixed-causal-oil-reaction",
            "published_utc": "2026-08-04T06:34:09Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 6, 54, 33, tzinfo=UTC),
    )

    assert article["category"] == "commodity_shock"
    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["context_reason"] == "reported_market_move_context"


def test_named_crude_extending_decline_is_retrospective_context():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "AzerNews",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Azeri Light oil falls below $97 as global crude prices "
                "extend sharp decline"
            ),
            "summary": "",
            "url": "https://example.com/oil-decline-recap",
            "published_utc": "2026-08-04T06:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 6, 3, 52, tzinfo=UTC),
    )

    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False


def test_currency_supported_is_retrospective_market_state():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Mitrade",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Forex Today: Mideast uncertainty keeps USD supported ahead "
                "of next batch of US data"
            ),
            "summary": "",
            "url": "https://example.com/usd-supported-recap",
            "published_utc": "2026-08-04T07:08:21Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 8, 2, 52, tzinfo=UTC),
    )

    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["context_reason"] == "reported_market_move_context"


def test_oil_claws_back_losses_is_retrospective_market_state():
    article = news.classify_article(
        {
            "source_id": "google_news_market_ticker",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Oil Prices Claw Back Losses as Asian Tech Markets Recover",
            "summary": "",
            "url": "https://example.com/oil-claws-back",
            "published_utc": "2026-08-04T08:05:02Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 8, 31, 13, tzinfo=UTC),
    )

    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False
    assert news.cluster_articles([article])[0]["context_reason"] == (
        "reported_market_move_context"
    )


def test_most_gulf_markets_gain_is_retrospective_market_state():
    article = news.classify_article(
        {
            "source_id": "google_news_market_ticker",
            "source_name": "Reuters",
            "source_kind": "rss",
            "source_quality": 0.95,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Most Gulf markets gain as investors weigh prospects for "
                "U.S.-Iran negotiations"
            ),
            "summary": "",
            "url": "https://example.com/gulf-markets-gain",
            "published_utc": "2026-08-04T08:25:36Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 8, 35, 10, tzinfo=UTC),
    )

    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False


def test_additional_market_recap_forms_are_retrospective_context():
    headlines = (
        "US dollar recovers as PMI beats forecasts",
        "Oil and Yields Sink as Big Tech Leads the Risk Rebound",
        "Gold, Silver Rise as Markets Await US Jobs Data",
        "Indian Rupee Strengthens to 95.34 Against US Dollar",
        "Morning Bid: Yen clings to gains but bond pressure builds",
        "Indian Bonds Trade Flat Ahead of RBI Policy Decision",
    )
    for index, headline in enumerate(headlines):
        article = news.classify_article(
            {
                "source_id": "google_news_market_ticker",
                "source_name": "Publisher",
                "source_kind": "rss",
                "source_quality": 0.55,
                "source_verified": False,
                "source_currencies": [],
                "title": headline,
                "summary": "",
                "url": f"https://example.com/market-recap-{index}",
                "published_utc": "2026-08-04T08:16:53Z",
            },
            first_seen=dt.datetime(2026, 8, 4, 8, 31, 13, tzinfo=UTC),
        )

        assert article["reports_prior_market_move"] is True, headline
        assert article["forward_signal_timely"] is False, headline
        assert news.cluster_articles([article])[0]["context_reason"] == (
            "reported_market_move_context"
        ), headline


def test_fx_pair_new_extreme_headline_is_retrospective_market_state():
    article = news.classify_article(
        {
            "source_id": "finnhub_fx_market_news",
            "source_name": "Secondary market commentary",
            "source_kind": "api",
            "source_quality": 0.70,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "USDCAD trades to a new low for the week/going back to May. "
                "What next technically?"
            ),
            "summary": "",
            "url": "https://example.com/usdcad-new-low-recap",
            "published_utc": "2026-08-21T13:33:00Z",
        },
        first_seen=dt.datetime(2026, 8, 21, 13, 33, 26, tzinfo=UTC),
    )

    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False
    assert article["directional_publish_eligible"] is False
    assert news.cluster_articles([article])[0]["context_reason"] == (
        "reported_market_move_context"
    )


def test_finnhub_pair_technical_new_lows_recap_is_never_fresh_policy_direction():
    article = news.classify_article(
        {
            "source_id": "finnhub_fx_market_news",
            "source_name": "Forexlive",
            "source_kind": "finnhub_news",
            "source_role": "aggregator_discovery",
            "source_quality": 0.70,
            "source_verified": False,
            "source_direct": False,
            "source_currencies": [],
            "title": (
                "EURUSD moves to new lows and tests a key cluster of "
                "technical levels"
            ),
            "summary": (
                "The EURUSD has moved sharply lower as the market digests a "
                "more hawkish message from Fed Chair Kevin Warsh. The "
                "probability of a Fed rate hike climbed to around 60%. That "
                "repricing pushed U.S. yields and the dollar higher."
            ),
            "url": (
                "https://investinglive.com/technical-analysis/"
                "eurusd-moves-to-new-lows-and-tests-a-key-cluster-of-technical-levels"
            ),
            "published_utc": "2026-08-28T16:23:44Z",
        },
        first_seen=dt.datetime(2026, 8, 28, 16, 31, 51, tzinfo=UTC),
    )

    assert article["classification_version"].endswith(
        "v151_pair_breakout_recap_boundary"
    )
    assert article["currencies"] == ["EUR", "USD"]
    assert article["reports_prior_market_move"] is True
    assert article["event_temporality"] == "retrospective_market_report"
    assert article["forward_signal_timely"] is False
    assert article["directional_publish_eligible"] is False
    assert article["currency_scores"]["EUR"] < 0
    assert article["currency_scores"]["USD"] > 0
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["EUR"] < 0
    assert topic["research_currency_scores"]["USD"] > 0
    assert topic["context_reason"] == "reported_market_move_context"


def test_finnhub_pair_technical_ma_breakout_is_retrospective_two_leg_move():
    article = news.classify_article(
        {
            "source_id": "finnhub_fx_market_news",
            "source_name": "Forexlive",
            "source_kind": "finnhub_news",
            "source_role": "aggregator_discovery",
            "source_quality": 0.70,
            "source_verified": False,
            "source_direct": False,
            "source_currencies": [],
            "title": "USDJPY jumps above its 100 day MA and makes a break for it.",
            "summary": "The pair has extended the move after clearing the average.",
            "url": "https://example.com/usdjpy-100-day-ma-breakout-recap",
            "published_utc": "2026-08-28T16:49:02Z",
        },
        first_seen=dt.datetime(2026, 8, 28, 17, 2, 58, tzinfo=UTC),
    )

    assert article["classification_version"].endswith(
        "v151_pair_breakout_recap_boundary"
    )
    assert article["currencies"] == ["JPY", "USD"]
    assert article["reports_prior_market_move"] is True
    assert article["event_temporality"] == "retrospective_market_report"
    assert article["forward_signal_timely"] is False
    assert article["directional_publish_eligible"] is False
    assert article["currency_scores"]["USD"] > 0
    assert article["currency_scores"]["JPY"] < 0
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["USD"] > 0
    assert topic["research_currency_scores"]["JPY"] < 0
    assert topic["context_reason"] == "reported_market_move_context"


def test_pair_recap_boundary_does_not_reclassify_genuine_official_policy_decision():
    article = news.classify_article(
        {
            "source_id": "federal_reserve_monetary_policy",
            "source_name": "Federal Reserve",
            "source_kind": "rss",
            "source_role": "primary_policy_release",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["USD"],
            "title": "Federal Reserve issues FOMC statement",
            "summary": (
                "The Committee decided to raise the target range for the "
                "federal funds rate by 25 basis points."
            ),
            "url": "https://www.federalreserve.gov/newsevents/pressreleases/example.htm",
            "published_utc": "2026-08-28T18:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 28, 18, 0, 2, tzinfo=UTC),
    )

    assert article["reports_prior_market_move"] is False
    assert article["event_temporality"] == "scheduled_or_current_release"
    assert article["category"] == "monetary_policy"
    assert article["official_policy_release"] is True
    assert article["exclusion_reason"] == ""


def test_business_activity_release_identity_precedes_policy_commentary():
    article = news.classify_article(
        {
            "source_id": "finnhub_fx_market_news",
            "source_name": "Secondary market news",
            "source_kind": "api",
            "source_quality": 0.70,
            "source_verified": False,
            "source_currencies": ["EUR"],
            "title": (
                "Euro area business activity sees further pick up in August "
                "despite France, Germany softness"
            ),
            "summary": (
                "August flash services PMI 51.7 vs 51.5 expected. The report "
                "may ease stagflation concerns for the ECB and affect monetary policy."
            ),
            "url": "https://example.com/euro-area-pmi",
            "published_utc": "2026-08-21T08:30:00Z",
        },
        first_seen=dt.datetime(2026, 8, 21, 8, 32, 12, tzinfo=UTC),
    )

    assert article["category"] == "business_activity_release"
    assert article["directional_publish_eligible"] is False


def test_oil_supply_blockade_does_not_guess_oil_exporter_risk_sign():
    article = news.classify_article(
        {
            "source_id": "google_news_systemic_catalyst",
            "source_name": "Secondary publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Iranian oil offers to Chinese buyers fall as US blockade "
                "bites, sources say"
            ),
            "summary": "",
            "url": "https://example.com/oil-offers-blockade",
            "published_utc": "2026-08-21T08:25:00Z",
        },
        first_seen=dt.datetime(2026, 8, 21, 8, 32, 36, tzinfo=UTC),
    )

    assert article["category"] == "risk_off_geopolitical_or_financial"
    assert article["directional_publish_eligible"] is False
    assert "NOK" not in article["currency_scores"]
    assert "CAD" not in article["currency_scores"]
    assert "MXN" not in article["currency_scores"]


def test_opposite_blockade_effect_claims_do_not_corroborate():
    restrictive = "Iran's oil blockade is working"
    mitigated = (
        "U.S. military transported hundreds of millions of barrels of oil "
        "despite Iran's blockade"
    )
    assert news.headlines_support_same_claim(restrictive, mitigated) is False

    common = {
        "category": "risk_off_geopolitical_or_financial",
        "topic_signature": "risk_off_geopolitical_or_financial|ALL|oil|risk_off",
        "topic_action": "risk_off",
        "topic_entities": ["oil"],
        "currency_scores": {"USD": 0.4},
        "directional_corroboration_required": True,
        "directional_publish_eligible": False,
        "directional_research_only": False,
        "reports_prior_market_move": False,
        "source_verified": False,
        "source_quality": 0.55,
        "relevant": True,
        "forward_signal_timely": True,
        "availability_lag_minutes": 1.0,
        "forward_timeliness_limit_minutes": 15.0,
        "published_utc": "2026-08-21T14:17:00Z",
        "first_seen_utc": "2026-08-21T14:18:00Z",
        "causal_known_utc": "2026-08-21T14:18:00Z",
        "last_seen_utc": "2026-08-21T14:18:00Z",
        "direct_currencies": [],
        "inferred_currencies": ["USD"],
    }
    clustered = news.cluster_articles(
        [
            {**common, "event_id": "restriction", "headline": restrictive,
             "source_name": "Publisher A", "source_id": "source_a"},
            {**common, "event_id": "relief", "headline": mitigated,
             "source_name": "Publisher B", "source_id": "source_b"},
        ],
        as_of=dt.datetime(2026, 8, 21, 14, 20, tzinfo=UTC),
    )
    assert len(clustered) == 2
    assert all(not row["directional_publish_eligible"] for row in clustered)


def test_crude_supply_and_price_surge_headline_is_retrospective_context():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Baird Maritime",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "US blockade squeezes Iranian crude supply to China as "
                "prices surge"
            ),
            "summary": "",
            "url": "https://example.com/crude-supply-prices-surge",
            "published_utc": "2026-08-21T17:27:07Z",
        },
        first_seen=dt.datetime(2026, 8, 21, 17, 44, 59, tzinfo=UTC),
    )

    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False
    assert article["directional_publish_eligible"] is False
    assert article["currency_scores"]["NOK"] > 0
    assert article["currency_scores"]["CAD"] > 0


def test_market_led_oil_recap_is_context_but_causal_announcement_is_not():
    recap = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Markets Rally as Oil Prices Fall on Diplomacy Hopes",
            "summary": "",
            "url": "https://example.com/market-recap",
            "published_utc": "2026-08-03T17:18:00Z",
        },
        first_seen=dt.datetime(2026, 8, 3, 17, 20, tzinfo=UTC),
    )
    catalyst = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Trump pauses Iran strikes and schedules new talks",
            "summary": "",
            "url": "https://example.com/causal-announcement",
            "published_utc": "2026-08-03T17:18:00Z",
        },
        first_seen=dt.datetime(2026, 8, 3, 17, 20, tzinfo=UTC),
    )

    assert recap["reports_prior_market_move"] is True
    assert catalyst["reports_prior_market_move"] is False


def test_halted_iran_strikes_are_deescalation_not_fresh_risk_off():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Trump Halts Iran Strikes as Six-Month War Enters Talks",
            "summary": "",
            "url": "https://example.com/halted-strikes",
            "published_utc": "2026-08-03T06:33:58Z",
        },
        first_seen=dt.datetime(2026, 8, 3, 6, 35, 30, tzinfo=UTC),
    )

    assert article["risk_off_score"] == 0.0
    assert article["risk_on_score"] > 0
    assert article["currency_scores"]["AUD"] > 0
    assert article["currency_scores"]["USD"] < 0


def test_cancelled_planned_military_strike_is_not_attack_escalation():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Trump Cancels Planned Military Strike on Iran as Oil Falls",
            "summary": "",
            "url": "https://example.com/cancelled-strike",
            "published_utc": "2026-08-03T12:19:24Z",
        },
        first_seen=dt.datetime(2026, 8, 3, 13, 6, 53, tzinfo=UTC),
    )

    assert article["risk_off_score"] == 0.0
    assert article["risk_on_score"] > 0
    assert article["directional_bias"]["AUD"] == "BULLISH"


def test_asian_shares_dip_after_us_stocks_rally_is_market_recap_context():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Seattle Post-Intelligencer",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Asian shares mostly dip after US stocks rally",
            "summary": "",
            "url": "https://example.com/ap-asia-market-recap",
            "published_utc": "2026-08-04T03:34:25Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 3, 41, 19, tzinfo=UTC),
    )

    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["context_reason"] == "reported_market_move_context"


def test_commodities_section_oil_drop_is_market_recap_context():
    article = news.classify_article(
        {
            "source_id": "google_news_market_ticker",
            "source_name": "Seeking Alpha",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Commodities: Oil Drops Amid Renewed Peace Deal Hopes",
            "summary": "",
            "url": "https://example.com/commodities-oil-recap",
            "published_utc": "2026-08-04T04:30:00Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 4, 33, 9, tzinfo=UTC),
    )

    assert article["reports_prior_market_move"] is True
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["CAD"] < 0
    assert topic["context_reason"] == "reported_market_move_context"


def test_ship_struck_in_hormuz_is_fresh_escalation_pending_corroboration():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_name": "Global Banking & Finance Review",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Status of US-Iran talks uncertain as ship struck in Hormuz",
            "summary": "",
            "url": "https://example.com/hormuz-ship-strike",
            "published_utc": "2026-08-04T03:43:17Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 4, 41, 45, tzinfo=UTC),
    )

    assert article["reports_prior_market_move"] is False
    assert article["risk_off_score"] > 0
    assert article["relevant"] is True
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["JPY"] > 0
    assert topic["context_reason"] == "source_late_for_reaction_horizon"


def test_compact_currency_up_headline_is_retrospective_context():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Metrobank Wealth Insights",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Peso up on yen intervention, Middle East peace deal hopes"
            ),
            "summary": "",
            "url": "https://example.com/peso-recap",
            "published_utc": "2026-08-04T00:44:54Z",
        },
        first_seen=dt.datetime(2026, 8, 4, 0, 59, tzinfo=UTC),
    )

    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False

    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["context_reason"] == "reported_market_move_context"


def test_timely_context_without_pair_direction_is_not_forward_eligible():
    article = {
        "event_id": "timely-context",
        "topic_id": "timely-context-topic",
        "topic_clustered": True,
        "headline": "Talks remain unresolved",
        "published_utc": "2026-08-04T00:00:00+00:00",
        "first_seen_utc": "2026-08-04T00:01:00+00:00",
        "source_quality": 0.55,
        "directional_confidence": 0.4,
        "currency_scores": {},
        "research_currency_scores": {"CAD": -0.5, "JPY": 0.2},
        "direct_currencies": [],
        "inferred_currencies": ["CAD", "JPY"],
        "post_window_minutes": 360,
        "estimated_reaction_horizon_minutes": 180,
        "forward_signal_timely": True,
        "reports_prior_market_move": False,
        "category": "commodity_shock",
        "scope": "all_pairs",
        "source_name": "Secondary Publisher",
        "source_verified": False,
    }
    output = news.build_pair_scores(
        [article],
        ["CAD_JPY"],
        as_of=dt.datetime(2026, 8, 4, 0, 2, tzinfo=UTC),
    )

    event = output["pairs"]["CAD_JPY"]["events"][0]
    assert event["pair_score"] == 0.0
    assert event["forward_pair_eligible"] is False


def test_initial_html_listing_back_catalog_is_retrospective_context():
    article = news.classify_article(
        {
            "source_id": "japan_mof_international_policy",
            "source_name": "Japan Ministry of Finance",
            "source_kind": "html_links",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_policy_release",
            "source_currencies": ["JPY"],
            "source_listing_bootstrap": True,
            "title": "Japan intervenes in foreign exchange market to support yen",
            "summary": "",
            "url": "https://www.mof.go.jp/english/example",
            "published_utc": "",
        },
        first_seen=dt.datetime(2026, 8, 3, 18, 30, tzinfo=UTC),
    )
    assert article["currency_scores"]["JPY"] > 0
    assert article["forward_signal_timely"] is False
    assert article["source_listing_bootstrap"] is True

    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["JPY"] > 0
    assert topic["directional_publish_eligible"] is False
    assert topic["context_reason"] == "source_listing_bootstrap_context"


def test_regional_inflation_strengthening_is_not_a_euro_currency_move():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Bloomberg",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "Euro-Area Inflation Strengthens as Oil Prices Rise",
            "summary": "",
            "url": "https://example.com/euro-area-inflation",
            "published_utc": "2026-07-31T09:03:59Z",
        },
        first_seen=dt.datetime(2026, 7, 31, 9, 4, tzinfo=UTC),
    )
    assert article["reports_prior_market_move"] is False


def test_currency_denomination_after_jump_is_not_a_usd_move():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": "India Forex Reserves Jump USD 6.12 Bn",
            "summary": "",
            "url": "https://example.com/reserves",
            "published_utc": "2026-07-31T14:10:24Z",
        },
        first_seen=dt.datetime(2026, 7, 31, 14, 11, tzinfo=UTC),
    )
    assert article["reports_prior_market_move"] is False


def test_unverified_rate_forecast_is_not_a_confirmed_policy_impulse():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Markets tip Bank of England interest rate hike in autumn"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/example-five",
            "published_utc": "2026-07-30T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 30, 12, 1, tzinfo=UTC),
    )
    assert article["policy_assertion_status"] == "unverified_speculation"
    assert article["currency_scores"] == {}
    assert article["context_only"] is True


def test_not_edging_towards_hike_is_not_hawkish():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Publisher",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "BoE Governor Bailey says central bank not 'edging' "
                "towards rate hike"
            ),
            "summary": "",
            "url": "https://news.google.com/rss/articles/example-six",
            "published_utc": "2026-07-30T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 30, 12, 1, tzinfo=UTC),
    )
    assert article["policy_assertion_status"] == "neutral_or_expected_hold"
    assert article["currency_scores"] == {}
    assert article["context_only"] is True


def test_semantic_topic_cluster_collapses_paraphrases_but_keeps_sources():
    first_seen = dt.datetime(2026, 7, 30, 12, 1, tzinfo=UTC)
    rows = [
        news.classify_article(
            {
                "source_id": source_id,
                "source_name": source_name,
                "source_kind": "rss",
                "source_quality": 0.8,
                "source_verified": False,
                "source_currencies": [],
                "title": title,
                "summary": "Missile attack raises geopolitical tensions.",
                "url": f"https://{source_id}.example/story",
                "published_utc": "2026-07-30T12:00:00Z",
            },
            first_seen=first_seen,
        )
        for source_id, source_name, title in (
            ("one", "Publisher One", "Iran conflict escalates after missile attack"),
            ("two", "Publisher Two", "Missile attack renews conflict with Iran"),
        )
    ]
    clustered = news.cluster_articles(rows)
    assert len(clustered) == 1
    assert clustered[0]["topic_article_count"] == 2
    assert clustered[0]["distinct_source_count"] == 2
    assert "#middle_east_escalation" in clustered[0]["topic_tags"]


def test_broad_geopolitical_tags_do_not_corroborate_unrelated_claims():
    first_seen = dt.datetime(2026, 7, 30, 12, 1, tzinfo=UTC)
    rows = [
        news.classify_article(
            {
                "source_id": source_id,
                "source_name": source_name,
                "source_kind": "rss",
                "source_quality": 0.55,
                "source_verified": False,
                "source_currencies": [],
                "title": title,
                "summary": "",
                "url": f"https://{source_id}.example/story",
                "published_utc": "2026-07-30T12:00:00Z",
            },
            first_seen=first_seen,
        )
        for source_id, source_name, title in (
            (
                "one",
                "Publisher One",
                "Iran missile attack hits US troops at Bahrain army base",
            ),
            (
                "two",
                "Publisher Two",
                "Iran changes military strategy as US blockade tensions escalate",
            ),
        )
    ]
    broad_signature = (
        "risk_off_geopolitical_or_financial|global|"
        "middle_east-united_states|escalation"
    )
    for row in rows:
        row.update(
            {
                "category": "risk_off_geopolitical_or_financial",
                "topic_signature": broad_signature,
                "currency_scores": {"JPY": 0.45, "AUD": -0.55},
                "directional_bias": {"JPY": "BULLISH", "AUD": "BEARISH"},
                "directional_evidence": True,
                "relevant": True,
                "context_only": False,
                "direct_currencies": [],
                "inferred_currencies": ["AUD", "JPY"],
                "currencies": ["AUD", "JPY"],
            }
        )

    assert rows[0]["topic_signature"] == rows[1]["topic_signature"]
    clustered = news.cluster_articles(rows)

    assert len(clustered) == 2
    assert all(row["directional_publish_eligible"] is False for row in clustered)
    assert all(row["currency_scores"] == {} for row in clustered)


def test_all_68_pairs_are_emitted_with_explicit_evidence_quality():
    article = {
        "event_id": "usd-policy",
        "topic_id": "usd-policy-topic",
        "topic_clustered": True,
        "topic_tags": ["#usd_hawkish_guidance"],
        "headline": "Fed signals higher rates",
        "published_utc": "2026-07-30T12:00:00+00:00",
        "first_seen_utc": "2026-07-30T12:01:00+00:00",
        "last_seen_utc": "2026-07-30T12:01:00+00:00",
        "source_quality": 1.0,
        "directional_confidence": 0.9,
        "currency_scores": {"USD": 0.8},
        "direct_currencies": ["USD"],
        "inferred_currencies": [],
        "post_window_minutes": 360,
        "scope": "currency",
        "category": "monetary_policy",
        "source_name": "Federal Reserve",
        "source_verified": True,
        "source_direct": True,
    }
    instruments = news.event_tagger.discover_instruments()
    output = news.build_pair_scores(
        [article],
        instruments,
        as_of=dt.datetime(2026, 7, 30, 12, 2, tzinfo=UTC),
    )
    assert len(instruments) == 68
    assert len(output["pairs"]) == 68
    assert output["coverage"]["all_pairs_emitted"] is True
    assert output["coverage"]["direct_directional_pair_count"] > 0
    assert output["coverage"]["global_proxy_directional_pair_count"] == 0
    assert output["pairs"]["EUR_USD"]["evidence_quality"] == "ONE_SIDED_DIRECT"
    assert output["pairs"]["EUR_USD"]["confidence_cap"] == 0.55
    assert (
        output["pairs"]["AUD_NZD"]["evidence_quality"]
        == "NO_CURRENT_EVIDENCE"
    )


def test_hkma_json_records_parser_preserves_official_date():
    rows = news.parse_json_records(
        json.dumps(
            {
                "result": {
                    "records": [
                        {
                            "title": "Adjustment of the Base Rate",
                            "link": "https://www.hkma.gov.hk/example",
                            "date": "2026-07-30",
                        }
                    ]
                }
            }
        ).encode(),
        {
            "source_id": "hkma",
            "name": "HKMA",
            "kind": "json_records",
            "records_path": "result.records",
            "fields": {"title": "title", "url": "link", "published": "date"},
            "currencies": ["HKD"],
            "verified": True,
        },
    )
    assert len(rows) == 1
    assert rows[0]["published_utc"] == "2026-07-30T00:00:00+00:00"
    assert rows[0]["source_verified"] is True


def test_bot_json_records_parser_resolves_relative_url_and_release_date():
    rows = news.parse_json_records(
        json.dumps(
            {
                "results": [
                    {
                        "listingTitle": "Monetary Policy Committee Decision",
                        "pagePath": "/content/bot/en/news-and-media/news/2026/news-20260813.html",
                        "issueDt": "13 Aug 2026",
                        "releaseNumber": "No. 42/2026",
                    }
                ]
            }
        ).encode(),
        {
            "source_id": "bot_mpc_decisions_direct_v1",
            "name": "Bank of Thailand MPC decisions",
            "kind": "json_records",
            "url": "https://www.bot.or.th/content/bot/en/news.json",
            "records_path": "results",
            "fields": {
                "title": "listingTitle",
                "url": "pagePath",
                "published": "issueDt",
                "summary": "releaseNumber",
            },
            "currencies": ["THB"],
            "verified": True,
            "direct": True,
        },
    )
    assert len(rows) == 1
    assert rows[0]["published_utc"] == "2026-08-13T00:00:00+00:00"
    assert rows[0]["url"] == (
        "https://www.bot.or.th/content/bot/en/news-and-media/news/2026/"
        "news-20260813.html"
    )


def test_json_records_parser_applies_title_allowlist_and_language():
    rows = news.parse_json_records(
        json.dumps(
            [
                {
                    "id": 1,
                    "date_gmt": "2026-07-08T12:49:28",
                    "link": "https://nbp.pl/rpp-08-07-2026/",
                    "title": {
                        "rendered": (
                            "Komunikat prasowy z posiedzenia Rady Polityki "
                            "Pieniężnej"
                        )
                    },
                },
                {
                    "id": 2,
                    "date_gmt": "2026-07-08T12:00:00",
                    "link": "https://nbp.pl/commemorative-coin/",
                    "title": {"rendered": "New commemorative coin"},
                },
            ]
        ).encode(),
        {
            "source_id": "nbp_mpc_releases_api_direct_v1",
            "name": "NBP MPC",
            "kind": "json_records",
            "url": "https://nbp.pl/wp-json/wp/v2/posts",
            "records_path": ".",
            "fields": {
                "title": "title.rendered",
                "url": "link",
                "published": "date_gmt",
                "id": "id",
            },
            "include_title_patterns": ["Rady Polityki Pieni"],
            "language": "pl",
            "currencies": ["PLN"],
            "verified": True,
            "direct": True,
        },
    )
    assert len(rows) == 1
    assert rows[0]["external_id"] == "1"
    assert rows[0]["language"] == "pl"


def test_fetch_source_bootstraps_same_site_session_before_json_api(monkeypatch):
    payload = json.dumps(
        {
            "response": {
                "docs": [
                    {
                        "document_title_string_s": "MAS Monetary Policy Statement",
                        "page_url_s": "/news/monetary-policy-statements/example",
                        "mas_date_tdt": "2026-07-27T08:00:00Z",
                        "itemid_s": "mas-1",
                    }
                ]
            }
        }
    ).encode()

    class Response:
        def __init__(self, data, url):
            self.data = data
            self.url = url
            self.status = 200
            self.headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, limit=-1):
            return self.data if limit < 0 else self.data[:limit]

        def geturl(self):
            return self.url

    class Opener:
        def __init__(self):
            self.urls = []

        def open(self, request, timeout=None):
            self.urls.append(request.full_url)
            if len(self.urls) == 1:
                return Response(b"<html>session bootstrap</html>", request.full_url)
            return Response(payload, request.full_url)

    opener = Opener()
    monkeypatch.setattr(news.urllib.request, "build_opener", lambda *args: opener)
    monkeypatch.setattr(
        news.urllib.request,
        "urlopen",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("bootstrap source must use its cookie-aware opener")
        ),
    )
    rows, state = news.fetch_source(
        {
            "source_id": "mas_monetary_policy_api_direct_v1",
            "name": "MAS",
            "kind": "json_records",
            "url": "https://www.mas.gov.sg/api/v1/search",
            "bootstrap_url": "https://www.mas.gov.sg/news",
            "records_path": "response.docs",
            "fields": {
                "title": "document_title_string_s",
                "url": "page_url_s",
                "published": "mas_date_tdt",
                "id": "itemid_s",
            },
            "trusted_domains": ["mas.gov.sg"],
            "currencies": ["SGD"],
            "verified": True,
            "direct": True,
        },
        {},
        timeout_sec=1.0,
        maximum_bytes=10_000,
        now=dt.datetime(2026, 8, 13, 12, 0, tzinfo=UTC),
    )
    assert len(opener.urls) == 2
    assert opener.urls[0] == "https://www.mas.gov.sg/news"
    assert rows[0]["url"] == (
        "https://www.mas.gov.sg/news/monetary-policy-statements/example"
    )
    assert state["last_status"] == 200


def test_google_publisher_suffix_does_not_create_a_currency_mention():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "Investing.com South Africa",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "Euro yields firm as Middle East war stokes inflation fears "
                "- Investing.com South Africa"
            ),
            "summary": (
                "Euro yields firm as Middle East war stokes inflation fears "
                "Investing.com South Africa"
            ),
            "url": "https://news.google.com/rss/articles/example",
            "published_utc": "2026-07-30T12:00:00Z",
        },
        first_seen=dt.datetime(2026, 7, 30, 12, 1, tzinfo=UTC),
    )
    assert "EUR" in article["direct_currencies"]
    assert "ZAR" not in article["direct_currencies"]


def test_official_search_proxy_rejects_untrusted_publishers():
    payload = b"""<?xml version="1.0"?>
    <rss><channel>
      <item>
        <title>Policy rate decision</title>
        <link>https://news.google.com/rss/articles/example</link>
        <source url="https://not-the-central-bank.example">Other Publisher</source>
        <pubDate>Thu, 30 Jul 2026 12:00:00 GMT</pubDate>
      </item>
    </channel></rss>"""
    rows = news.parse_rss(
        payload,
        {
            "source_id": "official_proxy",
            "name": "Official proxy",
            "kind": "rss",
            "url": "https://news.google.com/rss/search?q=example",
            "publisher_trusted_domains": ["central-bank.example"],
            "require_trusted_publisher": True,
            "currencies": ["XYZ"],
        },
    )
    assert rows == []


def test_topic_history_reconciliation_removes_superseded_recent_identity(tmp_path):
    connection = news.open_database(tmp_path / "topics.sqlite")
    try:
        topic = {
            "topic_id": "old-topic",
            "topic_signature": "old",
            "first_seen_utc": "2026-07-30T12:00:00+00:00",
            "last_seen_utc": "2026-07-30T12:01:00+00:00",
            "published_utc": "2026-07-30T12:00:00+00:00",
            "category": "market_news",
            "direct_currencies": ["EUR"],
            "topic_tags": ["#old"],
        }
        news.upsert_topic_events(connection, [topic])
        removed = news.reconcile_topic_history(
            connection,
            since=dt.datetime(2026, 7, 29, tzinfo=UTC),
            current_topic_ids=["new-topic"],
        )
        assert removed == 1
        assert connection.execute("SELECT COUNT(*) FROM topic_events").fetchone()[0] == 0
    finally:
        connection.close()


def test_topic_history_ignores_poll_only_freshness_changes(tmp_path):
    connection = news.open_database(tmp_path / "topics.sqlite")
    try:
        topic = {
            "topic_id": "stable-topic",
            "topic_signature": "stable",
            "first_seen_utc": "2026-08-03T12:00:00+00:00",
            "last_seen_utc": "2026-08-03T12:01:00+00:00",
            "published_utc": "2026-08-03T11:59:00+00:00",
            "category": "fx_intervention",
            "direct_currencies": ["JPY"],
            "topic_tags": ["#fx_intervention"],
            "topic_article_count": 1,
            "distinct_source_count": 1,
            "duplicate_observation_count": 1,
        }
        assert news.upsert_topic_events(connection, [topic]) == (1, 0)
        refreshed = {
            **topic,
            "last_seen_utc": "2026-08-03T12:02:00+00:00",
            "duplicate_observation_count": 2,
        }
        assert news.upsert_topic_events(connection, [refreshed]) == (0, 0)
        material = {
            **refreshed,
            "topic_article_count": 2,
            "headline_variants": ["new corroborating headline"],
        }
        assert news.upsert_topic_events(connection, [material]) == (0, 1)
    finally:
        connection.close()


def test_topic_history_uses_one_stable_representative_per_topic(tmp_path):
    connection = news.open_database(tmp_path / "topics.sqlite")
    try:
        base = {
            "topic_id": "oil-down",
            "topic_signature": "oil-down",
            "first_seen_utc": "2026-08-03T12:00:00+00:00",
            "last_seen_utc": "2026-08-03T12:01:00+00:00",
            "published_utc": "2026-08-03T11:59:00+00:00",
            "category": "commodity_shock",
            "direct_currencies": [],
            "topic_tags": ["#oil_down"],
            "distinct_source_count": 1,
        }
        single = {**base, "event_id": "single", "topic_article_count": 1}
        corroborated = {
            **base,
            "event_id": "corroborated",
            "topic_article_count": 4,
            "distinct_source_count": 3,
        }
        assert news.upsert_topic_events(
            connection,
            [single, corroborated, single],
        ) == (1, 0)
        stored = json.loads(
            connection.execute(
                "SELECT payload_json FROM topic_events WHERE topic_id = ?",
                ("oil-down",),
            ).fetchone()[0]
        )
        assert stored["event_id"] == "corroborated"
        assert news.upsert_topic_events(
            connection,
            [single, corroborated, single],
        ) == (0, 0)
    finally:
        connection.close()


def _secondary_macro_article(
    source_id: str,
    *,
    verified: bool = False,
) -> dict:
    return {
        "event_id": f"event-{source_id}",
        "source_id": source_id,
        "source_name": source_id,
        "source_quality": 0.8,
        "source_verified": verified,
        "source_direct": verified,
        "published_utc": "2026-07-30T12:00:00+00:00",
        "first_seen_utc": "2026-07-30T12:01:00+00:00",
        "last_seen_utc": "2026-07-30T12:02:00+00:00",
        "headline": "Australia inflation release slows sharply",
        "category": "inflation_release",
        "topic_signature": "inflation_release|AUD|australia|negative",
        "currency_scores": {"AUD": -0.5},
        "directional_bias": {"AUD": "BEARISH"},
        "directional_confidence": 0.8,
        "directional_evidence": True,
        "relevant": True,
        "context_only": False,
        "direct_currencies": ["AUD"],
        "inferred_currencies": [],
        "currencies": ["AUD"],
    }


def test_single_secondary_macro_topic_is_context_only():
    clustered = news.cluster_articles(
        [_secondary_macro_article("secondary-one")]
    )
    assert clustered[0]["currency_scores"] == {}
    assert clustered[0]["research_currency_scores"] == {"AUD": -0.5}
    assert clustered[0]["directional_publish_eligible"] is False
    assert clustered[0]["context_reason"] == (
        "secondary_uncorroborated_macro_context"
    )


def test_two_distinct_secondary_macro_sources_are_directional():
    second = _secondary_macro_article("secondary-two")
    second["headline"] = (
        "Australia inflation report shows price growth slowing sharply"
    )
    clustered = news.cluster_articles(
        [_secondary_macro_article("secondary-one"), second]
    )
    assert clustered[0]["distinct_source_count"] == 2
    assert clustered[0]["currency_scores"] == {"AUD": -0.5}
    assert clustered[0]["directional_publish_eligible"] is True
    assert clustered[0]["directional_source_grade"] == (
        "corroborated_secondary"
    )


def test_verified_macro_source_is_directional_without_corroboration():
    clustered = news.cluster_articles(
        [_secondary_macro_article("official", verified=True)]
    )
    assert clustered[0]["currency_scores"] == {"AUD": -0.5}
    assert clustered[0]["directional_publish_eligible"] is True
    assert clustered[0]["directional_source_grade"] == (
        "verified_primary_or_publisher"
    )


def test_single_secondary_commodity_claim_requires_corroboration():
    article = _secondary_macro_article("secondary-oil")
    article.update(
        {
            "headline": "Oil falls after an unconfirmed pause in strikes",
            "category": "commodity_shock",
            "topic_signature": "commodity_shock|CAD|oil|oil_down",
            "currency_scores": {"CAD": -0.5},
            "directional_bias": {"CAD": "BEARISH"},
            "currencies": ["CAD"],
            "direct_currencies": [],
            "inferred_currencies": ["CAD"],
        }
    )
    clustered = news.cluster_articles([article])
    assert clustered[0]["currency_scores"] == {}
    assert clustered[0]["research_currency_scores"] == {"CAD": -0.5}
    assert clustered[0]["directional_publish_eligible"] is False
    assert clustered[0]["context_reason"] == (
        "secondary_uncorroborated_directional_context"
    )


def _intervention_article(
    title,
    *,
    source_id="secondary-one",
    verified=False,
    published="2026-07-30T13:45:00+00:00",
):
    return news.classify_article(
        {
            "source_id": source_id,
            "source_name": source_id,
            "source_kind": "rss",
            "source_quality": 0.85,
            "source_verified": verified,
            "source_direct": verified,
            "source_currencies": ["JPY"] if verified else [],
            "title": title,
            "summary": "",
            "url": f"https://example.com/{source_id}",
            "published_utc": published,
        },
        first_seen=dt.datetime(2026, 7, 30, 13, 49, tzinfo=UTC),
    )


def test_yen_intervention_speculation_is_directional_research_context():
    article = _intervention_article(
        "Yen surges in sharp move, stoking intervention speculation"
    )
    assert article["category"] == "fx_intervention"
    assert article["intervention_status"] == "suspected"
    assert article["reports_prior_market_move"] is True
    assert article["currency_scores"]["JPY"] > 0
    assert article["directional_bias"]["JPY"] == "BULLISH"
    assert article["directional_publish_eligible"] is False
    assert "#jpy_suspected_strengthening" in article["topic_tags"]
    ledger = news.ledger_row(article)
    assert ledger["intervention_status"] == "suspected"
    assert ledger["reports_prior_market_move"] == 1
    assert "#jpy_suspected_strengthening" in ledger["topic_tags"]


def test_official_yen_buying_intervention_is_confirmed():
    article = _intervention_article(
        "Japan confirms it conducted yen-buying intervention",
        source_id="japan-mof",
        verified=True,
    )
    assert article["category"] == "fx_intervention"
    assert article["intervention_status"] == "official_confirmed"
    assert article["currency_scores"]["JPY"] > 0
    assert article["directional_publish_eligible"] is True
    assert "#jpy_official_confirmed_strengthening" in article["topic_tags"]
    pair_scores = news.build_pair_scores(
        [article],
        ["USD_JPY"],
        as_of=dt.datetime(2026, 7, 30, 13, 50, tzinfo=UTC),
    )
    pair = pair_scores["pairs"]["USD_JPY"]
    assert pair["direction"] == "SHORT"
    assert pair["currency_exposure_groups"] == ["JPY:LONG"]
    assert pair["currency_basket_policy"]["suggested_leg_risk_fraction"] == 0.333333
    assert pair["execution_eligible"] is False


def test_negated_intervention_claim_has_no_research_direction():
    article = _intervention_article(
        "Japan may not have intervened in FX market on Monday despite yen's surge"
    )
    assert article["category"] == "fx_intervention"
    assert article["intervention_status"] == "negated_or_denied"
    assert article["currency_scores"] == {}
    assert article["directional_bias"] == {}
    assert article["directional_publish_eligible"] is False
    assert article["context_only"] is True
    assert "#jpy_negated_or_denied_mixed" in article["topic_tags"]


def test_two_secondary_intervention_reports_become_corroborated_topic():
    first = _intervention_article(
        "Yen surges in suspected intervention",
        source_id="secondary-one",
    )
    second = _intervention_article(
        "Suspected intervention sends yen surging",
        source_id="secondary-two",
        published="2026-07-30T13:46:00+00:00",
    )
    clustered = news.cluster_articles([first, second])
    assert len(clustered) == 1
    assert clustered[0]["intervention_status"] == "corroborated"
    assert clustered[0]["directional_publish_eligible"] is False
    assert clustered[0]["research_currency_scores"]["JPY"] > 0
    assert clustered[0]["context_reason"] == "reported_market_move_context"
    assert "#jpy_corroborated_strengthening" in clustered[0]["topic_tags"]


def test_intervention_to_stem_yen_fall_is_yen_bullish():
    article = _intervention_article(
        "Japan conducts currency intervention to stem yen's fall vs. dollar"
    )
    assert article["intervention_status"] == "reported"
    assert article["currency_scores"]["JPY"] > 0
    assert article["directional_bias"]["JPY"] == "BULLISH"


def test_joint_currency_action_to_stem_yen_slide_is_intervention_support():
    article = _intervention_article(
        "Japan announces joint currency action with US to stem yen's 40-year slide"
    )
    assert article["category"] == "fx_intervention"
    assert article["intervention_status"] == "reported"
    assert article["currency_scores"]["JPY"] > 0
    assert article["directional_bias"]["JPY"] == "BULLISH"


def test_currency_actions_countering_disorderly_yen_moves_are_intervention_context():
    article = _intervention_article(
        'U.S.-Japan currency actions "countered disorderly yen movements": Bessent'
    )
    assert article["category"] == "fx_intervention"
    assert article["intervention_status"] == "reported"
    assert article["currency_scores"] == {}


def test_unrelated_stock_surge_does_not_leak_into_yen_direction():
    article = _intervention_article(
        "Asia stocks surge, yen awaits Ueda after intervention boost"
    )
    assert article["category"] == "fx_intervention"
    assert article["currency_scores"] == {}


def test_yen_weakens_after_intervention_surge_uses_current_clause():
    article = _intervention_article(
        "Yen weakens after intervention-led surge ahead of BOJ decision"
    )
    assert article["currency_scores"]["JPY"] < 0
    assert article["directional_bias"]["JPY"] == "BEARISH"


def test_yen_firms_after_intervention_is_retrospective_not_risk_on_bearish():
    article = _intervention_article(
        "Oil slides on peace hopes and yen firms after intervention"
    )
    assert article["reports_prior_market_move"] is True
    assert article["currency_scores"]["JPY"] > 0
    assert article["directional_bias"]["JPY"] == "BULLISH"
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["JPY"] > 0
    assert topic["context_reason"] == "reported_market_move_context"


def test_forward_intervention_support_intent_is_yen_bullish_and_timely():
    article = _intervention_article(
        "US Treasury Secretary vows further intervention to support Japanese yen"
    )
    assert article["reports_prior_market_move"] is False
    assert article["currency_scores"]["JPY"] > 0
    assert article["forward_signal_timely"] is True
    assert article["availability_lag_minutes"] == 4.0


def test_multi_topic_market_roundup_cannot_become_fresh_intervention():
    article = news.classify_article(
        {
            "source_id": "finnhub_fx_market_news",
            "source_name": "Finnhub market news",
            "source_kind": "api",
            "source_quality": 0.8,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "US stocks hit records again, but oil and earnings risks are "
                "growing. What can you trade?"
            ),
            "summary": (
                "Key takeaways for traders. Softer inflation lowered "
                "expectations for another Federal Reserve rate hike. "
                "Elsewhere an old intervention episode and BOJ rate hikes "
                "may not save the Japanese yen. Oil prices rise in one of "
                "several conditional scenarios. "
            )
            * 12,
            "url": "https://example.com/multi-topic-roundup",
            "published_utc": "2026-08-14T12:19:28Z",
        },
        first_seen=dt.datetime(2026, 8, 14, 12, 32, 11, tzinfo=UTC),
    )
    assert article["category"] != "fx_intervention"
    assert article["intervention_status"] == ""
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False
    assert article["context_reason"] == (
        "secondary_multi_topic_market_roundup_context"
    )


def test_unverified_session_wrap_cannot_publish_stale_policy_direction():
    article = news.classify_article(
        {
            "source_id": "secondary-session-wrap",
            "source_name": "Secondary session wrap",
            "source_kind": "rss",
            "source_quality": 0.75,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "European session wrap: Dollar falls, gold rebounds amid "
                "mixed markets"
            ),
            "summary": (
                "The session included old Bank of Japan rate-hike wagers, "
                "Federal Reserve commentary, oil moves, earnings and several "
                "conditional trading scenarios. "
            )
            * 14,
            "url": "https://example.com/european-session-wrap",
            "published_utc": "2026-08-14T11:53:35Z",
        },
        first_seen=dt.datetime(2026, 8, 14, 12, 2, 13, tzinfo=UTC),
    )
    assert article["category"] == "market_news"
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False
    assert article["context_reason"] == (
        "secondary_multi_topic_market_roundup_context"
    )


def test_pair_price_state_with_policy_explanation_is_retrospective_context():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "VT Markets",
            "source_kind": "rss",
            "source_quality": 0.55,
            "source_verified": False,
            "source_currencies": [],
            "title": (
                "EUR/JPY steadies near 185.70 as Japan inflation lifts BoJ "
                "hike bets and ECB tightening looms"
            ),
            "summary": "",
            "url": "https://example.com/eur-jpy-price-recap",
            "published_utc": "2026-08-24T17:14:49Z",
        },
        first_seen=dt.datetime(2026, 8, 24, 17, 19, 52, tzinfo=UTC),
    )
    assert article["reports_prior_market_move"] is True
    assert article["forward_signal_timely"] is False
    assert article["directional_publish_eligible"] is False
    assert article["event_temporality"] == "retrospective_market_report"


def test_soft_ppi_reducing_rate_hike_case_is_usd_dovish():
    article = news.classify_article(
        {
            "source_id": "secondary-macro",
            "source_name": "Secondary macro publisher",
            "source_kind": "rss",
            "source_quality": 0.75,
            "source_verified": False,
            "source_currencies": ["USD"],
            "title": (
                "Wholesale prices flatten in July and inflation eases. "
                "A Fed rate hike is in doubt."
            ),
            "summary": "",
            "url": "https://example.com/soft-ppi",
            "published_utc": "2026-08-13T12:38:00Z",
        },
        first_seen=dt.datetime(2026, 8, 13, 12, 43, 9, tzinfo=UTC),
    )
    assert article["category"] == "inflation_release"
    assert article["currency_scores"]["USD"] < 0
    assert article["directional_bias"]["USD"] == "BEARISH"


def test_softer_inflation_limiting_tightening_risk_is_usd_dovish():
    article = news.classify_article(
        {
            "source_id": "secondary-macro",
            "source_name": "Secondary macro publisher",
            "source_kind": "rss",
            "source_quality": 0.75,
            "source_verified": False,
            "source_currencies": ["USD"],
            "title": (
                "BTC/USD stays under pressure as softer U.S. inflation "
                "limits Fed tightening risks"
            ),
            "summary": "",
            "url": "https://example.com/soft-inflation-tightening-risk",
            "published_utc": "2026-08-13T12:50:44Z",
        },
        first_seen=dt.datetime(2026, 8, 13, 13, 0, 5, tzinfo=UTC),
    )
    assert article["category"] == "inflation_context"
    assert article["currency_scores"]["USD"] < 0
    assert article["directional_bias"]["USD"] == "BEARISH"


def test_paring_fed_rate_hike_bets_is_usd_dovish():
    article = news.classify_article(
        {
            "source_id": "secondary-macro",
            "source_name": "Secondary macro publisher",
            "source_kind": "rss",
            "source_quality": 0.75,
            "source_verified": False,
            "source_currencies": ["USD"],
            "title": (
                "Traders Pare Bets on Fed Rate Hike This Year "
                "as Oil Prices Fall"
            ),
            "summary": "",
            "url": "https://example.com/pare-fed-rate-hike-bets",
            "published_utc": "2026-08-13T14:24:14Z",
        },
        first_seen=dt.datetime(2026, 8, 13, 14, 41, 48, tzinfo=UTC),
    )
    assert article["currency_scores"]["USD"] < 0
    assert article["directional_bias"]["USD"] == "BEARISH"


def test_falling_rbnz_inflation_expectations_are_nzd_dovish():
    article = news.classify_article(
        {
            "source_id": "rbnz_official_search",
            "source_name": "Reserve Bank of New Zealand official search",
            "source_kind": "rss",
            "source_quality": 0.8,
            "source_verified": False,
            "source_currencies": ["NZD"],
            "title": (
                "RBNZ Survey: New Zealand two-year inflation expectations "
                "cool down to 2.34% in Q3"
            ),
            "summary": "",
            "url": "https://example.com/rbnz-survey-expectations",
            "published_utc": "2026-08-13T03:05:04Z",
        },
        first_seen=dt.datetime(2026, 8, 13, 3, 12, 35, tzinfo=UTC),
    )
    assert article["category"] == "monetary_policy"
    assert article["currency_scores"]["NZD"] < 0
    assert article["directional_bias"]["NZD"] == "BEARISH"


def test_secondary_inflation_expectations_keep_semantic_sign_but_abstain():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_discovery",
            "source_name": "TradingView",
            "source_kind": "rss",
            "source_quality": 0.7,
            "source_verified": False,
            "source_currencies": ["GBP"],
            "title": (
                "UK inflation expectations rise in August after recent falls, "
                "Citi/YouGov survey shows"
            ),
            "summary": "",
            "url": "https://example.com/uk-inflation-expectations",
            "published_utc": "2026-08-25T20:35:00Z",
        },
        first_seen=dt.datetime(2026, 8, 25, 20, 43, tzinfo=UTC),
    )
    assert article["category"] == "inflation_context"
    assert article["currency_scores"] == {}
    assert article["research_currency_scores"]["GBP"] > 0
    assert article["context_only"] is True
    assert article["directional_publish_eligible"] is False
    assert article["directional_research_only"] is True
    assert article["context_reason"] == (
        "secondary_inflation_expectations_require_rate_repricing"
    )


def test_rbnz_official_search_uses_five_minute_versioned_contract():
    config = json.loads(
        (news.ROOT / "config" / "news_sources_v1.json").read_text(
            encoding="utf-8"
        )
    )
    source = next(
        row for row in config["sources"] if row["source_id"] == "rbnz_official_search"
    )
    assert source["poll_interval_sec"] == 300
    assert source["source_contract_id"] == "rbnz_official_search_v2_20260815"
    assert source["source_cohort_id"] == source["source_contract_id"]
    assert source["kind"] == "rss"


def test_regional_fed_speech_sources_are_direct_versioned_contracts():
    config = json.loads(
        (news.ROOT / "config" / "news_sources_v1.json").read_text(
            encoding="utf-8"
        )
    )
    sources = {row["source_id"]: row for row in config["sources"]}
    for source_id, domain in (
        ("cleveland_fed_speeches", "clevelandfed.org"),
        ("richmond_fed_speeches", "richmondfed.org"),
    ):
        source = sources[source_id]
        assert source["kind"] == "html_links"
        assert source["verified"] is True
        assert source["direct"] is True
        assert source["poll_interval_sec"] == 180
        assert source["source_contract_id"].endswith("_v1_20260815")
        assert source["source_cohort_id"] == source["source_contract_id"]
        assert domain in source["trusted_domains"]


def test_kansas_city_fed_news_is_a_direct_versioned_contract():
    config = json.loads(
        (news.ROOT / "config" / "news_sources_v1.json").read_text(
            encoding="utf-8"
        )
    )
    source = next(
        row
        for row in config["sources"]
        if row["source_id"] == "kansas_city_fed_news_releases"
    )
    assert source["kind"] == "html_links"
    assert source["verified"] is True
    assert source["direct"] is True
    assert source["poll_interval_sec"] == 180
    assert source["source_role"] == "primary_policy_and_event_communication"
    assert source["source_contract_id"] == (
        "kansas_city_fed_news_releases_v2_persistent_bootstrap_20260827"
    )
    assert source["source_cohort_id"] == source["source_contract_id"]
    assert "kansascityfed.org" in source["trusted_domains"]


def test_kansas_city_fed_jackson_hole_clocks_are_exact_and_research_only():
    config = json.loads(
        (news.ROOT / "config" / "news_sources_v1.json").read_text(
            encoding="utf-8"
        )
    )
    source = next(
        row
        for row in config["sources"]
        if row["source_id"] == "kansas_city_fed_jackson_hole_calendar_2026"
    )
    assert source["kind"] == "recurring_release_calendar"
    assert source["source_role"] == "primary_policy_communication_calendar"
    assert source["directional_research_only"] is True
    assert source["verified"] is True
    assert source["direct"] is True
    assert source["source_cohort_id"] == source["source_contract_id"]

    rows = news.build_recurring_release_calendar(
        source,
        now=dt.datetime(2026, 8, 27, 12, 0, tzinfo=UTC),
    )
    assert [row["scheduled_utc"] for row in rows] == [
        "2026-08-28T00:00:00+00:00",
        "2026-08-28T14:00:00+00:00",
    ]
    assert {row["event_series_id"] for row in rows} == {
        "kansas_city_fed_jackson_hole_agenda_publication",
        "federal_reserve_chair_jackson_hole_remarks",
    }
    assert all(row["directional_research_only"] is True for row in rows)
    assert all(row["actual_value"] is None for row in rows)
    assert all(row["consensus_value"] is None for row in rows)


def test_major_policy_context_archives_are_versioned_and_causally_quarantined():
    config = json.loads(
        (news.ROOT / "config" / "news_sources_v1.json").read_text(
            encoding="utf-8"
        )
    )
    sources = {row["source_id"]: row for row in config["sources"]}
    expected = {
        "fed_monetary_policy": "fed_monetary_policy_context_archive_v1_20260816",
        "ecb_press": "ecb_press_policy_and_accounts_context_archive_v2_20260827",
        "boe_news": "boe_news_policy_context_archive_v1_20260816",
        "mas_monetary_policy_api_direct_v1": (
            "mas_monetary_policy_context_archive_v1_20260816"
        ),
    }
    for source_id, contract_id in expected.items():
        source = sources[source_id]
        assert source["detail_enrichment"] == "official_document_text"
        assert source["detail_context_archive_only"] is True
        assert source["detail_context_target_count"] >= 1
        assert source["detail_context_url_patterns"]
        assert source["source_contract_id"] == contract_id
        assert source["source_cohort_id"] == contract_id


def test_ukmto_official_search_is_versioned_and_explicitly_indirect():
    config = json.loads(
        (news.ROOT / "config" / "news_sources_v1.json").read_text(
            encoding="utf-8"
        )
    )
    source = next(
        row for row in config["sources"]
        if row["source_id"] == "ukmto_official_search"
    )
    assert source["poll_interval_sec"] == 120
    assert source["verified"] is False
    assert source["direct"] is False
    assert source["retrieval_via"] == (
        "google_news_official_site_search_fallback"
    )
    assert source["bootstrap_existing_items"] is True
    assert source["source_contract_id"] == (
        "ukmto_official_search_v2_20260815"
    )
    assert source["source_cohort_id"] == source["source_contract_id"]
    assert source["require_trusted_publisher"] is True
    assert source["publisher_trusted_domains"] == ["ukmto.org"]


def test_dol_ui_claims_official_search_is_versioned_indirect_and_inert():
    config = json.loads(
        (news.ROOT / "config" / "news_sources_v1.json").read_text(
            encoding="utf-8"
        )
    )
    source = next(
        row
        for row in config["sources"]
        if row["source_id"] == "dol_eta_ui_claims_official_search_v1"
    )
    assert source["poll_interval_sec"] == 60
    assert source["currencies"] == ["USD"]
    assert source["verified"] is False
    assert source["direct"] is False
    assert source["retrieval_via"] == (
        "google_news_official_site_search_fallback"
    )
    assert source["source_role"] == "official_publisher_search_fallback"
    assert source["directional_research_only"] is True
    assert source["source_contract_id"].endswith(
        "causal_activation_20260827T0910Z"
    )
    assert source["source_cohort_id"] == source["source_contract_id"]
    assert source["require_trusted_publisher"] is True
    assert source["publisher_trusted_domains"] == ["dol.gov"]


def test_first_rss_archive_snapshot_is_bootstrap_context(monkeypatch):
    payload = (
        b'<?xml version="1.0"?><rss><channel><item>'
        b'<title>UKMTO WARNING</title>'
        b'<link>https://www.ukmto.org/example-warning</link>'
        b'<pubDate>Fri, 14 Aug 2026 08:38:56 GMT</pubDate>'
        b'</item></channel></rss>'
    )

    class Response:
        status = 200
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, _maximum):
            return payload

    monkeypatch.setattr(
        news.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: Response(),
    )
    articles, state = news.fetch_source(
        {
            "source_id": "ukmto_official_search",
            "name": "UKMTO official-publisher search",
            "kind": "rss",
            "url": "https://news.google.com/rss/search?q=site:ukmto.org",
            "currencies": [],
            "verified": False,
            "direct": False,
            "bootstrap_existing_items": True,
            "source_contract_id": "ukmto-v2",
            "source_cohort_id": "ukmto-v2",
            "trusted_domains": ["ukmto.org"],
        },
        {},
        timeout_sec=2.0,
        maximum_bytes=10_000,
        now=dt.datetime(2026, 8, 15, 20, 27, tzinfo=UTC),
    )
    assert len(articles) == 1
    assert articles[0]["source_listing_bootstrap"] is True
    assert state["bootstrap_item_count"] == 1
    assert state["bootstrap_completed_utc"].startswith("2026-08-15T20:27")


def test_regional_fed_speech_listing_links_are_bounded_to_official_pages():
    cleveland = {
        "source_id": "cleveland_fed_speeches",
        "name": "Cleveland Fed speeches",
        "url": "https://www.clevelandfed.org/collections/speeches/",
        "link_patterns": [r"/collections/speeches/(?:20\d{2}/)?sp-"],
        "exclude_title_patterns": [r"^Speeches$"],
        "trusted_domains": ["clevelandfed.org"],
        "currencies": ["USD"],
        "verified": True,
        "direct": True,
    }
    richmond = {
        "source_id": "richmond_fed_speeches",
        "name": "Richmond Fed speeches",
        "url": "https://www.richmondfed.org/press_room/speeches",
        "link_patterns": [r"/press_room/speeches/[^/]+/20\d{2}/[^/]+"],
        "exclude_title_patterns": [r"^Speeches$"],
        "trusted_domains": ["richmondfed.org"],
        "currencies": ["USD"],
        "verified": True,
        "direct": True,
    }
    cleveland_rows = news.parse_html_links(
        b'<a href="/collections/speeches/2026/sp-20260815-policy">Policy outlook</a>'
        b'<a href="https://example.com/collections/speeches/2026/sp-bad">Bad</a>',
        cleveland,
    )
    richmond_rows = news.parse_html_links(
        b'<a href="/press_room/speeches/thomas_i_barkin/2026/barkin_speech_20260813">'
        b'The Mysterious U.S. Economy</a>'
        b'<a href="/press_room/speeches/thomas_i_barkin">Speaker index</a>',
        richmond,
    )
    assert [row["title"] for row in cleveland_rows] == ["Policy outlook"]
    assert [row["title"] for row in richmond_rows] == [
        "The Mysterious U.S. Economy"
    ]
    assert all(row["source_verified"] for row in cleveland_rows + richmond_rows)


def test_no_progress_on_peace_deal_is_failed_deescalation():
    article = news.classify_article(
        {
            "source_id": "secondary-risk",
            "source_name": "Secondary risk publisher",
            "source_kind": "rss",
            "source_quality": 0.75,
            "source_verified": False,
            "source_currencies": [],
            "title": "Iran says no progress on reviving interim peace deal with US",
            "summary": "",
            "url": "https://example.com/no-peace-progress",
            "published_utc": "2026-08-13T13:02:40Z",
        },
        first_seen=dt.datetime(2026, 8, 13, 13, 3, 50, tzinfo=UTC),
    )
    assert article["category"] == "risk_off_geopolitical_or_financial"
    assert article["risk_off_score"] > 0
    assert article["risk_on_score"] == 0


def test_verified_structured_retail_release_remains_relevant_context():
    article = news.classify_article(
        {
            "source_id": "census_economic_indicators",
            "source_name": "U.S. Census Bureau Economic Indicators",
            "source_kind": "html_links",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["USD"],
            "source_role": "primary_statistical_release",
            "title": "Advance Monthly Sales for Retail and Food Services",
            "summary": (
                "U.S. retail and food services sales for July 2026 were "
                "$763.6 billion, down 0.6 percent from the previous month."
            ),
            "url": "https://www.census.gov/retail/index.html",
            "published_utc": "2026-08-14T12:30:00Z",
            "structured_event": True,
            "event_series_id": "retail_sales",
            "event_name": "Advance Monthly Sales for Retail and Food Services",
            "actual_value": -0.6,
            "previous_value": 0.2,
        },
        first_seen=dt.datetime(2026, 8, 14, 12, 32, 11, tzinfo=UTC),
    )
    assert article["category"] == "growth_release"
    assert article["relevant"] is True
    assert article["context_only"] is True
    assert article["forward_signal_timely"] is True
    assert article["currency_scores"] == {}
    assert article["context_reason"] == (
        "official_structured_numeric_release_observation"
    )


def test_late_direction_is_retained_for_research_not_published_forward():
    article = news.classify_article(
        {
            "source_id": "official-policy",
            "source_name": "Official policy source",
            "source_kind": "rss",
            "source_quality": 0.95,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["GBP"],
            "title": "Monetary policy decision: rate hike raises policy rate",
            "summary": "",
            "url": "https://example.com/late-policy-release",
            "published_utc": "2026-07-30T12:00:00+00:00",
        },
        first_seen=dt.datetime(2026, 7, 30, 12, 40, tzinfo=UTC),
    )
    assert article["forward_signal_timely"] is False
    assert article["directional_publish_eligible"] is False
    topic = news.cluster_articles([article])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["GBP"] > 0
    assert topic["context_reason"] == "source_late_for_reaction_horizon"


def test_buy_yen_sell_dollars_maps_both_currency_legs():
    article = _intervention_article(
        "Japan intervened to buy yen and sell dollars ahead of BOJ decision"
    )
    assert article["currency_scores"]["JPY"] > 0
    assert article["currency_scores"]["USD"] < 0
    assert article["directional_bias"] == {
        "JPY": "BULLISH",
        "USD": "BEARISH",
    }
    assert "#jpy_reported_strengthening" in article["topic_tags"]


def test_structured_calendar_preserves_point_in_time_surprise_without_direction():
    source = {
        "source_id": "trading_economics_calendar",
        "name": "Trading Economics structured economic calendar",
        "kind": "economic_calendar",
        "quality": 0.95,
        "verified": False,
        "direct": False,
        "source_role": "structured_macro_calendar",
    }
    payload = json.dumps(
        [
            {
                "CalendarId": "12345",
                "Date": "2026-08-02T12:30:00Z",
                "Country": "United States",
                "Event": "Non Farm Payrolls",
                "Reference": "Jul",
                "SourceURL": "https://www.bls.gov/news.release/empsit.nr0.htm",
                "Actual": "180K",
                "Previous": "150K",
                "Forecast": "160K",
                "Revised": "145K",
                "ActualValue": 180.0,
                "PreviousValue": 150.0,
                "ForecastValue": 160.0,
                "RevisedValue": 145.0,
                "LastUpdate": "2026-08-02T12:30:03Z",
                "Importance": 3,
                "Currency": "USD",
                "Unit": "Thousand",
                "Symbol": "USNFP",
            }
        ]
    ).encode()
    rows = news.parse_economic_calendar(payload, source)
    assert len(rows) == 1
    assert rows[0]["source_currencies"] == ["USD"]
    assert rows[0]["scheduled_utc"] == "2026-08-02T12:30:00+00:00"
    assert rows[0]["published_utc"] == "2026-08-02T12:30:03+00:00"

    article = news.classify_article(
        rows[0],
        first_seen=dt.datetime(2026, 8, 2, 12, 30, 8, tzinfo=UTC),
    )
    assert article["actual_value"] == 180.0
    assert article["consensus_value"] == 160.0
    assert article["surprise_raw"] == 20.0
    assert article["revision_raw"] == 5.0
    assert article["directional_surprise_interpretation"] == "pending_series_semantics"
    assert article["causal_known_utc"] == "2026-08-02T12:30:08+00:00"
    assert article["currency_scores"] == {}
    assert article["execution_eligible"] is False

    revised_row = dict(rows[0])
    revised_row["source_reported_update_utc"] = "2026-08-02T12:35:00+00:00"
    revised_row["published_utc"] = "2026-08-02T12:35:00+00:00"
    revised_row["actual"] = "181K"
    revised_row["actual_value"] = 181.0
    revised = news.classify_article(
        revised_row,
        first_seen=dt.datetime(2026, 8, 2, 12, 35, 4, tzinfo=UTC),
    )
    assert revised["event_lineage_id"] == article["event_lineage_id"]
    assert revised["event_id"] != article["event_id"]


def test_alpha_vantage_sentiment_is_retained_as_vendor_evidence_only():
    source = {
        "source_id": "alpha_vantage_fx_news_sentiment",
        "name": "Alpha Vantage FX news sentiment",
        "kind": "alpha_vantage_news",
        "quality": 0.72,
        "source_role": "aggregator_sentiment",
    }
    payload = json.dumps(
        {
            "feed": [
                {
                    "title": "Dollar steady ahead of payrolls",
                    "summary": "Markets await the release.",
                    "url": "https://example.com/story",
                    "time_published": "20260802T102500",
                    "source": "Example Wire",
                    "overall_sentiment_score": "-0.15",
                    "overall_sentiment_label": "Somewhat-Bearish",
                    "topics": [{"topic": "Economy - Monetary", "relevance_score": "0.9"}],
                    "ticker_sentiment": [
                        {
                            "ticker": "FOREX:USD",
                            "ticker_sentiment_score": "-0.2",
                            "relevance_score": "0.8",
                        }
                    ],
                }
            ]
        }
    ).encode()
    rows = news.parse_alpha_vantage_news(payload, source)
    assert len(rows) == 1
    assert rows[0]["source_currencies"] == []
    assert rows[0]["vendor_currencies"] == ["USD"]
    assert rows[0]["vendor_currency_sentiment"] == {"USD": -0.2}
    assert rows[0]["published_utc"] == "2026-08-02T10:25:00+00:00"
    article = news.classify_article(
        rows[0],
        first_seen=dt.datetime(2026, 8, 2, 10, 26, tzinfo=UTC),
    )
    assert article["vendor_sentiment_score"] == -0.15
    assert article["vendor_sentiment_label"] == "Somewhat-Bearish"
    assert article["vendor_currency_sentiment"] == {"USD": -0.2}
    assert article["vendor_currencies"] == ["USD"]
    assert article["currencies"] == []
    assert article["execution_eligible"] is False
    topic = news.cluster_articles([article])[0]
    assert topic["vendor_currency_sentiment"] == {"USD": -0.2}
    assert topic["vendor_sentiment_research_only"] is True
    assert topic["vendor_sentiment_execution_eligible"] is False


def test_alpha_vendor_query_currency_does_not_create_local_fx_mapping():
    payload = json.dumps(
        {
            "feed": [
                {
                    "title": "Example Corp Q2 earnings call transcript",
                    "summary": "Revenue grew while margins were stable.",
                    "url": "https://example.com/equity",
                    "time_published": "20260814T002500",
                    "source": "Example Wire",
                    "ticker_sentiment": [
                        {
                            "ticker": "FOREX:TRY",
                            "ticker_sentiment_score": "0.1",
                            "relevance_score": "0.01",
                        }
                    ],
                }
            ]
        }
    ).encode()
    raw = news.parse_alpha_vantage_news(
        payload,
        {
            "source_id": "alpha_vantage_fx_news_sentiment",
            "name": "Alpha Vantage FX news sentiment",
            "quality": 0.72,
            "source_role": "aggregator_sentiment",
            "source_contract_id": "alpha_vantage_fx_ticker_rotation_20260814_v2",
            "source_cohort_id": "alpha_vantage_fx_ticker_rotation_20260814_v2",
        },
    )[0]
    article = news.classify_article(
        raw, first_seen=dt.datetime(2026, 8, 14, 0, 26, tzinfo=UTC)
    )
    assert raw["vendor_currencies"] == ["TRY"]
    assert article["vendor_currency_sentiment"] == {"TRY": 0.1}
    assert article["vendor_currencies"] == ["TRY"]
    assert article["currencies"] == []
    assert article["currency_scores"] == {}
    assert article["relevant"] is False


def test_alpha_vantage_http_200_quota_payload_is_not_healthy_empty_news():
    with pytest.raises(ValueError, match="alpha_vantage_provider_error"):
        news.parse_alpha_vantage_news(
            json.dumps({"Note": "API call frequency exceeded"}).encode(),
            {"source_id": "alpha_vantage_fx_news_sentiment"},
        )


def test_finnhub_fx_news_normalizes_vendor_fields_without_direction_vote():
    source = {
        "source_id": "finnhub_fx_market_news",
        "name": "Finnhub FX market news",
        "kind": "finnhub_news",
        "quality": 0.70,
        "source_role": "aggregator_discovery",
    }
    payload = json.dumps(
        [
            {
                "category": "forex",
                "datetime": 1785681000,
                "headline": "Dollar steady before central-bank decisions",
                "id": 987654,
                "related": "",
                "source": "Example News",
                "summary": "Investors await official releases.",
                "url": "https://example.com/fx-story?utm_source=finnhub",
            }
        ]
    ).encode()
    rows = news.parse_finnhub_news(payload, source)
    assert len(rows) == 1
    assert rows[0]["external_id"] == "987654"
    assert rows[0]["vendor_category"] == "forex"
    assert rows[0]["url"] == "https://example.com/fx-story"
    article = news.classify_article(
        rows[0],
        first_seen=dt.datetime(2026, 8, 2, 18, 31, tzinfo=UTC),
    )
    assert article["source_role"] == "aggregator_discovery"
    assert article["execution_eligible"] is False


def test_credentialed_source_is_inactive_until_key_exists(monkeypatch):
    source = {
        "enabled": True,
        "credential_env": ["TEST_NEWS_API_KEY"],
        "credential_query_param": "apikey",
        "url": "https://example.com/feed",
        "query_params": {"function": "NEWS_SENTIMENT"},
    }
    monkeypatch.delenv("TEST_NEWS_API_KEY", raising=False)
    assert news.source_runtime_status(source) == "credential_missing"
    monkeypatch.setenv("TEST_NEWS_API_KEY", "secret-value")
    assert news.source_runtime_status(source) == "enabled"
    request_url = news.source_request_url(source)
    assert "function=NEWS_SENTIMENT" in request_url
    assert "apikey=secret-value" in request_url
    assert news.redact_source_secret(request_url, source) == (
        "https://example.com/feed?function=NEWS_SENTIMENT&apikey=[REDACTED]"
    )


def test_source_request_url_resolves_relative_start_date():
    source = {
        "kind": "rss",
        "url": "https://example.test/feed",
        "query_params": {"lang": "de"},
        "relative_start_days": 2,
    }
    request_url = news.source_request_url(
        source,
        now=dt.datetime(2026, 8, 4, 6, 0, tzinfo=UTC),
    )
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(request_url).query)
    assert query["lang"] == ["de"]
    assert query["start_date"] == ["2026-08-02"]


def test_source_request_url_rotates_query_deterministically_by_utc_period():
    source = {
        "kind": "alpha_vantage_news",
        "url": "https://example.test/feed",
        "query_params": {"function": "NEWS_SENTIMENT"},
        "rotating_query_param": {
            "name": "tickers",
            "period_sec": 7200,
            "values": ["FOREX:USD", "FOREX:EUR", "FOREX:JPY"],
        },
    }
    first = news.source_request_url(
        source,
        now=dt.datetime(2026, 8, 4, 6, 0, tzinfo=UTC),
    )
    repeated = news.source_request_url(
        source,
        now=dt.datetime(2026, 8, 4, 7, 59, tzinfo=UTC),
    )
    next_period = news.source_request_url(
        source,
        now=dt.datetime(2026, 8, 4, 8, 0, tzinfo=UTC),
    )
    first_query = urllib.parse.parse_qs(urllib.parse.urlsplit(first).query)
    repeated_query = urllib.parse.parse_qs(urllib.parse.urlsplit(repeated).query)
    next_query = urllib.parse.parse_qs(urllib.parse.urlsplit(next_period).query)
    assert first_query["tickers"] == repeated_query["tickers"]
    assert first_query["tickers"] != next_query["tickers"]
    assert first_query["function"] == ["NEWS_SENTIMENT"]


def test_source_request_url_resolves_controlled_calendar_placeholders():
    request_url = news.source_request_url(
        {
            "kind": "html_links",
            "url": "https://example.test/releases/{year}/{month}/index.html",
        },
        now=dt.datetime(2026, 8, 4, 6, 0, tzinfo=UTC),
    )
    assert request_url == "https://example.test/releases/2026/08/index.html"


def test_official_http_and_https_urls_collapse_to_one_identity():
    assert news.canonical_url("http://www.boj.or.jp/en/mopo/mpmsche_minu/opinion_2026.htm") == (
        "https://www.boj.or.jp/en/mopo/mpmsche_minu/opinion_2026.htm"
    )


def test_official_html_body_extraction_ignores_navigation_and_scripts():
    body, kind = news.extract_official_document_text(
        b"<html><header>Navigation</header><body><main>" +
        b"The Committee judged that faster rate hikes may be appropriate " +
        b"as underlying inflation approaches two percent.</main>" +
        b"<script>not evidence</script></body></html>",
        content_type="text/html; charset=utf-8",
        url="https://www.boj.or.jp/release.html",
    )
    assert kind == "official_html_text"
    assert "faster rate hikes" in body
    assert "Navigation" not in body
    assert "not evidence" not in body


def test_official_context_archive_is_enriched_but_kept_bootstrap_only(monkeypatch):
    class Response:
        headers = {"Content-Type": "text/html; charset=utf-8"}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def geturl(self):
            return "https://www.bot.or.th/en/news-and-media/news/mpc/news-20260624.html"

        def read(self, _maximum):
            return (
                b"<html><main>The Monetary Policy Committee decided to keep "
                b"the policy rate unchanged while monitoring inflation and "
                b"economic growth risks.</main></html>"
            )

    monkeypatch.setattr(news.urllib.request, "urlopen", lambda *args, **kwargs: Response())
    article = {
        "title": "Press Release",
        "summary": "",
        "url": "https://www.bot.or.th/en/news-and-media/news/mpc/news-20260624.html",
        "published_utc": "",
        "source_listing_bootstrap": True,
    }
    enriched, first_seen, error = news.enrich_recent_official_release_details(
        [article],
        {
            "source_id": "bot_mpc_decisions_direct_v2",
            "detail_enrichment": "official_document_text",
            "detail_context_archive_only": True,
            "detail_context_url_patterns": ["2026"],
            "trusted_domains": ["bot.or.th"],
        },
        {},
        timeout_sec=10,
        maximum_bytes=1_000_000,
        now=dt.datetime(2026, 8, 14, 16, 0, tzinfo=UTC),
    )
    assert enriched == 1
    assert error == ""
    assert len(first_seen) == 1
    assert article["detail_context_archive_only"] is True
    assert article["detail_enrichment_research_only"] is True
    assert article["source_listing_bootstrap"] is True
    assert "policy rate unchanged" in article["summary"]
    classified = news.classify_article(
        article,
        first_seen=dt.datetime(2026, 8, 14, 16, 0, tzinfo=UTC),
    )
    assert classified["detail_context_archive_only"] is True
    assert classified["detail_enrichment_research_only"] is True
    assert classified["directional_publish_eligible"] is False


def test_existing_context_archive_reemits_quarantine_without_refetch(monkeypatch):
    monkeypatch.setattr(
        news.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: pytest.fail("archived body was refetched"),
    )
    url = "https://www.federalreserve.gov/monetary20260729a.htm"
    article = {
        "title": "Federal Reserve issues FOMC statement",
        "summary": "",
        "url": url,
        "published_utc": "2026-07-29T18:00:00Z",
        "source_listing_bootstrap": True,
    }
    enriched, first_seen, error = news.enrich_recent_official_release_details(
        [article],
        {
            "source_id": "fed_monetary_policy",
            "detail_enrichment": "official_document_text",
            "detail_context_archive_only": True,
            "detail_context_url_patterns": ["monetary2026"],
            "trusted_domains": ["federalreserve.gov"],
        },
        {
            "detail_first_seen_utc_by_url": {
                url: "2026-08-16T05:21:51+00:00"
            }
        },
        timeout_sec=10,
        maximum_bytes=1_000_000,
        now=dt.datetime(2026, 8, 16, 5, 30, tzinfo=UTC),
    )
    assert enriched == 0
    assert error == ""
    assert first_seen[url] == "2026-08-16T05:21:51+00:00"
    assert article["detail_context_archive_only"] is True
    assert article["detail_enrichment_research_only"] is True
    assert article["detail_available_utc"] == "2026-08-16T05:21:51+00:00"


def test_mnb_native_policy_context_archive_is_bounded_and_quarantined(monkeypatch):
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row["source_id"] == "mnb_policy_decisions_hu_direct_v1"
    )
    assert source["source_contract_id"] == (
        "mnb_policy_decisions_hu_direct_v2_context_archive_20260827"
    )
    assert source["source_cohort_id"] == source["source_contract_id"]
    assert source["parent_source_cohort_id"] == (
        "mnb_policy_decisions_hu_direct_v1_20260827"
    )
    assert source["detail_context_max_items_per_cycle"] == 2
    assert source["detail_context_target_count"] == 12

    class Response:
        headers = {"Content-Type": "text/html; charset=utf-8"}
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def geturl(self): return article["url"]
        def read(self, _maximum):
            return (
                b"<html><main>A Monetaris Tanacs mai ulesen az alapkamatot "
                b"25 bazisponttal, 5,50 szazalekra mersekelte. A Tanacs "
                b"tovabbra is elkoetelezett az inflacios cel fenntarthato "
                b"elerese mellett.</main></html>"
            )

    monkeypatch.setattr(news.urllib.request, "urlopen", lambda *_a, **_k: Response())
    article = {
        "source_id": source["source_id"],
        "source_name": source["name"],
        "source_kind": source["kind"],
        "source_verified": True,
        "source_direct": True,
        "source_role": "primary_policy_release",
        "source_currencies": ["HUF"],
        "directional_research_only": True,
        "title": "Kozlemeny a Monetaris Tanacs 2026. augusztus 25-i uleserol",
        "summary": "",
        "url": "https://www.mnb.hu/monetaris-politika/a-monetaris-tanacs/kozlemenyek/2026/kozlemeny-a-monetaris-tanacs-2026-augusztus-25-i-uleserol",
        "published_utc": "2026-08-25T12:00:00Z",
        "source_listing_bootstrap": True,
    }
    observed = dt.datetime(2026, 8, 27, 4, 30, tzinfo=UTC)
    enriched, state, error = news.enrich_recent_official_release_details(
        [article], source, {}, timeout_sec=10, maximum_bytes=1_000_000, now=observed
    )
    assert enriched == 1 and error == "" and len(state) == 1
    assert article["detail_context_archive_only"] is True
    assert article["detail_enrichment_research_only"] is True
    assert article["source_listing_bootstrap"] is True
    classified = news.classify_article(article, first_seen=observed)
    assert classified["forward_signal_timely"] is False
    assert classified["directional_publish_eligible"] is False


def test_mnb_future_unseen_release_uses_normal_detail_path(monkeypatch):
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row["source_id"] == "mnb_policy_decisions_hu_direct_v1"
    )
    known = "https://www.mnb.hu/monetaris-politika/a-monetaris-tanacs/kozlemenyek/2026/augusztus"
    future_url = "https://www.mnb.hu/monetaris-politika/a-monetaris-tanacs/kozlemenyek/2026/szeptember"
    article = {
        "source_id": source["source_id"], "source_name": source["name"],
        "source_kind": source["kind"], "source_verified": True,
        "source_direct": True, "source_role": "primary_policy_release",
        "source_currencies": ["HUF"], "directional_research_only": True,
        "title": "Kozlemeny a Monetaris Tanacs uj uleserol", "summary": "",
        "url": future_url, "published_utc": "2026-09-22T12:00:00Z",
    }
    news.annotate_html_listing_history(
        [article], {"known_item_urls": [known]}, listing_bootstrap=False
    )
    assert article["source_listing_new_item"] is True
    assert not article.get("source_listing_bootstrap")

    class Response:
        headers = {"Content-Type": "text/html; charset=utf-8"}
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def geturl(self): return future_url
        def read(self, _maximum):
            return (
                b"<html><main>A Monetaris Tanacs a mai ulesen dontott az "
                b"alapkamat szintjerol. A kozlemeny reszletesen ismerteti "
                b"az inflacios kilatasokat es a monetaris politika jovobeli "
                b"iranyultsagat.</main></html>"
            )

    monkeypatch.setattr(news.urllib.request, "urlopen", lambda *_a, **_k: Response())
    observed = dt.datetime(2026, 9, 22, 12, 1, tzinfo=UTC)
    enriched, _, error = news.enrich_recent_official_release_details(
        [article], source, {}, timeout_sec=10, maximum_bytes=1_000_000, now=observed
    )
    assert enriched == 1 and error == ""
    assert not article.get("detail_context_archive_only")
    assert not article.get("detail_existing_item_observed_late")
    assert not article.get("source_listing_bootstrap")


def test_news_collector_contract_rolls_after_persistent_html_bootstrap_quarantine():
    assert (
        news.COLLECTOR_CONTRACT_ID
        == "local_news_incremental_source_commit_v74_persistent_html_bootstrap_quarantine_20260827"
    )


def test_sarb_official_rss_uses_bounded_schannel_revocation_transport():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = {
        row["source_id"]: row for row in config["sources"]
    }["sarb_publications_rss_direct_v1"]
    assert source["http_transport"] == "curl"
    assert source["curl_retry_count"] == 2
    assert source["curl_ssl_revoke_best_effort"] is True
    assert source["source_contract_id"] == (
        "sarb_publications_rss_bounded_reset_retry_v7_20260825"
    )
    assert source["parent_source_cohort_id"] == (
        "sarb_publications_rss_schannel_revocation_best_effort_v6_20260825"
    )


def test_broad_google_news_sources_are_explicitly_indirect():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    sources = {row["source_id"]: row for row in config["sources"]}
    for source_id in (
        "google_news_fx_macro",
        "google_news_global_risk",
        "google_news_market_ticker",
    ):
        assert sources[source_id]["verified"] is False
        assert sources[source_id]["direct"] is False


def test_unverified_source_without_direct_flag_fails_closed_to_indirect():
    source = {"verified": False}
    assert news.configured_source_is_direct(source) is False
    assert news.source_role(source) == "news_aggregator"
    article = news.classify_article(
        {
            "source_id": "discovery",
            "source_name": "Discovery",
            "source_kind": "rss",
            "source_verified": False,
            "title": "Dollar market context",
            "summary": "",
            "url": "https://example.com/context",
            "published_utc": "2026-08-19T02:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 2, 0, tzinfo=UTC),
    )
    assert article["source_direct"] is False


def test_census_numeric_feed_has_immutable_source_identity():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row["source_id"] == "census_economic_indicators"
    )
    assert source["source_contract_id"] == (
        "census_economic_indicators_numeric_v3_statistical_fast_lane_20260827"
    )
    assert source["source_cohort_id"] == source["source_contract_id"]
    assert source["numeric_parser_activated_utc"] == "2026-08-19T02:10:00Z"
    assert source["source_role"] == "primary_statistical_release"
    assert source["directional_research_only"] is True
    assert source["research_only"] is True
    assert source["execution_eligible"] is False


def test_ecb_statistical_release_feed_fetches_bounded_first_party_detail_under_new_cohort():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row["source_id"] == "ecb_statistical_press_releases"
    )
    assert source["detail_enrichment"] == "official_document_text"
    assert source["detail_max_age_minutes"] == 180
    assert source["detail_max_items_per_cycle"] == 2
    assert source["detail_enrich_recent_existing_items"] is True
    assert source["trusted_domains"] == ["ecb.europa.eu"]
    assert source["source_contract_id"] == (
        "ecb_statistical_press_releases_detail_v2_first_party_text_20260827"
    )
    assert source["source_cohort_id"] == source["source_contract_id"]


def test_ecb_statistical_detail_enrichment_preserves_release_and_arrival_clocks(
    monkeypatch,
):
    page_url = (
        "https://www.ecb.europa.eu/press/stats/md/html/"
        "ecb.md2607~e7127e7d02.en.html"
    )
    page_payload = (
        b"<html><main><h1>Monetary developments in the euro area: July 2026</h1>"
        b"<p>Annual growth rate of broad monetary aggregate M3 stood at 3.4% "
        b"in July 2026, after 3.3% in June 2026.</p><p>Annual growth rate of "
        b"adjusted loans to households stood at 3.1% in July, compared with "
        b"3.0% in June.</p><p>Annual growth rate of adjusted loans to "
        b"non-financial corporations increased to 4.4% from 4.0%.</p>"
        b"</main></html>"
    )

    class Response:
        headers = {"Content-Type": "text/html; charset=utf-8"}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return page_url

        def read(self, _maximum):
            return page_payload

    monkeypatch.setattr(news.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())
    first_seen = dt.datetime(2026, 8, 27, 8, 8, 42, tzinfo=UTC)
    detail_available = dt.datetime(2026, 8, 27, 8, 25, tzinfo=UTC)
    contract = "ecb_statistical_press_releases_detail_v2_first_party_text_20260827"
    article = {
        "source_id": "ecb_statistical_press_releases",
        "source_name": "European Central Bank statistical press releases",
        "source_kind": "rss",
        "source_quality": 1.0,
        "source_verified": True,
        "source_direct": True,
        "source_role": "primary_central_bank_statistical_release",
        "source_contract_id": contract,
        "source_cohort_id": contract,
        "source_currencies": ["EUR"],
        "title": "Monetary developments in the euro area: July 2026",
        "summary": "",
        "url": page_url,
        "published_utc": "2026-08-27T08:00:00Z",
    }
    enriched, state, error = news.enrich_recent_official_release_details(
        [article],
        {
            "source_id": "ecb_statistical_press_releases",
            "detail_enrichment": "official_document_text",
            "detail_max_age_minutes": 180,
            "detail_max_items_per_cycle": 2,
            "trusted_domains": ["ecb.europa.eu"],
        },
        {},
        timeout_sec=10,
        maximum_bytes=1_000_000,
        now=detail_available,
    )
    assert enriched == 1 and error == ""
    assert state[page_url] == "2026-08-27T08:25:00+00:00"
    assert article["published_utc"] == "2026-08-27T08:00:00Z"
    assert article["detail_available_utc"] == "2026-08-27T08:25:00+00:00"
    assert article["detail_enrichment_kind"] == "official_html_text"
    classified = news.classify_article(article, first_seen=first_seen)
    assert classified["published_utc"] == "2026-08-27T08:00:00+00:00"
    assert classified["first_seen_utc"] == "2026-08-27T08:08:42+00:00"
    assert classified["causal_known_utc"] == "2026-08-27T08:25:00+00:00"
    assert classified["detail_enriched"] is True
    # Document availability improves context, but cannot invent a numeric
    # surprise or a directional release component without a separate,
    # versioned source-specific parser and causal consensus contract.
    assert classified["release_components"] == []
    assert classified["directional_publish_eligible"] is False


def test_abs_release_feed_fetches_bounded_first_party_detail_under_new_cohort():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row["source_id"] == "abs_latest_releases"
    )
    assert source["detail_enrichment"] == "official_document_text"
    assert source["detail_enrich_recent_existing_items"] is True
    assert source["detail_context_archive_only"] is True
    assert source["detail_context_target_count"] == 2
    assert source["detail_context_max_items_per_cycle"] == 2
    assert source["detail_context_url_patterns"] == [
        "/wage-price-index-australia/",
        "/annual-wage-growth-",
    ]
    assert source["detail_max_age_minutes"] == 180
    assert source["detail_max_items_per_cycle"] == 4
    assert source["trusted_domains"] == ["abs.gov.au"]
    assert source["source_contract_id"] == (
        "abs_latest_releases_detail_v9_official_page_clocks_20260827"
    )
    assert source["source_cohort_id"] == source["source_contract_id"]
    assert source["release_clock_contract_id"] == (
        news.ABS_OFFICIAL_PAGE_RELEASE_CLOCK_CONTRACT_ID
    )
    assert len(source["diagnostic_release_utc_by_url"]) == 4


def test_html_link_parser_carries_source_and_numeric_contracts():
    rows = news.parse_html_links(
        b'<a href="/statistics/wages">Wage Price Index</a>',
        {
            "source_id": "abs_latest_releases",
            "name": "Australian Bureau of Statistics",
            "url": "https://www.abs.gov.au/releases",
            "link_patterns": [r"/statistics/"],
            "trusted_domains": ["abs.gov.au"],
            "currencies": ["AUD"],
            "verified": True,
            "direct": True,
            "source_contract_id": "abs_contract_v3",
            "source_cohort_id": "abs_cohort_v3",
            "numeric_parser_activated_utc": "2026-08-19T02:01:00Z",
            "numeric_extraction_contract_id": "abs_numeric_v2",
        },
    )
    assert len(rows) == 1
    assert rows[0]["source_contract_id"] == "abs_contract_v3"
    assert rows[0]["source_cohort_id"] == "abs_cohort_v3"
    assert rows[0]["numeric_parser_activated_utc"] == "2026-08-19T02:01:00Z"
    assert rows[0]["numeric_extraction_contract_id"] == "abs_numeric_v2"
    assert news.OBSERVATION_TIME_CONTRACT_ID in {
        "observation_time_v4_fail_closed_consistent_integrity_sources_20260817"
    }


def test_abs_diagnostic_url_clock_is_versioned_and_never_forward():
    url = (
        "https://www.abs.gov.au/media-centre/media-releases/"
        "household-spending-rises-third-month-row"
    )
    rows = news.parse_html_links(
        (
            '<a href="/media-centre/media-releases/'
            'household-spending-rises-third-month-row">'
            "Media Release - Household spending rises for third month in a row"
            "</a>"
        ).encode(),
        {
            "source_id": "abs_latest_releases",
            "name": "Australian Bureau of Statistics",
            "url": "https://www.abs.gov.au/release-calendar/latest-releases",
            "link_patterns": [r"/media-centre/media-releases/"],
            "trusted_domains": ["abs.gov.au"],
            "currencies": ["AUD"],
            "verified": True,
            "direct": True,
            "source_role": "primary_statistical_release",
            "source_contract_id": "abs_v9",
            "source_cohort_id": "abs_v9",
            "release_clock_contract_id": (
                news.ABS_OFFICIAL_PAGE_RELEASE_CLOCK_CONTRACT_ID
            ),
            "release_clock_contract_activated_utc": "2026-08-27T05:00:00Z",
            "diagnostic_release_utc_by_url": {
                url: "2026-08-27T01:30:00Z"
            },
        },
    )
    assert len(rows) == 1
    raw = rows[0]
    assert raw["published_utc"] == "2026-08-27T01:30:00+00:00"
    assert raw["published_time_inferred"] is False
    assert raw["publication_clock_diagnostic_only"] is True
    classified = news.classify_article(
        raw,
        first_seen=dt.datetime(2026, 8, 27, 5, 1, tzinfo=UTC),
    )
    assert classified["causal_known_utc"] == "2026-08-27T05:01:00+00:00"
    assert classified["forward_signal_timely"] is False
    assert classified["directional_publish_eligible"] is False


def test_abs_clock_repair_inserts_new_version_without_rewriting_old_evidence(
    tmp_path,
):
    url = (
        "https://www.abs.gov.au/media-centre/media-releases/"
        "household-spending-rises-third-month-row"
    )
    legacy = news.classify_article(
        {
            "source_id": "abs_latest_releases",
            "source_name": "Australian Bureau of Statistics",
            "source_kind": "html_links",
            "source_role": "primary_statistical_release",
            "source_quality": 0.98,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["AUD"],
            "source_contract_id": "abs_v8",
            "source_cohort_id": "abs_v8",
            "title": "Media Release - Household spending rises for third month in a row",
            "summary": "",
            "url": url,
            "published_utc": "",
        },
        first_seen=dt.datetime(2026, 8, 27, 3, 58, 40, tzinfo=UTC),
    )
    # Reproduce the defective evidence exactly enough to prove the repair does
    # not mutate it. The new classifier itself correctly fails this row closed.
    legacy["classification_version"] = "legacy_retrieval_clock_v144"
    legacy["forward_signal_timely"] = True
    diagnostic_raw = news.parse_html_links(
        (
            '<a href="/media-centre/media-releases/'
            'household-spending-rises-third-month-row">'
            "Media Release - Household spending rises for third month in a row"
            "</a>"
        ).encode(),
        {
            "source_id": "abs_latest_releases",
            "name": "Australian Bureau of Statistics",
            "url": "https://www.abs.gov.au/release-calendar/latest-releases",
            "link_patterns": [r"/media-centre/media-releases/"],
            "trusted_domains": ["abs.gov.au"],
            "currencies": ["AUD"],
            "verified": True,
            "direct": True,
            "source_role": "primary_statistical_release",
            "source_contract_id": "abs_v9",
            "source_cohort_id": "abs_v9",
            "release_clock_contract_id": (
                news.ABS_OFFICIAL_PAGE_RELEASE_CLOCK_CONTRACT_ID
            ),
            "release_clock_contract_activated_utc": "2026-08-27T05:00:00Z",
            "diagnostic_release_utc_by_url": {
                url: "2026-08-27T01:30:00Z"
            },
        },
    )[0]
    diagnostic = news.classify_article(
        diagnostic_raw,
        first_seen=dt.datetime(2026, 8, 27, 5, 1, tzinfo=UTC),
    )

    connection = news.open_database(tmp_path / "news.sqlite")
    try:
        assert news.upsert_articles(
            connection,
            [legacy],
            dt.datetime(2026, 8, 27, 3, 58, 40, tzinfo=UTC),
        ) == (1, 0)
        assert news.upsert_articles(
            connection,
            [diagnostic],
            dt.datetime(2026, 8, 27, 5, 1, tzinfo=UTC),
        ) == (1, 0)
        current_source = {
            "source_id": "abs_latest_releases",
            "currencies": ["AUD"],
            "direct": True,
            "source_contract_id": "abs_v9",
            "source_cohort_id": "abs_v9",
            "diagnostic_release_utc_by_url": {
                url: "2026-08-27T01:30:00Z"
            },
        }
        assert news.reclassify_stored_articles(
            connection,
            sources={"abs_latest_releases": current_source},
            since=dt.datetime(2026, 8, 27, 0, 0, tzinfo=UTC),
        ) == 0
        rows = connection.execute(
            "SELECT published_utc,payload_json FROM articles ORDER BY published_utc"
        ).fetchall()
        assert len(rows) == 2
        payloads = [
            {**json.loads(row[1]), "stored_published_utc": row[0]}
            for row in rows
        ]
        old = next(
            row for row in payloads
            if row["classification_version"] == "legacy_retrieval_clock_v144"
        )
        repaired = next(
            row for row in payloads
            if row["classification_version"] == news.CLASSIFICATION_VERSION
        )
        assert old["stored_published_utc"] == "2026-08-27T03:58:40+00:00"
        assert old["forward_signal_timely"] is True
        assert old["source_contract_id"] == "abs_v8"
        assert repaired["stored_published_utc"] == "2026-08-27T01:30:00+00:00"
        assert repaired["forward_signal_timely"] is False
        assert repaired["source_contract_id"] == "abs_v9"
        current_view = news.load_context_articles(
            connection,
            since=dt.datetime(2026, 8, 27, 0, 0, tzinfo=UTC),
        )
        assert len(current_view) == 1
        assert current_view[0]["classification_version"] == news.CLASSIFICATION_VERSION
        assert current_view[0]["forward_signal_timely"] is False
    finally:
        connection.close()


def test_ons_two_hop_context_archive_is_research_only(monkeypatch):
    class Response:
        headers = {"Content-Type": "text/html; charset=utf-8"}

        def __init__(self, url, body):
            self.url, self.body = url, body

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def geturl(self):
            return self.url

        def read(self, _maximum):
            return self.body

    release_url = "https://www.ons.gov.uk/releases/gdpmonthlyestimateukjune2026"
    bulletin_url = "https://www.ons.gov.uk/economy/grossdomesticproductgdp/bulletins/gdpmonthlyestimateuk/june2026"
    responses = iter(
        [
            Response(release_url, b'<a href="/economy/grossdomesticproductgdp/bulletins/gdpmonthlyestimateuk/june2026">Bulletin</a>'),
            Response(bulletin_url, b"<html><main>Real gross domestic product increased by 0.2% in June 2026. This is an official historical bulletin retained only for current-view research.</main></html>"),
        ]
    )
    monkeypatch.setattr(news.urllib.request, "urlopen", lambda *args, **kwargs: next(responses))
    article = {
        "title": "GDP monthly estimate, UK: June 2026",
        "summary": "",
        "url": release_url,
        "published_utc": "2026-08-13T06:00:00Z",
    }
    enriched, _, error = news.enrich_recent_official_release_details(
        [article],
        {
            "source_id":"ons_published_releases",
            "detail_enrichment":"ons_release_bulletin",
            "detail_context_archive_only":True,
            "detail_context_url_patterns":["/releases/gdpmonthlyestimate"],
            "trusted_domains":["ons.gov.uk"],
        },
        {}, timeout_sec=10, maximum_bytes=1_000_000,
        now=dt.datetime(2026,8,16,19,0,tzinfo=UTC),
    )
    assert enriched == 1 and error == ""
    assert article["source_listing_bootstrap"] is True
    assert article["detail_enrichment_research_only"] is True
    assert article["detail_enrichment_kind"] == "ons_release_bulletin"


def test_japan_cpi_csv_uses_official_schedule_and_abstains_on_direction():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row.get("source_id") == "japan_cpi_national_yoy_csv"
    )
    payload = (
        "Group/Item,All items,All items less fresh food\n"
        "202606,3.3,3.4\n"
        "202607,3.1,3.2\n"
        "202608,,\n"
    ).encode("cp932")
    rows = news.parse_japan_cpi_csv(payload, source)
    assert len(rows) == 1
    assert rows[0]["actual_value"] == 3.1
    assert rows[0]["previous_value"] == 3.3
    assert rows[0]["published_utc"] == "2026-08-20T23:30:00+00:00"


def test_japan_current_cpi_summary_is_parsed_and_first_snapshot_is_bootstrap():
    source = {
        "source_id":"japan_cpi_current_summary_direct_v1",
        "name":"Statistics Bureau of Japan current national CPI summary",
        "url":"https://www.stat.go.jp/data/cpi/sokuhou/tsuki/index-z.html",
        "currencies":["JPY"],"verified":True,"direct":True,
        "source_contract_id":"japan_cpi_current_summary_v1_20260816",
        "source_cohort_id":"japan_cpi_current_summary_v1_20260816",
        "numeric_parser_activated_utc":"2026-08-16T19:12:00Z",
        "numeric_extraction_contract_id":"japan_cpi_current_summary_v1_20260816",
        "release_utc_by_reference":{"2026-06":"2026-07-23T23:30:00Z"},
    }
    payload = """<html><main><h1>2020年基準 消費者物価指数 全国 2026年（令和8年）6月分（2026年7月24日公表）</h1>
    <p>(1) 総合指数は2020年を100として113.6 前年同月比は1.7%の上昇</p></main></html>""".encode("cp932")
    rows = news.parse_japan_cpi_current_summary(payload, source)
    assert len(rows) == 1
    assert rows[0]["actual_value"] == pytest.approx(1.7)
    assert rows[0]["reference_period"] == "2026-06"
    assert rows[0]["published_utc"] == "2026-07-23T23:30:00+00:00"
    state = news.annotate_singleton_release_history(rows, {})
    assert state and rows[0]["source_listing_bootstrap"] is True
    assert rows[0]["numeric_direction_policy"] == "abstain_and_learn_response"

    article = news.classify_article(
        rows[0],
        first_seen=dt.datetime(2026, 8, 20, 23, 30, 20, tzinfo=UTC),
    )
    assert article["numeric_causal_known_utc"] == "2026-08-20T23:30:20+00:00"
    assert article["currency_scores"] == {}
    assert article["execution_eligible"] is False


def test_japan_cpi_schedule_materializes_exact_jst_release_clock():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row.get("source_id") == "japan_cpi_official_schedule"
    )
    rows = news.build_recurring_release_calendar(
        source,
        now=dt.datetime(2026, 8, 16, 7, 30, tzinfo=UTC),
    )
    july = next(
        row for row in rows
        if row["scheduled_utc"] == "2026-08-20T23:30:00+00:00"
    )
    assert july["event_series_id"] == "japan_cpi_national_yoy"
    assert july["source_currencies"] == ["JPY"]
    assert news.COLLECTOR_COHORT_ID == news.COLLECTOR_CONTRACT_ID


def test_boj_summary_body_maps_hawkish_but_remains_shadow_only():
    article = news.classify_article(
        {
            "source_id": "boj_updates",
            "source_name": "Bank of Japan",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_policy_release",
            "source_currencies": ["JPY"],
            "title": "Summary of Opinions at the Monetary Policy Meeting",
            "summary": (
                "Members said it would be appropriate to accelerate rate hikes "
                "and increase the policy rate faster than market expectations."
            ),
            "url": "https://www.boj.or.jp/en/mopo/mpmsche_minu/opinion_2026.pdf",
            "published_utc": "2026-08-09T23:50:00Z",
            "detail_enriched": True,
            "detail_enrichment_kind": "official_pdf_text",
            "detail_enrichment_research_only": True,
        },
        first_seen=dt.datetime(2026, 8, 9, 23, 50, 18, tzinfo=UTC),
    )
    assert article["official_policy_release"] is True
    assert article["policy_document_type"] == "summary_of_opinions"
    assert article["monetary_impulse"] > 0
    assert article["currency_scores"]["JPY"] > 0
    assert article["directional_publish_eligible"] is False


def test_research_only_enrichment_cannot_be_promoted_by_topic_aggregation():
    article = news.classify_article(
        {
            "source_id": "boj_updates",
            "source_name": "Bank of Japan",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_policy_release",
            "source_currencies": ["JPY"],
            "title": "Summary of Opinions at the Monetary Policy Meeting",
            "summary": "Members said faster rate hikes may be appropriate.",
            "url": "https://www.boj.or.jp/en/mopo/opinion.pdf",
            "published_utc": "2026-08-09T23:50:00Z",
            "detail_enriched": True,
            "detail_enrichment_research_only": True,
        },
        first_seen=dt.datetime(2026, 8, 10, 0, 1, tzinfo=UTC),
    )
    topic = news.cluster_articles([article])[0]
    assert topic["directional_publish_eligible"] is False
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"]["JPY"] > 0
    assert topic["context_reason"] == (
        "detail_enrichment_or_directional_research_only_context"
    )


def test_official_ceremonial_remarks_do_not_create_geopolitical_fx_signal():
    article = news.classify_article(
        {
            "source_id": "us_treasury_press",
            "source_name": "U.S. Treasury",
            "source_kind": "html_links",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_policy_release",
            "source_currencies": ["USD"],
            "title": (
                "Remarks from the Treasury Secretary at Joint Base Graham "
                "Renaming Ceremony"
            ),
            "summary": (
                "We pay tribute to military service, the Air Force, defense, "
                "and missions around the globe."
            ),
            "url": "https://home.treasury.gov/news/press-releases/example",
            "published_utc": "2026-08-10T17:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 10, 17, 1, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0.0
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False
    assert article["relevant"] is False
    assert article["exclusion_reason"] == "non_fx_ceremonial_remarks"


def test_plain_rss_poll_cannot_erase_enriched_policy_semantics(tmp_path):
    connection = news.open_database(tmp_path / "news.sqlite")
    try:
        first_seen = dt.datetime(2026, 8, 9, 23, 50, 18, tzinfo=UTC)
        raw = {
            "source_id": "boj_updates",
            "source_name": "Bank of Japan",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_policy_release",
            "source_currencies": ["JPY"],
            "title": "Summary of Opinions at the Monetary Policy Meeting",
            "url": "https://www.boj.or.jp/en/mopo/opinion.pdf",
            "published_utc": "2026-08-09T23:50:00Z",
        }
        enriched = news.classify_article(
            {
                **raw,
                "summary": (
                    "The Bank should continue to raise the policy interest rate; "
                    "the pace of policy interest rate hikes may be faster than "
                    "market expectations."
                ),
                "detail_enriched": True,
                "detail_enrichment_kind": "official_pdf_text",
                "detail_enrichment_research_only": True,
                "detail_available_utc": "2026-08-10T00:01:00Z",
                "detail_content_sha256": "abc123",
            },
            first_seen=first_seen,
        )
        plain = news.classify_article(
            raw,
            first_seen=first_seen + dt.timedelta(minutes=5),
        )
        assert enriched["monetary_impulse"] > 0
        assert plain["monetary_impulse"] == 0
        assert news.upsert_articles(connection, [enriched], first_seen) == (1, 0)
        assert news.upsert_articles(
            connection,
            [plain],
            first_seen + dt.timedelta(minutes=5),
        ) == (0, 1)
        row = connection.execute(
            "SELECT summary, monetary_impulse, currency_scores_json, payload_json "
            "FROM articles"
        ).fetchone()
        payload = json.loads(row[3])
        assert "continue to raise" in row[0]
        assert row[1] > 0
        assert json.loads(row[2])["JPY"] > 0
        assert payload["detail_enriched"] is True
        assert payload["detail_content_sha256"] == "abc123"
    finally:
        connection.close()


def test_atm_company_currency_supply_earnings_is_not_commodity_shock():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "CNBC TV18",
            "source_kind": "rss",
            "source_quality": 0.75,
            "source_verified": False,
            "source_direct": False,
            "source_currencies": [],
            "title": (
                "CMS Info Systems Q1 profit falls 11% as currency-supply "
                "disruption hits ATM volumes"
            ),
            "url": "https://example.test/cms-info-systems-q1",
            "published_utc": "2026-08-10T18:13:10Z",
        },
        first_seen=dt.datetime(2026, 8, 10, 18, 22, 35, tzinfo=UTC),
    )
    assert article["relevant"] is False
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False
    assert article["exclusion_reason"] == "non_fx_cash_handling_supply"


def test_currency_move_reversal_uses_current_leg_not_initial_rally():
    assert news.reported_currency_move_scores("Yen rally fades", ["JPY"])["JPY"] < 0
    assert news.reported_currency_move_scores("Yen gives up gains", ["JPY"])["JPY"] < 0
    assert news.reported_currency_move_scores("Yen pares losses", ["JPY"])["JPY"] > 0


def test_weak_retail_sales_reducing_hike_expectations_is_bearish_usd():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "TradingKey",
            "source_kind": "rss",
            "source_quality": 0.70,
            "source_verified": False,
            "source_direct": False,
            "source_currencies": ["USD"],
            "title": (
                "US July Retail Sales Unexpectedly Fall 0.6% as Spending Cools, "
                "Hitting Fed Rate-Hike Expectations Again"
            ),
            "url": "https://example.test/us-retail-sales",
            "published_utc": "2026-08-14T13:03:00Z",
        },
        first_seen=dt.datetime(2026, 8, 14, 13, 4, 7, tzinfo=UTC),
    )
    assert article["activity_release_direction"] == -1
    assert article["category"] == "growth_release"
    assert article["currency_scores"]["USD"] < 0
    assert article["directional_bias"]["USD"] == "BEARISH"
    assert article["monetary_impulse"] <= 0


def test_late_intervention_causal_recap_cannot_become_forward_signal():
    first_seen = dt.datetime(2026, 8, 10, 12, 31, 53, tzinfo=UTC)
    common = {
        "source_kind": "rss",
        "source_quality": 0.75,
        "source_verified": False,
        "source_direct": False,
        "source_currencies": [],
        "published_utc": "2026-08-10T12:04:55Z",
    }
    rows = [
        news.classify_article(
            {
                **common,
                "source_id": "google_news_fx_macro",
                "source_name": "Japan Wire by Kyodo News",
                "title": "BREAKING NEWS: BOJ Sept. rate hike signal led U.S. to join Japan in forex intervention",
                "url": "https://example.test/kyodo",
            },
            first_seen=first_seen,
        ),
        news.classify_article(
            {
                **common,
                "source_id": "google_news_fx_policy_ticker",
                "source_name": "Example syndicator",
                "title": "BOJ rate hike signal was deciding factor in first coordinated intervention in 28 years",
                "url": "https://example.test/syndicator",
            },
            first_seen=first_seen + dt.timedelta(seconds=2),
        ),
    ]
    assert all(row["reports_prior_market_move"] for row in rows)
    assert all(not row["directional_publish_eligible"] for row in rows)
    topics = news.cluster_articles(rows, as_of=first_seen + dt.timedelta(minutes=1))
    assert all(not topic["directional_publish_eligible"] for topic in topics)
    assert all(topic["context_only"] for topic in topics)


def test_japanese_mof_intervention_semantics_are_explicit_and_shadowable():
    article = news.classify_article(
        {
            "source_id": "japan_mof_press_conferences_ja",
            "source_name": "Japan Ministry of Finance",
            "source_kind": "html_links",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_policy_commentary",
            "source_currencies": ["JPY"],
            "directional_research_only": True,
            "title": "円買い・ドル売り介入を実施",
            "summary": "為替介入について大臣が説明した。",
            "url": "https://www.mof.go.jp/public_relations/conference/example.html",
            "published_utc": "2026-08-10T10:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 10, 10, 0, 10, tzinfo=UTC),
    )
    assert article["category"] == "fx_intervention"
    assert article["currency_scores"]["JPY"] > 0
    assert article["directional_publish_eligible"] is False


def test_persistent_policy_state_uses_latest_first_party_document():
    now = dt.datetime(2026, 8, 10, 12, tzinfo=UTC)
    base = {
        "source_verified": True,
        "official_policy_release": True,
        "policy_stance_bearing_eligible": True,
        "source_id": "boj_updates",
        "currencies": ["JPY"],
        "policy_document_type": "rate_decision",
        "source_url": "https://www.boj.or.jp/a",
    }
    state = news.build_persistent_policy_state(
        [
            {**base, "event_id": "old", "causal_known_utc": "2026-08-01T00:00:00Z", "published_utc": "2026-08-01T00:00:00Z", "headline": "Old", "summary": "", "monetary_impulse": -0.4},
            {**base, "event_id": "new", "causal_known_utc": "2026-08-09T00:00:00Z", "published_utc": "2026-08-09T00:00:00Z", "headline": "New", "summary": "", "monetary_impulse": 0.7},
        ],
        as_of=now,
    )
    assert state["currencies"]["JPY"]["event_id"] == "new"
    assert state["currencies"]["JPY"]["state"] == "HAWKISH"
    assert state["execution_eligible"] is False


def test_persistent_policy_state_does_not_replace_decision_with_future_clock():
    now = dt.datetime(2026, 8, 19, 2, tzinfo=UTC)
    base = {
        "source_verified": True,
        "official_policy_release": True,
        "policy_stance_bearing_eligible": True,
        "currencies": ["AUD"],
        "policy_document_type": "official_policy_document",
    }
    state = news.build_persistent_policy_state(
        [
            {
                **base,
                "source_id": "rba_media",
                "source_role": "primary_policy_release",
                "event_id": "completed-decision",
                "causal_known_utc": "2026-08-11T04:30:00Z",
                "published_utc": "2026-08-11T04:30:00Z",
                "headline": "RBA monetary policy decision",
                "summary": "The Board raised the cash rate.",
                "monetary_impulse": 0.7,
            },
            {
                **base,
                "source_id": "rba_policy_decision_calendar_2026",
                "source_role": "primary_policy_calendar",
                "event_id": "future-clock",
                "causal_known_utc": "2026-08-16T05:40:55Z",
                "published_utc": "2026-08-16T05:40:55Z",
                "scheduled_utc": "2026-12-08T03:30:00Z",
                "headline": "Reserve Bank of Australia Monetary Policy Decision",
                "summary": "Official published release-calendar entry.",
                "monetary_impulse": 0.0,
            },
        ],
        as_of=now,
    )
    assert state["schema_version"] == "persistent_policy_state_v3_stance_bearing_only"
    assert state["currencies"]["AUD"]["event_id"] == "completed-decision"
    assert state["currencies"]["AUD"]["state"] == "HAWKISH"
    assert state["excluded_policy_clock_count"] == 1


def test_persistent_policy_state_excludes_schedule_press_release():
    now = dt.datetime(2026, 8, 19, 2, tzinfo=UTC)
    common = {
        "source_verified": True,
        "official_policy_release": True,
        "policy_stance_bearing_eligible": True,
        "source_id": "boc_press",
        "source_role": "primary_policy_release",
        "currencies": ["CAD"],
    }
    state = news.build_persistent_policy_state(
        [
            {
                **common,
                "event_id": "decision",
                "causal_known_utc": "2026-07-15T10:00:00Z",
                "published_utc": "2026-07-15T10:00:00Z",
                "headline": "Bank of Canada interest rate decision",
                "monetary_impulse": 0.5,
            },
            {
                **common,
                "event_id": "schedule",
                "causal_known_utc": "2026-07-27T10:00:00Z",
                "published_utc": "2026-07-27T10:00:00Z",
                "headline": (
                    "Bank of Canada publishes its 2027 schedule for policy "
                    "interest rate announcements"
                ),
                "monetary_impulse": 0.0,
            },
        ],
        as_of=now,
    )
    assert state["currencies"]["CAD"]["event_id"] == "decision"
    assert state["currencies"]["CAD"]["state"] == "HAWKISH"
    assert state["excluded_policy_clock_count"] == 1


def test_persistent_policy_state_uses_authority_currency_not_referenced_currency():
    state = news.build_persistent_policy_state(
        [
            {
                "source_verified": True,
                "official_policy_release": True,
                "policy_stance_bearing_eligible": True,
                "source_id": "hkma_press_api",
                "event_id": "hkma-fed-response",
                "currencies": ["HKD", "USD"],
                "causal_known_utc": "2026-08-10T00:00:00Z",
                "published_utc": "2026-08-10T00:00:00Z",
                "headline": "HKMA response to the Fed rate decision",
                "summary": "",
                "monetary_impulse": 0.0,
            }
        ],
        as_of=dt.datetime(2026, 8, 10, 1, tzinfo=UTC),
        sources={"hkma_press_api": {"currencies": ["HKD"]}},
    )
    assert set(state["currencies"]) == {"HKD"}


def test_rba_ample_liquidity_speech_is_neutral_and_cannot_supersede_stance():
    first_seen = dt.datetime(2026, 8, 25, 4, 6, 59, tzinfo=UTC)
    article = news.classify_article(
        {
            "source_id": "rba_speeches",
            "source_name": "Reserve Bank of Australia speeches",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_policy_release",
            "source_currencies": ["AUD"],
            "title": (
                "The Road to Ample - Towards a Demand-driven Liquidity Regime"
            ),
            "summary": (
                "Speeches | RBA Speech. The Road to Ample. The transition "
                "changes the implementation framework for reserves and "
                "liquidity. A cited monetary policy report discusses rate "
                "increases. I want to stress that none of these issues bears "
                "on the stance of monetary policy itself."
            ),
            "url": "https://www.rba.gov.au/speeches/2026/sp-ag-2026-08-25.html",
            "published_utc": "2026-08-25T00:14:00Z",
        },
        first_seen=first_seen,
    )

    assert article["official_policy_release"] is True
    assert article["policy_document_type"] == (
        "liquidity_implementation_communication"
    )
    assert article["non_stance_liquidity_implementation"] is True
    assert article["policy_stance_bearing_eligible"] is False
    assert article["monetary_impulse"] == 0.0
    assert article["currency_scores"] == {}
    assert article["topic_action"] == "neutral_non_stance_liquidity"
    assert article["context_only"] is True
    assert article["directional_publish_eligible"] is False
    assert article["context_reason"] == (
        "official_liquidity_implementation_explicitly_non_stance"
    )

    prior_decision = {
        "source_verified": True,
        "official_policy_release": True,
        "policy_stance_bearing_eligible": True,
        "source_id": "rba_media",
        "source_role": "primary_policy_release",
        "currencies": ["AUD"],
        "event_id": "rba-august-decision",
        "causal_known_utc": "2026-08-11T04:30:00Z",
        "published_utc": "2026-08-11T04:30:00Z",
        "headline": "RBA monetary policy decision",
        "summary": "The Board raised the cash rate.",
        "source_url": "https://www.rba.gov.au/media-releases/2026/mr-26-19.html",
        "policy_document_type": "rate_decision",
        "monetary_impulse": 1.0,
    }
    state = news.build_persistent_policy_state(
        [prior_decision, article],
        as_of=dt.datetime(2026, 8, 25, 5, 0, tzinfo=UTC),
    )
    assert state["currencies"]["AUD"]["event_id"] == "rba-august-decision"
    assert state["excluded_non_stance_policy_count"] == 1


def test_policy_document_type_prefers_title_over_body_references():
    assert news.policy_document_type(
        "Speech by the Deputy Governor on market operations",
        (
            "The speaker referred to the latest Monetary Policy Report and "
            "minutes of the prior meeting."
        ),
    ) == "policy_communication"


def test_china_nbs_numeric_parser_extracts_allowlisted_releases() -> None:
    first_seen = news.parse_datetime("2026-08-16T14:35:00Z")
    common = {
        "source_id": "china_nbs_latest_releases_direct_v1",
        "source_verified": True,
        "source_direct": True,
        "published_utc": "2026-08-16T09:30:00Z",
        "detail_available_utc": "2026-08-16T14:34:00Z",
        "detail_enriched": True,
        "detail_enrichment_kind": "official_document_text",
        "numeric_parser_activated_utc": "2026-08-16T14:32:00Z",
    }
    cpi = news.official_numeric_release_fields(
        {
            **common,
            "title": "2.Consumer Price Index in July 2026",
            "summary": "In July 2026, China’s Consumer Price Index (CPI) increased by 0.5% year on year.",
        },
        first_seen=first_seen,
    )
    ppi = news.official_numeric_release_fields(
        {
            **common,
            "title": "Industrial Producer Price Indexes in July 2026",
            "summary": "China's producer price index for industrial products (PPI) decreased by 0.7% year on year.",
        },
        first_seen=first_seen,
    )
    gdp = news.official_numeric_release_fields(
        {
            **common,
            "title": "Preliminary Accounting Results of GDP for the Second Quarter and the First Half of 2026",
            "summary": "Table 3 Quarter-on-Quarter Growth Rate of GDP Unit: % Year | Q1 | Q2 | Q3 | Q4 2025 | 1.1 | 1.2 | 1.1 | 1.1 2026 | 1.3 | 0.9 | | Note: quarter-on-quarter.",
        },
        first_seen=first_seen,
    )
    assert cpi["event_series_id"] == "china_nbs_cpi_yoy"
    assert cpi["actual_value"] == 0.5
    assert ppi["event_series_id"] == "china_nbs_ppi_yoy"
    assert ppi["actual_value"] == -0.7
    assert gdp["event_series_id"] == "china_nbs_real_gdp_qoq"
    assert gdp["actual_value"] == 0.9
    assert cpi["numeric_causal_known_utc"] == "2026-08-16T14:35:00+00:00"


def test_china_nbs_numeric_parser_accepts_verified_official_html() -> None:
    fields = news.official_numeric_release_fields(
        {
            "source_id": "china_nbs_latest_releases_direct_v1",
            "source_verified": True,
            "source_direct": True,
            "title": "Consumer Price Index in July 2026",
            "summary": (
                "In July 2026, China’s Consumer Price Index (CPI) "
                "increased by 0.5% year on year."
            ),
            "published_utc": "2026-08-10T01:30:00Z",
            "detail_enriched": True,
            "detail_enrichment_kind": "official_html_text",
            "detail_available_utc": "2026-08-16T19:18:40Z",
            "numeric_parser_activated_utc": "2026-08-16T18:00:00Z",
        },
        first_seen=news.parse_datetime("2026-08-16T19:18:40Z"),
    )

    assert fields["structured_event"] is True
    assert fields["actual_value"] == 0.5
    assert fields["event_series_id"] == "china_nbs_cpi_yoy"


def test_poland_gus_numeric_parser_extracts_allowlisted_releases() -> None:
    first_seen = news.parse_datetime("2026-08-16T14:42:00Z")
    common = {
        "source_id": "poland_gus_economic_releases_direct_v1",
        "source_verified": True,
        "source_direct": True,
        "published_utc": "2026-08-16T08:00:00Z",
        "detail_available_utc": "2026-08-16T14:41:00Z",
        "detail_enriched": True,
        "detail_enrichment_kind": "official_html_text",
        "numeric_parser_activated_utc": "2026-08-16T14:38:33Z",
    }
    cpi = news.official_numeric_release_fields(
        {
            **common,
            "title": "Consumer price indices in July 2026",
            "summary": "Consumer prices in July 2026 increased by 3.0% compared with the corresponding month of the previous year. As related to the previous month consumer prices increased by 0.8%.",
        },
        first_seen=first_seen,
    )
    gdp = news.official_numeric_release_fields(
        {
            **common,
            "title": "Flash estimate of Gross Domestic Product in the 2nd quarter of 2026",
            "summary": "In the 2nd quarter of 2026 seasonally adjusted GDP (constant prices, reference year 2020) was higher by 0.9% than in the previous quarter and 3.7% higher than in the 2nd quarter of the previous year.",
        },
        first_seen=first_seen,
    )
    assert cpi["event_series_id"] == "poland_gus_cpi_yoy"
    assert cpi["actual_value"] == 3.0
    assert gdp["event_series_id"] == "poland_gus_real_gdp_qoq"
    assert gdp["actual_value"] == 0.9
    assert cpi["numeric_causal_known_utc"] == "2026-08-16T14:42:00+00:00"


def test_singstat_table_parser_uses_latest_observation_without_backdating() -> None:
    payload = json.dumps(
        {
            "Data": {
                "id": "M213781",
                "title": "Percent Change In Consumer Price Index",
                "dataLastUpdated": "23/07/2026",
                "row": [
                    {
                        "rowText": "All Items",
                        "uoM": "Per Cent",
                        "columns": [
                            {"key": "2026 May", "value": "1.2"},
                            {"key": "2026 Jun", "value": "1.4"},
                        ],
                    }
                ],
            }
        }
    ).encode()
    rows = news.parse_singstat_table(
        payload,
        {
            "source_id": "singstat_cpi_yoy_table_direct_v1",
            "name": "SingStat CPI",
            "url": "https://tablebuilder.singstat.gov.sg/api/table/tabledata/M213781",
            "resource_id": "M213781",
            "row_text": "All Items",
            "event_series_id": "singstat_cpi_yoy",
            "event_name": "Singapore consumer price index annual change",
            "event_country": "Singapore",
            "unit": "year_percent_change",
            "verified": True,
            "direct": True,
            "currencies": ["SGD"],
            "numeric_parser_activated_utc": "2026-08-16T14:46:00Z",
            "numeric_extraction_contract_id": "singstat_table_latest_observation_v1_20260816",
        },
    )
    assert len(rows) == 1
    assert rows[0]["actual_value"] == 1.4
    assert rows[0]["previous_value"] == 1.2
    assert rows[0]["reference_period"] == "2026 Jun"
    assert rows[0]["published_utc"] == ""
    assert rows[0]["timing_precision"] == "collector_first_seen"


def test_singstat_bootstrap_value_is_numeric_context_not_forward_evidence() -> None:
    first_seen = news.parse_datetime("2026-08-16T14:47:10Z")
    raw = {
        "source_id": "singstat_cpi_yoy_table_direct_v1",
        "source_name": "SingStat CPI",
        "source_kind": "singstat_table",
        "source_quality": 1.0,
        "source_verified": True,
        "source_direct": True,
        "source_role": "primary_statistical_release",
        "source_currencies": ["SGD"],
        "title": "Percent Change In Consumer Price Index",
        "summary": "Official SingStat All Items: 1.9 Per Cent for 2026 Jun.",
        "url": "https://tablebuilder.singstat.gov.sg/api/table/tabledata/M213781",
        "published_utc": "",
        "published_time_inferred": True,
        "external_id": "singstat:M213781:singstat_cpi_yoy:2026 Jun",
        "structured_event": True,
        "event_series_id": "singstat_cpi_yoy",
        "event_name": "Singapore consumer price index annual change",
        "event_country": "Singapore",
        "reference_period": "2026 Jun",
        "unit": "year_percent_change",
        "actual": "1.9",
        "actual_value": 1.9,
        "previous": "1.4",
        "previous_value": 1.4,
        "numeric_parser_activated_utc": "2026-08-16T14:46:00Z",
        "numeric_extraction_contract_id": "singstat_table_latest_observation_v1_20260816",
        "numeric_direction_policy": "abstain_and_learn_response",
        "source_listing_bootstrap": True,
    }
    row = news.classify_article(raw, first_seen=first_seen)
    assert row["actual_value"] == 1.9
    assert row["numeric_causal_known_utc"] == "2026-08-16T14:47:10+00:00"
    assert row["forward_signal_timely"] is False
    assert row["directional_publish_eligible"] is False
    assert row["execution_eligible"] is False


def test_singstat_frozen_verified_release_clock_changes_identity() -> None:
    payload = json.dumps(
        {
            "Data": {
                "id": "M213781",
                "title": "Singapore CPI",
                "row": [
                    {
                        "rowText": "All Items",
                        "uoM": "Per Cent",
                        "columns": [
                            {"key": "2026 May", "value": "1.8"},
                            {"key": "2026 Jun", "value": "1.9"},
                        ],
                    }
                ],
            }
        }
    ).encode()
    rows = news.parse_singstat_table(
        payload,
        {
            "source_id": "singstat_cpi_yoy_table_direct_v1",
            "resource_id": "M213781",
            "row_text": "All Items",
            "event_series_id": "singstat_cpi_yoy",
            "event_name": "Singapore CPI",
            "release_utc_by_reference": {
                "2026 Jun": "2026-07-23T05:00:00Z"
            },
            "release_clock_precision": (
                "official_date_plus_contemporaneous_exact_publication_time"
            ),
        },
    )
    assert rows[0]["published_utc"] == "2026-07-23T05:00:00+00:00"
    assert rows[0]["published_time_inferred"] is False
    assert rows[0]["external_id"].endswith("2026-07-23T05:00:00+00:00")
    assert rows[0]["timing_precision"].startswith("official_date_plus")


def test_ssb_jsonstat2_cpi_parser_selects_latest_published_rate() -> None:
    payload = json.dumps(
        {
            "version": "2.0",
            "class": "dataset",
            "label": "14700: Consumer price index, by goods and services and month",
            "updated": "2026-08-10T06:00:00Z",
            "id": ["VareTjenesteGrp", "ContentsCode", "Tid"],
            "size": [1, 1, 2],
            "dimension": {
                "VareTjenesteGrp": {
                    "category": {"index": {"00": 0}, "label": {"00": "Total"}}
                },
                "ContentsCode": {
                    "category": {
                        "index": {"Tolvmanedersendring": 0},
                        "label": {"Tolvmanedersendring": "12-month rate (per cent)"},
                    }
                },
                "Tid": {
                    "category": {
                        "index": {"2026M06": 0, "2026M07": 1},
                        "label": {"2026M06": "2026M06", "2026M07": "2026M07"},
                    }
                },
            },
            "value": [2.7, 3.0],
        }
    ).encode()
    rows = news.parse_ssb_jsonstat2_cpi_yoy(
        payload,
        {
            "source_id": "norway_ssb_cpi_yoy_table_direct_v1",
            "name": "Statistics Norway CPI",
            "url": "https://data.ssb.no/api/pxwebapi/v2/tables/14700/data",
            "table_id": "14700",
            "series_dimension_id": "ContentsCode",
            "series_code": "Tolvmanedersendring",
            "time_dimension_id": "Tid",
            "event_series_id": "norway_ssb_cpi_yoy",
            "event_name": "Norway consumer price index 12-month rate",
            "event_country": "Norway",
            "unit": "year_percent_change",
            "release_utc_by_reference": {
                "2026M07": "2026-08-10T06:00:00Z"
            },
            "verified": True,
            "direct": True,
            "currencies": ["NOK"],
        },
    )
    assert len(rows) == 1
    assert rows[0]["actual_value"] == 3.0
    assert rows[0]["previous_value"] == 2.7
    assert rows[0]["reference_period"] == "2026M07"
    assert rows[0]["source_native_update_date"] == "2026-08-10T06:00:00Z"
    assert rows[0]["published_utc"] == "2026-08-10T06:00:00+00:00"
    assert rows[0]["published_time_inferred"] is False
    assert rows[0]["timing_precision"] == "official_exact_schedule"
    assert rows[0]["external_id"].endswith(
        ":2026-08-10T06:00:00+00:00"
    )


def test_denmark_statbank_cpi_parser_handles_decimal_comma() -> None:
    payload = (
        "\ufeffVAREGR;ENHED;TID;INDHOLD\r\n"
        "000000 00 Forbrugerprisindeks i alt;300 annual change;2026M07 2026M07;1,70\r\n"
        "000000 00 Forbrugerprisindeks i alt;300 annual change;2026M06 2026M06;1,90\r\n"
    ).encode("utf-8")
    rows = news.parse_denmark_statbank_cpi_yoy(
        payload,
        {
            "source_id": "denmark_statbank_cpi_yoy_table_direct_v1",
            "name": "Statistics Denmark CPI",
            "url": "https://api.statbank.dk/v1/data/PRIS01/CSV",
            "table_id": "PRIS01",
            "period_column": "TID",
            "value_column": "INDHOLD",
            "event_series_id": "denmark_statbank_cpi_yoy",
            "event_name": "Denmark consumer price index annual change",
            "event_country": "Denmark",
            "unit": "year_percent_change",
            "release_utc_by_reference": {
                "2026M07": "2026-08-10T06:00:00Z"
            },
            "verified": True,
            "direct": True,
            "currencies": ["DKK"],
        },
    )
    assert len(rows) == 1
    assert rows[0]["actual_value"] == 1.7
    assert rows[0]["previous_value"] == 1.9
    assert rows[0]["reference_period"] == "2026M07"
    assert rows[0]["published_utc"] == "2026-08-10T06:00:00+00:00"
    assert rows[0]["published_time_inferred"] is False
    assert rows[0]["timing_precision"] == "official_exact_schedule"


def test_hong_kong_censtatd_parser_selects_composite_cpi_yoy() -> None:
    payload = json.dumps(
        {
            "header": {
                "status": {"name": "Success", "code": 0},
                "title": "Consumer Price Indices",
            },
            "dataSet": [
                {
                    "period": "202605",
                    "sv": "CC_CM_1920",
                    "svDesc": "Year-on-year % change",
                    "figure": 1.9,
                },
                {
                    "period": "202606",
                    "sv": "CC_CM_1920",
                    "svDesc": "Year-on-year % change",
                    "figure": 2.0,
                },
                {
                    "period": "202606",
                    "sv": "A_CM_1920",
                    "svDesc": "Year-on-year % change",
                    "figure": 1.8,
                },
            ],
        }
    ).encode()
    rows = news.parse_hong_kong_censtatd_cpi_yoy(
        payload,
        {
            "source_id": "hong_kong_censtatd_cpi_yoy_table_direct_v1",
            "name": "Hong Kong C&SD CPI",
            "url": "https://www.censtatd.gov.hk/api/get.php?id=510-60001",
            "table_id": "510-60001",
            "series_code": "CC_CM_1920",
            "series_description": "Year-on-year % change",
            "event_series_id": "hong_kong_censtatd_composite_cpi_yoy",
            "event_name": "Hong Kong Composite CPI year-on-year change",
            "event_country": "Hong Kong",
            "unit": "year_percent_change",
            "release_utc_by_reference": {
                "202606": "2026-07-21T08:30:00Z"
            },
            "verified": True,
            "direct": True,
            "currencies": ["HKD"],
        },
    )
    assert len(rows) == 1
    assert rows[0]["actual_value"] == 2.0
    assert rows[0]["previous_value"] == 1.9
    assert rows[0]["reference_period"] == "202606"
    assert rows[0]["published_utc"] == "2026-07-21T08:30:00+00:00"
    assert rows[0]["published_time_inferred"] is False
    assert rows[0]["timing_precision"] == "official_exact_schedule"


def test_bot_sdds_parser_selects_current_thailand_cpi_row() -> None:
    payload = b"""
    <table><tr height='29'>
      <td>Consumer price index</td>
      <td>2023=100</td>
      <td><span>&nbsp;</span>Jul/26</td>
      <td>102.1</td><td>f</td><td>102.9</td><td>f</td>
      <td><a href='https://index.tpso.go.th/cpi'>CPI</a></td>
    </tr></table>
    """
    rows = news.parse_bot_sdds_cpi_index(
        payload,
        {
            "source_id": "thailand_bot_sdds_cpi_index_direct_v1",
            "name": "Bank of Thailand SDDS CPI",
            "url": "https://www.bot.or.th/en/statistics/sdds.html",
            "table_id": "bot_sdds_current_snapshot",
            "row_label": "Consumer price index",
            "event_series_id": "thailand_headline_cpi_index",
            "event_name": "Thailand headline consumer price index",
            "event_country": "Thailand",
            "unit": "index_2023_100",
            "verified": True,
            "direct": True,
            "currencies": ["THB"],
        },
    )
    assert len(rows) == 1
    assert rows[0]["actual_value"] == 102.1
    assert rows[0]["previous_value"] == 102.9
    assert rows[0]["reference_period"] == "2026M07"
    assert rows[0]["published_utc"] == ""
    assert rows[0]["numeric_direction_policy"] == "abstain_and_learn_response"


def test_banxico_inflation_parser_selects_current_annual_cpi() -> None:
    payload = b"""
    <select id="selectMeses"><option value="6">Jun</option>
      <option value="7" selected="selected">Jul</option></select>
    <select id="selectAnios"><option value="2025">2025</option>
      <option value="2026" selected="selected">2026</option></select>
    <table><tr><td>CPI general index</td>
      <td id="tdSP30577">0.03</td>
      <td id="tdSP30579">1.49</td>
      <td id="tdSP30578">3.12</td>
    </tr></table>
    """
    rows = news.parse_banxico_inflation_snapshot(
        payload,
        {
            "source_id": "mexico_banxico_inflation_snapshot_direct_v1",
            "name": "Banco de Mexico SIE inflation",
            "url": "https://www.banxico.org.mx/tipcamb/llenarInflacionAction.do",
            "table_id": "banxico_sie_inflation_snapshot",
            "event_series_id": "mexico_headline_cpi_yoy",
            "event_name": "Mexico headline consumer price inflation",
            "event_country": "Mexico",
            "unit": "year_percent_change",
            "verified": True,
            "direct": True,
            "currencies": ["MXN"],
        },
    )
    assert len(rows) == 1
    assert rows[0]["actual_value"] == 3.12
    assert rows[0]["previous_value"] is None
    assert rows[0]["reference_period"] == "2026M07"
    assert rows[0]["source_native_components"]["headline_cpi_monthly"]["actual"] == 0.03
    assert rows[0]["numeric_direction_policy"] == "abstain_and_learn_response"


def test_sarb_homepage_rates_parser_selects_frozen_cpi_series() -> None:
    payload = json.dumps(
        [
            {
                "Name": "CPI",
                "SectionId": "HPRIR",
                "SectionName": "Inflation rates",
                "TimeseriesCode": "CPI1000F",
                "Date": "2026-06-30",
                "Value": 5.0,
                "UpDown": 1,
            },
            {
                "Name": "SARB Policy Rate",
                "TimeseriesCode": "MMRD002A",
                "Date": "2026-08-14",
                "Value": 7.0,
                "UpDown": 0,
            },
        ]
    ).encode()
    rows = news.parse_sarb_homepage_rates(
        payload,
        {
            "source_id": "south_africa_sarb_cpi_direct_v1",
            "name": "SARB headline CPI",
            "url": "https://custom.resbank.co.za/SarbWebApi/WebIndicators/HomePageRates",
            "table_id": "sarb_homepage_rates",
            "series_code": "CPI1000F",
            "event_series_id": "south_africa_headline_cpi_yoy",
            "event_name": "South Africa headline consumer price inflation",
            "event_country": "South Africa",
            "unit": "year_percent_change",
            "release_utc_by_reference": {
                "2026-06-30": "2026-07-22T08:00:00Z"
            },
            "verified": True,
            "direct": True,
            "currencies": ["ZAR"],
        },
    )
    assert len(rows) == 1
    assert rows[0]["actual_value"] == 5.0
    assert rows[0]["previous_value"] is None
    assert rows[0]["reference_period"] == "2026-06-30"
    assert rows[0]["published_utc"] == "2026-07-22T08:00:00+00:00"
    assert rows[0]["published_time_inferred"] is False
    assert rows[0]["timing_precision"] == "official_exact_schedule"
    assert rows[0]["source_native_components"]["CPI1000F"]["movement"] == 1
    assert rows[0]["numeric_direction_policy"] == "abstain_and_learn_response"

    policy_rows = news.parse_sarb_homepage_rates(
        payload,
        {
            "source_id": "south_africa_sarb_policy_rate_direct_v1",
            "name": "SARB policy rate",
            "url": "https://custom.resbank.co.za/SarbWebApi/WebIndicators/HomePageRates",
            "table_id": "sarb_homepage_rates",
            "series_code": "MMRD002A",
            "event_series_id": "south_africa_policy_rate",
            "event_name": "South Africa policy rate",
            "event_country": "South Africa",
            "unit": "percent",
            "source_role": "primary_policy_release",
            "verified": True,
            "direct": True,
            "currencies": ["ZAR"],
        },
    )
    assert len(policy_rows) == 1
    assert policy_rows[0]["actual_value"] == 7.0
    assert policy_rows[0]["reference_period"] == "2026-08-14"
    assert policy_rows[0]["source_role"] == "primary_policy_release"
    assert policy_rows[0]["numeric_direction_policy"] == "abstain_and_learn_response"


def test_cnb_homepage_inflation_parser_selects_current_value_and_period() -> None:
    payload = b"""
    <div><h2>Inflation</h2>
      <div class="inflationBox-graph inflationBox-graph--en" data-value="1.7">
        <span>1.7%</span></div>
      <p>July 2026</p>
      <a>More about inflation</a>
    </div>
    """
    rows = news.parse_cnb_homepage_inflation(
        payload,
        {
            "source_id": "czech_cnb_inflation_snapshot_direct_v1",
            "name": "CNB inflation snapshot",
            "url": "https://www.cnb.cz/en/index.html",
            "table_id": "cnb_homepage_inflation",
            "event_series_id": "czech_headline_cpi_yoy",
            "event_name": "Czech headline consumer price inflation",
            "event_country": "Czechia",
            "unit": "year_percent_change",
            "verified": True,
            "direct": True,
            "currencies": ["CZK"],
        },
    )
    assert len(rows) == 1
    assert rows[0]["actual_value"] == 1.7
    assert rows[0]["previous_value"] is None
    assert rows[0]["reference_period"] == "2026M07"
    assert rows[0]["published_utc"] == ""
    assert rows[0]["numeric_direction_policy"] == "abstain_and_learn_response"


def test_ksh_prices_snapshot_parser_selects_current_value_and_period() -> None:
    payload = b"""
    <div class="key-feature-value">
      <button><div class="value">1.2<span>%</span></div>
        <h3 class="title">Change in consumer prices</h3>
        <div class="ref-time-sm">July 2026</div></button>
      <section><div class="ref-time">Last data for period: July 2026</div></section>
    </div>
    """
    rows = news.parse_ksh_prices_snapshot(
        payload,
        {
            "source_id": "hungary_ksh_cpi_snapshot_direct_v1",
            "name": "KSH headline CPI snapshot",
            "url": "https://www.ksh.hu/prices?lang=en",
            "table_id": "ksh_prices_key_feature",
            "event_series_id": "hungary_headline_cpi_yoy",
            "event_name": "Hungary headline consumer price inflation",
            "event_country": "Hungary",
            "unit": "year_percent_change",
            "verified": True,
            "direct": True,
            "currencies": ["HUF"],
        },
    )
    assert len(rows) == 1
    assert rows[0]["actual_value"] == 1.2
    assert rows[0]["previous_value"] is None
    assert rows[0]["reference_period"] == "2026M07"
    assert rows[0]["published_utc"] == ""
    assert rows[0]["numeric_direction_policy"] == "abstain_and_learn_response"


def test_ksh_exact_calendar_combines_official_date_and_fixed_release_time() -> None:
    payload = b"""
    <table id="gyorstajekoztatok">
      <thead><tr><th>First releases</th><th>Latest release</th><th>Next release</th></tr></thead>
      <tbody>
        <tr><td>Consumer prices, July 2026</td><td>07/08/2026</td><td>08/09/2026</td></tr>
        <tr><td>Industrial producer prices, June 2026</td><td>31/07/2026</td><td>31/08/2026</td></tr>
      </tbody>
    </table>
    """
    source = {
        "source_id": "hungary_ksh_headline_cpi_release_clock_exact_v3",
        "name": news.KSH_V3_SOURCE_NAME,
        "kind": "ksh_release_calendar_verified_rule_exact_v3",
        "url": "https://www.ksh.hu/prices?lang=en",
        "publisher_url": "https://www.ksh.hu/prices?lang=en",
        "release_time_rule_url": (
            "https://www.ksh.hu/docs/bemutatkozas/eng/"
            "dissemination-and-communication-policy-2024.pdf"
        ),
        "release_time_rule_sha256": (
            "617f513efcccfd07fa159b0fdf9fc9e0e3243095e20deb50d88e88495fbd4030"
        ),
        "release_time_rule_observed_sha256": (
            "617f513efcccfd07fa159b0fdf9fc9e0e3243095e20deb50d88e88495fbd4030"
        ),
        "release_time_rule_archive_name": (
            "ksh_dissemination_policy_2024_617f513efcccfd07.pdf"
        ),
        "release_rule_bytes_verified": True,
        "release_time_rule_section": news.KSH_V3_POLICY_SECTION,
        "source_timezone": "Europe/Budapest",
        "official_release_time_local": "08:30",
        "public_knowledge_time_policy": (
            "public_release_at_0830_local; "
            "embargoed_or_pre_release_access_is_not_public_causal_knowledge"
        ),
        "reference_period_rule": "next_calendar_month_after_latest_release_title",
        "event_country": "Hungary",
        "event_mappings": [{
            "title_pattern": r"^Consumer prices,\s+[A-Za-z]+\s+[0-9]{4}$",
            "event_series_id": "hungary_headline_cpi_yoy",
            "event_name": "Hungary headline consumer price inflation",
        }],
        "currencies": ["HUF"],
        "verified": True,
        "direct": True,
        "retrieval_via": news.KSH_V3_RETRIEVAL_VIA,
        "source_role": "primary_statistical_calendar",
        "source_contract_id": (
            "hungary_ksh_headline_cpi_release_clock_exact_v3_verified_policy_bytes_20260817"
        ),
        "source_cohort_id": (
            "hungary_ksh_headline_cpi_release_clock_exact_v3_verified_policy_bytes_20260817"
        ),
        "quality": 1.0,
        "directional_research_only": True,
        "trusted_domains": ["ksh.hu", "www.ksh.hu"],
    }

    rows = news.parse_ksh_release_calendar_exact(payload, source)

    assert len(rows) == 1
    row = rows[0]
    assert row["scheduled_utc"] == "2026-09-08T06:30:00+00:00"
    assert row["reference_period"] == "2026M08"
    assert row["timing_precision"] == "minute"
    assert row["clock_semantics"] == "domestic_official_statistical_release"
    assert row["independent_domestic_event"] is True
    assert row["actual"] is None and row["consensus"] is None
    assert row["direction"] is None and row["execution_eligible"] is False
    assert row["release_time_basis"] == (
        "official_ksh_first_release_fixed_local_time"
    )
    assert row["release_time_rule_sha256"] == source["release_time_rule_sha256"]
    assert row["material_commitment"]["source_contract_id"] == source["source_contract_id"]
    assert row["material_commitment"]["reference_period_rule"] == source["reference_period_rule"]
    assert len(row["material_content_sha256"]) == 64


def test_ksh_exact_calendar_source_contract_is_official_and_new() -> None:
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    sources = {row["source_id"]: row for row in config["sources"]}
    retired = sources["hungary_ksh_release_calendar_exact_v1"]
    assert retired["enabled"] is False
    assert retired["runtime_supported"] is False
    assert retired["replacement_source_id"] == (
        "hungary_ksh_headline_cpi_release_clock_exact_v2"
    )
    retired_v2 = sources["hungary_ksh_headline_cpi_release_clock_exact_v2"]
    assert retired_v2["enabled"] is False
    assert retired_v2["runtime_supported"] is False
    assert retired_v2["replacement_source_id"] == (
        "hungary_ksh_headline_cpi_release_clock_exact_v3"
    )
    source = sources["hungary_ksh_headline_cpi_release_clock_exact_v3"]
    assert source["source_contract_id"] == (
        "hungary_ksh_headline_cpi_release_clock_exact_v3_verified_policy_bytes_20260817"
    )
    assert source["source_cohort_id"] == source["source_contract_id"]
    assert source["conditional_get"] is False
    assert source["http_transport"] == "curl"
    assert source["official_release_time_local"] == "08:30"
    assert source["source_timezone"] == "Europe/Budapest"
    assert source["release_time_rule_url"].startswith("https://www.ksh.hu/")
    assert source["release_time_rule_sha256"] == (
        "617f513efcccfd07fa159b0fdf9fc9e0e3243095e20deb50d88e88495fbd4030"
    )
    assert source["release_time_rule_archive_name"].endswith(".pdf")
    assert source["reference_period_rule"] == (
        "next_calendar_month_after_latest_release_title"
    )
    assert len(source["event_mappings"]) == 1
    assert source["verified"] is True and source["direct"] is True


def test_ksh_context_dedup_prefers_verified_v3_provenance_over_retired_rows() -> None:
    common = {
        "headline": "Hungary headline consumer price inflation",
        "summary": "Official pre-announced exact release clock.",
        "category": "inflation",
        "structured_event": True,
        "scheduled_utc": "2026-09-08T06:30:00+00:00",
        "published_utc": "2026-08-17T06:00:00+00:00",
        "first_seen_utc": "2026-08-17T06:01:00+00:00",
        "source_verified": True,
        "source_quality": 1.0,
        "currencies": ["HUF"],
        "currency_scores": {},
        "event_id": "legacy-event",
    }
    retired_v1 = {
        **common,
        "source_id": "hungary_ksh_release_calendar_exact_v1",
        "source_contract_id": (
            "hungary_ksh_release_calendar_exact_v1_0830_policy_20260817"
        ),
        "source_cohort_id": (
            "hungary_ksh_release_calendar_exact_v1_0830_policy_20260817"
        ),
    }
    retired_v2 = {
        **common,
        "source_id": "hungary_ksh_headline_cpi_release_clock_exact_v2",
        "source_contract_id": (
            "hungary_ksh_headline_cpi_release_clock_exact_v2_"
            "content_addressed_20260817"
        ),
        "source_cohort_id": (
            "hungary_ksh_headline_cpi_release_clock_exact_v2_"
            "content_addressed_20260817"
        ),
        "event_id": "v2-event",
    }
    verified_contract = (
        "hungary_ksh_headline_cpi_release_clock_exact_v3_"
        "verified_policy_bytes_20260817"
    )
    verified_v3 = {
        **common,
        "source_id": "hungary_ksh_headline_cpi_release_clock_exact_v3",
        "source_contract_id": verified_contract,
        "source_cohort_id": verified_contract,
        "release_time_rule_observed_sha256": (
            "617f513efcccfd07fa159b0fdf9fc9e0e3243095e20deb50d88e88495fbd4030"
        ),
        "release_time_rule_archive_name": (
            "ksh_dissemination_policy_2024_617f513efcccfd07.pdf"
        ),
        "release_rule_bytes_verified": True,
        "event_id": "v3-event",
    }

    topic = news.cluster_articles(
        [retired_v1, retired_v2, verified_v3],
        as_of=dt.datetime(2026, 8, 17, 7, 0, tzinfo=dt.timezone.utc),
    )[0]

    assert topic["source_id"] == verified_v3["source_id"]
    assert topic["source_contract_id"] == verified_contract
    assert topic["release_rule_bytes_verified"] is True
    assert topic["release_time_rule_observed_sha256"] == (
        verified_v3["release_time_rule_observed_sha256"]
    )
    assert topic["source_ids"] == sorted(
        [
            retired_v1["source_id"],
            retired_v2["source_id"],
            verified_v3["source_id"],
        ]
    )


def test_ksh_exact_calendar_uses_winter_budapest_offset() -> None:
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row["source_id"] == "hungary_ksh_headline_cpi_release_clock_exact_v3"
    )
    source = dict(
        source,
        release_time_rule_observed_sha256=source["release_time_rule_sha256"],
        release_rule_bytes_verified=True,
    )
    payload = b"""
    <table><tr><td>Consumer prices, November 2026</td>
    <td>08/12/2026</td><td>08/01/2027</td></tr></table>
    """
    row = news.parse_ksh_release_calendar_exact(payload, source)[0]
    assert row["scheduled_utc"] == "2027-01-08T07:30:00+00:00"
    assert row["reference_period"] == "2026M12"


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"<table><tr><td>Industrial prices</td><td>x</td><td>08/09/2026</td></tr></table>", "row is missing"),
        (b"<table><tr><td>Consumer prices, July 2026</td><td>07/08/2026</td><td>bad</td></tr></table>", "malformed next-release date"),
        (b"<table><tr><td>Consumer prices, July 2026</td><td>bad</td><td>08/09/2026</td></tr></table>", "malformed latest-release date"),
    ],
)
def test_ksh_exact_calendar_fails_health_on_missing_or_malformed_cpi_row(
    payload: bytes,
    message: str,
) -> None:
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row["source_id"] == "hungary_ksh_headline_cpi_release_clock_exact_v3"
    )
    source = dict(
        source,
        release_time_rule_observed_sha256=source["release_time_rule_sha256"],
        release_rule_bytes_verified=True,
    )
    with pytest.raises(ValueError, match=message):
        news.parse_ksh_release_calendar_exact(payload, source)


def test_ksh_exact_calendar_rejects_untrusted_rule_or_unpinned_policy() -> None:
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row["source_id"] == "hungary_ksh_headline_cpi_release_clock_exact_v3"
    )
    source = dict(
        source,
        release_time_rule_observed_sha256=source["release_time_rule_sha256"],
        release_rule_bytes_verified=True,
    )
    payload = b"<table><tr><td>Consumer prices, July 2026</td><td>07/08/2026</td><td>08/09/2026</td></tr></table>"
    untrusted = dict(source, release_time_rule_url="https://example.com/policy.pdf")
    with pytest.raises(ValueError, match="rule URL is invalid"):
        news.parse_ksh_release_calendar_exact(payload, untrusted)
    wrong_hash = dict(source, release_time_rule_sha256="0" * 64)
    with pytest.raises(ValueError, match="content hash is invalid"):
        news.parse_ksh_release_calendar_exact(payload, wrong_hash)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("official_release_time_local", "09:30", "release time is invalid"),
        ("source_timezone", "Europe/London", "source timezone is invalid"),
        ("url", "https://www.ksh.hu/industry", "calendar URL is invalid"),
        (
            "publisher_url",
            "https://www.ksh.hu/industry",
            "calendar URL is invalid",
        ),
        (
            "release_time_rule_url",
            "https://www.ksh.hu/another-policy.pdf",
            "rule URL is invalid",
        ),
        (
            "release_time_rule_archive_name",
            "another-policy.pdf",
            "archive identity is invalid",
        ),
        (
            "release_time_rule_section",
            "another section",
            "policy section is invalid",
        ),
        ("kind", "ksh_release_calendar_exact", "source kind is invalid"),
        ("name", "Impersonating source", "official-source semantics are invalid"),
        ("currencies", ["HUF", "USD"], "official-source semantics are invalid"),
        ("verified", False, "official-source semantics are invalid"),
        ("direct", False, "official-source semantics are invalid"),
        ("quality", 0.5, "official-source semantics are invalid"),
        ("retrieval_via", "secondary", "official-source semantics are invalid"),
        ("source_role", "secondary_commentary", "official-source semantics are invalid"),
        ("event_country", "United States", "official-source semantics are invalid"),
        ("directional_research_only", False, "official-source semantics are invalid"),
        ("trusted_domains", ["www.ksh.hu"], "official-source semantics are invalid"),
        ("encoding", "latin-1", "encoding override is invalid"),
    ],
)
def test_ksh_v3_contract_rejects_mutated_frozen_source_semantics(
    field: str,
    value: object,
    message: str,
) -> None:
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row["source_id"] == news.KSH_V3_SOURCE_ID
    )
    source = dict(
        source,
        release_time_rule_observed_sha256=source["release_time_rule_sha256"],
        release_rule_bytes_verified=True,
        **{field: value},
    )
    payload = (
        b"<table><tr><td>Consumer prices, July 2026</td>"
        b"<td>07/08/2026</td><td>08/09/2026</td></tr></table>"
    )
    with pytest.raises(ValueError, match=message):
        news.parse_ksh_release_calendar_exact(payload, source)


def test_ksh_v3_contract_rejects_mutated_mapping_and_date_sequence() -> None:
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row["source_id"] == news.KSH_V3_SOURCE_ID
    )
    source = dict(
        source,
        release_time_rule_observed_sha256=source["release_time_rule_sha256"],
        release_rule_bytes_verified=True,
    )
    valid_payload = (
        b"<table><tr><td>Consumer prices, July 2026</td>"
        b"<td>07/08/2026</td><td>08/09/2026</td></tr></table>"
    )
    changed_mapping = dict(
        source,
        event_mappings=[{
            "title_pattern": r"^Industrial producer prices.*$",
            "event_series_id": "hungary_industrial_producer_prices",
            "event_name": "Hungary industrial producer prices",
        }],
    )
    with pytest.raises(ValueError, match="does not match frozen contract"):
        news.parse_ksh_release_calendar_exact(valid_payload, changed_mapping)

    reversed_dates = (
        b"<table><tr><td>Consumer prices, July 2026</td>"
        b"<td>07/10/2026</td><td>08/09/2026</td></tr></table>"
    )
    with pytest.raises(
        ValueError,
        match="(inconsistent with headline month|date sequence is invalid)",
    ):
        news.parse_ksh_release_calendar_exact(reversed_dates, source)


def test_ksh_v3_real_pipeline_roundtrip_reaches_immutable_clock(tmp_path) -> None:
    from src.forex_system.ingestion.immutable_event_clock import (
        build_snapshot,
        read_stable_source,
    )

    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        row for row in config["sources"]
        if row["source_id"] == news.KSH_V3_SOURCE_ID
    )
    source = dict(
        source,
        release_time_rule_observed_sha256=source["release_time_rule_sha256"],
        release_rule_bytes_verified=True,
    )
    payload = (
        b"<table><tr><td>Consumer prices, July 2026</td>"
        b"<td>07/08/2026</td><td>08/09/2026</td></tr></table>"
    )
    parsed = news.parse_ksh_release_calendar_exact(payload, source)[0]
    classified = news.classify_article(
        parsed,
        first_seen=dt.datetime(2026, 8, 17, 7, 0, tzinfo=UTC),
    )
    assert classified["category"] == "inflation_context"
    assert classified["source_currencies"] == ["HUF"]
    assert classified["direct_currencies"] == ["HUF"]
    assert classified["currency_scores"] == {}
    assert classified["directional_bias"] == {}

    # Exercise the actual CSV boundary rather than passing a richer synthetic
    # mapping that production never publishes.
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=news.LEDGER_FIELDS)
    writer.writeheader()
    writer.writerow(news.ledger_row(classified))
    stream.seek(0)
    csv_row = next(csv.DictReader(stream))
    tagged = news.event_tagger.normalize_event(
        csv_row,
        source_type="live_news_watch",
    )
    assert tagged is not None
    assert tagged["source_type"] == "live_news_watch"
    assert tagged["direct_currencies"] == ["HUF"]
    assert tagged["context_only"] is True
    assert tagged["execution_eligible"] is False

    events_path = tmp_path / "events_latest.json"
    manifest_path = tmp_path / "manifest.json"
    events_path.write_text(json.dumps([tagged], sort_keys=True), encoding="utf-8")
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_schema_version": 2,
                "pipeline_version": "all_pair_news_event_tags_v3",
                "generated_utc": "2026-08-17T07:00:00+00:00",
                "event_count": 1,
                "events_artifact_name": events_path.name,
                "events_sha256": hashlib.sha256(events_path.read_bytes()).hexdigest(),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    stable = read_stable_source(events_path, manifest_path)
    snapshot = build_snapshot(
        stable,
        dt.datetime(2026, 8, 17, 7, 0, 1, tzinfo=UTC),
    )
    assert len(snapshot["event_versions"]) == 1
    assert snapshot["event_versions"][0]["source_id"] == news.KSH_V3_SOURCE_ID
    assert snapshot["event_versions"][0]["direct_currencies"] == ["HUF"]


def test_ksh_v3_retained_pre_v105_row_reclassifies_source_currency_provenance(
    tmp_path,
) -> None:
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        dict(row) for row in config["sources"]
        if row["source_id"] == news.KSH_V3_SOURCE_ID
    )
    source.update(
        release_time_rule_observed_sha256=source["release_time_rule_sha256"],
        release_rule_bytes_verified=True,
    )
    parsed = news.parse_ksh_release_calendar_exact(
        (
            b"<table><tr><td>Consumer prices, July 2026</td>"
            b"<td>07/08/2026</td><td>08/09/2026</td></tr></table>"
        ),
        source,
    )[0]
    observed = dt.datetime(2026, 8, 17, 7, 13, 18, tzinfo=UTC)
    retained = news.classify_article(parsed, first_seen=observed)
    retained["classification_version"] = "local_fx_news_rules_20260816_v104"
    retained["source_currencies"] = []

    connection = news.open_database(tmp_path / "news.sqlite")
    try:
        assert news.upsert_articles(connection, [retained], observed) == (1, 0)
        changed = news.reclassify_stored_articles(
            connection,
            sources={source["source_id"]: source},
            since=observed - dt.timedelta(minutes=1),
        )
        assert changed == 1
        payload = json.loads(
            connection.execute(
                "SELECT payload_json FROM articles WHERE source_id=?",
                (source["source_id"],),
            ).fetchone()[0]
        )
        assert payload["classification_version"] == news.CLASSIFICATION_VERSION
        assert payload["source_currencies"] == ["HUF"]
        assert payload["release_rule_bytes_verified"] is True
        assert payload["release_time_rule_observed_sha256"] == (
            news.KSH_V3_POLICY_SHA256
        )
    finally:
        connection.close()


def test_scb_cpi_snapshot_parser_preserves_cpi_and_target_cpif() -> None:
    payload = b"""
    <p>The inflation rate according to the CPI in July 2026 was 0.2 percent,
    down from 0.7 percent in June. The monthly change for the CPI from June to
    July was -0.3 percent. The inflation rate according to the CPIF (Consumer
    Price Index with fixed interest rate) was 0.7 percent in July, down from
    1.3 percent in June.</p>
    """
    rows = news.parse_scb_cpi_snapshot(
        payload,
        {
            "source_id": "sweden_scb_cpif_snapshot_direct_v1",
            "name": "Statistics Sweden CPI and CPIF",
            "url": "https://www.scb.se/PR0101-EN/?menu=open",
            "table_id": "scb_pr0101_key_figures",
            "event_series_id": "sweden_cpif_yoy",
            "event_name": "Sweden CPIF consumer price inflation",
            "event_country": "Sweden",
            "unit": "year_percent_change",
            "release_utc_by_reference": {
                "2026M07": "2026-08-13T06:00:00Z"
            },
            "verified": True,
            "direct": True,
            "currencies": ["SEK"],
        },
    )
    assert len(rows) == 1
    assert rows[0]["actual_value"] == 0.7
    assert rows[0]["previous_value"] == 1.3
    assert rows[0]["reference_period"] == "2026M07"
    assert rows[0]["source_native_components"]["headline_cpi_yoy"] == {
        "actual": 0.2,
        "previous": 0.7,
    }
    assert rows[0]["source_native_components"]["headline_cpi_monthly"]["actual"] == -0.3
    assert rows[0]["published_utc"] == "2026-08-13T06:00:00+00:00"
    assert rows[0]["published_time_inferred"] is False
    assert rows[0]["timing_precision"] == "official_exact_schedule"


def test_tuik_press_indicators_parser_selects_frozen_cpi_indicator() -> None:
    payload = json.dumps(
        {
            "data": [
                {
                    "id": 1,
                    "pressUrl": "/en/press/58297",
                    "date": "2026/7",
                    "value": 31.75,
                    "graphics": [
                        {
                            "type": "line",
                            "series": [
                                {
                                    "title": "Consumer Price Index - Annual",
                                    "data": [32.61, 32.11, 31.75],
                                }
                            ],
                        }
                    ],
                },
                {"id": 2, "date": "2026/6", "value": 7.6, "graphics": []},
            ],
            "isError": False,
            "message": "ok",
        }
    ).encode()
    rows = news.parse_tuik_press_indicators(
        payload,
        {
            "source_id": "turkey_tuik_cpi_indicator_direct_v1",
            "name": "TurkStat headline CPI",
            "url": "https://veriportali.tuik.gov.tr/api/en/press/indicators",
            "table_id": "tuik_press_indicators",
            "indicator_id": 1,
            "event_series_id": "turkey_headline_cpi_yoy",
            "event_name": "Turkiye headline consumer price inflation",
            "event_country": "Turkiye",
            "unit": "year_percent_change",
            "release_utc_by_reference": {
                "2026M07": "2026-08-03T07:00:00Z",
            },
            "verified": True,
            "direct": True,
            "currencies": ["TRY"],
        },
    )
    assert len(rows) == 1
    assert rows[0]["actual_value"] == 31.75
    assert rows[0]["previous_value"] == 32.11
    assert rows[0]["reference_period"] == "2026M07"
    assert rows[0]["published_utc"] == "2026-08-03T07:00:00+00:00"
    assert rows[0]["published_time_inferred"] is False
    assert rows[0]["timing_precision"] == "official_exact_schedule"
    assert rows[0]["source_native_components"]["headline_cpi_yoy"]["history_count"] == 3


def test_rbnz_ocr_snapshot_parser_preserves_update_clock() -> None:
    payload = b"""
    <html><body>
      <h1>The official cash rate (OCR)</h1>
      <section>
        <h2>Official Cash Rate</h2><strong>2.5</strong><span>%</span>
        <p>Updated: 2:00pm, 08 Jul 2026</p>
        <p>Next update: 2:00pm, 02 Sep 2026</p>
      </section>
    </body></html>
    """
    rows = news.parse_rbnz_ocr_snapshot(
        payload,
        {
            "source_id": "new_zealand_rbnz_ocr_snapshot_direct_v1",
            "name": "Reserve Bank of New Zealand official cash rate",
            "url": "https://www.rbnz.govt.nz/en/monetary-policy/about-monetary-policy/the-official-cash-rate",
            "table_id": "rbnz_current_ocr",
            "event_series_id": "new_zealand_official_cash_rate",
            "event_name": "New Zealand Official Cash Rate",
            "event_country": "New Zealand",
            "event_timezone": "Pacific/Auckland",
            "unit": "percent",
            "verified": True,
            "direct": True,
            "currencies": ["NZD"],
        },
    )
    assert len(rows) == 1
    assert rows[0]["actual_value"] == 2.5
    assert rows[0]["previous_value"] is None
    assert rows[0]["reference_period"] == "2026-07-08"
    assert rows[0]["published_utc"] == "2026-07-08T02:00:00+00:00"
    assert rows[0]["published_time_inferred"] is False
    assert rows[0]["timing_precision"] == "official_exact_schedule"
    assert rows[0]["source_native_components"]["next_update"] == {
        "local_time": "2:00pm",
        "local_date": "2026-09-02",
        "timezone": "Pacific/Auckland",
    }
def test_structured_numeric_release_is_bound_to_configured_source_currency():
    article = news.classify_article(
        {
            "source_id": "ons_published_releases",
            "source_name": "Office for National Statistics",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_currencies": ["GBP"],
            "title": "UK real GDP quarterly change",
            "summary": (
                "The comparison discusses Canada and Japan. Oil prices rise "
                "during sanctions and global risk-off conditions. The body "
                "also discusses employment, wages, and consumer inflation."
            ),
            "url": "https://www.ons.gov.uk/releases/test",
            "published_utc": "2026-08-14T06:00:00Z",
            "structured_event": True,
            "event_series_id": "uk_real_gdp_qoq",
            "event_name": "UK real GDP quarterly change",
            "actual_value": 0.3,
            "previous_value": 0.1,
        },
        first_seen=dt.datetime(2026, 8, 14, 6, 0, 10, tzinfo=UTC),
    )
    assert article["currencies"] == ["GBP"]
    assert set(article["currency_scores"]) <= {"GBP"}
    assert article["category"] == "growth_release"
    assert article["risk_off_score"] == 0.0
    assert article["risk_on_score"] == 0.0
    assert article["source_native_currency_bound"] is True


def test_structured_snapshot_without_native_publication_clock_is_not_timely():
    article = news.classify_article(
        {
            "source_id": "official_snapshot",
            "source_name": "Official statistics",
            "source_kind": "json",
            "source_quality": 1.0,
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_statistical_release",
            "source_currencies": ["NOK"],
            "title": "Norway consumer price index annual change",
            "summary": "",
            "url": "https://example.test/official",
            "published_utc": "",
            "structured_event": True,
            "event_series_id": "norway_cpi_yoy",
            "event_name": "Norway consumer price index annual change",
            "actual_value": 3.2,
            "reference_period": "2026M07",
        },
        first_seen=dt.datetime(2026, 8, 16, 16, 0, tzinfo=UTC),
    )
    assert article["published_time_inferred"] is True
    assert article["forward_signal_timely"] is False
    assert article["directional_publish_eligible"] is False
@pytest.mark.parametrize(
    ("currency", "headline"),
    [
        ("GBP", "Aberdeen does not expect a BoE rate hike this year"),
        ("USD", "Fed Rate-Hike Expectations Cool, Dollar Softens"),
        ("USD", "Bets on imminent Fed rate hike fade"),
        (
            "USD",
            "Fed Rate Hike Debate Builds, But Economists See No Move This Year",
        ),
        ("JPY", "Weak Japan growth muddies waters for BoJ rate hike"),
        ("USD", "Inflation data suggests Fed rate hike delay"),
        ("USD", "Probability of a Fed rate hike has dropped sharply"),
    ],
)
def test_reduced_rate_hike_expectations_are_dovish_and_require_corroboration(
    currency, headline
):
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_name": "secondary discovery",
            "source_kind": "rss",
            "source_quality": 0.65,
            "source_verified": False,
            "source_direct": False,
            "source_currencies": [currency],
            "title": headline,
            "summary": "",
            "url": f"https://example.test/{currency}/{len(headline)}",
            "published_utc": "2026-08-19T14:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 14, 1, tzinfo=UTC),
    )
    assert article["monetary_impulse"] < 0
    assert article["directional_corroboration_required"] is True
    assert article["currency_scores"][currency] < 0
    assert article["directional_publish_eligible"] is False


def test_wall_street_outcome_headline_is_retrospective_not_catalyst():
    article = news.classify_article(
        {
            "source_id": "gdelt_fx_macro_discovery",
            "source_verified": False,
            "source_direct": False,
            "source_quality": 0.65,
            "title": "Wall Street hits record as inflation cools, oil prices fall",
            "summary": "",
            "url": "https://example.test/wall-street-outcome",
            "published_utc": "2026-08-13T20:24:00Z",
        },
        first_seen=dt.datetime(2026, 8, 13, 20, 25, tzinfo=UTC),
    )
    assert article["reports_prior_market_move"] is True
    assert article["directional_publish_eligible"] is False


def test_live_multi_asset_recap_keeps_oil_direction_clause_local():
    article = news.classify_article(
        {
            "source_id": "google_news_market_ticker",
            "source_verified": False,
            "source_direct": False,
            "source_quality": 0.65,
            "title": (
                "LIVE | Dow Jones, Nasdaq, US Stock Market Today: Check Latest "
                "Move in Futures, Dow Jones Rise as Nvidia Falls 2.3%, "
                "Semiconductor Weakness, Oil Price Falls & Gold Surges | Why "
                "Are Stocks Down Today"
            ),
            "summary": "",
            "url": "https://example.test/live-multi-asset-recap",
            "published_utc": "2026-08-24T19:51:03Z",
        },
        first_seen=dt.datetime(2026, 8, 24, 19, 52, 55, tzinfo=UTC),
    )
    assert article["reports_prior_market_move"] is True
    assert article["directional_publish_eligible"] is False
    assert article["currency_scores"]["CAD"] < 0
    assert article["currency_scores"]["NOK"] < 0
    assert article["currency_scores"]["MXN"] < 0
    assert article["currency_scores"]["JPY"] > 0


def test_currency_underperformance_headline_is_retrospective_not_catalyst():
    article = news.classify_article(
        {
            "source_id": "google_news_market_ticker",
            "source_verified": False,
            "source_direct": False,
            "source_quality": 0.65,
            "title": "Risk-off vibe sees NZD and AUD underperform - Interest.co.nz",
            "summary": "",
            "url": "https://example.test/nzd-aud-underperform",
            "published_utc": "2026-08-24T19:57:00Z",
        },
        first_seen=dt.datetime(2026, 8, 24, 20, 7, 5, tzinfo=UTC),
    )
    assert article["reports_prior_market_move"] is True
    assert article["directional_publish_eligible"] is False


def test_secondary_multi_indicator_recap_requires_corroboration():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_verified": False,
            "source_direct": False,
            "source_quality": 0.65,
            "source_currencies": ["USD"],
            "title": (
                "US Economy: Inflation eases but prices remain high, retail "
                "sales fall, jobless claims rise"
            ),
            "summary": "",
            "url": "https://example.test/economic-recap",
            "published_utc": "2026-08-15T16:40:45Z",
        },
        first_seen=dt.datetime(2026, 8, 15, 16, 42, tzinfo=UTC),
    )
    assert article["directional_corroboration_required"] is True
    assert article["directional_publish_eligible"] is False


def test_hypothetical_geopolitical_escalation_is_not_observed_risk_event():
    article = news.classify_article(
        {
            "source_id": "google_news_systemic_catalyst",
            "source_verified": False,
            "source_direct": False,
            "source_quality": 0.65,
            "title": (
                "Trump is going to have to escalate Iran war: Rick Scott"
            ),
            "summary": "",
            "url": "https://example.test/escalation-opinion",
            "published_utc": "2026-08-19T14:18:00Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 14, 19, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0
    assert article["risk_on_score"] == 0
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_escalation_options_question_is_not_observed_risk_event():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_verified": False,
            "source_direct": False,
            "source_quality": 0.65,
            "title": (
                "US opens economic war front. What are Iran's options to "
                "escalate further? - India Today"
            ),
            "summary": "",
            "url": "https://example.test/escalation-options-question",
            "published_utc": "2026-08-24T20:32:15Z",
        },
        first_seen=dt.datetime(2026, 8, 24, 20, 37, 1, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0
    assert article["risk_on_score"] == 0
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_ceasefire_prospects_being_damaged_is_not_risk_on():
    article = news.classify_article(
        {
            "source_id": "google_news_global_risk",
            "source_verified": False,
            "source_direct": False,
            "source_quality": 0.65,
            "title": (
                "Ukrainian strike linked to refinery fire, impacting "
                "ceasefire prospects"
            ),
            "summary": "",
            "url": "https://example.test/ceasefire-damaged",
            "published_utc": "2026-08-19T11:24:40Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 11, 26, tzinfo=UTC),
    )
    assert article["risk_on_score"] == 0
    assert article["category"] != "risk_on_deescalation"
    assert article["directional_publish_eligible"] is False


def test_official_policy_speech_body_cannot_create_global_risk_basket():
    article = news.classify_article(
        {
            "source_id": "ecb_press",
            "source_role": "primary_policy_release",
            "source_verified": True,
            "source_direct": True,
            "source_quality": 1.0,
            "source_currencies": ["EUR"],
            "title": "Panel remarks about the European and global economic outlook",
            "summary": (
                "The discussion mentioned the Middle East war and possible "
                "escalation as part of the global backdrop."
            ),
            "url": "https://www.ecb.europa.eu/example",
            "published_utc": "2026-08-19T07:10:00Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 7, 11, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0
    assert article["currency_scores"] == {}


def test_potential_rate_hike_is_speculation_and_moderate_wage_pressure_is_dovish():
    potential = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_verified": False,
            "source_direct": False,
            "source_quality": 0.65,
            "source_currencies": ["SEK"],
            "title": "Riksbank signals potential 2026 rate hike",
            "summary": "",
            "url": "https://example.test/potential-hike",
            "published_utc": "2026-08-19T10:44:55Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 10, 46, tzinfo=UTC),
    )
    moderate = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_verified": False,
            "source_direct": False,
            "source_quality": 0.65,
            "source_currencies": ["EUR"],
            "title": (
                "ECB's Rehn says wage growth remains moderate with no "
                "second-round inflation effects"
            ),
            "summary": "",
            "url": "https://example.test/moderate-wages",
            "published_utc": "2026-08-19T07:06:53Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 7, 8, tzinfo=UTC),
    )
    assert potential["policy_assertion_status"] == "unverified_speculation"
    assert potential["monetary_impulse"] == 0
    assert potential["currency_scores"] == {}
    assert moderate["monetary_impulse"] < 0
    assert moderate["currency_scores"]["EUR"] < 0
    assert moderate["directional_publish_eligible"] is False


def test_conditional_hike_and_calendar_preview_abstain():
    headlines = (
        "RBA Signals Another Rate Hike If Inflation Reignites",
        "Inflation data the main focus on economic calendar today",
    )
    for index, headline in enumerate(headlines):
        article = news.classify_article(
            {
                "source_id": "google_news_fx_macro",
                "source_verified": False,
                "source_direct": False,
                "source_quality": 0.65,
                "source_currencies": ["AUD"],
                "title": headline,
                "summary": "",
                "url": f"https://example.test/preview/{index}",
                "published_utc": "2026-08-19T14:00:00Z",
            },
            first_seen=dt.datetime(2026, 8, 19, 14, 1, tzinfo=UTC),
        )
        assert article["monetary_impulse"] == 0
        assert article["currency_scores"] == {}
        assert article["directional_publish_eligible"] is False


def test_inflation_expected_to_ease_is_dovish_but_not_single_source_publishable():
    article = news.classify_article(
        {
            "source_id": "finnhub_fx_market_news",
            "source_verified": False,
            "source_direct": True,
            "source_quality": 0.7,
            "source_currencies": ["USD"],
            "title": "Cleveland Fed survey: businesses expect inflation to ease",
            "summary": "",
            "url": "https://example.test/inflation-expectations",
            "published_utc": "2026-08-19T14:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 14, 1, tzinfo=UTC),
    )
    assert article["monetary_impulse"] < 0
    assert article["directional_corroboration_required"] is True
    assert article["currency_scores"]["USD"] < 0
    assert article["directional_publish_eligible"] is False


def test_abs_inferred_release_clock_is_stable_across_polls_and_collapses():
    raw = {
        "source_id": "abs_latest_releases",
        "source_name": "Australian Bureau of Statistics",
        "source_kind": "html_links",
        "source_role": "primary_statistical_release",
        "source_quality": 0.98,
        "source_verified": True,
        "source_direct": True,
        "source_currencies": ["AUD"],
        "numeric_parser_activated_utc": "2026-08-19T02:01:00Z",
        "title": "Media Release - Annual wage growth of 3.2% in June quarter 2026",
        "summary": "",
        "url": "https://www.abs.gov.au/statistics/wage-price-index",
        "published_utc": "",
        "published_time_inferred": True,
    }
    first = news.classify_article(
        raw,
        first_seen=dt.datetime(2026, 8, 19, 10, 0, tzinfo=UTC),
    )
    repeated = news.classify_article(
        raw,
        first_seen=dt.datetime(2026, 8, 19, 11, 0, tzinfo=UTC),
    )
    assert first["event_id"] == repeated["event_id"]
    assert first["event_lineage_id"] == repeated["event_lineage_id"]
    assert first["scheduled_utc"] == repeated["scheduled_utc"] == ""
    assert first["source_reported_update_utc"] == ""
    assert repeated["source_reported_update_utc"] == ""
    assert len(news.collapse_exact_source_url_duplicates([first, repeated])) == 1
    changed = dict(repeated)
    changed["actual"] = "3.3"
    changed["actual_value"] = 3.3
    assert len(news.collapse_exact_source_url_duplicates([first, changed])) == 2


def test_official_fiscal_narrative_cannot_inherit_body_geopolitical_direction():
    article = news.classify_article(
        {
            "source_id": "australia_treasury",
            "source_role": "primary_fiscal_debt_release",
            "source_verified": True,
            "source_direct": True,
            "source_quality": 1.0,
            "source_currencies": ["AUD"],
            "title": "Wages continue to grow under Labor",
            "summary": (
                "Wages continued to grow. The war in the Middle East has "
                "created global uncertainty."
            ),
            "url": "https://treasury.gov.au/example",
            "published_utc": "2026-08-19T14:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 14, 1, tzinfo=UTC),
    )
    assert article["risk_off_score"] == 0
    assert article["currencies"] == ["AUD"]
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False


def test_explicitly_already_priced_article_is_research_context_only():
    article = news.classify_article(
        {
            "source_id": "finnhub_fx_market_news",
            "source_verified": False,
            "source_direct": True,
            "source_quality": 0.7,
            "source_currencies": ["USD"],
            "title": "Fitch affirms US rating as Fed tightening remains possible",
            "summary": (
                "The softer Fed hike odds are already priced in. This is not "
                "new information and the market reaction should be muted."
            ),
            "url": "https://example.test/already-priced",
            "published_utc": "2026-08-19T14:00:00Z",
        },
        first_seen=dt.datetime(2026, 8, 19, 14, 1, tzinfo=UTC),
    )
    assert article["non_catalyst_context"] is True
    assert article["currency_scores"] == {}
    assert article["directional_publish_eligible"] is False
    assert article["context_reason"] == "already_priced_or_non_catalyst_context"


def test_preclustered_empty_direction_cannot_remain_publishable():
    stale = {
        "topic_clustered": True,
        "event_id": "stale-topic",
        "currency_scores": {},
        "directional_bias": {"EUR": "BULLISH"},
        "directional_evidence": True,
        "directional_publish_eligible": True,
        "context_only": False,
        "reports_prior_market_move": False,
    }
    repaired = news.cluster_articles([stale])[0]
    assert repaired["directional_publish_eligible"] is False
    assert repaired["directional_evidence"] is False
    assert repaired["directional_bias"] == {}


def test_secondary_market_policy_expectation_cannot_publish_via_syndication():
    first_seen = dt.datetime(2026, 8, 21, 17, 30, tzinfo=UTC)
    articles = []
    for index, publisher in enumerate(("Reuters", "WTAQ", "WTVB")):
        articles.append(
            news.classify_article(
                {
                    "source_id": f"google_news_fx_macro_{index}",
                    "source_role": "news_aggregator",
                    "source_verified": False,
                    "source_direct": False,
                    "source_quality": 0.65,
                    "source_currencies": ["EUR"],
                    "title": (
                        "Traders are bracing for an increasingly hawkish ECB - "
                        f"{publisher}"
                    ),
                    "summary": "",
                    "url": f"https://example.test/ecb-expectation/{index}",
                    "published_utc": "2026-08-21T17:25:00Z",
                },
                first_seen=first_seen,
            )
        )
    for article in articles:
        assert article["currency_scores"] == {}
        assert article["research_currency_scores"]["EUR"] > 0
        assert article["directional_research_only"] is True
        assert article["directional_publish_eligible"] is False
        assert article["context_reason"] == (
            "secondary_market_policy_expectation_context"
        )
    topics = news.cluster_articles(articles)
    assert len(topics) == 1
    assert topics[0]["currency_scores"] == {}
    assert topics[0]["directional_evidence"] is False
    assert topics[0]["directional_publish_eligible"] is False


def test_ceasefire_expiry_is_not_misread_as_fresh_deescalation():
    for index, headline in enumerate(
        (
            "Strait of Hormuz shipping grinds to a halt ahead of U.S.-Iran ceasefire expiry",
            "US-Iran 60-day ceasefire agreement expires with no permanent deal in sight",
        )
    ):
        article = news.classify_article(
            {
                "source_id": "google_news_systemic_catalyst",
                "source_verified": False,
                "source_direct": False,
                "source_quality": 0.55,
                "title": headline,
                "summary": "",
                "url": f"https://example.test/ceasefire-expiry/{index}",
                "published_utc": "2026-08-17T08:00:00Z",
            },
            first_seen=dt.datetime(2026, 8, 17, 8, 1, tzinfo=UTC),
        )
        assert article["risk_on_score"] == 0
        assert article["category"] != "risk_on_deescalation"
        assert article["directional_publish_eligible"] is False


def test_threat_and_tentative_deal_headline_abstains_from_risk_on():
    article = news.classify_article(
        {
            "source_id": "google_news_systemic_catalyst",
            "source_verified": False,
            "source_direct": False,
            "source_quality": 0.55,
            "title": (
                "Trump threatens Oman as it works with Iran on a Strait of "
                "Hormuz deal"
            ),
            "summary": "",
            "url": "https://example.test/mixed-threat-deal",
            "published_utc": "2026-08-17T17:30:00Z",
        },
        first_seen=dt.datetime(2026, 8, 17, 17, 32, tzinfo=UTC),
    )
    assert article["risk_on_score"] == 0
    assert article["category"] != "risk_on_deescalation"
    assert article["directional_publish_eligible"] is False


def test_multiword_currency_extends_rally_recap_preserves_observed_sign():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_verified": False,
            "source_direct": False,
            "source_quality": 0.55,
            "source_currencies": ["NZD", "USD"],
            "title": (
                "New Zealand Dollar Extends Rally As Fed Rate-Hike Bets Fade"
            ),
            "summary": "",
            "url": "https://example.test/nzd-extends-rally",
            "published_utc": "2026-08-21T18:15:00Z",
        },
        first_seen=dt.datetime(2026, 8, 21, 18, 17, tzinfo=UTC),
    )
    assert article["reports_prior_market_move"] is True
    assert article["currency_scores"]["NZD"] > 0
    assert article["currency_scores"]["USD"] < 0
    assert article["forward_signal_timely"] is False
    assert article["directional_publish_eligible"] is False


@pytest.mark.parametrize(
    ("headline", "stronger", "weaker"),
    [
        (
            "EUR/USD breakout puts dollar pressure valve in focus",
            "EUR",
            "USD",
        ),
        (
            "USD/JPY breakout puts yen pressure in focus",
            "USD",
            "JPY",
        ),
    ],
)
def test_pair_recap_assigns_pressure_to_intervening_named_currency(
    headline, stronger, weaker
):
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_verified": False,
            "source_direct": False,
            "source_quality": 0.55,
            "title": headline,
            "summary": "",
            "url": f"https://example.test/{stronger.lower()}-{weaker.lower()}-pressure",
            "published_utc": "2026-08-20T06:02:35Z",
        },
        first_seen=dt.datetime(2026, 8, 20, 6, 6, 1, tzinfo=UTC),
    )
    assert article["reports_prior_market_move"] is True
    assert article["currency_scores"][stronger] > 0
    assert article["currency_scores"][weaker] < 0
    assert article["directional_publish_eligible"] is False


def test_greenback_surge_headline_is_retrospective_market_move():
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_verified": False,
            "source_direct": False,
            "source_quality": 0.55,
            "source_currencies": ["USD"],
            "title": (
                "Greenback Surges to Eight-Day Peak Following "
                "Hotter-Than-Expected PCE Data Before Jackson Hole 2026"
            ),
            "summary": "",
            "url": "https://example.test/greenback-surge-recap",
            "published_utc": "2026-08-27T07:42:22Z",
        },
        first_seen=dt.datetime(2026, 8, 27, 7, 59, 28, tzinfo=UTC),
    )
    assert article["reports_prior_market_move"] is True
    assert article["currency_scores"]["USD"] > 0
    assert article["forward_signal_timely"] is False
    assert article["directional_publish_eligible"] is False


@pytest.mark.parametrize(
    ("headline", "stronger", "weaker"),
    [
        (
            "Dollar/Yen Falls to Lower 159 Range as Dollar Selling Accelerates",
            "JPY",
            "USD",
        ),
        (
            "Thai baht/US dollar stronger on Thursday",
            "THB",
            "USD",
        ),
    ],
)
def test_named_currency_slash_pair_recap_preserves_base_quote_direction(
    headline, stronger, weaker
):
    article = news.classify_article(
        {
            "source_id": "google_news_fx_macro",
            "source_verified": False,
            "source_direct": False,
            "source_quality": 0.55,
            "title": headline,
            "summary": "",
            "url": f"https://example.test/named-pair-{stronger.lower()}-{weaker.lower()}",
            "published_utc": "2026-08-20T05:45:00Z",
        },
        first_seen=dt.datetime(2026, 8, 20, 5, 46, tzinfo=UTC),
    )
    assert article["reports_prior_market_move"] is True
    assert article["currency_scores"][stronger] > 0
    assert article["currency_scores"][weaker] < 0
    assert article["directional_publish_eligible"] is False


def test_riksbank_html_release_fallback_is_direct_and_extracts_policy_links():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        item
        for item in config["sources"]
        if item["source_id"] == "riksbank_monetary_policy_html"
    )
    payload = b"""
      <a href="/en-gb/press-and-published/notices-and-press-releases/
      press-releases/2026/policy-rate-unchanged-at-15.75-per-cent">
      20/08/2026 Press release Policy rate unchanged at 1.75 per cent</a>
    """.replace(b"\n", b"").replace(b"      ", b"")

    rows = news.parse_html_links(payload, source)

    assert len(rows) == 1
    assert rows[0]["source_direct"] is True
    assert rows[0]["source_role"] == "primary_policy_release"
    assert rows[0]["source_currencies"] == ["SEK"]
    assert rows[0]["published_utc"] == ""
    assert "Policy rate unchanged" in rows[0]["title"]


def test_rba_minutes_html_is_direct_and_extracts_exact_policy_documents():
    config = json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    source = next(
        item
        for item in config["sources"]
        if item["source_id"] == "rba_monetary_policy_minutes_html"
    )
    payload = b"""
      <a href="/monetary-policy/rba-board-minutes/2026/2026-08-11.html">
      11 August 2026</a>
      <a href="/monetary-policy/rba-board-minutes/2026/index.html">
      Minutes index</a>
    """

    rows = news.parse_html_links(payload, source)

    assert len(rows) == 1
    assert rows[0]["source_direct"] is True
    assert rows[0]["source_role"] == "primary_policy_release"
    assert rows[0]["source_currencies"] == ["AUD"]
    assert rows[0]["published_utc"] == ""
    assert rows[0]["url"].endswith("/2026/2026-08-11.html")
    assert source["detail_context_archive_only"] is True
    assert source["detail_context_url_patterns"] == ["2026-08-11\\.html$"]
    assert source["detail_context_target_count"] == 1


def test_official_minutes_direction_uses_decision_section_not_market_background():
    article = news.classify_article(
        {
            "source_id": "rba_monetary_policy_minutes_html",
            "source_name": "Reserve Bank of Australia monetary-policy minutes HTML",
            "source_kind": "html_links",
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_policy_release",
            "source_currencies": ["AUD"],
            "source_listing_bootstrap": True,
            "detail_context_archive_only": True,
            "title": "11 August 2026",
            "summary": (
                "Minutes of the Monetary Policy Board Meeting. Financial "
                "conditions. Markets had reduced expectations for "
                "further monetary policy tightening after weaker-than-expected "
                "data. Considerations for monetary policy. Inflation was still "
                "too high and risks were tilted to the upside. Financial "
                "stability considerations did not constrain policy. Several members "
                "judged that further tightening could be required. The Board "
                "would be ready to act, including increasing the cash rate if "
                "upside risks materialise. The decision. The cash rate was left "
                "unchanged at 4.35 per cent. Financial stability advice."
            ),
            "url": "https://www.rba.gov.au/monetary-policy/rba-board-minutes/2026/2026-08-11.html",
            "published_utc": "",
        },
        first_seen=dt.datetime(2026, 8, 25, 2, 20, tzinfo=UTC),
    )

    assert article["policy_document_type"] == "minutes_or_accounts"
    assert article["monetary_impulse"] > 0
    assert article["currency_scores"]["AUD"] > 0
    assert article["directional_bias"]["AUD"] == "BULLISH"
    assert article["directional_publish_eligible"] is False
    assert article["source_listing_bootstrap"] is True


def test_official_minutes_cash_rate_action_maps_directly():
    article = news.classify_article(
        {
            "source_id": "rba_monetary_policy_minutes_html",
            "source_name": "Reserve Bank of Australia monetary-policy minutes HTML",
            "source_kind": "html_links",
            "source_verified": True,
            "source_direct": True,
            "source_role": "primary_policy_release",
            "source_currencies": ["AUD"],
            "source_listing_bootstrap": True,
            "title": "17 March 2026",
            "summary": (
                "Minutes of the Monetary Policy Board Meeting. Markets had "
                "reduced tightening expectations. Considerations for monetary "
                "policy. The decision. The Board resolved by majority to "
                "increase the cash rate target by 25 basis points to 4.10 per "
                "cent. Financial stability advice."
            ),
            "url": "https://www.rba.gov.au/monetary-policy/rba-board-minutes/2026/2026-03-17.html",
            "published_utc": "",
        },
        first_seen=dt.datetime(2026, 8, 25, 2, 20, tzinfo=UTC),
    )

    assert article["monetary_impulse"] == 1.0
    assert article["currency_scores"] == {"AUD": 1.0}
    assert article["directional_publish_eligible"] is False


def test_boj_same_host_pdf_attachment_enriches_research_only_and_preserves_clocks(
    monkeypatch,
):
    page_url = "https://www.boj.or.jp/en/about/press/koen_2026/ko260827a.htm"
    pdf_url = (
        "https://www.boj.or.jp/en/about/press/koen_2026/data/ko260827a1.pdf"
    )
    page_payload = (
        b'<html><main>Speech by Deputy Governor HIMINO concerning Japan\'s '
        b'economy and monetary policy at a meeting with local leaders in '
        b'Saitama.</main>'
        b'<a href="/en/about/press/koen_2026/data/ko260827a1.pdf">'
        b'Full Text [PDF 561KB]</a>'
        b'<a href="/en/about/press/koen_2026/data/other.pdf">Appendix PDF</a>'
        b'</html>'
    )
    calls = []

    class Response:
        def __init__(self, url, content_type, payload):
            self._url = url
            self.headers = {"Content-Type": content_type}
            self._payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return self._url

        def read(self, _maximum):
            return self._payload

    def fake_open(request, **_kwargs):
        url = request.full_url
        calls.append(url)
        if url == page_url:
            return Response(url, "text/html; charset=utf-8", page_payload)
        assert url == pdf_url
        return Response(url, "application/pdf", b"%PDF-1.7 bounded fixture")

    original_extract = news.extract_official_document_text

    def fake_extract(payload, *, content_type, url, **kwargs):
        if url == pdf_url:
            return (
                "The Bank should continue to adjust the degree of monetary "
                "accommodation and raise the policy rate if the outlook is "
                "realized. Crude oil prices rise in a scenario discussing "
                "Canada, Mexico, Norway, the U.S. dollar, and the euro area. "
                "This official speech attachment contains the complete "
                "monetary policy discussion.",
                "official_pdf_text",
            )
        return original_extract(
            payload, content_type=content_type, url=url, **kwargs
        )

    monkeypatch.setattr(news.urllib.request, "urlopen", fake_open)
    monkeypatch.setattr(news, "extract_official_document_text", fake_extract)
    old_detail_clock = "2026-08-27T03:59:23.147022+00:00"
    now = dt.datetime(2026, 8, 27, 4, 20, tzinfo=UTC)
    article = {
        "source_id": "boj_updates",
        "source_name": "Bank of Japan updates",
        "source_kind": "rss",
        "source_verified": True,
        "source_direct": True,
        "source_role": "primary_policy_release",
        "source_currencies": ["JPY"],
        "title": "Speech by Deputy Governor HIMINO in Saitama",
        "summary": "",
        "url": page_url,
        "published_utc": "2026-08-27T01:35:00Z",
        "source_listing_bootstrap": True,
    }
    enriched, state, error = news.enrich_recent_official_release_details(
        [article],
        {
            "source_id": "boj_updates",
            "detail_enrichment": "official_document_text",
            "detail_follow_same_authority_pdf_attachments": True,
            "detail_attachment_link_text_patterns": ["full\\s+text"],
            "detail_attachment_max_items_per_page": 1,
            "archive_official_pdfs": False,
            "trusted_domains": ["boj.or.jp"],
        },
        {"detail_first_seen_utc_by_url": {page_url: old_detail_clock}},
        timeout_sec=10,
        maximum_bytes=1_000_000,
        now=now,
    )
    assert enriched == 1 and error == ""
    assert calls == [page_url, pdf_url]
    assert state[page_url] == old_detail_clock
    assert state[pdf_url] == "2026-08-27T04:20:00+00:00"
    assert article["published_utc"] == "2026-08-27T01:35:00Z"
    assert article["detail_available_utc"] == old_detail_clock
    assert article["detail_attachment_available_utc"] == (
        "2026-08-27T04:20:00+00:00"
    )
    assert article["detail_attachment_count"] == 1
    assert article["detail_attachment_urls"] == [pdf_url]
    assert article["detail_attachment_content_sha256"] == hashlib.sha256(
        b"%PDF-1.7 bounded fixture"
    ).hexdigest()
    assert article["detail_attachment_parser_contract_id"] == (
        news.OFFICIAL_PDF_ATTACHMENT_PARSER_CONTRACT_ID
    )
    assert article["detail_attachment_research_only"] is True
    classified = news.classify_article(
        article,
        first_seen=dt.datetime(2026, 8, 27, 3, 57, 42, tzinfo=UTC),
    )
    assert classified["published_utc"] == "2026-08-27T01:35:00+00:00"
    assert classified["first_seen_utc"] == "2026-08-27T03:57:42+00:00"
    assert classified["detail_available_utc"] == old_detail_clock
    assert classified["detail_attachment_available_utc"] == (
        "2026-08-27T04:20:00+00:00"
    )
    assert classified["detail_enrichment_research_only"] is True
    assert classified["issuer_bound_policy_attachment"] is True
    assert classified["issuer_bound_policy_attachment_contract_id"] == (
        "boj_official_policy_speech_pdf_currency_binding_v1_20260827"
    )
    assert classified["official_policy_release"] is False
    assert classified["policy_stance_bearing_eligible"] is False
    assert classified["source_native_currency_bound"] is True
    assert classified["currencies"] == ["JPY"]
    assert classified["direct_currencies"] == ["JPY"]
    assert set(classified["mentioned_currency_entities"]) >= {
        "CAD",
        "EUR",
        "JPY",
        "MXN",
        "NOK",
        "USD",
    }
    assert classified["currency_scores"] == {"JPY": 1.0}
    assert classified["directional_publish_eligible"] is False
    assert classified["execution_eligible"] is False

    topic = news.cluster_articles([classified])[0]
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"] == {"JPY": 1.0}
    assert topic["directional_publish_eligible"] is False
    assert topic["execution_eligible"] is False


def test_official_pdf_attachment_never_follows_cross_host_link(monkeypatch):
    page_url = "https://www.boj.or.jp/en/release.htm"
    page_payload = (
        b"<html><main>Official policy discussion with sufficient factual "
        b"text for bounded research enrichment.</main>"
        b'<a href="https://files.example.net/full.pdf">Full Text PDF</a></html>'
    )
    calls = []

    class Response:
        headers = {"Content-Type": "text/html"}
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def geturl(self): return page_url
        def read(self, _maximum): return page_payload

    def fake_open(request, **_kwargs):
        calls.append(request.full_url)
        assert request.full_url == page_url
        return Response()

    monkeypatch.setattr(news.urllib.request, "urlopen", fake_open)
    article = {
        "title": "Official policy discussion",
        "summary": "",
        "url": page_url,
        "published_utc": "2026-08-27T04:00:00Z",
    }
    enriched, _state, error = news.enrich_recent_official_release_details(
        [article],
        {
            "source_id": "boj_updates",
            "detail_enrichment": "official_document_text",
            "detail_follow_same_authority_pdf_attachments": True,
            "detail_attachment_link_text_patterns": ["full\\s+text"],
            "trusted_domains": ["boj.or.jp"],
        },
        {},
        timeout_sec=10,
        maximum_bytes=1_000_000,
        now=dt.datetime(2026, 8, 27, 4, 1, tzinfo=UTC),
    )
    assert enriched == 1 and error == ""
    assert calls == [page_url]
    assert article["detail_attachment_count"] == 0
    assert article["detail_attachment_discovery_state"] == (
        "no_matching_same_host_pdf"
    )


def test_official_pdf_attachment_error_shell_fails_closed(monkeypatch):
    page_url = "https://www.boj.or.jp/en/release.htm"
    pdf_url = "https://www.boj.or.jp/en/data/full.pdf"
    page_payload = (
        b"<html><main>Official policy landing page with enough factual text "
        b"for parsing.</main><a href='/en/data/full.pdf'>Full Text PDF</a></html>"
    )

    class Response:
        def __init__(self, url, content_type, payload):
            self._url, self._payload = url, payload
            self.headers = {"Content-Type": content_type}
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def geturl(self): return self._url
        def read(self, _maximum): return self._payload

    def fake_open(request, **_kwargs):
        if request.full_url == page_url:
            return Response(page_url, "text/html", page_payload)
        assert request.full_url == pdf_url
        return Response(
            pdf_url,
            "text/html",
            b"<html><h1>Access denied</h1><p>Verify you are human.</p></html>",
        )

    monkeypatch.setattr(news.urllib.request, "urlopen", fake_open)
    article = {
        "title": "Official policy discussion",
        "summary": "unchanged feed summary",
        "url": page_url,
        "published_utc": "2026-08-27T04:00:00Z",
    }
    enriched, state, error = news.enrich_recent_official_release_details(
        [article],
        {
            "source_id": "boj_updates",
            "detail_enrichment": "official_document_text",
            "detail_follow_same_authority_pdf_attachments": True,
            "detail_attachment_link_text_patterns": ["full\\s+text"],
            "trusted_domains": ["boj.or.jp"],
        },
        {},
        timeout_sec=10,
        maximum_bytes=1_000_000,
        now=dt.datetime(2026, 8, 27, 4, 1, tzinfo=UTC),
    )
    assert enriched == 0
    assert "non-PDF payload" in error
    assert state == {}
    assert article["summary"] == "unchanged feed summary"
    assert not article.get("detail_attachment_enriched")


def test_boj_updates_config_enables_bounded_same_host_pdf_context():
    config = news.apply_source_config_lineage(
        json.loads(news.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    )
    source = next(
        row for row in config["sources"] if row["source_id"] == "boj_updates"
    )
    assert source["detail_follow_same_authority_pdf_attachments"] is True
    assert source["detail_attachment_max_items_per_page"] == 1
    assert source["detail_attachment_maximum_bytes"] == 2_000_000
    assert source["source_contract_id"].startswith(
        "derived_source_config_lineage_v1:boj_updates:"
    )
    assert source["source_cohort_id"] == source["source_contract_id"]
