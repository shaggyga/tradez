import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sqlite3

import pytest

import oanda_official_release_fast_lane as fast


UTC = dt.timezone.utc


def _all68_quote_payload(
    event_time: dt.datetime,
    *,
    captured_utc: dt.datetime | None = None,
) -> dict:
    captured = captured_utc or event_time + dt.timedelta(seconds=3)
    quotes = {
        instrument: {
            "bid": 150.0 if instrument.endswith("_JPY") else 1.0,
            "ask": 150.01 if instrument.endswith("_JPY") else 1.0001,
            "pip": 0.01 if instrument.endswith("_JPY") else 0.0001,
            # A current executable cache does not require all 68 instruments
            # to tick after the event.  Preserve the venue clock and prove it
            # was still fresh when the raw item arrived.
            "time": fast.iso_utc(event_time - dt.timedelta(seconds=5)),
            "source": "practice_007_fast_executor_price_stream",
        }
        for instrument in fast.EXPECTED_QUOTE_INSTRUMENTS
    }
    return {
        "schema_version": 2,
        "generated_utc": fast.iso_utc(captured - dt.timedelta(seconds=1)),
        "producer": "practice_007_fast_executor_price_stream",
        "connection_generation": 7,
        "quote_count": len(quotes),
        "quotes": quotes,
        "coverage": {
            "current_quote_count": len(quotes),
            "last_known_quote_count": len(quotes),
            "retained_last_known_count": 0,
            "retained_last_known_instruments": [],
            "connection_generation": 7,
            "retained_quotes_execution_eligible": False,
        },
        "transport": {"source": "sqlite_wal", "sequence": 101},
        "research_only": True,
    }


def test_raw_append_boundary_freezes_one_immutable_exact_all68_capture(
    monkeypatch, tmp_path: Path
):
    monkeypatch.setattr(
        fast.news,
        "prospective_clock_attestation",
        lambda clock: bool(clock.get("attested")),
    )
    event_time = fast.QUOTE_CAPTURE_ACTIVATED_UTC + dt.timedelta(minutes=5)
    captured = event_time + dt.timedelta(seconds=3)
    payload = _all68_quote_payload(event_time, captured_utc=captured)
    database = tmp_path / "fast.sqlite"
    connection = fast.open_database(database)
    loader_calls = {"count": 0}

    def load_quotes():
        loader_calls["count"] += 1
        return payload

    source = {
        "source_id": "fed_speeches",
        "source_contract_id": "fed-speeches-test-v2",
        "source_cohort_id": "fed-speeches-test-v2",
    }
    row = {
        "title": "Future policy remarks",
        "url": "https://www.federalreserve.gov/newsevents/speech/future.htm",
        "published_utc": fast.iso_utc(event_time),
    }
    try:
        first = fast.append_observations(
            connection,
            source=source,
            rows=[row],
            first_seen=event_time,
            listing_bootstrap=False,
            observation_clock={"attested": True, "source": "test_clock"},
            quote_snapshot_loader=load_quotes,
            quote_captured_utc=captured,
        )
        # The duplicate cannot replace the frozen sidecar with a later read.
        second = fast.append_observations(
            connection,
            source=source,
            rows=[row],
            first_seen=event_time + dt.timedelta(seconds=30),
            listing_bootstrap=False,
            observation_clock={"attested": True, "source": "test_clock"},
            quote_snapshot_loader=lambda: (_ for _ in ()).throw(
                AssertionError("duplicate attempted quote recapture")
            ),
            quote_captured_utc=event_time + dt.timedelta(seconds=30),
        )
        stored = connection.execute(
            """
            SELECT timing_quality, observed_valid_quote_count,
                   proof_quote_count, capture_contract_id,
                   capture_cohort_id, capture_payload_json
            FROM official_release_quote_capture
            """
        ).fetchone()
        assert stored is not None
        capture = json.loads(stored[5])
        assert stored[:3] == ("prospective_exact_live_quote", 68, 68)
        assert stored[3:5] == (
            fast.QUOTE_CAPTURE_CONTRACT_ID,
            fast.QUOTE_CAPTURE_COHORT_ID,
        )
        assert capture["instrument_universe_sha256"] == (
            fast.EXPECTED_QUOTE_UNIVERSE_SHA256
        )
        assert capture["missing_instruments"] == []
        assert capture["invalid_instruments"] == {}
        assert len(capture["quotes"]) == 68
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE official_release_quote_capture SET proof_quote_count=0"
            )
    finally:
        connection.close()

    assert first[0:2] == (1, 0)
    assert second[0:2] == (0, 1)
    assert loader_calls["count"] == 1


def test_raw_append_boundary_retains_incomplete_capture_but_never_proves_it(
    monkeypatch, tmp_path: Path
):
    monkeypatch.setattr(
        fast.news,
        "prospective_clock_attestation",
        lambda clock: bool(clock.get("attested")),
    )
    event_time = fast.QUOTE_CAPTURE_ACTIVATED_UTC + dt.timedelta(minutes=10)
    captured = event_time + dt.timedelta(seconds=2)
    payload = _all68_quote_payload(event_time, captured_utc=captured)
    missing = fast.EXPECTED_QUOTE_INSTRUMENTS[-1]
    del payload["quotes"][missing]
    payload["quote_count"] = 67
    payload["coverage"]["current_quote_count"] = 67
    payload["coverage"]["last_known_quote_count"] = 67
    connection = fast.open_database(tmp_path / "fast.sqlite")
    try:
        fast.append_observations(
            connection,
            source={
                "source_id": "fed_speeches",
                "source_contract_id": "fed-speeches-test-v2",
                "source_cohort_id": "fed-speeches-test-v2",
            },
            rows=[
                {
                    "title": "Another future policy speech",
                    "url": "https://www.federalreserve.gov/speech/incomplete.htm",
                    "published_utc": fast.iso_utc(event_time),
                }
            ],
            first_seen=event_time,
            listing_bootstrap=False,
            observation_clock={"attested": True, "source": "test_clock"},
            quote_snapshot_loader=lambda: payload,
            quote_captured_utc=captured,
        )
        capture = json.loads(
            connection.execute(
                "SELECT capture_payload_json FROM official_release_quote_capture"
            ).fetchone()[0]
        )
    finally:
        connection.close()

    assert capture["timing_quality"] == "prospective_quote_coverage_invalid"
    assert capture["observed_valid_quote_count"] == 67
    assert capture["proof_quote_count"] == 0
    assert capture["quote_count"] == 0
    assert capture["quotes"] == {}
    assert capture["missing_instruments"] == [missing]
    assert len(capture["observed_quotes"]) == 67


def test_rbnz_http_403_surfaces_are_configured_but_not_runtime_callable():
    config = fast.read_json(fast.CONFIG_PATH, {})
    sources = {
        str(row.get("source_id") or ""): row
        for row in config.get("sources") or []
    }
    blocked = {
        "new_zealand_rbnz_ocr_snapshot_direct_v1",
        "rbnz_wholesale_interest_rates",
        "rbnz_official_overseas_reserves",
    }
    assert blocked <= set(sources)
    for source_id in blocked:
        source = sources[source_id]
        # Preserve the configured first-party source and its historical
        # parser lineage, while preventing another unauthorized HTTP attempt.
        assert source["enabled"] is True
        assert source["runtime_supported"] is False
        assert "permission_required" in source["runtime_blocker"]
        assert fast.news.source_runtime_status(source) == "unsupported"
        assert source["direct"] is True
        assert source["verified"] is True

    fallback = sources["rbnz_official_search"]
    assert fallback["direct"] is False
    assert fallback["verified"] is False
    assert "google_news" in fallback["retrieval_via"]


def test_atomic_json_retries_transient_windows_replace_denial(
    monkeypatch, tmp_path: Path
):
    path = tmp_path / "state.json"
    real_replace = os.replace
    attempts = {"count": 0}

    def replace_after_denial(source, destination):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise PermissionError(5, "target temporarily busy")
        return real_replace(source, destination)

    monkeypatch.setattr(fast.os, "replace", replace_after_denial)
    fast.write_json_atomic(path, {"status": "ok"})

    assert json.loads(path.read_text(encoding="utf-8")) == {"status": "ok"}
    assert attempts["count"] == 2
    assert not list(tmp_path.glob("*.tmp"))


def test_persistent_heartbeat_replace_denial_does_not_kill_collection(
    monkeypatch, tmp_path: Path
):
    heartbeat = tmp_path / "heartbeat.json"
    heartbeat.write_text('{"status":"old"}', encoding="utf-8")
    monkeypatch.setattr(
        fast,
        "write_json_atomic",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            PermissionError(5, "target persistently busy")
        ),
    )

    fast.publish_heartbeat(
        heartbeat,
        status="running_cycle",
        phase="polling_official_sources",
        cycle_started=dt.datetime(2026, 8, 28, tzinfo=UTC),
        progress_sequence=1,
    )

    assert json.loads(heartbeat.read_text(encoding="utf-8")) == {"status": "old"}


def test_real_config_binds_all_21_currencies_and_governed_official_sources():
    config = fast.read_json(fast.CONFIG_PATH, {})
    mapping = fast.read_json(fast.CENTRAL_BANK_MAP_PATH, {})
    sources = fast.selected_sources(config, mapping)
    expected = set(fast.official_fast_lane_source_ids(mapping))

    assert len(mapping["currencies"]) == 21
    assert {row["source_id"] for row in sources} == expected
    assert len(sources) >= 21
    assert all(row.get("verified") is True for row in sources)
    assert all(row.get("direct", True) is True for row in sources)


def test_fast_lane_map_separates_policy_communication_and_statistical_transports():
    config = fast.read_json(fast.CONFIG_PATH, {})
    mapping = fast.read_json(fast.CENTRAL_BANK_MAP_PATH, {})
    assert mapping["schema_version"] == "official_central_bank_source_map_v2"
    assert mapping["contract_id"] == (
        "official_central_bank_source_map_v9_us_policy_communication_clocks_20260827"
    )
    assert all(
        "statistical_release_source_ids" in row
        for row in mapping["currencies"]
    )
    eur = next(row for row in mapping["currencies"] if row["currency"] == "EUR")
    assert eur["release_source_ids"] == ["ecb_press"]
    assert eur["statistical_release_source_ids"] == [
        "ecb_statistical_press_releases"
    ]
    selected = {
        row["source_id"]: row for row in fast.selected_sources(config, mapping)
    }
    statistical = selected["ecb_statistical_press_releases"]
    assert statistical["source_contract_id"] == (
        "ecb_statistical_press_releases_detail_v2_first_party_text_20260827"
    )
    assert statistical["source_cohort_id"] == statistical["source_contract_id"]
    assert statistical["detail_enrichment"] == "official_document_text"
    assert statistical["trusted_domains"] == ["ecb.europa.eu"]
    usd = next(row for row in mapping["currencies"] if row["currency"] == "USD")
    assert usd["release_source_ids"] == ["fed_monetary_policy"]
    assert usd["statistical_release_source_ids"] == [
        "dol_eta_ui_claims_scheduled_direct_pdf_v1",
        "census_economic_indicators",
        "bea_releases",
    ]
    dol = selected["dol_eta_ui_claims_scheduled_direct_pdf_v1"]
    assert dol["document_numeric_parser"] == "dol_weekly_claims_headline_v1"
    assert dol["execution_eligible"] is False
    census = selected["census_economic_indicators"]
    assert census["kind"] == "rss"
    assert census["burst_poll_interval_sec"] == 15
    assert census["source_contract_id"] == (
        "census_economic_indicators_numeric_v3_statistical_fast_lane_20260827"
    )
    assert census["source_cohort_id"] == census["source_contract_id"]
    assert census["directional_research_only"] is True
    assert census["execution_eligible"] is False
    assert census["trusted_domains"] == ["census.gov"]
    bea = selected["bea_releases"]
    assert bea["kind"] == "rss"
    assert bea["burst_poll_interval_sec"] == 15
    assert bea["burst_poll_windows"] == [
        {"start": "08:25", "end": "08:45"}
    ]
    assert bea["source_contract_id"] == (
        "bea_releases_statistical_fast_lane_v1_20260827"
    )
    assert bea["source_cohort_id"] == bea["source_contract_id"]
    assert bea["directional_research_only"] is True
    assert bea["execution_eligible"] is False
    assert bea["trusted_domains"] == ["bea.gov"]
    zar = next(row for row in mapping["currencies"] if row["currency"] == "ZAR")
    assert zar["release_source_ids"] == [
        "south_africa_sarb_policy_rate_direct_v1"
    ]
    assert zar["statistical_release_source_ids"] == [
        "stats_sa_ppi_scheduled_direct_pdf_v1"
    ]
    assert set(fast.official_fast_lane_source_ids(mapping)) == {
        *fast.central_bank_release_source_ids(mapping),
        *fast.central_bank_communication_source_ids(mapping),
        *fast.statistical_release_source_ids(mapping),
    }


def _stats_sa_ppi_text(
    *,
    reference_month: str = "July",
    reference_year: int = 2026,
    release_date: str = "27 August 2026",
    prior_month: str = "June",
) -> str:
    return (
        "STATISTICAL RELEASE P0142.1 "
        f"Producer Price Index {reference_month} {reference_year} "
        f"Embargoed until: {release_date} 11:30 "
        f"Annual producer price inflation (final manufacturing) was 5,7% in "
        f"{reference_month} {reference_year}, compared with 7,5% in "
        f"{prior_month} {reference_year}. "
        f"The producer price index (PPI) decreased by 1,0% month-on-month in "
        f"{reference_month} {reference_year}. "
        "Statistics South Africa official publication."
    )


def _stats_sa_probe_source() -> dict:
    config = fast.read_json(fast.CONFIG_PATH, {})
    return next(
        row
        for row in config["sources"]
        if row.get("source_id") == "stats_sa_ppi_scheduled_direct_pdf_v1"
    )


def _dol_probe_source() -> dict:
    config = fast.read_json(fast.CONFIG_PATH, {})
    return next(
        row
        for row in config["sources"]
        if row.get("source_id") == "dol_eta_ui_claims_scheduled_direct_pdf_v1"
    )


def _dol_calendar_source() -> dict:
    config = fast.read_json(fast.CONFIG_PATH, {})
    return next(
        row
        for row in config["sources"]
        if row.get("source_id") == "dol_ui_claims_release_calendar_2026_v1"
    )


def _dol_claims_text(
    *,
    release_date: str = "August 27, 2026",
    initial_actual: str = "211,000",
    initial_change: str = "5,000",
    initial_change_direction: str = "increase",
    initial_unrevised: str = "207,000",
    initial_revised: str = "206,000",
    initial_revision: str = "1,000",
    initial_revision_direction: str = "down",
    insured_rate_sentence: str = (
        "The advance seasonally adjusted insured unemployment rate was 1.2 "
        "percent for the week ending August 15, unchanged from the previous "
        "week's unrevised rate. "
    ),
) -> str:
    return (
        "TRANSMISSION OF MATERIALS IN THIS RELEASE IS EMBARGOED UNTIL "
        f"8:30 A.M. (Eastern) Thursday, {release_date} "
        "UNEMPLOYMENT INSURANCE WEEKLY CLAIMS SEASONALLY ADJUSTED DATA "
        "In the week ending August 22, the advance figure for seasonally adjusted "
        f"initial claims was {initial_actual}, an {initial_change_direction} of "
        f"{initial_change} from the previous week's revised level. The previous "
        f"week's level was revised {initial_revision_direction} by "
        f"{initial_revision} from {initial_unrevised} to {initial_revised}. "
        f"{insured_rate_sentence}"
        "The advance number for seasonally adjusted "
        "insured unemployment during the week ending August 15 was 1,805,000, "
        "an increase of 6,000 from the previous week's revised level. The previous "
        "week's level was revised up 2,000 from 1,797,000 to 1,799,000."
    )


def test_stats_sa_direct_pdf_probe_is_exact_scheduled_and_calendar_aligned():
    config = fast.read_json(fast.CONFIG_PATH, {})
    mapping = fast.read_json(fast.CENTRAL_BANK_MAP_PATH, {})
    source = _stats_sa_probe_source()
    calendar = next(
        row
        for row in config["sources"]
        if row.get("source_id") == "stats_sa_ppi_release_calendar_2026_v1"
    )
    zar = next(row for row in mapping["currencies"] if row["currency"] == "ZAR")

    assert source["source_id"] in zar["statistical_release_source_ids"]
    assert source["kind"] == "scheduled_official_pdf"
    assert source["runtime_supported"] is False
    assert source["required_runtime_contract_id"] == (
        fast.news.SCHEDULED_OFFICIAL_PDF_PROBE_CONTRACT_ID
    )
    assert fast.news.source_runtime_status(source) == "enabled"
    assert fast.news.source_runtime_status(
        {**source, "required_runtime_contract_id": "unknown_old_runtime"}
    ) == "unsupported"
    assert source["exact_document_host"] == "www.statssa.gov.za"
    assert source["stop_after_first_valid_capture"] is True
    assert source["directional_research_only"] is True
    assert source["research_only"] is True
    assert source["proof_eligible"] is False
    assert source["promotion_eligible"] is False
    assert source["execution_eligible"] is False
    assert source["can_authorize"] is False
    assert [
        (row["event_name"], row["event_series_id"], row["local_date"], row["local_time"])
        for row in source["explicit_schedule"]
    ] == [
        (row["event_name"], row["event_series_id"], row["local_date"], row["local_time"])
        for row in calendar["explicit_schedule"]
    ]

    event = fast.news.scheduled_official_pdf_event(
        source,
        now=dt.datetime(2026, 8, 27, 9, 30, 36, tzinfo=UTC),
    )
    assert event is not None
    assert event["scheduled_utc"] == "2026-08-27T09:30:00+00:00"
    assert fast.news.parse_datetime(event["scheduled_utc"]).astimezone(
        fast.news.ZoneInfo("America/New_York")
    ).strftime("%Y-%m-%d %H:%M %z") == "2026-08-27 05:30 -0400"
    assert event["reference_period"] == "2026-07"
    assert fast.news.scheduled_official_pdf_url(source, event) == (
        "https://www.statssa.gov.za/publications/P01421/P01421July2026.pdf"
    )
    assert fast.news.scheduled_official_pdf_event(
        source,
        now=dt.datetime(2026, 8, 28, 9, 30, tzinfo=UTC),
    ) is None


def test_scheduled_pdf_url_layer_supports_one_fixed_official_url_for_dol_extension():
    source = _dol_probe_source()
    calendar = _dol_calendar_source()
    event = fast.news.scheduled_official_pdf_event(
        source,
        now=dt.datetime(2026, 8, 27, 12, 30, 10, tzinfo=UTC),
    )
    assert event is not None
    assert event["scheduled_utc"] == "2026-08-27T12:30:00+00:00"
    assert fast.news.scheduled_official_pdf_url(source, event) == (
        "https://www.dol.gov/ui/data.pdf"
    )
    assert source["runtime_supported"] is False
    assert fast.news.source_runtime_status(source) == "enabled"
    assert source["stop_after_first_valid_capture"] is False
    assert source["document_numeric_parser"] == "dol_weekly_claims_headline_v1"
    assert source["directional_research_only"] is True
    assert source["execution_eligible"] is False
    assert source["explicit_schedule"] == calendar["explicit_schedule"]
    events = fast.news.build_recurring_release_calendar(
        calendar,
        now=dt.datetime(2026, 8, 27, 10, 0, tzinfo=UTC),
    )
    event_row = next(
        row for row in events if row["scheduled_utc"] == "2026-08-27T12:30:00+00:00"
    )
    assert event_row["event_series_id"] == "dol_ui_weekly_claims_bundle"
    assert event_row["reference_period"] == "2026-08-22"
    assert event_row["actual_value"] is None
    assert event_row["consensus_value"] is None


def test_dol_weekly_claims_parser_extracts_revision_components_and_abstains():
    source = _dol_probe_source()
    event = fast.news.scheduled_official_pdf_event(
        source,
        now=dt.datetime(2026, 8, 27, 12, 30, 10, tzinfo=UTC),
    )
    assert event is not None
    parsed = fast.news.parse_dol_weekly_claims_pdf_text(
        _dol_claims_text(), source=source, event=event
    )
    assert parsed["embedded_release_utc"] == "2026-08-27T12:30:00+00:00"
    assert parsed["actual_value"] == 211000.0
    assert parsed["previous_value"] == 206000.0
    assert parsed["unrevised_previous_value"] == 207000.0
    assert parsed["revision_raw"] == -1000.0
    initial = parsed["source_native_components"]["initial_claims_sa"]
    assert initial["weekly_change"] == 5000
    continued = parsed["source_native_components"]["insured_unemployment_sa"]
    assert continued["actual"] == 1805000
    assert continued["revision"] == 2000
    rate = parsed["source_native_components"]["insured_unemployment_rate_sa"]
    assert rate["actual"] == 1.2
    assert rate["previous_unrevised"] == 1.2
    assert rate["weekly_change"] == 0.0
    assert len(parsed["release_components"]) == 3

    with pytest.raises(ValueError, match="does not match schedule"):
        fast.news.parse_dol_weekly_claims_pdf_text(
            _dol_claims_text(release_date="August 20, 2026"),
            source=source,
            event=event,
        )
    with pytest.raises(ValueError, match="arithmetic is inconsistent"):
        fast.news.parse_dol_weekly_claims_pdf_text(
            _dol_claims_text(initial_actual="212,000"),
            source=source,
            event=event,
        )

    changed = fast.news.parse_dol_weekly_claims_pdf_text(
        _dol_claims_text(
            insured_rate_sentence=(
                "The advance seasonally adjusted insured unemployment rate was "
                "1.2 percent for the week ending August 15, a decrease of 0.1 "
                "percentage point from the previous week's unrevised rate. "
            )
        ),
        source=source,
        event=event,
    )
    changed_rate = changed["source_native_components"][
        "insured_unemployment_rate_sa"
    ]
    assert changed_rate["previous_unrevised"] == pytest.approx(1.3)
    assert changed_rate["weekly_change"] == pytest.approx(-0.1)


def test_dol_weekly_claims_parser_accepts_official_unrevised_and_unchanged_forms():
    source = _dol_probe_source()
    event = fast.news.scheduled_official_pdf_event(
        source,
        now=dt.datetime(2026, 8, 27, 12, 30, 10, tzinfo=UTC),
    )
    assert event is not None

    unrevised = _dol_claims_text().replace(
        "211,000, an increase of 5,000 from the previous week's revised level. "
        "The previous week's level was revised down by 1,000 from 207,000 to 206,000.",
        "211,000, an increase of 5,000 from the previous week's unrevised level of 206,000.",
    ).replace(
        "1,805,000, an increase of 6,000 from the previous week's revised level. "
        "The previous week's level was revised up 2,000 from 1,797,000 to 1,799,000.",
        "1,805,000, an increase of 6,000 from the previous week's unrevised level of 1,799,000.",
    )
    parsed = fast.news.parse_dol_weekly_claims_pdf_text(
        unrevised, source=source, event=event
    )
    initial = parsed["source_native_components"]["initial_claims_sa"]
    continued = parsed["source_native_components"]["insured_unemployment_sa"]
    assert initial["previous_revised"] == 206000
    assert initial["previous_unrevised"] == 206000
    assert initial["revision"] == 0
    assert continued["previous_revised"] == 1799000
    assert continued["revision"] == 0

    unchanged = _dol_claims_text(initial_actual="206,000").replace(
        "an increase of 5,000 from the previous week's revised level",
        "unchanged from the previous week's revised level",
    )
    parsed = fast.news.parse_dol_weekly_claims_pdf_text(
        unchanged, source=source, event=event
    )
    assert parsed["source_native_components"]["initial_claims_sa"]["weekly_change"] == 0


def test_dol_probe_holds_embargo_then_archives_prospective_null_consensus(
    monkeypatch, tmp_path: Path
):
    source = _dol_probe_source()
    document_url = "https://www.dol.gov/ui/data.pdf"
    payloads = [
        b"%PDF-1.7\nfixture DOL claims document v1\n%%EOF\n",
        b"%PDF-1.7\nfixture DOL claims document v2 corrected\n%%EOF\n",
    ]
    calls = {"count": 0}

    class Response:
        status = 200
        headers = {
            "Content-Type": "application/pdf",
            "ETag": '"dol-fixture"',
            "Last-Modified": "Thu, 27 Aug 2026 12:30:02 GMT",
        }

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return document_url

        def read(self, maximum):
            assert maximum == source["direct_pdf_maximum_bytes"] + 1
            return payloads[min(calls["count"] - 1, 1)]

    def open_fixture(*_args, **_kwargs):
        calls["count"] += 1
        return Response()

    def forbidden_fetch(*_args, **_kwargs):
        raise AssertionError("pre-release DOL fetch was attempted")

    monkeypatch.setattr(fast.news.urllib.request, "urlopen", forbidden_fetch)
    rows, held = fast.news.fetch_scheduled_official_pdf(
        source,
        {},
        timeout_sec=2.0,
        maximum_bytes=8_000_000,
        now=dt.datetime(2026, 8, 27, 12, 29, 30, tzinfo=UTC),
    )
    assert rows == []
    assert held["scheduled_pdf_probe_state"] == "pre_release_clock_hold_no_fetch"

    monkeypatch.setattr(fast.news.urllib.request, "urlopen", open_fixture)
    monkeypatch.setattr(
        fast.news,
        "extract_official_document_text",
        lambda *_args, **_kwargs: (_dol_claims_text(), "official_pdf_text"),
    )
    monkeypatch.setattr(fast.news, "DEFAULT_OUTPUT_ROOT", tmp_path)
    observed = dt.datetime(2026, 8, 27, 12, 30, 5, tzinfo=UTC)
    rows, state = fast.news.fetch_scheduled_official_pdf(
        source,
        held,
        timeout_sec=2.0,
        maximum_bytes=8_000_000,
        now=observed,
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["actual_value"] == 211000.0
    assert row["previous_value"] == 206000.0
    assert row["consensus_value"] is None
    assert row["direction"] is None
    assert row["numeric_direction_policy"] == (
        "abstain_until_causal_consensus_and_rate_repricing"
    )
    assert row["evidence_classification"] == (
        "prospective_research_observation_unvalidated"
    )
    assert row["source_listing_bootstrap"] is False
    assert row["document_revision_number"] == 1
    assert row["is_material_revision"] is False
    assert row["research_only"] is True
    assert row["execution_eligible"] is False
    assert row["can_authorize"] is False
    assert Path(row["detail_archive_path"]).read_bytes() == payloads[0]

    classified = fast.news.classify_article(
        row, first_seen=observed + dt.timedelta(seconds=1)
    )
    assert classified["currency_scores"] == {}
    assert classified["consensus_value"] is None
    assert classified["surprise_raw"] is None
    assert classified["directional_publish_eligible"] is False
    assert classified["execution_eligible"] is False

    revision_rows, revision_state = fast.news.fetch_scheduled_official_pdf(
        source,
        state,
        timeout_sec=2.0,
        maximum_bytes=8_000_000,
        now=observed + dt.timedelta(seconds=15),
    )
    assert len(revision_rows) == 1
    revision = revision_rows[0]
    assert revision["document_revision_number"] == 2
    assert revision["is_material_revision"] is True
    assert revision["supersedes_material_content_sha256"] == hashlib.sha256(
        payloads[0]
    ).hexdigest()
    assert revision["revision_id"]
    assert Path(revision["detail_archive_path"]).read_bytes() == payloads[1]
    assert len(revision_state["observed_pdf_sha256_by_event"][revision["external_id"]]) == 2


def test_scheduled_pdf_probe_never_fetches_before_official_embargo(monkeypatch):
    source = _stats_sa_probe_source()

    def forbidden_fetch(*_args, **_kwargs):
        raise AssertionError("pre-release official PDF fetch was attempted")

    monkeypatch.setattr(fast.news.urllib.request, "urlopen", forbidden_fetch)
    rows, state = fast.news.fetch_scheduled_official_pdf(
        source,
        {},
        timeout_sec=2.0,
        maximum_bytes=8_000_000,
        now=dt.datetime(2026, 8, 27, 9, 29, 30, tzinfo=UTC),
    )
    assert rows == []
    assert state["scheduled_pdf_probe_state"] == (
        "pre_release_clock_hold_no_fetch"
    )
    assert state["active_scheduled_utc"] == "2026-08-27T09:30:00+00:00"


def test_scheduled_pdf_expected_404_retries_without_error_backoff(monkeypatch):
    source = _stats_sa_probe_source()
    document_url = (
        "https://www.statssa.gov.za/publications/P01421/P01421July2026.pdf"
    )

    def not_published(*_args, **_kwargs):
        raise fast.news.urllib.error.HTTPError(
            document_url,
            404,
            "Not Found",
            {},
            None,
        )

    monkeypatch.setattr(fast.news.urllib.request, "urlopen", not_published)
    rows, state = fast.news.fetch_scheduled_official_pdf(
        source,
        {},
        timeout_sec=2.0,
        maximum_bytes=8_000_000,
        now=dt.datetime(2026, 8, 27, 9, 30, 0, tzinfo=UTC),
    )
    assert rows == []
    assert state["last_status"] == 404
    assert state["last_error"] == ""
    assert state["consecutive_errors"] == 0
    assert state["scheduled_pdf_probe_state"] == "not_yet_published"


def test_stats_sa_pdf_parser_extracts_exact_headline_values_and_rejects_ambiguity():
    source = _stats_sa_probe_source()
    event = fast.news.scheduled_official_pdf_event(
        source,
        now=dt.datetime(2026, 8, 27, 9, 30, 36, tzinfo=UTC),
    )
    assert event is not None
    parsed = fast.news.parse_stats_sa_ppi_pdf_text(
        _stats_sa_ppi_text(),
        source=source,
        event=event,
    )
    assert parsed["actual_value"] == 5.7
    assert parsed["previous_value"] == 7.5
    assert parsed["embedded_release_utc"] == "2026-08-27T09:30:00+00:00"
    assert parsed["source_native_components"][
        "headline_final_manufacturing_ppi_mom"
    ]["actual"] == -1.0
    assert len(parsed["release_components"]) == 2

    duplicate = _stats_sa_ppi_text() + " " + _stats_sa_ppi_text()
    with pytest.raises(ValueError, match="missing or ambiguous"):
        fast.news.parse_stats_sa_ppi_pdf_text(
            duplicate,
            source=source,
            event=event,
        )


def test_stats_sa_probe_archives_hashes_and_quarantines_today_as_bootstrap(
    monkeypatch, tmp_path: Path
):
    source = _stats_sa_probe_source()
    document_url = (
        "https://www.statssa.gov.za/publications/P01421/P01421July2026.pdf"
    )
    payload = b"%PDF-1.7\nfixture official Stats SA document\n%%EOF\n"
    calls = {"count": 0}

    class Response:
        status = 200
        headers = {
            "Content-Type": "application/pdf",
            "ETag": '"fixture"',
            "Last-Modified": "Thu, 27 Aug 2026 09:30:14 GMT",
        }

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return document_url

        def read(self, maximum):
            assert maximum == source["direct_pdf_maximum_bytes"] + 1
            return payload

    def open_once(*_args, **_kwargs):
        calls["count"] += 1
        return Response()

    monkeypatch.setattr(fast.news.urllib.request, "urlopen", open_once)
    monkeypatch.setattr(
        fast.news,
        "extract_official_document_text",
        lambda *_args, **_kwargs: (_stats_sa_ppi_text(), "official_pdf_text"),
    )
    monkeypatch.setattr(fast.news, "DEFAULT_OUTPUT_ROOT", tmp_path)
    observed = dt.datetime(2026, 8, 27, 9, 30, 36, tzinfo=UTC)
    rows, state = fast.news.fetch_scheduled_official_pdf(
        source,
        {},
        timeout_sec=2.0,
        maximum_bytes=8_000_000,
        now=observed,
    )
    assert calls["count"] == 1
    assert len(rows) == 1
    row = rows[0]
    digest = hashlib.sha256(payload).hexdigest()
    archive = Path(row["detail_archive_path"])
    assert archive.read_bytes() == payload
    assert row["detail_content_sha256"] == digest
    assert row["material_content_sha256"] == digest
    assert row["detail_available_utc"] == "2026-08-27T09:30:36+00:00"
    assert row["numeric_causal_known_utc"] == "2026-08-27T10:00:00+00:00"
    assert row["document_server_last_modified_utc"] == (
        "2026-08-27T09:30:14+00:00"
    )
    assert row["evidence_classification"] == (
        "engineering_bootstrap_recovery_not_proof"
    )
    assert row["engineering_gap_observed_pdf_live_by_utc"] == (
        "2026-08-27T09:30:36Z"
    )
    assert row["consensus"] is None
    assert row["direction"] is None
    assert row["research_only"] is True
    assert row["proof_eligible"] is False
    assert row["execution_eligible"] is False
    assert row["can_authorize"] is False
    assert fast.observation_batch_is_bootstrap(
        initializing_source=False,
        rows=[row],
    ) is True
    assert state["last_document_sha256"] == digest
    assert state["scheduled_pdf_probe_state"] == "valid_capture_archived"

    classified = fast.news.classify_article(
        row,
        first_seen=observed + dt.timedelta(seconds=1),
    )
    assert classified["category"] == "inflation_release"
    assert classified["currency_scores"] == {}
    assert classified["consensus_value"] is None
    assert classified["surprise_raw"] is None
    assert classified["directional_publish_eligible"] is False
    assert classified["execution_eligible"] is False

    rows_again, state_again = fast.news.fetch_scheduled_official_pdf(
        source,
        state,
        timeout_sec=2.0,
        maximum_bytes=8_000_000,
        now=observed + dt.timedelta(seconds=15),
    )
    assert calls["count"] == 1
    assert rows_again == []
    assert state_again["scheduled_pdf_probe_state"] == "event_already_captured"

    clock = {
        "contract_id": fast.news.OBSERVATION_TIME_CONTRACT_ID,
        "trusted_for_prospective_evidence": True,
        "source": "test_exact_clock",
    }
    with fast.open_database(tmp_path / "fast.sqlite") as connection:
        inserted, _, _ = fast.append_observations(
            connection,
            source=source,
            rows=[row],
            first_seen=observed + dt.timedelta(seconds=1),
            listing_bootstrap=True,
            observation_clock=clock,
        )
        stored = connection.execute(
            "SELECT prospective_observation, listing_bootstrap, research_only, "
            "execution_eligible, can_authorize FROM official_release_observation"
        ).fetchone()
    assert inserted == 1
    assert stored == (0, 1, 1, 0, 0)


def test_fast_lane_reuses_main_collector_derived_source_lineage(tmp_path: Path):
    config = fast.read_json(fast.CONFIG_PATH, {})
    mapping = fast.read_json(fast.CENTRAL_BANK_MAP_PATH, {})
    main_config = fast.news.apply_source_config_lineage(config)
    main_sources = {row["source_id"]: row for row in main_config["sources"]}
    sources = fast.selected_sources(config, mapping)
    for source in sources:
        expected = main_sources[source["source_id"]]
        assert source["source_contract_id"] == expected["source_contract_id"]
        assert source["source_cohort_id"] == expected["source_cohort_id"]
        assert source.get("source_config_sha256", "") == expected.get(
            "source_config_sha256", ""
        )

    derived = next(row for row in sources if row["source_id"] == "boj_updates")
    with fast.open_database(tmp_path / "fast.sqlite") as connection:
        inserted, _, _ = fast.append_observations(
            connection,
            source=derived,
            rows=[{
                "headline": "Policy statement",
                "source_url": "https://www.boj.or.jp/en/example.htm",
                "published_utc": "2026-08-27T04:30:00Z",
            }],
            first_seen=dt.datetime(2026, 8, 27, 4, 31, tzinfo=UTC),
            listing_bootstrap=True,
            observation_clock={"source": "test", "attested": False},
        )
        stored = connection.execute(
            "SELECT source_contract_id,source_cohort_id "
            "FROM official_release_observation"
        ).fetchone()
    assert inserted == 1
    assert stored == (
        derived["source_contract_id"], derived["source_cohort_id"]
    )


def test_fast_lane_lineage_adoption_does_not_reset_poll_state(
    monkeypatch, tmp_path: Path
):
    config = fast.read_json(fast.CONFIG_PATH, {})
    mapping = fast.read_json(fast.CENTRAL_BANK_MAP_PATH, {})
    current = dt.datetime(2026, 8, 27, 4, 35, tzinfo=UTC)
    prior = {
        "last_attempt_utc": (current - dt.timedelta(seconds=30)).isoformat(),
        "last_success_utc": (current - dt.timedelta(seconds=30)).isoformat(),
        "last_status": 200,
        "etag": '"unchanged"',
        "last_modified": "Thu, 27 Aug 2026 04:00:00 GMT",
        "known_item_urls": ["https://www.boj.or.jp/en/known.htm"],
        "detail_first_seen_utc_by_url": {
            "https://www.boj.or.jp/en/known.htm": "2026-08-27T04:00:00Z"
        },
        "fast_lane_bootstrap_complete": True,
    }
    monkeypatch.setattr(fast, "due_for_fast_poll", lambda *_a, **_k: False)
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps({"sources": {"boj_updates": prior}}), encoding="utf-8"
    )
    config_path = tmp_path / "news.json"
    map_path = tmp_path / "map.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    map_path.write_text(json.dumps(mapping), encoding="utf-8")
    result = fast.run_cycle(
        config_path=config_path,
        central_bank_map_path=map_path,
        database_path=tmp_path / "fast.sqlite",
        state_path=state_path,
        snapshot_path=tmp_path / "snapshot.json",
        heartbeat_path=tmp_path / "heartbeat.json",
        now=current,
    )
    assert result["attempted_sources"] == 0
    stored = json.loads(state_path.read_text(encoding="utf-8"))["sources"][
        "boj_updates"
    ]
    assert stored["source_contract_id"].startswith(
        "derived_source_config_lineage_v1:boj_updates:"
    )
    assert stored["operational_status"] == "enabled"
    for key in (
        "last_attempt_utc", "last_success_utc", "last_status", "etag",
        "last_modified", "known_item_urls", "detail_first_seen_utc_by_url",
        "fast_lane_bootstrap_complete",
    ):
        assert stored[key] == prior[key]


def test_bootstrap_is_noncausal_and_later_new_material_is_prospective(
    monkeypatch, tmp_path: Path
):
    config = fast.read_json(fast.CONFIG_PATH, {})
    mapping = fast.read_json(fast.CENTRAL_BANK_MAP_PATH, {})
    current = dt.datetime(2026, 8, 28, 15, 1, tzinfo=UTC)
    phase = {"value": 1}

    monkeypatch.setattr(fast.news, "source_runtime_status", lambda source: "enabled")
    monkeypatch.setattr(
        fast.news,
        "normalized_observation_time",
        lambda value: (
            value,
            {"source": "test_exact_clock", "attested": True},
        ),
    )
    monkeypatch.setattr(
        fast.news,
        "prospective_clock_attestation",
        lambda clock: bool(clock.get("attested")),
    )

    def fake_fetch(source, prior, *, timeout_sec, maximum_bytes, now):
        suffix = ""
        if phase["value"] == 2 and source["source_id"] == "fed_monetary_policy":
            suffix = " updated"
        if phase["value"] == 3 and source["source_id"] == "fed_monetary_policy":
            suffix = " old new identity"
        if phase["value"] == 4 and source["source_id"] == "fed_monetary_policy":
            suffix = " current new identity"
        source_url = f"https://example.test/{source['source_id']}"
        if phase["value"] == 3 and source["source_id"] == "fed_monetary_policy":
            source_url += "/old-new-release"
        if phase["value"] == 4 and source["source_id"] == "fed_monetary_policy":
            source_url += "/current-new-release"
        published_utc = "2026-04-30T11:00:00+00:00"
        if phase["value"] == 4 and source["source_id"] == "fed_monetary_policy":
            published_utc = "2026-08-28T15:08:00+00:00"
        row = {
            "headline": f"{source['source_id']} decision{suffix}",
            "source_url": source_url,
            "published_utc": published_utc,
            "summary": f"policy statement{suffix}",
        }
        updated = dict(prior)
        updated.update(
            {
                "last_attempt_utc": now.isoformat(),
                "last_success_utc": now.isoformat(),
                "last_status": 200,
                "last_error": "",
                "consecutive_errors": 0,
                "source_contract_id": source.get("source_contract_id", ""),
            }
        )
        return [row], updated

    monkeypatch.setattr(fast.news, "fetch_source", fake_fetch)
    config_path = tmp_path / "news.json"
    map_path = tmp_path / "map.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    map_path.write_text(json.dumps(mapping), encoding="utf-8")
    paths = {
        "database_path": tmp_path / "fast.sqlite",
        "state_path": tmp_path / "state.json",
        "snapshot_path": tmp_path / "snapshot.json",
        "heartbeat_path": tmp_path / "heartbeat.json",
    }

    first = fast.run_cycle(
        config_path=config_path,
        central_bank_map_path=map_path,
        now=current,
        force=True,
        **paths,
    )
    assert first["inserted_observations"] == len(
        fast.official_fast_lane_source_ids(mapping)
    )
    assert first["source_map_contract_id"] == mapping["contract_id"]
    saved_state = json.loads(paths["state_path"].read_text(encoding="utf-8"))
    assert saved_state["source_map_contract_id"] == mapping["contract_id"]
    assert first["counts"]["prospective_observations"] == 0
    assert first["counts"]["bootstrap_observations"] == first["counts"]["observations"]

    phase["value"] = 2
    second = fast.run_cycle(
        config_path=config_path,
        central_bank_map_path=map_path,
        now=current + dt.timedelta(minutes=3),
        force=True,
        **paths,
    )
    assert second["inserted_observations"] == 1
    assert second["counts"]["prospective_observations"] == 0
    assert second["counts"]["observations"] == first["counts"]["observations"] + 1

    phase["value"] = 3
    third = fast.run_cycle(
        config_path=config_path,
        central_bank_map_path=map_path,
        now=current + dt.timedelta(minutes=6),
        force=True,
        **paths,
    )
    assert third["inserted_observations"] == 1
    assert third["counts"]["prospective_observations"] == 0

    phase["value"] = 4
    fourth = fast.run_cycle(
        config_path=config_path,
        central_bank_map_path=map_path,
        now=current + dt.timedelta(minutes=9),
        force=True,
        **paths,
    )
    assert fourth["inserted_observations"] == 1
    assert fourth["counts"]["prospective_observations"] == 1

    with sqlite3.connect(paths["database_path"]) as connection:
        row = connection.execute(
            """
            SELECT research_only, execution_eligible, can_authorize,
                   observation_clock_trusted, prospective_observation
            FROM official_release_observation
            WHERE prospective_observation = 1
            """
        ).fetchone()
    assert row == (1, 0, 0, 1, 1)


def test_fast_lane_never_appends_plain_rss_downgrade_after_official_detail(
    tmp_path: Path,
):
    source = {
        "source_id": "ecb_statistical_press_releases",
        "source_contract_id": (
            "ecb_statistical_press_releases_detail_v2_first_party_text_20260827"
        ),
        "source_cohort_id": (
            "ecb_statistical_press_releases_detail_v2_first_party_text_20260827"
        ),
    }
    url = (
        "https://www.ecb.europa.eu/press/stats/md/html/"
        "ecb.md2607~e7127e7d02.en.html"
    )
    common = {
        "title": "Monetary developments in the euro area: July 2026",
        "url": url,
        "published_utc": "2026-08-27T08:00:00Z",
    }
    database = tmp_path / "fast.sqlite"
    with fast.open_database(database) as connection:
        inserted, duplicates, _ = fast.append_observations(
            connection,
            source=source,
            rows=[{
                **common,
                "summary": "Official M3 and lending detail retained here.",
                "detail_enriched": True,
                "detail_available_utc": "2026-08-27T08:28:47Z",
            }],
            first_seen=dt.datetime(2026, 8, 27, 8, 28, 48, tzinfo=UTC),
            listing_bootstrap=True,
            observation_clock={"source": "test", "attested": True},
        )
        assert (inserted, duplicates) == (1, 0)
        inserted, duplicates, _ = fast.append_observations(
            connection,
            source=source,
            rows=[{**common, "summary": "", "detail_enriched": False}],
            first_seen=dt.datetime(2026, 8, 27, 8, 31, tzinfo=UTC),
            listing_bootstrap=False,
            observation_clock={"source": "test", "attested": True},
        )
        assert (inserted, duplicates) == (0, 1)
        retained = connection.execute(
            "SELECT COUNT(*),raw_payload_json "
            "FROM official_release_observation"
        ).fetchone()
    assert retained[0] == 1
    assert json.loads(retained[1])["detail_enriched"] is True


def test_due_poll_respects_interval_and_burst(monkeypatch):
    now = dt.datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    source = {
        "source_id": "test",
        "poll_interval_sec": 180,
        "source_contract_id": "v1",
    }
    state = {
        "source_contract_id": "v1",
        "last_attempt_utc": (now - dt.timedelta(seconds=60)).isoformat(),
    }
    monkeypatch.setattr(fast.news, "burst_poll_active", lambda source, now: False)
    assert fast.due_for_fast_poll(source, state, {}, now) is False
    assert fast.due_for_fast_poll(
        source,
        {**state, "last_attempt_utc": (now - dt.timedelta(seconds=181)).isoformat()},
        {},
        now,
    ) is True


def test_module_has_no_broker_or_authorization_surface():
    text = Path(fast.__file__).read_text(encoding="utf-8")
    forbidden = [
        "create_order",
        "close_trade",
        "OANDA_CREDS",
        "practice_007_canary_authorization",
        "execution_eligible\": True",
    ]
    assert all(token not in text for token in forbidden)
    assert fast.CONTRACT_ID.startswith("official_release_fast_lane_v4_")


def test_fast_lane_is_hidden_supervised_and_vault_reproducible():
    supervisor = (fast.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    vault_sync = (fast.ROOT / "forex_model_vault_sync.py").read_text(
        encoding="utf-8"
    )
    assert '-Name "official_release_fast_lane"' in supervisor
    assert 'oanda_official_release_fast_lane.py' in supervisor
    assert 'official_release_fast_lane_heartbeat_v4.json' in supervisor
    assert 'Path("trad/oanda_official_release_fast_lane.py")' in vault_sync
    assert 'Path("trad/test_oanda_official_release_fast_lane.py")' in vault_sync
