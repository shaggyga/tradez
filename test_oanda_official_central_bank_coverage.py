from __future__ import annotations

import copy
import datetime as dt

import oanda_official_central_bank_coverage as audit


NOW = dt.datetime(2026, 8, 17, 22, 30, tzinfo=dt.timezone.utc)


def real_inputs():
    return {
        "mapping": audit.news.load_json(audit.DEFAULT_MAP, {}),
        "source_config": audit.news.load_json(audit.DEFAULT_SOURCES, {}),
        "collector_state": audit.news.load_json(audit.DEFAULT_COLLECTOR_STATE, {}),
        "linked_config": audit.news.load_json(audit.DEFAULT_LINKS, {}),
        "instruments": audit.event_tagger.discover_instruments(),
        "as_of": NOW,
    }


def test_canonical_contract_maps_all_21_currencies_and_68_pairs() -> None:
    report = audit.build_report(**real_inputs())

    assert report["global_blockers"] == []
    assert report["schema_version"] == "official_central_bank_coverage_v2"
    assert report["contract_complete"] is True
    assert report["minimum_operational_complete"] is True
    assert report["statistical_release_contract_complete"] is True
    assert report["statistical_release_summary"]["mapped_sources"] == 5
    assert report["statistical_release_summary"]["operational"] == 5
    assert report["statistical_release_summary"]["blockers"] == 0
    assert report["currency_summary"]["configured_complete"] == 21
    assert report["currency_summary"]["release_operational"] == 21
    assert report["currency_summary"]["schedule_operational"] == 21
    assert report["pair_summary"]["emitted"] == 68
    assert report["pair_summary"]["both_legs_configured"] == 68
    assert report["pair_summary"]["both_legs_operational"] == 68
    assert set(report["currencies"]) == set(audit.EXPECTED_CURRENCIES)


def test_aggregator_cannot_satisfy_official_bank_contract() -> None:
    inputs = real_inputs()
    source_config = copy.deepcopy(inputs["source_config"])
    rba = next(row for row in source_config["sources"] if row.get("source_id") == "rba_media")
    rba["direct"] = False
    rba["verified"] = False
    inputs["source_config"] = source_config

    report = audit.build_report(**inputs)
    aud = report["currencies"]["AUD"]

    assert aud["configured_complete"] is False
    assert "not_verified_first_party:rba_media" in aud["blockers"]
    assert report["contract_complete"] is False


def test_ecb_statistical_fast_lane_source_cannot_substitute_for_policy_release() -> None:
    inputs = real_inputs()
    mapping = copy.deepcopy(inputs["mapping"])
    eur = next(row for row in mapping["currencies"] if row["currency"] == "EUR")
    assert eur["release_source_ids"] == ["ecb_press"]
    assert eur["statistical_release_source_ids"] == [
        "ecb_statistical_press_releases"
    ]
    inputs["mapping"] = mapping
    report = audit.build_report(**inputs)
    assert report["currencies"]["EUR"]["configured_complete"] is True
    assert report["currencies"]["EUR"]["statistical_release_sources"][0][
        "source_role"
    ] == "primary_central_bank_statistical_release"

    eur["release_source_ids"] = []
    report = audit.build_report(**inputs)
    assert report["currencies"]["EUR"]["configured_complete"] is False
    assert "missing_direct_policy_release_source" in report["currencies"]["EUR"]["blockers"]


def test_national_statistics_fast_lane_sources_do_not_contaminate_bank_coverage() -> None:
    report = audit.build_report(**real_inputs())

    usd = report["currencies"]["USD"]
    zar = report["currencies"]["ZAR"]
    assert [row["source_id"] for row in usd["release_sources"]] == [
        "fed_monetary_policy"
    ]
    assert [row["source_id"] for row in usd["statistical_release_sources"]] == [
        "dol_eta_ui_claims_scheduled_direct_pdf_v1",
        "census_economic_indicators",
        "bea_releases",
    ]
    census = next(
        row
        for row in usd["statistical_release_sources"]
        if row["source_id"] == "census_economic_indicators"
    )
    assert census["source_role"] == "primary_statistical_release"
    assert "census_economic_indicators" not in {
        row["source_id"] for row in usd["release_sources"]
    }
    assert [row["source_id"] for row in zar["release_sources"]] == [
        "south_africa_sarb_policy_rate_direct_v1"
    ]
    assert [row["source_id"] for row in zar["statistical_release_sources"]] == [
        "stats_sa_ppi_scheduled_direct_pdf_v1"
    ]
    assert usd["configured_complete"] is True
    assert zar["configured_complete"] is True
    assert usd["blockers"] == []
    assert zar["blockers"] == []


def test_us_policy_communications_and_event_clock_do_not_replace_fomc_calendar() -> None:
    inputs = real_inputs()
    report = audit.build_report(**inputs)
    usd = report["currencies"]["USD"]

    assert "kansas_city_fed_news_releases" in {
        row["source_id"] for row in usd["communication_sources"]
    }
    assert {row["source_id"] for row in usd["calendar_sources"]} == {
        "fomc_policy_decision_calendar_2026",
        "kansas_city_fed_jackson_hole_calendar_2026",
    }
    assert [
        row["source_id"] for row in usd["policy_decision_calendar_sources"]
    ] == ["fomc_policy_decision_calendar_2026"]
    assert [
        row["source_id"] for row in usd["communication_calendar_sources"]
    ] == ["kansas_city_fed_jackson_hole_calendar_2026"]

    # A healthy communications event clock must not mask a broken monetary-
    # policy decision calendar in policy-coverage readiness.
    source_config = copy.deepcopy(inputs["source_config"])
    fomc = next(
        row
        for row in source_config["sources"]
        if row.get("source_id") == "fomc_policy_decision_calendar_2026"
    )
    fomc["enabled"] = False
    inputs["source_config"] = source_config
    degraded = audit.build_report(**inputs)["currencies"]["USD"]
    assert degraded["schedule_operational"] is False


def test_statistical_transport_failure_is_reported_separately() -> None:
    inputs = real_inputs()
    source_config = copy.deepcopy(inputs["source_config"])
    source = next(
        row
        for row in source_config["sources"]
        if row.get("source_id") == "dol_eta_ui_claims_scheduled_direct_pdf_v1"
    )
    source["verified"] = False
    source["direct"] = False
    inputs["source_config"] = source_config

    report = audit.build_report(**inputs)
    usd = report["currencies"]["USD"]

    assert usd["configured_complete"] is True
    assert usd["blockers"] == []
    assert usd["statistical_release_configured_complete"] is False
    assert (
        "not_verified_first_party:dol_eta_ui_claims_scheduled_direct_pdf_v1"
        in usd["statistical_release_blockers"]
    )
    assert report["contract_complete"] is True
    assert report["statistical_release_contract_complete"] is False


def test_source_must_be_explicitly_bound_to_the_currency() -> None:
    inputs = real_inputs()
    source_config = copy.deepcopy(inputs["source_config"])
    rba = next(row for row in source_config["sources"] if row.get("source_id") == "rba_media")
    rba["currencies"] = ["CAD"]
    inputs["source_config"] = source_config

    report = audit.build_report(**inputs)

    assert "currency_binding_mismatch:rba_media" in report["currencies"]["AUD"]["blockers"]


def test_linked_policy_clocks_are_mapped_but_not_independent_confirmation() -> None:
    report = audit.build_report(**real_inputs())

    dkk = report["currencies"]["DKK"]
    hkd = report["currencies"]["HKD"]
    assert dkk["schedule_mode"] == "linked_driver"
    assert dkk["linked_driver_currency"] == "EUR"
    assert dkk["schedule_operational"] is True
    assert hkd["schedule_mode"] == "linked_driver"
    assert hkd["linked_driver_currency"] == "USD"
    assert hkd["schedule_operational"] is True


def test_singapore_framework_does_not_fake_a_rate_decision_calendar() -> None:
    report = audit.build_report(**real_inputs())
    sgd = report["currencies"]["SGD"]

    assert sgd["policy_framework"] == "sneer_policy_band"
    assert sgd["schedule_mode"] == "authority_publication_schedule"
    assert sgd["calendar_sources"] == []
    assert sgd["schedule_operational"] is True


def test_report_is_permanently_non_executable() -> None:
    report = audit.build_report(**real_inputs())

    assert report["policy"] == {
        "research_only": True,
        "execution_eligible": False,
        "can_confirm": False,
        "can_promote": False,
        "can_authorize": False,
        "matrix_weight": 0.0,
        "configured_policy_matches": True,
    }


def test_pair_universe_count_is_fail_closed() -> None:
    inputs = real_inputs()
    inputs["instruments"] = inputs["instruments"][:-1]
    report = audit.build_report(**inputs)

    assert "pair_universe_count:67" in report["global_blockers"]
    assert report["contract_complete"] is False
    assert report["minimum_operational_complete"] is False


def test_riksbank_policy_release_has_independent_official_html_transport() -> None:
    inputs = real_inputs()
    mapping = inputs["mapping"]
    sek = next(row for row in mapping["currencies"] if row["currency"] == "SEK")
    assert sek["release_source_ids"] == [
        "riksbank_press",
        "riksbank_monetary_policy_html",
    ]
    source = next(
        row
        for row in inputs["source_config"]["sources"]
        if row.get("source_id") == "riksbank_monetary_policy_html"
    )
    assert source["verified"] is True
    assert source["direct"] is True
    assert source["source_role"] == "primary_policy_release"
    assert source["currencies"] == ["SEK"]


def test_rba_minutes_have_independent_official_html_transport() -> None:
    inputs = real_inputs()
    mapping = inputs["mapping"]
    aud = next(row for row in mapping["currencies"] if row["currency"] == "AUD")
    assert aud["release_source_ids"] == [
        "rba_media",
        "rba_monetary_policy_minutes_html",
    ]
    source = next(
        row
        for row in inputs["source_config"]["sources"]
        if row.get("source_id") == "rba_monetary_policy_minutes_html"
    )
    assert source["verified"] is True
    assert source["direct"] is True
    assert source["source_role"] == "primary_policy_release"
    assert source["currencies"] == ["AUD"]


def test_mnb_has_native_language_and_english_policy_transports() -> None:
    inputs = real_inputs()
    mapping = inputs["mapping"]
    huf = next(row for row in mapping["currencies"] if row["currency"] == "HUF")
    assert huf["release_source_ids"] == [
        "mnb_policy_decisions_hu_direct_v1",
        "mnb_policy_decisions_direct_v1",
    ]
    source = next(
        row
        for row in inputs["source_config"]["sources"]
        if row.get("source_id") == "mnb_policy_decisions_hu_direct_v1"
    )
    assert source["verified"] is True
    assert source["direct"] is True
    assert source["source_role"] == "primary_policy_release"
    assert source["currencies"] == ["HUF"]
    assert source["detail_enrichment"] == "official_document_text"
