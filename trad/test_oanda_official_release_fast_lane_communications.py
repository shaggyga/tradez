from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import oanda_causal_source_factor_response_map_v5 as causal_v5
import oanda_event_technical_preflight as preflight
import oanda_local_news_sentiment as news
import oanda_official_release_fast_lane as fast
import oanda_official_release_fast_mapper as mapper
from oanda_official_release_fast_lane_contract import (
    OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID,
    OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID,
)


UTC = dt.timezone.utc


def _official_raw(
    *,
    source_id: str,
    source_name: str,
    currency: str,
    title: str,
    summary: str,
    url: str,
    published_utc: str = "2026-08-28T14:00:00+00:00",
) -> dict:
    return {
        "source_id": source_id,
        "source_name": source_name,
        "source_role": "primary_policy_release",
        "source_verified": True,
        "source_direct": True,
        "source_currencies": [currency],
        "title": title,
        "summary": summary,
        "url": url,
        "published_utc": published_utc,
    }


def _proof_exclusions(classified: dict) -> list[str]:
    return causal_v5._classifier_proof_exclusions(
        {"transport_observations": [{"mapping_payload": classified}]}
    )


def _friday_warsh_speech_fixture() -> dict:
    """Frozen regression shape of the 2026-08-28 official speech body."""

    return _official_raw(
        source_id="fed_speeches",
        source_name="Federal Reserve speeches",
        currency="USD",
        title="Warsh, In Our Time",
        summary=(
            "Speeches. Remarks by Chair Kevin Warsh at Jackson Hole. The "
            "Federal Reserve must keep monetary policy higher for longer while "
            "inflation remains a concern. The FOMC statement is one part of "
            "that record. The address compares the Bank of England, the "
            "Singapore dollar, and the Turkish lira as foreign examples; those "
            "references are not new policy observations for their issuers."
        ),
        url=(
            "https://www.federalreserve.gov/newsevents/speech/"
            "warsh20260828a.htm"
        ),
        published_utc="2026-08-28T14:00:00+00:00",
    )


def _riksbank_unlabelled_speech_fixture() -> dict:
    """Frozen shape of a speech whose source-native title omits ``speech``."""

    return _official_raw(
        source_id="riksbank_speeches",
        source_name="Sveriges Riksbank speeches",
        currency="SEK",
        title=(
            "Per Jansson: Inflation risks being elevated, but there is scope "
            "to wait and see"
        ),
        summary=(
            "Deputy Governor Per Jansson said monetary policy must respond if "
            "inflation risks persist, may raise the policy rate, and may keep "
            "rates higher for longer. The presentation discusses rising "
            "aviation fuel and oil "
            "prices and compares Canada, Mexico, Norway and Japan; those are "
            "foreign examples, not source observations for CAD, MXN, NOK or "
            "JPY."
        ),
        url=(
            "https://www.riksbank.se/en-gb/press-and-published/"
            "speeches-and-presentations/2026/per-jansson-inflation-risks"
        ),
        published_utc="2026-08-31T08:30:10+00:00",
    )


def test_governed_authoritative_communications_join_fast_lane() -> None:
    config = fast.read_json(fast.CONFIG_PATH, {})
    mapping = fast.read_json(fast.CENTRAL_BANK_MAP_PATH, {})
    communication_ids = set(fast.central_bank_communication_source_ids(mapping))
    selected = {
        row["source_id"]: row for row in fast.selected_sources(config, mapping)
    }

    assert "fed_speeches" in communication_ids
    assert communication_ids <= set(selected)
    assert all(selected[source_id]["verified"] is True for source_id in communication_ids)
    assert all(
        news.configured_source_is_direct(selected[source_id])
        for source_id in communication_ids
    )
    assert all(
        selected[source_id].get("trusted_domains")
        for source_id in communication_ids
    )
    assert set(
        news.ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_CURRENCIES_V2
    ) <= communication_ids
    authority_currency_by_communication = {
        source_id: row["currency"]
        for row in mapping["currencies"]
        for source_id in row.get("communication_source_ids") or []
    }
    assert news.ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_CURRENCIES_V2 == {
        source_id: authority_currency_by_communication[source_id]
        for source_id in news.ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_CURRENCIES_V2
    }
    assert fast.CONTRACT_ID != OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID
    assert fast.COLLECTOR_COHORT_ID != OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID


def test_communication_source_authority_is_fail_closed() -> None:
    mapping = {
        "currencies": [
            {
                "currency": "USD",
                "release_source_ids": [f"release_{index}"]
                if index < 21
                else [],
                "communication_source_ids": ["untrusted_speech_feed"]
                if index == 0
                else [],
                "statistical_release_source_ids": [],
            }
            for index in range(21)
        ]
    }
    config = {
        "sources": [
            {
                "source_id": f"release_{index}",
                "verified": True,
                "direct": True,
                "trusted_domains": ["example.test"],
            }
            for index in range(21)
        ]
        + [
            {
                "source_id": "untrusted_speech_feed",
                "verified": False,
                "direct": False,
                "trusted_domains": [],
            }
        ]
    }

    try:
        fast.selected_sources(config, mapping)
    except ValueError as exc:
        assert "direct, verified, and domain-bound" in str(exc)
    else:
        raise AssertionError("untrusted communication source was selected")


def test_missed_warsh_speech_bootstraps_but_future_speech_can_be_prospective(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        fast.news,
        "prospective_clock_attestation",
        lambda clock: bool(clock.get("attested")),
    )
    connection = fast.open_database(tmp_path / "fast.sqlite")
    source = {
        "source_id": "fed_speeches",
        "source_contract_id": "fed-speeches-test-v1",
        "source_cohort_id": "fed-speeches-test-v1",
    }
    try:
        fast.append_observations(
            connection,
            source=source,
            rows=[
                {
                    "title": "Warsh, In Our Time",
                    "url": (
                        "https://www.federalreserve.gov/newsevents/speech/"
                        "warsh20260828a.htm"
                    ),
                    "published_utc": "2026-08-28T14:00:00+00:00",
                }
            ],
            first_seen=dt.datetime(2026, 8, 28, 15, 1, tzinfo=UTC),
            listing_bootstrap=True,
            observation_clock={"attested": True, "source": "test_clock"},
        )
        fast.append_observations(
            connection,
            source=source,
            rows=[
                {
                    "title": "Future monetary policy speech",
                    "url": (
                        "https://www.federalreserve.gov/newsevents/speech/"
                        "future20260828a.htm"
                    ),
                    "published_utc": "2026-08-28T15:05:00+00:00",
                }
            ],
            first_seen=dt.datetime(2026, 8, 28, 15, 5, 3, tzinfo=UTC),
            listing_bootstrap=False,
            observation_clock={"attested": True, "source": "test_clock"},
        )
        rows = connection.execute(
            """
            SELECT raw_payload_json, prospective_observation,
                   collector_contract_id, collector_cohort_id
              FROM official_release_observation
             ORDER BY first_seen_utc
            """
        ).fetchall()
    finally:
        connection.close()

    assert len(rows) == 2
    assert json.loads(rows[0][0])["title"] == "Warsh, In Our Time"
    assert rows[0][1] == 0
    assert rows[1][1] == 1
    assert {(row[2], row[3]) for row in rows} == {
        (fast.CONTRACT_ID, fast.COLLECTOR_COHORT_ID)
    }


def test_friday_warsh_fixture_is_historical_but_future_cohort_is_issuer_bound(
) -> None:
    raw = _friday_warsh_speech_fixture()
    historical = news.classify_article(
        raw,
        first_seen=dt.datetime(2026, 8, 28, 14, 1, 5, tzinfo=UTC),
    )
    assert historical[
        "issuer_bound_policy_communication_activation_eligible"
    ] is False
    assert historical["issuer_bound_policy_communication"] is True
    assert historical["issuer_bound_policy_communication_source_identity"] is True
    assert historical["issuer_bound_policy_communication_contract_id"] == (
        news.ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_CONTRACT_ID_V2
    )
    assert historical["direct_currencies"] == ["USD"]
    assert set(historical["currency_scores"]) <= {"USD"}
    assert historical["directional_publish_eligible"] is False

    prospective = news.classify_article(
        raw,
        first_seen=(
            news.parse_datetime(
                news.ISSUER_BOUND_POLICY_COMMUNICATION_ACTIVATED_UTC
            )
            + dt.timedelta(minutes=1)
        ),
    )
    assert prospective[
        "issuer_bound_policy_communication_activation_eligible"
    ] is True
    assert prospective["issuer_bound_policy_communication"] is True
    assert prospective[
        "issuer_bound_policy_communication_contract_id"
    ] == news.ISSUER_BOUND_POLICY_COMMUNICATION_CONTRACT_ID
    assert prospective[
        "issuer_bound_policy_communication_cohort_id"
    ] == news.ISSUER_BOUND_POLICY_COMMUNICATION_COHORT_ID
    assert prospective["source_role"] == "primary_policy_communication"
    assert prospective["official_policy_release"] is False
    assert prospective["policy_document_type"] == "policy_communication"
    assert prospective["policy_stance_bearing_eligible"] is False
    assert prospective["source_currencies"] == ["USD"]
    assert {"GBP", "SGD", "TRY", "USD"} <= set(
        prospective["mentioned_currency_entities"]
    )
    assert prospective["direct_currencies"] == ["USD"]
    assert prospective["currencies"] == ["USD"]
    assert set(prospective["currency_scores"]) <= {"USD"}
    assert prospective["directional_publish_eligible"] is False
    assert prospective["directional_research_only"] is True


def test_riksbank_source_container_binds_scope_without_retroactive_candidate(
) -> None:
    raw = _riksbank_unlabelled_speech_fixture()
    activation = news.parse_datetime(
        news.ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_ACTIVATED_UTC_V2
    )
    assert activation is not None

    diagnostic = news.classify_article(
        raw,
        first_seen=activation - dt.timedelta(days=1),
    )
    assert diagnostic["issuer_bound_policy_communication"] is True
    assert diagnostic["issuer_bound_policy_communication_source_identity"] is True
    assert diagnostic["issuer_bound_policy_communication_activation_eligible"] is False
    assert diagnostic["policy_document_type"] == "policy_communication"
    assert diagnostic["direct_currencies"] == ["SEK"]
    assert diagnostic["currencies"] == ["SEK"]
    assert set(diagnostic["currency_scores"]) <= {"SEK"}
    assert set(diagnostic["research_currency_scores"]) <= {"SEK"}
    assert diagnostic["inferred_currencies"] == []

    preactivation_mapping = mapper.classify_observation(
        {
            "first_seen_utc": news.iso_utc(activation - dt.timedelta(days=1)),
            "prospective_observation": True,
            "raw": raw,
        }
    )
    assert preactivation_mapping["semantic_direction_available"] is True
    assert preactivation_mapping["classification_candidate_activation_eligible"] is False
    assert preactivation_mapping["prospective_semantic_candidate"] is False
    assert preactivation_mapping["forward_shadow_candidate"] is False

    prospective_mapping = mapper.classify_observation(
        {
            "first_seen_utc": news.iso_utc(activation + dt.timedelta(minutes=1)),
            "prospective_observation": True,
            "raw": raw,
        }
    )
    assert prospective_mapping[
        "issuer_bound_policy_communication_activation_eligible"
    ] is True
    assert prospective_mapping["classification_candidate_activation_eligible"] is True
    assert prospective_mapping["prospective_semantic_candidate"] is True
    assert prospective_mapping["forward_shadow_candidate"] is True
    assert set(prospective_mapping["currency_scores"]) == {"SEK"}
    assert prospective_mapping["inferred_currencies"] == []


def test_cbrt_student_contest_remains_excluded_from_current_proof() -> None:
    raw = _official_raw(
        source_id="tcmb_press",
        source_name="Central Bank of the Republic of Turkiye press releases",
        currency="TRY",
        title=(
            "Press Release on Results of CBRT Paper Contest for University "
            "Students (2026-37)"
        ),
        summary=(
            "The results of the CBRT Paper Contest for University Students "
            "have been announced."
        ),
        url=(
            "https://www.tcmb.gov.tr/wps/wcm/connect/en/tcmb+en/main+menu/"
            "announcements/press+releases/2026/ano2026-37"
        ),
        published_utc="2026-08-28T10:00:00+00:00",
    )
    classified = news.classify_article(
        raw,
        first_seen=dt.datetime(2026, 8, 28, 11, 27, 43, tzinfo=UTC),
    )

    # The live classifier is V151 while the sealed V5 causal cohort remains
    # pinned to V150.  This fixture verifies the current classifier and the
    # shared proof-exclusion semantics without pretending that a V151 article
    # belongs to the historical V5 cohort.
    assert classified["classification_version"] == news.CLASSIFICATION_VERSION
    assert classified["official_non_market_administrative"] is True
    assert classified["relevant"] is False
    assert classified["exclusion_reason"] == "official_non_market_administrative"
    assert _proof_exclusions(classified) == [
        "classifier_relevant_not_true",
        "classifier_exclusion_reason:official_non_market_administrative",
    ]


def test_ordinary_official_administration_cannot_become_current_proof() -> None:
    raw = _official_raw(
        source_id="fed_speeches",
        source_name="Federal Reserve speeches",
        currency="USD",
        title="Federal Reserve Board announces appointment to advisory council",
        summary=(
            "The Federal Reserve Board announced appointments of members to "
            "its advisory council."
        ),
        url=(
            "https://www.federalreserve.gov/newsevents/pressreleases/"
            "other20260828a.htm"
        ),
    )
    classified = news.classify_article(
        raw,
        first_seen=dt.datetime(2026, 8, 28, 14, 1, tzinfo=UTC),
    )

    assert classified["relevant"] is False
    assert classified["currency_scores"] == {}
    assert classified["research_currency_scores"] == {}
    assert "classifier_relevant_not_true" in _proof_exclusions(classified)


def test_scheduled_warsh_preflight_is_separate_and_directionless() -> None:
    mapping = fast.read_json(fast.CENTRAL_BANK_MAP_PATH, {})
    selected_ids = set(fast.official_fast_lane_source_ids(mapping))
    calendar_ids = {
        str(source_id)
        for row in mapping.get("currencies") or []
        for source_id in row.get("calendar_source_ids") or []
    }
    assert "fed_speeches" in selected_ids
    assert "kansas_city_fed_jackson_hole_calendar_2026" in calendar_ids
    assert selected_ids.isdisjoint(calendar_ids)

    observed = dt.datetime(2026, 8, 28, 13, 55, tzinfo=UTC)
    payload = preflight.build(
        [
            {
                "event_id": "warsh_jackson_hole_clock",
                "event_series_id": "federal_reserve_chair_jackson_hole_remarks",
                "headline": "Federal Reserve Chair Kevin Warsh Jackson Hole Remarks",
                "category": "monetary_policy",
                "severity": 85.0,
                "movement_potential": "HIGH",
                "scheduled_utc": "2026-08-28T14:00:00+00:00",
                "timing_precision": "minute",
                "direct_currencies_known": True,
                "direct_currencies": ["USD"],
            }
        ],
        {
            "generated_utc": observed.isoformat(),
            "quotes": {
                "EUR_USD": {"bid": 1.10, "ask": 1.1001},
                "USD_JPY": {"bid": 150.0, "ask": 150.01},
            },
        },
        {
            "updated_at": observed.isoformat(),
            "top_signals": [
                {"instrument": "EUR_USD", "direction": "long"},
                {"instrument": "USD_JPY", "direction": "short"},
            ],
        },
        {
            "currencies": [
                {
                    "currency": "USD",
                    "causal_pre_release_consensus": 0,
                    "daily_rate_context": True,
                }
            ]
        },
        observed,
        1,
    )
    event = payload["events"][0]
    assert event["minutes_until_event"] == 5.0
    assert event["readiness_state"] == "ready_for_neutral_post_release_verification"
    assert event["direction"] == "unknown_until_causal_release_evidence"
    assert event["research_only"] is True
    assert event["execution_eligible"] is False
