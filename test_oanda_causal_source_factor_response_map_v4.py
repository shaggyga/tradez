from __future__ import annotations

from datetime import timedelta

import oanda_causal_source_factor_response_map_v1 as v1
import oanda_causal_source_factor_response_map_v3 as v3
import oanda_causal_source_factor_response_map_v4 as subject
import oanda_local_news_sentiment as news


def observation(mapping_id, at, *, relevant=True, exclusion_reason=""):
    payload = {
        "source_id": "official",
        "source_name": "Official",
        "source_kind": "rss",
        "source_role": "primary_policy_release",
        "source_direct": True,
        "source_verified": True,
        "source_native_currency_bound": True,
        "source_currencies": ["TRY"],
        "direct_currencies": ["TRY"],
        "headline": mapping_id,
        "source_url": f"https://official.test/{mapping_id}",
        "published_utc": subject.iso(at),
        "category": "monetary_policy",
        "topic_action": "hold",
        "currency_scores": {},
        "research_currency_scores": {},
        "relevant": relevant,
        "exclusion_reason": exclusion_reason,
    }
    return {
        "mapping_id": mapping_id,
        "observation_id": f"obs_{mapping_id}",
        "source_id": "official",
        "source_contract_id": "source_contract",
        "first_seen_utc": subject.iso(at),
        "mapped_utc": subject.iso(at + timedelta(seconds=3)),
        "classification_version": subject.REQUIRED_CLASSIFICATION_VERSION,
        "mapper_contract_id": subject.REQUIRED_MAPPER_CONTRACT,
        "mapper_cohort_id": "mapper",
        "material_sha256": mapping_id,
        "observation_clock_source": "immutable",
        "collector_contract_id": "collector",
        "collector_cohort_id": "collector_cohort",
        "mapping_payload": payload,
        "raw_payload_hash": mapping_id,
        "currencies": ["TRY"],
    }


def test_v4_is_separate_v149_contract_and_does_not_import_v3(tmp_path):
    connection = subject.open_output_database(tmp_path / "v4.sqlite")
    try:
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        connection.close()
    assert subject.OUTPUT_DATABASE != v3.OUTPUT_DATABASE
    assert subject.SNAPSHOT_PATH != v3.SNAPSHOT_PATH
    assert subject.REPORT_PATH != v3.REPORT_PATH
    assert subject.CONTRACT_ID != v3.CONTRACT_ID
    assert subject.COHORT_ID != v3.COHORT_ID
    assert subject.ACTIVATED_UTC > v3.ACTIVATED_UTC
    assert subject.PARENT_CONTRACT_ID == v3.CONTRACT_ID
    assert subject.REQUIRED_CLASSIFICATION_VERSION.endswith(
        "v149_cbrt_non_market_administrative_boundary"
    )
    assert subject.POLICY["v3_rows_imported"] is False
    assert subject.POLICY["proof_requires_relevant_true"] is True
    assert subject.POLICY["proof_requires_empty_exclusion_reason"] is True


def test_v4_loader_binds_v149_and_mapper_contract(monkeypatch, tmp_path):
    calls = []

    def fake_loader(mapping_database, raw_database, **kwargs):
        calls.append((mapping_database, raw_database, kwargs))
        return [{"classification_version": kwargs["required_classifier"]}]

    monkeypatch.setattr(subject, "_V1_LOAD_CURRENT_OBSERVATIONS", fake_loader)
    rows = subject.load_current_observations(
        tmp_path / "mapping.sqlite", tmp_path / "raw.sqlite"
    )
    assert rows == [
        {"classification_version": subject.REQUIRED_CLASSIFICATION_VERSION}
    ]
    assert calls[0][2] == {
        "required_classifier": subject.REQUIRED_CLASSIFICATION_VERSION,
        "required_mapper_contract": subject.REQUIRED_MAPPER_CONTRACT,
    }


def test_v4_proof_requires_relevant_true_and_empty_exclusion_reason():
    before = subject.ACTIVATED_UTC - timedelta(minutes=1)
    after = subject.ACTIVATED_UTC + timedelta(minutes=1)
    events = subject.canonicalize_observations(
        [
            observation("before", before),
            observation("valid", after),
            observation("irrelevant", after, relevant=False),
            observation(
                "excluded",
                after + timedelta(minutes=1),
                exclusion_reason="official_non_market_administrative",
            ),
        ]
    )
    by_headline = {row["headline"]: row for row in events}
    assert by_headline["before"]["evidence_class"] == "preactivation_diagnostic"
    assert by_headline["before"]["prospective_proof_eligible"] is False
    assert by_headline["valid"]["evidence_class"] == "prospective_v4"
    assert by_headline["valid"]["prospective_proof_eligible"] is True
    assert by_headline["valid"]["proof_exclusion_reasons"] == []
    assert by_headline["irrelevant"]["prospective_proof_eligible"] is False
    assert by_headline["irrelevant"]["evidence_class"] == (
        "prospective_v4_excluded_diagnostic"
    )
    assert "classifier_relevant_not_true" in (
        by_headline["irrelevant"]["proof_exclusion_reasons"]
    )
    assert by_headline["excluded"]["prospective_proof_eligible"] is False
    assert any(
        reason.endswith("official_non_market_administrative")
        for reason in by_headline["excluded"]["proof_exclusion_reasons"]
    )


def test_cbrt_contest_is_retained_diagnostic_but_never_v4_proof():
    at = subject.ACTIVATED_UTC + timedelta(minutes=1)
    payload = news.classify_article(
        {
            "source_id": "tcmb_press",
            "source_name": "CBRT",
            "source_kind": "rss",
            "source_role": "primary_policy_release",
            "source_verified": True,
            "source_direct": True,
            "source_quality": 1.0,
            "source_currencies": ["TRY"],
            "title": (
                "Press Release on Results of CBRT Paper Contest for "
                "University Students (2026-37)"
            ),
            "summary": "Students interested in economics presented their papers.",
            "url": "https://www.tcmb.gov.tr/example/paper-contest",
            "published_utc": subject.iso(at),
        },
        first_seen=at,
    )
    row = observation("contest", at, relevant=False)
    row["mapping_payload"] = payload
    event = subject.canonicalize_observations([row])[0]
    assert event["headline"].startswith("Press Release on Results")
    assert event["prospective_proof_eligible"] is False
    assert event["evidence_class"] == "prospective_v4_excluded_diagnostic"
    assert event["source_facts"]["relevant"] is False
    assert event["source_facts"]["exclusion_reason"] == (
        "official_non_market_administrative"
    )


def test_v4_context_restores_v1_globals():
    before = (
        v1.CONTRACT_ID,
        v1.COHORT_ID,
        v1.ACTIVATED_UTC,
        v1.REQUIRED_CLASSIFICATION_VERSION,
    )
    with subject._v4_contract():
        assert v1.CONTRACT_ID == subject.CONTRACT_ID
        assert v1.COHORT_ID == subject.COHORT_ID
        assert v1.ACTIVATED_UTC == subject.ACTIVATED_UTC
        assert v1.REQUIRED_CLASSIFICATION_VERSION == (
            subject.REQUIRED_CLASSIFICATION_VERSION
        )
        assert v1.canonicalize_observations is subject.canonicalize_observations
    assert (
        v1.CONTRACT_ID,
        v1.COHORT_ID,
        v1.ACTIVATED_UTC,
        v1.REQUIRED_CLASSIFICATION_VERSION,
    ) == before


def test_hidden_supervisor_preserves_v4_and_starts_current_v6():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert '-Name "causal_source_factor_response_map_v4_preserved"' in supervisor
    assert '-Needle "oanda_causal_source_factor_response_map_v4.py"' in supervisor
    assert '-Needle "oanda_causal_source_factor_response_map_v3.py"' in supervisor
    assert '-Name "causal_source_factor_response_map_v6"' in supervisor
    assert "causal_source_factor_response_map_latest_v6.json" in supervisor
    assert "v6_v151_pair_breakout_recap_cutover" in supervisor
    assert 'ExpectedJsonField = "classification_version"' in supervisor
    assert supervisor.count(
        'ExpectedJsonValue = "local_fx_news_rules_20260828_v151_'
        'pair_breakout_recap_boundary"'
    ) == 4
