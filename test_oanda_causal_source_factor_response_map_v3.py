from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import oanda_causal_source_factor_response_map_v1 as v1
import oanda_causal_source_factor_response_map_v2 as v2
import oanda_causal_source_factor_response_map_v3 as subject


def test_v3_is_a_new_separate_v148_prospective_contract(tmp_path):
    connection = subject.open_output_database(tmp_path / "v3.sqlite")
    try:
        response_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' "
            "AND name='source_event_response'"
        ).fetchone()[0]
        assert "1,5,10,15,30,60,120" in response_sql.replace(" ", "")
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        connection.close()
    assert subject.OUTPUT_DATABASE != v2.OUTPUT_DATABASE
    assert subject.SNAPSHOT_PATH != v2.SNAPSHOT_PATH
    assert subject.REPORT_PATH != v2.REPORT_PATH
    assert subject.CONTRACT_ID != v2.CONTRACT_ID
    assert subject.COHORT_ID != v2.COHORT_ID
    assert subject.ACTIVATED_UTC > v2.ACTIVATED_UTC
    assert subject.HORIZONS_MIN == v2.HORIZONS_MIN
    assert subject.PARENT_CONTRACT_ID == v2.CONTRACT_ID
    assert subject.REQUIRED_CLASSIFICATION_VERSION.endswith(
        "v148_official_research_intervention_boundary"
    )
    assert subject.POLICY["v2_rows_imported"] is False


def test_v3_loader_explicitly_binds_v148_and_mapper_contract(
    monkeypatch, tmp_path
):
    calls = []

    def fake_loader(mapping_database, raw_database, **kwargs):
        calls.append((mapping_database, raw_database, kwargs))
        return [{"classification_version": kwargs["required_classifier"]}]

    monkeypatch.setattr(subject, "_V1_LOAD_CURRENT_OBSERVATIONS", fake_loader)
    mapping = tmp_path / "mapping.sqlite"
    raw = tmp_path / "raw.sqlite"
    rows = subject.load_current_observations(mapping, raw)
    assert rows == [
        {"classification_version": subject.REQUIRED_CLASSIFICATION_VERSION}
    ]
    assert calls[0][2] == {
        "required_classifier": subject.REQUIRED_CLASSIFICATION_VERSION,
        "required_mapper_contract": subject.REQUIRED_MAPPER_CONTRACT,
    }


def test_v148_rows_before_v3_activation_remain_diagnostic():
    before = subject.ACTIVATED_UTC - timedelta(minutes=1)
    after = subject.ACTIVATED_UTC + timedelta(minutes=1)

    def observation(mapping_id, at):
        payload = {
            "source_id": "official",
            "source_name": "Official",
            "source_kind": "rss",
            "source_role": "primary_policy_release",
            "source_direct": True,
            "source_verified": True,
            "source_native_currency_bound": True,
            "source_currencies": ["JPY"],
            "direct_currencies": ["JPY"],
            "headline": mapping_id,
            "source_url": f"https://official.test/{mapping_id}",
            "published_utc": subject.iso(at),
            "category": "monetary_policy",
            "topic_action": "hold",
            "currency_scores": {},
            "research_currency_scores": {},
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
            "currencies": ["JPY"],
        }

    events = subject.canonicalize_observations(
        [observation("before", before), observation("after", after)]
    )
    by_headline = {row["headline"]: row for row in events}
    assert by_headline["before"]["evidence_class"] == "preactivation_diagnostic"
    assert by_headline["before"]["prospective_proof_eligible"] is False
    assert by_headline["after"]["evidence_class"] == "prospective_v1"
    assert by_headline["after"]["prospective_proof_eligible"] is True


def test_v3_context_restores_sealed_v1_and_v2_contracts():
    v1_before = {
        "contract": v1.CONTRACT_ID,
        "cohort": v1.COHORT_ID,
        "activation": v1.ACTIVATED_UTC,
        "horizons": v1.HORIZONS_MIN,
        "classifier": v1.REQUIRED_CLASSIFICATION_VERSION,
    }
    v2_before = (v2.CONTRACT_ID, v2.COHORT_ID, v2.OUTPUT_DATABASE)
    with subject._v3_contract():
        assert v1.CONTRACT_ID == subject.CONTRACT_ID
        assert v1.COHORT_ID == subject.COHORT_ID
        assert v1.ACTIVATED_UTC == subject.ACTIVATED_UTC
        assert v1.REQUIRED_CLASSIFICATION_VERSION == (
            subject.REQUIRED_CLASSIFICATION_VERSION
        )
        assert v1.load_current_observations is subject.load_current_observations
        assert v1.canonicalize_observations is subject.canonicalize_observations
    assert v1.CONTRACT_ID == v1_before["contract"]
    assert v1.COHORT_ID == v1_before["cohort"]
    assert v1.ACTIVATED_UTC == v1_before["activation"]
    assert v1.HORIZONS_MIN == v1_before["horizons"]
    assert v1.REQUIRED_CLASSIFICATION_VERSION == v1_before["classifier"]
    assert (v2.CONTRACT_ID, v2.COHORT_ID, v2.OUTPUT_DATABASE) == v2_before


def test_v3_pre_semantic_bundle_keeps_gate_and_rejects_late_only():
    at = subject.ACTIVATED_UTC + timedelta(minutes=1)
    event = {
        "canonical_event_id": "v3_quote_event",
        "currency": "JPY",
        "first_known_utc": subject.iso(at),
    }
    quotes = {
        f"USD_X{index:02d}": {
            "bid": 1.0,
            "ask": 1.0002,
            "pip": 0.0001,
            "quote_time_utc": subject.iso(at + timedelta(seconds=5)),
        }
        for index in range(60)
    }

    def attached(seconds):
        return {
            "capture_contract_id": subject.REQUIRED_PRE_MAP_QUOTE_CONTRACT,
            "observation_id": "obs",
            "event_first_known_utc": subject.iso(at),
            "captured_utc": subject.iso(at + timedelta(seconds=seconds)),
            "quotes": quotes,
        }

    with subject._v3_contract():
        early = v1.build_live_entry_snapshot_from_pre_map(event, attached(10))
        late = v1.build_live_entry_snapshot_from_pre_map(event, attached(20))
    assert early is not None and late is not None
    assert early["timing_quality"] == "prospective_exact_live_quote"
    assert early["quote_count"] == 60
    assert late["timing_quality"] == "prospective_clock_missed"
    assert late["quote_count"] == 0


def test_hidden_supervisor_preserves_v3_and_starts_v4():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert '-Name "causal_source_factor_response_map_v4"' in supervisor
    assert '-Needle "oanda_causal_source_factor_response_map_v4.py"' in supervisor
    assert "causal_source_factor_response_map_latest_v4.json" in supervisor
    assert '-Needle "oanda_causal_source_factor_response_map_v3.py"' in supervisor
    assert '-Needle "oanda_causal_source_factor_response_map_v1.py"' in supervisor
    assert '-Needle "oanda_causal_source_factor_response_map_v2.py"' in supervisor
    assert "v4_v149_relevance_exclusion_gate_cutover" in supervisor
    assert '-Name "causal_source_factor_response_map_v3" `\n' not in supervisor
