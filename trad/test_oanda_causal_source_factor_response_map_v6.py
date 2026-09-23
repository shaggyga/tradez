from __future__ import annotations

from datetime import timedelta

import oanda_causal_source_factor_response_map_v5 as v5
import oanda_causal_source_factor_response_map_v6 as subject


def observation(mapping_id, at, *, relevant=True, exclusion_reason=""):
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
        "mapping_payload": {
            "source_id": "official",
            "source_name": "Official",
            "source_kind": "rss",
            "source_role": "primary_policy_release",
            "source_direct": True,
            "source_verified": True,
            "source_native_currency_bound": True,
            "source_currencies": ["USD"],
            "direct_currencies": ["USD"],
            "headline": mapping_id,
            "source_url": f"https://official.test/{mapping_id}",
            "published_utc": subject.iso(at),
            "category": "monetary_policy",
            "topic_action": "hold",
            "currency_scores": {},
            "research_currency_scores": {},
            "relevant": relevant,
            "exclusion_reason": exclusion_reason,
        },
        "raw_payload_hash": mapping_id,
        "currencies": ["USD"],
    }


def test_v6_is_clean_v151_contract_and_does_not_import_v5(tmp_path):
    connection = subject.open_output_database(tmp_path / "v6.sqlite")
    try:
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        connection.close()
    assert subject.OUTPUT_DATABASE != v5.OUTPUT_DATABASE
    assert subject.SNAPSHOT_PATH != v5.SNAPSHOT_PATH
    assert subject.CONTRACT_ID != v5.CONTRACT_ID
    assert subject.COHORT_ID != v5.COHORT_ID
    assert subject.ACTIVATED_UTC > v5.ACTIVATED_UTC
    assert subject.PARENT_CONTRACT_ID == v5.CONTRACT_ID
    assert subject.REQUIRED_CLASSIFICATION_VERSION.endswith(
        "v151_pair_breakout_recap_boundary"
    )
    assert subject.POLICY["v5_rows_imported"] is False


def test_v6_loader_binds_v151_and_mapper_contract(monkeypatch, tmp_path):
    calls = []

    def fake_loader(mapping_database, raw_database, **kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(subject, "_V1_LOAD_CURRENT_OBSERVATIONS", fake_loader)
    assert subject.load_current_observations(
        tmp_path / "mapping.sqlite", tmp_path / "raw.sqlite"
    ) == []
    assert calls == [{
        "required_classifier": subject.REQUIRED_CLASSIFICATION_VERSION,
        "required_mapper_contract": subject.REQUIRED_MAPPER_CONTRACT,
    }]


def test_v6_keeps_relevance_exclusion_and_activation_gates():
    before = subject.ACTIVATED_UTC - timedelta(minutes=1)
    after = subject.ACTIVATED_UTC + timedelta(minutes=1)
    events = subject.canonicalize_observations([
        observation("before", before),
        observation("valid", after),
        observation("irrelevant", after, relevant=False),
        observation("excluded", after, exclusion_reason="non_market"),
    ])
    by_headline = {row["headline"]: row for row in events}
    assert by_headline["before"]["evidence_class"] == "preactivation_diagnostic"
    assert by_headline["valid"]["evidence_class"] == "prospective_v6"
    assert by_headline["valid"]["prospective_proof_eligible"] is True
    assert by_headline["irrelevant"]["evidence_class"] == (
        "prospective_v6_excluded_diagnostic"
    )
    assert by_headline["irrelevant"]["prospective_proof_eligible"] is False
    assert by_headline["excluded"]["prospective_proof_eligible"] is False


def test_supervisor_preserves_v6_and_starts_v8():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert '-Name "causal_source_factor_response_map_v6_preserved"' in supervisor
    assert '-Needle "oanda_causal_source_factor_response_map_v6.py"' in supervisor
    assert '-Name "causal_source_factor_response_map_v8"' in supervisor
    assert "causal_source_factor_response_map_latest_v8.json" in supervisor
    assert '-Name "causal_source_factor_response_map_v5_preserved"' in supervisor
    assert '-Needle "oanda_causal_source_factor_response_map_v5.py"' in supervisor
    assert "v6_v151_pair_breakout_recap_cutover" in supervisor
