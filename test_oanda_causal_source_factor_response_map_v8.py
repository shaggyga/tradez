import datetime as dt
import sqlite3

import oanda_causal_source_factor_response_map_v8 as subject


UTC = dt.timezone.utc


def _mapping(*, first_seen="2026-09-01T11:45:01+00:00", relevant=False):
    return {
        "classification_version": subject.REQUIRED_CLASSIFICATION_VERSION,
        "mapper_contract_id": subject.REQUIRED_MAPPER_CONTRACT,
        "first_seen_utc": first_seen,
        "mapping_payload": {
            "event_id": "event-bond-survey",
            "currencies": ["JPY"],
            "direct_currencies": ["JPY"],
            "headline": "Bond Market Survey (September 2026)",
            "source_url": "https://www.boj.or.jp/en/paym/bond/bond_list/bond2609.pdf",
            "source_id": "boj_updates",
            "source_role": "primary_market_structure_survey",
            "category": "bond_market_functioning_survey",
            "topic_signature": (
                "bond_market_functioning_survey|JPY|general|"
                "neutral_market_structure_survey"
            ),
            "topic_action": "neutral_market_structure_survey",
            "transmission_mechanisms": [
                "sovereign_curve_microstructure",
                "market_liquidity_state",
            ],
            "relevant": relevant,
            "exclusion_reason": "",
            "official_market_structure_survey": True,
            "fast_lane_pre_map_quote_snapshot": {
                "capture_contract_id": subject.REQUIRED_PRE_MAP_QUOTE_CONTRACT,
                "timing_quality": "prospective_all_68_quotes",
                "quote_count": 68,
                "invalid_instruments": {},
                "missing_instruments": [],
            },
        },
    }


def test_v8_is_clean_v152_overlay_contract_and_does_not_import_v7(tmp_path):
    assert subject.PARENT_CONTRACT_ID.endswith(
        "v152_subject_bound_release_policy_targets_20260901"
    )
    assert subject.REQUIRED_CLASSIFICATION_VERSION.endswith(
        "v152_subject_bound_release_policy_targets"
    )
    assert subject.POLICY["v7_rows_imported"] is False
    assert subject.POLICY["market_structure_survey_direction_invented"] is False
    assert subject.POLICY["proof_eligibility_contract"] == (
        subject.PROOF_ELIGIBILITY_CONTRACT
    )
    assert subject.POLICY["proof_requires_classifier_relevant_or_exact_overlay"] is True
    assert subject.POLICY["proof_requires_relevant_true"] is False
    connection = subject.open_output_database(tmp_path / "v8.sqlite")
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "source_event_observation" in tables
        assert connection.execute(
            "SELECT COUNT(*) FROM source_event_observation"
        ).fetchone()[0] == 0
    finally:
        connection.close()


def test_v8_loader_binds_v152_and_mapper_contract(monkeypatch, tmp_path):
    calls = {}

    def fake_loader(mapping_database, raw_database, **kwargs):
        calls.update(kwargs)
        return []

    monkeypatch.setattr(subject, "_BASE_LOAD_CURRENT_OBSERVATIONS", fake_loader)
    assert subject.load_current_observations(
        tmp_path / "mapping.sqlite", tmp_path / "raw.sqlite"
    ) == []
    assert calls == {
        "required_classifier": subject.REQUIRED_CLASSIFICATION_VERSION,
        "required_mapper_contract": subject.REQUIRED_MAPPER_CONTRACT,
    }


def test_v8_market_structure_survey_is_proof_eligible_but_directionless(monkeypatch):
    event = {
        "canonical_event_id": "source_event_bond_survey",
        "currency": "JPY",
        "first_known_utc": "2026-09-01T11:45:01+00:00",
        "source_facts": {},
        "transport_observations": [_mapping()],
    }
    monkeypatch.setattr(
        subject,
        "_BASE_CANONICALIZE_OBSERVATIONS",
        lambda observations, activation_utc: [event],
    )
    rows = subject.canonicalize_observations([_mapping()])
    assert len(rows) == 1
    assert rows[0]["prospective_proof_eligible"] is True
    assert rows[0]["evidence_class"] == "prospective_v8"
    assert rows[0]["source_facts"]["relevant"] is True
    assert rows[0]["source_facts"]["official_market_structure_survey"] is True
    assert rows[0]["upstream_classifier_relevant"] is False


def test_v8_pre_cutover_row_never_becomes_proof(monkeypatch):
    event = {
        "canonical_event_id": "source_event_old",
        "currency": "JPY",
        "first_known_utc": "2026-09-01T11:44:59+00:00",
        "source_facts": {},
        "transport_observations": [_mapping(first_seen="2026-09-01T11:44:59+00:00")],
    }
    monkeypatch.setattr(
        subject,
        "_BASE_CANONICALIZE_OBSERVATIONS",
        lambda observations, activation_utc: [event],
    )
    row = subject.canonicalize_observations([_mapping()])[0]
    assert row["prospective_proof_eligible"] is False
    assert "evidence_class" not in row


def test_v8_does_not_admit_unrelated_v152_excluded_event(monkeypatch):
    mapping = _mapping()
    mapping["mapping_payload"] = {
        **mapping["mapping_payload"],
        "headline": "Bank of Japan releases an unrelated archived notice",
        "source_url": "https://www.boj.or.jp/en/announcements/release_2026.htm",
        "category": "market_news",
        "source_role": "primary_release",
        "relevant": False,
        "exclusion_reason": "classifier_not_relevant",
    }
    event = {
        "canonical_event_id": "source_event_unrelated",
        "currency": "JPY",
        "first_known_utc": "2026-09-01T11:45:01+00:00",
        "source_facts": {},
        "transport_observations": [mapping],
    }
    monkeypatch.setattr(
        subject,
        "_BASE_CANONICALIZE_OBSERVATIONS",
        lambda observations, activation_utc: [event],
    )

    row = subject.canonicalize_observations([mapping])[0]

    assert row["prospective_proof_eligible"] is False
    assert row["evidence_class"] == "prospective_v8_excluded_diagnostic"
    assert row["source_facts"]["relevant"] is False
    assert row["source_facts"]["exclusion_reason"] == "classifier_not_relevant"
    assert "official_market_structure_survey" not in row["source_facts"]


def test_supervisor_preserves_retired_causal_source_factor_response_map_diagnostics():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(encoding="utf-8")
    for version in [7, 8]:
        name = f"causal_source_factor_response_map_v{version}"
        assert f'-Name "{name}"' not in supervisor
        assert f'-Name "{name}_preserved"' in supervisor
        assert f'-Needle "oanda_{name}.py"' in supervisor
