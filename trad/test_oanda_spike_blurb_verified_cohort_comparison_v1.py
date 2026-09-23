import copy
import json

import pytest

import oanda_spike_blurb_verified_cohort_comparison_v1 as comparison


def test_frozen_cohort_comparison_remains_inert(tmp_path):
    result = comparison.build(tmp_path)
    assert result["cohort_count"] == 2
    assert result["independent_event_count"] == 4
    assert result["cell_count"] == 16
    assert result["confirmation_eligible_count"] == 0
    assert result["forecast_proof_eligible_count"] == 0
    assert result["execution_eligible_count"] == 0
    assert result["supported_execution_decision"] == "no_trade"


def test_v1_positive_response_seed_does_not_replicate_positive_in_v2(tmp_path):
    result = comparison.build(tmp_path)
    assert set(result["v1_discovery_point_positive_cells"]) == {
        "response_1m_breadth|h5",
        "response_1m_breadth|h15",
    }
    assert result["v1_positive_cells_remaining_positive_in_v2_count"] == 0


def test_report_is_deterministic_and_write_once(tmp_path):
    first = comparison.build(tmp_path)
    second = comparison.build(tmp_path)
    assert first == second
    assert json.loads((tmp_path / "VERIFIED_COHORT_COMPARISON_V1.json").read_text()) == first


def test_operational_upstream_is_rejected(monkeypatch, tmp_path):
    original = comparison.load_report

    def forged(path, contract_id):
        report = copy.deepcopy(original(path, contract_id))
        report["execution_eligible"] = True
        return report

    monkeypatch.setattr(comparison, "load_report", forged)
    with pytest.raises(RuntimeError, match="upstream_not_inert"):
        # Invoke the original validator explicitly against the forged serialized state.
        report = forged(comparison.REPLAY_V1, comparison.EXPECTED_CONTRACTS["replay_v1"])
        temp = tmp_path / "forged.json"
        temp.write_text(json.dumps(report), encoding="utf-8")
        original(temp, comparison.EXPECTED_CONTRACTS["replay_v1"])
