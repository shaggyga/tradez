from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path

import pytest

import oanda_sequential_portfolio_mistake_curriculum as runner
import oanda_sequential_portfolio_mistake_curriculum_verifier as independent_verifier
from src.forex_system.research.sequential_portfolio_mistake_curriculum_v1 import (
    CATEGORY_DEFINITIONS,
    POLICY,
    classify_decision,
    cluster_observations,
    json_text,
    render_markdown,
    validate_verified_binding,
)


ROOT = Path(__file__).resolve().parent


def _record(*, action: str = "enter", episode: str = "episode_1") -> dict:
    return {
        "decision_id": "decision_1",
        "clock_id": "clock_1",
        "sequence_no": 1,
        "decision_epoch": 100,
        "market_episode_id": episode,
        "coarse_situation_id": "coarse_1",
        "position_thesis_id_before": None,
        "physical_path_id": "path_1",
        "currency_resources": ["EUR", "USD"],
        "primary_equity": -2.0,
        "best_alternative_equity": 1.0,
        "decision": {
            "action": action,
            "instrument": "EUR_USD",
            "side": 1,
        },
        "state_before": {"realized_pips": 0.0, "position": None},
        "components": {
            "cost_clear": False,
            "predicted_confidence": 0.7,
            "brier": 0.49,
        },
        "decision_row_sha256": "d" * 64,
        "feedback_row_sha256": "f" * 64,
    }


def _alternative(identifier: str, instrument: str, side: int, equity: float) -> dict:
    return {
        "counterfactual_id": identifier,
        "branch_label": identifier,
        "terminal_equity_pips": equity,
        "action": "enter",
        "instrument": instrument,
        "side": side,
    }


def test_isolation_contract_and_all_categories_are_frozen() -> None:
    assert POLICY["research_only"] is True
    assert POLICY["execution_eligible"] is False
    assert POLICY["can_place_orders"] is False
    assert POLICY["can_authorize"] is False
    assert POLICY["proof_eligible"] is False
    assert POLICY["broker_access"] is False
    assert POLICY["account_access"] is False
    assert POLICY["signal_feed_write"] is False
    assert POLICY["lifecycle_write"] is False
    assert set(CATEGORY_DEFINITIONS) == {
        "direction",
        "entry",
        "management",
        "exit",
        "rotation",
        "cost_awareness",
        "calibration",
        "opportunity_selection",
    }
    source = (
        ROOT
        / "src"
        / "forex_system"
        / "research"
        / "sequential_portfolio_mistake_curriculum_v1.py"
    ).read_text(encoding="utf-8").lower()
    for forbidden in ("requests", "httpx", "authorization_id", "create_order", "close_trade"):
        assert forbidden not in source


def test_classification_separates_direction_entry_cost_calibration_and_selection() -> None:
    alternatives = [
        _alternative("flipped", "EUR_USD", -1, 0.5),
        _alternative("second", "GBP_CHF", 1, 1.0),
        _alternative("wait", "", 0, 0.0),
    ]
    rows = classify_decision(
        _record(), alternatives, minimum_material_regret_pips=0.25
    )
    assert {row["category"] for row in rows} == {
        "direction",
        "entry",
        "cost_awareness",
        "calibration",
        "opportunity_selection",
    }
    direction = next(row for row in rows if row["category"] == "direction")
    assert direction["comparator"]["counterfactual_id"] == "flipped"
    selection = next(row for row in rows if row["category"] == "opportunity_selection")
    assert selection["comparator"]["instrument"] == "GBP_CHF"


@pytest.mark.parametrize(
    ("action", "expected"),
    [("hold", "management"), ("exit", "exit"), ("rotate", "rotation")],
)
def test_action_specific_regret_categories(action: str, expected: str) -> None:
    record = _record(action=action)
    if action in {"hold", "exit"}:
        record["decision"]["instrument"] = "EUR_USD"
        record["decision"]["side"] = None
        record["components"] = {
            "cost_clear": True,
            "predicted_confidence": None,
            "brier": None,
        }
    alternatives = [_alternative("better", "", 0, 1.0)]
    alternatives[0]["action"] = "exit" if action == "hold" else "hold"
    rows = classify_decision(
        record, alternatives, minimum_material_regret_pips=0.25
    )
    assert expected in {row["category"] for row in rows}


def test_resource_graph_clustering_is_transitive_and_episode_bounded() -> None:
    base = classify_decision(
        _record(), [_alternative("wait", "", 0, 0.0)], minimum_material_regret_pips=0.25
    )[0]
    a = deepcopy(base)
    a.update(observation_id="a", currency_resources=["EUR", "USD"])
    b = deepcopy(base)
    b.update(
        observation_id="b",
        decision_id="decision_2",
        sequence_no=2,
        currency_resources=["USD", "JPY"],
    )
    c = deepcopy(base)
    c.update(
        observation_id="c",
        decision_id="decision_3",
        sequence_no=3,
        currency_resources=["JPY", "AUD"],
    )
    d = deepcopy(base)
    d.update(
        observation_id="d",
        decision_id="decision_4",
        sequence_no=4,
        market_episode_id="episode_2",
        currency_resources=["USD", "JPY"],
    )
    clustered, clusters = cluster_observations([d, c, a, b])
    assert len(clusters) == 2
    assert sorted(row["observation_count"] for row in clusters) == [1, 3]
    assert len({row["cluster_id"] for row in clustered if row["market_episode_id"] == "episode_1"}) == 1


def test_canonical_run_is_deterministic_and_source_database_is_unchanged(tmp_path: Path) -> None:
    state = json.loads(runner.DEFAULT_STATE.read_text(encoding="utf-8"))
    database = Path(state["database"])
    before = sha256(database.read_bytes()).hexdigest()
    first, first_json, first_md = runner.run(output_dir=tmp_path / "first")
    second, second_json, second_md = runner.run(output_dir=tmp_path / "second")
    after = sha256(database.read_bytes()).hexdigest()
    assert before == after
    assert first == second
    assert json_text(first) == first_json.read_text(encoding="utf-8")
    assert render_markdown(first) == first_md.read_text(encoding="utf-8")
    assert first_json.read_bytes() == second_json.read_bytes()
    assert first_md.read_bytes() == second_md.read_bytes()
    assert first["source_counts"] == {
        "applied_decisions": 48,
        "counterfactual_rows": 37,
        "feedback_rows": 48,
    }
    assert first["summary"]["raw_observation_count"] > 0
    assert first["summary"]["structural_cluster_count"] < first["summary"]["raw_observation_count"]
    assert first["summary"]["independent_regime_count"] is None
    assert first["cohort_id"].startswith("sequential_portfolio_mistake_curriculum_v1.")
    assert first["report_id"].startswith("sprmistakecurriculum_")
    assert first["report_sha256"] == independent_verifier.stable_hash(
        {key: value for key, value in first.items() if key not in {"report_id", "report_sha256"}}
    )
    receipt = json.loads(
        (
            first_json.parent
            / "sequential_portfolio_mistake_curriculum_verifier_v1.json"
        ).read_text(encoding="utf-8")
    )
    assert receipt["verified"] is True
    assert receipt["failures"] == []
    assert receipt["checks"]["source_roots"] == first["source_binding"]["verified_roots"]


def test_unverified_receipt_fails_closed() -> None:
    state = json.loads(runner.DEFAULT_STATE.read_text(encoding="utf-8"))
    verifier = json.loads(runner.DEFAULT_VERIFIER.read_text(encoding="utf-8"))
    verifier["verified"] = False
    verifier["failures"] = ["fixture"]
    with pytest.raises(ValueError, match="clean independent verification"):
        validate_verified_binding(state, verifier)


def _write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


@pytest.mark.parametrize(
    ("mutation", "expected_failure"),
    [
        ("root", "source_binding"),
        ("count", "source_counts"),
        ("content_id", "report_id"),
        ("safety", "unsafe_report_execution_eligible"),
        ("threshold", "classification_contract"),
        ("contract", "material_contract"),
    ],
)
def test_independent_verifier_rejects_forged_report_fields(
    tmp_path: Path, mutation: str, expected_failure: str
) -> None:
    report, report_path, _ = runner.run(output_dir=tmp_path / "artifact")
    forged = deepcopy(report)
    if mutation == "root":
        forged["source_binding"]["verified_roots"]["feedback"]["count"] += 1
    elif mutation == "count":
        forged["source_counts"]["feedback_rows"] += 1
    elif mutation == "content_id":
        forged["report_id"] = "sprmistakecurriculum_" + "0" * 28
    elif mutation == "safety":
        forged["execution_eligible"] = True
    elif mutation == "threshold":
        forged["classification_contract"]["minimum_material_regret_pips"] = 99.0
    elif mutation == "contract":
        forged["material_contract"]["source_database_sha256"] = "0" * 64
    forged_path = tmp_path / f"forged_{mutation}.json"
    receipt_path = tmp_path / f"receipt_{mutation}.json"
    _write_json(forged_path, forged)
    result = independent_verifier.verify(
        runner.DEFAULT_CONFIG, forged_path, receipt_path
    )
    assert result["verified"] is False
    assert expected_failure in result["failures"]


def test_changed_threshold_or_unsafe_config_cannot_reuse_existing_report(
    tmp_path: Path,
) -> None:
    _, report_path, _ = runner.run(output_dir=tmp_path / "artifact")
    config = json.loads(runner.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    config["classification"]["minimum_material_regret_pips"] = 0.5
    changed = tmp_path / "changed_config.json"
    _write_json(changed, config)
    result = independent_verifier.verify(
        changed, report_path, tmp_path / "changed_receipt.json"
    )
    assert result["verified"] is False
    assert "material_contract" in result["failures"]
    assert "cohort_id" in result["failures"]

    config["execution_eligible"] = True
    unsafe = tmp_path / "unsafe_config.json"
    _write_json(unsafe, config)
    unsafe_result = independent_verifier.verify(
        unsafe, report_path, tmp_path / "unsafe_receipt.json"
    )
    assert unsafe_result["verified"] is False
    assert "unsafe_config_execution_eligible" in unsafe_result["failures"]


def test_content_addressed_cohort_is_immutable(tmp_path: Path) -> None:
    _, report_path, _ = runner.run(output_dir=tmp_path / "artifact")
    report_path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="immutable artifact conflict"):
        runner.run(output_dir=tmp_path / "artifact")


def test_standalone_verifier_does_not_import_producer_or_core() -> None:
    assert independent_verifier.forbidden_imports() == []
