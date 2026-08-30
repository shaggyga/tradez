from __future__ import annotations

import ast
import copy
import json
from pathlib import Path

import pytest

import oanda_sequential_all68_mistake_curriculum as producer
import oanda_sequential_all68_mistake_curriculum_verifier as verifier
from src.forex_system.research.sequential_all68_mistake_curriculum_v1 import (
    CATEGORY_DEFINITIONS,
    POLICY,
    stable_hash,
)


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "sequential_all68_mistake_curriculum_v1.json"


def read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> tuple[dict, Path, Path, Path]:
    output = tmp_path_factory.mktemp("a68_mistake_clean")
    report, report_path, _, receipt_path = producer.run(config_path=CONFIG, output_dir=output)
    material_path = report_path.parent / "material_contract_v1.json"
    return report, report_path, material_path, receipt_path


def verify_copy(
    tmp_path: Path,
    *,
    report: dict,
    material: dict,
    config: dict | None = None,
) -> dict:
    tmp_path.mkdir(parents=True, exist_ok=True)
    report_path = tmp_path / "report.json"
    material_path = tmp_path / "material.json"
    config_path = tmp_path / "config.json"
    output_path = tmp_path / "receipt.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    material_path.write_text(json.dumps(material, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    config_path.write_text(json.dumps(config or read_json(CONFIG), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return verifier.verify(config_path, report_path, material_path, output_path)


def test_clean_curriculum_is_verified_and_content_addressed(built: tuple[dict, Path, Path, Path]) -> None:
    report, report_path, material_path, receipt_path = built
    receipt = read_json(receipt_path)
    assert receipt["verified"] is True
    assert receipt["failures"] == []
    assert report["cohort_id"].startswith("sequential_all68_mistake_curriculum_v1.")
    assert report["material_contract_sha256"] == stable_hash(read_json(material_path))
    assert report["report_id"] == "a68mistakecurriculum_" + report["report_sha256"][:28]
    assert report_path.parent.name == report["cohort_id"]


def test_safety_and_source_bindings_are_exact(built: tuple[dict, Path, Path, Path]) -> None:
    report, _, _, receipt_path = built
    for key, expected in POLICY.items():
        assert report[key] == expected
        assert read_json(receipt_path)[key] == expected
    assert report["evidence_role"] == "historical_training_curriculum"
    binding = report["source_binding"]
    assert binding["cohort_id"] == "sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262"
    assert binding["source_pack_id"] == "sequential_replay_source_pack_v1.b9d1526f3dfa057bd06d"
    config = read_json(CONFIG)["source"]
    assert binding["source_state_sha256"] == config["required_state_sha256"]
    assert binding["source_verifier_sha256"] == config["required_verifier_sha256"]
    assert binding["source_state_semantic_sha256"] == config["required_state_semantic_sha256"]
    assert binding["source_verifier_semantic_sha256"] == config["required_verifier_semantic_sha256"]
    assert binding["source_dataset_roots_sha256"] == config["required_dataset_roots_sha256"]


def test_primary_and_reviews_have_frozen_noninflating_weights(built: tuple[dict, Path, Path, Path]) -> None:
    report = built[0]
    primary = [row for row in report["curriculum_rows"] if row["row_role"] == "primary_feedback"]
    reviews = [row for row in report["curriculum_rows"] if row["row_role"] == "depth_one_review"]
    assert len(primary) == 144
    assert len(reviews) == 172
    assert {row["curriculum_weight"] for row in primary} == {1}
    assert {row["counts_as_market_repetition"] for row in primary} == {1}
    assert {row["curriculum_weight"] for row in reviews} == {0}
    assert {row["counts_as_market_repetition"] for row in reviews} == {0}
    assert all(row["mistake_labels"] == [] for row in reviews)
    assert report["summary"]["primary_weight_sum"] == 144
    assert report["summary"]["review_weight_sum"] == 0
    assert report["summary"]["deduplicated_primary_weight"] == 144
    assert report["summary"]["independent_regime_count"] is None


def test_all_requested_error_classes_are_present(built: tuple[dict, Path, Path, Path]) -> None:
    report = built[0]
    assert set(report["categories"]) == set(CATEGORY_DEFINITIONS)
    assert all(report["categories"][name]["label_count"] > 0 for name in CATEGORY_DEFINITIONS)
    observed = {
        label["category"]
        for row in report["curriculum_rows"]
        for label in row["mistake_labels"]
    }
    assert observed == set(CATEGORY_DEFINITIONS)


def test_clustering_uses_episode_clock_and_connected_currency_resources(built: tuple[dict, Path, Path, Path]) -> None:
    report = built[0]
    rows = {row["curriculum_row_id"]: row for row in report["curriculum_rows"]}
    for cluster in report["feedback_resource_components"]:
        members = [rows[identifier] for identifier in cluster["curriculum_row_ids"]]
        assert {row["session_episode_id"] for row in members} == {cluster["session_episode_id"]}
        assert {row["clock_id"] for row in members} == {cluster["clock_id"]}
        assert cluster["deduplicated_primary_weight"] in {0, 1}
        assert cluster["counts_as_regime_repetition"] == 0
    for cluster in report["structural_clusters"]:
        members = [rows[identifier] for identifier in cluster["curriculum_row_ids"]]
        assert {row["session_episode_id"] for row in members} == {cluster["session_episode_id"]}
        assert {row["clock_id"] for row in members} == set(cluster["global_clock_ids"])
        assert all(
            any(label["category"] == cluster["category"] for label in row["mistake_labels"])
            for row in members
        )
        assert cluster["counts_as_regime_repetition"] == 0
    # A primary and its flipped review retain opposite signed exposures but
    # share one underlying-currency component rather than two observations.
    primary = next(
        row for row in report["curriculum_rows"]
        if any(label["category"] == "direction" for label in row["mistake_labels"])
    )
    label = next(label for label in primary["mistake_labels"] if label["category"] == "direction")
    flipped = next(row for row in report["curriculum_rows"] if row["counterfactual_id"] == label["comparator_counterfactual_id"])
    assert primary["structural_cluster_id"] == flipped["structural_cluster_id"]
    assert set(primary["signed_currency_resources"]) != set(flipped["signed_currency_resources"])


def test_standalone_verifier_has_no_producer_or_core_import() -> None:
    source = Path(verifier.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    assert not [name for name in imported if "sequential_all68_mistake_curriculum" in name]
    assert verifier.forbidden_imports() == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("execution_eligible", True),
        ("proof_eligible", True),
        ("can_promote", True),
        ("can_place_orders", True),
        ("can_authorize", True),
        ("broker_access", True),
        ("account_access", True),
        ("supported_decision", "trade"),
    ],
)
def test_verifier_rejects_forged_report_safety(
    tmp_path: Path,
    built: tuple[dict, Path, Path, Path],
    field: str,
    value: object,
) -> None:
    report = copy.deepcopy(built[0])
    report[field] = value
    result = verify_copy(tmp_path, report=report, material=read_json(built[2]))
    assert result["verified"] is False
    assert "full_report_reconstruction" in result["failures"]


def test_verifier_rejects_forged_label_and_review_weight(tmp_path: Path, built: tuple[dict, Path, Path, Path]) -> None:
    material = read_json(built[2])
    forged_label = copy.deepcopy(built[0])
    primary = next(row for row in forged_label["curriculum_rows"] if row["mistake_labels"])
    primary["mistake_labels"][0]["category"] = "made_up_edge"
    result = verify_copy(tmp_path / "label", report=forged_label, material=material)
    assert result["verified"] is False

    forged_weight = copy.deepcopy(built[0])
    review = next(row for row in forged_weight["curriculum_rows"] if row["row_role"] == "depth_one_review")
    review["curriculum_weight"] = 1
    result = verify_copy(tmp_path / "weight", report=forged_weight, material=material)
    assert result["verified"] is False


def test_verifier_rejects_forged_cluster_and_material(tmp_path: Path, built: tuple[dict, Path, Path, Path]) -> None:
    material = read_json(built[2])
    forged_cluster = copy.deepcopy(built[0])
    forged_cluster["structural_clusters"][0]["deduplicated_primary_weight"] = 99
    result = verify_copy(tmp_path / "cluster", report=forged_cluster, material=material)
    assert result["verified"] is False

    forged_material = copy.deepcopy(material)
    forged_material["source_dataset_roots_sha256"] = "0" * 64
    result = verify_copy(tmp_path / "material", report=built[0], material=forged_material)
    assert result["verified"] is False
    assert "material_file" in result["failures"]


@pytest.mark.parametrize(
    "source_field",
    [
        "required_state_sha256",
        "required_verifier_sha256",
        "required_state_semantic_sha256",
        "required_verifier_semantic_sha256",
        "required_source_pack_manifest_sha256",
        "required_source_pack_verifier_sha256",
        "required_dataset_roots_sha256",
    ],
)
def test_verifier_rejects_source_receipt_hash_or_root_rebinding(
    tmp_path: Path,
    built: tuple[dict, Path, Path, Path],
    source_field: str,
) -> None:
    config = read_json(CONFIG)
    config["source"][source_field] = "f" * 64
    result = verify_copy(tmp_path, report=built[0], material=read_json(built[2]), config=config)
    assert result["verified"] is False
    assert any(failure.startswith("exception:") for failure in result["failures"])


def test_repeated_build_is_byte_stable_and_does_not_overwrite(tmp_path: Path) -> None:
    first = producer.run(config_path=CONFIG, output_dir=tmp_path)
    before = first[1].read_bytes()
    second = producer.run(config_path=CONFIG, output_dir=tmp_path)
    assert second[0]["cohort_id"] == first[0]["cohort_id"]
    assert second[0]["report_id"] == first[0]["report_id"]
    assert second[1].read_bytes() == before
