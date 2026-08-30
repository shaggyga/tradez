from __future__ import annotations

import ast
import copy
import gzip
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil

import pytest

import oanda_sequential_all68_policy_challenger as producer
import oanda_sequential_all68_policy_challenger_verifier as verifier
from src.forex_system.research.sequential_all68_policy_challenger_v1 import (
    ARM_NAMES,
    SAFETY,
    factor_consistent_candidates,
    row_hash,
    stable_hash,
)


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "sequential_all68_policy_challenger_v1.json"


def read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> tuple[dict, Path, Path]:
    output = tmp_path_factory.mktemp("policy_challenger_clean")
    return producer.run(config_path=CONFIG, output_dir=output)


def copy_cohort(tmp_path: Path, built: tuple[dict, Path, Path]) -> tuple[Path, Path, Path]:
    state, state_path, _ = built
    target = tmp_path / state["cohort_id"]
    shutil.copytree(state_path.parent, target)
    return target / "state.json", target / "material_contract.json", target / "verifier_receipt.json"


def test_clean_build_is_strictly_verified_and_safe(built: tuple[dict, Path, Path]) -> None:
    state, _, receipt_path = built
    receipt = read_json(receipt_path)
    assert receipt["verified"] is True
    assert receipt["failures"] == []
    for key, expected in SAFETY.items():
        assert state[key] == expected
        assert receipt[key] == expected
    assert state["cohort_id"].startswith("sequential_all68_policy_challenger_v1.")
    assert state["terminal_flat_all_arms"] is True


def test_exact_source_and_curriculum_bindings(built: tuple[dict, Path, Path]) -> None:
    state = built[0]
    assert state["source_binding"]["cohort_id"] == "sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262"
    assert state["source_binding"]["source_pack_id"] == "sequential_replay_source_pack_v1.b9d1526f3dfa057bd06d"
    assert state["source_binding"]["dataset_roots_sha256"] == "623430fa6acc16248427b6fdfaba1ffd99ef8931d79d2ac3dfcfcdcfb2d37dd0"
    assert state["mistake_curriculum_binding"]["cohort_id"] == "sequential_all68_mistake_curriculum_v1.26b13486af3803244125"
    assert state["mistake_curriculum_binding"]["report_id"] == "a68mistakecurriculum_a3b4d3a281c74afcbb245816f572"


def test_matched_arms_report_negative_result_honestly(built: tuple[dict, Path, Path]) -> None:
    state = built[0]
    assert tuple(state["arms"]) == ARM_NAMES
    assert state["arm_summaries"]["v1_baseline_reference"]["realized_pips"] == pytest.approx(-120.25)
    assert state["arm_summaries"]["cost_hurdle_2x"]["realized_pips"] == pytest.approx(-7.5)
    assert state["arm_summaries"]["explicit_hold_vs_switch_2x"]["realized_pips"] < 0.0
    assert state["arm_summaries"]["factor_conflict_suppressed_2x"]["realized_pips"] < 0.0
    assert state["arm_summaries"]["no_trade"]["realized_pips"] == 0.0
    assert state["arm_summaries"]["oof_remaining_move_calibrated_2x"]["realized_pips"] == 0.0
    assert state["arm_summaries"]["oof_remaining_move_calibrated_2x"]["execution_leg_count"] == 0


def test_one_clock_is_one_repetition_and_arms_are_zero_weight(built: tuple[dict, Path, Path]) -> None:
    state, state_path, _ = built
    root = state_path.parent
    clocks = verifier.load_dataset(root, state["datasets"]["clocks"])
    arm_rows = verifier.load_dataset(root, state["datasets"]["arm_decisions"])
    assert len(clocks) == 144
    assert sum(row["counts_as_market_repetition"] for row in clocks) == 144
    assert len(arm_rows) == 144 * len(ARM_NAMES)
    assert sum(row["counts_as_market_repetition"] for row in arm_rows) == 0
    assert state["market_repetition_count"] == 144
    assert state["independent_regime_count"] is None
    assert state["material_contract"]["dataset_limits"] == read_json(CONFIG)["dataset_limits"]


def test_oof_training_is_time_ordered_and_monday_abstains(built: tuple[dict, Path, Path]) -> None:
    state, state_path, _ = built
    root = state_path.parent
    calibrations = verifier.load_dataset(root, state["datasets"]["oof_calibrations"])
    by_session = {}
    for row in calibrations:
        by_session.setdefault(row["application_session_key"], set()).add(tuple(row["training_session_keys"]))
        assert row["same_session_training"] is False
    assert by_session["20260824_mon_overlap"] == {()}
    assert by_session["20260826_wed_overlap"] == {("20260824_mon_overlap",)}
    assert by_session["20260828_fri_overlap"] == {("20260824_mon_overlap", "20260826_wed_overlap")}
    arm_rows = verifier.load_dataset(root, state["datasets"]["arm_decisions"])
    monday = [row for row in arm_rows if row["arm"] == "oof_remaining_move_calibrated_2x" and row["session_key"] == "20260824_mon_overlap"]
    assert {row["decision"]["action"] for row in monday} == {"wait"}


def test_factor_conflict_suppression_uses_signed_currency_votes() -> None:
    rows = [
        {"instrument": "EUR_USD", "side": 1, "expected_move_pips": 5.0, "snapshot_id": "a"},
        {"instrument": "GBP_USD", "side": -1, "expected_move_pips": 2.0, "snapshot_id": "b"},
    ]
    accepted, votes, rejected = factor_consistent_candidates(rows, tie_epsilon=1e-12)
    assert [row["snapshot_id"] for row in accepted] == ["a"]
    assert rejected[0]["snapshot_id"] == "b"
    assert rejected[0]["conflicting_currencies"] == ["USD"]
    assert votes["USD"] < 0.0


def test_verifier_has_no_producer_or_challenger_core_import() -> None:
    tree = ast.parse(Path(verifier.__file__).read_text(encoding="utf-8"))
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
    assert not [name for name in imports if "sequential_all68_policy_challenger" in name]


def test_repeated_build_is_byte_identical_and_non_overwriting(tmp_path: Path) -> None:
    first = producer.run(config_path=CONFIG, output_dir=tmp_path)
    cohort = first[1].parent
    before = {path.relative_to(cohort).as_posix(): path.read_bytes() for path in sorted(cohort.rglob("*")) if path.is_file()}
    second = producer.run(config_path=CONFIG, output_dir=tmp_path)
    after = {path.relative_to(cohort).as_posix(): path.read_bytes() for path in sorted(cohort.rglob("*")) if path.is_file()}
    assert second[0]["cohort_id"] == first[0]["cohort_id"]
    assert after == before


def test_verifier_rejects_forged_state_safety(tmp_path: Path, built: tuple[dict, Path, Path]) -> None:
    state_path, material_path, _ = copy_cohort(tmp_path, built)
    state = read_json(state_path)
    state["execution_eligible"] = True
    state_path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output = state_path.parent / "tampered_receipt.json"
    receipt = verifier.verify(CONFIG, state_path, material_path, output)
    assert receipt["verified"] is False
    assert "state_safety:execution_eligible" in receipt["failures"]


def test_verifier_rejects_self_consistent_decision_tamper(tmp_path: Path, built: tuple[dict, Path, Path]) -> None:
    state_path, material_path, _ = copy_cohort(tmp_path, built)
    state = read_json(state_path)
    spec = state["datasets"]["arm_decisions"]
    rows = verifier.load_dataset(state_path.parent, spec)
    row = rows[0]
    row["decision"]["rationale"] = "forged but re-sealed"
    row["row_sha256"] = row_hash(row)
    raw = b"".join((json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8") for value in rows)
    payload = gzip.compress(raw, compresslevel=9, mtime=0)
    (state_path.parent / spec["relative_path"]).write_bytes(payload)
    spec.update({
        "gzip_bytes": len(payload), "gzip_sha256": sha256(payload).hexdigest(),
        "row_set_sha256": stable_hash(sorted(value["row_sha256"] for value in rows)),
        "ordered_row_sha256": stable_hash([value["row_sha256"] for value in rows]),
    })
    state_path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    receipt = verifier.verify(CONFIG, state_path, material_path, state_path.parent / "tampered_decision_receipt.json")
    assert receipt["verified"] is False
    assert "arm_decision_reconstruction" in receipt["failures"]


def test_verifier_rejects_source_hash_rebinding(tmp_path: Path, built: tuple[dict, Path, Path]) -> None:
    state_path, material_path, _ = copy_cohort(tmp_path / "cohort", built)
    config = read_json(CONFIG)
    config["source"]["required_state_sha256"] = "0" * 64
    config_path = tmp_path / "forged_config.json"
    config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    receipt = verifier.verify(config_path, state_path, material_path, state_path.parent / "source_tamper_receipt.json")
    assert receipt["verified"] is False
    assert any(value.startswith("exception:ValueError:exact source hash changed") for value in receipt["failures"])


def test_dataset_path_traversal_is_rejected(tmp_path: Path) -> None:
    spec = {"relative_path": "../outside.jsonl.gz", "row_count": 0, "gzip_bytes": 0,
            "gzip_sha256": "x", "row_set_sha256": "x", "ordered_row_sha256": "x"}
    with pytest.raises(ValueError, match="unsafe relative path"):
        verifier.load_dataset(tmp_path, spec)


def test_oversized_compressed_dataset_is_rejected_before_decompression(tmp_path: Path) -> None:
    raw = os.urandom(8192)
    payload = gzip.compress(raw, compresslevel=9, mtime=0)
    path = tmp_path / "oversized.jsonl.gz"
    path.write_bytes(payload)
    spec = {
        "relative_path": path.name, "row_count": 1, "gzip_bytes": len(payload),
        "gzip_sha256": sha256(payload).hexdigest(), "row_set_sha256": "x", "ordered_row_sha256": "x",
    }
    limits = {"maximum_gzip_bytes": 1024, "maximum_raw_bytes": 65536, "maximum_row_count": 10}
    with pytest.raises(ValueError, match="declared size exceeds frozen limit"):
        verifier.load_dataset(tmp_path, spec, limits)


def test_gzip_expansion_beyond_raw_cap_is_rejected_streaming(tmp_path: Path) -> None:
    raw = b" " * 8192
    payload = gzip.compress(raw, compresslevel=9, mtime=0)
    path = tmp_path / "expansion.jsonl.gz"
    path.write_bytes(payload)
    spec = {
        "relative_path": path.name, "row_count": 1, "gzip_bytes": len(payload),
        "gzip_sha256": sha256(payload).hexdigest(), "row_set_sha256": "x", "ordered_row_sha256": "x",
    }
    limits = {"maximum_gzip_bytes": 1024, "maximum_raw_bytes": 1024, "maximum_row_count": 10}
    with pytest.raises(ValueError, match="oversized_dataset_raw"):
        verifier.load_dataset(tmp_path, spec, limits)


def test_in_root_linked_dataset_is_rejected(tmp_path: Path) -> None:
    raw = b'{"row_sha256":"x"}\n'
    payload = gzip.compress(raw, compresslevel=9, mtime=0)
    target = tmp_path / "target.jsonl.gz"
    linked = tmp_path / "linked.jsonl.gz"
    target.write_bytes(payload)
    try:
        os.symlink(target, linked)
    except OSError as exc:
        pytest.skip(f"symlink unavailable on this platform: {exc}")
    spec = {
        "relative_path": linked.name, "row_count": 1, "gzip_bytes": len(payload),
        "gzip_sha256": sha256(payload).hexdigest(), "row_set_sha256": "x", "ordered_row_sha256": "x",
    }
    with pytest.raises(ValueError, match="linked path component rejected"):
        verifier.load_dataset(tmp_path, spec)


def test_linked_output_dataset_target_is_rejected(tmp_path: Path) -> None:
    target = tmp_path / "target.jsonl.gz"
    linked = tmp_path / "linked.jsonl.gz"
    target.write_bytes(b"placeholder")
    try:
        os.symlink(target, linked)
    except OSError as exc:
        pytest.skip(f"symlink unavailable on this platform: {exc}")
    with pytest.raises(ValueError, match="linked output dataset rejected"):
        producer.write_dataset(
            linked,
            [{"row_sha256": stable_hash({"value": 1}), "value": 1}],
            {"maximum_gzip_bytes": 1024, "maximum_raw_bytes": 4096, "maximum_row_count": 10},
        )
