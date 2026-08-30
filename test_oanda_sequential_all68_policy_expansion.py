from __future__ import annotations

import ast
import copy
import gzip
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

import oanda_sequential_all68_policy_expansion as producer
import oanda_sequential_all68_policy_expansion_verifier as verifier
import oanda_sequential_all68_policy_challenger as hardened
import oanda_sequential_all68_policy_challenger_verifier as independent


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "sequential_all68_policy_expansion_v1.json"
SESSIONS = (
    "20260715_wed_overlap",
    "20260722_wed_overlap",
    "20260729_wed_overlap",
    "20260805_wed_overlap",
    "20260812_wed_overlap",
    "20260819_wed_overlap",
    "20260826_wed_overlap_prior_discovery",
)


def read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> tuple[dict, Path, Path]:
    output = tmp_path_factory.mktemp("policy_expansion_clean")
    return producer.run(config_path=CONFIG, output_dir=output)


def copy_cohort(tmp_path: Path, built: tuple[dict, Path, Path]) -> tuple[Path, Path]:
    state, state_path, _ = built
    target = tmp_path / state["cohort_id"]
    shutil.copytree(state_path.parent, target)
    return target / "state.json", target / "material_contract.json"


def test_clean_build_is_strictly_verified_and_nonexecuting(built: tuple[dict, Path, Path]) -> None:
    state, _, receipt_path = built
    receipt = read_json(receipt_path)
    assert receipt["verified"] is True
    assert receipt["failures"] == []
    for key, expected in verifier.SAFETY.items():
        assert state[key] == expected
        assert receipt[key] == expected
    assert state["cohort_id"].startswith("sequential_all68_policy_expansion_v1.")
    assert state["source_binding"]["cohort_id"] == "seq_a68_wed_exp_v1.e7de4ecdd0255eb13306"
    assert state["source_binding"]["source_pack_id"] == "sequential_replay_source_pack_v1.554c8f74212202aa9b86"
    assert state["source_binding"]["dataset_roots_sha256"] == "aa00c3203d32f5c404057f0c854d5513714b60df5c0196e2f44cf3680c19081c"
    assert state["terminal_flat_all_arms"] is True


def test_all_336_clocks_are_retained_as_the_only_repetitions(built: tuple[dict, Path, Path]) -> None:
    state, state_path, _ = built
    clocks = independent.load_dataset(state_path.parent, state["datasets"]["clocks"], read_json(CONFIG)["dataset_limits"])
    arms = independent.load_dataset(state_path.parent, state["datasets"]["arm_decisions"], read_json(CONFIG)["dataset_limits"])
    assert len(clocks) == 336
    assert sum(row["counts_as_market_repetition"] for row in clocks) == 336
    assert len(arms) == 336 * 6
    assert sum(row["counts_as_market_repetition"] for row in arms) == 0
    assert sum(row["source_candidate_count"] for row in clocks) == 928
    assert {row["source_feedback_status"] for row in clocks}
    assert state["source_pair_context_count"] == 22848
    assert state["source_context_availability_counts"]["missing_causal_context"] == 3032


def test_generic_oof_map_uses_every_and_only_prior_wednesday(built: tuple[dict, Path, Path]) -> None:
    state, state_path, _ = built
    expected = {session: list(SESSIONS[:index]) for index, session in enumerate(SESSIONS)}
    assert state["application_training_sessions"] == expected
    assert state["session_roles"] == {
        "all_sessions_are_historical_training": True,
        "prior_discovery_session_keys": [SESSIONS[-1]],
    }
    rows = independent.load_dataset(
        state_path.parent, state["datasets"]["oof_calibrations"], read_json(CONFIG)["dataset_limits"]
    )
    for row in rows:
        assert row["training_session_keys"] == expected[row["application_session_key"]]
        assert row["same_session_training"] is False
        assert row["future_session_training"] is False
        assert row["historical_training"] is True
        assert row["proof_eligible"] is False
    first = [row for row in rows if row["application_session_key"] == SESSIONS[0]]
    assert first and {tuple(row["training_session_keys"]) for row in first} == {()}


def test_results_are_reported_as_historical_training_not_proof(built: tuple[dict, Path, Path]) -> None:
    state = built[0]
    summaries = state["arm_summaries"]
    assert summaries["v1_baseline_reference"]["realized_pips"] == pytest.approx(-186.2)
    assert summaries["cost_hurdle_2x"]["realized_pips"] == pytest.approx(1.35)
    assert summaries["explicit_hold_vs_switch_2x"]["realized_pips"] == pytest.approx(16.1)
    assert summaries["factor_conflict_suppressed_2x"]["realized_pips"] == pytest.approx(16.1)
    assert summaries["no_trade"]["realized_pips"] == 0.0
    assert summaries["oof_remaining_move_calibrated_2x"]["realized_pips"] == 0.0
    assert summaries["oof_remaining_move_calibrated_2x"]["execution_leg_count"] == 0
    assert state["evidence_role"] == "historical_training_policy_expansion"
    assert state["proof_eligible"] is False
    assert state["supported_decision"] == "no_trade"


def test_repeated_build_is_byte_identical(tmp_path: Path) -> None:
    first = producer.run(config_path=CONFIG, output_dir=tmp_path)
    root = first[1].parent
    before = {path.relative_to(root).as_posix(): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}
    second = producer.run(config_path=CONFIG, output_dir=tmp_path)
    after = {path.relative_to(root).as_posix(): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}
    assert second[0]["cohort_id"] == first[0]["cohort_id"]
    assert after == before


def test_verifier_is_runnable_without_producer_or_core_import() -> None:
    tree = ast.parse(Path(verifier.__file__).read_text(encoding="utf-8"))
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
    assert "oanda_sequential_all68_policy_expansion" not in imports
    assert not [name for name in imports if name.startswith("src.forex_system.research")]
    foundation_tree = ast.parse((ROOT / "oanda_sequential_all68_policy_challenger_verifier.py").read_text(encoding="utf-8"))
    foundation_imports = []
    for node in ast.walk(foundation_tree):
        if isinstance(node, ast.Import):
            foundation_imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            foundation_imports.append(node.module or "")
    assert not [name for name in foundation_imports if "sequential_all68_policy_challenger" in name]


def test_verifier_rejects_resealed_decision_tamper(tmp_path: Path, built: tuple[dict, Path, Path]) -> None:
    state_path, material_path = copy_cohort(tmp_path, built)
    state = read_json(state_path)
    spec = state["datasets"]["arm_decisions"]
    rows = independent.load_dataset(state_path.parent, spec, read_json(CONFIG)["dataset_limits"])
    rows[0]["decision"]["rationale"] = "forged and re-sealed"
    rows[0]["row_sha256"] = independent.row_hash(rows[0])
    raw = b"".join((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8") for row in rows)
    payload = gzip.compress(raw, compresslevel=9, mtime=0)
    (state_path.parent / spec["relative_path"]).write_bytes(payload)
    spec.update({
        "gzip_bytes": len(payload),
        "gzip_sha256": sha256(payload).hexdigest(),
        "row_set_sha256": independent.stable_hash(sorted(row["row_sha256"] for row in rows)),
        "ordered_row_sha256": independent.stable_hash([row["row_sha256"] for row in rows]),
    })
    state_path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    receipt = verifier.verify(CONFIG, state_path, material_path, state_path.parent / "tampered_receipt.json")
    assert receipt["verified"] is False
    assert "arm_decision_reconstruction" in receipt["failures"]


def test_verifier_rejects_source_hash_rebinding(tmp_path: Path, built: tuple[dict, Path, Path]) -> None:
    state_path, material_path = copy_cohort(tmp_path / "copy", built)
    config = copy.deepcopy(read_json(CONFIG))
    config["source"]["required_state_sha256"] = "0" * 64
    config_path = tmp_path / "forged_config.json"
    config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    receipt = verifier.verify(config_path, state_path, material_path, state_path.parent / "source_tamper_receipt.json")
    assert receipt["verified"] is False
    assert any(value.startswith("exception:ValueError:exact source hash changed") for value in receipt["failures"])


def test_dataset_path_escape_is_rejected(tmp_path: Path) -> None:
    spec = {
        "relative_path": "../outside.jsonl.gz", "row_count": 0, "gzip_bytes": 1,
        "gzip_sha256": "x", "row_set_sha256": "x", "ordered_row_sha256": "x",
    }
    with pytest.raises(ValueError, match="unsafe relative path"):
        independent.load_dataset(tmp_path, spec, read_json(CONFIG)["dataset_limits"])


def test_oversized_gzip_expansion_is_rejected_under_raw_cap(tmp_path: Path) -> None:
    payload = gzip.compress(b"x" * 8192, compresslevel=9, mtime=0)
    path = tmp_path / "bomb.jsonl.gz"
    path.write_bytes(payload)
    spec = {
        "relative_path": path.name, "row_count": 1, "gzip_bytes": len(payload),
        "gzip_sha256": sha256(payload).hexdigest(), "row_set_sha256": "x", "ordered_row_sha256": "x",
    }
    limits = {"maximum_gzip_bytes": 1024, "maximum_raw_bytes": 1024, "maximum_row_count": 10}
    with pytest.raises(ValueError, match="oversized_dataset_raw"):
        independent.load_dataset(tmp_path, spec, limits)
    with pytest.raises(ValueError, match="oversized_dataset_raw"):
        hardened.read_gzip_bounded(path, limits)


def test_linked_or_reparse_input_and_output_paths_are_rejected(tmp_path: Path) -> None:
    target = tmp_path / "target.jsonl.gz"
    linked = tmp_path / "linked.jsonl.gz"
    target.write_bytes(gzip.compress(b'{}\n', compresslevel=9, mtime=0))
    try:
        os.symlink(target, linked)
        relative = linked.name
        linked_path = linked
    except OSError:
        target_root = tmp_path / "target_root"
        linked_root = tmp_path / "linked_root"
        target_root.mkdir()
        target = target_root / "target.jsonl.gz"
        target.write_bytes(gzip.compress(b'{}\n', compresslevel=9, mtime=0))
        completed = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(linked_root), str(target_root)],
            text=True, capture_output=True,
        )
        if completed.returncode != 0:
            pytest.skip("neither symlink nor directory junction is available")
        relative = str(Path(linked_root.name) / target.name)
        linked_path = linked_root / target.name
    spec = {
        "relative_path": relative, "row_count": 1, "gzip_bytes": linked_path.stat().st_size,
        "gzip_sha256": sha256(linked_path.read_bytes()).hexdigest(), "row_set_sha256": "x", "ordered_row_sha256": "x",
    }
    with pytest.raises(ValueError, match="linked path component rejected"):
        independent.load_dataset(tmp_path, spec, read_json(CONFIG)["dataset_limits"])
    output_relative = str(Path(relative).parent / "output.jsonl.gz")
    with pytest.raises(ValueError, match="linked path component rejected"):
        hardened.safe_relative(tmp_path, output_relative)
