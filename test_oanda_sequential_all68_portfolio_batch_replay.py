from __future__ import annotations

import ast
from copy import deepcopy
from datetime import datetime, timezone
import gzip
from hashlib import sha256
import json
from pathlib import Path

import numpy as np
import pytest

import oanda_sequential_all68_portfolio_batch_replay as producer
import oanda_sequential_all68_portfolio_batch_replay_verifier as verifier
from src.forex_system.research.sequential_all68_portfolio_batch_replay_v1 import (
    PortfolioState,
    apply_fail_closed,
    scheduled_clocks,
    stable_aliases,
)
from src.forex_system.research.sequential_portfolio_replay_v1 import Decision, MarketSeries


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data/oanda_training_manager/research_ledgers/sequential_all68_portfolio_batch_replay_v1/sequential_all68_portfolio_batch_replay_v1.json"
RECEIPT = ROOT / "data/oanda_training_manager/research_ledgers/sequential_all68_portfolio_batch_replay_v1/sequential_all68_portfolio_batch_replay_verifier_v1.json"
EXPANSION_CONFIG = ROOT / "config/sequential_all68_portfolio_batch_replay_wednesday_expansion_v1.json"


def state() -> dict:
    return json.loads(STATE.read_text(encoding="utf-8"))


def rows(name: str) -> list[dict]:
    doc = state()
    spec = doc["datasets"][name]
    path = Path(doc["artifact_root"]) / spec["relative_path"]
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def market_without_fill() -> MarketSeries:
    epochs = np.asarray([0, 60, 120, 180], dtype=np.int64)
    bid = np.asarray([1.0, 1.0001, 1.0002, 1.0003], dtype=float)
    ask = bid + 0.0001
    return MarketSeries(
        "EUR_USD", 0.0001, epochs,
        bid, bid, bid, bid, ask, ask, ask, ask,
        (bid + ask) / 2.0, "x" * 64,
    )


def test_canonical_counts_and_repetition_contract() -> None:
    doc = state()
    assert doc["global_clock_count"] == 144
    assert doc["pair_context_count"] == 9792
    assert doc["market_repetition_count"] == 144
    assert doc["pair_contexts_count_as_rep"] == 0
    assert doc["counterfactuals_count_as_rep"] == 0
    assert doc["replays_count_as_rep"] == 0
    assert doc["independent_regime_count"] is None


def test_wednesday_expansion_config_is_fixed_before_policy_results() -> None:
    config = json.loads(EXPANSION_CONFIG.read_text(encoding="utf-8"))
    producer.validate_config(config)
    assert config["source"]["required_pack_id"] == (
        "sequential_replay_source_pack_v1.554c8f74212202aa9b86"
    )
    assert config["schedule"]["expected_session_count"] == 7
    assert config["schedule"]["expected_global_clock_count"] == 336
    assert config["schedule"]["expected_pair_context_count"] == 22848
    assert config["independence"]["prior_discovery_session_keys"] == [
        "20260826_wed_overlap_prior_discovery"
    ]
    assert config["independence"]["new_scheduled_clock_count"] == 288
    assert config["proof_eligible"] is False
    assert config["supported_decision"] == "no_trade"


def test_all_clocks_keep_all_68_contexts() -> None:
    grouped: dict[str, int] = {}
    for row in rows("pair_contexts"):
        grouped[row["clock_id"]] = grouped.get(row["clock_id"], 0) + 1
    assert len(grouped) == 144
    assert set(grouped.values()) == {68}


def test_stable_alias_contract() -> None:
    doc = state()
    assert doc["pair_aliases"] == stable_aliases(sorted(doc["pair_aliases"]))
    assert sorted(doc["pair_aliases"].values()) == [f"P{number:03d}" for number in range(1, 69)]


def test_missing_coverage_is_retained_not_filtered() -> None:
    doc = state()
    availability = doc["context_availability_counts"]
    assert availability == {
        "missing_causal_context": 1441,
        "missing_exact_execution_quote": 382,
        "missing_exact_feedback_quote": 290,
        "fully_ready": 8333,
    }
    contexts = rows("pair_contexts")
    assert any(not row["causal_ready"] for row in contexts)
    assert any(not row["execution_ready"] for row in contexts)
    assert any(not row["feedback_ready"] for row in contexts)


def test_missing_exact_fill_fails_closed_without_nearest_fill() -> None:
    original = PortfolioState()
    decision = Decision("enter", "EUR_USD", 1, 1, 0.6, 3.0, 5, "x", "y", "z", "c")
    after, result = apply_fail_closed(
        original, decision, {"EUR_USD": market_without_fill()},
        {"execution_epoch": 240},
        {"costs": {"slippage_per_execution_leg_pips": 0.125, "maximum_entry_spread_pips": 5.0}},
        "clock",
    )
    assert after == original
    assert result["status"] == "rejected"
    assert result["rejection_reason"] == "missing_exact_execution_quote"
    assert result["legs"] == []


def test_one_global_decision_and_single_state_chain() -> None:
    decisions = rows("decisions")
    assert len(decisions) == 144
    assert len({row["clock_id"] for row in decisions}) == 144
    assert sum(decision["counts_as_market_repetition"] for decision in decisions) == 144
    assert state()["action_counts"] == {"wait": 50, "enter": 24, "hold": 15, "exit": 24, "rotate": 31}


def test_executable_costs_and_rotation_legs() -> None:
    decisions = rows("decisions")
    rotations = [row for row in decisions if row["decision"]["action"] == "rotate" and row["execution"]["status"] == "applied"]
    assert rotations and all(len(row["execution"]["legs"]) == 2 for row in rotations)
    for row in decisions:
        for leg in row["execution"]["legs"]:
            assert leg["slippage_pips"] == 0.125
            assert leg["spread_pips"] > 0
            pip = 0.01 if leg["instrument"].endswith("_JPY") else 0.0001
            adverse = (leg["executed_price"] - leg["raw_price"]) / pip
            if leg["leg_kind"] == "open":
                assert abs(adverse - 0.125 * leg["side"]) < 1e-8
            else:
                assert abs(adverse + 0.125 * leg["side"]) < 1e-8


def test_source_is_content_bound_inside_cohort() -> None:
    doc = state()
    cohort = Path(doc["artifact_root"])
    assert (cohort / "source_pack_manifest.json").is_file()
    assert (cohort / "source_pack_clean_verifier_receipt.json").is_file()
    assert doc["source_binding"]["archive_count"] == 204
    assert doc["source_binding"]["pack_id"] == "sequential_replay_source_pack_v1.b9d1526f3dfa057bd06d"


def test_terminal_flat_and_nonexecuting_firewall() -> None:
    doc = state()
    assert doc["terminal_flat"] is True
    assert doc["research_only"] is True
    assert doc["execution_eligible"] is False
    assert doc["proof_eligible"] is False
    assert doc["can_promote"] is False
    assert doc["can_place_orders"] is False
    assert doc["can_authorize"] is False
    assert doc["supported_decision"] == "no_trade"
    assert all(row["terminal_flat"] for row in rows("terminals"))


def test_independent_verifier_and_import_isolation() -> None:
    result = verifier.verify()
    assert result["verified"] is True
    assert result["failures"] == []
    tree = ast.parse(Path(verifier.__file__).read_text(encoding="utf-8"))
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
        elif isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
    assert not any("sequential_all68_portfolio" in name or "sequential_portfolio_replay" in name for name in imports)


def test_schedule_is_constructed_without_market_coverage() -> None:
    sessions = [
        {"session_key": "a", "session_start_epoch": 0, "session_end_epoch": 14400},
        {"session_key": "b", "session_start_epoch": 20000, "session_end_epoch": 34400},
        {"session_key": "c", "session_start_epoch": 40000, "session_end_epoch": 54400},
    ]
    clocks = scheduled_clocks(sessions, 5)
    assert len(clocks) == 144
    assert sum(row["terminal_clock"] for row in clocks) == 3
    assert all(row["counts_as_market_repetition"] == 1 for row in clocks)


def test_rerun_is_byte_identical_and_current_pointers_are_exact_copies() -> None:
    first = producer.run()
    first_receipt = verifier.verify()
    assert first_receipt["verified"] is True
    cohort = Path(first["artifact_root"])
    before = {
        path.relative_to(cohort).as_posix(): path.read_bytes()
        for path in sorted(cohort.rglob("*"))
        if path.is_file()
    }
    expected_generated_utc = datetime.fromtimestamp(
        max(int(row["feedback_epoch"]) for row in rows("clocks")),
        tz=timezone.utc,
    ).isoformat()
    assert first["generated_utc"] == expected_generated_utc
    assert first_receipt["generated_utc"] == expected_generated_utc
    assert STATE.read_bytes() == (cohort / "state.json").read_bytes()
    report_pointer = STATE.parent / "SEQUENTIAL_ALL68_PORTFOLIO_BATCH_REPLAY_CURRENT.md"
    assert report_pointer.read_bytes() == (cohort / "report.md").read_bytes()
    assert RECEIPT.read_bytes() == (cohort / "verifier_receipt.json").read_bytes()

    second = producer.run()
    second_receipt = verifier.verify()
    after = {
        path.relative_to(cohort).as_posix(): path.read_bytes()
        for path in sorted(cohort.rglob("*"))
        if path.is_file()
    }
    assert second["cohort_id"] == first["cohort_id"]
    assert second_receipt == first_receipt
    assert after == before


def test_adversarial_self_hash_tamper_is_rejected(tmp_path: Path) -> None:
    row = {"id": "x", "value": 2, "row_sha256": "forged"}
    payload = gzip.compress((json.dumps(row) + "\n").encode("utf-8"), mtime=0)
    path = tmp_path / "rows.jsonl.gz"
    path.write_bytes(payload)
    spec = {
        "relative_path": path.name, "row_count": 1, "gzip_bytes": len(payload),
        "gzip_sha256": sha256(payload).hexdigest(),
        "row_set_sha256": "forged", "ordered_row_sha256": "forged",
    }
    with pytest.raises(ValueError, match="row hash"):
        verifier.load_dataset(tmp_path, spec)


def test_adversarial_dataset_path_traversal_is_rejected(tmp_path: Path) -> None:
    spec = {
        "relative_path": "../outside.jsonl.gz", "row_count": 0, "gzip_bytes": 0,
        "gzip_sha256": "x", "row_set_sha256": "x", "ordered_row_sha256": "x",
    }
    with pytest.raises(ValueError, match="path escape"):
        verifier.load_dataset(tmp_path, spec)


def test_verifier_rejects_forged_safety_and_top_level_summaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_read = verifier.read_json

    def forged_read(path: Path) -> dict:
        value = original_read(path)
        if Path(path).resolve() == verifier.STATE.resolve():
            value = deepcopy(value)
            value.update(
                {
                    "research_only": False,
                    "execution_eligible": True,
                    "proof_eligible": True,
                    "broker_access": True,
                    "account_access": True,
                    "supported_decision": "trade",
                    "action_counts": {"fabricated": 999_999},
                    "global_clock_count": 999_999,
                    "pair_context_count": 1,
                    "execution_leg_count": 0,
                    "context_availability_counts": {"fabricated": 1},
                }
            )
        return value

    monkeypatch.setattr(verifier, "read_json", forged_read)
    monkeypatch.setattr(verifier, "write_json", lambda *_args, **_kwargs: None)
    result = verifier.verify()
    assert result["verified"] is False
    assert "state_safety" in result["failures"]
    assert "state_summary_action_counts" in result["failures"]
    assert "state_summary_global_clock_count" in result["failures"]
    assert "state_summary_pair_context_count" in result["failures"]


def test_verifier_rejects_omitted_feedback_counterfactual_and_terminal_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_load = verifier.load_dataset
    omitted = {
        "feedback.jsonl.gz",
        "counterfactuals.jsonl.gz",
        "session_terminals.jsonl.gz",
    }

    def incomplete_load(root: Path, spec: dict, *args, **kwargs) -> list[dict]:
        if spec.get("relative_path") in omitted:
            return []
        return original_load(root, spec, *args, **kwargs)

    monkeypatch.setattr(verifier, "load_dataset", incomplete_load)
    monkeypatch.setattr(verifier, "write_json", lambda *_args, **_kwargs: None)
    result = verifier.verify()
    assert result["verified"] is False
    assert "feedback_count" in result["failures"]
    assert "counterfactuals_count" in result["failures"]
    assert "terminals_count" in result["failures"]
    assert "feedback_completeness" in result["failures"]
    assert "terminal_completeness" in result["failures"]


def test_verifier_rejects_self_consistent_clock_schedule_forgery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_load = verifier.load_dataset

    def forged_load(root: Path, spec: dict, *args, **kwargs) -> list[dict]:
        rows = original_load(root, spec, *args, **kwargs)
        if spec.get("relative_path") == "global_clocks.jsonl.gz":
            rows = deepcopy(rows)
            rows[0]["decision_epoch"] += 60
            rows[0]["row_sha256"] = verifier.row_hash(rows[0])
        return rows

    monkeypatch.setattr(verifier, "load_dataset", forged_load)
    monkeypatch.setattr(verifier, "write_json", lambda *_args, **_kwargs: None)
    result = verifier.verify()
    assert result["verified"] is False
    assert "clocks_row" in result["failures"]


def test_verifier_rejects_incomplete_dataset_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_read = verifier.read_json

    def incomplete_read(path: Path) -> dict:
        value = original_read(path)
        if Path(path).resolve() == verifier.STATE.resolve():
            value = deepcopy(value)
            value["datasets"].pop("feedback")
        return value

    monkeypatch.setattr(verifier, "read_json", incomplete_read)
    monkeypatch.setattr(verifier, "write_json", lambda *_args, **_kwargs: None)
    result = verifier.verify()
    assert result["verified"] is False
    assert "dataset_manifest" in result["failures"]
