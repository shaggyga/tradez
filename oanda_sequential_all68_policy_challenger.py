#!/usr/bin/env python3
"""Build the frozen, nonexecuting all-68 policy challenger sidecar."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gzip
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Iterable, Mapping

from src.forex_system.research.sequential_deterministic_io_v1 import canonical_gzip

from src.forex_system.research.sequential_all68_policy_challenger_v1 import (
    ARM_NAMES,
    SAFETY,
    apply_action,
    build_calibration,
    calibrate_candidates,
    candidate_roundtrip_observation,
    choose_cost_hurdle,
    choose_explicit_hold_vs_switch,
    choose_no_trade,
    eligible_2x,
    factor_consistent_candidates,
    flat_state,
    liquidation_equity,
    row_hash,
    seal_row,
    semantic_sha256,
    stable_hash,
)
from src.forex_system.research.sequential_portfolio_replay_v1 import load_archived_market


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "sequential_all68_policy_challenger_v1.json"
CORE = ROOT / "src" / "forex_system" / "research" / "sequential_all68_policy_challenger_v1.py"
FOUNDATION_CORE = ROOT / "src" / "forex_system" / "research" / "sequential_portfolio_replay_v1.py"
VERIFIER = ROOT / "oanda_sequential_all68_policy_challenger_verifier.py"
ALLOWED_LEDGER_ROOT = ROOT / "data" / "oanda_training_manager" / "research_ledgers"
ARTIFACT_TIME_CONTRACT = {
    "kind": "maximum_bound_source_feedback_epoch",
    "timezone": "UTC",
    "wall_clock_independent": True,
}


def read_json(path: Path) -> dict[str, Any]:
    require_regular_file(path, maximum_bytes=16 * 1024 * 1024)
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_sha(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while block := handle.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def is_link_or_reparse(path: Path) -> bool:
    try:
        attributes = int(getattr(path.lstat(), "st_file_attributes", 0)) if path.exists() or path.is_symlink() else 0
        return path.is_symlink() or bool(os.path.islink(path)) or bool(attributes & 0x400)
    except OSError:
        return True


def require_regular_file(path: Path, *, maximum_bytes: int | None = None) -> None:
    if not path.is_file() or is_link_or_reparse(path):
        raise ValueError(f"missing_or_linked_file:{path.name}")
    if maximum_bytes is not None and (path.stat().st_size <= 0 or path.stat().st_size > int(maximum_bytes)):
        raise ValueError(f"oversized_bound_file:{path.name}")


def reject_link_chain(path: Path) -> None:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current = current / part
        if (current.exists() or current.is_symlink()) and is_link_or_reparse(current):
            raise ValueError("linked path component rejected")


def safe_relative(root: Path, relative: str) -> Path:
    raw = Path(str(relative))
    if raw.is_absolute() or ".." in raw.parts:
        raise ValueError("unsafe relative path")
    reject_link_chain(root)
    unresolved = root / raw
    current = root
    for part in raw.parts:
        current = current / part
        if (current.exists() or current.is_symlink()) and is_link_or_reparse(current):
            raise ValueError("linked path component rejected")
    path = unresolved.resolve()
    root_resolved = root.resolve()
    if root_resolved not in path.parents and path != root_resolved:
        raise ValueError("path escapes root")
    return path


def atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".tmp-{os.getpid()}-{stable_hash(str(path))[:12]}"
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def immutable_bytes(path: Path, payload: bytes) -> None:
    if path.exists():
        if path.read_bytes() != payload:
            raise ValueError(f"immutable artifact conflict: {path}")
        return
    atomic_bytes(path, payload)


def json_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def deterministic_jsonl(rows: Iterable[Mapping[str, Any]]) -> bytes:
    raw = b"".join((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8") for row in rows)
    return canonical_gzip(raw, compresslevel=9)


def read_gzip_bounded(path: Path, limits: Mapping[str, Any]) -> bytes:
    require_regular_file(path, maximum_bytes=int(limits["maximum_gzip_bytes"]))
    with path.open("rb") as raw, gzip.GzipFile(fileobj=raw, mode="rb") as stream:
        value = stream.read(int(limits["maximum_raw_bytes"]) + 1)
        if len(value) > int(limits["maximum_raw_bytes"]):
            raise ValueError("oversized_dataset_raw")
        if stream.read(1):
            raise ValueError("oversized_dataset_tail")
    return value


def write_dataset(path: Path, rows: list[dict[str, Any]], limits: Mapping[str, Any]) -> dict[str, Any]:
    if len(rows) > int(limits["maximum_row_count"]):
        raise ValueError("oversized_dataset_row_count")
    raw = b"".join((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8") for row in rows)
    if len(raw) > int(limits["maximum_raw_bytes"]):
        raise ValueError("oversized_dataset_raw")
    payload = canonical_gzip(raw, compresslevel=9)
    if len(payload) > int(limits["maximum_gzip_bytes"]):
        raise ValueError("oversized_dataset_gzip")
    if path.exists() and is_link_or_reparse(path):
        raise ValueError("linked output dataset rejected")
    immutable_bytes(path, payload)
    return {
        "relative_path": path.name,
        "row_count": len(rows),
        "gzip_bytes": len(payload),
        "gzip_sha256": sha256(payload).hexdigest(),
        "row_set_sha256": stable_hash(sorted(row["row_sha256"] for row in rows)),
        "ordered_row_sha256": stable_hash([row["row_sha256"] for row in rows]),
    }


def load_dataset(root: Path, spec: Mapping[str, Any], limits: Mapping[str, Any]) -> list[dict[str, Any]]:
    path = safe_relative(root, str(spec["relative_path"]))
    if int(spec["gzip_bytes"]) > int(limits["maximum_gzip_bytes"]) or int(spec["row_count"]) > int(limits["maximum_row_count"]):
        raise ValueError("source dataset declared size exceeds frozen limit")
    require_regular_file(path, maximum_bytes=int(limits["maximum_gzip_bytes"]))
    payload_sha = file_sha(path)
    if path.stat().st_size != int(spec["gzip_bytes"]) or payload_sha != spec["gzip_sha256"]:
        raise ValueError("source dataset gzip identity mismatch")
    raw = read_gzip_bounded(path, limits)
    rows = [json.loads(line) for line in raw.decode("utf-8").splitlines() if line]
    if len(rows) != int(spec["row_count"]):
        raise ValueError("source dataset row count mismatch")
    if any(row_hash(row) != row.get("row_sha256") for row in rows):
        raise ValueError("source dataset row hash mismatch")
    if stable_hash(sorted(row["row_sha256"] for row in rows)) != spec["row_set_sha256"]:
        raise ValueError("source dataset row-set mismatch")
    if stable_hash([row["row_sha256"] for row in rows]) != spec["ordered_row_sha256"]:
        raise ValueError("source dataset order mismatch")
    return rows


def validate_config(config: Mapping[str, Any]) -> None:
    for key, expected in SAFETY.items():
        if config.get(key) != expected:
            raise ValueError(f"unsafe config: {key}")
    if tuple(config.get("arms", ())) != ARM_NAMES:
        raise ValueError("frozen challenger arm set or order changed")
    if float(config["policy"]["minimum_entry_score_cost_ratio"]) != 2.0:
        raise ValueError("2x cost hurdle changed")
    if config["policy"]["no_interim_retuning"] is not True:
        raise ValueError("interim retuning enabled")
    oof = config["oof_calibration"]
    if not all(oof[key] is True for key in ("same_session_training_forbidden", "future_session_training_forbidden")):
        raise ValueError("noncausal OOF training enabled")
    independence = config["independence"]
    for key in ("arms_count_as_market_repetitions", "candidates_count_as_market_repetitions", "pairs_count_as_market_repetitions", "reruns_count_as_market_repetitions"):
        if independence[key] is not False:
            raise ValueError(f"repetition inflation: {key}")
    if independence["independent_regime_count"] is not None:
        raise ValueError("independent regimes must remain unknown")
    limits = config["dataset_limits"]
    if not (0 < int(limits["maximum_gzip_bytes"]) <= int(limits["maximum_raw_bytes"])):
        raise ValueError("invalid frozen dataset byte limits")
    if not (0 < int(limits["maximum_row_count"]) <= 100_000):
        raise ValueError("invalid frozen dataset row limit")


def source_contract(config: Mapping[str, Any]) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any], dict[str, list[dict[str, Any]]]]:
    source = config["source"]
    root = safe_relative(ROOT, source["cohort_relative_root"])
    state_path, receipt_path = root / "state.json", root / "verifier_receipt.json"
    manifest_path, pack_receipt_path = root / "source_pack_manifest.json", root / "source_pack_clean_verifier_receipt.json"
    for path, required in (
        (state_path, source["required_state_sha256"]),
        (receipt_path, source["required_verifier_sha256"]),
        (manifest_path, source["required_source_pack_manifest_sha256"]),
        (pack_receipt_path, source["required_source_pack_verifier_sha256"]),
    ):
        require_regular_file(path, maximum_bytes=16 * 1024 * 1024)
        if file_sha(path) != required:
            raise ValueError(f"exact bound source hash changed: {path.name}")
    state, receipt = read_json(state_path), read_json(receipt_path)
    manifest, pack_receipt = read_json(manifest_path), read_json(pack_receipt_path)
    if state.get("cohort_id") != source["required_cohort_id"]:
        raise ValueError("source cohort mismatch")
    if state.get("source_binding", {}).get("pack_id") != source["required_source_pack_id"]:
        raise ValueError("source pack mismatch")
    if semantic_sha256(state, "generated_utc") != source["required_state_semantic_sha256"]:
        raise ValueError("source semantic state changed")
    if semantic_sha256(receipt, "generated_utc") != source["required_verifier_semantic_sha256"]:
        raise ValueError("source semantic verifier changed")
    if receipt.get("verified") is not True or receipt.get("failures") != []:
        raise ValueError("source replay is not cleanly verified")
    if pack_receipt.get("verified") is not True or pack_receipt.get("failures") != []:
        raise ValueError("source pack is not cleanly verified")
    if manifest.get("pack_id") != source["required_source_pack_id"]:
        raise ValueError("copied source pack manifest mismatch")
    if stable_hash(state["datasets"]) != source["required_dataset_roots_sha256"]:
        raise ValueError("source dataset roots changed")
    datasets = {key: load_dataset(root, spec, config["dataset_limits"]) for key, spec in state["datasets"].items()}
    binding = {
        "cohort_id": state["cohort_id"],
        "source_pack_id": manifest["pack_id"],
        "state_sha256": file_sha(state_path),
        "verifier_sha256": file_sha(receipt_path),
        "state_semantic_sha256": semantic_sha256(state, "generated_utc"),
        "verifier_semantic_sha256": semantic_sha256(receipt, "generated_utc"),
        "source_pack_manifest_sha256": file_sha(manifest_path),
        "source_pack_verifier_sha256": file_sha(pack_receipt_path),
        "dataset_roots_sha256": stable_hash(state["datasets"]),
        "dataset_specs": state["datasets"],
    }
    return root, state, manifest, binding, datasets


def mistake_contract(config: Mapping[str, Any]) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    contract = config["mistake_curriculum"]
    root = safe_relative(ROOT, contract["cohort_relative_root"])
    paths = {
        "report": root / "SEQUENTIAL_ALL68_MISTAKE_CURRICULUM_V1.json",
        "material": root / "material_contract_v1.json",
        "verifier": root / "verifier_receipt.json",
    }
    for name, required in (
        ("report", contract["required_report_sha256"]),
        ("material", contract["required_material_sha256"]),
        ("verifier", contract["required_verifier_sha256"]),
    ):
        require_regular_file(paths[name], maximum_bytes=16 * 1024 * 1024)
        if file_sha(paths[name]) != required:
            raise ValueError(f"exact mistake-curriculum {name} changed")
    report, receipt = read_json(paths["report"]), read_json(paths["verifier"])
    if report.get("cohort_id") != contract["required_cohort_id"] or report.get("report_id") != contract["required_report_id"]:
        raise ValueError("mistake-curriculum identity mismatch")
    if receipt.get("verified") is not True or receipt.get("failures") != []:
        raise ValueError("mistake curriculum is not cleanly verified")
    binding = {
        "cohort_id": report["cohort_id"], "report_id": report["report_id"],
        "report_sha256": file_sha(paths["report"]), "material_sha256": file_sha(paths["material"]),
        "verifier_sha256": file_sha(paths["verifier"]), "summary": report["summary"],
        "category_materiality_pips": {key: value["materiality_pips"] for key, value in sorted(report["categories"].items())},
    }
    return root, report, binding


def load_markets(manifest: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    source_root = safe_relative(ROOT, config["source"]["source_pack_relative_root"])
    markets: dict[str, dict[str, Any]] = {}
    for row in manifest["source_manifest"]:
        session = str(row["session_key"])
        archive = safe_relative(source_root, row["archive_relative_path"])
        require_regular_file(archive, maximum_bytes=int(config["source"]["maximum_archive_gzip_bytes"]))
        if file_sha(archive) != row["gzip_sha256"]:
            raise ValueError("source archive gzip hash mismatch")
        markets.setdefault(session, {})[str(row["instrument"])] = load_archived_market(
            archive,
            instrument=str(row["instrument"]),
            expected_raw_sha256=str(row["raw_sha256"]),
            maximum_raw_bytes=int(config["source"]["maximum_archive_raw_bytes"]),
        )
    if any(len(value) != 68 for value in markets.values()):
        raise ValueError("source session lacks all 68 instruments")
    return markets


def training_session_map(config: Mapping[str, Any]) -> dict[str, list[str]]:
    oof = config["oof_calibration"]
    return {
        "20260824_mon_overlap": list(oof["monday_training_sessions"]),
        "20260826_wed_overlap": list(oof["wednesday_training_sessions"]),
        "20260828_fri_overlap": list(oof["friday_training_sessions"]),
    }


def render_report(state: Mapping[str, Any]) -> str:
    lines = [
        "# Sequential All-68 Policy Challenger V1", "",
        "Status: frozen historical training comparison; research-only, nonexecuting, and proof-ineligible", "",
        f"- Cohort: `{state['cohort_id']}`",
        f"- Bound replay: `{state['source_binding']['cohort_id']}`",
        f"- Bound source pack: `{state['source_binding']['source_pack_id']}`",
        f"- Bound mistake curriculum: `{state['mistake_curriculum_binding']['cohort_id']}`",
        f"- Scheduled global clocks / market repetitions: **{state['global_clock_count']} / {state['market_repetition_count']}**",
        f"- Arm decisions: **{state['arm_decision_count']}** (zero extra repetitions)", "",
        "## Matched arm results", "",
        "| Arm | Actions | Legs | Result (pips) | Versus V1 | Terminal |", "|---|---:|---:|---:|---:|---|",
    ]
    baseline = float(state["arm_summaries"]["v1_baseline_reference"]["realized_pips"])
    for arm in state["arms"]:
        summary = state["arm_summaries"][arm]
        lines.append(
            f"| {arm} | {summary['non_wait_action_count']} | {summary['execution_leg_count']} | "
            f"{summary['realized_pips']:+.4f} | {summary['realized_pips'] - baseline:+.4f} | "
            f"{'flat' if summary['terminal_flat'] else 'censored'} |"
        )
    lines += [
        "", "The 2x cost hurdle is an abstention/turnover hypothesis, not evidence that high score predicts better returns.",
        "The explicit arm estimates incumbent continuation outside the new-entry set before hold/exit/switch decisions.",
        "The factor arm removes candidates whose signed currency legs contradict the same-clock aggregate factor vote.",
        "The OOF arm uses only completed earlier sessions and abstains unless its prior-session after-cost lower bound is positive.",
        "One scheduled global clock is one repetition. Arms, candidates, pairs, and reruns add zero repetitions.",
        "No arm can promote, authorize, publish a signal, access an account, or place an order. The supported decision remains no_trade.",
    ]
    return "\n".join(lines) + "\n"


def run(config_path: Path = CONFIG, output_dir: Path | None = None) -> tuple[dict[str, Any], Path, Path]:
    if is_link_or_reparse(config_path):
        raise ValueError("linked config rejected")
    reject_link_chain(config_path)
    config_path = config_path.resolve()
    config = read_json(config_path)
    validate_config(config)
    source_root, source_state, manifest, source_binding, source_data = source_contract(config)
    _, _, mistake_binding = mistake_contract(config)
    material = {
        "schema_version": 1,
        "config_sha256": file_sha(config_path),
        "runner_sha256": file_sha(Path(__file__).resolve()),
        "core_sha256": file_sha(CORE),
        "foundation_core_sha256": file_sha(FOUNDATION_CORE),
        "verifier_sha256": file_sha(VERIFIER),
        "source_binding": source_binding,
        "mistake_curriculum_binding": mistake_binding,
        "arms": list(ARM_NAMES),
        "costs": config["costs"],
        "policy": config["policy"],
        "oof_calibration": config["oof_calibration"],
        "dataset_limits": config["dataset_limits"],
        "independence": config["independence"],
        "artifact_time_contract": ARTIFACT_TIME_CONTRACT,
        "safety": SAFETY,
    }
    material_sha = stable_hash(material)
    cohort_id = config["experiment_key"] + "." + material_sha[:20]
    if output_dir is not None:
        reject_link_chain(output_dir)
    artifact_root = output_dir.resolve() if output_dir else safe_relative(ROOT, config["storage"]["artifact_relative_root"])
    if output_dir is None and ALLOWED_LEDGER_ROOT.resolve() not in artifact_root.parents:
        raise ValueError("artifact root outside research ledgers")
    cohort_root = safe_relative(artifact_root, str(Path("cohorts") / cohort_id))
    cohort_root.mkdir(parents=True, exist_ok=True)
    for name in ("state.json", "verifier_receipt.json", "source_pack_manifest.json", "source_pack_clean_verifier_receipt.json"):
        source_path = safe_relative(source_root, name)
        require_regular_file(source_path, maximum_bytes=16 * 1024 * 1024)
        output_path = safe_relative(cohort_root, f"bound_{name}")
        immutable_bytes(output_path, source_path.read_bytes())

    source_clocks = source_data["clocks"]
    source_decisions = {row["clock_id"]: row for row in source_data["decisions"]}
    source_feedback = {row["clock_id"]: row for row in source_data["feedback"]}
    if len(source_clocks) != 144 or set(source_decisions) != {row["clock_id"] for row in source_clocks}:
        raise ValueError("bound replay clock completeness failure")
    markets = load_markets(manifest, config)

    clock_rows: list[dict[str, Any]] = []
    for source_clock in source_clocks:
        clock_id = "policyclock_" + stable_hash(cohort_id, source_clock["clock_id"])[:28]
        clock_rows.append(seal_row({
            "clock_id": clock_id, "cohort_id": cohort_id,
            "source_clock_id": source_clock["clock_id"], "source_clock_row_sha256": source_clock["row_sha256"],
            "sequence_no": source_clock["sequence_no"], "session_key": source_clock["session_key"],
            "decision_epoch": source_clock["decision_epoch"], "execution_epoch": source_clock["execution_epoch"],
            "feedback_epoch": source_clock["feedback_epoch"], "session_end_epoch": source_clock["session_end_epoch"],
            "terminal_clock": source_clock["terminal_clock"], "counts_as_market_repetition": 1,
            "counts_as_regime_repetition": 0, "proof_eligible": False, "execution_eligible": False,
        }))

    # Candidate outcomes are calculated only to train strictly later sessions.
    training_rows: list[dict[str, Any]] = []
    raw_training: dict[str, list[dict[str, Any]]] = {}
    slippage = float(config["costs"]["slippage_per_execution_leg_pips"])
    for clock in source_clocks:
        decision = source_decisions[clock["clock_id"]]
        session = str(clock["session_key"])
        for candidate in decision["candidate_rank"]:
            observation = candidate_roundtrip_observation(
                candidate, markets[session], execution_epoch=int(clock["execution_epoch"]),
                feedback_epoch=int(clock["feedback_epoch"]), slippage=slippage,
            )
            raw_training.setdefault(session, []).append(observation)
            training_rows.append(seal_row({
                "training_observation_id": "policytrain_" + stable_hash(clock["clock_id"], candidate["snapshot_id"])[:28],
                "cohort_id": cohort_id, "source_clock_id": clock["clock_id"], "session_key": session,
                **observation, "counts_as_market_repetition": 0, "counts_as_regime_repetition": 0,
                "proof_eligible": False,
            }))
    session_training = training_session_map(config)
    calibrations: dict[str, dict[str, Any]] = {}
    calibration_rows: list[dict[str, Any]] = []
    for session in [row["session_key"] for row in manifest["sessions"]]:
        training_sessions = session_training[session]
        observations = [row for prior in training_sessions for row in raw_training.get(prior, [])]
        cells = build_calibration(observations, config["oof_calibration"])
        calibrations[session] = cells
        buckets = sorted(set(cells) | {"liquid", "normal", "elevated"})
        for bucket in buckets:
            cell = cells.get(bucket, {
                "observation_count": 0, "mean_after_cost_pips": None,
                "sample_standard_deviation_pips": None, "lower_bound_after_cost_pips": None,
                "sufficient_observations": False, "eligible": False,
            })
            calibration_rows.append(seal_row({
                "calibration_id": "policycal_" + stable_hash(cohort_id, session, bucket, training_sessions, cell)[:28],
                "cohort_id": cohort_id, "application_session_key": session,
                "training_session_keys": training_sessions, "liquidity_bucket": bucket, **cell,
                "same_session_training": session in training_sessions,
                "counts_as_market_repetition": 0, "counts_as_regime_repetition": 0,
                "proof_eligible": False,
            }))

    arm_rows: list[dict[str, Any]] = []
    terminal_rows: list[dict[str, Any]] = []
    arm_summaries: dict[str, dict[str, Any]] = {}
    for arm in ARM_NAMES:
        state = flat_state()
        actions: dict[str, int] = {}
        failures: dict[str, int] = {}
        legs = 0
        for clock, source_clock in zip(clock_rows, source_clocks):
            source_decision = source_decisions[source_clock["clock_id"]]
            source_fb = source_feedback[source_clock["clock_id"]]
            pre_state = json.loads(json.dumps(state))
            candidates = source_decision["candidate_rank"]
            policy_diagnostics: dict[str, Any]
            if arm == "v1_baseline_reference":
                decision_payload = source_decision["decision"]
                execution = source_decision["execution"]
                state = source_decision["state_after"]
                feedback = {"status": source_fb["status"], "liquidation_equity_pips": source_fb["liquidation_equity_pips"]}
                policy_diagnostics = {
                    "reference_only": True,
                    "source_decision_id": source_decision["decision_id"],
                    "source_decision_row_sha256": source_decision["row_sha256"],
                    "source_feedback_row_sha256": source_fb["row_sha256"],
                }
            else:
                if arm == "no_trade":
                    decision_payload, policy_diagnostics = choose_no_trade(state, terminal_clock=bool(clock["terminal_clock"]))
                elif arm == "cost_hurdle_2x":
                    decision_payload, policy_diagnostics = choose_cost_hurdle(
                        state, candidates, decision_epoch=int(clock["decision_epoch"]),
                        terminal_clock=bool(clock["terminal_clock"]), policy=config["policy"],
                    )
                elif arm == "explicit_hold_vs_switch_2x":
                    decision_payload, policy_diagnostics = choose_explicit_hold_vs_switch(
                        state, candidates, eligible_2x(candidates, config["policy"]["minimum_entry_score_cost_ratio"]),
                        decision_epoch=int(clock["decision_epoch"]), terminal_clock=bool(clock["terminal_clock"]),
                        policy=config["policy"], costs=config["costs"], rationale_prefix="explicit hold-vs-switch 2x",
                    )
                elif arm == "factor_conflict_suppressed_2x":
                    eligible = eligible_2x(candidates, config["policy"]["minimum_entry_score_cost_ratio"])
                    accepted, votes, rejected = factor_consistent_candidates(
                        eligible, tie_epsilon=float(config["policy"]["factor_vote_tie_epsilon"]),
                    )
                    decision_payload, policy_diagnostics = choose_explicit_hold_vs_switch(
                        state, candidates, accepted, decision_epoch=int(clock["decision_epoch"]),
                        terminal_clock=bool(clock["terminal_clock"]), policy=config["policy"],
                        costs=config["costs"], rationale_prefix="factor-conflict-suppressed 2x",
                    )
                    policy_diagnostics.update({"factor_vote_scores": votes, "factor_conflict_rejections": rejected})
                else:
                    accepted, rejected = calibrate_candidates(
                        candidates, calibrations[str(clock["session_key"])],
                        float(config["policy"]["minimum_entry_score_cost_ratio"]),
                    )
                    decision_payload, policy_diagnostics = choose_explicit_hold_vs_switch(
                        state, candidates, accepted, decision_epoch=int(clock["decision_epoch"]),
                        terminal_clock=bool(clock["terminal_clock"]), policy=config["policy"],
                        costs=config["costs"], rationale_prefix="prior-session OOF remaining-move calibration",
                    )
                    policy_diagnostics.update({
                        "training_session_keys": session_training[str(clock["session_key"])],
                        "calibration_cells": calibrations[str(clock["session_key"])],
                        "calibration_rejections": rejected,
                    })
                state, execution = apply_action(
                    state, decision_payload, markets[str(clock["session_key"])],
                    execution_epoch=int(clock["execution_epoch"]), costs=config["costs"],
                    clock_id=str(clock["clock_id"]), arm=arm,
                )
                status, equity = liquidation_equity(
                    state, markets[str(clock["session_key"])], int(clock["feedback_epoch"]), slippage,
                )
                feedback = {"status": status, "liquidation_equity_pips": equity}
            actions[decision_payload["action"]] = actions.get(decision_payload["action"], 0) + 1
            legs += len(execution["legs"])
            if execution["status"] != "applied":
                reason = execution["rejection_reason"] or execution["status"]
                failures[reason] = failures.get(reason, 0) + 1
            arm_rows.append(seal_row({
                "arm_decision_id": "policydecision_" + stable_hash(cohort_id, arm, clock["clock_id"], pre_state, decision_payload)[:28],
                "cohort_id": cohort_id, "arm": arm, "clock_id": clock["clock_id"],
                "source_clock_id": source_clock["clock_id"], "sequence_no": clock["sequence_no"],
                "session_key": clock["session_key"], "decision_epoch": clock["decision_epoch"],
                "state_before": pre_state, "candidate_count": len(candidates),
                "source_candidate_root_sha256": stable_hash(candidates),
                "decision": decision_payload, "policy_diagnostics": policy_diagnostics,
                "execution": execution, "state_after": state, "feedback": feedback,
                "counts_as_market_repetition": 0, "counts_as_regime_repetition": 0,
                "proof_eligible": False, "execution_eligible": False,
            }))
            if clock["terminal_clock"]:
                retry = {"status": "not_needed", "legs": [], "state_after": state}
                if state.get("position") is not None and arm != "v1_baseline_reference":
                    retry_decision = {
                        "action": "exit", "instrument": state["position"]["instrument"], "side": None,
                        "units": 0, "confidence": None, "expected_move_pips": None, "horizon_min": None,
                        "entry_condition": "", "invalidation": "", "rationale": "exact session-end terminal retry",
                        "candidate_snapshot_id": None, "branch_label": "terminal_retry",
                    }
                    state, retry = apply_action(
                        state, retry_decision, markets[str(clock["session_key"])],
                        execution_epoch=int(clock["session_end_epoch"]), costs=config["costs"],
                        clock_id=str(clock["clock_id"]), arm=arm,
                    )
                    legs += len(retry["legs"])
                elif arm == "v1_baseline_reference":
                    source_terminal = next(row for row in source_data["terminals"] if row["session_key"] == clock["session_key"])
                    retry = source_terminal["retry"]
                    state = source_terminal["state_after"]
                terminal_rows.append(seal_row({
                    "terminal_id": "policyterminal_" + stable_hash(cohort_id, arm, clock["session_key"])[:28],
                    "cohort_id": cohort_id, "arm": arm, "session_key": clock["session_key"],
                    "terminal_epoch": clock["session_end_epoch"], "retry": retry,
                    "terminal_flat": state.get("position") is None, "censored": state.get("position") is not None,
                    "state_after": state, "counts_as_market_repetition": 0,
                    "counts_as_regime_repetition": 0, "proof_eligible": False,
                }))
        arm_summaries[arm] = {
            "action_counts": actions, "non_wait_action_count": sum(value for key, value in actions.items() if key != "wait"),
            "execution_leg_count": legs, "failure_counts": failures,
            "realized_pips": float(state["realized_pips"]), "terminal_flat": state.get("position") is None,
        }

    datasets = {
        "clocks": write_dataset(safe_relative(cohort_root, "global_clocks.jsonl.gz"), clock_rows, config["dataset_limits"]),
        "arm_decisions": write_dataset(safe_relative(cohort_root, "arm_decisions.jsonl.gz"), arm_rows, config["dataset_limits"]),
        "terminals": write_dataset(safe_relative(cohort_root, "arm_session_terminals.jsonl.gz"), terminal_rows, config["dataset_limits"]),
        "oof_training": write_dataset(safe_relative(cohort_root, "oof_training_observations.jsonl.gz"), training_rows, config["dataset_limits"]),
        "oof_calibrations": write_dataset(safe_relative(cohort_root, "oof_calibrations.jsonl.gz"), calibration_rows, config["dataset_limits"]),
    }
    generated_utc = datetime.fromtimestamp(max(int(row["feedback_epoch"]) for row in clock_rows), tz=timezone.utc).isoformat()
    state_doc = {
        **SAFETY, "schema_version": 1, "generated_utc": generated_utc,
        "cohort_id": cohort_id, "material_sha256": material_sha, "material_contract": material,
        "artifact_root": str(cohort_root), "source_binding": source_binding,
        "mistake_curriculum_binding": mistake_binding, "arms": list(ARM_NAMES),
        "global_clock_count": len(clock_rows), "market_repetition_count": len(clock_rows),
        "source_pair_context_count": int(source_state["pair_context_count"]),
        "source_candidate_context_count": int(source_state["candidate_context_count"]),
        "arm_decision_count": len(arm_rows), "arm_decisions_count_as_rep": 0,
        "training_observation_count": len(training_rows), "training_observations_count_as_rep": 0,
        "calibration_cell_count": len(calibration_rows), "calibrations_count_as_rep": 0,
        "independent_regime_count": None, "arm_summaries": arm_summaries,
        "paired_pips_vs_no_trade": {
            arm: float(summary["realized_pips"]) - float(arm_summaries["no_trade"]["realized_pips"])
            for arm, summary in arm_summaries.items()
        },
        "paired_pips_vs_v1": {
            arm: float(summary["realized_pips"]) - float(arm_summaries["v1_baseline_reference"]["realized_pips"])
            for arm, summary in arm_summaries.items()
        },
        "terminal_flat_all_arms": all(summary["terminal_flat"] for summary in arm_summaries.values()),
        "datasets": datasets,
    }
    state_bytes = json_bytes(state_doc)
    report_bytes = render_report(state_doc).encode("utf-8")
    state_path = safe_relative(cohort_root, "state.json")
    report_path = safe_relative(cohort_root, "report.md")
    material_path = safe_relative(cohort_root, "material_contract.json")
    immutable_bytes(state_path, state_bytes)
    immutable_bytes(report_path, report_bytes)
    immutable_bytes(material_path, json_bytes(material))

    receipt_path = safe_relative(cohort_root, "verifier_receipt.json")
    completed = subprocess.run(
        [sys.executable, str(VERIFIER), "--config", str(config_path), "--state", str(state_path),
         "--material", str(material_path), "--output", str(receipt_path)],
        cwd=ROOT, text=True, capture_output=True,
    )
    if completed.returncode != 0:
        raise ValueError("independent policy-challenger verification failed: " + (completed.stdout + completed.stderr).strip())
    receipt = read_json(receipt_path)
    if receipt.get("verified") is not True or receipt.get("failures") != []:
        raise ValueError("independent policy-challenger verification failed closed")

    atomic_bytes(safe_relative(artifact_root, config["storage"]["state_name"]), state_path.read_bytes())
    atomic_bytes(safe_relative(artifact_root, config["storage"]["report_name"]), report_path.read_bytes())
    atomic_bytes(safe_relative(artifact_root, config["storage"]["verifier_name"]), receipt_path.read_bytes())
    return state_doc, state_path, receipt_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    args = parser.parse_args()
    state, state_path, receipt_path = run(args.config)
    print(json.dumps({
        "cohort_id": state["cohort_id"],
        "results": {arm: summary["realized_pips"] for arm, summary in state["arm_summaries"].items()},
        "verified": True,
    }, sort_keys=True))
    print(state_path)
    print(receipt_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
