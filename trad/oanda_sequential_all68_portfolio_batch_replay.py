#!/usr/bin/env python3
"""Build the historical, nonexecuting all-68 sequential portfolio replay."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.forex_system.research.sequential_deterministic_io_v1 import canonical_gzip

from src.forex_system.research.sequential_all68_portfolio_batch_replay_v1 import (
    SAFETY,
    PortfolioState,
    apply_fail_closed,
    choose_decision,
    context_status,
    decision_payload,
    exact_feedback_equity,
    legal_branches,
    ranked_candidates,
    scheduled_clocks,
    seal_row,
    stable_aliases,
    stable_hash,
    state_payload,
    terminal_retry,
)
from src.forex_system.research.sequential_portfolio_replay_v1 import load_archived_market


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "sequential_all68_portfolio_batch_replay_v1.json"
CORE = ROOT / "src" / "forex_system" / "research" / "sequential_all68_portfolio_batch_replay_v1.py"
FOUNDATION_CORE = ROOT / "src" / "forex_system" / "research" / "sequential_portfolio_replay_v1.py"
VERIFIER = ROOT / "oanda_sequential_all68_portfolio_batch_replay_verifier.py"


def parse_epoch(value: str) -> int:
    return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())


ARTIFACT_TIME_CONTRACT = {
    "kind": "maximum_scheduled_feedback_epoch",
    "timezone": "UTC",
    "wall_clock_independent": True,
}


def deterministic_generated_utc(clocks: Iterable[Mapping[str, Any]]) -> str:
    epochs = [int(row["feedback_epoch"]) for row in clocks]
    if not epochs:
        raise ValueError("cannot derive artifact timestamp without scheduled clocks")
    return datetime.fromtimestamp(max(epochs), tz=timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
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


def atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".tmp-{os.getpid()}-{stable_hash(str(path))[:12]}"
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def immutable_bytes(path: Path, payload: bytes) -> None:
    """Create a cohort artifact once, or prove an existing artifact is identical."""
    if path.exists():
        if path.read_bytes() != payload:
            raise ValueError(f"immutable artifact conflict: {path}")
        return
    atomic_bytes(path, payload)


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_bytes(path, (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8"))


def atomic_text(path: Path, value: str) -> None:
    atomic_bytes(path, value.encode("utf-8"))


def safe_path(root: Path, relative: str) -> Path:
    raw = Path(str(relative))
    if raw.is_absolute() or ".." in raw.parts:
        raise ValueError("unsafe relative path")
    path = (root / raw).resolve()
    if root.resolve() not in path.parents and path != root.resolve():
        raise ValueError("path escapes root")
    return path


def validate_config(config: Mapping[str, Any]) -> None:
    for key, expected in SAFETY.items():
        if config.get(key) != expected:
            raise ValueError(f"unsafe config: {key}")
    if not str(config["source"].get("required_pack_id", "")).startswith("sequential_replay_source_pack_v1."):
        raise ValueError("missing exact content-addressed source pack")
    if config["schedule"]["schedule_before_future_coverage"] is not True:
        raise ValueError("coverage-biased clock scheduling enabled")
    if config["schedule"]["one_global_portfolio_across_all_sessions"] is not True:
        raise ValueError("per-shard portfolios forbidden")
    independence = config["independence"]
    if independence["pair_contexts_count_as_market_repetitions"] is not False:
        raise ValueError("pair-context repetition inflation")
    if independence["counterfactuals_count_as_market_repetitions"] is not False:
        raise ValueError("counterfactual repetition inflation")
    if independence["replays_count_as_market_repetitions"] is not False:
        raise ValueError("replay repetition inflation")
    if independence["independent_regime_count"] is not None:
        raise ValueError("independent regimes must remain unknown")


def deterministic_jsonl(rows: Iterable[Mapping[str, Any]]) -> bytes:
    raw = b"".join((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8") for row in rows)
    return canonical_gzip(raw, compresslevel=9)


def write_dataset(path: Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload = deterministic_jsonl(rows)
    immutable_bytes(path, payload)
    return {
        "relative_path": path.name,
        "row_count": len(rows),
        "gzip_bytes": len(payload),
        "gzip_sha256": sha256(payload).hexdigest(),
        "row_set_sha256": stable_hash(sorted(row["row_sha256"] for row in rows)),
        "ordered_row_sha256": stable_hash([row["row_sha256"] for row in rows]),
    }


def source_contract(config: Mapping[str, Any]) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any], Path]:
    source_root = safe_path(ROOT, config["source"]["artifact_relative_root"])
    pack_id = str(config["source"]["required_pack_id"])
    manifest_path = source_root / "packs" / pack_id / "manifest.json"
    current_receipt_path = source_root / str(config["source"]["verifier_name"])
    binding_root = safe_path(ROOT, config["storage"]["artifact_relative_root"]) / "source_bindings" / pack_id
    receipt_path = binding_root / "clean_verifier_receipt.json"
    if not receipt_path.exists():
        current = read_json(current_receipt_path)
        if current.get("verified") is not True or current.get("failures") != [] or current.get("pack_id") != pack_id:
            raise ValueError("no clean exact-pack verifier receipt available to bind")
        atomic_bytes(receipt_path, current_receipt_path.read_bytes())
    manifest, receipt = read_json(manifest_path), read_json(receipt_path)
    if manifest.get("pack_id") != pack_id:
        raise ValueError("exact source manifest mismatch")
    if config["source"]["require_verified"] and receipt.get("verified") is not True:
        raise ValueError("source pack not independently verified")
    if config["source"]["require_zero_failures"] and receipt.get("failures") != []:
        raise ValueError("source verifier has failures")
    if receipt.get("pack_id") != pack_id:
        raise ValueError("source receipt pack mismatch")
    archive_hashes = [
        {
            "instrument": row["instrument"], "session_key": row["session_key"],
            "raw_sha256": row["raw_sha256"], "gzip_sha256": row["gzip_sha256"],
            "archive_relative_path": row["archive_relative_path"],
        }
        for row in manifest["source_manifest"]
    ]
    binding = {
        "pack_id": pack_id,
        "pack_manifest_sha256": file_sha(manifest_path),
        "clean_verifier_receipt_sha256": file_sha(receipt_path),
        "archive_binding_sha256": stable_hash(archive_hashes),
        "archive_count": len(archive_hashes),
    }
    return source_root, manifest, receipt, binding, receipt_path


def load_session_markets(source_root: Path, manifest: Mapping[str, Any], session_key: str, maximum_raw: int) -> dict[str, Any]:
    markets: dict[str, Any] = {}
    for row in manifest["source_manifest"]:
        if row["session_key"] != session_key:
            continue
        archive = safe_path(source_root, row["archive_relative_path"])
        if file_sha(archive) != row["gzip_sha256"]:
            raise ValueError(f"source archive gzip hash mismatch: {session_key}:{row['instrument']}")
        markets[row["instrument"]] = load_archived_market(
            archive,
            instrument=row["instrument"],
            expected_raw_sha256=row["raw_sha256"],
            maximum_raw_bytes=maximum_raw,
        )
    if len(markets) != 68:
        raise ValueError(f"session lacks all 68 source slices: {session_key}")
    return markets


def report(state: Mapping[str, Any]) -> str:
    actions = state["action_counts"]
    failures = state["failure_counts"]
    availability = state["context_availability_counts"]
    lines = [
        "# All-68 Sequential Portfolio Batch Replay V1", "",
        "Status: independently verifiable historical training/discovery; nonexecuting and proof-ineligible", "",
        f"- Cohort: `{state['cohort_id']}`",
        f"- Exact source pack: `{state['source_binding']['pack_id']}`",
        f"- Sessions/global clocks: **{state['session_count']} / {state['global_clock_count']}**",
        f"- Scheduled pair contexts: **{state['pair_context_count']:,}**",
        f"- Causal-ready contexts: **{state['causal_ready_context_count']:,}**",
        f"- Ranked candidate contexts: **{state['candidate_context_count']:,}**",
        f"- Missing causal/execution/feedback coverage: **{availability['missing_causal_context']:,} / {availability['missing_exact_execution_quote']:,} / {availability['missing_exact_feedback_quote']:,}**",
        f"- Execution legs: **{state['execution_leg_count']}**",
        f"- Terminal portfolio: **{'flat' if state['terminal_flat'] else 'unresolved/censored'}**",
        f"- Realized result: **{state['realized_pips']:+.4f} pips**", "",
        "## Primary actions", "",
    ]
    lines.extend(f"- {name}: {value}" for name, value in sorted(actions.items()))
    lines += ["", "## Selected-action failures", ""]
    if failures:
        lines.extend(f"- {name}: {value}" for name, value in sorted(failures.items()))
    else:
        lines.append("- none; unavailable unselected pair contexts remain explicitly recorded")
    lines += [
        "", "The 144 global clocks were scheduled before inspecting delayed-entry or feedback coverage.",
        "All 9,792 pair contexts remain in the ledger. Missing exact quotes were never nearest-filled.",
        "Pair contexts, counterfactuals, and reruns have count_as_rep=0. Independent regimes remain unknown.",
        "One portfolio state chain spans all three source sessions; it is never reset per pair or shard.",
        "This inspected historical result is curriculum evidence only and cannot promote, authorize, or execute.",
    ]
    return "\n".join(lines) + "\n"


def run(config_path: Path = CONFIG) -> dict[str, Any]:
    config = read_json(config_path.resolve())
    validate_config(config)
    source_root, manifest, receipt, binding, bound_receipt_path = source_contract(config)
    material_contract = {
        "schema_version": 1,
        "config_sha256": file_sha(config_path.resolve()),
        "runner_sha256": file_sha(Path(__file__).resolve()),
        "core_sha256": file_sha(CORE),
        "foundation_core_sha256": file_sha(FOUNDATION_CORE),
        "verifier_sha256": file_sha(VERIFIER),
        "source_binding": binding,
        "schedule": config["schedule"],
        "costs": config["costs"],
        "frozen_policy": config["frozen_policy"],
        "counterfactuals": config["counterfactuals"],
        "independence": config["independence"],
        "artifact_time_contract": ARTIFACT_TIME_CONTRACT,
        "safety": SAFETY,
    }
    material_sha = stable_hash(material_contract)
    cohort_id = config["experiment_key"] + "." + material_sha[:20]
    artifact_root = safe_path(ROOT, config["storage"]["artifact_relative_root"])
    cohort_root = artifact_root / "cohorts" / cohort_id
    cohort_root.mkdir(parents=True, exist_ok=True)
    # Bind immutable copies, rather than depending on a mutable current pointer.
    pack_id = str(config["source"]["required_pack_id"])
    immutable_bytes(
        cohort_root / "source_pack_manifest.json",
        (source_root / "packs" / pack_id / "manifest.json").read_bytes(),
    )
    immutable_bytes(
        cohort_root / "source_pack_clean_verifier_receipt.json",
        bound_receipt_path.read_bytes(),
    )

    sessions = [
        {
            "session_key": row["session_key"],
            "session_start_epoch": parse_epoch(row["start_utc"]),
            "session_end_epoch": parse_epoch(row["end_utc_exclusive"]),
        }
        for row in manifest["sessions"]
    ]
    clocks = scheduled_clocks(sessions, int(config["schedule"]["decision_cadence_min"]))
    if len(clocks) != int(config["schedule"]["expected_global_clock_count"]):
        raise ValueError("global clock count mismatch")
    instruments = sorted({row["instrument"] for row in manifest["source_manifest"]})
    aliases = stable_aliases(instruments)
    clock_rows: list[dict[str, Any]] = []
    for clock in clocks:
        clock_id = "a68clock_" + stable_hash(cohort_id, clock)[:28]
        clock_rows.append(seal_row({
            "clock_id": clock_id, "cohort_id": cohort_id, **clock,
            "proof_eligible": False, "execution_eligible": False,
        }))

    # Phase 1: emit every pair context after the schedule exists, independent of
    # delayed fill/feedback availability. No clock is filtered by future coverage.
    markets_by_session: dict[str, dict[str, Any]] = {}
    context_rows: list[dict[str, Any]] = []
    contexts_by_clock: dict[str, list[dict[str, Any]]] = {}
    candidate_by_clock: dict[str, list[Any]] = {}
    lookback = max(int(value) for value in config["frozen_policy"]["lookbacks_min"])
    for session in sessions:
        session_key = session["session_key"]
        markets = load_session_markets(source_root, manifest, session_key, int(config["source"]["maximum_archive_raw_bytes"]))
        markets_by_session[session_key] = markets
        for clock_row in (row for row in clock_rows if row["session_key"] == session_key):
            candidates = ranked_candidates(markets, int(clock_row["decision_epoch"]), config)
            candidate_map = {row.instrument: row for row in candidates}
            candidate_by_clock[clock_row["clock_id"]] = candidates
            rows: list[dict[str, Any]] = []
            for instrument in instruments:
                status = context_status(markets[instrument], int(clock_row["decision_epoch"]), lookback)
                candidate = candidate_map.get(instrument)
                row = seal_row({
                    "context_id": "a68context_" + stable_hash(clock_row["clock_id"], aliases[instrument])[:28],
                    "cohort_id": cohort_id,
                    "clock_id": clock_row["clock_id"],
                    "session_key": session_key,
                    "decision_epoch": int(clock_row["decision_epoch"]),
                    "pair_alias": aliases[instrument],
                    "instrument": instrument,
                    "source_sha256": markets[instrument].source_sha256,
                    **status,
                    "ranked_candidate": candidate is not None,
                    "candidate": asdict(candidate) if candidate is not None else None,
                    "counts_as_market_repetition": 0,
                    "counts_as_regime_repetition": 0,
                    "proof_eligible": False,
                })
                rows.append(row)
                context_rows.append(row)
            contexts_by_clock[clock_row["clock_id"]] = rows
    if len(context_rows) != int(config["schedule"]["expected_pair_context_count"]):
        raise ValueError("pair context count mismatch")

    # Phase 2: a single state chain across all sessions. No per-shard portfolio.
    state = PortfolioState()
    unresolved_terminal = False
    decision_rows: list[dict[str, Any]] = []
    feedback_rows: list[dict[str, Any]] = []
    counterfactual_rows: list[dict[str, Any]] = []
    terminal_rows: list[dict[str, Any]] = []
    for clock_row in clock_rows:
        clock_id = clock_row["clock_id"]
        markets = markets_by_session[clock_row["session_key"]]
        candidates = candidate_by_clock[clock_id]
        pre_state = state
        context_root = stable_hash([row["row_sha256"] for row in contexts_by_clock[clock_id]])
        decision = choose_decision(pre_state, candidates, clock_row, config, blocked=unresolved_terminal)
        decision_id = "a68decision_" + stable_hash(clock_id, state_payload(pre_state), decision_payload(decision), context_root)[:28]
        state, applied = apply_fail_closed(pre_state, decision, markets, clock_row, config, clock_id)
        decision_row = seal_row({
            "decision_id": decision_id, "cohort_id": cohort_id, "clock_id": clock_id,
            "session_key": clock_row["session_key"], "sequence_no": clock_row["sequence_no"],
            "decision_epoch": clock_row["decision_epoch"], "context_root_sha256": context_root,
            "causal_candidate_count": len(candidates),
            "candidate_rank": [asdict(row) for row in candidates],
            "state_before": state_payload(pre_state), "decision": decision_payload(decision),
            "execution": applied, "state_after": state_payload(state),
            "counts_as_market_repetition": 1, "counts_as_regime_repetition": 0,
            "proof_eligible": False, "execution_eligible": False,
        })
        decision_rows.append(decision_row)
        feedback_status, feedback_equity = exact_feedback_equity(
            state, markets, int(clock_row["feedback_epoch"]), float(config["costs"]["slippage_per_execution_leg_pips"])
        )
        feedback_rows.append(seal_row({
            "feedback_id": "a68feedback_" + stable_hash(decision_id, clock_row["feedback_epoch"])[:28],
            "cohort_id": cohort_id, "decision_id": decision_id, "clock_id": clock_id,
            "feedback_epoch": clock_row["feedback_epoch"], "status": feedback_status,
            "liquidation_equity_pips": feedback_equity,
            "counts_as_market_repetition": 0, "counts_as_regime_repetition": 0,
            "proof_eligible": False,
        }))
        adapter = {
            "session": {"normalized_units": config["frozen_policy"]["normalized_units"], "feedback_horizon_min": config["schedule"]["feedback_horizon_min"]},
            "frozen_policy": config["frozen_policy"],
        }
        for branch in legal_branches(pre_state, decision, candidates, adapter):
            _, branch_applied = apply_fail_closed(pre_state, branch, markets, clock_row, config, clock_id)
            # Recreate only enough state to price feedback; apply_fail_closed already
            # returns the canonical serializable state.
            if branch_applied["state_after"]["position"] is None:
                branch_feedback_status, branch_equity = "available_flat", float(branch_applied["state_after"]["realized_pips"])
            else:
                # Apply again to obtain the typed state without trusting serialization.
                typed_state, _ = apply_fail_closed(pre_state, branch, markets, clock_row, config, clock_id)
                branch_feedback_status, branch_equity = exact_feedback_equity(
                    typed_state, markets, int(clock_row["feedback_epoch"]), float(config["costs"]["slippage_per_execution_leg_pips"])
                )
            counterfactual_rows.append(seal_row({
                "counterfactual_id": "a68cf_" + stable_hash(decision_id, decision_payload(branch))[:28],
                "cohort_id": cohort_id, "decision_id": decision_id, "clock_id": clock_id,
                "branch": decision_payload(branch), "execution": branch_applied,
                "feedback_status": branch_feedback_status, "liquidation_equity_pips": branch_equity,
                "depth": 1, "counts_as_market_repetition": 0,
                "counts_as_regime_repetition": 0, "proof_eligible": False,
            }))
        if clock_row["terminal_clock"]:
            retry = {"status": "not_needed", "legs": [], "state_after": state_payload(state)}
            if not state.flat:
                state, retry = terminal_retry(
                    state, markets, int(clock_row["session_end_epoch"]),
                    float(config["costs"]["slippage_per_execution_leg_pips"]), clock_id,
                )
            terminal_flat = state.flat
            if not terminal_flat:
                unresolved_terminal = True
            terminal_rows.append(seal_row({
                "terminal_id": "a68terminal_" + stable_hash(cohort_id, clock_row["session_key"])[:28],
                "cohort_id": cohort_id, "session_key": clock_row["session_key"],
                "terminal_epoch": clock_row["session_end_epoch"], "retry": retry,
                "terminal_flat": terminal_flat, "censored": not terminal_flat,
                "state_after": state_payload(state), "counts_as_market_repetition": 0,
                "counts_as_regime_repetition": 0, "proof_eligible": False,
            }))

    datasets = {
        "clocks": write_dataset(cohort_root / "global_clocks.jsonl.gz", clock_rows),
        "pair_contexts": write_dataset(cohort_root / "pair_contexts.jsonl.gz", context_rows),
        "decisions": write_dataset(cohort_root / "portfolio_decisions.jsonl.gz", decision_rows),
        "feedback": write_dataset(cohort_root / "feedback.jsonl.gz", feedback_rows),
        "counterfactuals": write_dataset(cohort_root / "counterfactuals.jsonl.gz", counterfactual_rows),
        "terminals": write_dataset(cohort_root / "session_terminals.jsonl.gz", terminal_rows),
    }
    actions: dict[str, int] = {}
    failures: dict[str, int] = {}
    legs = 0
    for row in decision_rows:
        action = row["decision"]["action"]
        actions[action] = actions.get(action, 0) + 1
        legs += len(row["execution"]["legs"])
        if row["execution"]["status"] != "applied":
            reason = row["execution"]["rejection_reason"] or row["execution"]["status"]
            failures[reason] = failures.get(reason, 0) + 1
    for row in feedback_rows:
        if row["status"].startswith("missing"):
            failures[row["status"]] = failures.get(row["status"], 0) + 1
    context_availability = {
        "missing_causal_context": sum(not row["causal_ready"] for row in context_rows),
        "missing_exact_execution_quote": sum(not row["execution_ready"] for row in context_rows),
        "missing_exact_feedback_quote": sum(not row["feedback_ready"] for row in context_rows),
        "fully_ready": sum(row["fully_ready"] for row in context_rows),
    }
    generated_utc = deterministic_generated_utc(clock_rows)
    state_doc = {
        **SAFETY, "schema_version": 1, "generated_utc": generated_utc,
        "cohort_id": cohort_id, "material_sha256": material_sha,
        "material_contract": material_contract, "source_binding": binding,
        "artifact_root": str(cohort_root), "session_count": len(sessions),
        "instrument_count": len(instruments), "pair_aliases": aliases,
        "global_clock_count": len(clock_rows), "market_repetition_count": len(clock_rows),
        "pair_context_count": len(context_rows),
        "causal_ready_context_count": sum(row["causal_ready"] for row in context_rows),
        "candidate_context_count": sum(row["ranked_candidate"] for row in context_rows),
        "context_availability_counts": context_availability,
        "counterfactual_count": len(counterfactual_rows),
        "execution_leg_count": legs + sum(len(row["retry"].get("legs", [])) for row in terminal_rows),
        "action_counts": actions, "failure_counts": failures,
        "realized_pips": float(state.realized_pips), "terminal_flat": state.flat,
        "independent_regime_count": None,
        "independent_regime_state": "unknown_structural_sessions_only",
        "pair_contexts_count_as_rep": 0, "counterfactuals_count_as_rep": 0,
        "replays_count_as_rep": 0, "datasets": datasets,
    }
    state_payload_bytes = (
        json.dumps(state_doc, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    report_payload_bytes = report(state_doc).encode("utf-8")
    immutable_bytes(cohort_root / "state.json", state_payload_bytes)
    immutable_bytes(cohort_root / "report.md", report_payload_bytes)
    artifact_root.mkdir(parents=True, exist_ok=True)
    # Mutable current pointers are exact byte copies of their immutable cohort
    # artifacts; they are never independently regenerated.
    atomic_bytes(
        artifact_root / config["storage"]["state_name"],
        (cohort_root / "state.json").read_bytes(),
    )
    atomic_bytes(
        artifact_root / config["storage"]["report_name"],
        (cohort_root / "report.md").read_bytes(),
    )
    return state_doc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=CONFIG)
    args = parser.parse_args()
    state = run(args.config)
    print(json.dumps({
        "cohort_id": state["cohort_id"], "global_clocks": state["global_clock_count"],
        "pair_contexts": state["pair_context_count"], "realized_pips": state["realized_pips"],
        "terminal_flat": state["terminal_flat"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
