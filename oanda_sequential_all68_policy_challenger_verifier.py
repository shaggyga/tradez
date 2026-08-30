#!/usr/bin/env python3
"""Independent verifier for the all-68 policy challenger.

The verifier intentionally does not import the challenger producer or core.
It reconstructs source bindings, policy decisions, executable quote accounting,
OOF training, terminal state, and summary statistics from immutable inputs.
"""

from __future__ import annotations

import argparse
import copy
import csv
from datetime import datetime, timezone
import gzip
from hashlib import sha256
import io
import json
import math
import os
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "sequential_all68_policy_challenger_v1.json"
PRODUCER = ROOT / "oanda_sequential_all68_policy_challenger.py"
CORE = ROOT / "src" / "forex_system" / "research" / "sequential_all68_policy_challenger_v1.py"
FOUNDATION_CORE = ROOT / "src" / "forex_system" / "research" / "sequential_portfolio_replay_v1.py"
SAFETY = {
    "research_only": True, "execution_eligible": False, "proof_eligible": False,
    "can_promote": False, "can_place_orders": False, "can_authorize": False,
    "broker_access": False, "account_access": False, "signal_feed_write": False,
    "lifecycle_write": False, "supported_decision": "no_trade",
}
ARMS = (
    "no_trade", "v1_baseline_reference", "cost_hurdle_2x",
    "explicit_hold_vs_switch_2x", "factor_conflict_suppressed_2x",
    "oof_remaining_move_calibrated_2x",
)
ARTIFACT_TIME_CONTRACT = {
    "kind": "maximum_bound_source_feedback_epoch", "timezone": "UTC", "wall_clock_independent": True,
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def stable_hash(*parts: Any) -> str:
    return sha256(canonical_json(parts).encode("utf-8")).hexdigest()


def semantic_sha256(value: Mapping[str, Any], *volatile_fields: str) -> str:
    payload = copy.deepcopy(dict(value))
    for field in volatile_fields:
        payload.pop(field, None)
    return stable_hash(payload)


def row_hash(row: Mapping[str, Any]) -> str:
    return stable_hash({key: value for key, value in row.items() if key != "row_sha256"})


def seal(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["row_sha256"] = row_hash(payload)
    return payload


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


def read_gzip_bounded(path: Path, *, maximum_gzip_bytes: int, maximum_raw_bytes: int) -> bytes:
    require_regular_file(path, maximum_bytes=maximum_gzip_bytes)
    with path.open("rb") as raw, gzip.GzipFile(fileobj=raw, mode="rb") as stream:
        value = stream.read(int(maximum_raw_bytes) + 1)
        if len(value) > int(maximum_raw_bytes):
            raise ValueError("oversized_dataset_raw")
        if stream.read(1):
            raise ValueError("oversized_dataset_tail")
    return value


def load_dataset(root: Path, spec: Mapping[str, Any], limits: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    limits = limits or {"maximum_gzip_bytes": 4194304, "maximum_raw_bytes": 67108864, "maximum_row_count": 20000}
    path = safe_relative(root, str(spec["relative_path"]))
    if int(spec["gzip_bytes"]) > int(limits["maximum_gzip_bytes"]) or int(spec["row_count"]) > int(limits["maximum_row_count"]):
        raise ValueError("dataset declared size exceeds frozen limit")
    require_regular_file(path, maximum_bytes=int(limits["maximum_gzip_bytes"]))
    if path.stat().st_size != int(spec["gzip_bytes"]) or file_sha(path) != spec["gzip_sha256"]:
        raise ValueError("dataset gzip identity mismatch")
    raw = read_gzip_bounded(
        path,
        maximum_gzip_bytes=int(limits["maximum_gzip_bytes"]),
        maximum_raw_bytes=int(limits["maximum_raw_bytes"]),
    )
    rows = [json.loads(line) for line in raw.decode("utf-8").splitlines() if line]
    if len(rows) != int(spec["row_count"]):
        raise ValueError("dataset row count mismatch")
    if any(row_hash(row) != row.get("row_sha256") for row in rows):
        raise ValueError("dataset row hash mismatch")
    if stable_hash(sorted(row["row_sha256"] for row in rows)) != spec["row_set_sha256"]:
        raise ValueError("dataset row-set mismatch")
    if stable_hash([row["row_sha256"] for row in rows]) != spec["ordered_row_sha256"]:
        raise ValueError("dataset order mismatch")
    return rows


def source_contract(config: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, list[dict[str, Any]]], dict[str, Any]]:
    source = config["source"]
    root = safe_relative(ROOT, source["cohort_relative_root"])
    paths = {
        "state": root / "state.json", "receipt": root / "verifier_receipt.json",
        "manifest": root / "source_pack_manifest.json", "pack_receipt": root / "source_pack_clean_verifier_receipt.json",
    }
    for name, required in (
        ("state", source["required_state_sha256"]), ("receipt", source["required_verifier_sha256"]),
        ("manifest", source["required_source_pack_manifest_sha256"]),
        ("pack_receipt", source["required_source_pack_verifier_sha256"]),
    ):
        require_regular_file(paths[name], maximum_bytes=16 * 1024 * 1024)
        if file_sha(paths[name]) != required:
            raise ValueError(f"exact source hash changed: {name}")
    state, receipt = read_json(paths["state"]), read_json(paths["receipt"])
    manifest, pack_receipt = read_json(paths["manifest"]), read_json(paths["pack_receipt"])
    if state.get("cohort_id") != source["required_cohort_id"]:
        raise ValueError("source cohort mismatch")
    if manifest.get("pack_id") != source["required_source_pack_id"]:
        raise ValueError("source pack mismatch")
    if receipt.get("verified") is not True or receipt.get("failures") != []:
        raise ValueError("source replay verification not clean")
    if pack_receipt.get("verified") is not True or pack_receipt.get("failures") != []:
        raise ValueError("source pack verification not clean")
    if semantic_sha256(state, "generated_utc") != source["required_state_semantic_sha256"]:
        raise ValueError("source state semantic mismatch")
    if semantic_sha256(receipt, "generated_utc") != source["required_verifier_semantic_sha256"]:
        raise ValueError("source verifier semantic mismatch")
    if stable_hash(state["datasets"]) != source["required_dataset_roots_sha256"]:
        raise ValueError("source dataset root mismatch")
    datasets = {key: load_dataset(root, spec, config["dataset_limits"]) for key, spec in state["datasets"].items()}
    binding = {
        "cohort_id": state["cohort_id"], "source_pack_id": manifest["pack_id"],
        "state_sha256": file_sha(paths["state"]), "verifier_sha256": file_sha(paths["receipt"]),
        "state_semantic_sha256": semantic_sha256(state, "generated_utc"),
        "verifier_semantic_sha256": semantic_sha256(receipt, "generated_utc"),
        "source_pack_manifest_sha256": file_sha(paths["manifest"]),
        "source_pack_verifier_sha256": file_sha(paths["pack_receipt"]),
        "dataset_roots_sha256": stable_hash(state["datasets"]), "dataset_specs": state["datasets"],
    }
    return state, manifest, datasets, binding


def mistake_contract(config: Mapping[str, Any]) -> dict[str, Any]:
    contract = config["mistake_curriculum"]
    root = safe_relative(ROOT, contract["cohort_relative_root"])
    paths = {
        "report": root / "SEQUENTIAL_ALL68_MISTAKE_CURRICULUM_V1.json",
        "material": root / "material_contract_v1.json", "verifier": root / "verifier_receipt.json",
    }
    for name, required in (
        ("report", contract["required_report_sha256"]), ("material", contract["required_material_sha256"]),
        ("verifier", contract["required_verifier_sha256"]),
    ):
        require_regular_file(paths[name], maximum_bytes=16 * 1024 * 1024)
        if file_sha(paths[name]) != required:
            raise ValueError(f"mistake curriculum hash changed: {name}")
    report, receipt = read_json(paths["report"]), read_json(paths["verifier"])
    if report.get("cohort_id") != contract["required_cohort_id"] or report.get("report_id") != contract["required_report_id"]:
        raise ValueError("mistake curriculum identity mismatch")
    if receipt.get("verified") is not True or receipt.get("failures") != []:
        raise ValueError("mistake curriculum verification not clean")
    return {
        "cohort_id": report["cohort_id"], "report_id": report["report_id"],
        "report_sha256": file_sha(paths["report"]), "material_sha256": file_sha(paths["material"]),
        "verifier_sha256": file_sha(paths["verifier"]), "summary": report["summary"],
        "category_materiality_pips": {key: value["materiality_pips"] for key, value in sorted(report["categories"].items())},
    }


def load_quotes(manifest: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, dict[str, dict[int, tuple[float, float, float]]]]:
    root = safe_relative(ROOT, config["source"]["source_pack_relative_root"])
    answer: dict[str, dict[str, dict[int, tuple[float, float, float]]]] = {}
    for spec in manifest["source_manifest"]:
        path = safe_relative(root, spec["archive_relative_path"])
        require_regular_file(path, maximum_bytes=int(config["source"]["maximum_archive_gzip_bytes"]))
        if file_sha(path) != spec["gzip_sha256"]:
            raise ValueError("archive gzip hash mismatch")
        raw = read_gzip_bounded(
            path,
            maximum_gzip_bytes=int(config["source"]["maximum_archive_gzip_bytes"]),
            maximum_raw_bytes=int(config["source"]["maximum_archive_raw_bytes"]),
        )
        if len(raw) > int(config["source"]["maximum_archive_raw_bytes"]) or sha256(raw).hexdigest() != spec["raw_sha256"]:
            raise ValueError("archive raw identity mismatch")
        instrument = str(spec["instrument"])
        pip = 0.01 if instrument.endswith("_JPY") else 0.0001
        rows: dict[int, tuple[float, float, float]] = {}
        for row in csv.DictReader(io.StringIO(raw.decode("utf-8"))):
            epoch = int(datetime.fromisoformat(str(row["time"]).replace("Z", "+00:00")).timestamp())
            bid, ask = float(row["bid_open"]), float(row["ask_open"])
            if ask <= bid or epoch in rows:
                raise ValueError("invalid quote archive")
            rows[epoch] = (bid, ask, pip)
        answer.setdefault(str(spec["session_key"]), {})[instrument] = rows
    if any(len(markets) != 68 for markets in answer.values()):
        raise ValueError("not all 68 quote archives available")
    return answer


def quote(quotes: Mapping[str, Mapping[int, tuple[float, float, float]]], instrument: str, epoch: int) -> tuple[float, float, float, float]:
    if int(epoch) not in quotes[instrument]:
        raise ValueError("missing exact executable quote")
    bid, ask, pip = quotes[instrument][int(epoch)]
    return bid, ask, (ask - bid) / pip, pip


def flat_state() -> dict[str, Any]:
    return {"realized_pips": 0.0, "position": None}


def empty_decision(action: str, rationale: str, instrument: str | None = None) -> dict[str, Any]:
    return {"action": action, "instrument": instrument, "side": None, "units": 0, "confidence": None,
            "expected_move_pips": None, "horizon_min": None, "entry_condition": "", "invalidation": "",
            "rationale": rationale, "candidate_snapshot_id": None, "branch_label": "primary"}


def candidate_decision(candidate: Mapping[str, Any], action: str, policy: Mapping[str, Any], rationale: str) -> dict[str, Any]:
    return {"action": action, "instrument": candidate["instrument"], "side": int(candidate["side"]),
            "units": int(policy["normalized_units"]), "confidence": float(candidate["confidence"]),
            "expected_move_pips": float(candidate["expected_move_pips"]),
            "horizon_min": int(policy["feedback_horizon_min"]),
            "entry_condition": f"frozen_score_cost_ratio>={policy['minimum_entry_score_cost_ratio']}",
            "invalidation": "frozen_rank_reversal_or_maximum_holding", "rationale": rationale,
            "candidate_snapshot_id": candidate["snapshot_id"], "branch_label": "primary"}


def ranked(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted((dict(row) for row in rows), key=lambda row: (-float(row["score"]), row["instrument"], -int(row["side"])))


def eligible(rows: Sequence[Mapping[str, Any]], threshold: float) -> list[dict[str, Any]]:
    return ranked(row for row in rows if float(row["score"]) >= threshold)


def choose_cost(state: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]], clock: Mapping[str, Any], config: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    policy = config["policy"]
    choices = eligible(candidates, float(policy["minimum_entry_score_cost_ratio"]))
    pos = state.get("position")
    diag: dict[str, Any] = {"eligible_candidate_count": len(choices), "incumbent_estimate_status": "not_applicable"}
    if clock["terminal_clock"]:
        return (empty_decision("wait", "predeclared terminal boundary while flat") if pos is None else empty_decision("exit", "predeclared terminal flattening", pos["instrument"])), diag
    best = choices[0] if choices else None
    if pos is None:
        return (empty_decision("wait", "no candidate clears frozen 2x cost hurdle") if best is None else candidate_decision(best, "enter", policy, "highest frozen candidate clearing 2x cost hurdle")), diag
    held = max(0, int(clock["decision_epoch"]) - int(pos["entry_epoch"])) // 60
    diag["held_min"] = held
    if held >= int(policy["maximum_holding_min"]):
        return empty_decision("exit", "predeclared maximum holding time", pos["instrument"]), diag
    current = next((row for row in choices if row["instrument"] == pos["instrument"] and int(row["side"]) == int(pos["side"])), None)
    if best is None:
        diag["incumbent_estimate_status"] = "absent_below_new_entry_hurdle"
        return empty_decision("exit", "no continuing candidate clears frozen 2x hurdle", pos["instrument"]), diag
    if current is None:
        diag["incumbent_estimate_status"] = "absent_below_new_entry_hurdle"
        return candidate_decision(best, "rotate", policy, "incumbent absent from 2x entry set; rotate to best"), diag
    diag["incumbent_estimate_status"] = "available_above_entry_hurdle"
    if best["instrument"] == current["instrument"] and int(best["side"]) == int(current["side"]):
        return empty_decision("hold", "incumbent remains highest ranked"), diag
    multiple = float(best["score"]) / max(1e-12, float(current["score"]))
    diag["rotation_score_multiple"] = multiple
    if multiple >= float(policy["rotation_improvement_multiple"]):
        return candidate_decision(best, "rotate", policy, "alternative clears frozen rotation multiple"), diag
    return empty_decision("hold", "alternative does not clear frozen rotation multiple"), diag


def factor_filter(rows: Sequence[Mapping[str, Any]], epsilon: float) -> tuple[list[dict[str, Any]], dict[str, float], list[dict[str, Any]]]:
    votes: dict[str, float] = {}
    for row in rows:
        base, quote_currency = row["instrument"].split("_", 1)
        side, weight = int(row["side"]), float(row["expected_move_pips"])
        votes[base] = votes.get(base, 0.0) + side * weight
        votes[quote_currency] = votes.get(quote_currency, 0.0) - side * weight
    votes = {key: votes[key] for key in sorted(votes)}
    accepted, rejected = [], []
    for original in rows:
        row = dict(original); base, quote_currency = row["instrument"].split("_", 1); side = int(row["side"])
        conflicts = []
        for currency, exposure in ((base, side), (quote_currency, -side)):
            vote = votes.get(currency, 0.0)
            if abs(vote) > epsilon and (vote > 0) != (exposure > 0): conflicts.append(currency)
        if conflicts:
            rejected.append({"snapshot_id": row["snapshot_id"], "instrument": row["instrument"], "side": row["side"], "conflicting_currencies": sorted(conflicts)})
        else: accepted.append(row)
    return accepted, votes, rejected


def choose_explicit(state: Mapping[str, Any], all_rows: Sequence[Mapping[str, Any]], entries: Sequence[Mapping[str, Any]], clock: Mapping[str, Any], config: Mapping[str, Any], prefix: str) -> tuple[dict[str, Any], dict[str, Any]]:
    policy, costs = config["policy"], config["costs"]
    all_rank, entry_rank = ranked(all_rows), ranked(entries)
    pos = state.get("position")
    diag: dict[str, Any] = {"eligible_candidate_count": len(entry_rank), "incumbent_estimate_status": "not_applicable"}
    if clock["terminal_clock"]:
        return (empty_decision("wait", "predeclared terminal boundary while flat") if pos is None else empty_decision("exit", "predeclared terminal flattening", pos["instrument"])), diag
    best = entry_rank[0] if entry_rank else None
    if pos is None:
        return (empty_decision("wait", f"{prefix}: no candidate clears entry contract") if best is None else candidate_decision(best, "enter", policy, f"{prefix}: highest eligible new entry")), diag
    held = max(0, int(clock["decision_epoch"]) - int(pos["entry_epoch"])) // 60
    diag["held_min"] = held
    if held >= int(policy["maximum_holding_min"]):
        return empty_decision("exit", "predeclared maximum holding time", pos["instrument"]), diag
    incumbent = next((row for row in all_rank if row["instrument"] == pos["instrument"] and int(row["side"]) == int(pos["side"])), None)
    if incumbent is None:
        diag["incumbent_estimate_status"] = "unavailable_fail_closed"
        return empty_decision("exit", f"{prefix}: incumbent continuation estimate unavailable", pos["instrument"]), diag
    diag["incumbent_estimate_status"] = "available_independent_of_new_entry_hurdle"
    if best and best["instrument"] == incumbent["instrument"] and int(best["side"]) == int(incumbent["side"]): best = None
    slip = float(costs["slippage_per_execution_leg_pips"])
    liquidation_cost = float(incumbent["spread_pips"]) / 2.0 + slip
    hold_value = float(incumbent["expected_move_pips"]) - liquidation_cost
    exit_value = -liquidation_cost
    switch_value = None if best is None else float(best["expected_move_pips"]) - float(best["spread_pips"]) - float(costs["round_trip_slippage_pips"]) - liquidation_cost
    diag.update({"estimated_liquidation_cost_pips": liquidation_cost, "hold_value_pips": hold_value, "exit_value_pips": exit_value, "switch_value_pips": switch_value})
    if best is not None and float(switch_value) > max(hold_value + float(policy.get("explicit_switch_incremental_hurdle_pips", 0.0)), exit_value):
        return candidate_decision(best, "rotate", policy, f"{prefix}: switch value exceeds explicit hold and exit"), diag
    if hold_value >= exit_value: return empty_decision("hold", f"{prefix}: explicit hold value dominates"), diag
    return empty_decision("exit", f"{prefix}: explicit exit value dominates", pos["instrument"]), diag


def close_state(state: Mapping[str, Any], quotes: Mapping[str, Mapping[int, tuple[float, float, float]]], epoch: int, slippage: float) -> tuple[dict[str, Any], dict[str, Any]]:
    pos = state["position"]
    bid, ask, spread, pip = quote(quotes, pos["instrument"], epoch); side = int(pos["side"])
    raw = bid if side > 0 else ask; executed = raw - slippage * pip if side > 0 else raw + slippage * pip
    realized = side * (executed - float(pos["entry_price"])) / pip * int(pos["units"])
    after = {"realized_pips": float(state["realized_pips"]) + realized, "position": None}
    leg = {"leg_kind": "close", "instrument": pos["instrument"], "side": side, "units": int(pos["units"]),
           "execution_epoch": int(epoch), "raw_price": raw, "executed_price": executed, "spread_pips": spread,
           "slippage_pips": slippage, "realized_pips": realized, "thesis_id": pos["thesis_id"]}
    return after, leg


def open_state(state: Mapping[str, Any], decision: Mapping[str, Any], quotes: Mapping[str, Mapping[int, tuple[float, float, float]]], epoch: int, slippage: float, clock_id: str, arm: str) -> tuple[dict[str, Any], dict[str, Any]]:
    instrument, side = decision["instrument"], int(decision["side"])
    bid, ask, spread, pip = quote(quotes, instrument, epoch)
    raw = ask if side > 0 else bid; executed = raw + slippage * pip if side > 0 else raw - slippage * pip
    thesis = "challengerthesis_" + stable_hash(arm, instrument, side, clock_id)[:28]
    pos = {"instrument": instrument, "side": side, "units": int(decision["units"]), "entry_epoch": int(epoch),
           "entry_price": executed, "entry_raw_price": raw, "entry_spread_pips": spread,
           "entry_slippage_pips": slippage, "entry_clock_id": clock_id, "thesis_id": thesis}
    after = {"realized_pips": float(state["realized_pips"]), "position": pos}
    leg = {"leg_kind": "open", "instrument": instrument, "side": side, "units": int(decision["units"]),
           "execution_epoch": int(epoch), "raw_price": raw, "executed_price": executed, "spread_pips": spread,
           "slippage_pips": slippage, "realized_pips": 0.0, "thesis_id": thesis}
    return after, leg


def apply(state: Mapping[str, Any], decision: Mapping[str, Any], quotes: Mapping[str, Mapping[int, tuple[float, float, float]]], clock: Mapping[str, Any], config: Mapping[str, Any], arm: str) -> tuple[dict[str, Any], dict[str, Any]]:
    before = copy.deepcopy(dict(state)); action = decision["action"]; slip = float(config["costs"]["slippage_per_execution_leg_pips"])
    try:
        if action in {"wait", "hold"}: return before, {"status": "applied", "rejection_reason": "", "realized_delta_pips": 0.0, "legs": [], "state_after": before}
        if action in {"enter", "rotate"}:
            _, _, spread, _ = quote(quotes, decision["instrument"], int(clock["execution_epoch"]))
            if spread > float(config["costs"]["maximum_entry_spread_pips"]):
                return before, {"status": "rejected", "rejection_reason": "execution_spread_above_limit", "realized_delta_pips": 0.0, "legs": [], "state_after": before}
        if action == "exit":
            after, leg = close_state(before, quotes, int(clock["execution_epoch"]), slip)
            return after, {"status": "applied", "rejection_reason": "", "realized_delta_pips": leg["realized_pips"], "legs": [leg], "state_after": after}
        if action == "enter":
            after, leg = open_state(before, decision, quotes, int(clock["execution_epoch"]), slip, clock["clock_id"], arm)
            return after, {"status": "applied", "rejection_reason": "", "realized_delta_pips": 0.0, "legs": [leg], "state_after": after}
        if action != "rotate": raise ValueError("invalid action")
        closed, leg1 = close_state(before, quotes, int(clock["execution_epoch"]), slip)
        after, leg2 = open_state(closed, decision, quotes, int(clock["execution_epoch"]), slip, clock["clock_id"], arm)
        return after, {"status": "applied", "rejection_reason": "", "realized_delta_pips": leg1["realized_pips"], "legs": [leg1, leg2], "state_after": after}
    except (KeyError, TypeError, ValueError) as exc:
        reason = "missing_exact_execution_quote" if "missing exact" in str(exc).lower() else "invalid_exact_execution"
        return before, {"status": "rejected", "rejection_reason": reason, "realized_delta_pips": 0.0, "legs": [], "state_after": before}


def equity(state: Mapping[str, Any], quotes: Mapping[str, Mapping[int, tuple[float, float, float]]], epoch: int, slippage: float) -> tuple[str, float | None]:
    if state.get("position") is None: return "available_flat", float(state["realized_pips"])
    try: return "available", float(close_state(state, quotes, epoch, slippage)[0]["realized_pips"])
    except (KeyError, TypeError, ValueError): return "missing_exact_feedback_quote", None


def observation(candidate: Mapping[str, Any], quotes: Mapping[str, Mapping[int, tuple[float, float, float]]], clock: Mapping[str, Any], slip: float) -> dict[str, Any]:
    try:
        instrument, side = candidate["instrument"], int(candidate["side"])
        b0, a0, _, pip = quote(quotes, instrument, int(clock["execution_epoch"])); b1, a1, _, _ = quote(quotes, instrument, int(clock["feedback_epoch"]))
        entry = a0 + slip * pip if side > 0 else b0 - slip * pip; exit_price = b1 - slip * pip if side > 0 else a1 + slip * pip
        return {"status": "available", "instrument": instrument, "side": side, "snapshot_id": candidate["snapshot_id"], "liquidity_bucket": candidate["liquidity_bucket"], "after_cost_pips": side * (exit_price - entry) / pip}
    except (KeyError, TypeError, ValueError):
        return {"status": "missing_exact_quote", "instrument": candidate.get("instrument"), "side": candidate.get("side"), "snapshot_id": candidate.get("snapshot_id"), "liquidity_bucket": candidate.get("liquidity_bucket"), "after_cost_pips": None}


def calibrations(observations: Sequence[Mapping[str, Any]], config: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[float]] = {}
    for row in observations:
        if row["status"] == "available": groups.setdefault(row["liquidity_bucket"], []).append(float(row["after_cost_pips"]))
    result = {}
    for bucket in sorted(groups):
        values = groups[bucket]; count = len(values); mean = sum(values) / count
        std = math.sqrt(sum((value - mean) ** 2 for value in values) / max(1, count - 1)); lower = mean - float(config["lower_bound_z"]) * std / math.sqrt(count)
        enough = count >= int(config["minimum_training_observations"])
        result[bucket] = {"observation_count": count, "mean_after_cost_pips": mean, "sample_standard_deviation_pips": std,
                          "lower_bound_after_cost_pips": lower, "sufficient_observations": enough,
                          "eligible": enough and (lower > 0.0 if config["require_positive_after_cost_lower_bound"] else True)}
    return result


def calibrated_entries(rows: Sequence[Mapping[str, Any]], cells: Mapping[str, Mapping[str, Any]], threshold: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted, rejected = [], []
    for original in eligible(rows, threshold):
        row = dict(original); cell = cells.get(row["liquidity_bucket"]); reason = None
        if cell is None: reason = "missing_prior_session_calibration"
        elif not cell["sufficient_observations"]: reason = "insufficient_prior_session_calibration"
        elif not cell["eligible"]: reason = "nonpositive_prior_session_lower_bound"
        if reason: rejected.append({"snapshot_id": row["snapshot_id"], "reason": reason, "liquidity_bucket": row["liquidity_bucket"]})
        else:
            row["calibrated_after_cost_pips"] = float(cell["mean_after_cost_pips"]); row["calibrated_lower_bound_pips"] = float(cell["lower_bound_after_cost_pips"]); accepted.append(row)
    accepted.sort(key=lambda row: (-row["calibrated_lower_bound_pips"], -float(row["score"]), row["instrument"], -int(row["side"])))
    return accepted, rejected


def verify(config_path: Path, state_path: Path, material_path: Path, output_path: Path) -> dict[str, Any]:
    failures: list[str] = []
    checks: dict[str, Any] = {}
    try:
        for path in (config_path, state_path, material_path):
            reject_link_chain(path)
        reject_link_chain(output_path.parent)
        config, state, supplied_material = read_json(config_path), read_json(state_path), read_json(material_path)
        source_state, manifest, source_data, source_binding = source_contract(config)
        mistake_binding = mistake_contract(config)
        expected_material = {
            "schema_version": 1, "config_sha256": file_sha(config_path), "runner_sha256": file_sha(PRODUCER),
            "core_sha256": file_sha(CORE), "foundation_core_sha256": file_sha(FOUNDATION_CORE),
            "verifier_sha256": file_sha(Path(__file__).resolve()), "source_binding": source_binding,
            "mistake_curriculum_binding": mistake_binding, "arms": list(ARMS), "costs": config["costs"],
            "policy": config["policy"], "oof_calibration": config["oof_calibration"],
            "dataset_limits": config["dataset_limits"],
            "independence": config["independence"], "artifact_time_contract": ARTIFACT_TIME_CONTRACT, "safety": SAFETY,
        }
        if supplied_material != expected_material: failures.append("material_contract")
        expected_cohort = config["experiment_key"] + "." + stable_hash(expected_material)[:20]
        if state.get("cohort_id") != expected_cohort or state.get("material_sha256") != stable_hash(expected_material): failures.append("cohort_identity")
        for key, expected in SAFETY.items():
            if state.get(key) != expected: failures.append(f"state_safety:{key}")
        if tuple(state.get("arms", ())) != ARMS: failures.append("arm_contract")
        root = state_path.parent.resolve()
        if root.name != expected_cohort: failures.append("cohort_path")
        expected_names = {"clocks", "arm_decisions", "terminals", "oof_training", "oof_calibrations"}
        if set(state.get("datasets", {})) != expected_names: failures.append("dataset_manifest")
        limits = config["dataset_limits"]
        if not (0 < int(limits["maximum_gzip_bytes"]) <= int(limits["maximum_raw_bytes"])):
            raise ValueError("invalid frozen dataset byte limits")
        if not (0 < int(limits["maximum_row_count"]) <= 100_000):
            raise ValueError("invalid frozen dataset row limit")
        output = {key: load_dataset(root, spec, limits) for key, spec in state["datasets"].items()}
        quotes = load_quotes(manifest, config)
        src_clocks = source_data["clocks"]
        src_decisions = {row["clock_id"]: row for row in source_data["decisions"]}
        src_feedback = {row["clock_id"]: row for row in source_data["feedback"]}
        expected_clocks = []
        for src in src_clocks:
            clock_id = "policyclock_" + stable_hash(expected_cohort, src["clock_id"])[:28]
            expected_clocks.append(seal({"clock_id": clock_id, "cohort_id": expected_cohort, "source_clock_id": src["clock_id"],
                "source_clock_row_sha256": src["row_sha256"], "sequence_no": src["sequence_no"], "session_key": src["session_key"],
                "decision_epoch": src["decision_epoch"], "execution_epoch": src["execution_epoch"], "feedback_epoch": src["feedback_epoch"],
                "session_end_epoch": src["session_end_epoch"], "terminal_clock": src["terminal_clock"],
                "counts_as_market_repetition": 1, "counts_as_regime_repetition": 0, "proof_eligible": False, "execution_eligible": False}))
        if output["clocks"] != expected_clocks: failures.append("clock_reconstruction")

        slip = float(config["costs"]["slippage_per_execution_leg_pips"])
        raw_training: dict[str, list[dict[str, Any]]] = {}; expected_training = []
        for clock in src_clocks:
            for candidate in src_decisions[clock["clock_id"]]["candidate_rank"]:
                obs = observation(candidate, quotes[clock["session_key"]], clock, slip); raw_training.setdefault(clock["session_key"], []).append(obs)
                expected_training.append(seal({"training_observation_id": "policytrain_" + stable_hash(clock["clock_id"], candidate["snapshot_id"])[:28],
                    "cohort_id": expected_cohort, "source_clock_id": clock["clock_id"], "session_key": clock["session_key"], **obs,
                    "counts_as_market_repetition": 0, "counts_as_regime_repetition": 0, "proof_eligible": False}))
        if output["oof_training"] != expected_training: failures.append("oof_training_reconstruction")
        training_sessions = {"20260824_mon_overlap": list(config["oof_calibration"]["monday_training_sessions"]),
            "20260826_wed_overlap": list(config["oof_calibration"]["wednesday_training_sessions"]),
            "20260828_fri_overlap": list(config["oof_calibration"]["friday_training_sessions"])}
        session_cells: dict[str, dict[str, Any]] = {}; expected_cal_rows = []
        for session in [row["session_key"] for row in manifest["sessions"]]:
            prior = training_sessions[session]; obs = [row for key in prior for row in raw_training.get(key, [])]
            cells = calibrations(obs, config["oof_calibration"]); session_cells[session] = cells
            for bucket in sorted(set(cells) | {"liquid", "normal", "elevated"}):
                cell = cells.get(bucket, {"observation_count": 0, "mean_after_cost_pips": None, "sample_standard_deviation_pips": None,
                    "lower_bound_after_cost_pips": None, "sufficient_observations": False, "eligible": False})
                expected_cal_rows.append(seal({"calibration_id": "policycal_" + stable_hash(expected_cohort, session, bucket, prior, cell)[:28],
                    "cohort_id": expected_cohort, "application_session_key": session, "training_session_keys": prior,
                    "liquidity_bucket": bucket, **cell, "same_session_training": session in prior,
                    "counts_as_market_repetition": 0, "counts_as_regime_repetition": 0, "proof_eligible": False}))
        if output["oof_calibrations"] != expected_cal_rows: failures.append("oof_calibration_reconstruction")

        expected_arm_rows, expected_terminals, summaries = [], [], {}
        for arm in ARMS:
            state_chain = flat_state(); actions: dict[str, int] = {}; error_counts: dict[str, int] = {}; legs = 0
            for clock, src_clock in zip(expected_clocks, src_clocks):
                src_decision, src_fb = src_decisions[src_clock["clock_id"]], src_feedback[src_clock["clock_id"]]
                before = copy.deepcopy(state_chain); candidates = src_decision["candidate_rank"]
                if arm == "v1_baseline_reference":
                    decision, execution, state_chain = src_decision["decision"], src_decision["execution"], src_decision["state_after"]
                    feedback = {"status": src_fb["status"], "liquidation_equity_pips": src_fb["liquidation_equity_pips"]}
                    diag = {"reference_only": True, "source_decision_id": src_decision["decision_id"], "source_decision_row_sha256": src_decision["row_sha256"], "source_feedback_row_sha256": src_fb["row_sha256"]}
                else:
                    if arm == "no_trade":
                        if state_chain["position"] is not None: raise ValueError("no-trade state not flat")
                        decision = empty_decision("wait", "predeclared terminal boundary while flat" if clock["terminal_clock"] else "frozen no-trade comparator")
                        diag = {"policy_state": "flat_no_trade"}
                    elif arm == "cost_hurdle_2x": decision, diag = choose_cost(state_chain, candidates, clock, config)
                    elif arm == "explicit_hold_vs_switch_2x":
                        decision, diag = choose_explicit(state_chain, candidates, eligible(candidates, 2.0), clock, config, "explicit hold-vs-switch 2x")
                    elif arm == "factor_conflict_suppressed_2x":
                        accepted, votes, rejected = factor_filter(eligible(candidates, 2.0), float(config["policy"]["factor_vote_tie_epsilon"]))
                        decision, diag = choose_explicit(state_chain, candidates, accepted, clock, config, "factor-conflict-suppressed 2x")
                        diag.update({"factor_vote_scores": votes, "factor_conflict_rejections": rejected})
                    else:
                        accepted, rejected = calibrated_entries(candidates, session_cells[clock["session_key"]], 2.0)
                        decision, diag = choose_explicit(state_chain, candidates, accepted, clock, config, "prior-session OOF remaining-move calibration")
                        diag.update({"training_session_keys": training_sessions[clock["session_key"]], "calibration_cells": session_cells[clock["session_key"]], "calibration_rejections": rejected})
                    state_chain, execution = apply(state_chain, decision, quotes[clock["session_key"]], clock, config, arm)
                    status, liq = equity(state_chain, quotes[clock["session_key"]], int(clock["feedback_epoch"]), slip)
                    feedback = {"status": status, "liquidation_equity_pips": liq}
                actions[decision["action"]] = actions.get(decision["action"], 0) + 1; legs += len(execution["legs"])
                if execution["status"] != "applied":
                    reason = execution["rejection_reason"] or execution["status"]; error_counts[reason] = error_counts.get(reason, 0) + 1
                expected_arm_rows.append(seal({"arm_decision_id": "policydecision_" + stable_hash(expected_cohort, arm, clock["clock_id"], before, decision)[:28],
                    "cohort_id": expected_cohort, "arm": arm, "clock_id": clock["clock_id"], "source_clock_id": src_clock["clock_id"],
                    "sequence_no": clock["sequence_no"], "session_key": clock["session_key"], "decision_epoch": clock["decision_epoch"],
                    "state_before": before, "candidate_count": len(candidates), "source_candidate_root_sha256": stable_hash(candidates),
                    "decision": decision, "policy_diagnostics": diag, "execution": execution, "state_after": state_chain,
                    "feedback": feedback, "counts_as_market_repetition": 0, "counts_as_regime_repetition": 0,
                    "proof_eligible": False, "execution_eligible": False}))
                if clock["terminal_clock"]:
                    retry = {"status": "not_needed", "legs": [], "state_after": state_chain}
                    if state_chain["position"] is not None and arm != "v1_baseline_reference":
                        retry_clock = dict(clock); retry_clock["execution_epoch"] = clock["session_end_epoch"]
                        retry_decision = {"action": "exit", "instrument": state_chain["position"]["instrument"], "side": None, "units": 0,
                            "confidence": None, "expected_move_pips": None, "horizon_min": None, "entry_condition": "", "invalidation": "",
                            "rationale": "exact session-end terminal retry", "candidate_snapshot_id": None, "branch_label": "terminal_retry"}
                        state_chain, retry = apply(state_chain, retry_decision, quotes[clock["session_key"]], retry_clock, config, arm); legs += len(retry["legs"])
                    elif arm == "v1_baseline_reference":
                        src_terminal = next(row for row in source_data["terminals"] if row["session_key"] == clock["session_key"])
                        retry, state_chain = src_terminal["retry"], src_terminal["state_after"]
                    expected_terminals.append(seal({"terminal_id": "policyterminal_" + stable_hash(expected_cohort, arm, clock["session_key"])[:28],
                        "cohort_id": expected_cohort, "arm": arm, "session_key": clock["session_key"], "terminal_epoch": clock["session_end_epoch"],
                        "retry": retry, "terminal_flat": state_chain["position"] is None, "censored": state_chain["position"] is not None,
                        "state_after": state_chain, "counts_as_market_repetition": 0, "counts_as_regime_repetition": 0, "proof_eligible": False}))
            summaries[arm] = {"action_counts": actions, "non_wait_action_count": sum(value for key, value in actions.items() if key != "wait"),
                "execution_leg_count": legs, "failure_counts": error_counts, "realized_pips": float(state_chain["realized_pips"]),
                "terminal_flat": state_chain["position"] is None}
        if output["arm_decisions"] != expected_arm_rows: failures.append("arm_decision_reconstruction")
        if output["terminals"] != expected_terminals: failures.append("terminal_reconstruction")
        if state.get("arm_summaries") != summaries: failures.append("arm_summary_reconstruction")
        expected_generated = datetime.fromtimestamp(max(int(row["feedback_epoch"]) for row in expected_clocks), tz=timezone.utc).isoformat()
        checks = {"cohort_id": expected_cohort, "source_cohort_id": source_state["cohort_id"], "source_pack_id": manifest["pack_id"],
            "global_clock_count": len(expected_clocks), "arm_decision_count": len(expected_arm_rows),
            "training_observation_count": len(expected_training), "calibration_cell_count": len(expected_cal_rows), "arm_summaries": summaries}
        expected_top = {
            "generated_utc": expected_generated, "global_clock_count": len(expected_clocks), "market_repetition_count": len(expected_clocks),
            "source_pair_context_count": int(source_state["pair_context_count"]), "source_candidate_context_count": int(source_state["candidate_context_count"]),
            "arm_decision_count": len(expected_arm_rows), "arm_decisions_count_as_rep": 0,
            "training_observation_count": len(expected_training), "training_observations_count_as_rep": 0,
            "calibration_cell_count": len(expected_cal_rows), "calibrations_count_as_rep": 0, "independent_regime_count": None,
            "paired_pips_vs_no_trade": {arm: summaries[arm]["realized_pips"] - summaries["no_trade"]["realized_pips"] for arm in ARMS},
            "paired_pips_vs_v1": {arm: summaries[arm]["realized_pips"] - summaries["v1_baseline_reference"]["realized_pips"] for arm in ARMS},
            "terminal_flat_all_arms": all(summaries[arm]["terminal_flat"] for arm in ARMS),
        }
        for key, expected in expected_top.items():
            if state.get(key) != expected: failures.append(f"state_summary:{key}")
        if state.get("source_binding") != source_binding: failures.append("state_source_binding")
        if state.get("mistake_curriculum_binding") != mistake_binding: failures.append("state_mistake_binding")
    except Exception as exc:  # fail closed and preserve the exact verifier reason
        failures.append(f"exception:{type(exc).__name__}:{exc}")
    failures = sorted(set(failures))
    generated = None
    try: generated = read_json(state_path).get("generated_utc")
    except Exception: generated = None
    receipt = {**SAFETY, "schema_version": 1, "generated_utc": generated, "verified": not failures,
               "failures": failures, "checks": checks}
    payload = (json.dumps(receipt, indent=2, sort_keys=True) + "\n").encode("utf-8")
    if output_path.exists() and is_link_or_reparse(output_path):
        raise ValueError("linked verifier output rejected")
    if output_path.exists():
        if output_path.read_bytes() != payload: raise ValueError(f"immutable artifact conflict: {output_path}")
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = output_path.parent / f".tmp-{os.getpid()}-{stable_hash(str(output_path))[:12]}"; temporary.write_bytes(payload); os.replace(temporary, output_path)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--material", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    receipt = verify(args.config, args.state, args.material, args.output)
    print(json.dumps({"verified": receipt["verified"], "failures": receipt["failures"]}, sort_keys=True))
    return 0 if receipt["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
