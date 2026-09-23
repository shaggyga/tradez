#!/usr/bin/env python3
"""Independent verifier for the all-68 sequential portfolio batch replay.

It deliberately imports neither replay producer nor either replay core.
"""

from __future__ import annotations

import ast
import csv
from datetime import datetime, timezone
import gzip
from hashlib import sha256
import io
import json
import math
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "sequential_all68_portfolio_batch_replay_v1.json"
RUNNER = ROOT / "oanda_sequential_all68_portfolio_batch_replay.py"
CORE = ROOT / "src" / "forex_system" / "research" / "sequential_all68_portfolio_batch_replay_v1.py"
FOUNDATION_CORE = ROOT / "src" / "forex_system" / "research" / "sequential_portfolio_replay_v1.py"
ARTIFACT_ROOT = ROOT / "data" / "oanda_training_manager" / "research_ledgers" / "sequential_all68_portfolio_batch_replay_v1"
STATE = ARTIFACT_ROOT / "sequential_all68_portfolio_batch_replay_v1.json"
OUTPUT = ARTIFACT_ROOT / "sequential_all68_portfolio_batch_replay_verifier_v1.json"

SAFETY = {
    "research_only": True,
    "execution_eligible": False,
    "proof_eligible": False,
    "can_promote": False,
    "can_place_orders": False,
    "can_authorize": False,
    "broker_access": False,
    "account_access": False,
    "supported_decision": "no_trade",
}

DATASET_FILES = {
    "clocks": "global_clocks.jsonl.gz",
    "pair_contexts": "pair_contexts.jsonl.gz",
    "decisions": "portfolio_decisions.jsonl.gz",
    "feedback": "feedback.jsonl.gz",
    "counterfactuals": "counterfactuals.jsonl.gz",
    "terminals": "session_terminals.jsonl.gz",
}

ARTIFACT_TIME_CONTRACT = {
    "kind": "maximum_scheduled_feedback_epoch",
    "timezone": "UTC",
    "wall_clock_independent": True,
}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def stable_hash(*parts: Any) -> str:
    return sha256(canonical(parts).encode("utf-8")).hexdigest()


def file_sha(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while block := handle.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("expected object")
    return value


def atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def write_json(
    path: Path, value: Mapping[str, Any], *, immutable: bool = False
) -> None:
    payload = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    if immutable and path.exists():
        if path.read_bytes() != payload:
            raise ValueError(f"immutable artifact conflict: {path}")
        return
    atomic_bytes(path, payload)


def deterministic_generated_utc(clocks: list[Mapping[str, Any]]) -> str:
    epochs = [int(row["feedback_epoch"]) for row in clocks]
    if not epochs:
        raise ValueError("cannot derive artifact timestamp without scheduled clocks")
    return datetime.fromtimestamp(max(epochs), tz=timezone.utc).isoformat()


def row_hash(row: Mapping[str, Any]) -> str:
    return stable_hash({key: value for key, value in row.items() if key != "row_sha256"})


def sealed(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["row_sha256"] = row_hash(payload)
    return payload


def load_dataset(cohort_root: Path, spec: Mapping[str, Any], maximum_gzip: int = 64 * 1024 * 1024, maximum_raw: int = 256 * 1024 * 1024) -> list[dict[str, Any]]:
    name = str(spec["relative_path"])
    path = cohort_root / name
    if Path(name).is_absolute() or ".." in Path(name).parts or path.resolve().parent != cohort_root.resolve():
        raise ValueError("dataset path escape")
    if path.is_symlink() or path.stat().st_size > maximum_gzip:
        raise ValueError("unsafe dataset")
    payload = path.read_bytes()
    if sha256(payload).hexdigest() != spec["gzip_sha256"] or len(payload) != int(spec["gzip_bytes"]):
        raise ValueError("dataset gzip mismatch")
    with gzip.GzipFile(fileobj=io.BytesIO(payload), mode="rb") as handle:
        raw = handle.read(maximum_raw + 1)
    if len(raw) > maximum_raw:
        raise ValueError("dataset decompressed limit")
    rows = [json.loads(line) for line in raw.decode("utf-8").splitlines() if line]
    if len(rows) != int(spec["row_count"]):
        raise ValueError("dataset count mismatch")
    if any(row_hash(row) != row.get("row_sha256") for row in rows):
        raise ValueError("dataset row hash mismatch")
    if stable_hash(sorted(row["row_sha256"] for row in rows)) != spec["row_set_sha256"]:
        raise ValueError("dataset set root mismatch")
    if stable_hash([row["row_sha256"] for row in rows]) != spec["ordered_row_sha256"]:
        raise ValueError("dataset order root mismatch")
    return rows


def forbidden_imports() -> list[str]:
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    blocked: list[str] = []
    for node in ast.walk(tree):
        names: list[str]
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        else:
            continue
        for name in names:
            if "sequential_all68_portfolio" in name or "sequential_portfolio_replay" in name:
                blocked.append(name)
    return sorted(set(blocked))


def parse_epoch(value: str) -> int:
    return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())


def pip_size(instrument: str) -> float:
    return 0.01 if instrument.endswith("_JPY") else 0.0001


def read_market(path: Path, row: Mapping[str, Any], maximum_raw: int) -> dict[int, tuple[float, ...]]:
    if path.is_symlink() or file_sha(path) != row["gzip_sha256"]:
        raise ValueError("source archive mismatch")
    with gzip.open(path, "rb") as handle:
        raw = handle.read(maximum_raw + 1)
    if len(raw) > maximum_raw or sha256(raw).hexdigest() != row["raw_sha256"]:
        raise ValueError("source raw mismatch")
    values: dict[int, tuple[float, ...]] = {}
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))
    fields = ("bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low", "ask_close")
    for source in reader:
        epoch = parse_epoch(source["time"])
        values[epoch] = tuple(float(source[name]) for name in fields)
    return values


def exact_candidate(instrument: str, source_hash: str, market: Mapping[int, tuple[float, ...]], epoch: int, config: Mapping[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    lookbacks = [int(value) for value in config["frozen_policy"]["lookbacks_min"]]
    causal_epoch = epoch - 60
    full_minutes = [causal_epoch - minute * 60 for minute in range(max(lookbacks) + 1)]
    causal_ready = all(value in market for value in full_minutes)
    execution_ready = epoch + 60 in market
    feedback_ready = epoch + 300 in market
    status = {
        "causal_epoch": causal_epoch, "causal_ready": causal_ready,
        "execution_ready": execution_ready, "feedback_ready": feedback_ready,
        "fully_ready": causal_ready and execution_ready and feedback_ready,
        "missing_reasons": (["causal_context"] if not causal_ready else [])
        + (["exact_execution_quote"] if not execution_ready else [])
        + (["exact_feedback_quote"] if not feedback_ready else []),
    }
    if not causal_ready:
        return None, status
    pip = pip_size(instrument)
    current_row = market[causal_epoch]
    current = (current_row[3] + current_row[7]) / 2.0
    spread = (current_row[7] - current_row[3]) / pip
    if spread <= 0 or spread > float(config["costs"]["maximum_entry_spread_pips"]):
        return None, status
    moves, slopes = [], []
    for lookback in lookbacks:
        prior = market[causal_epoch - lookback * 60]
        prior_mid = (prior[3] + prior[7]) / 2.0
        move = (current - prior_mid) / pip
        moves.append(move)
        slopes.append(move / math.sqrt(max(1.0, float(lookback))))
    weights = [float(value) for value in config["frozen_policy"]["slope_weights"]]
    weighted = sum(weight * slope for weight, slope in zip(weights, slopes))
    if abs(weighted) <= 1e-12:
        return None, status
    side = 1 if weighted > 0 else -1
    aligned = sum((move > 0) == (side > 0) for move in moves)
    if aligned < int(config["frozen_policy"]["minimum_aligned_windows"]):
        return None, status
    score = abs(weighted) / max(0.01, spread + float(config["costs"]["round_trip_slippage_pips"]))
    if score < float(config["frozen_policy"]["minimum_score_cost_ratio"]):
        return None, status
    floor = float(config["frozen_policy"]["confidence_floor"])
    confidence = min(float(config["frozen_policy"]["confidence_ceiling"]), max(floor, floor + (score - 1.0) * float(config["frozen_policy"]["confidence_scale"])))
    snapshot_payload = {
        "instrument": instrument, "decision_epoch": epoch, "source_sha256": source_hash,
        "side": side, "score": round(score, 12), "moves_pips": [round(value, 12) for value in moves],
        "spread_pips": round(spread, 12),
    }
    candidate = {
        "instrument": instrument, "side": side, "score": score, "confidence": confidence,
        "expected_move_pips": abs(weighted) * math.sqrt(5.0), "spread_pips": spread,
        "liquidity_bucket": "liquid" if spread <= 2 else "normal" if spread <= 3 else "elevated" if spread <= 5 else "wide",
        "aligned_windows": aligned, "moves_pips": moves,
        "snapshot_id": "candidate_" + stable_hash(snapshot_payload)[:28],
    }
    return candidate, status


def decision_from_candidate(candidate: Mapping[str, Any], action: str, config: Mapping[str, Any], rationale: str) -> dict[str, Any]:
    return {
        "action": action, "instrument": candidate["instrument"], "side": candidate["side"],
        "units": int(config["frozen_policy"]["normalized_units"]), "confidence": candidate["confidence"],
        "expected_move_pips": candidate["expected_move_pips"], "horizon_min": int(config["schedule"]["feedback_horizon_min"]),
        "entry_condition": f"frozen_rank_score>={config['frozen_policy']['minimum_score_cost_ratio']}",
        "invalidation": "frozen_rank_reversal_or_maximum_holding", "rationale": rationale,
        "candidate_snapshot_id": candidate["snapshot_id"], "branch_label": "primary",
    }


def expected_decision(state: Mapping[str, Any], candidates: list[dict[str, Any]], epoch: int, terminal: bool, config: Mapping[str, Any], blocked: bool) -> dict[str, Any]:
    position = state["position"]
    if blocked:
        action = "wait" if position is None else "hold"
        return {"action": action, "instrument": None, "side": None, "units": 0, "confidence": None, "expected_move_pips": None, "horizon_min": None, "entry_condition": "", "invalidation": "" if position is None else "unresolved_terminal_state", "rationale": "prior unresolved terminal state", "candidate_snapshot_id": None, "branch_label": "primary"}
    if terminal:
        if position is None:
            return {"action":"wait","instrument":None,"side":None,"units":0,"confidence":None,"expected_move_pips":None,"horizon_min":None,"entry_condition":"","invalidation":"","rationale":"predeclared terminal boundary while flat","candidate_snapshot_id":None,"branch_label":"primary"}
        return {"action":"exit","instrument":position["instrument"],"side":None,"units":0,"confidence":None,"expected_move_pips":None,"horizon_min":None,"entry_condition":"","invalidation":"","rationale":"predeclared terminal flattening","candidate_snapshot_id":None,"branch_label":"primary"}
    best = candidates[0] if candidates else None
    if position is None:
        if best is None:
            return {"action":"wait","instrument":None,"side":None,"units":0,"confidence":None,"expected_move_pips":None,"horizon_min":None,"entry_condition":"","invalidation":"","rationale":"no frozen-policy candidate","candidate_snapshot_id":None,"branch_label":"primary"}
        return decision_from_candidate(best, "enter", config, "highest frozen cost-adjusted candidate")
    held = max(0, epoch - int(position["entry_epoch"])) // 60
    if held >= int(config["frozen_policy"]["maximum_holding_min"]):
        return {"action":"exit","instrument":position["instrument"],"side":None,"units":0,"confidence":None,"expected_move_pips":None,"horizon_min":None,"entry_condition":"","invalidation":"","rationale":"predeclared maximum holding time","candidate_snapshot_id":None,"branch_label":"primary"}
    current = next((row for row in candidates if row["instrument"] == position["instrument"] and row["side"] == position["side"]), None)
    if best is None:
        return {"action":"exit","instrument":position["instrument"],"side":None,"units":0,"confidence":None,"expected_move_pips":None,"horizon_min":None,"entry_condition":"","invalidation":"","rationale":"no continuing frozen-policy candidate","candidate_snapshot_id":None,"branch_label":"primary"}
    if current is None:
        return decision_from_candidate(best, "rotate", config, "current thesis invalidated; rotate to best candidate")
    if best["instrument"] == current["instrument"] and best["side"] == current["side"]:
        return {"action":"hold","instrument":None,"side":None,"units":0,"confidence":current["confidence"],"expected_move_pips":current["expected_move_pips"],"horizon_min":int(config["schedule"]["feedback_horizon_min"]),"entry_condition":"","invalidation":"frozen_rank_reversal_or_maximum_holding","rationale":"current position remains highest-ranked","candidate_snapshot_id":current["snapshot_id"],"branch_label":"primary"}
    if best["score"] / max(1e-12, current["score"]) >= float(config["frozen_policy"]["rotation_improvement_multiple"]):
        return decision_from_candidate(best, "rotate", config, "alternative clears frozen rotation-improvement multiple")
    return {"action":"hold","instrument":None,"side":None,"units":0,"confidence":current["confidence"],"expected_move_pips":current["expected_move_pips"],"horizon_min":int(config["schedule"]["feedback_horizon_min"]),"entry_condition":"","invalidation":"frozen_rank_reversal_or_maximum_holding","rationale":"alternative does not clear rotation cost hurdle","candidate_snapshot_id":current["snapshot_id"],"branch_label":"primary"}


def apply(state: Mapping[str, Any], decision: Mapping[str, Any], market_by_pair: Mapping[str, Mapping[int, tuple[float, ...]]], epoch: int, config: Mapping[str, Any], clock_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    action, slip = decision["action"], float(config["costs"]["slippage_per_execution_leg_pips"])
    if action in ("wait", "hold"):
        return dict(state), {"status":"applied","rejection_reason":"","realized_delta_pips":0.0,"legs":[],"state_after":dict(state)}
    pairs = [state["position"]["instrument"]] if action == "exit" else ([state["position"]["instrument"], decision["instrument"]] if action == "rotate" else [decision["instrument"]])
    if any(epoch not in market_by_pair[pair] for pair in pairs):
        return dict(state), {"status":"rejected","rejection_reason":"missing_exact_execution_quote","realized_delta_pips":0.0,"legs":[],"state_after":dict(state)}
    if action in ("enter", "rotate"):
        pair = decision["instrument"]; quote = market_by_pair[pair][epoch]; spread = (quote[4]-quote[0])/pip_size(pair)
        if spread > float(config["costs"]["maximum_entry_spread_pips"]):
            return dict(state), {"status":"rejected","rejection_reason":"execution_spread_above_limit","realized_delta_pips":0.0,"legs":[],"state_after":dict(state)}
    result = {"realized_pips": float(state["realized_pips"]), "position": state["position"]}
    legs, delta = [], 0.0
    if action in ("exit", "rotate"):
        pos = result["position"]; pair = pos["instrument"]; quote = market_by_pair[pair][epoch]; pip = pip_size(pair); bid, ask = quote[0], quote[4]
        raw = bid if pos["side"] > 0 else ask; executed = raw - slip*pip if pos["side"] > 0 else raw + slip*pip
        realized = pos["side"] * (executed-pos["entry_price"])/pip * pos["units"]
        spread=(ask-bid)/pip; delta += realized; result["realized_pips"] += realized
        legs.append({"leg_kind":"close","instrument":pair,"side":pos["side"],"units":pos["units"],"execution_epoch":epoch,"raw_price":raw,"executed_price":executed,"spread_pips":spread,"slippage_pips":slip,"realized_pips":realized,"thesis_id":pos["thesis_id"]})
        result["position"] = None
    if action in ("enter", "rotate"):
        pair=decision["instrument"]; side=int(decision["side"]); quote=market_by_pair[pair][epoch]; pip=pip_size(pair); bid,ask=quote[0],quote[4]
        raw=ask if side>0 else bid; executed=raw+slip*pip if side>0 else raw-slip*pip; spread=(ask-bid)/pip
        thesis="thesis_"+stable_hash("aligned_multiscale_momentum_rank_v1",pair,side,clock_id)[:28]
        result["position"]={"instrument":pair,"side":side,"units":int(decision["units"]),"entry_epoch":epoch,"entry_price":executed,"entry_raw_price":raw,"entry_spread_pips":spread,"entry_slippage_pips":slip,"entry_clock_id":clock_id,"thesis_id":thesis}
        legs.append({"leg_kind":"open","instrument":pair,"side":side,"units":int(decision["units"]),"execution_epoch":epoch,"raw_price":raw,"executed_price":executed,"spread_pips":spread,"slippage_pips":slip,"realized_pips":0.0,"thesis_id":thesis})
    payload={"status":"applied","rejection_reason":"","realized_delta_pips":delta,"legs":legs,"state_after":result}
    return result,payload


def expected_schedule(
    manifest: Mapping[str, Any], config: Mapping[str, Any], cohort_id: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cadence_min = int(config["schedule"]["decision_cadence_min"])
    cadence = cadence_min * 60
    execution_delay = int(config["schedule"]["execution_delay_min"]) * 60
    feedback_horizon = int(config["schedule"]["feedback_horizon_min"]) * 60
    if cadence <= 0 or execution_delay <= 0 or feedback_horizon <= 0:
        raise ValueError("invalid schedule widths")
    sessions: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for source in manifest.get("sessions") or []:
        key = str(source.get("session_key") or "")
        start = parse_epoch(str(source["start_utc"]))
        end = parse_epoch(str(source["end_utc_exclusive"]))
        if not key or key in seen_keys or start >= end or start % cadence:
            raise ValueError("invalid bound source session schedule")
        seen_keys.add(key)
        sessions.append(
            {
                "session_key": key,
                "session_start_epoch": start,
                "session_end_epoch": end,
            }
        )
    sessions.sort(key=lambda row: (int(row["session_start_epoch"]), row["session_key"]))
    clocks: list[dict[str, Any]] = []
    sequence = 0
    for session in sessions:
        epochs = list(
            range(
                int(session["session_start_epoch"]),
                int(session["session_end_epoch"]),
                cadence,
            )
        )
        for ordinal, epoch in enumerate(epochs, start=1):
            sequence += 1
            clock = {
                "sequence_no": sequence,
                "session_key": session["session_key"],
                "session_clock_ordinal": ordinal,
                "decision_epoch": epoch,
                "execution_epoch": epoch + execution_delay,
                "feedback_epoch": epoch + feedback_horizon,
                "session_end_epoch": int(session["session_end_epoch"]),
                "terminal_clock": ordinal == len(epochs),
                "counts_as_market_repetition": 1,
                "counts_as_regime_repetition": 0,
            }
            clocks.append(
                sealed(
                    {
                        "clock_id": "a68clock_" + stable_hash(cohort_id, clock)[:28],
                        "cohort_id": cohort_id,
                        **clock,
                        "proof_eligible": False,
                        "execution_eligible": False,
                    }
                )
            )
    return sessions, clocks


def compare_exact_rows(
    label: str,
    actual: list[dict[str, Any]],
    expected: list[dict[str, Any]],
    identity_key: str,
    failures: list[str],
) -> None:
    if len(actual) != len(expected):
        failures.append(f"{label}_count")
    try:
        actual_ids = [str(row[identity_key]) for row in actual]
        expected_ids = [str(row[identity_key]) for row in expected]
    except (KeyError, TypeError):
        failures.append(f"{label}_identity")
        return
    if len(set(actual_ids)) != len(actual_ids):
        failures.append(f"{label}_duplicate_identity")
    if set(actual_ids) != set(expected_ids):
        failures.append(f"{label}_identity_set")
    actual_by_id = {str(row[identity_key]): row for row in actual}
    for expected_row in expected:
        identity = str(expected_row[identity_key])
        if actual_by_id.get(identity) != expected_row:
            failures.append(f"{label}_row")
            break
    if actual_ids != expected_ids:
        failures.append(f"{label}_order")


def feedback_equity(
    state: Mapping[str, Any],
    markets: Mapping[str, Mapping[int, tuple[float, ...]]],
    epoch: int,
    slippage_pips: float,
) -> tuple[str, float | None]:
    position = state.get("position")
    if position is None:
        return "available_flat", float(state["realized_pips"])
    instrument = str(position["instrument"])
    market = markets[instrument]
    if int(epoch) not in market:
        return "missing_exact_feedback_quote", None
    quote = market[int(epoch)]
    pip = pip_size(instrument)
    side = int(position["side"])
    raw = float(quote[0] if side > 0 else quote[4])
    executed = raw - float(slippage_pips) * pip if side > 0 else raw + float(slippage_pips) * pip
    realized = side * (executed - float(position["entry_price"])) / pip * int(position["units"])
    return "available", float(state["realized_pips"]) + realized


def branch_decision(
    candidate: Mapping[str, Any],
    action: str,
    config: Mapping[str, Any],
    rationale: str,
    branch_label: str,
) -> dict[str, Any]:
    decision = decision_from_candidate(candidate, action, config, rationale)
    decision["branch_label"] = branch_label
    return decision


def legal_branches(
    state: Mapping[str, Any],
    primary: Mapping[str, Any],
    candidates: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    position = state.get("position")
    best = candidates[0] if candidates else None
    second = candidates[1] if len(candidates) > 1 else None
    branches: list[dict[str, Any]] = []
    if position is None:
        branches.append(
            {
                "action": "wait",
                "instrument": None,
                "side": None,
                "units": 0,
                "confidence": None,
                "expected_move_pips": None,
                "horizon_min": None,
                "entry_condition": "",
                "invalidation": "",
                "rationale": "depth-one no-trade branch",
                "candidate_snapshot_id": None,
                "branch_label": "wait",
            }
        )
        if best is not None:
            branches.append(
                branch_decision(
                    best,
                    "enter",
                    config,
                    "depth-one enter_best branch",
                    "enter_best",
                )
            )
        if second is not None:
            branches.append(
                branch_decision(
                    second,
                    "enter",
                    config,
                    "depth-one enter_second branch",
                    "enter_second",
                )
            )
        if best is not None:
            flipped = dict(best)
            flipped["side"] = -int(best["side"])
            flipped["snapshot_id"] = "candidate_" + stable_hash(best["snapshot_id"], "flipped")[:28]
            branches.append(
                branch_decision(
                    flipped,
                    "enter",
                    config,
                    "depth-one flipped-best branch",
                    "enter_flipped_best",
                )
            )
    else:
        branches.extend(
            [
                {
                    "action": "hold",
                    "instrument": None,
                    "side": None,
                    "units": 0,
                    "confidence": None,
                    "expected_move_pips": None,
                    "horizon_min": None,
                    "entry_condition": "",
                    "invalidation": "",
                    "rationale": "depth-one hold branch",
                    "candidate_snapshot_id": None,
                    "branch_label": "hold",
                },
                {
                    "action": "exit",
                    "instrument": position["instrument"],
                    "side": None,
                    "units": 0,
                    "confidence": None,
                    "expected_move_pips": None,
                    "horizon_min": None,
                    "entry_condition": "",
                    "invalidation": "",
                    "rationale": "depth-one exit branch",
                    "candidate_snapshot_id": None,
                    "branch_label": "exit",
                },
            ]
        )
        if best is not None and (
            best["instrument"] != position["instrument"]
            or int(best["side"]) != int(position["side"])
        ):
            branches.append(
                branch_decision(
                    best,
                    "rotate",
                    config,
                    "depth-one rotate-best branch",
                    "rotate_best",
                )
            )
    primary_identity = (
        primary.get("action"),
        primary.get("instrument"),
        primary.get("side"),
        int(primary.get("units") or 0),
    )
    unique: dict[str, dict[str, Any]] = {}
    for branch in branches:
        identity = (
            branch.get("action"),
            branch.get("instrument"),
            branch.get("side"),
            int(branch.get("units") or 0),
        )
        if identity != primary_identity:
            unique.setdefault(canonical(identity), branch)
    return [unique[key] for key in sorted(unique)]


def verify() -> dict[str, Any]:
    failures: list[str] = []
    state: dict[str, Any] = {}
    artifact_generated_utc: str | None = None
    try:
        config, state = read_json(CONFIG), read_json(STATE)
        for key, expected in SAFETY.items():
            if config.get(key) != expected:
                failures.append("config_safety")
                break
        for key, expected in SAFETY.items():
            if state.get(key) != expected:
                failures.append("state_safety")
                break
        cohort_id = str(state.get("cohort_id") or "")
        expected_cohort_root = (ARTIFACT_ROOT / "cohorts" / cohort_id).resolve()
        cohort_root = Path(str(state["artifact_root"])).resolve()
        if cohort_root != expected_cohort_root or ARTIFACT_ROOT.resolve() not in cohort_root.parents:
            failures.append("artifact_containment")
        archived_manifest = read_json(cohort_root / "source_pack_manifest.json")
        archived_receipt = read_json(cohort_root / "source_pack_clean_verifier_receipt.json")
        if archived_manifest.get("pack_id") != config["source"]["required_pack_id"]:
            failures.append("source_pack_id")
        if (
            archived_receipt.get("verified") is not True
            or archived_receipt.get("failures") != []
            or archived_receipt.get("pack_id") != archived_manifest.get("pack_id")
            or archived_receipt.get("material_sha256") != archived_manifest.get("material_sha256")
            or archived_receipt.get("source_manifest_sha256")
            != archived_manifest.get("source_manifest_sha256")
        ):
            failures.append("source_clean_receipt")
        archive_hashes = [
            {
                "instrument": row["instrument"],
                "session_key": row["session_key"],
                "raw_sha256": row["raw_sha256"],
                "gzip_sha256": row["gzip_sha256"],
                "archive_relative_path": row["archive_relative_path"],
            }
            for row in archived_manifest["source_manifest"]
        ]
        binding = {
            "pack_id": archived_manifest["pack_id"],
            "pack_manifest_sha256": file_sha(cohort_root / "source_pack_manifest.json"),
            "clean_verifier_receipt_sha256": file_sha(
                cohort_root / "source_pack_clean_verifier_receipt.json"
            ),
            "archive_binding_sha256": stable_hash(archive_hashes),
            "archive_count": len(archive_hashes),
        }
        if state.get("source_binding") != binding:
            failures.append("source_archive_binding")
        material = {
            "schema_version": 1,
            "config_sha256": file_sha(CONFIG),
            "runner_sha256": file_sha(RUNNER),
            "core_sha256": file_sha(CORE),
            "foundation_core_sha256": file_sha(FOUNDATION_CORE),
            "verifier_sha256": file_sha(Path(__file__)),
            "source_binding": binding,
            "schedule": config["schedule"],
            "costs": config["costs"],
            "frozen_policy": config["frozen_policy"],
            "counterfactuals": config["counterfactuals"],
            "independence": config["independence"],
            "artifact_time_contract": ARTIFACT_TIME_CONTRACT,
            "safety": SAFETY,
        }
        material_sha = stable_hash(material)
        if (
            material != state.get("material_contract")
            or material_sha != state.get("material_sha256")
            or config["experiment_key"] + "." + material_sha[:20] != cohort_id
        ):
            failures.append("material_contract")
        blocked_imports = forbidden_imports()
        if blocked_imports:
            failures.append("verifier_import_isolation")

        specs = state.get("datasets")
        if not isinstance(specs, dict) or set(specs) != set(DATASET_FILES):
            failures.append("dataset_manifest")
            raise ValueError("incomplete dataset manifest")
        for name, filename in DATASET_FILES.items():
            if specs[name].get("relative_path") != filename:
                failures.append("dataset_manifest")
        datasets = {
            name: load_dataset(cohort_root, specs[name]) for name in DATASET_FILES
        }

        sessions, expected_clocks = expected_schedule(archived_manifest, config, cohort_id)
        artifact_generated_utc = deterministic_generated_utc(expected_clocks)
        if (
            len(sessions) != int(config["schedule"]["expected_session_count"])
            or len(expected_clocks) != int(config["schedule"]["expected_global_clock_count"])
        ):
            failures.append("schedule_contract")
        compare_exact_rows(
            "clocks", datasets["clocks"], expected_clocks, "clock_id", failures
        )

        source_rows = list(archived_manifest["source_manifest"])
        instruments = sorted({str(row["instrument"]) for row in source_rows})
        session_keys = [str(row["session_key"]) for row in sessions]
        source_identities = [
            (str(row["session_key"]), str(row["instrument"])) for row in source_rows
        ]
        expected_source_identities = {
            (session_key, instrument)
            for session_key in session_keys
            for instrument in instruments
        }
        if (
            len(instruments) != 68
            or len(source_identities) != 68 * len(sessions)
            or len(set(source_identities)) != len(source_identities)
            or set(source_identities) != expected_source_identities
        ):
            failures.append("source_manifest_identity")
        aliases = {
            instrument: f"P{index:03d}"
            for index, instrument in enumerate(instruments, start=1)
        }
        if state.get("pair_aliases") != aliases:
            failures.append("stable_aliases")

        source_root = (ROOT / config["source"]["artifact_relative_root"]).resolve()
        markets: dict[str, dict[str, dict[int, tuple[float, ...]]]] = {}
        source_hashes: dict[tuple[str, str], str] = {}
        seen_archive_paths: set[str] = set()
        for row in source_rows:
            relative = Path(str(row["archive_relative_path"]))
            archive = (source_root / relative).resolve()
            if (
                relative.is_absolute()
                or ".." in relative.parts
                or source_root not in archive.parents
                or archive.as_posix().lower() in seen_archive_paths
                or archive.stat().st_size != int(row["gzip_bytes"])
            ):
                raise ValueError("unsafe or duplicate source archive")
            seen_archive_paths.add(archive.as_posix().lower())
            identity = (str(row["session_key"]), str(row["instrument"]))
            source_hashes[identity] = str(row["raw_sha256"])
            markets.setdefault(identity[0], {})[identity[1]] = read_market(
                archive,
                row,
                int(config["source"]["maximum_archive_raw_bytes"]),
            )

        expected_contexts: list[dict[str, Any]] = []
        contexts_by_clock: dict[str, list[dict[str, Any]]] = {}
        candidates_by_clock: dict[str, list[dict[str, Any]]] = {}
        for clock in expected_clocks:
            clock_contexts: list[dict[str, Any]] = []
            candidates: list[dict[str, Any]] = []
            session_key = str(clock["session_key"])
            for instrument in instruments:
                candidate, status = exact_candidate(
                    instrument,
                    source_hashes[(session_key, instrument)],
                    markets[session_key][instrument],
                    int(clock["decision_epoch"]),
                    config,
                )
                if candidate is not None:
                    candidates.append(candidate)
                context = sealed(
                    {
                        "context_id": "a68context_"
                        + stable_hash(clock["clock_id"], aliases[instrument])[:28],
                        "cohort_id": cohort_id,
                        "clock_id": clock["clock_id"],
                        "session_key": session_key,
                        "decision_epoch": int(clock["decision_epoch"]),
                        "pair_alias": aliases[instrument],
                        "instrument": instrument,
                        "source_sha256": source_hashes[(session_key, instrument)],
                        **status,
                        "ranked_candidate": candidate is not None,
                        "candidate": candidate,
                        "counts_as_market_repetition": 0,
                        "counts_as_regime_repetition": 0,
                        "proof_eligible": False,
                    }
                )
                clock_contexts.append(context)
                expected_contexts.append(context)
            candidates.sort(
                key=lambda row: (-row["score"], row["instrument"], -row["side"])
            )
            contexts_by_clock[str(clock["clock_id"])] = clock_contexts
            candidates_by_clock[str(clock["clock_id"])] = candidates
        if len(expected_contexts) != int(config["schedule"]["expected_pair_context_count"]):
            failures.append("pair_context_contract")
        compare_exact_rows(
            "pair_contexts",
            datasets["pair_contexts"],
            expected_contexts,
            "context_id",
            failures,
        )

        expected_decisions: list[dict[str, Any]] = []
        expected_feedback: list[dict[str, Any]] = []
        expected_counterfactuals: list[dict[str, Any]] = []
        expected_terminals: list[dict[str, Any]] = []
        state_chain: dict[str, Any] = {"realized_pips": 0.0, "position": None}
        unresolved_terminal = False
        action_counts: dict[str, int] = {}
        failure_counts: dict[str, int] = {}
        execution_leg_count = 0
        slippage = float(config["costs"]["slippage_per_execution_leg_pips"])
        for clock in expected_clocks:
            clock_id = str(clock["clock_id"])
            session_key = str(clock["session_key"])
            pre_state = state_chain
            candidates = candidates_by_clock[clock_id]
            context_root = stable_hash(
                [row["row_sha256"] for row in contexts_by_clock[clock_id]]
            )
            decision = expected_decision(
                pre_state,
                candidates,
                int(clock["decision_epoch"]),
                bool(clock["terminal_clock"]),
                config,
                unresolved_terminal,
            )
            decision_id = "a68decision_" + stable_hash(
                clock_id, pre_state, decision, context_root
            )[:28]
            state_chain, execution = apply(
                pre_state,
                decision,
                markets[session_key],
                int(clock["execution_epoch"]),
                config,
                clock_id,
            )
            decision_row = sealed(
                {
                    "decision_id": decision_id,
                    "cohort_id": cohort_id,
                    "clock_id": clock_id,
                    "session_key": session_key,
                    "sequence_no": int(clock["sequence_no"]),
                    "decision_epoch": int(clock["decision_epoch"]),
                    "context_root_sha256": context_root,
                    "causal_candidate_count": len(candidates),
                    "candidate_rank": candidates,
                    "state_before": pre_state,
                    "decision": decision,
                    "execution": execution,
                    "state_after": state_chain,
                    "counts_as_market_repetition": 1,
                    "counts_as_regime_repetition": 0,
                    "proof_eligible": False,
                    "execution_eligible": False,
                }
            )
            expected_decisions.append(decision_row)
            action = str(decision["action"])
            action_counts[action] = action_counts.get(action, 0) + 1
            execution_leg_count += len(execution["legs"])
            if execution["status"] != "applied":
                reason = str(execution["rejection_reason"] or execution["status"])
                failure_counts[reason] = failure_counts.get(reason, 0) + 1

            feedback_status, feedback_value = feedback_equity(
                state_chain,
                markets[session_key],
                int(clock["feedback_epoch"]),
                slippage,
            )
            feedback_row = sealed(
                {
                    "feedback_id": "a68feedback_"
                    + stable_hash(decision_id, int(clock["feedback_epoch"]))[:28],
                    "cohort_id": cohort_id,
                    "decision_id": decision_id,
                    "clock_id": clock_id,
                    "feedback_epoch": int(clock["feedback_epoch"]),
                    "status": feedback_status,
                    "liquidation_equity_pips": feedback_value,
                    "counts_as_market_repetition": 0,
                    "counts_as_regime_repetition": 0,
                    "proof_eligible": False,
                }
            )
            expected_feedback.append(feedback_row)
            if feedback_status.startswith("missing"):
                failure_counts[feedback_status] = failure_counts.get(feedback_status, 0) + 1

            for branch in legal_branches(pre_state, decision, candidates, config):
                branch_state, branch_execution = apply(
                    pre_state,
                    branch,
                    markets[session_key],
                    int(clock["execution_epoch"]),
                    config,
                    clock_id,
                )
                branch_status, branch_value = feedback_equity(
                    branch_state,
                    markets[session_key],
                    int(clock["feedback_epoch"]),
                    slippage,
                )
                expected_counterfactuals.append(
                    sealed(
                        {
                            "counterfactual_id": "a68cf_"
                            + stable_hash(decision_id, branch)[:28],
                            "cohort_id": cohort_id,
                            "decision_id": decision_id,
                            "clock_id": clock_id,
                            "branch": branch,
                            "execution": branch_execution,
                            "feedback_status": branch_status,
                            "liquidation_equity_pips": branch_value,
                            "depth": 1,
                            "counts_as_market_repetition": 0,
                            "counts_as_regime_repetition": 0,
                            "proof_eligible": False,
                        }
                    )
                )

            if bool(clock["terminal_clock"]):
                retry: dict[str, Any] = {
                    "status": "not_needed",
                    "legs": [],
                    "state_after": state_chain,
                }
                if state_chain["position"] is not None:
                    exit_decision = {
                        "action": "exit",
                        "instrument": state_chain["position"]["instrument"],
                        "side": None,
                        "units": 0,
                    }
                    state_chain, retry = apply(
                        state_chain,
                        exit_decision,
                        markets[session_key],
                        int(clock["session_end_epoch"]),
                        config,
                        clock_id,
                    )
                terminal_flat = state_chain["position"] is None
                if not terminal_flat:
                    unresolved_terminal = True
                execution_leg_count += len(retry.get("legs") or [])
                expected_terminals.append(
                    sealed(
                        {
                            "terminal_id": "a68terminal_"
                            + stable_hash(cohort_id, session_key)[:28],
                            "cohort_id": cohort_id,
                            "session_key": session_key,
                            "terminal_epoch": int(clock["session_end_epoch"]),
                            "retry": retry,
                            "terminal_flat": terminal_flat,
                            "censored": not terminal_flat,
                            "state_after": state_chain,
                            "counts_as_market_repetition": 0,
                            "counts_as_regime_repetition": 0,
                            "proof_eligible": False,
                        }
                    )
                )

        compare_exact_rows(
            "decisions", datasets["decisions"], expected_decisions, "decision_id", failures
        )
        compare_exact_rows(
            "feedback", datasets["feedback"], expected_feedback, "feedback_id", failures
        )
        compare_exact_rows(
            "counterfactuals",
            datasets["counterfactuals"],
            expected_counterfactuals,
            "counterfactual_id",
            failures,
        )
        compare_exact_rows(
            "terminals", datasets["terminals"], expected_terminals, "terminal_id", failures
        )

        context_availability = {
            "missing_causal_context": sum(
                not row["causal_ready"] for row in expected_contexts
            ),
            "missing_exact_execution_quote": sum(
                not row["execution_ready"] for row in expected_contexts
            ),
            "missing_exact_feedback_quote": sum(
                not row["feedback_ready"] for row in expected_contexts
            ),
            "fully_ready": sum(bool(row["fully_ready"]) for row in expected_contexts),
        }
        summary = {
            "schema_version": 1,
            "generated_utc": artifact_generated_utc,
            "cohort_id": cohort_id,
            "material_sha256": material_sha,
            "material_contract": material,
            "source_binding": binding,
            "artifact_root": str(expected_cohort_root),
            "session_count": len(sessions),
            "instrument_count": len(instruments),
            "pair_aliases": aliases,
            "global_clock_count": len(expected_clocks),
            "market_repetition_count": len(expected_clocks),
            "pair_context_count": len(expected_contexts),
            "causal_ready_context_count": sum(
                bool(row["causal_ready"]) for row in expected_contexts
            ),
            "candidate_context_count": sum(
                bool(row["ranked_candidate"]) for row in expected_contexts
            ),
            "context_availability_counts": context_availability,
            "counterfactual_count": len(expected_counterfactuals),
            "execution_leg_count": execution_leg_count,
            "action_counts": action_counts,
            "failure_counts": failure_counts,
            "realized_pips": float(state_chain["realized_pips"]),
            "terminal_flat": state_chain["position"] is None,
            "independent_regime_count": None,
            "independent_regime_state": "unknown_structural_sessions_only",
            "pair_contexts_count_as_rep": 0,
            "counterfactuals_count_as_rep": 0,
            "replays_count_as_rep": 0,
            "datasets": specs,
            **SAFETY,
        }
        for key, expected in summary.items():
            if state.get(key) != expected:
                failures.append("state_summary_" + key)

        if len(datasets["feedback"]) != len(expected_clocks):
            failures.append("feedback_completeness")
        if len(datasets["terminals"]) != len(sessions):
            failures.append("terminal_completeness")
    except Exception as exc:
        failures.append("exception:" + type(exc).__name__ + ":" + str(exc))
        if not state and STATE.exists():
            state = read_json(STATE)
    result = {
        "schema_version": 1,
        "generated_utc": (
            artifact_generated_utc
            or str(state.get("generated_utc") or "1970-01-01T00:00:00+00:00")
        ),
        "cohort_id": state.get("cohort_id"),
        "verified": not failures,
        "failures": sorted(set(failures)),
        "reconstructed_global_clocks": (
            int(state.get("global_clock_count") or 0) if not failures else None
        ),
        "reconstructed_pair_contexts": (
            int(state.get("pair_context_count") or 0) if not failures else None
        ),
        "independent_regime_count": None,
    }
    receipt_path: Path | None = None
    if result["verified"] and state.get("artifact_root"):
        try:
            receipt_root = Path(str(state["artifact_root"])).resolve()
            expected_receipt_root = (
                ARTIFACT_ROOT / "cohorts" / str(state.get("cohort_id") or "")
            ).resolve()
            if (
                receipt_root == expected_receipt_root
                and ARTIFACT_ROOT.resolve() in receipt_root.parents
            ):
                receipt_path = receipt_root / "verifier_receipt.json"
                write_json(receipt_path, result, immutable=True)
        except (OSError, RuntimeError, ValueError):
            raise
    if receipt_path is not None:
        # The mutable current receipt is republished from the exact immutable
        # cohort bytes, never independently regenerated.
        atomic_bytes(OUTPUT, receipt_path.read_bytes())
    else:
        write_json(OUTPUT, result)
    return result


def main() -> int:
    result=verify(); print(json.dumps(result,sort_keys=True)); return 0 if result["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
