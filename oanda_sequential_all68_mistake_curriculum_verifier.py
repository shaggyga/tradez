#!/usr/bin/env python3
"""Standalone verifier for the immutable all-68 mistake curriculum.

The verifier deliberately imports neither the producer nor its research core.
It opens the exact frozen all-68 cohort, verifies every source row/root and
receipt binding, then independently rebuilds all curriculum rows, labels,
connected-resource clusters, summaries, and content identities.
"""

from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
from datetime import datetime, timezone
from hashlib import sha256
import gzip
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "sequential_all68_mistake_curriculum_v1.json"
DEFAULT_ROOT = ROOT / "data" / "oanda_training_manager" / "research_ledgers" / "sequential_all68_mistake_curriculum_v1"
DEFAULT_REPORT = DEFAULT_ROOT / "sequential_all68_mistake_curriculum_v1.json"
DEFAULT_MATERIAL = DEFAULT_ROOT / "material_contract_v1.json"
DEFAULT_OUTPUT = DEFAULT_ROOT / "sequential_all68_mistake_curriculum_verifier_v1.json"
PRODUCER = ROOT / "oanda_sequential_all68_mistake_curriculum.py"
CORE = ROOT / "src" / "forex_system" / "research" / "sequential_all68_mistake_curriculum_v1.py"
REPORT_CONTRACT_ID = "sequential_all68_mistake_curriculum_v1_20260829"

POLICY = {
    "research_only": True,
    "execution_eligible": False,
    "proof_eligible": False,
    "can_promote": False,
    "can_place_orders": False,
    "can_authorize": False,
    "broker_access": False,
    "account_access": False,
    "signal_feed_write": False,
    "lifecycle_write": False,
    "supported_decision": "no_trade",
}

CATEGORY_DEFINITIONS = {
    "cost": "An entry or rotation underperformed its appropriate flat/hold/exit baseline after executable bid/ask and slippage by the frozen threshold; the gross proxy is retained separately and is not treated as causal attribution.",
    "entry": "A primary entry underperformed its best available depth-one alternative by the frozen material-regret threshold.",
    "direction": "The executable flipped-side alternative on the same instrument outperformed the selected side by the frozen threshold.",
    "calibration": "The selected scored action had Brier loss above the neutral 0.25 baseline at the declared feedback clock.",
    "rotation": "A primary rotation underperformed holding or exiting the prior position by the frozen threshold.",
    "management_exit": "Holding underperformed exit/rotation, or exiting underperformed hold, by the frozen threshold.",
    "opportunity_selection_hold_vs_rotate": "A different instrument or the opposing hold-versus-rotate policy outperformed the primary action by the frozen threshold.",
}

SOURCE_DATASETS = {
    "clocks": "global_clocks.jsonl.gz",
    "decisions": "portfolio_decisions.jsonl.gz",
    "feedback": "feedback.jsonl.gz",
    "counterfactuals": "counterfactuals.jsonl.gz",
    "pair_contexts": "pair_contexts.jsonl.gz",
    "terminals": "session_terminals.jsonl.gz",
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def stable_hash(*parts: Any) -> str:
    return sha256(canonical_json(parts).encode("utf-8")).hexdigest()


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def semantic_sha256(value: Mapping[str, Any], *volatile_fields: str) -> str:
    payload = dict(value)
    for field in volatile_fields:
        payload.pop(field, None)
    return stable_hash(payload)


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    payload = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != payload:
            raise ValueError(f"immutable verifier receipt conflict: {path}")
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def _round(value: float | int | None) -> float | None:
    return None if value is None else round(float(value), 12)


def row_hash(row: Mapping[str, Any]) -> str:
    return stable_hash({key: value for key, value in row.items() if key != "row_sha256"})


def safe_relative(base: Path, value: str, *, must_exist: bool = False) -> Path:
    relative = Path(str(value))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("unsafe relative path")
    path = (base / relative).resolve(strict=must_exist)
    if base.resolve() not in path.parents and path != base.resolve():
        raise ValueError("path escapes base")
    return path


def forbidden_imports() -> list[str]:
    blocked: list[str] = []
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        else:
            continue
        for name in names:
            if "sequential_all68_mistake_curriculum" in name:
                blocked.append(name)
    return sorted(set(blocked))


def validate_config(config: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    for key, expected in {**POLICY, "evidence_role": "historical_training_curriculum"}.items():
        if config.get(key) != expected:
            failures.append("unsafe_config_" + key)
    if int(config.get("schema_version", 0)) != 1:
        failures.append("config_schema")
    source = config.get("source")
    if not isinstance(source, dict):
        return failures + ["source_contract"]
    for key in (
        "required_state_sha256", "required_verifier_sha256",
        "required_state_semantic_sha256", "required_verifier_semantic_sha256",
        "required_source_pack_manifest_sha256", "required_source_pack_verifier_sha256",
        "required_dataset_roots_sha256",
    ):
        if len(str(source.get(key) or "")) != 64:
            failures.append("source_hash_" + key)
    classification = config.get("classification")
    if not isinstance(classification, dict):
        return failures + ["classification_contract"]
    if float(classification.get("minimum_material_regret_pips", 0.0)) <= 0:
        failures.append("classification_threshold")
    if float(classification.get("neutral_brier_baseline", -1.0)) != 0.25:
        failures.append("classification_brier")
    expected = {
        "categories_are_nonexclusive": True,
        "primary_feedback_weight": 1,
        "depth_one_alternative_weight": 0,
        "review_weight": 0,
        "raw_rows_are_not_independent": True,
        "structural_clusters_are_not_independent_regimes": True,
        "depth_one_is_local_not_global_optimum": True,
    }
    for key, value in expected.items():
        if classification.get(key) != value:
            failures.append("classification_" + key)
    return failures


def load_source_dataset(source_root: Path, state: Mapping[str, Any], name: str) -> list[dict[str, Any]]:
    spec = state["datasets"][name]
    if spec.get("relative_path") != SOURCE_DATASETS[name]:
        raise ValueError("dataset path changed:" + name)
    path = safe_relative(source_root, str(spec["relative_path"]), must_exist=True)
    payload = path.read_bytes()
    if len(payload) != int(spec["gzip_bytes"]) or sha256(payload).hexdigest() != spec["gzip_sha256"]:
        raise ValueError("dataset gzip binding changed:" + name)
    rows: list[dict[str, Any]] = []
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if not isinstance(row, dict) or row.get("row_sha256") != row_hash(row):
                raise ValueError("dataset row hash changed:" + name)
            rows.append(row)
    hashes = [str(row["row_sha256"]) for row in rows]
    if len(rows) != int(spec["row_count"]):
        raise ValueError("dataset row count changed:" + name)
    if stable_hash(hashes) != spec["ordered_row_sha256"] or stable_hash(sorted(hashes)) != spec["row_set_sha256"]:
        raise ValueError("dataset root changed:" + name)
    return rows


def source_bundle(config: Mapping[str, Any]) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Path], dict[str, list[dict[str, Any]]]]:
    source = config["source"]
    source_root = safe_relative(ROOT, str(source["cohort_relative_root"]), must_exist=True)
    expected_parent = (ROOT / "data" / "oanda_training_manager" / "research_ledgers" / "sequential_all68_portfolio_batch_replay_v1" / "cohorts").resolve()
    if expected_parent not in source_root.parents:
        raise ValueError("source cohort path")
    paths = {
        "state": safe_relative(source_root, str(source["state_name"]), must_exist=True),
        "receipt": safe_relative(source_root, str(source["verifier_name"]), must_exist=True),
        "pack": safe_relative(source_root, str(source["source_pack_manifest_name"]), must_exist=True),
        "pack_receipt": safe_relative(source_root, str(source["source_pack_verifier_name"]), must_exist=True),
    }
    state, receipt = read_json(paths["state"]), read_json(paths["receipt"])
    pack, pack_receipt = read_json(paths["pack"]), read_json(paths["pack_receipt"])
    if file_sha256(paths["state"]) != source.get("required_state_sha256"):
        raise ValueError("exact source state bytes changed")
    if file_sha256(paths["receipt"]) != source.get("required_verifier_sha256"):
        raise ValueError("exact source verifier bytes changed")
    if semantic_sha256(state, "generated_utc") != source.get("required_state_semantic_sha256"):
        raise ValueError("exact source semantic state changed")
    if semantic_sha256(receipt, "generated_utc") != source.get("required_verifier_semantic_sha256"):
        raise ValueError("exact source semantic receipt changed")
    if file_sha256(paths["pack"]) != source.get("required_source_pack_manifest_sha256"):
        raise ValueError("exact source-pack manifest hash changed")
    if file_sha256(paths["pack_receipt"]) != source.get("required_source_pack_verifier_sha256"):
        raise ValueError("exact source-pack verifier hash changed")
    for key, expected in {
        "research_only": True, "execution_eligible": False, "proof_eligible": False,
        "can_promote": False, "can_place_orders": False, "can_authorize": False,
        "broker_access": False, "account_access": False, "supported_decision": "no_trade",
    }.items():
        if state.get(key) != expected:
            raise ValueError("unsafe source state:" + key)
    cohort = str(source["required_cohort_id"])
    pack_id = str(source["required_source_pack_id"])
    if state.get("cohort_id") != cohort or receipt.get("cohort_id") != cohort:
        raise ValueError("source cohort mismatch")
    if state.get("evidence_role") not in (None, "historical_training_discovery"):
        raise ValueError("source evidence role")
    if receipt.get("verified") is not True or receipt.get("failures") != []:
        raise ValueError("source verifier")
    if state.get("source_binding", {}).get("pack_id") != pack_id:
        raise ValueError("state source pack")
    if state.get("material_contract", {}).get("source_binding", {}).get("pack_id") != pack_id:
        raise ValueError("material source pack")
    if pack.get("pack_id") != pack_id or pack_receipt.get("pack_id") != pack_id:
        raise ValueError("pack identity")
    if pack_receipt.get("verified") is not True or pack_receipt.get("failures") != []:
        raise ValueError("pack verifier")
    if stable_hash(state.get("datasets")) != source.get("required_dataset_roots_sha256"):
        raise ValueError("source dataset root contract")
    datasets = {name: load_source_dataset(source_root, state, name) for name in SOURCE_DATASETS}
    for name, rows in datasets.items():
        if any(row.get("cohort_id") != cohort for row in rows):
            raise ValueError("cross-cohort row:" + name)
    clocks = {row["clock_id"]: row for row in datasets["clocks"]}
    decisions = {row["decision_id"]: row for row in datasets["decisions"]}
    if len(clocks) != len(datasets["clocks"]) or len(decisions) != len(datasets["decisions"]):
        raise ValueError("duplicate clock or decision")
    if len(datasets["feedback"]) != len(clocks) or len(decisions) != len(clocks):
        raise ValueError("primary feedback completeness")
    seen_feedback: set[str] = set()
    for row in datasets["feedback"]:
        if row["decision_id"] not in decisions or row["clock_id"] not in clocks or row["decision_id"] in seen_feedback:
            raise ValueError("feedback relation")
        seen_feedback.add(str(row["decision_id"]))
    for row in datasets["counterfactuals"]:
        if row["decision_id"] not in decisions or row["clock_id"] not in clocks or int(row.get("depth", -1)) != 1:
            raise ValueError("counterfactual relation/depth")
        if int(row.get("counts_as_market_repetition", -1)) != 0 or int(row.get("counts_as_regime_repetition", -1)) != 0:
            raise ValueError("counterfactual repetition weight")
    return source_root, state, receipt, pack, pack_receipt, paths, datasets


def action_exposure(action: Mapping[str, Any], state_before: Mapping[str, Any]) -> tuple[str | None, int | None, list[str]]:
    name = str(action.get("action") or "")
    position = state_before.get("position") or {}
    instrument, side = action.get("instrument"), action.get("side")
    if name in {"hold", "exit"} or not instrument or side not in {-1, 1}:
        instrument, side = position.get("instrument"), position.get("side")
    if not instrument or side not in {-1, 1} or "_" not in str(instrument):
        return None, None, ["CASH:0"]
    base, quote = str(instrument).upper().split("_", 1)
    resources = [f"{base}:{'+1' if int(side) > 0 else '-1'}", f"{quote}:{'-1' if int(side) > 0 else '+1'}"]
    return str(instrument), int(side), sorted(resources)


def cost_proxy(execution: Mapping[str, Any], feedback_status: str, liquidation_slippage: float) -> float:
    value = sum(abs(float(leg.get("spread_pips") or 0.0)) + abs(float(leg.get("slippage_pips") or 0.0)) for leg in execution.get("legs") or [])
    if (execution.get("state_after") or {}).get("position") is not None and str(feedback_status).startswith("available"):
        value += abs(float(liquidation_slippage))
    return value


def review_payload(row: Mapping[str, Any], state_before: Mapping[str, Any], slippage: float) -> dict[str, Any]:
    branch = dict(row["branch"])
    instrument, side, resources = action_exposure(branch, state_before)
    return {
        "counterfactual_id": str(row["counterfactual_id"]),
        "branch_label": str(branch.get("branch_label") or ""),
        "action": str(branch.get("action") or ""),
        "instrument": instrument, "side": side,
        "signed_currency_resources": resources,
        "feedback_status": str(row.get("feedback_status") or ""),
        "terminal_equity_pips": _round(row.get("liquidation_equity_pips")),
        "execution_cost_proxy_pips": _round(cost_proxy(row.get("execution") or {}, str(row.get("feedback_status") or ""), slippage)),
        "source_counterfactual_row_sha256": str(row["row_sha256"]),
    }


def best(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    available = [row for row in rows if row.get("terminal_equity_pips") is not None and str(row.get("feedback_status") or "").startswith("available")]
    return sorted(available, key=lambda row: (-float(row["terminal_equity_pips"]), str(row["branch_label"]), str(row["counterfactual_id"])))[0] if available else None


def classify(decision_row: Mapping[str, Any], feedback: Mapping[str, Any], reviews: Sequence[Mapping[str, Any]], threshold: float, neutral: float, slippage: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    decision, state_before = dict(decision_row["decision"]), dict(decision_row["state_before"])
    action = str(decision.get("action") or "")
    primary_raw = feedback.get("liquidation_equity_pips")
    primary = float(primary_raw) if primary_raw is not None else None
    chosen = best(reviews)
    regret = max(0.0, float(chosen["terminal_equity_pips"]) - primary) if chosen is not None and primary is not None else 0.0
    cutoff = threshold - 1e-12
    labels: list[dict[str, Any]] = []
    def add(category: str, reason: str, comparator: Mapping[str, Any] | None, materiality: float, **extra: Any) -> None:
        row = {"category": category, "reason_code": reason, "comparator_counterfactual_id": comparator.get("counterfactual_id") if comparator else None, "materiality_pips": _round(max(0.0, materiality))}
        row.update(extra); labels.append(row)
    if primary is not None and chosen is not None and regret >= cutoff:
        if action == "enter": add("entry", "entry_underperformed_best_depth_one", chosen, regret)
        instrument, side = decision.get("instrument"), decision.get("side")
        flipped = best([row for row in reviews if instrument and side in {-1, 1} and row.get("instrument") == instrument and row.get("side") == -int(side)])
        if action in {"enter", "rotate"} and flipped is not None:
            value = float(flipped["terminal_equity_pips"]) - primary
            if value >= cutoff: add("direction", "flipped_same_instrument_outperformed", flipped, value)
        if action == "rotate":
            comparator = best([row for row in reviews if row.get("action") in {"hold", "exit"}])
            if comparator is not None:
                value = float(comparator["terminal_equity_pips"]) - primary
                if value >= cutoff: add("rotation", "rotation_underperformed_hold_or_exit", comparator, value)
        if action == "hold":
            comparator = best([row for row in reviews if row.get("action") in {"exit", "rotate"}])
            if comparator is not None:
                value = float(comparator["terminal_equity_pips"]) - primary
                if value >= cutoff: add("management_exit", "hold_underperformed_exit_or_rotate", comparator, value)
        elif action == "exit":
            comparator = best([row for row in reviews if row.get("action") == "hold"])
            if comparator is not None:
                value = float(comparator["terminal_equity_pips"]) - primary
                if value >= cutoff: add("management_exit", "exit_underperformed_hold", comparator, value)
        current = (state_before.get("position") or {}).get("instrument")
        selected = decision.get("instrument") or current
        opportunity = best([row for row in reviews if (row.get("action") in {"enter", "rotate"} and row.get("instrument") and row.get("instrument") != selected) or (action == "hold" and row.get("action") == "rotate") or (action == "rotate" and row.get("action") == "hold")])
        if opportunity is not None:
            value = float(opportunity["terminal_equity_pips"]) - primary
            if value >= cutoff: add("opportunity_selection_hold_vs_rotate", "alternative_instrument_or_hold_rotate_policy_outperformed", opportunity, value)
    baseline = best([row for row in reviews if row.get("action") in ({"wait"} if state_before.get("position") is None else {"hold", "exit"})])
    confidence, brier = decision.get("confidence"), None
    if action in {"enter", "rotate", "hold"} and confidence is not None and primary is not None and baseline is not None:
        target = 1.0 if primary > float(baseline["terminal_equity_pips"]) else 0.0
        brier = (float(confidence) - target) ** 2
        if brier > neutral + 1e-12:
            add("calibration", "brier_worse_than_neutral", baseline, regret, brier=_round(brier), excess_brier=_round(brier-neutral), binary_target=int(target))
    primary_cost = cost_proxy(decision_row.get("execution") or {}, str(feedback.get("status") or ""), slippage)
    after_cost, gross = None, None
    if action in {"enter", "rotate"} and primary is not None and baseline is not None:
        after_cost = primary - float(baseline["terminal_equity_pips"])
        gross = after_cost + primary_cost - float(baseline.get("execution_cost_proxy_pips") or 0.0)
        if after_cost <= -cutoff:
            add("cost", "failed_after_cost_clearance_vs_baseline", baseline, -after_cost, after_cost_relative_pips=_round(after_cost), gross_relative_proxy_pips=_round(gross), gross_proxy_positive=bool(gross > 0.0))
    labels.sort(key=lambda row: (str(row["category"]), str(row["reason_code"]), str(row.get("comparator_counterfactual_id") or "")))
    diagnostics = {
        "best_alternative_counterfactual_id": chosen.get("counterfactual_id") if chosen else None,
        "best_alternative_equity_pips": chosen.get("terminal_equity_pips") if chosen else None,
        "regret_pips": _round(regret),
        "baseline_counterfactual_id": baseline.get("counterfactual_id") if baseline else None,
        "after_cost_relative_pips": _round(after_cost), "gross_relative_proxy_pips": _round(gross),
        "execution_cost_proxy_pips": _round(primary_cost), "brier": _round(brier),
    }
    return labels, diagnostics


def build_rows(datasets: Mapping[str, Sequence[Mapping[str, Any]]], classification: Mapping[str, Any], slippage: float) -> list[dict[str, Any]]:
    clocks = {str(row["clock_id"]): row for row in datasets["clocks"]}
    feedback = {str(row["decision_id"]): row for row in datasets["feedback"]}
    alternatives: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in datasets["counterfactuals"]: alternatives[str(row["decision_id"])].append(row)
    output: list[dict[str, Any]] = []
    for decision_row in sorted(datasets["decisions"], key=lambda row: (int(row["sequence_no"]), str(row["decision_id"]))):
        decision_id = str(decision_row["decision_id"]); clock = clocks[str(decision_row["clock_id"])]; feedback_row = feedback[decision_id]
        state_before = dict(decision_row["state_before"])
        reviews = [review_payload(row, state_before, slippage) for row in sorted(alternatives.get(decision_id, []), key=lambda value: str(value["counterfactual_id"]))]
        labels, diagnostics = classify(decision_row, feedback_row, reviews, float(classification["minimum_material_regret_pips"]), float(classification["neutral_brier_baseline"]), slippage)
        action = dict(decision_row["decision"]); instrument, side, resources = action_exposure(action, state_before)
        identity = {"role":"primary_feedback", "decision_id":decision_id, "feedback_id":feedback_row["feedback_id"]}
        primary = {
            "curriculum_row_id":"a68mistake_"+stable_hash(identity)[:28], "row_role":"primary_feedback",
            "curriculum_weight":int(classification["primary_feedback_weight"]), "counts_as_market_repetition":1, "counts_as_regime_repetition":0,
            "source_cohort_id":str(decision_row["cohort_id"]), "session_episode_id":str(decision_row["session_key"]),
            "clock_id":str(decision_row["clock_id"]), "sequence_no":int(decision_row["sequence_no"]), "decision_epoch":int(decision_row["decision_epoch"]),
            "decision_id":decision_id, "feedback_id":str(feedback_row["feedback_id"]), "counterfactual_id":None,
            "action":str(action.get("action") or ""), "instrument":instrument, "side":side, "signed_currency_resources":resources,
            "feedback_status":str(feedback_row.get("status") or ""), "terminal_equity_pips":_round(feedback_row.get("liquidation_equity_pips")),
            "mistake_labels":labels, "diagnostics":diagnostics,
            "source_clock_row_sha256":str(clock["row_sha256"]), "source_decision_row_sha256":str(decision_row["row_sha256"]),
            "source_feedback_row_sha256":str(feedback_row["row_sha256"]), "source_counterfactual_row_sha256":None,
            "proof_eligible":False, "execution_eligible":False,
        }
        primary["row_sha256"] = row_hash(primary); output.append(primary)
        for review in reviews:
            identity = {"role":"depth_one_review", "decision_id":decision_id, "counterfactual_id":review["counterfactual_id"]}
            row = {
                "curriculum_row_id":"a68mistake_"+stable_hash(identity)[:28], "row_role":"depth_one_review",
                "curriculum_weight":int(classification["depth_one_alternative_weight"]), "counts_as_market_repetition":0, "counts_as_regime_repetition":0,
                "source_cohort_id":str(decision_row["cohort_id"]), "session_episode_id":str(decision_row["session_key"]),
                "clock_id":str(decision_row["clock_id"]), "sequence_no":int(decision_row["sequence_no"]), "decision_epoch":int(decision_row["decision_epoch"]),
                "decision_id":decision_id, "feedback_id":str(feedback_row["feedback_id"]), "counterfactual_id":review["counterfactual_id"],
                "action":review["action"], "instrument":review["instrument"], "side":review["side"], "signed_currency_resources":review["signed_currency_resources"],
                "feedback_status":review["feedback_status"], "terminal_equity_pips":review["terminal_equity_pips"], "mistake_labels":[],
                "diagnostics":{"branch_label":review["branch_label"], "execution_cost_proxy_pips":review["execution_cost_proxy_pips"], "review_only":True},
                "source_clock_row_sha256":str(clock["row_sha256"]), "source_decision_row_sha256":str(decision_row["row_sha256"]),
                "source_feedback_row_sha256":str(feedback_row["row_sha256"]), "source_counterfactual_row_sha256":review["source_counterfactual_row_sha256"],
                "proof_eligible":False, "execution_eligible":False,
            }
            row["row_sha256"] = row_hash(row); output.append(row)
    return sorted(output, key=lambda row: (int(row["sequence_no"]), 0 if row["row_role"]=="primary_feedback" else 1, str(row["curriculum_row_id"])))


class DisjointSet:
    def __init__(self, size: int) -> None: self.parent=list(range(size))
    def find(self, value: int) -> int:
        while self.parent[value]!=value:
            self.parent[value]=self.parent[self.parent[value]]; value=self.parent[value]
        return value
    def union(self,left:int,right:int)->None:
        a,b=self.find(left),self.find(right)
        if a!=b:self.parent[max(a,b)]=min(a,b)


def build_clusters(rows: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    output=[dict(row) for row in rows]; grouped:dict[tuple[str,str],list[int]]=defaultdict(list)
    for index,row in enumerate(output):grouped[(str(row["session_episode_id"]),str(row["clock_id"]))].append(index)
    clusters:list[dict[str,Any]]=[]; mapping:dict[str,str]={}
    for (episode,clock_id),indices in sorted(grouped.items()):
        dsu=DisjointSet(len(indices)); currencies=[{str(x).split(":",1)[0] for x in output[i]["signed_currency_resources"] if not str(x).startswith("CASH:")} for i in indices]
        for left in range(len(indices)):
            for right in range(left+1,len(indices)):
                if currencies[left] and currencies[left]&currencies[right]:dsu.union(left,right)
        components:dict[int,list[int]]=defaultdict(list)
        for local,global_index in enumerate(indices):components[dsu.find(local)].append(global_index)
        for component in components.values():
            members=sorted((output[i] for i in component),key=lambda row:str(row["curriculum_row_id"])); ids=[str(row["curriculum_row_id"]) for row in members]
            signed=sorted({str(x) for row in members for x in row["signed_currency_resources"]}); labels=sorted({str(label["category"]) for row in members if row["row_role"]=="primary_feedback" for label in row["mistake_labels"]})
            cluster_id="a68mistakecluster_"+stable_hash(episode,clock_id,signed,ids)[:28]
            for row in members:mapping[str(row["curriculum_row_id"])]=cluster_id
            cluster={
                "cluster_id":cluster_id,"session_episode_id":episode,"clock_id":clock_id,"sequence_no":min(int(row["sequence_no"]) for row in members),
                "currency_resources":sorted({x.split(":",1)[0] for x in signed if not x.startswith("CASH:")}),"signed_currency_resources":signed,"curriculum_row_ids":ids,
                "row_count":len(members),"primary_feedback_count":sum(row["row_role"]=="primary_feedback" for row in members),"zero_weight_review_count":sum(row["row_role"]=="depth_one_review" for row in members),
                "deduplicated_primary_weight":min(1,sum(int(row["curriculum_weight"]) for row in members)),"mistake_categories":labels,
                "counts_as_market_repetition":min(1,sum(int(row["curriculum_weight"]) for row in members)),"counts_as_regime_repetition":0,"proof_eligible":False,
            }
            cluster["row_sha256"]=row_hash(cluster);clusters.append(cluster)
    for row in output:row["structural_cluster_id"]=mapping[str(row["curriculum_row_id"])];row["row_sha256"]=row_hash(row)
    output.sort(key=lambda row:(int(row["sequence_no"]),0 if row["row_role"]=="primary_feedback" else 1,str(row["curriculum_row_id"])))
    clusters.sort(key=lambda row:(int(row["sequence_no"]),str(row["cluster_id"])))
    return output,clusters


def build_mistake_clusters(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    observations:list[dict[str,Any]]=[]
    for row in rows:
        if row["row_role"]!="primary_feedback":continue
        for label in row["mistake_labels"]:
            observations.append({"observation_id":"a68mistakeobs_"+stable_hash(row["curriculum_row_id"],label["category"],label["reason_code"])[:28],"category":str(label["category"]),"reason_code":str(label["reason_code"]),"session_episode_id":str(row["session_episode_id"]),"clock_id":str(row["clock_id"]),"sequence_no":int(row["sequence_no"]),"curriculum_row_id":str(row["curriculum_row_id"]),"instrument":row.get("instrument"),"signed_currency_resources":list(row["signed_currency_resources"]),"materiality_pips":_round(label["materiality_pips"])})
    dsu=DisjointSet(len(observations));currencies=[{str(x).split(":",1)[0] for x in row["signed_currency_resources"] if not str(x).startswith("CASH:")} for row in observations]
    for left in range(len(observations)):
        for right in range(left+1,len(observations)):
            a,b=observations[left],observations[right]
            if a["category"]==b["category"] and a["session_episode_id"]==b["session_episode_id"] and currencies[left] and currencies[left]&currencies[right]:dsu.union(left,right)
    groups:dict[int,list[dict[str,Any]]]=defaultdict(list)
    for index,row in enumerate(observations):groups[dsu.find(index)].append(row)
    output:list[dict[str,Any]]=[]
    for group in groups.values():
        group.sort(key=lambda row:(int(row["sequence_no"]),str(row["observation_id"])));category=str(group[0]["category"]);episode=str(group[0]["session_episode_id"]);ids=sorted(str(row["observation_id"]) for row in group);signed=sorted({str(x) for row in group for x in row["signed_currency_resources"]});values=[float(row["materiality_pips"]) for row in group];representative=sorted(group,key=lambda row:(-float(row["materiality_pips"]),int(row["sequence_no"]),str(row["observation_id"])))[0]
        cluster={"cluster_id":"a68mistakestruct_"+stable_hash(category,episode,ids)[:28],"category":category,"session_episode_id":episode,"observation_ids":ids,"observation_count":len(group),"global_clock_ids":sorted({str(row["clock_id"]) for row in group}),"deduplicated_global_clock_count":len({str(row["clock_id"]) for row in group}),"curriculum_row_ids":sorted({str(row["curriculum_row_id"]) for row in group}),"currency_resources":sorted({x.split(":",1)[0] for x in signed if not x.startswith("CASH:")}),"signed_currency_resources":signed,"instruments":sorted({str(row["instrument"]) for row in group if row.get("instrument")}),"reason_counts":dict(sorted(Counter(str(row["reason_code"]) for row in group).items())),"raw_materiality_pips":_round(sum(values)),"cluster_max_materiality_pips":_round(max(values)),"representative_observation_id":representative["observation_id"],"counts_as_regime_repetition":0,"proof_eligible":False}
        cluster["row_sha256"]=row_hash(cluster);output.append(cluster)
    return sorted(output,key=lambda row:(str(row["category"]),str(row["session_episode_id"]),str(row["cluster_id"])))


def build_expected_report(source_state: Mapping[str, Any], source_binding: Mapping[str, Any], material: Mapping[str, Any], cohort_id: str, classification: Mapping[str, Any], datasets: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    slippage=float(source_state["material_contract"]["costs"]["slippage_per_execution_leg_pips"])
    rows,feedback_components=build_clusters(build_rows(datasets,classification,slippage));clusters=build_mistake_clusters(rows);primary=[row for row in rows if row["row_role"]=="primary_feedback"];reviews=[row for row in rows if row["row_role"]=="depth_one_review"]
    category_counts=Counter(str(label["category"]) for row in primary for label in row["mistake_labels"]);reason_counts=Counter(str(label["reason_code"]) for row in primary for label in row["mistake_labels"]);materiality:dict[str,float]=defaultdict(float)
    for row in primary:
        for label in row["mistake_labels"]:materiality[str(label["category"])]+=float(label["materiality_pips"])
    categories={}
    for name,definition in CATEGORY_DEFINITIONS.items():
        category_clusters=[row for row in clusters if row["category"]==name]
        categories[name]={"definition":definition,"label_count":int(category_counts.get(name,0)),"unique_primary_clock_count":len({row["clock_id"] for row in primary if any(label["category"]==name for label in row["mistake_labels"])}),"materiality_pips":_round(materiality.get(name,0.0)),"structural_cluster_count":len(category_clusters),"cluster_max_materiality_pips":_round(sum(float(row["cluster_max_materiality_pips"]) for row in category_clusters))}
    priorities=sorted([{"category":name,"label_count":details["label_count"],"structural_cluster_count":details["structural_cluster_count"],"materiality_pips":details["materiality_pips"],"cluster_max_materiality_pips":details["cluster_max_materiality_pips"]} for name,details in categories.items() if details["label_count"]],key=lambda row:(-float(row["cluster_max_materiality_pips"]),-int(row["structural_cluster_count"]),-float(row["materiality_pips"]),str(row["category"])))
    report={
        "schema_version":1,"contract_id":REPORT_CONTRACT_ID,"cohort_id":cohort_id,"generated_utc":datetime.fromtimestamp(max(int(row["feedback_epoch"]) for row in datasets["clocks"]),tz=timezone.utc).isoformat(),
        "material_contract_sha256":stable_hash(material),"material_contract":dict(material),**POLICY,"evidence_role":"historical_training_curriculum",
        "source_binding":dict(source_binding),"classification_contract":dict(classification),
        "summary":{"source_global_clock_count":len(datasets["clocks"]),"source_primary_feedback_count":len(primary),"source_depth_one_review_count":len(reviews),"curriculum_row_count":len(rows),"primary_weight_sum":sum(int(row["curriculum_weight"]) for row in primary),"review_weight_sum":sum(int(row["curriculum_weight"]) for row in reviews),"primary_clocks_with_any_mistake":sum(bool(row["mistake_labels"]) for row in primary),"nonexclusive_mistake_label_count":sum(len(row["mistake_labels"]) for row in primary),"feedback_resource_component_count":len(feedback_components),"structural_cluster_count":len(clusters),"deduplicated_primary_weight":sum(int(row["deduplicated_primary_weight"]) for row in feedback_components),"predeclared_session_episode_count":len({row["session_episode_id"] for row in primary}),"independent_regime_count":None},
        "category_definitions":dict(CATEGORY_DEFINITIONS),"categories":categories,"reason_counts":dict(sorted(reason_counts.items())),"curriculum_priorities":priorities,
        "curriculum_rows":rows,"feedback_resource_components":feedback_components,"structural_clusters":clusters,
        "limitations":["This is already-inspected historical training evidence and cannot prove, promote, authorize, or execute a policy.","Primary feedback receives one unit of curriculum weight per global clock; every depth-one alternative/review receives zero.","Connected-currency components collapse opposing signed exposures that share an underlying currency, preventing direction-conflicted factor inflation.","Category labels intentionally overlap and cannot be summed as independent errors.","Depth-one alternatives are local reviews, not a globally optimal portfolio path.","The three predeclared session episodes are structural slices, not three proven independent regimes."],
    }
    report["report_sha256"]=stable_hash(report);report["report_id"]="a68mistakecurriculum_"+report["report_sha256"][:28]
    return report


def expected_material(config: Mapping[str, Any], config_path: Path, state: Mapping[str, Any], paths: Mapping[str, Path]) -> dict[str, Any]:
    return {
        "schema_version":1,"contract_id":REPORT_CONTRACT_ID,"config_sha256":file_sha256(config_path),"config_semantic_sha256":stable_hash(config),
        "producer_code_sha256":file_sha256(PRODUCER),"core_code_sha256":file_sha256(CORE),"verifier_code_sha256":file_sha256(Path(__file__).resolve()),
        "source_cohort_id":state["cohort_id"],"source_state_sha256":file_sha256(paths["state"]),"source_verifier_sha256":file_sha256(paths["receipt"]),"source_state_semantic_sha256":semantic_sha256(state,"generated_utc"),"source_verifier_semantic_sha256":semantic_sha256(read_json(paths["receipt"]),"generated_utc"),
        "source_pack_id":config["source"]["required_source_pack_id"],"source_pack_manifest_sha256":file_sha256(paths["pack"]),"source_pack_verifier_receipt_sha256":file_sha256(paths["pack_receipt"]),
        "source_dataset_roots_sha256":stable_hash(state["datasets"]),"source_dataset_specs":state["datasets"],"classification":config["classification"],
        "safety":{**POLICY,"evidence_role":"historical_training_curriculum"},
    }


def verify(config_path: Path=DEFAULT_CONFIG, report_path: Path=DEFAULT_REPORT, material_path: Path=DEFAULT_MATERIAL, output_path: Path=DEFAULT_OUTPUT) -> dict[str, Any]:
    failures:list[str]=[];blocked=forbidden_imports()
    if blocked:failures.append("verifier_import_isolation")
    config=read_json(config_path);report=read_json(report_path);material_file=read_json(material_path);failures.extend(validate_config(config))
    state:dict[str,Any]={};expected:dict[str,Any]={};material:dict[str,Any]={};source_binding:dict[str,Any]={}
    try:
        _,state,_,_,_,paths,datasets=source_bundle(config)
        material=expected_material(config,config_path,state,paths);material_sha=stable_hash(material);cohort_id=str(config["experiment_key"])+"."+material_sha[:20]
        source_binding={"cohort_id":state["cohort_id"],"source_pack_id":config["source"]["required_source_pack_id"],"source_state_sha256":material["source_state_sha256"],"source_verifier_sha256":material["source_verifier_sha256"],"source_state_semantic_sha256":material["source_state_semantic_sha256"],"source_verifier_semantic_sha256":material["source_verifier_semantic_sha256"],"source_pack_manifest_sha256":material["source_pack_manifest_sha256"],"source_pack_verifier_receipt_sha256":material["source_pack_verifier_receipt_sha256"],"source_material_sha256":state["material_sha256"],"source_dataset_roots_sha256":material["source_dataset_roots_sha256"],"source_dataset_specs":state["datasets"]}
        expected=build_expected_report(state,source_binding,material,cohort_id,config["classification"],datasets)
        if material_file!=material:failures.append("material_file")
        if report!=expected:failures.append("full_report_reconstruction")
        for key,expected_value in {**POLICY,"evidence_role":"historical_training_curriculum"}.items():
            if report.get(key)!=expected_value:failures.append("unsafe_report_"+key)
        if report.get("cohort_id")!=cohort_id:failures.append("cohort_id")
        if report.get("material_contract")!=material or report.get("material_contract_sha256")!=material_sha:failures.append("material_contract")
        if report.get("source_binding")!=source_binding:failures.append("source_binding")
        semantic=dict(report);supplied_id=semantic.pop("report_id",None);supplied_hash=semantic.pop("report_sha256",None);computed=stable_hash(semantic)
        if supplied_hash!=computed:failures.append("report_content_hash")
        if supplied_id!="a68mistakecurriculum_"+computed[:28]:failures.append("report_id")
    except Exception as exc:
        failures.append("exception:"+type(exc).__name__+":"+str(exc))
    result={
        "schema_version":1,"generated_utc":str(state.get("generated_utc") or "source_unavailable"),"verified":not failures,"failures":sorted(set(failures)),
        "cohort_id":report.get("cohort_id"),"report_id":report.get("report_id"),"report_file_sha256":file_sha256(report_path),"report_content_sha256":report.get("report_sha256"),
        "material_contract_sha256":stable_hash(material) if material else None,**POLICY,"evidence_role":"historical_training_curriculum",
        "checks":{"verifier_forbidden_imports":blocked,"source_cohort_id":state.get("cohort_id"),"source_pack_id":source_binding.get("source_pack_id"),"source_state_sha256":source_binding.get("source_state_sha256"),"source_verifier_sha256":source_binding.get("source_verifier_sha256"),"source_state_semantic_sha256":source_binding.get("source_state_semantic_sha256"),"source_verifier_semantic_sha256":source_binding.get("source_verifier_semantic_sha256"),"source_dataset_roots_sha256":source_binding.get("source_dataset_roots_sha256"),"curriculum_row_count":(expected.get("summary") or {}).get("curriculum_row_count"),"structural_cluster_count":(expected.get("summary") or {}).get("structural_cluster_count")},
    }
    atomic_json(output_path,result);return result


def main()->int:
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--config",type=Path,default=DEFAULT_CONFIG);parser.add_argument("--report",type=Path,default=DEFAULT_REPORT);parser.add_argument("--material",type=Path,default=DEFAULT_MATERIAL);parser.add_argument("--output",type=Path,default=DEFAULT_OUTPUT);args=parser.parse_args()
    result=verify(args.config.resolve(strict=True),args.report.resolve(strict=True),args.material.resolve(strict=True),args.output.resolve());print(json.dumps(result,indent=2,sort_keys=True));return 0 if result["verified"] else 1


if __name__=="__main__":raise SystemExit(main())
