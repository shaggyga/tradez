#!/usr/bin/env python3
"""Standalone verifier for the sealed sequential mistake curriculum.

This file intentionally does not import either the producer or its research
core.  It rebuilds source bindings, observations, clusters, summaries and the
content identity directly from the frozen replay database.
"""

from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "sequential_portfolio_mistake_curriculum_v1.json"
DEFAULT_REPORT = ROOT / "data" / "oanda_training_manager" / "research_ledgers" / "sequential_portfolio_mistake_curriculum_v1" / "sequential_portfolio_mistake_curriculum_v1.json"
DEFAULT_OUTPUT = ROOT / "data" / "oanda_training_manager" / "research_ledgers" / "sequential_portfolio_mistake_curriculum_v1" / "sequential_portfolio_mistake_curriculum_verifier_v1.json"
REPORT_CONTRACT_ID = "sequential_portfolio_mistake_curriculum_v1_20260829_hardened"
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
    "direction": "An executable opposite-side branch on the same instrument beat the primary trade by the material-regret threshold.",
    "entry": "A primary entry underperformed its best depth-one alternative by the material-regret threshold.",
    "management": "Holding an existing position underperformed its best depth-one alternative by the material-regret threshold.",
    "exit": "Exiting an existing position underperformed holding by the material-regret threshold.",
    "rotation": "A primary rotation underperformed holding or exiting by the material-regret threshold.",
    "cost_awareness": "An entry or rotation failed to clear its executable bid/ask and slippage economics at the declared feedback horizon.",
    "calibration": "A scored entry or rotation had Brier loss strictly worse than the neutral-probability baseline of 0.25.",
    "opportunity_selection": "A different-instrument depth-one alternative beat the selected instrument by the material-regret threshold.",
}
ROOT_TABLES = {
    "pair_contexts": "spr_pair_contexts",
    "clocks": "spr_clocks",
    "decisions": "spr_decisions",
    "execution_outcomes": "spr_execution_outcomes",
    "execution_legs": "spr_execution_legs",
    "counterfactuals": "spr_counterfactuals",
    "feedback": "spr_feedback",
    "administrative_events": "spr_administrative_events",
}
REQUIRED_TABLES = set(ROOT_TABLES.values()) | {"spr_sessions", "spr_session_seals"}


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


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    text = json.dumps(value, indent=2, sort_keys=True) + "\n"
    encoded = text.encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != encoded:
            raise ValueError(f"immutable verifier receipt conflict: {path}")
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(encoded)
    temporary.replace(path)


def _round(value: float | int | None) -> float | None:
    return None if value is None else round(float(value), 12)


def _loads(value: str, expected: type) -> Any:
    parsed = json.loads(value)
    if not isinstance(parsed, expected):
        raise ValueError(f"expected {expected.__name__} JSON")
    return parsed


def resolve_relative(path_text: str) -> Path:
    relative = Path(path_text)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("unsafe source path")
    resolved = (ROOT / relative).resolve(strict=True)
    if ROOT.resolve() not in resolved.parents:
        raise ValueError("source path escapes project root")
    return resolved


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
            if "sequential_portfolio_mistake_curriculum" in name or "sequential_portfolio_replay" in name:
                blocked.append(name)
    return sorted(set(blocked))


def validate_config(config: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    for key, expected in {**POLICY, "evidence_role": "historical_training_curriculum"}.items():
        if config.get(key) != expected:
            failures.append("unsafe_config_" + key)
    classification = config.get("classification")
    if not isinstance(classification, dict):
        return failures + ["classification_contract"]
    if float(classification.get("minimum_material_regret_pips", 0.0)) <= 0:
        failures.append("classification_threshold")
    if float(classification.get("neutral_brier_baseline", -1.0)) != 0.25:
        failures.append("classification_brier_baseline")
    for key in ("categories_are_nonexclusive", "raw_observations_are_not_independent", "structural_clusters_are_not_independent_regimes"):
        if classification.get(key) is not True:
            failures.append("classification_contract_" + key)
    return failures


def table_root(connection: sqlite3.Connection, table: str, session_id: str) -> dict[str, Any]:
    rows = sorted(str(row[0]) for row in connection.execute(f"SELECT row_sha256 FROM {table} WHERE session_id=?", (session_id,)))
    return {"count": len(rows), "set_sha256": stable_hash(rows)}


def source_snapshot(database: Path) -> sqlite3.Connection:
    source = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True, timeout=60.0)
    memory = sqlite3.connect(":memory:")
    source.backup(memory)
    source.close()
    memory.row_factory = sqlite3.Row
    memory.execute("PRAGMA foreign_keys=ON")
    return memory


def verify_source(
    config: Mapping[str, Any], state: Mapping[str, Any], receipt: Mapping[str, Any], connection: sqlite3.Connection
) -> tuple[list[str], dict[str, Any]]:
    failures: list[str] = []
    source = config["source"]
    for key, expected in {
        "research_only": True,
        "execution_eligible": False,
        "can_promote": False,
        "can_place_orders": False,
        "can_authorize": False,
        "supported_decision": "no_trade",
    }.items():
        if state.get(key) != expected:
            failures.append("unsafe_source_state_" + key)
        if key in receipt and receipt.get(key) != expected:
            failures.append("unsafe_source_receipt_" + key)
    if state.get("proof_eligible") is not False or state.get("evidence_role") != "historical_training_discovery":
        failures.append("unsafe_source_evidence")
    if state.get("cohort_id") != source.get("required_cohort_id") or receipt.get("cohort_id") != state.get("cohort_id"):
        failures.append("source_cohort")
    if state.get("session_id") != source.get("required_session_id") or receipt.get("session_id") != state.get("session_id"):
        failures.append("source_session")
    if receipt.get("verified") is not True or receipt.get("failures") != []:
        failures.append("source_verifier")
    tables = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not REQUIRED_TABLES.issubset(tables):
        failures.append("source_tables")
        return failures, {}
    if str(connection.execute("PRAGMA integrity_check").fetchone()[0]).lower() != "ok":
        failures.append("source_sqlite_integrity")
    if list(connection.execute("PRAGMA foreign_key_check")):
        failures.append("source_foreign_keys")
    session_id = str(state.get("session_id") or "")
    roots = {name: table_root(connection, table, session_id) for name, table in ROOT_TABLES.items()}
    if roots != state.get("roots"):
        failures.append("source_state_roots")
    if roots != (receipt.get("checks") or {}).get("roots"):
        failures.append("source_receipt_roots")
    session = connection.execute("SELECT * FROM spr_sessions WHERE session_id=?", (session_id,)).fetchone()
    if session is None or session["cohort_id"] != state.get("cohort_id") or session["evidence_role"] != "historical_training_discovery" or int(session["proof_eligible"]) != 0:
        failures.append("source_session_row")
    seal = connection.execute("SELECT * FROM spr_session_seals WHERE seal_id=? AND session_id=?", (state.get("session_seal_id"), session_id)).fetchone()
    seal_payload: dict[str, Any] = {}
    if seal is None:
        failures.append("source_seal_missing")
    else:
        seal_payload = _loads(str(seal["seal_json"]), dict)
        if stable_hash(seal_payload) != seal["seal_sha256"]:
            failures.append("source_seal_hash")
        if seal["seal_id"] != "sprseal_" + str(seal["seal_sha256"])[:28]:
            failures.append("source_seal_id")
        if seal_payload.get("roots") != roots:
            failures.append("source_seal_roots")
        if seal_payload.get("cohort_id") != state.get("cohort_id") or seal_payload.get("session_id") != session_id:
            failures.append("source_seal_identity")

    # Independently validate the fields used by this curriculum rather than
    # trusting their stored row hashes alone.
    for row in connection.execute("SELECT * FROM spr_clocks WHERE session_id=?", (session_id,)):
        causal = _loads(row["causal_json"], dict)
        if stable_hash(causal) != row["causal_sha256"]:
            failures.append("source_clock_causal_hash")
    for row in connection.execute("SELECT * FROM spr_decisions WHERE session_id=?", (session_id,)):
        if stable_hash(_loads(row["decision_json"], dict)) != row["decision_sha256"]:
            failures.append("source_decision_payload_hash")
        if stable_hash(_loads(row["state_before_json"], dict)) != row["state_before_sha256"]:
            failures.append("source_decision_state_hash")
    for row in connection.execute("SELECT * FROM spr_feedback WHERE session_id=?", (session_id,)):
        if stable_hash(_loads(row["components_json"], dict)) != row["components_sha256"]:
            failures.append("source_feedback_payload_hash")
    for row in connection.execute("SELECT * FROM spr_counterfactuals WHERE session_id=?", (session_id,)):
        if stable_hash(_loads(row["action_json"], dict)) != row["action_sha256"]:
            failures.append("source_counterfactual_action_hash")
        if stable_hash(_loads(row["result_json"], dict)) != row["result_sha256"]:
            failures.append("source_counterfactual_result_hash")
    return failures, {
        "roots": roots,
        "seal_sha256": str(seal["seal_sha256"]) if seal else None,
        "seal_created_utc": str(seal["created_utc"]) if seal else None,
        "seal_payload": seal_payload,
    }


def _resources(instrument: str | None, values: Sequence[str]) -> list[str]:
    resources = {str(value).upper() for value in values if value}
    if instrument and "_" in instrument:
        resources.update(str(instrument).upper().split("_", 1))
    return sorted(resources)


def load_records(connection: sqlite3.Connection, session_id: str) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    records: list[dict[str, Any]] = []
    query = """
      SELECT d.decision_id,d.clock_id,d.sequence_no,d.decision_json,d.state_before_json,
             d.position_thesis_id_before,d.row_sha256 AS decision_row_sha256,
             c.decision_epoch,c.market_episode_id,c.coarse_situation_id,
             f.primary_terminal_equity_pips,f.best_alternative_equity_pips,
             f.physical_path_id,f.currency_resources_json,f.components_json,
             f.row_sha256 AS feedback_row_sha256,o.status AS execution_status
      FROM spr_decisions d JOIN spr_clocks c ON c.clock_id=d.clock_id
      JOIN spr_feedback f ON f.decision_id=d.decision_id
      JOIN spr_execution_outcomes o ON o.decision_id=d.decision_id
      WHERE d.session_id=? ORDER BY d.sequence_no,d.decision_id
    """
    for row in connection.execute(query, (session_id,)):
        if row["execution_status"] != "applied":
            continue
        records.append({
            "decision_id": str(row["decision_id"]), "clock_id": str(row["clock_id"]),
            "sequence_no": int(row["sequence_no"]), "decision_epoch": int(row["decision_epoch"]),
            "market_episode_id": str(row["market_episode_id"]), "coarse_situation_id": str(row["coarse_situation_id"]),
            "position_thesis_id_before": row["position_thesis_id_before"], "physical_path_id": str(row["physical_path_id"]),
            "currency_resources": _loads(row["currency_resources_json"], list),
            "primary_equity": float(row["primary_terminal_equity_pips"]),
            "best_alternative_equity": float(row["best_alternative_equity_pips"]),
            "decision": _loads(row["decision_json"], dict), "state_before": _loads(row["state_before_json"], dict),
            "components": _loads(row["components_json"], dict),
            "decision_row_sha256": str(row["decision_row_sha256"]), "feedback_row_sha256": str(row["feedback_row_sha256"]),
        })
    alternatives: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in connection.execute("SELECT counterfactual_id,decision_id,branch_label,terminal_equity_pips,action_json FROM spr_counterfactuals WHERE session_id=? ORDER BY decision_id,branch_label,counterfactual_id", (session_id,)):
        action = _loads(row["action_json"], dict)
        alternatives[str(row["decision_id"])].append({
            "counterfactual_id": str(row["counterfactual_id"]), "branch_label": str(row["branch_label"]),
            "terminal_equity_pips": float(row["terminal_equity_pips"]), "action": str(action.get("action") or ""),
            "instrument": action.get("instrument"), "side": action.get("side"),
        })
    return records, dict(alternatives)


def observation(category: str, reason: str, record: Mapping[str, Any], comparator: Mapping[str, Any] | None, materiality: float, calibration_excess: float = 0.0) -> dict[str, Any]:
    decision, state_before, components = record["decision"], record["state_before"], record["components"]
    position = state_before.get("position") or {}
    instrument = decision.get("instrument") or position.get("instrument")
    identity = {"category": category, "reason_code": reason, "decision_id": record["decision_id"], "comparator_id": comparator.get("counterfactual_id") if comparator else None}
    return {
        "observation_id": "sprmistake_" + stable_hash(identity)[:28], "category": category,
        "reason_code": reason, "decision_id": record["decision_id"], "clock_id": record["clock_id"],
        "sequence_no": int(record["sequence_no"]), "decision_epoch": int(record["decision_epoch"]),
        "market_episode_id": record["market_episode_id"], "coarse_situation_id": record["coarse_situation_id"],
        "physical_path_id": record["physical_path_id"], "position_thesis_id": record["position_thesis_id_before"],
        "currency_resources": _resources(str(instrument) if instrument else None, record["currency_resources"]),
        "primary_action": decision.get("action"), "primary_instrument": instrument,
        "primary_side": decision.get("side") or position.get("side"),
        "primary_terminal_equity_pips": _round(record["primary_equity"]),
        "best_alternative_equity_pips": _round(record["best_alternative_equity"]),
        "materiality_pips": _round(max(0.0, materiality)), "calibration_excess_brier": _round(max(0.0, calibration_excess)),
        "predicted_confidence": _round(components.get("predicted_confidence")), "brier": _round(components.get("brier")),
        "cost_clear": components.get("cost_clear"), "comparator": dict(comparator) if comparator else None,
        "source_decision_row_sha256": record["decision_row_sha256"], "source_feedback_row_sha256": record["feedback_row_sha256"],
    }


def classify(record: Mapping[str, Any], alternatives: Sequence[Mapping[str, Any]], threshold: float) -> list[dict[str, Any]]:
    decision, state_before, components = record["decision"], record["state_before"], record["components"]
    action, primary = str(decision.get("action") or ""), float(record["primary_equity"])
    cutoff = primary + threshold - 1e-9
    ranked = sorted(alternatives, key=lambda row: (-float(row["terminal_equity_pips"]), str(row["branch_label"])))
    best = ranked[0] if ranked else None
    regret = max(0.0, float(best["terminal_equity_pips"]) - primary) if best else 0.0
    output: list[dict[str, Any]] = []
    def add(category: str, reason: str, comparator: Mapping[str, Any] | None, materiality: float, calibration: float = 0.0) -> None:
        output.append(observation(category, reason, record, comparator, materiality, calibration))
    if action in {"enter", "rotate"} and decision.get("instrument") and decision.get("side"):
        opposite = [row for row in alternatives if row.get("instrument") == decision.get("instrument") and row.get("side") == -int(decision["side"]) and float(row["terminal_equity_pips"]) >= cutoff]
        if opposite:
            chosen = sorted(opposite, key=lambda row: (-float(row["terminal_equity_pips"]), str(row["branch_label"])))[0]
            add("direction", "opposite_side_outperformed", chosen, float(chosen["terminal_equity_pips"]) - primary)
    action_categories = {"enter": ("entry", "entry_underperformed_best_alternative"), "hold": ("management", "hold_underperformed_best_alternative"), "exit": ("exit", "exit_underperformed_hold"), "rotate": ("rotation", "rotation_underperformed_hold_or_exit")}
    if action in action_categories and best and float(best["terminal_equity_pips"]) >= cutoff:
        add(*action_categories[action], best, regret)
    if action in {"enter", "rotate"} and components.get("cost_clear") is False:
        realized_before = float(state_before.get("realized_pips") or 0.0)
        add("cost_awareness", "failed_executable_cost_clearance", best, max(0.0, realized_before - primary, regret))
    brier = components.get("brier")
    if action in {"enter", "rotate"} and brier is not None and float(brier) > 0.25 + 1e-12:
        add("calibration", "brier_worse_than_neutral", best, regret, float(brier) - 0.25)
    position = state_before.get("position") or {}
    instrument = decision.get("instrument") or position.get("instrument")
    alternatives_other = [row for row in alternatives if row.get("action") in {"enter", "rotate"} and row.get("instrument") and row.get("side") in {-1, 1} and instrument and row.get("instrument") != instrument and float(row["terminal_equity_pips"]) >= cutoff]
    if alternatives_other:
        chosen = sorted(alternatives_other, key=lambda row: (-float(row["terminal_equity_pips"]), str(row["branch_label"])))[0]
        add("opportunity_selection", "different_instrument_outperformed", chosen, float(chosen["terminal_equity_pips"]) - primary)
    return sorted(output, key=lambda row: (row["category"], row["observation_id"]))


class DisjointSet:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
    def find(self, value: int) -> int:
        while self.parent[value] != value:
            self.parent[value] = self.parent[self.parent[value]]
            value = self.parent[value]
        return value
    def union(self, left: int, right: int) -> None:
        a, b = self.find(left), self.find(right)
        if a != b:
            self.parent[max(a, b)] = min(a, b)


def cluster(observations: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ordered = [dict(row) for row in sorted(observations, key=lambda row: row["observation_id"])]
    dsu = DisjointSet(len(ordered))
    for left in range(len(ordered)):
        for right in range(left + 1, len(ordered)):
            a, b = ordered[left], ordered[right]
            if a["category"] != b["category"] or a["market_episode_id"] != b["market_episode_id"]:
                continue
            shared_resources = bool(set(a["currency_resources"]) & set(b["currency_resources"]))
            shared_thesis = bool(a.get("position_thesis_id") and a.get("position_thesis_id") == b.get("position_thesis_id"))
            if shared_resources or shared_thesis:
                dsu.union(left, right)
    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for index, row in enumerate(ordered):
        groups[dsu.find(index)].append(row)
    clusters: list[dict[str, Any]] = []
    mapping: dict[str, str] = {}
    for group in groups.values():
        group.sort(key=lambda row: (row["sequence_no"], row["observation_id"]))
        category, episode = group[0]["category"], group[0]["market_episode_id"]
        ids = sorted(row["observation_id"] for row in group)
        cluster_id = "sprmistakecluster_" + stable_hash(category, episode, ids)[:28]
        for identifier in ids:
            mapping[identifier] = cluster_id
        materialities = [float(row["materiality_pips"]) for row in group]
        representative = sorted(group, key=lambda row: (-float(row["materiality_pips"]), row["observation_id"]))[0]
        clusters.append({
            "cluster_id": cluster_id, "category": category, "market_episode_id": episode,
            "observation_count": len(group), "observation_ids": ids,
            "decision_ids": sorted({row["decision_id"] for row in group}),
            "sequence_numbers": sorted({int(row["sequence_no"]) for row in group}),
            "currency_resources": sorted({resource for row in group for resource in row["currency_resources"]}),
            "instruments": sorted({str(row["primary_instrument"]) for row in group if row.get("primary_instrument")}),
            "reason_codes": dict(sorted(Counter(row["reason_code"] for row in group).items())),
            "raw_materiality_pips": _round(sum(materialities)), "cluster_max_materiality_pips": _round(max(materialities)),
            "representative_observation_id": representative["observation_id"],
        })
    for row in ordered:
        row["cluster_id"] = mapping[row["observation_id"]]
    clusters.sort(key=lambda row: (row["category"], row["cluster_id"]))
    ordered.sort(key=lambda row: (row["sequence_no"], row["category"], row["observation_id"]))
    return ordered, clusters


def build_expected_report(connection: sqlite3.Connection, state: Mapping[str, Any], config: Mapping[str, Any], material: Mapping[str, Any], source_binding: Mapping[str, Any], cohort_id: str) -> dict[str, Any]:
    records, alternatives = load_records(connection, str(state["session_id"]))
    observations: list[dict[str, Any]] = []
    threshold = float(config["classification"]["minimum_material_regret_pips"])
    for record in records:
        observations.extend(classify(record, alternatives.get(record["decision_id"], []), threshold))
    observations, clusters = cluster(observations)
    by_obs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_cluster: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in observations: by_obs[row["category"]].append(row)
    for row in clusters: by_cluster[row["category"]].append(row)
    categories: dict[str, Any] = {}
    for category in CATEGORY_DEFINITIONS:
        obs, grouped = by_obs.get(category, []), by_cluster.get(category, [])
        categories[category] = {
            "definition": CATEGORY_DEFINITIONS[category], "raw_observation_count": len(obs),
            "structural_cluster_count": len(grouped), "unique_decision_count": len({row["decision_id"] for row in obs}),
            "raw_materiality_pips": _round(sum(float(row["materiality_pips"]) for row in obs)),
            "cluster_max_materiality_pips": _round(sum(float(row["cluster_max_materiality_pips"]) for row in grouped)),
            "reason_counts": dict(sorted(Counter(row["reason_code"] for row in obs).items())),
        }
    priorities = sorted(({"category": category, "structural_cluster_count": details["structural_cluster_count"], "cluster_max_materiality_pips": details["cluster_max_materiality_pips"]} for category, details in categories.items() if details["raw_observation_count"]), key=lambda row: (-float(row["cluster_max_materiality_pips"]), -int(row["structural_cluster_count"]), row["category"]))
    report = {
        "schema_version": 1, "contract_id": REPORT_CONTRACT_ID, "cohort_id": cohort_id,
        "material_contract_sha256": stable_hash(material), "material_contract": dict(material),
        **POLICY, "evidence_role": "historical_training_curriculum", "source_binding": dict(source_binding),
        "classification_contract": dict(config["classification"]),
        "source_counts": {
            "applied_decisions": len(records),
            "feedback_rows": int(connection.execute("SELECT COUNT(*) FROM spr_feedback WHERE session_id=?", (state["session_id"],)).fetchone()[0]),
            "counterfactual_rows": int(connection.execute("SELECT COUNT(*) FROM spr_counterfactuals WHERE session_id=?", (state["session_id"],)).fetchone()[0]),
        },
        "summary": {
            "raw_observation_count": len(observations), "structural_cluster_count": len(clusters),
            "unique_decisions_with_observations": len({row["decision_id"] for row in observations}),
            "market_episodes_with_observations": len({row["market_episode_id"] for row in observations}),
            "independent_regime_count": None,
        },
        "categories": categories, "curriculum_priorities": priorities, "clusters": clusters, "observations": observations,
        "limitations": [
            "Already-inspected historical training evidence cannot confirm or promote a policy.",
            "Categories intentionally overlap and must not be summed as independent errors.",
            "Structural clusters reduce obvious repetition but are not independent market regimes.",
            "Depth-one alternatives diagnose local decisions, not the globally optimal path.",
            "Price-only replay has no news, rates, levels, or positioning attribution.",
        ],
    }
    report["report_sha256"] = stable_hash(report)
    report["report_id"] = "sprmistakecurriculum_" + report["report_sha256"][:28]
    return report


def material_contract(config: Mapping[str, Any], config_path: Path, state: Mapping[str, Any], state_path: Path, receipt_path: Path, database: Path) -> dict[str, Any]:
    return {
        "schema_version": 1, "contract_id": REPORT_CONTRACT_ID,
        "config_sha256": file_sha256(config_path), "config_semantic_sha256": stable_hash(config),
        "producer_code_sha256": file_sha256(ROOT / "oanda_sequential_portfolio_mistake_curriculum.py"),
        "core_code_sha256": file_sha256(ROOT / "src" / "forex_system" / "research" / "sequential_portfolio_mistake_curriculum_v1.py"),
        "verifier_code_sha256": file_sha256(Path(__file__).resolve()),
        "source_state_sha256": file_sha256(state_path), "source_verifier_receipt_sha256": file_sha256(receipt_path),
        "source_database_sha256": file_sha256(database), "source_cohort_id": state["cohort_id"],
        "source_session_id": state["session_id"], "source_session_seal_id": state["session_seal_id"],
        "source_session_seal_roots_sha256": stable_hash(state["roots"]),
        "classification": config["classification"],
        "policy": {**POLICY, "evidence_role": "historical_training_curriculum"},
    }


def verify(config_path: Path = DEFAULT_CONFIG, report_path: Path = DEFAULT_REPORT, output_path: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    failures: list[str] = []
    blocked = forbidden_imports()
    if blocked: failures.append("verifier_import_isolation")
    config, report = read_json(config_path), read_json(report_path)
    failures.extend(validate_config(config))
    source_cfg = config["source"]
    state_path = resolve_relative(str(source_cfg["state_relative_path"]))
    source_receipt_path = resolve_relative(str(source_cfg["verifier_relative_path"]))
    state, source_receipt = read_json(state_path), read_json(source_receipt_path)
    database = Path(str(state["database"])).resolve(strict=True)
    replay_root = (ROOT / "data" / "oanda_training_manager" / "research_ledgers" / "sequential_portfolio_replay_v1").resolve()
    if replay_root not in database.parents:
        failures.append("source_database_path")
    material = material_contract(config, config_path, state, state_path, source_receipt_path, database)
    material_sha = stable_hash(material)
    expected_cohort = str(config["experiment_key"]) + "." + material_sha[:20]
    source = source_snapshot(database)
    try:
        source_failures, source_checks = verify_source(config, state, source_receipt, source)
        failures.extend(source_failures)
        source_binding = {
            "cohort_id": state["cohort_id"], "session_id": state["session_id"],
            "session_seal_id": state["session_seal_id"], "session_seal_sha256": source_checks.get("seal_sha256"),
            "as_of_seal_utc": source_checks.get("seal_created_utc"), "verified_roots": source_checks.get("roots"),
            "source_state_sha256": material["source_state_sha256"],
            "source_verifier_receipt_sha256": material["source_verifier_receipt_sha256"],
            "source_database_sha256": material["source_database_sha256"],
        }
        expected_report = build_expected_report(source, state, config, material, source_binding, expected_cohort)
    finally:
        source.close()
    for key, expected in {**POLICY, "evidence_role": "historical_training_curriculum"}.items():
        if report.get(key) != expected: failures.append("unsafe_report_" + key)
    if report.get("cohort_id") != expected_cohort: failures.append("cohort_id")
    if report.get("material_contract_sha256") != material_sha or report.get("material_contract") != material:
        failures.append("material_contract")
    if report.get("classification_contract") != config.get("classification"):
        failures.append("classification_contract")
    if report.get("source_binding") != source_binding:
        failures.append("source_binding")
    semantic = dict(report)
    supplied_id = semantic.pop("report_id", None)
    supplied_hash = semantic.pop("report_sha256", None)
    computed_hash = stable_hash(semantic)
    if supplied_hash != computed_hash: failures.append("report_content_hash")
    if supplied_id != "sprmistakecurriculum_" + computed_hash[:28]: failures.append("report_id")
    if report.get("source_counts") != expected_report["source_counts"]: failures.append("source_counts")
    if report.get("summary") != expected_report["summary"]: failures.append("summary")
    if report.get("categories") != expected_report["categories"]: failures.append("categories")
    if report.get("curriculum_priorities") != expected_report["curriculum_priorities"]: failures.append("priorities")
    if report.get("clusters") != expected_report["clusters"]: failures.append("clusters")
    if report.get("observations") != expected_report["observations"]: failures.append("observations")
    if report != expected_report: failures.append("full_report_reconstruction")
    deterministic_time = str(source_binding.get("as_of_seal_utc") or datetime.now(timezone.utc).isoformat())
    result = {
        "schema_version": 1, "generated_utc": deterministic_time, "verified": not failures,
        "failures": sorted(set(failures)), "cohort_id": report.get("cohort_id"), "report_id": report.get("report_id"),
        "report_sha256": file_sha256(report_path), "report_content_sha256": report.get("report_sha256"),
        "material_contract_sha256": material_sha, **POLICY, "evidence_role": "historical_training_curriculum",
        "checks": {
            "verifier_forbidden_imports": blocked, "source_cohort_id": state.get("cohort_id"),
            "source_session_id": state.get("session_id"), "source_session_seal_id": state.get("session_seal_id"),
            "source_session_seal_sha256": source_binding.get("session_seal_sha256"),
            "source_database_sha256": material.get("source_database_sha256"),
            "source_state_sha256": material.get("source_state_sha256"),
            "source_verifier_receipt_sha256": material.get("source_verifier_receipt_sha256"),
            "source_roots": source_binding.get("verified_roots"),
            "raw_observation_count": expected_report["summary"]["raw_observation_count"],
            "structural_cluster_count": expected_report["summary"]["structural_cluster_count"],
        },
    }
    atomic_json(output_path, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    result = verify(args.config.resolve(strict=True), args.report.resolve(strict=True), args.output.resolve())
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
