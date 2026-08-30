"""Deterministic mistake curriculum for Sequential Portfolio Replay V1.

This module is a read-only research sidecar.  It consumes an independently
verified, sealed replay database and classifies local decision feedback.  It
has no broker, signal, lifecycle, authorization, or account interfaces.

Observations are deliberately not treated as independent samples.  Within
each curriculum category, observations from the same predeclared market
episode are joined when they share a currency resource (transitively) or a
position thesis.  Reports expose both raw observations and these structural
clusters; neither count is a claim about independent market regimes.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable, Mapping, Sequence


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

REPORT_CONTRACT_ID = "sequential_portfolio_mistake_curriculum_v1_20260829_hardened"

# The upstream V1 state predates explicit broker/account-access keys.  These
# six fields are its complete persisted isolation boundary; the sidecar adds
# stricter broker/account prohibitions to its own output contract above.
SOURCE_REQUIRED_POLICY = {
    key: POLICY[key]
    for key in (
        "research_only",
        "execution_eligible",
        "can_promote",
        "can_place_orders",
        "can_authorize",
        "supported_decision",
    )
}

CATEGORY_DEFINITIONS = {
    "direction": (
        "An executable opposite-side branch on the same instrument beat the "
        "primary trade by the material-regret threshold."
    ),
    "entry": (
        "A primary entry underperformed its best depth-one alternative by the "
        "material-regret threshold."
    ),
    "management": (
        "Holding an existing position underperformed its best depth-one "
        "alternative by the material-regret threshold."
    ),
    "exit": (
        "Exiting an existing position underperformed holding by the "
        "material-regret threshold."
    ),
    "rotation": (
        "A primary rotation underperformed holding or exiting by the "
        "material-regret threshold."
    ),
    "cost_awareness": (
        "An entry or rotation failed to clear its executable bid/ask and "
        "slippage economics at the declared feedback horizon."
    ),
    "calibration": (
        "A scored entry or rotation had Brier loss strictly worse than the "
        "neutral-probability baseline of 0.25."
    ),
    "opportunity_selection": (
        "A different-instrument depth-one alternative beat the selected "
        "instrument by the material-regret threshold."
    ),
}

REQUIRED_TABLES = frozenset(
    {
        "spr_sessions",
        "spr_clocks",
        "spr_decisions",
        "spr_execution_outcomes",
        "spr_counterfactuals",
        "spr_feedback",
        "spr_session_seals",
    }
)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def stable_hash(*parts: Any) -> str:
    return sha256(canonical_json(parts).encode("utf-8")).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _round(value: float | int | None) -> float | None:
    return None if value is None else round(float(value), 12)


def _loads(value: str, expected: type) -> Any:
    parsed = json.loads(value)
    if not isinstance(parsed, expected):
        raise ValueError(f"expected {expected.__name__} JSON")
    return parsed


def validate_verified_binding(
    state: Mapping[str, Any], verifier: Mapping[str, Any]
) -> None:
    """Fail closed unless state and independent verifier name one sealed run."""

    for key, expected in SOURCE_REQUIRED_POLICY.items():
        if state.get(key) != expected:
            raise ValueError(f"state research-isolation mismatch: {key}")
        if key in verifier and verifier.get(key) != expected:
            raise ValueError(f"verifier research-isolation mismatch: {key}")
    if verifier.get("verified") is not True or verifier.get("failures") != []:
        raise ValueError("source replay must have a clean independent verification")
    for key in ("cohort_id", "session_id"):
        if not str(state.get(key) or "") or state.get(key) != verifier.get(key):
            raise ValueError(f"state/verifier mismatch: {key}")
    if state.get("proof_eligible") is not False:
        raise ValueError("mistake curriculum source must remain proof-ineligible")
    if state.get("evidence_role") != "historical_training_discovery":
        raise ValueError("mistake curriculum only accepts historical training evidence")
    if not str(state.get("session_seal_id") or ""):
        raise ValueError("missing immutable session seal")
    if state.get("roots") != verifier.get("checks", {}).get("roots"):
        raise ValueError("state/verifier row-root mismatch")


def open_verified_database(
    database: Path,
    *,
    state: Mapping[str, Any],
    allowed_root: Path,
) -> sqlite3.Connection:
    """Open the bound SQLite ledger read-only and validate its immutable seal."""

    allowed = allowed_root.resolve()
    resolved = database.resolve(strict=True)
    if resolved != allowed and allowed not in resolved.parents:
        raise ValueError("source database escapes the replay research-ledger root")
    connection = sqlite3.connect(
        f"file:{resolved.as_posix()}?mode=ro", uri=True, timeout=60.0
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    missing = sorted(REQUIRED_TABLES - tables)
    if missing:
        connection.close()
        raise ValueError(f"source replay database is incomplete: {missing}")
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    if integrity.lower() != "ok":
        connection.close()
        raise ValueError(f"source replay database integrity failed: {integrity}")
    session = connection.execute(
        "SELECT cohort_id,session_id,evidence_role,proof_eligible FROM spr_sessions "
        "WHERE session_id=?",
        (state["session_id"],),
    ).fetchone()
    if session is None:
        connection.close()
        raise ValueError("bound replay session is absent")
    if (
        session["cohort_id"] != state["cohort_id"]
        or session["evidence_role"] != "historical_training_discovery"
        or int(session["proof_eligible"]) != 0
    ):
        connection.close()
        raise ValueError("bound replay session changed identity or evidence role")
    seal = connection.execute(
        "SELECT seal_id,seal_sha256,created_utc,seal_json FROM spr_session_seals "
        "WHERE seal_id=? AND session_id=?",
        (state["session_seal_id"], state["session_id"]),
    ).fetchone()
    if seal is None:
        connection.close()
        raise ValueError("bound immutable session seal is absent")
    seal_payload = _loads(seal["seal_json"], dict)
    if seal_payload.get("roots") != state["roots"]:
        connection.close()
        raise ValueError("database seal roots do not match verified receipt")
    return connection


def _resources(instrument: str | None, feedback_resources: Sequence[str]) -> list[str]:
    resources = {str(value).upper() for value in feedback_resources if value}
    if instrument and "_" in instrument:
        resources.update(str(instrument).upper().split("_", 1))
    return sorted(resources)


def _alternative_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    action = _loads(str(row["action_json"]), dict)
    return {
        "counterfactual_id": str(row["counterfactual_id"]),
        "branch_label": str(row["branch_label"]),
        "terminal_equity_pips": float(row["terminal_equity_pips"]),
        "action": str(action.get("action") or ""),
        "instrument": action.get("instrument"),
        "side": action.get("side"),
    }


def _observation(
    *,
    category: str,
    reason_code: str,
    record: Mapping[str, Any],
    decision: Mapping[str, Any],
    state_before: Mapping[str, Any],
    components: Mapping[str, Any],
    comparator: Mapping[str, Any] | None,
    materiality_pips: float,
    calibration_excess_brier: float = 0.0,
) -> dict[str, Any]:
    position = state_before.get("position") or {}
    primary_instrument = decision.get("instrument") or position.get("instrument")
    resources = _resources(
        str(primary_instrument) if primary_instrument else None,
        record["currency_resources"],
    )
    identity = {
        "category": category,
        "reason_code": reason_code,
        "decision_id": record["decision_id"],
        "comparator_id": comparator.get("counterfactual_id") if comparator else None,
    }
    return {
        "observation_id": "sprmistake_" + stable_hash(identity)[:28],
        "category": category,
        "reason_code": reason_code,
        "decision_id": record["decision_id"],
        "clock_id": record["clock_id"],
        "sequence_no": int(record["sequence_no"]),
        "decision_epoch": int(record["decision_epoch"]),
        "market_episode_id": record["market_episode_id"],
        "coarse_situation_id": record["coarse_situation_id"],
        "physical_path_id": record["physical_path_id"],
        "position_thesis_id": record["position_thesis_id_before"],
        "currency_resources": resources,
        "primary_action": decision.get("action"),
        "primary_instrument": primary_instrument,
        "primary_side": decision.get("side") or position.get("side"),
        "primary_terminal_equity_pips": _round(record["primary_equity"]),
        "best_alternative_equity_pips": _round(record["best_alternative_equity"]),
        "materiality_pips": _round(max(0.0, materiality_pips)),
        "calibration_excess_brier": _round(max(0.0, calibration_excess_brier)),
        "predicted_confidence": _round(components.get("predicted_confidence")),
        "brier": _round(components.get("brier")),
        "cost_clear": components.get("cost_clear"),
        "comparator": dict(comparator) if comparator else None,
        "source_decision_row_sha256": record["decision_row_sha256"],
        "source_feedback_row_sha256": record["feedback_row_sha256"],
    }


def classify_decision(
    record: Mapping[str, Any],
    alternatives: Sequence[Mapping[str, Any]],
    *,
    minimum_material_regret_pips: float,
) -> list[dict[str, Any]]:
    """Classify one verified decision without assuming categories are exclusive."""

    if minimum_material_regret_pips <= 0:
        raise ValueError("minimum material regret must be positive")
    decision = record["decision"]
    state_before = record["state_before"]
    components = record["components"]
    action = str(decision.get("action") or "")
    primary_equity = float(record["primary_equity"])
    material_cutoff = primary_equity + float(minimum_material_regret_pips) - 1e-9
    ranked = sorted(
        alternatives,
        key=lambda row: (-float(row["terminal_equity_pips"]), str(row["branch_label"])),
    )
    best = ranked[0] if ranked else None
    regret = (
        max(0.0, float(best["terminal_equity_pips"]) - primary_equity)
        if best
        else 0.0
    )
    output: list[dict[str, Any]] = []

    def add(
        category: str,
        reason: str,
        comparator: Mapping[str, Any] | None,
        materiality: float,
        calibration_excess: float = 0.0,
    ) -> None:
        output.append(
            _observation(
                category=category,
                reason_code=reason,
                record=record,
                decision=decision,
                state_before=state_before,
                components=components,
                comparator=comparator,
                materiality_pips=materiality,
                calibration_excess_brier=calibration_excess,
            )
        )

    if action in {"enter", "rotate"} and decision.get("instrument") and decision.get("side"):
        opposite = [
            alt
            for alt in alternatives
            if alt.get("instrument") == decision.get("instrument")
            and alt.get("side") == -int(decision["side"])
            and float(alt["terminal_equity_pips"]) >= material_cutoff
        ]
        if opposite:
            opposite_best = sorted(
                opposite,
                key=lambda row: (
                    -float(row["terminal_equity_pips"]),
                    str(row["branch_label"]),
                ),
            )[0]
            add(
                "direction",
                "opposite_side_outperformed",
                opposite_best,
                float(opposite_best["terminal_equity_pips"]) - primary_equity,
            )

    action_categories = {
        "enter": ("entry", "entry_underperformed_best_alternative"),
        "hold": ("management", "hold_underperformed_best_alternative"),
        "exit": ("exit", "exit_underperformed_hold"),
        "rotate": ("rotation", "rotation_underperformed_hold_or_exit"),
    }
    if action in action_categories and best and float(best["terminal_equity_pips"]) >= material_cutoff:
        category, reason = action_categories[action]
        add(category, reason, best, regret)

    if action in {"enter", "rotate"} and components.get("cost_clear") is False:
        realized_before = float(state_before.get("realized_pips") or 0.0)
        shortfall = max(0.0, realized_before - primary_equity, regret)
        add(
            "cost_awareness",
            "failed_executable_cost_clearance",
            best,
            shortfall,
        )

    brier = components.get("brier")
    if action in {"enter", "rotate"} and brier is not None and float(brier) > 0.25 + 1e-12:
        add(
            "calibration",
            "brier_worse_than_neutral",
            best,
            regret,
            float(brier) - 0.25,
        )

    position = state_before.get("position") or {}
    primary_instrument = decision.get("instrument") or position.get("instrument")
    different_instrument = [
        alt
        for alt in alternatives
        if alt.get("action") in {"enter", "rotate"}
        and alt.get("instrument")
        and alt.get("side") in {-1, 1}
        and primary_instrument
        and alt.get("instrument") != primary_instrument
        and float(alt["terminal_equity_pips"]) >= material_cutoff
    ]
    if different_instrument:
        selected = sorted(
            different_instrument,
            key=lambda row: (
                -float(row["terminal_equity_pips"]),
                str(row["branch_label"]),
            ),
        )[0]
        add(
            "opportunity_selection",
            "different_instrument_outperformed",
            selected,
            float(selected["terminal_equity_pips"]) - primary_equity,
        )
    return sorted(output, key=lambda row: (row["category"], row["observation_id"]))


def load_decision_records(
    connection: sqlite3.Connection, session_id: str
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    rows = connection.execute(
        """
        SELECT d.decision_id,d.clock_id,d.sequence_no,d.action,d.instrument,d.side,
               d.position_thesis_id_before,d.decision_json,d.state_before_json,
               d.row_sha256 AS decision_row_sha256,
               c.decision_epoch,c.market_episode_id,c.coarse_situation_id,
               f.primary_terminal_equity_pips,f.best_alternative_equity_pips,
               f.physical_path_id,f.currency_resources_json,f.components_json,
               f.row_sha256 AS feedback_row_sha256,
               o.status AS execution_status,o.rejection_reason
        FROM spr_decisions d
        JOIN spr_clocks c ON c.clock_id=d.clock_id
        JOIN spr_feedback f ON f.decision_id=d.decision_id
        JOIN spr_execution_outcomes o ON o.decision_id=d.decision_id
        WHERE d.session_id=?
        ORDER BY d.sequence_no,d.decision_id
        """,
        (session_id,),
    ).fetchall()
    records: list[dict[str, Any]] = []
    for row in rows:
        if row["execution_status"] != "applied":
            continue
        records.append(
            {
                "decision_id": str(row["decision_id"]),
                "clock_id": str(row["clock_id"]),
                "sequence_no": int(row["sequence_no"]),
                "decision_epoch": int(row["decision_epoch"]),
                "market_episode_id": str(row["market_episode_id"]),
                "coarse_situation_id": str(row["coarse_situation_id"]),
                "position_thesis_id_before": row["position_thesis_id_before"],
                "physical_path_id": str(row["physical_path_id"]),
                "currency_resources": _loads(row["currency_resources_json"], list),
                "primary_equity": float(row["primary_terminal_equity_pips"]),
                "best_alternative_equity": float(row["best_alternative_equity_pips"]),
                "decision": _loads(row["decision_json"], dict),
                "state_before": _loads(row["state_before_json"], dict),
                "components": _loads(row["components_json"], dict),
                "decision_row_sha256": str(row["decision_row_sha256"]),
                "feedback_row_sha256": str(row["feedback_row_sha256"]),
            }
        )
    alternatives: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in connection.execute(
        "SELECT counterfactual_id,decision_id,branch_label,terminal_equity_pips,"
        "action_json FROM spr_counterfactuals WHERE session_id=? "
        "ORDER BY decision_id,branch_label,counterfactual_id",
        (session_id,),
    ):
        alternatives[str(row["decision_id"])].append(_alternative_payload(row))
    return records, dict(alternatives)


class _DisjointSet:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, value: int) -> int:
        while self.parent[value] != value:
            self.parent[value] = self.parent[self.parent[value]]
            value = self.parent[value]
        return value

    def union(self, left: int, right: int) -> None:
        left_root, right_root = self.find(left), self.find(right)
        if left_root != right_root:
            self.parent[max(left_root, right_root)] = min(left_root, right_root)


def cluster_observations(
    observations: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Collapse same-category, same-episode, resource-connected observations."""

    ordered = [dict(row) for row in sorted(observations, key=lambda r: r["observation_id"])]
    dsu = _DisjointSet(len(ordered))
    for left in range(len(ordered)):
        for right in range(left + 1, len(ordered)):
            a, b = ordered[left], ordered[right]
            if a["category"] != b["category"] or a["market_episode_id"] != b["market_episode_id"]:
                continue
            shared_resources = bool(
                set(a["currency_resources"]) & set(b["currency_resources"])
            )
            shared_thesis = bool(
                a.get("position_thesis_id")
                and a.get("position_thesis_id") == b.get("position_thesis_id")
            )
            if shared_resources or shared_thesis:
                dsu.union(left, right)
    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for index, row in enumerate(ordered):
        groups[dsu.find(index)].append(row)
    clusters: list[dict[str, Any]] = []
    observation_cluster: dict[str, str] = {}
    for group in groups.values():
        group = sorted(group, key=lambda row: (row["sequence_no"], row["observation_id"]))
        observation_ids = sorted(row["observation_id"] for row in group)
        category = str(group[0]["category"])
        episode = str(group[0]["market_episode_id"])
        cluster_id = "sprmistakecluster_" + stable_hash(
            category, episode, observation_ids
        )[:28]
        for observation_id in observation_ids:
            observation_cluster[observation_id] = cluster_id
        materialities = [float(row["materiality_pips"]) for row in group]
        representative = sorted(
            group,
            key=lambda row: (
                -float(row["materiality_pips"]),
                -float(row["calibration_excess_brier"]),
                int(row["sequence_no"]),
                row["observation_id"],
            ),
        )[0]
        clusters.append(
            {
                "cluster_id": cluster_id,
                "category": category,
                "market_episode_id": episode,
                "observation_count": len(group),
                "observation_ids": observation_ids,
                "decision_ids": sorted({row["decision_id"] for row in group}),
                "sequence_numbers": sorted({int(row["sequence_no"]) for row in group}),
                "currency_resources": sorted(
                    {resource for row in group for resource in row["currency_resources"]}
                ),
                "instruments": sorted(
                    {
                        str(row["primary_instrument"])
                        for row in group
                        if row.get("primary_instrument")
                    }
                ),
                "reason_codes": dict(sorted(Counter(row["reason_code"] for row in group).items())),
                "raw_materiality_pips": _round(sum(materialities)),
                "cluster_max_materiality_pips": _round(max(materialities)),
                "representative_observation_id": representative["observation_id"],
            }
        )
    for row in ordered:
        row["cluster_id"] = observation_cluster[row["observation_id"]]
    clusters.sort(key=lambda row: (row["category"], row["cluster_id"]))
    ordered.sort(key=lambda row: (row["sequence_no"], row["category"], row["observation_id"]))
    return ordered, clusters


def build_report(
    connection: sqlite3.Connection,
    *,
    state: Mapping[str, Any],
    curriculum_cohort_id: str,
    material_contract: Mapping[str, Any],
    material_contract_sha256: str,
    source_binding: Mapping[str, Any],
    classification_contract: Mapping[str, Any],
) -> dict[str, Any]:
    minimum_material_regret_pips = float(
        classification_contract["minimum_material_regret_pips"]
    )
    if minimum_material_regret_pips <= 0:
        raise ValueError("minimum material regret must be positive")
    records, alternatives = load_decision_records(connection, str(state["session_id"]))
    observations: list[dict[str, Any]] = []
    for record in records:
        observations.extend(
            classify_decision(
                record,
                alternatives.get(record["decision_id"], []),
                minimum_material_regret_pips=minimum_material_regret_pips,
            )
        )
    observations, clusters = cluster_observations(observations)
    by_category_observations: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_category_clusters: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in observations:
        by_category_observations[row["category"]].append(row)
    for row in clusters:
        by_category_clusters[row["category"]].append(row)
    categories: dict[str, Any] = {}
    for category in CATEGORY_DEFINITIONS:
        obs = by_category_observations.get(category, [])
        cat_clusters = by_category_clusters.get(category, [])
        categories[category] = {
            "definition": CATEGORY_DEFINITIONS[category],
            "raw_observation_count": len(obs),
            "structural_cluster_count": len(cat_clusters),
            "unique_decision_count": len({row["decision_id"] for row in obs}),
            "raw_materiality_pips": _round(sum(float(row["materiality_pips"]) for row in obs)),
            "cluster_max_materiality_pips": _round(
                sum(float(row["cluster_max_materiality_pips"]) for row in cat_clusters)
            ),
            "reason_counts": dict(sorted(Counter(row["reason_code"] for row in obs).items())),
        }
    priorities = sorted(
        (
            {
                "category": category,
                "structural_cluster_count": details["structural_cluster_count"],
                "cluster_max_materiality_pips": details["cluster_max_materiality_pips"],
            }
            for category, details in categories.items()
            if details["raw_observation_count"]
        ),
        key=lambda row: (
            -float(row["cluster_max_materiality_pips"]),
            -int(row["structural_cluster_count"]),
            row["category"],
        ),
    )
    seal = connection.execute(
        "SELECT seal_sha256,created_utc FROM spr_session_seals WHERE seal_id=?",
        (state["session_seal_id"],),
    ).fetchone()
    source_counts = {
        "applied_decisions": len(records),
        "feedback_rows": int(
            connection.execute(
                "SELECT COUNT(*) FROM spr_feedback WHERE session_id=?",
                (state["session_id"],),
            ).fetchone()[0]
        ),
        "counterfactual_rows": int(
            connection.execute(
                "SELECT COUNT(*) FROM spr_counterfactuals WHERE session_id=?",
                (state["session_id"],),
            ).fetchone()[0]
        ),
    }
    core = {
        "schema_version": 1,
        "contract_id": REPORT_CONTRACT_ID,
        "cohort_id": curriculum_cohort_id,
        "material_contract_sha256": material_contract_sha256,
        "material_contract": dict(material_contract),
        **POLICY,
        "evidence_role": "historical_training_curriculum",
        "source_binding": dict(source_binding),
        "classification_contract": dict(classification_contract),
        "source_counts": source_counts,
        "summary": {
            "raw_observation_count": len(observations),
            "structural_cluster_count": len(clusters),
            "unique_decisions_with_observations": len(
                {row["decision_id"] for row in observations}
            ),
            "market_episodes_with_observations": len(
                {row["market_episode_id"] for row in observations}
            ),
            "independent_regime_count": None,
        },
        "categories": categories,
        "curriculum_priorities": priorities,
        "clusters": clusters,
        "observations": observations,
        "limitations": [
            "Already-inspected historical training evidence cannot confirm or promote a policy.",
            "Categories intentionally overlap and must not be summed as independent errors.",
            "Structural clusters reduce obvious repetition but are not independent market regimes.",
            "Depth-one alternatives diagnose local decisions, not the globally optimal path.",
            "Price-only replay has no news, rates, levels, or positioning attribution.",
        ],
    }
    core["report_sha256"] = stable_hash(core)
    core["report_id"] = "sprmistakecurriculum_" + core["report_sha256"][:28]
    return core


def render_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Sequential Portfolio Mistake Curriculum V1",
        "",
        "Status: generated historical training diagnostic; a clean independent verifier receipt is required; research-only; proof-ineligible",
        "",
        f"Report ID: `{report['report_id']}`  ",
        f"Curriculum cohort: `{report['cohort_id']}`  ",
        f"Source cohort: `{report['source_binding']['cohort_id']}`  ",
        f"Source session: `{report['source_binding']['session_id']}`  ",
        f"Immutable seal: `{report['source_binding']['session_seal_id']}`",
        "",
        "## Curriculum census",
        "",
        "| Category | Raw observations | Structural clusters | Cluster-max materiality |",
        "|---|---:|---:|---:|",
    ]
    for category, details in report["categories"].items():
        lines.append(
            f"| {category.replace('_', ' ')} | {details['raw_observation_count']} | "
            f"{details['structural_cluster_count']} | "
            f"{details['cluster_max_materiality_pips']:.2f} pips |"
        )
    summary = report["summary"]
    lines.extend(
        [
            "",
            f"The sidecar found **{summary['raw_observation_count']} nonexclusive observations** "
            f"across **{summary['structural_cluster_count']} category-specific structural clusters** "
            f"and {summary['unique_decisions_with_observations']} unique decisions.",
            "These are curriculum units, not independent statistical samples.",
            "",
            "## Priority order",
            "",
        ]
    )
    for index, row in enumerate(report["curriculum_priorities"], start=1):
        lines.append(
            f"{index}. **{row['category'].replace('_', ' ')}** — "
            f"{row['structural_cluster_count']} clusters; "
            f"{row['cluster_max_materiality_pips']:.2f} cluster-max pips."
        )
    lines.extend(["", "## Largest structural clusters", ""])
    ranked_clusters = sorted(
        report["clusters"],
        key=lambda row: (
            -float(row["cluster_max_materiality_pips"]),
            row["category"],
            row["cluster_id"],
        ),
    )[:12]
    if ranked_clusters:
        lines.extend(
            [
                "| Category | Episode | Decisions | Resources | Max materiality |",
                "|---|---|---|---|---:|",
            ]
        )
        for row in ranked_clusters:
            lines.append(
                f"| {row['category'].replace('_', ' ')} | {row['market_episode_id']} | "
                f"{', '.join(str(value) for value in row['sequence_numbers'])} | "
                f"{', '.join(row['currency_resources']) or 'unassigned'} | "
                f"{row['cluster_max_materiality_pips']:.2f} pips |"
            )
    else:
        lines.append("No material mistake observations crossed the frozen thresholds.")
    lines.extend(["", "## Frozen definitions", ""])
    for category, details in report["categories"].items():
        lines.append(f"- **{category.replace('_', ' ')}:** {details['definition']}")
    lines.extend(["", "## Limits", ""])
    for limitation in report["limitations"]:
        lines.append(f"- {limitation}")
    lines.append("")
    return "\n".join(lines)


def json_text(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"


__all__ = [
    "CATEGORY_DEFINITIONS",
    "POLICY",
    "REPORT_CONTRACT_ID",
    "build_report",
    "canonical_json",
    "classify_decision",
    "cluster_observations",
    "json_text",
    "load_decision_records",
    "open_verified_database",
    "read_json",
    "render_markdown",
    "stable_hash",
    "validate_verified_binding",
]
