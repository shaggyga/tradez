"""Deterministic all-68 mistake-directed curriculum primitives.

This module reads only the frozen, independently verified all-68 historical
batch replay.  It labels local decision errors and preserves every depth-one
alternative as a zero-weight review.  It has no broker, account, lifecycle,
authorization, signal-publication, or execution interface.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from hashlib import sha256
import gzip
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


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

REPORT_CONTRACT_ID = "sequential_all68_mistake_curriculum_v1_20260829"

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
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


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


def _round(value: float | int | None) -> float | None:
    return None if value is None else round(float(value), 12)


def row_hash(row: Mapping[str, Any]) -> str:
    return stable_hash({key: value for key, value in row.items() if key != "row_sha256"})


def load_dataset(
    source_root: Path,
    state: Mapping[str, Any],
    name: str,
) -> list[dict[str, Any]]:
    if name not in SOURCE_DATASETS:
        raise ValueError(f"unsupported source dataset: {name}")
    spec = state["datasets"][name]
    if spec.get("relative_path") != SOURCE_DATASETS[name]:
        raise ValueError(f"source dataset path changed: {name}")
    relative = Path(str(spec["relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"unsafe source dataset path: {name}")
    path = (source_root / relative).resolve(strict=True)
    if source_root.resolve() not in path.parents:
        raise ValueError(f"source dataset escaped cohort root: {name}")
    payload = path.read_bytes()
    if len(payload) != int(spec["gzip_bytes"]):
        raise ValueError(f"source gzip byte count changed: {name}")
    if sha256(payload).hexdigest() != spec["gzip_sha256"]:
        raise ValueError(f"source gzip hash changed: {name}")
    rows: list[dict[str, Any]] = []
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"non-object source row: {name}")
            if value.get("row_sha256") != row_hash(value):
                raise ValueError(f"source row hash changed: {name}")
            rows.append(value)
    hashes = [str(row["row_sha256"]) for row in rows]
    if len(rows) != int(spec["row_count"]):
        raise ValueError(f"source row count changed: {name}")
    if stable_hash(hashes) != spec["ordered_row_sha256"]:
        raise ValueError(f"source ordered row root changed: {name}")
    if stable_hash(sorted(hashes)) != spec["row_set_sha256"]:
        raise ValueError(f"source row-set root changed: {name}")
    return rows


def validate_source_bundle(
    source_root: Path,
    state: Mapping[str, Any],
    receipt: Mapping[str, Any],
    pack: Mapping[str, Any],
    pack_receipt: Mapping[str, Any],
    source_config: Mapping[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    for key, expected in {
        "research_only": True,
        "execution_eligible": False,
        "proof_eligible": False,
        "can_promote": False,
        "can_place_orders": False,
        "can_authorize": False,
        "broker_access": False,
        "account_access": False,
        "supported_decision": "no_trade",
    }.items():
        if state.get(key) != expected:
            raise ValueError(f"unsafe source state: {key}")
    if state.get("cohort_id") != source_config.get("required_cohort_id"):
        raise ValueError("wrong all-68 source cohort")
    # The frozen all-68 V1 state predates a persisted evidence_role field; its
    # exact state hash, proof_eligible=false boundary, and report contract bind
    # it as historical training/discovery.  Accept only that legacy absence or
    # the later explicit spelling.
    if state.get("evidence_role") not in (None, "historical_training_discovery"):
        raise ValueError("source evidence role changed")
    if receipt.get("verified") is not True or receipt.get("failures") != []:
        raise ValueError("all-68 source is not independently verified")
    if receipt.get("cohort_id") != state.get("cohort_id"):
        raise ValueError("all-68 state/receipt cohort mismatch")
    pack_id = str(source_config.get("required_source_pack_id") or "")
    if state.get("source_binding", {}).get("pack_id") != pack_id:
        raise ValueError("wrong exact source pack in batch state")
    if state.get("material_contract", {}).get("source_binding", {}).get("pack_id") != pack_id:
        raise ValueError("wrong exact source pack in batch material contract")
    if pack.get("pack_id") != pack_id or pack_receipt.get("pack_id") != pack_id:
        raise ValueError("source-pack identity mismatch")
    if pack_receipt.get("verified") is not True or pack_receipt.get("failures") != []:
        raise ValueError("source pack is not independently verified")
    if stable_hash(state.get("datasets")) != source_config.get("required_dataset_roots_sha256"):
        raise ValueError("source dataset-root contract changed")
    datasets = {
        name: load_dataset(source_root, state, name)
        for name in SOURCE_DATASETS
    }
    cohort_id = str(state["cohort_id"])
    for name, rows in datasets.items():
        if any(row.get("cohort_id") != cohort_id for row in rows):
            raise ValueError(f"cross-cohort source row: {name}")
    return datasets


def _action_exposure(
    action: Mapping[str, Any],
    state_before: Mapping[str, Any],
) -> tuple[str | None, int | None, list[str]]:
    action_name = str(action.get("action") or "")
    position = state_before.get("position") or {}
    instrument = action.get("instrument")
    side = action.get("side")
    if action_name in {"hold", "exit"} or not instrument or side not in {-1, 1}:
        instrument = position.get("instrument")
        side = position.get("side")
    if not instrument or side not in {-1, 1} or "_" not in str(instrument):
        return None, None, ["CASH:0"]
    base, quote = str(instrument).upper().split("_", 1)
    signed = [
        f"{base}:{'+1' if int(side) > 0 else '-1'}",
        f"{quote}:{'-1' if int(side) > 0 else '+1'}",
    ]
    return str(instrument), int(side), sorted(signed)


def _cost_proxy(
    execution: Mapping[str, Any],
    feedback_status: str,
    liquidation_slippage_pips: float,
) -> float:
    total = 0.0
    for leg in execution.get("legs") or []:
        total += abs(float(leg.get("spread_pips") or 0.0))
        total += abs(float(leg.get("slippage_pips") or 0.0))
    position = (execution.get("state_after") or {}).get("position")
    if position is not None and str(feedback_status).startswith("available"):
        total += abs(float(liquidation_slippage_pips))
    return total


def _review_payload(row: Mapping[str, Any], state_before: Mapping[str, Any], slippage: float) -> dict[str, Any]:
    action = dict(row["branch"])
    instrument, side, signed = _action_exposure(action, state_before)
    value = row.get("liquidation_equity_pips")
    return {
        "counterfactual_id": str(row["counterfactual_id"]),
        "branch_label": str(action.get("branch_label") or ""),
        "action": str(action.get("action") or ""),
        "instrument": instrument,
        "side": side,
        "signed_currency_resources": signed,
        "feedback_status": str(row.get("feedback_status") or ""),
        "terminal_equity_pips": _round(value),
        "execution_cost_proxy_pips": _round(
            _cost_proxy(row.get("execution") or {}, str(row.get("feedback_status") or ""), slippage)
        ),
        "source_counterfactual_row_sha256": str(row["row_sha256"]),
    }


def _best(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    available = [row for row in rows if row.get("terminal_equity_pips") is not None and str(row.get("feedback_status") or "").startswith("available")]
    if not available:
        return None
    return sorted(
        available,
        key=lambda row: (-float(row["terminal_equity_pips"]), str(row["branch_label"]), str(row["counterfactual_id"])),
    )[0]


def classify_primary(
    decision_row: Mapping[str, Any],
    feedback_row: Mapping[str, Any],
    reviews: Sequence[Mapping[str, Any]],
    *,
    threshold: float,
    neutral_brier: float,
    liquidation_slippage_pips: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    decision = dict(decision_row["decision"])
    state_before = dict(decision_row["state_before"])
    action = str(decision.get("action") or "")
    primary_value_raw = feedback_row.get("liquidation_equity_pips")
    primary_value = float(primary_value_raw) if primary_value_raw is not None else None
    best = _best(reviews)
    regret = (
        max(0.0, float(best["terminal_equity_pips"]) - float(primary_value))
        if best is not None and primary_value is not None
        else 0.0
    )
    cutoff = float(threshold) - 1e-12
    labels: list[dict[str, Any]] = []

    def add(category: str, reason: str, comparator: Mapping[str, Any] | None, materiality: float, **extra: Any) -> None:
        payload = {
            "category": category,
            "reason_code": reason,
            "comparator_counterfactual_id": comparator.get("counterfactual_id") if comparator else None,
            "materiality_pips": _round(max(0.0, materiality)),
        }
        payload.update(extra)
        labels.append(payload)

    if primary_value is not None and best is not None and regret >= cutoff:
        if action == "enter":
            add("entry", "entry_underperformed_best_depth_one", best, regret)

        instrument = decision.get("instrument")
        side = decision.get("side")
        flipped = _best([
            row for row in reviews
            if instrument and side in {-1, 1}
            and row.get("instrument") == instrument
            and row.get("side") == -int(side)
        ])
        if action in {"enter", "rotate"} and flipped is not None:
            flipped_regret = float(flipped["terminal_equity_pips"]) - primary_value
            if flipped_regret >= cutoff:
                add("direction", "flipped_same_instrument_outperformed", flipped, flipped_regret)

        if action == "rotate":
            hold_or_exit = _best([row for row in reviews if row.get("action") in {"hold", "exit"}])
            if hold_or_exit is not None:
                value = float(hold_or_exit["terminal_equity_pips"]) - primary_value
                if value >= cutoff:
                    add("rotation", "rotation_underperformed_hold_or_exit", hold_or_exit, value)

        if action == "hold":
            comparator = _best([row for row in reviews if row.get("action") in {"exit", "rotate"}])
            if comparator is not None:
                value = float(comparator["terminal_equity_pips"]) - primary_value
                if value >= cutoff:
                    add("management_exit", "hold_underperformed_exit_or_rotate", comparator, value)
        elif action == "exit":
            comparator = _best([row for row in reviews if row.get("action") == "hold"])
            if comparator is not None:
                value = float(comparator["terminal_equity_pips"]) - primary_value
                if value >= cutoff:
                    add("management_exit", "exit_underperformed_hold", comparator, value)

        current_position = (state_before.get("position") or {}).get("instrument")
        selected_instrument = decision.get("instrument") or current_position
        opportunity = _best([
            row for row in reviews
            if (
                row.get("action") in {"enter", "rotate"}
                and row.get("instrument")
                and row.get("instrument") != selected_instrument
            )
            or (action == "hold" and row.get("action") == "rotate")
            or (action == "rotate" and row.get("action") == "hold")
        ])
        if opportunity is not None:
            value = float(opportunity["terminal_equity_pips"]) - primary_value
            if value >= cutoff:
                add(
                    "opportunity_selection_hold_vs_rotate",
                    "alternative_instrument_or_hold_rotate_policy_outperformed",
                    opportunity,
                    value,
                )

    confidence = decision.get("confidence")
    baseline = _best([
        row for row in reviews
        if row.get("action") in ({"wait"} if state_before.get("position") is None else {"hold", "exit"})
    ])
    brier: float | None = None
    if (
        action in {"enter", "rotate", "hold"}
        and confidence is not None
        and primary_value is not None
        and baseline is not None
    ):
        target = 1.0 if primary_value > float(baseline["terminal_equity_pips"]) else 0.0
        brier = (float(confidence) - target) ** 2
        if brier > float(neutral_brier) + 1e-12:
            add(
                "calibration",
                "brier_worse_than_neutral",
                baseline,
                regret,
                brier=_round(brier),
                excess_brier=_round(brier - float(neutral_brier)),
                binary_target=int(target),
            )

    primary_cost = _cost_proxy(
        decision_row.get("execution") or {},
        str(feedback_row.get("status") or ""),
        liquidation_slippage_pips,
    )
    gross_relative_proxy: float | None = None
    after_cost_relative: float | None = None
    if action in {"enter", "rotate"} and primary_value is not None and baseline is not None:
        after_cost_relative = primary_value - float(baseline["terminal_equity_pips"])
        comparator_cost = float(baseline.get("execution_cost_proxy_pips") or 0.0)
        gross_relative_proxy = after_cost_relative + primary_cost - comparator_cost
        if after_cost_relative <= -cutoff:
            add(
                "cost",
                "failed_after_cost_clearance_vs_baseline",
                baseline,
                -after_cost_relative,
                after_cost_relative_pips=_round(after_cost_relative),
                gross_relative_proxy_pips=_round(gross_relative_proxy),
                gross_proxy_positive=bool(gross_relative_proxy > 0.0),
            )

    labels.sort(key=lambda row: (str(row["category"]), str(row["reason_code"]), str(row.get("comparator_counterfactual_id") or "")))
    diagnostics = {
        "best_alternative_counterfactual_id": best.get("counterfactual_id") if best else None,
        "best_alternative_equity_pips": best.get("terminal_equity_pips") if best else None,
        "regret_pips": _round(regret),
        "baseline_counterfactual_id": baseline.get("counterfactual_id") if baseline else None,
        "after_cost_relative_pips": _round(after_cost_relative),
        "gross_relative_proxy_pips": _round(gross_relative_proxy),
        "execution_cost_proxy_pips": _round(primary_cost),
        "brier": _round(brier),
    }
    return labels, diagnostics


def build_curriculum_rows(
    datasets: Mapping[str, Sequence[Mapping[str, Any]]],
    classification: Mapping[str, Any],
    *,
    liquidation_slippage_pips: float,
) -> list[dict[str, Any]]:
    clocks = {str(row["clock_id"]): row for row in datasets["clocks"]}
    feedback = {str(row["decision_id"]): row for row in datasets["feedback"]}
    counterfactuals: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in datasets["counterfactuals"]:
        counterfactuals[str(row["decision_id"])].append(row)
    output: list[dict[str, Any]] = []
    for decision_row in sorted(datasets["decisions"], key=lambda row: (int(row["sequence_no"]), str(row["decision_id"]))):
        decision_id = str(decision_row["decision_id"])
        clock = clocks[str(decision_row["clock_id"])]
        feedback_row = feedback[decision_id]
        state_before = dict(decision_row["state_before"])
        reviews = [
            _review_payload(row, state_before, liquidation_slippage_pips)
            for row in sorted(counterfactuals.get(decision_id, []), key=lambda value: str(value["counterfactual_id"]))
        ]
        labels, diagnostics = classify_primary(
            decision_row,
            feedback_row,
            reviews,
            threshold=float(classification["minimum_material_regret_pips"]),
            neutral_brier=float(classification["neutral_brier_baseline"]),
            liquidation_slippage_pips=liquidation_slippage_pips,
        )
        primary_action = dict(decision_row["decision"])
        instrument, side, signed = _action_exposure(primary_action, state_before)
        primary_identity = {
            "role": "primary_feedback",
            "decision_id": decision_id,
            "feedback_id": feedback_row["feedback_id"],
        }
        primary = {
            "curriculum_row_id": "a68mistake_" + stable_hash(primary_identity)[:28],
            "row_role": "primary_feedback",
            "curriculum_weight": int(classification["primary_feedback_weight"]),
            "counts_as_market_repetition": 1,
            "counts_as_regime_repetition": 0,
            "source_cohort_id": str(decision_row["cohort_id"]),
            "session_episode_id": str(decision_row["session_key"]),
            "clock_id": str(decision_row["clock_id"]),
            "sequence_no": int(decision_row["sequence_no"]),
            "decision_epoch": int(decision_row["decision_epoch"]),
            "decision_id": decision_id,
            "feedback_id": str(feedback_row["feedback_id"]),
            "counterfactual_id": None,
            "action": str(primary_action.get("action") or ""),
            "instrument": instrument,
            "side": side,
            "signed_currency_resources": signed,
            "feedback_status": str(feedback_row.get("status") or ""),
            "terminal_equity_pips": _round(feedback_row.get("liquidation_equity_pips")),
            "mistake_labels": labels,
            "diagnostics": diagnostics,
            "source_clock_row_sha256": str(clock["row_sha256"]),
            "source_decision_row_sha256": str(decision_row["row_sha256"]),
            "source_feedback_row_sha256": str(feedback_row["row_sha256"]),
            "source_counterfactual_row_sha256": None,
            "proof_eligible": False,
            "execution_eligible": False,
        }
        primary["row_sha256"] = row_hash(primary)
        output.append(primary)
        for review in reviews:
            review_identity = {
                "role": "depth_one_review",
                "decision_id": decision_id,
                "counterfactual_id": review["counterfactual_id"],
            }
            alternative = {
                "curriculum_row_id": "a68mistake_" + stable_hash(review_identity)[:28],
                "row_role": "depth_one_review",
                "curriculum_weight": int(classification["depth_one_alternative_weight"]),
                "counts_as_market_repetition": 0,
                "counts_as_regime_repetition": 0,
                "source_cohort_id": str(decision_row["cohort_id"]),
                "session_episode_id": str(decision_row["session_key"]),
                "clock_id": str(decision_row["clock_id"]),
                "sequence_no": int(decision_row["sequence_no"]),
                "decision_epoch": int(decision_row["decision_epoch"]),
                "decision_id": decision_id,
                "feedback_id": str(feedback_row["feedback_id"]),
                "counterfactual_id": review["counterfactual_id"],
                "action": review["action"],
                "instrument": review["instrument"],
                "side": review["side"],
                "signed_currency_resources": review["signed_currency_resources"],
                "feedback_status": review["feedback_status"],
                "terminal_equity_pips": review["terminal_equity_pips"],
                "mistake_labels": [],
                "diagnostics": {
                    "branch_label": review["branch_label"],
                    "execution_cost_proxy_pips": review["execution_cost_proxy_pips"],
                    "review_only": True,
                },
                "source_clock_row_sha256": str(clock["row_sha256"]),
                "source_decision_row_sha256": str(decision_row["row_sha256"]),
                "source_feedback_row_sha256": str(feedback_row["row_sha256"]),
                "source_counterfactual_row_sha256": review["source_counterfactual_row_sha256"],
                "proof_eligible": False,
                "execution_eligible": False,
            }
            alternative["row_sha256"] = row_hash(alternative)
            output.append(alternative)
    return sorted(output, key=lambda row: (int(row["sequence_no"]), 0 if row["row_role"] == "primary_feedback" else 1, str(row["curriculum_row_id"])))


class _DisjointSet:
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


def build_clusters(rows: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    output = [dict(row) for row in rows]
    grouped: dict[tuple[str, str], list[int]] = defaultdict(list)
    for index, row in enumerate(output):
        grouped[(str(row["session_episode_id"]), str(row["clock_id"]))].append(index)
    cluster_rows: list[dict[str, Any]] = []
    mapping: dict[str, str] = {}
    for (episode, clock_id), indices in sorted(grouped.items()):
        dsu = _DisjointSet(len(indices))
        currencies = [
            {str(resource).split(":", 1)[0] for resource in output[index]["signed_currency_resources"] if not str(resource).startswith("CASH:")}
            for index in indices
        ]
        for left in range(len(indices)):
            for right in range(left + 1, len(indices)):
                if currencies[left] and currencies[left] & currencies[right]:
                    dsu.union(left, right)
        components: dict[int, list[int]] = defaultdict(list)
        for local_index, global_index in enumerate(indices):
            components[dsu.find(local_index)].append(global_index)
        for component in components.values():
            members = sorted((output[index] for index in component), key=lambda row: str(row["curriculum_row_id"]))
            ids = [str(row["curriculum_row_id"]) for row in members]
            signed = sorted({str(value) for row in members for value in row["signed_currency_resources"]})
            labels = sorted({str(label["category"]) for row in members if row["row_role"] == "primary_feedback" for label in row["mistake_labels"]})
            cluster_id = "a68mistakecluster_" + stable_hash(episode, clock_id, signed, ids)[:28]
            for row in members:
                mapping[str(row["curriculum_row_id"])] = cluster_id
            cluster = {
                "cluster_id": cluster_id,
                "session_episode_id": episode,
                "clock_id": clock_id,
                "sequence_no": min(int(row["sequence_no"]) for row in members),
                "currency_resources": sorted({value.split(":", 1)[0] for value in signed if not value.startswith("CASH:")}),
                "signed_currency_resources": signed,
                "curriculum_row_ids": ids,
                "row_count": len(members),
                "primary_feedback_count": sum(row["row_role"] == "primary_feedback" for row in members),
                "zero_weight_review_count": sum(row["row_role"] == "depth_one_review" for row in members),
                "deduplicated_primary_weight": min(1, sum(int(row["curriculum_weight"]) for row in members)),
                "mistake_categories": labels,
                "counts_as_market_repetition": min(1, sum(int(row["curriculum_weight"]) for row in members)),
                "counts_as_regime_repetition": 0,
                "proof_eligible": False,
            }
            cluster["row_sha256"] = row_hash(cluster)
            cluster_rows.append(cluster)
    for row in output:
        row["structural_cluster_id"] = mapping[str(row["curriculum_row_id"])]
        row["row_sha256"] = row_hash(row)
    output.sort(key=lambda row: (int(row["sequence_no"]), 0 if row["row_role"] == "primary_feedback" else 1, str(row["curriculum_row_id"])))
    cluster_rows.sort(key=lambda row: (int(row["sequence_no"]), str(row["cluster_id"])))
    return output, cluster_rows


def build_mistake_clusters(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Cluster category observations across clocks inside predeclared episodes.

    Rows are already deduplicated at one primary weight per global clock.  This
    second layer prevents repeated USD/JPY/etc. manifestations in the same
    predeclared session from masquerading as separate structural evidence.
    Direction-conflicted exposures connect through the underlying currency,
    while their signed resources remain visible in the cluster.
    """

    observations: list[dict[str, Any]] = []
    for row in rows:
        if row["row_role"] != "primary_feedback":
            continue
        for label in row["mistake_labels"]:
            observation = {
                "observation_id": "a68mistakeobs_" + stable_hash(
                    row["curriculum_row_id"], label["category"], label["reason_code"]
                )[:28],
                "category": str(label["category"]),
                "reason_code": str(label["reason_code"]),
                "session_episode_id": str(row["session_episode_id"]),
                "clock_id": str(row["clock_id"]),
                "sequence_no": int(row["sequence_no"]),
                "curriculum_row_id": str(row["curriculum_row_id"]),
                "instrument": row.get("instrument"),
                "signed_currency_resources": list(row["signed_currency_resources"]),
                "materiality_pips": _round(label["materiality_pips"]),
            }
            observations.append(observation)
    dsu = _DisjointSet(len(observations))
    currencies = [
        {
            str(resource).split(":", 1)[0]
            for resource in row["signed_currency_resources"]
            if not str(resource).startswith("CASH:")
        }
        for row in observations
    ]
    for left in range(len(observations)):
        for right in range(left + 1, len(observations)):
            a, b = observations[left], observations[right]
            if a["category"] != b["category"] or a["session_episode_id"] != b["session_episode_id"]:
                continue
            if currencies[left] and currencies[left] & currencies[right]:
                dsu.union(left, right)
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for index, row in enumerate(observations):
        grouped[dsu.find(index)].append(row)
    output: list[dict[str, Any]] = []
    for group in grouped.values():
        group.sort(key=lambda row: (int(row["sequence_no"]), str(row["observation_id"])))
        category = str(group[0]["category"])
        episode = str(group[0]["session_episode_id"])
        observation_ids = sorted(str(row["observation_id"]) for row in group)
        signed = sorted({str(resource) for row in group for resource in row["signed_currency_resources"]})
        materialities = [float(row["materiality_pips"]) for row in group]
        representative = sorted(
            group,
            key=lambda row: (-float(row["materiality_pips"]), int(row["sequence_no"]), str(row["observation_id"])),
        )[0]
        cluster = {
            "cluster_id": "a68mistakestruct_" + stable_hash(category, episode, observation_ids)[:28],
            "category": category,
            "session_episode_id": episode,
            "observation_ids": observation_ids,
            "observation_count": len(group),
            "global_clock_ids": sorted({str(row["clock_id"]) for row in group}),
            "deduplicated_global_clock_count": len({str(row["clock_id"]) for row in group}),
            "curriculum_row_ids": sorted({str(row["curriculum_row_id"]) for row in group}),
            "currency_resources": sorted({value.split(":", 1)[0] for value in signed if not value.startswith("CASH:")}),
            "signed_currency_resources": signed,
            "instruments": sorted({str(row["instrument"]) for row in group if row.get("instrument")}),
            "reason_counts": dict(sorted(Counter(str(row["reason_code"]) for row in group).items())),
            "raw_materiality_pips": _round(sum(materialities)),
            "cluster_max_materiality_pips": _round(max(materialities)),
            "representative_observation_id": representative["observation_id"],
            "counts_as_regime_repetition": 0,
            "proof_eligible": False,
        }
        cluster["row_sha256"] = row_hash(cluster)
        output.append(cluster)
    return sorted(output, key=lambda row: (str(row["category"]), str(row["session_episode_id"]), str(row["cluster_id"])))


def build_report(
    *,
    source_state: Mapping[str, Any],
    source_binding: Mapping[str, Any],
    material_contract: Mapping[str, Any],
    cohort_id: str,
    classification: Mapping[str, Any],
    datasets: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    slippage = float(source_state["material_contract"]["costs"]["slippage_per_execution_leg_pips"])
    rows = build_curriculum_rows(datasets, classification, liquidation_slippage_pips=slippage)
    rows, feedback_components = build_clusters(rows)
    clusters = build_mistake_clusters(rows)
    primary = [row for row in rows if row["row_role"] == "primary_feedback"]
    reviews = [row for row in rows if row["row_role"] == "depth_one_review"]
    category_counts = Counter(
        str(label["category"])
        for row in primary
        for label in row["mistake_labels"]
    )
    reason_counts = Counter(
        str(label["reason_code"])
        for row in primary
        for label in row["mistake_labels"]
    )
    category_materiality: dict[str, float] = defaultdict(float)
    for row in primary:
        for label in row["mistake_labels"]:
            category_materiality[str(label["category"])] += float(label["materiality_pips"])
    categories = {}
    for name, definition in CATEGORY_DEFINITIONS.items():
        category_clusters = [row for row in clusters if row["category"] == name]
        categories[name] = {
            "definition": definition,
            "label_count": int(category_counts.get(name, 0)),
            "unique_primary_clock_count": len({row["clock_id"] for row in primary if any(label["category"] == name for label in row["mistake_labels"])}),
            "materiality_pips": _round(category_materiality.get(name, 0.0)),
            "structural_cluster_count": len(category_clusters),
            "cluster_max_materiality_pips": _round(sum(float(row["cluster_max_materiality_pips"]) for row in category_clusters)),
        }
    priorities = sorted(
        [
            {"category": name, "label_count": details["label_count"], "structural_cluster_count": details["structural_cluster_count"], "materiality_pips": details["materiality_pips"], "cluster_max_materiality_pips": details["cluster_max_materiality_pips"]}
            for name, details in categories.items()
            if details["label_count"]
        ],
        key=lambda row: (-float(row["cluster_max_materiality_pips"]), -int(row["structural_cluster_count"]), -float(row["materiality_pips"]), str(row["category"])),
    )
    report: dict[str, Any] = {
        "schema_version": 1,
        "contract_id": REPORT_CONTRACT_ID,
        "cohort_id": cohort_id,
        "generated_utc": datetime.fromtimestamp(
            max(int(row["feedback_epoch"]) for row in datasets["clocks"]),
            tz=timezone.utc,
        ).isoformat(),
        "material_contract_sha256": stable_hash(material_contract),
        "material_contract": dict(material_contract),
        **POLICY,
        "evidence_role": "historical_training_curriculum",
        "source_binding": dict(source_binding),
        "classification_contract": dict(classification),
        "summary": {
            "source_global_clock_count": len(datasets["clocks"]),
            "source_primary_feedback_count": len(primary),
            "source_depth_one_review_count": len(reviews),
            "curriculum_row_count": len(rows),
            "primary_weight_sum": sum(int(row["curriculum_weight"]) for row in primary),
            "review_weight_sum": sum(int(row["curriculum_weight"]) for row in reviews),
            "primary_clocks_with_any_mistake": sum(bool(row["mistake_labels"]) for row in primary),
            "nonexclusive_mistake_label_count": sum(len(row["mistake_labels"]) for row in primary),
            "feedback_resource_component_count": len(feedback_components),
            "structural_cluster_count": len(clusters),
            "deduplicated_primary_weight": sum(int(row["deduplicated_primary_weight"]) for row in feedback_components),
            "predeclared_session_episode_count": len({row["session_episode_id"] for row in primary}),
            "independent_regime_count": None,
        },
        "category_definitions": dict(CATEGORY_DEFINITIONS),
        "categories": categories,
        "reason_counts": dict(sorted(reason_counts.items())),
        "curriculum_priorities": priorities,
        "curriculum_rows": rows,
        "feedback_resource_components": feedback_components,
        "structural_clusters": clusters,
        "limitations": [
            "This is already-inspected historical training evidence and cannot prove, promote, authorize, or execute a policy.",
            "Primary feedback receives one unit of curriculum weight per global clock; every depth-one alternative/review receives zero.",
            "Connected-currency components collapse opposing signed exposures that share an underlying currency, preventing direction-conflicted factor inflation.",
            "Category labels intentionally overlap and cannot be summed as independent errors.",
            "Depth-one alternatives are local reviews, not a globally optimal portfolio path.",
            "The three predeclared session episodes are structural slices, not three proven independent regimes.",
        ],
    }
    report["report_sha256"] = stable_hash(report)
    report["report_id"] = "a68mistakecurriculum_" + report["report_sha256"][:28]
    return report


def render_markdown(report: Mapping[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Sequential All-68 Mistake Curriculum V1",
        "",
        "Status: independently verifiable historical training curriculum; nonexecuting, proof-ineligible, and no-trade.",
        "",
        f"- Cohort: `{report['cohort_id']}`",
        f"- Source batch: `{report['source_binding']['cohort_id']}`",
        f"- Source pack: `{report['source_binding']['source_pack_id']}`",
        f"- Primary feedback / zero-weight depth-one reviews: **{summary['source_primary_feedback_count']} / {summary['source_depth_one_review_count']}**",
        f"- Primary clocks with at least one mistake label: **{summary['primary_clocks_with_any_mistake']}**",
        f"- Nonexclusive labels / feedback components / mistake clusters: **{summary['nonexclusive_mistake_label_count']} / {summary['feedback_resource_component_count']} / {summary['structural_cluster_count']}**",
        f"- Deduplicated primary weight: **{summary['deduplicated_primary_weight']}**",
        "",
        "## Frozen priority order",
        "",
        "| Category | Labels | Structural clusters | Cluster-max materiality | Raw materiality |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in report["curriculum_priorities"]:
        lines.append(f"| {row['category']} | {row['label_count']} | {row['structural_cluster_count']} | {row['cluster_max_materiality_pips']:.2f} pips | {row['materiality_pips']:.2f} pips |")
    if not report["curriculum_priorities"]:
        lines.append("| none | 0 | 0 | 0.00 pips | 0.00 pips |")
    lines += [
        "",
        "The unit is predeclared session episode × global clock × connected currency-resource component.",
        "Signed exposure is retained, while opposing exposures sharing a currency remain one connected factor for deduplication.",
        "Alternatives and reviews remain inspectable with zero curriculum and repetition weight.",
        "Independent regime count remains unknown. This report cannot promote or execute anything.",
    ]
    return "\n".join(lines) + "\n"


def json_text(value: Mapping[str, Any]) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


__all__ = [
    "CATEGORY_DEFINITIONS",
    "POLICY",
    "REPORT_CONTRACT_ID",
    "SOURCE_DATASETS",
    "build_clusters",
    "build_mistake_clusters",
    "build_curriculum_rows",
    "build_report",
    "canonical_json",
    "file_sha256",
    "json_text",
    "load_dataset",
    "read_json",
    "render_markdown",
    "row_hash",
    "semantic_sha256",
    "stable_hash",
    "validate_source_bundle",
]
