#!/usr/bin/env python3
"""Small filesystem registry for forex model lifecycle state.

The registry is intentionally dumb and auditable.  It does not train, trade, or
call broker APIs.  It records candidate state and publishes the small manifests
that account managers are allowed to consume.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


LIFECYCLE_STAGES = [
    "proposed",
    "screened",
    "validated",
    "shadow",
    "canary",
    "production",
    "retired",
    "rejected",
]

PROMOTION_FILENAMES = {
    "research_leader": "research_leader.json",
    "shadow_candidate": "shadow_candidate.json",
    "canary_candidate": "canary_candidate.json",
    "technical_production": "technical_production.json",
    "major_move_factor_shadow": "major_move_factor_shadow.json",
    "major_move_factor_production": "major_move_factor_production.json",
}


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str), encoding="utf-8")
    os.replace(tmp, path)


def read_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def canonical_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """Return the identity-defining part of a research spec.

    Evaluation-stage fields are deliberately excluded so the same idea can move
    through screen -> validation -> shadow without becoming a new candidate.
    """
    return {
        key: spec.get(key)
        for key in [
            "dataset_kind",
            "model_type",
            "target",
            "outcome",
            "feature_set",
            "instrument_subset",
            "instrument_whitelist",
            "segment_filters",
            "research_role",
            "direction_target",
            "execution_policy",
            "specialist_profile",
            "specialist_parent_experiment_id",
            "parameters",
        ]
        if key in spec
    }


def candidate_id(spec: Dict[str, Any]) -> str:
    return hashlib.sha256(stable_json(canonical_spec(spec)).encode("utf-8")).hexdigest()


def experiment_spec_hash(spec: Dict[str, Any]) -> str:
    identity = {
        **canonical_spec(spec),
        "evaluation_stage": spec.get("evaluation_stage", "screen"),
        "validation_weeks": spec.get("validation_weeks"),
        "max_train_rows": spec.get("max_train_rows"),
    }
    return hashlib.sha256(stable_json(identity).encode("utf-8")).hexdigest()


def validation_spec_for_screen_pass(spec: Dict[str, Any]) -> Dict[str, Any]:
    """Return the validation spec that should follow a passed screen.

    The default validation profile is intentionally moderate.  Candidates that
    are closer to the live technical/primary lanes or that rely on path/return
    curve quality need a longer walk-forward check before they can influence any
    shadow, canary, or production manifest.
    """
    target = str(spec.get("target") or "").lower()
    role = str(spec.get("research_role") or "").lower()
    subset = str(spec.get("instrument_subset") or "").lower()
    dataset_kind = str(spec.get("dataset_kind") or "").lower()
    deployment_lane = str(spec.get("deployment_lane") or "").lower()
    is_major_move = (
        target.startswith("major_event")
        or role == "major_move_factor"
    )

    weeks = 8
    rows = 250_000
    reasons = ["default_8w_after_screen_pass"]

    if is_major_move:
        reasons.append("major_move_factor_kept_standard_to_limit_timeout_risk")
    else:
        if role == "return_curve_candidate" or "_curve_profit_" in target:
            weeks = max(weeks, 10)
            rows = max(rows, 300_000)
            reasons.append("return_curve_requires_longer_path_validation")
        if dataset_kind == "profitable_move_precursor" and subset not in {
            "all",
            "majors",
        }:
            weeks = max(weeks, 10)
            reasons.append("pair_family_precursor_requires_broader_holdout")
        if deployment_lane == "live_tech_champion_candidate":
            weeks = max(weeks, 12)
            rows = max(rows, 350_000)
            reasons.append("live_tech_champion_requires_12w_validation")
        elif deployment_lane == "live_primary_challenger_candidate":
            weeks = max(weeks, 10)
            rows = max(rows, 300_000)
            reasons.append("live_primary_challenger_requires_10w_validation")
        if subset in {
            "exotic_high_spread",
            "volatile_non_usd",
            "jpy_risk",
        }:
            weeks = max(weeks, 10)
            reasons.append("specialist_pair_family_requires_stability_check")

    return {
        **spec,
        "evaluation_stage": "validation",
        "validation_weeks": weeks,
        "max_train_rows": rows,
        "validation_profile": "screen_pass_robust_v2",
        "validation_profile_reason": ";".join(reasons),
    }


def should_enqueue_validation_followup(
    spec: Dict[str, Any],
    result: Dict[str, Any],
) -> tuple[bool, str]:
    """Guard long validations from near-zero live-lane screen wins.

    A screen pass is useful evidence, but live-account candidate lanes should
    not automatically spend long validation windows on statistically neat,
    economically tiny edges.  These thresholds are deliberately modest and can
    be tuned through environment variables without changing code.
    """
    deployment_lane = str(spec.get("deployment_lane") or "").lower()
    if deployment_lane not in {
        "live_tech_champion_candidate",
        "live_primary_challenger_candidate",
    }:
        return True, ""
    selected = result.get("selected_threshold") if isinstance(result, dict) else {}
    if not isinstance(selected, dict):
        selected = {}
    min_mean_net = safe_float(
        os.getenv("OANDA_LIVE_LANE_MIN_SCREEN_MEAN_NET_PIPS"),
        0.05,
    )
    min_bootstrap_lower = safe_float(
        os.getenv("OANDA_LIVE_LANE_MIN_SCREEN_BOOTSTRAP_LOWER_PIPS"),
        -0.02,
    )
    min_profit_factor = safe_float(
        os.getenv("OANDA_LIVE_LANE_MIN_SCREEN_PROFIT_FACTOR"),
        1.15,
    )
    mean_net = safe_float(selected.get("mean_net_pips"), -1.0)
    bootstrap_lower = safe_float(
        selected.get("bootstrap_lower_mean_net_pips"),
        -1.0,
    )
    profit_factor = safe_float(selected.get("median_profit_factor"), 0.0)
    if mean_net < min_mean_net:
        return (
            False,
            f"mean_net_pips {mean_net:.5f} below {min_mean_net:.5f}",
        )
    if bootstrap_lower < min_bootstrap_lower:
        return (
            False,
            "bootstrap_lower_mean_net_pips "
            f"{bootstrap_lower:.5f} below {min_bootstrap_lower:.5f}",
        )
    if profit_factor < min_profit_factor:
        return (
            False,
            f"median_profit_factor {profit_factor:.5f} below {min_profit_factor:.5f}",
        )
    return True, ""


def normalized_instrument_whitelist(value: Any) -> List[str]:
    if value in ("", None):
        return []
    if isinstance(value, str):
        raw_values = [part.strip() for part in value.replace(";", ",").split(",")]
    elif isinstance(value, (list, tuple, set)):
        raw_values = [str(part).strip() for part in value]
    else:
        return []
    out: List[str] = []
    for raw in raw_values:
        instrument = raw.upper().replace("/", "_").replace("-", "_")
        if "_" not in instrument:
            continue
        base, quote = instrument.split("_", 1)
        if len(base) != 3 or len(quote) != 3:
            continue
        out.append(f"{base}_{quote}")
    return list(dict.fromkeys(out))


def concentrated_validation_specialist_specs(
    spec: Dict[str, Any],
    result: Dict[str, Any],
    *,
    experiment_id: str,
) -> List[Dict[str, Any]]:
    """Derive pair/basket follow-ups when a broad validation only failed breadth.

    A high-AUC/high-week-stability model can fail the broad gate because one
    low-value instrument supplies too many selected trades.  In that case the
    right next step is not promotion; it is a narrower specialist test on the
    instruments that actually contributed value.
    """
    deployment_lane = str(spec.get("deployment_lane") or "").lower()
    if deployment_lane not in {
        "live_tech_champion_candidate",
        "live_primary_challenger_candidate",
    }:
        return []
    if str(spec.get("evaluation_stage") or "").lower() != "validation":
        return []
    if normalized_instrument_whitelist(spec.get("instrument_whitelist")):
        return []
    gate = result.get("gate") if isinstance(result, dict) else {}
    if not isinstance(gate, dict) or bool(gate.get("passed", False)):
        return []
    failed_keys = {
        key
        for key, value in gate.items()
        if key != "passed" and isinstance(value, bool) and not value
    }
    concentration_keys = {
        "top_pair_trade_share_at_most_35pct",
        "top_pair_trade_share_at_configured_limit",
    }
    if not failed_keys or not failed_keys.issubset(concentration_keys):
        return []
    selected = result.get("selected_threshold")
    if not isinstance(selected, dict):
        selected = {}
    if safe_float(selected.get("mean_net_pips")) <= 0:
        return []
    if safe_float(selected.get("bootstrap_lower_mean_net_pips")) <= 0:
        return []
    scorecards = result.get("scorecards")
    if not isinstance(scorecards, dict):
        return []
    instrument_rows = scorecards.get("instrument")
    if not isinstance(instrument_rows, list):
        return []
    eligible: List[Dict[str, Any]] = []
    for row in instrument_rows:
        if not isinstance(row, dict):
            continue
        instrument = str(row.get("segment") or "").upper().replace("/", "_")
        if "_" not in instrument:
            continue
        trades = safe_float(row.get("trades"))
        mean_net = safe_float(row.get("mean_net"))
        profit_factor = safe_float(row.get("profit_factor"))
        total_net = safe_float(row.get("total_net"))
        if trades < 20 or mean_net <= 0.03 or profit_factor < 1.20 or total_net <= 0:
            continue
        eligible.append({
            "instrument": instrument,
            "trades": trades,
            "mean_net": mean_net,
            "profit_factor": profit_factor,
            "total_net": total_net,
            "rank_score": total_net * max(min(profit_factor, 5.0), 0.0),
        })
    eligible.sort(
        key=lambda row: (
            safe_float(row.get("rank_score")),
            safe_float(row.get("total_net")),
            safe_float(row.get("trades")),
        ),
        reverse=True,
    )
    if not eligible:
        return []
    base = {
        **spec,
        "source": spec.get("source") or "model_space_agenda",
        "evaluation_stage": "screen",
        "validation_weeks": max(6, min(8, int(safe_float(spec.get("validation_weeks"), 6)))),
        "max_train_rows": min(max(int(safe_float(spec.get("max_train_rows"), 150000)), 100000), 180000),
        "specialist_parent_experiment_id": experiment_id,
        "agenda_reason": (
            str(spec.get("agenda_reason") or "")
            + ";concentrated_validation_value_specialist_followup"
        ).strip(";"),
    }
    out: List[Dict[str, Any]] = []
    basket = [row["instrument"] for row in eligible[:5]]
    if len(basket) >= 2:
        out.append({
            **base,
            "instrument_whitelist": basket,
            "specialist_profile": "value_instrument_basket_v1",
        })
    for row in eligible[:3]:
        out.append({
            **base,
            "instrument_whitelist": [row["instrument"]],
            "specialist_profile": "single_instrument_value_v1",
        })
    return out


class ModelLifecycleRegistry:
    def __init__(self, root: Path, promotions_root: Path) -> None:
        self.root = Path(root)
        self.promotions_root = Path(promotions_root)
        self.candidates_dir = self.root / "candidates"
        self.queue_path = self.root / "research_queue.jsonl"
        self.index_path = self.root / "index.json"
        self.root.mkdir(parents=True, exist_ok=True)
        self.candidates_dir.mkdir(parents=True, exist_ok=True)
        self.promotions_root.mkdir(parents=True, exist_ok=True)

    def candidate_path(self, cid: str) -> Path:
        return self.candidates_dir / f"{cid}.json"

    def load_candidate(self, cid: str) -> Dict[str, Any]:
        return read_json(self.candidate_path(cid), {})

    def save_candidate(self, record: Dict[str, Any]) -> None:
        cid = str(record["candidate_id"])
        atomic_write_json(self.candidate_path(cid), record)
        self.rebuild_index()

    def rebuild_index(self) -> None:
        rows = []
        for path in sorted(self.candidates_dir.glob("*.json")):
            record = read_json(path, {})
            if not isinstance(record, dict) or not record.get("candidate_id"):
                continue
            rows.append({
                "candidate_id": record.get("candidate_id"),
                "stage": record.get("stage"),
                "target": (record.get("spec") or {}).get("target"),
                "dataset_kind": (record.get("spec") or {}).get("dataset_kind"),
                "updated_utc": record.get("updated_utc"),
                "latest_experiment_id": record.get("latest_experiment_id"),
                "latest_score": record.get("latest_score"),
                "latest_gate_passed": record.get("latest_gate_passed"),
            })
        atomic_write_json(self.index_path, {"updated_utc": utc_iso(), "candidates": rows})

    def register_proposal(self, spec: Dict[str, Any], *, source: str = "generated") -> Dict[str, Any]:
        cid = candidate_id(spec)
        record = self.load_candidate(cid)
        now = utc_iso()
        if not record:
            record = {
                "candidate_id": cid,
                "created_utc": now,
                "updated_utc": now,
                "source": source,
                "stage": "proposed",
                "spec": canonical_spec(spec),
                "history": [],
            }
        else:
            record["updated_utc"] = now
        self.save_candidate(record)
        return record

    def append_history(self, cid: str, event: Dict[str, Any]) -> Dict[str, Any]:
        record = self.load_candidate(cid)
        if not record:
            raise KeyError(f"unknown lifecycle candidate {cid}")
        record.setdefault("history", []).append({"time_utc": utc_iso(), **event})
        record["updated_utc"] = utc_iso()
        self.save_candidate(record)
        return record

    def set_stage(
        self,
        cid: str,
        stage: str,
        *,
        reason: str = "",
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if stage not in LIFECYCLE_STAGES:
            raise ValueError(f"invalid lifecycle stage {stage!r}")
        record = self.load_candidate(cid)
        if not record:
            raise KeyError(f"unknown lifecycle candidate {cid}")
        record["stage"] = stage
        record["updated_utc"] = utc_iso()
        record.setdefault("history", []).append({
            "time_utc": utc_iso(),
            "event_type": "stage_transition",
            "stage": stage,
            "reason": reason,
            "payload": payload or {},
        })
        self.save_candidate(record)
        return record

    def enqueue(self, spec: Dict[str, Any], *, reason: str = "") -> None:
        self.register_proposal(spec, source=str(spec.get("source") or "queued"))
        line = {
            "time_utc": utc_iso(),
            "reason": reason,
            "spec": spec,
            "candidate_id": candidate_id(spec),
            "spec_hash": experiment_spec_hash(spec),
        }
        self.queue_path.parent.mkdir(parents=True, exist_ok=True)
        with self.queue_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(line, sort_keys=True, default=str) + "\n")

    def queued_specs(self) -> List[Dict[str, Any]]:
        if not self.queue_path.exists():
            return []
        out: List[Dict[str, Any]] = []
        for raw in self.queue_path.read_text(encoding="utf-8").splitlines():
            if not raw.strip():
                continue
            try:
                item = json.loads(raw)
            except Exception:
                continue
            spec = item.get("spec")
            if isinstance(spec, dict):
                out.append(spec)
        return out

    def next_queued_spec(self, completed_hashes: Iterable[str]) -> Optional[Dict[str, Any]]:
        completed = set(completed_hashes)
        for spec in self.queued_specs():
            if experiment_spec_hash(spec) not in completed:
                return spec
        return None

    def publish_manifest(self, name: str, manifest: Dict[str, Any]) -> Path:
        filename = PROMOTION_FILENAMES[name]
        path = self.promotions_root / filename
        atomic_write_json(path, {"published_utc": utc_iso(), **manifest})
        return path

    def record_evaluation(
        self,
        spec: Dict[str, Any],
        *,
        experiment_id: str,
        status: str,
        result: Dict[str, Any],
        artifact_path: str = "",
        detail_path: str = "",
        error: str = "",
    ) -> Dict[str, Any]:
        record = self.register_proposal(spec, source=str(spec.get("source") or "generated"))
        cid = str(record["candidate_id"])
        evaluation_stage = str(spec.get("evaluation_stage") or "screen")
        gate = result.get("gate", {}) if isinstance(result, dict) else {}
        passed = bool(gate.get("passed", False))
        if status == "error":
            next_stage = "rejected"
        elif evaluation_stage == "screen":
            next_stage = "screened" if passed else "rejected"
        elif evaluation_stage == "validation":
            next_stage = "validated" if passed else "rejected"
        else:
            next_stage = record.get("stage", "proposed")

        record.update({
            "updated_utc": utc_iso(),
            "stage": next_stage,
            "latest_experiment_id": experiment_id,
            "latest_score": result.get("score") if isinstance(result, dict) else None,
            "latest_gate_passed": passed,
            "latest_status": status,
            "latest_artifact_path": artifact_path,
            "latest_detail_path": detail_path,
            "latest_error": error,
        })
        record.setdefault("history", []).append({
            "time_utc": utc_iso(),
            "event_type": "evaluation",
            "evaluation_stage": evaluation_stage,
            "experiment_id": experiment_id,
            "status": status,
            "gate_passed": passed,
            "score": result.get("score") if isinstance(result, dict) else None,
            "artifact_path": artifact_path,
            "detail_path": detail_path,
            "error": error[:1000],
        })
        if evaluation_stage == "screen" and passed:
            should_enqueue, skip_reason = should_enqueue_validation_followup(
                spec,
                result,
            )
            if should_enqueue:
                validation_spec = validation_spec_for_screen_pass(spec)
                self.enqueue(validation_spec, reason=f"screen_passed:{experiment_id}")
            else:
                record["validation_followup_skipped_reason"] = skip_reason
                record.setdefault("history", []).append({
                    "time_utc": utc_iso(),
                    "event_type": "validation_followup_skipped",
                    "evaluation_stage": evaluation_stage,
                    "experiment_id": experiment_id,
                    "reason": skip_reason,
                })
        elif evaluation_stage == "validation" and not passed and status != "error":
            specialist_specs = concentrated_validation_specialist_specs(
                spec,
                result,
                experiment_id=experiment_id,
            )
            if specialist_specs:
                record.setdefault("history", []).append({
                    "time_utc": utc_iso(),
                    "event_type": "specialist_followups_enqueued",
                    "evaluation_stage": evaluation_stage,
                    "experiment_id": experiment_id,
                    "count": len(specialist_specs),
                    "profiles": [
                        followup.get("specialist_profile", "")
                        for followup in specialist_specs
                    ],
                    "instrument_whitelists": [
                        followup.get("instrument_whitelist", [])
                        for followup in specialist_specs
                    ],
                })
                for followup in specialist_specs:
                    self.enqueue(
                        followup,
                        reason=f"concentrated_validation_specialist:{experiment_id}",
                    )
        self.save_candidate(record)
        return record
