"""Run the residual layer on an immutable saved currency-projection packet."""
import argparse
import hashlib
import json
from pathlib import Path

from currency_projection_residual_layer_v2 import apply, fit_snapshot, score


CONTRACT = {"minimum_distinct_origins": 4, "minimum_distinct_utc_days": 1,
            "minimum_residual_weight": 0.0, "maximum_residual_weight": 1.0}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_inputs(projection_run, extension):
    projections = []
    sources = []
    for path in sorted(projection_run.glob("projection_*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        projections.extend(payload["predictions"])
        sources.append({"path": path.name, "sha256": sha256(path)})
    outcomes = {}
    for path in sorted(extension.glob("pair_*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for outcome in payload["outcomes"]:
            key = outcome["record_id"], outcome["target_id"]
            if key in outcomes and outcomes[key] != outcome:
                raise ValueError("residual_operator_conflicting_outcome")
            outcomes[key] = outcome
    return projections, outcomes, sources


def run(projection_run, extension, asof):
    rows, outcomes, source_frames = load_inputs(projection_run, extension)
    by_origin = {}
    for row in rows:
        by_origin.setdefault(row["origin_epoch"], []).append(row)
    learned, snapshots, unavailable = [], [], []
    for origin in sorted(by_origin):
        current = by_origin[origin]
        fitted = fit_snapshot(rows, outcomes, origin, CONTRACT)
        snapshots.extend(fitted.values())
        current_learned = apply(current, fitted)
        learned.extend(current_learned)
        available = {(row["base_method"], row["horizon_minutes"]) for row in current_learned}
        for scope, snapshot in fitted.items():
            if scope not in available:
                unavailable.append({"origin_epoch": origin, "base_method": scope[0], "horizon_minutes": scope[1], "status": snapshot["status"]})
    learned_keys = {(row["base_method"], row["horizon_minutes"], row["record_id"]) for row in learned}
    controls = {variant: [row for row in rows if row["variant"] == variant
                 and (row["base_method"], row["horizon_minutes"], row["record_id"]) in learned_keys]
                for variant in ("direct", "currency_projection", "half_residual")}
    return {"schema_version": "forex_currency_projection_residual_run.v1", "contract": CONTRACT,
            "source_frames": source_frames, "source_frame_count": len(source_frames), "source_prediction_rows": len(rows),
            "outcome_rows": len(outcomes), "asof": asof, "learned_rows": learned, "snapshots": snapshots,
            "unavailable": unavailable, "learned_scores": score(learned, outcomes, asof),
            "control_scores": {variant: score(part, outcomes, asof) for variant, part in controls.items()},
            "scope": "offline retrospective development diagnostic; no confirmation, policy, native issuance, or trading claim"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--projection-run", type=Path, required=True)
    parser.add_argument("--extension", type=Path, required=True)
    parser.add_argument("--asof", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run(args.projection_run, args.extension, args.asof)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
