"""Authenticated retained-data runner for the bounded curve-shape layer."""
import argparse
import json
from collections import defaultdict
from pathlib import Path

from curve_shape_layer_v2 import apply, fit_snapshot, join_curve
from currency_projection_residual_operator_v2 import _read_json, load_inputs


def _paired_scores(learned, direct, outcomes, asof, anchor_horizon):
    controls = {(row["record_id"], row["base_method"]): row for row in direct
                if row["horizon_minutes"] == anchor_horizon}
    groups = defaultdict(list)
    for row in learned:
        outcome = outcomes.get((row["record_id"], row["target_id"]))
        control = controls.get((row["record_id"], row["base_method"]))
        if outcome is None or control is None or outcome["value"] is None:
            continue
        if max(row["available_epoch"], control["available_epoch"], outcome["available_epoch"]) > asof:
            continue
        for kind, value in (("overall", None), ("origin", row["decision_epoch"]), ("utc_day", row["decision_epoch"] // 86400)):
            groups[row["base_method"], kind, value].append((row, control, outcome))
    result = []
    for (base, kind, value), part in sorted(groups.items()):
        learned_errors = [row["prediction_bps"] - outcome["value"] for row, _, outcome in part]
        control_errors = [control["prediction_bps"] - outcome["value"] for _, control, outcome in part]
        n = len(part)
        result.append({"base_method": base, "stratum": kind, "stratum_value": value, "mature_rows": n,
                       "support_sha256": __import__("hashlib").sha256(json.dumps(sorted(row["record_id"] for row, _, _ in part)).encode()).hexdigest(),
                       "mae_delta_bps": sum(map(abs, learned_errors)) / n - sum(map(abs, control_errors)) / n,
                       "mse_delta_bps2": sum(x*x for x in learned_errors) / n - sum(x*x for x in control_errors) / n,
                       "bias_delta_bps": sum(learned_errors) / n - sum(control_errors) / n})
    return result


def run(parent_recipe, parent_recipe_sha256, parent_paths, projection_run, asof, contract):
    rows, outcomes, sources, parent, recipe = load_inputs(
        parent_recipe, parent_recipe_sha256, parent_paths, projection_run)
    horizons = contract["feature_definition"]["horizons_minutes"]
    anchor_horizon = contract["feature_definition"]["anchor_horizon_minutes"]
    base_rows = [row for row in rows if row["variant"] == "direct"]
    curves = join_curve(base_rows, horizons, anchor_horizon)
    by_origin = defaultdict(list)
    for row in curves:
        by_origin[row["decision_epoch"]].append(row)
    learned, snapshots, coverage = [], [], []
    expected_instruments = recipe["contract"]["universe"]
    for origin in recipe["contract"]["origins"]:
        for base in recipe["contract"]["bases"]:
            history = [row for row in curves if row["base_method"] == base]
            current = [row for row in by_origin[origin] if row["base_method"] == base]
            snapshot = fit_snapshot(history, outcomes, origin, contract["layer_fit"])
            snapshots.append({"origin_epoch": origin, "base_method": base, **snapshot})
            for row in current:
                value = apply(row, snapshot)
                coverage.append({"origin_epoch": origin, "base_method": base, "record_id": row["record_id"],
                                 "issued": value is not None, "status": snapshot["status"],
                                 "available_epoch": row["available_epoch"], "instrument": row["instrument"],
                                 "target_id": row["target_id"]})
                if value is not None:
                    learned.append(value)
            current_by_instrument = {row["instrument"]: row for row in current}
            for instrument in expected_instruments:
                if instrument in current_by_instrument:
                    continue
                coverage.append({"origin_epoch": origin, "base_method": base,
                                 "instrument": instrument, "record_id": f"{instrument}:{origin}",
                                 "issued": False, "status": "no_complete_parent_curve",
                                 "available_epoch": None, "target_id": None})
    for item in coverage:
        outcome = outcomes.get((item["record_id"], item.get("target_id")))
        item["label_status"] = ("unavailable" if outcome is None or outcome["value"] is None else
                                "mature" if outcome["available_epoch"] <= asof else "unresolved")
        item["native_policy_status"] = "not_admitted_offline_diagnostic"
    return {"schema_version": "forex_curve_shape_run.v1", "contract": contract, "parent": parent,
            "source_frames": sources, "asof": asof, "curve_rows": len(curves), "learned_rows": learned,
            "snapshots": snapshots, "coverage": coverage,
            "paired_scores": _paired_scores(learned, base_rows, outcomes, asof, anchor_horizon),
            "scope": "offline retrospective development diagnostic; no native issuance, confirmation, or trading claim"}


def main():
    parser = argparse.ArgumentParser()
    for name in ("parent-recipe", "parent-paths", "projection-run", "contract", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--parent-recipe-sha256", required=True)
    parser.add_argument("--asof", type=int, required=True)
    args = parser.parse_args()
    result = run(args.parent_recipe, args.parent_recipe_sha256, _read_json(args.parent_paths),
                 args.projection_run, args.asof, _read_json(args.contract))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
