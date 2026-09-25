"""Causal curve-shape layer over authenticated prequential residual forecasts."""
import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path

from contracts import fingerprint
from curve_shape_layer_v2 import apply, fit_snapshot, join_curve
from curve_shape_operator_v2 import _paired_scores
from currency_projection_residual_operator_v2 import _read_json, load_inputs


def _sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _authenticated_residual(path, expected_sha256, expected_parent):
    path = Path(path)
    if _sha256(path) != expected_sha256:
        raise ValueError("residual_curve_shape_residual_result_pin_mismatch")
    result = _read_json(path)
    if result.get("parent") != expected_parent:
        raise ValueError("residual_curve_shape_parent_lineage_mismatch")
    rows = result.get("learned_rows")
    if not isinstance(rows, list) or not rows:
        raise ValueError("residual_curve_shape_missing_residual_rows")
    for row in rows:
        if row.get("variant") != "learned_residual":
            raise ValueError("residual_curve_shape_wrong_residual_variant")
        if row.get("forecast_id") != fingerprint({k: v for k, v in row.items() if k != "forecast_id"}):
            raise ValueError("residual_curve_shape_residual_forecast_identity")
    return rows


def run(parent_recipe, parent_recipe_sha256, parent_paths, projection_run, residual_result,
        residual_result_sha256, asof, contract):
    _, outcomes, sources, parent, recipe = load_inputs(
        parent_recipe, parent_recipe_sha256, parent_paths, projection_run)
    rows = _authenticated_residual(residual_result, residual_result_sha256, parent)
    horizons = contract["inputs"]["horizons_minutes"]
    anchor = contract["inputs"]["anchor_horizon_minutes"]
    curves = join_curve(rows, horizons, anchor)
    by_origin = defaultdict(list)
    for row in curves:
        by_origin[row["decision_epoch"]].append(row)
    learned, snapshots, coverage = [], [], []
    for origin in recipe["contract"]["origins"]:
        for base in recipe["contract"]["bases"]:
            history = [row for row in curves if row["base_method"] == base]
            current = [row for row in by_origin[origin] if row["base_method"] == base]
            snapshot = fit_snapshot(history, outcomes, origin, contract["layer_fit"])
            snapshots.append({"origin_epoch": origin, "base_method": base, **snapshot})
            current_by_instrument = {row["instrument"]: row for row in current}
            for instrument in recipe["contract"]["universe"]:
                row = current_by_instrument.get(instrument)
                if row is None:
                    coverage.append({"origin_epoch": origin, "base_method": base, "instrument": instrument,
                                     "record_id": f"{instrument}:{origin}", "issued": False,
                                     "status": "no_complete_residual_curve", "available_epoch": None,
                                     "target_id": None, "label_status": "unavailable",
                                     "native_policy_status": "not_admitted_offline_diagnostic"})
                    continue
                value = apply(row, snapshot)
                coverage_row = {"origin_epoch": origin, "base_method": base, "instrument": instrument,
                                "record_id": row["record_id"], "issued": value is not None,
                                "status": snapshot["status"], "available_epoch": row["available_epoch"],
                                "target_id": row["target_id"]}
                outcome = outcomes.get((row["record_id"], row["target_id"]))
                coverage_row["label_status"] = ("unavailable" if outcome is None or outcome["value"] is None else
                                                "mature" if outcome["available_epoch"] <= asof else "unresolved")
                coverage_row["native_policy_status"] = "not_admitted_offline_diagnostic"
                coverage.append(coverage_row)
                if value is not None:
                    value.pop("forecast_id")
                    value["variant"] = "residual_curve_shape"
                    value["method"] = base + "__residual_curve_shape"
                    value["parent_residual_forecast_id"] = row["forecast_id"]
                    value["forecast_id"] = fingerprint(value)
                    learned.append(value)
    return {"schema_version": "forex_residual_curve_shape_run.v1", "contract": contract,
            "parent": parent, "residual_result_sha256": residual_result_sha256,
            "source_frames": sources, "asof": asof, "curve_rows": len(curves),
            "learned_rows": learned, "snapshots": snapshots, "coverage": coverage,
            "paired_scores": _paired_scores(learned, rows, outcomes, asof, anchor),
            "scope": "offline retrospective development diagnostic; no confirmation, native issuance, policy, or trading claim"}


def main():
    parser = argparse.ArgumentParser()
    for name in ("parent-recipe", "parent-paths", "projection-run", "residual-result", "contract", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--parent-recipe-sha256", required=True)
    parser.add_argument("--residual-result-sha256", required=True)
    parser.add_argument("--asof", type=int, required=True)
    args = parser.parse_args()
    result = run(args.parent_recipe, args.parent_recipe_sha256, _read_json(args.parent_paths),
                 args.projection_run, args.residual_result, args.residual_result_sha256,
                 args.asof, _read_json(args.contract))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
