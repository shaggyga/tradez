"""Authenticated retained-data runner for the bounded curve-shape layer."""
import argparse
import json
from collections import defaultdict
from pathlib import Path

from curve_shape_layer_v2 import apply, fit_snapshot, join_curve
from currency_projection_residual_operator_v2 import _read_json, load_inputs


def run(parent_recipe, parent_recipe_sha256, parent_paths, projection_run, asof, contract):
    rows, outcomes, sources, parent, recipe = load_inputs(
        parent_recipe, parent_recipe_sha256, parent_paths, projection_run)
    horizons = contract["feature_definition"]["horizons_minutes"]
    base_rows = [row for row in rows if row["variant"] == "direct"]
    curves = join_curve(base_rows, horizons)
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
            for row in current:
                value = apply(row, snapshot)
                coverage.append({"origin_epoch": origin, "base_method": base, "record_id": row["record_id"],
                                 "issued": value is not None, "status": snapshot["status"],
                                 "available_epoch": row["available_epoch"]})
                if value is not None:
                    learned.append(value)
    return {"schema_version": "forex_curve_shape_run.v1", "contract": contract, "parent": parent,
            "source_frames": sources, "asof": asof, "curve_rows": len(curves), "learned_rows": learned,
            "snapshots": snapshots, "coverage": coverage,
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
