"""Authenticated, prequential residual-layer diagnostic over a sealed projection run."""
import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path

from currency_projection_residual_layer_v2 import CONTROLS, apply, fit_snapshot, score


CONTRACT = {"minimum_distinct_origins": 8, "minimum_distinct_utc_days": 3,
            "minimum_distinct_pairs": 20, "minimum_residual_weight": 0.0,
            "maximum_residual_weight": 1.0}
CONTRACT_PATH = Path(__file__).with_name("CURRENCY_PROJECTION_RESIDUAL_CONTRACT_V2.json")


def _sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json(path):
    if path.is_symlink() or path.is_junction():
        raise ValueError("residual_operator_linked_input_refused")
    return json.loads(path.read_text(encoding="utf-8"))


def _descriptor(path):
    return {"path": path.name, "bytes": path.stat().st_size, "sha256": _sha256(path)}


def _frozen_contract():
    frozen = _read_json(CONTRACT_PATH)
    declared = {key: frozen["residual_layer"].get(key) for key in CONTRACT}
    if declared != CONTRACT:
        raise ValueError("residual_operator_contract_source_drift")
    return frozen


def _verified_parent(parent_recipe, parent_recipe_sha256, parent_paths, projection_run):
    """Authenticate the exact upstream recipe, raw inputs, and output inventory."""
    from currency_projection_operator_v2 import identity_for, preflight, required
    from publication import verify_completed_run

    parent_recipe = Path(parent_recipe)
    if _sha256(parent_recipe) != parent_recipe_sha256:
        raise ValueError("residual_operator_parent_recipe_pin_mismatch")
    recipe = preflight(parent_recipe, parent_recipe_sha256, parent_paths)
    expected = identity_for(recipe)
    manifest = verify_completed_run(Path(projection_run), expected)
    required_names = required(recipe)
    if manifest["required_payloads"] != required_names:
        raise ValueError("residual_operator_parent_required_inventory_mismatch")
    descriptors = {item["path"]: item for item in manifest["payloads"]}
    if set(descriptors) != set(required_names):
        raise ValueError("residual_operator_parent_payload_closure_mismatch")
    return recipe, expected, descriptors


def load_inputs(parent_recipe, parent_recipe_sha256, parent_paths, projection_run):
    """Load only members authenticated by the preserved parent recipe/manifest."""
    from currency_projection_operator_v2 import checked_bytes

    recipe, identity, descriptors = _verified_parent(
        parent_recipe, parent_recipe_sha256, parent_paths, projection_run)
    projection_run = Path(projection_run)
    projections, sources = [], []
    for name in [f"projection_{index:03}.json" for index in range(len(recipe["contract"]["frames"]))]:
        path = projection_run / name
        if _descriptor(path) != descriptors.get(name):
            raise ValueError("residual_operator_parent_projection_descriptor_mismatch")
        payload = _read_json(path)
        projections.extend(payload["predictions"])
        sources.append(_descriptor(path))
    outcomes = {}
    for name in sorted(item for item in recipe["inputs"]["extension"]["files"] if item.startswith("pair_")):
        payload = json.loads(checked_bytes(parent_paths, recipe, "extension", name))
        for outcome in payload["outcomes"]:
            key = outcome["record_id"], outcome["target_id"]
            if key in outcomes and outcomes[key] != outcome:
                raise ValueError("residual_operator_conflicting_outcome")
            outcomes[key] = outcome
    return projections, outcomes, sources, {
        "parent_projection_recipe_sha256": parent_recipe_sha256,
        "parent_projection_run_identity": identity["fingerprint"],
        "parent_projection_manifest_sha256": _sha256(projection_run / "COMPLETION_MANIFEST.json"),
        "parent_projection_identity_sha256": _sha256(projection_run / "RUN_IDENTITY.json"),
        "parent_projection_payload_count": len(descriptors),
        "extension_original_run_identity": recipe["inputs"]["extension"]["original_run_identity"],
    }, recipe


def _hash_ids(values):
    return hashlib.sha256(json.dumps(sorted(values), separators=(",", ":")).encode()).hexdigest()


def _coverage(rows, learned, snapshots, outcomes, asof, expected_scopes):
    learned_keys = {(row["base_method"], row["horizon_minutes"], row["record_id"]) for row in learned}
    controls = {(row["base_method"], row["horizon_minutes"], row["record_id"]): row
                for row in rows if row["variant"] == "direct"}
    result = []
    for base, horizon, record_id in sorted(controls):
        direct = controls[base, horizon, record_id]
        snapshot = snapshots.get((direct["origin_epoch"], base, horizon))
        outcome = outcomes.get((direct["record_id"], direct["target_id"]))
        issued = (base, horizon, record_id) in learned_keys
        if issued:
            issue_status = "issued"
        elif snapshot is None:
            issue_status = "missing_scope_snapshot"
        else:
            issue_status = snapshot["status"]
        label_status = "unavailable"
        if outcome is not None and outcome["value"] is not None:
            label_status = "mature" if outcome["available_epoch"] <= asof else "unresolved"
        result.append({"origin_epoch": direct["origin_epoch"], "utc_day": direct["origin_epoch"] // 86400,
                       "instrument": direct["instrument"], "record_id": record_id, "base_method": base,
                       "horizon_minutes": horizon, "forecast_issuance_status": issue_status,
                       "learned_issued": issued, "label_status": label_status,
                       "native_policy_status": "not_admitted_offline_diagnostic"})
    return result


def _scope_coverage(coverage, snapshots, expected_scopes, origins):
    """One row for every origin/base/horizon scope, including zero-row scopes."""
    result = []
    for origin in sorted(origins):
        for base, horizon in expected_scopes:
            part = [row for row in coverage if row["origin_epoch"] == origin
                    and row["base_method"] == base and row["horizon_minutes"] == horizon]
            snapshot = snapshots[origin, base, horizon]
            result.append({"origin_epoch": origin, "utc_day": origin // 86400, "base_method": base,
                           "horizon_minutes": horizon, "snapshot_status": snapshot["status"],
                           "direct_control_rows": len(part), "learned_issued_rows": sum(row["learned_issued"] for row in part),
                           "mature_label_rows": sum(row["label_status"] == "mature" for row in part),
                           "unresolved_label_rows": sum(row["label_status"] == "unresolved" for row in part),
                           "unavailable_label_rows": sum(row["label_status"] == "unavailable" for row in part),
                           "native_policy_rows": 0,
                           "native_policy_reason": "not_admitted_offline_diagnostic"})
    return result


def _paired_scores(learned, rows, outcomes, asof):
    controls = defaultdict(dict)
    for row in rows:
        if row["variant"] in CONTROLS:
            controls[row["base_method"], row["horizon_minutes"], row["record_id"]][row["variant"]] = row
    comparisons = defaultdict(list)
    for learned_row in learned:
        key = learned_row["base_method"], learned_row["horizon_minutes"], learned_row["record_id"]
        outcome = outcomes.get((learned_row["record_id"], learned_row["target_id"]))
        if outcome is None or outcome["value"] is None or outcome["available_epoch"] > asof:
            continue
        for variant, control in controls[key].items():
            if control["available_epoch"] <= asof:
                scope = key[:2] + (variant,)
                comparisons[scope + ("aggregate",)].append((learned_row, control, outcome))
                comparisons[scope + ("origin", learned_row["origin_epoch"])].append((learned_row, control, outcome))
                comparisons[scope + ("utc_day", learned_row["origin_epoch"] // 86400)].append((learned_row, control, outcome))
    result = []
    for (base, horizon, variant, stratum, *stratum_value), matched in sorted(comparisons.items()):
        learned_errors = [item[0]["prediction_bps"] - item[2]["value"] for item in matched]
        control_errors = [item[1]["prediction_bps"] - item[2]["value"] for item in matched]
        result.append({"base_method": base, "horizon_minutes": horizon, "control_variant": variant,
                       "stratum": stratum, "stratum_value": stratum_value[0] if stratum_value else None,
                       "matched_mature_rows": len(matched),
                       "membership_sha256": _hash_ids(item[0]["record_id"] for item in matched),
                       "mae_delta_bps": sum(map(abs, learned_errors)) / len(matched) - sum(map(abs, control_errors)) / len(matched),
                       "mse_delta_bps2": sum(x * x for x in learned_errors) / len(matched) - sum(x * x for x in control_errors) / len(matched),
                       "bias_delta_bps": sum(learned_errors) / len(matched) - sum(control_errors) / len(matched)})
    return result


def run(parent_recipe, parent_recipe_sha256, parent_paths, projection_run, asof):
    frozen_contract = _frozen_contract()
    rows, outcomes, source_frames, parent, recipe = load_inputs(
        parent_recipe, parent_recipe_sha256, parent_paths, projection_run)
    expected_scopes = [(base, horizon) for base in recipe["contract"]["bases"]
                       for horizon in recipe["contract"]["horizons_minutes"]]
    by_origin = defaultdict(list)
    for row in rows:
        by_origin[row["origin_epoch"]].append(row)
    expected_origins = recipe["contract"]["origins"]
    present_parent_origins = set(by_origin)
    if not present_parent_origins.issubset(expected_origins):
        raise ValueError("residual_operator_parent_origin_outside_frozen_grid")
    learned, snapshots = [], {}
    for origin in expected_origins:
        current = by_origin[origin]
        fitted = fit_snapshot(rows, outcomes, origin, CONTRACT, expected_scopes)
        snapshots.update({(origin, *scope): snapshot for scope, snapshot in fitted.items()})
        learned.extend(apply(current, fitted))
    coverage = _coverage(rows, learned, snapshots, outcomes, asof, expected_scopes)
    scope_coverage = _scope_coverage(coverage, snapshots, expected_scopes, expected_origins)
    return {"schema_version": "forex_currency_projection_residual_run.v2", "contract": frozen_contract,
            "parent": parent, "source_frames": source_frames, "source_prediction_rows": len(rows),
            "outcome_rows": len(outcomes), "asof": asof,
            "expected_scope_count": len(expected_origins) * len(expected_scopes),
            "present_parent_origin_count": len(present_parent_origins),
            "missing_parent_origin_count": len(expected_origins) - len(present_parent_origins),
            "learned_rows": learned, "snapshots": list(snapshots.values()), "coverage_ledger": coverage,
            "scope_coverage": scope_coverage,
            "learned_scores": score(learned, outcomes, asof),
            "paired_scores": _paired_scores(learned, rows, outcomes, asof),
            "scope": "offline retrospective development diagnostic; no confirmation, policy, native issuance, or trading claim"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent-recipe", type=Path, required=True)
    parser.add_argument("--parent-recipe-sha256", required=True)
    parser.add_argument("--parent-paths", type=Path, required=True)
    parser.add_argument("--projection-run", type=Path, required=True)
    parser.add_argument("--asof", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run(args.parent_recipe, args.parent_recipe_sha256, _read_json(args.parent_paths),
                 args.projection_run, args.asof)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
