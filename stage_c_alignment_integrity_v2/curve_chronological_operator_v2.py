"""Curve-shape layer over preserved chronological forecast frames."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from contracts import fingerprint
from curve_shape_layer_v2 import apply, fit_snapshot, join_curve


DEFAULT_VARIANTS = ("raw_matched_expanding", "signed_only_expanding", "magnitude_interaction_expanding")


def _sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json(path):
    path = Path(path)
    if path.is_symlink() or path.is_junction():
        raise ValueError("curve_chronological_linked_input_refused")
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _checked_file(path, descriptor, max_bytes=None):
    path = Path(path)
    if path.is_symlink() or path.is_junction():
        raise ValueError("curve_chronological_linked_member_refused")
    if max_bytes is not None and descriptor["bytes"] > max_bytes:
        raise ValueError("curve_chronological_member_budget_exceeded")
    raw = path.read_bytes()
    if len(raw) != descriptor["bytes"] or hashlib.sha256(raw).hexdigest() != descriptor["sha256"]:
        raise ValueError("curve_chronological_member_hash_mismatch")
    return json.loads(raw)


def _bulk_members(bulk_reference, trusted_manifest, trusted_manifest_sha256):
    data = _read_json(bulk_reference)
    if _sha256(trusted_manifest) != trusted_manifest_sha256 or data.get("manifest_sha256") != trusted_manifest_sha256:
        raise ValueError("curve_chronological_untrusted_manifest_anchor")
    manifest = _read_json(trusted_manifest)
    sealed = {item["path"]: item for item in manifest.get("files", manifest.get("members", []))}
    described = data["members"]
    members = {item["path"]: item for item in described}
    if len(members) != len(described):
        raise ValueError("curve_chronological_duplicate_frame_descriptor")
    expected = {path: item for path, item in sealed.items() if path.startswith("run/frame_") and path.endswith(".json")}
    if members != expected:
        raise ValueError("curve_chronological_bulk_population_mismatch")
    return members


def _load_outcomes(extension_root, recipe):
    root = Path(extension_root)
    outcomes = {}
    for name, descriptor in sorted(recipe["inputs"]["extension"]["files"].items()):
        if not name.startswith("pair_"):
            continue
        payload = _checked_file(root / name, descriptor, recipe["contract"]["resources"]["max_member_bytes"])
        for outcome in payload["outcomes"]:
            key = outcome["record_id"], outcome["target_id"]
            if key in outcomes and outcomes[key] != outcome:
                raise ValueError("curve_chronological_conflicting_outcome")
            outcomes[key] = outcome
    return outcomes


def _load_frame_predictions(frames_root, bulk_reference, contract, expected_population,
                            trusted_manifest, trusted_manifest_sha256, variants=DEFAULT_VARIANTS):
    members = _bulk_members(bulk_reference, trusted_manifest, trusted_manifest_sha256)
    horizons = contract["feature_definition"]["horizons_minutes"]
    origins = sorted(expected_population["origins"])
    expected_descriptors = {f"run/frame_{horizon}_{origin}.json" for horizon in horizons for origin in origins}
    if set(members) != expected_descriptors:
        raise ValueError("curve_chronological_expected_frame_population_mismatch")
    frames_root = Path(frames_root)
    rows, coverage, checked = [], [], []
    for horizon in horizons:
        for origin in origins:
            rel = f"run/frame_{horizon}_{origin}.json"
            if rel not in members:
                raise ValueError("curve_chronological_missing_frame_descriptor")
            frame = _checked_file(frames_root / f"frame_{horizon}_{origin}.json", members[rel])
            checked.append(rel)
            for item in frame["coverage"]:
                coverage.append(item)
            for row in frame["predictions"]:
                if row["variant"] not in variants:
                    continue
                if row["forecast_id"] != fingerprint({k: v for k, v in row.items() if k != "forecast_id"}):
                    raise ValueError("curve_chronological_forecast_identity")
                rows.append(row)
    expected_keys = {
        (origin, pair, base, variant, horizon)
        for origin in origins
        for pair in expected_population["instruments"]
        for base in expected_population["base_methods"]
        for variant in variants
        for horizon in horizons
    }
    observed_keys = [
        (item["origin_epoch"], item["instrument"], item["base_method"], item["variant"], item["horizon_minutes"])
        for item in coverage if item["variant"] in variants
    ]
    if len(observed_keys) != len(set(observed_keys)) or set(observed_keys) != expected_keys:
        raise ValueError("curve_chronological_expected_coverage_population_mismatch")
    return rows, coverage, checked, origins


def _variant_curve_rows(rows, contract, variant):
    horizons = contract["feature_definition"]["horizons_minutes"]
    anchor = contract["feature_definition"]["anchor_horizon_minutes"]
    selected = [row for row in rows if row["variant"] == variant]
    curves = join_curve(selected, horizons, anchor)
    for row in curves:
        row["source_variant"] = variant
        row["variant"] = "curve_shape"
    return curves


def _panel_coverage_ledger(rows, coverage, origins, contract, variants=DEFAULT_VARIANTS):
    horizons = contract["feature_definition"]["horizons_minutes"]
    predicted = {
        (row["decision_epoch"], row["instrument"], row["base_method"], row["variant"], row["horizon_minutes"])
        for row in rows
        if row["variant"] in variants
    }
    coverage_reasons = {}
    instruments, base_methods = set(), set()
    for item in coverage:
        if item["variant"] not in variants:
            continue
        key = (item["origin_epoch"], item["instrument"], item["base_method"], item["variant"], item["horizon_minutes"])
        coverage_reasons[key] = item["reason"]
        instruments.add(item["instrument"])
        base_methods.add(item["base_method"])
    for row in rows:
        if row["variant"] not in variants:
            continue
        instruments.add(row["instrument"])
        base_methods.add(row["base_method"])

    slots, status_counts, reason_counts = [], Counter(), Counter()
    complete_by_origin = Counter()
    incomplete_by_origin = Counter()
    for origin in origins:
        for instrument in sorted(instruments):
            for base in sorted(base_methods):
                missing_reasons = Counter()
                observed = 0
                for variant in variants:
                    for horizon in horizons:
                        key = (origin, instrument, base, variant, horizon)
                        if key in predicted:
                            observed += 1
                        else:
                            reason = coverage_reasons.get(key, "missing_coverage_record")
                            missing_reasons[reason] += 1
                            reason_counts[reason] += 1
                if missing_reasons:
                    status = "incomplete_horizon_panel"
                    incomplete_by_origin[origin] += 1
                else:
                    status = "complete_all_declared_variants_and_horizons"
                    complete_by_origin[origin] += 1
                status_counts[status] += 1
                slots.append({
                    "origin_epoch": origin,
                    "instrument": instrument,
                    "base_method": base,
                    "status": status,
                    "observed_forecasts": observed,
                    "expected_forecasts": len(horizons) * len(variants),
                    "missing_reason_counts": dict(sorted(missing_reasons.items())),
                })

    expected_slots = len(origins) * len(instruments) * len(base_methods)
    if len(slots) != expected_slots:
        raise ValueError("curve_chronological_all68_slot_ledger_mismatch")
    return {
        "schema": "forex_curve_all68_slot_coverage.v1",
        "origins": len(origins),
        "instruments": len(instruments),
        "base_methods": sorted(base_methods),
        "variants": list(variants),
        "horizons_minutes": horizons,
        "expected_slots": expected_slots,
        "status_counts": dict(sorted(status_counts.items())),
        "missing_reason_counts": dict(sorted(reason_counts.items())),
        "complete_by_origin": dict(sorted((str(k), v) for k, v in complete_by_origin.items())),
        "incomplete_by_origin": dict(sorted((str(k), v) for k, v in incomplete_by_origin.items())),
        "slots": slots,
    }


def _anchor_controls(rows, anchor_horizon):
    controls = {}
    for row in rows:
        if row["horizon_minutes"] != anchor_horizon or row["variant"] not in DEFAULT_VARIANTS:
            continue
        controls[row["record_id"], row["base_method"], row["variant"]] = row
    return controls


def _score(learned, controls, outcomes, asof):
    groups = defaultdict(list)
    for row in learned:
        outcome = outcomes.get((row["record_id"], row["target_id"]))
        if outcome is None or outcome["value"] is None or outcome["available_epoch"] > asof:
            continue
        for variant in DEFAULT_VARIANTS:
            control = controls.get((row["record_id"], row["base_method"], variant))
            if control is None or max(row["available_epoch"], control["available_epoch"]) > asof:
                continue
            for stratum, value in (("overall", None), ("origin", row["decision_epoch"]), ("utc_day", row["decision_epoch"] // 86400)):
                groups[row["base_method"], variant, stratum, value].append((row, control, outcome))
    result = []
    for (base, variant, stratum, value), part in sorted(groups.items()):
        learned_errors = [row["prediction_bps"] - outcome["value"] for row, _, outcome in part]
        control_errors = [control["prediction_bps"] - outcome["value"] for _, control, outcome in part]
        n = len(part)
        result.append({
            "base_method": base,
            "control_variant": variant,
            "stratum": stratum,
            "stratum_value": value,
            "matched_mature_rows": n,
            "distinct_pairs": len({row["instrument"] for row, _, _ in part}),
            "support_sha256": fingerprint(sorted(row["record_id"] for row, _, _ in part)),
            "mae_delta_bps": sum(map(abs, learned_errors)) / n - sum(map(abs, control_errors)) / n,
            "mse_delta_bps2": sum(x * x for x in learned_errors) / n - sum(x * x for x in control_errors) / n,
            "bias_delta_bps": sum(learned_errors) / n - sum(control_errors) / n,
        })
    return result


def run(frames_root, bulk_reference, extension_root, recipe, contract, asof,
        trusted_manifest, trusted_manifest_sha256, source_variant="raw_matched_expanding"):
    if _sha256(trusted_manifest) != trusted_manifest_sha256:
        raise ValueError("curve_chronological_untrusted_manifest_anchor")
    manifest = _read_json(trusted_manifest)
    sealed = {item["path"]: item for item in manifest.get("files", manifest.get("members", []))}
    recipe_descriptor = sealed.get("source_snapshot/CHRONOLOGICAL_LAYER_OPERATOR_RECIPE_V2.json")
    if not recipe_descriptor or Path(recipe).stat().st_size != recipe_descriptor["bytes"] or _sha256(recipe) != recipe_descriptor["sha256"]:
        raise ValueError("curve_chronological_untrusted_recipe")
    recipe = _read_json(recipe)
    contract = _read_json(contract)
    parent = recipe["contract"]
    expected_population = {
        "origins": parent["new_origins"],
        "instruments": parent["universe"],
        "base_methods": parent["parent_surface_contract"]["layer_contract"]["base_methods"],
    }
    if sorted(parent["horizons_minutes"]) != sorted(contract["feature_definition"]["horizons_minutes"]):
        raise ValueError("curve_chronological_horizon_contract_mismatch")
    rows, coverage, checked_frames, origins = _load_frame_predictions(
        frames_root, bulk_reference, contract, expected_population, trusted_manifest, trusted_manifest_sha256)
    outcomes = _load_outcomes(extension_root, recipe)
    curves = _variant_curve_rows(rows, contract, source_variant)
    all68_coverage = _panel_coverage_ledger(rows, coverage, origins, contract)
    controls = _anchor_controls(rows, contract["feature_definition"]["anchor_horizon_minutes"])
    by_origin = defaultdict(list)
    for row in curves:
        by_origin[row["decision_epoch"]].append(row)
    learned, snapshots, issued_coverage = [], [], []
    for origin in origins:
        for base in sorted({row["base_method"] for row in curves}):
            history = [row for row in curves if row["base_method"] == base]
            current = [row for row in by_origin[origin] if row["base_method"] == base]
            snapshot = fit_snapshot(history, outcomes, origin, contract["layer_fit"])
            snapshots.append({"origin_epoch": origin, "base_method": base, **snapshot})
            for row in current:
                value = apply(row, snapshot)
                issued = value is not None
                issued_coverage.append({
                    "origin_epoch": origin,
                    "instrument": row["instrument"],
                    "base_method": base,
                    "record_id": row["record_id"],
                    "status": snapshot["status"],
                    "issued": issued,
                    "all_controls_present": all((row["record_id"], base, variant) in controls for variant in DEFAULT_VARIANTS),
                })
                if value is not None:
                    learned.append(value)
    paired_scores = _score(learned, controls, outcomes, asof)
    status_counts = Counter(item["status"] for item in snapshots)
    return {
        "schema_version": "forex_curve_chronological_run.v1",
        "contract": contract,
        "source_variant": source_variant,
        "frames_checked": len(checked_frames),
        "origins_checked": origins,
        "source_prediction_rows": len(rows),
        "curve_rows": len(curves),
        "snapshot_status_counts": dict(sorted(status_counts.items())),
        "learned_rows": learned,
        "snapshots": snapshots,
        "coverage": issued_coverage,
        "all68_coverage": all68_coverage,
        "input_coverage_rows": len(coverage),
        "paired_scores": paired_scores,
        "asof": asof,
        "base_model_fits": 0,
        "api_calls": 0,
        "policy_replays": 0,
        "scope": "offline development curve-shape diagnostic over preserved chronological frames; no confirmation, native policy, or trading claim",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--frames-root", type=Path, required=True)
    parser.add_argument("--bulk-reference", type=Path, required=True)
    parser.add_argument("--extension-root", type=Path, required=True)
    parser.add_argument("--recipe", type=Path, required=True)
    parser.add_argument("--trusted-manifest", type=Path, required=True)
    parser.add_argument("--trusted-manifest-sha256", required=True)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--asof", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-variant", default="raw_matched_expanding")
    args = parser.parse_args()
    result = run(args.frames_root, args.bulk_reference, args.extension_root, args.recipe,
                 args.contract, args.asof, args.trusted_manifest,
                 args.trusted_manifest_sha256, args.source_variant)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
