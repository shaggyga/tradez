"""Pinned status/run/resume/verify interface for the retained blend diagnostic."""
import argparse
import hashlib
import importlib.metadata
import json
import platform
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCES = ("forecast_blend_operator_v2.py", "forecast_blend_runner_v2.py", "forecast_blend_v2.py", "campaign_inspector_v2.py", "contracts.py", "publication.py", "FORECAST_BLEND_EXPERIMENT_CONTRACT.json")
PARENT_RECIPE = "RESIDUAL_CALIBRATION_OPERATOR_RECIPE.json"
PARENT_RECIPE_SHA256 = "a894436db0ab12f820f2ab4e242a41da2d3f9cae2b3dad7763955e34e622046d"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def recipe_for(paths):
    parent = ROOT / PARENT_RECIPE
    if sha(parent) != PARENT_RECIPE_SHA256:
        raise ValueError("blend_parent_recipe_drift")
    parent_recipe = read(parent)
    if set(paths) != {"joint", "technical"}:
        raise ValueError("blend_exact_dependency_paths_required")
    contract = read(ROOT / "FORECAST_BLEND_EXPERIMENT_CONTRACT.json")
    configuration = {"group": contract["population"]["group"], "universe": parent_recipe["universe"],
                     "assessment_asof_epoch": contract["population"]["assessment_asof_epoch"], "horizons_minutes": contract["population"]["horizons_minutes"],
                     "procedures": contract["population"]["procedures"], "main_wall_seconds": contract["resources"]["main_wall_seconds"],
                     "worker_count": contract["resources"]["workers"], "fit_count_cap": contract["resources"]["fit_count_cap"],
                     "model_load_count_cap": contract["resources"]["model_load_count_cap"], "blend_weight": "1/2"}
    if configuration["group"] != "legacy26" or configuration["horizons_minutes"] != [15, 60, 240, 720, 1440, 2880, 7200] or configuration["procedures"] != ["frozen", "adaptive"]:
        raise ValueError("blend_frozen_scope_required")
    dependencies = {}
    for alias, expected in parent_recipe["dependencies"].items():
        root = Path(paths[alias])
        manifest = read(root / "COMPLETION_MANIFEST.json")
        if manifest["run_identity"] != expected["identity"]:
            raise ValueError("blend_predecessor_identity_mismatch:" + alias)
        actual = {item["path"]: item["sha256"] for item in manifest["payloads"]}
        excluded = sorted(set(actual) - set(expected["payloads"]))
        if set(expected["payloads"]) - set(actual):
            raise ValueError("blend_predecessor_payload_missing:" + alias)
        dependencies[alias] = {"identity": expected["identity"], "payloads": expected["payloads"],
                               "excluded_timing_payloads": excluded}
    return {"schema_version": "forex_forecast_blend_recipe.v1", "run_id": "retained-equal-weight-forecast-blend-v5",
            "sources": {name: sha(ROOT / name) for name in SOURCES}, "dependencies": dependencies,
            "environment": {"python": platform.python_version(), "numpy": importlib.metadata.version("numpy")},
            "configuration": configuration, "input_tier": contract["reused_predecessors"]["input_tier"], "fit_allowed": False,
            "model_load_allowed": False, "broker_access": False, "confirmation": False,
            "parent_recipe": {"path": PARENT_RECIPE, "sha256": PARENT_RECIPE_SHA256}}


def preflight(recipe_path, digest, paths):
    if recipe_path.is_symlink() or sha(recipe_path) != digest:
        raise ValueError("blend_recipe_pin_mismatch")
    recipe = read(recipe_path)
    if set(recipe.get("sources", {})) != set(SOURCES) or any(sha(ROOT / name) != value for name, value in recipe["sources"].items()):
        raise ValueError("blend_source_drift_before_import")
    if recipe != recipe_for(paths):
        raise ValueError("blend_input_environment_drift")
    return recipe


def operate(action, recipe_path, digest, paths, runs, crash_after=None):
    try:
        if action not in {"status", "run", "resume", "verify"}:
            raise ValueError("unsupported_blend_action")
        recipe = preflight(recipe_path, digest, paths)
        sys.path.insert(0, str(ROOT))
        from forecast_blend_runner_v2 import identity_for, required, run, validate_completed
        from publication import verify_completed_run
        identity = identity_for(recipe)
        root = runs / recipe["run_id"]
        if action in {"run", "resume"}:
            run(paths, recipe, runs, resume=action == "resume", crash_after=crash_after)
        if (root / "COMPLETION_MANIFEST.json").exists():
            manifest = verify_completed_run(root, identity)
            if {item["path"] for item in manifest["payloads"]} != set(required()):
                raise ValueError("blend_payload_inventory_mismatch")
            validate_completed(root)
            status = "completed_verified"
        elif root.exists():
            if read(root / "RUN_IDENTITY.json") != identity:
                raise ValueError("blend_partial_identity_mismatch")
            status = "resumable"
        else:
            status = "ready"
        if action == "verify" and status != "completed_verified":
            raise ValueError("completed_blend_run_required")
        return {"status": status, "recipe_sha256": digest, "run_identity": identity["fingerprint"], "run_path": str(root),
                "next_action": "record_receipt_and_stop" if status == "completed_verified" else "resume" if status == "resumable" else "run",
                "engineering_ready": False, "forecast_evidence_status": "fixed_no_fit_development_diagnostic_not_confirmation",
                "policy_evidence_status": "not_evaluated", "demo_authorization_status": "not_granted", "independent_review": False}
    except Exception as exc:
        return {"status": "review_required", "reason": str(exc), "next_action": "preserve_evidence_and_escalate"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["status", "run", "resume", "verify"])
    for option in ("recipe", "paths", "runs-dir"):
        parser.add_argument("--" + option, type=Path, required=True)
    parser.add_argument("--recipe-sha256", required=True)
    parser.add_argument("--test-crash-after", type=int)
    args = parser.parse_args()
    result = operate(args.action, args.recipe, args.recipe_sha256, read(args.paths), args.runs_dir, args.test_crash_after)
    print(json.dumps(result, sort_keys=True))
    raise SystemExit(2 if result["status"] == "review_required" else 0)
