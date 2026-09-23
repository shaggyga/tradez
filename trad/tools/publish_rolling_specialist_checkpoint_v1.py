"""Read-only inventory/preflight for the dated specialist vault supplement.

Despite the historical publication-oriented filename, this tool never copies,
writes, seals, deletes or loads an estimator. Publication is a separate explicit
step. Until final acceptance exists, use --plan-only (no run results are read).
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path, PurePosixPath


ROOT = Path(__file__).resolve().parents[1]
VAULT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex")
PACKAGE = "ROLLING_SPECIALISTS_20260915"
DEPENDENCY_PINS = {
    "ROLLING_MODEL_COMPARISON_20260915": "de436d7db0e79567d6071fd1f66d61804e76d5da1ccea9243c7cac17a1af939f",
    "ROLLING_TRAINING_WINDOWS_20260915": "d9da5d6e8b4c90587ab69023737129a2ccc6e111b8b2b6205a5fbb343ada2ef3",
}
LEGACY = "../direction_decision_20260911/src/signed_cost_models_v1.py"
LEGACY_SHA = "8cebabbab7c6dcb32271a8fd3cbc656e912ae6a26e19a9ff1d35ad547342665c"
EXPECTED_CONTEXTS = {f"{group}_{h}m" for group in ("compact38", "compact50") for h in (30, 60)}
EXPECTED_ARMS = {"direct_ridge", "specialist_ridge", "direct_context_hgb", "specialist_context_hgb"}
EXPECTED_VARIANTS = EXPECTED_ARMS | {"direct", "mixture_raw", "mixture_calibrated"}


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def relative(value: str) -> str:
    name = PurePosixPath(value.replace("\\", "/"))
    if name.is_absolute() or any(part in ("..", "") for part in name.parts) or ":" in str(name):
        raise ValueError(f"Unsafe package-relative path: {value}")
    return str(name)


def under(root: Path, value: str) -> Path:
    path = (root / relative(value)).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError(f"Source escaped its declared root: {path}")
    return path


def checked(path: Path, expected: str | None = None, byte_count: int | None = None) -> dict:
    before = path.stat()
    sha = digest(path)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError(f"Source changed while hashing: {path}")
    if expected is not None and sha != expected:
        raise ValueError(f"SHA256 mismatch: {path}")
    if byte_count is not None and after.st_size != byte_count:
        raise ValueError(f"Byte count mismatch: {path}")
    return {"source_path": str(path), "bytes": after.st_size, "sha256": sha}


def load(path: Path, expected: str | None = None) -> tuple[dict, dict]:
    receipt = checked(path, expected)
    result = json.loads(path.read_text(encoding="utf-8-sig"))
    checked(path, receipt["sha256"])
    return result, receipt


def static_plan() -> dict:
    return {
        "schema": "rolling_specialist_publication_inventory_v1",
        "status": "plan_only_no_run_results_read",
        "target_package": PACKAGE,
        "writes_performed": False,
        "requires": ["Complete four-context RESULTS.json", "Final accepted receipt and explicit exact SHA256", "All referenced original bytes unchanged"],
        "model_counts": {"six_head_bundles": 20, "meta_models": 16, "calibrators": 4, "pair_priors": 2},
        "compact_result_counts": {"variant_metrics": 28, "component_diagnostics": 28, "probability_calibration_diagnostics": 4},
        "normalizers": "Exact count/mean/scale/supported arrays, 5 cutoffs x 68 pairs x 50 fields; ordered field/pair/cutoff names; no raw_x",
        "inherited_package_manifest_pins": DEPENDENCY_PINS,
        "legacy_source": {"project_relative_path": LEGACY, "sha256": LEGACY_SHA, "required_package_path": "source/forex/direction_decision_20260911/src/signed_cost_models_v1.py"},
        "included": ["Final RESULTS and accepted audit/summary/case evidence", "All exact saved model states and priors", "New accepted code/tests/guides", "SPECIALIST_INPUTS manifest including all 68 source records", "Compact exact normalizers", "Dependency closure references to earlier sealed packages"],
        "excluded": ["raw_x and other prepared row matrices", "OOF arrays", "Head and combined forecast Parquet", "Raw quotes/prices", "Databases", "Credentials"],
        "recreation_limits": ["Saved estimators and exact normalizers permit inference recreation with compatible registered input features and runtime.", "Full refit/evaluation requires local accepted base, endpoint, quote and prepared input artifacts or exact reconstruction from their source recipes; those row datasets are not duplicated into this package.", "Earlier packages and the vault root README remain untouched by this tool. A later authorized publisher must preserve the root README bytes before adding a dated pointer.", "No cloud synchronization claim."],
    }


def preflight(project: Path, vault: Path, acceptance_path: Path, acceptance_sha: str) -> dict:
    if len(acceptance_sha) != 64 or any(c not in "0123456789abcdef" for c in acceptance_sha):
        raise ValueError("An explicit lowercase final acceptance SHA256 is required")
    acceptance, acceptance_receipt = load(acceptance_path, acceptance_sha)
    if not str(acceptance.get("status", "")).startswith("accepted"):
        raise ValueError("Final acceptance is not accepted")
    run = project / "data/rolling_specialists_20260915_v1"
    results, results_receipt = load(run / "RESULTS.json")
    if results.get("status") != "complete" or set(results.get("contexts", {})) != EXPECTED_CONTEXTS:
        raise ValueError("All four contexts must be complete before inventory")
    if results.get("completed_base_bundles") != 20 or results.get("completed_variants") != 28:
        raise ValueError("Unexpected completed bundle/variant counts")
    if results.get("can_place_orders") or results.get("models_promoted"):
        raise ValueError("This supplement is for the unpromoted research comparison")

    files: dict[str, dict] = {}
    inherited: list[dict] = []

    def add(path: Path, destination: str, category: str, sha: str | None = None, count: int | None = None) -> None:
        destination = relative(destination)
        suffix = Path(destination).suffix.lower()
        if suffix in {".parquet", ".sqlite", ".db", ".csv"} or (suffix == ".npz" and not destination.endswith("/NORMALIZERS.npz")):
            raise ValueError(f"Excluded raw/row artifact requested: {destination}")
        entry = {**checked(path, sha, count), "path": destination, "category": category}
        previous = files.get(destination)
        if previous and (previous["sha256"], previous["bytes"]) != (entry["sha256"], entry["bytes"]):
            raise ValueError(f"Conflicting package destination: {destination}")
        if previous is None:
            files[destination] = entry

    dependency_indexes = []
    dependency_receipts = []
    for package, sha in DEPENDENCY_PINS.items():
        manifest, receipt = load(vault / package / "MANIFEST.json", sha)
        dependency_receipts.append({"package": package, **receipt})
        dependency_indexes.append((package, {item["path"]: item for item in manifest["files"]}))

    inputs_root = project / "data/rolling_specialist_inputs_20260915_v1"
    inputs, input_receipt = load(inputs_root / "SPECIALIST_INPUTS.json", results["inputs_sha256"])
    if inputs.get("status") != "complete" or len(inputs.get("pairs", {})) != 68:
        raise ValueError("The complete 68-pair input recipe is required")
    add(inputs_root / "SPECIALIST_INPUTS.json", "evidence/inputs/SPECIALIST_INPUTS.json", "complete_input_recipe_68_source_records", input_receipt["sha256"])
    add(run / "RESULTS.json", "evidence/comparison/RESULTS.json", "complete_results", results_receipt["sha256"])
    add(acceptance_path, "source/forex/trad/docs/validation/rolling_specialists_20260915/ACCEPTANCE.json", "final_acceptance", acceptance_sha)

    bindings = {}
    for source in (results, inputs, acceptance):
        for name, record in source.get("source_bindings", {}).items():
            name = name.replace("\\", "/")
            sha = record if isinstance(record, str) else record["sha256"]
            if name in bindings and bindings[name] != sha:
                raise ValueError(f"Conflicting exact source bindings: {name}")
            bindings[name] = sha
    if bindings.get(LEGACY) != LEGACY_SHA:
        raise ValueError("Missing exact legacy helper source binding")
    for name, sha in sorted(bindings.items()):
        if name == LEGACY:
            source = project.parent / "direction_decision_20260911/src/signed_cost_models_v1.py"
            old = vault / "SIGNED_COST_RESEARCH_20260912/direction_decision_20260911/src/signed_cost_models_v1.py"
            checked(old, LEGACY_SHA)
            add(source, "source/forex/direction_decision_20260911/src/signed_cost_models_v1.py", "exact_legacy_runtime_helper", sha)
            continue
        source = under(project, name)
        checked(source, sha)
        destination = "source/forex/trad/" + name
        found = False
        for package, index in dependency_indexes:
            entry = index.get(destination)
            if entry and entry["sha256"] == sha:
                inherited.append({"source_name": name, "package": package, "path": destination, **checked(vault / package / destination, sha, entry["bytes"])})
                found = True
                break
        if not found:
            add(source, destination, "new_accepted_source", sha)

    for name, record in acceptance.get("test_bindings", {}).items():
        sha = record if isinstance(record, str) else record["sha256"]
        name = relative(name)
        add(under(project, name), "source/forex/trad/" + name, "accepted_test", sha)
    for field, category in (("evidence_files", "accepted_evidence"), ("project_documents", "dated_project_document")):
        if not acceptance.get(field):
            raise ValueError(f"Final acceptance must explicitly bind {field}")
        for name, record in acceptance[field].items():
            path = relative(record.get("path", name))
            add(under(project, path), "source/forex/trad/" + path, category, record["sha256"], record.get("bytes"))

    normalizer_path = project / "docs/validation/rolling_specialists_20260915/NORMALIZERS.json"
    normalizers, _ = load(normalizer_path)
    if normalizers["source_manifest_sha256"] != input_receipt["sha256"]:
        raise ValueError("Normalizer source recipe mismatch")
    if any(normalizers["arrays"][name]["shape"] != [5, 68, 50] for name in ("count", "mean", "scale", "supported")):
        raise ValueError("Unexpected normalizer dimensions")
    for name in ("NORMALIZERS.json", "NORMALIZERS.npz", "COMPACT38_30M_CASE_REVIEW.json"):
        if "source/forex/trad/docs/validation/rolling_specialists_20260915/" + name not in files:
            raise ValueError(f"Missing accepted recreation/case evidence: {name}")
    checked(normalizer_path.parent / "NORMALIZERS.npz", normalizers["artifact"]["sha256"], normalizers["artifact"]["bytes"])

    def artifact(record: dict, category: str) -> None:
        path = relative(record["path"])
        add(under(run, path), "evidence/comparison/" + path, category, record["sha256"], record.get("bytes"))

    for name, context in sorted(results["contexts"].items()):
        if len(context["oof_models"]) != 4 or set(context["meta_models"]) != EXPECTED_ARMS or set(context["variants"]) != EXPECTED_VARIANTS:
            raise ValueError(f"Incomplete expected model states in {name}")
        for record in [*context["oof_models"], context["final_model"]]:
            artifact(record, "six_head_bundle")
        for record in context["meta_models"].values():
            artifact(record, "meta_model")
        artifact(context["calibration"], "probability_calibrator")
        artifact(context["probability_calibration_scores"], "probability_calibration_diagnostics")
        for variant in context["variants"].values():
            artifact(variant["metrics"], "variant_metrics")
            artifact(variant["diagnostics"], "component_diagnostics")
    if set(results["pair_priors"]) != {"30", "60"}:
        raise ValueError("Expected exactly two horizon prior files")
    for record in results["pair_priors"].values():
        artifact(record, "pair_prior")

    checked(acceptance_path, acceptance_sha)
    checked(run / "RESULTS.json", results_receipt["sha256"])
    checked(inputs_root / "SPECIALIST_INPUTS.json", input_receipt["sha256"])
    readme_before = checked(vault / "README.md")
    return {
        **static_plan(), "status": "complete_read_only_preflight", "acceptance": acceptance_receipt,
        "file_count": len(files), "payload_bytes": sum(item["bytes"] for item in files.values()),
        "category_counts": dict(sorted(Counter(item["category"] for item in files.values()).items())),
        "files": [files[key] for key in sorted(files)], "inherited_sources_verified": inherited,
        "dependency_manifests": dependency_receipts, "vault_root_readme_observed_unchanged": readme_before,
        "not_yet_created": ["Package README and recreation/dependency metadata", "Preserved prepublication vault README", "Final payload manifest", "ZIP and complete readback receipt", "Dated vault root pointer"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan-only", action="store_true", help="Print required inventory without reading run results")
    parser.add_argument("--project", type=Path, default=ROOT)
    parser.add_argument("--vault", type=Path, default=VAULT)
    parser.add_argument("--acceptance", type=Path)
    parser.add_argument("--acceptance-sha256")
    args = parser.parse_args()
    if args.plan_only:
        result = static_plan()
    else:
        if not args.acceptance_sha256:
            parser.error("--acceptance-sha256 is required; use --plan-only before final acceptance")
        acceptance = args.acceptance or args.project / "docs/validation/rolling_specialists_20260915/ACCEPTANCE.json"
        result = preflight(args.project.resolve(), args.vault.resolve(), acceptance.resolve(), args.acceptance_sha256)
    print(json.dumps(result, sort_keys=True, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
