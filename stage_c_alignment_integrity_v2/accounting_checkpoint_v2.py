"""Compact, dependency-locked accounting replay; reuses validated ZIP guards."""
from __future__ import annotations

import argparse
import importlib.metadata
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from portable_checkpoint_v2 import (MANIFEST, _plain_destination, digest, encoded, environment_lock,
                                    inspect_package, safe_member)
from publication import sha256_file, verify_completed_run
from reference_accounting_adapter_v2 import DEFAULT_TRAD, reference_dependency_identity

SCHEMA = "forex_portable_accounting_checkpoint.v6"
SOURCE_ALLOWLIST = (
    "native_policy_input_v2.py",
    "native_policy_fixture_v2.py", "native_policy_operator_v2.py", "test_native_policy_input_v2.py",
    "NATIVE_POLICY_OPERATOR_RECIPE.json", "NATIVE_POLICY_CONTRACT_V2.md",
    "fitted_consumer_v2.py", "fitted_fixture_v2.py", "fitted_runner_v2.py", "test_fitted_consumer_v2.py",
    "FITTED_FIXTURE_UNIVERSE.json", "FITTED_OPERATOR_RECIPE.json", "FITTED_CONTRACT_V2.md",
    "policy_continuation_v2.py", "policy_fixture_v2.py", "policy_runner_v2.py", "test_policy_continuation_v2.py",
    "POLICY_OPERATOR_RECIPE.json", "POLICY_CONTRACT_V2.md",
    "accounting_events_v2.py", "accounting_event_fixtures_v2.py", "accounting_event_audit_v2.py",
    "accounting_event_runner_v2.py", "accounting_checkpoint_v2.py", "reference_accounting_adapter_v2.py",
    "publication.py", "contracts.py", "portable_checkpoint_v2.py",
    "test_accounting_events_v2.py", "test_accounting_event_runner_v2.py",
    "accounting_stress_v2.py", "test_accounting_stress_v2.py", "test_resting_accounting_events_v2.py",
    "test_reference_accounting_adapter_v2.py", "accounting_fastpath_v2.py", "test_accounting_fastpath_v2.py",
    "test_accounting_review_repairs_v2.py", "forex_operator_v2.py", "test_forex_operator_v2.py", "OPERATOR_CONTRACT_V2.md", "OPERATOR_RECIPE.json",
)
PREDECESSORS = ("oanda_forecast_curve_contract_v1.py", "oanda_curve_management_adapter_v1.py", "oanda_curve_management_replay_v1.py", "src/forex_system/research/sequential_portfolio_replay_v1.py",
                "src/forex_system/__init__.py")


def verify_packaged_operator(root, runs, *, python_executable=sys.executable, recipe_name="OPERATOR_RECIPE.json"):
    recipe = root / "source" / recipe_name
    script = "native_policy_operator_v2.py" if recipe_name=="NATIVE_POLICY_OPERATOR_RECIPE.json" else "forex_operator_v2.py"
    command = [python_executable, "-I", "-B", str(root / "source" / script)]
    arguments = ["--recipe", str(recipe), "--recipe-sha256", sha256_file(recipe),
                 "--runs-dir", str(runs), "--trad-root", str(root / "trad")]
    for action in ("status", "run", "verify"):
        result = subprocess.run([*command, action, *arguments], cwd=root, capture_output=True, text=True, timeout=120)
        if result.returncode:
            raise ValueError(f"packaged_operator_acceptance_failed:{action}:{result.stdout[-2000:]}:{result.stderr[-1000:]}")
        response = json.loads(result.stdout)
        if response["status"] not in ({"ready", "resumable", "completed_verified"} if action == "status" else {"completed_verified"}):
            raise ValueError("packaged_operator_unexpected_status:" + action)
    verified = {}
    for row in response["runs"]:
        run_root = runs / row["run_id"]
        identity = json.loads((run_root / "RUN_IDENTITY.json").read_text())
        manifest = verify_completed_run(run_root, identity)
        verified[row["engine"]] = {"identity": identity["fingerprint"],
            "payloads": {item["path"]: item["sha256"] for item in manifest["payloads"]}}
    return {"recipe_sha256": sha256_file(recipe), "runs": verified}


def dependency_lock():
    result = environment_lock()
    result["packages"]["pytest"] = importlib.metadata.version("pytest")
    return result


def replay(root, runs, *, python_executable=sys.executable, run_tests=False):
    check = subprocess.run([python_executable, "-I", "-B", "-c",
        "import json,platform,importlib.metadata as m; print(json.dumps({'schema_version':'forex_fixture_dependencies.v2',"
        "'python_version':platform.python_version(),'packages':{n:m.version(n) for n in ('numpy','pyarrow','tzdata','pytest')}}))"],
        cwd=root, capture_output=True, text=True, timeout=30)
    if check.returncode or json.loads(check.stdout) != json.loads((root / "DEPENDENCIES.json").read_text()):
        raise ValueError("subprocess_dependency_lock_mismatch")
    operator = verify_packaged_operator(root, runs.parent / (runs.name + "-operator"), python_executable=python_executable)
    policy_operator = verify_packaged_operator(root, runs.parent / (runs.name + "-policy-operator"),
        python_executable=python_executable, recipe_name="POLICY_OPERATOR_RECIPE.json")
    fitted_operator = verify_packaged_operator(root, runs.parent / (runs.name + "-fitted-operator"),
        python_executable=python_executable, recipe_name="FITTED_OPERATOR_RECIPE.json")
    native_operator = verify_packaged_operator(root, runs.parent / (runs.name + "-native-operator"),
        python_executable=python_executable, recipe_name="NATIVE_POLICY_OPERATOR_RECIPE.json")
    result = subprocess.run([python_executable, "-I", "-B", str(root / "source" / "accounting_event_runner_v2.py"),
        "--run-id", "accounting-fixture", "--runs-dir", str(runs), "--trad-root", str(root / "trad")],
        cwd=root, capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise RuntimeError(f"accounting_replay_failed:{result.returncode}:{result.stderr[-2500:]}")
    run = runs / "accounting-fixture"
    identity = json.loads((run / "RUN_IDENTITY.json").read_text())
    manifest = verify_completed_run(run, identity)
    report = json.loads((run / "run_report.json").read_text())
    if report["event_count"] != 20 or report["accounting_oracle"]["event_arm_rows_checked"] != 40:
        raise ValueError("bounded_accounting_fixture_coverage_mismatch")
    if run_tests:
        tests = subprocess.run([python_executable, "-I", "-B", "-m", "pytest", "-q", "-p", "no:cacheprovider", "--noconftest",
            str(root / "source" / "test_accounting_events_v2.py"), str(root / "source" / "test_accounting_event_runner_v2.py"),
            str(root / "source" / "test_accounting_stress_v2.py"), str(root / "source" / "test_resting_accounting_events_v2.py"),
            str(root / "source" / "test_reference_accounting_adapter_v2.py"),
            str(root / "source" / "test_accounting_fastpath_v2.py"), str(root / "source" / "test_forex_operator_v2.py"), str(root / "source" / "test_accounting_review_repairs_v2.py"),
            str(root / "source" / "test_policy_continuation_v2.py"), str(root / "source" / "test_fitted_consumer_v2.py"), str(root / "source" / "test_native_policy_input_v2.py")],
            cwd=root, capture_output=True, text=True, timeout=300)
        if tests.returncode:
            raise RuntimeError(f"relocated_accounting_tests_failed:{tests.stdout[-2500:]}:{tests.stderr[-1000:]}")
    return {"identity": identity["fingerprint"], "payloads": {item["path"]: item["sha256"] for item in manifest["payloads"]},
            "event_count": 20, "independent_event_arm_rows": 40, "packaged_operator": operator,
            "packaged_policy_operator": policy_operator, "packaged_fitted_operator": fitted_operator,
            "packaged_native_operator": native_operator}


def export_checkpoint(package, *, source=ROOT, trad_root=DEFAULT_TRAD):
    receipt_path = package.with_suffix(package.suffix + ".receipt.json")
    evidence_path = package.with_suffix(package.suffix + ".validation")
    if package.exists() or receipt_path.exists() or evidence_path.exists():
        raise FileExistsError("accounting_checkpoint_is_immutable")
    reference_dependency_identity(trad_root)
    package.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="accounting-fixture-build-", dir=package.parent) as temporary:
        staging = Path(temporary)
        for name in SOURCE_ALLOWLIST:
            original, target = source / name, staging / "source" / name
            if not original.is_file() or original.is_symlink() or original.is_junction():
                raise ValueError("missing_or_linked_runtime_source")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(original.read_bytes())
        predecessor_hashes = {}
        for name in PREDECESSORS:
            original, target = trad_root / name, staging / "trad" / name
            if not original.is_file() or original.is_symlink() or original.is_junction():
                raise ValueError("missing_or_linked_reference_source")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(original.read_bytes())
            predecessor_hashes[name] = sha256_file(original)
        (staging / "PREDECESSOR_IDENTITIES.json").write_bytes(encoded(predecessor_hashes))
        lock = dependency_lock()
        (staging / "DEPENDENCIES.json").write_bytes(encoded(lock))
        (staging / "requirements.lock").write_text("".join(f"{name}=={version}\n" for name, version in sorted(lock["packages"].items())), encoding="utf-8")
        expected = replay(staging, staging / "expected")
        (staging / "EXPECTED_REPLAY.json").write_bytes(encoded(expected))
        (staging / "README_RESTORE.md").write_text(
            "# Synthetic accounting checkpoint\n\n"
            "Includes four frozen synthetic recipes: accounting, momentum policy, fitted consumer and native conditional-input policy. "
            "Native qualification reuses declared synthetic prospective-clock receipts; no historical remaining-horizon forecast evidence is included. "
            "Raw validation runs are retained beside the original ZIP in its .validation directory, outside this compact bundle. "
            "Restore regenerates those synthetic fixtures and must match EXPECTED_REPLAY.json exactly. "
            "Scope: 20 explicit synthetic order/fill/financing events, two independent arms, "
            "40 independent accounting checks, restart state and exact dependencies. "
            "This is not a historical policy result or full optimized-engine certification. "
            "No bulk prices, broker state, accounts, credentials or services are included.\n\n"
            "Verify the external ZIP SHA from its receipt. On a fresh machine provision DEPENDENCIES.json/requirements.lock, "
            "extract the trusted matching ZIP to a new bootstrap folder, then run source/accounting_checkpoint_v2.py "
            "restore --package <zip> --sha256 <external hash> --destination <new empty folder> --run-tests. "
            "The reviewed predecessor files and package initializer are included under trad/. No original machine path is required. "
            "Accept only RESTORE_RECEIPT.json after exact payload parity. Tests also exercise process death and resume.\n",
            encoding="utf-8")
        # Keep complete raw validation evidence locally; do not duplicate it in the
        # source/fixture bundle. The unchanged ZIP size guard remains authoritative.
        evidence_path.mkdir()
        for folder in sorted(staging.glob("expected*")):
            if folder.is_dir():shutil.copytree(folder,evidence_path/folder.name)
        files = sorted(path for path in staging.rglob("*") if path.is_file() and
                       (len(path.relative_to(staging).parts)==1 or path.relative_to(staging).parts[0] in {"source","trad"}))
        inventory = []
        for path in files:
            name = path.relative_to(staging).as_posix()
            safe_member(name)
            inventory.append({"path": name, "bytes": path.stat().st_size, "sha256": sha256_file(path)})
        manifest = {"schema_version": SCHEMA, "source_allowlist": list(SOURCE_ALLOWLIST),
                    "scope": "synthetic_accounting_policy_fitted_and_native_input_qualification", "members": inventory}
        with zipfile.ZipFile(package, "x", compression=zipfile.ZIP_DEFLATED) as archive:
            for item in inventory:
                archive.write(staging / item["path"], arcname=item["path"])
            archive.writestr(MANIFEST, encoded(manifest))
    inspect_package(package,sha256_file(package),expected_source_allowlist=SOURCE_ALLOWLIST,expected_schema=SCHEMA)
    receipt = {"schema_version": "forex_accounting_checkpoint_publication.v2", "status": "COMPLETE",
               "package": package.name, "sha256": sha256_file(package), "bytes": package.stat().st_size,
               "member_count": len(inventory) + 1, "expected_replay": expected,
               "local_validation_evidence": str(evidence_path), "validation_runs_included_in_zip": False}
    with receipt_path.open("xb") as handle:
        handle.write(encoded(receipt))
    return receipt


def restore_checkpoint(package, destination, expected_sha256, *, run_tests=False):
    payloads = inspect_package(package, expected_sha256, expected_source_allowlist=SOURCE_ALLOWLIST, expected_schema=SCHEMA)
    if json.loads(payloads["DEPENDENCIES.json"]) != dependency_lock():
        raise ValueError("accounting_dependency_lock_mismatch")
    _plain_destination(destination)
    destination.mkdir(parents=True, exist_ok=True)
    for name, raw in payloads.items():
        target = destination.joinpath(*safe_member(name).parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as handle:
            handle.write(raw)
        if sha256_file(target) != digest(raw):
            raise ValueError("restored_accounting_member_readback_mismatch")
    actual = replay(destination, destination / "replay", run_tests=run_tests)
    if actual != json.loads(payloads["EXPECTED_REPLAY.json"]):
        raise ValueError("relocated_accounting_replay_parity_failed")
    receipt = {"schema_version": "forex_accounting_checkpoint_restore.v2", "status": "VERIFIED", "sha256": expected_sha256,
               "restored_files": len(payloads), "dependency_lock_verified": True, "subprocess_replay_verified": True,
               "relocated_tests_ran_and_passed": run_tests, "replay": actual}
    receipt["packaged_operator_verified"] = True
    with (destination / "RESTORE_RECEIPT.json").open("xb") as handle:
        handle.write(encoded(receipt))
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    export = commands.add_parser("export")
    export.add_argument("--package", type=Path, required=True)
    export.add_argument("--trad-root", type=Path, default=DEFAULT_TRAD)
    restore = commands.add_parser("restore")
    restore.add_argument("--package", type=Path, required=True)
    restore.add_argument("--destination", type=Path, required=True)
    restore.add_argument("--sha256", required=True)
    restore.add_argument("--run-tests", action="store_true")
    args = parser.parse_args()
    if args.command == "export":
        result = export_checkpoint(args.package, trad_root=args.trad_root)
    else:
        result = restore_checkpoint(args.package, args.destination, args.sha256, run_tests=args.run_tests)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
