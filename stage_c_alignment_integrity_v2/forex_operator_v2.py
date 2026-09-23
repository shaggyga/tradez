"""Deterministic offline operator for the approved synthetic accounting recipe."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import sys

ROOT = Path(__file__).resolve().parent
SCHEMA = "forex_approved_operator_recipe.v5"
SOURCES = ("forex_operator_v2.py", "accounting_events_v2.py", "accounting_fastpath_v2.py",
           "accounting_event_fixtures_v2.py", "accounting_event_audit_v2.py", "accounting_event_runner_v2.py",
           "reference_accounting_adapter_v2.py", "publication.py", "contracts.py")
PREDECESSORS = ("oanda_curve_management_replay_v1.py", "src/forex_system/research/sequential_portfolio_replay_v1.py",
                "src/forex_system/__init__.py")
ABSENT_IMPORT_PATHS = ("src/__init__.py", "src/__init__.pyc", "src.py", "src.pyc",
                       "src/forex_system.py", "src/forex_system.pyc",
                       "src/forex_system/research/__init__.py", "src/forex_system/research/__init__.pyc",
                       "src/forex_system/research.py", "src/forex_system/research.pyc")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def environment():
    return {"python": platform.python_version(), "packages": {name: importlib.metadata.version(name)
            for name in ("numpy", "pyarrow", "tzdata", "pytest")}}


def approved_recipe(trad_root, recipe_name="synthetic_accounting_reference_optimized"):
    """Engineering publication only; not an operator command or self-approval."""
    if recipe_name not in {"synthetic_accounting_reference_optimized", "synthetic_policy_continuation", "synthetic_fitted_consumer"}:
        raise ValueError("unsupported_operator_recipe")
    policy=recipe_name=="synthetic_policy_continuation"
    fitted=recipe_name=="synthetic_fitted_consumer"
    sources=(*SOURCES,"policy_continuation_v2.py","policy_fixture_v2.py","policy_runner_v2.py","native_policy_input_v2.py") if policy else SOURCES
    if fitted:sources=("forex_operator_v2.py","fitted_consumer_v2.py","fitted_fixture_v2.py","fitted_runner_v2.py","FITTED_FIXTURE_UNIVERSE.json","publication.py","contracts.py")
    return {"schema_version": SCHEMA, "recipe": recipe_name,
            "sources": {name: sha(ROOT / name) for name in sources},
            "predecessors": {name: sha(trad_root / name) for name in (() if fitted else (*PREDECESSORS,"oanda_forecast_curve_contract_v1.py","oanda_curve_management_adapter_v1.py") if policy else PREDECESSORS)},
            "absent_import_paths": {name: None if not (trad_root / name).exists() and not (trad_root / name).is_symlink()
                                    else "unexpected_present" for name in (() if fitted else ABSENT_IMPORT_PATHS)},
            "environment": environment(), "run_ids": {"qualified":"operator-fitted-consumer"} if fitted else {engine: ("operator-policy-" if policy else "operator-accounting-") + engine for engine in ("reference", "optimized")},
            "engineering_ready": False, "forecast_evidence_status": "no_new_evidence",
            "policy_evidence_status": "synthetic_only", "demo_authorization_status": "not_granted"}


def operate(action, recipe_path, recipe_sha256, runs_dir, trad_root):
    try:
        if recipe_path.is_symlink() or sha(recipe_path) != recipe_sha256:
            raise ValueError("approved_recipe_hash_mismatch")
        recipe = json.loads(recipe_path.read_text())
        if recipe != approved_recipe(trad_root,recipe.get("recipe")):
            raise ValueError("source_dependency_environment_or_recipe_drift")
        if action not in {"status", "run", "resume", "verify"}:
            raise ValueError("unsupported_operator_action")
        # Runtime imports happen only after all declared source bytes are checked.
        sys.path.insert(0, str(ROOT))
        policy=recipe["recipe"]=="synthetic_policy_continuation"
        fitted=recipe["recipe"]=="synthetic_fitted_consumer"
        if fitted:
            from fitted_fixture_v2 import fixture as fitted_fixture
            from fitted_runner_v2 import run, identity_for
            inputs=fitted_fixture()
        elif policy:
            from policy_fixture_v2 import fixture as lifecycle_fixture
            from policy_runner_v2 import run, identity_for
        else:
            from accounting_event_fixtures_v2 import lifecycle_fixture
            from accounting_event_runner_v2 import run, identity_for
        from publication import verify_completed_run
        if not fitted:contract, events = lifecycle_fixture(trad_root)
        records = []
        for engine, run_id in recipe["run_ids"].items():
            root = runs_dir / run_id
            identity = identity_for(inputs) if fitted else identity_for(contract, events, trad_root, engine)
            if action in {"run", "resume"}:
                if fitted:run(inputs,run_id=run_id,runs_dir=runs_dir,resume=True)
                else:run(contract, events, run_id=run_id, runs_dir=runs_dir, trad_root=trad_root, resume=True, engine=engine)
            if (root / "COMPLETION_MANIFEST.json").exists():
                verify_completed_run(root, identity)
                report = json.loads((root / "run_report.json").read_text())
                expected_rows=None if fitted else report["event_count"]*len(contract["policies"]) if policy else 40
                if fitted and (report["universe_count"]!=68 or report["coverage_rows"]!=2856 or report["forecast_rows"]!=2442 or report["models"]!=9):
                    raise ValueError("required_fitted_consumer_acceptance_missing")
                if not fitted and (report["accounting_oracle"]["status"] != "verified" or report["accounting_oracle"]["event_arm_rows_checked"] != expected_rows):
                    raise ValueError("required_accounting_audit_missing")
                if policy and (report["frames"]!=11 or report["decision_count"]!=30 or
                        any(v["open_lot_count"] or v["pending_order_units"] or v["rejected_events"] for v in report["arms"].values())):
                    raise ValueError("required_policy_fixture_acceptance_missing")
                state = "completed_verified"
            elif root.exists():
                if not (root / "RUN_IDENTITY.json").exists() or json.loads((root / "RUN_IDENTITY.json").read_text()) != identity:
                    raise ValueError("partial_run_identity_mismatch")
                state = "partial_requires_checked_resume"
            else:
                state = "ready"
            records.append({"engine": engine, "run_id": run_id, "status": state, "path": str(root)})
        complete = all(row["status"] == "completed_verified" for row in records)
        if complete and not fitted:
            roots = [runs_dir / recipe["run_ids"][engine] for engine in ("reference", "optimized")]
            parity=("event_ledger.jsonl", "final_state.json", "accounting_audit.json", "run_inputs.json")
            if policy:parity += ("policy_decisions.jsonl","policy_state.json")
            for name in parity:
                if (roots[0] / name).read_bytes() != (roots[1] / name).read_bytes():
                    raise ValueError("reference_optimized_parity_failed:" + name)
        if action == "verify" and not complete:
            raise ValueError("required_completed_runs_missing")
        status = "completed_verified" if complete else "resumable" if any(row["status"].startswith("partial") for row in records) else "ready"
        return {"schema_version": "forex_operator_result.v2", "status": status,
                "next_action": "record_receipt_and_stop" if complete else "resume" if status == "resumable" else "run",
                "recipe_sha256": recipe_sha256, "runs": records,
                "engineering_ready": False, "forecast_evidence_status": "no_new_evidence",
                "policy_evidence_status": "synthetic_only", "demo_authorization_status": "not_granted"}
    except Exception as exc:
        return {"schema_version": "forex_operator_result.v2", "status": "review_required",
                "reason_code": type(exc).__name__, "detail": str(exc), "next_action": "preserve_evidence_and_escalate",
                "recipe_sha256": recipe_sha256}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("status", "run", "resume", "verify"))
    parser.add_argument("--recipe", type=Path, required=True)
    parser.add_argument("--recipe-sha256", required=True)
    parser.add_argument("--runs-dir", type=Path, required=True)
    parser.add_argument("--trad-root", type=Path, default=ROOT.parent / "trad")
    args = parser.parse_args()
    result = operate(args.action, args.recipe, args.recipe_sha256, args.runs_dir, args.trad_root)
    print(json.dumps(result, sort_keys=True))
    return 2 if result["status"] == "review_required" else 0


if __name__ == "__main__":
    raise SystemExit(main())
