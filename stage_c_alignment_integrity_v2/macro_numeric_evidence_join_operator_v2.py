"""Bounded operator for the numeric-evidence meter successor."""
import argparse
import hashlib
import importlib.metadata
import json
import platform
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCES = (
    "macro_numeric_evidence_join_operator_v2.py", "macro_numeric_evidence_join_v2.py",
    "macro_meter_v2.py", "macro_meter_operator_v2.py", "macro_component_state_v2.py",
    "contracts.py", "publication.py",
)
BASE_INPUTS = (
    "METER_INPUT_PLAN.json", "detail_currency_states.json", "detail_parent_manifest.json",
    "extraction_cache.json", "legacy_normalized_term.py", "news_sources_v1.json",
    "scoped_text_cache.json", "universe.json", "version_bindings.json",
    "numeric_source_asof.json", "repaired_component_source_asof.json", "unit_source_asof.json",
    "provenance_source_asof.json",
    "numeric_material_cache.json", "component_evidence_cache.json", "unit_source_bindings.json",
    "unit_unresolved_evidence.json", "provenance_bindings.json",
    "numeric_parent_manifest.json", "component_parent_manifest.json", "unit_parent_manifest.json", "provenance_parent_manifest.json",
)
REQUIRED = ("meter_currency_states.json", "meter_document_evidence.json", "meter_pair_views.json", "meter_report.json", "meter_view.html")
MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_TOTAL_INPUT_BYTES = 48 * 1024 * 1024


def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def read(path): return json.loads(path.read_text(encoding="utf-8"))
def encoded(value): return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)+"\n").encode()
def input_names(inputs): return BASE_INPUTS


def recipe_for(inputs):
    total = 0
    for name in BASE_INPUTS:
        path = inputs / name
        if not path.is_file() or path.is_symlink() or path.stat().st_size > MAX_INPUT_BYTES:
            raise ValueError("plain_bounded_numeric_join_inputs_required")
        total += path.stat().st_size
    if total > MAX_TOTAL_INPUT_BYTES: raise ValueError("numeric_join_total_input_limit")
    return {
        "schema":"macro_numeric_evidence_join_recipe.v2", "run_id":"numeric-evidence-join-20260914-20260921",
        "sources":{name:sha(ROOT/name) for name in SOURCES}, "inputs":{name:sha(inputs/name) for name in BASE_INPUTS},
        "environment":{"python":platform.python_version(),"numpy":importlib.metadata.version("numpy")},
        "configuration":{"network_allowed":False,"live_database_access":False,"broker_access":False,"historical_forecast_admission":False,"max_wall_seconds":120,
                         "max_compressed_member_bytes":MAX_INPUT_BYTES,"max_expanded_json_member_bytes":32*1024*1024,"max_expanded_json_total_bytes":64*1024*1024},
        "required_payloads":list(REQUIRED),
    }


def preflight(recipe, digest, inputs):
    if recipe.is_symlink() or sha(recipe) != digest: raise ValueError("numeric_join_recipe_pin_mismatch")
    record = read(recipe)
    if set(record["sources"]) != set(SOURCES) or any(sha(ROOT/name) != value for name,value in record["sources"].items()):
        raise ValueError("numeric_join_source_drift")
    if record != recipe_for(inputs): raise ValueError("numeric_join_input_or_environment_drift")
    return record


def _consumed(inputs, recipe):
    blobs = {}
    for name, digest in recipe["inputs"].items():
        value = (inputs/name).read_bytes()
        if len(value) > MAX_INPUT_BYTES or hashlib.sha256(value).hexdigest() != digest:
            raise ValueError("numeric_join_consumed_input_drift")
        blobs[name] = value
    if sum(map(len, blobs.values())) > MAX_TOTAL_INPUT_BYTES: raise ValueError("numeric_join_consumed_total_limit")
    return blobs


def operate(action, recipe, digest, inputs, runs, crash_after=None):
    try:
        if action not in {"status", "run", "resume", "verify"}: raise ValueError("unsupported_numeric_join_action")
        record = preflight(recipe, digest, inputs)
        sys.path.insert(0, str(ROOT))
        from publication import RunPublisher, effective_run_identity, verify_completed_run
        from macro_numeric_evidence_join_v2 import build_joined_meter
        identity = effective_run_identity(contract=record, dependency_hashes={**record["sources"], **record["inputs"]})
        root = runs / record["run_id"]
        if action in {"run", "resume"} and not (root/"COMPLETION_MANIFEST.json").exists():
            publisher = RunPublisher(runs, record["run_id"], identity); publisher.acquire(recover=action=="resume")
            try:
                started = time.monotonic(); outputs = build_joined_meter(_consumed(inputs, record))
                if time.monotonic()-started > record["configuration"]["max_wall_seconds"]: raise ValueError("numeric_join_wall_budget_exceeded")
                payloads = []
                for index,name in enumerate(REQUIRED, 1):
                    payloads.append(publisher.write_or_validate_payload(name, outputs[name].encode() if name.endswith(".html") else encoded(outputs[name])))
                    if crash_after == index:
                        import os; os._exit(91)
                publisher.complete(payloads, set(REQUIRED))
            except BaseException:
                if publisher._owner_token is not None: publisher.release()
                raise
        if (root/"COMPLETION_MANIFEST.json").exists():
            manifest = verify_completed_run(root, identity)
            if {row["path"] for row in manifest["payloads"]} != set(REQUIRED): raise ValueError("numeric_join_exact_payload_inventory_required")
            report = read(root/"meter_report.json")
            if report["numeric_surprises_computed"] or report["forecast_features_admitted"] or report["forecast_improvement_proven"]:
                raise ValueError("numeric_join_scope_violation")
            status = "completed_verified"
        elif root.exists():
            if read(root/"RUN_IDENTITY.json") != identity: raise ValueError("numeric_join_partial_identity_mismatch")
            status = "resumable"
        else: status = "ready"
        if action == "verify" and status != "completed_verified": raise ValueError("numeric_join_completion_required")
        return {"status":status,"run_path":str(root),"run_identity":identity["fingerprint"],"recipe_sha256":digest,
                "next_action":"record_receipt_and_stop" if status=="completed_verified" else "resume" if status=="resumable" else "run",
                "engineering_ready":False,"forecast_evidence_status":"offline_numeric_evidence_not_forecast_admission","policy_evidence_status":"not_evaluated","demo_authorization_status":"not_granted","independent_review":False}
    except Exception as exc:
        return {"status":"review_required","reason":str(exc),"next_action":"preserve_evidence_and_escalate"}


if __name__ == "__main__":
    parser=argparse.ArgumentParser();parser.add_argument("action",choices=["status","run","resume","verify"])
    for name in ("recipe","inputs","runs-dir"): parser.add_argument("--"+name,type=Path,required=True)
    parser.add_argument("--recipe-sha256",required=True);parser.add_argument("--test-crash-after",type=int);args=parser.parse_args()
    result=operate(args.action,args.recipe,args.recipe_sha256,args.inputs,args.runs_dir,args.test_crash_after)
    print(json.dumps(result,sort_keys=True));raise SystemExit(2 if result["status"]=="review_required" else 0)
