"""Run a bounded fixed blend diagnostic from retained JSON records only."""
import json
import math
import os
import time
import uuid
import psutil
from collections import Counter

from campaign_inspector_v2 import CampaignReader
from forecast_blend_v2 import block_sensitivity, build_chunk, outcome_map, summarize
from joint_readiness_schedule_v2 import visible_forecast
from publication import RunPublisher, effective_run_identity, verify_completed_run

GROUP = "legacy26"
HORIZONS = (15, 60, 240, 720, 1440, 2880, 7200)
PROCEDURES = ("frozen", "adaptive")


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def name(minutes, procedure):
    return f"{minutes}_{procedure}.json"


def required():
    files = []
    for minutes in HORIZONS:
        for procedure in PROCEDURES:
            suffix = name(minutes, procedure)
            files.extend(["blend_" + suffix, "coverage_" + suffix, "metrics_" + suffix, "blocks_" + suffix])
    return sorted(files + ["source_references.json", "run_report.json", "resource_receipts.json"])


def scientific_names():
    return [n for n in required() if n != "resource_receipts.json"]


def check_resources(root, started, config, phase):
    elapsed = time.monotonic() - started
    process = psutil.Process()
    rss = sum(p.memory_info().rss for p in [process, *process.children(recursive=True)] if p.is_running())
    disk = sum(p.stat().st_size for p in root.rglob("*") if p.is_file())
    if not math.isfinite(elapsed) or not 0 <= elapsed <= config["main_wall_seconds"]:
        raise ValueError("blend_main_wall_limit")
    if rss > config["max_rss_bytes"]:
        raise ValueError("blend_rss_limit")
    if disk + 1024 * 1024 > config["max_scratch_bytes"]:
        raise ValueError("blend_scratch_limit")
    return {"phase": phase, "elapsed_seconds": elapsed, "aggregate_rss_bytes": rss,
            "run_disk_bytes": disk, "process_id": os.getpid()}


def validate_resources(value):
    observations = value["observations"]
    if not observations or observations[-1]["phase"] != "final_precommit":
        raise ValueError("blend_final_resource_receipt_required")
    for row in observations:
        if not (0 <= row["elapsed_seconds"] <= 300 and 0 <= row["aggregate_rss_bytes"] <= 1024**3
                and 0 <= row["run_disk_bytes"] <= 1024**3):
            raise ValueError("blend_resource_receipt_out_of_bounds")


def identity_for(recipe):
    return effective_run_identity(contract={"recipe": recipe, "required_payloads": required()},
                                  dependency_hashes={**recipe["sources"], **{key: value["identity"]["fingerprint"] for key, value in recipe["dependencies"].items()}})


def _contract(recipe, outcomes, minutes, procedure):
    return {"group": GROUP, "horizon_minutes": minutes, "procedure": procedure,
            "assessment_asof_epoch": recipe["configuration"]["assessment_asof_epoch"], "outcomes": outcomes}


def validate_completed(root):
    report = json.loads((root / "run_report.json").read_bytes())
    if report["models_fitted"] != 0 or report["models_loaded"] != 0 or report["api_calls"] != 0:
        raise ValueError("blend_no_fit_no_model_no_api_required")
    # One blend coverage row represents the exact matched two-method base pair.
    if report["chunks"] != len(HORIZONS) * len(PROCEDURES) or report["coverage_rows"] != 19040:
        raise ValueError("blend_complete_scope_required")
    if report["new_forecasts_issued"] != 0 or report["engineering_ready"] is not False:
        raise ValueError("blend_promotion_forbidden")
    validate_resources(json.loads((root / "resource_receipts.json").read_bytes()))


def run(paths, recipe, runs, *, resume=False, crash_after=None):
    identity = identity_for(recipe)
    publisher = RunPublisher(runs, recipe["run_id"], identity)
    if (publisher.root / "COMPLETION_MANIFEST.json").exists():
        verify_completed_run(publisher.root, identity)
        return
    publisher.acquire(recover=resume)
    started = time.monotonic()
    attempt = uuid.uuid4().hex
    def guard(phase):
        row = {**check_resources(publisher.root, started, recipe["configuration"], phase), "attempt_id": attempt}
        with (publisher.root / "RESOURCE_ATTEMPTS.jsonl").open("ab") as handle:
            handle.write(encoded(row)); handle.flush(); os.fsync(handle.fileno())
        return row
    try:
        guard("before_inputs")
        reader = CampaignReader(paths, recipe["dependencies"])
        outcomes = outcome_map(reader, recipe["configuration"]["universe"])
        guard("after_inputs")
        payloads, counts, index = [], Counter(), 0
        for minutes in HORIZONS:
            for procedure in PROCEDURES:
                if time.monotonic() - started > recipe["configuration"]["main_wall_seconds"]:
                    raise ValueError("blend_main_wall_limit")
                suffix = name(minutes, procedure)
                source_rows = reader.read("joint", "forecasts_legacy26_" + suffix)
                source_coverage = reader.read("joint", "coverage_legacy26_" + suffix)
                contract = _contract(recipe, outcomes, minutes, procedure)
                rows, coverage = build_chunk(source_rows, source_coverage, outcomes, contract)
                metrics = summarize(rows, contract)
                blocks = block_sensitivity(rows, contract)
                payloads.extend([
                    publisher.write_or_validate_payload("blend_" + suffix, encoded(rows)),
                    publisher.write_or_validate_payload("coverage_" + suffix, encoded(coverage)),
                    publisher.write_or_validate_payload("metrics_" + suffix, encoded(metrics)),
                    publisher.write_or_validate_payload("blocks_" + suffix, encoded(blocks)),
                ])
                counts["coverage_rows"] += len(coverage)
                counts["blend_rows"] += len(rows)
                counts["mature_rows"] += metrics["mature_rows"]
                counts["unavailable_rows"] += sum(not item["blend_available"] for item in coverage)
                for source in source_rows:
                    status = visible_forecast(source, source["forecast"]["available_epoch"], maximum_conditioning_age_seconds=2)["status"]
                    counts["base_native_2s_" + status] += 1
                counts["blend_native_2s_available"] += sum(r["available_epoch"] - r["decision_epoch"] <= 2 for r in rows)
                counts["blend_native_2s_stale_conditioning"] += sum(r["available_epoch"] - r["decision_epoch"] > 2 for r in rows)
                index += 1
                guard("after_chunk_" + str(index))
                if crash_after == index:
                    os._exit(91)
        references = {"dependencies": recipe["dependencies"], "source_payloads": {
            "forecast": ["forecasts_legacy26_" + name(h, p) for h in HORIZONS for p in PROCEDURES],
            "coverage": ["coverage_legacy26_" + name(h, p) for h in HORIZONS for p in PROCEDURES],
            "outcomes": ["pair_" + pair + ".json" for pair in recipe["configuration"]["universe"]]},
            "base_models_deserialized": False, "models_fitted": 0, "outcomes_separate_from_blend_payloads": True}
        report = {"status": "completed_fixed_equal_weight_forecast_blend_diagnostic", "chunks": index,
                  **dict(sorted(counts.items())), "models_fitted": 0, "models_loaded": 0, "api_calls": 0,
                  "new_forecasts_issued": 0, "engineering_ready": False,
                  "forecast_evidence_status": "fixed_no_fit_development_diagnostic_not_confirmation",
                  "policy_evidence_status": "not_evaluated", "demo_authorization_status": "not_granted",
                  "independent_review": False, "selection": "no_weight_horizon_model_or_feature_selection",
                  "limitations": ["all outcomes are previously inspected development evidence", "moving-block intervals are descriptive only",
                                  "no effective-N p-values winner or economic claim", "native freshness refusals are retained in coverage but not promoted"]}
        payloads.extend([publisher.write_or_validate_payload("source_references.json", encoded(references)),
                         publisher.write_or_validate_payload("run_report.json", encoded(report))])
        if crash_after == 0:
            os._exit(91)
        guard("final_precommit")
        receipts = {"scope": "sampled_phase_boundaries_not_continuous_peak_or_OS_quota",
                    "observations": [json.loads(line) for line in (publisher.root / "RESOURCE_ATTEMPTS.jsonl").read_text().splitlines()]}
        validate_resources(receipts)
        old = publisher.read_verified_payload("resource_receipts.json")
        payloads.append(publisher.write_or_validate_payload("resource_receipts.json", old or encoded(receipts)))
        validate_completed(publisher.root)
        check_resources(publisher.root, started, recipe["configuration"], "before_manifest")
        publisher.complete(payloads, set(required()))
    except BaseException:
        if publisher._owner_token is not None:
            publisher.release()
        raise
