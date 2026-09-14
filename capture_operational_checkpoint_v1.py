"""Bounded, read-only operational evidence; never starts workers or broker I/O."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import time

import oanda_project_runtime_health as health

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data/oanda_training_manager"


def read(path, limit=2 * 1024 * 1024):
    with Path(path).open("rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("checkpoint_input_too_large")
    return json.loads(raw), hashlib.sha256(raw).hexdigest()


def take(value, names):
    return {name: value[name] for name in names.split() if name in value}


def capture(practice_recovery_manifest=ROOT / "config/practice_native_recovery_v2_20260913.json"):
    observed = time.time()
    supervision = health.read_supervisor_observation(DATA / "logs")
    profile_path = Path(supervision["operational_profile"]["path"])
    profile, profile_sha = read(profile_path)
    if profile_sha != supervision["operational_profile"]["sha256"]:
        raise ValueError("supervised_profile_bytes_changed")
    services = []
    for service in profile["services"]:
        row = {"name": service["name"], "source_matches_profile":
               hashlib.sha256((ROOT / service["script"]).read_bytes()).hexdigest() == service["source_sha256"]}
        try:
            value, seal = read(service["heartbeat"])
            row.update(heartbeat_sha256=seal,
                       heartbeat_file_age_sec=round(observed - Path(service["heartbeat"]).stat().st_mtime, 3),
                       heartbeat_schema_matches=value.get("schema_version") == service["heartbeat_schema"])
            row.update(take(value, "status phase errors last_error successful_cycles attempt_count last_failure studies"))
            if service["name"] == "research_feature_observations_v2":
                success = value.get("last_success")
                success = success if isinstance(success, dict) else {}
                coverage = success.get("coverage")
                coverage = coverage if isinstance(coverage, dict) else {}
                readiness = coverage.get("family_readiness")
                readiness = readiness if isinstance(readiness, dict) else {}
                row["last_success"] = take(success, "status elapsed_sec source_bytes last_publication_completed_utc archive")
                row["coverage"] = take(coverage, "accepted_instrument_count excluded_instrument_count expected_feature_instrument_count all_configured_feature_families_materialized")
                row["readiness"] = take(readiness, "fresh_quote_pairs_at_source_read model_output_pairs news_ready_pairs rich_M1_fresh_pairs rich_M1_materialized_pairs rich_M1_stale_or_missing_pairs native_M1_required_contiguous_rows structural_fresh_pairs_by_timeframe")
            elif service["name"] == "native_feature_candles_v1":
                row.update(take(value, "completed_requests submitted_requests deferred_due_inputs skipped_not_due configured_cache_receipts scheduling_contract"))
        except (OSError, ValueError, TypeError) as exc:
            row["read_error"] = type(exc).__name__
        services.append(row)
    # Read the trial selected by the installed recovery manifest. Account IDs,
    # credentials, marker bodies and arbitrary exception strings are omitted.
    manifest_path = Path(practice_recovery_manifest).resolve(strict=True)
    if manifest_path.parent != (ROOT / "config").resolve():
        raise ValueError("canonical_config_recovery_manifest_required")
    manifest, manifest_sha = read(manifest_path)
    trial_path = Path(manifest.get("trial_config_path", manifest.get("config_path", "")))
    if not trial_path.is_absolute():
        trial_path = ROOT / trial_path
    practice = {"recovery_manifest_path": str(manifest_path),
                "recovery_manifest_file_sha256": manifest_sha,
                "recovery_manifest_payload_seal": manifest.get("manifest_sha256")}
    if trial_path.is_file():
        config, config_sha = read(trial_path)
        trial = DATA / config["trial_id"]
        status, status_sha = read(trial / "status.json")
        recovery, recovery_sha = read(trial / "recovery_status.json")
        practice.update(config_path=str(trial_path), config_file_sha256=config_sha,
                        config_payload_seal=config.get("config_sha256"),
                        status_sha256=status_sha, recovery_status_sha256=recovery_sha,
                        **take(status, "state enabled entry_halt_latched loss_stop_latched stop_requested foreign_trade_count observed_epoch stop_epoch attempt_accounting last_candidate_scan unresolved_intents"),
                        position_count=len(status["positions"]) if isinstance(status.get("positions"), list) else None,
                        recovery=take(recovery, "phase source_valid worker_status_fresh worker_status_age_sec restart_counts broker_io_performed_by_recovery"))
    else:
        practice["status"] = "recovery_manifest_trial_path_unresolved"
    return {
        "schema_version": "forex_operational_checkpoint_v1_20260914",
        "captured_utc": datetime.now(timezone.utc).isoformat(),
        "profile_path": str(profile_path), "profile_sha256": profile_sha,
        "supervision": take(supervision, "status reasons running_worker_count expected_worker_count supervisor_age_sec"),
        "services": services, "practice": practice,
        "free_drive_gib": round(shutil.disk_usage(ROOT).free / 1024 ** 3, 3),
        "scope": "Actual source/profile and retained heartbeat read; no independent broker read, forecast rescore or profitability claim.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--practice-recovery-manifest", type=Path,
                        default=ROOT / "config/practice_native_recovery_v2_20260913.json")
    result = capture(parser.parse_args().practice_recovery_manifest)
    name = "CHECKPOINT_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ") + ".json"
    output = ROOT.parent / "operational_repairs_20260913" / name
    with output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, sort_keys=True, indent=2, allow_nan=False)
    print(json.dumps({"path": str(output), "supervision": result["supervision"],
                      "practice": result["practice"], "free_drive_gib": result["free_drive_gib"]}))
