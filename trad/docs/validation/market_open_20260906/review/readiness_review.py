"""Independent read-only startup review; writes only this review directory.

No runtime imports, credentials, broker/network calls, processes, tasks or
production database opens. CSV reads are bounded to 128 KiB per pair.
"""
import csv
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import re

OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[1] / "trad"
STATE = ROOT / "data/oanda_training_manager/state"
STUDY = ROOT / "data/oanda_training_manager/causal_forecast_study_v1_io_r2"
NOW = datetime.now(timezone.utc)
sources = {}


def read(path, cap=500_000):
    assert path.stat().st_size <= cap, str(path)
    raw = path.read_bytes()
    sources[str(path.relative_to(ROOT))] = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
    return raw


def load(path):
    return json.loads(read(path))


def age(value):
    if isinstance(value, (float, int)):
        stamp = datetime.fromtimestamp(value, timezone.utc)
    else:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return round((NOW - stamp).total_seconds(), 3)


contract = load(ROOT / "config/causal_forecast_study_v1_io_r2_20260906.json")
contract_semantic_sha = hashlib.sha256(json.dumps(contract, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
bindings = {}
for name, expected in contract["source_bindings"].items():
    actual = hashlib.sha256(read(ROOT / name)).hexdigest()
    bindings[name] = {"expected_sha256": expected, "actual_sha256": actual, "matches": expected == actual}
versions = {"python": platform.python_version(), "numpy": importlib.metadata.version("numpy"),
            "scikit-learn": importlib.metadata.version("scikit-learn")}
supervisor = read(ROOT / "oanda_always_on_supervisor.ps1").decode("utf-8-sig")
launcher = read(ROOT / "start_oanda_research_collection.ps1").decode("utf-8-sig")
watchdog = read(ROOT / "oanda_supervisor_watchdog.ps1").decode("utf-8-sig")
names_match = re.search(r"\$ResearchCollectionNames\s*=\s*@\((.*?)\n\)", supervisor, re.S)
allowlist = re.findall(r'"([a-z0-9_]+)"', names_match.group(1))
study_heartbeat = load(STUDY / "heartbeat.json")
scorecard = load(STUDY / "scorecard.json")
authorization = load(STATE / "practice_007_governed_canary_authorization_v1.json")
lifecycle = load(STATE / "evidence_lifecycle_v1.json")
verifier = load(STATE / "independent_evidence_verifier_v1.json")
integrity = load(STATE / "project_integrity_audit_v1.json")
clock = load(STATE / "clock_integrity_v1.json")
storage = load(STATE / "storage_headroom_v1.json")
quotes = load(STATE / "practice_007_market_quotes_v1.json")
archive = load(STATE / "all68_m1_forward_update_heartbeat_v1.json")
diagnostic_sources = {}
for name in ("oanda_practice_live_dashboard.py", "oanda_practice_shadow_strategy_lab.py", "oanda_entry_diagnostics.py"):
    raw = read(ROOT / name, cap=900_000)
    diagnostic_sources[name] = {"source_bound_by_current_study": name in bindings,
                                "sha256": hashlib.sha256(raw).hexdigest()}

pair_epochs, candles = {}, {}
for pair in contract["input_pairs"]:
    path = ROOT / "data/oanda_training_manager/candles" / f"{pair}_M1.csv"
    before = path.stat()
    with path.open("rb") as handle:
        header = handle.readline(8193)
        assert header.endswith(b"\n") and len(header) <= 8192
        handle.seek(max(len(header), before.st_size - 131072))
        raw = handle.read(131072)
    assert (before.st_size, before.st_mtime_ns) == (path.stat().st_size, path.stat().st_mtime_ns), "candle changed during bounded read"
    lines = raw.split(b"\n")[1:-1]
    rows = list(csv.DictReader([header.decode().strip()] + [line.decode() for line in lines]))
    time_key = next(key for key in ("time", "timestamp", "datetime") if key in rows[0])
    epochs = {datetime.fromisoformat(row[time_key].replace("Z", "+00:00")).timestamp() for row in rows}
    pair_epochs[pair] = epochs
    candles[pair] = {"file": str(path.relative_to(ROOT)), "file_bytes": before.st_size,
        "bytes_read": len(raw) + len(header), "tail_sha256": hashlib.sha256(raw).hexdigest(),
        "header_sha256": hashlib.sha256(header).hexdigest(), "parsed_tail_rows": len(rows),
        "latest_minute_start_utc": datetime.fromtimestamp(max(epochs), timezone.utc).isoformat(),
        "latest_bar_close_age_sec": round(NOW.timestamp() - max(epochs) - 60, 3)}
common = sorted(set.intersection(*pair_epochs.values()))
suffix = []
for stamp in reversed(common):
    if suffix and suffix[-1] - stamp != 60:
        break
    suffix.append(stamp)
    if len(suffix) >= contract["input_policy"]["maximum_common_bars"]:
        break

blocked_workers = ["practice_007_fast_executor", "strategy_lab", "proof_shadow_predictors",
                   "edge_evidence_worker", "allocator_proof_worker", "evidence_operations_worker",
                   "canonical_outcome_worker", "model_gap_live_signal"]
result = {
    "schema_version": "independent_collection_readiness_review_v1_20260906",
    "observed_utc": NOW.isoformat(), "canonical_project": str(ROOT),
    "assessment": "Static configuration and frozen bindings support a collection-only restart; running readiness requires the parent's post-launch observation. Practice entry is not ready.",
    "collection_configuration": {
        "allowlist": allowlist, "allowlist_count": len(allowlist),
        "excluded_trading_and_evidence_workers": {name: name not in allowlist for name in blocked_workers},
        "launcher_research_collection_only": '"-SafeCoreOnly", "-ResearchCollectionOnly"' in launcher,
        "supervisor_checks_allowlist_before_start": '$ResearchCollectionOnly -and $Name -notin $ResearchCollectionNames' in supervisor,
        "live_money_environment_disabled": '$env:FOREX_ALLOW_LIVE = "0"' in supervisor and '$env:FOREX_LIVE_EXECUTE = "0"' in supervisor,
        "legacy_watchdog_uses_safe_core_launcher": '"start_oanda_safe_core.ps1"' in watchdog,
        "watchdog_requirement": "Do not start the legacy recovery watchdog or enable its tasks for collection-only mode; it recovers the broader safe-core launcher.",
    },
    "frozen_study": {
        "contract_id": contract["contract_id"], "semantic_contract_sha256": contract_semantic_sha,
        "source_bindings": bindings, "all_bindings_match": all(row["matches"] for row in bindings.values()),
        "actual_dependency_versions": versions, "expected_dependency_versions": contract["dependency_versions"],
        "all_dependency_versions_match": versions == contract["dependency_versions"],
        "heartbeat_semantic_contract_matches": study_heartbeat["contract_sha256"] == contract_semantic_sha,
        "collection_enabled": contract["collection_enabled"],
        "safety_flags": {key: contract[key] for key in ("research_only", "can_place_orders", "can_authorize", "can_promote", "account_eligible", "proof_eligible", "historical_rows_imported")},
        "diagnostic_source_changes": diagnostic_sources,
        "diagnostic_scope": "Lab telemetry and dashboard log reading do not change any frozen study source or provide an order/promotion path. New exact-price offline evaluator is not wired into the registered worker.",
        "saved_heartbeat": study_heartbeat,
        "saved_heartbeat_age_sec": age(study_heartbeat["generated_epoch"]),
        "saved_scorecard_status": scorecard["status"], "saved_scorecard_counts": scorecard["collection_counts"],
    },
    "market_data": {
        "quote_producer": quotes.get("producer"), "quote_count": len(quotes["quotes"]),
        "tradeable_quote_count": sum(row.get("tradeable") is True for row in quotes["quotes"].values()),
        "eurusd_quote": quotes["quotes"]["EUR_USD"],
        "archive_saved_status": archive["status"], "archive_saved_phase": archive["phase"],
        "archive_heartbeat_age_sec": age(archive["updated_at"]), "archive_progress": archive["details"],
        "bounded_candle_tails": candles, "common_contiguous_suffix_bars_capped_512": len(suffix),
        "current_common_bar_age_sec": round(NOW.timestamp() - suffix[0] - 60, 3) if suffix else None,
        "required_common_contiguous_bars": 335, "maximum_common_bar_age_sec": 900,
        "post_weekend_warmup": "After the first post-gap minute appears, the frozen contiguous suffix resets. At least 335 common completed M1 bars (about 5h35) are required; archive publication, 15-minute fit cadence and build time add delay. H1 outcomes then require another original 1-hour horizon. No immediate opening predictions or trades are promised.",
    },
    "practice_entry_blockers": {
        "executor_excluded_by_collection_allowlist": "practice_007_fast_executor" not in allowlist,
        "authorization_generated_utc": authorization["generated_utc"], "authorization_age_sec": age(authorization["generated_utc"]),
        "authorization_max_age_sec_from_executor_launcher": 900,
        "entry_authorized": authorization["entry_authorized"], "authorized_entry_count": len(authorization["authorized_entries"]),
        "confirmed_candidate_count": authorization["confirmed_candidate_count"], "authorization_reason": authorization["reason"],
        "lifecycle_generated_utc": lifecycle["generated_utc"], "lifecycle_states": lifecycle["lifecycle"]["states"],
        "verifier_generated_utc": verifier["generated_utc"], "verifier_saved_status": verifier["status"],
        "fresh_signed_candidate_and_verifier_required": True,
        "diagnostic_only_not_gate_relaxation": True,
    },
    "saved_operational_states": {
        "integrity_status": integrity["status"], "integrity_audit_finished_utc": integrity["audit_finished_utc"],
        "integrity_age_sec": age(integrity["audit_finished_utc"]),
        "integrity_failed_checks": sorted(key for key, value in integrity["checks"].items() if value is False),
        "integrity_interpretation": "The retained whole-project audit is degraded and includes intentionally inactive research systems; it is not an all-clear and does not authorize collection or entries. Parent must verify the selected allowlist after startup.",
        "clock_status": clock["status"], "clock_reasons": clock["reasons"],
        "clock_generated_utc": clock["generated_utc"], "clock_discontinuity_active": clock["clock_discontinuity_active"],
        "clock_external_offset_sec": clock["external_https_clock"]["offset_sec"],
        "storage_status": storage["status"], "storage_generated_utc": storage["generated_utc"],
        "storage_free_gib_at_snapshot": storage["disk"]["free_gib"], "storage_minimum_free_gib": storage["disk"]["minimum_free_gib"],
    },
    "source_receipts": sources,
    "review_actions": {"production_database_reads": 0, "broker_calls": 0, "credential_reads": 0,
                       "process_mutations": 0, "scheduled_task_mutations": 0, "runtime_starts": 0,
                       "production_source_or_configuration_mutations": 0},
}
output = OUT / "INDEPENDENT_COLLECTION_READINESS_20260906.json"
with output.open("x", encoding="utf-8", newline="\n") as handle:
    json.dump(result, handle, indent=2)
    handle.write("\n")
print(json.dumps({"output": str(output), "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
    "bindings_match": result["frozen_study"]["all_bindings_match"],
    "heartbeat_semantic_contract_matches": result["frozen_study"]["heartbeat_semantic_contract_matches"],
    "dependencies_match": result["frozen_study"]["all_dependency_versions_match"],
    "common_suffix": len(suffix), "common_age_sec": result["market_data"]["current_common_bar_age_sec"],
    "tradeable_quote_count": result["market_data"]["tradeable_quote_count"],
    "authorization_age_sec": result["practice_entry_blockers"]["authorization_age_sec"]}, indent=2))
