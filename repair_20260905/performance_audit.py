"""Audit saved Forex performance without starting the system or opening its DBs.

Reads dated JSON and filesystem metadata, benchmarks JSON decode only, and writes
this audit directory. No broker, worker, credential or network dependencies.
"""
from __future__ import annotations
import datetime as dt
import gc
import hashlib
import json
from pathlib import Path
import shutil
import statistics
import time

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / "trad"
DATA = ROOT / "data/oanda_training_manager"
bindings = {}

def read(relative):
    path = ROOT / relative
    payload = path.read_bytes()
    bindings[relative] = {"sha256":hashlib.sha256(payload).hexdigest(),"bytes":len(payload),
                          "modified_utc":dt.datetime.fromtimestamp(path.stat().st_mtime,dt.timezone.utc).isoformat()}
    return json.loads(payload)

account = read("data/oanda_training_manager/state/governed_practice_accounting_v1.json")
lifecycle = read("data/oanda_training_manager/state/evidence_lifecycle_v1.json")
source = read("data/oanda_training_manager/local_news_sentiment/causal_source_factor_response_map_latest_v8.json")
rank = read("data/oanda_training_manager/state/source_conditioned_currency_rank_v7.json")
allocator = read("data/oanda_training_manager/state/allocator_proof_v1.json")
executor = read("data/oanda_training_manager/state/practice_007_top_executor_heartbeat_v1.json")
collector = read("data/oanda_training_manager/local_news_sentiment/collector_state_v1.json")
storage = read("data/oanda_training_manager/state/storage_headroom_v1.json")
watchlist = read("data/oanda_training_manager/state/news_technical_watchlist_v1.json")
integrity_path = "data/oanda_training_manager/state/project_integrity_audit_v1.json"
integrity = read(integrity_path)

def numeric_fields(value):
    return {k:v for k,v in value.items() if isinstance(v,(int,float)) and not isinstance(v,bool)}

velocity = lifecycle["proof_cohort_evidence_velocity"]
if isinstance(velocity, dict):
    velocity = velocity.get("cohorts", velocity.get("families", []))
if isinstance(velocity, dict):
    velocity = list(velocity.values())

# Structure is recorded in the saved lifecycle; keep the complete small velocity
# subtree so its effective-N/EV definitions can be inspected with the report.
accounting = account["accounting"]
continuity = accounting["account_operational_continuity"]
large_sections = sorted([
    {"key":key,"compact_json_bytes":len(json.dumps(value,separators=(",",":"),ensure_ascii=False).encode())}
    for key,value in integrity.items()
],key=lambda row:row["compact_json_bytes"],reverse=True)[:8]
integrity_summary = {k:integrity.get(k) for k in ["generated_utc","status","phase_durations_sec",
    "component_durations_sec","publication_latency_sec","maximum_publication_latency_sec","failures","actual_coverage","measurements"]}
del integrity
gc.collect()

decode_samples = []
decode_read_samples = []
for _ in range(5):
    start = time.perf_counter()
    payload = (ROOT/integrity_path).read_bytes()
    loaded = time.perf_counter()
    decoded = json.loads(payload)
    finished = time.perf_counter()
    decode_read_samples.append((loaded-start)*1000)
    decode_samples.append((finished-loaded)*1000)
    del decoded, payload
    gc.collect()

database_files = []
for directory in [DATA/"state", DATA/"local_news_sentiment"]:
    for path in directory.glob("*.sqlite"):
        stat = path.stat()
        wal = path.with_name(path.name+"-wal")
        database_files.append({"path":str(path.relative_to(ROOT)),"bytes":stat.st_size,
            "wal_bytes":wal.stat().st_size if wal.exists() else 0,
            "modified_utc":dt.datetime.fromtimestamp(stat.st_mtime,dt.timezone.utc).isoformat()})
database_files.sort(key=lambda row:row["bytes"]+row["wal_bytes"],reverse=True)
logs = DATA/"logs"
log_files = [p for p in logs.iterdir() if p.is_file()]
log_inventory = {"top_level_files":len(log_files),"top_level_bytes":sum(p.stat().st_size for p in log_files),
                 "recursive_inventory":False,"contents_read":False}
disk = shutil.disk_usage(ROOT)

result = {
    "schema_version":"forex_stopped_performance_audit_v1",
    "generated_utc":dt.datetime.now(dt.timezone.utc).isoformat(),
    "runtime_state":"intentionally_stopped","broker_requests":0,"production_database_connections":0,
    "scope":"Saved research/account/latency observations, current file metadata, and five offline JSON decode samples; no live load or trading test",
    "account":{"saved_utc":account["generated_utc"],"balance":continuity["balance"],"nav":continuity["nav"],
        "cumulative_pl":continuity["cumulative_pl"],"open_trades":continuity["open_trade_count"],"pending_orders":continuity["pending_order_count"],
        "attribution_by_bucket":accounting["by_bucket"],"execution_transport":accounting["execution_transport"],
        "virtual_equity_by_governed_cohort":accounting["virtual_equity_by_governed_cohort"],
        "return_win_rate_sharpe_drawdown":"not_established_from_complete_attributed_trade_and_cashflow_history"},
    "research":{"saved_utc":lifecycle["generated_utc"],"lifecycle":lifecycle["lifecycle"],
        "proof_cohort_evidence_velocity":lifecycle["proof_cohort_evidence_velocity"],
        "source_v8_counts":source.get("counts"),"rank_v7_ledger":rank.get("ledger"),
        "allocator_counts":allocator.get("counts"),"allocator_collection_state":allocator.get("collection_state"),
        "watchlist":{"saved_utc":watchlist["generated_utc"],"currency_factor_count":watchlist["currency_factor_count"],"watchlist_entry_count":watchlist["watchlist_entry_count"]}},
    "operations":{"integrity":integrity_summary,"executor":{"saved_utc":executor.get("updated_at"),
        "cycles":executor["details"].get("cycles"),"cycle_timings":executor["details"].get("cycle_timings"),
        "last_cycle_ms":executor["details"].get("last_cycle_ms"),"feed_candidates":executor["details"].get("feed_candidates"),
        "order_telemetry":executor["details"].get("order_telemetry")},
        "collector":{"saved_utc":collector["updated_utc"],"last_cycle":numeric_fields(collector["last_cycle"])},
        "snapshot_decode":{"path":integrity_path,"file_bytes":bindings[integrity_path]["bytes"],"samples":5,
            "read_ms":decode_read_samples,"decode_ms":decode_samples,"median_decode_ms":statistics.median(decode_samples),
            "min_decode_ms":min(decode_samples),"max_decode_ms":max(decode_samples),
            "cache_state":"same-process repeated reads; likely OS cache warm; not cold-disk or concurrent-runtime benchmark",
            "largest_sections_compact_bytes":large_sections}},
    "storage":{"saved_utc":storage["generated_utc"],"saved_totals":storage["totals"],"saved_logs":storage["logs"],
        "current_disk":{"free_bytes":disk.free,"free_gib":disk.free/1024**3,"total_bytes":disk.total},
        "current_top_level_logs":log_inventory,"sampled_database_count":len(database_files),
        "sampled_database_bytes":sum(row["bytes"] for row in database_files),"sampled_wal_bytes":sum(row["wal_bytes"] for row in database_files),
        "largest_database_files":database_files[:15],"inventory_boundary":"Top-level SQLite files in state and local_news_sentiment only; not whole-project storage",
        "growth_forecast":"not extrapolated from a stopped or five-minute zero-growth window"},
    "source_observation_hashes":bindings,
    "supported_decision":"no_trade",
}
(OUT/"PERFORMANCE_AUDIT.json").write_text(json.dumps(result,indent=2)+"\n",encoding="utf-8")
print(json.dumps({"output":str(OUT/"PERFORMANCE_AUDIT.json"),"snapshot_decode":result["operations"]["snapshot_decode"],
    "account":result["account"],"disk_free_gib":result["storage"]["current_disk"]["free_gib"]},indent=2))
