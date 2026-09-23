"""Saved-JSON publication benchmark: no DB connections, broker or workers."""
import ast
import copy
import datetime as dt
import gc
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile
import threading
import time
import tracemalloc
from typing import Any

ROOT = Path(r"C:\Users\zmoor\Documents\forex\trad")
OUT = Path(__file__).resolve().parent
sys.path[:0] = [str(ROOT), str(ROOT.parent)]
sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
os.environ["TEMP"] = str(OUT)
os.environ["TMP"] = str(OUT)

def guard(event, args):
    if event in {"socket.connect", "socket.bind", "subprocess.Popen", "os.system", "os.startfile", "sqlite3.connect"}:
        raise RuntimeError("Saved-JSON benchmark forbids DB/network/external actions")
    if event == "open" and isinstance(args[0], (str, bytes, os.PathLike)):
        path, mode, flags = args
        writing = isinstance(mode, str) and any(c in mode for c in "wax+")
        writing |= isinstance(flags, int) and bool(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND))
        if writing and not Path(os.fsdecode(path)).resolve().is_relative_to(OUT):
            raise RuntimeError("Saved-JSON benchmark write escaped fixture directory")

def no_thread(*args, **kwargs):
    raise RuntimeError("Saved-JSON benchmark forbids worker starts")

threading.Thread.start = no_thread
sys.addaudithook(guard)
from oanda_integrity_publication import (
    DETAIL_SECTIONS, compact_integrity_payload, restore_integrity_details,
    verified_detail_artifacts,
)

# Use the exact production atomic writer without importing or executing run().
writer_source = (ROOT / "oanda_project_integrity_audit.py").read_text(encoding="utf-8")
writer_node = next(n for n in ast.parse(writer_source).body if isinstance(n, ast.FunctionDef) and n.name == "atomic_json")
writer_namespace = dict(Path=Path, Any=Any, json=json, os=os, time=time)
exec(compile(ast.Module(body=[writer_node], type_ignores=[]), "production_atomic_json", "exec"), writer_namespace)
atomic_json = writer_namespace["atomic_json"]

source = ROOT / "data/oanda_training_manager/state/project_integrity_audit_v1.json"
source_bytes = source.read_bytes()
source_hash = hashlib.sha256(source_bytes).hexdigest()
payload = json.loads(source_bytes)
del source_bytes

def describe(times):
    return {"samples": len(times), "median_ms": round(statistics.median(times), 6),
            "minimum_ms": round(min(times), 6), "maximum_ms": round(max(times), 6),
            "raw_ms": [round(x, 6) for x in times], "p95_estimated": False}

def timed(operation, count=5):
    times = []
    for _ in range(count):
        gc.collect()
        start = time.perf_counter_ns()
        value = operation()
        times.append((time.perf_counter_ns() - start) / 1_000_000)
        del value
    return describe(times)

def peak(operation):
    gc.collect()
    tracemalloc.start()
    baseline = tracemalloc.get_traced_memory()[0]
    value = operation()
    _, high = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    del value
    return {"incremental_peak_python_allocated_bytes": high - baseline,
            "scope": "one separate tracemalloc sample; input object already resident; not process RSS or a timing sample"}

def emit(directory, value, compact):
    directory.mkdir(parents=True, exist_ok=True)
    published = compact_integrity_payload(value, snapshot_dir=directory) if compact else value
    atomic_json(directory / "current.json", published)
    with (directory / "history.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(published, sort_keys=True, separators=(",", ":")) + "\n")
    return published

result = {
    "schema_version": "integrity_saved_payload_optimization_benchmark_v1",
    "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    "python": sys.version,
    "input": {"path": str(source), "sha256": source_hash, "bytes": source.stat().st_size},
    "scope": "Actual saved payload, offline transform/serialization/decode/temp writes only. No full integrity run, current audit computation, database queries, ownership-guard timing, broker, network or workers. Repeated reads likely use warm OS cache. Five samples are not latency-percentile evidence.",
    "production_database_connections": 0, "network_calls": 0, "worker_starts": 0,
}

with tempfile.TemporaryDirectory(prefix="integrity_benchmark_", dir=OUT) as temporary:
    base = Path(temporary).resolve()
    assert base.is_relative_to(OUT.resolve())
    baseline_dir = base / "baseline"
    result["baseline_full_snapshot_and_history_write"] = timed(lambda: emit(baseline_dir, payload, False))
    baseline_snapshot_bytes = (baseline_dir / "current.json").read_bytes()
    baseline_history_row_bytes = (baseline_dir / "history.jsonl").stat().st_size // 5
    assert json.loads(baseline_snapshot_bytes) == payload

    initial_times = []
    for index in range(5):
        directory = base / f"first_{index}"
        gc.collect()
        start = time.perf_counter_ns()
        compact = emit(directory, payload, True)
        initial_times.append((time.perf_counter_ns() - start) / 1_000_000)
    result["first_compact_publication_with_new_detail_artifacts"] = describe(initial_times)
    repeat_dir = directory
    compact_before = copy.deepcopy(compact)
    artifact_paths = verified_detail_artifacts(compact, snapshot_dir=repeat_dir)
    artifact_state = {str(p.relative_to(repeat_dir)): (p.stat().st_size, p.stat().st_mtime_ns) for p in artifact_paths}
    result["detail_artifacts"] = {
        "count": len(artifact_paths), "total_bytes": sum(p.stat().st_size for p in artifact_paths),
        "per_section_rows": {key: len(payload[key]["episode_rows"]) for key in DETAIL_SECTIONS},
        "references": {key: compact[key]["_detail_reference"] for key in DETAIL_SECTIONS},
    }
    changed = dict(payload)
    for key in DETAIL_SECTIONS:
        changed[key] = dict(payload[key], generated_utc="2026-09-06T02:00:00+00:00")
    result["repeat_compact_publication_changed_clocks_same_rows_including_artifact_hash_verification"] = timed(lambda: emit(repeat_dir, changed, True))
    final_compact = json.loads((repeat_dir / "current.json").read_bytes())
    check_state = {str(p.relative_to(repeat_dir)): (p.stat().st_size, p.stat().st_mtime_ns) for p in verified_detail_artifacts(final_compact, snapshot_dir=repeat_dir)}
    assert artifact_state == check_state
    publication_reconstructed = restore_integrity_details(compact_before, snapshot_dir=repeat_dir) == payload
    assert publication_reconstructed
    assert restore_integrity_details(final_compact, snapshot_dir=repeat_dir) == changed
    compact_snapshot_bytes = (repeat_dir / "current.json").read_bytes()
    compact_history_row_bytes = len((json.dumps(compact_before, sort_keys=True, separators=(",", ":")) + "\r\n").encode())
    result["equivalence"] = {"exact_original_reconstruction": publication_reconstructed,
        "changed_clocks_exact_reconstruction": True, "unchanged_rows_reused_without_rewrite": True,
        "all_top_level_checks_verdicts_counts_authority_preserved": True,
        "all_non_episode_section_fields_preserved": True, "saved_source_hash_unchanged": hashlib.sha256(source.read_bytes()).hexdigest() == source_hash}
    result["bytes"] = {"baseline_current": len(baseline_snapshot_bytes), "compact_current": len(compact_snapshot_bytes),
        "baseline_history_row": baseline_history_row_bytes, "compact_history_row": compact_history_row_bytes,
        "current_reduction_percent": 100 * (1-len(compact_snapshot_bytes)/len(baseline_snapshot_bytes)),
        "history_row_reduction_percent": 100 * (1-compact_history_row_bytes/baseline_history_row_bytes)}
    print(json.dumps({"progress":"publication write timings complete", "bytes":result["bytes"]}), flush=True)
    result["full_snapshot_serialization"] = timed(lambda: json.dumps(payload, indent=2, sort_keys=True))
    result["compact_snapshot_serialization"] = timed(lambda: json.dumps(compact_before, indent=2, sort_keys=True))
    result["full_snapshot_decode"] = timed(lambda: json.loads(baseline_snapshot_bytes))
    result["compact_snapshot_decode"] = timed(lambda: json.loads(compact_snapshot_bytes))
    result["full_snapshot_serialization_peak"] = peak(lambda: json.dumps(payload, indent=2, sort_keys=True))
    result["compact_repeat_transform_and_serialization_peak"] = peak(lambda: json.dumps(compact_integrity_payload(changed, snapshot_dir=repeat_dir), indent=2, sort_keys=True))
    result["full_snapshot_decode_peak"] = peak(lambda: json.loads(baseline_snapshot_bytes))
    result["compact_snapshot_decode_peak"] = peak(lambda: json.loads(compact_snapshot_bytes))

result["source_sha256"] = {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in [
    "oanda_integrity_publication.py", "oanda_project_integrity_audit.py", "test_oanda_integrity_publication.py"]}
destination = OUT / "integrity_publication_benchmark.json"
destination.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
print(json.dumps({"output": str(destination), "bytes": result["bytes"], "timings": {k:v for k,v in result.items() if isinstance(v,dict) and "median_ms" in v}}, indent=2), flush=True)
