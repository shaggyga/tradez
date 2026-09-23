"""Synthetic sizing/performance proof for the rich existing-calculator adapter."""
from copy import deepcopy
from datetime import timedelta
import gzip
import hashlib
import itertools
import json
from pathlib import Path
import sys
import time

PROJECT = Path(r"C:\Users\zmoor\Documents\forex\trad")
sys.path.insert(0, str(PROJECT))
import oanda_research_feature_observation_worker_v1 as worker
from oanda_feature_observations_v1 import archive_observation_snapshot, build_observation_frame, canonical_bytes
from oanda_feature_move_mapping_v1 import read_feature_move_maps
from test_oanda_research_feature_observation_worker_v1 import NOW, candles, quotes


def main():
    output = Path(sys.argv[1])
    output.mkdir(exist_ok=False)
    started = time.perf_counter()
    pairs = [f"{a}_{b}" for a, b in itertools.combinations("USD EUR JPY GBP CHF AUD CAD NZD SEK NOK DKK PLN CZK HUF TRY ZAR MXN SGD HKD CNH THB".split(), 2)][:68]
    all_sources = {tf: candles(tf, count=1024 if tf == "M5" else 512) for tf in worker.NATIVE_SECONDS}
    snapshot = worker.build_research_observation(quotes(pairs), {pair: all_sources for pair in pairs}, source_read_completed_utc=NOW.isoformat(), generated_utc=NOW.isoformat(), clock=lambda: NOW.isoformat(), max_cycle_seconds=30)
    calculated_sec = time.perf_counter()-started
    frame = build_observation_frame(snapshot)
    per_pair = {pair: sum(len(group["values"]) for group in row["groups"].values()) for pair, row in frame["instruments"].items()}
    wide = {pair: sum(name.startswith("ma__") and value is not None for group in row["groups"].values() for name, value in group["values"].items()) for pair, row in frame["instruments"].items()}
    assert all(count == 643 for count in wide.values()), wide
    size = len(canonical_bytes({"original_snapshot": snapshot, "frame": frame}))
    assert size <= worker.MAX_ENVELOPE_BYTES, size
    print(json.dumps({"phase": "computed", "calculated_sec": calculated_sec, "scalar_count": sum(per_pair.values()), "expanded_envelope_without_outer_metadata_bytes": size}), flush=True)
    def shifted(value, delta):
        if isinstance(value, dict):
            return {key: shifted(item, delta) for key, item in value.items()}
        if isinstance(value, list):
            return [shifted(item, delta) for item in value]
        if isinstance(value, str) and (parsed := worker.parse_utc(value)) is not None:
            return (parsed+delta).isoformat()
        return value
    # Serialization/reader scale uses eight clock-shifted copies of the fully
    # computed synthetic frame. It does not claim eight timed live captures.
    paths = []
    for minute in (0, 5, 10, 15, 30, 45, 55, 60):
        clone = shifted(snapshot, timedelta(minutes=minute))
        clone["generated_epoch"] = (NOW+timedelta(minutes=minute)).timestamp()
        clone["snapshot_id"] = f"synthetic-rich-benchmark-{minute}"
        paths.append(archive_observation_snapshot(clone, output/"archive"))
    read_started = time.perf_counter()
    result = read_feature_move_maps(output/"archive", as_of_utc=(NOW+timedelta(minutes=60)).isoformat(), window_secs=(300, 900, 3600), include_all_comparisons=True)
    read_sec = time.perf_counter()-read_started
    for window, payload in result.items():
        assert payload["status"] == "available", (window, payload["summary"])
        assert payload["archive"]["errors"] == [], payload["archive"]
        assert len(payload["pairs"]) == 68
        assert payload["summary"]["available"] >= 643*68
    legacy = Path(r"C:\Users\zmoor\Documents\forex\git_publication_20260913\obs_universe_v3\tmp_absent")
    old_paths = list(legacy.glob("*/observations/date=*/hour=*/obs_*.json.gz"))
    assert len(old_paths) == 8
    for path in old_paths:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            old = json.load(stream)
        assert canonical_bytes(old["frame"]) == canonical_bytes(build_observation_frame(old["original_snapshot"]))
    receipt = {"status": "passed", "synthetic_only": True, "calculator_calls_pairs": 68, "rich_ma_values_per_pair": 643, "per_pair_scalar_count_range": [min(per_pair.values()), max(per_pair.values())], "frame_scalar_count": sum(per_pair.values()), "calculation_seconds": round(calculated_sec, 3), "frame_expanded_bytes": size, "eight_archive_compressed_bytes": sum(path.stat().st_size for path in paths), "shared_three_window_reader_seconds": round(read_sec, 3), "comparisons": {str(key): payload["summary"] for key, payload in result.items()}, "legacy_v3_frames_recreated_identically": len(old_paths), "total_seconds": round(time.perf_counter()-started, 3), "source_sha256": {name: hashlib.sha256((PROJECT/name).read_bytes()).hexdigest() for name in ("oanda_research_feature_calculator_v1.py", "oanda_research_feature_observation_worker_v1.py", "oanda_feature_observations_v1.py", "oanda_feature_move_mapping_v1.py")}}
    (output/"RICH_PRODUCER_BENCHMARK.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    print(json.dumps({key: receipt[key] for key in ("status", "frame_scalar_count", "calculation_seconds", "frame_expanded_bytes", "eight_archive_compressed_bytes", "shared_three_window_reader_seconds", "legacy_v3_frames_recreated_identically", "total_seconds")}))


if __name__ == "__main__":
    main()
