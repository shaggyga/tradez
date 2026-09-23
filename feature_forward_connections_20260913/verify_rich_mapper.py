import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import time

OUT = Path(__file__).parent
ROOT = OUT.parent / "trad"
FIXTURE = OUT.parent / "git_publication_20260913/rich_producer_bench_v2/archive"
sys.path.insert(0, str(ROOT))
import oanda_feature_move_mapping_v1 as current
import oanda_feature_observations_v1 as observations

files = sorted(FIXTURE.glob("date=20260913/hour=*/obs_*.json.gz"))
assert len(files) == 8
frames = []
for path in files:
    with gzip.open(path, "rb") as stream:
        raw = stream.read(32 * 1024 * 1024 + 1)
    assert len(raw) < 32 * 1024 * 1024
    envelope = json.loads(raw)
    frame = observations.build_observation_frame(envelope["original_snapshot"])
    assert current._canonical(frame) == current._canonical(envelope["frame"])
    frames.append(frame)
as_of = max(frame["generated_utc"] for frame in frames)
start = time.perf_counter()
results = current.read_feature_move_maps(FIXTURE, as_of_utc=as_of, include_all_comparisons=True)
elapsed = time.perf_counter() - start
old_path = OUT.parent / "git_publication_20260913/mapper_performance_review_001/source_001/oanda_feature_move_mapping_v1.py"
spec = importlib.util.spec_from_file_location("previous_mapper", old_path)
old = importlib.util.module_from_spec(spec)
spec.loader.exec_module(old)
checks = {}
for window, result in results.items():
    expected = old.build_feature_move_map(frames, as_of_utc=as_of, window_sec=window, include_all_comparisons=True)
    comparable = {key: value for key, value in result.items() if key != "archive"}
    assert current._canonical(comparable) == current._canonical(expected), window
    checks[window] = {"comparisons": len(result["all_feature_changes"]),
                      "sha256": hashlib.sha256(current._canonical(comparable)).hexdigest()}
proof = {"synthetic_only": True, "status": "passed", "old_frame_recreation_count": 8,
         "new_shared_reader_seconds": elapsed, "prior_measured_reader_seconds": 52.732,
         "whole_results_equal_preoptimization": checks,
         "source_sha256": {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                            for p in (Path(current.__file__), Path(observations.__file__), old_path)}}
destination = OUT / "RICH_MAPPER_VALIDATION.json"
assert not destination.exists()
destination.write_text(json.dumps(proof, indent=2) + "\n", encoding="utf-8")
print(json.dumps(proof))
