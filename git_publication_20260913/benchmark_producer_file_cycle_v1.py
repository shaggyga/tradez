"""Actual producer cycle on 204 freshly generated synthetic candle files."""
from datetime import datetime, timezone
import gzip
import hashlib
import itertools
import json
import random
from pathlib import Path
import sys
import time

PROJECT = Path(r"C:\Users\zmoor\Documents\forex\trad")
sys.path.insert(0, str(PROJECT))
import oanda_research_feature_observation_worker_v1 as worker
from test_oanda_research_feature_observation_worker_v1 import arguments, candles, quotes, valid_clock, write_candles

output = Path(sys.argv[1])
output.mkdir(exist_ok=False)
args = arguments(output)
args.max_cycle_sec = 30
pairs = [f"{a}_{b}" for a,b in itertools.combinations("USD EUR JPY GBP CHF AUD CAD NZD SEK NOK DKK PLN CZK HUF TRY ZAR MXN SGD HKD CNH THB".split(), 2)][:68]
now = datetime.now(timezone.utc)
row_count = int(sys.argv[2]) if len(sys.argv) > 2 else 512
for pair_index, pair in enumerate(pairs):
    for tf in worker.NATIVE_SECONDS:
        rows = candles(tf, count=row_count, now=now)
        rng = random.Random(pair + tf)
        for row in rows:
            shift = pair_index * .001 + rng.uniform(-.000003, .000003)
            for side in ("mid", "bid", "ask"):
                for key, value in row[side].items():
                    row[side][key] = round(value + shift, 8)
        write_candles(args.candle_root/f"{pair}_{tf}.csv", pair, tf, rows)
now = datetime.now(timezone.utc)
args.quote_snapshot.write_text(json.dumps(quotes(pairs, now=now)), encoding="utf-8")
args.clock_state.write_text(json.dumps(valid_clock(now)), encoding="utf-8")
started = time.perf_counter()
result = worker.run_cycle(args)
elapsed = time.perf_counter()-started
with gzip.open(result["archive"], "rt", encoding="utf-8") as stream:
    envelope = json.load(stream)
ready = result["coverage"]["family_readiness"]
receipt = {"synthetic_only": True, "status": result["status"], "end_to_end_seconds": elapsed, "source_read_bytes": result["source_bytes"], "archive_compressed_bytes": Path(result["archive"]).stat().st_size, "expanded_archive_bytes": len(worker.canonical_bytes(envelope)), "pairs": len(envelope["frame"]["instruments"]), "family_readiness": ready, "actual_read_completed_utc": envelope["original_snapshot"]["source_inputs"]["source_read_completed_utc"], "actual_publication_completed_utc": result["last_publication_completed_utc"], "source_sha256": {name: hashlib.sha256((PROJECT/name).read_bytes()).hexdigest() for name in ("oanda_research_feature_observation_worker_v1.py", "oanda_research_feature_calculator_v1.py", "oanda_feature_observations_v1.py")}}
(output/"PRODUCER_FILE_CYCLE_RECEIPT.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
print(json.dumps({k:v for k,v in receipt.items() if k not in ("family_readiness", "source_sha256")}), flush=True)
assert ready["rich_M1_materialized_pairs"] == 68, ready
assert elapsed <= args.max_cycle_sec, elapsed
