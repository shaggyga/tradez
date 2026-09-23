"""Exercise an upgraded cycle on an isolated coherent SQLite backup only."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import oanda_rolling_technical_worker_v2 as worker


def fingerprint(connection, cutoffs):
    result = hashlib.sha256()
    count = 0
    for pair, cutoff in sorted(cutoffs.items()):
        for row in connection.execute('SELECT pair,t,feature_hash,values_blob,feature_count,available_count,published FROM observations WHERE pair=? AND t<=? ORDER BY t', (pair, cutoff)):
            for value in row:
                raw = value if isinstance(value, bytes) else repr(value).encode()
                result.update(len(raw).to_bytes(8, 'big')); result.update(raw)
            count += 1
    return {'rows': count, 'sha256': result.hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--output-root', type=Path, required=True)
    parser.add_argument('--receipt', type=Path, required=True)
    args = parser.parse_args()
    operations, base = worker.read_config(args.config)
    source_root = Path(base['output_root']).resolve()
    output = args.output_root.resolve()
    if not output.is_relative_to(ROOT / 'data') or output == source_root or output.exists() or args.receipt.exists():
        raise ValueError('fresh_isolated_project_smoke_output_required')
    output.mkdir()
    source = sqlite3.connect((source_root / 'technical.sqlite').as_uri() + '?mode=ro', uri=True, timeout=3)
    source.execute('PRAGMA query_only=ON')
    destination = sqlite3.connect(output / 'technical.sqlite')
    start = time.monotonic()
    def progress(status, remaining, total):
        if time.monotonic() - start > 90:
            raise RuntimeError('bounded_snapshot_backup_timeout')
    try:
        source.execute('BEGIN')
        source.execute('SELECT COUNT(*) FROM observations').fetchone()
        source.backup(destination, pages=4096, progress=progress, sleep=.01)
    finally:
        destination.close(); source.rollback(); source.close()
    copied_at = worker.original.utc()
    smoke_base = {**base, 'output_root': str(output)}
    store = worker.TechnicalStore(output / 'technical.sqlite', worker.original.contract(base), max_bytes=base['maximum_dataset_bytes'])
    try:
        cutoffs = {pair: store.latest_time(pair) for pair in base['pairs']}
        before = fingerprint(store.connection, cutoffs)
        runtime_id, runtime = worker.record_runtime(store, operations, args.config)
        reports = []
        cache = {}
        for i in range(2):
            report = worker.run_cycle(operations, smoke_base, store, cache, runtime_id=runtime_id)
            reports.append({k: report[k] for k in ('generated_utc', 'current_pairs', 'fully_populated_pairs', 'elapsed_seconds', 'peer_alignment', 'incremental_computation', 'inserted_this_cycle', 'outcomes_settled_this_cycle')})
            print(json.dumps({'smoke_cycle': i + 1, **reports[-1]}), flush=True)
        after = fingerprint(store.connection, cutoffs)
        if before != after:
            raise AssertionError('existing_observation_bytes_or_publications_changed')
        integrity = store.connection.execute('PRAGMA quick_check').fetchone()[0]
        if integrity != 'ok':
            raise AssertionError('isolated_database_integrity_failed')
    finally:
        store.close()
    receipt = {'schema': 'rolling_operations_isolated_resume_smoke_v2', 'status': 'passed',
        'source_root': str(source_root), 'isolated_output_root': str(output),
        'snapshot_completed_utc': copied_at, 'completed_utc': worker.original.utc(),
        'config_sha256': worker.original.sha(args.config), 'source_bindings': operations['source_bindings'],
        'existing_observations_before': before, 'existing_observations_after': after,
        'runtime_id': runtime_id, 'cycles': reports, 'quick_check': integrity,
        'original_database_writes': False, 'can_place_orders': False, 'models_promoted': 0}
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    with args.receipt.open('x', encoding='utf-8') as stream:
        json.dump(receipt, stream, indent=2, sort_keys=True, allow_nan=False); stream.write('\n')
    print(json.dumps({'status': 'passed', 'receipt': str(args.receipt), 'existing_observations_unchanged': before['rows']}))


if __name__ == '__main__':
    main()
