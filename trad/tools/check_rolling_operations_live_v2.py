"""Read-only post-cutover check against the retained coherent resume checkpoint."""
from __future__ import annotations
import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.check_rolling_operations_resume_v2 import fingerprint
import oanda_rolling_technical_worker_v2 as worker


def readonly(path):
    connection = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=3)
    connection.execute('PRAGMA query_only=ON')
    connection.execute('BEGIN')
    return connection


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--smoke-receipt', type=Path, required=True)
    parser.add_argument('--receipt', type=Path, required=True)
    args = parser.parse_args()
    if args.receipt.exists():
        raise ValueError('fresh_receipt_required')
    operations, base = worker.read_config(args.config)
    smoke = json.loads(args.smoke_receipt.read_text(encoding='utf-8'))
    assert smoke['status'] == 'passed'
    snapshot_epoch = datetime.fromisoformat(smoke['snapshot_completed_utc']).timestamp()
    archived = readonly(Path(smoke['isolated_output_root']) / 'technical.sqlite')
    try:
        # Resume smoke rows were first published after the coherent copy.
        # Recover its original boundary and prove the complete saved digest.
        cutoffs = dict(archived.execute('SELECT pair,MAX(t) FROM observations WHERE published<=? GROUP BY pair', (snapshot_epoch,)))
        expected = fingerprint(archived, cutoffs)
        assert expected == smoke['existing_observations_before']
    finally:
        archived.close()
    output = Path(base['output_root'])
    live = readonly(output / 'technical.sqlite')
    try:
        actual = fingerprint(live, cutoffs)
        assert expected == actual, 'preexisting_observation_bytes_or_publications_changed'
        integrity = live.execute('PRAGMA quick_check').fetchone()[0]
        assert integrity == 'ok'
        counts = {table: live.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0]
                  for table in ('bars', 'observations', 'outcomes', 'panels', 'receipts', 'revisions')}
    finally:
        live.close()
    for _ in range(5):
        status = json.loads((output / 'status.json').read_text(encoding='utf-8'))
        envelope = json.loads((output / 'latest_features.json').read_text(encoding='utf-8'))
        if status.get('publication_generation') == envelope.get('publication_generation'):
            break
        time.sleep(.1)
    assert status['publication_generation'] == envelope['publication_generation']
    assert status['operations_schema'] == worker.SCHEMA
    assert status['operations_runtime_id'] == envelope['operations_runtime_id']
    now = time.time()
    assert 0 <= now - status['generated_epoch'] <= 180, 'current_worker_publication_required'
    aligned_status = Counter()
    stale_now = []
    checks = 0
    for pair in base['pairs']:
        row = envelope['pairs'][pair]
        observation = row['observation']
        if observation is not None and now - observation['bar_end_epoch'] > base['maximum_bar_age_seconds']:
            stale_now.append(pair)
        state = row['aligned_state']
        if state is None:
            aligned_status['unavailable'] += 1
            continue
        # Worker attachment already checks clock/hash/publication identity.
        # Recheck against its durable selected panel and read-time cutoff.
        ref = row['peer_alignment']
        panel = next(p for p in envelope['aligned_panel_publications']
                     if p['target_bar_start_epoch'] == ref['target_bar_start_epoch'])
        assert state['observation']['bar_start_epoch'] == panel['target_bar_start_epoch']
        assert state['availability_epoch'] >= state['observation']['published_epoch']
        assert state['availability_epoch'] >= panel['published_epoch']
        assert state['availability_epoch'] <= envelope['generated_epoch'] <= now
        aligned_status[state['peer']['status']] += 1
        checks += 1
    result = {'schema': 'rolling_operations_live_readback_v2', 'status': 'passed',
              'observed_utc': worker.original.utc(now), 'worker_publication_utc': status['generated_utc'],
              'operations_runtime_id': status['operations_runtime_id'],
              'source_bindings': operations['source_bindings'],
              'config_sha256': worker.original.sha(args.config), 'counts': counts,
              'preexisting_observations_preserved': actual, 'database_quick_check': integrity,
              'publication_generation': status['publication_generation'],
              'current_pairs_at_worker_publication': status['current_pairs'],
              'fully_populated_pairs_at_worker_publication': status['fully_populated_pairs'],
              'aligned_status_at_publication': dict(aligned_status), 'aligned_clock_checks': checks,
              'local_observations_expired_since_publication': stale_now,
              'cycle_seconds': status['elapsed_seconds'], 'dataset_bytes': status['dataset_bytes'],
              'storage_fraction': status['storage_fraction'], 'database_writes': 0,
              'can_place_orders': False}
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    with args.receipt.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
