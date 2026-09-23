"""Read-only paired comparison on one retained live observation envelope."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import oanda_rolling_technical_alignment_v2 as alignment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('new_readback_output_required')
    root = args.dataset.resolve()
    raw = (root / 'latest_features.json').read_bytes()
    envelope = json.loads(raw)
    observed = time.time()
    rows = {pair: entry['observation'] if entry['availability']['status'] == 'current' and
            entry['observation'] and 0 <= observed - entry['observation']['bar_end_epoch'] <= 180 else None
            for pair, entry in envelope['pairs'].items()}
    connection = sqlite3.connect((root / 'technical.sqlite').as_uri() + '?mode=ro', uri=True, timeout=3)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA query_only=ON')
    connection.execute('BEGIN')
    try:
        contract = json.loads(connection.execute("SELECT value FROM metadata WHERE key='contract'").fetchone()[0])['contract']
        store = SimpleNamespace(connection=connection, names=tuple(contract['feature_names']))
        result = alignment.build_aligned_panels(store, rows, now_epoch=observed,
            maximum_bar_age_seconds=180, max_candidate_minutes=8)
        old = envelope['peer_panel_publication']
        old_counts = Counter(row['status'] for row in old['panel']['by_pair'].values()) if old else Counter()
        comparisons = {}
        for pair, reference in result['by_pair'].items():
            old_peer = old['panel']['by_pair'].get(pair) if old else None
            comparisons[pair] = {'before': None if old_peer is None else {
                'status': old_peer['status'], 'finite_feature_count': old_peer['finite_feature_count'],
                'bar_start_epoch': old_peer['bar_start_epoch'], 'reason': old_peer['reason']},
                'after_reference': reference}
        if connection.total_changes:
            raise AssertionError('read_only_replay_wrote_database')
        report = {'schema': 'rolling_alignment_read_only_replay_v2', 'status': 'passed',
                  'captured_utc': datetime.fromtimestamp(observed, timezone.utc).isoformat(),
                  'envelope_generated_utc': envelope['generated_utc'], 'envelope_sha256': hashlib.sha256(raw).hexdigest(),
                  'module_sha256': hashlib.sha256(Path(alignment.__file__).read_bytes()).hexdigest(),
                  'before_latest_panel_status_counts': dict(old_counts),
                  'after_per_pair_exact_alignment_counts': dict(Counter(row['status'] for row in result['by_pair'].values())),
                  'retained_rows_read': result['retained_rows_read'],
                  'candidate_target_epochs': result['candidate_target_epochs'],
                  'structural_peer_limited_pairs': result['structural_peer_limited_pairs'],
                  'comparisons': comparisons, 'database_writes': connection.total_changes,
                  'scope': 'Same captured local inputs; retained exact-clock peer observations; no predictions or trading changes',
                  'original_local_vectors_replaced': False, 'panels_published': False}
    finally:
        connection.rollback()
        connection.close()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(report, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write('\n')
    print(json.dumps({key: report[key] for key in ('status', 'captured_utc', 'before_latest_panel_status_counts', 'after_per_pair_exact_alignment_counts', 'retained_rows_read', 'database_writes')}))


if __name__ == '__main__':
    main()
