"""Change-triggered successor; reuses the pinned V2 calculator and store.

Local file metadata is checked once a second, not broker data. A changed source
or a minute boundary wakes computation. No polling/network collector is added.
"""
import argparse
import json
import math
from pathlib import Path
import time

import oanda_rolling_technical_worker_v2 as previous


def source_signature(base):
    result = []
    for pair in sorted(base['pairs']):
        path = Path(base['candle_root']) / (pair + '_M1.csv')
        try:
            s = path.stat()
            result.append((pair, s.st_size, s.st_mtime_ns, s.st_ino))
        except FileNotFoundError:
            result.append((pair, None))
    return tuple(result)


class WakeGate:
    def __init__(self):
        self.signature = None
        self.minute = None

    def reason(self, signature, now):
        minute = math.floor(now / 60)
        why = ('startup' if self.signature is None else 'source_changed'
               if signature != self.signature else 'minute_boundary'
               if minute != self.minute else None)
        if why:
            # Capture before computation so a change during computation remains
            # visible at the next probe (never advance past unseen input).
            self.signature, self.minute = signature, minute
        return why


def latency_rows(store):
    result = []
    for pair in store.contract['pairs']:
        r = store.connection.execute('''SELECT o.t,o.published,b.first_observed
          FROM observations o JOIN bars b ON b.pair=o.pair AND b.t=o.t
          WHERE o.pair=? AND o.published>0 ORDER BY o.t DESC LIMIT 1''', (pair,)).fetchone()
        if r:
            result.append({'pair': pair, 'bar_close_epoch': r['t'] + 60,
                'source_read_completed_epoch': r['first_observed'],
                'feature_published_epoch': r['published'],
                'close_to_read_seconds': r['first_observed'] - r['t'] - 60,
                'read_to_publication_seconds': r['published'] - r['first_observed'],
                'upstream_arrival_epoch': None})
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True, help='Existing source-pinned V2 operations config')
    parser.add_argument('--duration-sec', type=float, default=3600)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args(argv)
    if not math.isfinite(args.duration_sec) or not 0 < args.duration_sec <= 604800:
        parser.error('duration must be finite, positive and at most seven days')
    operations, base = previous.read_config(args.config)
    original = previous.original
    expected = original.sha(args.config)
    output = Path(base['output_root'])
    output.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.duration_sec
    gate, cache = WakeGate(), {}
    with original.owner_lock(output):
        store = previous.TechnicalStore(output / 'technical.sqlite', original.contract(base),
                                        max_bytes=base['maximum_dataset_bytes'])
        try:
            runtime_id, runtime = previous.record_runtime(store, operations, args.config)
            original.atomic_json(output / 'change_worker_owner.json', {
                **runtime, 'scheduler_source_sha256': original.sha(__file__),
                'metadata_probe_seconds': 1, 'duration_seconds': args.duration_sec})
            while time.monotonic() < deadline:
                if (output / 'operations_stop_requested.json').exists():
                    break
                why = gate.reason(source_signature(base), time.time())
                if why:
                    if original.sha(args.config) != expected:
                        raise ValueError('operations_config_changed_restart_required')
                    previous.read_config(args.config)
                    started = time.time()
                    report = previous.run_cycle(operations, base, store, cache, runtime_id=runtime_id)
                    original.atomic_json(output / 'latency_current.json', {
                        'schema': 'technical_change_trigger_v1', 'trigger': why,
                        'detected_epoch': started, 'cycle_completed_epoch': time.time(),
                        'rows': latency_rows(store), 'status': report['status'],
                        'research_only': True, 'can_place_orders': False,
                        'limitation': 'Source receipt timestamps are read completion, not upstream arrival.'})
                if args.once:
                    break
                time.sleep(min(1., max(0., deadline-time.monotonic())))
        finally:
            store.close()


if __name__ == '__main__':
    main()
