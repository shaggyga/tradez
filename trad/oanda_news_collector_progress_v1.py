"""Publish genuine collector progress promptly; preserve ingestion and clocks."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import threading
import time
from collections.abc import Sequence
from functools import wraps

import oanda_local_news_sentiment as collector

SCHEMA = 'collector_progress_publication_v1_20261001'
BASE_SHA256 = '44e66d85f82b52d6dc82e2e277bad31d2ee8c16304bec9c6a083a8b17eeb6c50'
OriginalProgress = collector.CollectorCycleProgress
original_publish = collector.publish_collector_cycle_heartbeat
active_progress = threading.local()
original_groups = collector._cluster_candidate_groups


def report_completed(step, completed, total):
    progress = getattr(active_progress, 'value', None)
    if progress is not None and progress.phase == 'postprocessing_evidence':
        progress.update('postprocessing_evidence', dict(
            operational_completed_step=step, operational_completed_items=completed,
            operational_total_items=total))


class CompletedItems(Sequence):
    """Read-only view; iteration reports only AFTER the consumer finishes an item."""
    def __init__(self, items, step):
        self.items, self.step = items, step

    def __len__(self):
        return len(self.items)

    def __getitem__(self, index):
        return self.items[index]

    def __iter__(self):
        last_report = time.monotonic()
        for count, item in enumerate(self.items, 1):
            yield item
            # Resumption proves the consumer advanced beyond this item. A
            # stalled body, abandoned iterator or exception cannot renew it.
            if count == len(self.items) or time.monotonic() - last_report >= 2:
                report_completed(self.step, count, len(self.items))
                last_report = time.monotonic()


def grouped_with_progress(candidates):
    groups = original_groups(CompletedItems(candidates, 'cluster_candidates_completed'))
    return CompletedItems(groups, 'cluster_groups_completed')


def completed_call(function, name):
    @wraps(function)
    def call(*args, **kwargs):
        result = function(*args, **kwargs)
        report_completed(name + '_completed', 1, 1)
        return result
    return call


def install_postprocessing_progress():
    collector._cluster_candidate_groups = grouped_with_progress
    for name in ('cluster_articles', 'cluster_context_articles',
                 'build_persistent_policy_state', 'reconcile_topic_history',
                 'upsert_topic_events', 'write_ledger'):
        setattr(collector, name, completed_call(getattr(collector, name), name))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def publish(*, output_root, progress, status='running_cycle', cycle_in_progress=True):
    # Serialize periodic and progress-driven writers over snapshot AND replace.
    # An older snapshot must not overwrite one from a later completed update.
    with progress.publication_lock:
        value = original_publish(output_root=output_root, progress=progress,
            status=status, cycle_in_progress=cycle_in_progress)
        progress.last_publication_monotonic = time.monotonic()
        return value


def progress_type(output_root):
    class Progress(OriginalProgress):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._lock = threading.RLock()
            self.publication_lock = threading.RLock()
            self.last_publication_monotonic = float('-inf')
            active_progress.value = self

        def snapshot(self, **kwargs):
            # Sample generated time while the progress state cannot advance.
            with self._lock:
                value = super().snapshot(**kwargs)
                value['operational_progress_schema'] = SCHEMA
                return value

        def update(self, phase, details=None):
            previous = self.phase
            super().update(phase, details)
            # Every phase transition is published. Same-phase actual work is
            # bounded to one write/2sec; a timer alone never advances progress.
            if previous != self.phase or time.monotonic() - self.last_publication_monotonic >= 2:
                try:
                    publish(output_root=output_root, progress=self)
                except OSError:
                    # Same diagnostic-only failure policy as the original
                    # periodic publisher; the original stale gate stays active.
                    pass
    return Progress


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--progress-config', type=Path, required=True)
    options, remaining = parser.parse_known_args()
    config = json.loads(options.progress_config.read_bytes())
    if (config.get('schema_version') != SCHEMA
            or config.get('collector_source_sha256') != BASE_SHA256
            or sha(collector.__file__) != BASE_SHA256
            or config.get('wrapper_source_sha256') != sha(__file__)
            or config.get('can_place_orders') is not False):
        raise ValueError('exact_collector_progress_binding_required')
    sys.argv = [sys.argv[0], *remaining]
    args = collector.parse_args()
    collector.CollectorCycleProgress = progress_type(args.output_root)
    collector.publish_collector_cycle_heartbeat = publish
    install_postprocessing_progress()
    return collector.main()


if __name__ == '__main__':
    raise SystemExit(main())
