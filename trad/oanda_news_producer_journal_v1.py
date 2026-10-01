"""Durable operational failure/recovery records over the unchanged news producer."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import uuid

import oanda_local_news_sentiment_repair_v2 as producer

SCHEMA = 'news_producer_journal_v1_20261001'
BASE_SHA256 = '542e287e44330d2e83ef2ec1a9b363bc716cf5581cf8b05bce87de84c7fa4124'
original_cycle = producer.run_cycle


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def health_evidence(root):
    result = {}
    for name, relative in [('collector', 'local_news_sentiment/collector_heartbeat_v1.json'),
                           ('clock', 'state/clock_integrity_v1.json'),
                           ('producer', 'local_news_sentiment_repair_v2/heartbeat_v2.json')]:
        try:
            with (Path(root) / relative).open('rb') as handle:
                raw = handle.read(131073)
            if len(raw) > 131072:
                raise ValueError('diagnostic_input_byte_bound')
            value = json.loads(raw)
            result[name] = dict(sha256=hashlib.sha256(raw).hexdigest(), fields={
                k: value.get(k) for k in ('generated_utc', 'status', 'last_progress_utc',
                    'phase', 'errors', 'last_error', 'timestamp_normalization_trusted',
                    'host_clock_synchronized', 'clock_discontinuity_active')})
        except (OSError, ValueError, TypeError) as exc:
            result[name] = dict(read_error=type(exc).__name__ + ':' + str(exc)[:180])
    return result


class Journal:
    def __init__(self, root, metadata, clock=time.time):
        self.root = Path(root)
        self.clock = clock
        self.failed = False
        self.first = True
        directory = self.root / 'local_news_sentiment_repair_v2/operational_producer_receipts'
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / (uuid.uuid4().hex + '.jsonl')
        self.handle = self.path.open('xb')
        self.record('start', metadata=metadata, evidence=health_evidence(root))

    def record(self, event, **details):
        # Logging does not turn rejected data into accepted data. Failure to log
        # is explicit on stderr and never replaces the producer's own result.
        try:
            row = dict(schema_version=SCHEMA, observed_epoch=self.clock(), event=event, **details)
            raw = producer.encoded(row)
            if len(raw) > 16384:
                raise ValueError('diagnostic_record_byte_bound')
            self.handle.write(raw + b'\n')
            self.handle.flush()
            os.fsync(self.handle.fileno())
        except (OSError, ValueError) as exc:
            print('news_producer_journal_failed:' + type(exc).__name__, file=sys.stderr, flush=True)

    def cycle(self, data_root, **kwargs):
        try:
            heartbeat = original_cycle(data_root, **kwargs)
        except Exception as exc:
            self.failed = True
            self.record('unhandled_cycle_failure', error=type(exc).__name__ + ':' + str(exc)[:300],
                        evidence=health_evidence(data_root))
            raise
        failure = bool(heartbeat.get('last_error')) or heartbeat.get('status') != 'current'
        if failure or self.failed or self.first:
            self.record('cycle_failure' if failure else ('recovered' if self.failed else 'first_current'),
                        heartbeat=heartbeat, evidence=health_evidence(data_root))
        self.failed = failure
        self.first = False
        return heartbeat

    def close(self):
        self.handle.close()


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--journal-config', type=Path, required=True)
    parser.add_argument('--data-root', type=Path, required=True)
    args, remaining = parser.parse_known_args()
    config = json.loads(args.journal_config.read_bytes())
    if (config.get('schema_version') != SCHEMA
            or config.get('producer_source_sha256') != BASE_SHA256
            or sha(producer.__file__) != BASE_SHA256
            or config.get('wrapper_source_sha256') != sha(__file__)
            or config.get('can_place_orders') is not False):
        raise ValueError('exact_producer_journal_binding_required')
    journal = Journal(args.data_root, dict(config=config, config_sha256=sha(args.journal_config), pid=os.getpid()))
    producer.run_cycle = journal.cycle
    try:
        return producer.main(['--data-root', str(args.data_root), *remaining])
    finally:
        producer.run_cycle = original_cycle
        journal.close()


if __name__ == '__main__':
    raise SystemExit(main())
