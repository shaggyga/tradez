"""Operational scheduling successor over the unchanged v11 numeric cohort.

The separate append-only receipt journal identifies the scheduler for new
forecasts. Original worker bytes, contracts, gates and historical rows remain
unchanged. Results across scheduler versions are not independent populations.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import time
import threading
import oanda_news_health_retry_v1 as health_retry
import uuid

import oanda_joint_price_news_forecast_study_v11 as base

SCHEMA = 'joint_news_scheduler_v1_20261001'
BASE_SHA256 = '37f244b7a7b645cd2e8a443970300a88a348450956bafcfedf677f978cff3563'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_overlay(path, registry_path):
    value = json.loads(base.bounded_bytes(base.plain_c_path(path), 65536))
    if value.get('schema_version') != SCHEMA:
        raise ValueError('scheduler_configuration_identity')
    if (value.get('base_source_sha256') != BASE_SHA256
            or sha(base.__file__) != BASE_SHA256
            or value.get('scheduler_source_sha256') != sha(__file__)
            or value.get('registry_sha256') != sha(registry_path)
            or value.get('health_retry_source_sha256') != sha(health_retry.__file__)):
        raise ValueError('scheduler_exact_source_and_registry_binding')
    if value.get('changes') != ['overdue_news_before_post_poll_pair_dispatch', 'one_full_capture_retry_after_strict_healthy_recheck', 'skip_already_published_market_reference', 'bounded_exact_health_file_permission_retry']:
        raise ValueError('scheduler_scope_changed')
    if value.get('can_place_orders') is not False or value.get('research_only') is not True:
        raise ValueError('nontrading_scheduler_required')
    return value


class Receipts:
    """One exclusive durable journal per process, never retroactive attribution."""
    def __init__(self, directory, metadata, clock=time.time):
        self.clock = clock
        self.write_lock = threading.Lock()
        self.run_id = uuid.uuid4().hex
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / (self.run_id + '.jsonl')
        self.handle = self.path.open('xb')
        self.write('start', **metadata)

    def write(self, event, **details):
        row = dict(schema_version=SCHEMA, run_id=self.run_id,
                   observed_epoch=self.clock(), event=event, **details)
        with self.write_lock:
            self.handle.write(base.encoded(row) + b'\n')
            self.handle.flush()
            os.fsync(self.handle.fileno())

    def close(self):
        self.handle.close()


class ScheduledRunner(base.PairRunner):
    def __init__(self, *args, receipts, **kwargs):
        self.receipts = receipts
        super().__init__(*args, **kwargs)
        health_config = json.loads(self.news_io._session(self.news_session)['config'])
        health_retry.install_health_read_retry(self.news_io,
            [health_config['clock_path'], health_config['heartbeat_path']],
            record=self.receipts.write)
        self.news_pool = health_retry.NewsExecutor(self.news_pool, base.capture_shared_owned,
            recheck=lambda: self.news_io._health(
                json.loads(self.news_io._session(self.news_session)['config']), self.clock),
            record=self.receipts.write)
        self.receipts.write('baseline', ledgers=[
            dict(instrument=pair, family=family, contract_sha256=slot['ledger'].contract_hash,
                 max_forecast_rowid=slot['ledger'].db.execute(
                     'SELECT COALESCE(MAX(rowid),0) FROM forecasts').fetchone()[0])
            for pair, state in self.states.items()
            for family, slot in state['families'].items()])

    def schedule_fit(self, now, bucket):
        # The original issue gate prohibits a second forecast for this reference,
        # even in a later cadence bucket. Avoid computing a guaranteed refusal.
        # Use the original failed-basis mechanism: a new capture/bucket/failure
        # generation is reconsidered normally, never labelled a successful fit.
        for pair, state in self.states.items():
            capture = state.get('capture')
            if (capture is None or state.get('news_capture') is None
                    or capture.get('reference_start_epoch') is None
                    or capture.get('source_capture_sha256') is None):
                continue
            reference = base.number(capture['reference_start_epoch']) + 60
            try:
                hint = self.news_hint(state['news_capture'])
            except ValueError:
                continue  # Original scheduler performs failure invalidation.
            if hint is None:
                continue
            basis = base.schedule.failed_fit_basis(capture['source_capture_sha256'], bucket, hint[1])
            for family, slot in state['families'].items():
                if slot['failed_basis'] == basis:
                    continue
                row = slot['ledger'].db.execute(
                    'SELECT id,sha FROM forecasts WHERE reference=?', (reference,)).fetchone()
                if row is not None:
                    self.receipts.write('duplicate_reference_skipped', instrument=pair,
                        family=family, reference=reference, existing_forecast_id=row[0],
                        existing_forecast_sha256=row[1],
                        source_capture_sha256=capture['source_capture_sha256'], bucket=bucket)
                    slot['failed_basis'] = basis
        return super().schedule_fit(now, bucket)

    def finish_work(self):
        # Only attribute rows emitted by this exact completed call. A crash gap
        # remains unknown; a later process must never backfill it as its own work.
        watched = None
        if self.future is not None and self.future.done() and self.active[0] == 'fit':
            _, pair, detail = self.active
            family = detail[0]
            ledger = self.states[pair]['families'][family]['ledger']
            high = ledger.db.execute('SELECT COALESCE(MAX(rowid),0) FROM forecasts').fetchone()[0]
            watched = pair, family, ledger, high
        super().finish_work()
        if watched is not None and self.future is None:
            pair, family, ledger, high = watched
            rows = ledger.db.execute(
                'SELECT id,sha,reference,target FROM forecasts WHERE rowid>? ORDER BY rowid', (high,)).fetchall()
            self.receipts.write('fit_completed', instrument=pair, family=family,
                contract_sha256=ledger.contract_hash,
                forecasts=[dict(id=r[0], sha256=r[1], reference=r[2], target=r[3]) for r in rows])

    def tick(self):
        # Same v11 ordering, with overdue news checked at the second handoff too.
        self.finish_news(); self.finish_work()
        if (self.future is None and self.news_future is None
                and self.news_bootstrap_future is None and self.fresh_capture_pair is not None):
            handoff_now = base.number(self.clock())
            self.schedule_fit(handoff_now, int(handoff_now // 900))
        self.schedule_news(); self.schedule_work()
        now = base.number(self.clock())
        if now - self.last_poll >= 2:
            self.poll_quotes(); self.last_poll = now
            for pair, state in self.states.items():
                for slot in state['families'].values():
                    try:
                        slot['ledger'].settle()
                    except Exception as exc:
                        self.error(pair, exc)
        self.finish_work(); self.schedule_news(); self.schedule_work(); self.score()
        try:
            self.publish_status()
        except (OSError, base.summary_boundary.SummaryBoundaryError) as exc:
            base._status_failure_record(self, exc)


def run(config, scheduler_config, *, study=base.STUDY, candle_root=None,
        quote_path=None, news_io_config=None, duration_sec=0, once=False):
    overlay = validate_overlay(scheduler_config, config)
    paths = base.resolve_runtime_paths(study=study, candle_root=candle_root,
                                     quote_path=quote_path, news_io_config=news_io_config)
    registry = base.load_registry(config)
    base.verify_activated_study(registry, paths['study'])
    for variable in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
                     'NUMEXPR_NUM_THREADS', 'LOKY_MAX_CPU_COUNT'):
        os.environ[variable] = '1'
    import msvcrt
    lock = (paths['study'] / 'worker.lock').open('a+b')
    lock.seek(0)
    if not lock.read(1):
        lock.write(b'0'); lock.flush()
    lock.seek(0)
    try:
        msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError:
        lock.close()
        raise RuntimeError('pair_worker_already_running')
    runner = receipts = None
    begun = time.monotonic()
    try:
        receipts = Receipts(paths['study'] / 'operational_scheduler_receipts', dict(
            scheduler_config=overlay, scheduler_config_sha256=sha(scheduler_config),
            registry_sha256=sha(config), original_contracts_unchanged=True,
            unrecorded_crash_gap_policy='unknown_never_backfill', pid=os.getpid()))
        session = base.create_news_session(registry, paths['news_io_config'])
        runner = ScheduledRunner(registry, paths['study'], paths['candle_root'],
            paths['quote_path'], session=session, activate=False, receipts=receipts)
        if once:
            runner.poll_quotes(); runner.publish_status(force=True)
            return
        while True:
            runner.tick()
            if duration_sec > 0 and time.monotonic() - begun >= duration_sec:
                break
            time.sleep(.2)
        receipts.write('stop', reason='duration_completed')
    finally:
        try:
            if runner is not None:
                runner.close()
        finally:
            if receipts is not None:
                receipts.close()
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
            lock.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=base.DEFAULT_CONFIG)
    parser.add_argument('--scheduler-config', type=Path, required=True)
    parser.add_argument('--study', type=Path, default=base.STUDY)
    parser.add_argument('--candle-root', type=Path)
    parser.add_argument('--quote-path', type=Path)
    parser.add_argument('--news-io-config', type=Path, required=True)
    parser.add_argument('--duration-sec', type=float, default=0)
    parser.add_argument('--once', action='store_true')
    args = vars(parser.parse_args())
    run(**args)


if __name__ == '__main__':
    main()
