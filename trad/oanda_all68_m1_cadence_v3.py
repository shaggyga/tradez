"""Boundary-priority M1 maintenance; original CSV and gap evidence stay unchanged.

Three forward slots are reserved while at most one background gap job runs.
Every pair has one owner, every actual GET shares a rate limit, and only the
versioned complete-candle writer/recovery code can append or recover an archive row.
Large stale-tail downloads require a ten-row proof of a newly completed minute;
registered pip units correct recovered rows without modifying existing rows.
"""
from __future__ import annotations

import argparse
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import threading
import time

ROOT = Path(__file__).resolve().parent
DATA = ROOT / 'data/oanda_training_manager'
SCHEMA = 'all68_m1_boundary_cadence_v3_20260916'
SCHEDULE = 'three_forward_one_background_gap_tail_probe_v2_20260916'
TAIL_PROBE_COUNT = 10
MAX_FORWARD = 3
MAX_GAP = 1
MAX_GET_STARTS_PER_SECOND = 4
FORWARD_RETRY_SEC = 15
GAP_SCAN_INTERVAL_SEC = 900
FORWARD_HTTP_TIMEOUT_SEC = 15
GAP_HTTP_TIMEOUT_SEC = 10
SETTLE_SEC = 2
POLL_SEC = .25
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
FLAGS = dict(research_only=True, can_place_orders=False, can_authorize=False, can_promote=False)
PREDECESSOR_BINDINGS = {
    'oanda_all68_m1_forward_updater.py': '1092ecb773f71559abd330205dccda78e4a063a04855f1336d9ef66d51363320',
    'oanda_gpt_training_strategy_manager.py': 'a3337febaba2673365523d8e1613e01e49bdd06c78ef8462b5a8f7e8221b1cd9',
    'oanda_worker_heartbeat.py': 'b6425797957f13b916f0f8d5c3bbd95b464832d9d4978b0bdeb6f0b07a5f88c4',
}
PREDECESSOR_SCHEMA = 'all68_m1_boundary_cadence_v2_20260914'
SOURCE_BINDINGS = {
    **PREDECESSOR_BINDINGS,
    'oanda_all68_m1_cadence_v2.py': '455df16d61e0f42c3cee74839370c62098fa9f2e0e3b73a117c1348bb674e625',
    'oanda_all68_m1_forward_updater_v2.py': '54aa907e28ef959b41c3223c53083f8f8e64ca828aad51b125cc1a8f9c3eecb0',
    'config/pair_local_operational_v2_20260913.json': 'c9464343f012e0bdd83582480a0044bf1c6774eef2622f312bcdd4599e3869c5',
}


def utc():
    return datetime.now(timezone.utc)


def epoch(value):
    at = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if at.tzinfo is None:
        raise ValueError('aware_source_clock_required')
    return at.timestamp()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def source_check(root=ROOT):
    root = Path(root)
    for name, expected in SOURCE_BINDINGS.items():
        path = root / name
        if path.is_symlink() or path.is_junction() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError('m1_reused_source_changed:' + name)


def checked_directory(path):
    path = Path(path).absolute()
    if any(p.is_symlink() or p.is_junction() for p in (path, *path.parents)):
        raise ValueError('m1_reparse_path_refused')
    path = path.resolve(strict=False)
    if not path.is_relative_to(DATA):
        raise ValueError('m1_path_outside_project_data')
    return path


class GetRateLimit:
    """Limits actual GET starts, including local work delaying thread dispatch."""
    def __init__(self, clock=time.monotonic, sleep=time.sleep):
        self.clock, self.sleep = clock, sleep
        self.starts = deque()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = self.clock()
                while self.starts and self.starts[0] <= now - 1:
                    self.starts.popleft()
                if len(self.starts) < MAX_GET_STARTS_PER_SECOND:
                    self.starts.append(now)
                    return now
                pause = max(.001, self.starts[0] + 1 - now)
            self.sleep(min(pause, .25))


class CandleGetOnly:
    """One instrument/one GET per job; no account/order interface is exposed."""
    def __init__(self, client, pair, limiter, clock=utc):
        self._client, self.pair, self.limiter, self.clock = client, pair, limiter, clock
        self.receipts = []
        self.called = False

    def candles(self, instrument, *, granularity, count, end_time, price):
        if self.called or instrument != self.pair or granularity != 'M1' or price != 'BAM' or not 10 <= count <= 5000:
            raise ValueError('bounded_single_M1_BAM_GET_required')
        self.called = True
        self.limiter.acquire()
        began = self.clock()
        payload = self._client.candles(instrument, granularity=granularity, count=count, end_time=end_time, price=price)
        observed = self.clock()
        if observed < began:
            raise ValueError('GET_clock_rollback')
        raw = encoded(payload)
        if len(raw) > MAX_RESPONSE_BYTES or not isinstance(payload, dict):
            raise ValueError('bounded_candle_response_required')
        record = dict(retrieval_started_utc=began.isoformat(), retrieval_completed_utc=observed.isoformat(),
            instrument=instrument, granularity=granularity, price=price, requested_count=count,
            requested_end_utc=end_time.isoformat() if end_time else None,
            structured_response_sha256=hashlib.sha256(raw).hexdigest(), structured_response_bytes=len(raw),
            http_status=payload.get('_http_status'), source_error=payload.get('_error') is True,
            original_availability_claim=False)
        self.receipts.append(record)
        if not payload.get('_error'):
            if payload.get('instrument') != instrument or payload.get('granularity') != 'M1':
                raise ValueError('candle_response_identity')
            candles = payload.get('candles')
            if not isinstance(candles, list) or len(candles) > count:
                raise ValueError('candle_response_row_bound')
            for row in candles:
                if type(row.get('complete')) is not bool:
                    raise ValueError('explicit_complete_flag_required')
                if row['complete']:
                    start = epoch(row['time'])
                    if start % 60 or start + 60 > observed.timestamp():
                        raise ValueError('completed_source_minute_is_future_or_unaligned')
        return payload


class ForwardProbeOnly:
    """One logical forward call; at most two individually rate-limited GETs.

    A stale archive is probed with ten recent provider rows. Only evidence of a
    newer *completed* minute permits the original bounded (<=5,000) catch-up.
    A missing/incomplete/old provider minute never advances local chronology.
    The probe is not substituted for a required wider catch-up page.
    """
    def __init__(self, client, pair, limiter, local_last, clock=utc):
        self.client, self.pair, self.limiter, self.clock = client, pair, limiter, clock
        self.local_last = None if local_last is None else epoch(local_last.isoformat())
        self.receipts = []
        self.called = False
        self.decision = 'not_called'

    def _get(self, instrument, granularity, count, end_time, price, role):
        scope = CandleGetOnly(self.client, self.pair, self.limiter, self.clock)
        try:
            return scope.candles(instrument, granularity=granularity, count=count,
                                 end_time=end_time, price=price)
        finally:
            self.receipts.extend({**receipt, 'retrieval_role': role} for receipt in scope.receipts)

    def candles(self, instrument, *, granularity, count, end_time, price):
        if self.called or instrument != self.pair or granularity != 'M1' or price != 'BAM' or end_time is not None or not 10 <= count <= 5000:
            raise ValueError('bounded_single_forward_M1_BAM_call_required')
        self.called = True
        if count <= TAIL_PROBE_COUNT or self.local_last is None:
            self.decision = 'ordinary_tail' if self.local_last is not None else 'initial_bounded_page'
            return self._get(instrument, granularity, count, end_time, price, self.decision)
        payload = self._get(instrument, granularity, TAIL_PROBE_COUNT, None, price, 'recent_tail_probe')
        if payload.get('_error'):
            self.decision = 'probe_source_error'
            return payload
        completed = [epoch(row['time']) for row in payload['candles'] if row['complete']]
        newest = max(completed, default=-math.inf)
        if newest <= self.local_last:
            self.decision = 'no_new_completed_minute'
            return payload
        self.decision = 'new_completed_minute_requires_original_catchup'
        full = self._get(instrument, granularity, count, None, price, 'bounded_catchup_after_new_minute')
        if not full.get('_error'):
            full_newest = max((epoch(row['time']) for row in full['candles'] if row['complete']), default=-math.inf)
            if full_newest < newest:
                raise ValueError('catchup_response_regressed_behind_probe')
        return full


class Scheduler:
    """Pure bounded queue. Network completion never blocks the forward planner."""
    def __init__(self, pairs, latest=None):
        if not 1 <= len(pairs) <= 68 or len(set(pairs)) != len(pairs):
            raise ValueError('bounded_unique_pair_universe_required')
        self.pairs = sorted(pairs)
        self.state = {p:dict(latest_close=(latest or {}).get(p, 0), last_attempt=None,
                            last_finished=None, next_gap=0., last_error='', errors={'forward':'','gap':''}, forward_jobs=0, gap_jobs=0) for p in self.pairs}
        self.active = {}

    @staticmethod
    def target(now):
        return math.floor((now - SETTLE_SEC) / 60) * 60

    def plan(self, now, monotonic_now):
        target = self.target(now)
        busy = {p for p, _ in self.active}
        count = sum(kind == 'forward' for _, kind in self.active)
        candidates = []
        for pair in self.pairs:
            state = self.state[pair]
            if pair in busy or state['latest_close'] >= target:
                continue
            if state['last_attempt'] is not None and monotonic_now - state['last_attempt'] < FORWARD_RETRY_SEC:
                continue
            candidates.append((state['last_attempt'] if state['last_attempt'] is not None else -float('inf'), pair))
        jobs = [(pair, 'forward') for _, pair in sorted(candidates)[:max(0, MAX_FORWARD-count)]]
        for pair, kind in jobs:
            self.active[(pair, kind)] = target
            self.state[pair]['last_attempt'] = monotonic_now
            busy.add(pair)
        # One gap slot cannot consume the three reserved forward slots. A pair
        # is never read/rewritten by forward and gap jobs concurrently.
        if not any(kind == 'gap' for _, kind in self.active):
            choices = [(state['next_gap'], pair) for pair, state in self.state.items()
                       if pair not in busy and state['latest_close'] >= target
                       and state['forward_jobs'] and monotonic_now >= state['next_gap']]
            if choices:
                _, pair = min(choices)
                self.active[(pair, 'gap')] = target
                jobs.append((pair, 'gap'))
        return jobs

    def finish(self, pair, kind, result, now, monotonic_now):
        self.active.pop((pair, kind))
        state = self.state[pair]
        state['last_error'] = str(result.get('error') or '')[:200]
        state['errors'][kind] = state['last_error']
        if kind == 'forward':
            state['forward_jobs'] += 1
            state['last_finished'] = monotonic_now
            # Failure or a reread can never fabricate advancement.
            if not result.get('error') and result.get('after_last'):
                close = epoch(result['after_last']) + 60
                if close > now or close < state['latest_close']:
                    raise ValueError('forward_source_clock_regressed_or_future')
                state['latest_close'] = close
        else:
            state['gap_jobs'] += 1
            state['next_gap'] = monotonic_now + GAP_SCAN_INTERVAL_SEC


def run_job(existing, client, limiter, args, pair, kind, *, clock=utc):
    started = clock()
    path = args.candle_root / f'{pair}_M1.csv'
    local_last = existing.last_timestamp(path) if kind == 'forward' and path.exists() else None
    scope = (ForwardProbeOnly(client, pair, limiter, local_last, clock) if kind == 'forward'
             else CandleGetOnly(client, pair, limiter, clock))
    start_receipt=dict(schema_version=SCHEMA,instrument=pair,job_kind=kind,source_bindings=SOURCE_BINDINGS,
                       job_started_utc=started.isoformat(),**FLAGS)
    existing.write_json_atomic(args.state_root/f'{pair}.{kind}.started.json',start_receipt)
    try:
        if kind == 'forward':
            result = existing.update_pair(scope, path, max_requests=1, backfill_requests=0,
                batch_size=5000, pause_seconds=0., dry_run=False, recover_gaps=False)
        else:
            result = existing.recover_recent_gaps(scope, path, dry_run=False, enabled=True)
    except Exception as exc:
        result = dict(instrument=pair, error=type(exc).__name__ + ':' + str(exc)[:180], rows_appended=0, rows_recovered=0)
    if kind == 'forward':
        result = {**result, 'tail_probe_decision': scope.decision, 'actual_get_count': len(scope.receipts)}
    completed = clock()
    receipt = dict(schema_version=SCHEMA, scheduling_contract=SCHEDULE, instrument=pair, job_kind=kind,
        job_started_utc=started.isoformat(), job_completed_utc=completed.isoformat(),
        source_retrievals=scope.receipts, source_bindings=SOURCE_BINDINGS,
        writer_result=result, original_availability_claim=False,
        storage_scope='latest_per_pair_job_receipt; original_gap_receipts_remain_immutable', **FLAGS)
    # This named latest receipt is observation metadata, not an immutable
    # historical source feed. Existing gap request/response/publication records
    # retain their original separate files and clocks without modification.
    existing.write_json_atomic(args.state_root / f'{pair}.{kind}.json', receipt)
    return result


def restore_state(scheduler, state_root, now, monotonic_now, *, schema=SCHEMA, bindings=SOURCE_BINDINGS):
    """A worker restart does not reset the retry floor or gap scan cadence."""
    for pair in scheduler.pairs:
        for kind in ('forward', 'gap'):
            pending=state_root/f'{pair}.{kind}.started.json'
            pending_epoch=None
            if pending.exists():
                if pending.stat().st_size>8192:
                    raise ValueError('M1_start_receipt_bound')
                attempt=json.loads(pending.read_bytes())
                if attempt.get('schema_version')!=schema or attempt.get('instrument')!=pair or attempt.get('job_kind')!=kind or attempt.get('source_bindings')!=bindings:
                    raise ValueError('M1_start_receipt_identity')
                pending_epoch=epoch(attempt['job_started_utc'])
                if pending_epoch>now:
                    raise ValueError('M1_start_receipt_future')
                if kind=='forward':
                    scheduler.state[pair]['last_attempt']=max(scheduler.state[pair]['last_attempt'] if scheduler.state[pair]['last_attempt'] is not None else -math.inf, monotonic_now-(now-pending_epoch))
                else:
                    scheduler.state[pair]['next_gap']=max(scheduler.state[pair]['next_gap'], monotonic_now+max(0.,GAP_SCAN_INTERVAL_SEC-(now-pending_epoch)))
            path = state_root / f'{pair}.{kind}.json'
            if not path.exists():
                continue
            if path.stat().st_size > 128*1024:
                raise ValueError('m1_job_receipt_size_bound')
            value = json.loads(path.read_bytes())
            if value.get('schema_version') != schema or value.get('source_bindings') != bindings or value.get('instrument') != pair or value.get('job_kind') != kind:
                raise ValueError('m1_job_receipt_identity')
            completed = epoch(value['job_completed_utc'])
            if completed > now:
                raise ValueError('m1_job_receipt_future')
            age = now - max(completed,pending_epoch or completed)
            if kind == 'forward':
                scheduler.state[pair]['last_attempt'] = max(scheduler.state[pair]['last_attempt'] if scheduler.state[pair]['last_attempt'] is not None else -math.inf, monotonic_now-age)
            else:
                scheduler.state[pair]['next_gap'] = max(scheduler.state[pair]['next_gap'], monotonic_now+max(0., GAP_SCAN_INTERVAL_SEC-age))


def old_writer_running(root=ROOT):
    import psutil
    blocked = {'oanda_all68_m1_forward_updater.py', 'oanda_all68_m1_forward_updater_v2.py',
               'oanda_all68_m1_cadence_v2.py'}
    for process in psutil.process_iter(['name', 'cmdline']):
        try:
            info = process.info
            if not (info.get('name') or '').lower().startswith('python'):
                continue
            for argument in (info.get('cmdline') or [])[1:]:
                path = Path(argument)
                if path.name not in blocked:
                    continue
                if not path.is_absolute() or path.resolve() == (Path(root) / path.name).resolve():
                    return True  # Relative competing scripts are not silently assumed unrelated.
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            raise ValueError('m1_owner_inventory_unavailable') from None
    return False


@contextmanager
def owner_lock(path):
    import msvcrt
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a+b') as handle:
        if not path.stat().st_size:
            handle.write(b'0');handle.flush()
        handle.seek(0);msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
        try:
            yield
        finally:
            handle.seek(0);msvcrt.locking(handle.fileno(),msvcrt.LK_UNLCK,1)


def configured_pairs(existing):
    """The hash-bound registry, never a transient quote snapshot, owns coverage."""
    pairs = sorted(existing.verified_pip_sizes())
    if len(pairs) != 68 or any(not re.fullmatch('[A-Z]{3}_[A-Z]{3}', pair) for pair in pairs):
        raise ValueError('M1_registered_all68_pair_identity')
    return pairs


def stop_requested(state_root):
    path = state_root / 'stop_requested.json'
    if path.is_symlink() or path.is_junction():
        raise ValueError('M1_stop_flag_reparse_refused')
    if path.exists() and not path.is_file():
        raise ValueError('M1_stop_flag_must_be_regular_file')
    return path.is_file()


def run(args):
    source_check()
    import oanda_all68_m1_forward_updater_v2 as existing
    import oanda_worker_heartbeat as heart
    if Path(existing.__file__).resolve()!=ROOT/'oanda_all68_m1_forward_updater_v2.py' or Path(heart.__file__).resolve()!=ROOT/'oanda_worker_heartbeat.py':
        raise ValueError('mixed_M1_writer_import')
    if old_writer_running():
        raise ValueError('legacy_M1_writer_must_be_stopped_before_handover')
    for path in (args.candle_root,args.state_root,args.report.parent,args.heartbeat.parent,args.quote_snapshot.parent,args.predecessor_state_root):
        checked_directory(path)
    args.state_root.mkdir(parents=True,exist_ok=True)
    if args.state_root == args.predecessor_state_root:
        raise ValueError('successor_state_must_preserve_predecessor_receipts')
    # Sharing the predecessor's canonical lock excludes even a concurrent V2 start.
    with owner_lock(args.predecessor_state_root/'worker.lock'), owner_lock(args.state_root/'worker.lock'), heart.WorkerHeartbeat(args.heartbeat,
            worker='all68_m1_forward_archive',role='read_only_market_data_archive',interval_sec=5.) as heartbeat:
        client,meta=existing.resolve_readonly_oanda_client()
        if Path(existing.manager_module().__file__).resolve()!=ROOT/'oanda_gpt_training_strategy_manager.py':
            raise ValueError('mixed_M1_client_import')
        source_check()
        # Separate immutable GET-client instances give the gap lane a shorter
        # socket timeout without mutating a client concurrently used elsewhere.
        client_class=existing.manager_module().OandaClient
        clients={'forward':client_class(client.token,client.base_url,client.account_id,timeout=FORWARD_HTTP_TIMEOUT_SEC),
                 'gap':client_class(client.token,client.base_url,client.account_id,timeout=GAP_HTTP_TIMEOUT_SEC)}
        pairs=configured_pairs(existing)
        latest={}
        for pair in pairs:
            path=args.candle_root/f'{pair}_M1.csv'
            last=existing.last_timestamp(path) if path.exists() else None
            latest[pair]=last.timestamp()+60 if last is not None else 0
            if latest[pair]>time.time():
                raise ValueError('existing_M1_completed_clock_from_future')
        scheduler=Scheduler(pairs,latest);limiter=GetRateLimit()
        wall, mono = time.time(), time.monotonic()
        restore_state(scheduler,args.predecessor_state_root,wall,mono, schema=PREDECESSOR_SCHEMA, bindings=PREDECESSOR_BINDINGS)
        restore_state(scheduler,args.state_root,wall,mono)
        started=time.monotonic();last_report=-float('inf');last_sources=started
        forward_results={};gap_results={};active={};stopping=False
        with ThreadPoolExecutor(max_workers=MAX_FORWARD+MAX_GAP,thread_name_prefix='M1-cadence') as pool:
            while True:
                wall=time.time();mono=time.monotonic()
                if mono-last_sources>=30:
                    source_check();last_sources=mono
                for future in [f for f in active if f.done()]:
                    pair,kind=active.pop(future)
                    try:
                        result=future.result()
                    except Exception as exc:
                        result={'error':type(exc).__name__+':'+str(exc)[:180]}
                    scheduler.finish(pair,kind,result,time.time(),time.monotonic())
                    (forward_results if kind=='forward' else gap_results)[pair]=result
                    heartbeat.mark_progress(phase='refreshing_forward',instrument=pair,last_job=kind,
                        last_error=scheduler.state[pair]['last_error'])
                requested_stop=stop_requested(args.state_root)
                stopping=requested_stop or mono-started>=args.duration_sec
                if not stopping:
                    for pair,kind in scheduler.plan(wall,mono):
                        active[pool.submit(run_job,existing,clients[kind],limiter,args,pair,kind)]=(pair,kind)
                if mono-last_report>=5 or stopping and not active:
                    report=dict(schema_version=SCHEMA,scheduling_contract=SCHEDULE,generated_utc=utc().isoformat(),
                        started_utc=datetime.fromtimestamp(wall-(mono-started),timezone.utc).isoformat(),
                        source={'name':'OANDA REST-v20 Instrument Candles','granularity':'M1','price':'BAM',**meta},
                        source_bindings=SOURCE_BINDINGS,archive_paths={'candle_root':str(args.candle_root),'quote_snapshot':str(args.quote_snapshot)},
                        pair_universe_source='hash_bound_registered_all68_pip_metadata',
                        stop_requested=requested_stop,stop_reason=('local_stop_requested' if requested_stop else 'duration_elapsed' if stopping else None),
                        pair_count=len(pairs),max_forward_inflight=MAX_FORWARD,max_gap_inflight=MAX_GAP,
                        max_actual_GET_starts_per_second=MAX_GET_STARTS_PER_SECOND,forward_retry_sec=FORWARD_RETRY_SEC,
                        stale_tail_probe_count=TAIL_PROBE_COUNT,max_actual_GETs_per_forward_job=2,
                        pip_metadata_sha256=existing.PIP_METADATA_SHA256,
                        forward_socket_timeout_sec=FORWARD_HTTP_TIMEOUT_SEC,gap_socket_timeout_sec=GAP_HTTP_TIMEOUT_SEC,
                        gap_scan_interval_sec=GAP_SCAN_INTERVAL_SEC,target_complete_epoch=scheduler.target(wall),
                        current_boundary_pairs=sum(s['latest_close']>=scheduler.target(wall) for s in scheduler.state.values()),
                        active_jobs=[{'instrument':p,'kind':k} for p,k in scheduler.active],
                        error_count=sum(bool(error) for s in scheduler.state.values() for error in s['errors'].values()),
                        latest_forward_results=forward_results,latest_gap_results=gap_results,
                        pairs={p:{'latest_completed_bar_epoch':s['latest_close'],'completed_bar_age_sec':wall-s['latest_close'] if s['latest_close'] else None,
                                  'forward_jobs':s['forward_jobs'],'gap_jobs':s['gap_jobs'],'last_error':s['last_error'],'errors_by_kind':s['errors']} for p,s in scheduler.state.items()},
                        latest_result_scope='per_pair_latest; not_a_shared_simultaneous_cycle',**FLAGS)
                    existing.write_json_atomic(args.report,report)
                    heartbeat.update(phase='refreshing_forward' if any(k=='forward' for _,k in scheduler.active) else 'background_gap_or_waiting',
                        scheduling_contract=SCHEDULE,current_boundary_pairs=report['current_boundary_pairs'],pair_count=len(pairs),
                        error_count=report['error_count'],active_jobs=report['active_jobs'],report_schema=SCHEMA)
                    last_report=mono
                if stopping and not active:
                    return 0
                if active:
                    wait(active,timeout=POLL_SEC,return_when=FIRST_COMPLETED)
                else:
                    time.sleep(POLL_SEC)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candle-root',type=Path,default=DATA/'candles')
    parser.add_argument('--quote-snapshot',type=Path,default=DATA/'state/practice_007_market_quotes_v1.json')
    parser.add_argument('--state-root',type=Path,default=DATA/'state/m1_cadence_v3')
    parser.add_argument('--predecessor-state-root',type=Path,default=DATA/'state/m1_cadence_v2')
    parser.add_argument('--report',type=Path,default=DATA/'state/all68_m1_cadence_v3.json')
    parser.add_argument('--heartbeat',type=Path,default=DATA/'state/all68_m1_cadence_heartbeat_v3.json')
    parser.add_argument('--duration-sec',type=float,default=604800)
    args=parser.parse_args(argv)
    if not math.isfinite(args.duration_sec) or not 1<=args.duration_sec<=604800:
        raise ValueError('bounded_M1_worker_duration')
    return run(args)


if __name__=='__main__':
    raise SystemExit(main())
