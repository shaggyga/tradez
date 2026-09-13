"""Bounded research-only forward collection for fixed M1 risk distributions.

Only the three registered instrument-candle GETs are available. No account,
position, order, model fit, promotion, supervisor or dashboard action is exposed.
Immutable issued records have an actual post-write publication and later read.
"""
from __future__ import annotations

from contextlib import contextmanager
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import time

import oanda_forecast_curve_file_store_v1 as files
import oanda_m1_mba_research_capture_v1 as capture
import oanda_m1_risk_distribution_contract_v1 as contract

ROOT = Path(__file__).absolute().parent
SCHEMA = 'm1_risk_distribution_registry_v1_20260909'
REGISTRY_ID = 'm1_risk_distributions_v1_20260909'
COLLECTION_END = 1788957900  # 2026-09-09 12:45 UTC
LAST_REFERENCE = 1788954060  # 2026-09-09 11:41 UTC
SOURCE_NAMES = frozenset({
    'oanda_m1_mba_research_capture_v1.py', 'oanda_m1_path_risk_labels_v1.py',
    'oanda_m1_risk_distribution_contract_v1.py', 'oanda_m1_risk_distribution_outcomes_v1.py',
    'oanda_m1_risk_distribution_worker_v1.py', 'oanda_live_account_readonly_status.py',
    'oanda_forecast_curve_file_store_v1.py', 'oanda_forecast_curve_contract_v1.py',
})


def need(condition, reason):
    if not condition:
        raise ValueError(reason)


def source_check(registry):
    need(set(registry['source_bindings']) == SOURCE_NAMES, 'risk_worker_source_inventory')
    for name, expected in registry['source_bindings'].items():
        need(contract.sha(files._read(ROOT / name)) == expected,
             'risk_worker_source_changed')


def load_registry(path, expected_sha, *, clock=time.time):
    raw = files._read(Path(path))
    value = contract.decode(raw)
    need(contract.sha(raw) == expected_sha and raw == contract.canonical(value),
         'risk_worker_registry_bytes_identity')
    required = {'schema_version', 'registry_id', 'cohort_id', 'created_epoch',
                'first_reference_epoch', 'last_reference_epoch', 'collection_end_epoch',
                'capture_cadence_sec', 'capture_start_delay_sec', 'source_bindings',
                'metadata', 'fit_artifact_path', 'fit_artifact_sha256', 'output_root',
                'registered_methods', 'registered_labels', 'registered_horizons_minutes',
                'forecast_policy', *contract.AUTHORITY}
    need(set(value) == required and value['schema_version'] == SCHEMA and
         value['registry_id'] == REGISTRY_ID and value['cohort_id'] == contract.COHORT_ID and
         all(value[k] is expected for k, expected in contract.AUTHORITY.items()),
         'risk_worker_registry_schema_or_authority')
    now = contract.epoch(clock())
    first = contract.epoch(value['first_reference_epoch'])
    last = contract.epoch(value['last_reference_epoch'])
    created = contract.epoch(value['created_epoch'])
    need(first % 300 == 60 and first <= last == LAST_REFERENCE and
         created <= first and created <= now and value['collection_end_epoch'] == COLLECTION_END
         and last + 3600 < COLLECTION_END and value['capture_cadence_sec'] == 60
         and type(value['capture_cadence_sec']) is int and
         value['capture_start_delay_sec'] == 2 and type(value['capture_start_delay_sec']) is int,
         'risk_worker_fixed_clock_policy')
    need(value['registered_methods'] == list(contract.METHODS) and
         value['registered_labels'] == list(contract.labels.CONTINUOUS_LABELS) and
         value['registered_horizons_minutes'] == list(contract.labels.HORIZONS),
         'risk_worker_distribution_inventory')
    need(value['forecast_policy'] == {
        'all_methods_and_labels_required': True, 'maximum_entry_age_sec': 20,
        'minimum_same_session_rows': 204, 'original_targets_retimed': False,
        'refit_or_method_selection': False, 'risk_routing_to_management': False,
        'missing_prices_filled': False, 'late_issue_backfill': False},
        'risk_worker_forecast_policy')
    need(type(value['metadata']) is dict and set(value['metadata']) == set(contract.PAIRS),
         'risk_worker_metadata_inventory')
    for pair in contract.PAIRS:
        capture._metadata(value['metadata'][pair], pair)
    output = Path(value['output_root'])
    need(output.is_absolute() and output == ROOT / 'data/oanda_training_manager' / REGISTRY_ID,
         'risk_worker_fixed_output_root')
    fit = Path(value['fit_artifact_path'])
    need(fit.is_absolute() and value['fit_artifact_sha256'] == contract.TRAINING_ARTIFACT_SHA256,
         'risk_worker_fixed_training_artifact')
    need(contract.sha(files._read(fit)) == value['fit_artifact_sha256'], 'risk_worker_fit_changed')
    source_check(value)
    return value, expected_sha


def persist(directory, name, raw, *, clock=time.time):
    need(type(name) is str and contract.re.fullmatch('[a-z0-9_]{1,64}\\.json', name),
         'risk_worker_internal_file_name')
    need(type(raw) is bytes and 0 < len(raw) <= files.contract.MAX_BYTES,
         'risk_worker_record_byte_bound')
    started = contract.epoch(clock())
    path = directory / name
    need(files._write_exclusive(path, raw), 'risk_worker_existing_record')
    need(files._read(path) == raw, 'risk_worker_record_readback')
    completed = contract.epoch(clock())
    need(started <= completed, 'risk_worker_write_clock_regression')
    return {'file': name, 'bytes_sha256': contract.sha(raw), 'byte_count': len(raw),
            'write_started_epoch': started, 'write_readback_completed_epoch': completed}


def record(directory, name, value, *, clock=time.time):
    return persist(directory, name, contract.canonical(value), clock=clock)


def read_record(path, *, clock=time.time):
    started = contract.epoch(clock())
    raw = files._read(path)
    completed = contract.epoch(clock())
    need(started <= completed, 'risk_worker_read_clock_regression')
    return raw, {'file': path.name, 'bytes_sha256': contract.sha(raw), 'byte_count': len(raw),
                 'read_started_epoch': started, 'read_completed_epoch': completed}


def reason(error):
    message = str(error)
    return message if (isinstance(error, ValueError) and len(message) <= 160
                       and contract.re.fullmatch('[a-zA-Z0-9_:.<>/=-]+', message)) else type(error).__name__


def collect_pair(registry, cycle_dir, pair, cycle_epoch, fit_raw, *, clock=time.time):
    directory = files._directory(cycle_dir, (pair.lower(),), create=True)
    started = contract.epoch(clock())
    scheduled_issue = (registry['first_reference_epoch'] <= cycle_epoch <= registry['last_reference_epoch']
                       and cycle_epoch % 300 == 60)
    result = dict(instrument=pair, cycle_epoch=cycle_epoch, started_epoch=started,
        scheduled_issue=scheduled_issue, capture_attempted=False,
        status='not_attempted', phase='pre_capture', artifacts={}, **contract.AUTHORITY)
    try:
        source_check(registry)
        request_admission = contract.epoch(clock())
        need(request_admission >= started and
             request_admission + capture.MAX_SECONDS <= registry['collection_end_epoch'],
             'risk_worker_capture_stop_boundary')
        first_capture_clock = True
        def request_clock():
            nonlocal first_capture_clock
            actual = contract.epoch(clock())
            if first_capture_clock:
                first_capture_clock = False
                need(actual >= request_admission and
                     actual + capture.MAX_SECONDS <= registry['collection_end_epoch'],
                     'm1_worker_request_start_outside_window')
            return actual
        result['capture_invoked'] = True
        raw, receipt = capture.capture_once(pair, credential_path=ROOT / 'creds',
                                            metadata=registry['metadata'][pair], clock=request_clock)
        result['capture_attempted'] = receipt.get('request_started_epoch') is not None
        result['phase'] = 'captured'
        if raw:
            result['artifacts']['raw'] = persist(directory, 'source_raw.json', raw, clock=clock)
        else:
            result['empty_raw_response'] = {'bytes': 0, 'sha256': contract.sha(b'')}
        result['artifacts']['capture_receipt'] = record(directory, 'capture_receipt.json', receipt, clock=clock)
        result['capture_status'] = receipt['status']
        need(receipt['status'] == 'captured_not_issued',
             'risk_worker_capture_unavailable:' + str(receipt.get('error_reason', 'unknown')))
        after_capture = contract.epoch(clock())
        need(started <= contract.epoch(receipt['request_started_epoch']) <=
             contract.epoch(receipt['read_completed_epoch']) <=
             contract.epoch(receipt['capture_completed_epoch']) <= after_capture,
             'risk_worker_actual_capture_clock_order')
        begun = contract.epoch(clock())
        input_raw, raw_read = read_record(directory / 'source_raw.json', clock=clock)
        receipt_raw, receipt_read = read_record(directory / 'capture_receipt.json', clock=clock)
        consumed = contract.epoch(clock())
        input_receipt = contract.decode(receipt_raw)
        need(input_raw == raw and input_receipt == receipt, 'risk_worker_capture_readback_identity')
        input_consumption = dict(raw_sha256=contract.sha(input_raw),
            receipt_sha256=contract.digest(input_receipt), read_started_epoch=begun,
            read_completed_epoch=consumed)
        input_record = dict(input_consumption=input_consumption, raw_read=raw_read,
                            receipt_read=receipt_read)
        result['artifacts']['input_consumption'] = record(directory, 'input_consumption.json',
                                                         input_record, clock=clock)
        mapping = capture.map_verified_capture(input_raw, input_receipt, registry['metadata'][pair])
        result['artifacts']['mapping'] = record(directory, 'source_mapping.json',
            capture.mapping_document(mapping), clock=clock)
        result['last_complete_price_epoch'] = (mapping['complete_rows'][-1]['price_epoch']
                                                 if mapping['complete_rows'] else None)
        if not scheduled_issue:
            result.update(status='captured_only', phase='capture_complete')
        else:
            need(contract.epoch(clock()) <= cycle_epoch + contract.MAX_ENTRY_AGE_SEC,
                 'risk_worker_original_issue_window_expired')
            source_check(registry)
            issued = contract.issue_distribution(input_raw, input_receipt,
                registry['metadata'][pair], fit_raw, input_consumption=input_consumption,
                cohort_id=registry['cohort_id'], scheduled_reference_price_epoch=cycle_epoch,
                expected_source_bindings=registry['source_bindings'], clock=clock)
            contract.validate_issue(issued, input_raw=input_raw, input_receipt=input_receipt,
                metadata=registry['metadata'][pair], fit_raw=fit_raw,
                expected_source_bindings=registry['source_bindings'])
            source_check(registry)
            result['phase'] = 'issue_computed'
            publication_started = contract.epoch(clock())
            issue_persistence = record(directory, 'issued.json', issued, clock=clock)
            result['artifacts']['issued'] = issue_persistence
            result.update(issued_sha256=issued['issued_sha256'], nodes=len(issued['nodes']))
            pub = contract.publication_receipt(issued,
                persisted_bytes_sha256=issue_persistence['bytes_sha256'],
                publication_started_epoch=publication_started,
                expected_source_bindings=registry['source_bindings'], clock=clock)
            result['artifacts']['publication'] = record(directory, 'publication.json', pub, clock=clock)
            result['phase'] = 'published'
            consume_started = contract.epoch(clock())
            issue_raw, issue_read = read_record(directory / 'issued.json', clock=clock)
            pub_raw, pub_read = read_record(directory / 'publication.json', clock=clock)
            actual_issue, actual_pub = contract.decode(issue_raw), contract.decode(pub_raw)
            need(actual_issue == issued and actual_pub == pub, 'risk_worker_publication_readback_identity')
            consumer = contract.consumption_receipt(actual_issue, actual_pub,
                read_started_epoch=consume_started, expected_source_bindings=registry['source_bindings'], clock=clock)
            contract.validate_chain(actual_issue, actual_pub, consumer,
                input_raw=input_raw, input_receipt=input_receipt, metadata=registry['metadata'][pair],
                fit_raw=fit_raw, expected_source_bindings=registry['source_bindings'])
            source_check(registry)
            result['artifacts']['consumption'] = record(directory, 'consumption.json', consumer, clock=clock)
            result['artifacts']['consumer_read_evidence'] = record(directory,
                'consumer_read_evidence.json', {'issued': issue_read, 'publication': pub_read}, clock=clock)
            result.update(status='issued_published_consumed', phase='complete',
                issued_epoch=issued['issued_epoch'], publication_completed_epoch=pub['publication_completed_epoch'],
                consumption_completed_epoch=consumer['read_completed_epoch'])
    except Exception as error:
        result.update(status='withheld_or_partial', reason_code=reason(error), error_type=type(error).__name__)
    result['completed_epoch'] = contract.epoch(clock())
    need(result['completed_epoch'] >= started, 'risk_worker_pair_clock_regression')
    record(directory, 'pair_completed.json', result, clock=clock)
    return result


@contextmanager
def worker_lock(root):
    path = root / 'worker.lock'
    files._safe_components(root)
    if path.exists():
        files._safe_components(path, require_file=True)
    stream = path.open('a+b')
    try:
        if stream.tell() == 0:
            stream.write(b'0'); stream.flush()
        stream.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        stream.close()


def run(registry, registry_sha, *, clock=time.time, sleep=time.sleep, once=False):
    root = files._directory(Path(registry['output_root']), create=True)
    with worker_lock(root):
        started = contract.epoch(clock())
        source_check(registry)
        need(started < registry['collection_end_epoch'], 'risk_worker_collection_already_ended')
        fit_raw, fit_read = read_record(Path(registry['fit_artifact_path']), clock=clock)
        need(contract.sha(fit_raw) == registry['fit_artifact_sha256'], 'risk_worker_fit_identity')
        sessions = files._directory(root, ('sessions',), create=True)
        session = files._directory(sessions, (str(time.time_ns()),), create=True)
        fit_persistence = persist(session, 'training_artifact.json', fit_raw, clock=clock)
        record(session, 'started.json', dict(schema_version='m1_risk_worker_session_v1_20260909',
            registry_sha256=registry_sha, started_epoch=started, pid=os.getpid(), once=once,
            training_artifact_read=fit_read, training_artifact_persistence=fit_persistence,
            **contract.AUTHORITY), clock=clock)
        last_cycle = None
        previous_epoch = started
        count = 0
        stop_reason = 'collection_deadline'
        while contract.epoch(clock()) < registry['collection_end_epoch']:
            now = contract.epoch(clock())
            need(now >= previous_epoch, 'risk_worker_loop_clock_regression')
            previous_epoch = now
            cycle_epoch = int(now // 60) * 60
            if now < cycle_epoch + registry['capture_start_delay_sec'] or cycle_epoch == last_cycle:
                sleep(min(.25, max(0, registry['collection_end_epoch'] - now)))
                continue
            skipped_cycles = ([] if last_cycle is None else
                              list(range(last_cycle + 60, cycle_epoch, 60)))
            last_cycle = cycle_epoch
            cycle_path = root / 'cycles' / str(cycle_epoch)
            if cycle_path.exists():
                stop_reason = 'prior_cycle_directory_requires_review'
                break
            cycle_dir = files._directory(root, ('cycles', str(cycle_epoch)), create=True)
            begun = contract.epoch(clock())
            need(begun >= now, 'risk_worker_cycle_start_clock_regression')
            results = [collect_pair(registry, cycle_dir, pair, cycle_epoch, fit_raw, clock=clock)
                       for pair in contract.PAIRS]
            completed = contract.epoch(clock())
            need(completed >= begun, 'risk_worker_cycle_completion_clock_regression')
            previous_epoch = completed
            body = dict(schema_version='m1_risk_cycle_completed_v1_20260909',
                registry_sha256=registry_sha, cycle_epoch=cycle_epoch, started_epoch=begun,
                completed_epoch=completed, start_delay_sec=begun - cycle_epoch, pairs=results,
                skipped_prior_capture_cycle_epochs=skipped_cycles,
                retained_issue_count=sum('issued' in r.get('artifacts', {}) for r in results),
                publication_count=sum('publication' in r.get('artifacts', {}) for r in results),
                consumption_count=sum('consumption' in r.get('artifacts', {}) for r in results),
                complete_chain_count=sum(r['status'] == 'issued_published_consumed' for r in results),
                broker_order_count=0, **contract.AUTHORITY)
            record(cycle_dir, 'cycle_completed.json', body, clock=clock)
            count += 1
            print(json.dumps({'cycle_epoch': cycle_epoch, 'completed_epoch': completed,
                'retained_issue_count': body['retained_issue_count'],
                'publication_count': body['publication_count'],
                'consumption_count': body['consumption_count'],
                'complete_chain_count': body['complete_chain_count'],
                'pair_statuses': {r['instrument']: r['status'] for r in results},
                'reason_codes': {r['instrument']: r.get('reason_code') for r in results
                                 if r.get('reason_code')}, 'broker_order_count': 0}), flush=True)
            if once:
                stop_reason = 'once_complete'
                break
        source_check(registry)
        record(session, 'stopped.json', dict(schema_version='m1_risk_worker_stop_v1_20260909',
            registry_sha256=registry_sha, started_epoch=started, completed_epoch=contract.epoch(clock()),
            completed_cycles=count, reason=stop_reason, **contract.AUTHORITY), clock=clock)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry', required=True)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    registered, digest = load_registry(args.registry, args.expected_sha256)
    run(registered, digest, once=args.once)
