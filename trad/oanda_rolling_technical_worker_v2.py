"""Operational successor for the existing immutable rolling M1 observation contract.

The V1 calculator, input validation, database identity and original observations
remain unchanged. This worker adds bounded incremental computation, explicit
continuity diagnostics and exact-minute retained peer alignment. It has no
broker, order, account, news or model-fitting imports.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import time

import numpy as np

import oanda_rolling_technical_worker_v1 as original
import oanda_rolling_technical_alignment_v2 as alignment
import oanda_rolling_technical_continuity_v2 as continuity
from oanda_rolling_technical_store_v1 import TechnicalStore, encoded

ROOT = Path(__file__).resolve().parent
SCHEMA = 'rolling_technical_operations_v2_20260916'
FLAGS = dict(original.FLAGS)
NEW_SOURCES = (Path(__file__).name, 'oanda_rolling_technical_alignment_v2.py',
               'oanda_rolling_technical_continuity_v2.py')
SOURCES = tuple(sorted(set(original.SOURCES + NEW_SOURCES)))


def read_config(path):
    value = json.loads(Path(path).read_bytes())
    if value.get('schema') != SCHEMA or any(value.get(k) != v for k, v in FLAGS.items()):
        raise ValueError('explicit_operations_research_only_config_required')
    old_path = Path(value['observation_config']).resolve()
    if not old_path.is_relative_to(ROOT / 'config') or original.sha(old_path) != value['observation_config_sha256']:
        raise ValueError('original_observation_config_binding_changed')
    base = original.read_config(old_path)
    if set(value['source_bindings']) != set(SOURCES):
        raise ValueError('complete_operations_source_bindings_required')
    for name, expected in value['source_bindings'].items():
        if original.sha(ROOT / name) != expected:
            raise ValueError('operations_source_changed:' + name)
    if type(value.get('maximum_tail_rows')) is not int or not base['tail_rows'] <= value['maximum_tail_rows'] <= 8192:
        raise ValueError('bounded_maximum_tail_rows_required')
    if type(value.get('maximum_candidate_minutes')) is not int or not 1 <= value['maximum_candidate_minutes'] <= 8:
        raise ValueError('bounded_alignment_candidates_required')
    if value.get('interval_seconds') != base['interval_seconds']:
        raise ValueError('original_collection_cadence_required')
    return value, base


def incremental_features(data, pair, pip, latest):
    """Compute exactly the new suffix with full kernel context.

    Full source arrays still go through V1 ingest, including its complete
    overlapping-input revision checks. Prefix NaNs are never emitted because
    keep_from is strictly after the last published original minute.
    """
    clocks = data['time']
    if not len(clocks):
        raise ValueError('no_completed_source_rows')
    first_new = 0 if latest is None else int(np.searchsorted(clocks, latest, side='right'))
    start = max(0, first_new - (original.kernel.max_lookback_bars() - 1))
    if first_new == len(clocks):
        return {row['name']: np.full(len(clocks), np.nan) for row in original.kernel.feature_registry()}, {
            'source_rows': len(clocks), 'computed_rows': 0, 'new_rows': 0, 'context_start_index': start}
    suffix = {name: values[start:] for name, values in data.items()}
    calculated = original.kernel.compute_features(suffix, pair, pip)
    result = {}
    for name, values in calculated.items():
        result[name] = np.r_[np.full(start, np.nan), values] if start else values
    return result, {'source_rows': len(clocks), 'computed_rows': len(clocks) - start,
                    'new_rows': len(clocks) - first_new, 'context_start_index': start}


def read_with_context(source, pair, latest, base, operations):
    requested = base['tail_rows']
    attempts = []
    while True:
        data, receipt = original.read_pair(source, pair, time.time(), requested)
        attempts.append({'requested_rows': requested, 'returned_rows': len(data['time'])})
        if not len(data['time']):
            raise ValueError('no_completed_source_rows')
        if latest is not None and latest > int(data['time'][-1]):
            raise ValueError('source_latest_bar_regressed')
        if latest is not None and latest < int(data['time'][0]) and not receipt.get('range_truncated', True):
            raise ValueError('persisted_boundary_missing_from_complete_source')
        first_new = 0 if latest is None else int(np.searchsorted(data['time'], latest, side='right'))
        needs_more = latest is not None and receipt.get('range_truncated', True) and (
            latest < int(data['time'][0]) or first_new < original.kernel.max_lookback_bars() - 1)
        if not needs_more:
            receipt['operations_read_attempts'] = attempts
            return data, receipt
        if requested >= operations['maximum_tail_rows']:
            raise ValueError('bounded_backlog_context_exhausted_requires_explicit_import')
        requested = min(operations['maximum_tail_rows'], requested * 2)


def describe(current, maximum_age, now):
    families = {row['name']: row['family'] for row in original.kernel.feature_registry()}
    age = now - current['bar_end_epoch']
    available = Counter(families[name] for name, value in current['values'].items() if value is not None)
    missing = Counter(families[name] for name, value in current['values'].items() if value is None)
    publication_valid = (math.isfinite(current['published_epoch']) and
                         current['bar_end_epoch'] <= current['published_epoch'] <= now)
    return {'status': 'current' if 0 <= age <= maximum_age and publication_valid else 'stale',
            'bar_start_utc': original.utc(current['bar_start_epoch']),
            'bar_end_utc': original.utc(current['bar_end_epoch']), 'bar_age_seconds': round(age, 3),
            'published_utc': original.utc(current['published_epoch']),
            'available_features': current['available_features'], 'feature_count': current['feature_count'],
            'family_available': dict(available), 'family_missing': dict(missing),
            'missing_features': [name for name, value in current['values'].items() if value is None],
            'missingness_basis': 'original feature-specific elapsed support; no imputation',
            'feature_hash': current['feature_hash'],
            'publication_clock_valid': publication_valid}


def record_runtime(store, operations, config_path):
    record = {'schema': SCHEMA, 'started_utc': original.utc(), 'pid': os.getpid(),
              'config_path': str(Path(config_path).resolve()), 'config_sha256': original.sha(config_path),
              'source_bindings': operations['source_bindings'],
              'observation_contract_unchanged': True, **FLAGS}
    raw = encoded(record)
    identifier = hashlib.sha256(raw).hexdigest()
    with store.connection:
        store.connection.execute('INSERT INTO metadata(key,value) VALUES(?,?)', ('operations_run:' + identifier, raw))
    return identifier, record


def attach_aligned_state(publication, candidate, pair, reference, now, maximum_age):
    target = publication['target_bar_start_epoch']
    local = candidate['source_observations'].get(pair)
    if (local is None or not 0 <= now - target - 60 <= maximum_age or
            not target + 60 <= publication['published_epoch'] <= now or
            not target + 60 <= local['published_epoch'] <= now):
        return None
    peer = publication['panel']['by_pair'][pair]
    if local['bar_start_epoch'] != peer['bar_start_epoch'] or local['bar_start_epoch'] != target:
        raise RuntimeError('aligned_pair_clock_mismatch')
    return {'bar_start_epoch': target, 'bar_end_epoch': target + 60,
            'panel_id': publication['id'], 'panel_published_epoch': publication['published_epoch'],
            'observation': local, 'peer': peer, 'selection': reference,
            'availability_epoch': max(local['published_epoch'], publication['published_epoch']),
            'age_seconds_at_envelope': round(now - target - 60, 3)}


def run_cycle(operations, base, store, cache, *, runtime_id):
    started = time.monotonic()
    output = Path(base['output_root'])
    free = shutil.disk_usage(output).free
    if free < base['minimum_free_bytes'] or store.size_bytes() > base['maximum_dataset_bytes']:
        raise RuntimeError('rolling_dataset_storage_guard')
    results, observations, diagnostics, stage_times = {}, {}, {}, {}
    total_inserted = total_settled = total_source_rows = total_computed_rows = 0
    for pair, pip in sorted(base['pairs'].items()):
        pair_started = time.monotonic()
        source = Path(base['candle_root']) / (pair + '_M1.csv')
        try:
            st = source.stat()
            identity = (st.st_size, st.st_mtime_ns, st.st_ino)
            entry = cache.get(pair)
            if entry is None or entry['identity'] != identity:
                latest = store.latest_time(pair)
                data, receipt = read_with_context(source, pair, latest, base, operations)
                receipt['operations_runtime_id'] = runtime_id
                receipt['operations_schema'] = SCHEMA
                features, computation = incremental_features(data, pair, pip, latest)
                receipt['incremental_computation'] = computation
                lower = int(data['time'][max(0, len(data['time']) - base['bootstrap_rows'])]) if latest is None else latest + 1
                inserted = store.ingest(pair, data, features, receipt, keep_from=lower)
                total_inserted += inserted['inserted_observations']
                total_settled += store.settle(pair)
                total_source_rows += computation['source_rows']
                total_computed_rows += computation['computed_rows']
                entry = {'identity': identity, 'data': data, 'receipt': receipt,
                         'gap_cache': entry.get('gap_cache') if entry else None}
                if not receipt.get('skipped_rows', {}).get('bar_end_after_observation', 0):
                    cache[pair] = entry
                else:
                    cache.pop(pair, None)
            current = store.latest(pair)
            if current is None:
                raise ValueError('no_persisted_observation')
            results[pair] = describe(current, base['maximum_bar_age_seconds'], time.time())
            observations[pair] = current
            diagnostics[pair] = continuity.diagnose_pair(
                entry['data'], current['values'], pair, time.time(),
                source_receipt=entry['receipt'], max_rows=operations['maximum_tail_rows'],
                max_span_minutes=10080)
            interior = [item for item in diagnostics[pair]['continuity']['missing_intervals']
                        if item['location'] == 'interior_gap']
            gap_key = tuple((item['start_epoch'], item['end_epoch']) for item in interior)
            saved = entry.get('gap_cache')
            if saved is None or saved['key'] != gap_key or time.time() - saved['read_epoch'] >= 900:
                evidence_read = continuity.load_gap_evidence(
                    base['candle_root'], pair, interior, time.time(), max_minutes=8,
                    max_total_bytes=1024 * 1024)
                saved = {'key': gap_key, 'read_epoch': time.time(), 'result': evidence_read}
                entry['gap_cache'] = saved
            diagnostics[pair]['continuity'] = continuity.summarize_continuity(
                entry['data']['time'], pair, time.time(),
                range_truncated=bool(entry['receipt'].get('range_truncated', False)),
                gap_evidence=saved['result']['evidence'], max_rows=operations['maximum_tail_rows'],
                max_span_minutes=10080)
            diagnostics[pair]['gap_receipt_readback'] = {**saved['result'], 'read_epoch': saved['read_epoch'],
                'refresh_interval_seconds': 900, 'scope': 'At most 8 recent interior gap minutes; no network request'}
        except (OSError, ValueError, RuntimeError) as exc:
            results[pair] = {'status': 'unavailable', 'reason': type(exc).__name__ + ':' + str(exc)[:240]}
            observations[pair] = None
            diagnostics[pair] = {'status': 'unavailable', 'reason': results[pair]['reason']}
        stage_times[pair] = round(time.monotonic() - pair_started, 4)
    # Freshness is evaluated after the whole read cycle, never copied from a
    # fresh envelope or another pair's clock.
    now = time.time()
    for pair, row in observations.items():
        if row is not None:
            results[pair] = describe(row, base['maximum_bar_age_seconds'], now)
            if results[pair]['status'] != 'current':
                observations[pair] = None
    aligned = alignment.build_aligned_panels(
        store, observations, now_epoch=now,
        maximum_bar_age_seconds=base['maximum_bar_age_seconds'],
        max_candidate_minutes=operations['maximum_candidate_minutes'])
    publications = {}
    for candidate in aligned['candidates']:
        publication = store.publish_panel(candidate['panel'], candidate['source_observations'])
        publications[candidate['target_bar_start_epoch']] = publication
    published = time.time()
    publications = {target: value for target, value in publications.items()
                    if 0 <= published - target - 60 <= base['maximum_bar_age_seconds']
                    and target + 60 <= value['published_epoch'] <= published}
    pairs = {}
    aligned_status = Counter()
    for pair in base['pairs']:
        state = None
        ref = aligned['by_pair'].get(pair, {})
        target = ref.get('target_bar_start_epoch')
        if target in publications:
            publication = publications[target]
            candidate = next(item for item in aligned['candidates'] if item['target_bar_start_epoch'] == target)
            state = attach_aligned_state(publication, candidate, pair, ref, published, base['maximum_bar_age_seconds'])
        if state is None and target is not None:
            ref = {**ref, 'selected_status': ref['status'], 'status': 'unavailable',
                   'reason': 'selected_context_not_available_at_envelope'}
        row = observations[pair]
        if row is not None:
            results[pair] = describe(row, base['maximum_bar_age_seconds'], published)
            if results[pair]['status'] != 'current':
                row = None
        aligned_status[state['peer']['status'] if state else 'unavailable'] += 1
        pairs[pair] = {'availability': results[pair], 'observation': row,
                       'continuity': diagnostics[pair], 'aligned_state': state,
                       'peer_alignment': ref}
    counts = store.counts()
    newest = publications[max(publications)] if publications else None
    common = {'schema': original.SCHEMA, 'operations_schema': SCHEMA,
              'operations_runtime_id': runtime_id, 'generated_utc': original.utc(published),
              'generated_epoch': published, 'pid': os.getpid(), **FLAGS}
    report = {**common, 'status': 'collecting' if all(row['status'] == 'current' for row in results.values()) else 'partial',
              'universe_pairs': len(results), 'pairs_with_observations': sum('available_features' in row for row in results.values()),
              'current_pairs': sum(row['status'] == 'current' for row in results.values()),
              'fully_populated_pairs': sum(row.get('available_features') == row.get('feature_count') and 'feature_count' in row for row in results.values()),
              'feature_count': len(store.names), 'peer_feature_count': len(original.panel_kernel.panel_registry()),
              'inserted_this_cycle': total_inserted, 'outcomes_settled_this_cycle': total_settled,
              'counts': counts, 'outcome_states': {row[0]: row[1] for row in store.connection.execute('SELECT state,COUNT(*) FROM outcomes GROUP BY state')},
              'elapsed_seconds': round(time.monotonic() - started, 3), 'dataset_bytes': store.size_bytes(),
              'disk_free_bytes': free, 'storage_fraction': round(store.size_bytes() / base['maximum_dataset_bytes'], 5),
              'storage_policy': 'Append without deletion; stop at bound; no history silently discarded',
              'incremental_computation': {'source_rows_checked': total_source_rows, 'kernel_rows_computed': total_computed_rows},
              'pair_stage_seconds': stage_times, 'pairs': results,
              'peer_alignment': {'status_counts': dict(aligned_status),
                  'candidate_minutes': len(publications),
                  'structural_peer_limited_pairs': aligned['structural_peer_limited_pairs'],
                  'contract': 'Use aligned_state local and peer together; newer local observation remains separate'},
              'peer_panel': None if newest is None else {
                  'id': newest['id'], 'target_bar_start_utc': original.utc(newest['target_bar_start_epoch']),
                  'published_utc': original.utc(newest['published_epoch']),
                  'accepted_clock_pair_count': newest['panel']['accepted_clock_pair_count'],
                  'pair_status_counts': dict(Counter(row['status'] for row in newest['panel']['by_pair'].values()))},
              'observation_scope': 'technical_dataset_no_forecasts_or_trading_decisions'}
    envelope = {**common, 'envelope_time_is_not_feature_freshness': True,
                'peer_panel_publication': newest, 'aligned_panel_publications': list(publications.values()),
                'pairs': pairs}
    # Consumers can reject a torn two-file read using this common generation.
    generation = hashlib.sha256(encoded({'runtime': runtime_id, 'generated_epoch': published,
        'features': {pair: row['availability'].get('feature_hash') for pair, row in pairs.items()},
        'panels': {str(t): p['id'] for t, p in publications.items()}})).hexdigest()
    report['publication_generation'] = envelope['publication_generation'] = generation
    original.atomic_json(output / 'latest_features.json', envelope)
    original.atomic_json(output / 'status.json', report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--duration-sec', type=int, default=604800)
    args = parser.parse_args(argv)
    if not 1 <= args.duration_sec <= 604800:
        raise ValueError('bounded_worker_duration_required')
    operations, base = read_config(args.config)
    config_sha = original.sha(args.config)
    output = Path(base['output_root'])
    output.mkdir(parents=True, exist_ok=True)
    started, cache = time.monotonic(), {}
    with original.owner_lock(output):
        store = TechnicalStore(output / 'technical.sqlite', original.contract(base), max_bytes=base['maximum_dataset_bytes'])
        try:
            runtime_id, runtime = record_runtime(store, operations, args.config)
            original.atomic_json(output / 'owner.json', {**runtime, 'duration_seconds': args.duration_sec, 'operations_runtime_id': runtime_id})
            while time.monotonic() - started < args.duration_sec:
                cycle = time.monotonic()
                if (output / 'operations_stop_requested.json').exists():
                    original.atomic_json(output / 'operations_stopped.json', {**runtime, 'stopped_utc': original.utc(), 'reason': 'explicit_stop_request'})
                    break
                try:
                    if original.sha(args.config) != config_sha:
                        raise ValueError('operations_config_changed_restart_required')
                    read_config(args.config)
                    report = run_cycle(operations, base, store, cache, runtime_id=runtime_id)
                    print(json.dumps({key: report[key] for key in ('generated_utc', 'status', 'current_pairs', 'fully_populated_pairs', 'inserted_this_cycle', 'elapsed_seconds', 'peer_alignment')}), flush=True)
                except Exception as exc:
                    original.atomic_json(output / 'status.json', {'schema': original.SCHEMA,
                        'operations_schema': SCHEMA, 'generated_utc': original.utc(), 'status': 'error',
                        'operations_runtime_id': runtime_id,
                        'reason': type(exc).__name__ + ':' + str(exc)[:400], **FLAGS})
                    if args.once:
                        raise
                if args.once:
                    break
                time.sleep(max(1., operations['interval_seconds'] - (time.monotonic() - cycle)))
        finally:
            store.close()


if __name__ == '__main__':
    main()
