"""Build separate endpoint-only labels against a completed immutable base.

Each pair is re-read using exactly the base input receipt bounds/cutoff. Hash
mismatches abort instead of combining revisions. At most three worker processes
write disjoint pair sidecars within fixed per-pair allocations. No base files,
raw candle sources, live configurations, models or trading are changed.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
import multiprocessing
from pathlib import Path
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import oanda_rolling_technical_endpoint_labels_v1 as endpoint
import oanda_rolling_technical_inputs_v1 as inputs
import oanda_rolling_technical_ranges_v1 as ranges
from oanda_rolling_technical_dataset_v1 import SCHEMA as BASE_SCHEMA, file_sha, key_hash, split_maturity

SOURCE_NAMES = ('oanda_rolling_technical_endpoint_labels_v1.py',
                'tools/build_rolling_technical_endpoints_v1.py')
METADATA_RESERVE = 32 * 1024**2
TRANSIENT = ('changed_during_read', 'replaced_during_read', 'replaced_before_open',
             'truncated_during_read', 'rewritten_during_read', 'sharing violation')


def save(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, indent=2, allow_nan=False)+'\n', encoding='utf-8')


def assert_pins(bindings):
    for name, digest in bindings.items():
        if file_sha(ROOT/name) != digest:
            raise ValueError('overlay_or_base_source_binding_changed:'+name)


def verify_input_identity(previous, current):
    for name in ('pair', 'requested_start_epoch', 'requested_end_epoch_exclusive', 'observed_epoch',
                 'retained_rows', 'normalized_arrays_sha256', 'inputs_source_sha256', 'cutoff_epoch'):
        if previous.get(name) != current.get(name) or previous.get(name) is None:
            raise ValueError('base_input_identity_mismatch:'+name)
    old = {s['lane']:s for s in previous['sources']}
    new = {s['lane']:s for s in current['sources']}
    if set(old) != set(new):
        raise ValueError('base_input_source_lanes_changed')
    original_checked = []
    for lane, before in old.items():
        after = new[lane]
        if before['selected'] != after['selected']:
            raise ValueError('base_input_primary_selection_changed')
        if not before['selected']:
            continue
        for key in ('retained_rows', 'selected_normalized_rows_sha256', 'normalized_arrays_sha256'):
            if before.get(key) is None or before[key] != after.get(key):
                raise ValueError('base_input_source_digest_mismatch:'+lane+':'+key)
        key = 'selected_original_rows_sha256'
        if before.get(key) is not None:
            if before[key] != after.get(key):
                raise ValueError('base_selected_original_rows_changed:'+lane)
            original_checked.append(lane)
    return {'normalized_arrays_match':True, 'selected_normalized_rows_match':True,
            'selected_original_rows_checked':original_checked,
            'csv_prefix_growth_allowed_only_if_selected_rows_match':True}


def _float_bits_equal(left, right):
    left, right = np.asarray(left, dtype=np.float64), np.asarray(right, dtype=np.float64)
    if not np.array_equal(np.isfinite(left), np.isfinite(right)) or not np.array_equal(np.isnan(left), np.isnan(right)):
        return False
    finite = np.isfinite(left)
    return np.array_equal(left[finite].view(np.uint64), right[finite].view(np.uint64))


def _write_sidecar(path, columns, job, written_bytes):
    table = pa.table({name:pa.array(value, mask=~np.isfinite(value))
                     if np.asarray(value).dtype.kind == 'f' else pa.array(value)
                     for name,value in columns.items()})
    reserve = table.nbytes*2+1024**2
    if written_bytes+reserve > job['pair_byte_cap']:
        raise ValueError('endpoint_pair_storage_allocation_exceeded')
    if shutil.disk_usage(job['output']).free-reserve*job['workers'] < job['minimum_free_bytes']:
        raise ValueError('endpoint_free_space_reserve')
    if path.exists():
        raise ValueError('new_endpoint_shard_required')
    dictionaries = [field.name for field in table.schema
                    if pa.types.is_string(field.type) or pa.types.is_large_string(field.type)]
    pq.write_table(table, path, compression='zstd', row_group_size=8192,
                   use_dictionary=dictionaries)
    actual = pq.ParquetFile(path).read()
    if not table.equals(actual, check_metadata=True):
        raise ValueError('endpoint_full_table_readback_mismatch')
    for name,value in columns.items():
        if np.asarray(value).dtype.kind == 'f' and not _float_bits_equal(value, actual[name].to_numpy()):
            raise ValueError('endpoint_float_bit_readback_mismatch:'+name)
    return {'path':path.relative_to(job['output']).as_posix(), 'bytes':path.stat().st_size,
            'sha256':file_sha(path), 'rows':len(columns['bar_start_epoch']),
            'readback':'entire Arrow table, finite float64 bits and missing masks verified'}


def _process_pair(job):
    """Independent disjoint-output worker; suitable for Windows spawn."""
    assert_pins(job['bindings'])
    base, output = Path(job['base']), Path(job['output'])
    if file_sha(base/'DATASET.json') != job['base_manifest_sha256']:
        raise ValueError('base_manifest_changed')
    old_path = endpoint.checked_path(base, job['receipt_path'], job['receipt_sha256'])
    previous = json.loads(old_path.read_bytes())
    retries = []
    for attempt in range(3):
        try:
            data, current = ranges.read_range(job['pair'], job['recipe'],
                previous['requested_start_epoch'], previous['requested_end_epoch_exclusive'],
                previous['observed_epoch'], max_rows=previous['max_rows'], batch_rows=previous['batch_rows'])
            break
        except (OSError,ValueError) as exc:
            if attempt == 2 or not any(word in str(exc).lower() for word in TRANSIENT):
                raise
            retries.append(type(exc).__name__+':'+str(exc))
            time.sleep(.2)
    identity = verify_input_identity(previous, current)
    values = endpoint.compute_endpoint_outcomes(data, coverage_end_epoch=previous['requested_end_epoch_exclusive'])
    expected_times = data['time'][(data['time'] >= job['boundaries']['start']) & (data['time'] < job['boundaries']['end'])]
    consumed_times = []
    partitions, summaries = [], {str(h):Counter() for h in endpoint.DEFAULT_HORIZONS}
    written = 0
    checks = 0
    for record in job['partitions']:
        core_path = endpoint.checked_path(base, record['core']['path'], record['core']['sha256'])
        endpoint.checked_path(base, record['peers']['path'], record['peers']['sha256'])
        selected = ['instrument','bar_start_epoch','origin_split']
        for h in endpoint.DEFAULT_HORIZONS:
            selected += [f'label__{h}m__{field}' for field in
                         (*endpoint.NUMERIC_FIELDS, 'target_bar_start_epoch','target_bar_end_epoch',
                          'assumed_available_epoch','midpoint_valid','bidask_endpoint_valid','state','split_eligible')]
        core = pq.ParquetFile(core_path).read(columns=selected)
        times = core['bar_start_epoch'].to_numpy()
        if (core.num_rows != record['core']['rows'] or key_hash(times) != record['key_sha256'] or
                any(p != job['pair'] for p in core['instrument'].to_pylist())):
            raise ValueError('base_core_pair_or_clock_identity_mismatch')
        indexes = np.searchsorted(data['time'], times)
        if np.any(indexes >= len(data['time'])) or not np.array_equal(data['time'][indexes],times):
            raise ValueError('every_base_origin_must_match_reread_input')
        consumed_times.append(times)
        columns = {'bar_start_epoch':times}
        columns.update({name:a[indexes] for name,a in values.items()})
        for h in endpoint.DEFAULT_HORIZONS:
            prefix = f'endpoint_label__{h}m__'
            copied = core[f'label__{h}m__split_eligible'].to_numpy()
            independent = split_maturity(times,h,job['boundaries']['start'],job['boundaries']['train_end'],
                                          job['boundaries']['validation_end'],job['boundaries']['end'])
            if not np.array_equal(copied, independent):
                raise ValueError('base_split_boundary_flag_mismatch')
            columns[prefix+'split_eligible'] = copied
            for field in ('target_bar_start_epoch','target_bar_end_epoch','assumed_available_epoch'):
                if not np.array_equal(core[f'label__{h}m__{field}'].to_numpy(),columns[prefix+field]):
                    raise ValueError('strict_and_endpoint_target_clocks_must_match')
            for field in endpoint.NUMERIC_FIELDS:
                old = core[f'label__{h}m__{field}'].to_numpy()
                mask = np.isfinite(old)
                new = columns[prefix+field]
                if not _float_bits_equal(old[mask], new[mask]):
                    raise ValueError('strict_endpoint_numerical_parity_failed:'+field)
                checks += int(mask.sum())
            strict_mid = core[f'label__{h}m__midpoint_valid'].to_numpy()
            strict_net = core[f'label__{h}m__bidask_endpoint_valid'].to_numpy()
            mid, net = columns[prefix+'midpoint_valid'], columns[prefix+'bidask_endpoint_valid']
            if np.any(strict_mid & ~mid) or np.any(strict_net & ~net):
                raise ValueError('endpoint_overlay_must_preserve_all_strict_valid_labels')
            summary = summaries[str(h)]
            summary.update({'origins':len(times),'strict_midpoint_valid':int(strict_mid.sum()),
                'endpoint_midpoint_valid':int(mid.sum()),'additional_endpoint_midpoint_valid':int((mid & ~strict_mid).sum()),
                'strict_bidask_valid':int(strict_net.sum()),'endpoint_bidask_valid':int(net.sum()),
                'additional_endpoint_bidask_valid':int((net & ~strict_net).sum()),
                'endpoint_midpoint_split_eligible':int((mid & copied).sum()),
                'strict_midpoint_split_eligible':int((strict_mid & copied).sum())})
        path = output/'shards'/f"{job['pair']}_{record['block']:02d}_endpoints.parquet"
        part = _write_sidecar(path, columns, job, written)
        written += part['bytes']
        part.update(pair=job['pair'],block=record['block'],key_sha256=record['key_sha256'],
                    base_core_sha256=record['core']['sha256'],base_peer_sha256=record['peers']['sha256'])
        partitions.append(part)
        if file_sha(core_path) != record['core']['sha256']:
            raise ValueError('base_core_changed_during_overlay_read')
    if not np.array_equal(np.concatenate(consumed_times), expected_times):
        raise ValueError('overlay_must_retain_exact_complete_base_origin_population')
    assert_pins(job['bindings'])
    if file_sha(old_path) != job['receipt_sha256'] or file_sha(base/'DATASET.json') != job['base_manifest_sha256']:
        raise ValueError('base_manifest_or_receipt_changed')
    receipt = {'pair':job['pair'],'base_receipt_path':job['receipt_path'],
        'base_receipt_sha256':job['receipt_sha256'],'input_identity':identity,'reread':current,
        'read_retries':retries,'strict_valid_numeric_cells_checked':checks,
        'origin_rows':len(expected_times),'outcome_counts':{h:dict(c) for h,c in summaries.items()}}
    receipt_path = output/'receipts'/(job['pair']+'.json')
    save(receipt_path,receipt)
    return {'pair':job['pair'],'origin_rows':len(expected_times),'partitions':partitions,
        'bytes':written+receipt_path.stat().st_size,'receipt_path':receipt_path.relative_to(output).as_posix(),
        'receipt_sha256':file_sha(receipt_path),'strict_valid_numeric_cells_checked':checks,
        'outcome_counts':receipt['outcome_counts']}


def run(args):
    if not 1 <= args.workers <= 3 or not 1 <= args.max_output_gib <= 8 or not 16 <= args.minimum_free_gib <= 128:
        raise ValueError('bounded_endpoint_worker_and_storage_configuration_required')
    base = Path(args.base).resolve()
    base_bytes = (base/'DATASET.json').read_bytes()
    base_sha = hashlib.sha256(base_bytes).hexdigest()
    baseline = json.loads(base_bytes)
    if baseline.get('schema') != BASE_SCHEMA or baseline.get('status') != 'complete' or len(baseline.get('pairs',{})) != 68:
        raise ValueError('completed_68_pair_base_required')
    if file_sha(baseline['source_manifest']) != baseline['source_manifest_sha256']:
        raise ValueError('base_source_manifest_changed')
    recipes = inputs.discover_archive_sources(baseline['source_manifest'])
    if any(r['manifest_sha256'] != baseline['source_manifest_sha256'] for r in recipes.values()):
        raise ValueError('base_source_manifest_changed_during_discovery')
    bindings = dict(baseline['source_bindings'])
    bindings.update({n:file_sha(ROOT/n) for n in SOURCE_NAMES})
    assert_pins(bindings)
    output = Path(args.output).resolve()
    if output.exists() or not output.is_relative_to((ROOT/'data').resolve()) or output.is_relative_to(base):
        raise ValueError('new_independent_project_data_output_required')
    cap = args.max_output_gib*1024**3
    minimum = args.minimum_free_gib*1024**3
    if shutil.disk_usage(output.parent).free-cap < minimum:
        raise ValueError('full_overlay_budget_must_fit_above_drive_reserve')
    registry = endpoint.endpoint_registry(include_split_eligibility=True)
    report = {'schema':endpoint.SCHEMA,'status':'building','base_root':str(base),'base_manifest_sha256':base_sha,
        'source_bindings':bindings,'label_names':[r['name'] for r in registry], 'feature_names':[],
        'feature_modification':False,'base_origin_filtering':False,'model_fits':0,'can_place_orders':False,
        'intermediate_path_required':False,'strict_path_labels_modified':False,
        'scope':'fixed exact candle-close endpoints; spread proxy only; no actual fills/slippage/financing or continuous-tradability claim',
        'future_information':'labels only; never used as forecast entry eligibility or inputs',
        'cohort_comparison':'shared strict-valid cohort and additional endpoint-only cohort must be reported separately',
        'started_utc':datetime.now(timezone.utc).isoformat(),'workers':args.workers,'max_output_bytes':cap,
        'minimum_free_bytes':minimum,'metadata_reserve_bytes':METADATA_RESERVE,
        'pair_byte_cap':(cap-METADATA_RESERVE)//68,'origin_rows':0,'bytes':0,'pairs':{},'partitions':[]}
    # Complete receipt/job preflight before creating any output, so a missing
    # source artifact cannot leave an abandoned manifest marked "building".
    jobs=[]
    for pair in sorted(baseline['pairs']):
        receipt = baseline['pairs'][pair]['source_receipt']
        receipt_sha = file_sha(endpoint.checked_path(base,receipt))
        jobs.append({'base':str(base),'output':str(output),'base_manifest_sha256':base_sha,
            'pair':pair,'recipe':recipes[pair],'receipt_path':receipt,'receipt_sha256':receipt_sha,
            'bindings':bindings,'boundaries':baseline['boundaries'],'workers':args.workers,
            'partitions':[r for r in baseline['partitions'] if r['pair']==pair],
            'minimum_free_bytes':minimum,'pair_byte_cap':report['pair_byte_cap']})
    began=time.monotonic()
    created_output=False
    try:
        output.mkdir();created_output=True
        (output/'shards').mkdir();(output/'receipts').mkdir()
        (output/'BASE_DATASET.json').write_bytes(base_bytes)
        save(output/'ENDPOINT_LABEL_REGISTRY.json',registry)
        save(output/'ENDPOINT_DATASET.json',report)
        if args.workers == 1:
            results=map(_process_pair,jobs)
            executor=None
        else:
            executor=ProcessPoolExecutor(max_workers=args.workers,mp_context=multiprocessing.get_context('spawn'))
            futures=[executor.submit(_process_pair,job) for job in jobs]
            results=(f.result() for f in as_completed(futures))
        try:
            for result in results:
                report['pairs'][result['pair']]={k:v for k,v in result.items() if k not in ('pair','partitions')}
                report['partitions'].extend(result['partitions'])
                report['origin_rows']+=result['origin_rows'];report['bytes']+=result['bytes']
                if report['bytes']+METADATA_RESERVE>cap:
                    raise ValueError('global_endpoint_output_budget_exceeded')
                save(output/'ENDPOINT_DATASET.json',report)
                print(json.dumps({'pair':result['pair'],'rows':result['origin_rows'],'completed_pairs':len(report['pairs']),
                                  'elapsed_seconds':round(time.monotonic()-began,1)}),flush=True)
        finally:
            if executor is not None:
                executor.shutdown(wait=True,cancel_futures=True)
        report['partitions'].sort(key=lambda r:(r['pair'],r['block']))
        if set(report['pairs'])!=set(baseline['pairs']) or report['origin_rows']!=baseline['origin_rows']:
            raise ValueError('complete_base_origin_population_required')
        for record in report['partitions']:
            endpoint.checked_path(output,record['path'],record['sha256'])
        for result in report['pairs'].values():
            endpoint.checked_path(output,result['receipt_path'],result['receipt_sha256'])
        for job in jobs:
            endpoint.checked_path(base,job['receipt_path'],job['receipt_sha256'])
            for record in job['partitions']:
                for kind in ('core','peers'):
                    endpoint.checked_path(base,record[kind]['path'],record[kind]['sha256'])
        if file_sha(base/'DATASET.json')!=base_sha or file_sha(baseline['source_manifest'])!=baseline['source_manifest_sha256']:
            raise ValueError('base_or_source_manifest_changed')
        assert_pins(bindings)
        report.update(status='complete',completed_utc=datetime.now(timezone.utc).isoformat(),
                      elapsed_seconds=round(time.monotonic()-began,3),
                      registry_sha256=file_sha(output/'ENDPOINT_LABEL_REGISTRY.json'))
        save(output/'ENDPOINT_DATASET.json',report)
    except BaseException as exc:
        report.update(status='failed',failure={'type':type(exc).__name__,'message':str(exc)},
                      failed_utc=datetime.now(timezone.utc).isoformat())
        if created_output:
            save(output/'ENDPOINT_DATASET.json',report)
        raise
    return report


def parse_args(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--workers',type=int,default=3)
    parser.add_argument('--max-output-gib',type=int,default=4)
    parser.add_argument('--minimum-free-gib',type=int,default=32)
    return parser.parse_args(argv)


if __name__=='__main__':
    result=run(parse_args())
    print(json.dumps({k:result[k] for k in ('status','origin_rows','bytes','elapsed_seconds')}),flush=True)
