"""Date-generalized eight-week specialist inputs; pinned v1 numerics unchanged.

Exact six-week TRAIN followed by two separate one-week assessment periods.
Four chronological OOF blocks follow an initial two-week prefix. Missing raw
weeks do not authorize fabricated rows or a reduced instrument universe.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import numpy as np
import pyarrow.parquet as pq
from tools import prepare_rolling_specialists_v1 as legacy
from oanda_rolling_model_design_v1 import fit_normalizer, transform_inputs
from oanda_rolling_technical_dataset_v1 import file_sha, key_hash, iter_partitions

SCHEMA = 'rolling_specialist_fold_inputs_v2_20260915'
LEGACY_SHA256 = '8d6dbe5140efc45b3652e9c9e113979f562ef17c2df4759a015d174b642bf737'
WEEK = 7 * 86400
MAX_BYTES = legacy.MAX_BYTES
MIN_FREE_BYTES = legacy.MIN_FREE_BYTES
HORIZONS = legacy.HORIZONS
require = legacy.require
save = legacy.save
checked = legacy.checked
assert_pins = legacy.assert_pins
validate_destination = legacy.validate_destination
exact_arrays = legacy.exact_arrays
write_payload = legacy.write_payload


def verify_legacy():
    require(file_sha(ROOT / 'tools/prepare_rolling_specialists_v1.py') == LEGACY_SHA256,
            'pinned_v1_preparation_source_changed')


def derive_schedule(boundaries):
    required = ('start', 'train_end', 'validation_end', 'end')
    require(isinstance(boundaries, dict) and set(boundaries) == set(required),
            'exact_four_period_boundaries_required')
    require(all(isinstance(boundaries[k], int) and not isinstance(boundaries[k], bool)
                and boundaries[k] % 60 == 0 for k in required), 'integer_aligned_minute_boundaries_required')
    start, train_end, validation_end, end = (boundaries[k] for k in required)
    require(train_end - start == 6 * WEEK and validation_end - train_end == WEEK
            and end - validation_end == WEEK, 'exact_six_train_plus_one_plus_one_weeks_required')
    # Calendar alignment is explicit; the selected windows start on Monday UTC.
    dt = datetime.fromtimestamp(start, timezone.utc)
    require(dt.weekday() == 0 and start % 86400 == 0, 'monday_utc_midnight_period_start_required')
    cutoffs = [start + weeks * WEEK for weeks in (2, 3, 4, 5, 6)]
    return {'start': start, 'train_end': train_end, 'validation_end': validation_end, 'end': end,
            'fit_cutoffs_epoch': cutoffs,
            'fit_cutoff_dates_utc': [datetime.fromtimestamp(t, timezone.utc).strftime('%Y-%m-%d') for t in cutoffs],
            'period_identity': dt.strftime('%Y%m%d') + '_' + datetime.fromtimestamp(end, timezone.utc).strftime('%Y%m%d'),
            'initial_training_weeks': 2, 'oof_week_count': 4, 'final_normalizer_index': 4,
            'training_weeks': 6, 'assessment_week_count': 2}


def validate_partitions(manifest):
    pairs = manifest['pairs']
    require(len(pairs) == 68, 'all68_instrument_universe_required')
    seen = set()
    totals = {pair: 0 for pair in pairs}
    for record in manifest['partitions']:
        pair, block = record['pair'], record['block']
        require(pair in pairs and isinstance(block, int) and not isinstance(block, bool)
                and 0 <= block < 8, 'declared_pair_and_eight_week_block_required')
        require((pair, block) not in seen, 'unique_pair_week_partition_required')
        seen.add((pair, block))
        require(record['core']['rows'] > 0 and record['core']['rows'] == record['peers']['rows'],
                'positive_exact_core_peer_partition_rows_required')
        totals[pair] += record['core']['rows']
    require(all(totals[p] == pairs[p]['origin_rows'] and totals[p] > 0 for p in pairs)
            and sum(totals.values()) == manifest['origin_rows'], 'manifest_partition_population_identity_required')
    # A missing week is evidence of missing data, not a reason to synthesize a
    # partition; all existing partitions and all original rows are mandatory.
    return {'partitions': len(seen), 'possible_pair_weeks': 68 * 8,
            'missing_pair_weeks': 68 * 8 - len(seen)}


def merged_bindings(*manifests):
    verify_legacy()
    bindings = legacy.merged_bindings(*manifests)
    name = 'tools/prepare_rolling_specialists_v2.py'
    current = file_sha(ROOT / name)
    require(name not in bindings or bindings[name] == current, 'v2_inherited_source_changed')
    bindings[name] = current
    assert_pins(bindings)
    return bindings


def prepare_pair_values(raw_times, raw_x, retained_times, retained_split, old_x,
                        quote_times, mid, bid, ask, *, boundaries):
    schedule = derive_schedule(boundaries)
    t = np.asarray(raw_times)
    selected = np.asarray(retained_times)
    split = np.asarray(retained_split)
    require(t.ndim == 1 and t.dtype.kind in 'iu' and len(t) > 0
            and np.all((t >= schedule['start']) & (t < schedule['end'])),
            'raw_origins_inside_declared_period_required')
    require(selected.ndim == 1 and selected.dtype.kind in 'iu'
            and np.all((selected >= schedule['start']) & (selected < schedule['end'])),
            'retained_origins_inside_declared_period_required')
    expected = np.where(selected < schedule['train_end'], 0,
                        np.where(selected < schedule['validation_end'], 1, 2))
    require(np.array_equal(split, expected), 'original_split_matches_period_boundaries_required')
    retain = (t >= schedule['train_end']) | (t % 900 == 0)
    require(np.array_equal(selected, t[retain]), 'all_original_sampled_train_and_assessment_origins_required')
    return legacy.prepare_pair_values(raw_times, raw_x, retained_times, retained_split, old_x,
        quote_times, mid, bid, ask, start=schedule['start'], cutoffs=tuple(schedule['fit_cutoffs_epoch']))


def run(args):
    verify_legacy()
    base, prepared, quotes = (Path(value).resolve() for value in (args.base, args.prepared, args.quotes))
    paths = {'base': base / 'DATASET.json', 'prepared': prepared / 'PREPARED.json', 'quotes': quotes / 'QUOTE_PANEL.json'}
    manifests = {name: json.loads(path.read_bytes()) for name, path in paths.items()}
    hashes = {name: file_sha(path) for name, path in paths.items()}
    bm, pm, qm = (manifests[name] for name in ('base', 'prepared', 'quotes'))
    require(all(m['status'] == 'complete' for m in manifests.values()), 'completed_inputs_required')
    require(len(bm['pairs']) == 68 and set(bm['pairs']) == set(pm['pairs']) == set(qm['pairs']), 'all68_complete_raw_and_prepared_pairs_required')
    require(pm['base_sha256'] == qm['base_manifest_sha256'] == hashes['base'], 'same_accepted_base_required')
    require(pm['overlay_sha256'] == qm['endpoint_manifest_sha256'], 'same_accepted_endpoint_overlay_required')
    require(bm['boundaries'] == pm['boundaries'] == qm['boundaries'], 'same_original_time_boundaries_required')
    schedule = derive_schedule(bm['boundaries'])
    start = schedule['start']
    cutoffs = tuple(schedule['fit_cutoffs_epoch'])
    partition_coverage = validate_partitions(bm)
    names = list(pm['groups']['compact50'])
    require(len(names) == len(set(names)) == 50 and set(names) <= set(bm['feature_names']), 'registered_compact50_features_required')
    require(names[:38] == pm['groups']['compact38'] and all(n.startswith('peer__') for n in names[38:]), 'compact38_then12_peer_mapping_required')
    columns = [pm['feature_names'].index(name) for name in names]
    bindings = merged_bindings(bm, pm, qm)
    output = validate_destination(args.output)
    report = {'schema': SCHEMA, 'status': 'building', 'base_root': str(base), 'prepared_root': str(prepared), 'quote_root': str(quotes),
              'base_sha256': hashes['base'], 'prepared_sha256': hashes['prepared'], 'quote_sha256': hashes['quotes'],
              'endpoint_manifest_sha256': pm['overlay_sha256'], 'source_bindings': bindings,
              'feature_names': names, 'groups': {'compact38': names[:38], 'compact50': names}, 'horizons': list(HORIZONS),
              'boundaries': bm['boundaries'], 'fit_cutoff_epochs': list(cutoffs), 'fit_cutoff_dates_utc': schedule['fit_cutoff_dates_utc'],
              'fold_cutoffs_epoch': list(cutoffs),
              'oof_intervals': [{'start': cutoffs[i], 'end': cutoffs[i + 1], 'normalizer_index': i} for i in range(4)],
              'folds': [{'normalizer_index': i, 'fit_start_epoch': start, 'fit_cutoff_epoch': cutoffs[i],
                         'oof_start_epoch': cutoffs[i], 'oof_end_epoch': cutoffs[i + 1]} for i in range(4)],
              'final_normalizer_index': 4, 'schedule': schedule,
              'period_identity': schedule['period_identity'],
              'adapter_source': 'date-generalized preparation; exact pinned v1 numerical helper reused',
              'feature_family_ablation_applied': False, 'partition_coverage': partition_coverage,
              'normalizer_scope': 'per pair, ALL original raw feature origins with declared start<=bar_start<fit cutoff; before clock sampling and outcome filtering; each cutoff independently fit',
              'normalizer_contract': 'count>=20 and positive exact finite range/variance; unsupported field stays NaN; float64 statistics; caller transforms to float32',
              'fit_label_rule': 'original UTC15m TRAIN sample, finite valid horizon endpoints, target candle END strictly less than fold fit cutoff',
              'oof_issuance_rule': 'all retained original sampled TRAIN origins in fold interval, before future endpoint or maturity masks',
              'meta_label_rule': 'target candle END strictly less than its OOF fold end; labels read from exact bound prepared input',
              'entry_cost_definition': 'known_entry_long=(ask-mid)/mid*10000; known_entry_short=(mid-bid)/mid*10000; original completed-candle quotes only, not terminal quotes',
              'raw_input_reconstruction': 'exact float64 registered core+peer columns; no inverse of rounded old z-values; no candle or feature regeneration',
              'labels_duplicated': False, 'models_fit': 0, 'can_place_orders': False, 'live_changes': False,
              'maximum_bytes': MAX_BYTES, 'minimum_free_bytes': MIN_FREE_BYTES, 'pairs': {}, 'rows': 0, 'bytes': 0,
              'raw_partitions_verified': 0, 'raw_origin_rows': 0, 'final_transform_exact_to_old_prepared': False,
              'started_utc': datetime.now(timezone.utc).isoformat()}
    created = False
    began = time.monotonic()

    def process_pair(pair, tables):
        require(pair not in report['pairs'], 'one_contiguous_partition_group_per_pair_required')
        times = np.concatenate([table['bar_start_epoch'].to_numpy() for table in tables])
        raw = np.concatenate([np.column_stack([table[name].to_numpy() for name in names]) for table in tables])
        require(len(times) == bm['pairs'][pair]['origin_rows'], 'every_original_raw_pair_origin_required')
        require(np.all((times >= start) & (times < bm['boundaries']['end'])), 'raw_origins_inside_declared_period_required')
        pr, qr = pm['pairs'][pair], qm['pairs'][pair]
        pp = checked(prepared, pr['path'], pr['sha256'])
        qp = checked(quotes, qr['path'], qr['sha256'])
        checked(quotes, qr['receipt_path'], qr['receipt_sha256'])
        qt = pq.ParquetFile(qp).read(columns=['instrument', 'bar_start_epoch', 'quote__mid_close', 'quote__bid_close', 'quote__ask_close'])
        qt_times = qt['bar_start_epoch'].to_numpy()
        require(len(qt_times) == qr['rows'] and key_hash(qt_times) == qr['key_sha256'], 'quote_original_clock_digest_required')
        require(all(value == pair for value in qt['instrument'].to_pylist()), 'quote_instrument_identity_required')
        with np.load(pp, allow_pickle=False) as old:
            require(len(old['time']) == pr['rows'] and key_hash(old['time']) == pr['key_sha256'], 'old_prepared_original_key_digest_required')
            expected_split = np.where(old['time'] < pm['boundaries']['train_end'], 0,
                                      np.where(old['time'] < pm['boundaries']['validation_end'], 1, 2))
            require(np.array_equal(old['split'], expected_split), 'old_split_matches_original_clock_required')
            payload = prepare_pair_values(times, raw, old['time'], old['split'], old['x'][:, columns], qt_times,
                                          qt['quote__mid_close'].to_numpy(), qt['quote__bid_close'].to_numpy(), qt['quote__ask_close'].to_numpy(), boundaries=bm['boundaries'])
        reserve = sum(value.nbytes for value in payload.values()) * 2 + 1024**2
        require(report['bytes'] + reserve < MAX_BYTES and shutil.disk_usage(output).free - reserve >= MIN_FREE_BYTES,
                'bounded_specialist_preparation_storage_required')
        record = write_payload(output / 'pairs' / (pair + '.npz'), payload)
        record.update(raw_original_rows=len(times), training_clock_rows=int((payload['split'] == 0).sum()),
                      validation_rows=int((payload['split'] == 1).sum()), later_development_test_rows=int((payload['split'] == 2).sum()),
                      raw_split_counts={name: int(((times >= lower) & (times < upper)).sum()) for name, lower, upper in (('train',start,bm['boundaries']['train_end']),('validation',bm['boundaries']['train_end'],bm['boundaries']['validation_end']),('later_development_test',bm['boundaries']['validation_end'],bm['boundaries']['end']))},
                      supported_fields_by_cutoff=payload['normalizer_supported'].sum(axis=1).tolist(),
                      prefix_original_rows_by_cutoff=[int(((times >= start) & (times < cutoff)).sum()) for cutoff in cutoffs],
                      final_float32_transform_exact=True, old_prepared_sha256=pr['sha256'], quote_pair_sha256=qr['sha256'])
        report['pairs'][pair] = record
        report['rows'] += record['rows']
        report['raw_origin_rows'] += len(times)
        report['bytes'] += record['bytes']
        save(output / 'SPECIALIST_INPUTS.json', report)
        print(json.dumps({'pair': pair, 'complete_pairs': len(report['pairs']), 'rows': record['rows'], 'elapsed_seconds': round(time.monotonic() - began, 1)}), flush=True)

    try:
        output.mkdir()
        created = True
        (output / 'pairs').mkdir()
        save(output / 'SPECIALIST_INPUTS.json', report)
        previous, buffered = None, []
        for pair, table in iter_partitions(base, feature_names=names):
            require(all(value == pair for value in table['instrument'].to_pylist()), 'raw_instrument_identity_required')
            require(np.array_equal(table['bar_end_epoch'].to_numpy(), table['bar_start_epoch'].to_numpy() + 60), 'raw_original_bar_end_identity_required')
            if previous is not None and pair != previous:
                process_pair(previous, buffered)
                buffered = []
            previous = pair
            buffered.append(table)
            report['raw_partitions_verified'] += 1
        if buffered:
            process_pair(previous, buffered)
        require(set(report['pairs']) == set(pm['pairs']) and report['rows'] == pm['rows'], 'all68_exact_retained_origins_required')
        require(report['raw_partitions_verified'] == len(bm['partitions']) and report['raw_origin_rows'] == bm['origin_rows'], 'all_manifest_raw_partitions_and_origins_required')
        for pair, record in report['pairs'].items():
            checked(output, record['path'], record['sha256'])
            checked(prepared, pm['pairs'][pair]['path'], pm['pairs'][pair]['sha256'])
            checked(quotes, qm['pairs'][pair]['path'], qm['pairs'][pair]['sha256'])
            checked(quotes, qm['pairs'][pair]['receipt_path'], qm['pairs'][pair]['receipt_sha256'])
        require(all(file_sha(path) == hashes[name] for name, path in paths.items()), 'input_metadata_changed_during_preparation')
        assert_pins(bindings)
        require(report['bytes'] < MAX_BYTES and shutil.disk_usage(output).free >= MIN_FREE_BYTES, 'final_storage_bound_required')
        report.update(status='complete', completed_utc=datetime.now(timezone.utc).isoformat(),
                      elapsed_seconds=round(time.monotonic() - began, 3), final_transform_exact_to_old_prepared=True,
                      assessment_rows=sum(r['validation_rows'] + r['later_development_test_rows'] for r in report['pairs'].values()),
                      sampled_training_rows=sum(r['training_clock_rows'] for r in report['pairs'].values()))
        save(output / 'SPECIALIST_INPUTS.json', report)
    except BaseException as exc:
        report.update(status='failed', failure={'type': type(exc).__name__, 'message': str(exc)})
        if created:
            save(output / 'SPECIALIST_INPUTS.json', report)
        raise
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', type=Path, required=True)
    parser.add_argument('--prepared', type=Path, required=True)
    parser.add_argument('--quotes', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    result = run(parser.parse_args())
    print(json.dumps({name: result[name] for name in ('status', 'rows', 'raw_origin_rows', 'bytes', 'elapsed_seconds')}), flush=True)
