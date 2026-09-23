"""Create causal expanding-fold inputs from accepted raw rolling features.

This package contains no fitted predictive model and no duplicated labels.
Each normalizer sees all earlier real feature origins, before clock sampling
or any outcome filtering. Existing inputs and live services are read-only.
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
from oanda_rolling_model_design_v1 import fit_normalizer, transform_inputs
from oanda_rolling_technical_dataset_v1 import file_sha, key_hash, iter_partitions

SCHEMA = 'rolling_specialist_fold_inputs_v1_20260915'
MAX_BYTES = 2 * 1024**3
MIN_FREE_BYTES = 32 * 1024**3
HORIZONS = (30, 60)
TRAIN_START = int(datetime(2026, 7, 13, tzinfo=timezone.utc).timestamp())
FIT_CUTOFF_DATES = ('2026-07-27', '2026-08-03', '2026-08-10', '2026-08-17', '2026-08-24')
FIT_CUTOFFS = tuple(int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp()) for s in FIT_CUTOFF_DATES)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n', encoding='utf-8')


def checked(root, relative, digest):
    root = Path(root).resolve()
    path = (root / relative).resolve()
    require(path.is_relative_to(root) and path.is_file(), 'contained_existing_artifact_required')
    require(file_sha(path) == digest, 'input_artifact_hash_mismatch:' + relative)
    return path


def assert_pins(bindings):
    for name, digest in bindings.items():
        checked(ROOT, name, digest)


def merged_bindings(*manifests):
    bindings = {}
    for manifest in manifests:
        for name, digest in manifest['source_bindings'].items():
            require(name not in bindings or bindings[name] == digest, 'incompatible_inherited_source_binding')
            bindings.setdefault(name, digest)
    for name in ('tools/prepare_rolling_specialists_v1.py', 'oanda_rolling_model_design_v1.py',
                 'oanda_rolling_technical_dataset_v1.py'):
        digest = file_sha(ROOT / name)
        require(name not in bindings or bindings[name] == digest, 'inherited_source_changed')
        bindings.setdefault(name, digest)
    assert_pins(bindings)
    return bindings


def validate_destination(output):
    output = Path(output).resolve()
    require(not output.exists() and output.is_relative_to(ROOT / 'data'), 'new_independent_project_data_output_required')
    require(output.parent.is_dir(), 'existing_output_parent_required')
    require(shutil.disk_usage(output.parent).free >= MIN_FREE_BYTES + MAX_BYTES, 'two_gib_above_32gib_reserve_required')
    return output


def exact_arrays(left, right):
    left, right = np.asarray(left), np.asarray(right)
    if left.shape != right.shape or left.dtype != right.dtype:
        return False
    if left.dtype.kind != 'f':
        return np.array_equal(left, right)
    finite = np.isfinite(left)
    unsigned = np.uint64 if left.dtype.itemsize == 8 else np.uint32
    return (np.array_equal(finite, np.isfinite(right))
            and np.array_equal(np.isnan(left), np.isnan(right))
            and np.array_equal(np.isposinf(left), np.isposinf(right))
            and np.array_equal(np.isneginf(left), np.isneginf(right))
            and np.array_equal(left[finite].view(unsigned), right[finite].view(unsigned)))


def exact_join(source_times, wanted_times):
    source_times, wanted_times = np.asarray(source_times), np.asarray(wanted_times)
    require(source_times.ndim == wanted_times.ndim == 1 and len(source_times) > 0,
            'nonempty_source_and_vector_clocks_required')
    require(source_times.dtype.kind in 'iu' and wanted_times.dtype.kind in 'iu', 'integer_original_clocks_required')
    require(np.all(source_times[1:] > source_times[:-1]) and np.all(wanted_times[1:] > wanted_times[:-1]),
            'unique_ascending_original_clocks_required')
    index = np.searchsorted(source_times, wanted_times)
    require(np.all(index < len(source_times)) and np.array_equal(source_times[index], wanted_times),
            'every_retained_original_key_must_exist')
    return index


def prepare_pair_values(raw_times, raw_x, retained_times, retained_split, old_x,
                        quote_times, mid, bid, ask, *, cutoffs=FIT_CUTOFFS, start=TRAIN_START):
    raw_times, raw_x = np.asarray(raw_times), np.asarray(raw_x)
    retained_times, retained_split, old_x = np.asarray(retained_times), np.asarray(retained_split), np.asarray(old_x)
    require(raw_x.dtype == np.float64 and raw_x.ndim == 2 and raw_x.shape[0] == len(raw_times), 'raw_float64_registered_matrix_required')
    require(not np.isinf(raw_x).any(), 'raw_features_finite_or_nan_only')
    require(retained_split.shape == retained_times.shape and np.isin(retained_split, (0, 1, 2)).all(), 'explicit_original_split_required')
    require(old_x.dtype == np.float32 and old_x.shape == (len(retained_times), raw_x.shape[1]), 'old_float32_selected_feature_matrix_required')
    require(np.all(retained_times[retained_split == 0] % 900 == 0), 'original_utc15m_train_sample_required')
    require(tuple(cutoffs) == tuple(sorted(set(cutoffs))) and len(cutoffs) > 0 and start < cutoffs[0], 'ordered_expanding_prefix_cutoffs_required')
    selected = exact_join(raw_times, retained_times)
    raw_selected = raw_x[selected].copy()
    params = [fit_normalizer(raw_x[(raw_times >= start) & (raw_times < cutoff)]) for cutoff in cutoffs]
    final_x = transform_inputs(raw_selected, params[-1])
    require(exact_arrays(final_x, old_x), 'final_transform_must_match_accepted_float32_bits_and_masks')
    quote_index = exact_join(quote_times, retained_times)
    m, b, a = (np.asarray(values, dtype=np.float64)[quote_index] for values in (mid, bid, ask))
    finite = np.isfinite(m) & np.isfinite(b) & np.isfinite(a) & (m > 0) & (b > 0) & (a >= b)
    require(finite.all(), 'all_retained_current_quote_prices_valid')
    long_entry = (a - m) / m * 10000.
    short_entry = (m - b) / m * 10000.
    require(np.isfinite(long_entry).all() and np.isfinite(short_entry).all(), 'finite_current_entry_costs_required')
    require(np.all(long_entry >= 0) and np.all(short_entry >= 0), 'current_midpoint_must_be_inside_bidask')
    payload = {'raw_x': raw_selected, 'time': retained_times.astype(np.int64), 'split': retained_split.astype(np.int8),
               'known_entry_long_bps': long_entry, 'known_entry_short_bps': short_entry}
    for name in ('count', 'mean', 'scale', 'supported'):
        payload['normalizer_' + name] = np.stack([p[name] for p in params])
    return payload


def write_payload(path, payload):
    require(not path.exists(), 'new_pair_artifact_required')
    np.savez_compressed(path, **payload)
    with np.load(path, allow_pickle=False) as actual:
        require(set(actual.files) == set(payload), 'exact_npz_schema_readback_required')
        require(all(exact_arrays(value, actual[name]) for name, value in payload.items()), 'exact_npz_finite_bits_and_missing_masks_required')
    return {'path': 'pairs/' + path.name, 'sha256': file_sha(path), 'bytes': path.stat().st_size,
            'rows': len(payload['time']), 'key_sha256': key_hash(payload['time']),
            'readback': 'all arrays and finite float64/float32 bit patterns plus missing masks verified'}


def run(args):
    base, prepared, quotes = (Path(value).resolve() for value in (args.base, args.prepared, args.quotes))
    paths = {'base': base / 'DATASET.json', 'prepared': prepared / 'PREPARED.json', 'quotes': quotes / 'QUOTE_PANEL.json'}
    manifests = {name: json.loads(path.read_bytes()) for name, path in paths.items()}
    hashes = {name: file_sha(path) for name, path in paths.items()}
    bm, pm, qm = (manifests[name] for name in ('base', 'prepared', 'quotes'))
    require(all(m['status'] == 'complete' for m in manifests.values()), 'completed_inputs_required')
    require(len(bm['pairs']) == 68 and len(bm['partitions']) == 544 and set(bm['pairs']) == set(pm['pairs']) == set(qm['pairs']), 'all68_complete_raw_and_prepared_pairs_required')
    require(pm['base_sha256'] == qm['base_manifest_sha256'] == hashes['base'], 'same_accepted_base_required')
    require(pm['overlay_sha256'] == qm['endpoint_manifest_sha256'], 'same_accepted_endpoint_overlay_required')
    require(bm['boundaries'] == pm['boundaries'] == qm['boundaries'], 'same_original_time_boundaries_required')
    require(bm['boundaries']['start'] == TRAIN_START and bm['boundaries']['train_end'] == FIT_CUTOFFS[-1], 'declared_jul13_aug24_train_required')
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
              'boundaries': bm['boundaries'], 'fit_cutoff_epochs': list(FIT_CUTOFFS), 'fit_cutoff_dates_utc': list(FIT_CUTOFF_DATES),
              'fold_cutoffs_epoch': list(FIT_CUTOFFS),
              'oof_intervals': [{'start': FIT_CUTOFFS[i], 'end': FIT_CUTOFFS[i + 1], 'normalizer_index': i} for i in range(4)],
              'folds': [{'normalizer_index': i, 'fit_start_epoch': TRAIN_START, 'fit_cutoff_epoch': FIT_CUTOFFS[i],
                         'oof_start_epoch': FIT_CUTOFFS[i], 'oof_end_epoch': FIT_CUTOFFS[i + 1]} for i in range(4)],
              'final_normalizer_index': 4,
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
                                          qt['quote__mid_close'].to_numpy(), qt['quote__bid_close'].to_numpy(), qt['quote__ask_close'].to_numpy())
        reserve = sum(value.nbytes for value in payload.values()) * 2 + 1024**2
        require(report['bytes'] + reserve < MAX_BYTES and shutil.disk_usage(output).free - reserve >= MIN_FREE_BYTES,
                'bounded_specialist_preparation_storage_required')
        record = write_payload(output / 'pairs' / (pair + '.npz'), payload)
        record.update(raw_original_rows=len(times), training_clock_rows=int((payload['split'] == 0).sum()),
                      validation_rows=int((payload['split'] == 1).sum()), later_development_test_rows=int((payload['split'] == 2).sum()),
                      supported_fields_by_cutoff=payload['normalizer_supported'].sum(axis=1).tolist(),
                      prefix_original_rows_by_cutoff=[int(((times >= TRAIN_START) & (times < cutoff)).sum()) for cutoff in FIT_CUTOFFS],
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
        require(report['raw_partitions_verified'] == 544 and report['raw_origin_rows'] == bm['origin_rows'], 'all544_raw_partitions_and_origins_required')
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
