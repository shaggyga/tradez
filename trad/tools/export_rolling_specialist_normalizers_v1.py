"""Copy exact compact V1/V2 prefix normalizers; never decode raw feature rows.

The output is an immutable evidence directory containing NORMALIZERS.npz and
NORMALIZERS.json. Its source manifests and 68 pair artifacts are hash checked
before and after reading. No model/normalizer fitting or live action occurs.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import numpy as np
from oanda_rolling_model_design_v1 import COMPACT_LOCAL
from oanda_rolling_technical_panel_v1 import panel_registry

INPUT_SCHEMAS = {'rolling_specialist_fold_inputs_v1_20260915', 'rolling_specialist_fold_inputs_v2_20260915'}
SCHEMA = 'rolling_specialist_normalizer_extraction_v1_20260915'
ARRAY_DTYPES = {'count': np.dtype('int64'), 'mean': np.dtype('float64'),
                'scale': np.dtype('float64'), 'supported': np.dtype('bool')}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for block in iter(lambda: handle.read(1024*1024), b''):
            value.update(block)
    return value.hexdigest()


def checked(path, expected, byte_count=None):
    path = Path(path)
    before = path.stat()
    require(sha(path) == expected, 'source_artifact_sha256_mismatch:' + str(path))
    after = path.stat()
    require((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns), 'source_changed_while_reading')
    require(byte_count is None or after.st_size == byte_count, 'source_artifact_bytes_mismatch')
    return path


def array_record(value):
    value = np.ascontiguousarray(value)
    return {'dtype': str(value.dtype), 'shape': list(value.shape),
            'c_order_payload_sha256': hashlib.sha256(value.tobytes(order='C')).hexdigest()}


def validate_manifest(manifest):
    require(manifest.get('status') == 'complete' and manifest.get('schema') in INPUT_SCHEMAS,
            'complete_supported_specialist_inputs_required')
    pair_names = sorted(manifest['pairs'])
    require(len(pair_names) == 68 and all(re.fullmatch(r'[A-Z]{3}_[A-Z]{3}', p) for p in pair_names),
            'exact68_named_pairs_required')
    names = list(COMPACT_LOCAL) + [r['name'] for r in panel_registry()]
    require(manifest['feature_names'] == names and len(names) == len(set(names)) == 50,
            'exact_ordered_canonical_compact50_required')
    require(manifest['groups'] == {'compact38': names[:38], 'compact50': names}, 'exact_compact_group_mapping')
    b = manifest['boundaries']
    require(set(b) == {'start', 'train_end', 'validation_end', 'end'} and
            all(isinstance(t, int) and not isinstance(t, bool) and t % 60 == 0 for t in b.values()),
            'exact_integer_minute_boundaries_required')
    week = 7*86400
    require((b['train_end']-b['start'], b['validation_end']-b['train_end'], b['end']-b['validation_end']) == (6*week, week, week),
            'six_plus_one_plus_one_week_period_required')
    cuts = [b['start'] + i*week for i in (2, 3, 4, 5, 6)]
    require(manifest['fold_cutoffs_epoch'] == cuts and manifest['final_normalizer_index'] == 4,
            'five_period_derived_prefix_cutoffs_required')
    return pair_names, names, cuts


def source_path(root, relative):
    path = (root / relative).resolve()
    require(path.is_relative_to(root) and path.is_file(), 'contained_existing_pair_artifact_required')
    return path


def verify_bindings(manifest):
    for name, expected in manifest['source_bindings'].items():
        path = (ROOT/name).resolve()
        require(path.is_relative_to(ROOT) or name == '../direction_decision_20260911/src/signed_cost_models_v1.py',
                'declared_source_binding_path_required')
        checked(path, expected)


def collect(manifest_path):
    manifest_path = Path(manifest_path).resolve()
    manifest_sha = sha(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding='utf-8-sig'))
    checked(manifest_path, manifest_sha)
    pairs, names, cuts = validate_manifest(manifest)
    verify_bindings(manifest)
    arrays = {name: np.empty((5, 68, 50), dtype=dtype) for name, dtype in ARRAY_DTYPES.items()}
    records = {}
    for pid, pair in enumerate(pairs):
        record = manifest['pairs'][pair]
        path = source_path(manifest_path.parent, record['path'])
        checked(path, record['sha256'], record['bytes'])
        copied = {}
        with np.load(path, allow_pickle=False) as source:
            for name, dtype in ARRAY_DTYPES.items():
                # Deliberately access only four normalizer arrays. Do not
                # materialize raw_x, quote contexts, times, splits or labels.
                value = source['normalizer_' + name]
                require(value.shape == (5, 50) and value.dtype == dtype, 'normalizer_exact_dtype_and_shape:' + name)
                arrays[name][:, pid, :] = value
                copied[name] = array_record(value)['c_order_payload_sha256']
        checked(path, record['sha256'], record['bytes'])
        counts = arrays['count'][:, pid]
        support = arrays['supported'][:, pid]
        require(np.all(counts >= 0), 'nonnegative_normalizer_counts')
        require(np.all(~support | ((counts >= 20) & np.isfinite(arrays['mean'][:, pid])
                & np.isfinite(arrays['scale'][:, pid]) & (arrays['scale'][:, pid] > 0))),
                'supported_normalizer_numeric_contract')
        support_counts = support.sum(axis=1).tolist()
        require(support_counts == record['supported_fields_by_cutoff'], 'declared_supported_counts_match_arrays')
        prefix_counts = record['prefix_original_rows_by_cutoff']
        require(len(prefix_counts) == 5 and all(isinstance(x, int) and not isinstance(x, bool) and x > 0 for x in prefix_counts)
                and all(a <= b for a, b in zip(prefix_counts, prefix_counts[1:])), 'ordered_original_prefix_row_counts')
        require(np.all(counts <= np.asarray(prefix_counts)[:, None]), 'finite_counts_within_original_prefix_population')
        records[pair] = {'path': str(path), 'bytes': record['bytes'], 'sha256': record['sha256'],
                         'array_sha256': copied, 'prefix_original_rows_by_cutoff': prefix_counts,
                         'supported_fields_by_cutoff': support_counts}
    arrays.update(pair_names=np.asarray(pairs), feature_names=np.asarray(names), fold_cutoffs_epoch=np.asarray(cuts, dtype=np.int64))
    verify_sources(manifest_path, manifest_sha, records)
    verify_bindings(manifest)
    return manifest, manifest_sha, arrays, records


def verify_sources(manifest_path, manifest_sha, records):
    checked(manifest_path, manifest_sha)
    for record in records.values():
        checked(record['path'], record['sha256'], record['bytes'])


def export(inputs, output):
    inputs, output = Path(inputs).resolve(), Path(output).resolve()
    manifest_path = inputs / 'SPECIALIST_INPUTS.json' if inputs.is_dir() else inputs
    require(output.is_relative_to((ROOT/'docs/validation').resolve()) and not output.exists(),
            'new_docs_validation_destination_required')
    exporter_sha = sha(__file__)
    manifest, manifest_sha, arrays, source_records = collect(manifest_path)
    # All validation above occurs before creating an evidence directory.
    output.mkdir(parents=True)
    try:
        artifact = output/'NORMALIZERS.npz'
        with artifact.open('xb') as handle:
            np.savez_compressed(handle, **arrays)
        array_records = {name: array_record(value) for name, value in arrays.items()}
        with np.load(artifact, allow_pickle=False) as restored:
            require(set(restored.files) == set(arrays), 'exact_seven_array_readback_schema')
            for name, record in array_records.items():
                require(array_record(restored[name]) == record, 'exact_array_dtype_shape_and_bits_readback:' + name)
        verify_sources(manifest_path, manifest_sha, source_records)
        verify_bindings(manifest)
        require(sha(__file__) == exporter_sha, 'exporter_source_changed')
        result = {'schema': SCHEMA, 'status': 'complete', 'created_utc': datetime.now(timezone.utc).isoformat(),
            'source_manifest_path': str(manifest_path), 'source_manifest_sha256': manifest_sha,
            'source_manifest_schema': manifest['schema'], 'source_bindings': manifest['source_bindings'],
            'exporter': {'path': str(Path(__file__).resolve()), 'sha256': exporter_sha},
            'base_manifest_sha256': manifest['base_sha256'], 'endpoint_manifest_sha256': manifest['endpoint_manifest_sha256'],
            'prepared_manifest_sha256': manifest['prepared_sha256'], 'quote_manifest_sha256': manifest['quote_sha256'],
            'boundaries': manifest['boundaries'], 'groups': manifest['groups'],
            'pair_names': arrays['pair_names'].tolist(), 'feature_names': arrays['feature_names'].tolist(),
            'fold_cutoffs_epoch': arrays['fold_cutoffs_epoch'].tolist(), 'fit_cutoff_dates_utc': manifest['fit_cutoff_dates_utc'],
            'folds': manifest['folds'], 'final_normalizer_index': manifest['final_normalizer_index'],
            'matrix_axes': ['fold_cutoffs_epoch', 'pair_names', 'feature_names'],
            'normalizer_scope': manifest['normalizer_scope'], 'normalizer_contract': manifest['normalizer_contract'],
            'arrays': array_records, 'source_pair_files': source_records,
            'supported_pair_feature_cells_by_cutoff': arrays['supported'].sum(axis=(1, 2)).tolist(),
            'artifact': {'path': artifact.name, 'bytes': artifact.stat().st_size, 'sha256': sha(artifact)},
            'array_origin': 'exact copied existing arrays; no feature rows, targets or recomputed statistics',
            'readback': {'all_68_by5_original_normalizer_sets_exact_bytes': True,
                         'all_saved_arrays_exact_dtype_shape_bytes': True,
                         'all_source_pair_hashes_verified_before_and_after': True,
                         'allow_pickle': False, 'manifest_and_source_bindings_unchanged': True,
                         'all_seven_arrays_exact': True, 'source_pair_artifacts_verified': 68,
                         'source_normalizer_sets_verified': 340, 'dtype_shape_and_c_order_payload_bits': True},
            'raw_x_or_feature_rows_or_labels_included': False, 'models_fit': 0,
            'inference_sequence': [
                'Select the saved head-model fold index and recorded sorted pair index; final models use normalizer index4.',
                'Compute original causal compact50 values in recorded feature order and apply matching mean/scale/support; transform to float32.',
                'Select compact38 or compact50 and append the two original-known asymmetric entry costs and categorical pair ID.',
                'For a family experiment apply its recorded fixed53-slot mask; otherwise preserve every selected technical field.',
                'Meta models retain their own OOF pooled scalers, arms and column order. Calibrated probabilities belong only to the separate calibrated-mixture arm.'],
            'runtime': {'numpy': np.__version__, 'python': sys.version.split()[0]}}
        with (output/'NORMALIZERS.json').open('x', encoding='utf-8') as handle:
            json.dump(result, handle, sort_keys=True, indent=2, allow_nan=False)
            handle.write('\n')
        require(json.loads((output/'NORMALIZERS.json').read_text()) == result, 'registry_json_readback')
        return result
    except BaseException as exc:
        with (output/'EXPORT_FAILED.json').open('x', encoding='utf-8') as handle:
            json.dump({'status': 'failed', 'error': type(exc).__name__ + ':' + str(exc),
                       'source_manifest_sha256': manifest_sha}, handle, sort_keys=True, indent=2)
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inputs', type=Path, required=True, help='Completed input directory or SPECIALIST_INPUTS.json')
    parser.add_argument('--output', type=Path, required=True, help='New evidence directory under docs/validation')
    args = parser.parse_args()
    result = export(args.inputs, args.output)
    print(json.dumps({'status': result['status'], 'pairs': len(result['pair_names']),
                      'artifact': result['artifact'], 'source_manifest_sha256': result['source_manifest_sha256']}))
