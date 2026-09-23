"""Bounded development fit/assessment inputs; no models or result selection."""
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
from oanda_rolling_technical_endpoint_labels_v1 import iter_endpoint_partitions
from oanda_rolling_technical_dataset_v1 import file_sha, key_hash
from oanda_rolling_model_design_v1 import (
    SCHEMA, HORIZONS, TRAIN_SAMPLE_SECONDS, RECIPES,
    feature_groups, fit_normalizer, transform_inputs,
)


def save(path, obj):
    path.write_text(json.dumps(obj, indent=2, sort_keys=True, allow_nan=False)+'\n', encoding='utf-8')


def run(args):
    base, overlay, output = (Path(p).resolve() for p in (args.base, args.overlay, args.output))
    manifest_path = base/'DATASET.json'
    manifest = json.loads(manifest_path.read_bytes())
    overlay_path = overlay/'ENDPOINT_DATASET.json'
    supplemental = json.loads(overlay_path.read_bytes())
    if manifest['status'] != 'complete' or supplemental['status'] != 'complete' or len(manifest['pairs']) != 68:
        raise ValueError('completed_base_and_overlay_required')
    if file_sha(manifest_path) != supplemental['base_manifest_sha256']:
        raise ValueError('exact_overlay_base_required')
    names = manifest['feature_names']; groups = feature_groups(names)
    if output.exists() or not output.is_relative_to(ROOT/'data'):
        raise ValueError('new_project_data_output_required')
    if shutil.disk_usage(output.parent).free < 34*1024**3:
        raise ValueError('two_GiB_budget_above_32_GiB_reserve_required')
    pins = dict(supplemental['source_bindings'])
    for n in ['oanda_rolling_technical_dataset_v1.py', 'oanda_rolling_model_design_v1.py',
              'tools/prepare_rolling_model_comparison_v1.py']:
        current = file_sha(ROOT/n)
        if n in pins and pins[n] != current:
            raise ValueError('inherited_source_binding_mismatch')
        pins.setdefault(n, current)
    if any(file_sha(ROOT/n) != h for n,h in pins.items()):
        raise ValueError('source_binding_mismatch')
    output.mkdir(); (output/'pairs').mkdir()
    began = time.monotonic()
    report = {'schema': SCHEMA, 'status': 'building', 'base': str(base), 'overlay': str(overlay),
        'base_sha256': file_sha(manifest_path), 'overlay_sha256': file_sha(overlay_path),
        'source_bindings': pins, 'feature_names': names, 'groups': groups, 'recipes': RECIPES,
        'boundaries': manifest['boundaries'], 'pairs': {}, 'rows': 0, 'bytes': 0,
        'training_sample': 'all real TRAIN origins with original UTC start modulo900==0; no feature or outcome filtering',
        'assessment_sample': 'every original validation and later-development-test origin',
        'normalizer_scope': 'per-pair full TRAIN inputs only; minimum20 finite and positive variance; unsupported pair/field becomes NaN in every split',
        'normalizer_dtype': 'float64 mean/variance; standardized values rounded to float32 for learning; base float64 unchanged',
        'label_use': 'training selection separately requires endpoint-valid and target END strictly before TRAIN end',
        'models_fit': 0, 'can_place_orders': False, 'untouched_confirmation': False,
        'started_utc': datetime.now(timezone.utc).isoformat()}
    save(output/'PREPARED.json', report)
    buffered = []; previous_pair = None

    def write_pair(pair, tables):
        x = np.concatenate([np.column_stack([t[n].to_numpy() for n in names]) for t in tables])
        times = np.concatenate([t['bar_start_epoch'].to_numpy() for t in tables])
        splits = np.concatenate([t['origin_split'].to_numpy() for t in tables])
        train = splits == 'train'
        if len(times) != manifest['pairs'][pair]['origin_rows'] or np.any(times[1:] <= times[:-1]):
            raise ValueError('exact_ordered_pair_population_required')
        params = fit_normalizer(x[train])
        keep = ~train | (times % TRAIN_SAMPLE_SECONDS == 0)
        payload = {'time': times[keep], 'split': np.where(train,0,np.where(splits=='validation',1,2))[keep].astype(np.int8),
                   'x': transform_inputs(x[keep], params)}
        for h in HORIZONS:
            def col(prefix, field):
                return np.concatenate([t[f'{prefix}__{h}m__{field}'].to_numpy() for t in tables])[keep]
            for k,f in [('y','return_bps'),('long','long_net_bps'),('short','short_net_bps')]:
                payload[f'{k}_{h}'] = col('endpoint_label',f)
            payload[f'valid_{h}'] = col('endpoint_label','midpoint_valid') & col('endpoint_label','bidask_endpoint_valid')
            payload[f'strict_{h}'] = col('label','midpoint_valid') & col('label','bidask_endpoint_valid')
            payload[f'eligible_{h}'] = col('endpoint_label','split_eligible')
        for n,a in params.items():
            payload['normalizer_'+n] = a
        reserve = sum(a.nbytes for a in payload.values())*2+1024**2
        if report['bytes']+reserve > 2*1024**3 or shutil.disk_usage(output).free-reserve < 32*1024**3:
            raise ValueError('bounded_preparation_storage_exceeded')
        path = output/'pairs'/(pair+'.npz')
        np.savez_compressed(path, **payload)
        with np.load(path, allow_pickle=False) as loaded:
            if set(loaded.files) != set(payload) or any(not np.array_equal(loaded[n],a,equal_nan=True) for n,a in payload.items()):
                raise ValueError('prepared_full_array_readback_failed')
        result = {'path': path.relative_to(output).as_posix(), 'sha256': file_sha(path), 'bytes': path.stat().st_size,
            'rows': int(keep.sum()), 'training_full_origin_rows': int(train.sum()),
            'training_clock_rows': int((keep&train).sum()),
            'validation_rows': int((splits=='validation').sum()), 'later_development_test_rows': int((splits=='later_development_test').sum()),
            'key_sha256': key_hash(times[keep]), 'supported_pair_features': int(params['supported'].sum()),
            'readback': 'all NPZ arrays exact including float32 values and NaN masks'}
        report['pairs'][pair] = result; report['rows'] += result['rows']; report['bytes'] += result['bytes']
        save(output/'PREPARED.json', report)
        print(json.dumps({'pair':pair,'rows':result['rows'],'elapsed_seconds':round(time.monotonic()-began,1)}),flush=True)

    try:
        for pair, table in iter_endpoint_partitions(base, overlay, feature_names=names):
            if previous_pair is not None and pair != previous_pair:
                write_pair(previous_pair, buffered); buffered = []
            previous_pair = pair; buffered.append(table)
        if buffered:
            write_pair(previous_pair, buffered)
        if set(report['pairs']) != set(manifest['pairs']):
            raise ValueError('complete_all_pair_preparation_required')
        if file_sha(manifest_path) != report['base_sha256'] or file_sha(overlay_path) != report['overlay_sha256']:
            raise ValueError('dataset_metadata_changed')
        if any(file_sha(ROOT/n) != h for n,h in pins.items()):
            raise ValueError('bound_source_changed')
        report.update(status='complete', completed_utc=datetime.now(timezone.utc).isoformat(), elapsed_seconds=round(time.monotonic()-began,3))
        save(output/'PREPARED.json', report)
    except BaseException as exc:
        report.update(status='failed', failure={'type':type(exc).__name__,'message':str(exc)})
        save(output/'PREPARED.json',report)
        raise
    return report


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base', type=Path, required=True)
    p.add_argument('--overlay', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    r = run(p.parse_args())
    print(json.dumps({k:r[k] for k in ('status','rows','bytes','elapsed_seconds')}))
