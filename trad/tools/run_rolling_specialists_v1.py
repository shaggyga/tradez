"""Chronological OOF specialists and fixed combining models; offline research.

This extends the accepted rolling comparison. Neither legacy full-TRAIN
normalizers nor in-sample model outputs supply early OOF training predictions.
Every assessment origin is retained before any future scoring mask is applied.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gc
import hashlib
import importlib.metadata
import json
from pathlib import Path
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import joblib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from threadpoolctl import threadpool_limits

from oanda_rolling_model_design_v1 import transform_inputs
from oanda_rolling_technical_dataset_v1 import file_sha, key_hash
from oanda_rolling_technical_endpoint_labels_v1 import checked_path
from tools.run_rolling_model_comparison_v1 import load_inputs, pair_prior, save_forecast
import oanda_rolling_specialists_v1 as specialist
from oanda_rolling_specialist_scoring_v1 import score_specialist_variant, component_diagnostics

SCHEMA = 'rolling_specialist_comparison_v1_20260915'
HORIZONS = (30, 60)
GROUPS = ('compact38', 'compact50')
META_ARMS = ('direct_ridge', 'specialist_ridge', 'direct_context_hgb', 'specialist_context_hgb')
HEAD_NAMES = ('probability', 'direct', 'positive', 'nonpositive', 'long_exit', 'short_exit')
CONTEXT_NAMES = ('m1__return_15_bps', 'm1__return_60_bps', 'm1__path_efficiency_15',
                 'm1__return_vol_15_pips', 'm1__return_vol_60_pips', 'm1__spread_ratio_prior_120',
                 'm1__utc_hour_sin', 'm1__utc_hour_cos')
CHUNK = 8192
SUBDIRS = ('models', 'oof', 'head_forecasts', 'forecasts', 'metrics', 'diagnostics')


def save(path, obj):
    Path(path).write_text(json.dumps(obj, indent=2, sort_keys=True, allow_nan=False)+'\n', encoding='utf-8')


def assert_pins(pins):
    for name, digest in pins.items():
        if file_sha(ROOT/name) != digest:
            raise ValueError('bound_source_changed:'+name)


def merge_source_bindings(inherited, new_names):
    """Never replace an inherited digest with the current working-tree digest."""
    pins = {}
    for mapping in inherited:
        for name, expected in mapping.items():
            if name in pins and pins[name] != expected:
                raise ValueError('inherited_binding_conflict:'+name)
            pins[name] = expected
    assert_pins(pins)
    for name in new_names:
        digest = file_sha(ROOT/name)
        if name in pins and pins[name] != digest:
            raise ValueError('inherited_binding_conflict:'+name)
        pins.setdefault(name, digest)
    return pins


def storage_check(output):
    size = sum(p.stat().st_size for sub in SUBDIRS for p in (output/sub).iterdir())
    if size > 4*1024**3 or shutil.disk_usage(output).free < 32*1024**3:
        raise ValueError('specialist_storage_budget_or_reserve')
    return size


def load_specialist_inputs(path):
    path = Path(path).resolve()
    manifest_path = path/'SPECIALIST_INPUTS.json'
    m = json.loads(manifest_path.read_bytes())
    if m['status'] != 'complete' or len(m['pairs']) != 68:
        raise ValueError('complete_all68_specialist_inputs_required')
    prepared, quotes = Path(m['prepared_root']), Path(m['quote_root'])
    if file_sha(prepared/'PREPARED.json') != m['prepared_sha256'] or file_sha(quotes/'QUOTE_PANEL.json') != m['quote_sha256']:
        raise ValueError('specialist_predecessor_input_identity')
    data, pm, qm = load_inputs(prepared, quotes)
    if pm['base_sha256'] != m['base_sha256'] or set(pm['pairs']) != set(m['pairs']):
        raise ValueError('same_accepted_raw_population_required')
    names = list(m['feature_names'])
    if names != pm['groups']['compact50'] or m['groups']['compact38'] != pm['groups']['compact38']:
        raise ValueError('fixed_compact_feature_order_required')
    total = len(data['time'])
    del data['x']
    data['raw_x'] = np.empty((total, 50), dtype=np.float64)
    for name in ('entry_long', 'entry_short'):
        data[name] = np.empty(total, dtype=np.float64)
    data['normalizers'] = {name: np.empty((5, 68, 50), dtype=(bool if name == 'supported' else np.int64 if name == 'count' else np.float64))
                           for name in ('count', 'mean', 'scale', 'supported')}
    offset = 0
    for pid, pair in enumerate(data['pair_names']):
        rec = m['pairs'][pair]
        src = checked_path(path, rec['path'], rec['sha256'])
        with np.load(src, allow_pickle=False) as z:
            n = len(z['time']); sl = slice(offset, offset+n)
            if (n != rec['rows'] or key_hash(z['time']) != rec['key_sha256'] or
                    not np.array_equal(z['time'], data['time'][sl]) or
                    not np.array_equal(z['split'], data['split'][sl]) or
                    not np.all(data['pair_id'][sl] == pid)):
                raise ValueError('specialist_exact_original_pair_clock_join')
            if z['raw_x'].shape != (n, 50) or z['raw_x'].dtype != np.float64:
                raise ValueError('original_float64_registered_inputs_required')
            data['raw_x'][sl] = z['raw_x']
            data['entry_long'][sl] = z['known_entry_long_bps']
            data['entry_short'][sl] = z['known_entry_short_bps']
            for name in data['normalizers']:
                value = z['normalizer_'+name]
                if value.shape != (5, 50):
                    raise ValueError('five_prefix_normalizer_sets_required')
                data['normalizers'][name][:, pid, :] = value
        offset += n
    if offset != total or not np.isfinite(data['entry_long']+data['entry_short']).all() or np.any(data['entry_long'] < 0) or np.any(data['entry_short'] < 0):
        raise ValueError('complete_finite_current_entry_costs_required')
    if not np.allclose(data['entry_long']+data['entry_short'], data['spread'], rtol=1e-12, atol=1e-10):
        raise ValueError('asymmetric_entry_costs_must_match_actual_spread')
    data['feature_names'] = names
    data['fold_cutoffs'] = np.asarray(m['fold_cutoffs_epoch'], dtype=np.int64)
    if data['fold_cutoffs'].shape != (5,) or np.any(np.diff(data['fold_cutoffs']) != 7*86400):
        raise ValueError('four_ordered_weekly_OOF_blocks_required')
    if data['fold_cutoffs'][-1] != pm['boundaries']['train_end']:
        raise ValueError('final_normalizer_must_end_at_TRAIN_boundary')
    return data, m, pm, qm


def normalized_matrix(data, rows, fold):
    rows = np.asarray(rows, dtype=np.int64)
    out = np.full((len(rows), len(data['feature_names'])), np.nan, dtype=np.float32)
    ids = data['pair_id'][rows]
    for pid in np.unique(ids):
        mask = ids == pid
        params = {name: a[fold, pid] for name, a in data['normalizers'].items()}
        out[mask] = transform_inputs(data['raw_x'][rows[mask]], params)
    return out


def base_matrix(data, rows, group, fold):
    z = normalized_matrix(data, rows, fold)
    width = 38 if group == 'compact38' else 50 if group == 'compact50' else 0
    if not width:
        raise ValueError('undeclared_specialist_feature_group')
    # Known costs are separately declared inputs; future exit costs are labels.
    return np.column_stack((z[:, :width], data['entry_long'][rows], data['entry_short'][rows], data['pair_id'][rows]))


def fit_mask(data, h, cutoff):
    return ((data['split'] == 0) & data[f'valid_{h}'] &
            (data['time']+(h+1)*60 < cutoff))


def oof_population(data):
    t = data['time']; cuts = data['fold_cutoffs']
    rows = np.flatnonzero((data['split'] == 0) & (t >= cuts[0]) & (t < cuts[-1]))
    fold = np.searchsorted(cuts, t[rows], side='right')-1
    if np.any(fold < 0) or np.any(fold >= 4):
        raise ValueError('OOF_rows_outside_declared_blocks')
    return rows, fold.astype(np.int8)


def oof_fit_mask(data, rows, fold, h):
    ends = data['fold_cutoffs'][np.asarray(fold)+1]
    return data[f'valid_{h}'][rows] & (data['time'][rows]+(h+1)*60 < ends)


def exit_labels(data, rows, h):
    y = data[f'y_{h}'][rows]
    left = y-data['entry_long'][rows]-data[f'long_{h}'][rows]
    right = -y-data['entry_short'][rows]-data[f'short_{h}'][rows]
    if not np.isfinite(np.column_stack((y, left, right))).all() or np.any(left < -1e-7) or np.any(right < -1e-7):
        raise ValueError('finite_nonnegative_asymmetric_exit_labels_required')
    return np.maximum(left, 0.), np.maximum(right, 0.)


def selection_record(data, rows, h):
    keys = np.column_stack((data['pair_id'][rows], data['time'][rows])).astype('<i8')
    e1, e2 = exit_labels(data, rows, h)
    targets = np.column_stack((data[f'y_{h}'][rows], e1, e2)).astype('<f8')
    return {'rows': len(rows), 'pair_clock_sha256': hashlib.sha256(keys.tobytes()).hexdigest(),
            'targets_y_exit_long_exit_short_sha256': hashlib.sha256(targets.tobytes()).hexdigest(),
            'maximum_target_end_epoch': int(np.max(data['time'][rows]+(h+1)*60)),
            'counts_by_pair': {p: int(np.sum(data['pair_id'][rows] == i)) for i, p in enumerate(data['pair_names'])}}


def predict_heads_chunks(bundle, data, rows, group, fold):
    heads = {name: np.empty(len(rows), dtype=np.float64) for name in HEAD_NAMES}
    clips = {}
    for start in range(0, len(rows), CHUNK):
        stop = min(start+CHUNK, len(rows))
        values, clipped = specialist.predict_heads(bundle, base_matrix(data, rows[start:stop], group, fold))
        if set(values) != set(HEAD_NAMES):
            raise ValueError('exact_six_head_output_contract')
        for name in HEAD_NAMES:
            heads[name][start:stop] = values[name]
        for name, count in clipped.items():
            clips[name] = clips.get(name, 0)+int(count)
    if not all(np.isfinite(v).all() for v in heads.values()):
        raise ValueError('every_issued_specialist_prediction_must_be_finite')
    return heads, clips


def check_head_replay(path, expected, data, rows, group, fold):
    restored = joblib.load(path)['bundle']
    first = min(CHUNK, len(rows))
    replay, _ = specialist.predict_heads(restored, base_matrix(data, rows[:first], group, fold))
    if any(not np.array_equal(replay[n], expected[n][:first]) for n in HEAD_NAMES):
        raise ValueError('six_head_first_chunk_replay_failed')
    reps = np.array([np.flatnonzero(data['pair_id'][rows] == pid)[0] for pid in np.unique(data['pair_id'][rows])])
    replay, _ = specialist.predict_heads(restored, base_matrix(data, rows[reps], group, fold))
    if any(not np.allclose(replay[n], expected[n][reps], rtol=1e-12, atol=1e-10) for n in HEAD_NAMES):
        raise ValueError('six_head_all_pair_replay_failed')
    return {'first_chunk_exact_each_head': first, 'pair_representatives_each_head': len(reps), 'rtol': 1e-12, 'atol_bps': 1e-10}


def save_npz(path, payload):
    np.savez_compressed(path, **payload)
    with np.load(path, allow_pickle=False) as got:
        if set(got.files) != set(payload) or any(not np.array_equal(got[n], v, equal_nan=True) for n, v in payload.items()):
            raise ValueError('OOF_full_array_readback_failed')


def check_recorded_artifacts(output, value):
    if isinstance(value, dict):
        if 'path' in value and 'sha256' in value:
            checked_path(output, value['path'], value['sha256'])
        for child in value.values():
            check_recorded_artifacts(output, child)
    elif isinstance(value, list):
        for child in value:
            check_recorded_artifacts(output, child)


def probability_assessment(data, rows, h, prior, raw, calibrated, prior_positive):
    """Separate raw/calibrated positive-event scores; no calibration fitting."""
    common = np.isfinite(data[f'arima_{h}'][rows]) & np.isfinite(data[f'momentum_{h}'][rows]) & np.isfinite(prior)
    reports = {}
    for sid, split in ((1, 'validation'), (2, 'later_development_test')):
        eligible = (data['split'][rows] == sid) & common & data[f'valid_{h}'][rows] & data[f'eligible_{h}'][rows]
        cohorts = {'full_endpoint': eligible, 'shared_strict': eligible & data[f'strict_{h}'][rows],
                   'additional_endpoint_only': eligible & ~data[f'strict_{h}'][rows]}
        reports[split] = {}
        for cohort, mask in cohorts.items():
            y = (data[f'y_{h}'][rows][mask] > 0).astype(np.float64)
            scores = {}
            for name, values in (('raw', raw), ('calibrated', calibrated), ('TRAIN_pair_prior', prior_positive)):
                p = np.asarray(values)[mask]
                if not np.isfinite(p).all() or np.any((p < 0) | (p > 1)):
                    raise ValueError('probability_assessment_valid_probabilities_required')
                clipped = np.clip(p, 1e-12, 1-1e-12)
                scores[name] = {'rows': len(y), 'positive_rows': int(y.sum()),
                                'brier': float(np.mean((p-y)**2)) if len(y) else None,
                                'log_loss': float(np.mean(-(y*np.log(clipped)+(1-y)*np.log1p(-clipped)))) if len(y) else None}
            reports[split][cohort] = scores
    return {'positive_event': 'midpoint return >0; flats remain nonpositive', 'log_loss_probability_clip': 1e-12,
            'scope': 'same common valid mature rows for raw, chronological-OOF-calibrated and TRAIN prior; no later fitting', 'periods': reports}


def save_heads(path, data, rows, heads):
    table = pa.table({'pair_id': data['pair_id'][rows], 'bar_start_epoch': data['time'][rows], 'split': data['split'][rows], **heads})
    pq.write_table(table, path, compression='zstd', use_dictionary=['pair_id', 'split'], row_group_size=CHUNK)
    actual = pq.ParquetFile(path).read()
    if not table.equals(actual):
        raise ValueError('head_forecast_complete_readback_failed')
    for n, v in heads.items():
        if not np.array_equal(actual[n].to_numpy().view(np.uint64), v.view(np.uint64)):
            raise ValueError('head_forecast_finite_bit_readback_failed')


def predict_meta_chunks(bundle, meta, ids):
    values = np.empty(len(meta), dtype=np.float64)
    for start in range(0, len(meta), CHUNK):
        values[start:start+CHUNK] = specialist.predict_meta(bundle, meta[start:start+CHUNK], ids[start:start+CHUNK])
    if not np.isfinite(values).all():
        raise ValueError('finite_complete_meta_forecasts_required')
    return values


def run(args):
    inputs, comparison, output = map(lambda p: Path(p).resolve(), (args.inputs, args.comparison, args.output))
    if output.exists() or not output.is_relative_to(ROOT/'data'):
        raise ValueError('new_isolated_project_data_output_required')
    if shutil.disk_usage(output.parent).free < 36*1024**3:
        raise ValueError('four_GiB_budget_above_drive_reserve_required')
    im_path = inputs/'SPECIALIST_INPUTS.json'; im = json.loads(im_path.read_bytes())
    old_path = comparison/'RESULTS.json'; old = json.loads(old_path.read_bytes())
    if (old['status'] != 'complete' or old['prepared_sha256'] != im['prepared_sha256'] or
            old['quote_sha256'] != im['quote_sha256'] or old['assessment_rows'] != 911254):
        raise ValueError('exact_completed_comparison_required')
    pins = merge_source_bindings((im['source_bindings'], old['source_bindings']),
                                ('oanda_rolling_specialists_v1.py', 'oanda_rolling_specialist_scoring_v1.py',
                                 'tools/run_rolling_specialists_v1.py', '../direction_decision_20260911/src/signed_cost_models_v1.py'))
    assert_pins(pins)
    output.mkdir()
    for sub in SUBDIRS:
        (output/sub).mkdir()
    report = {'schema': SCHEMA, 'status': 'running', 'started_utc': datetime.now(timezone.utc).isoformat(),
              'inputs_root': str(inputs), 'inputs_sha256': file_sha(im_path), 'previous_comparison_root': str(comparison),
              'previous_comparison_sha256': file_sha(old_path), 'source_bindings': pins, 'horizons': list(HORIZONS),
              'groups': {g: im['groups'][g] for g in GROUPS}, 'meta_arms': list(META_ARMS),
              'added_current_quote_inputs': ['known_entry_long_bps', 'known_entry_short_bps'],
              'meta_context_names': list(CONTEXT_NAMES), 'fold_cutoffs_epoch': im['fold_cutoffs_epoch'],
              'training_selection': 'original UTC15m TRAIN samples; target END strictly before prefix fit cutoff; equal sampled-row weights',
              'meta_training': 'all issued TRAIN OOF inputs for unsupervised scaler; fitting targets additionally valid and END strictly before respective OOF block end',
              'calibration': 'raw chronological OOF probabilities only; separate calibrated-mixture arm, never calibrated same-row inputs to meta fitting',
              'primary_policy': 'unchanged current actual spread+1bp gate and fixed per-pair holding reservations',
              'secondary_policy': 'separately reported estimated side-specific entry+exit cost gate, same fixed1bp margin',
              'contexts': {}, 'pair_priors': {}, 'completed_base_bundles': 0, 'completed_variants': 0,
              'can_place_orders': False, 'models_promoted': 0, 'untouched_confirmation': False,
              'limits': ['Previously examined dates and follow-up horizons chosen from prior development evidence.',
                         'Endpoint proxies and dependent currency exposures are not account returns or fills.',
                         'The old full-TRAIN normalizers and saved models are controls only, never early OOF inputs.',
                         'New direct specialist includes two known quote costs beyond the registered compact fields.',
                         'Four OOF training weeks do not establish independent confirmation.'],
              'versions': {n: importlib.metadata.version(n) for n in ('numpy', 'scipy', 'scikit-learn', 'joblib', 'pyarrow')}}
    save(output/'RESULTS.json', report)
    began = time.monotonic()
    try:
        with threadpool_limits(limits=4):
            data, loaded, pm, qm = load_specialist_inputs(inputs)
            if loaded != im:
                raise ValueError('specialist_input_manifest_changed_during_load')
            assessment = np.flatnonzero(data['split'] != 0)
            oof_rows, oof_fold = oof_population(data)
            report.update(assessment_rows=len(assessment), oof_issued_origins=len(oof_rows), pair_names=data['pair_names'])
            context_cols = [data['feature_names'].index(n) for n in CONTEXT_NAMES]
            oof_context = np.empty((len(oof_rows), 8), dtype=np.float32)
            for fold in range(4):
                pos = np.flatnonzero(oof_fold == fold)
                oof_context[pos] = normalized_matrix(data, oof_rows[pos], fold)[:, context_cols]
            final_context = normalized_matrix(data, assessment, 4)[:, context_cols]
            for h in HORIZONS:
                prior, prior_params = pair_prior(data, h, assessment)
                prior_path = output/'models'/f'pair_prior_{h}m.json'; save(prior_path, prior_params)
                report['pair_priors'][str(h)] = {'path': prior_path.relative_to(output).as_posix(), 'sha256': file_sha(prior_path)}
                prior_positive = np.array([prior_params[p]['p_up'] for p in data['pair_names']])[data['pair_id'][assessment]]
                for group in GROUPS:
                    tag = f'{group}_{h}m'
                    ctx = {'horizon_minutes': h, 'group': group, 'oof_models': [], 'variants': {}, 'meta_models': {}}
                    report['contexts'][tag] = ctx
                    oof_heads = {n: np.full(len(oof_rows), np.nan) for n in HEAD_NAMES}
                    for fold in range(5):
                        rows = np.flatnonzero(fit_mask(data, h, int(data['fold_cutoffs'][fold])))
                        left, right = exit_labels(data, rows, h)
                        train_record = selection_record(data, rows, h)
                        start = time.monotonic()
                        bundle = specialist.fit_heads(base_matrix(data, rows, group, fold), data[f'y_{h}'][rows], left, right, pair_count=68)
                        role = f'oof{fold}' if fold < 4 else 'final'
                        model_path = output/'models'/f'{tag}_{role}.joblib'
                        joblib.dump({'bundle': bundle, 'group': group, 'fold_index': fold, 'fit_cutoff_epoch': int(data['fold_cutoffs'][fold]),
                                     'training_selection': train_record, 'inputs_sha256': report['inputs_sha256'], 'horizon_minutes': h}, model_path, compress=3)
                        predicted_rows = oof_rows[oof_fold == fold] if fold < 4 else assessment
                        head_values, clips = predict_heads_chunks(bundle, data, predicted_rows, group, fold)
                        replay = check_head_replay(model_path, head_values, data, predicted_rows, group, fold)
                        rec = {'path': model_path.relative_to(output).as_posix(), 'sha256': file_sha(model_path),
                               'training': train_record, 'prediction_rows': len(predicted_rows), 'clips': clips, 'replay': replay,
                               'elapsed_seconds': round(time.monotonic()-start, 3)}
                        if fold < 4:
                            ctx['oof_models'].append(rec)
                            for n in HEAD_NAMES:
                                oof_heads[n][oof_fold == fold] = head_values[n]
                        else:
                            ctx['final_model'] = rec
                            final_heads = head_values
                        report['completed_base_bundles'] += 1
                        assert_pins(pins); storage_check(output); save(output/'RESULTS.json', report)
                        print(json.dumps({'base_bundle': f'{tag}_{role}', 'rows': len(rows), 'elapsed_seconds': round(time.monotonic()-began, 1)}), flush=True)
                        del bundle; gc.collect()
                    if not all(np.isfinite(v).all() for v in oof_heads.values()):
                        raise ValueError('every_original_OOF_origin_requires_six_predictions')
                    meta_mask = oof_fit_mask(data, oof_rows, oof_fold, h)
                    meta = specialist.meta_features(oof_heads, oof_context, data['entry_long'][oof_rows], data['entry_short'][oof_rows])
                    final_meta = specialist.meta_features(final_heads, final_context, data['entry_long'][assessment], data['entry_short'][assessment])
                    oof_path = output/'oof'/f'{tag}.npz'
                    save_npz(oof_path, {'pair_id': data['pair_id'][oof_rows], 'time': data['time'][oof_rows], 'fold': oof_fold,
                                       'meta_fit_mask': meta_mask, 'meta_x': meta, **oof_heads})
                    ctx['oof'] = {'path': oof_path.relative_to(output).as_posix(), 'sha256': file_sha(oof_path),
                                  'issued_rows': len(oof_rows), 'meta_fit_selection': selection_record(data, oof_rows[meta_mask], h)}
                    calibration = specialist.fit_calibration(oof_heads['probability'][meta_mask], data[f'y_{h}'][oof_rows][meta_mask])
                    cal_path = output/'models'/f'{tag}_calibration.json'; save(cal_path, calibration)
                    if json.loads(cal_path.read_bytes()) != calibration:
                        raise ValueError('calibration_parameter_readback_failed')
                    ctx['calibration'] = {'path': cal_path.relative_to(output).as_posix(), 'sha256': file_sha(cal_path)}
                    cal_p = specialist.apply_calibration(final_heads['probability'], json.loads(cal_path.read_bytes()))
                    calibration_metrics_path = output/'diagnostics'/f'{tag}_probability_calibration.json'
                    save(calibration_metrics_path, probability_assessment(data, assessment, h, prior, final_heads['probability'], cal_p, prior_positive))
                    ctx['probability_calibration_scores'] = {'path': calibration_metrics_path.relative_to(output).as_posix(), 'sha256': file_sha(calibration_metrics_path)}
                    head_path = output/'head_forecasts'/f'{tag}.parquet'; save_heads(head_path, data, assessment, final_heads)
                    ctx['head_forecasts'] = {'path': head_path.relative_to(output).as_posix(), 'sha256': file_sha(head_path), 'rows': len(assessment)}
                    p = final_heads['probability']
                    variants = {'direct': final_heads['direct'],
                                'mixture_raw': p*final_heads['positive']-(1-p)*final_heads['nonpositive'],
                                'mixture_calibrated': specialist.calibrated_mixture(final_heads, calibration)}
                    for arm in META_ARMS:
                        model = specialist.fit_meta(meta, data['pair_id'][oof_rows], data[f'y_{h}'][oof_rows], arm=arm, fit_mask=meta_mask, pair_count=68)
                        model_path = output/'models'/f'{tag}_{arm}.joblib'
                        joblib.dump({'bundle': model, 'inputs_sha256': report['inputs_sha256'], 'oof_sha256': ctx['oof']['sha256'],
                                     'training_selection': ctx['oof']['meta_fit_selection'], 'horizon_minutes': h, 'group': group}, model_path, compress=3)
                        pred = predict_meta_chunks(model, final_meta, data['pair_id'][assessment])
                        restored = joblib.load(model_path)['bundle']
                        replay = specialist.predict_meta(restored, final_meta[:CHUNK], data['pair_id'][assessment[:CHUNK]])
                        if not np.array_equal(pred[:len(replay)], replay):
                            raise ValueError('meta_first_chunk_recreation_failed')
                        representatives = np.array([np.flatnonzero((data['pair_id'][assessment] == i) & (data['split'][assessment] == 2))[0] for i in range(68)])
                        replay = specialist.predict_meta(restored, final_meta[representatives], data['pair_id'][assessment[representatives]])
                        if not np.allclose(pred[representatives], replay, rtol=1e-12, atol=1e-10):
                            raise ValueError('meta_all68_later_recreation_failed')
                        variants[arm] = pred
                        ctx['meta_models'][arm] = {'path': model_path.relative_to(output).as_posix(), 'sha256': file_sha(model_path),
                                                  'first_chunk_exact': CHUNK, 'later_pair_representatives': 68}
                        del model, restored
                    for name, pred in variants.items():
                        vtag = f'{tag}_{name}'
                        forecast = save_forecast(output, vtag, data, assessment, pred)
                        e1, e2 = data['entry_long'][assessment], data['entry_short'][assessment]
                        metrics = score_specialist_variant(data, assessment, pred, h, prior, e1, e2, final_heads['long_exit'], final_heads['short_exit'])
                        metric_path = output/'metrics'/f'{vtag}.json'; save(metric_path, metrics)
                        diagnostic = component_diagnostics(data, assessment, pred, h, prior, e1, e2, final_heads)
                        diagnostic['head_probability_scope'] = 'Raw six-head probability diagnostics in every mean variant; separate context probability_calibration_scores records raw/calibrated/prior comparison.'
                        diagnostic_path = output/'diagnostics'/f'{vtag}.json'; save(diagnostic_path, diagnostic)
                        ctx['variants'][name] = {'forecast': forecast, 'metrics': {'path': metric_path.relative_to(output).as_posix(), 'sha256': file_sha(metric_path)},
                                                 'diagnostics': {'path': diagnostic_path.relative_to(output).as_posix(), 'sha256': file_sha(diagnostic_path)}}
                        report['completed_variants'] += 1
                        assert_pins(pins); storage_check(output); save(output/'RESULTS.json', report)
                        print(json.dumps({'scored_variant': vtag, 'completed_variants': report['completed_variants'], 'elapsed_seconds': round(time.monotonic()-began, 1)}), flush=True)
                    del final_heads, variants, meta, final_meta, oof_heads
                    gc.collect()
            assert_pins(pins)
            if file_sha(im_path) != report['inputs_sha256'] or file_sha(old_path) != report['previous_comparison_sha256']:
                raise ValueError('accepted_input_or_comparison_changed')
            check_recorded_artifacts(output, report['contexts'])
            check_recorded_artifacts(output, report['pair_priors'])
            report['artifacts'] = {p.relative_to(output).as_posix(): {'sha256': file_sha(p), 'bytes': p.stat().st_size}
                                   for sub in SUBDIRS for p in sorted((output/sub).iterdir())}
            report.update(status='complete', completed_utc=datetime.now(timezone.utc).isoformat(),
                          output_bytes=storage_check(output), elapsed_seconds=round(time.monotonic()-began, 3))
            save(output/'RESULTS.json', report)
    except BaseException as exc:
        report.update(status='failed', failure={'type': type(exc).__name__, 'message': str(exc)})
        save(output/'RESULTS.json', report)
        raise
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inputs', type=Path, required=True)
    parser.add_argument('--comparison', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    result = run(parser.parse_args())
    print(json.dumps({k: result[k] for k in ('status', 'completed_base_bundles', 'completed_variants', 'elapsed_seconds')}))
