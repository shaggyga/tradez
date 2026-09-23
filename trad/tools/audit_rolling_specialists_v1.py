"""Independent COMPLETE-artifact audit for chronological rolling specialists.

No fitting, calibration fitting, candle reread, feature regeneration, live
change, or original-artifact write. Model replay is deliberately bounded.
"""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import joblib
import numpy as np
import pyarrow.parquet as pq
from scipy.special import expit
from threadpoolctl import threadpool_limits

from tools import audit_rolling_model_comparison_v1 as previous_audit
from tools.prepare_rolling_specialists_v1 import exact_arrays
from oanda_rolling_model_design_v1 import fit_normalizer
import oanda_rolling_specialists_v1 as contract

require = previous_audit.require
sha = previous_audit.sha
checked = previous_audit.checked
read_json = previous_audit.read_json
HORIZONS = (30, 60)
HEADS = ('probability', 'direct', 'positive', 'nonpositive', 'long_exit', 'short_exit')
GROUPS = ('compact38', 'compact50')
META_ARMS = ('direct_ridge', 'specialist_ridge', 'direct_context_hgb', 'specialist_context_hgb')
VARIANTS = ('direct', 'mixture_raw', 'mixture_calibrated') + META_ARMS
SUBDIRS = ('models', 'oof', 'head_forecasts', 'forecasts', 'metrics', 'diagnostics')
LEGACY_RELATIVE = '../direction_decision_20260911/src/signed_cost_models_v1.py'


def check_sources(bindings):
    for name, digest in bindings.items():
        path = (ROOT / name).resolve()
        require(path.is_relative_to(ROOT) or name == LEGACY_RELATIVE, 'declared_source_scope')
        require(sha(path) == digest, 'bound_source_digest:' + name)


def check_references(root, value, inventory):
    count = 0
    if isinstance(value, dict):
        if 'path' in value and 'sha256' in value:
            path = value['path']
            require(path in inventory and inventory[path]['sha256'] == value['sha256'], 'reference_inventory_identity:' + path)
            checked(root, path, value['sha256'])
            count += 1
        count += sum(check_references(root, child, inventory) for child in value.values())
    elif isinstance(value, list):
        count += sum(check_references(root, child, inventory) for child in value)
    return count


def fold_population(times, split, cutoffs):
    rows = np.flatnonzero((split == 0) & (times >= cutoffs[0]) & (times < cutoffs[-1]))
    fold = np.searchsorted(cutoffs, times[rows], side='right') - 1
    require(np.isin(fold, (0, 1, 2, 3)).all(), 'four_oof_blocks_only')
    return rows, fold.astype(np.int8)


def mature_rows(data, h, cutoff):
    return np.flatnonzero((data['split'] == 0) & data[f'valid_{h}'] & (data['time'] + (h + 1) * 60 < cutoff))


def independent_normalize(raw, params):
    raw = np.asarray(raw, dtype=np.float64)
    supported = np.asarray(params['supported'], dtype=bool)
    result = np.full(raw.shape, np.nan)
    with np.errstate(over='ignore', invalid='ignore', divide='ignore'):
        result[:, supported] = (raw[:, supported] - params['mean'][supported]) / params['scale'][supported]
    require(np.all(np.isfinite(result)[np.isfinite(raw) & supported[None, :]]), 'finite_supported_normalized_cells')
    result[~np.isfinite(raw)] = np.nan
    with np.errstate(over='ignore'):
        compact = result.astype(np.float32)
    require(np.array_equal(np.isfinite(compact), np.isfinite(result)), 'normalized_float32_overflow')
    return compact


def normalized_rows(data, rows, fold):
    ids = data['pair_id'][rows]
    result = np.full((len(rows), 50), np.nan, dtype=np.float32)
    for pid in np.unique(ids):
        mask = ids == pid
        params = {name: value[fold, pid] for name, value in data['normalizers'].items()}
        result[mask] = independent_normalize(data['raw_x'][rows[mask]], params)
    return result


def base_rows(data, rows, group, fold):
    width = 38 if group == 'compact38' else 50
    z = normalized_rows(data, rows, fold)[:, :width]
    return np.column_stack((z, data['entry_long'][rows], data['entry_short'][rows], data['pair_id'][rows])).astype(np.float32)


def selection(data, rows, h):
    y = data[f'y_{h}'][rows]
    left = y - data['entry_long'][rows] - data[f'long_{h}'][rows]
    right = -y - data['entry_short'][rows] - data[f'short_{h}'][rows]
    require(np.isfinite(np.column_stack((y, left, right))).all() and np.all(left >= -1e-7) and np.all(right >= -1e-7), 'valid_asymmetric_training_targets')
    values = np.column_stack((y, np.maximum(left, 0.), np.maximum(right, 0.))).astype('<f8')
    keys = np.column_stack((data['pair_id'][rows], data['time'][rows])).astype('<i8')
    return {'rows': len(rows), 'pair_clock_sha256': hashlib.sha256(keys.tobytes()).hexdigest(),
            'targets_y_exit_long_exit_short_sha256': hashlib.sha256(values.tobytes()).hexdigest(),
            'maximum_target_end_epoch': int((data['time'][rows] + (h + 1) * 60).max()),
            'counts_by_pair': {name: int((data['pair_id'][rows] == i).sum()) for i, name in enumerate(data['pair_names'])}}


def load_full_inputs(root, metadata, pm):
    n = metadata['rows']
    data = {'raw_x': np.empty((n, 50), dtype=np.float64), 'time': np.empty(n, dtype=np.int64),
            'split': np.empty(n, dtype=np.int8), 'pair_id': np.empty(n, dtype=np.int16),
            'entry_long': np.empty(n), 'entry_short': np.empty(n), 'pair_names': sorted(metadata['pairs']),
            'feature_names': metadata['feature_names'], 'cutoffs': np.asarray(metadata['fold_cutoffs_epoch'], dtype=np.int64)}
    data['normalizers'] = {name: np.empty((5, 68, 50), dtype=bool if name == 'supported' else np.int64 if name == 'count' else float)
                           for name in ('count', 'mean', 'scale', 'supported')}
    for h in HORIZONS:
        for stem in ('y', 'long', 'short', 'valid'):
            data[f'{stem}_{h}'] = np.empty(n, dtype=bool if stem == 'valid' else float)
    offset = 0
    for pid, pair in enumerate(data['pair_names']):
        record = metadata['pairs'][pair]
        source = checked(root, record['path'], record['sha256'])
        old = pm['pairs'][pair]
        old_source = checked(metadata['prepared_root'], old['path'], old['sha256'])
        with np.load(source, allow_pickle=False) as a, np.load(old_source, allow_pickle=False) as b:
            size = len(a['time'])
            require(size == record['rows'] == old['rows'] and exact_arrays(a['time'], b['time']) and exact_arrays(a['split'], b['split']), 'exact_raw_prepared_pair_keys')
            require(hashlib.sha256(a['time'].astype('<i8').tobytes()).hexdigest() == record['key_sha256'], 'raw_retained_clock_digest')
            require(a['raw_x'].shape == (size, 50) and a['raw_x'].dtype == np.float64, 'raw50_float64')
            sl = slice(offset, offset + size)
            for name in ('raw_x', 'time', 'split'):
                data[name][sl] = a[name]
            data['pair_id'][sl] = pid
            for name in ('long', 'short'):
                data['entry_' + name][sl] = a['known_entry_' + name + '_bps']
            for name in data['normalizers']:
                require(a['normalizer_' + name].shape == (5, 50), 'five_prefix_normalizers')
                data['normalizers'][name][:, pid] = a['normalizer_' + name]
            for h in HORIZONS:
                for stem in ('y', 'long', 'short', 'valid'):
                    data[f'{stem}_{h}'][sl] = b[f'{stem}_{h}']
        offset += size
    require(offset == n and np.all(data['time'][data['split'] == 0] % 900 == 0), 'all_retained_rows_and_original_train_clock')
    require(np.all(np.diff(data['cutoffs']) == 7 * 86400) and data['cutoffs'][-1] == metadata['boundaries']['train_end'], 'declared_weekly_prefixes')
    return data


def raw_meta(heads, context, entry_long, entry_short):
    require(set(heads) == set(HEADS), 'six_raw_heads')
    p = heads['probability']
    return np.column_stack([*(heads[name] for name in HEADS),
                            p * heads['positive'] - (1 - p) * heads['nonpositive'],
                            p * heads['positive'] + (1 - p) * heads['nonpositive'],
                            entry_long, entry_short, context])


def replay_heads(bundle, design):
    require(set(bundle['models']) == set(HEADS), 'saved_six_model_identity')
    model = bundle['models']['probability']
    require(np.array_equal(model.classes_, [0, 1]), 'positive_probability_class_identity')
    result = {'probability': model.predict_proba(design)[:, 1], 'direct': bundle['models']['direct'].predict(design)}
    for name in HEADS[2:]:
        result[name] = np.maximum(bundle['models'][name].predict(design), 0.)
    return result


def replay_meta(bundle, raw, ids):
    columns = list(contract.META_COLUMNS[bundle['arm']])
    require(bundle['columns'] == columns and bundle['selected_meta_names'] == [contract.META_NAMES[i] for i in columns], 'saved_meta_column_identity')
    selected = raw[:, columns]
    if bundle['arm'].endswith('_ridge'):
        z = independent_normalize(selected, bundle['scaler'])
        missing = ~np.isfinite(z)
        onehot = np.eye(bundle['pair_count'], dtype=np.float32)[ids]
        design = np.column_stack((np.where(missing, 0., z), missing.astype(np.float32), onehot)).astype(np.float64)
    else:
        design = np.column_stack((selected, ids))
    return bundle['model'].predict(design)


def apply_platt_independently(raw_probability, calibration):
    p = np.clip(raw_probability, calibration['clip'], 1 - calibration['clip'])
    return expit(calibration['slope'] * np.log(p / (1 - p)) + calibration['intercept'])


def verify_probability_scores(data, h, raw, calibrated, prior_positive, record):
    for sid, split in ((1, 'validation'), (2, 'later_development_test')):
        valid = ((data['split'] == sid) & np.isfinite(data[f'arima_{h}']) & np.isfinite(data[f'momentum_{h}'])
                 & np.isfinite(data[f'prior_{h}']) & data[f'valid_{h}'] & data[f'eligible_{h}'])
        for name, mask in (('full_endpoint', valid), ('shared_strict', valid & data[f'strict_{h}']),
                           ('additional_endpoint_only', valid & ~data[f'strict_{h}'])):
            actual = (data[f'y_{h}'][mask] > 0).astype(float)
            for kind, prediction in (('raw', raw), ('calibrated', calibrated), ('TRAIN_pair_prior', prior_positive)):
                score = record['periods'][split][name][kind]
                require(score['rows'] == len(actual) and score['positive_rows'] == int(actual.sum()), 'probability_same_row_counts')
                p = prediction[mask]
                clipped = np.clip(p, 1e-12, 1 - 1e-12)
                previous_audit.close_number(score['brier'], float(np.mean((p - actual)**2)) if len(p) else None, 'probability_brier')
                loss = -(actual * np.log(clipped) + (1 - actual) * np.log1p(-clipped))
                previous_audit.close_number(score['log_loss'], float(np.mean(loss)) if len(p) else None, 'probability_log_loss')


def audit_gate_scores(data, prediction, h, metrics, entry_long, entry_short, heads):
    count = previous_audit.audit_scores(data, prediction, h, metrics['primary_current_spread'])
    changed = dict(data)
    changed['spread'] = np.where(prediction > 0, entry_long + heads['long_exit'], entry_short + heads['short_exit'])
    secondary = copy.deepcopy(metrics['secondary_expected_cost'])
    for value in secondary.values():
        for scope in ('primary', 'one_minute_entry_delay'):
            policies = value[scope]['policies']
            policies['origin_spread_threshold'] = policies.pop('expected_cost_threshold')
            policies['origin_spread_threshold_nonoverlap'] = policies.pop('expected_cost_threshold_nonoverlap')
    return count + previous_audit.audit_scores(changed, prediction, h, secondary)


def read_forecast(root, record, expected):
    table = pq.ParquetFile(checked(root, record['path'], record['sha256'])).read()
    require(table.num_rows == len(expected['time']), 'every_original_forecast_row')
    if 'rows' in record:
        require(record['rows'] == table.num_rows, 'forecast_record_row_count')
    for name, source in (('pair_id', 'pair_id'), ('bar_start_epoch', 'time'), ('split', 'split')):
        require(np.array_equal(table[name].to_numpy(), expected[source]), 'original_forecast_pair_clock_split')
    return table


def run(root, output):
    root, output = Path(root).resolve(), Path(output).resolve()
    require(root.is_relative_to(ROOT / 'data') and output.is_relative_to(ROOT / 'docs' / 'validation') and not output.exists(), 'bounded_new_audit_output')
    result_path = root / 'RESULTS.json'
    result_hash = sha(result_path)
    result = read_json(result_path)
    require(result['status'] == 'complete' and result['completed_base_bundles'] == 20 and result['completed_variants'] == 28, 'complete_20_bundle_28_variant_result_only')
    require(set(result['contexts']) == {f'{group}_{h}m' for group in GROUPS for h in HORIZONS}, 'complete_four_contexts')
    check_sources(result['source_bindings'])
    inventory = result['artifacts']
    actual_files = {p.relative_to(root).as_posix() for sub in SUBDIRS for p in (root / sub).iterdir()}
    require(set(inventory) == actual_files, 'complete_artifact_inventory')
    for name, item in inventory.items():
        path = checked(root, name, item['sha256'])
        require(path.stat().st_size == item['bytes'], 'artifact_size_identity')
    references = check_references(root, result['contexts'], inventory) + check_references(root, result['pair_priors'], inventory)
    inputs = Path(result['inputs_root'])
    require(sha(inputs / 'SPECIALIST_INPUTS.json') == result['inputs_sha256'], 'raw_fold_input_binding')
    im = read_json(inputs / 'SPECIALIST_INPUTS.json')
    require(im['status'] == 'complete' and len(im['pairs']) == 68, 'complete_68_fold_inputs')
    prepared, quotes = Path(im['prepared_root']), Path(im['quote_root'])
    require(sha(prepared / 'PREPARED.json') == im['prepared_sha256'] and sha(quotes / 'QUOTE_PANEL.json') == im['quote_sha256'], 'bound_original_prepared_quote_manifests')
    pm, qm = read_json(prepared / 'PREPARED.json'), read_json(quotes / 'QUOTE_PANEL.json')
    require(pm['base_sha256'] == qm['base_manifest_sha256'] == im['base_sha256'] and pm['overlay_sha256'] == qm['endpoint_manifest_sha256'] == im['endpoint_manifest_sha256'], 'base_endpoint_generation_identity')
    old_path = Path(result['previous_comparison_root']) / 'RESULTS.json'
    require(sha(old_path) == result['previous_comparison_sha256'], 'previous_comparison_unchanged')
    endpoint_root = Path(pm['overlay'])
    require(sha(endpoint_root / 'ENDPOINT_DATASET.json') == pm['overlay_sha256'], 'old_endpoint_manifest_unchanged')
    endpoint = read_json(endpoint_root / 'ENDPOINT_DATASET.json')
    for item in endpoint['pairs'].values():
        checked(endpoint_root, item['receipt_path'], item['receipt_sha256'])
    started = time.monotonic()
    assessment_data, _, sampled_count = previous_audit.load_expected(pm, qm, prepared, quotes)
    data = load_full_inputs(inputs, im, pm)
    require(data['pair_names'] == result['pair_names'] and result['groups'] == {name: im['groups'][name] for name in GROUPS}, 'declared_pair_and_group_mapping')
    require(result['meta_context_names'] == list(contract.CONTEXT_NAMES), 'declared_meta_context_mapping')
    assessment = np.flatnonzero(data['split'] != 0)
    require(len(assessment) == 911254 and sampled_count == 183540, 'accepted_assessment_and_train_sizes')
    for name in ('pair_id', 'time', 'split'):
        require(np.array_equal(data[name][assessment], assessment_data[name]), 'exact_assessment_from_retained_input_keys')
    require(np.allclose(data['entry_long'][assessment] + data['entry_short'][assessment], assessment_data['spread'], rtol=1e-12, atol=1e-10), 'known_entry_cost_actual_spread_identity')
    oof, fold_ids = fold_population(data['time'], data['split'], data['cutoffs'])
    require(len(oof) == 122418 and np.array_equal(data['cutoffs'], result['fold_cutoffs_epoch']), 'all122418_unchanged_oof_origins')
    context_columns = [data['feature_names'].index(name) for name in contract.CONTEXT_NAMES]
    oof_context = np.full((len(oof), 8), np.nan, dtype=np.float32)
    for fold in range(4):
        selected = fold_ids == fold
        oof_context[selected] = normalized_rows(data, oof[selected], fold)[:, context_columns]
    final_context = normalized_rows(data, assessment, 4)[:, context_columns]
    report = {'status': 'running', 'result_root': str(root), 'results_sha256': result_hash,
              'inputs_sha256': result['inputs_sha256'], 'audit_source_sha256': sha(__file__),
              'audit_helper_sha256': sha(previous_audit.__file__), 'started_utc': datetime.now(timezone.utc).isoformat(),
              'artifact_inventory_files': len(inventory), 'stored_artifact_references_verified': references,
              'assessment_origins': len(assessment), 'oof_origins': len(oof), 'sampled_train_origins': sampled_count,
              'endpoint_receipts_verified': len(endpoint['pairs']), 'contexts': {}, 'scalar_cost_cases': 0,
              'limits': ['No model or calibration refit; saved parameter application is replayed on bounded rows.',
                         'No original candles or full feature regeneration repeated; accepted source-bound artifacts are used.',
                         'Every forecast key and gate decision digest is checked; every concentration ranking and every diagnostic field is not independently recomputed.',
                         'Follow-up development research on previously examined dates; computational consistency is not profitability.']}
    with threadpool_limits(limits=4):
        for tag, ctx in result['contexts'].items():
            h, group = ctx['horizon_minutes'], ctx['group']
            require(tag == f'{group}_{h}m' and group in GROUPS and h in HORIZONS, 'context_identity')
            require(len(ctx['oof_models']) == 4 and set(ctx['meta_models']) == set(META_ARMS) and set(ctx['variants']) == set(VARIANTS), 'all_context_models_and_variants')
            with np.load(checked(root, ctx['oof']['path'], ctx['oof']['sha256']), allow_pickle=False) as a:
                for name, expected in (('pair_id', data['pair_id'][oof]), ('time', data['time'][oof]), ('fold', fold_ids)):
                    require(exact_arrays(a[name], expected), 'every_original_oof_key_and_fold')
                meta_mask = data[f'valid_{h}'][oof] & (data['time'][oof] + (h + 1) * 60 < data['cutoffs'][fold_ids + 1])
                require(exact_arrays(a['meta_fit_mask'], meta_mask), 'strict_per_oof_block_meta_purge')
                oof_heads = {name: a[name].copy() for name in HEADS}
                require(all(np.isfinite(value).all() for value in oof_heads.values()), 'complete_six_head_oof_issuance')
                meta = raw_meta(oof_heads, oof_context, data['entry_long'][oof], data['entry_short'][oof])
                require(exact_arrays(a['meta_x'], meta), 'exact_uncalibrated_raw_head_meta_reconstruction')
            meta_record = selection(data, oof[meta_mask], h)
            require(ctx['oof']['meta_fit_selection'] == meta_record, 'actual_meta_training_selection')
            head_table = read_forecast(root, ctx['head_forecasts'], assessment_data)
            final_heads = {name: head_table[name].to_numpy() for name in HEADS}
            require(all(np.isfinite(value).all() for value in final_heads.values()), 'every_assessment_has_six_head_values')
            final_meta = raw_meta(final_heads, final_context, data['entry_long'][assessment], data['entry_short'][assessment])
            context_record = {'meta_training': meta_record, 'base_bundles': [], 'meta_bundles': {}, 'variants': {}}
            for fold, record in enumerate([*ctx['oof_models'], ctx['final_model']]):
                fit = mature_rows(data, h, int(data['cutoffs'][fold]))
                expected_selection = selection(data, fit, h)
                require(record['training'] == expected_selection and expected_selection['maximum_target_end_epoch'] < data['cutoffs'][fold], 'fold_training_strict_maturity_and_hashes')
                saved = joblib.load(checked(root, record['path'], record['sha256']))
                require(saved['training_selection'] == expected_selection and saved['fold_index'] == fold and saved['fit_cutoff_epoch'] == data['cutoffs'][fold], 'saved_fold_identity_and_training')
                require(saved['inputs_sha256'] == result['inputs_sha256'] and saved['group'] == group and saved['horizon_minutes'] == h, 'saved_head_input_group_horizon_binding')
                bundle = saved['bundle']
                y = data[f'y_{h}'][fit]
                require(bundle['training_rows'] == len(fit) and bundle['positive_rows'] == int((y > 0).sum()) and bundle['nonpositive_rows'] == int((y <= 0).sum()) and bundle['flat_rows'] == int((y == 0).sum()), 'actual_conditional_head_populations')
                width = 41 if group == 'compact38' else 53
                for name, model in bundle['models'].items():
                    require(model.is_categorical_.tolist() == [False] * (width - 1) + [True], 'every_saved_head_categorical_pair_mask')
                    for param, value in contract.BASE_RECIPE.items():
                        require(model.get_params()[param] == value, 'fixed_saved_head_recipe')
                predicted_rows = oof[fold_ids == fold] if fold < 4 else assessment
                expected_heads = {name: value[fold_ids == fold] for name, value in oof_heads.items()} if fold < 4 else final_heads
                require(len(predicted_rows) == record['prediction_rows'], 'all_fold_issuance_count')
                first_count = min(8192, len(predicted_rows))
                replay = replay_heads(bundle, base_rows(data, predicted_rows[:first_count], group, fold))
                for name in HEADS:
                    require(previous_audit.finite_bits_equal(replay[name], expected_heads[name][:first_count]), 'bounded_head_first_chunk_exact_bits')
                representatives = np.asarray([np.flatnonzero(data['pair_id'][predicted_rows] == pid)[0] for pid in range(68)])
                replay = replay_heads(bundle, base_rows(data, predicted_rows[representatives], group, fold))
                for name in HEADS:
                    require(np.allclose(replay[name], expected_heads[name][representatives], rtol=1e-12, atol=1e-10), 'all68_head_representatives_replay')
                context_record['base_bundles'].append({'fold': fold, 'training': expected_selection, 'prediction_rows': len(predicted_rows), 'first_chunk_exact_each_head': first_count, 'pair_representatives_each_head': 68})
            calibration = read_json(checked(root, ctx['calibration']['path'], ctx['calibration']['sha256']))
            targets = data[f'y_{h}'][oof][meta_mask]
            require(calibration['rows'] == len(targets) and calibration['positive_rows'] == int((targets > 0).sum()) and calibration['flat_rows'] == int((targets == 0).sum()), 'calibration_uses_only_mature_oof_target_population')
            require(calibration['configuration'] == contract.CALIBRATION_RECIPE and 0 <= calibration['slope'] <= 8 and -8 <= calibration['intercept'] <= 8, 'fixed_bounded_calibration_recipe')
            calibrated_probability = apply_platt_independently(final_heads['probability'], calibration)
            expected_variants = {'direct': final_heads['direct'], 'mixture_raw': final_meta[:, 6],
                                 'mixture_calibrated': calibrated_probability * final_heads['positive'] - (1 - calibrated_probability) * final_heads['nonpositive']}
            prior_record = read_json(checked(root, result['pair_priors'][str(h)]['path'], result['pair_priors'][str(h)]['sha256']))
            require(set(prior_record) == set(data['pair_names']), 'all68_saved_pair_priors')
            prior_fit = mature_rows(data, h, data['cutoffs'][-1])
            for pid, pair in enumerate(data['pair_names']):
                y = data[f'y_{h}'][prior_fit[data['pair_id'][prior_fit] == pid]]
                expected_prior = {'status': 'available' if len(y) >= 20 else 'insufficient_training_labels',
                                  'training_rows': len(y), 'mean_bps': float(np.mean(y)) if len(y) else None,
                                  'p_up': float(np.mean(y > 0)) if len(y) else None,
                                  'p_down': float(np.mean(y < 0)) if len(y) else None,
                                  'p_flat': float(np.mean(y == 0)) if len(y) else None}
                require(prior_record[pair] == expected_prior, 'independent_saved_prior_parameters')
            prior_positive = np.asarray([prior_record[pair]['p_up'] if prior_record[pair]['status'] == 'available' else np.nan for pair in data['pair_names']])[assessment_data['pair_id']]
            probability_scores = read_json(checked(root, ctx['probability_calibration_scores']['path'], ctx['probability_calibration_scores']['sha256']))
            verify_probability_scores(assessment_data, h, final_heads['probability'], calibrated_probability, prior_positive, probability_scores)
            for name, entry in ctx['variants'].items():
                table = read_forecast(root, entry['forecast'], assessment_data)
                prediction = table['predicted_bps'].to_numpy()
                require(np.isfinite(prediction).all(), 'all_original_variant_forecasts_finite')
                if name in expected_variants:
                    require(previous_audit.finite_bits_equal(prediction, expected_variants[name]), 'all_row_direct_or_mixture_recreation')
                else:
                    saved = joblib.load(checked(root, ctx['meta_models'][name]['path'], ctx['meta_models'][name]['sha256']))
                    require(saved['training_selection'] == meta_record and saved['oof_sha256'] == ctx['oof']['sha256'] and saved['inputs_sha256'] == result['inputs_sha256'], 'saved_meta_training_source_binding')
                    bundle = saved['bundle']
                    require(bundle['arm'] == name and not bundle['probability_calibration_in_inputs'] and bundle['issued_oof_rows'] == len(oof) and bundle['training_rows'] == int(meta_mask.sum()), 'raw_oof_only_meta_training_metadata')
                    if name.endswith('_ridge'):
                        expected_scaler = fit_normalizer(meta[:, contract.META_COLUMNS[name]])
                        require(bundle['scaler_rows'] == len(oof) and all(exact_arrays(value, bundle['scaler'][key]) for key, value in expected_scaler.items()), 'meta_scaler_all_issued_oof_before_target_filter')
                    else:
                        width = len(contract.META_COLUMNS[name])
                        require(bundle['model'].is_categorical_.tolist() == [False] * width + [True], 'saved_meta_categorical_pair_field')
                    recipe = contract.META_RIDGE_RECIPE if name.endswith('_ridge') else contract.META_HGB_RECIPE
                    require(all(bundle['model'].get_params()[key] == value for key, value in recipe.items()), 'fixed_saved_meta_recipe')
                    first = np.arange(min(8192, len(assessment)))
                    replay = replay_meta(bundle, final_meta[first], data['pair_id'][assessment[first]])
                    require(previous_audit.finite_bits_equal(replay, prediction[first]), 'meta_first_chunk_exact_bits')
                    reps = np.asarray([np.flatnonzero((assessment_data['pair_id'] == pid) & (assessment_data['split'] == 2))[0] for pid in range(68)])
                    replay = replay_meta(bundle, final_meta[reps], data['pair_id'][assessment[reps]])
                    require(np.allclose(replay, prediction[reps], rtol=1e-12, atol=1e-10), 'all68_later_meta_representatives_replay')
                    context_record['meta_bundles'][name] = {'first_chunk_exact': len(first), 'later_pair_representatives': 68, 'meta_training_rows': int(meta_mask.sum())}
                metrics = read_json(checked(root, entry['metrics']['path'], entry['metrics']['sha256']))
                cases = audit_gate_scores(assessment_data, prediction, h, metrics, data['entry_long'][assessment], data['entry_short'][assessment], final_heads)
                report['scalar_cost_cases'] += cases
                context_record['variants'][name] = {'rows': len(prediction), 'keys_exact': True, 'both_gate_decisions_and_cohort_costs_verified': True, 'scalar_cost_cases': cases}
            report['contexts'][tag] = context_record
            print(json.dumps({'audited_context': tag, 'completed_contexts': len(report['contexts']), 'elapsed_seconds': round(time.monotonic() - started, 1)}), flush=True)
    require(sha(result_path) == result_hash and sha(inputs / 'SPECIALIST_INPUTS.json') == result['inputs_sha256'] and sha(old_path) == result['previous_comparison_sha256'], 'source_manifests_unchanged_after_audit')
    require(sha(endpoint_root / 'ENDPOINT_DATASET.json') == pm['overlay_sha256'], 'endpoint_unchanged_after_audit')
    for item in endpoint['pairs'].values():
        checked(endpoint_root, item['receipt_path'], item['receipt_sha256'])
    check_sources(result['source_bindings'])
    report.update(status='passed', completed_utc=datetime.now(timezone.utc).isoformat(), elapsed_seconds=round(time.monotonic() - started, 3),
                  six_head_bundles_replayed=20, individual_head_estimators_replayed=120, meta_models_replayed=16,
                  calibration_applications_verified=4, forecast_files_verified=28, head_forecast_files_verified=4,
                  total_assessment_key_rows_verified=911254 * 32, oof_context_rows_verified=122418 * 4,
                  original_artifact_writes=False, refits_performed=0)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('x', encoding='utf-8') as stream:
        json.dump(report, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write('\n')
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--comparison', type=Path, required=True)
    parser.add_argument('--output', type=Path, default=ROOT / 'docs/validation/rolling_specialists_20260915/INDEPENDENT_AUDIT.json')
    args = parser.parse_args()
    result = run(args.comparison, args.output)
    print(json.dumps({key: result[key] for key in ('status', 'six_head_bundles_replayed', 'meta_models_replayed', 'elapsed_seconds')}))
