"""Independent bounded readback of a COMPLETE rolling model comparison.

No refit, raw-candle reread, live changes, source changes, or full-model replay.
The audit deliberately does not import the comparison runner or scorer.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time

import joblib
import numpy as np
import pyarrow.parquet as pq
from threadpoolctl import threadpool_limits

ROOT = Path(__file__).resolve().parents[1]
HORIZONS = (5, 15, 30, 60)
CONTROL_NAMES = ('no_change', 'pair_train_mean', 'arima110_conditional_ols',
                 'momentum_exact_past_horizon', 'reversal_exact_past_horizon')


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def checked(root, relative, expected):
    root = Path(root).resolve()
    path = (root / relative).resolve()
    require(path.is_relative_to(root) and path.is_file(), 'contained_artifact_required:' + relative)
    require(sha(path) == expected, 'artifact_sha256_mismatch:' + relative)
    return path


def read_json(path):
    return json.loads(Path(path).read_bytes())


def finite_bits_equal(left, right):
    left, right = np.asarray(left, dtype=np.float64), np.asarray(right, dtype=np.float64)
    valid = np.isfinite(left)
    return (left.shape == right.shape and np.array_equal(valid, np.isfinite(right))
            and np.array_equal(np.isnan(left), np.isnan(right))
            and np.array_equal(left[valid].view(np.uint64), right[valid].view(np.uint64)))


def independent_decisions(clocks, pairs, prediction, spread, horizon):
    """Origin-only reservations, including origins lacking later outcomes."""
    issued = np.isfinite(prediction)
    signs = np.zeros(len(prediction), dtype=np.int8)
    signs[issued] = np.sign(prediction[issued]).astype(np.int8)
    sign = issued & (signs != 0)
    threshold = issued & np.isfinite(spread) & (spread >= 0) & (np.abs(prediction) > spread + 1.)
    accepted = np.zeros(len(prediction), dtype=bool)
    next_available = {}
    for i in np.flatnonzero(threshold):
        pair = int(pairs[i])
        if int(clocks[i]) >= next_available.get(pair, -1):
            accepted[i] = True
            next_available[pair] = int(clocks[i]) + horizon * 60
    return signs, {'all_issued_sign_diagnostic': sign,
                   'origin_spread_threshold': threshold,
                   'origin_spread_threshold_nonoverlap': accepted}


def decision_sha(mask, clocks, pair_ids, signs, pair_names):
    # Inputs have already been verified as sorted by pair then clock.
    records = np.empty(int(mask.sum()), dtype=[('p', 'S7'), ('t', '<i8'), ('s', 'i1')])
    records['p'] = np.asarray(pair_names, dtype='S7')[pair_ids[mask]]
    records['t'] = clocks[mask]
    records['s'] = signs[mask]
    return hashlib.sha256(records.tobytes()).hexdigest()


def learner_matrix(z, pair_ids, learner, pair_count):
    """Independent reconstruction from the saved standardized float32 inputs."""
    if learner == 'ridge':
        missing = ~np.isfinite(z)
        indicators = np.eye(pair_count, dtype=np.float32)[pair_ids]
        return np.concatenate((np.where(missing, np.float32(0), z),
                               missing.astype(np.float32), indicators), axis=1).astype(np.float64)
    require(learner == 'hgb', 'known_learner_required')
    return np.concatenate((z, pair_ids[:, None].astype(np.float32)), axis=1)


def close_number(observed, expected, label):
    if expected is None:
        require(observed is None, 'expected_null:' + label)
    else:
        require(observed is not None and np.isclose(observed, expected, rtol=1e-11, atol=1e-10),
                'numeric_metric_mismatch:' + label)


def load_expected(pm, qm, prepared, quotes):
    """Read each prepared/quote pair once; retain only small replay feature slices."""
    pair_names = sorted(pm['pairs'])
    chunks = {name: [] for name in ('time', 'split', 'pair_id', 'spread')}
    for h in HORIZONS:
        for stem in ('y', 'long', 'short', 'valid', 'strict', 'eligible', 'prior', 'arima',
                     'momentum', 'delay_long', 'delay_short', 'delay_valid'):
            chunks[f'{stem}_{h}'] = []
    key_digests = {h: hashlib.sha256() for h in HORIZONS}
    y_digests = {h: hashlib.sha256() for h in HORIZONS}
    train_counts = {h: {} for h in HORIZONS}
    replay_indices, replay_values, representatives = [], [], []
    offset = 0
    training_clock_rows = 0
    for pair_id, pair in enumerate(pair_names):
        pr, qr = pm['pairs'][pair], qm['pairs'][pair]
        pp = checked(prepared, pr['path'], pr['sha256'])
        qp = checked(quotes, qr['path'], qr['sha256'])
        checked(quotes, qr['receipt_path'], qr['receipt_sha256'])
        columns = ['bar_start_epoch', 'origin_split', 'quote__entry_spread_bps', 'quote__mid_close']
        columns += [f'forecast__arima110_conditional_ols__{h}m_bps' for h in HORIZONS]
        columns += [f'delayed_label__{h}m__{field}' for h in HORIZONS for field in ('long_net_bps', 'short_net_bps', 'valid')]
        qt = pq.ParquetFile(qp).read(columns=columns)
        clocks = qt['bar_start_epoch'].to_numpy()
        require(len(clocks) == qr['rows'] and np.all(clocks[1:] > clocks[:-1]), 'quote_clock_count_order')
        require(hashlib.sha256(clocks.astype('<i8').tobytes()).hexdigest() == qr['key_sha256'], 'quote_clock_digest')
        with np.load(pp, allow_pickle=False) as a:
            t, split = a['time'], a['split']
            require(len(t) == pr['rows'] and np.all(t[1:] > t[:-1]), 'prepared_clock_count_order')
            require(hashlib.sha256(t.astype('<i8').tobytes()).hexdigest() == pr['key_sha256'], 'prepared_clock_digest')
            require(np.isin(split, (0, 1, 2)).all(), 'three_known_splits_only')
            train = split == 0
            require(np.all(t[train] % 900 == 0), 'original_15_minute_training_clock_required')
            training_clock_rows += int(train.sum())
            expected_split = np.where(t < pm['boundaries']['train_end'], 0,
                                      np.where(t < pm['boundaries']['validation_end'], 1, 2))
            require(np.array_equal(split, expected_split), 'time_derived_split_identity')
            loc = np.searchsorted(clocks, t)
            require(np.all(loc < len(clocks)) and np.array_equal(clocks[loc], t), 'exact_quote_join')
            quote_splits = qt['origin_split'].to_numpy()[loc]
            require(np.array_equal(split, np.where(quote_splits == 'train', 0, np.where(quote_splits == 'validation', 1, 2))),
                    'quote_split_identity')
            later = split != 0
            later_t, later_split, later_loc = t[later], split[later], loc[later]
            n = len(later_t)
            require(n == pr['validation_rows'] + pr['later_development_test_rows'], 'all_later_pair_rows')
            chunks['time'].append(later_t)
            chunks['split'].append(later_split)
            chunks['pair_id'].append(np.full(n, pair_id, dtype=np.int16))
            chunks['spread'].append(qt['quote__entry_spread_bps'].to_numpy()[later_loc])
            representative = int(np.flatnonzero(later_split == 2)[0])
            representatives.append(offset + representative)
            sample = np.unique(np.r_[np.flatnonzero(offset + np.arange(n) < 8192), representative])
            z = a['x']
            require(z.dtype == np.float32 and z.shape == (len(t), len(pm['feature_names'])), 'float32_registered_matrix')
            unsupported = ~a['normalizer_supported']
            require(np.isnan(z[:, unsupported]).all(), 'unsupported_fields_remain_missing')
            replay_indices.append(offset + sample)
            replay_values.append(z[later][sample])
            mid = qt['quote__mid_close'].to_numpy()
            for h in HORIZONS:
                eligible_expected = t + (h + 1) * 60 < np.where(split == 0, pm['boundaries']['train_end'],
                    np.where(split == 1, pm['boundaries']['validation_end'], pm['boundaries']['end']))
                require(np.array_equal(a[f'eligible_{h}'], eligible_expected), 'independent_split_purge_rule')
                fit = train & a[f'valid_{h}'] & a[f'eligible_{h}']
                y = a[f'y_{h}'][fit]
                require(np.isfinite(y).all(), 'finite_mature_training_labels')
                key = np.column_stack((np.full(len(y), pair_id, dtype=np.int64), t[fit])).astype('<i8')
                key_digests[h].update(key.tobytes())
                y_digests[h].update(y.astype('<f8').tobytes())
                train_counts[h][pair] = len(y)
                prior = float(np.mean(y)) if len(y) >= 20 else np.nan
                chunks[f'prior_{h}'].append(np.full(n, prior))
                for stem in ('y', 'long', 'short', 'valid', 'strict', 'eligible'):
                    chunks[f'{stem}_{h}'].append(a[f'{stem}_{h}'][later])
                chunks[f'arima_{h}'].append(qt[f'forecast__arima110_conditional_ols__{h}m_bps'].to_numpy()[later_loc])
                past = np.searchsorted(clocks, later_t - h * 60)
                safe = np.minimum(past, len(clocks) - 1)
                available = ((past < len(clocks)) & (clocks[safe] == later_t - h * 60)
                             & np.isfinite(mid[later_loc]) & np.isfinite(mid[safe]) & (mid[safe] > 0))
                with np.errstate(invalid='ignore', divide='ignore', over='ignore'):
                    momentum = (mid[later_loc] / mid[safe] - 1.) * 10000.
                momentum[~available | ~np.isfinite(momentum)] = np.nan
                chunks[f'momentum_{h}'].append(momentum)
                for stem, field in (('delay_long', 'long_net_bps'), ('delay_short', 'short_net_bps'), ('delay_valid', 'valid')):
                    chunks[f'{stem}_{h}'].append(qt[f'delayed_label__{h}m__{field}'].to_numpy()[later_loc])
            offset += n
    data = {name: np.concatenate(values) for name, values in chunks.items()}
    data.update(pair_names=pair_names, replay_indices=np.concatenate(replay_indices),
                replay_x=np.concatenate(replay_values), representatives=np.asarray(representatives))
    training = {str(h): {'rows': sum(train_counts[h].values()), 'counts_by_pair': train_counts[h],
                        'pair_clock_sha256': key_digests[h].hexdigest(), 'target_sha256': y_digests[h].hexdigest()}
                for h in HORIZONS}
    return data, training, training_clock_rows


def audit_scores(data, prediction, h, metrics):
    """Independent decisions on every original row; bounded scalar cost cases."""
    scalar_cases = 0
    for split_id, split_name in ((1, 'validation'), (2, 'later_development_test')):
        selected = data['split'] == split_id
        clocks, pairs, pred, spread = (data[n][selected] if n != 'prediction' else prediction[selected]
                                       for n in ('time', 'pair_id', 'prediction', 'spread'))
        signs, decisions = independent_decisions(clocks, pairs, pred, spread, h)
        issued = np.isfinite(pred)
        common = (np.isfinite(data[f'arima_{h}'][selected]) & np.isfinite(data[f'momentum_{h}'][selected])
                  & np.isfinite(data[f'prior_{h}'][selected]))
        eligible = data[f'eligible_{h}'][selected]
        actual = data[f'y_{h}'][selected]
        own = issued & eligible & data[f'valid_{h}'][selected]
        own_report = metrics[split_name]['primary']['own_coverage_forecast_scores_without_comparator_restriction']
        require(own_report['scored_forecasts'] == int(own.sum()), 'own_forecast_coverage_count')
        close_number(own_report['mae_bps'], float(np.mean(np.abs(pred[own] - actual[own]))) if own.any() else None, 'own_mae')
        for mode in ('primary', 'one_minute_entry_delay'):
            report = metrics[split_name][mode]
            delayed = mode != 'primary'
            endpoint = data[f'valid_{h}'][selected].copy()
            if delayed:
                endpoint &= data[f'delay_valid_{h}'][selected]
            strict = data[f'strict_{h}'][selected] & endpoint
            base = endpoint & eligible & common
            cohorts = {'full_endpoint': base, 'shared_strict': base & strict, 'additional_endpoint_only': base & ~strict}
            long = data[f'{"delay_long" if delayed else "long"}_{h}'][selected]
            short = data[f'{"delay_short" if delayed else "short"}_{h}'][selected]
            require(report['input_origins'] == len(pred) and report['issued_forecasts'] == int(issued.sum()), 'all_origin_issuance')
            require(report['comparison_origins'] == int(common.sum()), 'origin_known_matched_count')
            for cohort_name, cohort in cohorts.items():
                mask = cohort & issued
                f = report['forecast_cohorts'][cohort_name]
                require(f['cohort_origins'] == int(cohort.sum()) and f['scored_forecasts'] == int(mask.sum()), 'forecast_cohort_count')
                close_number(f['mae_bps'], float(np.mean(np.abs(pred[mask] - actual[mask]))) if mask.any() else None, 'matched_mae')
            for policy, decision in decisions.items():
                p = report['policies'][policy]
                require(p['decisions'] == int(decision.sum()), 'independent_decision_count')
                require(p['decision_pair_clock_side_sha256'] == decision_sha(decision, clocks, pairs, signs, data['pair_names']),
                        'independent_origin_decision_digest')
                for cohort_name, cohort in cohorts.items():
                    mask = decision & cohort
                    c = p['cohorts'][cohort_name]
                    require(c['scored_decisions'] == int(mask.sum()), 'policy_cohort_count')
                    net = np.where(signs[mask] > 0, long[mask], short[mask])
                    for cost in (0., 1., 2.):
                        scenario = c['cost_scenarios'][str(cost)]
                        close_number(scenario['mean_net_bps'], float(np.mean(net - cost)) if len(net) else None, 'net_mean')
                    # Scalar arithmetic confirms selected-side and cost semantics;
                    # selected indices inherit full-history holding reservations.
                    candidates = np.flatnonzero(mask)
                    probes = candidates[np.linspace(0, len(candidates) - 1, min(10, len(candidates)), dtype=int)] if len(candidates) else []
                    for i in probes:
                        chosen = float(long[i]) if int(signs[i]) > 0 else float(short[i])
                        require(np.isfinite(chosen - 1.), 'finite_scalar_selected_side_cost')
                        scalar_cases += 1
    return scalar_cases


def run(comparison, output):
    comparison, output = Path(comparison).resolve(), Path(output).resolve()
    require(comparison.is_relative_to(ROOT / 'data'), 'local_comparison_artifact_required')
    require(not output.exists() and output.is_relative_to(ROOT / 'docs' / 'validation'), 'new_validation_record_required')
    manifest_path = comparison / 'RESULTS.json'
    result_sha = sha(manifest_path)
    result = read_json(manifest_path)
    require(result['status'] == 'complete' and result['learned_fit_count'] == 32, 'complete_32_fit_result_required')
    require(len(result['cells']) == 32 and len(result['baselines']) == 20, '32_learned_20_control_artifacts_required')
    prepared, quotes = Path(result['prepared_root']), Path(result['quote_root'])
    require(sha(prepared / 'PREPARED.json') == result['prepared_sha256'], 'prepared_manifest_binding')
    require(sha(quotes / 'QUOTE_PANEL.json') == result['quote_sha256'], 'quote_manifest_binding')
    pm, qm = read_json(prepared / 'PREPARED.json'), read_json(quotes / 'QUOTE_PANEL.json')
    require(pm['base_sha256'] == qm['base_manifest_sha256'] and pm['overlay_sha256'] == qm['endpoint_manifest_sha256'], 'shared_base_overlay_binding')
    require(pm['status'] == qm['status'] == 'complete' and len(pm['pairs']) == 68, 'complete_68_pair_inputs')
    for path, digest in result['source_bindings'].items():
        checked(ROOT, path, digest)
    endpoint_root = Path(pm['overlay'])
    require(sha(endpoint_root / 'ENDPOINT_DATASET.json') == pm['overlay_sha256'], 'endpoint_manifest_unchanged')
    endpoint = read_json(endpoint_root / 'ENDPOINT_DATASET.json')
    for record in endpoint['pairs'].values():
        checked(endpoint_root, record['receipt_path'], record['receipt_sha256'])
    began = time.monotonic()
    data, training, training_clock_rows = load_expected(pm, qm, prepared, quotes)
    n = len(data['time'])
    require(n == result['assessment_rows'] == 911254, 'all_911254_original_assessment_origins')
    require(training_clock_rows == pm['rows'] - n, 'complete_sampled_training_count')
    expected_tags = {f'{learner}_{group}_{h}m' for learner in ('ridge', 'hgb') for group in result['groups'] for h in HORIZONS}
    require(set(result['cells']) == expected_tags, 'complete_fixed_32_cell_set')
    require(set(result['baselines']) == {f'{name}_{h}m' for name in CONTROL_NAMES for h in HORIZONS}, 'complete_fixed_control_set')
    report = {'status': 'running', 'scope': 'Independent artifact keys, fit selections, bounded replay and decision/cost audit; no refits or raw rereads',
              'comparison_root': str(comparison), 'results_sha256': result_sha, 'audit_source_sha256': sha(__file__),
              'started_utc': datetime.now(timezone.utc).isoformat(), 'forecast_files': {}, 'training_by_horizon': training,
              'sampled_training_origins': training_clock_rows, 'assessment_origins_per_file': n,
              'endpoint_receipts_verified': len(endpoint['pairs']), 'source_bindings_verified': len(result['source_bindings']),
              'limits': ['No full model replay or refitting; first8192 exact plus68 later-test representatives only.',
                         'No raw-source or full feature regeneration repeated; accepted input artifacts and receipt bytes are verified.',
                         'Group concentration rankings and every metric field are not independently recalculated.',
                         'All periods are previously inspected development data; this audit does not establish profitable trading.']}
    with threadpool_limits(limits=4):
        for tag, cell in [*result['cells'].items(), *result['baselines'].items()]:
            h = cell['horizon_minutes']
            forecast = cell['forecast']
            table = pq.ParquetFile(checked(comparison, forecast['path'], forecast['sha256'])).read()
            require(table.num_rows == forecast['rows'] == n, 'forecast_original_row_count:' + tag)
            for stored, expected in (('pair_id', 'pair_id'), ('bar_start_epoch', 'time'), ('split', 'split')):
                require(np.array_equal(table[stored].to_numpy(), data[expected]), 'every_forecast_original_key:' + tag)
            prediction = table['predicted_bps'].to_numpy()
            metrics = read_json(checked(comparison, cell['metrics']['path'], cell['metrics']['sha256']))
            item = {'rows': n, 'finite_predictions': int(np.isfinite(prediction).sum()), 'key_identity': True,
                    'forecast_sha256': forecast['sha256'], 'scalar_cost_cases': audit_scores(data, prediction, h, metrics)}
            if tag in result['cells']:
                require(np.isfinite(prediction).all(), 'every_learned_origin_has_forecast')
                require(cell['training_selection'] == training[str(h)], 'same_horizon_actual_training_selection')
                bundle = joblib.load(checked(comparison, cell['model']['path'], cell['model']['sha256']))
                require(bundle['training_selection'] == training[str(h)] and bundle['prepared_sha256'] == result['prepared_sha256'], 'saved_model_training_binding')
                names = result['groups'][cell['group']]
                require(bundle['selected_feature_names'] == names and bundle['pair_names'] == data['pair_names'], 'saved_model_feature_pair_identity')
                columns = [pm['feature_names'].index(name) for name in names]
                model = bundle['estimator']
                if cell['learner'] == 'hgb':
                    require(model.is_categorical_.tolist() == [False] * len(names) + [True], 'saved_hgb_categorical_pair_field')
                for recipe_name, value in result['recipes'][cell['learner']].items():
                    require(model.get_params()[recipe_name] == value, 'saved_fixed_estimator_recipe')
                first = np.arange(8192)
                index = np.searchsorted(data['replay_indices'], first)
                x = learner_matrix(data['replay_x'][index][:, columns], data['pair_id'][first], cell['learner'], 68)
                replay = model.predict(x)
                require(finite_bits_equal(prediction[first], replay), 'first8192_saved_prediction_exact_bits')
                representatives = data['representatives']
                index = np.searchsorted(data['replay_indices'], representatives)
                x = learner_matrix(data['replay_x'][index][:, columns], data['pair_id'][representatives], cell['learner'], 68)
                replay = model.predict(x)
                require(np.allclose(prediction[representatives], replay, rtol=1e-12, atol=1e-10), 'all68_later_test_representatives_replay')
                item.update(first_chunk_exact_rows=8192, later_test_pair_representatives=68)
            else:
                control = cell['name']
                expected = (np.zeros(n) if control == 'no_change' else data[f'prior_{h}'] if control == 'pair_train_mean'
                            else data[f'arima_{h}'] if control == 'arima110_conditional_ols'
                            else data[f'momentum_{h}'] if control == 'momentum_exact_past_horizon' else -data[f'momentum_{h}'])
                require(finite_bits_equal(prediction, expected), 'independent_all_row_control_recreation:' + tag)
                item['all_control_predictions_recreated_exactly'] = True
            report['forecast_files'][tag] = item
            print(json.dumps({'audited': tag, 'complete_files': len(report['forecast_files']), 'elapsed_seconds': round(time.monotonic() - began, 1)}), flush=True)
    require(sha(manifest_path) == result_sha, 'completed_result_unchanged_during_audit')
    require(sha(endpoint_root / 'ENDPOINT_DATASET.json') == pm['overlay_sha256'], 'endpoint_metadata_unchanged_after_audit')
    for record in endpoint['pairs'].values():
        checked(endpoint_root, record['receipt_path'], record['receipt_sha256'])
    report.update(status='passed', completed_utc=datetime.now(timezone.utc).isoformat(),
                  elapsed_seconds=round(time.monotonic() - began, 3), forecast_files_verified=52,
                  total_forecast_key_rows_verified=n * 52, models_replayed=32, controls_recreated=20,
                  scalar_cost_cases=sum(r['scalar_cost_cases'] for r in report['forecast_files'].values()),
                  original_artifacts_changed=False)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('x', encoding='utf-8') as stream:
        json.dump(report, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write('\n')
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--comparison', type=Path, required=True)
    parser.add_argument('--output', type=Path, default=ROOT / 'docs/validation/rolling_model_comparison_20260915/INDEPENDENT_AUDIT.json')
    args = parser.parse_args()
    report = run(args.comparison, args.output)
    print(json.dumps({key: report[key] for key in ('status', 'forecast_files_verified', 'elapsed_seconds')}))
