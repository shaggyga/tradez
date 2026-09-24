import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from contracts import fingerprint
from later_surface_models_v2 import schedule, horizon_order, SavedModels
from later_surface_layer_v2 import frame, assess, layer_settings
from test_magnitude_layer_v2 import fixture, current_row


def contract():
    return json.loads((Path(__file__).parent/'LATER_REMAINING_SURFACE_CONTRACT_V2.json').read_bytes())


def test_serial_reservations_do_not_backdate_first_origin():
    c = contract(); s = schedule(c)
    assert len(s['fit_tasks']) == 16
    assert s['prewarm']['finish_epoch'] == c['fit_cutoff']+616
    intervals = [(r['start_epoch'], r['ready_epoch']) for r in s['fit_tasks']]
    intervals.append((s['prewarm']['start_epoch'], s['prewarm']['finish_epoch']))
    for a, b in intervals:
        assert a < b
        assert all(b <= x or a >= y for x, y in s['prediction_windows'])
    assert all(b <= x for (a, b), (x, y) in zip(intervals, intervals[1:]))
    assert c['prequential_origins'][0] < s['prewarm']['finish_epoch'] < c['prequential_origins'][1]


def test_all_later_slots_exact_target_first_and_single_worker():
    c = contract()
    for t in c['policy_origins']:
        order = horizon_order(t, c)
        assert len(order) == 8 and sorted(order) == c['horizons_minutes']
        assert t+order[0]*60 == c['common_target_epoch']
    assert horizon_order(c['prequential_origins'][0], c) == c['horizons_minutes']


def test_wrong_saved_model_inventory_refuses_before_loading():
    c = contract()
    with pytest.raises(ValueError, match='inventory'): SavedModels({}, c, schedule(c))


def setup(monkeypatch, *, supported=True):
    rows, outcomes, settings = fixture(n=12 if supported else 7)
    epoch = 500000
    current = [current_row(r, epoch) for r in rows[:20]]
    observations = [{'record_id': r['record_id'], 'instrument': r['instrument'], 'origin_epoch': epoch,
                     'available_epoch': epoch, 'features': [1.]} for r in current]
    values = {r['record_id']: {'signed': {'ridge': r['ridge_prediction_bps'], 'recovered_hgb': r['recovered_hgb_prediction_bps']},
                             'absolute': dict(r['magnitude_prediction_bps'])} for r in current}
    predictor = SimpleNamespace(available_epoch=0, predict=lambda h, obs, t: copy.deepcopy(values))
    c = contract(); c.update(policy_origins=[epoch], common_target_epoch=epoch+900, horizons_minutes=[15])
    c['layer_contract'].update(settings); c['layer_contract']['frozen_cutoff'] = 400000
    market = {'universe': sorted(r['instrument'] for r in current),
        'metadata': {r['instrument']: {} for r in current},
        'rows': [{'instrument': r['instrument'], 'price_epoch': epoch, 'status': 'valid_candle_close_pair'} for r in current]}
    monkeypatch.setattr('later_surface_layer_v2.NativeBatch', lambda trad:
        SimpleNamespace(prepare=lambda pred, *args: {'prediction': pred}, verify=lambda: None))
    return epoch, predictor, observations, rows+current, outcomes, market, c, Path('.')


def test_layer_arms_and_raw_controls_share_exact_support(monkeypatch):
    args = setup(monkeypatch); result, timing = frame(*args)
    assert len(result['coverage']) == 280 and len(result['predictions']) == 280
    assert timing['regression_fits'] == 8
    predictions = {(r['instrument'], r['base_method'], r['variant']): r for r in result['predictions']}
    for pair in args[5]['universe']:
        for base in ('ridge', 'recovered_hgb'):
            raw = predictions[pair, base, 'raw_unrestricted']
            for mode in ('frozen', 'expanding'):
                arms = [predictions[pair, base, v+'_'+mode] for v in ('raw_matched', 'signed_only', 'magnitude_interaction')]
                assert len({r['eligibility_snapshot_id'] for r in arms}) == 1
                assert arms[0]['prediction_bps'] == raw['prediction_bps']
                assert arms[0]['model_id'] == raw['model_id']
                assert arms[1]['model_ready_epoch'] == args[0]+1


def test_no_support_does_not_substitute_raw_for_layer(monkeypatch):
    result, timing = frame(*setup(monkeypatch, supported=False))
    assert len(result['predictions']) == 40
    assert {r['variant'] for r in result['predictions']} == {'raw_unrestricted'}
    assert sum(r['reason'] == 'insufficient_distinct_support' for r in result['coverage']) == 240
    assert timing['regression_fits'] == 0


def test_frozen_phase_not_available_before_cutoff(monkeypatch):
    args = setup(monkeypatch); args[6]['layer_contract']['frozen_cutoff'] = args[0]+1
    result, _ = frame(*args)
    assert result['snapshots']['frozen'] is None
    assert all(r['reason'] == 'layer_phase_not_started' for r in result['coverage'] if r['variant'].endswith('_frozen'))
    assert any(r['variant'] == 'magnitude_interaction_expanding' for r in result['predictions'])


def test_future_current_labels_do_not_change_frame(monkeypatch):
    args = setup(monkeypatch); before, _ = frame(*args)
    for row in args[3]:
        if row['decision_epoch'] == args[0]:
            args[4][row['record_id'], row['target_id']] = {'value': float('nan')}
    after, _ = frame(*args)
    assert after == before


def test_missing_reference_preserves_all_variant_coverage(monkeypatch):
    args = setup(monkeypatch); args[5]['rows'][0]['status'] = 'missing_exact_bar'
    result, _ = frame(*args)
    assert len(result['coverage']) == 280 and len(result['predictions']) == 266
    assert sum(r['reason'] == 'reference_missing_exact_bar' for r in result['coverage']) == 14


def test_fresh_model_replay_mismatch_refuses(monkeypatch):
    args = setup(monkeypatch); args[3][-1]['ridge_prediction_bps'] += 1
    with pytest.raises(ValueError, match='fresh_saved_model_prediction'): frame(*args)


def test_mature_assessment_requires_exact_target_and_keeps_missing(monkeypatch):
    args = setup(monkeypatch); result, _ = frame(*args); labels = {}
    for row in result['predictions']:
        labels[row['record_id'], row['target_id']] = {'label_end_epoch': row['target_epoch'],
            'available_epoch': row['target_epoch'], 'value': 0.}
    labels[next(iter(labels))]['value'] = None
    scores = assess([result], labels, 10**9)
    assert all(r['prediction_rows'] == 20 and r['mature_rows'] == 19 for r in scores['scores'])
    assert all(r['mature_rows'] == 0 for r in assess([result], labels, 0)['scores'])
    labels[next(iter(labels))]['label_end_epoch'] += 1
    with pytest.raises(ValueError, match='exact_target'): assess([result], labels, 10**9)


def test_numerical_layer_settings_remain_original():
    c = contract(); scoped = layer_settings(c)
    for key in ('minimum_distinct_origins', 'minimum_distinct_pairs', 'minimum_distinct_utc_days', 'layer_fit'):
        assert scoped[key] == c['layer_contract'][key]
    assert scoped['surface_contract_sha256'] == fingerprint(c)
