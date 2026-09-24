from types import SimpleNamespace

import pytest

from contracts import fingerprint
from currency_projection_v2 import BASES, project_frame
from currency_projection_native_v2 import build_frame
from test_currency_projection_v2 import fixture, solver


class FakeNative:
    def prepare(self, prediction, *unused):
        return {'prediction': prediction}

    def verify(self):
        return None


def setup(solver):
    raw, projection_contract, _ = fixture()
    saved = project_frame(raw, projection_contract, solver)
    origin = raw['origin_epoch']
    observations = []
    fresh = {}
    qualified = []
    for source in raw['predictions']:
        if not any(x['record_id'] == source['record_id'] for x in observations):
            observations.append({'record_id': source['record_id'], 'instrument': source['instrument'],
                                 'origin_epoch': origin, 'available_epoch': origin, 'source_member_sha256': 'source-' + source['instrument']})
        fresh.setdefault(source['record_id'], {'signed': {}})['signed'][source['base_method']] = source['prediction_bps']
        parent = {'diagnostic_forecast_id': source['forecast_id'], 'base_method': source['base_method'],
                  'absolute_prediction_bps': 7.0, 'absolute_fit_id': 'absolute-' + source['base_method'],
                  'base_prediction_sha256': 'base-' + source['record_id']}
        qualified.append(parent)
    current = sorted(observations, key=lambda x: x['instrument'])
    market = {'rows': [{'instrument': pair, 'price_epoch': origin, 'status': 'valid_candle_close_pair'} for pair in projection_contract['universe']],
              'metadata': {pair: {} for pair in projection_contract['universe']}}
    contract = {'universe': projection_contract['universe'], 'parent_surface_contract': {},
                'resources': {'fresh_base_projection_seconds': 1, 'native_frame_seconds': 2}}
    cohort = {'name': 'synthetic', 'target': origin + 360 * 60, 'policy_origins': [origin]}
    predictor = SimpleNamespace(available_epoch=origin, predict=lambda *unused: fresh)
    qualified_frame = {'predictions': qualified}
    return cohort, origin, predictor, FakeNative(), current, raw, saved, qualified_frame, market, projection_contract, solver, contract


def test_fresh_native_projection_has_new_clock_and_preserves_diagnostic_provenance(solver):
    args = setup(solver)
    result, timing = build_frame(*args)
    assert len(result['coverage']) == 36
    assert len(result['predictions']) == len(result['packets']) == 36
    assert timing['base_model_fits'] == 0
    assert all(x['available_epoch'] == 102 for x in result['predictions'])
    assert any(x['diagnostic_available_epoch'] != x['available_epoch'] for x in result['predictions'] if x['variant'] != 'direct')
    direct = [x for x in result['predictions'] if x['variant'] == 'direct']
    projected = [x for x in result['predictions'] if x['variant'] == 'currency_projection']
    assert all(x['eligibility_snapshot_id'] is None for x in direct)
    assert all(x['eligibility_snapshot_id'] is not None for x in projected)
    assert all(x['forecast_id'] != x['diagnostic_forecast_id'] for x in result['predictions'])


def test_changed_fresh_base_value_refuses_before_projection(solver):
    args = list(setup(solver))
    original = args[2].predict
    def changed(*unused):
        values = original()
        values[next(iter(values))]['signed']['ridge'] += 1
        return values
    args[2] = SimpleNamespace(predict=changed)
    with pytest.raises(ValueError, match='fresh_base_values'):
        build_frame(*args)


def test_diagnostic_timestamp_is_not_reused_as_native_timestamp(solver):
    args = list(setup(solver))
    for item in args[6]['predictions']:
        item['available_epoch'] += 100
        item['forecast_id'] = fingerprint({k: v for k, v in item.items() if k != 'forecast_id'})
    result, _ = build_frame(*args)
    assert all(x['available_epoch'] == args[1] + 2 for x in result['predictions'])
    assert all(x['diagnostic_available_epoch'] >= args[1] + 100 for x in result['predictions'])


def test_changed_saved_projection_value_or_lineage_refuses(solver):
    args = list(setup(solver))
    args[6]['predictions'][0]['prediction_bps'] += 1
    args[6]['predictions'][0]['forecast_id'] = fingerprint({k: v for k, v in args[6]['predictions'][0].items() if k != 'forecast_id'})
    with pytest.raises(ValueError, match='projection_value_or_lineage'):
        build_frame(*args)
