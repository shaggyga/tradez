"""Independent synthetic boundary tests. No fitted models or external data."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import math

import numpy as np
import pandas as pd
import pytest

import oanda_practice_shadow_strategy_lab as lab
import oanda_strategy_lab_historical_backtest as history
import oanda_supervised_m5_contract_v2 as contract


def candles(size=80, offset_minutes=0):
    start = datetime(2026, 1, 5, tzinfo=timezone.utc) + timedelta(minutes=offset_minutes)
    result = []
    for index in range(size):
        mid = 1.1 + index * .0001 + math.sin(index / 5) * .00002
        result.append({'time': (start + timedelta(minutes=5*index)).isoformat(), 'complete': True,
                       'mid': {'c': mid, 'h': mid+.0001, 'l': mid-.0001},
                       'bid': {'c': mid-.00005}, 'ask': {'c': mid+.00005}})
    return result


def m1_frame(rows=60, unit='ns'):
    index = pd.date_range('2026-01-05T00:00:00Z', periods=rows, freq='min').as_unit(unit)
    mid = 1.1 + np.arange(rows) * .00001
    return pd.DataFrame({'open':mid, 'high':mid+.0001, 'low':mid-.0001, 'close':mid,
                         'volume':10., 'bid_open':mid-.00005, 'bid_close':mid-.00005,
                         'ask_open':mid+.00005, 'ask_close':mid+.00005}, index=index)


@pytest.fixture
def no_fit(monkeypatch):
    lab.SUPERVISED_MODEL_CACHE.clear()
    monkeypatch.setattr(lab, '_fit_ridge', lambda x,y: None)
    monkeypatch.setattr(lab, '_predict_ridge', lambda model,x: np.zeros((len(x), 2)))
    yield
    lab.SUPERVISED_MODEL_CACHE.clear()


@pytest.mark.parametrize('unit', ['ns','us','ms','s'])
def test_equal_m1_clocks_are_accepted_independently_of_pandas_storage_unit(unit):
    result = history.build_pair_history('EUR_USD', m1_frame(unit=unit))
    assert len(result.m1_times) == 60
    assert len(result.m5_times) == 12


def test_sparse_s5_rows_cannot_prove_complete_m1_or_m5(tmp_path, monkeypatch):
    sparse = m1_frame().rename(columns={name: 'mid_'+name for name in ['open','high','low','close']})
    sparse['dt'] = sparse.index
    for side in ['bid','ask']:
        sparse[side+'_high'] = sparse[side+'_close']+.0001
        sparse[side+'_low'] = sparse[side+'_close']-.0001
    (tmp_path / 'EUR_USD_S5.parquet').touch()
    monkeypatch.setattr(pd, 'read_parquet', lambda path: sparse)
    try:
        result = history.load_pair_history_s5(tmp_path, 'EUR_USD')
    except ValueError:
        return  # Explicit refusal is an acceptable fail-closed result.
    assert len(result.m5_times) == 0, 'One S5 observation per minute became complete M5 candles'


def test_explicit_incomplete_m1_input_is_not_upgraded_to_complete_m5():
    frame = m1_frame()
    frame['complete'] = True
    frame.loc[frame.index[2], 'complete'] = False
    result = history.build_pair_history('EUR_USD', frame)
    assert pd.Timestamp('2026-01-05T00:00:00Z') not in result.m5_times


def test_prior_valid_metadata_is_cleared_when_all_current_inputs_withheld(no_fit):
    features = {f'PAIR_{i}': {'pip':.0001} for i in range(20)}
    data = {key: {'M5':candles()} for key in features}
    lab.augment_supervised_return_features(features, data)
    assert all(row['supervised_ready'] for row in features.values())
    for value in data.values():
        value['M5'][-1]['complete'] = False
    lab.augment_supervised_return_features(features, data)
    assert all(not row['supervised_ready'] for row in features.values())
    for row in features.values():
        assert not row.get('supervised_model_decision_time'), 'Prior decision clock survives withheld update'
        assert not row.get('supervised_data_quality'), 'Prior quality counts survive withheld update'
        assert not row.get('supervised_purged_training_rows'), 'Prior purge count survives withheld update'


def test_published_pair_clock_covers_consumed_pooled_training_maturity(no_fit):
    features = {f'PAIR_{i}': {'pip':.0001} for i in range(20)}
    data = {key: {'M5':candles(offset_minutes=-60 if key == 'PAIR_0' else 0)} for key in features}
    source_targets = [target for value in data.values() for target in lab._supervised_candle_data(value['M5'], .0001)['target_times']]
    lab.augment_supervised_return_features(features, data)
    assert not features['PAIR_0']['supervised_ready']
    assert features['PAIR_0']['supervised_withheld_reason']=='stale_m5_context_for_shared_fit'
    assert sum(row['supervised_ready'] for row in features.values())==19
    latest_target = max(source_targets)
    for row in features.values():
        if not row['supervised_ready']:
            continue
        assert row['supervised_model_decision_time'] >= latest_target, 'Per-pair clock predates known pooled targets; add shared as-of identity or restrict fit'


def test_malformed_boolean_pip_is_not_coerced_into_a_valid_instrument_pip(no_fit):
    features = {f'PAIR_{i}': {'pip':True} for i in range(20)}
    data = {key: {'M5':candles()} for key in features}
    lab.augment_supervised_return_features(features, data)
    assert all(not row['supervised_ready'] for row in features.values())


def test_invalid_middle_row_resets_recursive_state_without_mutating_input():
    values = candles(161)
    values[80]['complete'] = False
    before = deepcopy(values)
    result = lab._supervised_candle_data(values, .0001)
    first = lab._supervised_contiguous_candle_data(values[:80], .0001)
    last = lab._supervised_contiguous_candle_data(values[81:], .0001)
    assert result['x'] == first['x'] + last['x']
    assert result['y'] == first['y'] + last['y']
    assert result['current_x'] == last['current_x']
    assert values == before


def test_fingerprint_covers_each_consumed_field_and_completion():
    values = candles()
    original = contract.input_fingerprint(values, .0001)
    for side, field in [('mid','c'),('mid','h'),('mid','l'),('bid','c'),('ask','c')]:
        revised = deepcopy(values)
        revised[40][side][field] += .000001
        assert contract.input_fingerprint(revised, .0001) != original
    for key, replacement in [('complete',False),('time','2026-01-05T03:21:00Z')]:
        revised = deepcopy(values)
        revised[40][key] = replacement
        assert contract.input_fingerprint(revised, .0001) != original


def test_exact_unique_m1_clock_set_proves_five_members_without_sort_artifact():
    frame = m1_frame().drop(pd.Timestamp('2026-01-05T00:02:00Z'))
    result = history.build_pair_history('HKD_JPY', frame.iloc[::-1])
    assert len(result.m5_times) == 11
    assert pd.Timestamp('2026-01-05T00:00:00Z') not in result.m5_times
    assert result.pip == .0001


def test_origin_and_target_maturity_are_real_close_clocks():
    result = lab._supervised_candle_data(candles(), .0001)
    assert result['times'][0] == '2026-01-05T03:05:00Z'
    assert result['target_times'][0] == '2026-01-05T03:10:00Z'
    assert all(datetime.fromisoformat(target)-datetime.fromisoformat(origin) == timedelta(minutes=5)
               for origin,target in zip(result['times'],result['target_times'],strict=True))
    assert max(result['target_times']) <= result['model_decision_time']


def test_label_closing_exactly_on_validation_cutoff_cannot_change_training_fit(no_fit, monkeypatch):
    captures = []
    monkeypatch.setattr(lab, '_fit_ridge', lambda x,y: captures.append((x.copy(),y.copy())))
    features = {f'PAIR_{i}': {'pip':.0001} for i in range(20)}
    data = {key: {'M5':candles()} for key in features}
    lab.augment_supervised_return_features(features, data)
    for value in data.values():
        # This M5 closes at 05:55, the first validation origin in this fixture.
        row = next(r for r in value['M5'] if r['time'] == '2026-01-05T05:50:00+00:00')
        for side in ['mid','bid','ask']:
            row[side]['c'] += .00002
    lab.augment_supervised_return_features(features, data)
    assert len(captures) == 4
    assert len(captures[0][0]) == 660
    np.testing.assert_array_equal(captures[0][0], captures[2][0])
    np.testing.assert_array_equal(captures[0][1], captures[2][1])
    assert not np.array_equal(captures[1][1], captures[3][1])
