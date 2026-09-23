"""Endpoint evidence is retrospective and must not filter the origin population."""
import numpy as np
import pytest

import oanda_rolling_technical_labels_v1 as strict
import oanda_rolling_technical_endpoint_labels_v1 as endpoint

START=1_800_000_000


def data(indexes, *, constant=False):
    indexes=np.asarray(indexes)
    price=np.full(len(indexes),1.2) if constant else 1.2+indexes*.0001
    return {'time':START+indexes*60,'close':price,'bid_close':price-.00002,'ask_close':price+.00002}


def test_gap_does_not_hide_known_endpoint_return_and_spread_cost():
    source=data([0,1,3,4,5],constant=True)
    values=endpoint.compute_endpoint_outcomes(source,horizons=(5,),coverage_end_epoch=START+6*60)
    old=strict.compute_outcomes(source,horizons=(5,),coverage_end_epoch=START+6*60)
    assert old['label__5m__state'][0]=='gap_in_path'
    assert np.isnan(old['label__5m__return_bps'][0])
    assert values['endpoint_label__5m__state'][0]=='available'
    assert values['endpoint_label__5m__direction'][0]==0
    assert values['endpoint_label__5m__long_net_bps'][0]==pytest.approx(-.00004/1.2*10000)
    assert values['endpoint_label__5m__short_net_bps'][0]<0
    assert all(len(a)==5 for a in values.values())


def test_exact_target_required_no_nearest_clock_substitution():
    values=endpoint.compute_endpoint_outcomes(data([0,1,4,6]),horizons=(5,),coverage_end_epoch=START+8*60)
    assert values['endpoint_label__5m__state'].tolist()==['missing_target','available','pending_right_edge','pending_right_edge']
    assert np.isnan(values['endpoint_label__5m__return_bps'][0])


def test_empty_range_and_scanned_sparse_right_edge_are_distinguished():
    values=endpoint.compute_endpoint_outcomes(data([]),horizons=(5,),coverage_end_epoch=START+600)
    assert all(len(a)==0 for a in values.values())
    sparse=endpoint.compute_endpoint_outcomes(data([0,1]),horizons=(5,),coverage_end_epoch=START+600)
    assert sparse['endpoint_label__5m__state'].tolist()==['missing_target','missing_target']


def test_complete_path_matches_strict_numeric_bits():
    source=data(np.arange(150))
    old=strict.compute_outcomes(source)
    new=endpoint.compute_endpoint_outcomes(source)
    for h in endpoint.DEFAULT_HORIZONS:
        for field in endpoint.NUMERIC_FIELDS:
            a=old[f'label__{h}m__{field}'];b=new[f'endpoint_label__{h}m__{field}']
            assert np.array_equal(np.isnan(a),np.isnan(b))
            mask=np.isfinite(a)
            assert np.array_equal(a[mask].view(np.uint64),b[mask].view(np.uint64))


def test_direction_and_quote_validity_are_separate():
    source=data([0,5])
    source['bid_close'][1]=np.nan
    values=endpoint.compute_endpoint_outcomes(source,horizons=(5,))
    assert values['endpoint_label__5m__midpoint_valid'][0]
    assert not values['endpoint_label__5m__bidask_endpoint_valid'][0]
    assert np.isnan(values['endpoint_label__5m__long_net_bps'][0])
    # Target midpoint is unnecessary for net quotes if origin midpoint exists.
    source=data([0,5]);source['close'][1]=np.nan
    values=endpoint.compute_endpoint_outcomes(source,horizons=(5,))
    assert not values['endpoint_label__5m__midpoint_valid'][0]
    assert values['endpoint_label__5m__bidask_endpoint_valid'][0]


def test_extreme_arithmetic_and_crossed_quotes_do_not_publish_invalid_values():
    source=data([0,5]);source['ask_close'][1]=source['bid_close'][1]-.01
    values=endpoint.compute_endpoint_outcomes(source,horizons=(5,))
    assert not values['endpoint_label__5m__bidask_endpoint_valid'][0]
    source={'time':np.array([START,START+300]),'close':np.array([1e-300,1e300]),
            'bid_close':np.array([1e-300,1e300]),'ask_close':np.array([1e-300,1e300])}
    values=endpoint.compute_endpoint_outcomes(source,horizons=(5,))
    assert not values['endpoint_label__5m__midpoint_valid'][0]
    assert not values['endpoint_label__5m__bidask_endpoint_valid'][0]


def test_registry_excludes_all_endpoint_fields_from_predictors():
    registry=endpoint.endpoint_registry(include_split_eligibility=True)
    assert len(registry)==52
    assert len({r['name'] for r in registry})==52
    assert all(r['name'].startswith('endpoint_label__') and r['future_information'] and not r['model_input'] for r in registry)
    assert not any('favorable' in r['name'] or 'adverse' in r['name'] for r in registry)


@pytest.mark.parametrize('end',[START+1, float('nan'), True, START])
def test_invalid_coverage_refused(end):
    with pytest.raises(ValueError,match='coverage_end'):
        endpoint.compute_endpoint_outcomes(data([0,1]),coverage_end_epoch=end)


def test_future_changes_affect_labels_without_mutating_inputs():
    source=data([0,1,5]);saved={k:v.copy() for k,v in source.items()}
    endpoint.compute_endpoint_outcomes(source,horizons=(5,))
    for key in saved: np.testing.assert_array_equal(source[key],saved[key])
