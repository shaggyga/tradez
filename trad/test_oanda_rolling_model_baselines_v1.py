import json
import math
import numpy as np
import pytest

import oanda_rolling_model_baselines_v1 as baseline

START=1_800_000_000


def series(n=160,phi=.8):
    increments=.01*phi**np.arange(n-1)
    logs=np.r_[0.,np.cumsum(increments)]
    return START+np.arange(n)*60,np.exp(logs)


def parameters(phi=.5,cutoff=START):
    return {'schema_version':baseline.SCHEMA,'engine':baseline.ENGINE,'status':'fitted',
            'phi':phi,'phi_clip':.99,'training_cutoff_epoch':cutoff}


def test_conditional_ols_recovers_analytical_ar1():
    times,prices=series()
    params=baseline.fit_arima110(times,prices,START+125*60)
    assert params['status']=='fitted'
    assert params['phi']==pytest.approx(.8,abs=1e-12)
    assert params['valid_regression_pairs']==122
    assert params['last_response_end_epoch']<params['training_cutoff_epoch']
    assert not params['phi_was_clipped']


def test_geometric_horizon_and_current_increment_are_not_off_by_one():
    times=START+np.arange(4)*60
    prices=np.exp([0.,.01,.04,.02])
    values=baseline.predict_arima110(times,prices,parameters(),horizons=(1,5,15,30,60))
    assert np.isnan(values[1][0])
    assert values[1][2]==pytest.approx(np.expm1(.03*.5)*10000)
    assert values[1][3]==pytest.approx(np.expm1(-.02*.5)*10000)
    for h in (5,15,30,60):
        assert values[h][2]==pytest.approx(np.expm1(.03*sum(.5**k for k in range(1,h+1)))*10000)


def test_negative_phi_alternates_forecast_increment_and_recreates():
    times,prices=series(n=140,phi=-.4)
    params=baseline.fit_arima110(times,prices,START+125*60)
    assert params['phi']==pytest.approx(-.4,abs=1e-12)
    restored=json.loads(json.dumps(params,allow_nan=False))
    before=baseline.predict_arima110(times,prices,params)
    after=baseline.predict_arima110(times,prices,restored)
    for h in before:
        np.testing.assert_array_equal(before[h],after[h])


def test_future_closes_and_appended_future_rows_cannot_change_fit():
    times,prices=series()
    cutoff=START+125*60
    original=baseline.fit_arima110(times,prices,cutoff)
    changed=prices.copy();changed[times+60>=cutoff]=np.nan
    assert baseline.fit_arima110(times,changed,cutoff)==original
    more_t=np.r_[times,START+np.arange(160,180)*60]
    more_p=np.r_[prices,np.full(20,1e5)]
    assert baseline.fit_arima110(more_t,more_p,cutoff)==original


def test_training_requires_two_adjacent_real_minute_increments():
    times,prices=series(n=140)
    keep=np.arange(140)!=20
    params=baseline.fit_arima110(times[keep],prices[keep],START+125*60)
    assert params['valid_regression_pairs']==119  # missing row and both affected triples
    out=baseline.predict_arima110(times[keep],prices[keep],parameters(),horizons=(5,))
    gap_index=np.flatnonzero(times[keep]==START+21*60)[0]
    assert np.isnan(out[5][gap_index])
    assert np.isfinite(out[5][gap_index+1])


def test_invalid_prices_do_not_make_compressed_increments():
    times,prices=series(n=140);prices[20]=0
    params=baseline.fit_arima110(times,prices,START+125*60)
    assert params['valid_regression_pairs']==119
    states=baseline.arima110_origin_states(times,prices,parameters())
    assert states[20]=='missing_current_increment' and states[21]=='missing_current_increment'


def test_strict_bar_end_boundary_and_pre_fit_forecast_states():
    times,prices=series(n=140)
    cutoff=START+125*60
    params=baseline.fit_arima110(times,prices,cutoff)
    assert params['last_response_end_epoch']==cutoff-60
    states=baseline.arima110_origin_states(times,prices,params)
    assert states[123]=='pre_fit_origin'
    assert states[124]=='available'
    out=baseline.predict_arima110(times,prices,params,horizons=(5,))[5]
    assert np.isnan(out[:124]).all()


def test_cold_and_flat_models_remain_explicitly_unavailable():
    times,prices=series(n=10)
    cold=baseline.fit_arima110(times,prices,START+20*60)
    assert cold['status']=='unavailable' and cold['reason']=='insufficient_training_regression_pairs'
    assert np.isnan(baseline.predict_arima110(times,prices,cold)[60]).all()
    times=START+np.arange(130)*60
    flat=baseline.fit_arima110(times,np.ones(130),START+140*60)
    assert flat['reason']=='zero_training_regressor_energy' and flat['phi'] is None
    json.dumps(flat,allow_nan=False)


def test_stationarity_clip_is_saved_with_unclipped_value():
    n=110;increments=.0001*1.03**np.arange(n-1)
    times=START+np.arange(n)*60;prices=np.exp(np.r_[0.,np.cumsum(increments)])
    params=baseline.fit_arima110(times,prices,START+120*60)
    assert params['phi_unclipped']==pytest.approx(1.03,abs=1e-12)
    assert params['phi']==.99 and params['phi_was_clipped']


@pytest.mark.parametrize('change',[lambda t:np.r_[t[:4],t[3],t[5:]],lambda t:t+1])
def test_invalid_clocks_fail_instead_of_silent_sorting(change):
    times,prices=series(n=10)
    with pytest.raises(ValueError): baseline.fit_arima110(change(times),prices,START+120*60)


def test_model_engine_and_phi_are_validated():
    times,prices=series(n=5)
    with pytest.raises(ValueError,match='identified_arima'):
        baseline.predict_arima110(times,prices,{**parameters(),'engine':'sarimax'})
    with pytest.raises(ValueError,match='stationary_saved_phi'):
        baseline.predict_arima110(times,prices,{**parameters(),'phi':1.1})
