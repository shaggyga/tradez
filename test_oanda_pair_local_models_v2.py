"""Causal synthetic gap, independent-family and elapsed-time model checks."""
from datetime import datetime,timezone
import math
import numpy as np
import pytest
import oanda_pair_local_models_v2 as model

START=int(datetime(2026,9,4,10,tzinfo=timezone.utc).timestamp())
REOPEN=int(datetime(2026,9,6,21,tzinfo=timezone.utc).timestamp())
STATE='probabilistic_state_space';RIDGE='ridge_return_repaired'
def rows(count=480,step=1,start=START):
    return {start+i*step*60:1.1+.0001*(.015*i*step+2*math.sin(i*step/13.)) for i in range(count)}
def predict(data,**kwargs):return model.predict_all(data,max(data),instrument='EUR_USD',pip_size=.0001,**kwargs)

def test_interior_missing_minutes_do_not_erase_exact_elapsed_training():
    data=rows();data={t:p for i,(t,p) in enumerate(data.items()) if i%7!=2}
    result=predict(data)
    assert set(result)=={STATE,RIDGE}
    for family,(_,prob,diagnostic) in result.items():
        assert .25<=prob<=.75
        assert set(diagnostic['current_real_price_epochs'])<=set(data)
        assert diagnostic['synthetic_prices']==0 and diagnostic['time_compression'] is False
        for t in diagnostic['training_row_start_epochs_by_pair']['EUR_USD']:
            assert t in data and t+3600 in data and t+3660<=max(data)+60
    assert len(result[RIDGE][2]['training_row_start_epochs_by_pair']['EUR_USD'])>=24

def test_state_is_independent_of_missing_ridge_training():
    data=rows(25)
    readiness=model.family_readiness(data,max(data),instrument='EUR_USD',pip_size=.0001)
    assert readiness[STATE]['ready'] and not readiness[RIDGE]['ready']
    assert set(predict(data))=={STATE}
    assert predict(data,families=(RIDGE,))=={}
    assert predict(data,families=(STATE,))[STATE]==predict(data)[STATE]

def test_elapsed_rates_do_not_treat_sparse_intervals_as_one_minute():
    data={START+i*120:1.1+i*.0002 for i in range(25)}
    result=predict(data,families=(STATE,))[STATE]
    assert result[2]['elapsed_minutes']==[2.]*24
    assert result[2]['raw_drift_pips_per_minute']==pytest.approx(1.,rel=1e-10)
    assert result[2]['observed_return_rates_pips_per_minute']==pytest.approx([1.]*24)

def test_weekend_reset_reuses_only_valid_old_labels_not_cross_gap_returns():
    old=rows();new=rows(20,start=REOPEN);data=old|new
    result=predict(data)
    assert set(result)=={STATE,RIDGE}
    assert result[STATE][2]['current_real_price_epochs']==list(new)
    assert result[STATE][2]['state_segment_start_epoch']==REOPEN
    assert result[STATE][:2]==predict(new)[STATE][:2]
    for t in result[RIDGE][2]['training_row_start_epochs_by_pair']['EUR_USD']:
        assert t+3600<REOPEN or t>=REOPEN

@pytest.mark.parametrize('count',[1,7,8,9,15])
def test_short_reopen_does_not_borrow_old_observations(count):
    data=rows()|rows(count,start=REOPEN)
    assert predict(data)=={}

def test_label_endpoints_alone_cannot_bridge_a_closed_session():
    data={START+i*3600:1.1+i*.0001 for i in range(100)}
    readiness=model.family_readiness(data,max(data),instrument='EUR_USD',pip_size=.0001)
    assert readiness[RIDGE]['mature_exact_h1_training_rows']==0
    assert not readiness[STATE]['ready']

def test_missing_exact_h1_endpoint_is_not_replaced_by_next_price():
    data=rows(300,step=7)
    readiness=model.family_readiness(data,max(data),instrument='EUR_USD',pip_size=.0001)
    assert readiness[STATE]['ready']
    assert readiness[RIDGE]['mature_exact_h1_training_rows']==0
    assert set(predict(data))=={STATE}

def test_equivalent_price_and_pip_scaling_preserves_predictions():
    data=rows();scaled={t:p*100 for t,p in data.items()}
    base=predict(data);jpy=model.predict_all(scaled,max(scaled),instrument='EUR_JPY',pip_size=.01)
    for family in base:
        assert jpy[family][0]==pytest.approx(base[family][0],abs=1e-8)
        assert jpy[family][1]==pytest.approx(base[family][1],abs=1e-8)

def test_other_pair_identity_does_not_supply_peer_predictors():
    data=rows();a=predict(data);b=model.predict_all(data,max(data),instrument='GBP_USD',pip_size=.0001)
    for family in a:assert a[family][:2]==b[family][:2]

def test_one_numerical_family_failure_does_not_erase_the_other(monkeypatch):
    data=rows();expected=predict(data)[STATE]
    def fail(*args):raise np.linalg.LinAlgError('synthetic')
    monkeypatch.setattr(model,'_ridge',fail)
    result,readiness=model.predict_with_readiness(data,max(data),instrument='EUR_USD',pip_size=.0001)
    assert result=={STATE:expected}
    assert not readiness[RIDGE]['ready'] and readiness[STATE]['ready']
    assert readiness[RIDGE]['reasons']==['family_numerical_failure:LinAlgError']

def test_flat_prices_have_neutral_probability():
    data={t:1.1 for t in rows()}
    for expected,prob,diagnostics in predict(data).values():
        assert expected==0 and prob==.5 and diagnostics['forecast_sigma_pips']>=.25

@pytest.mark.parametrize('families',[(),('bad',),(STATE,STATE),'probabilistic_state_space'])
def test_unknown_empty_or_duplicate_family_selection_fails(families):
    with pytest.raises(ValueError):predict(rows(),families=families)

@pytest.mark.parametrize('value',[0.,-1.,float('nan'),float('inf'),True,'1.1'])
def test_invalid_price_fails_closed(value):
    data=rows();data[START]=value
    with pytest.raises(ValueError):predict(data)

@pytest.mark.parametrize('pair,pip',[('EUR_EUR',.0001),('eur_usd',.0001),('EUR_USD',True),('EUR_USD',.1)])
def test_identity_validation(pair,pip):
    data=rows()
    with pytest.raises(ValueError):model.predict_all(data,max(data),instrument=pair,pip_size=pip)

@pytest.mark.parametrize('kind',['future','not_latest','fractional','too_many'])
def test_timestamp_and_history_bounds(kind):
    data=rows();cutoff=max(data)
    if kind=='future':data[cutoff+60]=1.1
    if kind=='not_latest':cutoff+=60
    if kind=='fractional':data[START+.001]=1.1
    if kind=='too_many':data=rows(4097);cutoff=max(data)
    with pytest.raises(ValueError):model.predict_all(data,cutoff,instrument='EUR_USD',pip_size=.0001)
