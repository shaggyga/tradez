import copy
import json
import joblib
import numpy as np
import pytest
from threadpoolctl import threadpool_limits
from tools import run_rolling_model_comparison_v1 as run


def fixture():
    rng=np.random.default_rng(49)
    n=600
    data={'x':rng.normal(size=(n,3)).astype(np.float32),'pair_id':np.repeat([0,1],n//2).astype(np.int16),
          'pair_names':['EUR_USD','USD_JPY'],'time':np.tile(1800000000+np.arange(n//2)*60,2),
          'split':np.tile(np.r_[np.zeros(200),np.ones(50),np.full(50,2)],2).astype(np.int8),
          'spread':np.full(n,.5)}
    for h in run.HORIZONS:
        data[f'y_{h}']=data['x'][:,0]*2+.1
        data[f'long_{h}']=data[f'y_{h}']-.5
        data[f'short_{h}']=-data[f'y_{h}']-.5
        data[f'valid_{h}']=np.ones(n,dtype=bool)
        data[f'strict_{h}']=np.arange(n)%3!=0
        data[f'eligible_{h}']=np.ones(n,dtype=bool)
        data[f'arima_{h}']=np.ones(n)
        data[f'momentum_{h}']=np.ones(n)
        data[f'delay_long_{h}']=data[f'long_{h}']-.1
        data[f'delay_short_{h}']=data[f'short_{h}']-.1
        data[f'delay_valid_{h}']=np.arange(n)%7!=0
    return data


def test_training_label_masks_are_separate_from_issuance():
    d=fixture();d['valid_5'][0]=False;d['eligible_5'][1]=False
    selected=run.training_mask(d,5)
    assert selected.sum()==398 and not selected[0] and not selected[1]
    assert not selected[d['split']!=0].any()
    d['y_5'][2]=np.nan
    with pytest.raises(ValueError,match='finite'):run.training_mask(d,5)


def test_later_features_and_outcomes_cannot_change_fit_or_pair_prior(tmp_path):
    d=fixture();assessment=np.flatnonzero(d['split']!=0);mask=run.training_mask(d,5)
    changed=copy.deepcopy(d);changed['x'][assessment]=1e6;changed['y_5'][assessment]=-1e9
    p,_=run.pair_prior(d,5,assessment);q,_=run.pair_prior(changed,5,assessment)
    assert np.array_equal(p,q)
    with threadpool_limits(limits=2):
        a=run.matrix(d,np.flatnonzero(mask),[0,1],'ridge')
        b=run.matrix(changed,np.flatnonzero(mask),[0,1],'ridge')
        assert np.array_equal(a,b)
        model=run.make_estimator('ridge',a.shape[1]);model.fit(a,d['y_5'][mask])
        restored=run.make_estimator('ridge',b.shape[1]);restored.fit(b,changed['y_5'][mask])
        assert np.array_equal(model.coef_,restored.coef_)
        path=tmp_path/'model.joblib';joblib.dump(model,path)
        pred=run.predict_chunks(model,d,assessment,[0,1],'ridge')
        replay=run.predict_chunks(joblib.load(path),d,assessment,[0,1],'ridge')
        assert np.array_equal(pred,replay)


def test_hgb_pair_context_is_categorical_and_missing_values_supported(monkeypatch):
    d=fixture();d['x'][::5,1]=np.nan;mask=run.training_mask(d,5)
    monkeypatch.setitem(run.RECIPES,'hgb',{**run.RECIPES['hgb'],'max_iter':3,'min_samples_leaf':10})
    with threadpool_limits(limits=2):
        x=run.matrix(d,np.flatnonzero(mask),[0,1],'hgb');model=run.make_estimator('hgb',x.shape[1]);model.fit(x,d['y_5'][mask])
        assert model.is_categorical_.tolist()==[False,False,True]
        pred=run.predict_chunks(model,d,np.flatnonzero(d['split']!=0),[0,1],'hgb')
        assert np.isfinite(pred).all()


def test_delay_and_missing_future_do_not_change_decisions_and_common_mask_is_score_only():
    d=fixture();assessment=np.flatnonzero(d['split']!=0)
    d['arima_5'][assessment[::4]]=np.nan
    prediction=np.where(np.arange(len(assessment))%2,3.,-3.)
    prior=np.ones(len(assessment));reports=run.score_variant(d,assessment,prediction,5,prior)
    for value in reports.values():
        a=value['primary'];b=value['one_minute_entry_delay']
        assert a['issued_forecasts']==100
        assert a['comparison_origins']<100
        for policy in a['policies']:
            assert a['policies'][policy]['decisions']==b['policies'][policy]['decisions']
            assert a['policies'][policy]['decision_pair_clock_side_sha256']==b['policies'][policy]['decision_pair_clock_side_sha256']
        own=a['own_coverage_forecast_scores_without_comparator_restriction']
        assert own['scored_forecasts']==100
    json.dumps(reports,allow_nan=False)


def test_forecast_roundtrip_preserves_signed_zero_nan_and_original_keys(tmp_path):
    (tmp_path/'forecasts').mkdir();d=fixture();idx=np.array([200,201,202])
    rec=run.save_forecast(tmp_path,'fixture',d,idx,np.array([0.,-0.,np.nan]))
    assert rec['rows']==3 and rec['sha256']==run.file_sha(tmp_path/rec['path'])


def test_baseline_fit_provenance_is_not_falsely_shared():
    selection={'rows':400}
    assert run.baseline_training_description('pair_train_mean',selection,'q')['selection']==selection
    arima=run.baseline_training_description('arima110_conditional_ols',selection,'q')
    assert arima['quote_manifest_sha256']=='q' and not arima['uses_sampled_supervised_forecast_labels']
    for name in ['no_change','momentum_exact_past_horizon','reversal_exact_past_horizon']:
        assert run.baseline_training_description(name,selection,'q')['method']=='no_fitting'


def test_different_endpoint_overlay_is_rejected_even_when_base_matches(tmp_path):
    a=tmp_path/'prepared';b=tmp_path/'quotes';a.mkdir();b.mkdir()
    pairs={str(i):{} for i in range(68)}
    (a/'PREPARED.json').write_text(json.dumps({'schema':run.SCHEMA,'status':'complete','base_sha256':'same','overlay_sha256':'one','pairs':pairs}))
    (b/'QUOTE_PANEL.json').write_text(json.dumps({'status':'complete','base_manifest_sha256':'same','endpoint_manifest_sha256':'two','pairs':pairs}))
    with pytest.raises(ValueError,match='matched_68'):run.load_inputs(a,b)
