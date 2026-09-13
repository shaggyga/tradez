import copy
from datetime import datetime,timezone
from decimal import localcontext
import numpy as np
import pytest

import ma_historical_comparison_v1 as m

START=1783632000
HEADER='time,instrument,granularity,open,high,low,close,bid_open,bid_high,bid_low,bid_close,ask_open,ask_high,ask_low,ask_close\n'

def csv_bytes(count=1000,*,omit=(),gap_after=None,bad_cost=False):
    lines=[HEADER]
    for i in range(count):
        label=START+i*60+(120 if gap_after is not None and i>=gap_after else 0)
        if i in omit:continue
        price=1.1+np.sin(i*.027)*.002+i*.000001
        prices=[price,price+.0001,price-.0001,price]+[price-.00005,price+.00005,price-.00015,price-.00005]+[price+.00005,price+.00015,price-.00005,price+.00005]
        fields=[f'{p:.8f}' for p in prices]
        if bad_cost and i==count-1:fields[4]=''
        stamp=datetime.fromtimestamp(label,timezone.utc).isoformat().replace('+00:00','Z')
        lines.append(','.join([stamp,'EUR_USD','M1',*fields])+'\n')
    return ''.join(lines).encode()

def rows(count=1000,**kwargs):return m.read_rows(csv_bytes(count,**kwargs),'EUR_USD',START+count*60+1000)[0]

def test_exact_targets_sessions_grid_and_matched_family_origins():
    data=m.build_dataset(rows(1500,gap_after=700),'EUR_USD')
    assert data['inventory']['segments']==2 and data['inventory']['qualifying_segments']==2
    assert data['matrices']['ma_causal643_ridge'].shape[1]==643
    assert data['matrices']['compact_causal24_ridge'].shape[1]==24
    assert data['matrices']['compact24_plus_ma643_ridge'].shape[1]==667
    np.testing.assert_array_equal(data['matrices']['compact24_plus_ma643_ridge'][:,:24],data['matrices']['compact_causal24_ridge'])
    for h in m.HORIZONS:
        for r in data['labels'][h]:
            assert r['target_epoch']-r['reference_epoch']==h*60
            assert data['rows'][r['origin_index']]['label_epoch']%300==0
            assert r['reference_epoch']-r['segment_start_price_epoch']>=203*60
            assert (r['origin_index']<700)==(r['target_index']<700)
        assert data['counts'][h]['target_outside_contiguous_segment']>0


def test_feature_prefix_invariance_all_three_families_without_future_labels():
    original=rows(1000)
    full=m.build_dataset(original,'EUR_USD');short=m.build_dataset(original[:600],'EUR_USD')
    assert full['origins'][:len(short['origins'])]==short['origins']
    for family,x in short['matrices'].items():
        np.testing.assert_array_equal(x,full['matrices'][family][:len(x)])


def test_strict_shared_timeline_cutoffs_and_purges():
    r=rows(1000);cut=m.split_cutoffs(r)
    assert cut==m.split_cutoffs(r)
    for h in m.HORIZONS:
        assert m.split_name(cut[0]-h*60,cut[0],cut)=='purged_train_overlap'
        assert m.split_name(cut[1]-h*60,cut[1],cut)=='purged_validation_overlap'
        assert m.split_name(cut[0],cut[0]+h*60,cut)=='validation'
        assert m.split_name(cut[1],cut[1]+h*60,cut)=='test'
    data=m.build_dataset(r,'EUR_USD')
    for h,entries in data['labels'].items():
        for entry in entries:
            if entry['split']=='train':assert entry['target_epoch']<cut[0]
            if entry['split']=='validation':assert cut[0]<=entry['reference_epoch'] and entry['target_epoch']<cut[1]


def test_gap_removes_rows_and_does_not_reset_into_imputed_history():
    r=rows(700,omit=(300,))
    assert len(r)==699 and [b-a for a,b in m.segments(r)]==[300,399]
    data=m.build_dataset(r,'EUR_USD')
    assert data['inventory']['warmed_rows']==293
    assert all(o['reference_epoch'] if 'reference_epoch' in o else o['price_epoch'] for o in data['origins'])
    assert all(not(START+301*60<=o['label_epoch']<START+504*60) for o in data['origins'])


@pytest.mark.parametrize('change,reason',[
    (lambda raw:raw[:-1],'source_byte_bound_or_partial_tail'),
    (lambda raw:raw+raw.splitlines(keepends=True)[-1],'source_duplicate_or_unordered'),
    (lambda raw:raw.replace(b'EUR_USD',b'GBP_USD'),'source_row_identity'),
    (lambda raw:raw.replace(b',M1,',b',M5,'),'source_row_identity'),
    (lambda raw:raw.replace(b'Z,EUR_USD',b',EUR_USD'),'source_UTC_minute'),
    (lambda raw:raw.replace(b'1.10000000',b'NaN',1),'invalid_price_text'),
])
def test_raw_identity_time_price_and_tail_fail_closed(change,reason):
    with pytest.raises(ValueError,match=reason):m.read_rows(change(csv_bytes(20)),'EUR_USD',START+10000)


def test_future_complete_bar_rejected_against_actual_capture_clock():
    with pytest.raises(ValueError,match='source_future_at_actual_observation'):
        m.read_rows(csv_bytes(20),'EUR_USD',START+19*60)


def test_missing_bidask_does_not_remove_mid_feature_or_invent_cost():
    r,counts=m.read_rows(csv_bytes(500,bad_cost=True),'EUR_USD',START+500*60)
    assert len(r)==500 and counts=={'bid_ask_OHLC_unavailable':1}
    assert r[-1]['bid_close'] is None
    data=dict(rows=r)
    c=m.cost_rows(data,[dict(origin_index=400,target_index=499)],[1.])
    assert c==[dict(side=1,net_bps=None,reason='bid_ask_OHLC_unavailable')]


def test_target_math_and_cost_not_ambient_decimal_precision():
    r=rows(1000);data=dict(rows=r);records=[dict(origin_index=300,target_index=330)]
    expected=m.targets(data,records);cost=m.cost_rows(data,records,[1.])
    with localcontext() as context:
        context.prec=3
        np.testing.assert_array_equal(m.targets(data,records),expected)
        assert m.cost_rows(data,records,[1.])==cost


def test_train_only_scaler_and_independent_standardized_coefficient_formula():
    rng=np.random.default_rng(27)
    x=rng.normal(size=(100,5));x[:,4]=7;y=x[:,0]*.3-x[:,2]+.1
    v=rng.normal(size=(30,5));v[:,4]=999;vy=v[:,0]*.3-v[:,2]+.1
    model=m.select_model(x,y,v,vy,alphas=(1.,10.))
    np.testing.assert_allclose(model['mean'],np.mean(x,axis=0))
    assert model['scale'][4]==1 and model['constant_training_features']==1
    z=(x-np.array(model['mean']))/np.array(model['scale']);centered=z-z.mean(axis=0)
    beta=np.linalg.solve(centered.T@centered+model['alpha']*np.eye(5),centered.T@(y-y.mean()))
    np.testing.assert_allclose(model['coefficients'],beta,rtol=1e-12,atol=1e-12)
    np.testing.assert_allclose(m.predict(model,v),((v-model['mean'])/model['scale'])@beta+y.mean(),rtol=1e-12,atol=1e-12)


def test_alpha_tie_is_predeclared_larger_and_no_test_data_argument():
    x=np.zeros((10,2));y=np.zeros(10)
    model=m.select_model(x,y,x[:3],y[:3],alphas=(.1,1.,100.))
    assert model['alpha']==100.
    import inspect
    assert list(inspect.signature(m.select_model).parameters)==['x_train','y_train','x_validation','y_validation','alphas']


def test_cost_long_short_and_neutral_are_separate_from_direction_mae():
    data=dict(rows=[dict(close_text='1.1',bid_close='1.09',ask_close='1.11',cost_unavailable_reason=None),
                    dict(close_text='1.2',bid_close='1.19',ask_close='1.21',cost_unavailable_reason=None)])
    records=[dict(origin_index=0,target_index=1)]*3
    c=m.cost_rows(data,records,np.array([1.,-1.,0.]))
    np.testing.assert_allclose([c[0]['net_bps'],c[1]['net_bps']],[.08/1.1*10000,-.12/1.1*10000])
    assert c[2]['net_bps'] is None and c[2]['reason']=='neutral_no_position'
    result=m.metrics(np.array([1.,0.,-1.]),np.array([1.,2.,0.]),c)
    assert result['rows']==3 and result['direction_denominator']==1 and result['direction_correct']==1
    assert result['brier'] is None and result['probability_available'] is False


@pytest.mark.parametrize('days,has_interval',[(9,False),(10,True),(15,True)])
def test_date_blocks_not_rows_control_interval(days,has_interval):
    records=[dict(reference_epoch=START+d*86400+i*300) for d in range(days) for i in range(4)]
    actual=np.arange(len(records))*.01;model=actual+.2;baseline=actual+.5
    cost=[dict(net_bps=None,reason='neutral_no_position')]*len(records)
    result=m.paired_dependence(records,actual,model,baseline,cost,cost)
    assert result['UTC_date_count']==days
    assert (result['MAE_improvement_interval95'] is not None)==has_interval
    if has_interval:
        ci=result['MAE_improvement_interval95'];assert ci['low']==pytest.approx(.3) and ci['high']==pytest.approx(.3)
        assert m.paired_dependence(records,actual,model,baseline,cost,cost)==result
    assert result['used_for_model_selection'] is False and result['independent_sample_size'] is None


def test_zero_baseline_direction_undefined_but_no_position_cost_zero():
    records=[dict(reference_epoch=START)]
    result=m.paired_dependence(records,np.array([1.]),np.array([.5]),np.array([0.]),
        [dict(net_bps=-1.,reason=None)],[dict(net_bps=None,reason='neutral_no_position')])
    d=result['per_date'][0]
    assert d['direction_paired_denominator']==0 and d['direction_accuracy_delta'] is None
    assert d['cost_paired_denominator']==1 and d['mean_cost_delta_bps']==-1.


def test_prespecified_three_families_and_inert_no_probability_policy():
    assert m.POLICY['families']==['ma_causal643_ridge','compact_causal24_ridge','compact24_plus_ma643_ridge']
    assert m.POLICY['refit_after_validation'] is False and m.POLICY['probability_model'] is None
    assert m.POLICY['minimum_rows']=={'train':256,'validation':64,'test':64}
    assert all(m.POLICY[k] is v for k,v in m.FLAGS.items())


@pytest.mark.parametrize('existed',[True,False])
def test_failed_cli_preserves_existing_output_and_only_retains_new_partial(tmp_path,monkeypatch,existed):
    target=tmp_path/'result'
    if existed:target.mkdir();(target/'prior.json').write_text('{}')
    monkeypatch.setattr(m,'BASE',tmp_path)
    def failed(*args):
        if not existed:target.mkdir()
        raise ValueError('fixture_failure')
    monkeypatch.setattr(m,'run',failed)
    monkeypatch.setattr(m.sys,'argv',['compare','run','--design',str(tmp_path/'design.json'),'--output',str(target)])
    with pytest.raises(ValueError,match='fixture_failure'):m.main()
    assert (target/'COMPARISON_FAILED.json').exists() is not existed
    if existed:assert list(p.name for p in target.iterdir())==['prior.json']


def test_selection_write_flushes_then_fsyncs_before_return_and_never_overwrites(tmp_path,monkeypatch):
    calls=[];original=m.os.fsync
    def fsync(fd):
        calls.append(m.os.fstat(fd).st_size)
        original(fd)
    monkeypatch.setattr(m.os,'fsync',fsync)
    path=tmp_path/'PRE_TEST_SELECTIONS.json'
    result=m.write_new(path,{'final_test_evaluated':False})
    assert calls==[path.stat().st_size] and calls[0]>0
    before=path.read_bytes()
    with pytest.raises(FileExistsError):m.write_new(path,{'changed':True})
    assert path.read_bytes()==before and m.sha(before)==result['sha256']
