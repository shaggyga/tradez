import copy
import json
import os
from pathlib import Path
import sys

import numpy as np
import pytest
import path_risk_development_comparison_v1 as comparison
from test_oanda_m1_path_risk_labels_v1 import records,raw,START

def parsed(n):
    return comparison.labels.parse_csv(raw(records(n)),'EUR_USD',observed_epoch=START+(n+1)*60)[0]

def test_train_only_empirical_and_scaled_coefficients():
    y=np.array([1.,2.,3.,4.,10.]);s=np.array([1.,2.,3.,4.,10.])
    model=comparison.fit_quantiles(y,s)
    assert model['empirical']==[1,3,10]
    assert model['volatility_normalized']==[1,1,1]
    pred=comparison.predictions(model,[2,100])
    assert pred['training_empirical'].tolist()==[[1,3,10],[1,3,10]]
    assert pred['training_empirical_volatility_scaled'].tolist()==[[2,2,2],[100,100,100]]
    assert model['training_rows']==5

def test_validation_or_future_values_never_enter_fit():
    train=np.array([1.,2.,3.,4.]);scale=np.ones(4)
    model=comparison.fit_quantiles(train,scale);before=copy.deepcopy(model)
    future=np.array([1e9,-1e9])
    comparison.scores(future,comparison.predictions(model,[1,1])['training_empirical'])
    assert model==before

@pytest.mark.parametrize('scale',[[0,1],[-1,1],[float('nan'),1],[float('inf'),1]])
def test_scale_never_floored_or_imputed(scale):
    with pytest.raises(ValueError,match='positive_training_support'):
        comparison.fit_quantiles([1,2],scale)

def test_train_target_overlap_purge_exact_boundary():
    assert comparison.block_name(100,199,(200,400))=='train'
    assert comparison.block_name(100,200,(200,400))=='purged_train_overlap'
    assert comparison.block_name(200,399,(200,400))=='validation'
    assert comparison.block_name(200,400,(200,400))=='purged_validation_overlap'
    assert comparison.block_name(400,500,(200,400))=='test'

def test_dataset_same_grid_warmup_and_session_target():
    rows=parsed(700)
    for r in rows[350:]:r['label_epoch']+=86400*2;r['price_epoch']+=86400*2
    data=comparison.dataset(rows)
    assert data['inventory']['segments']==2
    for h,recs in data['records'].items():
        for r in recs:
            assert r['reference_label_epoch']%300==0
            assert r['origin_global_row'] in range(203,350) or r['origin_global_row']>=553
            assert r['target_epoch']-r['reference_epoch']==h*60
            assert r['target_global_row']<350 if r['origin_global_row']<350 else r['target_global_row']>=350
            assert r['target_global_row']==r['origin_global_row']+h

def test_fixed_cutoffs_depend_only_full_time_range():
    rows=parsed(700);expected=comparison.cutoffs(rows)
    for r in rows:r['mid']['close']='10'
    assert comparison.cutoffs(rows)==expected

def test_label_invalid_does_not_remove_other_label_values():
    rows=parsed(700)
    for r in rows[300:320]:r['bid']=r['ask']=None
    data=comparison.dataset(rows)
    affected=[r for r in data['records'][15] if 290<=r['origin_global_row']<=320]
    assert affected
    assert all(r['values']['signed_terminal_bps'] is not None for r in affected)
    assert any(r['values']['long_close_MAE_bps'] is None for r in affected)

def test_scores_pinball_quantile_coverage_and_width_hand_calculation():
    score=comparison.scores([0,10],np.array([[1,2,3],[1,2,3]],float))
    # q=.1: .9 and .9; q=.5: 1 and4; q=.9: .3 and6.3.
    assert score['mean_pinball_by_quantile']==pytest.approx([.9,2.5,3.3])
    assert score['median_MAE_bps']==5
    assert score['interval_coverage']==0 and score['mean_interval_width_bps']==2
    assert score['lower_miss_rate']==.5 and score['upper_miss_rate']==.5
    assert score['upper_tail_excess_mean_bps']==3.5

def test_no_quantile_crossing_allowed():
    with pytest.raises(ValueError,match='crossing'):
        comparison.scores([1],[[2,1,3]])

def test_date_dependence_uses_whole_dates_and_no_nine_date_CI():
    rows=[{'reference_epoch':START+i*86400} for i in range(9)]
    y=np.arange(9,dtype=float);pred={'training_empirical':np.zeros((9,3)),
        'training_empirical_volatility_scaled':np.ones((9,3))}
    result=comparison.paired_dates(rows,y,pred)
    assert result['UTC_date_count']==9
    assert result['scaled_pinball_improvement_interval95'] is None
    assert result['independent_sample_size'] is None and result['used_for_selection'] is False

def test_date_resample_reproducible_at_ten_dates():
    rows=[{'reference_epoch':START+i*86400} for i in range(10)]
    y=np.arange(10,dtype=float);pred={'training_empirical':np.zeros((10,3)),
        'training_empirical_volatility_scaled':np.ones((10,3))}
    first=comparison.paired_dates(rows,y,pred);second=comparison.paired_dates(rows,y,pred)
    assert first==second
    assert first['scaled_pinball_improvement_interval95']['resamples']==1000

def test_write_new_fsyncs_before_return_and_cannot_overwrite(tmp_path,monkeypatch):
    calls=[];original=os.fsync
    def fsync(fd):calls.append(fd);original(fd)
    monkeypatch.setattr(os,'fsync',fsync)
    path=tmp_path/'pre_scores.json'
    binding=comparison.write_new(path,{'training':True})
    assert len(calls)==1 and json.loads(path.read_bytes())=={'training':True}
    assert binding['sha256']==comparison.sha(path.read_bytes())
    with pytest.raises(FileExistsError):comparison.write_new(path,{'training':False})

def test_policy_marks_seen_history_and_no_selection_or_orders():
    policy=comparison.POLICY
    assert 'after_MA_final_period_inspection' in policy['evidence_class']
    assert policy['selected_for_promotion'] is False
    assert policy['can_place_orders'] is False
    assert policy['methods']==['training_empirical','training_empirical_volatility_scaled']
    assert policy['quantiles']==[.1,.5,.9]

def test_changed_protocol_cannot_be_attributed_to_existing_fits(tmp_path):
    p=tmp_path/'protocol.json';p.write_text(json.dumps({'created_epoch':100,'policy':'original'}))
    plan,identity=comparison.retained_protocol(p,200)
    p.write_text(json.dumps({'created_epoch':100,'policy':'changed'}))
    assert plan['policy']=='original'
    with pytest.raises(ValueError,match='frozen_protocol_changed'):
        comparison.verify_protocol_identity(identity)

@pytest.mark.parametrize('created',[201,0,True,float('nan'),float('inf')])
def test_protocol_clock_must_be_finite_actual_and_before_run(tmp_path,created):
    p=tmp_path/'protocol.json';p.write_text(json.dumps({'created_epoch':created}))
    with pytest.raises(ValueError,match='protocol_not_frozen_before_run'):
        comparison.retained_protocol(p,200)
