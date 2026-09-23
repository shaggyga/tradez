"""Timing, masking and comparator integration for the new replication adapter."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib

import numpy as np
import pytest

from tools import run_rolling_specialists_v2 as runner
from tools import run_rolling_specialists_v1 as previous
from oanda_rolling_model_design_v1 import learner_inputs


def metadata(start='2026-03-23'):
    t = int(datetime.fromisoformat(start).replace(tzinfo=timezone.utc).timestamp()); w=7*86400
    return {'schema': runner.preparation.SCHEMA, 'status': 'complete', 'assessment_rows': 321,
            'boundaries': {'start':t,'train_end':t+6*w,'validation_end':t+7*w,'end':t+8*w},
            'fold_cutoffs_epoch':[t+i*w for i in (2,3,4,5,6)]}


def data_fixture():
    rng = np.random.default_rng(41); n=12
    params={'mean':rng.normal(size=(5,68,50)), 'scale':rng.uniform(.5,2,size=(5,68,50)),
            'supported':np.ones((5,68,50),bool),'count':np.full((5,68,50),200,np.int64)}
    raw=rng.normal(size=(n,50));raw[1,4]=np.nan;params['supported'][4,1,8]=False
    return {'raw_x':raw,'normalizers':params,'pair_id':np.array([0]*6+[1]*6,np.int16),
            'feature_names':runner.families.feature_names(),'entry_long':np.linspace(.2,.5,n),
            'entry_short':np.linspace(.8,1.3,n),'split':np.array([0,0,1,1,2,2]*2,np.int8)}


def test_date_schedule_uses_actual_earlier_window_not_old_literal_population():
    for start in ('2026-03-23','2026-05-18'):
        runner.validate_replication_inputs(metadata(start),{'status':'complete'},2)
    wrong=metadata();wrong['fold_cutoffs_epoch'][-1]+=60
    with pytest.raises(ValueError,match='prefix_cutoffs'):runner.validate_replication_inputs(wrong,{'status':'complete'},2)


@pytest.mark.parametrize('start',['2026-07-13','2026-03-24'])
def test_later_or_nonmonday_window_rejected(start):
    with pytest.raises(ValueError):runner.validate_replication_inputs(metadata(start),{'status':'complete'},2)


def test_window_lengths_and_resource_scope_checked():
    wrong=metadata();wrong['boundaries']['end']+=60
    with pytest.raises(ValueError,match='six_train'):runner.validate_replication_inputs(wrong,{'status':'complete'},2)
    with pytest.raises(ValueError,match='threads'):runner.validate_replication_inputs(metadata(),{'status':'complete'},8)


def test_full_matrices_preserve_exact_previous_helper_values():
    d=data_fixture(); rows=np.arange(12)
    for group in ('compact38','compact50'):
        for fold in range(5):
            assert np.array_equal(runner.base_matrix(d,rows,group,fold),previous.base_matrix(d,rows,group,fold),equal_nan=True)


def test_family_mask_removes_signal_and_missingness_before_model_input():
    d=data_fixture();rows=np.arange(12);original=runner.family_matrix(d,rows,'drop_returns_path')
    keep=runner.families.feature_mask('drop_returns_path');changed=deepcopy(d)
    changed['raw_x'][:,~keep]=123456.; changed['normalizers']['supported'][:,:,~keep]=False
    assert np.array_equal(original,runner.family_matrix(changed,rows,'drop_returns_path'),equal_nan=True)
    assert np.isnan(original[:,:50][:,~keep]).all()
    assert np.array_equal(original[:,-3:],np.column_stack((d['entry_long'],d['entry_short'],d['pair_id'])))
    assert np.array_equal(d['raw_x'],data_fixture()['raw_x'],equal_nan=True)


def test_context_only_has_no_technical_or_meta_context_path():
    d=data_fixture();x=runner.family_matrix(d,np.arange(12),'context_only')
    assert np.isnan(x[:,:50]).all() and np.isfinite(x[:,-3:]).all()
    assert runner.families.MEAN_ARMS==('direct','mixture_raw')
    assert runner.META_ARMS==('direct_ridge','direct_context_hgb')


def test_comparator_keeps_old_information_set_without_quote_wings():
    d=data_fixture();rows=np.arange(12)
    for learner in ('ridge','hgb'):
        for group,width in [('compact38',38),('compact50',50)]:
            expected=learner_inputs(previous.normalized_matrix(d,rows,4)[:,:width],d['pair_id'],learner,68)
            if learner=='ridge':expected=expected.astype(np.float64)
            actual=runner.comparator_matrix(d,rows,group,learner)
            assert actual.dtype==expected.dtype and np.array_equal(actual,expected,equal_nan=True)
            changed=deepcopy(d);changed['entry_long']*=100;changed['entry_short']*=100
            assert np.array_equal(actual,runner.comparator_matrix(changed,rows,group,learner),equal_nan=True)


def test_original_row_sampling_and_representatives_never_substitute_pair_ids():
    d=data_fixture();reps=runner.later_representatives(d,np.arange(12))
    assert reps.tolist()==[4,10]
    d['split'][10:]=1
    assert runner.later_representatives(d,np.arange(12)).tolist()==[4]


def test_inherited_source_hash_cannot_be_silently_replaced():
    bad=hashlib.sha256(b'changed source').hexdigest()
    with pytest.raises(ValueError,match='bound_source_changed'):
        runner.merge_source_bindings([{'tools/run_rolling_specialists_v1.py':bad}],['tools/run_rolling_specialists_v2.py'])


def test_future_rows_do_not_change_earlier_matrix_or_fixed_masks():
    d=data_fixture(); rows=np.arange(6);changed=deepcopy(d);changed['raw_x'][6:]*=500
    for group in runner.families.GROUPS:
        assert np.array_equal(runner.family_matrix(d,rows,group),runner.family_matrix(changed,rows,group),equal_nan=True)


def identity_fixture():
    b=metadata()['boundaries']
    m={'boundaries':b,'base_sha256':'base','endpoint_manifest_sha256':'endpoint',
       'pairs':{'EUR_USD':{'old_prepared_sha256':'prepared','quote_pair_sha256':'quote'}}}
    pm={'boundaries':dict(b),'base_sha256':'base','overlay_sha256':'endpoint','pairs':{'EUR_USD':{'sha256':'prepared'}}}
    qm={'boundaries':dict(b),'base_manifest_sha256':'base','endpoint_manifest_sha256':'endpoint','pairs':{'EUR_USD':{'sha256':'quote'}}}
    return m,pm,qm


def test_loader_rejects_individually_valid_but_different_period_or_endpoint():
    m,pm,qm=identity_fixture();runner.validate_input_identity(m,pm,qm)
    qm['boundaries']['end']+=60
    with pytest.raises(ValueError,match='time_boundaries'):runner.validate_input_identity(m,pm,qm)
    m,pm,qm=identity_fixture();m['endpoint_manifest_sha256']='other'
    with pytest.raises(ValueError,match='endpoint_identity'):runner.validate_input_identity(m,pm,qm)
    m,pm,qm=identity_fixture();m['pairs']['EUR_USD']['quote_pair_sha256']='other'
    with pytest.raises(ValueError,match='pair_source_identity'):runner.validate_input_identity(m,pm,qm)


def test_origin_clock_checks_boundaries_sampling_and_duplicate_minutes():
    b=metadata()['boundaries'];t=np.array([b['start'],b['train_end'],b['validation_end']],np.int64)
    d={'time':t,'split':np.array([0,1,2],np.int8),'pair_id':np.zeros(3,np.int16)}
    runner.validate_origin_clocks(d,b)
    for times,splits in ((t+1,d['split']), (t,np.array([0,1,1])),
                         (np.array([b['start']+60,*t[1:]]),d['split']),
                         (np.array([t[0],t[1],t[1]]),np.array([0,1,1]))):
        with pytest.raises(ValueError):runner.validate_origin_clocks({**d,'time':times,'split':splits},b)


def test_completion_rehash_detects_modified_input_after_successful_initial_load(tmp_path):
    roots={name:tmp_path/name for name in ('inputs','prepared','quotes','base','endpoints')}
    for path in roots.values():path.mkdir()
    def put(root,name):
        path=roots[root]/name;path.write_bytes(name.encode());return runner.file_sha(path)
    m,pm,qm=identity_fixture()
    m.update(prepared_root=str(roots['prepared']),quote_root=str(roots['quotes']),base_root=str(roots['base']),
             prepared_sha256=put('prepared','PREPARED.json'),quote_sha256=put('quotes','QUOTE_PANEL.json'))
    m['base_sha256']=pm['base_sha256']=qm['base_manifest_sha256']=put('base','DATASET.json')
    m['endpoint_manifest_sha256']=pm['overlay_sha256']=qm['endpoint_manifest_sha256']=put('endpoints','ENDPOINT_DATASET.json')
    qm['endpoint_root']=str(roots['endpoints'])
    m['pairs']['EUR_USD'].update(path='raw.npz',sha256=put('inputs','raw.npz'))
    pm['pairs']['EUR_USD'].update(path='prepared.npz',sha256=put('prepared','prepared.npz'))
    qm['pairs']['EUR_USD'].update(path='quote.parquet',sha256=put('quotes','quote.parquet'),
                                 receipt_path='receipt.json',receipt_sha256=put('quotes','receipt.json'))
    m['pairs']['EUR_USD'].update(old_prepared_sha256=pm['pairs']['EUR_USD']['sha256'],quote_pair_sha256=qm['pairs']['EUR_USD']['sha256'])
    assert runner.recheck_input_artifacts(roots['inputs'],m,pm,qm)['pair_artifacts']==4
    (roots['quotes']/'receipt.json').write_bytes(b'changed after fitting')
    with pytest.raises(ValueError):runner.recheck_input_artifacts(roots['inputs'],m,pm,qm)
