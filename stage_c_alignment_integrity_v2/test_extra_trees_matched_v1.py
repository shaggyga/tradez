import copy,hashlib,json
from unittest import mock
import numpy as np
import pytest
import extra_trees_matched_v1 as et
import extra_trees_operator_v1 as operator
from contracts import TrainingView,fingerprint
from publication import RunPublisher,effective_run_identity

def sample():
    c=et.contract();c.update(training_start=100,minimum_training_rows=4)
    target={'target_id':'technical_endpoint_midpoint_elapsed_15m','horizon_seconds':900}
    obs=[{'record_id':str(i),'instrument':'EUR_USD','origin_epoch':100+i*10,'available_epoch':100+i*10,
          'features':[float(i+j)/20 for j in range(len(et.FEATURES))]} for i in range(12)]
    out=[{'record_id':str(i),'target_id':target['target_id'],'value':float(i%4),'available_epoch':1000+i*10,'label_end_epoch':1000+i*10} for i in range(12)]
    view=TrainingView(100,1500,1500,1500,c['evaluation_asof'])
    selected=et.population(obs,out,target,view)
    baseline={'training_population_sha256':fingerprint(selected),'target':target,'fit_cutoff':1500,'fit_id':'base'}
    params={'n_estimators':3,'random_state':37,'n_jobs':1,'min_samples_leaf':2}
    return obs,out,baseline,c,params

def test_future_labels_do_not_change_fit_and_saved_inference(tmp_path):
    obs,out,b,c,p=sample();meta,weights=et.fit_one(obs,out,b,15,1500,c,p)
    future=[{'record_id':'future','target_id':b['target']['target_id'],'value':999999.,'available_epoch':3000,'label_end_epoch':3000}]
    again=et.fit_one(obs,out+future,b,15,1500,c,p)
    assert meta==again[0] and weights==again[1]
    rows=[dict(obs[0],origin_epoch=1600,available_epoch=1600)]
    a=et.issue(meta,weights,rows,procedure='frozen',c=c)
    path=tmp_path/'model';path.write_bytes(weights)
    assert a==et.issue(json.loads(et.encoded(meta)),path.read_bytes(),rows,procedure='frozen',c=c)

def test_population_and_model_corruption_refused():
    obs,out,b,c,p=sample()
    with pytest.raises(ValueError,match='population'):et.fit_one(obs,out,dict(b,training_population_sha256='bad'),15,1500,c,p)
    meta,weights=et.fit_one(obs,out,b,15,1500,c,p)
    with pytest.raises(ValueError,match='bytes_changed'):et.validate_fit(meta,weights+b'x')

def test_ready_and_input_clocks_keep_coverage():
    obs,out,b,c,p=sample();meta,weights=et.fit_one(obs,out,b,15,1500,c,p)
    rows=[dict(obs[0],origin_epoch=1520),dict(obs[1],origin_epoch=1600,available_epoch=1601),dict(obs[2],origin_epoch=1600,features=None)]
    f,cov=et.issue(meta,weights,rows,procedure='frozen',c=c)
    assert not f and [x['reason'] for x in cov]==['model_not_ready','feature_not_ready','missing_features']

@pytest.mark.parametrize('orphan',[False,True])
def test_fit_bundle_restart_does_not_refit_after_export_crash(tmp_path,orphan):
    obs,out,b,c,p=sample();fitted=et.fit_one(obs,out,b,15,1500,c,p)
    ident=effective_run_identity(contract={'test':'crash'},dependency_hashes={'fixture':'a'*64})
    pub=RunPublisher(tmp_path,'fixture',ident);pub.acquire()
    expected={'fit_cutoff':1500,'baseline_fit_id':'base'}
    call=mock.Mock(return_value=fitted)
    a=et.fit_transaction(pub,'fit',call,expected,lambda *args:None)
    pub.release()
    if orphan:
        j=json.loads(pub.journal_path.read_text());j['payloads']={};pub.journal_path.write_bytes(et.encoded(j))
    resumed=RunPublisher(tmp_path,'fixture',ident);resumed.acquire(recover=True)
    try:
        got=et.fit_transaction(resumed,'fit',call,expected,lambda *args:None)
        assert call.call_count==1 and a==got
        with pytest.raises(ValueError,match='context_mismatch'):
            et.fit_transaction(resumed,'fit',call,{'fit_cutoff':1600},lambda *args:None)
    finally:resumed.release()

def score_fixture():
    c=et.contract();new=[];old=[];out=[]
    for h in c['horizon_minutes']:
        target=f'technical_endpoint_midpoint_elapsed_{h}m'
        for procedure in c['procedures']:
            rid=str(h)+procedure
            f={'target_id':target,'decision_epoch':2000,'available_epoch':2002,'instrument':'EUR_USD','prediction':2.}
            new.append({'record_id':rid,'procedure':procedure,'forecast':f})
            old.extend({'record_id':rid,'procedure':procedure,'method':m,'forecast':dict(f,prediction=0.)} for m in et.CONTROLS)
            out.append({'record_id':rid,'target_id':target,'value':3.,'available_epoch':3000})
    return new,old,out,c

def test_matched_scores_hand_calculation_and_duplicate_refusal():
    n,b,o,c=score_fixture();scores=et.paired_scores(n,b,o,c)
    overall=[s for s in scores if s['stratum']=='overall'];assert len(overall)==14
    assert all(s['new_mae_bps']==1 and s['paired_controls']['zero']['mae_delta_bps']==-2 for s in overall)
    for args,reason in [((n+n[:1],b,o,c),'duplicate_new'),((n,b,o+o[:1],c),'duplicate_outcome'),((n,b[1:],o,c),'missing_matched')]:
        with pytest.raises(ValueError,match=reason):et.paired_scores(*args)
    b[0]['forecast']['available_epoch']+=1
    with pytest.raises(ValueError,match='control_identity'):et.paired_scores(n,b,o,c)

def test_frozen_qualification_refused_before_parent_read(tmp_path):
    q=tmp_path/'q.json';q.write_text('{}')
    with pytest.raises(ValueError,match='qualification_pin'):et.run(tmp_path,tmp_path,q,tmp_path)

@pytest.mark.parametrize('key,value,reason',[('elapsed_seconds',601,'total_time'),('rss_bytes',2**31+1,'rss'),('output_bytes',2**29+1,'output'),('free_bytes',1,'free_disk'),('phase_elapsed',31,'phase_time')])
def test_resource_refusals(key,value,reason):
    sample={'elapsed_seconds':1,'rss_bytes':1,'output_bytes':1,'free_bytes':2**34,'phase_elapsed':1,'phase_limit':30}
    limits={'max_seconds':600,'max_rss_bytes':2**31,'max_output_bytes':2**29,'min_free_disk_bytes':2**33}
    assert operator.violation(sample,limits) is None
    sample[key]=value;assert operator.violation(sample,limits)==reason

def test_stop_worker_targets_only_owned_descendants():
    root=mock.Mock();child=mock.Mock();grandchild=mock.Mock()
    root.children.return_value=[child,grandchild]
    operator.stop_worker(root)
    root.children.assert_called_once_with(recursive=True)
    for p in [root,child,grandchild]:p.kill.assert_called_once_with()

def test_restore_read_guard_refuses_original_dependency(tmp_path):
    import subprocess,sys
    source=tmp_path/'original';source.mkdir();(source/'value').write_text('original')
    code='import sys;sys.path.insert(0,sys.argv[1]);import extra_trees_operator_v1 as op;op.deny_reads([sys.argv[2]]);open(sys.argv[2]+"/value").read()'
    result=subprocess.run([sys.executable,'-I','-B','-c',code,str(operator.ROOT),str(source)],capture_output=True,text=True,timeout=10)
    assert result.returncode!=0 and 'original_dependency_access_refused' in result.stderr
