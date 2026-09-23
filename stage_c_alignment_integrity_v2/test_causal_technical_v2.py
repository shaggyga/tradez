import json,os,sys,shutil,subprocess
from pathlib import Path
import numpy as np
import pandas as pd
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import causal_technical_operator_v2 as op
from causal_technical_adapter_v2 import contract,pair_records,FEATURES,HORIZONS,ENDPOINT_HORIZONS
from publication import RunPublisher
from causal_technical_runner_v2 import identity_for
INPUT=Path(os.environ.get('FOREX_TECHNICAL_INPUT',str(ROOT/'evidence/timed_20260922_022952/technical_adapter_step/prepared_slices_v2')))
RECIPE=ROOT/'CAUSAL_TECHNICAL_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text())
def call(action,runs,recipe=RECIPE,root=INPUT):return op.operate(action,recipe,op.sha(recipe),root,runs)
@pytest.fixture(scope='session')
def baseline(tmp_path_factory):
    supplied=os.environ.get('FOREX_TECHNICAL_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('technical-runs')
    r=call('verify' if supplied else 'run',runs);assert r['status']=='completed_verified',r
    return runs/read(RECIPE)['run_id']
def toy():
    t=np.arange(3000,dtype=np.int64)*60;mid=1+np.arange(3000)*.00001
    f=pd.DataFrame({'epoch':t,'mid':mid,'bid':mid-.0001,'ask':mid+.0001});c=contract();c.update(origin_start=120*60,origin_end=360*60,cadence_seconds=60*60,fit_cutoffs=[180*60,300*60]);return f,c

def test_future_prices_cannot_change_earlier_features():
    f,c=toy();a=pair_records('EUR_USD',f,.0001,'a'*64,c);cutoff=c['origin_start'];f.loc[f.epoch>=cutoff,['mid','bid','ask']]*=2
    b=pair_records('EUR_USD',f,.0001,'a'*64,c)
    assert a['observations'][0]==b['observations'][0]
    assert a['outcomes'][0]['value']!=b['outcomes'][0]['value']

def test_future_endpoint_deletion_keeps_origin_population():
    f,c=toy();a=pair_records('EUR_USD',f,.0001,'a'*64,c);b=pair_records('EUR_USD',f.loc[f.epoch<c['origin_end']],.0001,'a'*64,c)
    assert a['observations']==b['observations']
    assert sum(o['value'] is not None for o in a['outcomes'])>sum(o['value'] is not None for o in b['outcomes'])

def test_missing_origin_and_gap_not_filled():
    f,c=toy();f=f.loc[~f.epoch.isin([c['origin_start']-60,c['origin_start']-120])]
    a=pair_records('EUR_USD',f,.0001,'a'*64,c)
    assert a['observations'][0]['features'] is None
    assert len(a['observations'])==4 and all(o['value'] is None for o in a['outcomes'][:7])

def test_duplicate_current_and_invalid_quotes_refuse():
    f,c=toy();f=pd.concat([f,f.loc[f.epoch==c['origin_start']-60]],ignore_index=True)
    a=pair_records('EUR_USD',f,.0001,'a'*64,c);assert a['observations'][0]['features'] is None
    f,c=toy();f.loc[f.epoch==c['origin_start']-60,'ask']=0
    assert pair_records('EUR_USD',f,.0001,'a'*64,c)['observations'][0]['features'] is None

def test_exact_horizon_clocks_and_calendar_blocked():
    f,c=toy();r=pair_records('EUR_USD',f,.0001,'a'*64,c)
    for o,h in zip(r['outcomes'][:7],ENDPOINT_HORIZONS):assert o['available_epoch']==o['label_end_epoch']==c['origin_start']+h*60
    assert len(r['calendar_coverage'])==12 and all(x['status']=='blocked' for x in r['calendar_coverage'])

def test_original_feature_source_unchanged():assert op.sha(ROOT/'retained_direction_features_v1.py')==op.PREDECESSOR_SHA

def test_real_all68_and_target_support(baseline):
    r=read(baseline/'run_report.json');assert r['instruments']==68 and r['observation_rows']==5168
    assert r['outcome_rows']==36176 and r['calendar_blocked_rows']==15504 and r['feature_count']==26
    assert all(min(t['mature_training_rows'].values())>=100 for t in r['target_support'].values())

def test_actual_member_future_perturbation():
    m=read(INPUT/'SLICES_MANIFEST.json');entry=next(x for x in m['members'] if x['instrument']=='EUR_USD');f=pd.read_parquet(INPUT/entry['path']);c=contract()
    a=pair_records('EUR_USD',f,entry['pip_size'],entry['source_member_sha256'],c)
    f.loc[f.epoch>=c['fit_cutoffs'][0],['mid','bid','ask']]*=1.1
    b=pair_records('EUR_USD',f,entry['pip_size'],entry['source_member_sha256'],c)
    assert [r for r in a['observations'] if r['origin_epoch']<=c['fit_cutoffs'][0]]==[r for r in b['observations'] if r['origin_epoch']<=c['fit_cutoffs'][0]]

def test_status_and_missing_completion(tmp_path):
    assert call('status',tmp_path/'runs')['status']=='ready' and not (tmp_path/'runs').exists()
    assert call('verify',tmp_path/'runs')['status']=='review_required'

def test_source_drift_before_import(tmp_path):
    s=tmp_path/'source';s.mkdir()
    for n in op.SOURCES:shutil.copyfile(ROOT/n,s/n)
    shutil.copyfile(RECIPE,s/RECIPE.name);(s/'causal_technical_adapter_v2.py').write_text("raise RuntimeError('DO_NOT_IMPORT')\n")
    p=subprocess.run([sys.executable,'-I',str(s/'causal_technical_operator_v2.py'),'run','--recipe',str(s/RECIPE.name),'--recipe-sha256',op.sha(RECIPE),'--input-root',str(INPUT),'--runs-dir',str(tmp_path/'runs')],capture_output=True,text=True,timeout=30)
    assert p.returncode==2 and 'before_import' in p.stdout and 'DO_NOT_IMPORT' not in p.stdout+p.stderr

def test_slice_tamper(tmp_path):
    r=tmp_path/'inputs';shutil.copytree(INPUT,r);(r/'EUR_USD.parquet').write_bytes(b'changed')
    assert call('run',tmp_path/'runs',root=r)['status']=='review_required'

def test_drop_payload_from_completion_refused(baseline,tmp_path):
    r=tmp_path/baseline.name;shutil.copytree(baseline,r);p=r/'COMPLETION_MANIFEST.json';m=read(p)
    name=m['required_payloads'][0];m['required_payloads'].remove(name);m['payloads']=[x for x in m['payloads'] if x['path']!=name];p.write_text(json.dumps(m))
    assert call('verify',tmp_path)['status']=='review_required'

def test_concurrent_writer_refused(tmp_path):
    r=read(RECIPE);p=RunPublisher(tmp_path,r['run_id'],identity_for(r));p.acquire()
    try:assert call('run',tmp_path)['status']=='review_required'
    finally:p.release()

@pytest.mark.parametrize('boundary',[1,0])
def test_process_death_resume_payload_parity(baseline,tmp_path,boundary):
    p=subprocess.run([sys.executable,str(ROOT/'causal_technical_runner_v2.py'),'--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--input-root',str(INPUT),'--runs-dir',str(tmp_path),'--crash-after',str(boundary)],capture_output=True,text=True,timeout=120)
    assert p.returncode==91,p.stdout+p.stderr
    assert call('status',tmp_path)['status']=='resumable'
    assert call('resume',tmp_path)['status']=='completed_verified'
    hashes=lambda p:{x['path']:x['sha256'] for x in read(p/'COMPLETION_MANIFEST.json')['payloads']}
    assert hashes(baseline)==hashes(tmp_path/baseline.name)
