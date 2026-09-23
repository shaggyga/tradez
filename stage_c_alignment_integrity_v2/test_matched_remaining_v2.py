import copy,json,os,sys,shutil,subprocess
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import matched_remaining_operator_v2 as op
from matched_remaining_native_v2 import ORIGIN,TARGET,EPOCHS,NEW_MINUTES,REUSE_MINUTES,prepared_packets,validate_packets
from matched_remaining_runner_v2 import identity_for
from contracts import fingerprint
from publication import RunPublisher
BASE=ROOT/'evidence/timed_20260922_022952'
SLICES=Path(os.environ.get('FOREX_REMAINING_SLICES',str(BASE/'technical_adapter_step/prepared_slices_v2')))
TECHNICAL=Path(os.environ.get('FOREX_REMAINING_TECHNICAL',str(BASE/'technical_adapter_step/runs_v2/causal-technical-inputs')))
MATCHED=Path(os.environ.get('FOREX_REMAINING_MATCHED',str(BASE/'matched_campaign_step/runs/matched-development-campaign')))
TRAD=Path(os.environ.get('FOREX_REMAINING_TRAD',str(ROOT.parent/'trad')))
RECIPE=ROOT/'MATCHED_REMAINING_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text())
def call(action,runs,recipe=RECIPE,matched=MATCHED):return op.operate(action,recipe,op.sha(recipe),SLICES,TECHNICAL,matched,TRAD,runs)
@pytest.fixture(scope='session')
def baseline(tmp_path_factory):
    supplied=os.environ.get('FOREX_REMAINING_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('remaining-native-runs');r=call('verify' if supplied else 'run',runs);assert r['status']=='completed_verified',r
    return runs/read(RECIPE)['run_id']
@pytest.fixture(scope='session')
def first(baseline):
    meta=read(baseline/'fit_2880.json');tree=(baseline/'fit_2880.joblib').read_bytes();refs=read(baseline/'references.json')[str(ORIGIN)]
    obs=[o for pair in read(RECIPE)['universe'] for o in read(TECHNICAL/('pair_'+pair+'.json'))['observations'] if o['origin_epoch']==ORIGIN]
    packets,_=prepared_packets(meta,tree,obs,refs,trad_root=TRAD);return meta,tree,obs,refs,packets

def test_all68_coverage_and_real_reuse(baseline):
    r=read(baseline/'run_report.json');cov=read(baseline/'coverage.json')
    assert r['new_models_fitted']==10 and r['reused_models']==6 and r['coverage_rows']==2176
    assert len({x['instrument'] for x in cov})==68 and len({x['decision_epoch'] for x in cov})==8
    assert r['native_packets']==sum(x['reason']=='eligible' and x['method'] in ('ridge','recovered_hgb') for x in cov)
    for h in REUSE_MINUTES:
        for suffix in ('.json','.joblib'):assert (baseline/('fit_'+str(h)+suffix)).read_bytes()==(MATCHED/(f'fit_{h}_1721606400'+suffix)).read_bytes()

def test_original_target_and_fresh_conditioning(baseline):
    packets=read(baseline/'native_packets.json')
    assert {p['method'] for p in packets}=={'ridge','recovered_hgb'}
    for p in packets:
        node=p['prepared_curve']['nodes'][0];f=p['forecast']['forecast']
        assert node['original_target_epoch']==TARGET and p['original_target_epoch']==TARGET
        assert node['horizon_sec']==TARGET-p['conditioning_epoch'] and f['decision_epoch']==p['conditioning_epoch']
        assert p['prepared_curve']['scope']=='engineering_replay' and not p['observed_publication'] and not p['observed_execution']
        assert f['available_epoch']==f['decision_epoch']+2

def test_all_fits_mature_before_original_origin(baseline):
    for h in NEW_MINUTES+REUSE_MINUTES:
        m=read(baseline/('fit_'+str(h)+'.json'));assert m['maximum_outcome_available_epoch']<=m['fit_cutoff']<ORIGIN and m['ready_epoch']<=ORIGIN
        assert m['target']['horizon_seconds']==h*60 and m['training_population_sha256']==m['ridge']['training_population_sha256']

def test_native_packets_reproduce(first):
    meta,tree,obs,refs,p=first;assert validate_packets(p,meta,tree,obs,refs,trad_root=TRAD)

@pytest.mark.parametrize('field',['prediction','target','method'])
def test_rehashed_native_tamper_refused(first,field):
    meta,tree,obs,refs,original=first;p=copy.deepcopy(original)
    if field=='prediction':p[0]['forecast']['forecast']['prediction']+=1
    elif field=='target':p[0]['original_target_epoch']+=60
    else:p[0]['method']='unapproved_method'
    p[0]['packet_sha256']=fingerprint({k:v for k,v in p[0].items() if k!='packet_sha256'})
    with pytest.raises(ValueError,match='recomputation'):validate_packets(p,meta,tree,obs,refs,trad_root=TRAD)

def test_wrong_exact_remaining_model_refused(first,baseline):
    _,_,obs,refs,_=first
    with pytest.raises(ValueError,match='exact_remaining'):prepared_packets(read(baseline/'fit_720.json'),(baseline/'fit_720.joblib').read_bytes(),obs,refs,trad_root=TRAD)

def test_future_reference_clock_refused(first):
    meta,tree,obs,refs,_=first;changed=copy.deepcopy(refs);pair=next(iter(changed));changed[pair]['price_epoch']+=60;changed[pair]['record_sha256']=fingerprint({k:v for k,v in changed[pair].items() if k!='record_sha256'})
    with pytest.raises(ValueError,match='reference_binding'):prepared_packets(meta,tree,obs,changed,trad_root=TRAD)

def test_status_and_verify_incomplete(tmp_path):
    assert call('status',tmp_path/'runs')['status']=='ready' and not (tmp_path/'runs').exists()
    assert call('verify',tmp_path/'runs')['status']=='review_required'

def test_source_drift_before_import(tmp_path):
    s=tmp_path/'source';s.mkdir()
    for n in op.SOURCES:shutil.copyfile(ROOT/n,s/n)
    shutil.copyfile(RECIPE,s/RECIPE.name);(s/'matched_remaining_native_v2.py').write_text("raise RuntimeError('DO_NOT_IMPORT')\n")
    command=[sys.executable,'-I',str(s/'matched_remaining_operator_v2.py'),'run','--recipe',str(s/RECIPE.name),'--recipe-sha256',op.sha(RECIPE),'--slices',str(SLICES),'--technical',str(TECHNICAL),'--matched',str(MATCHED),'--trad-root',str(TRAD),'--runs-dir',str(tmp_path/'runs')]
    p=subprocess.run(command,capture_output=True,text=True,timeout=30)
    assert p.returncode==2 and 'before_import' in p.stdout and 'DO_NOT_IMPORT' not in p.stdout+p.stderr

def test_model_dependency_tamper(tmp_path):
    m=tmp_path/'matched';shutil.copytree(MATCHED,m);(m/'fit_2880_1721606400.joblib').write_bytes(b'changed')
    assert call('run',tmp_path/'runs',matched=m)['status']=='review_required'

def test_native_output_tamper(baseline,tmp_path):
    r=tmp_path/baseline.name;shutil.copytree(baseline,r);(r/'native_packets.json').write_text('[]');assert call('verify',tmp_path)['status']=='review_required'

def test_writer_exclusion(tmp_path):
    r=read(RECIPE);p=RunPublisher(tmp_path,r['run_id'],identity_for(r));p.acquire()
    try:assert call('run',tmp_path)['status']=='review_required'
    finally:p.release()

@pytest.mark.parametrize('boundary',[1,0])
def test_process_death_and_scientific_parity(baseline,tmp_path,boundary):
    command=[sys.executable,str(ROOT/'matched_remaining_runner_v2.py'),'--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--slices',str(SLICES),'--technical',str(TECHNICAL),'--matched',str(MATCHED),'--trad-root',str(TRAD),'--runs-dir',str(tmp_path),'--crash-after',str(boundary)]
    p=subprocess.run(command,capture_output=True,text=True,timeout=180);assert p.returncode==91,p.stdout+p.stderr
    assert call('status',tmp_path)['status']=='resumable' and call('resume',tmp_path)['status']=='completed_verified'
    hashes=lambda p:{x['path']:x['sha256'] for x in read(p/'COMPLETION_MANIFEST.json')['payloads'] if x['path']!='fit_resources.json'}
    assert hashes(baseline)==hashes(tmp_path/baseline.name)
