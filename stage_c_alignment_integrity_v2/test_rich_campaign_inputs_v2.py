import copy,json,os,shutil,subprocess,sys
from pathlib import Path
import numpy as np
import pandas as pd
import pytest
ROOT=Path(__file__).resolve().parent;BASE=ROOT/'evidence/timed_20260922_022952/rich_campaign_inputs_step'
INPUT=Path(os.environ.get('FOREX_RICH_INPUT',str(BASE/'inputs')))
CONTRACT=Path(os.environ.get('FOREX_RICH_CONTRACT',str(BASE/'PREPARATION_CONTRACT.json')))
LINEAGE=Path(os.environ.get('FOREX_RICH_LINEAGE',str(BASE/'LINEAGE_SUMMARY.json')))
TECHNICAL=Path(os.environ.get('FOREX_RICH_TECHNICAL',str(BASE.parent/'technical_adapter_step/runs_v2/causal-technical-inputs')))
TRAD=Path(os.environ.get('FOREX_RICH_TRAD',str(ROOT.parent/'trad')))
sys.path.insert(0,str(TRAD));sys.path.insert(0,str(ROOT))
import rich_campaign_operator_v2 as op
from rich_campaign_consumer_v2 import view,feature_sets
from rich_campaign_runner_v2 import identity_for,required,original,quotes
from rolling_registry_adapter_v2 import pair_features
from publication import RunPublisher
RECIPE=ROOT/'RICH_CAMPAIGN_INPUT_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def operate(action,runs):return op.operate(action,RECIPE,op.sha(RECIPE),INPUT,CONTRACT,LINEAGE,TECHNICAL,TRAD,runs)
@pytest.fixture(scope='module')
def baseline(tmp_path_factory):
    supplied=os.environ.get('FOREX_RICH_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('rich-baseline')
    r=operate('verify' if supplied else 'run',runs);assert r['status']=='completed_verified',r;return Path(r['run_path'])
@pytest.fixture(scope='module')
def record(baseline):return next(r for r in read(baseline/'pair_EUR_USD.json')['observations'] if r['origin_epoch']==1721606460)
def args():return {'lineage':read(LINEAGE),'legacy_names':read(TECHNICAL/'input_contract.json')['features']}

def test_all68_76_origins_actual_consumer_groups(baseline):
    r=read(baseline/'run_report.json')
    assert r['instruments']==68 and r['origin_rows']==5168 and r['feature_count']==228
    assert r['shared_legacy_eligible_rows']==4026 and r['endpoint_outcome_rows']==36176
    assert r['feature_groups']=={'legacy26':26,'compact38_cost2':40,'compact50_cost2':52,'full228_cost2':230}
    assert r['consumer_views_checked']==20672 and r['models_fitted']==0 and not r['outcomes_recomputed']
    assert len(read(baseline/'COMPLETION_MANIFEST.json')['payloads'])==210

def test_all_original_outcomes_and_observations_preserved(baseline):
    count=0
    for pair in read(RECIPE)['universe']:
        prior=read(TECHNICAL/f'pair_{pair}.json');out=read(baseline/f'outcomes_{pair}.json')
        assert out=={k:prior[k] for k in ('outcomes','path_outcomes','calendar_coverage')}
        records=read(baseline/f'pair_{pair}.json')['observations'];old={r['record_id']:r for r in prior['observations']}
        for r in records:
            assert r['original_legacy_observation']==old[r['record_id']]
            assert not {'outcomes','path_outcomes','calendar_coverage'}&set(r)
            count+=1
    assert count==5168

def test_original_eight_origin_sample_is_bit_identical(baseline):
    expected=read(ROOT/'ORIGINAL_ROLLING_SAMPLE_HASHES.json')['records'];seen={}
    for pair in read(RECIPE)['universe']:
        for r in read(baseline/f'pair_{pair}.json')['observations']:
            if r['record_id'] in expected:seen[r['record_id']]=r['original_rich_record_sha256']
    assert seen==expected and len(seen)==544

@pytest.mark.parametrize('group',['legacy26','compact38_cost2','compact50_cost2','full228_cost2'])
def test_views_use_exact_order_and_preserve_partial_values(record,group):
    params=args();v=view(record,asof=record['available_epoch'],group=group,**params)
    expected=record['original_legacy_observation']['features'] if group=='legacy26' else [record['values'][n] for n in v['feature_names'][:-2]]+record['known_entry_costs_bps']
    assert v['values']==expected and v['missing_mask']==[x is None for x in expected]
    assert not v['outcomes_included'] and not v['fit_performed']

@pytest.mark.parametrize('asof',[True,None,1721606459])
def test_asof_refusal_applies_to_legacy_and_rich(record,asof):
    for group in ['legacy26','full228_cost2']:
        with pytest.raises(ValueError,match='feature_not_available_asof'):view(record,asof=asof,group=group,**args())

def test_consumer_rejects_unknown_group_and_mutated_record(record):
    with pytest.raises(ValueError,match='registered_rich_feature_group_required'):view(record,asof=record['available_epoch'],group='future_labels',**args())
    bad=copy.deepcopy(record);bad['known_entry_costs_bps'][0]+=1
    with pytest.raises(ValueError,match='feature_record_identity_mismatch'):view(bad,asof=bad['available_epoch'],group='legacy26',**args())

def test_original_costs_are_current_wings_and_bad_quotes_remain_missing(record):
    frame=pd.read_parquet(INPUT/'EUR_USD.parquet');origin=record['origin_epoch'];row=frame.loc[frame.time==origin-60].iloc[0]
    expected=[(row.ask_close-row.close)/row.close*10000,(row.close-row.bid_close)/row.close*10000]
    assert record['known_entry_costs_bps']==expected==record['original_legacy_observation']['features'][-2:]
    bad=frame.copy();bad.loc[bad.time==origin-60,'ask_close']=0
    assert quotes(bad,[origin])[str(origin)]==[None,None]

def test_wider_prefix_future_perturbation_and_minimal_support(record):
    frame=pd.read_parquet(INPUT/'EUR_USD.parquet');origin=record['origin_epoch'];cut=origin-60
    modified=frame.astype({n:'float64' for n in frame.columns if n!='time'});modified.loc[modified.time>cut,modified.columns!='time']*=1.4
    source=record['source_member_sha256']
    def one(f):return pair_features('EUR_USD',f,.0001,source,[origin])['observations'][0]['values']
    assert one(frame)==one(modified)==one(frame.loc[(frame.time>=cut-602*60)&(frame.time<=cut)])

def test_original_dependency_consumed_bytes_guard(tmp_path):
    name='input_contract.json';shutil.copyfile(TECHNICAL/name,tmp_path/name);(tmp_path/name).write_text('{}')
    with pytest.raises(ValueError,match='original_technical_consumed_bytes_changed'):original(tmp_path,read(RECIPE),name)

def test_source_drift_before_numerical_import(tmp_path,monkeypatch):
    for n in op.SOURCES:shutil.copyfile(ROOT/n,tmp_path/n)
    (tmp_path/op.SOURCES[0]).write_text('# drift');monkeypatch.setattr(op,'ROOT',tmp_path)
    with pytest.raises(ValueError,match='rich_source_drift_before_import'):op.preflight(RECIPE,op.sha(RECIPE),INPUT,CONTRACT,LINEAGE,TECHNICAL,TRAD)

def test_one_writer_and_false_completion(tmp_path):
    p=RunPublisher(tmp_path,read(RECIPE)['run_id'],identity_for(read(RECIPE)));p.acquire()
    try:assert operate('run',tmp_path)['status']=='review_required'
    finally:p.release()
    assert operate('verify',tmp_path)['status']=='review_required'

@pytest.mark.parametrize('boundary',[1,0])
def test_real_interruption_resume_payload_identity(baseline,tmp_path,boundary):
    cmd=[sys.executable,'-I','-B',str(ROOT/'rich_campaign_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--input-root',str(INPUT),'--preparation-contract',str(CONTRACT),'--lineage',str(LINEAGE),'--technical',str(TECHNICAL),'--trad-root',str(TRAD),'--runs-dir',str(tmp_path),'--test-crash-after',str(boundary)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=600);assert p.returncode==91,p.stdout+p.stderr
    r=operate('resume',tmp_path);assert r['status']=='completed_verified',r
    for n in required(read(RECIPE)):assert (baseline/n).read_bytes()==(Path(r['run_path'])/n).read_bytes()
