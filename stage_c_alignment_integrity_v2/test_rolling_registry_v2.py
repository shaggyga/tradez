import copy,io,json,os,shutil,subprocess,sys
from pathlib import Path
import numpy as np
import pandas as pd
import pytest
ROOT=Path(__file__).resolve().parent
BASE=ROOT/'evidence/timed_20260922_022952/rolling_registry_step'
INPUT=Path(os.environ.get('FOREX_REGISTRY_INPUT',str(BASE/'inputs')))
LINEAGE=Path(os.environ.get('FOREX_REGISTRY_LINEAGE',str(BASE/'lineage_compact')))
TRAD=Path(os.environ.get('FOREX_REGISTRY_TRAD',str(ROOT.parent/'trad')))
sys.path.insert(0,str(TRAD));sys.path.insert(0,str(ROOT))
import rolling_registry_operator_v2 as op
import rolling_registry_adapter_v2 as a
from rolling_registry_runner_v2 import identity_for,required
from publication import RunPublisher
RECIPE=ROOT/'ROLLING_REGISTRY_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text(encoding='utf-8'))
@pytest.fixture(scope='module')
def baseline(tmp_path_factory):
    supplied=os.environ.get('FOREX_REGISTRY_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('registry-baseline')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUT,LINEAGE,TRAD,runs)
    assert r['status']=='completed_verified',r
    return Path(r['run_path'])
@pytest.fixture(scope='module')
def record(baseline):return read(baseline/'pair_EUR_USD.json')['observations'][1]

def test_populated_all68_original_registry_and_consumer(baseline):
    r=read(baseline/'populated_registry.json');report=read(baseline/'run_report.json')
    assert r['feature_count']==228 and r['origin_rows']==544 and len(r['columns'])==228
    assert sum(len(x['aliases']) for x in r['columns'])==13
    assert sum(x['population']['finite'] for x in r['columns'])==report['nonmissing_feature_cells']
    assert all(len(x['by_instrument'])==68 and len(x['by_utc_date'])==2 and len(x['by_utc_hour_band'])==4 for x in r['columns'])
    assert report['models_fitted']==0 and report['consumer_rows_checked']==544 and report['prior_artifacts_rehashed']==438
    assert len(read(baseline/'COMPLETION_MANIFEST.json')['payloads'])==141

def test_real_return_oracle_and_original_source_binding(record):
    raw=pd.read_parquet(INPUT/'EUR_USD.parquet').set_index('time');epoch=record['source_bar_start_epoch']
    expected=(raw.loc[epoch,'close']-raw.loc[epoch-60,'close'])/.0001
    assert record['values']['m1__return_1_pips']==expected
    m=read(INPUT/'INPUT_MANIFEST.json');row=next(x for x in m['members'] if x['instrument']=='EUR_USD')
    assert record['source_member_sha256']==row['source_member_sha256']

def test_future_perturbation_and_exact_603_bar_chunk_parity(record):
    frame=pd.read_parquet(INPUT/'EUR_USD.parquet');origin=record['origin_epoch'];epoch=origin-60
    changed=frame.astype({n:'float64' for n in a.kernel.INPUT_COLUMNS[1:]});mask=changed.time>epoch
    for n in a.kernel.INPUT_COLUMNS[1:]:changed.loc[mask,n]*=1.37
    x=a.pair_features('EUR_USD',frame,.0001,record['source_member_sha256'],[origin])['observations'][0]['values']
    y=a.pair_features('EUR_USD',changed,.0001,record['source_member_sha256'],[origin])['observations'][0]['values']
    chunk=frame.loc[(frame.time<=epoch)&(frame.time>=epoch-602*60)]
    z=a.pair_features('EUR_USD',chunk,.0001,record['source_member_sha256'],[origin])['observations'][0]['values']
    assert x==y==z

def test_duplicate_reference_is_missing_not_arbitrary_choice(record):
    frame=pd.read_parquet(INPUT/'EUR_USD.parquet');epoch=record['source_bar_start_epoch'];frame=pd.concat([frame,frame.loc[frame.time==epoch]],ignore_index=True)
    part=a.pair_features('EUR_USD',frame,.0001,record['source_member_sha256'],[record['origin_epoch']])
    assert part['quality']['duplicate_rows_excluded']==2
    assert not part['observations'][0]['exact_reference_present'] and all(v is None for v in part['observations'][0]['values'].values())

def test_peer_self_exclusion_and_exact_clock():
    epoch=1721628000;names=['EUR_USD','EUR_GBP','EUR_JPY','GBP_USD','USD_JPY']
    rows={pair:{'bar_start_epoch':epoch,'values':{f'm1__return_{h}_bps':float(i+h) for h in (1,5,15,60)}} for i,pair in enumerate(names)}
    first=a.peers.compute_panel(rows,epoch)['by_pair']['EUR_USD'];changed=copy.deepcopy(rows)
    for k in changed['EUR_USD']['values']:changed['EUR_USD']['values'][k]=1e8
    second=a.peers.compute_panel(changed,epoch)['by_pair']['EUR_USD'];assert first['values']==second['values']
    changed['EUR_USD']['bar_start_epoch']-=60
    stale=a.peers.compute_panel(changed,epoch)['by_pair']['EUR_USD'];assert stale['reason']=='asynchronous_bar_start' and all(v is None for v in stale['values'].values())

@pytest.mark.parametrize('asof',[None,True,1721606460])
def test_consumer_refuses_unavailable_asof(record,asof):
    with pytest.raises((ValueError,TypeError),match='feature_not_available_asof'):a.consume(record,asof=asof,feature_names=['m1__return_1_bps'])

@pytest.mark.parametrize('names',[[],['r1_pips'],['future_return'],['m1__return_1_bps','m1__return_1_bps']])
def test_consumer_refuses_alias_outcome_duplicate_or_empty(record,names):
    with pytest.raises(ValueError,match='unique_canonical_requested_fields_required'):a.consume(record,asof=record['available_epoch'],feature_names=names)

def test_consumer_original_missingness_and_tamper(record):
    names=list(record['values']);v=a.consume(record,asof=record['available_epoch'],feature_names=names)
    assert v['values']==list(record['values'].values()) and not v['outcomes_included'] and not v['fit_performed']
    changed=copy.deepcopy(record);changed['values'][names[0]]=123
    with pytest.raises(ValueError,match='feature_record_identity_mismatch'):a.consume(changed,asof=record['available_epoch'],feature_names=names)

def test_normalizer_arrays_prefix_and_order_are_preserved(baseline):
    lineage=read(baseline/'lineage_summary.json')
    for k in ('a','b'):
        v=lineage['normalizer_verification'][k];assert v['arrays_verified']==7 and v['shape']==[5,68,50]
        assert not v['admitted_for_current_population'] and v['reason']=='future_fit_cutoffs' and not v['normalizers_refit']
        meta=read(LINEAGE/f'normalizers_{k}.json');raw=(LINEAGE/f'normalizers_{k}.npz').read_bytes()
        with pytest.raises(ValueError,match='normalizer_order_mismatch'):a.verify_normalizer(raw,meta,list(reversed(meta['feature_names'])),meta['pair_names'],1721779260)
        bad=copy.deepcopy(meta);bad['arrays']['count']['c_order_payload_sha256']='0'*64
        with pytest.raises(ValueError,match='normalizer_array_contract_mismatch'):a.verify_normalizer(raw,bad,meta['feature_names'],meta['pair_names'],1721779260)

def test_missing_counts_are_not_zero_imputed():
    assert a.counts([None,None])['constant_status']=='insufficient_finite_support'
    r=a.counts([None,0.,0.]);assert r['finite']==2 and r['missing']==1 and r['constant_status']=='constant'
    assert a.counts([0.,1.])['nonconstant_on_observed_support']

def test_consumed_input_tamper_is_refused(tmp_path):
    row=read(INPUT/'INPUT_MANIFEST.json')['members'][0];p=tmp_path/row['path'];p.write_bytes(b'x'*row['bytes'])
    with pytest.raises(ValueError,match='input_consumed_bytes_changed'):op.checked_bytes(tmp_path,row)

def test_source_drift_preflight_before_import(tmp_path,monkeypatch):
    for n in op.SOURCES:shutil.copyfile(ROOT/n,tmp_path/n)
    (tmp_path/op.SOURCES[0]).write_text('# drift\n');monkeypatch.setattr(op,'ROOT',tmp_path)
    with pytest.raises(ValueError,match='registry_source_drift_before_import'):op.preflight(RECIPE,op.sha(RECIPE),INPUT,LINEAGE,TRAD)

def test_writer_exclusion_and_false_completion(tmp_path):
    p=RunPublisher(tmp_path,read(RECIPE)['run_id'],identity_for(read(RECIPE)));p.acquire()
    try:assert op.operate('run',RECIPE,op.sha(RECIPE),INPUT,LINEAGE,TRAD,tmp_path)['status']=='review_required'
    finally:p.release()
    assert op.operate('verify',RECIPE,op.sha(RECIPE),INPUT,LINEAGE,TRAD,tmp_path)['status']=='review_required'

@pytest.mark.parametrize('boundary',[1,0])
def test_real_process_death_exact_resume(baseline,tmp_path,boundary):
    cmd=[sys.executable,'-I','-B',str(ROOT/'rolling_registry_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--input-root',str(INPUT),'--lineage-root',str(LINEAGE),'--trad-root',str(TRAD),'--runs-dir',str(tmp_path),'--test-crash-after',str(boundary)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=180);assert p.returncode==91,p.stdout+p.stderr
    r=op.operate('resume',RECIPE,op.sha(RECIPE),INPUT,LINEAGE,TRAD,tmp_path);assert r['status']=='completed_verified',r
    for n in required(read(RECIPE)):assert (baseline/n).read_bytes()==(Path(r['run_path'])/n).read_bytes()
