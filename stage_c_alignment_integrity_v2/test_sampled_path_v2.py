import copy,importlib.abc,json,os,shutil,subprocess,sys
from pathlib import Path
import numpy as np,pandas as pd,pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import sampled_path_operator_v2 as op
from sampled_path_adapter_v2 import pair_records,asof,contract
from causal_technical_adapter_v2 import clean
from sampled_path_runner_v2 import identity_for,required,checked
from publication import RunPublisher
E=ROOT/'evidence/timed_20260922_022952';T=E/'technical_adapter_step'
INPUT=Path(os.environ.get('FOREX_PATH_INPUT',str(T/'prepared_slices_v2')))
TECH=Path(os.environ.get('FOREX_PATH_TECHNICAL',str(T/'runs_v2/causal-technical-inputs')))
RECIPE=ROOT/'SAMPLED_PATH_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def call(action,runs):return op.operate(action,RECIPE,op.sha(RECIPE),INPUT,TECH,runs)
@pytest.fixture(scope='session')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_PATH_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('sampled-path')
    r=call('verify' if supplied else 'run',runs);assert r['status']=='completed_verified',r;return Path(r['run_path'])

BASE=1700000040
def fixture(prices=(100.,101.,102.,99.,103.),horizons=(1,2,3),frame=None):
    raw=frame if frame is not None else pd.DataFrame({'epoch':BASE+np.arange(len(prices))*60,'mid':prices,'bid':np.array(prices)-.1,'ask':np.array(prices)+.1})
    c={**contract(),'origin_start':BASE+60,'origin_end':BASE+61,'cadence_seconds':60,'horizon_minutes':list(horizons),'coverage_end_epoch':BASE+len(prices)*60}
    cleaned,_=clean(raw);lookup={int(r.epoch):float(r.mid) for r in cleaned.itertuples()};out=[];paths=[]
    for h in horizons:
        value=(lookup[BASE+h*60]/lookup[BASE]-1)*10000 if BASE in lookup and BASE+h*60 in lookup else None
        row={'record_id':f'EUR_USD:{BASE+60}','target_id':f'technical_endpoint_midpoint_elapsed_{h}m','value':value};out.append(row)
        paths.append({**row,'target_id':f'technical_contiguous_midpoint_elapsed_{h}m','value':value if all(BASE+j*60 in lookup for j in range(h+1)) else None})
    old={'outcomes':out,'path_outcomes':paths};return raw,old,c
def part(args):return pair_records('EUR_USD',args[0],args[1],'0'*64,args[2])

def test_long_short_close_formulas_and_signed_extrema():
    p=part(fixture());x=p['rows'][2]['values']
    assert x['return_bps']==pytest.approx(-100)
    assert x['long_net_bps']==pytest.approx(-120) and x['short_net_bps']==pytest.approx(80)
    assert x['max_favorable_long_close_bps']==pytest.approx(180) and x['max_adverse_long_close_bps']==pytest.approx(-120)
    assert x['max_favorable_short_close_bps']==pytest.approx(80) and x['max_adverse_short_close_bps']==pytest.approx(-220)
    first=p['rows'][0]['values'];assert first['max_favorable_short_close_bps']<0 and first['max_adverse_long_close_bps']>0

def test_missing_intermediate_quote_preserves_midpoint_not_excursion():
    raw,old,c=fixture();raw.loc[1,'bid']=np.nan;x=part((raw,old,c))['rows'][2]['values']
    assert x['state']=='available' and x['midpoint_valid'] and x['bidask_endpoint_valid'] and not x['bidask_excursion_valid']
    assert x['max_favorable_long_close_bps'] is None

def test_gap_does_not_turn_exact_endpoints_into_path():
    raw,_,_=fixture();raw=raw.drop(index=1);args=fixture(frame=raw);x=part(args)['rows'][2]
    assert x['values']['state']=='gap_in_path' and x['values']['path_missing_bars']==1 and x['values']['return_bps'] is None
    assert x['original_endpoint_value'] is not None and x['shared_support_exact_match'] is None

def test_duplicates_removed_not_arbitrarily_selected():
    raw,_,_=fixture();raw=pd.concat([raw,raw.iloc[[1]]]);p=part(fixture(frame=raw));assert p['quality']['duplicate_rows_excluded']==2 and p['rows'][2]['values']['state']=='gap_in_path'

def test_missing_reference_retained_for_every_horizon():
    raw,_,_=fixture();p=part(fixture(frame=raw.iloc[1:]));assert len(p['rows'])==3 and all(r['values']['state']=='missing_exact_reference' for r in p['rows'])

def test_missing_tail_inside_closed_query_is_not_pending():
    raw,_,_=fixture();p=part(fixture(frame=raw.iloc[:2]));assert p['rows'][2]['values']['state']=='missing_target'

def test_right_edge_and_horizon_specific_asof():
    p=part(fixture(horizons=(1,3,8)))
    assert p['rows'][2]['values']['state']=='pending_right_edge'
    early=asof(p['rows'][1],BASE+120);assert early['status']=='pending_maturity' and early['values'] is None and set(early)=={'record_id','target_id','available_epoch','status','values'}
    assert asof(p['rows'][0],BASE+120)['status']=='available'
    assert asof(p['rows'][1],BASE+240)['values']==p['rows'][1]['values']

def test_future_beyond_target_cannot_change_earlier_label():
    args=fixture();before=part(args)['rows'][0];raw=args[0].copy();raw.loc[2:,['mid','bid','ask']]*=1.37
    # Only the first horizon is evaluated; original unmodified future labels are not consumed.
    c={**args[2],'horizon_minutes':[1]};assert part((raw,args[1],c))['rows'][0]==before

def test_asof_rejects_before_origin_and_noninteger():
    row=part(fixture())['rows'][0]
    for value in (BASE,True,1.5):
        with pytest.raises(ValueError):asof(row,value)

def test_all68_original_support_and_scalar_path_oracle(completed):
    r=read(completed/'run_report.json');assert r['rows']==36176 and r['instruments']==68 and r['models_fitted']==0
    assert len(read(completed/'COMPLETION_MANIFEST.json')['payloads'])==70
    total=0
    for member in read(INPUT/'SLICES_MANIFEST.json')['members']:
        raw,_=clean(pd.read_parquet(INPUT/member['path']));by={int(x.epoch):x for x in raw.itertuples()};part_=read(completed/('pair_'+member['instrument']+'.json'))
        assert len(part_['rows'])==76*7
        old={(x['record_id'],x['target_id']):x for x in read(TECH/('pair_'+member['instrument']+'.json'))['outcomes']}
        for row in part_['rows']:
            origin=row['origin_epoch']-60;h=row['horizon_minutes'];v=row['values'];target=origin+h*60
            assert row['original_endpoint_value']==old[(row['record_id'],f'technical_endpoint_midpoint_elapsed_{h}m')]['value']
            positions=[by.get(t) for t in range(origin,target+1,60)]
            expected='missing_exact_reference' if positions[0] is None else 'pending_right_edge' if target>=contract()['coverage_end_epoch'] else 'missing_target' if positions[-1] is None else 'gap_in_path' if any(x is None for x in positions) else 'available'
            assert v['state']==expected
            if expected!='available':assert v['return_bps'] is None;continue
            start,end=positions[0],positions[-1];assert v['return_bps']==(end.mid/start.mid-1)*10000;total+=1
            if v['bidask_excursion_valid']:
                long=[(x.bid-start.ask)/start.mid*10000 for x in positions[1:]];short=[(start.bid-x.ask)/start.mid*10000 for x in positions[1:]]
                assert v['max_favorable_long_close_bps']==max(long) and v['max_adverse_long_close_bps']==min(long)
                assert v['max_favorable_short_close_bps']==max(short) and v['max_adverse_short_close_bps']==min(short)
    assert total==r['shared_endpoint_values_exact']

def test_complete_run_idempotent_without_recompute(completed,monkeypatch):
    import sampled_path_runner_v2 as runner
    def forbidden(*a,**k):raise AssertionError('completed run must not recompute labels')
    monkeypatch.setattr(runner,'pair_records',forbidden)
    assert call('run',completed.parent)['status']=='completed_verified'

def test_consumed_bytes_rechecked(tmp_path):
    p=tmp_path/'input';p.write_bytes(b'good');h=op.sha(p);p.write_bytes(b'bad')
    with pytest.raises(ValueError,match='consumed_input_changed'):checked(p,h)

def test_recipe_pin_and_source_drift_refused(completed,tmp_path,monkeypatch):
    assert op.operate('status',RECIPE,'0'*64,INPUT,TECH,tmp_path)['status']=='review_required'
    original=op.sha
    monkeypatch.setattr(op,'sha',lambda p:'0'*64 if p.name=='sampled_path_adapter_v2.py' else original(p))
    assert call('status',tmp_path)['reason']=='sampled_path_source_drift_before_import'

def test_false_completion_and_payload_tamper_refused(completed,tmp_path):
    root=tmp_path/read(RECIPE)['run_id'];shutil.copytree(completed,root);(root/'run_report.json').write_text('{}')
    assert call('verify',tmp_path)['status']=='review_required'

def test_one_writer(completed,tmp_path):
    r=read(RECIPE);p=RunPublisher(tmp_path,r['run_id'],identity_for(r));p.acquire()
    try:assert call('run',tmp_path)['status']=='review_required'
    finally:p.release()

@pytest.mark.parametrize('boundary',[1,0])
def test_real_process_crash_resume_exact(completed,tmp_path,boundary):
    cmd=[sys.executable,'-I','-B',str(ROOT/'sampled_path_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--input-root',str(INPUT),'--technical',str(TECH),'--runs-dir',str(tmp_path),'--test-crash-after',str(boundary)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=600);assert p.returncode==91,p.stdout+p.stderr
    assert call('status',tmp_path)['status']=='resumable';cmd=cmd[:-2];cmd[4]='resume';p=subprocess.run(cmd,capture_output=True,text=True,timeout=600);assert p.returncode==0,p.stdout+p.stderr
    root=tmp_path/read(RECIPE)['run_id'];assert all(op.sha(root/n)==op.sha(completed/n) for n in required(read(RECIPE)))

def test_isolated_preflight_imports_no_numerical_or_model_library():
    code="""import sys,importlib.abc
sys.path.insert(0,sys.argv[1])
class Deny(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname.split('.')[0] in {'numpy','pandas','pyarrow','scipy','sklearn','joblib'}:raise AssertionError('preflight imported '+fullname)
sys.meta_path.insert(0,Deny())
from pathlib import Path
import sampled_path_operator_v2 as op
p=Path(sys.argv[2]);op.preflight(p,op.sha(p),Path(sys.argv[3]),Path(sys.argv[4]))
"""
    p=subprocess.run([sys.executable,'-I','-B','-c',code,str(ROOT),str(RECIPE),str(INPUT),str(TECH)],capture_output=True,text=True,timeout=60);assert p.returncode==0,p.stdout+p.stderr
