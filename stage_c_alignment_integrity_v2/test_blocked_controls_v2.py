import json,os,shutil,subprocess,sys
from pathlib import Path
from decimal import Decimal,localcontext
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import blocked_controls_operator_v2 as op
from publication import RunPublisher
from campaign_inspector_v2 import CampaignReader
E=ROOT/'evidence/timed_20260922_022952'
PATHS_FILE=Path(os.environ.get('FOREX_CONTROLS_PATHS',str(E/'blocked_controls_step/PATHS.json')))
RAW=Path(os.environ.get('FOREX_CONTROLS_RAW',str(E/'rich_campaign_inputs_step/inputs')))
TRAD=Path(os.environ.get('FOREX_CONTROLS_TRAD',str(ROOT.parent/'trad')))
sys.path.insert(0,str(TRAD))
RECIPE=ROOT/'BLOCKED_CONTROLS_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def call(action,runs):return op.operate(action,RECIPE,op.sha(RECIPE),read(PATHS_FILE),RAW,TRAD,runs)
@pytest.fixture(scope='session')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_CONTROLS_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('controls-baseline');r=call('verify' if supplied else 'run',runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])
def test_complete_scope_and_explicit_limitations(completed):
    r=read(completed/'run_report.json');assert r['paired_comparisons']==336 and r['day_shift_rows']==1344 and r['day_counts']==[3,4,5]
    assert r['raw_audit_rows']==136 and r['models_fitted']==0 and r['selected_model'] is None and r['p_value'] is None
    assert r['effective_sample_size'] is None and r['independent_observations_claimed'] is False
    for scope in read(completed/'blocked_control_scope.json'):
        assert len(scope['all_declared_instruments'])==68 and sorted(scope['balanced_instruments']+scope['excluded_instruments'])==scope['all_declared_instruments']
        assert scope['balanced_rows']==scope['day_count']*4*len(scope['balanced_instruments'])
        assert scope['mature_rows_before_balancing']==scope['balanced_rows']+scope['balance_excluded_mature_rows']
        assert scope['forecast_rows']==scope['mature_rows_before_balancing']+sum(scope['outcome_exclusions'].values())
def test_joint_day_mapping_all_shifts_and_exact_marginal_preservation(completed):
    groups={}
    for row in read(completed/'blocked_day_shifts.json'):
        key=tuple(row[n] for n in ('group','target_id','procedure','method','control'));groups.setdefault(key,[]).append(row)
        m=row['outcome_origin_mapping'];assert sorted(x for x,y in m)==sorted(y for x,y in m)
        assert all(x%86400==y%86400 for x,y in m)
    for rows in groups.values():
        assert [r['shift_days'] for r in rows]==list(range(len(rows))) and len({r['rows'] for r in rows})==1
        if rows[0]['control']=='zero':
            assert all(r['metrics']['control_mse_bps2']==rows[0]['metrics']['control_mse_bps2'] for r in rows)
            assert all(r['metrics']['control_mae_bps']==rows[0]['metrics']['control_mae_bps'] for r in rows)
def test_real_shift_decimal_oracle(completed):
    predicate=lambda x:x['group']=='full228_cost2' and x['method']=='ridge' and x['control']=='zero' and x['procedure']=='frozen' and x['target_id'].endswith('_15m')
    scope=next(x for x in read(completed/'blocked_control_scope.json') if predicate(x));row=next(x for x in read(completed/'blocked_day_shifts.json') if predicate(x) and x['shift_days']==1)
    reader=CampaignReader(read(PATHS_FILE),read(RECIPE)['dependencies']);values={(r['forecast']['decision_epoch'],r['forecast']['instrument']):r['forecast']['prediction'] for r in reader.read('family','forecasts_full228_cost2_15_frozen.json') if r['method']=='ridge'}
    labels={}
    for pair in scope['balanced_instruments']:
        for o in reader.read('rich','outcomes_'+pair+'.json')['outcomes']:
            if o['target_id']==scope['target_id']:labels[pair,int(o['record_id'].rsplit(':',1)[1])]=o['value']
    squared=[];absolute=[]
    with localcontext() as ctx:
        ctx.prec=60
        for origin,source in row['outcome_origin_mapping']:
            for pair in scope['balanced_instruments']:
                p=Decimal(str(values[origin,pair]));y=Decimal(str(labels[pair,source]));squared.append((p-y)**2-y*y);absolute.append(abs(p-y)-abs(y))
        assert len(squared)==row['rows']
        assert float(sum(squared)/len(squared))==pytest.approx(row['metrics']['mse_delta_bps2'],rel=1e-12,abs=1e-8)
        assert float(sum(absolute)/len(absolute))==pytest.approx(row['metrics']['mae_delta_bps'],rel=1e-12,abs=1e-10)
def test_real_all68_future_poison_and_known_leak_detection(completed):
    rows=read(completed/'future_leak_audit.json');cfg=read(RECIPE)['configuration']
    assert {(r['instrument'],r['origin_epoch']) for r in rows}=={(p,o) for p in cfg['universe'] for o in cfg['leak_origins']}
    import io,pandas as pd
    from rolling_registry_operator_v2 import checked_bytes
    raw_by_pair={}
    for member in read(RAW/'INPUT_MANIFEST.json')['members']:
        raw_by_pair[member['instrument']]=pd.read_parquet(io.BytesIO(checked_bytes(RAW,member)))
    missing={(r['instrument'],r['origin_epoch']) for r in rows if r['status']=='unavailable_support'}
    assert missing=={('TRY_JPY',1721736060),('USD_THB',1721822460)}
    assert sum(r['status']=='verified_causal_and_leak_detected' for r in rows)==134
    for r in rows:
        assert r['clean_values_sha256']==r['prefix_values_sha256']==r['poisoned_values_sha256']
        assert r['prefix_differences']==r['future_differences']==[] and r['finite_causal_features']>0
        frame=raw_by_pair[r['instrument']];future=frame.loc[frame.time==r['origin_epoch']]
        assert not r['planted_leak']['admitted_to_campaign']
        if len(future)==0:
            assert r['status']=='unavailable_support' and not r['planted_leak']['detected']
            assert r['planted_leak']['original_value'] is None and r['planted_leak']['poisoned_value'] is None
        else:
            assert len(future)==1 and r['status']=='verified_causal_and_leak_detected' and r['planted_leak']['detected']
            assert r['planted_leak']['original_value']==float(future.close.iloc[0])
            assert r['planted_leak']['poisoned_value']==pytest.approx(float(future.close.iloc[0])*1.37,rel=1e-15)
        assert r['planted_leak']['available_at']>r['origin_epoch']
def test_leak_audit_does_not_call_missing_support_a_pass():
    import io,pandas as pd
    from leak_positive_audit_v2 import audit_frame
    from rolling_registry_operator_v2 import checked_bytes
    m=read(RAW/'INPUT_MANIFEST.json')['members'][0];frame=pd.read_parquet(io.BytesIO(checked_bytes(RAW,m)));origin=read(RECIPE)['configuration']['leak_origins'][0]
    frame=frame.loc[frame.time!=origin]
    r=audit_frame(m['instrument'],frame,m['pip_size'],m['source_member_sha256'],origin)
    assert r['status']=='unavailable_support' and not r['planted_leak']['detected']
def test_known_future_reading_producer_fails_causality_audit(monkeypatch):
    import io,pandas as pd
    import leak_positive_audit_v2 as audit
    from rolling_registry_operator_v2 import checked_bytes
    m=read(RAW/'INPUT_MANIFEST.json')['members'][0];frame=pd.read_parquet(io.BytesIO(checked_bytes(RAW,m)));origin=read(RECIPE)['configuration']['leak_origins'][0]
    original=audit.pair_features
    def deliberately_leaky(pair,data,pip,source,origins):
        result=original(pair,data,pip,source,origins)
        for row in result['observations']:
            future=data.loc[data.time==row['origin_epoch']]
            row['values']['deliberate_future_close']=float(future.close.iloc[0]) if len(future)==1 else None
        return result
    monkeypatch.setattr(audit,'pair_features',deliberately_leaky)
    r=audit.audit_frame(m['instrument'],frame,m['pip_size'],m['source_member_sha256'],origin)
    assert r['status']=='audit_failed' and r['prefix_differences']==['deliberate_future_close'] and r['future_differences']==['deliberate_future_close']
def test_source_drift_refused_before_import(tmp_path,monkeypatch):
    for n in op.SOURCES:shutil.copyfile(ROOT/n,tmp_path/n)
    (tmp_path/'blocked_controls_report_v2.py').write_text("raise RuntimeError('MUST_NOT_IMPORT')")
    monkeypatch.setattr(op,'ROOT',tmp_path)
    with pytest.raises(ValueError,match='source_drift_before_import'):op.preflight(RECIPE,op.sha(RECIPE),read(PATHS_FILE),RAW,TRAD)
def test_raw_consumed_byte_drift_refused(tmp_path):
    from rolling_registry_operator_v2 import checked_bytes
    row=read(RAW/'INPUT_MANIFEST.json')['members'][0];data=bytearray((RAW/row['path']).read_bytes());data[0]^=1;(tmp_path/row['path']).write_bytes(data)
    with pytest.raises(ValueError,match='consumed_bytes_changed'):checked_bytes(tmp_path,row)
def test_one_writer_and_false_completion(tmp_path):
    r=read(RECIPE);p=RunPublisher(tmp_path,r['run_id'],op.identity_for(r));p.acquire()
    try:assert call('run',tmp_path)['status']=='review_required'
    finally:p.release()
    assert call('verify',tmp_path)['status']=='review_required'
def test_actual_consumer_denies_network_and_estimator_imports(completed,tmp_path):
    code="""import sys,json,builtins,socket
from pathlib import Path
sys.path.insert(0,sys.argv[1]);original=builtins.__import__
def guarded(name,*args,**kwargs):
 if name.split('.')[0] in {'sklearn','joblib'}:raise AssertionError('estimator_import_forbidden:'+name)
 return original(name,*args,**kwargs)
def denied(*a,**k):raise AssertionError('network_forbidden')
builtins.__import__=guarded;socket.socket.connect=denied;socket.socket.connect_ex=denied;socket.socket.sendto=denied;socket.create_connection=denied;socket.getaddrinfo=denied
import blocked_controls_operator_v2 as op
r=op.operate('run',Path(sys.argv[2]),sys.argv[3],json.loads(Path(sys.argv[4]).read_text(encoding='utf-8')),Path(sys.argv[5]),Path(sys.argv[6]),Path(sys.argv[7]))
assert r['status']=='completed_verified',r
assert not any(x.split('.')[0] in {'sklearn','joblib'} for x in sys.modules)
print(json.dumps(r))
"""
    p=subprocess.run([sys.executable,'-I','-B','-c',code,str(ROOT),str(RECIPE),op.sha(RECIPE),str(PATHS_FILE),str(RAW),str(TRAD),str(tmp_path)],capture_output=True,text=True,timeout=600)
    assert p.returncode==0,p.stdout+p.stderr
    for n in op.REQUIRED:assert (completed/n).read_bytes()==(tmp_path/completed.name/n).read_bytes()
@pytest.mark.parametrize('boundary',[1,0])
def test_actual_process_death_resume(completed,tmp_path,boundary):
    cmd=[sys.executable,'-I','-B',str(ROOT/'blocked_controls_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--paths',str(PATHS_FILE),'--raw-root',str(RAW),'--trad-root',str(TRAD),'--runs-dir',str(tmp_path),'--test-crash-after',str(boundary)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=600);assert p.returncode==91,p.stdout+p.stderr
    assert call('resume',tmp_path)['status']=='completed_verified'
    for n in op.REQUIRED:assert (completed/n).read_bytes()==(tmp_path/completed.name/n).read_bytes()
