import base64,copy,json,os,sys,subprocess,shutil
from pathlib import Path
from decimal import Decimal
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
from policy_continuation_v2 import PolicyReplay
from matched_policy_fixture_v2 import fixture
from matched_policy_input_v2 import adapted_candidates
from historical_native_input_v2 import financing_rates,financing_event_rates,SCENARIOS
from contracts import fingerprint
import matched_policy_operator_v2 as op
INPUT=Path(os.environ.get('FOREX_MATCHED_POLICY_INPUT',str(ROOT/'evidence/timed_20260922_022952/matched_policy_step/MATCHED_POLICY_INPUTS.json')))
TRAD=Path(os.environ.get('FOREX_MATCHED_POLICY_TRAD',str(ROOT.parent/'trad')))
RECIPE=ROOT/'MATCHED_POLICY_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text())
@pytest.fixture(scope='module')
def inputs():return read(INPUT)
@pytest.fixture(scope='module')
def historical(inputs):return fixture(inputs,'ridge','candle_one_bp_slippage_rollover',TRAD)

def test_all68_same_target_and_causal_native_inputs(historical):
    c,fs,cov=historical
    assert len(cov)==544 and len({x['instrument'] for x in cov})==68
    assert sum(len(f.get('historical_packets',[])) for f in fs)==538
    assert len(fs)==19 and len({f['target_epoch'] for f in fs if f['kind']=='decision'})==1
    assert all(not p['observed_execution'] and not p['observed_publication'] for f in fs for p in f.get('historical_packets',[]))

@pytest.mark.parametrize('kind',['prediction','target','method','fit','feature','market','pip','candidate','terminal'])
def test_tamper_rejected_before_any_ledger_mutation(historical,kind):
    c,frames,_=historical;p=PolicyReplay(c,trad_root=TRAD);f=copy.deepcopy(frames[0])
    if kind=='prediction':
        packet=f['historical_packets'][0];packet['forecast']['forecast']['prediction']+=1;packet['packet_sha256']=fingerprint({k:v for k,v in packet.items() if k!='packet_sha256'})
    elif kind=='target':f['target_epoch']+=60
    elif kind=='method':f['method']='recovered_hgb'
    elif kind=='fit':f['matched_input']['tree_base64']=base64.b64encode(b'not a pickle').decode()
    elif kind=='feature':f['matched_input']['observations'][0]['features'][0]+=1
    elif kind=='market':f['quotes'][next(iter(f['quotes']))]['ask']='10000'
    elif kind=='pip':p.book.config['metadata'][f['historical_packets'][0]['prepared_curve']['instrument']]['pip_size']='1'
    elif kind=='candidate':f['candidates'][0]['expected_terminal_price']='10000'
    else:f['terminal']=True
    before=copy.deepcopy(p.book.state)
    with pytest.raises(ValueError):p.apply(f)
    assert p.events==[] and p.decisions==[] and p.book.state==before and p.cursor is None

def test_missing_mark_preserves_holdings_and_recovers(historical):
    c,frames,_=historical;p=PolicyReplay(c,trad_root=TRAD);deferred=[]
    for f in frames:
        before={a:copy.deepcopy(v['lots']) for a,v in p.book.state['arms'].items()}
        start=len(p.decisions);p.apply(f)
        for d in p.decisions[start:]:
            if d['reason']=='liquidation_wealth_unavailable_no_discretionary_action':
                assert d['action']=='WAIT' and d['values']['baseline_liquidation_wealth_usd'] is None
                assert d['values']['exit_value_usd'] is None and d['values']['alternatives']==[]
                assert p.book.state['arms'][d['arm']]['lots']==before[d['arm']]
                deferred.append(d)
    assert deferred and all(not v['lots'] for v in p.book.state['arms'].values())
    assert all(r['receipt']['status']!='rejected' for r in p.rows)

def test_rollover_rates_convert_quote_units_once():
    from accounting_event_fixtures_v2 import panel
    from reference_accounting_adapter_v2 import load_reference
    q=panel(1000);scenario=SCENARIOS['candle_one_bp_slippage_rollover'];usd=financing_rates(q,1000,scenario,TRAD);quote=financing_event_rates(q,1000,scenario,TRAD);ref=load_reference(TRAD)
    for pair in ('EUR_USD','USD_JPY'):
        for side in ('long','short'):
            converted,_=ref.convert_pnl_to_usd(Decimal(quote[pair][side])*1000,pair[4:],q,1000,60)
            assert abs(converted-Decimal(usd[pair][side])*1000)<Decimal('1e-25')
    assert Decimal(quote['USD_JPY']['long'])!=Decimal(usd['USD_JPY']['long'])

def test_forced_exit_with_unavailable_value_is_intent_not_fake_fill(historical):
    c,frames,_=historical;p=PolicyReplay(c,trad_root=TRAD)
    p.apply(frames[0]);p.apply(frames[1]);pair=p.position('fixed_hold')['instrument']
    f=copy.deepcopy(frames[2]);f['terminal']=True;f['quotes'].pop(pair,None)
    # Unit-test the decision primitive; historical frame admission separately
    # requires the exact recorded terminal epoch and authenticated market panel.
    action,candidate,reason,values=p.propose('fixed_hold',[],f)
    assert action=='EXIT' and reason=='risk_or_predeclared_deadline' and values['baseline_liquidation_wealth_usd'] is None
    assert p.position('fixed_hold') is not None

def test_unavailable_value_cannot_extend_challenger_persistence(historical):
    c,frames,_=historical;p=PolicyReplay(c,trad_root=TRAD)
    p.apply(frames[0]);p.apply(frames[1]);pair=p.position('hysteresis')['instrument']
    p.memory['hysteresis'].update(streak=1,challenger_key=['EUR_USD',1,frames[0]['target_epoch'],'example'])
    f=copy.deepcopy(frames[2]);f['quotes'].pop(pair,None)
    action,_,reason,_=p.propose('hysteresis',[],f)
    assert action=='WAIT' and reason=='liquidation_wealth_unavailable_no_discretionary_action'
    assert p.memory['hysteresis']['streak']==0 and p.memory['hysteresis']['challenger_key'] is None

@pytest.mark.parametrize('boundary',[7,0])
def test_real_crash_resume_payload_parity(historical,tmp_path,boundary):
    from policy_runner_v2 import run
    c,frames,_=historical;run(c,frames,run_id='clean',runs_dir=tmp_path,trad_root=TRAD)
    p=tmp_path/'input.json';p.write_text(json.dumps({'contract':c,'frames':frames}))
    cmd=[sys.executable,'-I','-B',str(ROOT/'policy_runner_v2.py'),'--input',str(p),'--runs-dir',str(tmp_path),'--run-id','partial','--trad-root',str(TRAD)]
    killed=subprocess.run(cmd+['--test-crash-after-frame',str(boundary)],capture_output=True,text=True,timeout=180);assert killed.returncode==91,killed.stderr
    resumed=subprocess.run(cmd+['--resume'],capture_output=True,text=True,timeout=180);assert resumed.returncode==0,resumed.stderr
    for r in read(tmp_path/'clean/COMPLETION_MANIFEST.json')['payloads']:assert (tmp_path/'clean'/r['path']).read_bytes()==(tmp_path/'partial'/r['path']).read_bytes()

def test_operator_source_drift_before_import(tmp_path):
    recipe=read(RECIPE)
    for n in set(recipe['sources'])|set(recipe['base_recipe']['sources'])|{RECIPE.name}:shutil.copyfile(ROOT/n,tmp_path/n)
    (tmp_path/'matched_policy_input_v2.py').write_text("raise RuntimeError('DO_NOT_IMPORT')\n")
    r=tmp_path/RECIPE.name;cmd=[sys.executable,'-I',str(tmp_path/'matched_policy_operator_v2.py'),'run','--recipe',str(r),'--recipe-sha256',op.sha(r),'--input',str(INPUT),'--trad-root',str(TRAD),'--runs-dir',str(tmp_path/'runs')]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=30)
    assert p.returncode==2 and 'source_drift_before_import' in p.stdout and 'DO_NOT_IMPORT' not in p.stderr

def test_input_tamper_refused(tmp_path):
    p=tmp_path/'input.json';p.write_text('{}')
    assert op.operate('run',RECIPE,op.sha(RECIPE),p,tmp_path/'runs',TRAD)['status']=='review_required'
