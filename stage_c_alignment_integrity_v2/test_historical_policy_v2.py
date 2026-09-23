from copy import deepcopy
from pathlib import Path
import json
import os
import sys
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parent))
from accounting_events_v2 import EventLedger,event_contract
from accounting_event_fixtures_v2 import event,panel
from policy_continuation_v2 import PolicyReplay,policy_contract,retrospective_policy_contract
from historical_native_input_v2 import validate_historical_frame,candidate,make_packet
from historical_policy_fixture_v2 import fixture


def test_recent_quote_mark_is_not_an_actual_execution_fill():
    book=EventLedger(event_contract())
    book.apply(event('intent',1000,0,arm='hold',order_id='entry',instrument='EUR_USD',action='open',side=1,notional_usd='1000',target_epoch=200000))
    book.apply(event('activate',1060,0,arm='hold',order_id='entry'))
    units=book.state['arms']['hold']['orders']['entry']['units']
    book.apply(event('fill',1060,1,arm='hold',order_id='entry',fill_id='f1',units=units,filled_epoch=1060,fee_usd='0',execution_evidence_id='synthetic-f1'))
    quotes=panel(1060)
    assert book.reconcile(book.state['arms']['hold'],quotes,1062)['equity_usd'] is not None
    assert book.reconcile(book.state['arms']['hold'],quotes,1121)['equity_usd'] is None
    book.apply(event('intent',1062,0,arm='hold',order_id='exit',instrument='EUR_USD',action='reduce',units=units,lot_ids=['f1'],quotes=quotes))
    book.apply(event('activate',1122,0,arm='hold',order_id='exit',quotes=panel(1120)))
    result=book.apply(event('fill',1122,1,arm='hold',order_id='exit',fill_id='bad',units=units,filled_epoch=1122,fee_usd='0',execution_evidence_id='not-exact',quotes=panel(1120)))
    assert result['receipt']['status']=='rejected'
    assert book.state['arms']['hold']['lots']


def test_policy_and_ledger_tiers_cannot_be_mixed():
    c=policy_contract();c['ledger']=retrospective_policy_contract()['ledger']
    with pytest.raises(ValueError,match='tier_mismatch'):PolicyReplay(c)


@pytest.fixture(scope='module')
def historical():
    path=os.environ.get('FOREX_HISTORICAL_POLICY_INPUT')
    if not path:pytest.skip('explicit historical scenario inputs required')
    return fixture(json.loads(Path(path).read_text(encoding='utf-8')))


def test_real_fitted_native_packets_are_offline_and_all68_visible(historical):
    c,frames,coverage=historical
    assert len(coverage)==544 and len({r['instrument'] for r in coverage})==68
    packets=[p for f in frames for p in f.get('historical_packets',[])]
    assert len(packets)==258
    assert all(p['prepared_curve']['scope']=='engineering_replay' and p['observed_publication'] is False for p in packets)
    assert all(p['prepared_curve']['nodes'][0]['probability_up'] is None for p in packets)
    assert sum(len(f.get('candidates',[])) for f in frames)>0


@pytest.mark.parametrize('kind',['candidate','quote','tier','delay','fill'])
def test_historical_tampering_refused_before_ledger_events(historical,kind):
    c,frames,_=deepcopy(historical);p=PolicyReplay(c)
    f=deepcopy(next(f for f in frames if f.get('candidates')))
    if kind=='candidate':f['candidates'][0]['expected_terminal_price']='1000'
    elif kind=='quote':f['quotes'][next(iter(f['quotes']))]['ask']='1000'
    elif kind=='tier':f['input_tier']='synthetic_fresh_conditional_native_curve.v1'
    elif kind=='delay':p.book.config['execution_delay_sec']=60
    elif kind=='fill':
        f=deepcopy(next(f for f in frames if f['kind']=='execution'));f['fills']['cash']['evidence_id']='fabricated'
    with pytest.raises(ValueError):p.apply(f)
    assert p.events==[]


def test_reference_point_and_forecast_must_match_original_inputs(historical):
    c,frames,_=historical;packet=deepcopy(next(p for f in frames for p in f.get('historical_packets',[])))
    packet['remaining_forecast']['forecast']['prediction']+=1
    with pytest.raises(ValueError,match='not_reproduced'):
        make_packet(packet['remaining_forecast'],packet['model'],packet['observation'],packet['reference_point'],packet['metadata'])


def test_native_exact_target_and_future_availability_refused(historical):
    c,frames,_=historical;f=next(f for f in frames if f.get('candidates'));p=f['historical_packets'][0]
    with pytest.raises(ValueError,match='exact_target'):
        candidate(p,f['quotes'],f['epoch'],f['target_epoch']-1,f['scenario'],p['metadata'])
    with pytest.raises(ValueError,match='not_available'):
        candidate(p,f['quotes'],f['epoch']-1,f['target_epoch'],f['scenario'],p['metadata'])


def test_historical_consumer_closes_and_audits_actual_ledger(historical):
    from accounting_event_audit_v2 import audit_accounting
    c,frames,_=historical;p=PolicyReplay(c)
    for f in frames:p.apply(f)
    assert len(p.decisions)==54
    assert any(d['action']=='ENTER' for d in p.decisions)
    assert all(not arm['lots'] for arm in p.book.state['arms'].values())
    assert all(r['receipt']['status']!='rejected' for r in p.rows)
    assert audit_accounting(c['ledger'],p.events,p.rows)['status']=='verified'


@pytest.mark.parametrize('boundary',[11,0])
def test_historical_actual_process_death_and_resume(historical,tmp_path,boundary):
    import subprocess
    from policy_runner_v2 import run
    c,frames,_=historical;root=Path(__file__).resolve().parent
    run(c,frames,run_id='clean',runs_dir=tmp_path)
    inp=tmp_path/'inputs.json';inp.write_text(json.dumps({'contract':c,'frames':frames}))
    cmd=[sys.executable,'-I','-B',str(root/'policy_runner_v2.py'),'--input',str(inp),'--runs-dir',str(tmp_path),'--run-id','partial']
    killed=subprocess.run(cmd+['--test-crash-after-frame',str(boundary)],capture_output=True,text=True,timeout=120)
    assert killed.returncode==91,killed.stderr
    resumed=subprocess.run(cmd+['--resume'],capture_output=True,text=True,timeout=120)
    assert resumed.returncode==0,resumed.stderr
    for row in json.loads((tmp_path/'clean/COMPLETION_MANIFEST.json').read_text())['payloads']:
        assert (tmp_path/'clean'/row['path']).read_bytes()==(tmp_path/'partial'/row['path']).read_bytes()


def test_historical_operator_refuses_source_drift_before_import(tmp_path):
    import shutil,subprocess
    from historical_policy_operator_v2 import sha
    root=Path(__file__).resolve().parent
    recipe=json.loads((root/'HISTORICAL_POLICY_OPERATOR_RECIPE.json').read_text())
    names=set(recipe['sources'])|set(recipe['base_recipe']['sources'])|{'HISTORICAL_POLICY_OPERATOR_RECIPE.json'}
    for name in names:shutil.copyfile(root/name,tmp_path/name)
    with (tmp_path/'historical_policy_fixture_v2.py').open('a') as f:f.write('\nraise RuntimeError("UNREVIEWED_HISTORICAL_SOURCE_EXECUTED")\n')
    r=tmp_path/'HISTORICAL_POLICY_OPERATOR_RECIPE.json'
    process=subprocess.run([sys.executable,'-I','-B',str(tmp_path/'historical_policy_operator_v2.py'),'run',
        '--recipe',str(r),'--recipe-sha256',sha(r),'--input',str(tmp_path/'does-not-exist.json'),
        '--runs-dir',str(tmp_path/'runs'),'--trad-root',str(root.parent/'trad')],capture_output=True,text=True)
    assert process.returncode==2 and 'source_drift_before_import' in process.stdout
    assert 'UNREVIEWED_HISTORICAL_SOURCE_EXECUTED' not in process.stderr and not (tmp_path/'runs').exists()
