import copy,json,os
from decimal import Decimal,localcontext,Context
from pathlib import Path
import pytest
from contracts import fingerprint
from fixed_blend_remaining_policy_v2 import fixture,blend_packet,combined_packets
from policy_continuation_v2 import PolicyReplay

ROOT=Path(__file__).resolve().parent
INPUT=Path(os.environ.get('FOREX_MATCHED_POLICY_INPUT',str(ROOT/'evidence/timed_20260922_022952/matched_policy_step/MATCHED_POLICY_INPUTS.json')))
TRAD=Path(os.environ.get('FOREX_MATCHED_POLICY_TRAD',str(ROOT.parent/'trad')))

@pytest.fixture(scope='module')
def data():return json.loads(INPUT.read_bytes())

@pytest.fixture(scope='module')
def prepared(data):return fixture(data,'candle_one_bp_slippage_rollover',TRAD)

def test_all68_pairing_and_original_target(prepared):
    c,frames,coverage=prepared
    assert len(coverage)==544 and len({x['instrument'] for x in coverage})==68
    assert sum(x['status']=='admitted' for x in coverage)==536
    assert len(frames)==19
    assert all(not p['observed_execution'] and not p['observed_publication'] for f in frames for p in f.get('historical_packets',[]))
    assert sum(len(f.get('historical_packets',[])) for f in frames)==538

def test_exact_decimal_blend_and_dual_lineage(data):
    packets=data['packets'];a=next(p for p in packets if p['method']=='ridge');b=next(p for p in packets if p['method']=='recovered_hgb' and p['conditioning_epoch']==a['conditioning_epoch'] and p['reference_point']==a['reference_point'])
    result=blend_packet(a,b,TRAD)
    with localcontext(Context(prec=192)):
        expected=(Decimal(str(a['forecast']['forecast']['prediction']))+Decimal(str(b['forecast']['forecast']['prediction'])))/2
        terminal=Decimal(a['reference_point']['reference_close'])*(1+expected/10000)
    assert Decimal(result['prediction_bps'])==expected
    assert Decimal(result['prepared_curve']['nodes'][0]['expected_terminal_price'])==terminal
    assert result['base_packet_sha256']==[a['packet_sha256'],b['packet_sha256']]
    assert result['source_bindings']['ridge_packet']==a['packet_sha256']
    assert result['source_bindings']['hgb_packet']==b['packet_sha256']

def test_missing_partner_has_no_fallback_and_duplicates_refuse(data):
    one=[data['packets'][0]]
    assert combined_packets(one,TRAD)==[]
    with pytest.raises(ValueError,match='duplicate'):combined_packets(one*2,TRAD)

@pytest.mark.parametrize('mutation',['base_prediction','blend_prediction','target','method','base_clock','feature','market','pip','candidate','terminal','missing_base'])
def test_tamper_refuses_before_accounting_mutation(prepared,mutation):
    c,frames,_=prepared;replay=PolicyReplay(c,trad_root=TRAD);frame=copy.deepcopy(frames[0])
    if mutation=='base_prediction':
        p=frame['base_packets'][0];p['forecast']['forecast']['prediction']+=1;p['packet_sha256']=fingerprint({k:v for k,v in p.items() if k!='packet_sha256'})
    elif mutation=='blend_prediction':
        p=frame['historical_packets'][0];p['prediction_bps']='999';p['packet_sha256']=fingerprint({k:v for k,v in p.items() if k!='packet_sha256'})
    elif mutation=='target':frame['target_epoch']+=60
    elif mutation=='method':frame['method']='ridge'
    elif mutation=='base_clock':frame['matched_input']['fit']['ready_epoch']=frame['epoch']+60
    elif mutation=='feature':frame['matched_input']['observations'][0]['features'][0]+=1
    elif mutation=='market':frame['quotes'][next(iter(frame['quotes']))]['ask']='1000'
    elif mutation=='pip':replay.book.config['metadata'][frame['historical_packets'][0]['prepared_curve']['instrument']]['pip_size']='1'
    elif mutation=='candidate':frame['candidates'][0]['expected_terminal_price']='1000'
    elif mutation=='terminal':frame['terminal']=True
    else:frame['base_packets'].pop()
    before=copy.deepcopy(replay.book.state)
    with pytest.raises(ValueError):replay.apply(frame)
    assert replay.book.state==before and replay.events==[] and replay.decisions==[] and replay.cursor is None

def test_risk_cost_policy_contract_unchanged(data,prepared):
    from matched_policy_fixture_v2 import fixture as parent
    c,frames,cov=prepared
    original,old,_=parent(data,'ridge','candle_one_bp_slippage_rollover',TRAD)
    assert c==original
    for a,b in zip(frames,old):
        assert a['epoch']==b['epoch'] and a['quotes']==b['quotes'] and a['scenario']==b['scenario']
        if a['kind'] in ('execution','financing'):
            assert {k:v for k,v in a.items() if k not in ('model_profile','method')}=={k:v for k,v in b.items() if k not in ('model_profile','method')}
