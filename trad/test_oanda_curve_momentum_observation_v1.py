import copy
from decimal import Decimal,localcontext
import hashlib
from pathlib import Path

import pytest

import oanda_curve_momentum_observation_v1 as bench
import oanda_s5_mba_research_capture_v1 as source
import test_oanda_s5_mba_research_capture_v1 as fixture

NOW=fixture.NOW


def make(monkeypatch,*,pair='GBP_USD',mutate=None,decision=None):
    value=fixture.payload(pair)
    if mutate:
        mutate(value)
    raw=fixture.raw(value)
    calls=fixture.install_fake(monkeypatch,raw)
    meta={'instrument':pair,'base_currency':pair[:3],'quote_currency':pair[4:],
          'pip_size':'0.01' if pair=='USD_JPY' else '0.0001'}
    clocks=iter([NOW-1,NOW,NOW+.1])
    body,receipt=source.capture_once(pair,credential_path=Path('synthetic_unused'),metadata=meta,clock=lambda:next(clocks))
    assert len(calls)==1
    # The benchmark must never perform another request.
    monkeypatch.setattr(source.requests,'Session',lambda:(_ for _ in ()).throw(AssertionError('network_attempt')))
    cutoff=NOW+1 if decision is None else decision
    quote=dict(instrument=pair,quote_id='actual_test_quote',bid='170.10' if pair=='USD_JPY' else '1.20000',
        ask='170.12' if pair=='USD_JPY' else '1.20020',market_epoch=cutoff-.5,
        available_epoch=cutoff-.2,tradeable=True)
    consumption=dict(raw_sha256=hashlib.sha256(body).hexdigest(),receipt_sha256=receipt['receipt_sha256'],
        read_started_epoch=NOW+.2,read_completed_epoch=NOW+.3)
    return body,receipt,consumption,dict(metadata=meta,decision_epoch=cutoff,target_epoch=cutoff+600,quote=quote)


def test_exact_formula_source_clocks_and_existing_usd_admission(monkeypatch):
    body,receipt,consumed,args=make(monkeypatch)
    result=bench.build_momentum_candidate(body,receipt,consumed,**args)
    assert result['status']=='available' and result['side']==1
    assert result['source_available_epoch']==NOW+.3
    assert result['available_epoch']==NOW+.8
    assert result['reference_price_epoch']==int((NOW-10)//5)*5+5
    with localcontext() as context:
        context.prec=96
        weighted=Decimal('.5')*(Decimal('.6')/Decimal(30).sqrt()+Decimal('1.2')/Decimal(60).sqrt())
        expected=weighted*Decimal(600).sqrt()
    assert Decimal(result['expected_move_pips'])==expected
    assert [r['window_sec'] for r in result['windows']]==[30,60]
    assert result['candidate_sha256']==bench.digest({k:v for k,v in result.items() if k!='candidate_sha256'})
    assert all(result[k] is v for k,v in bench.FLAGS.items())
    assert result['policy']['confidence_scope']=='uncalibrated_score_mapping_not_probability'
    assert result['policy']['slippage_bps_per_leg']=='0.1'


@pytest.mark.parametrize('pair',['EUR_USD','GBP_USD','USD_JPY'])
def test_explicit_native_pip_preserved(pair,monkeypatch):
    body,receipt,consumed,args=make(monkeypatch,pair=pair)
    result=bench.build_momentum_candidate(body,receipt,consumed,**args)
    assert result['pip_size']==args['metadata']['pip_size']
    assert result['status']=='available'


def test_negative_direction(monkeypatch):
    def reverse(value):
        prices=[{side:row[side] for side in ('mid','bid','ask')} for row in value['candles']]
        for row,p in zip(value['candles'],reversed(prices)):
            row.update(p)
    body,receipt,consumed,args=make(monkeypatch,mutate=reverse)
    result=bench.build_momentum_candidate(body,receipt,consumed,**args)
    assert result['side']==-1 and Decimal(result['expected_signed_move_pips'])<0


def test_exact_neutral_does_not_invent_direction(monkeypatch):
    def flat(value):
        for row in value['candles']:
            for side in ('bid','ask','mid'):
                row[side]=copy.deepcopy(value['candles'][0][side])
    body,receipt,consumed,args=make(monkeypatch,mutate=flat)
    result=bench.build_momentum_candidate(body,receipt,consumed,**args)
    assert result['reason_code']=='momentum_exactly_neutral' and 'side' not in result


@pytest.mark.parametrize('mutate,reason',[
    (lambda value:value['candles'].__delitem__(-7),'momentum_exact_window_endpoint_missing'),
    (lambda value:value['candles'].__delitem__(slice(7,11)),'momentum_original_sampling_support_unavailable'),
    (lambda value:value['candles'].__delitem__(slice(0,10)),'momentum_thirteen_real_rows_required'),
])
def test_missing_real_support_never_filled(monkeypatch,mutate,reason):
    body,receipt,consumed,args=make(monkeypatch,mutate=mutate)
    result=bench.build_momentum_candidate(body,receipt,consumed,**args)
    assert result['status']=='unavailable' and result['reason_code']==reason
    assert 'expected_move_pips' not in result


def test_source_reference_stale(monkeypatch):
    body,receipt,consumed,args=make(monkeypatch,decision=NOW+40)
    assert bench.build_momentum_candidate(body,receipt,consumed,**args)['reason_code']=='momentum_reference_stale_or_future'


@pytest.mark.parametrize('field,value',[
    ('raw_sha256','0'*64),('receipt_sha256','0'*64),
    ('read_started_epoch',NOW),('read_completed_epoch',NOW+2),
])
def test_consumption_must_be_exact_and_available(monkeypatch,field,value):
    body,receipt,consumed,args=make(monkeypatch)
    consumed[field]=value
    with pytest.raises(ValueError,match='momentum_'):
        bench.build_momentum_candidate(body,receipt,consumed,**args)


@pytest.mark.parametrize('field,value,reason',[
    ('tradeable',False,'momentum_current_quote_not_tradeable'),
    ('tradeable',1,'momentum_current_quote_not_tradeable'),
    ('market_epoch',NOW-6,'momentum_current_quote_stale'),
    ('ask','1.20000','momentum_positive_quote_spread_required'),
])
def test_quote_refusals(monkeypatch,field,value,reason):
    body,receipt,consumed,args=make(monkeypatch)
    args['quote'][field]=value
    assert bench.build_momentum_candidate(body,receipt,consumed,**args)['reason_code']==reason


@pytest.mark.parametrize('field,value',[
    ('instrument','EUR_USD'),('available_epoch',NOW+2),('market_epoch',NOW+2),
    ('bid',1.2),('bid','NaN'),('ask',True),('bid','-1'),('bid','Infinity'),
])
def test_invalid_quote_is_not_sanitized_to_valid(monkeypatch,field,value):
    body,receipt,consumed,args=make(monkeypatch)
    args['quote'][field]=value
    with pytest.raises(ValueError,match='momentum_'):
        bench.build_momentum_candidate(body,receipt,consumed,**args)


@pytest.mark.parametrize('decision,target',[(True,NOW+600),(float('nan'),NOW+600),(NOW+1,NOW),
    (NOW+1,NOW+4000),(NOW+1,float('inf'))])
def test_bad_clocks(monkeypatch,decision,target):
    body,receipt,consumed,args=make(monkeypatch)
    args.update(decision_epoch=decision,target_epoch=target)
    with pytest.raises(ValueError,match='momentum_'):
        bench.build_momentum_candidate(body,receipt,consumed,**args)


def test_decimal_context_does_not_change_output(monkeypatch):
    body,receipt,consumed,args=make(monkeypatch)
    baseline=bench.build_momentum_candidate(body,receipt,consumed,**args)
    with localcontext() as context:
        context.prec=4
        assert bench.build_momentum_candidate(body,receipt,consumed,**args)==baseline


def test_source_bytes_cannot_be_changed(monkeypatch):
    body,receipt,consumed,args=make(monkeypatch)
    with pytest.raises(ValueError,match='raw_response_binding_mismatch'):
        bench.build_momentum_candidate(body+b' ',receipt,consumed,**args)


@pytest.mark.parametrize('value',['1e-999999999','1e-31','1e31','1.000000000000000000000000000000001'])
def test_extreme_decimal_refused_before_arithmetic(monkeypatch,value):
    body,receipt,consumed,args=make(monkeypatch)
    args['quote']['bid']=value
    with pytest.raises(ValueError,match='momentum_'):
        bench.build_momentum_candidate(body,receipt,consumed,**args)


def test_returned_evidence_has_no_mutable_alias(monkeypatch):
    body,receipt,consumed,args=make(monkeypatch)
    original_consumed=copy.deepcopy(consumed)
    first=bench.build_momentum_candidate(body,receipt,consumed,**args)
    first['policy']['windows_sec'].append(999)
    first['source_consumption']['read_completed_epoch']=0
    assert consumed==original_consumed
    second=bench.build_momentum_candidate(body,receipt,consumed,**args)
    assert second['policy']['windows_sec']==[30,60]
    consumed['read_completed_epoch']=0
    assert second['source_consumption']==original_consumed
