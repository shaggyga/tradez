import copy
from datetime import datetime,timezone
from decimal import Decimal, localcontext
import hashlib
import json
from pathlib import Path

import pytest
import requests

import oanda_live_account_readonly_status as credentials
import oanda_s5_mba_research_capture_v1 as source

NOW=1788932001.0


def metadata(pair='EUR_USD'):
    return {'instrument':pair,'base_currency':pair[:3],'quote_currency':pair[4:],
            'pip_size':.01 if pair=='USD_JPY' else .0001}


def payload(pair='EUR_USD',count=20):
    first=int((NOW-10)//5)*5-(count-1)*5
    candles=[]
    scale=Decimal('0.01') if pair=='USD_JPY' else Decimal('0.0001')
    for i in range(count):
        mid=(Decimal('170.10') if pair=='USD_JPY' else Decimal('1.20000'))+scale*Decimal(i)/10
        series={}
        for side,offset in [('bid',-scale),('ask',scale*Decimal('0.9')),('mid',Decimal(0))]:
            center=mid+offset
            series[side]={'o':str(center-scale/10),'h':str(center+scale/5),
                          'l':str(center-scale/5),'c':str(center)}
        candles.append({'complete':True,'volume':i+1,
            'time':datetime.fromtimestamp(first+i*5,timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000000000Z'),**series})
    return {'instrument':pair,'granularity':'S5','candles':candles}


def raw(value):return json.dumps(value,separators=(',',':')).encode()


def reseal(receipt):
    receipt['receipt_sha256']=source._hash({k:v for k,v in receipt.items() if k!='receipt_sha256'})
    return receipt


def install_fake(monkeypatch,body,*,status=200,error=None):
    calls=[]
    class Response:
        status_code=status
        headers={'Date':'test-server-date','Content-Type':'application/json','RequestID':'test-request'}
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def iter_content(self,chunk_size):
            assert chunk_size==8192
            if error:raise error
            for start in range(0,len(body),8192):yield body[start:start+8192]
    class Session:
        trust_env=True
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def mount(self,prefix,adapter):
            assert prefix=='https://' and adapter.max_retries.total==0
        def get(self,url,**kwargs):
            assert self.trust_env is False
            calls.append((url,kwargs))
            return Response()
    monkeypatch.setattr(source.requests,'Session',Session)
    monkeypatch.setattr(credentials,'read_creds',lambda _: 'unused synthetic fixture')
    monkeypatch.setattr(credentials,'cfg_value',lambda _,*keys: 'disposable-'+'auth')
    return calls


def captured(monkeypatch,value=None):
    value=payload() if value is None else value
    pair=value['instrument'];body=raw(value)
    calls=install_fake(monkeypatch,body)
    clocks=iter([NOW-1,NOW,NOW+.1])
    actual,receipt=source.capture_once(pair,credential_path=Path('unused'),metadata=metadata(pair),clock=lambda:next(clocks))
    assert actual==body and receipt['status']=='captured_not_issued'
    return actual,receipt,calls


@pytest.mark.parametrize('pair',source.PAIRS)
def test_only_fixed_get_and_exact_metadata(pair,monkeypatch):
    body,receipt,calls=captured(monkeypatch,payload(pair))
    assert len(calls)==1
    url,options=calls[0]
    assert url==source.HOST+'/v3/instruments/'+pair+'/candles'
    assert options['params']=={'granularity':'S5','price':'MBA','count':120,'smooth':'false'}
    assert options['allow_redirects'] is False and options['timeout']==(3.05,8) and options['stream'] is True
    assert set(options['headers'])=={'Authorization'}
    assert receipt['metadata']==metadata(pair) and receipt['first_observed_epoch']==NOW
    assert 'disposable-auth' not in json.dumps(receipt) and all(receipt[k] is v for k,v in source.FLAGS.items())
    mapped=source.map_verified_capture(body,receipt,metadata(pair),'official_midpoint')
    assert mapped['metadata']==metadata(pair)


@pytest.mark.parametrize('pair',['EUR_CAD','EUR_USD?x=1','../EUR_USD','eur_usd','USD_USD'])
def test_unknown_instrument_never_requests(pair,monkeypatch):
    calls=install_fake(monkeypatch,b'{}')
    with pytest.raises(ValueError,match='allowlisted'):
        source.capture_once(pair,credential_path=Path('unused'),metadata=metadata(),clock=lambda:NOW)
    assert calls==[]


def test_exact_ohlc_both_variants_original_clock_no_tradeability(monkeypatch):
    body,receipt,_=captured(monkeypatch)
    m=source.map_verified_capture(body,receipt,metadata(),'official_midpoint')
    ba=source.map_verified_capture(body,receipt,metadata(),'ba_derived_midpoint')
    assert m['complete_rows']==ba['complete_rows']
    assert m['complete_rows'][0]['mid']==json.loads(body)['candles'][0]['mid']
    assert len(m['complete_rows'])==20 and len(m['last13_feature_rows'])==13
    assert m['last13_feature_rows'][-1]['mid_close']-ba['last13_feature_rows'][-1]['mid_close']==pytest.approx(.000005)
    assert m['first_observed_epoch']==m['source_attestation']['read_completed_epoch']==NOW
    assert all(row['available_epoch']==NOW and row['price_epoch']==row['bar_label_epoch']+5 for row in m['complete_rows'])
    assert all('tradeable' not in row for row in m['complete_rows'])
    assert m['coverage']['tradeability_observed'] is False
    assert m['historical_ingestion_equivalence_proven'] is False


def test_provider_gap_remains_absent_and_full_query_range_is_bounded(monkeypatch):
    value=payload();del value['candles'][12]
    body,receipt,_=captured(monkeypatch,value)
    m=source.map_verified_capture(body,receipt,metadata(),'official_midpoint')
    assert len(m['complete_rows'])==19 and m['coverage']['missing_s5_intervals']==1
    assert m['coverage']['first_complete_bar_label_epoch']==m['complete_rows'][0]['bar_label_epoch']
    assert m['coverage']['last_complete_bar_label_epoch']==m['complete_rows'][-1]['bar_label_epoch']
    assert m['source_attestation']['coverage_start_label_epoch']==m['complete_rows'][0]['bar_label_epoch']
    assert m['source_attestation']['raw_source_sha256']==hashlib.sha256(body).hexdigest()


def test_incomplete_last_candle_retained_separately_not_features_or_coverage(monkeypatch):
    value=payload();value['candles'][-1]['complete']=False
    body,receipt,_=captured(monkeypatch,value)
    m=source.map_verified_capture(body,receipt,metadata(),'official_midpoint')
    assert len(m['incomplete_rows'])==1 and len(m['complete_rows'])==19
    assert m['coverage']['last_complete_bar_label_epoch']<m['incomplete_rows'][0]['bar_label_epoch']
    assert m['last13_feature_rows'][-1]['bar_start_epoch']==m['complete_rows'][-1]['bar_label_epoch']


def test_short_complete_response_is_not_padded(monkeypatch):
    body,receipt,_=captured(monkeypatch,payload(count=8))
    m=source.map_verified_capture(body,receipt,metadata(),'official_midpoint')
    assert len(m['complete_rows'])==8 and m['last13_feature_rows']==[]
    assert m['feature_rows_status']=='fewer_than_13_complete_real_bars'


@pytest.mark.parametrize('mutate',[
    lambda p:p['candles'][0]['bid'].update(c=1.2),
    lambda p:p['candles'][0]['bid'].update(c='NaN'),
    lambda p:p['candles'][0]['bid'].update(c='1e2'),
    lambda p:p['candles'][0]['bid'].update(c='0'),
    lambda p:p['candles'][0]['bid'].update(h='1'),
    lambda p:p['candles'][0]['ask'].update(c='1.19'),
    lambda p:p['candles'][0].update(complete='true'),
    lambda p:p['candles'][0].update(volume=True),
    lambda p:p['candles'][0].update(volume=-1),
    lambda p:p['candles'][0].update(time=p['candles'][1]['time']),
    lambda p:p['candles'][0].update(time=p['candles'][0]['time'].replace('.000000000Z','.000000001Z')),
    lambda p:p['candles'][0].update(time=p['candles'][0]['time'].replace('Z','')),
    lambda p:p['candles'][0].update(complete=False),
    lambda p:p['candles'][0].update(account='untrusted extra'),
    lambda p:p.update(granularity='M1'),
])
def test_full_response_adversaries_fail_without_retained_invalid_body(monkeypatch,mutate):
    value=payload();mutate(value)
    calls=install_fake(monkeypatch,raw(value))
    clocks=iter([NOW-1,NOW,NOW+.1])
    body,receipt=source.capture_once('EUR_USD',credential_path=Path('unused'),metadata=metadata(),clock=lambda:next(clocks))
    assert body==b'' and receipt['status']=='failed' and len(calls)==1
    assert receipt['error_reason']!='request_or_validation_failed'


@pytest.mark.parametrize('mutate',[
    lambda r:r.update(can_place_orders=True),
    lambda r:r.update(execution_eligible=True),
    lambda r:r.update(extra_authority_claim=True),
    lambda r:r['request']['params'].update(count=20),
    lambda r:r['request'].update(url=source.HOST+'/v3/accounts/anything'),
    lambda r:r.update(first_observed_epoch=NOW+.5),
    lambda r:r.update(capture_completed_epoch=NOW-1),
    lambda r:r.update(capture_completed_epoch=NOW+30),
    lambda r:r.update(complete_provider_response=False),
    lambda r:r['coverage'].update(missing_s5_intervals=100),
    lambda r:r.update(complete_candle_count=1),
    lambda r:r['source_bindings'].update(oanda_s5_mba_research_capture_v1_py='0'*64),
])
def test_resealed_receipt_derived_proof_mutations_rejected(monkeypatch,mutate):
    body,receipt,_=captured(monkeypatch);mutate(receipt);reseal(receipt)
    with pytest.raises(ValueError):source.map_verified_capture(body,receipt,metadata(),'official_midpoint')


def test_raw_and_metadata_tamper(monkeypatch):
    body,receipt,_=captured(monkeypatch)
    with pytest.raises(ValueError,match='raw_response_binding'):
        source.map_verified_capture(body+b' ',receipt,metadata(),'official_midpoint')
    changed=metadata();changed['pip_size']=.01
    with pytest.raises(ValueError,match='metadata_binding'):
        source.map_verified_capture(body,receipt,changed,'official_midpoint')
    with pytest.raises(ValueError,match='convention'):
        source.map_verified_capture(body,receipt,metadata(),'unknown')


@pytest.mark.parametrize('convention',source.CONVENTIONS)
def test_source_arithmetic_and_mapping_hash_ignore_caller_decimal_precision(monkeypatch,convention):
    body,receipt,_=captured(monkeypatch)
    normal=source.map_verified_capture(body,receipt,metadata(),convention)
    with localcontext() as context:
        context.prec=3
        low=source.map_verified_capture(body,receipt,metadata(),convention)
    assert low==normal and low['mapping_sha256']==normal['mapping_sha256']


@pytest.mark.parametrize('pip',['1e-1000','0.'+'0'*100+'1','1','NaN',True])
def test_pip_precision_and_magnitude_are_bounded_before_request(monkeypatch,pip):
    calls=install_fake(monkeypatch,b'{}');values=metadata();values['pip_size']=pip
    with pytest.raises(ValueError,match='pip'):
        source.capture_once('EUR_USD',credential_path=Path('unused'),metadata=values,clock=lambda:NOW)
    assert calls==[]


def test_map_replays_raw_even_after_new_seal(monkeypatch):
    body,receipt,_=captured(monkeypatch)
    value=json.loads(body);value['candles'][0]['mid']['c']='999';changed=raw(value)
    receipt['raw_response']={'sha256':hashlib.sha256(changed).hexdigest(),'bytes':len(changed)};reseal(receipt)
    with pytest.raises(ValueError,match='ohlc'):
        source.map_verified_capture(changed,receipt,metadata(),'official_midpoint')


@pytest.mark.parametrize('status',[301,401,429,500])
def test_http_failures_no_redirect_or_retry_or_private_body(monkeypatch,status):
    calls=install_fake(monkeypatch,b'private untrusted response',status=status)
    body,receipt=source.capture_once('EUR_USD',credential_path=Path('unused'),metadata=metadata(),clock=lambda:NOW)
    assert len(calls)==1 and body==b'' and receipt['error_reason']=='non_200_response'
    assert 'private' not in json.dumps(receipt)


def test_requests_exception_never_serializes_auth_or_raw_details(monkeypatch):
    calls=install_fake(monkeypatch,b'{}',error=requests.Timeout('Authorization sensitive-body'))
    body,receipt=source.capture_once('EUR_USD',credential_path=Path('unused'),metadata=metadata(),clock=lambda:NOW)
    assert body==b'' and len(calls)==1 and receipt['error_type']=='Timeout'
    assert 'sensitive-body' not in json.dumps(receipt) and 'Authorization' not in json.dumps(receipt)


def test_payload_size_budget(monkeypatch):
    calls=install_fake(monkeypatch,b' '* (source.MAX_BYTES+1))
    body,receipt=source.capture_once('EUR_USD',credential_path=Path('unused'),metadata=metadata(),clock=lambda:NOW)
    assert body==b'' and len(calls)==1 and receipt['error_reason']=='response_budget_exceeded'


def test_actual_elapsed_budget(monkeypatch):
    calls=install_fake(monkeypatch,raw(payload()))
    ticks=iter([0,16]);monkeypatch.setattr(source.time,'monotonic',lambda:next(ticks))
    body,receipt=source.capture_once('EUR_USD',credential_path=Path('unused'),metadata=metadata(),clock=lambda:NOW)
    assert body==b'' and len(calls)==1 and receipt['error_reason']=='response_budget_exceeded'


def test_duplicate_json_key_rejected(monkeypatch):
    body=raw(payload()).replace(b'"instrument":"EUR_USD"',b'"instrument":"EUR_USD","instrument":"EUR_USD"')
    install_fake(monkeypatch,body)
    _,receipt=source.capture_once('EUR_USD',credential_path=Path('unused'),metadata=metadata(),clock=lambda:NOW)
    assert receipt['error_reason']=='duplicate_json_key'


def test_mapper_is_pure_and_capture_outputs_compatible_features(monkeypatch):
    body,receipt,_=captured(monkeypatch)
    monkeypatch.setattr(source.requests,'Session',lambda: (_ for _ in ()).throw(AssertionError('unexpected GET')))
    import oanda_recovered_second_curve_v1 as curve
    mapped=source.map_verified_capture(body,receipt,metadata(),'ba_derived_midpoint')
    capture=curve.capture_s5_rows(mapped['last13_feature_rows'],instrument='EUR_USD',pip_size=.0001,
        source_sha256=mapped['source_sha256'],clock=lambda:NOW+.5,scope='current_research',
        sampling_policy='retained_fit_window',price_convention=mapped['price_convention'])
    assert curve.validate_capture(capture) and capture['reference_available_epoch']==NOW
    assert mapped==source.map_verified_capture(body,receipt,metadata(),'ba_derived_midpoint')


def test_real_retained_mba_series_parse_without_new_network():
    path=Path(__file__).resolve().parent.parent/'overnight_curve_buildout_20260909/second_ridge/current_s5_capture_001/OANDA_EUR_USD_S5_MBA_RAW.json'
    if not path.exists():pytest.skip('one-shot actual response absent')
    body=path.read_bytes()
    assert hashlib.sha256(body).hexdigest()=='4fa6405630a9346410bd75be87718a9e6cbeaf4727f53ccc9dab266cccdc47c4'
    complete,incomplete=source._parse_response(body,'EUR_USD',1788929533.3450396)
    assert len(complete)==20 and not incomplete
    assert source._coverage(complete)['missing_s5_intervals']>=2
    assert complete[-1]['mid']['c']=='1.16316'
