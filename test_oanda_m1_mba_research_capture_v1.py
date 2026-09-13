from copy import deepcopy
from datetime import datetime,timezone
from decimal import Context,Decimal,localcontext
import csv,io,json

import pytest

import oanda_m1_mba_research_capture_v1 as m
import oanda_m1_path_risk_labels_v1 as labels

NOW=1788939601.

def metadata(pair='EUR_USD'):
    return dict(instrument=pair,base_currency=pair[:3],quote_currency=pair[4:],pip_size='0.01' if pair=='USD_JPY' else '0.0001')

def payload(pair='EUR_USD',n=300,*,incomplete=False,gap=None):
    last=int(NOW)//60*60-60
    out=[]
    for i in range(n):
        stamp=last-(n-1-i)*60
        with localcontext(Context(prec=96)):
            mid=Decimal('150' if pair=='USD_JPY' else '1.1')+Decimal(i)*Decimal('0.00001')
            spread=Decimal('.01' if pair=='USD_JPY' else '.0001')
            series={}
            for side,close in [('mid',mid),('bid',mid-spread),('ask',mid+spread)]:
                series[side]={k:format(v,'.8f') for k,v in dict(o=close,h=close+spread,l=close-spread,c=close).items()}
        out.append(dict(complete=True,volume=100+i,time=datetime.fromtimestamp(stamp,timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000000000Z'),**series))
    if gap is not None:out.pop(gap)
    if incomplete:out[-1]['complete']=False
    return dict(instrument=pair,granularity='M1',candles=out)

def raw_of(value):return json.dumps(value,separators=(',',':')).encode()

class Response:
    def __init__(self,raw,status=200):self.raw=raw;self.status_code=status;self.headers={'Date':'diagnostic date','Content-Type':'application/json','RequestID':'request-fixture'}
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def iter_content(self,chunk_size):
        for i in range(0,len(self.raw),chunk_size):yield self.raw[i:i+chunk_size]

def fake_capture(monkeypatch,raw,*,pair='EUR_USD',status=200,clocks=None,error=None):
    calls=[];state={}
    class Session:
        def __init__(self):self.trust_env=True
        def __enter__(self):return self
        def __exit__(self,*args):state['closed']=True
        def mount(self,url,adapter):state['mount']=(url,adapter.max_retries.total)
        def get(self,url,**kw):
            calls.append(dict(url=url,**kw));state['trust_env']=self.trust_env
            if error is not None:raise error
            return Response(raw,status)
    monkeypatch.setattr(m.requests,'Session',Session)
    monkeypatch.setattr(m,'_token',lambda _: 'synthetic-private-token')
    clock=iter(clocks or [NOW,NOW+.1,NOW+.2,NOW+.3])
    output=m.capture_once(pair,credential_path='not-read',metadata=metadata(pair),clock=lambda:next(clock))
    return *output,calls,state

def reseal(receipt):
    receipt['receipt_sha256']=m._hash({k:v for k,v in receipt.items() if k!='receipt_sha256'})

def make_capture(monkeypatch,pair='EUR_USD',n=300,**kwargs):
    raw,receipt,_,_=fake_capture(monkeypatch,raw_of(payload(pair,n,**kwargs)),pair=pair)
    assert receipt['status']=='captured_not_issued'
    return raw,receipt,m.map_verified_capture(raw,receipt,metadata(pair))


@pytest.mark.parametrize('pair',m.PAIRS)
def test_one_fixed_practice_GET_no_account_retry_redirect_or_environment(monkeypatch,pair):
    raw,receipt,calls,state=fake_capture(monkeypatch,raw_of(payload(pair)),pair=pair)
    assert receipt['status']=='captured_not_issued' and len(calls)==1
    call=calls[0]
    assert call['url']==m.HOST+'/v3/instruments/'+pair+'/candles'
    assert call['params']==dict(granularity='M1',price='MBA',count=300,smooth='false')
    assert call['allow_redirects'] is False and call['stream'] is True and call['timeout']==(3.05,8)
    assert state['trust_env'] is False and state['mount']==('https://',0) and state['closed']
    assert 'synthetic-private-token' not in json.dumps(receipt)
    assert receipt['raw_response']['sha256']==m._sha(raw)
    assert receipt['first_observed_epoch']==receipt['read_completed_epoch']==NOW+.1
    assert receipt['request_started_epoch']==NOW and receipt['capture_completed_epoch']==NOW+.2


def test_raw_to_official_M_CSV_preserves_lexemes_and_label_parser_compatibility(monkeypatch):
    raw,receipt,mapping=make_capture(monkeypatch)
    rows,info=labels.parse_csv(mapping['canonical_csv_bytes'],'EUR_USD',observed_epoch=receipt['read_completed_epoch'])
    assert len(rows)==300 and info['bid_ask_missingness']=={}
    records=list(csv.DictReader(io.StringIO(mapping['canonical_csv_bytes'].decode())))
    original=json.loads(raw)['candles']
    for source,row,parsed in zip(original,records,rows):
        assert row['close']==source['mid']['c'] and row['bid_open']==source['bid']['o'] and row['ask_low']==source['ask']['l']
        assert parsed['price_epoch']==parsed['label_epoch']+60
    assert 'spread_pips' not in records[0]
    assert mapping['coverage']['consecutive_complete_suffix_rows']==300
    scale,reason=labels.past_volatility_scale(rows,299,60)
    assert scale>0 and reason is None
    assert mapping['semantics']['midpoint_training_ingestion_equivalence_proven'] is False


def test_completed_rows_and_incomplete_rows_retained_but_never_filled(monkeypatch):
    _,receipt,out=make_capture(monkeypatch,incomplete=True,gap=270)
    assert receipt['candle_count']==299 and receipt['complete_candle_count']==298
    assert len(out['incomplete_rows'])==1 and len(out['complete_rows'])==298
    assert out['coverage']['missing_m1_intervals']==1
    assert out['coverage']['consecutive_complete_suffix_rows']==28
    rows,_=labels.parse_csv(out['canonical_csv_bytes'],'EUR_USD',observed_epoch=out['first_observed_epoch'])
    assert len(rows)==298
    scale,reason=labels.past_volatility_scale(rows,len(rows)-1,15)
    assert scale is None and reason=='past60_complete_support_missing'


def test_all_incomplete_returns_explicit_empty_complete_domain(monkeypatch):
    raw=payload(n=1,incomplete=True)
    body,receipt,_,_=fake_capture(monkeypatch,raw_of(raw))
    mapping=m.map_verified_capture(body,receipt,metadata())
    assert mapping['status']=='no_complete_rows'
    assert mapping['coverage']['first_complete_bar_label_epoch'] is None
    assert mapping['canonical_csv']['row_count']==0 and len(mapping['incomplete_rows'])==1


def test_map_is_pure_original_clock_and_low_decimal_precision_invariant(monkeypatch):
    raw,receipt,a=make_capture(monkeypatch)
    original=deepcopy(receipt)
    monkeypatch.setattr(m,'verify_source_bindings_on_disk',lambda:(_ for _ in ()).throw(AssertionError('mapper I/O')))
    with localcontext(Context(prec=3)):
        b=m.map_verified_capture(raw,receipt,metadata())
    assert a==b and receipt==original
    assert b['first_observed_epoch']==NOW+.1
    document=m.mapping_document(b)
    assert 'canonical_csv_bytes' not in document
    assert document['mapping_sha256']==m._hash({k:v for k,v in document.items() if k!='mapping_sha256'})
    document['coverage']['gaps'].append('mutated')
    assert b['coverage']['gaps']==[]


@pytest.mark.parametrize('mutation',['identity','granularity','duplicate','order','nan','bool_complete','missing_side',
    'float_price','crossed','outside','nonminute','fraction','future','future_close','complete_after_incomplete',
    'zero','negative','exponent','too_precise','volume','extra','too_many'])
def test_provider_defects_keep_raw_failed_evidence_and_refuse_mapping(monkeypatch,mutation):
    p=payload(n=3);c=p['candles']
    if mutation=='identity':p['instrument']='USD_JPY'
    if mutation=='granularity':p['granularity']='S5'
    if mutation=='duplicate':c[1]['time']=c[0]['time']
    if mutation=='order':c.reverse()
    if mutation=='nan':c[0]['volume']=float('nan')
    if mutation=='bool_complete':c[0]['complete']=1
    if mutation=='missing_side':del c[0]['bid']
    if mutation=='float_price':c[0]['mid']['c']=1.1
    if mutation=='crossed':c[0]['bid']['h']='9'
    if mutation=='outside':c[0]['mid']['c']='9'
    if mutation=='nonminute':c[0]['time']=c[0]['time'].replace(':00.000000000',':05.000000000')
    if mutation=='fraction':c[0]['time']=c[0]['time'].replace('.000000000','.000000001')
    if mutation=='future':c[-1]['time']=datetime.fromtimestamp(NOW+119,timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    if mutation=='future_close':c[-1]['time']=datetime.fromtimestamp(int(NOW)//60*60,timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    if mutation=='complete_after_incomplete':c[0]['complete']=False
    if mutation=='zero':c[0]['mid']['l']='0'
    if mutation=='negative':c[0]['mid']['l']='-1'
    if mutation=='exponent':c[0]['mid']['c']='1e0'
    if mutation=='too_precise':c[0]['mid']['c']='1.'+'1'*33
    if mutation=='volume':c[0]['volume']=True
    if mutation=='extra':p['orders_enabled']=True
    if mutation=='too_many':p=payload(n=301)
    raw=raw_of(p);body,receipt,_,_=fake_capture(monkeypatch,raw)
    assert receipt['status']=='failed' and body==raw
    assert receipt['raw_response']['retention_scope']=='complete_response'
    with pytest.raises(ValueError,match='successful_receipt'):m.map_verified_capture(body,receipt,metadata())


def test_duplicate_json_keys_rejected_without_silent_collapse(monkeypatch):
    raw=raw_of(payload(n=1)).replace(b'"granularity":"M1"',b'"granularity":"M1","granularity":"M1"')
    body,receipt,_,_=fake_capture(monkeypatch,raw)
    assert body==raw and receipt['status']=='failed' and receipt['error_reason']=='m1_duplicate_json_key'


@pytest.mark.parametrize('mutation',['raw','hash','source','metadata','request','flags','coverage','count','first_observed',
    'time_text','future_read','HTTP','complete_query','extra','limits'])
def test_resealed_receipt_cannot_change_original_source_contract(monkeypatch,mutation):
    raw,receipt,_=make_capture(monkeypatch)
    if mutation=='raw':raw+=b' '
    if mutation=='hash':receipt['receipt_sha256']='0'*64
    if mutation=='source':receipt['source_bindings'][m.__name__+'.py']='0'*64
    if mutation=='metadata':receipt['metadata']['pip_size']='0.01'
    if mutation=='request':receipt['request']['params']['count']=204
    if mutation=='flags':receipt['execution_eligible']=True
    if mutation=='coverage':receipt['coverage']['last_complete_bar_label_epoch']+=60
    if mutation=='count':receipt['complete_candle_count']-=1
    if mutation=='first_observed':receipt['first_observed_epoch']+=1
    if mutation=='time_text':receipt['request_started_utc']='yesterday'
    if mutation=='future_read':receipt['read_completed_epoch']=NOW+30
    if mutation=='HTTP':receipt['http_status']=201
    if mutation=='complete_query':receipt['source_query_complete']=False
    if mutation=='extra':receipt['account_id']='fake'
    if mutation=='limits':receipt['limits']['response_bytes']+=1
    if mutation!='hash':reseal(receipt)
    with pytest.raises(ValueError):m.map_verified_capture(raw,receipt,metadata())


def test_source_query_domain_never_extends_before_first_or_after_last_complete(monkeypatch):
    _,receipt,out=make_capture(monkeypatch,n=20,gap=10)
    proof=out['source_attestation'];rows=out['complete_rows']
    assert proof['first_complete_bar_label_epoch']==rows[0]['bar_label_epoch']
    assert proof['last_complete_bar_label_epoch']==rows[-1]['bar_label_epoch']
    assert proof['read_completed_epoch']==receipt['first_observed_epoch']
    assert out['coverage']['missing_m1_intervals']==1
    assert out['semantics']['tradeability_observed'] is False


@pytest.mark.parametrize('status',[301,401,429,500])
def test_HTTP_errors_retained_without_redirect_retry_or_token_leak(monkeypatch,status):
    raw=b'{"errorMessage":"fixture only"}'
    body,receipt,calls,_=fake_capture(monkeypatch,raw,status=status)
    assert body==raw and receipt['status']=='failed' and len(calls)==1
    assert 'synthetic-private-token' not in json.dumps(receipt)


def test_timeout_failure_never_serializes_request_exception_secrets(monkeypatch):
    # This constructed value is an explicitly synthetic redaction fixture.
    body,receipt,calls,_=fake_capture(monkeypatch,b'',error=RuntimeError('Bearer '+'synthetic-private-token private account'))
    assert body==b'' and len(calls)==1 and receipt['status']=='failed'
    assert receipt['error_reason']=='m1_request_or_validation_failed'
    assert 'synthetic-private-token' not in json.dumps(receipt)


def test_oversize_response_is_explicit_partial_diagnostic(monkeypatch):
    body,receipt,calls,_=fake_capture(monkeypatch,b'x'*(m.MAX_BYTES+1))
    assert len(body)==m.MAX_BYTES and len(calls)==1
    assert receipt['status']=='failed' and receipt['complete_provider_response'] is False
    assert receipt['raw_response']['retention_scope']=='partial_or_no_response'


@pytest.mark.parametrize('clocks',[[NOW,NOW-1,NOW+1],[NOW,NOW+21,NOW+22],[NOW,NOW+.1,NOW+21,NOW+22]])
def test_reversed_or_over_budget_actual_clocks_fail(monkeypatch,clocks):
    _,receipt,calls,_=fake_capture(monkeypatch,raw_of(payload(n=2)),clocks=clocks)
    assert receipt['status']=='failed' and len(calls)==1


def test_mapping_document_refuses_CSV_or_metadata_tamper(monkeypatch):
    _,_,mapping=make_capture(monkeypatch)
    bad=deepcopy(mapping);bad['canonical_csv_bytes']+=b'\n'
    with pytest.raises(ValueError,match='CSV_binding'):m.mapping_document(bad)
    bad=deepcopy(mapping);bad['coverage']['missing_m1_intervals']=5
    with pytest.raises(ValueError,match='mapping_seal'):m.mapping_document(bad)


@pytest.mark.parametrize('mutation',['duration_bool','first_observation_bool','request_numeric_boolean'])
def test_resealed_clock_or_transport_type_coercion_is_rejected(monkeypatch,mutation):
    raw,receipt,_=make_capture(monkeypatch)
    if mutation=='duration_bool':receipt['request_duration_sec']=False
    if mutation=='first_observation_bool':receipt['first_observed_epoch']=True
    if mutation=='request_numeric_boolean':receipt['request']['redirects_allowed']=0
    reseal(receipt)
    with pytest.raises(ValueError):m.map_verified_capture(raw,receipt,metadata())


def test_invalid_pair_or_metadata_refused_before_any_GET(monkeypatch):
    monkeypatch.setattr(m,'_token',lambda _:(_ for _ in ()).throw(AssertionError('credentials touched')))
    for pair in ['EUR_JPY','EUR_USD/../../accounts','eur_usd']:
        with pytest.raises(ValueError):m.capture_once(pair,credential_path='unused',metadata=metadata(pair))
    md=metadata();md['pip_size']=.0001
    with pytest.raises(ValueError):m.capture_once('EUR_USD',credential_path='unused',metadata=md)
