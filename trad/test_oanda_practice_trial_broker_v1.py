"""Offline fake-transport tests. No credentials, network, account or broker writes."""
from copy import deepcopy
from decimal import getcontext
import hashlib
import json
import pytest
from datetime import datetime, timezone

import oanda_practice_trial_broker_v1 as b

ACCOUNT = '-'.join(['101', '001', '00000000', '007'])
FINGERPRINT = hashlib.sha256(ACCOUNT.encode()).hexdigest()
NOW = 1789000000.0
MARKET_TIME = datetime.fromtimestamp(NOW,timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


class Clock:
    def __init__(self, now=NOW): self.now = now
    def __call__(self): return self.now


def encoded(payload): return json.dumps(payload).encode()


class Fake:
    def __init__(self):
        self.calls=[]; self.prepared=None; self.filled=False; self.closed=False
        self.post_timeout=False; self.post_accept=True; self.close_timeout=False
        self.trade_override=None; self.order_override=None; self.tx_override=None
        self.foreign=False; self.pending=False; self.fail_open_read=False
        self.price_payload=None; self.candle_payload=None

    def trade(self):
        p=self.prepared; order=p['order']
        result={'id':'700','instrument':order['instrument'],'initialUnits':order['units'],
                'openTime':MARKET_TIME,
                'currentUnits':'0' if self.closed else order['units'],'state':'CLOSED' if self.closed else 'OPEN',
                'clientExtensions':deepcopy(order['tradeClientExtensions']),
                'stopLossOrder':{'id':'701','state':'PENDING','price':order['stopLossOnFill']['price']}}
        if 'takeProfitOnFill' in order:
            result['takeProfitOrder']={'id':'702','state':'PENDING','price':order['takeProfitOnFill']['price']}
        if self.trade_override: self.trade_override(result)
        return result

    def __call__(self, method,url,*,headers,params,body):
        self.calls.append((method,url,deepcopy(params),deepcopy(body)))
        assert url.startswith(b.BASE_URL+'/')
        assert headers['Authorization'].startswith('Bearer ')
        if method=='POST':
            assert url.endswith('/orders')
            if self.post_accept:self.filled=True
            if self.post_timeout:raise RuntimeError('synthetic private detail')
            return 201,encoded({'orderFillTransaction':{'id':'600'}})
        if method=='PUT':
            assert url.endswith('/trades/700/close') and body=={'units':'ALL'}
            self.closed=True
            if self.close_timeout:raise RuntimeError('synthetic private detail')
            return 200,encoded({'orderFillTransaction':{'id':'800'}})
        if '/orders/@' in url:
            if not self.filled:return 404,encoded({'errorCode':'NO_SUCH_ORDER'})
            order={**deepcopy(self.prepared['order']),'id':'500','state':'FILLED','fillingTransactionID':'600',
                   'createTime':MARKET_TIME,'filledTime':MARKET_TIME}
            if self.order_override:self.order_override(order)
            return 200,encoded({'order':order})
        if url.endswith('/transactions/600'):
            order=self.prepared['order']
            tx={'id':'600','accountID':ACCOUNT,'type':'ORDER_FILL','orderID':'500',
                'time':MARKET_TIME,
                'clientOrderID':order['clientExtensions']['id'],'instrument':order['instrument'],
                'units':order['units'],'price':order['priceBound'],'tradeOpened':{'tradeID':'700'}}
            if self.tx_override:self.tx_override(tx)
            return 200,encoded({'transaction':tx})
        if url.endswith('/trades/700'):return 200,encoded({'trade':self.trade()})
        if url.endswith('/summary'):
            return 200,encoded({'account':{'id':ACCOUNT,'currency':'USD','NAV':'41.60','balance':'41.60',
                'marginUsed':'0','marginAvailable':'41.60','marginRate':'0.02','openTradeCount':int(self.foreign),
                'pendingOrderCount':int(self.pending),'hedgingEnabled':False}})
        if url.endswith('/openTrades'):
            if self.fail_open_read:raise RuntimeError('private body')
            rows=[self.trade()] if self.filled and not self.closed else []
            if self.foreign:rows.append({'id':'900','instrument':'GBP_USD','currentUnits':'1'})
            return 200,encoded({'trades':rows,'lastTransactionID':'900'})
        if url.endswith('/pendingOrders'):
            return 200,encoded({'orders':[{'id':'123','state':'PENDING'}] if self.pending else []})
        if url.endswith('/pricing'):return 200,encoded(self.price_payload)
        if url.endswith('/candles'):return 200,encoded(self.candle_payload)
        if url.endswith('/transactions/sinceid'):
            return 200,encoded({'transactions':[{'id':'801','accountID':ACCOUNT,'type':'ORDER_FILL','time':MARKET_TIME}],'lastTransactionID':'801'})
        if url.endswith('/instruments'):return 200,encoded({'instruments':[{'name':'EUR_USD','pipLocation':-4,'marginRate':'0.02'}]})
        raise AssertionError('Unexpected fake route')


def setup(*,units='10',take='1.12'):
    fake=Fake();clock=Clock()
    client=b.PracticeTrialBroker(account_id=ACCOUNT,token='synthetic',expected_account_sha256=FINGERPRINT,
                                trial_id='trial_test',transport=fake,clock=clock)
    prepared=client.prepare_market_order('intent_1','EUR_USD',units,'1.09' if int(units)>0 else '1.13',take,
                                         price_bound='1.10',target_epoch=NOW+3600)
    fake.prepared=prepared
    return client,fake,clock,prepared


def posts(fake):return [x for x in fake.calls if x[0]=='POST']


def test_no_io_constructor_and_fixed_scope():
    c,f,_,p=setup()
    assert f.calls==[] and p['order']['positionFill']=='OPEN_ONLY'
    assert p['order']['stopLossOnFill']=={'price':'1.09','timeInForce':'GTC'}
    assert p['original_target_epoch']==NOW+3600


@pytest.mark.parametrize('field,value,reason',[
    ('environment','live','practice_account_scope_required'),('account_key','OTHER','practice_account_scope_required'),
    ('account_id','bad','practice_007_account_required'),('expected_account_sha256','0'*64,'account_fingerprint_mismatch'),
    ('trial_id','../bad','invalid_trial_id'),('token','bad\nvalue','practice_token_unavailable')])
def test_scope_refusals(field,value,reason):
    kw=dict(account_id=ACCOUNT,token='synthetic',expected_account_sha256=FINGERPRINT,trial_id='trial',transport=Fake())
    kw[field]=value
    with pytest.raises(b.BrokerError,match=reason):b.PracticeTrialBroker(**kw)


def test_stop_only_supported():
    _,_,_,p=setup(take=None)
    assert 'takeProfitOnFill' not in p['order']


@pytest.mark.parametrize('units,stop,take,bound,target',[
    ('0','1','3','2',NOW+60),('1.0000001','1','3','2',NOW+60),('1','2','3','2',NOW+60),
    ('-1','1','0.5','2',NOW+60),('1','1','1.5','2',NOW+60),('1','1','3','2',NOW),
    ('1','1','3','2',float('nan')),('1','1','3','2',NOW+86401)])
def test_bad_prepared_orders(units,stop,take,bound,target):
    c,_,_,_=setup()
    with pytest.raises(b.BrokerError):c.prepare_market_order('intent','EUR_USD',units,stop,take,price_bound=bound,target_epoch=target)


def test_success_requires_durable_claim_then_flat_reads_then_one_post():
    c,f,_,p=setup();calls=[]
    result=c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=lambda x:calls.append(x) or True)
    assert result['status']=='filled_open' and len(posts(f))==1 and len(calls)==1
    post_index=next(i for i,x in enumerate(f.calls) if x[0]=='POST')
    assert [x[1].split('/')[-1] for x in f.calls[post_index-3:post_index]]==['summary','openTrades','pendingOrders']
    assert result['original_target_epoch']==p['original_target_epoch']
    assert 'accountID' not in result['transaction']


def test_timeout_accepted_reconciles_and_never_resubmits():
    c,f,_,p=setup();f.post_timeout=True
    first=c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=lambda _:True)
    second=c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=lambda _:pytest.fail('second claim'))
    assert first['status']==second['status']=='filled_open' and len(posts(f))==1
    assert first['submission_receipt']['reason_code']=='transport_unresolved'
    assert 'private' not in json.dumps(first['submission_receipt'])


def test_timeout_missing_stays_unknown_without_resubmit():
    c,f,_,p=setup();f.post_timeout=True;f.post_accept=False
    assert c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=lambda _:True)['status']=='unknown'
    assert c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=lambda _:True)['status']=='unknown'
    assert len(posts(f))==1


def test_restart_claim_false_prevents_retry_even_404():
    c,f,_,p=setup()
    assert c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=lambda _:False)['status']=='unknown'
    assert posts(f)==[]


@pytest.mark.parametrize('kind',['raise','nonbool'])
def test_failed_claim_prevents_post(kind):
    c,f,_,p=setup()
    def claim(_):
        if kind=='raise':raise RuntimeError('private detail')
        return 1
    assert c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=claim)['status']=='not_submitted'
    assert not posts(f)


@pytest.mark.parametrize('kind',['foreign','pending'])
def test_nonflat_blocks_after_durable_claim(kind):
    c,f,_,p=setup();setattr(f,kind,True)
    r=c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=lambda _:True)
    assert r['reason_code']=='account_not_flat_before_submission' and not posts(f)


def test_target_expires_during_claim_no_post():
    c,f,clock,p=setup()
    def claim(_):clock.now=p['original_target_epoch'];return True
    assert c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=claim)['reason_code']=='intent_or_observation_expired_after_claim'
    assert not posts(f)


def test_prepared_tamper_even_resealed_cannot_change_order_kind():
    c,f,_,p=setup();p['order']['type']='LIMIT'
    p['intent_sha256']=b._sha(b._json({k:v for k,v in p.items() if k!='intent_sha256'}))
    with pytest.raises(b.BrokerError,match='reconstruction'):c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=lambda _:True)
    assert not f.calls


@pytest.mark.parametrize('change',[
    lambda x:x.update(instrument='GBP_USD'),lambda x:x.update(units='11'),
    lambda x:x.update(clientOrderID='different'),lambda x:x.update(tradesClosed=[{'tradeID':'99'}]),
    lambda x:x.update(price='1.1001'),lambda x:x.update(accountID='other')])
def test_fill_adversaries_never_confirm(change):
    c,f,_,p=setup();f.tx_override=change
    assert c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=lambda _:True)['status']=='unknown'
    assert len(posts(f))==1


def test_missing_protective_order_unknown():
    c,f,_,p=setup();f.trade_override=lambda x:x.pop('stopLossOrder')
    assert c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=lambda _:True)['reason_code']=='protective_order_not_confirmed'


def test_broker_normalized_decimal_order_prices_accepted():
    c,f,_,p=setup()
    def changed(o):o['units']='10.0';o['priceBound']='1.10000';o['stopLossOnFill']['price']='1.09000';o['stopLossOnFill']['triggerCondition']='DEFAULT'
    f.order_override=changed
    assert c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=lambda _:True)['status']=='filled_open'


def test_close_owned_exact_and_reconciled_after_timeout():
    c,f,clock,p=setup();f.filled=True;f.close_timeout=True;clock.now=p['original_target_epoch']
    out=c.close_owned_trade('700','EUR_USD',original_target_epoch=p['original_target_epoch'],horizon_due=True)
    assert out['status']=='confirmed_reduction' and out['complete_close'] and out['transaction_attribution'] is False
    assert out['observed_reduction_units']=='10'
    assert sum(x[0]=='PUT' for x in f.calls)==1


@pytest.mark.parametrize('kind',['tag','trial','instrument','target','early'])
def test_close_foreign_or_early_refused_without_put(kind):
    c,f,_,p=setup();f.filled=True
    if kind=='tag':f.trade_override=lambda t:t['clientExtensions'].update(tag='foreign')
    if kind=='trial':f.trade_override=lambda t:t['clientExtensions'].update(comment='other|target='+repr(NOW+3600))
    kwargs={'horizon_due':kind=='early'}
    if kind=='target':kwargs['original_target_epoch']=NOW+7200
    with pytest.raises(b.BrokerError):c.close_owned_trade('700','GBP_USD' if kind=='instrument' else 'EUR_USD',**kwargs)
    assert not any(x[0]=='PUT' for x in f.calls)


def test_close_read_failure_never_means_closed():
    c,f,_,p=setup();f.filled=True;f.fail_open_read=True
    out=c.close_owned_trade('700','EUR_USD')
    assert out['status']=='unknown' and not out['complete_close']


def test_already_closed_no_second_put():
    c,f,_,p=setup();f.filled=True;f.closed=True
    assert c.close_owned_trade('700','EUR_USD')['already_closed']
    assert not any(x[0]=='PUT' for x in f.calls)


def test_normalized_account_instruments_transactions():
    c,f,_,_=setup()
    a=c.account_summary();assert a['NAV']=='41.60' and 'id' not in a and a['observed_epoch']==NOW
    assert c.instruments()['rows'][0]['pipLocation']==-4
    tx=c.transactions_since('800');assert tx['lastTransactionID']=='801' and 'accountID' not in tx['rows'][0]


def price_payload():
    return {'prices':[{'instrument':'EUR_USD','time':'2026-09-10T00:26:39.123456789Z',
                       'tradeable':True,'bids':[{'price':'1.10000'}],'asks':[{'price':'1.10002'}]}],
            'homeConversions':[{'currency':'USD','accountGain':'1','accountLoss':'1','positionValue':'1'}]}


def test_pricing_real_tradeability_conversion_and_original_clock():
    c,f,clock,_=setup();f.price_payload=price_payload();clock.now=b._epoch(f.price_payload['prices'][0]['time'])+1
    r=c.pricing(['EUR_USD']);q=r['quotes']['EUR_USD']
    assert q['ask']=='1.10002' and q['tradeable'] and q['market_time'].endswith('123456789Z')
    assert q['observed_epoch']==clock.now and r['home_conversions']['rows'][0]['positionValue']=='1'
    assert f.calls[-1][2]['includeHomeConversions']=='true'


@pytest.mark.parametrize('kind',['tradeability','crossed','future','missing_bid','duplicate_conversion'])
def test_bad_pricing_fail_closed(kind):
    c,f,clock,_=setup();f.price_payload=price_payload();r=f.price_payload['prices'][0];clock.now=b._epoch(r['time'])+1
    if kind=='tradeability':r['tradeable']='true'
    if kind=='crossed':r['bids'][0]['price']='2'
    if kind=='future':clock.now=b._epoch(r['time'])-1
    if kind=='missing_bid':r['bids']=[]
    if kind=='duplicate_conversion':f.price_payload['homeConversions']*=2
    with pytest.raises(b.BrokerError):c.pricing(['EUR_USD'])


def test_candles_keep_original_rows_no_fill():
    c,f,clock,_=setup();clock.now=1789000020.0
    last=int(clock.now//60)*60
    from datetime import datetime,timezone
    def row(t,complete):return {'time':datetime.fromtimestamp(t,timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),'complete':complete,'mid':{'h':'1.2','l':'1.0','c':'1.1'}}
    f.candle_payload={'instrument':'EUR_USD','granularity':'M1','candles':[row(last-i*60,i>0) for i in range(16,-1,-1)]}
    f.candle_payload['candles'].pop(4)
    r=c.completed_m1_candles('EUR_USD')
    assert len(r['rows'])==15 and r['rows'][-1]['label_epoch']==last-60
    assert any(y['label_epoch']-x['label_epoch']==120 for x,y in zip(r['rows'],r['rows'][1:]))
    assert f.calls[-1][2]=={'price':'M','granularity':'M1','count':'17','smooth':'false'}


@pytest.mark.parametrize('raw',[b'[]',b'{"x":NaN}',b'bad',b'x'*(b.MAX_BYTES+1)],ids=['array','nonfinite','not_json','over_bound'])
def test_malformed_response_sanitized(raw):
    c,_,_,_=setup();c._transport=lambda *a,**kw:(200,raw)
    with pytest.raises(b.BrokerError) as exc:c.account_summary()
    assert ACCOUNT not in str(exc.value) and 'Bearer' not in str(exc.value)


def test_redirect_never_followed_by_adapter():
    c,f,_,_=setup();c._transport=lambda *a,**kw:(302,b'{}')
    with pytest.raises(b.BrokerError,match='http_refusal'):c.account_summary()


def test_clock_rollback_refuses_read():
    c,f,_,_=setup();times=iter([NOW,NOW-1]);c._clock=lambda:next(times)
    with pytest.raises(b.BrokerError,match='rollback'):c.account_summary()


def test_low_decimal_context_does_not_change_order():
    original=getcontext().prec
    try:
        c,_,_,p=setup(units='12345');getcontext().prec=2
        assert c._prepared(p)==p
    finally:getcontext().prec=original


def test_low_decimal_context_cannot_round_units_under_bound():
    original=getcontext().prec
    try:
        c,_,_,_=setup();getcontext().prec=2
        with pytest.raises(b.BrokerError,match='units_precision_or_bound'):
            c.prepare_market_order('big','EUR_USD','100000001','1.09',price_bound='1.1',target_epoch=NOW+60)
    finally:getcontext().prec=original


def test_postclaim_read_failure_cannot_resubmit():
    c,f,_,p=setup();f.fail_open_read=True
    out=c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=lambda _:True)
    assert out['status']=='not_submitted' and out['reason_code']=='pretransport_read_transport_failed'
    f.fail_open_read=False
    assert c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=lambda _:pytest.fail('no new claim'))['status']=='unknown'
    assert not posts(f)


def test_public_owned_and_exact_trade_apis():
    c,f,_,p=setup();f.filled=True
    result=c.exact_trade('700')
    assert c.owned_trade(result['trade'],expected=p)==p['original_target_epoch']


@pytest.mark.parametrize('method,path,route',[
    ('POST','/accounts/other/orders','submit_market'),('GET','/accounts','account_summary'),
    ('PUT','/accounts/other/trades/700/close','close_owned_trade'),
    ('POST','/instruments/EUR_USD/candles','completed_m1_candles')])
def test_internal_transport_cannot_escape_account_or_methods(method,path,route):
    c,f,_,_=setup()
    with pytest.raises(b.BrokerError):c._request(method,path,route=route)
    assert not f.calls


def test_clock_rollback_between_reads_is_sticky():
    c,f,clock,_=setup();c.account_summary();clock.now-=1
    with pytest.raises(b.BrokerError,match='rollback'):c.pending_orders()
    clock.now+=10
    with pytest.raises(b.BrokerError,match='rollback'):c.pending_orders()


@pytest.mark.parametrize('where',['order','fill','trade'])
def test_original_future_execution_clocks_refused(where):
    c,f,_,p=setup()
    future=datetime.fromtimestamp(NOW+1,timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    if where=='order':f.order_override=lambda x:x.update(createTime=future)
    if where=='fill':f.tx_override=lambda x:x.update(time=future)
    if where=='trade':f.trade_override=lambda x:x.update(openTime=future)
    assert c.submit_market(p,validate_before_submit=lambda *args: True,claim_submission=lambda _:True)['status']=='unknown'


def test_transport_arguments_fixed_and_bounded(monkeypatch):
    seen={}
    class Response:
        status_code=200
        def __enter__(self):return self
        def __exit__(self,*args):return False
        def iter_content(self,n):assert n==65536;yield b'{}'
    class Session:
        trust_env=True
        def request(self,*args,**kw):seen.update(kw);return Response()
        def close(self):pass
    monkeypatch.setattr(b.requests,'Session',Session)
    t=b._RequestsTransport()
    assert t.session.trust_env is False
    assert t('GET',b.BASE_URL+'/anything',headers={},params={},body=None)==(200,b'{}')
    assert seen['allow_redirects'] is False and seen['verify'] is True and seen['stream'] is True
    assert seen['timeout']==(3.0,10.0)


def test_transport_decompressed_byte_bound(monkeypatch):
    class Response:
        status_code=200
        def __enter__(self):return self
        def __exit__(self,*args):return False
        def iter_content(self,n):yield b'x'*(b.MAX_BYTES+1)
    class Session:
        def request(self,*a,**kw):return Response()
    monkeypatch.setattr(b.requests,'Session',Session)
    with pytest.raises(b.BrokerError,match='byte_bound'):
        b._RequestsTransport()('GET',b.BASE_URL,headers={},params=None,body=None)


def test_fractional_units_preserved_without_rounding():
    c,f,_,_=setup()
    p=c.prepare_market_order('fraction','EUR_USD','1.230000','1.09',price_bound='1.10',target_epoch=NOW+60)
    f.prepared=p
    assert p['order']['units']=='1.23'
    out=c.submit_market(p,claim_submission=lambda _:True,validate_before_submit=lambda *args:True)
    assert out['status']=='filled_open' and out['units']=='1.23'


@pytest.mark.parametrize('mode',['refuse','exception','nonbool','expire'])
def test_final_policy_hook_prevents_unsafe_post(mode):
    c,f,clock,p=setup()
    def validate(prepared,account,flatness):
        assert account['observed_epoch']==NOW and not flatness['open_trades']['rows']
        assert prepared==p
        if mode=='exception':raise RuntimeError('private detail')
        if mode=='nonbool':return 1
        if mode=='expire':clock.now+=16;return True
        return False
    out=c.submit_market(p,claim_submission=lambda _:True,validate_before_submit=validate)
    assert out['status']=='not_submitted'
    assert not posts(f)


def test_final_hook_cannot_mutate_claimed_order():
    c,f,_,p=setup()
    def validate(prepared,account,flatness):
        prepared['order']['units']='100000';account['NAV']='1000000';return True
    out=c.submit_market(p,claim_submission=lambda _:True,validate_before_submit=validate)
    assert out['status']=='filled_open' and posts(f)[0][3]['order']['units']=='10'


def test_zero_units_open_trade_not_closed():
    c,f,_,p=setup();f.filled=True;f.trade_override=lambda t:t.update(currentUnits='0')
    with pytest.raises(b.BrokerError,match='zero_units_open_trade'):c.close_owned_trade('700','EUR_USD')
    assert not any(x[0]=='PUT' for x in f.calls)


def test_not_tradeable_quote_retained_as_false_not_inferred():
    c,f,clock,_=setup();f.price_payload=price_payload();row=f.price_payload['prices'][0]
    row['tradeable']=False;clock.now=b._epoch(row['time'])+1
    assert c.pricing(['EUR_USD'])['quotes']['EUR_USD']['tradeable'] is False


def test_scheduler_pause_before_actual_post_start_withholds():
    c,f,clock,p=setup()
    def validate(*args):
        samples=iter([NOW,NOW+16])
        c._clock=lambda:next(samples)
        return True
    out=c.submit_market(p,claim_submission=lambda _:True,validate_before_submit=validate)
    assert out['status']=='not_submitted' and out['reason_code']=='submit_deadline_elapsed'
    assert not posts(f)


def test_concurrent_same_intent_one_claim_and_one_post():
    from concurrent.futures import ThreadPoolExecutor
    c,f,_,p=setup();claims=[]
    def submit():
        return c.submit_market(p,claim_submission=lambda x:claims.append(x) or True,
                               validate_before_submit=lambda *args:True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _:submit(),range(2)))
    assert all(r['status']=='filled_open' for r in results)
    assert len(claims)==1 and len(posts(f))==1
