import copy
import json
import time
from decimal import Decimal

import pytest

from trad import oanda_exact_quote_receipts_v1 as q
from trad import oanda_quote_transport as transport
from trad import oanda_practice_shadow_strategy_lab as lab
from trad.oanda_curve_management_replay_v1 import convert_pnl_to_usd, quote_at

NOW = 1790825000.5
STAMP = '2026-10-01T03:23:20.123456789Z'


def raw(pair='EUR_USD', bid='1.10000000000000000001', ask='1.10020000000000000001', **extra):
    return json.dumps(dict(type='PRICE', instrument=pair, time=STAMP,
                          bids=[dict(price=bid)], asks=[dict(price=ask)],
                          tradeable=True, status='tradeable', **extra)).encode()


def snapshot(pairs=('EUR_USD',)):
    rows = {p: q.parse_price(raw(p), received_epoch=NOW, generation=1, session_id='fixture') for p in pairs}
    body = dict(schema=q.SCHEMA, producer='exact_raw_price_sidecar', session_id='fixture',
                connection_generation=1, snapshot_created_epoch=NOW + .1, instruments=sorted(pairs),
                quotes=rows, refusals={}, quote_count=len(rows), research_only=True, can_place_orders=False)
    return {**body, 'snapshot_sha256': q.digest(body)}


def seal(s):
    s['snapshot_sha256'] = q.digest({k: v for k, v in s.items() if k not in ('snapshot_sha256', 'transport')})
    return s


def mapped(s=None, **kwargs):
    s = s or snapshot()
    args = dict(observed_epoch=NOW+.2, decision_epoch=NOW+.3, instruments=s['instruments'])
    args.update(kwargs)
    return q.map_receipts(s, **args)


def test_exact_decimal_clock_and_id_into_existing_consumer():
    result = mapped()
    v = result['quotes']['EUR_USD']
    assert v['bid'] == '1.10000000000000000001'
    assert v['market_time_rfc3339'] == STAMP and v['available_epoch'] == NOW+.2
    assert quote_at(result['quotes'], 'EUR_USD', NOW+.3, 30)['bid'] == Decimal(v['bid'])
    assert not result['manager_activation'] and not result['can_place_orders']


@pytest.mark.parametrize('mutation,reason', [
    (lambda p:p['bids'][0].update(price=1.1), 'original_decimal_string'),
    (lambda p:p['bids'][0].update(price='NaN'), 'decimal_string'),
    (lambda p:p['bids'][0].update(price='2'), 'crossed'),
    (lambda p:p.update(time='2026-10-01T03:24:00Z'), 'future_market'),
    (lambda p:p.update(tradeable='true'), 'explicit_tradeability'),
    (lambda p:p.update(status='non-tradeable'), 'tradeability_conflict'),
    (lambda p:p.update(instrument='BTC_USD/../'), 'invalid_instrument'),
    (lambda p:p.update(bids=[]), 'price_levels'),
])
def test_parser_rejects_invalid_original_inputs(mutation, reason):
    p=json.loads(raw());mutation(p)
    with pytest.raises(ValueError, match=reason):
        q.parse_price(json.dumps(p).encode(), received_epoch=NOW, generation=1, session_id='fixture')


def test_duplicate_keys_and_raw_bound():
    with pytest.raises(ValueError, match='duplicate'):
        q.parse_price(b'{"type":"PRICE","type":"PRICE"}',received_epoch=NOW,generation=1,session_id='x')
    with pytest.raises(ValueError, match='raw_price_bound'):
        q.parse_price(b' '* (q.MAX_RAW+1),received_epoch=NOW,generation=1,session_id='x')


@pytest.mark.parametrize('mutation', [
    lambda s:s['quotes']['EUR_USD'].update(bid='1.0'),
    lambda s:s['quotes']['EUR_USD'].update(quote_id='changed'),
    lambda s:s.update(connection_generation=2),
    lambda s:s.update(session_id='other'),
    lambda s:s['quotes']['EUR_USD'].update(received_epoch=NOW+1),
])
def test_consumer_rejects_resealed_wrong_receipts(mutation):
    s=snapshot();mutation(s);result=mapped(seal(s))
    assert not result['quotes'] and set(result['refusals']) == {'EUR_USD'}


def test_population_identity_and_lookahead():
    s=snapshot();s['quote_count']=2
    with pytest.raises(ValueError, match='population'): mapped(seal(s))
    with pytest.raises(ValueError, match='inventory'): mapped(instruments=['GBP_USD'])
    with pytest.raises(ValueError, match='precedes'): mapped(decision_epoch=NOW)
    s=snapshot();s['transport']={'publication_started_epoch':NOW+3}
    with pytest.raises(ValueError, match='publication_clock'):mapped(s)


def test_stale_reread_and_nontradeable_refused():
    assert not mapped(observed_epoch=NOW+90,decision_epoch=NOW+90)['quotes']
    s=snapshot();p=json.loads(raw());p.update(tradeable=False,status='non-tradeable')
    s['quotes']['EUR_USD']=q.parse_price(json.dumps(p).encode(),received_epoch=NOW,generation=1,session_id='fixture')
    r=mapped(seal(s));assert 'not_explicitly_tradeable' in r['refusals']['EUR_USD']


def test_direct_inverse_conversion_profit_and_loss():
    s=snapshot(('EUR_USD','USD_JPY'))
    s['quotes']['USD_JPY']=q.parse_price(raw('USD_JPY','150.00','150.02'),received_epoch=NOW,generation=1,session_id='fixture')
    quotes=mapped(seal(s))['quotes']
    for amount in ('100','-100'):
        pnl,detail=convert_pnl_to_usd(amount,'JPY',quotes,NOW+.3,30)
        expected=Decimal(amount)/Decimal('150.02' if amount=='100' else '150.00')
        assert abs(pnl-expected)<Decimal('1e-26')
    pos,_=convert_pnl_to_usd('10','EUR',quotes,NOW+.3,30)
    neg,_=convert_pnl_to_usd('-10','EUR',quotes,NOW+.3,30)
    assert pos == 10*Decimal(quotes['EUR_USD']['bid'])
    assert neg == -10*Decimal(quotes['EUR_USD']['ask'])


def wait_written(pub,generation):
    end=time.monotonic()+5
    while time.monotonic()<end:
        if pub.publisher.stats()['written_generation']>=generation:return
        time.sleep(.01)
    raise AssertionError(pub.publisher.stats())


def test_real_publisher_reconnect_invalid_update_restart_and_sqlite_fallback(tmp_path,monkeypatch):
    path=tmp_path/'exact.json'
    monkeypatch.setattr(transport,'_atomic_json_mirror',lambda *a: (_ for _ in ()).throw(PermissionError('fixture')))
    pub=q.ExactQuoteReceiptPublisher(path,['EUR_USD'],session_id='first')
    try:
        pub(raw(),received_epoch=NOW,connection_generation=1)
        g=pub.flush();wait_written(pub,g)
        s=transport.load_quote_snapshot(path)
        assert s['quotes']['EUR_USD']['bid']=='1.10000000000000000001'
        assert 'publication_started_epoch' in s['transport']
        first_id=s['quotes']['EUR_USD']['quote_id']
        pub(None,received_epoch=time.time(),connection_generation=2)
        wait_written(pub,pub.publisher.stats()['submitted_generation'])
        assert transport.load_quote_snapshot(path)['quotes']=={}
        p=json.loads(raw());p['asks'][0]['price']='0.1'
        pub(json.dumps(p).encode(),received_epoch=NOW,connection_generation=2)
        assert pub.refusals['EUR_USD']=='crossed_quote'
        with pytest.raises(ValueError,match='generation_regression'):pub(None,received_epoch=NOW,connection_generation=1)
    finally:pub.close()
    restart=q.ExactQuoteReceiptPublisher(path,['EUR_USD'],session_id='second')
    try:
        restart(raw(),received_epoch=NOW,connection_generation=1)
        wait_written(restart,restart.flush())
        assert transport.load_quote_snapshot(path)['quotes']['EUR_USD']['quote_id']!=first_id
    finally:restart.close()


def test_stream_calls_opt_in_observer_before_float_conversion(tmp_path,monkeypatch):
    pub=q.ExactQuoteReceiptPublisher(tmp_path/'exact.json',['EUR_USD'])
    stream=lab.MultiPriceStream(lambda:('unused','unused'),['EUR_USD'],lambda *a,**k:None,raw_price_observer=pub)
    class Response:
        status_code=200
        def iter_lines(self):
            yield raw()
            stream._stop.set()
        def close(self):pass
    monkeypatch.setattr(lab.requests,'get',lambda *a,**k:Response())
    try:
        stream._run()
        wait_written(pub,pub.flush())
        s=transport.load_quote_snapshot(tmp_path/'exact.json')
        assert s['quotes']['EUR_USD']['bid']=='1.10000000000000000001'
        assert isinstance(stream.snapshot()['EUR_USD'].bid,float)
        assert stream.stats()['raw_price_observer_errors']==0
    finally:pub.close();stream.stop()


def test_callback_failure_is_observable_without_changing_legacy_quotes(monkeypatch):
    stream=lab.MultiPriceStream(lambda:('unused','unused'),['EUR_USD'],lambda *a,**k:None,
                               raw_price_observer=lambda *a,**k: (_ for _ in ()).throw(ValueError('fixture')))
    stream._activate_connection()
    assert stream.stats()['raw_price_observer_errors']==1


def test_bounded_publication_and_observed_reader(tmp_path):
    from datetime import datetime,timezone
    import sqlite3
    path=tmp_path/'exact.json'
    pub=q.ExactQuoteReceiptPublisher(path,['EUR_USD'])
    try:
        for i in range(7):
            p=json.loads(raw());p['time']=datetime.fromtimestamp(time.time()-1,timezone.utc).isoformat()
            pub(json.dumps(p).encode(),received_epoch=time.time(),connection_generation=1)
            wait_written(pub,pub.flush())
        result=q.read_receipts(path,instruments=['EUR_USD'])
        assert result['quotes']['EUR_USD']['available_epoch']==result['observed_epoch']
        assert result['publication_started_epoch']<=result['observed_epoch']
        with sqlite3.connect(transport.quote_database_path(path)) as db:
            assert db.execute('select count(*) from quote_snapshots_v2').fetchone()[0]==4
        pub(b'invalid',received_epoch=time.time(),connection_generation=1)
        wait_written(pub,pub.flush())
        assert not q.read_receipts(path,instruments=['EUR_USD'])['quotes']
    finally:pub.close()
