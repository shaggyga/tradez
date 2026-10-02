from pathlib import Path
import sys,json,time,datetime
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'trad'))
import oanda_exact_quote_stream_v1 as w
import oanda_practice_quote_stream as original
import oanda_exact_quote_receipts_v1 as receipts


def test_frozen_targets_and_tampered_source(tmp_path):
    w.verify_sources()
    for name in w.EXPECTED:(tmp_path/name).write_bytes((w.ROOT/name).read_bytes())
    (tmp_path/next(iter(w.EXPECTED))).write_text('changed')
    with pytest.raises(ValueError,match='unreviewed_quote_dependency'):w.verify_sources(tmp_path)


def test_actual_worker_to_exact_consumer(monkeypatch,tmp_path):
    events=[];stats=[]
    class Heartbeat:
        def start(self):return self
        def update(self,**kw):events.append(kw)
        def close(self):pass
    class Client:
        def __init__(self,*a):pass
        def close(self):pass
    class Base:
        def __init__(self,*args,**kwargs):self.observer=kwargs['raw_price_observer'];events.append({'legacy_path':kwargs['research_snapshot_path']})
        def start(self):
            now=time.time();self.observer(None,received_epoch=now,connection_generation=1)
            raw=json.dumps(dict(type='PRICE',instrument='EUR_USD',time=datetime.datetime.fromtimestamp(now-1,datetime.timezone.utc).isoformat(),bids=[dict(price='1.10000000000000000001')],asks=[dict(price='1.10020000000000000001')],tradeable=True)).encode()
            self.observer(raw,received_epoch=now,connection_generation=1)
        def wait_ready(self,*a):stats.append(self.stats())
        def stats(self):return {'connected':True}
        def stop(self):events.append({'stopped':True})
    lab=original.lab
    monkeypatch.setattr(lab,'WorkerHeartbeat',lambda *a,**k:Heartbeat())
    monkeypatch.setattr(lab,'read_credentials',lambda *a,**k:('fixture','fixture_007'))
    monkeypatch.setattr(lab,'MarketDataClient',Client)
    monkeypatch.setattr(lab,'selected_instruments',lambda *a:['EUR_USD'])
    monkeypatch.setattr(lab,'account_pip_sizes',lambda *a:{'EUR_USD':.0001})
    monkeypatch.setattr(lab,'close_log_handles',lambda:None)
    monkeypatch.setattr(lab,'MultiPriceStream',Base)
    path=tmp_path/'exact.json'
    assert w.main(['--exact-quote-output',str(path),'--duration-sec','1','--heartbeat-state',str(tmp_path/'heartbeat.json'),'--research-market-quote-snapshot',str(tmp_path/'legacy.json')])==0
    assert lab.MultiPriceStream is Base and {'stopped':True} in events
    result=receipts.read_receipts(path,instruments=['EUR_USD'])
    assert result['quotes']['EUR_USD']['bid']=='1.10000000000000000001'
    assert result['quotes']['EUR_USD']['ask']=='1.10020000000000000001'
    assert stats[0]['exact_receipts']['connection_generation']==1
    assert not any(x.get('can_place_orders') for x in events)


def test_close_on_constructor_failure(tmp_path):
    closed=[]
    class Pub:
        def __init__(self,*a):pass
        def close(self):closed.append(True)
    class Bad:
        def __init__(self,*a,**k):raise RuntimeError('fixture_failure')
    with pytest.raises(RuntimeError,match='fixture_failure'):
        w.stream_class(Bad,Pub,tmp_path/'exact.json')(None,['EUR_USD'])
    assert closed==[True]


def test_existing_hook_and_shared_path_refused(tmp_path):
    class Base:pass
    cls=w.stream_class(Base,None,tmp_path/'same.json')
    with pytest.raises(ValueError,match='existing_raw_observer'):
        cls(None,['EUR_USD'],raw_price_observer=lambda:None)
    with pytest.raises(ValueError,match='separate_exact_receipt_path'):
        cls(None,['EUR_USD'],research_snapshot_path=tmp_path/'same.json')


def test_stop_failure_still_closes(tmp_path):
    closed=[]
    class Pub:
        def __init__(self,*a):pass
        def close(self):closed.append(True)
    class Base:
        def __init__(self,*a,**k):pass
        def stop(self):raise RuntimeError('stop_failure')
    obj=w.stream_class(Base,Pub,tmp_path/'exact.json')(None,['EUR_USD'])
    with pytest.raises(RuntimeError,match='stop_failure'):obj.stop()
    assert closed==[True]
