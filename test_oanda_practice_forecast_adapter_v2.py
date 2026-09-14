"""Native owned SQLite publication to separate practice policy; synthetic only."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

import pytest
import oanda_practice_forecast_adapter_v2 as a
import oanda_practice_trial_runner_v2 as r


@pytest.fixture
def native():
    # Reuse the reviewed native owner's recreation fixture; do not duplicate a
    # second forecast/anchor generator. Its numeric fit alone is a fixture spy.
    directory=Path(__file__).parent.parent/'revamp_8h_20260912/runtime/joint_native_anchor_001'
    sys.path.append(str(directory))
    try:
        from test_native_owner_successor_003 import SuccessorOwner
        owner=SuccessorOwner();owner.setUp()
        try:
            owner.issue();owner.owner.consume_publications()
            yield owner
        finally:owner.tearDown()
    finally:sys.path.remove(str(directory))


def native_row(native):
    db=native.owner.db;db.row_factory=sqlite3.Row
    return dict(db.execute('''SELECT f.id,f.bucket,f.attempt_id,f.payload,f.sha,a.epoch attempt_epoch,a.reference_id,
        p.epoch published,p.forecast_sha publication_sha,c.epoch consumed,
        c.forecast_sha consumption_sha,c.publication_sha consumption_publication_sha
        FROM forecasts f JOIN attempts a ON a.id=f.attempt_id
        LEFT JOIN publication p ON p.id=f.id LEFT JOIN consumption c ON c.id=f.id
        ORDER BY f.bucket DESC LIMIT 1''').fetchone())


def candidate(native, row=None, observed=None):
    return a.candidate_from_row(native_row(native) if row is None else row,native.contract,
        activated_epoch=native.owner.activated_epoch,observed_epoch=native.clock() if observed is None else observed)


def test_original_native_anchor_endpoint_probability_and_receipts(native):
    value=candidate(native);row=native_row(native);arm=json.loads(row['payload'])['forecasts'][0]
    assert float(value['reference_price']).hex()==arm['native_anchor']['origin_mid_hex']
    assert float(value['expected_terminal_price']).hex()==arm['native_anchor']['expected_endpoint_mid_hex']
    assert float(value['probability_up']).hex()==arm['native_anchor']['native_probability_up_hex']
    assert value['original_target_epoch']-value['reference_epoch']==3600
    assert value['publication_epoch']==row['published'] and value['available_epoch']==row['consumed']
    assert value['native_anchor']==arm['native_anchor']
    assert value['remaining_probability_available'] is False and value['can_place_orders'] is False


@pytest.mark.parametrize('field,value',[
    ('sha','0'*64),('id','wrong'),('attempt_id','wrong'),('attempt_epoch',1),
    ('reference_id','wrong'),('publication_sha','0'*64),('consumption_sha','0'*64),
    ('consumption_publication_sha','0'*64),('published',None),('consumed',None)])
def test_source_receipt_or_clock_mismatch_is_withheld(native,field,value):
    row=native_row(native);row[field]=value
    with pytest.raises(a.AdapterError):candidate(native,row)


@pytest.mark.parametrize('age',[901,3601])
def test_original_forecast_is_never_retimed_to_current_quote(native,age):
    row=native_row(native);issued=json.loads(row['payload'])['forecasts'][0]['issued_epoch']
    with pytest.raises(a.AdapterError,match='stale'):candidate(native,row,issued+age)


def test_tampered_native_probability_fails_even_with_resealed_storage(native):
    row=native_row(native);value=json.loads(row['payload']);value['forecasts'][0]['probability_up']=.9
    row['payload']=a.encoded(value).decode();row['sha']=a.digest(value)
    row['publication_sha']=row['consumption_sha']=row['sha']
    row['consumption_publication_sha']=a.digest({'epoch':row['published'],'forecast_sha':row['sha']})
    with pytest.raises(a.AdapterError,match='contract_invalid'):candidate(native,row)


def test_readonly_query_uses_actual_owned_sqlite_and_does_not_modify_it(native,tmp_path,monkeypatch):
    study=tmp_path/'joint_price_news_study_v7';pair='EUR_USD'
    path=study/'pairs'/pair/a.FAMILY/'study.sqlite';path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as target:native.owner.db.backup(target)
    registry={'pairs':{pair:{'families':{a.FAMILY:{'contract':native.contract,'contract_sha256':a.digest(native.contract)}}}}}
    activation={'activation_completed_epoch':native.owner.activated_epoch}
    monkeypatch.setattr(a,'validate_source_config',lambda *args:(registry,activation,study))
    before=hashlib.sha256(path.read_bytes()).hexdigest()
    value=a.read_candidates(tmp_path,native.clock(),source_config={'study_path':study.name},clock=native.clock)
    assert len(value['signals'])==1 and value['rejected']==[]
    assert value['receipts'][0]['query_only'] is True
    assert hashlib.sha256(path.read_bytes()).hexdigest()==before


def test_source_changes_during_read_refuse_entire_batch(native,tmp_path,monkeypatch):
    calls=[]
    def changed(*args):
        calls.append(1)
        if len(calls)>1:raise a.AdapterError('registered_source_changed')
        return {'pairs':{'EUR_USD':{'families':{a.FAMILY:{'contract':native.contract}}}}},{'activation_completed_epoch':native.owner.activated_epoch},tmp_path
    monkeypatch.setattr(a,'validate_source_config',changed)
    with pytest.raises(a.AdapterError,match='registered_source_changed'):
        a.read_candidates(tmp_path,native.clock(),source_config={'study_path':'joint_price_news_study_v7'},clock=native.clock)


def test_native_forecast_to_exact_broker_order_stop_reconcile_and_original_target_exit(native,tmp_path,monkeypatch):
    import test_oanda_practice_trial_broker_v1 as transport_fixture
    from test_oanda_practice_trial_runner_v2 import FakeLock
    signal=candidate(native);clock=native.clock
    def iso(value):return datetime.fromtimestamp(value,timezone.utc).isoformat().replace("+00:00","Z")
    monkeypatch.setattr(transport_fixture,'MARKET_TIME',iso(clock()))
    metadata=dict(name='EUR_USD',type='CURRENCY',marginRate='0.02',minimumTradeSize='1',tradeUnitsPrecision=0,
        pipLocation=-4,displayPrecision=5,maximumOrderUnits='100000000',maximumPositionSize='0')
    class Fake(transport_fixture.Fake):
        def trade(self):
            value=super().trade();value['price']=self.prepared['order']['priceBound'];return value
        def __call__(self,method,url,**kwargs):
            if url.endswith('/summary'):
                self.calls.append((method,url,None,None));opened=int(self.filled and not self.closed)
                return 200,transport_fixture.encoded({'account':{'id':transport_fixture.ACCOUNT,'currency':'USD',
                    'NAV':'41.60','balance':'41.60','marginUsed':'0','marginAvailable':'41.60','marginRate':'0.02',
                    'openTradeCount':opened,'openPositionCount':opened,'pendingOrderCount':0,'lastTransactionID':'900'}})
            if url.endswith('/transactions/sinceid'):
                self.calls.append((method,url,None,None));return 200,transport_fixture.encoded({'transactions':[],'lastTransactionID':'900'})
            if url.endswith('/instruments'):
                self.calls.append((method,url,None,None));return 200,transport_fixture.encoded({'instruments':[metadata]})
            return super().__call__(method,url,**kwargs)
    fake=Fake();fake.post_timeout=True  # Ambiguous transport still reconciles once.
    fake.price_payload={'prices':[{'instrument':'EUR_USD','time':iso(clock()),'tradeable':True,
        'bids':[{'price':'1.10000'}],'asks':[{'price':'1.10002'}]}],'homeConversions':[]}
    broker=r.PracticeTrialBroker(account_id=transport_fixture.ACCOUNT,token='synthetic',
        expected_account_sha256=transport_fixture.FINGERPRINT,trial_id=r.TRIAL_ID,transport=fake,clock=clock)
    cfg={'enabled':True,'config_sha256':'c'*64,'account_sha256':transport_fixture.FINGERPRINT,
        'trial_id':r.TRIAL_ID,'start_epoch':clock()-1,'stop_epoch':clock()+47*3600,'forecast_source':{}}
    monkeypatch.setattr(r,'validate_config',lambda *args,**kwargs:cfg)
    monkeypatch.setattr(r,'process_preflight',lambda:{'ready':True})
    ledger=r.Ledger(tmp_path/'trial.sqlite');runner=r.Runner(broker,ledger,cfg,FakeLock(),clock=clock,
        reader=lambda *args,**kwargs:{'signals':[],'rejected':[{'reason':'original_target_elapsed'}]})
    try:
        runner.initialize(broker.account_summary(),broker.open_trades(),broker.pending_orders())
        prepare=broker.prepare_market_order
        def prepare_retained(*args,**kwargs):
            fake.prepared=prepare(*args,**kwargs);return fake.prepared
        monkeypatch.setattr(broker,'prepare_market_order',prepare_retained)
        last=int(clock()//60)*60-60
        context={'signal':signal,'metadata':metadata,'candles':{'instrument':'EUR_USD','observed_epoch':clock(),
            'source_sha256':'b'*64,'rows':[{'label_epoch':last-(14-i)*60,'complete':True,
            'mid':{'h':'1.10020','l':'1.09980','c':'1.10000'}} for i in range(15)]}}
        runner.enter(context,{'status':'available'})
        assert len(transport_fixture.posts(fake))==1
        assert ledger.intents()[0]['status']=='filled_open'
        prepared=fake.prepared;assert prepared['original_target_epoch']==signal['original_target_epoch']
        assert prepared['order']['stopLossOnFill']['timeInForce']=='GTC'
        assert 'takeProfitOnFill' not in prepared['order']
        runner.reconcile();runner.enter(context,{'status':'available'})
        assert len(transport_fixture.posts(fake))==1
        clock.value=signal['original_target_epoch']
        assert runner.cycle()['state']=='practice_trial_enabled'
        assert fake.closed and ledger.intents()[0]['status']=='filled_closed'
        assert len([call for call in fake.calls if call[0]=='PUT'])==1
    finally:ledger.close();broker.close()
