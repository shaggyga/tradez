"""Disposable ledger and fake transport lifecycle tests. No broker or live reads."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import zlib

import pytest

import oanda_practice_trial_runner_v2 as r

NOW = datetime(2026,9,9,18,0,tzinfo=timezone.utc).timestamp()
TEST_STOP=NOW+47*3600

def trial_config(**kw):
    return dict(enabled=True,config_sha256='c'*64,account_sha256='d'*64,trial_id=r.TRIAL_ID,start_epoch=NOW-60,stop_epoch=TEST_STOP,forecast_source={},**kw)


def signal(target=None):
    return dict(instrument='GBP_USD',side=1,reference_epoch=NOW-300,reference_price='1.2500',
        original_target_epoch=NOW+3300 if target is None else target,expected_terminal_price='1.2520',
        probability_up='0.60',issued_epoch=NOW-290,available_epoch=NOW-289,forecast_sha256='a'*64)


def prepared(identity='1', client='client-1'):
    return dict(intent_id=identity,intent_sha256='b'*64,
        order=dict(instrument='GBP_USD',units='10',tradeClientExtensions={'id':client}))


def context(target=None):
    return {'signal':signal(target)}


class FakeLock:
    def __init__(self):self.checks=0;self.fail=False
    def check(self):
        self.checks+=1
        if self.fail:raise RuntimeError('account_lock_lost')


class FakeBroker:
    def __init__(self, clock):
        self.clock=clock;self.trades=[];self.pending=[];self.calls=[];self.nav='100'
        self.account_error=False;self.quote_error=False;self.pending_error=False;self.close_unknown=False
        self.reconciled_status='unknown'
    def account_summary(self):
        self.calls.append('account')
        if self.account_error:raise r.BrokerError('account_unavailable')
        return dict(currency='USD',NAV=self.nav,marginUsed='0',marginAvailable='100',marginRate='0.02',
            openTradeCount=len(self.trades),openPositionCount=len(self.trades),pendingOrderCount=len(self.pending),
            observed_epoch=self.clock(),lastTransactionID='10')
    def open_trades(self):
        self.calls.append('trades');return {'rows':deepcopy(self.trades),'observed_epoch':self.clock()}
    def pending_orders(self):
        self.calls.append('pending')
        if self.pending_error:raise r.BrokerError('pending_unavailable')
        return {'rows':deepcopy(self.pending),'observed_epoch':self.clock()}
    def owned_trade(self, trade, expected=None):
        if not trade.get('owned'):raise r.BrokerError('unowned_trade')
        if expected is not None and trade['clientExtensions']['id']!=expected['order']['tradeClientExtensions']['id']:
            raise r.BrokerError('owned_trade_intent_mismatch')
        return trade['target']
    def pricing(self, pairs):
        self.calls.append(('pricing',tuple(pairs)))
        if self.quote_error:raise r.BrokerError('quote_unavailable')
        return dict(quotes={p:dict(instrument=p,bid='1.2500',ask='1.2502',tradeable=True,
            market_epoch=self.clock(),observed_epoch=self.clock()) for p in pairs},
            home_conversions=dict(observed_epoch=self.clock(),rows=[]))
    def close_owned_trade(self, trade_id, instrument, **kwargs):
        self.calls.append(('close',trade_id,instrument))
        if self.close_unknown:return {'status':'unknown'}
        self.trades=[t for t in self.trades if t['id']!=trade_id]
        return {'status':'confirmed_closed','trade_id':trade_id}
    def reconcile_market(self, value):
        self.calls.append(('reconcile',value['intent_id']))
        if self.reconciled_status=='closed_when_flat':
            return {'status':'filled_open' if self.trades else 'filled_closed','trade_id':'101'}
        return {'status':self.reconciled_status}
    def transactions_since(self, cursor):
        self.calls.append(('transactions',cursor));return {'rows':[],'lastTransactionID':'10'}
    def submit_market(self,*args,**kwargs):
        raise AssertionError('No test may accidentally submit')


@pytest.fixture
def case(tmp_path):
    current=[NOW]
    clock=lambda:current[0]
    ledger=r.Ledger(tmp_path/'trial.sqlite')
    broker=FakeBroker(clock);lock=FakeLock()
    cfg=trial_config()
    runner=r.Runner(broker,ledger,cfg,lock,clock=clock,
        reader=lambda *args,**kwargs: (_ for _ in ()).throw(AssertionError('unexpected forecast read')))
    runner.initialize(broker.account_summary(),broker.open_trades(),broker.pending_orders())
    runner.last_scan=NOW
    yield SimpleNamespace(current=current,clock=clock,ledger=ledger,broker=broker,lock=lock,runner=runner,path=tmp_path/'trial.sqlite')
    ledger.close()


def retain(case,*,claim=True,target=None,identity='1',client='client-1'):
    p=prepared(identity,client)
    sha=case.ledger.evidence('context',context(target),case.clock())
    case.ledger.prepare(p,sha)
    if claim:assert case.ledger.claim(p,case.clock())
    return p


def add_trade(case,*,owned=True,target=None,stop='1.2480',client='client-1'):
    target=NOW+3300 if target is None else target
    trade=dict(id='101',instrument='GBP_USD',currentUnits='10',price='1.2502',
        clientExtensions={'id':client},owned=owned,target=target,
        openTime=datetime.fromtimestamp(NOW-200,timezone.utc).isoformat(),
        stopLossOrder={'state':'PENDING','price':stop} if stop else None)
    case.broker.trades.append(trade)
    return trade


def test_full_durable_claim_survives_reopen(case):
    p=retain(case)
    second=r.Ledger(case.path)
    try:
        assert second.claim(p,NOW+1) is False
        row=second.intents()[0]
        assert row['attempted']==1 and row['status']=='unknown'
        assert second.db.execute('PRAGMA synchronous').fetchone()[0]==2
        assert second.context(row['context_sha'])==context()
        assert second.db.execute("SELECT COUNT(*) FROM events WHERE kind='submission_claim'").fetchone()[0]==1
    finally:second.close()


def test_claim_requires_exact_retained_preparation(case):
    with pytest.raises(ValueError,match='unretained_intent'):case.ledger.claim(prepared(),NOW)
    p=retain(case,claim=False);changed=deepcopy(p);changed['order']['units']='11'
    with pytest.raises(ValueError,match='unretained_intent'):case.ledger.claim(changed,NOW)
    with pytest.raises(ValueError,match='intent_identity_conflict'):case.ledger.prepare(changed,'d'*64)


def test_restarted_unclaimed_intent_is_closed_without_transport(case):
    retain(case,claim=False);case.runner.reconcile()
    assert case.ledger.intents()[0]['status']=='not_submitted'
    assert not any(isinstance(c,tuple) and c[0]=='reconcile' for c in case.broker.calls)
    assert case.ledger.session(NOW)['entries_today']==0


def test_unknown_claim_is_reconcile_only_and_blocks_reentry(case):
    p=retain(case);case.runner.reconcile();case.runner.reconcile()
    assert case.ledger.intents(unresolved=True)[0]['status']=='unknown'
    assert case.ledger.claim(p,NOW+1) is False
    result=case.runner.cycle()
    assert result['state']=='entries_halted_reconciliation_required'
    assert case.ledger.session(NOW)['entries_today']==1


@pytest.mark.parametrize('terminal',['not_filled','not_submitted','filled_closed'])
def test_terminal_intent_does_not_reconcile_or_reset_claim(case,terminal):
    p=retain(case);case.ledger.outcome(p['intent_id'],{'status':terminal})
    before=len(case.broker.calls);case.runner.reconcile()
    assert len(case.broker.calls)==before
    assert case.ledger.claim(p,NOW+1) is False
    assert case.ledger.intents(unresolved=True)==[]


def test_session_claim_counters_and_latches_survive_reopen(case):
    retain(case);case.ledger.latch('loss_stop_latched');case.ledger.latch('entry_halt_latched')
    second=r.Ledger(case.path)
    try:
        session=second.session(NOW)
        assert session['entries_today']==1 and session['last_entry_epoch_by_instrument']=={'GBP_USD':NOW}
        assert session['loss_stop_latched'] is True and second.get('entry_halt_latched') is True
        assert second.session(NOW+86400)['entries_today']==0
        assert second.session(NOW+86400)['last_entry_epoch_by_instrument']=={'GBP_USD':NOW}
    finally:second.close()


def test_retained_evidence_hash_is_checked_before_use(case):
    p=retain(case);row=case.ledger.intents()[0]
    changed=context();changed['signal']['expected_terminal_price']='9.0'
    with case.ledger.db:
        case.ledger.db.execute('UPDATE evidence SET body=? WHERE sha=?',(zlib.compress(r.encoded(changed)),row['context_sha']))
    with pytest.raises(ValueError):case.ledger.context(row['context_sha'])


def test_transaction_duplicate_payload_conflict_refuses_without_cursor_advance(case):
    case.ledger.retain_transactions({'rows':[{'id':'11','type':'ORDER_FILL'}],'lastTransactionID':'11'})
    with pytest.raises(ValueError):
        case.ledger.retain_transactions({'rows':[{'id':'11','type':'ALTERED'}],'lastTransactionID':'12'})
    assert case.ledger.get('transaction_cursor')=='11'


def test_initialization_preserves_original_baseline_and_config(case):
    original=case.ledger.get('initial')
    case.broker.nav='200';case.runner.initialize(case.broker.account_summary(),case.broker.open_trades(),case.broker.pending_orders())
    assert case.ledger.get('initial')==original
    case.runner.config['config_sha256']='f'*64
    with pytest.raises(ValueError,match='persisted_config_changed'):
        case.runner.initialize(case.broker.account_summary(),case.broker.open_trades(),case.broker.pending_orders())


@pytest.mark.parametrize('field,value',[('openTradeCount',1),('openPositionCount',1),('pendingOrderCount',1),('currency','GBP'),('NAV','0')])
def test_initial_account_must_be_confirmed_flat_positive_usd(tmp_path,field,value):
    ledger=r.Ledger(tmp_path/'init.sqlite');broker=FakeBroker(lambda:NOW)
    runner=r.Runner(broker,ledger,trial_config(),FakeLock(),clock=lambda:NOW)
    account=broker.account_summary();account[field]=value
    try:
        with pytest.raises(ValueError,match='initial_account_not_flat_usd'):runner.initialize(account,broker.open_trades(),broker.pending_orders())
        assert ledger.get('initial') is None
    finally:ledger.close()


def test_unknown_quote_preserves_owned_position_and_fixed_stop(case):
    retain(case);add_trade(case);case.broker.quote_error=True
    guard,positions,foreign=case.runner.manage(case.broker.account_summary(),case.broker.open_trades(),case.broker.pending_orders())
    assert positions[0]['decision']['action']=='wait_unknown' and not foreign
    assert not any(isinstance(c,tuple) and c[0]=='close' for c in case.broker.calls)
    assert len(case.broker.trades)==1


def test_foreign_position_is_never_closed_and_halts_entries(case):
    add_trade(case,owned=False)
    _,positions,foreign=case.runner.manage(case.broker.account_summary(),case.broker.open_trades(),case.broker.pending_orders())
    assert foreign==['101'] and positions==[] and case.ledger.get('entry_halt_latched') is True
    assert not any(isinstance(c,tuple) and c[0]=='close' for c in case.broker.calls)


def test_orphan_owned_trade_is_closed_without_fabricating_an_intent(case):
    add_trade(case)
    case.runner.manage(case.broker.account_summary(),case.broker.open_trades(),case.broker.pending_orders())
    assert ('close','101','GBP_USD') in case.broker.calls
    assert case.ledger.intents()==[] and case.ledger.get('entry_halt_latched') is True


def test_missing_broker_protection_closes_exact_owned_trade(case):
    retain(case);add_trade(case,stop=None)
    _,positions,_=case.runner.manage(case.broker.account_summary(),case.broker.open_trades(),case.broker.pending_orders())
    assert positions[0]['decision']['reason']=='protective_stop_missing'
    assert ('close','101','GBP_USD') in case.broker.calls


def test_original_target_closes_without_quote_and_unknown_close_stays_open(case):
    target=NOW-1;retain(case,target=target);add_trade(case,target=target)
    case.broker.quote_error=True;case.broker.close_unknown=True
    _,positions,_=case.runner.manage(case.broker.account_summary(),case.broker.open_trades(),case.broker.pending_orders())
    assert positions[0]['decision']['action']=='close'
    assert ('close','101','GBP_USD') in case.broker.calls and len(case.broker.trades)==1
    assert case.ledger.get('completed_flat') is None


def test_session_loss_is_durable_and_closes_owned_trade(case):
    retain(case);add_trade(case);case.broker.nav='94'
    guard,positions,_=case.runner.manage(case.broker.account_summary(),case.broker.open_trades(),case.broker.pending_orders())
    assert guard['reason']=='session_loss_stop' and case.ledger.get('loss_stop_latched') is True
    assert positions[0]['decision']['action']=='close'
    case.broker.nav='100'
    assert case.ledger.session(NOW)['loss_stop_latched'] is True


def test_end_completes_only_after_reconciled_owned_close(case):
    retain(case,target=TEST_STOP-300);add_trade(case,target=TEST_STOP-300)
    case.broker.reconciled_status='closed_when_flat';case.current[0]=TEST_STOP
    result=case.runner.cycle()
    assert result['state']=='completed_flat'
    assert case.ledger.get('completed_flat') and case.ledger.intents(unresolved=True)==[]
    assert ('close','101','GBP_USD') in case.broker.calls


def test_end_unknown_submission_does_not_infer_flat_complete(case):
    retain(case);case.current[0]=TEST_STOP
    result=case.runner.cycle()
    assert result['state']=='close_only_awaiting_confirmation'
    assert case.ledger.get('completed_flat') is None


def test_end_foreign_trade_blocks_completion_without_closing_it(case):
    add_trade(case,owned=False);case.current[0]=TEST_STOP
    result=case.runner.cycle()
    assert result['state']=='close_only_awaiting_confirmation' and len(case.broker.trades)==1
    assert not any(isinstance(c,tuple) and c[0]=='close' for c in case.broker.calls)


def test_end_account_outage_still_attempts_exact_owned_deadline_close(case):
    retain(case,target=TEST_STOP-300);add_trade(case,target=TEST_STOP-300)
    case.current[0]=TEST_STOP;case.broker.account_error=True
    try:case.runner.cycle()
    except r.BrokerError:pass
    assert ('close','101','GBP_USD') in case.broker.calls
    assert case.ledger.get('completed_flat') is None


def test_lock_loss_prevents_any_broker_read(case):
    case.lock.fail=True;case.broker.calls=[]
    with pytest.raises(RuntimeError,match='account_lock_lost'):case.runner.cycle()
    assert case.broker.calls==[]


def test_whole_forecast_failure_never_uses_an_old_batch(case,monkeypatch):
    monkeypatch.setattr(r,'validate_config',lambda *args,**kwargs:case.runner.config)
    monkeypatch.setattr(r,'process_preflight',lambda:{'ready':True})
    case.runner.reader=lambda *args,**kwargs: (_ for _ in ()).throw(ValueError('source_closure_changed'))
    with pytest.raises(ValueError,match='source_closure_changed'):case.runner.scan_and_enter()
    assert case.ledger.intents()==[]


def test_config_or_process_change_latches_entry_halt(case,monkeypatch):
    monkeypatch.setattr(r,'validate_config',lambda *args,**kwargs:case.runner.config)
    monkeypatch.setattr(r,'process_preflight',lambda:{'ready':False})
    with pytest.raises(ValueError,match='activation_or_process_changed'):case.runner.scan_and_enter()
    assert case.ledger.get('entry_halt_latched') is True and case.ledger.intents()==[]


def test_normalized_intent_identity_cannot_refresh_same_forecast(case):
    original=signal();changed=deepcopy(original);changed['issued_epoch']=NOW;changed['available_epoch']=NOW
    assert case.runner.intent_id(original)==case.runner.intent_id(changed)
    changed['forecast_sha256']='d'*64
    assert case.runner.intent_id(original)!=case.runner.intent_id(changed)


def test_validate_config_source_scope_and_changed_bytes(tmp_path,monkeypatch):
    monkeypatch.setattr(r,'ROOT',tmp_path)
    import oanda_practice_forecast_adapter_v2 as adapter
    monkeypatch.setattr(adapter,'validate_source_config',lambda *args:None)
    names={'oanda_practice_trial_runner_v2.py','oanda_practice_trial_broker_v1.py',
        'oanda_practice_forecast_adapter_v2.py','oanda_practice_trial_policy_v2.py',
        'oanda_live_account_readonly_status.py','oanda_trade_reconciliation_v1.py',
        'src/forex_system/contracts/signed_currency_exposure.py',
        'oanda_practice_trial_runtime_v3.py', r.CANDIDATE_NAME}
    bindings={}
    for name in names:
        path=tmp_path/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'fixture')
        bindings[name]=hashlib.sha256(b'fixture').hexdigest()
    cfg=dict(schema_version=r.SCHEMA,trial_id=r.TRIAL_ID,environment='practice',account_label='practice_007',
        user_authorized_practice_trial=True,start_epoch=NOW-60,stop_epoch=TEST_STOP,policy=r.POLICY,forecast_source={},
        account_sha256='6879dca9af3472e4c94ab81a460982068a6efce6dc25ef851ea4bd9bb9fbdad2',
        strategy='verified_native_joint_v7_original_h1',enabled=True,source_bindings=bindings)
    cfg['config_sha256']=r.digest(cfg);path=tmp_path/'config.json';path.write_bytes(r.encoded(cfg))
    assert r.validate_config(path,require_enabled=True)==cfg
    def source_down(*args):raise ValueError('research_source_unavailable')
    monkeypatch.setattr(adapter,'validate_source_config',source_down)
    with pytest.raises(ValueError,match='research_source_unavailable'):r.validate_config(path)
    assert r.validate_config(path,verify_forecast_source=False)==cfg
    original=deepcopy(cfg)
    for changes in ({'stop_epoch':NOW-60+48*3600+1},{'start_epoch':TEST_STOP},
                    {'start_epoch':True},{'trial_id':'old_expired_trial'},
                    {'account_sha256':'0'*64},{'strategy':'unverified_new_strategy'}):
        changed={**deepcopy(original),**changes};changed.pop('config_sha256')
        changed['config_sha256']=r.digest(changed);path.write_bytes(r.encoded(changed))
        with pytest.raises(ValueError,match='config_scope'):r.validate_config(path,verify_forecast_source=False)
    path.write_bytes(r.encoded(original))
    (tmp_path/'oanda_practice_trial_policy_v2.py').write_bytes(b'changed')
    with pytest.raises(ValueError,match='source_closure_changed'):r.validate_config(path)


def test_close_only_recovery_skips_forecasts_and_new_orders(case,tmp_path,monkeypatch):
    from contextlib import nullcontext
    retain(case);add_trade(case);case.broker.reconciled_status='closed_when_flat'
    root=tmp_path/'project';state=root/'data/oanda_training_manager'/case.runner.trial_id
    state.mkdir(parents=True);(state/'trial.sqlite').touch()
    monkeypatch.setattr(r,'ROOT',root)
    modes=[]
    def validate(path,**kwargs):
        modes.append(kwargs);return case.runner.config
    monkeypatch.setattr(r,'validate_config',validate)
    monkeypatch.setattr(r,'Ledger',lambda *args:case.ledger)
    monkeypatch.setattr(r,'AccountLock',lambda *args:nullcontext(case.lock))
    monkeypatch.setattr(r.PracticeTrialBroker,'from_credentials',lambda *args,**kwargs:case.broker)
    monkeypatch.setattr(case.broker,'close',lambda:None,raising=False)
    monkeypatch.setattr(r.time,'time',case.clock)
    monkeypatch.setattr(sys,'argv',['runner','--close-only','--config','fixture.json'])
    assert r.main()==0
    assert modes==[{'require_enabled':False,'verify_forecast_source':False}]
    assert case.broker.trades==[]
    assert ('close','101','GBP_USD') in case.broker.calls


def test_immutable_trial_window_prevents_reusing_ledger_to_extend_session(case):
    changed={**case.runner.config,'stop_epoch':case.runner.stop_epoch+60}
    with pytest.raises(ValueError,match='immutable_trial_window_changed'):
        r.Runner(case.broker,case.ledger,changed,case.lock)


@pytest.mark.parametrize('script',['oanda_technical_account_manager_auto.py','oanda_advisor_account_manager_auto.py',
                                  'oanda_practice_top_signal_executor.py'])
def test_competing_account_manager_process_is_not_silently_missed(monkeypatch,script):
    processes=[SimpleNamespace(pid=1,info=dict(name='powershell.exe',cmdline=['powershell','-File','oanda_always_on_supervisor.ps1','-ResearchCollectionOnly'])),
               SimpleNamespace(pid=2,info=dict(name='python.exe',cmdline=['python',script]))]
    fake=SimpleNamespace(process_iter=lambda *args,**kwargs:processes,NoSuchProcess=LookupError,AccessDenied=PermissionError)
    monkeypatch.setitem(sys.modules,'psutil',fake)
    result=r.process_preflight()
    assert result['ready'] is False and result['competing_execution_pids']==[2]


def entry_context():
    return dict(signal=signal(),metadata=dict(name='GBP_USD',type='CURRENCY',marginRate='0.02',minimumTradeSize='1',
        tradeUnitsPrecision=0,pipLocation=-4,displayPrecision=5,maximumOrderUnits='100000000',maximumPositionSize='0'),
        candles=dict(instrument='GBP_USD',observed_epoch=NOW,source_sha256='b'*64,
            rows=[dict(label_epoch=NOW-900+i*60,complete=True,mid=dict(h='1.2502',l='1.2498',c='1.2500')) for i in range(15)]))


def install_fake_submission(case,monkeypatch,*,before_validation=None,validation_quote=None):
    counters={'simulated_writes':0,'callback_results':[]}
    monkeypatch.setattr(r,'validate_config',lambda *args,**kwargs:case.runner.config)
    monkeypatch.setattr(r,'process_preflight',lambda:{'ready':True})
    def prepare_order(intent_id,pair,units,stop,*,price_bound,target_epoch):
        owner=SimpleNamespace(trial_id=r.TRIAL_ID,account_sha256=case.runner.config['account_sha256'])
        return r.PracticeTrialBroker._build_prepared(owner,intent_id,pair,units,stop,
            price_bound=price_bound,target_epoch=target_epoch,prepared_epoch=case.clock())
    def fake_submit(value,*,claim_submission,validate_before_submit):
        assert callable(validate_before_submit)
        assert case.ledger.context(case.ledger.intents()[0]['context_sha'])['signal']==signal()
        if not claim_submission(deepcopy(value)):
            return {'status':'not_submitted','reason_code':'claim_refused'}
        # Query through a second connection proves the claim committed before
        # any simulated write, not merely visible in the calling transaction.
        second=r.Ledger(case.path)
        try:assert second.intents()[0]['attempted']==1
        finally:second.close()
        if before_validation:before_validation()
        account=case.broker.account_summary()
        if validation_quote:
            original=case.broker.pricing
            def pricing(pairs):
                result=original(pairs)
                for quote in result['quotes'].values():quote.update(validation_quote)
                return result
            case.broker.pricing=pricing
        allowed=validate_before_submit(deepcopy(value),account,
            {'open_trades':case.broker.open_trades(),'pending_orders':case.broker.pending_orders()})
        counters['callback_results'].append(allowed)
        if allowed:counters['simulated_writes']+=1
        return {'status':'unknown' if allowed else 'not_submitted','reason_code':'simulated'}
    monkeypatch.setattr(case.broker,'prepare_market_order',prepare_order,raising=False)
    monkeypatch.setattr(case.broker,'submit_market',fake_submit)
    return counters


def test_final_callback_same_policy_exact_order_single_claim(case,monkeypatch):
    counters=install_fake_submission(case,monkeypatch)
    case.runner.enter(entry_context(),{'status':'available'})
    assert counters=={'simulated_writes':1,'callback_results':[True]}
    assert case.ledger.session(NOW)['entries_today']==1
    case.runner.enter(entry_context(),{'status':'available'})
    assert counters['simulated_writes']==1
    assert case.ledger.intents()[0]['status']=='unknown'


@pytest.mark.parametrize('quote',[{'bid':'1.2501','ask':'1.2503'},
                                 {'tradeable':False},{'market_epoch':NOW-16}])
def test_final_callback_unexecutable_bound_or_invalid_quote_refuses(case,monkeypatch,quote):
    counters=install_fake_submission(case,monkeypatch,validation_quote=quote)
    case.runner.enter(entry_context(),{'status':'available'})
    assert counters['simulated_writes']==0 and counters['callback_results']==[False]
    assert case.ledger.intents()[0]['status']=='not_submitted'
    assert case.ledger.session(NOW)['entries_today']==1


def test_changed_reoptimized_stop_does_not_reject_safe_immutable_order(case,monkeypatch):
    counters=install_fake_submission(case,monkeypatch,validation_quote={'bid':'1.2499','ask':'1.2502'})
    case.runner.enter(entry_context(),{'status':'available'})
    assert counters=={'simulated_writes':1,'callback_results':[True]}
    intent=case.ledger.intents()[0]
    context=case.ledger.context(intent['context_sha'])
    row=case.ledger.db.execute("SELECT sha FROM evidence WHERE kind='pretransport_revalidation'").fetchone()
    evidence=case.ledger.context(row[0])
    proof=evidence['immutable_order_validation']['candidate_result']
    assert evidence['check']['stop_loss_price']!=context['assessment']['stop_loss_price']
    assert proof['stop_loss_price']==intent['prepared']['order']['stopLossOnFill']['price']
    assert proof['original_target_epoch']==context['signal']['original_target_epoch']
    assert r.Decimal(proof['fresh_modeled_loss_usd'])<=r.Decimal(proof['fresh_risk_budget_usd'])
    assert r.Decimal(proof['fresh_modeled_margin_usd'])<=r.Decimal(proof['fresh_margin_budget_usd'])
    assert case.ledger.session(NOW)['entries_today']==1


def test_loss_breach_at_final_account_read_is_permanently_latched(case,monkeypatch):
    counters=install_fake_submission(case,monkeypatch,before_validation=lambda:setattr(case.broker,'nav','94'))
    case.runner.enter(entry_context(),{'status':'available'})
    assert counters['simulated_writes']==0 and case.ledger.get('loss_stop_latched') is True
    case.broker.nav='100'
    assert case.ledger.session(NOW)['loss_stop_latched'] is True


def test_loss_breach_at_initial_entry_read_is_latched_without_claim(case,monkeypatch):
    counters=install_fake_submission(case,monkeypatch);case.broker.nav='94'
    case.runner.enter(entry_context(),{'status':'available'})
    assert counters['simulated_writes']==0 and case.ledger.intents()==[]
    assert case.ledger.get('loss_stop_latched') is True


@pytest.mark.parametrize('latch',['stop_requested','entry_halt_latched','loss_stop_latched'])
def test_final_callback_new_stop_or_halt_refuses(case,monkeypatch,latch):
    counters=install_fake_submission(case,monkeypatch,before_validation=lambda:case.ledger.latch(latch))
    case.runner.enter(entry_context(),{'status':'available'})
    assert counters['simulated_writes']==0 and counters['callback_results']==[False]
    assert case.ledger.get(latch) is True


def test_final_callback_rechecks_config_and_process_after_claim(case,monkeypatch):
    def changed():monkeypatch.setattr(r,'process_preflight',lambda:{'ready':False})
    counters=install_fake_submission(case,monkeypatch,before_validation=changed)
    case.runner.enter(entry_context(),{'status':'available'})
    assert counters['simulated_writes']==0 and case.ledger.get('entry_halt_latched') is True


def test_final_callback_does_not_reuse_old_signal_clock(case,monkeypatch):
    counters=install_fake_submission(case,monkeypatch,before_validation=lambda:case.current.__setitem__(0,NOW+611))
    case.runner.enter(entry_context(),{'status':'available'})
    assert counters['simulated_writes']==0 and counters['callback_results']==[False]


def test_manual_stop_closes_confirmed_owned_and_completes_flat(case):
    retain(case);add_trade(case);case.broker.reconciled_status='closed_when_flat'
    case.ledger.latch('stop_requested')
    result=case.runner.cycle()
    assert result['state']=='completed_flat'
    assert case.ledger.get('completed_flat')['reason']=='manual_stop'
    assert ('close','101','GBP_USD') in case.broker.calls


def test_manual_stop_with_unknown_close_preserves_unresolved_state(case):
    retain(case);add_trade(case);case.broker.close_unknown=True;case.ledger.latch('stop_requested')
    result=case.runner.cycle()
    assert result['state']=='close_only_awaiting_confirmation'
    assert case.ledger.get('completed_flat') is None and len(case.broker.trades)==1


def test_mandatory_multiple_owned_closes_only_owned_without_account(case):
    one=add_trade(case);two=deepcopy(one);two['id']='102';case.broker.trades.append(two)
    foreign=deepcopy(one);foreign.update(id='103',owned=False);case.broker.trades.append(foreign)
    case.broker.account_error=True
    with pytest.raises(r.BrokerError):case.runner.cycle()
    assert ('close','101','GBP_USD') in case.broker.calls and ('close','102','GBP_USD') in case.broker.calls
    assert ('close','103','GBP_USD') not in case.broker.calls
    assert case.ledger.get('entry_halt_latched') is True


def test_missing_protection_closes_even_when_pending_read_fails(case):
    add_trade(case,stop=None);case.broker.pending_error=True
    with pytest.raises(r.BrokerError):case.runner.cycle()
    assert ('close','101','GBP_USD') in case.broker.calls


def test_context_corruption_closes_exact_owned_and_halts_entries(case):
    retain(case);add_trade(case);row=case.ledger.intents()[0]
    with case.ledger.db:
        case.ledger.db.execute('UPDATE evidence SET body=? WHERE sha=?',(b'corrupted',row['context_sha']))
    case.runner.manage(case.broker.account_summary(),case.broker.open_trades(),case.broker.pending_orders())
    assert ('close','101','GBP_USD') in case.broker.calls and case.ledger.get('entry_halt_latched') is True


def test_account_lock_native_binary_marker_acquire_heartbeat_and_release(tmp_path):
    path=tmp_path/'practice_order.lock'
    with r.AccountLock(path) as lock:
        marker=path.read_bytes()
        assert marker==lock.marker
        assert marker.endswith(b'\n') and not marker.endswith(b'\r\n')
        assert marker.startswith(str(os.getpid()).encode()+b' ')
        lock.check()
        before=path.stat().st_mtime_ns
        # Exercise the actual heartbeat thread and native file I/O. The
        # heartbeat must update liveness without translating or rewriting bytes.
        deadline=time.monotonic()+5
        while path.stat().st_mtime_ns==before and time.monotonic()<deadline:
            time.sleep(.05)
        assert path.stat().st_mtime_ns>before
        assert path.read_bytes()==marker
        assert lock.thread.is_alive()
        lock.check()
    assert not path.exists()
    assert lock.fd is None and not lock.thread.is_alive()


def test_account_lock_refuses_actual_live_pid_without_touching_marker(tmp_path):
    path=tmp_path/'practice_order.lock'
    marker=f'{os.getpid()} existing-owner\n'.encode()
    path.write_bytes(marker)
    with pytest.raises(RuntimeError,match='account_lock_busy'):
        with r.AccountLock(path):raise AssertionError('must not acquire another live owner')
    assert path.read_bytes()==marker
    assert list(tmp_path.glob('*.dead.*'))==[]


def test_account_lock_preserves_dead_marker_and_acquires_new_exact_bytes(tmp_path):
    import psutil
    dead_pid=2147483647
    assert psutil.pid_exists(dead_pid) is False
    path=tmp_path/'practice_order.lock';marker=f'{dead_pid} departed-owner\n'.encode()
    path.write_bytes(marker)
    with r.AccountLock(path) as lock:
        lock.check()
        assert path.read_bytes()==lock.marker and path.read_bytes()!=marker
        retained=list(tmp_path.glob('practice_order.lock.dead.*'))
        assert len(retained)==1 and retained[0].read_bytes()==marker
    assert not path.exists() and retained[0].read_bytes()==marker


@pytest.mark.parametrize('marker',[b'',b'not-a-pid\n',b'123.5 invalid\n'])
def test_account_lock_unknown_owner_is_not_removed(tmp_path,marker):
    path=tmp_path/'practice_order.lock';path.write_bytes(marker)
    with pytest.raises(RuntimeError,match='account_lock_busy'):
        with r.AccountLock(path):raise AssertionError('unknown ownership must refuse')
    assert path.read_bytes()==marker


def test_account_lock_lost_identity_is_not_deleted_on_exit(tmp_path):
    path=tmp_path/'practice_order.lock';replacement=b'foreign-marker\n'
    with r.AccountLock(path) as lock:
        path.write_bytes(replacement)
        with pytest.raises(RuntimeError,match='account_lock_lost'):lock.check()
    assert path.read_bytes()==replacement


@pytest.mark.parametrize('code',['account_lock_busy','account_lock_unavailable','account_lock_lost'])
def test_safe_error_exposes_only_known_local_runtime_codes(code):
    assert r.safe_error(RuntimeError(code))==dict(type='RuntimeError',code=code)


@pytest.mark.parametrize('message',['credential=private-secret','account_lock_lost extra-secret','account_lock_busy\nprivate-secret'])
def test_safe_error_never_echoes_arbitrary_runtime_text(message):
    assert r.safe_error(RuntimeError(message))==dict(type='RuntimeError',code='operation_failed')
