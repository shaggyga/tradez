"""Corrected account-lane boundaries, AST-only manager methods/fake transport."""
import ast
import hashlib
import importlib.util
import math
from pathlib import Path
import re
from types import SimpleNamespace,MethodType
from datetime import datetime,timezone
from decimal import Decimal,localcontext
import pytest

@pytest.mark.parametrize('status,expected,simulated',[
    ('accepted',1,0),('accepted_after_recheck',1,0),('uncertain_after_recheck',0,0),
    ('skipped',0,0),('error',0,0),('dry_run',0,1),('already_flat',0,0),
])
def test_weekend_summary_counts_only_confirmed_closes(status,expected,simulated):
    path=ROOT/'oanda_technical_account_manager_auto.py';raw=path.read_bytes()
    tree=ast.parse(raw);cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='ForexManager')
    method=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='run_volatile_weekend_flatten_pass')
    scope={'normalize_instrument':lambda x:x,'trade_direction_from_units':lambda x:'LONG',
           'iso_utc':lambda:'fixture-clock','log':lambda x:None}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[method],type_ignores=[])),str(path),'exec'),scope)
    state={'last_volatile_weekend_flatten_dry_run_count':9};obj=SimpleNamespace(cfg=SimpleNamespace(account_lane='tech'),load_state=lambda:state,
        save_state=lambda value:None,volatile_weekend_flatten_due=lambda *a,**k:True,
        oanda=SimpleNamespace(get_open_trades=lambda:[] if status=='already_flat' else [{'id':'fixture','instrument':'GBP_USD','currentUnits':'100'}]),
        lane_prefix=lambda:'fixture',close_trade=lambda *a:status,log_error=lambda *a:None)
    assert scope[method.name](obj)==expected
    assert state['last_volatile_weekend_flatten_count']==expected
    assert state['last_volatile_weekend_flatten_dry_run_count']==simulated
    assert path.read_bytes()==raw

ROOT=Path(__file__).resolve().parents[2]/'trad'
NOW=1788952000.0
spec=importlib.util.spec_from_file_location('candidate_trade_reconciliation',ROOT/'oanda_trade_reconciliation_v1.py')
core=importlib.util.module_from_spec(spec);spec.loader.exec_module(core)
FILES=('oanda_technical_account_manager_auto.py','oanda_advisor_account_manager_auto.py')
METHODS=('observe_open_trade','find_open_trade','trade_lookup_blocks_open','partial_close_trade',
    '_close_with_state_confirmation','close_trade','handle_position_action','tighten_trade')
HELPERS=('safe_float','safe_int','clamp','normalize_instrument','trade_direction_from_units',
         'as_optional_float','first_optional_float')

@pytest.fixture(params=FILES)
def manager(request):
    path=ROOT/request.param;raw=path.read_bytes();tree=ast.parse(raw)
    cls=next(node for node in tree.body if isinstance(node,ast.ClassDef) and node.name=='ForexManager')
    funcs=[n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name in METHODS]
    helpers=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in HELPERS]
    assert {n.name for n in funcs}==set(METHODS)
    module=ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0)]+helpers+funcs,type_ignores=[])
    scope=dict(math=math,re=re,time=SimpleNamespace(time=lambda:NOW),trade_reconciliation=core,cfg_bool=lambda *args,default=True:default)
    exec(compile(ast.fix_missing_locations(module),str(path),'exec'),scope)
    obj=SimpleNamespace(cfg=SimpleNamespace(broker_recheck_before_actions=True,partial_close_min_pct=10,partial_close_max_pct=90),
        logs=[],errors=[],requests=[],opens=[],remembered=[],source_sha=hashlib.sha256(raw).hexdigest())
    for name in METHODS:setattr(obj,name,MethodType(scope[name],obj))
    obj.should_execute=lambda:True
    obj.log_action=lambda action,kind,status,**kw:obj.logs.append(dict(kind=kind,status=status,**kw))
    obj.log_error=lambda *args:obj.errors.append('recheck unavailable')
    obj.action_key=lambda *args,**kw:'fixture_key';obj.recently_attempted_action=lambda _:False
    obj.remember_action=lambda key:obj.remembered.append(key)
    obj.open_trade_from_order=lambda action,prices:obj.opens.append(action)
    obj.instrument_meta={'GBP_USD':SimpleNamespace(pip_size=.0001,display_precision=5)}
    obj.trade=dict(id='fixture_target',instrument='GBP_USD',currentUnits='100',price='1.2',stopLossOrder={'price':'1.1'})
    obj.quote=dict(instrument='GBP_USD',time=datetime.fromtimestamp(NOW-1,timezone.utc).isoformat(),status='tradeable',
        bids=[{'price':'1.2099'}],asks=[{'price':'1.2101'}])
    def fail_close(trade_id,units=None):obj.requests.append(dict(kind='close',trade_id=trade_id,units=units));raise TimeoutError('fixture')
    def fail_read():raise TimeoutError('fixture')
    def dependent(*args):obj.requests.append(dict(kind='dependent',arguments=args));return {'fixture':True}
    obj.oanda=SimpleNamespace(close_trade=fail_close,get_open_trades=fail_read,get_prices=lambda _:{'GBP_USD':obj.quote},set_dependent_orders=dependent)
    yield obj
    assert hashlib.sha256(path.read_bytes()).hexdigest()==obj.source_sha

@pytest.mark.parametrize('recheck',[True,False])
@pytest.mark.parametrize('partial',[True,False])
def test_unknown_never_claims_close_or_requested_units(manager,recheck,partial):
    manager.cfg.broker_recheck_before_actions=recheck
    status=manager.partial_close_trade(manager.trade,{'partial_close_pct':50}) if partial else manager.close_trade(manager.trade,{})
    assert status=='uncertain_after_recheck' and manager.logs[-1]['units']==''
    assert manager.logs[-1]['raw_extra']['reconciliation']['complete_close'] is False

def test_failed_post_close_recheck_withholds_flip(manager):
    calls=[]
    def read():
        calls.append(1)
        if len(calls)==1:return [manager.trade]
        raise TimeoutError('fixture')
    manager.oanda.get_open_trades=read
    manager.handle_position_action({'action':'FLIP','trade_id':'fixture_target','instrument':'GBP_USD'}, {'fixture_target':manager.trade},{})
    assert manager.opens==[] and manager.remembered==[]

def test_confirmed_closed_after_ambiguous_response_permits_flip(manager):
    calls=[]
    def read():calls.append(1);return [manager.trade] if len(calls)==1 else []
    manager.oanda.get_open_trades=read
    manager.handle_position_action({'action':'FLIP','trade_id':'fixture_target','instrument':'GBP_USD'}, {'fixture_target':manager.trade},{})
    assert len(manager.opens)==1
    assert manager.logs[-1]['status']=='accepted_after_recheck'
    assert manager.logs[-1]['raw_extra']['reconciliation']['transaction_attribution'] is False

def test_missing_exact_id_does_not_retarget(manager):
    manager.oanda.get_open_trades=lambda:[{**manager.trade,'id':'fixture_other'}]
    assert manager.find_open_trade(trade_id='fixture_target') is None
    manager.handle_position_action({'action':'CLOSE','trade_id':'fixture_target','instrument':'GBP_USD'}, {'fixture_target':manager.trade},{})
    assert manager.requests==[]

def test_missing_id_in_original_by_id_does_not_fallback(manager):
    other={**manager.trade,'id':'fixture_other'};manager.oanda.get_open_trades=lambda:[other]
    manager.handle_position_action({'action':'CLOSE','trade_id':'fixture_target','instrument':'GBP_USD'}, {'fixture_other':other},{})
    assert manager.requests==[]

@pytest.mark.parametrize('after,expected',[('50','accepted_after_recheck'),('80','uncertain_after_recheck'),
    ('-50','uncertain_after_recheck'),('100','uncertain_after_recheck')])
def test_partial_requires_exact_signed_reduction(manager,after,expected):
    manager.oanda.get_open_trades=lambda:[{**manager.trade,'currentUnits':after}]
    assert manager.partial_close_trade(manager.trade,{'partial_close_pct':50})==expected

def test_unknown_blocks_entry_gate(manager):
    assert manager.trade_lookup_blocks_open('GBP_USD') is True
    manager.oanda.get_open_trades=lambda:[]
    assert manager.trade_lookup_blocks_open('GBP_USD') is False

def test_tighten_rejects_looser_stop_and_does_not_complete_dedupe(manager):
    manager.oanda.get_open_trades=lambda:[manager.trade]
    manager.handle_position_action({'action':'TIGHTEN','trade_id':'fixture_target','instrument':'GBP_USD','stop_loss':1.0}, {'fixture_target':manager.trade},{})
    assert manager.requests==[] and manager.remembered==[]

def test_tighten_accepts_more_protective_stop(manager):
    manager.oanda.get_open_trades=lambda:[manager.trade]
    assert manager.tighten_trade('fixture_target',{'instrument':'GBP_USD','stop_loss':1.15},manager.instrument_meta['GBP_USD'])=='accepted'
    assert manager.requests[-1]['arguments'][2]==1.15

@pytest.mark.parametrize('change',[{'time':datetime.fromtimestamp(NOW-31,timezone.utc).isoformat()},
    {'time':datetime.fromtimestamp(NOW+1,timezone.utc).isoformat()},{'instrument':'EUR_USD'},
    {'status':'non-tradeable'},{'tradeable':False},{'bids':[{'price':'2'}]}])
def test_tighten_rejects_invalid_current_quote(manager,change):
    manager.oanda.get_open_trades=lambda:[manager.trade];manager.quote.update(change)
    assert manager.tighten_trade('fixture_target',{'instrument':'GBP_USD','stop_loss':1.15},manager.instrument_meta['GBP_USD'])=='skipped'
    assert manager.requests==[]

def test_tighten_short_orientation(manager):
    manager.trade.update(currentUnits='-100',stopLossOrder={'price':'1.3'})
    manager.oanda.get_open_trades=lambda:[manager.trade]
    assert manager.tighten_trade('fixture_target',{'instrument':'GBP_USD','stop_loss':1.25},manager.instrument_meta['GBP_USD'])=='accepted'
    manager.requests.clear()
    assert manager.tighten_trade('fixture_target',{'instrument':'GBP_USD','stop_loss':1.35},manager.instrument_meta['GBP_USD'])=='skipped'
    assert manager.requests==[]

@pytest.mark.parametrize('rows',[None,{},[None],[{'id':'fixture'}],
    [{'id':'x','instrument':'GBP_USD','currentUnits':'1'}]*2])
def test_malformed_complete_observation_is_unknown(rows):
    assert core.observe_trade_rows(rows,trade_id='x',observed_epoch=NOW)['status']=='unknown'

def test_observation_is_detached_and_exact_id_instrument_conflict_refuses():
    row={'id':'x','instrument':'GBP_USD','currentUnits':'10'}
    got=core.observe_trade_rows([row],trade_id='x',observed_epoch=NOW)
    row['currentUnits']='999';assert got['trade']['currentUnits']=='10'
    assert core.observe_trade_rows([row],trade_id='x',instrument='EUR_USD',observed_epoch=NOW)['status']=='unknown'


def test_full_close_reconciliation_ignores_ambient_decimal_precision():
    trade={'id':'x','instrument':'GBP_USD','currentUnits':'123456789'}
    observed=core.observe_trade_rows([],trade_id='x',observed_epoch=NOW)
    expected=core.reconcile_reduction(trade,observed)
    with localcontext() as context:
        context.prec=2
        assert core.reconcile_reduction(trade,observed)==expected
    assert expected['requested_units']=='123456789'


def test_manager_intended_units_ignore_ambient_decimal_precision(manager):
    manager.trade['currentUnits']='123456789';manager.oanda.get_open_trades=lambda:[]
    with localcontext() as context:
        context.prec=2
        assert manager.close_trade(manager.trade,{})=='accepted_after_recheck'
    assert manager.logs[-1]['raw_extra']['requested_units']=='123456789'


def test_trailing_distance_ignores_ambient_decimal_precision(manager):
    kwargs=dict(instrument='GBP_USD',pip_size=.0001,display_precision=5,observed_epoch=NOW)
    action={'trailing_stop_pips':'12.3456'}
    expected=core.protective_update(manager.trade,action,manager.quote,**kwargs)
    with localcontext() as context:
        context.prec=2
        assert core.protective_update(manager.trade,action,manager.quote,**kwargs)==expected
    assert expected['trailing_distance']==.00123


@pytest.mark.parametrize('trade_patch',[{'id':'fixture_wrong'},{'instrument':'EUR_USD'}])
def test_by_id_mapping_cannot_retarget_or_change_instrument(manager,trade_patch):
    manager.trade.update(trade_patch);manager.oanda.get_open_trades=lambda:[manager.trade]
    manager.handle_position_action({'action':'CLOSE','trade_id':'fixture_target','instrument':'GBP_USD'}, {'fixture_target':manager.trade},{})
    assert manager.requests==[]


@pytest.mark.parametrize('trailing',[
    {'distance':'.001'}, {'distance':'.001','trailingStopValue':'1.2098'}])
def test_missing_or_stronger_existing_trailing_stop_not_weakened(manager,trailing):
    manager.trade['trailingStopLossOrder']=trailing;manager.oanda.get_open_trades=lambda:[manager.trade]
    assert manager.tighten_trade('fixture_target',{'instrument':'GBP_USD','stop_loss':1.15},manager.instrument_meta['GBP_USD'])=='skipped'
    assert manager.requests==[]


@pytest.mark.parametrize('bad_stamp',[None,'not a timestamp','2026-09-09T11:00:00'])
def test_missing_invalid_naive_quote_time_is_not_replaced(manager,bad_stamp):
    manager.quote['time']=bad_stamp;manager.oanda.get_open_trades=lambda:[manager.trade]
    assert manager.tighten_trade('fixture_target',{'instrument':'GBP_USD','stop_loss':1.15},manager.instrument_meta['GBP_USD'])=='skipped'
    assert manager.requests==[]


@pytest.mark.parametrize('path_name',FILES)
@pytest.mark.parametrize('response',[{},None,{'trades':None},{'trades':{}},{'trades':[]}])
def test_transport_requires_explicit_complete_trade_list(path_name,response):
    tree=ast.parse((ROOT/path_name).read_bytes())
    node=next(n for cls in tree.body if isinstance(cls,ast.ClassDef) for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='get_open_trades')
    module=ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0),node],type_ignores=[])
    scope={};exec(compile(ast.fix_missing_locations(module),str(ROOT/path_name),'exec'),scope)
    obj=SimpleNamespace(cfg=SimpleNamespace(oanda_account_id='fixture_only'),request=lambda *args,**kw:response)
    if response=={'trades':[]}:
        assert scope['get_open_trades'](obj)==[]
    else:
        with pytest.raises(ValueError,match='complete open-trades response'):scope['get_open_trades'](obj)
