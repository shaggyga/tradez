"""Reproduce existing source behavior with AST-only methods and fake transport.

Passing tests demonstrate the old defects; they do not accept those behaviors.
No manager module import, account access, real transport, source edit or state IO.
"""
import ast
import hashlib
import math
from pathlib import Path
import re
from types import SimpleNamespace, MethodType

import pytest

ROOT=Path(__file__).resolve().parents[2]/'trad'
FILES=('oanda_technical_account_manager_auto.py','oanda_advisor_account_manager_auto.py')
METHODS=('find_open_trade','partial_close_trade','close_trade','handle_position_action','tighten_trade')
HELPERS=('safe_float','safe_int','clamp','normalize_instrument','trade_direction_from_units',
         'as_optional_float','first_optional_float')


@pytest.fixture(params=FILES)
def manager(request):
    path=ROOT/request.param;raw=path.read_bytes();tree=ast.parse(raw,filename=str(path))
    cls=next(node for node in tree.body if isinstance(node,ast.ClassDef) and node.name=='ForexManager')
    methods=[node for node in cls.body if isinstance(node,ast.FunctionDef) and node.name in METHODS]
    helpers=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in HELPERS]
    assert {node.name for node in methods}==set(METHODS)
    assert {node.name for node in helpers}==set(HELPERS)
    module=ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0)]+helpers+methods,type_ignores=[])
    namespace=dict(math=math,re=re,cfg_bool=lambda *args,default=True:default)
    exec(compile(ast.fix_missing_locations(module),str(path),'exec'),namespace)
    obj=SimpleNamespace(cfg=SimpleNamespace(broker_recheck_before_actions=True,partial_close_min_pct=10,partial_close_max_pct=90),
        source_path=str(path),source_sha256=hashlib.sha256(raw).hexdigest(),logs=[],errors=[],requests=[],opens=[],remembered=[])
    for name in METHODS:setattr(obj,name,MethodType(namespace[name],obj))
    obj.should_execute=lambda:True
    obj.log_action=lambda action,kind,status,**kw:obj.logs.append(dict(kind=kind,status=status,**kw))
    obj.log_error=lambda kind,error:obj.errors.append(dict(kind=kind,error_type=type(error).__name__))
    obj.action_key=lambda *args,**kw:'fixture_key'
    obj.recently_attempted_action=lambda _:False
    obj.remember_action=lambda key:obj.remembered.append(key)
    obj.open_trade_from_order=lambda action,prices:obj.opens.append(dict(action=action,prices=prices))
    obj.instrument_meta={'GBP_USD':SimpleNamespace(pip_size=.0001,display_precision=5)}
    obj.trade=dict(id='fixture_target',instrument='GBP_USD',currentUnits='100',price='1.2',stopLossOrder={'price':'1.1'})
    def fail_close(trade_id,units=None):
        obj.requests.append(dict(kind='close',trade_id=trade_id,units=units));raise TimeoutError('synthetic close response unknown')
    def fail_read():raise TimeoutError('synthetic recheck unavailable')
    def dependent(*args):obj.requests.append(dict(kind='dependent_orders',arguments=args));return {'fixture':True}
    obj.oanda=SimpleNamespace(close_trade=fail_close,get_open_trades=fail_read,set_dependent_orders=dependent)
    yield obj
    assert hashlib.sha256(path.read_bytes()).hexdigest()==obj.source_sha256


@pytest.mark.parametrize('recheck',[True,False])
def test_demonstrates_unknown_full_close_is_mislabeled_confirmed(manager,recheck):
    manager.cfg.broker_recheck_before_actions=recheck
    status=manager.close_trade(manager.trade,{})
    assert status=='accepted_after_recheck'
    assert manager.logs[-1]['status']=='accepted_after_recheck'
    assert len(manager.errors)==int(recheck)


@pytest.mark.parametrize('recheck',[True,False])
def test_demonstrates_unknown_partial_close_becomes_zero_remaining(manager,recheck):
    manager.cfg.broker_recheck_before_actions=recheck
    status=manager.partial_close_trade(manager.trade,{'partial_close_pct':50})
    assert status=='accepted_after_recheck'
    assert manager.logs[-1]['units']==50
    assert len(manager.errors)==int(recheck)


def test_demonstrates_failed_close_and_failed_recheck_admit_flip_open(manager):
    calls=[]
    def reads():
        calls.append(True)
        if len(calls)==1:return [manager.trade]
        raise TimeoutError('synthetic failed recheck after failed close')
    manager.oanda.get_open_trades=reads
    manager.handle_position_action({'action':'FLIP','trade_id':'fixture_target','instrument':'GBP_USD'},
        {'fixture_target':manager.trade},{})
    assert len(manager.requests)==1 and manager.requests[0]['kind']=='close'
    assert len(manager.opens)==1 and manager.opens[0]['action']['action']=='OPEN'
    assert manager.logs[-1]['status']=='accepted_after_recheck'


def test_confirmed_still_open_control_does_not_admit_flip(manager):
    manager.oanda.get_open_trades=lambda:[manager.trade]
    manager.handle_position_action({'action':'FLIP','trade_id':'fixture_target','instrument':'GBP_USD'},
        {'fixture_target':manager.trade},{})
    assert manager.opens==[] and manager.logs[-1]['status']=='skipped'


def test_demonstrates_exact_missing_id_falls_back_to_other_trade(manager):
    other={**manager.trade,'id':'fixture_other'}
    manager.oanda.get_open_trades=lambda:[other]
    assert manager.find_open_trade(trade_id='fixture_target') is other


def test_demonstrates_handle_action_retargets_a_different_same_pair_trade(manager):
    other={**manager.trade,'id':'fixture_other'}
    manager.oanda.get_open_trades=lambda:[other]
    def close(trade_id,units=None):manager.requests.append(dict(kind='close',trade_id=trade_id,units=units));return {'fixture':True}
    manager.oanda.close_trade=close
    manager.handle_position_action({'action':'CLOSE','trade_id':'fixture_target','instrument':'GBP_USD'},
        {'fixture_target':manager.trade},{})
    assert manager.requests[0]['trade_id']=='fixture_other'


def test_demonstrates_any_unit_reduction_is_logged_as_requested_partial_amount(manager):
    manager.oanda.get_open_trades=lambda:[{**manager.trade,'currentUnits':'80'}]
    status=manager.partial_close_trade(manager.trade,{'partial_close_pct':50})
    assert status=='accepted_after_recheck' and manager.logs[-1]['units']==50


def test_demonstrates_tighten_does_not_reject_a_looser_existing_stop(manager):
    manager.oanda.get_open_trades=lambda:[manager.trade]
    manager.handle_position_action({'action':'TIGHTEN','trade_id':'fixture_target','instrument':'GBP_USD','stop_loss':1.0},
        {'fixture_target':manager.trade},{})
    assert manager.requests[0]['kind']=='dependent_orders'
    assert manager.requests[0]['arguments'][2]==1.0
    assert manager.logs[-1]['status']=='accepted'
