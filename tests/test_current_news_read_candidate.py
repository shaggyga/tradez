"""Actual staged scheduler initializer and original pre-issue read path, no process launch."""
import ast,json,sys
from pathlib import Path
from types import SimpleNamespace
import pytest
R=Path(__file__).resolve().parents[1];sys.path.insert(0,str(R/'trad'))
import oanda_news_health_retry_v1 as retry


def consumer(tmp_path,*,permanent=False,stale=False):
 paths={k:str(tmp_path/k) for k in ['clock_path','heartbeat_path','current_path']};calls=[];events=[]
 def read(path,limit):
  calls.append(path)
  if path==paths['current_path'] and (permanent or calls.count(path)==1):raise PermissionError('sharing violation')
  return json.dumps({'as_of_utc':-300 if stale else 100}).encode(),{'sha256':'exact'}
 io=SimpleNamespace(read_exact=read,_session=lambda _:dict(config=json.dumps(paths)),_health=lambda *a:None)
 class Parent:
  def __init__(self,*a,**kw):
   self.news_io=io;self.news_session='session';self.news_pool=object();self.states={};self.clock=lambda:101
 tree=ast.parse((R/'tools/runtime_candidates/oanda_joint_news_scheduler_current_read_v1.py').read_text())
 init=next(n for c in tree.body if isinstance(c,ast.ClassDef) and c.name=='ScheduledRunner' for n in c.body if isinstance(n,ast.FunctionDef) and n.name=='__init__')
 cls=ast.ClassDef(name='ScheduledRunner',bases=[ast.Name(id='Parent',ctx=ast.Load())],keywords=[],body=[init],decorator_list=[])
 def install(io,paths,*,record):return retry.install_health_read_retry(io,paths,record=record,sleep=lambda _:None)
 ns=dict(Parent=Parent,json=json,health_retry=SimpleNamespace(install_health_read_retry=install,NewsExecutor=lambda x,*a,**k:x),base=SimpleNamespace(capture_shared_owned=object()))
 exec(compile(ast.fix_missing_locations(ast.Module(body=[cls],type_ignores=[])),'staged_constructor','exec'),ns)
 ns['ScheduledRunner'](receipts=SimpleNamespace(write=lambda *a,**k:events.append((a,k))))
 tree=ast.parse((R/'trad/rolling_news_io_v1.py').read_text());fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='reobserve_before_issue')
 def need(ok,reason):
  if not ok:raise ValueError(reason)
 space=dict(time=SimpleNamespace(time=lambda:101),_eligible=lambda *a:({'config':json.dumps(paths),'graph':{}},None),json=json,need=need,source_graph=lambda:{},_health=lambda *a:{},read_exact=io.read_exact,original=SimpleNamespace(MAX_CURRENT_NEWS_BYTES=10000,_validate_current=lambda *a:None,_module=lambda *a:None,_epoch=float),strict_health_json=json.loads,adapter=SimpleNamespace(epoch=float),_put=lambda *a:'proof',INERT={},_failure=lambda *a:None)
 paths['archive_root']=str(tmp_path)
 exec(compile(ast.Module(body=[fn],type_ignores=[]),'original_preissue_consumer','exec'),space)
 return lambda:space['reobserve_before_issue']('session','capture'),calls,paths,events


def test_staged_actual_preissue_consumer_recovers_exact_current_path(tmp_path):
 run,calls,p,events=consumer(tmp_path);assert run()['observed_epoch']==101
 assert calls.count(p['current_path'])==2
 assert any(x[0]==('health_read_recovered',) for x in events)


def test_staged_actual_preissue_permanent_failure_remains_bounded(tmp_path):
 run,calls,p,_=consumer(tmp_path,permanent=True)
 with pytest.raises(PermissionError):run()
 assert calls.count(p['current_path'])==4


def test_recovered_bytes_do_not_bypass_original_expiry(tmp_path):
 run,calls,p,_=consumer(tmp_path,stale=True)
 with pytest.raises(ValueError,match='rolling_issue_news_stale'):run()
