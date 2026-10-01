import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace
import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = Path(os.environ.get('FOREX_RETRY_TEST_SOURCE', str(ROOT / 'trad/oanda_news_health_retry_v1.py')))
spec = importlib.util.spec_from_file_location('tested_health_retry', SOURCE)
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)


def test_retry_returns_exact_original_bytes_and_proof(tmp_path):
    path=tmp_path/'heartbeat.json'; calls=[]; sleeps=[]; records=[]
    result=(b'exact', {'sha256':'exact-hash'})
    def read(p,limit):
        calls.append((p,limit))
        if len(calls)<3: raise PermissionError('locked')
        return result
    io=SimpleNamespace(read_exact=read)
    m.install_health_read_retry(io,[path],sleep=sleeps.append,record=lambda *a,**k:records.append((a,k)))
    assert io.read_exact(path,100) is result
    assert calls==[(path,100)]*3 and sleeps==[.02,.05]
    assert records[0][1]['attempts']==3


def test_permanent_failure_bounded(tmp_path):
    path=tmp_path/'heartbeat'; calls=[]; sleeps=[]; records=[]
    error=PermissionError('permanent')
    def read(*args): calls.append(args); raise error
    io=SimpleNamespace(read_exact=read)
    m.install_health_read_retry(io,[path],sleep=sleeps.append,record=lambda *a,**k:records.append((a,k)))
    with pytest.raises(PermissionError) as caught: io.read_exact(path,8)
    assert caught.value is error and len(calls)==4 and sleeps==[.02,.05,.1]
    assert records[0][0]==('health_read_retry_exhausted',)


@pytest.mark.parametrize('error',[ValueError('bad_hash'),FileNotFoundError('missing'),OSError('io')])
def test_other_failures_not_retried(tmp_path,error):
    path=tmp_path/'heartbeat'; calls=[]
    def read(*args): calls.append(args); raise error
    io=SimpleNamespace(read_exact=read)
    m.install_health_read_retry(io,[path],sleep=lambda _:pytest.fail('sleep'),record=lambda *a,**k:pytest.fail('record'))
    with pytest.raises(type(error)): io.read_exact(path,8)
    assert len(calls)==1


def test_unlisted_file_not_retried(tmp_path):
    def read(*args): raise PermissionError('archive')
    io=SimpleNamespace(read_exact=read)
    m.install_health_read_retry(io,[tmp_path/'heartbeat'],sleep=lambda _:pytest.fail('sleep'),record=lambda *a,**k:pytest.fail('record'))
    with pytest.raises(PermissionError): io.read_exact(tmp_path/'archive',8)


def test_actual_health_validator_remains_strict(tmp_path):
    # Execute the unchanged consumer health function with controlled file bytes.
    import ast, json
    tree=ast.parse((ROOT/'trad/rolling_news_io_v1.py').read_text())
    fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_health')
    calls=[]; heartbeat=tmp_path/'heartbeat'; clockpath=tmp_path/'clock'
    def read(path,limit):
        calls.append(path)
        if path==heartbeat and calls.count(path)==1: raise PermissionError('locked')
        return b'{"generated":101}',{'sha256':'exact'}
    io=SimpleNamespace(read_exact=read)
    m.install_health_read_retry(io,[heartbeat,clockpath],record=lambda *a,**k:None,sleep=lambda _:None)
    def validate(value,now):
        if value['generated']>now: raise ValueError('upstream_collector_stale_or_future')
    producer=SimpleNamespace(validate_clock_state=lambda *a:None,validate_collector_observation=validate)
    ns=dict(adapter=SimpleNamespace(epoch=float),original=SimpleNamespace(CURRENT_PRODUCER_MODULE='producer',_module=lambda _:producer),read_exact=io.read_exact,strict_health_json=json.loads)
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'original_health','exec'),ns)
    with pytest.raises(ValueError,match='stale_or_future'):
        ns['_health']({'clock_path':clockpath,'heartbeat_path':heartbeat},lambda:100)
    assert calls.count(heartbeat)==2
