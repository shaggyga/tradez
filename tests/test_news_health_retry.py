import ast
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'trad'))
from oanda_news_health_retry_v1 import capture_with_health_retry, NewsExecutor


def test_actual_health_read_can_refuse_publication_after_start_clock():
    source = ROOT / 'trad/rolling_news_io_v1.py'
    tree = ast.parse(source.read_text())
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_health')
    def check(value, now):
        if value['generated'] > now:
            raise ValueError('upstream_collector_stale_or_future')
    producer = SimpleNamespace(validate_clock_state=lambda *a: None,
                               validate_collector_observation=check)
    namespace = dict(adapter=SimpleNamespace(epoch=float),
        original=SimpleNamespace(CURRENT_PRODUCER_MODULE='producer', _module=lambda _: producer),
        read_exact=lambda *a: (b'publication', {'sha256': 'a' * 64}),
        strict_health_json=lambda _: {'generated': 100.1})
    exec(compile(ast.Module(body=[fn], type_ignores=[]), str(source), 'exec'), namespace)
    health = namespace['_health']
    config = dict(clock_path='clock', heartbeat_path='heartbeat')
    with pytest.raises(ValueError, match='stale_or_future'):
        health(config, lambda: 100.)
    assert health(config, lambda: 100.2)['observed_epoch'] == 100.2


def test_whole_capture_retried_once_only_after_healthy_recheck():
    calls = []
    records = []
    def capture(value, *, clock):
        calls.append((value, clock()))
        if len(calls) == 1:
            raise ValueError('upstream_collector_stale_or_future')
        return 'new-fully-validated-handle'
    proof = {'observed_epoch': 101., 'files': {'heartbeat': {'sha256': 'a' * 64}}}
    result = capture_with_health_retry(capture, ('same-session',), {'clock': lambda: 101.},
        recheck=lambda: proof, record=lambda *a, **k: records.append((a, k)))
    assert result == 'new-fully-validated-handle'
    assert calls == [('same-session', 101.), ('same-session', 101.)]
    assert len(records) == 1


@pytest.mark.parametrize('probe_error', [ValueError('still_stale'), OSError('read_failed')])
def test_still_stale_or_unreadable_does_not_retry(probe_error):
    calls = []
    def capture():
        calls.append(1)
        raise ValueError('upstream_collector_stale_or_future')
    def probe(): raise probe_error
    with pytest.raises(ValueError, match='upstream_collector'):
        capture_with_health_retry(capture, (), {}, recheck=probe, record=lambda *a, **k: None)
    assert len(calls) == 1


@pytest.mark.parametrize('error', [ValueError('source_binding_mismatch'), PermissionError('locked')])
def test_other_errors_are_not_retried(error):
    def capture(): raise error
    def forbidden(): pytest.fail('health probe must not run')
    with pytest.raises(type(error)):
        capture_with_health_retry(capture, (), {}, recheck=forbidden, record=forbidden)


def test_second_failure_is_final():
    calls = []
    def capture():
        calls.append(1)
        raise ValueError('upstream_collector_stale_or_future')
    with pytest.raises(ValueError):
        capture_with_health_retry(capture, (), {},
            recheck=lambda: {'observed_epoch': 1., 'files': {}}, record=lambda *a, **k: None)
    assert len(calls) == 2


def test_other_executor_work_and_shutdown_are_unchanged():
    class Executor:
        def submit(self, f, *args, **kwargs): return f(*args, **kwargs)
        def shutdown(self, **kwargs): return kwargs
    pool = NewsExecutor(Executor(), object(), recheck=lambda: None, record=lambda: None)
    assert pool.submit(lambda x: x + 1, 2) == 3
    assert pool.shutdown(wait=True, cancel_futures=True) == dict(wait=True, cancel_futures=True)
