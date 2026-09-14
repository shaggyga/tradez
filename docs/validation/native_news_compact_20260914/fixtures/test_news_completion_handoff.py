"""Virtual-time orchestration regressions; no DB, broker, or real model orders."""
import ast
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace

import pytest
import oanda_joint_price_news_forecast_study_v7 as worker
from test_async_bootstrap import owner, Pool


SHA = 'a' * 64


class Ledger:
    def __init__(self):
        self.attempts = []
        self.settlements = 0

    def begin_attempt(self, bucket, quote):
        self.attempts.append((bucket, dict(quote)))
        return 'attempt-' + str(len(self.attempts))

    def settle(self):
        self.settlements += 1


def runner(monkeypatch, *, ready=True):
    r, times, state = owner()
    r.news_bootstrap_complete = True
    r.news_refresh_next_monotonic = 0
    r.future = r.active = None
    r.pool = Pool()
    r.prefer_fit = True
    r.fresh_capture_pair = None
    r.fresh_priority_allowed = True
    r.family_cursors = {}
    r.fit_cursor = 0
    r.news_io.session_status = lambda session: {'failure_serial': state['failure_serial'], 'usable': True}
    r.news_io._eligible = lambda session, handle: (state, [None, None, None, session, state['failure_serial']])
    r.news_io.capture_metadata = lambda handle: {'descriptor': {'capture_sha256': SHA}}
    ledger = Ledger()
    slot = {'ledger': ledger, 'last_success_bucket': -1, 'last_try': 0., 'failed_basis': None,
            'building': False, 'reason': '', 'last_attempt': None}
    capture = {'source_capture_sha256': 'b' * 64, 'first_observed_epoch': r.clock(),
               'news_first_observed_epoch': r.clock(), 'news_expires_epoch': r.clock() + 600}
    r.states = {'EUR_USD': {'capture': None, 'news_capture': None, 'history_share': None,
                           'families': {'ridge': slot}}}
    r.current = {'EUR_USD': {'ridge': {'market_epoch': r.clock()}}}
    calls = []
    r.record_scheduler_event = lambda event, *args, **kwargs: calls.append(event)
    r.poll_quotes = lambda: (calls.append('quotes'), r.current['EUR_USD']['ridge'].update(market_epoch=r.clock()))
    r.score = lambda: calls.append('outcome_score')
    r.publish_status = lambda: calls.append('status')
    monkeypatch.setattr(worker, 'input_readiness', lambda *args: {'status': 'ready' if ready else 'withheld'})

    def capture_dispatch(now, bucket):
        if r.news_capture is None:
            return False
        r.active = ('capture', 'EUR_USD', None)
        r.future = Future()
        calls.append('pair_capture')
        return True

    def finish():
        if r.future is None or not r.future.done():
            return
        kind = r.active[0]
        r.future.result()
        if kind == 'capture':
            r.states['EUR_USD'].update(capture=capture, news_capture=r.news_capture,
                                       history_share=r.history_share)
            r.fresh_capture_pair = 'EUR_USD'
        else:
            slot['building'] = False
            slot['last_success_bucket'] = int(r.clock() // 900)
            calls.append('fit_finished')
        r.future = r.active = None

    r.schedule_capture = capture_dispatch
    r.finish_work = finish
    return r, times, state, calls, ledger, capture


def complete_slow_news(r, times):
    r.schedule_news()
    assert len(r.news_pool.jobs) == 1
    times[0] += 114
    r.news_future.set_result((object(), object(), SHA, 0))


def test_114_second_capture_allows_real_guarded_fit_before_another_news_capture(monkeypatch):
    r, times, _, calls, ledger, capture = runner(monkeypatch)
    original_capture = dict(capture)
    complete_slow_news(r, times)
    r.tick()
    assert len(r.news_pool.jobs) == 1
    assert r.active[0] == 'capture'
    assert r.news_refresh_next_monotonic == times[0] + 60
    # Pair preparation itself runs past the cooldown. Its ready fit must get
    # the existing actual schedule_fit opportunity before the overdue refresh.
    times[0] += 66
    r.current['EUR_USD']['ridge']['market_epoch'] = r.clock()
    r.future.set_result(None)
    r.tick()
    assert r.active[0] == 'fit' and len(r.pool.jobs) == 1
    assert r.pool.jobs[0][0] is worker.fit_capture
    assert len(r.news_pool.jobs) == 1 and len(ledger.attempts) == 1
    assert r.pool.jobs[0][1][0] is capture and capture == original_capture
    # A running fit cannot be queued behind a new session-lock holder.
    times[0] += 61
    r.tick()
    assert len(r.news_pool.jobs) == 1
    r.future.set_result({'fixture': 'completed'})
    r.tick()
    assert len(r.news_pool.jobs) == 2 and r.news_future is not None
    assert len(ledger.attempts) == 1
    assert {'quotes', 'outcome_score', 'status', 'fit_finished'}.issubset(calls)
    assert ledger.settlements >= 2


def test_ineligible_fit_does_not_prevent_overdue_news_refresh(monkeypatch):
    r, times, _, _, ledger, _ = runner(monkeypatch, ready=False)
    complete_slow_news(r, times)
    r.tick()
    times[0] += 61
    r.future.set_result(None)
    r.tick()
    assert len(r.news_pool.jobs) == 2
    assert not ledger.attempts and r.future is None


def test_failed_long_capture_uses_observed_completion_cooldown(monkeypatch):
    r, times, state, calls, _, _ = runner(monkeypatch)
    r.schedule_news()
    times[0] += 114
    state['failure_serial'] = 1
    r.news_future.set_exception(ValueError('fixture source validation refused'))
    r.tick()
    assert r.news_future is None and r.news_capture is None
    assert len(r.news_pool.jobs) == 1 and r.errors == 1
    times[0] += 59
    r.tick()
    assert len(r.news_pool.jobs) == 1
    times[0] += 1
    r.tick()
    assert len(r.news_pool.jobs) == 2
    assert {'quotes', 'outcome_score', 'status'}.issubset(calls)


def test_news_dispatch_does_not_race_pending_pair_work(monkeypatch):
    r, _, _, _, _, _ = runner(monkeypatch)
    r.future = Future()
    r.schedule_news()
    assert not r.news_pool.jobs


def test_pair_dispatch_does_not_race_news_before_lock_acquisition(monkeypatch):
    r, _, _, calls, ledger, _ = runner(monkeypatch)
    r.news_capture = r.history_share = object()
    r.news_future = Future()
    r.schedule_work()
    assert 'pair_capture' not in calls and not ledger.attempts


def test_dispatch_exception_keeps_finite_backoff(monkeypatch):
    r, times, _, _, _, _ = runner(monkeypatch)
    attempts = []
    def refused(*args, **kwargs):
        attempts.append(times[0])
        times[0] += 114
        raise RuntimeError('fixture executor refusal')
    r.news_pool.submit = refused
    with pytest.raises(RuntimeError, match='executor refusal'):
        r.schedule_news()
    assert not r.news_schedule.capture_in_flight and r.news_future is None
    times[0] += 59
    r.schedule_news()
    assert len(attempts) == 1
    times[0] += 1
    with pytest.raises(RuntimeError, match='executor refusal'):
        r.schedule_news()
    assert len(attempts) == 2


def test_guarded_fit_and_clock_authority_methods_remain_exact_ast():
    here = Path(__file__).resolve()
    baseline = here.with_name('baseline_worker_stage006.py')
    def methods(path):
        module = ast.parse(path.read_text(encoding='utf-8'))
        cls = next(n for n in module.body if isinstance(n, ast.ClassDef) and n.name == 'PairRunner')
        return {node.name: ast.dump(node, include_attributes=False) for node in cls.body if isinstance(node, ast.FunctionDef)}
    before, after = methods(baseline), methods(Path(worker.__file__))
    for name in ('schedule_fit', 'schedule_capture', 'finish_work', 'news_hint', 'observe_news_failure', 'poll_quotes', 'score'):
        assert before[name] == after[name], name
