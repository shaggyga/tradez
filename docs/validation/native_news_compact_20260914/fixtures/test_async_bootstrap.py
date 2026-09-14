from concurrent.futures import Future
from types import SimpleNamespace
import threading

import oanda_joint_price_news_forecast_study_v7 as worker


class Pool:
    def __init__(self):
        self.jobs = []

    def submit(self, function, *args, **kwargs):
        future = Future()
        self.jobs.append((function, args, kwargs, future))
        return future


def owner():
    runner = worker.PairRunner.__new__(worker.PairRunner)
    runner.news_pool = Pool()
    runner.news_session = object()
    state = {"lock": threading.RLock(), "failure_serial": 0}
    bootstrap = lambda *args, **kwargs: None
    runner.news_io = SimpleNamespace(bootstrap_inputs=bootstrap, _session=lambda session: state)
    runner.news_bootstrap_complete = False
    runner.news_bootstrap_future = None
    runner.news_bootstrap_next_monotonic = 0
    runner.news_bootstrap_report = None
    runner.news_future = None
    runner.news_schedule = worker.schedule.SharedNewsSchedule()
    runner.news_capture = runner.history_share = None
    runner.states = {}
    runner.queue = ["EUR_USD"]
    runner.errors = 0
    runner.last_error = ""
    times = [100.0]
    runner.monotonic = lambda: times[0]
    runner.clock = lambda: 2000 + times[0]
    runner.record_scheduler_event = lambda *args, **kwargs: None
    runner.last_poll = 0
    return runner, times, state


def test_cold_news_bootstrap_does_not_block_quote_settlement_and_status_tick():
    runner, times, state = owner()
    calls = []
    for method in ("finish_work", "schedule_work", "poll_quotes", "score", "publish_status"):
        setattr(runner, method, lambda method=method: calls.append(method))
    runner.tick()
    assert len(runner.news_pool.jobs) == 1
    assert not runner.news_pool.jobs[0][3].done()
    assert {"poll_quotes", "score", "publish_status"}.issubset(calls)
    assert runner.news_capture is None and runner.history_share is None
    runner.tick()
    assert len(runner.news_pool.jobs) == 1


def test_successful_bootstrap_cannot_supply_a_capture_or_skip_fresh_capture():
    runner, times, state = owner()
    runner.schedule_news()
    runner.news_bootstrap_future.set_result({
        "status": "cache_prepared_fresh_capture_required", "fresh_health_proven": False,
        "capture_handle_returned": False, "original_availability_unchanged": True,
        "elapsed_sec": 80, "manifest_sha256": "a" * 64,
    })
    state["failure_serial"] = 1
    runner.finish_news()
    assert runner.news_bootstrap_complete and runner.news_capture is None
    assert runner.news_schedule.failure_serial == 1
    runner.schedule_news()
    assert len(runner.news_pool.jobs) == 2
    assert runner.news_pool.jobs[1][0] is worker.capture_shared_owned
    assert runner.news_capture is None and runner.history_share is None


def test_failed_bootstrap_retries_after_cooldown_without_enabling_forecasts():
    runner, times, state = owner()
    runner.schedule_news()
    runner.news_bootstrap_future.set_exception(ValueError("original evidence unavailable"))
    runner.finish_news()
    assert not runner.news_bootstrap_complete and runner.errors == 1
    assert runner.news_capture is None
    runner.schedule_news()
    assert len(runner.news_pool.jobs) == 1
    times[0] += 61
    runner.schedule_news()
    assert len(runner.news_pool.jobs) == 2
    assert runner.news_pool.jobs[1][0] is runner.news_io.bootstrap_inputs


def test_long_failed_bootstrap_cooldown_starts_when_failure_is_observed():
    runner, times, state = owner()
    runner.schedule_news()
    times[0] += 121
    runner.news_bootstrap_future.set_exception(ValueError("cold validation time bound"))
    runner.finish_news()
    runner.schedule_news()
    assert len(runner.news_pool.jobs) == 1
    times[0] += 59
    runner.schedule_news()
    assert len(runner.news_pool.jobs) == 1
    times[0] += 2
    runner.schedule_news()
    assert len(runner.news_pool.jobs) == 2
    assert runner.news_capture is None and runner.history_share is None
