from pathlib import Path
import datetime
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'trad'))
import oanda_news_collector_progress_v1 as wrapper


def test_phase_update_publishes_actual_progress_immediately(tmp_path, monkeypatch):
    rows = []
    monkeypatch.setattr(wrapper, 'original_publish', lambda **kw: rows.append(kw['progress'].snapshot()) or rows[-1])
    p = wrapper.progress_type(tmp_path)(datetime.datetime.now(datetime.timezone.utc))
    p.update('clustering', {'completed_rows': 1})
    assert len(rows) == 1 and rows[-1]['progress_sequence'] == 1
    p.update('publishing', {'completed_rows': 2})
    assert len(rows) == 2 and rows[-1]['details']['completed_rows'] == 2
    assert rows[-1]['last_progress_utc'] <= rows[-1]['generated_utc']


def test_periodic_snapshot_does_not_invent_progress(tmp_path, monkeypatch):
    monkeypatch.setattr(wrapper, 'original_publish', lambda **kw: kw['progress'].snapshot())
    p = wrapper.progress_type(tmp_path)(datetime.datetime.now(datetime.timezone.utc))
    before = p.snapshot()
    after = wrapper.publish(output_root=tmp_path, progress=p)
    assert after['progress_sequence'] == before['progress_sequence'] == 0
    assert after['last_progress_utc'] == before['last_progress_utc']


def test_same_phase_writes_are_bounded_but_state_is_current(tmp_path, monkeypatch):
    rows = []
    monkeypatch.setattr(wrapper, 'original_publish', lambda **kw: rows.append(kw['progress'].snapshot()) or rows[-1])
    monkeypatch.setattr(wrapper.time, 'monotonic', lambda: 100.)
    p = wrapper.progress_type(tmp_path)(datetime.datetime.now(datetime.timezone.utc))
    for i in range(10): p.update('collecting', {'completed_rows': i})
    assert len(rows) == 1
    assert p.snapshot()['progress_sequence'] == 10


def test_writers_are_serialized_and_snapshot_is_current(tmp_path, monkeypatch):
    active = 0
    maximum = 0
    lock = threading.Lock()
    def write(**kw):
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(maximum, active)
        result = kw['progress'].snapshot()
        with lock: active -= 1
        return result
    monkeypatch.setattr(wrapper, 'original_publish', write)
    p = wrapper.progress_type(tmp_path)(datetime.datetime.now(datetime.timezone.utc))
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _: wrapper.publish(output_root=tmp_path, progress=p), range(40)))
    assert maximum == 1
    assert p.snapshot()['progress_sequence'] == 0


def test_original_ingestion_source_is_unchanged():
    assert wrapper.sha(wrapper.collector.__file__) == wrapper.BASE_SHA256
