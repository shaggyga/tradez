import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'trad'))
import oanda_news_producer_journal_v1 as wrapper


def records(journal):
    return [json.loads(line) for line in journal.path.read_text().splitlines()]


def test_actual_cycle_retains_failure_after_success_overwrites_heartbeat(tmp_path, monkeypatch):
    calls = []
    def capture(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise PermissionError('example publication-independent input failure')
        return {'status': 'current', 'generated_utc': '2026-10-01T00:00:00Z'}
    monkeypatch.setattr(wrapper.producer, 'capture_repaired_snapshot', capture)
    j = wrapper.Journal(tmp_path, {'source': wrapper.BASE_SHA256})
    failed = j.cycle(tmp_path, clock=lambda: 1000.)
    healthy = j.cycle(tmp_path, clock=lambda: 1015., previous_errors=failed['errors'])
    j.cycle(tmp_path, clock=lambda: 1030., previous_errors=healthy['errors'])
    assert failed['status'] == 'unavailable' and failed['errors'] == 1
    assert healthy['status'] == 'current' and healthy['errors'] == 1
    current = json.loads((tmp_path/'local_news_sentiment_repair_v2/heartbeat_v2.json').read_bytes())
    assert current['last_error'] == ''
    rows = records(j)
    assert [r['event'] for r in rows] == ['start', 'cycle_failure', 'recovered']
    assert rows[1]['heartbeat'] == failed and rows[2]['heartbeat'] == healthy
    assert 'PermissionError' in rows[1]['heartbeat']['last_error']
    j.close()


def test_unhandled_error_is_logged_and_same_exception_reraised(tmp_path, monkeypatch):
    error = OSError('heartbeat publication failed')
    def fail(*a, **k): raise error
    monkeypatch.setattr(wrapper, 'original_cycle', fail)
    j = wrapper.Journal(tmp_path, {})
    with pytest.raises(OSError) as seen: j.cycle(tmp_path)
    assert seen.value is error
    assert records(j)[-1]['event'] == 'unhandled_cycle_failure'
    j.close()


def test_journal_io_failure_does_not_mask_producer_refusal(tmp_path, monkeypatch, capsys):
    heartbeat = {'status': 'unavailable', 'last_error': 'real refusal'}
    monkeypatch.setattr(wrapper, 'original_cycle', lambda *a, **k: heartbeat)
    j = wrapper.Journal(tmp_path, {})
    monkeypatch.setattr(wrapper.os, 'fsync', lambda *a: (_ for _ in ()).throw(OSError('disk')))
    assert j.cycle(tmp_path) is heartbeat
    assert 'news_producer_journal_failed:OSError' in capsys.readouterr().err
    j.close()


def test_context_has_exact_hash_and_bounded_fields(tmp_path):
    p = tmp_path/'state/clock_integrity_v1.json';p.parent.mkdir()
    p.write_text('{"status":"ok","unrelated":"not recorded"}')
    evidence = wrapper.health_evidence(tmp_path)
    assert evidence['clock']['sha256'] == wrapper.sha(p)
    assert 'unrelated' not in evidence['clock']['fields']
    p.write_bytes(b'x'*131073)
    assert 'byte_bound' in wrapper.health_evidence(tmp_path)['clock']['read_error']


def test_restart_preserves_prior_journal_and_baseline_counter(tmp_path):
    p=tmp_path/'local_news_sentiment_repair_v2/heartbeat_v2.json';p.parent.mkdir()
    p.write_text('{"status":"current","errors":14,"last_error":""}')
    a=wrapper.Journal(tmp_path, {});a.close();saved=a.path.read_bytes()
    b=wrapper.Journal(tmp_path, {})
    assert b.path != a.path and a.path.read_bytes() == saved
    assert records(b)[0]['evidence']['producer']['fields']['errors']==14
    b.close()


def test_original_producer_source_unchanged():
    assert wrapper.sha(wrapper.producer.__file__) == wrapper.BASE_SHA256
