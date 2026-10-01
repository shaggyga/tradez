import copy
import datetime as dt
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'trad'))
import oanda_news_collector_progress_v1 as wrapper


def test_progress_requires_completed_consumption(monkeypatch):
    now = [0.]
    reports = []
    monkeypatch.setattr(wrapper.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(wrapper, 'report_completed', lambda *a: reports.append(a))
    items = wrapper.CompletedItems(['first', 'second'], 'test')
    iterator = iter(items)
    assert next(iterator) == 'first' and reports == []
    now[0] = 240.
    assert reports == []  # A stalled consumer has no timer-driven renewal.
    assert next(iterator) == 'second'
    assert reports == [('test', 1, 2)]
    iterator.close()  # Abandoned second item is not reported as complete.
    assert reports == [('test', 1, 2)]


def test_exhaustion_reports_last_item_but_empty_read_does_not(monkeypatch):
    reports = []
    monkeypatch.setattr(wrapper, 'report_completed', lambda *a: reports.append(a))
    view = wrapper.CompletedItems([1, 2, 3], 'test')
    assert len(view) == 3 and view[-1] == 3 and view[:2] == [1, 2]
    assert reports == []
    assert list(view) == [1, 2, 3]
    assert reports[-1] == ('test', 3, 3)
    reports.clear()
    assert list(wrapper.CompletedItems([], 'empty')) == [] and reports == []


def test_call_completion_is_not_reported_on_exception(monkeypatch):
    reports = []
    monkeypatch.setattr(wrapper, 'report_completed', lambda *a: reports.append(a))
    error = ValueError('failed work')
    def fail(): raise error
    with pytest.raises(ValueError) as seen: wrapper.completed_call(fail, 'operation')()
    assert seen.value is error and reports == []
    value = object()
    assert wrapper.completed_call(lambda: value, 'operation')() is value
    assert reports == [('operation_completed', 1, 1)]


def test_only_postprocessing_reports_and_no_owner_is_inert(monkeypatch):
    rows = []
    p = SimpleNamespace(phase='collecting_sources', update=lambda *a: rows.append(a))
    monkeypatch.setattr(wrapper.active_progress, 'value', p, raising=False)
    wrapper.report_completed('work', 1, 2)
    assert not rows
    p.phase = 'postprocessing_evidence'
    wrapper.report_completed('work', 1, 2)
    assert rows[0][1]['operational_completed_items'] == 1
    monkeypatch.delattr(wrapper.active_progress, 'value')
    wrapper.report_completed('work', 2, 2)
    assert len(rows) == 1


def test_actual_clustering_and_context_outputs_are_byte_equivalent(monkeypatch):
    news = wrapper.collector
    observed = dt.datetime(2026, 8, 18, 12, tzinfo=dt.timezone.utc)
    rows = [news.classify_article(dict(source_id='google_news_systemic_catalyst',
        source_kind='rss', source_verified=False, source_quality=.65,
        title=title, url='https://example.com/'+str(index),
        published_utc='2026-08-18T11:40:21Z'), first_seen=observed)
        for index, title in enumerate(['Vessel struck during Strait of Hormuz transit',
            'Ship attacked during Strait of Hormuz transit', 'Oil supply concerns mount'])]
    encode = lambda v: json.dumps(v, sort_keys=True, default=str)
    baseline = [encode(news.cluster_articles(copy.deepcopy(rows), as_of=observed)),
                encode(news.cluster_context_articles(copy.deepcopy(rows), as_of=observed))]
    reports = []
    monkeypatch.setattr(wrapper, 'report_completed', lambda *a: reports.append(a))
    monkeypatch.setattr(news, '_cluster_candidate_groups', wrapper.grouped_with_progress)
    assert encode(news.cluster_articles(copy.deepcopy(rows), as_of=observed)) == baseline[0]
    assert encode(news.cluster_context_articles(copy.deepcopy(rows), as_of=observed)) == baseline[1]
    assert any(r[0] == 'cluster_candidates_completed' for r in reports)
    assert any(r[0] == 'cluster_groups_completed' for r in reports)


def test_installation_wraps_original_calls_without_replacing_results(monkeypatch):
    originals = {}
    names = ('cluster_articles', 'cluster_context_articles', 'build_persistent_policy_state',
             'reconcile_topic_history', 'upsert_topic_events', 'write_ledger')
    for name in names:
        originals[name] = getattr(wrapper.collector, name)
        monkeypatch.setattr(wrapper.collector, name, originals[name])
    monkeypatch.setattr(wrapper.collector, '_cluster_candidate_groups', wrapper.original_groups)
    wrapper.install_postprocessing_progress()
    for name in names:
        assert getattr(wrapper.collector, name).__wrapped__ is originals[name]
