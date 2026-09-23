import importlib
import json
from pathlib import Path
import sqlite3
import sys
import time
from types import SimpleNamespace
import pytest
import observe_news_clock_boundaries_v2 as o

sys.path.insert(0,str(o.ROOT))
producer=importlib.import_module('oanda_local_news_sentiment_repair_v1')


def test_missing_clock_names_path_and_type_without_inventing_time():
    value=o.clock_field(producer,None,'cycle_started_utc',False)
    assert value==dict(path='cycle_started_utc',present=False,type='NoneType',status='invalid',reason_code='news_clock_invalid')


def test_malformed_clock_text_cannot_escape_sanitized_report():
    value=o.clock_field(producer,'PRIVATE ARTICLE TEXT','generated_utc')
    assert value['status']=='invalid' and 'PRIVATE' not in json.dumps(value)


def test_frozen_clock_state_validator_uses_own_read_completion():
    stamp=producer.iso(time.time()+60)
    value=o.inspect(producer,'clock_state',dict(generated_utc=stamp,status='ok'),time.time())
    assert value['validation']['reason_code']=='news_clock_state_stale_or_future'


def test_each_new_failure_count_can_trigger_once_but_initial_failure_does_not():
    baseline,trigger=o.new_failure(None,{'errors':5});assert baseline==5 and not trigger
    baseline,trigger=o.new_failure(baseline,{'errors':5});assert not trigger
    baseline,trigger=o.new_failure(baseline,{'errors':6});assert trigger
    baseline,trigger=o.new_failure(baseline,{'errors':6});assert not trigger


def test_unique_bytes_preserve_first_observation_and_return_detached_metadata(tmp_path):
    store=o.PrivateStore(tmp_path);a=store.retain('clock',b'{}',dict(read_completed_epoch=1));a['bytes']=999
    b=store.retain('clock',b'{}',dict(read_completed_epoch=2))
    assert b['first_observed_read_completed_epoch']==1 and b['bytes']==2 and len(store.seen)==1


def test_private_byte_bound_precedes_new_file_creation(tmp_path,monkeypatch):
    monkeypatch.setattr(o,'MAX_PRIVATE_BYTES',1);store=o.PrivateStore(tmp_path)
    with pytest.raises(ValueError,match='observer_private_byte_bound'):store.retain('clock',b'{}',dict(read_completed_epoch=1))
    assert not list((tmp_path/'private_unique_bytes').iterdir())


def test_single_query_only_article_diagnostic_identifies_missing_publication_without_ids(tmp_path,monkeypatch):
    data=tmp_path/'data';news=data/'local_news_sentiment';news.mkdir(parents=True)
    path=news/'local_news_sentiment_v1.sqlite';stamp=producer.iso(time.time()-10)
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE articles(event_id TEXT,published_utc TEXT,first_seen_utc TEXT,last_seen_utc TEXT,relevant INTEGER,duplicate_count INTEGER,payload_json TEXT)')
        payload=dict(event_id='PRIVATE_EVENT_ID',relevant=True,published_utc=stamp)
        db.execute('INSERT INTO articles VALUES(?,?,?,?,?,?,?)',('PRIVATE_EVENT_ID',None,stamp,stamp,1,0,json.dumps(payload)))
    before=path.read_bytes();monkeypatch.setattr(o,'DATA',data);output=tmp_path/'out';output.mkdir()
    result=o.diagnose_articles(producer,o.PrivateStore(output))
    assert result['article_validation']['reason_code']=='news_clock_invalid'
    assert result['invalid_epoch_fields'][0]['path']=='rows[0].published_utc'
    assert result['invalid_epoch_fields'][0]['type']=='NoneType'
    assert 'PRIVATE_EVENT_ID' not in json.dumps(result) and path.read_bytes()==before


def test_fixed_deadline_four_hour_bound_and_sample_ceiling():
    assert o.END_EPOCH==1788957900.0
    assert o.END_UTC=='2026-09-09T12:45:00+00:00'
    window=o.make_window(o.END_EPOCH-14400,50)
    assert window['max_samples']==14401 and window['monotonic_stop']==14450
    assert o.window_open(window,o.END_EPOCH-1,14449,14400)
    assert not o.window_open(window,o.END_EPOCH-1,14449,14401)


@pytest.mark.parametrize('wall',[o.END_EPOCH,o.END_EPOCH+1,o.END_EPOCH-14400.001])
def test_start_must_be_inside_declared_fixed_window(wall):
    with pytest.raises(ValueError,match='observer_start_outside_fixed_window'):
        o.make_window(wall,1)


@pytest.mark.parametrize('wall',[True,None,float('nan'),float('inf'),-1])
def test_invalid_start_clock_refuses(wall):
    with pytest.raises(ValueError,match='observer_invalid_wall_clock'):o.make_window(wall,1)


@pytest.mark.parametrize('mono',[True,None,float('nan'),float('inf'),-1])
def test_invalid_monotonic_start_refuses(mono):
    with pytest.raises(ValueError,match='observer_invalid_monotonic_clock'):
        o.make_window(o.END_EPOCH-10,mono)


def test_deadline_equality_and_forward_clock_jump_stop_without_extension():
    window=o.make_window(o.END_EPOCH-10,20)
    assert not o.window_open(window,o.END_EPOCH,21,1)
    assert not o.window_open(window,o.END_EPOCH+100,21,1)
    assert not o.window_open(window,o.END_EPOCH-9,30,1)


def test_wall_rollback_cannot_extend_monotonic_budget():
    window=o.make_window(o.END_EPOCH-10,20)
    with pytest.raises(ValueError,match='observer_wall_clock_regression_or_invalid'):
        o.window_open(window,o.END_EPOCH-11,21,1)
    # Even a wall clock still above the start cannot override the original budget.
    assert not o.window_open(window,o.END_EPOCH-9,30,1)
    with pytest.raises(ValueError,match='observer_monotonic_clock_regression_or_invalid'):
        o.window_open(window,o.END_EPOCH-9,19,1)


def test_sleep_bound_respects_remaining_wall_and_monotonic_time():
    window=o.make_window(o.END_EPOCH-10,20)
    assert o.pause_seconds(window,21,o.END_EPOCH-10,20)==1
    assert o.pause_seconds(window,30,o.END_EPOCH-.25,29)==.25
    assert o.pause_seconds(window,31,o.END_EPOCH-1,29.75)==.25
    assert o.pause_seconds(window,30,o.END_EPOCH,29)==0
    assert o.pause_seconds(window,21,o.END_EPOCH-8,22)==0


def test_run_fixed_window_starts_no_late_sample_and_uses_one_diagnostic(tmp_path,monkeypatch):
    class Clock:
        elapsed=0.0
        def time(self):return o.END_EPOCH-3.5+self.elapsed
        def monotonic(self):return self.elapsed
        def sleep(self,seconds):
            assert 0<=seconds<=1
            self.elapsed+=seconds
    clock=Clock();reads=[];diagnostics=[]
    fake=SimpleNamespace(SOURCE_FILES=(),source_bindings=lambda:{})
    monkeypatch.setattr(o,'BASE',tmp_path)
    monkeypatch.setattr(o,'verify_sources',lambda:{})
    monkeypatch.setattr(o.importlib,'import_module',lambda name:fake)
    monkeypatch.setattr(o,'time',clock)
    monkeypatch.setattr(o,'FILES',{'repaired_heartbeat':tmp_path/'unused.json'})
    def read_fixture(path,cap=o.MAX_FILE_BYTES):
        now=clock.time();assert now<o.END_EPOCH;reads.append(now)
        raw=o.encoded({'errors':5+len(reads),'status':'unavailable'})
        return raw,dict(read_started_epoch=now,read_completed_epoch=now,sha256=o.digest(raw),bytes=len(raw))
    monkeypatch.setattr(o,'read',read_fixture)
    monkeypatch.setattr(o,'inspect',lambda *args:dict(clock_fields=[],status='unavailable'))
    def diagnostic(*args):
        diagnostics.append(clock.time());return dict(status='synthetic_later_diagnostic')
    monkeypatch.setattr(o,'diagnose_articles',diagnostic)
    output=tmp_path/'disposable_observation'
    o.run(output)
    result=json.loads((output/'NEWS_CLOCK_PASSIVE_OBSERVATION_20260909.json').read_bytes())
    started=json.loads((output/'OBSERVATION_STARTED.json').read_bytes())
    assert len(reads)==4 and len(diagnostics)==1 and result['sample_count']==4
    assert result['completed_epoch']==o.END_EPOCH and result['GET'] is False
    assert result['runtime_writes'] is False and result['all_registered_sources_unchanged']
    assert started['fixed_end_epoch']==o.END_EPOCH and started['max_samples']==4
    assert all(b-a>=1 for a,b in zip(reads,reads[1:]))


def test_late_run_refuses_before_output_creation(tmp_path,monkeypatch):
    fake=SimpleNamespace(SOURCE_FILES=(),source_bindings=lambda:{})
    monkeypatch.setattr(o,'BASE',tmp_path)
    monkeypatch.setattr(o,'verify_sources',lambda:{})
    monkeypatch.setattr(o.importlib,'import_module',lambda name:fake)
    monkeypatch.setattr(o,'time',SimpleNamespace(time=lambda:o.END_EPOCH,monotonic=lambda:1))
    output=tmp_path/'never_created'
    with pytest.raises(ValueError,match='observer_start_outside_fixed_window'):o.run(output)
    assert not output.exists()
