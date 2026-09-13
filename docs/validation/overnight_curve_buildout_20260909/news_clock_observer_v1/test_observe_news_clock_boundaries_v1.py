import importlib
import json
from pathlib import Path
import sqlite3
import sys
import time
import pytest
import observe_news_clock_boundaries_v1 as o

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
