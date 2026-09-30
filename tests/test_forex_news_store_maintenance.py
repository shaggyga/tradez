import importlib.util
from pathlib import Path
import sqlite3
import pytest

spec=importlib.util.spec_from_file_location('maintenance',Path(__file__).resolve().parents[1]/'tools/forex_news_store_maintenance.py')
maintenance=importlib.util.module_from_spec(spec);spec.loader.exec_module(maintenance)


def store(tmp_path):
    path=tmp_path/'news.sqlite'
    with sqlite3.connect(path) as db:
        db.executescript('CREATE TABLE articles(event_id TEXT PRIMARY KEY,payload_json TEXT,first_seen_utc TEXT,last_seen_utc TEXT,duplicate_count INTEGER,relevant INTEGER,published_utc TEXT); CREATE TABLE topic_events(topic_signature TEXT,published_utc TEXT); CREATE INDEX idx_articles_relevant ON articles(relevant,first_seen_utc);')
        db.executemany('INSERT INTO articles VALUES(?,?,?,?,?,?,?)',[(str(i),'payload:'+str(i),'2026-09-29','2026-09-30',i%3,i%2,'2026-09-30') for i in range(100)])
    return path


def test_index_application_keeps_payloads_order_and_is_idempotent(tmp_path):
    path=store(tmp_path)
    with sqlite3.connect(path) as db: before=list(db.execute('SELECT * FROM articles ORDER BY event_id'))
    result=maintenance.maintain(path,apply=True,since='2026-09-29')
    assert len(result['indexes_created'])==2
    assert result['before']==result['after']
    assert any('TEMP B-TREE' in r[-1] for r in result['before_plan'])
    assert not any('TEMP B-TREE' in r[-1] for r in result['after_plan'])
    assert maintenance.maintain(path,apply=True,since='2026-09-29')['indexes_created']==[]
    with sqlite3.connect(path) as db: assert before==list(db.execute('SELECT * FROM articles ORDER BY event_id'))


def test_read_only_does_not_create_indexes(tmp_path):
    path=store(tmp_path)
    result=maintenance.maintain(path,since='2026-09-29')
    assert result['indexes_created']==[]
    with sqlite3.connect(path) as db:
        assert not db.execute("SELECT name FROM sqlite_master WHERE name=?",(next(iter(maintenance.INDEXES)),)).fetchall()


def test_conflicting_index_refused(tmp_path):
    path=store(tmp_path)
    with sqlite3.connect(path) as db:
        db.execute('CREATE INDEX idx_articles_relevant_published_event_v1 ON articles(event_id)')
    with pytest.raises(ValueError,match='definition_mismatch'):
        maintenance.maintain(path,apply=True,since='2026-09-29')


def test_missing_database_not_created(tmp_path):
    path=tmp_path/'missing.sqlite'
    with pytest.raises(ValueError,match='existing_plain'): maintenance.maintain(path,apply=True)
    assert not path.exists()


def test_deadline_rolls_back(tmp_path):
    path=store(tmp_path)
    with pytest.raises(sqlite3.OperationalError): maintenance.maintain(path,apply=True,since='2026-09-29',maximum_seconds=-1)
    with sqlite3.connect(path) as db:
        assert not db.execute("SELECT name FROM sqlite_master WHERE name=?",(next(iter(maintenance.INDEXES)),)).fetchall()
