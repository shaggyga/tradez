from pathlib import Path
import sqlite3
import sys
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
import forex_technical_store_maintenance as m

def fixture(path):
    with sqlite3.connect(path) as db:
        db.executescript('CREATE TABLE observations(pair TEXT,t INTEGER,published REAL,values_blob BLOB,PRIMARY KEY(pair,t)); CREATE TABLE outcomes(pair TEXT,t INTEGER,horizon INTEGER,state TEXT,body BLOB,PRIMARY KEY(pair,t,horizon));')
        db.executemany('INSERT INTO observations VALUES(?,?,?,?)',[('EUR_USD',i*60,i*60+60,b'feature') for i in range(100)])
        db.executemany('INSERT INTO outcomes VALUES(?,?,?,?,?)',[('EUR_USD',i*60,60,'available',b'original') for i in range(90)])
    return path

def test_identical_pending_and_outcome_populations_with_covering_indexes(tmp_path):
    p=fixture(tmp_path/'store.sqlite');v=m.maintain(p,apply=True)
    assert len(v['indexes_created'])==2 and v['content_updates']==0
    assert v['before']['EUR_USD_60']['rows']==v['after']['EUR_USD_60']['rows']==10
    assert v['before']['states']['result_sha256']==v['after']['states']['result_sha256']
    assert m.maintain(p,apply=True)['indexes_created']==[]

def test_read_only_does_not_add_indexes(tmp_path):
    p=fixture(tmp_path/'store.sqlite');assert m.maintain(p)['indexes_created']==[]
    with sqlite3.connect(p) as db:assert not db.execute("SELECT name FROM sqlite_master WHERE name LIKE 'idx_%'").fetchall()

def test_deadline_rolls_back(tmp_path):
    p=fixture(tmp_path/'store.sqlite')
    with pytest.raises(sqlite3.OperationalError):m.maintain(p,apply=True,maximum_seconds=-1)
    with sqlite3.connect(p) as db:assert not db.execute("SELECT name FROM sqlite_master WHERE name LIKE 'idx_%'").fetchall()
