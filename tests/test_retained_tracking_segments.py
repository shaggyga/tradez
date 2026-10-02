import json
import shutil
import sqlite3
from pathlib import Path
import sys

import pytest

sys.path[:0]=[str(Path(__file__).resolve().parents[1]/'trad'),str(Path(__file__).resolve().parent)]
import oanda_retained_forecast_tracking_v1 as m
from test_retained_forecast_tracking import fixtures


def rollover(tmp_path,monkeypatch):
    db,t,v,c=fixtures(tmp_path)
    m.record(db,t,v,c,1031)
    original=dict(db.execute('SELECT * FROM forecasts').fetchone())
    db.close()
    monkeypatch.setattr(m,'MAX_STORE',(tmp_path/'tracking.sqlite').stat().st_size)
    return t,v,c,original


def test_rollover_dedup_settlement_and_restore(tmp_path,monkeypatch):
    t,v,c,original=rollover(tmp_path,monkeypatch)
    db=m.open_store(tmp_path/'tracking.sqlite')
    assert db.segment_count==2
    assert m.record(db,t,v,c,1032)['new_predictions']==0
    assert dict(db.execute('SELECT * FROM main.forecasts').fetchone())==original
    v['forecasts'][0]['expected_return_bps']=4
    r=m.record(db,t,v,c,1033)
    assert r['new_predictions']==1 and r['groups'][0]['observed']==2 and r['news_links']==1
    assert db.execute('SELECT count(*) FROM s1.forecasts').fetchone()[0]==1
    r=m.record(db,t,{'status':'unavailable'},{},1322)
    assert r['groups'][0]['settled']==2
    assert db.execute('SELECT observed FROM main.forecasts').fetchone()[0]==1031
    db.close()
    restored=tmp_path/'relocated';restored.mkdir()
    for p in m.segment_paths(tmp_path/'tracking.sqlite'):shutil.copy2(p,restored/p.name)
    db=m.open_store(restored/'tracking.sqlite',readonly=True)
    assert db.execute('SELECT count(*) FROM all_forecasts').fetchone()[0]==2
    assert db.execute('SELECT count(*) FROM all_news_links').fetchone()[0]==1
    with pytest.raises(sqlite3.OperationalError):db.execute('DELETE FROM main.forecasts')
    db.close();t.close()


def test_inventory_gap_refuses(tmp_path):
    (tmp_path/'tracking.sqlite.part002.sqlite').touch()
    with pytest.raises(ValueError,match='inventory'):m.open_store(tmp_path/'tracking.sqlite')


def test_capacity_refusal_preserves_original(tmp_path,monkeypatch):
    t,v,c,original=rollover(tmp_path,monkeypatch)
    monkeypatch.setattr(m,'MAX_SEGMENTS',1)
    before=(tmp_path/'tracking.sqlite').read_bytes()
    with pytest.raises(ValueError,match='segment_capacity'):m.open_store(tmp_path/'tracking.sqlite')
    assert (tmp_path/'tracking.sqlite').read_bytes()==before
    t.close()


def test_old_pending_missing_target_does_not_disappear(tmp_path,monkeypatch):
    t,v,c,original=rollover(tmp_path,monkeypatch)
    t.execute('DELETE FROM bars WHERE t=1260')
    db=m.open_store(tmp_path/'tracking.sqlite')
    r=m.record(db,t,{'status':'unavailable'},{},1322)
    assert r['groups'][0]['pending']==1
    assert db.execute('SELECT next_check FROM main.forecasts').fetchone()[0]==1622
    db.close();t.close()


def test_empty_successor_after_crash_is_recovered(tmp_path,monkeypatch):
    t,v,c,original=rollover(tmp_path,monkeypatch)
    (tmp_path/'tracking.sqlite.part001.sqlite').touch()
    db=m.open_store(tmp_path/'tracking.sqlite')
    assert db.segment_count==2
    assert m.record(db,t,v,c,1032)['new_predictions']==0
    db.close();t.close()


def test_free_space_refusal(tmp_path,monkeypatch):
    monkeypatch.setattr(m,'MIN_FREE',10**30)
    with pytest.raises(ValueError,match='free_space'):m.open_store(tmp_path/'tracking.sqlite')


def test_missing_last_segment_is_not_silently_replaced(tmp_path,monkeypatch):
    t,v,c,original=rollover(tmp_path,monkeypatch)
    db=m.open_store(tmp_path/'tracking.sqlite');db.close()
    (tmp_path/'tracking.sqlite.part001.sqlite').rename(tmp_path/'preserved.sqlite')
    with pytest.raises(ValueError,match='history_missing'):
        m.open_store(tmp_path/'tracking.sqlite',readonly=True)
    with pytest.raises(ValueError,match='history_missing'):
        m.open_store(tmp_path/'tracking.sqlite')
    t.close()


def test_totals_equal_original_query_after_restart(tmp_path):
    db,t,v,c=fixtures(tmp_path)
    m.record(db,t,v,c,1031)
    m.record(db,t,{'status':'unavailable'},{},1322)
    result=m.summary_groups(db)
    raw=db.execute('SELECT count(*) AS n,sum(error) AS e,avg(direction) AS d FROM forecasts').fetchone()
    assert result[0]['observed']==raw['n']
    assert result[0]['mae_bps']==pytest.approx(raw['e']/raw['n'])
    assert result[0]['direction_fraction']==raw['d']
    db.close()
    db=m.open_store(tmp_path/'tracking.sqlite')
    assert m.summary_groups(db)==result
    db.close();t.close()


def test_management_consumer_finds_earliest_anchor_across_segments(tmp_path,monkeypatch):
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
    import forex_retained_management_readiness as consumer
    t,v,c,original=rollover(tmp_path,monkeypatch)
    db=m.open_store(tmp_path/'tracking.sqlite')
    v['forecasts'][0]['expected_return_bps']=4
    m.record(db,t,v,c,1033)
    anchors=consumer.first_observed_anchors(db,v['registry_sha256'])
    assert len(anchors)==1 and anchors[0]['id']==original['id'] and anchors[0]['observed']==1031
    db.close();t.close()
