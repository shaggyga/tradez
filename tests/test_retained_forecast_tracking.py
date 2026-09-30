import copy
import json
from pathlib import Path
import sqlite3
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'trad'))
import oanda_retained_forecast_tracking_v1 as m


def fixtures(tmp_path):
    technical = sqlite3.connect(':memory:')
    technical.row_factory = sqlite3.Row
    technical.execute('CREATE TABLE bars(pair TEXT,t INTEGER,first_observed REAL,body TEXT)')
    for close, price in ((1320, 1.101), (1380, 1.102), (1920, 1.098)):
        technical.execute('INSERT INTO bars VALUES (?,?,?,?)', ('EUR_USD', close-60, close+1, json.dumps({'close': price})))
    f = dict(instrument='EUR_USD', connection='m5', horizon_minutes=5,
             reference_epoch=1020, issued_epoch=1025, target_epoch=1320,
             reference_mid=1.1, expected_return_bps=3., feature_hash='a'*64,
             input_hash='b'*64, panel_sha256=None, registry_sha256='c'*64, **m.FLAGS)
    view = dict(status='current', generated_epoch=1030, registry_sha256='c'*64,
                payload_sha256='d'*64, connections=[{'id':'m5','horizon_minutes':5}], forecasts=[f])
    context = dict(status='current', generated_epoch=1030, payload_sha256='e'*64, topics=[{
        'available_epoch':1028, 'headline':'Fed guidance', 'interpretation_id':'f'*64,
        'interpretation':{'claims':[{'currency':'USD','direction':None}]}}])
    return m.open_store(tmp_path/'tracking.sqlite'), technical, view, context


def test_prospective_observation_settlement_and_repeated_publication(tmp_path):
    db, t, v, c = fixtures(tmp_path)
    assert m.record(db,t,v,c,1031)['new_predictions'] == 1
    original = db.execute('SELECT body FROM forecasts').fetchone()[0]
    v['generated_epoch']=1040;v['forecasts'][0]['issued_epoch']=1035
    assert m.record(db,t,v,c,1041)['new_predictions'] == 0
    assert db.execute('SELECT body FROM forecasts').fetchone()[0] == original
    result = m.record(db,t,{'status':'unavailable'}, {}, 1322)
    g = result['groups'][0]
    assert g['observed']==g['settled']==1 and g['pending']==0
    assert g['mae_bps']==pytest.approx(abs((1.101/1.1-1)*10000-3))
    assert g['no_change_mae_bps']==pytest.approx(abs((1.101/1.1-1)*10000))
    link=json.loads(db.execute('SELECT body FROM news_links').fetchone()[0])
    assert link['decision_epoch']==1031 and link['available_epoch']==1028
    assert link['forecast_ids']==[db.execute('SELECT id FROM forecasts').fetchone()[0]]
    db.close();t.close()


@pytest.mark.parametrize('field,value', [('issued_epoch',1040),('target_epoch',1321),
    ('reference_mid',0),('expected_return_bps',float('nan')),('can_place_orders',True),
    ('horizon_minutes',True),('registry_sha256','other')])
def test_bad_forecasts_do_not_enter_ledger(tmp_path,field,value):
    db,t,v,c=fixtures(tmp_path);v['forecasts'][0][field]=value
    with pytest.raises(ValueError):m.record(db,t,v,c,1031)
    assert db.execute('SELECT count(*) FROM forecasts').fetchone()[0]==0
    db.close();t.close()


def test_expired_forecast_not_backfilled_and_future_news_not_linked(tmp_path):
    db,t,v,c=fixtures(tmp_path);c['topics'][0]['available_epoch']=1040
    assert m.record(db,t,v,c,1031)['news_links']==0
    with pytest.raises(ValueError):m.record(db,t,v,c,1400)
    assert db.execute('SELECT observed FROM forecasts').fetchone()[0]==1031
    db.close();t.close()


def test_separate_targets_registries_and_changed_prediction_identity(tmp_path):
    db,t,v,c=fixtures(tmp_path)
    f=copy.deepcopy(v['forecasts'][0]);f.update(connection='m15',horizon_minutes=15,target_epoch=1920)
    v['connections'].append({'id':'m15','horizon_minutes':15});v['forecasts'].append(f)
    m.record(db,t,v,c,1031)
    v['forecasts'][0]['expected_return_bps']=4
    m.record(db,t,v,c,1032)
    assert db.execute('SELECT count(*) FROM forecasts').fetchone()[0]==3
    v['registry_sha256']='9'*64
    for f in v['forecasts']:f['registry_sha256']='9'*64
    r=m.record(db,t,v,c,1033)
    assert len(r['groups'])==4 and sum(x['observed'] for x in r['groups'])==5
    db.close();t.close()


def test_missing_targets_retry_without_starving_and_revisions_refuse(tmp_path):
    db,t,v,c=fixtures(tmp_path);m.record(db,t,v,c,1031)
    t.execute('DELETE FROM bars WHERE t=1260')
    r=m.record(db,t,{'status':'unavailable'},{},1322)
    assert r['groups'][0]['pending']==1
    assert db.execute('SELECT next_check FROM forecasts').fetchone()[0]==1622
    t.execute('CREATE TABLE revisions(pair TEXT)');t.execute("INSERT INTO revisions VALUES ('EUR_USD')")
    r=m.record(db,t,{'status':'unavailable'},{},1623)
    assert r['groups'][0]['unavailable']==1 and r['groups'][0]['mae_bps'] is None
    assert json.loads(db.execute('SELECT outcome FROM forecasts').fetchone()[0])['reason']=='original_input_revision'
    db.close();t.close()


def test_summary_drift_and_staleness_withhold_scores(tmp_path):
    db,t,v,c=fixtures(tmp_path);r=m.record(db,t,v,c,1031)
    r['payload_sha256']=m.digest(r);p=tmp_path/'summary.json';p.write_text(m.encoded(r))
    assert m.read_current(p,now=1032)['status']=='current'
    assert m.read_current(p,now=1212)['status']=='unavailable'
    r['groups'][0]['settled']=100;p.write_text(m.encoded(r))
    assert m.read_current(p,now=1032)['status']=='unavailable'
    db.close();t.close()


def test_loaded_source_cannot_relabel_after_edit(tmp_path,monkeypatch):
    db,t,v,c=fixtures(tmp_path)
    monkeypatch.setattr(m,'LOADED_SOURCE_SHA256','0'*64)
    with pytest.raises(ValueError,match='loaded_tracking_source_changed'):
        m.record(db,t,v,c,1031)
    assert db.execute('SELECT count(*) FROM forecasts').fetchone()[0]==0
    db.close();t.close()
