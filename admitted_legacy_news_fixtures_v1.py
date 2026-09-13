"""Real disposable price/history/current-proof captures; never a broker seam."""
import copy
import csv
import datetime as dt
import hashlib
import gzip
import json
import math
from pathlib import Path
import sqlite3

import pytest
import oanda_causal_forecast_inputs_joint_news_v1 as inputs
import oanda_news_causal_aggregation_guard_v1 as guard
import oanda_source_governance as governance

NOW=1788811201.0
OLD_VERSION='local_fx_news_rules_20260904_v164_conflict_duration_recap_guard'

def member(epoch, index, *, version=OLD_VERSION, score=None, observed=None, verified=False):
    value=math.sin(index*.71)*.8 if score is None else score
    return {'event_id':f'joint_fixture_{index}', 'headline':f'Central bank economic bulletin edition {index} reports current activity',
        'source_id':f'publisher_{index}', 'source_name':f'Publisher {index}', 'publisher_url':f'https://publisher{index}.example',
        'source_url':f'https://publisher{index}.example/report', 'source_verified':verified, 'source_direct':verified,
        'source_quality':.8, 'directional_confidence':.6, 'published_utc':inputs._iso(epoch-10),
        'first_seen_utc':inputs._iso(epoch), 'causal_known_utc':inputs._iso(epoch),
        'observed_available_utc':inputs._iso(observed) if observed else None,
        'currency_scores':{'EUR':value},'forward_signal_timely':True,'forward_timeliness_limit_minutes':30,
        'estimated_reaction_horizon_minutes':60,'reports_prior_market_move':False,'context_only':False,
        'classification_version':version,'category':'economic_activity','scope':'currencies','direct_currencies':['EUR'],
        'inferred_currencies':[],'semantic_claims':[], 'observation_clock_trusted':True}

def make_sources(tmp_path, *, instrument='EUR_USD', pip_size=.0001, now=NOW, count=2100, current_verified=False):
    root=Path(tmp_path);candles=root/'candles';candles.mkdir(parents=True,exist_ok=True)
    end=int(now//60)*60-60;start=end-(count-1)*60
    with (candles/f'{instrument}_M1.csv').open('w',encoding='utf-8',newline='') as handle:
        writer=csv.writer(handle);writer.writerow(['datetime','instrument','granularity','complete','close'])
        for i in range(count):
            writer.writerow([inputs._iso(start+i*60),instrument,'M1','true',1.15+pip_size*(.004*i+3*math.sin(i/95)+math.sin(i/17))])
    database=root/'state/source_governance_v1.sqlite';database.parent.mkdir(exist_ok=True)
    with sqlite3.connect(database) as con:
        con.executescript('''CREATE TABLE source_events(source_event_id TEXT PRIMARY KEY,raw_payload_sha256 TEXT,payload_json TEXT,effective_from_utc TEXT);
          CREATE TABLE news_fast_lane_mappings_v3(source_event_id TEXT,raw_payload_sha256 TEXT,batch_seq INTEGER);
          CREATE TABLE news_fast_lane_batches_v3(batch_seq INTEGER PRIMARY KEY,contract_id TEXT);
          CREATE TABLE news_fast_lane_visibility_v3(batch_seq INTEGER PRIMARY KEY,mapping_visible_utc TEXT,availability_basis TEXT,consumer_first_observation_required INTEGER);''')
        for i,epoch in enumerate(range((start//900)*900, end,900)):
            raw=member(epoch+1,i);payload=json.dumps({'raw_payload':raw},sort_keys=True)
            sha=governance.source_event_substantive_sha256({'payload_json':payload})
            con.execute('INSERT INTO source_events VALUES(?,?,?,?)',(raw['event_id'],sha,payload,inputs._iso(epoch+1)))
            con.execute('INSERT INTO news_fast_lane_mappings_v3 VALUES(?,?,?)',(raw['event_id'],sha,i+1))
            con.execute('INSERT INTO news_fast_lane_batches_v3 VALUES(?,?)',(i+1,'news_source_governance_fast_lane_v3_committed_visibility_20260905'))
            con.execute('INSERT INTO news_fast_lane_visibility_v3 VALUES(?,?,?,?)',(i+1,inputs._iso(epoch+2),'post_commit_independent_mapping_read',1))
    raw=member(now-80,9000,version=guard.CLASSIFICATION_VERSION,score=.4,verified=current_verified)
    original={'topic_id':'joint_current_fixture','published_utc':raw['published_utc'],'first_seen_utc':raw['first_seen_utc'],
        'causal_known_utc':raw['causal_known_utc'],'post_window_minutes':60,'headline':raw['headline']}
    asof=dt.datetime.fromtimestamp(now-5,dt.timezone.utc)
    topic=guard.guard_topic(original,[raw],as_of=asof)
    snapshot=guard.build_current_news_snapshot([topic],as_of=asof)
    snapshot['generated_utc']=inputs._iso(now-1)
    snapshot['source_bindings']={name:inputs._bindings()[name] for name in inputs.CURRENT_SOURCE_FILES}
    current=root/'local_news_sentiment/joint_news_current_v1.json';current.parent.mkdir(exist_ok=True)
    current.write_text(json.dumps(snapshot),encoding='utf-8')
    from fixture_admitted_history_v2 import prepare_fixture_admissions
    prepare_fixture_admissions(database)
    return candles,current,database

def make_joint_capture(tmp_path, *, instrument='EUR_USD', pip_size=.0001, now=NOW):
    candles,_,_=make_sources(tmp_path,instrument=instrument,pip_size=pip_size,now=now)
    descriptor=inputs.capture_news_inputs(Path(tmp_path),clock=lambda:now)
    capture=inputs.capture_inputs(candles,instrument,pip_size=pip_size,clock=lambda:now,news_capture=descriptor)
    assert capture['status']=='ready',capture['reasons']
    assert capture['available_families']==[inputs.FAMILY],capture['family_readiness']
    return capture,descriptor,now

def reseal(cap):
    cap['source_capture_sha256']=inputs._digest({k:v for k,v in cap.items() if k!='source_capture_sha256'});return cap

