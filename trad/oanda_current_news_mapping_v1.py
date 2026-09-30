"""Prospective current-root news/technical/forecast observation ledger.

Observe first, then join. No reconstructed forecast, refit or retrospective
backdating; descriptive event outcomes and original H1 forecast scoring differ.
"""
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
import oanda_news_technical_timing_v1 as timing
import oanda_operational_dashboard_selection_v1 as selection

SCHEMA='current_news_forecast_mapping_v1_20260930'
MAX_STORE=256*1024**2
def encoded(v):return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False)
def digest(v):return hashlib.sha256(encoded(v).encode()).hexdigest()

def open_store(path):
    path=Path(path)
    if sum(p.stat().st_size for p in path.parent.glob(path.name+'*'))>MAX_STORE:raise ValueError('mapping_store_capacity')
    db=sqlite3.connect(path,timeout=2)
    db.executescript('''CREATE TABLE IF NOT EXISTS forecasts (id TEXT PRIMARY KEY,pair TEXT,target REAL,observed REAL,body TEXT,outcome TEXT);
        CREATE TABLE IF NOT EXISTS mappings (id TEXT PRIMARY KEY,pair TEXT,decision REAL,body TEXT,outcome TEXT);
        CREATE TABLE IF NOT EXISTS features (id TEXT PRIMARY KEY,body TEXT);
        CREATE INDEX IF NOT EXISTS forecast_pair ON forecasts(pair,observed);
        CREATE INDEX IF NOT EXISTS mapping_pending ON mappings(decision);''')
    return db

def forecast_outcome(technical,pair,forecast,now):
    target=forecast['target_epoch'];ref=forecast['reference_mid']
    if now<target:return {'status':'pending_target'}
    if timing.revised(technical,pair):return {'status':'unavailable','reason':'original_input_revision'}
    # Quote-origin forecasts have fractional-second targets. Use the first
    # completed M1 close at/after target, explicitly preserving the delay.
    close=math.ceil(target/60)*60
    if now<close:return {'status':'pending_target_bar'}
    row=technical.execute('SELECT body,first_observed FROM bars WHERE pair=? AND t=?',(pair,close-60)).fetchone()
    if row is None:return {'status':'pending_target_bar'}
    if not close<=row['first_observed']<=now:return {'status':'unavailable','reason':'target_observation_clock'}
    body=timing.decode(row['body']);realized=(body['close']/ref-1)*10000
    return {'status':'settled','realized_return_bps':realized,
            'predicted_return_bps':forecast['predicted_return_bps'],
            'absolute_error_bps':abs(realized-forecast['predicted_return_bps']),
            'no_change_absolute_error_bps':abs(realized),
            'direction_correct':None if realized==0 or forecast['predicted_return_bps']==0 else (realized>0)==(forecast['predicted_return_bps']>0),
            'target_epoch':target,'target_bar_first_observed':row['first_observed'],
            'outcome_bar_close_epoch':close,'target_close_delay_seconds':close-target,
            'settlement_observed_epoch':now,'target_bar_sha256':digest(body),
            'scope':'original_forecast_reference_to_first_M1_close_at_or_after_H1_target; delay<60sec; not original producer settlement, fills or trading_PnL'}

def record(db,technical,context,view,decision):
    if context.get('status')!='current' or not 0<=decision-context['generated_epoch']<=60:raise ValueError('current_context_required')
    if view.get('status') not in ('current','partial_unavailable'):raise ValueError('qualified_current_selection_required')
    pairs=set();coverage={r['instrument']:r for r in view.get('pair_coverage',{}).get('rows',[])}
    for kind in ('price','joint'):
        producer=view.get(kind,{})
        if producer.get('status')!='current':continue
        for row in producer['rows']:
            pair=row['instrument'];pairs.add(pair)
            for f in row['active_forecasts']:
                if not f['publication_epoch']<=f['consumption_epoch']<=decision<f['target_epoch']:continue
                body={**f,'producer_kind':kind,'registry_sha256':producer['registry_sha256'],
                      'selection_sha256':view['selection_sha256'],'first_monitor_observed_epoch':decision}
                db.execute('INSERT OR IGNORE INTO forecasts VALUES (?,?,?,?,?,NULL)',
                           (f['forecast_sha256'],pair,f['target_epoch'],decision,encoded(body)))
    # Only explicit text currencies create pair links; macro context without an
    # explicit currency stays unassigned, never forced onto every pair.
    inserted=0;feature_cache={}
    for topic in sorted(context.get('topics',[]),key=lambda t:t['available_epoch']):
        if topic['available_epoch']>decision:continue
        currencies={c.get('currency') for c in topic['interpretation']['claims'] if c.get('currency')}
        for pair in sorted(pairs):
            if not currencies.intersection(pair.split('_')):continue
            key=digest([SCHEMA,topic['interpretation_id'],pair])
            if db.execute('SELECT 1 FROM mappings WHERE id=?',(key,)).fetchone():continue
            if inserted>=256:break
            if pair not in feature_cache:
                qualified=view.get('pair_coverage',{}).get('status')=='current' and coverage.get(pair,{}).get('feature_status') in ('complete','partial')
                feature_cache[pair]=timing.select_asof(technical,pair,decision) if qualified else {'status':'unavailable','reason':'technical_source_not_current'}
            features=feature_cache[pair]
            if features.get('status')=='available':
                fid=digest(features);db.execute('INSERT OR IGNORE INTO features VALUES (?,?)',(fid,encoded(features)))
                feature_ref={k:v for k,v in features.items() if k!='features'};feature_ref['snapshot_sha256']=fid
            else:feature_ref=features
            fs=[{'forecast_sha256':fid,'first_monitor_observed_epoch':obs} for fid,obs,raw in db.execute(
                'SELECT id,observed,body FROM forecasts WHERE pair=? AND observed<=? AND target>? ORDER BY observed DESC LIMIT 16',(pair,decision,decision))
                if json.loads(raw)['publication_epoch']<=decision]
            body={'schema_version':SCHEMA,'instrument':pair,'decision_epoch':decision,'news_available_epoch':topic['available_epoch'],
                  'interpretation_id':topic['interpretation_id'],'headline':topic['headline'],'claims':topic['interpretation']['claims'],
                  'news_publication_utc':topic['published_utc'],'news_first_seen_utc':topic['first_seen_utc'],
                  'news_payload_sha256':context['payload_sha256'],'selection_sha256':view['selection_sha256'],
                  'technical':feature_ref,'forecast_references':fs,'scope':'observed conjunction, not evidence news caused the move or model consumed typed context'}
            db.execute('INSERT INTO mappings VALUES (?,?,?,?,NULL)',(key,pair,decision,encoded(body)));inserted+=1
    db.commit()
    # Limited oldest-first settlement; incomplete paths remain explicit, and
    # unavailable results become terminal only after one extra day of support.
    for fid,pair,target,raw in db.execute('SELECT id,pair,target,body FROM forecasts WHERE outcome IS NULL AND target<=? ORDER BY target LIMIT 256',(decision,)).fetchall():
        result=forecast_outcome(technical,pair,json.loads(raw),decision)
        if result['status']=='settled' or decision-target>86400:
            db.execute('UPDATE forecasts SET outcome=? WHERE id=?',(encoded(result),fid))
    for mid,pair,known in db.execute('SELECT id,pair,decision FROM mappings WHERE outcome IS NULL AND decision+3660<=? ORDER BY decision LIMIT 256',(decision,)).fetchall():
        result=timing.evaluate(technical,pair,known,horizons=(5,15,60))
        available=result.get('horizons',{})
        if (available and all(x['status']=='available' for x in available.values())) or decision-known>86400:
            db.execute('UPDATE mappings SET outcome=? WHERE id=?',(encoded(result),mid))
    db.commit()
    forecasts=[json.loads(r[0]) for r in db.execute('SELECT outcome FROM forecasts WHERE outcome IS NOT NULL ORDER BY target DESC LIMIT 10000')]
    settled=[r for r in forecasts if r['status']=='settled']
    counts={t:db.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in ('forecasts','mappings','features')}
    recent=[{**json.loads(b),'outcome':json.loads(o) if o else {'status':'pending_or_missing_path'}} for b,o in db.execute('SELECT body,outcome FROM mappings ORDER BY decision DESC,id LIMIT 12')]
    return {'schema_version':SCHEMA,'status':'current','generated_epoch':decision,'new_mappings':inserted,'counts':counts,
            'settled_forecasts':len(settled),'forecast_mae_bps':sum(r['absolute_error_bps'] for r in settled)/len(settled) if settled else None,
            'score_window':'latest10000 settled-or-terminal forecast records; unique forecast hashes, overlapping targets remain dependent',
            'no_change_mae_bps':sum(r['no_change_absolute_error_bps'] for r in settled)/len(settled) if settled else None,
            'settled_event_mappings':db.execute('SELECT COUNT(*) FROM mappings WHERE outcome IS NOT NULL').fetchone()[0],
            'recent':recent,'selection_sha256':view['selection_sha256'],
            'producer_coverage':{k:{x:view.get(k,{}).get(x) for x in ('status','forecast_pair_count','state_counts','reason_counts')} for k in ('price','joint')},
            'scope':'new prospective monitor observations; descriptive overlap, not independent model qualification or news incremental value',
            'research_only':True,'can_place_orders':False,'can_promote':False}

def collect(root,output,context,*,clock=time.time):
    root=Path(root);output=Path(output)
    view=selection.read_dashboard_sources(root)
    # The producer reader validates source/config/summary identities before use.
    technical=timing.readonly(root/'data/rolling_technical_dataset_20260915_v3/technical.sqlite')
    deadline=time.monotonic()+8;technical.set_progress_handler(lambda:int(time.monotonic()>deadline),10000)
    db=open_store(output/'mapping.sqlite')
    try:return record(db,technical,context,view,clock())
    finally:db.close();technical.close()
