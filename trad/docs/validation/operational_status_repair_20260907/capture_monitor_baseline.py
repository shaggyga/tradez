"""One bounded, read-only health observation; exclusive-create a review receipt."""
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import urlopen
import json, hashlib, sqlite3, time

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
DATA=ROOT/'data/oanda_training_manager'
def load(path):return json.loads(path.read_text(encoding='utf-8-sig'))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    started=time.time()
    with urlopen('http://127.0.0.1:8765/api/main',timeout=30) as response:
        raw=response.read(24*1024*1024+1)
    assert len(raw)<=24*1024*1024
    api=json.loads(raw)
    registry=load(ROOT/'config/pair_local_forecast_study_v1_20260907.json')
    for name,expected in registry['source_bindings'].items():assert sha(ROOT/name)==expected,name
    pair_root=DATA/'pair_local_forecast_study_v1'
    pairs={}
    for path in sorted((pair_root/'pairs').glob('*/study.sqlite')):
        db=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=.2)
        deadline=time.monotonic()+2
        db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
        try:
            db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
            counts={name:db.execute(f'SELECT count(*) FROM {name}').fetchone()[0]
                    for name in ('attempts','diagnostics','forecasts','publication','consumption','entries','outcomes','exclusions')}
            reasons=dict(db.execute('SELECT reason,count(*) FROM exclusions GROUP BY reason').fetchall())
            next_target=db.execute('SELECT min(f.target) FROM forecasts f LEFT JOIN outcomes o ON o.id=f.id LEFT JOIN exclusions x ON x.id=f.id WHERE o.id IS NULL AND x.id IS NULL').fetchone()[0]
        finally:db.rollback();db.close()
        row={'counts':counts,'exclusion_reasons':reasons,'next_unsettled_target_epoch':next_target}
        score_path=path.parent/'scorecard.json'
        if score_path.exists():
            score=load(score_path)
            row['scorecard']={'sha256':sha(score_path),**{key:score.get(key) for key in
                ('generated_utc','status','coverage','paired_summaries','decision_exclusion_counts','outcome_counts')}}
        pairs[path.parent.name]=row
    assert len(pairs)==68
    coverage=api.get('pair_local_forecasts',{})
    news=api.get('news_sentiment',{})
    receipt={'schema_version':'forex_bot_monitor_baseline_v1','observed_utc':datetime.now(timezone.utc).isoformat(),
        'observation_started_epoch':started,'observation_finished_epoch':time.time(),'api_sha256':hashlib.sha256(raw).hexdigest(),
        'pair_coverage':{key:coverage.get(key) for key in ('status','reason','generated_epoch','counts','registry_sha256')},
        'pair_worker':load(pair_root/'heartbeat.json'),
        'eurusd_companion_worker':load(DATA/'causal_forecast_study_eurusd_v1/heartbeat.json'),
        'archive_worker':load(DATA/'state/all68_m1_forward_update_heartbeat_v1.json'),
        'collection_status':api.get('collection_status'),
        'news':{key:news.get(key) for key in ('status','generated_utc','as_of_utc','pair_bias_status','pair_bias_reason','active_topic_count','screened_article_count','evidence_age_sec')},
        'eurusd_news':news.get('pair_bias',{}).get('EUR_USD'),
        'supplemental_diagnostics':{key:value for key,value in api.items() if 'diagnostic' in key and isinstance(value,dict) and value.get('supplemental') is True},
        'pairs':pairs,'totals':{key:sum(row['counts'][key] for row in pairs.values()) for key in next(iter(pairs.values()))['counts']},
        'scorecard_pairs':sum('scorecard' in row for row in pairs.values()),
        'automation':{'id':'check-forex-bot-health','status':'ACTIVE','interval_minutes':15},
        'research_only':True,'can_place_orders':False,
        'limitations':['Separate API, heartbeat and ledger snapshots retain their own clocks; this is not a single atomic observation.',
            'A recorded outcome is not necessarily a valid scored decision; use each scorecard coverage and exclusions.',
            'Scorecard missing-target counts include forecasts whose original H1 target has not arrived; do not treat them all as late outcomes.',
            'Overlapping per-pair forecasts are not independent trials; no demonstrated prediction improvement claimed.']}
    destination=OUT/'BOT_MONITOR_BASELINE_20260907.json'
    with destination.open('x',encoding='utf8') as handle:json.dump(receipt,handle,indent=2);handle.write('\n')
    print(json.dumps({'output':str(destination),'totals':receipt['totals'],'scorecard_pairs':receipt['scorecard_pairs'],'pair_coverage':receipt['pair_coverage'],'news':receipt['news']}))

if __name__=='__main__':main()
