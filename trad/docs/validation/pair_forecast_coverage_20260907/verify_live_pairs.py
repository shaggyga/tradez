"""Read-only, consistent per-ledger checks of actually issued pair forecasts."""
import hashlib,json,sqlite3,sys,time,zlib
from pathlib import Path
from urllib.request import urlopen
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
sys.path.insert(0,str(ROOT))
from oanda_pair_local_forecast_study_v1 import load_registry,digest
from oanda_causal_forecast_inputs_pair_v1 import validate_capture
registry=load_registry(ROOT/'config/pair_local_forecast_study_v1_20260907.json')
rows=[]
for pair,item in registry['pairs'].items():
    path=ROOT/'data/oanda_training_manager/pair_local_forecast_study_v1/pairs'/pair/'study.sqlite'
    with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        active=dict(db.execute('SELECT * FROM activation').fetchone())
        assert active['contract_sha']==item['contract_sha256']
        counts={t:db.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in
            ('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')}
        checks=[]
        for row in db.execute('SELECT * FROM forecasts ORDER BY bucket').fetchall():
            payload=json.loads(row['payload']);assert digest(payload)==row['sha']
            capture=json.loads(zlib.decompress(db.execute('SELECT payload FROM inputs WHERE id=?',(payload['input_capture_sha256'],)).fetchone()[0]))
            assert digest(capture)==payload['input_capture_sha256'];validate_capture(capture,instrument=pair,pip_size=item['pip_size'])
            pub=dict(db.execute('SELECT * FROM publication WHERE id=?',(row['id'],)).fetchone())
            consumed=dict(db.execute('SELECT * FROM consumption WHERE id=?',(row['id'],)).fetchone())
            assert pub['forecast_sha']==consumed['forecast_sha']==row['sha']
            assert payload['target_epoch']==payload['reference_epoch']+3600
            assert len(payload['forecasts'])==2
            assert {arm['family'] for arm in payload['forecasts']}==set(item['contract']['cohorts'])
            for arm in payload['forecasts']:
                assert arm['instrument']==pair and arm['cohort_id']==item['contract']['cohorts'][arm['family']]
                assert active['epoch']<capture['first_observed_epoch']<=arm['features_available_epoch']<arm['issued_epoch']<=pub['epoch']<=consumed['epoch']
                assert arm['feature_cutoff_epoch']<=capture['first_observed_epoch']
                assert arm['training_label_maturity_max_epoch']<=arm['training_labels_available_max_epoch']<arm['issued_epoch']
                assert arm['can_place_orders'] is False and arm['proof_eligible'] is False and arm['account_eligible'] is False
            entry=db.execute('SELECT q.payload FROM entries e JOIN quotes q ON q.id=e.quote_id WHERE e.id=?',(row['id'],)).fetchone()
            entry=json.loads(entry[0]) if entry else None
            if entry:
                assert entry['market_epoch']>consumed['epoch'] and entry['available_epoch']>consumed['epoch']
                assert entry['available_epoch']<payload['target_epoch']
            checks.append({'decision_id':row['id'],'forecast_sha256':row['sha'],
                'published_epoch':pub['epoch'],'consumed_epoch':consumed['epoch'],
                'target_epoch':payload['target_epoch'],'input_capture_sha256':payload['input_capture_sha256'],
                'current_own_bars':capture['current_common_bars'],'evaluation_entry_observed':bool(entry),
                'forecasts':[{k:f[k] for k in ('family','side','probability_up','predicted_return_bps','issued_epoch','reference_epoch','reference_mid','target_epoch')} for f in payload['forecasts']]})
        diagnostic=db.execute('SELECT epoch,payload FROM diagnostics ORDER BY bucket DESC LIMIT 1').fetchone()
        rows.append({'instrument':pair,'pip_size':item['pip_size'],'activated_epoch':active['epoch'],'counts':counts,
            'checked_publications':checks,'latest_diagnostic':dict(diagnostic) if diagnostic else None})
        db.rollback()
with urlopen('http://127.0.0.1:8765/api/main',timeout=30) as response:api=json.load(response)
pair_api=api['pair_local_forecasts'];assert pair_api['status']=='current',pair_api
assert len(pair_api['rows'])==68
assert pair_api['counts'].get('forecast',0)>1,pair_api['counts']
preserved={}
for name in ('causal_forecast_study_v1_io_r2_20260906.json','causal_forecast_study_gap_v2_20260907.json','causal_forecast_study_eurusd_v1_20260907.json'):
    contract=json.loads((ROOT/'config'/name).read_bytes())
    for source,expected in contract['source_bindings'].items():assert hashlib.sha256((ROOT/source).read_bytes()).hexdigest()==expected,source
    preserved[name]={'contract_sha256':digest(contract),'source_bindings_unchanged':True}
assert hashlib.sha256((ROOT/'FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json').read_bytes()).hexdigest()=='3671297affd5addd4c2e4080c66d4d74382c24678a3430d7e4694814e5993792'
report={'schema_version':'pair_live_publication_verification_v1_20260907','observed_epoch':time.time(),
    'registry_sha256':digest(registry),'pairs':rows,'dashboard_pair_local_forecasts':pair_api,
    'published_pairs':sum(bool(row['checked_publications']) for row in rows),
    'published_sets':sum(row['counts']['forecasts'] for row in rows),
    'outcomes':sum(row['counts']['outcomes'] for row in rows),'preserved_studies':preserved,
    'original_h1_publication_input_and_entry_checks':'passed','can_place_orders':False,'prediction_improvement_demonstrated':False}
destination=OUT/('LIVE_PAIR_VERIFICATION_'+str(int(time.time()))+'.json')
with destination.open('x',encoding='utf8') as handle:json.dump(report,handle,indent=2)
print(json.dumps({'receipt':destination.name,'published_pairs':report['published_pairs'],
    'published_sets':report['published_sets'],'dashboard_counts':pair_api['counts'],'outcomes':report['outcomes']}))
