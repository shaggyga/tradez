"""Read-only verification of actual published forecasts and independent clocks."""
import hashlib,json,sqlite3,sys,time,zlib
from pathlib import Path
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
sys.path.insert(0,str(ROOT))
from oanda_causal_forecast_study_eurusd_v1 import load_contract
from oanda_causal_forecast_ledger_eurusd_v1 import digest
contract=load_contract(ROOT/'config/causal_forecast_study_eurusd_v1_20260907.json')
database=ROOT/'data/oanda_training_manager/causal_forecast_study_eurusd_v1/study.sqlite'
with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True) as db:
    db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
    activation=dict(db.execute('SELECT * FROM activation').fetchone())
    counts={t:db.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in
        ('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')}
    checks=[]
    for row in db.execute('SELECT * FROM forecasts ORDER BY bucket LIMIT 20').fetchall():
        payload=json.loads(row['payload']);assert digest(payload)==row['sha']
        capture_raw=zlib.decompress(db.execute('SELECT payload FROM inputs WHERE id=?',(payload['input_capture_sha256'],)).fetchone()[0])
        capture=json.loads(capture_raw);assert digest(capture)==payload['input_capture_sha256']
        assert set(capture['series'])=={'EUR_USD'}
        publication=dict(db.execute('SELECT * FROM publication WHERE id=?',(row['id'],)).fetchone())
        consumption=dict(db.execute('SELECT * FROM consumption WHERE id=?',(row['id'],)).fetchone())
        assert publication['forecast_sha']==consumption['forecast_sha']==row['sha']
        assert len(payload['forecasts'])==2
        assert {f['family'] for f in payload['forecasts']}==set(contract['cohorts'])
        assert payload['target_epoch']==payload['reference_epoch']+3600
        for forecast in payload['forecasts']:
            assert capture['first_observed_epoch']<=forecast['features_available_epoch']<forecast['issued_epoch']<=publication['epoch']<=consumption['epoch']
            assert forecast['feature_cutoff_epoch']<=capture['first_observed_epoch']
            assert forecast['training_label_maturity_max_epoch']<=forecast['training_labels_available_max_epoch']<forecast['issued_epoch']
            assert not forecast['can_place_orders'] and not forecast['account_eligible'] and not forecast['proof_eligible']
            maturity=forecast['diagnostics'].get('training_label_maturity_max_epoch')
            assert maturity is None or maturity<=forecast['training_label_maturity_max_epoch']
        entry=db.execute('SELECT q.payload FROM entries e JOIN quotes q ON q.id=e.quote_id WHERE e.id=?',(row['id'],)).fetchone()
        entry=json.loads(entry[0]) if entry else None
        if entry:
            assert entry['market_epoch']>consumption['epoch'] and entry['available_epoch']>consumption['epoch']
            assert entry['available_epoch']<payload['target_epoch']
        checks.append({'decision_id':row['id'],'forecasts':[{k:f[k] for k in
            ('family','side','probability_up','predicted_return_bps','issued_epoch','reference_epoch','reference_mid','target_epoch','feature_cutoff_epoch','features_available_epoch','training_label_maturity_max_epoch','training_labels_available_max_epoch')} for f in payload['forecasts']],
            'published_epoch':publication['epoch'],'consumed_epoch':consumption['epoch'],
            'source_capture_sha256':capture['source_capture_sha256'],'retained_eurusd_bars':capture['current_common_bars'],
            'evaluation_entry_observed':bool(entry),'evaluation_entry_market_epoch':entry['market_epoch'] if entry else None,
            'evaluation_entry_available_epoch':entry['available_epoch'] if entry else None})
    db.rollback()
assert checks,'No actually published forecast set yet'
report={'observed_epoch':time.time(),'contract_sha256':digest(contract),'counts':counts,'checked_sets':checks,
    'actual_publication_and_clock_checks':'passed','broker_order_calls':0,'account_trading_enabled':False,
    'accuracy_improvement_demonstrated':False,'outcome_interpretation':'Unfinished original-H1 forecasts remain unscored.'}
destination=OUT/('LIVE_COMPANION_VERIFICATION_'+str(int(time.time()))+'.json')
with destination.open('x',encoding='utf8') as f:json.dump(report,f,indent=2)
print(json.dumps({'receipt':destination.name,'counts':counts,'forecasts':checks[0]['forecasts'],'entry':checks[0]['evaluation_entry_observed']}))
