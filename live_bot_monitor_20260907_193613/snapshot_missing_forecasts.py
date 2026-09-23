"""One bounded API observation and read-only latest ledger records; no scoring."""
from collections import Counter
from contextlib import closing
from datetime import datetime,timezone
import hashlib,json,sqlite3,time,urllib.request
from pathlib import Path

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
WORK=Path(__file__).resolve().parent
STUDY=ROOT/'data/oanda_training_manager/pair_local_forecast_study_v1'
CONFIG=ROOT/'config/pair_local_forecast_study_v1_20260907.json'
EXPECTED='e0aa74fa5d20b7be983050625773834ea68d74ffc15b566ab5e03df9b3380d65'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def utc(t):return datetime.fromtimestamp(t,timezone.utc).isoformat() if t is not None else None
def encode(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def main():
    started=time.time();raw=CONFIG.read_bytes();assert sha(raw)==EXPECTED;registry=json.loads(raw)
    assert all(sha((ROOT/k).read_bytes())==v for k,v in registry['source_bindings'].items())
    with urllib.request.urlopen('http://127.0.0.1:8765/api/main',timeout=10) as response:
        api_raw=response.read(12*1024*1024+1)
    assert len(api_raw)<=12*1024*1024
    observed=time.time();api=json.loads(api_raw)['pair_local_forecasts']
    assert api['status']=='current' and api['registry_sha256']==EXPECTED
    assert 0<=observed-api['generated_epoch']<=60
    apirows={r['instrument']:r for r in api['rows']}
    pairs={}
    for pair,item in sorted(registry['pairs'].items()):
        path=STUDY/'pairs'/pair/'study.sqlite';deadline=time.monotonic()+2
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=.2)) as db:
            db.row_factory=sqlite3.Row;db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
            db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
            contract=db.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
            assert contract['sha']==item['contract_sha256'] and contract['payload'].encode()==encode(item['contract'])
            counts={table:db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] for table in ('forecasts','publication','attempts','diagnostics','inputs')}
            latest=db.execute('SELECT f.payload,f.sha,f.bucket,p.epoch published_epoch,c.epoch consumed_epoch FROM forecasts f JOIN publication p ON p.id=f.id LEFT JOIN consumption c ON c.id=f.id ORDER BY f.bucket DESC LIMIT 1').fetchone()
            attempt=db.execute('SELECT bucket,epoch,reference_id FROM attempts ORDER BY bucket DESC LIMIT 1').fetchone()
            diag=db.execute('SELECT bucket,epoch,payload FROM diagnostics ORDER BY bucket DESC LIMIT 1').fetchone()
            quote=db.execute('SELECT market,available FROM quotes ORDER BY available DESC LIMIT 1').fetchone()
            cutoff=time.time();db.rollback()
        row={'instrument':pair,'counts':counts,'observed_utc':utc(cutoff),'observed_epoch':cutoff,
             'current_bucket':int(cutoff//900),'current_bucket_start_utc':utc(int(cutoff//900)*900),
             'api_row':apirows.get(pair),'latest_attempt':dict(attempt) if attempt else None,
             'latest_diagnostic':{'bucket':diag['bucket'],'epoch':diag['epoch'],'utc':utc(diag['epoch']),'payload':json.loads(diag['payload'])} if diag else None,
             'latest_observed_quote':dict(quote) if quote else None,'latest_publication':None}
        if latest:
            payload=json.loads(latest['payload']);assert sha(encode(payload))==latest['sha']
            issues=[a['issued_epoch'] for a in payload['forecasts']]
            row['latest_publication']={'decision_id':payload['decision_id'],'bucket':latest['bucket'],
                'forecast_sha256':latest['sha'],'reference_epoch':payload['reference_epoch'],
                'target_epoch':payload['target_epoch'],'target_utc':utc(payload['target_epoch']),
                'issued_epoch_min':min(issues),'issued_epoch_max':max(issues),'issued_utc':utc(max(issues)),
                'published_epoch':latest['published_epoch'],'consumed_epoch':latest['consumed_epoch'],
                'target_passed_at_ledger_read':payload['target_epoch']<=cutoff}
        row['current_bucket_attempt_exists']=bool(attempt and attempt['bucket']==row['current_bucket'])
        row['latest_diagnostic_matches_latest_attempt']=bool(attempt and diag and attempt['bucket']==diag['bucket'])
        row['fresh_ledger_quote_without_current_bucket_attempt']=bool(not row['current_bucket_attempt_exists'] and quote and 0<=cutoff-quote['market']<=60 and 0<=cutoff-quote['available']<=60)
        row['ever_published_now_missing']=bool(counts['publication'] and (row['api_row'] or {}).get('status')!='forecast')
        pairs[pair]=row
    assert sha(CONFIG.read_bytes())==EXPECTED
    assert all(sha((ROOT/k).read_bytes())==v for k,v in registry['source_bindings'].items())
    result={'schema_version':'missing_forecast_ledger_snapshot_v1_20260907','status':'passed','started_utc':utc(started),
        'finished_utc':utc(time.time()),'api_received_utc':utc(observed),'api_main_sha256':sha(api_raw),
        'api_pair_summary':{k:v for k,v in api.items() if k!='rows'},'registry_sha256':EXPECTED,
        'registered_source_bindings':registry['source_bindings'],'helper_sha256':sha(Path(__file__).read_bytes()),
        'pair_api_status_counts':dict(Counter((r['api_row'] or {}).get('status','missing') for r in pairs.values())),
        'ever_published_pairs':[p for p,r in pairs.items() if r['counts']['publication']],
        'ever_published_now_missing':[p for p,r in pairs.items() if r['ever_published_now_missing']],
        'no_current_bucket_attempt':[p for p,r in pairs.items() if not r['current_bucket_attempt_exists']],
        'fresh_ledger_quote_without_current_bucket_attempt':[p for p,r in pairs.items() if r['fresh_ledger_quote_without_current_bucket_attempt']],
        'pairs':pairs,'runtime_mutations':False,'scoring_runs':0,
        'limitations':['API and 68 ledger transactions have different bounded observation times; this is not globally atomic.',
            'A fresh retained quote without a current-bucket attempt is a scheduling candidate, not proof of input readiness or scheduler fault.',
            'A previous publication is historical evidence; its original target may already have passed, so it need not appear as a current forecast.']}
    output=WORK/'MISSING_FORECAST_LEDGER_BASELINE_20260907.json'
    with output.open('x',encoding='utf-8') as handle:json.dump(result,handle,indent=2);handle.write('\n')
    print(json.dumps({k:v for k,v in result.items() if k not in ('pairs','registered_source_bindings','limitations')},indent=2))
if __name__=='__main__':main()
