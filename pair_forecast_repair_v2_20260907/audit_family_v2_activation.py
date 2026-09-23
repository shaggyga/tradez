"""One-shot bounded read-only audit of the activated136family ledgers. No fitting/scoring."""
from collections import Counter
from contextlib import closing
from datetime import datetime,timezone
import hashlib,json,sqlite3,sys,time,zlib
from pathlib import Path

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
WORK=Path(__file__).resolve().parent
STUDY=ROOT/'data/oanda_training_manager/pair_local_forecast_study_v2'
CONFIG=ROOT/'config/pair_local_forecast_study_v2_20260907.json'
REGISTRY_SHA='f7dc675925a765c0bb369282895f2839326476a05eadb7f5df6e0f27a10ed505'
TABLES=('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')
def encoded(v):return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def sha(v):return hashlib.sha256(v).hexdigest()
def utc(v):return datetime.fromtimestamp(v,timezone.utc).isoformat()
def read(path,limit):
    with path.open('rb') as f:raw=f.read(limit+1)
    if len(raw)>limit:raise ValueError('bounded_file_limit')
    return raw
def require(value,message):
    if not value:raise ValueError(message)
def source_check(registry):
    require(sha(read(CONFIG,1024*1024))==REGISTRY_SHA,'registry_hash_mismatch')
    for name,expected in registry['source_bindings'].items():
        require(sha(read(ROOT/name,2*1024*1024))==expected,'registered_source_changed:'+name)
def main():
    started=time.time();raw=read(CONFIG,1024*1024);require(sha(raw)==REGISTRY_SHA,'registry_hash')
    registry=json.loads(raw);source_check(registry);sys.path.insert(0,str(ROOT))
    import oanda_causal_forecast_inputs_pair_v2 as inputs
    from oanda_fixed_forecast_evaluation_pair_v2 import forecast_errors,validate_protocol
    from oanda_causal_forecast_ledger_pair_v2 import validate_contract
    from oanda_exact_price_scoring import decimal_value,quote_midpoint
    rows=[];validated_captures={};family_totals={family:Counter() for family in ('probabilistic_state_space','ridge_return_repaired')}
    for pair,item in sorted(registry['pairs'].items()):
        for family,registered in sorted(item['families'].items()):
            contract=registered['contract'];validate_contract(contract);expected=registered['contract_sha256']
            path=(STUDY/'pairs'/pair/family/'study.sqlite').resolve()
            require(path.is_relative_to(STUDY.resolve()) and path.is_file() and path.stat().st_size<=128*1024*1024,'database_path_or_size')
            deadline=time.monotonic()+3;begun=time.time()
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=.2)) as db:
                db.row_factory=sqlite3.Row;db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
                db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
                saved=db.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
                require(saved['sha']==expected and saved['payload'].encode()==encoded(contract),'contract_binding')
                active=dict(db.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone())
                require(active['contract_sha']==expected,'activation_binding')
                counts={t:db.execute(f'SELECT count(*) FROM {t}').fetchone()[0] for t in TABLES}
                require(counts['forecasts']<=1024 and counts['attempts']<=2048 and counts['inputs']<=2048,'audit_row_bound')
                attempts={r['id']:dict(r) for r in db.execute('SELECT * FROM attempts')}
                publications={r['id']:dict(r) for r in db.execute('SELECT * FROM publication')}
                consumptions={r['id']:dict(r) for r in db.execute('SELECT * FROM consumption')}
                diagnostics=[dict(r) for r in db.execute('SELECT * FROM diagnostics')]
                forecasts=[dict(r) for r in db.execute('SELECT * FROM forecasts ORDER BY bucket')]
                captures=list(db.execute('SELECT id,payload FROM inputs'))
                quotes={r['id']:json.loads(r['payload']) for r in db.execute('SELECT id,payload FROM quotes WHERE id IN (SELECT reference_id FROM attempts)')}
                outcomes=[dict(r) for r in db.execute('SELECT * FROM outcomes')]
                highwater=db.execute('SELECT max(epoch) FROM clocks').fetchone()[0];cutoff=time.time();db.rollback()
            require(0<active['epoch']<=highwater<=cutoff,'actual_clock_order')
            require(len({r['bucket'] for r in forecasts})==len(forecasts) and len({r['reference'] for r in forecasts})==len(forecasts),'duplicate_success_bucket_or_reference')
            capture_ids=set()
            for capture_row in captures:
                identity=capture_row['id'];capture_ids.add(identity)
                if identity not in validated_captures:
                    decompressor=zlib.decompressobj();decoded=decompressor.decompress(capture_row['payload'],24*1024*1024+1)
                    require(len(decoded)<=24*1024*1024 and decompressor.eof and not decompressor.unused_data,'capture_compression_bound')
                    capture=json.loads(decoded);require(sha(encoded(capture))==identity,'input_capture_hash')
                    require(capture['output_instrument']==pair and decimal_value(capture['pip_size'])==decimal_value(item['pip_size']),'capture_pair_pip')
                    if capture.get('status')=='ready':inputs.validate_capture(capture,instrument=pair,pip_size=item['pip_size'])
                    validated_captures[identity]=capture
                else:
                    # An identical hash in another family DB must bind identical bytes.
                    require(zlib.decompress(capture_row['payload'])==encoded(validated_captures[identity]),'shared_capture_content_mismatch')
            for identity,attempt in attempts.items():
                quote=quotes[attempt['reference_id']]
                require(identity==sha(encoded({'contract':expected,'bucket':attempt['bucket'],'sequence':attempt['sequence']})),'attempt_identity')
                require(int(attempt['epoch']//900)==attempt['bucket'] and active['epoch']<quote['available_epoch']<=attempt['epoch'],'attempt_clock')
                require(0<=attempt['epoch']-quote['market_epoch']<=60,'attempt_reference_freshness')
            protocol={**contract['evaluation_protocol'],'historical_start_utc':utc(active['epoch']),'historical_end_utc':utc(cutoff)}
            validate_protocol(protocol);manifest=[];verified=0;due=0
            for record in forecasts:
                payload=json.loads(record['payload']);identity=record['id'];attempt=attempts[record['attempt_id']];quote=quotes[attempt['reference_id']]
                require(sha(encoded(payload))==record['sha'],'forecast_payload_hash')
                require(identity==sha(encoded({'contract':expected,'bucket':record['bucket']})),'forecast_bucket_identity')
                require(payload['attempt_id']==attempt['id'] and payload['attempt_epoch']==attempt['epoch'],'forecast_attempt_binding')
                require(payload['family']==family and payload['instrument']==pair and payload['reference_quote_id']==quote['quote_id'],'forecast_family_reference')
                require(payload['input_capture_sha256'] in capture_ids,'published_capture_missing')
                capture=validated_captures[payload['input_capture_sha256']]
                require(capture['family_readiness'][family]['ready'] is True,'published_family_input_unready')
                require(record['reference']==payload['reference_epoch']==quote['market_epoch'] and record['target']==payload['target_epoch']==quote['market_epoch']+3600,'original_h1_target')
                require(len(payload['forecasts'])==1 and payload['forecasts'][0]['family']==family,'singleton_family_denominator')
                arm=payload['forecasts'][0];require(decimal_value(arm['reference_mid'])==quote_midpoint(quote),'exact_reference_midpoint')
                require(arm['input_source_observed_epoch']==capture['first_observed_epoch'],'original_input_observation')
                pub=publications.get(identity);consumer=consumptions.get(identity)
                if pub:
                    require(pub['forecast_sha']==record['sha'] and arm['issued_epoch']<=pub['epoch']<=cutoff,'publication_hash_clock')
                if consumer:
                    require(pub is not None and consumer['forecast_sha']==record['sha'] and consumer['publication_sha']==sha(encoded({'epoch':pub['epoch'],'forecast_sha':record['sha']})),'consumer_receipt_hash')
                    require(pub['epoch']<=consumer['epoch']<=cutoff,'consumer_clock')
                    require(not forecast_errors({**arm,'committed_available_epoch':consumer['epoch']},payload,protocol),'exported_forecast_clock_validation')
                    verified+=1
                due+=record['target']<=cutoff
                manifest.append({'decision_id':identity,'attempt_id':record['attempt_id'],'bucket':record['bucket'],
                    'forecast_sha256':record['sha'],'input_capture_sha256':payload['input_capture_sha256'],
                    'reference_epoch':record['reference'],'issued_epoch':arm['issued_epoch'],'target_epoch':record['target'],
                    'publication_epoch':pub['epoch'] if pub else None,'consumption_epoch':consumer['epoch'] if consumer else None,
                    'input_observed_epoch':arm['input_source_observed_epoch'],'computation_started_epoch':arm['computation_started_epoch'],
                    'computation_completed_epoch':arm['computation_completed_epoch']})
            for diagnostic in diagnostics:
                require(diagnostic['attempt_id'] in attempts,'diagnostic_attempt')
                payload=json.loads(diagnostic['payload'])
                require(payload.get('input_capture_sha256') is None or payload['input_capture_sha256'] in capture_ids,'diagnostic_capture_missing')
            scored=0;scorepath=path.parent/'scorecard.json';scoremeta={'exists':scorepath.is_file()}
            if scorepath.is_file():
                raw_score=read(scorepath,8*1024*1024);report=json.loads(raw_score)
                require(report['study_contract_sha256']==expected and report['family']==family,'scorecard_identity')
                scored=report['coverage']['scored_decisions'];require(scored==len(report['decisions']) and scored<=due,'scorecard_denominator')
                scoremeta.update(generated_utc=report['generated_utc'],sha256=sha(raw_score),scored_decisions=scored)
            totals=family_totals[family];totals.update(counts);totals.update(verified_consumed_forecasts=verified,original_targets_due=due,stored_scored_decisions=scored)
            rows.append({'instrument':pair,'family':family,'contract_sha256':expected,'activation_epoch':active['epoch'],
                'read_started_utc':utc(begun),'observed_cutoff_utc':utc(cutoff),'counts':counts,'verified_consumed_forecasts':verified,
                'original_targets_due':due,'stored_scored_decisions':scored,'scorecard':scoremeta,'forecasts':manifest})
    require(len(rows)==136,'expected136family_ledgers')
    summary_raw=read(STUDY/'summary.json',1024*1024);summary=json.loads(summary_raw)
    heartbeat=json.loads(read(STUDY/'heartbeat.json',65536));observed=time.time()
    coherent=heartbeat['summary_sha256']==sha(encoded(summary))
    require(summary['registry_sha256']==heartbeat['registry_sha256']==REGISTRY_SHA,'runtime_registry_binding')
    require(summary['payload_sha256']==sha(encoded({k:v for k,v in summary.items() if k!='payload_sha256'})),'summary_payload_seal')
    require(0<=observed-summary['generated_epoch']<=90 and 0<=observed-heartbeat['generated_epoch']<=90,'current_runtime_status')
    prior=json.loads((ROOT.parent/'live_bot_monitor_20260907_193613/PERFORMANCE_BASELINE_20260907.json').read_text())
    old_sets={'pair_v1':prior['registered_source_bindings'],'eurusd_v1':prior['original_eurusd_supplemental_diagnostic']['registered_source_bindings']}
    old_checks={scope:all(sha(read(ROOT/name,2*1024*1024))==expected for name,expected in bindings.items()) for scope,bindings in old_sets.items()}
    require(all(old_checks.values()),'prior_registered_source_changed');source_check(registry)
    output={'schema_version':'read_only_independent136family_activation_audit_v2_20260907','status':'passed',
        'started_utc':utc(started),'finished_utc':utc(time.time()),'registry_sha256':REGISTRY_SHA,
        'registered_source_bindings':registry['source_bindings'],'helper_sha256':sha(Path(__file__).read_bytes()),
        'family_ledger_count':len(rows),'unique_validated_capture_count':len(validated_captures),
        'activation_epoch_range':[min(r['activation_epoch'] for r in rows),max(r['activation_epoch'] for r in rows)],
        'family_totals':{k:dict(v) for k,v in family_totals.items()},'ledgers':rows,
        'runtime_summary':{'generated_epoch':summary['generated_epoch'],'sha256':sha(summary_raw),
            'heartbeat_generated_epoch':heartbeat['generated_epoch'],'heartbeat_binds_this_summary':coherent,
            'status_counts':dict(Counter(slot['status'] for row in summary['rows'] for slot in row['families'].values())),
            'pairs_with_forecast':sum(any(s['status']=='forecast' for s in r['families'].values()) for r in summary['rows']),
            'readiness_rows':[{'instrument':r['instrument'],'families':{f:{'status':s['status'],'reason':s['reason'],
                'current_readiness':s['current_readiness'],'last_attempt':s['last_attempt']} for f,s in r['families'].items()}} for r in summary['rows']],
            'worker_errors':heartbeat['errors'],'orders_enabled':heartbeat['can_place_orders']},
        'old_registered_sources_unchanged':old_checks,'old_registered_source_bindings':old_sets,
        'scoring_runs':0,'model_computations':0,'broker_requests':0,'orders':0,'runtime_writes':False,
        'limitations':['136independent SQLite transactions and separately observed runtime files have individual clocks; no globally atomic claim.',
            'Published predictions whose original H1 targets remain future are not scored outcomes, wins or measured accuracy.',
            'The status snapshot is usable as a coherent producer generation only when heartbeat_binds_this_summary is true.',
            'Hash and internal-clock validation are engineering evidence, not independent proof of future profitability or execution authority.']}
    path=WORK/'PAIR_FAMILY_V2_RUNTIME_AUDIT_20260907.json'
    with path.open('x',encoding='utf-8') as handle:json.dump(output,handle,indent=2);handle.write('\n')
    print(json.dumps({'output':str(path),'sha256':sha(path.read_bytes()),'started_utc':output['started_utc'],'finished_utc':output['finished_utc'],
        'family_totals':output['family_totals'],'unique_validated_captures':len(validated_captures),
        'status_counts':output['runtime_summary']['status_counts'],'pairs_with_forecast':output['runtime_summary']['pairs_with_forecast'],
        'heartbeat_binds_this_summary':coherent,'old_source_checks':old_checks}))
if __name__=='__main__':main()
