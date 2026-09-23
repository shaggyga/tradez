"""One-shot bounded RO preflight audit; does not fit, publish, settle or import rows."""
import datetime as dt
import hashlib,json,math,sqlite3,sys,time,zlib
from contextlib import closing
from pathlib import Path
import numpy as np
sys.dont_write_bytecode=True
ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad');sys.path.insert(0,str(ROOT))
import oanda_causal_forecast_inputs_joint_news_v1 as inputs
import oanda_joint_price_news_models_v1 as model
from oanda_causal_forecast_ledger_joint_news_v1 import validate_contract,digest,encoded
from oanda_fixed_forecast_evaluation_joint_news_v1 import forecast_errors
from oanda_exact_price_scoring import decimal_value,quote_midpoint
WORK=Path(__file__).resolve().parent;STUDY=WORK/'live_preflight'
TABLES=('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')
def sha(raw):return hashlib.sha256(raw).hexdigest()
def require(condition,reason):
    if not condition:raise ValueError(reason)
def utc(v):return dt.datetime.fromtimestamp(v,dt.timezone.utc).isoformat()
def db_hashes(path):return {p.name:{'bytes':p.stat().st_size,'sha256':sha(p.read_bytes())} for p in (path,path.with_name(path.name+'-wal')) if p.is_file()}
def fitted(query,fit):
    q=np.r_[1.,(query-np.asarray(fit['mean']))/np.asarray(fit['scale'])]
    raw=float(q@np.asarray(fit['coefficients']))
    return float(np.clip(raw*fit['expected_return_shrinkage'],-2*fit['sigma_pips'],2*fit['sigma_pips']))
def main():
    started=time.time();result_raw=(STUDY/'result.json').read_bytes();result=json.loads(result_raw)
    require(result['ended_epoch']<=started and result['errors']==0,'completed_error_free_preflight_required')
    raw=(STUDY/'registry.json').read_bytes();registry=json.loads(raw);registry_sha=sha(raw)
    require(registry_sha==result['registry_sha256'],'registry_result_binding')
    require(len(registry['source_bindings'])==16,'source16_required')
    require(set(registry['pairs'])=={'AUD_CAD','EUR_HUF','EUR_JPY','EUR_USD'},'exact_preflight_pairs')
    for name,expected in registry['source_bindings'].items():require(sha((ROOT/name).read_bytes())==expected,'source_changed:'+name)
    records=[];captures={};shared={}
    for pair,item in sorted(registry['pairs'].items()):
        family='ridge_price_news_v1';slot=item['families'][family];contract=slot['contract'];validate_contract(contract)
        require(contract['contract_id'].endswith('.preflight_live_wiring'),'prototype_cohort_required')
        path=STUDY/'pairs'/pair/family/'study.sqlite';require(path.stat().st_size<128*1024*1024,'db_bound')
        wal=path.with_name(path.name+'-wal')
        require(not wal.exists() or wal.stat().st_size==0,'completed_preflight_must_be_checkpointed_for_immutable_read')
        before=db_hashes(path);deadline=time.monotonic()+3
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True,timeout=.2)) as db:
            db.row_factory=sqlite3.Row;db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
            db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
            saved=dict(db.execute('SELECT * FROM contract').fetchone());active=dict(db.execute('SELECT * FROM activation').fetchone())
            counts={t:db.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0] for t in TABLES}
            require(all(n<=1024 for n in counts.values()),'row_bound')
            rows={t:[dict(r) for r in db.execute(f'SELECT * FROM {t}')] for t in TABLES};db.rollback()
        require(before==db_hashes(path),'database_changed_during_audit')
        require(saved['sha']==slot['contract_sha256']==digest(contract) and json.loads(saved['payload'])==contract,'contract_binding')
        require(active['contract_sha']==saved['sha'] and active['epoch']<=result['started_epoch'],'actual_activation_clock')
        require(counts==result['counts'][pair][family],'retained_counts_match_finished_run')
        quotes={r['id']:json.loads(r['payload']) for r in rows['quotes']};attempts={r['id']:r for r in rows['attempts']}
        pubs={r['id']:r for r in rows['publication']};cons={r['id']:r for r in rows['consumption']};entries={r['id']:r for r in rows['entries']}
        for r in rows['inputs']:
            decoder=zlib.decompressobj();decoded=decoder.decompress(r['payload'],24*1024*1024+1)
            require(len(decoded)<=24*1024*1024 and decoder.eof and not decoder.unused_data,'capture_bound')
            capture=json.loads(decoded);require(digest(capture)==r['id'],'capture_hash')
            inputs.validate_capture(capture,instrument=pair,pip_size=item['pip_size']);captures[r['id']]=capture
            descriptor_path=Path(capture['news_capture_path']);news=inputs._load_news(capture)
            shared[capture['news_capture_sha256']]={'path':str(descriptor_path),'file_sha256':sha(descriptor_path.read_bytes()),
                'source_news_evidence_epoch':news['news_evidence_epoch'],'source_generated_epoch':news['news_generated_epoch'],
                'original_consumer_observed_epoch':news['first_observed_epoch'],'history_rows':len(news['history'])}
        manifest=[]
        for record in rows['forecasts']:
            payload=json.loads(record['payload']);arm=payload['forecasts'][0];ident=record['id'];attempt=attempts[record['attempt_id']]
            reference=quotes[attempt['reference_id']];publication=pubs[ident];consumer=cons[ident];entry=entries[ident];entry_quote=quotes[entry['quote_id']]
            capture=captures[payload['input_capture_sha256']]
            require(digest(payload)==record['sha']==publication['forecast_sha']==consumer['forecast_sha'],'publication_hash')
            require(consumer['publication_sha']==digest({'epoch':publication['epoch'],'forecast_sha':record['sha']}),'consumer_hash')
            require(record['reference']==reference['market_epoch']==payload['reference_epoch'] and record['target']==reference['market_epoch']+3600,'original_h1_target')
            require(active['epoch']<capture['first_observed_epoch']<=arm['computation_started_epoch']<=arm['computation_completed_epoch']<arm['issued_epoch']<=publication['epoch']<=consumer['epoch'],'actual_construction_publication_order')
            require(consumer['epoch']<entry_quote['market_epoch']<=entry_quote['available_epoch']<=consumer['epoch']+60 and entry_quote['available_epoch']<record['target'] and entry['epoch']>=entry_quote['available_epoch'],'strictly_later_executable_entry')
            protocol={**contract['evaluation_protocol'],'historical_start_utc':utc(active['epoch'])}
            require(not forecast_errors({**arm,'committed_available_epoch':consumer['epoch']},payload,protocol),'frozen_scorer_clock_validation')
            for key in ('news_capture_sha256','news_evidence_epoch','news_generated_epoch','news_first_observed_epoch','news_available_epoch','news_expires_epoch'):
                require(arm[key]==capture[key],'original_news_binding:'+key)
            require(arm['issued_epoch']<=arm['news_expires_epoch'] and 0<=arm['issued_epoch']-arm['news_evidence_epoch']<=300,'original_expiry_at_actual_issue')
            d=arm['diagnostics'];price_rows,_=inputs.price_inputs._verified_rows(capture['price_capture'],instrument=pair,pip_size=item['pip_size'])
            context=model._context(price_rows,capture['reference_start_epoch'],capture['news_frames'],capture['current_news_features'])
            epochs,sessions,current,training,frames,query_news=context
            require(d['training_row_start_epochs_by_pair'][pair]==training,'exact_original_training_rows')
            technical=model.price._features(price_rows,epochs,sessions,capture['reference_start_epoch'],float(item['pip_size']))
            joint=fitted(model._joint_features(technical,query_news),d['fitted_joint'])
            neutral=fitted(model._joint_features(technical,np.asarray(model.NEUTRAL_NEWS)),d['fitted_joint'])
            matched=fitted(technical,d['matched_price_only_fitted'])
            require(math.isclose(matched,d['matched_price_only_expected_pips'],abs_tol=1e-12,rel_tol=1e-12),'retained_matched_coefficient_replay')
            require(math.isclose(neutral,d['neutral_news_ablation_expected_pips'],abs_tol=1e-12,rel_tol=1e-12),'retained_neutral_coefficient_replay')
            require(math.isclose(joint-neutral,d['news_ablation_difference_pips'],abs_tol=1e-12,rel_tol=1e-12),'retained_news_difference_replay')
            predicted_bps=10000*joint*float(decimal_value(item['pip_size']))/float(quote_midpoint(reference))
            require(math.isclose(predicted_bps,arm['predicted_return_bps'],abs_tol=1e-12,rel_tol=1e-12),'emitted_magnitude_coefficient_replay')
            require(record['target']>started and not rows['outcomes'],'no_due_outcomes_or_accuracy_claim')
            manifest.append({'decision_id':ident,'forecast_sha256':record['sha'],'input_capture_sha256':payload['input_capture_sha256'],
                'reference_epoch':record['reference'],'target_epoch':record['target'],'issue_epoch':arm['issued_epoch'],'publication_epoch':publication['epoch'],
                'consumption_epoch':consumer['epoch'],'entry_market_epoch':entry_quote['market_epoch'],'entry_available_epoch':entry_quote['available_epoch'],
                'news_provenance':{k:arm[k] for k in ('news_capture_sha256','news_evidence_epoch','news_generated_epoch','news_first_observed_epoch','news_available_epoch','news_expires_epoch')},
                'training_rows':len(training),'nonzero_news_context_training_rows':d['nonzero_news_context_training_rows'],'vetted_news_training_rows':d['vetted_news_training_rows'],
                'joint_expected_pips':joint,'matched_price_only_expected_pips':matched,'neutral_news_ablation_expected_pips':neutral,'news_ablation_difference_pips':joint-neutral,
                'current_news_features':d['current_news_features'],'coefficient_replay_tolerance':1e-12})
        records.append({'instrument':pair,'contract_sha256':saved['sha'],'activation_epoch':active['epoch'],'database_files':before,'counts':counts,'forecasts':manifest})
    for name,expected in registry['source_bindings'].items():require(sha((ROOT/name).read_bytes())==expected,'source_changed_during_audit:'+name)
    report={'status':'passed','schema_version':'joint_disposable_preflight_independent_audit_v1_20260907','started_utc':utc(started),'finished_utc':utc(time.time()),
        'registry_sha256':registry_sha,'result_sha256':sha(result_raw),'helper_sha256':sha(Path(__file__).read_bytes()),'source_bindings':registry['source_bindings'],
        'ledgers':records,'shared_news_captures':shared,'validated_price_news_captures':len(captures),'forecasts':sum(len(r['forecasts']) for r in records),
        'outcomes_due':0,'outcomes':0,'model_fits':0,'coefficient_replays':len(records)*3,'scoring_runs':0,'runtime_writes':False,'broker_calls':False,'orders_enabled':False,
        'limitations':['Disposable wiring evidence only; never include these cohorts or rows in canonical prospective performance.',
            'Stored fitted coefficients were replayed without refitting; capture/feature/training availability was independently reconstructed.',
            'Current news context features can be nonzero while vetted directional features are zero. This is descriptive context, not direct verified news direction.',
            'Four successful publications and later quotes demonstrate wiring, not prediction success. H1 outcomes remain future.']}
    path=STUDY/'INDEPENDENT_PREFLIGHT_AUDIT_20260907.json'
    with path.open('x',encoding='utf8') as handle:json.dump(report,handle,indent=2,allow_nan=False)
    print(json.dumps({'status':report['status'],'path':str(path),'sha256':sha(path.read_bytes()),'forecasts':report['forecasts'],'captures':len(captures),'shared_news_captures':len(shared),'outcomes':0}))
if __name__=='__main__':main()
