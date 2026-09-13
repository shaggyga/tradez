"""Bounded read-only original-outcome assessment; no ledger construction or fitting."""
import sys
sys.dont_write_bytecode=True
from pathlib import Path
import importlib.util,json,sqlite3,time,gzip,hashlib
from contextlib import closing
from decimal import Context,ROUND_HALF_EVEN,Decimal,localcontext
from collections import Counter,defaultdict
from concurrent.futures import ProcessPoolExecutor

HERE=Path(__file__).resolve().parent
ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
PRIOR=ROOT.parent/'revamp_baseline_20260908/performance/evaluate_frozen_baseline.py'
spec=importlib.util.spec_from_file_location('original_baseline_helpers',PRIOR)
h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h)
REGISTRY=ROOT/'config/joint_price_news_study_v3_20260908.json'
REGISTRY_SHA='ee075e69e80ca56dfdf45abe1f7a1a301612176e67f7622eab1adf2bd9af8771'
STUDY='joint_price_news_study_v3';FAMILY='ridge_price_news_v1'
TABLES=('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')

def retain(path,obj):
    raw=h.encoded(h.joint.jsonable(obj));path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('xb') as f:
        with gzip.GzipFile(filename='',mode='wb',fileobj=f,mtime=0,compresslevel=1) as z:z.write(raw)
    return {'path':str(path),'sha256':h.sha(path),'logical_payload_sha256':hashlib.sha256(raw).hexdigest(),'bytes':path.stat().st_size}

def assess(item):
    pair,registered=item;contract=registered['contract'];expected=registered['contract_sha256']
    path=ROOT/'data/oanda_training_manager'/STUDY/'pairs'/pair/FAMILY/'study.sqlite'
    start=time.time();deadline=time.monotonic()+20
    assert path.is_file() and path.stat().st_size<=512*1024*1024
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        db.set_progress_handler(lambda:int(time.monotonic()>deadline),10000)
        saved=db.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
        assert saved['sha']==expected==h.digest(contract) and json.loads(saved['payload'])==contract
        active=db.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
        assert active['contract_sha']==expected and 0<active['epoch']<=start
        counts={t:db.execute('SELECT count(*) FROM '+t).fetchone()[0] for t in TABLES}
        assert counts['quotes']<=100000 and counts['forecasts']<=256 and counts['diagnostics']<=4096
        for table in ('quotes','forecasts'):
            assert db.execute('SELECT coalesce(sum(length(payload)),0) FROM '+table).fetchone()[0]<=128*1024*1024
        forecasts=[dict(r) for r in db.execute('''SELECT f.*,c.epoch consumed,c.forecast_sha consumed_sha,c.publication_sha,
            p.epoch published,p.forecast_sha published_sha FROM forecasts f LEFT JOIN consumption c ON c.id=f.id
            LEFT JOIN publication p ON p.id=f.id ORDER BY f.bucket''')]
        quotes=[json.loads(r[0]) for r in db.execute('SELECT payload FROM quotes ORDER BY available,market,id')]
        entries={r['id']:dict(r) for r in db.execute('SELECT * FROM entries')}
        outcomes={r['id']:dict(r) for r in db.execute('SELECT * FROM outcomes')}
        exclusions=[dict(r) for r in db.execute('SELECT * FROM exclusions ORDER BY epoch,id')]
        diagnostics=[dict(r) for r in db.execute('SELECT attempt_id,bucket,epoch,payload FROM diagnostics ORDER BY epoch')]
        highwater=db.execute('SELECT max(epoch) FROM clocks').fetchone()[0]
        cutoff=time.time();assert cutoff>=highwater
        db.rollback()
    decisions=[]
    for r in forecasts:
        value=json.loads(r['payload']);assert h.digest(value)==r['sha'] and value['decision_id']==r['id']
        assert value['reference_epoch']==r['reference'] and value['target_epoch']==r['target']==r['reference']+3600
        assert value['instrument']==pair and len(value['forecasts'])==1
        arm=value['forecasts'][0];assert arm['family']==FAMILY
        if r['published'] is not None:assert r['published_sha']==r['sha'] and active['epoch']<=arm['issued_epoch']<=r['published']<=cutoff
        if r['consumed'] is not None:
            assert r['consumed_sha']==r['sha'] and r['published']<=r['consumed']<=cutoff
            assert r['publication_sha']==h.digest({'epoch':r['published'],'forecast_sha':r['sha']})
        arm['committed_available_epoch']=r['consumed'];decisions.append(value)
    protocol=dict(contract['evaluation_protocol'])
    protocol.update(historical_start_utc=h.utc(active['epoch']),historical_end_utc=h.utc(cutoff))
    full={'schema_version':h.joint.SCHEMA,'observed_cutoff_epoch':cutoff,'decisions':decisions,'quotes':quotes,'collection_counts':counts}
    selected=[d for d in decisions if d['decision_id'] in outcomes]
    assert set(outcomes)<={d['decision_id'] for d in decisions}
    for d in selected:
        identity=d['decision_id'];assert identity in entries and d['target_epoch']<=cutoff
        assert entries[identity]['epoch']<=outcomes[identity]['epoch']<=cutoff
        assert not any(e['id']==identity for e in exclusions)
    evidence=retain(HERE/'inputs'/f'{pair}.json.gz',{'contract':contract,'activation':dict(active),'dataset':full,
        'entries':entries,'outcomes':outcomes,'exclusions':exclusions,'forecast_receipts':forecasts,'diagnostics':diagnostics,
        'transaction_started_epoch':start,'snapshot_observed_epoch':cutoff})
    report=h.joint.evaluate({**full,'decisions':selected},protocol)
    scored={r['decision_id']:r for r in report['decisions']}
    errors=[]
    for identity,r in scored.items():
        if r['entry']['quote_id']!=entries[identity]['quote_id'] or r['target']['quote_id']!=outcomes[identity]['quote_id']:
            errors.append({'decision_id':identity,'reason':'original_endpoint_mismatch'})
    missing=sorted(set(outcomes)-set(scored))
    card_summary={'status':'absent'};cardpath=path.parent/'scorecard.json'
    if cardpath.is_file():
        assert cardpath.stat().st_size<=16*1024*1024
        raw=cardpath.read_bytes();card=json.loads(raw)
        assert card['instrument']==pair and card['family']==FAMILY and card['study_contract_sha256']==expected
        cardrows={r['decision_id']:r for r in card.get('decisions',[])};mismatches=[]
        for identity in set(cardrows)&set(scored):
            actual=h.joint.jsonable(scored[identity])
            for field in ('reference_quote','entry','target','scores','ablation_comparison'):
                if cardrows[identity].get(field)!=actual.get(field):mismatches.append({'decision_id':identity,'field':field})
        card_summary={'status':'separately_observed','observed_epoch':time.time(),'sha256':hashlib.sha256(raw).hexdigest(),
            'generated_utc':card['generated_utc'],'stored_scored_rows':len(cardrows),'stored_only_ids':sorted(set(cardrows)-set(scored)),
            'completed_not_in_card':sorted(set(scored)-set(cardrows)),'overlap_mismatches':mismatches}
        errors.extend(mismatches)
    parsed_diag=[{**d,'payload':json.loads(d['payload'])} for d in diagnostics]
    result={'status':'passed' if not errors and not missing else 'requires_review','instrument':pair,'family':FAMILY,'study':STUDY,
        'database_path':str(path),'contract_sha256':expected,'activation_epoch':active['epoch'],'transaction_started_epoch':start,
        'snapshot_observed_epoch':cutoff,'snapshot_observed_utc':h.utc(cutoff),'counts':counts,'evidence':evidence,
        'full_input_sha256':h.digest(full),'completed_input_sha256':report['input_sha256'],'protocol_sha256':report['protocol_sha256'],
        'original_completed_not_scored':missing,'verification_errors':errors,'exclusions':exclusions,'diagnostics':parsed_diag,
        'unexpired_published':sum(r['consumed'] is not None and r['target']>cutoff for r in forecasts),
        'due_without_outcome_or_exclusion':[r['id'] for r in forecasts if r['target']<=cutoff and r['id'] not in outcomes and not any(e['id']==r['id'] for e in exclusions)],
        'scored_rows':[h.compact_row(r,{'study':STUDY,'instrument':pair,'family':FAMILY}) for r in report['decisions']],
        'evaluator_coverage':report['coverage'],'evaluator_exclusions':report['exclusions'],'quote_exclusions':report['quote_exclusions'],
        'stored_scorecard':card_summary,'elapsed_sec':time.time()-start}
    return result

def main():
    started=time.time();assert h.sha(REGISTRY)==REGISTRY_SHA
    registry=json.loads(REGISTRY.read_bytes());sources=registry['source_bindings']
    assert len(sources)==20 and all(h.sha(ROOT/n)==v for n,v in sources.items())
    assert len(registry['pairs'])==68
    items=[(p,v['families'][FAMILY]) for p,v in sorted(registry['pairs'].items())]
    results=[]
    with ProcessPoolExecutor(max_workers=4) as pool:
        for result in pool.map(assess,items,chunksize=1):
            results.append(result);h.write_new(HERE/'results'/f"{result['instrument']}.json",result)
            if len(results)%8==0 or len(results)==68:
                print(json.dumps({'ledgers':len(results),'completed':sum(len(r['scored_rows']) for r in results),'elapsed_sec':time.time()-started}),flush=True)
    assert h.sha(REGISTRY)==REGISTRY_SHA and all(h.sha(ROOT/n)==v for n,v in sources.items())
    rows=[r for x in results if x['status']=='passed' for r in x['scored_rows']]
    groups=defaultdict(list)
    for r in rows:
        for name,score in {**r['scores'],**r['ablation_comparison']['scores']}.items():groups[name].append(score)
    with localcontext(Context(prec=80,rounding=ROUND_HALF_EVEN)):
        metrics={name:h.summarize(scores) for name,scores in groups.items()}
        for name,scores in groups.items():
            metrics[name]['stress_mean_net_bps_per_valid_decision']={k:h.mean(s['stress_net_bps'][k] for s in scores) for k in ('0','0.5','1')}
            if name=='fair_coin':metrics[name]['probability_scope']='fixed_0.5_probability_baseline'
            elif name=='rolling_class_rate':metrics[name]['probability_scope']='causally_available_prior_labels_only'
        deltas={name:{'same_completed_denominator':len(rows),
            'mean_joint_minus_comparator_net_bps':h.mean(r['ablation_comparison']['paired_deltas'][name]['joint_minus_comparator_net_bps'] for r in rows),
            'mean_joint_minus_comparator_absolute_error_bps':h.mean(r['ablation_comparison']['paired_deltas'][name]['joint_minus_comparator_absolute_error_bps'] for r in rows)} for name in h.joint.COMPARATORS}
    counts=Counter()
    for r in results:counts.update(r['counts'])
    result={'schema_version':'joint_v3_original_outcome_assessment_v1','status':'passed' if all(r['status']=='passed' for r in results) else 'requires_review',
        'started_utc':h.utc(started),'completed_utc':h.utc(time.time()),'snapshot_start_utc':h.utc(min(r['transaction_started_epoch'] for r in results)),
        'snapshot_end_utc':h.utc(max(r['snapshot_observed_epoch'] for r in results)),'activation_start_utc':h.utc(min(r['activation_epoch'] for r in results)),
        'activation_end_utc':h.utc(max(r['activation_epoch'] for r in results)),'registry_path':str(REGISTRY),'registry_sha256':REGISTRY_SHA,
        'source_bindings':sources,'registered_sources_unchanged':True,'helper_sha256':h.sha(__file__),'prior_pure_helper_sha256':h.sha(PRIOR),
        'ledgers':len(results),'counts':dict(counts),'pairs_ever_published':sum(r['counts']['publication']>0 for r in results),
        'pairs_with_unexpired':sum(r['unexpired_published']>0 for r in results),'unexpired_published':sum(r['unexpired_published'] for r in results),
        'original_exclusions':dict(Counter(e['reason'] for r in results for e in r['exclusions'])),
        'due_without_outcome_or_exclusion':sum(len(r['due_without_outcome_or_exclusion']) for r in results),
        'unscored_original_completed':sum(len(r['original_completed_not_scored']) for r in results),'reconciliation_errors':sum(len(r['verification_errors']) for r in results),
        'stored_scorecard_scored_rows':sum(r['stored_scorecard'].get('stored_scored_rows',0) for r in results),
        'stored_cards_behind_completed':sum(len(r['stored_scorecard'].get('completed_not_in_card',[])) for r in results),
        'metrics':metrics,'paired_deltas':deltas,'dependence':h.dependence(rows),
        'reference_start_utc':h.utc(min(r['reference_epoch'] for r in rows)),'reference_end_utc':h.utc(max(r['reference_epoch'] for r in rows)),
        'target_start_utc':h.utc(min(r['target_epoch'] for r in rows)),'target_end_utc':h.utc(max(r['target_epoch'] for r in rows)),
        'ledger_results':[{'instrument':r['instrument'],'status':r['status'],'path':str(HERE/'results'/f"{r['instrument']}.json"),'sha256':h.sha(HERE/'results'/f"{r['instrument']}.json")} for r in results],
        'limits':{'max_parallel_processes':4,'per_db_bytes':512*1024*1024,'quotes_per_db':100000,'forecasts_per_db':256,'query_seconds':20},
        'limitations':['Only original retained completed outcomes are scored. No future outcomes, missing quote repair, training, forecast regeneration, ledger writes or broker calls.',
            'Per-database query-only transactions have distinct actual clocks; this is not a globally atomic snapshot. Full coherent logical exports are retained compressed outside the project.',
            'All retained quotes are passed to the unchanged exact registered scorer; scored entry and target IDs reconcile with original stored receipts.',
            'Matched price-only and neutral-news estimates were retained before issue and scored on the identical joint decisions and executable endpoints. They are not independent price-family forecasts.',
            'Brier is descriptive for uncalibrated probabilities. Matched comparator probabilities do not exist; no probability results are manufactured.',
            'Net bps describe executable bid/ask endpoints and explicit stress, not actual trades, financing, USD account P/L or leverage return.',
            'H1 windows overlap, instruments share currencies and inputs, and the observation spans less than a day. Counts are not independent trials and no confident improvement or edge is established.',
            'Original separately generated scorecards remain unchanged; any generation-timing set differences are disclosed in individual results.',
            'Historical pre-repair and current cohort aggregates use different periods/denominators and are not a controlled before/after experiment.']}
    h.write_new(HERE/'V3_COMPLETED_PERFORMANCE_20260909.json',result)
    print(json.dumps({'status':result['status'],'counts':result['counts'],'metrics':h.joint.jsonable(metrics),'paired_deltas':h.joint.jsonable(deltas)}),flush=True)

if __name__=='__main__':main()
