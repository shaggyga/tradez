"""Score only originally retained completed outcomes, from immutable backup DBs.

Uses unchanged registered exact evaluators and every retained quote. Never
constructs a ledger, regenerates forecasts, settles a ledger, or reads live DBs.
Original full-export hash and completed-only selection hash are both retained.
"""
import argparse
from collections import Counter,defaultdict
from contextlib import closing
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime,timezone
from decimal import Decimal,localcontext,Context,ROUND_HALF_EVEN
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import sys
import time

sys.dont_write_bytecode=True
ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad').resolve()
WORK=Path(__file__).resolve().parent
DEST=Path(r'D:\ForexRecovery\revamp_20260908T1353Z\databases').resolve()
MANIFEST=WORK/'SELECTED_LEDGER_BACKUP_MANIFEST_20260908.json'
sys.path.insert(0,str(ROOT))
import oanda_fixed_forecast_evaluation_joint_news_v1 as joint
import oanda_fixed_forecast_evaluation_pair_v2 as price

def encoded(v):return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(v):return hashlib.sha256(encoded(v)).hexdigest()
def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    return h.hexdigest()
def utc(t):return datetime.fromtimestamp(t,timezone.utc).isoformat()
def write_new(path,v):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('x',encoding='utf-8',newline='\n') as f:
        json.dump(joint.jsonable(v),f,indent=2,allow_nan=False);f.write('\n')

def mean(values):
    vals=[v if isinstance(v,Decimal) else Decimal(int(v)) if type(v)is bool else Decimal(str(v)) for v in values if v is not None]
    return sum(vals,Decimal(0))/len(vals) if vals else None

def summarize(scores):
    active=[r for r in scores if r['side']]
    mse=mean(r['squared_error_bps'] for r in scores)
    return {'valid_completed_decisions':len(scores),'directional_decisions':len(active),'neutral_decisions':len(scores)-len(active),
        'direction_hits':sum(bool(r['direction_correct']) for r in active),'direction_hit_rate_when_directional':mean(r['direction_correct'] for r in active),
        'positive_after_spread':sum(bool(r['positive_after_spread']) for r in active),'positive_after_spread_rate_when_directional':mean(r['positive_after_spread'] for r in active),
        'mean_brier':mean(r.get('brier') for r in scores),'brier_denominator':sum(r.get('brier')is not None for r in scores),
        'mae_bps':mean(r['absolute_error_bps'] for r in scores),'magnitude_denominator':sum(r['absolute_error_bps']is not None for r in scores),'rmse_bps':mse.sqrt() if mse is not None else None,
        'mean_net_bps_per_valid_decision':mean(r['net_bps'] for r in scores),
        'stress_mean_net_bps_per_valid_decision':{stress:mean(r['stress_net_bps'][stress] for r in scores) for stress in ('0.0','0.5','1.0') if scores and stress in scores[0]['stress_net_bps']},
        'probability_scope':'uncalibrated_registered_predictions' if any(r.get('brier')is not None for r in scores) else 'no_probability_forecast_provided'}

def dependence(rows):
    clocks={(r['instrument'],r['reference_epoch'],r['target_epoch']) for r in rows}
    nonoverlap=0
    for pair in sorted({r['instrument'] for r in rows}):
        last_end=-math.inf
        for ref,target in sorted({(r['reference_epoch'],r['target_epoch']) for r in rows if r['instrument']==pair}):
            if ref>=last_end:nonoverlap+=1;last_end=target
    return {'scored_rows':len(rows),'pairs':len({r['instrument'] for r in rows}),'distinct_pair_reference_target_events':len(clocks),
        'distinct_exact_reference_epochs':len({r['reference_epoch'] for r in rows}),'reference_15minute_utc_buckets':len({int(r['reference_epoch'])//900 for r in rows}),
        'reference_utc_hours':len({int(r['reference_epoch'])//3600 for r in rows}),'reference_utc_days':len({int(r['reference_epoch'])//86400 for r in rows}),
        'greedy_nonoverlapping_H1_reference_windows_within_pairs':nonoverlap,'independent_sample_size':None,
        'warning':'Clock counts and nonoverlapping within-pair windows do not establish independence: currencies share market factors, families share inputs, and H1 windows overlap.'}

def compact_row(row,item):
    scorekeys=('side','probability_up','brier','direction_correct','absolute_error_bps','squared_error_bps','net_bps','positive_after_spread','stress_net_bps')
    result={k:row[k] for k in ('decision_id','reference_epoch','target_epoch','actual_return_bps','actual_midpoint_move','outcome_class','baseline_issue_cutoff_epoch','common_decision_epoch','actual_holding_sec')}
    result.update(study=item['study'],instrument=item['instrument'],family=item['family'],
        reference=row['reference_quote'],entry=row['entry'],target=row['target'],
        scores={name:{k:r[k] for k in scorekeys} for name,r in row['scores'].items()},rolling_baseline_training=row['rolling_baseline_training'])
    if 'ablation_comparison' in row:result['ablation_comparison']=row['ablation_comparison']
    return result

def evaluate_one(item):
    path=Path(item['destination']).resolve(); assert path.is_relative_to(DEST) and sha(path)==item['snapshot_sha256']
    started=time.time();cutoff=item['snapshot_observed_epoch'];family=item['family']
    scorer=joint if family=='ridge_price_news_v1' else price
    deadline=time.monotonic()+30
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True,timeout=.1)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.set_progress_handler(lambda:int(time.monotonic()>deadline),10000)
        saved=db.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone();contract=json.loads(saved['payload'])
        assert saved['sha']==item['contract_sha256']==digest(contract)
        assert contract['instrument']==item['instrument'] and contract['family']==family
        active=db.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
        assert active['epoch']==item['activation_epoch'] and active['contract_sha']==saved['sha']
        assert item['counts']['quotes']<=250000 and item['counts']['forecasts']<=4096
        for table in ('quotes','forecasts'):
            assert db.execute(f'SELECT coalesce(sum(length(payload)),0) FROM {table}').fetchone()[0]<=512*1024*1024
        rows=db.execute('''SELECT f.*,c.epoch AS consumed,c.forecast_sha AS consumed_sha,c.publication_sha,
            p.epoch AS published,p.forecast_sha AS published_sha FROM forecasts f
            LEFT JOIN consumption c ON c.id=f.id LEFT JOIN publication p ON p.id=f.id ORDER BY f.bucket''').fetchall()
        quotes=[json.loads(r[0]) for r in db.execute('SELECT payload FROM quotes ORDER BY available,market,id')]
        entries={r['id']:dict(r) for r in db.execute('SELECT * FROM entries')}
        outcomes={r['id']:dict(r) for r in db.execute('SELECT * FROM outcomes')}
        exclusions=[dict(r) for r in db.execute('SELECT * FROM exclusions ORDER BY epoch,id')]
        diagnostics=[dict(r) for r in db.execute('SELECT attempt_id,bucket,epoch,payload FROM diagnostics ORDER BY epoch DESC LIMIT 3')]
        last_quote=db.execute('SELECT max(market),max(available) FROM quotes').fetchone()
    decisions=[];verification_errors=[];publication_rows=[]
    for row in rows:
        value=json.loads(row['payload']); assert digest(value)==row['sha'] and value['decision_id']==row['id']
        assert value['reference_epoch']==row['reference'] and value['target_epoch']==row['target']
        assert len(value['forecasts'])==1;arm=value['forecasts'][0]
        if row['published']is not None:
            assert row['published_sha']==row['sha'] and arm['issued_epoch']<=row['published']<=cutoff
        if row['consumed']is not None:
            assert row['published']is not None and row['consumed_sha']==row['sha']
            assert row['publication_sha']==digest({'epoch':row['published'],'forecast_sha':row['sha']})
            assert row['published']<=row['consumed']<=cutoff
        arm['committed_available_epoch']=row['consumed'];decisions.append(value)
        publication_rows.append({'decision_id':row['id'],'bucket':row['bucket'],'reference_epoch':row['reference'],'target_epoch':row['target'],'issued_epoch':arm['issued_epoch'],'published_epoch':row['published'],'consumed_epoch':row['consumed'],'has_recorded_entry':row['id']in entries,'has_recorded_outcome':row['id']in outcomes})
    counts={k:item['counts'][k] for k in ('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')}
    protocol=dict(contract['evaluation_protocol'])
    protocol.update(historical_start_utc=utc(active['epoch']),historical_end_utc=utc(max(cutoff,active['epoch']+.000001)))
    full={'schema_version':scorer.SCHEMA,'observed_cutoff_epoch':cutoff,'decisions':decisions,'quotes':quotes,'collection_counts':counts}
    full_hash=digest(full)
    selected=[d for d in decisions if d['decision_id']in outcomes]
    assert set(outcomes)<={d['decision_id'] for d in decisions}
    for d in selected:
        identity=d['decision_id'];assert identity in entries and d['target_epoch']<=cutoff
        assert entries[identity]['epoch']<=outcomes[identity]['epoch']<=cutoff
        assert not any(x['id']==identity for x in exclusions)
    dataset={**full,'decisions':selected}
    report=scorer.evaluate(dataset,protocol)
    scored={r['decision_id']:r for r in report['decisions']}
    for identity,row in scored.items():
        if row['entry']['quote_id']!=entries[identity]['quote_id'] or row['target']['quote_id']!=outcomes[identity]['quote_id']:
            verification_errors.append({'decision_id':identity,'reason':'scorer_selected_quote_differs_from_original_stored_entry_or_outcome'})
    missing=sorted(set(outcomes)-set(scored))
    stored=item['stored_scorecard'];card_summary={'capture_status':stored['status']}
    if stored['status']=='captured_separately':
        cardpath=Path(stored['path']);assert sha(cardpath)==stored['sha256'];card=json.loads(cardpath.read_bytes())
        assert card['instrument']==item['instrument'] and card['family']==family and card['study_contract_sha256']==item['contract_sha256']
        cardrows={r['decision_id']:r for r in card.get('decisions',[])}
        mismatches=[]
        for identity in set(cardrows)&set(scored):
            a=cardrows[identity];b=joint.jsonable(scored[identity])
            for field in ('reference_quote','entry','target','scores','ablation_comparison'):
                if field in a or field in b:
                    if a.get(field)!=b.get(field):mismatches.append({'decision_id':identity,'field':field})
        card_summary.update(sha256=stored['sha256'],generated_utc=card['generated_utc'],stored_scored_rows=len(cardrows),
            original_coverage=card['coverage'],original_exclusions=card['decision_exclusion_counts'],stored_only_ids=sorted(set(cardrows)-set(scored)),
            retained_completed_not_in_stored_card=sorted(set(scored)-set(cardrows)),overlapping_scored_payload_mismatches=mismatches,
            caveat='Scorecard captured separately after database snapshot; set differences can reflect generation timing. Original payload is preserved.')
        verification_errors.extend(mismatches)
    compact=[compact_row(r,item) for r in report['decisions']]
    result={'status':'passed' if not missing and not verification_errors else 'reconciliation_requires_review',
        'study':item['study'],'instrument':item['instrument'],'family':family,'database_snapshot_path':str(path),'database_snapshot_sha256':item['snapshot_sha256'],
        'snapshot_observed_utc':item['snapshot_observed_utc'],'snapshot_observed_epoch':cutoff,'activation_epoch':active['epoch'],
        'original_contract_sha256':item['contract_sha256'],'original_full_export_sha256':full_hash,'completed_selection_input_sha256':report['input_sha256'],'evaluation_protocol_sha256':report['protocol_sha256'],
        'original_counts':counts,'publication_rows':publication_rows,'latest_quote_market_epoch':last_quote[0],'latest_quote_available_epoch':last_quote[1],
        'quote_observation_age_sec_at_snapshot':cutoff-last_quote[1] if last_quote[1] else None,
        'published_unexpired_at_snapshot':sum(r['consumed_epoch']is not None and r['target_epoch']>cutoff for r in publication_rows),
        'target_due_without_recorded_outcome_or_exclusion':sorted(r['decision_id'] for r in publication_rows if r['target_epoch']<=cutoff and r['decision_id']not in outcomes and not any(e['id']==r['decision_id'] for e in exclusions)),
        'recorded_entries':list(entries.values()),'recorded_outcomes':list(outcomes.values()),'recorded_exclusions':exclusions,
        'last_retained_diagnostics':[{**d,'payload':json.loads(d['payload'])} for d in diagnostics],
        'stored_scorecard':card_summary,'recorded_completed_not_scored':missing,'verification_errors':verification_errors,
        'evaluator_coverage':report['coverage'],'evaluator_exclusions':report['exclusions'],'quote_exclusions':report['quote_exclusions'],
        'scored_rows':compact,'evaluation_elapsed_sec':time.time()-started}
    return result

def build_summary(results):
    groups=defaultdict(list);rowgroups=defaultdict(list);allrows=[]
    for item in results:
        if item['status']!='passed':continue
        for row in item['scored_rows']:
            allrows.append(row)
            for name,score in row['scores'].items():groups[(item['study'],item['family'],name)].append(score)
            for name,score in row.get('ablation_comparison',{}).get('scores',{}).items():groups[(item['study'],item['family'],name)].append(score)
            rowgroups[(item['study'],item['family'])].append(row)
    summaries=[{'study':k[0],'family':k[1],'scored_arm':k[2],**summarize(rows)} for k,rows in sorted(groups.items())]
    dependence_counts=[{'study':k[0],'family':k[1],**dependence(rows)} for k,rows in sorted(rowgroups.items())]
    paired=[]
    for (study,family),rows in sorted(rowgroups.items()):
        if family!='ridge_price_news_v1':continue
        for comparator in joint.COMPARATORS:
            paired.append({'study':study,'comparator':comparator,'exact_same_valid_completed_decisions':len(rows),
                'mean_joint_minus_comparator_absolute_error_bps':mean(r['ablation_comparison']['paired_deltas'][comparator]['joint_minus_comparator_absolute_error_bps'] for r in rows),
                'mean_joint_minus_comparator_net_bps':mean(r['ablation_comparison']['paired_deltas'][comparator]['joint_minus_comparator_net_bps'] for r in rows),
                'warning':'Paired engineering decomposition on pre-issue retained comparator magnitudes; does not isolate a causal news effect; comparators supplied no probability.'})
    strict_matches=[]
    price_rows={family:{(r['instrument'],r['reference_epoch'],r['target_epoch']):r for r in allrows if r['study']=='pair_local_forecast_study_v2' and r['family']==family} for family in ('ridge_return_repaired','probabilistic_state_space')}
    def economic_identity(row):
        return tuple(tuple(row[k][field] for field in ('instrument','market_epoch','bid','ask')) for k in ('reference','entry','target'))
    for family,lookup in price_rows.items():
        candidates=[(r,lookup[(r['instrument'],r['reference_epoch'],r['target_epoch'])]) for r in allrows if r['study']=='joint_price_news_study_v2' and (r['instrument'],r['reference_epoch'],r['target_epoch'])in lookup]
        matching=[(a,b) for a,b in candidates if economic_identity(a)==economic_identity(b)]
        strict_matches.append({'joint_study':'joint_price_news_study_v2','independent_price_family':family,'same_instrument_reference_target_count':len(candidates),'exact_same_reference_entry_target_prices_and_market_times_count':len(matching),
            'joint_summary':summarize([a['scores'][a['family']] for a,b in matching]),'independent_price_summary':summarize([b['scores'][family] for a,b in matching]),
            'matched_ids':[{'joint':a['decision_id'],'price':b['decision_id'],'instrument':a['instrument'],'reference_epoch':a['reference_epoch']} for a,b in matching],
            'warning':'Independent family cohorts have different issue/entry clocks and availability. Only exact endpoint matches are reported here; broad cohort averages are not a paired comparison.'})
    studies=[]
    for study in ('joint_price_news_study_v2','joint_price_news_study_v1','pair_local_forecast_study_v2'):
        items=[r for r in results if r['study']==study]; counts=Counter()
        for r in items:counts.update(r['original_counts'])
        studies.append({'study':study,'ledgers':len(items),'counts':dict(counts),'pairs_ever_published':len({r['instrument'] for r in items if r['original_counts']['publication']}),
            'pairs_with_unexpired_published_forecast_at_own_snapshot':len({r['instrument'] for r in items if r['published_unexpired_at_snapshot']}),
            'unexpired_published_forecasts_at_own_snapshot':sum(r['published_unexpired_at_snapshot'] for r in items),
            'stored_scorecard_scored_rows':sum(r['stored_scorecard'].get('stored_scored_rows',0) for r in items),
            'exact_rescored_original_completed_rows':sum(len(r['scored_rows']) for r in items if r['status']=='passed'),
            'original_exclusion_reasons':dict(Counter(e['reason'] for r in items for e in r['recorded_exclusions'])),
            'target_due_without_recorded_outcome_or_exclusion':sum(len(r['target_due_without_recorded_outcome_or_exclusion']) for r in items)})
    return {'study_coverage':studies,'metrics':summaries,'matched_joint_internal_comparators':paired,'exact_endpoint_independent_price_matches':strict_matches,'dependence_counts':dependence_counts,
        'limitations':['Original stored completed outcomes only; forecasts without original outcome rows remain in coverage and are never synthesized or counted as successful.',
            'All retained quotes are supplied to the unchanged registered exact evaluator; stored entry/target IDs must reconcile. No forecast regeneration or model fitting.',
            'The original complete-export hash and selected completed-only input hash are distinct. Original scorecards remain preserved separately and are not overwritten.',
            'Family and cohort denominators differ. Joint internal comparators are the only guaranteed paired price/news decomposition.',
            'Probabilities are uncalibrated; Brier is descriptive, and neutral-news/matched-price comparator probabilities were not provided.',
            'Net bps use executable bid/ask plus explicit 0/.5/1bps stress, not actual fills, financing, account USD P/L or margin returns.',
            'Per-database backup transactions have different observation clocks. The collection is not a single globally atomic market snapshot.',
            'Overlapping horizons, shared currencies and model features prevent treating rows as independent trials. No confidence interval or independent effective sample size is asserted.']}

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--limit',type=int,default=0);ap.add_argument('--workers',type=int,default=1);args=ap.parse_args()
    assert 1<=args.workers<=4
    manifest=json.loads(MANIFEST.read_bytes());assert manifest['status']=='passed'
    assert all(sha(ROOT/name)==expected for name,expected in manifest['source_bindings'].items())
    entries=manifest['entries'][:args.limit] if args.limit else manifest['entries']
    label='PILOT' if args.limit else 'CURRENT'
    dest=WORK/f'{label}_PREDICTION_BASELINE_20260908.json'
    assert not dest.exists();started=time.time();results=[]
    def retain_results(evaluated):
        for n,(item,result) in enumerate(zip(entries,evaluated),1):
            results.append(result)
            write_new(WORK/'scored_snapshots'/label/item['study']/item['instrument']/f"{item['family']}.json",result)
            if n%8==0 or n==len(entries):print(json.dumps({'completed':n,'total':len(entries),'last':item['instrument'],'status':result['status'],'scored':len(result['scored_rows']),'elapsed_sec':time.time()-started}),flush=True)
    if args.workers==1:
        retain_results(map(evaluate_one,entries))
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            retain_results(pool.map(evaluate_one,entries,chunksize=1))
    with localcontext(Context(prec=80,rounding=ROUND_HALF_EVEN)):
        summary=build_summary(results)
    source_ok=all(sha(ROOT/name)==expected for name,expected in manifest['source_bindings'].items())
    value={'schema_version':'preserved_completed_prediction_baseline_v1','status':'passed' if source_ok and all(r['status']=='passed' for r in results) else 'requires_review',
        'evaluation_started_utc':utc(started),'evaluation_completed_utc':utc(time.time()),'backup_manifest_path':str(MANIFEST),'backup_manifest_sha256':sha(MANIFEST),
        'evaluator_source_bindings':manifest['source_bindings'],'registered_sources_unchanged':source_ok,'helper_sha256':sha(__file__),
        'snapshot_observation_start_utc':min(r['snapshot_observed_utc'] for r in results),'snapshot_observation_end_utc':max(r['snapshot_observed_utc'] for r in results),
        'database_count':len(results),'maximum_parallel_workers':args.workers,'ledger_results':[{'study':r['study'],'instrument':r['instrument'],'family':r['family'],'status':r['status'],
            'path':str(WORK/'scored_snapshots'/label/r['study']/r['instrument']/f"{r['family']}.json"),'sha256':sha(WORK/'scored_snapshots'/label/r['study']/r['instrument']/f"{r['family']}.json")} for r in results],**summary}
    write_new(dest,value);print(json.dumps({'path':str(dest),'sha256':sha(dest),'status':value['status'],'elapsed_sec':time.time()-started}),flush=True)

if __name__=='__main__':main()
