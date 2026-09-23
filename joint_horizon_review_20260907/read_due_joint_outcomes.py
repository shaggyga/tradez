"""Bounded existing H1 outcome/scorecard observation; no scoring or runtime writes."""
from collections import Counter
from contextlib import closing
from datetime import datetime,timezone
from decimal import Decimal
import hashlib,json,sqlite3,time
from pathlib import Path
ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad');WORK=Path(__file__).resolve().parent
def sha(raw):return hashlib.sha256(raw).hexdigest()
def utc(t):return datetime.fromtimestamp(t,timezone.utc).isoformat()
def main():
    begun=time.time();versions={}
    for version in ('v1','v2'):
        config=ROOT/'config'/f'joint_price_news_study_{version}_20260907.json';registry=json.loads(config.read_bytes())
        if any(sha((ROOT/n).read_bytes())!=v for n,v in registry['source_bindings'].items()):raise ValueError('frozen_source_changed')
        total=Counter();pairs=[];scores=[];cutoffs=[];targets=[]
        folder=WORK/'retained_scorecards'/version;folder.mkdir(parents=True,exist_ok=True)
        for pair,item in registry['pairs'].items():
            registered=item['families']['ridge_price_news_v1'];base=ROOT/'data/oanda_training_manager'/f'joint_price_news_study_{version}'/'pairs'/pair/'ridge_price_news_v1'
            path=base/'study.sqlite';deadline=time.monotonic()+1;at=time.time()
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=.2)) as db:
                db.execute('PRAGMA query_only=ON');db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000);db.execute('BEGIN')
                forecast,due,first=db.execute('SELECT COUNT(*),SUM(target<=?),MIN(target) FROM forecasts',(at,)).fetchone()
                outcomes=db.execute('SELECT COUNT(*) FROM outcomes').fetchone()[0];db.rollback()
            total.update(forecasts=forecast,original_targets_due=due or 0,retained_outcomes=outcomes)
            if first is not None:targets.append(first)
            entry={'pair':pair,'observed_utc':utc(at),'forecasts':forecast,'original_targets_due':due or 0,'retained_outcomes':outcomes,'stored_scored_rows':0}
            scorepath=base/'scorecard.json'
            if scorepath.is_file():
                with scorepath.open('rb') as h:raw=h.read(8*1024*1024+1)
                if len(raw)>8*1024*1024:raise ValueError('scorecard_bound')
                value=json.loads(raw);observed=time.time()
                if value.get('study_contract_sha256')!=registered['contract_sha256'] or value['family']!='ridge_price_news_v1':raise ValueError('scorecard_contract_identity')
                generated=datetime.fromisoformat(value['generated_utc'].replace('Z','+00:00')).timestamp()
                if generated>observed:raise ValueError('future_scorecard')
                decisions=value['decisions']
                if len(decisions)>64 or value['coverage']['scored_decisions']!=len(decisions):raise ValueError('scorecard_row_count')
                for row in decisions:
                    if row['target_epoch']>generated:raise ValueError('scored_target_future')
                    s=row['scores']['ridge_price_news_v1'];scores.append({'pair':pair,'decision_id':row['decision_id'],'target_epoch':row['target_epoch'],
                        'side':s['side'],'direction_correct':s['direction_correct'],'positive_after_spread':s['positive_after_spread'],
                        'net_bps':s['net_bps'],'brier':s['brier'],'absolute_error_bps':s['absolute_error_bps']})
                with (folder/(pair+'.json')).open('xb') as h:h.write(raw)
                total.update(stored_scored_rows=len(decisions));cutoffs.append(generated)
                entry.update(stored_scored_rows=len(decisions),scorecard_sha256=sha(raw),scorecard_generated_utc=value['generated_utc'],scorecard_age_sec=observed-generated)
            pairs.append(entry)
        active=[s for s in scores if s['side']];count=len(scores)
        metrics={'scored_decisions':count,'directional_decisions':len(active),'direction_correct':sum(bool(s['direction_correct']) for s in active),
            'positive_after_spread':sum(bool(s['positive_after_spread']) for s in active),
            'mean_net_bps_per_scored_decision':str(sum((Decimal(str(s['net_bps'])) for s in scores),Decimal(0))/count) if count else None,
            'mean_brier':str(sum((Decimal(str(s['brier'])) for s in scores),Decimal(0))/count) if count else None}
        versions[version]={'registry_sha256':sha(config.read_bytes()),'source_bindings_unchanged':True,'totals':dict(total),'stored_scorecard_metrics':metrics,
            'earliest_retained_original_target_utc':utc(min(targets)) if targets else None,'scored_record_generated_range_utc':[utc(min(cutoffs)),utc(max(cutoffs))] if cutoffs else None,
            'pairs':pairs,'scored_rows':scores}
    result={'status':'observed','started_utc':utc(begun),'finished_utc':utc(time.time()),'versions':versions,
        'runtime_writes':False,'scoring_runs':0,'model_fits':0,'broker_calls':False,
        'limitations':['Reports existing stored scorer results only; outcomes can arrive before their60-second scorecard refresh.',
            'Each ledger and scorecard has its own observation clock. Stored row counts need not match a simultaneous global outcome count.',
            'These first overlapping, cross-pair results are not independent trials and cannot establish accuracy, calibration or news-added value.',
            'Net bps are executable bid/ask score units before financing and additional slippage, not dollar account profit.']}
    p=WORK/'CURRENT_JOINT_H1_OUTCOME_OBSERVATION_20260907.json'
    with p.open('x',encoding='utf8') as h:json.dump(result,h,indent=2)
    print(json.dumps({'path':str(p),'sha256':sha(p.read_bytes()),'started_utc':result['started_utc'],'finished_utc':result['finished_utc'],'versions':{v:{'totals':x['totals'],'metrics':x['stored_scorecard_metrics'],'earliest_target':x['earliest_retained_original_target_utc']} for v,x in versions.items()}}))
if __name__=='__main__':main()
