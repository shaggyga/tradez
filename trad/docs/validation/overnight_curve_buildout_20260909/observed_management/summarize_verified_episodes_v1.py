"""Compact deterministic summary of separately retained verifier reports.

This reads no runtime state. It never adds different counterfactual arms into an
account balance or position count, and never counts repeated snapshots twice.
"""
import argparse
from collections import Counter
from decimal import Decimal,Context,localcontext
import hashlib,json,math,os,re,time
from pathlib import Path

SCHEMA='observed_management_compact_results_v1_20260909'
VERIFIER_SCHEMA='observed_management_offline_verification_v1_20260909'
REGISTRY_SHA='60689101fb63a8bcce2465d19f627b9b2ce357d1ee271c65298f255663d991e1'
VERIFIER_SHA='870165e26cc0deeb40c638dbf0ea1afa9f678d8e4645552fabc1cb8e65a1959d'
EPISODES=('episode_01','episode_02','episode_03')
ARMS=('legacy_momentum_reference','usd_momentum_manager','usd_curve_manager','no_trade','curve_hold_no_rotation')
ACTIONS={'enter','exit','hold','wait','rotate'}
MAX_REPORTS=8
MAX_BYTES=64*1024*1024
TRAD=Path(r'C:\Users\zmoor\Documents\forex\trad')
RUNTIME_ROOT=TRAD/'data/oanda_training_manager/observed_curve_management_v1_20260909'

def need(ok,reason):
    if not ok:raise ValueError(reason)
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(value):return hashlib.sha256(canonical(value)).hexdigest()
def epoch(value):
    need(type(value) in (int,float) and math.isfinite(value) and value>0,'summary_clock')
    return float(value)
def cash(value):
    need(type(value) is str and len(value)<=128 and re.fullmatch(r'-?\d+(?:\.\d+)?',value),'summary_exact_cash_text')
    result=Decimal(value);need(result.is_finite() and abs(result)<Decimal('1e12'),'summary_cash_bound')
    return result
def count(value):
    need(type(value) is int and 0<=value<=128,'summary_count_bound');return value
def strict_json(raw):
    def pairs(rows):
        result={}
        for k,v in rows:need(k not in result,'summary_duplicate_json_key');result[k]=v
        return result
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('summary_nonfinite_json')))

def economic_evidence(report,episode_id):
    """Retain original file identities for independent leg-price cost attribution."""
    rows=report.get('verified_files',[])
    need(isinstance(rows,list) and len(rows)<=10000,'summary_file_reference_bound')
    selected=[];seen=set();prefix='episodes/'+episode_id+'/'
    for row in rows:
        need(isinstance(row,dict),'summary_file_reference_shape')
        path=row.get('relative_path')
        need(type(path) is str and len(path)<=1024 and not path.startswith('/')
             and not any(part in ('','..','.') for part in path.split('/'))
             and '\\' not in path and ':' not in path,'summary_file_reference_path')
        need(path not in seen,'summary_duplicate_file_reference');seen.add(path)
        need(type(row.get('sha256')) is str and re.fullmatch(r'[0-9a-f]{64}',row['sha256'])
             and type(row.get('bytes')) is int and 0<=row['bytes']<=64*1024*1024,'summary_file_reference_identity')
        if path.startswith(prefix):selected.append({key:row[key] for key in ('relative_path','sha256','bytes')})
    return dict(runtime_root=str(RUNTIME_ROOT),verified_episode_files=selected,
        scope='References from the selected verifier report only; not reread here. Config, plans, settlements, states and raw quote/receipt files preserve original units, leg prices, quote IDs and clocks. No spread or slippage is inferred from leg counts.',
        direct_price_cost_attribution_performed=False)

def validate_report(report,now):
    need(isinstance(report,dict) and report.get('schema_version')==VERIFIER_SCHEMA,'summary_verifier_schema')
    need(report.get('registry_sha256')==REGISTRY_SHA and report.get('helper_sha256')==VERIFIER_SHA,'summary_fixed_verifier_or_registry')
    started,end=epoch(report['verification_started_epoch']),epoch(report['verification_completed_epoch'])
    need(started<=end<=now,'summary_future_or_reversed_verification')
    sources=report.get('source_bindings')
    need(isinstance(sources,dict) and len(sources)==20 and all(type(v) is str and re.fullmatch(r'[0-9a-f]{64}',v) for v in sources.values()),'summary_source_closure')
    need(report.get('runtime_writes') is False and report.get('broker_actions') is False and report.get('model_fits') is False,'summary_verifier_scope')
    episodes=report.get('episodes');issues=report.get('issues')
    need(isinstance(episodes,list) and isinstance(issues,list) and len(episodes)<=3 and len(issues)<=3,'summary_episode_inventory')
    episode_ids=[e.get('episode_id') for e in episodes];issue_ids=[e.get('episode_id') for e in issues]
    need(len(set(episode_ids+issue_ids))==3 and set(episode_ids+issue_ids)==set(EPISODES),'summary_missing_or_duplicate_episode_disposition')
    need(report.get('status')==('issues' if issues else 'passed'),'summary_issue_status_disagrees')
    for episode in episodes:
        steps=episode.get('steps',[]);missed=episode.get('missed_slots',[])
        need(isinstance(steps,list) and len(steps)<=64 and isinstance(missed,list) and len(missed)<=64,'summary_step_bound')
        need(episode.get('verified_completed_steps',0)==len(steps),'summary_completed_step_count')
        ids=[step.get('step_id') for step in steps];missed_ids=[row.get('step_id') for row in missed]
        need(len(ids)==len(set(ids)) and len(missed_ids)==len(set(missed_ids)) and not set(ids)&set(missed_ids),'summary_duplicate_step')
        for step in steps:
            need(type(step.get('terminal')) is bool and set(step.get('decisions',{}))==set(ARMS)
                 and all(action in ACTIONS for action in step['decisions'].values()),'summary_step_actions')
            need(epoch(step['completed_epoch'])<=end,'summary_future_step')
            for arm in ARMS:
                row=step['independent_arithmetic'][arm]
                cash(row['realized_usd']);count(row['open_legs']);count(row['close_legs'])
                need(type(row.get('position_open')) is bool,'summary_virtual_position_flag')
                if row['liquidation_usd'] is not None:cash(row['liquidation_usd'])
        if 'per_arm' in episode:
            need(set(episode['per_arm'])==set(ARMS),'summary_five_arm_cash_required')
            for arm,value in episode['per_arm'].items():
                cash(value['realized_usd']);need(type(value.get('position_open')) is bool,'summary_virtual_position_flag')
                if value['latest_liquidation_usd'] is not None:cash(value['latest_liquidation_usd'])
                if steps:
                    last=steps[-1]['independent_arithmetic'][arm]
                    need(value['realized_usd']==last['realized_usd'] and value['position_open']==last['position_open']
                         and value['latest_liquidation_usd']==last['liquidation_usd'],'summary_cash_not_latest_verified_state')
            need(cash(episode['per_arm']['no_trade']['realized_usd'])==0
                 and episode['per_arm']['no_trade']['position_open'] is False,'summary_no_trade_state')
        if episode.get('completed_matched_endpoint_eligible') is True:
            need(episode['status']=='terminal_flat' and episode.get('terminal_all_five_flat') is True
                 and not episode.get('uncompleted_or_unobserved_slots') and steps and steps[-1]['terminal']
                 and all(not value['position_open'] for value in episode['per_arm'].values()),'summary_false_completed_eligibility')
            with localcontext(Context(prec=96)):
                delta=cash(episode['per_arm']['usd_curve_manager']['realized_usd'])-cash(episode['per_arm']['usd_momentum_manager']['realized_usd'])
                need(cash(episode['matched_terminal_curve_minus_momentum_usd'])==delta,'summary_matched_delta_disagrees')
        else:
            need(episode.get('matched_terminal_curve_minus_momentum_usd') is None,'summary_incomplete_delta_must_be_withheld')

def summarize_reports(reports,*,clock=time.time):
    """Choose by verification clock only; later issues never reveal older gains."""
    now=epoch(clock());need(isinstance(reports,(list,tuple)) and 1<=len(reports)<=MAX_REPORTS,'summary_report_count')
    for report in reports:validate_report(report,now)
    source=reports[0]['source_bindings']
    need(all(r['source_bindings']==source for r in reports),'summary_source_closure_changed')
    unique={digest(report):report for report in reports}
    ordered=sorted(unique.items(),key=lambda item:(item[1]['verification_completed_epoch'],item[0]))
    retained={episode_id:dict(steps={},missed={},identity=None) for episode_id in EPISODES}
    chosen={};historical_issues=[];by_sha=dict(ordered)
    for report_sha,report in ordered:
        for issue in report['issues']:
            historical_issues.append(dict(report_sha256=report_sha,**issue))
            chosen[issue['episode_id']]=(None,issue,report_sha,report['verification_completed_epoch'])
        for episode in report['episodes']:
            episode_id=episode['episode_id'];prior=retained[episode_id]
            current={step['step_id']:digest(step) for step in episode.get('steps',[])}
            missed={row['step_id']:digest(row) for row in episode.get('missed_slots',[])}
            need(all(current.get(k)==value for k,value in prior['steps'].items()),'summary_prior_step_changed_or_disappeared')
            need(all(missed.get(k)==value for k,value in prior['missed'].items()),'summary_prior_missed_slot_changed_or_disappeared')
            identity=None
            if 'registered_start_epoch' in episode:
                pub=episode['original_published_source_evidence']
                identity=(episode['registered_start_epoch'],episode['native_target_epoch'],
                    tuple(sorted((r['relative_path'],r['sha256']) for r in pub['files'])))
            need(prior['identity'] is None or identity==prior['identity'],'summary_original_episode_identity_changed')
            prior.update(steps=current,missed=missed,identity=identity)
            chosen[episode_id]=(episode,None,report_sha,report['verification_completed_epoch'])
    output=[];eligible=[]
    for episode_id in EPISODES:
        episode,issue,report_sha,as_of=chosen[episode_id]
        if issue is not None:
            output.append(dict(episode_id=episode_id,status='latest_verification_issue',issue=issue,
                selected_report_sha256=report_sha,as_of_epoch=as_of,completed_matched_endpoint_eligible=False))
            continue
        steps=episode.get('steps',[]);normal=[step for step in steps if not step['terminal']]
        result=dict(episode_id=episode_id,status=episode['status'],selected_report_sha256=report_sha,as_of_epoch=as_of,
            original_start_epoch=episode.get('registered_start_epoch'),original_target_epoch=episode.get('native_target_epoch'),
            verified_completed_steps=len(steps),recorded_missed_slots=episode.get('missed_slots',[]),
            uncompleted_slots=[{**row,'current_disposition':'future_scheduled' if row['scheduled_epoch']>as_of else 'unobserved_or_uncompleted'}
                for row in episode.get('uncompleted_or_unobserved_slots',[])],
            candidate_support=dict(nonterminal_decision_steps=len(normal),
                curve_available_steps=sum(step['curve_candidate_count']>0 for step in normal),
                momentum_available_steps=sum(step['momentum_candidate_count']>0 for step in normal)),
            selected_quote_missing_steps=sum(step['selected_observation_epoch'] is None for step in steps),
            completed_matched_endpoint_eligible=episode.get('completed_matched_endpoint_eligible') is True,
            retained_disposition=episode.get('retained_episode_result'),
            economic_source_evidence=economic_evidence(by_sha[report_sha],episode_id),per_arm={})
        for arm,value in episode.get('per_arm',{}).items():
            result['per_arm'][arm]=dict(virtual_scenario_position_open=value['position_open'],
                realized_scenario_cash_usd=value['realized_usd'],latest_hypothetical_liquidation_usd=value['latest_liquidation_usd'],
                decision_counts=dict(sorted(Counter(step['decisions'][arm] for step in steps).items())),
                virtual_action_status_counts=dict(sorted(Counter(step['action_statuses'][arm] for step in steps).items())),
                virtual_open_legs=sum(step['independent_arithmetic'][arm]['open_legs'] for step in steps),
                virtual_close_legs=sum(step['independent_arithmetic'][arm]['close_legs'] for step in steps))
        output.append(result)
        if result['completed_matched_endpoint_eligible']:eligible.append(episode)
    with localcontext(Context(prec=96)):
        totals={arm:None if not eligible else str(sum((cash(e['per_arm'][arm]['realized_usd']) for e in eligible),Decimal(0))) for arm in ARMS}
        deltas=[cash(e['matched_terminal_curve_minus_momentum_usd']) for e in eligible]
        matched_sum=None if not deltas else str(sum(deltas,Decimal(0)))
        matched_mean=None if not deltas else str(sum(deltas,Decimal(0))/len(deltas))
    body=dict(schema_version=SCHEMA,generated_epoch=now,registry_sha256=REGISTRY_SHA,expected_verifier_sha256=VERIFIER_SHA,
        source_bindings=source,input_report_count=len(reports),unique_input_report_count=len(ordered),
        input_reports=[dict(report_sha256=key,verification_completed_epoch=report['verification_completed_epoch']) for key,report in ordered],
        episodes=output,historical_input_issues=historical_issues,
        matched_completed_support=dict(eligible_episode_ids=[e['episode_id'] for e in eligible],
            eligible_episode_count=len(eligible),predeclared_episode_count=3,
            ineligible_episode_ids=[row['episode_id'] for row in output if not row['completed_matched_endpoint_eligible']],
            common_five_arm_episode_denominator=len(eligible),independent_sample_size=None),
        hypothetical_completed_scenario_totals_by_arm_usd=totals,
        matched_completed_curve_minus_momentum_sum_usd=matched_sum,
        matched_completed_curve_minus_momentum_mean_usd=matched_mean,
        scopes=[
            'Separate virtual arms are counterfactual research scenarios, not simultaneous account positions.',
            'Per-arm sums use each eligible episode once and reset declared sizing per episode; they are not actual account PnL.',
            'No completed-episode measurement is replaced with an open mark or a missing-as-zero value.',
            'Future, missed, unresolved and verification-issue support remains visible; no efficacy conclusion or independent-trial claim.',
            'This serializer checks retained report consistency; it does not recheck runtime source files or rerun the verifier.'],
        account_state_observed=False,broker_actions=False,runtime_writes=False)
    return {**body,'summary_sha256':digest(body)}

def main(argv=None):
    parser=argparse.ArgumentParser();parser.add_argument('--report',action='append',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args(argv);need(1<=len(args.report)<=MAX_REPORTS,'summary_report_count')
    reports=[];files=[];total=0
    for text in args.report:
        path=Path(text).resolve();need(path.stat().st_size<=MAX_BYTES,'summary_input_byte_bound');raw=path.read_bytes();total+=len(raw)
        need(total<=MAX_BYTES,'summary_total_byte_bound');reports.append(strict_json(raw))
        files.append(dict(path=str(path),bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest()))
    summary=summarize_reports(reports);summary['input_file_evidence']=files
    summary['summary_sha256']=digest({k:v for k,v in summary.items() if k!='summary_sha256'})
    output=Path(args.output).resolve();need(output.suffix=='.json' and output.parent.is_dir()
        and TRAD.resolve() not in output.parents,'summary_external_output_required')
    raw=(json.dumps(summary,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    with output.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    print(json.dumps(dict(path=str(output),sha256=hashlib.sha256(raw).hexdigest(),
        matched_completed_support=summary['matched_completed_support'])))

if __name__=='__main__':main()
