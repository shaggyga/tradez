"""Read original verified campaign records; later outcomes require explicit reveal.

No fitting, accounting replay, service connection or evidence promotion occurs.
"""
import hashlib,json
from copy import deepcopy
from collections import Counter
from pathlib import Path
from contracts import fingerprint
from publication import verify_completed_run

MAX_JSON_BYTES=64*1024*1024

class CampaignReader:
    def __init__(self, paths, dependencies):
        if set(paths)!=set(dependencies):raise ValueError('exact_campaign_dependency_paths_required')
        self.paths={k:Path(v) for k,v in paths.items()};self.dependencies=dependencies;self.cache={}
        for alias,root in self.paths.items():
            expected=dependencies[alias]
            manifest=verify_completed_run(root,expected['identity'])
            actual={p['path']:p['sha256'] for p in manifest['payloads'] if p['path'] not in expected['excluded_timing_payloads']}
            if actual!=expected['payloads']:raise ValueError('campaign_scientific_dependency_mismatch:'+alias)

    def read(self,alias,name):
        key=(alias,name)
        if key in self.cache:return self.cache[key]
        if Path(name).name!=name or name not in self.dependencies[alias]['payloads']:
            raise ValueError('unregistered_inspection_payload')
        with (self.paths[alias]/name).open('rb') as f:raw=f.read(MAX_JSON_BYTES+1)
        if len(raw)>MAX_JSON_BYTES:raise ValueError('inspection_json_size_limit')
        if hashlib.sha256(raw).hexdigest()!=self.dependencies[alias]['payloads'][name]:
            raise ValueError('inspection_consumed_bytes_changed')
        value=[json.loads(line) for line in raw.splitlines()] if name.endswith('.jsonl') else json.loads(raw)
        self.cache[key]=value;return value

    def source(self,alias,name,record):
        return {'dependency':alias,'run_identity':self.dependencies[alias]['identity']['fingerprint'],
            'payload':name,'payload_sha256':self.dependencies[alias]['payloads'][name],
            'original_record_sha256':fingerprint(record)}

    def forecast(self,*,pair,origin,target,method,procedure,reveal_outcomes=False,outcome_asof=None):
        if type(reveal_outcomes) is not bool:raise ValueError('explicit_boolean_outcome_reveal_required')
        coverage=[r for r in self.read('matched','coverage.json') if
            (r['instrument'],r['decision_epoch'],r['target_id'],r['method'],r['procedure'])==(pair,origin,target,method,procedure)]
        if len(coverage)!=1:raise ValueError('request_outside_unique_registered_coverage')
        coverage=coverage[0]
        records=[r for r in self.read('matched','forecasts.json') if
            (r['record_id'],r['forecast']['target_id'],r['method'],r['procedure'])==(coverage['record_id'],target,method,procedure)]
        if len(records)!=(1 if coverage['reason']=='eligible' else 0):raise ValueError('forecast_coverage_record_mismatch')
        result={'schema_version':'forex_original_forecast_inspection.v1','coverage':coverage,
            'original_forecast':records[0] if records else None,
            'coverage_source':self.source('matched','coverage.json',coverage),
            'forecast_source':self.source('matched','forecasts.json',records[0]) if records else None,
            'outcomes_revealed':False,'outcome':None,'observed_execution':False}
        if not reveal_outcomes:
            if outcome_asof is not None:raise ValueError('outcome_asof_requires_explicit_reveal')
            return deepcopy(result)
        if type(outcome_asof) is not int or outcome_asof<origin:raise ValueError('explicit_later_outcome_asof_required')
        # Pair identity came from registered coverage, never directly from a path.
        part=self.read('technical','pair_'+pair+'.json')
        labels=[o for o in part['outcomes'] if (o['record_id'],o['target_id'])==(coverage['record_id'],target)]
        if len(labels)!=1:raise ValueError('unique_registered_outcome_required')
        outcome=labels[0];result.update(outcomes_revealed=True,outcome_asof=outcome_asof)
        if outcome['available_epoch']>outcome_asof:
            result['outcome']={'status':'not_yet_available','available_epoch':outcome['available_epoch']}
        else:
            result['outcome']=outcome;result['outcome_source']=self.source('technical','pair_'+pair+'.json',outcome)
        return deepcopy(result)

    def decision(self,*,method,scenario,arm,epoch,engine='reference',reveal_outcomes=False,outcome_asof=None):
        if type(reveal_outcomes) is not bool:raise ValueError('explicit_boolean_outcome_reveal_required')
        alias='policy/'+method+'/'+scenario+'/'+engine
        if alias not in self.dependencies:raise ValueError('unregistered_policy_run')
        records=[r for r in self.read(alias,'policy_decisions.jsonl') if (r['arm'],r['epoch'])==(arm,epoch)]
        if len(records)!=1:raise ValueError('unique_original_policy_decision_required')
        result={'schema_version':'forex_original_policy_inspection.v1','original_decision':records[0],
            'source':self.source(alias,'policy_decisions.jsonl',records[0]),'outcomes_revealed':False,
            'outcome':None,'input_tier':'declared_historical_candle_scenario_not_observed_execution'}
        if not reveal_outcomes:
            if outcome_asof is not None:raise ValueError('outcome_asof_requires_explicit_reveal')
            return deepcopy(result)
        if type(outcome_asof) is not int or outcome_asof<epoch:raise ValueError('explicit_later_outcome_asof_required')
        ledger=[r for r in self.read(alias,'event_ledger.jsonl') if epoch<=r['epoch']<=outcome_asof]
        result.update(outcomes_revealed=True,outcome_asof=outcome_asof)
        if not ledger:result['outcome']={'status':'no_recorded_event_in_requested_window'}
        else:
            last=ledger[-1]
            result['outcome']={'status':'recorded_scenario_events','last_event_epoch':last['epoch'],
                'arm_state':last['arms'][arm],
                'subsequent_arm_events':[{'epoch':r['epoch'],'kind':r['kind'],'event_id':r['event_id'],'receipt':r['receipt']} for r in ledger if r.get('arm')==arm and r['epoch']>epoch]}
            result['outcome_source']=self.source(alias,'event_ledger.jsonl',last)
        return deepcopy(result)

    def report(self):
        technical=self.read('technical','run_report.json');matched=self.read('matched','run_report.json')
        scores=self.read('matched','scores.json');remaining=self.read('remaining','run_report.json')
        coverage=self.read('matched','coverage.json');origins=sorted({r['decision_epoch'] for r in coverage})
        groups=Counter((r['target_id'],r['procedure'],r['method'],r['reason']) for r in coverage)
        support={'first_origin_epoch':origins[0],'last_origin_epoch':origins[-1],'origin_count':len(origins),
            'instrument_count':len({r['instrument'] for r in coverage}),'coverage_rows':len(coverage),
            'groups':[{'target_id':k[0],'procedure':k[1],'method':k[2],'reason':k[3],'rows':v} for k,v in sorted(groups.items())]}
        if support['instrument_count']!=68 or len(coverage)!=matched['coverage_rows']:
            raise ValueError('original_all68_report_coverage_mismatch')
        policies=[];episodes=[]
        for alias in sorted(k for k in self.paths if k.startswith('policy/') and k.endswith('/reference')):
            report=self.read(alias,'run_report.json');policies.append({'source':self.source(alias,'run_report.json',report),'report':report})
            memory=self.read(alias,'policy_state.json')
            for arm,state in memory.items():
                for number,episode in enumerate(state['episodes']):
                    episodes.append({'dependency':alias,'arm':arm,'episode_number':number,
                        'original_episode':episode,
                        'movement_scope':'observed_replay_frame_marks_only_not_continuous_extrema_or_barrier_order',
                        'valuation_coverage':report['valuation_coverage'][arm]})
        return deepcopy({'schema_version':'forex_matched_campaign_inspection_report.v1',
            'scope':'aggregate_report_reveals_previously_inspected_development_outcomes',
            'original_technical_report':technical,'original_matched_report':matched,'original_scores':scores,
            'original_remaining_report':remaining,'policies':policies,'observed_episode_movement':episodes,
            'original_forecast_coverage':support,
            'movement_support':{'contiguous_path_support':technical['contiguous_path_support'],
                'intrabar_barrier_order':'unsupported_by_current_close_only_inputs',
                'continuous_drawdown':'not_established','policy_episode_extrema':'observed_frame_marks_only'},
            'selected_model':None,'engineering_ready':False,'forecast_evidence_status':'bounded_development_only',
            'policy_evidence_status':'declared_candle_scenarios_only','demo_authorization_status':'not_granted',
            'independent_review':False})
