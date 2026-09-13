#!/usr/bin/env python3
"""Category follow/fade news reaction research using explicit endpoint net bps.

This is a new unit/availability contract, not a rescore or conversion of retained
v1 pip averages. Historical and forward qualification remain separate work.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import math
import sqlite3
from collections import defaultdict
from datetime import datetime,timezone
from pathlib import Path
from typing import Any,Iterable,Mapping

try:
    from oanda_news_reaction_contract_v2 import ENDPOINT_CONTRACT,REACTION_CONTRACT,aware_time,finite_number,positive_integer,validate_retained_call
except ModuleNotFoundError:
    from trad.oanda_news_reaction_contract_v2 import ENDPOINT_CONTRACT,REACTION_CONTRACT,aware_time,finite_number,positive_integer,validate_retained_call

try:
    from oanda_news_research_report_io_v1 import read_json_snapshot,new_run_report_path,publish_new_json_report
except ModuleNotFoundError:
    from trad.oanda_news_research_report_io_v1 import read_json_snapshot,new_run_report_path,publish_new_json_report

HORIZONS_MIN=(5,15,30,60,120,240)
THRESHOLDS_BPS=(0.0,0.25,0.5,1.0,2.0,4.0)
DEFAULT_REPORT=Path(__file__).resolve().parent/'data'/'oanda_training_manager'/'reports'/'news_event_reaction'/'news_event_reaction_bps_v3_latest.json'
SURPRISE_FIELDS={
 'actual':('actual','actual_value','economic_actual','release_actual'),
 'consensus':('consensus','consensus_value','economic_consensus','forecast_value'),
 'previous':('previous','previous_value','economic_previous'),
 'surprise':('surprise','surprise_value','standardized_surprise')}


def utc_now() -> str:return datetime.now(timezone.utc).isoformat()


def _load_calls_snapshot(report_path: Path,version: str='cleaned_current',*,as_of_utc: Any=None):
    payload,identity=read_json_snapshot(report_path)
    policy=payload.get('policy')
    if not isinstance(policy,Mapping) or policy.get('chronological_first_seen_only') is not True:
        raise ValueError('news_replay_literal_first_seen_contract_required')
    if payload.get('endpoint_contract')!=ENDPOINT_CONTRACT:
        raise ValueError('new_exact_endpoint_report_required;legacy_pips_not_reconstructed')
    rows=payload.get('scored_call_details')
    if not isinstance(rows,list) or any(not isinstance(row,Mapping) for row in rows):raise ValueError('scored_call_mapping_list_required')
    asof=aware_time(utc_now() if as_of_utc is None else as_of_utc,'as_of_utc').isoformat()
    return [validate_retained_call(row,as_of_utc=asof) for row in rows if str(row.get('version') or '')==version],identity


def load_calls(report_path: Path,version: str='cleaned_current',*,as_of_utc: Any=None) -> list[dict[str,Any]]:
    return _load_calls_snapshot(report_path,version,as_of_utc=as_of_utc)[0]


LEG_RECEIPT_FIELDS=('endpoint_contract','pair','direction','horizon_minutes','signal_utc','entry_utc',
 'nominal_target_utc','exit_utc','as_of_utc','entry_bid','entry_ask','exit_bid','exit_ask',
 'follow_net_bps','fade_net_bps','pip_contract','topic_id','category','return_unit','bps_denominator',
 'cost_contract','price_clock_convention')


def _leg_receipt(row: Mapping[str,Any]) -> dict[str,Any]:
    return {field:row[field] for field in LEG_RECEIPT_FIELDS}


def _endpoint_evidence_hash(legs: list[Mapping[str,Any]]) -> str:
    # Revalidation time is deliberately excluded; original scoring time and
    # every consumed endpoint/category/topic field remain bound.
    payload=[_leg_receipt(row) for row in sorted(legs,key=lambda row:row['pair'])]
    return hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def _leg_identity(row: Mapping[str,Any]) -> str:
    # Different original scoring/provenance receipts are not identical
    # duplicates. Refuse a conflict instead of selecting by input order.
    return json.dumps(_leg_receipt(row),sort_keys=True,separators=(',',':'),allow_nan=False)


def build_episode_rows(calls: Iterable[Mapping[str,Any]],cluster_minutes: int=30,*,as_of_utc: Any=None) -> list[dict[str,Any]]:
    """Earliest-known topic/pair fanout; one deterministically selected topic per cluster."""
    cluster_minutes=positive_integer(cluster_minutes,'cluster_minutes')
    asof=aware_time(utc_now() if as_of_utc is None else as_of_utc,'as_of_utc').isoformat()
    grouped=defaultdict(list)
    for raw in calls:
        row=validate_retained_call(raw,as_of_utc=asof)
        topic=str(row.get('topic_id') or '').strip()
        if not topic:raise ValueError('topic_id_required')
        row['topic_id']=topic
        row['category']=str(row.get('category') or 'unclassified').strip() or 'unclassified'
        grouped[(row['horizon_minutes'],topic)].append(row)
    topics=[]
    for (horizon,topic),all_rows in sorted(grouped.items()):
        earliest=min(row['signal_epoch'] for row in all_rows)
        current=[row for row in all_rows if row['signal_epoch']==earliest]
        pairs={}
        duplicate_count=0
        for row in sorted(current,key=lambda r:(r['pair'],_leg_identity(r))):
            pair=row['pair']
            if pair in pairs:
                if _leg_identity(row)!=_leg_identity(pairs[pair]):raise ValueError('conflicting_same_clock_topic_pair_legs')
                duplicate_count+=1
                continue
            pairs[pair]=row
        legs=[pairs[pair] for pair in sorted(pairs)]
        categories={str(row.get('category') or 'unclassified') for row in legs}
        if len(categories)!=1:raise ValueError('same_topic_clock_has_conflicting_categories')
        maturity=max(row['actual_exit_epoch'] for row in legs)
        identity={'contract':REACTION_CONTRACT,'horizon_minutes':horizon,'topic_id':topic,'signal_epoch':earliest,'pairs':sorted(pairs),'category':next(iter(categories))}
        topics.append({**identity,'signal_utc':datetime.fromtimestamp(earliest,timezone.utc).isoformat(),
          'category':next(iter(categories)),'pair_legs':len(legs),'retained_pairs':sorted(pairs),
          'follow_net_bps':math.fsum(row['follow_net_bps'] for row in legs)/len(legs),
          'fade_net_bps':math.fsum(row['fade_net_bps'] for row in legs)/len(legs),
          'return_unit':'bps','bps_denominator':'equal_weight_common_entry_mid_notional_pair_legs',
          'actual_exit_epoch':maturity,'outcome_maturity_epoch':maturity,
          'outcome_time':datetime.fromtimestamp(maturity,timezone.utc).isoformat(),
          'nominal_target_utc':datetime.fromtimestamp(earliest+horizon*60,timezone.utc).isoformat(),
          'later_fanout_rows_excluded':len(all_rows)-len(current),'identical_same_clock_pair_duplicates_excluded':duplicate_count,
          'leg_endpoint_receipts':[_leg_receipt(row) for row in legs],
          'endpoint_evidence_sha256':_endpoint_evidence_hash(legs),
          'topic_identity_sha256':hashlib.sha256(json.dumps(identity,sort_keys=True,separators=(',',':')).encode()).hexdigest()})
    output=[]
    for horizon in sorted({r['horizon_minutes'] for r in topics}):
        candidates=sorted((r for r in topics if r['horizon_minutes']==horizon),key=lambda r:(r['signal_epoch'],r['topic_id']))
        cluster_start=-math.inf
        for row in candidates:
            if row['signal_epoch']>cluster_start+cluster_minutes*60:
                cluster_start=row['signal_epoch']
                output.append({**row,'episode_id':f'H{horizon}-{row["topic_identity_sha256"][:20]}',
                  'cluster_policy':f'first_topic_clock_then_lexicographic_topic_id;later_topics_within_inclusive_{cluster_minutes}m_window_excluded',
                  'cluster_minutes':cluster_minutes,'pair_fanout_policy':'earliest_topic_signal_clock_only;unique_pair_equal_notional',
                  'research_only':True,'account_eligible':False})
    return output


def surprise_coverage(database_path: Path) -> dict[str,Any]:
    path=Path(database_path)
    if not path.is_file():return {'status':'database_missing','topics':0,'coverage':{},'coverage_semantics':'field_presence_only_not_asof_consensus_or_surprise_evidence'}
    connection=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)
    rows=connection.execute('SELECT payload_json FROM topic_events').fetchall();connection.close()
    counts={label:0 for label in SURPRISE_FIELDS};complete=0;invalid_payload_rows=0;mapping_payload_rows=0
    for (text,) in rows:
        try:payload=json.loads(text)
        except (TypeError,ValueError,json.JSONDecodeError):invalid_payload_rows+=1;continue
        if not isinstance(payload,Mapping):invalid_payload_rows+=1;continue
        mapping_payload_rows+=1
        present={label:any(payload.get(name) not in (None,'') for name in names) for label,names in SURPRISE_FIELDS.items()}
        for label in present:counts[label]+=int(present[label])
        complete+=int(present['actual'] and present['consensus'])
    total=len(rows)
    return {'status':'field_presence_only','topics':total,
      'mapping_payload_rows':mapping_payload_rows,'malformed_or_nonmapping_payload_rows':invalid_payload_rows,
      'coverage_semantics':'field_presence_only_not_asof_consensus_or_surprise_evidence',
      'causal_consensus_availability_verified':False,'numeric_surprise_verified':False,
      'actual_and_consensus_topics':complete,'actual_and_consensus_coverage':round(complete/total,6) if total else 0.0,
      'coverage':{k:round(v/total,6) if total else 0.0 for k,v in counts.items()}}


def _mean_and_iid_selection_heuristic(values: list[float]) -> tuple[float|None,float|None]:
    if not values:return None,None
    mean=math.fsum(values)/len(values)
    if len(values)<2:return mean,mean
    variance=math.fsum((value-mean)**2 for value in values)/(len(values)-1)
    return mean,mean-1.645*math.sqrt(variance/len(values))


def metrics(rows: Iterable[Mapping[str,Any]],action_by_category: Mapping[str,str]) -> dict[str,Any]:
    values=[];actions=defaultdict(int);available=0
    for row in rows:
        available+=1
        action=str(action_by_category.get(str(row.get('category')),'no_trade'))
        if action not in ('follow','fade','no_trade'):raise ValueError('unknown_category_action')
        actions[action]+=1
        if action!='no_trade':values.append(finite_number(row.get(action+'_net_bps'),action+'_net_bps'))
    mean,heuristic=_mean_and_iid_selection_heuristic(values)
    return {'available_episodes':available,'episodes':len(values),'return_unit':'bps',
      'average_net_bps':mean,'total_net_bps':math.fsum(values),
      'win_rate':sum(value>0 for value in values)/len(values) if values else None,
      'iid_style_lower_selection_heuristic_bps':heuristic,
      'uncertainty_scope':'IID-style selection-score heuristic only; overlapping horizons/topics are not independent confidence or promotion evidence',
      'actions':dict(sorted(actions.items()))}


def _learn_category_actions(rows: list[Mapping[str,Any]],minimum_category_episodes: int,threshold_bps: float) -> dict[str,str]:
    grouped=defaultdict(list)
    for row in rows:grouped[str(row.get('category') or 'unclassified')].append(row)
    output={}
    for category,group in sorted(grouped.items()):
        if len(group)<minimum_category_episodes:output[category]='no_trade';continue
        follow=math.fsum(finite_number(r.get('follow_net_bps'),'follow_net_bps') for r in group)/len(group)
        fade=math.fsum(finite_number(r.get('fade_net_bps'),'fade_net_bps') for r in group)/len(group)
        output[category]=('follow' if follow>=fade else 'fade') if max(follow,fade)>=threshold_bps else 'no_trade'
    return output


def _validated_episode_rows(rows: list[dict[str,Any]],*,as_of_utc: Any) -> list[dict[str,Any]]:
    """Reproduce every fitted episode from complete retained endpoint evidence.

    Original scoring cutoffs are validated and preserved in every leg. The
    current validation clock is separate and cannot manufacture old labels.
    Excluded calls are not retained in a selected episode, so their original
    builder counts remain labelled metadata rather than re-proved evidence.
    """
    asof=aware_time(as_of_utc,'as_of_utc').isoformat();ordered=[]
    horizons=set();cluster_windows=set();all_legs=[]
    scalar_fields=('contract','horizon_minutes','topic_id','category','pair_legs','retained_pairs','pairs',
      'return_unit','bps_denominator','topic_identity_sha256','episode_id','cluster_policy',
      'cluster_minutes','pair_fanout_policy','endpoint_evidence_sha256')
    numeric_fields=('signal_epoch','actual_exit_epoch','outcome_maturity_epoch','follow_net_bps','fade_net_bps')
    clock_fields=('signal_utc','nominal_target_utc','outcome_time')
    for row in rows:
        if not isinstance(row,Mapping) or row.get('contract')!=REACTION_CONTRACT:
            raise ValueError('new_bps_episode_contract_required')
        horizon=positive_integer(row.get('horizon_minutes'),'horizon_minutes');horizons.add(horizon)
        positive_integer(row.get('pair_legs'),'pair_legs')
        cluster=positive_integer(row.get('cluster_minutes'),'cluster_minutes');cluster_windows.add(cluster)
        legs=row.get('leg_endpoint_receipts')
        if not isinstance(legs,list) or not legs or any(not isinstance(leg,Mapping) for leg in legs):
            raise ValueError('complete_nonempty_leg_endpoint_receipts_required')
        validated=[]
        for leg in legs:
            if any(field not in leg for field in LEG_RECEIPT_FIELDS):
                raise ValueError('missing_complete_leg_endpoint_receipt_field')
            computed=validate_retained_call(leg,as_of_utc=asof)
            for field in LEG_RECEIPT_FIELDS:
                if field in ('signal_utc','entry_utc','nominal_target_utc','exit_utc','as_of_utc'):
                    matches=aware_time(leg[field],field)==aware_time(computed[field],field)
                else:
                    matches=leg[field]==computed[field]
                if not matches:raise ValueError('retained_leg_claim_mismatch:'+field)
            validated.append(computed)
        if len({leg['pair'] for leg in validated})!=len(validated):
            raise ValueError('unique_retained_pair_legs_required')
        if len({leg['signal_epoch'] for leg in validated})!=1:
            raise ValueError('retained_fanout_must_share_earliest_signal_clock')
        if any(leg['horizon_minutes']!=horizon for leg in validated):
            raise ValueError('retained_leg_horizon_mismatch')
        rebuilt=build_episode_rows(validated,cluster_minutes=cluster,as_of_utc=asof)
        if len(rebuilt)!=1 or rebuilt[0]['pair_legs']!=len(validated):
            raise ValueError('retained_leg_fanout_does_not_reproduce_episode')
        expected=rebuilt[0]
        for field in scalar_fields:
            if row.get(field)!=expected[field]:
                raise ValueError('episode_aggregate_mismatch:'+field)
        for field in numeric_fields:
            if finite_number(row.get(field),field)!=expected[field]:
                raise ValueError('episode_aggregate_mismatch:'+field)
        for field in clock_fields:
            if aware_time(row.get(field),field)!=aware_time(expected[field],field):
                raise ValueError('episode_aggregate_mismatch:'+field)
        if row.get('research_only') is not True or row.get('account_eligible') is not False:
            raise ValueError('research_only_episode_required')
        counts={}
        for field in ('later_fanout_rows_excluded','identical_same_clock_pair_duplicates_excluded'):
            value=row.get(field)
            if isinstance(value,bool) or not isinstance(value,int) or value<0:
                raise ValueError('nonnegative_builder_exclusion_count_required:'+field)
            counts[field]=value
        ordered.append({**row,**expected,**counts,'validation_as_of_utc':asof,
          'excluded_call_count_scope':'original_builder_metadata;excluded_call_bytes_not_in_retained_episode'})
        all_legs.extend(validated)
    if len(horizons)>1:raise ValueError('fit_horizon_requires_one_horizon')
    if len(cluster_windows)>1:raise ValueError('fit_requires_one_cluster_policy')
    ordered.sort(key=lambda r:(r['signal_epoch'],str(r.get('episode_id'))))
    identities=[r.get('episode_id') for r in ordered]
    if any(not isinstance(value,str) or not value for value in identities) or len(set(identities))!=len(identities):
        raise ValueError('unique_nonempty_episode_id_required')
    if ordered:
        cohort=build_episode_rows(all_legs,cluster_minutes=next(iter(cluster_windows)),as_of_utc=asof)
        if [r['episode_id'] for r in cohort]!=identities:
            raise ValueError('retained_episode_set_violates_topic_fanout_or_cluster_selection')
    return ordered


def _partition_episodes(rows: list[dict[str,Any]],*,as_of_utc: Any) -> tuple[list,list,list,dict]:
    ordered=_validated_episode_rows(rows,as_of_utc=as_of_utc)
    clocks=sorted({r['signal_epoch'] for r in ordered})
    if len(clocks)<3:raise ValueError('three_distinct_episode_clocks_required')
    selection_start=clocks[max(1,min(len(clocks)-2,int(len(clocks)*.6)))]
    holdout_start=clocks[max(2,min(len(clocks)-1,int(len(clocks)*.8)))]
    train_raw=[r for r in ordered if r['signal_epoch']<selection_start]
    selection_raw=[r for r in ordered if selection_start<=r['signal_epoch']<holdout_start]
    train=[r for r in train_raw if r['outcome_maturity_epoch']<selection_start]
    selection=[r for r in selection_raw if r['outcome_maturity_epoch']<holdout_start]
    holdout=[r for r in ordered if r['signal_epoch']>=holdout_start]
    if not train or not selection or not holdout:raise ValueError('empty_split_after_strict_actual_maturity_purge')
    return train,selection,holdout,{'train':len(train),'selection':len(selection),'holdout':len(holdout),
      'train_purged':len(train_raw)-len(train),'selection_purged':len(selection_raw)-len(selection),
      'selection_start_epoch':selection_start,'holdout_start_epoch':holdout_start,
      'purge_policy':'actual_max_leg_exit_maturity_strictly_before_next_origin_block;no_nominal_fallback',
      'train_episode_ids':[r.get('episode_id') for r in train],'selection_episode_ids':[r.get('episode_id') for r in selection],
      'holdout_episode_ids':[r.get('episode_id') for r in holdout]}


def fit_horizon(rows: list[dict[str,Any]],minimum_episodes: int=60,minimum_category_episodes: int=8,*,as_of_utc: Any=None) -> dict[str,Any]:
    asof=aware_time(utc_now() if as_of_utc is None else as_of_utc,'as_of_utc').isoformat()
    base={'contract':REACTION_CONTRACT,'research_only':True,'development_only':True,'account_eligible':False,
      'episodes':len(rows),'as_of_utc':asof,'return_unit':'bps','threshold_grid_bps':list(THRESHOLDS_BPS),
      'threshold_units_policy':'New explicit bps grid; no conversion from old native-pip thresholds',
      'shadow_decision':'research_policy_only;independent_forward_evidence_required'}
    if not rows:return {**base,'status':'no_episodes','minimum_episodes':minimum_episodes}
    # Validate every available label even when support is insufficient.
    validated=_validated_episode_rows(rows,as_of_utc=asof)
    if len(validated)<minimum_episodes:
        return {**base,'status':'insufficient_news_episodes','minimum_episodes':minimum_episodes,
                'label_validation':'all_present_labels_validated;no_policy_fitted'}
    train,selection,holdout,split=_partition_episodes(validated,as_of_utc=asof)
    candidates=[]
    for threshold in THRESHOLDS_BPS:
        actions=_learn_category_actions(train,minimum_category_episodes,threshold)
        candidates.append({'threshold_bps':threshold,'actions':actions,'selection':metrics(selection,actions)})
    viable=[r for r in candidates if r['selection']['episodes']>=10 and r['selection']['average_net_bps']>0 and r['selection']['iid_style_lower_selection_heuristic_bps']>0]
    if not viable:return {**base,'status':'no_viable_actual_maturity_purged_selection_policy','split':split,'selection_candidates':candidates}
    selected=max(viable,key=lambda r:(r['selection']['iid_style_lower_selection_heuristic_bps'],-r['threshold_bps']))
    identity={'contract':REACTION_CONTRACT,'threshold_bps':selected['threshold_bps'],'actions':selected['actions']}
    policy_hash=hashlib.sha256(json.dumps(identity,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    final=metrics(holdout,selected['actions'])
    return {**base,'status':'selection_policy_frozen_reported','split':split,
      'selected_policy':selected,'selected_policy_sha256':policy_hash,'selection_candidates':candidates,
      'final_report_block':{**final,'report_only':True,'policy_frozen_before_evaluation':True,
        'historical_access_provenance':'previous_access_unknown_or_already_opened_development_period;not_untouched_holdout'},
      'final_block_role':'within_run_report_only;no_policy_filtering_ranking_retraining_or_promotion'}


def run_audit(replay_path: Path,news_database: Path,output_path: Path|None=None,minimum_episodes: int=60,*,as_of_utc: Any=None) -> dict[str,Any]:
    output_path=new_run_report_path(DEFAULT_REPORT) if output_path is None else Path(output_path)
    if output_path.exists():
        raise ValueError('refuse_overwriting_existing_news_report;legacy_reaction_report_targets_also_forbidden')
    asof=aware_time(utc_now() if as_of_utc is None else as_of_utc,'as_of_utc').isoformat()
    calls,replay_identity=_load_calls_snapshot(replay_path,as_of_utc=asof);episodes=build_episode_rows(calls,as_of_utc=asof)
    horizons={str(h):fit_horizon([r for r in episodes if r['horizon_minutes']==h],minimum_episodes=minimum_episodes,as_of_utc=asof) for h in HORIZONS_MIN}
    payload={'schema_version':3,'contract':REACTION_CONTRACT,'model':'category_follow_fade_exact_endpoint_bps_v2',
      'generated_at':utc_now(),'as_of_utc':asof,'research_only':True,'development_only':True,'account_eligible':False,'execution_adapter':False,
      'return_unit':'bps','endpoint_contract':ENDPOINT_CONTRACT,'threshold_grid_bps':list(THRESHOLDS_BPS),
      'pair_fanout_policy':'earliest_topic_signal_clock_only;unique_pair_equal_entry_mid_notional',
      'episode_maturity_policy':'maximum_actual_exit_of_retained_earliest_clock_legs',
      'replay':replay_identity['path'],'replay_sha256':replay_identity['sha256'],'replay_input_snapshot':replay_identity,
      'output_path':str(output_path.absolute()),'publication_contract':'immutable_new_run_no_overwrite',
      'calls':len(calls),'episodes':len(episodes),'episode_rows':episodes,
      'surprise_field_presence':surprise_coverage(news_database),'horizons':horizons,
      'historical_affected_runs_unknown':True,'legacy_pip_results_converted':False}
    publish_new_json_report(output_path,payload)
    return payload


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--replay',type=Path,required=True);parser.add_argument('--news-database',type=Path,required=True)
    parser.add_argument('--output',type=Path,default=None,help='Absent target path; default creates an immutable unique run report');parser.add_argument('--minimum-episodes',type=int,default=60)
    args=parser.parse_args();result=run_audit(args.replay,args.news_database,args.output,minimum_episodes=max(20,args.minimum_episodes))
    print(json.dumps({'output':result['output_path'],'episodes':result['episodes'],'contract':REACTION_CONTRACT},indent=2));return 0


if __name__=='__main__':raise SystemExit(main())
