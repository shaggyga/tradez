"""Describe fixed archived forecasts; never mark them strict/causal proof."""
from pathlib import Path
import collections
import datetime as dt
import hashlib
import json
import math
import statistics

OUT=Path(__file__).resolve().parent
SOURCE=OUT/'selected_candidate_rows.json'
raw=json.loads(SOURCE.read_text(encoding='utf-8'))
def epoch(value):return dt.datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
def iso(value):return dt.datetime.fromtimestamp(value,dt.timezone.utc).isoformat()
def mean(values):return statistics.fmean(values) if values else None
def write(name,payload):
    path=OUT/name;path.write_text(json.dumps(payload,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    return {'path':name,'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'bytes':path.stat().st_size}

groups=collections.defaultdict(list)
normalized=[]
for row in raw:
    p=json.loads(row['forecast_json']);outcome=row['outcome'];d=json.loads(outcome['diagnostics_json']) if outcome else {}
    reference=epoch(row['entry_time']);recorded=epoch(row['recorded_utc'])
    assert p['cohort_id'].startswith(row['family']+'.20260829.')
    assert hashlib.sha256(row['forecast_json'].encode()).hexdigest()==row['payload_sha256']
    assert epoch('2026-08-29T12:36:36.550804+00:00')<=reference<=epoch('2026-09-05T02:09:17+00:00')
    groups[row['entry_time']].append(row)
    normalized.append({'forecast_id':row['event_id'],'source_forecast_rowid':row['forecast_rowid'],'family':row['family'],'cohort_id':p['cohort_id'],'model_id':row['model_id'],'model_version':row['model_version'],'feature_version':row['feature_version'],'forecast_contract_version':p.get('forecast_contract_version'),'cost_model_version':p.get('cost_model_version'),'instrument':row['instrument'],'input_timeframe':p.get('input_timeframe'),'horizon_sec':3600,'reference_time_utc':row['entry_time'],'target_time_utc':iso(reference+3600),'issued_utc':None,'published_utc':None,'committed_utc':None,'recorded_utc':row['recorded_utc'],'recorded_clock_semantics':'preflush pending-list append timestamp, not commit or first visibility','data_cutoff_utc':row['data_cutoff_utc'],'features_available_utc':None,'training_cutoff_utc':p.get('training_cutoff_utc'),'training_dataset_sha256':p.get('training_dataset_sha256'),'max_training_label_maturity_utc':None,'max_training_label_available_utc':None,'probability_up':row['probability_up'],'expected_signed_pips':p.get('expected_signed_pips'),'predicted_magnitude_pips':row['predicted_magnitude_pips'],'direction':row['direction'],'pip':row['pip'],'entry_quote':{'bid':row['entry_bid'],'ask':row['entry_ask'],'provider_utc':p.get('provider_quote_time'),'received_utc':None,'source_snapshot_reference_utc':row['entry_time'],'postpublication_executable':False},'outcome':{'source_outcome_rowid':outcome['row_id'],'observed_utc':outcome['observed_utc'],'quote_received_utc':outcome['exit_time'],'provider_utc':d.get('provider_quote_time'),'bid':outcome['exit_bid'],'ask':outcome['exit_ask'],'stored_horizon_anchor_utc':row['recorded_utc'],'stored_target_time_utc':iso(recorded+3600),'actual_lateness_from_original_target_sec':epoch(outcome['exit_time'])-reference-3600,'actual_lateness_from_recorded_target_sec':epoch(outcome['exit_time'])-recorded-3600,'theoretical_pips_stored':outcome['theoretical_pips'],'diagnostics':d} if outcome else None,'integrity_events':row['integrity_events'],'payload_sha256':row['payload_sha256'],'payload_verified':True,'strict_eligible':False,'strict_blockers':['missing_forecast_issue_visibility_and_commit_certificates','missing_feature_first_availability_certificate','missing_training_label_availability_certificate','entry_quote_precedes_recorded_forecast','stored_target_shifted_to_recorded_clock']})

for item in normalized:
    outcome=item['outcome']
    if outcome is None:
        item['strict_blockers'][-1]='outcome_missing'
        continue
    canonical=outcome['diagnostics'].get('maturity_worker')=='canonical_quote_snapshot_v1'
    outcome['stored_exit_time']=outcome['quote_received_utc']
    outcome['producer_clock_contract']='canonical_local_snapshot_recorded_relative' if canonical else 'legacy_recovery_provider_quote_time'
    outcome['stored_exit_clock_minus_original_target_sec']=outcome.pop('actual_lateness_from_original_target_sec')
    outcome['stored_exit_clock_minus_recorded_target_sec']=outcome.pop('actual_lateness_from_recorded_target_sec')
    if not canonical:
        outcome['provider_utc']=outcome['stored_exit_time']
        outcome['quote_received_utc']=None
        outcome['stored_horizon_anchor_utc']=None
        outcome['stored_target_time_utc']=None
        item['strict_blockers'][-1]='legacy_outcome_lacks_local_quote_availability_clock'

paired=[];exclusions=collections.Counter()
for reference,rows in sorted(groups.items()):
    if len(rows)!=4:
        exclusions['not_all_four_families']+=1;continue
    if not all(r['outcome'] for r in rows):
        exclusions['missing_one_or_more_outcomes']+=1;continue
    if len({(r['entry_bid'],r['entry_ask'],r['pip']) for r in rows})!=1:
        exclusions['different_entry_quotes_or_pip']+=1;continue
    if len({(r['outcome']['exit_time'],r['outcome']['exit_bid'],r['outcome']['exit_ask']) for r in rows})!=1:
        exclusions['different_endpoint_clock_or_quote']+=1;continue
    paired.append((reference,rows))

metrics={}
for family in sorted({r['family'] for r in raw}):
    observations=[]
    for reference,rows in paired:
        r=next(x for x in rows if x['family']==family);p=json.loads(r['forecast_json']);o=r['outcome'];pip=r['pip']
        entrymid=(r['entry_bid']+r['entry_ask'])/2;exitmid=(o['exit_bid']+o['exit_ask'])/2
        actual=(exitmid-entrymid)/pip;probability=r['probability_up'];expected=p['expected_signed_pips'];predsign=1 if r['direction']=='buy' else -1
        net=((o['exit_bid']-r['entry_ask']) if predsign==1 else (r['entry_bid']-o['exit_ask']))/pip
        assert abs(net-o['theoretical_pips'])<0.000101
        observations.append({'reference_time_utc':reference,'forecast_id':r['event_id'],'actual_mid_signed_pips':actual,'probability_up':probability,'expected_signed_pips':expected,'label_up':int(actual>0),'gross_direction_hit_including_flats':predsign*actual>0,'net_bid_ask_pips':net,'brier':(probability-int(actual>0))**2,'signed_move_absolute_error_pips':abs(expected-actual),'zero_move_absolute_error_pips':abs(actual),'spread_drag_pips':predsign*actual-net,'target_lateness_sec':epoch(o['exit_time'])-epoch(reference)-3600})
    n=len(observations)
    metrics[family]={'n_same_paired_endpoints':n,'brier_up_vs_nonup':mean([r['brier'] for r in observations]),'constant_probability_0_5_brier':.25 if n else None,'brier_difference_model_minus_constant':mean([r['brier']-.25 for r in observations]),'gross_direction_accuracy_flats_miss':mean([float(r['gross_direction_hit_including_flats']) for r in observations]),'signed_move_mae_pips':mean([r['signed_move_absolute_error_pips'] for r in observations]),'zero_move_mae_pips':mean([r['zero_move_absolute_error_pips'] for r in observations]),'signed_move_mae_difference_model_minus_zero':mean([r['signed_move_absolute_error_pips']-r['zero_move_absolute_error_pips'] for r in observations]),'mean_net_bid_ask_pips':mean([r['net_bid_ask_pips'] for r in observations]),'positive_net_bid_ask_rate':mean([float(r['net_bid_ask_pips']>0) for r in observations]),'mean_net_bid_ask_minus_additional_0_25pip_slippage':mean([r['net_bid_ask_pips']-.25 for r in observations]),'no_trade_mean_net_pips':0.0,'model_coverage_on_paired_set':1.0,'no_trade_coverage':0.0,'mean_spread_drag_pips':mean([r['spread_drag_pips'] for r in observations]),'observations':observations}

common_first=next(iter(metrics.values()))['observations'] if metrics else []
dates=collections.Counter(x['reference_time_utc'][:10] for x in common_first)
score={'schema':'fixed_forecast_archival_diagnostic_v1','generated_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'classification':'already_inspected_archive_diagnostic_not_strict_causal_or_tradable_proof','strict_eligible':False,'source_extract':{'path':SOURCE.name,'sha256':hashlib.sha256(SOURCE.read_bytes()).hexdigest()},'fixed_population':{'instrument':'EUR_USD','horizon_sec':3600,'families':sorted(metrics),'forecast_universe_rows':len(raw),'original_reference_epochs':len(groups),'complete_matched_endpoint_epochs':len(paired),'exclusion_epoch_counts':dict(exclusions),'retained_utc_day_counts':dict(dates),'outcome_timing':'Stored target is original recorded_utc +1h, while original reference and entry quote precede it; no clock substitution or retimed target repair was attempted.','fitting_or_parameter_tuning':False},'paired_outcome_class_balance':{'up':sum(x['actual_mid_signed_pips']>0 for x in common_first),'down':sum(x['actual_mid_signed_pips']<0 for x in common_first),'flat':sum(x['actual_mid_signed_pips']==0 for x in common_first)},'metric_definitions':{'direction':'Forecast direction times actual midpoint change >0; flats miss.','brier':'(original frozen probability_up - 1[midpoint change>0])**2. Flat counts as non-up for this binary label.','signed_move_mae':'Absolute error between original expected_signed_pips and actual signed midpoint move.','net_bid_ask':'Buy:exitbid-entryask; sell:entrybid-exitask, divided by0.0001. Historical snapshotquotes predatingforecastrecording; not a realizable trade result.','additional_slippage':'Fixed illustrative0.25pip subtraction from quote-net, matching named cohort cost assumption; not measured execution slippage.','baseline_rolling_class_rate':'Unavailable; historical label availability not established.','no_trade':'Zero net and zero coverage; not a direction forecast.','dependence':'368 overlapping issueepochs from6 UTCdays. No independent-sample significance, confidence interval or prospective promotion claim.'},'family_metrics':metrics}
score['fixed_population']['outcome_timing']='Mixed retained producers: canonical_quote_snapshot_v1 targets recorded_utc+1h and reports local snapshot exit time; legacy recovered results use entry-relative age and report provider quote time, with local receipt missing. No clock substitution or target repair was attempted.'
score['fixed_population']['retained_endpoint_producer_counts']=dict(collections.Counter('canonical_quote_snapshot_v1' if json.loads(rows[0]['outcome']['diagnostics_json']).get('maturity_worker')=='canonical_quote_snapshot_v1' else 'legacy_recovered_provider_quote_clock' for _,rows in paired))
for _,rows in paired:
    assert len({json.loads(r['outcome']['diagnostics_json']).get('maturity_worker','legacy_recovered_provider_quote_clock') for r in rows})==1
normal_binding=write('normalized_forecasts.json',{'schema':'fixed_forecast_archive_normalized_v1','rows':normalized})
score_binding=write('archival_scorecard.json',score)
write('archive_product_bindings.json',{'normalized':normal_binding,'scorecard':score_binding})
print(json.dumps({'normalized':normal_binding,'scorecard':score_binding,'population':score['fixed_population'],'classes':score['paired_outcome_class_balance'],'metrics':{k:{a:b for a,b in v.items() if a!='observations'} for k,v in metrics.items()}},indent=2))
