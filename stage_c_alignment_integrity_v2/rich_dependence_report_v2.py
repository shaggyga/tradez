"""Read original forecasts, form paired time panels, record every current attempt."""
from collections import Counter,defaultdict
from datetime import datetime,timezone
import math
from contracts import fingerprint
from paired_blocks_v2 import paired_origins,describe

def build(reader,configuration):
    c=reader.read('family','experiment_contract.json');base=reader.read('baseline','forecasts.json');base_scores=reader.read('baseline','scores.json')
    current_scores=reader.read('family','scores.json');outcomes=[]
    for pair in configuration['universe']:outcomes.extend(reader.read('rich','outcomes_'+pair+'.json')['outcomes'])
    indexed=defaultdict(list)
    for row in base:indexed[(row['forecast']['target_id'],row['procedure'],row['method'])].append(row)
    panels=[];diagnostics=[];scopes=[];count=0
    for group in c['groups']:
        for minutes in c['horizon_minutes']:
            target=f'technical_endpoint_midpoint_elapsed_{minutes}m'
            for procedure in c['procedures']:
                name=f'forecasts_{group}_{minutes}_{procedure}.json';rows=reader.read('family',name)
                for method in c['methods']:
                    selected=[r for r in rows if r['method']==method]
                    original_score=next(x for x in current_scores if (x['group'],x['target_id'],x['procedure'],x['method'])==(group,target,procedure,method))
                    for control in ('zero','history_mean','ridge','recovered_hgb'):
                        matched=indexed[(target,procedure,control)];p,scope=paired_origins(selected,matched,outcomes,asof=c['evaluation_asof'])
                        b=next(x for x in base_scores if (x['target_id'],x['procedure'],x['method'])==(target,procedure,control))
                        if scope['paired_support_sha256']!=original_score['support_sha256'] or scope['paired_support_sha256']!=b['support_sha256'] or scope['raw_paired_rows']!=original_score['rows']:raise ValueError('original_scoring_support_drift')
                        n=scope['raw_paired_rows']
                        if n==0:raise ValueError('declared_mature_comparison_support_missing')
                        actual={'mae_bps':math.fsum(x['absolute_error_delta_sum'] for x in p)/n,'mse_bps2':math.fsum(x['squared_error_delta_sum'] for x in p)/n}
                        for key,value in actual.items():
                            if not math.isclose(value,original_score[key]-b[key],rel_tol=1e-12,abs_tol=1e-8):raise ValueError('paired_scalar_score_reconciliation_failed')
                        key={'group':group,'target_id':target,'procedure':procedure,'method':method,'control':control}
                        count+=1;panels.extend({**key,**x} for x in p)
                        dates=sorted({datetime.fromtimestamp(x,timezone.utc).date().isoformat() for x in scope['origin_epochs']})
                        scopes.append({**key,**scope,'utc_dates':dates,'utc_date_count':len(dates),'paired_deltas':actual,'forecast_source':reader.source('family',name,selected),'baseline_source':reader.source('baseline','forecasts.json',matched)})
                        for length in configuration['block_lengths']:diagnostics.append({**key,**describe(p,length,replicates=configuration['replicates'],seed=configuration['seed'])})
    if count!=336:raise ValueError('all_84_by_four_control_comparisons_required')
    attempts=[]
    for meta in reader.read('family','model_inventory.json'):
        for method in c['methods']:
            attempts.append({'status':'new_fixed_candidate_fit','group':meta['group'],'method':method,'target_id':meta['target']['target_id'],'fit_cutoff':meta['fit_cutoff'],'ready_epoch':meta['ready_epoch'],'model_id':fingerprint({'fit_id':meta['fit_id'],'method':method}),
                'fit_id':meta['fit_id'],'model_sha256':meta['model_sha256'][method],'training_population_sha256':meta['training_population_sha256'],'training_rows':meta['training_rows'],'transform_sha256':meta['fitted_transform']['sha256'],
                'raw_width':meta['fitted_transform']['raw_width'],'transformed_width':meta['fitted_transform']['transformed_width'],'selected':False})
    for meta in reader.read('baseline','model_inventory.json'):
        for method in ('ridge','recovered_hgb'):
            attempts.append({'status':'reused_baseline_fit','group':'legacy26','method':method,'target_id':meta['target']['target_id'],'fit_cutoff':meta['fit_cutoff'],'ready_epoch':meta['ready_epoch'],
                'model_id':fingerprint({'fit_id':meta['fit_id'],'method':method}),'fit_id':meta['fit_id'],'training_population_sha256':meta['training_population_sha256'],'training_rows':meta['training_rows'],'selected':False})
    if len(attempts)!=112 or len({a['model_id'] for a in attempts})!=112:raise ValueError('exact_current_attempt_inventory_required')
    lineage=reader.read('rich','lineage_summary.json')
    ledger={'scope':'this_matched_three_group_comparison_and_reused_baseline; not_an_exhaustive_global_historical_registry','new_fixed_model_fits':84,'reused_baseline_fits':28,'model_fits_performed_by_this_report':0,
        'reused_non_estimator_controls':['zero','history_mean_of_same_mature_prefix'],'forecast_procedures':c['procedures'],
        'attempts':attempts,'prior_lineage_source':reader.source('rich','lineage_summary.json',lineage),'prior_lineage':lineage,
        'engineering_attempts':'source/run failures and superseded recipes remain in checkpoint WORK_LOG and findings; restore repetitions are not new candidates',
        'selection_limitations':['later2026compact_schema_choices_on_inspected2024dates','repeated_development_inspection','no_parameter_search_in_this_fixed_comparison','no_winner_selected'],
        'unresolved_global_inventory':'missing_exact_sequence_runs_and_wider_historical_families_remain_unverified; D_drive_deferred'}
    report={'status':'verified_paired_development_diagnostics','paired_comparisons':count,'new_score_groups':84,'controls_per_group':4,'origin_panel_rows':len(panels),'block_sensitivity_rows':len(diagnostics),'models_fitted':0,
        'universe_currency_incidence_not_exposure':dict(sorted(Counter(currency for pair in configuration['universe'] for currency in pair.split('_')).items())),
        'assessment_origin_counts':sorted({s['unique_origins'] for s in scopes}),'assessment_date_counts':sorted({s['utc_date_count'] for s in scopes}),
        'distinct_actual_origin_grids':len({fingerprint(s['origin_epochs']) for s in scopes}),
        'origin_support_status':'target_specific_mature_rows; missing_rows_not_zero_filled; no_cross_target_joint_interval',
        'whole_window_interval_status':'insufficient_distinct_time_blocks','independent_observations_claimed':False,'effective_sample_size':None,'selected_model':None,
        'limitations':['five_inspected_development_dates','related_currencies_not_resampled_independently','overlapping_multiday_targets_exceed_short_blocks','moving_block_edge_weighting_and_block_length_sensitivity','later_research_schema_selection','descriptive_ranges_are_not_confirmatory_confidence_intervals'],
        'engineering_ready':False,'forecast_evidence_status':'paired_dependent_inspected_development_only','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    return {'paired_origin_panels.json':panels,'paired_comparison_scope.json':scopes,'block_sensitivity.json':diagnostics,'attempt_ledger.json':ledger,'run_report.json':report}
