"""Native completed-M1 forecast diagnostics; no executable or account returns.

The fixed ledger independently replays retained source bytes before exporting.
This pure evaluator verifies values and full-denominator accounting; a caller's
JSON assertion is not an independently attested live publication or source read.
"""
from collections import Counter
from datetime import datetime
from decimal import Decimal
import math
import re
import native_m1_outcome_v1 as outcome

EVALUATOR_VERSION='native_joint_exact_m1_evaluation_v1_20260913'
SCHEMA='native_joint_forecast_evaluation_input_v1_20260913'
PROTOCOL_SCHEMA='native_joint_forecast_evaluation_protocol_v1_20260913'
FAMILY='ridge_price_news_v1'
COMPARATORS={'matched_price_only':'matched_price_only_expected_pips',
             'neutral_news_ablation':'neutral_news_ablation_expected_pips'}
need=outcome.need
encoded=outcome.encode
digest=outcome.digest


def finite(value):return outcome.native.clock(value,'native_evaluation_number')


def epoch(value):
    need(type(value) is str and len(value)<=64,'native_utc_required')
    dt=datetime.fromisoformat(value.replace('Z','+00:00'))
    need(dt.tzinfo is not None,'native_timezone_required')
    return finite(dt.timestamp())


def validate_pair_metadata(value):
    pair=outcome._pair(value.get('instrument'))
    text=value.get('pip_size')
    need(type(text) in (str,int,float) and len(str(text))<=32,'native_registered_pip_required')
    pip=Decimal(str(text));need(pip in (Decimal('.0001'),Decimal('.001'),Decimal('.01')),'native_registered_pip_required')
    return pair,pip


def validate_cohorts(cohorts,instrument,family=None):
    need(type(cohorts) is dict and set(cohorts)=={FAMILY} and (family is None or family==FAMILY),'native_single_family_required')
    prefix='joint_price_news_native_v1_20260913.'+instrument+'.'+FAMILY
    name=cohorts[FAMILY]
    need(type(name) is str and len(name)<=256 and (name==prefix or name.startswith(prefix+'.')),'distinct_native_cohort_required')
    return FAMILY


def validate_protocol(protocol):
    need(type(protocol) is dict and protocol.get('schema_version')==PROTOCOL_SCHEMA,'native_protocol_schema')
    from native_m1_ledger_v1 import validate_policy
    validate_policy(protocol.get('native_outcome_policy'))
    pair,pip=validate_pair_metadata(protocol);validate_cohorts(protocol.get('cohorts'),pair,protocol.get('family'))
    need(type(protocol.get('horizon_sec')) is int and protocol['horizon_sec']==3600 and
         protocol.get('input_timeframe')=='M1','native_fixed_h1_required')
    for flag in ('proof_eligible','account_eligible','collection_enabled'):
        need(protocol.get(flag) is False,'native_offline_protocol_flags')
    need(protocol.get('native_outcome_recipe')==outcome.SCORE_RECIPE and
         protocol.get('native_target_recipe')=='exact_completed_M1_close_at_origin_bar_start_plus_3600',
         'native_original_target_recipe_required')
    need(protocol.get('baselines')==['fair_coin','zero_move','no_trade'],'native_declared_baselines')
    need(protocol.get('executable_quote_policy') is None,'native_executable_policy_separate')
    need(epoch(protocol['historical_start_utc'])<epoch(protocol['historical_end_utc']),'native_evaluation_window')
    for key,expected in (('maximum_input_age_sec',900),('maximum_news_age_sec',300),('quote_max_age_sec',60),
        ('maximum_entry_delay_sec',60),('maximum_target_quote_delay_sec',60)):
        need(type(protocol.get(key)) is int and protocol[key]==expected,'native_inherited_input_tolerance')


def forecast_errors(forecast,decision,protocol):
    errors=[]
    try:
        anchor=outcome.anchor_value(forecast['native_anchor']).as_dict();sha=digest(anchor)
        need(type(decision.get('decision_id')) is str and bool(decision['decision_id']) and
             forecast.get('forecast_id')==decision['decision_id']+':'+FAMILY,'native_forecast_id_binding')
        need(forecast.get('native_anchor_sha256')==decision.get('native_anchor_sha256')==sha,'native_anchor_hash')
        for key,expected in (('instrument',protocol['instrument']),('horizon_sec',3600),('input_timeframe','M1'),
            ('family',FAMILY),('cohort_id',protocol['cohorts'][FAMILY]),('model_version',protocol['model_version']),
            ('feature_version',protocol['feature_version'])):
            need(encoded(forecast.get(key))==encoded(expected),'native_identity:'+key)
        _,pip=validate_pair_metadata(forecast)
        need(float(pip).hex()==anchor['pip_size_hex'] and anchor['instrument']==protocol['instrument'],'native_anchor_pair_pip')
        need(forecast['reference_epoch']==decision['reference_epoch']==anchor['origin_close_epoch'] and
             forecast['target_epoch']==decision['target_epoch']==anchor['target_close_epoch'],'native_immutable_target')
        need(type(forecast['reference_mid']) is str and float(forecast['reference_mid']).hex()==anchor['origin_mid_hex'],
             'native_original_mid_bits')
        need(type(forecast['probability_up']) is float and forecast['probability_up'].hex()==anchor['native_probability_up_hex']
             and type(forecast['side']) is int and forecast['side']==anchor['native_expected_delta_side'],'native_probability_delta_semantics')
        bps=10000*float.fromhex(anchor['expected_signed_pips_hex'])*float(pip)/float.fromhex(anchor['origin_mid_hex'])
        need(type(forecast['predicted_return_bps']) is float and forecast['predicted_return_bps'].hex()==bps.hex(),'native_bps_representation')
        need(forecast['probability_event']=='strict_native_target_close_above_original_origin_close'
             and decision['reference_quote_role']=='current_context_not_native_model_anchor','native_explicit_context_role')
        issued=finite(forecast['issued_epoch']);committed=finite(forecast['committed_available_epoch'])
        observed=finite(forecast['input_source_observed_epoch']);started=finite(forecast['computation_started_epoch'])
        completed=finite(forecast['computation_completed_epoch']);maturity=finite(forecast['feature_cutoff_epoch'])
        need(issued==anchor['issued_epoch'] and started==anchor['computation_started_epoch'] and
             completed==anchor['computation_completed_epoch'] and maturity==anchor['origin_close_epoch'],
             'native_anchor_computation_clock_binding')
        need(epoch(protocol['historical_start_utc'])<observed==anchor['feature_decision_epoch']<=started<=completed<issued
             <=committed<anchor['target_close_epoch'],'native_publication_availability_order')
        need(finite(decision['attempt_epoch'])<=started and maturity<=observed and issued-maturity<=900,
             'native_input_age_or_attempt_clock')
        need(forecast['features_available_epoch']==forecast['training_labels_available_max_epoch']==completed,
             'native_model_availability_binding')
        training=[finite(forecast[k]) for k in ('training_news_available_max_epoch',
            'training_news_feature_cutoff_max_epoch','training_label_maturity_max_epoch')]
        need(0<training[0]<=training[1]<=training[2]<=maturity,'native_training_maturity')
        for k in ('training_news_available_max_epoch','training_news_feature_cutoff_max_epoch','training_label_maturity_max_epoch'):
            need(finite(forecast['diagnostics'][k])==finite(forecast[k]),'native_training_diagnostic_binding')
        news=[finite(forecast[k]) for k in ('news_evidence_epoch','news_generated_epoch','news_first_observed_epoch','news_available_epoch')]
        need(0<news[0]<=news[1]<=news[2]<=news[3]<=observed and 0<=issued-news[0]<=300
             and issued<=finite(forecast['news_expires_epoch']),'native_news_clock_order')
        need(forecast['news_capture_sha256']==decision['news_capture_sha256'] and
             re.fullmatch('[0-9a-f]{64}',forecast['news_capture_sha256']),'native_news_capture_binding')
        for key in COMPARATORS.values():
            value=forecast['diagnostics'][key]
            need(type(value) in (int,float) and math.isfinite(value),'native_finite_preissue_comparator')
        for value in (forecast,decision):
            need(value.get('research_only') is True and all(value.get(k) is False for k in
                ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible')),'native_inert_forecast')
    except (KeyError,ValueError,TypeError,OverflowError,AttributeError) as exc:
        errors.append(type(exc).__name__+':'+str(exc))
    return errors


def validate_exact_target(target,anchor):
    """Validate native representation, without pretending hashes attest a CSV."""
    need(type(target) is dict and set(target)=={'status','target_bar_start_epoch','target_close_epoch',
        'target_mid_hex','target_row_evidence','source_capture_sha256','source_read_completed_epoch',
        'source_validated_epoch','original_origin_preserved','later_origin_present','later_origin_equals_original'},
        'native_exact_target_shape')
    need(target['status']=='exact_target' and type(target['target_bar_start_epoch']) is int and
         type(target['target_close_epoch']) is int and target['target_bar_start_epoch']==anchor['target_bar_start_epoch']
         and target['target_close_epoch']==anchor['target_close_epoch'],'native_exact_target_clock_binding')
    need(target['original_origin_preserved'] is True and type(target['later_origin_present']) is bool,
         'native_original_origin_preserved')
    need((type(target['later_origin_equals_original']) is bool if target['later_origin_present']
          else target['later_origin_equals_original'] is None),'native_origin_revision_status')
    text=target['target_mid_hex'];need(type(text) is str and len(text)<=40,'native_target_canonical_hex')
    value=float.fromhex(text);need(math.isfinite(value) and value>0 and value.hex()==text,'native_target_canonical_hex')
    row=target['target_row_evidence']
    need(type(row) is dict and set(row)=={'close_lexeme','close_hex','row_sha256','complete_flag'},'native_target_row_shape')
    need(row['close_hex']==text and type(row['close_lexeme']) is str and 0<len(row['close_lexeme'])<=4*1024**2
         and float(row['close_lexeme']).hex()==text,'native_target_row_value_binding')
    need(row['complete_flag'] in ('explicit_true','absent_clock_completion_only'),'native_target_completion_evidence')
    for sha in (row['row_sha256'],target['source_capture_sha256']):
        need(type(sha) is str and re.fullmatch('[0-9a-f]{64}',sha),'native_target_sha256_identity')
    need(anchor['target_close_epoch']<=finite(target['source_read_completed_epoch'])
         <=finite(target['source_validated_epoch']),'native_target_source_read_clock')
    return value


def evaluate(dataset,protocol):
    validate_protocol(protocol)
    need(dataset.get('schema_version')==SCHEMA,'native_dataset_schema')
    publications=dataset['publications'];statuses=dataset['native_scores']
    need(type(publications) is list and type(statuses) is list and len(publications)==len(statuses)<=65536,
         'native_complete_forecast_denominator')
    cutoff=finite(dataset['observed_cutoff_epoch']);ids=set();counts=Counter();scores=[];records=[]
    need(finite(dataset['export_read_started_epoch'])<=cutoff<=finite(dataset['export_read_completed_epoch'])
         ==finite(dataset['current_export_consumable_no_earlier_than_epoch']),'native_export_read_availability')
    need(dataset.get('availability_scope')=='original_owner_observation_after_prior_commit_not_final_ack_row_visibility',
         'native_owner_knowledge_scope_required')
    comparisons={name:[] for name in COMPARATORS};comparison_failures=Counter()
    for publication,status in zip(publications,statuses,strict=True):
        decision=publication['forecast'];identity=decision['decision_id']
        need(type(identity) is str and identity not in ids and status['decision_id']==identity,'native_forecast_identity_order')
        ids.add(identity);need(digest(decision)==publication['forecast_sha256'],'native_forecast_digest')
        need(len(decision['forecasts'])==1,'native_one_arm_required')
        arm={**decision['forecasts'][0],'committed_available_epoch':publication['consumed_epoch']}
        errors=forecast_errors(arm,decision,protocol)
        if publication['publication_epoch'] is None or publication['consumed_epoch'] is None:
            errors.append('native_not_consumed')
        elif not arm['issued_epoch']<=publication['publication_epoch']<=publication['consumed_epoch']<arm['target_epoch']:
            errors.append('native_publication_target_order')
        state=status['status'];need(state in ('pending','unknown','scored'),'native_status_required')
        if errors:
            need(state!='scored','native_invalid_publication_scored')
            state='unknown';counts['invalid_publication']+=1
        if state=='scored':
            body=status['outcome'];ack=status['visibility']
            need(body['forecast_sha256']==digest(decision) and ack['score_sha256']==digest(body)
                 and ack['decision_id']==identity and ack['attempt_id']==body['attempt_id'],'native_score_identity')
            target=body['target'];value=validate_exact_target(target,arm['native_anchor'])
            expected=outcome.score_native(arm['native_anchor'],value)
            need(encoded(expected)==encoded(body['score']),'native_score_value_replay')
            need(arm['target_epoch']<=target['source_read_completed_epoch']<=target['source_validated_epoch']
                 <=body['source_available_epoch']<=body['scoring_observed_epoch']<=ack['outcome_available_epoch']<=cutoff,
                 'native_outcome_availability_order')
            scores.append(expected)
            actual=float.fromhex(expected['actual_signed_pips_hex'])
            for name,key in COMPARATORS.items():
                prediction=float(arm['diagnostics'][key]);error=prediction-actual
                if not math.isfinite(error):comparison_failures[name]+=1;continue
                side=outcome.native.direction(prediction);actual_side=expected['actual_direction']
                comparisons[name].append({'absolute_error_pips':abs(error),'predicted_direction':side,
                    'direction_denominator_eligible':bool(side and actual_side),
                    'direction_correct':side==actual_side if side and actual_side else None})
        counts[state]+=1;records.append({'decision_id':identity,'status':state,'errors':errors})
    def mean(key):return math.fsum(row[key] for row in scores)/len(scores) if scores else None
    directions=[s for s in scores if s['direction_denominator_eligible']]
    comparison_summary={}
    for name,values in comparisons.items():
        directional=[v for v in values if v['direction_denominator_eligible']]
        comparison_summary[name]={'n':len(values),'scored_native_target_denominator':len(scores),
            'unknown_comparison_count':comparison_failures[name],
            'mae_pips':math.fsum(v['absolute_error_pips'] for v in values)/len(values) if values else None,
            'direction_denominator':len(directional),
            'direction_accuracy':sum(v['direction_correct'] for v in directional)/len(directional) if directional else None,
            'probability_metrics':None,'prediction_source':'immutable_preissue_native_pip_scalar',
            'target_source':'same_first_admitted_native_target_as_joint',
            'comparison_is_causal_news_effect':False}
    return {'schema_version':EVALUATOR_VERSION,'family':FAMILY,'instrument':protocol['instrument'],
        'forecast_count':len(publications),'status_counts':dict(counts),'records':records,
        'collection_counts':dataset['collection_counts'],
        'preissue_native_comparisons':comparison_summary,
        'native_scores':{'n':len(scores),'mae_pips':mean('absolute_error_pips'),'mse_pips':mean('squared_error_pips'),
            'zero_move_mae_pips':mean('zero_baseline_absolute_error_pips'),'brier':mean('brier'),'fair_coin_brier':.25 if scores else None,
            'direction_denominator':len(directions),'direction_accuracy':sum(s['direction_correct'] for s in directions)/len(directions) if directions else None},
        'comparison_scope':'native_original_h1_completed_M1_close_forecast_diagnostics',
        'source_authentication_scope':'fixed_ledger_export_replays_bytes_pure_evaluator_does_not_attest_caller_json',
        'executable_result':None,'account_return':None,'position_management_authorized':False,
        'can_place_orders':False,'can_promote':False,'proof_eligible':False,'account_eligible':False}
