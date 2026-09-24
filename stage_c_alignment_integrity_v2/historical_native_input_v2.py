"""Offline prepared-curve adapter, with no simulated observed publication receipt.

Reuse prepared native nodes and the existing ledger candidate consumer. This
adapter maps gross model predictions into that consumer's representation only;
the existing Decimal core still owns sizing, conversion, costs and accounting.
"""
from copy import deepcopy
from decimal import Decimal, Context, localcontext
from contracts import fingerprint
from native_policy_input_v2 import load_native
from remaining_horizon_v2 import issue_remaining
from historical_market_inputs_v2 import validate_record
from reference_accounting_adapter_v2 import DEFAULT_TRAD, load_reference

TIER='historical_fitted_candle_policy_scenario.v1'
ORIGIN=1721606460
TARGET=1721779260
ROLLOVER=ORIGIN+86400+1
SCENARIOS={name:{'scenario_id':name,'input_tier':TIER,'slippage_bps_per_leg':slip,
    'single_rollover_cost_bps_of_base_usd':cost,'rollover_epoch':ROLLOVER,
    'fill_rule':'full_pending_units_at_next_completed_M1_close_after_58s_delay',
    'arrival_rule':'assumed_M1_close_plus_two_seconds_model_availability',
    'observed_execution':False,'broker_access':False}
    for name,slip,cost in [('candle_zero_slippage_financing','0','0'),('candle_one_bp_slippage_rollover','1','1')]}


def validate_scenario(scenario):
    if scenario!=SCENARIOS.get(scenario.get('scenario_id')):
        from later_policy_scenario_v2 import scenarios
        if scenario!=scenarios().get(scenario.get('scenario_id')):
            from chronological_policy_scenario_v2 import scenarios as chronological_scenarios
            if scenario!=chronological_scenarios().get(scenario.get('scenario_id')):
                raise ValueError('frozen_retrospective_scenario_required')


def panel(points,epoch):
    result={}
    for r in points:
        validate_record(r);pair=r['instrument']
        if pair in result:raise ValueError('duplicate_market_point')
        if r['status']!='valid_candle_close_pair':continue
        if not 0<=epoch-r['price_epoch']<=60:raise ValueError('historical_quote_not_available_or_stale')
        result[pair]={'instrument':pair,'quote_id':'candle-scenario:'+r['record_sha256'],
            'bid':r['bid'],'ask':r['ask'],'market_epoch':r['price_epoch'],
            'available_epoch':r['assumed_available_epoch'],'tradeable':True}
    return result


def financing_rates(quotes,epoch,scenario,trad_root=DEFAULT_TRAD):
    """Signed USD per base unit, for common-value decision comparisons."""
    validate_scenario(scenario);ref=load_reference(trad_root);result={}
    with localcontext(ref.CTX):
        for pair in sorted(quotes):
            try:base=ref.usd_rates(pair[:3],quotes,epoch,60)['buy_currency_usd']
            except (ValueError,KeyError):continue
            # Explicit scenario costs, identical signed convention for both sides.
            cost=-base*Decimal(scenario['single_rollover_cost_bps_of_base_usd'])/10000
            result[pair]={'long':str(cost),'short':str(cost)}
    return result


def financing_event_rates(quotes,epoch,scenario,trad_root=DEFAULT_TRAD):
    """Convert scenario USD costs to the ledger's quote-currency/unit input.

    EventLedger converts the signed quote amount to USD exactly once. Supplying
    a USD rate directly would apply that conversion to the wrong unit for crosses.
    """
    usd=financing_rates(quotes,epoch,scenario,trad_root);ref=load_reference(trad_root);result={}
    with localcontext(ref.CTX):
        for pair,rates in usd.items():
            try:conversion=ref.usd_rates(pair[4:],quotes,epoch,60)
            except (ValueError,KeyError):continue
            result[pair]={}
            for side,text in rates.items():
                amount=Decimal(text);rate=conversion['buy_currency_usd' if amount<0 else 'sell_currency_usd']
                result[pair][side]=str(amount/rate)
    return result


def make_packet(remaining,model,observation,reference,metadata,trad_root=DEFAULT_TRAD):
    metadata={**metadata,'pip_size':str(Decimal(metadata['pip_size']))}
    validate_record(reference)
    if reference['status']!='valid_candle_close_pair':raise ValueError('reference_price_unavailable')
    origin=remaining['conditioning_epoch'];target=remaining['original_target_epoch']
    expected,reason=issue_remaining([model],observation,decision_epoch=origin,target_epoch=target,
        available_epoch=remaining['forecast']['available_epoch'],procedure=remaining['procedure'])
    if reason!='eligible' or expected!=remaining:raise ValueError('remaining_forecast_not_reproduced_from_model_input')
    pair=remaining['forecast']['instrument']
    if reference['instrument']!=pair or reference['price_epoch']!=origin or observation['instrument']!=pair:
        raise ValueError('reference_origin_identity_mismatch')
    if observation.get('source_member_sha256')!=reference['source_member_sha256']:
        raise ValueError('reference_feature_member_mismatch')
    c,_=load_native(trad_root);horizon=target-origin;pip=metadata['pip_size']
    if metadata['base_currency']!=pair[:3] or metadata['quote_currency']!=pair[4:]:raise ValueError('packet_metadata_mismatch')
    sources={'fitted_model':model['model_id'],'feature_observation':fingerprint(observation),
             'reference_price':reference['record_sha256'],'remaining_forecast':remaining['remaining_forecast_id']}
    with localcontext(Context(prec=192)):
        move=Decimal(reference['reference_close'])*Decimal(str(remaining['forecast']['prediction']))/10000/Decimal(pip)
    prepared=c.prepare_curve(instrument=pair,pip_size=pip,forecast_cohort='remaining-two-day-development',
        model_sha256=model['model_id'],feature_version='reviewed-five-feature-v2',source_bindings=sources,
        input_capture_sha256=fingerprint({'observation':observation,'reference':reference}),
        input_available_epoch=origin,reference_epoch=origin,reference_label_epoch=origin-60,
        reference_price=reference['reference_close'],reference_price_kind='retained_M1_close_binary_roundtrip_decimal',
        bar_duration_sec=60,model_fitted_epoch=model['ready_epoch'],computation_started_epoch=origin,
        computed_epoch=remaining['forecast']['available_epoch'],
        points=[{'horizon_sec':horizon,'target_epoch':target,'target_label_epoch':target-60,
                 'model_id':model['model_id'],'predicted_signed_pips':str(move)}],
        policy=c.make_policy(native_horizons_sec=[horizon],maximum_reference_age_sec=2,maximum_build_sec=2,
            maximum_issue_delay_sec=1,maximum_publication_delay_sec=1,maximum_decision_age_sec=2,minimum_remaining_sec=0),
        computation_sha256=remaining['remaining_forecast_id'],scope='engineering_replay',
        input_context={'input_tier':TIER,'conditioning_kind':'fresh_features_direct_remaining_horizon_model',
                       'conditioning_epoch':origin,'observed_publication':False})
    packet={'schema_version':'historical_prepared_native_packet.v1','input_tier':TIER,
        'prepared_curve':prepared,'remaining_forecast':remaining,'model':model,'observation':observation,
        'reference_point':reference,'metadata':deepcopy(metadata),'source_bindings':sources,
        'assumed_available_epoch':remaining['forecast']['available_epoch'],'observed_publication':False}
    packet['packet_sha256']=fingerprint(packet)
    return packet


def candidate(packet,quotes,epoch,target,scenario,metadata,trad_root=DEFAULT_TRAD):
    validate_scenario(scenario)
    if packet.get('packet_sha256')!=fingerprint({k:v for k,v in packet.items() if k!='packet_sha256'}):raise ValueError('historical_packet_identity_mismatch')
    expected=make_packet(packet['remaining_forecast'],packet['model'],packet['observation'],packet['reference_point'],metadata,trad_root)
    if packet!=expected:raise ValueError('historical_prepared_packet_recomputation_mismatch')
    return prepared_candidate(packet,quotes,epoch,target,scenario,metadata,trad_root)


def prepared_candidate(packet,quotes,epoch,target,scenario,metadata,trad_root=DEFAULT_TRAD):
    """Value an already model-authenticated curve using the shared Decimal core.

    Callers must first recompute their specific model/input packet. This common
    projection contains no model authority or prediction-generation shortcut.
    """
    validate_scenario(scenario)
    c,_=load_native(trad_root);prepared=c.validate_prepared(packet['prepared_curve'],expected_source_bindings=packet['source_bindings'])
    if prepared['scope']!='engineering_replay' or packet['observed_publication'] is not False:raise ValueError('historical_scope_mismatch')
    if not packet['assumed_available_epoch']<=epoch or epoch-prepared['reference_epoch']>2:raise ValueError('historical_conditioning_not_available_or_stale')
    node=prepared['nodes'][0]
    if node['original_target_epoch']!=target:raise ValueError('historical_exact_target_required')
    ref=load_reference(trad_root);pair=prepared['instrument'];q=ref.quote_at(quotes,pair,epoch,60)
    rates=financing_rates(quotes,epoch,scenario,trad_root)
    if pair not in rates:raise ValueError('financing_conversion_unavailable')
    with localcontext(Context(prec=192)):
        terminal=Decimal(node['expected_terminal_price']);change=terminal-q['mid'];pips=change/Decimal(metadata['pip_size'])
    bound={k:deepcopy(q[k]) for k in ('instrument','quote_id','market_epoch','available_epoch','tradeable')}
    bound.update(bid=str(q['bid']),ask=str(q['ask']))
    future_rates=rates[pair] if epoch<scenario['rollover_epoch']<target else {'long':'0','short':'0'}
    body={'status':'available','scope':'engineering_replay','input_tier':TIER,'instrument':pair,
        'side':1 if change>0 else -1 if change<0 else 0,'curve_id':'retrospective-'+packet['packet_sha256'],
        'curve_sha256':packet['packet_sha256'],'node_id':node['node_id'],'node_sha256':node['node_sha256'],
        'model_id':node['model_id'],'model_sha256':prepared['model_sha256'],'forecast_cohort':prepared['forecast_cohort'],
        'reference_epoch':prepared['reference_epoch'],'reference_label_epoch':prepared['reference_label_epoch'],
        'issued_epoch':packet['assumed_available_epoch'],'issued_clock_kind':'assumed_replay_availability_not_observed_issue',
        'available_epoch':packet['assumed_available_epoch'],'decision_epoch':epoch,'original_target_epoch':target,
        'target_label_epoch':target-60,'target_price_window_end_epoch':target,'horizon_sec':node['horizon_sec'],
        'remaining_sec':target-epoch,'target_selection_policy':prepared['target_selection_policy'],
        'target_window_policy':'exact_only','target_is_exact':True,'pip_size':str(metadata['pip_size']),
        'expected_terminal_price':str(terminal),'expected_remaining_price_change':str(change),'expected_remaining_move_pips':str(pips),
        'source_bindings':packet['source_bindings'],'decision_quote':bound,'decision_quote_sha256':ref.digest(bound),
        'thesis_version':'fixed-original-two-day-target','remaining_financing_usd_per_base_unit':future_rates,
        'original_probability_up':None,'original_probability_scope':'not_provided','original_uncertainty_scope':'not_provided',
        'observed_publication':False,'observed_execution':False,**c.AUTHORITY}
    body['candidate_sha256']=fingerprint(body)
    return body


def adapted_candidates(frame,config,trad_root=DEFAULT_TRAD):
    candidates=[];refusals=[];seen=set()
    for p in frame['historical_packets']:
        pair=p['prepared_curve']['instrument']
        if pair in seen:raise ValueError('duplicate_historical_packet')
        seen.add(pair)
        try:candidates.append(candidate(p,frame['quotes'],frame['epoch'],frame['target_epoch'],frame['scenario'],config['metadata'][pair],trad_root))
        except (ValueError,KeyError,TypeError) as exc:
            refusals.append({'instrument':pair,'reason':str(exc),'packet_sha256':p.get('packet_sha256')})
    return candidates,refusals


def validate_historical_frame(frame,config,trad_root=DEFAULT_TRAD):
    if frame.get('input_tier')!=TIER:raise ValueError('historical_frame_tier_required')
    validate_scenario(frame['scenario'])
    if frame.get('model_profile')=='later_remaining_layer_policy.v1':
        from later_policy_input_v2 import validate_frame_authority
        validate_frame_authority(frame,config)
    if frame.get('model_profile')=='chronological_layer_policy.v1':
        from chronological_policy_input_v2 import validate_frame_authority as chronological_authority
        chronological_authority(frame,config)
    if frame.get('model_profile')=='currency_projection_policy.v1':
        from currency_projection_policy_input_v2 import validate_frame_authority as projection_authority
        projection_authority(frame,config)
    if str(config['slippage_bps_per_leg'])!=frame['scenario']['slippage_bps_per_leg']:
        raise ValueError('historical_slippage_contract_mismatch')
    if config['execution_delay_sec']!=58:raise ValueError('historical_execution_delay_contract_mismatch')
    points=frame['market_points']
    if len(points)!=68 or {p['instrument'] for p in points}!=set(config['metadata']):raise ValueError('historical_all68_market_points_required')
    if frame['quotes']!=panel(points,frame['epoch']):raise ValueError('historical_quote_point_binding_mismatch')
    if frame['kind']=='decision':
        if frame.get('candidate_kind')!='curve':raise ValueError('historical_native_candidate_kind_required')
        if frame.get('model_profile')=='later_remaining_layer_policy.v1':
            from later_policy_input_v2 import adapted_candidates as later_candidates
            candidates,refusals=later_candidates(frame,config,trad_root)
        elif frame.get('model_profile')=='chronological_layer_policy.v1':
            from chronological_policy_input_v2 import adapted_candidates as chronological_candidates
            candidates,refusals=chronological_candidates(frame,config,trad_root)
        elif frame.get('model_profile')=='currency_projection_policy.v1':
            from currency_projection_policy_input_v2 import adapted_candidates as projection_candidates
            candidates,refusals=projection_candidates(frame,config,trad_root)
        elif frame.get('model_profile') is not None:
            from matched_policy_input_v2 import adapted_candidates as matched_candidates
            candidates,refusals=matched_candidates(frame,config,trad_root)
        else:candidates,refusals=adapted_candidates(frame,config,trad_root)
        if frame.get('candidates')!=candidates or frame.get('historical_refusals')!=refusals:raise ValueError('historical_candidate_recomputation_mismatch')
    elif frame['kind']=='execution':
        for arm,fill in frame['fills'].items():
            if fill!={'units':'remaining','evidence_id':f"scenario-full-fill:{frame['scenario']['scenario_id']}:{arm}:{frame['epoch']}"}:
                raise ValueError('historical_declared_fill_binding_mismatch')
    elif frame['kind']=='financing':
        if frame['epoch']!=frame['scenario']['rollover_epoch'] or frame['rates']!=financing_event_rates(frame['quotes'],frame['epoch'],frame['scenario'],trad_root):
            raise ValueError('historical_rollover_scenario_mismatch')
