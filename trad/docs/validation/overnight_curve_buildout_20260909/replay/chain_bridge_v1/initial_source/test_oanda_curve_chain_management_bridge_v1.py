"""Full synthetic original chains and caller read contexts, never actual outcomes."""
from copy import deepcopy
from decimal import Decimal,localcontext
import math

import pytest
import oanda_curve_chain_management_bridge_v1 as b
import oanda_forecast_curve_contract_v1 as c
from test_oanda_forecast_curve_contract_v1 import inputs,SOURCES,reseal


def read(value,start,end,*,digest=None):
    return {'payload_sha256':digest or c.content_hash(value),'read_started_epoch':start,'read_completed_epoch':end}


def fixture(*,pips='3',bid='1.1000',ask='1.1002',side=None,offset=0,decision_fraction=0.,window=0):
    values=inputs(input_context={'price_convention':'official_midpoint','historical_ingestion_equivalence_proven':False})
    for key in ('input_available_epoch','reference_epoch','reference_label_epoch','model_fitted_epoch',
                'computation_started_epoch','computed_epoch'):values[key]+=offset
    for row in values['points']:
        row['predicted_signed_pips']=pips;row['target_epoch']+=offset;row['target_label_epoch']+=offset
    values['target_selection_policy']={'kind':'exact_price_epoch' if window==0 else 'first_complete_bar_at_or_after_nominal',
                                       'maximum_delay_sec':window}
    prepared=c.prepare_curve(**values)
    curve=c.issue_curve(prepared,expected_source_bindings=SOURCES,clock=lambda:1005.5+offset)
    pub=c.publication_receipt(curve,persisted_bytes_sha256=c.content_hash(curve),publication_started_epoch=1005.6+offset,
                              expected_source_bindings=SOURCES,clock=lambda:1005.8+offset)
    consume=c.consume_curve(curve,pub,expected_source_bindings=SOURCES,clock=lambda:1006.+offset)
    target=1035.+offset;decision=1010.+offset+decision_fraction
    chain={'curve':curve,'publication':pub,'consumption':consume,'management_target_epoch':target}
    quote={'instrument':'EUR_USD','quote_id':'fixture.current.quote','bid':bid,'ask':ask,
           'market_epoch':1009.+offset,'available_epoch':1009.+offset,'tradeable':True}
    quotes={'EUR_USD':quote}
    state={'position':None,'realized_usd':'0'}
    if side is not None:
        state['position']={'instrument':'EUR_USD','side':side,'base_units':500,
                           'entry_epoch':1007.+offset,'entry_decision_epoch':1006.5+offset,
                           'entry_price':'1.1001','original_target_epoch':target}
    config={'schema_version':b.mechanics.CONFIG_SCHEMA,**b.mechanics.SAFETY,'account_currency':'USD','maximum_open_positions':1,
        'metadata':{'EUR_USD':{'base_currency':'EUR','quote_currency':'USD','pip_size':'.0001','unit_increment':1}},
        'notional_usd':'1000','conversion_policy':'direct_or_inverse_executable_bid_ask_no_triangulation',
        'sizing_policy':'fixed_usd_notional_integer_base_units_at_decision','curve_target_window_policy':'exact_only' if not window else 'nominal_management_boundary',
        'maximum_entry_spread_bps':'10','maximum_holding_sec':3600,'cadence_sec':60,'execution_delay_sec':1,
        'quote_max_age_sec':5,'minimum_entry_cost_ratio':'1','slippage_bps_per_leg':'.1','switch_incremental_hurdle_usd':'0',
        'legacy_policy':{'minimum_score_cost_ratio':1.,'rotation_improvement_multiple':1.25,'maximum_holding_min':60}}
    expected={key:deepcopy(prepared[key]) for key in ('forecast_cohort','model_sha256','feature_version','source_bindings',
        'reference_price_kind','bar_duration_sec','target_selection_policy')}
    expected.update(policy_sha256=prepared['policy']['policy_sha256'],price_convention='official_midpoint',
        model_ids_by_horizon={str(n['horizon_sec']):n['model_id'] for n in prepared['nodes']})
    context={'schema_version':b.TRUST_SCHEMA,**b.mechanics.SAFETY,'registry_sha256':'a'*64,
             'usd_config_sha256':b.mechanics.digest(config),'bridge_source_bindings':{**b.REUSED_BINDINGS,b.SOURCE_NAME:'b'*64},
             'pairs':{'EUR_USD':expected}}
    observed={'schema_version':b.OBSERVATION_SCHEMA,'registry_read':read(None,1000+offset,1000.1+offset,digest='a'*64),
        'state_known_epoch':1007.+offset,'state_read':read(state,1007.1+offset,1007.2+offset),
        'quotes_read':read(quotes,1009.2+offset,1009.3+offset),
        'chain_reads':{'EUR_USD':{'curve':read(curve,1006.1+offset,1006.2+offset),
            'publication':read(pub,1006.3+offset,1006.4+offset),'consumption':read(consume,1006.5+offset,1006.6+offset)}}}
    return [chain],quotes,state,dict(decision_epoch=decision,management_target_epoch=target,usd_config=config,
                                    trusted_context=context,observation_context=observed)


def run(values):return b.select_from_curve_chains(*values[:3],**values[3])


def update_read(values,kind):
    chains,quotes,state,args=values
    if kind=='state':args['observation_context']['state_read']['payload_sha256']=b.mechanics.digest(state)
    elif kind=='quotes':args['observation_context']['quotes_read']['payload_sha256']=b.mechanics.digest(quotes)
    else:
        args['observation_context']['chain_reads']['EUR_USD'][kind]['payload_sha256']=c.content_hash(chains[0][kind])


def test_full_chain_admitted_prepared_and_selected_without_activation():
    values=fixture();result=run(values)
    assert result['selection']['decision']['action']=='enter'
    assert result['selection']['entry_candidate_count']==result['selection']['continuation_candidate_count']==1
    assert result['refusals']==[]
    assert result['evidence'][0]['original_chain']==values[0][0]
    assert result['evidence'][0]['admission']['new_entry']['semantic_admitted']
    assert all(result[k] is v for k,v in b.mechanics.SAFETY.items())
    assert all(result[k] is False for k in ('caller_io_authenticated','broker_state_authenticated',
        'actual_source_files_verified_here','prospective_computation_receipt','positions_managed',
        'manager_activation','conditional_remaining_probability_created'))


@pytest.mark.parametrize('held,action,signed',[(1,'hold','2'),(-1,'exit','-2')])
def test_old_short_terminal_crossing_refuses_new_long_and_keeps_both_incumbent_orientations(held,action,signed):
    result=run(fixture(pips='-3',bid='1.0994',ask='1.0996',side=held))
    row=result['evidence'][0]
    assert row['admission']['reason_code']=='countertrend_from_original_terminal_crossing'
    assert row['admission']['continuation']['incumbent_signed_remaining_move_pips']==signed
    assert result['selection']['entry_candidate_count']==0 and result['selection']['continuation_candidate_count']==1
    assert result['selection']['decision']['action']==action


def test_zero_and_negative_continuation_are_not_missing_placeholders():
    result=run(fixture(bid='1.1002',ask='1.1004',side=-1))
    assert result['evidence'][0]['prepared_candidate']['signed_price_change']=='0'
    assert result['selection']['continuation_candidate_count']==1 and result['selection']['entry_candidate_count']==0
    assert result['selection']['decision']['action']=='hold'


def test_fields_and_original_uncertainty_are_exact_in_new_named_clock_projection():
    result=run(fixture(offset=1788948000,decision_fraction=.0059996))
    row=result['evidence'][0];original=row['admission']['candidate'];compat=row['compatibility_candidate']
    assert compat['schema_version']==b.COMPATIBILITY_SCHEMA and 'candidate_sha256' not in compat
    assert compat['original_candidate']==original and compat['original_candidate_sha256']==original['candidate_sha256']
    assert compat['remaining_sec']==str(Decimal(str(original['original_target_epoch']))-Decimal(str(original['decision_epoch'])))
    assert original['remaining_sec']==original['original_target_epoch']-original['decision_epoch']
    assert Decimal(str(original['remaining_sec']))!=Decimal(compat['remaining_sec'])
    for key,value in original.items():
        if key not in ('schema_version','remaining_sec','candidate_sha256'):assert compat[key]==value
    assert compat['original_probability_up']=='0.7' and not compat['probability_is_remaining_move_probability']
    assert compat['terminal_interval']==original['terminal_interval']
    assert row['prepared_candidate']['source_payload']==compat


@pytest.mark.parametrize('direction',[-math.inf,math.inf])
def test_one_ulp_remaining_corruption_is_not_fixed_by_resealing(direction):
    result=run(fixture(offset=1788948000,decision_fraction=.0059996))
    row=deepcopy(result['evidence'][0]['admission']['candidate'])
    row['remaining_sec']=math.nextafter(row['remaining_sec'],direction)
    row['candidate_sha256']=c.content_hash({k:v for k,v in row.items() if k!='candidate_sha256'})
    with pytest.raises(ValueError,match='original_float_remaining'):
        b._compatibility(row,result['trusted_context']['bridge_source_bindings'])


@pytest.mark.parametrize('field,change',[
    ('forecast_cohort','wrong'),('model_sha256','f'*64),('feature_version','wrong'),('policy_sha256','f'*64),
    ('price_convention','ba_derived_midpoint'),('reference_price_kind','wrong'),('bar_duration_sec',60),
])
def test_trusted_cohort_model_policy_and_price_identity_cannot_drift(field,change):
    values=fixture();values[3]['trusted_context']['pairs']['EUR_USD'][field]=change
    with pytest.raises(ValueError):run(values)


@pytest.mark.parametrize('field', ['curve','publication','consumption'])
def test_tampered_chain_is_not_accepted_by_downstream_hash_rebinding(field):
    values=fixture();value=values[0][0][field]
    if field=='curve':
        value['prepared_curve']['nodes'][1]['predicted_signed_pips']='-9'
        reseal(value,'curve_sha256','curve_id','forecast_curve_v1_')
    elif field=='publication':
        value['publication_completed_epoch']=1005.4;reseal(value,'publication_sha256')
    else:
        value['available_epoch']=1005.4;reseal(value,'consumption_sha256')
    update_read(values,field)
    with pytest.raises(ValueError):run(values)


@pytest.mark.parametrize('change',[lambda q:q.update(market_epoch=1000),lambda q:q.update(tradeable=False)])
def test_stale_or_nontradeable_quote_is_explicit_refusal_not_zero(change):
    values=fixture(side=1);change(values[1]['EUR_USD']);update_read(values,'quotes');result=run(values)
    assert result['selection']['decision']['action']=='exit' and result['selection']['continuation_candidate_count']==0
    assert result['evidence'][0]['prepared_candidate'] is None
    assert result['refusals'][0]['stage']=='original_adapter'


def test_missing_quote_is_explicit_refusal_without_inventing_midpoint():
    values=fixture(side=1);values[1].clear();update_read(values,'quotes');result=run(values)
    assert result['refusals'][0]['reason_code']=='decision_quote_missing'
    assert result['selection']['decision']['action']=='exit'


@pytest.mark.parametrize('mutation',[
    lambda v:v[3]['observation_context']['state_read'].update(read_completed_epoch=1011),
    lambda v:v[3]['observation_context'].update(state_known_epoch=1008),
    lambda v:v[3]['observation_context']['quotes_read'].update(read_completed_epoch=1008.9),
    lambda v:v[3]['observation_context']['chain_reads']['EUR_USD']['curve'].update(read_started_epoch=1005.1,read_completed_epoch=1005.4),
    lambda v:v[3]['observation_context']['registry_read'].update(payload_sha256='f'*64),
    lambda v:v[3]['observation_context']['chain_reads']['EUR_USD']['consumption'].update(payload_sha256='f'*64),
])
def test_caller_clock_or_object_binding_contradictions_raise(mutation):
    values=fixture(side=1);mutation(values)
    with pytest.raises(ValueError):run(values)


@pytest.mark.parametrize('key,value',[('entry_epoch',1011),('original_target_epoch',1065),('side',0),('base_units',True),('instrument','GBP_USD')])
def test_future_or_incomparable_incumbent_state_refused(key,value):
    values=fixture(side=1);values[2]['position'][key]=value;update_read(values,'state')
    with pytest.raises(ValueError):run(values)


def test_dependency_context_and_config_hash_cannot_be_overridden():
    values=fixture();values[3]['trusted_context']['bridge_source_bindings']['oanda_curve_management_replay_v1.py']='f'*64
    with pytest.raises(ValueError,match='dependency_context'):run(values)
    values=fixture();values[3]['usd_config']['notional_usd']='2000'
    with pytest.raises(ValueError,match='trusted_usd_policy'):run(values)


def test_duplicate_pairs_mixed_targets_and_extra_read_records_refused():
    for kind in ('duplicate','target','read'):
        values=fixture()
        if kind=='duplicate':values[0].append(deepcopy(values[0][0]))
        elif kind=='target':values[0][0]['management_target_epoch']=1065
        else:values[3]['observation_context']['chain_reads']['GBP_USD']={}
        with pytest.raises(ValueError):run(values)


def test_valid_chain_without_requested_native_node_is_refusal_not_interpolation():
    values=fixture();values[3]['management_target_epoch']=1040;values[0][0]['management_target_epoch']=1040
    result=run(values)
    assert result['refusals'][0]['reason_code']=='requested_native_target_unavailable'
    assert result['selection']['decision']['action']=='wait'


def test_absent_entire_chain_remains_missing_and_terminal_exit_intent_has_no_fill():
    values=fixture(side=1);values[0].clear();values[3]['observation_context']['chain_reads'].clear()
    result=run(values)
    assert result['refusals']==[{'instrument':'EUR_USD','stage':'inventory','reason_code':'curve_chain_not_supplied'}]
    values[3].update(decision_epoch=1035,terminal=True)
    result=run(values)
    assert result['selection']['decision']['action']=='exit' and result['positions_managed'] is False


def test_original_training_window_is_explicit_approximation_not_silently_exact():
    result=run(fixture(window=7))
    compat=result['evidence'][0]['compatibility_candidate']
    assert result['original_target_window_policy']=='nominal_management_boundary'
    assert compat['target_is_exact'] is False and compat['target_price_window_end_epoch']==1042
    assert compat['original_target_epoch']==1035
    values=fixture(window=7);values[3]['usd_config']['curve_target_window_policy']='exact_only'
    values[3]['trusted_context']['usd_config_sha256']=b.mechanics.digest(values[3]['usd_config'])
    assert run(values)['refusals'][0]['reason_code']=='nonexact_model_target_requires_explicit_policy'


def test_return_ownership_semantic_revalidation_and_low_decimal_context():
    values=fixture(side=1);before=deepcopy(values);expected=run(values)
    assert values==before
    with localcontext() as context:
        context.prec=2
        assert run(values)==expected
    changed=deepcopy(expected);changed['evidence'][0]['admission']['new_entry']['semantic_admitted']=False
    changed['bridge_sha256']=b.mechanics.digest({k:v for k,v in changed.items() if k!='bridge_sha256'})
    with pytest.raises(ValueError,match='bridge_semantic_mismatch'):
        b.validate_bridge(changed,*values[:3],**values[3])
    validated=b.validate_bridge(expected,*values[:3],**values[3]);validated['input_state']['position']['side']=-1
    assert expected['input_state']['position']['side']==1 and values==before
