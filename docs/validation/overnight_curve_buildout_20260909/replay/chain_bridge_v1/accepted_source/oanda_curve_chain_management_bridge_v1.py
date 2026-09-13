"""Pure original-chain replay into separate prepared management channels.

The trusted context and read receipts are caller assertions, not authenticated
I/O. No source file, registry, quote, account or clock is read here. This is an
unregistered research component, not a forecast issuer or management runtime.
"""
from copy import deepcopy
from decimal import Context, Decimal, localcontext
import json
import math

import oanda_curve_direction_admission_v1 as admission
import oanda_curve_management_channels_v1 as channels
import oanda_curve_management_replay_v1 as mechanics
import oanda_curve_management_adapter_v1 as adapter
import oanda_forecast_curve_contract_v1 as contract

SCHEMA='curve_chain_management_bridge_v1_20260909'
TRUST_SCHEMA='curve_chain_management_trusted_context_v1_20260909'
OBSERVATION_SCHEMA='curve_chain_management_observation_context_v1_20260909'
COMPATIBILITY_SCHEMA='chain_bridge_decimal_clock_compatibility_v1_20260909'
SOURCE_NAME='oanda_curve_chain_management_bridge_v1.py'
MAX_CHAINS=68
MAX_TOTAL_CHAIN_BYTES=8*1024*1024
REUSED_BINDINGS={
    'oanda_curve_direction_admission_v1.py':'b3453296c20aa7e207826f30c7c7b2d697b42929f595aabeb5f9b21087b8ba7a',
    'oanda_curve_management_channels_v1.py':'6cba43bdd1a0ea5215b3bf716ce56211b5e173a678f4f0bca223adbf0ebb0880',
    'oanda_curve_management_replay_v1.py':'52fd04c6b087df4be775e490002afd0744caeb245604b8f577ab0fcff41de8f3',
    'oanda_curve_management_adapter_v1.py':'8fa6cccf1e2525d0186ab7abe0c68fccc4800db7b785a8b9f79f28c320ad1241',
    'oanda_forecast_curve_contract_v1.py':'c03549586f922f6031790fce68a5afb5b98e4eda6157f20c4720b1adb05d0746',
    'src/forex_system/research/sequential_portfolio_replay_v1.py':'e1201fdd3387825c9295f13045aeb00e2f87cce9ff819e789faf922ea2e514a6',
}
PAIR_IDENTITY_KEYS={'forecast_cohort','model_sha256','feature_version','source_bindings','policy_sha256',
    'model_ids_by_horizon','price_convention','target_selection_policy','reference_price_kind','bar_duration_sec'}
READ_KEYS={'payload_sha256','read_started_epoch','read_completed_epoch'}


def need(condition,reason):
    if not condition:raise contract.CurveContractError(reason)


def _owned(value):
    """Bound JSON structure before owning a detached input snapshot."""
    return json.loads(contract.canonical_bytes(mechanics.jsonable(value)))


def _sha(value):
    need(type(value) is str and len(value)==64 and all(c in '0123456789abcdef' for c in value),'sha256_required')
    return value


def _read(value,payload_sha,decision,*,original_available=None):
    need(type(value) is dict and set(value)==READ_KEYS,'caller_read_receipt_shape')
    need(_sha(value['payload_sha256'])==payload_sha,'caller_read_payload_mismatch')
    start=contract.epoch(value['read_started_epoch']);end=contract.epoch(value['read_completed_epoch'])
    need(start<=end<=decision,'caller_read_clock_order')
    if original_available is not None:
        need(contract.epoch(original_available)<=end,'original_available_after_own_read')
    return end


def _trust(context,policy,config):
    need(type(context) is dict and set(context)=={'schema_version','registry_sha256','usd_config_sha256',
        'bridge_source_bindings','pairs',*mechanics.SAFETY},'trusted_context_shape')
    need(context['schema_version']==TRUST_SCHEMA and all(context.get(k) is v for k,v in mechanics.SAFETY.items()),'trusted_context_authority')
    _sha(context['registry_sha256'])
    need(context['usd_config_sha256']==mechanics.digest(config),'trusted_usd_policy_mismatch')
    sources=context['bridge_source_bindings']
    need(type(sources) is dict and set(sources)==set(REUSED_BINDINGS)|{SOURCE_NAME},'bridge_source_inventory')
    need(all(sources.get(k)==v for k,v in REUSED_BINDINGS.items()),'bridge_dependency_context_mismatch')
    _sha(sources[SOURCE_NAME])
    pairs=context['pairs'];need(type(pairs) is dict and set(pairs)==set(policy['metadata']),'trusted_pair_universe_mismatch')
    for pair,expected in pairs.items():
        mechanics.pair_name(pair)
        need(type(expected) is dict and set(expected)==PAIR_IDENTITY_KEYS,'trusted_pair_identity_shape')
        for key in ('model_sha256','policy_sha256'):_sha(expected[key])
        for key in ('forecast_cohort','feature_version','reference_price_kind'):
            need(type(expected[key]) is str and 0<len(expected[key])<=240,'trusted_identity_token')
        need(expected['price_convention'] in ('official_midpoint','ba_derived_midpoint'),'trusted_price_convention')
        need(type(expected['model_ids_by_horizon']) is dict and 1<=len(expected['model_ids_by_horizon'])<=64,'trusted_horizon_models')
        need(type(expected['source_bindings']) is dict and 1<=len(expected['source_bindings'])<=64,'trusted_forecast_sources')
        for digest in expected['source_bindings'].values():_sha(digest)
    return context


def _state(state,observations,decision,target,policy):
    need(type(state) is dict and set(state)=={'position','realized_usd'},'single_scenario_state_shape')
    contract.decimal_number(state['realized_usd'])
    known=contract.epoch(observations['state_known_epoch'])
    read=observations['state_read']
    _read(read,mechanics.digest(state),decision,original_available=known)
    need(known<=contract.epoch(read['read_started_epoch']),'state_created_after_read_started')
    position=state['position']
    if position is None:return None
    need(type(position) is dict,'position_object_required')
    pair=mechanics.pair_name(position.get('instrument'))
    need(pair in policy['metadata'],'position_outside_declared_universe')
    need(type(position.get('side')) is int and position['side'] in (-1,1),'position_side')
    need(type(position.get('base_units')) is int and 0<position['base_units']<=10**12,'position_integer_units')
    entered=contract.epoch(position.get('entry_epoch'))
    need(entered<=known,'position_not_known_at_state_clock')
    need(contract.epoch(position.get('original_target_epoch'))==target,'incumbent_common_target_mismatch')
    if 'entry_decision_epoch' in position:
        need(contract.epoch(position['entry_decision_epoch'])<entered,'position_entry_clock_order')
    return dict(instrument=pair,side=position['side'],original_target_epoch=target,
        observed_epoch=known,state_sha256=mechanics.digest(state))


def _identity(curve,expected):
    prepared=curve['prepared_curve']
    need(prepared['scope']=='current_research','nonprospective_curve_scope')
    for key in ('forecast_cohort','model_sha256','feature_version','source_bindings','reference_price_kind','bar_duration_sec','target_selection_policy'):
        need(prepared[key]==expected[key],'trusted_curve_identity_mismatch:'+key)
    need(prepared['policy']['policy_sha256']==expected['policy_sha256'],'trusted_curve_policy_mismatch')
    need(prepared['input_context'].get('price_convention')==expected['price_convention'],'trusted_curve_price_convention_mismatch')
    models=expected['model_ids_by_horizon']
    need(set(models)=={str(h) for h in prepared['policy']['native_horizons_sec']},'trusted_native_model_inventory')
    for node in prepared['nodes']:
        if node['status']=='forecast':
            need(node['model_id']==models[str(node['horizon_sec'])],'trusted_native_model_mismatch')


def _compatibility(candidate,sources):
    need(candidate.get('schema_version')==adapter.SCHEMA and candidate.get('status')=='available','original_candidate_schema')
    need(candidate.get('candidate_sha256')==contract.content_hash({k:v for k,v in candidate.items() if k!='candidate_sha256'}),'original_candidate_seal')
    need(all(candidate.get(k) is v for k,v in contract.AUTHORITY.items()),'original_candidate_authority')
    target=contract.epoch(candidate['original_target_epoch']);decision=contract.epoch(candidate['decision_epoch'])
    remaining=candidate.get('remaining_sec')
    need(type(remaining) is float and math.isfinite(remaining) and remaining==target-decision,'original_float_remaining_clock_mismatch')
    with localcontext(Context(prec=192)):
        normalized=str(Decimal(str(target))-Decimal(str(decision)))
    body=deepcopy(candidate);del body['candidate_sha256']
    body.update(schema_version=COMPATIBILITY_SCHEMA,remaining_sec=normalized,
        original_candidate=deepcopy(candidate),original_candidate_sha256=candidate['candidate_sha256'],
        bridge_source_bindings=deepcopy(sources),remaining_clock_normalization=dict(
            original_schema_version=adapter.SCHEMA,original_remaining_sec=remaining,normalized_remaining_sec=normalized,
            original_arithmetic='python_float_target_minus_decision_exact_equality_required',
            normalized_arithmetic='Decimal(str(original_target_epoch))-Decimal(str(decision_epoch))',
            timestamps_changed=False,original_candidate_changed=False,
            scope='representation_only_no_tolerance_no_retiming_no_target_window_change'))
    return {**body,'compatibility_sha256':contract.content_hash(body)}


def select_from_curve_chains(chains,quotes,state,*,decision_epoch,management_target_epoch,
                            usd_config,trusted_context,observation_context,terminal=False,hold_only=False):
    """Replay original chains and select; authenticate neither I/O nor a state producer.

    A chain has curve/publication/consumption and its intended management target.
    Caller observations bind full object hashes and original read start/end
    clocks, not these derived records' hypothetical availability. Missing or
    expired valid forecasts retain explicit refusals; malformed evidence raises.
    The caller must independently verify actual registry/source bytes and state,
    quote and chain-read provenance before treating this as prospective input.
    """
    decision=contract.epoch(decision_epoch);target=contract.epoch(management_target_epoch)
    need(type(terminal) is bool and type(hold_only) is bool,'explicit_bridge_flags')
    need(type(chains) is list and len(chains)<=MAX_CHAINS,'bounded_chain_inventory')
    owned=[];total=0
    for chain in chains:
        raw=contract.canonical_bytes(chain);total+=len(raw)
        need(total<=MAX_TOTAL_CHAIN_BYTES,'total_chain_byte_bound')
        owned.append(json.loads(raw))
    quotes,state,config,context,observed=map(_owned,(quotes,state,usd_config,trusted_context,observation_context))
    need(type(quotes) is dict and len(quotes)<=136,'bounded_quote_inventory')
    with localcontext(mechanics.CTX):
        policy=mechanics.validate_config(config);_trust(context,policy,config)
        need(type(observed) is dict and set(observed)=={'schema_version','registry_read','state_known_epoch',
            'state_read','quotes_read','chain_reads'},'observation_context_shape')
        need(observed['schema_version']==OBSERVATION_SCHEMA,'observation_context_schema')
        _read(observed['registry_read'],context['registry_sha256'],decision)
        held=_state(state,observed,decision,target,policy)
        quotes_observed=_read(observed['quotes_read'],mechanics.digest(quotes),decision)
        for pair,quote in quotes.items():
            mechanics.pair_name(pair)
            need(type(quote) is dict and quote.get('instrument')==pair,'quote_inventory_identity')
            need(contract.epoch(quote.get('available_epoch'))<=quotes_observed,'quote_available_after_own_read')
        need(type(observed['chain_reads']) is dict,'chain_read_inventory')
        seen=set();evidence=[];continuing=[];entry=[];refusals=[]
        for chain in owned:
            need(type(chain) is dict and set(chain)=={'curve','publication','consumption','management_target_epoch'},'chain_shape')
            need(contract.epoch(chain['management_target_epoch'])==target,'mixed_management_targets')
            curve,pub,consume=chain['curve'],chain['publication'],chain['consumption']
            need(type(curve) is dict and type(curve.get('prepared_curve')) is dict,'curve_shape')
            pair=mechanics.pair_name(curve['prepared_curve'].get('instrument'))
            need(pair in context['pairs'] and pair not in seen,'undeclared_or_duplicate_pair')
            seen.add(pair);expected=context['pairs'][pair]
            contract.validate_consumption(curve,pub,consume,expected_source_bindings=expected['source_bindings'])
            _identity(curve,expected)
            reads=observed['chain_reads'].get(pair)
            need(type(reads) is dict and set(reads)=={'curve','publication','consumption'},'chain_read_shape')
            for name,value,available in (('curve',curve,curve['issued_epoch']),('publication',pub,pub['publication_completed_epoch']),
                                         ('consumption',consume,consume['available_epoch'])):
                _read(reads[name],contract.content_hash(value),decision,original_available=available)
            metadata={**mechanics.jsonable(policy['metadata'][pair]),'instrument':pair}
            result=admission.admit_candidate_for_target(curve,pub,consume,decision_epoch=decision,target_epoch=target,
                quote=quotes.get(pair),metadata=metadata,expected_source_bindings=expected['source_bindings'],
                maximum_quote_age_sec=float(policy['quote_max_age_sec']),target_window_policy=policy['curve_target_window_policy'],
                incumbent=held if held and held['instrument']==pair else None)
            row=dict(instrument=pair,original_chain=deepcopy(chain),original_read_context=deepcopy(reads),
                admission=result,compatibility_candidate=None,prepared_candidate=None)
            if result['status']!='classified':
                refusals.append(dict(instrument=pair,stage='original_adapter',reason_code=result['reason_code'],admission_sha256=result['admission_sha256']))
            else:
                compatibility=_compatibility(result['candidate'],context['bridge_source_bindings'])
                prepared,rejected=mechanics.prepare_candidates([compatibility],'curve',quotes,decision,target,policy)
                row['compatibility_candidate']=compatibility
                if rejected:
                    refusals.append(dict(instrument=pair,stage='usd_preparation',reason_code=rejected[0]['reason'],
                        admission_sha256=result['admission_sha256'],compatibility_sha256=compatibility['compatibility_sha256']))
                else:
                    need(len(prepared)==1,'single_prepared_candidate_required')
                    row['prepared_candidate']=mechanics.jsonable(prepared[0]);continuing.append(prepared[0])
                    if result['new_entry']['semantic_admitted']:entry.append(prepared[0])
            evidence.append(row)
        need(set(observed['chain_reads'])==seen,'unmatched_chain_read_context')
        absent=sorted(set(context['pairs'])-seen)
        refusals.extend(dict(instrument=pair,stage='inventory',reason_code='curve_chain_not_supplied') for pair in absent)
        selected=channels.choose_usd_action_with_channels(state,entry,continuing,quotes,decision,config,
            terminal=terminal,hold_only=hold_only)
        body=dict(schema_version=SCHEMA,**mechanics.SAFETY,decision_epoch=decision,management_target_epoch=target,
            original_target_window_policy=policy['curve_target_window_policy'],original_targets_changed=False,
            trusted_context=deepcopy(context),observation_context=deepcopy(observed),input_state=deepcopy(state),
            input_quotes_sha256=mechanics.digest(quotes),input_usd_config_sha256=mechanics.digest(config),
            evidence=evidence,refusals=refusals,selection=selected,source_binding_declarations_checked=True,
            actual_source_files_verified_here=False,caller_io_authenticated=False,broker_state_authenticated=False,
            prospective_computation_receipt=False,positions_managed=False,manager_activation=False,
            conditional_remaining_probability_created=False,risk_attachment=None,
            scope='pure_chain_semantics_and_selection_only_no_issuer_execution_settlement_or_performance',
            derived_clock_scope='decision_information_cutoff_only_no_actual_completion_clock_claim')
        body=mechanics.jsonable(body)
        return {**body,'bridge_sha256':mechanics.digest(body)}


def validate_bridge(result,*arguments,**keywords):
    expected=select_from_curve_chains(*arguments,**keywords)
    need(type(result) is dict and result==expected,'bridge_semantic_mismatch')
    return deepcopy(expected)
