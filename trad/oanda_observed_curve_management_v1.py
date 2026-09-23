"""Bounded single-pair virtual management on independently observed quotes.

No broker/account/runtime data I/O. Caller retains raw capture and durable plan
publication receipts. Reused USD arithmetic/selection is unchanged; observed
quote actions are a separate experiment, never claimed as actual broker fills.
"""
from copy import deepcopy
from decimal import Decimal, localcontext
import datetime as dt
import hashlib
import math
import time

import oanda_curve_management_replay_v1 as mechanics
import oanda_research_quote_receipt_v1 as quotes
import oanda_curve_momentum_observation_v1 as momentum
from oanda_forecast_curve_contract_v1 import content_hash, AUTHORITY

SCHEMA = 'observed_curve_management_v1_20260909'
CONFIG_SCHEMA = 'observed_curve_management_contract_v1_20260909'
STATE_SCHEMA = 'observed_curve_management_state_v1_20260909'
PLAN_SCHEMA = 'observed_curve_management_plan_v1_20260909'
PUBLICATION_SCHEMA = 'observed_management_plan_publication_v1_20260909'
ORIGINAL_CURVE_SCHEMA = 'native_curve_management_candidate_v1_20260909'
COMPATIBILITY_SCHEMA = 'observed_curve_decimal_clock_compatibility_v1_20260909'
NORMALIZER_SOURCE = 'oanda_observed_curve_management_v1.py'
SAFETY = mechanics.SAFETY
ARMS = mechanics.ARMS
INSTRUMENT = 'GBP_USD'
HARD_STOP_EPOCH = dt.datetime(2026, 9, 9, 13, tzinfo=dt.timezone.utc).timestamp()
MAX_OBSERVATIONS = 32
MAX_RAW_BYTES = 8 * 1024 * 1024
POLICY = dict(instrument=INSTRUMENT, initial_curve_only=True, price_convention='official_midpoint',
    decision_cadence_sec=60, maximum_decision_lateness_sec=10, execution_delay_sec=1,
    maximum_quote_wait_sec=10, maximum_quote_age_sec=5, entry_cutoff_before_target_sec=12,
    terminal_grace_sec=15, target_window_policy='nominal_management_boundary',
    earliest_quote='first_verified_current_tradeable_observation_after_plan_created_plus_delay_and_publication',
    target_scope='fixed_original_nominal_native_target_with_explicit_training_window_approximation',
    execution_scope='hypothetical_bid_ask_action_not_broker_fill',
    pending_state='no_next_decision_until_completed_settlement_is_observed')
CURVE_BINDING_KEYS = ('curve_id', 'curve_sha256', 'node_id', 'model_id', 'model_sha256',
    'forecast_cohort', 'reference_epoch', 'issued_epoch', 'original_target_epoch',
    'expected_terminal_price', 'source_bindings')
REUSED_BINDINGS = {
    'oanda_curve_management_adapter_v1.py': '8fa6cccf1e2525d0186ab7abe0c68fccc4800db7b785a8b9f79f28c320ad1241',
    'oanda_curve_management_replay_v1.py': '52fd04c6b087df4be775e490002afd0744caeb245604b8f577ab0fcff41de8f3',
    'src/forex_system/research/sequential_portfolio_replay_v1.py': mechanics.LEGACY_SOURCE_SHA256,
}


def need(ok, reason):
    if not ok:
        raise ValueError(reason)


def epoch(value):
    need(type(value) in (int, float) and math.isfinite(value) and 0 < value < 1e12, 'invalid_observed_clock')
    return float(value)


def digest(value):
    return mechanics.digest(value)


def sealed(body, key):
    value = mechanics.jsonable(body)
    return {**value, key: digest(value)}


def check_seal(value, key):
    need(isinstance(value, dict) and value.get(key) == digest({k:v for k,v in value.items() if k != key}),
         'sealed_record_hash_mismatch')


def check_flags(value):
    need(all(value.get(k) is v for k, v in SAFETY.items()), 'inert_research_flags_required')


def validate_config(config):
    need(isinstance(config, dict) and config.get('schema_version') == CONFIG_SCHEMA, 'config_schema')
    check_flags(config)
    need(config.get('policy') == POLICY, 'fixed_observation_policy_required')
    need(isinstance(config.get('study_id'), str) and 1 <= len(config['study_id']) <= 160, 'study_identity')
    created, start, target, stop = [epoch(config.get(k)) for k in
        ('created_epoch', 'start_epoch', 'native_target_epoch', 'hard_stop_epoch')]
    need(created <= start < target and target+15 <= stop <= HARD_STOP_EPOCH, 'session_native_target_or_hard_stop')
    need(target-start <= 3600, 'session_longer_than_native_hour')
    binding = config.get('curve_binding')
    need(isinstance(binding, dict) and all(k in binding for k in CURVE_BINDING_KEYS), 'initial_curve_binding_required')
    need(binding.get('price_convention') == 'official_midpoint', 'initial_midpoint_convention')
    need(epoch(binding['reference_epoch']) <= epoch(binding['issued_epoch']) <= created,
         'initial_curve_not_issued_before_registration')
    need(epoch(binding['original_target_epoch']) == target, 'initial_curve_native_target_mismatch')
    need(target-epoch(binding['reference_epoch']) == 3600, 'initial_native_h1_required')
    need(mechanics.number(binding['expected_terminal_price']) > 0, 'initial_terminal_price')
    for key in ('curve_sha256', 'model_sha256'):
        mechanics._require_sha(binding[key])
    for key in ('curve_id', 'node_id', 'model_id', 'forecast_cohort'):
        need(isinstance(binding[key], str) and bool(binding[key]), 'initial_curve_identity')
    sources = config.get('source_bindings')
    need(isinstance(sources, dict) and 1 <= len(sources) <= 64, 'source_bindings_required')
    for value in sources.values():
        mechanics._require_sha(value)
    mechanics._require_sha(sources.get(NORMALIZER_SOURCE))
    need(all(sources.get(k) == v for k, v in REUSED_BINDINGS.items()), 'reused_mechanics_source_binding')
    need(isinstance(binding['source_bindings'], dict) and binding['source_bindings']
         and all(sources.get(k) == v for k, v in binding['source_bindings'].items()), 'curve_source_closure')
    need(config.get('momentum_policy') == momentum.POLICY, 'fixed_momentum_policy_required')
    policy = mechanics.validate_config(config['usd_policy'])
    expected = dict(notional_usd='2500', maximum_entry_spread_bps='5', maximum_holding_sec='3600',
        cadence_sec='60', execution_delay_sec='1', quote_max_age_sec='5', minimum_entry_cost_ratio='1',
        slippage_bps_per_leg='0.1', switch_incremental_hurdle_usd='0.05')
    need(all(policy[k] == Decimal(v) for k, v in expected.items()), 'fixed_usd_experiment_policy_required')
    need(set(policy['metadata']) == {INSTRUMENT} and policy['metadata'][INSTRUMENT]['pip_size'] == Decimal('.0001'),
         'single_pair_metadata_required')
    need(policy['curve_target_window_policy'] == POLICY['target_window_policy'], 'nominal_management_policy_required')
    need(policy['legacy_policy'] == dict(minimum_score_cost_ratio=1.0,
         rotation_improvement_multiple=1.25, maximum_holding_min=60)
         and policy.get('feedback_horizon_sec') == 3600, 'fixed_legacy_reference_policy_required')
    return policy


def initial_states(config, *, clock=time.time):
    validate_config(config); observed = epoch(clock())
    need(config['created_epoch'] <= observed <= config['start_epoch'], 'initial_state_clock')
    return sealed(dict(schema_version=STATE_SCHEMA, config_sha256=digest(config), generation=0,
        known_epoch=observed, last_scheduled_epoch=None, last_plan_sha256=None,
        arms={arm: mechanics.flat_state() for arm in ARMS}, **SAFETY), 'state_sha256')


def _state(value, config):
    check_seal(value, 'state_sha256'); check_flags(value)
    need(value.get('schema_version') == STATE_SCHEMA and value.get('config_sha256') == digest(config), 'state_contract')
    need(type(value.get('generation')) is int and 0 <= value['generation'] <= 128, 'state_generation_bound')
    known = epoch(value.get('known_epoch'))
    need(set(value.get('arms', {})) == set(ARMS), 'five_separate_arm_states_required')
    for arm, state in value['arms'].items():
        mechanics.number(state['realized_usd'])
        pos = state.get('position')
        if pos is not None:
            need(arm != 'no_trade' and pos['instrument'] == INSTRUMENT and type(pos['side']) is int
                and pos['side'] in (-1, 1) and type(pos['base_units']) is int and pos['base_units'] > 0,
                'position_identity')
            need(epoch(pos['entry_decision_epoch']) < epoch(pos['entry_epoch']) <= known,
                 'position_future_at_state_observation')
            need(pos['original_target_epoch'] == config['native_target_epoch'], 'position_original_target_changed')
            need(mechanics.number(pos['entry_price']) > 0, 'position_entry_price')
    return known


def _curve_candidates(rows, binding, mapped, cutoff, source_bindings):
    need(isinstance(rows, list) and len(rows) <= 1, 'single_initial_curve_candidate_required')
    retained = []
    for row in rows:
        need(isinstance(row, dict) and row.get('status') == 'available'
             and row.get('schema_version') == ORIGINAL_CURVE_SCHEMA, 'curve_candidate_status_or_schema')
        need(row.get('candidate_sha256') == content_hash({k:v for k,v in row.items() if k != 'candidate_sha256'}),
             'curve_candidate_seal')
        need(all(row.get(k) == binding[k] for k in CURVE_BINDING_KEYS), 'initial_curve_replaced_or_revised')
        need(row.get('instrument') == INSTRUMENT and row.get('input_context', {}).get('price_convention') == 'official_midpoint',
             'curve_candidate_convention')
        need(all(row.get(k) is v for k, v in AUTHORITY.items()), 'curve_candidate_authority')
        need(row.get('probability_is_remaining_move_probability') is False, 'remaining_probability_not_authorized')
        need(row.get('target_selection_policy') == {'kind':'first_complete_bar_at_or_after_nominal', 'maximum_delay_sec':7}
             and row.get('target_is_exact') is False, 'original_training_target_window_required')
        need(row.get('decision_epoch') == cutoff, 'curve_information_cutoff')
        target = epoch(row['original_target_epoch'])
        decision = epoch(row['decision_epoch'])
        # The frozen adapter subtracts binary floats. Validate that original
        # representation exactly before translating it for the Decimal reader.
        need(type(row.get('remaining_sec')) is float and math.isfinite(row['remaining_sec'])
             and row['remaining_sec'] == target-decision, 'original_adapter_remaining_clock_mismatch')
        normalized = str(Decimal(str(target))-Decimal(str(decision)))
        body = deepcopy(row)
        del body['candidate_sha256']
        body.update(schema_version=COMPATIBILITY_SCHEMA, remaining_sec=normalized,
            original_candidate=deepcopy(row), original_candidate_sha256=row['candidate_sha256'],
            normalizer_source_bindings={NORMALIZER_SOURCE:source_bindings[NORMALIZER_SOURCE],
                'oanda_curve_management_adapter_v1.py':REUSED_BINDINGS['oanda_curve_management_adapter_v1.py']},
            remaining_clock_normalization=dict(original_schema_version=ORIGINAL_CURVE_SCHEMA,
                original_remaining_sec=row['remaining_sec'], normalized_remaining_sec=normalized,
                original_arithmetic='python_float_target_minus_decision_exact_equality_required',
                normalized_arithmetic='Decimal(str(original_target_epoch))-Decimal(str(decision_epoch))',
                timestamps_changed=False, original_candidate_changed=False,
                scope='numeric_representation_compatibility_only_no_tolerance_or_target_change'))
        retained.append(sealed(body, 'compatibility_sha256'))
    return retained


def plan_actions(states, frame, config, *, clock=time.time):
    """Choose all five actions before sampling the actual computation end clock."""
    with localcontext(mechanics.CTX):
        policy = validate_config(config); known = _state(states, config)
        cutoff = epoch(frame.get('decision_epoch')); scheduled = epoch(frame.get('scheduled_epoch'))
        terminal = frame.get('terminal'); need(type(terminal) is bool, 'explicit_terminal_flag')
        target = config['native_target_epoch']
        need(known <= cutoff <= config['hard_stop_epoch'], 'state_not_known_at_information_cutoff')
        need(scheduled >= config['start_epoch'] and scheduled <= cutoff, 'decision_schedule_future_or_before_start')
        if states['last_scheduled_epoch'] is not None:
            need(scheduled > states['last_scheduled_epoch'], 'duplicate_or_reversed_schedule_slot')
        if terminal:
            need(scheduled == target-1 and cutoff <= target+14, 'terminal_schedule_or_grace')
        else:
            slot = (scheduled-config['start_epoch'])/60
            need(abs(slot-round(slot)) <= 1e-8 and cutoff-scheduled <= 10 and cutoff < target,
                 'normal_cadence_late_or_off_grid')
        decision_quote_refusal = frame.get('decision_quote_refusal')
        if frame.get('quote_raw') is None and frame.get('quote_receipt') is None:
            need(terminal and isinstance(decision_quote_refusal, dict)
                 and isinstance(decision_quote_refusal.get('reason_code'), str)
                 and 0 < len(decision_quote_refusal['reason_code']) <= 256,
                 'terminal_only_explicit_missing_decision_quote')
            # The mandatory terminal action depends on the position/deadline,
            # not a made-up decision price. Later execution still needs a quote.
            mapped = None; quote_map = {}
        else:
            need(decision_quote_refusal is None, 'decision_quote_and_refusal_conflict')
            mapped = quotes.map_quote_snapshot(frame['quote_raw'], frame['quote_receipt'], decision_epoch=cutoff,
                instruments=(INSTRUMENT,), maximum_quote_age_sec=5)
            quote_map = mapped['quotes']
        refusals = {'curve':[], 'momentum':[]}; prepared = {}
        curve_rows = [] if terminal else _curve_candidates(frame.get('curve_candidates', []),
            config['curve_binding'], mapped, cutoff, config['source_bindings'])
        raw_momentum = None
        source = frame.get('momentum_source')
        if not terminal and source is not None:
            raw_momentum = momentum.build_momentum_candidate(source['raw_bytes'], source['receipt'], source['consumption'],
                metadata=quotes.METADATA[INSTRUMENT], decision_epoch=cutoff, target_epoch=target,
                quote=quote_map.get(INSTRUMENT))
            need(raw_momentum.get('schema_version') == momentum.SCHEMA
                 and all(raw_momentum.get(k) is v for k,v in momentum.FLAGS.items())
                 and raw_momentum.get('policy') == momentum.POLICY and all(
                config['source_bindings'].get(k) == v for k,v in raw_momentum['source_bindings'].items()), 'momentum_source_closure')
        momentum_rows = [raw_momentum] if raw_momentum is not None and raw_momentum['status'] == 'available' else []
        if not momentum_rows and not terminal:
            refusals['momentum'].append(deepcopy(raw_momentum) if raw_momentum is not None else {'reason':'momentum_source_missing'})
        for kind, rows in (('curve', curve_rows), ('momentum', momentum_rows)):
            prepared[kind], rejected = mechanics.prepare_candidates(rows, kind, quote_map, cutoff, target, policy)
            refusals[kind].extend(rejected)
            supplied = frame.get(kind+'_refusals', [])
            need(isinstance(supplied, list) and len(supplied) <= 32, 'bounded_candidate_refusals_required')
            refusals[kind].extend(deepcopy(supplied))
        decisions = {}; diagnostics = {}
        for arm in ARMS:
            state = states['arms'][arm]
            if arm == 'no_trade':
                decisions[arm] = mechanics._empty_decision('wait', cutoff, 'fixed_no_trade'); diagnostics[arm] = {}
            elif arm == 'legacy_momentum_reference':
                decisions[arm], diagnostics[arm] = mechanics.choose_legacy_action(state, prepared['momentum'], cutoff, policy, terminal=terminal)
            else:
                kind = 'momentum' if arm == 'usd_momentum_manager' else 'curve'
                decisions[arm], diagnostics[arm] = mechanics.choose_usd_action(state, prepared[kind], quote_map, cutoff, policy,
                    terminal=terminal, hold_only=arm == 'curve_hold_no_rotation')
        completed = epoch(clock())
        need(cutoff <= completed <= config['hard_stop_epoch'], 'plan_computation_clock')
        return sealed(dict(schema_version=PLAN_SCHEMA, config_sha256=digest(config), study_id=config['study_id'],
            before_state_sha256=states['state_sha256'], before_generation=states['generation'],
            scheduled_epoch=scheduled, decision_epoch=cutoff, information_cutoff_epoch=cutoff,
            plan_created_epoch=completed, terminal=terminal, native_target_epoch=target,
            decision_quote_mapping=mapped, decisions=decisions, decision_diagnostics=diagnostics,
            decision_quote_refusal=deepcopy(decision_quote_refusal),
            curve_candidate_originals=deepcopy(frame.get('curve_candidates', [])) if not terminal else [],
            curve_candidate_compatibility=deepcopy(curve_rows),
            prepared_candidate_evidence={k:[mechanics._candidate_evidence(row) for row in rows] for k,rows in prepared.items()},
            candidate_refusals=refusals, momentum_candidate=raw_momentum,
            cadence_lateness_sec=cutoff-scheduled,
            scopes=['All five choices completed before the actual plan-created clock and before later quote observations.',
                'Legacy selector reference uses the new predeclared S5 momentum heuristic, not old deployed-candidate equivalence.',
                'No position action or broker fill occurs during planning.'], **SAFETY), 'plan_sha256')


def make_plan_publication(plan, *, started_epoch, completed_epoch):
    check_seal(plan, 'plan_sha256'); check_flags(plan)
    started, completed = epoch(started_epoch), epoch(completed_epoch)
    need(plan['plan_created_epoch'] <= started <= completed, 'plan_publication_clock')
    return sealed(dict(schema_version=PUBLICATION_SCHEMA, plan_sha256=plan['plan_sha256'],
        publication_started_epoch=started, publication_completed_epoch=completed,
        scope='caller_attested_completed_durable_plan_write', **SAFETY), 'publication_sha256')


def _virtual_action(state, decision, quote_map, observed, policy):
    """Atomic observed-quote accounting; original market clocks stay unchanged."""
    before = deepcopy(state); action = decision['action']
    if action in ('wait', 'hold'):
        need((action == 'wait') == (state['position'] is None), 'action_position_mismatch')
        return before, {'status':'applied_virtual_action', 'legs':[], 'realized_delta_usd':Decimal(0)}
    try:
        after = deepcopy(before); legs = []
        q = mechanics.quote_at(quote_map, INSTRUMENT, observed, 5)
        if action in ('enter', 'rotate'):
            need(decision['instrument'] == INSTRUMENT and type(decision['side']) is int and decision['side'] in (-1,1),
                 'virtual_open_identity')
            need(type(decision['base_units']) is int and decision['base_units'] > 0, 'decision_integer_units_required')
            need((q['ask']-q['bid'])/q['mid']*10000 <= policy['maximum_entry_spread_bps'], 'observed_entry_spread_above_limit')
        if action in ('exit', 'rotate'):
            pos = after['position']; need(pos is not None, 'cannot_close_flat')
            price, slip = mechanics._executed_price(q, pos['side'], False, policy)
            pnl = pos['side']*pos['base_units']*(price-mechanics.number(pos['entry_price']))
            usd, conversion = mechanics.convert_pnl_to_usd(pnl, 'USD', quote_map, observed, 5)
            legs.append(dict(kind='close', instrument=INSTRUMENT, side=pos['side'], base_units=pos['base_units'],
                price=price, slippage_price=slip, quote_id=q['quote_id'], market_epoch=q['market_epoch'],
                available_epoch=q['available_epoch'], virtual_action_epoch=observed, realized_usd=usd,
                quote_currency_pnl=pnl, conversion=conversion, original_entry=pos))
            after['realized_usd'] = mechanics.number(after['realized_usd'])+usd;after['position'] = None
        if action in ('enter', 'rotate'):
            need(after['position'] is None, 'cannot_open_while_positioned')
            price, slip = mechanics._executed_price(q, decision['side'], True, policy)
            pos = dict(instrument=INSTRUMENT, side=decision['side'], base_units=decision['base_units'],
                entry_epoch=observed, entry_price=price, entry_quote_id=q['quote_id'],
                entry_decision_epoch=decision['decision_epoch'], sizing=decision['sizing'], candidate_id=decision['candidate_id'],
                original_target_epoch=decision['original_target_epoch'])
            after['position'] = pos
            legs.append(dict(kind='open', instrument=INSTRUMENT, side=pos['side'], base_units=pos['base_units'],
                price=price, slippage_price=slip, quote_id=q['quote_id'], market_epoch=q['market_epoch'],
                available_epoch=q['available_epoch'], virtual_action_epoch=observed, realized_usd=Decimal(0), position=pos))
        return after, dict(status='applied_virtual_action', legs=legs,
            realized_delta_usd=mechanics.number(after['realized_usd'])-mechanics.number(before['realized_usd']))
    except (ValueError, KeyError, TypeError) as exc:
        return before, dict(status='no_virtual_fill', reason_code=str(exc), legs=[], realized_delta_usd=Decimal(0))


def _mark(state, mapped, observed, policy):
    if state['position'] is None:
        return dict(status='available_flat', liquidation_usd=state['realized_usd'])
    after, result = _virtual_action(state, {'action':'exit'}, mapped, observed, policy)
    return dict(status='available' if result['status']=='applied_virtual_action' else 'unavailable',
        liquidation_usd=after['realized_usd'] if result['status']=='applied_virtual_action' else None,
        hypothetical_liquidation=result, broker_fill_observed=False)


def settle_actions(states, plan, publication, quote_observations, *, as_of_epoch, config, clock=time.time):
    """Elect the first eligible common quote; complete once or retain pending state."""
    with localcontext(mechanics.CTX):
        policy = validate_config(config); _state(states, config); check_seal(plan, 'plan_sha256'); check_flags(plan)
        check_seal(publication, 'publication_sha256');check_flags(publication)
        now = epoch(as_of_epoch)
        need(plan['schema_version']==PLAN_SCHEMA and plan['config_sha256']==digest(config)
             and plan['before_state_sha256']==states['state_sha256'] and plan['before_generation']==states['generation'],
             'plan_state_or_config_mismatch')
        need(publication['schema_version']==PUBLICATION_SCHEMA and publication['plan_sha256']==plan['plan_sha256'],
             'plan_publication_identity')
        pub = epoch(publication['publication_completed_epoch'])
        need(plan['plan_created_epoch'] <= epoch(publication['publication_started_epoch']) <= pub <= now,
             'publication_not_completed_before_observation')
        target = config['native_target_epoch']; terminal = plan['terminal']
        due = max(plan['plan_created_epoch']+1, pub, target if terminal else 0)
        deadline = target+15 if terminal else due+10
        need(isinstance(quote_observations, (list, tuple)) and len(quote_observations)<=MAX_OBSERVATIONS, 'observation_count_bound')
        need(all(isinstance(row, dict) and type(row.get('raw_bytes')) is bytes
                 and isinstance(row.get('receipt'), dict) for row in quote_observations),
             'observation_raw_bytes_and_receipt_shape_required')
        need(sum(len(row['raw_bytes']) for row in quote_observations)<=MAX_RAW_BYTES, 'observation_byte_bound')
        observed_rows=[]; reasons=[]; identities={}
        for row in quote_observations:
            try:
                raw, receipt = row['raw_bytes'], row['receipt']
                observed = epoch(receipt['read_completed_epoch']);started=epoch(receipt['read_started_epoch'])
                need(observed <= now, 'future_quote_observation')
                mapped = quotes.map_quote_snapshot(raw, receipt, decision_epoch=observed,
                    instruments=(INSTRUMENT,), maximum_quote_age_sec=5)
                key=receipt['receipt_sha256']
                need(key not in identities or identities[key]==hashlib.sha256(raw).hexdigest(), 'conflicting_observation_identity')
                identities[key]=hashlib.sha256(raw).hexdigest()
                need(started>=pub, 'quote_read_started_before_plan_publication')
                need(due<=observed<=deadline, 'quote_observation_outside_action_window')
                need(INSTRUMENT in mapped['quotes'], mapped['refusals'].get(INSTRUMENT,'required_quote_unavailable'))
                observed_rows.append((observed,key,mapped))
            except (ValueError, KeyError, TypeError) as exc:
                reasons.append(dict(reason_code=str(exc), receipt_sha256=row.get('receipt',{}).get('receipt_sha256')))
        selected=min(observed_rows,key=lambda item:(item[0],item[1])) if observed_rows else None
        if selected is None and now<deadline and due<=deadline:
            completed = epoch(clock())
            need(now <= completed <= config['hard_stop_epoch'], 'settlement_computation_clock')
            return sealed(dict(schema_version=SCHEMA, status='pending_quote', plan_sha256=plan['plan_sha256'],
                as_of_epoch=now, settlement_computed_epoch=completed,
                observation_not_before_epoch=due, observation_deadline_epoch=deadline,
                source_refusals=reasons, states=deepcopy(states), **SAFETY), 'settlement_sha256')
        arm_rows={};new_arms={}
        for arm in ARMS:
            before=states['arms'][arm];decision=plan['decisions'][arm]
            if selected is None:
                after=deepcopy(before);action=dict(status='no_virtual_fill',reason_code='no_eligible_observed_quote_within_window',
                    legs=[],realized_delta_usd=Decimal(0))
                mark=(dict(status='available_flat',liquidation_usd=before['realized_usd']) if before['position'] is None
                      else dict(status='unavailable',liquidation_usd=None))
            else:
                observed,_,mapped=selected
                reason=None
                if decision['action'] in ('enter','rotate') and not (
                    plan['plan_created_epoch']<=target-12 and deadline<target and observed<target):
                    reason='complete_entry_observation_window_not_before_native_target'
                if decision['action']=='hold' and before['position'] is not None and observed>=target:
                    reason='hold_would_cross_original_native_target'
                if reason:
                    after=deepcopy(before);action=dict(status='no_virtual_fill',reason_code=reason,legs=[],realized_delta_usd=Decimal(0))
                else:
                    after,action=_virtual_action(before,decision,mapped['quotes'],observed,policy)
                mark=_mark(after,mapped['quotes'],observed,policy)
            new_arms[arm]=after
            arm_rows[arm]=dict(decision=decision,state_before=before,state_after=after,virtual_action=action,
                liquidation_mark=mark,terminal_flat=after['position'] is None,
                native_deadline_lateness_sec=max(0,selected[0]-target) if terminal and selected else None,
                broker_fill_observed=False,counts_as_independent_market_trial=False)
        completed = epoch(clock())
        need(now <= completed <= config['hard_stop_epoch'], 'settlement_computation_clock')
        next_state=sealed(dict(schema_version=STATE_SCHEMA,config_sha256=digest(config),generation=states['generation']+1,
            known_epoch=completed,last_scheduled_epoch=plan['scheduled_epoch'],last_plan_sha256=plan['plan_sha256'],
            arms=new_arms,**SAFETY),'state_sha256')
        return sealed(dict(schema_version=SCHEMA,status='completed',config_sha256=digest(config),
            plan_sha256=plan['plan_sha256'],publication_sha256=publication['publication_sha256'],
            as_of_epoch=now,settlement_computed_epoch=completed,
            observation_not_before_epoch=due,observation_deadline_epoch=deadline,
            selected_observation_epoch=selected[0] if selected else None,
            selected_quote_mapping=selected[2] if selected else None,source_refusals=reasons,
            arms=arm_rows,states=next_state,native_target_epoch=target,terminal=terminal,
            terminal_all_flat=all(s['position'] is None for s in new_arms.values()) if terminal else None,
            independent_sample_size=None,broker_fills_performed=False,**SAFETY),'settlement_sha256')
