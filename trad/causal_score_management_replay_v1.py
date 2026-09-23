"""Thin retrospective numerical comparison using unchanged USD mechanics.

This is a one-position fixed-notional experiment, not the legacy multi-position
ATR-risk/margin allocator. No I/O, historical publication authentication or
trading authority is supplied. Only known state crosses decision boundaries.
"""
from copy import deepcopy
from decimal import Decimal, localcontext
import causal_score_gate_v1 as gate
import oanda_curve_management_replay_v1 as mechanics
import oanda_curve_management_channels_v1 as channels

SCHEMA = 'causal_static_score_matched_management_v1_20260913'
ARMS = ('ungated_entry', 'past_score_entry', 'past_score_hold', 'no_trade')
MAX_REPLAY_SEEN = 4096
MAX_REPLAY_HISTORY = 512
MAX_INPUT_BYTES = 1024 * 1024
MAX_STATE_BYTES = 2 * 1024 * 1024
MAX_ROW_BYTES = 2 * 1024 * 1024
MAX_OUTPUT_BYTES = 16 * 1024 * 1024
MAX_TREE_DEPTH = 32
MAX_TREE_NODES = 131072


def _estimated_bytes(value, limit):
    """Conservative JSON byte bound before copying/encoding known-only data."""
    total = 0; nodes = 0; stack = [(value, 0)]
    while stack:
        item, depth = stack.pop(); nodes += 1
        gate.need(depth <= MAX_TREE_DEPTH and nodes <= MAX_TREE_NODES, 'bounded_replay_tree_required')
        kind = type(item)
        if item is None:
            total += 4
        elif kind is bool:
            total += 5
        elif kind is str:
            total += 2 + 6 * len(item)
        elif kind in (int, float, Decimal):
            gate.number(item); total += len(gate.canonical(item))
        elif kind is dict:
            gate.need(len(item) <= MAX_TREE_NODES - nodes, 'bounded_replay_tree_required')
            gate.need(all(type(key) is str for key in item), 'string_replay_keys_required')
            total += 2 + 2 * len(item)
            for key, child in item.items():
                stack.append((key, depth + 1)); stack.append((child, depth + 1))
        elif kind in (list, tuple):
            gate.need(len(item) <= MAX_TREE_NODES - nodes, 'bounded_replay_tree_required')
            total += 2 + len(item)
            stack.extend((child, depth + 1) for child in item)
        else:
            raise ValueError('plain_replay_value_required')
        gate.need(total <= limit, 'replay_byte_budget_exceeded')
    return total


def _compact_score_receipt(scored, prior_state):
    """Final state plus immutable deltas reconstruct prior inventories once."""
    next_state = scored['next_score_state']
    new = [{'sample_id': identity, 'sample_sha256': gate.digest(item['sample']),
            'first_seen_decision_epoch': item['first_seen_decision_epoch']}
           for identity, item in sorted(next_state['seen'].items()) if identity not in prior_state['seen']]
    receipts = [{**{key: value for key, value in receipt.items() if key != 'history_sample_ids'},
                 'history_sample_ids_sha256': gate.digest(receipt['history_sample_ids'])}
                for receipt in scored['percentile_receipts']]
    return {'schema': scored['schema'], 'decision_epoch': scored['decision_epoch'],
        'prior_state_sha256': scored['prior_state_sha256'], 'next_state_sha256': next_state['state_sha256'],
        'policy_sha256': scored['policy_sha256'], 'new_observations': new,
        'entry_references': [{'instrument': row['instrument'], 'candidate_id': row['candidate_id'],
                              'prepared_row_sha256': gate.digest(row)} for row in scored['entry_rows']],
        'continuation_count': len(scored['continuation_rows']),
        'continuation_rows_sha256': gate.digest(scored['continuation_rows']),
        'refusals': deepcopy(scored['refusals']), 'percentile_receipts': receipts,
        'research_only': True, 'can_place_orders': False, 'provenance_authenticated_here': False}

def _nav(state, quotes, epoch, config, balance, *, observed=None):
    value = mechanics.liquidation_equity(state, quotes, epoch, config, observed_epoch=observed)
    return {**value, 'starting_balance_usd': balance,
            'nav_usd': None if value['equity_usd'] is None else balance + value['equity_usd']}

def _prepare_exact(rows, quotes, decision, target, config):
    gate.need(type(rows) in (list, tuple) and len(rows) <= 68, 'bounded_prepared_continuation_required')
    gate._check_no_outcomes(rows)
    gate.need(all(type(row) is dict and type(row.get('source_payload')) is dict for row in rows), 'prepared_source_required')
    originals = [row['source_payload'] for row in rows]
    prepared, refusals = mechanics.prepare_candidates(originals, 'curve', quotes, decision, target, config)
    gate.need(not refusals and len(prepared) == len(rows), 'current_preparation_refused')
    expected = sorted((mechanics.digest(row) for row in prepared))
    supplied = sorted((mechanics.digest(row) for row in rows))
    gate.need(expected == supplied, 'prepared_current_economics_changed')
    return deepcopy(list(rows))

def replay_matched_frames(frames, *, gate_policy, mechanics_config, decision_epochs,
                         starting_balance_usd, as_of_epoch):
    """No artificial terminal action at a truncated prefix's last observation.

    The full original session schedule stays fixed when comparing prefixes.
    Future execution/feedback maps are accessed only after all decisions are
    frozen. Late settlement evidence embargoes subsequent decisions until its
    observation clock; unobserved settlements never become visible state.
    """
    with localcontext(mechanics.CTX):
        return _replay(frames, gate_policy, mechanics_config, decision_epochs,
                       starting_balance_usd, as_of_epoch)

def _replay(frames, gate_policy, raw_config, schedule, starting_balance, as_of):
    _estimated_bytes([gate_policy, raw_config, starting_balance], MAX_INPUT_BYTES)
    config = mechanics.validate_config(raw_config); gate.validate_policy(gate_policy)
    gate.need(gate_policy['maximum_seen'] <= MAX_REPLAY_SEEN and
              gate_policy['maximum_history'] <= MAX_REPLAY_HISTORY, 'bounded_replay_history_policy_required')
    balance = mechanics.number(starting_balance, 'starting_balance')
    gate.need(balance > 0, 'positive_starting_balance_required')
    cutoff = gate.epoch(as_of)
    gate.need(type(schedule) is list and 2 <= len(schedule) <= 4096, 'bounded_original_schedule_required')
    _estimated_bytes(schedule, MAX_INPUT_BYTES)
    schedule = [gate.epoch(value) for value in schedule]
    gate.need(all(b-a == float(config['cadence_sec']) for a,b in zip(schedule,schedule[1:])), 'fixed_unique_cadence_required')
    gate.need(type(frames) in (list,tuple) and 0 <= len(frames) <= len(schedule), 'bounded_prefix_frames_required')
    states = {arm: mechanics.flat_state() for arm in ARMS}
    pending = None; score_state = gate.initial_state(gate_policy); rows = []; retained_bytes = 0
    def retain(row):
        nonlocal retained_bytes
        row_bytes = _estimated_bytes(row, MAX_ROW_BYTES)
        state_bytes = _estimated_bytes(score_state, MAX_STATE_BYTES)
        # Leave one MiB for final states, pending action and schedule metadata.
        gate.need(retained_bytes + row_bytes + state_bytes <= MAX_OUTPUT_BYTES - MAX_INPUT_BYTES,
                  'replay_output_budget_exceeded')
        rows.append(row); retained_bytes += row_bytes
    for index, frame in enumerate(frames):
        decision = schedule[index]
        if decision > cutoff:
            break
        gate.need(type(frame) is dict, 'frame_object_required')
        gate.need(gate.epoch(frame.get('decision_epoch')) == decision, 'frame_schedule_identity_mismatch')
        terminal = index == len(schedule)-1
        gate.need(frame.get('terminal') is terminal, 'original_terminal_identity_required')
        if pending is not None and not pending['unresolved_execution'] and pending['observed_epoch'] <= decision:
            states = deepcopy(pending['states']); pending = None
        if pending is not None:
            # Do not access this frame's current forecasts or future market maps.
            known = {'decision_epoch': decision, 'status': 'pending_observation_embargo',
                     'visible_states': deepcopy(states), 'pending_decision_epoch': pending['decision_epoch']}
            retain({**known, 'decision_prefix_sha256': mechanics.digest(known)})
            continue
        target = gate.epoch(frame.get('management_target_epoch'))
        execution = decision + float(config['execution_delay_sec'])
        gate.need(gate.epoch(frame.get('execution_epoch')) == execution, 'precommitted_execution_clock_required')
        if not terminal:
            gate.need(execution < target <= schedule[-1] + float(config['execution_delay_sec']), 'original_target_outside_session')
            gate.need(target in {value+float(config['execution_delay_sec']) for value in schedule}, 'native_target_off_original_grid')
        quotes = frame.get('decision_quotes', {})
        gate.need(type(quotes) is dict and len(quotes) <= 136, 'bounded_decision_quotes_required')
        _estimated_bytes(quotes, MAX_INPUT_BYTES)
        if terminal:
            prepared = []; scored = None
        else:
            _estimated_bytes([frame.get('prepared_continuation', []), frame.get('score_records', [])], MAX_INPUT_BYTES)
            _estimated_bytes(score_state, MAX_STATE_BYTES)
            prepared = _prepare_exact(frame.get('prepared_continuation', []), quotes, decision, target, config)
            scored = gate.build_score_channels(prepared, frame.get('score_records', []), score_state,
                decision_epoch=decision, policy=gate_policy)
            _estimated_bytes(scored['next_score_state'], MAX_STATE_BYTES)
        decisions = {}; diagnostics = {}; before_marks = {}
        for arm in ARMS:
            state = states[arm]
            mark = _nav(state, quotes, decision, config, balance); before_marks[arm] = mark
            if arm == 'no_trade':
                decisions[arm] = mechanics._empty_decision('wait', decision, 'fixed_no_trade')
                diagnostics[arm] = {}
                continue
            position = state['position']
            deadline = (min(position['original_target_epoch'], position['entry_epoch'] + float(config['maximum_holding_sec']))
                        if position is not None else None)
            deadline_exit = position is not None and execution >= deadline
            if mark['nav_usd'] is None and not terminal and not deadline_exit:
                decisions[arm] = mechanics._empty_decision('hold' if position is not None else 'wait', decision, 'current_nav_unavailable')
                diagnostics[arm] = {'reason': 'unmarked_nav_no_new_entry_or_rotation'}
                continue
            entries = ([] if terminal else [row for row in prepared if row['side'] != 0]
                       if arm == 'ungated_entry' else scored['entry_rows'])
            selection = channels.choose_usd_action_with_channels(state, entries, prepared, quotes, decision,
                raw_config, terminal=terminal or deadline_exit, hold_only=arm == 'past_score_hold')
            decisions[arm] = selection['decision']; diagnostics[arm] = selection['diagnostics']
        # This block is complete before execution or feedback maps are accessed.
        known = {'decision_epoch': decision, 'terminal': terminal, 'original_management_target_epoch': target,
            'visible_states_before': deepcopy(states), 'decision_quotes': deepcopy(quotes),
            'prepared_continuation': deepcopy(prepared),
            'score_gate': None if scored is None else _compact_score_receipt(scored, score_state),
            'decisions': deepcopy(decisions), 'diagnostics': diagnostics, 'decision_nav': before_marks}
        decision_hash = mechanics.digest(known)
        if scored is not None:
            score_state = scored['next_score_state']
        if execution > cutoff:
            retain({**known, 'decision_prefix_sha256': decision_hash,
                         'settlement_status': 'execution_not_yet_in_observed_prefix'})
            pending = {'decision_epoch': decision, 'execution_epoch': execution, 'observed_epoch': None, 'states': deepcopy(states),
                       'unresolved_execution': True, 'decisions': deepcopy(decisions)}
            continue
        observed = gate.epoch(frame.get('execution_observed_epoch'))
        gate.need(observed >= execution, 'settlement_observation_precedes_market_execution')
        if observed > cutoff:
            retain({**known, 'decision_prefix_sha256': decision_hash,
                         'settlement_status': 'execution_evidence_not_observed_by_cutoff'})
            # The actual later receipt clock is not information in this as-of
            # prefix. Keep only a pending precommitted action and continue
            # recording every scheduled embargo inside the requested prefix.
            pending = {'decision_epoch': decision, 'execution_epoch': execution, 'observed_epoch': None, 'states': deepcopy(states),
                       'unresolved_execution': True, 'decisions': deepcopy(decisions)}
            continue
        execution_quotes = frame.get('execution_quotes', {})
        gate.need(type(execution_quotes) is dict and len(execution_quotes) <= 136, 'bounded_execution_quotes_required')
        _estimated_bytes(execution_quotes, MAX_INPUT_BYTES)
        after = {}; settlements = {}; marks = {}; feedback = {}
        for arm in ARMS:
            after[arm], settlements[arm] = mechanics.apply_action(states[arm], decisions[arm], execution_quotes,
                execution, config, observed_epoch=observed)
            marks[arm] = _nav(after[arm], execution_quotes, execution, config, balance, observed=observed)
        # Future feedback is an isolated descriptive branch; it never changes state.
        if frame.get('feedback_epoch') is not None:
            feedback_observed = gate.epoch(frame.get('feedback_observed_epoch'))
            if feedback_observed <= cutoff:
                feedback_epoch = gate.epoch(frame['feedback_epoch'])
                gate.need(execution <= feedback_epoch <= feedback_observed, 'feedback_clock_order')
                feedback_quotes = frame.get('feedback_quotes', {})
                gate.need(type(feedback_quotes) is dict and len(feedback_quotes) <= 136, 'bounded_feedback_quotes_required')
                _estimated_bytes(feedback_quotes, MAX_INPUT_BYTES)
                feedback = {arm: _nav(after[arm], feedback_quotes, feedback_epoch, config, balance,
                                     observed=feedback_observed) for arm in ARMS}
        retain({**known, 'decision_prefix_sha256': decision_hash, 'execution_epoch': execution,
            'execution_observed_epoch': observed, 'settlement_status': 'observed_retrospective_hypothetical_action',
            'settlements': settlements, 'post_execution_nav': marks, 'descriptive_feedback': feedback})
        pending = {'decision_epoch': decision, 'observed_epoch': observed, 'states': after, 'unresolved_execution': False}
    if pending is not None and not pending['unresolved_execution'] and pending['observed_epoch'] <= cutoff:
        states = deepcopy(pending['states']); pending = None
    public_pending = (None if pending is None else {
        'decision_epoch': pending['decision_epoch'], 'execution_epoch': pending['execution_epoch'],
        'unresolved_execution': True, 'decisions': deepcopy(pending['decisions']),
        'status': 'precommitted_action_not_observed_by_cutoff', 'visible_states_before': deepcopy(states)})
    result = {'schema': SCHEMA, 'arms': ARMS, 'rows': rows, 'visible_final_states': states,
        'pending_settlement': public_pending, 'score_state': score_state, 'original_decision_schedule': schedule,
        'as_of_epoch': cutoff, 'starting_balance_usd': balance, 'mechanics_config_sha256': mechanics.digest(raw_config),
        'gate_policy_sha256': gate.digest(gate_policy), 'research_only': True, 'can_place_orders': False,
        'broker_fill_observed': False, 'forecast_chain_authenticated_here': False,
        'scope': 'One-position fixed-USD-notional numerical comparison; no ATR risk, broker margin or multi-position simulation.',
        'receipt_format': 'compact_score_deltas_with_final_inventory_once_v1',
        'maximum_serialized_bytes': MAX_OUTPUT_BYTES}
    _estimated_bytes(result, MAX_OUTPUT_BYTES)
    gate.need(len(gate.canonical(result)) <= MAX_OUTPUT_BYTES, 'replay_serialized_output_budget_exceeded')
    return result
