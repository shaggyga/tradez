"""Pure USD selector with separate prepared entry and continuation channels.

Inputs are prepared research candidates, not broker state or authenticated
forecast chains. The caller must replay original curve and admission contracts
before preparation. This module deliberately performs no direction admission,
freshness certification, I/O, settlement, or activation. An entry channel is a
subset of the same prepared continuation inventory; filtering entries cannot
delete an incumbent's signed estimate. Existing economics remain unchanged.
"""
from copy import deepcopy
from decimal import localcontext

import oanda_curve_management_replay_v1 as mechanics

SCHEMA = 'separate_entry_continuation_usd_selector_v1_20260909'


def need(condition, reason):
    if not condition:
        raise ValueError(reason)


def _channel(rows, epoch):
    need(isinstance(rows, (list, tuple)) and len(rows) <= 68, 'bounded_prepared_channel_required')
    result, by_pair = [], {}
    for original in rows:
        need(isinstance(original, dict), 'prepared_candidate_required')
        row = deepcopy(original)
        pair = mechanics.pair_name(row.get('instrument'))
        need(pair not in by_pair, 'ambiguous_channel_instrument')
        need(row.get('kind') == 'curve', 'prepared_curve_candidate_required')
        need(mechanics.clock(row.get('decision_epoch')) == epoch and
             mechanics.clock(row.get('available_epoch')) <= epoch, 'channel_decision_clock')
        need(mechanics.clock(row.get('original_target_epoch')) > epoch, 'channel_target_elapsed')
        need(type(row.get('side')) is int and row['side'] in (-1, 0, 1), 'channel_side')
        need(type(row.get('base_units')) is int and row['base_units'] > 0, 'channel_integer_units')
        need(isinstance(row.get('source_payload'), dict), 'prepared_source_payload_required')
        for field in ('signed_price_change', 'expected_terminal_price', 'current_mid',
                      'expected_gross_usd', 'round_trip_cost_usd', 'new_entry_net_usd', 'score'):
            row[field] = mechanics.number(row.get(field), field)
        need(row['round_trip_cost_usd'] > 0 and row['expected_terminal_price'] > 0 and
             row['current_mid'] > 0, 'channel_positive_price_and_cost')
        need(row['new_entry_net_usd'] == row['expected_gross_usd'] - row['round_trip_cost_usd'] and
             row['score'] == row['expected_gross_usd'] / row['round_trip_cost_usd'],
             'prepared_channel_economics_changed')
        need(row['side'] * row['signed_price_change'] >= 0 and
             (row['side'] == 0) == (row['signed_price_change'] == 0), 'channel_signed_direction')
        by_pair[pair] = row
        result.append(row)
    result.sort(key=lambda r: (-r['new_entry_net_usd'], r['instrument'], -r['side']))
    return result, by_pair


def choose_usd_action_with_channels(state, entry_candidates, continuation_candidates,
                                   quotes, decision_epoch, config, *, terminal=False,
                                   hold_only=False):
    """Return a sealed, inert decision using the existing USD valuation formula.

    Both channels contain original output rows from ``prepare_candidates``
    computed inside ``with localcontext(mechanics.CTX)``, as in the original
    replay pipeline. Other preparation precision is refused rather than repriced.
Entry candidates have already passed the caller's separately validated semantic
gate. Neutral or refused entries stay in continuation. Every entry row must
match its continuation row exactly; mixing valuations or targets is rejected.
No supplied admission is certified here. Outcome testing and a chain-replaying
study adapter are still required before a new paper protocol can be activated.
"""
    with localcontext(mechanics.CTX):
        policy = mechanics.validate_config(config)
        epoch = mechanics.clock(decision_epoch)
        need(type(terminal) is bool and type(hold_only) is bool, 'explicit_selector_flags')
        need(isinstance(state, dict) and 'position' in state, 'explicit_position_state_required')
        position = state['position']
        if position is not None:
            need(isinstance(position, dict), 'position_object_required')
            mechanics.pair_name(position.get('instrument'))
            need(type(position.get('side')) is int and position['side'] in (-1, 1), 'position_side')
            need(type(position.get('base_units')) is int and position['base_units'] > 0, 'position_integer_units')
            need(mechanics.clock(position.get('entry_epoch')) <= epoch, 'position_from_future')
            mechanics.clock(position.get('original_target_epoch'))
        # Terminal exits do not depend on obtaining a valid forecast channel.
        if terminal:
            decision = mechanics._empty_decision('exit' if position else 'wait', epoch, 'predeclared_terminal')
            diagnostics, entries, continuing = {}, [], []
        else:
            entries, _ = _channel(entry_candidates, epoch)
            continuing, by_pair = _channel(continuation_candidates, epoch)
            for row in entries:
                need(row['side'] != 0, 'neutral_new_entry_not_admitted')
                need(row['instrument'] in by_pair and mechanics.digest(row) ==
                     mechanics.digest(by_pair[row['instrument']]), 'entry_not_exact_continuation_subset')
            decision, diagnostics = _choose(state, entries, continuing, quotes, epoch,
                                            policy, hold_only=hold_only)
        body = dict(schema_version=SCHEMA, **mechanics.SAFETY, positions_managed=False,
            manager_activation=False, decision_epoch=epoch,
            decision=mechanics.jsonable(decision), diagnostics=mechanics.jsonable(diagnostics),
            input_state_sha256=mechanics.digest(state),
            validated_config_sha256=mechanics.digest(policy),
            supplied_quote_inventory_sha256=mechanics.digest(quotes),
            prepared_entry_channel_sha256=mechanics.digest(entries),
            prepared_continuation_channel_sha256=mechanics.digest(continuing),
            entry_candidate_count=len(entries), continuation_candidate_count=len(continuing),
            terminal=terminal, hold_only=hold_only,
            semantic_direction_admission_verified_here=False,
            original_forecast_chain_replayed_here=False,
            fresh_inputs_verified=False, computation_clock_receipt=False,
            scope='pure_prepared_candidate_selection_only_no_actions_or_settlement')
        return {**body, 'selection_sha256': mechanics.digest(body)}


def _choose(state, entries, continuing, quotes, epoch, config, *, hold_only):
    """Existing USD selector economics, with only the two channel lookups split."""
    position = state.get('position')
    eligible = [r for r in entries if r['score'] >= mechanics.number(config['minimum_entry_cost_ratio'])]
    best = eligible[0] if eligible else None
    if position is None:
        if best is None:
            return mechanics._empty_decision('wait', epoch, 'no_candidate_clears_cost_hurdle'), {}
        return mechanics._candidate_decision(best, 'enter', 'highest_expected_net_usd'), {}
    deadline = min(float(position['entry_epoch']) + float(config['maximum_holding_sec']),
                   float(position['original_target_epoch']))
    if epoch + float(config['execution_delay_sec']) >= deadline:
        return mechanics._empty_decision('exit', epoch, 'predeclared_holding_or_native_target_deadline'), {'position_deadline': deadline}
    if hold_only:
        return mechanics._empty_decision('hold', epoch, 'hold_current_no_rotation'), {'position_deadline': deadline}
    current = next((c for c in continuing if c['instrument'] == position['instrument']), None)
    if current is None:
        return mechanics._empty_decision('exit', epoch, 'incumbent_estimate_unavailable'), {'incumbent_estimate_status': 'unavailable'}
    if current['original_target_epoch'] != deadline:
        return mechanics._empty_decision('exit', epoch, 'incumbent_native_target_incomparable'), {
            'incumbent_estimate_status': 'incomparable_deadline', 'position_deadline': deadline,
            'candidate_native_target': current['original_target_epoch']}
    try:
        pair = position['instrument']; meta = config['metadata'][pair]
        q = mechanics.quote_at(quotes, pair, epoch, config['quote_max_age_sec'])
        conversion = mechanics.usd_rates(meta['quote_currency'], quotes, epoch, config['quote_max_age_sec'])
        units = position['base_units']
        expected_quote = units * position['side'] * current['signed_price_change']
        rate = conversion['sell_currency_usd'] if expected_quote >= 0 else conversion['buy_currency_usd']
        gross = expected_quote * rate
        slip = q['mid'] * mechanics.number(config['slippage_bps_per_leg']) / 10000
        liquidation_cost = units * ((q['ask'] - q['bid']) / 2 + slip) * conversion['buy_currency_usd']
        hold_value, exit_value = gross - liquidation_cost, -liquidation_cost
        alternatives = [c for c in eligible if c['instrument'] != pair or c['side'] != position['side']]
        best = alternatives[0] if alternatives else None
        switch_value = best['new_entry_net_usd'] - liquidation_cost if best else None
        diagnostics = {'incumbent_estimate_status': 'available_independent_of_new_entry_hurdle',
            'hold_value_usd': hold_value, 'exit_value_usd': exit_value,
            'switch_value_usd': switch_value, 'incumbent_quote_id': q['quote_id'],
            'incumbent_original_side': position['side'], 'forecast_side': current['side']}
        if best and switch_value > max(hold_value + mechanics.number(config['switch_incremental_hurdle_usd']), exit_value):
            return mechanics._candidate_decision(best, 'rotate', 'switch_exceeds_hold_and_exit_in_usd'), diagnostics
        return mechanics._empty_decision('hold' if hold_value >= exit_value else 'exit', epoch,
            'hold_dominates_in_usd' if hold_value >= exit_value else 'exit_dominates_in_usd'), diagnostics
    except (ValueError, KeyError, TypeError) as exc:
        return mechanics._empty_decision('exit', epoch, 'incumbent_valuation_unavailable'), {'reason': str(exc)}
