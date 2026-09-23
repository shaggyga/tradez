from copy import deepcopy
from decimal import Context, Decimal, localcontext
import pytest
import attribute_verified_episode_costs_v1 as costs

ARM = 'usd_curve_manager'


def fixture(side=1, close=True):
    """Small exact goldens; independent of the decomposition implementation."""
    position = dict(instrument='GBP_USD', side=side, base_units=1000,
        entry_price='1.200212001' if side == 1 else '1.199987999', entry_quote_id='q0')
    net = '0.775987' if side == 1 else '0.776009'
    steps, rows = [], []
    previous = {arm: dict(position=None, realized_usd='0') for arm in costs.summary.ARMS}
    for i in range(3 if close else 2):
        ending = i == 2
        bid, ask = (('1.2010', '1.2014') if side == 1 else ('1.1988', '1.1992')) if ending else ('1.2000', '1.2002')
        quote = dict(bid=bid, ask=ask, quote_id='q'+str(i), market_epoch=1000.+i*60,
                     available_epoch=1001.+i*60)
        legs = []
        if i == 0:
            legs = [dict(kind='open', instrument='GBP_USD', side=side, base_units=1000,
                price=position['entry_price'], slippage_price='0.000012001', quote_id='q0',
                market_epoch=1000., available_epoch=1001., virtual_action_epoch=1001.,
                realized_usd='0', position=deepcopy(position))]
        elif ending:
            legs = [dict(kind='close', instrument='GBP_USD', side=side, base_units=1000,
                price='1.200987988' if side == 1 else '1.199211990',
                slippage_price='0.000012012' if side == 1 else '0.000011990', quote_id='q2',
                market_epoch=1120., available_epoch=1121., virtual_action_epoch=1121.,
                realized_usd=net, quote_currency_pnl=net, original_entry=deepcopy(position))]
        states = deepcopy(previous)
        states[ARM] = dict(position=None if ending else deepcopy(position), realized_usd=net if ending else '0')
        arms, arithmetic, decisions, statuses = {}, {}, {}, {}
        for arm in costs.summary.ARMS:
            active = arm == ARM
            action = 'enter' if active and i == 0 else 'exit' if active and ending else 'hold' if active else 'wait'
            decisions[arm] = action
            statuses[arm] = 'applied_virtual_action'
            arms[arm] = dict(decision=dict(action=action), state_before=deepcopy(previous[arm]),
                state_after=deepcopy(states[arm]), virtual_action=dict(status='applied_virtual_action',
                    legs=deepcopy(legs) if active else [], realized_delta_usd=net if active and ending else '0'),
                liquidation_mark=dict(hypothetical_liquidation=dict(legs=[{'fake_expensive_mark': True}])))
            arithmetic[arm] = dict(open_legs=int(active and i == 0), close_legs=int(active and ending),
                realized_usd=net if active and ending else '0', position_open=active and not ending,
                liquidation_usd=net if active and ending else '0')
        value = dict(status='completed', arms=arms, states=dict(state_sha256=str(i)*64),
            native_target_epoch=1121., terminal=ending, selected_observation_epoch=1001.+i*60,
            selected_quote_mapping=dict(quotes={'GBP_USD': quote}), settlement_computed_epoch=1002.+i*60,
            **costs.worker.manager.SAFETY)
        rows.append(costs.worker.manager.sealed(value, 'settlement_sha256'))
        steps.append(dict(step_id=f'step_{i:03}', state_sha256=str(i)*64,
            settlement_computed_epoch=1002.+i*60, selected_observation_epoch=1001.+i*60,
            terminal=ending, decisions=decisions, action_statuses=statuses, independent_arithmetic=arithmetic))
        previous = states
    episode = dict(episode_id='episode_01', native_target_epoch=1121., steps=steps,
        status='terminal_flat' if close else 'in_progress_or_partial', completed_matched_endpoint_eligible=close,
        per_arm={arm: dict(realized_usd=s['realized_usd'], position_open=s['position'] is not None)
                 for arm, s in previous.items()})
    return episode, rows


def reseal(row):
    row.pop('settlement_sha256', None)
    return costs.worker.manager.sealed(row, 'settlement_sha256')


@pytest.mark.parametrize('side,expected', [(1, '0.775987'), (-1, '0.776009')])
def test_exact_long_and_short_quote_cost_goldens(side, expected):
    episode, rows = fixture(side)
    before = deepcopy((episode, rows))
    result = costs.decompose_episode(episode, rows)
    arm = result['per_arm'][ARM]
    assert Decimal(arm['gross_midpoint_pnl_usd']) == Decimal('1.1')
    assert Decimal(arm['spread_cost_usd']) == Decimal('0.3')
    assert Decimal(arm['realized_usd']) == Decimal(expected)
    assert arm['completed_round_trips'] == 1 and arm['actual_virtual_legs'] == 2
    assert result['per_arm']['no_trade']['actual_virtual_legs'] == 0
    assert result['per_arm']['no_trade']['realized_usd'] == '0'
    assert result['independent_sample_size'] is None and result['broker_fills_observed'] is False
    assert (episode, rows) == before


def test_open_entry_costs_are_excluded_from_realized_trades():
    episode, rows = fixture(close=False)
    result = costs.decompose_episode(episode, rows)
    arm = result['per_arm'][ARM]
    assert arm['realized_usd'] == arm['gross_midpoint_pnl_usd'] == arm['spread_cost_usd'] == '0'
    assert arm['completed_round_trips'] == 0 and arm['actual_virtual_legs'] == 1
    assert arm['open_entry_costs_excluded_from_realized'] == {'spread_usd': '0.1000', 'slippage_usd': '0.012001000'}
    assert result['completed_matched_endpoint_eligible'] is False


def test_global_decimal_precision_does_not_change_costs():
    episode, rows = fixture()
    expected = costs.decompose_episode(episode, rows)
    with localcontext(Context(prec=4)):
        assert costs.decompose_episode(episode, rows) == expected


@pytest.mark.parametrize('kind', ['price', 'slip', 'quote_id', 'market_clock', 'available_clock',
    'action_clock', 'units', 'side_bool', 'bid_ask_inversion', 'missing_quote', 'net', 'original_entry'])
def test_changed_leg_or_quote_is_refused_even_with_recomputed_body_seal(kind):
    episode, rows = fixture()
    row = rows[-1]
    leg = row['arms'][ARM]['virtual_action']['legs'][0]
    quote = row['selected_quote_mapping']['quotes']['GBP_USD']
    if kind == 'price': leg['price'] = '1.2010'
    elif kind == 'slip': leg['slippage_price'] = '0'
    elif kind == 'quote_id': leg['quote_id'] = 'changed'
    elif kind == 'market_clock': leg['market_epoch'] += 1
    elif kind == 'available_clock': leg['available_epoch'] += 1
    elif kind == 'action_clock': leg['virtual_action_epoch'] += 1
    elif kind == 'units': leg['base_units'] = 999
    elif kind == 'side_bool': leg['side'] = True
    elif kind == 'bid_ask_inversion': quote['bid'] = '1.3'
    elif kind == 'missing_quote': row['selected_quote_mapping'] = None
    elif kind == 'net': leg['realized_usd'] = '9'
    elif kind == 'original_entry': leg['original_entry']['entry_quote_id'] = 'other'
    rows[-1] = reseal(row)
    with pytest.raises(ValueError):
        costs.decompose_episode(episode, rows)


def test_missing_step_cannot_turn_into_zero_cost():
    episode, rows = fixture()
    with pytest.raises(ValueError, match='cost_step_inventory'):
        costs.decompose_episode(episode, rows[:-1])


def test_old_seal_after_a_price_edit_is_refused():
    episode, rows = fixture()
    rows[0]['arms'][ARM]['virtual_action']['legs'][0]['price'] = '1.2'
    with pytest.raises(ValueError, match='sealed_record_hash_mismatch'):
        costs.decompose_episode(episode, rows)


def test_only_virtual_action_legs_count_not_liquidation_marks():
    episode, rows = fixture()
    for row in rows:
        for arm in row['arms'].values():
            arm['liquidation_mark'] = {'hypothetical_liquidation': {'legs': deepcopy(rows[0]['arms'][ARM]['virtual_action']['legs']) * 100}}
    rows = [reseal(row) for row in rows]
    result = costs.decompose_episode(episode, rows)
    assert result['per_arm'][ARM]['completed_round_trips'] == 1
    assert result['per_arm'][ARM]['actual_virtual_legs'] == 2


def test_failed_action_cannot_retain_charged_legs():
    episode, rows = fixture()
    rows[0]['arms'][ARM]['virtual_action']['status'] = 'no_virtual_fill'
    episode['steps'][0]['action_statuses'][ARM] = 'no_virtual_fill'
    rows[0] = reseal(rows[0])
    with pytest.raises(ValueError, match='cost_failed_action_has_legs'):
        costs.decompose_episode(episode, rows)


def test_changed_verifier_final_cash_is_refused():
    episode, rows = fixture()
    episode['per_arm'][ARM]['realized_usd'] = '1'
    with pytest.raises(ValueError, match='cost_final_cash_reconciliation'):
        costs.decompose_episode(episode, rows)
