from copy import deepcopy
from decimal import Decimal

import pytest

import oanda_curve_management_replay_v1 as m


def metadata():
    return {p: {'base_currency': p[:3], 'quote_currency': p[4:], 'pip_size': pip, 'unit_increment': 1}
            for p, pip in [('EUR_USD', '.0001'), ('GBP_JPY', '.01'), ('USD_HUF', '.01'), ('HKD_JPY', '.0001')]}


def config():
    return {'schema_version': m.CONFIG_SCHEMA, **m.SAFETY, 'account_currency': 'USD',
            'maximum_open_positions': 1, 'metadata': metadata(), 'notional_usd': '1000',
            'conversion_policy': 'direct_or_inverse_executable_bid_ask_no_triangulation',
            'sizing_policy': 'fixed_usd_notional_integer_base_units_at_decision',
            'curve_target_window_policy': 'exact_only',
            'maximum_entry_spread_bps': '10', 'maximum_holding_sec': 3600,
            'cadence_sec': 300, 'execution_delay_sec': 60, 'quote_max_age_sec': 60,
            'minimum_entry_cost_ratio': '1', 'slippage_bps_per_leg': '1',
            'switch_incremental_hurdle_usd': '0', 'feedback_horizon_sec': 300,
            'decision_epochs': [1000, 1300, 1600],
            'legacy_policy': {'minimum_score_cost_ratio': .75, 'rotation_improvement_multiple': 1.25,
                              'maximum_holding_min': 60}}


def q(pair='EUR_USD', bid='1.0000', ask='1.0002', epoch=1000):
    return {'instrument': pair, 'quote_id': f'q.{pair}.{epoch}.{bid}', 'bid': bid, 'ask': ask,
            'market_epoch': epoch, 'available_epoch': epoch, 'tradeable': True}


def quotes(epoch=1000):
    return {'EUR_USD': q(epoch=epoch), 'GBP_JPY': q('GBP_JPY', '150.00', '150.02', epoch),
            'GBP_USD': q('GBP_USD', '1.2500', '1.2502', epoch),
            'USD_JPY': q('USD_JPY', '150.00', '150.02', epoch),
            'USD_HUF': q('USD_HUF', '350.00', '350.02', epoch),
            'HKD_JPY': q('HKD_JPY', '19.2', '19.202', epoch),
            'USD_HKD': q('USD_HKD', '7.79', '7.80', epoch)}


def curve(pair='EUR_USD', epoch=1000, change='.01', target=1660):
    quote = quotes(epoch)[pair];mid = (Decimal(quote['bid']) + Decimal(quote['ask'])) / 2
    pip = Decimal(metadata()[pair]['pip_size']);delta = Decimal(change)
    return {'instrument': pair, 'side': 1 if delta > 0 else -1, 'curve_id': 'synthetic-curve',
            'curve_sha256': 'a' * 64, 'node_id': f'node.{pair}.{epoch}', 'model_id': 'synthetic-model',
            'forecast_cohort': 'synthetic-only', 'reference_epoch': epoch - 10,
            'reference_price': str(mid), 'pip_size': str(pip), 'issued_epoch': epoch - 5,
            'available_epoch': epoch - 1, 'decision_epoch': epoch, 'original_target_epoch': target,
            'horizon_sec': target - (epoch - 10), 'remaining_sec': target - epoch,
            'target_window_policy': 'exact_only', 'target_is_exact': True,
            'target_selection_policy': {'kind': 'exact_price_epoch', 'maximum_delay_sec': 0},
            'target_price_window_end_epoch': target,
            'expected_terminal_price': str(mid + delta), 'expected_remaining_price_change': str(delta),
            'expected_remaining_move_pips': str(delta / pip), 'source_bindings': {'synthetic.py': 'b' * 64},
            'probability_up': .6, 'probability_scope': 'original_interval_uncalibrated'}


def momentum(pair='EUR_USD', epoch=1000, magnitude='100', side=1):
    return {'instrument': pair, 'side': side, 'expected_move_pips': magnitude, 'score': '10',
            'confidence': '.6', 'snapshot_id': f'momentum.{pair}.{epoch}', 'available_epoch': epoch - 1,
            'decision_epoch': epoch, 'original_target_epoch': 1660}


def frames():
    return [{'decision_epoch': epoch, 'execution_epoch': epoch + 60, 'terminal': epoch == 1600,
             'management_target_epoch': 1660, 'decision_quotes': quotes(epoch),
             'execution_quotes': quotes(epoch + 60), 'curve_candidates': [curve(epoch=epoch)],
             'momentum_candidates': [momentum(epoch=epoch)]} for epoch in (1000, 1300, 1600)]


def test_all_five_arms_and_exact_denominators():
    out = m.replay(frames(), config())
    assert out['status'] == 'mechanics_complete'
    assert set(out['summaries']) == set(m.ARMS)
    assert len(out['rows']) == 15 and out['global_decision_clocks'] == 3
    assert out['independent_sample_size'] is None
    assert all(v['terminal_flat'] for v in out['summaries'].values())
    assert out['summaries']['no_trade']['realized_usd'] == '0'
    assert all(row['counts_as_additional_market_repetition'] is False for row in out['counterfactuals'])


def test_replay_deterministic_and_does_not_mutate_inputs():
    fs, cfg = frames(), config();before = deepcopy((fs, cfg))
    a = m.replay(fs, cfg);b = m.replay(fs, cfg)
    assert a == b and (fs, cfg) == before


@pytest.mark.parametrize('flag', list(m.SAFETY))
def test_inert_flags_required(flag):
    cfg = config();cfg[flag] = not cfg[flag]
    with pytest.raises(ValueError, match='inert'):
        m.replay(frames(), cfg)


@pytest.mark.parametrize('key,value', [('notional_usd', '0'), ('slippage_bps_per_leg', '-1'),
    ('quote_max_age_sec', 61), ('execution_delay_sec', 300), ('maximum_open_positions', True),
    ('conversion_policy', 'triangulate'), ('sizing_policy', 'future_execution_price')])
def test_bad_contract_rejected(key, value):
    cfg = config();cfg[key] = value
    with pytest.raises(ValueError):
        m.replay(frames(), cfg)


@pytest.mark.parametrize('value', [True, None, [], {}, 'NaN', 'Infinity', float('nan')])
def test_nonfinite_financial_values_fail(value):
    with pytest.raises(ValueError):
        m.number(value)


def test_metadata_pips_are_explicit_for_huf_and_hkd_jpy():
    md = m.validate_metadata(metadata())
    assert md['USD_HUF']['pip_size'] == Decimal('.01')
    assert md['HKD_JPY']['pip_size'] == Decimal('.0001')


@pytest.mark.parametrize('bad', ['eur_usd', 'USD_USD', 'USD_USD_extra', 'USD_US'])
def test_instrument_validation(bad):
    with pytest.raises(ValueError):
        m.pair_name(bad)


def test_direct_and_inverse_profit_loss_conversion():
    direct = {'CHF_USD': q('CHF_USD', '1.1', '1.2')}
    assert m.convert_pnl_to_usd('10', 'CHF', direct, 1000, 60)[0] == Decimal('11')
    assert m.convert_pnl_to_usd('-10', 'CHF', direct, 1000, 60)[0] == Decimal('-12')
    inverse = {'USD_JPY': q('USD_JPY', '100', '125')}
    assert m.convert_pnl_to_usd('1000', 'JPY', inverse, 1000, 60)[0] == Decimal('8')
    assert m.convert_pnl_to_usd('-1000', 'JPY', inverse, 1000, 60)[0] == Decimal('-10')


def test_conversion_never_uses_triangulation_or_ignores_bad_direct():
    with pytest.raises(ValueError, match='missing_usd_conversion'):
        m.usd_rates('CHF', {'CHF_JPY': q('CHF_JPY'), 'USD_JPY': q('USD_JPY')}, 1000, 60)
    qs = {'CHF_USD': q('CHF_USD', epoch=1001), 'USD_CHF': q('USD_CHF')}
    with pytest.raises(ValueError, match='future'):
        m.usd_rates('CHF', qs, 1000, 60)


@pytest.mark.parametrize('field,value', [('market_epoch', 1001), ('available_epoch', 1001),
    ('market_epoch', 939), ('bid', 1.0), ('bid', '-1'), ('ask', '.9'), ('instrument', 'GBP_USD')])
def test_invalid_quote_rejected(field, value):
    raw = q();raw[field] = value
    with pytest.raises(ValueError):
        m.quote_at({'EUR_USD': raw}, 'EUR_USD', 1000, 60)


def test_integer_units_freeze_at_decision_and_not_future_fill_price():
    cfg = m.validate_config(config());qs = quotes()
    units, receipt = m.size_at_decision('EUR_USD', qs, 1000, cfg)
    assert units == 999 and receipt['decision_value_usd'] <= Decimal('1000')
    baseline = m.replay(frames(), config())
    changed = frames();changed[0]['execution_quotes']['EUR_USD'] = q(bid='2', ask='2.0002', epoch=1060)
    revised = m.replay(changed, config())
    left = next(r for r in baseline['rows'] if r['arm'] == 'usd_curve_manager')
    right = next(r for r in revised['rows'] if r['arm'] == 'usd_curve_manager')
    assert left['decision'] == right['decision'] and left['decision']['base_units'] == 999


def test_foreign_quote_pnl_and_slippage_have_real_usd_units():
    cfg = m.validate_config(config());cfg['slippage_bps_per_leg'] = Decimal(0)
    cs, _ = m.prepare_candidates([curve('GBP_JPY', change='1')], 'curve', quotes(), 1000, 1660, cfg)
    d = m._candidate_decision(cs[0], 'enter', 'fixture')
    state, opened = m.apply_action(m.flat_state(), d, quotes(1060), 1060, cfg)
    end = quotes(1360);end['GBP_JPY'] = q('GBP_JPY', '151.02', '151.04', 1360)
    end['USD_JPY'] = q('USD_JPY', '150', '160', 1360)
    final, closed = m.apply_action(state, {'action': 'exit', 'decision_epoch': 1300}, end, 1360, cfg)
    assert opened['status'] == closed['status'] == 'applied'
    assert d['base_units'] == 799
    assert closed['legs'][0]['quote_currency_pnl'] == Decimal('799')
    assert final['realized_usd'] == Decimal('799') / Decimal('160')


def test_failed_rotation_preserves_old_position_and_cash_atomically():
    cfg = m.validate_config(config())
    cs, _ = m.prepare_candidates([curve()], 'curve', quotes(), 1000, 1660, cfg)
    state, _ = m.apply_action(m.flat_state(), m._candidate_decision(cs[0], 'enter', 'fixture'), quotes(1060), 1060, cfg)
    other, _ = m.prepare_candidates([curve('GBP_JPY', 1300, '1')], 'curve', quotes(1300), 1300, 1660, cfg)
    missing = quotes(1360);missing.pop('USD_JPY')
    final, receipt = m.apply_action(state, m._candidate_decision(other[0], 'rotate', 'fixture'), missing, 1360, cfg)
    assert final == state and receipt['status'] == 'rejected' and receipt['legs'] == []


def test_rotation_records_close_then_open_with_two_costs():
    cfg = m.validate_config(config())
    cs, _ = m.prepare_candidates([curve()], 'curve', quotes(), 1000, 1660, cfg)
    state, _ = m.apply_action(m.flat_state(), m._candidate_decision(cs[0], 'enter', 'fixture'), quotes(1060), 1060, cfg)
    other, _ = m.prepare_candidates([curve('GBP_JPY', 1300, '1')], 'curve', quotes(1300), 1300, 1660, cfg)
    after, receipt = m.apply_action(state, m._candidate_decision(other[0], 'rotate', 'fixture'), quotes(1360), 1360, cfg)
    assert [r['kind'] for r in receipt['legs']] == ['close', 'open']
    assert all(r['slippage_price'] > 0 for r in receipt['legs'])
    assert after['position']['instrument'] == 'GBP_JPY'


@pytest.mark.parametrize('key,value,reason', [
    ('available_epoch', 1001, 'available'), ('original_target_epoch', 1661, 'incomparable'),
    ('pip_size', '.01', 'pip_metadata'), ('remaining_sec', 1, 'remaining_clock'),
    ('expected_remaining_move_pips', '1', 'price_basis'), ('side', -1, 'side_or_terminal'),
    ('issued_epoch', 999.5, 'clock_order'), ('curve_sha256', 'broken', 'sha256'),
    ('probability_up', '1.1', 'probability')])
def test_invalid_curve_kept_as_explicit_refusal(key, value, reason):
    raw = curve();raw[key] = value
    accepted, rejected = m.prepare_candidates([raw], 'curve', quotes(), 1000, 1660, m.validate_config(config()))
    assert not accepted and reason in rejected[0]['reason'] and rejected[0]['source_payload'] == raw


def test_duplicate_candidates_reject_all_members_and_bad_objects_do_not_crash():
    accepted, rejected = m.prepare_candidates([curve(), curve(), None], 'curve', quotes(), 1000, 1660, m.validate_config(config()))
    assert not accepted and len(rejected) == 3


def test_incumbent_below_entry_hurdle_still_has_hold_value():
    cfg = m.validate_config(config())
    cs, _ = m.prepare_candidates([curve()], 'curve', quotes(), 1000, 1660, cfg)
    state, _ = m.apply_action(m.flat_state(), m._candidate_decision(cs[0], 'enter', 'fixture'), quotes(1060), 1060, cfg)
    weak, _ = m.prepare_candidates([curve(epoch=1300, change='.000001')], 'curve', quotes(1300), 1300, 1660, cfg)
    assert weak[0]['score'] < cfg['minimum_entry_cost_ratio']
    decision, diagnostics = m.choose_usd_action(state, weak, quotes(1300), 1300, cfg)
    assert decision['action'] == 'hold'
    assert diagnostics['incumbent_estimate_status'] == 'available_independent_of_new_entry_hurdle'


def test_reversed_forecast_projects_onto_actual_incumbent_side():
    cfg = m.validate_config(config());cfg['minimum_entry_cost_ratio'] = Decimal('1000')
    cs, _ = m.prepare_candidates([curve()], 'curve', quotes(), 1000, 1660, cfg)
    state, _ = m.apply_action(m.flat_state(), m._candidate_decision(cs[0], 'enter', 'fixture'), quotes(1060), 1060, cfg)
    reversed_rows, _ = m.prepare_candidates([curve(epoch=1300, change='-.01')], 'curve', quotes(1300), 1300, 1660, cfg)
    decision, diag = m.choose_usd_action(state, reversed_rows, quotes(1300), 1300, cfg)
    assert decision['action'] == 'exit' and diag['hold_value_usd'] < diag['exit_value_usd']
    assert diag['forecast_side'] == -1 and diag['incumbent_original_side'] == 1


def test_missing_terminal_quote_never_manufactures_flatness_or_profit():
    fs = frames();fs[-1]['execution_quotes'] = {}
    out = m.replay(fs, config())
    assert out['status'] == 'unresolved_terminal_position'
    assert out['summaries']['usd_curve_manager']['terminal_flat'] is False
    assert out['matched_comparisons']['usd_momentum_manager']['terminal_realized_delta_usd'] is None
    assert out['global_decision_clocks'] == 3


@pytest.mark.parametrize('mutation', ['drop_frame', 'latency', 'target', 'terminal', 'schedule'])
def test_schedule_and_native_target_boundaries(mutation):
    fs, cfg = frames(), config()
    if mutation == 'drop_frame':fs.pop(1)
    elif mutation == 'latency':fs[0]['execution_epoch'] += 1
    elif mutation == 'target':fs[0]['management_target_epoch'] = 2000
    elif mutation == 'terminal':fs[0]['terminal'] = True
    else:cfg['decision_epochs'][1] += 1
    with pytest.raises(ValueError):m.replay(fs, cfg)


def test_future_feedback_changes_neither_decision_nor_size():
    fs = frames();fs[1]['curve_candidates'] = [curve(epoch=1300, change='-.01')]
    fs[1]['feedback_epoch'] = 1560;fs[1]['feedback_quotes'] = quotes(1560)
    before = m.replay(fs, config())
    fs[1]['feedback_quotes']['EUR_USD'] = q(bid='1.5', ask='1.5002', epoch=1560)
    after = m.replay(fs, config())
    assert [r['decision'] for r in before['rows']] == [r['decision'] for r in after['rows']]
    assert before['counterfactuals'] != after['counterfactuals']


def test_original_probability_is_retained_but_never_becomes_new_confidence():
    cs, rejected = m.prepare_candidates([curve()], 'curve', quotes(), 1000, 1660, m.validate_config(config()))
    assert not rejected and cs[0]['confidence'] is None
    assert cs[0]['source_payload']['probability_up'] == .6


def test_same_input_and_usd_selector_produce_identical_matched_trajectories():
    out = m.replay(frames(), config())
    assert Decimal(out['matched_comparisons']['usd_momentum_manager']['terminal_realized_delta_usd']) == 0
    a = [r['decision']['action'] for r in out['rows'] if r['arm'] == 'usd_momentum_manager']
    b = [r['decision']['action'] for r in out['rows'] if r['arm'] == 'usd_curve_manager']
    assert a == b


def test_currency_normalized_value_ranks_economics_not_native_pip_size():
    cfg = m.validate_config(config())
    rows, errors = m.prepare_candidates([curve('GBP_JPY', change='1'), curve(change='.01')],
                                        'curve', quotes(), 1000, 1660, cfg)
    assert not errors
    assert rows[0]['instrument'] == 'EUR_USD'
    assert rows[0]['source_payload']['expected_remaining_move_pips'] == rows[1]['source_payload']['expected_remaining_move_pips']
    assert rows[0]['new_entry_net_usd'] > rows[1]['new_entry_net_usd']


def test_curve_hold_arm_preserves_incumbent_while_curve_manager_switches():
    fs = frames()
    fs[1]['curve_candidates'] = [curve(epoch=1300, change='-.01')]
    out = m.replay(fs, config())
    rows = {(r['clock_index'], r['arm']): r for r in out['rows']}
    assert rows[(1, 'curve_hold_no_rotation')]['decision']['action'] == 'hold'
    assert rows[(1, 'usd_curve_manager')]['decision']['action'] == 'rotate'
    assert out['summaries']['curve_hold_no_rotation']['completed_position_closures'] == 1
    assert out['summaries']['usd_curve_manager']['completed_position_closures'] == 2


def test_local_counterfactual_has_matched_state_feedback_and_zero_weight():
    fs = frames();fs[0]['feedback_epoch'] = 1360;fs[0]['feedback_quotes'] = quotes(1360)
    out = m.replay(fs, config())
    branch = next(b for b in out['counterfactuals'] if b['clock_index'] == 0)
    parent = next(r for r in out['rows'] if r['row_sha256'] == branch['parent_row_sha256'])
    assert branch['state_before'] == parent['state_before']
    assert branch['feedback_epoch'] == parent['feedback_epoch']
    assert branch['primary_feedback'] == parent['primary_feedback']
    assert branch['counts_as_additional_market_repetition'] is False
    assert Decimal(branch['primary_minus_branch_equity_usd']) < 0


def test_curve_and_momentum_inputs_do_not_claim_same_policy_as_legacy_reference():
    out = m.replay(frames(), config())
    assert out['matched_comparisons']['usd_momentum_manager']['selector_input_attribution'] == 'same_usd_selector_different_input'
    assert out['matched_comparisons']['legacy_momentum_reference']['selector_input_attribution'] == 'different_policy_control'


@pytest.mark.parametrize('action', ['enter', 'exit', 'hold', 'wait', 'rotate'])
def test_all_public_actions_reject_future_or_missing_decision_clock(action):
    cfg = m.validate_config(config())
    before = m.flat_state()
    after, receipt = m.apply_action(before, {'action': action, 'decision_epoch': 1100}, quotes(1060), 1060, cfg)
    assert after == before and receipt['status'] == 'rejected'
    after, receipt = m.apply_action(before, {'action': action}, quotes(1060), 1060, cfg)
    assert after == before and receipt['status'] == 'rejected'


def test_traded_quote_requires_exact_execution_time_even_if_fresh():
    cfg = m.validate_config(config())
    cs, _ = m.prepare_candidates([curve()], 'curve', quotes(), 1000, 1660, cfg)
    wrong = quotes(1060);wrong['EUR_USD'] = q(epoch=1059)
    after, receipt = m.apply_action(m.flat_state(), m._candidate_decision(cs[0], 'enter', 'fixture'), wrong, 1060, cfg)
    assert after['position'] is None and 'exact_execution' in receipt['reason']


def test_later_observed_settlement_does_not_invent_earlier_arrival():
    fs = frames()
    fs[-1]['execution_observed_epoch'] = 5000
    for raw in fs[-1]['execution_quotes'].values():raw['available_epoch'] = 5000
    result = m.replay(fs, config())
    assert result['summaries']['usd_curve_manager']['terminal_flat']
    final = next(r for r in result['rows'] if r['arm'] == 'usd_curve_manager' and r['terminal'])
    assert final['decision']['decision_epoch'] == 1600
    assert final['execution']['legs'][0]['market_epoch'] == 1660
    assert final['execution']['legs'][0]['available_epoch'] == 5000
    assert final['execution']['legs'][0]['observed_epoch'] == 5000


@pytest.mark.parametrize('index', [0, 1])
def test_fill_state_observed_after_next_decision_cannot_enter_that_decision(index):
    fs = frames()
    fs[index]['execution_observed_epoch'] = fs[index + 1]['decision_epoch'] + 0.001
    with pytest.raises(ValueError, match='execution_state_not_observed_before_next_decision'):
        m.replay(fs, config())


def test_fill_observed_exactly_at_next_decision_is_available_to_its_state():
    fs = frames()
    fs[0]['execution_observed_epoch'] = fs[1]['decision_epoch']
    for raw in fs[0]['execution_quotes'].values():raw['available_epoch'] = fs[1]['decision_epoch']
    result = m.replay(fs, config())
    following = next(r for r in result['rows'] if r['arm'] == 'usd_curve_manager' and r['clock_index'] == 1)
    assert following['state_before']['position']['entry_epoch'] == 1060


def test_future_conversion_quote_not_admitted_by_later_observation():
    qs = {'USD_JPY': q('USD_JPY', '150', '151', 1061)}
    qs['USD_JPY']['available_epoch'] = 5000
    with pytest.raises(ValueError, match='future'):
        m.usd_rates('JPY', qs, 1060, 60, observed_epoch=5000)


def test_incumbent_cannot_borrow_return_after_its_original_deadline():
    cfg = m.validate_config(config())
    cs, _ = m.prepare_candidates([curve()], 'curve', quotes(), 1000, 1660, cfg)
    state, _ = m.apply_action(m.flat_state(), m._candidate_decision(cs[0], 'enter', 'fixture'), quotes(1060), 1060, cfg)
    state['position']['original_target_epoch'] = 1500
    later, _ = m.prepare_candidates([curve(epoch=1300)], 'curve', quotes(1300), 1300, 1660, cfg)
    decision, diag = m.choose_usd_action(state, later, quotes(1300), 1300, cfg)
    assert decision['action'] == 'exit' and diag['incumbent_estimate_status'] == 'incomparable_deadline'


def test_native_target_beyond_maximum_holding_refuses_new_entry():
    cfg = m.validate_config(config());cfg['maximum_holding_sec'] = Decimal(60)
    rows, errors = m.prepare_candidates([curve()], 'curve', quotes(), 1000, 1660, cfg)
    assert not rows and 'maximum_holding_deadline' in errors[0]['reason']


def test_native_target_between_fills_is_explicitly_unsupported():
    fs = frames();fs[0]['management_target_epoch'] = 1500
    with pytest.raises(ValueError, match='native_target_off_exact_execution_grid'):
        m.replay(fs, config())


@pytest.mark.parametrize('target', [1059, 1060, 4661])
def test_direct_entry_cannot_bypass_original_target_window(target):
    cfg = m.validate_config(config())
    cs, _ = m.prepare_candidates([curve()], 'curve', quotes(), 1000, 1660, cfg)
    decision = m._candidate_decision(cs[0], 'enter', 'fixture');decision['original_target_epoch'] = target
    after, receipt = m.apply_action(m.flat_state(), decision, quotes(1060), 1060, cfg)
    assert after == m.flat_state() and receipt['reason'] == 'entry_native_target_elapsed_or_beyond_holding_limit'


def test_common_deadline_prevents_legacy_and_hold_arm_using_later_target_prices():
    fs = frames();fs[0]['management_target_epoch'] = 1360
    fs[0]['curve_candidates'] = [curve(target=1360)]
    fs[0]['momentum_candidates'][0]['original_target_epoch'] = 1360
    result = m.replay(fs, config())
    for arm in m.ARMS:
        middle = next(r for r in result['rows'] if r['arm'] == arm and r['clock_index'] == 1)
        assert middle['decision']['action'] == ('wait' if arm == 'no_trade' else 'exit')
        assert middle['state_after']['position'] is None


def test_public_hold_cannot_bypass_common_deadline():
    cfg = m.validate_config(config())
    cs, _ = m.prepare_candidates([curve(target=1360)], 'curve', quotes(), 1000, 1360, cfg)
    state, _ = m.apply_action(m.flat_state(), m._candidate_decision(cs[0], 'enter', 'fixture'), quotes(1060), 1060, cfg)
    after, receipt = m.apply_action(state, m._empty_decision('hold', 1300, 'fixture'), quotes(1360), 1360, cfg)
    assert after == state and receipt['reason'] == 'hold_reaches_common_native_deadline'


def test_neutral_curve_is_valid_incumbent_zero_value_but_cannot_open():
    cfg = m.validate_config(config())
    raw = curve(change='0');raw['side'] = 0
    neutral, rejected = m.prepare_candidates([raw], 'curve', quotes(), 1000, 1660, cfg)
    assert not rejected and neutral[0]['score'] == 0
    decision, _ = m.choose_usd_action(m.flat_state(), neutral, quotes(), 1000, cfg)
    assert decision['action'] == 'wait'
    cs, _ = m.prepare_candidates([curve()], 'curve', quotes(), 1000, 1660, cfg)
    state, _ = m.apply_action(m.flat_state(), m._candidate_decision(cs[0], 'enter', 'fixture'), quotes(1060), 1060, cfg)
    raw = curve(epoch=1300, change='0');raw['side'] = 0
    neutral, rejected = m.prepare_candidates([raw], 'curve', quotes(1300), 1300, 1660, cfg)
    decision, diagnostics = m.choose_usd_action(state, neutral, quotes(1300), 1300, cfg)
    assert not rejected and decision['action'] == 'hold'
    assert diagnostics['hold_value_usd'] == diagnostics['exit_value_usd']


@pytest.mark.parametrize('side,change', [(0, '.01'), (1, '0'), (-1, '0')])
def test_neutral_requires_exact_zero_signed_estimate(side, change):
    raw = curve(change=change);raw['side'] = side
    accepted, rejected = m.prepare_candidates([raw], 'curve', quotes(), 1000, 1660, m.validate_config(config()))
    assert not accepted and 'curve_side_or_terminal_mismatch' in rejected[0]['reason']


@pytest.mark.parametrize('tradeable', [False, None, 1, 'true'])
def test_quote_requires_exact_tradeability_including_fill_and_conversion(tradeable):
    raw = q();raw['tradeable'] = tradeable
    with pytest.raises(ValueError, match='explicitly_tradeable'):
        m.quote_at({'EUR_USD': raw}, 'EUR_USD', 1000, 60)


@pytest.mark.parametrize('field,value', [('quote_id', 'another'), ('market_epoch', 999), ('bid', '.9999'), ('available_epoch', 999)])
def test_adapter_decision_quote_evidence_cannot_be_swapped(field, value):
    raw = curve();raw['decision_quote'] = q();raw['decision_quote'][field] = value
    raw['decision_quote_sha256'] = m.digest(raw['decision_quote'])
    accepted, rejected = m.prepare_candidates([raw], 'curve', quotes(), 1000, 1660, m.validate_config(config()))
    assert not accepted and 'curve_decision_quote_binding_mismatch' in rejected[0]['reason']


def test_adapter_decision_quote_hash_is_checked_and_valid_binding_admitted():
    raw = curve();raw['decision_quote'] = q();raw['decision_quote_sha256'] = m.digest(raw['decision_quote'])
    accepted, rejected = m.prepare_candidates([raw], 'curve', quotes(), 1000, 1660, m.validate_config(config()))
    assert accepted and not rejected
    raw['decision_quote_sha256'] = 'f' * 64
    accepted, rejected = m.prepare_candidates([raw], 'curve', quotes(), 1000, 1660, m.validate_config(config()))
    assert not accepted and 'curve_decision_quote_hash_mismatch' in rejected[0]['reason']


def test_nonexact_training_target_cannot_enter_exact_replay_by_silent_nominal_relabel():
    raw = curve();raw['target_selection_policy'] = {'kind': 'first_complete_bar_at_or_after_nominal', 'maximum_delay_sec': 7}
    raw['target_is_exact'] = False;raw['target_price_window_end_epoch'] += 7
    accepted, rejected = m.prepare_candidates([raw], 'curve', quotes(), 1000, 1660, m.validate_config(config()))
    assert not accepted and 'nonexact_model_target_requires_explicit_policy' in rejected[0]['reason']


def test_nominal_target_management_is_separately_predeclared_and_retains_training_window():
    cfg = config();cfg['curve_target_window_policy'] = 'nominal_management_boundary'
    fs = frames()
    for frame in fs:
        raw = frame['curve_candidates'][0]
        raw['target_window_policy'] = cfg['curve_target_window_policy']
        raw['target_selection_policy'] = {'kind': 'first_complete_bar_at_or_after_nominal', 'maximum_delay_sec': 7}
        raw['target_is_exact'] = False;raw['target_price_window_end_epoch'] += 7
    out = m.replay(fs, cfg)
    assert out['curve_target_window_policy'] == 'nominal_management_boundary'
    assert out['summaries']['usd_curve_manager']['terminal_flat']
    first = next(r for r in out['rows'] if r['arm'] == 'usd_curve_manager')
    assert first['decision']['original_target_epoch'] == 1660
    retained = first['accepted_candidate_evidence'][0]['original_evidence']
    assert retained['target_is_exact'] is False and retained['target_price_window_end_epoch'] == 1667
    assert retained['target_selection_policy']['maximum_delay_sec'] == 7


@pytest.mark.parametrize('key,value', [('target_window_policy', 'nominal_management_boundary'),
    ('target_is_exact', False), ('target_price_window_end_epoch', 1661),
    ('target_selection_policy', {'kind': 'exact_price_epoch', 'maximum_delay_sec': 7})])
def test_target_window_identity_and_clocks_cannot_disagree(key, value):
    raw = curve();raw[key] = value
    accepted, rejected = m.prepare_candidates([raw], 'curve', quotes(), 1000, 1660, m.validate_config(config()))
    assert not accepted and rejected
