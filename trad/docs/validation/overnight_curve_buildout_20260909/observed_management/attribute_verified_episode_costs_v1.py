"""Exact quote-based cost decomposition of already verified paper episodes.

Only realized virtual trades are decomposed. Hypothetical liquidation marks
are excluded. Every input settlement must match an accepted verifier's retained
file digest. No runtime write, broker request, management or fit is performed.
"""
from copy import deepcopy
from decimal import Context, Decimal, localcontext
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))
import summarize_verified_episodes_v1 as summary
sys.path.insert(0, str(summary.TRAD))
import oanda_observed_curve_management_worker_v1 as worker

SCHEMA = 'observed_episode_quote_cost_attribution_v1_20260909'
SUMMARY_SHA = 'da845b43606d138977a74b2ba3260068405e0a55f77f416f3e8aed7e342cda9c'
MAX_BYTES = 16 * 1024 * 1024
MAX_TOTAL_BYTES = 32 * 1024 * 1024


def need(condition, reason):
    if not condition:
        raise ValueError(reason)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def number(value):
    result = summary.cash(value)
    return result


def decompose_episode(episode, settlements):
    """Separate arithmetic using the exact original selected bid and ask."""
    need(len(settlements) == len(episode['steps']) <= 64, 'cost_step_inventory')
    with localcontext(Context(prec=96)):
        states = {arm: dict(position=None, gross=Decimal(0), spread=Decimal(0),
            slippage=Decimal(0), net=Decimal(0), trades=[], actual_legs=0) for arm in summary.ARMS}
        prior_clock = 0
        for step, settlement in zip(episode['steps'], settlements):
            worker.manager.check_seal(settlement, 'settlement_sha256')
            worker.manager.check_flags(settlement)
            need(settlement['status'] == 'completed' and set(settlement['arms']) == set(summary.ARMS),
                 'cost_completed_five_arm_settlement')
            need(settlement['states']['state_sha256'] == step['state_sha256'] and
                 settlement['settlement_computed_epoch'] == step['settlement_computed_epoch'] and
                 settlement['selected_observation_epoch'] == step['selected_observation_epoch'] and
                 settlement['native_target_epoch'] == episode['native_target_epoch'] and
                 settlement['terminal'] is step['terminal'], 'cost_original_step_identity')
            need(prior_clock <= settlement['settlement_computed_epoch'], 'cost_step_clock_order')
            prior_clock = settlement['settlement_computed_epoch']
            mapping = settlement['selected_quote_mapping']
            q = None if mapping is None else mapping['quotes']['GBP_USD']
            for arm, state in states.items():
                retained = settlement['arms'][arm]
                action = retained['virtual_action']
                need(retained['decision']['action'] == step['decisions'][arm] and
                     action['status'] == step['action_statuses'][arm], 'cost_step_action_identity')
                before_cash = state['net']
                before = retained['state_before']
                pos = state['position']
                need(number(before['realized_usd']) == before_cash and
                     before['position'] == (None if pos is None else pos['position']), 'cost_original_state_chain')
                legs = action['legs']
                need(type(legs) is list and len(legs) <= 2, 'cost_leg_bound')
                if action['status'] == 'no_virtual_fill':
                    need(not legs, 'cost_failed_action_has_legs')
                else:
                    need(action['status'] == 'applied_virtual_action', 'cost_action_status')
                opened = closed = 0
                for leg in legs:
                    need(q is not None and leg['instrument'] == 'GBP_USD' and
                         leg['kind'] in ('open', 'close'), 'cost_leg_instrument_or_quote')
                    bid, ask = number(q['bid']), number(q['ask'])
                    need(0 < bid <= ask, 'cost_positive_ordered_quote')
                    mid, half = (bid + ask) / 2, (ask - bid) / 2
                    slip = mid * Decimal('0.1') / 10000
                    side, units = leg['side'], leg['base_units']
                    need(type(side) is int and side in (-1, 1) and type(units) is int and
                         0 < units < 10000000, 'cost_side_or_units')
                    opening = leg['kind'] == 'open'
                    execution = mid + (side if opening else -side) * (half + slip)
                    need(number(leg['price']) == execution and number(leg['slippage_price']) == slip,
                         'cost_original_leg_price')
                    need(leg['quote_id'] == q['quote_id'] and leg['market_epoch'] == q['market_epoch'] and
                         leg['available_epoch'] == q['available_epoch'] and
                         leg['virtual_action_epoch'] == settlement['selected_observation_epoch'],
                         'cost_original_quote_identity')
                    evidence = dict(step_id=step['step_id'], quote_id=q['quote_id'], bid=str(bid), ask=str(ask),
                        midpoint=str(mid), execution_price=str(execution), half_spread_price=str(half),
                        slippage_price=str(slip), market_epoch=q['market_epoch'],
                        available_epoch=q['available_epoch'], action_epoch=leg['virtual_action_epoch'])
                    state['actual_legs'] += 1
                    if opening:
                        need(state['position'] is None and number(leg['realized_usd']) == 0, 'cost_open_while_positioned')
                        position = leg['position']
                        need(position['side'] == side and position['base_units'] == units and
                             number(position['entry_price']) == execution and position['entry_quote_id'] == q['quote_id'],
                             'cost_entry_position_binding')
                        state['position'] = dict(position=deepcopy(position), evidence=evidence,
                            midpoint=mid, spread=units * half, slippage=units * slip)
                        opened += 1
                    else:
                        pos = state['position']
                        need(pos is not None and leg['original_entry'] == pos['position'] and
                             pos['position']['side'] == side and pos['position']['base_units'] == units,
                             'cost_close_original_entry')
                        gross = side * units * (mid - pos['midpoint'])
                        spread = pos['spread'] + units * half
                        slippage = pos['slippage'] + units * slip
                        net = gross - spread - slippage
                        need(number(leg['realized_usd']) == net and number(leg['quote_currency_pnl']) == net,
                             'cost_exact_net_reconciliation')
                        state['trades'].append(dict(side=side, base_units=units,
                            entry=pos['evidence'], exit=evidence, gross_midpoint_pnl_usd=str(gross),
                            spread_cost_usd=str(spread), slippage_cost_usd=str(slippage), realized_usd=str(net)))
                        for key, value in (('gross', gross), ('spread', spread), ('slippage', slippage), ('net', net)):
                            state[key] += value
                        state['position'] = None
                        closed += 1
                after = retained['state_after']
                need(number(after['realized_usd']) == state['net'] and
                     after['position'] == (None if state['position'] is None else state['position']['position']) and
                     number(action['realized_delta_usd']) == state['net'] - before_cash, 'cost_retained_state_reconciliation')
                arithmetic = step['independent_arithmetic'][arm]
                need(arithmetic['open_legs'] == opened and arithmetic['close_legs'] == closed and
                     number(arithmetic['realized_usd']) == state['net'], 'cost_verifier_arithmetic_reconciliation')
        per_arm = {}
        for arm, state in states.items():
            need(number(episode['per_arm'][arm]['realized_usd']) == state['net'] and
                 episode['per_arm'][arm]['position_open'] is (state['position'] is not None), 'cost_final_cash_reconciliation')
            need(state['gross'] - state['spread'] - state['slippage'] == state['net'], 'cost_total_identity')
            per_arm[arm] = dict(gross_midpoint_pnl_usd=str(state['gross']), spread_cost_usd=str(state['spread']),
                slippage_cost_usd=str(state['slippage']), realized_usd=str(state['net']),
                completed_round_trips=len(state['trades']), actual_virtual_legs=state['actual_legs'],
                position_open=state['position'] is not None, completed_trades=state['trades'],
                open_entry_costs_excluded_from_realized=(None if state['position'] is None else
                    dict(spread_usd=str(state['position']['spread']), slippage_usd=str(state['position']['slippage']))))
        return dict(episode_id=episode['episode_id'], status=episode['status'],
            original_target_epoch=episode['native_target_epoch'], per_arm=per_arm,
            completed_matched_endpoint_eligible=episode['completed_matched_endpoint_eligible'],
            independent_sample_size=None, broker_fills_observed=False)


def evaluate(report_path, expected_sha, *, clock=time.time):
    started = summary.epoch(clock())
    source_raw = Path(__file__).read_bytes()
    need(sha((BASE / 'summarize_verified_episodes_v1.py').read_bytes()) == SUMMARY_SHA,
         'cost_summary_source_changed')
    worker.files._safe_components(report_path, require_file=True)
    with report_path.open('rb') as handle:
        raw = handle.read(MAX_BYTES + 1)
    need(sha(raw) == expected_sha and len(raw) <= MAX_BYTES, 'cost_report_identity_or_bound')
    report = summary.strict_json(raw)
    summary.validate_report(report, started)
    spec, _ = worker.load_spec(Path(report['registry_path']), summary.REGISTRY_SHA)
    need(report['source_bindings'] == spec['source_bindings'], 'cost_original_source_closure')
    refs = {r['relative_path']: r for r in report['verified_files']}
    need(len(refs) == len(report['verified_files']), 'cost_duplicate_file_reference')
    results, evidence, total = [], [], 0
    last_read = started
    for episode in report['episodes']:
        if not episode.get('steps'):
            results.append(dict(episode_id=episode['episode_id'], status=episode['status'],
                attribution='unavailable_without_verified_steps'))
            continue
        summary.economic_evidence(report, episode['episode_id'])
        settlements = []
        for step in episode['steps']:
            parts = ('episodes', episode['episode_id'], 'steps', step['step_id'])
            key = '/'.join((*parts, 'settlement.json'))
            need(key in refs, 'cost_missing_original_file_reference')
            read_started = summary.epoch(clock())
            payload = worker.io.read_file(Path(spec['output_root']), parts, 'settlement.json')
            completed = summary.epoch(clock())
            total += len(payload)
            need(last_read <= read_started <= completed and total <= MAX_TOTAL_BYTES, 'cost_read_clock_or_budget')
            last_read = completed
            need(len(payload) == refs[key]['bytes'] and sha(payload) == refs[key]['sha256'],
                 'cost_original_settlement_changed')
            settlements.append(summary.strict_json(payload))
            evidence.append(dict(**refs[key], attribution_read_started_epoch=read_started,
                                 attribution_read_completed_epoch=completed))
        results.append(decompose_episode(episode, settlements))
    worker.verify_sources(spec)
    need(Path(__file__).read_bytes() == source_raw and
         sha((BASE / 'summarize_verified_episodes_v1.py').read_bytes()) == SUMMARY_SHA,
         'cost_source_changed_during_evaluation')
    end = summary.epoch(clock())
    need(end >= last_read, 'cost_completion_clock_regression')
    return dict(schema_version=SCHEMA, status='completed', started_epoch=started, completed_epoch=end,
        report_path=str(report_path), report_bytes_sha256=expected_sha,
        verifier_source_sha256=summary.VERIFIER_SHA, summary_source_sha256=SUMMARY_SHA,
        helper_source_sha256=sha(source_raw), registry_sha256=summary.REGISTRY_SHA,
        source_bindings=spec['source_bindings'], episodes=results, prior_verification_issues=report['issues'],
        retained_input_evidence=evidence, total_read_bytes=total,
        formula='realized_USD = signed_units_times_midpoint_change - entry_and_exit_half_spreads - entry_and_exit_fixed_slippage',
        costs_included=['actual_retained_bid_ask_half_spreads', 'registered_0.1_bps_per_leg_hypothetical_slippage'],
        costs_not_modeled=['financing', 'commissions', 'market_impact'],
        scope='Separate counterfactual arms; realized trades only; no liquidation marks counted as action legs.',
        runtime_writes=False, broker_actions=False, model_fits=False, independent_sample_size=None)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.absolute()
    need(output.parent == BASE and output.suffix == '.json', 'cost_new_external_output_required')
    result = evaluate(args.report.absolute(), args.expected_sha256)
    raw = (json.dumps(result, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    with output.open('xb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    need(output.read_bytes() == raw, 'cost_output_readback')
    print(json.dumps(dict(output=str(output), sha256=sha(raw), episodes=len(result['episodes']),
        status=result['status'], runtime_writes=False)))


if __name__ == '__main__':
    main()
