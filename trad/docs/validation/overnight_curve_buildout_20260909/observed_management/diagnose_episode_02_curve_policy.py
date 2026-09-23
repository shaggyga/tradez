"""Read-only, accepted-byte-bound explanation of one already completed episode.

No model invocation, policy search, source/runtime write or broker interaction.
Only a fresh external evidence directory is created.
"""
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, Context, localcontext
import hashlib
import json
import os
from pathlib import Path
import time

HERE = Path(__file__).resolve().parent
TRAD = HERE.parents[1] / 'trad'
ARMS = ('usd_curve_manager', 'curve_hold_no_rotation')
PINS = {
    'OBSERVED_EPISODE_TERMINAL_02_20260909.json': 'd80d6564fb65745328d8df2cf4c539e00a6760b5b7ad724e3c180a558cc782b9',
    'COST_ATTRIBUTION_EPISODES_01_02_20260909.json': 'b011111415e5f7d96e399da5dc81ae4f7177415c66164b292f50f94e150e2eef',
}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def utc(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def write_new(path, value):
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    with path.open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return {'path': str(path), 'sha256': sha(raw), 'bytes': len(raw)}


def run():
    began = time.time()
    evidence = []

    def read(path, expected, size=None):
        before = path.stat()
        require(before.st_size <= 4 * 1024 * 1024, 'bounded_file')
        started = time.time()
        raw = path.read_bytes()
        ended = time.time()
        after = path.stat()
        require((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns), 'changed_during_read')
        require(sha(raw) == expected and (size is None or len(raw) == size), 'accepted_source_bytes_changed')
        evidence.append({'path': str(path), 'sha256': expected, 'bytes': len(raw),
                         'read_started_epoch': started, 'read_completed_epoch': ended})
        return raw

    accepted, costs = [json.loads(read(HERE / name, pin)) for name, pin in PINS.items()]
    require(accepted['status'] == 'passed' and not accepted['issues'], 'accepted_verification_required')
    require(costs['report_bytes_sha256'] == PINS['OBSERVED_EPISODE_TERMINAL_02_20260909.json'], 'cost_original_verifier')
    registry_path = Path(accepted['registry_path'])
    registry = json.loads(read(registry_path, accepted['registry_sha256']))
    require(costs['registry_sha256'] == accepted['registry_sha256'], 'common_registry')
    for name, pin in registry['source_bindings'].items():
        read(TRAD / name, pin)
    root = Path(registry['output_root'])
    index = {item['relative_path']: item for item in accepted['verified_files']}

    def retained(rel):
        item = index[rel]
        return json.loads(read(root / rel, item['sha256'], item['bytes']))

    episode = next(e for e in accepted['episodes'] if e['episode_id'] == 'episode_02')
    cost = next(e for e in costs['episodes'] if e['episode_id'] == 'episode_02')
    require(episode['completed_matched_endpoint_eligible'] and episode['terminal_all_five_flat']
            and not episode['missed_slots'] and not episode['uncompleted_or_unobserved_slots'], 'complete_episode_required')
    config = retained('episodes/episode_02/episode_config.json')
    initial = retained('episodes/episode_02/initial_curve_consumption.json')
    original = episode['original_published_source_evidence']
    curve = None
    for item in original['files']:
        value = json.loads(read(Path(original['root']) / item['relative_path'], item['sha256'], item['bytes']))
        if item['relative_path'].endswith('/curve.json'):
            curve = value
    require(curve is not None and curve['curve_sha256'] == config['curve_binding']['curve_sha256'], 'original_curve_binding')
    policy = config['usd_policy']
    terminal = Decimal(config['curve_binding']['expected_terminal_price'])
    pip = Decimal('0.0001')
    traces, events, reason_counts = [], [], {arm: Counter() for arm in ARMS}
    same_entry = cost['per_arm'][ARMS[0]]['completed_trades'][0]['entry'] == cost['per_arm'][ARMS[1]]['completed_trades'][0]['entry']
    require(same_entry, 'do_not_claim_unmatched_entry_matched')
    with localcontext(Context(prec=96)):
        for step in episode['steps']:
            stem = f"episodes/episode_02/steps/{step['step_id']}"
            plan = retained(stem + '/plan.json')
            settlement = retained(stem + '/settlement.json')
            require(plan['decision_epoch'] == step['decision_epoch'] and
                    settlement['settlement_computed_epoch'] == step['settlement_computed_epoch'] and
                    settlement['states']['state_sha256'] == step['state_sha256'], 'accepted_step_identity')
            require(plan['plan_created_epoch'] <= step['publication_completed_epoch'] <= settlement['selected_observation_epoch']
                    <= settlement['settlement_computed_epoch'], 'actual_plan_publication_quote_clocks')
            rows = plan['curve_candidate_originals']
            prepared = plan['prepared_candidate_evidence']['curve']
            require(len(rows) <= 1 and len(prepared) <= 1, 'single_fixed_native_node')
            candidate = rows[0] if rows else None
            p = prepared[0] if prepared else None
            q = plan['decision_quote_mapping']
            q = None if q is None else q['quotes'].get('GBP_USD')
            mid = (Decimal(q['bid']) + Decimal(q['ask'])) / 2 if q else None
            if candidate:
                require(candidate['curve_sha256'] == curve['curve_sha256'] and
                        candidate['node_id'] == config['curve_binding']['node_id'] and
                        Decimal(candidate['expected_terminal_price']) == terminal and
                        candidate['original_target_epoch'] == episode['native_target_epoch'], 'no_refit_new_curve_or_retarget')
                require(Decimal(candidate['expected_remaining_move_pips']) == (terminal - mid) / pip, 'original_remaining_move_formula')
            selected = settlement['selected_quote_mapping']['quotes']['GBP_USD']
            selected_mid = (Decimal(selected['bid']) + Decimal(selected['ask'])) / 2
            trace = {'step_id': step['step_id'], 'decision_epoch': plan['decision_epoch'], 'decision_utc': utc(plan['decision_epoch']),
                     'plan_created_epoch': plan['plan_created_epoch'], 'publication_completed_epoch': step['publication_completed_epoch'],
                     'selected_observation_epoch': settlement['selected_observation_epoch'],
                     'selected_observation_utc': utc(settlement['selected_observation_epoch']),
                     'settlement_computed_epoch': settlement['settlement_computed_epoch'], 'terminal': plan['terminal'],
                     'original_terminal_price': str(terminal), 'original_target_epoch': episode['native_target_epoch'],
                     'decision_midpoint': None if mid is None else str(mid), 'execution_observation_midpoint': str(selected_mid),
                     'candidate_available': candidate is not None, 'candidate_refusals': plan['candidate_refusals']['curve'],
                     'remaining_pips_used': None if candidate is None else candidate['expected_remaining_move_pips'],
                     'remaining_sec': None if candidate is None else candidate['remaining_sec'],
                     'prepared_entry_economics': None if p is None else {k: p[k] for k in ('side', 'new_entry_net_usd', 'round_trip_cost_usd', 'signed_price_change')},
                     'arms': {}}
            for arm in ARMS:
                a = settlement['arms'][arm]
                decision = plan['decisions'][arm]
                require(decision == a['decision'] and decision['action'] == step['decisions'][arm], 'original_decision_identity')
                before = a['state_before']['position']
                diagnostics = plan['decision_diagnostics'][arm]
                action, reason = decision['action'], decision['reason']
                reason_counts[arm][action + ':' + reason] += 1
                if plan['terminal']:
                    require(action == ('exit' if before else 'wait') and reason == 'predeclared_terminal', 'terminal_policy')
                elif before is None:
                    eligible = p is not None and Decimal(p['new_entry_net_usd']) >= 0
                    require(action == ('enter' if eligible else 'wait'), 'flat_entry_cost_rule')
                elif arm == 'curve_hold_no_rotation':
                    require(action == 'hold' and reason == 'hold_current_no_rotation', 'fixed_hold_rule')
                else:
                    require(p is not None and mid is not None, 'actual_incumbent_estimate_missing')
                    units = before['base_units']; side = before['side']
                    gross = units * side * (terminal - mid)
                    liquidation = units * ((Decimal(q['ask']) - Decimal(q['bid'])) / 2 + mid * Decimal('0.1') / 10000)
                    hold, exit_value = gross - liquidation, -liquidation
                    require(Decimal(diagnostics['hold_value_usd']) == hold and Decimal(diagnostics['exit_value_usd']) == exit_value,
                            'independent_existing_side_continuation_arithmetic')
                    opposite = p['side'] != side and Decimal(p['new_entry_net_usd']) >= 0
                    switch = Decimal(p['new_entry_net_usd']) - liquidation if opposite else None
                    require(diagnostics['switch_value_usd'] == (None if switch is None else str(switch)), 'switch_value_arithmetic')
                    expected = ('rotate' if switch is not None and switch > max(hold + Decimal('0.05'), exit_value)
                                else 'hold' if hold >= exit_value else 'exit')
                    require(action == expected, 'fixed_usd_selector_branch')
                legs = a['virtual_action']['legs']
                trace['arms'][arm] = {'decision': decision, 'diagnostics': diagnostics,
                    'position_before': before, 'position_after': settlement['states']['arms'][arm]['position'],
                    'existing_side_expected_gross_usd': None if before is None or candidate is None else str(before['base_units'] * before['side'] * (terminal - mid)),
                    'virtual_action_status': a['virtual_action']['status'], 'legs': legs}
                if legs:
                    events.append({'step_id': step['step_id'], 'arm': arm, 'action': action, 'declared_reason': reason,
                                   'decision_epoch': plan['decision_epoch'], 'selected_observation_epoch': settlement['selected_observation_epoch'],
                                   'leg_count': len(legs)})
            traces.append(trace)
        m, h = [cost['per_arm'][arm] for arm in ARMS]
        gross_difference = Decimal(h['gross_midpoint_pnl_usd']) - Decimal(m['gross_midpoint_pnl_usd'])
        extra_cost = sum(Decimal(m[k]) - Decimal(h[k]) for k in ('spread_cost_usd', 'slippage_cost_usd'))
        net_difference = Decimal(h['realized_usd']) - Decimal(m['realized_usd'])
        require(gross_difference + extra_cost == net_difference, 'exact_arm_difference_decomposition')
        for arm in ARMS:
            value = cost['per_arm'][arm]
            require(value['realized_usd'] == episode['per_arm'][arm]['realized_usd'], 'accepted_net_reconciliation')
            for trade in value['completed_trades']:
                for kind in ('entry', 'exit'):
                    t = next(t for t in traces if t['step_id'] == trade[kind]['step_id'])
                    require(trade[kind]['action_epoch'] == t['selected_observation_epoch'], 'cost_actual_action_time')
                    require(trade[kind]['midpoint'] == t['execution_observation_midpoint'], 'cost_exact_selected_midpoint')
            require(sum(Decimal(t['realized_usd']) for t in value['completed_trades']) == Decimal(value['realized_usd']), 'trade_sum')
        first = next(t for t in traces if t['candidate_available'])
        last = traces[-1]
        result = {'schema_version': 'episode_02_curve_policy_diagnosis_v1_20260909', 'status': 'passed_readonly_reconciliation',
                  'started_epoch': began, 'completed_epoch': time.time(), 'helper_source_sha256': sha(Path(__file__).read_bytes()),
                  'episode_id': 'episode_02', 'instrument': 'GBP_USD', 'verified_step_count': len(traces),
                  'registry_sha256': accepted['registry_sha256'], 'source_bindings': registry['source_bindings'],
                  'curve_binding': config['curve_binding'], 'usd_policy': policy, 'initial_reference_price': '1.35629',
                  'initial_predicted_signed_pips': '-0.7231159329455111', 'retained_probability_up': '0.4789531759996235',
                  'retained_residual_std_pips': '8.584295670314077', 'probability_used_for_selector': False,
                  'uncertainty_used_for_selector': False, 'same_initial_entry_exact_quote_side_units': same_entry,
                  'same_later_entries': False, 'original_native_target_utc': utc(episode['native_target_epoch']),
                  'terminal_selected_midpoint': last['execution_observation_midpoint'],
                  'terminal_quote_scope': 'actual later-observed tradeable pricing tick, not an exact target-time candle or broker fill',
                  'per_arm_cost_attribution': {arm: cost['per_arm'][arm] for arm in ARMS},
                  'hold_minus_manager': {'gross_midpoint_usd': str(gross_difference), 'manager_extra_cost_usd': str(extra_cost),
                                        'net_usd': str(net_difference)},
                  'action_reason_counts': {arm: dict(reason_counts[arm]) for arm in ARMS}, 'actual_action_events': events,
                  'steps': traces, 'input_evidence': evidence,
                  'findings': [
                      'No arithmetic or action-branch implementation mismatch was found against the frozen rules on all 59 retained decisions.',
                      'One fixed H1 terminal estimate was reused throughout; crossing below it changed remaining direction from short to long without new model inference.',
                      'The first short entry was exactly matched across both arms; manager exit and later long entry break subsequent entry comparability.',
                      'The manager made two round trips, with no atomic rotate action; gross path/side selection explains most of the difference, with incremental costs separately reconciled.',
                      'As price fell farther below the unchanged endpoint estimate, expected long continuation rose; this is declared fixed-terminal arithmetic, not a newly learned conditional recovery forecast.',
                      'The selector contains no adverse-excursion, stop-loss or recalibrated remaining-probability gate; retained original uncertainty is disclosed but unused in this selection.',
                  ],
                  'limits': ['One completed episode selected for diagnosis after observing its result; no policy tuning, model fitting or independent sample-size claim.',
                            'Original training selects the first real bar within 0..7 seconds after the nominal target; management explicitly uses the nominal boundary approximation.',
                            'Selected current quotes can have market timestamps preceding their later actual read; execution and observation clocks are distinct and retained.',
                            'USD2500 notional and 0.1bps per-leg slippage are fixed research assumptions; this is virtual accounting, not account profit, liquidity or broker fills.'],
                  'runtime_writes': False, 'broker_actions': False, 'model_fits': False, 'policy_changes': False}
    destination = HERE / 'episode_02_policy_diagnosis_001'
    destination.mkdir(exist_ok=False)
    receipt = write_new(destination / 'EPISODE_02_CURVE_POLICY_DIAGNOSIS_20260909.json', result)
    print(json.dumps({'receipt': receipt, 'counts': result['action_reason_counts'], 'difference': result['hold_minus_manager']}))


if __name__ == '__main__':
    run()
