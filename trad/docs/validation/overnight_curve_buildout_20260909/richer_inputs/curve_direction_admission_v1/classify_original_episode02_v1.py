"""New semantic classification of accepted old evidence; not a policy replay.

Original decision clocks are replay context only. Actual diagnostic start/end
clocks are retained separately. No decisions, positions or P&L are recomputed.
"""
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
TRAD = HERE.parents[2] / 'trad'
MANAGEMENT = HERE.parents[1] / 'observed_management'
MODULE_PIN = 'b3453296c20aa7e207826f30c7c7b2d697b42929f595aabeb5f9b21087b8ba7a'
VERIFIER_PIN = 'd80d6564fb65745328d8df2cf4c539e00a6760b5b7ad724e3c180a558cc782b9'
ARMS = ('usd_curve_manager', 'curve_hold_no_rotation')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def need(ok, reason):
    if not ok:
        raise ValueError(reason)


def run():
    started = time.time()
    inputs = []

    def read(path, pin, size=None):
        first = path.stat(); begin = time.time()
        need(first.st_size <= 4 * 1024 * 1024, 'classification_file_bound')
        raw = path.read_bytes(); end = time.time(); last = path.stat()
        need((first.st_size, first.st_mtime_ns) == (last.st_size, last.st_mtime_ns), 'source_changed_during_read')
        need(sha(raw) == pin and (size is None or len(raw) == size), 'accepted_source_mismatch')
        inputs.append(dict(path=str(path), sha256=pin, bytes=len(raw), read_started_epoch=begin, read_completed_epoch=end))
        return raw

    read(TRAD / 'oanda_curve_direction_admission_v1.py', MODULE_PIN)
    sys.path.insert(0, str(TRAD))
    import oanda_curve_direction_admission_v1 as gate
    accepted = json.loads(read(MANAGEMENT / 'OBSERVED_EPISODE_TERMINAL_02_20260909.json', VERIFIER_PIN))
    need(accepted['status'] == 'passed' and not accepted['issues'], 'accepted_original_verifier_required')
    registry = json.loads(read(Path(accepted['registry_path']), accepted['registry_sha256']))
    for name, pin in registry['source_bindings'].items():
        read(TRAD / name, pin)
    root = Path(registry['output_root'])
    index = {row['relative_path']: row for row in accepted['verified_files']}

    def retained(rel):
        meta = index[rel]
        return json.loads(read(root / rel, meta['sha256'], meta['bytes']))

    prefix = 'episodes/episode_02/'
    config = retained(prefix + 'episode_config.json')
    state = retained(prefix + 'initial_states.json')
    episode = next(e for e in accepted['episodes'] if e['episode_id'] == 'episode_02')
    rows = []; counts = Counter(); classifications = 0
    metadata = config['usd_policy']['metadata']['GBP_USD']
    for step in episode['steps']:
        stem = prefix + 'steps/' + step['step_id'] + '/'
        plan = retained(stem + 'plan.json')
        need(plan['before_state_sha256'] == state['state_sha256'] and state['known_epoch'] <= plan['decision_epoch'],
             'original_incumbent_state_or_clock')
        row = dict(step_id=step['step_id'], original_decision_epoch=plan['decision_epoch'],
                   original_decision_clock_scope='replay_context_not_new_actual_decision',
                   original_plan_sha256=plan['plan_sha256'], original_state_sha256=state['state_sha256'],
                   original_state_known_epoch=state['known_epoch'], original_terminal=plan['terminal'], arms={})
        if plan['terminal']:
            need(not plan['curve_candidate_originals'], 'terminal_original_candidate_absent')
            row['status'] = 'terminal_originally_no_curve_candidate_not_classified'
            counts[row['status']] += 1
        else:
            bundle = retained(stem + 'curve_consumption.json')
            mapping = plan['decision_quote_mapping']
            quote = None if mapping is None else mapping['quotes'].get('GBP_USD')
            row['original_curve_sha256'] = bundle['curve']['curve_sha256']
            row['original_consumption_sha256'] = bundle['consumption']['consumption_sha256']
            original_candidates = plan['curve_candidate_originals']
            for arm in ARMS:
                position = state['arms'][arm]['position']
                held = None if position is None else dict(instrument=position['instrument'], side=position['side'],
                    original_target_epoch=position['original_target_epoch'], observed_epoch=state['known_epoch'],
                    state_sha256=state['state_sha256'])
                computation_started = time.time()
                value = gate.admit_candidate_for_target(bundle['curve'], bundle['publication'], bundle['consumption'],
                    decision_epoch=plan['decision_epoch'], target_epoch=config['native_target_epoch'], quote=quote,
                    metadata=metadata, expected_source_bindings=bundle['curve']['prepared_curve']['source_bindings'],
                    maximum_quote_age_sec=config['policy']['maximum_quote_age_sec'],
                    target_window_policy=config['policy']['target_window_policy'], incumbent=held)
                completed = time.time(); classifications += 1
                need(started <= computation_started <= completed, 'actual_diagnostic_clock_order')
                if original_candidates:
                    need(len(original_candidates) == 1 and value['candidate'] == original_candidates[0],
                         'exact_original_candidate_reconstruction')
                else:
                    need(value['status'] == 'unavailable', 'missing_original_candidate_not_invented')
                row['arms'][arm] = dict(status=value['status'], reason_code=value['reason_code'],
                    original_forecast_side=value['original_forecast_side'], rebased_remaining_side=value['rebased_remaining_side'],
                    new_entry={k:v for k,v in value['new_entry'].items() if k != 'candidate'},
                    opposite_side_rotation={k:v for k,v in value['opposite_side_rotation'].items() if k != 'candidate'},
                    continuation={k:v for k,v in value['continuation'].items() if k != 'candidate'},
                    original_incumbent=held, original_candidate_sha256=value['input_candidate_sha256'],
                    admission_sha256=value['admission_sha256'], new_diagnostic_computation_started_epoch=computation_started,
                    new_diagnostic_computed_epoch=completed, fresh_input_update_verified=value['fresh_input_update_verified'])
            first = row['arms'][ARMS[0]]
            row['status'] = first['status']; counts[first['status'] + ':' + first['reason_code']] += 1
        rows.append(row)
        state = retained(stem + 'state_after.json')
        need(state['state_sha256'] == step['state_sha256'], 'accepted_original_next_state')
    by_step = {row['step_id']: row for row in rows}
    seven = by_step['step_007']['arms']
    need(not seven['usd_curve_manager']['new_entry']['semantic_admitted']
         and seven['usd_curve_manager']['reason_code'] == 'countertrend_from_original_terminal_crossing', 'step007_not_new_countertrend_evidence')
    need(seven['usd_curve_manager']['original_incumbent'] is None, 'do_not_invent_manager_short_after_actual_exit')
    need(seven['curve_hold_no_rotation']['original_incumbent']['side'] == -1
         and seven['curve_hold_no_rotation']['continuation']['status'] == 'available_for_incumbent'
         and seven['curve_hold_no_rotation']['continuation']['incumbent_signed_remaining_move_pips'] == '-3.9268840670544889',
         'actual_hold_arm_short_continuation_retained')
    four = by_step['step_004']['arms']['usd_curve_manager']
    need(four['original_incumbent']['side'] == -1 and four['continuation']['status'] == 'available_for_incumbent'
         and four['continuation']['incumbent_signed_remaining_move_pips'] == '-0.6268840670544889',
         'original_manager_short_continuation_at_exit_context')
    need(sha((TRAD / 'oanda_curve_direction_admission_v1.py').read_bytes()) == MODULE_PIN, 'new_module_changed')
    result = dict(schema_version='original_episode02_direction_diagnostic_v1_20260909', status='passed',
        original_episode='episode_02', original_decision_context_count=len(rows), original_terminal_context_count=1,
        classified_nonterminal_contexts=len(rows)-1, actual_gate_calls_two_original_arms=classifications,
        context_counts=dict(counts), started_epoch=started, completed_epoch=time.time(),
        module_sha256=MODULE_PIN, helper_sha256=sha(Path(__file__).read_bytes()), original_verifier_sha256=VERIFIER_PIN,
        original_registry_sha256=accepted['registry_sha256'], input_evidence=inputs, rows=rows,
        limits=['Diagnostic classification after the outcome was observed, not proof the gate ran at the old decision time.',
                'No original decision, position, quote election, P&L or source record was changed or replayed as a new policy.',
                'Semantic admission is only a necessary condition; no cost, confidence, full eligibility or profit assertion.',
                'At step007 the original USD manager was flat. The separately retained no-rotation arm actually still held short; no counterfactual incumbent is invented.',
                'A terminal context originally contains no curve candidate and is explicitly not passed through a new hypothetical forecast.'],
        runtime_writes=False, broker_actions=False, model_fits=False, pnl_rerun=False, policy_decisions_rerun=False)
    output = HERE / 'original_episode02_classification_001'
    output.mkdir(exist_ok=False)
    path = output / 'ORIGINAL_EPISODE02_DIRECTION_CLASSIFICATION_20260909.json'
    raw = (json.dumps(result, sort_keys=True, indent=2, allow_nan=False)+'\n').encode()
    with path.open('xb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    print(json.dumps(dict(path=str(path),sha256=sha(raw),bytes=len(raw),counts=dict(counts),gate_calls=classifications)))


if __name__ == '__main__':
    run()
