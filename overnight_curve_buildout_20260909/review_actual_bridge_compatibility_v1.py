"""Root source/result checks of the already-produced retained-chain diagnostic.

No replay, broker request, original raw-market/model-input read, or activation.
"""
from pathlib import Path
import hashlib
import json
import time

BASE = Path(__file__).resolve().parent
ROOT = BASE.parent / 'trad'
FOLDER = BASE / 'replay/chain_bridge_v1'
RECEIPT = FOLDER / 'actual_retained_compatibility_001/ACTUAL_RETAINED_H3_BRIDGE_DIAGNOSTIC_20260909.json'
COMPACT = FOLDER / 'ACTUAL_RETAINED_H3_BRIDGE_COMPACT_20260909.json'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def bound(path, expected):
    raw = path.read_bytes()
    if len(raw) > 4 * 1024 * 1024 or sha(raw) != expected:
        raise ValueError('review_input_changed')
    return raw


def main():
    raw = bound(RECEIPT, '5ee91121dbd8e98b9162b51a8d7e33c2665cae7d8992fc5a7a00eb8de8d4b865')
    value = json.loads(raw)
    compact_raw = bound(COMPACT, '89958a08bfd7d37762fa8a7f31434eab959bb87664b2cffac0d9a82696851c79')
    helper = FOLDER / 'diagnose_latest_retained_gbp_h3.py'
    helper_raw = bound(helper, value['diagnostic_helper_sha256'])
    assert value['status'] == 'diagnostic_completed'
    assert value['source_bindings_unchanged'] is True and value['registry_unchanged'] is True
    assert value['broker_requests'] == value['new_forecasts_created'] == 0
    assert all(value[key] is False for key in ('manager_activation', 'account_state_used',
        'positions_or_fills_created', 'pnl_computed', 'current_forecast_update_claim'))
    assert value['original_horizon_sec'] == 10800 and value['training_target_maximum_delay_sec'] == 7
    assert value['started_epoch'] <= value['decision_epoch'] <= value['diagnostic_computed_epoch'] <= value['completed_epoch']
    assert value['decision_epoch'] < value['original_target_epoch']
    assert value['original_target_epoch'] - value['original_reference_epoch'] == 10800
    assert all(row['read_started_epoch'] <= row['read_completed_epoch'] <= value['decision_epoch']
               for row in value['input_file_reads'])
    for name, expected in value['source_bindings'].items():
        bound(ROOT / name, expected)
    binding = value['bridge_result']
    result_path = Path(binding['path'])
    assert result_path.parent == RECEIPT.parent and result_path.name == 'bridge_result.json'
    result = json.loads(bound(result_path, binding['sha256']))
    assert result['selection']['decision'] == value['diagnostic_decision']
    assert result['selection']['entry_candidate_count'] == value['prepared_entry_count']
    assert result['selection']['continuation_candidate_count'] == value['prepared_continuation_count']
    # These checks bind the owner-produced result; they do not repeat its model replay.
    review = dict(schema_version='root_actual_chain_bridge_review_v1_20260909',
        reviewed_epoch=time.time(), status='source_and_retained_result_review_passed',
        diagnostic_receipt=dict(path=str(RECEIPT), sha256=sha(raw)),
        compact_receipt=dict(path=str(COMPACT), sha256=sha(compact_raw)),
        reviewed_helper=dict(path=str(helper), sha256=sha(helper_raw)),
        retained_result=dict(path=str(result_path), sha256=binding['sha256']),
        source_bindings_rehashed=value['source_bindings'],
        original_fixed_h3_target_preserved=True,
        current_quote_and_state_are_explicitly_separate_from_original_forecast=True,
        new_state_is_hypothetical_flat_only=True,
        interpretation='Actual retained-input/current-quote interface compatibility, not a fresh forecast, position-management experiment, fill, account action or efficacy result.',
        review_scope=['Read the complete diagnostic helper source and its original receipt/result.',
            'Checked source identities, original target, actual recorded chronology and exact decision projection.',
            'Did not rerun the diagnostic, replay raw S5/model inputs, reread the model weights, or independently authenticate the earlier I/O.'],
        broker_requests=False, runtime_writes=False, tests_rerun=False,
        independent_original_inference_replay=False, model_or_manager_activation=False)
    output = BASE / 'ROOT_ACTUAL_CHAIN_BRIDGE_COMPATIBILITY_REVIEW_20260909.json'
    encoded = (json.dumps(review, indent=2, sort_keys=True) + '\n').encode()
    with output.open('xb') as handle:
        handle.write(encoded)
    print(json.dumps(dict(path=str(output), sha256=sha(encoded), status=review['status'])))


if __name__ == '__main__':
    main()
