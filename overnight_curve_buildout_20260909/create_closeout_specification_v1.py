"""Build the explicit evidence references consumed by the reviewed assembler."""
import argparse
import hashlib
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent
PREFIX = 'docs/validation/overnight_curve_buildout_20260909/'
REFERENCES = {
    'Curve pipeline preactivation': 'PIPELINE_PREACTIVATION_ACCEPTANCE_20260909.json',
    'Completed joint outcomes': 'joint_v3_reassessment_v1/actual_assessment_001/V3_COMPLETED_PERFORMANCE_20260909.json',
    'Joint probability reliability bins': 'joint_v3_reassessment_v1/calibration_v1/actual_calibration_002/JOINT_PROBABILITY_BINS_20260909.json',
    'All retained pilot horizon results': 'prospective_pilot/complete_results_v3_002/COMPLETE_PILOT_HORIZON_RESULTS_20260909.json',
    'All retained risk results': 'richer_inputs/prospective_risk_v1/complete_results_004/COMPLETE_RETAINED_RISK_RESULTS_20260909.json',
    'Three completed paper episodes': 'observed_management/OBSERVED_THREE_EPISODE_FINAL_RESULT_20260909.json',
    'Paper episode independent acceptance': 'observed_management/OBSERVED_THREE_EPISODE_FINAL_ACCEPTANCE_20260909.json',
    'Account manager corrections': 'management_correctness_audit_v1/ACCOUNT_MANAGER_CORRECTION_ACCEPTANCE_20260909.json',
    'Original-chain management bridge integration': 'replay/chain_bridge_v1/PURE_INTEGRATION_RESULT_20260909.json',
    'Actual retained-chain compatibility diagnostic': 'replay/chain_bridge_v1/ACTUAL_RETAINED_H3_BRIDGE_COMPACT_20260909.json',
    'Root compatibility evidence review': 'ROOT_ACTUAL_CHAIN_BRIDGE_COMPATIBILITY_REVIEW_20260909.json',
    'Entry direction admission': 'richer_inputs/curve_direction_admission_v1/DIRECTION_ADMISSION_FINAL_ACCEPTANCE_20260909.json',
    'Curve risk attachment': 'richer_inputs/curve_risk_attachment_v1/CURVE_RISK_ATTACHMENT_ACCEPTANCE_20260909.json',
    'Future shared-grid documentary protocol': 'richer_inputs/curve_risk_attachment_v1/SHARED_GRID_FRESH_UPDATE_MANAGEMENT_DOCUMENTARY_PROTOCOL_20260909.json',
    'Causal MA repair': 'richer_inputs/MA_CAUSAL_FEATURES_ACCEPTANCE_20260909.json',
    'Wider feature comparison': 'richer_inputs/ma_comparison_v1/MA_COMPARISON_COMPACT_ASSESSMENT_20260909.json',
    'Cross-window features': 'richer_inputs/CROSS_WINDOW_IMPLEMENTATION_VALIDATION_20260909.json',
    'Entry news feature join': 'entry_news_features/ENTRY_NEWS_FEATURE_JOIN_ACCEPTANCE_20260909.json',
    'Dated current news audit': 'entry_news_features/current_refresh_002/ENTRY_NEWS_CURRENT_REFRESH_20260909.json',
    'Currency exposure diagnostics': 'richer_inputs/portfolio_risk_reuse_review/CURRENCY_EXPOSURE_IMPLEMENTATION_VALIDATION_20260909.json',
    'Feature dictionary metadata refresh': 'feature_dictionary_source_refresh_v1/FEATURE_DICTIONARY_METADATA_REFRESH_VALIDATION_20260909.json',
    'Deployed ledger reader': 'ledger_endpoint_reliability_probe_v1/OBSERVER_API_V2_DEPLOYMENT_ACCEPTANCE_20260909.json',
    'Completed reader reliability observation': 'ledger_endpoint_reliability_probe_v1/LEDGER_ENDPOINT_V2_RELIABILITY_ASSESSMENT_20260909.json',
    'Retained TRY readiness refresh': 'try_pair_publication_audit_v1/TRY_READINESS_REFRESH_001_20260909.json',
    'Closeout runtime observation': 'operations_v3/OPERATIONS_V3_OBSERVATION_004_20260909.json',
    'User-requested canceled later evaluations': 'final_evaluations_20260909/FINAL_EVALUATION_EARLY_CLOSE_PLAN_CHANGE_20260909.json',
    'Portable preparation review': 'PORTABLE_EVIDENCE_PREPARATION_INDEPENDENT_REVIEW_20260909.json',
    'Final validation assembler review': 'FINAL_VALIDATION_ASSEMBLER_INDEPENDENT_REVIEW_20260909.json',
    'Vault publisher review': 'OVERNIGHT_PUBLICATION_HELPER_INDEPENDENT_REVIEW_20260909.json',
}
GAPS = [
    'No profitable predictor or position policy established: completed joint, wider-feature and paper comparisons remain unfavorable; overlapping outcomes are not independent trials.',
    'Fresh shared-grid curve/risk issuance is documentary only. Original S5 target windows and M1 exact targets cannot be silently aligned or relabeled.',
    'New direction, two-channel management, chain-bridge and account-manager corrections remain inactive. Broker transaction/fill attribution and live execution were not tested.',
    'Full-horizon empirical risk distributions are not calibrated conditional remaining-position risk or executable stop-fill probabilities.',
    'Point-in-time news/factor/response-memory clocks, broader co-movement integration and independent prospective acceptance remain required.',
    'TRY/JPY and USD/TRY lack required joint training rows in the latest retained readiness records; no minimum or clock requirement was relaxed.',
    'The original producer envelope still has mismatches; the deployed independent ledger view handles forecast visibility separately. Historical failed news inputs cannot be reconstructed.',
    'Logical news capture capacity remains bounded. The inactive lossless storage experiment does not eliminate the active logical limit.',
    'User requested early closeout. Planned later joint/pilot/risk evaluations were not run; each completed result keeps its original cutoff and pending/missing outcomes.',
    'Main research continues with orders disabled. Existing bounded risk/news observation runs end at 12:45 UTC and pilot collection at 13:00 UTC; those future completions are not claimed by this receipt.',
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--copy-receipt', type=Path, required=True)
    parser.add_argument('--expected-sha256', required=True)
    args = parser.parse_args()
    path = args.copy_receipt.absolute()
    if path.parent != BASE:
        raise ValueError('copy_receipt_scope')
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != args.expected_sha256:
        raise ValueError('copy_receipt_changed')
    copied = json.loads(raw)
    if copied['status'] != 'copied_exact_members':
        raise ValueError('completed_copy_required')
    inventory = {r['intended_member']: r for r in copied['entries']}
    evidence = []
    for label, relative in REFERENCES.items():
        member = PREFIX + relative
        record = inventory[member]
        evidence.append(dict(label=label, archive_member=member, sha256=record['original_source']['sha256']))
    value = dict(evidence=evidence, remaining_gaps=GAPS)
    output = BASE / 'FINAL_VALIDATION_SPECIFICATION_20260909.json'
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    with output.open('xb') as handle:
        handle.write(raw)
    if output.read_bytes() != raw:
        raise ValueError('specification_readback')
    print(json.dumps(dict(path=str(output), sha256=hashlib.sha256(raw).hexdigest(), references=len(evidence))))


if __name__ == '__main__':
    main()
