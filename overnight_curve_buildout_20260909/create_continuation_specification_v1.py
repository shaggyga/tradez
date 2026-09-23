"""Make an explicit result index from the verified additional-evidence copy."""
import argparse
import hashlib
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent
PREFIX = 'docs/validation/overnight_curve_buildout_20260909/'
GAPS = [
    'The larger unchanged joint study, all-pair after-spread dispersion and original paper results do not establish profitable prediction or position management.',
    'The new main outcomes are cumulative dependent observations. Independent sessions and genuinely held-out magnitude, cost and probability acceptance remain required.',
    'The shared-grid/fresh-update curve and risk comparison is still documentary; incompatible original S5/M1 targets and full-horizon risk cannot be relabeled as conditional remaining-position risk.',
    'New manager reconciliation, direction, separate continuation channels and chain-bridge companions remain inactive; execution and broker fill attribution were not tested.',
    'The two TRY family gaps retain actual logged training counts. Missing inputs, sparse minutes, maturity and chronology requirements were not weakened.',
    'News-clock polling cannot reconstruct earlier overwritten failed inputs; producer-envelope errors remain separate from independent ledger availability.',
    'Logical news capture capacity, point-in-time historical response-memory clocks and broader co-movement integration remain open.',
    'Each evaluation keeps its original inventory/read cutoff, pending horizons and unavailable observations. Later clock time is not a completed assessment.',
    'This continuation indexes additional evidence. The separately retained early-close validation indexes its original629members, and the verified new source manifest inventories the entire exported tree.',
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--copy-receipt', type=Path, required=True)
    parser.add_argument('--expected-sha256', required=True)
    args = parser.parse_args()
    path = args.copy_receipt.absolute()
    if path.parent != BASE:
        raise ValueError('continuation_copy_scope')
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != args.expected_sha256:
        raise ValueError('continuation_copy_changed')
    copied = json.loads(raw)
    if copied['status'] != 'copied_exact_members':
        raise ValueError('actual_continuation_copy_required')
    references = []
    # Every selected JSON evidence record is retained, not just favorable cells.
    for row in copied['entries']:
        name = row['intended_member']
        if name.endswith('.json'):
            references.append(dict(label=name.removeprefix(PREFIX), archive_member=name,
                sha256=row['original_source']['sha256']))
    if not 1 <= len(references) <= 100:
        raise ValueError('continuation_reference_bound')
    value = dict(evidence=references, remaining_gaps=GAPS)
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    output = BASE / 'CONTINUATION_VALIDATION_SPECIFICATION_20260909.json'
    with output.open('xb') as handle:
        handle.write(raw)
    if output.read_bytes() != raw:
        raise ValueError('continuation_specification_readback')
    print(json.dumps(dict(path=str(output), sha256=hashlib.sha256(raw).hexdigest(), references=len(references))))


if __name__ == '__main__':
    main()
