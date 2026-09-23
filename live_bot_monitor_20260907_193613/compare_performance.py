"""Compare two retained one-shot snapshots; never accesses live data."""
import argparse
from collections import Counter
from decimal import Decimal, localcontext
import hashlib
import json
from pathlib import Path


def load(path):
    payload = path.read_bytes()
    return json.loads(payload), hashlib.sha256(payload).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--current', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    target = args.output.resolve()
    if target.parent != root or target.exists():
        raise ValueError('new_monitor_output_required')
    baseline, baseline_sha = load(args.baseline)
    current, current_sha = load(args.current)
    if baseline['registry_sha256'] != current['registry_sha256']:
        raise ValueError('registry_changed')
    if baseline['registered_source_bindings'] != current['registered_source_bindings']:
        raise ValueError('registered_sources_changed')
    old = {(pair, row['decision_id']): row for pair, value in baseline['pairs'].items()
           for row in value.get('fresh_scored_rows', [])}
    new = {(pair, row['decision_id']): row for pair, value in current['pairs'].items()
           for row in value.get('fresh_scored_rows', [])}
    disappeared = sorted(set(old)-set(new))
    changed = sorted(key for key in set(old)&set(new) if old[key] != new[key])
    added = sorted(set(new)-set(old))
    reconciliation = {}
    for key in ('duplicate_reference_groups', 'retained_outcomes_not_fresh_scored',
                'fresh_scored_without_retained_outcome', 'selected_quote_mismatches',
                'fresh_scored_not_in_stored_scorecard', 'stored_scored_not_in_fresh_evaluation',
                'saved_vs_fresh_scoring_mismatches'):
        reconciliation[key] = {pair: value[key] for pair, value in current['pairs'].items()
                               if value.get(key)}
    families = {}
    with localcontext() as ctx:
        ctx.prec = 80
        for family in ('probabilistic_state_space', 'ridge_return_repaired'):
            scores = [new[key]['scores'][family] for key in added]
            families[family] = {
                'new_scored_decisions':len(scores),
                'direction_hits':sum(score['direction_correct'] is True for score in scores),
                'positive_after_spread':sum(score['positive_after_spread'] is True for score in scores),
                'mean_net_bps':str(sum(Decimal(score['net_bps']) for score in scores)/len(scores)) if scores else None,
            }
    report = {
        'status':'compared', 'baseline_sha256':baseline_sha, 'current_sha256':current_sha,
        'baseline_cutoff':baseline['finished_utc'], 'current_cutoff':current['finished_utc'],
        'baseline_scored':len(old), 'current_scored':len(new), 'new_scored':len(added),
        'disappeared_scored_ids':disappeared, 'changed_prior_scored_rows':changed,
        'new_scored_ids':added, 'new_decision_metrics':families,
        'snapshot_reconciliation':reconciliation,
        'pair_status_counts':dict(Counter(value.get('status') for value in current['pairs'].values())),
        'aggregate_counts':current['aggregate_counts'],
        'cumulative_metrics':current['fresh_pair_family_metrics'],
        'scope':'Two-model pair study only; original EUR companion remains separate.',
        'limitations':['Correlated pairs and overlapping H1 targets are not independent trials.',
                       'Research quote outcomes are not executed broker returns.',
                       'Stored-scorecard lag is reported separately from exact fresh evaluation.'],
    }
    with target.open('x', encoding='utf-8') as handle:
        json.dump(report, handle, indent=2)
        handle.write('\n')
    print(json.dumps({key:value for key,value in report.items()
                      if key not in ('new_scored_ids','cumulative_metrics')}, separators=(',',':')))


if __name__ == '__main__':
    main()
