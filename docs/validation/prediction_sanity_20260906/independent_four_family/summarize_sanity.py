"""Add human-readable independent review and raw timing/integrity diagnostics."""
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path
import statistics

OUT = Path(__file__).resolve().parent
PROJECT = OUT.parent.parent/'trad'
raw = json.loads((PROJECT/'docs/validation/fixed_evaluation_20260906/selected_candidate_rows.json').read_text())
paired = json.loads((OUT/'independently_paired_endpoints.json').read_text())
result = json.loads((OUT/'independent_four_family_results.json').read_text())
included_ids = {forecast['event_id'] for row in paired for forecast in row['families'].values()}
epoch = lambda value: datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
holding = [epoch(row['exit_time'])-epoch(row['reference']) for row in paired]
matched_events = [event for row in raw if row['event_id'] in included_ids for event in row.get('integrity_events',[])]
timing = {
    'matched_epoch_stored_endpoint_delay_from_original_3600_sec_target':{
        'min':min(holding)-3600,'median':statistics.median(holding)-3600,'max':max(holding)-3600,
        'within_original_target_plus_60_sec':sum(3600<=x<=3660 for x in holding),
        'greater_than_original_target_plus_60_sec':sum(x>3660 for x in holding),
    },
    'all_extracted_integrity_event_rows':sum(len(row.get('integrity_events',[])) for row in raw),
    'matched_forecasts_with_any_integrity_event':sum(bool(row.get('integrity_events')) for row in raw if row['event_id'] in included_ids),
    'integrity_event_rows_on_matched_forecasts':len(matched_events),
    'matched_integrity_event_types':dict(Counter(x['event_type'] for x in matched_events)),
    'matched_integrity_event_reasons':dict(Counter(x['reason'] for x in matched_events)),
    'interpretation':'These observations describe retained provenance. They do not erase the arithmetic or establish acceptable prospective target clocks.',
}
with (OUT/'independent_timing_diagnostic.json').open('x',encoding='utf-8') as handle:
    json.dump(timing,handle,indent=2)
    handle.write('\n')
rows = []
labels = {'cross_pair_graph_transfer':'Graph','modern_tabular_probabilistic_repaired':'Tabular',
          'probabilistic_state_space':'State space','ridge_return_repaired':'Ridge'}
for family,metric in result['family_metrics'].items():
    rows.append(f"| {labels[family]} | {metric['correct_direction_count']}/368 | {metric['direction_accuracy']:.2%} | {metric['brier_up_vs_nonup']:.5f} | {metric['mean_net_bid_ask_pips']:.3f} |")
body = '''# Independent sanity check of saved EURUSD four-family predictions

The previously reported direction percentages are arithmetically correct. Independent recomputation starts from each retained `forecast_json` and its outcome quotes, verifies all 1,628 payload hashes, and uses exact decimal price arithmetic. It does not import the prior scorer or access a runtime database. Every compared prior metric agrees within 1e-9; decimal and floating-point arithmetic assign the same direction label to all 368 paired outcomes. Stored theoretical quote returns also match.

| Model | Correct direction | Accuracy | Brier, lower is better | Mean archived quote-net pips |
|---|---:|---:|---:|---:|
'''+'\n'.join(rows)+'''

These are 368 matched archived EURUSD endpoints from six UTC days, with 172 up moves, 195 down moves and one flat. A constant sell direction would describe 195/368 correct (52.99%); constant 50% probability has Brier 0.25. The zero-move magnitude baseline has MAE 3.934 pips. All four models have worse Brier and magnitude errors than these simple baselines on this subset, and their stored quote-net averages are negative. These comparisons do not authorize selecting a constant direction or reversing the signals.

“None accurate” is too broad. Each model correctly predicted 156–176 of the 368 outcomes. The supportable statement is that none of these four showed a useful advantage on this particular archived comparison. The results do not establish that every project model fails, or that each algorithm will underperform across all periods.

All four models supplied 407 original forecasts; no family is missing from this selected forecast population. Missing outcome rows number 21 for graph, 21 for tabular, 21 for state space and 22 for ridge. Matching excludes 22 reference epochs with at least one missing outcome and 17 with different endpoint clocks or quotes, leaving 368. Missing models elsewhere in the project require the separate broader inventory.

Direction and probability are distinct outputs for the tabular model: its expected-return direction differs from the majority probability direction on 74 of these paired forecasts. The reported 47.55% preserves its emitted direction. Thresholding probability at 0.5 gives 161/368, or 43.75%, so changing that interpretation does not rescue this result. The other three have no such disagreements. There are no exact probability ties or zero expected-return forecasts. The one flat is a direction miss and a non-up Brier label; removing it leaves all four accuracies below 50%.

The archive cannot establish executable prediction performance. All reference quotes predate forecast recording, with median lag 84.63 seconds and maximum 10,695.93 seconds across the full 1,628 forecasts. The paired set mixes 353 canonical and 15 legacy endpoint epochs. The retained clocks and shifted targets are reviewed separately in `independent_timing_diagnostic.json`. Overlapping forecasts and six days of data do not supply 368 independent trials.

The models are not all moving together. Graph and ridge agree in direction on 269/368 (73.10%) and have probability correlation 0.617. Other direction agreements range from 40.22% to 48.10%; their probability correlations range from -0.212 to 0.083. At least one model is correct on 337/368 epochs, but knowing afterward which one was correct is not an available ensemble rule. The pairwise statistics are descriptive and no ensemble was fitted or promoted.

Evidence is preserved in `independent_four_family_results.json`, `independently_paired_endpoints.json`, `independently_excluded_references.json`, and the two standalone scripts in this directory. Production source, configuration, databases and runtime were not changed.
'''
with (OUT/'INDEPENDENT_FOUR_FAMILY_SANITY_20260906.md').open('x',encoding='utf-8') as handle:
    handle.write(body)
receipt = {'source_result_sha256':hashlib.sha256((OUT/'independent_four_family_results.json').read_bytes()).hexdigest(),
           'files':{path.name:hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(OUT.iterdir()) if path.is_file()}}
with (OUT/'INDEPENDENT_SANITY_RECEIPT.json').open('x',encoding='utf-8') as handle:
    json.dump(receipt,handle,indent=2)
    handle.write('\n')
print(json.dumps(timing,indent=2))
