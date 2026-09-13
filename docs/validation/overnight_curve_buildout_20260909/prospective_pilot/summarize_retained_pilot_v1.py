"""Render every retained pilot cell without selecting models or rescoring outcomes."""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import time

REGISTRY_SHA = 'ae64f2cae6df44dad81b46deb16e95e4d5f67dabb7fc1e65289b96ba6b0fe2b2'
PAIRS = ('EUR_USD', 'GBP_USD', 'USD_JPY')
VIEWS = ('retained_training_target', 'nominal_exact')
CONVENTIONS = ('official_midpoint', 'ba_derived_midpoint')
HORIZONS = (15, 30, 60, 120, 180, 300, 600, 900, 1800, 3600, 7200, 10800, 14400)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def number(value, places=4):
    if value is None:
        return '—'
    converted = Decimal(str(value))
    require(converted.is_finite(), 'nonfinite_metric')
    return f'{converted:.{places}f}'


def percentage(value):
    return '—' if value is None else number(Decimal(str(value))*100, 2)+'%'


def cell_key(row):
    pair = row['forecast_cohort'].split('/')[-2]
    return (row['view'], row['price_convention'], pair, row['horizon_sec'])


def summarize(path, expected):
    require(path.stat().st_size <= 16*1024*1024, 'report_bound')
    raw = path.read_bytes()
    require(sha(raw) == expected, 'input_report_hash')
    report = json.loads(raw, parse_constant=lambda value: (_ for _ in ()).throw(ValueError('nonfinite_json')))
    require(report['registry_sha256'] == REGISTRY_SHA and report['status'] == 'completed'
            and report['issues'] == [] and report['source_closure_unchanged'] is True, 'accepted_report_required')
    require(report['research_only'] is True and all(report[k] is False for k in
        ('can_place_orders', 'can_promote', 'can_authorize', 'proof_eligible', 'account_eligible', 'execution_eligible')),
        'report_authority')
    groups = report['horizon_summary']['groups']
    indexed = {cell_key(row): row for row in groups}
    expected_keys = {(v,c,p,h) for v in VIEWS for c in CONVENTIONS for p in PAIRS for h in HORIZONS}
    require(len(groups) == len(indexed) == 156 and set(indexed) == expected_keys, 'complete_registered_grid')
    for row in groups:
        require(type(row['scored_nodes']) is int and 0 <= row['direction_denominator'] <= row['scored_nodes']
            and 0 <= row['brier_denominator'] <= row['scored_nodes']
            and 0 <= row['executable_non_neutral_denominator'] <= row['scored_nodes'], 'cell_denominators')
    descriptions = []
    for view in VIEWS:
        for convention in CONVENTIONS:
            rows = [indexed[(view,convention,p,h)] for p in PAIRS for h in HORIZONS]
            scored = [r for r in rows if r['scored_nodes'] > 0]
            net = [r for r in scored if r['mean_original_side_net_bps'] is not None]
            descriptions.append(dict(view=view, price_convention=convention, registered_cells=len(rows),
                cells_with_scored_nodes=len(scored), cells_with_non_neutral_net_metric=len(net),
                cells_with_positive_mean_net=sum(Decimal(r['mean_original_side_net_bps']) > 0 for r in net),
                cells_with_lower_mae_than_zero=sum(Decimal(r['mae_bps']) < Decimal(r['zero_baseline_mae_bps']) for r in scored),
                scope='Cell descriptors only; neither independent trials nor a pooled forecast accuracy.'))
    body = dict(schema_version='retained_pilot_complete_grid_presentation_v1_20260909',
        generated_epoch=time.time(), source_report=dict(path=str(path), bytes=len(raw), sha256=expected),
        original_evaluation_started_epoch=report['started_epoch'], original_evaluation_completed_epoch=report['completed_epoch'],
        original_registry_sha256=report['registry_sha256'], original_source_bindings=report['source_bindings'],
        helper_sha256=sha(Path(__file__).read_bytes()), curve_chain_status_counts=report['curve_chain_status_counts'],
        source_capture_status_counts=report['source_capture_status_counts'], view_dispositions=report['view_dispositions'],
        matched_comparison_status_counts=report['matched_comparison_status_counts'],
        original_horizon_summary_metadata={k:v for k,v in report['horizon_summary'].items() if k not in ('groups','source_report_hashes')},
        complete_original_groups=[indexed[key] for key in sorted(indexed)], cell_descriptions=descriptions,
        independent_sample_size=None, model_selection=False, rescoring_performed=False, runtime_writes=False,
        research_only=True, can_place_orders=False, can_promote=False)
    lines = ['# Complete retained pilot horizon results', '',
        'This is a presentation of every original cell in one verified evaluation. No model, pair, horizon or midpoint convention is selected from its return.', '',
        f'Original evaluation: {datetime.fromtimestamp(report["started_epoch"],timezone.utc).isoformat()} to {datetime.fromtimestamp(report["completed_epoch"],timezone.utc).isoformat()}.',
        f'Input report SHA256: `{expected}`. Exact metrics, counts and reasons are retained in the companion JSON.', '',
        'The retained-training-target view follows the recovered model’s actual training endpoint convention. Nominal exact endpoints are a separate diagnostic; their probability event mismatch counts remain explicit. Neither view is a new calibrated probability.', '',
        'Each N below is that cell’s scored node count. Nearby origins, horizons, midpoint conventions and currency pairs overlap; their counts cannot be added as independent trials. Independent sample size is unknown. Direction excludes neutral predictions and zero outcomes under the original evaluator; net uses its own executable non-neutral denominator. A dash is unavailable, not zero. Net bps uses original bid/ask endpoints and the registered cost convention, not broker fills or a managed account return.', '']
    for view in VIEWS:
        for convention in CONVENTIONS:
            lines += [f'## {view} / {convention}', '',
                '| Pair | Horizon | Scored N | Direction correct/N | Direction | MAE / zero, bps | Mean net, bps | Positive net/N | Brier / N | Neutral | Unavailable | Event mismatch |',
                '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
            for pair in PAIRS:
                for horizon in HORIZONS:
                    r = indexed[(view,convention,pair,horizon)]
                    horizon_text = str(horizon)+'s' if horizon < 60 else str(horizon//60)+'m'
                    lines.append(f'| {pair} | {horizon_text} | {r["scored_nodes"]} | {r["direction_correct"]}/{r["direction_denominator"]} | {percentage(r["direction_accuracy"])} | {number(r["mae_bps"])} / {number(r["zero_baseline_mae_bps"])} | {number(r["mean_original_side_net_bps"])} | {r["positive_original_side_count"]}/{r["executable_non_neutral_denominator"]} | {number(r["mean_original_probability_brier"],6)} / {r["brier_denominator"]} | {r["neutral_predictions"]} | {r["unavailable_nodes"]} | {r["probability_event_mismatch_count"]} |')
            lines.append('')
    lines += ['## Limitations', '',
        'An apparently favorable cell is exploratory evidence from the same short, overlapping collection period. The table does not establish an accepted trading strategy. Zero-change MAE and fair-coin Brier 0.25 are distinct baselines; no-trade has zero modeled trading P&L. Pending, coverage failures, provider omissions and withholding remain in the source dispositions and per-cell reasons. No missing outcome is inserted as a loss, win or zero return.', '']
    require(path.read_bytes() == raw, 'report_changed_during_presentation')
    return body, '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--output-directory', type=Path, required=True)
    args = parser.parse_args()
    body, markdown = summarize(args.report.resolve(), args.expected_sha256)
    output = args.output_directory.resolve()
    require(output.parent == Path(__file__).resolve().parent, 'external_pilot_child_directory_required')
    output.mkdir(exist_ok=False)
    raw = (json.dumps(body, indent=2, sort_keys=True, allow_nan=False)+'\n').encode()
    (output/'COMPLETE_PILOT_HORIZON_RESULTS_20260909.json').write_bytes(raw)
    (output/'COMPLETE_PILOT_HORIZON_RESULTS_20260909.md').write_text(markdown,encoding='utf-8')
    print(json.dumps(dict(output=str(output), sha256=sha(raw), groups=len(body['complete_original_groups']),
        cell_descriptions=body['cell_descriptions'])))


if __name__ == '__main__':
    main()
