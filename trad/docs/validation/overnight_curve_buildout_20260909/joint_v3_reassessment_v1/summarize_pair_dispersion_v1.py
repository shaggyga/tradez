"""All-pair aggregation of retained scorer values, without rerunning the scorer."""
from collections import Counter
from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext
import hashlib
import json
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
INPUT = HERE / 'actual_assessment_002/V3_COMPLETED_PERFORMANCE_20260909.json'
INPUT_SHA = '119164bcf43eadbb841b821a9479a51c5305b43a8121477a3016c47d2750c77e'
FAMILY = 'ridge_price_news_v1'
CTX = Context(prec=80, rounding=ROUND_HALF_EVEN)


def need(ok, reason):
    if not ok:
        raise ValueError(reason)


def read(path, expected):
    need(path.is_file() and not path.is_symlink() and path.stat().st_size <= 16 * 1024 * 1024, 'retained_file_bound')
    raw = path.read_bytes()
    need(hashlib.sha256(raw).hexdigest() == expected, 'retained_result_changed')
    return json.loads(raw)


def aggregate(rows):
    scores = [r['scores'][FAMILY] for r in rows]
    for score in scores:
        need(type(score['direction_correct']) is bool and type(score['positive_after_spread']) is bool, 'original_boolean_required')
        need(score['side'] in (-1, 1) and Decimal(score['net_bps']).is_finite(), 'original_directional_score_required')
    count = len(scores)
    with localcontext(CTX):
        return dict(outcomes=count, direction_hits=sum(s['direction_correct'] for s in scores),
            positive_after_spread=sum(s['positive_after_spread'] for s in scores),
            mean_net_bps=str(sum((Decimal(s['net_bps']) for s in scores), Decimal(0)) / count) if count else None)


def main():
    started = time.time()
    own = Path(__file__).read_bytes()
    output = HERE / 'pair_dispersion_001'
    need(not output.exists(), 'fresh_output_required')
    original = read(INPUT, INPUT_SHA)
    need(original['status'] == 'passed' and len(original['ledger_results']) == 68, 'complete_original_assessment_required')
    pairs = []
    all_rows = []
    sources = []
    seen_pairs = set()
    for record in original['ledger_results']:
        path = Path(record['path']).absolute()
        need(path.parent == INPUT.parent / 'results' and '..' not in path.parts, 'original_result_scope')
        result = read(path, record['sha256'])
        pair = result['instrument']
        need(pair == record['instrument'] and pair not in seen_pairs and result['status'] == 'passed', 'original_pair_identity')
        seen_pairs.add(pair)
        rows = result['scored_rows']
        need(len(rows) == result['counts']['outcomes'] and not result['verification_errors'], 'original_pair_reconciliation')
        summary = aggregate(rows)
        pairs.append(dict(instrument=pair, **summary,
            published=result['counts']['publication'], exclusions=dict(Counter(r['reason'] for r in result['exclusions']))))
        all_rows.extend(rows)
        sources.append(dict(path=str(path), sha256=record['sha256'], bytes=path.stat().st_size))
    combined = aggregate(all_rows)
    target = original['metrics'][FAMILY]
    need(combined['outcomes'] == target['valid_completed_decisions'] == original['counts']['outcomes']
        and combined['direction_hits'] == target['direction_hits']
        and combined['positive_after_spread'] == target['positive_after_spread']
        and Decimal(combined['mean_net_bps']) == Decimal(target['mean_net_bps_per_valid_decision']), 'global_score_reconciliation')
    for source in sources:
        read(Path(source['path']), source['sha256'])
    read(INPUT, INPUT_SHA)
    need(Path(__file__).read_bytes() == own, 'summary_helper_changed')
    completed = time.time()
    need(0 < started <= completed, 'summary_clock_order')
    value = dict(schema_version='all_pair_retained_score_dispersion_v1_20260909', status='passed',
        started_epoch=started, completed_epoch=completed, helper_sha256=hashlib.sha256(own).hexdigest(),
        original_report=dict(path=str(INPUT), sha256=INPUT_SHA), result_sources=sources,
        original_snapshot_start_utc=original['snapshot_start_utc'], original_snapshot_end_utc=original['snapshot_end_utc'],
        original_dependence=original['dependence'], global_reconciliation=combined,
        pairs=sorted(pairs, key=lambda p: p['instrument']),
        positive_mean_net_pairs=sum(p['mean_net_bps'] is not None and Decimal(p['mean_net_bps']) > 0 for p in pairs),
        negative_mean_net_pairs=sum(p['mean_net_bps'] is not None and Decimal(p['mean_net_bps']) < 0 for p in pairs),
        zero_mean_net_pairs=sum(p['mean_net_bps'] is not None and Decimal(p['mean_net_bps']) == 0 for p in pairs),
        unscored_pairs=sum(p['outcomes'] == 0 for p in pairs),
        limits=['All 68 registered pairs retained in alphabetical order; no post-hoc trading subset selected.',
            'Only original per-row scored values were aggregated with the original 80-digit arithmetic; no scorer or raw-input replay.',
            'These cumulative dependent observations do not establish pair-specific skill, independent replication or future results.'],
        model_or_runtime_changes=False, database_reads=0, network_requests=0)
    output.mkdir()
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    path = output / 'ALL_PAIR_SCORE_DISPERSION_20260909.json'
    with path.open('xb') as handle:
        handle.write(raw)
    need(path.read_bytes() == raw, 'summary_output_readback')
    print(json.dumps(dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest(),
        positive_mean_net_pairs=value['positive_mean_net_pairs'], negative_mean_net_pairs=value['negative_mean_net_pairs'],
        unscored_pairs=value['unscored_pairs'], global_reconciliation=combined)))


if __name__ == '__main__':
    main()
