"""Read actual bounded archives; compute diagnostics without issuing forecasts."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / 'trad'
sys.path.insert(0, str(ROOT))
from oanda_causal_forecast_inputs_pair_v1 import capture_inputs, compute_predictions, coverage_probe


def main():
    preflight_raw = (OUT / 'COVERAGE_PREFLIGHT.json').read_bytes()
    quote_raw = (OUT / 'QUOTE_METADATA_SNAPSHOT.json').read_bytes()
    preflight = json.loads(preflight_raw)
    snapshot = json.loads(quote_raw)
    assert hashlib.sha256(quote_raw).hexdigest() == preflight['source_quote_sha256']
    assert snapshot['producer'] == 'practice_007_dedicated_quote_stream'
    pips = preflight['pip_sizes']
    assert len(pips) == 68 and set(pips) == set(snapshot['quotes'])
    assert all(pips[pair] == snapshot['quotes'][pair]['pip'] for pair in pips)
    started, timer = time.time(), time.perf_counter()
    coverage = coverage_probe(ROOT / 'data/oanda_training_manager/candles', sorted(pips), pip_sizes=pips)
    rows = []
    for pair, pip in sorted(pips.items()):
        capture = capture_inputs(ROOT / 'data/oanda_training_manager/candles', pair, pip_size=pip)
        result = compute_predictions(capture)
        source = capture.get('sources', {}).get(pair, {})
        rows.append({
            'instrument': pair, 'pip_size': pip,
            'status': 'computed_not_issued' if result['status'] == 'ready' else 'abstain',
            'reasons': capture.get('reasons') or result.get('reasons'),
            'current_pair_bars': capture.get('current_common_bars'),
            'retained_real_rows': capture.get('retained_real_rows_by_pair', {}).get(pair),
            'reference_start_epoch': capture.get('reference_start_epoch'),
            'source_capture_sha256': capture['source_capture_sha256'],
            'captured_bytes_sha256': source.get('captured_bytes_sha256'),
            'source_observed_epoch': source.get('first_observed_epoch'),
            'capture_observed_epoch': capture.get('first_observed_epoch'),
            'computed_epoch': result.get('computed_epoch'),
            'model_source_sha256': result['model_source_sha256'],
            'predictions': result['predictions'],
            'issued_epoch': None, 'published_epoch': None,
            'research_only': True, 'can_place_orders': False,
        })
    finished = time.time()
    ready = [row['instrument'] for row in rows if row['status'] == 'computed_not_issued']
    report = {
        'schema_version': 'live_pair_compute_preflight_v1_20260907',
        'started_epoch': started, 'completed_epoch': finished,
        'completed_utc': datetime.fromtimestamp(finished, timezone.utc).isoformat(),
        'total_duration_sec': time.perf_counter() - timer,
        'metadata_preflight_sha256': hashlib.sha256(preflight_raw).hexdigest(),
        'metadata_quote_snapshot_sha256': hashlib.sha256(quote_raw).hexdigest(),
        'coverage_probe': coverage, 'pair_count': len(rows),
        'computed_not_issued_pair_count': len(ready), 'computed_not_issued_pairs': ready,
        'abstention_reasons': dict(Counter(reason for row in rows if row['status'] == 'abstain' for reason in row['reasons'])),
        'rows': rows,
        'semantics': 'Read-only actual local archive eligibility and model computation; no forecast issuance, publication, scoring, study activation, runtime changes or broker operations.',
        'accuracy_measured': False, 'runtime_started': False,
        'research_only': True, 'can_place_orders': False, 'can_promote': False,
    }
    destination = OUT / 'LIVE_PAIR_COMPUTE_PREFLIGHT.json'
    with destination.open('x', encoding='utf-8') as handle:
        json.dump(report, handle, indent=2, allow_nan=False)
        handle.write('\n')
    print(json.dumps({'coverage_ready_count': coverage['ready_pair_count'],
        'computed_not_issued_count': len(ready), 'computed_not_issued_pairs': ready,
        'total_duration_sec': report['total_duration_sec'], 'destination': str(destination)}))


if __name__ == '__main__':
    main()
