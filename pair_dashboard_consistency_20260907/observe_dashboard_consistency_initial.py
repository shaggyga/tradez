"""Bounded read-only dashboard/reader observation across publication cycles.

Every unavailable or failed read is retained with its actual clocks and reason.
No retries at the detector layer hide failures. Reader-internal retry diagnostics
are retained verbatim. This script never reloads a worker or edits runtime state.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import sys
import time
import urllib.request

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / 'trad'
DATA = ROOT / 'data/oanda_training_manager'
STUDY = DATA / 'pair_local_forecast_study_v1'
API = 'http://127.0.0.1:8765/api/main'
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def bounded_read(path, limit):
    with path.open('rb') as handle:
        raw = handle.read(limit + 1)
    if len(raw) > limit:
        raise ValueError('observation_byte_limit')
    return raw


def generation_observation():
    result = {'begun_epoch': time.time()}
    for filename, limit in (('summary.json', 512 * 1024), ('heartbeat.json', 65536)):
        observed = time.time()
        try:
            raw = bounded_read(STUDY / filename, limit)
            value = json.loads(raw)
            result[filename] = {key: value.get(key) for key in
                ('schema_version', 'generated_epoch', 'registry_sha256', 'summary_sha256', 'payload_sha256',
                 'counts', 'errors', 'last_error', 'heartbeat_publication_errors', 'pair_count', 'phase')}
            result[filename].update(file_sha256=sha(raw), read_started_epoch=observed, read_finished_epoch=time.time())
        except Exception as exc:
            result[filename] = {'error': type(exc).__name__ + ':' + str(exc),
                'read_started_epoch': observed, 'read_finished_epoch': time.time()}
    result['completed_epoch'] = time.time()
    return result


def coverage_view(value):
    if not isinstance(value, dict):
        raise ValueError('missing_pair_local_forecasts_object')
    # Retain all top-level retry diagnostics from the patched reader without
    # assuming their exact key names. Keep each unavailable row's own reason.
    result = {key: item for key, item in value.items() if key != 'rows'}
    rows = value.get('rows', [])
    result['row_count'] = len(rows)
    result['row_reasons'] = {row.get('instrument'): {'status': row.get('status'),
        'reason': row.get('reason'), 'observed_epoch': row.get('observed_epoch')}
        for row in rows}
    return result


def direct_read(reader):
    start = time.time()
    timer = time.perf_counter()
    try:
        result = {'coverage': coverage_view(reader(DATA))}
    except Exception as exc:
        result = {'error': type(exc).__name__ + ':' + str(exc)}
    return {**result, 'started_epoch': start, 'finished_epoch': time.time(),
        'duration_sec': time.perf_counter() - timer}


def api_read():
    start = time.time()
    timer = time.perf_counter()
    try:
        request = urllib.request.Request(API, method='GET', headers={'Accept': 'application/json'})
        with urllib.request.urlopen(request, timeout=4) as response:
            raw = response.read(16 * 1024 * 1024 + 1)
            status = response.status
        if len(raw) > 16 * 1024 * 1024:
            raise ValueError('api_observation_byte_limit')
        payload = json.loads(raw)
        result = {'http_status': status, 'body_sha256': sha(raw),
            'coverage': coverage_view(payload.get('pair_local_forecasts'))}
    except Exception as exc:
        result = {'error': type(exc).__name__ + ':' + str(exc)}
    return {**result, 'started_epoch': start, 'finished_epoch': time.time(),
        'duration_sec': time.perf_counter() - timer}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duration-sec', type=float, default=35)
    parser.add_argument('--interval-sec', type=float, default=.35)
    parser.add_argument('--output', type=Path, default=OUT / 'DASHBOARD_API_CONSISTENCY_PROBE_20260907.json')
    args = parser.parse_args()
    if not 30 <= args.duration_sec <= 50 or not .1 <= args.interval_sec <= 1:
        raise ValueError('bounded_observation_window_required')
    if args.output.exists():
        raise ValueError('refuse_to_replace_observation_evidence')
    import oanda_practice_live_dashboard as dashboard
    registry_raw = bounded_read(ROOT / 'config/pair_local_forecast_study_v1_20260907.json', 1024 * 1024)
    registry = json.loads(registry_raw)
    bindings = registry['source_bindings']
    before = {name: sha((ROOT / name).read_bytes()) for name in bindings}
    if before != bindings:
        raise ValueError('frozen_study_sources_changed_before_observation')
    reader_before = sha((ROOT / 'oanda_practice_live_dashboard.py').read_bytes())
    started, timer = time.time(), time.monotonic()
    samples = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        while time.monotonic() - timer < args.duration_sec:
            iteration = time.monotonic()
            reader_future = pool.submit(direct_read, dashboard.summarize_pair_local_forecasts)
            api_future = pool.submit(api_read)
            files = generation_observation()
            samples.append({'sample': len(samples), 'file_observation': files,
                'direct_reader': reader_future.result(), 'api': api_future.result()})
            delay = min(args.interval_sec - (time.monotonic() - iteration), args.duration_sec - (time.monotonic() - timer))
            if delay > 0:
                time.sleep(delay)
    completed = time.time()
    after = {name: sha((ROOT / name).read_bytes()) for name in bindings}
    reader_after = sha((ROOT / 'oanda_practice_live_dashboard.py').read_bytes())
    failures = []
    for sample in samples:
        for lane in ('direct_reader', 'api'):
            read = sample[lane]
            coverage = read.get('coverage', {})
            invalid_rows = [pair for pair, row in coverage.get('row_reasons', {}).items()
                if row.get('reason') == 'invalid_pair_publication']
            if 'error' in read or coverage.get('status') != 'current' or invalid_rows:
                failures.append({'sample': sample['sample'], 'lane': lane,
                    'started_epoch': read['started_epoch'], 'finished_epoch': read['finished_epoch'],
                    'status': coverage.get('status'),
                    'reason': ('invalid_pair_publication:' + ','.join(invalid_rows)) if invalid_rows else read.get('error', coverage.get('reason')),
                    'coverage': coverage})
    generations = sorted({sample['file_observation'].get('summary.json', {}).get('generated_epoch')
        for sample in samples} - {None})
    api_times = sorted(sample['api']['duration_sec'] for sample in samples)
    api_unavailable = sum(sample['api'].get('coverage', {}).get('status') != 'current' for sample in samples)
    reader_unavailable = sum(sample['direct_reader'].get('coverage', {}).get('status') != 'current' for sample in samples)
    heartbeat_errors = [sample['sample'] for sample in samples if (
        sample['file_observation'].get('heartbeat.json', {}).get('errors') != 0
        or sample['file_observation'].get('heartbeat.json', {}).get('heartbeat_publication_errors') != 0
        or sample['file_observation'].get('heartbeat.json', {}).get('phase') != 'research_collection')]
    retained_retries = {lane: sum(len(sample[lane].get('coverage', {}).get('publication_read_failures', []))
        for sample in samples) for lane in ('direct_reader', 'api')}
    report = {'schema_version': 'pair_dashboard_consistency_observation_v1_20260907',
        'started_epoch': started, 'completed_epoch': completed,
        'completed_utc': datetime.fromtimestamp(completed, timezone.utc).isoformat(),
        'duration_sec': time.monotonic() - timer, 'sample_count': len(samples),
        'read_count': len(samples) * 2, 'failure_count': len(failures), 'failures': failures,
        'api_unavailable_count': api_unavailable, 'direct_reader_unavailable_count': reader_unavailable,
        'api_latency_median_sec': statistics.median(api_times),
        'api_latency_p95_sec': api_times[max(0, (len(api_times) * 95 + 99) // 100 - 1)],
        'heartbeat_error_samples': heartbeat_errors,
        'reader_internal_failed_attempts_retained': retained_retries,
        'failure_reasons': dict(Counter(row['reason'] for row in failures)),
        'summary_generations_observed': generations,
        'summary_generation_transitions': max(0, len(generations) - 1),
        'frozen_source_bindings_unchanged': before == after == bindings,
        'frozen_source_bindings_before': before, 'frozen_source_bindings_after': after,
        'reader_source_unchanged_during_observation': reader_before == reader_after,
        'reader_source_sha256': reader_after, 'registry_file_sha256': sha(registry_raw),
        'status': 'passed' if (not failures and not heartbeat_errors and len(generations) >= 3
            and before == after == bindings and reader_before == reader_after) else 'observed_failure_or_incomplete_coverage',
        'samples': samples, 'can_place_orders': False, 'runtime_reloaded': False,
        'limitations': ['Finite observation window; this does not prove all future reads will succeed.',
            'Detector retries do not hide unavailable reads. Every direct-reader and API result is retained.',
            'Independent file reads can straddle publication boundaries; they are generation diagnostics, not a coherent-pair assertion.',
            'Current coverage is publication availability, not predictive accuracy.'],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as handle:
        json.dump(report, handle, indent=2, allow_nan=False)
        handle.write('\n')
    print(json.dumps({key: report[key] for key in ('status', 'sample_count', 'read_count', 'duration_sec',
        'failure_count', 'failure_reasons', 'summary_generation_transitions', 'frozen_source_bindings_unchanged',
        'api_unavailable_count', 'api_latency_median_sec', 'api_latency_p95_sec', 'heartbeat_error_samples')}))


if __name__ == '__main__':
    main()
