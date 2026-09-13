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


def canonical_hash(value):
    return sha(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode())


def row_clocks(row):
    value = {'observed_epoch': row.get('observed_epoch'), 'last_attempt_epoch': row.get('last_attempt_epoch')}
    forecast = row.get('latest_published_forecast')
    if isinstance(forecast, dict):
        value['forecast'] = {key: forecast.get(key) for key in ('decision_id', 'publication_epoch',
            'reference_available_epoch', 'target_epoch', 'forecast_sha256')}
        value['forecast']['arms'] = {arm['family']: {key: arm.get(key) for key in
            ('cohort_id', 'instrument', 'reference_epoch', 'reference_mid', 'issued_epoch', 'target_epoch')}
            for arm in forecast.get('forecasts', [])}
    return value


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
            if filename == 'summary.json':
                result[filename]['canonical_summary_sha256'] = canonical_hash(value)
                result[filename]['row_clocks'] = {row['instrument']: row_clocks(row) for row in value.get('rows', [])}
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
        'reason': row.get('reason'), 'observed_epoch': row.get('observed_epoch'), 'original_clocks': row_clocks(row)}
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


def api_read(timeout=4):
    start = time.time()
    timer = time.perf_counter()
    try:
        request = urllib.request.Request(API, method='GET', headers={'Accept': 'application/json'})
        with urllib.request.urlopen(request, timeout=timeout) as response:
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


def retained_provenance_checks(samples):
    summaries = {}
    heartbeats = set()
    for sample in samples:
        files = sample['file_observation']
        summary = files.get('summary.json', {})
        heartbeat = files.get('heartbeat.json', {})
        if summary.get('canonical_summary_sha256'):
            summaries.setdefault(summary['canonical_summary_sha256'], summary)
        if heartbeat.get('generated_epoch') is not None:
            heartbeats.add((heartbeat['generated_epoch'], heartbeat.get('summary_sha256')))
    checks = []
    for sample in samples:
        for lane in ('direct_reader', 'api'):
            read = sample[lane]
            view = read.get('coverage', {})
            retained = view.get('retained_generation')
            if not retained:
                continue
            errors = []
            selected_sha = retained.get('summary_sha256')
            known = summaries.get(selected_sha)
            if selected_sha != view.get('summary_sha256') or selected_sha != retained.get('heartbeat_summary_sha256'):
                errors.append('retained_summary_heartbeat_hash_disagreement')
            if (retained.get('heartbeat_generated_epoch'), retained.get('heartbeat_summary_sha256')) not in heartbeats:
                errors.append('retained_heartbeat_generation_not_independently_observed')
            if retained.get('generated_epoch') != view.get('generated_epoch') or retained.get('observed_epoch') != view.get('observed_epoch'):
                errors.append('retained_summary_clock_disagreement')
            if known is None:
                errors.append('retained_summary_bytes_not_independently_observed')
            else:
                if known.get('generated_epoch') != retained.get('generated_epoch'):
                    errors.append('retained_generation_clock_changed')
                for pair, row in view.get('row_reasons', {}).items():
                    expected = known.get('row_clocks', {}).get(pair)
                    actual = row.get('original_clocks', {})
                    if expected is None or any(expected.get(key) != actual.get(key) for key in ('observed_epoch', 'last_attempt_epoch')):
                        errors.append('retained_row_clock_changed:' + pair)
                    elif actual.get('forecast') is not None and actual['forecast'] != expected.get('forecast'):
                        errors.append('retained_original_forecast_clock_or_anchor_changed:' + pair)
            checks.append({'sample': sample['sample'], 'phase': sample['phase'], 'lane': lane,
                'summary_sha256': selected_sha, 'generated_epoch': retained.get('generated_epoch'),
                'heartbeat_generated_epoch': retained.get('heartbeat_generated_epoch'),
                'heartbeat_summary_sha256': retained.get('heartbeat_summary_sha256'),
                'observed_epoch': retained.get('observed_epoch'), 'errors': errors,
                'status': 'verified' if not errors else 'unverified_or_mismatched'})
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duration-sec', type=float, default=50)
    parser.add_argument('--interval-sec', type=float, default=.35)
    parser.add_argument('--output', type=Path, default=OUT / 'DASHBOARD_API_CONSISTENCY_PROBE_RETAINED_20260907.json')
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
    primed = False
    steady_started = None
    steady_elapsed = 0
    with ThreadPoolExecutor(max_workers=2) as pool:
        known_cold_summaries = set()
        def take_sample(phase, timeout=4):
            iteration = time.monotonic()
            reader_future = pool.submit(direct_read, dashboard.summarize_pair_local_forecasts)
            api_future = pool.submit(api_read, timeout)
            files = generation_observation()
            sample = {'sample': len(samples), 'phase': phase, 'file_observation': files,
                'direct_reader': reader_future.result(), 'api': api_future.result()}
            samples.append(sample)
            return sample, iteration

        # A new process has no retained verified bytes. Preserve every cold read,
        # including a legitimate unavailable result during a generation handoff.
        while time.monotonic() - timer < 6:
            remaining = 6 - (time.monotonic() - timer)
            sample, iteration = take_sample('cold_start', min(4, max(.05, remaining)))
            summary_sha = sample['file_observation'].get('summary.json', {}).get('canonical_summary_sha256')
            if summary_sha:
                known_cold_summaries.add(summary_sha)
            sample['priming_provenance_ready'] = all(sample[lane].get('coverage', {}).get('summary_sha256') in known_cold_summaries
                for lane in ('direct_reader', 'api'))
            if sample['priming_provenance_ready'] and all(sample[lane].get('coverage', {}).get('status') == 'current' for lane in ('direct_reader', 'api')):
                primed = True
                break
            delay = min(args.interval_sec - (time.monotonic() - iteration), 6 - (time.monotonic() - timer))
            if delay > 0:
                time.sleep(delay)
        cold_elapsed = time.monotonic() - timer
        if primed:
            steady_started = time.time()
            steady_timer = time.monotonic()
            while time.monotonic() - steady_timer < args.duration_sec:
                _, iteration = take_sample('steady')
                delay = min(args.interval_sec - (time.monotonic() - iteration), args.duration_sec - (time.monotonic() - steady_timer))
                if delay > 0:
                    time.sleep(delay)
            steady_elapsed = time.monotonic() - steady_timer
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
                    'phase': sample['phase'],
                    'started_epoch': read['started_epoch'], 'finished_epoch': read['finished_epoch'],
                    'status': coverage.get('status'),
                    'reason': ('invalid_pair_publication:' + ','.join(invalid_rows)) if invalid_rows else read.get('error', coverage.get('reason')),
                    'coverage': coverage})
    steady_samples = [sample for sample in samples if sample['phase'] == 'steady']
    steady_failures = [failure for failure in failures if failure['phase'] == 'steady']
    cold_failures = [failure for failure in failures if failure['phase'] == 'cold_start']
    generations = sorted({sample['file_observation'].get('summary.json', {}).get('generated_epoch')
        for sample in steady_samples} - {None})
    api_times = sorted(sample['api']['duration_sec'] for sample in (steady_samples or samples))
    api_unavailable = sum(sample['api'].get('coverage', {}).get('status') != 'current' for sample in samples)
    reader_unavailable = sum(sample['direct_reader'].get('coverage', {}).get('status') != 'current' for sample in samples)
    heartbeat_errors = [sample['sample'] for sample in samples if (
        sample['file_observation'].get('heartbeat.json', {}).get('errors') != 0
        or sample['file_observation'].get('heartbeat.json', {}).get('heartbeat_publication_errors') != 0
        or sample['file_observation'].get('heartbeat.json', {}).get('phase') != 'research_collection')]
    retained_retries = {lane: sum(len(sample[lane].get('coverage', {}).get('publication_read_failures', []))
        for sample in samples) for lane in ('direct_reader', 'api')}
    retained_checks = retained_provenance_checks(samples)
    steady_retained = [check for check in retained_checks if check['phase'] == 'steady']
    retained_failures = [check for check in steady_retained if check['errors']]
    report = {'schema_version': 'pair_dashboard_consistency_observation_v1_20260907',
        'started_epoch': started, 'completed_epoch': completed,
        'completed_utc': datetime.fromtimestamp(completed, timezone.utc).isoformat(),
        'duration_sec': time.monotonic() - timer, 'sample_count': len(samples),
        'cold_start_coherent': primed, 'cold_start_duration_sec': cold_elapsed,
        'cold_start_sample_count': len(samples) - len(steady_samples),
        'cold_start_failure_count': len(cold_failures),
        'cold_start_failure_reasons': dict(Counter(row['reason'] for row in cold_failures)),
        'steady_started_epoch': steady_started, 'steady_duration_sec': steady_elapsed,
        'steady_sample_count': len(steady_samples), 'steady_read_count': len(steady_samples) * 2,
        'steady_failure_count': len(steady_failures),
        'steady_failure_reasons': dict(Counter(row['reason'] for row in steady_failures)),
        'read_count': len(samples) * 2, 'failure_count': len(failures), 'failures': failures,
        'api_unavailable_count': api_unavailable, 'direct_reader_unavailable_count': reader_unavailable,
        'api_latency_median_sec': statistics.median(api_times),
        'api_latency_scope': 'steady_window' if steady_samples else 'cold_start_only',
        'api_latency_p95_sec': api_times[max(0, (len(api_times) * 95 + 99) // 100 - 1)],
        'heartbeat_error_samples': heartbeat_errors,
        'reader_internal_failed_attempts_retained': retained_retries,
        'retained_generation_checks': retained_checks,
        'steady_retained_generation_read_count': len(steady_retained),
        'steady_retained_generation_failure_count': len(retained_failures),
        'steady_retained_generation_counts_by_reader': dict(Counter(check['lane'] for check in steady_retained)),
        'failure_reasons': dict(Counter(row['reason'] for row in failures)),
        'summary_generations_observed': generations,
        'summary_generation_transitions': max(0, len(generations) - 1),
        'frozen_source_bindings_unchanged': before == after == bindings,
        'frozen_source_bindings_before': before, 'frozen_source_bindings_after': after,
        'reader_source_unchanged_during_observation': reader_before == reader_after,
        'reader_source_sha256': reader_after, 'registry_file_sha256': sha(registry_raw),
        'status': 'passed' if (primed and steady_elapsed >= args.duration_sec and not steady_failures
            and not heartbeat_errors and len(generations) >= 4 and steady_retained and not retained_failures
            and before == after == bindings and reader_before == reader_after) else 'observed_failure_or_incomplete_coverage',
        'samples': samples, 'can_place_orders': False, 'runtime_reloaded': False,
        'limitations': ['Finite observation window; this does not prove all future reads will succeed.',
            'Cold-start priming lasts at most six seconds plus in-flight read overhead; all cold unavailable reads remain in this report. Only the separately identified steady window is required to be continuously current.',
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
        'api_unavailable_count', 'api_latency_median_sec', 'api_latency_p95_sec', 'heartbeat_error_samples',
        'cold_start_failure_count', 'steady_sample_count', 'steady_read_count', 'steady_failure_count', 'steady_duration_sec',
        'steady_retained_generation_read_count', 'steady_retained_generation_failure_count')}))


if __name__ == '__main__':
    main()
