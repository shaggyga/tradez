"""Summarize a completed, exact-byte-bound passive observation; no runtime reads."""
from collections import Counter
from pathlib import Path
import datetime as dt
import hashlib
import json
import math
import os
import stat
import time

BASE = Path(__file__).resolve().parent
RUN = BASE / 'actual_observation_001'
SOURCE_SHA = 'dc63f1470b6351e774af01c0732de0cc84ac64e04de0b1f7081870a4f643f86e'
REGISTRY_SHA = 'ee075e69e80ca56dfdf45abe1f7a1a301612176e67f7622eab1adf2bd9af8771'
END = 1788957900.0
KINDS = ('collector_heartbeat', 'clock_state', 'repaired_heartbeat')
CAP = 64 * 1024 * 1024


def need(ok, reason):
    if not ok:
        raise ValueError(reason)


def epoch(value):
    need(type(value) in (int, float) and math.isfinite(value) and value > 0, 'summary_clock')
    return value


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read_bound(path, cap=CAP):
    path = Path(path).absolute()
    need('..' not in path.parts, 'summary_path')
    for item in [path, *path.parents]:
        info = item.lstat()
        need(not stat.S_ISLNK(info.st_mode) and not getattr(info, 'st_file_attributes', 0) & 1024, 'summary_reparse')
    with path.open('rb') as stream:
        before = os.fstat(stream.fileno())
        raw = stream.read(cap + 1)
        after = os.fstat(stream.fileno())
    ident = lambda x: (x.st_dev, x.st_ino, x.st_size, x.st_mtime_ns)
    need(len(raw) <= cap and ident(before) == ident(after) == ident(path.stat()), 'summary_changed_or_bound')
    return raw


def summarize(started, result, samples, *, sample_sha, computed_epoch):
    computed = epoch(computed_epoch)
    need(started.get('schema_version') == 'news_clock_passive_observer_v2_20260909', 'summary_start_schema')
    need(result.get('schema_version') == 'news_clock_passive_observer_result_v2_20260909', 'summary_result_schema')
    need(result.get('status') == 'completed', 'summary_not_completed')
    need(started.get('observer_sha256') == result.get('observer_sha256') == SOURCE_SHA, 'summary_source')
    need(started.get('registry_sha256') == result.get('registry_sha256') == REGISTRY_SHA, 'summary_registry')
    need(result.get('all_registered_sources_unchanged') is True, 'summary_closure')
    need(started.get('GET') is False and result.get('GET') is False and started.get('runtime_writes') is False and result.get('runtime_writes') is False, 'summary_authority')
    begin = epoch(started['started_epoch']); finish = epoch(result['completed_epoch'])
    duration = epoch(result['maximum_duration_sec']); observed = epoch(result['observed_duration_sec'])
    need(duration == END - begin == started['maximum_duration_sec'], 'summary_original_duration')
    need(begin == result['started_epoch'] and begin < END and begin <= finish <= computed, 'summary_window')
    need(finish >= END or observed >= duration, 'summary_original_stop_not_reached')
    stop_basis = 'fixed_wall_deadline' if finish >= END else 'original_monotonic_budget'
    need(started['fixed_end_epoch'] == result['fixed_end_epoch'] == END, 'summary_deadline')
    need(result['sanitized_samples_sha256'] == sample_sha, 'summary_sample_hash')
    need(type(samples) is list and 0 < len(samples) == result['sample_count'] <= started['max_samples'] <= 14401, 'summary_sample_count')
    counts = Counter(); invalid = []; transitions = []; prior = None; previous_start = begin
    reasons = Counter(); unique = {kind: set() for kind in KINDS}; read_ranges = {kind: [] for kind in KINDS}
    error_values = []; run_lengths = Counter(); longest = {}; active_runs = {}; starts = []
    for index, sample in enumerate(samples):
        need(sample['index'] == index, 'summary_index')
        a = epoch(sample['started_epoch']); b = epoch(sample['completed_epoch'])
        need(previous_start <= a < END and a <= b <= finish, 'summary_sample_clock'); previous_start = a; starts.append(a)
        need(set(sample['files']) == set(KINDS), 'summary_file_inventory')
        for kind in KINDS:
            value = sample['files'][kind]
            if 'inspection' not in value:
                need(value.get('status') == 'unavailable' and a <= epoch(value['observed_epoch']) <= b, 'summary_failed_read')
                counts[kind + ':read_or_validation_failure'] += 1
                state = 'read_or_validation_failure'; reason = value.get('reason_code')
            else:
                read = value['read']; inspection = value['inspection']
                need(a <= epoch(read['read_started_epoch']) <= epoch(read['read_completed_epoch']) <= b, 'summary_read_clock')
                need(read['sha256'] == value['private_source']['sha256'], 'summary_private_hash')
                unique[kind].add(read['sha256']); read_ranges[kind].append(read['read_completed_epoch'])
                state = inspection.get('validation', {}).get('status', inspection.get('status', 'unknown'))
                counts[kind + ':' + state] += 1; reason = inspection.get('validation', {}).get('reason_code')
                for field in inspection['clock_fields']:
                    if field['status'] == 'invalid':
                        invalid.append(dict(sample=index, kind=kind, read_completed_epoch=read['read_completed_epoch'], source_sha256=read['sha256'], field=field))
                if kind == 'repaired_heartbeat' and inspection.get('errors') is not None:
                    need(type(inspection['errors']) is int and inspection['errors'] >= 0, 'summary_error_count'); error_values.append(inspection['errors'])
            if reason:
                need(type(reason) is str and len(reason) <= 160, 'summary_reason_bound'); reasons[kind + ':' + reason] += 1
            bad = state not in ('passed', 'current')
            if bad:
                previous = active_runs.get(kind)
                active_runs[kind] = (previous[0], previous[1] + 1) if previous else (a, 1)
                run_duration = b - active_runs[kind][0]
                old = longest.get(kind, {'samples': 0, 'observed_span_sec': 0})
                longest[kind] = {'samples': max(old['samples'], active_runs[kind][1]), 'observed_span_sec': max(old['observed_span_sec'], run_duration)}
            else:
                active_runs.pop(kind, None)
        value = sample['files']['repaired_heartbeat']; inspection = value.get('inspection', {})
        state = (inspection.get('status'), inspection.get('errors'), inspection.get('last_error_code'))
        if state != prior:
            transitions.append(dict(sample=index, read=value.get('read'), status=state[0], errors=state[1], last_error_code=state[2])); prior = state
    need(dict(counts) == result['counts'], 'summary_counts')
    need(invalid == result['invalid_epoch_fields'], 'summary_invalid_fields')
    need(transitions == result['producer_status_transitions'], 'summary_transitions')
    need(type(result['db_diagnostic_attempted']) is bool, 'summary_diagnostic_flag')
    diag = result.get('db_diagnostic')
    need((diag is not None) == result['db_diagnostic_attempted'], 'summary_diagnostic_presence')
    diagnostic = None if diag is None else {key: diag[key] for key in ('status', 'reason_code', 'row_count', 'payload_bytes', 'invalid_epoch_field_count', 'article_validation', 'database_read_started_epoch', 'database_read_completed_epoch', 'pure_validation_completed_epoch', 'observed_epoch', 'scope') if key in diag}
    invalid_types = Counter((item['kind'] + ':' + item['field']['path'] + ':' + item['field'].get('type', 'unknown')) for item in invalid)
    return dict(schema_version='news_clock_passive_compact_summary_v1_20260909', status='completed_observation',
        observation_started_epoch=begin, observation_completed_epoch=finish, summary_computed_epoch=computed,
        fixed_end_epoch=END, original_monotonic_budget_sec=duration, observed_monotonic_duration_sec=observed, termination_boundary=stop_basis, sample_count=len(samples), counts=dict(counts), rejection_reasons=dict(reasons),
        invalid_epoch_field_count=len(invalid), invalid_field_types=dict(invalid_types), producer_status_transitions=transitions,
        producer_error_first=error_values[0] if error_values else None, producer_error_last=error_values[-1] if error_values else None,
        producer_error_max=max(error_values) if error_values else None,
        producer_error_counter_decreases=sum(b < a for a,b in zip(error_values,error_values[1:])),
        unique_source_versions={k:len(v) for k,v in unique.items()},
        actual_read_completion_ranges={k:([min(v),max(v)] if v else None) for k,v in read_ranges.items()},
        longest_observed_noncurrent_runs=longest, maximum_sample_start_gap_sec=max((b-a for a,b in zip(starts,starts[1:])),default=0),
        db_diagnostic_attempted=result['db_diagnostic_attempted'], later_diagnostic=diagnostic,
        private_bytes=result['private_bytes'], private_versions=result['unique_private_versions'],
        all_registered_sources_unchanged=True, observer_sha256=SOURCE_SHA, registry_sha256=REGISTRY_SHA,
        sanitized_samples_sha256=sample_sha, GET=False, runtime_writes=False,
        limits=[*result['limitations'], 'Run spans describe sampled observations, not continuous outage durations; polling and bounded work may miss intervening states.',
          'Current heartbeat status is not a full current-news semantic replay, input-readiness proof, calibrated prediction, or execution authorization.',
          'Raw/private bytes and the full sample log remain local; only compact summaries and source/test evidence are selected.'])



def finalize_summary_clocks(result, *, began, completed, own_before, own_after):
    start = epoch(began); finish = epoch(completed); processing = epoch(result['summary_computed_epoch'])
    need(start <= processing <= finish, 'summary_completion_clock')
    need(type(own_before) is bytes and own_before == own_after, 'summary_own_source_changed')
    result = dict(result)
    result['summary_started_epoch'] = start
    result['summary_processing_started_epoch'] = processing
    result['summary_computed_epoch'] = finish
    result['summary_source_sha256'] = sha(own_before)
    return result


def run():
    began = epoch(time.time())
    own_before = read_bound(Path(__file__))
    source = read_bound(BASE/'observe_news_clock_boundaries_v2.py'); need(sha(source) == SOURCE_SHA, 'summary_source')
    paths = [RUN/'OBSERVATION_STARTED.json', RUN/'NEWS_CLOCK_PASSIVE_OBSERVATION_20260909.json', RUN/'sanitized_samples.jsonl']
    raws = [read_bound(p) for p in paths]; values = [json.loads(raws[0]), json.loads(raws[1])]
    samples = [json.loads(line) for line in raws[2].splitlines()]
    result = summarize(*values, samples, sample_sha=sha(raws[2]), computed_epoch=epoch(time.time()))
    need(read_bound(BASE/'observe_news_clock_boundaries_v2.py') == source, 'summary_source_changed')
    for p,raw in zip(paths,raws): need(read_bound(p) == raw, 'summary_input_changed')
    result['bound_inputs'] = [dict(path=str(p),bytes=len(raw),sha256=sha(raw)) for p,raw in zip(paths,raws)]
    own_after = read_bound(Path(__file__))
    result = finalize_summary_clocks(result, began=began, completed=epoch(time.time()), own_before=own_before, own_after=own_after)
    output = BASE/'completed_summary_002'; output.mkdir(exist_ok=False)
    p = output/'NEWS_CLOCK_V2_COMPLETED_SUMMARY_20260909.json'
    raw = (json.dumps(result,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    with p.open('xb') as stream: stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    need(p.read_bytes()==raw,'summary_output_readback')
    print(json.dumps({'status':result['status'],'path':str(p),'sha256':sha(raw),'samples':result['sample_count'],'counts':result['counts']}))


if __name__ == '__main__':
    run()