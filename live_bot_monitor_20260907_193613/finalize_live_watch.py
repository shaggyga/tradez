"""Finalize the completed observation from retained evidence only; never query runtime."""
from __future__ import annotations
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from pathlib import Path
import re
import time

ROOT = Path(__file__).resolve().parent
END = 1788813373.0
START_UTC = '2026-09-07T19:36:13Z'
END_UTC = '2026-09-07T20:36:13Z'
BASELINE_SHA256 = '53c7c40091ee47e8faa91f6d747c8c9171e10a8f6e3b6904d073fc951a5ae6eb'
FAMILIES = {'probabilistic_state_space': 'State space', 'ridge_return_repaired': 'Ridge'}
ACCOUNT_ID = re.compile(r'(?<!\d)\d{3}-\d{3}-\d{8}-\d{3}(?!\d)')


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def epoch(text):
    stamp = datetime.fromisoformat(text.replace('Z', '+00:00'))
    require(stamp.tzinfo is not None, 'timezone_required')
    return stamp.timestamp()


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def clean(value):
    if isinstance(value, str):
        return ACCOUNT_ID.sub('[redacted account identifier]', value)
    if isinstance(value, dict):
        return {clean(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    return value


def read(name, bindings, *, lines=False):
    path = ROOT / name
    require(path.resolve().parent == ROOT and path.is_file(), 'required_local_input_missing:' + name)
    before = path.stat()
    require(before.st_size <= 128 * 1024 * 1024, 'input_too_large:' + name)
    raw = path.read_bytes()
    after = path.stat()
    require((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns), 'input_changed_during_read:' + name)
    bindings[name] = {'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}
    if lines:
        require(raw.endswith(b'\n'), 'incomplete_sample_line')
        return [json.loads(line) for line in raw.splitlines() if line.strip()]
    return json.loads(raw)


def numeric(value):
    return type(value) in (int, float) and math.isfinite(value)


def span(values):
    values = [v for v in values if numeric(v)]
    return {'observations': len(values), 'first': values[0] if values else None,
            'last': values[-1] if values else None, 'min': min(values) if values else None,
            'max': max(values) if values else None}


def fmt(value, places=2):
    if value is None:
        return 'unavailable'
    try:
        number = Decimal(str(value))
        return f'{number:.{places}f}' if number.is_finite() else 'unavailable'
    except (InvalidOperation, ValueError):
        return 'unavailable'


def fmt_range(value):
    return 'unavailable' if value['observations'] == 0 else f"{fmt(value['min'], 0)}–{fmt(value['max'], 0)}"


def sample_summary(rows, interval):
    good = [r for r in rows if r['event'] == 'live_sample']
    errors = [r for r in rows if r['event'] == 'sample_error']
    require(good, 'no_successful_api_samples')
    current_pairs = [r for r in good if r['pair_summary'].get('status') == 'current']
    news_counts = Counter(r['news'].get('pair_bias_status') or 'unknown' for r in good)
    runs, run = [], []
    for row in rows:
        stale = row['event'] == 'live_sample' and row['news'].get('pair_bias_status') == 'stale'
        if not stale or (run and row['observed_epoch'] - run[-1]['observed_epoch'] > interval * 1.5):
            if run:
                runs.append(run)
            run = []
        if stale:
            run.append(row)
    if run:
        runs.append(run)
    longest = max(runs, key=lambda v: (v[-1]['observed_epoch'] - v[0]['observed_epoch'], len(v)), default=[])
    latest = good[-1]
    missing = {}
    for row in latest['pair_readiness']:
        if row.get('status') == 'forecast':
            continue
        reason = row.get('reason') or row.get('status') or 'unknown'
        group = ('own_minute_warmup_at_last_attempt' if 'current_common_warmup' in reason
                 else 'fresh_quote_unavailable' if 'no_fresh_quote' in reason
                 else 'market_closed_or_no_stream_quote' if 'market_closed_or_no_stream_quote' in reason
                 else 'ridge_model_unavailable' if 'family_prediction_unavailable:ridge' in reason
                 else 'building' if row.get('status') == 'building' else 'other')
        missing.setdefault(group, []).append({k: row.get(k) for k in
            ('instrument', 'status', 'reason', 'current_common_bars', 'required_current_common_bars', 'last_attempt_epoch')})
    result = {'persisted_samples': len(rows), 'successful_api_samples': len(good), 'api_errors': len(errors),
        'api_error_details': [{k: r.get(k) for k in ('observed_utc', 'error_type', 'error')} for r in errors],
        'first_sample_utc': rows[0]['observed_utc'], 'last_sample_utc': rows[-1]['observed_utc'],
        'first_success_utc': good[0]['observed_utc'], 'last_success_utc': latest['observed_utc'],
        'sample_spacing_sec': span([b['observed_epoch']-a['observed_epoch'] for a, b in zip(rows, rows[1:])]),
        'api_response_sec': span([r.get('api_response_sec') for r in good]),
        'pair_status_sample_counts': dict(Counter(r['pair_summary'].get('status') or 'unknown' for r in good)),
        'forecast_pairs': span([r['pair_summary'].get('counts', {}).get('forecast', 0) for r in current_pairs]),
        'current_price_pairs': span([r['market'].get('current_pair_count') for r in good if r['market'].get('status') == 'current']),
        'market_status_sample_counts': dict(Counter(r['market'].get('status') or 'unknown' for r in good)),
        'news': {'status_sample_counts': dict(news_counts), 'evidence_age_sec': span([r['news'].get('evidence_age_sec') for r in good]),
                 'longest_observed_stale_run': {'sample_count': len(longest),
                    'first_utc': longest[0]['observed_utc'] if longest else None,
                    'last_utc': longest[-1]['observed_utc'] if longest else None,
                    'sampled_span_sec': longest[-1]['observed_epoch']-longest[0]['observed_epoch'] if longest else 0},
                 'sampling_caveat': 'Nominal 30-second sampling. These are observed states and first-to-last stale sample spans, not exact continuous durations. Errors or spacing over45seconds split runs; transitions between samples are unknown.'},
        'account': {key: span([r['account'].get(key) for r in good if r['account'].get(
            'positions_current' if key == 'open_trades' else 'orders_current' if key == 'pending_orders' else 'account_values_current') is True])
            for key in ('nav', 'balance', 'open_trades', 'pending_orders', 'unrealized_pl')},
        'account_not_current_samples': {key: sum(r['account'].get(key) is not True for r in good)
            for key in ('account_values_current', 'positions_current', 'orders_current')},
        'collection_not_current_samples': {key: sum(r['collection'].get('observations', {}).get(key, {}).get('current') is not True for r in good)
            for key in ('study', 'quote_stream', 'account')},
        'alert_counts': dict(Counter(a for r in good for a in r.get('alerts', []))),
        'unexpected_authority_samples': sum(r['pair_summary'].get('can_place_orders') is not False or r['pair_summary'].get('can_promote') is not False for r in good),
        'retained_verified_generation_samples': sum(bool(r['pair_summary'].get('retained_generation')) for r in good),
        'retained_read_failure_reasons': dict(Counter(f.get('reason') or 'unknown' for r in good for f in r['pair_summary'].get('publication_read_failures') or [])),
        'last_successful_api_readiness': {'observed_utc': latest['observed_utc'], 'source_generated_epoch': latest['pair_summary'].get('generated_epoch'),
            'summary_status': latest['pair_summary'].get('status'), 'counts': latest['pair_summary'].get('counts'), 'missing_groups': missing,
            'interpretation': 'API reasons and minute counts are from their retained last attempts. Independently recomputed input diagnosis has a separate earlier clock below.'}}
    return result


def compact_collection(source):
    workers = {}
    for name, wrapper in source['worker_heartbeats_and_publications'].items():
        state = wrapper.get('state') or {}
        workers[name] = {'read_status': wrapper.get('read_status'), **{k: state.get(k) for k in
            ('status', 'phase', 'age_seconds', 'errors', 'heartbeat_publication_errors', 'progress_age_sec', 'can_place_orders', 'can_promote')},
            'cycle_error_count': (state.get('details') or {}).get('error_count')}
    check = source['process_check']
    return {'observed_utc': source['observed_utc'], 'started_utc': source['started_utc'],
        'process_check': {k: check.get(k) for k in ('status', 'running_worker_count', 'preserved_other_worker_count', 'preserved_other_process_count', 'same_process_identities_as_185143')},
        'workers': workers, 'updater_cycle': source['updater_cycle']['summary'], 'archive_summary': source['archive_summary'],
        'cycle_gap_status_counts': source['cycle_gap_status_counts'], 'recovery_receipt_checks': source['recovery_receipt_checks']}


def scored_index(source):
    return {(pair, row['decision_id']): row for pair, value in source['pairs'].items() for row in value.get('fresh_scored_rows', [])}


def performance_summary(baseline, final, delta):
    old, new = scored_index(baseline), scored_index(final)
    require(len(old) == 83, 'initial_baseline_must_have83_scored_decisions')
    added = sorted(set(new)-set(old))
    require(delta['baseline_scored'] == len(old) and delta['current_scored'] == len(new), 'delta_scored_counts_mismatch')
    require(delta['new_scored_ids'] == [list(key) for key in added] and delta['new_scored'] == len(added), 'delta_new_ids_mismatch')
    require(delta['disappeared_scored_ids'] == [list(k) for k in sorted(set(old)-set(new))], 'delta_disappeared_ids_mismatch')
    require(delta['changed_prior_scored_rows'] == [list(k) for k in sorted(set(old)&set(new)) if old[k] != new[k]], 'delta_changed_ids_mismatch')
    require(not delta['disappeared_scored_ids'] and not delta['changed_prior_scored_rows'], 'prior_scored_evidence_changed_or_disappeared')
    counts = {key: {'baseline': baseline['aggregate_counts'].get(key), 'final': final['aggregate_counts'].get(key),
        'change': final['aggregate_counts'][key]-baseline['aggregate_counts'][key]} for key in baseline['aggregate_counts'] if key in final['aggregate_counts']}
    lag = delta['snapshot_reconciliation']['fresh_scored_not_in_stored_scorecard']
    score_ages = {pair: epoch(final['finished_utc'])-epoch(value['stored_scorecard']['generated_utc']) for pair, value in final['pairs'].items()
                  if value.get('stored_scorecard', {}).get('generated_utc')}
    old_outcomes = {(pair, row['id']) for pair, value in baseline['pairs'].items() for row in value.get('retained_outcome_rows', [])}
    new_outcomes = {(pair, row['id']) for pair, value in final['pairs'].items() for row in value.get('retained_outcome_rows', [])}
    supplemental = final.get('original_eurusd_supplemental_diagnostic') or {}
    return {'baseline_finished_utc': baseline['finished_utc'], 'final_finished_utc': final['finished_utc'],
        'registered_sources_unchanged': baseline['registered_source_bindings'] == final['registered_source_bindings'],
        'pair_status_counts': final['pair_status_counts'], 'ledger_and_score_counts': counts,
        'baseline_metrics': baseline['fresh_pair_family_metrics'], 'final_metrics': final['fresh_pair_family_metrics'],
        'new_scored_ids': added, 'new_scored_count': len(added), 'new_decision_metrics': delta['new_decision_metrics'],
        'new_retained_outcome_ids': sorted(new_outcomes-old_outcomes),
        'disappeared_scored_ids': delta['disappeared_scored_ids'], 'changed_prior_scored_rows': delta['changed_prior_scored_rows'],
        'reconciliation': delta['snapshot_reconciliation'],
        'stored_scorecard_lag': {'pairs': lag, 'missing_newly_scored_decisions': sum(len(ids) for ids in lag.values()),
            'stored_scorecard_age_sec_at_final_read': span(list(score_ages.values())),
            'interpretation': 'A scorecard age alone does not establish missing scores: a pair with no new matured outcome may retain an old complete scorecard. Missing scored IDs and metric mismatches are reported separately.'},
        'original_eurusd_supplemental': {k: supplemental.get(k) for k in ('status', 'generated_utc', 'freshness', 'original_scorer', 'original_decision_count', 'excluded_decision_count', 'retained_decision_count', 'capacity_state')},
        'original_eurusd_scored': (supplemental.get('report') or {}).get('coverage', {}).get('paired_scored_decisions'),
        'scope': 'Newly matured/scored pair-decision IDs since the initial baseline; issuance may precede this hour. Correlated pairs and overlapping H1 targets are not independent trials. These are quote-based research returns, not executed P/L.'}


def report_text(summary):
    s, p = summary['samples'], summary['performance']
    news, account = s['news'], s['account']
    final_collection = summary['collection_snapshots'][-1]
    missing = s['last_successful_api_readiness']
    lines = [f"Completed observation of the requested {START_UTC}–{END_UTC} window. The bot remained partially operational in research mode; this is not a trading-readiness or profitability pass.", '',
        f"Persisted sampling ran {s['first_sample_utc']}–{s['last_sample_utc']}: {s['successful_api_samples']} successful API samples and {s['api_errors']} API errors. Earlier live checks preceded the persisted loop. Forecast coverage ranged {fmt_range(s['forecast_pairs'])}/68 pairs; current-price coverage ranged {fmt_range(s['current_price_pairs'])}/68. Final API observation: {missing['observed_utc']}, counts {json.dumps(missing['counts'], sort_keys=True)}.", '',
        f"NAV ranged ${fmt(account['nav']['min'],4)}–${fmt(account['nav']['max'],4)}; last ${fmt(account['nav']['last'],4)}. Observed open trades ranged {fmt_range(account['open_trades'])}; pending orders {fmt_range(account['pending_orders'])}. Account freshness exceptions: {json.dumps(s['account_not_current_samples'], sort_keys=True)}. No private account identifiers are included. Research quote outcomes below are separate from account P/L.", '',
        f"News sampled states: {json.dumps(news['status_sample_counts'], sort_keys=True)}; maximum retained evidence age {fmt(news['evidence_age_sec']['max'],1)}s. Longest observed stale run: {news['longest_observed_stale_run']['sample_count']} samples spanning {fmt(news['longest_observed_stale_run']['sampled_span_sec'],1)}s, {news['longest_observed_stale_run']['first_utc']}–{news['longest_observed_stale_run']['last_utc']}. With nominal 30-second sampling, this is not an exact continuous outage duration. Stale news is withheld, not converted to neutral.", '',
        f"Collection/process checks are separately timestamped at {', '.join(c['observed_utc'] for c in summary['collection_snapshots'])}. Final check: {json.dumps(final_collection['process_check'], sort_keys=True)}. Pair-worker errors: {final_collection['workers'].get('pair_worker',{}).get('errors')}; original EUR companion errors: {final_collection['workers'].get('eurusd_companion',{}).get('errors')}. API alerts: {json.dumps(s['alert_counts'], sort_keys=True)}. Retained, heartbeat-bound summary generations served {s['retained_verified_generation_samples']} samples; read-mismatch diagnostics remain in the JSON and are distinct from unavailable API results.", '',
        f"Performance snapshots: initial {p['baseline_finished_utc']} and final {p['final_finished_utc']}. {p['new_scored_count']} newly scored pair decisions were added; exact IDs are retained in the JSON. Outcomes may belong to forecasts issued before the watch. Prior scored rows changed: {len(p['changed_prior_scored_rows'])}; disappeared: {len(p['disappeared_scored_ids'])}.", '',
        '| Snapshot | Model | Scored | Direction hits / directional | Positive after spread / directional | Mean net bps |',
        '| --- | --- | ---: | ---: | ---: | ---: |']
    for scope, key in [('Initial', 'baseline_metrics'), ('Final', 'final_metrics')]:
        for family, name in FAMILIES.items():
            m = p[key][family]
            lines.append(f"| {scope} | {name} | {m['decisions']} | {m.get('direction_hits','unavailable')} / {m['directional_decisions']} | {m.get('positive_after_spread','unavailable')} / {m['directional_decisions']} | {fmt(m['mean_net_bps_per_decision'],3)} |")
    lines += ['', 'Newly scored decisions only:', '']
    for family, name in FAMILIES.items():
        m = p['new_decision_metrics'][family]
        lines.append(f"- {name}: {m['direction_hits']} direction hits and {m['positive_after_spread']} positive after spread among {m['new_scored_decisions']} newly scored decisions; mean net {fmt(m['mean_net_bps'],3)} bps.")
    lines += ['', f"Final saved-score lag: {p['stored_scorecard_lag']['missing_newly_scored_decisions']} fresh-scored IDs missing from saved scorecards across {len(p['stored_scorecard_lag']['pairs'])} pairs. Exact stored-versus-fresh reconciliation is retained. An old scorecard with no newer outcomes is not automatically an incorrect scorecard.", '',
        f"Final API missing-forecast reasons at {missing['observed_utc']}: " + '; '.join(f"{key}: {len(value)}" for key, value in missing['missing_groups'].items()) + '. Minute counts in these API reasons describe the last recorded attempt, not an independently recomputed current count.', '',
        f"Independent missing-input audit covers {summary['missing_input_audit']['input_observation_window']}, with ledger reads {summary['missing_input_audit']['ledger_observation_window']}. Its dated input groups are {json.dumps(summary['missing_input_audit']['group_counts'], sort_keys=True)}. Those earlier counts are not substituted for final readiness. Remaining causes include real-minute continuity, fresh quote/archive endpoints, mature Ridge labels and the fixed attempt cadence; no bars or forecasts were fabricated.", '',
        f"The retained AUD/CAD incident records HTTP504 followed by recovery at {summary['aud_cad_recovery']['observed_utc']} in the next ordinary updater cycle; the receipt does not claim omitted minutes were recovered. Final minute-cycle counts: {json.dumps(final_collection['updater_cycle'], sort_keys=True)}.", '']
    agreement = summary['baseline_model_agreement']
    for group in ('agreement', 'disagreement'):
        g = agreement['groups'][group]
        text = '; '.join(f"{FAMILIES.get(f,f)} {m.get('direction_hits')} hits, {m.get('positive_after_spread')} positive after spread, mean {fmt(m.get('mean_net_bps_per_decision'),3)} bps" for f, m in g['families'].items())
        lines.append(f"Frozen initial-baseline {group}: {g['scored_decisions']} decisions; {text}.")
    lines += ['', f"Agreement analysis uses only the initial {agreement['baseline_finished_utc']} snapshot and does not validate an agreement-based strategy. Pair rows and H1 outcomes overlap. The original EUR/USD diagnostic remains supplemental: {json.dumps(p['original_eurusd_supplemental'], sort_keys=True)}, with {p['original_eurusd_scored']} scored retained unique decisions; it does not repair the frozen scorer failure or count as registered performance.", '',
        'This finalizer only read retained files and created this report plus its JSON summary. The observation session action counts, every input hash, sampled failures and snapshot clocks are retained in LIVE_WATCH_SUMMARY_20260907.json. No model, ledger, account, runtime, project or vault changes were made by this finalizer.']
    return '\n'.join(lines) + '\n'


def main():
    require(time.time() >= END, 'requested_hour_not_finished')
    output_names = ('LIVE_WATCH_REPORT_20260907.md', 'LIVE_WATCH_SUMMARY_20260907.json')
    require(all(not (ROOT/name).exists() for name in output_names), 'new_outputs_required_do_not_overwrite')
    bindings = {}
    session = read('live_session_result.json', bindings)
    require(session.get('status') == 'completed' and epoch(session['finished_utc']) >= END, 'session_not_completed_through_requested_end')
    require(epoch(session['requested_window_end_utc']) == END, 'session_window_mismatch')
    rows = read('live_samples.jsonl', bindings, lines=True)
    require(rows and all(r.get('event') in ('live_sample','sample_error') and numeric(r.get('observed_epoch')) for r in rows), 'invalid_sample_rows')
    require(all(b['observed_epoch'] > a['observed_epoch'] for a,b in zip(rows,rows[1:])), 'nonmonotonic_sample_clock')
    require(rows[-1]['observed_epoch'] >= END and rows[0]['observed_epoch'] >= epoch(START_UTC), 'sample_window_incomplete')
    require(session['persisted_samples'] == len(rows) and session['api_failures'] == sum(r['event']=='sample_error' for r in rows), 'session_sample_count_mismatch')
    baseline = read('PERFORMANCE_BASELINE_20260907.json', bindings)
    require(bindings['PERFORMANCE_BASELINE_20260907.json']['sha256'] == BASELINE_SHA256, 'initial_baseline_bytes_changed')
    final = read('PERFORMANCE_FINAL_20260907.json', bindings)
    delta = read('PERFORMANCE_DELTA_FINAL_20260907.json', bindings)
    collection_baseline = read('COLLECTION_OPERATIONAL_BASELINE_VERIFIED_20260907.json', bindings)
    collection_final = read('COLLECTION_FINAL_20260907.json', bindings)
    require(final.get('status') == 'completed' and epoch(final['started_utc']) >= END and epoch(final['finished_utc']) >= epoch(final['started_utc']), 'final_performance_capture_not_after_window')
    require(epoch(collection_final['started_utc']) >= END and epoch(collection_final['observed_utc']) >= epoch(collection_final['started_utc']), 'final_collection_capture_not_after_window')
    require(delta.get('status') == 'compared' and delta['baseline_sha256'] == bindings['PERFORMANCE_BASELINE_20260907.json']['sha256'] and delta['current_sha256'] == bindings['PERFORMANCE_FINAL_20260907.json']['sha256'], 'delta_not_bound_to_initial_and_final_files')
    require(delta['baseline_cutoff'] == baseline['finished_utc'] and delta['current_cutoff'] == final['finished_utc'], 'delta_clock_mismatch')
    require(baseline['registry_sha256'] == final['registry_sha256'] and baseline['registered_source_bindings'] == final['registered_source_bindings'], 'performance_registration_changed')
    audit = read('MISSING_FORECASTS_AUDIT_20260907.json', bindings)
    agreement = read('MODEL_AGREEMENT_BASELINE_20260907.json', bindings)
    require(agreement['baseline_sha256'] == bindings['PERFORMANCE_BASELINE_20260907.json']['sha256'], 'agreement_baseline_binding_mismatch')
    recovery = read('AUD_CAD_504_RECOVERY_20260907.json', bindings)
    for key, hash_key in [('prior_evidence', 'prior_evidence_sha256'), ('current_cycle_evidence', 'current_cycle_sha256')]:
        read(recovery[key], bindings)
        require(bindings[recovery[key]]['sha256'] == recovery[hash_key], 'recovery_evidence_binding_mismatch')
    checkpoints = [collection_baseline]
    for path in sorted(ROOT.glob('COLLECTION_20260907_*.json')):
        checkpoints.append(read(path.name, bindings))
    checkpoints.append(collection_final)
    checkpoints.sort(key=lambda value: epoch(value['observed_utc']))
    groups = {name: {key: value.get(key) for key in ('scored_decisions','families','mean_reconstructed_roundtrip_spread_drag_bps','mean_reconstructed_absolute_entry_to_target_midpoint_move_bps','spread_reconstruction_denominator','spread_exceeds_absolute_entry_to_target_move')} for name,value in agreement['groups'].items()}
    summary = {'schema_version': 'completed_live_watch_report_v1_20260907', 'status': 'completed_observation', 'operation_assessment': 'partially_operational_research_only',
        'generated_utc': iso(time.time()), 'finalizer_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'requested_window': {'start_utc': START_UTC, 'end_utc': END_UTC},
        'session': session, 'samples': sample_summary(rows, session['interval_sec']),
        'collection_snapshots': [compact_collection(c) for c in checkpoints],
        'performance': performance_summary(baseline, final, delta),
        'missing_input_audit': {'written_utc': audit['written_utc'], 'input_observation_window': audit['input_observation_window'], 'ledger_observation_window': audit['ledger_observation_window'],
            'group_counts': {k:v['count'] for k,v in audit['primary_current_input_groups'].items()}, 'ready_now_but_missing_forecast': audit['ready_now_but_missing_forecast'], 'reader_or_worker_assessment': audit['reader_or_worker_assessment']},
        'baseline_model_agreement': {'baseline_finished_utc': agreement['baseline_finished_utc'], 'analyzed_utc': agreement['analyzed_utc'], 'groups': groups, 'limitations': agreement['limitations']},
        'aud_cad_recovery': recovery, 'input_bindings': bindings,
        'limitations': ['Sampled status is not continuous monitoring proof; events between samples may be missed.', 'The initial performance/collection baselines were captured after the requested window began; performance deltas cover their exact stated clocks.', 'Success rates on correlated overlapping research decisions do not establish an independent edge, calibrated probabilities or broker profit.', 'The final collection and performance snapshots are separate reads after the observation window and retain their own clocks.'],
        'finalizer_actions': {'runtime_or_network_reads': 0, 'orders': 0, 'project_writes': 0, 'vault_writes': 0, 'existing_artifacts_overwritten': 0}}
    summary = clean(summary)
    report = clean(report_text(summary))
    summary['report_sha256'] = hashlib.sha256(report.encode('utf-8')).hexdigest()
    require(not ACCOUNT_ID.search(report + json.dumps(summary)), 'private_account_identifier_in_output')
    # Both outputs are created exclusively only after every required input and clock check.
    with (ROOT/output_names[0]).open('x', encoding='utf-8', newline='\n') as handle:
        handle.write(report)
    with (ROOT/output_names[1]).open('x', encoding='utf-8', newline='\n') as handle:
        json.dump(summary, handle, indent=2, allow_nan=False)
        handle.write('\n')
    print(json.dumps({'status': summary['status'], 'outputs': list(output_names), 'persisted_samples': len(rows), 'new_scored_decisions': summary['performance']['new_scored_count']}))


if __name__ == '__main__':
    main()
