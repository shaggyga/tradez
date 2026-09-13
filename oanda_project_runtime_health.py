"""Read-only, bounded current supervision and integrity-check scope projection.

Legacy artifact validity is never upgraded by retirement. A fresh supervisor
observation explains operational scope; it does not prove prediction quality.
"""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import math
import os
import re
import time

EXPECTED_RESEARCH_WORKERS = frozenset({
    'account_snapshot','live_dashboard','local_news_sentiment','official_release_fast_lane',
    'official_release_fast_mapper','all68_m1_forward_archive','practice_007_quote_stream',
    'clock_integrity_monitor','pair_local_forecast_study_v1','pair_local_forecast_study_v2',
    'source_governance_news_fast_lane','project_integrity_audit','storage_headroom_guard',
    'local_news_sentiment_repair_v1','joint_price_news_study_v3',
})
INACTIVE_REASONS = frozenset({'disabled','research_collection_only','safe_core_only'})
CHECK_WORKERS = {
    'continuous_narrative_meter_sealed_current_and_inert':('continuous_narrative_meter',),
    'event_technical_preflight_source_transport_current_and_inert':('event_technical_preflight',),
    'executable_move_census_current_independently_verified':('executable_move_census_capture_v3h','executable_move_census_verifier_v3h'),
    'gdelt_attention_magnitude_proof_fail_closed':('gdelt_attention_magnitude_proof',),
    'independent_verifier_matches':('independent_evidence_verifier',),
    'live_move_news_forward_proof_contract_current':('live_move_news_snapshot','live_move_news_outcomes'),
    'live_move_persistent_context_current_and_inert':('live_move_persistent_news_context',),
    'move_first_live_arm_alignment_current_causal_and_inert':('move_first_live_arm_alignment_v1',),
    'move_first_live_case_capture_current_and_inert':('move_first_live_case_capture_v4',),
    'move_first_news_mapping_audit_current':('move_first_news_case_audit',),
    'move_first_operational_mapping_alignment_v4_current_subsecond_causal_and_inert':('move_first_operational_mapping_alignment_v4',),
    'official_release_fast_response_watch_current_and_inert':('official_release_fast_response_watch',),
    'official_source_depth_complete_current_and_zero_weight':('official_currency_source_depth',),
    'scheduled_event_quote_capture_current_append_only_and_inert':('scheduled_event_quote_capture_v1',),
    'scheduled_event_quote_capture_v2_current_append_only_and_inert':('scheduled_event_quote_capture_v2',),
    'signal_availability_market_state_consistent':('practice_007_signal_feed_availability',),
    'executor_healthy':('practice_007_fast_executor',),
    'four_frozen_proof_families_running':('proof_shadow_predictors',),
    'quote_crosscheck_healthy':('practice_007_quote_stream',),
    'official_release_fast_lane_current_and_inert':('official_release_fast_lane',),
    'official_release_fast_mapping_current_and_inert':('official_release_fast_mapper',),
    'news_source_governance_fast_lane_current_prospective_and_inert':('source_governance_news_fast_lane',),
    'news_watchlist_uses_current_classification_contract':('news_technical_watchlist',),
}


def _epoch(value):
    if not isinstance(value,str):
        raise ValueError('explicit_timezone_timestamp_required')
    dt = datetime.fromisoformat(value.replace('Z','+00:00'))
    if dt.tzinfo is None:
        raise ValueError('explicit_timezone_timestamp_required')
    result = dt.timestamp()
    if not math.isfinite(result) or result <= 0:
        raise ValueError('positive_finite_timestamp_required')
    return result


def _complete_json_lines(raw):
    def reject_nonfinite(value):
        raise ValueError('nonfinite_supervisor_json_number:'+value)
    lines = raw.splitlines(keepends=True)
    return [json.loads(line,parse_constant=reject_nonfinite) for line in lines if line.endswith((b'\n',b'\r')) and line.strip()]


def read_supervisor_observation(logs, *, clock=time.time):
    """Observe newest log only; do not fall back to a healthier old supervisor."""
    result = {'schema_version':'project_supervision_observation_v1_20260907',
              'status':'unavailable','observed_epoch':None,'reasons':[], 'workers':{},
              'expected_workers':sorted(EXPECTED_RESEARCH_WORKERS),
              'scope':'Fresh supervisor-reported process and output checks; not an independent OS process inventory or forecast-performance assessment.'}
    try:
        logs = Path(logs).resolve()
        paths = list(logs.glob('always_on_supervisor_*.jsonl'))
        if not paths:
            raise ValueError('supervisor_log_missing')
        path = max(paths,key=lambda p:p.name)
        if path.resolve().parent != logs:
            raise ValueError('supervisor_log_path_escape')
        with path.open('rb') as handle:
            before = os.fstat(handle.fileno())
            size = before.st_size
            head = handle.read(min(size,65536))
            offset = max(0,size-2*1024*1024)
            handle.seek(offset)
            tail = handle.read(size-offset)
            after = os.fstat(handle.fileno())
        current = path.stat()
        if ((before.st_dev,before.st_ino)!=(after.st_dev,after.st_ino)
                or (after.st_dev,after.st_ino)!=(current.st_dev,current.st_ino)
                or after.st_size<size or len(tail)!=size-offset):
            raise ValueError('supervisor_log_replaced_or_truncated')
        if offset:
            tail = tail.split(b'\n',1)[1] if b'\n' in tail else b''
        start_rows = _complete_json_lines(head)
        starts = [row for row in start_rows if row.get('event')=='supervisor_started']
        tail_rows = _complete_json_lines(tail)
        heartbeats = [row for row in tail_rows if row.get('event')=='heartbeat']
        if len(starts)!=1 or not heartbeats:
            raise ValueError('supervisor_start_or_heartbeat_missing')
        start,heartbeat = starts[0],heartbeats[-1]
        observed = clock()
        if type(observed) not in (float,int) or not math.isfinite(observed) or observed<=0:
            raise ValueError('invalid_consumer_clock')
        result['observed_epoch']=observed
        started,generated = _epoch(start['time']),_epoch(heartbeat['time'])
        if not started<=generated<=observed or observed-generated>90:
            raise ValueError('stale_future_or_reversed_supervisor_clock')
        if start.get('research_collection_only') is not True:
            raise ValueError('research_only_supervision_not_established')
        announced = start.get('research_collection_names')
        if (not isinstance(announced,list) or len(announced)!=len(set(announced))
                or not EXPECTED_RESEARCH_WORKERS <= set(announced)):
            raise ValueError('expected_research_workers_not_in_supervisor_allowlist')
        managed = heartbeat.get('managed')
        if not isinstance(managed,list) or len(managed)>256:
            raise ValueError('bounded_supervisor_worker_list_required')
        workers = {}
        seen_active_pids = set()
        for row in managed:
            name,pids,freshness = row.get('name'),row.get('pids'),row.get('freshness')
            if (not isinstance(name,str) or not re.fullmatch(r'[a-z0-9_]+',name) or name in workers
                    or type(row.get('running')) is not bool or not isinstance(pids,list)
                    or any(type(pid) is not int or pid<=0 for pid in pids) or len(pids)!=len(set(pids))
                    or not isinstance(freshness,dict)):
                raise ValueError('invalid_or_duplicate_supervised_worker')
            running = row['running']
            if running and seen_active_pids.intersection(pids):
                raise ValueError('one_pid_claimed_by_multiple_workers')
            if running:
                seen_active_pids.update(pids)
            explicitly_inactive = not running and not pids and freshness.get('reason') in INACTIVE_REASONS
            healthy = running and bool(pids) and freshness.get('fresh') is True
            if running and freshness.get('reason') not in ('fresh','not_checked'):
                healthy = False
            if running and freshness.get('reason')=='fresh':
                age = freshness.get('age_sec')
                if type(age) not in (float,int) or not math.isfinite(age) or age<0:
                    healthy = False
            # An unchecked dashboard process is reported separately, never as a
            # verified HTTP response. Supervisor output age is its own observed age.
            workers[name]={'running':running,'pids':pids,'freshness':freshness,
                           'supervisor_check_ok':healthy,'explicitly_inactive':explicitly_inactive}
        result.update(workers=workers,generated_epoch=generated,supervisor_age_sec=observed-generated,
            source={'path':str(path),'observed_size_bytes':size,'head_sha256':hashlib.sha256(head).hexdigest(),
                    'tail_sha256':hashlib.sha256(tail).hexdigest(),'tail_start_offset_before_partial_line_drop':offset})
        issues = []
        for name in sorted(EXPECTED_RESEARCH_WORKERS):
            if not workers.get(name,{}).get('supervisor_check_ok'):
                issues.append('required_worker_unhealthy_or_missing:'+name)
        for name,row in workers.items():
            if row['running'] and name not in EXPECTED_RESEARCH_WORKERS:
                issues.append('unexpected_active_worker:'+name)
        later_errors = [row for row in tail_rows if row.get('event')=='supervisor_error' and _epoch(row['time'])>generated]
        if later_errors:
            issues.append('supervisor_error_after_latest_heartbeat')
        result.update(status='degraded' if issues else 'current',reasons=issues,
            running_worker_count=sum(r['running'] for r in workers.values()),
            expected_worker_count=len(EXPECTED_RESEARCH_WORKERS),
            output_unchecked_workers=[name for name,row in workers.items() if row['running'] and row['freshness'].get('reason')=='not_checked'],
            later_supervisor_errors=[{'time':row['time'],'error':str(row.get('error',''))[:500]} for row in later_errors])
    except (OSError,ValueError,TypeError,KeyError,IndexError,AttributeError) as exc:
        result.update(status='unavailable',workers={},reasons=[type(exc).__name__+': '+str(exc)])
    return result


def scope_integrity_checks(checks, runtime):
    """Explain inactive scope without changing any artifact check or failure."""
    workers = runtime.get('workers',{}) if runtime.get('status') in ('current','degraded') else {}
    scopes = {}
    for check,passed in checks.items():
        names = CHECK_WORKERS.get(check,())
        if not names:
            scope = 'shared_or_unmapped_artifact_check'
        elif any(name not in workers for name in names):
            scope = 'unknown_runtime_scope'
        elif all(workers[name].get('explicitly_inactive') for name in names) and not set(names)&EXPECTED_RESEARCH_WORKERS:
            scope = 'inactive_legacy_component'
        elif all(workers[name].get('running') for name in names):
            scope = 'active_component'
        else:
            scope = 'mixed_or_unhealthy_component'
        scopes[check]={'scope':scope,'workers':list(names),'artifact_check_passed':passed,
                       'scope_observed_epoch':runtime.get('observed_epoch')}
    legacy_failures = sorted(name for name,row in scopes.items() if row['artifact_check_passed'] is False and row['scope']=='inactive_legacy_component')
    current_failures = sorted(name for name,row in scopes.items() if row['artifact_check_passed'] is False and row['scope']!='inactive_legacy_component')
    live_claims = {}
    for check in ('executor_healthy','four_frozen_proof_families_running'):
        name = CHECK_WORKERS[check][0]
        worker = workers.get(name)
        live_claims[check]={'retained_artifact_claim':checks.get(check),
            'currently_running':worker.get('running') if worker else None,
            'current_supervisor_check_ok':worker.get('supervisor_check_ok') if worker else None,
            'scope':scopes.get(check,{}).get('scope','unknown_runtime_scope')}
    return {'check_scopes':scopes,'inactive_component_failures':legacy_failures,
            'active_shared_or_unknown_failures':current_failures,'live_runtime_assertions':live_claims,
            'interpretation':'Inactive legacy failures remain failed. Retirement explains live relevance; it does not repair missing evidence, make a stale result current, or authorize any orders.'}
