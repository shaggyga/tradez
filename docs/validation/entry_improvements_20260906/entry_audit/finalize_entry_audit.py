"""Finalize review evidence only; no production writes or runtime imports."""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent.parent/'trad'
load=lambda name:json.loads((OUT/name).read_text())
scan=load('executor_log_scan.json');detail=load('executor_gate_detail.json');accounting=load('accounting_week_review.json')
events=load('executor_week_event_index.json')
parse=lambda value:datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
lo,hi=parse(scan['start_inclusive_utc']),parse(scan['end_exclusive_utc'])
times=sorted(row['time']for row in events if row['event']=='execution_selection_summary')
gaps=sorted([{'previous_summary_utc':a,'next_summary_utc':b,'gap_sec':parse(b)-parse(a)}for a,b in zip(times,times[1:])],key=lambda row:row['gap_sec'],reverse=True)
sentinels=[row for row in accounting['week_routeability_sentinels']if lo<=parse(row['observed_utc'])<hi]
sources={}
for name,ranges in {
    'oanda_practice_shadow_strategy_lab.py':[(1362,1430),(2315,2391),(2988,3010),(3026,3059),(3096,3129),(3278,3308),(3367,3400),(3704,3716),(3804,3849),(3892,3981),(6032,6074),(6102,6145),(6189,6220)],
    'oanda_practice_top_signal_executor.py':[(625,644),(850,874),(1386,1405)],
}.items():
    path=ROOT/name;raw=path.read_bytes();lines=raw.decode('utf-8-sig').splitlines()
    sources[name]={'sha256':hashlib.sha256(raw).hexdigest(),'observed_utc':datetime.now(timezone.utc).isoformat(),
        'note':'Current source semantics observed during audit; weekly log examples predate new lineage diagnostics.',
        'excerpts':[{'start_line':a,'end_line':b,'text':'\n'.join(f'{i+1}: {lines[i]}'for i in range(a-1,min(b,len(lines))))}for a,b in ranges]}
shape={
    'selection_summary_first_in_window':{key:scan['first_event_examples']['execution_selection_summary'][key]for key in
        ('event','time','matrix_candidates','accepted_candidates','signal_feed_candidates','qualified_candidates','nonconflicting_qualified_candidates','selected_id','selection_mode')},
    'selection_summary_nonconflicting_example':detail['nonconflicting_summary_examples'],
    'execution_skip_first_in_window':scan['first_event_examples']['execution_skipped'],
    'cost_block_occurrences':detail['cost_block_occurrences'],
    'fast_executor_start_first_in_window':scan['first_event_examples']['fast_executor_start'],
    'old_trade_exit_recovery_example':scan['first_event_examples']['practice_trade_exit_observed'],
    'order_outcome_events_observed_in_week':[],
    'parsing_notes':[
        'JSONL timestamps live in time; filter actual UTC timestamps, not filename or sentinel_date alone.',
        'Rotated part_...jsonl files contain earlier portions of the same run and must be included once.',
        'execution_selection_summary appears at least 60s apart and is not every loop cycle.',
        'accepted_candidates/matrix_candidates count locally passed candidates, which are empty in feed-consumer mode.',
        'signal_feed_candidates counts upstream feed rows per observed summary, before the execution prefilter; do not sum as unique forecasts.',
        'qualified_candidates is pre-final-gate; selected_id is preliminary and can still fail direction/cost/authorization gates.',
        'execution_skipped carries candidate_blocks[].reason; top-level no_nonconflicting_signal_capacity conflates several final gates.',
        'Skip logs repeat at most once per 10s for the same top-level reason; nested reason changes can be suppressed inside that throttle.',
        'practice_trade_exit_observed source recent_transaction_recovery is a replay observation; its log time is not broker trade time.',
        'practice_order_filled, practice_order_not_filled, practice_order_error distinguish broker submission outcomes; absent here and cross-checked against accounting.',
    ],
}
result={
    'schema_version':'independent_week_entry_audit_v1',
    'generated_utc':datetime.now(timezone.utc).isoformat(),
    'window_start_inclusive_utc':scan['start_inclusive_utc'],'window_end_exclusive_utc':scan['end_exclusive_utc'],
    'account_suffix':'-007','classification':'local_retained_execution_and_account_evidence',
    'conclusion':'No position entries were recorded during the requested week; the executor ran, but candidates were blocked before order submission.',
    'funnel':{
        'exact_loop_cycles':None,'exact_loop_cycles_reason':'Only sampled selection logs survive for the week; final saved heartbeat is Sep5 outside window.',
        'sampled_selection_summaries':6997,'summaries_with_feed_candidates':5589,
        'feed_candidate_observations_across_summaries':2440194,
        'summaries_with_qualified_candidates':960,'qualified_candidate_observations_across_summaries':1276,
        'summaries_with_nonconflicting_qualified_candidates':1,
        'nonconflicting_qualified_observations_across_summaries':1,
        'logged_final_selection_skip_events':6099,
        'logged_direction_conflict_block_candidate_observations':7972,
        'logged_movement_to_cost_block_candidate_observations':6,
        'logged_distinct_direction_conflict_candidate_ids':1386,
        'logged_distinct_movement_to_cost_candidate_ids':2,
        'successful_signed_canary_consumptions_observed':0,
        'authorization_rejections_logged':0,
        'authorization_requests_exact':None,
        'broker_order_outcome_events_logged':0,
        'week_accounting_submission_attributions':0,
        'position_entry_fills_observed':0,
        'counts_are_different_observation_granularities_not_a_unique_signal_conversion_rate':True,
    },
    'account_crosscheck':{
        'valid_current_week_snapshots':510,'unchanged_last_transaction_id':'2461',
        'unchanged_balance_and_nav':41.6042,
        'valid_snapshot_before_week':accounting['nearest_valid_snapshot_before_week'],
        'valid_snapshot_after_week':accounting['nearest_valid_snapshot_after_week'],
        'latest_retained_order_submission_utc':accounting['latest_submission_utc'],
        'historical_fills_not_this_week':33,
        'in_window_no_submit_routeability_sentinels':sentinels,
        'source_database':accounting['database'],'source_database_sha256':accounting['database_sha256'],
    },
    'runtime_evidence':{
        'sessions_observed_in_window':6,'new_session_starts_in_window':5,
        'selection_first':times[0],'selection_last':times[-1],
        'largest_selection_receipt_gaps':gaps[:5],
        'feed_cache_states_sampled':detail['sampled_feed_cache_states'],
        'price_stream_connections_logged':36,'price_stream_error_events':32,
        'price_stream_http_401_events':2,'executor_cycle_error_events':2,
        'note':'Session/activity receipts establish operation, not uninterrupted uptime. Gaps are observation gaps; missing opportunities cannot be quantified.',
    },
    'gate_scope':{
        'direction_rule':'Any opposite-direction contributor at the same preferred horizon of the same instrument marks the aggregate conflicted.',
        'includes_zero_weight_shadow_or_failed_components':True,
        'cross_horizon_opposition_directly_mixed':False,
        'freshness':'Feed expiry and cache freshness limit incoming records. Semantic deduplication keys include direction, allowing opposite prior/current directions within TTL to coexist.',
        'intended_scope_evidence':'Execution prefilter docstring explicitly retains research-only/timing/correlation/opposing rows for consensus and vetoes; filtered-weight exclusion does not remove rows from the raw conflict veto.',
        'weekly_shadow_only_conflict_count':None,
        'weekly_shadow_only_conflict_count_reason':'Archived logs lack full contributor lineage/eligibility/weight records; current added fields cannot be backfilled as historical proof.',
        'authorization':'Five startup receipts show entry_authorized=false. Final saved authorization reports zero_confirmed_candidates. No consumption DB exists and no submission outcomes were logged.',
        'recommendation':'Preserve execution gates. Register offline research to compare eligibility-aware conflict scope with complete contributor lineage and fixed decisions; do not bypass the veto to generate trades.',
    },
    'exact_cost_examples':detail['cost_block_occurrences'],
    'missing_improvements':[
        'Cumulative run/cycle and candidate-stage counters with explicit sampled-versus-exact semantics; accepted local and shared-feed counts must be distinct.',
        'Separate final conflict, portfolio, cost, authorization, order-attempt and fill reasons rather than calling all rejections no_nonconflicting_signal_capacity.',
        'Capture immutable contributor IDs, issue/availability/expiry clocks, horizon, role, eligibility and execution weights for every conflict.',
        'Off-line fixed comparison of current any-opposition veto against eligibility-aware and latest-version conflict definitions, with complete lineage and no live gate relaxation.',
        'Monitor and explain receipt gaps, especially the 8791.5-second late-Aug31/early-Sep1 gap and 467.7-second Sep2 gap; reconstruct worker lifecycle where receipts exist.',
        'Display recovered historical trade observations using original broker timestamps and label startup replay so they are not counted as new weekly trades.',
        'Track signed-authorization readiness separately from executor process health; an enabled running executor is not permission to open a position.',
    ],
    'limitations':[
        'No broker endpoint or private credential was accessed; the no-entry finding is independently supported by retained order logs and unchanged bracketing transaction IDs.',
        'No huge canonical outcome database, 2.8GB allocator database or multi-gigabyte log was scanned. Nine dated executor files totaling214,463,783bytes were inspected; accounting database is157,077,504bytes with only4323snapshot/38attribution rows.',
        'Accounting reads used read-only immutable SQLite with no WAL present, explicit query_only and a bounded VM-step budget. No production files were written.',
        'Signal observation counts repeat across summaries. The one sampled nonconflicting observation does not imply only one such event occurred; detailed skip logs preserve two distinct cost-blocked IDs.',
        'These results do not demonstrate that relaxing gates would have been profitable, or that every blocked signal was economically invalid.',
    ],
    'runtime_started':False,'source_changed':False,'broker_accessed':False,
}
with(OUT/'source_gate_semantics.json').open('x',encoding='utf-8')as handle:json.dump(sources,handle,indent=2);handle.write('\n')
with(OUT/'log_shape_examples.json').open('x',encoding='utf-8')as handle:json.dump(shape,handle,indent=2);handle.write('\n')
with(OUT/'WEEK_ENTRY_AUDIT_20260906.json').open('x',encoding='utf-8')as handle:json.dump(result,handle,indent=2);handle.write('\n')
text='''# Last completed FX week: independent entry audit

**The user's zero-entry observation is supported by the saved records.** From August 30, 2026 at 21:00 UTC through September 4 at 21:00 UTC, the practice-007 executor ran across six observed sessions, but no order outcomes or position-entry fills were recorded. All 510 valid/current account snapshots retain transaction ID 2461, and valid snapshots immediately before and after the whole interval retain the same ID. The latest retained order submission was August 4, not this week.

The immediate bottleneck was candidate gating. The 6,997 minute-level selection summaries contained 2,440,194 repeated feed-candidate observations and 1,276 qualified-candidate observations. Only one sampled qualified observation was nonconflicting. There were 6,099 final-selection skip events: their nested reasons contain 7,972 direction-conflict candidate observations and six movement-to-cost observations across two distinct candidate IDs. No signed canary consumption database or order outcome exists for the week. Exact loop-cycle totals cannot be recovered from these sampled records.

The conflict rule treats any opposing contributor at the same instrument and preferred horizon as a veto, including research-only, account-ineligible, failed or zero-weight contributors retained for diagnostics. It does not directly pool opposite horizons. The source explicitly preserves these contributors for vetoes. Archived logs omit the lineage needed to determine how many historical conflicts arose solely from ineligible rows. This warrants an offline comparison with full contributor clocks and eligibility, while preserving the current trading gates.

The two nonconflicting candidates retained in detailed skip logs failed concrete cost requirements. CHF/JPY sell on September 1 had 3.6 pips projected movement against 2.9 pips spread (1.2414 ratio, minimum 2.0), leaving 0.7 pips instant net against a 1.0-pip minimum. AUD/JPY buy on September 2 had 2.5 pips projected movement against 2.0–2.1 pips spread (1.25–1.1905 ratio), leaving 0.5–0.4 pips instant net. Historical/calibrated projected net was larger, but the cost gate correctly checks the current projection.

The logs also replay 55 old trade exits at five restarts, producing 275 recovery observations. Their timestamps are recovery times and must not be counted as weekly trade exits. The largest selection-receipt gap is about 2 hours 26 minutes; a second is about 7 minutes 48 seconds. These show monitoring/continuity gaps, without proving how many signals were missed.

The next improvements are explicit funnel counters, separate conflict/cost/authorization statuses, complete signal lineage, recovered-trade labels and receipt-gap reporting. Strong research predictions and authorization readiness must be demonstrated before changing execution policy. Zero entries alone is not evidence that safe entries were available.

Evidence: `WEEK_ENTRY_AUDIT_20260906.json`, `executor_log_scan.json`, `executor_gate_detail.json`, `accounting_week_review.json`, `source_gate_semantics.json` and bounded `log_shape_examples.json`. The accounting sentinel date-candidate list includes a pre-window Sunday record; the final JSON filters actual timestamps and retains five in-window sentinel checks. All work was read-only on the canonical project; output files are confined to this review directory.
'''
with(OUT/'WEEK_ENTRY_AUDIT_20260906.md').open('x',encoding='utf-8')as handle:handle.write(text)
receipt={path.name:hashlib.sha256(path.read_bytes()).hexdigest()for path in sorted(OUT.iterdir())if path.is_file()}
with(OUT/'ENTRY_AUDIT_RECEIPT_20260906.json').open('x',encoding='utf-8')as handle:json.dump(receipt,handle,indent=2);handle.write('\n')
print(json.dumps({'report':str(OUT/'WEEK_ENTRY_AUDIT_20260906.json'),'sha256':receipt['WEEK_ENTRY_AUDIT_20260906.json'],
                   'largest_gaps':gaps[:2],'sentinels_exact_window':len(sentinels)},indent=2))
