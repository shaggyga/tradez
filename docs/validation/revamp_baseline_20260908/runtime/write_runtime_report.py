"""Finalize retained runtime evidence only; no reads of live project or runtime state."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json

OUT=Path(__file__).parent
def sha(raw):return hashlib.sha256(raw).hexdigest()
def read(name):return json.loads((OUT/name).read_text(encoding='utf-8'))
def short(record):
    api=record['api'];supervisor=record['supervisor']
    return {'started_utc':record['started_utc'],'finished_utc':record['finished_utc'],
        'supervisor_status':supervisor['status'],'running_workers':supervisor['running_worker_count'],'expected_workers':supervisor['expected_worker_count'],
        'api_http_status':api['http_status'],
        'joint_status':api['joint_v2']['status'],'joint_coverage':api['joint_v2']['actual_unelapsed_forecast_pairs'],
        'joint_reasons':api['joint_v2']['reason_counts'],'joint_counts':api['joint_v2'].get('ledger_counts'),
        'price_status':api['price_v2']['status'],'price_reason':api['price_v2'].get('reason'),
        'price_coverage':api['price_v2']['actual_unelapsed_forecast_pairs'],'price_counts':api['price_v2'].get('ledger_counts'),
        'market':api['market'],'news':api['news'],'positions':api['account_positions_only'],
        'news_collector':record['heartbeats']['news_collector']['value'],
        'guarded_news':record['heartbeats']['guarded_news']['value']}

def main():
    names=['RUNTIME_INITIAL_20260908.json','RUNTIME_FOLLOWUP_20260908.json','RUNTIME_RECOVERY_20260908.json']
    records=[read(name) for name in names];first,last=records[0],records[-1]
    conflict=read('NEWS_TOPIC_IDENTITY_CONFLICT_CASE_20260908.json');pub=read('JOINT_LAST_PUBLICATION_20260908.json')
    case=conflict['conflicts'][0];pa,pb=case['brief_topics'];latest=pub['latest_publication']
    sources_unchanged=all(r['source_sha256']==first['source_sha256'] for r in records)
    registrations_unchanged=all(all(r['registered_controls'][k]['sha256']==v['sha256'] for k,v in first['registered_controls'].items()) for r in records)
    all_bindings_match=all(v['source_bindings']['all_match'] for r in records for v in r['registered_controls'].values())
    no_pids_missing=all(not r['os_processes']['reported_worker_pids_not_confirmed_in_os'] for r in records)
    elapsed=(datetime.fromisoformat(last['finished_utc'])-datetime.fromisoformat(first['started_utc'])).total_seconds()
    price_delta={k:last['api']['price_v2']['ledger_counts'][k]-first['api']['price_v2']['ledger_counts'][k] for k in first['api']['price_v2']['ledger_counts']}
    joint_unchanged=all(r['api']['joint_v2']['ledger_counts']==first['api']['joint_v2']['ledger_counts'] for r in records)
    summary={'schema':'revamp_runtime_baseline_summary_v1_20260908','generated_utc':datetime.now(timezone.utc).isoformat(),
        'result':'research_processes_alive_joint_forecasting_blocked','observation_scope':'Two baseline observations plus one bounded price-API recovery observation; no sustained availability or predictive-performance claim.',
        'observation_span_sec':elapsed,'observations':[short(r) for r in records],
        'source_files_unchanged':sources_unchanged,'registrations_unchanged':registrations_unchanged,
        'all_registered_source_bindings_match':all_bindings_match,'all_supervised_pids_confirmed_in_os':no_pids_missing,
        'joint_counts_unchanged':joint_unchanged,'price_ledger_delta_initial_to_recovery':price_delta,
        'last_successful_joint_publication':latest,'latest_joint_publication_age_sec_at_read':latest['observed_epoch']-latest['publication_epoch'],
        'conflict':{'topic_id':case['topic_id'],'source_indices':case['source_indices'],'headlines':[pa['headline'],pb['headline']],
            'member_counts':[len(pa['article_event_ids']),len(pb['article_event_ids'])],
            'member_sets_disjoint':not(set(pa['article_event_ids'])&set(pb['article_event_ids'])),
            'individual_snapshot_status':case['individual_snapshot_status'],'individual_snapshot_news_state':case['individual_snapshot_news_state'],
            'pair_errors':case['pair_replay']['errors'],'input_as_of_utc':conflict['guarded_snapshot']['as_of_utc'],
            'diagnosis':'The producer assigns the same topic/story identity to different retained grouped variants with different original clocks and member sets. The two public headlines describe the same syndicated story; this reproduction does not demonstrate opposing or incompatible directional news. The current-snapshot contract requires one immutable current object per identity and correctly rejects the collision. The exact historical grouping sequence that produced both variants was not replayed.'},
        'first_remediation_scope':'In an isolated copy, reproduce this exact two-topic collision, resolve topic identity/canonical grouping upstream, and verify unique snapshot identities while preserving original member availability/expiry and independent-source deduplication. Do not accept arbitrary duplicate winners or relabel invalid data neutral. A producer/guard source change requires appropriately new source-bound joint-study input registration; do not patch the frozen running study in place.',
        'separate_issues':['Price-v2 API temporarily rejected mismatched summary/heartbeat generations in the follow-up; the recovery read accepted 68 pairs. The exact producer-pair failure phase was not captured, so its cause is not established here.','News evidence crossed its 300-second freshness threshold between collector publications; fresh heartbeat does not establish fresh eligible news.'],
        'non_actions':['No trading or broker API calls','No starts, stops or runtime reloads','No project/vault source or database writes','No credential reads or account IDs retained','No model execution, fitting or scoring audit']}
    summary_path=OUT/'RUNTIME_BASELINE_SUMMARY_20260908.json'
    with summary_path.open('x',encoding='utf-8') as file:json.dump(summary,file,indent=2,ensure_ascii=False);file.write('\n')
    text=f'''# Research runtime baseline — 8 September 2026

The 15 expected research workers were alive with OS-confirmed process IDs, but combined price/news forecasting was blocked. This is a process and input baseline, not a successful trading or forecast-performance result. No runtime or trading changes were made.

| UTC observation | Joint H1 pairs | Price-only H1 pairs | Current tradeable prices | News evidence |
| --- | ---: | ---: | ---: | --- |
| 13:58:33–34 | 0/68 | 68/68 | 66/68 | Context only; age 228.8s |
| 14:04:29–30 | 0/68 | Withheld: summary-generation mismatch | 65/68 | Context only; age 195.0s |
| 14:06:46–48 | 0/68 | 68/68 | 66/68 | Stale; age 332.6s |

The supervisor, local HTTP API, quote transport and account observations were current. All supervised PIDs matched the OS in each observation. News collection advanced to a new cycle and published a new snapshot; joint summary/heartbeat clocks also advanced. Those current process clocks did **not** mean forecasts were progressing: joint counts stayed at 2,872 attempts, 2,864 publications/consumptions, 2,779 outcomes and 85 exclusions. Its cumulative worker errors remained 4 and heartbeat-publication errors 0. Price-only publications advanced 9,004 → 9,121 and outcomes 8,036 → 8,158, with worker errors 0.

Read-only latest-publication queries across all 68 registered joint-v2 ledgers found the last committed/consumed publication at **{latest['publication_utc']}** for {latest['instrument']}. Its original H1 target was **{latest['target_utc']}**; every pair's latest target had elapsed by this audit. No active combined forecast remained. The ledger check retained receipt/forecast hash equality and original clocks; it did not re-evaluate numerical performance.

The direct cause is a fresh guarded news payload with `status=unavailable` and `conflicting_current_topic_identity`. The bounded reproduction retains two unchanged decoded topic records from `local_news_sentiment/topics_latest.json`, whose cutoff exactly matches the guarded payload at **{conflict['guarded_snapshot']['as_of_utc']}**. Both frozen source bindings and full topic guard proofs were checked. Each record alone yields a valid **context-only** snapshot; their pair reproduces the exact rejection.

Both records share `{case['topic_id']}` and the same story ID. One is the TradingView version of “Wall St futures slip as oil surge puts markets on edge,” published 09:11:26 and first seen 09:21:25.896713 UTC, with 10 members. The other is the SRN News version, published 10:59:07 and first seen 11:10:59.161351 UTC, with 7 disjoint members. They differ in publisher, source groups and original clocks. This is a producer identity/grouping incompatibility with the snapshot's unique-identity contract; this case does not demonstrate conflicting market direction. The exact historical sequence forming the two groups was not replayed.

The source explains the mismatch boundary: `oanda_local_news_sentiment.py:18297` preserves already-clustered candidates independently; lines 18564–18579 derive a topic ID from signature, day and normalized claim, without the group member set. `oanda_news_causal_aggregation_guard_v1.py:320` requires repeated current IDs to identify an identical reguarded object. The history upsert already collapses shared-ID representatives, while the current joint payload consumes the uncollapsed published list. Choosing an arbitrary representative is not established as a safe repair: member causality, expiry and syndication deduplication must survive any reconciliation.

The first remediation should use the retained **108,490-byte** two-topic case in an isolated copy, resolve unique current topic identities upstream, and add regression checks for same-story variants, genuinely different claims, original clocks and independent-source counts. Preserve the guard's rejection of contradictory identity; do not fabricate neutral input or extend expired evidence. Any changed frozen producer/guard binding must be handled through a properly versioned study/input transition. No live repair was performed here.

Two separate limitations remain recorded. The price-only API's intervening generation mismatch recovered on the next bounded observation; the exact conflicting producer bytes at the failure instant were not captured, so this audit does not assign its root cause. News evidence also became older than 300 seconds while its collector stayed alive. Main-table context labels and joint-input availability are separate checks: context-only labels did not establish a valid globally sealed joint snapshot.

All four registered study configurations and their source bindings remained unchanged and matched. They retain research-only collection, with orders, promotion, authorization, account eligibility and proof eligibility disabled. The locally exposed account snapshot reported **zero positions** with current observation clocks. No account identifier, credential, direct broker call or order action was retained or performed; order-list contents were not audited.

Evidence: `RUNTIME_INITIAL_20260908.json`, `RUNTIME_FOLLOWUP_20260908.json`, `RUNTIME_RECOVERY_20260908.json`, `JOINT_LAST_PUBLICATION_20260908.json`, and `NEWS_TOPIC_IDENTITY_CONFLICT_CASE_20260908.json`. The JSON summary and receipt bind the observations and capture helpers. These are three discrete observations over {elapsed:.1f} seconds, not a continuous monitor. Raw large API/topic documents were hash-observed, not copied wholesale; the exact minimal conflicting topic records and guarded small payload are retained.
'''
    report_path=OUT/'RUNTIME_BASELINE_REVIEW_20260908.md'
    with report_path.open('x',encoding='utf-8') as file:file.write(text)
    evidence=names+['JOINT_LAST_PUBLICATION_20260908.json','NEWS_TOPIC_IDENTITY_CONFLICT_CASE_20260908.json','RUNTIME_BASELINE_SUMMARY_20260908.json','RUNTIME_BASELINE_REVIEW_20260908.md','capture_runtime_baseline.py','capture_news_conflict.py','capture_joint_last_publication.py','write_runtime_report.py']
    receipt={'schema':'revamp_runtime_baseline_receipt_v1_20260908','created_utc':datetime.now(timezone.utc).isoformat(),
        'result':'observation_complete_material_blocker_reproduced_not_repaired','source_sha256':first['source_sha256'],
        'source_files_unchanged':sources_unchanged,'registrations_unchanged':registrations_unchanged,'all_registered_source_bindings_match':all_bindings_match,
        'artifacts':[{'path':str(OUT/name),'bytes':(OUT/name).stat().st_size,'sha256':sha((OUT/name).read_bytes())} for name in evidence]}
    receipt_path=OUT/'RUNTIME_BASELINE_RECEIPT_20260908.json'
    with receipt_path.open('x',encoding='utf-8') as file:json.dump(receipt,file,indent=2);file.write('\n')
    print(json.dumps({'report':str(report_path),'report_sha256':sha(report_path.read_bytes()),'receipt':str(receipt_path),'receipt_sha256':sha(receipt_path.read_bytes()),'summary_sha256':sha(summary_path.read_bytes())},indent=2))
if __name__=='__main__':main()
