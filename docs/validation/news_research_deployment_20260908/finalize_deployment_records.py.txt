"""Retain deployment observations and publish dated project/vault handoff records."""
from pathlib import Path
from datetime import datetime,timezone
import argparse,hashlib,json,sys,time

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent/'trad'
sys.path.insert(0,str(ROOT))
import oanda_joint_price_news_forecast_study_v3 as worker

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def read(path):return json.loads(path.read_text(encoding='utf-8-sig'))
def write(path,value):path.write_text(json.dumps(value,indent=2,allow_nan=False)+'\n',encoding='utf-8')

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--runtime',type=Path,required=True)
    parser.add_argument('--ledger',type=Path,required=True)
    parser.add_argument('--cadence',type=Path,required=True)
    args=parser.parse_args()
    runtime,ledger,cadence=map(read,(args.runtime,args.ledger,args.cadence))
    supervisor=runtime['supervisor'];joint=runtime['api']['selected_joint_study']
    assert supervisor['status']=='current' and supervisor['running_worker_count']==supervisor['expected_worker_count']==17
    assert joint['study_version']=='joint_v3' and joint['status']=='current' and joint['row_count']==68
    assert ledger['status']=='passed' and ledger['verified_ledgers']==68
    registry=worker.load_registry(worker.DEFAULT_CONFIG)
    assert joint['registry_sha256']==ledger['registry_sha256']==worker.digest(registry)
    assert ledger['source_bindings']==registry['source_bindings']
    assert runtime['os_processes']['reported_worker_pids_not_confirmed_in_os']==[]
    assert cadence['status']=='completed_readonly' and type(cadence['source_or_runtime_changes']) is int and cadence['source_or_runtime_changes']==0
    assert cadence['broker_calls']==cadence['database_connections']==0
    for name,expected in runtime['source_sha256'].items():assert sha(ROOT/name)==expected
    assert 0<=time.time()-datetime.fromisoformat(runtime['finished_utc']).timestamp()<=300
    activation=read(HERE/'DEPLOYMENT_ACTIVATION_20260908.json')['activation']
    selected=read(HERE/'PRIMARY_SELECTION_RECEIPT_20260908.json')
    assert selected['selection']['registry_sha256']==worker.digest(registry)
    assert joint['primary_selection']['selected']=='v3'
    assert joint['primary_selection']['registry_sha256']==selected['selection']['registry_sha256']
    assert joint['primary_selection']['activated_epoch']==selected['selection']['activated_epoch']
    assert read(ROOT/'config/joint_forecast_primary_current.json')==selected['selection']
    producer_record=runtime['heartbeats']['repaired_news_producer'];producer=producer_record['value']
    assert producer['source_status']==producer['status']=='current' and 0<=producer_record['generated_age_sec']<=90
    assert producer['research_only'] is True
    assert all(producer[flag] is False for flag in ('can_place_orders','can_promote','can_authorize','execution_eligible'))
    for name,item in runtime['registered_controls'].items():
        assert item['source_bindings']['all_match'] is True
        prior=read(HERE/'DEPLOYMENT_SOURCE_FREEZE_20260908.json')['prior_registries']
        assert item['sha256']==prior[Path(item['path']).name]['sha256']
    live_pids={item['pid']:item for item in runtime['os_processes']['processes']}
    for item in read(HERE/'SERVICE_RELOAD_RECEIPT_20260908.json')['preserve']:
        actual=live_pids[item['pid']]
        assert actual['created_utc']==item['created_utc'] and actual['script_path'].lower()==item['script'].lower()
    destination=ROOT/'docs/validation/news_research_deployment_20260908'
    paths=[args.runtime,args.ledger,args.cadence,
           HERE/'runtime/RUNTIME_INTEGRATION_BEFORE_20260908.json',
           HERE/'runtime/RUNTIME_AFTER_START_20260908.json',HERE/'runtime/RUNTIME_SELECTED_20260908.json',
           HERE/'runtime/RUNTIME_FINAL_20260908.json',HERE/'DASHBOARD_WORK_DEFERRED_20260908.json',
           HERE/'DEPLOYMENT_SOURCE_FREEZE_20260908.json',HERE/'DEPLOYMENT_ACTIVATION_20260908.json',
           HERE/'SERVICE_RELOAD_RECEIPT_20260908.json',HERE/'NEWS_CADENCE_RELOAD_RECEIPT_20260908.json',
           HERE/'PRIMARY_SELECTION_RECEIPT_20260908.json',HERE/'PRIMARY_SELECTION_INITIAL_WITHHELD_20260908.json',
           HERE/'LIVE_CAPTURE_CADENCE_DIAGNOSTIC_20260908.json',HERE/'SUPERVISOR_CADENCE_GATES_FINAL_20260908.xml',
           HERE/'LIVE_CAPTURE_CADENCE_AFTER60_20260908.json',
           HERE/'remaining_pair_input_triage/REMAINING_PAIR_CURRENT_INPUT_TRIAGE_20260908.json',
           HERE/'RUNTIME_SCOPE_FINAL_20260908.xml',HERE/'BEFORE_RECORDS_MANIFEST_20260908.json',
           HERE/'VAULT_MAPPING_TESTS_20260908.xml',
           HERE/'OPERATIONAL_SCOPE_FINAL_20260908.json',HERE/'FINAL_OPERATIONAL_INDEPENDENT_REVIEW_20260908.json',
           HERE/'FINAL_OPERATIONAL_INDEPENDENT_REVIEW_V2_20260908.json',
           HERE/'activate_research_v3.py',HERE/'reload_research_services.ps1',HERE/'reload_news_cadence.ps1',
           HERE/'select_research_v3_primary.py',HERE/'runtime/observe_repaired_runtime.py',
           HERE/'inspect_research_v3.py',HERE/'compare_capture_cadence.py',Path(__file__),
           *sorted(p for p in (HERE/'before_records').rglob('*') if p.is_file())]
    paths=list(dict.fromkeys(path.resolve() for path in paths))
    for path in paths:
        if not path.is_file():raise ValueError('required_evidence_missing:'+str(path))
        path.relative_to(HERE.resolve())
    before=read(HERE/'BEFORE_RECORDS_MANIFEST_20260908.json')['files']
    assert len(before)==8
    for row in before:assert sha(Path(row['copy']))==row['sha256']
    core=ROOT/'docs/validation/news_identity_integration_20260908/EVIDENCE_COPY_MANIFEST_20260908.json'
    assert sha(core)=='7252b16d8bedd3d0d7f7d4d5a95e94c2cb4d5f8858e0d8b8c3cc55e30e586e77'
    destination.mkdir(exist_ok=False)
    copies=[]
    for path in paths:
        rel=path.resolve().relative_to(HERE.resolve())
        target=destination/rel
        if target.suffix in ('.py','.ps1'):target=target.with_name(target.name+'.txt')
        target.parent.mkdir(parents=True,exist_ok=True)
        with target.open('xb') as f:f.write(path.read_bytes())
        assert sha(target)==sha(path)
        copies.append({'original':str(path),'path':target.relative_to(ROOT).as_posix(),'sha256':sha(path),'bytes':path.stat().st_size})
    manifest=destination/'DEPLOYMENT_EVIDENCE_MANIFEST_20260908.json'
    write(manifest,{'status':'verified_exact_copies','files':copies,'raw_news_and_databases_included':False})
    producer=runtime['heartbeats']['repaired_news_producer']['value']
    counts=ledger['counts'];coverage=joint['actual_unelapsed_forecast_pairs']
    observed=runtime['finished_utc']
    report=ROOT/'docs/FOREX_NEWS_RESEARCH_DEPLOYMENT_20260908.md'
    report.write_text(f'''# Repaired news study: deployment and live verification

Recorded observation: **{observed}**. This is dated evidence, not ongoing telemetry.

The separate repaired-news study is running and selected in the dashboard. The final local observation verified **{coverage}/68 pairs with unelapsed combined H1 forecasts**, **17/17 managed research workers**, and the new registry/source bindings. The independent read-only ledger inspection passed all **68** databases: **{counts['forecasts']} forecasts, {counts['publication']} publications, {counts['consumption']} consumptions, {counts['entries']} later research entries, {counts['outcomes']} completed outcomes, and {counts['exclusions']} exclusions**. A research entry is a selected bid/ask quote, not an executed position. Orders, promotion and authorization remain disabled.

The independent ledger observation completed at `{datetime.fromtimestamp(ledger['completed_epoch'],timezone.utc).isoformat()}`; its total issued decisions and the API's unique currently forecast pairs have different denominators and observation clocks.

## What was repaired

The fresh pre-deployment observation had **0/68** active joint-v2 forecasts. Its retained reasons included 39 history-row-limit failures, 24 stale-news failures, three closed/non-stream quotes and two stale quotes. This differed from the earlier morning topic-identity collision; both observations remain retained.

1. A separate producer reconciles compatible topic identities before the unchanged causal admission guard. It selects the complete relevant publication window in one read-only SQLite transaction, retains original member payloads/arrival clocks/expiry, and checks current collector progress and independently fresh clock evidence. Conflicting identities, duplicate members within a topic and stale/future observations still fail closed; compatible exact duplicates across variants collapse with their identity preserved. The classifier and original collector are unchanged.
2. A separate adapter replaces the exhausted 5,000-row ceiling with a complete, paged 48-hour read bounded by 10,000 rows, 128 MiB original wrappers, 1 MiB per row and 20 seconds. It never truncates the selected history. The final real probe read **5,508 events**, using **13,257,016 canonical bytes** and **2,394,116 compressed bytes**, within unchanged 16 MiB/8 MiB capture limits. These are finite capacity limits; continued growth remains monitored.
3. Verified immutable captures are shared while their source generation, governed database signatures and expiry remain valid. In the final 68-request probe there was one history read; median cache request time was 0.218 seconds. Source changes still invalidate the cache.
4. The live initial 15-second producer interval caused nearly one large capture per pair. The supervisor now uses the producer's existing **60-second** interval. The original 300-second news age and member expiry remain unchanged. In the first 208.9-second after window, five shared captures supported seven completed pair captures; files per completed pair fell from 1.059 to 0.714. The bytes/time rate was about 418.5 versus 197.6 MiB/hour when expressed in hourly units. The denominators differ: the before rate uses the span between captures, while the after rate includes startup time from reload completion. This is observed short-window churn, not a controlled 50% improvement or guaranteed long-run rate. Median pair-completion spacing worsened from 20.5 to 31.0 seconds in that first window; faster coverage was **not** demonstrated there. The extended comparison is also retained. The change is retained for lower capture/storage churn; processing speed remains open.

The subsequent 638.8-second cumulative window contained 44 pair captures (41 ready, three withheld), 17 shared files and 40,917,071 bytes. Median completion spacing was 10.871 seconds across 43 intervals, with 0.386 shared files per completed capture. This shows better observed reuse and spacing over the longer window; it is still not a matched controlled throughput experiment. The withheld inputs included one `future_news_mapping` and two stale/future pair references despite zero cumulative worker errors. A refusal to admit data is distinct from a worker exception.
5. The dashboard explicitly selects v3 by registry hash and actual selection time. Invalid selected evidence is withheld; old v1/v2 results remain separate. Runtime health expects 17 workers and storage inventories 420 managed databases. The retired watchlist's failed classification check now maps to its actual disabled producer. Its artifact check remains failed; its scope becomes active/unknown if that producer is running or unobserved.

## Registration and actual clocks

- Registry: `config/joint_price_news_study_v3_20260908.json`; canonical SHA-256 `{worker.digest(registry)}`.
- Empty ledger activation: `{datetime.fromtimestamp(activation['activation_started_epoch'],timezone.utc).isoformat()}` through `{datetime.fromtimestamp(activation['activation_completed_epoch'],timezone.utc).isoformat()}`. All 68 began empty, with no forecast or outcome imports.
- Primary dashboard selection: `{datetime.fromtimestamp(selected['selection']['activated_epoch'],timezone.utc).isoformat()}`. Selection time is separate from ledger activation.
- Study root: `data/oanda_training_manager/joint_price_news_study_v3`.
- Repaired producer output: `data/oanda_training_manager/local_news_sentiment_repair_v1/current_news_v1.json`.
- Exactly 20 source bindings identify the new cohort. The four prior 68-pair registries and their registered model/input/scoring sources still match their original hashes. Existing quote, account, original news and study processes were preserved during the targeted operational reloads.

The numerical model, 34-feature definition, matched price-only comparator, neutral-news ablation and original reference-plus-3,600-second target are unchanged. The repair does not activate the older broad-feature or multi-horizon engines.

## What the news results mean

The final real EUR/USD preflight fit was ready with 176 training examples and zero vetted directional-news rows. Its computed expected move was +0.98906 pips, with an uncalibrated p(up) of 0.54207; neutral-news ablation was also +0.98906 pips, while the separately fitted matched price-only model gave +0.32063 pips. That preflight was not issued into a study. Initial live examples also retained nonzero context inputs, which can change a joint estimate without a vetted directional claim. These comparisons demonstrate correct input use and model decomposition, not causal news impact or predictive edge.

The earlier frozen joint-v2 baseline remains **2,779 completed outcomes, 52.07% directional accuracy, 30.66% positive after spread, and −2.9693 mean net basis points**. Its magnitude and probability errors did not beat the stated zero-move/fixed-50% baselines. The new study's current forecasts cannot establish improved accuracy before their original targets and subsequent independent-session evaluation. Spread-net research outcomes are not realized account P/L.

## Verification and limits

Final focused batches: producer/reconciler **118 passed**; adapter/ledger **231 passed**; worker/preparation **113 passed**; dashboard/health/storage first integration **123 passed**; operational gates after the cadence change **97 passed**; expanded storage **27 passed**; final runtime scope **30 passed**; independent read-only inspector **29 passed**. Some batches overlap; do not add them as independent test cases.

The inspector independently checked stored contract/activation, input and forecast hashes, publication and consumption receipts, new cohort identities, exact later entry quotes and original H1 endpoints on coherent per-database read-only transactions. It does not claim a single atomic snapshot across 68 databases or re-score completed outcomes.

All intermediate source-transition failures, slow benchmarks and prior test receipts remain retained. One initial primary-selection preflight withheld before any pointer write; its exact rejected projection was not retained, so its root cause is not claimed. The following observation and bounded preflight verified current publications before selection.

A later API observation withheld all rows with `pair_summary_generation_mismatch` while the independent producer retained valid forecasts. The frozen worker's separate summary/heartbeat timers can temporarily expose different generations; an exact prior generation may not be in the display cache. Further dashboard work was deferred at the user's request. An unlaunched retry edit was restored to the exact preceding source hash; the display's binding checks remain unchanged. The failed API observation and following current observation remain retained, rather than portraying the service as continuously available.

Remaining pair statuses in the final API observation: `{json.dumps(joint['reason_counts'],sort_keys=True)}`. Forecast availability, quote currentness and completed evaluation are separate states. Producer cumulative errors at that observation: **{producer.get('errors')}**, last error `{producer.get('last_error','')}`.

The bounded 20:38 UTC input triage found GBP/NZD and NZD/HKD both ready on current actual inputs (175/165 mature labels, 144/129 nonzero-context rows, zero vetted-direction rows). Their separate fits took 8.55/1.60 seconds and were not issued into any live ledger. A pinned 5,551-row history snapshot had no invalid clocks, visibility after the sampled cutoff or effective time after visibility. The old GBP/NZD rejection's exact offending row/cutoff was not retained. An arrival between the retained cutoff and later database snapshot is a possible mechanism, not an established historical cause. These current successes indicate recoverable input/refresh gaps, not structurally absent models; improve bounded retries and retain specific rejected clock evidence in a future registered worker/input version. Three TRY instruments reported no tradeable stream quote. No quotes or missing bars were fabricated.

The first recovery and broad-model work remain in [pending improvements](../FOREX_PENDING_IMPROVEMENTS.md): restore compatible historical engines, repair duplicated cross features and pip-scale rankings in separate versions, evaluate the blurb/analogue data causally, improve calibration/cost and currency co-movement models, and compare technical-only/news-only/combined forecasts on matched independent time blocks. Capture storage growth, finite history capacity and slow recovery after transient input refusals remain core follow-ups. Dashboard generation races are deferred per the user's stated priority. No evidence retention deletion was introduced.

## Evidence

- [Deployment validation](../FOREX_NEWS_RESEARCH_DEPLOYMENT_VALIDATION_20260908.json)
- [Finalized implementation and test copies](validation/news_identity_integration_20260908/EVIDENCE_COPY_MANIFEST_20260908.json)
- [Actual deployment/runtime copies](validation/news_research_deployment_20260908/DEPLOYMENT_EVIDENCE_MANIFEST_20260908.json)
- [Original baseline and recovery](FOREX_REVAMP_BASELINE_RECOVERY_20260908.md)

The vault export contains source and compact evidence, not all private runtime databases, raw news or credentials. Cloud synchronization is separate from verifying local OneDrive bytes.
''',encoding='utf-8')
    operational=['oanda_always_on_supervisor.ps1','oanda_project_runtime_health.py','oanda_storage_headroom_guard.py',
                 'oanda_practice_live_dashboard.py','oanda_main_signal_dashboard.html',
                 'test_oanda_joint_news_v3_operational_gate.py','test_oanda_project_runtime_health.py',
                 'test_oanda_storage_headroom_guard.py','test_oanda_joint_price_news_dashboard_v3.py']
    validation={'schema_version':'forex_news_research_deployment_validation_20260908','status':'live_research_verified',
        'observed_utc':observed,'registry_sha256':worker.digest(registry),'source_bindings':registry['source_bindings'],
        'operational_source_bindings':{name:sha(ROOT/name) for name in operational},
        'registered_pairs':68,'active_unelapsed_forecast_pairs':coverage,'runtime_workers':17,
        'ledger_inspection_counts':counts,'runtime_reason_counts':joint['reason_counts'],
        'evidence_manifests':[{ 'path':p.relative_to(ROOT).as_posix(),'sha256':sha(p)} for p in (
            manifest,ROOT/'docs/validation/news_identity_integration_20260908/EVIDENCE_COPY_MANIFEST_20260908.json')],
        'report':{'path':report.relative_to(ROOT).as_posix(),'sha256':sha(report)},
        'research_only':True,'can_place_orders':False,'can_promote':False,'can_authorize':False,
        'prediction_improvement_demonstrated':False,'historical_forecasts_imported':False,
        'prior_source_registries_verified_unchanged':True,'actual_selection':selected['selection'],
        'cadence_observation':{'path':args.cadence.name,'sha256':sha(args.cadence)},
        'copy_scope':'Exact compact local evidence; no raw news, full captures, DBs or credentials exported.'}
    validation_path=ROOT/'FOREX_NEWS_RESEARCH_DEPLOYMENT_VALIDATION_20260908.json'
    write(validation_path,validation)
    # Preserve the previous current review explicitly; embedded earlier snapshots retain original dates.
    for name in ('FOREX_AUDIT_STATE_CURRENT.json','FOREX_COMMONS_CURRENT.json'):
        path=ROOT/name;state=read(path)
        state.setdefault('previous_latest_reviews',[]).append(state['latest_review'])
        state['latest_review']={'observed_utc':observed,'runtime_state':'repaired_joint_research_live_verified',
            'report':report.relative_to(ROOT).as_posix(),'validation':validation_path.name,'validation_sha256':sha(validation_path),
            'managed_worker_count':17,'registered_pairs':68,'active_unelapsed_forecast_pairs':coverage,'primary_study':'joint_v3',
            'can_place_orders':False,'practice_trading_ready':False,'prediction_improvement_demonstrated':False,
            'monitoring':'This turn observed live in chat; no schedule created. Earlier one-hour watch remains dated evidence.',
            'all_other_embedded_snapshots_retain_their_original_timestamps':True}
        write(path,state)
    queue=ROOT/'FOREX_PENDING_IMPROVEMENTS.md';old=queue.read_text(encoding='utf-8')
    queue.write_text(old.replace('# Forex pending improvements\n',f'''# Forex pending improvements

## Current — September 8 repaired-news research deployed; predictive acceptance pending

The separate v3 cohort is activated with 68 initially empty ledgers and selected in the live dashboard. The {observed} observation verified {coverage}/68 unelapsed combined forecasts and 17/17 research workers. The complete news-history read, topic-identity reconciliation, shared-capture cache, 60-second producer cadence and explicit version selection are implemented and live-verified. All original registrations/forecasts and scoring rules remain preserved. See [deployment and evidence](docs/FOREX_NEWS_RESEARCH_DEPLOYMENT_20260908.md).

Operational acceptance does not close predictive acceptance. Continue original-H1 outcome evaluation against matched price-only, neutral-news and fixed baselines across independent sessions; calibration, magnitude, spread costs and trading readiness remain unresolved. Broader 227/220/795/MA/second-ridge recovery, duplicated cross features, blurb/analogue integration and co-movement research remain open below. The new complete history has finite 10,000-row/128-MiB-wrapper limits and 16-MiB/8-MiB capture limits; monitor capacity and retained storage growth. No automatic evidence deletion was added. Continue investigating intermittent summary/heartbeat generation mismatches with exact failure bytes.

The prior 5,000-row and stale current-news publication failures are repaired in v3; they remain truthful limitations of the unchanged older cohorts. Historical watchlist/artifact failures remain failed, with inactive producers identified explicitly. Orders and promotion remain disabled. Earlier dated queue entries below are retained evidence.
''',1),encoding='utf-8')
    with (ROOT/'FOREX_PROJECT_LOG.md').open('a',encoding='utf-8') as f:
        f.write(f'\n\n## September 8 — repaired news research deployed and observed\n\nAt {observed}, the separate v3 registry was selected with {coverage}/68 current combined H1 forecast pairs and 17/17 research workers. The initial activation created 68 empty ledgers using actual clocks; no historical predictions were imported. Repaired topic admission, complete paged history and verified capture reuse passed their final tests and actual inputs. The operational producer interval is 60 seconds after a measured 15-second recapture problem; all expiry limits remain fixed. Source-bound validation: {validation_path.name}, SHA-256 {sha(validation_path)}. Original model/scorer contracts remain unchanged, and no order or promotion capability was enabled. New performance acceptance and broad-model recovery remain pending.\n')
    print(json.dumps({'report':str(report),'validation':str(validation_path),'validation_sha256':sha(validation_path),
                      'runtime_evidence_files':len(copies),'active_pairs':coverage,'outcomes':counts['outcomes']}))

if __name__=='__main__':main()
