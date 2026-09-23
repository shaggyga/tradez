"""Assemble final evidence inputs after root verified source/test/runtime/UI checks."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
def load(p): return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def bound(name): return {'path':name,'sha256':sha(OUT/name)}

text=(OUT/'OPERATIONAL_STATUS_REPORT_DRAFT.md').read_text(encoding='utf-8')
text=text.replace('DRAFT — final source-bound tests, runtime, and UI observations pending. This file is staged outside the canonical project and must not be copied as the final report until those checks are complete.\n\n','')
text=text.replace('Actual final test, runtime, UI, and metric observations belong here after verification.',
    'The final combined 318-test suite and controlled runtime/desktop/mobile checks passed. These are engineering and display checks; predictive improvement remains unproven.')
text=text.replace('FINAL_METRICS_PENDING: insert only source-bound final observed counts and metrics, with original/raw versus filtered sample denominators and their exact observation times.', '''At the final API/ledger baseline recorded **18:33:11 UTC**, the pair study had 88 published and independently consumed forecast sets, 87 research entry-quote matches, 36 retained outcome rows, and three ledger exclusions. These are research records, not broker trades. Nineteen stored pair scorecards each had one paired scored decision at their own generation times (mostly around 18:18, with some around 18:30); they are not a re-score of all 36 outcome rows. Missing-target scorecard counts include pending original H1 targets and earlier observation cutoffs and must not all be described as failed outcomes.

The API baseline showed 16 forecasting pairs, 35 awaiting history/training, and 17 without fresh quotes, with zero pair-worker errors. News was explicitly stale at an analytical age of 302.938 seconds, despite a newer output timestamp. The later **18:35:13 UTC** desktop/mobile check showed 16 forecasting pairs, 43 warming and nine without fresh quotes. Its news evidence was 172 seconds old and EUR/USD was Short, with 18 related topics, one current directional topic and 13 topics carrying directional context. These changed observations demonstrate why every count and direction keeps its own clock; the earlier all-neutral audit is not a permanent assertion.

The final supplemental EUR/USD report was generated at **18:32:56 UTC**. All six original decisions now had outcome rows; excluding the two repeated-reference decisions left four retained and four scored decisions:

| Family | Direction correct | Positive after spread | Mean net return | Brier score |
| --- | ---: | ---: | ---: | ---: |
| State space | 3/4 (75%) | 3/4 (75%) | -0.8168 bps | 0.27697 |
| Ridge | 0/4 (0%) | 0/4 (0%) | -3.7195 bps | 0.52468 |

Both mean returns remained negative after spread. The fixed fair-coin probability baseline had Brier 0.25 on the same four decisions; the no-trade return baseline was zero. Four overlapping observations are insufficient to establish predictive quality, calibration, profitability or a model improvement. The original scorer still reported `duplicate_market_reference_epoch`; its original scorecard remained at 16:29:39 UTC and was visibly marked stale. The original companion's eight reported errors were retained, separately from the pair worker's zero errors.''')
text=text.replace("FINAL_VALIDATION_PENDING: insert the actual controlled reload outcome, preserved supervisor/other-worker identities, and final live/UI checks. Do not repeat the previous coverage report's counts as current counts.", '''The controlled dashboard-only reload passed at **18:32:59 UTC**, replacing the dashboard pair with PIDs **27724/11604**. Supervisor **22400** and all thirteen other workers' 26 process identities were preserved. The current API retained the unchanged registrations and disabled order/promotion flags.

Actual browser checks passed at widths **1440 and 360**, with zero JavaScript page errors and no page overflow. They exercised EUR/USD filtering, scoped news labels, the separately named companion, original scorer failure, and the supplemental denominators/results. The retained screenshots show a flat account with no open positions or pending orders. Two initial verifier issues were corrected without changing product source or repeating the reload: a case-sensitive assertion did not recognize CSS-uppercase “ORDERS DISABLED,” and a later assertion blindly toggled a details panel closed when persisted state had already opened it on mobile. `LIVE_UI_FIRST_FAILURE.json` preserves the first observed UI text and empty page-error list; the second verifier issue is documented here rather than mislabeled as a product failure. The final passing browser receipt and screenshots bind the tested source version.''')
text=text.replace('This record adds no second automation and does not claim that a future check has run.',
    'This record adds no second automation and does not claim that a future check has run. The initial runtime monitoring records are `data/oanda_training_manager/research_monitor/checks.jsonl` and `baseline_v1.json`; they are operational data and are excluded from the source-only export.')
assert not any(marker in text for marker in ('DRAFT —','FINAL_METRICS_PENDING','FINAL_VALIDATION_PENDING'))
final=OUT/'OPERATIONAL_STATUS_REPORT_FINAL.md'
with final.open('x',encoding='utf-8') as handle:handle.write(text)
test=load(OUT/'FINAL_SOURCE_TEST_BINDING.json')
sources={**test['final_implementation_source_sha256'],**test['test_source_sha256']}
assert all(sha(ROOT/name)==digest for name,digest in sources.items())
checks=[{'role':'runtime',**bound('runtime_reload/DASHBOARD_OPERATIONAL_RUNTIME_VERIFICATION_20260907.json'),'expected_status':'passed'},
        {'role':'ui',**bound('LIVE_OPERATIONAL_DASHBOARD_VALIDATION.json'),'expected_status':'passed'},
        {'role':'independent_review',**bound('DASHBOARD_INDEPENDENT_REVIEW_20260907.json'),'expected_status':'passed'},
        {'role':'tests',**bound('FINAL_SOURCE_TEST_BINDING.json'),'expected_status':'passed'}]
excluded_directories={'fixtures','fixtures_v2','fixtures_v3','fixtures_v4','staged','__pycache__','before_current_records'}
excluded_files={'OPERATIONAL_STATUS_REPORT_DRAFT.md','FINALIZATION_INPUTS.json','VAULT_EXPORT_VERIFICATION.json'}
extensions={'.py','.ps1','.cjs','.js','.json','.xml','.md','.png','.html'}
files=[]
for path in sorted(OUT.rglob('*')):
    if (path.is_file() and path.suffix in extensions and path.name not in excluded_files
            and not (set(path.relative_to(OUT).parts)&excluded_directories)):
        files.append(bound(path.relative_to(OUT).as_posix()))
baseline=load(OUT/'BOT_MONITOR_BASELINE_20260907.json')
ui=load(OUT/'LIVE_OPERATIONAL_DASHBOARD_VALIDATION.json')
manifest={'schema':'operational_status_finalization_inputs_v1',
    'status':'source_frozen_tests_runtime_and_ui_verified','prepared_utc':datetime.now(timezone.utc).isoformat(),
    'tested_source_sha256':sources,'primary_test_xml':bound('combined_tests.xml'),
    'passing_json_checks':checks,'evidence_files':files,'final_report':bound(final.name),
    'observed_metrics':{'baseline_observed_utc':baseline['observed_utc'],'pair_totals':baseline['totals'],
        'pair_coverage':baseline['pair_coverage'],'scorecard_pairs':baseline['scorecard_pairs'],
        'pair_scorecard_scope':'19 separately timestamped scorecards, each one paired scored decision; not all 36 outcome rows independently rescored',
        'baseline_news':baseline['news'],'baseline_eurusd_news':baseline['eurusd_news'],
        'supplemental_eurusd':baseline['supplemental_diagnostics'],
        'live_ui_observed_utc':ui['observed_utc'],'live_ui_widths':[row['width'] for row in ui['layouts']],
        'live_ui_pair_counts':{'forecast':16,'warming':43,'missing_fresh_quote':9},
        'live_ui_news':{'evidence_age_sec_displayed':172,'eurusd_direction':'SHORT','related':18,'current_directional':1,'context_directional':13},
        'live_ui_page_errors':ui['page_errors'],'independent_clock_snapshots_not_atomic':True}}
with (OUT/'FINALIZATION_INPUTS.json').open('x',encoding='utf-8') as handle:
    json.dump(manifest,handle,indent=2);handle.write('\n')
print(json.dumps({'final_report':str(final),'evidence_files':len(files),'tested_sources':len(sources),'status':manifest['status']}))
