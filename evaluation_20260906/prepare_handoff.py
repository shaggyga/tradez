"""Record the fixed offline comparison and preserve its reproducible evidence."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import xml.etree.ElementTree as ET

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
DEST=ROOT/'docs/validation/fixed_evaluation_20260906'
DEST.mkdir(parents=True,exist_ok=True)
def read(path):return json.loads(path.read_text(encoding='utf-8-sig'))
def write(path,value):path.write_text(json.dumps(value,indent=2,allow_nan=False)+'\n',encoding='utf-8')
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
stamp=datetime.now(timezone.utc).isoformat()
archive=read(OUT/'data/archival_scorecard.json')
strict=read(OUT/'strict_evaluation_report.json')
availability=read(OUT/'data/availability_diagnostic.json')
independent=read(OUT/'archival_independent_verification.json')
suite=ET.parse(OUT/'integration_tests.xml').find('testsuite').attrib
assert suite['tests']=='158' and all(suite[k]=='0' for k in ('errors','failures','skipped'))
assert strict['coverage']['total_forecasts']==1628 and strict['coverage']['paired_scored_decisions']==0
assert independent['verified'] and independent['paired_identical_endpoints']==368
for item in availability['local_evidence_artifacts'] if 'local_evidence_artifacts' in availability else []:
    assert sha(OUT/'data'/item['path'])==item['sha256']
for item in availability['source_code_and_registry']:
    assert sha(ROOT/item['path'])==item['sha256']

data_names=('AVAILABILITY_REVIEW.md','availability_diagnostic.json','inventory.json','fixed_cohort_contracts.json',
            'selected_candidate_rows.json','normalized_forecasts.json','archival_scorecard.json',
            'archive_product_bindings.json','normalize_and_score_archive.py','inventory.py','finish_inventory.py',
            'complete_availability_metadata.py')
for name in data_names:(DEST/name).write_bytes((OUT/'data'/name).read_bytes())
for relative in ('strict_input_final.json','strict_evaluation_report.json','archival_independent_verification.json',
                 'verify_archival_results.py','integration_tests.xml','run_tests.py','stopped_before_evaluation.json',
                 'baselines/BASELINE_VALIDATION.json','baselines/integration_tests.xml',
                 'scorer_tests/INDEPENDENT_SCORER_REVIEW.md','scorer_tests/integration_tests.xml'):
    target=DEST/relative;target.parent.mkdir(parents=True,exist_ok=True)
    target.write_bytes((OUT/relative).read_bytes())

results={'schema_version':'forex_fixed_evaluation_results_v1','generated_utc':stamp,
    'protocol':'config/fixed_forecast_evaluation_v1_20260906.json',
    'classification':'already_inspected_archive_diagnostic_and_strict_availability_check',
    'forex_stopped':True,'proof_eligible':False,'account_eligible':False,
    'archive_population':archive['fixed_population'],
    'archive_class_balance':archive['paired_outcome_class_balance'],
    'archive_metric_definitions':archive['metric_definitions'],
    'archive_family_metrics':{f:{k:v for k,v in row.items() if k!='observations'} for f,row in archive['family_metrics'].items()},
    'strict_evaluation':{k:v for k,v in strict.items() if k not in ('exclusions','decisions')},
    'availability':availability['strict_clock_eligibility_counts'],
    'outcome_producers':availability['outcome_producer_counts'],
    'stored_direction_probability_disagreements':independent['stored_direction_vs_probability_side_mismatches'],
    'interpretation':['All four archived probabilities have worse Brier than constant0.5 on the368paired stored endpoints.',
        'All four archived signed-return forecasts have greater MAE than zero change on exactly those endpoints.',
        'All four stored bid/ask results are negative; these prices do not establish executable entries after publication.',
        'Strict evaluator accepts0of407shared decisions because required availability evidence is absent.',
        'Neither the archive nor new fixture tests establish independent predictive confirmation.'],
    'evidence_root':'docs/validation/fixed_evaluation_20260906'}
write(ROOT/'FOREX_FIXED_EVALUATION_RESULTS_20260906.json',results)

followup={'schema_version':'forex_fixed_evaluation_followup_v1','updated_utc':stamp,'runtime_disposition':'intentionally_stopped',
    'completed':['Fixed all four current families/EUR_USD/one-hour scope and exact existing cohorts/versions',
                 'Preserved1628original forecasts and nullable outcomes; independently checked368paired archived endpoints',
                 'Built strict offline scorer, same-decision baselines, target/reference binding and original-side preservation',
                 'Verified causal rolling-label filtering, rejection cases and portable evidence with158tests'],
    'pending':[{'id':'FX-20260906-FOUR-FAMILY-ENTRY-TARGET-AVAILABILITY','priority':'P1','status':'open',
        'required':'Separate successor collection must publish actual postcommit forecast availability and feature/training-label availability; use a subsequent executable quote while retaining the original forecast target.',
        'historical_handling':'Preserve existing cohorts and results as diagnostics; never infer missing clocks or backfill old proof.'},
        {'id':'FX-20260906-CALIBRATION-REPLAY-ISSUE-CLOCK','priority':'P1','status':'open',
        'required':'Legacy calibration replay still lacks original-issue-time label availability. The new causal baseline helper supplies a tested separate evaluation path; it does not repair or activate the legacy producer.'},
        {'id':'FIXED-EVALUATION-FRESH-PROSPECTIVE-STUDY','status':'not_started',
        'required':'A new immutable contract and untouched future sample with the required clocks, baseline comparisons and independent evidence; no restart or collection was authorized by this offline evaluation.'}],
    'other_optimization_priorities':'FOREX_OPTIMIZATION_BACKLOG_20260906.json',
    'no_parameter_tuning':True,'no_model_selected':True,'no_collection_activation':True}
write(ROOT/'FOREX_FIXED_EVALUATION_FOLLOWUP_20260906.json',followup)

report='''# Fixed EUR/USD forecast evaluation — September 6, 2026

The narrower test is implemented and was applied to a preserved extract of the actual project. On **368 matching archived observations**, all four existing models scored worse than simple probability and price-change baselines and had negative stored bid/ask results. **Zero of the 407 shared forecast decisions qualifies for the new strict causal comparison**, because original availability timestamps are missing. Forex remains stopped.

This is a bounded engineering evaluation of already inspected history. It does not establish prospective predictive skill or realizable trading performance. The new scorer cannot promote a strategy or start collection.

## Fixed scope and actual results

The comparison includes all four existing active families, EUR/USD, M1 inputs and the existing one-hour forecast horizon. Exact cohort and predictor/feature version identities are fixed in `config/fixed_forecast_evaluation_v1_20260906.json`. The window covers the active cohorts through shutdown, with actual extracted observations from August 30 through September 4. No family, horizon, threshold or parameter was chosen for a favorable result.

There are **1,628 forecasts: 407 per family**, with 1,543 recorded outcomes and **85 missing outcomes retained**. Of 407 original reference epochs, 22 lack one or more outcomes and 17 have differing endpoint clocks or prices. The remaining **368 epochs have identical entry and endpoint prices and clocks across all four families**. They overlap and span only six UTC days; this is not 368 independent trials.

| Original family | Direction correct | Probability Brier | Signed price-change MAE | Stored quote net, mean |
|---|---:|---:|---:|---:|
| Graph transfer | 42.39% | 0.3615 | 5.555 pips | −1.930 pips |
| Tabular | 47.55% | 0.2738 | 4.436 pips | −2.328 pips |
| State space | 47.83% | 0.3868 | 7.722 pips | −1.759 pips |
| Ridge | 44.29% | 0.3784 | 6.825 pips | −1.673 pips |
| Constant 50% probability | No direction | **0.2500** | — | No trading action |
| Zero price change | Neutral | — | **3.934 pips** | No trading action |
| No trade | No direction | — | — | **0 pips** |

Lower Brier and MAE are better. Brier compares the emitted up probability against whether the midpoint increased; flat is non-up and is also reported separately. Directional hits use the original emitted direction and count flats as misses. Price-change error uses the original signed expected movement. Mean stored spread drag is 1.645 pips. These values concern one pair, so there is no mixed-pair pip averaging here, but they are still not dollar returns or a portfolio equity curve.

The complete archive also has 82 forecasts whose emitted direction differs from the side favored by a 50% probability threshold. This can reflect a difference between expected return and movement probability. We retain both rather than silently replacing the original trading direction. No reversal, calibration fitting, threshold tuning or winning-family selection was performed.

## Why none is strict causal evidence

Every stored entry quote precedes forecast recording. Recording is a pending-list timestamp before database flush, not a certificate of consumer-visible publication. The median recording delay from the original reference is **84.63 seconds**, with a maximum approximately **2 hours 58 minutes**. No record supplies the required original issue, postcommit availability, feature first-availability, or training-label maturity/availability maxima.

The outcome clocks are also inconsistent with one fixed original target. The canonical worker produced **1,483 outcomes** at recording time plus one hour while retaining the earlier entry quote. Another **60 recovered outcomes** use a different provider-clock convention and lack local quote receipt times. Of the 368 matching archive epochs, 353 use the canonical convention and 15 the recovered convention. Their stored outcomes can be compared descriptively across models, but cannot be relabeled into a verified original-reference one-hour test.

These defects are recorded in the new open P1 `FX-20260906-FOUR-FAMILY-ENTRY-TARGET-AVAILABILITY`. Existing source and cohorts were preserved. The evaluator explicitly rejects missing provenance; it never substitutes recording time, row order, file mtime or a data cutoff for availability. Correcting these measurement gaps would not by itself prove that the models predict well.

## Implemented evaluation behavior

`oanda_fixed_forecast_evaluation.py` is a standalone, offline JSON evaluator. It requires all four exact cohorts and versions at a common decision, original issued/publication clocks, features available by issue, and training labels both mature and available strictly before issue. Every forecast binds its original reference midpoint and unchanged target. The economic comparison uses the first eligible executable quote after all four publications and exits at the original target within the fixed tolerance; entry delay cannot extend the forecast horizon.

The evaluator preserves original sides and abstentions. It reports coverage and explicit exclusions before scores. It calculates probability Brier, directional hits, signed-return MAE/RMSE, executable net in normalized basis points, and fixed extra cost stress of 0, 0.5 and 1.0 bps. It never sums overlapping simulated positions into portfolio performance or treats calendar blocks as independent sample size.

`oanda_causal_prediction_baselines.py` supplies constant 50%, zero move, no trade and a rolling class-rate baseline. The rolling baseline uses at most 100 unique prior matching market outcomes, with Beta(1,1) smoothing and a minimum of 20 labels; earlier or insufficient history falls back to 50%. Both maturity and availability must strictly precede the earliest original issue in the paired comparison. Repeated labels from four model families count once. Contradictory duplicates fail.

The rolling baseline is tested on controlled fixtures but **unavailable for this historical archive**, because its label-availability evidence is absent. Historical momentum is also unavailable without the original price windows and availability clocks. No timestamps or baselines were fabricated to fill these gaps. The strict run reports 1,628 forecasts, 407 shared decisions and **zero scored pairs**.

## Verification and reproduction

The combined guarded suite passed **158 tests**, with zero failures or skips. It covers future or simultaneous training labels, row-order changes, original target preservation, delayed/missing publication, price-anchor mismatches, side/probability disagreement, flat outcomes, abstentions, malformed quotes, conflicting identifiers, paired arithmetic, coverage, adapter provenance, canonical record sync and issue validation. Independent reviewers examined the scorer and data extraction. Root independently recomputed the 368-observation archive results and all 1,628 forecast payload hashes.

The source database was queried using an index, URI `mode=ro`, `query_only` and a single read transaction. No logical write, checkpoint, broker request or runtime start occurred. Double SHA-256 checks of the approximately 59.8 GB main database and 712 MB WAL matched; sizes and modification times stayed unchanged. SQLite may maintain ephemeral reader marks in SHM, whose byte identity is not claimed. The bounded query took about 1.1 seconds; complete duplicate hashing took about 307 seconds. Those are audit extraction costs, not runtime performance measurements.

The source archive includes the bounded original extract, normalized records, complete archive calculations, strict input/output, source/hash bindings and test evidence under `docs/validation/fixed_evaluation_20260906`. It excludes the full production database and credentials. Earlier dated audit, optimization and prediction receipts remain unchanged.

For offline reproduction from the source root, use a new output path:

```powershell
python -B oanda_fixed_forecast_evaluation.py --input docs/validation/fixed_evaluation_20260906/strict_input_final.json --protocol config/fixed_forecast_evaluation_v1_20260906.json --output ../evaluation_reproduction/strict_result.json
```

Expected result: `no_clock_valid_paired_decisions`, 407 shared decisions, zero scored. This verifies the preserved archive is rejected consistently; it does not produce new market data. To reproduce the archival arithmetic, copy `selected_candidate_rows.json` and `normalize_and_score_archive.py` to a disposable folder and run that script there. The source-database inventory scripts are historical acquisition records and are unnecessary for this replay.

## Remaining work and operating state

The evaluation code and archival comparison are complete. A separately versioned producer must capture true forecast visibility, feature/training-label availability, a subsequent executable entry quote and one original target before a new strict sample can be collected. Legacy calibration's issue-time availability issue also remains open; the new baseline helper does not modify that producer. A fresh prospective study must preserve these fixed comparisons and independent evidence requirements.

`FOREX_FIXED_EVALUATION_FOLLOWUP_20260906.json` records these prerequisites. `FOREX_FIXED_EVALUATION_VALIDATION_20260906.json` binds the reviewed source and evidence. The new configuration has collection, proof and account eligibility disabled. Forex workers and autostart remain stopped; no strategy was activated.
'''
(ROOT/'docs/FOREX_FIXED_EVALUATION_20260906.md').write_text(report,encoding='utf-8')

def before(path,marker,text):
    content=path.read_text(encoding='utf-8')
    if text in content:return
    assert marker in content,path
    path.write_text(content.replace(marker,text+'\n'+marker,1),encoding='utf-8')
before(ROOT/'FOREX_PROJECT_LOG.md','## 2026-09-06 — compact integrity', '''## 2026-09-06 — fixed EUR/USD baseline evaluation, runtime stopped

Accepted the user's request for a small fixed forecast comparison. Preserved
all four active families/EUR_USD/one-hour cohorts and versions, including 1,628
forecasts and 85 missing outcomes. On 368 identical stored endpoints, all four
models have worse Brier than constant 50%, worse signed-move MAE than zero
change, and negative stored bid/ask results. These are archival diagnostics.

Built a separate strict offline evaluator and causal rolling baseline helper.
The original reference price, target and emitted direction remain bound;
training-label maturity and availability must precede original issue. Missing
clocks are rejected. The actual archive yields zero strictly scored pairs.
Combined guarded validation: 158 passed, no failures or skips.

Registered P1 FX-20260906-FOUR-FAMILY-ENTRY-TARGET-AVAILABILITY: all stored entry
quotes predate recording, publication/feature/label availability evidence is
missing, and outcome producers use different target/quote clock conventions.
No historical receipt or cohort was backfilled. The legacy calibration P1
remains open; future collection needs a separate correctly timestamped producer.

Read-only indexed acquisition left main database/WAL hashes, sizes and mtimes
unchanged; SQLite's ephemeral SHM reader marks are not claimed unchanged.
The narrow extract and calculations are preserved for offline reproduction.
No model was selected, tuned, retrained, promoted, started or traded.
See docs/FOREX_FIXED_EVALUATION_20260906.md and its dated validation receipt.
''')
before(ROOT/'FOREX_PENDING_IMPROVEMENTS.md','## September 6 — measured reporting', '''## September 6 — fixed comparison completed; fresh evidence prerequisites

Completed: fixed four-family EUR/USD one-hour archive comparison, strict offline
scorer and issue-time causal baselines, 158 passing tests and reproducible data.
All four models underperform naive probability/magnitude baselines on 368
matching stored endpoints. Zero of 407 shared decisions has sufficient clocks
for strict causal scoring. See docs/FOREX_FIXED_EVALUATION_20260906.md.

P1 FX-20260906-FOUR-FAMILY-ENTRY-TARGET-AVAILABILITY remains open: a separate
producer must record actual publication and feature/label availability, bind
the original target, and obtain an executable quote afterward. Preserve old
records as diagnostics. The separate calibration replay issue remains open.
Fresh prospective collection and runtime restart remain unrequested; Forex is
stopped. FOREX_FIXED_EVALUATION_FOLLOWUP_20260906.json is the current checklist.

Earlier September 6 entries below remain dated snapshots of prior work.
''')
before(ROOT/'README.md','September 6 optimization:', '''The fixed EUR/USD evaluation is complete: all four models underperform simple
baselines on the matching archived prices, while missing availability clocks
prevent strict causal scoring. Read [the fixed comparison](docs/FOREX_FIXED_EVALUATION_20260906.md)
and [remaining prerequisites](FOREX_FIXED_EVALUATION_FOLLOWUP_20260906.json).
The new offline evaluator passed 158 combined tests. Forex remains stopped.
''')
before(ROOT/'FOREX_AUDIT_START_HERE.md','September 6 update: begin', '''Latest September 6 follow-up: begin with `FIXED_EVALUATION_CURRENT.md`,
`FIXED_EVALUATION_RESULTS_CURRENT.json`, `FIXED_EVALUATION_VALIDATION_CURRENT.json`,
`FIXED_EVALUATION_PROTOCOL_CURRENT.json` and `FIXED_EVALUATION_FOLLOWUP_CURRENT.json`.
These are vault names. In source, use `docs/FOREX_FIXED_EVALUATION_20260906.md`,
the corresponding `FOREX_FIXED_EVALUATION_*_20260906.json` records and
`config/fixed_forecast_evaluation_v1_20260906.json`.

All four fixed EUR/USD families underperform simple baselines on 368 matching
archived endpoints. All 407 shared decisions lack the original availability
evidence for strict causal scoring. A new P1 entry/target-clock issue is open;
the earlier calibration issue remains open. The new evaluator passed 158 tests.
No live producer, collection or trading was started. Prior records follow.
''')
before(ROOT/'docs/AUDIT_STATE_CURRENT.md','**September 6 optimization update:**', '''**September 6 fixed-evaluation update:** [the EUR/USD comparison](FOREX_FIXED_EVALUATION_20260906.md)
adds a fixed baseline assessment, strict offline scoring and a newly documented
entry/target availability issue. All four models lose to naive baselines on the
matching stored prices; missing clocks prevent causal scoring. No original
runtime snapshot was rewritten or worker started. Use the current issue register
and FOREX_FIXED_EVALUATION_FOLLOWUP_20260906.json for the remaining prerequisites.
''')
print(json.dumps({'results':str(ROOT/'FOREX_FIXED_EVALUATION_RESULTS_20260906.json'),
                  'preserved_evidence_files':sum(p.is_file() for p in DEST.rglob('*')),
                  'tests':158,'strict_scored_pairs':0,'archival_paired_observations':368}))
