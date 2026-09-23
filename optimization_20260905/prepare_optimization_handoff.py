"""Write the dated optimization log, report and portable supporting evidence."""
from datetime import datetime, timezone
from pathlib import Path
import json

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / 'trad'
stamp = datetime.now(timezone.utc).isoformat()
benchmark = json.loads((OUT/'integrity_publication_benchmark.json').read_text())
validation = ROOT/'docs/validation/optimization_20260906'
validation.mkdir(parents=True, exist_ok=True)
for name in ('integrity_publication_benchmark.json', 'integrity_publication_receipt.json',
             'INTEGRITY_PUBLICATION_OPTIMIZATION.md', 'integrity_consumer_review.json',
             'integrity_consumer_review.md', 'benchmark_integrity_publication.py',
             'run_publication_tests.py', 'run_consumer_tests.py', 'run_tests.py'):
    (validation/name).write_bytes((OUT/name).read_bytes())

backlog_path = ROOT/'FOREX_OPTIMIZATION_BACKLOG_20260906.json'
backlog = json.loads(backlog_path.read_text())
backlog['updated_utc'] = stamp
backlog['review'] = 'docs/FOREX_OPTIMIZATION_REVIEW_20260906.md'
for item in backlog['items']:
    if item['id'] == 'OPT-20260906-INTEGRITY-PUBLICATION':
        item['status'] = 'implemented_offline_validated'
        item['validation'] = 'FOREX_OPTIMIZATION_VALIDATION_20260906.json'
        item['measured_bytes'] = benchmark['bytes']
        item['live_validation'] = 'unverified; runtime intentionally stopped'
    elif item['id'] == 'OPT-20260906-PREDICTION-QUALITY':
        item['status'] = 'assessment_complete_research_unconfirmed'
        item['assessment'] = 'FOREX_PREDICTION_QUALITY_20260906.json'
        item['result'] = {'shadow_outcomes':8414, 'direction_hits':4156, 'positive_after_spread':716,
                          'calibration_replay_is_not_issue_time_proof':True, 'confirmed_hypotheses':0}
        item['next_work'] = [
            'Add exact-cohort prediction metrics, frozen same-decision baselines and normalized economics in a separate registered scorecard.',
            'Require all calibration training labels to have matured before the forecast issue; completed-outcome row ordering is insufficient.',
            'Keep the existing diagnostic replay and old outcomes separate from a new untouched prospective study.'
        ]
backlog_path.write_text(json.dumps(backlog, indent=2)+'\n', encoding='utf-8')

report = '''# Forex optimization and improvement log — September 6, 2026

Implemented the integrity-publication optimization identified by the September 5 performance audit, recorded the remaining priorities, and independently assessed the saved predictions. The actual project remains stopped. No production database, saved current integrity report, broker account, trading policy or frozen research cohort was changed by this optimization.

## Measured improvement

The compact publisher removes only `episode_rows` from five embedded alignment sections. All checks, failures, status, authority flags, contracts, counts, metrics and other existing fields remain at their original paths. The omitted arrays are retained in immutable files addressed by SHA-256, with schema, size and row-count validation. Reconstructing the saved report produces exactly the original JSON value.

| Saved-payload measurement | Before | After |
|---|---:|---:|
| Current report JSON | 98,926,690 bytes | 167,893 bytes |
| One appended history row | 73,356,296 bytes | 136,573 bytes |
| Snapshot + history publication, median | 3.326 seconds | 1.173 seconds with new artifacts; 1.079 seconds with unchanged detail |
| Summary decode, median | 422.254 ms | 0.759 ms |

The current summary is **99.83% smaller**. Repeated publication took **67.56% less time** in this bounded benchmark. Five retained detail files occupy **73,162,637 bytes**, in addition to the compact summary and history. They are reused when only summary clocks or metrics change. Changed rows create new immutable detail files; no historic artifact is automatically removed. This is not a claim that all project storage shrank by 99.83%.

The benchmark used the actual saved 98.9 MB report, five timing samples per operation and likely warm filesystem caches. First and repeat publication timings include row serialization, artifact verification and snapshot/history writes. Decode timing includes only reading the JSON value into memory. Database queries, audit computation, raw alignment input parsing, ownership-guard latency and complete runtime behavior were excluded. The earlier saved 16.125-second full audit has **not** been remeasured.

One separately instrumented sample reduced incremental Python allocation from 364,305,136 bytes for original summary serialization to 196,030,476 bytes for compact transformation plus serialization. This excludes the already resident input object and is not process RSS. The full standalone inputs are still read and validated.

## Publication, consumers and recovery

Artifacts are complete, flushed and verified before any references are published. Existing corrupted hash-addressed files cause failure and are preserved. The existing generation-ownership check still prevents an older audit pass from publishing. Current JSON, Markdown and JSONL remain separate filesystem operations; there is no new claim of a transaction across those files.

Available consumer source uses summary fields, while alignment validators read their existing full standalone inputs. Current checkpoint exports now capture the exact current snapshot bytes and its verified detail bytes together, including when the snapshot changes between file discovery and ZIP creation. Missing or corrupt dependencies abort export before replacing an existing checkpoint. Only referenced detail files are included.

To reconstruct a compact snapshot, use `oanda_integrity_publication.restore_integrity_details(payload, snapshot_dir=...)`, supplying the directory corresponding to `data/oanda_training_manager/state` in the original or restored project. References are relative to this trusted snapshot directory. Four-hour saved records carry an explicit project-relative source locator and can use `restore_snapshot_integrity(..., project_root=...)` after relocation. An archived historical JSONL needs the union of its own referenced detail files and this original reference base; exporting the current checkpoint alone does not promise a historical-log closure. No detail garbage collection or historical rewriting was added.

Source-only vault recreation still excludes runtime data. Its purpose is to preserve the reviewed implementation and records. The compact-current transitive export path is tested on disposable fixtures; the full runtime checkpoint job was not run for this handoff.

## Validation and evidence

The combined guarded offline suite passed **131 tests**, with **one Windows symlink-privilege skip**, across compact publication, integrity validation, exporter coherence, relocated recovery, the four-hour observer, canonical record sync and issue-register validation. The guard blocked workers, subprocesses, networking and SQLite outside disposable fixtures. Independent reviews examined both publisher and consumers. Ordinary path traversal and malformed-reference rejection passed despite the platform-specific symlink skip.

Exact reconstruction, unchanged non-row values, changing-clock reuse, retained historical artifacts, missing/corrupt metadata, publication faults, stale owners, concurrent export changes and copied-snapshot restoration were checked. The actual saved source report's hash remained unchanged. [The validation receipt](../FOREX_OPTIMIZATION_VALIDATION_20260906.json) binds reviewed source and evidence. [Detailed benchmark](validation/optimization_20260906/integrity_publication_benchmark.json) and [consumer review](validation/optimization_20260906/integrity_consumer_review.md) preserve scope and measurements.

The calculation and benchmark scripts under `docs/validation` are preserved records of this local run. Copy them to a disposable review directory before reproduction and retain their documented original input paths; they are not runtime entrypoints. Original runtime inputs are not all in the source-only vault export. Prior dated audit, repair and performance receipts remain unchanged.

## How good were the predictions?

**No reliable useful edge has been demonstrated.** The broad current strict-horizon shadow diagnostic got direction right on **4,156 / 8,414 outcomes (49.39%)**; **716 / 8,414 (8.51%)** had a positive executable result after spread. These are correlated simulated signal outcomes, not an account's live-trade win rate or a single score for every model.

Equation calibration improved replay direction to 52.34% globally and 51.57% by pair. However, completed-outcome ordering does not establish that the calibration labels were available before each forecast was issued. These are diagnostic replay scores, not verified forward prediction accuracy. Global Brier 0.249301 is only slightly better than the constant-50% reference of 0.25; pair Brier 0.250504 is worse. Neither substitutes for a frozen baseline evaluated on the same decisions.

The four current model families have negative saved after-cost means and only 8–9 effective episodes each. Their exact-cohort directional hit rates are absent. Historical movement-clearance models show some discrimination, but selected no trades under the strict allocation rules and remain inspected-archive discovery. Current source-news/rank prospective evidence and confirmed hypotheses are zero. [The prediction assessment](FOREX_PREDICTION_QUALITY_20260906.md) separates cohorts, horizons, costs, baseline limitations and causality; [its receipt](../FOREX_PREDICTION_QUALITY_20260906.json) retains denominators and input hashes.

## Remaining improvements

[The dated backlog](../FOREX_OPTIMIZATION_BACKLOG_20260906.json) records what was implemented and what remains: profile expensive queries on coherent realistic read-only fixtures; measure storage growth and verified retention candidates; profile duplicate source work and useful per-source yield; and create a separate time-causal prediction scorecard with frozen baselines and independent after-cost evidence. No model was selected or tuned from the favorable slices of these already-inspected results.

Live timing, a complete clean integrity cycle, source V9/rank V8 activation and new prospective prediction evidence remain unverified. Source V9/rank V8 are still disabled. Forex autostart remains disabled and the original stopped-state requirement remains in force.
'''
(ROOT/'docs/FOREX_OPTIMIZATION_REVIEW_20260906.md').write_text(report, encoding='utf-8')

def insert_before(path, marker, text):
    content = path.read_text(encoding='utf-8')
    if text in content:
        return
    assert marker in content, (path, marker)
    path.write_text(content.replace(marker, text+'\n'+marker, 1), encoding='utf-8')

insert_before(ROOT/'FOREX_PROJECT_LOG.md', '## 2026-09-05 — independent audit repairs', '''## 2026-09-06 — compact integrity publication and prediction assessment, runtime stopped

Implemented compact integrity summaries with immutable, hash-verified episode
detail; preserved every check and other existing value. Checkpoint exports pin
the current snapshot and verified detail together, and saved four-hour records
carry a portable source locator. No current runtime snapshot was rewritten.

On the saved 98.9 MB payload, the current summary is 167,893 bytes and repeat
publication median falls from 3.326 to 1.079 seconds. Five detail artifacts
retain 73,162,637 bytes. This is publication overhead, not a full-cycle timing.
The combined guarded suite passed 131 tests with one Windows symlink skip.
Source and evidence bindings: `FOREX_OPTIMIZATION_VALIDATION_20260906.json`.

Prediction review: 4,156 of 8,414 current shadow outcomes had correct gross
direction (49.39%); 716 were positive after spread (8.51%). Calibration replay
is not verified original-issue-time accuracy. No reliable useful edge is
demonstrated. Exact cohorts, baselines and limitations are recorded in
`docs/FOREX_PREDICTION_QUALITY_20260906.md` and its dated JSON receipt.

The user-requested improvement log is `FOREX_OPTIMIZATION_BACKLOG_20260906.json`.
Query profiling, storage/retention assessment, useful source yield and new
time-causal predictive evidence remain pending. No worker, supervisor,
dashboard, broker action or source successor was activated. Prior dated
audit/repair records and source archives are preserved.
''')
insert_before(ROOT/'FOREX_PENDING_IMPROVEMENTS.md', '## September 5 —', '''## September 6 — measured reporting optimization and prediction review

Completed offline: compact integrity publication and recoverable immutable
detail, coherent checkpoint dependency export, and copied-observer reference
location. The saved summary falls from 98.9 MB to 168 KB; full detail is retained.
Combined validation: 131 passed, one Windows symlink skip. See
`docs/FOREX_OPTIMIZATION_REVIEW_20260906.md` and its dated validation receipt.

Prediction assessment is complete; predictive confirmation is not. Current
shadow direction is 49.39% and positive-after-spread rate 8.51% over 8,414
correlated outcomes. The ~52% calibration replay lacks original-issue-time
label availability enforcement. Preserve it as a diagnostic; a new study must
enforce matured-label cutoffs, frozen baselines and independent evaluation.

`FOREX_OPTIMIZATION_BACKLOG_20260906.json` keeps query profiling, storage-growth
and retention assessment, useful source yield and a complete prospective
scorecard pending. The complete live audit and future source V9/rank V8
activation remain unverified. Forex remains intentionally stopped.

The September 5 section below is the preserved pre-optimization queue state.
''')
insert_before(ROOT/'README.md', 'September 5 repair', '''September 6 optimization: the saved integrity summary shrinks from 98.9 MB
to 168 KB while retaining full detail separately; 131 offline tests passed
(one Windows symlink skip). See [optimization results](docs/FOREX_OPTIMIZATION_REVIEW_20260906.md),
[prediction quality](docs/FOREX_PREDICTION_QUALITY_20260906.md) and
[remaining priorities](FOREX_OPTIMIZATION_BACKLOG_20260906.json).
Current shadow signals had 49.39% directional accuracy and 8.51% positive
outcomes after spread. No reliable useful edge is demonstrated. Forex remains
stopped; these are source and saved-data findings.
''')
insert_before(ROOT/'FOREX_AUDIT_START_HERE.md', 'September 5 update:', '''September 6 update: begin with `OPTIMIZATION_REVIEW_CURRENT.md`,
`OPTIMIZATION_VALIDATION_CURRENT.json`, `PREDICTION_QUALITY_CURRENT.md`,
`PREDICTION_QUALITY_CURRENT.json` and `OPTIMIZATION_BACKLOG_CURRENT.json`.
Those are vault names; corresponding source files are
`docs/FOREX_OPTIMIZATION_REVIEW_20260906.md`,
`FOREX_OPTIMIZATION_VALIDATION_20260906.json`,
`docs/FOREX_PREDICTION_QUALITY_20260906.md`,
`FOREX_PREDICTION_QUALITY_20260906.json` and
`FOREX_OPTIMIZATION_BACKLOG_20260906.json`.

The measured improvement reduces repeated integrity-publication overhead while
preserving full evidence separately. Current shadow direction was 49.39%, with
8.51% positive after spread; this is not a live-trade win rate. No reliable edge
is demonstrated. All measurements use saved data and isolated fixtures.
The runtime remains stopped. The following dated records remain historical.
''')
insert_before(ROOT/'docs/AUDIT_STATE_CURRENT.md', '**September 5 source-repair update:**', '''**September 6 optimization update:** [the reporting optimization](FOREX_OPTIMIZATION_REVIEW_20260906.md)
and [prediction assessment](FOREX_PREDICTION_QUALITY_20260906.md) add measured
source improvements and an explicit scorecard. The original stopped-state
runtime JSON below and saved degraded integrity publication were not changed.
The compact publisher has offline validation; live timing remains unverified.
See `FOREX_OPTIMIZATION_VALIDATION_20260906.json` and the current backlog.
''')
print('Prepared dated optimization report, permanent evidence, project log, queue and source/vault guides.')
