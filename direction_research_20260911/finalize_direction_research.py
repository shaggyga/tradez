"""Publish the completed research result and append its project record."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil

ROOT=Path(__file__).resolve().parent
PROJECT=ROOT.parent/'trad'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    result=json.loads((ROOT/'evaluation_001/RESULTS.json').read_text())
    prepared=json.loads((ROOT/'prepared_001/PREPARED.json').read_text())
    recreation=json.loads((ROOT/'review/SAVED_MODEL_RECREATION.json').read_text())
    if not recreation['verified'] or recreation['models_verified']!=96:
        raise ValueError('completed_recreation_required')
    stamp=datetime.now(timezone.utc).isoformat()
    fmt=lambda value,digits=3:'—' if value is None else f'{value:.{digits}f}'
    percent=lambda value:'—' if value is None else f'{100*value:.2f}%'
    rows=[]
    for horizon in map(str,prepared['spec']['horizons_minutes']):
        arms=result['aggregate'][horizon]
        for arm,metrics in arms.items():
            rows.append(f"| {horizon}m | {arm} | {metrics['rows']:,} | {percent(metrics['accuracy'])} | {percent(metrics['balanced_accuracy'])} | {fmt(metrics['brier'],4)} | {fmt(metrics['mean_bid_ask_close_net_bps'])} |")
    dayrows=[]
    for fold in result['folds']:
        if fold['status']!='completed':continue
        for kind in ('logistic','hgb'):
            a=fold['metrics'][kind+'__combined']; b=fold['metrics'][kind+'__technical']
            dayrows.append(f"| {fold['fold']} | {fold['horizon_minutes']}m | {kind} | {percent(a['accuracy'])} | {fmt(100*(a['accuracy']-b['accuracy']),2)} | {fmt(a['brier']-b['brier'],4)} | {fmt(a['mean_bid_ask_close_net_bps'])} |")
    report='''# Direction research — September 11 evening build

The missing shared feature comparison is now implemented and completed. It fits direct up/down learners on the same price, peer-currency and originally known news rows, at 5, 15, 30 and 60 minutes. The existing active H1 learner and trading configuration were not changed.

The first results show modest directional information, not a trading edge. The 15-minute combined gradient-boosting arm achieved **52.48% direction accuracy**, versus **52.35% technical-only** on the same rows. Its Brier score was **0.24952** versus **0.25005**, and mean bid/ask endpoint net was **−2.983 bps**. Added inputs did not help consistently across horizons or dates; all32learned arm/horizon aggregates remained negative after the recorded endpoint spreads. These figures belong to this new comparison and are not an improvement measurement against the older 47.51% H1 result from another period.

A separately recorded check after inspecting those results makes the limitation clearer: **simply reversing the prior 15-minute move beat every learned arm's mean bid/ask endpoint return on matched rows**. At the 30-minute horizon its direction accuracy was **53.66%**, above every learned arm on all three dates. Reversal itself still lost after spreads. This does not support claiming that the broader models learned useful trading decisions beyond a basic control. The [post-hoc reversal diagnostic](review/POST_HOC_REVERSAL_CONTROL_20260911.md) preserves its separate design, matched denominators and all results; the original study specification/results were not changed.

This is a new representation and integration comparison with explicit predecessors. Gradient boosting, cross-pair direction, news combinations, and larger historical feature engines already existed; their failures remain recorded. The new difference is the shared causal news join, independent peer inputs, direct binary target, and identical exact-time evaluation across all arms.

## What was built

- A separate immutable snapshot of **68 pairs, 447,875 price rows and 13,475 original committed news mappings**. Prices begin September 6 at 22:00 UTC; news at 23:00; both stop September 11 at 20:00 UTC.
- **24 technical fields, 75 peer fields, eight news aggregates and an explicit news-availability field**. Declared pair identity is included in every model. The combined arm has 108 numeric fields before pair identity and train-derived missing indicators.
- Price rates use bps for cross-pair comparison. Peer means, breadth and dispersion exclude the target pair itself. Missing prices are never filled; absent retained news remains NaN with a missing-status flag.
- Four fixed input groups under direct logistic regression and gradient boosting. All eight arms use the same rows for each horizon/fold. Baselines are training-only global/pair prevalence and prior 15-minute momentum.
- Three expanding chronological folds: train September 7–8 then evaluate September 9; expand through September 9 for September 10; expand through September 10 for September 11 before 20:00 UTC. Every training label matures strictly before the next evaluation date.
- Fixed parameters, no threshold search, no automatic selection, and no model promotion. Probabilities are direct uncalibrated classifier estimates.

## Completed results

Accuracy excludes flat future midpoint outcomes and abstentions; coverage and both denominators are retained in the JSON. Balanced accuracy gives the two observed directions equal weight. Lower Brier is better. Net bps is a hypothetical bid/ask **candle-close endpoint** diagnostic on common two-sided cost coverage, with abstentions valued zero; it excludes slippage, financing and actual fills. It is not account profit. Rows repeat pairs, dates and overlapping horizons and are not independent trials.

| Horizon | Arm | Rows | Direction accuracy | Balanced accuracy | Brier | Mean net bps |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
'''.replace('all32learned','all 32 learned ')+ '\n'.join(rows)+'''

The day-by-day combined-versus-technical differences show whether an aggregate improvement repeats. Positive accuracy difference helps; negative Brier difference helps. Three dates are too few for a strong generalization claim.

| Validation date | Horizon | Learner | Combined accuracy | Accuracy difference (percentage points) | Brier difference | Combined net bps |
| --- | --- | --- | ---: | ---: | ---: | ---: |
'''+ '\n'.join(dayrows)+f'''

Full paired feature-group deltas, per-pair outcomes, baseline scores, training sample counts and all warnings are retained in [RESULTS.json](evaluation_001/RESULTS.json). There are **{result['forecast_rows']:,} held-out pair/time/horizon rows**, with eight fitted-arm predictions on each; this is not a count of independent market events.

## Verification and recreation

**65 integrated checks passed**, including future-label mutation, training-only preprocessing, exact target maturity, prefix invariance, target-pair exclusion, source tampering, missing news and common cost support. Existing joblib/NumPy deprecation warnings and one pandas preparation future warning were retained; they did not fail the checks.

The loader tests were also made portable by retaining the three exact source dependencies beside the tests; all 28 passed against those retained bytes. They overlap the 65 checks and are not additional independent tests.

All **96 saved models** were loaded and used again on their exact retained feature rows: **{recreation['model_prediction_values_verified']:,} probabilities recreated**, maximum absolute difference **{recreation['maximum_probability_difference']}**. This verifies saved-model recreation, not financial validity or a fresh refit replication.

Read [STUDY_SPEC.json](STUDY_SPEC.json), [the reuse plan](reuse_review/DIRECTION_REUSE_PLAN_20260911.md), [source inventory](data_inventory/DATA_INVENTORY.md), [independent source review](review/EVALUATOR_REVIEW_20260911.json), [input manifest](snapshot_001/manifest.json), [prepared dataset](prepared_001/PREPARED.json), and [recreation receipt](review/SAVED_MODEL_RECREATION.json).

From this directory, using the recorded timeseries312 Python runtime:

```powershell
& 'C:\\Users\\zmoor\\AppData\\Local\\CodexRuntimes\\timeseries312\\Scripts\\python.exe' -m pytest tests -q
& 'C:\\Users\\zmoor\\AppData\\Local\\CodexRuntimes\\timeseries312\\Scripts\\python.exe' verify_saved_models.py --prepared prepared_001 --evaluated evaluation_001 --output review/RECREATION_REPEAT.json
& 'C:\\Users\\zmoor\\AppData\\Local\\CodexRuntimes\\timeseries312\\Scripts\\python.exe' src/run_direction_research_v1.py prepare --snapshot snapshot_001 --output prepared_repeat --spec STUDY_SPEC.json --pair-registry pair_metadata.json
& 'C:\\Users\\zmoor\\AppData\\Local\\CodexRuntimes\\timeseries312\\Scripts\\python.exe' src/run_direction_research_v1.py evaluate --prepared prepared_001 --output evaluation_repeat
```

The final command refits the frozen comparison and refuses an existing output. It is a reproduction on already inspected history, not new holdout evidence. The preparation command recreates features from the retained snapshot with the separately retained registered pair/pip projection; that projection records its original full-registry hash and has its own file identity. The prepared receipt binds source files, exact parameters, runtime versions, feature order, source snapshot and pair metadata. No broker, provider credential, live ledger or service restart is required. Preserve the original output directories.

## Remaining directional buildout

1. **Longer comparable news history.** The committed lane begins September 6; deeper price history alone cannot manufacture earlier news availability. This run uses several days, not multiple independent market regimes. Historical price receipt/backfill timing remains unverified.
2. **Richer official-event inputs.** The retained score mappings cover 17 currencies. Original pre-release consensus, actual/revisions, event-time policy/rate repricing, and the source-conditioned blurb response memory are not silently substituted by these eight news aggregates. The existing meter and event infrastructure remain predecessors to integrate separately when usable observations exist.
3. **Probability calibration and price/path magnitude.** A directional probability at four endpoints is not a full expected-price or risk curve. The existing native curve, remaining-risk and management work still need a matched connection after a directional candidate has useful evidence.
4. **Independent continuation and entry/exit economics.** This frozen cohort is discovery. Any next comparison must preserve its results, name a specific change and evaluate new observations. Candle-close economics do not establish entry timing, stop execution or portfolio returns.

Canonical numerical models, live workers, account limits and old study ledgers were not modified. The sealed weekend package remains unchanged. This report is the completed first direction-research build, not a claim that the full prediction problem is solved.

Published {stamp}.
'''
    report_path=ROOT/'DIRECTION_RESEARCH_REPORT.md'
    with report_path.open('x',encoding='utf-8') as h:h.write(report)
    canonical=PROJECT/'docs/FOREX_DIRECTION_RESEARCH_20260911.md'
    summary='''# Direction research — September 11 evening

The [completed direction build and results](../../direction_research_20260911/DIRECTION_RESEARCH_REPORT.md) add an immutable 68-pair price/news snapshot, 24 technical and 75 independent-peer fields, eight original news aggregates with explicit missingness, and direct up/down forecasts at 5/15/30/60 minutes.

Eight fixed feature/learner arms were evaluated on the same chronological rows across three dates. 65 integrated tests passed; all96saved models reproduced their retained probabilities. This is retrospective discovery using several days of news, not a profitable-model qualification. The report preserves all results and links source hashes, fit parameters, per-pair/fold results and recreation commands.

No active numerical model, broker policy, service, old ledger or sealed weekend release was changed. Remaining gaps include longer causally comparable news, structured official-release/consensus/rate inputs, probability calibration and integration with the existing price/risk curve and position manager.

Initial 15-minute combined boosting accuracy was52.48%versus52.35%technical-only on the same rows; its mean bid/ask endpoint net was−2.983bps. All32learned arm/horizon aggregates remained negative. This does not establish a profitable replacement for the active model.

A separately logged post-hoc reversal control outperformed every learned arm's mean endpoint return on common rows and remained negative itself. The full report separates this diagnostic from the original frozen comparison.
'''.replace('all96saved','all 96 saved').replace('was52.48%versus52.35%technical-only','was 52.48% versus 52.35% technical-only').replace('was−2.983bps','was −2.983 bps').replace('All32learned','All 32 learned')
    if canonical.exists():raise ValueError('canonical_report_already_exists')
    before=ROOT/'canonical_docs_before_direction';before.mkdir(exist_ok=False)
    names=['README.md','FOREX_PENDING_IMPROVEMENTS.md','FOREX_PROJECT_LOG.md',
           'docs/FOREX_CHANGE_REGISTER_20260911.md','docs/FOREX_MODEL_REUSE_REGISTER_20260911.md']
    saved={name:(PROJECT/name).read_bytes() for name in names}
    for name,raw in saved.items():
        target=before/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
    notice='**September 11 evening — direction research completed:** [Build, measured results and recreation](docs/FOREX_DIRECTION_RESEARCH_20260911.md) records the new same-row technical/peer/news comparison at 5/15/30/60 minutes. 65 tests passed and 96 saved models were recreated. Results remain discovery; the active learner and trading configuration are unchanged.\n\n'
    updates={}
    for name,raw in saved.items():
        text=raw.decode('utf-8-sig')
        if name in ('README.md','FOREX_PENDING_IMPROVEMENTS.md'):
            first,rest=text.split('\n',1);text=first+'\n\n'+notice+rest.lstrip('\n')
        elif name=='FOREX_PROJECT_LOG.md':
            text += '\n\n## September 11 evening — direct direction feature bridge\n\n'+notice
        else:
            first,rest=text.split('\n',1)
            local='**September 11 evening update:** [Direction research](FOREX_DIRECTION_RESEARCH_20260911.md) completes a separate fixed technical/peer/news comparison and source-bound reconstruction. FXG-016 has a tested research implementation and measured discovery results; profitable direction remains unestablished. FXG-017/018 remain open for richer currency-meter/event inputs and longer original-known news. Prior results and the sealed weekend release remain intact.\n\n'
            text=first+'\n\n'+local+rest.lstrip('\n')
            if name=='docs/FOREX_CHANGE_REGISTER_20260911.md':
                marker='| FXG-016 — EVIDENCE NEEDED |'
                if text.count(marker)!=1:raise ValueError('change_register_row_missing')
                text=text.replace(marker,'| FXG-016 — COMPARISON COMPLETED / EDGE UNESTABLISHED |',1)
                text=text.replace('Dependencies: FXG-011/012 and the reuse check. [Models][models] |',
                    'Dependencies: FXG-011/012 and the reuse check. September 11: fixed 108-field technical/peer/news bridge completed; all 32 learned horizon/arm means remained negative after bid/ask endpoint costs. [New comparison](FOREX_DIRECTION_RESEARCH_20260911.md). [Models][models] |',1)
            elif name=='docs/FOREX_MODEL_REUSE_REGISTER_20260911.md':
                marker='| Current joint34 H1 |'
                row='| Direction feature bridge108 plus pair identity, September 11 | Three validation dates, 60,482 pair/time/horizon rows and 96 saved fits. H15 combined HGB: 52.48% direction, Brier 0.249524, mean net −2.983 bps; all 32 learned aggregates negative. | Fixed same-row technical/peer/news factorial; exact contiguous labels and original news clocks. Post-hoc reversal had better mean net on matched rows than all learned aggregates, but remained negative. Retrospective discovery, no selected replacement. [Results and recreation](FOREX_DIRECTION_RESEARCH_20260911.md). |\n'
                if text.count(marker)!=1:raise ValueError('reuse_register_row_missing')
                text=text.replace(marker,row+marker,1)
        updates[name]=text.encode('utf-8')
    for name,raw in saved.items():
        if (PROJECT/name).read_bytes()!=raw:raise ValueError('concurrent_document_edit:'+name)
    canonical.write_text(summary,encoding='utf-8')
    for name,raw in updates.items():
        if name=='FOREX_PROJECT_LOG.md' and not raw.startswith(saved[name]):
            raise ValueError('project_log_prefix_changed')
        (PROJECT/name).write_bytes(raw)
    changes={name:{'before_sha256':sha(saved[name]),'after_sha256':sha(raw)} for name,raw in updates.items()}
    changes[str(canonical.relative_to(PROJECT))]={'before_sha256':None,'after_sha256':sha(canonical.read_bytes())}
    (ROOT/'DOCUMENTATION_UPDATE.json').write_text(json.dumps({'published_utc':stamp,'changes':changes,
        'active_models_changed':False,'sealed_weekend_release_changed':False},indent=2)+'\n')
    (ROOT/'pair_metadata.json').write_text(json.dumps({'schema':'retained_registered_pair_pip_projection_v1',
        'original_registry_sha256':prepared['pair_registry_sha256'],
        'pairs':{pair:{'pip_size':pip} for pair,pip in prepared['pip_map'].items()}},indent=2,sort_keys=True)+'\n')
    (ROOT/'requirements-observed.txt').write_text('\n'.join(f'{name}=={version}'
        for name,version in prepared['runtime'].items() if name!='python')+'\n')
    # Retain exact implementation/spec/test bytes for rebuilding without future local edits.
    release=ROOT/'source_release';release.mkdir(exist_ok=False)
    for dirname in ('src','tests','retained_sources'):
        (release/dirname).mkdir()
        for path in (ROOT/dirname).glob('*.py'):shutil.copy2(path,release/dirname/path.name)
    for name in ('STUDY_SPEC.json','verify_saved_models.py','pair_metadata.json','requirements-observed.txt'):
        shutil.copy2(ROOT/name,release/name)
    manifest={str(path.relative_to(release)).replace('\\','/'):{'sha256':sha(path.read_bytes()),'bytes':path.stat().st_size}
              for path in release.rglob('*') if path.is_file()}
    (ROOT/'SOURCE_RELEASE_MANIFEST.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
    print(json.dumps({'report':str(report_path),'canonical_report':str(canonical),'source_files':len(manifest)}))


if __name__=='__main__':main()
