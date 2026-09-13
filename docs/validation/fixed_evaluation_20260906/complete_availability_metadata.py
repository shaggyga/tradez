"""Finish explicit mixed-producer clock metadata without opening SQLite."""
from pathlib import Path
import collections
import datetime as dt
import hashlib
import json
import statistics

OUT=Path(__file__).resolve().parent
ROOT=OUT.parents[1]/'trad'
path=OUT/'availability_diagnostic.json'
record=json.loads(path.read_text(encoding='utf-8'))
rows=json.loads((OUT/'selected_candidate_rows.json').read_text(encoding='utf-8'))
normalized=json.loads((OUT/'normalized_forecasts.json').read_text(encoding='utf-8'))['rows']
def epoch(value):return dt.datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
def quantiles(values):
    v=sorted(values)
    return {'n':len(v),'min':min(v) if v else None,'median':statistics.median(v) if v else None,'p95':v[int((len(v)-1)*.95)] if v else None,'max':max(v) if v else None}
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()

producers=collections.Counter()
for family,summary in record['family_clock_diagnostics'].items():
    items=[r for r in rows if r['family']==family]
    summary['original_entry_before_recorded_count']=sum(epoch(r['entry_time'])<epoch(r['recorded_utc']) for r in items)
    summary['missing_explicit_clock_field_counts']={'issued_utc':len(items),'published_utc':len(items),'committed_utc':len(items),'features_available_utc':len(items),'max_training_label_maturity_utc':len(items),'max_training_label_available_utc':len(items)}
    summary['exit_clock_minus_original_reference_plus_hour_sec_mixed_producers']=summary.pop('exit_minus_original_reference_plus_hour_sec')
    summary['exit_clock_minus_recorded_plus_hour_sec_mixed_producers']=summary.pop('exit_minus_recorded_plus_hour_sec')
    family_producers=collections.Counter()
    canonical_lateness=[]
    for r in items:
        if r['outcome'] is None:continue
        d=json.loads(r['outcome']['diagnostics_json'])
        producer=d.get('maturity_worker','legacy_recovered_provider_quote_clock')
        family_producers[producer]+=1;producers[producer]+=1
        if producer=='canonical_quote_snapshot_v1':canonical_lateness.append(epoch(r['outcome']['exit_time'])-epoch(r['entry_time'])-3600)
    summary['outcome_producer_counts']=dict(family_producers)
    summary['canonical_local_exit_minus_original_target_sec']=quantiles(canonical_lateness)

record['clock_semantics']['exit_time']='Mixed producers: canonical_quote_snapshot_v1 retains local snapshot generated time at recorded_utc+3600; legacy recovered outcome records retain provider quote.time, with no local quote receipt and an entry-relative age target.'
record['clock_semantics']['recorded_utc']='utc_now sampled while appending to pending Python list before flush; recording before persistence, not a postcommit visibility certificate.'
record['strict_blockers'][-1]='1483 canonical outcomes target recorded_utc+3600 rather than original reference+3600;60 legacy recovered outcomes lack a local quote receipt clock.'
record['outcome_producer_counts']=dict(producers)
record['strict_clock_eligibility_counts']={'forecast_universe_rows':len(rows),'strict_eligible_rows':0,'null_issued_clock_rows':sum(r['issued_utc'] is None for r in normalized),'null_published_clock_rows':sum(r['published_utc'] is None for r in normalized),'null_committed_clock_rows':sum(r['committed_utc'] is None for r in normalized),'null_feature_first_availability_clock_rows':sum(r['features_available_utc'] is None for r in normalized),'null_training_label_first_availability_clock_rows':sum(r['max_training_label_available_utc'] is None for r in normalized),'entry_quote_reference_precedes_forecast_recording_rows':sum(epoch(r['entry_time'])<epoch(r['recorded_utc']) for r in rows),'missing_outcome_rows':sum(r['outcome'] is None for r in rows)}
extra_source='oanda_practice_shadow_strategy_lab.py'
record['source_code_and_registry'].append({'path':extra_source,'sha256':sha(ROOT/extra_source)})
record['finished_utc']=dt.datetime.now(dt.timezone.utc).isoformat()
record['local_artifacts']=[{'path':p.name,'sha256':sha(p),'bytes':p.stat().st_size} for p in [OUT/'inventory.py',OUT/'inventory.json',OUT/'selected_candidate_rows.json',OUT/'fixed_cohort_contracts.json',OUT/'normalize_and_score_archive.py',OUT/'normalized_forecasts.json',OUT/'archival_scorecard.json',OUT/'finish_inventory.py',Path(__file__).resolve()]]
path.write_text(json.dumps(record,indent=2,sort_keys=True)+'\n',encoding='utf-8')
md='''# Availability of the fixed EUR/USD one-hour proof forecasts

The archived records can support a clearly labeled diagnostic, but **cannot establish a strict causal and executable forecast comparison**. All four current families were included before examining forecast outcomes. Forex remained stopped. The only production SQL was indexed SELECT/schema work through URI `mode=ro`, `query_only`, in one read transaction. Database and WAL were not written or checkpointed. SQLite may update ephemeral reader metadata in SHM; SHM byte identity is not claimed.

The source is `trad/data/oanda_training_manager/state/strategy_shadow_outcomes_v1.sqlite`, not the separate one-hour contributor study. The extract preserves original typed forecast columns, exact immutable forecast JSON and verified hashes, nullable outcomes, and integrity events. All 1,628 original forecast hashes verify. Each of the four families has 407 EUR/USD reference epochs; the actual recorded sample spans August 30 through September 4. There are 1,543 matched outcomes, and 85 missing outcomes remain in the universe.

The original `entry_time` comes from the feature snapshot generated before model computation. Every forecast's recording time is later than that reference; the median gap is 84.63 seconds, with a maximum 10,695.93 seconds. Recording is itself a pending-list append timestamp before database flush, so it cannot be substituted for actual postcommit publication. No retained record contains a separate first visibility/commit certificate, historical feature first availability, or maximum training label first availability certificate. Training cutoff and dataset hash alone do not supply those clocks.

Outcome producers also differ. The canonical quote worker supplies 1,483 outcomes at `recorded_utc + 1 hour`, while retaining the earlier entry quote. Its `exit_time` is the local quote snapshot generation clock. The other 60 are recovered outcomes produced by the strategy lab: they use entry-relative age and provider quote time as `exit_time`, with no local quote receipt clock retained. The normalized extract represents unavailable clocks as null and distinguishes these producers. It never relabels `recorded_utc` as publication, or provider quote time as local receipt.

For a fair archival diagnostic, 368 original epochs have all four forecasts and exactly identical entry quote, endpoint clock, and endpoint quote across families. Another 22 epochs lack one or more outcomes, and 17 have differing endpoints. Among the retained epochs, 353 use the canonical producer and 15 the legacy recovered producer. These 368 overlapping epochs span only six UTC days, so raw row counts do not establish independent statistical power. They are not prospective proof, exact original-reference one-hour returns, or realizable trades.

The fixed nonfitted baselines are constant 50% up probability, zero signed price change, and no trade. Rolling class-rate and historical momentum baselines require unavailable historical availability evidence and are left unavailable. On identical archived endpoints, every model has worse Brier score than 0.25 and worse signed-move mean absolute error than zero change. Every model also has a negative mean after the stored bid/ask spread. Full rows, definitions, missingness, and all four results are preserved in `archival_scorecard.json`; no family was selected or tuned from these results.

The source database/WAL checkpoint hashes, schema/index query plan, exact cohort contracts, code bindings, normalized all-forecast extract, and archival calculations are retained beside this report. The full raw database is intentionally not copied into the vault. A future strict sample needs genuine forecast issue and postcommit visibility records, linked feature/label availability, an executable quote after publication, and a fixed original-reference target that is never shifted after computation.
'''
(OUT/'AVAILABILITY_REVIEW.md').write_text(md,encoding='utf-8')
print(json.dumps({'strict_counts':record['strict_clock_eligibility_counts'],'source_producers':dict(producers),'binding':{'path':path.name,'sha256':sha(path)},'database_bindings':record['database_file_bindings']},indent=2))
