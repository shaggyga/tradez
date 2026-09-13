"""Retain only compact, locally bounded completed-news evidence for curation."""
from pathlib import Path
import datetime as dt
import hashlib
import json
import xml.etree.ElementTree as ET

BASE=Path(__file__).resolve().parents[1]
HERE=Path(__file__).resolve().parent
OUT=HERE/'completed_summary_002'

def binding(path):
    raw=path.read_bytes()
    return dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))

def save(path,value):
    with path.open('x',encoding='utf-8',newline='\n') as stream:json.dump(value,stream,indent=2,sort_keys=True);stream.write('\n')

def main():
    summary=OUT/'NEWS_CLOCK_V2_COMPLETED_SUMMARY_20260909.json';v=json.loads(summary.read_bytes())
    assert v['status']=='completed_observation' and v['termination_boundary'] in ('fixed_wall_deadline','original_monotonic_budget') and v['all_registered_sources_unchanged'] is True
    xml=HERE/'news_clock_summary_closed_tests.xml';suites=list(ET.fromstring(xml.read_bytes()).iter('testsuite'))
    assert sum(int(x.get('tests',0)) for x in suites)==38 and not any(int(x.get(k,0)) for x in suites for k in ('failures','errors','skipped'))
    review=HERE/'NEWS_CLOCK_SUMMARY_DUAL_DEADLINE_REVIEW_20260909.json';rv=json.loads(review.read_bytes());assert rv['status']=='passed'
    for b in rv['bindings']:
        path=Path(b['path']);assert path.is_relative_to(BASE) and binding(path)['sha256']==b['sha256']
    source=HERE/'observe_news_clock_boundaries_v2.py';assert binding(source)['sha256']==v['observer_sha256']
    now=dt.datetime.now(dt.timezone.utc)
    utc=lambda x:dt.datetime.fromtimestamp(x,dt.timezone.utc).isoformat()
    text=f'''# Completed passive news-clock observation\n\nObserved {utc(v['observation_started_epoch'])} through {utc(v['observation_completed_epoch'])}; {v['sample_count']:,} samples. The source-bound observation reached its original dual wall/monotonic limit. Its compact summary was prepared separately at {now.isoformat()}.\n\nValidation counts: `{json.dumps(v['counts'],sort_keys=True)}`. Invalid clock-field observations: {v['invalid_epoch_field_count']:,}; these retain field/type distinctions and do not by themselves establish host-clock drift. Repaired-producer error counter first/last/max: {v['producer_error_first']}/{v['producer_error_last']}/{v['producer_error_max']}. All producer status transitions and rejection reasons remain in the compact JSON.\n\nA later read-only article diagnostic was attempted: {v['db_diagnostic_attempted']}. Such an independent later input cannot establish the exact bytes used by an earlier failed worker. A current repaired heartbeat does not prove full current-news semantic validity, model readiness or profitable prediction. No GET, runtime changes, source changes or production writes occurred in this summary.\n\nCounts describe sampled observations; gaps and the longest sampled noncurrent spans do not establish continuous outage durations. Raw unique source bytes and the full sanitized sample log remain local, with exact hashes bound by the compact report. The unchanged observer's 26 tests and this summarizer's 38 tests cover different, overlapping evidence boundaries; they are not a trading-performance denominator.\n'''
    md=OUT/'NEWS_CLOCK_V2_COMPLETED_SUMMARY_20260909.md';md.write_text(text,encoding='utf-8')
    selected=[HERE/'summarize_completed_news_clock_v1.py',HERE/'test_summarize_completed_news_clock_v1.py',xml,review,Path(__file__),summary,md]
    acceptance=OUT/'NEWS_CLOCK_V2_COMPLETION_ACCEPTANCE_20260909.json'
    save(acceptance,dict(schema_version='news_clock_v2_completion_acceptance_v1_20260909',status='completed_observation_verified_not_full_operational_acceptance',created_utc=now.isoformat(),evidence=[binding(p) for p in selected],original_observer_result=v['bound_inputs'][1],original_start=v['bound_inputs'][0],excluded_sample_log=v['bound_inputs'][2],GET=False,runtime_writes=False,original_sources_unchanged=True,limits=v['limits']))
    selected.append(acceptance)
    entries=[]
    for p in selected:
        assert p.is_relative_to(BASE) and len(p.read_bytes())<4*1024*1024
        rel=p.relative_to(BASE).as_posix();assert not any(x.startswith('private') for x in p.parts) and p.name!='sanitized_samples.jsonl'
        member='docs/validation/overnight_curve_buildout_20260909/'+rel
        if p.name.startswith('test_') and p.suffix=='.py':member+='.txt'
        entries.append(dict(original_source=binding(p),intended_member=member,kind='source' if p.suffix=='.py' else 'retained_evidence',group='completed_passive_news_clock_v2',artifact_status='Completed dated passive observation and exact-log summary; no live health, future input, calibration or execution claim.'))
    selection=BASE/'CURATED_COMPLETED_NEWS_CLOCK_V2_SELECTION_20260909.json'
    save(selection,dict(schema_version='portable_evidence_selection_v1_20260909',created_utc=now.isoformat(),entries=entries,file_count=len(entries),selected_bytes=sum(e['original_source']['bytes'] for e in entries),canonical_source_dependencies=[],broker_requests=False,runtime_writes=False,excluded=['Full sanitized sample log','All private_unique_bytes','Article payloads and event IDs'],scope='BASE-only new compact evidence; unchanged original observer/source tests remain in the historical published629 index.'))
    print(json.dumps({'acceptance':binding(acceptance),'selection':binding(selection),'file_count':len(entries),'samples':v['sample_count'],'counts':v['counts']}))

if __name__=='__main__':main()