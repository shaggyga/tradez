"""Deterministic12-curve/all-retained-domain engineering comparison, never live."""
from collections import Counter
from contextlib import ExitStack
from copy import deepcopy
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from unittest.mock import patch

sys.dont_write_bytecode=True
BASE=Path(__file__).resolve().parent
sys.path.insert(0,str(BASE))
import evaluate_prospective_pilot_v3 as v3
old=v3.original
PRIOR=BASE.parent/'scalability_v2/actual_evaluation_003'
ASSESSMENT_SHA='2f22bb9626ce5424f1a249e66f322184c2d2ab048b597b8200af17f8dde90b81'
REGISTRY=old.ROOT/'config/recovered_second_curve_pilot_v1_20260909.json'
REGISTRY_SHA='ae64f2cae6df44dad81b46deb16e95e4d5f67dabb7fc1e65289b96ba6b0fe2b2'
MIRROR=BASE.parents[1]/'ev3w12'
OUTPUT=BASE/'wide_source_12_differential_001'
MAX_COPY_BYTES=192*1024*1024


def sha(raw):return hashlib.sha256(raw).hexdigest()


def bound_read(path,expected=None,size=None):
    old.files._safe_components(path,require_file=True)
    before=old.files._identity(path.stat())
    # Retained evaluator manifests are larger than an individual curve record.
    # This separate external reader never changes the frozen store's limit.
    with path.open('rb') as f:
        opened=old.files._identity(os.fstat(f.fileno()))
        raw=f.read(16*1024*1024+1)
        finished=old.files._identity(os.fstat(f.fileno()))
    old.files._safe_components(path,require_file=True)
    assert 0<len(raw)<=16*1024*1024 and len({before,opened,finished,old.files._identity(path.stat())})==1
    if expected is not None:assert sha(raw)==expected,(str(path),'sha')
    if size is not None:assert len(raw)==size,(str(path),'size')
    return raw


def prior_read(ref):
    path=Path(ref['path']);assert not path.is_absolute() and '..' not in path.parts
    return bound_read(PRIOR/path,ref['sha256'],ref['bytes'])


def plan():
    assessment_raw=bound_read(PRIOR/'PROSPECTIVE_PILOT_EVALUATION.json',ASSESSMENT_SHA)
    assessment=json.loads(assessment_raw);refs={r['path']:r for r in assessment['evidence_files']}
    manifest=json.loads(prior_read(refs['manifest.json']));sources=json.loads(prior_read(refs['source_captures.json']))
    registry_raw=bound_read(REGISTRY,REGISTRY_SHA);registry=json.loads(registry_raw)
    runtime=Path(registry['output_root']);by_path={r['relative_path']:r for r in manifest}
    assert len(by_path)==len(manifest)
    chosen=[];chosen_report_refs=[]
    for name in sorted(k for k in refs if k.startswith('curves/curve_')):
        item=json.loads(prior_read(refs[name]))
        if item['status']=='scored_chain':
            chosen.append(item);chosen_report_refs.append(refs[name])
        if len(chosen)==12:break
    assert len(chosen)==12
    pairs={item['curve']['prepared_curve']['instrument'] for item in chosen}
    retained_sources=[r for r in sources if r['instrument'] in pairs]
    keep=set();source_dirs={r['directory'] for r in retained_sources}
    proof_dirs={r['variant_directory'] for r in chosen}
    curve_dirs={r['directory'] for r in chosen}
    hashes={r['curve']['curve_sha256'] for r in chosen}
    cycle_dirs={d.rsplit('/',1)[0] for d in source_dirs}
    for name,row in by_path.items():
        if name in {d+'/cycle_completed.json' for d in cycle_dirs}:
            keep.add(name)
        if any(name.startswith(d+'/') for d in proof_dirs|curve_dirs):keep.add(name)
        for directory in source_dirs:
            prefix=directory+'/'
            if name.startswith(prefix):
                suffix=name[len(prefix):]
                if suffix in ('source_raw.json','capture_receipt.json') or suffix in {
                    c+'/source_mapping.json' for c in old.pilot.CONVENTIONS}:keep.add(name)
        if name.startswith('published/consumptions/'):
            raw=bound_read(runtime/name,row['sha256'],row['bytes'])
            if json.loads(raw).get('curve_sha256') in hashes:keep.add(name)
    rows=[by_path[name] for name in sorted(keep)]
    total=sum(r['bytes'] for r in rows);assert total<=MAX_COPY_BYTES,total
    clone_runtime=MIRROR/'data/oanda_training_manager'/registry['study_id']
    assert max(len(str(clone_runtime/r['relative_path'])) for r in rows)<250
    return dict(schema_version='wide_source_12_plan_v1_20260909',original_assessment_sha256=ASSESSMENT_SHA,
        registry_sha256=REGISTRY_SHA,original_runtime=str(runtime),private_runtime=str(clone_runtime),
        selected_curve_reports=chosen_report_refs,selected_curve_sha256=[r['curve']['curve_sha256'] for r in chosen],
        selected_pairs=sorted(pairs),selected_original_proof_directories=sorted(proof_dirs),
        retained_source_records=len(retained_sources),source_status_counts=dict(Counter(r['status'] for r in retained_sources)),
        retained_source_directories=sorted(source_dirs),retained_cycle_directories=sorted(cycle_dirs),
        files=rows,file_count=len(rows),bytes=total,maximum_copy_bytes=MAX_COPY_BYTES,
        original_observed_file_count=len(manifest),omitted_file_count=len(manifest)-len(rows),
        selection='First12 scored_chain curve files in existing hash-sorted report order, independent of values/performance. All same-pair originally observed source domains remain, including failures. Unrelated model attempts/curves are deliberately omitted.',
        engineering_subset_not_registered=True,original_price_issue_source_clocks_unchanged=True),registry


def copy_new(path,raw):
    assert MIRROR in path.parents and len(str(path))<250
    path.parent.mkdir(parents=True,exist_ok=True);old.files._safe_components(path.parent)
    with path.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    assert bound_read(path)==raw


def forbidden(*args,**kwargs):raise AssertionError('no_runtime_or_GET_action')


def normalized_v2(right):
    value=deepcopy(right);a=value['assessment']
    a['schema_version']=a.pop('original_evaluation_schema')
    a['evaluator_source_sha256']=a.pop('original_evaluator_source_sha256')
    a.pop('validation_memo');a.pop('optimized_engine_sources');a['limits'].pop()
    return value


def compare(left,right):
    from benchmark_retained_198_v3 import compare as original_compare
    return original_compare(normalized_v2(left),right)


def run():
    assert not MIRROR.exists() and not OUTPUT.exists()
    prepared,registry=plan();OUTPUT.mkdir();MIRROR.mkdir()
    v3.save(OUTPUT/'SUBSET_PLAN.json',prepared)
    print(json.dumps(dict(phase='copy_exact_subset',files=prepared['file_count'],bytes=prepared['bytes'],sources=prepared['source_status_counts'])),flush=True)
    runtime=Path(prepared['original_runtime']);clone_runtime=Path(prepared['private_runtime'])
    for row in prepared['files']:
        rel=Path(row['relative_path']);assert not rel.is_absolute() and '..' not in rel.parts
        copy_new(clone_runtime/rel,bound_read(runtime/rel,row['sha256'],row['bytes']))
    for directory in prepared['retained_source_directories']:
        path=clone_runtime/directory;assert clone_runtime in path.parents;path.mkdir(parents=True,exist_ok=True)
    for name,expected in registry['source_bindings'].items():copy_new(MIRROR/name,bound_read(old.ROOT/name,expected))
    cloned=deepcopy(registry);cloned['output_root']=str(clone_runtime)
    assert [k for k in registry if cloned[k]!=registry[k]]==['output_root']
    registry_raw=old.contract.canonical_bytes(cloned);clone_path=MIRROR/'benchmark_registry_only.json';copy_new(clone_path,registry_raw)
    v3_before=bound_read(Path(v3.__file__));trace=[];score_timings={}
    def actual_clock():
        actual=time.time();trace.append(actual);return actual
    phase='v2_actual_clock'
    try:
        with ExitStack() as stack:
            stack.enter_context(patch.object(old.pilot,'ROOT',MIRROR))
            for module,names in ((old.files,('publish_curve','consume_published_curve','_write_exclusive')),
                                 (old.candles,('capture_once',)),(old.pilot,('run_cycle',))):
                for name in names:stack.enter_context(patch.object(module,name,forbidden))
            original_score=old.outcomes.score_curve_from_captures
            def measured_score(*args,**kwargs):
                tick=time.perf_counter()
                try:return original_score(*args,**kwargs)
                finally:score_timings[phase]=score_timings.get(phase,0)+time.perf_counter()-tick
            stack.enter_context(patch.object(old.outcomes,'score_curve_from_captures',measured_score))
            v2start=time.time();tick=time.perf_counter()
            left=v3.v2.evaluate(clone_path,sha(registry_raw),clock=actual_clock)
            v2sec=time.perf_counter()-tick;v2end=time.time()
            assert left['assessment']['curve_chain_status_counts']=={'scored_chain':12}
            left_ref=v3.save(OUTPUT/'V2_WIDE_SOURCE_RESULT.json',left)
            v3.save(OUTPUT/'V2_ACTUAL_CLOCK_TRACE.json',dict(values=trace,actual_started_epoch=v2start,actual_completed_epoch=v2end))
            print(json.dumps(dict(phase='v2_complete',wall_sec=v2sec,score_sec=score_timings[phase],clock_calls=len(trace))),flush=True)
            cursor=0
            def replay_clock():
                nonlocal cursor
                assert cursor<len(trace),'extra_clock_call'
                result=trace[cursor];cursor+=1;return result
            phase='v3_replayed_clock';v3start=time.time();tick=time.perf_counter()
            right=v3.evaluate(clone_path,sha(registry_raw),clock=replay_clock)
            v3sec=time.perf_counter()-tick;v3end=time.time()
            assert cursor==len(trace)
        assert bound_read(Path(v3.__file__))==v3_before
        checks=compare(left,right)
        for value,a,b in ((left,v2start,v2end),(right,v3start,v3end)):
            assert all(a<=r['observed_epoch']<=b for r in value['manifest'])
        right_ref=v3.save(OUTPUT/'V3_WIDE_SOURCE_RESULT.json',right)
        report=dict(schema_version='wide_source12_differential_v1_20260909',status='passed',completed_epoch=time.time(),
            selected_curves=12,selected_curve_sha256=prepared['selected_curve_sha256'],
            original_assessment_sha256=ASSESSMENT_SHA,benchmark_registry_sha256=sha(registry_raw),
            registry_field_changes=['output_root'],source_status_counts=prepared['source_status_counts'],
            retained_file_count=prepared['file_count'],retained_bytes=prepared['bytes'],exact_checks=checks,
            actual_v2_interval=[v2start,v2end],actual_v3_interval=[v3start,v3end],v2_wall_sec=v2sec,v3_wall_sec=v3sec,
            rolling_score_wall_sec=score_timings,wall_speedup=v2sec/v3sec,
            source_selection_supplied_capture_counts=[r['report'].get('supplied_capture_count') for r in right['curves']],
            v2_cache=left['assessment']['validation_memo'],v3_cache=right['assessment']['validation_memo'],v3_bank=right['assessment']['capture_bank'],
            final_v3_source_sha256=sha(v3_before),benchmark_source_sha256=sha(Path(__file__).read_bytes()),
            clock_calls=len(trace),evidence_files=[left_ref,right_ref],
            limits=['Deterministic curve/proof subset; source domains are preserved. This is not the complete study denominator or an accuracy claim.',
                'V2 used actual new evaluator clocks; V3 replayed them solely for equality. Their actual run and file-read intervals are separate.',
                'One serial comparison under current shared machine load. Evaluator times exclude output serialization; scoring component times include unchanged scorer plus version-specific validators.',
                'No source/registry/runtime/model writes, GETs, model fits or forecast publications.'],**old.contract.AUTHORITY)
        ref=v3.save(OUTPUT/'WIDE_SOURCE_12_DIFFERENTIAL_20260909.json',report)
        print(json.dumps(dict(status='passed',receipt=ref,v2_sec=v2sec,v3_sec=v3sec,rolling_score_wall_sec=score_timings)),flush=True)
    except Exception as error:
        ref=v3.save(OUTPUT/'SUBSET_BENCHMARK_FAILED.json',dict(status='failed',phase=phase,error_type=type(error).__name__,
            reason=str(error)[:240],observed_epoch=time.time(),**old.contract.AUTHORITY))
        print(json.dumps(dict(status='failed',receipt=ref)),flush=True);raise


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    if args.execute:run()
    else:
        prepared,_=plan();print(json.dumps({k:v for k,v in prepared.items() if k not in ('files','retained_source_directories','retained_cycle_directories')},indent=2))
