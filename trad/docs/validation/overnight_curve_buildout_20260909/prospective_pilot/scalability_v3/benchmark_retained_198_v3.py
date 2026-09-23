"""Run V3 once on the unchanged retained198 inventory; reuse bound V1/V2 outputs."""
from contextlib import ExitStack
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
import time
from unittest.mock import patch

sys.dont_write_bytecode = True
BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))
import evaluate_prospective_pilot_v3 as v3

old = v3.original
V2 = BASE.parent/'scalability_v2'
MIRROR = BASE.parents[1]/'ev2'
PINS = {
    MIRROR/'V1_FIXED_INVENTORY_RESULT.json':'f06357904a7d75b4fc780aa22be2a707e4129f9557d28a6199cc194d0f71fbb3',
    MIRROR/'V2_FIXED_INVENTORY_CLOCK_REPLAY_RESULT.json':'e51df83ec87618945caa3917e7351086f7e4523c0ac16843530ffcd0608e1df5',
    V2/'RETAINED_198_INVENTORY_20260909.json':'d6e3cbfab64bd5804a0957828b7293ab0ec262330c4686f61b25a9434daa20f6',
    V2/'V1_ACTUAL_EVALUATION_CLOCK_TRACE_20260909.json':'b934ae2a7c0858515c5ac75675d36e3538fc40a124563afa785418d1e2cd635d',
    MIRROR/'benchmark_registry_only.json':'3bd80b3e6a520c2b5a7942e32056deb9095e1e0bf7a2e5b3efe11d0e3aaf37da',
}


def binding(path):
    old.files._safe_components(path, require_file=True)
    raw=path.read_bytes()
    return dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw)),raw


def forbid(*args,**kwargs):
    raise AssertionError('benchmark_forbidden_runtime_or_GET_action')


def compare(left,right):
    checks={}
    for key in ('source_captures','variant_attempts','curves','matched_comparisons'):
        checks[key]=left[key]==right[key];assert checks[key],key
    clean=lambda rows:[{k:v for k,v in r.items() if k!='observed_epoch'} for r in rows]
    checks['manifest_except_separately_actual_read_times']=clean(left['manifest'])==clean(right['manifest'])
    assert checks['manifest_except_separately_actual_read_times']
    assessment=deepcopy(right['assessment'])
    assert assessment.pop('original_evaluation_schema')==left['assessment']['schema_version']
    assessment['schema_version']=left['assessment']['schema_version']
    assessment['evaluator_source_sha256']=assessment.pop('original_evaluator_source_sha256')
    assessment.pop('validation_memo');assessment.pop('capture_bank')
    assert assessment.pop('optimized_engine_sources')==v3.v2.EXPECTED_SOURCES
    assert assessment.pop('v2_source_sha256')==v3.EXPECTED_V2_SHA
    assessment['limits'].pop()
    checks['assessment_except_explicit_v3_metadata']=assessment==left['assessment']
    assert checks['assessment_except_explicit_v3_metadata']
    return checks


def main():
    dest=BASE/'retained_198_differential_001'
    assert not dest.exists();old.files._safe_components(BASE);dest.mkdir()
    phase='verify_prior_inventory';start=time.time()
    try:
        loaded={};evidence=[]
        for path,expected in PINS.items():
            ref,raw=binding(path);assert ref['sha256']==expected,(path,'changed_prior')
            evidence.append(ref);loaded[path.name]=json.loads(raw)
        inventory=loaded['RETAINED_198_INVENTORY_20260909.json']
        runtime=Path(inventory['private_clone_runtime'])
        assert len(inventory['files'])==3140
        for row in inventory['files']:
            path=runtime/row['relative_path'];assert runtime in path.parents
            ref,_=binding(path);assert (ref['sha256'],ref['bytes'])==(row['sha256'],row['bytes'])
        left=loaded['V1_FIXED_INVENTORY_RESULT.json'];prior_v2=loaded['V2_FIXED_INVENTORY_CLOCK_REPLAY_RESULT.json']
        for key in ('source_captures','variant_attempts','curves','matched_comparisons'):
            assert left[key]==prior_v2[key]
        assert left['assessment']['curve_chain_status_counts']=={'scored_chain':198,'withheld':6}
        clock_values=loaded['V1_ACTUAL_EVALUATION_CLOCK_TRACE_20260909.json']['actual_clock_values'];cursor=0
        assert len(clock_values)==1538
        def replay_clock():
            nonlocal cursor
            assert cursor<len(clock_values),'extra_clock_invocation'
            value=clock_values[cursor];cursor+=1;return value
        own=binding(Path(v3.__file__))[0]
        phase='v3_fixed_inventory_clock_replay';actual_start=time.time();tick=time.perf_counter()
        with ExitStack() as stack:
            stack.enter_context(patch.object(old.pilot,'ROOT',MIRROR))
            for module,names in ((old.files,('publish_curve','consume_published_curve','_write_exclusive')),
                                 (old.candles,('capture_once',)),(old.pilot,('run_cycle',))):
                for name in names:stack.enter_context(patch.object(module,name,forbid))
            right=v3.evaluate(MIRROR/'benchmark_registry_only.json',PINS[MIRROR/'benchmark_registry_only.json'],clock=replay_clock)
        duration=time.perf_counter()-tick;actual_end=time.time()
        assert cursor==len(clock_values),'missing_clock_invocation'
        assert binding(Path(v3.__file__))[0]==own,'candidate_changed'
        phase='complete_differential'
        checks=compare(left,right)
        assert all(actual_start<=r['observed_epoch']<=actual_end for r in right['manifest'])
        result_ref=v3.save(dest/'V3_FIXED_INVENTORY_CLOCK_REPLAY_RESULT.json',right)
        prior_benchmark=json.loads((V2/'RETAINED_198_CURVE_BENCHMARK_20260909.json').read_bytes())
        report=dict(schema_version='v3_retained_198_differential_v1_20260909',status='passed',
            started_epoch=start,completed_epoch=time.time(),source=own,
            test_source=binding(BASE/'test_evaluate_prospective_pilot_v3.py')[0],
            benchmark_source=binding(Path(__file__))[0],source_evidence=evidence,
            exact_checks=checks,curves=198,withheld_curves=6,cycle_count=38,file_count=3140,
            original_source_read_clock_calls=len(clock_values),v3_wall_sec=duration,
            actual_v3_started_epoch=actual_start,actual_v3_completed_epoch=actual_end,
            prior_v1_wall_sec=prior_benchmark['baseline_v1']['wall_sec'],
            prior_v2_wall_sec=prior_benchmark['optimized_v2']['wall_sec'],
            prior_v2_to_current_v3_ratio=prior_benchmark['optimized_v2']['wall_sec']/duration,
            capture_bank=right['assessment']['capture_bank'],validation_memo=right['assessment']['validation_memo'],
            result_file=result_ref,limits=[
                'Exactly the prior retained inventory and actual V1 clock trace, replayed solely for engineering equality. V3 real run and file-read clocks are separately retained.',
                'Original V1/V2 results are byte-pinned and reused, not rerun. Timing ratios compare separate runs under potentially different concurrent load.',
                'One matched inventory; not a guarantee for later larger inventories or total export time. Final report serialization is outside the measured evaluator wall time.',
                'No live evaluation, GET, model fit, forecast issue, source edit or runtime write.'],**old.contract.AUTHORITY)
        ref=v3.save(dest/'V3_RETAINED_198_DIFFERENTIAL_20260909.json',report)
        print(json.dumps(dict(status='passed',receipt=ref,v3_wall_sec=duration,bank=report['capture_bank'])),flush=True)
        return 0
    except Exception as error:
        ref=v3.save(dest/'BENCHMARK_FAILED.json',dict(status='failed',phase=phase,error_type=type(error).__name__,
            reason=str(error)[:240],observed_epoch=time.time(),**old.contract.AUTHORITY))
        print(json.dumps(dict(status='failed',receipt=ref,phase=phase)),flush=True)
        return 1


if __name__=='__main__':raise SystemExit(main())
