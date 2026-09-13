"""New external output/scheduling wrapper; original assessment arithmetic unchanged."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys
import time

sys.dont_write_bytecode=True
BASE=Path(__file__).resolve().parent
ROOT=Path(r'C:/Users/zmoor/Documents/forex/trad')
ASSESSOR=ROOT.parent/'program_assessment_20260909T0316Z/performance/assess_v3.py'
PRIOR=ROOT.parent/'revamp_baseline_20260908/performance/evaluate_frozen_baseline.py'
PINS={ASSESSOR:'bad4ea610f01a19e82ae1e38709775ab306c609e2cdb799e35a5da75c44ea5a2',
      PRIOR:'186e9000e2af2360e23bbdb752fb83bd26df61ca7c387899a880adae2739b7f1'}
REGISTRY=ROOT/'config/joint_price_news_study_v3_20260908.json'
REGISTRY_SHA='ee075e69e80ca56dfdf45abe1f7a1a301612176e67f7622eab1adf2bd9af8771'


def need(ok,reason):
    if not ok:raise ValueError(reason)


def safe(path):
    path=Path(path).absolute();need('..' not in path.parts,'path_escape')
    for component in reversed([path,*path.parents]):
        info=component.lstat()
        need(not stat.S_ISLNK(info.st_mode) and not getattr(info,'st_file_attributes',0)&1024,'reparse_path')
    return path


def hashed(path,limit=2*1024*1024):
    path=safe(path)
    with path.open('rb') as stream:
        before=os.fstat(stream.fileno());raw=stream.read(limit+1);after=os.fstat(stream.fileno())
    need(len(raw)<=limit and (before.st_ino,before.st_size,before.st_mtime_ns)==
         (after.st_ino,after.st_size,after.st_mtime_ns),'file_read_bound_or_change')
    return hashlib.sha256(raw).hexdigest(),raw


def bindings():
    result={}
    for path,expected in {**PINS,REGISTRY:REGISTRY_SHA}.items():
        actual,raw=hashed(path);need(actual==expected,'pinned_helper_or_registry_changed')
        result[str(path)]=actual
    registry=json.loads(hashed(REGISTRY)[1]);need(len(registry['source_bindings'])==20,'registered_source_inventory')
    for name,expected in registry['source_bindings'].items():
        path=ROOT/name;actual,_=hashed(path);need(actual==expected,'registered_source_changed');result[str(path)]=actual
    result[str(Path(__file__).resolve())]=hashed(Path(__file__))[0]
    return result


class SerialExecutor:
    """Only scheduling changes: execute original callable on this same thread."""
    def __init__(self,max_workers):need(max_workers==4,'unexpected_original_executor_request')
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def map(self,function,items,chunksize=1):
        items=list(items);need(len(items)<=68 and chunksize==1,'assessment_pair_bound')
        for item in items:yield function(item)


def load_original():
    spec=importlib.util.spec_from_file_location('original_joint_assessment_for_serial_reuse',ASSESSOR)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    need(module.REGISTRY_SHA==REGISTRY_SHA and module.PRIOR==PRIOR and module.ROOT==ROOT,'original_helper_constants')
    return module


def save(path,value):
    raw=json.dumps(value,sort_keys=True,indent=2,allow_nan=False).encode()+b'\n'
    with path.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
    need(path.read_bytes()==raw,'output_readback')


def sync_inventory(output):
    records=[];total=0
    paths=[]
    for i,path in enumerate(output.iterdir(),1):
        need(i<=6,'output_root_inventory');safe(path);paths.append(path)
    for directory in (output/'inputs',output/'results'):
        if directory.exists():
            safe(directory);children=[]
            for i,path in enumerate(directory.iterdir(),1):
                need(i<=68,'output_pair_inventory');children.append(path)
            paths.extend(children)
    for path in paths:
        if path.is_dir():continue
        safe(path)
        # Windows _commit requires a writable descriptor; these are exclusively
        # new external outputs. No bytes are changed by this durability step.
        with path.open('r+b') as stream:
            os.fsync(stream.fileno());h=hashlib.sha256();size=0
            for block in iter(lambda:stream.read(1024*1024),b''):
                size+=len(block);total+=len(block)
                need(size<=256*1024*1024 and total<=4*1024**3,'output_byte_bound');h.update(block)
        records.append(dict(relative_path=path.relative_to(output).as_posix(),sha256=h.hexdigest(),bytes=size))
    return sorted(records,key=lambda r:r['relative_path'])


def run(output):
    output=Path(output).absolute();need(output.parent==BASE and not output.exists(),'fresh_external_output_required')
    safe(BASE);before=bindings();original=load_original();need(bindings()==before,'sources_changed_on_import')
    output.mkdir();started=time.time()
    save(output/'WRAPPER_STARTED.json',dict(started_epoch=started,source_bindings=before,
        actual_parallel_workers=1,original_executor_upper_bound=4,arithmetic_changed=False,
        no_runtime_writes=True,no_GET=True,no_fits=True))
    old_here,old_executor=original.HERE,original.ProcessPoolExecutor
    try:
        original.HERE=output;original.ProcessPoolExecutor=SerialExecutor
        original.main()
        need(bindings()==before,'sources_changed_during_assessment')
        report_path=output/'V3_COMPLETED_PERFORMANCE_20260909.json'
        _,raw=hashed(report_path,16*1024*1024);report=json.loads(raw)
        need(report['registry_sha256']==REGISTRY_SHA and report['ledgers']==68,'original_completed_report_identity')
        inventory=sync_inventory(output)
        result=dict(status=report['status'],started_epoch=started,completed_epoch=time.time(),
            source_bindings=before,output_files=inventory,actual_parallel_workers=1,
            original_assessor_unchanged=True,original_arithmetic_unchanged=True,
            scope='Fresh per-ledger read-only clocks; only original completed outcomes; counterfactual paired comparators, no independent trial claim.',
            runtime_writes=False,GET_requests=False,model_fits=False)
        save(output/'WRAPPER_COMPLETED.json',result);return result
    except Exception as error:
        save(output/'WRAPPER_FAILED.json',dict(status='failed',error_type=type(error).__name__,
            completed_epoch=time.time(),partial_outputs_preserved=True,runtime_writes=False,GET_requests=False))
        raise
    finally:original.HERE=old_here;original.ProcessPoolExecutor=old_executor


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output-directory',required=True)
    result=run(parser.parse_args().output_directory)
    print(json.dumps({'status':result['status'],'actual_parallel_workers':1}))
