"""Pinned, bounded offline Extra Trees execution; no model/control regeneration.

The supervisor samples resource use every 100ms; limits are not OS quotas.
Only its own child is terminated on a violation. All partial artifacts remain.
"""
from pathlib import Path
import argparse,hashlib,importlib.metadata,json,os,platform,shutil,subprocess,sys,time

ROOT=Path(__file__).resolve().parent
QUALIFICATION_SHA256='208c12b14a23fae23cc633705a6893ab418d622afd11f9af357fd509d26c4339'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text(encoding='utf8'))
def encoded(v):return (json.dumps(v,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
def recipe_for(baseline,qualification):
    if sha(qualification)!=QUALIFICATION_SHA256:raise ValueError('qualification_pin')
    q=read(qualification);base=read(Path(baseline)/'RUN_IDENTITY.json')
    names=set(base['contract']['recipe']['sources'])|{'extra_trees_operator_v1.py','extra_trees_matched_v1.py','matched_campaign_models_v2.py','matched_campaign_runner_v2.py','publication.py','contracts.py'}
    return {'schema':'forex.extra_trees_execution_recipe.v1','sources':{n:sha(ROOT/n) for n in sorted(names)},
        'qualification_sha256':QUALIFICATION_SHA256,'input_run_identity':q['input_run_identity'],
        'baseline_run_identity':q['baseline_run_identity'],'resources':q['resource_limits'],
        'environment':{'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in ['numpy','pandas','scipy','scikit-learn','joblib','threadpoolctl','psutil']}},
        'scope':'one qualified inspected-development candidate; original controls reused; no promotion or trading'}

def check_recipe(args):
    if sha(args.recipe)!=args.recipe_sha256:raise ValueError('execution_recipe_pin')
    r=read(args.recipe)
    if r!=recipe_for(args.baseline,args.qualification):raise ValueError('execution_source_or_environment_changed')
    return r

def violation(sample,limits):
    if sample['elapsed_seconds']>limits['max_seconds']:return 'total_time'
    if sample['rss_bytes']>limits['max_rss_bytes']:return 'rss'
    if sample['output_bytes']>limits['max_output_bytes']:return 'output'
    if sample['free_bytes']<limits['min_free_disk_bytes']:return 'free_disk'
    if sample.get('phase_limit') is not None and sample['phase_elapsed']>sample['phase_limit']:return 'phase_time'
    return None

def deny_reads(roots):
    denied=[str(Path(p).resolve()).casefold().rstrip('\\/') for p in roots]
    def audit(event,args):
        if event in {'open','os.listdir','os.scandir'} and args and isinstance(args[0],(str,bytes,os.PathLike)):
            path=os.path.abspath(os.fsdecode(args[0])).casefold().rstrip('\\/')
            if any(path==r or path.startswith(r+os.sep) for r in denied):
                raise PermissionError('original_dependency_access_refused')
    sys.addaudithook(audit)

def stop_worker(proc):
    import psutil
    try:owned=proc.children(recursive=True)+[proc]
    except psutil.NoSuchProcess:return
    for child in reversed(owned[:-1]):
        try:child.kill()
        except psutil.NoSuchProcess:pass
    try:proc.kill()
    except psutil.NoSuchProcess:pass

def status(args,r):
    sys.path.insert(0,str(ROOT))
    from publication import verify_completed_run
    for root,expected in [(args.input,r['input_run_identity']),(args.baseline,r['baseline_run_identity'])]:
        identity=read(root/'RUN_IDENTITY.json')
        if identity['fingerprint']!=expected:raise ValueError('status_parent_identity')
        verify_completed_run(root,identity)
    root=args.runs/'extra-trees-matched-development'
    if not root.exists():return {'status':'ready','next_action':'run with the identical pinned recipe','models_fitted':0}
    if not (root/'COMPLETION_MANIFEST.json').exists():return {'status':'resumable','next_action':'resume identical recipe; lock and journal verified by resume','models_fitted':0}
    identity=read(root/'RUN_IDENTITY.json');e=identity['contract']['experiment']
    if e['source_hashes']!=r['sources'] or e['input_identity']!=r['input_run_identity'] or e['baseline_identity']!=r['baseline_run_identity'] or e['qualification_sha256']!=r['qualification_sha256']:
        raise ValueError('status_completed_identity_mismatch')
    m=verify_completed_run(root,identity)
    return {'status':'completed_verified','run_identity':identity['fingerprint'],'payloads':len(m['payloads']),'models_fitted':0,'next_action':'review saved results; use --verify-replay for numerical replication'}

def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['input','baseline','qualification','runs','recipe','receipt']:
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--resume',action='store_true');p.add_argument('--worker',action='store_true')
    p.add_argument('--verify-replay',action='store_true')
    p.add_argument('--status',action='store_true')
    p.add_argument('--deny-read-root',action='append',default=[])
    args=p.parse_args()
    if args.deny_read_root:deny_reads(args.deny_read_root)
    r=check_recipe(args)
    if args.status:
        result=status(args,r);print(json.dumps(result));return 0
    args.runs.mkdir(parents=True,exist_ok=True);args.receipt.parent.mkdir(parents=True,exist_ok=True)
    phase_path=args.receipt.with_suffix('.phase.json')
    if args.worker:
        sys.path.insert(0,str(ROOT))
        import extra_trees_matched_v1 as candidate
        def phase(name,limit):
            tmp=phase_path.with_suffix('.tmp');tmp.write_bytes(encoded({'phase':name,'monotonic':time.monotonic(),'limit':limit}));os.replace(tmp,phase_path)
        phase('authentication',None)
        if args.verify_replay:
            result=candidate.replay_saved(args.input,args.baseline,args.runs/'extra-trees-matched-development')
        else:
            result=candidate.run(args.input,args.baseline,args.qualification,args.runs,resume=args.resume,phase=phase)
        check_recipe(args)
        args.receipt.with_suffix('.result.json').write_bytes(encoded(result));return 0
    import psutil
    limits=r['resources'];start=time.monotonic()
    if shutil.disk_usage(args.runs).free<limits['min_free_disk_bytes']:raise ValueError('insufficient_free_disk')
    env={**os.environ,**{k:'1' for k in ['OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS']}}
    # Receipt paths must be unique per activation; prior attempts remain intact.
    if args.receipt.exists() or phase_path.exists():raise ValueError('receipt_already_exists')
    log=args.receipt.with_suffix('.worker.txt').open('wb')
    child=subprocess.Popen([sys.executable,'-I','-B',str(Path(__file__).resolve()),*sys.argv[1:],'--worker'],env=env,stdout=log,stderr=subprocess.STDOUT)
    proc=psutil.Process(child.pid);peak=0;reason=None;sample={}
    try:
        while True:
            exited=child.poll() is not None
            rss=0
            if not exited:
                try:owned=[proc,*proc.children(recursive=True)]
                except psutil.NoSuchProcess:owned=[]
                for member in owned:
                    try:rss+=member.memory_info().rss
                    except psutil.NoSuchProcess:pass
            peak=max(peak,rss)
            sample={'elapsed_seconds':time.monotonic()-start,'rss_bytes':rss,'output_bytes':sum(f.stat().st_size for f in args.runs.rglob('*') if f.is_file()),'free_bytes':shutil.disk_usage(args.runs).free}
            if phase_path.exists() and not exited:
                phase=read(phase_path);sample.update(phase_limit=phase['limit'],phase_elapsed=time.monotonic()-phase['monotonic'],phase=phase['phase'])
            reason=violation(sample,limits)
            if reason:
                if not exited:stop_worker(proc);child.wait(timeout=10)
                break
            if exited:break
            time.sleep(.1)
    finally:
        if child.poll() is None:stop_worker(proc);child.wait(timeout=10)
        log.close()
        args.receipt.write_bytes(encoded({'status':'pass' if child.returncode==0 and reason is None else 'failed','exit_code':child.returncode,'violation':reason,'peak_rss_bytes':peak,'sample':sample,'recipe_sha256':args.recipe_sha256,'resource_enforcement':'100ms sampled supervisor; no OS quota','models_in_parent':0}))
    if child.returncode or reason:return 2
    check_recipe(args);print(args.receipt.with_suffix('.result.json').read_text());return 0

if __name__=='__main__':raise SystemExit(main())
