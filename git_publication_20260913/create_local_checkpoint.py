"""Create an exact-byte local source checkpoint from the reviewed allowlist."""
import hashlib,json,os,stat,subprocess
from datetime import datetime,timezone
from pathlib import Path
HERE=Path(__file__).absolute().parent
REPO=HERE.parent/'trad'
TAG='checkpoint-2026-09-13-signals'
REPORT=HERE/'credential_audit_final_scope.json'
def git(*args,check=True):
    result=subprocess.run(['git','-C',str(REPO),*args],capture_output=True)
    if check and result.returncode:raise RuntimeError('Git operation failed: '+args[0]+' exit '+str(result.returncode))
    return result
def sha(raw):return hashlib.sha256(raw).hexdigest()
report_raw=REPORT.read_bytes();audit=json.loads(report_raw)
assert audit['summary']['complete_and_passed'] and audit['findings']==[] and audit['exclusions']==[]
assert git('rev-parse','HEAD').stdout.decode().strip()==audit['audited_head']
assert git('rev-parse','--show-object-format').stdout.strip()==b'sha1'
if git('diff','--cached','--name-only','-z').stdout:
    prior=json.loads((HERE/'CHECKPOINT_INTENT.json').read_bytes())
    assert prior['previous_head']==audit['audited_head'] and prior['audit_sha256']==sha(report_raw)
assert git('show-ref','--verify','--quiet','refs/tags/'+TAG,check=False).returncode==1
rows={r['path']:r for r in audit['candidate_content_digests']}
assert set(rows)==set(audit['approved_git_paths']) and len(rows)==3303
current=set(p.decode() for p in git('ls-files','--cached','--others','--exclude-standard','-z').stdout.split(b'\0') if p)
assert current==set(rows)
verified_parents=set();expected_index={};total=0
for name,row in rows.items():
    p=REPO/name
    assert not Path(name).is_absolute() and '..' not in Path(name).parts and p.is_relative_to(REPO)
    for parent in (*reversed(p.parents),p.parent):
        if parent in verified_parents:continue
        s=parent.lstat();assert stat.S_ISDIR(s.st_mode) and not getattr(s,'st_file_attributes',0)&1024
        verified_parents.add(parent)
    s=p.lstat();assert stat.S_ISREG(s.st_mode) and not stat.S_ISLNK(s.st_mode) and not getattr(s,'st_file_attributes',0)&1024
    assert s.st_size==row['size'] and s.st_size<=5*1024**2
    raw=p.read_bytes();assert len(raw)==row['size'] and sha(raw)==row['sha256']
    expected_index[name]=hashlib.sha1(('blob '+str(len(raw))+'\0').encode()+raw).hexdigest()
    total+=len(raw)
assert total<=256*1024**2
paths_path=HERE/'approved_paths.nul'
path_bytes=b'\0'.join(n.encode() for n in sorted(rows))+b'\0'
if paths_path.exists():assert paths_path.read_bytes()==path_bytes
else:
    with paths_path.open('xb') as f:f.write(path_bytes);f.flush();os.fsync(f.fileno())
intent={'status':'all_candidate_bytes_verified_before_staging','previous_head':audit['audited_head'],'files':len(rows),'bytes':total,'audit_sha256':sha(report_raw),'tag':TAG,'remote_actions':False}
if (HERE/'CHECKPOINT_INTENT.json').exists():assert json.loads((HERE/'CHECKPOINT_INTENT.json').read_bytes())==intent
else:
    with (HERE/'CHECKPOINT_INTENT.json').open('x',encoding='utf-8') as f:json.dump(intent,f,indent=2);f.flush();os.fsync(f.fileno())
git('--literal-pathspecs','add','--pathspec-from-file='+str(paths_path),'--pathspec-file-nul')
# Reprocess previously cached entries under the new exact-byte attributes.
git('--literal-pathspecs','add','--renormalize','--pathspec-from-file='+str(paths_path),'--pathspec-file-nul')
staged={}
for entry in git('ls-files','--stage','-z').stdout.split(b'\0'):
    if not entry:continue
    meta,name=entry.split(b'\t',1);mode,oid,stage=meta.split()
    assert stage==b'0' and mode in (b'100644',b'100755')
    staged[name.decode()]=oid.decode()
assert staged==expected_index, 'Staged bytes differ from the approved raw files'
git('commit','-m','Checkpoint audited Forex source and signal roadmap','-m','Preserve the current source, tests, configuration and research records. Record completed comparisons and the next signal-research decisions. Exclude generated/private runtime payloads and preserve exact file bytes. No trading or service activation.')
head=git('rev-parse','HEAD').stdout.decode().strip()
assert head!=audit['audited_head']
git('tag','-a',TAG,'-m','Audited local source checkpoint with signal-research roadmap; offline, not a profitability or deployment claim.')
assert git('rev-parse',TAG+'^{commit}').stdout.decode().strip()==head
assert not git('diff','--cached','--name-only','-z').stdout
receipt={'status':'local_git_checkpoint_committed_and_tagged','created_utc':datetime.now(timezone.utc).isoformat(),'repository':str(REPO),'branch':git('branch','--show-current').stdout.decode().strip(),'previous_head':audit['audited_head'],'commit':head,'tag':TAG,'tracked_files':len(staged),'verified_raw_source_bytes':total,'exact_worktree_to_index_blobs_verified':True,'audit_sha256':sha(report_raw),'remote_actions':False,'services_started':False,'archive_and_vault_remain_separate':True}
with (HERE/'LOCAL_CHECKPOINT_RECEIPT.json').open('x',encoding='utf-8') as f:json.dump(receipt,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
print(json.dumps(receipt))
