"""Preserve the first scanner follow-up and correct its stale stage observation."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json, sys
sys.dont_write_bytecode=True
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
VAULT=Path(r'C:\Users\zmoor\OneDrive\thevault\projects\forex')
EVIDENCE=ROOT/'docs/validation/export_scanner_optimization_20260907'
REPORT=ROOT/'docs/FOREX_EXPORT_SCANNER_OPTIMIZATION_20260907.md'
RECEIPT=ROOT/'FOREX_EXPORT_SCANNER_OPTIMIZATION_VALIDATION_20260907.json'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def load(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def encoded(v):return (json.dumps(v,indent=2)+'\n').encode('utf-8')
def exclusive(p,b):
    p.parent.mkdir(parents=True,exist_ok=True)
    with p.open('xb') as handle:handle.write(b)
def binding(p):return {'path':p.relative_to(ROOT).as_posix(),'sha256':sha(p)}
assert sha(RECEIPT)=='36bf41b54c69908151e27d56ee42aca9c9ffc208554b7a2fcc75b3476c6b668d'
value=load(RECEIPT)
assert all(sha(ROOT/r['path'])==r['sha256'] for r in value['source_bindings'])
preserved=[]
for source in (REPORT,RECEIPT):
    for destination in (OUT/'superseded_36bf'/source.name,VAULT/'maintenance/scanner_followup_superseded_36bf'/source.name):
        exclusive(destination,source.read_bytes())
        assert sha(destination)==sha(source)
        preserved.append({'source':str(source),'preserved':str(destination),'sha256':sha(source)})
stage_path=OUT/'ORPHANED_STAGE_PRESERVATION_20260907.json'
stage=load(stage_path)
exclusive(OUT/'ORPHANED_STAGE_PRESERVATION_INITIAL_METADATA_20260907.json',stage_path.read_bytes())
stage['original_directory_created_utc']='2026-09-07T18:58:02.1938797Z'
stage['original_directory_last_write_utc']='2026-09-07T18:58:04.4688636Z'
stage['directory_timestamp_note']='The first preservation receipt lazily read the removed old directory path and returned 1601 sentinel times. These corrected times match both the pre-move read-only inventory and the preserved directory. All member timestamps and hashes were captured before movement and were already correct. The initial receipt is retained separately.'
stage_path.write_bytes(encoded(stage))
assert load(VAULT/'source/WORKTREE_SOURCE_LATEST.json')['archive']=='forex_worktree_source_885144f85e85de600e4470d8.zip'
for row in stage['members']:
    assert sha(Path(stage['preserved_resolved_path'])/row['name'])==row['sha256']
failure={'status':'preflight_stopped_before_publication','recorded_utc':datetime.now(timezone.utc).isoformat(),
         'exit_code':1,'error':'ValueError: unexpected_staged_source_snapshot',
         'writes_by_failed_resume':False,'first_scanner_receipt_sha256':sha(RECEIPT),
         'recovery':'Preserved orphan stage with hashes outside source staging; no deletion or published pointer replacement.',
         'superseded_scanner_record_copies':preserved}
failure_path=OUT/'RESUME_FIRST_PREFLIGHT_FAILURE_20260907.json'
exclusive(failure_path,encoded(failure))
text=REPORT.read_text(encoding='utf-8')
start=text.index('The first operational publisher copied')
end=text.index('\n\nOnly the greedy',start)
text=text[:start]+'''The first operational publisher copied the 149 records and preserved the previous records, then spent more than 16 minutes in its initial source credential scan. The new minute-gap source capture contains a 252,696-byte uninterrupted encoded identifier run. The old assignment expression retried an unbounded greedy key prefix at each byte, producing quadratic work on that run. At 18:58:26 UTC the verified publisher child was stopped; its matching launcher exited. No other process was targeted.

The earlier observation that staging had not begun was stale by termination. The first resume preflight later found a ZIP created at 18:58:02 UTC and a manifest written at 18:58:04 UTC. This interrupted stage contains 1,985 files and the prior scanner; no successful verification or publication receipt exists for it. The ZIP and manifest were moved without deletion to `maintenance/interrupted_source_stage_20260907_185802`, with their exact hashes and member timestamps retained. The published 885144 archive and pointer remained unchanged. The first resume stopped before any publication writes.

The superseded scanner report and receipt (SHA-256 `36bf41b54c69908151e27d56ee42aca9c9ffc208554b7a2fcc75b3476c6b668d`) are preserved separately in workspace evidence and vault maintenance. The original interruption record is retained as written; the timestamped orphan-stage preservation evidence corrects its pre-staging assumption. A metadata-only correction also retains the initial preservation receipt: a lazy directory timestamp read after movement returned sentinel dates, while all member timestamps/hashes were valid; directory times now agree with both pre-move inspection and the preserved directory.''' + text[end:]
REPORT.write_text(text,encoding='utf-8')
new_paths=[]
for source in (stage_path,OUT/'ORPHANED_STAGE_PRESERVATION_INITIAL_METADATA_20260907.json',failure_path,Path(__file__)):
    target=EVIDENCE/source.name
    exclusive(target,source.read_bytes());assert sha(target)==sha(source)
    new_paths.append(target)
value['report']=binding(REPORT)
value['source_bindings']=[binding(ROOT/r['path']) for r in value['source_bindings']]+[binding(p) for p in new_paths]
value['observed_utc']=datetime.now(timezone.utc).isoformat()
value['superseded_scanner_followup']={'sha256':'36bf41b54c69908151e27d56ee42aca9c9ffc208554b7a2fcc75b3476c6b668d','reason':'Staging began between the prior observation and publisher termination. Preserve and correct that timing claim.','preserved_copies':preserved}
value['orphaned_stage_preservation']=stage
value['first_resume_preflight']=failure
sys.path[:0]=[str(ROOT),str(ROOT.parent)]
from tools import vault_worktree_snapshot as snapshot
for p in [REPORT,*new_paths]:snapshot.audit_payload(p.relative_to(ROOT).as_posix(),p.read_bytes(),set())
snapshot.audit_payload(RECEIPT.name,encoded(value),set())
RECEIPT.write_bytes(encoded(value))
assert all(sha(ROOT/r['path'])==r['sha256'] for r in value['source_bindings'])
print(json.dumps({'status':'corrected_and_preserved','receipt_sha256':sha(RECEIPT),'bindings':len(value['source_bindings'])}))
