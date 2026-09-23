"""Publish and independently verify compact canonical records and a source archive."""
from pathlib import Path
from hashlib import sha256
from datetime import datetime,timezone
import json,subprocess,sys
BASE=Path(__file__).resolve().parent
ROOT=BASE.parent
PROJECT=ROOT/'trad'
VAULT=Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')

def digest(path):return sha256(path.read_bytes()).hexdigest()
def save(name,value):
    with (BASE/name).open('x',encoding='utf-8') as f:json.dump(value,f,indent=2,allow_nan=False);f.write('\n')
def run(label,args):
    result=subprocess.run([sys.executable,'-B',*map(str,args)],cwd=PROJECT,capture_output=True,
                          text=True,encoding='utf-8',creationflags=subprocess.CREATE_NO_WINDOW)
    save(label+'_RESULT_20260908.json',{'returncode':result.returncode,'stdout':result.stdout,
        'stderr':result.stderr,'completed_utc':datetime.now(timezone.utc).isoformat()})
    if result.returncode:raise ValueError('export_stage_failed:'+label)
    return json.loads(result.stdout)

def main():
    validation=PROJECT/'FOREX_NEWS_RESEARCH_DEPLOYMENT_VALIDATION_20260908.json'
    expected=digest(validation)
    canonical=run('CANONICAL_EXPORT',[PROJECT/'forex_model_vault_sync.py','--root',ROOT,'--destination',VAULT,'--canonical-only'])
    assert canonical['file_count']==179
    records=[]
    for destination in canonical['destinations']:
        for row in destination['canonical_project_records']['records']:
            assert digest(ROOT/row['source'])==digest(VAULT/row['name'])==row['sha256']
            records.append({'source':row['source'],'destination':row['name'],'sha256':row['sha256']})
    assert len(records)==179
    print(json.dumps({'stage':'canonical_records_verified','records':179}),flush=True)
    snapshot=run('SOURCE_EXPORT',[PROJECT/'tools/vault_worktree_snapshot.py','--root',PROJECT,'--vault-project',VAULT])
    assert snapshot['verification']['status']=='passed'
    archive,manifest=VAULT/'source'/snapshot['archive'],VAULT/'source'/snapshot['manifest']
    assert digest(archive)==snapshot['archive_sha256'] and digest(manifest)==snapshot['manifest_sha256']
    members=json.loads(manifest.read_bytes())
    assert members['credential_audit']['passed']
    for row in members['files']:assert digest(PROJECT/row['path'])==row['sha256']
    print(json.dumps({'stage':'source_verified','files':members['file_count']}),flush=True)
    built=run('READABILITY_BUILD',[PROJECT/'tools/audit_forex_vault_readability.py','--vault-project',VAULT,'--build'])
    checked=run('READABILITY_CHECK',[PROJECT/'tools/audit_forex_vault_readability.py','--vault-project',VAULT,'--check'])
    readability=json.loads((VAULT/'VAULT_READABILITY_REPORT.json').read_bytes())
    assert readability['status']=='pass_with_declared_external_dependencies'
    assert readability['record_count']==179 and readability['source_archive_sha256']==snapshot['archive_sha256']
    for row in records:assert digest(ROOT/row['source'])==digest(VAULT/row['destination'])==row['sha256']
    assert digest(validation)==expected
    receipt={'status':'passed','verified_utc':datetime.now(timezone.utc).isoformat(),'canonical_record_count':179,
             'records':records,'source_snapshot':snapshot,'source_file_count':members['file_count'],
             'credential_audit':members['credential_audit'],'validation_sha256':expected,
             'readability_build':built,'readability_check':checked,
             'readability_report_sha256':digest(VAULT/'VAULT_READABILITY_REPORT.json'),
             'knowledge_index_sha256':digest(VAULT/'KNOWLEDGE_INDEX.md'),
             'cloud_sync_observed':False,'private_databases_or_credentials_exported':False,
             'runtime_or_broker_actions':False}
    save('FINAL_VAULT_VERIFICATION_20260908.json',receipt)
    print(json.dumps({'status':'passed','canonical_records':179,'source_files':members['file_count'],
                      'archive':str(archive),'receipt_sha256':digest(BASE/'FINAL_VAULT_VERIFICATION_20260908.json')}),flush=True)

if __name__=='__main__':main()
