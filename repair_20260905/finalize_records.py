"""Build dated, hash-bound repair records after all offline checks complete."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import xml.etree.ElementTree as ET

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / 'trad'
NOW = datetime.now(timezone.utc).isoformat()

def read(path): return json.loads(path.read_text(encoding='utf-8-sig'))
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def write(path, value): path.write_text(json.dumps(value,indent=2)+'\n',encoding='utf-8')

sources = ['oanda_practice_top_signal_executor.py','oanda_practice_shadow_strategy_lab.py',
    'oanda_causal_source_factor_response_map_v9.py','oanda_source_conditioned_currency_rank_v8.py',
    'oanda_source_publication_successor_integrity.py','oanda_prospective_governance.py',
    'oanda_source_governance_news_fast_lane.py','oanda_source_governance.py','oanda_project_integrity_audit.py',
    'tools/credential_audit.py','tools/vault_worktree_snapshot.py','oanda_always_on_supervisor.ps1',
    'forex_model_vault_sync.py','config/source_factor_response_v9.json','config/source_conditioned_currency_rank_v8.json',
    'config/shadow_runtime_retirements_v1.json','test_oanda_practice_top_signal_executor.py',
    'test_credential_audit.py','test_vault_worktree_snapshot.py','test_oanda_source_governance_news_fast_lane.py',
    'test_oanda_prospective_governance.py','test_oanda_source_publication_successors.py',
    'test_oanda_source_v9_state_publication.py','test_oanda_shadow_runtime_retirements.py',
    'test_oanda_publication_repair_integration.py','test_oanda_causal_source_factor_response_map_v8.py',
    'test_oanda_source_conditioned_currency_rank_v7.py','docs/VAULT_RECREATION_CURRENT.md']
policy_path=ROOT/'config/shadow_runtime_retirements_v1.json'
policy=read(policy_path)
policy['updated_utc']=NOW
write(policy_path,policy)

suite_paths=['execution/pytest_execution.xml','security/pytest_security.xml','integration_tests.xml']
suites=[]
unique=set()
for relative in suite_paths:
    path=OUT/relative
    tree=ET.parse(path)
    cases=list(tree.iter('testcase'))
    failures=sum(case.find('failure') is not None for case in cases)
    errors=sum(case.find('error') is not None for case in cases)
    skipped=sum(case.find('skipped') is not None for case in cases)
    assert failures==errors==0, relative
    for case in cases:
        if case.find('skipped') is None: unique.add((case.get('classname'),case.get('name')))
    suites.append({'path':relative,'sha256':sha(path),'tests':len(cases),'passed':len(cases)-skipped,
                   'skipped':skipped,'failures':failures,'errors':errors})

performance=read(OUT/'PERFORMANCE_AUDIT.json')
performance['repair_fixture_benchmarks']={
    'execution':read(OUT/'execution/execution_fixture_performance.json'),
    'fastlane':read(OUT/'fastlane/performance.json')}
performance['repair_fixture_benchmark_limit']='Disposable offline fixtures; not live throughput, loaded broker latency or strategy profitability.'
write(ROOT/'FOREX_PERFORMANCE_AUDIT_20260905.json',performance)

original=read(OUT.parent/'audit_20260905/AUDIT_FINDINGS.json')
remaining={
    'A03':'The successor is disabled and requires explicit future activation and new independent prospective evidence.',
    'A04':'Historical availability-clock exposure remains unestablished; old receipts are immutable diagnostics.',
    'A06':'A complete clean integrity cycle remains required after a separately requested restart.'}
outcomes=[]
for finding in original['findings']:
    key=finding['id']
    outcomes.append({'audit_id':key,'priority':finding['priority'],'title':finding['title'],
                     'source_fix_implemented':True,'offline_validation':'passed',
                     'acceptance_status':'implemented_pending_operational_or_historical_validation' if key in remaining else 'complete_for_stated_offline_repair_scope',
                     'remaining_validation':remaining.get(key),
                     'historical_production_impact':'not_established'})

artifact_paths=[*suite_paths,'fastlane/repair_receipt.json','fastlane/performance.json',
    'fastlane/SUCCESSOR_REVIEW.md','fastlane/successor_publication_repair.json',
    'fastlane/RANK_RESTART_REVIEW.md','fastlane/rank_restart_review_after_exit_fix.json',
    'fastlane/REPAIR_REPORT.md','execution/EXECUTION_REPAIR_REPORT.md','execution/execution_fixture_performance.json',
    'security/SECURITY_REPAIR_REPORT.md','security/security_source_bindings.json',
    'performance_review.json','structure_audit.json','credential_final_scan.json','stopped_state_final.json']
artifact_paths += [str(p.relative_to(OUT)).replace('\\','/') for p in (OUT/'evidence').glob('*') if p.is_file() and p.suffix in {'.json','.md','.xml','.py'}]
receipt={
    'schema_version':'forex_independent_audit_repair_validation_v1','generated_utc':NOW,
    'canonical_project':str(ROOT),'status':'source_repairs_offline_verified_runtime_stopped',
    'supported_decision':'no_trade','runtime_restarted':False,'collection_activated':False,
    'broker_requests':0,'production_database_writes':0,'old_audit_outputs_modified':False,
    'original_findings_sha256':sha(OUT.parent/'audit_20260905/AUDIT_FINDINGS.json'),
    'pre_repair_source_archive':'forex_worktree_source_05b9f6405123bc9dfe5969df.zip',
    'pre_repair_source_archive_sha256':'abea6c0adf4cb3953b7205ee8462c059e83f62fc0961c04ec379dd2c56609515',
    'findings':outcomes,
    'additional_advisory':{'component':'prospective_governance_discovery_lock',
        'source_fix_implemented':True,'offline_validation':'passed','production_surface':'unwired; no confirmation or routing authority activated'},
    'test_suites':suites,'unique_passing_test_cases_across_recorded_suites':len(unique),
    'excluded_tests':{'execution_publisher_thread_tests':5,'reason':'Thread starts prohibited during stopped-state execution audit; these are not claimed passing.'},
    'source_bindings':[{'path':name,'sha256':sha(ROOT/name),'bytes':(ROOT/name).stat().st_size} for name in sources],
    'local_evidence_root':str(OUT),
    'local_evidence_files':[{'path':name,'sha256':sha(OUT/name),'bytes':(OUT/name).stat().st_size} for name in sorted(set(artifact_paths))],
    'performance_record':{'path':'FOREX_PERFORMANCE_AUDIT_20260905.json','sha256':sha(ROOT/'FOREX_PERFORMANCE_AUDIT_20260905.json')},
    'independent_reviews':[
        'Execution reviewer verified credential literal contexts and final root supervisor/integrity/recreation integration.',
        'Fastlane reviewer reproduced delayed rank-selection and inherited source-publication faults; repaired successors were retested.',
        'Performance reviewer compared account, research, latency and resource claims with saved inputs and fixture receipts.'],
    'limits':[
        'Focused tests are not an exhaustive audit of all historical modules.',
        'The last complete saved runtime integrity report is still degraded; no replacement live pass was run.',
        'Source V9 and rank V8 ship disabled and have no collected prospective performance proof.',
        'Old source/rank and fastlane V1/V2 evidence was not rewritten or imported into repaired prospective proof.',
        'Final local checks precede the external broker action; they are not broker-side atomic revocation.',
        'Full production-sized concurrency/load and broker integration were not tested.',
        'Vault export verification is a separate dated receipt; it attests local bytes, not completed OneDrive cloud upload.']}
write(ROOT/'FOREX_REPAIR_VALIDATION_20260905.json',receipt)
receipt_hash=sha(ROOT/'FOREX_REPAIR_VALIDATION_20260905.json')

register=read(ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json')
existing={row['issue_id']:row for row in register['issues']}
old_ids={'A04':'FX-20260905-FASTLANE-DURABLE-AVAILABILITY','A06':'FX-20260905-FASTLANE-PUBLICATION-STATE','A07':'FX-20260905-FASTLANE-RETRY-CURSOR'}
new_ids={'A01':'FX-20260905-FINAL-CANARY-REVOCATION','A02':'FX-20260905-CONVERSION-FAIL-CLOSED',
    'A03':'FX-20260905-SOURCE-PUBLICATION-ENTRY-CLOCK','A05':'FX-20260905-FASTLANE-LATE-COMMIT',
    'A08':'FX-20260905-CREDENTIAL-LITERAL-CLASSIFICATION','A09':'FX-20260905-SOURCE-ONLY-RECOVERY-GUIDE'}
components={'A01':'practice_top_signal_executor','A02':'practice_currency_conversion','A03':'source_publication_successors',
    'A05':'news_source_governance_fast_lane','A08':'credential_source_export_preflight','A09':'vault_recreation_documentation'}
for finding in outcomes:
    key=finding['audit_id']; issue_id=(old_ids|new_ids)[key]
    row=existing.get(issue_id)
    if row is None:
        row={'issue_id':issue_id,'title':finding['title'],'priority':finding['priority'],'owner_component':components[key],
             'status':'open','acceptance_tests':['Reproduce the reported failure on isolated fixtures or inspect the source-only guide mismatch',
             'Verify the repaired invariant and valid positive behavior while preserving original evidence and stopped runtime'],
             'evidence_paths':['FOREX_INDEPENDENT_AUDIT_20260905.json','docs/FOREX_REPAIR_REVIEW_20260905.md'],
             'status_history':[{'at_utc':original['generated_utc'],'status':'open'}]}
        register['issues'].insert(0,row)
    row['status']='implemented_collecting' if key in remaining else 'complete'
    row['status_history'].append({'at_utc':NOW,'status':row['status']})
    row['validation_artifacts']=[{'path':'FOREX_REPAIR_VALIDATION_20260905.json','sha256':receipt_hash}]
    row['implementation_ref']='FOREX_REPAIR_VALIDATION_20260905.json#'+key
    row['remaining_validation']=remaining.get(key)
    row['runtime_disposition']='stopped; no collection activated'
advisory_id='FX-20260905-DISCOVERY-LOCK-DEFINITION-BOUNDARY'
assert advisory_id not in existing, 'Finalization already applied; inspect before rerunning.'
register['issues'].insert(0,{'issue_id':advisory_id,'title':'Bind unwired discovery locks to frozen definitions and actual registration time',
    'priority':'P2','owner_component':'prospective_governance','status':'complete',
    'acceptance_tests':['Reject caller-altered frozen cell definitions and backdated lock/confirmation requests',
                        'Accept valid future confirmation registration without activating live confirmation or routing'],
    'evidence_paths':['docs/FOREX_REPAIR_REVIEW_20260905.md'],
    'validation_artifacts':[{'path':'FOREX_REPAIR_VALIDATION_20260905.json','sha256':receipt_hash}],
    'status_history':[{'at_utc':original['generated_utc'],'status':'open'},{'at_utc':NOW,'status':'complete'}]})
register['generated_utc']=NOW
write(ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json',register)
print(json.dumps({'receipt_sha256':receipt_hash,'unique_passing_test_cases':len(unique),'test_suites':suites,
                  'issue_count':len(register['issues']),'status_counts':{s:sum(r['status']==s for r in register['issues']) for s in sorted({r['status'] for r in register['issues']})}},indent=2))
