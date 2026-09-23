"""Prepare a selection manifest only; no project/vault copy, scan, compile or sync."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json

BASE=Path(__file__).resolve().parent
ROOT=BASE.parent/'trad'
LEDGER=BASE/'coherent_reader_design/ledger_observer_v1'
ALIAS=BASE/'coherent_reader_design/producer_alias_diagnosis_001'
RETIRE=BASE/'old_joint_retirement_preflight_v1'
sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
selected={}
def add(path,group,kind='summary_or_receipt',status='accepted_or_explicitly_dated_evidence'):
    assert path.is_file(),path
    assert path not in selected,path
    relative=path.relative_to(BASE).as_posix()
    intended='docs/validation/overnight_curve_buildout_20260909/'+relative
    if path.name.startswith('test_') and path.suffix=='.py':intended+='.txt'
    if path.suffix=='.ps1':intended+='.txt'
    selected[path]={'group':group,'kind':kind,'artifact_status':status,'original_source':{'path':str(path),'sha256':sha(path),'bytes':path.stat().st_size},'intended_member':intended,'copy_performed':False}

for path in sorted(ALIAS.iterdir()):
    if path.is_file() and path.suffix in ('.json','.md','.xml'):add(path,'publication_alias_and_immutable_boundary')
for name in ('diagnose_mutable_publication.py','EXTRACTED_FROZEN_PUBLICATION_METHODS.py.txt'):
    add(ALIAS/name,'publication_alias_and_immutable_boundary','offline_helper_or_exact_source')

for path in sorted(LEDGER.iterdir()):
    if path.is_file() and path.suffix in ('.json','.md','.xml') and not path.name.startswith('RETAINED_'):
        add(path,'independent_ledger_api_and_table')
for name in ('apply_reviewed_api_handler.py','verify_live_api_table.py','retain_live_missing_reasons.py','reload_reviewed_dashboard.ps1'):
    add(LEDGER/name,'independent_ledger_api_and_table','offline_helper_or_historical_operational_source')
for folder in ('accepted_source','api_accepted_source','api_before_generation_review','table_visibility_accepted_source','table_visibility_before','table_visibility_before_expiry_review'):
    for path in sorted((LEDGER/folder).iterdir()):
        if path.is_file():add(path,'independent_ledger_api_and_table','exact_source_or_manifest')
for folder,names in {
 'handler_application_001':['HANDLER_APPLICATION_20260909.json','dashboard_before.py.txt','dashboard_applied.py.txt'],
 'handler_portability_001':['api_portable_handler_final_tests.xml','HANDLER_APPLICATION_INDEPENDENT_REVIEW_20260909.json','PORTABLE_HANDLER_IMPLEMENTATION_VALIDATION_20260909.json','test_oanda_joint_v3_ledger_observer_handler_draft.py.txt','test_oanda_joint_v3_ledger_observer_handler_v1.py.txt'],
 'api_runtime_preparation_001':['PINNED_API_CONFIGURATION_PREPARATION_20260909.json','joint_v3_ledger_observer_api_v1_20260909.json','dashboard_handler_pinned.patch','dashboard_handler_pinned.py.txt'],
 'actual_candidate_001':['CANDIDATE_VALIDATION_RECEIPT_20260909.json'],
 'actual_api_candidate_001':['API_ACTUAL_CANDIDATE_VALIDATION_20260909.json'],
 'actual_api_candidate_002':['API_ACTUAL_CANDIDATE_VALIDATION_20260909.json'],
 'actual_live_api_table_001':['LIVE_API_TABLE_ACCEPTANCE_20260909.json','LIVE_MISSING_LEDGER_REASONS_20260909.json'],
}.items():
    for name in names:add(LEDGER/folder/name,'independent_ledger_api_and_table')

for path in sorted(RETIRE.iterdir()):
    if path.is_file() and path.suffix in ('.json','.xml','.md','.py','.ps1'):
        add(path,'old_joint_retirement_and_reload','offline_helper_or_receipt')
for path in sorted((RETIRE/'before_source').iterdir()):
    if path.is_file():add(path,'old_joint_retirement_and_reload','exact_before_source')
for folder,names in {
 'actual_observation_001':['OLD_JOINT_RETIREMENT_OBLIGATIONS_20260909.json'],
 'actual_observation_002':['OLD_JOINT_RETIREMENT_OBLIGATIONS_20260909.json'],
 'before_combined_reload_001':['OLD_JOINT_RETIREMENT_OBLIGATIONS_20260909.json','OLD_JOINT_RETIREMENT_IDENTITY_COMPARISON_20260909.json'],
 'combined_reload_001':['BEFORE.json','FAILURE.json'],
 'combined_reload_recovery_001':['BEFORE.json','AFTER.json'],
 'combined_reload_recovery_001_post_obligations':['OLD_JOINT_RETIREMENT_OBLIGATIONS_20260909.json','OLD_JOINT_RETIREMENT_IDENTITY_COMPARISON_20260909.json'],
}.items():
    for name in names:add(RETIRE/folder/name,'old_joint_retirement_and_reload')

# Inspect JSON keys, not values printed to stdout. These selected observations must
# contain only compact proofs/status/identities, never raw accounts/news/commands.
blocked_keys={'raw_payload','article_body','article_text','account_id','accountid','account_ids','authorization','access_token','api_key','commandline','command_line'}
def check_keys(value,path):
    if isinstance(value,dict):
        for key,child in value.items():
            assert key.lower() not in blocked_keys,(path,key)
            check_keys(child,path)
    elif isinstance(value,list):
        for child in value:check_keys(child,path)
for path in selected:
    if path.suffix=='.json':check_keys(json.loads(path.read_text(encoding='utf-8-sig')),path)

native_names=['oanda_immutable_summary_publication_v1.py','test_oanda_immutable_summary_publication_v1.py','oanda_joint_v3_ledger_status_observer_v1.py','test_oanda_joint_v3_ledger_status_observer_v1.py','oanda_joint_v3_ledger_observer_api_v1.py','test_oanda_joint_v3_ledger_observer_api_v1.py','oanda_practice_live_dashboard.py','oanda_main_signal_dashboard.html','test_oanda_joint_v3_ledger_observer_handler_v1.py','test_oanda_joint_v3_ledger_table_visibility.py','config/joint_v3_ledger_observer_api_v1_20260909.json','config/joint_price_news_study_v3_20260908.json','config/joint_forecast_primary_current.json','oanda_always_on_supervisor.ps1','oanda_project_runtime_health.py','start_oanda_research_collection.ps1','config/joint_study_runtime_retirement_v1_20260909.json']
dependencies=[]
for name in native_names:
    path=ROOT/name;assert path.is_file(),name
    dependencies.append({'path':str(path),'sha256':sha(path),'bytes':path.stat().st_size,'source_snapshot_member':name,'copy_performed':False,'status':'existing_canonical_source_snapshot_dependency'})
entries=list(selected.values())
manifest={'schema_version':'curated_api_table_retirement_evidence_selection_v1_20260909','status':'selection_ready_for_root_export_review','created_utc':datetime.now(timezone.utc).isoformat(),'plan':{'path':str(BASE/'PORTABLE_EVIDENCE_PLAN.md'),'sha256':sha(BASE/'PORTABLE_EVIDENCE_PLAN.md')},'scope':'Publication alias/immutable boundary, independent ledger/API/table visibility, old joint-v1/v2 terminal-obligation audit, failed initial reload and reviewed recovery. Selection only; no file copying or vault writes.','entries':entries,'canonical_source_dependencies':dependencies,'counts':{'evidence_files':len(entries),'evidence_bytes':sum(row['original_source']['bytes'] for row in entries),'canonical_source_dependencies':len(dependencies)},'dated_observation_caveats':['Root earlier09:40 operations observation reported66 eligible forecasts. Later local HTTP/table observation reported65 with67 verified ledgers: AUD_HKD hit observer_pair_time_budget, while TRY_JPY/USD_TRY had no verified publication. These observations are not additive samples and do not establish missing current training inputs.','The producer mutable-summary bug remains in the original frozen worker. The separate source-bound reader proves original committed ledger forecasts; it does not validate the mismatched old heartbeat.','Initial combined reload failed because an intended wrapper exited between two identity reads. Failure, original inventory and recovery review/actual result are retained rather than replaced.','Historical test phases overlap. Do not sum repeated XMLs into unique behavioral cases or performance sample sizes.'],'excluded_categories':['Raw API/main/HTML response captures under private_raw, including any local account/news observations.','Raw immutable producer summary/heartbeat observation inventories and full per-pair forecast snapshots.','SQLite databases/WALs, per-ledger selected quote records, raw news/article payloads and private benchmark mirrors.','Process command argument values and launcher/audit stdout.','Scanner-triggering obsolete redaction fixtures from unrelated model work.'],'portability':{'intended_member_basis':'Project-relative source-snapshot member under docs/validation/overnight_curve_buildout_20260909; outer ZIP prefixes are exporter-owned.','tests_and_operational_helpers':'Test sources and historical PowerShell helpers use .txt destinations to prevent automatic collection or accidental execution. Exact bytes/hashes remain unchanged.','private_dependencies_required_for_replay':['Original joint-v3 registered68 SQLite ledgers and activation metadata.','Retained raw summary/heartbeat observation files for exact45-sample alias reproduction.','Private full API responses/earlier original quote fixtures for dated actual-case replay.','The original Windows runtime/process identities cannot be recreated from a portable source export.'],'runtime_requirements':'Python3.12, Node.js and applicable original pinned project/dependency sources; a source hash does not mean model weights or runtime data are included.'},'privacy_and_export_checks':{'selected_json_private_field_key_check':'passed','raw_capture_and_runtime_database_excluded':True,'credential_scanner_run':False,'export_compile_run':False,'vault_write_performed':False,'required_before_publication':'Root must run the existing credential-aware scanner, compile exported Python source, verify unchanged inventory/hashes, and use the existing vault synchronizer/readability checks.'}}
for path,row in selected.items():assert sha(path)==row['original_source']['sha256'],path
for row in dependencies:assert sha(Path(row['path']))==row['sha256'],row['path']
out=BASE/'CURATED_API_TABLE_RETIREMENT_EVIDENCE_SELECTION_20260909.json'
with out.open('xb') as f:f.write((json.dumps(manifest,indent=2,sort_keys=True)+'\n').encode())
md=BASE/'CURATED_API_TABLE_RETIREMENT_EVIDENCE_SELECTION_20260909.md'
text=f'''# API, table and retirement evidence selection

Selected {len(entries)} exact evidence files ({sum(row['original_source']['bytes'] for row in entries):,} bytes), plus {len(dependencies)} existing canonical source dependencies. This is a selection manifest, not a completed project copy or vault publication. Every entry records the original absolute path, SHA256, byte count and intended project-relative archive member.

The selection preserves the producer alias finding, immutable-boundary and independent-ledger/API proof, table source and expiry corrections, final131-case validation, actual served-HTML/API acceptance, terminal-obligation audit, failed initial reload, and recovery. Earlier test/source phases remain dated and must not be added as independent samples.

The later HTTP observation selected65 current original forecasts after67/68 ledgers were verified. AUD_HKD was withheld by the observer's per-pair time budget; TRY_JPY/USD_TRY had no verified original publication. This does not contradict an earlier66-forecast observation or prove a current-input failure. Current readiness remains unobserved by this reader.

Raw responses, news/articles, databases/WALs, selected raw quotes, private capture inventories and process command arguments are excluded. Receipts can bind excluded machine-local inputs without making them portable. Test sources and historical PowerShell helpers are retained as exact bytes with .txt destination suffixes.

Before publication, root still needs the existing credential-aware scanner, export compilation and inventory checks, and the established vault synchronizer/readability verification. No vault write or source/runtime change occurred during selection.
'''
with md.open('x',encoding='utf-8',newline='\n') as f:f.write(text)
print(json.dumps({'manifest':str(out),'sha256':sha(out),'report':str(md),'report_sha256':sha(md),**manifest['counts']}))
