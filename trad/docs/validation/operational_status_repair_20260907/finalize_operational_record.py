"""Finalize only after an explicit, source-bound live/UI validation manifest exists.

This script prepares canonical documentation and validation evidence. It does
not reload workers, modify registered study sources, or publish the vault.
"""
from datetime import datetime, timezone
from pathlib import Path
import hashlib, json, shutil, sys, xml.etree.ElementTree as ET

sys.dont_write_bytecode = True
OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / 'trad'
EVIDENCE_REL = 'docs/validation/operational_status_repair_20260907'
REPORT = 'docs/FOREX_OPERATIONAL_STATUS_REPAIR_20260907.md'
RECEIPT = 'FOREX_OPERATIONAL_STATUS_REPAIR_VALIDATION_20260907.json'
FINAL_INPUTS = 'FINALIZATION_INPUTS.json'
PRIOR = {
    'FOREX_PAIR_DASHBOARD_CONSISTENCY_VALIDATION_20260907.json': 'b84890295124ae0dc6a0dae522f7c2aa51e0db5b55302287f9a54859ec966d59',
    'FOREX_PAIR_FORECAST_COVERAGE_VALIDATION_20260907.json': '6953f2d7259be748847ec0dd6a34abd1b2f482dd8be7aa6913bc7d906ff4cfb5',
    'FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json': '3671297affd5addd4c2e4080c66d4d74382c24678a3430d7e4694814e5993792',
}
NAV_FILES = ('README.md', 'FOREX_AUDIT_START_HERE.md', 'docs/AUDIT_STATE_CURRENT.md',
             'docs/VAULT_RECREATION_CURRENT.md', 'docs/SYSTEM_ORIENTATION_CURRENT.md')
MUTABLE_RECORDS = (*NAV_FILES, 'FOREX_PROJECT_LOG.md', 'FOREX_PENDING_IMPROVEMENTS.md',
                   'forex_model_vault_sync.py', 'FOREX_AUDIT_STATE_CURRENT.json', 'FOREX_COMMONS_CURRENT.json')

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path): return json.loads(path.read_text(encoding='utf-8-sig'))
def write(path, value): path.write_text(json.dumps(value, indent=2)+'\n', encoding='utf-8')
def within(root, relative):
    path = (root/relative).resolve()
    if not path.is_relative_to(root.resolve()) or path == root.resolve():
        raise ValueError('evidence_path_escape')
    return path
def verified_file(root, row):
    path = within(root, row['path'])
    if not path.is_file() or sha(path) != row['sha256']:
        raise ValueError('bound_file_mismatch:'+row['path'])
    return path
def counts(path):
    suites = list(ET.parse(path).getroot().iter('testsuite'))
    if not suites: raise ValueError('missing_test_suites')
    result = {k:sum(int(s.attrib.get(k,0)) for s in suites) for k in ('tests','errors','failures','skipped')}
    if result['tests'] <= 0 or result['errors'] or result['failures']:
        raise ValueError('tests_not_passed')
    return result
def insert_after_title(path, text):
    current = path.read_text(encoding='utf-8')
    if 'September 7 operational-status repair:' in current:
        raise ValueError('operational_navigation_already_present')
    first, rest = current.split('\n',1)
    path.write_text(first+'\n\n'+text+'\n\n'+rest.lstrip('\n'),encoding='utf-8')

def main():
    manifest = load(OUT/FINAL_INPUTS)
    if manifest.get('status') != 'source_frozen_tests_runtime_and_ui_verified':
        raise ValueError('final_evidence_not_authorized_or_ready')
    for name, digest in PRIOR.items():
        if sha(ROOT/name) != digest: raise ValueError('prior_receipt_changed:'+name)
    for name, digest in manifest['tested_source_sha256'].items():
        if sha(within(ROOT,name)) != digest: raise ValueError('tested_source_changed:'+name)
    # Source bytes used by both registered families remain exact. No imports of
    # the workers are needed for this independent byte comparison.
    registrations = {}
    for name in ('config/pair_local_forecast_study_v1_20260907.json',
                 'config/causal_forecast_study_eurusd_v1_20260907.json'):
        config = load(ROOT/name)
        for source, digest in config['source_bindings'].items():
            if sha(ROOT/source) != digest: raise ValueError('registered_source_changed:'+source)
        registrations[name] = {'sha256':sha(ROOT/name), 'source_bindings':config['source_bindings']}
    if registrations['config/pair_local_forecast_study_v1_20260907.json']['sha256'] != 'e0aa74fa5d20b7be983050625773834ea68d74ffc15b566ab5e03df9b3380d65':
        raise ValueError('pair_registration_changed')
    if registrations['config/causal_forecast_study_eurusd_v1_20260907.json']['sha256'] != '8c6fc84e36b7b46a872f9652210f8c5cc022013f7b8c39f07b1078e791bb318b':
        raise ValueError('eurusd_registration_changed')
    primary_tests = counts(verified_file(OUT, manifest['primary_test_xml']))
    for check in manifest['passing_json_checks']:
        payload = load(verified_file(OUT,check))
        actual = payload
        for key in check.get('status_path',['status']): actual = actual[key]
        if actual != check['expected_status']:
            raise ValueError('final_check_not_passed:'+check['path'])
    if not {'runtime','ui'}.issubset({check.get('role') for check in manifest['passing_json_checks']}):
        raise ValueError('runtime_and_ui_verification_required')
    evidence_paths = [(row, verified_file(OUT,row)) for row in manifest['evidence_files']]
    supplied = {row['path'] for row, _ in evidence_paths}
    required = {'MINUTE_GAP_AUDIT.json','MINUTE_GAP_EURUSD_SOURCE_CAPTURE.json',
        'MINUTE_GAP_EURUSD_FRESH_BROKER_GET.json','MINUTE_GAP_UPDATER_CYCLE_SNAPSHOT.json',
        'MINUTE_GAP_FINDINGS.md','FEED_AUDIT.json','FINDINGS.md','MONITOR_CONFIGURATION_SNAPSHOT.json',
        'supplemental_diagnostic/DIAGNOSTIC_ADAPTER_VALIDATION_20260907.json',
        'supplemental_diagnostic/diagnostic_report_tests.xml',
        'supplemental_diagnostic/REFERENCE_EPOCH_AUDIT.json',
        'supplemental_diagnostic/EURUSD_SUPPLEMENTAL_DIAGNOSTIC_20260907.json',
        manifest['primary_test_xml']['path']} | {check['path'] for check in manifest['passing_json_checks']}
    if not required.issubset(supplied): raise ValueError('required_bound_evidence_missing')
    report_source = verified_file(OUT,manifest['final_report'])
    report_text = report_source.read_text(encoding='utf-8')
    if any(marker in report_text for marker in ('DRAFT —','FINAL_METRICS_PENDING','FINAL_VALIDATION_PENDING')):
        raise ValueError('report_not_final')
    if (ROOT/RECEIPT).exists() or (ROOT/REPORT).exists() or (ROOT/EVIDENCE_REL).exists():
        raise ValueError('canonical_operational_record_already_exists')
    backup = OUT/'before_current_records'
    backup.mkdir(exist_ok=False)
    for name in MUTABLE_RECORDS:
        target = backup/name
        target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(ROOT/name,target)
    evidence = ROOT/EVIDENCE_REL
    evidence.mkdir(parents=True,exist_ok=False)
    for row, source in evidence_paths:
        target = within(evidence,row['path'])
        target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(source,target)
        if sha(target) != row['sha256']: raise ValueError('evidence_copy_mismatch')
    shutil.copytree(backup,evidence/'before_current_records')
    shutil.copy2(OUT/FINAL_INPUTS,evidence/FINAL_INPUTS)
    (ROOT/REPORT).write_text(report_text,encoding='utf-8')
    mapping_path=ROOT/'forex_model_vault_sync.py'
    text=mapping_path.read_text(encoding='utf-8')
    anchor='CANONICAL_PROJECT_RECORDS = (\n'
    if text.count(anchor)!=1 or RECEIPT in text: raise ValueError('unexpected_canonical_mapping')
    text=text.replace(anchor,anchor+
        '    (Path("trad/'+REPORT+'"), "OPERATIONAL_STATUS_REPAIR_CURRENT.md"),\n'+
        '    (Path("trad/'+RECEIPT+'"), "OPERATIONAL_STATUS_REPAIR_VALIDATION_CURRENT.json"),\n',1)
    mapping_path.write_text(text,encoding='utf-8')
    now=datetime.now(timezone.utc).isoformat()
    nav='''September 7 operational-status repair: the dashboard distinguishes partial
research operation, each pair's news context and eligible forward news, and
the original companion scorecard from supplemental EUR/USD diagnostics. The
six audited EUR/USD interior minute gaps were also absent from a fresh broker
response; no prices or forecasts were backfilled. Registered studies, H1 targets
and disabled order gates remain unchanged. Source-bound tests, controlled
dashboard reload and live/UI verification are recorded in the new report.
The existing 15-minute bot-health monitor stays quiet unless something meaningful
changes or needs action. Earlier reports retain their dated counts and scope.
The canonical vault mapping now contains 149 records.'''
    for name in NAV_FILES:
        if name=='FOREX_AUDIT_START_HERE.md':
            links='Vault records: `OPERATIONAL_STATUS_REPAIR_CURRENT.md` and `OPERATIONAL_STATUS_REPAIR_VALIDATION_CURRENT.json`.'
        else:
            prefix='' if name.startswith('docs/') else 'docs/'
            rp='../' if name.startswith('docs/') else ''
            links=f'[Operational repair report]({prefix}FOREX_OPERATIONAL_STATUS_REPAIR_20260907.md) · [Validation]({rp}{RECEIPT}).'
        insert_after_title(ROOT/name,nav+'\n\n'+links)
        if name=='docs/SYSTEM_ORIENTATION_CURRENT.md':
            path=ROOT/name
            text=path.read_text(encoding='utf-8').replace(
                'Architecture reference originally written 2026-08-13. For the current stopped\n'
                'runtime, source lineages and vault records read `../FOREX_AUDIT_START_HERE.md`\n'
                'and `AUDIT_STATE_CURRENT.md` (September 4/5 reset). Older runtime labels below\n'
                'are historical reference.',
                'Architecture reference originally written 2026-08-13. For current runtime,\n'
                'source lineages and vault records read `../FOREX_AUDIT_START_HERE.md`\n'
                'and `AUDIT_STATE_CURRENT.md`, using their latest dated review. Older runtime\n'
                'labels below are historical reference.')
            path.write_text(text,encoding='utf-8')
    with (ROOT/'FOREX_PROJECT_LOG.md').open('a',encoding='utf-8') as handle:
        handle.write(f'''\n\n## 2026-09-07 — operational status, news context and supplemental scoring\n\nRecorded {now}. Pair-specific neutral news context and forward-signal status
are visible; global feed counts appear once. Partial-operation status separates
all-pair research from the original EUR/USD/shared companion. A bounded
read-only audit verified six actual EUR/USD broker minute omissions, with no
recoverable prices or local read/parser fault. A separate engineering report
excludes every member of repeated-reference groups before calling the unchanged
EUR/USD scorer; original registered scorecards and evidence remain intact.
Final tests and actual runtime/UI checks are in
[the operational repair report](docs/FOREX_OPERATIONAL_STATUS_REPAIR_20260907.md).
The user-requested active `check-forex-bot-health` monitor runs every 15 minutes,
quiet while unchanged and notifying on meaningful changes or action needed.
No second automation, source collector revision, synthetic candle, registered
model edit, forecast backfill, or trading authorization was introduced.\n''')
    pending='''## September 7 — operational status verified; feed and model research remain

Pair-specific news context, global feed health, partial-operation status and
supplemental EUR/USD engineering scores are now distinguished. Original scorer
failure and registered evidence remain visible; all duplicate-reference members
are excluded only in the separate diagnostic. Predictive improvement, calibrated
probabilities and after-cost acceptance remain unproven. The six audited EUR/USD
gaps are broker omissions; sparse-minute models require a separate registered
research revision, not filled prices or changed current requirements.

Remaining feed work includes the observed GDELT rate limit, HKMA timeout, SCB
parse failure and missing/unsupported external access, subject to new verification.
The active 15-minute `check-forex-bot-health` monitor checks the actual bot and
notifies only for meaningful change or action needed. It keeps orders disabled.
See [the operational record](docs/FOREX_OPERATIONAL_STATUS_REPAIR_20260907.md).'''
    current=(ROOT/'FOREX_PENDING_IMPROVEMENTS.md').read_text(encoding='utf-8')
    first,rest=current.split('\n',1)
    (ROOT/'FOREX_PENDING_IMPROVEMENTS.md').write_text(first+'\n\n'+pending+'\n\n'+rest.lstrip('\n'),encoding='utf-8')
    sys.path.insert(0,str(ROOT))
    import forex_model_vault_sync as records
    from oanda_issue_register_validator import validate_register
    if len(records.CANONICAL_PROJECT_RECORDS)!=149: raise ValueError('canonical_mapping_count')
    register=validate_register(ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json',root=ROOT)
    if not register['valid']: raise ValueError('issue_register_invalid')
    sources=set(manifest['tested_source_sha256']) | set(NAV_FILES) | {
        REPORT,'forex_model_vault_sync.py','FOREX_PROJECT_LOG.md','FOREX_PENDING_IMPROVEMENTS.md',
        'FOREX_ISSUE_REGISTER_CURRENT.json','config/causal_forecast_study_current.json'}
    for name, registration in registrations.items():
        sources.add(name); sources.update(registration['source_bindings'])
    sources.update(p.relative_to(ROOT).as_posix() for p in evidence.rglob('*') if p.is_file())
    receipt={'schema_version':'forex_operational_status_repair_validation_v1_20260907',
        'observed_utc':now,'status':'operational_status_news_context_and_supplemental_diagnostics_verified',
        'tests':primary_tests,'primary_test_xml':EVIDENCE_REL+'/'+manifest['primary_test_xml']['path'],
        'final_runtime_ui_checks':[{**row,'path':EVIDENCE_REL+'/'+row['path']} for row in manifest['passing_json_checks']],
        'registered_studies_unchanged':registrations,'prior_receipts_unchanged':PRIOR,
        'prior_source_archive_preserved_by_publisher':{'archive':'forex_worktree_source_885144f85e85de600e4470d8.zip',
            'sha256':'dbdcd79d73a761c7b8f0e1d591d483878c05eb973b2668e0913e7e5e06e3881e'},
        'minute_gap_audit':EVIDENCE_REL+'/MINUTE_GAP_AUDIT.json',
        'feed_audit':EVIDENCE_REL+'/FEED_AUDIT.json',
        'observed_metrics':manifest['observed_metrics'],
        'monitoring':{'id':'check-forex-bot-health','interval_minutes':15,'status':'ACTIVE',
            'notify':'meaningful change, failure or action needed; quiet while unchanged',
            'future_runs_not_claimed':True},
        'canonical_record_count':149,'issue_register_validation':register,
        'research_only':True,'can_place_orders':False,'can_promote':False,
        'prediction_improvement_demonstrated':False,
        'limitations':['News context is not an eligible forward signal; feed transport errors are separate pending work.',
            'Missing broker minute prices remain absent; no synthetic candle or forecast backfill was performed.',
            'Supplemental outcome-blind reference-group exclusion does not repair the registered scorecard or establish prospective performance.',
            'Earlier failed and incomplete dashboard-consistency probes remain unchanged in their own dated record.',
            'Live/UI checks are bounded observations, not a promise of uninterrupted uptime.',
            'Latest-review metadata includes this receipt hash after creation and is separately covered by canonical export hash verification.'],
        'source_bindings':[{'path':name,'bytes':(ROOT/name).stat().st_size,'sha256':sha(ROOT/name)} for name in sorted(sources)]}
    write(ROOT/RECEIPT,receipt)
    for name in ('FOREX_AUDIT_STATE_CURRENT.json','FOREX_COMMONS_CURRENT.json'):
        path=ROOT/name;value=load(path)
        value['latest_review']={'observed_utc':now,'runtime_state':'research_collection_partial_operation_status_verified',
            'supersedes_prior_runtime_status':True,'report':REPORT,'validation':RECEIPT,'validation_sha256':sha(ROOT/RECEIPT),
            'managed_worker_count':14,'pair_registry':'pair_local_forecast_study_v1_20260907','registered_pairs':68,
            'primary_study':'eurusd_v1','cross_pair_study':'gap_v2','can_place_orders':False,'practice_trading_ready':False,
            'prediction_improvement_demonstrated':False,'supplemental_diagnostics_are_registered_performance':False,
            'monitoring':receipt['monitoring'],'all_other_embedded_snapshots_retain_their_original_timestamps':True,
            'prior_dashboard_consistency_validation':'FOREX_PAIR_DASHBOARD_CONSISTENCY_VALIDATION_20260907.json'}
        write(path,value)
    for name,digest in manifest['tested_source_sha256'].items():
        if sha(ROOT/name)!=digest: raise ValueError('tested_source_changed_during_finalization:'+name)
    for name,digest in PRIOR.items():
        if sha(ROOT/name)!=digest: raise ValueError('prior_receipt_changed_during_finalization:'+name)
    print(json.dumps({'receipt':str(ROOT/RECEIPT),'sha256':sha(ROOT/RECEIPT),
                      'source_bindings':len(sources),'tests':primary_tests,'canonical_record_count':149}))

if __name__=='__main__': main()
