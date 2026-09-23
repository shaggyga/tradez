"""Package baseline evidence and update existing project navigation/backlog."""
from datetime import datetime, timezone
from pathlib import Path
from hashlib import sha256
import ast
import json
import shutil
import sys

sys.dont_write_bytecode = True
BASE = Path(__file__).resolve().parent
PROJECT = BASE.parent / 'trad'
RECOVERY = Path('D:/ForexRecovery/revamp_20260908T1353Z')
EVIDENCE = PROJECT / 'docs/validation/revamp_baseline_20260908'
REPORT = PROJECT / 'docs/FOREX_REVAMP_BASELINE_RECOVERY_20260908.md'
VALIDATION = PROJECT / 'FOREX_REVAMP_BASELINE_RECOVERY_VALIDATION_20260908.json'

def digest(path):
    h = sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()

def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8') as out:
        json.dump(value, out, indent=2, allow_nan=False)
        out.write('\n')

def replace_once(relative, old, new):
    path = PROJECT / relative
    value = path.read_text(encoding='utf-8-sig')
    assert value.count(old) == 1, (relative, old)
    path.write_text(value.replace(old, new), encoding='utf-8')

required = ['runtime/RUNTIME_BASELINE_REVIEW_20260908.md',
            'runtime/RUNTIME_BASELINE_RECEIPT_20260908.json',
            'performance/PRESERVED_PREDICTION_PERFORMANCE_BASELINE_20260908.md',
            'performance/PRESERVED_PREDICTION_BASELINE_COMPACT_20260908.json',
            'performance/SELECTED_LEDGER_BACKUP_MANIFEST_20260908.json',
            'reconciliation/SECOND_RIDGE_ENGINEERING_SMOKE_20260908.md',
            'reconciliation/SECOND_RIDGE_ENGINEERING_CLOCK_CHECK_20260908.json',
            'SOURCE_ARTIFACT_RECOVERY_20260908.json']
assert all((BASE / name).is_file() for name in required)
source_receipt = json.loads((BASE / 'SOURCE_ARTIFACT_RECOVERY_20260908.json').read_text(encoding='utf-8'))
source_manifest = json.loads(next((RECOVERY / 'source/current').glob('*.manifest.json')).read_text(encoding='utf-8'))
assert source_receipt['status'] == 'passed' and source_receipt['source_files'] == 2202
before = {}
for row in source_manifest['files']:
    path = PROJECT / row['path']
    assert digest(path) == row['sha256'], row['path']
    before[row['path']] = row['sha256']

selected = ['SOURCE_ARTIFACT_RECOVERY_20260908.json', 'ISOLATED_WORKSPACE.json', 'RECOVERY_README.md', 'prepare_recovery.py']
for subdir in ['runtime', 'reconciliation']:
    selected.extend(str(path.relative_to(BASE)).replace('\\', '/')
                    for path in sorted((BASE / subdir).iterdir())
                    if path.is_file() and path.suffix in {'.md', '.json', '.py'})
selected.extend('performance/' + name for name in [
    'PRESERVED_PREDICTION_PERFORMANCE_BASELINE_20260908.md',
    'PRESERVED_PREDICTION_BASELINE_COMPACT_20260908.json',
    'CURRENT_PREDICTION_BASELINE_20260908.json',
    'SELECTED_LEDGER_BACKUP_MANIFEST_20260908.json',
    'backup_selected_ledgers.py', 'evaluate_frozen_baseline.py'])
assert len(selected) == len(set(selected))
sys.path.insert(0, str(PROJECT))
from tools.vault_worktree_snapshot import audit_payload
for name in selected:
    path = BASE / name
    assert path.is_file() and path.stat().st_size < 2 * 1024 * 1024
    audit_payload('docs/validation/revamp_baseline_20260908/' + name, path.read_bytes(), set())
EVIDENCE.mkdir(parents=True, exist_ok=False)
copied = []
for name in selected:
    source, target = BASE / name, EVIDENCE / name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    assert digest(source) == digest(target)
    copied.append({'source': str(source), 'copy': str(target), 'bytes': target.stat().st_size, 'sha256': digest(target)})
evidence_manifest = EVIDENCE / 'EVIDENCE_MANIFEST_20260908.json'
save(evidence_manifest, {'schema': 'forex_revamp_baseline_evidence_v1', 'files': copied,
                         'scope': 'Exact compact baseline reports, observations, manifests and capture helpers. Original databases, bulk input matrices and fitted artifacts remain in the D recovery checkpoint.'})

replace_once('README.md', 'The project is organized around',
    '**Revamp baseline completed September 8:** [baseline and recovery](docs/FOREX_REVAMP_BASELINE_RECOVERY_20260908.md) records an isolated source copy, 272 coherent study backups and recovered second-ridge inference. The fresh observation found a news topic-identity collision blocking combined forecasts and an intermittent price-display generation mismatch. Completed forecasts still lost after spread; these are dated findings, not current telemetry.\n\nThe project is organized around')
replace_once('FOREX_PENDING_IMPROVEMENTS.md', 'Remaining acceptance:\n',
    'Remaining acceptance:\n\n- **First remediation: current news topic identity.** The September 8 13:58–14:06 UTC baseline reproduced two individually valid context records with one topic/story ID but different syndication groups and clocks; together they make the guarded input unavailable. Last joint-v2 publication was 10:33:00 UTC. Repair upstream identity/group reconciliation in a new version using the retained case; preserve original member clocks, deduplication and the guard.\n- **First-phase recovery completed.** An isolated 2,202-file source baseline, 63 static source/artifact backups, 272 coherent study DB snapshots and a real-input second-ridge 13-horizon inference check are verified. This closes preparation and present inference compatibility, not deployment, broad-model recovery or predictive improvement. See `docs/FOREX_REVAMP_BASELINE_RECOVERY_20260908.md`.\n')
replace_once('FOREX_PENDING_IMPROVEMENTS.md',
    '## Current — existing engines audited September 8 UTC; combined operation last verified September 7',
    '## Current — September 8 baseline and recovery complete; operational repairs and predictive acceptance remain')
replace_once('FOREX_PENDING_IMPROVEMENTS.md',
    '- Investigate the intermittent joint summary/heartbeat generation mismatch retained at 23:31 and 23:35 UTC. Later producer pairs were coherent; the failure phase was not captured. Preserve binding checks and distinguish the five-second API cache from actual producer state before selecting a repair.',
    '- Resolve intermittent summary/heartbeat generation mismatch. September 7 joint failures remain retained; the September 8 baseline also captured price-v2 API withholding all pairs, then recovery to 68 while the worker continued. The exact conflicting bytes at the failure were not captured. Preserve binding checks and distinguish API cache state from producer state before selecting a repair.')
for filename, name in [('docs/RESEARCH_INDEX.md', 'FOREX_REVAMP_BASELINE_RECOVERY_20260908.md'),
                       ('docs/RESEARCH_INDEX_VAULT.md', 'REVAMP_BASELINE_RECOVERY_CURRENT.md')]:
    replace_once(filename, '## Active work\n',
        '## Active work\n\n- [September 8 revamp baseline, recovery checkpoint and first comparator](%s)\n' % name)
replace_once('FOREX_AUDIT_START_HERE.md', '## Current map\n',
    '## Current map\n\n- `REVAMP_BASELINE_RECOVERY_CURRENT.md` and `REVAMP_BASELINE_RECOVERY_VALIDATION_CURRENT.json`: September 8 source/selected-database recovery, isolated second-ridge inference, fresh runtime blocker reproduction and original-outcome performance baseline. The D recovery checkpoint is local and scoped; it is not a complete vault database backup or live deployment.\n')
with (PROJECT / 'FOREX_PROJECT_LOG.md').open('a', encoding='utf-8') as out:
    out.write('\n\n## 2026-09-08 — first revamp phase: baseline and recovery completed\n\n'
              'Reconciled the clarified vault/backlog and freshly verified source, dictionary and model bindings. Preserved the current 2,202-file source snapshot in an isolated copy, 36 D intrahour modules separately from v4 artifacts, actual fitted models and the full unified matrix.63 static backup files total 597,968,093 bytes. Online backups preserve 272 selected study databases (6,319,161,344 bytes), each individually coherent and integrity-checked. No live worker, registry, trading setting, model implementation or original ledger was edited.\n\n'
              'The source/artifact recovery check reproduced second-ridge 13 horizons twice with an independent coefficient oracle and retained UTC input verification. It is engineering replay on pre-fit historical data, not a forecast-success claim. MA and unified v4/v5 compatibility remain open.\n\n'
              'Three retained runtime observations 13:58:33–14:06:48 UTC found 15 expected workers alive but no current combined forecasts. A two-topic case exactly reproduces conflicting_current_topic_identity; last successful joint publication was 10:33 UTC. Price-v2 retained an intervening generation-mismatch failure and later restored 68-pair visibility while its worker progressed. These are the first remediation items.\n\n'
              'Frozen scoring reconciled 12,034 original completed model records. Joint v2 has 2,779 outcomes, 52.07% direction, 30.66% positive after spread and -2.9693 mean net bps; all four main model groups underperform their zero-move magnitude and fixed 50% probability baselines. The +0.2009 bps joint-v2 difference versus matched price-only remains a dependent sample diagnostic. Previous records remain dated evidence. See docs/FOREX_REVAMP_BASELINE_RECOVERY_20260908.md and its source-bound validation receipt.\n')
replace_once('forex_model_vault_sync.py', 'CANONICAL_PROJECT_RECORDS = (\n',
    'CANONICAL_PROJECT_RECORDS = (\n'
    '    (Path("trad/docs/FOREX_REVAMP_BASELINE_RECOVERY_20260908.md"), "REVAMP_BASELINE_RECOVERY_CURRENT.md"),\n'
    '    (Path("trad/FOREX_REVAMP_BASELINE_RECOVERY_VALIDATION_20260908.json"), "REVAMP_BASELINE_RECOVERY_VALIDATION_CURRENT.json"),\n')
ast.parse((PROJECT / 'forex_model_vault_sync.py').read_text(encoding='utf-8-sig'))
intentional = {'README.md', 'FOREX_PENDING_IMPROVEMENTS.md', 'FOREX_PROJECT_LOG.md',
               'FOREX_AUDIT_START_HERE.md', 'docs/RESEARCH_INDEX.md', 'docs/RESEARCH_INDEX_VAULT.md',
               'forex_model_vault_sync.py'}
unchanged = []
for name, previous_hash in before.items():
    observed = digest(PROJECT / name)
    if name not in intentional:
        assert observed == previous_hash, name
        unchanged.append({'path': name, 'sha256': observed})
unchanged_manifest = EVIDENCE / 'UNCHANGED_BASELINE_SOURCE_MEMBERS_20260908.json'
save(unchanged_manifest, {'source_snapshot_id': source_receipt['source_snapshot_id'],
                         'files': unchanged, 'intentional_documentation_and_mapping_changes': sorted(intentional)})

validation = {
    'schema': 'forex_revamp_baseline_recovery_validation_v1', 'status': 'passed_first_phase',
    'recorded_utc': datetime.now(timezone.utc).isoformat(),
    'report': {'path': str(REPORT), 'sha256': digest(REPORT)},
    'evidence_manifest': {'path': str(evidence_manifest), 'sha256': digest(evidence_manifest), 'file_count': len(copied)},
    'unchanged_source_manifest': {'path': str(unchanged_manifest), 'sha256': digest(unchanged_manifest), 'files': len(unchanged)},
    'source_recovery': {'path': str(BASE / 'SOURCE_ARTIFACT_RECOVERY_20260908.json'), 'sha256': digest(BASE / 'SOURCE_ARTIFACT_RECOVERY_20260908.json')},
    'database_recovery': {'path': str(BASE / 'performance/SELECTED_LEDGER_BACKUP_MANIFEST_20260908.json'), 'sha256': digest(BASE / 'performance/SELECTED_LEDGER_BACKUP_MANIFEST_20260908.json'), 'databases': 272, 'bytes': 6319161344},
    'performance': {'path': str(BASE / 'performance/PRESERVED_PREDICTION_BASELINE_COMPACT_20260908.json'), 'sha256': digest(BASE / 'performance/PRESERVED_PREDICTION_BASELINE_COMPACT_20260908.json')},
    'inference_smoke': {'path': str(BASE / 'reconciliation/SECOND_RIDGE_ENGINEERING_SMOKE_20260908.json'), 'sha256': digest(BASE / 'reconciliation/SECOND_RIDGE_ENGINEERING_SMOKE_20260908.json')},
    'diagnostic_mode': 'offline engineering replay; historical input predates fit; no efficacy claim',
    'runtime_restart_or_broker_actions': False, 'original_models_or_registrations_changed': False,
    'original_ledger_writes': False, 'original_completed_outcomes_rescored_on_copies_only': True,
    'live_implementation_edits': False,
    'intentional_navigation_mapping_changes': [{'path': str(PROJECT / name), 'sha256': digest(PROJECT / name)} for name in sorted(intentional)],
    'open_findings': ['news producer topic identity collision blocks combined forecasts',
                      'intermittent price-v2 API generation mismatch; recovered observation retained',
                      'after-spread loss and baseline underperformance',
                      'MA opaque-artifact/dependency inference and unified v4/v5 reconstruction',
                      'new combined event/currency feature gains require later matched evidence'],
}
save(VALIDATION, validation)

# Preserve the human restore guide and baseline receipts alongside the D assets.
recovery_docs = RECOVERY / 'baseline_evidence'
recovery_docs.mkdir(parents=True, exist_ok=False)
recovery_doc_copies = []
for source in [BASE / 'RECOVERY_README.md', REPORT, VALIDATION,
               BASE / 'runtime/RUNTIME_BASELINE_REVIEW_20260908.md',
               BASE / 'runtime/NEWS_TOPIC_IDENTITY_CONFLICT_CASE_20260908.json',
               BASE / 'performance/SELECTED_LEDGER_BACKUP_MANIFEST_20260908.json',
               BASE / 'performance/PRESERVED_PREDICTION_BASELINE_COMPACT_20260908.json',
               BASE / 'performance/PRESERVED_PREDICTION_PERFORMANCE_BASELINE_20260908.md',
               BASE / 'reconciliation/SECOND_RIDGE_ENGINEERING_SMOKE_20260908.json']:
    target = recovery_docs / source.name
    shutil.copyfile(source, target)
    assert digest(source) == digest(target)
    recovery_doc_copies.append({'source': str(source), 'copy': str(target), 'sha256': digest(target)})
save(BASE / 'RECOVERY_EVIDENCE_COPIES_20260908.json', {'status': 'passed', 'files': recovery_doc_copies})
print(json.dumps({'status': 'passed', 'evidence_files': len(copied), 'baseline_source_members_unchanged': len(unchanged),
                  'validation': str(VALIDATION), 'validation_sha256': digest(VALIDATION)}), flush=True)
