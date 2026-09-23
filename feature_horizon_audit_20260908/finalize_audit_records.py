"""Bind the completed audit and correct current navigation; no runtime actions."""
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import ast
import json
import shutil

BASE = Path(__file__).resolve().parent
PROJECT = BASE.parent / 'trad'
EVIDENCE = PROJECT / 'docs/validation/existing_feature_horizon_20260908'
REPORT = PROJECT / 'docs/FOREX_EXISTING_FEATURE_HORIZON_AUDIT_20260908.md'
VALIDATION = PROJECT / 'FOREX_EXISTING_FEATURE_HORIZON_AUDIT_VALIDATION_20260908.json'

def digest(path):
    return sha256(path.read_bytes()).hexdigest()

def save(path, obj):
    with path.open('x', encoding='utf-8') as out:
        json.dump(obj, out, indent=2, allow_nan=False)
        out.write('\n')

def replace_once(path, old, new):
    text = path.read_text(encoding='utf-8-sig')
    assert text.count(old) == 1, (path, old)
    path.write_text(text.replace(old, new), encoding='utf-8')

registrations = []
all_sources = {}
contracts = 0

def walk(obj):
    global contracts
    if isinstance(obj, dict):
        if isinstance(obj.get('source_bindings'), dict):
            contracts += 1
            for name, expected in obj['source_bindings'].items():
                assert name not in all_sources or all_sources[name] == expected
                all_sources[name] = expected
        for value in obj.values():
            walk(value)
    elif isinstance(obj, list):
        for value in obj:
            walk(value)

for name in ['joint_price_news_study_v1_20260907.json',
             'joint_price_news_study_v2_20260907.json',
             'pair_local_forecast_study_v2_20260907.json']:
    path = PROJECT / 'config' / name
    value = json.loads(path.read_text(encoding='utf-8-sig'))
    walk(value)
    registrations.append({'path': str(path), 'sha256': digest(path)})
assert len(all_sources) >= 16
bindings = []
for name, expected in sorted(all_sources.items()):
    actual = digest(PROJECT / name)
    assert actual == expected, name
    bindings.append({'path': str(PROJECT / name), 'sha256': actual, 'matches_registration': True})

preserved_names = ['docs/FOREX_HORIZON_COVERAGE_REVIEW_20260907.md',
                   'FOREX_HORIZON_COVERAGE_REVIEW_VALIDATION_20260907.json',
                   'oanda_main_signal_dashboard.html', 'oanda_practice_live_dashboard.py']
preserved = [{'path': str(PROJECT / name), 'sha256': digest(PROJECT / name)} for name in preserved_names]
assert preserved[1]['sha256'] == 'ccb29bad61ec59bce3897c954ecbc3ce73f9c87b38a3068221aa461553abec0b'

names = [
    'CURVE_INVENTORY_CLOSURE_20260908.json',
    'CURVE_SOURCE_ARCHIVE_INVENTORY_20260908.json',
    'EXISTING_CURVE_MODEL_EVIDENCE_20260908.json',
    'EXISTING_CURVE_VALIDATION_REVIEW_RECEIPT_20260908.json',
    'EXISTING_TRAINED_HORIZON_MODELS_REVIEW_20260908.md',
    'FEATURE_IMPLEMENTATION_LINEAGE_AUDIT_20260908.json',
    'FEATURE_IMPLEMENTATION_LINEAGE_REVIEW_20260908.md',
    'LEGACY_FEATURE_SNAPSHOT_CAUSE_20260908.json',
    'MA_GRID_EXISTING_CURVE_AUDIT_20260908.json',
    'RECOVERY_MEMBERS_RUNTIME_20260908.json',
    'SOURCE_VAULT_RUNTIME_RECEIPT_20260908.json',
    'SOURCE_VAULT_RUNTIME_REVIEW_20260908.md',
    'audit_ma_grid.py', 'capture_feature_lineage.py', 'collect_existing_curve_evidence.py',
    'finalize_curve_inventory.py', 'inventory_curve_sources.py', 'verify_recovery_members_and_runtime.py',
]
EVIDENCE.mkdir(parents=True, exist_ok=False)
copied = []
for name in names:
    source, target = BASE / name, EVIDENCE / name
    assert source.is_file() and source.stat().st_size < 2 * 1024 * 1024
    shutil.copyfile(source, target)
    assert digest(source) == digest(target)
    copied.append({'source': str(source), 'copy': str(target), 'bytes': target.stat().st_size, 'sha256': digest(target)})
manifest = EVIDENCE / 'EVIDENCE_MANIFEST_20260908.json'
save(manifest, {'schema_version': 'existing_feature_horizon_evidence_v1',
                'recorded_utc': datetime.now(timezone.utc).isoformat(), 'files': copied,
                'scope': 'Exact audit reports, JSON and capture scripts. Underlying retained source copies, models and data remain at inventoried local paths; this is not engine recovery.'})

relative_report = 'docs/FOREX_EXISTING_FEATURE_HORIZON_AUDIT_20260908.md'
replace_once(PROJECT / 'README.md', 'The project is organized around',
    '**Existing model inventory corrected September 8 UTC:** the project already has 227/220-feature models, a 795-input eight-horizon curve, a 643-feature MA grid and second-ridge curves. The current 34-input H1 study is a separate narrow path. Read [the completed implementation and validation audit](' + relative_report + ') before proposing another curve. Some older source/models remain on D and in AppData; the vault source archive omits the ignored intrahour source package.\n\nThe project is organized around')
for filename, destination in [('docs/RESEARCH_INDEX.md', 'FOREX_EXISTING_FEATURE_HORIZON_AUDIT_20260908.md'),
                              ('docs/RESEARCH_INDEX_VAULT.md', 'EXISTING_FEATURE_HORIZON_AUDIT_CURRENT.md')]:
    replace_once(PROJECT / filename, '## Active work\n',
        '## Active work\n\n- [Existing 200-plus features and horizon engines: corrected full-project audit](' + destination + ')\n')
replace_once(PROJECT / 'FOREX_AUDIT_START_HERE.md', '## Current map\n',
    '## Current map\n\n- `EXISTING_FEATURE_HORIZON_AUDIT_CURRENT.md` and `EXISTING_FEATURE_HORIZON_AUDIT_VALIDATION_CURRENT.json`: completed audit of the existing 227/220/795/MA/second-ridge engines. The current H1 model is only one separate path. Older source/models remain at `D:/forex/trad` and `C:/Users/zmoor/AppData/Local/ForexResearchData/unified_intrahour_v1`. The ignored intrahour source package is absent from the current C checkout and inspected vault source archives; a valid archive hash does not establish a complete historical-engine backup.\n')
replace_once(PROJECT / 'docs/ACTIVE_PIPELINE.md', '## Research and history\n',
    '## Research and history\n\nThis page describes the running H1 path, not the entire implemented project. The older 227/220-feature models, 795-input eight-horizon curve, MA grid and second-ridge curves are real. See the September 8 existing-feature/horizon audit in [the research index](RESEARCH_INDEX.md) for retained results, missing C source/artifacts, D/AppData locations and the corrected recovery plan.\n')
replace_once(PROJECT / 'docs/SYSTEM_ORIENTATION_CURRENT.md', 'Start with [the active pipeline](ACTIVE_PIPELINE.md).',
    'The project already contains broad historical horizon engines beyond the current 34-input H1 study. The September 8 existing-feature/horizon audit in [the research index](RESEARCH_INDEX.md) identifies their source, fitted artifacts, validation defects and incomplete C/vault recovery.\n\nStart with [the active pipeline](ACTIVE_PIPELINE.md).')
pending = PROJECT / 'FOREX_PENDING_IMPROVEMENTS.md'
text = pending.read_text(encoding='utf-8-sig')
old = next(line for line in text.splitlines() if line.startswith('- Implement and evaluate the broader joint horizon curve requested'))
replacement = ('- Reconcile and reuse the existing horizon engines before building anything new: the archived 227/corrected 220 feature family, fitted 795-input eight-horizon model, MA grid and second-ridge curves already exist. Restore compatible source/artifact coverage from the documented C/D/AppData locations, repair the five duplicated 15-minute cross features and native-pip ranking in a new version, then join actual point-in-time news and evaluate technical-only/news-only/combined variants on matched time-blocked outcomes. Historical macro fields were constants. Keep per-horizon missingness and availability explicit. See `docs/FOREX_EXISTING_FEATURE_HORIZON_AUDIT_20260908.md`. The prior H1-only coverage review remains dated evidence, not a complete project inventory.')
replace_once(pending, old, replacement)
replace_once(pending, '## Current — combined price/news operation verified September 7',
    '## Current — existing engines audited September 8 UTC; combined operation last verified September 7')
log = PROJECT / 'FOREX_PROJECT_LOG.md'
with log.open('a', encoding='utf-8') as out:
    out.write('\n\n## 2026-09-08 UTC — existing feature and horizon audit; scope correction\n\n'
              'The earlier H1-focused review missed implemented broad engines. The completed audit verified the real 227/corrected 220 feature families, fitted 795-input eight-horizon curve, 643-field MA grid/610 fitted cells, second-ridge surfaces and direct currency panel. Retained C/D/AppData artifacts and reports are hash-bound.\n\n'
              'Confirmed findings: C lacks 36 intrahour source modules, omitted by the Git-based vault export because the whole directory is ignored; matching copies remain on D/checkpoints. Historical macro fields are constant across both technical datasets. Five nominal 15-minute cross features duplicate one-minute columns across 205,029 rows. The retained unified model has 49.416% mean direction accuracy and slightly worse error than no change. Original 227-field timing defects and stronger cost-survival than direction results remain explicit. Current D v5 source is not automatically compatible with the fitted v4 artifact.\n\n'
              'The pending plan now begins with recovery, compatibility, specific feature/cost repairs and measured news integration using existing engines. Guides and vault mappings point to the new audit; previous dated reports and receipts remain unchanged. No model training/loading/rescoring/restoration, runtime reload, order action, registration or ledger edit occurred. See `docs/FOREX_EXISTING_FEATURE_HORIZON_AUDIT_20260908.md` and `FOREX_EXISTING_FEATURE_HORIZON_AUDIT_VALIDATION_20260908.json`.\n')
mapping = PROJECT / 'forex_model_vault_sync.py'
replace_once(mapping, 'CANONICAL_PROJECT_RECORDS = (\n',
    'CANONICAL_PROJECT_RECORDS = (\n'
    '    (Path("trad/docs/FOREX_EXISTING_FEATURE_HORIZON_AUDIT_20260908.md"), "EXISTING_FEATURE_HORIZON_AUDIT_CURRENT.md"),\n'
    '    (Path("trad/FOREX_EXISTING_FEATURE_HORIZON_AUDIT_VALIDATION_20260908.json"), "EXISTING_FEATURE_HORIZON_AUDIT_VALIDATION_CURRENT.json"),\n')
ast.parse(mapping.read_text(encoding='utf-8-sig'))
for row in registrations + bindings + preserved:
    assert digest(Path(row['path'])) == row['sha256']
changed = ['README.md', 'docs/RESEARCH_INDEX.md', 'docs/RESEARCH_INDEX_VAULT.md',
           'FOREX_AUDIT_START_HERE.md', 'docs/ACTIVE_PIPELINE.md', 'docs/SYSTEM_ORIENTATION_CURRENT.md',
           'FOREX_PENDING_IMPROVEMENTS.md', 'FOREX_PROJECT_LOG.md', 'forex_model_vault_sync.py']
save(VALIDATION, {
    'schema_version': 'existing_feature_horizon_audit_validation_v1_20260908',
    'recorded_utc': datetime.now(timezone.utc).isoformat(), 'status': 'passed_audit_binding_checks',
    'report': {'path': str(REPORT), 'sha256': digest(REPORT)},
    'evidence_manifest': {'path': str(manifest), 'sha256': digest(manifest), 'file_count': len(copied)},
    'evidence_files': copied,
    'registered_sources_checked': bindings, 'registration_files_unchanged': registrations,
    'source_binding_dictionaries_checked': contracts,
    'earlier_report_receipt_and_dashboard_unchanged': preserved,
    'audit_navigation_and_mapping_changes': [{'path': str(PROJECT / name), 'sha256': digest(PROJECT / name)} for name in changed],
    'checks': ['Exact evidence-copy hashes', 'All discovered source bindings in joint v1/v2 and price v2 match current files',
               'Earlier horizon report/receipt and dashboard bytes preserved', 'Vault mapping Python syntax parsed',
               'Read-only original artifacts, metadata, source and runtime evidence captured by bounded sub-audits'],
    'runtime_actions': False, 'broker_actions': False, 'model_loads': False, 'training_runs': 0,
    'model_recovery': False, 'new_scores_computed': False, 'registry_or_ledger_writes': False,
    'limitations': ['Retained scores were audited, not reproduced by new training or rescoring.',
                    'Current runtime sources match registrations; this is not a new live outcome or profitability claim.',
                    'The evidence package and current C source export do not restore ignored D source, AppData models or large datasets.',
                    'Parquet population checks use metadata plus ten decoded columns, not a full feature recomputation.',
                    'Historical v4 duplicate features are observed directly; current v5 fallback as their cause is an inference.'],
})
print(json.dumps({'status': 'passed', 'report': str(REPORT), 'validation': str(VALIDATION),
                  'validation_sha256': digest(VALIDATION), 'evidence_files': len(copied),
                  'registered_sources': len(bindings), 'source_binding_dicts': contracts}))
