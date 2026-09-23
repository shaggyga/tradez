"""Publish a bounded audit addendum; preserve models, databases and prior audits."""
from pathlib import Path
from hashlib import sha256
from datetime import datetime, timezone
import json
import shutil
import subprocess
import sys

BASE = Path(__file__).resolve().parent
ROOT = BASE.parent
PROJECT = ROOT / 'trad'
VAULT = Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
EVIDENCE = PROJECT / 'docs/validation/blurb_dataset_20260908'
REPORT = PROJECT / 'docs/FOREX_BLURB_DATASET_AUDIT_20260908.md'
VALIDATION = PROJECT / 'FOREX_BLURB_DATASET_AUDIT_VALIDATION_20260908.json'

def digest(path):
    return sha256(path.read_bytes()).hexdigest()

def save(path, value):
    with path.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write('\n')

def replace_once(relative, old, new):
    path = PROJECT / relative
    text = path.read_text(encoding='utf-8-sig')
    assert text.count(old) == 1, (relative, old)
    path.write_text(text.replace(old, new), encoding='utf-8')

required = ['BLURB_DATASET_INVENTORY_20260908.json', 'BLURB_DATASET_REVIEW_20260908.md',
            'BLURB_MODEL_USE_REVIEW_20260908.md']
assert all((BASE / name).is_file() for name in required)
previous = PROJECT / 'FOREX_EXISTING_FEATURE_HORIZON_AUDIT_VALIDATION_20260908.json'
previous_value = json.loads(previous.read_text(encoding='utf-8'))
previous_hash = digest(previous)
assert previous_hash == '1821b2b1adc6b837bccc4e13e32287e62d92d28220acb4b729951183448399b6'
preserved = [previous_value['report'], *previous_value['registered_sources_checked'],
             *previous_value['registration_files_unchanged']]
for row in preserved:
    assert digest(Path(row['path'])) == row['sha256']

EVIDENCE.mkdir(parents=True, exist_ok=False)
copies = []
for source in sorted(BASE.iterdir()):
    if source.name == Path(__file__).name or source.suffix not in {'.json', '.md', '.py'}:
        continue
    assert source.stat().st_size < 2 * 1024 * 1024
    target = EVIDENCE / source.name
    shutil.copyfile(source, target)
    assert digest(source) == digest(target)
    copies.append({'source': str(source), 'copy': str(target), 'sha256': digest(target), 'bytes': target.stat().st_size})

replace_once('README.md', 'The project is organized around',
    'The [blurb dataset audit addendum](docs/FOREX_BLURB_DATASET_AUDIT_20260908.md) adds the retained 7,048-row movement/news dataset, 2,935-factor reconstruction and earlier news/technical comparisons. These are separate from the constant macro columns found in specific older model matrices.\n\nThe project is organized around')
for path, report in [('docs/RESEARCH_INDEX.md', 'FOREX_BLURB_DATASET_AUDIT_20260908.md'),
                     ('docs/RESEARCH_INDEX_VAULT.md', 'BLURB_DATASET_AUDIT_CURRENT.md')]:
    replace_once(path, '## Active work\n',
        '## Active work\n\n- [Retained blurb dataset and historical news/technical model-use audit](' + report + ')\n')
replace_once('FOREX_AUDIT_START_HERE.md', '## Current map\n',
    '## Current map\n\n- `BLURB_DATASET_AUDIT_CURRENT.md` and `BLURB_DATASET_AUDIT_VALIDATION_CURRENT.json`: actual retained movement/news CSV and factor databases, recovered source bundle, historical joint comparisons and inactive analog-input trace. Constant macro columns in one model family do not mean the project lacked blurb data.\n')
replace_once('FOREX_PENDING_IMPROVEMENTS.md', 'Remaining acceptance:\n',
    'Remaining acceptance:\n\n- Include the existing blurb/factor datasets in the recovery and integration plan: 7,048 labelled moves, 2,935 factors and separate historical news/technical comparisons already exist. The registered recovered-blurb analog arm emits zero because it has no qualifying prequential orientation predictions. Audit usable entry-time news and event-response features separately from retrospective explanations; do not recollect or claim this work never existed. See `docs/FOREX_BLURB_DATASET_AUDIT_20260908.md`.\n')
with (PROJECT / 'FOREX_PROJECT_LOG.md').open('a', encoding='utf-8') as stream:
    stream.write('\n\n## 2026-09-08 UTC — retained blurb dataset audit addendum\n\n'
                 'Verified the actual 7,048-row/68-pair legacy CSV (1,095 retained news matches), the 405,794,816-byte spike/blurb reconstruction database with 2,935 factors and 6,465 distinct market episodes, and the separate movement/news database. The recovered ZIP contains source/example material; its missing bulk corpus does not imply the separate datasets are absent.\n\n'
                 'The historical analog tables contain no scored prequential forecasts; recovered_blurb_analog_v1 is registered inactive and emits zero. Separate news-plus-technical backtests did exist. The earlier constant-macro finding applies to specific fitted matrices, not the whole project. Guides and pending work now include this lineage. Earlier audit reports, model sources, registrations, databases and runtime remain unchanged. See `docs/FOREX_BLURB_DATASET_AUDIT_20260908.md`.\n')
replace_once('forex_model_vault_sync.py', 'CANONICAL_PROJECT_RECORDS = (\n',
    'CANONICAL_PROJECT_RECORDS = (\n'
    '    (Path("trad/docs/FOREX_BLURB_DATASET_AUDIT_20260908.md"), "BLURB_DATASET_AUDIT_CURRENT.md"),\n'
    '    (Path("trad/FOREX_BLURB_DATASET_AUDIT_VALIDATION_20260908.json"), "BLURB_DATASET_AUDIT_VALIDATION_CURRENT.json"),\n')
import ast
ast.parse((PROJECT / 'forex_model_vault_sync.py').read_text(encoding='utf-8-sig'))
for row in preserved:
    assert digest(Path(row['path'])) == row['sha256']
assert digest(previous) == previous_hash
save(VALIDATION, {
    'schema': 'blurb_dataset_audit_validation_v1_20260908',
    'recorded_utc': datetime.now(timezone.utc).isoformat(), 'status': 'passed_audit_binding_checks',
    'report': {'path': str(REPORT), 'sha256': digest(REPORT)}, 'evidence': copies,
    'preceding_audit_validation_unchanged': {'path': str(previous), 'sha256': previous_hash},
    'preceding_report_and_registered_sources_unchanged': preserved,
    'model_runtime_database_broker_actions': False,
    'new_training_or_rescoring': False,
    'scope': 'Read-only dataset and model-use audit; added documentation, navigation and canonical vault mappings. Full datasets remain local.',
})
result = subprocess.run([sys.executable, str(PROJECT / 'forex_model_vault_sync.py'),
                         '--root', str(ROOT), '--destination', str(VAULT), '--canonical-only'],
                        cwd=PROJECT, capture_output=True, text=True, encoding='utf-8',
                        creationflags=subprocess.CREATE_NO_WINDOW)
save(BASE / 'CANONICAL_EXPORT_RESULT_20260908.json',
     {'returncode': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr})
assert result.returncode == 0
export = json.loads(result.stdout)
assert export['file_count'] == 164
verified = []
for destination in export['destinations']:
    for row in destination['canonical_project_records']['records']:
        assert digest(ROOT / row['source']) == digest(VAULT / row['name']) == row['sha256']
        verified.append({'source': row['source'], 'destination': row['name'], 'sha256': row['sha256']})
assert len(verified) == 164
receipt = BASE / 'BLURB_ADDENDUM_VAULT_VERIFICATION_20260908.json'
save(receipt, {'status': 'passed', 'recorded_utc': datetime.now(timezone.utc).isoformat(),
               'canonical_records_verified': verified, 'record_count': len(verified),
               'validation_sha256': digest(VALIDATION),
               'source_archive_updated': False,
               'source_archive_scope': 'The earlier verified source snapshot remains a dated pre-addendum archive. This publication updates canonical audit records only; no model/database recovery is claimed.'})
print(json.dumps({'status': 'passed', 'report': str(REPORT), 'validation': str(VALIDATION),
                  'evidence_files': len(copies), 'canonical_records': len(verified),
                  'vault_receipt': str(receipt), 'vault_receipt_sha256': digest(receipt)}), flush=True)
