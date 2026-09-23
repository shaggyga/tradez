"""Create a new inactive local restoration tree; never modify source evidence."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import shutil
import sqlite3
import sys
import zipfile

ROOT = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def copy_file(source, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    assert digest(source) == digest(target)


def copy_tree(source, target):
    for path in sorted(source.rglob('*')):
        if path.is_file() and not any(part in {'__pycache__', '.pytest_cache'} for part in path.parts):
            copy_file(path, target/path.relative_to(source))


def build(output):
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    study = output/ROOT.name
    for name in ('src', 'tests', 'evaluation_001', 'review', 'reuse_review'):
        copy_tree(ROOT/name, study/name)
    for name in ('STUDY_SPEC.json', 'verify_saved_cost_study.py', 'DIRECTION_DECISION_REPORT.md', 'build_local_restoration.py', 'publish_research_records.py'):
        copy_file(ROOT/name, study/name)
    for path in sorted((ROOT/'event_bridge').iterdir()):
        if path.is_file() and path.suffix in {'.py', '.md', '.json', '.xml'}:
            copy_file(path, study/'event_bridge'/path.name)
    # A SQLite backup includes committed WAL content coherently without changing source.
    source = ROOT/'event_bridge/fresh_captures.sqlite'
    target = study/'event_bridge/fresh_captures.sqlite'
    with sqlite3.connect(source.resolve().as_uri()+'?mode=ro', uri=True) as original:
        original.execute('PRAGMA query_only=ON')
        with sqlite3.connect(target) as destination:
            original.backup(destination)
            assert destination.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
            destination.execute('PRAGMA journal_mode=DELETE')
            capture_ids = destination.execute('SELECT capture_id,raw_sha256,feature_sha256 FROM captures ORDER BY capture_id').fetchall()
            assert capture_ids == original.execute('SELECT capture_id,raw_sha256,feature_sha256 FROM captures ORDER BY capture_id').fetchall()
            assert destination.execute('SELECT COUNT(*) FROM publication_receipts').fetchone()[0] == len(capture_ids)
    predecessor = ROOT.parent/'direction_research_20260911'
    for name in ('snapshot_001', 'prepared_001'):
        copy_tree(predecessor/name, output/predecessor.name/name)
    # Full source release supports both loader parity and reconstruction of the prior panel.
    copy_tree(predecessor/'source_release', output/predecessor.name/'source_release')
    copy_file(predecessor/'evaluation_001/RESULTS.json', output/predecessor.name/'evaluation_001/RESULTS.json')
    copy_file(predecessor/'COMPLETION_RECEIPT.json', output/predecessor.name/'COMPLETION_RECEIPT.json')
    for name in ('oanda_continuous_narrative_meter.py', 'config/news_sources_v1.json'):
        copy_file(ROOT.parent/'trad'/name, output/'trad'/name)
    packages = {name:importlib.metadata.version(name) for name in ('numpy','pandas','scipy','scikit-learn','joblib','pyarrow','threadpoolctl','pytest')}
    environment = {'python':sys.version,'platform':platform.platform(),'packages':packages}
    (output/'ENVIRONMENT.json').write_text(json.dumps(environment,indent=2)+'\n',encoding='utf-8')
    (output/'requirements.txt').write_text(''.join(f'{name}=={version}\n' for name,version in packages.items()),encoding='utf-8')
    (output/'README.md').write_text('''# Local signed-cost research restoration

This inactive package contains code, tests, fitted artifacts, evaluation inputs, the exact predecessor snapshot/panel, and a coherent backup of two news captures. It contains local article versions; keep it in the private project workspace. No credentials, account database or broker services are included.

Use Python matching ENVIRONMENT.json and requirements.txt. The known compatible runtime was Python 3.12 on Windows. Joblib artifacts must be loaded only from this trusted, hash-verified local package.

From direction_decision_20260911:

```powershell
python -m pytest tests event_bridge -q -p no:cacheprovider
python verify_saved_cost_study.py --evaluation evaluation_001 --output new_recreation/RESULT.json
python event_bridge/replay_fresh_captures.py --database event_bridge/fresh_captures.sqlite --project ../trad --output RESTORED_METER_CHECK.json
python src/run_signed_cost_study_v1.py --predecessor ../direction_research_20260911 --output NEW_REFIT --spec STUDY_SPEC.json
```

The first two checks and offline meter replay are executed on the extracted package as recorded outside the ZIP. Refit inputs are retained and reconstructed; a second full fit is not claimed. New proof names and refit output directories must not replace old evidence.

The inherited event_bridge/inspect_inventory.py and verify_retained_bridge.py are historical local audit recipes, not portable checks: they additionally require original canonical databases. Continuous collection is inactive; COLLECTION_PROFILE.json records its original local source paths, limits and prospective status. Do not interpret the September 12 captures as current signals.

MANIFEST.json binds every packaged file except itself. Extra test caches or new verification outputs after extraction are not original package files. Source/output identity proves recreation, not causality beyond the documented observation conventions or profitability.
''',encoding='utf-8')
    manifest = {'created_utc':datetime.now(timezone.utc).isoformat(),'scope':'local_inactive_research_restoration','can_place_orders':False,
                'sqlite_backup':{'original_path':str(source),'packaged_path':target.relative_to(output).as_posix(),'captures':capture_ids,'integrity_check':'ok'},
                'files':{path.relative_to(output).as_posix():{'sha256':digest(path),'bytes':path.stat().st_size} for path in sorted(output.rglob('*')) if path.is_file()}}
    (output/'MANIFEST.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    archive = output.with_suffix('.zip')
    with zipfile.ZipFile(archive,'x',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as z:
        for path in sorted(output.rglob('*')):
            if path.is_file():z.write(path,path.relative_to(output).as_posix())
    receipt = {'directory':str(output),'archive':str(archive),'archive_sha256':digest(archive),'archive_bytes':archive.stat().st_size,
               'manifest_sha256':digest(output/'MANIFEST.json'),'files':len(manifest['files']),'sqlite_captures':len(capture_ids)}
    print(json.dumps(receipt,indent=2))
    return receipt


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--receipt',type=Path,required=True)
    args=parser.parse_args()
    if args.receipt.exists():raise FileExistsError('build_receipt_exists')
    receipt=build(args.output)
    with args.receipt.open('x',encoding='utf-8') as out:json.dump(receipt,out,indent=2,sort_keys=True)
