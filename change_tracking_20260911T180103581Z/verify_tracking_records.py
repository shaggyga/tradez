"""Verify new tracking documents, navigation history and prior reading-report seals."""
import datetime as dt
import hashlib
import json
import re
from pathlib import Path

out = Path(__file__).resolve().parent
project = Path('C:/Users/zmoor/Documents/forex/trad')
watch = project.parent / 'live_watch_20260910_2200'
documents = [project / 'docs/FOREX_CHANGE_REGISTER_20260911.md',
             project / 'docs/FOREX_MODEL_REUSE_REGISTER_20260911.md']
bindings = []
links = []
for path in documents:
    raw = path.read_bytes()
    body = raw.decode('utf-8')
    bindings.append({'path': str(path), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()})
    destinations = re.findall(r'\]\(([^)]+)\)', body)
    destinations += re.findall(r'^\[[^\]]+\]:\s*(\S+)\s*$', body, re.M)
    for dest in destinations:
        dest = dest.strip('<>')
        if dest.startswith(('https://', 'http://', '#')):
            continue
        dest = re.sub(r':\d+$', '', dest.split('#', 1)[0])
        target = Path(dest)
        if not target.is_absolute():
            target = path.parent / target
        if not target.is_file():
            raise SystemExit(f'Missing link in {path.name}: {dest}')
        links.append({'document': path.name, 'destination': dest})

change_text = documents[0].read_text(encoding='utf-8')
ids = re.findall(r'^\| (FXG-\d{3}) ', change_text, re.M)
if ids != [f'FXG-{n:03}' for n in range(1, 27)]:
    raise SystemExit('Change IDs are missing, duplicate, or reordered unexpectedly.')

nav = json.loads((out / 'NAVIGATION_UPDATE_20260911.json').read_text())
history_checks = []
for row in nav['changes']:
    current = Path(row['path']).read_bytes()
    if hashlib.sha256(current).hexdigest() != row['after_sha256']:
        raise SystemExit(f"Navigation file changed after update: {row['path']}")
    previous = (out / 'before' / Path(row['path']).name).read_bytes()
    if hashlib.sha256(previous).hexdigest() != row['before_sha256']:
        raise SystemExit('Before-state hash mismatch.')
    if Path(row['path']).name == 'FOREX_PROJECT_LOG.md':
        retained = current.startswith(previous)
    else:
        end = previous.index(b'\n') + 1
        retained = current.startswith(previous[:end]) and current.endswith(previous[end:])
    if not retained:
        raise SystemExit(f"Original documentation history changed: {row['path']}")
    history_checks.append({'path': row['path'], 'original_bytes_preserved': True})

seals = []
for name, expected in [
    ('EXISTING_PICTURE_INDEX_20260911.json', '00082cfcce894ff971d9ea762db6df8c91bd51fcc809fd95631a7b2e1e657b3c'),
    ('LIVE_WATCH_120_MINUTES_EVIDENCE_INDEX_20260911.json', '99f7c12d6adba33860caa8cafad8d48d8d033fa3cf79c911de519910e7575667'),
]:
    raw = (watch / name).read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected:
        raise SystemExit(f'Prior reading index changed: {name}')
    manifest = json.loads(raw)
    for row in manifest['records']:
        relative = row.get('path', row.get('member'))
        path = watch / relative
        payload = path.read_bytes()
        if len(payload) != row['bytes'] or hashlib.sha256(payload).hexdigest() != row['sha256']:
            raise SystemExit(f'Prior sealed reading report changed: {relative}')
    seals.append({'index': name, 'sha256': expected, 'reading_records_verified': len(manifest['records'])})

result = {
    'schema': 'forex_change_and_research_reuse_tracking_validation_v1_20260911',
    'recorded_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
    'status': 'verified', 'change_ids': ids, 'documents': bindings,
    'file_links_checked': len(links), 'links': links,
    'navigation_history_checks': history_checks, 'prior_seals': seals,
    'scope': 'Documentation/links/history and selected prior reading-report integrity only; no fit, model rescore, archive rehash, live health check, broker or runtime action.',
}
dest = out / 'TRACKING_RECORDS_VERIFIED_20260911.json'
dest.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
print(json.dumps({'validation': str(dest), 'changes': len(ids), 'file_links_checked': len(links),
                  'prior_reading_records_verified': sum(x['reading_records_verified'] for x in seals),
                  'documents': bindings, 'sha256': hashlib.sha256(dest.read_bytes()).hexdigest()}))
