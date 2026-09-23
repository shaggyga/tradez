"""Read-only validation of the design documentation and navigation update."""
from pathlib import Path
import hashlib
import json
import re
from urllib.parse import unquote

work = Path(r'C:\Users\zmoor\Documents\forex')
vault = Path(r'C:\Users\zmoor\OneDrive\thevault\projects\forex')
package = vault / 'DESIGN_ALIGNMENT_20260921'
local = work / 'design_alignment_20260921'

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def read(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))

def require(condition, message):
    if not condition:
        raise RuntimeError(message)

pointer = read(vault / 'DESIGN_ALIGNMENT_LATEST.json')
manifest = read(package / 'MANIFEST.json')
require(sha(package / 'MANIFEST.json') == pointer['manifest_sha256'], 'Manifest pointer mismatch')
require(sha(package / 'PACKAGE_READBACK.json') == pointer['readback_sha256'], 'Readback pointer mismatch')
for item in manifest['files']:
    rel = Path(item['path'])
    require(not rel.is_absolute() and '..' not in rel.parts, 'Unsafe manifest path')
    for root in [package, local]:
        p = root / rel
        require(p.is_file() and p.stat().st_size == item['bytes'] and sha(p) == item['sha256'], f'Payload mismatch: {p}')

inventory = read(package / 'previous_documents/INVENTORY.json')
for item in inventory:
    require(sha(package / item['archived_relative_path']) == item['sha256'], f'Preimage mismatch: {item["source"]}')
changes = read(package / 'DOCUMENT_CHANGES.json')
for item in changes:
    require(sha(Path(item['path'])) == item['after_sha256'], f'Updated document changed: {item["path"]}')

coverage = (package / 'DESIGN_COVERAGE.md').read_text(encoding='utf-8')
require(set(re.findall(r'^\| (R\d{2}) \|', coverage, re.M)) == {f'R{i:02}' for i in range(1,16)}, 'Requirement rows incomplete')
require(set(re.findall(r'^\| (WP\d+) ', coverage, re.M)) == {f'WP{i}' for i in range(12)}, 'Work-package rows incomplete')
require(set(re.findall(r'^\| (TST\d{2}) \|', coverage, re.M)) == {f'TST{i:02}' for i in range(1,57)}, 'Acceptance rows incomplete')
path_text = (package / 'PATH_FORWARD.md').read_text(encoding='utf-8')
pending = path_text.split('## Disposition of all 37 historical pending items')[1].split('## Acceptance rule')[0]
ids = set()
for line in pending.splitlines():
    if line.startswith('| '):
        first_column = line.split('|')[1]
        ids.update(int(x) for x in re.findall(r'(?<!\w)(\d{1,2})(?=\s)', first_column))
require(ids == set(range(1,38)), f'Pending IDs differ: {sorted(ids)}')

for name in ['README.md','FOREX_PENDING_IMPROVEMENTS.md']:
    original = (package / 'previous_documents/workspace/trad' / name).read_text(encoding='utf-8-sig')
    current = (work / 'trad' / name).read_text(encoding='utf-8-sig')
    require(current.endswith(original), f'Existing user documentation was not preserved: {name}')

require(sha(vault / 'RECOVERY_CHECKPOINT_20260921/MANIFEST.json') == '00d879d1ef0b13261b97f2f4f08da1acc5aa33755122057e29df2d5b72ae3e07', 'Original recovery manifest changed')
require(sha(package / 'specification/FOREX_CODEX_ENGINEERING_DESIGN.md') == '61d9713034f6f058d9f59ff11c0dd05b2a7e36583dffb2cd6d6300c64b8dc374', 'Governing design changed')

paths = [Path(x['path']) for x in changes if x['path'].endswith('.md')]
paths += [package / name for name in ['README.md','PATH_FORWARD.md','DESIGN_COVERAGE.md','CAUSALITY_AUDIT.md','OPERATIONS_AUDIT.md','VAULT_CLEANUP.md','HISTORICAL_RECORDS_INDEX.md','HISTORICAL_LINK_MAP.md']]
paths.append(work / 'trad/docs/FOREX_DESIGN_PATH_FORWARD_20260921.md')
link_count = 0
broken = []
for source in paths:
    body = source.read_text(encoding='utf-8-sig')
    if body.startswith('**Current direction'):
        body = body.split('\n---\n',1)[0]
    body = re.sub(r'```[^\n]*\n.*?```', '', body, flags=re.S)
    body = re.sub(r'`[^`\n]*`', '', body)
    for target in re.findall(r'\[[^\]\n]+\]\(([^)\n]+)\)', body):
        target = unquote(target.strip('<>')).split('#')[0]
        if not target or re.match(r'^(https?|mailto|app):',target):
            continue
        target = re.sub(r':\d+$','',target)
        if re.match(r'^/[A-Za-z]:/',target):
            target = target[1:]
        destination = Path(target)
        if not destination.is_absolute():
            destination = source.parent / destination
        link_count += 1
        if not destination.exists():
            broken.append({'document':str(source),'target':target})
require(not broken, f'Broken new navigation links: {json.dumps(broken)}')
print(json.dumps({'result':'passed','package_payloads':len(manifest['files']),'preserved_preimages':len(inventory),'updated_documents':len(changes),'requirements':15,'work_packages':12,'acceptance_cases_mapped':56,'pending_items_mapped':37,'existing_local_link_targets_checked':link_count,'historical_body_links_out_of_scope':True,'engine_tests_run':False,'original_recovery_manifest_unchanged':True}))
