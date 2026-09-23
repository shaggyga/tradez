"""Check that staging preserves every installed registered source byte."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(r'C:\Users\zmoor\Documents\forex\trad')
AREA = ROOT.parent / 'operational_repairs_20260913'
stage = json.loads((AREA / 'compact_native_stage_008/SOURCE_STAGE.json').read_bytes())
checked = {}
for name, expected in stage['source_bindings'].items():
    actual = hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
    indexed = subprocess.run(['git', 'show', ':' + name], cwd=ROOT, check=True, capture_output=True).stdout
    indexed_sha = hashlib.sha256(indexed).hexdigest()
    if actual != expected or indexed_sha != expected:
        raise ValueError('registered_source_bytes_changed:' + name)
    checked[name] = expected
subprocess.run(['git', '-c', 'core.whitespace=cr-at-eol', 'diff', '--cached', '--check'], cwd=ROOT, check=True)
receipt = {'observed_utc': datetime.now(timezone.utc).isoformat(),
           'source_bindings': checked, 'source_count': len(checked),
           'working_and_index_bytes_match_frozen_stage': True,
           'whitespace_check': 'passed_with_cr_at_eol_preserved',
           'scope': 'Read-only Git/source verification; no runtime or broker I/O.'}
output = AREA / ('GIT_V8_SOURCE_CHECK_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '.json')
with output.open('x', encoding='utf-8') as stream:
    json.dump(receipt, stream, sort_keys=True, indent=2)
print(json.dumps({'receipt': str(output), 'source_count': len(checked), 'passed': True}))
