"""Retain exact current documentation before the final dated consolidation."""
from pathlib import Path
import hashlib
import json
import os
import time

BASE = Path(__file__).resolve().parent
ROOT = BASE.parent / 'trad'
OUT = BASE / 'documentation_before_final_001'
FILES = ['docs/FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md',
         'FOREX_PENDING_IMPROVEMENTS.md', 'FOREX_PROJECT_LOG.md',
         'README.md', 'FOREX_AUDIT_START_HERE.md']


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    if OUT.exists():
        raise ValueError('fresh_history_directory_required')
    payloads = {name: (ROOT / name).read_bytes() for name in FILES}
    if any(len(raw) > 4 * 1024 * 1024 for raw in payloads.values()):
        raise ValueError('documentation_bound')
    OUT.mkdir()
    records = []
    for name, raw in payloads.items():
        target = OUT / Path(name).name
        with target.open('xb') as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        if target.read_bytes() != raw or (ROOT / name).read_bytes() != raw:
            raise ValueError('documentation_changed_during_retention')
        records.append(dict(original_path=str(ROOT / name), retained_path=str(target),
                            sha256=sha(raw), bytes=len(raw)))
    value = dict(schema_version='before_final_documentation_history_v1_20260909',
        retained_epoch=time.time(), helper_sha256=sha(Path(__file__).read_bytes()),
        records=records, original_files_changed=False, vault_writes=False)
    raw = (json.dumps(value, indent=2, sort_keys=True) + '\n').encode()
    receipt = OUT / 'BEFORE_FINAL_DOCUMENTATION_HISTORY_20260909.json'
    with receipt.open('xb') as handle:
        handle.write(raw)
    print(json.dumps(dict(path=str(receipt), sha256=sha(raw), files=len(records))))


if __name__ == '__main__':
    main()
