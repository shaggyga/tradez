"""Inspect source export eligibility without publishing or copying payloads."""
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(r'C:\Users\zmoor\Documents\forex\trad')
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from tools import vault_worktree_snapshot as snapshot


def main():
    started = time.time()
    state, names, modes = snapshot.source_state(ROOT)
    private_values = snapshot.known_private_values([ROOT / 'creds'])
    source_binding = snapshot.digest((ROOT / 'tools/vault_worktree_snapshot.py').read_bytes())
    scanned, excluded, failures = [], Counter(), []
    for name in names:
        omitted = snapshot.excluded_reason(name)
        if omitted:
            excluded[omitted] += 1
            continue
        try:
            if modes.get(name) in {'120000', '160000'}:
                raise ValueError('unsupported_source_link_or_submodule')
            raw = snapshot.regular_file(ROOT, name).read_bytes()
            snapshot.audit_payload(name, raw, private_values)
            if name.endswith('.py'):
                compile(raw, name, 'exec', dont_inherit=True)
            scanned.append(dict(path=name, sha256=snapshot.digest(raw), bytes=len(raw)))
        except SyntaxError as error:
            failures.append(dict(path=name, type='SyntaxError', line=error.lineno))
        except Exception as error:
            # Scanner exceptions contain member/rule names, but no payload is retained.
            failures.append(dict(path=name, type=type(error).__name__,
                reason='credential_or_source_eligibility_check_failed'))
    after, after_names, _ = snapshot.source_state(ROOT)
    source_end = snapshot.digest((ROOT / 'tools/vault_worktree_snapshot.py').read_bytes())
    report = dict(schema_version='source_export_readonly_preflight_v1_20260909',
        started_epoch=started, completed_epoch=time.time(),
        status='source_checks_passed_at_individual_reads' if not failures else 'issues',
        source_inventory_unchanged_during_scan=(state == after and names == after_names),
        inspector_source_unchanged=source_binding == source_end,
        inspector_source_sha256=source_binding, helper_source_sha256=snapshot.digest(Path(__file__).read_bytes()),
        scanned_files=scanned, eligible_file_count=len(scanned), excluded_counts=dict(excluded), failures=failures,
        known_private_value_check_performed=bool(private_values),
        scope='Bounded Git-listed source only; ignored runtime/private trees not recursively inventoried. Individual reads while implementation continues; final publication requires a fresh unchanged snapshot.',
        source_mutations=False, vault_writes=False, broker_requests=False)
    path = OUT / 'SOURCE_EXPORT_PREFLIGHT_20260909.json'
    raw = (json.dumps(report, sort_keys=True, indent=2)+'\n').encode()
    with path.open('xb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    print(json.dumps(dict(path=str(path), sha256=snapshot.digest(raw), status=report['status'],
        files=len(scanned), failures=failures, inventory_unchanged=report['source_inventory_unchanged_during_scan'],
        vault_writes=False)))


if __name__ == '__main__':
    main()
