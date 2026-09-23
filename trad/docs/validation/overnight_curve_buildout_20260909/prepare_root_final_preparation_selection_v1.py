"""Select exact retained pre-final documentation and independent review evidence."""
from pathlib import Path
import hashlib
import json
import time

BASE = Path(__file__).resolve().parent
FILES = [
    'prepare_root_final_preparation_selection_v1.py',
    'retain_before_final_documentation_v1.py',
    'documentation_before_final_001/BEFORE_FINAL_DOCUMENTATION_HISTORY_20260909.json',
    'documentation_before_final_001/FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md',
    'documentation_before_final_001/FOREX_PENDING_IMPROVEMENTS.md',
    'documentation_before_final_001/FOREX_PROJECT_LOG.md',
    'documentation_before_final_001/README.md',
    'documentation_before_final_001/FOREX_AUDIT_START_HERE.md',
    'review_actual_bridge_compatibility_v1.py',
    'ROOT_ACTUAL_CHAIN_BRIDGE_COMPATIBILITY_REVIEW_20260909.json',
    'PORTABLE_EVIDENCE_PREFLIGHT_006_20260909.json',
]


def main():
    entries = []
    for name in FILES:
        path = BASE / name
        raw = path.read_bytes()
        entries.append(dict(
            intended_member='docs/validation/overnight_curve_buildout_20260909/' + name,
            original_source=dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw)),
            kind='source' if name.endswith('.py') else 'retained_evidence',
            group='final_preparation_history_and_review',
            artifact_status='Exact dated preparation or review. Earlier documentation and preflight do not imply final publication or current runtime status.'))
    value = dict(schema_version='curated_root_final_preparation_selection_v1_20260909',
        prepared_epoch=time.time(), entries=entries, canonical_source_dependencies=[],
        file_count=len(entries), total_bytes=sum(row['original_source']['bytes'] for row in entries),
        project_copy_performed=False, vault_writes=False, broker_requests=False)
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    output = BASE / 'CURATED_ROOT_FINAL_PREPARATION_SELECTION_20260909.json'
    with output.open('xb') as handle:
        handle.write(raw)
    if output.read_bytes() != raw:
        raise ValueError('selection_readback_mismatch')
    print(json.dumps(dict(path=str(output), sha256=hashlib.sha256(raw).hexdigest(),
        files=len(entries), bytes=value['total_bytes'])))


if __name__ == '__main__':
    main()
