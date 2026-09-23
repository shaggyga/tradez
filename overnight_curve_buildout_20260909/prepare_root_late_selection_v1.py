"""Select explicit dated root receipts; no payload copy, inference or export."""
from pathlib import Path
import hashlib
import json
import time

BASE = Path(__file__).resolve().parent
FILES = [
    'prepare_root_late_selection_v1.py',
    'PORTABLE_PREFLIGHT_005_FAILURE_NOTE_20260909.md',
    'operations_v3/OPERATIONS_V3_OBSERVATION_002_20260909.json',
    'source_export_preflight_003/SOURCE_EXPORT_PREFLIGHT_20260909.json',
]


def main():
    entries = []
    for name in FILES:
        path = BASE / name
        raw = path.read_bytes()
        entries.append(dict(intended_member='docs/validation/overnight_curve_buildout_20260909/' + name,
            original_source=dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw)),
            kind='source' if name.endswith('.py') else 'retained_evidence',
            group='dated_late_root_observations',
            artifact_status='Original dated observation or preparation failure; no later runtime or publication status implied.'))
    result = dict(schema_version='curated_root_late_selection_v1_20260909', prepared_epoch=time.time(),
        entries=entries, canonical_source_dependencies=[], file_count=len(entries),
        total_bytes=sum(r['original_source']['bytes'] for r in entries),
        project_copy_performed=False, vault_writes=False, broker_requests=False)
    raw = (json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    output = BASE / 'CURATED_ROOT_LATE_EVIDENCE_SELECTION_V1_20260909.json'
    with output.open('xb') as handle:
        handle.write(raw)
    print(json.dumps(dict(path=str(output), sha256=hashlib.sha256(raw).hexdigest(),
        files=len(entries), bytes=result['total_bytes'])))


if __name__ == '__main__':
    main()
