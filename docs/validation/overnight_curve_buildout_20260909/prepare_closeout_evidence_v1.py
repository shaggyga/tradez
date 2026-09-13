"""Select explicit final observations for the user-requested early closeout."""
from pathlib import Path
import hashlib
import json
import os
import time

BASE = Path(__file__).resolve().parent
FILES = [
    'prepare_closeout_evidence_v1.py',
    'operations_v3/OPERATIONS_V3_OBSERVATION_003_20260909.json',
    'operations_v3/OPERATIONS_V3_OBSERVATION_004_20260909.json',
    'try_pair_publication_audit_v1/refresh_try_readiness_v1.py',
    'try_pair_publication_audit_v1/TRY_READINESS_REFRESH_001_20260909.json',
    'try_pair_publication_audit_v1/TRY_READINESS_REFRESH_INDEPENDENT_REVIEW_20260909.json',
    'final_evaluations_20260909/FINAL_EVALUATION_PREPARATION_20260909.json',
    'final_evaluations_20260909/FINAL_EVALUATION_EARLY_CLOSE_PLAN_CHANGE_20260909.json',
]


def main():
    entries = []
    for name in FILES:
        path = BASE / name
        raw = path.read_bytes()
        entries.append(dict(intended_member='docs/validation/overnight_curve_buildout_20260909/' + name,
            original_source=dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw)),
            kind='source' if name.endswith('.py') else 'retained_evidence',
            group='user_requested_early_closeout',
            artifact_status='Original dated observation, independent source review or canceled-before-start evaluation plan. No nine-AM observation or completed later evaluation implied.'))
    result = dict(schema_version='curated_user_requested_closeout_selection_v1_20260909',
        prepared_epoch=time.time(), entries=entries, canonical_source_dependencies=[],
        file_count=len(entries), total_bytes=sum(r['original_source']['bytes'] for r in entries),
        project_copy_performed=False, vault_writes=False, broker_requests=False)
    raw = (json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    output = BASE / 'CURATED_USER_REQUESTED_CLOSEOUT_SELECTION_20260909.json'
    with output.open('xb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    if output.read_bytes() != raw:
        raise ValueError('closeout_selection_readback')
    print(json.dumps(dict(path=str(output), sha256=hashlib.sha256(raw).hexdigest(),
        files=len(entries), bytes=result['total_bytes'])))


if __name__ == '__main__':
    main()
