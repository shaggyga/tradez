"""Select explicit continuation observations and retain the resumed work scope."""
from pathlib import Path
import hashlib
import json
import os
import time

BASE = Path(__file__).resolve().parent
FILES = [
    'prepare_continuation_root_evidence_v1.py',
    'retain_early_close_publication_for_continuation.py',
    'continuation_prior_publication_001/FOREX_OVERNIGHT_CURVE_BUILDOUT_VALIDATION_20260909.json',
    'continuation_prior_publication_001/OVERNIGHT_VAULT_PUBLICATION_20260909.json',
    'continuation_prior_publication_001/PRIOR_PUBLICATION_RETENTION_20260909.json',
    'joint_v3_reassessment_v1/summarize_pair_dispersion_v1.py',
    'joint_v3_reassessment_v1/pair_dispersion_001/ALL_PAIR_SCORE_DISPERSION_20260909.json',
    'joint_v3_reassessment_v1/pair_dispersion_001/PAIR_DISPERSION_INDEPENDENT_REVIEW_20260909.json',
    'operations_v3/OPERATIONS_V3_OBSERVATION_005_20260909.json',
    'try_pair_publication_audit_v1/TRY_READINESS_REFRESH_002_20260909.json',
]


def main():
    entries = []
    for name in FILES:
        path = BASE / name
        raw = path.read_bytes()
        entries.append(dict(intended_member='docs/validation/overnight_curve_buildout_20260909/' + name,
            original_source=dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw)),
            kind='source' if name.endswith('.py') else 'retained_evidence', group='resumed_continuation_to_0900',
            artifact_status='Dated original continuation observation or explicitly prior published evidence; observation cutoffs and inactive status remain separate.'))
    result = dict(schema_version='curated_continuation_root_evidence_v1_20260909',
        prepared_epoch=time.time(), user_resumption_instruction='Continue work till 9',
        first_root_observation_of_resumption_utc='2026-09-09 12:25:49 UTC',
        work_deadline_utc='2026-09-09 13:00:00 UTC', entries=entries, canonical_source_dependencies=[],
        file_count=len(entries), total_bytes=sum(r['original_source']['bytes'] for r in entries),
        scope='Additional evidence only. The preserved early-close validation separately indexes its original629members; the newly verified source archive inventories the full exported tree.',
        project_copy_performed=False, vault_writes=False, broker_requests=False, automation_created=False)
    raw = (json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    output = BASE / 'CURATED_CONTINUATION_ROOT_EVIDENCE_SELECTION_20260909.json'
    with output.open('xb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    if output.read_bytes() != raw:
        raise ValueError('continuation_selection_readback')
    print(json.dumps(dict(path=str(output), sha256=hashlib.sha256(raw).hexdigest(),
        files=len(entries), bytes=result['total_bytes'])))


if __name__ == '__main__':
    main()
