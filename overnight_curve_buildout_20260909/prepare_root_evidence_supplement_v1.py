"""Select explicit root-owned documentation and engineering evidence; no copies."""
from pathlib import Path
import hashlib
import json
import time

BASE=Path(__file__).resolve().parent
PREFIX='docs/validation/overnight_curve_buildout_20260909/'

FILES=[
    'WORK_PLAN.md', 'PORTABLE_EVIDENCE_PLAN.md', 'PROGRESS_0725_20260909.md',
    'prepare_portable_evidence_v1.py', 'test_prepare_portable_evidence_v1.py',
    'PORTABLE_EVIDENCE_PREPARATION_INDEPENDENT_REVIEW_20260909.json',
    'portable_evidence_preparation_final_tests.xml',
    'portable_evidence_preparation_tests.xml',
    'PORTABLE_EVIDENCE_PREFLIGHT_001_FAILURE_20260909.json',
    'PORTABLE_EVIDENCE_PREFLIGHT_002_20260909.json',
    'PORTABLE_EVIDENCE_PREFLIGHT_004_20260909.json',
    'portable_evidence_test_history/initial_tests.xml',
    'portable_evidence_test_history/initial_manifest.json',
    'capture_process_usage.ps1', 'capture_process_usage_v2.ps1',
    'process_usage_001/PROCESS_RESOURCE_OBSERVATION_20260909.json',
    'process_usage_002/PROCESS_RESOURCE_OBSERVATION_20260909.json',
    'operations_v3/OPERATIONS_V3_ACTUAL_ACCEPTANCE_20260909.json',
    'documentation_history_1025_20260909/FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md',
    'documentation_history_1025_20260909/FOREX_PENDING_IMPROVEMENTS.md',
    'documentation_history_1025_20260909/DOCUMENTATION_CONSOLIDATION_RECEIPT_20260909.json',
    'update_buildout_records_interim_v2.py',
]


def main():
    entries=[]
    for name in FILES:
        path=BASE/name
        raw=path.read_bytes()
        entries.append(dict(intended_member=PREFIX+name,
            original_source=dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw)),
            kind='source' if path.suffix in ('.py','.ps1') else 'retained_evidence',
            group='root_records_and_export_preparation',
            artifact_status='Dated evidence; final validation and source publication are separate.'))
    value=dict(schema_version='curated_root_evidence_selection_v1_20260909',prepared_epoch=time.time(),
        helper_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),entries=entries,
        canonical_source_dependencies=[],file_count=len(entries),total_bytes=sum(x['original_source']['bytes'] for x in entries),
        no_project_or_vault_copy_performed=True,
        limits=['Resource observations are uncontrolled before/after process measurements, not a throughput benchmark.',
                'Earlier documentation and failed export-preparation receipts retain their original chronology.',
                'Raw captures, credentials, process command arguments and full private evaluation inventories are excluded.',
                'Final runtime/outcome/navigation receipts will have a separate exact selection or manifest.'])
    out=BASE/'CURATED_ROOT_EVIDENCE_SELECTION_20260909.json'
    raw=(json.dumps(value,indent=2,sort_keys=True)+'\n').encode()
    with out.open('xb') as h:h.write(raw)
    print(json.dumps(dict(path=str(out),sha256=hashlib.sha256(raw).hexdigest(),files=len(entries),bytes=value['total_bytes'])))


if __name__=='__main__':main()
