"""Retain a recovery checkpoint for the continued, offline work."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json

ROOT = Path(__file__).resolve().parent
study = ROOT / 'direction_richer_archive_003'
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
now = datetime.now(timezone.utc).isoformat()
state = {
    'observed_utc': now, 'latest_user_priority': 'current best tested build by next market reopening',
    'original_run_target_utc': '2026-09-13T01:42:00Z', 'services_or_orders_activated': False,
    'background_reviewers': 'all three reached account usage limit; no completed review is invented',
    'completed_preparation': {'path': str(study / 'prepared_002'),
        'receipt_sha256': sha(study / 'prepared_002/RICHER_PREPARATION_RECEIPT.json'),
        'primary_rows': 53512475, 'original_hourly_ids': 875146, 'pairs': 68},
    'retained_evaluation_001_failure': {'path': str(study / 'evaluation_001/EVALUATION_FAILED.json'),
        'sha256': sha(study / 'evaluation_001/EVALUATION_FAILED.json'), 'fits_completed': 0,
        'cause': 'four immutable pair receipts exceed the original16MiB reader bound'},
    'consumer_revision': {'path': str(study / 'TRANSPORT_CONSUMER_FREEZE_001.json'),
        'sha256': sha(study / 'TRANSPORT_CONSUMER_FREEZE_001.json'),
        'change': '32MiB pair-receipt read only; exact prepared bytes and scientific choices retained',
        'root_checks': 23, 'new_independent_review': False},
    'evaluation_002': {'path': str(study / 'evaluation_002'), 'exec_session': 90427,
        'status': 'running; first H60 fit started; no completion claimed'},
    'availability_transport_001': {'path': str(study / 'availability_transport_001'), 'exec_session': 26776,
        'status': 'running; unchanged availability math, new explicit receipt reader'},
    'pending': ['complete six fixed fits', 'complete coverage reconciliation',
        'recompute result metrics with derived transport checker', 'replay six saved artifacts with derived transport verifier',
        'seal and publish inactive typed outcome v4 profile after root final review',
        'publish canonical/vault status and readiness gaps'],
    'typed_outcomes': {'path': str(ROOT / 'runtime/outcome_probability_integration_v4_001'),
        'latest_author_receipt': 'TESTS_004.json', 'author_passes': 170,
        'scope': 'source/owned synthetic SQLite only; original clocks, nullable midpoint probability, hold/flat and content-ID wrapper',
        'independent_scope': '19 independent boundary cases passed before last wrapper/hold review; final review interrupted by usage limit'},
    'operational_blocker': 'earlier clock-monitor startup automatic approval review rejected blocked by policy; not retried',
    'private_state_policy': 'no D reads, credentials, private DB migration, expired trial reset or service activation'
}
path = ROOT / 'CONTINUATION_STATE_002.json'
with path.open('x', encoding='utf-8') as f:
    json.dump(state, f, indent=2); f.write('\n')
with (ROOT / 'WORK_LOG.md').open('a', encoding='utf-8') as f:
    f.write(f'\n\n### {now} — complete preparation and separately frozen receipt reader\n\n'
        'Preparation003 completed all68 pairs,53,512,475 primary rows and875,146 original hourly IDs, with no removed IDs. '
        'Its receipt998c24d465cb71c26cbf365d2a5de48b80140d9d952690d068204d2976522daa remains immutable. '
        'Evaluation001 failed before the first fit because four detailed pair receipts exceed16MiB; the largest is24,234,851bytes. '
        'A separately frozen transport consumer raises only that per-pair read allowance to32MiB, keeping all strict source/path/hash/ID checks '
        'and all scientific choices. Root23 checks include exact unchanged ASTs, reproducing the old bound, enforcing the new bound, and reading '
        'the four original large receipts against their retained hashes. No raw feature preparation was repeated. '
        'Consumer freeze813c6f16e8d59256d85e0f658751b779912e4d6cef82f8fbba4da3de14edcef8 and evaluation002 retain both scientific and transport provenance. '
        'All three background reviewers reached account usage limits before independently reviewing these new transport bytes. '
        'Root is continuing bounded work in the existing turn; no later completion or independent review is inferred. '
        'User priority is current best tested build before market reopening.\n')
print(json.dumps({'checkpoint': str(path), 'sha256': sha(path)}))
