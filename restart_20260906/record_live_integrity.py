"""Retain the first completed compact runtime audit independently of startup."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import sys

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / 'trad'
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))
from oanda_integrity_publication import restore_integrity_details

path = ROOT/'data/oanda_training_manager/state/project_integrity_audit_v1.json'
raw = path.read_bytes()
payload = json.loads(raw)
restored = restore_integrity_details(payload, snapshot_dir=path.parent)
sections = payload['_detail_publication']['section_keys']
receipt = {
    'schema_version': 'forex_research_restart_live_integrity_observation_v1',
    'observed_utc': datetime.now(timezone.utc).isoformat(),
    'audit_started_utc': payload['audit_started_utc'],
    'audit_finished_utc': payload['audit_finished_utc'],
    'status': payload['status'], 'supported_decision': payload['supported_decision'],
    'can_place_orders': payload['can_place_orders'], 'can_promote': payload['can_promote'],
    'snapshot_bytes': len(raw), 'snapshot_sha256': hashlib.sha256(raw).hexdigest(),
    'detail_artifacts_verified': len(sections),
    'detail_rows_restored': {key: len(restored[key]['episode_rows']) for key in sections},
    'failures': payload['failures'],
    'publication_status': payload['publication_status'],
    'snapshot_fresh_at_publication': payload['snapshot_fresh_at_publication'],
    'publication_latency_sec': payload['publication_latency_sec'],
    'interpretation': 'Initial refresh completed after the startup receipt observation; degraded checks remain unrelaxed. This is not an execution authorization or an entire-cycle speed comparison.',
    'prior_startup_receipt_sha256': hashlib.sha256((ROOT/'FOREX_RESEARCH_RESTART_VALIDATION_20260906.json').read_bytes()).hexdigest(),
}
with (OUT/'initial_live_integrity_snapshot.json').open('xb') as f: f.write(raw)
encoded = (json.dumps(receipt, indent=2)+'\n').encode()
with (OUT/'initial_live_integrity_observation.json').open('xb') as f: f.write(encoded)
vault = Path(r'C:\Users\zmoor\OneDrive\thevault\projects\forex\maintenance')
with (vault/'RESEARCH_RESTART_LIVE_INTEGRITY_OBSERVATION_20260906.json').open('xb') as f: f.write(encoded)
print(json.dumps(receipt, indent=2))
