"""Verify completed projection-policy outputs without changing their frozen run source closure."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
PARITY = ('policy_decisions.jsonl', 'policy_state.json', 'event_ledger.jsonl', 'final_state.json', 'accounting_audit.json')
read = lambda p: json.loads(Path(p).read_bytes())
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()

def review(contract_path, runs):
    from publication import verify_completed_run
    c, runs = read(contract_path), Path(runs); rows = []
    for method in c['methods']:
        for scenario in c['scenarios']:
            pair = []
            for engine in c['engines']:
                root = runs / (method + '-' + scenario + '-' + engine); identity = read(root / 'RUN_IDENTITY.json')
                manifest = verify_completed_run(root, identity); report = read(root / 'run_report.json')
                if report['accounting_oracle']['status'] != 'verified' or report['decision_count'] != 54:
                    raise ValueError('currency_projection_policy_accounting_gate')
                pair.append(root); rows.append({'run_id': root.name, 'identity': identity['fingerprint'], 'payloads': len(manifest['payloads'])})
            if any(sha(pair[0] / name) != sha(pair[1] / name) for name in PARITY):
                raise ValueError('currency_projection_policy_engine_neutral_parity')
    if len(rows) != c['expected_run_count']:
        raise ValueError('currency_projection_policy_matrix_count')
    return {'status': 'accepted_within_scope', 'runs': len(rows), 'engine_neutral_parity_files': list(PARITY), 'independent_review': False, **c['readiness']}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--contract', type=Path, required=True); parser.add_argument('--runs-dir', type=Path, required=True)
    print(json.dumps(review(parser.parse_args().contract, parser.parse_args().runs_dir), sort_keys=True))
