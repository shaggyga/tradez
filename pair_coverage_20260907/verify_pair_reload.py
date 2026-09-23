"""Read-only validation for the first pair-local supervisor reload.

No worker, activation, broker, or process mutation is performed. The only
optional write is the requested review receipt outside the runtime directory.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import sys
import time

PROJECT = Path(r'C:\Users\zmoor\Documents\forex\trad')
EVIDENCE = PROJECT.parent/'pair_coverage_20260907'
EMPTY_TABLES = ('quotes', 'attempts', 'diagnostics', 'inputs', 'forecasts',
                'publication', 'consumption', 'entries', 'outcomes', 'exclusions')


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def verify_activations(registry, study_root, *, now_epoch):
    if type(now_epoch) not in (int, float) or not math.isfinite(now_epoch):
        raise ValueError('finite_current_clock_required')
    root = Path(study_root).resolve()
    rows = []
    for instrument, item in sorted(registry['pairs'].items()):
        contract = item['contract']
        if instrument != contract['instrument'] or digest(contract) != item['contract_sha256']:
            raise ValueError('pair_contract_binding_mismatch')
        database = (root/'pairs'/instrument/'study.sqlite').resolve()
        if not database.is_relative_to(root) or not database.is_file():
            raise ValueError('existing_pair_database_required:'+instrument)
        with closing(sqlite3.connect(database.as_uri()+'?mode=ro', uri=True, timeout=1)) as reader:
            reader.execute('PRAGMA query_only=ON')
            reader.execute('BEGIN')
            retained = reader.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
            activation = reader.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
            counts = {table:reader.execute('SELECT COUNT(*) FROM '+table).fetchone()[0] for table in EMPTY_TABLES}
            if (retained is None or retained[0] != item['contract_sha256']
                    or retained[1] != encoded(contract).decode()):
                raise ValueError('immutable_registered_contract_mismatch:'+instrument)
            if (activation is None or activation[1] != item['contract_sha256']
                    or type(activation[0]) not in (int, float) or not math.isfinite(activation[0])
                    or not 0 < activation[0] <= now_epoch):
                raise ValueError('preexisting_actual_activation_required:'+instrument)
            if any(counts.values()):
                raise ValueError('initial_pair_reload_requires_zero_evidence:'+instrument)
            reader.rollback()
        rows.append({'instrument':instrument, 'database':str(database),
            'activation_epoch':activation[0], 'contract_sha256':item['contract_sha256'], 'counts':counts})
    if not rows:
        raise ValueError('nonempty_activated_pair_registry_required')
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    destination = args.output.resolve()
    if not destination.is_relative_to(EVIDENCE.resolve()):
        raise ValueError('review_receipt_must_stay_in_pair_evidence_directory')
    sys.path.insert(0, str(PROJECT))
    import oanda_pair_local_forecast_study_v1 as worker
    config = PROJECT/'config/pair_local_forecast_study_v1_20260907.json'
    registry = worker.load_registry(config)
    observed = time.time()
    rows = verify_activations(registry, worker.STUDY, now_epoch=observed)
    pointers = {}
    for name in ('causal_forecast_study_current.json',):
        path = PROJECT/'config'/name
        pointers[name] = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
    receipt = {'schema_version':'pair_initial_reload_preflight_v1_20260907',
        'status':'passed', 'observed_epoch':observed,
        'observed_utc':datetime.fromtimestamp(observed, timezone.utc).isoformat(),
        'registry_sha256':digest(registry), 'registry_file_sha256':hashlib.sha256(config.read_bytes()).hexdigest(),
        'source_bindings_verified':registry['source_bindings'],
        'dependencies_verified':registry['dependency_versions'],
        'pair_count':len(rows), 'activations':rows, 'existing_pointer_sha256':pointers,
        'runtime_writes':0, 'activations_created':0, 'process_starts':0, 'process_stops':0,
        'can_place_orders':False, 'can_promote':False}
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open('x', encoding='utf-8', newline='\n') as handle:
        json.dump(receipt, handle, indent=2, allow_nan=False)
        handle.write('\n')
    print(json.dumps({'status':'passed','pair_count':len(rows),'receipt':str(destination)}))


if __name__ == '__main__':
    main()
