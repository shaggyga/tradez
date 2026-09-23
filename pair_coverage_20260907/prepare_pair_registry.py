"""Prepare an inert registry, then separately activate empty research ledgers.

Nothing happens when imported. --prepare writes only a new config and evidence
receipt. --activate separately registers empty per-pair ledgers using actual
post-contract-commit clocks. Neither mode starts workers or observes quotes.
Existing registered config bytes are never overwritten.
An interruption after config creation but before its preparation receipt needs
manual evidence review; that preparation boundary is deliberately fail-closed.
"""
from copy import deepcopy
from contextlib import closing
from datetime import datetime, timedelta, timezone
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import time

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / 'trad'
REGISTRY = ROOT / 'config/pair_local_forecast_study_v1_20260907.json'
RUNTIME = ROOT / 'data/oanda_training_manager/pair_local_forecast_study_v1'
PREPARATION = OUT / 'PAIR_REGISTRY_PREPARATION_20260907.json'
ACTIVATION = OUT / 'PAIR_REGISTRY_ACTIVATION_20260907.json'
SOURCE_NAMES = frozenset({
    'oanda_pair_local_forecast_study_v1.py', 'oanda_causal_forecast_ledger_pair_v1.py',
    'oanda_fixed_forecast_evaluation_pair_v1.py', 'oanda_pair_local_models_v1.py',
    'oanda_causal_forecast_inputs_pair_v1.py', 'oanda_causal_forecast_inputs.py',
    'oanda_causal_forecast_inputs_gap_v2.py', 'oanda_exact_price_scoring.py',
    'oanda_causal_prediction_baselines.py',
})
INERT_FLAGS = ('account_eligible', 'proof_eligible', 'can_authorize', 'can_place_orders',
               'can_promote', 'historical_rows_imported')


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def digest(value):
    return sha(encoded(value))


def exclusive_write(path, raw):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def source_bindings():
    return {name: sha((ROOT / name).read_bytes()) for name in sorted(SOURCE_NAMES)}


def metadata():
    quote_raw = (OUT / 'QUOTE_METADATA_SNAPSHOT.json').read_bytes()
    preflight_raw = (OUT / 'COVERAGE_PREFLIGHT.json').read_bytes()
    quote = json.loads(quote_raw)
    preflight = json.loads(preflight_raw)
    if (quote.get('producer') != 'practice_007_dedicated_quote_stream'
            or preflight.get('producer') != quote['producer']
            or preflight.get('source_quote_sha256') != sha(quote_raw)):
        raise ValueError('metadata_evidence_binding_mismatch')
    pips = preflight['pip_sizes']
    if len(pips) != 68 or set(pips) != set(quote['quotes']):
        raise ValueError('complete_68_pair_metadata_required')
    for pair, pip in pips.items():
        if type(pip) not in (int, float) or pip not in (.0001, .001, .01) or pip != quote['quotes'][pair]['pip']:
            raise ValueError('explicit_quote_pip_metadata_mismatch:' + pair)
    return dict(sorted(pips.items())), {
        'quote_snapshot_sha256': sha(quote_raw), 'preflight_sha256': sha(preflight_raw),
        'observed_epoch': preflight['observed_epoch'], 'producer': quote['producer'],
        'semantics': 'Frozen observed quote metadata; no currency-suffix pip inference and no outcome-based pair selection.',
    }


def build_registry():
    """Build and validate in memory; do not write files or activate ledgers."""
    sys.path.insert(0, str(ROOT))
    from oanda_causal_forecast_ledger_pair_v1 import SCHEMA, validate_contract
    from oanda_causal_forecast_inputs_pair_v1 import NUMERICAL_SOURCE_SHA256, dependency_versions
    from oanda_pair_local_models_v1 import PARAMETERS
    from oanda_pair_local_forecast_study_v1 import REQUIRED_SOURCE_BINDINGS, REGISTRY_SCHEMA

    if SOURCE_NAMES != REQUIRED_SOURCE_BINDINGS:
        raise ValueError('worker_registration_source_set_mismatch')
    pips, pip_evidence = metadata()
    bindings = source_bindings()
    if bindings['oanda_pair_local_models_v1.py'] != NUMERICAL_SOURCE_SHA256:
        raise ValueError('numerical_loader_freeze_mismatch')
    template_path = ROOT / 'config/causal_forecast_study_eurusd_v1_20260907.json'
    template_raw = template_path.read_bytes()
    template = json.loads(template_raw)
    dependencies = dependency_versions()
    if dependencies != template['dependency_versions']:
        raise ValueError('validated_runtime_dependency_versions_required')
    for name in SOURCE_NAMES & set(template['source_bindings']):
        if bindings[name] != template['source_bindings'][name]:
            raise ValueError('registered_predecessor_helper_changed:' + name)
    prepared = datetime.now(timezone.utc)
    registry = {
        'schema_version': REGISTRY_SCHEMA, 'registry_id': 'pair_local_forecast_study_v1_20260907',
        'prepared_utc': prepared.isoformat(), 'collection_enabled': True, 'research_only': True,
        **dict.fromkeys(INERT_FLAGS, False), 'source_bindings': bindings,
        'dependency_versions': dependencies, 'pip_metadata_evidence': pip_evidence,
        'predecessor_template_file_sha256': sha(template_raw), 'pairs': {},
        'pair_selection': 'All 68 instruments in the frozen observed quote metadata, independent of returns or current readiness.',
        'activation_policy': 'Prepared config is inert until separate actual-clock per-pair ledger registration; no orders or promotion.',
    }
    for pair, pip in pips.items():
        contract = deepcopy(template)
        contract.pop('companion_contract_sha256', None)
        identity = digest({'sources': bindings, 'parameters': dict(PARAMETERS),
            'instrument': pair, 'pip_size': pip, 'dependencies': dependencies})
        cohorts = {family: f'causal_pair_local_v1_20260907.{pair}.{family}.{identity[:16]}'
            for family in ('probabilistic_state_space', 'ridge_return_repaired')}
        contract.update({
            'schema_version': SCHEMA, 'contract_id': f'causal_pair_local_collection_v1_20260907.{pair}',
            'instrument': pair, 'pip_size': pip, 'input_pairs': [pair], 'cohorts': cohorts,
            'prepared_utc': prepared.isoformat(), 'source_bindings': bindings,
            'dependency_versions': dependencies, 'model_version': 'sha256:' + identity,
            'feature_version': 'sha256:' + bindings['oanda_causal_forecast_inputs_pair_v1.py'],
            'numeric_model_source_sha256': bindings['oanda_pair_local_models_v1.py'],
            'predecessor_contract_sha256': digest(template),
            'operational_revision': 'Separate own-pair ridge and EWMA research cohort. Peers cannot withhold this pair; no historical forecasts or outcomes imported.',
            'input_policy': {
                'alignment': 'Actual own-pair UTC minute starts',
                'availability': 'Conservative observed clock after stable source read; computation completed before issuance',
                'gap_policy': 'Require 61 consecutive own-pair closes. Ridge uses complete real 121-price windows and exact H1 labels; EWMA resets at the latest own-pair gap. No peer inputs, imputation or compressed timestamps.',
                'maximum_common_bar_age_sec': 900, 'maximum_source_rows_per_pair': 1024,
                'minimum_current_common_bars': 61,
            },
            'collection_enabled': True, 'research_only': True, **dict.fromkeys(INERT_FLAGS, False),
        })
        contract['evaluation_protocol'].update({
            'contract_id': f'causal_pair_local_evaluation_v1_20260907.{pair}',
            'instrument': pair, 'pip_size': pip, 'cohorts': cohorts,
            'model_version': contract['model_version'], 'feature_version': contract['feature_version'],
            'historical_start_utc': prepared.isoformat(),
            'historical_end_utc': (prepared + timedelta(days=365)).isoformat(),
        })
        validate_contract(contract)
        registry['pairs'][pair] = {'pip_size': pip, 'contract_sha256': digest(contract), 'contract': contract}
    raw = encoded(registry)
    if len(raw) > 1024 * 1024:
        raise ValueError('registry_exceeds_worker_1MiB_bound')
    if source_bindings() != bindings:
        raise ValueError('source_changed_during_preparation')
    return registry


def prepare():
    if REGISTRY.exists() or PREPARATION.exists() or ACTIVATION.exists():
        raise ValueError('refuse_to_replace_prepared_or_activated_registration')
    registry = build_registry()
    raw = encoded(registry)
    # Validate the final bytes through the worker reader before publishing config.
    candidate = OUT / ('PAIR_REGISTRY_CANDIDATE.' + str(time.time_ns()) + '.json')
    exclusive_write(candidate, raw)
    from oanda_pair_local_forecast_study_v1 import load_registry
    try:
        if load_registry(candidate) != registry or source_bindings() != registry['source_bindings']:
            raise ValueError('final_registry_or_source_validation_failed')
        exclusive_write(REGISTRY, raw)
    finally:
        candidate.unlink(missing_ok=True)
    receipt = {'schema_version': 'pair_registry_preparation_receipt_v1_20260907',
        'prepared_observed_epoch': time.time(), 'registry_path': str(REGISTRY),
        'registry_file_sha256': sha(raw), 'registry_payload_sha256': digest(registry),
        'registry_bytes': len(raw), 'pair_count': len(registry['pairs']),
        'source_bindings': registry['source_bindings'], 'pip_metadata_evidence': registry['pip_metadata_evidence'],
        'study_activation_performed': False, 'runtime_started': False,
        'research_only': True, 'can_place_orders': False, 'can_promote': False}
    exclusive_write(PREPARATION, encoded(receipt))
    return receipt


def assert_existing_empty_compatible(path, contract_sha):
    if not path.exists():
        return
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as connection:
        connection.execute('PRAGMA query_only=ON')
        connection.execute('BEGIN')
        row = connection.execute('SELECT sha FROM contract WHERE id=1').fetchone()
        if row is None or row[0] != contract_sha:
            raise ValueError('refuse_existing_nonmatching_pair_database')
        active = connection.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
        if active is not None and (active[1] != contract_sha or type(active[0]) not in (int, float)
                or not math.isfinite(active[0]) or not 0 < active[0] <= time.time()):
            raise ValueError('refuse_invalid_existing_activation_receipt')
        for table in ('quotes', 'attempts', 'diagnostics', 'inputs', 'forecasts', 'publication',
                      'consumption', 'entries', 'outcomes', 'exclusions'):
            if connection.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0] != 0:
                raise ValueError('refuse_activation_after_evidence_collection:' + table)


def activate():
    sys.path.insert(0, str(ROOT))
    from oanda_pair_local_forecast_study_v1 import load_registry
    from oanda_causal_forecast_ledger_pair_v1 import CausalForecastLedger
    if ACTIVATION.exists():
        raise ValueError('activation_receipt_already_exists')
    preparation = json.loads(PREPARATION.read_bytes())
    raw = REGISTRY.read_bytes()
    if sha(raw) != preparation['registry_file_sha256']:
        raise ValueError('prepared_registry_file_binding_changed')
    registry = load_registry(REGISTRY)
    if digest(registry) != preparation['registry_payload_sha256']:
        raise ValueError('prepared_registry_payload_binding_changed')
    runtime = RUNTIME.resolve()
    if not runtime.is_relative_to((ROOT / 'data/oanda_training_manager').resolve()):
        raise ValueError('runtime_path_outside_intended_training_root')
    paths = {pair: (runtime / 'pairs' / pair / 'study.sqlite').resolve() for pair in registry['pairs']}
    if any(not path.is_relative_to(runtime) for path in paths.values()):
        raise ValueError('pair_database_path_outside_intended_runtime')
    for pair, path in paths.items():
        assert_existing_empty_compatible(path, registry['pairs'][pair]['contract_sha256'])
    begun = time.time()
    rows = []
    for pair, item in registry['pairs'].items():
        path = paths[pair]
        ledger = CausalForecastLedger(path, item['contract'], activate=True)
        try:
            counts = ledger.counts()
            if any(counts.values()):
                raise ValueError('new_registration_must_have_zero_observations')
            rows.append({'instrument': pair, 'pip_size': item['pip_size'],
                'contract_sha256': ledger.contract_hash, 'activation_epoch': ledger.activated_epoch,
                'database': str(path), 'counts': counts})
        finally:
            ledger.close()
        rows[-1]['database_sha256_after_close'] = sha(path.read_bytes())
    if source_bindings() != registry['source_bindings']:
        raise ValueError('source_changed_during_activation')
    receipt = {'schema_version': 'pair_registry_activation_receipt_v1_20260907',
        'begun_epoch': begun, 'completed_epoch': time.time(),
        'registry_file_sha256': sha(raw), 'registry_payload_sha256': digest(registry),
        'registered_pair_count': len(rows), 'pairs': rows,
        'forecasts_imported': 0, 'quotes_imported': 0, 'outcomes_imported': 0,
        'runtime_started': False, 'research_only': True, 'can_place_orders': False, 'can_promote': False}
    exclusive_write(ACTIVATION, encoded(receipt))
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--prepare', action='store_true')
    mode.add_argument('--activate', action='store_true')
    args = parser.parse_args()
    result = prepare() if args.prepare else activate()
    print(json.dumps({key: value for key, value in result.items() if key not in ('source_bindings', 'pairs')}, indent=2))


if __name__ == '__main__':
    main()
