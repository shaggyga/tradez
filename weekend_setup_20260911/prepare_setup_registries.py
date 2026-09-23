"""Prepare isolated successor study configurations without activating a ledger.

Reuse original pair metadata/contracts and the existing joint registry builder.
The numeric model and all timing/cost limits remain unchanged. New cohort IDs
identify the changed worker schedule; old study databases cannot be resumed
under new source bindings. Call only after the staged source has been frozen.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import sys
import time

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT/'src'))
import oanda_pair_local_forecast_study_v2 as price
import prepare_joint_price_news_registry_v3 as joint_builder

SCOPE = 'weekend_setup_20260911'
ORIGINAL_METADATA_SHA256 = 'f7dc675925a765c0bb369282895f2839326476a05eadb7f5df6e0f27a10ed505'


def exclusive_json(path, value):
    raw = price.encoded(value)
    with path.open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    if path.read_bytes() != raw:
        raise ValueError('prepared_file_readback_changed')


def prepare():
    original = ROOT/'historical_config_copies/pair_local_forecast_study_v2_20260907.json'
    raw = original.read_bytes()
    if hashlib.sha256(raw).hexdigest() != ORIGINAL_METADATA_SHA256:
        raise ValueError('audited_original_pair_metadata_changed')
    old = json.loads(raw)
    if old['registry_id'] != 'pair_local_forecast_study_v2_20260907':
        raise ValueError('original_pair_registry_required')
    new = deepcopy(old)
    bindings = {name: hashlib.sha256((ROOT/'src'/name).read_bytes()).hexdigest()
                for name in sorted(price.REQUIRED_SOURCE_BINDINGS)}
    for name, expected in old['source_bindings'].items():
        if name != 'oanda_pair_local_forecast_study_v2.py' and bindings.get(name) != expected:
            raise ValueError('unexpected_price_model_or_contract_change:'+name)
    dependencies = {'python': platform.python_version(), **{
        name: importlib.metadata.version(name) for name in ('numpy', 'scikit-learn')}}
    prepared = time.time()
    new.update(source_bindings=bindings, dependency_versions=dependencies, created_epoch=prepared,
               preparation_clock_is_activation=False, predecessor_registry_sha256=hashlib.sha256(raw).hexdigest(),
               purpose='Original price-only models with the bounded scheduling repair; separate successor cohorts, no imported forecasts.')
    old_ids, new_ids = set(), set()
    for pair, item in new['pairs'].items():
        for family, spec in item['families'].items():
            contract = spec['contract']
            old_ids.update(contract['cohorts'].values())
            identity = f'causal_pair_family_v2_20260907.{pair}.{family}.{SCOPE}'
            new_ids.add(identity)
            contract.update(contract_id=identity, cohorts={family: identity}, source_bindings=bindings,
                            dependency_versions=dependencies)
            protocol = contract['evaluation_protocol']
            protocol.update(contract_id=identity+'.evaluation', cohorts={family: identity},
                historical_start_utc=datetime.fromtimestamp(prepared, timezone.utc).isoformat(),
                historical_end_utc=datetime.fromtimestamp(prepared+366*86400, timezone.utc).isoformat())
            price.validate_contract(contract)
            spec['contract_sha256'] = price.digest(contract)
    if old_ids & new_ids:
        raise ValueError('old_cohort_identity_reused')
    output = ROOT/'src/config/pair_local_forecast_weekend_setup_20260911.json'
    exclusive_json(output, new)
    checked = price.load_registry(output)
    joint_output = ROOT/'src/config/joint_price_news_weekend_setup_20260911.json'
    joint = joint_builder.prepare_registry(joint_output, scope=SCOPE, metadata_path=original,
                                          metadata_sha256=hashlib.sha256(raw).hexdigest())
    return {'status': 'prepared_not_activated', 'price_registry': str(output.relative_to(ROOT)),
        'price_registry_sha256': hashlib.sha256(output.read_bytes()).hexdigest(),
        'price_pairs': len(checked['pairs']), 'price_family_contracts': len(new_ids),
        'joint': joint, 'orders_enabled': False, 'worker_started': False,
        'database_opened': False, 'old_forecasts_imported': False,
        'note': 'Collection-enabled is an inert research capability flag, not evidence of a running service.'}


if __name__ == '__main__':
    result = prepare()
    exclusive_json(ROOT/'SUCCESSOR_REGISTRIES_PREPARED.json', result)
    print(json.dumps(result, indent=2))
