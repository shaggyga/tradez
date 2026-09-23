"""Prepare the bounded successor after actual research activation and old flat completion.

Writes only an isolated handover folder. No broker calls, worker launches or live
configuration changes. The original deadline, practice account and policy stay exact.
"""
import argparse
from contextlib import closing
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time

AREA = Path(__file__).resolve().parent
ROOT = AREA.parent / 'trad'
DATA = ROOT / 'data/oanda_training_manager'
OLD_CONFIG = ROOT / 'config/practice007_native_v7_20260913_v1.json'
NEW_TRIAL = 'practice007_native_v7_20260913_v2'
NEW_MANIFEST = 'config/practice_native_recovery_v3_20260914.json'


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def prepare(stage):
    stage = stage.resolve()
    if stage.parent != AREA.resolve():
        raise ValueError('owned_source_stage_required')
    sys.path.insert(0, str(ROOT))
    import oanda_practice_trial_runner_v2 as runner
    import oanda_practice_trial_recovery_v2 as recovery
    from oanda_practice_forecast_adapter_v2 import validate_source_config
    old = runner.validate_config(OLD_CONFIG, verify_forecast_source=False)
    trial = recovery.local_trial(DATA / old['trial_id'], old)
    if not trial['completed'] or trial['unresolved_intents'] or trial['loss_stop_latched']:
        raise ValueError('old_completed_flat_without_loss_latch_required')
    with closing(sqlite3.connect((DATA / old['trial_id'] / 'trial.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        if db.execute('SELECT count(*) FROM intents').fetchone()[0] != 0:
            raise ValueError('nonzero_old_intents_require_explicit_accounting_migration')
    if time.time() >= old['stop_epoch']:
        raise ValueError('original_finite_trial_has_expired')
    registry = ROOT / 'config/joint_price_news_operational_v4_20260913.json'
    study = DATA / 'operational_repair_20260913_v4/joint_price_news_study_v7'
    selected = dict(registry_path=str(registry.relative_to(ROOT)).replace('\\', '/'),
                    registry_sha256=sha(registry.read_bytes()),
                    study_path=str(study.relative_to(ROOT)).replace('\\', '/'),
                    activation_sha256=sha((study / 'activation_receipt.json').read_bytes()))
    validate_source_config(ROOT, selected)
    new = deepcopy(old)
    new.pop('config_sha256')
    new.update(trial_id=NEW_TRIAL, forecast_source=selected,
               scope='Successor of the completed-flat zero-intent native practice trial after an explicit news source-generation handover. Original finite window, account, execution sources and policy remain unchanged; no profitability acceptance.')
    new['config_sha256'] = sha(encoded(new))
    config_raw = encoded(new)
    prior_manifest = ROOT / 'config/practice_native_recovery_v2_20260913.json'
    manifest = json.loads(prior_manifest.read_bytes())
    manifest.pop('manifest_sha256')
    manifest.update(trial_id=NEW_TRIAL, config_path='config/' + NEW_TRIAL + '.json',
                    config_file_sha256=sha(config_raw))
    wrapper = (AREA / 'practice_handover_candidate_v2/start_oanda_practice_recovery_v2.ps1').read_bytes()
    manifest['source_bindings']['start_oanda_practice_recovery_v2.ps1'] = sha(wrapper)
    manifest['manifest_sha256'] = sha(encoded(manifest))
    pointer_path = ROOT / 'config/operational_dashboard_current.json'
    pointer_raw = pointer_path.read_bytes()
    pointer = json.loads(pointer_raw)
    pointer.update(joint=selected, activated_epoch=time.time())
    files = {manifest['config_path']: config_raw, NEW_MANIFEST: encoded(manifest),
             'start_oanda_practice_recovery_v2.ps1': wrapper,
             'config/operational_dashboard_current.json': encoded(pointer)}
    if any((ROOT / name).exists() for name in (manifest['config_path'], NEW_MANIFEST)):
        raise ValueError('fresh_successor_configuration_required')
    folder = stage / 'practice_handover'
    folder.mkdir()
    for name, raw in files.items():
        path = folder / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('xb') as stream:
            stream.write(raw)
    prior = folder / 'retained_prior'
    prior.mkdir()
    for path in (OLD_CONFIG, prior_manifest, pointer_path, ROOT / 'start_oanda_practice_recovery_v2.ps1'):
        (prior / path.name).write_bytes(path.read_bytes())
    receipt = dict(status='prepared_after_actual_activation_not_installed_or_started',
                   old_trial=old['trial_id'], old_completed_flat=True, old_intent_count=0,
                   trial_id=NEW_TRIAL, start_epoch=new['start_epoch'], stop_epoch=new['stop_epoch'],
                   original_window_unchanged=True, policy_unchanged=new['policy'] == old['policy'],
                   execution_sources_unchanged=new['source_bindings'] == old['source_bindings'],
                   files={name: sha(raw) for name, raw in files.items()},
                   source=selected, broker_io_performed=False, workers_started=False)
    (folder / 'PRACTICE_HANDOVER.json').write_bytes(encoded(receipt))
    return receipt


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-stage', type=Path, required=True)
    print(json.dumps(prepare(parser.parse_args().source_stage)))
