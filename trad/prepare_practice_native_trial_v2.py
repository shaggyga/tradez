"""Seal one authorized finite practice trial and perform GET-only preflight."""
import hashlib
import json
from pathlib import Path
import time

import oanda_practice_trial_runner_v2 as runner

ROOT=Path(__file__).resolve().parent
CONFIG=ROOT/'config/practice007_native_v7_20260913_v1.json'


def prepare():
    receipt=json.loads((ROOT.parent/'current_verdict_20260913/practice_successor_implementation_receipt.json').read_bytes())
    bindings=receipt['source_bindings']
    if any(hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=sha for name,sha in bindings.items()):
        raise ValueError('reviewed_execution_source_changed')
    registry=ROOT/'config/joint_price_news_operational_v3_20260913.json'
    study=ROOT/'data/oanda_training_manager/operational_repair_20260913_v3/joint_price_news_study_v7'
    source={'registry_path':registry.relative_to(ROOT).as_posix(),'registry_sha256':hashlib.sha256(registry.read_bytes()).hexdigest(),
            'study_path':study.relative_to(ROOT).as_posix(),'activation_sha256':hashlib.sha256((study/'activation_receipt.json').read_bytes()).hexdigest()}
    start=time.time()
    config=dict(schema_version=runner.SCHEMA,trial_id='practice007_native_v7_20260913_v1',environment='practice',
        account_label='practice_007',account_sha256='6879dca9af3472e4c94ab81a460982068a6efce6dc25ef851ea4bd9bb9fbdad2',
        user_authorized_practice_trial=True,start_epoch=start,stop_epoch=start+48*3600,policy=runner.POLICY,
        strategy='verified_native_joint_v7_original_h1',enabled=True,source_bindings=bindings,forecast_source=source,
        scope='Finite separate OANDA practice experiment under prior user authorization. No real-money authority or profitability acceptance; no synthetic signal. Preserve original H1 target and conservative limits.')
    config['config_sha256']=runner.digest(config)
    with CONFIG.open('xb') as handle:handle.write(runner.encoded(config))
    return runner.validate_config(CONFIG,require_enabled=True)


def get_preflight(config):
    broker=runner.PracticeTrialBroker.from_credentials(ROOT/'creds',expected_account_sha256=config['account_sha256'],trial_id=config['trial_id'])
    try:result=runner.preflight(broker,config)
    finally:broker.close()
    state=ROOT/'data/oanda_training_manager'/config['trial_id']
    state.mkdir(parents=True,exist_ok=True)
    runner.atomic_json(state/'preflight.json',result)
    return result


if __name__=='__main__':
    config=prepare()
    result=get_preflight(config)
    print(json.dumps({k:result[k] for k in ('mode','ready','environment','flat','candidate_count','forecast_status','forecast_rejections','processes')}))
