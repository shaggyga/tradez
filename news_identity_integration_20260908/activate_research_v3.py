"""One-time explicit empty research activation; no process or broker operations."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json, sys, time

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent/'trad'
sys.path.insert(0,str(ROOT))
import prepare_joint_price_news_registry_v3 as prepare
import oanda_joint_price_news_forecast_study_v3 as worker

FROZEN={
 'oanda_local_news_sentiment_repair_v1.py':'c2fbad7c290ce1edaa977e99988ab563a2afc6571dc456271e45bde3ca7b2b6b',
 'oanda_news_topic_identity_reconciliation_v1.py':'d34b0c72693497e9be6eadc86bd17f0a2c1c1b79ecf010928faf88a5033a7884',
 'oanda_causal_forecast_inputs_joint_news_v2.py':'ade4fc5eb2aea195d7609964d7b8979ca433a28028e059c65adc1b1b404a277c',
 'oanda_causal_forecast_ledger_joint_news_v2.py':'17782392f8e3994ff83620109141770b1f1f8182d863444ed4aaa6067bf4ef72',
 'oanda_joint_price_news_forecast_study_v3.py':'5dfc29c29fbb61daef4f9f9d5c50f8c029a4d9ed57438288839176e6b55a7f80',
 'prepare_joint_price_news_registry_v3.py':'3cd33aa233607f90a6189782dc377fc075da30f50a1b17534274ff7f0dfc1e57',
}

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()

def preserved_bindings():
    records={}
    for prefix in ('pair_local_forecast','joint_price_news'):
        for version in ('v1','v2'):
            path=ROOT/'config'/f'{prefix}_study_{version}_20260907.json'
            value=json.loads(path.read_bytes())
            bindings=value['source_bindings']
            for name,expected in bindings.items():
                if sha(ROOT/name)!=expected:raise ValueError('prior_registered_source_changed:'+name)
            records[path.name]={'sha256':sha(path),'source_bindings':bindings,'pairs':len(value['pairs'])}
    return records

def main():
    for name,expected in FROZEN.items():
        if sha(ROOT/name)!=expected:raise ValueError('reviewed_source_changed:'+name)
    if worker.DEFAULT_CONFIG.exists() or worker.STUDY.exists():raise ValueError('fresh_config_and_study_required')
    before=preserved_bindings()
    preflight={'schema_version':'news_repair_deployment_source_freeze_20260908',
               'observed_epoch':time.time(),'reviewed_new_sources':FROZEN,'prior_registries':before,
               'source_bindings':prepare.make_registry()['source_bindings']}
    with (HERE/'DEPLOYMENT_SOURCE_FREEZE_20260908.json').open('xb') as f:f.write(worker.encoded(preflight))
    prepared=prepare.prepare_registry(worker.DEFAULT_CONFIG)
    activated=worker.activate_fresh_study()
    registry=worker.load_registry(worker.DEFAULT_CONFIG)
    assert worker.verify_activated_study(registry,worker.STUDY)
    after=preserved_bindings()
    if before!=after:raise ValueError('prior_registration_changed_during_activation')
    result={'schema_version':'news_repair_deployment_activation_20260908','verified_utc':datetime.now(timezone.utc).isoformat(),
            'prepared':prepared,'activation':activated,'prior_registries_unchanged':True,
            'research_only':True,'can_place_orders':False,'can_promote':False,'can_authorize':False,
            'historical_rows_imported':False,'process_started':False}
    with (HERE/'DEPLOYMENT_ACTIVATION_20260908.json').open('xb') as f:f.write(worker.encoded(result))
    print(json.dumps({'status':activated['status'],'ledgers':len(activated['ledgers']),
                      'activated_from_epoch':activated['activation_started_epoch'],
                      'activated_through_epoch':activated['activation_completed_epoch'],
                      'registry_sha256':activated['registry_sha256'],'old_registries_unchanged':True}))

if __name__=='__main__':main()
