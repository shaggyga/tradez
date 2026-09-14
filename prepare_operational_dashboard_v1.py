"""Publish an explicit current producer selection after source verification."""
import hashlib
import json
from pathlib import Path
import time
import oanda_operational_dashboard_selection_v1 as view

ROOT=Path(__file__).resolve().parent


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare():
    value=dict(schema_version=view.SCHEMA,activated_epoch=time.time(),research_only=True,
        can_place_orders=False,can_promote=False,can_authorize=False,
        source_bindings={name:sha(ROOT/name) for name in sorted(view.SOURCE_FILES)},
        features={'archive_path':'data/oanda_training_manager/operational_repair_20260913_v1/feature_observations_v2'})
    for kind,registry,study in (
        ('price','config/pair_local_operational_v2_20260913.json','data/oanda_training_manager/operational_repair_20260913_v1/pair_local_forecast_study_v3'),
        ('joint','config/joint_price_news_operational_v3_20260913.json','data/oanda_training_manager/operational_repair_20260913_v3/joint_price_news_study_v7')):
        value[kind]={'registry_path':registry,'registry_sha256':sha(ROOT/registry),'study_path':study,
                     'activation_sha256':sha(ROOT/study/'activation_receipt.json')}
    path=ROOT/view.POINTER
    with path.open('xb') as handle:handle.write(view.encoded(value))
    checked=view.read_selection(ROOT,time.time())
    return {'status':'selected','path':str(path),'sha256':sha(path),'sources':len(checked['source_bindings'])}


if __name__=='__main__':print(json.dumps(prepare()))
