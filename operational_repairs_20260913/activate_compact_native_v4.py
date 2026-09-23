"""Activate the actually installed new empty native cohort, never an old study."""
import hashlib
import json
from pathlib import Path
import sys

root=Path(__file__).resolve().parent.parent/'trad'
sys.path.insert(0,str(root))
import oanda_joint_price_news_forecast_study_v7 as native

registry=root/'config/joint_price_news_operational_v4_20260913.json'
study=root/'data/oanda_training_manager/operational_repair_20260913_v4/joint_price_news_study_v7'
native.activate_fresh_study(config=registry,study=study)
raw=(study/'activation_receipt.json').read_bytes()
receipt=json.loads(raw)
print(json.dumps({'status':receipt['status'],'activation_sha256':hashlib.sha256(raw).hexdigest(),
                  'study':str(study),'scope':'Fresh actual activation, no imported history or orders'}))
