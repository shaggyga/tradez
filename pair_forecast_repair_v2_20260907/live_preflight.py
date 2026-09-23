"""Disposable research wiring check using actual observed local inputs/quotes."""
from pathlib import Path
import json
import sys
import time
from prepare_registry import make_registry,worker,ROOT

OUT=Path(__file__).resolve().parent/'live_preflight'
if OUT.exists():raise SystemExit('Refuse to reuse preflight database directory')
OUT.mkdir()
registry=make_registry('preflight_live_wiring',{'EUR_USD','USD_THB','USD_JPY','HKD_JPY'})
worker.atomic_json(OUT/'registry.json',registry);worker.load_registry(OUT/'registry.json')
runner=worker.PairRunner(registry,OUT,ROOT/'data/oanda_training_manager/candles',
    ROOT/'data/oanda_training_manager/state/practice_007_market_quotes_v1.json',activate=True)
started=time.time();last=0
try:
    while time.time()-started<90:
        runner.tick()
        if time.time()-last>=20:
            counts={pair:{family:slot['ledger'].counts() for family,slot in state['families'].items()} for pair,state in runner.states.items()}
            print(json.dumps({'elapsed':time.time()-started,'errors':runner.errors,'last_error':runner.last_error,'counts':counts}),flush=True);last=time.time()
        time.sleep(.2)
    runner.publish_status(force=True)
    counts={pair:{family:slot['ledger'].counts() for family,slot in state['families'].items()} for pair,state in runner.states.items()}
    result={'started_epoch':started,'ended_epoch':time.time(),'errors':runner.errors,'last_error':runner.last_error,
        'counts':counts,'registry_sha256':worker.digest(registry),'scope':'Disposable wiring observation; never imported into activated primary performance.'}
    worker.atomic_json(OUT/'result.json',result);print(json.dumps(result),flush=True)
finally:runner.close()
