"""Read-only per-pair readiness and observed pip metadata; no forecasts issued."""
import hashlib,json,sys,time
from pathlib import Path
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
sys.path.insert(0,str(ROOT))
from oanda_causal_forecast_inputs import _read_tail,_parse_source
raw=(ROOT/'data/oanda_training_manager/state/practice_007_market_quotes_v1.json').read_bytes()
observed=time.time();quotes=json.loads(raw)
assert quotes['producer']=='practice_007_dedicated_quote_stream'
pairs={p:float(q['pip']) for p,q in quotes['quotes'].items()}
assert len(pairs)==68 and set(pairs.values())<={.0001,.001,.01}
rows=[]
for pair,pip in sorted(pairs.items()):
    try:
        source=_read_tail(ROOT/'data/oanda_training_manager/candles'/f'{pair}_M1.csv')
        prices=_parse_source(source,pair,time.time());reference=max(prices);count=0
        while reference-count*60 in prices:count+=1
        eligible=[t for t in prices if t//60%3==0 and t+3600<=reference
                  and all(t+i*60 in prices for i in range(-60,61))]
        age=time.time()-reference-60
        rows.append(dict(instrument=pair,pip_size=pip,current_bars=count,mature_ridge_rows=len(eligible),
                         bar_age_sec=age,ready=count>=61 and len(eligible)>=24 and 0<=age<=900))
    except Exception as exc:rows.append(dict(instrument=pair,pip_size=pip,ready=False,reason=str(exc)))
result={'observed_epoch':observed,'producer':quotes['producer'],'source_quote_sha256':hashlib.sha256(raw).hexdigest(),
        'pip_sizes':pairs,'rows':rows,'ready_pairs':[row['instrument'] for row in rows if row['ready']]}
(OUT/'QUOTE_METADATA_SNAPSHOT.json').write_bytes(raw)
(OUT/'COVERAGE_PREFLIGHT.json').write_text(json.dumps(result,indent=2),encoding='utf8')
print(json.dumps({'ready_count':len(result['ready_pairs']),'ready_pairs':result['ready_pairs'],
                 'warmup_examples':[row for row in rows if not row['ready']][:8]}))
