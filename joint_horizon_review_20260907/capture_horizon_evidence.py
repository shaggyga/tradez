"""Bounded read-only source/API horizon inventory; no account identifiers."""
import hashlib,json,time,urllib.request
from datetime import datetime,timezone
from pathlib import Path

out=Path(__file__).parent
project=out.parent/'trad'
started=time.time()
with urllib.request.urlopen('http://127.0.0.1:8765/api/main',timeout=30) as response:
    raw=response.read(24*1024*1024+1)
assert len(raw)<=24*1024*1024
data=json.loads(raw)
observed=time.time()
joint=data.get('joint_price_news_forecasts') or {}
price=data.get('pair_local_forecasts') or {}
matrix=data.get('horizon_signal_matrix') or {}
movers=data.get('live_movers') or {}
def coverage(source):
    arms=[a for row in source.get('rows',[]) for a in row.get('active_forecasts',[])]
    return {**{key:source.get(key) for key in ('study_version','status','reason','generated_epoch','observed_epoch','summary_sha256','counts')},
            'active_arms':len(arms),'published_horizons_sec':sorted({a.get('horizon_sec') for a in arms if a.get('horizon_sec') is not None}),
            'target_reference_offsets_sec':sorted({a['target_epoch']-a['reference_epoch'] for a in arms})}
sources=['oanda_main_signal_dashboard.html','oanda_practice_live_dashboard.py','oanda_second_forecast.py','oanda_signal_contribution_feed.py','oanda_joint_price_news_models_v1.py','docs/SIGNAL_ENGINE_REFRESH_20260721.md']
result={'schema':'joint_horizon_readonly_inventory_v1_20260907','started_epoch':started,'observed_epoch':observed,
        'observed_utc':datetime.fromtimestamp(observed,timezone.utc).isoformat(),'raw_api_ingest_sha256':hashlib.sha256(raw).hexdigest(),
        'joint':coverage(joint),'price_only':coverage(price),
        'legacy_horizon_matrix':{**{key:matrix.get(key) for key in ('generated_utc','snapshot_age_sec','status','counts')},
          'rows':[{key:row.get(key) for key in ('instrument','horizon_sec','horizon_label','state','reason','projected_net_pips','confidence','target_market_open')} for row in matrix.get('rows',[])[:32]]},
        'observed_move_census':{**{key:movers.get(key) for key in ('status','snapshot_age_sec','generated_utc','snapshot_utc')},
          'mode_row_counts':{key:len(rows) for key,rows in (movers.get('modes') or {}).items() if isinstance(rows,list)}},
        'source_sha256':{name:hashlib.sha256((project/name).read_bytes()).hexdigest() for name in sources},
        'scope':'Dated API/source inventory only; old horizon matrix is not treated as verified current joint forecast evidence. Raw full API hash is ingestion provenance only; private account fields are intentionally not retained.'}
(out/'HORIZON_SOURCE_API_INVENTORY_20260907.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps(result,indent=2))
