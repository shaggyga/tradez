"""Read-only 45-minute watch. Writes only this audit directory."""
import json,math,time,shutil,statistics,hashlib
from pathlib import Path
from datetime import datetime,timezone
ROOT=Path(r"C:\Users\zmoor\Documents\forex\trad")
OUT=Path(__file__).resolve().parent
START=datetime.fromisoformat("2026-09-13T22:20:12+00:00").timestamp()
END=START+45*60
PATHS={
"quotes":"state/practice_007_market_quotes_v1.json",
"quote_stream":"state/practice_007_quote_stream_heartbeat_v1.json",
"clock":"state/clock_integrity_v1.json",
"m1_heartbeat":"state/all68_m1_forward_update_heartbeat_v1.json",
"m1_report":"state/all68_m1_forward_update_v1.json",
"features":"state/research_feature_observations_heartbeat_v1.json",
"price":"pair_local_forecast_study_v2/heartbeat.json",
"news":"local_news_sentiment/pair_sentiment_latest.json",
"news_collector":"local_news_sentiment/collector_heartbeat_v1.json",
"news_repair":"local_news_sentiment_repair_v1/heartbeat.json",
"joint":"joint_price_news_study_v3/heartbeat.json",
"forward":"feature_forward_v2/feature_forward_status_v1.json",
"account":"state/account_007_dashboard_v1.json",
"market_summary":"market_sentiment_ticker/LATEST.json",
}
BASE=ROOT/"data/oanda_training_manager"
MAJORS=["EUR_USD","GBP_USD","USD_JPY","USD_CAD","AUD_USD","NZD_USD","USD_CHF"]
def stamp(v):
    try:
        if isinstance(v,(int,float)): return float(v)
        return datetime.fromisoformat(v.replace("Z","+00:00")).timestamp()
    except (TypeError,ValueError,AttributeError): return None
def age(j,now,*keys):
    for key in keys:
        t=stamp(j.get(key))
        if t is not None:return round(now-t,2)
    return None
def read(key):
    p=BASE/PATHS[key]
    for n in range(3):
        try:return json.loads(p.read_bytes())
        except (OSError,ValueError) as exc:
            if n==2:return {"read_error":type(exc).__name__}
            time.sleep(.05)
def fields(j,keys):return {k:j.get(k) for k in keys}
baseline={}
samples=[]
first=None
def collect():
    now=time.time()
    data={k:read(k) for k in PATHS}
    q=data["quotes"];qr=[]
    for pair,r in q.get("quotes",{}).items():
        try:
            bid,ask,pip=float(r["bid"]),float(r["ask"]),float(r["pip"])
            qa=now-stamp(r["time"]);mid=(bid+ask)/2
            valid=0<bid<=ask and r.get("tradeable") is True and 0<=qa<=30
            if valid:baseline.setdefault(pair,mid)
            qr.append(dict(pair=pair,mid=mid,bid=bid,ask=ask,market_time=r["time"],age_sec=round(qa,2),fresh=valid,
                tradeable=r.get("tradeable"),spread_pips=round((ask-bid)/pip,3),spread_bps=round((ask-bid)/mid*1e4,4),
                change_since_first_fresh_bps=round((mid/baseline[pair]-1)*1e4,4) if pair in baseline and valid else None))
        except (ValueError,KeyError,TypeError,ZeroDivisionError):qr.append({"pair":pair,"invalid":True,"fresh":False})
    feat=data["features"];s=feat.get("last_success") or {};cov=s.get("coverage") or {};ready=cov.get("family_readiness") or {}
    fh=data["forward"];ev=fh.get("evaluation") or {}
    observed={
    "sample_utc":datetime.fromtimestamp(now,timezone.utc).isoformat(),"elapsed_sec":round(now-START,1),"remaining_sec":round(max(0,END-now),1),
    "read_errors":{k:v["read_error"] for k,v in data.items() if "read_error" in v},
    "quotes":{"snapshot_age_sec":age(q,now,"generated_utc"),"fresh_tradeable":sum(r["fresh"] for r in qr),"total":len(qr),"nontradeable":sum(r.get("tradeable") is False for r in qr),"median_fresh_spread_bps":statistics.median([r["spread_bps"] for r in qr if r["fresh"]]) if any(r["fresh"] for r in qr) else None,"pairs":qr},
    "quote_stream":fields(data["quote_stream"],["status","phase","pid","progress_age_sec"])|{"age_sec":age(data["quote_stream"],now,"updated_at")},
    "clock":fields(data["clock"],["status","source_fresh","host_clock_synchronized","clock_discontinuity_active"])|{"age_sec":age(data["clock"],now,"generated_utc")},
    "m1":{"heartbeat_age_sec":age(data["m1_heartbeat"],now,"updated_at"),"phase":data["m1_heartbeat"].get("phase"),"progress_age_sec":data["m1_heartbeat"].get("progress_age_sec"),"report_age_sec":age(data["m1_report"],now,"generated_utc"),"report":fields(data["m1_report"],["pair_count","error_count","total_rows_appended","gap_recovery_cycle"])},
    "features":{"heartbeat_age_sec":age(feat,now,"updated_at"),"last_success_age_sec":age(s,now,"last_publication_completed_utc"),"phase":feat.get("phase"),"result":feat.get("result",{}).get("status"),"error":feat.get("result",{}).get("reason"),"archive":s.get("archive"),"accepted_quotes":cov.get("accepted_instrument_count"),"readiness":{k:v for k,v in ready.items() if k!="native_M1_age_sec_by_pair"}},
    "price":fields(data["price"],["phase","counts","pairs_with_forecast","errors","last_error","can_place_orders"])|{"age_sec":age(data["price"],now,"generated_epoch")},
    "news":{"age_sec":age(data["news"],now,"generated_utc"),"coverage":data["news"].get("coverage"),"active_article_count":data["news"].get("active_article_count"),"collector":fields(data["news_collector"],["status","phase","phase_age_sec","progress_age_sec"])|{"age_sec":age(data["news_collector"],now,"heartbeat_utc","generated_utc")}},
    "news_repair":fields(data["news_repair"],["status","source_status","errors","last_error"])|{"age_sec":age(data["news_repair"],now,"generated_utc")},
    "joint":fields(data["joint"],["phase","counts","errors","last_error"])|{"age_sec":age(data["joint"],now,"generated_epoch")},
    "forward":fields(fh,["status","errors","can_place_orders"])|{"age_sec":age(fh,now,"generated_utc"),"counts":ev.get("counts"),"feature_event_counts":ev.get("feature_event_counts")},
    "account":fields(data["account"].get("aggregate") or {},["nav","balance","pl","unrealizedPL","openTradeCount","pendingOrderCount","positions_current","orders_current","snapshot_state","current_errors"])|{"age_sec":age(data["account"],now,"time"),"environment":data["account"].get("environment")},
    "market_summary_age_sec":age(data["market_summary"],now,"generated_utc","generated_epoch"),
    "disk_free_gib":round(shutil.disk_usage(ROOT).free/1024**3,3)
    }
    if len(samples)%4==0:
        (OUT/"latest_price_summary.json").write_bytes((BASE/"pair_local_forecast_study_v2/summary.json").read_bytes())
    return observed
def compact(s):
    f=s["features"]["readiness"]
    return {"utc":s["sample_utc"],"min":round(s["elapsed_sec"]/60,1),"quotes":s["quotes"]["fresh_tradeable"],"feature_age":s["features"]["last_success_age_sec"],"rich_fresh":f.get("rich_M1_fresh_pairs"),"structural":f.get("structural_fresh_pairs_by_timeframe"),"forecast_pairs":s["price"]["pairs_with_forecast"],"news_directional":(s["news"]["coverage"] or {}).get("directional_pair_count"),"news_errors":s["news_repair"]["errors"],"forward_counts":s["forward"]["counts"],"positions":s["account"]["openTradeCount"],"nav":s["account"]["nav"],"read_errors":s["read_errors"],"free_gib":s["disk_free_gib"]}
(OUT/"watch_contract.json").write_text(json.dumps({"started_utc":datetime.fromtimestamp(START,timezone.utc).isoformat(),"deadline_utc":datetime.fromtimestamp(END,timezone.utc).isoformat(),"cadence_sec":15,"mode":"read_only_live_watch_no_schedule_no_execution_changes"},indent=2))
while True:
    loop=time.monotonic()
    try:
        s=collect()
        samples.append(s)
        with (OUT/"samples.jsonl").open("a",encoding="utf-8") as f:f.write(json.dumps(s,separators=(",",":"))+"\n")
        (OUT/"latest.json").write_text(json.dumps(s,indent=2),encoding="utf-8")
        if first is None:
            first=s;(OUT/"first.json").write_text(json.dumps(s,indent=2),encoding="utf-8")
        if len(samples)%4==1 or time.time()>=END:print(json.dumps(compact(s)),flush=True)
    except Exception as exc:print(json.dumps({"sampler_error":type(exc).__name__+":"+str(exc)[:180]}),flush=True)
    if time.time()>=END:break
    time.sleep(min(max(.1,15-(time.monotonic()-loop)),max(.1,END-time.time())))
(OUT/"completed.json").write_text(json.dumps({"completed_utc":datetime.now(timezone.utc).isoformat(),"samples":len(samples),"actual_first_sample_utc":first["sample_utc"] if first else None,"watch_requested_start_utc":datetime.fromtimestamp(START,timezone.utc).isoformat(),"last":compact(samples[-1]) if samples else None},indent=2),encoding="utf-8")
print("WATCH_COMPLETED",flush=True)

