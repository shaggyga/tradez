#!/usr/bin/env python3
"""Coverage-refresh V2 of the frozen pure-factor response analog contract."""

from __future__ import annotations

import argparse,json,sqlite3
from pathlib import Path
from typing import Any,Iterable,Mapping

from oanda_spike_blurb_factor_ledger import FACTOR_CONTRACT_ID
from oanda_spike_blurb_factor_reconstruction import CANDLES,CONFIG,REPORT_ROOT,atomic_text,canonical_json,file_sha256,load_contract,sha256_bytes,utc_now
from oanda_spike_blurb_factor_response_analogs import (
    HORIZONS_MIN,MINIMUM_PREQUENTIAL_ANALOGS,RESPONSE_COLUMNS,analog_key,
    attach_prequential_mapping,ensure_schema,measure_currency_response,
)
from oanda_spike_blurb_movement_inventory import DATABASE,stable_id
from oanda_spike_blurb_response_entry_replay import PairSeries,load_market
from oanda_spike_blurb_response_entry_replay_v2 import load_fresh_watch_events


CONTRACT_ID="spike_blurb_factor_response_analogs_v2_20260819"
OUTPUT_JSON=REPORT_ROOT/"SPIKE_BLURB_FACTOR_RESPONSE_ANALOGS_V2.json"
OUTPUT_MD=REPORT_ROOT/"SPIKE_BLURB_FACTOR_RESPONSE_ANALOGS_V2.md"


def build_rows_v2(watches:Iterable[Mapping[str,Any]],market:Mapping[str,PairSeries])->list[dict[str,Any]]:
    raw=[]
    for watch in watches:
        baseline=((int(watch["known_epoch"])+59)//60)*60
        for horizon in HORIZONS_MIN:
            target=baseline+horizon*60;response=measure_currency_response(market,str(watch["currency"]),baseline,target)
            if response is None:continue
            raw.append({"response_id":stable_id("factor_response",CONTRACT_ID,watch["factor_id"],horizon),"contract_id":CONTRACT_ID,
                "factor_id":watch["factor_id"],"source_evidence_id":watch["source_evidence_id"],"source_id":watch["source_id"],
                "underlying_event_id":watch["underlying_event_id"],"source_batch_id":watch["source_batch_id"],"analog_key":analog_key(watch),
                "currency":watch["currency"],"factor_type":watch["factor_type"],"relevance_state":watch["relevance_state"],
                "numeric_measurement_state":watch.get("numeric_measurement_state") or "unknown","signed_factor_score":float(watch["signed_factor_score"]),
                "known_utc":watch["known_utc"],"published_utc":watch["published_utc"],"publication_lag_sec":int(watch["publication_lag_sec"]),
                "baseline_epoch":baseline,"target_epoch":target,"horizon_min":horizon,**response,"prior_matured_analog_n":0,
                "prior_alignment_mean":None,"prequential_orientation_sign":0,"prequential_prediction_sign":0,"prequential_correct":None,
                "research_only":1,"execution_eligible":0})
    return attach_prequential_mapping(raw)


def snapshot(database_path:Path,*,watch_count:int|None,exclusions:Mapping[str,int],reused:bool)->dict[str,Any]:
    c=sqlite3.connect(f"file:{database_path.resolve().as_posix()}?mode=ro",uri=True)
    total,factors,keys,currencies=c.execute("select count(*),count(distinct factor_id),count(distinct analog_key),count(distinct currency) from factor_response_observations where contract_id=?",(CONTRACT_ID,)).fetchone()
    metrics=[]
    for r in c.execute("""select horizon_min,count(*),avg(abs(currency_response_bps)),avg(response_breadth),sum(case when prequential_correct is not null then 1 else 0 end),avg(prequential_correct) from factor_response_observations where contract_id=? group by horizon_min order by horizon_min""",(CONTRACT_ID,)):
        metrics.append({"horizon_min":int(r[0]),"n":int(r[1]),"average_absolute_currency_response_bps":float(r[2]),"average_response_breadth":float(r[3]),"prequential_prediction_n":int(r[4] or 0),"prequential_direction_accuracy":float(r[5]) if r[5] is not None else None})
    coverage=dict(c.execute("select factor_type,count(distinct factor_id) from factor_response_observations where contract_id=? group by factor_type order by factor_type",(CONTRACT_ID,)).fetchall())
    integrity=str(c.execute("pragma integrity_check").fetchone()[0]);c.close()
    result={"schema_version":2,"generated_utc":utc_now(),"contract_id":CONTRACT_ID,"factor_contract_id":FACTOR_CONTRACT_ID,
        "supersedes_for_price_coverage_only":"spike_blurb_factor_response_analogs_v1_20260819","fresh_watch_count":watch_count,"watch_exclusions":dict(exclusions),
        "response_row_count":int(total),"factor_count":int(factors),"analog_key_count":int(keys),"currency_count":int(currencies),
        "factor_type_coverage":coverage,"horizon_metrics":metrics,"sqlite_integrity":integrity,"reused_existing_immutable_cohort":reused,
        "discovery_only":True,"execution_eligible":False,"supported_execution_decision":"no_trade"}
    result["snapshot_sha256"]=sha256_bytes(canonical_json(result).encode());return result


def build(config_path:Path=CONFIG,candle_root:Path=CANDLES,database_path:Path=DATABASE)->dict[str,Any]:
    cfg=load_contract(config_path);market=load_market(candle_root,cfg["expected_instruments"])
    manifest=[{"instrument":p.instrument,"sha256":p.price_file_sha256} for p in market.values()];price_sha=sha256_bytes(canonical_json(manifest).encode());builder_sha=file_sha256(Path(__file__).resolve())
    c=sqlite3.connect(database_path,timeout=120);c.execute("pragma journal_mode=WAL");c.execute("pragma synchronous=FULL");c.execute("pragma foreign_keys=ON");ensure_schema(c)
    existing=c.execute("select builder_sha256,price_manifest_sha256 from factor_response_analog_contracts where contract_id=?",(CONTRACT_ID,)).fetchone()
    if existing:
        c.close()
        if tuple(existing)!=(builder_sha,price_sha):raise RuntimeError("immutable_factor_response_v2_contract_collision")
        return snapshot(database_path,watch_count=None,exclusions={},reused=True)
    watches,exclusions=load_fresh_watch_events(c);states=dict(c.execute("select factor_id,numeric_measurement_state from factor_observations where factor_contract_id=?",(FACTOR_CONTRACT_ID,)).fetchall())
    for w in watches:w["numeric_measurement_state"]=states.get(w["factor_id"],"unknown")
    rows=build_rows_v2(watches,market);contract={"contract_id":CONTRACT_ID,"factor_contract_id":FACTOR_CONTRACT_ID,"created_utc":utc_now(),"builder_sha256":builder_sha,"price_manifest_sha256":price_sha,
        "contract_json":canonical_json({"change_from_v1":"price_archive_coverage_refresh_only","horizons_min":HORIZONS_MIN,"minimum_prequential_analogs":MINIMUM_PREQUENTIAL_ANALOGS,"research_only":True,"execution_eligible":False})}
    marks=",".join("?" for _ in RESPONSE_COLUMNS)
    with c:
        c.execute("insert into factor_response_analog_contracts values (?,?,?,?,?,?)",tuple(contract.values()));c.executemany(f"insert into factor_response_observations ({','.join(RESPONSE_COLUMNS)}) values ({marks})",[[row.get(col) for col in RESPONSE_COLUMNS] for row in rows])
    c.close();return snapshot(database_path,watch_count=len(watches),exclusions=exclusions,reused=False)


def render(r:Mapping[str,Any])->str:
    lines=["# Pure-Source Factor/Response Analog Ledger V2","",f"- Generated: `{r['generated_utc']}`",f"- Contract: `{r['contract_id']}`",f"- Factors: **{r['factor_count']:,}**; currencies: **{r['currency_count']:,}**; identities: **{r['analog_key_count']:,}**","","V2 changes only the frozen price-manifest coverage. The mapping, horizons, source filters, minimum prior analog count, and safety disposition remain unchanged from V1.","","| Horizon | N | Mean absolute response | Prequential N | Accuracy |","|---:|---:|---:|---:|---:|"]
    for x in r["horizon_metrics"]:
        acc="—" if x["prequential_direction_accuracy"] is None else f"{100*x['prequential_direction_accuracy']:.1f}%";lines.append(f"| {x['horizon_min']}m | {x['n']:,} | {x['average_absolute_currency_response_bps']:.3f} bps | {x['prequential_prediction_n']:,} | {acc} |")
    lines.extend(["","Discovery-only; no executable pair selection, promotion, or authorization. Supported decision: `no_trade`.",""]);return "\n".join(lines)


def main()->int:
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--config",type=Path,default=CONFIG);p.add_argument("--candles",type=Path,default=CANDLES);p.add_argument("--database",type=Path,default=DATABASE);p.add_argument("--output-json",type=Path,default=OUTPUT_JSON);p.add_argument("--output-md",type=Path,default=OUTPUT_MD);a=p.parse_args();r=build(a.config,a.candles,a.database);atomic_text(a.output_json,json.dumps(r,indent=2,sort_keys=True)+"\n");atomic_text(a.output_md,render(r));print(json.dumps(r,indent=2,sort_keys=True));return 0


if __name__=="__main__":raise SystemExit(main())
