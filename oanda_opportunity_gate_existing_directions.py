#!/usr/bin/env python3
"""Test the frozen opportunity model as a veto on existing signal directions."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

import joblib
import numpy as np

import oanda_68_pair_opportunity_census as source
import oanda_executable_opportunity_ranking as ranking
import oanda_executable_opportunity_feature_contract as feature_contract

ROOT=Path(__file__).resolve().parent
EDGE=ROOT/"data"/"oanda_training_manager"/"state"/"edge_evidence_v1.sqlite"
OUTPUT=ROOT/"data"/"oanda_training_manager"/"reports"/"executable_opportunity_ranking"/"OPPORTUNITY_GATE_EXISTING_DIRECTIONS_20260808.json"
REPORT=ROOT/"data"/"oanda_training_manager"/"reports"/"executable_opportunity_ranking"/"OPPORTUNITY_GATE_EXISTING_DIRECTIONS_20260808.md"


def atomic(path:Path,value:str)->None:
    path.parent.mkdir(parents=True,exist_ok=True);tmp=path.with_suffix(path.suffix+f".{os.getpid()}.tmp");tmp.write_text(value,encoding="utf-8");os.replace(tmp,path)


def parse_epoch(value:str)->int:
    parsed=dt.datetime.fromisoformat(value.replace("Z","+00:00"));return int(parsed.timestamp())//60*60


def summarize(values:list[float])->dict[str,Any]:
    gross=sum(max(value,0) for value in values);loss=-sum(min(value,0) for value in values);best=max(values) if values else None
    return {"n":len(values),"wins":sum(value>0 for value in values),"win_rate":sum(value>0 for value in values)/len(values) if values else None,
            "average_net_pips":sum(values)/len(values) if values else None,"total_net_pips":sum(values),"profit_factor":gross/loss if loss else None,
            "average_without_best_pips":(sum(values)-best)/(len(values)-1) if len(values)>1 else None,"proof_eligible":False}


def deduplicate(rows:list[tuple[Any,...]])->list[dict[str,Any]]:
    grouped=defaultdict(list)
    for family,instrument,direction,entry_time,net in rows:
        epoch=parse_epoch(str(entry_time));grouped[(str(family),str(instrument),str(direction),epoch)].append(float(net))
    return [{"family":key[0],"instrument":key[1],"direction":key[2],"epoch":key[3],"net_pips":sum(values)/len(values)} for key,values in grouped.items()]


def run(edge:Path=EDGE,output:Path=OUTPUT,report:Path=REPORT)->dict[str,Any]:
    config=ranking.read_json(ranking.CONFIG);source._PIP_SIZES=source.load_pip_sizes(ranking.QUOTES);by_pair,_=source.load_minutes(ranking.DATABASE);models=joblib.load(ranking.ARTIFACT/"models.joblib")
    all_results=[]
    db=sqlite3.connect(f"file:{edge.as_posix()}?mode=ro",uri=True,timeout=30)
    for horizon in config.get("horizons_sec") or []:
        horizon=int(horizon);bundle=models[str(horizon)];start=int(bundle["cutoffs"]["validation_start_epoch"])
        frame=feature_contract.labeled_frame(
            by_pair,horizon,float(config["modeled_slippage_pips"]),
            float(config["maximum_entry_spread_pips"]),aligned_only=False
        )
        frame=frame[frame["epoch"]>=start].copy();frame["clear_probability"]=ranking.probability(bundle["clear_model"],bundle["clear_calibrator"],frame[ranking.FEATURES]);frame["predicted_magnitude_pips"]=np.maximum(0,bundle["magnitude_model"].predict(frame[ranking.FEATURES]))
        scores={(str(row.instrument),int(row.epoch)):(float(row.clear_probability),float(row.predicted_magnitude_pips),float(row.entry_cost_pips)) for row in frame.itertuples()}
        start_iso=dt.datetime.fromtimestamp(start,dt.timezone.utc).isoformat()
        raw=db.execute("""SELECT family,instrument,direction,entry_time,modeled_after_cost_pips
                          FROM canonical_economic_outcome_labels WHERE horizon_sec=? AND entry_time>=?""",(horizon,start_iso)).fetchall()
        decisions=deduplicate(raw);matched=[];gated=[]
        for row in decisions:
            score=scores.get((row["instrument"],row["epoch"]))
            if score is None:continue
            matched.append(row);probability,magnitude,cost=score
            if probability>=float(config["minimum_clear_probability"]) and magnitude>=float(config["minimum_predicted_magnitude_cost_ratio"])*cost:
                gated.append({**row,"clear_probability":probability,"predicted_magnitude_pips":magnitude,"entry_cost_pips":cost})
        families=[]
        for family in sorted({row["family"] for row in matched}):
            base=[row["net_pips"] for row in matched if row["family"]==family];selected=[row["net_pips"] for row in gated if row["family"]==family]
            if selected:families.append({"family":family,"baseline":summarize(base),"gated":summarize(selected),"average_lift_pips":summarize(selected)["average_net_pips"]-summarize(base)["average_net_pips"]})
        all_results.append({"horizon_sec":horizon,"source_rows":len(raw),"family_pair_minute_direction_rows":len(decisions),"matched_rows":len(matched),
                            "gated_rows":len(gated),"baseline":summarize([row["net_pips"] for row in matched]),"gated":summarize([row["net_pips"] for row in gated]),
                            "families_with_gated_rows":sorted(families,key=lambda row:(-row["gated"]["n"],row["family"])),"proof_eligible":False})
    db.close()
    payload={"schema_version":1,"cohort_id":"opportunity_gate_existing_directions_engineering_20260808","research_only":True,"execution_eligible":False,
             "evidence_class":"already_inspected_archive_engineering_diagnostic","results":all_results,
             "limitations":["existing signal directions are correlated","archive already inspected","family results are not multiplicity adjusted","no prospective confirmation"]}
    atomic(output,json.dumps(payload,indent=2,sort_keys=True))
    lines=["# Opportunity Gate on Existing Directions","","Engineering diagnostic only; cannot promote or execute.","",
           "| Horizon | Matched N | Baseline avg | Gate N | Gate win | Gate avg | Gate PF | Without best |",
           "|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for result in all_results:
        base=result["baseline"];gate=result["gated"]
        def fmt(value:Any,pattern:str=".3f")->str:return "n/a" if value is None else format(value,pattern)
        lines.append(f"| {result['horizon_sec']}s | {result['matched_rows']} | {fmt(base['average_net_pips'])} | {gate['n']} | {fmt(gate['win_rate'],'.1%')} | {fmt(gate['average_net_pips'])} | {fmt(gate['profit_factor'])} | {fmt(gate['average_without_best_pips'])} |")
    lines += ["","This tests opportunity filtering only. It does not treat correlated family signals as independent evidence.",""]
    atomic(report,"\n".join(lines));return payload


def main()->int:
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--edge",type=Path,default=EDGE);parser.add_argument("--output",type=Path,default=OUTPUT);parser.add_argument("--report",type=Path,default=REPORT);args=parser.parse_args();run(args.edge,args.output,args.report);return 0


if __name__=="__main__":raise SystemExit(main())
