#!/usr/bin/env python3
"""Evaluate transparent source-native rules on the historical discovery replay."""

from __future__ import annotations

import argparse
import json
import math
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping

ROOT=Path(__file__).resolve().parent
INPUT=ROOT/"data"/"oanda_training_manager"/"reports"/"direct_source_response"/"DIRECT_SOURCE_HISTORICAL_REPLAY_20260808.json"
CONFIG=ROOT/"config"/"direct_source_simple_rules_v1.json"
OUTPUT=ROOT/"data"/"oanda_training_manager"/"reports"/"direct_source_response"/"DIRECT_SOURCE_SIMPLE_RULES_20260808.json"
REPORT=ROOT/"data"/"oanda_training_manager"/"reports"/"direct_source_response"/"DIRECT_SOURCE_SIMPLE_RULES_20260808.md"


def read_json(path:Path)->dict[str,Any]:
    try:value=json.loads(path.read_text(encoding="utf-8"))
    except (OSError,json.JSONDecodeError):return {}
    return value if isinstance(value,dict) else {}


def atomic(path:Path,value:str)->None:
    path.parent.mkdir(parents=True,exist_ok=True);temporary=path.with_suffix(path.suffix+f".{os.getpid()}.tmp");temporary.write_text(value,encoding="utf-8");os.replace(temporary,path)


def source_rule(event:Mapping[str,Any],config:Mapping[str,Any])->dict[str,Any]:
    series=str(event.get("event_series_id") or "")
    policy=(config.get("series") or {}).get(series)
    if not isinstance(policy,Mapping):
        return {"direction":"abstain","reason":(config.get("excluded_series") or {}).get(series,"series_not_allowlisted"),"basis":"none"}
    polarity=float(policy.get("polarity") or 0)
    consensus=event.get("consensus_value");actual=event.get("actual_value")
    try:actual=float(actual)
    except (TypeError,ValueError):return {"direction":"abstain","reason":"actual_missing","basis":"none"}
    if consensus is not None:
        try:delta=actual-float(consensus);basis="causal_consensus_surprise"
        except (TypeError,ValueError):delta=None
    elif bool(policy.get("actual_is_period_change")):
        delta=actual;basis="source_native_period_change"
    else:
        previous=event.get("revised_previous_value")
        if previous is None:
            previous=event.get("previous_value")
        try:delta=actual-float(previous);basis="change_from_previous_proxy"
        except (TypeError,ValueError):delta=None
    if delta is None or not math.isfinite(delta) or abs(delta)<1e-12 or not polarity:
        return {"direction":"abstain","reason":"missing_or_zero_comparable_change","basis":"none"}
    signed=polarity*delta
    return {"direction":"strengthen" if signed>0 else "weaken","reason":"allowlisted_numeric_rule",
            "basis":basis,"raw_delta":delta,"polarity":polarity,"signed_impulse":signed,"meaning":policy.get("meaning")}


def metrics(rows:list[dict[str,Any]],key:str)->dict[str,Any]:
    values=[float(row[key]) for row in rows]
    wins=[value>0 for value in values];gross=sum(max(0,value) for value in values);loss=-sum(min(0,value) for value in values)
    return {"n":len(values),"wins":sum(wins),"win_rate":sum(wins)/len(values) if values else None,
            "average_net_pips":sum(values)/len(values) if values else None,"total_net_pips":sum(values),
            "profit_factor":gross/loss if loss>0 else None,"minimum_net_pips":min(values) if values else None,
            "maximum_net_pips":max(values) if values else None,"proof_eligible":False}


def evaluate(replay:Mapping[str,Any],config:Mapping[str,Any])->dict[str,Any]:
    grouped=defaultdict(list)
    for row in replay.get("details") or []:
        if int(row.get("horizon_sec") or 0) not in set(config.get("preferred_horizons_sec") or []):continue
        # Several indicators can share one release clock and one currency
        # factor.  They are one decision opportunity, not independent trades.
        grouped[(row.get("currency"),row.get("source_time_utc"),int(row.get("horizon_sec")))].append(row)
    evaluated=[];abstentions=[];magnitude=[]
    for (currency,source_time,horizon),group in sorted(grouped.items()):
        release_rows={}
        for row in group:
            release_rows.setdefault(str(row.get("release_key") or ""),row)
        release_keys=sorted(release_rows)
        bundle_id=f"{currency}|{source_time}|{horizon}"
        chosen=min(group,key=lambda row:(float(row.get("entry_spread_pips") or 1e9),str(row.get("instrument"))))
        if float(chosen.get("entry_spread_pips") or 1e9)>float(config.get("maximum_entry_spread_pips") or 3):
            abstentions.append({"episode_id":bundle_id,"release_keys":release_keys,"horizon_sec":horizon,"reason":"spread_gate"});continue
        absolute_move=chosen.get("absolute_move_pips")
        if absolute_move is None:
            absolute_move=abs(float(chosen.get("currency_return_pips") or 0))
        oracle_best=chosen.get("best_after_cost_pips")
        if oracle_best is None:
            oracle_best=max(float(chosen.get("strengthening_after_cost_pips") or 0),float(chosen.get("weakening_after_cost_pips") or 0))
        movement_cleared=chosen.get("movement_cleared_cost")
        if movement_cleared is None:
            movement_cleared=float(oracle_best)>0
        magnitude.append({
            "episode_id":bundle_id,"release_keys":release_keys,
            "event_series_ids":sorted({str(row.get("event_series_id") or "") for row in release_rows.values()}),
            "currency":currency,"source_time_utc":source_time,"horizon_sec":horizon,
            "instrument":chosen.get("instrument"),"entry_spread_pips":chosen.get("entry_spread_pips"),
            "absolute_move_pips":absolute_move,
            "movement_cleared_cost":bool(movement_cleared),
            "oracle_best_after_cost_pips":oracle_best,
        })
        component_rules=[(row,source_rule(row,config)) for row in release_rows.values()]
        actionable=[(row,rule) for row,rule in component_rules if rule["direction"]!="abstain"]
        directions={rule["direction"] for _row,rule in actionable}
        if not actionable:
            abstentions.append({"episode_id":bundle_id,"release_keys":release_keys,"horizon_sec":horizon,
                                "reason":"no_allowlisted_component_rule",
                                "component_reasons":[rule["reason"] for _row,rule in component_rules]});continue
        if len(directions)!=1:
            abstentions.append({"episode_id":bundle_id,"release_keys":release_keys,"horizon_sec":horizon,
                                "reason":"same_clock_rule_conflict","component_directions":sorted(directions)});continue
        rule=actionable[0][1]
        net=float(chosen["strengthening_after_cost_pips"] if rule["direction"]=="strengthen" else chosen["weakening_after_cost_pips"])
        flipped=float(chosen["weakening_after_cost_pips"] if rule["direction"]=="strengthen" else chosen["strengthening_after_cost_pips"])
        signed_move=float(chosen["currency_return_pips"])*(1 if rule["direction"]=="strengthen" else -1)
        evaluated.append({"episode_id":bundle_id,"release_keys":release_keys,
                          "event_series_ids":sorted({str(row.get("event_series_id") or "") for row,_rule in actionable}),
                          "event_names":sorted({str(row.get("event_name") or "") for row,_rule in actionable}),
                          "currency":currency,"source_time_utc":source_time,"horizon_sec":horizon,"instrument":chosen.get("instrument"),
                          "entry_spread_pips":chosen.get("entry_spread_pips"),"predicted_currency_direction":rule["direction"],
                          "rule_basis":rule["basis"],"component_rules":[component for _row,component in actionable],
                          "rule_after_cost_pips":net,"flipped_after_cost_pips":flipped,
                          "direction_hit":signed_move>0,"rule":rule})
    by_horizon={}
    for horizon in config.get("preferred_horizons_sec") or []:
        rows=[row for row in evaluated if row["horizon_sec"]==horizon]
        mag=[row for row in magnitude if row["horizon_sec"]==horizon]
        by_horizon[str(horizon)]={
            "magnitude_opportunity": {
                "n":len(mag),
                "cost_clear_rate":sum(bool(row["movement_cleared_cost"]) for row in mag)/len(mag) if mag else None,
                "mean_absolute_move_pips":sum(float(row["absolute_move_pips"]) for row in mag)/len(mag) if mag else None,
                "mean_oracle_best_after_cost_pips":sum(float(row["oracle_best_after_cost_pips"]) for row in mag)/len(mag) if mag else None,
                "directional_action":"abstain",
                "proof_eligible":False,
            },
            "direction_rule":metrics(rows,"rule_after_cost_pips"),
            "flipped_negative_control":metrics(rows,"flipped_after_cost_pips"),
        }
    return {"magnitude_observations":magnitude,"evaluated":evaluated,"abstentions":abstentions,"by_horizon":by_horizon}


def run(input_path:Path=INPUT,config_path:Path=CONFIG,output:Path=OUTPUT,report:Path=REPORT)->dict[str,Any]:
    replay=read_json(input_path);config=read_json(config_path);result=evaluate(replay,config)
    payload={"schema_version":1,"rule_contract_id":config.get("rule_contract_id"),"research_only":True,"execution_eligible":False,
             "evidence_class":"same-window_historical_discovery","proof_eligible":False,
             "limitations":["small sample","rules declared after inspecting source inventory","previous is not consensus","no rates confirmation","no untouched holdout"],**result}
    atomic(output,json.dumps(payload,indent=2,sort_keys=True))
    lines=["# Direct-Source Simple Rules","","Historical discovery only; cannot promote or execute.","",
           f"- Evaluated decisions: **{len(result['evaluated'])}**",f"- Abstentions: **{len(result['abstentions'])}**","",
           "| Horizon | Opportunity N | Cost clear | Mean abs move | Direction N | Win rate | Avg net | Flipped avg |","|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for horizon,summary in result["by_horizon"].items():
        mag=summary["magnitude_opportunity"];rule=summary["direction_rule"];flip=summary["flipped_negative_control"]
        lines.append(f"| {horizon} | {mag['n']} | {mag['cost_clear_rate']:.1%} | {mag['mean_absolute_move_pips']:.3f} | {rule['n']} | {rule['win_rate']:.1%} | {rule['average_net_pips']:.3f} | {flip['average_net_pips']:.3f} |")
    lines += ["","The magnitude-only arm predicts a 5–15 minute opportunity window but abstains on side. The directional arm above is intentionally aggressive and remains shadow-only.",""]
    lines[-2]="The magnitude-only arm predicts a 5-15 minute opportunity window but abstains on side. The directional arm above is intentionally aggressive and remains shadow-only."
    atomic(report,"\n".join(lines));return payload


def main()->int:
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--input",type=Path,default=INPUT);parser.add_argument("--config",type=Path,default=CONFIG);parser.add_argument("--output",type=Path,default=OUTPUT);parser.add_argument("--report",type=Path,default=REPORT);args=parser.parse_args();run(args.input,args.config,args.output,args.report);return 0


if __name__=="__main__":raise SystemExit(main())
