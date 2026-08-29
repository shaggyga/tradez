#!/usr/bin/env python3
"""Two-stage cost-clearance, magnitude, and conditional-direction ranker.

This is a research-only engineering cohort.  It learns from pre-entry quote
state, predicts movement opportunity before side, and evaluates top-one versus
an exactly-three currency-disjoint basket.  It cannot authorize or trade.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, mean_absolute_error, roc_auc_score

import oanda_68_pair_opportunity_census as source

ROOT=Path(__file__).resolve().parent
CONFIG=ROOT/"config"/"executable_opportunity_ranking_v1.json"
DATABASE=ROOT/"data"/"oanda_training_manager"/"state"/"practice_007_quote_intensity_shadow_v1.sqlite"
QUOTES=ROOT/"data"/"oanda_training_manager"/"state"/"practice_007_market_quotes_v1.json"
ARTIFACT=ROOT/"data"/"oanda_training_manager"/"model_space"/"proof_cohorts"/"executable_opportunity_ranking_v1"
OUTPUT=ROOT/"data"/"oanda_training_manager"/"reports"/"executable_opportunity_ranking"/"EXECUTABLE_OPPORTUNITY_RANKING_20260808.json"
REPORT=ROOT/"data"/"oanda_training_manager"/"reports"/"executable_opportunity_ranking"/"EXECUTABLE_OPPORTUNITY_RANKING_20260808.md"

FEATURES=[
    "spread_pips","entry_cost_pips","log_updates","imbalance_5s","imbalance_30s","imbalance_120s",
    "return_1m_pips","return_5m_pips","return_15m_pips","return_30m_pips",
    "abs_return_1m_pips","abs_return_5m_pips","abs_return_15m_pips","abs_return_30m_pips",
    "momentum_cost_ratio","spread_rank","movement_rank","updates_rank",
    "currency_factor_5m_pips","pair_residual_5m_pips","hour_sin","hour_cos",
]


def read_json(path:Path)->dict[str,Any]:
    try:value=json.loads(path.read_text(encoding="utf-8"))
    except (OSError,json.JSONDecodeError):return {}
    return value if isinstance(value,dict) else {}


def atomic(path:Path,value:str)->None:
    path.parent.mkdir(parents=True,exist_ok=True);tmp=path.with_suffix(path.suffix+f".{os.getpid()}.tmp");tmp.write_text(value,encoding="utf-8");os.replace(tmp,path)


def sha(path:Path)->str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_frame(by_pair:Mapping[str,Mapping[int,Mapping[str,float]]],horizon:int,slippage:float,maximum_spread:float)->pd.DataFrame:
    rows=[]
    for instrument,timeline in by_pair.items():
        pip=source.pip_size(instrument)
        for epoch,current in timeline.items():
            if epoch%horizon:continue
            past={seconds:timeline.get(epoch-seconds) for seconds in (60,300,900,1800)}
            future=timeline.get(epoch+horizon)
            if future is None or any(value is None for value in past.values()):continue
            spread=float(current["spread"])
            if spread<=0 or spread>maximum_spread:continue
            returns={seconds:(float(current["mid"])-float(value["mid"]))/pip for seconds,value in past.items()}
            window=source.modeled_window(instrument,current,future,slippage)
            hour=dt.datetime.fromtimestamp(epoch,dt.timezone.utc).hour+dt.datetime.fromtimestamp(epoch,dt.timezone.utc).minute/60
            entry_cost=spread+slippage
            rows.append({
                "epoch":epoch,"instrument":instrument,"base":instrument.split("_")[0],"quote":instrument.split("_")[1],
                "spread_pips":spread,"entry_cost_pips":entry_cost,"log_updates":math.log1p(float(current["updates"])),
                "imbalance_5s":float(current["imbalance_5s"]),"imbalance_30s":float(current["imbalance_30s"]),
                "imbalance_120s":float(current["imbalance_120s"]),
                "return_1m_pips":returns[60],"return_5m_pips":returns[300],"return_15m_pips":returns[900],"return_30m_pips":returns[1800],
                "abs_return_1m_pips":abs(returns[60]),"abs_return_5m_pips":abs(returns[300]),
                "abs_return_15m_pips":abs(returns[900]),"abs_return_30m_pips":abs(returns[1800]),
                "momentum_cost_ratio":(0.65*abs(returns[300])+0.35*abs(returns[900]))/entry_cost,
                "hour_sin":math.sin(2*math.pi*hour/24),"hour_cos":math.cos(2*math.pi*hour/24),
                "future_move_pips":float(window["signed_move_pips"]),"future_magnitude_pips":float(window["absolute_move_pips"]),
                "actual_cost_pips":float(window["modeled_cost_pips"]),"cost_clear":int(bool(window["movement_cleared_cost"])),
                "future_up":int(float(window["signed_move_pips"])>0),
            })
    frame=pd.DataFrame(rows)
    if frame.empty:return frame
    frame["spread_rank"]=frame.groupby("epoch")["spread_pips"].rank(pct=True,ascending=True)
    frame["movement_rank"]=frame.groupby("epoch")["momentum_cost_ratio"].rank(pct=True,ascending=True)
    frame["updates_rank"]=frame.groupby("epoch")["log_updates"].rank(pct=True,ascending=True)
    factor=np.zeros(len(frame),dtype=float);residual=np.zeros(len(frame),dtype=float)
    for _,indexes in frame.groupby("epoch").groups.items():
        indices=list(indexes);sums={};counts={}
        for index in indices:
            row=frame.loc[index];move=float(row["return_5m_pips"])
            for currency,value in ((row["base"],move),(row["quote"],-move)):
                sums[currency]=sums.get(currency,0.0)+value;counts[currency]=counts.get(currency,0)+1
        strengths={currency:sums[currency]/counts[currency] for currency in sums}
        for index in indices:
            row=frame.loc[index];expected=strengths[row["base"]]-strengths[row["quote"]]
            factor[index]=expected;residual[index]=float(row["return_5m_pips"])-expected
    frame["currency_factor_5m_pips"]=factor;frame["pair_residual_5m_pips"]=residual
    return frame.sort_values(["epoch","instrument"]).reset_index(drop=True)


def calibration(model:Any,x:pd.DataFrame,y:pd.Series)->LogisticRegression|None:
    if len(x)<50 or y.nunique()<2:return None
    probability=np.clip(model.predict_proba(x)[:,1],1e-6,1-1e-6)
    logit=np.log(probability/(1-probability)).reshape(-1,1)
    result=LogisticRegression(C=1.0,max_iter=300,random_state=20260808);result.fit(logit,y);return result


def probability(model:Any,calibrator:LogisticRegression|None,x:pd.DataFrame)->np.ndarray:
    raw=np.clip(model.predict_proba(x)[:,1],1e-6,1-1e-6)
    if calibrator is None:return raw
    return calibrator.predict_proba(np.log(raw/(1-raw)).reshape(-1,1))[:,1]


def model_config(raw:Mapping[str,Any])->dict[str,Any]:
    return {"learning_rate":float(raw.get("learning_rate") or .05),"max_iter":int(raw.get("max_iter") or 120),
            "max_leaf_nodes":int(raw.get("max_leaf_nodes") or 15),"min_samples_leaf":int(raw.get("min_samples_leaf") or 50),
            "l2_regularization":float(raw.get("l2_regularization") or 1.0),"random_state":int(raw.get("random_state") or 20260808)}


def select_disjoint(rows:pd.DataFrame,count:int)->pd.DataFrame:
    selected=[];used=set()
    for index,row in rows.sort_values(["predicted_ev_pips","instrument"],ascending=[False,True]).iterrows():
        legs=source.currency_legs(str(row["instrument"]))
        if used.isdisjoint(legs):selected.append(index);used.update(legs)
        if len(selected)==count:break
    return rows.loc[selected] if len(selected)==count else rows.iloc[0:0]


def metrics(values:list[float])->dict[str,Any]:
    gross=sum(max(value,0) for value in values);loss=-sum(min(value,0) for value in values);best=max(values) if values else None
    return {"n":len(values),"wins":sum(value>0 for value in values),"win_rate":sum(value>0 for value in values)/len(values) if values else None,
            "average_net_pips":sum(values)/len(values) if values else None,"total_net_pips":sum(values),
            "profit_factor":gross/loss if loss else None,"minimum_net_pips":min(values) if values else None,"maximum_net_pips":best,
            "average_without_best_pips":(sum(values)-best)/(len(values)-1) if len(values)>1 else None,"proof_eligible":False}


def allocate(frame:pd.DataFrame,config:Mapping[str,Any])->dict[str,Any]:
    eligible=frame[(frame["predicted_clear_probability"]>=float(config["minimum_clear_probability"])) &
                   (frame["predicted_direction_confidence"]>=float(config["minimum_direction_confidence"])) &
                   (frame["predicted_magnitude_pips"]>=float(config["minimum_predicted_magnitude_cost_ratio"])*frame["entry_cost_pips"]) &
                   (frame["predicted_ev_pips"]>0)].copy()
    top=[];flip=[];basket=[];basket_flip=[];decisions=0
    for _,group in eligible.groupby("epoch"):
        decisions+=1;row=group.sort_values(["predicted_ev_pips","instrument"],ascending=[False,True]).iloc[0]
        direction=1 if row["predicted_up_probability"]>=.5 else -1;move=float(row["future_move_pips"]);cost=float(row["actual_cost_pips"])
        top.append(direction*move-cost);flip.append(-direction*move-cost)
        chosen=select_disjoint(group,3)
        if len(chosen)==3:
            values=[];flipped=[]
            for _,leg in chosen.iterrows():
                side=1 if leg["predicted_up_probability"]>=.5 else -1;future=float(leg["future_move_pips"]);leg_cost=float(leg["actual_cost_pips"])
                values.append(side*future-leg_cost);flipped.append(-side*future-leg_cost)
            basket.append(sum(values)/3);basket_flip.append(sum(flipped)/3)
    return {"eligible_pair_rows":len(eligible),"decision_timestamps":decisions,
            "strict_top_one":metrics(top),"strict_top_one_flipped_control":metrics(flip),
            "exactly_three_currency_disjoint":metrics(basket),"exactly_three_currency_disjoint_flipped_control":metrics(basket_flip),
            "no_trade_net_pips":0.0}


def fit_horizon(frame:pd.DataFrame,horizon:int,config:Mapping[str,Any])->tuple[dict[str,Any],dict[str,Any]]:
    epochs=np.array(sorted(frame["epoch"].unique()));train_fraction=float(config["train_fraction"]);cal_fraction=float(config["calibration_fraction"])
    split1=epochs[max(1,int(len(epochs)*train_fraction))-1];split2=epochs[max(2,int(len(epochs)*(train_fraction+cal_fraction)))-1]
    train=frame[frame["epoch"]<split1-horizon].copy();cal=frame[(frame["epoch"]>=split1)&(frame["epoch"]<split2-horizon)].copy();valid=frame[frame["epoch"]>=split2].copy()
    params=model_config(config.get("model") or {})
    clear_model=HistGradientBoostingClassifier(**params);clear_model.fit(train[FEATURES],train["cost_clear"])
    clear_cal=calibration(clear_model,cal[FEATURES],cal["cost_clear"])
    direction_train=train[train["cost_clear"]==1];direction_cal=cal[cal["cost_clear"]==1]
    direction_model=HistGradientBoostingClassifier(**params);direction_model.fit(direction_train[FEATURES],direction_train["future_up"])
    direction_calibrator=calibration(direction_model,direction_cal[FEATURES],direction_cal["future_up"])
    magnitude_model=HistGradientBoostingRegressor(loss="absolute_error",**params);magnitude_model.fit(train[FEATURES],train["future_magnitude_pips"])
    valid["predicted_clear_probability"]=probability(clear_model,clear_cal,valid[FEATURES])
    valid["predicted_up_probability"]=probability(direction_model,direction_calibrator,valid[FEATURES])
    valid["predicted_direction_confidence"]=(2*valid["predicted_up_probability"]-1).abs()
    valid["predicted_magnitude_pips"]=np.maximum(0,magnitude_model.predict(valid[FEATURES]))
    valid["predicted_ev_pips"]=valid["predicted_clear_probability"]*valid["predicted_magnitude_pips"]*valid["predicted_direction_confidence"]-valid["entry_cost_pips"]
    clear_auc=roc_auc_score(valid["cost_clear"],valid["predicted_clear_probability"]) if valid["cost_clear"].nunique()>1 else None
    clear_valid=valid[valid["cost_clear"]==1]
    direction_accuracy=float(((clear_valid["predicted_up_probability"]>=.5).astype(int)==clear_valid["future_up"]).mean()) if len(clear_valid) else None
    diagnostics={"horizon_sec":horizon,"rows":{"train":len(train),"calibration":len(cal),"validation":len(valid),"validation_clear":int(valid["cost_clear"].sum())},
                 "cutoffs":{"train_end_epoch":int(split1),"validation_start_epoch":int(split2)},
                 "clearance":{"base_rate":float(valid["cost_clear"].mean()),"roc_auc":clear_auc,
                              "brier":float(brier_score_loss(valid["cost_clear"],valid["predicted_clear_probability"]))},
                 "conditional_direction_accuracy":direction_accuracy,
                 "magnitude_mae_pips":float(mean_absolute_error(valid["future_magnitude_pips"],valid["predicted_magnitude_pips"])),
                 "allocation":allocate(valid,config),"proof_eligible":False}
    models={"clear_model":clear_model,"clear_calibrator":clear_cal,"direction_model":direction_model,
            "direction_calibrator":direction_calibrator,"magnitude_model":magnitude_model,
            "features":FEATURES,"horizon_sec":horizon,"cutoffs":diagnostics["cutoffs"]}
    return diagnostics,models


def run(config_path:Path=CONFIG,database:Path=DATABASE,quotes:Path=QUOTES,output:Path=OUTPUT,report:Path=REPORT,artifact:Path=ARTIFACT)->dict[str,Any]:
    config=read_json(config_path);source._PIP_SIZES=source.load_pip_sizes(quotes);by_pair,instruments=source.load_minutes(database);results=[];models={}
    for horizon in config.get("horizons_sec") or []:
        frame=build_frame(by_pair,int(horizon),float(config["modeled_slippage_pips"]),float(config["maximum_entry_spread_pips"]));result,bundle=fit_horizon(frame,int(horizon),config);results.append(result);models[str(horizon)]=bundle
    highwater=max(epoch for timeline in by_pair.values() for epoch in timeline);artifact.mkdir(parents=True,exist_ok=True)
    joblib.dump(models,artifact/"models.joblib")
    manifest={"cohort_id":config.get("cohort_id"),"created_utc":dt.datetime.now(dt.timezone.utc).isoformat(),"research_only":True,"execution_eligible":False,
              "source_highwater_epoch":highwater,"source_highwater_utc":dt.datetime.fromtimestamp(highwater,dt.timezone.utc).isoformat(),
              "prospective_start_epoch":highwater+60,"prospective_start_utc":dt.datetime.fromtimestamp(highwater+60,dt.timezone.utc).isoformat(),
              "source_instrument_count":len(instruments),"feature_contract":FEATURES,"config_sha256":sha(config_path),"source_code_sha256":sha(Path(__file__)),
              "material_change_requires_new_cohort":True}
    atomic(artifact/"manifest.json",json.dumps(manifest,indent=2,sort_keys=True))
    payload={"schema_version":1,**manifest,"evidence_class":"already_inspected_archive_engineering_discovery","proof_eligible":False,"can_place_orders":False,
             "results":results,"limitations":["short archive","validation period was previously inspected by other research","minute-average spread plus modeled slippage","no untouched prospective outcomes"]}
    atomic(output,json.dumps(payload,indent=2,sort_keys=True))
    lines=["# Executable Opportunity Ranking","","Engineering discovery only; no execution or promotion path.","",f"- Cohort: `{manifest['cohort_id']}`",f"- Prospective evidence begins strictly after: `{manifest['source_highwater_utc']}`","",
           "| Horizon | Train / Cal / Validation | Clear base | Clear AUC | Brier | Direction on clears | Magnitude MAE | Top-one N | Win | Avg net | Basket N | Basket avg |",
           "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for result in results:
        rows=result["rows"];allocation=result["allocation"];top=allocation["strict_top_one"];basket=allocation["exactly_three_currency_disjoint"]
        def value(v:Any,fmt:str=".3f")->str:return "n/a" if v is None else format(v,fmt)
        lines.append(f"| {result['horizon_sec']}s | {rows['train']} / {rows['calibration']} / {rows['validation']} | {result['clearance']['base_rate']:.1%} | {value(result['clearance']['roc_auc'])} | {result['clearance']['brier']:.3f} | {value(result['conditional_direction_accuracy'],'.1%')} | {result['magnitude_mae_pips']:.3f} | {top['n']} | {value(top['win_rate'],'.1%')} | {value(top['average_net_pips'])} | {basket['n']} | {value(basket['average_net_pips'])} |")
    lines += ["","A positive engineering estimate cannot graduate from this archive. Only records after the frozen prospective start can count toward proof.",""]
    atomic(report,"\n".join(lines));return payload


def main()->int:
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--config",type=Path,default=CONFIG);parser.add_argument("--database",type=Path,default=DATABASE);parser.add_argument("--quotes",type=Path,default=QUOTES);parser.add_argument("--output",type=Path,default=OUTPUT);parser.add_argument("--report",type=Path,default=REPORT);parser.add_argument("--artifact",type=Path,default=ARTIFACT);args=parser.parse_args();run(args.config,args.database,args.quotes,args.output,args.report,args.artifact);return 0


if __name__=="__main__":raise SystemExit(main())
