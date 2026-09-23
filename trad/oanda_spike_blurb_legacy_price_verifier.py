#!/usr/bin/env python3
"""Independently verify the immutable legacy executable-price archive."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


ROOT=Path(__file__).resolve().parent
DATABASE=ROOT/"data"/"oanda_training_manager"/"state"/"spike_blurb_factor_reconstruction_v1.sqlite"
LABELS=ROOT/"data"/"oanda_training_manager"/"news_event_tags"/"significant_move_news_tags.csv"
REPORT_ROOT=ROOT/"data"/"oanda_training_manager"/"reports"/"spike_blurb_factor_reconstruction"
OUTPUT=REPORT_ROOT/"SPIKE_BLURB_LEGACY_PRICE_VERIFICATION_V1.json"
PRICE_CONTRACT_ID="spike_blurb_legacy_price_reacquisition_v1_20260819"


def pip_size(instrument: str)->float:
    return .01 if instrument.endswith("_JPY") or instrument in {"USD_THB","USD_HUF","EUR_HUF"} else .0001


def close(left: Any,right: Any,tolerance: float=1e-8)->bool:
    if left is None or right is None:return left is right
    return math.isfinite(float(left)) and math.isfinite(float(right)) and abs(float(left)-float(right))<=tolerance*max(1.0,abs(float(left)),abs(float(right)))


def recompute(instrument: str,candles:list[Mapping[str,Any]])->dict[str,Any]:
    first,last=candles[0],candles[-1]; pip=pip_size(instrument)
    entry_ask=float(first["ask_o"]);entry_bid=float(first["bid_o"]);exit_bid=float(last["bid_c"]);exit_ask=float(last["ask_c"])
    long_net=(exit_bid-entry_ask)/pip;short_net=(entry_bid-exit_ask)/pip
    spreads=[(float(row["ask_c"])-float(row["bid_c"]))/pip for row in candles]
    return {"candle_count":len(candles),"first_utc":str(first["time"]),"last_utc":str(last["time"]),
            "entry_bid":entry_bid,"entry_ask":entry_ask,"exit_bid":exit_bid,"exit_ask":exit_ask,
            "average_spread_pips":sum(spreads)/len(spreads),"maximum_spread_pips":max(spreads),
            "long_net_pips":long_net,"short_net_pips":short_net,"best_direction":"long" if long_net>=short_net else "short",
            "best_net_pips":max(long_net,short_net),
            "long_mfe_pips":(max(float(r["bid_h"]) for r in candles)-entry_ask)/pip,
            "long_mae_pips":(min(float(r["bid_l"]) for r in candles)-entry_ask)/pip,
            "short_mfe_pips":(entry_bid-min(float(r["ask_l"]) for r in candles))/pip,
            "short_mae_pips":(entry_bid-max(float(r["ask_h"]) for r in candles))/pip}


FLOAT_FIELDS=("entry_bid","entry_ask","exit_bid","exit_ask","average_spread_pips","maximum_spread_pips","long_net_pips","short_net_pips","best_net_pips","long_mfe_pips","long_mae_pips","short_mfe_pips","short_mae_pips")


def verify_window(record: Mapping[str,Any])->list[str]:
    errors=[]
    try: raw=gzip.decompress(record["payload_gzip"])
    except Exception:return ["payload_gzip_invalid"]
    if hashlib.sha256(raw).hexdigest()!=record["payload_sha256"]:errors.append("payload_hash_mismatch")
    try:payload=json.loads(raw)
    except Exception:return errors+["payload_json_invalid"]
    if payload.get("instrument")!=record["instrument"]:errors.append("payload_instrument_mismatch")
    candles=payload.get("candles") or []
    epochs=[row.get("epoch") for row in candles]
    if epochs!=sorted(set(epochs)):errors.append("candle_epochs_not_unique_sorted")
    if int(record["candle_count"])!=len(candles):errors.append("candle_count_mismatch")
    if record["coverage_state"]=="exact_window" and not candles:return errors+["exact_window_without_candles"]
    if not candles:return errors
    expected=recompute(str(record["instrument"]),candles)
    for field in ("candle_count","first_utc","last_utc","best_direction"):
        if record[field]!=expected[field]:errors.append(f"{field}_mismatch")
    for field in FLOAT_FIELDS:
        if not close(record[field],expected[field]):errors.append(f"{field}_mismatch")
    return errors


def verify(database_path:Path=DATABASE,labels_path:Path=LABELS)->dict[str,Any]:
    with labels_path.open("r",encoding="utf-8-sig",newline="") as handle:expected_labels=sum(1 for _ in csv.DictReader(handle))
    database=sqlite3.connect(f"file:{database_path.resolve().as_posix()}?mode=ro",uri=True);database.row_factory=sqlite3.Row;database.execute("PRAGMA query_only=ON")
    rows=list(database.execute("SELECT * FROM legacy_executable_price_windows WHERE contract_id=? ORDER BY move_id",(PRICE_CONTRACT_ID,)))
    violations=Counter();examples=[]
    for record in rows:
        errors=verify_window(record)
        for error in errors:violations[error]+=1
        if errors and len(examples)<25:examples.append({"move_id":record["move_id"],"errors":errors})
    integrity=str(database.execute("PRAGMA integrity_check").fetchone()[0]);database.close()
    result={"schema_version":1,"generated_utc":datetime.now(timezone.utc).isoformat(),"price_contract_id":PRICE_CONTRACT_ID,
            "expected_label_count":expected_labels,"stored_window_count":len(rows),"complete":len(rows)==expected_labels,
            "violation_count":sum(violations.values()),"violations":dict(sorted(violations.items())),"violation_examples":examples,
            "sqlite_integrity":integrity,"verified":len(rows)==expected_labels and not violations and integrity=="ok",
            "research_only":True,"execution_eligible":False}
    encoded=json.dumps(result,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode();result["snapshot_sha256"]=hashlib.sha256(encoded).hexdigest();return result


def parse_args()->argparse.Namespace:
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--database",type=Path,default=DATABASE);p.add_argument("--labels",type=Path,default=LABELS);p.add_argument("--output",type=Path,default=OUTPUT);return p.parse_args()


def main()->int:
    a=parse_args();r=verify(a.database,a.labels);a.output.parent.mkdir(parents=True,exist_ok=True);tmp=a.output.with_suffix(a.output.suffix+".tmp");tmp.write_text(json.dumps(r,indent=2,sort_keys=True)+"\n",encoding="utf-8");tmp.replace(a.output);print(json.dumps(r,indent=2,sort_keys=True));return 0 if r["verified"] else 2


if __name__=="__main__":raise SystemExit(main())
