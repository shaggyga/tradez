#!/usr/bin/env python3
"""V9-only rank cohort: actual observed forecast availability gates every entry.

Disabled until explicitly enabled. First observation is persisted independently
of decisions, so an old quote waits for a later fresh quote rather than freezing
a pre-availability fill or erasing the pending opportunity on restart.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable, Iterator, Mapping

import oanda_causal_source_factor_response_map_v9 as source
import oanda_source_conditioned_currency_rank_v7 as parent
import oanda_source_conditioned_currency_rank_v1 as base
import oanda_source_conditioned_currency_rank_v5 as no_trade

ROOT=Path(__file__).resolve().parent
DEFAULT_MANIFEST=ROOT/"config/source_conditioned_currency_rank_v8.json"
DEFAULT_LEDGER=parent.STATE/"source_conditioned_currency_rank_v8.sqlite"
DEFAULT_STATE=parent.STATE/"source_conditioned_currency_rank_v8.json"
DEFAULT_REPORT=parent.REPORT_ROOT/"SOURCE_CONDITIONED_CURRENCY_RANK_V8.md"
SCHEMA_VERSION="source_conditioned_currency_rank_v8_observed_publication_v1"
CONTRACT_ID="source_conditioned_currency_rank_v8_observed_publication_entry_20260905"
BASE_COHORT_ID="source_conditioned_currency_rank_v8_observed_publication_20260905"
PARENT_CONTRACT_ID=parent.CONTRACT_ID
REQUIRED_SOURCE_CONTRACT_ID=source.CONTRACT_ID
NO_TRADE_BASELINE_CONTRACT_ID="source_conditioned_currency_rank_v8_no_trade_zero_value_20260905"
_BASE_OPEN=base.open_ledger
_BASE_LOAD=base.load_source_forecasts


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def read_config(path:Path=DEFAULT_MANIFEST)->dict[str,Any]:
    value=json.loads(path.read_text(encoding="utf-8-sig"))
    if value.get("contract_id")!=CONTRACT_ID or value.get("required_source_contract_id")!=REQUIRED_SOURCE_CONTRACT_ID or value.get("historical_rows_imported") is not False:
        raise ValueError("rank V8 contract mismatch or predecessor import")
    if value.get("research_only") is not True or any(value.get(k) is not False for k in ("execution_eligible","can_place_orders","can_authorize","can_promote")):
        raise ValueError("rank V8 must remain research only")
    if type(value.get("collection_enabled")) is not bool:
        raise ValueError("rank collection_enabled must be an explicit boolean")
    return value


def adapter_definition_sha256()->str:
    return base.digest(CONTRACT_ID,base.file_sha256(Path(__file__)),base.file_sha256(DEFAULT_MANIFEST),REQUIRED_SOURCE_CONTRACT_ID,PARENT_CONTRACT_ID,
                       *(base.file_sha256(ROOT/name) for name in ("oanda_source_conditioned_currency_rank_v1.py","oanda_source_conditioned_currency_rank_v5.py","oanda_currency_rank_model.py")))


def open_ledger(path:Path=DEFAULT_LEDGER)->sqlite3.Connection:
    if path.exists():
        check=sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro",uri=True)
        try:
            tables={row[0] for row in check.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if "rank_decision" in tables and "source_forecast_first_observed" not in tables:
                raise ValueError("rank V8 refuses a predecessor or foreign ledger schema")
            if "rank_decision" in tables and check.execute("SELECT 1 FROM rank_decision WHERE contract_id<>? LIMIT 1",(CONTRACT_ID,)).fetchone():
                raise ValueError("rank V8 refuses predecessor or foreign ledger")
        finally: check.close()
    con=_BASE_OPEN(path)
    no_trade.ensure_no_trade_schema(con)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS source_forecast_first_observed(
          forecast_id TEXT PRIMARY KEY, source_cohort_id TEXT NOT NULL,
          source_payload_sha256 TEXT NOT NULL, first_observed_utc TEXT NOT NULL,
          source_available_utc TEXT NOT NULL, source_issued_utc TEXT NOT NULL,
          contract_id TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS rank_cohort_binding(
          singleton INTEGER PRIMARY KEY CHECK(singleton=1),contract_id TEXT NOT NULL,
          source_cohort_id TEXT NOT NULL,definition_sha256 TEXT NOT NULL,
          config_sha256 TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS rank_selection_intent(
          intent_id TEXT PRIMARY KEY,market_episode_id TEXT NOT NULL,horizon_min INTEGER NOT NULL,
          selection_completed_utc TEXT NOT NULL,intent_payload_json TEXT NOT NULL,
          contract_id TEXT NOT NULL,UNIQUE(market_episode_id,horizon_min));
        CREATE TABLE IF NOT EXISTS rank_selection_availability(
          intent_id TEXT PRIMARY KEY,selection_committed_utc TEXT NOT NULL,
          intent_payload_sha256 TEXT NOT NULL,contract_id TEXT NOT NULL,
          FOREIGN KEY(intent_id) REFERENCES rank_selection_intent(intent_id));
        CREATE TRIGGER IF NOT EXISTS rank_binding_no_update BEFORE UPDATE ON rank_cohort_binding
          BEGIN SELECT RAISE(ABORT,'rank binding is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rank_binding_no_delete BEFORE DELETE ON rank_cohort_binding
          BEGIN SELECT RAISE(ABORT,'rank binding is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS first_observed_no_update BEFORE UPDATE ON source_forecast_first_observed
          BEGIN SELECT RAISE(ABORT,'source observation is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS first_observed_no_delete BEFORE DELETE ON source_forecast_first_observed
          BEGIN SELECT RAISE(ABORT,'source observation is immutable'); END;
    """)
    for table in ("rank_selection_intent","rank_selection_availability"):
        for action in ("UPDATE","DELETE"):
            con.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_no_{action.lower()} BEFORE {action} ON {table} BEGIN SELECT RAISE(ABORT,'append_only:{table}'); END")
    con.commit()
    return con


def load_source_forecasts(path:Path)->list[dict[str,Any]]:
    """Read rows and their committed-forecast attestation in one snapshot."""
    if not path.exists(): raise ValueError("rank V8 required source database is missing")
    source.load_activation(path,source.read_config())
    con=sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro",uri=True,timeout=15)
    con.row_factory=sqlite3.Row
    try:
        con.execute("PRAGMA query_only=ON")
        con.execute("BEGIN")
        activation=con.execute("SELECT * FROM source_cohort_activation WHERE singleton=1").fetchone()
        if not activation or activation["contract_id"]!=REQUIRED_SOURCE_CONTRACT_ID:
            raise ValueError("rank requires registered V9 activation")
        activated,recorded=base.parse_time(activation["activated_utc"]),base.parse_time(activation["recorded_utc"])
        if activated is None or recorded is None or activated<recorded:
            raise ValueError("rank invalid source activation clocks")
        if con.execute("SELECT 1 FROM source_factor_forecast WHERE contract_id<>? OR cohort_id<>? LIMIT 1",(REQUIRED_SOURCE_CONTRACT_ID,activation["cohort_id"])).fetchone():
            raise ValueError("rank V8 source contract/cohort contamination")
        rows=con.execute("""SELECT f.*,ep.market_episode_id,e.first_known_utc,
            sf.factor_known_utc semantic_factor_clock,sf.prospective_proof_eligible factor_prospective,
            e.prospective_proof_eligible event_prospective,sf.contract_id factor_contract,sf.cohort_id factor_cohort,
            e.contract_id event_contract,e.cohort_id event_cohort,
            a.available_utc,a.forecast_payload_sha256,a.availability_contract,
            a.contract_id receipt_contract_id,a.cohort_id receipt_cohort_id
            FROM source_factor_forecast f JOIN source_event_episode ep USING(canonical_event_id)
            JOIN source_event_observation e USING(canonical_event_id)
            JOIN source_factor_observation sf USING(factor_observation_id)
            JOIN source_forecast_availability a USING(forecast_id)
            WHERE f.forecast_state='forecast' AND f.prospective_proof_eligible=1""").fetchall()
        output=[]
        for raw in rows:
            row=dict(raw)
            issued,available,first=map(base.parse_time,(row["issued_utc"],row["available_utc"],row["first_known_utc"]))
            payload=json.loads(row["forecast_payload_json"])
            semantic,cutoff=map(base.parse_time,(payload.get("semantic_available_utc"),row["training_cutoff_utc"]))
            if None in (issued,available,first,semantic,cutoff) or not (activated<=first<=semantic<=cutoff<=issued<=available):
                raise ValueError("rank invalid source availability chronology")
            if payload.get("semantic_available_utc")!=row["semantic_factor_clock"] or row["factor_prospective"]!=1 or row["event_prospective"]!=1:
                raise ValueError("rank semantic availability/proof does not match its source factor")
            if (row["factor_contract"],row["event_contract"],row["factor_cohort"],row["event_cohort"])!=(REQUIRED_SOURCE_CONTRACT_ID,REQUIRED_SOURCE_CONTRACT_ID,activation["cohort_id"],activation["cohort_id"]):
                raise ValueError("rank source factor/event lineage mismatch")
            if row["availability_contract"]!=source.AVAILABILITY_CONTRACT or row["receipt_contract_id"]!=REQUIRED_SOURCE_CONTRACT_ID or row["receipt_cohort_id"]!=activation["cohort_id"]:
                raise ValueError("rank source receipt contract mismatch")
            if source.base.digest(row["forecast_payload_json"])!=row["forecast_payload_sha256"]:
                raise ValueError("rank source forecast receipt hash mismatch")
            for key in ("forecast_id","canonical_event_id","currency","factor_key","horizon_min","issued_utc","training_cutoff_utc","forecast_state","effective_event_n","probability_strengthening","predicted_currency_factor_bps","predicted_absolute_factor_bps","prospective_proof_eligible"):
                if row[key]!=payload.get(key):
                    raise ValueError("rank source forecast row/payload mismatch:"+key)
            if any(base.number(row[key]) is None for key in ("probability_strengthening","predicted_currency_factor_bps","predicted_absolute_factor_bps")):
                raise ValueError("rank source nonfinite prediction")
            row.update(source_contract_id=row["contract_id"],source_cohort_id=row["cohort_id"],source_issued_utc=row["issued_utc"])
            output.append(row)
        return output
    finally: con.close()


def observe_forecasts(con:sqlite3.Connection,rows:list[dict[str,Any]],*,observed_utc:datetime,max_capture_lag_sec:float)->list[dict[str,Any]]:
    output=[]
    for source_row in rows:
        row=dict(source_row)
        issued,available=base.parse_time(row["source_issued_utc"]),base.parse_time(row["available_utc"])
        # A restart cannot refresh an expired prediction by observing it anew.
        if available>observed_utc or issued>observed_utc or (observed_utc-issued).total_seconds()>min(max_capture_lag_sec,60*int(row["horizon_min"])):
            continue
        con.execute("INSERT OR IGNORE INTO source_forecast_first_observed VALUES (?,?,?,?,?,?,?)",(row["forecast_id"],row["source_cohort_id"],row["forecast_payload_sha256"],base.iso(observed_utc),row["available_utc"],row["source_issued_utc"],CONTRACT_ID))
        receipt=con.execute("SELECT source_cohort_id,source_payload_sha256,first_observed_utc,source_available_utc,source_issued_utc,contract_id FROM source_forecast_first_observed WHERE forecast_id=?",(row["forecast_id"],)).fetchone()
        if (receipt[0],receipt[1],receipt[3],receipt[4],receipt[5])!=(row["source_cohort_id"],row["forecast_payload_sha256"],row["available_utc"],row["source_issued_utc"],CONTRACT_ID):
            raise ValueError("immutable consumer availability binding mismatch")
        first=base.parse_time(receipt[2])
        if first is None or first<available or first>observed_utc:
            raise ValueError("invalid consumer availability clock")
        row["issued_utc"]=base.iso(first)
        row["consumer_first_observed_utc"]=base.iso(first)
        output.append(row)
    con.commit()
    return output


def publish_selection_availability(con:sqlite3.Connection,*,clock:Callable[[],datetime])->None:
    """Attest only after the immutable completed selection has committed."""
    con.commit()
    available=source.clock_now(clock)
    pending=con.execute("""SELECT i.intent_id,i.selection_completed_utc,i.intent_payload_json
        FROM rank_selection_intent i LEFT JOIN rank_selection_availability a USING(intent_id)
        WHERE a.intent_id IS NULL""").fetchall()
    for intent_id,completed,payload in pending:
        if base.parse_time(completed)>available:
            raise ValueError("rank selection availability clock moved backwards")
        con.execute("INSERT INTO rank_selection_availability VALUES (?,?,?,?)",(intent_id,base.iso(available),base.digest(payload),CONTRACT_ID))
    con.commit()


def fill_pending_selections(con:sqlite3.Connection,quotes:Mapping[str,Any],*,clock:Callable[[],datetime],max_quote_age_sec:float,max_capture_lag_sec:float)->int:
    """Fill the frozen pair/side from a later quote; never select again here."""
    observed=source.clock_now(clock)
    pending=con.execute("""SELECT i.*,a.selection_committed_utc,a.intent_payload_sha256
        FROM rank_selection_intent i JOIN rank_selection_availability a USING(intent_id)
        LEFT JOIN rank_decision d ON d.market_episode_id=i.market_episode_id AND d.horizon_min=i.horizon_min
        WHERE d.decision_id IS NULL ORDER BY i.selection_completed_utc,i.intent_id""").fetchall()
    inserted=0
    for row in pending:
        intent=json.loads(row["intent_payload_json"])
        if base.digest(row["intent_payload_json"])!=row["intent_payload_sha256"] or row["contract_id"]!=CONTRACT_ID:
            raise ValueError("rank pending selection immutable binding mismatch")
        boundary=base.parse_time(row["selection_committed_utc"])
        if boundary is None or boundary<base.parse_time(row["selection_completed_utc"]) or boundary>observed:
            raise ValueError("rank pending selection chronology")
        required={item for group in intent["trigger_rows"] for item in group["source_forecast_ids"]}
        required.update(item for arm in intent["arms"] for item in arm.get("source_forecast_ids",[]))
        expired=False
        for forecast_id in required:
            receipt=con.execute("SELECT source_issued_utc,first_observed_utc FROM source_forecast_first_observed WHERE forecast_id=?",(forecast_id,)).fetchone()
            if not receipt: raise ValueError("pending selection source observation is missing")
            boundary=max(boundary,base.parse_time(receipt[1]))
            expired |= (observed-base.parse_time(receipt[0])).total_seconds()>min(max_capture_lag_sec,60*int(row["horizon_min"]))
        arms=[];waiting=False
        for original in intent["arms"]:
            arm=dict(original)
            arm.update(selection_intent_id=row["intent_id"],selection_completed_utc=row["selection_completed_utc"],selection_committed_utc=row["selection_committed_utc"])
            if arm["selected"] and expired:
                arm.update(selected=False,entry_quote={},abstain_reason="intent_expired_before_fresh_entry_quote")
            elif arm["selected"]:
                quote,reason=base.quote_for_instrument(quotes,arm["instrument"],cutoff=observed,not_before=boundary,max_age_sec=max_quote_age_sec,max_source_capture_lag_sec=max_capture_lag_sec)
                if quote is None or base.parse_time(quote["quote_utc"])<=boundary:
                    waiting=True
                    break
                arm["selection_quote"]=arm.pop("entry_quote",{})
                arm["entry_quote"]=quote
                after_cost=float(arm["predicted_gross_bps"])-1.2*float(quote["spread_bps"])
                arm["predicted_after_cost_bps"]=after_cost
                if after_cost<=0:
                    arm.update(selected=False,abstain_reason="frozen_intent_does_not_clear_entry_spread")
            arms.append(arm)
        if waiting: continue
        completed=source.clock_now(clock)
        if completed<observed: raise ValueError("rank fill clock moved backwards")
        inserted+=int(base.persist_decision(con,episode_id=row["market_episode_id"],horizon_min=row["horizon_min"],cutoff=completed,
                      source_database=Path(intent["source_database"]),trigger_rows=intent["trigger_rows"],source_state=intent["source_state"],
                      ticker=intent["ticker"],quotes=quotes,arms=arms,price_result=intent["price_result"]))
    return inserted


def mature_outcomes(con:sqlite3.Connection,quote_bars_database:Path,*,observed_utc:datetime,clock:Callable[[],datetime])->int:
    """A horizon is not mature until its executable exit quote is observed.

    Preserve the predecessor math, but reject an exit bar later than this
    observation cutoff and retain both observation and recording clocks.
    """
    rows=con.execute("""SELECT f.* FROM rank_forecast f LEFT JOIN rank_outcome o USING(forecast_id)
        WHERE f.selected=1 AND o.forecast_id IS NULL AND julianday(f.maturity_utc)<=julianday(?)
        ORDER BY f.maturity_utc,f.forecast_id""",(base.iso(observed_utc),)).fetchall()
    inserted=0
    for row in rows:
        target=base.parse_time(row["maturity_utc"])
        exit_quote=base.load_exit_quote(quote_bars_database,row["instrument"],target,max_alignment_sec=90.0)
        if exit_quote is None or base.parse_time(exit_quote["quote_utc"])>observed_utc:
            continue
        if any(base.number(exit_quote[key]) is None for key in ("bid","ask","pip")):
            continue
        entry_bid,entry_ask,pip=float(row["entry_bid"]),float(row["entry_ask"]),float(row["pip"])
        if float(exit_quote["pip"])!=pip:
            continue
        sign=1.0 if row["direction"]=="buy" else -1.0
        gross=sign*((exit_quote["bid"]+exit_quote["ask"])/2-(entry_bid+entry_ask)/2)/pip
        executable=((exit_quote["bid"]-entry_ask) if row["direction"]=="buy" else (entry_bid-exit_quote["ask"]))/pip
        cost=gross-executable
        recorded=source.clock_now(clock)
        if recorded<observed_utc:
            raise ValueError("rank outcome recording clock moved backwards")
        payload={"forecast_id":row["forecast_id"],"arm":row["arm"],"instrument":row["instrument"],"direction":row["direction"],
                 "maturity_utc":row["maturity_utc"],"exit_quote_utc":exit_quote["quote_utc"],"exit_alignment_sec":exit_quote["alignment_sec"],
                 "gross_mid_pips":gross,"executable_after_cost_pips":executable,"realized_cost_pips":cost,
                 "maturity_observed_utc":base.iso(observed_utc),"recorded_utc":base.iso(recorded),"research_only":True,"execution_eligible":False}
        before=con.total_changes
        con.execute("INSERT OR IGNORE INTO rank_outcome VALUES (?,?,?,?,?,?,?,?,?,?,?,1,0,?)",(row["forecast_id"],row["maturity_utc"],exit_quote["quote_utc"],exit_quote["alignment_sec"],exit_quote["bid"],exit_quote["ask"],gross,executable,cost,int(executable>0),json.dumps(payload,sort_keys=True,separators=(",",":")),CONTRACT_ID))
        inserted+=con.total_changes-before
    con.commit()
    return inserted


@contextmanager
def configured()->Iterator[None]:
    patches={base:{"CONTRACT_ID":CONTRACT_ID,"SCHEMA_VERSION":SCHEMA_VERSION,"BASE_COHORT_ID":BASE_COHORT_ID,"adapter_definition_sha256":adapter_definition_sha256},
             no_trade:{"CONTRACT_ID":CONTRACT_ID,"NO_TRADE_BASELINE_CONTRACT_ID":NO_TRADE_BASELINE_CONTRACT_ID}}
    previous={module:{key:getattr(module,key) for key in values} for module,values in patches.items()}
    try:
        for module,values in patches.items():
            for key,value in values.items(): setattr(module,key,value)
        yield
    finally:
        for module,values in previous.items():
            for key,value in values.items(): setattr(module,key,value)


def run_cycle(*,config_path:Path=DEFAULT_MANIFEST,source_database:Path=source.OUTPUT_DATABASE,
              ledger_path:Path=DEFAULT_LEDGER,state_path:Path=DEFAULT_STATE,report_path:Path=DEFAULT_REPORT,
              ticker_path:Path=parent.DEFAULT_TICKER,quotes_path:Path=parent.DEFAULT_QUOTES,
              quote_bars_database:Path=parent.DEFAULT_QUOTE_BARS,clock:Callable[[],datetime]=utc_now)->dict[str,Any]:
    config=read_config(config_path)
    if config.get("collection_enabled") is not True:
        return {"schema_version":SCHEMA_VERSION,"contract_id":CONTRACT_ID,"status":"inactive","generated_utc":base.iso(source.clock_now(clock)),"research_only":True,"can_place_orders":False}
    rows=load_source_forecasts(source_database)
    # Sample after the source snapshot/attestation read. An attestation with a
    # delayed commit cannot claim an earlier usable entry at this consumer.
    observed=source.clock_now(clock)
    max_lag=float(config["maximum_capture_lag_sec"])
    max_quote_age=float(config["maximum_quote_age_sec"])
    ticker,quotes=base.read_json(ticker_path),base.read_json(quotes_path)
    with configured():
        con=open_ledger(ledger_path)
        try:
            activation=source.load_activation(source_database,source.read_config())
            config_sha=base.digest(json.dumps({key:value for key,value in config.items() if key!="collection_enabled"},sort_keys=True))
            binding=(CONTRACT_ID,activation["cohort_id"],adapter_definition_sha256(),config_sha)
            con.execute("INSERT OR IGNORE INTO rank_cohort_binding VALUES (1,?,?,?,?)",binding)
            saved=con.execute("SELECT contract_id,source_cohort_id,definition_sha256,config_sha256 FROM rank_cohort_binding WHERE singleton=1").fetchone()
            if tuple(saved)!=binding:
                raise ValueError("rank material change requires a new cohort ledger")
            con.commit()
            ready=observe_forecasts(con,rows,observed_utc=observed,max_capture_lag_sec=max_lag)
            groups=base.group_source_forecasts(ready)
            existing={(str(row[0]),int(row[1])) for row in con.execute("SELECT market_episode_id,horizon_min FROM rank_decision UNION SELECT market_episode_id,horizon_min FROM rank_selection_intent")}
            triggers:dict[tuple[str,int],list[dict[str,Any]]]={}
            for row in groups:
                key=(row["market_episode_id"],row["horizon_min"])
                if key not in existing: triggers.setdefault(key,[]).append(row)
            new_intents=0
            for (episode,horizon),trigger in sorted(triggers.items()):
                earliest=base.trigger_knowledge_cutoff(trigger)
                # Wait for a quote after observation, without consuming the
                # decision slot as a timing-induced abstention.
                has_quote=any(base.quote_for_instrument(quotes,pair,cutoff=observed,not_before=earliest,max_age_sec=max_quote_age,max_source_capture_lag_sec=max_lag)[0] is not None for pair in base.price_rank.EXPECTED_INSTRUMENTS)
                if not has_quote: continue
                state=base.active_currency_source_state(groups,cutoff=observed,horizon_min=horizon)
                price_arm,price_result=base.build_price_only_arm(ticker,quotes,cutoff=observed,entry_not_before=earliest,max_quote_age_sec=max_quote_age,max_source_capture_lag_sec=max_lag)
                source_arm=base.build_source_only_arm(state,quotes,cutoff=observed,entry_not_before=earliest,max_quote_age_sec=max_quote_age,max_source_capture_lag_sec=max_lag)
                hybrid=base.build_source_price_timing_arm(trigger,state,ticker,quotes,cutoff=observed,entry_not_before=earliest,max_quote_age_sec=max_quote_age,max_source_capture_lag_sec=max_lag)
                completed=source.clock_now(clock)
                if completed<observed: raise ValueError("rank selection completion clock moved backwards")
                intent={"source_database":str(source_database),"trigger_rows":trigger,"source_state":state,"ticker":ticker,"arms":[price_arm,source_arm,hybrid],"price_result":price_result}
                con.execute("INSERT INTO rank_selection_intent VALUES (?,?,?,?,?,?)",("rank_intent_"+base.digest(episode,horizon,CONTRACT_ID)[:32],episode,horizon,base.iso(completed),json.dumps(intent,sort_keys=True,separators=(",",":")),CONTRACT_ID))
                new_intents+=1
            publish_selection_availability(con,clock=clock)
            # Pending selections use later quotes with fixed pair/side. A
            # delayed ledger commit therefore cannot backdate an executable
            # entry to the cycle's earlier quote snapshot.
            new_decisions=fill_pending_selections(con,quotes,clock=clock,max_quote_age_sec=max_quote_age,max_capture_lag_sec=max_lag)
            settled=source.clock_now(clock)
            matured=mature_outcomes(con,quote_bars_database,observed_utc=settled,clock=clock)
            baseline=no_trade.persist_no_trade_baselines(con,observed_utc=settled)
            summary=no_trade.add_no_trade_summary(con,base.ledger_summary(con))
            integrity=con.execute("PRAGMA quick_check(1)").fetchone()[0]
        finally: con.close()
    result={"schema_version":SCHEMA_VERSION,"contract_id":CONTRACT_ID,"parent_adapter_contract_id":PARENT_CONTRACT_ID,
            "required_source_contract_id":REQUIRED_SOURCE_CONTRACT_ID,"generated_utc":base.iso(source.clock_now(clock)),"decision_cutoff_utc":base.iso(observed),
            "status":"ok","source_forecast_rows":len(rows),"observed_unexpired_source_forecasts":len(ready),"new_selection_intents":new_intents,"new_decisions":new_decisions,
            "matured_outcomes":matured,"no_trade_baseline":baseline,"ledger":summary,"sqlite_integrity":integrity,
            "research_only":True,"execution_eligible":False,"can_place_orders":False,"can_authorize":False,"can_promote":False,
            "policy":{"entry_clock":"strictly_after_committed_frozen_rank_selection_and_all_source_observations","source_expiry":"original_actual_issue_clock","historical_rows_imported":False,"pair_and_side_frozen_before_entry_quote":True}}
    base.atomic_write(state_path,json.dumps(result,indent=2,sort_keys=True)+"\n")
    base.atomic_write(report_path,"# Source-conditioned rank V8\n\nResearch only. Entries require observed V9 forecast availability.\n\n```json\n"+json.dumps(result,indent=2,sort_keys=True)+"\n```\n")
    return result


def main()->int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config",type=Path,default=DEFAULT_MANIFEST)
    parser.add_argument("--once",action="store_true")
    parser.add_argument("--interval-sec",type=float,default=5.0)
    parser.add_argument("--duration-sec",type=float,default=0.0)
    args=parser.parse_args()
    started=time.monotonic()
    while True:
        result=run_cycle(config_path=args.config)
        print(json.dumps({key:result.get(key) for key in ("status","contract_id","generated_utc","source_forecast_rows","new_decisions","matured_outcomes")},sort_keys=True),flush=True)
        if args.once or result["status"]=="inactive" or (args.duration_sec>0 and time.monotonic()-started>=args.duration_sec):
            break
        time.sleep(max(1.0,args.interval_sec))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
