"""Independent read-only V9/V8 readiness and selected-entry clock verifier.

Standard-library only; imports no producer, runtime, broker, or execution code.
An inactive disposition is distinct from live readiness or predictive proof.
"""
from __future__ import annotations
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
from typing import Any

ROOT=Path(__file__).resolve().parent
LOCAL_NEWS=ROOT/"data/oanda_training_manager/local_news_sentiment"
STATE=ROOT/"data/oanda_training_manager/state"
SOURCE_CONTRACT="causal_source_factor_response_map_v9_publication_availability_20260905"
RANK_CONTRACT="source_conditioned_currency_rank_v8_observed_publication_entry_20260905"
PARENT_RANK="source_conditioned_currency_rank_v7_v8_input_explicit_no_trade_20260901"
CLASSIFIER="local_fx_news_rules_20260904_v164_conflict_duration_recap_guard"
AVAILABILITY="source_forecast_committed_row_availability_v1_20260905"


def _sha(text:str)->str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _digest(*parts:Any)->str:
    return _sha("\x1f".join(str(part) for part in parts))


def _file_sha(path:Path)->str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _time(value:Any)->datetime:
    stamp=datetime.fromisoformat(str(value).replace("Z","+00:00"))
    if stamp.tzinfo is None: raise ValueError("naive clock")
    return stamp.astimezone(timezone.utc)


def _read(path:Path)->dict[str,Any]:
    value=json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value,dict): raise ValueError("expected JSON object:"+str(path))
    return value


def _source_implementation()->str:
    paths=[ROOT/f"oanda_causal_source_factor_response_map_v{version}.py" for version in range(1,10)]
    paths+=[ROOT/"src/forex_system/features/currency_state_engine.py"]
    return _sha(json.dumps({str(path.relative_to(ROOT)).replace("\\","/"):_file_sha(path) for path in paths},sort_keys=True))


def _rank_implementation()->str:
    return _digest(RANK_CONTRACT,_file_sha(ROOT/"oanda_source_conditioned_currency_rank_v8.py"),_file_sha(ROOT/"config/source_conditioned_currency_rank_v8.json"),SOURCE_CONTRACT,PARENT_RANK,
                   *(_file_sha(ROOT/name) for name in ("oanda_source_conditioned_currency_rank_v1.py","oanda_source_conditioned_currency_rank_v5.py","oanda_currency_rank_model.py")))


def _connect(path:Path)->sqlite3.Connection:
    connection=sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro",uri=True,timeout=15)
    connection.row_factory=sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("BEGIN")
    return connection


def _immutable(connection:sqlite3.Connection,tables:tuple[str,...])->None:
    for table in tables:
        definitions=[str(row[0]).lower() for row in connection.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?",(table,))]
        for action in ("update","delete"):
            if not any("before "+action in sql and "raise(abort" in sql.replace(" ","") for sql in definitions):
                raise ValueError("immutable trigger missing:"+table+":"+action)


def check_successor_readiness(
    *, source_config_path:Path=ROOT/"config/source_factor_response_v9.json",
    rank_config_path:Path=ROOT/"config/source_conditioned_currency_rank_v8.json",
    source_database:Path=LOCAL_NEWS/"causal_source_factor_response_map_v9.sqlite",
    rank_database:Path=STATE/"source_conditioned_currency_rank_v8.sqlite",
    source_state_path:Path=LOCAL_NEWS/"causal_source_factor_response_map_latest_v9.json",
    rank_state_path:Path=STATE/"source_conditioned_currency_rank_v8.json",
    now_utc:datetime|None=None, maximum_state_age_sec:float=300.0,
) -> dict[str,Any]:
    now=(now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
    result={"contract":"source_publication_successors_independent_readiness_v1_20260905","checked_utc":now.isoformat(),
            "ok":False,"status":"failed","operationally_ready":False,"prospective_performance_verified":False,
            "research_only":True,"execution_eligible":False,"can_place_orders":False,"can_authorize":False,"can_promote":False,
            "source_contract_id":SOURCE_CONTRACT,"rank_contract_id":RANK_CONTRACT,"failures":[]}
    try:
        source_config,rank_config=_read(source_config_path),_read(rank_config_path)
        if source_config.get("contract_id")!=SOURCE_CONTRACT or source_config.get("required_classification_version")!=CLASSIFIER or source_config.get("historical_source_ledger_import_allowed") is not False:
            raise ValueError("source configuration identity/import violation")
        if rank_config.get("contract_id")!=RANK_CONTRACT or rank_config.get("required_source_contract_id")!=SOURCE_CONTRACT or rank_config.get("historical_rows_imported") is not False:
            raise ValueError("rank configuration identity/import violation")
        for label,config in (("source",source_config),("rank",rank_config)):
            if config.get("research_only") is not True or any(config.get(key) is not False for key in ("can_place_orders","can_authorize","can_promote")):
                raise ValueError(label+" authority configuration violation")
            if type(config.get("collection_enabled")) is not bool:
                raise ValueError(label+" explicit activation disposition missing")
        if rank_config.get("execution_eligible") is not False:
            raise ValueError("rank execution authority configuration violation")
        if source_config["collection_enabled"]!=rank_config["collection_enabled"]:
            raise ValueError("successor collection disposition mismatch")
        if not source_config["collection_enabled"]:
            result.update(ok=True,status="inactive",reason="explicitly_disabled_pending_future_registered_collection",evidence_assessed=False)
            return result

        source_state,rank_state=_read(source_state_path),_read(rank_state_path)
        for label,state,contract in (("source",source_state,SOURCE_CONTRACT),("rank",rank_state,RANK_CONTRACT)):
            if state.get("contract_id")!=contract:
                raise ValueError(label+" state identity mismatch")
            if state.get("status")!="ok":
                raise ValueError(label+" state is not a completed ok publication")
            age=(now-_time(state.get("generated_utc"))).total_seconds()
            if not 0<=age<=maximum_state_age_sec:
                raise ValueError(label+" state missing fresh completed publication")
        policy=source_state.get("policy") or {}
        if policy.get("research_only") is not True or any(policy.get(key) is not False for key in ("can_place_orders","can_authorize","can_promote")):
            raise ValueError("source publication authority violation")
        if rank_state.get("research_only") is not True or any(rank_state.get(key) is not False for key in ("execution_eligible","can_place_orders","can_authorize","can_promote")):
            raise ValueError("rank publication authority violation")
        forecasts={}
        connection=_connect(source_database)
        try:
            if connection.execute("PRAGMA quick_check(1)").fetchone()[0]!="ok": raise ValueError("source database integrity")
            if connection.execute("PRAGMA foreign_key_check").fetchone(): raise ValueError("source database foreign-key integrity")
            _immutable(connection,("source_cohort_activation","source_activation_visibility","source_event_observation","source_event_transport","source_factor_observation","source_event_response","source_forecast_availability","source_factor_forecast"))
            activation=connection.execute("SELECT * FROM source_cohort_activation WHERE singleton=1").fetchone()
            visibility=connection.execute("SELECT registration_committed_utc FROM source_activation_visibility WHERE singleton=1").fetchone()
            cfg_sha=_sha(json.dumps({k:v for k,v in source_config.items() if k!="collection_enabled"},sort_keys=True))
            implementation=_source_implementation()
            if not activation or not visibility or activation["contract_id"]!=SOURCE_CONTRACT or activation["config_sha256"]!=cfg_sha or activation["implementation_sha256"]!=implementation:
                raise ValueError("source activation identity/configuration/source-code binding")
            recorded,committed,activated=_time(activation["recorded_utc"]),_time(visibility[0]),_time(activation["activated_utc"])
            if not recorded<=committed<=activated<=now or recorded>=activated:
                raise ValueError("source activation is backdated or not yet effective")
            cohort="source_v9_"+_digest(SOURCE_CONTRACT,activated.isoformat(),cfg_sha,implementation)[:24]
            if activation["cohort_id"]!=cohort: raise ValueError("source activation cohort hash mismatch")
            rows=connection.execute("""SELECT f.*,e.first_known_utc,sf.factor_known_utc semantic_factor_clock,
                 sf.prospective_proof_eligible factor_prospective,e.prospective_proof_eligible event_prospective,
                 sf.contract_id factor_contract,sf.cohort_id factor_cohort,e.contract_id event_contract,e.cohort_id event_cohort,
                 a.available_utc,a.forecast_payload_sha256,
                 a.availability_contract,a.contract_id receipt_contract,a.cohort_id receipt_cohort
                 FROM source_factor_forecast f JOIN source_event_observation e USING(canonical_event_id)
                 JOIN source_factor_observation sf USING(factor_observation_id)
                 LEFT JOIN source_forecast_availability a USING(forecast_id)""").fetchall()
            if len(rows)!=connection.execute("SELECT COUNT(*) FROM source_factor_forecast").fetchone()[0]:
                raise ValueError("source forecast lacks event or factor lineage")
            for raw in rows:
                row=dict(raw)
                if row["contract_id"]!=SOURCE_CONTRACT or row["cohort_id"]!=cohort: raise ValueError("foreign source forecast lineage")
                # A not-yet-attested forecast is not consumable; no receipt is
                # an incomplete publication, not permission to use its quote.
                if row["available_utc"] is None: continue
                if row["receipt_contract"]!=SOURCE_CONTRACT or row["receipt_cohort"]!=cohort or row["availability_contract"]!=AVAILABILITY:
                    raise ValueError("source availability receipt lineage")
                if _sha(row["forecast_payload_json"])!=row["forecast_payload_sha256"]:
                    raise ValueError("source availability receipt hash")
                payload=json.loads(row["forecast_payload_json"])
                if payload.get("semantic_available_utc")!=row["semantic_factor_clock"]:
                    raise ValueError("source semantic clock does not match immutable factor")
                if (row["factor_contract"],row["event_contract"],row["factor_cohort"],row["event_cohort"])!=(SOURCE_CONTRACT,SOURCE_CONTRACT,cohort,cohort):
                    raise ValueError("source event/factor cohort lineage")
                if row["prospective_proof_eligible"] and (row["factor_prospective"]!=1 or row["event_prospective"]!=1):
                    raise ValueError("source forecast proof lacks eligible factor/event")
                for key in ("forecast_id","canonical_event_id","currency","factor_key","horizon_min","issued_utc","training_cutoff_utc","forecast_state","effective_event_n","probability_strengthening","predicted_currency_factor_bps","predicted_absolute_factor_bps","prospective_proof_eligible"):
                    if row[key]!=payload.get(key): raise ValueError("source forecast row/payload mismatch:"+key)
                first,semantic,cutoff,issued,available=map(_time,(row["first_known_utc"],payload["semantic_available_utc"],row["training_cutoff_utc"],row["issued_utc"],row["available_utc"]))
                if not first<=semantic<=cutoff<=issued<=available<=now: raise ValueError("source semantic/publication chronology")
                if row["prospective_proof_eligible"] and first<activated: raise ValueError("preactivation source credited as prospective")
                forecasts[row["forecast_id"]]=row
        finally:connection.close()

        connection=_connect(rank_database)
        try:
            if connection.execute("PRAGMA quick_check(1)").fetchone()[0]!="ok": raise ValueError("rank database integrity")
            if connection.execute("PRAGMA foreign_key_check").fetchone(): raise ValueError("rank database foreign-key integrity")
            _immutable(connection,("rank_cohort_binding","source_forecast_first_observed","rank_selection_intent","rank_selection_availability","rank_decision","rank_forecast","rank_outcome"))
            binding=connection.execute("SELECT * FROM rank_cohort_binding WHERE singleton=1").fetchone()
            rank_config_sha=_sha(json.dumps({k:v for k,v in rank_config.items() if k!="collection_enabled"},sort_keys=True))
            rank_implementation=_rank_implementation()
            if not binding or (binding["contract_id"],binding["source_cohort_id"],binding["definition_sha256"],binding["config_sha256"])!=(RANK_CONTRACT,cohort,rank_implementation,rank_config_sha):
                raise ValueError("rank immutable cohort/configuration/source-code binding")
            observations={}
            for raw in connection.execute("SELECT * FROM source_forecast_first_observed"):
                receipt=dict(raw);forecast=forecasts.get(receipt["forecast_id"])
                if not forecast or forecast["forecast_state"]!="forecast" or forecast["prospective_proof_eligible"]!=1:
                    raise ValueError("rank observation lacks eligible attested source forecast")
                if (receipt["source_cohort_id"],receipt["source_payload_sha256"],receipt["source_available_utc"],receipt["source_issued_utc"],receipt["contract_id"])!=(cohort,forecast["forecast_payload_sha256"],forecast["available_utc"],forecast["issued_utc"],RANK_CONTRACT):
                    raise ValueError("rank observation source binding")
                observed=_time(receipt["first_observed_utc"])
                if not _time(forecast["available_utc"])<=observed<=now or (observed-_time(forecast["issued_utc"])).total_seconds()>min(float(rank_config["maximum_capture_lag_sec"]),60*forecast["horizon_min"]):
                    raise ValueError("rank consumer first observation chronology/expiry")
                observations[receipt["forecast_id"]]=observed
            intents={}
            for raw in connection.execute("""SELECT i.*,a.selection_committed_utc,a.intent_payload_sha256,a.contract_id receipt_contract
                FROM rank_selection_intent i LEFT JOIN rank_selection_availability a USING(intent_id)"""):
                intent=dict(raw)
                if intent["contract_id"]!=RANK_CONTRACT: raise ValueError("rank selection intent lineage")
                if intent["selection_committed_utc"] is None: continue
                if intent["receipt_contract"]!=RANK_CONTRACT or _sha(intent["intent_payload_json"])!=intent["intent_payload_sha256"]:
                    raise ValueError("rank committed selection intent hash/lineage")
                if not _time(intent["selection_completed_utc"])<=_time(intent["selection_committed_utc"])<=now:
                    raise ValueError("rank committed selection chronology")
                intent["payload"]=json.loads(intent["intent_payload_json"])
                intents[intent["intent_id"]]=intent
            decisions=0;selected=0
            for decision in connection.execute("SELECT * FROM rank_decision"):
                decisions+=1
                if decision["contract_id"]!=RANK_CONTRACT or tuple(decision[key] for key in ("research_only","execution_eligible","can_authorize","can_promote"))!=(1,0,0,0):
                    raise ValueError("rank decision lineage/authority")
                payload=json.loads(decision["source_payload_json"])
                trigger_ids={item for row in payload["trigger_rows"] for item in row["source_forecast_ids"]}
                for forecast in connection.execute("SELECT * FROM rank_forecast WHERE decision_id=?",(decision["decision_id"],)):
                    if forecast["contract_id"]!=RANK_CONTRACT or (forecast["research_only"],forecast["execution_eligible"])!=(1,0):
                        raise ValueError("rank forecast lineage/authority")
                    if not forecast["selected"]: continue
                    selected+=1
                    arm=json.loads(forecast["forecast_payload_json"])
                    intent=intents.get(arm.get("selection_intent_id"))
                    if not intent or (intent["market_episode_id"],intent["horizon_min"])!=(decision["market_episode_id"],decision["horizon_min"]):
                        raise ValueError("selected rank entry lacks a committed frozen selection")
                    selected_arm=next((item for item in intent["payload"]["arms"] if item["arm"]==forecast["arm"]),None)
                    if not selected_arm or not selected_arm["selected"] or (selected_arm["instrument"],selected_arm["direction"])!=(forecast["instrument"],forecast["direction"]):
                        raise ValueError("rank selected pair or side changed after intent")
                    if (intent["payload"]["trigger_rows"],intent["payload"]["source_state"])!=(payload["trigger_rows"],payload["active_currency_source_state"]):
                        raise ValueError("rank selection source evidence changed after intent")
                    required=trigger_ids|set(arm.get("source_forecast_ids") or [])
                    if not required or not required.issubset(observations):
                        raise ValueError("selected rank entry lacks contributing observation receipts")
                    entry=_time(forecast["entry_quote_utc"])
                    if not max(observations[item] for item in required)<=entry<=_time(forecast["issued_utc"]):
                        raise ValueError("selected rank quote precedes observed forecast availability")
                    if entry<=_time(intent["selection_committed_utc"]):
                        raise ValueError("selected rank quote precedes committed rank selection")
                    if arm.get("entry_quote",{}).get("quote_utc")!=forecast["entry_quote_utc"]:
                        raise ValueError("rank entry row/payload clock mismatch")
            outcome_count=0
            for row in connection.execute("""SELECT o.*,f.selected,f.direction,f.entry_bid,f.entry_ask,f.pip
                FROM rank_outcome o JOIN rank_forecast f USING(forecast_id)"""):
                outcome_count+=1
                payload=json.loads(row["outcome_payload_json"])
                if row["contract_id"]!=RANK_CONTRACT or row["selected"]!=1 or (row["research_only"],row["execution_eligible"])!=(1,0):
                    raise ValueError("rank outcome lineage/authority")
                maturity,exit_time,observed,recorded=map(_time,(row["maturity_utc"],row["exit_quote_utc"],payload.get("maturity_observed_utc"),payload.get("recorded_utc")))
                if not maturity<=exit_time<=observed<=recorded<=now:
                    raise ValueError("rank outcome credits an unobserved future exit quote")
                if payload.get("forecast_id")!=row["forecast_id"] or payload.get("exit_quote_utc")!=row["exit_quote_utc"]:
                    raise ValueError("rank outcome payload binding")
                sign=1 if row["direction"]=="buy" else -1
                gross=sign*((row["exit_bid"]+row["exit_ask"])/2-(row["entry_bid"]+row["entry_ask"])/2)/row["pip"]
                executable=((row["exit_bid"]-row["entry_ask"]) if row["direction"]=="buy" else (row["entry_bid"]-row["exit_ask"]))/row["pip"]
                if not all(math.isclose(expected,float(row[key]),abs_tol=1e-6) for key,expected in (("gross_mid_pips",gross),("executable_after_cost_pips",executable),("realized_cost_pips",gross-executable))):
                    raise ValueError("rank executable bid/ask outcome arithmetic")
        finally:connection.close()
        result.update(ok=True,status="collecting_no_performance_proof",operationally_ready=True,evidence_assessed=True,
                      source_cohort_id=cohort,attested_source_forecasts=len(forecasts),consumer_observations=len(observations),rank_decisions=decisions,selected_rank_forecasts=selected,matured_rank_outcomes=outcome_count,
                      reason="clock_and_lineage_checks_passed; statistical_confirmation_not_assessed")
    except (OSError,ValueError,KeyError,TypeError,ArithmeticError,sqlite3.Error) as exc:
        result["failures"]=[str(exc)]
    return result
