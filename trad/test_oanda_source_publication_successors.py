"""Offline V9/V8 integration regressions; every write is in pytest tmp_path."""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest
import oanda_causal_source_factor_response_map_v9 as source
import oanda_source_conditioned_currency_rank_v8 as rank
import oanda_prospective_governance as governance
import oanda_source_publication_successor_integrity as independent

UTC=timezone.utc
T0=datetime(2026,9,8,12,tzinfo=UTC)


def enabled_config(tmp_path:Path,module)->Path:
    default=module.CONFIG_PATH if module is source else module.DEFAULT_MANIFEST
    payload=json.loads(default.read_text(encoding="utf-8"))
    payload["collection_enabled"]=True
    target=tmp_path/("source_config.json" if module is source else "rank_config.json")
    target.write_text(json.dumps(payload),encoding="utf-8")
    return target


def mapping(currency:str,key:str,raw:datetime,mapped:datetime)->dict:
    return {"mapping_id":"map-"+key,"observation_id":"obs-"+key,"source_id":"official","source_contract_id":"raw_contract",
            "first_seen_utc":raw.isoformat(),"mapped_utc":mapped.isoformat(),"classification_version":source.REQUIRED_CLASSIFICATION_VERSION,
            "mapper_contract_id":source.REQUIRED_MAPPER_CONTRACT,"mapper_cohort_id":"mapper_cohort","material_sha256":"fixture-material",
            "observation_clock_source":"fixture-observed","collector_contract_id":"collector","collector_cohort_id":"collector-cohort",
            "currencies":[currency],"mapping_payload":{"event_id":key,"headline":"Official inflation release "+key,
            "market_episode_id":"episode-"+key,"source_url":"https://example.invalid/official/"+key,"source_id":"official",
            "category":"inflation","source_role":"primary_release","currencies":[currency],"direct_currencies":[currency],
            "source_direct":True,"source_verified":True,"relevant":True,"exclusion_reason":""}}


def insert_response(con:sqlite3.Connection,event:dict,sign:int,available:datetime):
    # Only diagnostic training labels. Valid values for the actual V9 schema;
    # no prior ledger rows are copied and no training label is traded directly.
    cols=list(con.execute("PRAGMA table_info(source_event_response)"))
    row={c[1]: ("" if c[2]=="TEXT" else 0) if c[3] else None for c in cols}
    raw=source.base.parse_time(event["first_known_utc"])
    row.update(response_id="response-"+event["canonical_event_id"],canonical_event_id=event["canonical_event_id"],currency=event["currency"],
               horizon_min=5,event_clock_utc=raw.isoformat(),maturity_utc=(raw+timedelta(minutes=5)).isoformat(),
               maturity_state="valid_canonical_ls_factor_and_bid_ask_paths",currency_factor_bps=5.*sign,absolute_currency_factor_bps=5.,
               research_only=1,execution_eligible=0,contract_id=source.CONTRACT_ID,cohort_id=source.base.COHORT_ID)
    con.execute("INSERT INTO source_event_response VALUES ("+",".join("?" for _ in cols)+")",tuple(row[c[1]] for c in cols))
    con.execute("INSERT INTO source_response_availability VALUES (?,?,?)",(row["response_id"],available.isoformat(),source.CONTRACT_ID))


def source_fixture(tmp_path:Path):
    path=tmp_path/"source.sqlite"
    config=enabled_config(tmp_path,source)
    activation=source.register_activation(path,activated_utc=T0-timedelta(minutes=10),config_path=config,clock=lambda:T0-timedelta(minutes=11))
    con=source.open_output_database(path)
    with source.configured(activation,clock=lambda:T0+timedelta(seconds=91)):
        for currency,sign in (("EUR",1),("USD",-1)):
            for index in range(8):
                raw=T0-timedelta(hours=4,minutes=10*index)
                event=source.canonicalize_observations([mapping(currency,f"training-{currency}-{index}",raw,raw+timedelta(seconds=90))],activation_utc=T0-timedelta(minutes=10))[0]
                source.insert_events(con,[event])
                insert_response(con,event,sign,T0-timedelta(seconds=60))
            target=source.canonicalize_observations([mapping(currency,"target-"+currency,T0,T0+timedelta(seconds=90))],activation_utc=T0-timedelta(minutes=10))[0]
            assert target["prospective_proof_eligible"]
            assert all(source.base.parse_time(row["factor_known_utc"])>=T0+timedelta(seconds=90) for row in target["factors"])
            source.insert_events(con,[target])
        con.commit()
    return path,config,activation,con


def quote(path:Path,when:datetime):
    path.write_text(json.dumps({"quotes":{"EUR_USD":{"bid":1.,"ask":1.0001,"pip":.0001,"time":when.isoformat()}}}),encoding="utf-8")


def test_inactive_successors_never_open_or_create_ledgers(tmp_path,monkeypatch):
    def forbidden(*args,**kwargs): raise AssertionError("inactive intake opened")
    monkeypatch.setattr(source,"load_activation",forbidden)
    monkeypatch.setattr(rank,"load_source_forecasts",forbidden)
    assert source.run_cycle(output_database=tmp_path/"source.sqlite")["status"]=="inactive"
    assert rank.run_cycle(ledger_path=tmp_path/"rank.sqlite")["status"]=="inactive"
    assert not list(tmp_path.iterdir())


def test_activation_cannot_backdate_or_reuse_a_cohort(tmp_path):
    path=tmp_path/"source.sqlite"
    with pytest.raises(ValueError,match="precede"):
        source.register_activation(path,activated_utc=T0-timedelta(seconds=1),clock=lambda:T0)
    assert not path.exists()
    source.register_activation(path,activated_utc=T0+timedelta(seconds=1),clock=lambda:T0)
    with pytest.raises(ValueError,match="immutable"):
        source.register_activation(path,activated_utc=T0+timedelta(seconds=2),clock=lambda:T0)
    for activated,clock in ((T0.replace(tzinfo=None),lambda:T0),(T0+timedelta(seconds=1),lambda:T0.replace(tzinfo=None))):
        with pytest.raises(ValueError,match="timezone-aware"):
            source.register_activation(tmp_path/"naive.sqlite",activated_utc=activated,clock=clock)
    with pytest.raises(ValueError,match="follow"):
        source.register_activation(tmp_path/"equal.sqlite",activated_utc=T0,clock=lambda:T0)


def test_foreign_ledger_is_rejected_without_mutation(tmp_path):
    path=tmp_path/"foreign.sqlite"
    con=sqlite3.connect(path)
    con.execute("CREATE TABLE source_event_observation(contract_id TEXT)")
    con.execute("INSERT INTO source_event_observation VALUES ('old-v8')")
    con.commit();con.close()
    before=hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError,match="foreign"):
        source.open_output_database(path)
    assert hashlib.sha256(path.read_bytes()).hexdigest()==before


def test_delayed_attestation_cannot_create_pre_observed_entry_and_fresh_quote_can_trade(tmp_path):
    source_path,_,activation,con=source_fixture(tmp_path)
    clock_value=[T0+timedelta(seconds=91)]
    before_commit=[]
    class DelayedAttestationCommit:
        def __init__(self,inner):self.inner=inner;self.commits=0
        def __getattr__(self,key):return getattr(self.inner,key)
        def commit(self):
            self.commits+=1
            if self.commits==2:
                # Forecasts are committed, but receipts are in this writer's
                # pending transaction and invisible to a separate reader.
                before_commit.extend(rank.load_source_forecasts(source_path))
                clock_value[0]+=timedelta(seconds=11)
            return self.inner.commit()
    try:
        with source.configured(activation,clock=lambda:clock_value[0]):
            source.insert_forecasts(DelayedAttestationCommit(con),clock=lambda:clock_value[0])
    finally:con.close()
    assert before_commit==[]
    rows=rank.load_source_forecasts(source_path)
    assert rows and all(source.base.parse_time(row["issued_utc"])>=T0+timedelta(seconds=91) for row in rows)
    assert all(source.base.parse_time(row["available_utc"])==T0+timedelta(seconds=91) for row in rows)
    config=enabled_config(tmp_path,rank)
    quotes=tmp_path/"quotes.json"
    quote(quotes,T0+timedelta(seconds=92))
    args=dict(config_path=config,source_database=source_path,ledger_path=tmp_path/"rank.sqlite",state_path=tmp_path/"rank.json",report_path=tmp_path/"rank.md",ticker_path=tmp_path/"absent-ticker.json",quotes_path=quotes,quote_bars_database=tmp_path/"absent-quotes.sqlite")
    # First consumer observation after delayed attestation commit: old quote
    # is rejected even though it follows the producer's receipt timestamp.
    first=rank.run_cycle(**args,clock=lambda:T0+timedelta(seconds=103))
    assert first["new_decisions"]==0
    # A later cycle/restart must preserve the original observation and permit
    # a genuinely available fresh entry, rather than permanently abstaining.
    quote(quotes,T0+timedelta(seconds=104))
    second=rank.run_cycle(**args,clock=lambda:T0+timedelta(seconds=105))
    assert second["new_selection_intents"]>0 and second["new_decisions"]==0
    quote(quotes,T0+timedelta(seconds=106))
    third=rank.run_cycle(**args,clock=lambda:T0+timedelta(seconds=107))
    assert third["new_decisions"]>0
    reader=sqlite3.connect(args["ledger_path"])
    try:
        selected=reader.execute("SELECT entry_quote_utc,contract_id FROM rank_forecast WHERE arm='source_only' AND selected=1").fetchall()
        assert selected and all(row[0]==(T0+timedelta(seconds=106)).isoformat() and row[1]==rank.CONTRACT_ID for row in selected)
        assert reader.execute("SELECT MIN(first_observed_utc) FROM source_forecast_first_observed").fetchone()[0]==(T0+timedelta(seconds=103)).isoformat()
        before=reader.execute("SELECT COUNT(*) FROM rank_decision").fetchone()[0]
    finally:reader.close()
    fourth=rank.run_cycle(**args,clock=lambda:T0+timedelta(seconds=108))
    assert fourth["new_decisions"]==0 and fourth["ledger"]["decisions"]==before


def test_stale_or_future_forecasts_never_refresh_their_expiry(tmp_path):
    path,_,activation,con=source_fixture(tmp_path)
    try:
        with source.configured(activation,clock=lambda:T0+timedelta(seconds=91)):
            source.insert_forecasts(con,clock=lambda:T0+timedelta(seconds=91))
    finally:con.close()
    rows=rank.load_source_forecasts(path)
    ledger=rank.open_ledger(tmp_path/"rank.sqlite")
    try:
        assert rank.observe_forecasts(ledger,rows,observed_utc=T0+timedelta(seconds=90),max_capture_lag_sec=120)==[]
        assert rank.observe_forecasts(ledger,rows,observed_utc=T0+timedelta(seconds=212),max_capture_lag_sec=120)==[]
        assert ledger.execute("SELECT COUNT(*) FROM source_forecast_first_observed").fetchone()[0]==0
    finally:ledger.close()


def test_unpublished_forecast_and_corrupt_attestation_fail_closed(tmp_path):
    path,_,activation,con=source_fixture(tmp_path)
    try:
        with source.configured(activation,clock=lambda:T0+timedelta(seconds=91)):
            source.insert_forecasts(con,clock=lambda:T0+timedelta(seconds=91))
        with pytest.raises(sqlite3.IntegrityError,match="append_only"):
            con.execute("UPDATE source_forecast_availability SET available_utc='1900-01-01'")
        con.rollback()
        # Adversarial fixture bypasses immutability solely to verify the
        # independent reader detects a corrupt row/receipt binding.
        con.execute("DROP TRIGGER source_forecast_availability_no_update")
        con.execute("UPDATE source_forecast_availability SET forecast_payload_sha256='bad'")
        con.commit()
        with pytest.raises(ValueError,match="hash mismatch"):
            rank.load_source_forecasts(path)
    finally:con.close()


def test_registration_rejects_backdate_and_changed_frozen_definition(tmp_path):
    database=tmp_path/"governance.sqlite"
    con=sqlite3.connect(database)
    governance.initialize_governance_tables(con)
    cell={"cell_id":"candidate","family":"fixture","instrument":"EUR_USD","horizon_sec":300,"session":"london","liquidity_bucket":"liquid_le_2","minimum_economic_edge_pips":1.,"sequential_method":"fixed_method","sequential_clip_bound_pips":20.}
    inference={"graduation_ladder":{"B_discovery_candidate":{"cell_ids":["candidate"]}},"candidate_definitions":{"candidate":governance.candidate_definition(cell)}}
    con.execute("INSERT INTO immutable_governance_snapshots VALUES (?,?,?,?,?)",("sha",T0.isoformat(),10,"cfg",json.dumps(inference)))
    con.commit();con.close()
    with pytest.raises(ValueError,match="definition"):
        governance.lock_discovery_candidate(database,governance_sha256="sha",cell={**cell,"instrument":"USD_JPY"},observed_utc=(T0+timedelta(seconds=1)).isoformat())
    with pytest.raises(ValueError,match="precede"):
        governance.lock_discovery_candidate(database,governance_sha256="sha",cell=cell,observed_utc=(T0-timedelta(seconds=1)).isoformat())
    with pytest.raises(ValueError,match="equal"):
        governance.lock_discovery_candidate(database,governance_sha256="sha",cell=cell,locked_utc=(T0-timedelta(days=30)).isoformat(),observed_utc=(T0+timedelta(seconds=1)).isoformat())
    lock=governance.lock_discovery_candidate(database,governance_sha256="sha",cell=cell,observed_utc=(T0+timedelta(seconds=1)).isoformat())
    with pytest.raises(ValueError,match="sample starts"):
        governance.open_confirmation_cohort(database,candidate_lock_id=lock,start_utc=(T0+timedelta(seconds=2)).isoformat(),observed_utc=(T0+timedelta(seconds=3)).isoformat())
    assert governance.open_confirmation_cohort(database,candidate_lock_id=lock,start_utc=(T0+timedelta(seconds=4)).isoformat(),observed_utc=(T0+timedelta(seconds=3)).isoformat()).startswith("confirmation_")


def test_independent_inactive_disposition_does_not_claim_evidence_or_live_readiness():
    result=independent.check_successor_readiness(now_utc=T0)
    assert result["ok"] and result["status"]=="inactive"
    assert not result["operationally_ready"] and not result["prospective_performance_verified"] and not result["evidence_assessed"]


def test_full_source_runner_and_independent_post_fix_clock_verifier(tmp_path,monkeypatch):
    path,config,activation,con=source_fixture(tmp_path)
    con.close()
    # The fixture already contains same-cohort observations and diagnostic
    # training responses. An empty incremental input read exercises the full
    # producer cycle without ever opening a production input database.
    monkeypatch.setattr(source,"_BASE_LOAD",lambda *args,**kwargs:[])
    source_state=tmp_path/"source.json"
    published=source.run_cycle(config_path=config,output_database=path,snapshot_path=source_state,report_path=tmp_path/"source.md",clock=lambda:T0+timedelta(seconds=91),
                               mapping_database=tmp_path/"absent-map.sqlite",raw_database=tmp_path/"absent-raw.sqlite",candle_root=tmp_path/"absent-candles",quote_path=tmp_path/"absent-quotes.json",technical_path=tmp_path/"absent-technical.json")
    assert published["status"]=="ok" and published["contract_id"]==source.CONTRACT_ID
    assert published["counts"]["preactivation_diagnostic_events"]==16
    assert published["counts"]["prospective_proof_forecasts"]>0
    reader=sqlite3.connect(path)
    try:
        assert reader.execute("SELECT COUNT(*) FROM source_forecast_availability").fetchone()[0]==published["counts"]["forecasts"]
    finally:reader.close()
    rank_config=enabled_config(tmp_path,rank)
    quotes=tmp_path/"quotes.json"
    args=dict(config_path=rank_config,source_database=path,ledger_path=tmp_path/"rank.sqlite",state_path=tmp_path/"rank.json",report_path=tmp_path/"rank.md",ticker_path=tmp_path/"absent-ticker.json",quotes_path=quotes,quote_bars_database=tmp_path/"absent-quote-bars.sqlite")
    quote(quotes,T0+timedelta(seconds=10))
    assert rank.run_cycle(**args,clock=lambda:T0+timedelta(seconds=92))["new_decisions"]==0
    quote(quotes,T0+timedelta(seconds=93))
    assert rank.run_cycle(**args,clock=lambda:T0+timedelta(seconds=94))["new_selection_intents"]>0
    quote(quotes,T0+timedelta(seconds=95))
    assert rank.run_cycle(**args,clock=lambda:T0+timedelta(seconds=96))["new_decisions"]>0
    check=dict(source_config_path=config,rank_config_path=rank_config,source_database=path,rank_database=args["ledger_path"],source_state_path=source_state,rank_state_path=args["state_path"],now_utc=T0+timedelta(seconds=97))
    verified=independent.check_successor_readiness(**check)
    assert verified["ok"],verified
    assert verified["selected_rank_forecasts"]>0 and not verified["prospective_performance_verified"]
    # The verifier is independent: corrupt only the persisted selected quote
    # and ensure it rejects it, without rerunning either producer helper.
    writer=sqlite3.connect(args["ledger_path"])
    try:
        writer.execute("DROP TRIGGER rank_forecast_no_update")
        writer.execute("UPDATE rank_forecast SET entry_quote_utc=? WHERE selected=1",((T0+timedelta(seconds=93)).isoformat(),))
        writer.commit()
        writer.execute("CREATE TRIGGER rank_forecast_no_update BEFORE UPDATE ON rank_forecast BEGIN SELECT RAISE(ABORT,'rank_forecast is append-only'); END")
        writer.commit()
    finally:writer.close()
    broken=independent.check_successor_readiness(**check)
    assert not broken["ok"] and "precedes committed rank selection" in broken["failures"][0],broken


def test_enabled_successors_require_complete_publication_and_activation(tmp_path):
    source_config=enabled_config(tmp_path,source)
    rank_config=enabled_config(tmp_path,rank)
    result=independent.check_successor_readiness(source_config_path=source_config,rank_config_path=rank_config,source_database=tmp_path/"none.sqlite",rank_database=tmp_path/"no-rank.sqlite",source_state_path=tmp_path/"none.json",rank_state_path=tmp_path/"no-rank.json",now_utc=T0)
    assert not result["ok"] and not result["operationally_ready"]


@pytest.mark.parametrize("bad_value",[0,1,"false",None])
def test_collection_disposition_requires_boolean(tmp_path,bad_value):
    source_config=enabled_config(tmp_path,source)
    data=json.loads(source_config.read_text())
    data["collection_enabled"]=bad_value
    source_config.write_text(json.dumps(data))
    with pytest.raises(ValueError,match="boolean"):
        source.read_config(source_config)
    assert not independent.check_successor_readiness(source_config_path=source_config,now_utc=T0)["ok"]


def test_readiness_rejects_rank_authority_and_fresh_error_publication(tmp_path):
    source_config=enabled_config(tmp_path,source)
    rank_config=enabled_config(tmp_path,rank)
    data=json.loads(rank_config.read_text())
    data["execution_eligible"]=True
    rank_config.write_text(json.dumps(data))
    assert not independent.check_successor_readiness(source_config_path=source_config,rank_config_path=rank_config,now_utc=T0)["ok"]
    rank_config=enabled_config(tmp_path,rank)
    for name,contract in (("source",source.CONTRACT_ID),("rank",rank.CONTRACT_ID)):
        (tmp_path/(name+".json")).write_text(json.dumps({"contract_id":contract,"generated_utc":T0.isoformat(),"status":"building"}))
    result=independent.check_successor_readiness(source_config_path=source_config,rank_config_path=rank_config,source_state_path=tmp_path/"source.json",rank_state_path=tmp_path/"rank.json",now_utc=T0)
    assert not result["ok"] and "completed ok" in result["failures"][0]


@pytest.mark.parametrize("delay_stage",["consumer_commit","selection_calculation","intent_commit"])
def test_slow_rank_work_cannot_backdate_selection_or_fill(tmp_path,monkeypatch,delay_stage):
    path,_,activation,con=source_fixture(tmp_path)
    try:
        with source.configured(activation,clock=lambda:T0+timedelta(seconds=91)):
            source.insert_forecasts(con,clock=lambda:T0+timedelta(seconds=91))
    finally:con.close()
    clock=[T0+timedelta(seconds=92)]
    if delay_stage=="consumer_commit":
        original=rank.observe_forecasts
        def delayed(*args,**kwargs):
            result=original(*args,**kwargs)
            clock[0]+=timedelta(seconds=11)
            return result
        monkeypatch.setattr(rank,"observe_forecasts",delayed)
    elif delay_stage=="selection_calculation":
        original=rank.base.build_source_only_arm
        def delayed(*args,**kwargs):
            result=original(*args,**kwargs)
            clock[0]+=timedelta(seconds=11)
            return result
        monkeypatch.setattr(rank.base,"build_source_only_arm",delayed)
    else:
        original=rank.publish_selection_availability
        def delayed(*args,**kwargs):
            clock[0]+=timedelta(seconds=11)
            return original(*args,**kwargs)
        monkeypatch.setattr(rank,"publish_selection_availability",delayed)
    config=enabled_config(tmp_path,rank)
    quotes=tmp_path/"quotes.json"
    quote(quotes,T0+timedelta(seconds=92))
    args=dict(config_path=config,source_database=path,ledger_path=tmp_path/"rank.sqlite",state_path=tmp_path/"rank.json",report_path=tmp_path/"rank.md",ticker_path=tmp_path/"absent-ticker.json",quotes_path=quotes,quote_bars_database=tmp_path/"absent-bars.sqlite",clock=lambda:clock[0])
    first=rank.run_cycle(**args)
    assert first["new_selection_intents"]>0 and first["new_decisions"]==0
    reader=sqlite3.connect(args["ledger_path"])
    try:
        assert reader.execute("SELECT COUNT(*) FROM rank_forecast").fetchone()[0]==0
        assert source.base.parse_time(reader.execute("SELECT MIN(selection_committed_utc) FROM rank_selection_availability").fetchone()[0])>=T0+timedelta(seconds=103)
    finally:reader.close()
    next_quote=clock[0]+timedelta(seconds=1)
    quote(quotes,next_quote)
    clock[0]+=timedelta(seconds=2)
    second=rank.run_cycle(**args)
    assert second["new_decisions"]>0
    reader=sqlite3.connect(args["ledger_path"])
    try:
        rows=reader.execute("SELECT entry_quote_utc,issued_utc FROM rank_forecast WHERE selected=1").fetchall()
        assert rows and all(source.base.parse_time(row[0])==next_quote and source.base.parse_time(row[1])>=next_quote for row in rows)
    finally:reader.close()


@pytest.mark.parametrize("exit_delay_sec",[0,60])
def test_fixed_selection_restart_matures_executable_bid_ask_without_reranking(tmp_path,monkeypatch,exit_delay_sec):
    test_delayed_attestation_cannot_create_pre_observed_entry_and_fresh_quote_can_trade(tmp_path)
    reader=sqlite3.connect(tmp_path/"rank.sqlite")
    try:
        selected=reader.execute("SELECT forecast_id,instrument,direction,maturity_utc FROM rank_forecast WHERE arm='source_only' AND selected=1").fetchall()
        assert selected and all(row[1:3]==("EUR_USD","buy") for row in selected)
        intents_before=reader.execute("SELECT intent_id,intent_payload_json FROM rank_selection_intent ORDER BY intent_id").fetchall()
    finally:reader.close()
    target=max(source.base.parse_time(row[3]) for row in selected)
    exit_time=target+timedelta(seconds=exit_delay_sec)
    bars=tmp_path/"absent_quotes.sqlite"
    writer=sqlite3.connect(bars)
    writer.execute("CREATE TABLE quote_bars(instrument TEXT,last_epoch REAL,close_bid REAL,close_ask REAL,pip REAL)")
    writer.execute("INSERT INTO quote_bars VALUES (?,?,?,?,?)",("EUR_USD",exit_time.timestamp(),1.0011,1.0012,.0001))
    writer.commit();writer.close()
    def forbidden(*args,**kwargs):raise AssertionError("frozen intent was reranked")
    for name in ("build_price_only_arm","build_source_only_arm","build_source_price_timing_arm"):
        monkeypatch.setattr(rank.base,name,forbidden)
    args=dict(config_path=tmp_path/"rank_config.json",source_database=tmp_path/"source.sqlite",ledger_path=tmp_path/"rank.sqlite",state_path=tmp_path/"rank.json",report_path=tmp_path/"rank.md",ticker_path=tmp_path/"absent-ticker.json",quotes_path=tmp_path/"quotes.json",quote_bars_database=bars)
    matured=rank.run_cycle(**args,clock=lambda:target+timedelta(seconds=1))
    if exit_delay_sec:
        assert matured["matured_outcomes"]==0
        matured=rank.run_cycle(**args,clock=lambda:exit_time+timedelta(seconds=1))
    assert matured["matured_outcomes"]==len(selected)
    reader=sqlite3.connect(args["ledger_path"])
    try:
        outcomes=reader.execute("SELECT forecast_id,executable_after_cost_pips,realized_cost_pips,contract_id FROM rank_outcome").fetchall()
        assert {row[0] for row in outcomes}=={row[0] for row in selected}
        assert all(row[1]==pytest.approx(10.) and row[2]==pytest.approx(1.) and row[3]==rank.CONTRACT_ID for row in outcomes)
        assert reader.execute("SELECT intent_id,intent_payload_json FROM rank_selection_intent ORDER BY intent_id").fetchall()==intents_before
    finally:reader.close()
    assert rank.run_cycle(**args,clock=lambda:exit_time+timedelta(seconds=2))["matured_outcomes"]==0
