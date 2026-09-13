"""Isolated worker boundaries; broker/network access and live paths are forbidden."""
from copy import deepcopy
from concurrent.futures import Future
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import socket

import pytest

import oanda_pair_local_forecast_study_v1 as worker
from test_oanda_causal_forecast_ledger_pair_v1 import (
    Clock, completed, contract_fixture, digest, issued, minimal_capture_validation_seam, opened, ready_attempt,
)

NOW = 1_800_000_000.0


def stamp(epoch=NOW):
    return datetime.fromtimestamp(epoch,timezone.utc).isoformat()


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args,**kwargs):
        raise AssertionError("Worker fixture may not make network calls")
    monkeypatch.setattr(socket,"create_connection",forbidden)
    monkeypatch.setattr(socket.socket,"connect",forbidden)
    import requests
    monkeypatch.setattr(requests.sessions.Session,"request",forbidden)


@pytest.fixture
def snapshot(tmp_path):
    payload = {"producer":"practice_007_dedicated_quote_stream","generated_utc":stamp(),
               "coverage":{"retained_last_known_instruments":[]},"quotes":{"EUR_USD":{
                   "instrument":"EUR_USD","source":"stream","tradeable":True,"time":stamp(NOW-1),
                   "pip":.0001,"bid":"1.16234567890123456789","ask":"1.16244567890123456789"}}}
    path = tmp_path/"quotes.json"
    def save():
        path.write_text(json.dumps(payload),encoding="utf-8")
    save()
    return payload,path,save


def test_snapshot_uses_actual_observation_clock_and_preserves_exact_prices(snapshot):
    payload,path,_ = snapshot
    parsed,observed,sha = worker.read_snapshot(path,clock=lambda:NOW+2)
    assert observed == NOW+2
    assert sha == hashlib.sha256(path.read_bytes()).hexdigest()
    quote = worker.pair_quote(parsed,observed,sha,"EUR_USD",.0001)
    assert quote["available_epoch"] == NOW+2
    assert quote["market_epoch"] == NOW-1
    assert quote["bid"] == payload["quotes"]["EUR_USD"]["bid"]
    assert quote["ask"] == payload["quotes"]["EUR_USD"]["ask"]
    assert quote["source_snapshot_sha256"] == sha


@pytest.mark.parametrize("field,value",[("producer","other"),("generated_utc",stamp(NOW+1)),
    ("generated_utc",stamp(NOW-61)),("generated_utc","2027-01-15T08:00:00"),
    ("coverage",{}),("quotes",[]),("coverage",{"retained_last_known_instruments":"EUR_USD"})])
def test_snapshot_rejects_invalid_identity_clock_and_shape(snapshot,field,value):
    snapshot[0][field] = value
    snapshot[2]()
    with pytest.raises((ValueError,TypeError,KeyError)):
        worker.read_snapshot(snapshot[1],clock=lambda:NOW)


def test_snapshot_read_is_bounded(snapshot):
    snapshot[1].write_bytes(b" "*262145)
    with pytest.raises(ValueError,match="size_limit"):
        worker.read_snapshot(snapshot[1],clock=lambda:NOW)


@pytest.mark.parametrize("body",[b"{",b"[]",b"null"])
def test_malformed_snapshot_never_yields_a_quote(snapshot,body):
    snapshot[1].write_bytes(body)
    with pytest.raises((ValueError,TypeError)):
        worker.read_snapshot(snapshot[1],clock=lambda:NOW)


@pytest.mark.parametrize("field,value",[("source","retained"),("tradeable",False),("tradeable",1),
    ("time",stamp(NOW+1)),("time",stamp(NOW-61)),("pip",.01),
    ("bid","NaN"),("ask","Infinity")])
def test_pair_quote_rejects_stale_future_retained_or_wrong_pip_data(snapshot,field,value):
    snapshot[0]["quotes"]["EUR_USD"][field] = value
    with pytest.raises((ValueError,TypeError,KeyError)):
        worker.pair_quote(snapshot[0],NOW,"f"*64,"EUR_USD",.0001)


def test_retained_coverage_overrides_plausible_fresh_pair_fields(snapshot):
    snapshot[0]["coverage"]["retained_last_known_instruments"] = ["EUR_USD"]
    with pytest.raises(ValueError,match="retained"):
        worker.pair_quote(snapshot[0],NOW,"f"*64,"EUR_USD",.0001)


def test_missing_pair_not_substituted_from_another_pair(snapshot):
    with pytest.raises(ValueError,match="no_pair_quote"):
        worker.pair_quote(snapshot[0],NOW,"f"*64,"USD_JPY",.01)


@pytest.mark.parametrize("pair,pip",[("AUD_JPY",.01),("HKD_JPY",.0001),("USD_HUF",.01)])
def test_quote_uses_frozen_metadata_even_for_unusual_pair_precision(snapshot,pair,pip):
    snapshot[0]["quotes"] = {pair:{**snapshot[0]["quotes"]["EUR_USD"],"instrument":pair,"pip":pip}}
    quote = worker.pair_quote(snapshot[0],NOW,"f"*64,pair,pip)
    assert quote["pip_size"] == pip
    assert quote["instrument"] == pair


def registry_fixture(tmp_path,monkeypatch):
    bindings = {}
    for name in worker.REQUIRED_SOURCE_BINDINGS:
        raw = ("# fixture: "+name+"\n").encode()
        (tmp_path/name).write_bytes(raw)
        bindings[name] = hashlib.sha256(raw).hexdigest()
    monkeypatch.setattr(worker,"ROOT",tmp_path)
    dependencies = {"python":worker.platform.python_version(),
                    **{package:worker.importlib.metadata.version(package) for package in ("numpy","scikit-learn")}}
    pairs = {}
    for pair,pip in (("EUR_USD",.0001),("HKD_JPY",.0001)):
        contract = contract_fixture(pair,pip)
        contract.update(source_bindings=bindings,dependency_versions=dependencies,
                        numeric_model_source_sha256=bindings["oanda_pair_local_models_v1.py"])
        pairs[pair] = {"pip_size":pip,"contract":contract,"contract_sha256":digest(contract)}
    registry = {"schema_version":worker.REGISTRY_SCHEMA,"registry_id":"pair_local_forecast_study_v1_20260907",
                "collection_enabled":True,"research_only":True,"source_bindings":bindings,
                "dependency_versions":dependencies,"pairs":pairs,
                **{flag:False for flag in ("can_place_orders","can_promote","can_authorize","account_eligible","proof_eligible","historical_rows_imported")}}
    path = tmp_path/"registry.json"
    path.write_text(json.dumps(registry))
    return registry,path


def test_registry_binds_distinct_pairs_cohorts_sources_and_dependencies(tmp_path,monkeypatch):
    registry,path = registry_fixture(tmp_path,monkeypatch)
    assert worker.load_registry(path) == registry


@pytest.mark.parametrize("case",["source_changed","source_missing","dependency_changed","contract_changed","pair_pip_changed","orders_enabled"])
def test_invalid_registry_fails_before_activation(tmp_path,monkeypatch,case):
    registry,path = registry_fixture(tmp_path,monkeypatch)
    if case=="source_changed":(tmp_path/"oanda_pair_local_models_v1.py").write_text("# changed\n")
    if case=="source_missing":registry["source_bindings"].pop("oanda_pair_local_models_v1.py")
    if case=="dependency_changed":registry["dependency_versions"]["numpy"]="invalid"
    if case=="contract_changed":registry["pairs"]["EUR_USD"]["contract"]["contract_id"]="changed"
    if case=="pair_pip_changed":registry["pairs"]["HKD_JPY"]["pip_size"]=.01
    if case=="orders_enabled":registry["can_place_orders"]=True
    path.write_text(json.dumps(registry))
    with pytest.raises(ValueError):
        worker.load_registry(path)


def test_nonempty_scorecard_keeps_real_mature_decisions_and_exact_prices(opened,tmp_path):
    ledger,clock,contract = opened
    completed(ledger,clock)
    path = tmp_path/"scorecard.json"
    report = worker.write_scorecard(ledger,path)
    assert report["coverage"]["paired_scored_decisions"] == 1
    assert report["collection_counts"]["outcomes"] == 1
    saved = json.loads(path.read_text())
    assert saved["study_contract_sha256"] == digest(contract)
    assert saved["coverage"]["paired_scored_decisions"] == 1
    assert "uncalibrated" in saved["interpretation"]


def test_atomic_json_never_replaces_destination_after_serialization_failure(tmp_path):
    path = tmp_path/"summary.json"
    worker.atomic_json(path,{"old":True})
    original = path.read_bytes()
    with pytest.raises(ValueError):
        worker.atomic_json(path,{"bad":float("nan")})
    assert path.read_bytes() == original
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("field,value",[("bid",0),("ask",0),("bid",-1),("bid",2),("instrument","USD_JPY")])
def test_quote_boundary_rejects_crossed_nonpositive_and_other_pair_prices(snapshot,field,value):
    snapshot[0]["quotes"]["EUR_USD"][field] = value
    with pytest.raises(ValueError):
        worker.pair_quote(snapshot[0],NOW,"f"*64,"EUR_USD",.0001)


@pytest.mark.parametrize("clock",[True,float("nan"),float("inf")])
def test_snapshot_observation_clock_must_be_finite_nonboolean(snapshot,clock):
    with pytest.raises(ValueError):
        worker.read_snapshot(snapshot[1],clock=lambda:clock)


def test_retained_coverage_rejects_nested_objects(snapshot):
    snapshot[0]["coverage"]["retained_last_known_instruments"]=[{}]
    snapshot[2]()
    with pytest.raises(ValueError):
        worker.read_snapshot(snapshot[1],clock=lambda:NOW)


def test_verified_publication_reads_committed_original_target_and_reference(opened):
    ledger,clock,contract = opened
    decision,reference,_,_ = issued(ledger,clock)
    publication = worker.verified_publication(ledger)
    assert publication["publication_verified"] is True
    assert publication["decision_id"] == decision
    assert publication["target_epoch"] == reference["market_epoch"]+3600
    assert publication["reference_available_epoch"] == reference["available_epoch"]
    assert publication["publication_verified_epoch"] == clock.value
    assert all(arm["instrument"]==contract["instrument"] for arm in publication["forecasts"])


@pytest.mark.parametrize("case",["hash","publication_sha","pair","target","cohort","reference_mid","publication_future"])
def test_ro_summary_verification_rejects_tampered_receipts(opened,monkeypatch,case):
    ledger,clock,_ = opened
    issued(ledger,clock)
    original_read = ledger._read
    def tampered(sql,*args):
        rows=[dict(row) for row in original_read(sql,*args)]
        payload=json.loads(rows[0]["payload"])
        if case=="hash":rows[0]["sha"]="b"*64
        if case=="publication_sha":rows[0]["forecast_sha"]="b"*64
        if case=="publication_future":rows[0]["published"]=clock.value+1
        if case=="pair":payload["instrument"]="AAA_BBB"
        if case=="target":payload["target_epoch"]+=1
        if case=="cohort":payload["forecasts"][0]["cohort_id"]="different.cohort"
        if case=="reference_mid":payload["forecasts"][0]["reference_mid"]="1.11"
        if case in {"pair","target","cohort","reference_mid"}:
            rows[0]["sha"]=rows[0]["forecast_sha"]=digest(payload)
            rows[0]["payload"]=json.dumps(payload)
        return rows
    monkeypatch.setattr(ledger,"_read",tampered)
    with pytest.raises(ValueError):
        worker.verified_publication(ledger)


class ManualPool:
    def __init__(self,**kwargs):
        self.calls=[]
    def submit(self,function,*args):
        future=Future()
        self.calls.append((function,args,future))
        return future
    def shutdown(self,**kwargs):
        for _,_,future in self.calls:
            if not future.done():future.cancel()


@pytest.fixture
def runner(tmp_path,monkeypatch,snapshot):
    project=tmp_path/"trad"
    project.mkdir()
    registry,_=registry_fixture(project,monkeypatch)
    study=project/"data/oanda_training_manager/pair_local_forecast_study_v1"
    clock=Clock(NOW)
    for pair,item in registry["pairs"].items():
        ledger=worker.CausalForecastLedger(study/"pairs"/pair/"study.sqlite",item["contract"],clock=clock,activate=True)
        ledger.close()
    monkeypatch.setattr(worker,"ThreadPoolExecutor",ManualPool)
    instance=worker.PairRunner(registry,study,project/"candles",snapshot[1],clock=clock)
    clock.advance(2)
    try:
        yield instance,clock,snapshot,registry,project
    finally:
        instance.close()


def add_hkd_quote(snapshot,clock):
    snapshot[0]["generated_utc"]=stamp(clock.value)
    snapshot[0]["quotes"]["EUR_USD"]["time"]=stamp(clock.value)
    snapshot[0]["quotes"]["HKD_JPY"]={**snapshot[0]["quotes"]["EUR_USD"],"instrument":"HKD_JPY","pip":.0001}
    snapshot[2]()


def test_one_slow_pair_fit_does_not_stop_quote_observations_or_settlement(runner,monkeypatch):
    instance,clock,snapshot,_,_=runner
    settlements=[]
    for pair,state in instance.states.items():
        original=state["ledger"].settle
        monkeypatch.setattr(state["ledger"],"settle",lambda pair=pair,original=original:(settlements.append(pair),original())[1])
    instance.tick()
    pending=instance.future
    assert pending is not None and not pending.done()
    first_count=instance.states["EUR_USD"]["ledger"].db.execute("SELECT COUNT(*) FROM quotes").fetchone()[0]
    clock.advance(3)
    add_hkd_quote(snapshot,clock)
    instance.tick()
    assert instance.future is pending and not pending.done()
    assert instance.states["EUR_USD"]["ledger"].db.execute("SELECT COUNT(*) FROM quotes").fetchone()[0]>first_count
    assert set(instance.current)=={"EUR_USD","HKD_JPY"}
    assert settlements.count("EUR_USD")==2 and settlements.count("HKD_JPY")==2
    assert len(instance.fit_pool.calls)==1


def test_pair_failures_keep_independent_cadence_and_next_pair_runs(runner):
    instance,clock,snapshot,_,_=runner
    add_hkd_quote(snapshot,clock)
    instance.poll_quotes();instance.schedule_fit()
    assert instance.active[0]=="EUR_USD"
    instance.future.set_exception(ValueError("fixture_model_failure"))
    instance.finish_fit()
    assert instance.errors==1
    assert "fixture_model_failure" in instance.states["EUR_USD"]["reason"]
    assert instance.states["EUR_USD"]["ledger"].db.execute("SELECT COUNT(*) FROM diagnostics").fetchone()[0]==1
    instance.schedule_fit()
    assert instance.active[0]=="HKD_JPY"
    assert instance.states["HKD_JPY"]["ledger"].db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0]==1


def test_stale_reference_never_reserves_an_attempt(runner):
    instance,clock,_,_,_=runner
    instance.poll_quotes()
    clock.advance(61)
    instance.schedule_fit()
    assert instance.future is None
    assert all(state["ledger"].db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0]==0 for state in instance.states.values())


def test_last_attempt_warmup_not_retried_in_same_pair_bucket(runner):
    instance,clock,_,_,_=runner
    instance.poll_quotes();instance.schedule_fit()
    instance.future.set_result(({"status":"abstain","reasons":["current_common_warmup:2<61"]},
                               {"status":"abstain","reasons":["current_common_warmup:2<61"]}))
    instance.finish_fit();instance.schedule_fit()
    assert instance.future is None
    assert instance.states["EUR_USD"]["current_common_bars"]==2
    assert instance.states["EUR_USD"]["ledger"].db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0]==1
    instance.publish_status(force=True)
    assert instance.summary["rows"][0]["status"]=="warming"


def test_worker_publication_and_dashboard_consumer_agree(runner):
    import oanda_practice_live_dashboard as dashboard
    instance,clock,_,registry,project=runner
    config=project/"config/pair_local_forecast_study_v1_20260907.json"
    config.parent.mkdir()
    config.write_text(json.dumps(registry))
    ledger=instance.states["EUR_USD"]["ledger"]
    issued(ledger,clock)
    instance.states["EUR_USD"]["publication"]=worker.verified_publication(ledger)
    instance.publish_status(force=True)
    result=dashboard.summarize_pair_local_forecasts(instance.study.parent,now_epoch=clock.value)
    assert result["status"]=="current",result
    assert result["counts"]["forecast"]==1,result
    assert result["rows"][0]["latest_published_forecast"]["target_epoch"]==NOW+3603


def test_new_summary_reverifies_cached_publication_and_withholds_on_verification_failure(runner,monkeypatch):
    instance,clock,_,_,_=runner
    ledger=instance.states["EUR_USD"]["ledger"]
    issued(ledger,clock)
    instance.states["EUR_USD"]["publication"]=worker.verified_publication(ledger)
    instance.publish_status(force=True)
    assert instance.summary["rows"][0]["latest_published_forecast"] is not None
    original=worker.verified_publication
    def check(current):
        if current is ledger:raise ValueError("fixture_changed_receipt")
        return original(current)
    monkeypatch.setattr(worker,"verified_publication",check)
    clock.advance(15)
    instance.publish_status(force=True)
    assert instance.summary["rows"][0]["latest_published_forecast"] is None


def test_finished_fit_commits_and_consumes_before_any_later_quote_entry(runner):
    instance,clock,_,_,_=runner
    state=instance.states["EUR_USD"]
    ledger=state["ledger"]
    bucket,reference,capture,result=ready_attempt(ledger,clock)
    state["building"]=True
    instance.active=("EUR_USD",bucket)
    instance.future=Future()
    instance.future.set_result((capture,result))
    instance.finish_fit()
    assert instance.future is None and instance.active is None
    assert state["building"] is False
    counts=ledger.counts()
    assert counts["forecasts"]==counts["publication"]==counts["consumption"]==1
    assert counts["entries"]==0
    assert state["publication"]["target_epoch"]==reference["market_epoch"]+3600


def test_score_worker_only_schedules_changed_mature_outcomes(runner):
    instance,clock,_,_,_=runner
    instance.score()
    assert instance.score_future is None
    ledger=instance.states["EUR_USD"]["ledger"]
    completed(ledger,clock)
    instance.score()
    assert instance.score_pair=="EUR_USD"
    pending=instance.score_future
    instance.score()
    assert instance.score_future is pending
    pending.set_result({"collection_counts":{"outcomes":1}})
    instance.score()
    assert instance.score_future is None
    clock.advance(900)
    instance.score()
    assert instance.score_future is None


def test_failed_fitter_submission_does_not_leave_a_phantom_build(runner,monkeypatch):
    instance,_,_,_,_=runner
    def submit(*args,**kwargs):
        raise RuntimeError("fixture_submit_failed")
    monkeypatch.setattr(instance.fit_pool,"submit",submit)
    instance.poll_quotes();instance.schedule_fit()
    assert instance.future is None and instance.active is None
    assert instance.states["EUR_USD"]["building"] is False
    assert instance.states["EUR_USD"]["ledger"].counts()["attempts"]==1


def test_once_observes_without_reserving_and_abandoning_model_attempts(runner,monkeypatch):
    instance,_,_,registry,_=runner
    monkeypatch.setattr(worker,"load_registry",lambda path:registry)
    monkeypatch.setattr(worker,"PairRunner",lambda *args,**kwargs:instance)
    monkeypatch.setattr(worker,"STUDY",instance.study)
    original_close=instance.close
    monkeypatch.setattr(instance,"close",lambda:None)
    try:
        worker.run(once=True)
        assert instance.states["EUR_USD"]["ledger"].counts()["quotes"]==1
        assert all(state["ledger"].counts()["attempts"]==0 for state in instance.states.values())
        assert instance.future is None
        assert (instance.study/"summary.json").is_file()
    finally:
        monkeypatch.setattr(instance,"close",original_close)
