"""Bounded publication-consumer fixtures: no live runtime, database or broker calls."""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess

import pytest

import oanda_practice_live_dashboard as dashboard

NOW = 1788800400.0
FLAGS = {"research_only":True,"can_place_orders":False,"can_promote":False,
         "can_authorize":False,"account_eligible":False,"proof_eligible":False,"historical_rows_imported":False}
FAMILIES = ("ridge_return_repaired","probabilistic_state_space")


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",", ":")).encode()).hexdigest()


@pytest.fixture(autouse=True)
def no_external_access(monkeypatch):
    def forbidden(*args,**kwargs):
        raise AssertionError("Dashboard publication fixtures forbid databases and network")
    monkeypatch.setattr(sqlite3,"connect",forbidden)
    monkeypatch.setattr(socket,"create_connection",forbidden)
    monkeypatch.setattr(socket.socket,"connect",forbidden)
    import requests
    monkeypatch.setattr(requests.sessions.Session,"request",forbidden)


@pytest.fixture
def publication(tmp_path):
    project = tmp_path/"trad"
    root = project/"data/oanda_training_manager"
    root.mkdir(parents=True)
    source = b"# immutable fixture source\n"
    bindings = {name:hashlib.sha256(source).hexdigest() for name in dashboard._PAIR_FORECAST_SOURCE_BINDINGS}
    for name in bindings:
        (project/name).write_bytes(source)
    pairs = {}
    for pair in ("EUR_USD","GBP_USD","USD_JPY"):
        contract = {**FLAGS,"instrument":pair,"pip_size":.01 if pair.endswith("JPY") else .0001,
                    "cohorts":{family:pair+"."+family for family in FAMILIES},"source_bindings":bindings}
        pairs[pair] = {"pip_size":contract["pip_size"],"contract":contract,"contract_sha256":digest(contract)}
    registry = {**FLAGS,"schema_version":"pair_local_forecast_registry_v1_20260907",
                "registry_id":"pair_local_forecast_study_v1_20260907","collection_enabled":True,
                "pairs":pairs,"source_bindings":bindings}
    arms = [{"family":family,"cohort_id":pairs["EUR_USD"]["contract"]["cohorts"][family],
             "instrument":"EUR_USD","reference_epoch":NOW-100,"issued_epoch":NOW-95,
             "target_epoch":NOW+3500,"reference_mid":"1.16234567890123456789",
             "side":1,"probability_up":.986,"predicted_return_bps":3.75} for family in FAMILIES]
    forecast = {"decision_id":"fwd.fixture.eurusd","instrument":"EUR_USD","horizon_sec":3600,
                "publication_epoch":NOW-94,"target_epoch":NOW+3500,"reference_available_epoch":NOW-99,
                "publication_verified":True,"forecast_sha256":"e"*64,"forecasts":arms}
    rows = [{"instrument":pair,"contract_sha256":pairs[pair]["contract_sha256"],"activated_epoch":NOW-200,
             "observed_epoch":NOW-3,"status":status,"reason":reason,"current_common_bars":bars,
             "required_current_common_bars":61,"latest_published_forecast":forecast if pair=="EUR_USD" else None}
            for pair,status,reason,bars in (("EUR_USD","forecast","forecast",61),
                                          ("GBP_USD","warming","own_pair_warmup:12<61",12),
                                          ("USD_JPY","unavailable","no_fresh_quote",None))]
    summary = {**FLAGS,"schema_version":"pair_local_forecast_summary_v1_20260907",
               "registry_sha256":digest(registry),"generated_epoch":NOW-2,"rows":rows}
    heartbeat = {**FLAGS,"schema_version":"pair_local_forecast_heartbeat_v1_20260907",
                 "registry_sha256":digest(registry),"generated_epoch":NOW-1}
    config_path = project/"config/pair_local_forecast_study_v1_20260907.json"
    summary_path = root/"pair_local_forecast_study_v1/summary.json"
    heartbeat_path = summary_path.with_name("heartbeat.json")
    def save(rebind_registry=True):
        if rebind_registry:
            summary["registry_sha256"] = heartbeat["registry_sha256"] = digest(registry)
        summary["payload_sha256"] = digest({key:value for key,value in summary.items() if key!="payload_sha256"})
        heartbeat["summary_sha256"] = digest(summary)
        for path,value in ((config_path,registry),(summary_path,summary),(heartbeat_path,heartbeat)):
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_text(json.dumps(value),encoding="utf-8")
    save()
    return root,registry,summary,heartbeat,save


def read(fixture,now=NOW):
    return dashboard.summarize_pair_local_forecasts(fixture[0],now_epoch=now)


def test_verified_pair_publication_keeps_original_clocks_and_explicit_coverage(publication):
    result = read(publication)
    assert result["status"] == "current"
    assert result["counts"] == {"forecast":1,"warming":1,"unavailable":1}
    forecasts = result["rows"][0]["latest_published_forecast"]
    assert forecasts["target_epoch"] == NOW+3500
    assert forecasts["forecasts"][0]["reference_mid"] == "1.16234567890123456789"
    assert result["rows"][1]["reason_label"] == "Needs 61 own minute bars · last attempt 12/61"
    assert result["rows"][2]["reason_label"] == "No fresh quote"
    assert result["probability_scope"] == "uncalibrated_model_estimate_not_verified_accuracy"
    assert result["can_place_orders"] is False


@pytest.mark.parametrize("field,value",[("instrument","GBP_USD"),("cohort_id","wrong.cohort"),
    ("target_epoch",NOW+3501),("reference_epoch",NOW-101),("issued_epoch",NOW-100),
    ("issued_epoch",NOW-93),("side",True),("probability_up",1.01),("reference_mid",0),
    ("predicted_return_bps","NaN"),("family","cross_pair_graph_transfer")])
def test_invalid_arm_never_appears_as_forecast(publication,field,value):
    publication[2]["rows"][0]["latest_published_forecast"]["forecasts"][0][field] = value
    publication[4]()
    result = read(publication)
    assert result["rows"][0]["status"] == "unavailable"
    assert result["rows"][0]["latest_published_forecast"] is None
    assert result["rows"][1]["status"] == "warming"


@pytest.mark.parametrize("field,value",[("publication_verified",False),("forecast_sha256","bad"),
    ("reference_available_epoch",NOW-201),("publication_epoch",NOW),
    ("target_epoch",NOW-95),("horizon_sec",900),("decision_id",None)])
def test_invalid_publication_binding_is_withheld(publication,field,value):
    publication[2]["rows"][0]["latest_published_forecast"][field] = value
    publication[4]()
    assert read(publication)["rows"][0]["latest_published_forecast"] is None


@pytest.mark.parametrize("which,field,value",[("summary","registry_sha256","f"*64),
    ("heartbeat","schema_version","other"),("heartbeat","can_place_orders",True),
    ("summary","can_promote",True),("heartbeat","generated_epoch",NOW+1),
    ("heartbeat","generated_epoch",NOW-4)])
def test_invalid_summary_identity_or_clock_fails_whole_projection(publication,which,field,value):
    publication[2 if which=="summary" else 3][field] = value
    publication[4](rebind_registry=False)
    result = read(publication)
    assert result["status"] == "unavailable"
    assert all(row["latest_published_forecast"] is None for row in result["rows"])


def test_summary_bytes_cannot_change_without_both_publication_seals(publication):
    path = publication[0]/"pair_local_forecast_study_v1/summary.json"
    changed = json.loads(path.read_text())
    changed["rows"][0]["latest_published_forecast"]["forecasts"][0]["probability_up"] = .5
    path.write_text(json.dumps(changed))
    assert read(publication)["status"] == "unavailable"
    changed["payload_sha256"] = digest({key:value for key,value in changed.items() if key!="payload_sha256"})
    path.write_text(json.dumps(changed))
    assert read(publication)["status"] == "unavailable"


def test_per_pair_clock_not_global_heartbeat_controls_readiness(publication):
    publication[2]["rows"][0]["observed_epoch"] = NOW-100
    publication[4]()
    result = read(publication)
    assert result["status"] == "current"
    assert result["rows"][0]["status"] == "unavailable"
    assert result["rows"][0]["reason_label"] == "Pair observation is not current"
    assert result["rows"][1]["status"] == "warming"


def test_old_summary_not_freshened_by_current_heartbeat(publication):
    assert read(publication,NOW+91)["status"] == "unavailable"


def test_original_target_elapsed_is_never_extended(publication):
    later = NOW+3501
    publication[2]["generated_epoch"] = later-2
    publication[3]["generated_epoch"] = later-1
    for row in publication[2]["rows"]:
        row["observed_epoch"] = later-3
    publication[4]()
    row = read(publication,later)["rows"][0]
    assert row["latest_published_forecast"] is None
    assert row["reason_label"] == "H1 target elapsed; next forecast pending"


@pytest.mark.parametrize("change",["missing","duplicate","unknown","non_dict"])
def test_exact_registered_pair_coverage_required(publication,change):
    rows = publication[2]["rows"]
    if change=="missing": rows.pop()
    if change=="duplicate": rows[-1] = copy.deepcopy(rows[0])
    if change=="unknown": rows[-1]["instrument"] = "AAA_BBB"
    if change=="non_dict": rows[-1] = []
    publication[4]()
    assert read(publication)["status"] == "unavailable"


def test_changed_registered_source_invalidates_projection(publication):
    (publication[0].parent.parent/"oanda_pair_local_models_v1.py").write_text("# changed source\n")
    assert read(publication)["reason"] == "pair_registry_source_binding_mismatch"


def test_contract_change_requires_its_own_recorded_hash(publication):
    publication[1]["pairs"]["EUR_USD"]["contract"]["cohorts"][FAMILIES[0]] = "unregistered.cohort"
    publication[4]()
    assert read(publication)["reason"] == "pair_registry_contract_binding"


@pytest.mark.parametrize("filename,limit",[("summary.json",512*1024),("heartbeat.json",65536)])
def test_local_publication_reads_are_bounded(publication,filename,limit):
    (publication[0]/"pair_local_forecast_study_v1"/filename).write_bytes(b" "*(limit+1))
    assert read(publication)["reason"] == "pair_summary_size_limit"


def test_missing_summary_retains_registered_pair_inventory_without_forecasts(publication):
    (publication[0]/"pair_local_forecast_study_v1/summary.json").unlink()
    result = read(publication)
    assert len(result["rows"]) == 3
    assert result["counts"] == {"unavailable":3}


def test_renderer_has_pair_selector_all_pairs_reasons_and_uncalibrated_label(publication):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node required for isolated renderer fixture")
    html = Path(dashboard.__file__).with_name("oanda_main_signal_dashboard.html").read_text(encoding="utf-8")
    functions = html[html.index("    let marketOverviewWindow="):html.index("    function renderAvailableSignals(data)")]
    data = {"pair_local_forecasts":read(publication),"market_overview":{"generated_epoch":NOW,
        "rows":[{"instrument":pair,"status":"current","quote_age_sec":3,"quote_epoch":NOW-3,
                 "mid":1.1624,"spread_pips":1.6,"changes":{"15m":{"return_bps":1,"price_change_pips":1.1624}}}
                for pair in ("EUR_USD","GBP_USD")]}}
    harness = """const elements={};const $=id=>elements[id]??={innerHTML:''};
const num=(v,d=2)=>v==null?'—':Number(v).toFixed(d);const signed=(v,d=2)=>v==null?'—':(Number(v)>=0?'+':'')+Number(v).toFixed(d);const cls=()=>'';
const esc=v=>String(v??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
"""+f"Date.now=()=>{NOW*1000};\n"+functions+"\nconst data="+json.dumps(data)+""";
renderMarketOverview(data);const current=elements['market-overview'].innerHTML;
marketOverviewPair='USD_JPY';renderMarketOverview(data);const selected=elements['market-overview'].innerHTML;
marketOverviewPair='';marketOverviewAll=true;Date.now=()=>"""+str((NOW+91)*1000)+""";
renderMarketOverview(data);process.stdout.write(JSON.stringify({current,selected,stale:elements['market-overview'].innerHTML}));"""
    result = json.loads(subprocess.run([node,"-e",harness],capture_output=True,text=True,encoding="utf-8",check=True,timeout=15).stdout)
    assert "1 pairs forecast · 1 warming · 1 unavailable" in result["current"]
    assert "Show all 3" in result["current"]
    assert "Needs 61 own minute bars · last attempt 12/61" in result["current"]
    assert "uncalibrated model estimate, not verified accuracy" in result["current"]
    assert "Ridge: Buy" in result["current"]
    assert "No fresh quote" in result["selected"]
    assert "Ridge: Buy" not in result["selected"]
    assert "Ridge: Buy" not in result["stale"]
    assert "Forecast status is not current" in result["stale"]
