"""News evidence mapping and aggregate activity renderer fixtures."""
from copy import deepcopy
from datetime import datetime,timezone
import json
from pathlib import Path
import shutil
import socket
import subprocess

import pytest

import oanda_practice_live_dashboard as dashboard

NOW=1788804000.0


def stamp(epoch=NOW):
    return datetime.fromtimestamp(epoch,timezone.utc).isoformat()


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args,**kwargs):
        raise AssertionError("News dashboard fixtures forbid network calls")
    monkeypatch.setattr(socket,"create_connection",forbidden)
    monkeypatch.setattr(socket.socket,"connect",forbidden)


@pytest.fixture
def snapshot():
    names=["EUR_USD"]+[f"{chr(65+i//26)}{chr(65+i%26)}X_USD" for i in range(66)]+["USD_HUF"]
    policy={"research_only":True,"execution_eligible":False,"matrix_weight":0}
    pairs={pair:{**policy,"instrument":pair,"direction":"NEUTRAL","score":0,"confidence":0,
                 "evidence_quality":"NO_CURRENT_EVIDENCE","active_event_count":19,
                 "context_directional_event_count":13,"directional_event_count":0,"as_of_utc":stamp(NOW-100),
                 "events":[{"headline":"A <retained> headline","source_name":"Source & one","reaction_phase":"continuation_context","category":"context"}]}
           for pair in names}
    return {"schema_version":"local_fx_news_sentiment_v3","generated_utc":stamp(NOW-2),"as_of_utc":stamp(NOW-100),
            "instrument_count":68,"active_article_count":23,"policy":policy,"pairs":pairs,
            "coverage":{"all_pairs_emitted":True,"pair_count":68,"directional_pair_count":0}}


def project(snapshot):
    return dashboard.summarize_news_pair_bias(snapshot,now_epoch=NOW)


def test_all_neutral_pair_records_retain_context_and_pair_specific_counts(snapshot):
    snapshot["pairs"]["USD_HUF"].update(active_event_count=16,context_directional_event_count=10)
    result=project(snapshot)
    assert result["pair_bias_status"]=="current"
    assert len(result["pair_bias"])==68
    assert result["active_topic_count"]==23
    assert result["evidence_age_sec"]==100
    eur=result["pair_bias"]["EUR_USD"];huf=result["pair_bias"]["USD_HUF"]
    assert eur["display_label"]=="Context only" and eur["direction"]=="NEUTRAL"
    assert (eur["relevant_event_count"],eur["context_directional_event_count"],eur["current_directional_event_count"])==(19,13,0)
    assert (huf["relevant_event_count"],huf["context_directional_event_count"])==(16,10)
    assert eur["events"][0]["headline"]=="A <retained> headline"


def test_no_related_events_and_balanced_current_evidence_are_distinct(snapshot):
    snapshot["pairs"]["EUR_USD"].update(active_event_count=0,context_directional_event_count=0,events=[])
    snapshot["pairs"]["USD_HUF"].update(directional_event_count=2,evidence_quality="TWO_SIDED_DIRECT")
    result=project(snapshot)
    assert result["pair_bias"]["EUR_USD"]["display_label"]=="No current news signal"
    assert result["pair_bias"]["USD_HUF"]["display_label"]=="Mixed or weak current evidence"


def test_directional_pair_beyond_legacy_top12_keeps_its_original_direction(snapshot):
    snapshot["pairs"]["USD_HUF"].update(direction="SHORT",score=-0.2,confidence=0.1,directional_event_count=2,evidence_quality="ONE_SIDED_DIRECT")
    result=project(snapshot)
    assert result["pair_bias"]["USD_HUF"]["display_label"]=="Short"
    assert result["pair_bias"]["USD_HUF"]["direction"]=="SHORT"


@pytest.mark.parametrize("case",["schema","empty_pairs","missing_pair","coverage_false","wrong_count","unsafe_policy","missing_clock","future_clock"])
def test_unknown_mapping_or_schema_never_becomes_neutral(snapshot,case):
    if case=="schema":snapshot["schema_version"]="unknown"
    if case=="empty_pairs":snapshot["pairs"]={}
    if case=="missing_pair":snapshot["pairs"].pop("EUR_USD")
    if case=="coverage_false":snapshot["coverage"]["all_pairs_emitted"]=False
    if case=="wrong_count":snapshot["coverage"]["pair_count"]=67
    if case=="unsafe_policy":snapshot["policy"]["execution_eligible"]=True
    if case=="missing_clock":snapshot.pop("as_of_utc")
    if case=="future_clock":snapshot["generated_utc"]=stamp(NOW+1)
    result=project(snapshot)
    assert result["pair_bias_status"]=="unavailable"
    assert result["pair_bias"]=={}


@pytest.mark.parametrize("field,value",[("direction",None),("direction","UP"),("instrument","GBP_USD"),
    ("active_event_count",None),("directional_event_count",True),("context_directional_event_count",99),
    ("as_of_utc",stamp(NOW-101)),("research_only",False),("events",{}),
    ("score",None),("score",float('nan')),("score",2),("score",0.5),("confidence",True),("confidence",-1),("directional_event_count",1)])
def test_invalid_pair_remains_unavailable_without_erasing_other_pairs(snapshot,field,value):
    snapshot["pairs"]["EUR_USD"][field]=value
    result=project(snapshot)
    assert result["pair_bias"]["EUR_USD"]["status"]=="unavailable"
    assert result["pair_bias"]["EUR_USD"]["direction"] is None
    assert result["pair_bias"]["USD_HUF"]["status"]=="current"


def test_direction_threshold_rounding_retains_producer_label(snapshot):
    for direction in ("LONG","NEUTRAL"):
        snapshot["pairs"]["EUR_USD"].update(direction=direction,score=0.12,directional_event_count=1,evidence_quality="ONE_SIDED_DIRECT")
        assert project(snapshot)["pair_bias"]["EUR_USD"]["direction"]==direction


def test_fresh_generation_does_not_refresh_old_evidence_cutoff(snapshot):
    snapshot["as_of_utc"]=stamp(NOW-301)
    for row in snapshot["pairs"].values():row["as_of_utc"]=snapshot["as_of_utc"]
    result=project(snapshot)
    assert result["pair_bias_status"]=="stale"
    assert all(row["status"]=="stale" for row in result["pair_bias"].values())


def render_script(functions,body):
    node=shutil.which("node")
    if node is None:pytest.skip("Node required for isolated renderer fixtures")
    harness="""const elements={};const $=id=>elements[id]??={innerHTML:''};
const num=(v,d=2)=>v==null?'—':Number(v).toFixed(d);const signed=(v,d=2)=>v==null?'—':(Number(v)>=0?'+':'')+Number(v).toFixed(d);const cls=()=>'';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
"""+f"Date.now=()=>{NOW*1000};\n"+functions+"\n"+body
    return json.loads(subprocess.run([node],input=harness,capture_output=True,text=True,encoding="utf-8",check=True,timeout=15).stdout)


def test_main_news_renderer_shows_global_count_once_pair_context_and_escaped_headlines(snapshot):
    html=Path(dashboard.__file__).with_name("oanda_main_signal_dashboard.html").read_text(encoding="utf-8")
    functions=html[html.index("    let marketOverviewWindow="):html.index("    function renderAvailableSignals(data)")]
    data={"news_sentiment":{**project(snapshot),"generated_utc":snapshot["generated_utc"],"top_pairs":[]},
          "market_overview":{"generated_epoch":NOW,"rows":[{"instrument":pair,"status":"current","quote_age_sec":2,"quote_epoch":NOW-2,"mid":1.1}
                      for pair in ("EUR_USD","USD_HUF")]}}
    body="const data="+json.dumps(data)+""";
renderMarketOverview(data);const current=elements['market-overview'].innerHTML;
delete data.news_sentiment.pair_bias.EUR_USD;renderMarketOverview(data);const missing=elements['market-overview'].innerHTML;
process.stdout.write(JSON.stringify({current,missing}));"""
    result=render_script(functions,body)
    assert result["current"].count("23 active news topics across the feed")==1
    assert result["current"].count("Context only")==2
    assert "19 related · 0 current directional" in result["current"]
    assert "13 with directional context" in result["current"]
    assert "&lt;retained&gt;" in result["current"] and "Source &amp; one" in result["current"]
    assert "No directional news" not in result["current"]
    assert "No verified pair summary" in result["missing"]


def test_pair_coverage_heads_global_activity_while_companion_errors_remain_visible():
    html=Path(dashboard.__file__).with_name("oanda_main_signal_dashboard.html").read_text(encoding="utf-8")
    functions=html[html.index("    function collectionHeaderState(data)"):html.index("    function render(data)")]
    functions=html[html.index('    function jointForecastActivity(data)'):html.index('    function renderMarketOverview(data)')]+functions
    data={"pair_local_forecasts":{"status":"current","generated_epoch":NOW-2,"research_only":True,"can_place_orders":False,"can_promote":False,
            "rows":[{"instrument":f"P{i}","observed_epoch":NOW-3,"status":"forecast" if i<22 else "warming" if i<53 else "unavailable",
                     "reason_label":"No fresh quote" if i>=53 else "",
                     "latest_published_forecast":{"publication_epoch":NOW-4,"target_epoch":NOW+3500} if i<22 else None} for i in range(68)]},
          "collection_status":{"selected_study":"eurusd_v1","running":True,"registered_study_trading_enabled":False,
              "forecasting":{"blocked":True,"label":"Forecasts blocked by missing EUR/USD minutes"},
              "observations":{"study":{"reported_cumulative_errors":6,"reported_cumulative_heartbeat_publication_errors":0},"quote_stream":{"current":True}},
              "model_attempts":{"counts":{"attempts":12,"diagnostics":9,"publication":3,"outcomes":3},"status":"available"}}}
    body="const data="+json.dumps(data)+""";
const currentHeader=collectionHeaderState(data);renderCollectionStatus(data);const current=elements['collection-status'].innerHTML;
data.pair_local_forecasts.generated_epoch="""+str(NOW-100)+""";
const staleHeader=collectionHeaderState(data);renderCollectionStatus(data);const stale=elements['collection-status'].innerHTML;
process.stdout.write(JSON.stringify({currentHeader,current,staleHeader,stale}));"""
    result=render_script(functions,body)
    assert result["currentHeader"]["text"]=="Partially operational · 22/68 pairs forecasting · research only"
    assert "22/68 pairs publishing H1 forecasts" in result["current"]
    assert "31 need history or training inputs · 15 lack fresh quotes" in result["current"]
    assert result["current"].index("22/68 pairs publishing")<result["current"].index("Forecasts blocked")
    assert "EUR/USD companion study" in result["current"]
    assert "6 reported errors" in result["current"] and "Reported study errors: 6 total" in result["current"]
    assert result["staleHeader"]["text"]=="Pair forecast status unavailable"
    assert "publishing H1 forecasts" not in result["stale"]


@pytest.fixture
def diagnostic():
    return {"schema_version":"eurusd_supplemental_diagnostic_report_v1_20260907",
            "status":"available","freshness":"current","generated_utc":stamp(NOW-30),
            "supplemental":True,"registered_scorecard":False,"research_only":True,
            "execution_eligible":False,"account_eligible":False,"proof_eligible":False,
            "can_place_orders":False,"can_promote":False,"capacity_state":"near_limit",
            "original_scorer":{"status":"failed","error":"duplicate_market_reference_epoch"},
            "original_scorecard":{"exists":True,"stale":True},
            "original_decision_count":6,"excluded_decision_count":2,"retained_decision_count":4,
            "report":{"coverage":{"paired_scored_decisions":3},"decisions":[{"large":"not for main API"}],
                      "paired_summaries":{"ridge_return_repaired":{"decisions":3,"directional_decisions":2,
                          "direction_hit_rate_when_directional":"0.5","positive_after_spread_rate_when_directional":"0","mean_net_bps_per_decision":"-1.25"}}},
            "excluded_duplicate_reference_groups":[{"reference_epoch":NOW-7200,"decision_ids":["a","b"]}]}


def test_supplemental_main_projection_keeps_counts_and_omits_full_decision_payload(monkeypatch,tmp_path,diagnostic):
    import oanda_study_diagnostic_report_v1 as adapter
    monkeypatch.setattr(adapter,"get_eurusd_diagnostic_report",lambda root:deepcopy(diagnostic))
    result=dashboard.summarize_eurusd_supplemental_diagnostic(tmp_path)
    assert (result["original_decision_count"],result["excluded_decision_count"],result["retained_decision_count"],result["paired_scored_decisions"])==(6,2,4,3)
    assert result["original_scorer"]["error"]=="duplicate_market_reference_epoch"
    assert "report" not in result and "excluded_duplicate_reference_groups" not in result
    assert result["paired_summaries"]==diagnostic["report"]["paired_summaries"]


def test_diagnostic_adapter_failure_is_sanitized_without_breaking_main_api(monkeypatch,tmp_path):
    import oanda_study_diagnostic_report_v1 as adapter
    def broken(root):raise RuntimeError("private runtime detail")
    monkeypatch.setattr(adapter,"get_eurusd_diagnostic_report",broken)
    result=dashboard.summarize_eurusd_supplemental_diagnostic(tmp_path)
    assert result["status"]=="unavailable" and result["error"]=="diagnostic_adapter_unavailable"
    assert "private" not in json.dumps(result)


@pytest.mark.parametrize("case",["current","stale","future","unsafe","capacity"])
def test_supplemental_renderer_keeps_registered_failure_and_withholds_unverified_metrics(diagnostic,case):
    html=Path(dashboard.__file__).with_name("oanda_main_signal_dashboard.html").read_text(encoding="utf-8")
    functions=html[html.index("    function collectionHeaderState(data)"):html.index("    function render(data)")]
    functions=html[html.index('    function jointForecastActivity(data)'):html.index('    function renderMarketOverview(data)')]+functions
    diagnostic["paired_scored_decisions"]=diagnostic["report"]["coverage"]["paired_scored_decisions"]
    diagnostic["paired_summaries"]=diagnostic.pop("report")["paired_summaries"]
    if case=="stale":diagnostic["generated_utc"]=stamp(NOW-121)
    if case=="future":diagnostic["generated_utc"]=stamp(NOW+1)
    if case=="unsafe":diagnostic["registered_scorecard"]=True
    if case=="capacity":diagnostic.update(status="unavailable",error="diagnostic_row_limit")
    data={"eurusd_supplemental_diagnostic":diagnostic,"pair_local_forecasts":{"status":"unavailable"},
          "collection_status":{"selected_study":"eurusd_v1","observations":{"study":{"reported_cumulative_errors":6}}}}
    body="const data="+json.dumps(data)+";renderCollectionStatus(data);process.stdout.write(JSON.stringify(elements['collection-status'].innerHTML));"
    result=render_script(functions,body)
    assert "Supplemental EUR/USD diagnostic" in result and "Original scorer: failed" in result
    assert "duplicate_market_reference_epoch" in result and "Original scorecard: stale" in result
    assert "6 reported errors" in result
    assert '<details class="compact-details" data-state-key="companion-study-details">' in result
    if case=="current":
        assert "6 original decisions · 2 excluded in duplicate-reference groups · 4 retained · 3 scored unique decisions" in result
        assert "direction correct 50.0% · positive after spread 0.0%" in result
        assert "mean net -1.25 bps" in result
        assert "not registered performance or trading authorization" in result
        assert "Approaching diagnostic history limit" in result
    else:
        assert "Diagnostic unavailable" in result and "direction correct" not in result
    if case=="capacity":assert "diagnostic_row_limit" in result


@pytest.mark.parametrize("case",["evidence_stale","pair_unverified","pair_clock_stale","unknown_schema"])
def test_secondary_news_renderer_cannot_reintroduce_stale_or_unverified_direction(case):
    html=Path(dashboard.__file__).with_name("oanda_main_signal_dashboard.html").read_text(encoding="utf-8")
    functions=html[html.index("    function renderAvailableSignals(data)"):html.index("    function levelTrack(row)")]
    news={"fresh":True,"pair_bias_status":"current","generated_utc":stamp(NOW-1),"as_of_utc":stamp(NOW-5),
          "top_pairs":[{"instrument":"EUR_USD","direction":"long","status":"current","as_of_utc":stamp(NOW-5),"events":[{"headline":"Directional headline"}]}]}
    if case=="evidence_stale":news["as_of_utc"]=stamp(NOW-301)
    if case=="pair_unverified":news["top_pairs"][0]["status"]="unavailable"
    if case=="pair_clock_stale":news["top_pairs"][0]["as_of_utc"]=stamp(NOW-301)
    if case=="unknown_schema":news["pair_bias_status"]="unavailable"
    body="renderAvailableSignals("+json.dumps({"news_sentiment":news})+");process.stdout.write(JSON.stringify(elements['available-signals'].innerHTML));"
    assert "Directional headline" not in render_script(functions,body)


@pytest.mark.parametrize("case",["current","stale","invalid_pair"])
def test_signal_attachment_uses_verified_news_and_preserves_missing_status(monkeypatch,tmp_path,snapshot,case):
    snapshot["pairs"]["EUR_USD"].update(direction="SHORT",score=-0.2,directional_event_count=1,evidence_quality="GLOBAL_TOPIC_PROXY")
    if case=="stale":
        snapshot["as_of_utc"]=stamp(NOW-301)
        for row in snapshot["pairs"].values():row["as_of_utc"]=snapshot["as_of_utc"]
    if case=="invalid_pair":snapshot["pairs"]["EUR_USD"]["score"]=None
    monkeypatch.setattr(dashboard.time,"time",lambda:NOW)
    monkeypatch.setattr(dashboard,"load_json_dict",lambda path:snapshot if path==dashboard.LOCAL_NEWS_SENTIMENT else {})
    monkeypatch.setattr(dashboard,"discover_lab_logs",lambda *args:[])
    monkeypatch.setattr(dashboard,"resolve_display_signals",lambda *args:{"rows":[{"instrument":"EUR_USD"}],"age_sec":2})
    for name in ("summarize_eurusd_supplemental_diagnostic","summarize_executor_entry_diagnostics","heartbeat_status",
                 "summarize_post_gap_execution","summarize_live_movers","summarize_live_move_news",
                 "summarize_continuous_narrative","summarize_adaptive_level_bands"):
        monkeypatch.setattr(dashboard,name,lambda *args,**kwargs:{})
    monkeypatch.setattr(dashboard,"summarize_primary_practice_account",lambda *args:{"ok":False,"snapshot_age_sec":None})
    result=dashboard.build_main_state(tmp_path/"logs")
    context=result["top_signals"][0]["news_context"]
    if case=="current":
        assert context["status"]=="current" and context["direction"]=="short"
        assert context["as_of_utc"]==snapshot["as_of_utc"]
        assert result["news_sentiment"]["top_pairs"][0]["status"]=="current"
    else:
        assert context["status"]=="unavailable" and context["direction"] is None
        assert context["score"] is None and context["events"]==[]
        assert result["news_sentiment"]["top_pairs"]==[]


@pytest.mark.parametrize("case",["current","stale","unavailable","future"])
def test_collapsed_pair_context_and_reason_withhold_unverified_news_direction(case):
    html=Path(dashboard.__file__).with_name("oanda_main_signal_dashboard.html").read_text(encoding="utf-8")
    functions=html[html.index("    function newsContextCurrent(news)"):html.index("    function renderFamilies(data)")]
    news={"status":"current","direction":"short","score":-0.2,"as_of_utc":stamp(NOW-5),"generated_utc":stamp(NOW-2)}
    if case=="stale":news["as_of_utc"]=stamp(NOW-301)
    if case=="unavailable":news["status"]="unavailable"
    if case=="future":news["generated_utc"]=stamp(NOW+1)
    data={"top_signals":[{"instrument":"EUR_USD","direction":"long","news_context":news,"signal_is_live":True}]}
    body="const signalSide=s=>s?.direction||'';const normalizeSide=s=>s;const sideLabel=s=>s;const pct=()=>'';const horizon=()=>'';const restoreDetailState=()=>{};renderSignals("+json.dumps(data)+");process.stdout.write(JSON.stringify(elements.signals.innerHTML));"
    result=render_script(functions,body)
    if case=="current":assert "news short -0.20 shadow" in result
    else:assert "news unavailable" in result and "Unavailable" in result and "short -0.20" not in result
