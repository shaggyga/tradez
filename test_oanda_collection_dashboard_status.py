"""Collection-status fixtures allow only their own bounded read-only study DBs."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
from urllib.parse import unquote, urlsplit

import pytest

import oanda_practice_live_dashboard as dashboard

NOW = datetime(2026, 9, 6, 20, tzinfo=timezone.utc).timestamp()
CONTRACT_ID = "causal_four_family_future_collection_v1_io_r2_20260906"
_FIXTURE_CONNECT = sqlite3.connect


def stamp(age=0):
    return datetime.fromtimestamp(NOW - age, timezone.utc).isoformat()


@pytest.fixture(autouse=True)
def forbid_database_and_network(monkeypatch, tmp_path):
    def forbidden(*args, **kwargs):
        raise AssertionError("Collection dashboard tests forbid database/network access")
    def fixture_readonly(database, *args, **kwargs):
        parsed = urlsplit(str(database))
        path = unquote(parsed.path)
        if path.startswith('/') and len(path)>2 and path[2]==':':
            path = path[1:]
        if parsed.scheme != 'file' or parsed.query != 'mode=ro' or not kwargs.get('uri') or not Path(path).resolve().is_relative_to(tmp_path.resolve()):
            forbidden()
        return _FIXTURE_CONNECT(database, *args, **kwargs)
    monkeypatch.setattr(sqlite3, "connect", fixture_readonly)
    dashboard._COLLECTION_LEDGER_CACHE.clear()
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    import requests
    monkeypatch.setattr(requests.sessions.Session, "request", forbidden)


@pytest.fixture
def publications(tmp_path):
    root = tmp_path / "trad/data/oanda_training_manager"
    contract = {"contract_id": CONTRACT_ID, "collection_enabled": True, "research_only": True,
                "can_place_orders": False, "can_authorize": False, "can_promote": False,
                "account_eligible": False, "proof_eligible": False, "historical_rows_imported": False}
    contract_sha = hashlib.sha256(json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    payloads = {
        "contract": contract,
        "study": {"schema_version": "causal_forecast_study_heartbeat_v1", "contract_id": CONTRACT_ID,
                  "contract_sha256": contract_sha, "activated_epoch": NOW - 1000, "generated_epoch": NOW - 5,
                  "research_only": True, "can_place_orders": False, "can_promote": False,
                  "account_eligible": False, "proof_eligible": False, "supported_decision": "no_trade",
                  "phase": "waiting_for_tradeable_quote", "last_reason": "market_closed_or_quote_not_tradeable"},
        "quote": {"role": "practice_007_quote_stream", "status": "running", "updated_at": stamp(5),
                  "details": {"account_suffix": "-007", "can_place_orders": False, "real_money_routing": False,
                              "stream": {"connected": True, "last_event_age_sec": 2}}},
        "account": {"time": stamp(5), "accounts": [{"account_id": "practice-fixture-007", "env": "practice",
                    "verified_at_utc": stamp(5), "ok": True, "account_values_current": True,
                    "positions_current": True, "orders_current": True}]},
    }
    paths = {"contract": root.parent.parent / "config/causal_forecast_study_v1_io_r2_20260906.json",
             "study": root / "causal_forecast_study_v1_io_r2/heartbeat.json",
             "quote": root / "state/practice_007_quote_stream_heartbeat_v1.json",
             "account": root / "state/account_007_dashboard_v1.json"}

    def persist():
        for name, payload in payloads.items():
            paths[name].parent.mkdir(parents=True, exist_ok=True)
            paths[name].write_text(json.dumps(payload), encoding="utf-8")
    persist()
    return root, payloads, paths, persist


def read(publications):
    return dashboard.summarize_collection_status(publications[0], now_epoch=NOW)


def test_fresh_closed_market_collectors_show_running_without_trading(publications):
    result = read(publications)
    assert result["running"] is True
    assert result["label"] == "Research collection running; study trading disabled"
    assert result["registered_study_trading_enabled"] is False
    assert result["study_phase"] == "waiting_for_tradeable_quote"
    assert result["study_reason"] == "market_closed_or_quote_not_tradeable"
    assert result["warmup"]["required_common_m1_bars"] == 335
    assert result["warmup"]["last_reported_common_bars"] is None
    assert result["strategy_or_execution_activation_changed"] is False
    assert "active" not in result


@pytest.mark.parametrize("component", ["study", "quote", "account"])
@pytest.mark.parametrize("clock_case,expected", [("old", "stale"), ("future", "future_timestamp"), ("missing", "invalid_clock"), ("naive", "invalid_clock")])
def test_each_observation_requires_a_fresh_explicit_clock(publications, component, clock_case, expected):
    _, payloads, _, persist = publications
    target = payloads[component]["accounts"][0] if component == "account" else payloads[component]
    key = {"study": "generated_epoch", "quote": "updated_at", "account": "verified_at_utc"}[component]
    target[key] = {"old": stamp(91), "future": stamp(-1), "missing": None, "naive": "2026-09-06T20:00:00"}[clock_case]
    persist()
    result = read(publications)
    observation_name = "quote_stream" if component == "quote" else component
    assert result["observations"][observation_name]["status"] == expected
    assert result["running"] is None
    assert result["label"] == "Research collection status unknown"
    if component == "study":
        assert result["study_phase"] is None


@pytest.mark.parametrize("component", ["study", "quote", "account", "contract"])
def test_missing_file_never_means_zero_or_running(publications, component):
    publications[2][component].unlink()
    result = read(publications)
    assert result["running"] is None
    if component in {"study", "contract"}:
        assert result["registered_study_trading_enabled"] is None


@pytest.mark.parametrize("field,value", [("contract_id", "old-contract"), ("contract_sha256", "0" * 64),
    ("can_place_orders", True), ("proof_eligible", True), ("supported_decision", "trade"), ("activated_epoch", NOW + 1)])
def test_unregistered_or_noninert_study_cannot_claim_disabled_trading(publications, field, value):
    publications[1]["study"][field] = value
    publications[3]()
    result = read(publications)
    assert result["running"] is None
    assert result["registered_study_trading_enabled"] is None


def test_changed_contract_cannot_reuse_old_heartbeat_hash(publications):
    publications[1]["contract"]["can_authorize"] = True
    publications[3]()
    assert read(publications)["observations"]["study"]["status"] == "identity_or_safety_mismatch"


@pytest.mark.parametrize("field,value", [("connected", False), ("last_event_age_sec", 1000), ("last_event_age_sec", None)])
def test_fresh_heartbeat_does_not_hide_disconnected_or_stalled_stream(publications, field, value):
    publications[1]["quote"]["details"]["stream"][field] = value
    publications[3]()
    result = read(publications)
    assert result["running"] is None
    assert result["observations"]["quote_stream"]["status"] == "unhealthy"


def test_cached_account_does_not_count_as_current(publications):
    publications[1]["account"]["accounts"][0]["positions_current"] = False
    publications[3]()
    result = read(publications)
    assert result["running"] is None
    assert result["observations"]["account"]["status"] == "unhealthy"


@pytest.mark.parametrize("heartbeat_age,event_age,current", [(80, 80, False), (45, 46, False), (45, 45, True)])
def test_quote_event_freshness_includes_elapsed_time_since_heartbeat(publications, heartbeat_age, event_age, current):
    publications[1]["quote"]["updated_at"] = stamp(heartbeat_age)
    publications[1]["quote"]["details"]["stream"]["last_event_age_sec"] = event_age
    publications[3]()
    result = read(publications)
    quote = result["observations"]["quote_stream"]
    assert quote["current_stream_event_age_sec"] == heartbeat_age + event_age
    assert quote["current"] is current
    assert result["running"] is (True if current else None)


def test_warmup_progress_only_uses_explicit_last_attempt(publications):
    publications[1]["study"].update(phase="collecting", last_reason="ValueError:contiguous_common_warmup:47<335")
    publications[3]()
    result = read(publications)
    assert result["warmup"]["last_reported_common_bars"] == 47
    assert result["warmup"]["progress_scope"] == "last_reported_attempt"
    assert "not an estimated completion time" in result["warmup"]["explanation"]


def test_cumulative_errors_are_reported_without_turning_recent_liveness_into_failure(publications):
    publications[1]["study"].update(errors=3, heartbeat_publication_errors=1)
    publications[3]()
    result = read(publications)
    assert result["running"] is True
    assert result["observations"]["study"]["reported_cumulative_errors"] == 3
    assert result["observations"]["study"]["reported_cumulative_heartbeat_publication_errors"] == 1
    assert "not_forecast_success_or_error_free" in result["running_scope"]


@pytest.mark.parametrize("content", [b"not json", b"\xff", b"x" * 262145], ids=["invalid-json", "invalid-utf8", "oversized"])
def test_malformed_or_oversized_heartbeat_is_unknown(publications, content):
    publications[2]["study"].write_bytes(content)
    result = read(publications)
    assert result["observations"]["study"]["status"] == "missing"
    assert result["running"] is None


def test_main_payload_reports_collection_without_setting_existing_active(publications, monkeypatch):
    root = publications[0]
    logs = root / "logs"
    logs.mkdir()
    monkeypatch.setattr(dashboard.time, "time", lambda: NOW)
    monkeypatch.setattr(dashboard, "discover_lab_logs", lambda *args: [])
    monkeypatch.setattr(dashboard, "summarize_executor_entry_diagnostics", lambda *args: {})
    for constant in ("ACCOUNT_SNAPSHOT", "ACCOUNT_007_SNAPSHOT", "SIGNAL_SNAPSHOT", "LOCAL_NEWS_SENTIMENT"):
        monkeypatch.setattr(dashboard, constant, root / "absent")
    for function in ("heartbeat_status", "load_json_dict", "resolve_display_signals", "summarize_post_gap_execution",
                     "summarize_live_movers", "summarize_live_move_news", "summarize_continuous_narrative",
                     "summarize_adaptive_level_bands"):
        monkeypatch.setattr(dashboard, function, lambda *args, **kwargs: {})
    monkeypatch.setattr(dashboard, "summarize_primary_practice_account", lambda *args: {"ok": True, "snapshot_age_sec": 5})
    diagnostic = {"status": "unavailable", "error": "fixture", "supplemental": True}
    monkeypatch.setattr(dashboard, "summarize_eurusd_supplemental_diagnostic", lambda *args: diagnostic)
    result = dashboard.build_main_state(logs)
    assert result["legacy_collection_status"]["running"] is True
    assert result['collection_status']['selected_study']=='joint_price_news_v2'
    assert result['collection_status']['running'] is False
    assert result['joint_collection_status']==result['collection_status']
    assert result["active"] is False
    assert result["execution"]["worker_heartbeat"] == {}
    assert result["eurusd_supplemental_diagnostic"] == diagnostic


def test_main_html_header_and_banner_keep_collection_separate_from_active_trading(publications):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js unavailable for isolated HTML-function test")
    html = Path(dashboard.__file__).with_name("oanda_main_signal_dashboard.html").read_text(encoding="utf-8")
    functions = html[html.index("    function collectionHeaderState(data)"):html.index("    function render(data)")]
    functions=html[html.index('    function jointForecastActivity(data)'):html.index('    function renderMarketOverview(data)')]+functions
    assert 'id="collection-status"' in html
    payload = {"active": False, "collection_status": read(publications)}
    harness = """'use strict';
const elements={};const $=id=>elements[id]??=( {innerHTML:''} );
const num=(value,digits=1)=>value==null?'-':Number(value).toFixed(digits);
const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
""" + functions + "\nconst data=" + json.dumps(payload) + ";Object.freeze(data);" + """
const collecting=collectionHeaderState(data);renderCollectionStatus(data);
const collectionHtml=elements['collection-status'].innerHTML;
const stale=collectionHeaderState({active:false,collection_status:{running:null}});
const activeData={active:true,collection_status:data.collection_status};
const active=collectionHeaderState(activeData);renderCollectionStatus(activeData);
process.stdout.write(JSON.stringify({collecting,stale,active,html:collectionHtml,activeHtml:elements['collection-status'].innerHTML,unchanged:data.active===false}));
"""
    result = subprocess.run([node], input=harness, capture_output=True, text=True, encoding='utf-8', check=True, timeout=15)
    rendered = json.loads(result.stdout)
    assert rendered["collecting"] == {"className": "dot collection", "text": "Research collection running; study trading disabled"}
    assert rendered["stale"]["className"] == "dot"
    assert "unknown" in rendered["stale"]["text"]
    assert rendered["active"]["className"] == "dot live"
    assert "Research collection running; study trading disabled" in rendered["activeHtml"]
    assert "Data collection running; trading disabled" not in rendered["activeHtml"]
    assert rendered["unchanged"] is True
    assert "orders and promotion are disabled" in rendered["html"]
    assert "5h35" in rendered["html"]
    assert "Current warm-up progress is not reported" in rendered["html"]


def make_ledger(publications, *, folder="causal_forecast_study_v1_io_r2", bars=18, required=335, published=False):
    root, payloads, _, _ = publications
    path = root/folder/'study.sqlite'
    path.parent.mkdir(parents=True, exist_ok=True)
    contract = payloads['contract']
    raw = json.dumps(contract, sort_keys=True, separators=(',',':'))
    contract_sha = hashlib.sha256(raw.encode()).hexdigest()
    db = _FIXTURE_CONNECT(path)
    db.executescript('''CREATE TABLE contract(id INTEGER PRIMARY KEY,sha TEXT,payload TEXT);
        CREATE TABLE activation(id INTEGER PRIMARY KEY,epoch REAL,contract_sha TEXT);
        CREATE TABLE attempts(bucket INTEGER PRIMARY KEY,epoch REAL,reference_id TEXT);
        CREATE TABLE diagnostics(bucket INTEGER PRIMARY KEY,epoch REAL,payload TEXT);
        CREATE TABLE forecasts(id TEXT PRIMARY KEY,bucket INTEGER UNIQUE,target REAL,sha TEXT,payload TEXT);
        CREATE TABLE publication(id TEXT PRIMARY KEY,epoch REAL,forecast_sha TEXT);
        CREATE TABLE quotes(id TEXT PRIMARY KEY,market REAL,available REAL,payload TEXT);
        CREATE TABLE outcomes(id TEXT PRIMARY KEY,quote_id TEXT,epoch REAL);''')
    db.execute('INSERT INTO contract VALUES(1,?,?)',(contract_sha,raw))
    db.execute('INSERT INTO activation VALUES(1,?,?)',(payloads['study']['activated_epoch'],contract_sha))
    db.execute('INSERT INTO attempts VALUES(1,?,?)',(NOW-10,'reference'))
    reason = f"ValueError:{'current' if required!=335 else 'contiguous'}_common_warmup:{bars}<{required}"
    db.execute('INSERT INTO diagnostics VALUES(1,?,?)',(NOW-8,json.dumps({'status':'abstain','reasons':[reason]})))
    if published:
        db.execute('INSERT INTO attempts VALUES(2,?,?)',(NOW-6,'reference2'))
        db.execute('INSERT INTO forecasts VALUES(?,?,?,?,?)',('forecast2',2,NOW+3600,'sha','{}'))
        db.execute('INSERT INTO publication VALUES(?,?,?)',('forecast2',NOW-4,'sha'))
    db.commit()
    db.close()
    return path


def test_retained_rejection_survives_cleared_heartbeat_with_current_feed(publications):
    publications[1]['study'].update(phase='collecting',last_reason='')
    publications[3]()
    path = make_ledger(publications)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    result = read(publications)
    assert result['running'] is True
    assert result['observations']['quote_stream']['current'] is True
    assert result['study_reason'] == ''
    assert result['forecasting']['state'] == 'blocked_warmup'
    assert result['forecasting']['blocked'] is True
    assert result['warmup']['last_reported_common_bars'] == 18
    assert result['warmup']['progress_scope'] == 'last_retained_rejection'
    assert result['model_attempts']['counts']['publication'] == 0
    assert result['model_attempts']['counts']['diagnostics'] == 1
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


def test_later_publication_resolves_blocker_without_erasing_last_rejection(publications):
    make_ledger(publications,published=True)
    result = read(publications)
    assert result['forecasting']['state'] == 'published'
    assert result['forecasting']['blocked'] is False
    assert result['forecasting']['latest_rejection_reason'] is None
    assert '18<335' in result['forecasting']['last_rejection_reason']
    assert result['model_attempts']['counts']['publication'] == 1


def make_published_forecast(publications, *, corruption=None, families=None, reference_epoch=None, reference_available=None):
    payloads=publications[1]
    reference_epoch=NOW-100 if reference_epoch is None else reference_epoch
    reference_available=reference_epoch+1 if reference_available is None else reference_available
    folder=publications[2]['study'].parent.name
    families = families or (('ridge_return_repaired','probabilistic_state_space') if folder == 'causal_forecast_study_eurusd_v1' else
                            ('cross_pair_graph_transfer','modern_tabular_probabilistic_repaired','probabilistic_state_space','ridge_return_repaired'))
    payloads['contract']['cohorts']={family:'fixture:'+family for family in families}
    payloads['contract']['quote_max_age_sec']=60
    payloads['study']['contract_sha256']=hashlib.sha256(json.dumps(payloads['contract'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
    pointer=publications[0].parent.parent/'config/causal_forecast_study_current.json'
    if pointer.exists():
        selected=json.loads(pointer.read_text())
        selected['contract_sha256']=payloads['study']['contract_sha256']
        pointer.write_text(json.dumps(selected))
    publications[3]()
    path=make_ledger(publications,folder=folder,published=True)
    arms=[{'family':family,'cohort_id':'fixture:'+family,'instrument':'EUR_USD','side':1,'probability_up':'0.7',
           'reference_epoch':reference_epoch,'reference_mid':'1.10000000000000001','issued_epoch':NOW-5,
           'target_epoch':reference_epoch+3600,'predicted_return_bps':'1.25'} for family in payloads['contract']['cohorts']]
    if corruption=='future_issue':arms[0]['issued_epoch']=NOW+1
    if corruption=='invalid_probability':arms[0]['probability_up']='1.0000000000000000000000001'
    if corruption=='invalid_price':arms[0]['reference_mid']='NaN'
    payload={'decision_id':'forecast2','reference_epoch':reference_epoch,'reference_quote_id':'reference2','target_epoch':reference_epoch+3600,'forecasts':arms}
    raw=json.dumps(payload,sort_keys=True,separators=(',',':'))
    sha=hashlib.sha256(raw.encode()).hexdigest()
    db=_FIXTURE_CONNECT(path)
    db.execute('INSERT INTO quotes VALUES(?,?,?,?)',('reference2',reference_epoch,reference_available,'{}'))
    db.execute('UPDATE forecasts SET target=?,sha=?,payload=? WHERE id=?',(reference_epoch+3600,sha,raw,'forecast2'))
    db.execute('UPDATE publication SET forecast_sha=? WHERE id=?',(sha if corruption!='hash' else '0'*64,'forecast2'))
    db.commit();db.close()


def test_four_arm_publication_keeps_decimal_reference_and_original_clocks(publications):
    make_published_forecast(publications)
    result=read(publications)['model_attempts']
    assert result['forecast_payload_status']=='verified_retained_publication'
    publication=result['latest_published_forecast']
    assert publication['status']=='in_progress'
    assert publication['horizon_sec']==3600
    assert publication['publication_epoch']==NOW-4
    assert len(publication['forecasts'])==4
    assert publication['forecasts'][0]['reference_mid']=='1.10000000000000001'
    assert publication['forecasts'][0]['reference_epoch']==NOW-100
    assert publication['forecasts'][0]['issued_epoch']==NOW-5
    assert 'not_scored_accuracy' in publication['comparison_scope']


def test_actual_publication_shape_allows_market_tick_just_before_activation(publications):
    # Sanitized timing geometry from the first live EUR/USD companion: the
    # broker tick preceded registration by 0.155609s; observation followed it.
    select_eurusd_study(publications)
    reference=NOW-26.5518799
    publications[1]['study']['activated_epoch']=reference+.1556088
    make_published_forecast(publications,reference_epoch=reference,reference_available=reference+.4)
    result=read(publications)['model_attempts']
    publication=result['latest_published_forecast']
    assert result['forecast_payload_status']=='verified_retained_publication'
    assert publication['status']=='in_progress'
    assert len(publication['forecasts'])==2
    assert all(arm['reference_epoch']<publications[1]['study']['activated_epoch'] for arm in publication['forecasts'])
    assert publication['target_epoch']==reference+3600


@pytest.mark.parametrize('corruption',['missing','preactivation','stale','wrong_market','after_issue'])
def test_published_reference_needs_its_real_postactivation_fresh_observation(publications,corruption):
    make_published_forecast(publications)
    path=publications[0]/'causal_forecast_study_v1_io_r2/study.sqlite'
    db=_FIXTURE_CONNECT(path)
    if corruption=='missing':db.execute('DELETE FROM quotes')
    elif corruption=='preactivation':db.execute('UPDATE quotes SET available=?,market=?',(NOW-1001,NOW-1002))
    elif corruption=='stale':db.execute('UPDATE quotes SET available=?',(NOW-39,))
    elif corruption=='wrong_market':db.execute('UPDATE quotes SET market=?',(NOW-101,))
    else:db.execute('UPDATE quotes SET available=?,market=?',(NOW-3,NOW-4))
    db.commit();db.close()
    result=read(publications)['model_attempts']
    assert result['latest_published_forecast'] is None
    assert result['forecast_payload_status']=='invalid_retained_publication'


@pytest.mark.parametrize('corruption',['hash','future_issue','invalid_probability','invalid_price'])
def test_invalid_published_payload_cannot_populate_active_comparison(publications,corruption):
    make_published_forecast(publications,corruption=corruption)
    result=read(publications)['model_attempts']
    assert result['status']=='available'
    assert result['forecast_payload_status']=='invalid_retained_publication'
    assert result['latest_published_forecast'] is None


def test_elapsed_forecast_remains_historical_not_in_progress(publications):
    make_published_forecast(publications)
    result=dashboard.summarize_collection_status(publications[0],now_epoch=NOW+3600)
    assert result['model_attempts']['latest_published_forecast']['status']=='target_elapsed'
    assert result['forecasting']['observation_current'] is False


def test_cached_ledger_does_not_query_each_main_refresh(publications,monkeypatch):
    make_ledger(publications)
    first = read(publications)
    def no_second_query(*args,**kwargs):
        raise AssertionError('A second refresh must reuse the scoped cache')
    monkeypatch.setattr(dashboard.sqlite3,'connect',no_second_query)
    second = read(publications)
    assert second['model_attempts']['latest_attempt'] == first['model_attempts']['latest_attempt']
    assert second['model_attempts']['observed_epoch'] == first['model_attempts']['observed_epoch']
    assert second['model_attempts']['cache_age_sec'] >= 0


def test_published_forecast_retains_scoped_cache_identity(publications,monkeypatch):
    make_published_forecast(publications)
    first=read(publications)
    assert first['model_attempts']['latest_published_forecast'] is not None
    def no_second_query(*args,**kwargs):
        raise AssertionError('A published forecast must not disable the bounded refresh cache')
    monkeypatch.setattr(dashboard.sqlite3,'connect',no_second_query)
    second=read(publications)
    assert second['model_attempts']['latest_published_forecast']==first['model_attempts']['latest_published_forecast']
    assert second['model_attempts']['observed_epoch']==first['model_attempts']['observed_epoch']


@pytest.mark.parametrize('field,value',[('contract_sha','f'*64),('activation',NOW-999),('future_diagnostic',NOW+1),('oversized','x'*8193)])
def test_invalid_ledger_evidence_is_unknown_not_clean_or_ready(publications,field,value):
    path = make_ledger(publications)
    db = _FIXTURE_CONNECT(path)
    if field=='contract_sha':
        db.execute('UPDATE contract SET sha=?',(value,))
    elif field=='activation':
        db.execute('UPDATE activation SET epoch=?',(value,))
    elif field=='future_diagnostic':
        db.execute('UPDATE diagnostics SET epoch=?',(value,))
    else:
        db.execute('UPDATE diagnostics SET payload=?',(value,))
    db.commit();db.close()
    result=read(publications)
    assert result['model_attempts']['status']=='unavailable'
    assert result['model_attempts']['counts']=={}
    assert result['forecasting']['state']!='published'
    assert result['warmup']['last_reported_common_bars'] is None


def test_query_count_caps_are_explicit(publications,monkeypatch):
    path=make_ledger(publications)
    db=_FIXTURE_CONNECT(path)
    db.executemany('INSERT INTO attempts VALUES(?,?,?)',[(i,NOW-5,'reference') for i in range(2,8)])
    db.commit();db.close()
    monkeypatch.setattr(dashboard,'COLLECTION_LEDGER_ROW_LIMIT',3)
    result=read(publications)['model_attempts']
    assert result['counts']['attempts']==3
    assert result['counts_capped']==['attempts']


def select_gap_study(publications):
    root,payloads,paths,persist=publications
    payloads['contract']['contract_id']='causal_four_family_future_collection_gap_v2_20260907'
    payloads['contract']['input_policy']={'minimum_current_common_bars':61}
    sha=hashlib.sha256(json.dumps(payloads['contract'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
    payloads['study'].update(contract_id=payloads['contract']['contract_id'],contract_sha256=sha,phase='collecting',last_reason='')
    paths['contract']=root.parent.parent/'config/causal_forecast_study_gap_v2_20260907.json'
    paths['study']=root/'causal_forecast_study_gap_v2/heartbeat.json'
    persist()
    pointer=root.parent.parent/'config/causal_forecast_study_current.json'
    pointer.write_text(json.dumps({'schema_version':'causal_study_current_pointer_v1','selected_study':'gap_v2','contract_sha256':sha}))
    return pointer


def test_explicit_gap_selector_uses_new_path_hash_and_required_bars(publications):
    select_gap_study(publications)
    make_ledger(publications,folder='causal_forecast_study_gap_v2',bars=47,required=61)
    result=read(publications)
    assert result['selected_study']=='gap_v2'
    assert result['running'] is True
    assert result['warmup']['required_common_m1_bars']==61
    assert result['warmup']['last_reported_common_bars']==47
    assert 'historical training segments' in result['warmup']['explanation']
    assert result['forecasting']['blocked'] is True


def select_eurusd_study(publications):
    root,payloads,paths,persist=publications
    payloads['contract']['contract_id']='causal_eurusd_two_family_collection_v1_20260907'
    payloads['contract']['input_policy']={'minimum_current_common_bars':61}
    sha=hashlib.sha256(json.dumps(payloads['contract'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
    payloads['study'].update(contract_id=payloads['contract']['contract_id'],contract_sha256=sha,phase='collecting',last_reason='')
    paths['contract']=root.parent.parent/'config/causal_forecast_study_eurusd_v1_20260907.json'
    paths['study']=root/'causal_forecast_study_eurusd_v1/heartbeat.json'
    persist()
    pointer=root.parent.parent/'config/causal_forecast_study_current.json'
    pointer.write_text(json.dumps({'schema_version':'causal_study_current_pointer_v1','selected_study':'eurusd_v1','contract_sha256':sha}))
    return pointer


def test_eurusd_selector_publishes_exactly_its_two_fixed_families(publications):
    select_eurusd_study(publications)
    make_published_forecast(publications)
    result=read(publications)
    assert result['selected_study']=='eurusd_v1'
    assert result['running'] is True
    assert result['registered_study_trading_enabled'] is False
    assert result['expected_model_count']==2
    assert result['warmup']['required_common_m1_bars']==61
    assert result['warmup']['bar_label']=='EUR/USD minute bars'
    publication=result['model_attempts']['latest_published_forecast']
    assert publication['status']=='in_progress'
    assert {arm['family'] for arm in publication['forecasts']}=={'ridge_return_repaired','probabilistic_state_space'}
    assert len(publication['forecasts'])==2
    assert publication['horizon_sec']==3600
    assert all(arm['reference_mid']=='1.10000000000000001' for arm in publication['forecasts'])


@pytest.mark.parametrize('selection,families',[
    ('eurusd_v1',('cross_pair_graph_transfer','probabilistic_state_space')),
    ('eurusd_v1',('ridge_return_repaired','probabilistic_state_space','cross_pair_graph_transfer','modern_tabular_probabilistic_repaired')),
    ('gap_v2',('ridge_return_repaired','probabilistic_state_space')),
    ('gap_v2',('one','two','three','four')),
])
def test_selected_study_cannot_redefine_fixed_model_families(publications,selection,families):
    (select_eurusd_study if selection=='eurusd_v1' else select_gap_study)(publications)
    make_published_forecast(publications,families=families)
    result=read(publications)['model_attempts']
    assert result['forecast_payload_status']=='invalid_retained_publication'
    assert result['latest_published_forecast'] is None


def test_eurusd_selector_hash_mismatch_does_not_use_healthy_old_study(publications):
    pointer=select_eurusd_study(publications)
    value=json.loads(pointer.read_text());value['contract_sha256']='f'*64
    pointer.write_text(json.dumps(value))
    result=read(publications)
    assert result['running'] is None
    assert result['registered_study_trading_enabled'] is None
    assert result['model_attempts']['status']=='unavailable'


@pytest.mark.parametrize('bad_pointer',[None,{}, {'selected_study':{}},
    {'schema_version':'causal_study_current_pointer_v1','selected_study':'../other','contract_sha256':'0'*64},
    {'schema_version':'causal_study_current_pointer_v1','selected_study':'io_r2','contract_sha256':'0'*64}])
def test_invalid_or_mismatched_pointer_never_falls_back_to_valid_legacy(publications,bad_pointer):
    path=publications[0].parent.parent/'config/causal_forecast_study_current.json'
    path.write_text(json.dumps(bad_pointer))
    result=read(publications)
    assert result['running'] is None
    assert result['registered_study_trading_enabled'] is None
    assert result['model_attempts']['status']=='unavailable'


def test_stale_collector_keeps_dated_rejection_without_claiming_liveness(publications):
    make_ledger(publications)
    publications[1]['study']['generated_epoch']=NOW-100
    publications[3]()
    result=read(publications)
    assert result['running'] is None
    assert result['forecasting']['last_rejection_epoch']==NOW-8
    assert result['forecasting']['observation_current'] is False


def test_main_html_prioritizes_market_context_bot_and_positions_with_freshness():
    node=shutil.which('node')
    if node is None:
        pytest.skip('Node.js unavailable for isolated HTML-function test')
    html=Path(dashboard.__file__).with_name('oanda_main_signal_dashboard.html').read_text(encoding='utf-8')
    body=html[html.index('<main>'):html.index('<script>')]
    assert body.index('id="market-overview"')<body.index('id="collection-status"')<body.index('id="account"')<body.index('id="available-signals"')
    assert body.index('Historical observations &amp; research')<body.index('id="live-movers"')
    functions=html[html.index("    let marketOverviewWindow="):html.index('    function levelTrack(row)')]
    account=html[html.index('    function renderAccount(data)'):html.index('    function renderNarrativeMeter(data)')]
    data={'market_overview':{'generated_epoch':NOW,'rows':[{'instrument':'EUR_USD','status':'current','quote_age_sec':5,'mid':1.1,'tradeable':True,'spread_pips':1,
            'changes':{'15m':{'return_bps':2,'price_change_pips':2.2}},'technical':{'status':'current','trend_15m':'up','momentum_bps_5m':1,'momentum_bps_15m':2}}]},
          'news_sentiment':{'fresh':True,'pair_bias_status':'current','generated_utc':stamp(3),'as_of_utc':stamp(5),'top_pairs':[{'instrument':'USD_JPY','status':'current','as_of_utc':stamp(5),'direction':'long','events':[{'headline':'<unsafe>'}]}]},
          'account':{'verified_at_utc':stamp(5),'positions_current':True,'orders_current':True,'account_values_current':True,'positions':[],'nav':41,'pending_orders':0}}
    harness="""const elements={};const $=id=>elements[id]??={innerHTML:''};
const num=(v,d=2)=>v==null?'—':Number(v).toFixed(d);const signed=(v,d=2)=>v==null?'—':(Number(v)>=0?'+':'')+Number(v).toFixed(d);const cls=v=>Number(v)>0?'good':Number(v)<0?'bad':'';
const esc=v=>String(v??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));const restoreDetailState=()=>{};
"""+f"Date.now=()=>{NOW*1000};\n"+functions+account+'\nconst data='+json.dumps(data)+""";
renderMarketOverview(data);renderAvailableSignals(data);renderAccount(data);
const current=JSON.parse(JSON.stringify(elements));Date.now=()=>Date.now.old;Date.now.old="""+str((NOW+400)*1000)+""";
renderMarketOverview(data);renderAvailableSignals(data);renderAccount(data);
process.stdout.write(JSON.stringify({current,stale:elements}));"""
    result=json.loads(subprocess.run([node],input=harness,capture_output=True,text=True,encoding='utf-8',check=True,timeout=15).stdout)
    assert '+0.020%' in result['current']['market-overview']['innerHTML']
    assert '&lt;unsafe&gt;' in result['current']['available-signals']['innerHTML']
    assert 'No open positions.' in result['current']['account']['innerHTML']
    assert 'Current market prices unavailable' in result['stale']['market-overview']['innerHTML']
    assert '+0.020%' not in result['stale']['market-overview']['innerHTML']
    assert '<unsafe>' not in result['current']['available-signals']['innerHTML']
    assert 'not confirmed flat' in result['stale']['account']['innerHTML']


def test_global_zero_direction_count_without_verified_pair_mapping_is_unavailable():
    node=shutil.which('node')
    if not node:
        pytest.skip('Node is needed for isolated renderer fixture')
    html=(Path(__file__).parent/'oanda_main_signal_dashboard.html').read_text(encoding='utf-8')
    functions=html[html.index('    let marketOverviewWindow='):html.index('    function renderAvailableSignals(data)')]
    data={'market_overview':{'generated_epoch':NOW,'rows':[{'instrument':'EUR_USD','status':'current','quote_age_sec':5,'mid':1.1,'spread_pips':1}]},
          'news_sentiment':{'fresh':True,'generated_utc':stamp(3),'active_article_count':23,'active_scored_pair_count':0,'top_pairs':[]}}
    harness="""const elements={};const $=id=>elements[id]??={innerHTML:''};
const num=(v,d=2)=>v==null?'—':Number(v).toFixed(d);const signed=(v,d=2)=>v==null?'—':(Number(v)>=0?'+':'')+Number(v).toFixed(d);const cls=()=>'';
const esc=v=>String(v??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
"""+f'Date.now=()=>{NOW*1000};\n'+functions+'\nconst data='+json.dumps(data)+""";
const render=()=>{renderMarketOverview(data);return elements['market-overview'].innerHTML};
const screened=render();delete data.news_sentiment.active_scored_pair_count;const missingCount=render();
data.news_sentiment.active_scored_pair_count=2;const missingPair=render();
data.news_sentiment.active_scored_pair_count=0;data.news_sentiment.fresh=false;const stale=render();
process.stdout.write(JSON.stringify({screened,missingCount,missingPair,stale}));"""
    result=json.loads(subprocess.run([node],input=harness,capture_output=True,text=True,encoding='utf-8',check=True,timeout=15).stdout)
    for label in ('screened','missingCount','missingPair','stale'):
        assert 'No directional news' not in result[label]
        assert 'articles screened' not in result[label]
        assert 'Unavailable' in result[label]


def test_selected_market_window_controls_technical_momentum_and_two_model_comparison():
    node=shutil.which('node')
    if not node:
        pytest.skip('Node is needed for isolated renderer fixture')
    html=(Path(__file__).parent/'oanda_main_signal_dashboard.html').read_text(encoding='utf-8')
    functions=html[html.index('    let marketOverviewWindow='):html.index('    function renderAvailableSignals(data)')]
    data={'market_overview':{'generated_epoch':NOW,'rows':[{'instrument':'EUR_USD','status':'current','quote_age_sec':5,'quote_epoch':NOW-5,'mid':1.1,'spread_pips':1,
              'changes':{'5m':{'return_bps':-2,'price_change_pips':-2.2},'15m':{'return_bps':3,'price_change_pips':3.3},'60m':{'return_bps':0,'price_change_pips':0}},
              'technical':{'status':'current','as_of_epoch':NOW-5,'trend_15m':'legacy_wrong_window','momentum_bps_15m':999}}]},
          'collection_status':{'model_attempts':{'latest_published_forecast':{'status':'in_progress','publication_epoch':NOW-4,'target_epoch':NOW+3500,'instrument':'EUR_USD',
              'forecasts':[{'family':family,'side':1,'probability_up':.6,'predicted_return_bps':1,'reference_mid':'1.099','reference_epoch':NOW-100,'issued_epoch':NOW-5}
                           for family in ('ridge_return_repaired','probabilistic_state_space')]}}}}
    harness="""const elements={};const $=id=>elements[id]??={innerHTML:''};
const num=(v,d=2)=>v==null?'—':Number(v).toFixed(d);const signed=(v,d=2)=>v==null?'—':(Number(v)>=0?'+':'')+Number(v).toFixed(d);const cls=()=>'';
const esc=v=>String(v??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
"""+f'Date.now=()=>{NOW*1000};\n'+functions+'\nconst data='+json.dumps(data)+""";
const result={};for(const window of ['5m','15m','60m']){marketOverviewWindow=window;renderMarketOverview(data);result[window]=elements['market-overview'].innerHTML;}
delete data.market_overview.rows[0].changes['60m'];renderMarketOverview(data);result.missing=elements['market-overview'].innerHTML;
process.stdout.write(JSON.stringify(result));"""
    result=json.loads(subprocess.run([node],input=harness,capture_output=True,text=True,encoding='utf-8',check=True,timeout=15).stdout)
    for window,value,direction in (('5m','-2.00','Down'),('15m','+3.00','Up'),('60m','+0.00','Flat')):
        assert f'Technical bias ({window})' in result[window]
        assert f'{window} momentum {value} bps' in result[window]
        assert f'<td>{direction}<div' in result[window]
        assert 'legacy_wrong_window' not in result[window]
        assert '999.00' not in result[window]
        assert '2 model predictions' in result[window]
        assert 'Ridge: Up' in result[window]
        assert 'State space: Up' in result[window]
        assert result[window].index('Other price-only models') < result[window].index('Ridge: Up') < result[window].index('forecast-comparison-EUR_USD')
        assert 'in progress, unscored' in result[window]
    assert '60m momentum' not in result['missing']
    assert 'No 60m history anchor' in result['missing']
