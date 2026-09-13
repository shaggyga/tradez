"""Collection-status fixtures never start workers, open databases or use network."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess

import pytest

import oanda_practice_live_dashboard as dashboard

NOW = datetime(2026, 9, 6, 20, tzinfo=timezone.utc).timestamp()
CONTRACT_ID = "causal_four_family_future_collection_v1_io_r2_20260906"


def stamp(age=0):
    return datetime.fromtimestamp(NOW - age, timezone.utc).isoformat()


@pytest.fixture(autouse=True)
def forbid_database_and_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Collection dashboard tests forbid database/network access")
    monkeypatch.setattr(sqlite3, "connect", forbidden)
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
    result = dashboard.build_main_state(logs)
    assert result["collection_status"]["running"] is True
    assert result["active"] is False
    assert result["execution"]["worker_heartbeat"] == {}


def test_main_html_header_and_banner_keep_collection_separate_from_active_trading(publications):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js unavailable for isolated HTML-function test")
    html = Path(dashboard.__file__).with_name("oanda_main_signal_dashboard.html").read_text(encoding="utf-8")
    functions = html[html.index("    function collectionHeaderState(data)"):html.index("    function render(data)")]
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
    result = subprocess.run([node, "-e", harness], capture_output=True, text=True, check=True, timeout=15)
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
