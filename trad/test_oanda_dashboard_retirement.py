"""Retired crypto routes must not read archives or start any collection."""
import json
from pathlib import Path
import re
import shutil
import subprocess
import threading
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import urlopen

from trad import oanda_practice_live_dashboard as dashboard


def test_retired_crypto_endpoint_returns_gone_without_loading_files(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('Retired crypto endpoint must not read any file')

    monkeypatch.setattr(Path, 'read_bytes', forbidden)
    with ThreadingHTTPServer(('127.0.0.1', 0), dashboard.DashboardHandler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            try:
                urlopen(f'http://127.0.0.1:{server.server_port}/api/crypto', timeout=3)
            except HTTPError as error:
                assert error.code == 410
                payload = json.load(error)
                assert payload['status'] == 'removed'
                assert payload['can_place_orders'] is False
            else:
                raise AssertionError('Retired route returned success')
        finally:
            server.shutdown()
            thread.join(timeout=3)


def test_frontend_forex_only_startup_and_unavailable_status(tmp_path):
    html = dashboard.MAIN_HTML_PATH.read_text(encoding='utf-8')
    scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.S)
    assert 'Crypto Shadow' not in html
    assert '/api/crypto' not in html
    assert 'crypto-page' not in html
    node = shutil.which('node')
    assert node, 'Node is required to verify dashboard JavaScript'
    for i, script in enumerate(scripts):
        path = tmp_path / f'script_{i}.js'
        path.write_text(script, encoding='utf-8')
        subprocess.run([node, '--check', str(path)], check=True, capture_output=True)
    script = scripts[0]
    health = script[script.index('    function renderDashboardHealth('):script.index('    const moverModeLabels=')]
    header = script[script.index('    function collectionHeaderState('):script.index('    function pairCoverageActivity(')]
    test = """
const assert=require('node:assert/strict');
const elements={};const $=id=>elements[id]??=( {innerHTML:''} );
const esc=value=>String(value).replaceAll('<','&lt;');
const availableForecastCoverage=()=>({joint:{current:false},label:'0 combined'});
""" + health + header + """
const failed={operational_dashboard:{selected:true,status:'unavailable',reason:'dashboard_source_changed'},collection_status:{observations:{quote_stream:{current:false}}}};
renderDashboardHealth(failed);
assert.match(elements['dashboard-health'].innerHTML,/no longer matches/);
assert.match(elements['dashboard-health'].innerHTML,/no fresh verified observation/);
assert.equal(collectionHeaderState(failed).text,'Dashboard connected · forecast data unavailable');
renderDashboardHealth({operational_dashboard:{selected:true,status:'partial_unavailable',price:{reason:'stale_or_future_summary'},joint:{reason:'model_source_changed'}}});
assert.match(elements['dashboard-health'].innerHTML,/no current verified update/);
assert.match(elements['dashboard-health'].innerHTML,/requires separate review/);
renderDashboardHealth({collection_status:{observations:{quote_stream:{current:true}}}});
assert.equal(elements['dashboard-health'].innerHTML,'');
"""
    path = tmp_path / 'status_test.js'
    path.write_text(test, encoding='utf-8')
    subprocess.run([node, str(path)], check=True, capture_output=True)


def test_forex_supervisor_cannot_launch_crypto():
    source = Path(__file__).with_name('oanda_always_on_supervisor.ps1').read_text(encoding='utf-8')
    assert 'EnableCrypto' not in source
    assert 'crypto_shadow_live_tracker.py' not in source
