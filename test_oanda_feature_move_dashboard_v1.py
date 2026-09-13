"""AST and isolated JavaScript fixtures only: no dashboard import/server/archive."""
import ast
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import threading
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / 'oanda_practice_live_dashboard.py'
HTML = ROOT / 'oanda_main_signal_dashboard.html'
PREDECESSOR_HTML_SHA256 = 'abb4150a22ace6321a0537ff4869fe02b8573568487c926b11ec93c1c7d666ef'
NOW = 1789308000.0


def source_tree():
    return ast.parse(SOURCE.read_bytes())


def scope():
    names = {'FEATURE_OBSERVATION_ARCHIVE_ROOT', 'FEATURE_MOVE_WINDOWS', 'FEATURE_MOVE_MAX_BYTES'}
    nodes = [node for node in source_tree().body if
             isinstance(node, ast.FunctionDef) and node.name in {'feature_move_window', 'feature_move_instrument', 'build_feature_move_response'}
             or isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id in names for target in node.targets)]
    assert len(nodes) == 6
    clock = SimpleNamespace(time=lambda: NOW, monotonic=lambda: 10.)
    result = dict(Path=Path, __file__=str(SOURCE), time=clock, datetime=datetime, timezone=timezone, json=json, Any=Any, re=re)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), 'exec'), result)
    return result


def payload(window=300):
    return dict(schema_version='feature_move_mapping_v1', status='available', generated_utc='2026-09-13T14:00:00+00:00',
                window_sec=window, instrument_filter=None, summary={}, feature_changes=[], pairs=[], limitations=[], can_place_orders=False,
                archive={'errors': [], 'bounded_sample': False})


@pytest.mark.parametrize('query,expected', [({}, 300), ({'window': ['300']}, 300), ({'window': ['900']}, 900), ({'window': ['3600']}, 3600)])
def test_only_three_exact_query_windows(query, expected):
    assert scope()['feature_move_window'](query) == expected


@pytest.mark.parametrize('query', [{'window': ['']}, {'window': ['300', '900']}, {'window': ['300.0']}, {'window': ['0300']},
    {'window': ['-300']}, {'window': [300]}, {'window': [True]}, {'window': [[]]}, {'window': '300'}, {'window': []},
    {'root': ['C:/private']}, {'window': ['../300']}])
def test_invalid_query_cannot_choose_other_paths_or_expand_work(query):
    with pytest.raises(ValueError):
        scope()['feature_move_window'](query)


@pytest.mark.parametrize('value', ['', 'eur_usd', 'EUR/USD', 'USD_USD', '../EUR_USD', 1, True, [], 'EUR_USD&root=x'])
def test_invalid_instrument_refused_before_reader(value):
    with pytest.raises(ValueError): scope()['feature_move_instrument']({'instrument': [value]})


def test_instrument_filter_exact_server_argument_and_echo():
    env = scope()
    assert env['feature_move_instrument']({}) is None
    assert env['feature_move_instrument']({'instrument': ['EUR_USD']}) == 'EUR_USD'
    for value in [[], ['EUR_USD', 'GBP_USD'], 'EUR_USD']:
        with pytest.raises(ValueError): env['feature_move_instrument']({'instrument': value})
    seen = []
    def reader(root, **kwargs):
        seen.append(kwargs['instrument'])
        return {**payload(), 'instrument_filter': kwargs['instrument']}
    assert env['build_feature_move_response'](300, instrument='EUR_USD', reader=reader, now_epoch=NOW)['status'] == 'available'
    assert seen == ['EUR_USD']
    assert env['build_feature_move_response'](300, instrument='EUR_USD', reader=lambda *a, **k: payload(), now_epoch=NOW)['status'] == 'unavailable'


def test_reader_only_receives_fixed_versioned_archive_and_asof_and_preserves_null_zero():
    env = scope()
    expected = payload(900)
    expected['feature_changes'] = [{'instrument': 'EUR_USD', 'before': None, 'after': 0, 'change': None}]
    expected['archive'] = {'errors': ['missing component'], 'bounded_sample': True}
    calls = []
    def read(root, **kwargs):
        calls.append((root, kwargs))
        return expected
    actual = env['build_feature_move_response'](900, reader=read, now_epoch=NOW)
    assert calls == [(ROOT / 'data/oanda_training_manager/feature_observations_v1',
                      {'as_of_utc': datetime.fromtimestamp(NOW, timezone.utc).isoformat(), 'window_sec': 900, 'instrument': None})]
    assert actual == expected and actual is not expected
    actual['feature_changes'][0]['after'] = 5
    assert expected['feature_changes'][0]['after'] == 0


@pytest.mark.parametrize('case', ['schema', 'orders', 'int_alias', 'wrong_window', 'feature_count', 'pair_count', 'nan', 'oversize'])
def test_reader_bad_publication_refuses_without_partial_rows(case):
    value = payload()
    if case == 'schema': value['schema_version'] = 'old'
    if case == 'orders': value['can_place_orders'] = 0
    if case == 'int_alias': value['window_sec'] = 300.0
    if case == 'wrong_window': value['window_sec'] = 900
    if case == 'feature_count': value['feature_changes'] = [{}] * 51
    if case == 'pair_count': value['pairs'] = [{}] * 69
    if case == 'nan': value['summary']['unknown'] = float('nan')
    if case == 'oversize': value['limitations'] = ['x' * (2 * 1024 * 1024)]
    result = scope()['build_feature_move_response'](300, reader=lambda *a, **k: value, now_epoch=NOW)
    assert result['status'] == 'unavailable' and result['feature_changes'] == [] and result['pairs'] == []
    assert result['can_place_orders'] is False


@pytest.mark.parametrize('status', ['waiting_for_observations', 'stale_observations', 'waiting_for_comparable_features', 'observation_validation_failed'])
def test_reader_status_is_preserved_and_never_promoted_to_available(status):
    value = {**payload(), 'status': status}
    assert scope()['build_feature_move_response'](300, reader=lambda *a, **k: value, now_epoch=NOW) == value


def test_reader_failure_is_named_private_text_hidden_and_later_read_recovers():
    env = scope()
    def fail(*a, **k): raise OSError('private content and path')
    failed = env['build_feature_move_response'](300, reader=fail, now_epoch=NOW)
    assert failed['reason'] == 'feature_observations_unavailable' and 'private' not in str(failed)
    assert env['build_feature_move_response'](300, reader=lambda *a, **k: payload(), now_epoch=NOW)['status'] == 'available'


@pytest.mark.parametrize('value', [True, 300.0, 0, 301, '300'])
def test_helper_refuses_invalid_window_before_reader(value):
    calls = []
    with pytest.raises(ValueError): scope()['build_feature_move_response'](value, reader=lambda *a, **k: calls.append(1), now_epoch=NOW)
    assert calls == []


@pytest.mark.parametrize('url,status,window', [('/api/feature-moves', 200, 300), ('/api/feature-moves?window=900', 200, 900),
    ('/api/feature-moves?window=3600', 200, 3600), ('/api/feature-moves?window=', 400, None),
    ('/api/feature-moves?window=300&window=900', 400, None), ('/api/feature-moves?root=elsewhere', 400, None)])
def test_route_is_independent_of_main_state_and_model_eligibility(url, status, window):
    node = next(method for cls in source_tree().body if isinstance(cls, ast.ClassDef) and cls.name == 'DashboardHandler'
                for method in cls.body if isinstance(method, ast.FunctionDef) and method.name == 'do_GET')
    env = scope()
    env.update(urlparse=urlparse, parse_qs=parse_qs)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE), 'exec'), env)
    calls, sent = [], []
    class Request:
        path = url
        def current_feature_moves(self, value, instrument): calls.append(value); assert instrument is None; return payload(value)
        def send_json(self, value, status=200): sent.append((value, status))
        def __getattr__(self, key): raise AssertionError('unrelated handler access:' + key)
    env['do_GET'](Request())
    assert sent[0][1] == status and calls == ([] if window is None else [window])


@pytest.mark.parametrize('query,expected', [('instrument=EUR_USD', 'EUR_USD'), ('instrument=EUR_USD&instrument=GBP_USD', None), ('instrument=', None), ('instrument=EUR%2FUSD', None)])
def test_route_pair_filter_dispatch_or_refusal(query, expected):
    node = next(method for cls in source_tree().body if isinstance(cls, ast.ClassDef) and cls.name == 'DashboardHandler'
                for method in cls.body if isinstance(method, ast.FunctionDef) and method.name == 'do_GET')
    env = scope(); env.update(urlparse=urlparse, parse_qs=parse_qs)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE), 'exec'), env)
    calls, sent = [], []
    request = SimpleNamespace(path='/api/feature-moves?window=300&' + query,
        current_feature_moves=lambda window, instrument: calls.append((window, instrument)) or payload(window),
        send_json=lambda value, status=200: sent.append(status))
    env['do_GET'](request)
    assert calls == ([(300, expected)] if expected else [])
    assert sent == ([200] if expected else [400])


def test_cache_is_window_scoped_byte_owned_and_invalidates_on_expiry_or_clock_rollback():
    cls = next(node for node in source_tree().body if isinstance(node, ast.ClassDef) and node.name == 'DashboardHandler')
    nodes = [node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == 'current_feature_moves'
             or isinstance(node, (ast.Assign, ast.AnnAssign)) and
             any(isinstance(target, ast.Name) and target.id in {'feature_move_cache', 'feature_move_cache_lock'}
                 for target in (node.targets if isinstance(node, ast.Assign) else [node.target]))]
    assert len(nodes) == 3
    cache_cls = ast.ClassDef(name='Cache', bases=[], keywords=[], body=nodes, decorator_list=[])
    ast.fix_missing_locations(cache_cls)
    tick, calls = [NOW, 10.], []
    def build(window, **kwargs): calls.append(window); return payload(window)
    env = dict(threading=threading, json=json, Any=Any, build_feature_move_response=build,
               time=SimpleNamespace(time=lambda: tick[0], monotonic=lambda: tick[1]))
    exec(compile(ast.Module(body=[cache_cls], type_ignores=[]), str(SOURCE), 'exec'), env)
    cache = env['Cache']
    first = cache.current_feature_moves(300)
    first['pairs'].append({'poison': True})
    assert cache.current_feature_moves(300)['pairs'] == []
    cache.current_feature_moves(900)
    assert calls == [300, 900]
    tick[:] = [NOW + 5, 15.]
    cache.current_feature_moves(300)
    tick[:] = [NOW + 4, 16.]
    cache.current_feature_moves(300)
    tick[:] = [NOW + 4.5, 15.5]
    cache.current_feature_moves(300)
    assert calls == [300, 900, 300, 300, 300]
    for pair in ['EUR_USD', 'GBP_USD', 'USD_JPY', 'USD_CAD', 'USD_CHF', 'AUD_USD', 'NZD_USD', 'EUR_GBP', 'EUR_JPY']:
        cache.current_feature_moves(300, pair)
    assert len(cache.feature_move_cache) == 8 and (300, None) not in cache.feature_move_cache


def test_only_two_marked_html_insertions_and_all_legacy_bytes_protected():
    raw = HTML.read_bytes()
    for part in ('PANEL', 'SCRIPT'):
        expression = rb'<!-- FEATURE_MOVE_' + part.encode() + rb'_V1_START -->.*?<!-- FEATURE_MOVE_' + part.encode() + rb'_V1_END -->'
        assert len(re.findall(expression, raw, re.S)) == 1
        raw = re.sub(expression, b'', raw, count=1, flags=re.S)
    assert hashlib.sha256(raw).hexdigest() == PREDECESSOR_HTML_SHA256


def node_check(body):
    node = shutil.which('node')
    if not node:
        candidates = sorted((Path.home() / '.cache/codex-runtimes').glob('*/dependencies/node/bin/node.exe'))
        assert candidates, 'Node is required for isolated UI tests'
        node = str(candidates[0])
    text = HTML.read_text(encoding='utf-8')
    added = text.split('<!-- FEATURE_MOVE_SCRIPT_V1_START -->', 1)[1].split('<!-- FEATURE_MOVE_SCRIPT_V1_END -->', 1)[0]
    script = added.split('<script>', 1)[1].split('</script>', 1)[0]
    harness = r"""const assert=require('node:assert/strict');
const elements=new Map();
const document={getElementById(id){if(!elements.has(id))elements.set(id,{open:false,innerHTML:'',listeners:{},addEventListener(name,callback){this.listeners[name]=callback;},querySelectorAll(){return [];}});return elements.get(id);}};
const setInterval=()=>0,setTimeout=()=>0,clearTimeout=()=>{};
let fetch=()=>{throw new Error('Unexpected fetch; no real endpoint allowed');};
""" + script + '\n(async()=>{\n' + body + '\nconsole.log("passed");\n})().catch(error=>{console.error(error);process.exitCode=1;});'
    result = subprocess.run([node], input=harness, capture_output=True, text=True, encoding='utf-8', timeout=15)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'passed'


def test_ui_missing_future_stale_quiet_and_categorical_are_distinct_and_escaped():
    value = payload(900)
    value['status'] = 'stale_observations'
    value['feature_changes'] = [dict(instrument='EUR_USD', feature_name='<script>bad</script>', feature_id='one',
        group='primary', input_timeframe='M1', before=None, after=0, change=None, status='unavailable',
        reason='future_feature_observation', pair_move_pct=None, history_count=0),
        dict(instrument='GBP_USD', feature_name='quiet', before=0, after=0, change=0, status='available',
             pair_move_pct=0, pair_move_status='available', unusual_percentile=0, history_count=6),
        dict(instrument='USD_JPY', feature_name='state', before='LOW', after='HIGH', status='state_transition', units='state')]
    node_check(f"const data={json.dumps(value)};const html=fmRender(data);" + r"""
assert(html.includes('stale observations'));assert(html.includes('future feature observation'));
assert(html.includes('Quiet — unchanged'));assert(html.includes('0.0th percentile'));
assert(html.includes('state transition'));assert(html.includes('LOW → HIGH'));
assert(html.includes('— → 0'));assert(html.includes('&lt;script&gt;bad&lt;/script&gt;'));
assert(!html.includes('<script>bad'));assert(html.includes('Not ranked'));
assert(fmRender({...data,status:'waiting_for_observations',feature_changes:[],pairs:[]}).includes('Missing observations are not zero changes'));
""")


def test_ui_archive_bounds_errors_rejected_and_hidden_rows_remain_visible():
    value = payload()
    value['archive'] = {'bounded_sample': True, 'errors': ['bad <archive>']}
    value['summary'] = dict(display_truncated=True, visible_feature_comparisons=50, total_feature_comparisons=1000,
                            rejected_frames={'future_frame': 2}, unavailable_reasons={'source_changed': 8})
    value['pairs'] = [dict(instrument='EUR_USD', status='available', move_pct=0, feature_count=100)]
    node_check(f"const data={json.dumps(value)};const html=fmRender(data,'EUR_USD');" + r"""
for(const text of ['Bounded archive sample','Archive read errors','bad &lt;archive&gt;','50 of 1000','before the display limit','future frame (2)','source changed (8)','EUR_USD','Quiet — unchanged'])assert(html.includes(text),text);
assert(!html.includes('bad <archive>'));
""")


def test_ui_pair_filter_and_sort_use_comparable_rank_not_incompatible_raw_feature_units():
    node_check(r"""
const rows=[{instrument:'EUR_USD',feature_name:'a',unusual_percentile:2,change:1000000,pair_move_pct:0.1},
{instrument:'GBP_USD',feature_name:'b',unusual_percentile:99,change:0.001,pair_move_pct:-0.9},
{instrument:'EUR_USD',feature_name:'c',unusual_percentile:null,change:1,pair_move_pct:0.1}];
const data={feature_changes:rows};assert.equal(fmRows(data,'','unusual')[0].feature_name,'b');
assert.equal(fmRows(data,'','pair')[0].instrument,'GBP_USD');assert.deepEqual(fmRows(data,'EUR_USD','name').map(x=>x.feature_name),['a','c']);
assert.equal(rows[0].feature_name,'a');
""")


def test_ui_newer_window_wins_and_failure_clears_previous_current_rows_without_main_request():
    value = payload(900)
    node_check(f"const sample={json.dumps(value)};" + r"""
const pending=[];fetch=(url,options)=>new Promise(resolve=>pending.push({url,resolve,options}));
document.getElementById('feature-move-panel').open=true;
const first=fmRefresh();fmWindow=300;const second=fmRefresh();
assert.deepEqual(pending.map(row=>row.url),['/api/feature-moves?window=900','/api/feature-moves?window=300']);
const response=data=>({ok:true,headers:{get:()=>null},text:async()=>JSON.stringify(data)});
pending[1].resolve(response({...sample,window_sec:300,status:'waiting_for_observations'}));await second;
pending[0].resolve(response(sample));await first;
assert.equal(fmPayload.window_sec,300);assert.equal(fmPayload.status,'waiting_for_observations');
fetch=async()=>{throw new Error('offline')};await fmRefresh();assert.equal(fmPayload,null);
assert(document.getElementById('feature-move-results').innerHTML.includes('previous rows are withheld'));
document.getElementById('feature-move-panel').open=false;fetch=()=>{throw new Error('closed panel requested')};await fmRefresh();
""")


def test_ui_wrong_shape_window_action_and_size_refused():
    value = payload(900)
    node_check(f"const data={json.dumps(value)};" + r"""
for(const change of [{can_place_orders:true},{window_sec:300},{schema_version:'old'},{pairs:[null]},{feature_changes:Array(51).fill({instrument:'EUR_USD'})}])assert.throws(()=>fmValidate({...data,...change},900));
document.getElementById('feature-move-panel').open=true;
fetch=async()=>({ok:true,headers:{get:()=>2097153},text:async()=>{throw new Error('must not decode oversized text');}});
await fmRefresh();assert.equal(fmPayload,null);
assert(document.getElementById('feature-move-results').innerHTML.includes('unavailable'));
""")


def test_ui_pair_selection_requests_server_filter_and_rejects_wrong_echo():
    value = payload(900)
    node_check(f"const data={json.dumps(value)};" + r"""
document.getElementById('feature-move-panel').open=true;
const calls=[];fetch=async(url)=>{calls.push(url);return {ok:true,headers:{get:()=>null},text:async()=>JSON.stringify({...data,instrument_filter:'GBP_USD',feature_changes:[{instrument:'GBP_USD',feature_name:'outside original global top50',status:'available'}]})};};
await fmChoosePair('GBP_USD');assert.deepEqual(calls,['/api/feature-moves?window=900&instrument=GBP_USD']);
assert.equal(fmPayload.instrument_filter,'GBP_USD');assert(document.getElementById('feature-move-results').innerHTML.includes('outside original global top50'));
assert.throws(()=>fmValidate({...data,instrument_filter:'EUR_USD'},900,'GBP_USD'));
assert.throws(()=>fmValidate({...data,instrument_filter:'GBP_USD',feature_changes:[{instrument:'EUR_USD'}]},900,'GBP_USD'));
""")
