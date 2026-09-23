from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

import oanda_all68_m1_cadence_v3 as candidate

BASE = 1789351200  # Explicit synthetic scheduler time, not a current-source claim.


def finish(scheduler,pair,kind,now,mono,*,last_close=None,error=''):
    result={'error':error}
    if last_close is not None:result['after_last']=datetime.fromtimestamp(last_close-60,timezone.utc).isoformat()
    scheduler.finish(pair,kind,result,now,mono)


def test_slow_gap_spanning_boundary_does_not_block_other_67_pairs():
    pairs=[f'P{i:02d}_USD' for i in range(68)]
    scheduler=candidate.Scheduler(pairs,{p:BASE for p in pairs})
    for state in scheduler.state.values():state['forward_jobs']=1
    assert scheduler.plan(BASE+59,0)==[(pairs[0],'gap')]
    active={(pairs[0],'gap'):(30.,BASE)}
    completions={};same_pair_overlap=False;max_forward=0;max_gap=0
    mono=0.
    while mono<=78 and len(completions)<68:
        now=BASE+59+mono
        for job,(end,target) in list(active.items()):
            if end<=mono:
                pair,kind=job
                finish(scheduler,pair,kind,now,mono,last_close=target if kind=='forward' else None)
                active.pop(job)
                if kind=='forward' and target>=BASE+60:completions.setdefault(pair,mono-1)
        for pair,kind in scheduler.plan(now,mono):
            same_pair_overlap |= any(p==pair for p,_ in active)
            active[(pair,kind)]=(mono+(30 if kind=='gap' else 2),scheduler.active[(pair,kind)])
        max_forward=max(max_forward,sum(k=='forward' for _,k in active))
        max_gap=max(max_gap,sum(k=='gap' for _,k in active))
        mono+=.25
    assert len(completions)==68
    assert min(completions[p] for p in pairs[1:])<30
    assert max(completions.values())<=75
    assert 29<=completions[pairs[0]]<=75  # Same pair waits; never claim zero delay.
    assert not same_pair_overlap
    assert max_forward==3 and max_gap==1


def test_forward_missing_bar_and_error_retry_have_floor_and_remain_fair():
    scheduler=candidate.Scheduler(['EUR_USD','GBP_USD','USD_JPY','AUD_USD'])
    jobs=scheduler.plan(BASE+2,0)
    assert len(jobs)==3
    for pair,kind in jobs:finish(scheduler,pair,kind,BASE+3,1,error='network_unavailable')
    remaining=scheduler.plan(BASE+3,1)
    assert len(remaining)==1 and remaining[0][0] not in {p for p,_ in jobs}
    assert not scheduler.plan(BASE+16.99,14.99)
    assert len(scheduler.plan(BASE+17,15))==2  # One remaining forward is active.
    assert all(state['latest_close']==0 for state in scheduler.state.values())


def test_boundary_due_overrides_no_old_120_second_cycle_delay():
    scheduler=candidate.Scheduler(['EUR_USD'],{'EUR_USD':BASE})
    assert scheduler.plan(BASE+61.99,0)==[]
    assert scheduler.plan(BASE+62,0)==[('EUR_USD','forward')]


def test_gap_floor_and_no_simultaneous_pair_writer():
    scheduler=candidate.Scheduler(['EUR_USD'],{'EUR_USD':BASE})
    scheduler.state['EUR_USD']['forward_jobs']=1
    assert scheduler.plan(BASE+10,0)==[('EUR_USD','gap')]
    assert scheduler.plan(BASE+62,52)==[]
    finish(scheduler,'EUR_USD','gap',BASE+63,53,error='gap_failed')
    assert scheduler.plan(BASE+63,53)==[('EUR_USD','forward')]
    finish(scheduler,'EUR_USD','forward',BASE+64,54,last_close=BASE+60)
    assert scheduler.plan(BASE+65,55)==[]
    assert scheduler.state['EUR_USD']['next_gap']==953
    assert scheduler.state['EUR_USD']['errors']['gap']=='gap_failed'
    assert scheduler.state['EUR_USD']['errors']['forward']==''


def test_global_actual_get_start_rate_is_bounded():
    clock=[0.]
    limiter=candidate.GetRateLimit(clock=lambda:clock[0],sleep=lambda delay:clock.__setitem__(0,clock[0]+delay))
    starts=[limiter.acquire() for _ in range(17)]
    for end in starts:
        assert sum(end-1<start<=end for start in starts)<=4
    assert starts[4]>=1 and starts[-1]>=4


def candle(at,complete=True):
    return {'time':datetime.fromtimestamp(at,timezone.utc).isoformat(),'complete':complete,'volume':12,
      **{side:{'o':'1.1000','h':'1.1002','l':'1.0998','c':'1.1001'} for side in ('mid','bid','ask')}}


class Client:
    def __init__(self,payload):self.payload=payload;self.calls=[]
    def candles(self,instrument,**kwargs):self.calls.append((instrument,kwargs));return self.payload


def scoped(payload,*,now=BASE+62):
    client=Client(payload)
    wrapper=candidate.CandleGetOnly(client,'EUR_USD',candidate.GetRateLimit(),clock=lambda:datetime.fromtimestamp(now,timezone.utc))
    return wrapper,client


def fetch(wrapper):return wrapper.candles('EUR_USD',granularity='M1',count=10,end_time=None,price='BAM')


def test_get_proxy_preserves_original_payload_and_receipt_clock():
    payload={'instrument':'EUR_USD','granularity':'M1','candles':[candle(BASE),candle(BASE+60,False)],'_http_status':200}
    wrapper,client=scoped(payload)
    assert fetch(wrapper) is payload
    assert wrapper.receipts[0]['structured_response_sha256']==hashlib.sha256(candidate.encoded(payload)).hexdigest()
    assert wrapper.receipts[0]['retrieval_completed_utc']==datetime.fromtimestamp(BASE+62,timezone.utc).isoformat()
    assert wrapper.receipts[0]['original_availability_claim'] is False
    assert len(client.calls)==1
    with pytest.raises(ValueError,match='single_M1'):fetch(wrapper)
    assert not hasattr(wrapper,'create_market_order') and not hasattr(wrapper,'get_account')


@pytest.mark.parametrize('payload,reason',[
 ({'instrument':'GBP_USD','granularity':'M1','candles':[]},'identity'),
 ({'instrument':'EUR_USD','granularity':'M5','candles':[]},'identity'),
 ({'instrument':'EUR_USD','granularity':'M1','candles':[candle(BASE+60)]},'future_or_unaligned'),
 ({'instrument':'EUR_USD','granularity':'M1','candles':[candle(BASE,complete='true')]},'complete_flag'),
])
def test_mismatched_and_incomplete_claims_rejected(payload,reason):
    wrapper,_=scoped(payload)
    with pytest.raises(ValueError,match=reason):fetch(wrapper)


def write_receipt(root,pair,kind,completed,*,started=False):
    value=dict(schema_version=candidate.SCHEMA,instrument=pair,job_kind=kind,source_bindings=candidate.SOURCE_BINDINGS)
    value['job_started_utc' if started else 'job_completed_utc']=datetime.fromtimestamp(completed,timezone.utc).isoformat()
    (root/f'{pair}.{kind}{".started" if started else ""}.json').write_bytes(candidate.encoded(value))


def test_restart_preserves_completed_and_inflight_retry_floor(tmp_path):
    write_receipt(tmp_path,'EUR_USD','forward',BASE+1)
    write_receipt(tmp_path,'EUR_USD','forward',BASE+5,started=True)
    write_receipt(tmp_path,'EUR_USD','gap',BASE+3,started=True)
    scheduler=candidate.Scheduler(['EUR_USD'])
    candidate.restore_state(scheduler,tmp_path,BASE+10,100)
    assert scheduler.state['EUR_USD']['last_attempt']==95
    assert scheduler.state['EUR_USD']['next_gap']==993
    assert scheduler.plan(BASE+10,100)==[]
    assert scheduler.plan(BASE+20,110)==[('EUR_USD','forward')]


def test_future_receipt_and_wrong_sources_refused(tmp_path):
    write_receipt(tmp_path,'EUR_USD','gap',BASE+30)
    scheduler=candidate.Scheduler(['EUR_USD'])
    with pytest.raises(ValueError,match='future'):candidate.restore_state(scheduler,tmp_path,BASE+10,10)
    path=tmp_path/'EUR_USD.gap.json';value=json.loads(path.read_bytes());value['source_bindings']={};path.write_bytes(candidate.encoded(value))
    with pytest.raises(ValueError,match='identity'):candidate.restore_state(scheduler,tmp_path,BASE+40,40)


def test_reused_source_bytes_are_verified(tmp_path):
    for name in candidate.SOURCE_BINDINGS:
        (tmp_path/name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path/name).write_text('not the reviewed source')
    with pytest.raises(ValueError,match='reused_source_changed'):candidate.source_check(tmp_path)


def test_forward_job_routes_gap_off_and_keeps_existing_writer_contract(tmp_path):
    seen=[]
    class Existing:
        @staticmethod
        def write_json_atomic(path,value):path.write_bytes(candidate.encoded(value))
        @staticmethod
        def update_pair(client,path,**kwargs):
            seen.append(kwargs)
            fetch(client)
            return {'after_last':datetime.fromtimestamp(BASE,timezone.utc).isoformat(),'rows_appended':1,'rows_recovered':0,'error':''}
    client=Client({'instrument':'EUR_USD','granularity':'M1','candles':[candle(BASE)],'_http_status':200})
    args=SimpleNamespace(candle_root=tmp_path,state_root=tmp_path)
    result=candidate.run_job(Existing,client,candidate.GetRateLimit(),args,'EUR_USD','forward',clock=lambda:datetime.fromtimestamp(BASE+62,timezone.utc))
    assert result['rows_appended']==1
    assert seen==[dict(max_requests=1,backfill_requests=0,batch_size=5000,pause_seconds=0.,dry_run=False,recover_gaps=False)]
    receipt=json.loads((tmp_path/'EUR_USD.forward.json').read_bytes())
    assert receipt['source_retrievals'][0]['retrieval_completed_utc']==receipt['job_completed_utc']
    assert (tmp_path/'EUR_USD.forward.started.json').exists()


def test_original_gap_writer_under_new_lane_preserves_bytes_and_actual_clocks(tmp_path,monkeypatch):
    sys.path.append(r'C:\Users\zmoor\Documents\forex\trad')
    import oanda_all68_m1_forward_updater_v2 as original
    import test_oanda_all68_m1_forward_updater as fixtures
    path=fixtures.fixture_archive(tmp_path)
    before=path.read_bytes().splitlines(keepends=True)
    payload=fixtures.broker_payload()
    now=fixtures.BASE.replace(hour=12)
    monkeypatch.setattr(original,'utc_now',lambda:now)
    args=SimpleNamespace(candle_root=tmp_path,state_root=tmp_path)
    result=candidate.run_job(original,Client(payload),candidate.GetRateLimit(),args,'EUR_USD','gap',clock=lambda:now)
    assert result['status']=='recovered_actual_broker_minute' and result['rows_recovered']==1
    after=path.read_bytes().splitlines(keepends=True)
    assert after[0]==before[0] and after[1]==before[1] and after[3:]==before[2:]
    receipt=json.loads((tmp_path/'EUR_USD.gap.json').read_bytes())
    assert receipt['source_retrievals'][0]['retrieval_completed_utc']==now.isoformat()
    source=json.loads(Path(result['observation_receipt']).read_bytes())
    assert source['response']==payload and source['response_observed_utc']==now.isoformat()
    assert source['historical_availability_asserted'] is False


def test_original_gap_omission_does_not_fill_or_retry_early(tmp_path,monkeypatch):
    sys.path.append(r'C:\Users\zmoor\Documents\forex\trad')
    import oanda_all68_m1_forward_updater_v2 as original
    import test_oanda_all68_m1_forward_updater as fixtures
    path=fixtures.fixture_archive(tmp_path)
    before=path.read_bytes();payload=fixtures.broker_payload(minute=0)
    now=fixtures.BASE.replace(hour=12);monkeypatch.setattr(original,'utc_now',lambda:now)
    args=SimpleNamespace(candle_root=tmp_path,state_root=tmp_path)
    client=Client(payload)
    first=candidate.run_job(original,client,candidate.GetRateLimit(),args,'EUR_USD','gap',clock=lambda:now)
    assert first['status']=='broker_omitted_requested_minute'
    second=candidate.run_job(original,client,candidate.GetRateLimit(),args,'EUR_USD','gap',clock=lambda:now)
    assert second['status']=='unresolved_no_retry_due' and len(client.calls)==1
    assert path.read_bytes()==before


def test_loaded_old_source_hashes_match_the_candidate_review():
    candidate.source_check(Path(r'C:\Users\zmoor\Documents\forex\trad'))


def test_directory_guard_normalizes_before_checking_containment(tmp_path,monkeypatch):
    owned=tmp_path/'data';owned.mkdir()
    monkeypatch.setattr(candidate,'DATA',owned)
    assert candidate.checked_directory(owned/'sub'/'..'/'inside')==owned/'inside'
    with pytest.raises(ValueError,match='outside_project_data'):
        candidate.checked_directory(owned/'sub'/'..'/'..'/'outside')

class SequenceClient:
    def __init__(self, *payloads):
        self.payloads = iter(payloads)
        self.calls = []
    def candles(self, instrument, **kwargs):
        self.calls.append((instrument, kwargs))
        return next(self.payloads)


def payload_at(*times, incomplete=None):
    rows = [candle(at) for at in times]
    if incomplete is not None:
        rows.append(candle(incomplete, False))
    return {'instrument': 'EUR_USD', 'granularity': 'M1', 'candles': rows, '_http_status': 200}


def probe_wrapper(client, last=BASE, now=BASE+3600):
    return candidate.ForwardProbeOnly(client, 'EUR_USD', candidate.GetRateLimit(),
        datetime.fromtimestamp(last, timezone.utc) if last is not None else None,
        lambda:datetime.fromtimestamp(now, timezone.utc))


def logical_fetch(wrapper, count=65):
    return wrapper.candles('EUR_USD', granularity='M1', count=count, end_time=None, price='BAM')


@pytest.mark.parametrize('payload', [payload_at(BASE), payload_at(), payload_at(BASE, incomplete=BASE+60)])
def test_stale_empty_or_incomplete_only_probe_does_not_download_large_page(payload):
    client = SequenceClient(payload)
    wrapper = probe_wrapper(client)
    assert logical_fetch(wrapper, 873) is payload
    assert [call[1]['count'] for call in client.calls] == [10]
    assert wrapper.decision == 'no_new_completed_minute'
    assert wrapper.receipts[0]['structured_response_sha256'] == hashlib.sha256(candidate.encoded(payload)).hexdigest()


def test_probe_proves_new_minute_then_retains_full_bounded_catchup_payload():
    probe = payload_at(*range(BASE+20*60, BASE+29*60, 60))
    full = payload_at(*range(BASE, BASE+29*60, 60))
    client = SequenceClient(probe, full)
    wrapper = probe_wrapper(client)
    assert logical_fetch(wrapper, 65) is full
    assert [c[1]['count'] for c in client.calls] == [10, 65]
    assert [r['retrieval_role'] for r in wrapper.receipts] == ['recent_tail_probe', 'bounded_catchup_after_new_minute']
    assert wrapper.decision == 'new_completed_minute_requires_original_catchup'
    with pytest.raises(ValueError, match='single_forward'):
        logical_fetch(wrapper)


@pytest.mark.parametrize('last,count,decision', [(BASE,10,'ordinary_tail'), (None,5000,'initial_bounded_page')])
def test_recent_or_empty_archive_keeps_one_original_request(last,count,decision):
    payload = payload_at(BASE)
    client = SequenceClient(payload)
    wrapper = probe_wrapper(client, last=last)
    assert logical_fetch(wrapper, count) is payload
    assert [c[1]['count'] for c in client.calls] == [count]
    assert wrapper.decision == decision


@pytest.mark.parametrize('failure_stage', ['probe','catchup'])
def test_http_error_retained_no_silent_probe_promotion(failure_stage):
    error = {'_error': True, '_http_status': 429, '_error_text': 'synthetic bounded rate response'}
    client = SequenceClient(error) if failure_stage == 'probe' else SequenceClient(payload_at(BASE+60), error)
    wrapper = probe_wrapper(client)
    assert logical_fetch(wrapper) is error
    assert len(client.calls) == (1 if failure_stage == 'probe' else 2)


def test_catchup_regression_refused_before_writer_receives_any_rows():
    client = SequenceClient(payload_at(BASE+120), payload_at(BASE+60))
    with pytest.raises(ValueError, match='regressed_behind_probe'):
        logical_fetch(probe_wrapper(client))
    assert len(client.calls) == 2


def test_probe_rejects_future_complete_without_catchup():
    client = SequenceClient(payload_at(BASE+3600))
    with pytest.raises(ValueError, match='future_or_unaligned'):
        logical_fetch(probe_wrapper(client))
    assert len(client.calls) == 1


def test_two_actual_requests_share_same_global_rate_limiter():
    clock = [0.]
    limiter = candidate.GetRateLimit(clock=lambda:clock[0], sleep=lambda delay:clock.__setitem__(0,clock[0]+delay))
    for _ in range(4): limiter.acquire()
    client = SequenceClient(payload_at(BASE+60), payload_at(BASE,BASE+60))
    wrapper = candidate.ForwardProbeOnly(client,'EUR_USD',limiter,datetime.fromtimestamp(BASE,timezone.utc),
        lambda:datetime.fromtimestamp(BASE+3600,timezone.utc))
    logical_fetch(wrapper)
    assert list(limiter.starts) == [1.,1.]


def test_successor_reuses_latest_predecessor_retry_without_rewriting_old_receipts(tmp_path):
    old = tmp_path/'old'; old.mkdir()
    new = tmp_path/'new'; new.mkdir()
    def write(root, schema, bindings, kind, when):
        record = {'schema_version':schema,'source_bindings':bindings,'instrument':'EUR_USD','job_kind':kind,
                  'job_started_utc':datetime.fromtimestamp(when,timezone.utc).isoformat()}
        (root/f'EUR_USD.{kind}.started.json').write_bytes(candidate.encoded(record))
    write(old,candidate.PREDECESSOR_SCHEMA,candidate.PREDECESSOR_BINDINGS,'forward',BASE+8)
    write(old,candidate.PREDECESSOR_SCHEMA,candidate.PREDECESSOR_BINDINGS,'gap',BASE+7)
    write(new,candidate.SCHEMA,candidate.SOURCE_BINDINGS,'forward',BASE+3)
    write(new,candidate.SCHEMA,candidate.SOURCE_BINDINGS,'gap',BASE+4)
    original = {p.name:p.read_bytes() for p in old.iterdir()}
    scheduler = candidate.Scheduler(['EUR_USD'])
    candidate.restore_state(scheduler,old,BASE+10,100,schema=candidate.PREDECESSOR_SCHEMA,bindings=candidate.PREDECESSOR_BINDINGS)
    candidate.restore_state(scheduler,new,BASE+10,100)
    assert scheduler.state['EUR_USD']['last_attempt'] == 98
    assert scheduler.state['EUR_USD']['next_gap'] == 997
    assert not scheduler.plan(BASE+20,110)
    assert scheduler.plan(BASE+23,113) == [('EUR_USD','forward')]
    assert {p.name:p.read_bytes() for p in old.iterdir()} == original


@pytest.mark.parametrize('argument', [str(candidate.ROOT/'oanda_all68_m1_cadence_v2.py'),'oanda_all68_m1_cadence_v2.py',str(candidate.ROOT/'oanda_all68_m1_forward_updater_v2.py')])
def test_competing_old_and_standalone_writers_refused(monkeypatch, argument):
    import psutil
    monkeypatch.setattr(psutil,'process_iter',lambda fields:[SimpleNamespace(info={'name':'python.exe','cmdline':['python.exe','-B',argument]})])
    assert candidate.old_writer_running()


def test_canonical_predecessor_owner_lock_excludes_successor_without_mutating_archive(tmp_path):
    lock = tmp_path/'worker.lock'
    with candidate.owner_lock(lock):
        with pytest.raises(OSError):
            with candidate.owner_lock(lock):
                pytest.fail('a second owner acquired the old lock')
    assert lock.read_bytes() == b'0'


def test_successor_updater_only_changes_pip_conversion_in_existing_functions():
    import ast
    old = ast.parse((candidate.ROOT/'oanda_all68_m1_forward_updater.py').read_text(encoding='utf-8-sig'))
    new = ast.parse((candidate.ROOT/'oanda_all68_m1_forward_updater_v2.py').read_text(encoding='utf-8-sig'))
    funcs = lambda tree:{n.name:ast.dump(n,include_attributes=False) for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))}
    old_functions,new_functions=funcs(old),funcs(new)
    changed = {name for name,value in old_functions.items() if new_functions.get(name)!=value}
    assert changed == {'_actual_gap_row'}
    assert set(new_functions)-set(old_functions) == {'verified_pip_sizes','registered_pip_size'}


def test_full_catchup_appends_every_new_row_and_preserves_existing_bytes(tmp_path,monkeypatch):
    import pandas as pd
    import oanda_all68_m1_forward_updater_v2 as writer
    import test_oanda_all68_m1_forward_updater as fixtures
    path = fixtures.fixture_archive(tmp_path,minutes=(0,))
    origin = int(fixtures.BASE.timestamp())
    original = path.read_bytes()
    def conversion(payload, instrument, granularity):
        rows = []
        for raw in payload['candles']:
            if not raw['complete']:continue
            row={'time':raw['time'],'datetime':raw['time'],'instrument':instrument,'granularity':granularity,
                 'volume':raw['volume'],'spread_pips':0.}
            for component,prefix in [('mid',''),('bid','bid_'),('ask','ask_')]:
                for key,name in [('o','open'),('h','high'),('l','low'),('c','close')]: row[prefix+name]=float(raw[component][key])
            rows.append(row)
        return pd.DataFrame(rows)
    monkeypatch.setattr(writer,'manager_module',lambda:SimpleNamespace(candle_df_from_oanda=conversion))
    now=datetime.fromtimestamp(origin+3600,timezone.utc)
    monkeypatch.setattr(writer,'utc_now',lambda:now)
    probe=payload_at(*range(origin+20*60,origin+30*60,60))
    complete=payload_at(*range(origin,origin+30*60,60))
    client=SequenceClient(probe,complete)
    result=candidate.run_job(writer,client,candidate.GetRateLimit(),SimpleNamespace(candle_root=tmp_path,state_root=tmp_path),
                             'EUR_USD','forward',clock=lambda:now)
    assert not result['error'],result
    assert result['rows_appended']==29 and result['actual_get_count']==2
    assert [call[1]['count'] for call in client.calls]==[10,63]
    assert path.read_bytes().startswith(original)
    actual=pd.read_csv(path)
    assert len(actual)==30 and len(set(actual.datetime))==30
    assert actual.datetime.iloc[1]==datetime.fromtimestamp(origin+60,timezone.utc).isoformat()
    # Historical same-minute quote changes are not silently rewritten by tail catch-up.
    assert path.read_bytes().splitlines()[1]==original.splitlines()[1]

@pytest.mark.parametrize('snapshot', [{}, {'quotes':{}}, {'quotes':{'EUR_USD':{'tradeable':False}}}, {'quotes':{'BOG_US':{}}}, 'malformed'])
def test_all68_universe_independent_of_transient_quote_snapshot(tmp_path,snapshot):
    import oanda_all68_m1_forward_updater_v2 as writer
    path=tmp_path/'quote.json';path.write_text(json.dumps(snapshot) if isinstance(snapshot,dict) else snapshot)
    pairs=candidate.configured_pairs(writer)
    assert len(pairs)==68 and pairs==sorted(writer.verified_pip_sizes())
    assert 'EUR_USD' in pairs and 'BOG_US' not in pairs


def test_unregistered_or_incomplete_registry_refused_before_any_get():
    with pytest.raises(ValueError,match='registered_all68'):
        candidate.configured_pairs(SimpleNamespace(verified_pip_sizes=lambda:{'EUR_USD':.0001}))


def test_graceful_stop_flag_scope(tmp_path):
    assert not candidate.stop_requested(tmp_path)
    (tmp_path/'unrelated.json').write_text('{}')
    assert not candidate.stop_requested(tmp_path)
    (tmp_path/'stop_requested.json').write_text('{"reason":"synthetic controlled handover"}')
    assert candidate.stop_requested(tmp_path)
