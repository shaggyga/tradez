from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

import oanda_all68_m1_cadence_v2 as candidate

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
    for name in candidate.SOURCE_BINDINGS:(tmp_path/name).write_text('not the reviewed source')
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
    import oanda_all68_m1_forward_updater as original
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
    import oanda_all68_m1_forward_updater as original
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
