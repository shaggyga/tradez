"""Disposable source caches/publications; no network, accounts or live writes."""
from copy import deepcopy
from contextlib import closing
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import oanda_feature_candle_inputs_v2 as inputs
import oanda_feature_event_inputs_v1 as events
import oanda_native_feature_candle_updater_v1 as native
import oanda_research_feature_observation_worker_v2 as worker
from oanda_feature_observations_v1 import build_observation_frame
from test_oanda_research_feature_observation_worker_v1 import candles, quotes, write_candles, valid_clock

NOW = datetime(2026,9,14,1,0,tzinfo=timezone.utc)


def open_candles(timeframe, count, now=NOW):
    seconds = inputs.SECONDS[timeframe]
    end = int(now.timestamp())//seconds*seconds
    output=[]
    while len(output)<count:
        end-=seconds
        at=datetime.fromtimestamp(end,timezone.utc)
        if not inputs.scheduled_closed(at):
            row=candles(timeframe,1,at+timedelta(seconds=seconds))[0]
            output.append(row)
    return list(reversed(output))


def test_completed_native_weekend_state_and_strict_ma_suffix(tmp_path):
    for timeframe in ("M1","M5","H1"):
        rows=open_candles(timeframe,720)
        path=tmp_path/f"EUR_USD_{timeframe}.csv"
        write_candles(path,"EUR_USD",timeframe,rows)
        kept,receipt=inputs.read_tail(path,"EUR_USD",timeframe,clock=lambda:NOW.isoformat())
        assert kept[-1]["time"] == rows[-1]["time"]
        assert receipt["source_read_completed_utc"] == NOW.isoformat()
        if timeframe=="M1":
            assert len(kept)==240
            assert receipt["scheduled_weekend_gaps_retained"]==0
        else:
            assert len(kept)==720
            assert receipt["scheduled_weekend_gaps_retained"]>=1


def test_intraday_gap_is_not_a_weekend_and_no_missing_rows_are_filled(tmp_path):
    rows=open_candles("M5",40)
    del rows[-6]
    path=tmp_path/"EUR_USD_M5.csv"
    write_candles(path,"EUR_USD","M5",rows)
    kept,receipt=inputs.read_tail(path,"EUR_USD","M5",observed_utc=NOW.isoformat())
    assert len(kept)==5
    assert receipt["discarded_rows_before_intraday_or_unknown_gap"]==34
    assert receipt["scheduled_weekend_gaps_retained"]==0


@pytest.mark.parametrize("mutation",["future","closed","incomplete"])
def test_closed_incomplete_future_bars_are_refused(tmp_path,mutation):
    row=open_candles("H1",1)[0]
    if mutation=="future":row["time"]=NOW.isoformat()
    if mutation=="closed":row["time"]="2026-09-12T12:00:00+00:00"
    if mutation=="incomplete":row["complete"]=False
    with pytest.raises(ValueError):inputs.validate_candle(row,3600,NOW)


def test_exact_aggregation_discards_partial_weekend_bucket():
    rows=open_candles("H1",720)
    aggregated=inputs.aggregate(rows,14400,3600)
    assert len(aggregated)>=60
    assert aggregated[-1]["time"]=="2026-09-11T16:00:00+00:00"
    with pytest.raises(ValueError,match="duplicate"):
        inputs.aggregate(rows+[rows[-1]],14400,3600)


def test_weekend_elapsed_calendar_including_dst():
    assert inputs.open_elapsed_seconds("2026-09-11T20:00:00Z","2026-09-13T23:00:00Z")==10800
    assert inputs.open_elapsed_seconds("2026-10-30T20:00:00Z","2026-11-01T23:00:00Z")==7200
    assert inputs.open_elapsed_seconds("2026-09-14T10:00:00Z","2026-09-14T11:00:00Z")==3600
    with pytest.raises(ValueError):inputs.open_elapsed_seconds("2026-08-01T00:00:00Z",NOW)


class FakeCandles:
    def __init__(self,payload):self.payload=payload;self.calls=[]
    def candles(self,pair,**kwargs):
        self.calls.append((pair,kwargs))
        return deepcopy(self.payload)


def test_native_collector_bootstrap_receipts_completed_only_and_incremental(tmp_path):
    rows=open_candles("H1",720)
    pending=deepcopy(rows[-1]);pending.update(time=NOW.isoformat(),complete=False)
    client=FakeCandles({"instrument":"EUR_USD","granularity":"H1","candles":rows+[pending]})
    report=native.refresh_one(client,tmp_path,"EUR_USD","H1",clock=lambda:NOW)
    assert client.calls[0][1]==dict(granularity="H1",count=720,end_time=None,price="BAM")
    assert report["retained_rows"]==720 and report["incomplete_rows_withheld"]==1
    path=tmp_path/"EUR_USD_H1.csv"
    assert hashlib.sha256(path.read_bytes()).hexdigest()==report["cache_sha256"]
    assert len(inputs.read_tail(path,"EUR_USD","H1",clock=lambda:NOW.isoformat())[0])==720
    client.payload["candles"]=rows[-4:]
    refreshed=native.refresh_one(client,tmp_path,"EUR_USD","H1",clock=lambda:NOW)
    assert client.calls[-1][1]["count"]==10
    assert refreshed["changed"] is False
    assert refreshed["retrieval_completed_utc"]==NOW.isoformat()


def test_event_availability_clocks_are_original_and_expiry_cannot_extend():
    observed=NOW.timestamp();published=observed-45;expires=observed+1
    group=events.event_group({"x":1},publication=published,observed=observed,expires=expires,evidence={"fixture":True})
    assert events.epoch(group["clock"]["bar_complete_utc"])==published
    assert events.epoch(group["clock"]["component_clocks"]["valid_until_utc"])==expires
    with pytest.raises(ValueError):events.event_group({"x":1},publication=published,observed=expires,expires=expires,evidence={})
    with pytest.raises(ValueError):events.event_group({"x":1},publication=observed+1,observed=observed,expires=expires,evidence={})


def test_v2_versioned_features_keep_ma_warmup_and_completed_higher_state():
    sets={tf:open_candles(tf,720) for tf in inputs.SECONDS}
    sets["M1"],_=inputs.supported_suffix(sets["M1"],60,allow_weekend=False)
    snapshot=worker.build_research_observation(quotes(now=NOW),{"EUR_USD":sets},
        source_read_completed_utc=NOW.isoformat(),clock=lambda:NOW.isoformat())
    readiness=snapshot["coverage"]["family_readiness"]
    assert readiness["rich_M1_fresh_pairs"]==1
    # 5 active hours since Friday20:00: first Monday H4 is incomplete, stale.
    assert readiness["structural_fresh_pairs_by_timeframe"]["H4"]==0
    assert readiness["structural_fresh_pairs_by_timeframe"]["H1"]==1
    frame=build_observation_frame(snapshot)
    assert frame["source"]["producer_contract_id"]==worker.SCHEMA
    assert frame["instruments"]["EUR_USD"]["groups"]["timeframe:H1"]["component_clocks"]["candle_calendar_contract"]==inputs.CALENDAR
    sets["M1"]=sets["M1"][-203:]
    snapshot=worker.build_research_observation(quotes(now=NOW),{"EUR_USD":sets},
        source_read_completed_utc=NOW.isoformat(),clock=lambda:NOW.isoformat())
    assert snapshot["coverage"]["family_readiness"]["rich_M1_fresh_pairs"]==0


def test_native_roots_and_immutable_archive_run_cycle(tmp_path):
    args=SimpleNamespace(quote_snapshot=tmp_path/"quotes.json",candle_root=tmp_path/"m1",native_candle_root=tmp_path/"native",
        book_snapshot=None,news_snapshot=None,model_study=[],archive_root=tmp_path/"v2",heartbeat=tmp_path/"heartbeat.json",
        clock_state=tmp_path/"clock.json",max_daily_archive_mib=128,minimum_free_mib=128,max_cycle_sec=45)
    args.quote_snapshot.write_text(json.dumps(quotes(now=NOW)))
    args.clock_state.write_text(json.dumps(valid_clock(NOW)))
    for tf in inputs.SECONDS:
        root=args.candle_root if tf=="M1" else args.native_candle_root
        write_candles(root/f"EUR_USD_{tf}.csv","EUR_USD",tf,open_candles(tf,720))
    result=worker.run_cycle(args,clock=lambda:NOW.isoformat())
    assert result["status"]=="published"
    with gzip.open(result["archive"],"rt") as stream: envelope=json.load(stream)
    assert envelope["frame"]["source"]["producer_contract_id"]==worker.SCHEMA
    assert Path(result["publication_receipt"]).exists()
    assert result["coverage"]["family_readiness"]["native_input_rows_by_pair"]["EUR_USD"]=={"M1":240,"M5":720,"H1":720}


def test_news_adapter_uses_real_repaired_replay_and_marks_empty_defaults(tmp_path):
    from test_oanda_local_news_sentiment_repair_v2 import make_repaired_fixture
    fixture=make_repaired_fixture(tmp_path,now=NOW.timestamp())
    original=deepcopy(fixture["snapshot"])
    groups,status=events.news_observations(fixture["snapshot"],["EUR_USD","AUD_NZD"],
        read_completed_utc=(NOW+timedelta(seconds=1)).isoformat(),clock=lambda:(NOW+timedelta(seconds=2)).isoformat())
    assert status["status"]=="verified_publication_observed"
    assert groups["EUR_USD"]["values"]["context_balance"] == -.4
    assert groups["EUR_USD"]["value_states"]["context_balance"]=="observed"
    assert groups["EUR_USD"]["value_states"]["vetted_balance"]=="default_no_evidence"
    assert all(v=="default_no_evidence" for v in groups["AUD_NZD"]["value_states"].values())
    assert fixture["snapshot"]==original
    with pytest.raises(ValueError,match="stale_or_future"):
        events.news_observations(original,["EUR_USD"],read_completed_utc=NOW.isoformat(),clock=lambda:(NOW+timedelta(minutes=6)).isoformat())
    original["source_bindings"][next(iter(original["source_bindings"]))]="0"*64
    with pytest.raises(ValueError,match="source_binding"):
        events.news_observations(original,["EUR_USD"],read_completed_utc=NOW.isoformat(),clock=lambda:NOW.isoformat())


def test_model_adapter_reads_real_disposable_consumed_ledger_and_refuses_expiry_or_binding(tmp_path,monkeypatch):
    from test_oanda_causal_forecast_ledger_pair_v2 import Clock,contract_fixture,issued
    import oanda_causal_forecast_inputs_pair_v2 as price_inputs
    from oanda_causal_forecast_ledger_pair_v2 import CausalForecastLedger
    monkeypatch.setattr(price_inputs,"validate_capture",lambda *a,**k:None)
    clock=Clock(NOW.timestamp())
    contract=contract_fixture()
    name=Path(events.__file__).name
    contract["source_bindings"]={name:events.source_hash(name)}
    path=tmp_path/"study/pairs/EUR_USD/probabilistic_state_space/study.sqlite"
    path.parent.mkdir(parents=True)
    with closing(CausalForecastLedger(path,contract,clock=clock,activate=True)) as ledger:
        issued(ledger,clock)
        before=ledger.counts()
        groups,report,evidence=events.model_observations([tmp_path/"study"],["EUR_USD"],clock=lambda:events.iso(clock()))
        assert report[0]["status"]=="verified_publication_observed"
        assert len(groups["EUR_USD"])==1
        assert len(evidence)==1 and ledger.counts()==before
        group=next(iter(groups["EUR_USD"].values()))
        assert group["input_timeframe"]=="EVENT"
        assert all(k.startswith("supervised_") for k in group["values"])
        clock.advance(3601)
        groups,report,_=events.model_observations([tmp_path/"study"],["EUR_USD"],clock=lambda:events.iso(clock()))
        assert not groups and "stale_or_future" in report[0]["reason"]


def test_native_joint_model_adapter_keeps_original_anchor_and_publication_receipts():
    from joint_native_anchor_v1 import make_native_anchor
    family="ridge_price_news_v1";pair="EUR_USD";t=NOW.timestamp()
    flags={"research_only":True,**{k:False for k in ("can_place_orders","can_promote","can_authorize","account_eligible","proof_eligible")}}
    cohort="joint_price_news_native_v1_20260913.EUR_USD.ridge_price_news_v1.fixture"
    protocol={"instrument":pair,"cohorts":{family:cohort},"model_version":"fixture_model","feature_version":"fixture_features","historical_start_utc":events.iso(t-100)}
    contract={"instrument":pair,"family":family,"pip_size":.0001,"cohorts":{family:cohort},"evaluation_protocol":protocol,
              "source_bindings":{Path(events.__file__).name:events.source_hash(Path(events.__file__).name)}}
    anchor=make_native_anchor(instrument=pair,pip_size=.0001,origin_bar_start_epoch=int(t-60),origin_mid=1.1,
        expected_signed_pips=2.,probability_up=.6,source_anchor_complete=True,source_observed_epoch=t+1,
        feature_decision_epoch=t+1,computation_started_epoch=t+2,computation_completed_epoch=t+3,issued_epoch=t+4)
    training={"training_news_available_max_epoch":t-10,"training_news_feature_cutoff_max_epoch":t-5,"training_label_maturity_max_epoch":t}
    arm={**flags,"family":family,"instrument":pair,"pip_size":.0001,"horizon_sec":3600,"input_timeframe":"M1","cohort_id":cohort,
         "model_version":"fixture_model","feature_version":"fixture_features","forecast_id":"decision:"+family,
         "native_anchor":anchor.as_dict(),"native_anchor_sha256":anchor.sha256,"reference_epoch":t,"target_epoch":t+3600,
         "reference_mid":"1.1","probability_up":.6,"side":1,"predicted_return_bps":10000*2.*.0001/1.1,
         "probability_event":"strict_native_target_close_above_original_origin_close","issued_epoch":t+4,
         "input_source_observed_epoch":t+1,"computation_started_epoch":t+2,"computation_completed_epoch":t+3,
         "feature_cutoff_epoch":t,"features_available_epoch":t+3,"training_labels_available_max_epoch":t+3,
         **training,"news_evidence_epoch":t-5,"news_generated_epoch":t-4,"news_first_observed_epoch":t-3,
         "news_available_epoch":t-2,"news_expires_epoch":t+295,"news_capture_sha256":"a"*64,
         "diagnostics":{**training,"matched_price_only_expected_pips":1.,"neutral_news_ablation_expected_pips":.5}}
    decision={**flags,"decision_id":"decision","attempt_id":"attempt","instrument":pair,"family":family,"pip_size":.0001,
        "reference_quote_id":"quote","reference_quote_role":"current_context_not_native_model_anchor","reference_epoch":t,
        "target_epoch":t+3600,"attempt_epoch":t+1,"native_anchor_sha256":anchor.sha256,"news_capture_sha256":"a"*64,"forecasts":[arm]}
    sha=events.digest(decision);pub={"epoch":t+5,"forecast_sha":sha}
    saved={"payload":json.dumps(contract),"sha":events.digest(contract)}
    activation={"epoch":t-100,"contract_sha":saved["sha"]}
    row={"id":"decision","payload":json.dumps(decision),"sha":sha,"published":t+5,"consumed":t+6,
        "publication_forecast_sha":sha,"consumption_forecast_sha":sha,"consumption_publication_sha":events.digest(pub),
        "attempt_id":"attempt","reference_quote_id":"quote","reference_quote":json.dumps({"quote_id":"quote","instrument":pair,"pip_size":.0001,"bid":"1.5","ask":"1.5002"})}
    values,receipt=events.verify_model_record(saved,activation,row,pair=pair,observed=t+7)
    assert values["predicted_return_bps"]==arm["predicted_return_bps"]
    assert events.epoch(receipt["original_reference_utc"])==t
    assert events.epoch(receipt["original_target_utc"])==t+3600
    row["consumption_publication_sha"]="0"*64
    with pytest.raises(ValueError,match="consumption_digest"):
        events.verify_model_record(saved,activation,row,pair=pair,observed=t+7)


def test_native_boundary_ignores_recent_maintenance_and_prioritizes_h1():
    t=NOW.timestamp()
    h1={"latest_bar_utc":events.iso(t-7200),"retrieval_completed_utc":events.iso(t-1)}
    m5={"latest_bar_utc":events.iso(t-600),"retrieval_completed_utc":events.iso(t-61)}
    assert native.due_priority("H1",h1,t)==(0,int(t))
    assert native.select_jobs(["EUR_USD"],{("EUR_USD","H1"):h1,("EUR_USD","M5"):m5},t,{},set(),set(),1)==[("EUR_USD","H1",int(t))]
    assert native.due_priority("H1",h1,t,last_attempt=t-1) is None
    assert native.due_priority("H1",h1,t,last_attempt=t-native.RETRY_SEC)==(0,int(t))
    h1["latest_bar_utc"]=events.iso(t-3600)
    assert native.due_priority("H1",h1,t) is None


def test_all68_hour_boundary_dispatch_with_four_inflight_and_rate_limit_under75s_without_sleep():
    """Discrete event simulation uses the production scheduling/rate decisions.

    Four old M5 GETs remain in flight for20s at the boundary. Native H1 GETs
    then take2s each. New H1 must preempt remaining M5; no real sleeps/network.
    """
    pairs=[f"AAA_{chr(65+i//26)}{chr(65+i%26)}B" for i in range(68)]
    base=NOW.timestamp();receipts={};attempts={};completed=set();active={};started=[];h1_completed=[]
    for pair in pairs:
        receipts[(pair,"H1")]={"latest_bar_utc":events.iso(base-7200),"retrieval_completed_utc":events.iso(base-1)}
        receipts[(pair,"M5")]={"latest_bar_utc":events.iso(base-600),"retrieval_completed_utc":events.iso(base-61)}
    for pair in pairs[:4]:active[(pair,"M5")]=(20.,int(base))
    rate=native.DispatchBudget();maximum=4
    for tick in range(751):
        now=round(tick*.1,1)
        for key,(ends,boundary) in list(active.items()):
            if ends<=now:
                pair,tf=key;active.pop(key);completed.add((pair,tf,boundary))
                receipts[key]={"latest_bar_utc":events.iso(base+now//native.SECONDS[tf]*native.SECONDS[tf]-native.SECONDS[tf]),"retrieval_completed_utc":events.iso(base+now)}
                if tf=="H1":h1_completed.append((pair,now))
        slots=rate.slots(now,len(active))
        jobs=native.select_jobs(pairs,receipts,base+now,attempts,set(active),completed,slots)
        for pair,tf,boundary in jobs:
            assert (pair,tf) not in active
            rate.reserve(now);attempts[(pair,tf)]=base+now;active[(pair,tf)]=(now+2.,boundary);started.append((now,pair,tf))
        maximum=max(maximum,len(active))
        if len(h1_completed)==68:break
    assert len(h1_completed)==68 and max(t for _,t in h1_completed)<=55
    assert maximum==4
    assert all(tf=="H1" for _,_,tf in started[:68])
    assert all(sum(at<=other<at+1 for other,_,_ in started)<=4 for at,_,_ in started)


def test_native_scheduler_actual_executor_fake_gets_no_overlapping_inputs(tmp_path,monkeypatch):
    import threading
    pairs=["EUR_USD","GBP_USD"];monkeypatch.setattr(native.existing,"priced_instruments",lambda p:pairs)
    class Client:
        def __init__(self):self.lock=threading.Lock();self.active=set();self.maximum=0;self.calls=[]
        def candles(self,pair,**kwargs):
            key=(pair,kwargs['granularity'])
            with self.lock:
                assert key not in self.active;self.active.add(key);self.maximum=max(self.maximum,len(self.active));self.calls.append(key)
            payload={"instrument":pair,"granularity":key[1],"candles":open_candles(key[1],10)}
            with self.lock:self.active.remove(key)
            return payload
    args=SimpleNamespace(candle_root=tmp_path/"native",quote_snapshot=tmp_path/"unused.json",heartbeat=tmp_path/"heart.json",report=tmp_path/"report.json",pause_sec=.1)
    client=Client();report=native.run_cycle(client,args,clock=lambda:NOW)
    assert report['errors']==[] and report['completed_requests']==4
    assert len(set(client.calls))==4 and client.maximum<=4
    assert report['scheduling_contract']==native.SCHEDULE
    assert report['configured_cache_receipts']==4
