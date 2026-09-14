"""Owned synthetic clocks/candles, original calculator and archive semantics."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import oanda_research_feature_observation_worker_v1 as base
import oanda_research_feature_observation_worker_v2 as worker
from oanda_feature_observations_v1 import build_observation_frame, payload_sha256
from test_oanda_research_feature_observation_worker_v1 import candles, quotes, valid_clock, write_candles

NOW=datetime(2026,9,14,3,0,tzinfo=timezone.utc)
PAIRS=('AUD_USD','EUR_USD','GBP_USD','NZD_USD','USD_CAD','USD_CHF','USD_JPY','ZAR_JPY')


class Clock:
    def __init__(self):self.value=0.
    def monotonic(self):return self.value
    def utc(self):return (NOW+timedelta(seconds=self.value)).isoformat()
    def advance(self,seconds):self.value+=seconds


def args_fixture(tmp_path,pairs=PAIRS):
    args=SimpleNamespace(quote_snapshot=tmp_path/'quotes.json',candle_root=tmp_path/'m1',
        native_candle_root=tmp_path/'native',archive_root=tmp_path/'archive',heartbeat=tmp_path/'heartbeat.json',
        clock_state=tmp_path/'clock.json',book_snapshot=None,news_snapshot=None,model_study=[],
        max_daily_archive_mib=128,minimum_free_mib=128,max_cycle_sec=45,
        interval_sec=60,duration_sec=130,once=False)
    args.quote_snapshot.write_text(json.dumps(quotes(pairs,now=NOW)))
    args.clock_state.write_text(json.dumps(valid_clock(NOW)))
    for pair in pairs:
        write_candles(args.candle_root/f'{pair}_M1.csv',pair,'M1',candles('M1',240,now=NOW))
    return args


def test_slow_calculators_leave_real_publication_headroom_and_recreate_all_pairs(tmp_path,monkeypatch):
    clock=Clock();args=args_fixture(tmp_path)
    original=base.calculator.calculate_pair
    computed=[]
    def slow(pair,*a,**kw):
        computed.append(pair)
        value=original(pair,*a,**kw)
        clock.advance(8)
        return value
    monkeypatch.setattr(base.calculator,'calculate_pair',slow)
    archive=worker.archive_observation_snapshot
    def slow_publication(*a,**kw):
        clock.advance(9)
        return archive(*a,**kw)
    monkeypatch.setattr(worker,'archive_observation_snapshot',slow_publication)
    result=worker.run_cycle(args,clock=clock.utc,monotonic=clock.monotonic)
    assert result['status']=='published' and result['elapsed_sec']==41
    assert len(computed)==4
    with gzip.open(result['archive'],'rt') as handle:envelope=json.load(handle)
    original=envelope['original_snapshot']
    assert set(original['observation_inputs']['instruments'])==set(PAIRS)
    assert original['coverage']['all_configured_feature_families_materialized'] is False
    assert original['coverage']['cycle_budget']['publication_reserve_seconds']==15
    assert len(original['coverage']['cycle_budget']['unavailable_invalid_pip_or_budget_pairs'])==4
    for pair in set(PAIRS)-set(computed):
        row=original['observation_inputs']['instruments'][pair]
        assert row['coverage']['rich_ma_status']=='unavailable'
        assert all(value is None for name,value in row['groups']['primary']['values'].items() if name.startswith('ma__'))
    assert build_observation_frame(original)==envelope['frame']
    receipt=json.loads(Path(result['publication_receipt']).read_bytes())
    assert receipt['payload_sha256']==envelope['payload_sha256']==payload_sha256(original)


def test_overrunning_one_calculation_still_refuses_late_archive(tmp_path,monkeypatch):
    clock=Clock();args=args_fixture(tmp_path,pairs=('EUR_USD',))
    original=base.calculator.calculate_pair
    def too_slow(*a,**kw):
        value=original(*a,**kw);clock.advance(46);return value
    monkeypatch.setattr(base.calculator,'calculate_pair',too_slow)
    with pytest.raises(ValueError,match='cycle_time_bound_before_publication'):
        worker.run_cycle(args,clock=clock.utc,monotonic=clock.monotonic)
    assert not list(args.archive_root.rglob('obs_*.json.gz'))
    assert not list(args.archive_root.rglob('*.publication.json'))


def test_input_deadline_retains_every_pair_and_explicitly_withholds_optional_sources(tmp_path,monkeypatch):
    clock=Clock();args=args_fixture(tmp_path)
    args.news_snapshot=tmp_path/'news_not_read.json'
    args.book_snapshot=tmp_path/'books_not_read.json'
    args.model_study=[tmp_path/'study_not_read']
    original=worker.candles.read_tail;calls=[]
    def slow_read(*a,**kw):
        calls.append(str(a[0]));clock.advance(10)
        return original(*a,**kw)
    monkeypatch.setattr(worker.candles,'read_tail',slow_read)
    result=worker.run_cycle(args,clock=clock.utc,monotonic=clock.monotonic)
    assert len(calls)==3 and result['status']=='published'
    assert result['coverage']['observed_universe_count']==8
    assert result['coverage']['cycle_budget']['calculator_allowance_seconds']==0
    assert result['event_status']['news']['reason']=='cycle_input_bound'
    assert result['event_status']['models'][0]['reason']=='cycle_input_bound'
    with gzip.open(result['archive'],'rt') as handle:envelope=json.load(handle)
    receipts=envelope['original_snapshot']['source_inputs']['source_receipts']
    assert receipts['books']['reason']=='cycle_input_bound'
    assert sum(r.get('reason')=='cycle_input_bound' for p in receipts['candles'].values() for r in p.values())==21


def test_fair_rotation_reaches_every_pair_and_preserves_cross_section_math():
    orders=[worker.rotated_pairs(PAIRS,(NOW+timedelta(minutes=n)).isoformat()) for n in range(len(PAIRS))]
    assert {order[0] for order in orders}==set(PAIRS)
    assert all(set(order)==set(PAIRS) and len(order)==len(PAIRS) for order in orders)
    universe=quotes(PAIRS,now=NOW)
    sets={pair:{'M1':candles('M1',240,now=NOW)} for pair in PAIRS}
    options=dict(source_read_completed_utc=NOW.isoformat(),clock=lambda:NOW.isoformat(),max_cycle_seconds=30)
    ordinary=base.build_research_observation(universe,sets,**options)
    rotated=base.build_research_observation(universe,sets,calculation_order=orders[1],**options)
    assert ordinary==rotated


@pytest.mark.parametrize('order',[[],['EUR_USD']*8,list(PAIRS[:-1])+['XXX_YYY'],list(PAIRS[:-1])+[None]])
def test_calculation_order_cannot_drop_duplicate_or_invent_pairs(order):
    with pytest.raises(ValueError,match='exact_quote_universe'):
        base.build_research_observation(quotes(PAIRS,now=NOW),{},source_read_completed_utc=NOW.isoformat(),
            clock=lambda:NOW.isoformat(),calculation_order=order)


def test_zero_remaining_budget_does_not_start_calculator(monkeypatch):
    def forbidden(*a,**kw):raise AssertionError('calculator must not start')
    monkeypatch.setattr(base.calculator,'calculate_pair',forbidden)
    result=base.build_research_observation(quotes(PAIRS,now=NOW),{},source_read_completed_utc=NOW.isoformat(),
        clock=lambda:NOW.isoformat(),monotonic=lambda:0,max_cycle_seconds=0)
    assert len(result['observation_inputs']['instruments'])==8
    assert result['coverage']['family_readiness']['rich_M1_materialized_pairs']==0


def test_failure_survives_next_read_and_restart_until_later_success(tmp_path,monkeypatch):
    clock=Clock();args=args_fixture(tmp_path,pairs=('EUR_USD',));records=[];calls=0
    monkeypatch.setattr(worker,'parse_args',lambda argv:args)
    monkeypatch.setattr(worker.time,'monotonic',clock.monotonic)
    monkeypatch.setattr(worker.time,'sleep',clock.advance)
    monkeypatch.setattr(base,'utc_now',clock.utc)
    original_write=base.atomic_json
    def record(path,value):
        records.append(deepcopy(value));original_write(path,value)
    monkeypatch.setattr(base,'atomic_json',record)
    def cycle(_):
        nonlocal calls
        calls+=1;clock.advance(2)
        if calls<3:raise ValueError('real_failure_'+str(calls))
        return {'status':'published','last_publication_completed_utc':clock.utc()}
    monkeypatch.setattr(worker,'run_cycle',cycle)
    assert worker.main([])==0
    assert len(records)==6
    assert records[1]['last_failure']==records[2]['last_failure']
    assert records[3]['last_failure']==records[4]['last_failure']
    assert records[1]['last_failure']['failed_utc']==(NOW+timedelta(seconds=2)).isoformat()
    assert records[5]['last_failure'] is None
    original_write(args.heartbeat,records[3])
    assert worker.retained_failure(args.heartbeat)==records[3]['last_failure']
    args.once=True;args.duration_sec=130
    records.clear();worker.main([])
    assert records[0]['last_failure']['reason']=='real_failure_2'
    assert records[-1]['last_failure'] is None


def test_unrelated_or_invalid_heartbeat_is_not_failure_authority(tmp_path):
    path=tmp_path/'heartbeat.json'
    path.write_text(json.dumps({'schema_version':'wrong','worker':worker.WORKER,'last_failure':{'reason':'x'}}))
    assert worker.retained_failure(path) is None
    path.write_text('x'*1048577)
    assert worker.retained_failure(path) is None
