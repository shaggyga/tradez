"""Pilot integration tests use retained coefficients with synthetic quotes; no GETs or orders."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

import oanda_recovered_curve_pilot_v1 as pilot
import oanda_forecast_curve_contract_v1 as contract
import oanda_recovered_second_curve_v1 as recovered
from test_oanda_recovered_curve_bridge_v1 import MODEL


BASE = datetime(2026,9,9,5,20,tzinfo=timezone.utc).timestamp()


class Clock:
    def __init__(self, value=BASE): self.value=value
    def __call__(self):
        self.value += .01
        return self.value


@pytest.fixture
def registry(tmp_path):
    spec = importlib.util.spec_from_file_location('pilot_registry_fixture',
        Path(__file__).parent.parent/'overnight_curve_buildout_20260909/prepare_prospective_pilot_registry.py')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    value = module.registry_value(BASE-60)
    value['output_root'] = str(tmp_path/'pilot')
    return value


@pytest.fixture(scope='module')
def model():
    if not MODEL.exists(): pytest.skip('retained coefficient artifact unavailable')
    return recovered.load_model(MODEL)


def mapping():
    rows=[]
    for index in range(13):
        mid=1.10+index*.00001
        rows.append(dict(bar_start_epoch=BASE-70+index*5,available_epoch=BASE-.5,
            complete=True,bid_close=mid-.0001,ask_close=mid+.0001,
            mid_close=mid,mid_high=mid+.00001,mid_low=mid-.00001,spread_pips=2,volume=10+index))
    return dict(instrument='EUR_USD',source_sha256='7'*64,source_receipt_sha256='8'*64,
                last13_feature_rows=rows)


def quote_snapshot_file(tmp_path):
    rows={pair:dict(bid=1.1 if pair!='USD_JPY' else 150.0,
        ask=1.1002 if pair!='USD_JPY' else 150.02,
        pip=.0001 if pair!='USD_JPY' else .01,source='stream',
        time='2026-09-09T05:19:59.500000000Z',tradeable=True) for pair in pilot.INSTRUMENTS}
    payload=dict(schema_version=3,generated_utc='2026-09-09T05:19:59.800000+00:00',
        producer='practice_007_dedicated_quote_stream',connection_generation=1,research_only=True,
        quote_count=3,quotes=rows,coverage=dict(current_quote_count=3,last_known_quote_count=3,
        retained_last_known_count=0,retained_last_known_instruments=[],connection_generation=1,
        current_tradeable_quote_count=3,current_non_tradeable_quote_count=0,
        current_tradeability_unknown_count=0,current_non_tradeable_instruments=[],
        current_tradeability_unknown_instruments=[],tradeability_contract='oanda_client_price_status_boolean_v1',
        retained_quotes_execution_eligible=False,execution_requires_independent_freshness_check=True))
    path=tmp_path/'quotes.json';path.write_text(json.dumps(payload),encoding='utf8')
    return path


def patch_quote_capture(monkeypatch,tmp_path):
    original=pilot.quotes.capture_quote_snapshot
    path=quote_snapshot_file(tmp_path)
    monkeypatch.setattr(pilot.quotes,'capture_quote_snapshot',lambda *,clock:original(path,clock=clock))


def test_real_persistence_chain_and_observation_survive_roundtrip(registry,model,tmp_path,monkeypatch):
    patch_quote_capture(monkeypatch,tmp_path)
    answer=pilot.attempt_variant(registry,model,mapping(),'official_midpoint',('cycles','fixture','eur_usd'),clock=Clock())
    assert answer['status']=='issued_and_consumed', answer
    assert 'reason_code' not in answer, answer
    assert answer['candidate_count']==13
    root=Path(registry['output_root'])
    decision=json.loads((root/answer['decision_observation']['relative_path']).read_bytes())
    assert decision['position_actions_performed'] is False and decision['broker_fills_performed'] is False
    assert decision['entry_observation']['first_observed_epoch']>answer['consumption']['observed_epoch']
    assert all(c['target_is_exact'] is False for c in decision['candidates'])
    assert {c['target_window_policy'] for c in decision['candidates']}=={'nominal_management_boundary'}
    curve=json.loads((root/'published/curves'/answer['curve_sha256']/'curve.json').read_bytes())
    assert curve['prepared_curve']['input_context']['historical_ingestion_equivalence_proven'] is False


def test_failed_quote_read_retains_the_real_issued_forecast_without_claiming_decision(registry,model,monkeypatch):
    def fail(**_): raise contract.CurveContractError('quote_source_unavailable')
    monkeypatch.setattr(pilot.quotes,'capture_quote_snapshot',fail)
    answer=pilot.attempt_variant(registry,model,mapping(),'official_midpoint',('cycles','fixture','eur_usd'),clock=Clock())
    assert answer['status']=='issued_and_consumed'
    assert answer['decision_status']=='failed_after_issue'
    assert answer['reason_code']=='quote_source_unavailable'
    assert 'decision_observation' not in answer
    assert (Path(registry['output_root'])/'published/curves'/answer['curve_sha256']/'publication.json').exists()


def test_source_change_prevents_publication_and_is_recorded(registry,model,monkeypatch):
    monkeypatch.setattr(pilot,'source_bindings',lambda:{'changed.py':'0'*64})
    answer=pilot.attempt_variant(registry,model,mapping(),'official_midpoint',('cycles','fixture','eur_usd'),clock=Clock())
    assert answer['status']=='failed'
    assert answer['reason_code']=='pilot_registered_source_changed'
    assert not (Path(registry['output_root'])/'published').exists()


def test_issue_cutoff_refuses_without_computation(registry,model):
    registry['issue_cutoff_epoch']=BASE
    answer=pilot.attempt_variant(registry,model,mapping(),'official_midpoint',('cycles','fixture','eur_usd'),clock=Clock())
    assert answer['reason_code']=='pilot_issue_cutoff_reached'
    assert not (Path(registry['output_root'])/'published').exists()


def test_cutoff_crossed_at_actual_issue_sample_is_never_published(registry,model):
    registry['issue_cutoff_epoch']=BASE+.055
    answer=pilot.attempt_variant(registry,model,mapping(),'official_midpoint',('cycles','fixture','eur_usd'),clock=Clock())
    assert answer['reason_code']=='pilot_actual_issue_clock_past_cutoff'
    assert answer['logical_issue_created'] is True
    assert answer['registry_issue_admitted'] is False
    assert answer['publication_completed'] is False
    assert not (Path(registry['output_root'])/'published').exists()


def test_publication_failure_retains_issue_stage_without_claiming_publication(registry,model,monkeypatch):
    def fail(*_,**__):raise contract.CurveContractError('publication_disk_failed')
    monkeypatch.setattr(pilot.files,'publish_curve',fail)
    answer=pilot.attempt_variant(registry,model,mapping(),'official_midpoint',('cycles','fixture','eur_usd'),clock=Clock())
    assert answer['status']=='issued_not_published'
    assert answer['registry_issue_admitted'] is True
    assert answer['publication_completed'] is False
    assert answer['consumption_completed'] is False
    assert (Path(registry['output_root'])/'cycles/fixture/eur_usd/official_midpoint/issue_stage.json').exists()


def test_consumption_failure_retains_successful_publication_pointer(registry,model,monkeypatch):
    def fail(*_,**__):raise contract.CurveContractError('consumer_read_failed')
    monkeypatch.setattr(pilot.files,'consume_published_curve',fail)
    answer=pilot.attempt_variant(registry,model,mapping(),'official_midpoint',('cycles','fixture','eur_usd'),clock=Clock())
    assert answer['status']=='published_not_consumed'
    assert answer['publication_completed'] is True
    assert answer['consumption_completed'] is False
    assert answer['publication']['descriptor']['curve_sha256']==answer['curve_sha256']


def test_source_capture_failure_is_bounded_to_exactly_three_get_helper_calls(registry,monkeypatch):
    called=[]
    def fail(pair,**kwargs):
        called.append(pair)
        return b'',dict(status='failed',error_reason='non_200_response',**contract.AUTHORITY)
    monkeypatch.setattr(pilot.candles,'capture_once',fail)
    cycle=pilot.run_cycle(registry,None,session_id='synthetic_fixture',clock=Clock())
    assert called==list(pilot.INSTRUMENTS)
    assert cycle['curves_issued']==0
    assert len(cycle['pairs'])==3 and all(p['reason_code']=='non_200_response' for p in cycle['pairs'])


def test_collection_stops_before_any_additional_get(registry,monkeypatch):
    registry['collection_stop_epoch']=BASE
    def forbidden(*_,**__): raise AssertionError('must not call candle helper beyond cutoff')
    monkeypatch.setattr(pilot.candles,'capture_once',forbidden)
    result=pilot.run_cycle(registry,None,session_id='synthetic_fixture',clock=Clock())
    assert result['pairs']==[]


def test_collection_does_not_start_request_inside_final_minute(registry,monkeypatch):
    registry['collection_stop_epoch']=BASE+30
    def forbidden(*_,**__):raise AssertionError('no new GET inside deadline guard')
    monkeypatch.setattr(pilot.candles,'capture_once',forbidden)
    assert pilot.run_cycle(registry,None,session_id='synthetic_fixture',clock=Clock())['pairs']==[]


def test_deadline_clock_and_orders_cannot_be_changed_in_registry(registry,tmp_path,monkeypatch):
    monkeypatch.setattr(pilot,'ROOT',tmp_path)
    registry['output_root']=str(tmp_path/'data/oanda_training_manager'/pilot.STUDY_ID)
    monkeypatch.setattr(pilot,'source_bindings',lambda:registry['source_bindings'])
    def load(value):
        path=tmp_path/'registry.json';raw=contract.canonical_bytes(value);path.write_bytes(raw)
        return pilot.load_registry(path,hashlib.sha256(raw).hexdigest())
    assert load(registry)[0]==registry
    for change,reason in [({'orders_enabled':True},'pilot_must_be_order_incapable'),
        ({'collection_stop_epoch':registry['collection_stop_epoch']+60},'pilot_user_deadline_changed'),
        ({'issue_cutoff_epoch':registry['collection_stop_epoch']-3600},'pilot_stop_or_maturity_window_invalid'),
        ({'target_window_policy':'exact_only'},'pilot_explicit_target_approximation_required')]:
        with pytest.raises(contract.CurveContractError,match=reason):load({**registry,**change})


def test_worker_lock_prevents_concurrent_writer_and_remains_after_exit(tmp_path):
    with pilot.worker_lock(tmp_path):
        with pytest.raises(contract.CurveContractError,match='pilot_worker_already_locked'):
            with pilot.worker_lock(tmp_path): pass
    assert (tmp_path/'worker_lock.bin').exists()
    with pilot.worker_lock(tmp_path): pass


def test_restart_preserves_completed_or_interrupted_cycle_slot(tmp_path):
    root=tmp_path/'pilot'
    assert pilot.next_initial_cycle(root,now=BASE,cycle_sec=60)==BASE
    pilot.write_record(root,('cycles','cycle_123'),'cycle_started.json',
        dict(scheduled_cycle_epoch=BASE,started_epoch=BASE+.2,**contract.AUTHORITY))
    assert pilot.next_initial_cycle(root,now=BASE+5,cycle_sec=60)==BASE+60
    assert pilot.next_initial_cycle(root,now=BASE+65,cycle_sec=60)==BASE+65


def test_cycle_cannot_run_before_predeclared_schedule(registry):
    with pytest.raises(contract.CurveContractError,match='pilot_cycle_started_before_schedule'):
        pilot.run_cycle(registry,None,session_id='synthetic_fixture',clock=Clock(),scheduled_epoch=BASE+30)


def test_unsafe_error_text_is_not_serialized_as_reason():
    assert pilot._reason(ValueError('request failed with secret token=xyz'))=='operation_failed'
    assert pilot._reason(ValueError('invalid_clock'))=='invalid_clock'
