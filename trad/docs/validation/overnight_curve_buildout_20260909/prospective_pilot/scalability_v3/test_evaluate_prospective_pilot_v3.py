from copy import copy, deepcopy
import json
from pathlib import Path
import sys
import time

import pytest

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))
sys.path.insert(0, str(BASE.parent))
import evaluate_prospective_pilot_v3 as v3
import test_evaluate_prospective_pilot as old_tests

world = old_tests.world
old = v3.original


def capture(labels=(100, 110), observed=130):
    rows = [dict(bar_start_epoch=t, available_epoch=observed, complete=True,
                 mid_close='1.10005', bid_close='1.10000', ask_close='1.10010') for t in labels]
    att = dict(instrument='EUR_USD', granularity='S5', raw_source_sha256='a'*64,
               complete_provider_response=True, coverage_start_label_epoch=labels[0],
               coverage_end_label_epoch=labels[-1], read_started_epoch=observed-1,
               read_completed_epoch=observed, source_receipt_sha256='b'*64)
    return old.outcomes.capture_outcome_candles(rows, instrument='EUR_USD', source_sha256='a'*64,
        coverage_start_label_epoch=labels[0], coverage_end_label_epoch=labels[-1], complete_range=True,
        read_started_epoch=observed-1, read_completed_epoch=observed, source_attestation=att,
        clock=lambda: observed+1)


def compare(left, right):
    for key in ('source_captures', 'variant_attempts', 'curves', 'matched_comparisons'):
        assert right[key] == left[key], key
    clean = lambda rows: [{k: v for k, v in r.items() if k != 'observed_epoch'} for r in rows]
    assert clean(right['manifest']) == clean(left['manifest'])
    a = deepcopy(right['assessment'])
    assert a.pop('original_evaluation_schema') == left['assessment']['schema_version']
    a['schema_version'] = left['assessment']['schema_version']
    a['evaluator_source_sha256'] = a.pop('original_evaluator_source_sha256')
    a.pop('validation_memo'); bank = a.pop('capture_bank')
    assert bank['accounted_bytes'] <= bank['maximum_bytes']
    assert bank['retained_canonical_bytes'] <= bank['maximum_canonical_bytes']
    assert not bank['whole_scores_cached'] and not bank['dynamic_clock_checks_cached']
    assert a.pop('optimized_engine_sources') == v3.v2.EXPECTED_SOURCES
    assert a.pop('v2_source_sha256') == v3.EXPECTED_V2_SHA
    a['limits'].pop()
    assert a == left['assessment']
    return bank


def parity(world, offset=100):
    left = old.evaluate(world['path'], world['sha'], clock=lambda: old_tests.NOW+offset)
    right = v3.evaluate(world['path'], world['sha'], clock=lambda: old_tests.NOW+offset)
    return left, right, compare(left, right)


def test_whole_election_scores_omission_provenance_exact(world):
    left, right, bank = parity(world)
    assert bank['counts']['validation_hit'] > 0
    assert bank['counts']['semantic_hash_hit'] > 0
    assert bank['counts']['indexed_target_lookup'] > 0
    assert old_tests.first_view(right, 'nominal_exact')['reason_code'] == 'provider_reported_no_complete_target_bar'
    assert old_tests.first_view(right)['selection_delay_sec'] == 5


@pytest.mark.parametrize('world', ['quotes'], indirect=True)
def test_separate_entry_receipts_keep_unmatched_cost_denominator(world):
    _, right, _ = parity(world)
    rows = [r for r in right['matched_comparisons'] if r['status']=='matched_scored_same_source_endpoint']
    assert rows and all(not r['paired_bid_ask_cost_denominator_eligible'] for r in rows)


@pytest.mark.parametrize('mutation', ['raw', 'publication_missing', 'consumption_missing', 'input_resealed', 'future_source'])
def test_adversarial_complete_inventory_refusal_parity(world, mutation):
    offset=100
    if mutation=='raw':
        p=world['root']/'cycles/cycle_fixture_origin/eur_usd/source_raw.json';p.write_bytes(p.read_bytes()+b' ')
    elif mutation=='publication_missing':
        p=world['root']/'published/curves'/world['attempts']['official_midpoint']['curve_sha256']/'publication.json'
        p.rename(p.with_suffix('.fixture_omitted'))
    elif mutation=='consumption_missing':
        p=world['root']/'published/consumptions'/(world['attempts']['official_midpoint']['consumption']['consumption_sha256']+'.json')
        p.rename(p.with_suffix('.fixture_omitted'))
    elif mutation=='input_resealed':
        p=world['root']/'cycles/cycle_fixture_origin/eur_usd/official_midpoint/model_input.json';v=json.loads(p.read_bytes())
        v['rows'][0]['volume']+=1
        v['capture_sha256']=old.recovered._hash({k:x for k,x in v.items() if k!='capture_sha256'})
        p.write_bytes(old.contract.canonical_bytes(v))
    else:offset=80
    parity(world, offset)


def test_original_caller_mutation_does_not_mutate_admitted_private_snapshot():
    bank=v3.CaptureBank(); source=capture(); reference=bank.admit(source)
    immutable=bank.validate(reference)
    source['rows'][0]['mid_close']='99'
    assert immutable['rows'][0]['mid_close']=='1.10005'
    with pytest.raises(old.contract.CurveContractError):bank.admit(source)
    with pytest.raises(TypeError):immutable['rows'][0]['mid_close']='99'
    with pytest.raises(TypeError):immutable['source_query_attestation']['instrument']='USD_JPY'
    with pytest.raises(TypeError):immutable['rows'][0]=immutable['rows'][1]
    bank.close()


def test_copies_are_same_immutable_reference_but_forged_or_closed_reference_refuses():
    bank=v3.CaptureBank(); ref=bank.admit(capture())
    assert copy(ref) is ref and deepcopy(ref) is ref
    with pytest.raises(AttributeError):ref.claimed_sha='a'*64
    forged=v3._CaptureReference()
    with pytest.raises(old.contract.CurveContractError):bank.validate(forged)
    with pytest.raises(old.contract.CurveContractError):ref['rows']
    bank.close()
    with pytest.raises(old.contract.CurveContractError):ref['capture_sha256']


def test_equal_hash_subclass_cannot_impersonate_factory_reference():
    bank=v3.CaptureBank(); ref=bank.admit(capture())
    class Forged(v3._CaptureReference):
        def __hash__(self):return hash(ref)
        def __eq__(self,other):return other is ref or other is self
    forged=Forged()
    assert bank._refs.get(forged) is bank._refs[ref]
    with pytest.raises(old.contract.CurveContractError):bank.validate(forged)
    assert bank.validate(ref)['instrument']=='EUR_USD'
    bank.close()


@pytest.mark.parametrize('nominal,delay,now', [(95,0,130),(100,0,104),(100,0,105),(105,0,130),
    (105,7,130),(115,0,130),(110,7,130),(105,7,109),(105,7,110)])
def test_indexed_real_target_branch_order_matches_original(nominal,delay,now):
    bank=v3.CaptureBank(); source=capture(); owned=bank.validate(bank.admit(source))
    left=v3._ORIGINAL_SELECT(source,nominal,delay,now)
    right=bank.select_target(owned,nominal,delay,now)
    assert left==right
    bank.close()


def test_semantic_hash_reuse_is_exact_and_nested_values_cannot_change():
    bank=v3.CaptureBank(); source=capture(); owned=bank.validate(bank.admit(source))
    semantic={k:v for k,v in owned.items() if k not in ('capture_sha256','first_observed_epoch')}
    plain={k:v for k,v in source.items() if k not in ('capture_sha256','first_observed_epoch')}
    assert bank.semantic_hash(semantic)==v3._ORIGINAL_HASH(plain)
    assert bank.summary()['counts']['semantic_hash_hit']==1
    plain['instrument']='GBP_USD'
    assert bank.semantic_hash(plain)==v3._ORIGINAL_HASH(plain)
    assert bank.summary()['counts']['semantic_hash_hit']==1
    bank.close()


def test_tiny_bank_budget_falls_back_to_original_mutable_copies():
    bank=v3.CaptureBank(maximum_bytes=1); source=capture(); fallback=bank.admit(source)
    assert fallback==source and fallback is not source
    assert bank.validate(fallback)==v3._ORIGINAL_VALIDATE(source)
    assert bank.summary()['retained_entries']==0
    assert bank.summary()['counts']['admission_budget_fallback']==1
    bank.close()


def test_hash_collision_does_not_accept_different_capture(monkeypatch):
    monkeypatch.setattr(v3,'digest',lambda raw:'a'*64)
    bank=v3.CaptureBank(); first=capture(); second=capture(observed=135)
    a=bank.admit(first);b=bank.admit(second)
    assert a is not b and bank.validate(a)['first_observed_epoch']!=bank.validate(b)['first_observed_epoch']
    bank.close()


def test_fresh_clock_and_all_sources_still_reach_original_rolling_election(world):
    left, right, _=parity(world,offset=74)
    assert left['assessment']['view_dispositions']==right['assessment']['view_dispositions']


def test_registry_source_change_and_no_runtime_write_paths(world,monkeypatch):
    def forbidden(*a,**k):raise AssertionError('forbidden_runtime_or_GET')
    monkeypatch.setattr(old.candles,'capture_once',forbidden)
    monkeypatch.setattr(old.files,'_write_exclusive',forbidden)
    v3.evaluate(world['path'],world['sha'],clock=lambda:old_tests.NOW+100)
    path=world['source_root']/old.pilot.SOURCE_FILES[0];path.write_bytes(path.read_bytes()+b'\n')
    with pytest.raises(old.contract.CurveContractError,match='registered_source_changed'):
        v3.evaluate(world['path'],world['sha'],clock=lambda:old_tests.NOW+100)


def test_scoped_aliases_restore_after_exception():
    before=(old._source_capture,old.outcomes.validate_candle_capture,old.outcomes.content_hash,old.outcomes._select_target)
    with pytest.raises(RuntimeError):
        with v3.validation_session():raise RuntimeError('fixture')
    assert before==(old._source_capture,old.outcomes.validate_candle_capture,old.outcomes.content_hash,old.outcomes._select_target)
