from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
import hashlib
import json
from pathlib import Path

import pytest

import oanda_immutable_summary_publication_v1 as boundary

SCHEMA = 'joint_price_news_forecast_summary_v4_fixture_only'
REGISTRY = 'a'*64
FAMILY = 'ridge_price_news_v1'


def seal(value):
    value['payload_sha256'] = boundary._digest({k:v for k,v in value.items() if k != 'payload_sha256'})
    return value


def summary():
    return seal(dict(schema_version=SCHEMA, registry_sha256=REGISTRY, generated_epoch=1000.0,
        research_only=True, **{flag:False for flag in boundary.INERT_FLAGS}, rows=[
            dict(instrument='EUR_USD', pip_size=.0001, families={FAMILY:dict(
                status='building', observed_epoch=1000.0,
                last_attempt=dict(attempt_id='fixture_only', reason='building', epoch=998.0),
                current_readiness=dict(observed_epoch=997.0, diagnostics=dict(ready=True)),
                latest_forecast=dict(reference_epoch=950.0, issued_epoch=990.0, target_epoch=4550.0))})]))


def freeze(value=None):
    return boundary.freeze_summary(summary() if value is None else value,
        expected_schema=SCHEMA, expected_registry_sha256=REGISTRY)


def publish(frozen=None):
    frozen = freeze() if frozen is None else frozen
    return boundary.verify_written_summary(frozen, frozen.raw, read_completed_epoch=1001.0)


def test_later_fit_reason_diagnostics_and_forecast_mutation_cannot_change_published_bytes_or_header():
    value = summary()
    original = deepcopy(value)
    frozen = freeze(value)
    published = publish(frozen)
    slot = value['rows'][0]['families'][FAMILY]
    slot['last_attempt']['reason'] = 'published'
    slot['current_readiness']['diagnostics']['ready'] = False
    slot['latest_forecast']['target_epoch'] = 9999999.0
    slot['status'] = 'forecast'
    fields = boundary.heartbeat_fields(published, generated_epoch=1006.0)
    assert json.loads(frozen.raw) == original
    assert fields['summary_sha256'] == boundary._digest(original) != boundary._digest(value)
    assert fields['counts']['building'] == 1 and fields['counts']['forecast'] == 0
    assert fields['summary_generated_epoch'] == 1000.0
    assert fields['summary_read_completed_epoch'] == 1001.0
    assert fields['generated_epoch'] == 1006.0


def test_actual_retained_chf_zar_case_is_losslessly_frozen_and_old_mutated_hash_refused():
    parent = Path(__file__).resolve().parent.parent
    path = parent/'overnight_curve_buildout_20260909/coherent_reader_design/actual_forward_001/private_envelope_bytes/1c89ea3e3e6ee6267a6ffe226bcd6f1b4315ee93e5091cb24d5d8e74a3f21ad5.json'
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == path.stem
    value = json.loads(raw)
    frozen = boundary.freeze_summary(value, expected_schema=value['schema_version'],
        expected_registry_sha256=value['registry_sha256'])
    assert frozen.raw == raw
    published = boundary.verify_written_summary(frozen, raw, read_completed_epoch=1788938738.0)
    slot = next(row['families'][FAMILY] for row in value['rows'] if row['instrument'] == 'CHF_ZAR')
    slot['last_attempt']['reason'] = 'published'
    assert boundary._digest(value) == '73802c9986d5dcc53445ead7c14188773942329b1a0393743636aa99d8a1da7d'
    assert boundary.heartbeat_fields(published, generated_epoch=1788938742.8028383)['summary_sha256'] == path.stem
    with pytest.raises(boundary.SummaryBoundaryError, match='payload_seal'):
        boundary.freeze_summary(value, expected_schema=value['schema_version'],
            expected_registry_sha256=value['registry_sha256'])


def test_boundary_objects_are_frozen_and_every_returned_header_is_independent():
    frozen = freeze(); published = publish(frozen)
    with pytest.raises(FrozenInstanceError):
        frozen.raw = b'changed'
    with pytest.raises(FrozenInstanceError):
        published.read_completed_epoch = 1002.0
    first = boundary.heartbeat_fields(published, generated_epoch=1002.0)
    first['counts']['forecast'] = 68
    assert boundary.heartbeat_fields(published, generated_epoch=1002.0)['counts']['forecast'] == 0


def test_concurrent_caller_mutations_and_header_reads_cannot_modify_admitted_snapshot():
    value = summary(); frozen = freeze(value); published = publish(frozen)
    original_raw = frozen.raw
    def task(index):
        value['rows'][0]['families'][FAMILY]['last_attempt']['reason'] = str(index)
        header = boundary.heartbeat_fields(published, generated_epoch=1002.0+index)
        digest = header['summary_sha256']
        header['counts']['building'] = -1
        return digest
    with ThreadPoolExecutor(max_workers=8) as pool:
        hashes = list(pool.map(task, range(80)))
    assert set(hashes) == {frozen.summary_sha256}
    assert frozen.raw == original_raw


@pytest.mark.parametrize('observed', [b'', b'{}', bytearray(b'{}'), memoryview(b'{}'), None])
def test_incomplete_or_nonimmutable_readback_never_advances_publication(observed):
    with pytest.raises(boundary.SummaryBoundaryError, match='readback_mismatch'):
        boundary.verify_written_summary(freeze(), observed, read_completed_epoch=1001.0)


def test_superseded_or_edited_written_bytes_cannot_be_acknowledged_as_candidate():
    frozen = freeze()
    newer = summary(); newer['generated_epoch'] = 1015.0; seal(newer)
    with pytest.raises(boundary.SummaryBoundaryError, match='readback_mismatch'):
        boundary.verify_written_summary(frozen, boundary._encoded(newer), read_completed_epoch=1016.0)
    with pytest.raises(boundary.SummaryBoundaryError, match='readback_mismatch'):
        boundary.verify_written_summary(frozen, frozen.raw+b'\n', read_completed_epoch=1001.0)


def test_simulated_failed_atomic_write_or_readback_keeps_previous_admitted_generation():
    old = publish()
    newer = summary(); newer['generated_epoch'] = 1015.0; seal(newer)
    candidate = freeze(newer)
    state = {'published': old}
    def caller_publish(writer):
        observed_raw = writer(candidate.raw)  # raises before replacing state
        accepted = boundary.verify_written_summary(candidate, observed_raw, read_completed_epoch=1016.0)
        state['published'] = accepted
    def fail(raw):
        raise OSError('synthetic_atomic_replace_failure')
    with pytest.raises(OSError):
        caller_publish(fail)
    assert state['published'] is old
    with pytest.raises(boundary.SummaryBoundaryError, match='readback_mismatch'):
        caller_publish(lambda raw: old.snapshot.raw)
    assert state['published'] is old
    caller_publish(lambda raw: raw)
    assert state['published'].snapshot.raw == candidate.raw


@pytest.mark.parametrize('clock', [True, False, None, '1001', -1, 0, float('nan'), float('inf'), 10**400])
def test_malformed_or_nonfinite_actual_readback_clocks_are_bounded_errors(clock):
    with pytest.raises(boundary.SummaryBoundaryError, match='clock_'):
        boundary.verify_written_summary(freeze(), freeze().raw, read_completed_epoch=clock)


def test_readback_and_heartbeat_cannot_precede_original_summary_or_actual_readback():
    frozen = freeze()
    with pytest.raises(boundary.SummaryBoundaryError, match='readback_before_summary'):
        boundary.verify_written_summary(frozen, frozen.raw, read_completed_epoch=999.0)
    with pytest.raises(boundary.SummaryBoundaryError, match='heartbeat_clock_order'):
        boundary.heartbeat_fields(publish(frozen), generated_epoch=1000.9)
    with pytest.raises(boundary.SummaryBoundaryError, match='publication_type'):
        boundary.heartbeat_fields(frozen, generated_epoch=1001.0)


def test_later_heartbeat_preserves_old_snapshot_clock_instead_of_refreshing_rows_or_h1_targets():
    published = publish()
    header = boundary.heartbeat_fields(published, generated_epoch=9000.0)
    decoded = json.loads(published.snapshot.raw)
    assert header['summary_generated_epoch'] == 1000.0
    assert decoded['rows'][0]['families'][FAMILY]['latest_forecast']['target_epoch'] == 4550.0
    assert decoded['rows'][0]['families'][FAMILY]['observed_epoch'] == 1000.0


@pytest.mark.parametrize('field', boundary.INERT_FLAGS+('research_only',))
def test_wrapper_authority_tamper_rejected_even_if_resealed(field):
    value = summary(); value[field] = not value[field]; seal(value)
    with pytest.raises(boundary.SummaryBoundaryError, match='inert_flags'):
        freeze(value)


def test_registry_schema_row_identity_and_future_observation_rejected():
    value = summary(); value['registry_sha256'] = 'b'*64; seal(value)
    with pytest.raises(boundary.SummaryBoundaryError, match='registry_identity'):freeze(value)
    value = summary(); value['schema_version'] = 'other_study'; seal(value)
    with pytest.raises(boundary.SummaryBoundaryError, match='schema_identity'):freeze(value)
    value = summary(); value['rows'].append(deepcopy(value['rows'][0])); seal(value)
    with pytest.raises(boundary.SummaryBoundaryError, match='pair_identity'):freeze(value)
    value = summary(); value['rows'][0]['families'][FAMILY]['observed_epoch'] = 1001.0; seal(value)
    with pytest.raises(boundary.SummaryBoundaryError, match='future_row'):freeze(value)


def test_partial_one_or_two_family_counts_are_from_original_frozen_rows():
    value = summary(); row = value['rows'][0]
    row['families'][FAMILY]['status'] = 'forecast'
    row['families']['price_baseline_fixture'] = {'status':'warming', 'observed_epoch':1000.0}
    other = deepcopy(row); other['instrument'] = 'GBP_USD'
    for slot in other['families'].values():slot['status'] = 'unavailable'
    value['rows'].append(other); seal(value)
    header = boundary.heartbeat_fields(publish(freeze(value)), generated_epoch=1002.0)
    assert header['pair_count']==2 and header['family_count']==4 and header['pairs_with_forecast']==1
    assert header['counts']==dict(forecast=1,warming=1,building=0,unavailable=2,ready=0)


@pytest.mark.parametrize('mutation', ['raw', 'hash', 'payload', 'generated', 'schema', 'registry'])
def test_forged_frozen_snapshots_revalidated_on_use(mutation):
    frozen = freeze()
    changes = dict(raw={'raw':b'{}'}, hash={'summary_sha256':'f'*64},
        payload={'payload_sha256':'f'*64}, generated={'generated_epoch':999.0},
        schema={'schema_version':'wrong_fixture'}, registry={'registry_sha256':'b'*64})
    forged = replace(frozen, **changes[mutation])
    with pytest.raises(boundary.SummaryBoundaryError):
        boundary.verify_written_summary(forged, forged.raw, read_completed_epoch=1001.0)


def test_forged_publication_acknowledgement_revalidated():
    value = publish()
    with pytest.raises(boundary.SummaryBoundaryError, match='readback_identity'):
        boundary.heartbeat_fields(replace(value, readback_sha256='f'*64), generated_epoch=1002.0)
    with pytest.raises(boundary.SummaryBoundaryError, match='heartbeat_clock_order'):
        boundary.heartbeat_fields(replace(value, read_completed_epoch=999.0), generated_epoch=1002.0)


def test_canonical_byte_structure_and_size_limits_and_no_duplicate_keys():
    frozen = freeze()
    with pytest.raises(boundary.SummaryBoundaryError, match='noncanonical_bytes'):
        boundary._decode(frozen.raw+b'\n')
    with pytest.raises(boundary.SummaryBoundaryError, match='noncanonical_bytes'):
        boundary._decode(b'{"a":1,"a":1}')
    with pytest.raises(boundary.SummaryBoundaryError, match='raw_bytes'):
        boundary._decode(b' '* (boundary.MAX_BYTES+1))
    with pytest.raises(boundary.SummaryBoundaryError, match='string_limit'):
        boundary._encoded('a'*(boundary.MAX_STRING+1))
    with pytest.raises(boundary.SummaryBoundaryError, match='byte_limit'):
        boundary._encoded(['a'*8192 for _ in range(129)])
    nested=[]; cursor=nested
    for _ in range(boundary.MAX_DEPTH+1):child=[];cursor.append(child);cursor=child
    with pytest.raises(boundary.SummaryBoundaryError, match='structure_limit'):
        boundary._encoded(nested)
    with pytest.raises(boundary.SummaryBoundaryError, match='json_type'):
        boundary._encoded((1,2))


def test_no_top_level_io_or_runtime_imports():
    import ast
    tree = ast.parse(Path(boundary.__file__).read_bytes())
    imported = []
    for node in tree.body:
        if isinstance(node,ast.Import):imported.extend(n.name for n in node.names)
        if isinstance(node,ast.ImportFrom):imported.append(node.module)
    assert set(imported) == {'dataclasses','hashlib','json','math','re'}
    assert not any(isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id in {'open','exec','eval'} for n in ast.walk(tree))
