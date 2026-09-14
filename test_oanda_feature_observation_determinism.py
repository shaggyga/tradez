"""Permanent archive determinism regression; only isolated scratch writes."""
from copy import deepcopy
from datetime import datetime, timezone
import gzip
import hashlib
import importlib.util
import itertools
import json
from pathlib import Path
import subprocess
import sys

import pytest

HERE = Path(__file__).parent
TRAD = HERE


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


import oanda_feature_observations_v1 as fixed
PREDECESSOR = HERE / 'docs/validation/feature_observation_determinism_20260914/oanda_feature_observations_pre_fix.py'
assert hashlib.sha256(PREDECESSOR.read_bytes()).hexdigest() == 'aaf9e15e35f013c8bc7ae1133c04914102514307009ec5f981ef8db14d268388'
old = load(PREDECESSOR, 'archived_predecessor_feature_observations')


def fixture(order=('r1_pips', 'm5_r1_pips', 'r3_pips', 'm5_r3_pips')):
    origin = '2026-09-14T02:00:00+00:00'
    observed = '2026-09-14T02:06:10+00:00'
    clock = {'observed_utc': observed, 'clock_basis': 'producer_capture'}
    primary = {'candle_time': origin, 'm5_r1_pips': 10., 'm5_r3_pips': 20.,
               'series_origins': {'M5': origin}, 'missing': None, 'zero': 0.}
    values = {'r1_pips': 1., 'm5_r1_pips': 2., 'r3_pips': 3., 'm5_r3_pips': 4.}
    structural = {key: values[key] for key in order}
    structural.update(candle_time=origin, series_origins={'M5': origin})
    return {'schema_version': 1, 'snapshot_id': 'deterministic-m5-alias-fixture',
            'generated_utc': observed, 'generated_epoch': datetime.fromisoformat(observed).timestamp(),
            'observation_source': {'producer_id': 'synthetic-regression',
               'producer_contract_id': 'deterministic-alias-regression-v1',
               'observation_implementation_sha256': fixed.observation_source_sha256()},
            'observation_inputs': {'instruments': {'EUR_USD': {'groups': {
               'primary': fixed.capture_feature_group(primary, input_timeframe='M1', clock=clock),
               'timeframe:M5': fixed.capture_feature_group(structural, input_timeframe='M5', clock=clock)}}}},
            'instruments': {}}


def canonical_roundtrip(value):
    return json.loads(fixed.canonical_bytes(value))


def test_predecessor_reproduces_conflict_metadata_recreation_bug():
    value = fixture()
    before = old.build_observation_frame(value)
    after = old.build_observation_frame(canonical_roundtrip(value))
    assert old.canonical_bytes(before) != old.canonical_bytes(after)
    first = before['instruments']['EUR_USD']['groups']['primary']['conflicts']
    replay = after['instruments']['EUR_USD']['groups']['primary']['conflicts']
    assert first['m5_r1_pips']['other_feature_name'] == 'm5_r1_pips'
    assert replay['m5_r1_pips']['other_feature_name'] == 'r1_pips'


@pytest.mark.parametrize('order', list(itertools.permutations(('r1_pips', 'm5_r1_pips', 'r3_pips', 'm5_r3_pips'))))
def test_all_feature_insert_orders_recreate_identical_exact_frames(order):
    value = fixture(order)
    before = fixed.build_observation_frame(value)
    assert fixed.canonical_bytes(before) == fixed.canonical_bytes(fixed.build_observation_frame(canonical_roundtrip(value)))
    assert fixed.canonical_bytes(before) == fixed.canonical_bytes(fixed.build_observation_frame(fixture()))


def test_real_archive_write_readback_recreates_exact_original_and_preserves_clocks(tmp_path):
    value = fixture()
    path = fixed.archive_observation_snapshot(value, tmp_path)
    envelope = json.loads(gzip.open(path, 'rb').read())
    assert envelope['payload_sha256'] == fixed.payload_sha256(value)
    assert envelope['original_snapshot'] == fixed.normalize_json(value)
    assert fixed.canonical_bytes(envelope['frame']) == fixed.canonical_bytes(fixed.build_observation_frame(envelope['original_snapshot']))
    for name, group in envelope['frame']['instruments']['EUR_USD']['groups'].items():
        captured = value['observation_inputs']['instruments']['EUR_USD']['groups'][name]
        assert group['values'] == captured['values']
        assert group['value_states'] == captured['value_states']
        assert group['observed_utc'] == captured['clock']['observed_utc']
    primary = envelope['frame']['instruments']['EUR_USD']['groups']['primary']
    assert primary['values']['missing'] is None and primary['values']['zero'] == 0.
    assert primary['conflicts']['m5_r1_pips']['reason'] == 'same_identity_different_value_state_or_completion'
    saved = path.read_bytes()
    assert fixed.archive_observation_snapshot(canonical_roundtrip(value), tmp_path) == path
    assert path.read_bytes() == saved


def test_recreation_in_fresh_process_uses_original_archive_bytes(tmp_path):
    path = fixed.archive_observation_snapshot(fixture(), tmp_path)
    script = "import sys,json,gzip;sys.path.insert(0,sys.argv[1]);import oanda_feature_observations_v1 as m;e=json.loads(gzip.open(sys.argv[2],'rb').read());assert m.canonical_bytes(e['frame'])==m.canonical_bytes(m.build_observation_frame(e['original_snapshot']));print(e['payload_sha256'])"
    result = subprocess.run([sys.executable, '-B', '-c', script, str(HERE), str(path)], check=True, capture_output=True, text=True, timeout=10)
    assert result.stdout.strip() == fixed.payload_sha256(fixture())


def test_revised_implementation_identity_creates_distinct_source_schema():
    value = fixture()
    previous = deepcopy(value)
    previous['observation_source']['observation_implementation_sha256'] = old.observation_source_sha256()
    assert fixed.build_observation_frame(value)['source_schema_id'] != old.build_observation_frame(previous)['source_schema_id']


def test_predecessor_archive_is_not_silently_rewritten_or_accepted(tmp_path):
    value = fixture()
    value['observation_source']['observation_implementation_sha256'] = old.observation_source_sha256()
    path = old.archive_observation_snapshot(value, tmp_path)
    saved = path.read_bytes()
    envelope = json.loads(gzip.open(path, 'rb').read())
    assert fixed.canonical_bytes(envelope['frame']) != fixed.canonical_bytes(fixed.build_observation_frame(envelope['original_snapshot']))
    assert path.read_bytes() == saved


def test_new_forward_source_identity_refuses_prior_ledger_and_registers_empty_successor(tmp_path):
    import shutil
    import sqlite3
    names = ('oanda_feature_forward_ledger_v1.py', 'oanda_feature_forward_protocol_v1.py',
             'oanda_exact_price_scoring.py', 'oanda_feature_move_mapping_v1.py',
             'oanda_feature_observations_v1.py', 'oanda_feature_candle_inputs_v2.py',
             'oanda_feature_research_clock_v1.py', 'oanda_feature_forward_worker_v1.py')
    kit = tmp_path / 'kit'; kit.mkdir()
    for name in names:
        shutil.copyfile(PREDECESSOR if name == "oanda_feature_observations_v1.py" else TRAD / name, kit / name)
    sys.path.insert(0, str(TRAD))
    try:
        ledger = load(kit / 'oanda_feature_forward_ledger_v1.py', 'test_candidate_forward_ledger')
        prior_path = tmp_path / 'prior.sqlite'
        owner = ledger.ForwardLedger(prior_path, max_bytes=16*1024*1024, minimum_free_bytes=0)
        previous = dict(owner.source_pins); owner.close()
        prior_bytes = prior_path.read_bytes()
        shutil.copyfile(HERE / 'oanda_feature_observations_v1.py', kit / 'oanda_feature_observations_v1.py')
        with pytest.raises(ValueError, match='ledger_source_generation_changed'):
            ledger.ForwardLedger(prior_path, max_bytes=16*1024*1024, minimum_free_bytes=0)
        assert prior_path.read_bytes() == prior_bytes
        owner = ledger.ForwardLedger(tmp_path / 'successor.sqlite', max_bytes=16*1024*1024, minimum_free_bytes=0)
        current = dict(owner.source_pins)
        assert len(current) == 8
        assert {name for name in current if current[name] != previous[name]} == {'oanda_feature_observations_v1.py'}
        assert owner.db.execute('select count(*) from ff_batches').fetchone()[0] == 0
        assert owner.db.execute('select count(*) from ff_jobs').fetchone()[0] == 0
        owner.close()
    finally:
        sys.path.remove(str(TRAD))
