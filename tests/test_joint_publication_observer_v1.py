import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import oanda_joint_publication_observer_v1 as m


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


@pytest.fixture
def fixture(tmp_path):
    root = tmp_path / 'project'
    root.mkdir()
    (root / 'config').mkdir()
    study = root / 'data/oanda_training_manager/joint_price_news_study_v3'
    study.mkdir(parents=True)
    sources = {}
    for name in m.SOURCE_NAMES:
        raw = ('# source identity fixture ' + name).encode()
        (root / name).write_bytes(raw)
        sources[name] = sha(raw)
    registry = {**m.AUTHORITY, 'registry_id': m.REGISTRY_ID,
                'schema_version': 'joint_price_news_registry_v3_20260908',
                'collection_enabled': True, 'source_bindings': sources,
                'pairs': {'EUR_USD': {}, 'GBP_USD': {}}}
    raw = encoded(registry)
    (root / 'config' / (m.REGISTRY_ID + '.json')).write_bytes(raw)
    registry_sha = sha(raw)

    def summary(generated=990, marker=0):
        body = {**m.AUTHORITY, 'schema_version': m.SUMMARY_SCHEMA,
                'registry_sha256': registry_sha, 'generated_epoch': generated,
                'rows': [{'instrument': p, 'marker': marker} for p in registry['pairs']]}
        return {**body, 'payload_sha256': sha(encoded(body))}

    def heartbeat(value, generated=995):
        digest = value if isinstance(value, str) else sha(encoded(value))
        return {**m.AUTHORITY, 'schema_version': m.HEARTBEAT_SCHEMA,
                'registry_sha256': registry_sha, 'generated_epoch': generated,
                'summary_sha256': digest}

    def write(value, hb=None):
        (study / 'summary.json').write_bytes(encoded(value))
        (study / 'heartbeat.json').write_bytes(encoded(hb or heartbeat(value)))

    return root, study, registry, registry_sha, summary, heartbeat, write


def observe(fixture, reader=None, **kwargs):
    root, _, _, digest, *_ = fixture
    return (reader or m.JointV3EnvelopeObserver(digest)).observe(
        root, clock=kwargs.pop('clock', lambda: 1000), **kwargs)


def reseal(value):
    value['payload_sha256'] = sha(encoded({k: v for k, v in value.items()
                                          if k != 'payload_sha256'}))
    return value


def test_exact_pair_is_only_transport_acceptance(fixture):
    _, _, _, _, summary, _, write = fixture
    value = summary()
    value['rows'][0]['families'] = {'invalid_unverified_forecast': {'target_epoch': 0}}
    write(reseal(value))
    result = observe(fixture)
    assert result['status'] == 'coherent_envelope_observed'
    assert result['rows_validated'] is False
    assert all(result[k] is expected for k, expected in m.AUTHORITY.items())
    assert 'active_forecasts' not in result
    assert len(result['attempts'][0]['source_reads']) == 20


def test_pending_bytes_require_later_real_heartbeat(fixture):
    _, _, _, digest, summary, heartbeat, write = fixture
    reader = m.JointV3EnvelopeObserver(digest)
    a, b = summary(990, 1), summary(992, 2)
    write(a, heartbeat('0' * 64))
    first = observe(fixture, reader)
    assert first['reason'] == 'heartbeat_generation_not_observed'
    write(b, heartbeat(a))
    second = observe(fixture, reader)
    assert second['status'] == 'coherent_envelope_observed'
    assert second['selected']['used_retained_generation'] is True
    assert second['selected']['canonical_summary_sha256'] == sha(encoded(a))
    assert second['selected']['summary_generated_epoch'] == 990
    assert second['selected']['summary_first_observed_epoch'] == 1000
    assert second['retained_bytes'][-1]['raw'] == encoded(a)


def test_no_previously_observed_generation_is_withheld(fixture):
    _, _, _, _, summary, heartbeat, write = fixture
    write(summary(992), heartbeat(summary(990)))
    assert observe(fixture)['reason'] == 'heartbeat_generation_not_observed'


@pytest.mark.parametrize('kind', ['seal', 'summary_future', 'summary_stale',
                                   'summary_authority', 'heartbeat_future',
                                   'heartbeat_stale', 'heartbeat_authority',
                                   'summary_schema', 'heartbeat_identity'])
def test_invalid_current_record_cannot_hide_behind_cache(fixture, kind):
    _, _, _, digest, summary, heartbeat, write = fixture
    reader = m.JointV3EnvelopeObserver(digest)
    a = summary(990)
    write(a)
    assert observe(fixture, reader)['status'] == 'coherent_envelope_observed'
    b, hb = summary(992), heartbeat(a)
    if kind == 'seal':
        b['payload_sha256'] = '0' * 64
    if kind == 'summary_future':
        b['generated_epoch'] = 1001
    if kind == 'summary_stale':
        b['generated_epoch'] = 909
    if kind == 'summary_authority':
        b['can_place_orders'] = True
    if kind == 'summary_schema':
        b['schema_version'] = 'some_other_cohort'
    if kind == 'heartbeat_future':
        hb['generated_epoch'] = 1001
    if kind == 'heartbeat_stale':
        hb['generated_epoch'] = 909
    if kind == 'heartbeat_authority':
        hb['can_authorize'] = 0
    if kind == 'heartbeat_identity':
        hb['registry_sha256'] = '0' * 64
    if kind != 'seal':
        reseal(b)
    write(b, hb)
    result = observe(fixture, reader)
    assert result['status'] == 'unavailable'
    assert result['selected'] is None
    assert len(result['attempts']) == 1


def test_pending_summary_expiry_is_not_refreshed(fixture):
    _, _, _, digest, summary, heartbeat, write = fixture
    reader = m.JointV3EnvelopeObserver(digest)
    a = summary(909)
    write(a, heartbeat('0' * 64, 950))
    assert observe(fixture, reader, clock=lambda: 950)['reason'] == 'heartbeat_generation_not_observed'
    write(summary(992), heartbeat(a))
    assert observe(fixture, reader)['reason'] == 'summary_stale_or_future'


def test_matching_heartbeat_cannot_precede_selected_summary(fixture):
    _, _, _, _, summary, heartbeat, write = fixture
    a = summary(990)
    write(a, heartbeat(a, 989))
    assert observe(fixture)['reason'] == 'paired_generation_clock_order'


def test_duplicate_pair_inventory_is_rejected(fixture):
    _, _, _, _, summary, _, write = fixture
    a = summary()
    a['rows'][1]['instrument'] = 'EUR_USD'
    write(reseal(a))
    assert observe(fixture)['reason'] == 'summary_pair_inventory'


@pytest.mark.parametrize('target', ['registry', 'source'])
def test_changed_registered_identity_blocks_cached_generation(fixture, target):
    root, _, registry, digest, summary, _, write = fixture
    reader = m.JointV3EnvelopeObserver(digest)
    write(summary())
    assert observe(fixture, reader)['status'] == 'coherent_envelope_observed'
    if target == 'registry':
        registry['extra'] = 'different identity'
        (root / 'config' / (m.REGISTRY_ID + '.json')).write_bytes(encoded(registry))
    else:
        (root / next(iter(m.SOURCE_NAMES))).write_bytes(b'changed')
    result = observe(fixture, reader)
    assert result['reason'] == ('registry_identity_changed' if target == 'registry'
                                 else 'registered_source_changed')


def test_exact_duplicate_json_key_is_rejected_and_bytes_retained(fixture):
    _, study, _, _, summary, _, write = fixture
    write(summary())
    raw = (study / 'summary.json').read_bytes()
    corrupt = raw[:-1] + b',"rows":[]}'
    (study / 'summary.json').write_bytes(corrupt)
    result = observe(fixture)
    assert result['reason'] == 'duplicate_json_key'
    assert result['retained_bytes'][0]['raw'] == corrupt


@pytest.mark.parametrize('value', [b'{"x": NaN}', b'{"x": 1e400}', b'[]', b'{'])
def test_malformed_or_nonfinite_summary_is_withheld(fixture, value):
    _, study, _, _, summary, _, write = fixture
    write(summary())
    (study / 'summary.json').write_bytes(value)
    assert observe(fixture)['status'] == 'unavailable'


def test_generation_capacity_and_late_admission(fixture):
    _, _, _, digest, *_ = fixture
    reader = m.JointV3EnvelopeObserver(digest)
    key = ('root', digest)
    for stamp in (100, 200, 50, 300, 40):
        raw = str(stamp).encode()
        reader._admit(key, m._Generation(raw, sha(raw), stamp, stamp + 1))
    assert [v.generated_epoch for v in reader._cache[key]] == [300, 200]
    for i in range(5):
        reader._admit((str(i), digest), m._Generation(b'x', sha(b'x'), 1, 2))
    assert len(reader._cache) == 4
    assert key not in reader._cache


def test_equal_generation_time_cannot_redefine_bytes(fixture):
    _, _, _, digest, summary, _, write = fixture
    reader = m.JointV3EnvelopeObserver(digest)
    write(summary(990, 1))
    assert observe(fixture, reader)['status'] == 'coherent_envelope_observed'
    write(summary(990, 2))
    assert observe(fixture, reader)['reason'] == 'generation_clock_conflict'


def test_returned_mutation_cannot_change_cache(fixture):
    _, _, _, digest, summary, heartbeat, write = fixture
    reader = m.JointV3EnvelopeObserver(digest)
    a = summary()
    write(a)
    result = observe(fixture, reader)
    result['selected']['summary_generated_epoch'] = 99999
    result['retained_bytes'][-1]['raw'] = b'altered'
    write(summary(992), heartbeat(a))
    later = observe(fixture, reader)
    assert later['selected']['summary_generated_epoch'] == 990
    assert later['retained_bytes'][-1]['raw'] == encoded(a)


def test_wait_retries_only_pairing_and_records_failed_attempt(fixture):
    _, _, _, _, summary, heartbeat, write = fixture
    a = summary()
    write(a, heartbeat('0' * 64))
    mono = [0.0]
    pauses = []

    def sleep(duration):
        pauses.append(duration)
        mono[0] += duration
        write(a)

    result = observe(fixture, max_wait_sec=6, sleep=sleep,
                     monotonic_clock=lambda: mono[0])
    assert result['status'] == 'coherent_envelope_observed'
    assert len(result['attempts']) == 2
    assert result['attempts'][0]['reason'] == 'heartbeat_generation_not_observed'
    assert pauses == [.5]


def test_attempt_and_elapsed_bounds(fixture):
    _, _, _, _, summary, heartbeat, write = fixture
    write(summary(), heartbeat('0' * 64))
    pauses = []
    result = observe(fixture, max_wait_sec=6, sleep=pauses.append,
                     monotonic_clock=lambda: 0)
    assert len(result['attempts']) == 13
    assert len(pauses) == 12
    ticks = iter([0, 7])
    result = observe(fixture, max_wait_sec=6, sleep=lambda _: pytest.fail('late sleep'),
                     monotonic_clock=lambda: next(ticks))
    assert len(result['attempts']) == 1


def test_no_new_read_begins_when_sleep_reaches_deadline(fixture):
    _, _, _, _, summary, heartbeat, write = fixture
    write(summary(), heartbeat('0' * 64))
    now = [0]

    def sleep(duration):
        now[0] = 6
        # A successful new pair must remain unread after the deadline.
        write(summary())

    result = observe(fixture, max_wait_sec=6, sleep=sleep,
                     monotonic_clock=lambda: now[0])
    assert result['reason'] == 'heartbeat_generation_not_observed'
    assert len(result['attempts']) == 1


def test_consumer_and_monotonic_regression(fixture):
    _, _, _, _, summary, heartbeat, write = fixture
    write(summary())
    ticks = [1000, 999]
    result = observe(fixture, clock=lambda: ticks.pop(0) if ticks else 999)
    assert result['reason'] == 'consumer_clock_regression'
    write(summary(), heartbeat('0' * 64))
    ticks = iter([1, 0])
    result = observe(fixture, max_wait_sec=6, monotonic_clock=lambda: next(ticks))
    assert result['reason'] == 'monotonic_clock_regression'


@pytest.mark.parametrize('budget', [-1, 7, True, float('nan'), float('inf')])
def test_invalid_wait_budget(fixture, budget):
    with pytest.raises(m.ObservationError, match='wait_budget'):
        observe(fixture, max_wait_sec=budget)


def test_byte_limit_is_explicit(fixture):
    _, study, _, _, summary, _, write = fixture
    write(summary())
    (study / 'heartbeat.json').write_bytes(b' ' * (m.MAX_HEARTBEAT_BYTES + 1))
    assert observe(fixture)['reason'] == 'file_byte_limit'


def test_future_summary_cannot_age_valid_during_later_read(fixture, monkeypatch):
    _, _, _, _, summary, heartbeat, write = fixture
    a = summary(1001)
    write(a, heartbeat(a, 1001))
    now = [1000]
    original = m.read_stable

    def read(path, limit, *, clock):
        if Path(path).name == 'heartbeat.json':
            now[0] = 1002
        return original(path, limit, clock=clock)

    monkeypatch.setattr(m, 'read_stable', read)
    result = observe(fixture, clock=lambda: now[0])
    assert result['reason'] == 'summary_stale_or_future'


def test_registered_source_change_during_envelope_read_is_rejected(fixture, monkeypatch):
    root, _, _, _, summary, _, write = fixture
    write(summary())
    original = m.read_stable

    def read(path, limit, *, clock):
        result = original(path, limit, clock=clock)
        if Path(path).name == 'heartbeat.json':
            (root / sorted(m.SOURCE_NAMES)[0]).write_bytes(b'changed_after_first_hash')
        return result

    monkeypatch.setattr(m, 'read_stable', read)
    result = observe(fixture)
    assert result['reason'] == 'registered_source_changed'


def test_cross_file_clock_regression_is_rejected(fixture, monkeypatch):
    _, _, _, _, summary, _, write = fixture
    write(summary())
    original = m.read_stable

    def read(path, limit, *, clock):
        raw, receipt = original(path, limit, clock=clock)
        if Path(path).name == sorted(m.SOURCE_NAMES)[0]:
            receipt['read_started_epoch'] = 999
        return raw, receipt

    monkeypatch.setattr(m, 'read_stable', read)
    assert observe(fixture)['reason'] == 'consumer_clock_regression'


def test_concurrent_generation_admission_cannot_backdate_consumption(fixture, monkeypatch):
    _, _, _, digest, summary, _, write = fixture
    a = summary()
    write(a)
    reader = m.JointV3EnvelopeObserver(digest)
    # Deterministic interleaving: another caller admits this generation after
    # the current call's final cutoff but before its cache selection obtains it.
    monkeypatch.setattr(reader, '_select', lambda key, wanted:
                        m._Generation(encoded(a), wanted, 990, 1001))
    result = observe(fixture, reader)
    assert result['reason'] == 'selected_generation_observed_after_cutoff'
    assert result['selected'] is None


def test_cache_does_not_cross_project_roots(fixture, tmp_path):
    root, _, _, digest, summary, heartbeat, write = fixture
    reader = m.JointV3EnvelopeObserver(digest)
    a = summary()
    write(a)
    assert observe(fixture, reader)['status'] == 'coherent_envelope_observed'
    write(summary(992), heartbeat(a))
    second = tmp_path / 'second'
    shutil.copytree(root, second)
    result = reader.observe(second, clock=lambda: 1000)
    assert result['reason'] == 'heartbeat_generation_not_observed'


def test_regular_path_and_relative_path_guards(fixture):
    root, _, _, digest, summary, _, write = fixture
    write(summary())
    with pytest.raises(m.ObservationError, match='absolute_path_required'):
        m.JointV3EnvelopeObserver(digest).observe('relative/project')
    path = root / 'directory_instead_of_file'
    path.mkdir()
    with pytest.raises(m.ObservationError, match='path_type'):
        m.read_stable(path, 100, clock=lambda: 1000)


@pytest.mark.skipif(os.name != 'nt', reason='Windows junction semantics')
def test_actual_ntfs_junction_is_rejected(fixture, tmp_path):
    root, _, _, digest, summary, _, write = fixture
    write(summary())
    junction = tmp_path / 'junction'
    # Native PowerShell creates this local fixture; no deletion/move or cmd shell.
    quoted_link = str(junction).replace("'", "''")
    quoted_root = str(root).replace("'", "''")
    command = ("New-Item -ItemType Junction -Path '" + quoted_link +
               "' -Target '" + quoted_root + "' | Out-Null")
    subprocess.run(['powershell', '-NoProfile', '-Command', command], check=True,
                   capture_output=True, timeout=20)
    result = m.JointV3EnvelopeObserver(digest).observe(junction, clock=lambda: 1000)
    assert result['reason'] == 'reparse_path'
