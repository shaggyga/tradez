"""Rolling materialized news observations, separate from the retained raw archive.

One validated producer snapshot creates one parsed frame. Hot reads use at most
48 hours of compact observations. Historical raw snapshots are replay inputs,
never a prerequisite for current capture. This is a new prospective cohort;
neither old transport receipts nor inferred historical availability are imported.
"""
from pathlib import Path
from types import SimpleNamespace
import bisect
import copy
import gzip
import hashlib
import json
import math
import os
import re
import sqlite3
import threading
import time
import uuid
import weakref
import zlib

import oanda_causal_forecast_inputs_joint_news_v3 as original

SCHEMA = 'rolling_materialized_news_capture_v1_20260930'
CONFIG = 'rolling_materialized_news_config_v1_20260930'
DESCRIPTOR = 'rolling_materialized_news_descriptor_v1_20260930'
POINT = 'rolling_materialized_news_observation_v1_20260930'
WINDOW = 48 * 3600
RETENTION = 72 * 3600
MAX_POINTS = 5000
MAX_POINT = 128 * 1024
MAX_CAPTURE = 32 * 1024**2
MAX_DB = 128 * 1024**2
INERT = dict(research_only=True, can_place_orders=False, can_promote=False,
             can_authorize=False, account_eligible=False, proof_eligible=False,
             execution_eligible=False, joint_model_consumption_proven=False)


def need(value, reason):
    if not value:
        raise ValueError(reason)


def encode(value, maximum=MAX_CAPTURE):
    raw = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
    need(len(raw) <= maximum, 'rolling_byte_bound')
    return raw


def digest(value, maximum=MAX_CAPTURE):
    return hashlib.sha256(encode(value, maximum)).hexdigest()


adapter = SimpleNamespace(original=original, epoch=original._clock, sha=digest)


def path_for(value, *, directory=False, missing=False):
    path = Path(value).absolute()
    need(path.drive.upper() == 'C:' and '..' not in path.parts, 'rolling_plain_c_path')
    for part in (path, *path.parents):
        need(not part.is_symlink() and not part.is_junction(), 'rolling_reparse_refused')
    if path.exists():
        need(path.is_dir() if directory else path.is_file(), 'rolling_path_kind')
    else:
        need(missing, 'rolling_path_missing')
    return path


def read_exact(value, limit):
    path = path_for(value)
    raw = original._read(path, limit)
    return raw, {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}


def strict_health_json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            need(key not in result, 'duplicate_json_key')
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=unique, parse_constant=lambda _: need(False, 'nonfinite_json'))


def source_graph():
    values = original._bindings()
    values[Path(__file__).name] = read_exact(__file__, 1024**2)[1]['sha256']
    return values


def _put(root, value, maximum=MAX_CAPTURE):
    raw = encode(value, maximum)
    key = hashlib.sha256(raw).hexdigest()
    root = path_for(root, directory=True)
    path = root / (key + '.json.gz')
    if not path.exists():
        temporary = root / (uuid.uuid4().hex + '.pending')
        with temporary.open('xb') as stream:
            stream.write(gzip.compress(raw, mtime=0)); stream.flush(); os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            pass
        finally:
            temporary.unlink()
    need(_get(root, key, maximum) == value, 'rolling_archive_readback_mismatch')
    return key


def _get(root, key, maximum=MAX_CAPTURE):
    need(type(key) is str and re.fullmatch('[a-f0-9]{64}', key), 'rolling_hash_required')
    raw, _ = read_exact(Path(root) / (key + '.json.gz'), maximum + 65536)
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    body = decoder.decompress(raw, maximum + 1)
    need(len(body) <= maximum and decoder.eof and not decoder.unconsumed_tail and not decoder.unused_data,
         'rolling_archive_geometry')
    need(hashlib.sha256(body).hexdigest() == key, 'rolling_archive_hash')
    return strict_health_json(body)


class Session: pass
class Capture: pass
_sessions = weakref.WeakKeyDictionary()
_captures = weakref.WeakKeyDictionary()


def _session(session):
    need(type(session) is Session and session in _sessions, 'rolling_owned_session_required')
    return _sessions[session]


def _capture(capture):
    need(type(capture) is Capture and capture in _captures, 'rolling_owned_capture_required')
    return _captures[capture]


def create_session(config):
    keys = {'schema_version', 'current_path', 'clock_path', 'heartbeat_path', 'store_path', 'archive_root', 'source_bindings'}
    need(type(config) is dict and set(config) == keys and config['schema_version'] == CONFIG, 'rolling_config_shape')
    graph = source_graph()
    need(config['source_bindings'] == graph, 'rolling_sources_changed')
    for field in ('current_path', 'clock_path', 'heartbeat_path'):
        path_for(config[field])
    root = path_for(config['archive_root'], directory=True, missing=True)
    root.mkdir(parents=True, exist_ok=True)
    dbpath = path_for(config['store_path'], missing=True)
    need(dbpath.parent == root.parent, 'rolling_owned_index_location')
    profile = {'schema': POINT, 'source_bindings': graph, 'window': WINDOW, 'retention': RETENTION,
               'availability': 'actual_validated_snapshot_archive_readback_then_materializer_observation', **INERT}
    with sqlite3.connect(dbpath) as db:
        db.execute('PRAGMA synchronous=FULL')
        db.executescript('CREATE TABLE IF NOT EXISTS profile(id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL);'
                        'CREATE TABLE IF NOT EXISTS points(observed REAL PRIMARY KEY, body TEXT NOT NULL, sha TEXT NOT NULL);')
        rows = db.execute('SELECT body FROM profile').fetchall()
        if not rows:
            db.execute('INSERT INTO profile VALUES(1,?)', (encode(profile).decode(),))
        else:
            need(rows == [(encode(profile).decode(),)], 'rolling_index_generation_changed')
        need(db.execute('SELECT COUNT(*) FROM points').fetchone()[0] <= MAX_POINTS, 'rolling_point_count_bound')
    session = Session()
    _sessions[session] = {'config': encode(config), 'graph': graph, 'lock': threading.RLock(),
                          'failure_serial': 0, 'usable': False, 'last_error': None, 'last_capture': None}
    return session


def _health(config, clock):
    now = adapter.epoch(clock())
    producer = original._module(original.CURRENT_PRODUCER_MODULE)
    values = {}
    for field in ('clock_path', 'heartbeat_path'):
        raw, proof = read_exact(config[field], 1024**2)
        values[field] = {'value': strict_health_json(raw), 'sha256': proof['sha256']}
    producer.validate_clock_state(values['clock_path']['value'], now)
    producer.validate_collector_observation(values['heartbeat_path']['value'], now)
    return {'observed_epoch': now, 'files': values}


def _failure(state, config, operation, exc):
    state['failure_serial'] += 1
    state['usable'] = False
    state['last_error'] = operation + ':' + type(exc).__name__ + ':' + str(exc)[:300]


def session_status(session):
    state = _session(session)
    return {'failure_serial': state['failure_serial'], 'usable': state['usable'],
            'last_error': state['last_error'], 'last_failure': state['last_error'],
            'bootstrap_progress': None, 'capture_schema': SCHEMA, **INERT}


def bootstrap_inputs(session, *, clock=time.time):
    state = _session(session)
    need(source_graph() == state['graph'], 'rolling_sources_changed')
    # Cold startup never replays the raw archive. The bounded hot index is checked
    # on actual capture; this diagnostic grants no freshness or forecast authority.
    return {'status': 'cache_prepared_fresh_capture_required', 'elapsed_sec': 0.,
            'fresh_health_proven': False, 'capture_handle_returned': False,
            'original_availability_unchanged': True, 'scope': 'rolling_index_only_no_archive_replay', **INERT}


def _validate_point(point, graph):
    need(point['schema_version'] == POINT and point['source_generation_sha256'] == digest(graph), 'rolling_point_generation')
    start, end, observed = (adapter.epoch(point[k]) for k in ('scan_started_epoch', 'scan_completed_epoch', 'observed_epoch'))
    need(start <= end <= observed <= start + 300, 'rolling_point_clock_order')
    need(type(point['frame']) is dict and digest(point['frame']) == point['frame_sha256'], 'rolling_frame_hash')
    need(point['frame']['available_max_epoch'] <= observed, 'rolling_future_frame')
    encode(point, MAX_POINT)


def _validate_record(record, graph):
    point = record['point']
    _validate_point(point, graph)
    need(digest(point, MAX_POINT) == record['point_sha256'], 'rolling_record_point_hash')
    available = adapter.epoch(record['available_epoch'])
    need(point['observed_epoch'] <= available <= point['scan_started_epoch']+300, 'rolling_record_availability')


def capture_shared(session, *, clock=time.time):
    state = _session(session); config = json.loads(state['config'])
    with state['lock']:
        try:
            wall = time.monotonic(); before = _health(config, clock)
            need(source_graph() == state['graph'], 'rolling_sources_changed')
            raw, proof = read_exact(config['current_path'], original.MAX_CURRENT_NEWS_BYTES)
            snapshot = strict_health_json(raw)
            guard = original._module('oanda_news_causal_aggregation_guard_v2')
            topics = original._validate_current(snapshot, state['graph'], guard)
            start = original._epoch(snapshot['as_of_utc']); generated = original._epoch(snapshot['generated_utc'])
            observed = adapter.epoch(clock())
            need(start <= generated <= observed <= start + 300, 'rolling_snapshot_stale_or_future')
            members = {}
            for topic in topics:
                for value in topic['causal_aggregation_guard']['members']:
                    member = copy.deepcopy(value)
                    member['observed_available_utc'] = original._iso(max(observed, original._epoch(member['observed_available_utc']) if member.get('observed_available_utc') else observed))
                    key = member['event_id']
                    need(key not in members or members[key] == member, 'rolling_conflicting_revision')
                    members[key] = member
            # Reuse the original guarded aggregation and eight-feature math.
            frame = original._frame({'news_capture_sha256': digest([proof['sha256'], observed]),
                'first_observed_epoch': observed, 'current_members': list(members.values())}, observed, live=True)
            snapshot_sha = _put(config['archive_root'], snapshot)
            materialized = adapter.epoch(clock())
            point = {'schema_version': POINT, 'source_generation_sha256': digest(state['graph']),
                'snapshot_sha256': snapshot_sha, 'source_file_sha256': proof['sha256'],
                'scan_started_epoch': start, 'scan_completed_epoch': generated,
                'feature_epoch': observed, 'observed_epoch': materialized,
                'frame': frame, 'frame_sha256': digest(frame), **INERT}
            _validate_point(point, state['graph'])
            point_sha = _put(config['archive_root'], point, MAX_POINT)
            # This named materializer actually read the durable parsed record.
            available = adapter.epoch(clock())
            need(materialized <= available <= start + 300, 'rolling_materialization_expired')
            record = {'point_sha256': point_sha, 'available_epoch': available, 'point': point}
            record_sha = _put(config['archive_root'], record, MAX_POINT)
            path = path_for(config['store_path'])
            need(path.stat().st_size <= MAX_DB, 'rolling_index_byte_bound')
            with sqlite3.connect(path, timeout=2) as db:
                db.execute('PRAGMA synchronous=FULL')
                db.execute('INSERT INTO points VALUES(?,?,?)', (available, encode(record, MAX_POINT).decode(), record_sha))
                # All records already have exact durable archive/readback. Only
                # expired index entries are removed; archived evidence stays.
                db.execute('DELETE FROM points WHERE observed<?', (available-RETENTION,))
                need(db.execute('SELECT COUNT(*) FROM points').fetchone()[0] <= MAX_POINTS, 'rolling_point_count_bound')
                db.commit()
            with sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=2) as db:
                rows = db.execute('SELECT observed,body,sha FROM points WHERE observed>=? ORDER BY observed', (available-WINDOW-300,)).fetchall()
            need(len(rows) <= MAX_POINTS, 'rolling_point_count_bound')
            records = []
            for stamp, body, sha in rows:
                value = strict_health_json(body)
                need(value['available_epoch'] == stamp and digest(value, MAX_POINT) == sha, 'rolling_index_hash')
                need(_get(config['archive_root'], sha, MAX_POINT) == value, 'rolling_index_archive_binding')
                _validate_record(value, state['graph'])
                records.append(value)
            after = _health(config, clock); read = adapter.epoch(clock())
            need(before['observed_epoch'] <= read and available <= read <= start+300, 'rolling_capture_clock_order')
            # Keep every interval needed by the declared 48h window. No age or
            # coverage gate can be renewed by reading an old point again.
            bundle = {'records': records, 'source_generation_sha256': digest(state['graph'])}
            body = {'schema_version': SCHEMA, 'config_sha256': digest(config), 'source_graph': state['graph'],
                'readback_sha256': digest(bundle), 'readback_bytes': len(encode(bundle)),
                'read_completed_epoch': read, 'first_observed_epoch': adapter.epoch(clock()),
                'health_before': before, 'health_after': after, 'bundle': bundle, **INERT}
            need(time.monotonic()-wall <= 30, 'rolling_capture_duration_bound')
            need(source_graph() == state['graph'], 'rolling_sources_changed')
            key = _put(config['archive_root'], body)
            returned = adapter.epoch(clock())
            need(body['first_observed_epoch'] <= returned <= start+300 and time.monotonic()-wall <= 30,
                 'rolling_final_capture_duration_or_freshness')
            need(source_graph() == state['graph'], 'rolling_sources_changed')
            descriptor = {'schema_version': DESCRIPTOR, 'capture_sha256': key,
                          'capture_path': str(Path(config['archive_root'])/(key+'.json.gz'))}
            capture = Capture()
            _captures[capture] = (bundle, encode(body), encode(descriptor), session, state['failure_serial'], 'actual_capture',
                                  (body['readback_sha256'], tuple(r['available_epoch'] for r in records)),
                                  encode({k: v for k, v in body.items() if k not in ('bundle', 'health_before', 'health_after')}))
            state['usable'] = True; state['last_error'] = None; state['last_capture'] = descriptor
            return capture
        except Exception as exc:
            _failure(state, config, 'capture_shared', exc)
            raise


def capture_metadata(capture):
    parts = _capture(capture)
    return {'capture': json.loads(parts[7]), 'descriptor': json.loads(parts[2]),
            'scope': parts[5], 'replay_is_not_fresh_capture': parts[5] != 'actual_capture'}


def _eligible(session, capture):
    state = _session(session); parts = _capture(capture)
    need(parts[3] is session and parts[5] == 'actual_capture' and parts[4] == state['failure_serial'] and state['usable'],
         'rolling_capture_invalidated_or_replay_only')
    return state, parts


def pair_features(capture, instrument, decision):
    need(type(instrument) is str and re.fullmatch('[A-Z]{3}_[A-Z]{3}', instrument), 'rolling_pair_identity')
    decision = adapter.epoch(decision); parts = _capture(capture)
    records = parts[0]['records']; context, epochs = parts[6]
    index = bisect.bisect_right(epochs, decision)-1
    row = records[index] if index >= 0 else None
    point = row['point'] if row else None
    usable = bool(row and point['scan_started_epoch'] <= decision <= point['scan_started_epoch']+300)
    coverage = None if row is None else {'read_started_epoch': point['scan_started_epoch'], 'read_completed_epoch': point['scan_completed_epoch']}
    selected = digest([row['point_sha256'], row['available_epoch']]) if row else digest(None)
    timing = {'decision_epoch': decision, 'context_sha256': context,
        'transport_selection_sha256': selected, 'source_generation_sha256': parts[0]['source_generation_sha256'],
        'consumer_id': 'rolling_materializer_v1', 'index_row_ordinal': index if row else None,
        'consumer_observed_epoch': row['available_epoch'] if row else None,
        'observed_publication_prefix': index+1, 'coverage': coverage,
        'coverage_sha256': digest(coverage) if coverage else None, 'coverage_usable': usable}
    result = {'decision_epoch': decision, 'context_sha256': context, 'source_generation_sha256': parts[0]['source_generation_sha256'],
        'transport_selection_sha256': selected, 'transport_timing': timing, 'transport_timing_sha256': digest(timing),
        'coverage_usable': usable, 'status': 'available' if usable else 'unavailable',
        'frame': None, 'features': None, 'reason': None if usable else 'no_fresh_observed_rolling_point', **INERT}
    if usable:
        frame = copy.deepcopy(point['frame'])
        frame['members'] = [m for m in frame['members'] if decision-3600 <= m['known_epoch'] <= decision]
        frame['directional'] = [m for m in frame['directional'] if m['expires_epoch'] >= decision]
        features, expires = original._pair_features(frame, instrument, decision)
        result.update(frame=frame, frame_sha256=digest(frame), features=features, expires_epoch=expires)
    return result


def current_pair_features(session, capture, instrument, decision_epoch):
    _eligible(session, capture)
    need(decision_epoch >= capture_metadata(capture)['capture']['first_observed_epoch'], 'rolling_decision_before_capture')
    return pair_features(capture, instrument, decision_epoch)


def reobserve_before_issue(session, capture, *, clock=time.time):
    state, _ = _eligible(session, capture); config = json.loads(state['config'])
    try:
        need(source_graph() == state['graph'], 'rolling_sources_changed')
        health = _health(config, clock)
        raw, proof = read_exact(config['current_path'], original.MAX_CURRENT_NEWS_BYTES)
        snapshot = strict_health_json(raw)
        original._validate_current(snapshot, state['graph'], original._module('oanda_news_causal_aggregation_guard_v2'))
        now = adapter.epoch(clock())
        need(original._epoch(snapshot['as_of_utc']) <= now <= original._epoch(snapshot['as_of_utc'])+300, 'rolling_issue_news_stale')
        value = {'health': health, 'current_snapshot_sha256': proof['sha256'], 'observed_epoch': now, **INERT}
        key = _put(config['archive_root'], value)
        return {'health_proof_sha256': key, 'observed_epoch': now, 'readback_observed_epoch': adapter.epoch(clock()),
                'scope': 'current_health_only_retained_input_expiry_guards_still_required', **INERT}
    except Exception as exc:
        _failure(state, config, 'reobserve_before_issue', exc); raise


def replay_capture(session, descriptor):
    state = _session(session); config = json.loads(state['config'])
    need(descriptor['schema_version'] == DESCRIPTOR, 'rolling_descriptor_schema')
    key = descriptor['capture_sha256']
    need(descriptor['capture_path'] == str(Path(config['archive_root'])/(key+'.json.gz')), 'rolling_descriptor_path')
    value = _get(config['archive_root'], key)
    need(value['source_graph'] == source_graph() == state['graph'] and value['config_sha256'] == digest(config), 'rolling_replay_identity')
    need(digest(value['bundle']) == value['readback_sha256'], 'rolling_replay_bundle_hash')
    for row in value['bundle']['records']:
        point = row['point']; _validate_record(row, state['graph'])
        need(_get(config['archive_root'], row['point_sha256'], MAX_POINT) == point, 'rolling_replay_point_hash')
        snapshot = _get(config['archive_root'], point['snapshot_sha256'])
        topics = original._validate_current(snapshot, state['graph'], original._module('oanda_news_causal_aggregation_guard_v2'))
        members = {}
        observed = point['feature_epoch']
        for topic in topics:
            for item in topic['causal_aggregation_guard']['members']:
                item = copy.deepcopy(item)
                item['observed_available_utc'] = original._iso(max(observed, original._epoch(item['observed_available_utc']) if item.get('observed_available_utc') else observed))
                need(item['event_id'] not in members or members[item['event_id']] == item, 'rolling_replay_revision')
                members[item['event_id']] = item
        frame = original._frame({'news_capture_sha256': digest([point['source_file_sha256'], observed]), 'first_observed_epoch': observed,
                                 'current_members': list(members.values())}, observed, live=True)
        need(frame == point['frame'], 'rolling_replay_frame_mismatch')
    capture = Capture()
    _captures[capture] = (value['bundle'], encode(value), encode(descriptor), None, None, 'immutable_replay',
                          (value['readback_sha256'], tuple(r['available_epoch'] for r in value['bundle']['records'])),
                          encode({k: v for k, v in value.items() if k not in ('bundle', 'health_before', 'health_after')}))
    return capture


def close_session(session):
    _sessions.pop(session, None)
