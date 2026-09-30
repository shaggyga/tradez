import copy
import json
import sqlite3
from pathlib import Path

import pytest
import rolling_news_io_v1 as io
import rolling_news_history_v1 as history
import revision_joint_inputs_v7 as inputs
from test_oanda_local_news_sentiment_repair_v2 import make_repaired_fixture, NOW


def setup(tmp_path):
    f = make_repaired_fixture(tmp_path)
    data = f['data_root']; current = data/'current.json'
    current.write_text(json.dumps(f['snapshot']))
    cfg = {'schema_version': io.CONFIG, 'current_path': str(current),
           'clock_path': str(data/'state/clock_integrity_v1.json'),
           'heartbeat_path': str(data/'local_news_sentiment/collector_heartbeat_v1.json'),
           'store_path': str(data/'rolling.sqlite'), 'archive_root': str(data/'archive'),
           'source_bindings': io.source_graph()}
    return io.create_session(cfg), cfg


def test_actual_materialization_and_replay_match_without_legacy_archive(tmp_path):
    session, cfg = setup(tmp_path)
    capture = io.capture_shared(session, clock=lambda: NOW)
    point = io.current_pair_features(session, capture, 'EUR_USD', NOW)
    assert point['coverage_usable'] and point['features'][0] == pytest.approx(-.4)
    assert not io.pair_features(capture, 'EUR_USD', NOW-1)['coverage_usable']
    assert not io.pair_features(capture, 'EUR_USD', NOW+301)['coverage_usable']
    replay = io.replay_capture(session, io.capture_metadata(capture)['descriptor'])
    assert io.pair_features(replay, 'EUR_USD', NOW) == point
    with pytest.raises(ValueError, match='replay_only'):
        io.current_pair_features(session, replay, 'EUR_USD', NOW)
    # There is deliberately no old publication / consumer DB in this fixture.
    assert io.session_status(session)['usable'] is True


def test_foreign_capture_and_failed_refresh_cannot_issue(tmp_path):
    session, cfg = setup(tmp_path)
    capture = io.capture_shared(session, clock=lambda: NOW)
    other = io.create_session(cfg)
    with pytest.raises(ValueError, match='invalidated'):
        io.current_pair_features(other, capture, 'EUR_USD', NOW)
    with pytest.raises(ValueError):
        io.capture_shared(session, clock=lambda: NOW+301)
    with pytest.raises(ValueError, match='invalidated'):
        io.current_pair_features(session, capture, 'EUR_USD', NOW+302)


@pytest.mark.parametrize('defect', ['payload', 'source', 'clock', 'index', 'archive'])
def test_corruption_or_staleness_refuses_current_capture(tmp_path, defect):
    session, cfg = setup(tmp_path)
    first = io.capture_shared(session, clock=lambda: NOW)
    if defect == 'payload':
        p = Path(cfg['current_path']); x = json.loads(p.read_bytes()); x['topic_count'] += 1; p.write_text(json.dumps(x))
    elif defect == 'source':
        io._session(session)['graph'] = {}
    elif defect == 'clock':
        p = Path(cfg['clock_path']); x = json.loads(p.read_bytes()); x['status'] = 'untrusted'; p.write_text(json.dumps(x))
    elif defect == 'index':
        with sqlite3.connect(cfg['store_path']) as db:
            db.execute("UPDATE points SET body=replace(body,'rolling_materialized','forged_materialized')")
    else:
        row = io._capture(first)[0]['records'][0]
        p = Path(cfg['archive_root'])/(io.digest(row)+'.json.gz'); p.write_bytes(b'corrupt')
    with pytest.raises((ValueError, OSError, KeyError, Exception)):
        io.capture_shared(session, clock=lambda: NOW+1)
    assert not io.session_status(session)['usable']


def test_historical_missing_coverage_is_not_zero_imputed(tmp_path):
    session, _ = setup(tmp_path)
    capture = io.capture_shared(session, clock=lambda: NOW)
    shared = history.prepare_universal_history_share(session, capture, ['EUR_USD'])
    origins = history.universal_origins(NOW)
    result = history.project_pair_history(shared, session=session, news_capture=capture, instrument='EUR_USD', origins=origins)
    assert len(result['records']) == len(origins)
    assert all(row['point']['features'] is None and row['status'] == 'unavailable' for row in result['records'])
    with pytest.raises(ValueError, match='outside_contract'):
        history.project_pair_history(shared, session=session, news_capture=capture, instrument='USD_JPY', origins=origins)


def test_current_empty_population_is_distinct_from_unobserved(tmp_path):
    session, cfg = setup(tmp_path)
    # A pair with no relevant currency still has a proven complete current read.
    capture = io.capture_shared(session, clock=lambda: NOW)
    current = io.pair_features(capture, 'AUD_NZD', NOW)
    assert current['coverage_usable'] and current['features'][0] == 0
    assert io.pair_features(capture, 'AUD_NZD', NOW-1)['features'] is None


def test_prospective_price_news_capture_keeps_native_training_gates(tmp_path):
    from test_oanda_causal_forecast_inputs_joint_news_v1 import make_sources
    candles, _, _ = make_sources(tmp_path/'prices', now=NOW)
    session, _ = setup(tmp_path/'news')
    handle = io.capture_shared(session, clock=lambda: NOW)
    shared = history.prepare_universal_history_share(session, handle, ['EUR_USD'])
    capture = inputs.capture_inputs(candles, 'EUR_USD', pip_size=.0001, session=session,
        news_capture=handle, history_share=shared, clock=lambda: NOW)
    assert capture['status'] == 'ready', capture['reasons']
    assert capture['available_families'] == []
    assert not capture['news_frames']
    assert capture['history_diagnostics']['all_requested_origins_accounted']
    inputs.validate_capture(capture, session=session, news_capture=handle, history_share=shared)
    result = inputs.compute_predictions(capture, session=session, news_capture=handle, history_share=shared, clock=lambda: NOW)
    assert result['status'] == 'abstain' and not result['predictions']


def test_expired_hot_rows_removed_only_after_durable_archive(tmp_path, monkeypatch):
    session, cfg = setup(tmp_path)
    first = io.capture_shared(session, clock=lambda: NOW)
    old = io._capture(first)[0]['records'][0]
    archived = Path(cfg['archive_root'])/(io.digest(old)+'.json.gz')
    assert archived.is_file()
    monkeypatch.setattr(io, 'RETENTION', 1)
    io.capture_shared(session, clock=lambda: NOW+2)
    with sqlite3.connect(cfg['store_path']) as db:
        assert db.execute('SELECT COUNT(*) FROM points').fetchone()[0] == 1
    assert io._get(cfg['archive_root'], io.digest(old)) == old


def test_issue_health_does_not_extend_retained_feature_expiry(tmp_path):
    session, _ = setup(tmp_path)
    capture = io.capture_shared(session, clock=lambda: NOW)
    proof = io.reobserve_before_issue(session, capture, clock=lambda: NOW+1)
    assert proof['scope'] == 'current_health_only_retained_input_expiry_guards_still_required'
    assert io.capture_metadata(capture)['capture']['first_observed_epoch'] == NOW
    assert not io.pair_features(capture, 'EUR_USD', NOW+301)['coverage_usable']
