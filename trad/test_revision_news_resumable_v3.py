"""New caller generation over the original synthetic named consumer stores."""
import copy
import ast
import hashlib
import inspect
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

import projection_revision_consumer_v2 as incremental
import revision_news_io_v12 as io
import revision_transport_v6 as transport
import revision_news_io_v11 as previous_io
from test_projection_revision_consumer_v2 import owned, fixture, mutate_preserving_schema


def configurations(owned, root):
    profile = incremental.operations_profile_for(owned.config["publication_path"], owned.config["observation_path"], **owned.kwargs)
    profile_path = root / "reader-profile.json"
    profile_path.write_bytes(io.encode(profile))
    archive = root / "archive-v11"
    archive.mkdir()
    state = root / "state-v5"
    state.mkdir()
    base = {key: value for key, value in owned.config.items() if key not in ("transport_config_path", "transport_config_sha256")}
    base.update(schema_version=transport._owners().CONFIG, archive_root=str(archive))
    base_path = root / "base-v5.json"
    base_path.write_bytes(io.encode(base))
    shared = {"reader_profile_path": str(profile_path), "reader_profile_sha256": hashlib.sha256(profile_path.read_bytes()).hexdigest()}
    transport_config = {"schema_version": transport.CONFIG, "news_io_config_path": str(base_path),
        "news_io_config_sha256": hashlib.sha256(base_path.read_bytes()).hexdigest(), "state_root": str(state),
        "interval_sec": 60, "duration_sec": 600, **shared, "reader_cache_path": str(root / "transport-cache.sqlite")}
    transport_path = root / "transport-v5.json"
    transport_path.write_bytes(io.encode(transport_config))
    config = {**base, "schema_version": io.CONFIG, "transport_config_path": str(transport_path),
        "transport_config_sha256": hashlib.sha256(transport_path.read_bytes()).hexdigest(), **shared,
        "reader_cache_path": str(root / "joint-cache.sqlite")}
    return config


def test_resumable_transport_and_joint_preserve_original_features_and_clocks(owned, tmp_path):
    config = configurations(owned, tmp_path)
    runner = transport.open_runner(config["transport_config_path"], config["transport_config_sha256"])
    session = None
    try:
        result = transport.run_cycle(runner, clock_provider=fixture.proof_clock(2.5))
        for _ in range(20):
            if result["status"] != "starting":
                break
            result = transport.run_cycle(runner, clock_provider=fixture.proof_clock(2.5))
        assert result["status"] == "ready", result
        assert result["bootstrap_progress"]["validated_observations"] == 2
        assert result["bootstrap_progress"]["current_context_available"] is False
        fixture.health(config, 2.6)
        session = io.create_session(config)
        progress = io.bootstrap_inputs(session, clock=lambda: fixture.epoch(2.61))
        assert progress["capture_handle_returned"] is False
        assert progress["observation_count"] == 3
        capture = io.capture_shared(session, clock=lambda: fixture.epoch(2.62))
        current = io.current_pair_features(session, capture, "EUR_USD", fixture.epoch(2.63))
        old = incremental.legacy.read_observations(config["publication_path"], config["observation_path"], **owned.kwargs)
        expected = io.adapter.pair_features_as_of(io.adapter.prepare_revision_context(old), "EUR_USD", fixture.epoch(2.63))
        assert current == expected
        descriptor = io.capture_metadata(capture)["descriptor"]
        replayed = io.replay_capture(session, descriptor)
        assert io.capture_metadata(replayed)["scope"] == "immutable_replay"
        assert io.capture_metadata(replayed)["capture"] == io.capture_metadata(capture)["capture"]
        with pytest.raises(ValueError, match="io_capture_invalidated_or_replay_only"):
            io.current_pair_features(session, replayed, "EUR_USD", fixture.epoch(2.63))
        assert io.source_graph()["projection_revision_consumer_v2.py"] == hashlib.sha256(Path(incremental.__file__).read_bytes()).hexdigest()
        assert config["reader_cache_path"] != transport._state(runner).config["reader_cache_path"]
    finally:
        if session is not None:
            io.close_session(session)
        transport.close_runner(runner)


def test_resumable_io_refuses_mixed_profile_and_cache(owned, tmp_path):
    config = configurations(owned, tmp_path)
    mixed = copy.deepcopy(config)
    mixed["reader_profile_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="io_transport_shared_reader_profile_required"):
        io.create_session(mixed)
    same = copy.deepcopy(config)
    same["reader_cache_path"] = str(tmp_path / "transport-cache.sqlite")
    with pytest.raises(ValueError, match="separate_process_validation_caches_required"):
        io.create_session(same)


def _phase_clock(owned, config, monkeypatch):
    """Keep actual accepted contexts; simulate only nonauthorizing work time."""
    context = incremental.legacy.read_observations(config["publication_path"], config["observation_path"], **owned.kwargs)
    elapsed = [0.0]
    durations = {"validation": 0.0, "archive": 0.0}
    real_store = io._store_capture_archive

    def store(*args, **kwargs):
        result = real_store(*args, **kwargs)
        elapsed[0] += durations["archive"]
        return result

    class ValidatedReader:
        calls = 0
        def bootstrap_step(self):
            self.calls += 1
            elapsed[0] += durations["validation"]
            return {"status": "prefix_verified_requires_fresh_read", "current_context_available": False}
        def read_observations(self):
            return context

    monkeypatch.setattr(io, "time", SimpleNamespace(monotonic=lambda: elapsed[0]))
    monkeypatch.setattr(io, "_store_capture_archive", store)
    return elapsed, durations, ValidatedReader(), lambda: fixture.epoch(3) + elapsed[0]


def test_prefix_and_archive_have_separate_non_authorizing_phase_budgets(owned, tmp_path, monkeypatch):
    config = configurations(owned, tmp_path)
    session = io.create_session(config)
    state = io._session(session)
    original_reader = state["validation_reader"]
    elapsed, durations, fake_reader, clock = _phase_clock(owned, config, monkeypatch)
    durations.update(validation=325.0, archive=325.0)
    state["validation_reader"] = fake_reader
    try:
        result = io.bootstrap_inputs(session, clock=clock)
        assert result["elapsed_sec"] == 650.0
        assert result["validation_phase_seconds"] == result["archive_phase_seconds"] == 325.0
        assert result["maximum_phase_seconds"] == 330
        assert result["maximum_total_seconds"] == 660
        assert result["capture_handle_returned"] is False
        assert result["fresh_health_proven"] is False
        assert state["usable"] is False
        assert state["bootstrap_validated_context"] is not None
    finally:
        state["validation_reader"] = original_reader
        io.close_session(session)


def test_archive_overrun_keeps_prefix_but_retry_rechecks_original_inventory(owned, tmp_path, monkeypatch):
    config = configurations(owned, tmp_path)
    session = io.create_session(config)
    state = io._session(session)
    original_reader = state["validation_reader"]
    elapsed, durations, fake_reader, clock = _phase_clock(owned, config, monkeypatch)
    durations["archive"] = 331.0
    state["validation_reader"] = fake_reader
    try:
        with pytest.raises(ValueError, match="io_bootstrap_archive_duration_bound"):
            io.bootstrap_inputs(session, clock=clock)
        assert state["usable"] is False
        assert state["bootstrap_validated_context"] is not None
        prior_calls = fake_reader.calls
        durations["archive"] = 0.0
        result = io.bootstrap_inputs(session, clock=clock)
        assert fake_reader.calls == prior_calls + 1
        assert result["retained_prefix_before_retry"] is True
        assert result["capture_handle_returned"] is False
    finally:
        state["validation_reader"] = original_reader
        io.close_session(session)


def test_validation_phase_overrun_does_not_begin_archive(owned, tmp_path, monkeypatch):
    config = configurations(owned, tmp_path)
    session = io.create_session(config)
    state = io._session(session)
    original_reader = state["validation_reader"]
    elapsed, durations, fake_reader, clock = _phase_clock(owned, config, monkeypatch)
    durations["validation"] = 331.0
    state["validation_reader"] = fake_reader
    monkeypatch.setattr(io, "_store_capture_archive", lambda *args, **kwargs: pytest.fail("archive before valid prefix"))
    try:
        with pytest.raises(ValueError, match="io_bootstrap_validation_duration_bound"):
            io.bootstrap_inputs(session, clock=clock)
        assert state["usable"] is False
        assert state["bootstrap_validated_context"] is None
    finally:
        state["validation_reader"] = original_reader
        io.close_session(session)


@pytest.mark.parametrize("failure", ["changed_prefix", "invalid_suffix", "source_graph"])
def test_archive_retry_rejects_semantic_or_source_failure(owned, tmp_path, monkeypatch, failure):
    config = configurations(owned, tmp_path)
    session = io.create_session(config)
    state = io._session(session)
    elapsed = [0.0]
    real_store = io._store_capture_archive
    def slow_store(*args, **kwargs):
        result = real_store(*args, **kwargs)
        elapsed[0] += 331.0
        return result
    monkeypatch.setattr(io, "time", SimpleNamespace(monotonic=lambda: elapsed[0]))
    monkeypatch.setattr(io, "_store_capture_archive", slow_store)
    try:
        with pytest.raises(ValueError, match="io_bootstrap_archive_duration_bound"):
            io.bootstrap_inputs(session, clock=lambda: fixture.epoch(3) + elapsed[0])
        assert state["bootstrap_validated_context"] is not None
        if failure == "changed_prefix":
            mutate_preserving_schema(config["observation_path"], "acknowledgments",
                "UPDATE acknowledgments SET body=body || ' ' WHERE rowid=(SELECT MIN(rowid) FROM acknowledgments)")
            reason = "verified_consumer_inventory_changed"
        elif failure == "invalid_suffix":
            with sqlite3.connect(config["observation_path"]) as connection:
                connection.execute("INSERT INTO observations VALUES(?,?,?,?)", (3, "bad", "0" * 64, "{}"))
                connection.execute("INSERT INTO acknowledgments VALUES(?,?,?)", ("bad", "0" * 64, "{}"))
            reason = "consumer_stored_digest_invalid"
        else:
            def changed_graph():
                raise ValueError("io_bound_module_source_changed")
            monkeypatch.setattr(io, "source_graph", changed_graph)
            reason = "io_bound_module_source_changed"
        with pytest.raises(ValueError, match=reason):
            io.bootstrap_inputs(session, clock=lambda: fixture.epoch(3) + elapsed[0])
        assert state["usable"] is False
        assert state["bootstrap_validated_context"] is None
    finally:
        io.close_session(session)


def test_current_capture_and_replay_guards_remain_exactly_unchanged():
    assert io.MAX_SECONDS == previous_io.MAX_SECONDS == 30
    for name in ("capture_shared", "_validate_capture_value", "_validate_health_files", "current_pair_features", "replay_capture"):
        assert ast.dump(ast.parse(inspect.getsource(getattr(io, name)))) == ast.dump(ast.parse(inspect.getsource(getattr(previous_io, name))))
    assert io.TRANSPORT_SOURCES["revision_transport_v6.py"] == hashlib.sha256(Path(transport.__file__).read_bytes()).hexdigest()
