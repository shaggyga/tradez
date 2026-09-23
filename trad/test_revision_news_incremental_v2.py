"""New caller generation over the original synthetic named consumer stores."""
import copy
import hashlib
import json
from pathlib import Path

import pytest

import projection_revision_consumer_v2 as incremental
import revision_news_io_v11 as io
import revision_transport_v5 as transport
from test_projection_revision_consumer_v2 import owned, fixture


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


def test_transport_and_joint_share_exact_reader_generation_with_original_clocks(owned, tmp_path):
    config = configurations(owned, tmp_path)
    runner = transport.open_runner(config["transport_config_path"], config["transport_config_sha256"])
    session = None
    try:
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


def test_io_refuses_mixed_reader_profiles_or_shared_process_cache(owned, tmp_path):
    config = configurations(owned, tmp_path)
    mixed = copy.deepcopy(config)
    mixed["reader_profile_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="io_transport_shared_reader_profile_required"):
        io.create_session(mixed)
    same = copy.deepcopy(config)
    same["reader_cache_path"] = str(tmp_path / "transport-cache.sqlite")
    with pytest.raises(ValueError, match="separate_process_validation_caches_required"):
        io.create_session(same)
