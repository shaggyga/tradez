"""Original real receipt fixtures; no live input, process or config changes."""
import copy
import importlib.util
from pathlib import Path
import sqlite3
import time
from types import SimpleNamespace
from unittest import mock

import pytest

import projection_revision_consumer_v2 as incremental


FIXTURE_PATH = Path(__file__).parent / "docs/validation/native_news_compact_20260914/fixtures/test_core_operational_v2.py"
spec = importlib.util.spec_from_file_location("incremental_consumer_owned_fixture", FIXTURE_PATH)
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


@pytest.fixture
def owned(tmp_path):
    source, config, runner = fixture.make(tmp_path)
    for ordinal in range(2):
        fixture.add_article(source, ordinal, ordinal + .1)
        result = fixture.transport.run_cycle(runner, clock_provider=fixture.proof_clock(ordinal + .5))
        assert result["status"] == "ready", result
    kwargs = {key: config[key] for key in ("cohort_id", "consumer_id", "input_identity")}
    kwargs["expected_policy"] = config["policy"]
    readers = []
    def create(**changes):
        value = incremental.IncrementalReader(config["publication_path"], config["observation_path"],
            cache_path=tmp_path / ("cache" + str(len(readers)) + ".sqlite"), **kwargs, **changes)
        readers.append(value)
        return value
    yield SimpleNamespace(source=source, config=config, runner=runner, kwargs=kwargs, create=create, readers=readers)
    for reader in readers:
        reader.close()
    fixture.transport.close_runner(runner)


def complete(reader):
    for _ in range(100):
        progress = reader.bootstrap_step()
        assert progress["current_context_available"] is False
        if progress["status"] == "prefix_verified_requires_fresh_read":
            return progress
    raise AssertionError("bounded fixture failed to complete")


def mutate_preserving_schema(path, table, action, parameters=(), trigger_action="UPDATE"):
    """Corrupt only an isolated test database, then restore exact trigger SQL."""
    with sqlite3.connect(path) as con:
        name = table + "_no_" + trigger_action.lower()
        sql = con.execute("SELECT sql FROM sqlite_master WHERE name=?", (name,)).fetchone()[0]
        con.execute("DROP TRIGGER " + name)
        con.execute(action, parameters)
        con.execute(sql)


def test_step_limit_requires_following_fresh_read_and_matches_v1_exactly(owned):
    old = incremental.legacy.read_observations(owned.config["publication_path"], owned.config["observation_path"], **owned.kwargs)
    new = owned.create(max_step_receipts=1)
    with pytest.raises(incremental.BootstrapPending) as pending:
        new.read_observations()
    assert pending.value.progress["validated_observations"] == 1
    assert pending.value.progress["status"] == "bootstrapping"
    progress = new.bootstrap_step()
    assert progress["validated_observations"] == 2
    assert progress["status"] == "prefix_verified_requires_fresh_read"
    before = new.capture_count
    context = new.read_observations()
    assert new.capture_count == before + 1
    assert type(context) is incremental.legacy.PreparedConsumer
    for minute in (0, .49, .51, 1.49, 1.51, 8):
        at = fixture.epoch(minute)
        assert incremental.latest_consumed_from_context(context, at) == incremental.legacy.latest_consumed_from_context(old, at)
        assert incremental.selected_timing(context, at) == incremental.legacy.selected_timing(old, at)
        assert list(incremental.iter_selected_headers(context, at)) == list(incremental.legacy.iter_selected_headers(old, at))
    for header in incremental.iter_selected_headers(context, fixture.epoch(1.51)):
        assert incremental.resolve_selected_member(context, header, fixture.epoch(1.51)) == incremental.legacy.resolve_selected_member(old, header, fixture.epoch(1.51))
    assert incremental.export_recipe(context) == incremental.legacy.export_recipe(old)
    meta = incremental.prepared_context_metadata(context)
    oldmeta = incremental.legacy.prepared_context_metadata(old)
    assert {key: value for key, value in meta.items() if key != "incremental_validation"} == oldmeta
    assert meta["incremental_validation"]["step_expanded_bytes"] == 0


def test_warm_reads_skip_old_semantics_and_new_receipt_is_validated_once(owned):
    new = owned.create()
    complete(new)
    with mock.patch.object(incremental.legacy, "_validate_observation", wraps=incremental.legacy._validate_observation) as validate:
        new.read_observations()
        assert validate.call_count == 0
        fixture.add_article(owned.source, 2, 2.1)
        result = fixture.transport.run_cycle(owned.runner, clock_provider=fixture.proof_clock(2.5))
        assert result["status"] == "ready", result
        validate.reset_mock()  # The frozen writer independently validates its new receipt.
        context = new.read_observations()
        assert validate.call_count == 1
        assert incremental.prepared_context_metadata(context)["observation_count"] == 3
        validate.reset_mock()
        new.read_observations()
        assert validate.call_count == 0


def test_cumulative_historical_work_is_not_a_per_step_work_budget(owned, monkeypatch):
    new = owned.create()
    # Scale the per-step work budget to these small fully real fixture scans.
    monkeypatch.setattr(new, "max_step_bytes", 150_000)
    monkeypatch.setattr(incremental.legacy, "MAX_EXPANDED_SCAN_BYTES", 150_000)
    with pytest.raises(ValueError, match="expanded_scan_work_bound"):
        incremental.legacy.read_observations(owned.config["publication_path"], owned.config["observation_path"], **owned.kwargs)
    first = new.bootstrap_step()
    assert first["validated_observations"] == 1
    assert 0 < first["step_expanded_bytes"] <= 150_000
    second = new.bootstrap_step()
    assert second["historical_expanded_bytes"] > 150_000
    assert 0 < second["step_expanded_bytes"] <= 150_000
    context = new.read_observations()
    assert incremental.prepared_context_metadata(context)["expanded_scan_bytes_checked"] == second["historical_expanded_bytes"]


@pytest.mark.parametrize("table,sql", [
    ("observations", "UPDATE observations SET body=body||' ' WHERE observation_sequence=1"),
    ("acknowledgments", "UPDATE acknowledgments SET body=body||' ' WHERE observation_id=(SELECT observation_id FROM observations WHERE observation_sequence=1)"),
])
def test_any_old_packed_row_change_withholds_current_context(owned, table, sql):
    new = owned.create()
    complete(new)
    mutate_preserving_schema(owned.config["observation_path"], table, sql)
    with pytest.raises(ValueError, match="verified_consumer_inventory_changed"):
        new.read_observations()
    assert new.failed and not new.complete


def test_ack_deletion_and_unacknowledged_row_deletion_are_not_hidden(owned):
    path = owned.config["observation_path"]
    with sqlite3.connect(path) as con:
        oid = con.execute("SELECT observation_id FROM observations WHERE observation_sequence=1").fetchone()[0]
    mutate_preserving_schema(path, "acknowledgments", "DELETE FROM acknowledgments WHERE observation_id=?", (oid,), "DELETE")
    new = owned.create()
    complete(new)
    mutate_preserving_schema(path, "observations", "DELETE FROM observations WHERE observation_sequence=1", trigger_action="DELETE")
    with pytest.raises(ValueError, match="verified_consumer_inventory_changed:observations"):
        new.read_observations()


def test_late_old_acknowledgment_restarts_original_time_order_without_authorizing(owned):
    path = owned.config["observation_path"]
    with sqlite3.connect(path) as con:
        oid = con.execute("SELECT observation_id FROM observations WHERE observation_sequence=1").fetchone()[0]
        saved = con.execute("SELECT * FROM acknowledgments WHERE observation_id=?", (oid,)).fetchone()
    mutate_preserving_schema(path, "acknowledgments", "DELETE FROM acknowledgments WHERE observation_id=?", (oid,), "DELETE")
    new = owned.create(max_step_receipts=1)
    complete(new)
    before = new.read_observations()
    assert incremental.prepared_context_metadata(before)["observation_count"] == 1
    with sqlite3.connect(path) as con:
        con.execute("INSERT INTO acknowledgments VALUES(?,?,?)", saved)
    with pytest.raises(incremental.BootstrapPending) as pending:
        new.read_observations()
    assert pending.value.progress["validated_observations"] == 1
    complete(new)
    actual = new.read_observations()
    expected = incremental.legacy.read_observations(owned.config["publication_path"], path, **owned.kwargs)
    for minute in (.51, 1.51):
        assert incremental.latest_consumed_from_context(actual, fixture.epoch(minute)) == incremental.legacy.latest_consumed_from_context(expected, fixture.epoch(minute))


def test_scan_object_inventory_is_checked_even_when_not_referenced(owned):
    path = owned.config["observation_path"]
    frame = incremental.compact.value_frame({"unused_original_fixture": 1})
    with sqlite3.connect(path) as con:
        con.execute("INSERT INTO scan_objects VALUES(?,?)", (frame[1], incremental.legacy.encode_frame(frame).decode()))
    new = owned.create()
    complete(new)
    new.read_observations()
    mutate_preserving_schema(path, "scan_objects", "DELETE FROM scan_objects", trigger_action="DELETE")
    with pytest.raises(ValueError, match="verified_consumer_inventory_changed:scan_objects"):
        new.read_observations()


def test_invalid_new_suffix_never_falls_back_to_previous_current_context(owned):
    new = owned.create()
    complete(new)
    new.read_observations()
    with sqlite3.connect(owned.config["observation_path"]) as con:
        con.execute("INSERT INTO observations VALUES(?,?,?,?)", (3, "bad", "0" * 64, "{}"))
        con.execute("INSERT INTO acknowledgments VALUES(?,?,?)", ("bad", "0" * 64, "{}"))
    for _ in range(2):
        with pytest.raises(ValueError, match="consumer_stored_digest_invalid"):
            new.read_observations()
        assert new.failed and not new.complete


def test_disk_cache_restart_revalidates_originals_and_ignores_claimed_complete_progress(owned):
    first = owned.create(max_step_receipts=1)
    complete(first)
    first.read_observations()
    cache_path = first.cache_path
    first.close()
    owned.readers.remove(first)
    with sqlite3.connect(cache_path) as con:
        con.execute("UPDATE progress SET body=?", (b'{"status":"complete","validated_observations":999999}',))
    resumed = incremental.IncrementalReader(owned.config["publication_path"], owned.config["observation_path"],
                cache_path=cache_path, max_step_receipts=1, **owned.kwargs)
    owned.readers.append(resumed)
    with mock.patch.object(incremental.legacy, "_validate_observation", wraps=incremental.legacy._validate_observation) as validate:
        with pytest.raises(incremental.BootstrapPending) as pending:
            resumed.read_observations()
        assert validate.call_count == 1
        assert pending.value.progress["validated_observations"] == 1
    assert resumed.cache_path.stat().st_size < incremental.MAX_CACHE_BYTES


def test_source_identity_and_shared_operations_profile_are_pinned(owned):
    new = owned.create()
    complete(new)
    wrong = copy.deepcopy(new.profile)
    wrong["consumer_id"] += "_other"
    with pytest.raises(ValueError, match="exact_incremental_reader_profile_required"):
        owned.create(operations_profile=wrong)
    with mock.patch.object(incremental.publisher, "identity", return_value=(0, 0)):
        with pytest.raises(ValueError, match="incremental_reader_generation_changed"):
            new.read_observations()


def test_durable_inventory_deletion_is_detected_before_restarting_validation(owned):
    first = owned.create()
    complete(first)
    path = first.cache_path
    first.close()
    owned.readers.remove(first)
    with sqlite3.connect(path) as con:
        con.execute("DELETE FROM inventory WHERE rowid=(SELECT MIN(rowid) FROM inventory)")
    with pytest.raises(ValueError, match="validation_cache_inventory_corrupt"):
        incremental.IncrementalReader(owned.config["publication_path"], owned.config["observation_path"],
                                     cache_path=path, **owned.kwargs)


def test_cooperative_time_boundary_retains_progress_but_never_authorizes(owned, monkeypatch):
    new = owned.create()
    real_time = time.monotonic()
    clock = [real_time]
    original = incremental.legacy._validate_observation
    def finish_after_boundary(*args):
        result = original(*args)
        clock[0] = real_time + 21
        return result
    monkeypatch.setattr(incremental, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    with mock.patch.object(incremental.legacy, "_validate_observation", side_effect=finish_after_boundary):
        progress = new.bootstrap_step()
    assert progress["status"] == "bootstrapping"
    assert progress["validated_observations"] == 1
    assert progress["current_context_available"] is False


def test_configuration_and_cache_disk_bounds_are_explicit(owned):
    with pytest.raises(ValueError, match="bounded_step_expansion_required"):
        owned.create(max_step_bytes=1)
    with pytest.raises(ValueError, match="bounded_step_receipt_count_required"):
        owned.create(max_step_receipts=65)
    with pytest.raises(ValueError, match="bounded_step_seconds_required"):
        owned.create(max_step_seconds=31)
    with pytest.raises(ValueError, match="separate_validation_cache_required"):
        incremental.IncrementalReader(owned.config["publication_path"], owned.config["observation_path"],
                                     cache_path=owned.config["observation_path"], **owned.kwargs)


def test_original_databases_are_read_only_during_all_incremental_steps(owned):
    paths = [Path(owned.config[key]) for key in ("publication_path", "observation_path")]
    before = [path.read_bytes() for path in paths]
    new = owned.create(max_step_receipts=1)
    complete(new)
    new.read_observations()
    assert [path.read_bytes() for path in paths] == before


def test_archived_manifest_streams_original_history_and_preserves_exact_results(owned, monkeypatch):
    old = incremental.legacy.read_observations(owned.config["publication_path"], owned.config["observation_path"], **owned.kwargs)
    manifest = incremental.legacy.export_manifest(old)
    replay = incremental.ManifestReplay(manifest, incremental.legacy.iter_recipe_objects(old), max_step_receipts=1)
    monkeypatch.setattr(incremental.legacy, "MAX_EXPANDED_SCAN_BYTES", 150_000)
    first = replay.step()
    assert first["validated_observations"] == 1 and first["current_context_available"] is False
    with pytest.raises(ValueError, match="complete_archived_replay_required"):
        replay.prepared_context()
    second = replay.step()
    assert second["historical_expanded_bytes"] > 150_000
    assert second["actual_database_capture_performed"] is False
    restored = replay.prepared_context()
    assert incremental.export_manifest(restored) == manifest
    for minute in (.51, 1.51, 8):
        assert incremental.latest_consumed_from_context(restored, fixture.epoch(minute)) == incremental.legacy.latest_consumed_from_context(old, fixture.epoch(minute))


def test_archive_new_invalid_receipt_never_returns_partial_context(owned):
    old = incremental.legacy.read_observations(owned.config["publication_path"], owned.config["observation_path"], **owned.kwargs)
    manifest = incremental.legacy.export_manifest(old)
    manifest["consumer"]["observations"][-1]["receipt_sha256"] = "0" * 64
    replay = incremental.ManifestReplay(manifest, incremental.legacy.iter_recipe_objects(old), max_step_receipts=1)
    replay.step()
    with pytest.raises(ValueError, match="consumer_supplied_digest_invalid"):
        replay.step()
    with pytest.raises(ValueError, match="complete_archived_replay_required"):
        replay.prepared_context()
