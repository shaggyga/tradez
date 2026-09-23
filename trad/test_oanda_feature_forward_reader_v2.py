"""Only owned fixtures; frozen reader/worker/protocol/ledgers remain unchanged."""
from copy import deepcopy
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time

import pytest

import oanda_feature_forward_reader_v2 as reader
import oanda_feature_forward_worker_v2 as worker
import oanda_feature_forward_protocol_v1 as protocol
import oanda_feature_forward_ledger_v1 as ledger
from oanda_feature_observations_v1 import archive_forward_frame
from test_oanda_feature_move_mapping_v1 import frame, START
from test_oanda_feature_forward_v1 import maps, quote, proof, NOW, PAIR


def write_frame(root, archive, index, *, publication_delay=1, source_schema="test-schema-v1"):
    value = frame(index, rsi=70 if index == 31 else None)
    value["source_schema_id"] = source_schema
    value["source_payload_sha256"] = hashlib.sha256(f"source-{index}".encode()).hexdigest()
    receipt_path = reader._receipt_path(archive, value)
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    archive_path = receipt_path.with_name(receipt_path.name.replace(".publication.json", ".gz"))
    receipt = dict(schema_version="feature_observation_publication_receipt_v1",
        snapshot_id=value["snapshot_id"], source_schema_id=value["source_schema_id"],
        payload_sha256=value["source_payload_sha256"], frame_sha256=reader._hash(reader.mapper._canonical(value)),
        source_read_completed_utc=value["generated_utc"],
        publication_completed_utc=(START+timedelta(minutes=index, seconds=publication_delay)).isoformat(),
        publication_scope="archive_visible_before_this_receipt",
        archive_relative_path=archive_path.relative_to(archive).as_posix(), archive_sha256="f"*64)
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    path = archive_forward_frame(value, root, archive_receipt=receipt)
    stamp = START.timestamp()+index*60+publication_delay
    os.utime(path, (stamp, stamp))
    return path, receipt_path


def fixture_cache(tmp_path, count=32):
    root, archive = tmp_path/"forward", tmp_path/"archive"
    for i in range(count):
        write_frame(root, archive, i)
    cache = reader.VerifiedFrameCache(root, archive)
    return cache, root, archive


def warm(cache, epoch, rounds=12):
    for _ in range(rounds):
        cache.refresh(as_of_epoch=epoch)


def mapping(cache, epoch):
    return cache(cache.root, as_of_utc=reader.mapper._iso(epoch))[300]


def test_twelve_frames_cannot_meet_five_history_but_verified_cache_can(tmp_path):
    cache, root, archive = fixture_cache(tmp_path)
    now = START.timestamp()+31*60+5
    warm(cache, now)
    result = mapping(cache, now)
    expected = reader.mapper.build_feature_move_map([frame(i, rsi=70 if i == 31 else None) for i in range(32)],
        as_of_utc=reader.mapper._iso(now), include_all_comparisons=True)
    by_name = {r["feature_name"]: r for r in result["all_feature_changes"]}
    expected_by_name = {r["feature_name"]: r for r in expected["all_feature_changes"]}
    for name in by_name:
        for key in ("before", "after", "change", "history_count", "unusual_percentile", "ranking_reason"):
            assert by_name[name][key] == expected_by_name[name][key]
    assert by_name["rsi14"]["history_count"] >= 5
    prepared = protocol.prepare_comparisons(result, window_sec=300, observed_epoch=now)
    assert prepared["category_counts"]["alert"] >= 1
    assert prepared["category_counts"]["control"] >= 1
    old = reader.mapper.build_feature_move_map([frame(i) for i in range(20, 32)],
        as_of_utc=reader.mapper._iso(now), include_all_comparisons=True)
    assert max(r["history_count"] for r in old["all_feature_changes"]) < 5


def test_bounded_bootstrap_and_unchanged_cache_need_no_redecode(tmp_path, monkeypatch):
    cache, _, _ = fixture_cache(tmp_path)
    now = START.timestamp()+31*60+5
    first = cache.refresh(as_of_epoch=now)
    assert first["files_read"] <= reader.MAX_NEW_FRAMES
    assert first["cache_frames"] <= reader.MAX_NEW_FRAMES
    warm(cache, now)
    def fail(*args, **kwargs):
        raise AssertionError("unchanged frame decoded again")
    monkeypatch.setattr(reader, "read_verified_frame", fail)
    last = cache.refresh(as_of_epoch=now)
    assert last["files_read"] == 0
    assert last["cache_frames"] == 32


def test_stale_endpoint_is_still_rejected_at_same_75_second_boundary(tmp_path):
    cache, _, _ = fixture_cache(tmp_path)
    now = START.timestamp()+31*60+5
    warm(cache, now)
    late = mapping(cache, now+71)
    assert late["status"] == "stale_observations"
    assert late["all_feature_changes"] == []
    assert protocol.PROTOCOL["minimum_history_count"] == 5
    assert protocol.PROTOCOL["source_max_age_sec"] == 75


def test_original_publication_after_cutoff_cannot_become_history(tmp_path):
    root, archive = tmp_path/"forward", tmp_path/"archive"
    path, _ = write_frame(root, archive, 0, publication_delay=100)
    with pytest.raises(ValueError, match="unavailable_at_cutoff"):
        reader.read_verified_frame(path, archive, as_of_epoch=START.timestamp()+60)
    cache = reader.VerifiedFrameCache(root, archive)
    result = cache.refresh(as_of_epoch=START.timestamp()+60)
    assert result["cache_frames"] == 0
    cache.refresh(as_of_epoch=START.timestamp()+101)
    assert len(cache.cache) == 1


def test_cached_revision_latches_refusal_without_replacing_old_evidence(tmp_path):
    cache, _, _ = fixture_cache(tmp_path, count=1)
    now = START.timestamp()+5
    cache.refresh(as_of_epoch=now)
    item = next(iter(cache.cache.values()))
    original = item["frame_sha256"]
    item["path"].write_bytes(b"changed")
    with pytest.raises(ValueError, match="cached_forward_frame_revised"):
        cache.refresh(as_of_epoch=now+1)
    with pytest.raises(ValueError, match="integrity_refused"):
        mapping(cache, now+2)
    assert item["frame_sha256"] == original


def test_external_receipt_and_partition_identity_are_required(tmp_path):
    root, archive = tmp_path/"forward", tmp_path/"archive"
    path, receipt_path = write_frame(root, archive, 0)
    value = json.loads(receipt_path.read_text())
    value["frame_sha256"] = "0"*64
    receipt_path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="external_publication_receipt_mismatch"):
        reader.read_verified_frame(path, archive, as_of_epoch=START.timestamp()+5)


def test_new_endpoint_preserves_original_values_and_reference_history(tmp_path):
    cache, root, archive = fixture_cache(tmp_path)
    now = START.timestamp()+31*60+5
    warm(cache, now)
    old_hashes = {v["frame"]["snapshot_id"]: v["frame_sha256"] for v in cache.cache.values()}
    write_frame(root, archive, 32)
    later = START.timestamp()+32*60+5
    report = cache.refresh(as_of_epoch=later)
    assert report["files_read"] == 1
    assert report["cache_frames"] == 32
    result = mapping(cache, later)
    assert result["latest_observed_utc"] == (START+timedelta(minutes=32)).isoformat()
    for item in cache.cache.values():
        identity = item["frame"]["snapshot_id"]
        if identity in old_hashes:
            assert item["frame_sha256"] == old_hashes[identity]


def test_cache_restart_reconstructs_same_values_without_backdating(tmp_path):
    cache, root, archive = fixture_cache(tmp_path)
    now = START.timestamp()+31*60+5
    warm(cache, now)
    before = mapping(cache, now)["all_feature_changes"]
    restarted = reader.VerifiedFrameCache(root, archive)
    assert mapping(restarted, now)["status"] == "waiting_for_observations"
    warm(restarted, now)
    assert mapping(restarted, now)["all_feature_changes"] == before


def identity():
    return dict(cohort_id=worker.COHORT, source_bindings=worker.source_pins(),
                protocol_sha256=protocol.PROTOCOL_SHA256)


def test_new_ledger_refuses_legacy_database_before_writing(tmp_path):
    path = tmp_path/"old.sqlite"
    old = ledger.ForwardLedger(path, clock=lambda: NOW)
    old.close()
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="old_or_foreign"):
        worker.ForwardLedgerV2(path, extension_identity=identity(), clock=lambda: NOW)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


def test_new_ledger_restart_settles_original_targets_and_quiet_controls(tmp_path):
    now = [NOW]
    path = tmp_path/"new.sqlite"
    owner = worker.ForwardLedgerV2(path, extension_identity=identity(), clock=lambda: now[0])
    owner.observe_quotes({PAIR:quote(NOW)}, read_started_epoch=NOW, read_completed_epoch=NOW,
                         source_receipt={}, force_reference=True)
    results = owner.publish_comparisons(maps())
    assert all(r["status"] == "published" for r in results)
    publications = dict(owner.db.execute("SELECT batch,completed FROM ff_publications"))
    owner.close()
    now[0] = NOW+1
    owner = worker.ForwardLedgerV2(path, extension_identity=identity(), clock=lambda: now[0])
    owner.observe_quotes({PAIR:quote(now[0])}, read_started_epoch=now[0],read_completed_epoch=now[0],source_receipt={})
    now[0] = NOW+301
    owner.observe_quotes({PAIR:quote(now[0],bid="1.1010",ask="1.1012")},read_started_epoch=now[0],read_completed_epoch=now[0],source_receipt={})
    result = owner.settle()
    assert result["scored"] == 3
    for _, body in owner.db.execute("SELECT job,body FROM ff_outcomes"):
        value = json.loads(body)
        assert value["target_epoch"] == publications[value["batch_id"]]+300
    totals = owner.db.execute("SELECT sum(alert),sum(control),sum(excluded) FROM ff_categories").fetchone()
    assert totals == (3,3,0)
    owner.close()


def test_wrong_extension_identity_refuses_new_cohort_restart(tmp_path):
    path = tmp_path/"new.sqlite"
    owner = worker.ForwardLedgerV2(path, extension_identity=identity(), clock=lambda: NOW)
    owner.close()
    changed = identity(); changed["cohort_id"] = "different"
    with pytest.raises(ValueError, match="old_or_foreign"):
        worker.ForwardLedgerV2(path, extension_identity=changed, clock=lambda: NOW)


def test_successor_worker_keeps_long_windows_deferred_and_settles_when_reader_fails(tmp_path):
    now = [NOW]
    owner = worker.ForwardLedgerV2(tmp_path/"worker.sqlite", extension_identity=identity(), clock=lambda: now[0])
    class Cache:
        last_report = {}
        failed = False
        def refresh(self, **kwargs):
            self.last_report = {"owned_fixture": True}
        def readiness(self, **kwargs):
            return {"ready":True,"reason":None}
        def __call__(self, *args, **kwargs):
            assert kwargs["window_secs"] == (300,)
            if self.failed:
                raise ValueError("owned_reader_failure")
            return {300: maps(now[0])[300]}
    cache = Cache()
    def quotes_reader(path, clock):
        at = clock()
        return {PAIR:quote(at)},at,at,{"owned_fixture":True}
    runner = worker.ForwardWorkerV2(owner, frame_cache=cache, clock_check=lambda:proof(now[0]),
        archive_root=tmp_path, forward_frame_root=tmp_path, quote_path=tmp_path/"owned.json",quote_reader=quotes_reader)
    first = runner.tick()
    assert first["deferred_comparison_windows_sec"] == [900,3600]
    assert owner.db.execute("SELECT count(*) FROM ff_jobs").fetchone() == (3,)
    for window, raw in owner.db.execute("SELECT window,body FROM ff_batches"):
        if window != 300:
            body = ledger.unpack(raw)
            assert body["comparisons"] == [] and body["pairs"] == []
    now[0] += 5
    runner.tick()
    now[0] = NOW+300
    cache.failed = True
    result = runner.tick()
    assert result["settlement"]["scored"] == 1
    outcome = json.loads(owner.db.execute("SELECT body FROM ff_outcomes").fetchone()[0])
    assert outcome["publication_epoch"] == NOW and outcome["target_epoch"] == NOW+300
    owner.close()


def test_readiness_waits_for_actual_history_and_processing_headroom(tmp_path):
    cache, _, _ = fixture_cache(tmp_path)
    now = START.timestamp()+31*60+5
    cache.refresh(as_of_epoch=now)
    assert cache.readiness(as_of_epoch=now)["reason"] == "insufficient_prior_clock_history"
    warm(cache, now)
    assert cache.readiness(as_of_epoch=now)["ready"] is True
    delayed = cache.readiness(as_of_epoch=now+36)
    assert delayed["ready"] is False
    assert delayed["reason"] == "awaiting_fresher_endpoint_for_processing_headroom"
    assert delayed["final_source_age_limit_seconds"] == 75


def test_waiting_endpoint_does_not_consume_bucket_or_stop_quotes(tmp_path):
    now = [NOW]
    owner = worker.ForwardLedgerV2(tmp_path/"waiting.sqlite", extension_identity=identity(), clock=lambda:now[0])
    class Cache:
        ready = False
        last_report = {}
        def refresh(self, **kwargs): pass
        def readiness(self, **kwargs):
            return {"ready":self.ready,"reason":None if self.ready else "awaiting_fresher_endpoint_for_processing_headroom"}
        def __call__(self, *args, **kwargs):
            return {300: maps(now[0])[300]}
    cache = Cache()
    def quotes_reader(path,clock):
        at = clock();return {PAIR:quote(at)},at,at,{"owned_fixture":True}
    runner = worker.ForwardWorkerV2(owner,frame_cache=cache,clock_check=lambda:proof(now[0]),
        archive_root=tmp_path,forward_frame_root=tmp_path,quote_path=tmp_path/"quotes.json",quote_reader=quotes_reader)
    first = runner.tick()
    assert first["quotes"]["observed_not_needed"] == 1
    assert not first["decision"]["attempted"]
    assert owner.db.execute("SELECT count(*) FROM ff_batches").fetchone() == (0,)
    assert owner.db.execute("SELECT count(*) FROM ff_refusals").fetchone() == (1,)
    now[0] += 10;cache.ready = True
    second = runner.tick()
    assert second["decision"]["attempted"] is True
    assert all(r["status"]=="published" for r in second["publication"])
    assert owner.db.execute("SELECT count(*) FROM ff_jobs").fetchone() == (3,)
    owner.close()


def test_invalid_initial_clock_has_no_ledger_clock_or_refusal_effect(tmp_path):
    now = [NOW]
    owner = worker.ForwardLedgerV2(tmp_path/"invalid-clock.sqlite", extension_identity=identity(), clock=lambda:now[0])
    class Cache:
        last_report = {}
        def refresh(self, **kwargs):
            raise AssertionError("clock refusal must precede cache access")
    runner = worker.ForwardWorkerV2(owner,frame_cache=Cache(),clock_check=lambda:{"valid":False,"reason":"fixture"},
        archive_root=tmp_path,forward_frame_root=tmp_path,quote_path=tmp_path/"quotes.json")
    before_clock = owner._last_clock
    before_changes = owner.db.total_changes
    now[0] += 7200
    report = runner.tick()
    assert report["status"] == "verified_clock_unavailable"
    assert owner._last_clock == before_clock
    assert owner.db.total_changes == before_changes
    assert owner.db.execute("SELECT count(*) FROM ff_refusals").fetchone() == (0,)
    assert runner.last_decision_bucket is None
    owner.close()


def test_partial_quote_refusals_are_nonfatal_when_other_quotes_are_usable(tmp_path):
    now = [NOW]
    owner = worker.ForwardLedgerV2(tmp_path/"quote-refusal.sqlite", extension_identity=identity(), clock=lambda:now[0])
    class Cache:
        ready = False
        last_report = {}
        def refresh(self, **kwargs): pass
        def readiness(self, **kwargs):
            return {"ready":False,"reason":"awaiting_fresher_endpoint_for_processing_headroom"}
        def __call__(self, *args, **kwargs):
            raise AssertionError("not due while cache waiting")
    def quotes_reader(path, clock):
        at = clock()
        return {PAIR:quote(at), "USD_TRY":quote(at-3600)}, at, at, {"owned_fixture":True}
    runner = worker.ForwardWorkerV2(owner, frame_cache=Cache(), clock_check=lambda:proof(now[0]),
        archive_root=tmp_path, forward_frame_root=tmp_path, quote_path=tmp_path/"quotes.json", quote_reader=quotes_reader)
    report = runner.tick()
    assert report["quotes"]["refused"] == 1
    assert report["quote_refusals"]["reason"] == "one_or_more_quotes_refused"
    assert report["quote_refusals"]["nonfatal_when_other_quotes_available"] is True
    assert report["errors"] == []
    assert report["status"] != "partial_unavailable"
    owner.close()
