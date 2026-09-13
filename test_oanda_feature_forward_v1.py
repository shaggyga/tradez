"""Owned tiny fixtures only; no archive, model, network or original ledger."""
import copy
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import zlib

try:
    import oanda_feature_forward_protocol_v1 as p
    import oanda_feature_forward_ledger_v1 as l
    import oanda_feature_forward_worker_v1 as w
except ModuleNotFoundError:
    from trad import oanda_feature_forward_protocol_v1 as p
    from trad import oanda_feature_forward_ledger_v1 as l
    from trad import oanda_feature_forward_worker_v1 as w

NOW = 1800000000.0
PAIR = "EUR_USD"


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def row(**updates):
    result = dict(instrument=PAIR, feature_id="fixed:rsi", feature_name="rsi", feature_kind="indicator",
                  status="available", reason=None, before=40., after=50., change=10., units="points",
                  history_count=8, unusual_percentile=97, ranking_reason=None)
    result.update(updates)
    return result


def maps(now=NOW, rows=None):
    values = [row(), row(feature_id="fixed:quiet", change=0., before=2., after=2., unusual_percentile=50)] if rows is None else rows
    result = {}
    for window in p.PROTOCOL["comparison_windows_sec"]:
        result[window] = {"schema_version": "feature_move_mapping_v1", "window_sec": window,
                          "instrument_filter": None, "generated_utc": iso(now), "latest_observed_utc": iso(now),
                          "status": "available", "summary": {"total_feature_comparisons": len(values)},
                          "pairs": [{"instrument": PAIR, "status": "available"}],
                          "feature_changes": copy.deepcopy(values[:1]), "all_feature_changes": copy.deepcopy(values),
                          "source_snapshots": [{"snapshot_id": str(now), "generated_utc": iso(now),
                                                "source_schema_id": "fixed", "source_payload_sha256": "a"*64}]}
    return result


def quote(now, bid="1.1000", ask="1.1002", **updates):
    result = {"time": iso(now), "bid": bid, "ask": ask, "tradeable": True, "source": "owned_fixture"}
    result.update(updates)
    return result


def proof(now):
    payload = {"schema_version": 1, "generated_utc": iso(now), "status": "ok", "timestamp_normalization_trusted": True,
               "host_clock_synchronized": True, "clock_discontinuity_active": False, "clock_sources_consistent": True,
               "source_fresh": True, "source_age_sec": 0, "broker_clock_lead_sec": 0.2,
               "broker_clock_sample_count": 32, "external_https_clock": {}}
    return w.clock_gate.validate_clock_state(payload, now_epoch=now)


class Pure(unittest.TestCase):
    def test_fixed_threshold_and_zero_control(self):
        self.assertEqual(p.classify(row(unusual_percentile=95))[0], "alert")
        self.assertEqual(p.classify(row(unusual_percentile=94.999))[0], "control")
        self.assertEqual(p.classify(row(change=0., unusual_percentile=100))[0], "control")

    def test_no_feature_sign_direction(self):
        self.assertEqual(p.classify(row(change=-10.))[0], "alert")
        self.assertIsNone(p.score_pair(quote(NOW), quote(NOW+1), quote(NOW+300))["inferred_feature_direction"])

    def test_exclusion_categories(self):
        for change in ({"history_count": 4}, {"history_count": True}, {"before": None}, {"before": float("nan")},
                       {"feature_kind": "model_output"}, {"feature_kind": "metadata"}, {"status": "unavailable"},
                       {"unusual_percentile": None}, {"unusual_percentile": True}):
            with self.subTest(change=change):
                self.assertEqual(p.classify(row(**change))[0], "excluded")

    def test_complete_not_top50(self):
        data = maps(rows=[row(feature_id=str(i)) for i in range(70)])[300]
        self.assertEqual(len(p.prepare_comparisons(data, window_sec=300, observed_epoch=NOW)["comparisons"]), 70)
        del data["all_feature_changes"]
        self.assertRaises(ValueError, p.prepare_comparisons, data, window_sec=300, observed_epoch=NOW)

    def test_count_alias_filter_and_duplicate_refused(self):
        for mutation in (lambda v: v["summary"].update(total_feature_comparisons=True),
                         lambda v: v.update(instrument_filter=PAIR),
                         lambda v: v["all_feature_changes"].__setitem__(1, copy.deepcopy(v["all_feature_changes"][0]))):
            data = maps()[300]
            mutation(data)
            self.assertRaises(ValueError, p.prepare_comparisons, data, window_sec=300, observed_epoch=NOW)

    def test_stale_source_excluded_not_republished_fresh(self):
        data = maps(NOW-76)[300]
        result = p.prepare_comparisons(data, window_sec=300, observed_epoch=NOW)
        self.assertTrue(all(r["forward_category"] == "excluded" for r in result["comparisons"]))

    def test_future_source_refused(self):
        self.assertRaises(ValueError, p.prepare_comparisons, maps(NOW+1)[300], window_sec=300, observed_epoch=NOW)

    def test_genuine_empty_mapper_retained_and_mismatch_refused(self):
        value = w.mapper.build_feature_move_map([], as_of_utc=iso(NOW), include_all_comparisons=True)
        result = p.prepare_comparisons(value, window_sec=300, observed_epoch=NOW)
        self.assertEqual(result["comparisons"], [])
        value["summary"]["total_feature_comparisons"] = 1
        self.assertRaises(ValueError, p.prepare_comparisons, value, window_sec=300, observed_epoch=NOW)

    def test_exact_costs_and_no_dummy_probability(self):
        score = p.score_pair(quote(NOW), quote(NOW+1, "1.1001", "1.1003"), quote(NOW+300, "1.1010", "1.1012"))
        self.assertEqual(score["probes"]["fixed_long"]["net_price_move"], "0.0007")
        self.assertEqual(score["probes"]["fixed_short"]["net_price_move"], "-0.0011")
        self.assertNotIn("probability_up", score["probes"]["fixed_long"])
        self.assertNotIn("brier_up_vs_not_up", score["probes"]["fixed_long"])

    def test_reference_not_entry_controls_magnitude(self):
        score = p.score_pair(quote(NOW, "1", "1"), quote(NOW+1, "1.001", "1.001"), quote(NOW+300, "1.001", "1.001"))
        self.assertTrue(score["price_move_at_least_threshold"])
        self.assertEqual(score["probes"]["fixed_long"]["net_bps"], "0")

    def test_exact_threshold_and_confusion(self):
        base = quote(NOW, "1", "1")
        yes = p.score_pair(base, base, quote(NOW+300, "1.0005", "1.0005"))
        no = p.score_pair(base, base, quote(NOW+300, "1.0004999999999999999999999", "1.0004999999999999999999999"))
        self.assertTrue(yes["price_move_at_least_threshold"])
        self.assertFalse(no["price_move_at_least_threshold"])
        self.assertEqual([p.confusion(c,s) for c,s in (("alert",yes),("alert",no),("control",yes),("control",no))],
                         ["hit", "false_alert", "missed_move", "quiet_control"])

    def test_price_and_clock_alias_rejections(self):
        for bad in (True, float("nan"), float("inf"), 0):
            self.assertRaises(ValueError, p.epoch, bad)
        for changes in ({"bid": True}, {"tradeable": 1}, {"bid": "1e999999"}, {"ask": "1"}):
            self.assertRaises(ValueError, p.price_quote, quote(NOW, **changes))

    def test_event_identity_stable_and_horizon_distinct(self):
        a = p.event_id("batch", PAIR, "feature", 300)
        self.assertEqual(a, p.event_id("batch", PAIR, "feature", 300))
        self.assertNotEqual(a, p.event_id("batch", PAIR, "feature", 900))
        self.assertRaises(ValueError, p.event_id, "batch", PAIR, "feature", 300.0)


class OwnedLedger(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="feature_forward_owned_")
        self.directory = Path(self.tmp.name)
        self.time = NOW
        self.db = l.ForwardLedger(self.directory/"owned.sqlite", clock=lambda: self.time, minimum_free_bytes=0)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def observe(self, stamp=None, force_reference=False, **changes):
        stamp = self.time if stamp is None else stamp
        return self.db.observe_quotes({PAIR: quote(stamp, **changes)}, read_started_epoch=self.time, read_completed_epoch=self.time,
                                      force_reference=force_reference)

    def published(self):
        self.observe(force_reference=True)
        return self.db.publish_comparisons(maps(self.time))

    def complete(self):
        self.published()
        self.time += 1
        self.observe()
        self.time = NOW+300
        self.observe(bid="1.1010", ask="1.1012")
        return self.db.settle()

    def test_publication_after_commit_and_highwater(self):
        self.published()
        rows = self.db.db.execute("SELECT body FROM ff_publications").fetchall()
        self.assertEqual(len(rows), 3)
        for raw, in rows:
            value = json.loads(raw)
            self.assertEqual(value["quote_highwater"], 1)
            self.assertEqual(value["reference_quotes"][PAIR]["seq"], 1)

    def test_shared_pair_outcome_full_feature_denominator(self):
        result = self.complete()
        self.assertEqual(result["scored"], 3)
        summary = self.db.summary()
        self.assertEqual(summary["counts"]["jobs"], 9)
        self.assertEqual(summary["feature_event_counts"]["300:300"]["all_feature_events"], 2)
        self.assertEqual(summary["feature_event_counts"]["300:300"]["hit"], 1)
        self.assertEqual(summary["feature_event_counts"]["300:300"]["missed_move"], 1)
        self.assertEqual(summary["pair_probe_counts"]["300:300"]["scored"], 1)

    def test_publication_quote_cannot_be_entry(self):
        self.published()
        self.time = NOW+300
        self.observe(bid="1.1010", ask="1.1012")
        self.assertEqual(self.db.settle()["unknown"], 3)
        reasons = [json.loads(r[0])["reason"] for r in self.db.db.execute("SELECT body FROM ff_outcomes")]
        self.assertEqual(set(reasons), {"subsequent_entry_unavailable"})

    def test_late_quote_with_old_market_is_not_target(self):
        self.published()
        self.time += 1
        self.observe()
        self.time = NOW+361
        self.assertEqual(self.observe(stamp=NOW+330)["refused"], 1)
        self.assertEqual(self.db.settle()["unknown"], 3)

    def test_inclusive_entry_target_delays(self):
        self.published()
        self.time = NOW+60
        self.observe()
        self.time = NOW+360
        self.observe(bid="1.1010", ask="1.1012")
        self.assertEqual(self.db.settle()["scored"], 3)

    def test_unknown_and_unavailable_retained(self):
        self.db.publish_comparisons(maps(rows=[row(status="unavailable", reason="missing")]))
        self.time += 361
        self.db.settle()
        counts = self.db.summary()["feature_event_counts"]["300:300"]
        self.assertEqual(counts["excluded"], 1)
        self.assertEqual(counts["unknown"], 1)

    def test_quote_collision_and_same_byte_duplicate(self):
        self.assertEqual(self.observe(force_reference=True)["admitted"], 1)
        self.assertEqual(self.observe()["duplicate"], 1)
        self.assertEqual(self.observe(bid="1.0999")["refused"], 1)
        self.assertEqual(self.db.db.execute("SELECT count(*) FROM ff_quotes").fetchone()[0], 1)

    def test_quote_bad_pair_does_not_stop_good_pair(self):
        values = {"BAD": quote(NOW), PAIR: quote(NOW)}
        result = self.db.observe_quotes(values, read_started_epoch=NOW, read_completed_epoch=NOW, force_reference=True)
        self.assertEqual(result, {"refused": 1, "admitted": 1})

    def test_quote_future_and_missing_clock_refused(self):
        self.assertEqual(self.observe(stamp=NOW+1)["refused"], 1)
        self.assertEqual(self.observe(time=None)["refused"], 1)

    def test_restart_dedup_and_immutable_scored_target(self):
        self.complete()
        previous = self.db.db.execute("SELECT job,body FROM ff_outcomes ORDER BY job").fetchall()
        self.db.close()
        self.db = l.ForwardLedger(self.directory/"owned.sqlite", clock=lambda: self.time, minimum_free_bytes=0)
        self.time += 1
        self.observe(bid="1.1020", ask="1.1022")
        self.db.settle()
        self.assertEqual(previous, self.db.db.execute("SELECT job,body FROM ff_outcomes ORDER BY job").fetchall())

    def test_cadence_dedup_not_latest_selection(self):
        self.published()
        self.time += 20
        result = self.db.publish_comparisons(maps(self.time, [row(change=-20.)]))
        self.assertTrue(all(r["status"] == "cadence_already_published" for r in result))
        self.assertEqual(self.db.db.execute("SELECT count(*) FROM ff_batches").fetchone()[0], 3)

    def test_same_source_identity_mutation_refused(self):
        self.published()
        altered = maps()
        for value in altered.values():
            value["source_snapshots"][0]["source_payload_sha256"] = "b"*64
        self.assertTrue(all(r["status"] == "admission_refused" for r in self.db.publish_comparisons(altered)))

    def test_actual_commit_failure_no_publication(self):
        original = self.db.capacity
        calls = [0]
        def fail(*args, **kwargs):
            calls[0] += 1
            if calls[0] == 2:
                raise ValueError("owned_precommit_failure")
            return original(*args, **kwargs)
        with patch.object(self.db, "capacity", side_effect=fail):
            results = self.db.publish_comparisons(maps())
        self.assertEqual(results[0]["status"], "admission_refused")
        self.assertEqual(self.db.db.execute("SELECT count(*) FROM ff_publications").fetchone()[0], 2)

    def test_post_commit_recovery_does_not_backdate(self):
        original = self.db.now
        calls = [0]
        def fail():
            calls[0] += 1
            if calls[0] == 2:
                raise RuntimeError("owned_after_commit_failure")
            return original()
        with patch.object(self.db, "now", side_effect=fail):
            self.assertRaises(RuntimeError, self.db.publish_comparisons, maps())
        self.assertEqual(self.db.db.execute("SELECT count(*) FROM ff_batches").fetchone()[0], 1)
        self.assertEqual(self.db.db.execute("SELECT count(*) FROM ff_publications").fetchone()[0], 0)
        self.time += 5
        self.db.publish_comparisons(maps(self.time))
        completed = self.db.db.execute("SELECT min(completed) FROM ff_publications").fetchone()[0]
        self.assertEqual(completed, NOW+5)

    def test_backstep_and_append_only(self):
        self.published()
        self.time -= 1
        self.assertRaises(ValueError, self.db.now)
        self.assertRaises(sqlite3.IntegrityError, self.db.db.execute, "DELETE FROM ff_batches")

    def test_single_owner_lock_and_reopen(self):
        self.assertRaises(ValueError, l.ForwardLedger, self.directory/"owned.sqlite", minimum_free_bytes=0)
        self.db.close()
        self.db = l.ForwardLedger(self.directory/"owned.sqlite", clock=lambda: self.time, minimum_free_bytes=0)
        self.assertTrue(self.db.due())

    def test_latest_refusal_clock_and_reason_survive_restart(self):
        self.time += 100
        self.db.refusal("owned_refusal")
        self.db.close()
        self.time -= 50
        self.db = l.ForwardLedger(self.directory/"owned.sqlite", clock=lambda: self.time, minimum_free_bytes=0)
        self.assertRaises(ValueError, self.db.now)
        self.assertEqual(self.db.last_refusal["reason"], "owned_refusal")

    def test_source_expires_during_preparation(self):
        def guard():
            self.time = NOW+76
            return {"valid": True}
        self.db.publication_guard = guard
        result = self.db.publish_comparisons(maps(NOW))
        self.assertEqual(self.db.db.execute("SELECT count(*) FROM ff_publications").fetchone()[0], 0)
        self.assertTrue(all(r["status"] == "admission_refused" for r in result))

    def test_score_availability_after_computation(self):
        self.published()
        self.time += 1
        self.observe()
        self.time = NOW+300
        self.observe()
        original = p.score_pair
        def score(*args):
            result = original(*args)
            self.time += 1
            return result
        with patch.object(p, "score_pair", side_effect=score):
            self.db.settle()
        self.assertEqual(self.db.db.execute("SELECT min(available) FROM ff_outcomes").fetchone()[0], NOW+301)

    def test_demand_skips_irrelevant_quotes_and_keeps_first_entry_target(self):
        self.assertEqual(self.observe()["observed_not_needed"], 1)
        self.published()
        self.time += 1
        self.assertEqual(self.observe()["admitted"], 1)
        self.time += 1
        self.assertEqual(self.observe()["observed_not_needed"], 1)
        self.time = NOW+299
        self.assertEqual(self.observe()["observed_not_needed"], 1)
        self.time += 1
        self.assertEqual(self.observe()["admitted"], 1)
        self.time += 1
        self.assertEqual(self.observe()["observed_not_needed"], 1)
        self.db.settle()
        for raw, in self.db.db.execute("SELECT body FROM ff_outcomes"):
            body = json.loads(raw)
            self.assertEqual(body["entry"]["market_epoch"], NOW+1)
            self.assertEqual(body["target"]["market_epoch"], NOW+300)

    def test_different_publication_times_keep_needed_quotes(self):
        self.observe(force_reference=True)
        original = self.db._publish
        def publish(*args):
            result = original(*args)
            self.time += 1
            return result
        with patch.object(self.db, "_publish", side_effect=publish):
            self.db.publish_comparisons(maps())
        self.assertEqual(self.observe()["admitted"], 1)
        self.time = NOW+300
        self.assertEqual(self.observe()["admitted"], 1)
        self.time += 1
        self.assertEqual(self.observe()["admitted"], 1)
        self.time += 1
        self.assertEqual(self.observe()["admitted"], 1)
        self.db.settle()
        targets = sorted(json.loads(raw)["target"]["market_epoch"] for raw, in self.db.db.execute("SELECT body FROM ff_outcomes"))
        self.assertEqual(targets, [NOW+300,NOW+301,NOW+302])

    def test_shared_read_proof_stored_once_for_multiple_quotes(self):
        q = {PAIR: quote(NOW), "GBP_USD": quote(NOW)}
        self.db.observe_quotes(q, read_started_epoch=NOW, read_completed_epoch=NOW,
                               source_receipt={"owned": "shared"}, force_reference=True)
        self.assertEqual(self.db.db.execute("SELECT count(*) FROM ff_quote_receipts").fetchone()[0], 1)
        self.assertEqual(self.db.db.execute("SELECT count(*) FROM ff_quotes").fetchone()[0], 2)

    def test_compact_category_counts_equal_full_event_replay(self):
        self.observe(force_reference=True)
        values = [row(), row(feature_id="control", change=0., unusual_percentile=50),
                  row(feature_id="excluded", status="unavailable", reason="missing")]
        self.db.publish_comparisons(maps(self.time, values))
        self.time += 1
        self.observe()
        self.time = NOW+300
        self.observe(bid="1.1010", ask="1.1012")
        self.db.settle()
        self.time = NOW+961
        self.db.settle()
        expected = {}
        for event in self.db.iter_events():
            count = expected.setdefault(f'{event["window_sec"]}:{event["horizon_sec"]}', Counter())
            category, state = event["comparison"]["forward_category"], event["status"]
            count["all_feature_events"] += 1
            count[category] += 1
            count[state] += 1
            count[f"{category}_{state}"] += 1
            if state == "scored":
                label = p.confusion(category, event["outcome"]["score"])
                count["excluded_with_scored_pair" if label == "excluded" else label] += 1
        self.assertEqual(self.db.summary()["feature_event_counts"], {key:dict(value) for key,value in expected.items()})

    def test_summary_never_decompresses_full_comparisons(self):
        self.complete()
        with patch.object(l, "unpack", side_effect=AssertionError("full_history_decode")):
            self.assertEqual(self.db.summary()["counts"]["outcomes"], 3)
            self.assertRaises(AssertionError, list, self.db.iter_events())

    def test_compact_original_outcomes_commit_together(self):
        self.published()
        self.time += 1
        self.observe()
        self.time = NOW+300
        self.observe()
        original = self.db.capacity
        count = [0]
        def refuse(*args, **kwargs):
            count[0] += 1
            if count[0] == 2:
                raise ValueError("owned_precommit_refusal")
            return original(*args, **kwargs)
        with patch.object(self.db, "capacity", side_effect=refuse):
            self.db.settle()
        self.assertEqual(self.db.db.execute("SELECT job FROM ff_outcomes ORDER BY job").fetchall(),
                         self.db.db.execute("SELECT job FROM ff_compact_outcomes ORDER BY job").fetchall())
        self.assertEqual(self.db.db.execute("SELECT count(*) FROM ff_outcomes").fetchone()[0], 2)

    def test_cap_and_free_floor_reopen_identity(self):
        self.db.close()
        self.assertRaises(ValueError, l.ForwardLedger, self.directory/"owned.sqlite", max_bytes=4096*1024*1024, minimum_free_bytes=0)
        self.assertRaises(ValueError, l.ForwardLedger, self.directory/"owned.sqlite", minimum_free_bytes=1)
        self.db = l.ForwardLedger(self.directory/"owned.sqlite", clock=lambda: self.time, minimum_free_bytes=0)
        self.assertRaises(ValueError, l.ForwardLedger, self.directory/"too_large.sqlite", max_bytes=4097*1024*1024)
        self.assertRaises(ValueError, l.ForwardLedger, self.directory/"alias.sqlite", max_bytes=True)

    def test_restart_rejects_source_generation_change(self):
        self.db.close()
        with patch.object(l.hashlib, "sha256", wraps=hashlib.sha256) as hasher:
            # A different source identity is rejected before data mutations.
            fake = type("Digest", (), {"hexdigest": lambda self: "0"*64})()
            hasher.return_value = fake
            hasher.side_effect = lambda raw=b"": fake
            self.assertRaises(ValueError, l.ForwardLedger, self.directory/"owned.sqlite", minimum_free_bytes=0)
        self.db = l.ForwardLedger(self.directory/"owned.sqlite", clock=lambda: self.time, minimum_free_bytes=0)

    def test_physical_journal_bound_and_no_history_pruning(self):
        maximum = self.db.db.execute("PRAGMA max_page_count").fetchone()[0]
        page = self.db.db.execute("PRAGMA page_size").fetchone()[0]
        self.assertLess(2*maximum*page+4*l.RESERVE, self.db.max_bytes+1)
        self.assertEqual(self.db.db.execute("PRAGMA journal_mode").fetchone()[0], "delete")

    def test_bound_failure_before_payload_write_and_reserved_settlement(self):
        self.published()
        original = self.db.capacity
        def cap(size, *, reserved=False):
            if not reserved:
                raise ValueError("ledger_capacity_refused")
            return original(size, reserved=reserved)
        self.time = NOW+1
        with patch.object(self.db, "capacity", side_effect=cap):
            self.observe()
            self.time = NOW+300
            self.observe(bid="1.1010", ask="1.1012")
            self.assertEqual(self.db.settle()["scored"], 3)
            result = self.db.publish_comparisons(maps(self.time))
            self.assertTrue(all(r["status"] == "admission_refused" for r in result))

    def test_compressed_decode_bounded_and_single_stream(self):
        self.assertEqual(l.unpack(zlib.compress(b'{"a":1}')), {"a": 1})
        self.assertRaises(ValueError, l.unpack, zlib.compress(b"{}")+zlib.compress(b"{}"))
        with patch.object(p, "MAX_BATCH_BYTES", 10):
            self.assertRaises(ValueError, l.unpack, zlib.compress(b" "*11))

    def test_foreign_database_not_modified(self):
        other = self.directory/"foreign.sqlite"
        db = sqlite3.connect(other)
        db.execute("CREATE TABLE untouched(x)")
        db.commit()
        db.close()
        before = other.read_bytes()
        self.assertRaises(sqlite3.Error, l.ForwardLedger, other, minimum_free_bytes=0)
        self.assertEqual(before, other.read_bytes())


class Worker(unittest.TestCase):
    setUp, tearDown = OwnedLedger.setUp, OwnedLedger.tearDown
    def reader(self, path, *, clock):
        now = clock()
        return {PAIR: quote(now)}, now, now, {"sha256": "a"*64}

    def worker(self, **kwargs):
        return w.ForwardWorker(self.db, clock_check=kwargs.pop("clock_check", lambda: proof(self.time)),
                               archive_root=self.directory, quote_path=self.directory/"quotes.json",
                               mapping_reader=kwargs.pop("mapping_reader", lambda *a, **k: maps(self.time)),
                               quote_reader=self.reader, **kwargs)

    def test_clock_false_blocks_all_admission(self):
        worker = self.worker(clock_check=lambda: {"valid": False, "reason": "owned_invalid"})
        self.assertEqual(worker.tick()["status"], "verified_clock_unavailable")
        self.assertEqual(self.db.db.execute("SELECT count(*) FROM ff_quotes").fetchone()[0], 0)

    def test_three_windows_share_one_read_and_only_once_per_bucket(self):
        calls = []
        def reader(*args, **kwargs):
            calls.append(kwargs)
            return maps(self.time)
        worker = self.worker(mapping_reader=reader)
        worker.tick()
        self.time += 5
        worker.tick()
        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0]["include_all_comparisons"], True)
        self.assertEqual(calls[0]["window_secs"], (300, 900, 3600))

    def test_pending_settles_when_mapping_fails(self):
        worker = self.worker()
        worker.tick()
        self.time += 5
        worker.tick()
        self.time = NOW+300
        worker.mapping_reader = lambda *a, **k: (_ for _ in ()).throw(ValueError("owned_missing_archive"))
        result = worker.tick()
        self.assertEqual(result["settlement"]["scored"], 3)
        self.assertEqual(result["status"], "partial_unavailable")

    def test_real_owned_quote_file_missing_clock_not_generation_fallback(self):
        path = self.directory/"quotes.json"
        data = {"schema_version": 1, "producer": "practice_007_dedicated_quote_stream", "research_only": True,
                "generated_utc": iso(NOW), "quote_count": 1, "quotes": {PAIR: quote(NOW)}}
        path.write_bytes(p.canonical(data))
        quotes, start, end, receipt = w.read_quotes(path, clock=lambda: NOW)
        self.assertEqual(receipt["sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
        del quotes[PAIR]["time"]
        self.assertEqual(self.db.observe_quotes(quotes, read_started_epoch=start, read_completed_epoch=end)["refused"], 1)

    def test_current_dedicated_quote_schema_is_accepted(self):
        path = self.directory/"quotes.json"
        data = {"schema_version": 3, "producer": "practice_007_dedicated_quote_stream", "research_only": True,
                "generated_utc": iso(NOW), "quote_count": 1, "coverage": {"current_tradeable_quote_count": 1},
                "quotes": {PAIR: quote(NOW)}}
        path.write_bytes(p.canonical(data))
        quotes, _, _, receipt = w.read_quotes(path, clock=lambda: NOW)
        self.assertEqual(quotes, data["quotes"])
        self.assertEqual(receipt["schema_version"], 3)

    def test_quote_snapshot_type_alias_duplicate_and_count(self):
        path = self.directory/"quotes.json"
        for data in ({"schema_version": True}, {"schema_version": 1, "producer": "old_producer"}):
            path.write_bytes(p.canonical(data))
            self.assertRaises(ValueError, w.read_quotes, path, clock=lambda: NOW)
        path.write_bytes(b'{"schema_version":1,"schema_version":1}')
        self.assertRaises(ValueError, w.read_quotes, path, clock=lambda: NOW)

    def test_original_clock_cannot_be_renewed_by_fresh_replacement(self):
        worker = self.worker()
        original = proof(self.time)
        self.time += 91
        self.assertTrue(worker.verified_clock()["valid"])
        self.assertRaises(ValueError, worker.complete_clock, original)

    def test_clock_ages_during_mapping_no_publication(self):
        def reader(*args, **kwargs):
            self.time += 91
            return maps(self.time)
        worker = self.worker(mapping_reader=reader)
        result = worker.tick()
        self.assertEqual(result["status"], "partial_unavailable")
        self.assertEqual(self.db.db.execute("SELECT count(*) FROM ff_publications").fetchone()[0], 0)

    def test_summary_not_redecoded_on_quote_only_tick(self):
        worker = self.worker()
        with patch.object(self.db, "summary", wraps=self.db.summary) as summary:
            worker.tick()
            self.time += 5
            worker.tick()
            self.assertEqual(summary.call_count, 1)


if __name__ == "__main__":
    unittest.main()
