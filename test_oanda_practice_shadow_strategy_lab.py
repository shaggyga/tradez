import json
import sqlite3
import threading
import time
import unittest
import tempfile
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from trad.oanda_practice_shadow_strategy_lab import (
    AsyncJsonSnapshotWriter,
    AsyncSignalFeedCache,
    FULL_HORIZONS_SEC,
    LanePerformance,
    PracticeExecutor,
    SharedSignalFeed,
    _supervised_candle_data,
    ahl_multihorizon_trend_setup,
    aggregate_complete_candles,
    atomic_json,
    augment_cross_sectional_features,
    build_practice_order,
    build_features,
    build_ma_series_by_timeframe,
    build_timeframe_feature_views,
    classify_miss,
    classify_volatility_regime,
    close_log_handles,
    default_lanes,
    daily_candle_refresh_due,
    economic_gates,
    execution_price_snapshot,
    evaluate_family,
    filter_lanes,
    log_event_counts,
    log_line,
    merge_candle_cache,
    merge_price_overlay,
    normalized_trailing_distance,
    outcome_quote_rejection_reason,
    parse_args,
    partial_shadow_lane_threshold,
    profit_lock_stop_price,
    process_pending,
    should_track_shadow_outcome,
    slow_candle_refresh_due,
    theoretical_pips,
    write_live_model_feature_snapshot,
)


class _TestFeedConnection:
    def rollback(self):
        return None


class _BlockingSignalFeed:
    def __init__(self, started, release, rows):
        self.started = started
        self.release = release
        self.rows = rows
        self.connection = _TestFeedConnection()
        self.closed = False

    def recent(self, _limit):
        self.started.set()
        self.release.wait()
        return self.rows

    def close(self):
        self.closed = True


class _CapturingPublishFeed:
    def __init__(self):
        self.calls = []

    def publish(self, rows, source, ttl_sec):
        materialized = [dict(row) for row in rows]
        self.calls.append((materialized, source, ttl_sec))
        return {"latency_ms": {"commit": 0.1}}


class PartialCycleShadowPublishTests(unittest.TestCase):
    def test_checkpoint_has_margin_for_measured_long_cycles(self):
        self.assertEqual(partial_shadow_lane_threshold(209), 52)
        self.assertEqual(partial_shadow_lane_threshold(3), 1)

    def test_partial_publish_is_forced_research_only(self):
        executor = PracticeExecutor.__new__(PracticeExecutor)
        executor.signal_feed = _CapturingPublishFeed()
        executor.args = SimpleNamespace(
            execution_feed_source="strategy_lab",
            execution_signal_feed_ttl_sec=90.0,
        )
        executor.log_path = Path("unused.jsonl")
        result = executor.publish_partial_cycle_shadow(
            [
                {
                    "id": "candidate-1",
                    "instrument": "EUR_USD",
                    "preconsensus_class": "accepted",
                    "account_eligible": True,
                    "research_only": False,
                },
                {
                    "id": "hard-reject",
                    "instrument": "USD_JPY",
                    "preconsensus_class": "hard_reject",
                },
            ]
        )
        rows, source, ttl_sec = executor.signal_feed.calls[0]
        self.assertEqual(source, "strategy_lab_partial_shadow")
        self.assertEqual(ttl_sec, 90.0)
        self.assertEqual(result["published"], 1)
        self.assertFalse(rows[0]["account_eligible"])
        self.assertTrue(rows[0]["research_only"])
        self.assertEqual(rows[0]["source_kind"], "partial_cycle_shadow")
        self.assertIn(
            "partial_cycle_incomplete_consensus",
            rows[0]["preconsensus_blockers"],
        )


class AsyncSignalFeedCacheTests(unittest.TestCase):
    def test_blocked_refresh_never_blocks_snapshot_and_starts_fail_closed(self):
        started = threading.Event()
        release = threading.Event()
        row = {
            "instrument": "EUR_USD",
            "feed_published_epoch": time.time(),
        }
        cache = AsyncSignalFeedCache(
            Path("unused.sqlite3"),
            limit=10,
            refresh_sec=60.0,
            ttl_sec=30.0,
            feed_factory=lambda: _BlockingSignalFeed(started, release, [row]),
        )
        cache.start()
        self.assertTrue(started.wait(1.0))

        before = time.perf_counter()
        rows, state = cache.snapshot()
        elapsed = time.perf_counter() - before

        self.assertLess(elapsed, 0.1)
        self.assertEqual(rows, [])
        self.assertEqual(state["state"], "warming")
        self.assertTrue(state["refreshing"])
        self.assertTrue(state["fail_closed"])

        release.set()
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            rows, state = cache.snapshot()
            if state["fresh"]:
                break
            time.sleep(0.01)
        self.assertTrue(state["fresh"])
        self.assertEqual(rows[0]["instrument"], "EUR_USD")
        self.assertTrue(cache.stop())

    def test_snapshot_reports_source_generation_and_semantic_duplicates(self):
        started = threading.Event()
        release = threading.Event()
        release.set()
        published_epoch = time.time()
        rows = [
            {
                "id": "first",
                "instrument": "EUR_USD",
                "direction": "buy",
                "family": "trend",
                "input_timeframe": "M5",
                "model_id": "model-a",
                "signal_reference_horizon_sec": 300,
                "feed_source": "strategy_lab",
                "feed_published_epoch": published_epoch,
            },
            {
                "id": "second",
                "instrument": "EUR_USD",
                "direction": "buy",
                "family": "trend",
                "input_timeframe": "M5",
                "model_id": "model-a",
                "signal_reference_horizon_sec": 300,
                "feed_source": "strategy_lab",
                "feed_published_epoch": published_epoch,
            },
            {
                "id": "third",
                "instrument": "USD_JPY",
                "direction": "sell",
                "family": "shadow",
                "feed_source": "one_hour_shadow_v1",
                "feed_published_epoch": published_epoch - 1.0,
            },
        ]
        cache = AsyncSignalFeedCache(
            Path("unused.sqlite3"),
            limit=10,
            refresh_sec=60.0,
            ttl_sec=30.0,
            feed_factory=lambda: _BlockingSignalFeed(started, release, rows),
        )
        cache.start()
        deadline = time.monotonic() + 1.0
        state = {}
        while time.monotonic() < deadline:
            _rows, state = cache.snapshot()
            if state.get("fresh"):
                break
            time.sleep(0.01)

        self.assertEqual(
            state["source_counts"],
            {"one_hour_shadow_v1": 1, "strategy_lab": 2},
        )
        self.assertEqual(
            state["source_generation_counts"],
            {"one_hour_shadow_v1": 1, "strategy_lab": 1},
        )
        self.assertEqual(state["semantic_duplicate_rows"], 1)
        self.assertIn("strategy_lab", state["source_newest_age_sec"])
        self.assertTrue(cache.stop())


class AsyncJsonSnapshotWriterTests(unittest.TestCase):
    def test_slow_write_is_coalesced_off_the_calling_thread(self):
        started = threading.Event()
        release = threading.Event()
        calls = []

        def slow_writer(_path, payload):
            calls.append(payload["generation"])
            started.set()
            release.wait()

        writer = AsyncJsonSnapshotWriter(
            Path("unused.json"),
            writer=slow_writer,
        )
        writer.start()
        before = time.perf_counter()
        first = writer.submit({"generation": 1})
        elapsed = time.perf_counter() - before
        self.assertLess(elapsed, 0.1)
        self.assertTrue(first["pending"])
        self.assertTrue(started.wait(1.0))

        before = time.perf_counter()
        writer.submit({"generation": 2})
        self.assertLess(time.perf_counter() - before, 0.1)
        release.set()

        deadline = time.monotonic() + 1.0
        status = {}
        while time.monotonic() < deadline:
            status = writer.status()
            if status.get("written_generation") == 2:
                break
            time.sleep(0.01)
        self.assertEqual(status["written_generation"], 2)
        self.assertEqual(calls, [1, 2])
        self.assertTrue(writer.stop())


class AsyncSignalFeedExpiryTests(unittest.TestCase):
    def test_expired_source_rows_fail_closed_even_after_successful_refresh(self):
        started = threading.Event()
        release = threading.Event()
        release.set()
        row = {
            "instrument": "EUR_USD",
            "feed_published_epoch": time.time() - 120.0,
        }
        cache = AsyncSignalFeedCache(
            Path("unused.sqlite3"),
            limit=10,
            refresh_sec=60.0,
            ttl_sec=1.0,
            feed_factory=lambda: _BlockingSignalFeed(started, release, [row]),
        )
        cache.start()
        deadline = time.monotonic() + 1.0
        state = {}
        while time.monotonic() < deadline:
            rows, state = cache.snapshot()
            if state.get("generation") == 1:
                break
            time.sleep(0.01)

        self.assertEqual(rows, [])
        self.assertEqual(state["state"], "stale")
        self.assertTrue(state["source_data_stale"])
        self.assertTrue(state["fail_closed"])
        self.assertTrue(cache.stop())


class LogEventCountTests(unittest.TestCase):
    def test_shadow_miss_classes_are_counted_for_lightweight_monitoring(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "strategy.jsonl"
            log_line(path, "shadow_signal", instrument="EUR_USD")
            log_line(path, "shadow_miss", miss_class="near_threshold")
            log_line(path, "shadow_miss", miss_class="hard_reject")

            counts = log_event_counts(path)

            self.assertEqual(counts["shadow_signal"], 1)
            self.assertEqual(counts["shadow_miss"], 2)
            self.assertEqual(counts["shadow_miss:near_threshold"], 1)
            self.assertEqual(counts["shadow_miss:hard_reject"], 1)
            close_log_handles()

    def test_count_only_log_events_preserve_metrics_without_writing_detail(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "strategy.jsonl"
            log_line(
                path,
                "shadow_miss",
                miss_class="hard_reject",
                _count_only=True,
            )

            counts = log_event_counts(path)

            self.assertEqual(counts["shadow_miss"], 1)
            self.assertEqual(counts["shadow_miss:hard_reject"], 1)
            self.assertFalse(path.exists())

    def test_atomic_json_retries_transient_windows_replace_denial(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "state.json"
            original_replace = Path.replace
            attempts = 0

            def transient_denial(source, target):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise PermissionError("destination is briefly open")
                return original_replace(source, target)

            with patch.object(Path, "replace", new=transient_denial):
                atomic_json(path, {"ready": True})

            self.assertEqual(attempts, 2)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"ready": True})

    def test_atomic_json_tolerates_a_longer_transient_reader_lock(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "state.json"
            original_replace = Path.replace
            attempts = 0

            def transient_denial(source, target):
                nonlocal attempts
                attempts += 1
                if attempts <= 6:
                    raise PermissionError("destination remains briefly open")
                return original_replace(source, target)

            with patch.object(Path, "replace", new=transient_denial), patch(
                "trad.oanda_practice_shadow_strategy_lab.time.sleep"
            ):
                atomic_json(path, {"ready": True})

            self.assertEqual(attempts, 7)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"ready": True})

    def test_atomic_json_tolerates_overlapping_monitor_reader_locks(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "state.json"
            original_replace = Path.replace
            attempts = 0

            def transient_denial(source, target):
                nonlocal attempts
                attempts += 1
                if attempts <= 12:
                    raise PermissionError("several monitor readers overlap")
                return original_replace(source, target)

            with patch.object(Path, "replace", new=transient_denial), patch(
                "trad.oanda_practice_shadow_strategy_lab.time.sleep"
            ):
                atomic_json(path, {"ready": True})

            self.assertEqual(attempts, 13)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"ready": True})


def candle_rows(values: list[float], seconds: int) -> list[dict[str, object]]:
    start = datetime(2026, 7, 13, 20, 0, tzinfo=timezone.utc)
    return [
        {
            "time": (start + timedelta(seconds=index * seconds)).isoformat().replace("+00:00", "Z"),
            "complete": True,
            "volume": 1,
            "mid": {
                "o": str(value),
                "h": str(value + 0.01),
                "l": str(value - 0.01),
                "c": str(value),
            },
        }
        for index, value in enumerate(values)
    ]


class TimeframeFeatureViewTests(unittest.TestCase):
    def test_slow_candle_refresh_uses_elapsed_time_not_wall_clock_minute(self):
        cache = {
            "EUR_USD": {"M1": [{}], "M5": [{}], "H1": [{}]},
            "GBP_USD": {"M1": [{}], "M5": [{}], "H1": [{}]},
        }
        instruments = ["EUR_USD", "GBP_USD"]

        self.assertFalse(
            slow_candle_refresh_due(
                cache,
                instruments,
                100.0,
                300.0,
                now_monotonic=399.0,
            )
        )
        self.assertTrue(
            slow_candle_refresh_due(
                cache,
                instruments,
                100.0,
                300.0,
                now_monotonic=400.0,
            )
        )

    def test_slow_candle_refresh_is_due_when_a_timeframe_is_missing(self):
        cache = {"EUR_USD": {"M1": [{}], "M5": [{}]}}

        self.assertTrue(
            slow_candle_refresh_due(
                cache,
                ["EUR_USD"],
                100.0,
                300.0,
                now_monotonic=101.0,
            )
        )

    def test_daily_candles_refresh_hourly_and_when_missing(self):
        cache = {"EUR_USD": {"M1": [{}], "M5": [{}], "H1": [{}], "D": [{}]}}
        self.assertFalse(
            daily_candle_refresh_due(
                cache,
                ["EUR_USD"],
                100.0,
                3600.0,
                now_monotonic=3699.0,
            )
        )
        self.assertTrue(
            daily_candle_refresh_due(
                cache,
                ["EUR_USD"],
                100.0,
                3600.0,
                now_monotonic=3700.0,
            )
        )
        self.assertTrue(
            daily_candle_refresh_due(
                {"EUR_USD": {"M1": [{}], "M5": [{}], "H1": [{}]}},
                ["EUR_USD"],
                100.0,
                3600.0,
                now_monotonic=101.0,
            )
        )

    def test_complete_candle_aggregation_rejects_partial_buckets(self):
        rows = candle_rows([1.0, 1.1, 1.2], 300)
        aggregated = aggregate_complete_candles(rows, 600, 300)

        self.assertEqual(len(aggregated), 1)
        self.assertEqual(aggregated[0]["volume"], 2.0)
        self.assertAlmostEqual(float(aggregated[0]["mid"]["o"]), 1.0)
        self.assertAlmostEqual(float(aggregated[0]["mid"]["c"]), 1.1)

    def test_live_views_cover_available_trained_timeframe_semantics(self):
        views = build_timeframe_feature_views(
            "EUR_USD",
            {
                "M1": candle_rows([1.1 + index * 0.00001 for index in range(120)], 60),
                "M5": candle_rows([1.1 + index * 0.00002 for index in range(360)], 300),
                "H1": candle_rows([1.1 + index * 0.00003 for index in range(500)], 3600),
            },
            0.0001,
        )

        self.assertEqual(
            set(views),
            {"M1", "M5", "M10", "M15", "M30", "H1", "H2", "H3", "H4"},
        )
        self.assertEqual(views["H1"]["input_timeframe_seconds"], 3600)
        self.assertEqual(views["H4"]["input_timeframe_seconds"], 14400)
        self.assertEqual(views["M30"]["current_volume"], 6.0)

    def test_timeframe_view_cache_reuses_unchanged_completed_bars(self):
        candle_sets = {
            "M1": candle_rows(
                [1.1 + index * 0.00001 for index in range(120)],
                60,
            ),
            "M5": candle_rows(
                [1.1 + index * 0.00002 for index in range(360)],
                300,
            ),
            "H1": candle_rows(
                [1.1 + index * 0.00003 for index in range(500)],
                3600,
            ),
        }
        memo = {}
        first = build_timeframe_feature_views(
            "EUR_USD",
            candle_sets,
            0.0001,
            cache=memo,
        )
        second = build_timeframe_feature_views(
            "EUR_USD",
            candle_sets,
            0.0001,
            cache=memo,
        )

        self.assertIs(first["M5"], second["M5"])
        self.assertIs(first["H4"], second["H4"])
        self.assertEqual(len(memo), 9)

        candle_sets["M5"] = [
            *candle_sets["M5"],
            candle_rows([1.2], 300)[0]
            | {"time": "2026-07-15T02:00:00Z"},
        ]
        third = build_timeframe_feature_views(
            "EUR_USD",
            candle_sets,
            0.0001,
            cache=memo,
        )
        self.assertIsNot(second["H4"], third["H4"])

    def test_ma_grid_series_cover_dense_minute_and_long_contexts(self):
        candle_sets = {
            "M1": candle_rows(
                [1.1 + index * 0.00001 for index in range(5000)],
                60,
            ),
            "H1": candle_rows(
                [1.1 + index * 0.00003 for index in range(500)],
                3600,
            ),
        }
        series, origins = build_ma_series_by_timeframe(candle_sets)

        self.assertTrue(
            {
                "M1",
                "M2",
                "M3",
                "M4",
                "M5",
                "M7",
                "M45",
                "H1",
                "H2",
                "H4",
                "H12",
                "D1",
            }
            <= set(series)
        )
        self.assertLessEqual(max(map(len, series.values())), 512)
        self.assertEqual(set(series), set(origins))

    def test_candle_cache_merge_retains_warmup_and_replaces_overlap(self):
        old = candle_rows([1.0 + index * 0.01 for index in range(200)], 60)
        new = candle_rows([2.0 + index * 0.01 for index in range(120)], 60)
        for row in new:
            parsed = datetime.fromisoformat(str(row["time"]).replace("Z", "+00:00"))
            row["time"] = (parsed + timedelta(minutes=80)).isoformat().replace(
                "+00:00",
                "Z",
            )
        cache = {"EUR_USD": {"M1": old}}

        merge_candle_cache(cache, {"EUR_USD": {"M1": new}})

        rows = cache["EUR_USD"]["M1"]
        self.assertEqual(len(rows), 200)
        self.assertEqual(rows[0]["time"], old[0]["time"])
        self.assertEqual(rows[-1]["mid"]["c"], new[-1]["mid"]["c"])

    def test_live_snapshot_separates_structural_and_execution_context(self):
        candle_sets = {
            "M1": candle_rows([1.1 + index * 0.00001 for index in range(120)], 60),
            "M5": candle_rows([1.1 + index * 0.00002 for index in range(360)], 300),
            "H1": candle_rows([1.1 + index * 0.00003 for index in range(500)], 3600),
        }
        features, reason = build_features("EUR_USD", candle_sets, 0.0001)
        self.assertEqual(reason, "")
        views = build_timeframe_feature_views("EUR_USD", candle_sets, 0.0001)
        views["M1"]["depth_imbalance"] = 0.4
        quote = SimpleNamespace(
            bid=1.1,
            ask=1.1001,
            time=datetime.now(timezone.utc).isoformat(),
            source="unit",
            tradeable=True,
        )

        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "snapshot.json"
            written = write_live_model_feature_snapshot(
                path,
                "unit-contract",
                {"EUR_USD": features},
                {"EUR_USD": quote},
                {"EUR_USD": {"depth_imbalance": 0.4}},
                {"EUR_USD": views},
            )
            payload = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(written, 1)
        row = payload["instruments"]["EUR_USD"]
        self.assertEqual(
            set(row["timeframe_features"]),
            {"M1", "M5", "M10", "M15", "M30", "H1", "H2", "H3", "H4"},
        )
        self.assertEqual(
            set(row["series"]),
            {"M1", "M5", "M10", "M15", "M30", "H1", "H2", "H3", "H4"},
        )
        self.assertEqual(
            set(row["structural_series"]), {"M1", "M30", "H1", "H4"}
        )
        self.assertEqual(
            set(row["structural_series"]["M1"]),
            {
                "bar_start_utc",
                "bar_start_times_utc",
                "open",
                "high",
                "low",
                "close",
                "tick_activity",
                "historical_spread_pips",
            },
        )
        self.assertNotIn("depth_imbalance", row["timeframe_features"]["M1"])
        self.assertNotIn("m1__depth_imbalance", row["unified_forecast_features"])
        self.assertEqual(row["microstructure"]["depth_imbalance"], 0.4)
        self.assertEqual(
            payload["contract"]["intrahour_forecast"]["decision_timeframe"],
            "M1",
        )
        self.assertEqual(
            payload["coverage"],
            {
                "expected_feature_instrument_count": 1,
                "quote_input_count": 1,
                "accepted_instrument_count": 1,
                "excluded_instrument_count": 0,
                "quote_exclusions": [],
                "fail_closed_on_invalid_quote": True,
            },
        )

    def test_live_snapshot_explains_fail_closed_quote_exclusions(self):
        current = datetime.now(timezone.utc)
        quotes = {
            "EUR_USD": SimpleNamespace(
                bid=1.1,
                ask=1.1001,
                time=current.isoformat(),
                source="unit",
                tradeable=True,
            ),
            "USD_TRY": SimpleNamespace(
                bid=40.0,
                ask=40.1,
                time=(current - timedelta(seconds=31)).isoformat(),
                source="unit",
                tradeable=True,
            ),
        }
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "snapshot.json"
            written = write_live_model_feature_snapshot(
                path,
                "unit-coverage",
                {"EUR_USD": {}, "USD_TRY": {}},
                quotes,
                {},
            )
            payload = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(written, 1)
        self.assertEqual(payload["coverage"]["expected_feature_instrument_count"], 2)
        self.assertEqual(payload["coverage"]["excluded_instrument_count"], 1)
        self.assertEqual(
            payload["coverage"]["quote_exclusions"][0]["reason"], "stale_quote"
        )


class PendingOutcomeBatchTests(unittest.TestCase):
    def test_quote_overlay_selects_newest_without_rewriting_provenance(self):
        older = SimpleNamespace(
            bid=1.1000,
            ask=1.1002,
            time="2026-08-28T12:00:00+00:00",
            tradeable=True,
            source="stream",
        )
        newer = SimpleNamespace(
            bid=1.1001,
            ask=1.1003,
            time="2026-08-28T12:00:05+00:00",
            tradeable=True,
            source="lab_pricing",
        )

        merged, provenance = merge_price_overlay(
            {"EUR_USD": older},
            {"EUR_USD": newer},
            provider="rest_snapshot",
            observed_utc="2026-08-28T12:00:06+00:00",
        )

        self.assertIs(merged["EUR_USD"], newer)
        self.assertEqual(merged["EUR_USD"].time, newer.time)
        self.assertEqual(merged["EUR_USD"].source, "lab_pricing")
        self.assertEqual(provenance["selected"], 1)
        self.assertEqual(
            provenance["selected_source_counts"], {"lab_pricing": 1}
        )
        self.assertTrue(provenance["timestamps_preserved"])
        self.assertTrue(provenance["tradeability_preserved"])

    def test_quote_overlay_cannot_replace_newer_quote_with_older_snapshot(self):
        newer = SimpleNamespace(
            bid=1.1001,
            ask=1.1003,
            time="2026-08-28T12:00:05+00:00",
            tradeable=True,
            source="stream",
        )
        older = SimpleNamespace(
            bid=1.1000,
            ask=1.1002,
            time="2026-08-28T12:00:00+00:00",
            tradeable=True,
            source="lab_pricing",
        )

        merged, provenance = merge_price_overlay(
            {"EUR_USD": newer},
            {"EUR_USD": older},
            provider="rest_snapshot",
        )

        self.assertIs(merged["EUR_USD"], newer)
        self.assertEqual(provenance["selected"], 0)

    def test_quote_overlay_preserves_nontradeable_state_for_fail_closed_gate(self):
        closed = SimpleNamespace(
            bid=1.1001,
            ask=1.1003,
            time=datetime.now(timezone.utc).isoformat(),
            tradeable=False,
            source="lab_pricing",
        )

        merged, _ = merge_price_overlay(
            {},
            {"EUR_USD": closed},
            provider="rest_snapshot",
        )

        self.assertEqual(
            outcome_quote_rejection_reason(merged["EUR_USD"]),
            "quote_not_tradeable",
        )

    def test_quote_quality_rejects_stale_and_closed_prices(self):
        current = datetime.now(timezone.utc)
        self.assertEqual(
            outcome_quote_rejection_reason(
                SimpleNamespace(time=current.isoformat(), tradeable=True)
            ),
            "",
        )
        self.assertEqual(
            outcome_quote_rejection_reason(
                SimpleNamespace(
                    time=(current - timedelta(seconds=31)).isoformat(),
                    tradeable=True,
                )
            ),
            "stale_quote",
        )
        self.assertEqual(
            outcome_quote_rejection_reason(
                SimpleNamespace(time=current.isoformat(), tradeable=False)
            ),
            "quote_not_tradeable",
        )

    def test_outcome_batch_limit_leaves_unprocessed_items_pending(self):
        with tempfile.TemporaryDirectory() as temporary:
            now = time.monotonic()
            pending = [
                {
                    "id": f"signal-{index}",
                    "lane_id": "momentum.fast",
                    "family": "momentum",
                    "profile": "fast",
                    "kind": "signal",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "blocked_reason": "",
                    "remaining_horizons": [60.0],
                    "created_monotonic": now - 61.0,
                    "pip": 0.0001,
                    "entry_bid": 1.1000,
                    "entry_ask": 1.1002,
                    "entry_time": "2026-07-16T12:00:00Z",
                    "track_exit_path": False,
                }
                for index in range(2)
            ]
            prices = {
                "EUR_USD": SimpleNamespace(
                    bid=1.1004,
                    ask=1.1006,
                    time=datetime.now(timezone.utc).isoformat(),
                    tradeable=True,
                )
            }

            emitted = process_pending(
                prices,
                pending,
                Path(temporary) / "outcomes.jsonl",
                max_outcomes=1,
            )
            close_log_handles()

        self.assertEqual(emitted, 1)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["id"], "signal-1")

    def test_rotating_queue_bounds_scan_without_starving_later_items(self):
        with tempfile.TemporaryDirectory() as temporary:
            now = time.monotonic()
            base = {
                "lane_id": "momentum.fast",
                "family": "momentum",
                "profile": "fast",
                "kind": "signal",
                "instrument": "EUR_USD",
                "direction": "buy",
                "blocked_reason": "",
                "remaining_horizons": [60.0],
                "pip": 0.0001,
                "entry_bid": 1.1000,
                "entry_ask": 1.1002,
                "entry_time": "2026-07-16T12:00:00Z",
                "track_exit_path": False,
            }
            pending = deque(
                [
                    {
                        **base,
                        "id": "future",
                        "created_monotonic": now,
                    },
                    {
                        **base,
                        "id": "matured",
                        "created_monotonic": now - 61.0,
                    },
                ]
            )
            prices = {
                "EUR_USD": SimpleNamespace(
                    bid=1.1004,
                    ask=1.1006,
                    time=datetime.now(timezone.utc).isoformat(),
                    tradeable=True,
                )
            }
            first = process_pending(
                prices,
                pending,
                Path(temporary) / "outcomes.jsonl",
                max_scan_items=1,
            )
            second = process_pending(
                prices,
                pending,
                Path(temporary) / "outcomes.jsonl",
                max_scan_items=1,
            )
            close_log_handles()

        self.assertEqual(first, 0)
        self.assertEqual(second, 1)
        self.assertEqual([row["id"] for row in pending], ["future"])

    def test_path_outcome_records_executable_spread_and_time_to_positive(self):
        class CaptureStore:
            def __init__(self):
                self.rows = []

            def observe(self, payload):
                self.rows.append(payload)

            def summary(self):
                return {"captured": len(self.rows)}

        with tempfile.TemporaryDirectory() as temporary:
            now = time.monotonic()
            pending = [
                {
                    "id": "signal-path",
                    "lane_id": "momentum.fast",
                    "family": "momentum",
                    "profile": "fast",
                    "model_id": "momentum.fast",
                    "input_timeframe": "M1",
                    "kind": "signal",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "blocked_reason": "",
                    "remaining_horizons": [60.0],
                    "created_monotonic": now - 61.0,
                    "pip": 0.0001,
                    "entry_bid": 1.1000,
                    "entry_ask": 1.1002,
                    "entry_time": "2026-07-16T12:00:00Z",
                    "max_favorable_pips": 0.0,
                    "max_adverse_pips": 0.0,
                    "path_samples": 0,
                }
            ]
            prices = {
                "EUR_USD": SimpleNamespace(
                    bid=1.1004,
                    ask=1.1006,
                    time=datetime.now(timezone.utc).isoformat(),
                    tradeable=True,
                )
            }
            store = CaptureStore()
            emitted = process_pending(
                prices,
                pending,
                Path(temporary) / "outcomes.jsonl",
                outcome_store=store,
            )
            close_log_handles()

        self.assertEqual(emitted, 1)
        self.assertEqual(len(store.rows), 1)
        row = store.rows[0]
        self.assertAlmostEqual(row["entry_spread_pips"], 2.0)
        self.assertGreaterEqual(row["first_positive_sec"], 60.0)
        self.assertEqual(row["path_samples"], 1)
        self.assertEqual(row["positive_path_samples"], 1)

    def test_nontradeable_matured_outcome_is_censored(self):
        class CaptureStore:
            def __init__(self):
                self.rows = []

            def observe(self, payload):
                self.rows.append(payload)

            def summary(self):
                return {"captured": len(self.rows)}

        with tempfile.TemporaryDirectory() as temporary:
            log_path = Path(temporary) / "outcomes.jsonl"
            pending = [
                {
                    "id": "weekend-crossing-signal",
                    "lane_id": "kalman_local_trend.fast",
                    "family": "kalman_local_trend",
                    "profile": "fast",
                    "kind": "signal",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "blocked_reason": "",
                    "remaining_horizons": [60.0],
                    "created_monotonic": time.monotonic() - 91.0,
                    "pip": 0.0001,
                    "entry_bid": 1.1000,
                    "entry_ask": 1.1002,
                    "entry_time": "2026-07-17T20:59:00Z",
                    "track_exit_path": False,
                }
            ]
            prices = {
                "EUR_USD": SimpleNamespace(
                    bid=1.0990,
                    ask=1.1010,
                    time=datetime.now(timezone.utc).isoformat(),
                    tradeable=False,
                )
            }
            store = CaptureStore()

            emitted = process_pending(
                prices,
                pending,
                log_path,
                outcome_store=store,
            )
            close_log_handles()
            rows = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]

        self.assertEqual(emitted, 0)
        self.assertEqual(pending, [])
        self.assertEqual(store.rows, [])
        self.assertEqual(rows[-1]["event"], "shadow_outcome_censored_batch")
        self.assertEqual(rows[-1]["reason_counts"], {"quote_not_tradeable": 1})


def features_for(
    closes: list[float],
    *,
    m5_closes: list[float] | None = None,
    opens: list[float] | None = None,
    highs: list[float] | None = None,
    lows: list[float] | None = None,
    volumes: list[float] | None = None,
) -> dict[str, object]:
    m5 = m5_closes or [1.0 + index * 0.00001 for index in range(210)]
    pip = 0.0001
    window = closes[-20:]
    low = min(window)
    high = max(window)
    last = closes[-1]
    return {
        "instrument": "EUR_USD",
        "pip": pip,
        "candle_time": "2026-07-13T22:00:00Z",
        "closes": closes,
        "opens": opens or list(closes),
        "highs": highs or [value + 0.0001 for value in closes],
        "lows": lows or [value - 0.0001 for value in closes],
        "volumes": volumes or [100.0] * len(closes),
        "m5_closes": m5,
        "m10_closes": m5[1::2],
        "m15_closes": m5[2::3],
        "m30_closes": m5[5::6],
        "h1_closes": m5[11::12],
        "last": last,
        "r1_pips": (last - closes[-2]) / pip,
        "r3_pips": (last - closes[-4]) / pip,
        "r5_pips": (last - closes[-6]) / pip,
        "m5_r1_pips": (m5[-1] - m5[-2]) / pip,
        "m5_r3_pips": (m5[-1] - m5[-4]) / pip,
        "pos20": 0.5 if high == low else (last - low) / (high - low),
        "m1_atr14_pips": 2.0,
        "m5_atr14_pips": 5.0,
        "volume_ratio_12": 1.0,
        "volume_ratio_30": 1.0,
        "current_candle_spread_pips": 0.8,
        "median_spread_12_pips": 1.0,
        "spread_ratio_12": 0.8,
        "spread_drop_3_pips": 0.2,
    }


class CatalogTests(unittest.TestCase):
    def test_shared_horizon_surface_starts_at_one_minute_through_d1(self):
        self.assertEqual(FULL_HORIZONS_SEC[0], 60)
        self.assertEqual(FULL_HORIZONS_SEC[-1], 86400)
        self.assertEqual(len(FULL_HORIZONS_SEC), 15)

    def test_direct_h1_backfill_builds_ready_h4_series(self):
        m1 = candle_rows([1.0 + index * 0.00001 for index in range(120)], 60)
        m5 = candle_rows([1.0 + index * 0.00002 for index in range(360)], 300)
        h1 = candle_rows([1.0 + index * 0.00010 for index in range(160)], 3600)

        features, reason = build_features(
            "EUR_USD",
            {"M1": m1, "M5": m5, "H1": h1},
            0.0001,
        )

        self.assertEqual(reason, "")
        self.assertIsNotNone(features)
        self.assertGreaterEqual(len(features["h4_closes"]), 40)
        self.assertTrue(features["series_origins"]["H4"])

    def test_ahl_daily_trend_score_is_causal_and_shadow_sized(self):
        daily = candle_rows(
            [1.0 + index * 0.001 for index in range(60)],
            86400,
        )
        setup = ahl_multihorizon_trend_setup(
            {
                "pip": 0.0001,
                "d1_closes": [float(row["mid"]["c"]) for row in daily],
                "d1_candles": daily,
                "d1_origin": daily[-1]["time"],
            },
            {
                "lookbacks_days": "5,10,21,42",
                "volatility_lookback_days": 20,
                "target_annualized_volatility": 0.10,
                "min_abs_score": 2,
            },
        )

        self.assertEqual(setup.direction, "buy")
        self.assertEqual(setup.signal["trend_score"], 4)
        self.assertEqual(setup.signal["input_timeframe"], "D1")
        self.assertEqual(setup.signal["reference_horizon_sec"], 86400)
        self.assertEqual(setup.signal["conviction_fraction"], 1.0)
        self.assertGreater(setup.signal["research_risk_weight"], 0.0)
        self.assertTrue(setup.signal["research_shadow"])

    def setUp(self):
        self.lanes = default_lanes()
        self.by_id = {lane.lane_id: lane for lane in self.lanes}

    def test_default_catalog_has_four_variants_for_each_default_family(self):
        self.assertEqual(len(self.lanes), 209)
        family_counts = Counter(lane.family for lane in self.lanes)
        self.assertEqual(family_counts.pop("ahl_multihorizon_trend"), 1)
        self.assertEqual(set(family_counts.values()), {4})
        profile_counts = Counter(lane.profile for lane in self.lanes)
        self.assertEqual(profile_counts.pop("shadow"), 1)
        self.assertEqual(set(profile_counts.values()), {52})
        self.assertIn("ahl_multihorizon_trend", {lane.family for lane in self.lanes})
        self.assertIn("currency_strength", {lane.family for lane in self.lanes})
        self.assertIn("relative_value_reversion", {lane.family for lane in self.lanes})
        self.assertIn("supervised_return_rank", {lane.family for lane in self.lanes})
        self.assertIn("higher_timeframe_alignment", {lane.family for lane in self.lanes})
        self.assertIn("momentum", {lane.family for lane in self.lanes})
        self.assertIn("donchian_breakout", {lane.family for lane in self.lanes})
        self.assertIn("stochastic_reversal", {lane.family for lane in self.lanes})
        self.assertIn("cross_sectional_pair_rank", {lane.family for lane in self.lanes})
        self.assertIn("regime_switching", {lane.family for lane in self.lanes})
        self.assertIn("pattern_count_forecast", {lane.family for lane in self.lanes})
        self.assertIn("failed_breakout_reversal", {lane.family for lane in self.lanes})
        self.assertIn("efficiency_filtered_momentum", {lane.family for lane in self.lanes})
        self.assertIn("cross_pair_lead_lag", {lane.family for lane in self.lanes})
        self.assertIn("spread_compression_momentum", {lane.family for lane in self.lanes})
        self.assertIn("rsi_trend_continuation", {lane.family for lane in self.lanes})
        self.assertIn("volatility_squeeze_breakout", {lane.family for lane in self.lanes})
        self.assertIn("kama_adaptive_trend", {lane.family for lane in self.lanes})
        self.assertIn("cci_reversion", {lane.family for lane in self.lanes})
        self.assertIn("breakout_retest", {lane.family for lane in self.lanes})

    def test_default_cli_filters_keep_dedicated_shadow_research_lane(self):
        args = parse_args([])
        selected = filter_lanes(self.lanes, args.families, args.profiles)

        self.assertEqual(len(selected), 209)
        shadow = [lane for lane in selected if lane.lane_id == "ahl_multihorizon_trend.shadow"]
        self.assertEqual(len(shadow), 1)
        self.assertFalse(shadow[0].parameters["account_eligible"])
        self.assertFalse(args.execution_allow_unvalidated_signals)
        self.assertTrue(args.execution_horizon_scaled_protection)
        self.assertEqual(args.execution_intrahour_max_spread_pips, 2.0)
        self.assertEqual(args.execution_intrahour_min_gross_to_spread, 2.5)
        self.assertEqual(args.execution_min_signal_confidence, 0.56)
        self.assertEqual(args.execution_min_signal_expected_net_pips, 1.0)
        self.assertIsNone(args.research_market_quote_snapshot)
        self.assertIn("volume_climax_reversal", {lane.family for lane in self.lanes})
        self.assertIn("spread_mean_reversion", {lane.family for lane in self.lanes})
        self.assertIn("volatility_shock_fade", {lane.family for lane in self.lanes})
        self.assertIn("session_range_breakout", {lane.family for lane in self.lanes})
        self.assertIn("micro_channel_break", {lane.family for lane in self.lanes})
        self.assertIn("trend_momentum_confluence", {lane.family for lane in self.lanes})
        self.assertIn("breakout_volume_confluence", {lane.family for lane in self.lanes})
        self.assertIn("oscillator_reversion_confluence", {lane.family for lane in self.lanes})
        self.assertIn("cross_market_confluence", {lane.family for lane in self.lanes})
        self.assertIn("inverse_correlation_veto", {lane.family for lane in self.lanes})
        self.assertIn("signal_combination_rules", {lane.family for lane in self.lanes})
        self.assertTrue(
            {
                "kalman_local_trend",
                "markov_sign_transition",
                "online_ar_forecast",
                "variance_ratio_regime",
                "permutation_entropy_momentum",
                "cusum_breakout",
                "har_volatility_momentum",
                "bipower_jump_reversal",
                "garch_volatility_breakout",
                "hurst_regime_forecast",
                "theil_sen_trend",
                "vwap_deviation_reversion",
                "ny_session_vwap_sell_reversion",
            }.issubset({lane.family for lane in self.lanes})
        )

    def test_research_upgrade_families_start_shadow_only(self):
        research_families = {
            "ahl_multihorizon_trend",
            "kalman_local_trend",
            "markov_sign_transition",
            "online_ar_forecast",
            "variance_ratio_regime",
            "permutation_entropy_momentum",
            "cusum_breakout",
            "har_volatility_momentum",
            "bipower_jump_reversal",
            "garch_volatility_breakout",
            "hurst_regime_forecast",
            "theil_sen_trend",
            "vwap_deviation_reversion",
            "ny_session_vwap_sell_reversion",
        }
        research_lanes = [lane for lane in self.lanes if lane.family in research_families]
        self.assertEqual(len(research_lanes), 53)
        self.assertTrue(all(not lane.parameters["account_eligible"] for lane in research_lanes))
        self.assertEqual(len(self.by_id), len(self.lanes))

    def test_profiles_change_family_and_execution_parameters(self):
        strict = self.by_id["currency_strength.strict"].parameters
        loose = self.by_id["currency_strength.loose"].parameters
        self.assertGreater(strict["min_strength_r3"], loose["min_strength_r3"])
        self.assertLess(strict["max_spread_pips"], loose["max_spread_pips"])
        self.assertGreater(strict["min_reward_to_spread"], loose["min_reward_to_spread"])

    def test_clear_loser_loose_and_inverse_veto_lanes_are_shadow_only(self):
        self.assertFalse(self.by_id["trend_momentum_confluence.loose"].parameters["account_eligible"])
        self.assertTrue(self.by_id["oscillator_reversion_confluence.fast"].parameters["account_eligible"])
        self.assertTrue(
            all(
                not self.by_id[f"inverse_correlation_veto.{profile}"].parameters["account_eligible"]
                for profile in ("strict", "balanced", "fast", "loose")
            )
        )


class PracticeExecutionTests(unittest.TestCase):
    def test_execution_price_snapshot_uses_rest_cache_without_stream(self):
        quote = SimpleNamespace(
            bid=1.1001,
            ask=1.1003,
            time=datetime.now(timezone.utc).isoformat(),
            tradeable=True,
        )

        cached = {"EUR_USD": quote}
        snapshot = execution_price_snapshot(None, cached)

        self.assertEqual(snapshot, {"EUR_USD": quote})
        self.assertIsNot(snapshot, cached)

    def test_execution_price_snapshot_merges_partial_stream_over_rest_cache(self):
        older = datetime.now(timezone.utc) - timedelta(seconds=1)
        newer = datetime.now(timezone.utc)
        rest_eur = SimpleNamespace(
            bid=1.1001,
            ask=1.1003,
            time=older.isoformat(),
            tradeable=True,
        )
        stream_eur = SimpleNamespace(
            bid=1.1002,
            ask=1.1004,
            time=newer.isoformat(),
            tradeable=True,
        )
        rest_jpy = SimpleNamespace(
            bid=150.01,
            ask=150.03,
            time=newer.isoformat(),
            tradeable=True,
        )
        stream = SimpleNamespace(snapshot=lambda: {"EUR_USD": stream_eur})

        snapshot = execution_price_snapshot(
            stream,
            {"EUR_USD": rest_eur, "USD_JPY": rest_jpy},
        )

        self.assertIs(snapshot["EUR_USD"], stream_eur)
        self.assertIs(snapshot["USD_JPY"], rest_jpy)

    def test_execution_price_snapshot_refreshes_stale_cache_from_rest(self):
        older = datetime.now(timezone.utc) - timedelta(minutes=5)
        newer = datetime.now(timezone.utc)
        cached = SimpleNamespace(
            bid=1.1001,
            ask=1.1003,
            time=older.isoformat(),
            tradeable=True,
        )
        fresh = SimpleNamespace(
            bid=1.1004,
            ask=1.1006,
            time=newer.isoformat(),
            tradeable=True,
        )

        snapshot = execution_price_snapshot(
            None,
            {"EUR_USD": cached},
            snapshot_provider=lambda: {"EUR_USD": fresh},
        )

        self.assertIs(snapshot["EUR_USD"], fresh)

    def test_late_bound_consumer_feed_recovers_async_cache_before_start(self):
        args = parse_args(["--execution-feed-consumer-only"])
        args.execution_signal_feed_database = None
        executor = PracticeExecutor(
            None,
            "test",
            Path("unused.jsonl"),
            LanePerformance(),
            args,
        )
        self.assertIsNone(executor.signal_feed_cache)

        args.execution_signal_feed_database = Path("late-bound.sqlite3")
        self.assertTrue(executor.ensure_async_signal_feed_cache())
        self.assertIsInstance(executor.signal_feed_cache, AsyncSignalFeedCache)
        self.assertEqual(executor.last_feed_cache_stats["state"], "warming")
        self.assertTrue(executor.last_feed_cache_stats["fail_closed"])
        self.assertFalse(executor.ensure_async_signal_feed_cache())
        executor.close()

    def test_local_candidates_survive_an_expired_shared_feed_batch(self):
        local = [{"id": "fresh", "signal_confidence": 0.61}]
        shared = [{"id": "other", "signal_confidence": 0.57}]

        merged = PracticeExecutor.merge_signal_candidates(local, shared)

        self.assertEqual({row["id"] for row in merged}, {"fresh", "other"})

    def test_local_candidate_overrides_stale_shared_copy(self):
        local = [{"id": "same", "entry_time": "fresh"}]
        shared = [{"id": "same", "entry_time": "stale"}]

        merged = PracticeExecutor.merge_signal_candidates(local, shared)

        self.assertEqual(merged, local)

    def test_candidate_quote_refresh_uses_latest_stream_price(self):
        current = datetime.now(timezone.utc).isoformat()
        executor = PracticeExecutor(
            None,
            "test",
            Path("unused.jsonl"),
            LanePerformance(),
            SimpleNamespace(),
        )
        executor.set_price_snapshot_provider(
            lambda: {
                "EUR_USD": SimpleNamespace(
                    bid=1.1001,
                    ask=1.1003,
                    time=current,
                    tradeable=True,
                )
            }
        )

        refreshed = executor.refresh_candidate_quotes(
            [
                {
                    "id": "candidate",
                    "instrument": "EUR_USD",
                    "bid": 1.0,
                    "ask": 1.1,
                    "entry_time": "stale",
                    "pip": 0.0001,
                    "signal_strength_pips": 4.0,
                    "stop_loss_pips": 5.0,
                    "take_profit_r": 1.5,
                }
            ]
        )[0]

        self.assertEqual(refreshed["bid"], 1.1001)
        self.assertEqual(refreshed["ask"], 1.1003)
        self.assertEqual(refreshed["entry_time"], current)
        self.assertAlmostEqual(refreshed["spread_pips"], 2.0)
        self.assertAlmostEqual(refreshed["signal_to_spread"], 2.0)
        self.assertIn("forecast_created_monotonic", refreshed)

    def test_compact_signal_snapshot_omits_horizon_contributors(self):
        ranked = [
            {
                "id": "signal",
                "instrument": "EUR_USD",
                "direction": "buy",
                "signal_eligible": False,
                "signal_confidence": 0.56,
                "signal_score": 0.1,
                "normalized_rank_score": 0.2,
                "aggregate_signal_id": "aggregate_signal_test",
                "aggregate_signal_lineage_contract_id": "lineage-v1",
                "direction_conflict": True,
                "direction_conflict_contract_id": "conflict-v1",
                "direction_conflict_contributor_count": 1,
                "direction_conflict_account_eligible_count": 0,
                "direction_conflict_execution_component_count": 0,
                "direction_conflict_shadow_only_count": 1,
                "direction_conflict_only_shadow_or_account_ineligible": True,
                "direction_conflict_contributors": [{"family": "inverse-veto"}],
                "horizon_breakdown": [
                    {
                        "horizon_sec": 60,
                        "direction": "buy",
                        "signal_eligible": False,
                        "signal_confidence": 0.56,
                        "projected_net_pips": 0.2,
                        "all_contributor_gross_to_spread": 8.0,
                        "directional_gross_to_spread": 1.5,
                        "aggregate_signal_id": "aggregate_signal_test_60",
                        "aggregate_signal_lineage_contract_id": "lineage-v1",
                        "direction_conflict": True,
                        "direction_conflict_contract_id": "conflict-v1",
                        "direction_conflict_contributor_count": 1,
                        "direction_conflict_account_eligible_count": 0,
                        "direction_conflict_execution_component_count": 0,
                        "direction_conflict_shadow_only_count": 1,
                        "direction_conflict_only_shadow_or_account_ineligible": True,
                        "direction_conflict_contributors": [
                            {"family": "inverse-veto"}
                        ],
                        "contributors": [{"model_id": "large-duplicate"}],
                    }
                ],
            }
        ]
        executor = PracticeExecutor(
            None,
            "test",
            Path("unused.jsonl"),
            LanePerformance(),
            SimpleNamespace(execution_top_lanes=8),
        )

        class Promotion:
            def rank_signal_candidates(self, *args, **kwargs):
                return ranked

        executor.promotion = Promotion()
        executor.add_signal_agreement = lambda rows: rows
        executor.enrich_candidate_economics = lambda rows: rows
        executor.consolidate_ranked_signals = lambda rows: rows
        executor.apply_prediction_quality = lambda row: {}
        executor.performance.recent_negative_veto = lambda *args, **kwargs: {
            "veto": False,
            "positive_ready": False,
        }
        executor.args.execution_min_signal_confidence = 0.54
        executor.args.execution_min_signal_expected_net_pips = 0.05
        executor.args.execution_recent_veto_min_samples = 5
        executor.args.execution_recent_veto_max_win_rate = 40.0
        executor.args.execution_min_historical_reliability = 0.15
        executor.args.execution_allow_unvalidated_signals = True
        selected, top = executor.ranked_candidate([])

        self.assertIsNone(selected)
        self.assertIs(top[0]["signal_eligible"], False)
        self.assertNotIn("contributors", top[0]["horizon_breakdown"][0])
        self.assertEqual(
            top[0]["horizon_breakdown"][0]["directional_gross_to_spread"],
            1.5,
        )
        self.assertEqual(top[0]["aggregate_signal_id"], "aggregate_signal_test")
        self.assertEqual(
            top[0]["direction_conflict_contract_id"],
            "conflict-v1",
        )
        self.assertTrue(
            top[0]["direction_conflict_only_shadow_or_account_ineligible"]
        )
        self.assertEqual(
            top[0]["horizon_breakdown"][0]["aggregate_signal_id"],
            "aggregate_signal_test_60",
        )
        self.assertEqual(
            top[0]["horizon_breakdown"][0]["direction_conflict_contract_id"],
            "conflict-v1",
        )

    def test_signal_snapshot_labels_pre_final_direction_conflict_selection(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "signal_snapshot.json"
            executor = PracticeExecutor(
                None,
                "test",
                Path(temporary) / "events.jsonl",
                LanePerformance(),
                SimpleNamespace(
                    execution_signal_snapshot=str(path),
                    execution_signal_snapshot_sec=0.5,
                    execution_top_lanes=8,
                ),
            )
            selected = {
                "id": "conflicted-signal",
                "signal_group_id": "group",
                "instrument": "USD_JPY",
                "direction": "buy",
                "preferred_horizon_sec": 43_200,
                "signal_confidence": 0.58,
                "projected_net_pips": 1.54,
                "direction_conflict": True,
            }
            executor.last_qualified_candidates = [selected]

            executor.write_signal_snapshot(selected, [], 1, 1)

            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(
                payload["selected_stage"],
                "signal_gate_pre_final_execution_gates",
            )
            self.assertEqual(
                payload["selected_final_gate_status"],
                "blocked_direction_conflict",
            )
            self.assertFalse(
                payload["selected_routable_after_direction_conflict_gate"]
            )
            self.assertTrue(payload["selected"]["direction_conflict"])
            self.assertFalse(
                payload["selected"]["routable_after_direction_conflict_gate"]
            )
            self.assertEqual(
                payload["selected"]["final_execution_status"],
                "blocked_direction_conflict",
            )
            self.assertEqual(payload["qualified_signal_count"], 1)
            self.assertEqual(payload["nonconflicting_qualified_signal_count"], 0)

    def test_signal_snapshot_reports_ranked_quote_rejections(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "signal_snapshot.json"
            executor = PracticeExecutor(
                None,
                "test",
                Path(temporary) / "events.jsonl",
                LanePerformance(),
                SimpleNamespace(
                    execution_signal_snapshot=str(path),
                    execution_signal_snapshot_sec=0.5,
                    execution_top_lanes=8,
                ),
            )
            now = datetime.now(timezone.utc)
            executor.set_price_snapshot_provider(
                lambda: {
                    "EUR_USD": SimpleNamespace(
                        bid=1.1001,
                        ask=1.1003,
                        time=now.isoformat(),
                        tradeable=True,
                    ),
                    "USD_JPY": SimpleNamespace(
                        bid=150.01,
                        ask=150.03,
                        time=(now - timedelta(minutes=5)).isoformat(),
                        tradeable=True,
                    ),
                }
            )

            executor.write_signal_snapshot(
                None,
                [
                    {"instrument": "EUR_USD", "direction": "buy"},
                    {"instrument": "USD_JPY", "direction": "sell"},
                ],
                2,
                2,
            )

            quality = json.loads(path.read_text(encoding="utf-8"))["snapshot_quality"]
            self.assertEqual(quality["market_quote_scope"], "ranked_signal_instruments")
            self.assertEqual(quality["market_quote_provider_status"], "ok")
            self.assertEqual(quality["market_quote_expected_instrument_count"], 2)
            self.assertEqual(quality["market_quote_count"], 1)
            self.assertEqual(quality["market_quote_coverage_ratio"], 0.5)
            self.assertEqual(quality["market_quote_rejected_count"], 1)
            self.assertEqual(
                quality["market_quote_rejection_reasons"],
                {"stale_quote": 1},
            )
            self.assertEqual(
                quality["market_quote_rejected_instruments"],
                {"USD_JPY": "stale_quote"},
            )

    def test_ranked_candidate_continues_local_only_when_feed_is_locked(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "strategy.jsonl"
            executor = PracticeExecutor(
                None,
                "test",
                path,
                LanePerformance(),
                SimpleNamespace(
                    execution_feed_source="strategy_lab",
                    execution_signal_feed_ttl_sec=8.0,
                    execution_signal_feed_limit=50_000,
                    execution_min_signal_confidence=0.54,
                    execution_min_signal_expected_net_pips=0.05,
                    execution_top_lanes=8,
                ),
            )

            class Connection:
                rollback_called = False

                def rollback(self):
                    self.rollback_called = True

            class LockedFeed:
                connection = Connection()

                def publish(self, *args, **kwargs):
                    raise sqlite3.OperationalError("database is locked")

                def recent(self, *args, **kwargs):
                    raise AssertionError("recent must not run after publish contention")

            executor.signal_feed = LockedFeed()

            class Promotion:
                def rank_signal_candidates(self, *args, **kwargs):
                    return []

            executor.promotion = Promotion()
            executor.refresh_candidate_quotes = lambda rows: rows
            executor.enrich_candidate_economics = lambda rows: rows
            executor.add_signal_agreement = lambda rows: rows
            executor.compact_signal_rows = lambda rows: rows

            selected, ranked = executor.ranked_candidate([])
            close_log_handles()

            self.assertIsNone(selected)
            self.assertEqual(ranked, [])
            self.assertTrue(executor.signal_feed.connection.rollback_called)
            events = [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertIn(
                "signal_feed_contention",
                {event["event"] for event in events},
            )

    def test_hot_feed_omits_hard_rejects_but_keeps_input_unchanged(self):
        rows = [
            {"id": "accepted", "preconsensus_class": "accepted"},
            {"id": "near", "preconsensus_class": "near_threshold"},
            {"id": "hard", "preconsensus_class": "hard_reject"},
            {"id": "legacy"},
        ]

        published = PracticeExecutor.hot_feed_publish_candidates(rows)

        self.assertEqual(
            [row["id"] for row in published],
            ["accepted", "near", "legacy"],
        )
        self.assertEqual(len(rows), 4)

    def test_shadow_outcome_sampling_keeps_signals_and_is_deterministic(self):
        self.assertTrue(
            should_track_shadow_outcome("signal", "signal", "", 0.0)
        )
        first = should_track_shadow_outcome(
            "blocked-setup",
            "miss",
            "hard_reject",
            0.25,
        )
        second = should_track_shadow_outcome(
            "blocked-setup",
            "miss",
            "hard_reject",
            0.25,
        )
        self.assertEqual(first, second)
        self.assertFalse(
            should_track_shadow_outcome(
                "hard-disabled",
                "miss",
                "hard_reject",
                0.0,
            )
        )
        self.assertTrue(
            should_track_shadow_outcome(
                "near-full",
                "miss",
                "near_threshold",
                0.10,
            )
        )
        self.assertFalse(
            should_track_shadow_outcome(
                "near-disabled",
                "miss",
                "near_threshold",
                1.0,
                0.0,
            )
        )

    def test_compact_signal_rows_keeps_only_first_full_breakdown(self):
        rows = [
            {
                "id": str(index),
                "horizon_breakdown": [
                    {
                        "horizon_sec": 60,
                        "signal_confidence": 0.6,
                        "projected_net_pips": 0.2,
                        "raw_probability_up": 0.57,
                        "raw_ensemble_signed_net_pips": 0.4,
                        "raw_ensemble_aligned_weight_pct": 72.0,
                        "all_contributor_gross_to_spread": 8.0,
                        "directional_gross_to_spread": 1.5,
                        "large_diagnostic": {"values": list(range(50))},
                    }
                ],
            }
            for index in range(2)
        ]

        compact = PracticeExecutor.compact_signal_rows(rows)

        self.assertIn(
            "large_diagnostic",
            compact[0]["horizon_breakdown"][0],
        )
        self.assertNotIn(
            "large_diagnostic",
            compact[1]["horizon_breakdown"][0],
        )
        self.assertEqual(
            compact[1]["horizon_breakdown"][0]["horizon_sec"],
            60,
        )
        self.assertEqual(
            compact[1]["horizon_breakdown"][0]["raw_probability_up"],
            0.57,
        )
        self.assertEqual(
            compact[1]["horizon_breakdown"][0][
                "raw_ensemble_signed_net_pips"
            ],
            0.4,
        )
        self.assertEqual(
            compact[1]["horizon_breakdown"][0][
                "directional_gross_to_spread"
            ],
            1.5,
        )

    def test_compact_execution_log_rows_omits_repeated_matrix_breakdown(self):
        rows = [
            {
                "id": "signal-1",
                "instrument": "USD_JPY",
                "direction": "sell",
                "signal_confidence": 0.61,
                "signal_eligible": False,
                "execution_horizon_sec": 3600,
                "agreement_archetypes": ["trend_momentum"],
                "horizon_breakdown": [{"large": list(range(500))}],
                "paper_consensus_evidence": {"large": list(range(500))},
                "prediction_quality": {"large": list(range(500))},
            }
        ]

        compact = PracticeExecutor.compact_execution_log_rows(rows)

        self.assertEqual(compact[0]["id"], "signal-1")
        self.assertEqual(compact[0]["instrument"], "USD_JPY")
        self.assertEqual(compact[0]["direction"], "sell")
        self.assertEqual(compact[0]["signal_confidence"], 0.61)
        self.assertIs(compact[0]["signal_eligible"], False)
        self.assertNotIn("horizon_breakdown", compact[0])
        self.assertNotIn("paper_consensus_evidence", compact[0])
        self.assertNotIn("prediction_quality", compact[0])

    def test_trailing_distance_respects_broker_minimum_and_precision(self):
        distance, effective_pips = normalized_trailing_distance(
            configured_pips=2.5,
            pip=0.0001,
            minimum_price_distance=0.0005,
            precision=5,
        )
        self.assertEqual(distance, 0.0005)
        self.assertEqual(effective_pips, 5.0)

    def test_profit_lock_is_directional_monotonic_and_market_buffered(self):
        buy = profit_lock_stop_price(
            "buy", 1.10000, 3.0, 0.0001, 2.5, 2.0, 0.2, 0.8, 1.5,
            1.09950, 5, 0.25,
        )
        sell = profit_lock_stop_price(
            "sell", 1.10000, 3.0, 0.0001, 2.5, 2.0, 0.2, 0.8, 1.5,
            1.10050, 5, 0.25,
        )

        self.assertEqual(buy["price"], 1.10005)
        self.assertAlmostEqual(buy["lock_pips"], 0.5)
        self.assertEqual(sell["price"], 1.09995)
        self.assertAlmostEqual(sell["lock_pips"], 0.5)
        self.assertIsNone(
            profit_lock_stop_price(
                "buy", 1.10000, 3.0, 0.0001, 2.5, 2.0, 0.2, 0.8, 1.5,
                1.10005, 5, 0.25,
            )
        )

    def test_flat_executor_skips_periodic_account_poll(self):
        class NoPollClient:
            def get(self, *args, **kwargs):
                raise AssertionError("flat executor should not poll open trades")

        executor = PracticeExecutor(
            NoPollClient(),
            "test",
            Path("unused.jsonl"),
            LanePerformance(),
            SimpleNamespace(),
        )
        self.assertEqual(executor.manage_open_trades(), [])

    def test_execution_lock_serializes_account_writers(self):
        with tempfile.TemporaryDirectory() as temporary:
            args = SimpleNamespace(execution_lock_path=Path(temporary) / "order.lock")
            first = PracticeExecutor(None, "test", Path(temporary) / "first.jsonl", LanePerformance(), args)
            second = PracticeExecutor(None, "test", Path(temporary) / "second.jsonl", LanePerformance(), args)
            descriptor = first.acquire_execution_lock(timeout_sec=0.1)
            self.assertIsNotNone(descriptor)
            self.assertIsNone(second.acquire_execution_lock(timeout_sec=0.1))
            first.release_execution_lock(descriptor)
            replacement = second.acquire_execution_lock(timeout_sec=0.1)
            self.assertIsNotNone(replacement)
            second.release_execution_lock(replacement)

    def test_lane_rank_uses_only_matured_signal_horizon(self):
        ranking = LanePerformance(horizon_sec=300)
        for value in (1.0, 0.5, -0.5):
            ranking.observe("momentum.fast", 300, value, "signal")
        ranking.observe("momentum.fast", 60, 20.0, "signal")
        ranking.observe("momentum.fast", 300, 20.0, "miss")
        for value in (-1.0, -2.0, -3.0):
            ranking.observe("pullback.fast", 300, value, "signal")
        top = ranking.top(min_samples=3, limit=2)
        self.assertEqual([row["lane_id"] for row in top], ["momentum.fast", "pullback.fast"])
        self.assertEqual(top[0]["n"], 3)
        self.assertLess(top[0]["lower_confidence"], top[0]["avg"])
        self.assertEqual(top[0]["score"], top[0]["lower_confidence"])

    def test_recent_negative_lane_veto_rotates_clear_live_loser(self):
        ranking = LanePerformance(horizon_sec=300)
        for value in (-2.0, -1.0, -3.0, -0.5, -1.5):
            ranking.observe("momentum.fast", 300, value, "signal")

        evidence = ranking.recent_negative_veto("momentum.fast")

        self.assertTrue(evidence["veto"])
        self.assertFalse(evidence["positive_ready"])
        self.assertEqual(evidence["n"], 5)
        for value in (20.0, 3.0, 3.0, 3.0, 3.0):
            ranking.observe("momentum.fast", 300, value, "signal")
        recovered = ranking.recent_negative_veto("momentum.fast")
        self.assertFalse(recovered["veto"])
        self.assertTrue(recovered["positive_ready"])

    def test_execution_validation_rejects_small_negative_history(self):
        validation = PracticeExecutor.candidate_validation_state(
            {
                "historical_reliability": 0.30,
                "historical_win_probability": 0.60,
                "sample_adjusted_historical_edge_pips": 0.5,
                "promotion_evidence": {
                    "raw": {"n": 7, "avg": -1.5, "win_rate": 14.3}
                },
            },
            {"positive_ready": True},
        )

        self.assertFalse(validation["validated"])
        self.assertTrue(validation["negative_historical_warmup"])

    def test_paper_consensus_is_shadow_only_and_cannot_validate(self):
        validation = PracticeExecutor.candidate_validation_state(
            {
                "historical_reliability": 0.0,
                "historical_win_probability": 0.0,
                "sample_adjusted_historical_edge_pips": 0.0,
                "paper_consensus_evidence": {
                    "eligible": True,
                    "account_scope": "practice_007_only",
                },
            },
            {"positive_ready": False},
        )

        self.assertFalse(validation["validated"])
        self.assertTrue(validation["paper_consensus_ready"])
        self.assertTrue(validation["paper_consensus_shadow_only"])

    def test_execution_validation_accepts_replicated_positive_history(self):
        validation = PracticeExecutor.candidate_validation_state(
            {
                "historical_reliability": 0.30,
                "historical_win_probability": 0.56,
                "sample_adjusted_historical_edge_pips": 0.4,
                "promotion_evidence": {
                    "raw": {"n": 35, "avg": 0.5, "win_rate": 57.1}
                },
            },
            {"positive_ready": False},
        )

        self.assertTrue(validation["validated"])
        self.assertTrue(validation["historical_ready"])

    def test_execution_validation_rejects_explicitly_unpromoted_history(self):
        validation = PracticeExecutor.candidate_validation_state(
            {
                "historical_reliability": 0.41,
                "historical_win_probability": 0.504,
                "sample_adjusted_historical_edge_pips": 0.12,
                "promotion_evidence": {
                    "eligible": False,
                    "raw": {"n": 177, "avg": -1.06, "win_rate": 44.63},
                },
            },
            {"positive_ready": False},
        )

        self.assertFalse(validation["validated"])
        self.assertFalse(validation["historical_ready"])
        self.assertTrue(validation["negative_historical_warmup"])
        self.assertTrue(validation["promotion_rejected"])

    def test_signal_agreement_reports_raw_and_independent_breadth(self):
        rows = PracticeExecutor.add_signal_agreement(
            [
                {
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "family": "momentum",
                    "lane_id": "momentum.fast",
                },
                {
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "family": "ema_trend_cross",
                    "lane_id": "ema_trend_cross.balanced",
                },
                {
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "family": "donchian_breakout",
                    "lane_id": "donchian_breakout.fast",
                },
                {
                    "instrument": "EUR_USD",
                    "direction": "sell",
                    "family": "bollinger_reversion",
                    "lane_id": "bollinger_reversion.fast",
                },
            ]
        )

        buy = rows[0]
        self.assertEqual(buy["agreement_family_count"], 3)
        self.assertEqual(buy["opposing_family_count"], 1)
        self.assertEqual(buy["agreement_archetype_count"], 2)
        self.assertEqual(buy["opposing_archetype_count"], 1)
        self.assertEqual(
            buy["agreement_archetypes"],
            ["breakout_expansion", "trend_momentum"],
        )
        self.assertTrue(buy["strategy_independence_shadow_only"])

    def test_practice_order_has_signed_units_and_attached_bounds(self):
        candidate = {
            "direction": "buy",
            "instrument": "EUR_USD",
            "lane_id": "momentum.fast",
            "pip": 0.0001,
            "bid": 1.1000,
            "ask": 1.1002,
            "stop_loss_pips": 5.0,
            "take_profit_r": 1.5,
        }
        order = build_practice_order(candidate, 100, 0.5, "test-client-id")["order"]
        self.assertEqual(order["units"], "100")
        self.assertEqual(order["positionFill"], "OPEN_ONLY")
        self.assertEqual(order["priceBound"], "1.10025")
        self.assertEqual(order["stopLossOnFill"]["price"], "1.09970")
        self.assertEqual(order["takeProfitOnFill"]["price"], "1.10095")
        self.assertEqual(order["clientExtensions"]["tag"], "strategy_lab_top")

        candidate["direction"] = "sell"
        sell = build_practice_order(candidate, 100, 0.5, "test-sell-id")["order"]
        self.assertEqual(sell["units"], "-100")
        self.assertEqual(sell["priceBound"], "1.09995")
        self.assertEqual(sell["stopLossOnFill"]["price"], "1.10050")
        self.assertEqual(sell["takeProfitOnFill"]["price"], "1.09925")

    def test_open_ended_order_keeps_stop_and_encodes_staged_trailing(self):
        candidate = {
            "direction": "buy",
            "instrument": "EUR_USD",
            "lane_id": "momentum.fast",
            "pip": 0.0001,
            "bid": 1.1000,
            "ask": 1.1002,
            "stop_loss_pips": 5.0,
            "take_profit_r": 1.5,
            "execution_horizon_sec": 300,
            "open_ended_profit": True,
            "trailing_activation_pips": 4.5,
            "trailing_distance_pips": 3.0,
            "signal_confidence": 0.61,
        }
        order = build_practice_order(candidate, 250, 0.5, "test-open-ended")["order"]
        self.assertIn("stopLossOnFill", order)
        self.assertNotIn("takeProfitOnFill", order)
        self.assertIn("|ta4.50|td3.00|c0.610", order["tradeClientExtensions"]["comment"])

    def test_shared_signal_feed_merges_independent_producers(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "feed.sqlite"
            first = SharedSignalFeed(path)
            second = SharedSignalFeed(path)
            first.publish([{"id": "minute", "family": "momentum"}], "strategy", 10.0)
            second.publish([{"id": "second", "family": "ridge"}], "ridge", 10.0)
            ids = {row["id"] for row in first.recent()}
            first.close()
            second.close()
        self.assertEqual(ids, {"minute", "second"})

    def test_ranked_signals_consolidate_pair_and_preserve_horizon_curve(self):
        candidates = [
            {
                "id": "aud-m5",
                "instrument": "AUD_USD",
                "direction": "buy",
                "family": "momentum",
                "lane_id": "momentum.fast",
                "model_id": "momentum.fast",
                "input_timeframe": "M1",
                "signal_role": "structural",
                "execution_horizon_sec": 300,
                "signal_eligible": True,
                "signal_confidence": 0.58,
                "signal_score": 0.14,
                "projected_net_pips": 0.24,
                "projected_net_pips_per_hour": 2.88,
                "signal_horizon_curve": [
                    {
                        "horizon_sec": 300,
                        "signal_eligible": True,
                        "signal_confidence": 0.58,
                        "signal_score": 0.14,
                        "projected_net_pips": 0.24,
                        "projected_net_pips_per_hour": 2.88,
                    },
                    {
                        "horizon_sec": 1800,
                        "signal_eligible": True,
                        "signal_confidence": 0.57,
                        "signal_score": 0.10,
                        "projected_net_pips": 0.50,
                        "projected_net_pips_per_hour": 1.0,
                    },
                ],
            },
            {
                "id": "aud-m15",
                "instrument": "AUD_USD",
                "direction": "buy",
                "family": "higher_timeframe_alignment",
                "lane_id": "higher_timeframe_alignment.balanced",
                "model_id": "higher_timeframe_alignment.balanced",
                "input_timeframe": "M15",
                "signal_role": "structural",
                "execution_horizon_sec": 900,
                "signal_eligible": True,
                "signal_confidence": 0.61,
                "signal_score": 0.16,
                "projected_net_pips": 0.32,
                "projected_net_pips_per_hour": 1.28,
                "signal_horizon_curve": [
                    {
                        "horizon_sec": 900,
                        "signal_eligible": True,
                        "signal_confidence": 0.61,
                        "signal_score": 0.16,
                        "projected_net_pips": 0.32,
                        "projected_net_pips_per_hour": 1.28,
                    },
                    {
                        "horizon_sec": 1800,
                        "signal_eligible": True,
                        "signal_confidence": 0.60,
                        "signal_score": 0.15,
                        "projected_net_pips": 0.60,
                        "projected_net_pips_per_hour": 1.2,
                    },
                ],
            },
        ]
        consolidated = PracticeExecutor.consolidate_ranked_signals(candidates)
        self.assertEqual(len(consolidated), 1)
        signal = consolidated[0]
        self.assertEqual(signal["instrument"], "AUD_USD")
        self.assertEqual(signal["component_horizons_sec"], [300, 900, 1800])
        self.assertEqual(signal["component_family_count"], 2)
        self.assertEqual(signal["component_archetype_count"], 1)
        self.assertEqual(signal["component_archetypes"], ["trend_momentum"])
        shared = next(row for row in signal["horizon_breakdown"] if row["horizon_sec"] == 1800)
        steps = {row["input_timeframe"]: row["steps_ahead"] for row in shared["contributors"]}
        self.assertEqual(steps, {"M15": 2.0, "M1": 30.0})
        self.assertEqual(shared["best_input_timeframe"], "M15")

    def test_timing_only_signal_cannot_open_account_position(self):
        signal = PracticeExecutor.consolidate_ranked_signals(
            [
                {
                    "id": "s1-only",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "family": "ridge_return",
                    "model_id": "ridge_return.s1.EUR_USD.h60",
                    "input_timeframe": "S1",
                    "signal_role": "entry_exit_timing",
                    "execution_horizon_sec": 60,
                    "signal_eligible": True,
                    "signal_confidence": 0.60,
                    "signal_score": 0.20,
                    "projected_net_pips": 0.20,
                }
            ]
        )[0]
        self.assertFalse(signal["signal_eligible"])
        self.assertIn("structural_signal_required", signal["signal_blocked_by"])

    def test_all_models_and_opposing_signals_contribute_to_final_matrix_cell(self):
        rows = [
            {
                "id": "buy-fast",
                "instrument": "EUR_USD",
                "direction": "buy",
                "family": "momentum",
                "lane_id": "momentum.fast",
                "model_id": "momentum.fast",
                "input_timeframe": "M1",
                "signal_role": "structural",
                "execution_horizon_sec": 300,
                "signal_eligible": True,
                "signal_confidence": 0.70,
                "signal_score": 0.30,
                "normalized_rank_score": 0.30,
                "projected_net_pips": 1.0,
                "historical_reliability": 1.0,
            },
            {
                "id": "buy-balanced",
                "instrument": "EUR_USD",
                "direction": "buy",
                "family": "momentum",
                "lane_id": "momentum.balanced",
                "model_id": "momentum.balanced",
                "input_timeframe": "M5",
                "signal_role": "structural",
                "execution_horizon_sec": 300,
                "signal_eligible": True,
                "signal_confidence": 0.60,
                "signal_score": 0.20,
                "normalized_rank_score": 0.20,
                "projected_net_pips": 0.5,
                "historical_reliability": 1.0,
            },
            {
                "id": "sell-reversal",
                "instrument": "EUR_USD",
                "direction": "sell",
                "family": "reversal",
                "lane_id": "reversal.fast",
                "model_id": "reversal.fast",
                "input_timeframe": "M15",
                "signal_role": "structural",
                "execution_horizon_sec": 300,
                "signal_eligible": False,
                "signal_confidence": 0.65,
                "signal_score": 0.10,
                "normalized_rank_score": 0.10,
                "projected_net_pips": 0.4,
                "historical_reliability": 1.0,
            },
        ]

        signal = PracticeExecutor.consolidate_ranked_signals(rows)[0]
        cell = signal["horizon_breakdown"][0]

        self.assertEqual(cell["component_count"], 3)
        self.assertEqual(cell["timeframe_count"], 3)
        self.assertEqual(
            {row["model_id"] for row in cell["contributors"]},
            {"momentum.fast", "momentum.balanced", "reversal.fast"},
        )
        self.assertEqual(signal["direction"], "buy")
        self.assertLess(cell["signal_confidence"], 0.70)
        self.assertEqual(
            cell["aggregation_method"],
            "family_capped_reliability_weighted_consensus",
        )

    def test_horizon_lineage_and_shadow_only_conflict_basis_are_explicit(self):
        rows = [
            {
                "id": "sell-breakout",
                "instrument": "GBP_USD",
                "direction": "sell",
                "family": "breakout_retest",
                "lane_id": "breakout_retest.fast",
                "model_id": "breakout_retest.fast",
                "input_timeframe": "M1",
                "signal_role": "structural",
                "execution_horizon_sec": 300,
                "signal_eligible": True,
                "account_eligible": True,
                "signal_confidence": 0.75,
                "signal_score": 0.4,
                "normalized_rank_score": 0.4,
                "projected_net_pips": 2.0,
                "historical_reliability": 1.0,
                "signal_candle_time": "2026-09-01T17:00:00+00:00",
            },
            {
                "id": "sell-strength",
                "instrument": "GBP_USD",
                "direction": "sell",
                "family": "currency_strength",
                "lane_id": "currency_strength.fast",
                "model_id": "currency_strength.fast",
                "input_timeframe": "M1",
                "signal_role": "structural",
                "execution_horizon_sec": 300,
                "signal_eligible": True,
                "account_eligible": True,
                "signal_confidence": 0.72,
                "signal_score": 0.35,
                "normalized_rank_score": 0.35,
                "projected_net_pips": 1.8,
                "historical_reliability": 1.0,
                "signal_candle_time": "2026-09-01T17:00:00+00:00",
            },
            {
                "id": "inverse-veto",
                "instrument": "GBP_USD",
                "direction": "buy",
                "family": "inverse_correlation_veto",
                "lane_id": "inverse_correlation_veto.fast",
                "model_id": "inverse_correlation_veto.fast",
                "input_timeframe": "M1",
                "signal_role": "structural",
                "execution_horizon_sec": 300,
                "signal_eligible": False,
                "account_eligible": False,
                "research_only": True,
                "research_blocked_reason": "account_ineligible",
                "signal_confidence": 0.60,
                "signal_score": 0.1,
                "normalized_rank_score": 0.1,
                "projected_net_pips": 0.5,
                "historical_reliability": 1.0,
                "signal_candle_time": "2026-09-01T17:00:00+00:00",
            },
        ]

        signal = PracticeExecutor.consolidate_ranked_signals(rows)[0]
        repeated = PracticeExecutor.consolidate_ranked_signals(rows)[0]
        cell = signal["horizon_breakdown"][0]

        self.assertEqual(cell["direction"], "sell")
        self.assertTrue(cell["aggregate_signal_id"].startswith("aggregate_signal_"))
        self.assertEqual(
            cell["aggregate_signal_id"],
            repeated["horizon_breakdown"][0]["aggregate_signal_id"],
        )
        self.assertEqual(signal["aggregate_signal_id"], cell["aggregate_signal_id"])
        self.assertTrue(signal["direction_conflict"])
        self.assertEqual(signal["direction_conflict_contributor_count"], 1)
        self.assertEqual(signal["direction_conflict_account_eligible_count"], 0)
        self.assertEqual(signal["direction_conflict_execution_component_count"], 0)
        self.assertEqual(signal["direction_conflict_shadow_only_count"], 1)
        self.assertTrue(
            signal["direction_conflict_only_shadow_or_account_ineligible"]
        )
        self.assertEqual(
            signal["direction_conflict_contributors"][0]["family"],
            "inverse_correlation_veto",
        )
        changed = [dict(row) for row in rows]
        changed[2]["id"] = "inverse-veto-new-cohort"
        changed_id = PracticeExecutor.consolidate_ranked_signals(changed)[0][
            "aggregate_signal_id"
        ]
        self.assertNotEqual(changed_id, signal["aggregate_signal_id"])

    def test_many_timeframes_from_one_family_do_not_flood_consensus(self):
        rows = [
            {
                "id": f"ma-{index}",
                "instrument": "USD_NOK",
                "direction": "sell",
                "family": "moving_average_feature_grid",
                "model_id": f"ma-{index}",
                "input_timeframe": f"M{index + 1}",
                "signal_role": "structural",
                "execution_horizon_sec": 300,
                "signal_eligible": False,
                "account_eligible": True,
                "signal_confidence": 0.55,
                "projected_net_pips": 1.0,
                "historical_reliability": 1.0,
            }
            for index in range(20)
        ]
        rows.append(
            {
                "id": "currency-factor",
                "instrument": "USD_NOK",
                "direction": "buy",
                "family": "currency_strength",
                "model_id": "currency-strength",
                "input_timeframe": "M1",
                "signal_role": "structural",
                "execution_horizon_sec": 300,
                "signal_eligible": False,
                "account_eligible": True,
                "signal_confidence": 0.70,
                "projected_net_pips": 1.0,
                "historical_reliability": 1.0,
            }
        )

        cell = PracticeExecutor.consolidate_ranked_signals(rows)[0][
            "horizon_breakdown"
        ][0]
        family_weights = Counter()
        for contributor in cell["contributors"]:
            family_weights[contributor["family"]] += contributor[
                "raw_matrix_weight"
            ]

        self.assertEqual(cell["direction"], "buy")
        self.assertAlmostEqual(
            family_weights["moving_average_feature_grid"],
            family_weights["currency_strength"],
        )

    def test_directional_cost_ratio_excludes_opposing_contributors(self):
        rows = [
            {
                "id": "buy-trend",
                "instrument": "EUR_USD",
                "direction": "buy",
                "family": "momentum",
                "model_id": "momentum.fast",
                "input_timeframe": "M5",
                "signal_role": "structural",
                "execution_horizon_sec": 7200,
                "signal_eligible": True,
                "account_eligible": True,
                "signal_confidence": 0.72,
                "signal_score": 0.25,
                "normalized_rank_score": 0.25,
                "projected_net_pips": 3.0,
                "gross_to_spread": 1.5,
                "historical_reliability": 1.0,
            },
            {
                "id": "sell-reversal",
                "instrument": "EUR_USD",
                "direction": "sell",
                "family": "reversal",
                "model_id": "reversal.fast",
                "input_timeframe": "M15",
                "signal_role": "structural",
                "execution_horizon_sec": 7200,
                "signal_eligible": False,
                "account_eligible": True,
                "signal_confidence": 0.53,
                "signal_score": 0.01,
                "normalized_rank_score": 0.01,
                "projected_net_pips": 0.1,
                "gross_to_spread": 20.0,
                "historical_reliability": 0.2,
            },
        ]

        cell = PracticeExecutor.consolidate_ranked_signals(rows)[0][
            "horizon_breakdown"
        ][0]

        self.assertEqual(cell["direction"], "buy")
        self.assertAlmostEqual(cell["directional_gross_to_spread"], 1.5)
        self.assertEqual(
            cell["gross_to_spread"], cell["all_contributor_gross_to_spread"]
        )
        self.assertGreater(
            cell["all_contributor_gross_to_spread"],
            cell["directional_gross_to_spread"],
        )

    def test_research_only_family_stays_raw_but_cannot_steer_filtered_consensus(self):
        rows = [
            {
                "id": "validated-buy",
                "instrument": "EUR_USD",
                "direction": "buy",
                "family": "currency_strength",
                "model_id": "currency_strength.validated",
                "input_timeframe": "M1",
                "signal_role": "structural",
                "execution_horizon_sec": 300,
                "signal_eligible": True,
                "account_eligible": True,
                "signal_confidence": 0.60,
                "projected_net_pips": 0.8,
                "historical_reliability": 1.0,
            },
            {
                "id": "research-sell",
                "instrument": "EUR_USD",
                "direction": "sell",
                "family": "moving_average_feature_grid",
                "model_id": "moving_average_feature_grid.h8",
                "input_timeframe": "H8",
                "signal_role": "structural",
                "execution_horizon_sec": 300,
                "signal_eligible": False,
                "account_eligible": False,
                "research_only": True,
                "signal_confidence": 0.99,
                "projected_net_pips": 10.0,
                "historical_reliability": 1.0,
            },
        ]

        cell = PracticeExecutor.consolidate_ranked_signals(rows)[0][
            "horizon_breakdown"
        ][0]
        contributors = {
            row["family"]: row for row in cell["contributors"]
        }

        self.assertEqual(cell["direction"], "buy")
        self.assertEqual(cell["direction_state"], "buy")
        self.assertLess(cell["raw_probability_up"], 0.5)
        self.assertGreater(cell["filtered_probability_up"], 0.5)
        self.assertGreater(
            contributors["moving_average_feature_grid"]["raw_matrix_weight"],
            0.0,
        )
        self.assertEqual(
            contributors["moving_average_feature_grid"]["matrix_weight"],
            0.0,
        )
        self.assertEqual(cell["research_only_component_count"], 1)

    def test_research_only_surface_is_neutral_after_filtering(self):
        cell = PracticeExecutor.consolidate_ranked_signals(
            [
                {
                    "id": "research-only",
                    "instrument": "USD_JPY",
                    "direction": "sell",
                    "family": "timeframe_equation_matrix",
                    "model_id": "timeframe_equation_matrix.m1",
                    "input_timeframe": "M1",
                    "signal_role": "structural",
                    "execution_horizon_sec": 300,
                    "signal_eligible": False,
                    "account_eligible": False,
                    "research_only": True,
                    "signal_confidence": 0.90,
                    "projected_net_pips": 2.0,
                }
            ]
        )[0]["horizon_breakdown"][0]

        self.assertEqual(cell["direction_state"], "neutral")
        self.assertFalse(cell["filtered_surface_available"])
        self.assertEqual(cell["filtered_probability_up"], 0.5)
        self.assertEqual(cell["ensemble_signed_net_pips"], 0.0)
        self.assertIn(
            "ensemble_no_filtered_structural_support",
            cell["signal_blocked_by"],
        )

    def test_negative_quality_veto_stays_raw_but_not_filtered(self):
        rows = [
            {
                "id": "healthy-buy",
                "instrument": "GBP_USD",
                "direction": "buy",
                "family": "currency_strength",
                "model_id": "currency_strength.fast",
                "input_timeframe": "M1",
                "signal_role": "structural",
                "execution_horizon_sec": 300,
                "signal_eligible": True,
                "account_eligible": True,
                "signal_confidence": 0.58,
                "projected_net_pips": 0.5,
                "historical_reliability": 1.0,
            },
            {
                "id": "failed-sell",
                "instrument": "GBP_USD",
                "direction": "sell",
                "family": "momentum",
                "model_id": "momentum.failed",
                "input_timeframe": "M1",
                "signal_role": "structural",
                "execution_horizon_sec": 300,
                "signal_eligible": False,
                "account_eligible": True,
                "signal_confidence": 0.99,
                "projected_net_pips": 8.0,
                "historical_reliability": 1.0,
                "prediction_quality": {"negative_evidence": True},
            },
        ]

        cell = PracticeExecutor.consolidate_ranked_signals(rows)[0][
            "horizon_breakdown"
        ][0]
        failed = next(
            row
            for row in cell["contributors"]
            if row["model_id"] == "momentum.failed"
        )

        self.assertEqual(cell["direction"], "buy")
        self.assertLess(cell["raw_probability_up"], 0.5)
        self.assertGreater(cell["filtered_probability_up"], 0.5)
        self.assertGreater(failed["raw_matrix_weight"], 0.0)
        self.assertEqual(failed["matrix_weight"], 0.0)

    def test_negative_component_edge_cannot_become_inverse_profit(self):
        rows = [
            {
                "id": "losing-buy",
                "instrument": "EUR_USD",
                "direction": "buy",
                "family": "momentum",
                "model_id": "momentum.fast",
                "input_timeframe": "M1",
                "signal_role": "structural",
                "execution_horizon_sec": 60,
                "signal_eligible": False,
                "signal_confidence": 0.70,
                "signal_score": -0.20,
                "normalized_rank_score": -0.20,
                "projected_net_pips": -2.0,
                "historical_reliability": 1.0,
            },
            {
                "id": "losing-sell",
                "instrument": "EUR_USD",
                "direction": "sell",
                "family": "reversal",
                "model_id": "reversal.fast",
                "input_timeframe": "M5",
                "signal_role": "structural",
                "execution_horizon_sec": 60,
                "signal_eligible": False,
                "signal_confidence": 0.60,
                "signal_score": -0.10,
                "normalized_rank_score": -0.10,
                "projected_net_pips": -1.0,
                "historical_reliability": 1.0,
            },
        ]

        cell = PracticeExecutor.consolidate_ranked_signals(rows)[0][
            "horizon_breakdown"
        ][0]

        self.assertEqual(cell["projected_net_pips"], 0.0)
        self.assertEqual(cell["ensemble_signed_net_pips"], 0.0)

    def test_rejected_setup_has_reduced_vote_and_remains_in_finality_count(self):
        accepted = {
            "id": "accepted-buy",
            "instrument": "EUR_USD",
            "direction": "buy",
            "family": "momentum",
            "model_id": "momentum.fast",
            "input_timeframe": "M1",
            "signal_role": "structural",
            "execution_horizon_sec": 300,
            "signal_eligible": True,
            "account_eligible": True,
            "signal_confidence": 0.65,
            "projected_net_pips": 0.8,
            "matrix_input_weight": 1.0,
            "preconsensus_class": "accepted",
        }
        rejected = {
            "id": "rejected-sell",
            "instrument": "EUR_USD",
            "direction": "sell",
            "family": "reversal",
            "model_id": "reversal.loose",
            "input_timeframe": "M5",
            "signal_role": "structural",
            "execution_horizon_sec": 300,
            "signal_eligible": False,
            "account_eligible": True,
            "signal_confidence": 0.90,
            "projected_net_pips": 1.5,
            "matrix_input_weight": 0.10,
            "preconsensus_class": "hard_reject",
        }

        cell = PracticeExecutor.consolidate_ranked_signals(
            [accepted, rejected]
        )[0]["horizon_breakdown"][0]

        self.assertEqual(cell["component_count"], 2)
        self.assertEqual(
            cell["setup_class_counts"],
            {"accepted": 1, "hard_reject": 1},
        )
        weights = {
            row["preconsensus_class"]: row["matrix_weight"]
            for row in cell["contributors"]
        }
        self.assertGreater(weights["accepted"], weights["hard_reject"])
        self.assertEqual(cell["direction"], "buy")

    def test_second_curve_is_timing_evidence_for_structural_side(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "second.json"
            path.write_text(
                json.dumps(
                    {
                        "forecast_curves": [
                            {
                                "instrument": "AUD_USD",
                                "points": [
                                    {
                                        "horizon_sec": horizon,
                                        "probability_up": 0.44,
                                        "predicted_signed_pips": -0.2,
                                    }
                                    for horizon in (60, 180, 300, 600)
                                ],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            args = SimpleNamespace(
                execution_second_forecast_state=path,
                execution_second_curve_max_age_sec=30.0,
                execution_second_curve_max_horizon_sec=300,
                execution_second_curve_min_points=3,
                execution_second_curve_oppose_probability=0.47,
            )
            executor = PracticeExecutor(None, "test", Path("unused.jsonl"), LanePerformance(), args)
            buy = executor.second_curve_state("AUD_USD", "buy")
            sell = executor.second_curve_state("AUD_USD", "sell")
        self.assertEqual(buy["state"], "opposed")
        self.assertEqual(sell["state"], "aligned")
        self.assertEqual(buy["points"], 3)

    def test_second_curve_blocks_direction_that_cannot_clear_spread(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "second.json"
            path.write_text(
                json.dumps(
                    {
                        "forecast_curves": [
                            {
                                "instrument": "EUR_USD",
                                "points": [
                                    {
                                        "horizon_sec": horizon,
                                        "probability_up": 0.58,
                                        "predicted_signed_pips": 0.4,
                                        "spread_pips": 1.2,
                                    }
                                    for horizon in (30, 60, 120)
                                ],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            args = SimpleNamespace(
                execution_second_forecast_state=path,
                execution_second_curve_max_age_sec=30.0,
                execution_second_curve_max_horizon_sec=300,
                execution_second_curve_min_points=3,
                execution_second_curve_oppose_probability=0.47,
                execution_second_curve_min_net_pips=0.05,
                execution_second_curve_min_aligned_fraction=0.5,
            )
            executor = PracticeExecutor(None, "test", Path("unused.jsonl"), LanePerformance(), args)
            state = executor.second_curve_state("EUR_USD", "buy")
        self.assertEqual(state["state"], "cost_blocked")
        self.assertLess(state["directional_net_pips"], 0.0)

    def test_prediction_quality_calibrates_positive_evidence_and_vetoes_negative(self):
        class QualityFit:
            def __init__(self, evidence):
                self.evidence = evidence

            def quality_evidence(self, *args, **kwargs):
                return dict(self.evidence)

        args = SimpleNamespace(
            execution_prediction_quality=True,
            execution_prediction_quality_min_samples=30,
            execution_prediction_quality_max_weight=0.75,
            execution_prediction_quality_negative_veto=True,
            execution_min_signal_confidence=0.54,
            execution_min_signal_expected_net_pips=0.05,
        )
        candidate = {
            "lane_id": "ridge.fast",
            "family": "ridge",
            "model_id": "ridge.s1.EUR_USD.h60",
            "input_timeframe": "S1",
            "instrument": "EUR_USD",
            "direction": "buy",
            "execution_horizon_sec": 60,
            "entry_time": "2026-07-17T13:00:00Z",
            "signal_confidence": 0.56,
            "projected_net_pips": 0.2,
            "signal_eligible": True,
        }
        positive = {
            "eligible": True,
            "negative_evidence": False,
            "sample_count": 100,
            "evidence_strength": 0.5,
            "calibrated_positive_probability": 0.7,
            "calibrated_expected_net_pips": 1.0,
            "holdout": {"median_time_to_positive_sec": 30.0},
        }
        executor = PracticeExecutor(
            None,
            "test",
            Path("unused.jsonl"),
            LanePerformance(),
            args,
            exit_fit=QualityFit(positive),
        )
        row = dict(candidate)
        quality = executor.apply_prediction_quality(row)
        self.assertGreater(row["signal_confidence"], candidate["signal_confidence"])
        self.assertGreater(row["projected_net_pips"], candidate["projected_net_pips"])
        self.assertGreater(quality["calibration_weight"], 0.0)

        executor.exit_fit = QualityFit(
            {
                **positive,
                "eligible": False,
                "negative_evidence": True,
            }
        )
        row = dict(candidate)
        executor.apply_prediction_quality(row)
        self.assertFalse(row["signal_eligible"])
        self.assertIn("prediction_quality_negative", row["signal_blocked_by"])

    def test_dynamic_sizing_increases_with_confidence_and_respects_margin(self):
        args = SimpleNamespace(
            execution_dynamic_sizing=True,
            execution_units=100,
            execution_min_signal_confidence=0.54,
            execution_max_units=5000,
        )
        executor = PracticeExecutor(None, "test", Path("unused.jsonl"), LanePerformance(), args)
        executor.account_currency = "USD"
        executor.instrument_meta = {
            "EUR_USD": {"margin_rate": 0.02, "minimum_trade_size": 1.0, "pip_location": -4.0}
        }
        candidate = {
            "instrument": "EUR_USD",
            "bid": 1.1000,
            "ask": 1.1002,
            "pip": 0.0001,
            "stop_loss_pips": 5.0,
        }
        summary = {"NAV": "50", "balance": "50", "marginUsed": "0", "marginAvailable": "50", "marginRate": "0.02"}
        low_units, low = executor.dynamic_sizing({**candidate, "signal_confidence": 0.54}, summary)
        high_units, high = executor.dynamic_sizing({**candidate, "signal_confidence": 0.72}, summary)
        self.assertGreater(high_units, low_units)
        self.assertLessEqual(high["estimated_margin"], 50 * 0.24 + 0.01)

    def test_margin_weighted_currency_concentration_blocks_shared_usd_side(self):
        args = SimpleNamespace(
            execution_max_currency_direction_margin_pct=45.0,
        )
        executor = PracticeExecutor(None, "test", Path("unused.jsonl"), LanePerformance(), args)
        trades = [
            {
                "instrument": "EUR_USD",
                "currentUnits": "1000",
                "initialMargin": "20",
            }
        ]
        reason, details = executor.portfolio_margin_blocker(
            {"instrument": "GBP_USD", "direction": "buy"},
            trades,
            {"NAV": "50"},
            {"estimated_margin": 5.0},
        )
        self.assertEqual(reason, "currency_direction_margin_concentration")
        self.assertEqual(
            details["projected_currency_direction_margin_pct"]["USD:short"],
            50.0,
        )

    def test_volatility_regime_uses_movement_relative_to_spread(self):
        self.assertEqual(classify_volatility_regime(1.0, 0.5), "compressed")
        self.assertEqual(classify_volatility_regime(3.0, 0.5), "normal")
        self.assertEqual(classify_volatility_regime(5.0, 0.5), "expanded")


class FamilySignalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lanes = {lane.lane_id: lane for lane in default_lanes()}

    def test_ema_trend_cross_is_distinct_and_profile_sensitive(self):
        closes = [1.0] * 58 + [0.9997, 1.0005]
        features = features_for(closes)
        self.assertEqual(
            evaluate_family(self.lanes["ema_trend_cross.balanced"], features).direction,
            "buy",
        )
        self.assertIsNone(
            evaluate_family(self.lanes["ema_trend_cross.strict"], features).direction
        )

    def test_pattern_count_lane_preserves_full_prediction(self):
        features = features_for([1.0 + index * 0.0001 for index in range(60)])
        forecast = {
            "pattern": "U D U",
            "decision_candle_time": "2026-07-13T22:00:00Z",
            "predicted_direction": "buy",
            "probability_up": 0.57,
            "expected_abs_move_pips": 1.8,
            "movement_coefficient": 1.25,
        }
        features["pattern_count_forecasts"] = {"sign:3:1": forecast}
        setup = evaluate_family(self.lanes["pattern_count_forecast.fast"], features)
        self.assertEqual(setup.direction, "buy")
        self.assertIs(setup.signal["pattern_forecast"], forecast)
        self.assertEqual(setup.signal["strength_pips"], 1.8)

    def test_bollinger_reentry_detects_reversal_after_lower_band_excursion(self):
        closes = [1.0] * 58 + [0.995, 1.0]
        setup = evaluate_family(
            self.lanes["bollinger_reversion.balanced"],
            features_for(closes),
        )
        self.assertEqual(setup.direction, "buy")
        self.assertIn("bb_lower", setup.signal)
        self.assertEqual(setup.signal["previous_rsi14"], 0.0)

    def test_currency_strength_requires_pair_and_currency_confirmation(self):
        features = features_for([1.0] * 54 + [1.0001, 1.0002, 1.0003, 1.0004, 1.0005, 1.0006])
        features.update(
            {
                "cross_sample_count": 6,
                "cross_strength_r3": 0.70,
                "cross_strength_r5": 0.80,
                "cross_breadth_r3": 0.50,
                "pair_norm_r1": 0.20,
                "pair_norm_r3": 0.60,
            }
        )
        setup = evaluate_family(self.lanes["currency_strength.balanced"], features)
        self.assertEqual(setup.direction, "buy")
        self.assertEqual(setup.signal["cross_sample_count"], 6)

    def test_relative_value_reversion_waits_for_reversal(self):
        features = features_for([1.0] * 54 + [0.9994, 0.9993, 0.9992, 0.9991, 0.9990, 0.9991])
        features.update(
            {
                "cross_sample_count": 6,
                "cross_strength_r3": 0.10,
                "relative_residual_r3": -1.20,
                "relative_residual_r5": -0.90,
                "pair_norm_r1": 0.20,
                "pos20": 0.30,
            }
        )
        setup = evaluate_family(self.lanes["relative_value_reversion.balanced"], features)
        self.assertEqual(setup.direction, "buy")

    def test_supervised_rank_uses_cost_aware_rank_and_validation(self):
        features = features_for([1.0] * 54 + [1.0001, 1.0002, 1.0003, 1.0004, 1.0005, 1.0006])
        features.update(
            {
                "supervised_ready": True,
                "supervised_direction": "buy",
                "supervised_expected_net_pips": 2.0,
                "supervised_long_net_pips": 2.0,
                "supervised_short_net_pips": -3.0,
                "supervised_rank_percentile": 0.99,
                "supervised_model_rows": 6000,
                "supervised_validation_rows": 1200,
                "supervised_validation_hit_rate": 0.50,
                "supervised_validation_avg_norm": 0.10,
                "supervised_model_candle_time": "2026-07-13T22:00:00Z",
            }
        )
        setup = evaluate_family(self.lanes["supervised_return_rank.strict"], features)
        self.assertEqual(setup.direction, "buy")
        self.assertEqual(setup.signal["decision_candle_time"], "2026-07-13T22:00:00Z")

    def test_higher_timeframe_alignment_keeps_fast_entry(self):
        closes = [1.0] * 58 + [0.9997, 1.0005]
        features = features_for(closes)
        features["m15_closes"] = [1.0 + index * 0.00005 for index in range(120)]
        features["h1_closes"] = [1.0 + index * 0.00020 for index in range(36)]
        setup = evaluate_family(self.lanes["higher_timeframe_alignment.loose"], features)
        self.assertEqual(setup.direction, "buy")


class ExpandedFamilySignalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lanes = {lane.lane_id: lane for lane in default_lanes()}

    def test_every_lane_evaluates_against_the_shared_feature_schema(self):
        features = features_for([1.0 + index * 0.00001 for index in range(60)])
        features.update(
            {
                "cross_pair_count": 68,
                "pair_rank_r3": 0.5,
                "pair_rank_r5": 0.5,
            }
        )
        for lane in self.lanes.values():
            with self.subTest(lane=lane.lane_id):
                self.assertTrue(evaluate_family(lane, features).reason)

    def test_research_upgrade_families_emit_on_distinct_synthetic_regimes(self):
        def closes_from_returns(returns: list[float]) -> list[float]:
            closes = [1.0]
            for value in returns:
                closes.append(closes[-1] + value * 0.0001)
            return closes

        scenarios = {
            "kalman_local_trend": ([0.04 + 0.01 * index for index in range(70)], "buy"),
            "markov_sign_transition": ([0.2, 0.1, 0.3] * 30, "buy"),
            "online_ar_forecast": ([0.45, -0.15] * 40 + [0.45], "sell"),
            "variance_ratio_regime": (
                [0.10, 0.16, 0.22, 0.28, 0.34, 0.28, 0.22, 0.16] * 10,
                "buy",
            ),
            "permutation_entropy_momentum": ([0.25] * 70, "buy"),
            "cusum_breakout": ([0.05, -0.05] * 25 + [0.05] * 12, "buy"),
            "har_volatility_momentum": (
                [0.05, -0.05] * 25 + [0.7, -0.5] * 8 + [0.7, 0.7, 0.7],
                "buy",
            ),
            "bipower_jump_reversal": ([0.1, -0.1] * 30 + [4.0, -0.5], "sell"),
            "garch_volatility_breakout": (
                [0.05, -0.05] * 25 + [1.0, -0.8] * 8 + [0.8, 0.8, 0.8],
                "buy",
            ),
            "hurst_regime_forecast": (
                ([0.25] * 12 + [-0.1] * 2) * 5 + [0.25] * 9 + [-0.4, -0.4, 0.2],
                "buy",
            ),
            "theil_sen_trend": ([0.12] * 70, "buy"),
        }
        for family, (returns, expected_direction) in scenarios.items():
            with self.subTest(family=family):
                setup = evaluate_family(
                    self.lanes[f"{family}.loose"],
                    features_for(closes_from_returns(returns)),
                )
                self.assertEqual(setup.direction, expected_direction)
                self.assertTrue(setup.signal["research_shadow"])
                self.assertIn("theory", setup.signal)
                self.assertIn("expected_signed_move_pips", setup.signal)

    def test_tick_vwap_reversion_emits_and_labels_volume_semantics(self):
        closes = [1.0] * 68 + [0.9975, 0.9976]
        features = features_for(closes)
        setup = evaluate_family(self.lanes["vwap_deviation_reversion.loose"], features)

        self.assertEqual(setup.direction, "buy")
        self.assertEqual(
            setup.signal["volume_semantics"],
            "oanda_tick_volume_not_exchange_volume",
        )
        self.assertLess(setup.signal["tick_vwap_deviation_pips"], 0.0)

    def test_new_york_vwap_family_is_session_gated_and_sell_only(self):
        closes = [1.0] * 68 + [1.0025, 1.0024]
        features = features_for(closes)
        features["candle_time"] = "2026-07-13T17:00:00Z"
        setup = evaluate_family(
            self.lanes["ny_session_vwap_sell_reversion.loose"],
            features,
        )

        self.assertEqual(setup.direction, "sell")
        self.assertEqual(setup.signal["target_horizon_sec"], 14400)
        self.assertEqual(setup.signal["side_constraint"], "sell_only")
        features["candle_time"] = "2026-07-13T23:00:00Z"
        outside = evaluate_family(
            self.lanes["ny_session_vwap_sell_reversion.loose"],
            features,
        )
        self.assertIsNone(outside.direction)
        self.assertEqual(outside.reason, "outside_new_york_session")

    def test_momentum_and_donchian_breakout_emit_independently(self):
        m5 = [1.0 + index * 0.0001 for index in range(210)]
        rising = [1.0 + index * 0.0001 for index in range(60)]
        self.assertEqual(
            evaluate_family(self.lanes["momentum.loose"], features_for(rising, m5_closes=m5)).direction,
            "buy",
        )

        breakout = [1.0] * 47 + [1.0 + index * 0.0001 for index in range(12)] + [1.0015]
        self.assertEqual(
            evaluate_family(self.lanes["donchian_breakout.loose"], features_for(breakout, m5_closes=m5)).direction,
            "buy",
        )

    def test_efficiency_filtered_momentum_and_stochastic_reversal_emit(self):
        m5 = [1.0 + index * 0.0001 for index in range(210)]
        efficient = [1.0] * 42 + [1.0 + index * 0.0001 for index in range(18)]
        self.assertEqual(
            evaluate_family(
                self.lanes["efficiency_filtered_momentum.loose"],
                features_for(efficient, m5_closes=m5),
            ).direction,
            "buy",
        )

        stochastic_tail = [
            1.0,
            1.0,
            1.0,
            0.9997,
            0.9994,
            0.9993,
            0.9990,
            0.9990,
            0.9989,
            0.9989,
            0.9988,
            0.9990,
        ]
        stochastic_closes = [1.0] * 48 + stochastic_tail
        stochastic = features_for(
            stochastic_closes,
            highs=[value + 0.0003 for value in stochastic_closes],
            lows=[value - 0.0003 for value in stochastic_closes],
        )
        stochastic["pos20"] = 0.3
        self.assertEqual(
            evaluate_family(self.lanes["stochastic_reversal.fast"], stochastic).direction,
            "buy",
        )

    def test_failed_breakout_range_and_volume_families_emit(self):
        impulse_closes = [1.0] * 59 + [1.0010]
        opens = list(impulse_closes)
        opens[-1] = 1.0
        highs = [value + 0.00005 for value in impulse_closes]
        lows = [value - 0.00005 for value in impulse_closes]
        highs[-1] = 1.00105
        lows[-1] = 0.99995
        impulse = features_for(
            impulse_closes,
            opens=opens,
            highs=highs,
            lows=lows,
            volumes=[100.0] * 59 + [250.0],
        )
        self.assertEqual(evaluate_family(self.lanes["range_expansion.loose"], impulse).direction, "buy")
        self.assertEqual(evaluate_family(self.lanes["volume_impulse.loose"], impulse).direction, "buy")

        failed = [1.0] * 59 + [1.0002]
        failed_opens = list(failed)
        failed_opens[-1] = 0.9994
        failed_highs = [value + 0.0001 for value in failed]
        failed_lows = [value - 0.0001 for value in failed]
        failed_lows[-1] = 0.9988
        failed_features = features_for(
            failed,
            opens=failed_opens,
            highs=failed_highs,
            lows=failed_lows,
        )
        failed_features["pos20"] = 0.25
        self.assertEqual(
            evaluate_family(self.lanes["failed_breakout_reversal.fast"], failed_features).direction,
            "buy",
        )

    def test_reversal_families_emit_from_distinct_patterns(self):
        engulfing_closes = [1.0] * 57 + [1.0010, 0.9995, 1.0012]
        opens = list(engulfing_closes)
        opens[-2] = 1.0010
        opens[-1] = 0.9993
        engulfing = features_for(
            engulfing_closes,
            opens=opens,
            highs=[max(open_, close) + 0.0001 for open_, close in zip(opens, engulfing_closes)],
            lows=[min(open_, close) - 0.0001 for open_, close in zip(opens, engulfing_closes)],
        )
        engulfing["pos20"] = 0.3
        self.assertEqual(
            evaluate_family(self.lanes["candlestick_reversal.loose"], engulfing).direction,
            "buy",
        )

        displaced = features_for([1.0] * 58 + [0.9980, 0.9986])
        displaced["pos20"] = 0.2
        self.assertEqual(
            evaluate_family(self.lanes["atr_mean_reversion.loose"], displaced).direction,
            "buy",
        )

    def test_regression_pair_rank_and_regime_families_emit(self):
        rising = features_for([1.0 + index * 0.0001 for index in range(60)])
        self.assertEqual(
            evaluate_family(self.lanes["linear_regression_trend.loose"], rising).direction,
            "buy",
        )
        self.assertEqual(
            evaluate_family(self.lanes["regime_switching.loose"], rising).direction,
            "buy",
        )

        ranked = features_for([1.0] * 54 + [1.0001, 1.0002, 1.0003, 1.0004, 1.0005, 1.0006])
        ranked.update({"cross_pair_count": 68, "pair_rank_r3": 0.98, "pair_rank_r5": 0.99})
        self.assertEqual(
            evaluate_family(self.lanes["cross_sectional_pair_rank.loose"], ranked).direction,
            "buy",
        )

    def test_cross_pair_and_spread_activity_families_emit(self):
        lead = features_for([1.0] * 54 + [1.0001, 1.00012, 1.00014, 1.00016, 1.00018, 1.0002])
        lead.update(
            {
                "cross_sample_count": 6,
                "cross_strength_r1": 0.35,
                "cross_strength_r3": 0.45,
                "cross_breadth_r1": 0.4,
                "pair_norm_r1": 0.05,
                "volume_ratio_12": 1.3,
            }
        )
        self.assertEqual(
            evaluate_family(self.lanes["cross_pair_lead_lag.balanced"], lead).direction,
            "buy",
        )

        momentum = features_for(
            [1.0] * 54 + [1.0001, 1.0002, 1.0003, 1.00045, 1.0006, 1.0008],
            highs=[1.0001] * 54 + [1.0002, 1.0003, 1.0004, 1.0005, 1.00065, 1.00082],
            lows=[0.9999] * 54 + [1.0, 1.0001, 1.0002, 1.00035, 1.0005, 1.00055],
        )
        momentum.update({"volume_ratio_12": 1.25, "spread_ratio_12": 0.75, "spread_drop_3_pips": 0.2})
        self.assertEqual(
            evaluate_family(self.lanes["spread_compression_momentum.balanced"], momentum).direction,
            "buy",
        )


    def test_cross_market_confluence_requires_multiple_sources(self):
        features = features_for([1.0] * 54 + [1.0001, 1.0002, 1.0003, 1.0004, 1.0005, 1.0006])
        features.update(
            {
                "cross_sample_count": 6,
                "cross_strength_r1": 0.50,
                "cross_strength_r3": 0.65,
                "cross_strength_r5": 0.70,
                "cross_breadth_r3": 0.40,
                "pair_norm_r1": 0.30,
                "pair_norm_r3": 0.70,
                "relative_residual_r3": 0.40,
            }
        )
        setup = evaluate_family(self.lanes["cross_market_confluence.balanced"], features)
        self.assertEqual(setup.direction, "buy")
        self.assertGreaterEqual(setup.signal["buy_votes"], 5)

    def test_data_mined_combination_lane_preserves_rule_audit(self):
        features = features_for([1.0 + index * 0.00001 for index in range(60)])
        features["signal_combination_forecast"] = {
            "ready": True,
            "rule_id": "h300-r1",
            "rule_size": 3,
            "conditions": [
                {"feature": "rsi_14", "operator": "<=", "threshold": 35.0},
                {"feature": "volume_ratio_12", "operator": ">=", "threshold": 1.2},
                {"feature": "cross_strength_m3", "operator": ">=", "threshold": 0.3},
            ],
            "condition_text": "rsi_14 <= 35 AND volume_ratio_12 >= 1.2 AND cross_strength_m3 >= 0.3",
            "predicted_direction": "buy",
            "membership": 0.90,
            "train_support": 300.0,
            "holdout_support": 120.0,
            "holdout_n": 115,
            "holdout_lower_probability_edge": 0.06,
            "holdout_brier": 0.21,
            "expected_net_pips": 0.40,
            "horizon_sec": 300,
        }
        setup = evaluate_family(self.lanes["signal_combination_rules.strict"], features)
        self.assertEqual(setup.direction, "buy")
        self.assertEqual(setup.signal["rule_id"], "h300-r1")
        self.assertEqual(len(setup.signal["rule_conditions"]), 3)


class SupervisedTrainingTests(unittest.TestCase):
    def test_training_labels_include_entry_and_exit_spread(self):
        candles = []
        start = datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc)
        for index in range(80):
            mid = 1.1000 + index * 0.0001
            candles.append(
                {
                    "time": (start + timedelta(minutes=5 * index)).isoformat().replace("+00:00", "Z"),
                    "complete": True,
                    "mid": {"h": str(mid + 0.00005), "l": str(mid - 0.00005), "c": str(mid)},
                    "bid": {"c": str(mid - 0.0001)},
                    "ask": {"c": str(mid + 0.0001)},
                }
            )
        data = _supervised_candle_data(candles, 0.0001)
        self.assertIsNotNone(data)
        atr = data["current_atr_pips"]
        self.assertAlmostEqual(data["y"][-1][0] * atr, -1.0)
        self.assertAlmostEqual(data["y"][-1][1] * atr, -3.0)


class CrossSectionalFeatureTests(unittest.TestCase):
    def test_currency_strength_excludes_the_pair_being_scored(self):
        feature_cache = {}
        for instrument, r3, r5 in (
            ("NZD_USD", 1.0, 1.0),
            ("NZD_JPY", 2.0, 2.0),
            ("AUD_USD", -1.0, -1.0),
            ("USD_JPY", -1.0, -1.0),
        ):
            features = features_for([1.0] * 60)
            features.update(
                {
                    "instrument": instrument,
                    "r3_pips": r3,
                    "r5_pips": r5,
                    "m1_atr14_pips": 1.0,
                }
            )
            feature_cache[instrument] = features
        augment_cross_sectional_features(feature_cache)
        scored = feature_cache["NZD_USD"]
        self.assertEqual(scored["cross_sample_count"], 1)
        self.assertAlmostEqual(scored["cross_strength_r3"], 1.0)
        self.assertAlmostEqual(scored["relative_residual_r3"], 0.0)


class EconomicsTests(unittest.TestCase):
    def setUp(self):
        self.lanes = {lane.lane_id: lane for lane in default_lanes()}
        closes = [1.0] * 54 + [1.0001, 1.0002, 1.0003, 1.0004, 1.0005, 1.0007]
        self.features = features_for(closes)
        self.features["m5_atr14_pips"] = 10.0

    def test_strict_and_loose_gates_classify_same_quote_differently(self):
        strict_reasons, strict_metrics = economic_gates(
            self.lanes["currency_strength.strict"], self.features, 1.10000, 1.10015
        )
        loose_reasons, _ = economic_gates(
            self.lanes["currency_strength.loose"], self.features, 1.10000, 1.10015
        )
        self.assertIn("spread_absolute", strict_reasons)
        self.assertEqual(loose_reasons, [])
        self.assertEqual(
            classify_miss(self.lanes["currency_strength.strict"], strict_metrics),
            "near_threshold",
        )

    def test_extreme_spread_is_a_hard_reject_not_a_threshold_miss(self):
        _, metrics = economic_gates(
            self.lanes["currency_strength.loose"], self.features, 1.1000, 1.1100
        )
        self.assertEqual(
            classify_miss(self.lanes["currency_strength.loose"], metrics),
            "hard_reject",
        )

    def test_shadow_return_includes_entry_and_exit_spread(self):
        pips = theoretical_pips(
            "buy",
            0.0001,
            entry_bid=1.1000,
            entry_ask=1.1002,
            exit_bid=1.1005,
            exit_ask=1.1007,
        )
        self.assertAlmostEqual(pips, 3.0)

    def test_pattern_forecast_gates_sample_edge_and_movement(self):
        signal = {
            "strength_pips": 2.0,
            "pattern_forecast": {
                "combined_pattern_count": 10,
                "direction_edge": 0.01,
                "movement_coefficient": 0.80,
            },
        }
        reasons, metrics = economic_gates(
            self.lanes["pattern_count_forecast.fast"],
            self.features,
            1.10000,
            1.10005,
            signal,
        )
        self.assertIn("pattern_sample_count", reasons)
        self.assertIn("pattern_direction_edge", reasons)
        self.assertIn("pattern_movement_coefficient", reasons)
        self.assertEqual(metrics["pattern_sample_count"], 10)

    def test_feature_pips_use_broker_metadata_instead_of_currency_heuristic(self):
        m1_values = [400.0] * 59 + [400.01]
        m5_values = [400.0] * 40
        features, reason = build_features(
            "USD_HUF",
            {"M1": candle_rows(m1_values, 60), "M5": candle_rows(m5_values, 300)},
            pip_size=0.01,
        )
        self.assertEqual(reason, "")
        self.assertIsNotNone(features)
        self.assertEqual(features["pip"], 0.01)
        self.assertAlmostEqual(features["r1_pips"], 1.0)


class PracticeExecutorReentryTests(unittest.TestCase):
    def make_executor(self):
        executor = PracticeExecutor.__new__(PracticeExecutor)
        executor.args = SimpleNamespace(
            execution_reentry_cooldown_sec=900.0,
            execution_instrument_reentry_cooldown_sec=300.0,
            execution_jpy_factor_cooldown_sec=900.0,
            execution_max_open_jpy_factor_positions=1,
        )
        executor.recent_pair_direction_exits = {}
        executor.recent_instrument_exits = {}
        executor.recent_factor_exits = {}
        return executor

    def test_direction_conflict_is_never_live_executable(self):
        executor = self.make_executor()
        reason, _ = executor.reentry_blocker(
            {
                "instrument": "EUR_USD",
                "direction": "buy",
                "direction_conflict": True,
            },
            [],
        )
        self.assertEqual(reason, "direction_conflict_shadow_only")

    def test_recent_jpy_factor_exit_blocks_propagated_pair(self):
        executor = self.make_executor()
        executor.recent_factor_exits["JPY:short"] = time.time() - 60.0
        reason, detail = executor.reentry_blocker(
            {
                "instrument": "GBP_JPY",
                "direction": "buy",
                "direction_conflict": False,
            },
            [],
        )
        self.assertEqual(reason, "jpy_factor_reentry_cooldown")
        self.assertEqual(detail["correlation_factor"], "JPY:short")

    def test_open_jpy_factor_blocks_correlated_pair_capacity(self):
        executor = self.make_executor()
        reason, detail = executor.reentry_blocker(
            {
                "instrument": "EUR_JPY",
                "direction": "buy",
                "direction_conflict": False,
            },
            [{"instrument": "USD_JPY", "currentUnits": "100"}],
        )
        self.assertEqual(reason, "jpy_factor_position_capacity")
        self.assertEqual(detail["open_factor_positions"], 1)

    def test_intrahour_gate_requires_movement_not_confidence_alone(self):
        executor = self.make_executor()
        reason, detail = executor.cost_capture_blocker(
            {
                "execution_horizon_sec": 900,
                "spread_pips": 1.8,
                "liquidity_quality": 0.9,
                "signal_confidence": 0.64,
                "projected_net_pips": 0.34,
                "gross_to_spread": 0.19,
                "projected_gross_movement_pips": 0.34,
            }
        )
        self.assertEqual(reason, "intrahour_movement_to_cost")
        self.assertEqual(detail["min_gross_to_spread"], 2.5)

    def test_intrahour_gate_accepts_liquid_after_cost_edge(self):
        executor = self.make_executor()
        reason, detail = executor.cost_capture_blocker(
            {
                "execution_horizon_sec": 1800,
                "spread_pips": 1.8,
                "liquidity_quality": 0.9,
                "signal_confidence": 0.56,
                "projected_net_pips": 1.16,
                "gross_to_spread": 2.5,
                "projected_gross_movement_pips": 4.5,
            }
        )
        self.assertEqual(reason, "")
        self.assertTrue(detail["applied"])

    def test_wide_spread_exotic_is_not_an_intrahour_candidate(self):
        executor = self.make_executor()
        reason, _ = executor.cost_capture_blocker(
            {
                "execution_horizon_sec": 3600,
                "spread_pips": 153.0,
                "liquidity_quality": 0.2,
                "signal_confidence": 0.8,
                "projected_net_pips": 100.0,
                "gross_to_spread": 2.0,
            }
        )
        self.assertEqual(reason, "intrahour_spread_bucket")

    def test_multihour_gate_rejects_marginal_movement_to_cost(self):
        executor = self.make_executor()
        reason, detail = executor.cost_capture_blocker(
            {
                "execution_horizon_sec": 14400,
                "spread_pips": 1.9,
                "liquidity_quality": 0.85,
                "signal_confidence": 0.56,
                "projected_net_pips": 0.9995,
                "gross_to_spread": 1.381053,
                "projected_gross_movement_pips": 2.624,
            }
        )
        self.assertEqual(reason, "multihour_movement_to_cost")
        self.assertEqual(detail["min_gross_to_spread"], 2.0)
        self.assertEqual(detail["min_after_cost_pips"], 1.0)

    def test_multihour_gate_accepts_costed_higher_horizon_edge(self):
        executor = self.make_executor()
        reason, detail = executor.cost_capture_blocker(
            {
                "execution_horizon_sec": 14400,
                "spread_pips": 1.9,
                "liquidity_quality": 0.85,
                "signal_confidence": 0.56,
                "projected_net_pips": 2.0,
                "gross_to_spread": 2.0,
                "projected_gross_movement_pips": 3.8,
            }
        )
        self.assertEqual(reason, "")
        self.assertTrue(detail["applied"])
        self.assertEqual(detail["gate"], "multihour")

    def test_cost_gate_uses_current_edge_not_larger_historical_calibration(self):
        executor = self.make_executor()
        reason, detail = executor.cost_capture_blocker(
            {
                "execution_horizon_sec": 14400,
                "spread_pips": 1.0,
                "liquidity_quality": 0.9,
                "signal_confidence": 0.6,
                "projected_net_pips": 5.0,
                "instant_projected_net_pips": 0.2,
                "gross_to_spread": 2.5,
                "projected_gross_movement_pips": 2.5,
            }
        )
        self.assertEqual(reason, "multihour_after_cost_edge")
        self.assertEqual(detail["projected_net_pips"], 5.0)
        self.assertEqual(detail["cost_gate_after_cost_pips"], 0.2)

    def test_h2_protection_is_not_a_five_pip_scalper_stop(self):
        executor = self.make_executor()
        row = {
            "execution_horizon_sec": 7200,
            "stop_loss_pips": 5.0,
            "spread_pips": 1.8,
            "atr_pips": 20.0,
            "projected_gross_movement_pips": 140.0,
        }
        shape = executor.apply_horizon_risk_shape(row)
        self.assertTrue(shape["applied"])
        self.assertEqual(row["stop_loss_pips"], 16.0)
        self.assertTrue(shape["risk_normalized_sizing"])

    def test_h1_protection_remains_intrahour_shape(self):
        executor = self.make_executor()
        row = {
            "execution_horizon_sec": 3600,
            "stop_loss_pips": 5.0,
            "spread_pips": 1.8,
            "projected_gross_movement_pips": 20.0,
        }
        shape = executor.apply_horizon_risk_shape(row)
        self.assertFalse(shape["applied"])
        self.assertEqual(row["stop_loss_pips"], 5.0)


class ExecutionDiagnosticRateLimitTests(unittest.TestCase):
    def test_repeated_skip_is_rate_limited_but_reason_change_is_immediate(self):
        executor = object.__new__(PracticeExecutor)
        executor.log_path = Path("unused.jsonl")
        executor.last_execution_skip_log_monotonic = -float("inf")
        executor.last_execution_skip_reason = ""
        with patch(
            "trad.oanda_practice_shadow_strategy_lab.time.monotonic",
            side_effect=[100.0, 105.0, 106.0, 117.0],
        ), patch("trad.oanda_practice_shadow_strategy_lab.log_line") as mocked:
            self.assertTrue(executor.log_execution_skip("no_capacity", count=1))
            self.assertFalse(executor.log_execution_skip("no_capacity", count=1))
            self.assertTrue(executor.log_execution_skip("cooldown", count=1))
            self.assertTrue(executor.log_execution_skip("cooldown", count=1))
        self.assertEqual(mocked.call_count, 3)

    def test_promotion_log_compaction_keeps_only_decision_evidence(self):
        compact = PracticeExecutor.compact_promotion_log_rows(
            [
                {
                    "lane_id": "example.loose",
                    "family": "example",
                    "eligible": False,
                    "blocked_by": ["minimum_samples"],
                    "training": {"n": 10, "avg": -1.0, "raw_rows": [1, 2, 3]},
                    "holdout": {"n": 3, "win_rate": 66.7, "raw_rows": [4, 5]},
                    "large_unused_payload": {"rows": list(range(100))},
                }
            ]
        )
        self.assertEqual(compact[0]["lane_id"], "example.loose")
        self.assertEqual(compact[0]["training"], {"n": 10, "avg": -1.0})
        self.assertEqual(compact[0]["holdout"], {"n": 3, "win_rate": 66.7})
        self.assertNotIn("large_unused_payload", compact[0])


if __name__ == "__main__":
    unittest.main()
