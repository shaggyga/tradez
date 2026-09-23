import json
import sqlite3
import tempfile
import tracemalloc
import unittest
from pathlib import Path

from trad.oanda_edge_evidence import (
    EvidenceThresholds,
    SpilledEconomicLabels,
    SpilledRows,
    archetype_incremental_tests,
    audit_forecast_contract,
    audit_forecast_contract_snapshot,
    audit_unavailable_microstructure,
    build_economic_labels,
    build_archetype_results,
    build_cell_results,
    build_report,
    candidate_condition_results,
    collapse_factor_episodes,
    count_immutable_snapshot_rows,
    count_duplicate_forecast_payloads,
    gate_distances,
    close_forecast_contract_cache,
    forecast_contract_cache_identity,
    load_rows,
    resolve_forecast_contract_snapshot,
    signed_currency_factors,
)
from trad.oanda_strategy_archetypes import strategy_archetype
from trad.oanda_shadow_outcome_store import ShadowOutcomeStore


def row(event_id, instrument, direction, epoch, net, *, family="seed"):
    return {
        "event_id": event_id,
        "family": family,
        "instrument": instrument,
        "direction": direction,
        "horizon_sec": 3600,
        "entry_epoch": float(epoch),
        "entry_day": "2026-08-01",
        "entry_week": "2026-W31",
        "entry_time": "2026-08-01T12:00:00+00:00",
        "session": "london",
        "liquidity": "liquid_le_2",
        "spread_pips": 1.0,
        "executable_pips": float(net) + 0.25,
        "net_pips": float(net),
        "factors": (
            {"currency:JPY:long", "currency:USD:short"}
            if instrument == "USD_JPY" and direction == "sell"
            else {"currency:JPY:long", "currency:EUR:short"}
            if instrument == "EUR_JPY" and direction == "sell"
            else {f"instrument:{instrument}:{direction}"}
        ),
    }


def spilled_rows(source_rows):
    spilled = SpilledRows.create()
    values = []
    for spool_id, item in enumerate(source_rows, 1):
        values.append(
            (
                spool_id,
                int(item.get("row_id") or spool_id),
                item["event_id"], item["horizon_sec"], item["entry_time"],
                item["entry_epoch"], item["entry_day"], item["entry_week"],
                item["family"], item["instrument"], item["direction"],
                item["executable_pips"], item["net_pips"], item["spread_pips"],
                item["session"], item["liquidity"],
                item.get("max_favorable_pips", 0.0),
                item.get("max_adverse_pips", 0.0),
                item.get("first_positive_sec"),
                item.get("configured_stop_hit_sec"),
                item.get("configured_target_hit_sec"),
                strategy_archetype(item["family"]),
                int(item["entry_epoch"] // 60),
            )
        )
    spilled.append_many(values)
    spilled.finish_loading()
    return spilled


class EdgeEvidenceTests(unittest.TestCase):
    def test_disk_spool_python_memory_is_batch_bounded(self):
        spilled = SpilledRows.create()
        tracemalloc.start()
        try:
            for chunk_start in range(0, 50_000, 1_000):
                batch = []
                for offset in range(1_000):
                    value = chunk_start + offset + 1
                    batch.append(
                        (
                            value, value, f"event-{value}", 3600,
                            "2026-08-01T12:00:00+00:00", float(value),
                            "2026-08-01", "2026-W31", "seed", "EUR_USD",
                            "buy", 1.0, 0.75, 1.0, "london", "liquid_le_2",
                            2.0, 1.0, 10.0, None, 30.0, "trend", value // 60,
                        )
                    )
                spilled.append_many(batch)
            current, peak = tracemalloc.get_traced_memory()
            self.assertEqual(len(spilled), 50_000)
            self.assertLess(peak, 16 * 1024 * 1024)
            self.assertLess(current, 4 * 1024 * 1024)
        finally:
            tracemalloc.stop()
            spilled.close()

    def test_disk_spooled_aggregation_preserves_family_and_archetype_semantics(self):
        rows = [
            row("a", "USD_JPY", "sell", 1000, 2.0, family="ema_trend_cross"),
            row("b", "EUR_JPY", "sell", 1030, 4.0, family="ema_trend_cross"),
            row("c", "EUR_USD", "buy", 5000, -1.5, family="ema_trend_cross"),
            row("d", "GBP_USD", "sell", 9000, 3.25, family="cusum_breakout"),
            row("e", "USD_CHF", "buy", 9000, -0.75, family="cusum_breakout"),
        ]
        for index, item in enumerate(rows, 1):
            item["row_id"] = index
            item["factors"] = signed_currency_factors(
                item["instrument"], item["direction"]
            )
            item["max_favorable_pips"] = float(index)
            item["max_adverse_pips"] = float(index) / 2.0
            item["configured_target_hit_sec"] = 10.0 if index % 2 else None
            item["configured_stop_hit_sec"] = 20.0 if index % 2 else 5.0
        thresholds = EvidenceThresholds(minimum_effective_n=1)
        expected_cells, expected_families = build_cell_results(
            rows, thresholds=thresholds, slippage_pips=0.25
        )
        expected_archetypes = build_archetype_results(
            rows, thresholds=thresholds, slippage_pips=0.25
        )
        spilled = spilled_rows(rows)
        try:
            actual_cells, actual_families = build_cell_results(
                spilled, thresholds=thresholds, slippage_pips=0.25
            )
            actual_archetypes = build_archetype_results(
                spilled, thresholds=thresholds, slippage_pips=0.25
            )
            self.assertEqual(expected_cells, actual_cells)
            self.assertEqual(expected_families, actual_families)
            self.assertEqual(expected_archetypes, actual_archetypes)
        finally:
            spilled.close()

    def test_disk_spooled_economic_labels_preserve_source_order_and_alternatives(self):
        rows = [
            row("a", "EUR_USD", "buy", 1200, 2.0),
            row("b", "GBP_USD", "buy", 1205, 5.0),
            row("c", "USD_JPY", "sell", 1800, -1.0),
        ]
        for index, item in enumerate(rows, 1):
            item["row_id"] = index
            item.update(
                {
                    "max_favorable_pips": 6.0,
                    "max_adverse_pips": 1.0,
                    "first_positive_sec": 30.0,
                    "configured_stop_hit_sec": None,
                    "configured_target_hit_sec": 90.0,
                }
            )
        expected = build_economic_labels(rows, slippage_pips=0.25)
        spilled = spilled_rows(rows)
        try:
            labels = build_economic_labels(spilled, slippage_pips=0.25)
            self.assertIsInstance(labels, SpilledEconomicLabels)
            self.assertEqual(expected, list(labels))
        finally:
            spilled.close()

    def test_disk_spooled_archetype_agreement_is_exact(self):
        rows = [
            row("a", "EUR_USD", "buy", 1200, 2.0, family="ema_trend_cross"),
            row("b", "EUR_USD", "buy", 1201, 3.0, family="session_range_breakout"),
            row("c", "GBP_USD", "sell", 5000, -1.0, family="ema_trend_cross"),
            row("d", "GBP_USD", "sell", 5001, 1.0, family="session_range_breakout"),
        ]
        for index, item in enumerate(rows, 1):
            item["row_id"] = index
            item["factors"] = signed_currency_factors(
                item["instrument"], item["direction"]
            )
        expected = archetype_incremental_tests(rows, 0.25)
        spilled = spilled_rows(rows)
        try:
            self.assertEqual(expected, archetype_incremental_tests(spilled, 0.25))
        finally:
            spilled.close()

    def test_combined_forecast_contract_audit_is_exact(self):
        connection = sqlite3.connect(":memory:")
        connection.execute(
            "CREATE TABLE canonical_forecasts ("
            "track_outcome INTEGER, kind TEXT, model_version TEXT, "
            "feature_version TEXT, payload_sha256 TEXT, forecast_json TEXT)"
        )
        connection.executemany(
            "INSERT INTO canonical_forecasts VALUES (?,?,?,?,?,?)",
            [
                (1, "signal", "m1", "f1", "a", json.dumps({
                    "microstructure_at_entry": {"order_book_available": 1}
                })),
                (0, "miss", "unknown", "unknown", "a", json.dumps({
                    "microstructure_at_entry": {"ask_total_liquidity": 2}
                })),
                (1, "signal", "m2", "f2", "b", "not-json"),
            ],
        )
        progress = []
        contract, unavailable = audit_forecast_contract(
            connection,
            batch_size=2,
            progress_callback=lambda phase, details: progress.append((phase, details)),
        )
        self.assertEqual(contract["captured"], 3)
        self.assertEqual(contract["tracked_for_outcome"], 2)
        self.assertEqual(contract["signal"], 2)
        self.assertEqual(contract["miss"], 1)
        self.assertEqual(contract["unknown_model_version"], 1)
        self.assertEqual(contract["unknown_feature_version"], 1)
        self.assertEqual(contract["duplicate_payloads"], 1)
        self.assertEqual(contract["duplicate_payload_rows_scanned"], 3)
        self.assertEqual(unavailable["order_book_available_rows"], 1)
        self.assertEqual(unavailable["pricing_depth_informative_rows"], 1)
        self.assertEqual(unavailable["malformed_forecast_json_rows"], 1)
        self.assertEqual(progress[-1][1]["forecast_rows_inspected"], 3)
        connection.close()

    def test_paged_forecast_snapshot_is_exact_excludes_appends_and_releases_wal(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.sqlite"
            writer = sqlite3.connect(source)
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute(
                "CREATE TABLE canonical_forecasts ("
                "track_outcome INTEGER, kind TEXT, model_version TEXT, "
                "feature_version TEXT, payload_sha256 TEXT, forecast_json TEXT)"
            )
            original = [
                (1, "signal", "m1", "f1", "a", json.dumps({
                    "microstructure_at_entry": {"order_book_available": 1}
                })),
                (0, "miss", "unknown", "unknown", "a", json.dumps({
                    "microstructure_at_entry": {"ask_total_liquidity": 2}
                })),
                (1, "signal", "m2", "f2", "b", "not-json"),
            ]
            writer.executemany(
                "INSERT INTO canonical_forecasts VALUES (?,?,?,?,?,?)", original
            )
            writer.commit()
            expected_connection = sqlite3.connect(source)
            expected = audit_forecast_contract(expected_connection, batch_size=2)
            expected_connection.close()
            maximum_rowid = int(
                writer.execute(
                    "SELECT MAX(rowid) FROM canonical_forecasts"
                ).fetchone()[0]
            )
            appended = False
            checkpoints = []

            def progress(_phase, _details):
                nonlocal appended
                if appended:
                    return
                writer.execute(
                    "INSERT INTO canonical_forecasts VALUES (?,?,?,?,?,?)",
                    (1, "signal", "later", "later", "later", "{}"),
                )
                writer.commit()
                checkpoints.append(writer.execute(
                    "PRAGMA wal_checkpoint(PASSIVE)"
                ).fetchone())
                appended = True

            actual = audit_forecast_contract_snapshot(
                source,
                maximum_rowid=maximum_rowid,
                batch_size=2,
                progress_callback=progress,
            )
            self.assertEqual(actual, expected)
            self.assertTrue(appended)
            self.assertTrue(checkpoints)
            self.assertEqual(checkpoints[-1][0], 0)
            self.assertEqual(checkpoints[-1][1], checkpoints[-1][2])
            self.assertEqual(
                writer.execute(
                    "SELECT COUNT(*) FROM canonical_forecasts"
                ).fetchone()[0],
                len(original) + 1,
            )
            writer.close()

    def test_immutable_forecast_contract_audit_cache_is_exact_and_invalidates(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.sqlite"
            writer = sqlite3.connect(source)
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute(
                "CREATE TABLE canonical_forecasts ("
                "track_outcome INTEGER, kind TEXT, model_version TEXT, "
                "feature_version TEXT, payload_sha256 TEXT, forecast_json TEXT)"
            )
            writer.execute(
                "CREATE TRIGGER canonical_forecasts_no_update "
                "BEFORE UPDATE ON canonical_forecasts "
                "BEGIN SELECT RAISE(ABORT, 'immutable'); END"
            )
            writer.execute(
                "CREATE TRIGGER canonical_forecasts_no_delete "
                "BEFORE DELETE ON canonical_forecasts "
                "BEGIN SELECT RAISE(ABORT, 'immutable'); END"
            )
            original = [
                (1, "signal", "m1", "f1", "a", json.dumps({
                    "microstructure_at_entry": {"order_book_available": 1}
                })),
                (0, "miss", "unknown", "unknown", "a", json.dumps({
                    "microstructure_at_entry": {"ask_total_liquidity": 2}
                })),
                (1, "signal", "m2", "f2", "b", "not-json"),
            ]
            writer.executemany(
                "INSERT INTO canonical_forecasts VALUES (?,?,?,?,?,?)", original
            )
            writer.commit()

            reader = sqlite3.connect(source)
            first_highwater = int(reader.execute(
                "SELECT MAX(rowid) FROM canonical_forecasts"
            ).fetchone()[0])
            first_identity = forecast_contract_cache_identity(reader, source)
            reader.close()
            self.assertIsNotNone(first_identity)

            cache = {}
            progress = []
            first = resolve_forecast_contract_snapshot(
                source,
                maximum_rowid=first_highwater,
                source_identity=first_identity,
                audit_cache=cache,
                progress_callback=lambda phase, details: progress.append(
                    (phase, details)
                ),
                batch_size=2,
            )
            second = resolve_forecast_contract_snapshot(
                source,
                maximum_rowid=first_highwater,
                source_identity=first_identity,
                audit_cache=cache,
                progress_callback=lambda phase, details: progress.append(
                    (phase, details)
                ),
                batch_size=2,
            )
            self.assertEqual(first[2], "full_scan")
            self.assertEqual(second[2], "cache_hit")
            self.assertEqual(first[:2], second[:2])
            self.assertEqual(first[0]["captured"], len(original))
            self.assertEqual(
                progress[-1][1]["inventory_step"],
                "reusing_immutable_forecast_contract_audit",
            )

            writer.execute(
                "INSERT INTO canonical_forecasts VALUES (?,?,?,?,?,?)",
                (1, "signal", "m3", "f3", "a", "{}"),
            )
            writer.commit()
            reader = sqlite3.connect(source)
            second_highwater = int(reader.execute(
                "SELECT MAX(rowid) FROM canonical_forecasts"
            ).fetchone()[0])
            second_identity = forecast_contract_cache_identity(reader, source)
            reader.close()
            third = resolve_forecast_contract_snapshot(
                source,
                maximum_rowid=second_highwater,
                source_identity=second_identity,
                audit_cache=cache,
                batch_size=2,
            )
            exact_full = audit_forecast_contract_snapshot(
                source, maximum_rowid=second_highwater, batch_size=2
            )
            self.assertEqual(third[2], "incremental_extension")
            self.assertEqual(third[0]["captured"], len(original) + 1)
            self.assertEqual(third[0]["duplicate_payloads"], 2)
            self.assertEqual(third[:2], exact_full)
            close_forecast_contract_cache(cache)
            writer.close()

    def test_forecast_contract_cache_requires_immutability_triggers(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.sqlite"
            connection = sqlite3.connect(source)
            connection.execute("CREATE TABLE canonical_forecasts(value TEXT)")
            connection.execute("INSERT INTO canonical_forecasts VALUES ('x')")
            connection.commit()
            self.assertIsNone(
                forecast_contract_cache_identity(connection, source)
            )
            connection.close()

    def test_windowed_snapshot_count_is_exact_with_rowid_gaps(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.sqlite"
            writer = sqlite3.connect(source)
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("CREATE TABLE items(value TEXT)")
            writer.executemany(
                "INSERT INTO items(rowid,value) VALUES (?,?)",
                [(1, "a"), (3, "b"), (250_001, "c"), (500_005, "d")],
            )
            writer.commit()
            self.assertEqual(
                count_immutable_snapshot_rows(
                    source,
                    table="items",
                    maximum_rowid=500_004,
                    rowid_window=250_000,
                ),
                3,
            )
            writer.close()

    def test_microstructure_audit_is_exact_and_reports_bounded_progress(self):
        connection = sqlite3.connect(":memory:")
        connection.execute("CREATE TABLE canonical_forecasts(forecast_json TEXT NOT NULL)")
        connection.executemany("INSERT INTO canonical_forecasts VALUES (?)", [
            (json.dumps({"microstructure_at_entry": {
                "order_book_available": 1, "position_book_available": 0,
                "bid_total_liquidity": 2, "ask_total_liquidity": 0,
            }}),),
            (json.dumps({"microstructure_at_entry": {}}),),
            ("not-json",),
        ])
        progress = []
        result = audit_unavailable_microstructure(
            connection, batch_size=2,
            progress_callback=lambda phase, details: progress.append((phase, details)),
        )
        self.assertEqual(result["forecast_rows_inspected"], 3)
        self.assertEqual(result["order_book_available_rows"], 1)
        self.assertEqual(result["position_book_available_rows"], 0)
        self.assertEqual(result["pricing_depth_informative_rows"], 1)
        self.assertEqual(result["malformed_forecast_json_rows"], 1)
        self.assertEqual(progress[-1][1]["forecast_rows_inspected"], 3)
        connection.close()

    def test_duplicate_forecast_payload_scan_is_exact_and_reports_progress(self):
        connection = sqlite3.connect(":memory:")
        connection.execute("CREATE TABLE canonical_forecasts(payload_sha256 TEXT NOT NULL)")
        connection.executemany(
            "INSERT INTO canonical_forecasts VALUES (?)",
            [("a",), ("b",), ("a",), ("a",), ("c",), ("b",)],
        )
        progress = []
        duplicates, scanned = count_duplicate_forecast_payloads(
            connection,
            batch_size=2,
            progress_callback=lambda phase, details: progress.append((phase, details)),
        )
        self.assertEqual((duplicates, scanned), (3, 6))
        self.assertEqual(progress[-1][1]["duplicate_scan_rows"], 6)
        self.assertEqual(progress[-1][1]["duplicate_payloads"], 3)
        connection.close()

    def test_candidate_conditions_use_forecast_time_metadata(self):
        item = row(
            "candidate-1",
            "EUR_USD",
            "buy",
            1_722_500_000,
            3.0,
            family="volatility_squeeze_breakout",
        )
        results = candidate_condition_results(
            [item],
            metadata={
                "candidate-1": {
                    "signal": {"squeeze_width_ratio": 0.2, "pos20": 0.95},
                    "volatility_regime": "compressed",
                    "stop_loss_pips": 5.0,
                    "take_profit_r": 1.5,
                }
            },
            thresholds=EvidenceThresholds(minimum_effective_n=2),
            slippage_pips=0.25,
            macro_releases=[(item["entry_epoch"] + 600, {"EUR"})],
        )
        values = {(result["dimension"], result["value"]) for result in results}
        self.assertIn(("squeeze_width_ratio", "tight"), values)
        self.assertIn(("session_range_location", "near_high"), values)
        self.assertIn(("macro_event_proximity", "pre_release_30m"), values)
        self.assertTrue(all(not result["eligible"] for result in results))

    def test_incremental_archetype_test_collapses_same_archetype_names(self):
        rows = [
            row("a", "EUR_USD", "buy", 1200, 2.0, family="ema_trend_cross"),
            row("b", "EUR_USD", "buy", 1200, 2.0, family="session_range_breakout"),
        ]
        result = archetype_incremental_tests(rows, 0.25)
        self.assertLessEqual(len(result), 1)

    def test_pre_boundary_dedicated_outcome_is_excluded(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.sqlite"
            store = ShadowOutcomeStore(source, batch_size=8, flush_sec=0.1)
            store.observe(
                {
                    "id": "pre-boundary",
                    "horizon_sec": 60,
                    "lane_id": "lane",
                    "family": "seed",
                    "profile": "research",
                    "kind": "signal",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "theoretical_pips": 2.0,
                    "outcome_delay_sec": 1.0,
                    "entry_time": "2026-08-06T12:00:00+00:00",
                    "exit_time": "2026-08-06T12:00:59+00:00",
                    "entry_bid": 1.1,
                    "entry_ask": 1.1002,
                    "exit_bid": 1.1004,
                    "exit_ask": 1.1006,
                    "pip": 0.0001,
                    "entry_spread_pips": 2.0,
                    "maturity_worker": "canonical_quote_snapshot_v1",
                    "forecast_recorded_utc": "2026-08-06T12:00:00+00:00",
                    "quote_snapshot_generated_utc": "2026-08-06T12:00:59+00:00",
                }
            )
            store.close()
            rows, integrity = load_rows(
                source, slippage_pips=0.25, maximum_delay_sec=15.0
            )
            self.assertEqual(rows, [])
            self.assertEqual(integrity["pre_boundary_outcomes_excluded"], 1)

    def test_economic_labels_measure_cost_clearance_and_rotation_opportunity(self):
        first = row("a", "EUR_USD", "buy", 1200, 2.0)
        second = row("b", "GBP_USD", "buy", 1205, 5.0)
        for item in (first, second):
            item.update(
                {
                    "entry_time": "2026-08-01T12:00:00+00:00",
                    "max_favorable_pips": 6.0,
                    "max_adverse_pips": 1.0,
                    "first_positive_sec": 30.0,
                    "configured_stop_hit_sec": None,
                    "configured_target_hit_sec": 90.0,
                }
            )
        labels = build_economic_labels([first, second], slippage_pips=0.25)
        label = next(item for item in labels if item["event_id"] == "a")
        self.assertTrue(label["cleared_total_cost"])
        self.assertTrue(label["target_before_stop"])
        self.assertEqual(label["best_alternative_net_pips"], 5.0)
        self.assertEqual(label["holding_incremental_vs_best_alternative_pips"], -3.0)
        self.assertFalse(label["was_best_available_pair"])

    def test_correlated_jpy_propagation_collapses_to_one_episode(self):
        rows = [
            row("a", "USD_JPY", "sell", 1000, 2.0),
            row("b", "EUR_JPY", "sell", 1030, 4.0),
            row("c", "USD_JPY", "buy", 1040, -1.0),
        ]
        episodes = collapse_factor_episodes(rows, 3600)
        self.assertEqual(len(episodes), 2)
        self.assertIn(2, {item["raw_count"] for item in episodes})

    def test_cells_use_effective_n_and_publish_gate_distance(self):
        rows = [
            row("a", "USD_JPY", "sell", 1000, 2.0),
            row("b", "USD_JPY", "sell", 1030, 4.0),
        ]
        cells, families = build_cell_results(
            rows,
            thresholds=EvidenceThresholds(minimum_effective_n=2),
            slippage_pips=0.25,
        )
        self.assertEqual(cells[0]["raw_n"], 2)
        self.assertEqual(cells[0]["effective_n"], 1)
        sample_gate = next(
            item
            for item in gate_distances(cells[0], EvidenceThresholds(minimum_effective_n=2))
            if item["gate"] == "effective_sample_size"
        )
        self.assertEqual(sample_gate["distance"], -1.0)
        self.assertFalse(sample_gate["passed"])
        self.assertEqual(families[0]["effective_n"], 1)

    def test_report_is_fail_closed_and_freezes_completed_day(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            connection = sqlite3.connect(source)
            connection.executescript(
                """
                CREATE TABLE outcomes (
                    row_id INTEGER PRIMARY KEY,
                    event_id TEXT, horizon_sec INTEGER, observed_utc TEXT,
                    lane_id TEXT, family TEXT, profile TEXT, kind TEXT,
                    instrument TEXT, direction TEXT, theoretical_pips REAL,
                    outcome_delay_sec REAL, entry_time TEXT,
                    entry_spread_pips REAL, exit_time TEXT,
                    UNIQUE(event_id,horizon_sec)
                );
                """
            )
            connection.execute(
                "INSERT INTO outcomes VALUES (1,'e1',3600,'2026-08-01T13:00:00+00:00',"
                "'lane','volatility_squeeze_breakout','balanced','signal','EUR_USD',"
                "'buy',2.0,1.0,'2026-08-01T12:00:00+00:00',1.0,"
                "'2026-08-01T13:00:00+00:00')"
            )
            connection.commit()
            connection.close()
            output = root / "latest.json"
            evidence = root / "evidence.sqlite"
            progress = []
            report = build_report(
                source,
                evidence_database=evidence,
                output_json=output,
                output_markdown=root / "report.md",
                registry_path=root / "missing.md",
                progress_callback=lambda phase, details: progress.append(
                    (phase, details)
                ),
            )
            self.assertFalse(report["can_place_orders"])
            self.assertEqual(report["shadow_allocator"]["action"], "no_trade")
            self.assertTrue(output.is_file())
            saved = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(saved["cell_count"], 1)
            phases = [phase for phase, _details in progress]
            self.assertIn("loading_rows", phases)
            self.assertIn("building_cells", phases)
            self.assertIn("persisting_evidence", phases)
            self.assertEqual(phases[-1], "report_complete")
            restored = build_report(
                source,
                evidence_database=evidence,
                output_json=output,
                output_markdown=root / "report.md",
                registry_path=root / "missing.md",
            )
            self.assertEqual(
                report["prospective_governance"]["governance_sha256"],
                restored["prospective_governance"]["governance_sha256"],
            )
            self.assertEqual(report["top_cells"], restored["top_cells"])
            self.assertEqual(
                report["newly_frozen_daily_snapshots"][
                    "economic_labels_attempted_incrementally"
                ],
                1,
            )
            self.assertEqual(
                restored["newly_frozen_daily_snapshots"][
                    "economic_labels_attempted_incrementally"
                ],
                0,
            )
            check = sqlite3.connect(evidence)
            self.assertEqual(check.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(
                check.execute("SELECT COUNT(*) FROM immutable_daily_snapshots").fetchone()[0],
                1,
            )
            self.assertEqual(
                check.execute(
                    "SELECT COUNT(*) FROM immutable_forecast_daily_snapshots"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                check.execute(
                    "SELECT COUNT(*) FROM canonical_economic_outcome_labels"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                check.execute("SELECT COUNT(*) FROM economic_outcome_labels").fetchone()[0],
                0,
            )
            check.close()
            late = sqlite3.connect(source)
            late.execute(
                "INSERT INTO outcomes VALUES (2,'e2',3600,'2026-08-01T14:00:00+00:00',"
                "'lane','volatility_squeeze_breakout','balanced','signal','GBP_USD',"
                "'buy',1.0,1.0,'2026-08-01T13:00:00+00:00',1.0,"
                "'2026-08-01T14:00:00+00:00')"
            )
            late.commit(); late.close()
            with self.assertRaisesRegex(RuntimeError, "late outcome row"):
                build_report(
                    source,
                    evidence_database=evidence,
                    output_json=output,
                    output_markdown=root / "report.md",
                    registry_path=root / "missing.md",
                )

    def test_compact_and_verbose_diagnostics_reproduce_same_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            compact_path = root / "compact.sqlite"
            compact = ShadowOutcomeStore(compact_path, batch_size=8, flush_sec=0.1)
            compact.observe(
                {
                    "id": "equivalence-1",
                    "horizon_sec": 3600,
                    "lane_id": "lane",
                    "family": "seed",
                    "profile": "proof",
                    "model_id": "seed.h1",
                    "kind": "signal",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "theoretical_pips": 2.0,
                    "outcome_delay_sec": 1.0,
                    "entry_time": "2026-08-06T12:00:00+00:00",
                    "exit_time": "2026-08-06T13:00:01+00:00",
                    "entry_bid": 1.1,
                    "entry_ask": 1.1002,
                    "exit_bid": 1.1004,
                    "exit_ask": 1.1006,
                    "pip": 0.0001,
                    "entry_spread_pips": 2.0,
                    "forecast_recorded_utc": "2026-08-06T12:00:00+00:00",
                    "irrelevant_large_block": {"values": list(range(100))},
                }
            )
            compact.flush(force=True)
            columns = [
                row[1]
                for row in compact.connection.execute("PRAGMA table_info(canonical_outcomes)")
            ]
            stored = list(
                compact.connection.execute("SELECT * FROM canonical_outcomes").fetchone()
            )
            compact.close()

            verbose_path = root / "verbose.sqlite"
            verbose = ShadowOutcomeStore(verbose_path, batch_size=8, flush_sec=0.1)
            diagnostics_index = columns.index("diagnostics_json")
            stored[diagnostics_index] = json.dumps(
                {
                    "forecast_recorded_utc": "2026-08-06T12:00:00+00:00",
                    "irrelevant_large_block": {"values": list(range(1000))},
                    "duplicated_forecast_payload": "x" * 10000,
                }
            )
            verbose.connection.execute(
                f"INSERT INTO canonical_outcomes ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                stored,
            )
            verbose.connection.commit()
            verbose.close()

            compact_rows, _ = load_rows(
                compact_path, slippage_pips=0.25, maximum_delay_sec=15.0
            )
            verbose_rows, _ = load_rows(
                verbose_path, slippage_pips=0.25, maximum_delay_sec=15.0
            )
            compact_cells, _ = build_cell_results(
                compact_rows,
                thresholds=EvidenceThresholds(minimum_effective_n=1),
                slippage_pips=0.25,
            )
            verbose_cells, _ = build_cell_results(
                verbose_rows,
                thresholds=EvidenceThresholds(minimum_effective_n=1),
                slippage_pips=0.25,
            )
            self.assertEqual(compact_cells, verbose_cells)


if __name__ == "__main__":
    unittest.main()
