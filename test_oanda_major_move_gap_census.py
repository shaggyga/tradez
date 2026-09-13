import unittest
import json
import csv
import tempfile
import sqlite3
from pathlib import Path

import oanda_major_move_gap_census as census


class NonIterableReadOnlySequence:
    """Support indexed access and slicing while rejecting whole-list copies."""

    def __init__(self, values):
        self._values = tuple(values)

    def __len__(self):
        return len(self._values)

    def __getitem__(self, index):
        return self._values[index]

    def __iter__(self):
        raise AssertionError("source lookup must not iterate/copy the full sequence")


def test_supervisor_census_uses_resolved_available_python_runtime() -> None:
    supervisor = (Path(__file__).resolve().parent / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    start = supervisor.index('-Name "major_move_gap_census"')
    block = supervisor[start : start + 900]
    assert "-Executable $Python" in block
    assert "-Executable $ProjectPython" not in block
    assert '-Needle "oanda_major_move_gap_census.py" `\n            -Executable $Python `' in block


def test_supervisor_census_uses_liveness_heartbeat_not_completed_report() -> None:
    supervisor = (
        Path(__file__).resolve().parent / "oanda_always_on_supervisor.ps1"
    ).read_text(encoding="utf-8")
    start = supervisor.index('-Name "major_move_gap_census"')
    block = supervisor[start : start + 1800]
    assert '"--heartbeat", (Join-Path $State "major_move_gap_census_heartbeat_v1.json")' in block
    assert 'LiteralPath = (Join-Path $State "major_move_gap_census_heartbeat_v1.json")' in block
    assert 'ExpectedJsonField = "worker"' in block
    assert 'ExpectedJsonValue = "oanda_major_move_gap_census"' in block
    freshness = block.split("-Freshness @{", 1)[1].split("}", 1)[0]
    assert "MAJOR_MOVE_GAP_CENSUS_CURRENT.json" not in freshness


class MajorMoveGapCensusTests(unittest.TestCase):
    def test_cycle_cadence_is_start_to_start_not_runtime_plus_interval(self):
        self.assertEqual(
            census.cycle_sleep_seconds(
                21_600.0,
                1_000.0,
                100_000.0,
                now_monotonic=4_600.0,
            ),
            18_000.0,
        )
        self.assertEqual(
            census.cycle_sleep_seconds(
                21_600.0,
                1_000.0,
                10_000.0,
                now_monotonic=4_600.0,
            ),
            5_400.0,
        )
        self.assertEqual(
            census.cycle_sleep_seconds(
                21_600.0,
                1_000.0,
                100_000.0,
                now_monotonic=23_000.0,
            ),
            0.0,
        )

    def test_relevant_source_events_reuses_read_only_index_sequences_exactly(self):
        early = {
            "source_event_id": "early",
            "effective_epoch": 100,
            "valid_until_epoch": None,
            "superseded_epoch": None,
        }
        shared = {
            "source_event_id": "shared",
            "effective_epoch": 200,
            "valid_until_epoch": None,
            "superseded_epoch": None,
        }
        expired = {
            "source_event_id": "expired",
            "effective_epoch": 250,
            "valid_until_epoch": 275,
            "superseded_epoch": None,
        }
        latest = {
            "source_event_id": "latest",
            "effective_epoch": 300,
            "valid_until_epoch": None,
            "superseded_epoch": None,
        }
        event_values = {
            "EUR": (early, shared, expired),
            "USD": (shared, latest),
        }
        epoch_values = {
            currency: tuple(int(row["effective_epoch"]) for row in events)
            for currency, events in event_values.items()
        }
        source_index = {
            currency: {
                "events": NonIterableReadOnlySequence(event_values[currency]),
                "epochs": NonIterableReadOnlySequence(epoch_values[currency]),
            }
            for currency in event_values
        }
        before = {
            currency: tuple(dict(row) for row in events)
            for currency, events in event_values.items()
        }

        selected = census.relevant_source_events(
            source_index,
            ("EUR", "USD"),
            start_epoch=150,
            end_epoch=300,
            causal_at_entry=True,
        )

        self.assertEqual(
            [row["source_event_id"] for row in selected],
            ["shared", "latest"],
        )
        self.assertIs(selected[0], shared)
        self.assertIs(selected[1], latest)
        self.assertEqual(
            {
                currency: tuple(dict(row) for row in events)
                for currency, events in event_values.items()
            },
            before,
        )
        for currency in source_index:
            self.assertEqual(
                source_index[currency]["epochs"]._values,
                epoch_values[currency],
            )

    def test_source_index_load_is_bounded_to_move_knowledge_window(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "sources.sqlite"
            db = sqlite3.connect(path)
            db.execute(
                """CREATE TABLE source_events (
                    source_event_id TEXT PRIMARY KEY, source_id TEXT,
                    source_population TEXT, event_type TEXT,
                    story_cluster_id TEXT, effective_from_utc TEXT,
                    valid_until_utc TEXT, superseded_at_utc TEXT,
                    base_currency TEXT, quote_currency TEXT, payload_json TEXT
                )"""
            )
            for event_id, timestamp, currency in (
                ("old", "2025-01-01T00:00:00+00:00", "EUR"),
                ("inside", "2026-08-19T12:00:00+00:00", "USD"),
                ("new", "2026-08-20T00:00:00+00:00", "JPY"),
            ):
                db.execute(
                    "INSERT INTO source_events VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        event_id, "source", "official", "release", event_id,
                        timestamp, None, None, currency, None,
                        json.dumps({"currencies": [currency]}),
                    ),
                )
            db.execute(
                "CREATE INDEX ix_test_effective ON source_events(effective_from_utc)"
            )
            db.commit()
            db.close()
            index, highwater = census.load_source_index(
                path,
                minimum_effective_epoch=census.parse_epoch(
                    "2026-08-19T11:00:00+00:00"
                ),
                maximum_effective_epoch=census.parse_epoch(
                    "2026-08-19T13:00:00+00:00"
                ),
            )
            self.assertEqual(set(index), {"USD"})
            self.assertEqual(index["USD"]["events"][0]["source_event_id"], "inside")
            self.assertEqual(highwater, "2026-08-20T00:00:00+00:00")

    def test_recent_news_remap_reports_source_load_and_row_progress(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "sources.sqlite"
            db = sqlite3.connect(path)
            db.execute(
                """CREATE TABLE source_events (
                    source_event_id TEXT PRIMARY KEY, source_id TEXT,
                    source_population TEXT, event_type TEXT,
                    story_cluster_id TEXT, effective_from_utc TEXT,
                    valid_until_utc TEXT, superseded_at_utc TEXT,
                    base_currency TEXT, quote_currency TEXT, payload_json TEXT
                )"""
            )
            db.execute(
                "INSERT INTO source_events VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "event",
                    "official_eur",
                    "official",
                    "release",
                    "story",
                    "2026-08-19T11:55:00+00:00",
                    None,
                    None,
                    "EUR",
                    None,
                    json.dumps({"currencies": ["EUR"]}),
                ),
            )
            db.commit()
            db.close()
            row = census.base_row(
                inventory="prospective_h5_h15_path",
                move_id="move",
                instrument="EUR_USD",
                start_epoch=census.parse_epoch("2026-08-19T12:00:00+00:00"),
                end_epoch=census.parse_epoch("2026-08-19T12:15:00+00:00"),
                selected_side=1,
            )
            row["source_evidence_class"] = "prospective_forecast_path"
            progress = []

            result = census.remap_recent_news(
                [row],
                path,
                60,
                progress_callback=lambda **value: progress.append(value),
            )

            self.assertEqual(result["rows_remapped"], 1)
            self.assertEqual(row["pre_entry_source_count"], 1)
            self.assertEqual(
                [value["phase"] for value in progress],
                ["loading_causal_source_window", "remapping_recent_news", "remapping_recent_news"],
            )
            self.assertEqual(progress[-1]["rows_remapped"], 1)
            self.assertEqual(progress[-1]["rows_to_remap"], 1)

    def test_pre_entry_directional_count_excludes_recap_and_research_evidence(self):
        recap = {
            "payload_json": json.dumps(
                {
                    "raw_payload": {
                        "directional_evidence": True,
                        "directional_publish_eligible": True,
                        "reports_prior_market_move": True,
                    }
                }
            )
        }
        research = {
            "payload_json": json.dumps(
                {
                    "raw_payload": {
                        "directional_evidence": True,
                        "directional_publish_eligible": False,
                        "directional_research_only": True,
                    }
                }
            )
        }
        eligible = {
            "payload_json": json.dumps(
                {
                    "raw_payload": {
                        "directional_evidence": True,
                        "directional_publish_eligible": True,
                        "reports_prior_market_move": False,
                        "context_only": False,
                        "directional_research_only": False,
                    }
                }
            )
        }
        self.assertFalse(census.source_is_directional(recap))
        self.assertFalse(census.source_is_directional(research))
        self.assertTrue(census.source_is_directional(eligible))

    def test_detail_enrichment_cannot_be_used_before_available(self):
        effective, adjusted = census.causal_effective_epoch(
            "2026-08-10T05:14:58+00:00",
            json.dumps(
                {
                    "raw_payload": {
                        "detail_enriched": True,
                        "detail_available_utc": "2026-08-10T14:17:04+00:00",
                        "currency_scores": {"JPY": 1.0},
                    }
                }
            ),
        )
        self.assertTrue(adjusted)
        self.assertEqual(
            census.iso_epoch(effective), "2026-08-10T14:17:04+00:00"
        )

    def test_nonoverlap_keeps_largest_room(self):
        rows = [
            {"start_epoch": 0, "endpoint_after_cost_pips": 3.0},
            {"start_epoch": 60, "endpoint_after_cost_pips": 8.0},
            {"start_epoch": 600, "endpoint_after_cost_pips": 4.0},
        ]
        selected = census.nonoverlapping_extremes(rows, 300)
        self.assertEqual([row["start_epoch"] for row in selected], [60, 600])

    def test_factor_episode_deduplicates_common_jpy_shock(self):
        rows = [
            census.base_row(
                inventory="x", move_id="a", instrument="USD_JPY",
                start_epoch=1_000, end_epoch=1_900, selected_side=-1,
            ),
            census.base_row(
                inventory="x", move_id="b", instrument="EUR_JPY",
                start_epoch=1_020, end_epoch=1_920, selected_side=-1,
            ),
        ]
        for index, row in enumerate(rows):
            row["endpoint_after_cost_pips"] = float(index + 1)
        count = census.assign_factor_episodes(
            rows, {"factor_window_short_sec": 900, "factor_window_long_sec": 3600}
        )
        self.assertEqual(count, 1)
        self.assertEqual(rows[0]["factor_episode_id"], rows[1]["factor_episode_id"])
        self.assertEqual(rows[0]["factor_primary_token"], "JPY+")
        self.assertTrue(rows[1]["factor_representative"])

    def test_causal_strength_dedupes_huf_zar_grid_without_inflation(self):
        start = census.parse_epoch("2026-08-27T04:15:00Z")
        huf_end = census.parse_epoch("2026-08-27T05:10:00Z")
        zar_end = census.parse_epoch("2026-08-27T05:14:00Z")
        rows = [
            census.base_row(
                inventory="x",
                move_id=instrument,
                instrument=instrument,
                start_epoch=start,
                end_epoch=end,
                selected_side=1,
            )
            for instrument, end in (
                ("EUR_HUF", huf_end),
                ("USD_HUF", huf_end),
                ("EUR_ZAR", zar_end),
                ("USD_ZAR", zar_end),
            )
        ]
        for row, value in zip(rows, (15.44, 14.27, 9.81, 8.98)):
            row["endpoint_after_cost_pips"] = value
        surfaces = {
            (huf_end, 60): {
                "valid": True,
                "status": "ready",
                "as_of_utc": census.iso_epoch(huf_end),
                "as_of_age_sec": 0,
                "oldest_pair_age_sec": 0,
                "observation_count": 68,
                "expected_observation_count": 68,
                "coverage_pct": 100.0,
                "currency_count": 21,
                "missing_instruments": [],
                "missing_currencies": [],
                "source_contract_id": "test_all68",
                "currency_strength_bps": {
                    "EUR": 1.99,
                    "USD": 1.35,
                    "HUF": -7.40,
                },
            },
            (zar_end, 60): {
                "valid": True,
                "status": "ready",
                "as_of_utc": census.iso_epoch(zar_end),
                "as_of_age_sec": 0,
                "oldest_pair_age_sec": 0,
                "observation_count": 68,
                "expected_observation_count": 68,
                "coverage_pct": 100.0,
                "currency_count": 21,
                "missing_instruments": [],
                "missing_currencies": [],
                "source_contract_id": "test_all68",
                "currency_strength_bps": {
                    "EUR": 2.58,
                    "USD": 2.07,
                    "ZAR": -6.64,
                },
            },
        }

        count = census.assign_factor_episodes(
            rows,
            {"factor_window_short_sec": 900, "factor_window_long_sec": 3600},
            strength_surfaces=surfaces,
        )

        self.assertEqual(count, 2)
        self.assertEqual(
            [row["factor_primary_token"] for row in rows],
            ["HUF-", "HUF-", "ZAR-", "ZAR-"],
        )
        self.assertEqual(rows[0]["factor_episode_id"], rows[1]["factor_episode_id"])
        self.assertEqual(rows[2]["factor_episode_id"], rows[3]["factor_episode_id"])
        self.assertTrue(rows[0]["factor_representative"])
        self.assertFalse(rows[1]["factor_representative"])
        self.assertTrue(rows[2]["factor_representative"])
        self.assertFalse(rows[3]["factor_representative"])
        self.assertEqual(
            rows[0]["factor_primary_method"], "causal_all68_currency_strength"
        )
        self.assertEqual(
            rows[0]["factor_primary_scores_bps"], {"EUR+": 1.99, "HUF-": 7.4}
        )

    def test_archive_factor_surface_is_end_bound_and_missing_currency_falls_back(self):
        start = 1_000
        end = start + 15 * 60
        request = {(end, 15)}
        observations = {}

        def candle(epoch, mid):
            return {
                "epoch": epoch,
                "bid_close": mid - 0.00005,
                "ask_close": mid + 0.00005,
            }

        for instrument, old, current, future in (
            ("EUR_HUF", 100.0, 101.0, 95.0),
            ("USD_HUF", 100.0, 101.0, 95.0),
            ("EUR_USD", 1.0, 1.0, 1.0),
        ):
            census.collect_causal_factor_observations(
                instrument,
                [
                    candle(start - 60, old),
                    candle(end - 60, current),
                    # This bar opens at the watermark, so its close is future
                    # information and must never influence the surface.
                    candle(end, future),
                ],
                request,
                observations,
                maximum_pair_age_sec=0,
            )
        census.collect_causal_factor_observations(
            "EUR_TRY",
            [candle(start - 60, 50.0)],
            request,
            observations,
            maximum_pair_age_sec=0,
        )
        surfaces, meta = census.solve_causal_factor_strength_surfaces(
            request,
            observations,
            ["EUR_HUF", "USD_HUF", "EUR_USD", "EUR_TRY"],
            minimum_observation_count=3,
            expected_observation_count=4,
        )
        surface = surfaces[(end, 15)]

        self.assertTrue(surface["valid"])
        self.assertEqual(census.parse_epoch(surface["as_of_utc"]), end)
        self.assertLess(surface["currency_strength_bps"]["HUF"], 0)
        self.assertEqual(surface["missing_instruments"], ["EUR_TRY"])
        self.assertEqual(surface["missing_currencies"], ["TRY"])
        self.assertEqual(meta["valid_surface_count"], 1)

        huf = census.base_row(
            inventory="x",
            move_id="huf",
            instrument="EUR_HUF",
            start_epoch=start,
            end_epoch=end,
            selected_side=1,
        )
        missing = census.base_row(
            inventory="x",
            move_id="try",
            instrument="EUR_TRY",
            start_epoch=start,
            end_epoch=end,
            selected_side=1,
        )
        huf["endpoint_after_cost_pips"] = 2.0
        missing["endpoint_after_cost_pips"] = 1.0
        census.assign_factor_episodes(
            [huf, missing],
            {"factor_window_short_sec": 900, "factor_window_long_sec": 3600},
            strength_surfaces=surfaces,
        )
        self.assertEqual(
            huf["factor_primary_method"], "causal_all68_currency_strength"
        )
        self.assertEqual(huf["factor_primary_token"], "HUF-")
        self.assertEqual(
            missing["factor_primary_method"],
            "time_bucket_token_recurrence_fallback",
        )
        self.assertEqual(missing["factor_strength_missing_currencies"], ["TRY"])

    def test_action_branch_priority_can_use_factor_episode_representatives(self):
        rows = [
            {"action_branch": "direction_model", "factor_representative": False},
            {"action_branch": "direction_model", "factor_representative": True},
            {"action_branch": "source_latency", "factor_representative": True},
        ]
        self.assertEqual(
            census.counter_rows(rows, "action_branch", representatives=True),
            [
                {"action_branch": "direction_model", "count": 1},
                {"action_branch": "source_latency", "count": 1},
            ],
        )

    def test_unavailable_history_is_not_called_model_miss(self):
        row = census.base_row(
            inventory="legacy_significant_2h_labels",
            move_id="old",
            instrument="EUR_USD",
            start_epoch=1_000,
            end_epoch=8_200,
            selected_side=0,
        )
        index = {
            "minimum": {7200: 10_000}, "maximum": {7200: 20_000},
            "by_pair_horizon": {}, "by_horizon": {},
            "times_pair_horizon": {}, "times_horizon": {},
        }
        census.apply_gap_classification(row, index, {})
        self.assertEqual(row["primary_gap"], "historical_direction_not_retained")
        self.assertIn("historical_replay_required", row["gap_tags"])
        self.assertNotIn("selected_wrong_direction", row["gap_tags"])

    def test_top_signal_wrong_direction_is_explicit(self):
        signal = {
            "direction": -1,
            "signal_eligible": False,
            "validated": False,
            "direction_conflict": False,
            "projected_net_pips": 0.0,
            "blocked_by": "[]",
        }
        self.assertEqual(census.classify_top_signal(signal, 1), "selected_wrong_direction")

    def test_prospective_correct_direction_reports_first_failed_gate(self):
        forecast = {
            "direction": 1,
            "passed_frozen_gate": False,
            "predicted_clear_probability": 0.20,
            "predicted_direction_confidence": 0.50,
            "predicted_magnitude_pips": 10.0,
            "predicted_ev_pips": 2.0,
            "modeled_entry_cost_pips": 2.0,
        }
        result = census.classify_prospective(
            forecast,
            1,
            {
                "minimum_clear_probability": 0.55,
                "minimum_direction_confidence": 0.10,
                "minimum_predicted_magnitude_cost_ratio": 1.5,
            },
        )
        self.assertEqual(result, "correct_direction_low_cost_clearance")

    def test_h1_consensus_uses_one_vote_per_family(self):
        models = [
            {"family": "a", "generated_epoch": 1, "direction": 1, "probability_up": 0.7, "predicted_magnitude_pips": 4},
            {"family": "a", "generated_epoch": 2, "direction": -1, "probability_up": 0.3, "predicted_magnitude_pips": 5},
            {"family": "b", "generated_epoch": 1, "direction": -1, "probability_up": 0.4, "predicted_magnitude_pips": 3},
        ]
        result = census.summarize_h1_models(models, -1)
        self.assertEqual(result["model_count"], 2)
        self.assertEqual(result["short_votes"], 2)
        self.assertTrue(result["consensus_correct"])

    def test_headline_liquidity_bucket_excludes_wide_cost_rows(self):
        row = census.base_row(
            inventory="x", move_id="wide", instrument="EUR_TRY",
            start_epoch=1_000, end_epoch=1_900, selected_side=1,
        )
        row["modeled_cost_pips"] = 25.0
        row["endpoint_after_cost_pips"] = 100.0
        census.assign_liquidity_bucket(row)
        self.assertEqual(row["liquidity_bucket"], "wide_cost")
        self.assertEqual(row["after_cost_multiple"], 4.0)

    def test_report_does_not_present_cluster_token_as_observed_factor_rank(self):
        source = Path(census.__file__).read_text(encoding="utf-8")
        self.assertIn(
            "synchronized 21-currency strength surface",
            source,
        )

    def test_legacy_replay_recovers_only_executable_archive_overlap(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "EUR_USD_M1.csv"
            columns = [
                "time", "bid_open", "bid_high", "bid_low", "bid_close",
                "ask_open", "ask_high", "ask_low", "ask_close",
            ]
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=columns)
                writer.writeheader()
                writer.writerow({
                    "time": "2026-01-01T00:00:00Z",
                    "bid_open": 1.1000, "bid_high": 1.1012, "bid_low": 1.0998, "bid_close": 1.1010,
                    "ask_open": 1.1002, "ask_high": 1.1014, "ask_low": 1.1000, "ask_close": 1.1012,
                })
                writer.writerow({
                    "time": "2026-01-01T00:01:00Z",
                    "bid_open": 1.1010, "bid_high": 1.1022, "bid_low": 1.1008, "bid_close": 1.1020,
                    "ask_open": 1.1012, "ask_high": 1.1024, "ask_low": 1.1010, "ask_close": 1.1022,
                })
            covered = census.base_row(
                inventory="legacy_significant_2h_labels", move_id="covered",
                instrument="EUR_USD", start_epoch=census.parse_epoch("2026-01-01T00:00:00Z"),
                end_epoch=census.parse_epoch("2026-01-01T00:01:00Z"), selected_side=0,
            )
            unavailable = census.base_row(
                inventory="legacy_significant_2h_labels", move_id="unavailable",
                instrument="EUR_USD", start_epoch=census.parse_epoch("2025-01-01T00:00:00Z"),
                end_epoch=census.parse_epoch("2025-01-01T00:01:00Z"), selected_side=0,
            )
            progress = []
            result = census.recover_legacy_executable_paths(
                [covered, unavailable],
                root,
                tolerance_sec=1,
                progress_callback=lambda **value: progress.append(value),
            )
            self.assertEqual(result["recovered_rows"], 1)
            self.assertEqual(result["unavailable_rows"], 1)
            self.assertEqual(covered["selected_side"], 1)
            self.assertAlmostEqual(covered["gross_magnitude_pips"], 20.0)
            self.assertAlmostEqual(covered["endpoint_after_cost_pips"], 18.0)
            self.assertEqual(covered["legacy_replay_status"], "recovered_executable_bam")
            self.assertEqual(unavailable["legacy_replay_status"], "executable_archive_unavailable")
            instrument_progress = [
                value
                for value in progress
                if value.get("phase") == "recovering_legacy_executable_paths"
            ]
            self.assertGreaterEqual(len(instrument_progress), 2)
            self.assertEqual(instrument_progress[0]["completed_instruments"], 0)
            self.assertEqual(instrument_progress[-1]["completed_instruments"], 1)
            self.assertEqual(
                progress[-1]["phase"], "legacy_executable_replay_complete"
            )


if __name__ == "__main__":
    unittest.main()
