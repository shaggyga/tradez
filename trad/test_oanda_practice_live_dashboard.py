import json
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import trad.oanda_practice_live_dashboard as dashboard
from trad.oanda_practice_live_dashboard import (
    build_top_signal_horizon_matrix,
    build_signal_component_matrix,
    canonical_horizon_label,
    curve_for_direction,
    heartbeat_status,
    load_equation_path,
    overlay_primary_account,
    resolve_display_signals,
    signal_freshness_sec,
    aggregate_prediction_accuracy_rows,
    summarize_arima_accuracy_grid,
    summarize_timeframe_calibration_accuracy_grid,
    summarize_prediction_ledger,
    summarize_primary_practice_account,
    summarize_signal_families,
    summarize_account_snapshot,
    summarize_historical_calibration,
    summarize_historical_strategy_accuracy_grid,
    summarize_ma_feature_grid_accuracy,
    summarize_model_gap_accuracy_grid,
    summarize_model_gap_completion,
    summarize_live_movers,
    summarize_live_move_news,
    summarize_continuous_narrative,
    summarize_adaptive_level_bands,
    summarize_lab,
    summarize_primary_signal_system,
    summarize_signal_feed,
    summarize_vault_checkpoint,
    summarize_vault_model_docs,
)


class PatternDashboardTests(unittest.TestCase):
    def test_primary_executable_census_reads_only_prospective_cohort_g(self):
        self.assertEqual(
            dashboard.EXECUTABLE_MOVE_CENSUS.name,
            "executable_move_census_latest_v3_20260902g.json",
        )

    def test_adaptive_level_summary_remains_outside_execution_surface(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "levels.json"
            path.write_text(json.dumps({
                "schema_version": "causal_level_band_prospective_v1",
                "contract_id": "contract",
                "cohort_id": "cohort",
                "generated_utc": datetime.now(timezone.utc).isoformat(),
                "status": "running",
                "instrument_count": 68,
                "ready_context_count": 61,
                "blocked_context_count": 7,
                "current_valid_quote_band_count": 1,
                "ledger": {
                    "forecasts": 4,
                    "entries": 3,
                    "outcomes": 2,
                    "censors": 0,
                    "response_labels": {"contact_reject": 1},
                    "horizon_cells": [{
                        "horizon_sec": 900,
                        "n": 2,
                        "bounce_avg_net_pips": 1.2,
                        "break_avg_net_pips": -2.0,
                        "bounce_after_cost_win_rate": 0.5,
                        "break_after_cost_win_rate": 0.0,
                    }],
                },
                "top_approaching_bands": [{
                    "instrument": "EUR_USD",
                    "band_id": "band1",
                    "band_version_id": "bandv1",
                    "source": "confirmed_pivot_cluster",
                    "name": "swing_cluster",
                    "origin_kind": "pivot_high",
                    "physical_role": "resistance",
                    "band_lower": 1.1,
                    "band_center": 1.101,
                    "band_upper": 1.102,
                    "anchor_count": 3,
                    "distance_pips": 1.5,
                    "distance_atr": 0.2,
                    "approach_zone_pips": 3.0,
                    "velocity_1_pips_per_min": 0.4,
                    "velocity_3_pips_per_min": 0.3,
                    "acceleration_pips_per_min2": 0.1,
                    "time_to_contact_min": 5.0,
                    "path_efficiency_5": 0.8,
                    "monotonicity_5": 0.6,
                    "impulse_toward_band_atr": 0.3,
                    "equilibrium_stretch_atr": 1.0,
                    "atr_m5_pips": 5.0,
                    "spread_pips": 1.2,
                    "state": "approaching",
                    "armed": False,
                    "bounce_hypothesis": "short",
                    "break_hypothesis": "long",
                    "response_probability_state": "absent_no_frozen_model",
                    "empirical_cost_clearance_state": "unknown_collecting_prospective_outcomes",
                }],
            }), encoding="utf-8")
            result = summarize_adaptive_level_bands(path)

        self.assertTrue(result["fresh"])
        self.assertEqual(result["rows"][0]["instrument"], "EUR_USD")
        self.assertEqual(result["forecasts"], 4)
        self.assertTrue(result["research_only"])
        self.assertFalse(result["execution_eligible"])
        self.assertFalse(result["can_authorize"])
        self.assertFalse(result["response_probabilities_available"])
        self.assertNotIn("units", result["rows"][0])
        self.assertNotIn("action", result["rows"][0])

    def test_continuous_narrative_summary_is_observation_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "narrative.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": "continuous_currency_narrative_meter_v2",
                        "meter_contract_id": "meter-contract",
                        "generated_utc": datetime.now(timezone.utc).isoformat(),
                        "clock_utc": "2026-08-24T16:20:00Z",
                        "currencies": {
                            "USD": {
                                "score": 0.5,
                                "direction": "POSITIVE",
                                "active_story_count": 3,
                                "source_family_count": 2,
                                "agreement": 0.8,
                                "attention_acceleration": 0.2,
                                "evidence_class": "trusted",
                            },
                            "EUR": {
                                "score": -0.4,
                                "direction": "NEGATIVE",
                                "active_story_count": 2,
                                "source_family_count": 1,
                                "agreement": 0.7,
                                "attention_acceleration": -0.1,
                                "evidence_class": "trusted",
                            },
                        },
                        "pairs": {
                            "EUR_USD": {
                                "score": -0.9,
                                "direction": "SHORT",
                            }
                        },
                        "models": {"narrative_acceleration_v1": {"status": "active"}},
                    }
                ),
                encoding="utf-8",
            )
            result = summarize_continuous_narrative(path)

        self.assertTrue(result["fresh"])
        self.assertTrue(result["research_only"])
        self.assertFalse(result["execution_eligible"])
        self.assertEqual(result["practice_account_scope"], "007_observation_only")
        self.assertEqual(result["strongest"][0]["currency"], "USD")
        self.assertEqual(result["weakest"][0]["currency"], "EUR")
        self.assertEqual(result["top_pair_hypotheses"][0]["instrument"], "EUR_USD")

    def test_oanda_html_renders_continuous_narrative_meter(self):
        html_path = Path(dashboard.__file__).with_name(
            "oanda_main_signal_dashboard.html"
        )
        html = html_path.read_text(encoding="utf-8")

        self.assertIn('id="narrative-meter"', html)
        self.assertIn("function renderNarrativeMeter", html)
        self.assertIn("Continuous narrative meter", html)
        self.assertIn("Research hypothesis — not validated edge", html)
        self.assertIn("renderNarrativeMeter(data)", html)
        self.assertNotIn("Practice 006", html)

    def test_oanda_html_renders_adaptive_level_band_panel(self):
        html_path = Path(dashboard.__file__).with_name(
            "oanda_main_signal_dashboard.html"
        )
        html = html_path.read_text(encoding="utf-8")

        self.assertIn('id="adaptive-level-bands"', html)
        self.assertIn("function renderAdaptiveLevels", html)
        self.assertIn("Adaptive support / resistance collisions", html)
        self.assertIn("These are not probabilities", html)
        self.assertIn("renderAdaptiveLevels(data)", html)

    def test_live_mover_summary_exposes_reranking_charts_and_currency_breadth(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "moves.json"
            now = time.time()
            rows = [
                {
                    "instrument": "EUR_USD",
                    "move_5m_bps": 3.0,
                    "move_5m_pips": 3.3,
                    "move_15m_bps": 5.0,
                    "move_15m_pips": 5.5,
                    "move_60m_bps": 8.0,
                    "move_60m_pips": 8.8,
                    "current_spread_pips": 1.2,
                    "chart_points": [[int(now) - 60, 0.0], [int(now), 3.0]],
                },
                {
                    "instrument": "USD_JPY",
                    "move_5m_bps": -4.0,
                    "move_5m_pips": -6.0,
                    "move_15m_bps": -6.0,
                    "move_15m_pips": -9.0,
                    "move_60m_bps": -9.0,
                    "move_60m_pips": -13.5,
                    "current_spread_pips": 1.4,
                    "chart_points": [[int(now) - 60, 0.0], [int(now), -4.0]],
                },
            ]
            velocity = {
                "instrument": "EUR_USD",
                "state": "active",
                "signed_move_bps": 6.0,
                "signed_move_pips": 6.6,
                "velocity_bps_per_hour": 24.0,
                "absolute_velocity_bps_per_hour": 24.0,
                "duration_minutes": 15,
                "executable_net_pips": 5.4,
                "effective_cost_pips": 1.2,
                "path_efficiency": 0.8,
                "chart_points": [[int(now) - 900, 0.0], [int(now), 6.0]],
            }
            path.write_text(
                json.dumps(
                    {
                        "generated_utc": "2026-08-18T23:00:00Z",
                        "instrument_count": 68,
                        "directional_history_complete_instrument_count": 65,
                        "directional_leg_count": 123,
                        "round_trip_cost_clear_count": 61,
                        "rows": rows,
                        "rankings": {
                            "live_velocity": [velocity],
                            "latest_5m": rows,
                            "latest_15m": rows,
                            "latest_60m": rows,
                        },
                    }
                ),
                encoding="utf-8",
            )

            # Keep this legacy-payload fixture isolated from the canonical
            # live executable census that may exist on the developer host.
            # The companion fixed-horizon test below supplies its own census.
            with patch.object(
                dashboard,
                "EXECUTABLE_MOVE_CENSUS",
                Path(temporary) / "missing_executable_census.json",
            ):
                summary = summarize_live_movers(path)

        self.assertEqual(summary["status"], "collecting")
        self.assertEqual(summary["modes"]["velocity"][0]["instrument"], "EUR_USD")
        self.assertEqual(len(summary["modes"]["velocity"][0]["chart_points"]), 2)
        self.assertEqual(summary["modes"]["5m"][0]["instrument"], "USD_JPY")
        self.assertEqual(summary["breadth"]["5m"], {"up": 1, "down": 1, "flat": 0, "covered": 2})
        self.assertTrue(summary["research_only"])
        self.assertFalse(summary["execution_eligible"])

    def test_oanda_html_renders_live_reranking_mover_charts(self):
        html_path = Path(dashboard.__file__).with_name(
            "oanda_main_signal_dashboard.html"
        )
        html = html_path.read_text(encoding="utf-8")

        self.assertIn('id="live-movers"', html)
        self.assertIn("function renderLiveMovers", html)
        self.assertIn("function moverSparkline", html)
        self.assertIn("exec5", html)
        self.assertIn("aria-pressed", html)
        self.assertIn("Historical observed moves", html)
        self.assertIn("these are not bot trades or predictions", html)
        self.assertNotIn("Legacy velocity", html)
        self.assertNotIn("Live velocity", html)
        self.assertIn("button.mover-mode", html)
        self.assertIn("renderLiveMovers(data)", html)

    def test_live_mover_summary_prefers_fixed_horizon_census(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            latest = root / "latest.json"
            census = root / "census.json"
            latest.write_text(json.dumps({"generated_utc": "2026-08-30T21:00:00Z"}), encoding="utf-8")
            census.write_text(json.dumps({
                "generated_utc": "2026-08-30T21:05:00Z",
                "side_count": 136,
                "horizons_min": [5],
                "frame_count": 6,
                "latest_frame_utc": "2026-08-30T21:05:00Z",
                "schedule_census": {"expected_open_frames": 6, "observed_frames": 6, "missing_open_frames": 0},
                "review_queue": {"total_cleared_arm_observations": 3, "distinct_factor_episode_cases": 2},
                "horizons": [{
                    "horizon_min": 5,
                    "expected_side_count": 136,
                    "valid_count": 130,
                    "cleared_count": 1,
                    "invalid_count": 6,
                    "pending_count": 0,
                    "rows": [{
                        "instrument": "EUR_USD", "side": "long", "state": "cleared",
                        "net_pips": 2.5, "net_bps": 2.2,
                        "entry_scheduled_utc": "2026-08-30T21:00:00Z",
                        "exit_scheduled_utc": "2026-08-30T21:05:00Z",
                        "path_points": [{"scheduled_utc": "2026-08-30T21:01:00Z", "net_pips": 0.4}, {"scheduled_utc": "2026-08-30T21:05:00Z", "net_pips": 2.5}],
                    }],
                }],
            }), encoding="utf-8")
            with patch.object(dashboard, "EXECUTABLE_MOVE_CENSUS", census):
                summary = summarize_live_movers(latest)

        self.assertEqual(summary["status"], "live")
        self.assertEqual(summary["generated_utc"], "2026-08-30T21:05:00Z")
        self.assertEqual(summary["modes"]["exec5"][0]["instrument"], "EUR_USD")
        self.assertEqual(summary["modes"]["exec5"][0]["duration_minutes"], 5)
        self.assertEqual(summary["executable_census"]["mode_stats"]["exec5"]["expected_side_count"], 136)
        self.assertEqual(summary["executable_census"]["review_queue"]["total_cleared_arm_observations"], 3)

    def test_live_mover_summary_does_not_label_pre_activation_as_live(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            latest = root / "latest.json"
            census = root / "census.json"
            latest.write_text("{}", encoding="utf-8")
            census.write_text(
                json.dumps(
                    {
                        "schema_version": "executable_move_census_latest_v1",
                        "status": "collecting_pre_activation",
                        "generated_utc": datetime.now(timezone.utc).isoformat(),
                        "side_count": 136,
                        "horizons_min": [1, 5, 10, 15, 30, 60],
                        "frame_count": 0,
                        "horizons": [],
                    }
                ),
                encoding="utf-8",
            )
            with patch.object(dashboard, "EXECUTABLE_MOVE_CENSUS", census):
                summary = summarize_live_movers(latest)

        self.assertEqual(summary["status"], "collecting")
        self.assertEqual(summary["executable_census"]["status"], "collecting")

    def test_live_move_news_summary_preserves_evidence_boundaries(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "move_news.json"
            path.write_text(
                json.dumps(
                    {
                        "generated_utc": datetime.now(timezone.utc).isoformat(),
                        "mover_count": 1,
                        "strict_directional_mover_count": 0,
                        "strict_aligned_mover_count": 0,
                        "movers": [
                            {
                                "instrument": "EUR_USD",
                                "move_direction": "up",
                                "executable_net_pips": 3.2,
                                "duration_minutes": 8,
                                "strict_forward_alignment": "no_strict_direction",
                                "explanation_state": "research_context_aligned_not_publishable",
                                "factor_primary_token": "USD+",
                                "factor_representative": True,
                                "strict_forward_independent_story_count": 0,
                                "pre_move_event_count": 12,
                                "broad_research_side": "long",
                                "continuous_narrative_state": {
                                    "direction": "NEUTRAL",
                                    "score": 0.0,
                                },
                                "recent_context_stories": [
                                    {
                                        "headline": "Nearby context only",
                                        "source_id": "discovery",
                                    }
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            summary = summarize_live_move_news(path)

        self.assertTrue(summary["fresh"])
        self.assertEqual(summary["rows"][0]["strict_forward_story_count"], 0)
        self.assertEqual(
            summary["rows"][0]["explanation_state"],
            "research_context_aligned_not_publishable",
        )
        self.assertEqual(summary["rows"][0]["factor_primary_token"], "USD+")
        self.assertTrue(summary["rows"][0]["factor_representative"])
        self.assertEqual(summary["rows"][0]["raw_event_count"], 12)
        self.assertEqual(summary["rows"][0]["context_headline"], "Nearby context only")
        self.assertTrue(summary["research_only"])
        self.assertFalse(summary["execution_eligible"])
        self.assertIn("forward_outcomes", summary)
        self.assertFalse(summary["forward_outcomes"]["execution_eligible"])

    def test_oanda_html_renders_live_move_news_attribution(self):
        html_path = Path(dashboard.__file__).with_name(
            "oanda_main_signal_dashboard.html"
        )
        html = html_path.read_text(encoding="utf-8")

        self.assertIn('id="live-move-news"', html)
        self.assertIn("function renderLiveMoveNews", html)
        self.assertIn("Did current news explain the observed move?", html)
        self.assertIn("Quote-bound forward proof", html)
        self.assertIn("factor_representative_average_after_cost_spread_multiple", html)
        self.assertIn("renderLiveMoveNews(data)", html)

    def test_oanda_dashboard_is_summary_first_with_collapsed_deep_research(self):
        html_path = Path(dashboard.__file__).with_name(
            "oanda_main_signal_dashboard.html"
        )
        html = html_path.read_text(encoding="utf-8")

        self.assertIn('id="oanda-deep-research"', html)
        self.assertIn('id="oanda-evidence-proof"', html)
        self.assertIn("Historical observations &amp; research", html)
        self.assertIn("Research &amp; diagnostics", html)
        self.assertIn('id="signal-matrix"', html)
        self.assertIn("function renderSignalMatrix", html)
        self.assertIn("liveMatrixMode='live'", html)
        self.assertIn("Current forecast separated from matured holdout evidence", html)
        self.assertIn("Decision board", html)
        self.assertIn("collapsed by default", html)
        self.assertNotIn('<details id="oanda-deep-research" class="dashboard-deep" open', html)
        self.assertLess(html.index('id="account"'), html.index('id="top"'))
        self.assertLess(html.index('id="live-movers"'), html.index('id="top"'))
        self.assertLess(html.index('id="oanda-evidence-proof"'), html.index('id="live-movers"'))







    def test_sanitize_payload_replaces_nested_non_finite_numbers(self):
        payload = {
            "finite": 1.25,
            "values": [float("nan"), float("inf"), -float("inf")],
        }

        sanitized = dashboard.sanitize_payload(payload)

        self.assertEqual(sanitized["finite"], 1.25)
        self.assertEqual(sanitized["values"], [None, None, None])
        json.dumps(sanitized, allow_nan=False)

    def test_atomic_cache_write_sanitizes_non_finite_numbers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cache.json"
            dashboard.write_json_atomic(
                path,
                {"top_signals": [{"normalized_rank_score": -float("inf")}]},
            )
            payload = json.loads(path.read_text(encoding="utf-8"))

        self.assertIsNone(payload["top_signals"][0]["normalized_rank_score"])

    def test_historical_sweep_prefers_full_span_v2_and_falls_back_to_v1(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_root = Path(temporary)
            state_dir = data_root / "state"
            state_dir.mkdir()
            v1 = state_dir / "timeframe_horizon_sweep_v1.json"
            v2 = state_dir / "timeframe_horizon_sweep_full_span_v2.json"
            v1.write_text(json.dumps({"schema_version": 1, "status": "complete"}))

            payload, path = dashboard.load_historical_sweep_state(data_root)
            self.assertEqual(path, v1)
            self.assertEqual(payload["schema_version"], 1)

            v2.write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "status": "running",
                        "history_coverage": "full_available_span",
                    }
                )
            )
            payload, path = dashboard.load_historical_sweep_state(data_root)
            self.assertEqual(path, v2)
            self.assertEqual(payload["history_coverage"], "full_available_span")

    def test_worker_heartbeat_health_is_independent_from_log_age(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "heartbeat.json"
            path.write_text(
                json.dumps({"status": "running", "phase": "maturing_outcomes"}),
                encoding="utf-8",
            )
            status = heartbeat_status(path)

        self.assertTrue(status["fresh"])
        self.assertEqual(status["phase"], "maturing_outcomes")

    def test_full_state_is_bounded_and_cached(self):
        class TestHandler(dashboard.DashboardHandler):
            log_dir = Path("logs")
            max_runs = 20
            max_lines = 20000
            state_cache = None
            state_cache_at = 0.0

        with patch.object(dashboard, "build_state", return_value={"ok": True}) as build:
            self.assertEqual(TestHandler.current_state(), {"ok": True})
            self.assertEqual(TestHandler.current_state(), {"ok": True})

        build.assert_called_once_with(Path("logs"), 8, 5000)

    def test_vault_checkpoint_uses_manifest_without_recursive_walk(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            nested = root / "large" / "archive"
            nested.mkdir(parents=True)
            (nested / "not-in-checkpoint.bin").write_bytes(b"ignored")
            (root / "forex_model_checkpoint_current.manifest.json").write_text(
                json.dumps(
                    {
                        "created_utc": "2026-07-17T20:00:00+00:00",
                        "content_sha256": "abc123",
                        "file_count": 1,
                        "total_bytes": 12,
                        "files": [
                            {
                                "path": "docs/model.md",
                                "size": 12,
                                "sha256": "def456",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            summary = summarize_vault_checkpoint(root)

        self.assertEqual(summary["file_count"], 1)
        self.assertEqual(summary["content_sha256"], "abc123")
        self.assertEqual(summary["files"][0]["path"], "docs/model.md")
        self.assertEqual(summary["files"][0]["size_bytes"], 12)

    def test_vault_model_docs_are_bounded_to_model_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "session_momentum.md").write_text(
                """# Session Momentum

- Family ID: `session_momentum`
- Taxonomy: `rule_strategy`
- Variants: 1
- Runs: 177
- Source-of-truth state: `no_fully_validated_source_of_truth`
- Reference run: `run_example`
- Timeframes/horizons: H1
""",
                encoding="utf-8",
            )
            nested = root / "unrelated"
            nested.mkdir()
            (nested / "ignored.md").write_text("# Ignored", encoding="utf-8")

            rows = summarize_vault_model_docs(root)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["family"], "session_momentum")
        self.assertEqual(rows[0]["runs"], 177)
        self.assertEqual(rows[0]["source_state"], "no_fully_validated_source_of_truth")

    def test_dedicated_primary_snapshot_replaces_only_007(self):
        aggregate = {
            "accounts": [
                {"account_id": "101-001-1-005", "balance": "10"},
                {"account_id": "101-001-1-007", "balance": "20"},
            ],
            "aggregate": {"account_count": 2},
        }
        dedicated = {
            "time": "2026-07-16T12:00:00Z",
            "accounts": [{"account_id": "101-001-1-007", "balance": "21"}],
        }
        merged = overlay_primary_account(aggregate, dedicated)
        by_id = {row["account_id"]: row for row in merged["accounts"]}
        self.assertEqual(by_id["101-001-1-005"]["balance"], "10")
        self.assertEqual(by_id["101-001-1-007"]["balance"], "21")
        self.assertEqual(merged["aggregate"]["account_count"], 2)

    def test_primary_account_summary_preserves_legacy_valid_snapshot(self):
        payload = {
            "time": "2026-08-28T21:59:56Z",
            "accounts": [
                {
                    "account_id": "101-001-37981792-007",
                    "env": "practice",
                    "ok": True,
                    "balance": "41.6042",
                    "NAV": "41.7042",
                    "pl": "-8.343",
                    "unrealizedPL": "0.1",
                    "marginUsed": "1.0",
                    "marginAvailable": "40.7042",
                    "openTradeCount": 0,
                    "pendingOrderCount": 0,
                    "trades": [],
                }
            ],
            "aggregate": {},
        }

        result = summarize_primary_practice_account(payload)

        self.assertTrue(result["ok"])
        self.assertTrue(result["account_values_current"])
        self.assertTrue(result["positions_current"])
        self.assertTrue(result["orders_current"])
        self.assertEqual(result["balance"], 41.6042)
        self.assertEqual(result["total_pl"], -8.243)
        self.assertEqual(result["open_trades"], 0)
        self.assertEqual(result["pending_orders"], 0)
        self.assertEqual(result["positions"], [])

    def test_primary_account_summary_does_not_turn_503_into_zero_or_flat(self):
        last_verified = {
            "state": "retained_stale",
            "fields": {
                "nav": 41.6042,
                "balance": 41.6042,
                "pl": -8.343,
                "unrealizedPL": 0.0,
            },
            "positions_orders_retained": False,
        }
        payload = {
            "time": "2026-08-28T23:12:44Z",
            "accounts": [
                {
                    "account_id": "101-001-37981792-007",
                    "env": "practice",
                    "ok": False,
                    "status_code": 503,
                    "error": "System under maintenance",
                    "account_values_current": False,
                    "positions_current": False,
                    "orders_current": False,
                }
            ],
            "aggregate": {
                "account_count": 1,
                "snapshot_state": "retained_stale_account_values",
                "account_values_current": False,
                "positions_current": False,
                "orders_current": False,
                "last_verified": last_verified,
            },
        }

        result = summarize_primary_practice_account(payload)

        self.assertFalse(result["ok"])
        self.assertFalse(result["broker_ok"])
        self.assertEqual(result["snapshot_state"], "retained_stale_account_values")
        self.assertEqual(result["status_code"], 503)
        self.assertEqual(result["error"], "System under maintenance")
        for field in (
            "balance",
            "nav",
            "realized_pl",
            "unrealized_pl",
            "total_pl",
            "margin_used",
            "margin_available",
            "margin_used_pct",
            "open_trades",
            "pending_orders",
            "positions",
        ):
            self.assertIsNone(result[field], field)
        self.assertEqual(result["last_verified"], last_verified)

    def test_account_snapshot_aggregate_marks_failed_read_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshot = (
                Path(directory)
                / "data"
                / "oanda_training_manager"
                / "state"
                / "account_dashboard_v1.json"
            )
            snapshot.parent.mkdir(parents=True)
            snapshot.write_text(
                json.dumps(
                    {
                        "accounts": [
                            {
                                "account_id": "101-001-37981792-007",
                                "ok": False,
                                "status_code": 503,
                                "error": "maintenance",
                                "account_values_current": False,
                                "positions_current": False,
                                "orders_current": False,
                            }
                        ],
                        "aggregate": {
                            "account_count": 1,
                            "snapshot_state": "unavailable",
                            "account_values_current": False,
                            "positions_current": False,
                            "orders_current": False,
                        },
                    }
                ),
                encoding="utf-8",
            )

            result = summarize_account_snapshot(snapshot)

        account = result["accounts"][0]
        paper = result["environment_aggregates"]["paper"]
        self.assertFalse(result["active"])
        self.assertFalse(account["account_values_current"])
        self.assertIsNone(account["balance"])
        self.assertIsNone(account["NAV"])
        self.assertIsNone(account["openTradeCount"])
        self.assertIsNone(account["pendingOrderCount"])
        self.assertEqual(paper["account_count"], 1)
        self.assertEqual(paper["current_account_count"], 0)
        self.assertFalse(paper["current"])
        self.assertIsNone(paper["average_balance"])
        self.assertIsNone(paper["average_nav"])
        self.assertIsNone(paper["open_trades"])
        self.assertIsNone(paper["pending_orders"])

    def test_oanda_html_distinguishes_unavailable_account_from_flat(self):
        html_path = Path(dashboard.__file__).with_name(
            "oanda_main_signal_dashboard.html"
        )
        html = html_path.read_text(encoding="utf-8")

        self.assertIn("Current broker state unavailable", html)
        self.assertIn("not confirmed flat", html)
        self.assertIn("Position/order state unavailable", html)
        self.assertIn("account_values_current", html)
        self.assertNotIn("`${a.open_trades||0}`", html)

    def test_empty_live_snapshot_retains_last_observed_signal(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cache = root / "last_signal.json"
            current = {
                "updated_at": "2026-07-16T05:00:00+00:00",
                "top_signals": [{"instrument": "EUR_USD", "direction": "buy"}],
            }
            first = resolve_display_signals(current, root, cache)
            retained = resolve_display_signals(
                {"updated_at": "2026-07-16T05:01:00+00:00", "top_signals": []},
                root,
                cache,
            )

        self.assertEqual(first["rows"][0]["instrument"], "EUR_USD")
        self.assertEqual(retained["rows"][0]["instrument"], "EUR_USD")
        self.assertFalse(retained["live"])
        self.assertEqual(retained["source"], "persistent_cache")

    def test_signal_freshness_tracks_horizon_with_a_bounded_window(self):
        self.assertEqual(signal_freshness_sec({"preferred_horizon_sec": 60}), 90.0)
        self.assertEqual(signal_freshness_sec({"preferred_horizon_sec": 120}), 120.0)
        self.assertEqual(signal_freshness_sec({"preferred_horizon_sec": 14400}), 300.0)

    def test_h24_is_the_d1_alias_not_a_second_bucket(self):
        self.assertEqual(canonical_horizon_label(86400), "D1")
        signal = {
            "instrument": "EUR_USD",
            "direction": "buy",
            "execution_validation": {"validated": True},
            "horizon_breakdown": [
                {
                    "horizon_sec": 86400,
                    "direction": "buy",
                    "signal_confidence": 0.61,
                    "projected_net_pips": 8.0,
                    "projected_net_pips_per_hour": 0.33,
                    "gross_to_spread": 2.0,
                    "family_count": 3,
                    "signal_eligible": True,
                }
            ],
        }

        matrix = build_top_signal_horizon_matrix([signal])

        self.assertEqual(len(matrix["rows"]), 1)
        self.assertEqual(matrix["rows"][0]["horizon_label"], "D1")
        self.assertTrue(matrix["horizon_alias_policy"]["deduplicated"])

    def test_direction_conflicted_projection_remains_blocked(self):
        signal = {
            "instrument": "USD_CAD",
            "direction": "sell",
            "direction_conflict": True,
            "execution_validation": {"validated": False},
            "horizon_breakdown": [
                {
                    "horizon_sec": 86400,
                    "direction": "sell",
                    "signal_confidence": 0.5,
                    "projected_net_pips": 71.943,
                    "projected_net_pips_per_hour": 2.998,
                    "gross_to_spread": 10.0,
                    "family_count": 17,
                    "signal_eligible": False,
                    "signal_blocked_by": ["unvalidated_signal"],
                }
            ],
        }

        row = build_top_signal_horizon_matrix([signal])["rows"][0]

        self.assertEqual(row["state"], "blocked")
        self.assertEqual(row["reason"], "direction conflict")
        self.assertTrue(row["research_only"])

    def test_top_horizon_matrix_omits_disabled_subminute_inputs(self):
        signal = {
            "instrument": "EUR_USD",
            "direction": "buy",
            "execution_validation": {"validated": True},
            "horizon_breakdown": [
                {
                    "horizon_sec": 300,
                    "direction": "buy",
                    "best_input_timeframe": "S10",
                    "signal_confidence": 0.9,
                    "projected_net_pips": 2.0,
                    "signal_eligible": True,
                },
                {
                    "horizon_sec": 600,
                    "direction": "buy",
                    "best_input_timeframe": "M1",
                    "signal_confidence": 0.6,
                    "projected_net_pips": 1.0,
                    "signal_eligible": True,
                },
            ],
        }

        rows = build_top_signal_horizon_matrix([signal])["rows"]

        self.assertEqual([row["horizon_sec"] for row in rows], [600])
        self.assertEqual(rows[0]["input_timeframe"], "M1")

    def test_historical_grid_keeps_deep_and_s5_supplement_sources(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reports = root / "reports"
            reports.mkdir()
            common = {
                "timeframe": "M1",
                "instrument_count": 68,
                "cycle_count": 100,
            }
            (reports / "strategy_lab_timeframe_m1_deep.json").write_text(
                json.dumps(
                    {
                        **common,
                        "source": "m1_csv_deep_history",
                        "family_summaries": [
                            {"family": "momentum", "horizons": {"60": {"n": 10, "win_rate": 60, "avg": 0.2}}}
                        ],
                    }
                ),
                encoding="utf-8",
            )
            (reports / "strategy_lab_timeframe_m1_s5.json").write_text(
                json.dumps(
                    {
                        **common,
                        "source": "s5_parquet_resampled",
                        "family_summaries": [
                            {"family": "momentum", "horizons": {"30": {"n": 8, "win_rate": 50, "avg": 0.1}}}
                        ],
                    }
                ),
                encoding="utf-8",
            )

            rows = summarize_historical_strategy_accuracy_grid(root)

        self.assertEqual(len(rows), 2)
        self.assertEqual({tuple(row["cells"]) for row in rows}, {("30",), ("60",)})

    def test_model_gap_grid_reads_every_holdout_cell(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reports = root / "reports" / "modern_model_gap"
            reports.mkdir(parents=True)
            (reports / "shared_panel_model_benchmark_latest.json").write_text(
                json.dumps(
                    {
                        "generated_utc": "2026-07-19T00:00:00Z",
                        "dataset": {"instruments": 68},
                        "results": [
                            {
                                "status": "evaluated",
                                "model": "catboost",
                                "holdout": {
                                    "all_prediction_cell_metrics": [
                                        {
                                            "input_timeframe": "M5",
                                            "horizon_sec": 7200,
                                            "events": 120,
                                            "best_side_rate": 0.58,
                                            "win_rate": 0.54,
                                            "mean_net_pips": 0.3,
                                        }
                                    ]
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            rows = summarize_model_gap_accuracy_grid(root)

        self.assertEqual(len(rows), 1)
        cell = rows[0]["cells"]["7200"]
        self.assertAlmostEqual(cell["direction_accuracy_pct"], 58.0)
        self.assertAlmostEqual(cell["after_cost_win_rate_pct"], 54.0)
        self.assertEqual(cell["after_cost_n"], 120)

    def test_ma_feature_grid_exposes_direction_and_pip_metrics(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reports = root / "reports" / "ma_feature_grid"
            reports.mkdir(parents=True)
            (reports / "ma_feature_grid_latest.json").write_text(
                json.dumps(
                    {
                        "generated_at": "2026-07-27T00:00:00Z",
                        "timeframe_reports": [
                            {
                                "timeframe": "M7",
                                "status": "fitted",
                                "feature_count": 643,
                                "metrics": {
                                    "holdout": {
                                        "3600": {
                                            "n": 900,
                                            "pair_count": 68,
                                            "direction_accuracy": 0.53,
                                            "executable_win_rate": 0.48,
                                            "executable_average_net_pips": -0.2,
                                            "signed_pip_mae": 2.4,
                                            "signed_pip_correlation": 0.12,
                                            "magnitude_pip_mae": 1.8,
                                            "magnitude_pip_correlation": 0.41,
                                            "brier_score": 0.248,
                                        }
                                    }
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            rows = summarize_ma_feature_grid_accuracy(root)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["input_timeframe"], "M7")
        cell = rows[0]["cells"]["3600"]
        self.assertAlmostEqual(cell["direction_accuracy_pct"], 53.0)
        self.assertAlmostEqual(cell["signed_pip_mae"], 2.4)
        self.assertAlmostEqual(cell["magnitude_pip_correlation"], 0.41)
        self.assertEqual(cell["direction_n"], 900)

    def test_model_gap_completion_exposes_every_registered_model(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            model_space = root / "model_space"
            model_space.mkdir()
            (model_space / "model_gap_completion_latest.json").write_text(
                json.dumps(
                    {
                        "generated_utc": "2026-07-19T00:00:00Z",
                        "summary": {"models": 30, "evidence_complete": True},
                        "models": [
                            {
                                "model": "catboost",
                                "family": "modern_tabular_probabilistic",
                                "runtime_status": "runtime_available",
                                "account_wired": False,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            result = summarize_model_gap_completion(root)

        self.assertEqual(result["summary"]["models"], 30)
        self.assertEqual(result["models"][0]["model"], "catboost")
        self.assertFalse(result["models"][0]["account_wired"])

    def test_best_signal_matrix_keeps_only_m1_plus_component_horizons(self):
        signal = {
            "instrument": "EUR_USD",
            "direction": "buy",
            "preferred_horizon_sec": 300,
            "horizon_breakdown": [
                {
                    "horizon_sec": 60,
                    "best_model_id": "momentum.fast",
                    "best_input_timeframe": "M1",
                    "setup_class_counts": {
                        "accepted": 1,
                        "near_threshold": 1,
                    },
                    "contributors": [
                        {
                            "family": "momentum",
                            "model_id": "momentum.fast",
                            "lane_id": "momentum.fast",
                            "input_timeframe": "M1",
                            "signal_role": "structural",
                            "signal_confidence": 0.61,
                            "projected_net_pips": 0.4,
                            "projected_net_pips_per_hour": 24.0,
                            "signal_score": 0.2,
                            "historical_reliability": 0.75,
                            "signal_eligible": True,
                            "signal_blocked_by": [],
                            "steps_ahead": 1,
                            "preconsensus_class": "accepted",
                            "matrix_input_weight": 1.0,
                        },
                        {
                            "family": "pullback",
                            "model_id": "pullback.loose",
                            "input_timeframe": "M5",
                            "signal_role": "structural",
                            "signal_confidence": 0.52,
                            "projected_net_pips": -0.1,
                            "signal_eligible": False,
                            "signal_blocked_by": ["expected_net_edge"],
                            "preconsensus_class": "near_threshold",
                            "matrix_input_weight": 0.25,
                        },
                    ],
                }
            ],
            "entry_exit_curve": {
                "points": [
                    {
                        "horizon_sec": 30,
                        "side_probability": 0.57,
                        "directional_pips": 0.12,
                        "steps_ahead": 30,
                    }
                ]
            },
        }

        matrix = build_signal_component_matrix(
            signal,
            {
                ("momentum.fast", 60): {
                    "lane_id": "momentum.fast",
                    "eligible": True,
                    "independent_blocks": 40,
                    "holdout_blocks": 12,
                    "raw": {"n": 80},
                    "holdout": {
                        "n": 24,
                        "win_rate": 62.5,
                        "avg": 0.35,
                        "lower_confidence": 0.08,
                    },
                }
            },
        )

        self.assertEqual(matrix["horizons_sec"], [60])
        self.assertTrue(matrix["rows"][0]["finality"])
        self.assertEqual(matrix["rows"][0]["model_id"], "Final consensus")
        self.assertEqual(matrix["rows"][0]["cells"]["60"]["component_count"], 2)
        self.assertEqual(
            matrix["rows"][0]["cells"]["60"]["setup_class_counts"],
            {"accepted": 1, "near_threshold": 1},
        )
        momentum = next(row for row in matrix["rows"] if row["model_id"] == "momentum.fast")
        self.assertTrue(momentum["cells"]["60"]["winner"])
        self.assertTrue(momentum["cells"]["60"]["signal_eligible"])
        self.assertEqual(momentum["cells"]["60"]["historical_win_rate_pct"], 62.5)
        self.assertEqual(momentum["cells"]["60"]["historical_n"], 24)
        self.assertEqual(momentum["cells"]["60"]["preconsensus_class"], "accepted")
        self.assertFalse(
            any(row["model_id"] == "S1 timing curve" for row in matrix["rows"])
        )

    def test_arima_grid_selects_specification_on_calibration_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state = root / "state"
            reports = root / "reports"
            state.mkdir()
            reports.mkdir()
            results = reports / "m5_results.csv"
            header = (
                "pair,window_id,horizon_minutes,model,error,calibration_total_net_pips,"
                "calibration_profit_factor,calibration_mean_net_pips,direction_accuracy,"
                "win_rate,mean_net_pips,test_rows,trades\n"
            )
            results.write_text(
                header
                + "EUR_USD,1,60,calibration_winner,,20,2,1,0.40,0.25,-1,100,20\n"
                + "EUR_USD,1,60,test_winner,,0,0,0,0.90,0.80,4,100,20\n",
                encoding="utf-8",
            )
            summary = reports / "m5_summary.json"
            summary.write_text(json.dumps({"results_csv": str(results)}), encoding="utf-8")
            (state / "arima_multiframe_sweep_v1.json").write_text(
                json.dumps({"runs": [{"timeframe": "M5", "summary": str(summary)}]}),
                encoding="utf-8",
            )

            row = summarize_arima_accuracy_grid(root)[0]

        cell = row["cells"]["3600"]
        self.assertEqual(cell["direction_accuracy_pct"], 40.0)
        self.assertEqual(cell["after_cost_win_rate_pct"], 25.0)
        self.assertEqual(cell["models"], ["calibration_winner"])

    def test_timeframe_grid_uses_global_causal_oos_counts(self):
        state = {
            "global_surfaces": {
                "timeframe_equation_matrix.m5|10800": {
                    "lane_id": "timeframe_equation_matrix.m5",
                    "horizon_sec": 10800,
                    "oos_n": 240,
                    "n": 300,
                    "independent_blocks": 20,
                    "calibrated_accuracy": 0.58,
                    "calibrated_win_rate": 0.53,
                    "calibrated_average_net_pips": 0.4,
                }
            }
        }

        row = summarize_timeframe_calibration_accuracy_grid(
            Path("unused"), state
        )[0]

        self.assertEqual(row["input_timeframe"], "M5")
        self.assertEqual(row["cells"]["10800"]["direction_n"], 240)
        self.assertAlmostEqual(
            row["cells"]["10800"]["direction_accuracy_pct"], 58.0
        )

    def test_prediction_grid_aggregates_models_by_timeframe_and_horizon(self):
        source_rows = [
            {
                "id": "historical",
                "label": "Historical model",
                "input_timeframe": "M5",
                "source_kind": "historical_replay",
                "source": "history.json",
                "cells": {
                    "7200": {
                        "direction_accuracy_pct": 60.0,
                        "after_cost_win_rate_pct": 55.0,
                        "avg_net_pips": 1.0,
                        "direction_n": 100,
                        "after_cost_n": 100,
                    }
                },
            },
            {
                "id": "live",
                "label": "Live model",
                "input_timeframe": "m5",
                "source_kind": "live_oos",
                "source": "live.json",
                "cells": {
                    "7200": {
                        "direction_accuracy_pct": 50.0,
                        "after_cost_win_rate_pct": 45.0,
                        "avg_net_pips": -1.0,
                        "direction_n": 300,
                        "after_cost_n": 300,
                    }
                },
            },
        ]

        rows = aggregate_prediction_accuracy_rows(source_rows)

        self.assertEqual(len(rows), 1)
        cell = rows[0]["cells"]["7200"]
        self.assertEqual(cell["after_cost_n"], 400)
        self.assertEqual(cell["historical_n"], 100)
        self.assertEqual(cell["live_n"], 300)
        self.assertEqual(cell["after_cost_win_rate_pct"], 47.5)
        self.assertEqual(cell["avg_net_pips"], -0.5)

    def test_prediction_ledger_keeps_accepted_and_near_threshold_only(self):
        rows = [
            {
                "event": "shadow_outcome", "id": "accepted", "kind": "signal",
                "instrument": "EUR_USD", "direction": "buy", "family": "momentum",
                "lane_id": "momentum.fast", "profile": "fast", "horizon_sec": 300,
                "entry_time": "2026-07-15T10:00:00Z", "exit_time": "2026-07-15T10:05:00Z",
                "theoretical_pips": 1.2,
            },
            {
                "event": "shadow_outcome", "id": "near", "kind": "miss",
                "miss_class": "near_threshold", "blocked_reason": "signal_vs_spread",
                "instrument": "GBP_USD", "direction": "sell", "family": "pullback",
                "lane_id": "pullback.loose", "profile": "loose", "horizon_sec": 300,
                "entry_time": "2026-07-15T10:01:00Z", "exit_time": "2026-07-15T10:06:00Z",
                "theoretical_pips": -0.4,
            },
            {
                "event": "shadow_outcome", "id": "hard", "kind": "miss",
                "miss_class": "hard_reject", "instrument": "USD_ZAR", "direction": "buy",
                "horizon_sec": 300, "theoretical_pips": 20.0,
            },
        ]
        with tempfile.TemporaryDirectory() as temporary:
            log_dir = Path(temporary)
            path = log_dir / "practice_strategy_lab_test.jsonl"
            path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

            ledger = summarize_prediction_ledger(log_dir, max_lines=100)

        self.assertEqual(ledger["accepted"]["n"], 1)
        self.assertEqual(ledger["near_miss"]["n"], 1)
        self.assertEqual({row["id"] for row in ledger["rows"]}, {"accepted", "near"})
        self.assertEqual(ledger["near_miss"]["avg_net_pips"], -0.4)

    def test_main_family_summary_uses_best_holdout_surface(self):
        promotion = {
            "signal_evidence": [
                {
                    "family": "momentum",
                    "lane_id": "momentum.fast",
                    "horizon_sec": 300,
                    "eligible": False,
                    "blocked_by": ["lower_confidence"],
                    "raw": {"n": 20},
                    "holdout": {"n": 5, "avg": -0.2, "lower_confidence": -0.7, "win_rate": 40},
                },
                {
                    "family": "momentum",
                    "lane_id": "momentum.balanced",
                    "horizon_sec": 900,
                    "eligible": True,
                    "raw": {"n": 80},
                    "holdout": {"n": 24, "avg": 0.4, "lower_confidence": 0.1, "win_rate": 58},
                },
            ]
        }
        active = [
            {"family": "momentum", "instrument": "AUD_USD", "direction": "buy", "model_id": "m1"},
            {"family": "momentum", "instrument": "EUR_USD", "direction": "sell", "model_id": "m2"},
        ]
        row = summarize_signal_families(promotion, active)[0]
        self.assertEqual(row["best_horizon_sec"], 900)
        self.assertEqual(row["active_signals"], 2)
        self.assertEqual(row["active_pairs"], 2)
        self.assertTrue(row["eligible"])

    def test_s1_curve_is_directional_and_requires_strong_alignment(self):
        points = [
            {"horizon_sec": horizon, "probability_up": 0.55, "predicted_signed_pips": 0.2}
            for horizon in (60, 180, 300)
        ]
        self.assertEqual(curve_for_direction(points, "buy")["state"], "aligned")
        self.assertEqual(curve_for_direction(points, "sell")["state"], "opposed")

    def test_shared_signal_feed_counts_all_producers(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "signal_feed.sqlite"
            connection = sqlite3.connect(path)
            connection.executescript(
                """
                CREATE TABLE candidates (
                    candidate_id TEXT PRIMARY KEY,
                    published_epoch REAL NOT NULL,
                    expires_epoch REAL NOT NULL,
                    source TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE executions (
                    client_id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL,
                    submitted_epoch REAL NOT NULL,
                    status TEXT NOT NULL,
                    trade_id TEXT,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE contributor_registry (
                    contributor_id TEXT PRIMARY KEY,
                    source_kind TEXT NOT NULL,
                    expected INTEGER NOT NULL,
                    account_eligible INTEGER NOT NULL,
                    last_seen_epoch REAL
                );
                """
            )
            connection.execute(
                "INSERT INTO candidates VALUES (?, ?, ?, ?, ?)",
                ("ridge-1", 1.0, 9999999999.0, "second_forecast_hot", "{}"),
            )
            connection.execute(
                "INSERT INTO executions VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "order-1",
                    "ridge-1",
                    2.0,
                    "filled",
                    "123",
                    json.dumps({"instrument": "EUR_USD", "signal_confidence": 0.61}),
                ),
            )
            connection.executemany(
                "INSERT INTO contributor_registry VALUES (?, ?, ?, ?, ?)",
                [
                    ("ngboost", "model_gap", 1, 0, time.time()),
                    ("momentum.fast", "strategy_lane", 1, 1, time.time()),
                ],
            )
            connection.commit()
            connection.close()
            summary = summarize_signal_feed(path)
        self.assertEqual(summary["status_counts"]["filled"], 1)
        self.assertEqual(summary["active_sources"]["second_forecast_hot"], 1)
        self.assertEqual(summary["latest_execution"]["trade_id"], "123")
        self.assertEqual(summary["contributors"]["registered"], 2)
        self.assertEqual(summary["contributors"]["fresh"], 2)
        self.assertEqual(summary["contributors"]["model_gap_registered"], 1)

    def test_primary_system_exposes_full_horizon_virtual_space(self):
        result = summarize_primary_signal_system(
            None,
            {"lane_count": 156, "lanes": [], "execution": {}},
            {},
            {},
            {
                "status": "historical_backfill",
                "source_complete": False,
                "horizons_sec": [60, 180, 300, 600, 900, 1800, 3600],
                "raw_rows": 123,
                "backfill": {"cursor": 1000, "source_max_row": 2000},
            },
        )
        self.assertEqual(result["lab"]["virtual_lane_horizons"], 1092)
        self.assertEqual(result["promotion"]["horizons_sec"][-1], 3600)
        self.assertFalse(result["promotion"]["source_complete"])

    def test_s1_ridge_is_counted_inside_the_unified_matrix(self):
        result = summarize_primary_signal_system(
            None,
            {
                "lane_count": 156,
                "lanes": [],
                "execution": {},
                "horizon_options_sec": [60, 180, 300, 600, 900, 1800, 3600],
            },
            {},
            {},
            {"horizons_sec": [15, 30, 60, 180, 300, 600, 900, 1800, 3600]},
            {
                "model_family": "ridge_return",
                "input_timeframe": "S1",
                "training_timeframe": "S5",
                "profiles": ["fast", "balanced", "strict"],
                "physical_lane_count": 3,
                "outcome_horizons_sec": [15, 30, 60, 180, 300, 600, 900, 1800, 3600],
                "lane_horizon_surfaces": 27,
                "pair_model_surfaces": 531,
                "pairs": 59,
                "accepted_signals": 12,
                "matured_outcomes": 8,
                "status": "live",
                "active": True,
            },
        )
        self.assertEqual(result["lab"]["lanes"], 159)
        self.assertEqual(result["lab"]["virtual_lane_horizons"], 1119)
        self.assertEqual(result["lab"]["signals"], 12)
        self.assertEqual(result["matrix"]["input_timeframes"], ["M1 / multi-context", "S1"])
        self.assertEqual(result["matrix"]["rows"][1]["pair_model_surfaces"], 531)

    def test_unified_candidate_dashboard_row_stays_shadow_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shadow = root / "curves.json"
            shadow.write_text(
                json.dumps(
                    {
                        "one_curve_per_pair": True,
                        "forecasts": [
                            {
                                "instrument": "EUR_USD",
                                "forecast_curve": {"60": {}, "3600": {}},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            validation = root / "validation.json"
            validation.write_text(
                json.dumps(
                    {
                        "deployment_eligible": True,
                        "artifact_sha256": "abc",
                        "shadow_forecasts": str(shadow),
                        "selection": {
                            "winner": {
                                "model_candidate": "extra_trees_mtf_full"
                            }
                        },
                        "untouched_final": {
                            "selected_model": {
                                "mean_horizon_mae_relative_to_no_change": 0.98
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            matrix = dashboard.summarize_unified_forecast_matrix(
                {}, {}, validation
            )

        row = matrix["rows"][0]
        self.assertEqual(row["input_timeframe"], "MULTI")
        self.assertEqual(row["pairs"], 1)
        self.assertTrue(row["one_curve_per_pair"])
        self.assertFalse(row["account_eligible"])

    def test_timeframe_equation_family_is_a_real_matrix_row(self):
        result = summarize_primary_signal_system(
            None,
            {
                "active": True,
                "instrument_count": 68,
                "lane_count": 156,
                "lanes": [],
                "execution": {},
                "horizon_options_sec": [60, 300, 3600, 14400],
                "timeframe_horizon_matrix": {
                    "model_family": "timeframe_equation_matrix",
                    "input_timeframes": ["S5", "M1", "H4"],
                    "training_timeframe": "rolling_live_equation",
                    "physical_lane_count": 3,
                    "horizons_sec": [60, 300, 3600, 14400],
                    "lane_horizon_surfaces": 12,
                    "ready_timeframes": ["S5", "M1"],
                    "forecasts_in_window": 40,
                },
            },
            {},
            {},
        )
        matrix_row = next(
            row
            for row in result["matrix"]["rows"]
            if row["model_family"] == "timeframe_equation_matrix"
        )
        self.assertEqual(matrix_row["physical_lanes"], 3)
        self.assertEqual(matrix_row["lane_horizon_surfaces"], 12)
        self.assertEqual(matrix_row["pair_model_surfaces"], 816)
        self.assertEqual(matrix_row["status"], "live")
        self.assertFalse(matrix_row["account_eligible"])

    def test_pattern_forecast_and_matured_diagnostics_are_exposed(self):
        prediction = {
            "decision_candle_time": "2026-07-14T05:20:00Z",
            "pattern": "U D U",
            "sequence_pips": [0.2, -0.1, 0.3],
            "target_horizon_sec": 60,
            "probability_up": 0.508,
            "direction_edge": 0.008,
            "expected_signed_move_pips": 0.1,
            "expected_abs_move_pips": 1.4,
            "baseline_abs_move_pips": 1.0,
            "movement_coefficient": 1.4,
            "historical_pattern_count": 900,
            "live_pattern_count": 3,
            "effective_sample_count": 120,
            "pattern_mode": "sign",
            "pattern_order": 3,
        }
        rows = [
            {
                "time": "2026-07-14T05:20:01Z",
                "event": "lab_start",
                "run_label": "test",
                "instrument_count": 1,
                "lanes": [
                    {
                        "lane_id": "pattern_count_forecast.fast",
                        "family": "pattern_count_forecast",
                        "profile": "fast",
                        "parameters": {},
                    }
                ],
                "outcome_horizons": [60],
            },
            {
                "time": "2026-07-14T05:20:02Z",
                "event": "shadow_miss",
                "id": "p1",
                "lane_id": "pattern_count_forecast.fast",
                "family": "pattern_count_forecast",
                "profile": "fast",
                "instrument": "EUR_USD",
                "direction": "buy",
                "blocked_reasons": ["pattern_direction_edge"],
                "miss_class": "near_threshold",
                "signal": {"pattern_forecast": prediction},
            },
            {
                "time": "2026-07-14T05:21:02Z",
                "event": "shadow_outcome",
                "id": "p1",
                "lane_id": "pattern_count_forecast.fast",
                "family": "pattern_count_forecast",
                "profile": "fast",
                "kind": "miss",
                "miss_class": "near_threshold",
                "horizon_sec": 60,
                "theoretical_pips": 0.4,
                "pattern_prediction": prediction,
                "actual_signed_move_pips": 0.5,
                "actual_abs_move_pips": 0.5,
                "actual_movement_coefficient": 0.5,
                "direction_correct": True,
                "signed_move_error_pips": 0.4,
                "absolute_move_error_pips": -0.9,
                "probability_brier_score": 0.242064,
            },
            {
                "time": "2026-07-14T05:25:02Z",
                "event": "shadow_outcome",
                "id": "p1",
                "lane_id": "pattern_count_forecast.fast",
                "family": "pattern_count_forecast",
                "profile": "fast",
                "kind": "miss",
                "miss_class": "near_threshold",
                "horizon_sec": 300,
                "theoretical_pips": -1.0,
                "pattern_prediction": prediction,
                "actual_signed_move_pips": -1.0,
                "actual_abs_move_pips": 1.0,
                "actual_movement_coefficient": 1.0,
                "direction_correct": False,
                "signed_move_error_pips": -1.1,
                "absolute_move_error_pips": -0.4,
                "probability_brier_score": 0.258064,
            },
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lab.jsonl"
            path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
            summary = summarize_lab(path, 100)
        self.assertEqual(summary["pattern_diagnostics"]["matured"], 1)
        self.assertEqual(summary["pattern_diagnostics"]["direction_accuracy"], 100.0)
        self.assertEqual(summary["pattern_forecasts"][0]["movement_coefficient"], 1.4)
        self.assertEqual(summary["pattern_forecasts"][0]["actual_signed_move_pips"], 0.5)
        self.assertEqual(summary["pattern_breakdown"]["variants"][0]["matured"], 1)
        self.assertEqual(summary["pattern_breakdown"]["sequences"][0]["pattern"], "U D U")
        self.assertEqual(summary["pattern_breakdown"]["sequences"][0]["direction_accuracy"], 100.0)

    def test_configured_horizons_are_visible_before_outcomes_mature(self):
        rows = [
            {
                "time": "2026-07-15T15:25:25Z",
                "event": "lab_start",
                "run_label": "multihorizon",
                "lanes": [
                    {
                        "lane_id": "momentum.fast",
                        "family": "momentum",
                        "profile": "fast",
                        "parameters": {},
                    }
                ],
                "outcome_horizons": [60, 180, 300, 600, 900, 1800, 3600],
            }
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lab.jsonl"
            path.write_text(json.dumps(rows[0]) + "\n", encoding="utf-8")
            summary = summarize_lab(path, 100)
        self.assertEqual(summary["horizon_options_sec"], [60, 180, 300, 600, 900, 1800, 3600])
        self.assertEqual(summary["target_horizon_sec"], 300)

    def test_equation_path_uses_three_sevenths_past_context(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "micro.sqlite3"
            connection = sqlite3.connect(database)
            connection.executescript(
                """
                CREATE TABLE predictions (
                    id INTEGER PRIMARY KEY, origin_time_ns INTEGER, target_time_ns INTEGER,
                    outcome_time_ns INTEGER, instrument TEXT, model_id TEXT, horizon_ms INTEGER,
                    expected_signed_pips REAL, actual_signed_pips REAL, theoretical_pips REAL,
                    direction_correct INTEGER, entry_bid REAL, entry_ask REAL
                );
                CREATE TABLE quotes (time_ns INTEGER, instrument TEXT, mid REAL);
                """
            )
            origin = 10_000_000_000
            connection.execute(
                "INSERT INTO predictions VALUES (1, ?, ?, ?, 'EUR_USD', 'equation.linear', 7000, 1.2, 0.5, -0.3, 1, 1.1, 1.1002)",
                (origin, origin + 7_000_000_000, origin + 7_000_000_000),
            )
            connection.executemany(
                "INSERT INTO quotes VALUES (?, 'EUR_USD', ?)",
                [(origin - 3_000_000_000, 1.1000), (origin, 1.1001), (origin + 7_000_000_000, 1.10015)],
            )
            connection.commit()
            connection.close()
            snapshot = root / "snapshot.json"
            snapshot.write_text(json.dumps({"database": str(database)}), encoding="utf-8")

            result = load_equation_path(snapshot, 1)

        self.assertEqual(result["past_window_ms"], 3000.0)
        self.assertEqual(result["future_window_ms"], 7000.0)
        self.assertEqual(len(result["points"]), 3)

    def test_historical_calibration_does_not_borrow_unmatched_live_models(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for report_dir in (
                "m1_hgb_reversal_120_oanda_candidate_20260707",
                "all68_latest_hgb_reversal_m30_full_trader_style_backtest",
                "all68_latest_hgb_reversal_h1_full_trader_style_backtest",
                "all68_latest_hgb_reversal_h4_full_trader_style_backtest",
            ):
                path = root / "reports" / report_dir / "summary.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps({"opened_trades": 1000, "win_rate": 0.6, "profit_factor": 1.5, "max_drawdown_pct": 10}), encoding="utf-8")
            state = root / "state"
            state.mkdir()
            (state / "hgb_live_outcomes_v1.json").write_text(
                json.dumps([{"model_family": "arima", "timeframe": "H1", "horizon_minutes": 120, "win": True}]),
                encoding="utf-8",
            )

            result = summarize_historical_calibration(root)

        self.assertEqual(len(result["rows"]), 4)
        self.assertTrue(all(row["live_n"] == 0 for row in result["rows"]))
        self.assertTrue(all(row["adjusted_win_rate"] == 60.0 for row in result["rows"]))

    def test_live_account_retains_manager_assignment_from_state_path(self):
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            snapshot = data_root / "oanda_training_manager" / "state" / "account_dashboard_v1.json"
            snapshot.parent.mkdir(parents=True)
            snapshot.write_text(json.dumps({"accounts": [], "aggregate": {}}), encoding="utf-8")
            live_state = data_root / "technical_scout_manager" / "account_live_primary_challenger_scout" / "state.json"
            live_state.parent.mkdir(parents=True)
            live_state.write_text(
                json.dumps(
                    {
                        "last_account_snapshot": {
                            "id": "001-001-21715580-002",
                            "balance": 9.68,
                            "nav": 9.69,
                            "unrealized_pl": 0.01,
                            "open_trade_count": 2,
                            "pending_order_count": 6,
                        }
                    }
                ),
                encoding="utf-8",
            )

            result = summarize_account_snapshot(snapshot)

        account = result["live_accounts"][0]
        self.assertEqual(account["role"], "live primary challenger scout")
        self.assertEqual(account["assigned_strategies"], ["live primary challenger scout"])
        self.assertEqual(account["openTradeCount"], 2)

    def test_current_broker_live_snapshot_replaces_stale_manager_state(self):
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            snapshot = data_root / "oanda_training_manager" / "state" / "account_dashboard_v1.json"
            snapshot.parent.mkdir(parents=True)
            snapshot.write_text(
                json.dumps(
                    {
                        "accounts": [],
                        "aggregate": {},
                        "live_accounts": [
                            {
                                "account_id": "001-001-21715580-002",
                                "role": "primary_live",
                                "env": "live",
                                "ok": True,
                                "balance": "72.0343",
                                "NAV": "72.0343",
                                "pl": "-7.9213",
                                "unrealizedPL": "0",
                                "openTradeCount": 0,
                                "pendingOrderCount": 0,
                                "trades": [],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stale_state = data_root / "technical_scout_manager" / "account_live_primary_challenger_scout" / "state.json"
            stale_state.parent.mkdir(parents=True)
            stale_state.write_text(
                json.dumps({"last_account_snapshot": {"id": "001-001-21715580-002", "open_trade_count": 2}}),
                encoding="utf-8",
            )

            result = summarize_account_snapshot(snapshot)

        account = result["live_accounts"][0]
        self.assertEqual(account["role"], "primary challenger scout")
        self.assertEqual(account["registry_status"], "current broker GET snapshot")
        self.assertEqual(account["openTradeCount"], 0)
        self.assertIsNotNone(account["lifetime_return_pct"])
        self.assertIn("read-only GET", result["environment_aggregates"]["live"]["source"])

    def test_managed_practice_scope_does_not_restore_disabled_live_state(self):
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            snapshot = (
                data_root
                / "oanda_training_manager"
                / "state"
                / "account_dashboard_v1.json"
            )
            snapshot.parent.mkdir(parents=True)
            snapshot.write_text(
                json.dumps(
                    {
                        "account_scope": "managed_practice_only",
                        "accounts": [
                            {
                                "account_id": "101-001-37981792-002",
                                "role": "practice_002",
                                "ok": True,
                                "balance": "56",
                                "NAV": "56",
                                "pl": "0",
                                "unrealizedPL": "0",
                                "openTradeCount": 0,
                                "pendingOrderCount": 0,
                            },
                            {
                                "account_id": "101-001-37981792-007",
                                "role": "practice_007",
                                "ok": True,
                                "balance": "42",
                                "NAV": "42",
                                "pl": "0",
                                "unrealizedPL": "0",
                                "openTradeCount": 0,
                                "pendingOrderCount": 0,
                            },
                        ],
                        "live_accounts": [],
                        "aggregate": {},
                    }
                ),
                encoding="utf-8",
            )
            stale_state = (
                data_root
                / "forex_gpt_manager"
                / "account_gpt_prod_live"
                / "state.json"
            )
            stale_state.parent.mkdir(parents=True)
            stale_state.write_text(
                json.dumps(
                    {
                        "last_account_snapshot": {
                            "id": "001-001-21715580-001",
                            "balance": "47",
                            "nav": "47",
                        }
                    }
                ),
                encoding="utf-8",
            )

            result = summarize_account_snapshot(snapshot)

        self.assertEqual(result["environment_aggregates"]["paper"]["account_count"], 2)
        self.assertEqual(result["environment_aggregates"]["live"]["account_count"], 0)
        self.assertEqual(result["live_accounts"], [])
        self.assertIn(
            "excluded",
            result["environment_aggregates"]["live"]["source"],
        )


if __name__ == "__main__":
    unittest.main()
