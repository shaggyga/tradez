import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from trad.oanda_lane_promotion import (
    LanePromotionModel,
    LanePromotionStore,
    PromotionThresholds,
    agreement_adjusted_signal_thresholds,
    evaluate_promotion_scope,
    reconcile_promotion_state,
)


def outcome_rows(count: int, *, horizon: int = 3600, negative_holdout: bool = False) -> list[dict]:
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    rows = []
    for index in range(count):
        value = 1.2
        if negative_holdout and index >= int(count * 0.70):
            value = -1.0
        rows.append(
            {
                "row_id": index + 1,
                "horizon_sec": horizon,
                "lane_id": "oscillator_reversion_confluence.fast",
                "family": "oscillator_reversion_confluence",
                "profile": "fast",
                "instrument": ("EUR_USD", "EUR_GBP", "USD_JPY")[index % 3],
                "direction": "buy",
                "entry_time": (start + timedelta(hours=index * 2)).isoformat(),
                "endpoint_pips": value,
            }
        )
    return rows


class PromotionEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.thresholds = PromotionThresholds(
            min_samples=30,
            min_independent_blocks=12,
            min_holdout_blocks=4,
            min_pairs=3,
            min_sessions=2,
            min_segment_samples=5,
        )

    def test_positive_h1_scope_passes_independent_holdout(self):
        result = evaluate_promotion_scope(outcome_rows(60), 3600, self.thresholds)
        self.assertTrue(result["eligible"])
        self.assertGreaterEqual(result["independent_blocks"], 12)
        self.assertEqual(result["horizon_sec"], 3600)
        self.assertEqual(result["positive_time_blocks"], 3)

    def test_negative_h1_holdout_is_no_trade(self):
        result = evaluate_promotion_scope(
            outcome_rows(60, negative_holdout=True),
            3600,
            self.thresholds,
        )
        self.assertFalse(result["eligible"])
        self.assertIn("holdout_net_edge", result["blocked_by"])
        self.assertIn("holdout_confidence", result["blocked_by"])

    def test_underfilled_scope_score_is_shrunk_toward_negative_prior(self):
        result = evaluate_promotion_scope(outcome_rows(8), 3600, self.thresholds)
        self.assertFalse(result["eligible"])
        self.assertLess(result["evidence_strength"], 1.0)
        self.assertLess(result["adjusted_lower_confidence"], result["holdout"]["lower_confidence"])
        self.assertEqual(result["score"], result["adjusted_lower_confidence"])

    def test_partial_backfill_merges_without_erasing_richer_incumbent(self):
        incumbent_row = {
            "lane_id": "momentum.fast",
            "horizon_sec": 300,
            "eligible": False,
            "score": -0.1,
            "independent_blocks": 20,
            "raw": {"n": 100},
            "training": {"n": 14},
            "holdout": {"n": 6},
        }
        candidate_row = {
            "lane_id": "equation.fast",
            "horizon_sec": 7200,
            "eligible": False,
            "score": -0.2,
            "independent_blocks": 2,
            "raw": {"n": 8},
            "training": {"n": 1},
            "holdout": {"n": 1},
        }
        reconciled = reconcile_promotion_state(
            {
                "source_complete": False,
                "raw_rows": 8,
                "horizons_sec": [7200],
                "signal_evidence": [candidate_row],
            },
            {
                "source_complete": True,
                "raw_rows": 100,
                "horizons_sec": [300],
                "signal_evidence": [incumbent_row],
            },
        )

        self.assertEqual(reconciled["evidence_count"], 2)
        self.assertEqual(reconciled["raw_rows"], 100)
        self.assertEqual(reconciled["horizons_sec"], [300, 7200])
        self.assertFalse(reconciled["artifact_update"]["replaced"])
        self.assertEqual(reconciled["qualified_evidence"], [])

    def test_complete_compacted_rebuild_replaces_larger_incumbent(self):
        candidate = {
            "source_complete": True,
            "raw_rows": 80,
            "horizons_sec": [300, 3600],
            "signal_evidence": [
                {
                    "lane_id": "momentum.fast",
                    "horizon_sec": 300,
                    "eligible": False,
                    "raw": {"n": 80},
                    "training": {"n": 8},
                    "holdout": {"n": 4},
                }
            ],
            "qualified_evidence": [],
        }
        reconciled = reconcile_promotion_state(
            candidate,
            {
                "source_complete": True,
                "raw_rows": 200,
                "horizons_sec": [300],
                "signal_evidence": [
                    {
                        "lane_id": "old.fast",
                        "horizon_sec": 300,
                        "eligible": False,
                        "raw": {"n": 200},
                        "training": {"n": 20},
                        "holdout": {"n": 8},
                    }
                ],
            },
        )

        self.assertTrue(reconciled["source_complete"])
        self.assertEqual(reconciled["raw_rows"], 80)
        self.assertTrue(reconciled["artifact_update"]["replaced"])
        self.assertEqual(
            reconciled["artifact_update"]["reason"],
            "complete_compacted_rebuild",
        )

    def test_clean_multi_family_agreement_relaxes_only_soft_signal_gates(self):
        two = agreement_adjusted_signal_thresholds(0.535, 0.05, 2, 0)
        three = agreement_adjusted_signal_thresholds(0.535, 0.05, 3, 0)
        opposed = agreement_adjusted_signal_thresholds(0.535, 0.05, 4, 1)

        self.assertEqual(two, (0.527, 0.025, "two_family_agreement"))
        self.assertEqual(three, (0.52, 0.0, "three_plus_family_agreement"))
        self.assertEqual(opposed, (0.535, 0.05, "base"))

    def test_sqlite_state_persists_and_qualifies_h1_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "outcomes.sqlite"
            state = root / "promotion.json"
            connection = sqlite3.connect(database)
            connection.execute(
                """
                CREATE TABLE outcomes (
                    row_id INTEGER PRIMARY KEY,
                    horizon_sec INTEGER,
                    lane_id TEXT,
                    family TEXT,
                    profile TEXT,
                    kind TEXT,
                    instrument TEXT,
                    direction TEXT,
                    entry_time TEXT,
                    endpoint_pips REAL
                )
                """
            )
            connection.executemany(
                """
                INSERT INTO outcomes (
                    row_id, horizon_sec, lane_id, family, profile, kind,
                    instrument, direction, entry_time, endpoint_pips
                ) VALUES (?, ?, ?, ?, ?, 'signal', ?, ?, ?, ?)
                """,
                [
                    (
                        row["row_id"],
                        row["horizon_sec"],
                        row["lane_id"],
                        row["family"],
                        row["profile"],
                        row["instrument"],
                        row["direction"],
                        row["entry_time"],
                        row["endpoint_pips"],
                    )
                    for row in outcome_rows(60)
                ],
            )
            connection.commit()
            connection.close()

            model = LanePromotionModel(database, state, [3600], thresholds=self.thresholds)
            fitted = model.fit()
            candidates = model.qualify_candidates(
                [
                    {
                        "id": "candidate-1",
                        "lane_id": "oscillator_reversion_confluence.fast",
                        "family": "oscillator_reversion_confluence",
                        "profile": "fast",
                        "instrument": "EUR_USD",
                        "signal_to_spread": 4.0,
                        "spread_pips": 0.7,
                        "account_eligible": True,
                    }
                ]
            )

            self.assertEqual(fitted["eligible_count"], 1)
            self.assertEqual(candidates[0]["execution_horizon_sec"], 3600)
            self.assertGreater(candidates[0]["projected_net_pips"], 0.0)
            self.assertEqual(json.loads(state.read_text(encoding="utf-8"))["horizons_sec"], [3600])

    def test_signal_rank_can_use_partial_history_without_promoting_whole_lane(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = LanePromotionModel(
                root / "missing.sqlite",
                root / "missing.json",
                [300],
                fit_enabled=False,
            )
            model.signal_evidence = {
                ("momentum.fast", 300): {
                    "lane_id": "momentum.fast",
                    "family": "momentum",
                    "profile": "fast",
                    "horizon_sec": 300,
                    "eligible": False,
                    "independent_blocks": 3,
                    "raw": {"n": 18, "avg": 0.25, "win_rate": 55.6},
                    "training": {"n": 2, "avg": 0.20, "win_rate": 50.0},
                    "holdout": {"n": 1, "avg": 0.35, "win_rate": 100.0},
                }
            }
            ranked = model.rank_signal_candidates(
                [
                    {
                        "id": "signal-1",
                        "lane_id": "momentum.fast",
                        "family": "momentum",
                        "profile": "fast",
                        "instrument": "EUR_USD",
                        "direction": "buy",
                        "forecast_horizon_sec": 300,
                        "probability_up": 0.62,
                        "projected_net_pips": 0.8,
                        "signal_to_spread": 2.0,
                        "spread_pips": 0.8,
                        "account_eligible": True,
                    }
                ],
                min_confidence=0.54,
                min_expected_net_pips=0.05,
            )

        self.assertTrue(ranked[0]["signal_eligible"])
        self.assertGreater(ranked[0]["signal_confidence"], 0.54)
        self.assertFalse(model.signal_evidence[("momentum.fast", 300)]["eligible"])

    def test_preconsensus_miss_contributes_but_cannot_be_execution_eligible(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = LanePromotionModel(
                root / "missing.sqlite",
                root / "missing.json",
                [300],
                fit_enabled=False,
            )
            ranked = model.rank_signal_candidates(
                [
                    {
                        "id": "near-miss",
                        "lane_id": "momentum.fast",
                        "family": "momentum",
                        "instrument": "EUR_USD",
                        "direction": "buy",
                        "probability_up": 0.70,
                        "projected_net_pips": 1.0,
                        "spread_pips": 0.5,
                        "account_eligible": True,
                        "preconsensus_class": "near_threshold",
                        "preconsensus_blockers": ["signal_vs_spread"],
                        "matrix_input_weight": 0.25,
                    }
                ],
                min_confidence=0.50,
                min_expected_net_pips=-1.0,
            )

        self.assertEqual(len(ranked), 1)
        self.assertFalse(ranked[0]["signal_eligible"])
        self.assertIn(
            "setup_gate:signal_vs_spread",
            ranked[0]["signal_blocked_by"],
        )
        self.assertEqual(
            ranked[0]["signal_horizon_curve"][0]["matrix_input_weight"],
            0.25,
        )

    def test_partial_model_curve_only_votes_at_predicted_horizons(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = LanePromotionModel(
                root / "missing.sqlite",
                root / "missing.json",
                [300, 3600, 14400],
                fit_enabled=False,
            )
            ranked = model.rank_signal_candidates(
                [
                    {
                        "id": "partial-curve",
                        "lane_id": "model_signal.ngboost.m5",
                        "model_id": "ngboost",
                        "family": "modern_tabular_probabilistic",
                        "instrument": "EUR_USD",
                        "direction": "buy",
                        "spread_pips": 0.5,
                        "account_eligible": False,
                        "forecast_curve": {
                            "300": {
                                "probability_up": 0.61,
                                "predicted_signed_pips": 1.2,
                            },
                            "14400": {
                                "probability_up": 0.58,
                                "predicted_signed_pips": 4.0,
                            },
                        },
                    }
                ]
            )

        horizons = {
            row["horizon_sec"] for row in ranked[0]["signal_horizon_curve"]
        }
        self.assertEqual(horizons, {300, 14400})

    def test_short_horizon_must_clear_extra_turnover_cost(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = LanePromotionModel(
                root / "missing.sqlite",
                root / "missing.json",
                [60, 300],
                fit_enabled=False,
            )
            ranked = model.rank_signal_candidates(
                [
                    {
                        "id": "signal-curve",
                        "lane_id": "momentum.fast",
                        "family": "momentum",
                        "profile": "fast",
                        "instrument": "EUR_USD",
                        "direction": "buy",
                        "probability_up": 0.65,
                        "projected_net_pips": 0.8,
                        "signal_to_spread": 2.0,
                        "spread_pips": 0.8,
                        "account_eligible": True,
                    }
                ],
                min_confidence=0.54,
                min_expected_net_pips=0.05,
            )

            self.assertEqual(ranked[0]["execution_horizon_sec"], 300)
        curve = {
            row["horizon_sec"]: row
            for row in ranked[0]["signal_horizon_curve"]
        }
        self.assertGreater(curve[60]["short_horizon_cost_pips"], 0.0)
        self.assertGreater(curve[300]["short_horizon_cost_pips"], 0.0)
        self.assertGreater(
            curve[60]["short_horizon_cost_pips"],
            curve[300]["short_horizon_cost_pips"],
        )
        self.assertGreater(curve[60]["required_net_edge_pips"], curve[300]["required_net_edge_pips"])

    def test_predictor_promotion_applies_to_exact_horizon_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = LanePromotionModel(
                root / "missing.sqlite",
                root / "missing.json",
                [300, 3600],
                fit_enabled=False,
            )
            evidence = {
                "eligible": True,
                "independent_blocks": 12,
                "raw": {"n": 12, "avg": 0.5, "win_rate": 66.0},
                "training": {"n": 8, "avg": 0.4, "win_rate": 62.0},
                "holdout": {"n": 4, "avg": 0.7, "win_rate": 75.0},
            }
            ranked = model.rank_signal_candidates(
                [
                    {
                        "id": "model-signal",
                        "lane_id": "model_signal.catboost.m1",
                        "family": "modern_tabular_probabilistic",
                        "instrument": "EUR_USD",
                        "input_timeframe": "M1",
                        "account_eligible": True,
                        "spread_pips": 0.2,
                        "forecast_curve": {
                            "300": {
                                "probability_up": 0.62,
                                "projected_net_pips": 0.5,
                                "account_eligible": True,
                                "predictor_promotion_evidence": evidence,
                            },
                            "3600": {
                                "probability_up": 0.63,
                                "projected_net_pips": 2.0,
                                "account_eligible": False,
                                "research_only": True,
                            },
                        },
                    }
                ]
            )

        points = {
            row["horizon_sec"]: row for row in ranked[0]["signal_horizon_curve"]
        }
        self.assertTrue(points[300]["account_eligible"])
        self.assertTrue(points[300]["signal_eligible"])
        self.assertFalse(points[3600]["account_eligible"])
        self.assertFalse(points[3600]["signal_eligible"])
        self.assertIn("predictor_cell_not_promoted", points[3600]["signal_blocked_by"])

    def test_structural_impulse_is_costed_without_double_probability_discount(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = LanePromotionModel(
                root / "missing.sqlite",
                root / "missing.json",
                [60, 300],
                fit_enabled=False,
            )
            ranked = model.rank_signal_candidates(
                [
                    {
                        "id": "structural",
                        "lane_id": "momentum.fast",
                        "family": "momentum",
                        "instrument": "EUR_USD",
                        "direction": "buy",
                        "signal_strength_pips": 20.0,
                        "signal_reference_horizon_sec": 300,
                        "signal_to_spread": 4.0,
                        "spread_pips": 1.0,
                        "atr_pips": 10.0,
                        "account_eligible": True,
                    }
                ],
                min_confidence=0.50,
                min_expected_net_pips=-10.0,
            )

        curve = {row["horizon_sec"]: row for row in ranked[0]["signal_horizon_curve"]}
        self.assertEqual(curve[60]["projection_method"], "costed_structural_impulse")
        self.assertGreaterEqual(curve[60]["projected_gross_movement_pips"], 4.0)
        self.assertLess(
            curve[60]["instant_projected_net_pips"],
            curve[300]["instant_projected_net_pips"],
        )
        self.assertLess(curve[300]["projected_gross_movement_pips"], 20.0)

    def test_equation_projection_is_capped_by_horizon_volatility(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = LanePromotionModel(
                root / "missing.sqlite",
                root / "missing.json",
                [60],
                fit_enabled=False,
            )
            ranked = model.rank_signal_candidates(
                [
                    {
                        "id": "equation",
                        "lane_id": "equation.fast",
                        "family": "equation",
                        "instrument": "EUR_USD",
                        "direction": "buy",
                        "predicted_signed_pips": 50.0,
                        "signal_reference_horizon_sec": 60,
                        "signal_to_spread": 4.0,
                        "spread_pips": 0.5,
                        "atr_pips": 5.0,
                        "account_eligible": True,
                    }
                ],
                min_confidence=0.50,
                min_expected_net_pips=-10.0,
            )

        signal = ranked[0]
        self.assertEqual(signal["projection_method"], "horizon_scaled_equation")
        self.assertAlmostEqual(signal["projected_gross_movement_pips"], 2.2361, places=4)
        self.assertAlmostEqual(signal["instant_projected_net_pips"], 1.7361, places=4)

    def test_h2_and_longer_signals_wait_for_matured_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = LanePromotionModel(
                root / "missing.sqlite",
                root / "missing.json",
                [7200],
                fit_enabled=False,
            )
            ranked = model.rank_signal_candidates(
                [
                    {
                        "id": "h2-cold-start",
                        "lane_id": "momentum.fast",
                        "family": "momentum",
                        "profile": "fast",
                        "instrument": "EUR_USD",
                        "direction": "buy",
                        "forecast_horizon_sec": 7200,
                        "probability_up": 0.90,
                        "projected_net_pips": 2.0,
                        "signal_to_spread": 4.0,
                        "spread_pips": 0.5,
                        "account_eligible": True,
                    }
                ],
                min_confidence=0.54,
                min_expected_net_pips=0.05,
            )

        signal = ranked[0]
        self.assertFalse(signal["signal_eligible"])
        self.assertEqual(signal["signal_confidence"], 0.5)
        self.assertIn("cold_start_evidence", signal["signal_blocked_by"])
        self.assertFalse(signal["cold_start_ready"])
        self.assertEqual(signal["cold_start_required_samples"], 30)
        self.assertEqual(signal["cold_start_required_blocks"], 4)

    def test_normalized_ranking_prefers_liquid_margin_efficient_signal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = LanePromotionModel(
                root / "missing.sqlite",
                root / "missing.json",
                [300],
                fit_enabled=False,
            )
            ranked = model.rank_signal_candidates(
                [
                    {
                        "id": "exotic",
                        "lane_id": "momentum.fast",
                        "family": "momentum",
                        "instrument": "EUR_NOK",
                        "direction": "buy",
                        "probability_up": 0.65,
                        "projected_net_pips": 10.0,
                        "spread_pips": 20.0,
                        "atr_pips": 15.0,
                        "pip": 0.0001,
                        "bid": 11.0,
                        "ask": 11.004,
                        "margin_rate": 0.05,
                        "liquidity_quality": 0.20,
                        "account_eligible": True,
                    },
                    {
                        "id": "major",
                        "lane_id": "momentum.fast",
                        "family": "momentum",
                        "instrument": "EUR_USD",
                        "direction": "buy",
                        "probability_up": 0.65,
                        "projected_net_pips": 0.8,
                        "spread_pips": 0.8,
                        "atr_pips": 10.0,
                        "pip": 0.0001,
                        "bid": 1.10,
                        "ask": 1.10008,
                        "margin_rate": 0.0333,
                        "liquidity_quality": 0.95,
                        "account_eligible": True,
                    },
                ],
                min_confidence=0.54,
                min_expected_net_pips=0.05,
            )

        self.assertEqual(ranked[0]["id"], "major")
        self.assertGreater(
            ranked[0]["normalized_rank_score"],
            ranked[1]["normalized_rank_score"],
        )
        self.assertLessEqual(
            abs(ranked[1]["legacy_score_tiebreaker"]),
            0.001,
        )

    def test_legacy_score_cannot_overpower_margin_hour_cost_penalty(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = LanePromotionModel(
                root / "missing.sqlite",
                root / "missing.json",
                [300, 14400],
                fit_enabled=False,
            )
            ranked = model.rank_signal_candidates(
                [
                    {
                        "id": "wide-exotic",
                        "lane_id": "ridge_return.s1.fast",
                        "family": "ridge_return",
                        "instrument": "USD_NOK",
                        "direction": "sell",
                        "probability_up": 0.42,
                        "projected_net_pips": 30.0,
                        "forecast_horizon_sec": 14400,
                        "spread_pips": 50.0,
                        "atr_pips": 5.0,
                        "pip_return_on_margin_per_pip_pct": 0.02,
                        "liquidity_quality": 0.60,
                        "account_eligible": True,
                    },
                    {
                        "id": "liquid-major",
                        "lane_id": "momentum.fast",
                        "family": "momentum",
                        "instrument": "EUR_USD",
                        "direction": "buy",
                        "probability_up": 0.60,
                        "projected_net_pips": 0.6,
                        "forecast_horizon_sec": 300,
                        "spread_pips": 0.8,
                        "atr_pips": 8.0,
                        "pip_return_on_margin_per_pip_pct": 0.45,
                        "liquidity_quality": 1.0,
                        "account_eligible": True,
                    },
                ],
                min_confidence=0.5,
                min_expected_net_pips=-100.0,
            )

        self.assertEqual(ranked[0]["id"], "liquid-major")
        self.assertLessEqual(
            abs(ranked[1]["legacy_score_tiebreaker"]),
            0.001,
        )

    def test_compact_store_incrementally_backfills_only_selected_signal_horizons(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_path = root / "source.sqlite"
            compact_path = root / "compact.sqlite"
            source = LanePromotionStore(source_path)
            for index, row in enumerate(outcome_rows(60)):
                source.observe(
                    event_id=f"h1-{index}",
                    horizon_sec=3600,
                    lane_id=row["lane_id"],
                    family=row["family"],
                    profile=row["profile"],
                    kind="signal",
                    instrument=row["instrument"],
                    direction=row["direction"],
                    entry_time=row["entry_time"],
                    endpoint_pips=row["endpoint_pips"],
                )
            source.observe(
                event_id="excluded-300",
                horizon_sec=300,
                lane_id="other.fast",
                family="other",
                profile="fast",
                kind="signal",
                instrument="EUR_USD",
                direction="buy",
                entry_time="2026-07-01T00:00:00+00:00",
                endpoint_pips=1.0,
            )
            source.close()

            compact = LanePromotionStore(compact_path)
            progress = compact.backfill_from(source_path, [3600], chunk_rows=17, max_chunks=10)
            compact.close()

            connection = sqlite3.connect(compact_path)
            count = connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0]
            horizons = connection.execute("SELECT DISTINCT horizon_sec FROM outcomes").fetchall()
            connection.close()
            self.assertTrue(progress["source_complete"])
            self.assertEqual(count, 60)
            self.assertEqual(horizons, [(3600,)])

    def test_legacy_second_ridge_lane_is_normalized_into_s1_matrix_key(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "promotion.sqlite"
            store = LanePromotionStore(path)
            store.observe(
                event_id="legacy-s1",
                horizon_sec=300,
                lane_id="second_ridge_forecast.h300.fast",
                family="second_ridge_forecast",
                profile="fast",
                kind="signal",
                instrument="EUR_USD",
                direction="buy",
                entry_time="2026-07-15T12:00:00+00:00",
                endpoint_pips=0.5,
            )
            store.flush()
            row = store.connection.execute(
                "SELECT lane_id, family FROM outcomes WHERE event_id = 'legacy-s1'"
            ).fetchone()
            store.close()
        self.assertEqual(row, ("ridge_return.s1.fast", "ridge_return"))

    def test_expanding_horizons_rewinds_backfill_for_previously_skipped_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_path = root / "source.sqlite"
            compact_path = root / "compact.sqlite"
            source = LanePromotionStore(source_path)
            for horizon in (300, 3600):
                source.observe(
                    event_id=f"event-{horizon}",
                    horizon_sec=horizon,
                    lane_id="momentum.fast",
                    family="momentum",
                    profile="fast",
                    kind="signal",
                    instrument="EUR_USD",
                    direction="buy",
                    entry_time="2026-07-15T12:00:00+00:00",
                    endpoint_pips=0.5,
                )
            source.close()

            compact = LanePromotionStore(compact_path)
            first = compact.backfill_from(source_path, [3600], max_chunks=10)
            expanded = compact.backfill_from(source_path, [300, 3600], max_chunks=10)
            horizons = compact.connection.execute(
                "SELECT horizon_sec FROM outcomes ORDER BY horizon_sec"
            ).fetchall()
            compact.close()

        self.assertTrue(first["horizon_backfill_reset"])
        self.assertTrue(expanded["horizon_backfill_reset"])
        self.assertEqual(horizons, [(300,), (3600,)])

    def test_dedicated_s1_ledger_syncs_into_unified_promotion_store(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_path = root / "s1.sqlite"
            unified_path = root / "unified.sqlite"
            source = LanePromotionStore(source_path)
            source.observe(
                event_id="s1-event",
                horizon_sec=60,
                lane_id="ridge_return.s1.fast",
                family="ridge_return",
                profile="fast",
                kind="signal",
                instrument="EUR_USD",
                direction="buy",
                entry_time="2026-07-15T12:00:00+00:00",
                endpoint_pips=0.4,
            )
            source.flush()

            unified = LanePromotionStore(unified_path)
            progress = unified.sync_from_compact(source_path, [60])
            row = unified.connection.execute(
                "SELECT lane_id, horizon_sec, endpoint_pips FROM outcomes"
            ).fetchone()
            unified.close()
            source.close()

        self.assertTrue(progress["source_complete"])
        self.assertEqual(progress["inserted"], 1)
        self.assertEqual(row, ("ridge_return.s1.fast", 60, 0.4))


if __name__ == "__main__":
    unittest.main()
