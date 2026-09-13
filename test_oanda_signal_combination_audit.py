import json
import copy
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from trad.oanda_signal_combination_audit import (
    SignalCombinationModel,
    FUZZY_VALIDATION_CONTRACT,
    build_signal_vector,
    chronological_partitions,
    feature_domain,
    fuzzy_membership,
    independence_adjusted_rule_score,
    mine_fuzzy_rules,
    rule_feature_domain_summary,
    write_rule_state,
)
from trad.oanda_strategy_exit_fit import PATH_LEVELS, StrategyExitFit, level_key, simulate_exit
from trad.oanda_strategy_exit_fit_worker import sync_shadow_outcomes


class ExitFitTests(unittest.TestCase):
    def test_rule_state_preserves_raw_order_and_adds_shadow_independence_ranking(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            database = root / "rules.sqlite"
            connection = sqlite3.connect(database)
            connection.execute(
                "CREATE TABLE snapshots (snapshot_id TEXT PRIMARY KEY)"
            )
            connection.execute(
                "CREATE TABLE outcomes (horizon_sec INTEGER)"
            )
            connection.commit()
            connection.close()
            rules = [
                {
                    "rule_id": "raw-first",
                    "score": 100.0,
                    "holdout": {"weighted_support": 100},
                    "condition_count": 2,
                    "conditions": [
                        {"feature": "atr_m1_pips"},
                        {"feature": "live_spread_atr"},
                    ],
                },
                {
                    "rule_id": "independent-second",
                    "score": 80.0,
                    "holdout": {"weighted_support": 80},
                    "condition_count": 2,
                    "conditions": [
                        {"feature": "atr_m1_pips"},
                        {"feature": "ema_gap_m15_5_13"},
                    ],
                },
            ]
            for rule in rules:
                rule["validation_contract"] = FUZZY_VALIDATION_CONTRACT
                rule["selection_calibration"] = rule.pop("holdout")
            payload = write_rule_state(root / "rules.json", database, rules, {})
        self.assertEqual(payload["rules"][0]["rule_id"], "raw-first")
        self.assertEqual(
            payload["top_rules_independence_adjusted"][0]["rule_id"],
            "independent-second",
        )
        self.assertTrue(
            payload["rules"][0]["independence_adjusted_shadow_only"]
        )

    def test_independence_adjusted_score_penalizes_same_domain_depth_only_in_shadow(self):
        redundant = {
            "score": 100.0,
            "conditions": [
                {"feature": "atr_m1_pips"},
                {"feature": "live_spread_atr"},
            ],
        }
        independent = {
            "score": 100.0,
            "conditions": [
                {"feature": "atr_m1_pips"},
                {"feature": "ema_gap_m15_5_13"},
            ],
        }
        self.assertEqual(independence_adjusted_rule_score(redundant), 50.0)
        self.assertEqual(independence_adjusted_rule_score(independent), 100.0)

    def test_rule_feature_domains_separate_condition_depth_from_information_breadth(self):
        conditions = [
            {"feature": "atr_m1_pips"},
            {"feature": "live_spread_atr"},
            {"feature": "ema_gap_m15_5_13"},
        ]
        summary = rule_feature_domain_summary(conditions)
        self.assertEqual(summary["feature_domain_count"], 2)
        self.assertEqual(summary["redundant_condition_count"], 1)
        self.assertEqual(
            summary["feature_domains"],
            ["trend_momentum", "volatility_liquidity"],
        )
        self.assertTrue(summary["feature_domain_shadow_only"])

    def test_raw_strategy_features_reuse_canonical_strategy_archetypes(self):
        self.assertEqual(feature_domain("raw_momentum_vote"), "trend_momentum")
        self.assertEqual(
            feature_domain("raw_higher_timeframe_alignment_vote"),
            "trend_momentum",
        )
        self.assertEqual(
            feature_domain("raw_currency_strength_activity"),
            "cross_sectional_value",
        )

    def test_offline_exit_worker_imports_new_shadow_path_rows(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "shadow.sqlite"
            connection = sqlite3.connect(source)
            connection.execute(
                """
                CREATE TABLE outcomes (
                    row_id INTEGER PRIMARY KEY,
                    event_id TEXT,
                    horizon_sec INTEGER,
                    lane_id TEXT,
                    family TEXT,
                    profile TEXT,
                    kind TEXT,
                    instrument TEXT,
                    direction TEXT,
                    entry_time TEXT,
                    exit_time TEXT,
                    theoretical_pips REAL,
                    max_favorable_pips REAL,
                    max_adverse_pips REAL,
                    path_samples INTEGER,
                    diagnostics_json TEXT
                )
                """
            )
            connection.execute(
                "INSERT INTO outcomes VALUES (1, ?, 300, 'test.fast', 'test', 'fast', "
                "'signal', 'EUR_USD', 'buy', '2026-01-01T00:00:00Z', "
                "'2026-01-01T00:05:00Z', 1.5, 2.0, 0.5, 10, ?)",
                ("event-1", json.dumps({"favorable_hits": {"2": 10.0}})),
            )
            connection.commit()
            connection.close()
            cursor = root / "cursor.json"
            cursor.write_text('{"source_row_id": 0}', encoding="utf-8")
            fit = StrategyExitFit(root / "exit.sqlite", root / "exit.json", fit_enabled=False)
            result = sync_shadow_outcomes(
                fit,
                source,
                cursor,
                batch_rows=100,
                max_batches=1,
            )
            count = fit.connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0]
            fit.close()
            self.assertEqual(result["inserted"], 1)
            self.assertEqual(count, 1)

    def test_exit_fit_state_only_mode_never_opens_the_large_ledger(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            database = root / "exit.sqlite"
            fit = StrategyExitFit(
                database,
                root / "exit.json",
                fit_enabled=False,
                record_enabled=False,
            )
            fit.observe(kind="signal", event_id="ignored", horizon_sec=300)
            fit.flush()
            fit.close()
            self.assertFalse(database.exists())

    def test_exit_fit_does_not_persist_rejected_shadow_misses(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            fit = StrategyExitFit(root / "exit.sqlite", root / "exit.json", fit_enabled=False)
            fit.observe(
                event_id="miss-1",
                horizon_sec=300,
                lane_id="test.fast",
                family="test",
                profile="fast",
                kind="miss",
                instrument="EUR_USD",
                direction="buy",
                endpoint_pips=2.0,
            )
            count = fit.connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0]
            fit.close()
        self.assertEqual(count, 0)

    def test_first_barrier_is_conservative_when_stop_and_target_both_hit(self):
        row = {
            "endpoint_pips": 3.0,
            "favorable_hits": {"4": 20.0},
            "adverse_hits": {"2": 10.0},
        }
        self.assertEqual(simulate_exit(row, 2.0, 2.0), -2.0)
        row["favorable_hits"]["4"] = 5.0
        self.assertEqual(simulate_exit(row, 2.0, 2.0), 4.0)

    def test_fit_promotes_only_after_chronological_holdout(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            fit = StrategyExitFit(root / "exit.sqlite", root / "exit.json", refresh_new_rows=1)
            favorable = {level_key(level): 10.0 for level in PATH_LEVELS if level <= 6.0}
            base = datetime(2026, 1, 1, tzinfo=timezone.utc)
            for index in range(300):
                entry_time = base + timedelta(seconds=300 * index)
                fit.observe(
                    event_id=f"event-{index}",
                    horizon_sec=300,
                    lane_id="test.fast",
                    family="test",
                    profile="fast",
                    kind="signal",
                    instrument="EUR_USD",
                    direction="buy",
                    entry_time=entry_time.isoformat(),
                    exit_time=(entry_time + timedelta(seconds=300)).isoformat(),
                    endpoint_pips=2.0,
                    max_favorable_pips=6.0,
                    max_adverse_pips=0.0,
                    path_samples=100,
                    favorable_hits=favorable,
                    adverse_hits={},
                )
            state = fit.fit()
            self.assertTrue(state["global"]["eligible"])
            self.assertIsNotNone(fit.recommendation("test.fast", "test"))
            fit.close()

    def test_prediction_quality_is_chronological_and_path_aware(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            fit = StrategyExitFit(root / "exit.sqlite", root / "exit.json", fit_enabled=False)
            base = datetime(2026, 1, 1, tzinfo=timezone.utc)
            for index in range(300):
                entry_time = base + timedelta(seconds=300 * index)
                fit.observe(
                    event_id=f"quality-{index}",
                    horizon_sec=300,
                    lane_id="ridge.fast",
                    family="ridge",
                    profile="fast",
                    model_id="ridge.s1.EUR_USD.h300",
                    input_timeframe="S1",
                    kind="signal",
                    instrument="EUR_USD",
                    direction="buy",
                    entry_time=entry_time.isoformat(),
                    exit_time=(entry_time + timedelta(seconds=300)).isoformat(),
                    endpoint_pips=2.0,
                    entry_spread_pips=1.2,
                    max_favorable_pips=4.0,
                    max_adverse_pips=0.5,
                    first_positive_sec=10.0,
                    path_samples=100,
                    positive_path_samples=80,
                    favorable_hits={},
                    adverse_hits={},
                    volatility_regime="normal",
                )
            state = fit.fit()
            self.assertIn("300", state["prediction_quality_by_horizon"])
            pair_family_timeframes = state["prediction_quality_by_horizon"]["300"][
                "pair_family_timeframes"
            ]
            self.assertIn("EUR_USD::ridge::S1", pair_family_timeframes)
            self.assertNotIn(
                "prediction_quality_evidence",
                state["horizon_states"]["300"],
            )
            evidence = fit.quality_evidence(
                "ridge.fast",
                "ridge",
                "EUR_USD",
                300,
                "normal",
                model_id="ridge.s1.EUR_USD.h300",
                input_timeframe="S1",
                direction="buy",
                entry_time="2026-01-01T00:30:00Z",
            )
            self.assertTrue(state["prediction_quality"]["eligible"])
            self.assertIsNotNone(evidence)
            self.assertTrue(evidence["eligible"])
            self.assertEqual(evidence["scope"], "pair_model_timeframe")
            self.assertEqual(
                evidence["holdout"]["median_time_to_positive_sec"],
                10.0,
            )
            self.assertEqual(evidence["holdout"]["positive_path_fraction"], 0.8)
            matrix = json.loads(
                (root / "pair_family_timeframe_horizon_v1.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(matrix["counts"]["observed_cells"], 1)
            self.assertEqual(
                matrix["rows"][0]["cell_key"],
                "EUR_USD|ridge|S1|300",
            )
            fit.close()

    def test_prediction_quality_marks_persistent_negative_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            fit = StrategyExitFit(root / "exit.sqlite", root / "exit.json", fit_enabled=False)
            base = datetime(2026, 1, 1, tzinfo=timezone.utc)
            for index in range(100):
                entry_time = base + timedelta(seconds=300 * index)
                fit.observe(
                    event_id=f"negative-{index}",
                    horizon_sec=300,
                    lane_id="ridge.fast",
                    family="ridge",
                    profile="fast",
                    model_id="ridge.s1.EUR_USD.h300",
                    input_timeframe="S1",
                    kind="signal",
                    instrument="EUR_USD",
                    direction="buy",
                    entry_time=entry_time.isoformat(),
                    exit_time=(entry_time + timedelta(seconds=300)).isoformat(),
                    endpoint_pips=-1.0,
                    entry_spread_pips=1.2,
                    max_favorable_pips=0.0,
                    max_adverse_pips=2.0,
                    path_samples=100,
                    positive_path_samples=0,
                    favorable_hits={},
                    adverse_hits={},
                    volatility_regime="normal",
                )
            fit.fit()
            evidence = fit.quality_evidence(
                "ridge.fast",
                "ridge",
                "EUR_USD",
                300,
                "normal",
                model_id="ridge.s1.EUR_USD.h300",
                input_timeframe="S1",
                direction="buy",
                entry_time="2026-01-01T00:30:00Z",
            )
            self.assertIsNotNone(evidence)
            self.assertFalse(evidence["eligible"])
            self.assertTrue(evidence["negative_evidence"])
            fit.close()

    def test_exit_fit_selects_pair_horizon_and_volatility_regime(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            fit = StrategyExitFit(
                root / "exit.sqlite",
                root / "exit.json",
                horizons_sec=[300, 3600],
            )
            favorable = {
                level_key(level): 10.0
                for level in PATH_LEVELS
                if level <= 6.0
            }
            base = datetime(2026, 1, 1, tzinfo=timezone.utc)
            for index in range(100):
                entry_time = base + timedelta(seconds=3600 * index)
                fit.observe(
                    event_id=f"expanded-{index}",
                    horizon_sec=3600,
                    lane_id="test.fast",
                    family="test",
                    profile="fast",
                    kind="signal",
                    instrument="EUR_USD",
                    direction="buy",
                    entry_time=entry_time.isoformat(),
                    exit_time=(entry_time + timedelta(seconds=3600)).isoformat(),
                    endpoint_pips=2.0,
                    max_favorable_pips=6.0,
                    max_adverse_pips=0.0,
                    path_samples=100,
                    favorable_hits=favorable,
                    adverse_hits={},
                    volatility_regime="expanded",
                )
            fit.fit()
            recommendation = fit.recommendation(
                "test.fast",
                "test",
                "EUR_USD",
                3600,
                "expanded",
            )
            fit.close()
            self.assertIsNotNone(recommendation)
            self.assertEqual(recommendation["scope"], "pair_lane_regime")
            self.assertEqual(recommendation["horizon_sec"], 3600)
            self.assertIsNone(
                fit.recommendation(
                    "test.fast",
                    "test",
                    "EUR_USD",
                    300,
                    "expanded",
                )
            )


def with_mature_outcomes(rows, horizon_sec):
    for row in rows:
        row['outcome_time'] = (datetime.fromisoformat(row['origin_time'].replace('Z', '+00:00')) + timedelta(seconds=horizon_sec)).isoformat()
        row.setdefault('signed_move_pips', 1.0)
        row.setdefault('long_net_pips', 0.8)
        row.setdefault('short_net_pips', -1.2)
    return rows


class CombinationAuditTests(unittest.TestCase):
    def test_chronological_partitions_align_timestamps_and_purge_horizon(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = []
        for minute in range(20):
            for instrument in ("EUR_USD", "GBP_USD"):
                rows.append(
                    {
                        "row_id": len(rows),
                        "instrument": instrument,
                        "origin_time": (base + timedelta(minutes=minute)).isoformat(),
                    }
                )
        ordered, train, selection, holdout, metadata = chronological_partitions(
            with_mature_outcomes(rows, 120),
            120,
            2,
        )
        train_rows = ordered[train]
        selection_rows = ordered[selection]
        holdout_rows = ordered[holdout]
        self.assertTrue(metadata["timestamp_aligned"])
        self.assertEqual({row["origin_time"] for row in selection_rows}, {selection_rows[0]["origin_time"]})
        self.assertGreaterEqual(
            datetime.fromisoformat(selection_rows[0]["origin_time"]).timestamp()
            - datetime.fromisoformat(train_rows[-1]["origin_time"]).timestamp(),
            120,
        )
        self.assertGreaterEqual(
            datetime.fromisoformat(holdout_rows[0]["origin_time"]).timestamp()
            - datetime.fromisoformat(selection_rows[-1]["origin_time"]).timestamp(),
            120,
        )

    def test_signal_vector_labels_tick_vwap_ema9_and_session_features(self):
        closes = [1.1000 + index * 0.0001 for index in range(60)]
        vector = build_signal_vector(
            {
                "candle_time": "2026-07-17T13:15:00Z",
                "pip": 0.0001,
                "m1_atr14_pips": 4.0,
                "m5_atr14_pips": 8.0,
                "closes": closes,
                "opens": [value - 0.00002 for value in closes],
                "highs": [value + 0.00005 for value in closes],
                "lows": [value - 0.00005 for value in closes],
                "volumes": [100.0 + index for index in range(60)],
                "m5_closes": closes[::5],
            }
        )
        self.assertGreater(vector["ema9_distance_m1_atr"], 0.0)
        self.assertGreater(vector["tick_vwap_distance_m1_30_atr"], 0.0)
        self.assertIn("ema9_vs_tick_vwap30_atr", vector)
        self.assertEqual(vector["session_london_new_york_overlap"], 1.0)

    def test_fuzzy_membership_softens_threshold_boundary(self):
        below = fuzzy_membership(0.9, ">=", 1.0, 0.2)
        at = fuzzy_membership(1.0, ">=", 1.0, 0.2)
        above = fuzzy_membership(1.1, ">=", 1.0, 0.2)
        self.assertLess(below, at)
        self.assertLess(at, above)
        self.assertAlmostEqual(at, 0.5)

    def test_fuzzy_equality_is_highest_at_center(self):
        self.assertGreater(
            fuzzy_membership(1.0, "~=", 1.0, 0.2),
            fuzzy_membership(1.3, "~=", 1.0, 0.2),
        )

    def test_rule_miner_finds_supported_two_signal_relation(self):
        rows = []
        for index in range(1200):
            positive = index % 4 == 0
            x = 1.2 + (index % 7) * 0.01 if positive else -0.6 + (index % 11) * 0.05
            y = -1.1 - (index % 5) * 0.01 if positive else 0.4 - (index % 9) * 0.04
            signed = 2.0 if positive else (-0.4 if index % 2 else 0.2)
            rows.append(
                {
                    "row_id": index,
                    "instrument": "EUR_USD",
                    "origin_time": f"2026-01-01T{index // 60:02d}:{index % 60:02d}:00Z",
                    "features": {"signal_a": x, "signal_b": y, "noise": (index % 13) / 13.0},
                    "signed_move_pips": signed,
                    "long_net_pips": signed - 0.2,
                    "short_net_pips": -signed - 0.2,
                }
            )
        rules = mine_fuzzy_rules(
            with_mature_outcomes(rows, 300),
            300,
            minimum_train_support=50,
            minimum_holdout_support=20,
            max_rules=20,
        )
        self.assertTrue(rules)
        self.assertTrue(any(len(rule["conditions"]) >= 2 for rule in rules))
        self.assertTrue(all(rule["selection"]["lower_probability_edge"] > 0.0 for rule in rules))
        self.assertTrue(all(rule["final_holdout_role"] == "report_only_after_rule_set_frozen" for rule in rules))

    def test_rule_miner_freezes_selection_before_adverse_final_period(self):
        rows = []
        for index in range(2000):
            active = index % 4 == 0
            if index < 1800:
                signed = 3.0 if active else (0.2 if index % 2 else -0.2)
            else:
                signed = -3.0 if active else (0.2 if index % 2 else -0.2)
            rows.append(
                {
                    "row_id": index,
                    "instrument": ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD")[index % 4],
                    "origin_time": (
                        datetime(2026, 1, 1, tzinfo=timezone.utc)
                        + timedelta(minutes=index)
                    ).isoformat(),
                    "features": {
                        "signal_a": 1.0 if active else -1.0,
                        "signal_b": -1.0 if active else 1.0,
                    },
                    "signed_move_pips": signed,
                    "long_net_pips": signed - 0.1,
                    "short_net_pips": -signed - 0.1,
                }
            )
        with_mature_outcomes(rows, 300)
        kwargs = dict(minimum_train_support=40, minimum_holdout_support=20, max_rules=20,
                      as_of_utc='2026-01-05T00:00:00Z')
        with self.assertRaisesRegex(ValueError, 'separate_selection_and_final_holdout'):
            mine_fuzzy_rules(rows, 300, chronological_validation_blocks=1, **kwargs)
        original = mine_fuzzy_rules(rows, 300, **kwargs)
        changed = copy.deepcopy(rows)
        # Only the final 15% changes; identities, selection and calibration must not.
        for row in changed[1700:]:
            row['signed_move_pips'] *= -1
            row['long_net_pips'], row['short_net_pips'] = row['short_net_pips'], row['long_net_pips']
        rescored = mine_fuzzy_rules(changed, 300, **kwargs)
        def frozen(items):
            return [{k:v for k,v in row.items() if k not in ('final_holdout', 'final_holdout_independence_audit')} for row in items]
        self.assertTrue(original)
        self.assertEqual(frozen(original), frozen(rescored))
        self.assertNotEqual([r['final_holdout'] for r in original], [r['final_holdout'] for r in rescored])
        self.assertTrue(all(not r['account_eligible'] for r in original))

    def test_rule_miner_beam_search_reaches_four_feature_interaction(self):
        rows = []
        for repeat in range(160):
            for state in range(16):
                bits = [(state >> shift) & 1 for shift in range(4)]
                features = {
                    name: (1.0 + 0.01 * (repeat % 5)) if bit else (-1.0 + 0.01 * (repeat % 5))
                    for name, bit in zip(("a", "b", "c", "d"), bits)
                }
                if bits[0] and bits[1]:
                    signed = 4.0 if all(bits) else 0.6
                else:
                    signed = 0.3 if (state + repeat) % 2 else -0.3
                index = len(rows)
                rows.append(
                    {
                        "row_id": index,
                        "instrument": ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD")[repeat % 4],
                        "origin_time": (
                            datetime(2026, 1, 1, tzinfo=timezone.utc)
                            + timedelta(minutes=index)
                        ).isoformat(),
                        "features": features,
                        "signed_move_pips": signed,
                        "long_net_pips": signed - 0.1,
                        "short_net_pips": -signed - 0.1,
                    }
                )
        rules = mine_fuzzy_rules(
            with_mature_outcomes(rows, 300),
            300,
            minimum_train_support=40,
            minimum_holdout_support=20,
            max_rules=50,
            max_conditions=4,
            beam_width=16,
            expansion_conditions=8,
            maximum_base_features=4,
        )
        self.assertTrue(any(rule["condition_count"] == 4 for rule in rules))
        self.assertTrue(all(not rule["account_eligible"] for rule in rules))
        self.assertTrue(
            all(rule["experimental_depth"] for rule in rules if rule["condition_count"] > 3)
        )

    def test_rule_miner_keeps_overlapping_holdout_evidence_in_shadow(self):
        rows = []
        for index in range(1200):
            positive = index % 4 == 0
            signed = 8.0 if positive else -0.2
            rows.append(
                {
                    "row_id": index,
                    "instrument": ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD")[index % 4],
                    "origin_time": (
                        datetime(2026, 1, 1, tzinfo=timezone.utc)
                        + timedelta(seconds=index % 120)
                    ).isoformat(),
                    "features": {
                        "signal_a": 1.0 if positive else -1.0,
                        "signal_b": -1.0 if positive else 1.0,
                        "noise": (index % 17) / 17.0,
                    },
                    "signed_move_pips": signed,
                    "long_net_pips": signed - 0.1,
                    "short_net_pips": -signed - 0.1,
                }
            )
        # All 2h labels overlap all origin blocks. A valid train/selection split
        # cannot be manufactured by falling back to row-index partitions.
        with self.assertRaisesRegex(ValueError, 'empty_partition_after_actual_maturity_purge'):
            mine_fuzzy_rules(with_mature_outcomes(rows, 7200), 7200,
                minimum_train_support=40, minimum_holdout_support=20, max_rules=20)

    def test_model_exposes_rule_probability_and_membership(self):
        with tempfile.TemporaryDirectory() as folder:
            state_path = Path(folder) / "rules.json"
            state_path.write_text(
                json.dumps(
                    {
                        "schema_version": 3,
                        "generated_at": "2026-01-01T00:00:00Z",
                        "search_config": {"chronological_validation_blocks": 2},
                        "rules": [
                            {
                                "rule_id": "h300-r1",
                                "horizon_sec": 300,
                                "conditions": [
                                    {"feature": "a", "operator": ">=", "threshold": 1.0, "width": 0.2},
                                    {"feature": "b", "operator": "<=", "threshold": -1.0, "width": 0.2},
                                ],
                                "condition_text": "a >= 1 AND b <= -1",
                                "predicted_direction": "buy",
                                "account_eligible": True,
                                "forward_refit_confirmed": True,
                                "training": {"weighted_support": 200},
                                "holdout": {
                                    "weighted_support": 80,
                                    "n": 75,
                                    "probability_up": 0.64,
                                    "expected_net_pips": 0.5,
                                    "expected_signed_move_pips": 0.7,
                                    "lower_probability_edge": 0.05,
                                    "brier": 0.22,
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            prediction = SignalCombinationModel(state_path).predict({"a": 1.4, "b": -1.4})
            self.assertTrue(prediction["ready"])
            self.assertEqual(prediction["rule_id"], "h300-r1")
            self.assertGreater(prediction["membership"], 0.8)
            self.assertTrue(prediction["account_eligible"])
            self.assertTrue(prediction["validation_policy_eligible"])

    def test_model_downgrades_legacy_rule_state_to_research_only(self):
        with tempfile.TemporaryDirectory() as folder:
            state_path = Path(folder) / "legacy_rules.json"
            state_path.write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "generated_at": "2026-01-01T00:00:00Z",
                        "rules": [
                            {
                                "rule_id": "legacy",
                                "horizon_sec": 300,
                                "conditions": [
                                    {
                                        "feature": "a",
                                        "operator": ">=",
                                        "threshold": 1.0,
                                        "width": 0.2,
                                    },
                                    {
                                        "feature": "b",
                                        "operator": "<=",
                                        "threshold": -1.0,
                                        "width": 0.2,
                                    },
                                ],
                                "predicted_direction": "buy",
                                "account_eligible": True,
                                "training": {"weighted_support": 200},
                                "holdout": {
                                    "weighted_support": 80,
                                    "probability_up": 0.64,
                                    "expected_net_pips": 0.5,
                                    "lower_probability_edge": 0.05,
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            prediction = SignalCombinationModel(state_path).predict(
                {"a": 1.4, "b": -1.4}
            )
            self.assertTrue(prediction["ready"])
            self.assertFalse(prediction["account_eligible"])
            self.assertFalse(prediction["validation_policy_eligible"])
            self.assertEqual(
                prediction["validation_policy_reason"],
                "legacy_combination_validation_schema",
            )

    def test_model_fails_closed_when_rule_artifact_is_stale(self):
        with tempfile.TemporaryDirectory() as folder:
            state_path = Path(folder) / "stale-rules.json"
            state_path.write_text(
                json.dumps(
                    {
                        "schema_version": 3,
                        "generated_at": "2020-01-01T00:00:00Z",
                        "search_config": {"chronological_validation_blocks": 2},
                        "rules": [
                            {
                                "rule_id": "stale",
                                "horizon_sec": 300,
                                "conditions": [
                                    {
                                        "feature": "a",
                                        "operator": ">=",
                                        "threshold": 1.0,
                                        "width": 0.2,
                                    }
                                ],
                                "predicted_direction": "buy",
                                "account_eligible": True,
                                "forward_refit_confirmed": True,
                                "training": {"weighted_support": 200},
                                "holdout": {
                                    "weighted_support": 80,
                                    "probability_up": 0.64,
                                    "expected_net_pips": 0.5,
                                    "lower_probability_edge": 0.05,
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            prediction = SignalCombinationModel(
                state_path,
                maximum_state_age_sec=3600.0,
            ).predict({"a": 1.4})

            self.assertFalse(prediction["ready"])
            self.assertEqual(prediction["reason"], "stale_combination_model_state")
            self.assertEqual(prediction["rules_available"], 0)


if __name__ == "__main__":
    unittest.main()
