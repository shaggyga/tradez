import unittest
import json
import tempfile
import time
import sqlite3
from pathlib import Path
from unittest.mock import patch

import numpy as np

from trad.oanda_proof_shadow_predictors import (
    FAMILIES,
    atomic_json,
    build_forecasts,
    code_hash,
    currency_factor_matrix,
    durable_proof_counts,
    feature_vector,
    normal_probability_up,
    ridge_fit_predict,
    run_with_progress_heartbeat,
    supervised_rows,
)


def snapshot():
    instruments = {}
    pairs = ("EUR_USD", "USD_JPY", "EUR_JPY", "GBP_USD")
    for index, pair in enumerate(pairs):
        pip = 0.01 if pair.endswith("_JPY") else 0.0001
        base = 150.0 if pair.endswith("_JPY") else 1.1
        values = [
            base + pip * (0.03 * step + np.sin(step / 9.0 + index))
            for step in range(320)
        ]
        instruments[pair] = {
            "feature_origin_utc": "2026-08-06T12:00:00+00:00",
            "quote": {
                "bid": values[-1],
                "ask": values[-1] + 2 * pip,
                "pip": pip,
                "time": "2026-08-06T12:01:00+00:00",
            },
            "series": {"M1": values},
        }
    return {
        "generated_epoch": 1786017600.0,
        "generated_utc": "2026-08-06T12:00:00+00:00",
        "instruments": instruments,
    }


class ProofShadowPredictorTests(unittest.TestCase):
    def test_operational_changes_do_not_mutate_frozen_prediction_contract(self):
        self.assertEqual(
            code_hash(),
            "1bca3b3b583dcad31d25af7dff44e005c417e81d2e6392d763ade02f04cb0752",
        )

    def test_slow_build_publishes_progress_heartbeats(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "progress.json"

            def slow_operation():
                time.sleep(0.08)
                return json.loads(target.read_text(encoding="utf-8"))

            observed = run_with_progress_heartbeat(
                target,
                {"phase": "building_forecasts"},
                slow_operation,
                interval_sec=0.01,
            )

            self.assertEqual(observed["phase"], "building_forecasts")
            self.assertGreaterEqual(observed["progress_sequence"], 1)

    def test_durable_counts_survive_process_counter_reset(self):
        cohorts = {family: f"{family}.frozen" for family in FAMILIES}
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "ledger.sqlite"
            evidence = Path(directory) / "edge_evidence_v1.json"
            evidence.write_text(
                json.dumps(
                    {
                        "prospective_governance": {
                            "proof_cohorts": {
                                "cohorts": [
                                    {
                                        "family": family,
                                        "cohort_id": cohorts[family],
                                        "forecast_count": index + 1,
                                        "matured_outcome_count": index % 2,
                                    }
                                    for index, family in enumerate(FAMILIES)
                                ]
                                + [
                                    {
                                        "family": FAMILIES[0],
                                        "cohort_id": "superseded",
                                        "forecast_count": 999,
                                        "matured_outcome_count": 999,
                                    }
                                ]
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            counts = durable_proof_counts(database, cohorts, evidence)

        self.assertEqual(counts[FAMILIES[0]], {"forecasts": 1, "matured": 0})
        self.assertEqual(counts[FAMILIES[1]], {"forecasts": 2, "matured": 1})

    def test_atomic_json_retries_transient_windows_access_denial(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "proof.json"
            real_replace = __import__("os").replace
            attempts = 0

            def flaky_replace(source, destination):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise PermissionError(5, "transient access denial")
                real_replace(source, destination)

            with patch(
                "trad.oanda_proof_shadow_predictors.os.replace",
                side_effect=flaky_replace,
            ):
                atomic_json(target, {"status": "healthy"})

            self.assertEqual(attempts, 2)
            self.assertEqual(json.loads(target.read_text(encoding="utf-8")), {"status": "healthy"})
            self.assertFalse(list(target.parent.glob("proof.json.*.tmp")))

    def test_supervised_features_are_strictly_backward_looking(self):
        prices = np.arange(200.0)
        vector = feature_vector(prices, 100, 1.0)
        self.assertEqual(vector[0], 1.0)
        self.assertEqual(vector[5], 60.0)
        x, y = supervised_rows(prices, 1.0, horizon_steps=10)
        self.assertEqual(len(x), len(y))
        self.assertTrue(np.allclose(y, 10.0))

    def test_ridge_probability_is_finite(self):
        x = np.arange(300.0).reshape(100, 3)
        y = x[:, 0] * 0.1
        prediction, sigma = ridge_fit_predict(x, y, x[-1])
        self.assertTrue(np.isfinite(prediction))
        self.assertGreater(sigma, 0.0)
        self.assertGreater(normal_probability_up(prediction, sigma), 0.5)

    def test_currency_factor_graph_has_shared_nodes(self):
        raw = snapshot()["instruments"]
        series = {key: np.asarray(value["series"]["M1"]) for key, value in raw.items()}
        pips = {key: value["quote"]["pip"] for key, value in raw.items()}
        currencies, factors, minimum = currency_factor_matrix(series, pips)
        self.assertIn("USD", currencies)
        self.assertEqual(factors.shape[0], minimum - 1)

    def test_forecasts_are_proof_only_and_deterministic(self):
        cohorts = {
            family: f"{family}.governed-test" for family in (
                "ridge_return_repaired",
                "modern_tabular_probabilistic_repaired",
                "cross_pair_graph_transfer",
                "probabilistic_state_space",
            )
        }
        first = build_forecasts(snapshot(), cohort_ids=cohorts)
        second = build_forecasts(snapshot(), cohort_ids=cohorts)
        self.assertTrue(first)
        self.assertEqual([row["id"] for row in first], [row["id"] for row in second])
        self.assertTrue(set(row["family"] for row in first).issubset(set(FAMILIES)))
        self.assertTrue(all(row["research_only"] for row in first))
        self.assertTrue(all(not row["account_eligible"] for row in first))
        self.assertTrue(all(not row["can_place_orders"] for row in first))
        self.assertTrue(all(row["entry_time"] == snapshot()["generated_utc"] for row in first))
        self.assertTrue(all(row["cohort_id"] == cohorts[row["family"]] for row in first))
        self.assertTrue(all(row["training_dataset_sha256"].startswith("sha256:") for row in first))
        self.assertTrue(all(row["forecast_contract_version"].endswith("cohort_bound") for row in first))


if __name__ == "__main__":
    unittest.main()
