import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from trad.oanda_edge_evidence import clipped_mean_pvalue
from trad.oanda_proof_cohort_registry import ensure_cohorts, verify_transition_replay
from trad.oanda_prospective_governance import (
    apply_hierarchical_fdr,
    build_governance,
    graduation_ladder,
    lock_discovery_candidate,
    open_confirmation_cohort,
)


def cohort_spec(alpha=10.0, cutoff="2026-08-06T12:00:00+00:00"):
    return {
        "family": "ridge_return_repaired",
        "source_sha256": "sha256:source",
        "model_specification": {"model": "ridge", "alpha": alpha},
        "hyperparameters": {"alpha": alpha},
        "feature_schema_version": "sha256:features",
        "training_policy": {"mode": "causal_rolling", "lookback": 512},
        "forecast_contract_version": "forecast_v2",
        "cost_model_version": "cost_v1",
        "dimensions": {"horizon_sec": 3600},
        "deduplication_rules": {"episode": "signed_factor"},
        "promotion_thresholds": {"minimum_edge": 1.25},
        "initial_training_cutoff_utc": cutoff,
        "initial_training_dataset_sha256": "sha256:data",
        "pre_governance_forecast_count": 191,
    }


def evidence_cell(cell_id="positive", *, positive=True):
    if positive:
        return {
            "cell_id": cell_id,
            "family": "ridge_return_repaired",
            "instrument": "EUR_USD",
            "horizon_sec": 3600,
            "session": "london",
            "liquidity_bucket": "liquid_le_2",
            "raw_n": 100,
            "effective_n": 100,
            "avg_net_pips": 4.0,
            "cost_clearance_probability": 0.7,
            "mfe_to_mae_ratio": 2.0,
            "expected_shortfall_pips": -3.0,
            "minimum_economic_edge_pips": 1.25,
            "time_uniform_lower_bound_pips": 1.5,
            "best_episode_profit_share": 0.1,
            "one_sided_pvalue_zero_bounded": 1e-9,
            "cost_shocks": {
                "total_slippage_0.50_pips": {"avg_net_pips": 3.5},
                "total_slippage_1.00_pips": {"avg_net_pips": 3.0},
            },
            "gate_distances": [],
            "lower_confidence_pips": 2.0,
            "unadjusted_upper_confidence_pips": 6.0,
            "minimum_detectable_edge_pips": 1.0,
            "additional_effective_episodes_for_power": 0,
        }
    result = evidence_cell(cell_id, positive=True)
    result.update(
        {
            "avg_net_pips": -1.0,
            "time_uniform_lower_bound_pips": -4.0,
            "one_sided_pvalue_zero_bounded": 1.0,
            "lower_confidence_pips": -2.0,
            "unadjusted_upper_confidence_pips": 0.0,
        }
    )
    result["cost_shocks"]["total_slippage_0.50_pips"]["avg_net_pips"] = -1.5
    result["cost_shocks"]["total_slippage_1.00_pips"]["avg_net_pips"] = -2.0
    return result


class ProspectiveGovernanceTests(unittest.TestCase):
    def test_material_change_creates_new_immutable_cohort(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database, state = root / "cohorts.sqlite", root / "cohorts.json"
            first = ensure_cohorts(database, state, [cohort_spec()])
            restart = ensure_cohorts(
                database,
                state,
                [cohort_spec(cutoff="2026-08-07T12:00:00+00:00")],
            )
            changed = ensure_cohorts(database, state, [cohort_spec(alpha=20.0)])
            self.assertEqual(first, restart)
            self.assertNotEqual(first, changed)
            connection = sqlite3.connect(database)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM proof_cohorts").fetchone()[0], 2)
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("UPDATE proof_cohorts SET family='changed'")
            connection.rollback()
            connection.close()
            saved = json.loads(state.read_text(encoding="utf-8"))
            self.assertEqual(saved["active_cohorts"], changed)
            self.assertEqual(saved["cohorts"][0]["pre_governance_forecast_count"], 191)
            self.assertEqual(saved["lineage_verification"]["status"], "verified")

    def test_superseded_contract_cannot_be_silently_reactivated(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database, state = root / "cohorts.sqlite", root / "cohorts.json"
            first = ensure_cohorts(database, state, [cohort_spec(alpha=10.0)])
            second = ensure_cohorts(database, state, [cohort_spec(alpha=20.0)])
            self.assertNotEqual(first, second)
            with self.assertRaisesRegex(RuntimeError, "refusing to reactivate superseded"):
                ensure_cohorts(database, state, [cohort_spec(alpha=10.0)])

            repeated = cohort_spec(alpha=10.0)
            repeated["cohort_generation_id"] = "independent_replication_2"
            third = ensure_cohorts(database, state, [repeated])
            self.assertNotEqual(first, third)
            self.assertNotEqual(second, third)

            connection = sqlite3.connect(database)
            verification = verify_transition_replay(connection)
            connection.close()
            self.assertEqual(verification["cohort_count"], 3)
            self.assertEqual(verification["transition_count"], 3)

    def test_transition_replay_fails_closed_on_inconsistent_append(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database, state = root / "cohorts.sqlite", root / "cohorts.json"
            active = ensure_cohorts(database, state, [cohort_spec()])
            connection = sqlite3.connect(database)
            connection.execute(
                "INSERT INTO proof_cohort_transitions VALUES (?,?,?,?,?,?)",
                (
                    "transition_bad_append",
                    "ridge_return_repaired",
                    None,
                    active["ridge_return_repaired"],
                    "2026-08-07T00:00:00+00:00",
                    "invalid_duplicate",
                ),
            )
            connection.commit()
            with self.assertRaisesRegex(RuntimeError, "transition replay failed"):
                verify_transition_replay(connection)
            connection.close()

    def test_random_and_permuted_returns_do_not_create_discovery(self):
        balanced = [1.0, -1.0] * 100
        self.assertGreaterEqual(clipped_mean_pvalue(balanced, 0.0, 20.0), 0.99)
        cell = evidence_cell("random", positive=False)
        family = {
            "family": cell["family"],
            "horizon_sec": cell["horizon_sec"],
            "one_sided_pvalue_zero_bounded": 1.0,
        }
        result = apply_hierarchical_fdr([cell], [family], family_q=0.05, cell_q=0.05)
        self.assertEqual(result["family_survivors"], 0)
        self.assertEqual(result["cell_survivors"], 0)
        ladder = graduation_ladder(
            [cell],
            {
                "immutability": "immutable_trigger_enforced",
                "duplicate_event_horizons": 0,
                "invalid_entry_times_excluded": 0,
                "invalid_boundary_times_excluded": 0,
            },
        )
        self.assertEqual(ladder["B_discovery_candidate"]["count"], 0)

    def test_profitable_fixture_can_reach_discovery_but_never_self_confirm(self):
        cell = evidence_cell()
        family = {
            "family": cell["family"],
            "horizon_sec": cell["horizon_sec"],
            "one_sided_pvalue_zero_bounded": 1e-9,
        }
        apply_hierarchical_fdr([cell], [family], family_q=0.05, cell_q=0.05)
        ladder = graduation_ladder(
            [cell],
            {
                "immutability": "immutable_trigger_enforced",
                "duplicate_event_horizons": 0,
                "invalid_entry_times_excluded": 0,
                "invalid_boundary_times_excluded": 0,
            },
        )
        self.assertEqual(ladder["B_discovery_candidate"]["count"], 1)
        self.assertEqual(ladder["C_locked_prospective_confirmation"]["count"], 0)
        self.assertFalse(ladder["D_practice_canary"]["auto_route"])

    def test_repeated_generation_has_identical_inference_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            sqlite3.connect(source).close()
            evidence = root / "evidence.sqlite"
            config = {"discovery": {"family_fdr_q": 0.05, "cell_fdr_q": 0.05}}
            integrity = {
                "source_highwater_row_id": 1,
                "immutability": "immutable_trigger_enforced",
                "duplicate_event_horizons": 0,
                "invalid_entry_times_excluded": 0,
                "invalid_boundary_times_excluded": 0,
            }
            first = build_governance(
                cells=[evidence_cell("negative", positive=False)],
                families=[{
                    "family": "ridge_return_repaired",
                    "horizon_sec": 3600,
                    "one_sided_pvalue_zero_bounded": 1.0,
                }],
                integrity=integrity,
                source_database=source,
                cohort_state_path=root / "missing.json",
                evidence_database=evidence,
                config=config,
            )
            second = build_governance(
                cells=[evidence_cell("negative", positive=False)],
                families=[{
                    "family": "ridge_return_repaired",
                    "horizon_sec": 3600,
                    "one_sided_pvalue_zero_bounded": 1.0,
                }],
                integrity=integrity,
                source_database=source,
                cohort_state_path=root / "missing.json",
                evidence_database=evidence,
                config=config,
            )
            self.assertEqual(first["governance_sha256"], second["governance_sha256"])
            connection = sqlite3.connect(evidence)
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM immutable_governance_snapshots").fetchone()[0],
                1,
            )
            connection.close()

    def test_confirmation_cannot_backdate_or_reuse_discovery_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            sqlite3.connect(source).close()
            evidence = root / "evidence.sqlite"
            cell = evidence_cell("locked-positive")
            result = build_governance(
                cells=[cell],
                families=[{
                    "family": cell["family"],
                    "horizon_sec": cell["horizon_sec"],
                    "one_sided_pvalue_zero_bounded": 1e-9,
                }],
                integrity={
                    "source_highwater_row_id": 1,
                    "immutability": "immutable_trigger_enforced",
                    "duplicate_event_horizons": 0,
                    "invalid_entry_times_excluded": 0,
                    "invalid_boundary_times_excluded": 0,
                },
                source_database=source,
                cohort_state_path=root / "missing.json",
                evidence_database=evidence,
                config={"discovery": {"family_fdr_q": 0.05, "cell_fdr_q": 0.05}},
            )
            lock_id = lock_discovery_candidate(
                evidence,
                governance_sha256=result["governance_sha256"],
                cell=cell,
                locked_utc="2026-08-06T12:00:00+00:00",
            )
            with self.assertRaises(ValueError):
                open_confirmation_cohort(
                    evidence,
                    candidate_lock_id=lock_id,
                    start_utc="2026-08-06T12:00:00+00:00",
                )
            confirmation = open_confirmation_cohort(
                evidence,
                candidate_lock_id=lock_id,
                start_utc="2026-08-06T12:00:01+00:00",
            )
            self.assertTrue(confirmation.startswith("confirmation_"))


if __name__ == "__main__":
    unittest.main()
