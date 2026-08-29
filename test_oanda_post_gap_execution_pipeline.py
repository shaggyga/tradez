import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from trad.oanda_post_gap_execution_pipeline import (
    EXPECTED_MATRIX_CELLS,
    audit_panel_economics,
    build_candidate_policy,
    build_model_cell_leaderboard,
    promote_or_hold,
)
from trad.oanda_shared_timeframe_horizon_panel import (
    CANONICAL_HORIZONS_SEC,
    CANONICAL_TIMEFRAMES,
)


class PostGapExecutionPipelineTests(unittest.TestCase):
    def test_full_matrix_economics_verifies_bid_ask_identity(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            panel = root / "panel.parquet"
            timeframes = CANONICAL_TIMEFRAMES
            horizons = CANONICAL_HORIZONS_SEC
            rows = []
            for pair_index in range(68):
                instrument = f"P{pair_index:02d}_USD"
                for timeframe in timeframes:
                    for horizon in horizons:
                        entry_spread = 1.0
                        exit_spread = 1.2
                        mid_move = 3.0
                        rows.append(
                            {
                                "instrument": instrument,
                                "direction": "LONG",
                                "input_timeframe": timeframe,
                                "horizon_sec": horizon,
                                "entry_spread_pips": entry_spread,
                                "market_mid_move_pips": mid_move,
                                "realized_net_pips": mid_move - (entry_spread + exit_spread) / 2,
                                "opposite_net_pips": -mid_move - (entry_spread + exit_spread) / 2,
                                "current_volume": 100.0,
                                "volume_ratio_12": 1.0,
                                "volume_ratio_30": 1.0,
                            }
                        )
            pd.DataFrame(rows).to_parquet(panel, index=False)
            market_report = root / "market.json"
            market_report.write_text(
                json.dumps({"dataset": {"sources": [{"path": str(panel)}]}}),
                encoding="utf-8",
            )
            result = audit_panel_economics(market_report)
            self.assertTrue(result["validation_passed"])
            self.assertEqual(result["scope"]["instruments"], 68)
            self.assertEqual(
                result["scope"]["timeframe_horizon_cells"],
                EXPECTED_MATRIX_CELLS,
            )
            self.assertEqual(result["violations"]["pricing_identity"], 0)
            self.assertAlmostEqual(result["cells"][0]["mean_round_trip_cost_pips"], 2.2)

    def test_cell_leaderboard_uses_cost_break_even_and_fdr(self):
        market = {
            "results": [
                {
                    "model": "ridge",
                    "cells": [
                        {
                            "input_timeframe": "M1",
                            "horizon_sec": 300,
                            "events": 1000,
                            "trades": 1000,
                            "direction_accuracy": 0.60,
                            "win_rate": 0.60,
                            "mean_net_pips": 0.2,
                            "median_net_pips": 0.1,
                            "sum_net_pips": 200.0,
                            "profit_factor": 1.2,
                        }
                    ],
                }
            ]
        }
        economics = {
            "cells": [
                {
                    "input_timeframe": "M1",
                    "horizon_sec": 300,
                    "n": 1000,
                    "minimum_direction_accuracy_to_break_even": 0.52,
                    "mean_round_trip_cost_pips": 1.0,
                    "mean_absolute_mid_move_pips": 2.0,
                }
            ]
        }
        rows = build_model_cell_leaderboard(market, economics)
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["eligible"])
        self.assertLess(rows[0]["direction_edge_fdr_q_value"], 0.10)

    def test_challenger_with_more_negative_cells_is_not_promoted(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "incumbent.json"
            expired = datetime.now(timezone.utc) - timedelta(hours=1)
            incumbent = {
                "status": "active",
                "policy_id": "old",
                "frozen_until_utc": expired.isoformat(),
                "cells": {
                    "a": {"eligible": True, "negative_evidence": False},
                    "b": {"eligible": False, "negative_evidence": True},
                },
            }
            path.write_text(json.dumps(incumbent), encoding="utf-8")
            challenger = {
                "status": "active",
                "policy_id": "new",
                "cells": {
                    "a": {"eligible": True, "negative_evidence": False},
                    "b": {"eligible": False, "negative_evidence": True},
                    "c": {"eligible": False, "negative_evidence": True},
                },
            }
            selected, decision = promote_or_hold(challenger, path)
            self.assertEqual(selected["policy_id"], "old")
            self.assertEqual(decision["action"], "hold_incumbent")

    def test_shadow_only_incumbent_does_not_freeze_active_policy(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "incumbent.json"
            path.write_text(
                json.dumps(
                    {
                        "status": "shadow_only",
                        "policy_id": "shadow",
                        "frozen_until_utc": (
                            datetime.now(timezone.utc) + timedelta(hours=2)
                        ).isoformat(),
                        "cells": {},
                    }
                ),
                encoding="utf-8",
            )
            challenger = {
                "status": "active",
                "policy_id": "active",
                "cells": {},
            }
            selected, decision = promote_or_hold(challenger, path)
            self.assertEqual(selected["policy_id"], "active")
            self.assertEqual(decision["action"], "promote_challenger")


if __name__ == "__main__":
    unittest.main()
