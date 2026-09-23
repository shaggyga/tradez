import argparse
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from trad import oanda_shared_timeframe_horizon_panel as shared_panel
from trad.oanda_model_gap_live_signal_worker import (
    LiveForecastLedger,
    archive_feature_snapshot,
)
from trad.oanda_prospective_live_panel import run, validate_existing
from trad.oanda_signal_contribution_feed import SignalContributionFeed


class ProspectiveLivePanelTests(unittest.TestCase):
    def test_archived_features_join_to_deduplicated_executable_outcome(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            generated = time.time() - 120.0
            generated_utc = datetime.fromtimestamp(
                generated,
                timezone.utc,
            ).isoformat()
            snapshot_id = "prospective-unit"
            features = {
                name: 0.0
                for name in (
                    *shared_panel.BASE_SCALAR_FEATURES,
                    *shared_panel.CROSS_PAIR_FEATURES,
                )
            }
            features.update(
                {
                    "pip": 0.0001,
                    "input_timeframe_seconds": 3600,
                    "candle_time": datetime.fromtimestamp(
                        generated - 60.0,
                        timezone.utc,
                    ).isoformat(),
                    "depth_imbalance": 0.2,
                }
            )
            snapshot = {
                "snapshot_id": snapshot_id,
                "generated_epoch": generated,
                "generated_utc": generated_utc,
                "instruments": {
                    "EUR_USD": {
                        "feature_origin_utc": features["candle_time"],
                        "quote": {
                            "bid": 1.1000,
                            "ask": 1.1001,
                            "time": generated_utc,
                            "source": "unit",
                        },
                        "features": features,
                        "timeframe_features": {"H1": features},
                        "microstructure": {"bid_levels": 2},
                    }
                },
            }
            archive_feature_snapshot(snapshot, root / "archive")
            feed = SignalContributionFeed(root / "feed.sqlite")
            ledger = LiveForecastLedger(root / "ledger.sqlite")
            candidates = []
            for model_id in ("catboost", "ngboost"):
                candidates.append(
                    feed.normalize_forecast(
                        {
                            "model_id": model_id,
                            "instrument": "EUR_USD",
                            "input_timeframe": "H1",
                            "generated_epoch": generated,
                            "bid": 1.1000,
                            "ask": 1.1001,
                            "pip": 0.0001,
                            "forecast_curve": {
                                "60": {"probability_up": 0.65},
                            },
                            "producer_metadata": {"artifact_sha256": model_id},
                        },
                        "unit",
                        now=generated,
                    )
                )
            self.assertEqual(ledger.register(candidates, snapshot_id), 2)
            ledger.mature(
                {
                    "generated_epoch": generated + 60.0,
                    "instruments": {
                        "EUR_USD": {
                            "quote": {"bid": 1.1003, "ask": 1.1004}
                        }
                    },
                },
                max_delay_sec=5.0,
            )
            ledger.close()
            feed.close()
            args = argparse.Namespace(
                archive=root / "archive",
                ledger=root / "ledger.sqlite",
                output=root / "panel.parquet",
                report=root / "report.json",
                window_days=30,
                min_events=1,
            )

            report = run(args)
            panel = pd.read_parquet(args.output)

        self.assertEqual(report["status"], "ready")
        self.assertEqual(report["artifact"]["events"], 1)
        self.assertEqual(len(panel), 2)
        results = dict(zip(panel["direction"], panel["realized_net_pips"]))
        self.assertAlmostEqual(results["LONG"], 2.0)
        self.assertAlmostEqual(results["SHORT"], -4.0)
        self.assertEqual(set(panel["input_timeframe"]), {"H1"})

    def test_existing_artifact_validation_does_not_rebuild_panel(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "panel.parquet"
            pd.DataFrame({"value": [1, 2]}).to_parquet(output, index=False)
            report = root / "report.json"
            report.write_text(
                __import__("json").dumps(
                    {
                        "status": "ready",
                        "generated_utc": datetime.now(timezone.utc).isoformat(),
                        "artifact": {
                            "path": str(output),
                            "bytes": output.stat().st_size,
                            "rows": 2,
                            "events": 1,
                            "sha256": "unit",
                        },
                        "contract": {
                            "features_are_point_in_time_archives": True,
                            "outcomes_are_later_executable_bid_ask_observations": True,
                            "duplicate_model_predictions_are_one_market_event": True,
                            "long_and_short_targets_include_entry_and_exit_spread": True,
                            "account_execution_authorized": False,
                        },
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                output=output,
                report=report,
                min_events=1,
            )
            before = output.stat().st_mtime_ns
            result = validate_existing(args)
            after = output.stat().st_mtime_ns

        self.assertEqual(result["status"], "valid")
        self.assertEqual(result["artifact"]["rows"], 2)
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
