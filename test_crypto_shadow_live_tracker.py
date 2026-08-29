import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from trad.crypto_shadow_live_tracker import CryptoShadowTracker


class CryptoShadowTrackerTests(unittest.TestCase):
    def test_state_summary_is_restored_and_updated_without_database_rescan(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            state_path = data_dir / "coinbase_shadow_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "message_count": 10,
                        "ticker_count": 8,
                        "trade_count": 6,
                        "signal_count": 4,
                        "matured_count": 2,
                        "sequence_gaps": 1,
                        "latest": {"BTC-USD": {"price": 100.0}},
                        "model_summaries": [
                            {
                                "model_id": "momentum_10s",
                                "horizon_sec": 60,
                                "matured": 2,
                                "direction_accuracy": 50.0,
                                "average_gross_bps": 1.0,
                                "average_net_bps": -2.0,
                                "net_win_rate": 0.0,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            tracker = CryptoShadowTracker(
                SimpleNamespace(
                    products=["BTC-USD"],
                    horizons=[60],
                    data_dir=data_dir,
                    round_trip_fee_bps=60.0,
                )
            )
            tracker._observe_summary("momentum_10s", 60, 4.0, 1.0, True)
            tracker.write_state()
            tracker.connection.close()
            payload = json.loads(state_path.read_text(encoding="utf-8"))

        summary = payload["model_summaries"][0]
        self.assertEqual(payload["message_count"], 10)
        self.assertEqual(summary["matured"], 3)
        self.assertEqual(summary["direction_accuracy"], 66.667)
        self.assertEqual(summary["average_gross_bps"], 2.0)
        self.assertEqual(summary["average_net_bps"], -1.0)
        self.assertEqual(summary["net_win_rate"], 33.333)


if __name__ == "__main__":
    unittest.main()
