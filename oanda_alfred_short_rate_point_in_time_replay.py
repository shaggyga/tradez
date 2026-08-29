#!/usr/bin/env python3
"""Build ALFRED initial-release rates and replay the one locked H24 rule."""

from pathlib import Path

import oanda_alfred_macro_relative_strength_research as research
import oanda_alfred_short_rate_initial_release_snapshot as snapshot


ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "data" / "oanda_training_manager" / "reports" / "macro_relative_strength" / "ALFRED_SHORT_RATE_POINT_IN_TIME_REPLAY_20260816.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "macro_relative_strength" / "ALFRED_SHORT_RATE_POINT_IN_TIME_REPLAY_20260816.md"


def run():
    source = snapshot.run()
    replay = research.run(
        database_path=snapshot.DATABASE,
        state_path=snapshot.OUTPUT,
        alfred_config_path=snapshot.CONFIG,
        config_path=ROOT / "config" / "alfred_short_rate_point_in_time_replay_v1.json",
        output_path=OUTPUT,
        report_path=REPORT,
    )
    return {"source": source, "replay": replay}


if __name__ == "__main__":
    run()
