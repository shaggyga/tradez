#!/usr/bin/env python3
"""Build initial-release CPI values and replay one locked H168 rule."""

from pathlib import Path

import oanda_alfred_macro_relative_strength_research as research
import oanda_alfred_short_rate_initial_release_snapshot as snapshot


ROOT = Path(__file__).resolve().parent
SOURCE_CONFIG = ROOT / "config" / "alfred_cpi_initial_release_snapshot_v1.json"
SOURCE_DB = ROOT / "data" / "oanda_training_manager" / "state" / "alfred_cpi_initial_release_snapshot_v1.sqlite"
SOURCE_STATE = ROOT / "data" / "oanda_training_manager" / "state" / "alfred_cpi_initial_release_snapshot_v1.json"
SOURCE_REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "alfred_vintages" / "ALFRED_CPI_INITIAL_RELEASE_SNAPSHOT_20260816.md"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "reports" / "macro_relative_strength" / "ALFRED_CPI_POINT_IN_TIME_REPLAY_20260816.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "macro_relative_strength" / "ALFRED_CPI_POINT_IN_TIME_REPLAY_20260816.md"


def run():
    source = snapshot.run(SOURCE_CONFIG, SOURCE_DB, SOURCE_STATE, SOURCE_REPORT)
    replay = research.run(
        database_path=SOURCE_DB,
        state_path=SOURCE_STATE,
        alfred_config_path=SOURCE_CONFIG,
        config_path=ROOT / "config" / "alfred_cpi_point_in_time_replay_v1.json",
        output_path=OUTPUT,
        report_path=REPORT,
    )
    return {"source": source, "replay": replay}


if __name__ == "__main__":
    run()
