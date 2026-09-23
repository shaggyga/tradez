import unittest
import json
import sqlite3
import tempfile
from pathlib import Path

from trad.oanda_cftc_positioning_shadow import (
    collect,
    contract_snapshot,
    load_release_schedule,
    normalized_position,
    scheduled_release,
)


def row(report_date: str, lev_long: int, lev_short: int) -> dict:
    return {
        "report_date_as_yyyy_mm_dd": report_date,
        "market_and_exchange_names": "EURO FX - CHICAGO MERCANTILE EXCHANGE",
        "open_interest_all": "1000",
        "lev_money_positions_long": str(lev_long),
        "lev_money_positions_short": str(lev_short),
        "asset_mgr_positions_long": "400",
        "asset_mgr_positions_short": "200",
        "dealer_positions_long_all": "100",
        "dealer_positions_short_all": "300",
    }


class CftcPositioningShadowTests(unittest.TestCase):
    def test_normalises_all_suffixed_dealer_fields(self):
        self.assertAlmostEqual(normalized_position(row("2026-07-28", 200, 100), "dealer"), -0.2)

    def test_first_seen_is_preserved_only_for_same_report(self):
        current = row("2026-07-28", 200, 100)
        previous = row("2026-07-21", 150, 100)
        snapshot = contract_snapshot(
            "EUR",
            [current, previous],
            "2026-08-05T04:00:00+00:00",
            prior={
                "report_date": "2026-07-28",
                "first_seen_utc": "2026-08-01T19:31:00+00:00",
            },
        )
        self.assertEqual(snapshot["first_seen_utc"], "2026-08-01T19:31:00+00:00")
        self.assertAlmostEqual(snapshot["leveraged_money_weekly_change"], 0.05)
        self.assertEqual(snapshot["directional_interpretation"], "research_context_only")

    def test_new_report_gets_new_observation_time(self):
        snapshot = contract_snapshot(
            "EUR",
            [row("2026-08-04", 200, 100), row("2026-07-28", 150, 100)],
            "2026-08-07T19:31:00+00:00",
            prior={
                "report_date": "2026-07-28",
                "first_seen_utc": "2026-08-01T19:31:00+00:00",
            },
        )
        self.assertEqual(snapshot["first_seen_utc"], "2026-08-07T19:31:00+00:00")

    def test_official_2026_release_schedule_maps_report_time_causally(self):
        ordinary = scheduled_release("2026-07-28", load_release_schedule())
        delayed = scheduled_release("2026-06-16", load_release_schedule())

        self.assertEqual(ordinary["scheduled_release_utc"], "2026-07-31T19:30:00+00:00")
        self.assertFalse(ordinary["holiday_delayed_release"])
        self.assertEqual(delayed["scheduled_release_utc"], "2026-06-22T19:30:00+00:00")
        self.assertTrue(delayed["holiday_delayed_release"])

    def test_governed_collector_bootstraps_then_marks_only_new_report_prospective(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            common = {
                "output": root / "state.json",
                "database": root / "evidence.sqlite",
                "report": root / "report.md",
            }
            initial = collect(
                **common,
                observed_utc="2026-08-14T12:00:00+00:00",
                acquired={
                    "EUR": [
                        row("2026-08-04T00:00:00.000", 200, 100),
                        row("2026-07-28T00:00:00.000", 150, 100),
                    ]
                },
            )
            self.assertEqual(initial["schema_version"], 2)
            self.assertEqual(initial["evidence"]["inserted_this_cycle"], 2)
            self.assertEqual(initial["evidence"]["prospective_observations"], 0)
            self.assertTrue(initial["currencies"]["EUR"]["bootstrap_current_view"])
            repeated = collect(
                **common,
                observed_utc="2026-08-14T13:00:00+00:00",
                acquired={
                    "EUR": [
                        row("2026-08-04T00:00:00.000", 200, 100),
                        row("2026-07-28T00:00:00.000", 150, 100),
                    ]
                },
            )
            self.assertEqual(repeated["evidence"]["inserted_this_cycle"], 0)
            prospective = collect(
                **common,
                observed_utc="2026-08-21T19:31:00+00:00",
                acquired={
                    "EUR": [
                        row("2026-08-11T00:00:00.000", 240, 100),
                        row("2026-08-04T00:00:00.000", 200, 100),
                    ]
                },
            )
            self.assertEqual(prospective["evidence"]["inserted_this_cycle"], 1)
            self.assertEqual(prospective["evidence"]["prospective_observations"], 1)
            self.assertTrue(prospective["currencies"]["EUR"]["prospective_eligible"])
            self.assertEqual(
                prospective["currencies"]["EUR"]["observation_kind"],
                "new_report_first_observed",
            )
            self.assertIn("Bootstrap values", common["report"].read_text(encoding="utf-8"))

    def test_same_report_revision_is_versioned_but_not_proof_eligible(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            common = {
                "output": root / "state.json",
                "database": root / "evidence.sqlite",
                "report": root / "report.md",
            }
            collect(
                **common,
                observed_utc="2026-08-14T12:00:00+00:00",
                acquired={"EUR": [row("2026-08-04T00:00:00.000", 200, 100)]},
            )
            revised = collect(
                **common,
                observed_utc="2026-08-14T13:00:00+00:00",
                acquired={"EUR": [row("2026-08-04T00:00:00.000", 201, 100)]},
            )
            latest = revised["currencies"]["EUR"]
            self.assertEqual(latest["observation_version"], 2)
            self.assertEqual(latest["observation_kind"], "same_report_revision_first_seen")
            self.assertFalse(latest["prospective_eligible"])
            db = sqlite3.connect(common["database"])
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute(
                    "UPDATE positioning_observations SET currency='USD' WHERE currency='EUR'"
                )
            db.close()

    def test_source_inventory_marks_cftc_runtime_as_governed_and_supported(self):
        config = json.loads(
            (Path(__file__).resolve().parent / "config" / "news_sources_v1.json").read_text(
                encoding="utf-8"
            )
        )
        source = next(
            item for item in config["sources"] if item.get("source_id") == "cftc_cot_positioning"
        )
        self.assertTrue(source["enabled"])
        self.assertTrue(source["runtime_supported"])
        self.assertTrue(source["externally_managed"])
        self.assertEqual(source["runtime_adapter"], "oanda_cftc_positioning_shadow.py")
        self.assertEqual(
            source["source_contract_id"], "cftc_tff_currency_positioning_v2_20260814"
        )


if __name__ == "__main__":
    unittest.main()
