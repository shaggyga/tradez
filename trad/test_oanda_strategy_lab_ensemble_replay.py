import json
import tempfile
import unittest
from pathlib import Path

from trad.oanda_strategy_lab_ensemble_replay import (
    EnsembleSpec,
    SourceAccumulator,
    attach_outcomes,
    build_report,
    candidate_for_group,
    output_paths,
)


CYCLE = "20260714T020441753848"
INSTRUMENT = "EUR_USD"


def setup_row(
    family: str,
    profile: str,
    direction: str = "buy",
    *,
    event: str = "shadow_signal",
    miss_class: str | None = None,
) -> dict[str, object]:
    lane_id = f"{family}.{profile}"
    row: dict[str, object] = {
        "event": event,
        "id": f"{CYCLE}-{lane_id}-{INSTRUMENT}",
        "lane_id": lane_id,
        "family": family,
        "profile": profile,
        "instrument": INSTRUMENT,
        "direction": direction,
        "time": "2026-07-14T02:04:41.750000+00:00",
        "signal_candle_time": "2026-07-14T02:03:00Z",
        "entry_time": "2026-07-14T02:04:41Z",
        "entry_bid": 1.1000,
        "entry_ask": 1.1002,
    }
    if miss_class is not None:
        row["miss_class"] = miss_class
    return row


class EnsembleVoteTests(unittest.TestCase):
    def test_four_profiles_from_one_family_are_one_ensemble_voter(self):
        rows = [setup_row("momentum", profile) for profile in ("strict", "balanced", "fast", "loose")]
        spec = EnsembleSpec("two_families", False, "family", 2, 1, 1.0)
        self.assertIsNone(candidate_for_group(spec, (CYCLE, INSTRUMENT), rows))

        rows.append(setup_row("donchian_breakout", "balanced"))
        candidate = candidate_for_group(spec, (CYCLE, INSTRUMENT), rows)
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate["voter_count"], 2)
        self.assertEqual(candidate["family_count"], 2)
        self.assertEqual(candidate["direction"], "buy")

    def test_accepted_only_excludes_near_threshold_rows(self):
        rows = [
            setup_row("momentum", "balanced"),
            setup_row(
                "donchian_breakout",
                "balanced",
                event="shadow_miss",
                miss_class="near_threshold",
            ),
        ]
        accepted = EnsembleSpec("accepted", False, "family", 2, 1, 1.0)
        with_near = EnsembleSpec("with_near", True, "family", 2, 1, 1.0)
        self.assertIsNone(candidate_for_group(accepted, (CYCLE, INSTRUMENT), rows))
        self.assertIsNotNone(candidate_for_group(with_near, (CYCLE, INSTRUMENT), rows))

    def test_conflicting_family_votes_fail_unanimous_ensemble(self):
        rows = [
            setup_row("momentum", "balanced", "buy"),
            setup_row("donchian_breakout", "balanced", "sell"),
        ]
        spec = EnsembleSpec("unanimous", False, "family", 2, 1, 1.0)
        self.assertIsNone(candidate_for_group(spec, (CYCLE, INSTRUMENT), rows))

    def test_cluster_vote_requires_distinct_strategy_clusters(self):
        same_cluster = [
            setup_row("momentum", "balanced"),
            setup_row("pullback", "balanced"),
            setup_row("ema_trend_cross", "balanced"),
        ]
        spec = EnsembleSpec("three_clusters", False, "cluster", 3, 3, 1.0)
        self.assertIsNone(candidate_for_group(spec, (CYCLE, INSTRUMENT), same_cluster))

        diversified = same_cluster[:1] + [
            setup_row("donchian_breakout", "balanced"),
            setup_row("bollinger_reversion", "balanced"),
        ]
        candidate = candidate_for_group(spec, (CYCLE, INSTRUMENT), diversified)
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate["cluster_count"], 3)


class EnsembleOutcomeTests(unittest.TestCase):
    def test_outcomes_attach_after_decision_and_use_constituent_median(self):
        rows = [
            setup_row("momentum", "balanced"),
            setup_row("donchian_breakout", "balanced"),
        ]
        spec = EnsembleSpec("accepted", False, "family", 2, 1, 1.0)
        candidate = candidate_for_group(spec, (CYCLE, INSTRUMENT), rows)
        assert candidate is not None
        source_outcomes = {
            (rows[0]["id"], 60): 1.0,
            (rows[1]["id"], 60): 3.0,
        }
        outcomes = attach_outcomes([candidate], source_outcomes, [60, 180])
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(outcomes[0]["theoretical_pips"], 2.0)
        self.assertEqual(outcomes[0]["constituent_outcome_count"], 2)
        self.assertEqual(outcomes[0]["constituent_dispersion_pips"], 2.0)

    def test_future_outcomes_cannot_change_ensemble_candidates(self):
        rows = [
            {"event": "lab_start", "outcome_horizons": [60], "lane_count": 2, "instrument_count": 1},
            setup_row("momentum", "balanced"),
            setup_row("donchian_breakout", "balanced"),
        ]
        spec = EnsembleSpec("accepted", False, "family", 2, 1, 1.0)
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "lab.jsonl"
            source.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
            first_report, first_events = build_report(source, [spec])

            outcome_rows = [
                {
                    "event": "shadow_outcome",
                    "id": row["id"],
                    "horizon_sec": 60,
                    "theoretical_pips": value,
                }
                for row, value in zip(rows[1:], (999.0, -999.0))
            ]
            source.write_text(
                "".join(json.dumps(row) + "\n" for row in rows + outcome_rows),
                encoding="utf-8",
            )
            second_report, second_events = build_report(source, [spec])

        first_signals = [row for row in first_events if row["event"] == "ensemble_signal"]
        second_signals = [row for row in second_events if row["event"] == "ensemble_signal"]
        self.assertEqual(first_signals, second_signals)
        self.assertEqual(first_report["candidate_count"], second_report["candidate_count"])
        self.assertEqual(first_report["outcome_count"], 0)
        self.assertEqual(second_report["outcome_count"], 1)

    def test_default_output_names_do_not_match_raw_lab_glob(self):
        source = Path("practice_strategy_lab_account_timestamp.jsonl")
        summary, events = output_paths(source, None, None)
        self.assertTrue(summary.name.startswith("strategy_ensemble_"))
        self.assertTrue(events.name.startswith("strategy_ensemble_"))
        self.assertFalse(events.name.startswith("practice_strategy_lab_"))

    def test_incremental_reader_processes_appends_once_and_waits_for_newline(self):
        start = {"event": "lab_start", "outcome_horizons": [60]}
        signal = setup_row("momentum", "balanced")
        outcome = {
            "event": "shadow_outcome",
            "id": signal["id"],
            "horizon_sec": 60,
            "theoretical_pips": 1.5,
        }
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "lab.jsonl"
            source.write_bytes((json.dumps(start) + "\n" + json.dumps(signal) + "\n").encode())
            accumulator = SourceAccumulator(source)
            self.assertEqual(accumulator.read_available(), 2)
            self.assertEqual(accumulator.read_available(), 0)
            self.assertEqual(sum(map(len, accumulator.groups.values())), 1)

            with source.open("ab") as handle:
                handle.write(json.dumps(outcome).encode())
            self.assertEqual(accumulator.read_available(), 0)
            self.assertEqual(accumulator.outcomes, {})

            with source.open("ab") as handle:
                handle.write(b"\n")
            self.assertEqual(accumulator.read_available(), 1)
            self.assertEqual(accumulator.outcomes[(str(signal["id"]), 60)], 1.5)


if __name__ == "__main__":
    unittest.main()
