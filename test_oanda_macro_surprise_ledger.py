import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from trad.oanda_macro_surprise_ledger import INGEST_CONTRACT_ID, ingest, release_key


class MacroSurpriseLedgerTests(unittest.TestCase):
    def test_prefilter_preserves_structured_rows_without_loading_unrelated_payloads(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, ledger = root / "news.sqlite", root / "macro.sqlite"
            connection = sqlite3.connect(source)
            connection.execute(
                "CREATE TABLE articles(event_id TEXT,first_seen_utc TEXT,payload_json TEXT)"
            )
            connection.executemany(
                "INSERT INTO articles VALUES(?,?,?)",
                [
                    (
                        "release",
                        "2026-08-28T12:00:01Z",
                        json.dumps(
                            {
                                "structured_event": True,
                                "event_series_id": "TEST_CPI",
                                "actual_value": 2.1,
                                "first_seen_utc": "2026-08-28T12:00:01Z",
                            }
                        ),
                    ),
                    (
                        "narrative",
                        "2026-08-28T12:00:02Z",
                        json.dumps({"headline": "unrelated", "blob": "x" * 100_000}),
                    ),
                ],
            )
            connection.commit()
            connection.close()

            result = ingest(source, ledger)

            self.assertEqual(result["articles_inspected"], 2)
            self.assertEqual(result["candidate_articles_loaded"], 1)
            self.assertEqual(result["structured_articles_seen"], 1)

    def test_unclocked_state_poll_reuses_one_identity(self):
        base = {
            "structured_event": True,
            "context_only": True,
            "published_time_inferred": True,
            "source_id": "official-policy-state",
            "source_url": "https://example.test/policy-rate",
            "event_series_id": "official_policy_rate",
            "actual_value": 7.0,
        }
        first = {**base, "first_seen_utc": "2026-08-20T09:00:00Z", "reference_period": "2026-08-20"}
        second = {**base, "first_seen_utc": "2026-08-21T09:00:00Z", "reference_period": "2026-08-21"}
        self.assertEqual(release_key("poll-a", first), release_key("poll-b", second))

    def test_unclocked_state_polls_ingest_as_revisions_not_releases(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, ledger = root / "news.sqlite", root / "macro.sqlite"
            connection = sqlite3.connect(source)
            connection.execute(
                "CREATE TABLE articles(event_id TEXT,first_seen_utc TEXT,payload_json TEXT)"
            )
            for index, date in enumerate(("2026-08-20", "2026-08-21"), start=1):
                payload = {
                    "structured_event": True,
                    "context_only": True,
                    "published_time_inferred": True,
                    "source_verified": True,
                    "source_direct": True,
                    "source_id": "official-policy-state",
                    "source_url": "https://example.test/policy-rate",
                    "event_series_id": "official_policy_rate",
                    "event_name": "Policy rate state",
                    "first_seen_utc": f"{date}T09:00:00Z",
                    "reference_period": date,
                    "actual_value": 7.0,
                }
                connection.execute(
                    "INSERT INTO articles VALUES(?,?,?)",
                    (f"poll-{index}", payload["first_seen_utc"], json.dumps(payload)),
                )
            connection.commit()
            connection.close()
            result = ingest(source, ledger)
            self.assertEqual(result["release_count"], 1)
            self.assertEqual(result["total_revisions"], 2)

    def test_exact_scheduled_releases_keep_distinct_identities(self):
        base = {
            "structured_event": True,
            "context_only": True,
            "source_id": "official-calendar",
            "event_series_id": "official_cpi",
            "first_seen_utc": "2026-08-20T09:00:02Z",
            "actual_value": 2.0,
        }
        first = {**base, "scheduled_utc": "2026-08-20T09:00:00Z", "reference_period": "July"}
        second = {**base, "scheduled_utc": "2026-09-20T09:00:00Z", "reference_period": "August"}
        self.assertNotEqual(release_key("release-a", first), release_key("release-b", second))

    def test_causal_consensus_is_joined_and_post_release_value_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, ledger = root / "news.sqlite", root / "macro.sqlite"
            connection = sqlite3.connect(source)
            connection.execute(
                "CREATE TABLE articles(event_id TEXT,first_seen_utc TEXT,payload_json TEXT)"
            )
            payload = {
                "structured_event": True,
                "source_listing_bootstrap": True,
                "source_verified": True,
                "event_series_id": "US_CPI",
                "event_name": "CPI",
                "scheduled_utc": "2026-01-02T13:30:00+00:00",
                "first_seen_utc": "2026-01-02T13:30:01+00:00",
                "actual_value": 3.0,
            }
            connection.execute(
                "INSERT INTO articles VALUES(?,?,?)",
                ("event-1", payload["first_seen_utc"], json.dumps(payload)),
            )
            connection.commit()
            connection.close()
            consensus = root / "consensus.jsonl"
            consensus.write_text(
                "\n".join(
                    json.dumps(row)
                    for row in (
                        {
                            "event_series_id": "US_CPI",
                            "scheduled_utc": payload["scheduled_utc"],
                            "captured_utc": "2026-01-02T13:00:00+00:00",
                            "source_timestamp_utc": "2026-01-02T12:59:00+00:00",
                            "consensus_value": 2.8,
                            "source_verified": True,
                            "source_id": "verified-calendar",
                        },
                        {
                            "event_series_id": "US_CPI",
                            "scheduled_utc": payload["scheduled_utc"],
                            "captured_utc": "2026-01-02T13:31:00+00:00",
                            "consensus_value": 9.9,
                            "source_verified": True,
                            "source_id": "late",
                        },
                    )
                ),
                encoding="utf-8",
            )
            result = ingest(source, ledger, consensus_jsonl=consensus)
            self.assertEqual(result["actual_and_consensus_count"], 1)
            self.assertEqual(result["causal_actual_and_consensus_count"], 1)
            self.assertEqual(result["noncausal_actual_and_consensus_count"], 0)
            self.assertEqual(result["causal_consensus_observation_count"], 1)
            self.assertEqual(result["status"], "ready")
            check = sqlite3.connect(ledger)
            surprise, known = check.execute(
                "SELECT surprise_raw,known_before_recorded_timestamp FROM macro_release_latest"
            ).fetchone()
            self.assertAlmostEqual(surprise, 0.2)
            self.assertEqual(known, 1)
            stored = json.loads(
                check.execute("SELECT payload_json FROM macro_release_latest").fetchone()[0]
            )
            self.assertTrue(stored["source_listing_bootstrap"])
            self.assertEqual(stored["macro_ingest_contract_id"], INGEST_CONTRACT_ID)
            check.close()

    def test_revision_is_immutable_and_consensus_absence_is_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "news.sqlite"
            ledger = root / "macro.sqlite"
            connection = sqlite3.connect(source)
            connection.execute(
                "CREATE TABLE articles(event_id TEXT,first_seen_utc TEXT,payload_json TEXT)"
            )
            payload = {
                "structured_event": True,
                "source_id": "official",
                "source_name": "Official Statistics",
                "source_verified": True,
                "source_direct": True,
                "first_seen_utc": "2026-01-02T13:30:01+00:00",
                "scheduled_utc": "2026-01-02T13:30:00+00:00",
                "event_series_id": "CPI",
                "event_name": "CPI",
                "event_country": "US",
                "actual": "3.0%",
                "actual_value": 3.0,
                "previous": "2.9%",
                "previous_value": 2.9,
            }
            connection.execute(
                "INSERT INTO articles VALUES(?,?,?)",
                ("event-1", payload["first_seen_utc"], json.dumps(payload)),
            )
            connection.commit()
            connection.close()
            result = ingest(source, ledger)
            self.assertEqual(result["release_count"], 1)
            self.assertEqual(result["actual_count"], 1)
            self.assertEqual(result["consensus_count"], 0)
            self.assertEqual(result["status"], "causal_consensus_source_unavailable")
            result_again = ingest(source, ledger)
            self.assertEqual(result_again["new_revisions"], 0)
            check = sqlite3.connect(ledger)
            with self.assertRaises(sqlite3.IntegrityError):
                check.execute("UPDATE macro_release_revisions SET actual_value=4")
            check.close()

    def test_inferred_publication_clock_is_preserved_in_immutable_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, ledger = root / "news.sqlite", root / "macro.sqlite"
            connection = sqlite3.connect(source)
            connection.execute(
                "CREATE TABLE articles(event_id TEXT,first_seen_utc TEXT,payload_json TEXT)"
            )
            payload = {
                "structured_event": True,
                "source_id": "official_snapshot",
                "source_verified": True,
                "source_direct": True,
                "first_seen_utc": "2026-08-16T20:00:00+00:00",
                "causal_known_utc": "2026-08-16T20:00:00+00:00",
                "published_time_inferred": True,
                "event_series_id": "official_policy_rate",
                "event_name": "Policy rate snapshot",
                "currencies": ["JPY"],
                "actual_value": 0.5,
            }
            connection.execute(
                "INSERT INTO articles VALUES(?,?,?)",
                ("snapshot-1", payload["first_seen_utc"], json.dumps(payload)),
            )
            connection.commit()
            connection.close()
            ingest(source, ledger)
            check = sqlite3.connect(ledger)
            stored = json.loads(
                check.execute("SELECT payload_json FROM macro_release_latest").fetchone()[0]
            )
            check.close()
            self.assertTrue(stored["published_time_inferred"])
            self.assertEqual(
                stored["knowledge_time_provenance_contract_id"],
                "macro_publication_clock_provenance_v1_20260816",
            )

    def test_exact_source_native_publication_clock_is_preserved_as_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, ledger = root / "news.sqlite", root / "macro.sqlite"
            connection = sqlite3.connect(source)
            connection.execute(
                "CREATE TABLE articles(event_id TEXT,first_seen_utc TEXT,payload_json TEXT)"
            )
            payload = {
                "structured_event": True,
                "source_verified": True,
                "source_direct": True,
                "first_seen_utc": "2026-08-16T19:12:00+00:00",
                "numeric_causal_known_utc": "2026-08-16T19:12:00+00:00",
                "published_utc": "2026-07-23T23:30:00+00:00",
                "published_time_inferred": False,
                "event_series_id": "japan_cpi_national_yoy",
                "event_name": "Japan CPI",
                "currencies": ["JPY"],
                "actual_value": 1.7,
            }
            connection.execute(
                "INSERT INTO articles VALUES(?,?,?)",
                ("japan-cpi", payload["first_seen_utc"], json.dumps(payload)),
            )
            connection.commit()
            connection.close()
            ingest(source, ledger)
            check = sqlite3.connect(ledger)
            stored = json.loads(
                check.execute("SELECT payload_json FROM macro_release_latest").fetchone()[0]
            )
            check.close()
            self.assertEqual(
                stored["source_native_published_utc"], payload["published_utc"]
            )

    def test_numeric_parser_activation_is_the_structured_knowledge_time(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, ledger = root / "news.sqlite", root / "macro.sqlite"
            connection = sqlite3.connect(source)
            connection.execute(
                "CREATE TABLE articles(event_id TEXT,first_seen_utc TEXT,payload_json TEXT)"
            )
            payload = {
                "structured_event": True,
                "source_id": "statcan_daily_releases",
                "source_name": "Statistics Canada",
                "source_verified": True,
                "source_direct": True,
                "first_seen_utc": "2026-08-14T12:32:47+00:00",
                "causal_known_utc": "2026-08-14T12:32:47+00:00",
                "numeric_causal_known_utc": "2026-08-14T12:43:00+00:00",
                "scheduled_utc": "2026-08-14T12:30:00+00:00",
                "event_series_id": "statcan_wholesale_sales_mom",
                "event_name": "Wholesale sales monthly change",
                "event_country": "Canada",
                "currencies": ["CAD"],
                "actual_value": 2.8,
            }
            connection.execute(
                "INSERT INTO articles VALUES(?,?,?)",
                ("statcan-1", payload["first_seen_utc"], json.dumps(payload)),
            )
            connection.commit()
            connection.close()
            ingest(source, ledger)
            check = sqlite3.connect(ledger)
            known = check.execute(
                "SELECT causal_known_utc FROM macro_release_latest"
            ).fetchone()[0]
            check.close()
            self.assertEqual(known, payload["numeric_causal_known_utc"])

    def test_inline_consensus_never_self_certifies_pre_release_availability(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, ledger = root / "news.sqlite", root / "macro.sqlite"
            connection = sqlite3.connect(source)
            connection.execute(
                "CREATE TABLE articles(event_id TEXT,first_seen_utc TEXT,payload_json TEXT)"
            )
            payload = {
                "structured_event": True,
                "source_id": "official-release",
                "source_verified": True,
                "source_direct": True,
                "event_series_id": "US_CPI",
                "event_name": "CPI",
                "scheduled_utc": "2026-01-02T13:30:00+00:00",
                "first_seen_utc": "2026-01-02T13:30:01+00:00",
                "actual_value": 3.0,
                "consensus_value": 2.8,
                "consensus": "2.8%",
            }
            connection.execute(
                "INSERT INTO articles VALUES(?,?,?)",
                ("event-inline", payload["first_seen_utc"], json.dumps(payload)),
            )
            connection.commit()
            connection.close()

            result = ingest(source, ledger)
            self.assertEqual(result["actual_and_consensus_count"], 1)
            self.assertEqual(result["causal_actual_and_consensus_count"], 0)
            self.assertEqual(result["noncausal_actual_and_consensus_count"], 1)
            self.assertEqual(
                result["status"], "causal_consensus_source_unavailable"
            )
            check = sqlite3.connect(ledger)
            known, standardized, stored_payload = check.execute(
                "SELECT known_before_recorded_timestamp,standardized_surprise,payload_json "
                "FROM macro_release_latest"
            ).fetchone()
            check.close()
            self.assertEqual(known, 0)
            self.assertIsNone(standardized)
            self.assertFalse(
                json.loads(stored_payload)["consensus_causally_verified"]
            )


if __name__ == "__main__":
    unittest.main()
