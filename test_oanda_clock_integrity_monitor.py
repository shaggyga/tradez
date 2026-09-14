import json
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from trad import oanda_clock_integrity_monitor as monitor
from trad.oanda_clock_integrity_monitor import (
    append_history,
    apply_continuity_guard,
    atomic_json,
    build_state,
)


class ClockIntegrityMonitorTests(unittest.TestCase):
    def test_main_loop_retries_after_persistent_publication_oserror(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "clock.json"
            source = Path(directory) / "heartbeat.json"
            source.write_text('{"details":{"stream":{}}}', encoding="utf-8")
            argv = [
                "oanda_clock_integrity_monitor.py",
                "--source", str(source),
                "--output", str(output),
                "--interval-sec", "5",
                "--duration-sec", "5",
            ]
            with mock.patch.object(monitor.sys, "argv", argv), mock.patch.object(
                monitor, "build_state", return_value={"status": "degraded"}
            ) as build, mock.patch.object(
                monitor, "https_clock_sample", return_value={"status": "unavailable"}
            ), mock.patch.object(
                monitor,
                "atomic_json",
                side_effect=PermissionError(5, "destination locked"),
            ) as publish, mock.patch.object(
                monitor, "append_history"
            ) as history, mock.patch.object(
                monitor.time, "monotonic", side_effect=[0.0, 0.0, 0.0, 6.0]
            ), mock.patch.object(monitor.time, "sleep") as sleep:
                self.assertEqual(monitor.main(), 0)

            self.assertEqual(build.call_count, 2)
            self.assertEqual(publish.call_count, 2)
            self.assertEqual(history.call_count, 2)
            sleep.assert_called_once_with(5.0)

    def test_material_clock_offset_jump_is_quarantined_then_expires(self):
        observed = datetime(2026, 8, 25, 8, 0, tzinfo=timezone.utc)
        previous = {"broker_clock_lead_sec": 14400.2}
        current = {
            "status": "ok",
            "timestamp_normalization_trusted": True,
            "host_clock_synchronized": True,
            "broker_clock_lead_sec": 0.2,
            "reasons": [],
        }
        guarded = apply_continuity_guard(
            current, previous, observed_utc=observed, quarantine_sec=600.0
        )
        self.assertTrue(guarded["clock_discontinuity_detected"])
        self.assertTrue(guarded["clock_discontinuity_active"])
        self.assertEqual(guarded["clock_discontinuity_offset_jump_sec"], 14400.0)
        self.assertEqual(guarded["status"], "degraded")
        self.assertFalse(guarded["timestamp_normalization_trusted"])
        self.assertIn("clock_discontinuity_quarantine_active", guarded["reasons"])

        still_guarded = apply_continuity_guard(
            {**current, "broker_clock_lead_sec": 0.1},
            guarded,
            observed_utc=observed + timedelta(minutes=5),
            quarantine_sec=600.0,
        )
        self.assertFalse(still_guarded["clock_discontinuity_detected"])
        self.assertTrue(still_guarded["clock_discontinuity_active"])
        self.assertEqual(still_guarded["status"], "degraded")

        expired = apply_continuity_guard(
            {**current, "broker_clock_lead_sec": 0.1},
            still_guarded,
            observed_utc=observed + timedelta(minutes=11),
            quarantine_sec=600.0,
        )
        self.assertFalse(expired["clock_discontinuity_active"])
        self.assertEqual(expired["status"], "ok")
        self.assertTrue(expired["timestamp_normalization_trusted"])

    def test_clock_history_is_compact_append_only_and_inert(self):
        with tempfile.TemporaryDirectory() as directory:
            history = Path(directory) / "clock.jsonl"
            state = {
                "generated_utc": "2026-08-25T08:00:00+00:00",
                "status": "ok",
                "broker_clock_lead_sec": 0.2,
                "broker_clock_sample_count": 128,
                "external_https_clock": {"offset_sec": -0.1},
                "clock_sources_consistent": True,
                "clock_discontinuity_detected": False,
                "clock_discontinuity_active": False,
                "reasons": [],
            }
            append_history(history, state)
            append_history(history, {**state, "broker_clock_lead_sec": 0.1})
            rows = [json.loads(line) for line in history.read_text().splitlines()]
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["broker_clock_lead_sec"], 0.2)
            self.assertEqual(rows[1]["broker_clock_lead_sec"], 0.1)
            self.assertTrue(all(row["research_only"] for row in rows))
            self.assertTrue(all(not row["can_place_orders"] for row in rows))

    def test_atomic_json_retries_transient_windows_destination_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "clock.json"
            output.write_text('{"generation":"old"}', encoding="utf-8")
            real_replace = __import__("os").replace
            attempts = 0

            def transient_lock(source, destination):
                nonlocal attempts
                attempts += 1
                if attempts < 3:
                    raise PermissionError(5, "destination locked")
                return real_replace(source, destination)

            with mock.patch(
                "trad.oanda_clock_integrity_monitor.os.replace",
                side_effect=transient_lock,
            ), mock.patch("trad.oanda_clock_integrity_monitor.time.sleep") as sleep:
                atomic_json(
                    output,
                    {"generation": "new"},
                    replace_attempts=3,
                    retry_delay_sec=0.01,
                )

            self.assertEqual(
                json.loads(output.read_text(encoding="utf-8")),
                {"generation": "new"},
            )
            self.assertEqual(attempts, 3)
            self.assertEqual(sleep.call_count, 2)
            self.assertEqual(list(output.parent.glob(f".{output.name}.*.tmp")), [])

    def test_atomic_json_persistent_lock_preserves_old_state_and_cleans_temp(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "clock.json"
            output.write_text('{"generation":"old"}', encoding="utf-8")
            with mock.patch(
                "trad.oanda_clock_integrity_monitor.os.replace",
                side_effect=PermissionError(5, "destination locked"),
            ):
                with self.assertRaises(PermissionError):
                    atomic_json(
                        output,
                        {"generation": "new"},
                        replace_attempts=2,
                        retry_delay_sec=0.0,
                    )
            self.assertEqual(
                json.loads(output.read_text(encoding="utf-8")),
                {"generation": "old"},
            )
            self.assertEqual(list(output.parent.glob(f".{output.name}.*.tmp")), [])

    def test_atomic_json_concurrent_publishers_do_not_share_temp_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "clock.json"
            failures: list[BaseException] = []

            def publish(writer: int) -> None:
                try:
                    for sequence in range(40):
                        atomic_json(
                            output,
                            {"writer": writer, "sequence": sequence},
                            replace_attempts=100,
                            retry_delay_sec=0.001,
                        )
                except BaseException as exc:  # captured for the parent test
                    failures.append(exc)

            threads = [threading.Thread(target=publish, args=(writer,)) for writer in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            self.assertEqual(failures, [])
            final = json.loads(output.read_text(encoding="utf-8"))
            self.assertIn(final["writer"], {0, 1})
            self.assertEqual(final["sequence"], 39)
            self.assertEqual(list(output.parent.glob(f".{output.name}.*.tmp")), [])

    def test_large_but_fresh_broker_offset_is_mitigated_not_synchronized(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "heartbeat.json"
            source.write_text(
                json.dumps(
                    {
                        "details": {
                            "stream": {
                                "broker_clock_lead_sec": 60.9,
                                "broker_clock_sample_count": 128,
                                "clock_sync_status": "local_clock_behind_broker",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            state = build_state(
                source,
                service={"state": "stopped", "query_ok": True, "error": ""},
            )
            self.assertEqual(state["status"], "mitigated")
            self.assertTrue(state["timestamp_normalization_trusted"])
            self.assertFalse(state["host_clock_synchronized"])
            self.assertIn("windows_time_service_not_running", state["reasons"])
            self.assertFalse(state["can_place_orders"])

    def test_small_offset_with_running_service_is_ok(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "heartbeat.json"
            source.write_text(
                json.dumps(
                    {
                        "details": {
                            "stream": {
                                "broker_clock_lead_sec": 0.25,
                                "broker_clock_sample_count": 128,
                                "clock_sync_status": "synchronized",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            state = build_state(
                source,
                service={"state": "running", "query_ok": True, "error": ""},
            )
            self.assertEqual(state["status"], "ok")
            self.assertTrue(state["host_clock_synchronized"])

    def test_https_clock_can_mitigate_stopped_windows_service(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "heartbeat.json"
            source.write_text('{"details":{"stream":{}}}', encoding="utf-8")
            state = build_state(
                source,
                service={"state": "stopped", "query_ok": True, "error": ""},
                external_clock={"status": "ok", "offset_sec": 0.4, "round_trip_ms": 100.0},
            )
            self.assertEqual(state["status"], "mitigated")
            self.assertTrue(state["timestamp_normalization_trusted"])

    def test_independent_https_clock_accepts_host_when_stream_timestamp_lags(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "heartbeat.json"
            source.write_text(json.dumps({"details": {"stream": {
                "broker_clock_lead_sec": -20.0,
                "broker_clock_sample_count": 128,
            }}}), encoding="utf-8")
            state = build_state(
                source,
                service={"state": "running", "query_ok": True, "error": ""},
                external_clock={"status": "ok", "offset_sec": 0.4, "round_trip_ms": 100.0},
            )
            self.assertEqual(state["status"], "mitigated")
            self.assertTrue(state["timestamp_normalization_trusted"])
            self.assertTrue(state["host_clock_synchronized"])
            self.assertTrue(state["independent_https_host_clock_trusted"])
            self.assertFalse(state["clock_sources_consistent"])
            self.assertIn("clock_sources_disagree", state["reasons"])

    def test_stream_only_offset_jump_does_not_quarantine_independently_attested_host(self):
        observed = datetime(2026, 9, 14, 15, 0, tzinfo=timezone.utc)
        external = {"status": "ok", "offset_sec": 0.2, "round_trip_ms": 100.0}
        prior = {"broker_clock_lead_sec": 0.1, "external_https_clock": external}
        current = {"broker_clock_lead_sec": -20.0, "external_https_clock": external}
        guarded = apply_continuity_guard(current, prior, observed_utc=observed)
        self.assertFalse(guarded["clock_discontinuity_detected"])
        self.assertFalse(guarded["clock_discontinuity_active"])

    def test_large_https_offset_remains_degraded(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "heartbeat.json"
            source.write_text('{"details":{"stream":{}}}', encoding="utf-8")
            state = build_state(
                source,
                service={"state": "stopped", "query_ok": True, "error": ""},
                external_clock={"status": "ok", "offset_sec": 60.0, "round_trip_ms": 100.0},
            )
            self.assertEqual(state["status"], "degraded")
            self.assertIn("external_clock_offset_exceeds_2s", state["reasons"])

    def test_boolean_or_malformed_broker_clock_values_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "heartbeat.json"
            for lead, samples in ((True, 128), (0.1, "nan"), (0.1, True)):
                source.write_text(
                    json.dumps(
                        {
                            "details": {
                                "stream": {
                                    "broker_clock_lead_sec": lead,
                                    "broker_clock_sample_count": samples,
                                }
                            }
                        }
                    ),
                    encoding="utf-8",
                )
                state = build_state(
                    source,
                    service={"state": "running", "query_ok": True, "error": ""},
                )
                self.assertFalse(state["timestamp_normalization_trusted"])
                self.assertFalse(state["host_clock_synchronized"])

    def test_non_mapping_nested_clock_payloads_fail_closed_without_crash(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "heartbeat.json"
            for details, external in (
                (123, []),
                (["not", "a", "mapping"], "also-not-a-mapping"),
                ({"stream": 42}, object()),
            ):
                source.write_text(
                    json.dumps({"details": details}), encoding="utf-8"
                )
                state = build_state(
                    source,
                    service="not-a-mapping",
                    external_clock=external,
                )
                self.assertEqual(state["status"], "degraded")
                self.assertFalse(state["timestamp_normalization_trusted"])
                self.assertFalse(state["host_clock_synchronized"])

    def test_conflicting_fresh_broker_and_https_sources_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "heartbeat.json"
            source.write_text(
                json.dumps(
                    {
                        "details": {
                            "stream": {
                                "broker_clock_lead_sec": 60.0,
                                "broker_clock_sample_count": 128,
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            state = build_state(
                source,
                service={"state": "stopped", "query_ok": True, "error": ""},
                external_clock={
                    "status": "ok",
                    "offset_sec": 290.0,
                    "round_trip_ms": 100.0,
                },
            )
            self.assertEqual(state["status"], "degraded")
            self.assertFalse(state["clock_sources_consistent"])
            self.assertEqual(state["clock_source_disagreement_sec"], 230.0)
            self.assertIn("clock_sources_disagree", state["reasons"])


if __name__ == "__main__":
    unittest.main()
