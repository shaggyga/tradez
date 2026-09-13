import argparse
import copy
import csv
import datetime as dt
import io
import hashlib
import json
from pathlib import Path

import pytest

import oanda_all68_m1_forward_updater as updater


def test_recent_archive_uses_gap_sized_request_not_full_5000():
    reference = dt.datetime(2026, 8, 28, 12, 0, tzinfo=dt.timezone.utc)
    assert updater.forward_request_count(
        updater.pd.Timestamp("2026-08-28T11:54:00Z"),
        batch_size=5000,
        reference_time=reference,
    ) == 10
    assert updater.forward_request_count(
        updater.pd.Timestamp("2026-08-28T10:00:00Z"),
        batch_size=5000,
        reference_time=reference,
    ) == 123


def test_missing_or_very_stale_archive_keeps_recovery_batch():
    reference = dt.datetime(2026, 8, 28, 12, 0, tzinfo=dt.timezone.utc)
    assert updater.forward_request_count(
        None, batch_size=5000, reference_time=reference
    ) == 5000
    assert updater.forward_request_count(
        updater.pd.Timestamp("2026-08-01T00:00:00Z"),
        batch_size=5000,
        reference_time=reference,
    ) == 5000


def test_atomic_report_replaces_complete_json(tmp_path: Path):
    path = tmp_path / "state" / "report.json"
    updater.write_json_atomic(path, {"schema": 1, "rows": [1, 2, 3]})
    assert json.loads(path.read_text(encoding="utf-8"))["rows"] == [1, 2, 3]
    assert not list(path.parent.glob("*.tmp"))


def test_priced_instruments_uses_only_pair_shaped_quotes(tmp_path: Path):
    path = tmp_path / "quotes.json"
    path.write_text(
        json.dumps(
            {
                "quotes": {
                    "EUR_USD": {"bid": 1.1},
                    "USD_JPY": {"bid": 150.0},
                    "INVALID": {"bid": 1.0},
                    "GBP_USD": None,
                }
            }
        ),
        encoding="utf-8",
    )
    assert updater.priced_instruments(path) == ["EUR_USD", "USD_JPY"]


def test_run_once_writes_health_report_without_account_or_order_surface(
    tmp_path: Path, monkeypatch
):
    candle = tmp_path / "EUR_USD_M1.csv"
    candle.write_text("time,datetime\n", encoding="utf-8")
    report = tmp_path / "report.json"
    monkeypatch.setattr(updater, "candle_files", lambda: [candle])
    monkeypatch.setattr(
        updater,
        "resolve_readonly_oanda_client",
        lambda: (object(), {"environment": "practice", "base_url": "https://api-fxpractice.oanda.com"}),
    )
    monkeypatch.setattr(
        updater,
        "update_pair",
        lambda *args, **kwargs: {
            "instrument": "EUR_USD",
            "rows_appended": 2,
            "rows_backfilled": 0,
            "error": "",
        },
    )
    args = argparse.Namespace(
        pairs=[],
        bootstrap_all_priced=False,
        max_requests_per_pair=1,
        backfill_requests_per_pair=0,
        batch_size=5000,
        pause_seconds=0.0,
        dry_run=False,
        report=report,
        interval_sec=0.0,
        duration_sec=0.0,
    )
    assert updater.run_once(args) == 0
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["source"]["environment"] == "practice"
    assert payload["total_rows_appended"] == 2
    assert payload["error_count"] == 0
    assert payload["gap_recovery_cycle"] == {
        "cycle_index": 1,
        "every_cycles": 1,
        "enabled_this_cycle": True,
    }


def test_run_once_defers_gap_recovery_until_its_scheduled_cycle(tmp_path: Path, monkeypatch):
    candle = tmp_path / "EUR_USD_M1.csv"
    candle.write_text("time,datetime\n", encoding="utf-8")
    report = tmp_path / "report.json"
    calls = []
    monkeypatch.setattr(updater, "candle_files", lambda: [candle])
    monkeypatch.setattr(updater, "resolve_readonly_oanda_client", lambda: (object(), {}))
    monkeypatch.setattr(updater, "update_pair", lambda *args, **kwargs: calls.append(kwargs) or {
        "instrument": "EUR_USD", "rows_appended": 0, "rows_backfilled": 0, "error": "",
        "gap_recovery": {"status": "disabled"}, "rows_recovered": 0,
    })
    args = argparse.Namespace(pairs=[], bootstrap_all_priced=False, max_requests_per_pair=1,
        backfill_requests_per_pair=0, batch_size=10, pause_seconds=0.0, dry_run=False,
        report=report, interval_sec=0.0, duration_sec=0.0, gap_recovery_every_cycles=10)
    assert updater.run_once(args, cycle_index=1) == 0
    assert calls[0]["recover_gaps"] is False
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["gap_recovery_cycle"]["enabled_this_cycle"] is False
    assert updater.run_once(args, cycle_index=10) == 0
    assert calls[1]["recover_gaps"] is True


COLUMNS = ["time", "datetime", "instrument", "granularity", "open", "high", "low", "close",
           "bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low",
           "ask_close", "spread_pips", "volume"]
BASE = dt.datetime(2026, 9, 7, 10, 0, tzinfo=dt.timezone.utc)


@pytest.fixture(autouse=True)
def no_runtime_client(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("runtime manager/client must not be loaded in fixture tests")
    monkeypatch.setattr(updater, "manager_module", forbidden)


def fixture_archive(tmp_path, minutes=(0, 2, 3), newline="\n"):
    path = tmp_path / "EUR_USD_M1.csv"
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator=newline)
    writer.writerow(COLUMNS)
    for minute in minutes:
        stamp = (BASE + dt.timedelta(minutes=minute)).isoformat()
        # Intentional numeric formatting must survive an insertion byte-for-byte.
        writer.writerow([stamp, stamp, "EUR_USD", "M1"] + ["1.100000"] * 12 + ["0.000", "012"])
    path.write_bytes(stream.getvalue().encode())
    return path


def broker_payload(minute=1):
    return {"instrument": "EUR_USD", "granularity": "M1", "candles": [{
        "time": (BASE + dt.timedelta(minutes=minute)).isoformat().replace("+00:00", ".000000000Z"),
        "complete": True, "volume": 7,
        "mid": {"o": "1.10005", "h": "1.10015", "l": "1.09995", "c": "1.10005"},
        "bid": {"o": "1.10000", "h": "1.10010", "l": "1.09990", "c": "1.10000"},
        "ask": {"o": "1.10010", "h": "1.10020", "l": "1.10000", "c": "1.10010"},
    }]}


class FixtureClient:
    def __init__(self, payload, callback=None):
        self.payload, self.callback, self.calls = payload, callback, []

    def candles(self, instrument, **kwargs):
        self.calls.append((instrument, kwargs))
        if self.callback:
            self.callback()
        return copy.deepcopy(self.payload)


def fixed_clock(monkeypatch, *values):
    if not values:
        values = (BASE + dt.timedelta(hours=2),)
    times = iter(values)
    latest = values[-1]
    monkeypatch.setattr(updater, "utc_now", lambda: next(times, latest))


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_recovery_inserts_only_actual_row_preserving_existing_bytes_and_observation_clocks(tmp_path, monkeypatch, newline):
    path = fixture_archive(tmp_path, newline=newline)
    original = path.read_bytes()
    now = BASE + dt.timedelta(hours=2)
    fixed_clock(monkeypatch, now, now + dt.timedelta(seconds=2), now + dt.timedelta(seconds=3))
    client = FixtureClient(broker_payload())
    result = updater.recover_recent_gaps(client, path)
    assert result["status"] == "recovered_actual_broker_minute", result["error"]
    assert result["rows_recovered"] == 1 and result["unresolved_minutes"] == 0
    assert result["historical_availability_asserted"] is False
    assert result["synthetic_rows"] == 0
    lines = path.read_bytes().splitlines(keepends=True)
    assert b"".join(lines[:2] + lines[3:]) == original
    assert next(csv.DictReader([lines[0].decode(), lines[2].decode()]))["bid_close"] == "1.10000"
    assert updater.scan_recent_gaps(path)["missing"] == []
    instrument, request = client.calls[0]
    assert instrument == "EUR_USD" and request == {
        "granularity": "M1", "count": 10, "price": "BAM", "end_time": BASE + dt.timedelta(minutes=2)}
    observed = json.loads(Path(result["observation_receipt"]).read_text())
    published = json.loads(Path(result["publication_receipt"]).read_text())
    assert observed["response"] == broker_payload()
    assert observed["response_sha256"] == hashlib.sha256(json.dumps(
        observed["response"], sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    assert observed["response_observed_utc"] == (now + dt.timedelta(seconds=2)).isoformat()
    assert observed["candle_utc"] == (BASE + dt.timedelta(minutes=1)).isoformat()
    assert published["publication_recorded_utc"] == (now + dt.timedelta(seconds=3)).isoformat()
    assert published["existing_rows_rewritten"] == 0
    assert published["publication_clock_valid"] is True
    assert updater.recover_recent_gaps(client, path)["requests"] == 0
    assert len(client.calls) == 1


def test_broker_omitted_minute_remains_missing_and_attempts_stop_across_restarts(tmp_path, monkeypatch):
    path = fixture_archive(tmp_path)
    original = path.read_bytes()
    client = FixtureClient({"instrument": "EUR_USD", "granularity": "M1", "candles": []})
    fixed_clock(monkeypatch)
    first = updater.recover_recent_gaps(client, path)
    assert first["status"] == "broker_omitted_requested_minute"
    receipt_bytes = Path(first["request_receipt"]).read_bytes()
    second = updater.recover_recent_gaps(client, path)
    assert second["attempt_state_counts"] == {"retry_cooldown": 1}
    fixed_clock(monkeypatch, BASE + dt.timedelta(hours=2, minutes=15))
    assert updater.recover_recent_gaps(client, path)["requests"] == 1
    fixed_clock(monkeypatch, BASE + dt.timedelta(hours=3))
    exhausted = updater.recover_recent_gaps(client, path)
    assert exhausted["attempt_state_counts"] == {"attempts_exhausted": 1}
    assert exhausted["unresolved_minutes"] == 1 and len(client.calls) == 2
    assert path.read_bytes() == original
    assert Path(first["request_receipt"]).read_bytes() == receipt_bytes


def test_dry_run_detects_without_calls_receipts_or_csv_mutation(tmp_path):
    path = fixture_archive(tmp_path)
    original = path.read_bytes()
    client = FixtureClient(broker_payload())
    result = updater.recover_recent_gaps(client, path, dry_run=True)
    assert result["status"] == "dry_run_detected_only"
    assert result["unresolved_minutes"] == 1 and result["requests"] == 0
    assert path.read_bytes() == original and not client.calls
    assert not (tmp_path / ".gap_recovery_v1").exists()


def test_only_one_newest_gap_requested_per_cycle_and_large_gaps_ignored(tmp_path, monkeypatch):
    path = fixture_archive(tmp_path, (0, 2, 4, 20, 22, 23))
    fixed_clock(monkeypatch)
    client = FixtureClient(broker_payload(21))
    result = updater.recover_recent_gaps(client, path)
    assert result["missing_minutes_detected"] == 3
    assert result["large_gaps_skipped"] == 1
    assert result["rows_recovered"] == 1 and result["unresolved_minutes"] == 2, result
    assert result["requested_minute_utc"] == (BASE + dt.timedelta(minutes=21)).isoformat()
    assert len(client.calls) == 1


def test_scan_is_bounded_and_does_not_look_for_old_gaps(tmp_path):
    path = tmp_path / "EUR_USD_M1.csv"
    header = b"datetime,instrument,granularity,close,padding\n"
    with path.open("wb") as handle:
        handle.write(header)
        for minute in range(10000):
            if minute in (1, 9998):
                continue
            handle.write((f"{(BASE + dt.timedelta(minutes=minute)).isoformat()},EUR_USD,M1,1.1," + "x" * 150 + "\n").encode())
    assert path.stat().st_size > updater.GAP_TAIL_BYTES
    scan = updater.scan_recent_gaps(path)
    assert scan["bytes_scanned"] <= updater.GAP_TAIL_BYTES + len(header)
    assert scan["start"] > len(header)
    assert scan["missing"] == [int((BASE + dt.timedelta(minutes=9998)).timestamp())]


@pytest.mark.parametrize("mutation", ["incomplete", "missing_complete", "missing_bid", "nan", "negative",
    "wrong_instrument", "wrong_granularity", "crossed", "bad_ohlc", "duplicate", "bad_volume", "unrounded_time"])
def test_bad_or_incomplete_broker_rows_are_never_inserted(tmp_path, monkeypatch, mutation):
    path = fixture_archive(tmp_path)
    original = path.read_bytes()
    fixed_clock(monkeypatch)
    payload = broker_payload()
    candle = payload["candles"][0]
    if mutation == "incomplete": candle["complete"] = False
    elif mutation == "missing_complete": candle.pop("complete")
    elif mutation == "missing_bid": candle.pop("bid")
    elif mutation == "nan": candle["mid"]["c"] = "NaN"
    elif mutation == "negative": candle["bid"]["o"] = "-1"
    elif mutation == "wrong_instrument": payload["instrument"] = "USD_JPY"
    elif mutation == "wrong_granularity": payload["granularity"] = "M5"
    elif mutation == "crossed": candle["bid"] = copy.deepcopy(candle["ask"]); candle["ask"] = copy.deepcopy(candle["mid"])
    elif mutation == "bad_ohlc": candle["mid"]["h"] = "0.9"
    elif mutation == "duplicate": payload["candles"].append(copy.deepcopy(candle))
    elif mutation == "bad_volume": candle["volume"] = True
    elif mutation == "unrounded_time": candle["time"] = candle["time"].replace(".000000000", ".000000001")
    result = updater.recover_recent_gaps(FixtureClient(payload), path)
    assert result["status"] == "recovery_error" and result["rows_recovered"] == 0
    assert path.read_bytes() == original
    assert not list(tmp_path.rglob("*.published.json"))


def test_neighbor_only_response_does_not_rewrite_existing_row(tmp_path, monkeypatch):
    path = fixture_archive(tmp_path)
    original = path.read_bytes()
    fixed_clock(monkeypatch)
    result = updater.recover_recent_gaps(FixtureClient(broker_payload(2)), path)
    assert result["status"] == "broker_omitted_requested_minute"
    assert path.read_bytes() == original


def test_concurrent_archive_change_aborts_insertion_without_losing_new_row(tmp_path, monkeypatch):
    path = fixture_archive(tmp_path)
    fixed_clock(monkeypatch)
    def concurrent_change():
        with path.open("ab") as handle:
            handle.write(b"another writer appended this\n")
    client = FixtureClient(broker_payload(), concurrent_change)
    result = updater.recover_recent_gaps(client, path)
    assert result["status"] == "recovery_error" and "archive changed" in result["error"]
    assert path.read_bytes().endswith(b"another writer appended this\n")
    assert result["rows_recovered"] == 0
    assert not list(tmp_path.glob("*.tmp"))
    assert Path(result["observation_receipt"]).exists()


def test_crash_after_reservation_consumes_attempt_and_does_not_backdate(tmp_path, monkeypatch):
    path = fixture_archive(tmp_path)
    fixed_clock(monkeypatch)
    client = FixtureClient(None, lambda: (_ for _ in ()).throw(RuntimeError("fixture transport crash")))
    first = updater.recover_recent_gaps(client, path)
    assert first["status"] == "recovery_error"
    assert Path(first["request_receipt"]).exists()
    assert not list(tmp_path.rglob("*.observed.json"))
    assert updater.recover_recent_gaps(client, path)["requests"] == 0
    assert len(client.calls) == 1


def test_crash_after_csv_publication_does_not_duplicate_row_on_restart(tmp_path, monkeypatch):
    path = fixture_archive(tmp_path)
    fixed_clock(monkeypatch)
    real_write = updater._write_immutable_json
    def fail_final_receipt(receipt, payload):
        if receipt.name.endswith(".published.json"):
            raise OSError("fixture failure after CSV replace")
        real_write(receipt, payload)
    monkeypatch.setattr(updater, "_write_immutable_json", fail_final_receipt)
    client = FixtureClient(broker_payload())
    first = updater.recover_recent_gaps(client, path)
    assert first["status"] == "recovery_error" and first["rows_recovered"] == 1
    assert Path(first["observation_receipt"]).exists()
    assert not Path(first["publication_receipt"]).exists()
    assert updater.recover_recent_gaps(client, path)["requests"] == 0
    assert len(client.calls) == 1


def test_rollback_during_response_refuses_publication(tmp_path, monkeypatch):
    path = fixture_archive(tmp_path)
    original = path.read_bytes()
    fixed_clock(monkeypatch, BASE + dt.timedelta(hours=2), BASE + dt.timedelta(hours=1))
    result = updater.recover_recent_gaps(FixtureClient(broker_payload()), path)
    assert result["status"] == "recovery_error" and "clock rolled back" in result["error"]
    assert path.read_bytes() == original
    assert Path(result["observation_receipt"]).exists()


def test_rollback_after_publication_is_marked_without_inventing_valid_clock(tmp_path, monkeypatch):
    path = fixture_archive(tmp_path)
    now = BASE + dt.timedelta(hours=2)
    fixed_clock(monkeypatch, now, now, now - dt.timedelta(seconds=1))
    result = updater.recover_recent_gaps(FixtureClient(broker_payload()), path)
    assert "publication_receipt" in result, result
    receipt = json.loads(Path(result["publication_receipt"]).read_text())
    assert receipt["publication_clock_valid"] is False
    assert result["status"] == "recovered_with_invalid_publication_clock"
    assert result["error"] and result["rows_recovered"] == 1
    assert receipt["publication_recorded_utc"] == (now - dt.timedelta(seconds=1)).isoformat()


def test_malformed_attempt_receipt_blocks_retry_instead_of_resetting_budget(tmp_path, monkeypatch):
    path = fixture_archive(tmp_path)
    fixed_clock(monkeypatch)
    epoch = int((BASE + dt.timedelta(minutes=1)).timestamp())
    receipt = tmp_path / ".gap_recovery_v1" / "EUR_USD" / f"{epoch}.attempt1.requested.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text("interrupted partial JSON")
    client = FixtureClient(broker_payload())
    result = updater.recover_recent_gaps(client, path)
    assert result["attempt_state_counts"] == {"invalid_attempt_receipt": 1}
    assert not client.calls


@pytest.mark.parametrize("suffix", [b"\n", b"broken"])
def test_malformed_or_incomplete_tail_causes_no_fetch(tmp_path, suffix):
    path = fixture_archive(tmp_path)
    with path.open("ab") as handle:
        handle.write(suffix)
    client = FixtureClient(broker_payload())
    assert updater.recover_recent_gaps(client, path)["status"] == "recovery_error"
    assert not client.calls


def test_update_pair_integrates_recovery_without_importing_manager(tmp_path, monkeypatch):
    path = fixture_archive(tmp_path)
    fixed_clock(monkeypatch)
    monkeypatch.setattr(updater, "fetch_newer_rows", lambda *args, **kwargs: (updater.pd.DataFrame(), {"requests": 0, "error": ""}))
    monkeypatch.setattr(updater, "fetch_older_rows", lambda *args, **kwargs: (updater.pd.DataFrame(), {"requests": 0, "error": ""}))
    result = updater.update_pair(FixtureClient(broker_payload()), path, max_requests=0,
                                backfill_requests=0, batch_size=5000, pause_seconds=0, dry_run=False)
    assert result["rows_appended"] == 0 and result["rows_backfilled"] == 0
    assert result["rows_recovered"] == 1, result
    assert result["gap_recovery"]["status"] == "recovered_actual_broker_minute"


def test_disabled_gap_recovery_does_not_read_archive_or_call_client(monkeypatch, tmp_path):
    monkeypatch.setattr(updater, "scan_recent_gaps", lambda *args: pytest.fail("unexpected archive read"))
    client = FixtureClient(None)
    result = updater.recover_recent_gaps(client, tmp_path / "missing.csv", enabled=False)
    assert result["status"] == "disabled" and not client.calls


def test_large_archive_prefix_and_existing_tail_remain_byte_identical(tmp_path, monkeypatch):
    minutes = [minute for minute in range(7000) if minute != 6998]
    path = fixture_archive(tmp_path, minutes)
    original = path.read_bytes()
    assert len(original) > updater.GAP_TAIL_BYTES
    fixed_clock(monkeypatch, BASE + dt.timedelta(minutes=7010))
    result = updater.recover_recent_gaps(FixtureClient(broker_payload(6998)), path)
    assert result["rows_recovered"] == 1, result["error"]
    lines = path.read_bytes().splitlines(keepends=True)
    assert b"".join(lines[:6999] + lines[7000:]) == original


def test_observation_receipt_failure_blocks_csv_publication(tmp_path, monkeypatch):
    path = fixture_archive(tmp_path)
    original = path.read_bytes()
    fixed_clock(monkeypatch)
    real_write = updater._write_immutable_json
    def fail_observation(receipt, payload):
        if receipt.name.endswith(".observed.json"):
            raise OSError("fixture observation persistence failure")
        return real_write(receipt, payload)
    monkeypatch.setattr(updater, "_write_immutable_json", fail_observation)
    result = updater.recover_recent_gaps(FixtureClient(broker_payload()), path)
    assert result["status"] == "recovery_error" and path.read_bytes() == original
    assert list(tmp_path.rglob("*.requested.json"))
    assert not list(tmp_path.rglob("*.published.json"))


def test_immutable_json_cannot_overwrite_existing_receipt(tmp_path):
    path = tmp_path / "receipt.json"
    updater._write_immutable_json(path, {"observed_at": "original"})
    original = path.read_bytes()
    with pytest.raises(FileExistsError):
        updater._write_immutable_json(path, {"observed_at": "forged"})
    assert path.read_bytes() == original


def test_future_attempt_clock_does_not_trigger_repeated_fetch(tmp_path, monkeypatch):
    path = fixture_archive(tmp_path)
    fixed_clock(monkeypatch)
    first = updater.recover_recent_gaps(FixtureClient({"instrument": "EUR_USD", "granularity": "M1", "candles": []}), path)
    fixed_clock(monkeypatch, BASE + dt.timedelta(hours=1))
    client = FixtureClient(broker_payload())
    result = updater.recover_recent_gaps(client, path)
    assert result["attempt_state_counts"] == {"invalid_attempt_clock": 1}
    assert not client.calls
    assert Path(first["request_receipt"]).exists()


def test_missing_and_empty_archive_have_explicit_status(tmp_path):
    client = FixtureClient(None)
    assert updater.recover_recent_gaps(client, tmp_path / "missing.csv")["status"] == "missing_archive"
    path = fixture_archive(tmp_path, ())
    assert updater.recover_recent_gaps(client, path)["status"] == "insufficient_tail_rows"
    assert not client.calls


@pytest.mark.parametrize("component,bad_value", [("o", "2.1"), ("c", "2.1"), ("l", ".4"), ("h", "3.1")])
def test_midpoint_outside_actual_bidask_is_never_published(tmp_path, monkeypatch, component, bad_value):
    path = fixture_archive(tmp_path)
    original = path.read_bytes()
    fixed_clock(monkeypatch)
    payload = broker_payload()
    candle = payload["candles"][0]
    candle["bid"] = {"o": "1", "c": "1", "l": ".5", "h": "2"}
    candle["ask"] = {"o": "2", "c": "2", "l": "1", "h": "3"}
    candle["mid"] = {"o": "1.5", "c": "1.5", "l": ".75", "h": "2.5"}
    candle["mid"][component] = bad_value
    result = updater.recover_recent_gaps(FixtureClient(payload), path)
    assert result["status"] == "recovery_error" and "midpoint outside" in result["error"]
    assert result["rows_recovered"] == 0 and path.read_bytes() == original
    assert Path(result["observation_receipt"]).exists()
    assert not list(tmp_path.rglob("*.published.json"))


@pytest.mark.parametrize("mutation", ["duplicate_header", "missing_identity", "missing_close", "missing_clock",
                                    "conflicting_clocks", "wrong_pair", "wrong_timeframe", "malformed_quote"])
def test_malformed_archive_identity_or_clock_prevents_gap_request(tmp_path, mutation):
    path = fixture_archive(tmp_path)
    table = list(csv.reader(io.StringIO(path.read_text())))
    if mutation == "duplicate_header": table[0][1] = table[0][0]
    elif mutation == "missing_identity": table[0][2] = "unknown_instrument"
    elif mutation == "missing_close": table[0][7] = "unknown_close"
    elif mutation == "missing_clock": table[0][:2] = ["unknown_time", "unknown_datetime"]
    elif mutation == "conflicting_clocks": table[1][0] = (BASE + dt.timedelta(hours=1)).isoformat()
    elif mutation == "wrong_pair": table[1][2] = "GBP_USD"
    elif mutation == "wrong_timeframe": table[1][3] = "M5"
    stream = io.StringIO(newline="")
    csv.writer(stream, lineterminator="\n").writerows(table)
    raw = stream.getvalue().encode()
    if mutation == "malformed_quote": raw = raw.replace(b"1.100000", b'"1.100000', 1)
    path.write_bytes(raw)
    client = FixtureClient(broker_payload())
    result = updater.recover_recent_gaps(client, path)
    assert result["status"] == "recovery_error", result
    assert result["requests"] == 0 and not client.calls
    assert path.read_bytes() == raw and not (tmp_path / ".gap_recovery_v1").exists()


def test_run_once_reports_unresolved_gaps_and_gap_errors_separately_from_appends(tmp_path, monkeypatch):
    monkeypatch.setattr(updater, "candle_files", lambda: [tmp_path / "EUR_USD_M1.csv"])
    monkeypatch.setattr(updater, "resolve_readonly_oanda_client", lambda: (object(), {"environment": "practice"}))
    monkeypatch.setattr(updater, "update_pair", lambda *args, **kwargs: {
        "instrument": "EUR_USD", "rows_appended": 2, "rows_backfilled": 0,
        "rows_recovered": 0, "error": "", "gap_recovery": {
            "status": "recovery_error", "requests": 1, "unresolved_minutes": 3,
            "missing_minutes_detected": 3, "error": "fixture failure"}})
    args = argparse.Namespace(pairs=[], bootstrap_all_priced=False, max_requests_per_pair=1,
        backfill_requests_per_pair=0, batch_size=5000, pause_seconds=0, dry_run=False,
        report=tmp_path / "report.json")
    assert updater.run_once(args) == 1
    report = json.loads(args.report.read_text())
    assert report["total_rows_appended"] == 2
    assert report["total_rows_recovered"] == 0
    assert report["gap_recovery_requests"] == 1
    assert report["gap_recovery_unresolved_minutes"] == 3
    assert report["gap_recovery_unknown_pair_count"] == 0
    assert report["error_count"] == 1
