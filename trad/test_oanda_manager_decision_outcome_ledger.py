import datetime as dt
import json
from pathlib import Path
import sqlite3
import tempfile

import trad.oanda_manager_decision_outcome_ledger as ledger


UTC = dt.timezone.utc


def write_quote_database(path: Path, snapshots):
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE quote_snapshots_v2(sequence INTEGER PRIMARY KEY, published_epoch REAL, payload_json TEXT)"
    )
    for sequence, published_epoch, quote_epoch, bid, ask in snapshots:
        payload = {
            "quotes": {
                "USD_JPY": {
                    "bid": bid,
                    "ask": ask,
                    "pip": 0.01,
                    "time": dt.datetime.fromtimestamp(quote_epoch, UTC).isoformat(),
                }
            }
        }
        connection.execute(
            "INSERT INTO quote_snapshots_v2 VALUES (?,?,?)",
            (sequence, published_epoch, json.dumps(payload)),
        )
    connection.commit()
    connection.close()


def manager_row(observed_epoch=1000.0, horizon_sec=300.0):
    return {
        "observed_utc": dt.datetime.fromtimestamp(observed_epoch, UTC).isoformat(),
        "kind": "research_watchlist_signal",
        "signal": {
            "episode_id": "episode-1",
            "instrument": "USD_JPY",
            "direction": "long",
            "horizon_sec": horizon_sec,
            "entry_bid": 150.00,
            "entry_ask": 150.02,
            "execution_eligible": False,
        },
    }


def test_import_and_mature_uses_embedded_broker_quote_time_not_host_publish_time():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        decisions = root / "manager.jsonl"
        decisions.write_text(json.dumps(manager_row()) + "\n", encoding="utf-8")
        quote_db = root / "quotes.sqlite"
        # Host publication time is 50 seconds behind the embedded broker time.
        write_quote_database(quote_db, [(1, 1260.0, 1310.0, 150.08, 150.10)])
        connection = ledger.connect(root / "ledger.sqlite")
        assert ledger.import_forecasts(connection, decisions)["inserted"] == 1
        result = ledger.mature_forecasts(connection, quote_db, now_epoch=1320.0)
        assert result["matured"] == 1
        row = connection.execute(
            "SELECT target_quote_distance_sec,executable_net_pips,no_trade_net_pips FROM manager_outcomes"
        ).fetchone()
        assert row[0] == 10.0
        assert round(row[1], 6) == 6.0
        assert row[2] == 0.0
        status = ledger.state_payload(connection, root / "ledger.sqlite")
        assert status["outcomes"]["matured"] == 1
        assert status["can_place_orders"] is False
        connection.close()


def test_late_target_quote_is_quarantined_not_matured():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        decisions = root / "manager.jsonl"
        decisions.write_text(json.dumps(manager_row()) + "\n", encoding="utf-8")
        quote_db = root / "quotes.sqlite"
        write_quote_database(quote_db, [(1, 1370.0, 1420.0, 150.08, 150.10)])
        connection = ledger.connect(root / "ledger.sqlite")
        ledger.import_forecasts(connection, decisions)
        result = ledger.mature_forecasts(connection, quote_db, now_epoch=1430.0)
        assert result["quarantined"] == 1
        assert connection.execute("SELECT COUNT(*) FROM manager_outcomes").fetchone()[0] == 0
        assert connection.execute(
            "SELECT status FROM manager_forecasts"
        ).fetchone()[0] == "quarantined_maturity"
        assert connection.execute(
            "SELECT reason FROM manager_integrity_events"
        ).fetchone()[0] == "target_quote_too_late"
        connection.close()


def test_non_forecast_rows_are_ignored_and_import_is_idempotent():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        decisions = root / "manager.jsonl"
        decisions.write_text(
            json.dumps({"kind": "no_trade", "observed_utc": "2026-08-10T00:00:00Z"})
            + "\n" + json.dumps(manager_row()) + "\n",
            encoding="utf-8",
        )
        connection = ledger.connect(root / "ledger.sqlite")
        first = ledger.import_forecasts(connection, decisions)
        second = ledger.import_forecasts(connection, decisions)
        assert first == {"inserted": 1, "duplicates": 0, "ignored": 1, "invalid": 0}
        assert second == {"inserted": 0, "duplicates": 1, "ignored": 1, "invalid": 0}
        connection.close()
