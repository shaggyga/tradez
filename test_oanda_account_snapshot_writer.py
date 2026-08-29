import json

try:
    from oanda_account_snapshot_writer import (
        aggregate_account_rows,
        retain_failed_account_values,
        write_snapshot_targets,
    )
except ModuleNotFoundError:
    from trad.oanda_account_snapshot_writer import (
        aggregate_account_rows,
        retain_failed_account_values,
        write_snapshot_targets,
    )


def test_one_fetch_payload_is_atomically_mirrored_without_duplicate_target(tmp_path):
    primary = tmp_path / "account_007_dashboard_v1.json"
    compatibility = tmp_path / "account_dashboard_v1.json"
    payload = {"time": "2026-08-28T01:00:00Z", "aggregate": {"nav": 41.6042}}

    targets = write_snapshot_targets(
        primary,
        [compatibility, primary],
        payload,
    )

    assert targets == [primary, compatibility]
    assert json.loads(primary.read_text(encoding="utf-8")) == payload
    assert json.loads(compatibility.read_text(encoding="utf-8")) == payload
    assert not list(tmp_path.glob("*.tmp"))


def verified_row():
    return {
        "role": "practice_007",
        "env": "practice",
        "account_id": "101-001-37981792-007",
        "ok": True,
        "NAV": "41.6042",
        "balance": "41.6042",
        "pl": "-8.3430",
        "unrealizedPL": "0.0",
        "openTradeCount": "0",
        "pendingOrderCount": 0,
        "trades": [],
    }


def failed_row():
    return {
        "role": "practice_007",
        "env": "practice",
        "account_id": "101-001-37981792-007",
        "ok": False,
        "status_code": 503,
        "error": "System under maintenance, please try again later.",
    }


def test_current_account_aggregate_is_explicitly_current():
    rows = retain_failed_account_values(
        [verified_row()],
        previous_snapshot=None,
        observed_time="2026-08-28T21:00:00+00:00",
    )
    aggregate = aggregate_account_rows(rows)

    assert aggregate["snapshot_state"] == "current"
    assert aggregate["account_values_current"] is True
    assert aggregate["positions_current"] is True
    assert aggregate["orders_current"] is True
    assert aggregate["nav"] == 41.6042
    assert aggregate["balance"] == 41.6042
    assert aggregate["pl"] == -8.343
    assert aggregate["openTradeCount"] == 0
    assert aggregate["pendingOrderCount"] == 0
    assert aggregate["current_errors"] == []
    assert aggregate["last_verified"] is None


def test_503_retains_financial_context_but_nulls_current_aggregate_and_positions():
    previous = {
        "time": "2026-08-28T21:00:00+00:00",
        "accounts": [verified_row()],
    }
    rows = retain_failed_account_values(
        [failed_row()],
        previous_snapshot=previous,
        observed_time="2026-08-28T21:02:30+00:00",
    )
    aggregate = aggregate_account_rows(rows)

    assert rows[0]["ok"] is False
    assert rows[0]["account_values_current"] is False
    assert rows[0]["positions_current"] is False
    assert rows[0]["orders_current"] is False
    assert rows[0]["last_verified"] == {
        "source_time_utc": "2026-08-28T21:00:00+00:00",
        "age_sec": 150.0,
        "fields": {
            "NAV": 41.6042,
            "balance": 41.6042,
            "pl": -8.343,
            "unrealizedPL": 0.0,
        },
        "positions_orders_retained": False,
    }
    assert "trades" not in rows[0]
    assert "openTradeCount" not in rows[0]
    assert "pendingOrderCount" not in rows[0]

    assert aggregate["snapshot_state"] == "retained_stale_account_values"
    assert aggregate["nav"] is None
    assert aggregate["balance"] is None
    assert aggregate["pl"] is None
    assert aggregate["unrealizedPL"] is None
    assert aggregate["openTradeCount"] is None
    assert aggregate["pendingOrderCount"] is None
    assert aggregate["positions_current"] is False
    assert aggregate["orders_current"] is False
    assert aggregate["last_verified"]["fields"] == {
        "nav": 41.6042,
        "balance": 41.6042,
        "pl": -8.343,
        "unrealizedPL": 0.0,
    }
    assert aggregate["last_verified"]["oldest_age_sec"] == 150.0
    assert aggregate["current_errors"][0]["status_code"] == 503


def test_repeated_failure_keeps_original_verified_time_instead_of_refreshing_stale_age():
    first_failure_rows = retain_failed_account_values(
        [failed_row()],
        previous_snapshot={
            "time": "2026-08-28T21:00:00+00:00",
            "accounts": [verified_row()],
        },
        observed_time="2026-08-28T21:02:30+00:00",
    )
    second_failure_rows = retain_failed_account_values(
        [failed_row()],
        previous_snapshot={
            "time": "2026-08-28T21:02:30+00:00",
            "accounts": first_failure_rows,
        },
        observed_time="2026-08-28T21:05:00+00:00",
    )

    assert second_failure_rows[0]["last_verified"]["source_time_utc"] == "2026-08-28T21:00:00+00:00"
    assert second_failure_rows[0]["last_verified"]["age_sec"] == 300.0


def test_failure_without_verified_context_is_unavailable_not_zero():
    rows = retain_failed_account_values(
        [failed_row()],
        previous_snapshot={
            "time": "2026-08-28T21:00:00+00:00",
            "accounts": [failed_row()],
        },
        observed_time="2026-08-28T21:01:00+00:00",
    )
    aggregate = aggregate_account_rows(rows)

    assert aggregate["snapshot_state"] == "unavailable"
    assert aggregate["nav"] is None
    assert aggregate["balance"] is None
    assert aggregate["pl"] is None
    assert aggregate["openTradeCount"] is None
    assert aggregate["pendingOrderCount"] is None
    assert aggregate["last_verified"] is None
