from datetime import datetime, timedelta, timezone

import sqlite3
import pytest

import oanda_spike_blurb_existing_source_reconciliation_v1 as reconcile


UTC = timezone.utc


def test_availability_states_keep_first_seen_separate():
    start = datetime(2026, 1, 1, 12, tzinfo=UTC)
    assert reconcile.availability_state(start - timedelta(minutes=5), start - timedelta(minutes=1), start) == "pre_entry_causal_available"
    assert reconcile.availability_state(start - timedelta(minutes=5), start + timedelta(minutes=2), start) == "during_first_wave"
    assert reconcile.availability_state(start - timedelta(minutes=5), start + timedelta(hours=1), start) == "published_pre_entry_observed_late"


def test_relevance_prefers_exact_currency():
    assert reconcile.relevance_state(["JPY"], {"JPY", "USD"}, "JPY") == "exact_factor_currency"
    assert reconcile.relevance_state(["USD"], {"JPY", "USD"}, "JPY") == "exact_single_episode_leg"
    assert reconcile.relevance_state(["AUD", "JPY", "USD"], {"JPY", "USD"}, "JPY") == "narrow_multi_currency"
    assert reconcile.relevance_state(["AUD", "CAD", "CHF", "JPY", "USD"], {"JPY", "USD"}, "JPY") == "broad_risk_mapping"


def test_source_candidate_tables_are_immutable(tmp_path):
    connection = sqlite3.connect(tmp_path / "test.sqlite")
    reconcile.ensure_schema(connection)
    connection.execute(
        "INSERT INTO existing_source_reconciliation_contracts VALUES (?,?,?,?,?)",
        ("c", "{}", "a" * 64, "b" * 64, "2026-01-01T00:00:00+00:00"),
    )
    connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute("UPDATE existing_source_reconciliation_contracts SET contract_json='x' WHERE contract_id='c'")
