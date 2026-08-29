import datetime as dt

import oanda_alfred_short_rate_initial_release_snapshot as snapshot


def test_conservative_provider_date_availability_waits_until_next_local_day():
    # June uses daylight time in Chicago: local midnight is 05:00 UTC.
    assert snapshot.conservative_available_utc(
        "2026-06-10", "America/Chicago"
    ) == "2026-06-11T05:00:00+00:00"
    # January uses standard time: local midnight is 06:00 UTC.
    assert snapshot.conservative_available_utc(
        "2026-01-10", "America/Chicago"
    ) == "2026-01-11T06:00:00+00:00"


def test_snapshot_contract_is_historical_and_never_executable():
    contract = snapshot.snapshot_contract(
        {
            "source_contract_id": "test",
            "provider_date_availability_policy": "next_provider_local_day_start",
        }
    )
    assert contract["provider_output_type"] == 4
    assert contract["historical_backfill_only"] is True
    assert contract["proof_eligible"] is False
    assert contract["execution_eligible"] is False


def test_snapshot_configuration_declares_no_execution_path():
    config = snapshot.read_json(snapshot.CONFIG)
    assert config["fred_output_type"] == 4
    assert config["historical_backfill_only"] is True
    assert config["proof_eligible"] is False
    assert config["execution_eligible"] is False
