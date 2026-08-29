import datetime as dt
import json
import sqlite3
from pathlib import Path

import oanda_alfred_unemployment_relative_prospective as worker


UTC = dt.timezone.utc


def test_supervisor_uses_independent_progress_heartbeat_for_long_cycle():
    supervisor = (Path(__file__).resolve().parent / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    start = supervisor.index('-Name "alfred_unemployment_relative_prospective"')
    block = supervisor[start : start + 1_400]
    assert 'alfred_unemployment_relative_prospective_heartbeat_v1.json' in block
    assert 'LiteralPath = (Join-Path $State "alfred_unemployment_relative_prospective_v1.json")' not in block
    assert 'WatchedPhases = @("running_cycle")' in block


def test_only_later_prospective_observation_creates_signed_pair_event(tmp_path):
    database = tmp_path / "alfred.sqlite"
    db = sqlite3.connect(database)
    db.execute(
        """CREATE TABLE vintage_observations (
          series_id TEXT,currency TEXT,observation_date TEXT,value REAL,
          first_seen_utc TEXT,version INTEGER,bootstrap_current_view INTEGER,
          prospective_eligible INTEGER,cohort_id TEXT
        )"""
    )
    rows = [
        ("EUR_UN", "EUR", "2026-05-01", 6.1, "2026-08-01T00:00:00Z", 1, 1, 0, "source.cohort"),
        ("EUR_UN", "EUR", "2026-06-01", 6.0, "2026-08-16T19:40:00Z", 1, 0, 1, "source.cohort"),
        ("USD_UN", "USD", "2026-05-01", 3.9, "2026-08-01T00:00:00Z", 1, 1, 0, "source.cohort"),
        ("USD_UN", "USD", "2026-06-01", 4.1, "2026-08-16T19:35:00Z", 1, 0, 1, "source.cohort"),
    ]
    db.executemany("INSERT INTO vintage_observations VALUES (?,?,?,?,?,?,?,?,?)", rows)
    db.commit()
    db.close()
    state = tmp_path / "state.json"
    state.write_text(
        json.dumps(
            {
                "cohort": {
                    "cohort_id": "source.cohort",
                    "source_contract_id": "source.contract",
                }
            }
        ),
        encoding="utf-8",
    )
    source_config = tmp_path / "source.json"
    source_config.write_text(
        json.dumps(
            {
                "series": [
                    {
                        "series_id": "EUR_UN",
                        "currency": "EUR",
                        "cross_currency_frequency_group": "monthly_unemployment_rate",
                    },
                    {
                        "series_id": "USD_UN",
                        "currency": "USD",
                        "cross_currency_frequency_group": "monthly_unemployment_rate",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    config = {
        "source_cohort_id": "source.cohort",
        "source_contract_id": "source.contract",
        "cohort_start_utc": "2026-08-16T19:32:15Z",
        "eligible_frequency_groups": ["monthly_unemployment_rate"],
        "maximum_signal_age_sec": 604800,
    }
    events, status = worker.load_events(
        database,
        state,
        source_config,
        ["EUR_USD"],
        config,
        dt.datetime(2026, 8, 16, 20, tzinfo=UTC),
    )
    assert status["source_cohort_match"] is True
    assert len(events) == 1
    assert round(events[0]["acceleration_differential"], 8) == 0.3
    assert events[0]["predicted_side"] == "long"
    assert events[0]["signal_utc"] == "2026-08-16T19:40:00+00:00"
    assert events[0]["base_observation"]["reference_date"] == "2026-06-01"
    # The exact event envelope must remain persistable by issue_forecasts.
    json.dumps(events[0], sort_keys=True)

    higher_rate_config = {
        **config,
        "change_direction_policy": "higher_is_stronger",
        "event_id_prefix": "rate_event_",
        "factor_episode_prefix": "rate_factor_",
    }
    rate_events, _ = worker.load_events(
        database,
        state,
        source_config,
        ["EUR_USD"],
        higher_rate_config,
        dt.datetime(2026, 8, 16, 20, tzinfo=UTC),
    )
    assert rate_events[0]["predicted_side"] == "short"
    assert round(rate_events[0]["acceleration_differential"], 8) == -0.3
    assert rate_events[0]["event_id"].startswith("rate_event_")
    assert rate_events[0]["factor_episode_id"].startswith("rate_factor_")


def test_source_cohort_mismatch_blocks_events(tmp_path):
    state = tmp_path / "state.json"
    state.write_text(
        json.dumps(
            {
                "cohort": {
                    "cohort_id": "other",
                    "source_contract_id": "source.contract",
                }
            }
        ),
        encoding="utf-8",
    )
    events, status = worker.load_events(
        tmp_path / "missing.sqlite",
        state,
        tmp_path / "source.json",
        ["EUR_USD"],
        {
            "source_cohort_id": "required",
            "source_contract_id": "source.contract",
        },
        dt.datetime(2026, 8, 16, 20, tzinfo=UTC),
    )
    assert events == []
    assert status["source_cohort_match"] is False
