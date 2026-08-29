import datetime as dt
import json
import sqlite3
from pathlib import Path

import pytest

import oanda_internal_macro_expectation as worker


UTC = dt.timezone.utc


def make_config(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "contract_id": "test_internal_expectation",
                "minimum_training_episodes": 1,
                "maximum_training_episodes": 12,
                "maximum_forecast_age_days": 120,
                "target": "next_distinct_official_release_for_currency_and_series",
            }
        ),
        encoding="utf-8",
    )


def make_source(path: Path) -> None:
    db = sqlite3.connect(path)
    db.execute(
        """CREATE TABLE macro_release_revisions(
             row_id INTEGER PRIMARY KEY, revision_id TEXT, release_key TEXT,
             causal_known_utc TEXT,event_series_id TEXT,event_name TEXT,
             currencies_json TEXT,reference_period TEXT,unit TEXT,
             actual_value REAL,previous_value REAL,source_id TEXT,
             source_verified INTEGER,source_direct INTEGER,payload_json TEXT)"""
    )
    db.commit()
    db.close()


def add_episode(
    path: Path,
    *,
    revision: str,
    release: str,
    known: str,
    period: str,
    actual: float,
) -> None:
    db = sqlite3.connect(path)
    db.execute(
        """INSERT INTO macro_release_revisions
           (revision_id,release_key,causal_known_utc,event_series_id,event_name,
            currencies_json,reference_period,unit,actual_value,previous_value,
            source_id,source_verified,source_direct,payload_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            revision,
            release,
            known,
            "official_cpi_yoy",
            "Official CPI",
            '["AAA"]',
            period,
            "percent",
            actual,
            None,
            "official_aaa",
            1,
            1,
            "{}",
        ),
    )
    db.commit()
    db.close()


def run(tmp_path: Path, observed: dt.datetime) -> dict:
    return worker.run_once(
        source_path=tmp_path / "source.sqlite",
        config_path=tmp_path / "config.json",
        database_path=tmp_path / "expectations.sqlite",
        output_path=tmp_path / "state.json",
        report_path=tmp_path / "report.md",
        observed=observed,
        code_path=Path(worker.__file__),
    )


def test_issues_point_in_time_baseline_without_calling_it_consensus(tmp_path: Path) -> None:
    make_config(tmp_path / "config.json")
    make_source(tmp_path / "source.sqlite")
    add_episode(
        tmp_path / "source.sqlite",
        revision="r1",
        release="release-1",
        known="2026-08-01T12:00:00+00:00",
        period="June 2026",
        actual=2.0,
    )
    payload = run(tmp_path, dt.datetime(2026, 8, 2, tzinfo=UTC))
    assert payload["summary"]["forecast_count"] == 1
    assert payload["market_consensus_observations_added"] == 0
    assert payload["can_populate_market_consensus"] is False
    db = sqlite3.connect(tmp_path / "expectations.sqlite")
    row = db.execute(
        "SELECT expected_value,market_consensus,research_only,execution_eligible FROM expectation_forecasts"
    ).fetchone()
    db.close()
    assert row == (2.0, 0, 1, 0)


def test_later_distinct_release_matures_then_opens_next_forecast(tmp_path: Path) -> None:
    make_config(tmp_path / "config.json")
    make_source(tmp_path / "source.sqlite")
    add_episode(
        tmp_path / "source.sqlite",
        revision="r1",
        release="release-1",
        known="2026-08-01T12:00:00+00:00",
        period="June 2026",
        actual=2.0,
    )
    run(tmp_path, dt.datetime(2026, 8, 2, tzinfo=UTC))
    add_episode(
        tmp_path / "source.sqlite",
        revision="r2",
        release="release-2",
        known="2026-08-10T12:00:00+00:00",
        period="July 2026",
        actual=2.5,
    )
    payload = run(tmp_path, dt.datetime(2026, 8, 10, 12, 1, tzinfo=UTC))
    assert payload["new_outcomes"] == 1
    assert payload["summary"]["matured_count"] == 1
    assert payload["summary"]["pending_count"] == 1
    db = sqlite3.connect(tmp_path / "expectations.sqlite")
    outcome = db.execute(
        "SELECT expected_value,actual_value,forecast_error FROM expectation_outcomes"
    ).fetchone()
    db.close()
    assert outcome == (2.0, 2.5, 0.5)


def test_duplicate_source_representations_do_not_create_a_fake_episode(tmp_path: Path) -> None:
    make_source(tmp_path / "source.sqlite")
    for revision, release in (("r1", "release-a"), ("r2", "release-b")):
        add_episode(
            tmp_path / "source.sqlite",
            revision=revision,
            release=release,
            known="2026-08-01T12:00:00+00:00",
            period="June 2026",
            actual=2.0,
        )
    rows = worker.load_episodes(
        tmp_path / "source.sqlite", dt.datetime(2026, 8, 2, tzinfo=UTC)
    )
    assert len(rows) == 1


def test_forecast_and_outcome_ledgers_are_immutable(tmp_path: Path) -> None:
    make_config(tmp_path / "config.json")
    make_source(tmp_path / "source.sqlite")
    add_episode(
        tmp_path / "source.sqlite",
        revision="r1",
        release="release-1",
        known="2026-08-01T12:00:00+00:00",
        period="June 2026",
        actual=2.0,
    )
    run(tmp_path, dt.datetime(2026, 8, 2, tzinfo=UTC))
    db = sqlite3.connect(tmp_path / "expectations.sqlite")
    with pytest.raises(sqlite3.IntegrityError):
        db.execute("UPDATE expectation_forecasts SET expected_value=99")
    db.close()
