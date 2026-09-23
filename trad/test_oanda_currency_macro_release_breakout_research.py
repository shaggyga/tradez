import datetime as dt
import json
import sqlite3
from pathlib import Path

import oanda_currency_macro_release_breakout_research as worker


UTC = dt.timezone.utc


def make_config(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "source_id": "official_cad",
                        "currencies": ["CAD"],
                        "verified": True,
                        "direct": True,
                        "source_role": "primary_statistical_release",
                    },
                    {
                        "source_id": "ambiguous",
                        "currencies": ["USD", "CAD"],
                        "verified": True,
                        "direct": True,
                        "source_role": "primary_statistical_release",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )


def make_news(path: Path, *, published: str, known: str) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE articles(event_id TEXT,payload_json TEXT,first_seen_utc TEXT)"
    )
    payload = {
        "source_id": "official_cad",
        "source_verified": True,
        "source_direct": True,
        "source_listing_bootstrap": False,
        "published_utc": published,
        "causal_known_utc": known,
        "numeric_causal_known_utc": known,
        "actual_value": 1.2,
        "consensus_value": None,
        "event_name": "Official CAD release",
    }
    connection.execute(
        "INSERT INTO articles VALUES (?,?,?)",
        ("event-1", json.dumps(payload), known),
    )
    connection.commit()
    connection.close()


def test_source_currency_map_rejects_ambiguous_sources(tmp_path):
    config = tmp_path / "sources.json"
    make_config(config)
    assert worker.source_currency_map(config) == {"official_cad": "CAD"}


def test_past_release_cannot_enter_new_prospective_cohort(tmp_path):
    config = tmp_path / "sources.json"
    news = tmp_path / "news.sqlite"
    make_config(config)
    make_news(
        news,
        published="2026-08-14T12:30:00+00:00",
        known="2026-08-16T06:16:00+00:00",
    )
    rows = worker.load_structured_macro_events(news, config)
    assert len(rows) == 1
    assert rows[0]["currency"] == "CAD"
    assert rows[0]["prospective_eligible"] is False


def test_prompt_official_actual_is_prospectively_eligible(tmp_path):
    config = tmp_path / "sources.json"
    news = tmp_path / "news.sqlite"
    make_config(config)
    make_news(
        news,
        published="2026-08-16T12:30:00+00:00",
        known="2026-08-16T12:31:00+00:00",
    )
    rows = worker.load_structured_macro_events(news, config)
    assert rows[0]["prospective_eligible"] is True
    assert rows[0]["causal_ingest_latency_minutes"] == 1.0


def test_first_seen_substituted_for_publication_cannot_self_certify(tmp_path):
    config = tmp_path / "sources.json"
    news = tmp_path / "news.sqlite"
    make_config(config)
    make_news(
        news,
        published="2026-08-16T12:30:00+00:00",
        known="2026-08-16T12:30:00+00:00",
    )
    connection=sqlite3.connect(news)
    payload=json.loads(connection.execute("SELECT payload_json FROM articles").fetchone()[0])
    payload["published_time_inferred"]=True
    connection.execute("UPDATE articles SET payload_json=?",(json.dumps(payload),))
    connection.commit();connection.close()
    rows=worker.load_structured_macro_events(news,config)
    assert rows[0]["prospective_eligible"] is False
    assert rows[0]["eligibility_reason"]=="inferred_publication_clock_excluded"


def test_confirmation_requirement_never_treats_one_pair_as_independent_factor():
    assert worker.confirmation_requirement(0) is None
    assert worker.confirmation_requirement(1) is None
    assert worker.confirmation_requirement(2) == 2
    assert worker.confirmation_requirement(3) == 2
    assert worker.confirmation_requirement(7) == 4
    assert worker.confirmation_requirement(18) == 4


def test_run_once_is_research_only_and_does_not_merge_usd_cohort(tmp_path):
    config = tmp_path / "sources.json"
    news = tmp_path / "news.sqlite"
    make_config(config)
    make_news(
        news,
        published="2026-08-16T12:30:00+00:00",
        known="2026-08-16T12:31:00+00:00",
    )
    payload = worker.run_once(
        news_db=news,
        config_path=config,
        quote_db=tmp_path / "missing-quotes.sqlite",
        database_path=tmp_path / "evidence.sqlite",
        output_path=tmp_path / "state.json",
        report_path=tmp_path / "report.md",
        observed=dt.datetime(2026, 8, 16, 12, 32, tzinfo=UTC),
    )
    assert payload["research_only"] is True
    assert payload["can_place_orders"] is False
    assert payload["can_promote"] is False
    assert payload["policy"]["frozen_usd_cohort_unchanged"] is True
    assert payload["contract_id"] == "currency_macro_release_two_sided_v2_20260816"


def test_numeric_clock_wins_and_duplicate_representations_collapse(tmp_path):
    config = tmp_path / "sources.json"
    news = tmp_path / "news.sqlite"
    make_config(config)
    make_news(
        news,
        published="2026-08-16T12:30:00+00:00",
        known="2026-08-16T12:31:00+00:00",
    )
    connection = sqlite3.connect(news)
    payload = json.loads(
        connection.execute("SELECT payload_json FROM articles").fetchone()[0]
    )
    payload["causal_known_utc"] = "2026-08-16T12:35:00+00:00"
    payload["numeric_causal_known_utc"] = "2026-08-16T12:31:00+00:00"
    connection.execute(
        "UPDATE articles SET payload_json=? WHERE event_id='event-1'",
        (json.dumps(payload),),
    )
    connection.execute(
        "INSERT INTO articles VALUES (?,?,?)",
        (
            "duplicate-event",
            json.dumps(payload),
            "2026-08-16T12:40:00+00:00",
        ),
    )
    connection.commit()
    connection.close()

    rows = worker.load_structured_macro_events(news, config)
    assert len(rows) == 1
    assert rows[0]["known_utc"] == "2026-08-16T12:31:00+00:00"
    assert rows[0]["duplicate_representation_count"] == 2
    assert rows[0]["source_event_ids"] == ["duplicate-event", "event-1"]


def test_prefilter_keeps_derived_child_source_and_skips_unmapped_payload(tmp_path):
    config = tmp_path / "sources.json"
    news = tmp_path / "news.sqlite"
    make_config(config)
    make_news(
        news,
        published="2026-08-16T12:30:00+00:00",
        known="2026-08-16T12:31:00+00:00",
    )
    connection = sqlite3.connect(news)
    payload = json.loads(
        connection.execute("SELECT payload_json FROM articles").fetchone()[0]
    )
    payload["source_id"] = "official_cad:derived-series"
    connection.execute(
        "UPDATE articles SET payload_json=? WHERE event_id='event-1'",
        (json.dumps(payload),),
    )
    connection.execute(
        "INSERT INTO articles VALUES (?,?,?)",
        (
            "unmapped",
            json.dumps({"source_id": "other", "blob": "x" * 100_000}),
            "2026-08-16T12:31:00+00:00",
        ),
    )
    connection.commit()
    connection.close()

    rows = worker.load_structured_macro_events(news, config)

    assert len(rows) == 1
    assert rows[0]["currency"] == "CAD"
