import datetime as dt
import json
from pathlib import Path
import sqlite3

import oanda_macro_release_breakout_research as macro
from oanda_instrument_pips import fallback_pip_size


UTC = dt.timezone.utc


def test_bls_loader_prefilter_preserves_release_and_skips_unrelated_payload(
    tmp_path: Path,
):
    news_db = tmp_path / "news.sqlite"
    connection = sqlite3.connect(news_db)
    connection.execute(
        "CREATE TABLE articles(event_id TEXT,payload_json TEXT,first_seen_utc TEXT)"
    )
    release = {
        "source_kind": "bls_current_release",
        "source_verified": True,
        "source_direct": True,
        "source_listing_bootstrap": False,
        "causal_known_utc": "2026-09-01T12:30:01Z",
        "published_utc": "2026-09-01T12:30:00Z",
        "event_name": "Employment Situation",
    }
    connection.executemany(
        "INSERT INTO articles VALUES(?,?,?)",
        [
            ("release", json.dumps(release), release["causal_known_utc"]),
            (
                "unrelated",
                json.dumps({"source_kind": "rss", "blob": "x" * 100_000}),
                release["causal_known_utc"],
            ),
        ],
    )
    connection.commit()
    connection.close()

    rows = macro.load_structured_macro_events(news_db)

    assert len(rows) == 1
    assert rows[0]["event_id"] == "release"


def test_full_schema_loader_uses_configured_source_lineage_including_children(
    tmp_path: Path,
):
    news_db = tmp_path / "news.sqlite"
    config_path = tmp_path / "sources.json"
    config_path.write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "source_id": "test_bls_release",
                        "kind": "bls_current_release",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    connection = sqlite3.connect(news_db)
    connection.execute(
        "CREATE TABLE articles("
        "event_id TEXT,source_id TEXT,payload_json TEXT,first_seen_utc TEXT)"
    )
    connection.execute(
        "CREATE INDEX idx_articles_source_id ON articles(source_id)"
    )
    common = {
        "source_kind": "bls_current_release",
        "source_verified": True,
        "source_direct": True,
        "source_listing_bootstrap": False,
        "causal_known_utc": "2026-09-01T12:30:01Z",
        "published_utc": "2026-09-01T12:30:00Z",
        "event_name": "Configured BLS release",
    }
    connection.executemany(
        "INSERT INTO articles VALUES(?,?,?,?)",
        [
            (
                "base",
                "test_bls_release",
                json.dumps({**common, "source_id": "test_bls_release"}),
                common["causal_known_utc"],
            ),
            (
                "child",
                "test_bls_release:series",
                json.dumps(
                    {**common, "source_id": "test_bls_release:series"}
                ),
                common["causal_known_utc"],
            ),
            (
                "unrelated",
                "large_unrelated_feed",
                json.dumps(
                    {
                        "source_kind": "rss",
                        "blob": "x" * 100_000,
                    }
                ),
                common["causal_known_utc"],
            ),
        ],
    )
    connection.commit()
    connection.close()

    rows = macro.load_structured_macro_events(
        news_db, config_path=config_path
    )

    assert [row["event_id"] for row in rows] == ["base", "child"]
    assert macro.configured_bls_current_release_source_ids(config_path) == [
        "bls_ppi_current_release_v1",
        "test_bls_release",
    ]



def synthetic_rows(*, persistent: bool = True):
    event_epoch = int(dt.datetime(2026, 9, 1, 12, 30, tzinfo=UTC).timestamp())
    starts = {
        "EUR_USD": 1.10,
        "GBP_USD": 1.30,
        "AUD_USD": 0.70,
        "NZD_USD": 0.62,
        "USD_JPY": 150.0,
        "USD_CHF": 0.82,
        "USD_CAD": 1.38,
    }
    rows = {}
    for instrument, starting_mid in starts.items():
        pip = fallback_pip_size(instrument)
        values = {}
        for offset in range(-10, 22):
            epoch = event_epoch + offset * 60
            values[epoch] = {
                "mid": starting_mid,
                "spread_pips": 1.0,
                "imbalance_30s": 0.0,
                "imbalance_120s": 0.0,
            }
        rows[instrument] = values

    # A four-pair USD-strength impulse in minutes one and two must be ignored
    # because the contract does not consider the first three minutes.
    for instrument in ("EUR_USD", "GBP_USD", "AUD_USD", "USD_CAD"):
        usd_is_base = instrument.startswith("USD_")
        sign = 1 if usd_is_base else -1
        pip = fallback_pip_size(instrument)
        for offset in (1, 2):
            rows[instrument][event_epoch + offset * 60].update({
                "mid": starts[instrument] + sign * 2.0 * pip,
                "imbalance_30s": sign * 0.20,
            })

    # Four independent pair observations express one weak-USD factor.  It is
    # actionable only when the same side persists for two completed minutes.
    offsets = (7, 8) if persistent else (8,)
    for instrument in ("AUD_USD", "GBP_USD", "USD_CAD", "USD_CHF"):
        usd_is_base = instrument.startswith("USD_")
        sign = -1 if usd_is_base else 1
        pip = fallback_pip_size(instrument)
        for offset in offsets:
            rows[instrument][event_epoch + offset * 60].update({
                "mid": starts[instrument] + sign * 2.0 * pip,
                "imbalance_30s": sign * 0.20,
            })
        if persistent:
            rows[instrument][event_epoch + 9 * 60].update({
                "mid": starts[instrument] + sign * 2.5 * pip,
                "imbalance_30s": sign * 0.20,
            })
    return rows


def event_fixture():
    return {
        "event_id": "synthetic_release",
        "currency": "USD",
        "published_utc": "2026-09-01T12:30:00+00:00",
        "known_utc": "2026-09-01T12:30:00+00:00",
        "consensus_value": None,
    }


def test_two_sided_release_rule_ignores_initial_impulse_and_requires_persistence():
    setup = macro.evaluate_two_sided(event_fixture(), synthetic_rows())
    assert setup["status"] == "triggered"
    assert setup["currency_direction"] == "WEAKEN"
    assert setup["confirmation_minute_utc"] == "2026-09-01T12:38:00+00:00"
    assert setup["entry_minute_utc"] == "2026-09-01T12:39:00+00:00"
    assert setup["confirmation_count"] == 4
    assert setup["one_usd_factor_counted"] is True
    assert setup["causal_consensus_available"] is False


def test_two_sided_release_rule_rejects_one_minute_confirmation():
    setup = macro.evaluate_two_sided(
        event_fixture(), synthetic_rows(persistent=False)
    )
    assert setup["status"] == "no_trigger"
    assert setup["execution_eligible"] is False


def test_historical_ppi_case_is_discovery_only_and_after_cost_positive():
    case = macro.discovery_case(macro.QUOTE_DB)
    assert case["event"]["prospective_eligible"] is False
    setup = case["setup"]
    assert setup["currency_direction"] == "WEAKEN"
    assert setup["entry_minute_utc"] == "2026-08-13T12:39:00+00:00"
    assert all(
        row["estimated_after_spread_pips"] > 0
        for row in setup["replay_outcomes"].values()
    )
