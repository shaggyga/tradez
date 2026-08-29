import json
import hashlib
import sqlite3
from types import SimpleNamespace

import pandas as pd
import pytest

import oanda_all68_m1_forward_updater as updater
import oanda_moving_average_crossover_sweep as ma_sweep
import oanda_pure_change_strategy_space as branch


def test_news_blurb_census_separates_official_and_summary_coverage(tmp_path):
    path = tmp_path / "news.sqlite"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE articles(
            source_id TEXT, category TEXT, relevant INTEGER,
            source_verified INTEGER, summary TEXT
        );
        CREATE TABLE topic_events(topic_id TEXT);
        INSERT INTO articles VALUES
            ('central_bank','policy',1,1,'full official blurb'),
            ('central_bank','policy',1,1,''),
            ('aggregator','macro',0,0,'secondary summary');
        INSERT INTO topic_events VALUES ('story_a'),('story_b');
        """
    )
    connection.commit()
    connection.close()
    result = branch.news_blurb_census(path)
    assert result["articles"] == 3
    assert result["articles_with_blurb"] == 2
    assert result["official_verified_articles"] == 2
    assert result["official_verified_with_blurb"] == 1
    assert result["story_clusters"] == 2


def test_strategy_space_is_broad_but_research_only():
    config = branch.read_json(branch.CONFIG)
    result = branch.strategy_space(config)
    assert result["ma_configurations"] > 10_000
    assert result["ma_pair_horizon_evaluations_at_68_pairs"] > 1_000_000
    assert result["multitimeframe_stack_configurations"] > 1_000


def test_practice_candle_updater_refuses_live_only_credentials(monkeypatch):
    monkeypatch.setattr(
        updater,
        "manager",
        SimpleNamespace(
            CREDS_PATH="unused",
            load_creds=lambda path: {"OANDA_LIVE_API_KEY": "live-only-token"},
        ),
    )
    with pytest.raises(RuntimeError, match="refuses live credentials"):
        updater.resolve_readonly_oanda_client()


def test_priced_instruments_come_from_practice_quote_snapshot(tmp_path):
    path = tmp_path / "quotes.json"
    path.write_text(
        json.dumps({"quotes": {"EUR_USD": {"bid": 1}, "BAD": {}, "USD_JPY": {"ask": 1}}}),
        encoding="utf-8",
    )
    assert updater.priced_instruments(path) == ["EUR_USD", "USD_JPY"]


def test_ma_sweep_reads_bootstrapped_csv(tmp_path):
    path = tmp_path / "EUR_USD_M1.csv"
    pd.DataFrame(
        {
            "datetime": ["2026-08-07T12:00:00Z", "2026-08-07T12:01:00Z"],
            "open": [1.1, 1.2], "high": [1.2, 1.3], "low": [1.0, 1.1],
            "close": [1.15, 1.25], "volume": [10, 12],
            "bid_open": [1.09, 1.19], "bid_close": [1.14, 1.24],
            "ask_open": [1.11, 1.21], "ask_close": [1.16, 1.26],
            "spread_pips": [2.0, 2.0],
        }
    ).to_csv(path, index=False)
    assert ma_sweep.discover_instruments(tmp_path, "all", 0) == ["EUR_USD"]
    frame = ma_sweep.read_pair_frame(path)
    assert len(frame) == 2
    assert str(frame.index.tz) == "UTC"


def test_recovered_attribution_bundle_is_hash_bound_and_schema_gated(tmp_path, monkeypatch):
    bundle = tmp_path / "recovered.zip"
    bundle.write_bytes(b"historical-reference")
    schema = tmp_path / "config" / "episode.json"
    schema.parent.mkdir()
    schema.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(branch, "ROOT", tmp_path)
    config = {
        "historical_news_attribution": {
            "vault_path": str(bundle),
            "bundle_sha256": hashlib.sha256(bundle.read_bytes()).hexdigest(),
            "canonical_schema": "config/episode.json",
            "mode": "historical_ex_post_attribution_only",
        }
    }
    result = branch.historical_attribution_state(config)
    assert result["bundle_integrity_ok"] is True
    assert result["schema_present"] is True
    assert result["execution_eligible"] is False


def test_atomic_backfill_prepends_and_deduplicates(tmp_path):
    path = tmp_path / "EUR_USD_M1.csv"
    columns = ["datetime", "instrument", "close"]
    pd.DataFrame(
        {
            "datetime": ["2026-08-07T10:01:00+00:00", "2026-08-07T10:02:00+00:00"],
            "instrument": ["EUR_USD", "EUR_USD"],
            "close": [1.1, 1.2],
        }
    ).to_csv(path, index=False)
    older = pd.DataFrame(
        {
            "datetime": ["2026-08-07T10:00:00+00:00", "2026-08-07T10:01:00+00:00"],
            "instrument": ["EUR_USD", "EUR_USD"],
            "close": [1.0, 9.9],
        }
    )
    assert updater.prepend_rows_atomic(path, older, columns, dry_run=False) == 1
    result = pd.read_csv(path)
    assert list(result["datetime"]) == [
        "2026-08-07T10:00:00+00:00",
        "2026-08-07T10:01:00+00:00",
        "2026-08-07T10:02:00+00:00",
    ]
    assert result.loc[1, "close"] == 1.1
    assert updater.first_timestamp(path) == pd.Timestamp("2026-08-07T10:00:00Z")


def test_zero_forward_requests_is_a_true_backfill_only_mode():
    class NoCallClient:
        def candles(self, *args, **kwargs):
            raise AssertionError("forward endpoint must not be called")

    rows, metadata = updater.fetch_newer_rows(
        NoCallClient(),
        "EUR_USD",
        pd.Timestamp("2026-08-07T10:00:00Z"),
        max_requests=0,
        batch_size=5000,
        pause_seconds=0,
    )
    assert rows.empty
    assert metadata["requests"] == 0
