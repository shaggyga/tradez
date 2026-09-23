from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3

import pytest

import oanda_currency_rank_model as price_rank
import oanda_source_conditioned_currency_rank_v1 as subject


UTC = timezone.utc


def create_source_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE source_event_observation(
          canonical_event_id TEXT PRIMARY KEY, first_known_utc TEXT NOT NULL
        );
        CREATE TABLE source_event_episode(
          canonical_event_id TEXT PRIMARY KEY, market_episode_id TEXT NOT NULL
        );
        CREATE TABLE source_factor_forecast(
          forecast_id TEXT PRIMARY KEY, canonical_event_id TEXT NOT NULL,
          currency TEXT NOT NULL, factor_key TEXT NOT NULL,
          horizon_min INTEGER NOT NULL, issued_utc TEXT NOT NULL,
          training_cutoff_utc TEXT NOT NULL, forecast_state TEXT NOT NULL,
          effective_event_n INTEGER NOT NULL,
          probability_strengthening REAL,
          predicted_currency_factor_bps REAL,
          predicted_absolute_factor_bps REAL,
          prospective_proof_eligible INTEGER NOT NULL,
          contract_id TEXT NOT NULL, cohort_id TEXT NOT NULL
        );
        """
    )
    connection.commit()
    connection.close()


def insert_source_forecast(
    path: Path,
    *,
    forecast_id: str,
    event_id: str,
    episode_id: str,
    currency: str,
    issued: datetime,
    predicted: float,
    probability: float,
    factor_key: str = "action:rate_change",
    prospective: int = 1,
    state: str = "forecast",
) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        "INSERT OR IGNORE INTO source_event_observation VALUES (?,?)",
        (event_id, subject.iso(issued)),
    )
    connection.execute(
        "INSERT OR IGNORE INTO source_event_episode VALUES (?,?)",
        (event_id, episode_id),
    )
    connection.execute(
        "INSERT INTO source_factor_forecast VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            forecast_id, event_id, currency, factor_key, 5,
            subject.iso(issued), subject.iso(issued), state, 9, probability,
            predicted, abs(predicted), prospective, "source_contract", "source_cohort",
        ),
    )
    connection.commit()
    connection.close()


def ticker_payload(now: datetime) -> dict:
    strengths = {
        horizon: {currency: 0.0 for currency in price_rank.EXPECTED_CURRENCIES}
        for horizon in ("5", "15", "60")
    }
    for horizon in strengths:
        strengths[horizon]["USD"] = 4.0
        strengths[horizon]["GBP"] = -4.0
    pairs = {}
    for instrument in price_rank.EXPECTED_INSTRUMENTS:
        pairs[instrument] = {
            "instrument": instrument, "mid": 1.0, "spread_bps": 1.0,
            "windows": {
                window: {"return_bps": 0.0, "return_pips": 0.0}
                for window in ("5", "15", "60")
            },
        }
    pairs["GBP_USD"]["windows"]["5"].update(return_bps=-5.0, return_pips=-5.0)
    pairs["GBP_USD"]["windows"]["15"].update(return_bps=-6.0, return_pips=-6.0)
    pairs["EUR_JPY"]["windows"]["5"].update(return_bps=2.0, return_pips=2.0)
    pairs["EUR_JPY"]["windows"]["15"].update(return_bps=3.0, return_pips=3.0)
    return {
        "schema_version": price_rank.EXPECTED_SCHEMA,
        "status": "ready", "fresh": True, "generated_utc": subject.iso(now),
        "horizons": {
            horizon: {"currency_strength_bps": values}
            for horizon, values in strengths.items()
        },
        "pair_moves": pairs,
    }


def quotes_payload(now: datetime) -> dict:
    return {
        "generated_utc": subject.iso(now),
        "quotes": {
            instrument: {
                "bid": 1.0, "ask": 1.0001, "pip": 0.0001,
                "time": subject.iso(now),
            }
            for instrument in price_rank.EXPECTED_INSTRUMENTS
        },
    }


def create_quote_bars(path: Path, maturity: datetime) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE quote_bars(
          instrument TEXT, minute_epoch INTEGER, last_epoch REAL,
          close_bid REAL, close_ask REAL, pip REAL
        )
        """
    )
    for instrument in ("GBP_USD", "EUR_JPY"):
        if instrument == "GBP_USD":
            bid, ask = 0.9989, 0.9990
        else:
            bid, ask = 1.0010, 1.0011
        connection.execute(
            "INSERT INTO quote_bars VALUES (?,?,?,?,?,?)",
            (instrument, int(maturity.timestamp() // 60) * 60,
             maturity.timestamp() + 10.0, bid, ask, 0.0001),
        )
    connection.commit()
    connection.close()


def test_loader_filters_nonprospective_and_factor_dedup(tmp_path: Path) -> None:
    database = tmp_path / "source.sqlite"
    create_source_database(database)
    issued = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
    insert_source_forecast(
        database, forecast_id="a", event_id="event", episode_id="episode",
        currency="EUR", issued=issued, predicted=4.0, probability=0.7,
    )
    insert_source_forecast(
        database, forecast_id="b", event_id="event", episode_id="episode",
        currency="EUR", issued=issued, predicted=6.0, probability=0.8,
        factor_key="mechanism:rates",
    )
    insert_source_forecast(
        database, forecast_id="diagnostic", event_id="old", episode_id="old_episode",
        currency="JPY", issued=issued, predicted=-5.0, probability=0.2,
        prospective=0,
    )
    rows = subject.load_source_forecasts(database)
    assert {row["forecast_id"] for row in rows} == {"a", "b"}
    groups = subject.group_source_forecasts(rows)
    assert len(groups) == 1
    assert groups[0]["predicted_currency_factor_bps"] == 5.0
    assert groups[0]["effective_event_n"] == 9
    assert groups[0]["source_forecast_ids"] == ["a", "b"]


def test_missing_source_is_unavailable_and_source_only_abstains() -> None:
    issued = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
    groups = [{
        "market_episode_id": "episode", "currency": "EUR", "horizon_min": 5,
        "issued_utc": subject.iso(issued), "predicted_currency_factor_bps": 4.0,
        "predicted_absolute_factor_bps": 4.0, "probability_strengthening": 0.7,
        "effective_event_n": 9, "source_forecast_ids": ["a"],
        "source_contract_ids": ["c"], "source_cohort_ids": ["k"],
        "canonical_event_ids": ["e"], "factor_keys": ["f"],
    }]
    state = subject.active_currency_source_state(groups, cutoff=issued, horizon_min=5)
    assert set(state) == {"EUR"}
    assert "JPY" not in state
    result = subject.build_source_only_arm(
        state, quotes_payload(issued), cutoff=issued,
        entry_not_before=issued, max_quote_age_sec=90.0,
        max_source_capture_lag_sec=120.0,
    )
    assert result["selected"] is False
    assert result["abstain_reason"] == "insufficient_opposite_source_currencies"


def test_full_cycle_is_immutable_and_matures_executable_outcomes(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    ticker = tmp_path / "ticker.json"
    quotes = tmp_path / "quotes.json"
    bars = tmp_path / "bars.sqlite"
    ledger = tmp_path / "ledger.sqlite"
    state = tmp_path / "state.json"
    report = tmp_path / "report.md"
    create_source_database(source)
    observed = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
    issued = observed - timedelta(seconds=30)
    insert_source_forecast(
        source, forecast_id="eur", event_id="eur_event", episode_id="eur_episode",
        currency="EUR", issued=issued, predicted=4.0, probability=0.75,
    )
    insert_source_forecast(
        source, forecast_id="jpy", event_id="jpy_event", episode_id="jpy_episode",
        currency="JPY", issued=issued, predicted=-4.0, probability=0.25,
    )
    ticker.write_text(json.dumps(ticker_payload(observed)), encoding="utf-8")
    quotes.write_text(json.dumps(quotes_payload(observed)), encoding="utf-8")
    create_quote_bars(bars, observed + timedelta(minutes=5))
    first = subject.run_cycle(
        source_database=source, ticker_path=ticker, quotes_path=quotes,
        quote_bars_database=bars, ledger_path=ledger, state_path=state,
        report_path=report, observed_utc=observed,
    )
    assert first["new_decisions"] == 2
    assert first["ledger"]["decisions"] == 2
    assert first["ledger"]["adapter_cohort_count"] == 1
    assert "arms" not in first["ledger"]
    connection = sqlite3.connect(ledger)
    try:
        assert connection.execute("SELECT COUNT(*) FROM rank_forecast").fetchone()[0] == 6
        assert connection.execute(
            "SELECT COUNT(*) FROM rank_forecast WHERE arm='source_only' AND selected=1"
        ).fetchone()[0] == 2
        assert connection.execute(
            "SELECT COUNT(*) FROM rank_forecast WHERE arm='source_price_timing' AND selected=1"
        ).fetchone()[0] == 2
    finally:
        connection.close()
    second = subject.run_cycle(
        source_database=source, ticker_path=ticker, quotes_path=quotes,
        quote_bars_database=bars, ledger_path=ledger, state_path=state,
        report_path=report, observed_utc=observed + timedelta(minutes=6),
    )
    assert second["new_decisions"] == 0
    assert second["matured_outcomes"] >= 4
    connection = sqlite3.connect(ledger)
    try:
        outcomes = connection.execute(
            "SELECT executable_after_cost_pips,realized_cost_pips FROM rank_outcome"
        ).fetchall()
        assert outcomes
        assert all(row[1] > 0.0 for row in outcomes)
        assert connection.execute("PRAGMA quick_check(1)").fetchone()[0] == "ok"
    finally:
        connection.close()


def test_module_has_no_execution_or_authorization_imports() -> None:
    tree = ast.parse(Path(subject.__file__).read_text(encoding="utf-8"))
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    forbidden = ("executor", "execution", "broker", "signal_feed", "lifecycle", "authorization")
    assert not any(any(word in name for word in forbidden) for name in imported)


def test_manifest_freezes_research_boundary_and_contract() -> None:
    manifest = json.loads(subject.DEFAULT_MANIFEST.read_text(encoding="utf-8"))
    assert manifest["contract_id"] == subject.CONTRACT_ID
    assert manifest["base_cohort_id"] == subject.BASE_COHORT_ID
    assert manifest["research_only"] is True
    assert manifest["execution_eligible"] is False
    assert manifest["can_place_orders"] is False
    assert manifest["can_authorize"] is False
    assert manifest["can_promote"] is False
    assert manifest["source"]["required_prospective_proof_eligible"] is True
    assert manifest["source"]["sealed_v1_fallback_allowed"] is False
    assert "news_technical_watchlist" in manifest["explicit_non_dependencies"]


def test_default_source_is_v2_only_and_missing_v2_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing_v2 = tmp_path / "missing_v2.sqlite"
    sealed_v1 = tmp_path / "causal_source_factor_response_map_v1.sqlite"
    create_source_database(sealed_v1)
    monkeypatch.setattr(subject, "SOURCE_V2", missing_v2)
    assert subject.resolve_source_database() == missing_v2.resolve()
    assert subject.resolve_source_database().exists() is False
    assert subject.load_source_forecasts(subject.resolve_source_database()) == []
    assert subject.resolve_source_database(sealed_v1) == sealed_v1.resolve()


def test_pre_source_quote_is_rejected_by_all_comparison_arms(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    ticker = tmp_path / "ticker.json"
    quotes = tmp_path / "quotes.json"
    ledger = tmp_path / "ledger.sqlite"
    state = tmp_path / "state.json"
    report = tmp_path / "report.md"
    create_source_database(source)
    observed = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
    issued = observed - timedelta(seconds=30)
    quote_time = issued - timedelta(seconds=1)
    insert_source_forecast(
        source, forecast_id="eur", event_id="eur_event", episode_id="eur_episode",
        currency="EUR", issued=issued, predicted=4.0, probability=0.75,
    )
    insert_source_forecast(
        source, forecast_id="jpy", event_id="jpy_event", episode_id="jpy_episode",
        currency="JPY", issued=issued, predicted=-4.0, probability=0.25,
    )
    ticker.write_text(json.dumps(ticker_payload(observed)), encoding="utf-8")
    quotes.write_text(json.dumps(quotes_payload(quote_time)), encoding="utf-8")
    quote, reason = subject.quote_for_instrument(
        quotes_payload(quote_time), "EUR_JPY", cutoff=observed,
        not_before=issued, max_age_sec=90.0, max_source_capture_lag_sec=120.0,
    )
    assert quote is None
    assert reason == "quote_before_source_knowledge"
    snapshot = subject.run_cycle(
        source_database=source, ticker_path=ticker, quotes_path=quotes,
        quote_bars_database=tmp_path / "missing_bars.sqlite",
        ledger_path=ledger, state_path=state, report_path=report,
        observed_utc=observed,
    )
    assert snapshot["new_decisions"] == 2
    connection = sqlite3.connect(ledger)
    try:
        selected = connection.execute("SELECT SUM(selected) FROM rank_forecast").fetchone()[0]
        assert int(selected or 0) == 0
        assert connection.execute("SELECT COUNT(*) FROM rank_forecast").fetchone()[0] == 6
    finally:
        connection.close()


def test_source_only_rejects_quote_before_newer_contributing_leg() -> None:
    cutoff = datetime(2026, 8, 28, 12, 1, tzinfo=UTC)
    first_known = cutoff - timedelta(seconds=50)
    quote_time = cutoff - timedelta(seconds=25)
    second_known = cutoff - timedelta(seconds=10)
    source_state = {
        "EUR": {
            "currency": "EUR", "direction_state": "strengthen",
            "predicted_currency_factor_bps": 5.0,
            "knowledge_cutoff_utc": subject.iso(first_known),
            "source_forecast_ids": ["eur"], "market_episode_ids": ["eur_episode"],
        },
        "JPY": {
            "currency": "JPY", "direction_state": "weaken",
            "predicted_currency_factor_bps": -5.0,
            "knowledge_cutoff_utc": subject.iso(second_known),
            "source_forecast_ids": ["jpy"], "market_episode_ids": ["jpy_episode"],
        },
    }
    result = subject.build_source_only_arm(
        source_state, quotes_payload(quote_time), cutoff=cutoff,
        entry_not_before=first_known, max_quote_age_sec=90.0,
        max_source_capture_lag_sec=120.0,
    )
    assert result["selected"] is False
    assert result["abstain_reason"] == "source_gap_does_not_clear_stressed_cost"


def test_ledger_summary_does_not_mix_adapter_cohorts(tmp_path: Path) -> None:
    connection = subject.open_ledger(tmp_path / "cohorts.sqlite")
    try:
        for index, (cohort, pnl) in enumerate((("cohort_a", 10.0), ("cohort_b", -10.0))):
            decision = f"decision_{index}"
            forecast = f"forecast_{index}"
            connection.execute(
                "INSERT INTO rank_decision VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,0,0,0,?)",
                (
                    decision, f"episode_{index}", 5, "2026-08-28T12:00:00+00:00",
                    "source", "[]", "[]", cohort, "ticker", "quotes", "{}", "{}",
                    subject.CONTRACT_ID,
                ),
            )
            connection.execute(
                "INSERT INTO rank_forecast VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,?,?)",
                (
                    forecast, decision, "source_only", 5,
                    "2026-08-28T12:00:00+00:00", "2026-08-28T12:05:00+00:00",
                    1, "EUR_JPY", "buy", "2026-08-28T12:00:00+00:00",
                    1.0, 1.0001, 0.0001, 2.0, 1.0, "", "{}",
                    subject.CONTRACT_ID, cohort,
                ),
            )
            connection.execute(
                "INSERT INTO rank_outcome VALUES (?,?,?,?,?,?,?,?,?,?,?,1,0,?)",
                (
                    forecast, "2026-08-28T12:05:00+00:00",
                    "2026-08-28T12:05:01+00:00", 1.0, 1.0, 1.0001,
                    pnl, pnl, 1.0, int(pnl > 0.0), "{}", subject.CONTRACT_ID,
                ),
            )
        connection.commit()
        summary = subject.ledger_summary(connection)
        assert "arms" not in summary
        assert summary["adapter_cohort_count"] == 2
        by_cohort = {row["adapter_cohort_id"]: row for row in summary["cohorts"]}
        assert by_cohort["cohort_a"]["arms"][0]["mean_after_cost_pips"] == 10.0
        assert by_cohort["cohort_b"]["arms"][0]["mean_after_cost_pips"] == -10.0
    finally:
        connection.close()


def test_ledger_tables_reject_update_and_delete(tmp_path: Path) -> None:
    ledger = tmp_path / "append_only.sqlite"
    connection = subject.open_ledger(ledger)
    try:
        connection.execute(
            """
            INSERT INTO rank_decision VALUES (
              'decision','episode',5,'2026-08-28T12:00:00+00:00','source',
              '[]','[]','cohort','ticker_hash','quotes_hash','{}','{}',
              1,0,0,0,?
            )
            """,
            (subject.CONTRACT_ID,),
        )
        connection.execute(
            """
            INSERT INTO rank_forecast VALUES (
              'forecast','decision','price_only',5,
              '2026-08-28T12:00:00+00:00','2026-08-28T12:05:00+00:00',
              0,'','','',NULL,NULL,NULL,NULL,NULL,'fixture','{}',1,0,?,?
            )
            """,
            (subject.CONTRACT_ID, subject.BASE_COHORT_ID),
        )
        connection.execute(
            """
            INSERT INTO rank_outcome VALUES (
              'forecast','2026-08-28T12:05:00+00:00',
              '2026-08-28T12:05:01+00:00',1.0,1.0,1.1,0.0,-1.0,1.0,0,
              '{}',1,0,?
            )
            """,
            (subject.CONTRACT_ID,),
        )
        connection.commit()
        for table in ("rank_decision", "rank_forecast", "rank_outcome"):
            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                connection.execute(f"UPDATE {table} SET contract_id='mutated'")
            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                connection.execute(f"DELETE FROM {table}")
        assert connection.execute("PRAGMA quick_check(1)").fetchone()[0] == "ok"
    finally:
        connection.close()


def test_atomic_write_retries_transient_windows_replace_denial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "rank.json"
    real_replace = subject.os.replace
    attempts: list[tuple[Path, Path]] = []

    def flaky_replace(source: Path, destination: Path) -> None:
        attempts.append((Path(source), Path(destination)))
        if len(attempts) == 1:
            raise PermissionError("transient target lock")
        real_replace(source, destination)

    monkeypatch.setattr(subject.os, "replace", flaky_replace)
    monkeypatch.setattr(subject.time, "sleep", lambda _seconds: None)

    subject.atomic_write(target, "published\n")

    assert target.read_text(encoding="utf-8") == "published\n"
    assert len(attempts) == 2
    assert attempts[0][0] == attempts[1][0]
    assert attempts[0][0].name.startswith(f".{target.name}.")
    assert not list(tmp_path.glob("*.tmp"))


def test_atomic_write_persistent_denial_is_bounded_and_cleans_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "rank.json"
    attempts = 0

    def denied_replace(_source: Path, _destination: Path) -> None:
        nonlocal attempts
        attempts += 1
        raise PermissionError("persistent target lock")

    monkeypatch.setattr(subject.os, "replace", denied_replace)
    monkeypatch.setattr(subject.time, "sleep", lambda _seconds: None)

    with pytest.raises(PermissionError, match="persistent target lock"):
        subject.atomic_write(target, "unpublished\n")

    assert attempts == 8
    assert not target.exists()
    assert not list(tmp_path.glob("*.tmp"))


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
