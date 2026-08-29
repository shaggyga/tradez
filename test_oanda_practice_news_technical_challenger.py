from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import oanda_practice_news_technical_challenger as challenger


def row(now: datetime, **updates):
    value = {
        "arm": "news_technical_confirmed",
        "cohort_id": "news-cohort-1",
        "episode_id": "news_episode_abc",
        "currency": "JPY",
        "instrument": "USD_JPY",
        "direction": "short",
        "news_direction": "short",
        "technical_direction": "short",
        "news_confidence": 0.62,
        "technical_confidence": 0.58,
        "technical_expected_net_pips": 0.2,
        "news_thesis_state": "price_aligned",
        "persistent_sma_confirmation": {"state": "aligned_persistent"},
        "initial_reaction": {"state": "aligned", "signed_move_pips": 2.2},
        "news_factor_first_known_utc": (now - timedelta(minutes=3)).isoformat(),
        "news_factor_expires_utc": (now + timedelta(minutes=30)).isoformat(),
        "news_factor_source_ids": ["boj_updates"],
        "news_factor_publisher_count": 1,
        "news_factor_article_count": 1,
        "entry_quote": {"fresh": True, "spread_pips": 1.2},
        "headlines": ["BOJ updates policy guidance"],
    }
    value.update(updates)
    return value


def test_selector_routes_only_fresh_news_with_aligned_technical_timing():
    now = datetime(2026, 8, 19, 16, 0, tzinfo=timezone.utc)
    args = challenger.parse_args([])
    accepted, rejected = challenger.ranked_rows(
        {"status": "ok", "market_state": "fresh", "watchlist": [row(now)]},
        now,
        args,
    )
    assert len(accepted) == 1
    assert not rejected


def test_selector_rejects_uncorroborated_secondary_and_conflicted_technical():
    now = datetime(2026, 8, 19, 16, 0, tzinfo=timezone.utc)
    args = challenger.parse_args([])
    secondary = row(
        now,
        news_factor_source_ids=["google_news_systemic_catalyst"],
        uncorroborated_research=True,
    )
    conflicted = row(now, technical_direction="long")
    accepted, rejected = challenger.ranked_rows(
        {"status": "ok", "market_state": "fresh", "watchlist": [secondary, conflicted]},
        now,
        args,
    )
    assert accepted == []
    assert rejected["source_not_official_or_corroborated"] == 1
    assert rejected["technical_direction_not_aligned"] == 1


def test_aggressive_practice_switch_allows_labeled_secondary_with_technical_confirmation():
    now = datetime(2026, 8, 19, 16, 0, tzinfo=timezone.utc)
    args = challenger.parse_args(["--allow-uncorroborated-secondary"])
    secondary = row(
        now,
        arm="uncorroborated_news_response_h15",
        news_factor_source_ids=["google_news_systemic_catalyst"],
        news_factor_article_count=1,
        news_factor_publisher_count=1,
        uncorroborated_research=True,
        news_confidence=0.38,
        technical_confidence=0.49,
    )
    accepted, rejected = challenger.ranked_rows(
        {"status": "ok", "market_state": "fresh", "watchlist": [secondary]},
        now,
        args,
    )
    assert len(accepted) == 1
    assert not rejected


def test_official_price_reaction_can_time_entry_when_technical_is_neutral():
    now = datetime(2026, 8, 19, 16, 0, tzinfo=timezone.utc)
    args = challenger.parse_args(["--allow-official-price-reaction-timing"])
    official = row(
        now,
        technical_direction="neutral",
        technical_confidence=None,
        news_thesis_state="awaiting_technical_confirmation",
        persistent_sma_confirmation={"state": "not_available"},
    )
    accepted, rejected = challenger.ranked_rows(
        {"status": "ok", "market_state": "fresh", "watchlist": [official]},
        now,
        args,
    )
    assert len(accepted) == 1
    assert not rejected


def test_single_source_secondary_still_requires_technical_alignment():
    now = datetime(2026, 8, 19, 16, 0, tzinfo=timezone.utc)
    args = challenger.parse_args(
        [
            "--allow-uncorroborated-secondary",
            "--allow-official-price-reaction-timing",
        ]
    )
    secondary = row(
        now,
        technical_direction="neutral",
        technical_confidence=None,
        news_factor_source_ids=["google_news_systemic_catalyst"],
        uncorroborated_research=True,
    )
    accepted, rejected = challenger.ranked_rows(
        {"status": "ok", "market_state": "fresh", "watchlist": [secondary]},
        now,
        args,
    )
    assert accepted == []
    assert rejected["technical_direction_not_aligned"] == 1


def test_selector_deduplicates_correlated_pair_legs_per_currency_episode():
    now = datetime(2026, 8, 19, 16, 0, tzinfo=timezone.utc)
    args = challenger.parse_args([])
    second = row(
        now,
        instrument="EUR_JPY",
        entry_quote={"fresh": True, "spread_pips": 1.8},
        technical_confidence=0.55,
    )
    accepted, rejected = challenger.ranked_rows(
        {"status": "ok", "market_state": "fresh", "watchlist": [row(now), second]},
        now,
        args,
    )
    assert len(accepted) == 1
    assert accepted[0]["instrument"] == "USD_JPY"
    assert rejected["same_factor_episode_deduplicated"] == 1


def test_candidate_uses_live_quote_and_clears_cost_for_fast_rotation_policy():
    now = datetime(2026, 8, 19, 16, 0, tzinfo=timezone.utc)
    args = challenger.parse_args([])
    candidate, reason = challenger.candidate_from_row(
        row(now),
        SimpleNamespace(bid=147.100, ask=147.112, time=now.isoformat()),
        {"pip_location": -2},
        args,
    )
    assert reason == ""
    assert candidate is not None
    assert candidate["direction"] == "sell"
    assert candidate["lane_id"].startswith("news006.")
    assert candidate["execution_exit_horizon_sec"] == 900
    assert candidate["gross_to_spread"] >= 1.25
    configured = challenger.configure_executor_args(args)
    assert configured.execution_dynamic_sizing is False
    assert configured.execution_units == 100
    assert configured.execution_max_units == 100
    assert configured.execution_max_open_positions == 8
    assert args.max_daily_fills == 48
    assert args.reentry_cooldown_sec == 0.0
    assert configured.execution_max_currency_direction_positions == 8
    assert configured.execution_max_open_jpy_factor_positions == 8


def test_repeat_episode_pair_removes_post_exit_cooldown_but_throttles_failed_retry(tmp_path):
    now = datetime.now(timezone.utc)
    value = row(now)
    ledger = challenger.open_ledger(tmp_path / "ledger.sqlite")
    try:
        challenger.record_attempt(
            ledger,
            value,
            "filled",
            True,
            allow_repeat=True,
        )
        assert challenger.already_attempted(
            ledger,
            value,
            allow_repeat=True,
            retry_sec=10.0,
        )
        assert not challenger.already_attempted(
            ledger,
            value,
            allow_repeat=True,
            retry_sec=0.0,
        )
    finally:
        ledger.close()


def test_trade_ownership_is_disjoint_from_governed_executor():
    executor = object.__new__(challenger.NewsTechnicalPracticeExecutor)
    owned = {
        "clientExtensions": {
            "tag": "strategy_lab_top",
            "comment": "news006.news_technical_confirmed.abc|h900",
        }
    }
    other = {
        "clientExtensions": {
            "tag": "strategy_lab_top",
            "comment": "momentum.fast|h900",
        }
    }
    assert executor.owns_trade(owned) is True
    assert executor.owns_trade(other) is False


def test_final_submission_boundary_is_exactly_practice_006_and_100_units(monkeypatch):
    executor = object.__new__(challenger.NewsTechnicalPracticeExecutor)
    executor.account_id = "101-001-37981792-006"
    candidate = {
        "lane_id": "news006.news_technical_confirmed.abc",
        "source_news_arm": "news_technical_confirmed",
    }
    monkeypatch.setattr(challenger.lab, "BASE_URL", "https://api-fxpractice.oanda.com")
    assert executor.final_submission_blocker(candidate, 100, {}) == ""
    assert executor.final_submission_blocker(candidate, 101, {}) == "news_challenger_units_not_fixed_100"
    executor.account_id = "101-001-37981792-007"
    assert executor.final_submission_blocker(candidate, 100, {}) == "news_challenger_not_account_006"


def test_portfolio_capacity_is_eight_not_one():
    args = challenger.parse_args(["--allow-uncorroborated-secondary"])
    executor = object.__new__(challenger.NewsTechnicalPracticeExecutor)
    executor.args = challenger.configure_executor_args(args)
    candidate = {"instrument": "NX_NY", "direction": "buy"}
    seven = [
        {"instrument": f"A{i}_B{i}", "currentUnits": "100"}
        for i in range(7)
    ]
    eight = seven + [{"instrument": "C8_D8", "currentUnits": "100"}]
    assert executor.portfolio_blocker(candidate, seven) == ""
    assert executor.portfolio_blocker(candidate, eight) == "max_open_positions"


def test_heartbeat_tracks_dedicated_account_position_and_protection(tmp_path):
    class FakeExecutor:
        fills = 1
        disabled_reason = ""

        @staticmethod
        def owns_trade(trade):
            return str((trade.get("clientExtensions") or {}).get("comment") or "").startswith("news006.")

    args = challenger.parse_args(["--allow-uncorroborated-secondary"])
    output = tmp_path / "heartbeat.json"
    ledger = challenger.open_ledger(tmp_path / "ledger.sqlite")
    try:
        challenger.write_state(
            output,
            status="running",
            account_id="101-001-37981792-006",
            accepted=[],
            rejected=Counter(),
            executor=FakeExecutor(),
            connection=ledger,
            attempts=1,
            technical_exits=0,
            errors=0,
            args=args,
            account_summary={
                "balance": "49.1",
                "NAV": "49.2",
                "pendingOrderCount": "1",
            },
            open_trades=[
                {
                    "id": "802",
                    "instrument": "AUD_USD",
                    "currentUnits": "-100",
                    "price": "0.71215",
                    "unrealizedPL": "0.01",
                    "clientExtensions": {"comment": "news006.arm.case|h300"},
                    "stopLossOrder": {"id": "803"},
                }
            ],
        )
    finally:
        ledger.close()
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["account"]["open_trade_count"] == 1
    assert payload["account"]["owned_open_trade_count"] == 1
    assert payload["account"]["pending_order_count"] == 1
    assert payload["account"]["positions"][0]["direction"] == "short"
    assert payload["account"]["positions"][0]["stop_loss_order_id"] == "803"
