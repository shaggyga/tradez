from __future__ import annotations

import datetime as dt

import oanda_local_sentiment_advisor as advisor


def news_pair(score: float, confidence: float, *, verified: bool = True) -> dict:
    return {
        "score": score,
        "confidence": confidence,
        "active_event_count": 2,
        "events": [
            {
                "age_minutes": 10,
                "headline": "Policy surprise",
                "pair_score": score,
                "source_name": "Official source",
                "source_verified": verified,
            },
            {
                "age_minutes": 20,
                "headline": "Policy surprise confirmed",
                "pair_score": score,
                "source_name": "Second source",
                "source_verified": verified,
            },
        ],
    }


def signal(instrument: str, probability_up: float) -> dict:
    return {
        "instrument": instrument,
        "horizon_breakdown": [
            {
                "horizon_sec": 3600,
                "raw_probability_up": probability_up,
                "paper_consensus_eligible": False,
            }
        ],
    }


def test_build_decision_ranks_sentiment_but_never_authorizes_orders() -> None:
    decision = advisor.build_decision(
        {
            "generated_utc": advisor.iso_utc(),
            "pairs": {
                "EUR_USD": news_pair(0.8, 0.8),
                "GBP_USD": news_pair(-0.5, 0.6),
            },
        },
        {
            "updated_at": advisor.iso_utc(),
            "top_signals": [
                signal("EUR_USD", 0.58),
                signal("GBP_USD", 0.44),
            ],
        },
        {
            "accounts": [
                {
                    "account_id": "101-001-123-002",
                    "balance": "50",
                    "NAV": "50",
                    "openTradeCount": 0,
                    "pendingOrderCount": 0,
                    "trades": [],
                }
            ]
        },
    )

    assert decision["decision_engine"] == "deterministic_local_sentiment_shadow"
    assert decision["new_trade_candidates"][0]["instrument"] == "EUR_USD"
    assert decision["new_trade_candidates"][0]["action"] == "WATCH"
    assert decision["orders_to_execute"] == []
    assert decision["event_permissions"] == []
    assert decision["execution_eligible"] is False


def test_unverified_only_evidence_caps_confidence() -> None:
    row = advisor.score_pair(
        "AUD_USD",
        news_pair(-0.9, 0.95, verified=False),
        {"direction": "SHORT", "confidence": 0.8},
    )

    assert row["outlook_confidence"] <= 45.0
    assert row["research_only"] is True
    assert "unverified" in row["reason"]


def test_opposing_price_confirmation_rejects_candidate() -> None:
    row = advisor.score_pair(
        "EUR_USD",
        news_pair(0.5, 0.55),
        {"direction": "SHORT", "confidence": 1.0},
    )

    assert row["action"] == "REJECT"
    assert row["price_confirmation"]["direction"] == "SHORT"


def test_open_trade_review_is_hold_only() -> None:
    decision = advisor.build_decision(
        {
            "generated_utc": advisor.iso_utc(),
            "pairs": {"AUD_NZD": news_pair(-0.7, 0.7)},
        },
        {"top_signals": [signal("AUD_NZD", 0.42)]},
        {
            "accounts": [
                {
                    "account_id": "101-001-123-002",
                    "balance": "50",
                    "NAV": "51",
                    "openTradeCount": 1,
                    "pendingOrderCount": 0,
                    "trades": [
                        {
                            "id": "42",
                            "instrument": "AUD_NZD",
                            "currentUnits": "100",
                        }
                    ],
                }
            ]
        },
    )

    assert decision["open_position_actions"][0]["action"] == "HOLD"
    assert decision["open_position_actions"][0]["outlook_confidence"] > 0
    assert decision["orders_to_execute"] == []


def test_fresh_market_ticker_can_confirm_news_when_signal_is_neutral() -> None:
    decision = advisor.build_decision(
        {
            "generated_utc": advisor.iso_utc(),
            "pairs": {"EUR_USD": news_pair(0.7, 0.7)},
        },
        {"top_signals": [signal("EUR_USD", 0.5)]},
        {"accounts": []},
        market_payload={
            "fresh": True,
            "current_regime": "USD_WEAKNESS",
            "quote_age_seconds": 30,
            "pair_moves": {
                "EUR_USD": {
                    "windows": {
                        "15": {"return_bps": 8.0},
                        "60": {"return_bps": 10.0},
                    }
                }
            },
        },
    )

    candidate = decision["new_trade_candidates"][0]
    assert candidate["price_confirmation"]["direction"] == "LONG"
    assert candidate["price_confirmation"]["state"] == "market_only"
    assert decision["input_state"]["market_regime"] == "USD_WEAKNESS"


def test_stale_news_is_hard_blocked_from_candidates() -> None:
    decision = advisor.build_decision(
        {
            "generated_utc": advisor.iso_utc(
                advisor.utc_now() - dt.timedelta(seconds=301)
            ),
            "pairs": {"EUR_USD": news_pair(0.9, 0.9)},
        },
        {"top_signals": [signal("EUR_USD", 0.7)]},
        {"accounts": []},
    )

    assert decision["portfolio_mode"] == "DATA_STALE"
    assert decision["new_trade_candidates"] == []
    assert decision["input_state"]["news_hard_blocked"] is True
    assert decision["orders_to_execute"] == []


def test_unavailable_account_is_unknown_and_does_not_create_position_review() -> None:
    decision = advisor.build_decision(
        {
            "generated_utc": advisor.iso_utc(),
            "pairs": {"EUR_USD": news_pair(0.7, 0.7)},
        },
        {"top_signals": [signal("EUR_USD", 0.6)]},
        {
            "environment": "practice",
            "accounts": [
                {
                    "account_id": "101-001-123-002",
                    "env": "practice",
                    "ok": False,
                    "account_values_current": False,
                    "positions_current": False,
                    "orders_current": False,
                    # Stale-looking fields must not leak into the review.
                    "balance": "50",
                    "NAV": "51",
                    "openTradeCount": 1,
                    "pendingOrderCount": 0,
                    "trades": [
                        {
                            "id": "42",
                            "instrument": "EUR_USD",
                            "currentUnits": "100",
                        }
                    ],
                }
            ],
            "aggregate": {
                "snapshot_state": "retained_stale_account_values",
                "account_values_current": False,
                "positions_current": False,
                "orders_current": False,
            },
        },
    )

    assert decision["account"]["account_current"] is False
    assert decision["account"]["snapshot_state"] == "retained_stale_account_values"
    assert decision["account"]["balance"] is None
    assert decision["account"]["nav"] is None
    assert decision["account"]["open_trades"] is None
    assert decision["account"]["pending_orders"] is None
    assert decision["open_position_actions"] == []
    assert decision["orders_to_execute"] == []
