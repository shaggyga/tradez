from __future__ import annotations

import datetime as dt
import tempfile
from pathlib import Path
from types import SimpleNamespace

import oanda_gpt_prod_live_account_manager as live


UTC = dt.timezone.utc


def _raw_watch(**overrides):
    watch = {
        "headline": "Central bank announces an unexpected policy change",
        "summary": "The decision materially changes the near-term rate path.",
        "source_name": "Central bank",
        "source_url": "https://example.test/policy-update",
        "published_utc": "2026-07-13T11:55:00+00:00",
        "reported_update_utc": "2026-07-13T11:56:00+00:00",
        "freshness_minutes": 4,
        "category": "central_bank",
        "currencies": ["GBP"],
        "pair_hints": ["GBP/USD"],
        "severity": 92,
        "movement_potential": "EXTREME",
        "directional_bias": {"GBP": "UNKNOWN"},
        "why_market_moving": "The policy path changed unexpectedly.",
        "technical_confirmation_to_watch_for": ["M1 basket break and retest"],
        "expires_minutes": 180,
    }
    watch.update(overrides)
    return watch


def test_normalize_news_watch_accepts_fresh_high_severity_source() -> None:
    now = dt.datetime(2026, 7, 13, 12, 0, tzinfo=UTC)

    watch = live.normalize_news_watch_item(_raw_watch(), now=now)

    assert watch is not None
    assert watch["watch_id"].startswith("news_")
    assert watch["currencies"] == ["GBP"]
    assert watch["pair_hints"] == ["GBP_USD"]
    assert watch["requires_technical_confirmation"] is True
    assert watch["first_seen_latency_minutes"] == 4.0


def test_normalize_news_watch_filters_unavailable_pair_hints_when_supplied() -> None:
    now = dt.datetime(2026, 7, 13, 12, 0, tzinfo=UTC)

    rejected = live.normalize_news_watch_item(
        _raw_watch(pair_hints=["USD/IRR"], currencies=["USD", "IRR"]),
        now=now,
        available_instruments=["EUR_USD", "USD_CHF"],
    )
    accepted = live.normalize_news_watch_item(
        _raw_watch(pair_hints=["USD/CHF"], currencies=["USD", "CHF"]),
        now=now,
        available_instruments=["EUR_USD", "USD_CHF"],
    )
    currency_only = live.normalize_news_watch_item(
        _raw_watch(pair_hints=[], currencies=["USD", "JPY"]),
        now=now,
        available_instruments=["EUR_USD", "USD_CHF"],
    )

    assert rejected is None
    assert accepted is not None
    assert accepted["pair_hints"] == ["USD_CHF"]
    assert currency_only is not None
    assert currency_only["pair_hints"] == []


def test_filter_news_watches_removes_persisted_unavailable_pair_hints() -> None:
    watches = [
        {"watch_id": "bad", "pair_hints": ["USD_IRR"], "currencies": ["USD", "IRR"]},
        {"watch_id": "good", "pair_hints": ["USD/CHF"], "currencies": ["USD", "CHF"]},
        {"watch_id": "currency_only", "pair_hints": [], "currencies": ["USD", "JPY"]},
    ]

    filtered = live.filter_news_watches_for_available_instruments(
        watches,
        available_instruments=["EUR_USD", "USD_CHF"],
    )

    assert [watch["watch_id"] for watch in filtered] == ["good", "currency_only"]
    assert filtered[0]["pair_hints"] == ["USD_CHF"]
    assert filtered[1]["pair_hints"] == []


def test_normalize_news_watch_rejects_stale_or_low_severity_items() -> None:
    now = dt.datetime(2026, 7, 13, 12, 0, tzinfo=UTC)

    assert live.normalize_news_watch_item(
        _raw_watch(reported_update_utc="2026-07-13T10:00:00+00:00"),
        now=now,
    ) is None
    assert live.normalize_news_watch_item(_raw_watch(severity=55), now=now) is None
    assert live.normalize_news_watch_item(_raw_watch(source_url=""), now=now) is None


def test_strict_news_watch_requires_actual_citation_and_corroboration() -> None:
    now = dt.datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    item = _raw_watch(
        corroborating_sources=[
            {
                "source_name": "Second source",
                "source_url": "https://second.example.test/corroboration",
            }
        ]
    )
    citations = [
        {"url": item["source_url"], "title": item["headline"]},
        {
            "url": "https://second.example.test/corroboration",
            "title": "Independent corroboration",
        },
    ]

    accepted = live.normalize_news_watch_item(
        item,
        now=now,
        verified_citations=citations,
        require_corroboration=True,
    )
    rejected = live.normalize_news_watch_item(
        item,
        now=now,
        verified_citations=citations[:1],
        require_corroboration=True,
    )

    assert accepted is not None
    assert accepted["source_verified"] is True
    assert accepted["corroboration_count"] == 1
    assert rejected is None


def test_official_source_can_arm_without_second_source() -> None:
    now = dt.datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    item = _raw_watch(
        source_name="Federal Reserve",
        source_url="https://www.federalreserve.gov/newsevents/pressreleases/test.htm",
        corroborating_sources=[],
    )

    watch = live.normalize_news_watch_item(
        item,
        now=now,
        verified_citations=[{"url": item["source_url"], "title": item["headline"]}],
        require_corroboration=True,
    )

    assert watch is not None
    assert watch["source_verified"] is True
    assert watch["corroboration_count"] == 0


def test_extract_web_search_citations_ignores_model_json_urls() -> None:
    payload = {
        "output": [
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": '{"source_url":"https://invented.example.test/story"}',
                        "annotations": [
                            {
                                "type": "url_citation",
                                "url": "https://verified.example.test/story",
                                "title": "Verified result",
                            }
                        ],
                    }
                ],
            }
        ]
    }

    citations = live.extract_web_search_citations(payload)

    assert citations == [
        {
            "url": "https://verified.example.test/story",
            "title": "Verified result",
        }
    ]


def test_citation_match_replaces_same_source_url_path_only_with_title_overlap() -> None:
    matched = live.match_verified_web_citation(
        "https://www.reuters.com/invented-old-path",
        "Reuters",
        "Dollar rises as Middle East tensions intensify",
        [
            {
                "url": "https://www.reuters.com/markets/currencies/dollar-rises-middle-east-2026-07-13/",
                "title": "Dollar rises as Middle East tensions intensify",
            },
            {
                "url": "https://www.reuters.com/world/unrelated-story/",
                "title": "Unrelated agricultural report",
            },
        ],
    )

    assert matched is not None
    assert "markets/currencies" in matched["url"]


def test_openai_response_wrapper_returns_parsed_json_and_raw_metadata() -> None:
    class FakeResponse:
        status_code = 200
        text = ""

        def json(self):
            return {"output_text": '{"status":"ok"}', "output": []}

        def close(self):
            return None

    class FakeSession:
        def post(self, *args, **kwargs):
            return FakeResponse()

    client = object.__new__(live.advisor.OpenAIClient)
    client.cfg = SimpleNamespace(
        http_max_retries=0,
        disable_http_keepalive=False,
        openai_timeout_seconds=10,
        http_retry_sleep_seconds=0,
    )
    client.s = FakeSession()

    parsed, raw = client._post_response_with_raw({"model": "test"})

    assert parsed == {"status": "ok"}
    assert raw["output_text"] == '{"status":"ok"}'


def test_openai_response_wrapper_retries_truncated_output_json(monkeypatch) -> None:
    class FakeResponse:
        status_code = 200
        text = ""

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

        def close(self):
            return None

    class FakeSession:
        def __init__(self):
            self.calls = 0

        def post(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return FakeResponse(
                    {
                        "output_text": '{"status":"unterminated',
                        "output": [],
                        "status": "incomplete",
                        "incomplete_details": {"reason": "max_output_tokens"},
                    }
                )
            return FakeResponse({"output_text": '{"status":"ok"}', "output": []})

    client = object.__new__(live.advisor.OpenAIClient)
    client.cfg = SimpleNamespace(
        http_max_retries=1,
        disable_http_keepalive=False,
        openai_timeout_seconds=10,
        http_retry_sleep_seconds=0,
    )
    client.s = FakeSession()
    monkeypatch.setattr(live.advisor.time, "sleep", lambda _: None)

    parsed, raw = client._post_response_with_raw({"model": "test"})

    assert client.s.calls == 2
    assert parsed == {"status": "ok"}
    assert raw["output_text"] == '{"status":"ok"}'


def test_merge_news_watches_preserves_first_seen_and_deduplicates() -> None:
    first = dt.datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    watch = live.normalize_news_watch_item(_raw_watch(), now=first)
    assert watch is not None

    initial = live.merge_news_watch_records([], [watch], {}, now=first)
    refreshed_watch = dict(watch)
    refreshed_watch["first_seen_utc"] = "2026-07-13T12:05:00+00:00"
    second = live.merge_news_watch_records(
        initial["active"],
        [refreshed_watch],
        initial["seen"],
        now=first + dt.timedelta(minutes=5),
    )

    assert len(initial["new"]) == 1
    assert second["new"] == []
    assert len(second["refreshed"]) == 1
    assert second["active"][0]["first_seen_utc"] == watch["first_seen_utc"]


def test_news_watch_due_uses_configured_interval() -> None:
    now = dt.datetime(2026, 7, 13, 12, 10, tzinfo=UTC)

    assert live.news_watch_scan_due_at("2026-07-13T12:04:59+00:00", 300, now=now)
    assert not live.news_watch_scan_due_at("2026-07-13T12:06:00+00:00", 300, now=now)


def _confirmed_order(direction: str = "LONG") -> dict:
    return {
        "instrument": "GBP_USD",
        "action": "OPEN",
        "direction": direction,
        "expected_R": 1.4,
        "stop_loss": 1.31,
        "news_watch_id": "news_test",
        "technical_confirmation": {
            "status": "CONFIRMED",
            "timeframe": "M1",
            "trigger": "break and retest",
            "evidence": "two related GBP pairs broke with expanding range",
            "invalidation_level": 1.31,
            "basket_confirmation_pairs": ["GBP_USD", "EUR_GBP"],
            "exhaustion_check": "entry is on the retest, not the impulse high",
        },
    }


def test_news_entry_gate_requires_matching_local_direction() -> None:
    manager = object.__new__(live.LiveGPTProdManager)
    manager.cfg = SimpleNamespace(enable_openai_web_search=True)
    packet = {
        "active_news_watches": [
            {
                "watch_id": "news_test",
                "currencies": ["GBP"],
                "pair_hints": ["GBP_USD"],
            }
        ],
        "news_watch_technical_trigger": {
            "signals": [
                {
                    "instrument": "GBP_USD",
                    "direction": "LONG",
                    "news_watch_ids": ["news_test"],
                }
            ]
        },
    }
    approved = {"orders_to_execute": [_confirmed_order("LONG")]}
    blocked = {"orders_to_execute": [_confirmed_order("SHORT")]}

    manager.apply_news_watch_technical_entry_gate(
        approved,
        packet,
        reason="event_trigger:GBP_STRENGTH",
    )
    manager.apply_news_watch_technical_entry_gate(
        blocked,
        packet,
        reason="event_trigger:GBP_STRENGTH",
    )

    assert len(approved["orders_to_execute"]) == 1
    assert approved["orders_to_execute"][0]["news_watch_technical_gate"] == "approved"
    assert blocked["orders_to_execute"] == []
    assert blocked["new_trade_candidates"][0]["action"] == "WATCH"


def test_technical_confirmation_rejects_missing_exhaustion_check() -> None:
    order = _confirmed_order()
    order["technical_confirmation"].pop("exhaustion_check")

    ok, reason = live.technical_confirmation_complete(order)

    assert not ok
    assert "exhaustion_check" in reason


def test_live_decision_quality_flags_hold_inside_orders_to_execute() -> None:
    review = live.live_decision_quality_review(
        {
            "orders_to_execute": [
                {
                    "action": "HOLD",
                    "instrument": "USD_CHF",
                    "direction": "LONG",
                    "trade_id": "5",
                }
            ],
            "underdeployment_reason": "Event risk blocks additional entries.",
        }
    )

    assert review["local_verdict"] == "retry_decision_quality"
    assert review["invalid_order_actions"][0]["action"] == "HOLD"
    assert any("non-executable" in issue for issue in review["issues"])


def test_live_decision_quality_rejects_vague_max_allocation_blocker() -> None:
    review = live.live_decision_quality_review(
        {
            "orders_to_execute": [],
            "underdeployment_reason": "No new open recommended because max allocation achieved.",
            "risk_notes": [],
        }
    )

    assert review["local_verdict"] == "retry_decision_quality"
    assert review["vague_flat_reason"] is not None


def test_live_decision_quality_requires_candidate_or_underdeployment_reason_when_flat() -> None:
    review = live.live_decision_quality_review(
        {
            "orders_to_execute": [],
            "new_trade_candidates": [],
            "risk_notes": ["BoJ intervention risk noted."],
            "macro_thesis_consistency": {"event_risk": "JPY intervention risk"},
        }
    )

    assert review["local_verdict"] == "retry_decision_quality"
    assert review["missing_flat_accountability"] is not None


def test_live_decision_quality_flags_candidate_usd_thesis_rationale_contradiction() -> None:
    review = live.live_decision_quality_review(
        {
            "portfolio_bias": "USD_BULLISH",
            "portfolio_mode": "USD_BULLISH",
            "market_summary": "Strong USD_RALLY across majors.",
            "orders_to_execute": [],
            "underdeployment_reason": "Thesis conflict blocks the candidate.",
            "new_trade_candidates": [
                {
                    "action": "WATCH",
                    "instrument": "AUD_USD",
                    "direction": "LONG",
                    "outlook_confidence": 70,
                    "risk_pct": 1.5,
                    "expected_R": 1.2,
                    "entry_min": 0.6925,
                    "entry_max": 0.695,
                    "stop_loss": 0.6875,
                    "take_profit": 0.705,
                    "why_now": "Technical conditions align with USD rally.",
                    "reason": "Awaiting breakout confirmation.",
                }
            ],
        }
    )

    assert review["local_verdict"] == "retry_decision_quality"
    assert review["candidate_thesis_contradictions"][0]["instrument"] == "AUD_USD"
    assert review["candidate_thesis_contradictions"][0]["candidate_usd_thesis"] == "USD_SHORT"


def test_sanitize_orders_to_execute_moves_hold_to_position_actions() -> None:
    manager = object.__new__(live.LiveGPTProdManager)
    decision = {
        "orders_to_execute": [
            {
                "action": "HOLD",
                "instrument": "USD_CHF",
                "trade_id": "5",
                "direction": "LONG",
            },
            {
                "action": "OPEN",
                "instrument": "USD_JPY",
                "direction": "LONG",
            },
        ],
        "open_position_actions": [
            {
                "action": "HOLD",
                "instrument": "USD_CHF",
                "trade_id": "5",
                "direction": "LONG",
            }
        ],
    }

    manager.sanitize_orders_to_execute_actions(
        decision,
        open_trades=[{"id": "5", "instrument": "USD_CHF"}],
    )

    assert [order["action"] for order in decision["orders_to_execute"]] == ["OPEN"]
    assert decision["open_position_actions"][0]["action"] == "HOLD"
    assert len(decision["open_position_actions"]) == 1
    assert (
        decision["orders_to_execute_action_cleanup"][0]["moved_to"]
        == "duplicate_open_position_action_dropped"
    )


def test_event_scan_due_clears_stale_latest_technical_watch_when_watch_expired() -> None:
    manager = object.__new__(live.LiveGPTProdManager)
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        live.advisor.write_json(
            data_dir / "latest_news_technical_watch.json",
            {
                "status": "checked",
                "active_watch_count": 1,
                "signal_count": 0,
                "signals": [],
            },
        )
        manager.cfg = SimpleNamespace(data_dir=data_dir)
        manager.instruments = {"USD_CHF": {}, "USD_JPY": {}}
        manager.load_state = lambda: {
            "last_news_watch_scan_utc": live.advisor.iso_utc(live.advisor.utc_now()),
            "last_news_watch_price_scan_utc": live.advisor.iso_utc(live.advisor.utc_now()),
            "active_news_watches": [
                {
                    "watch_id": "expired",
                    "source_verified": True,
                    "expires_utc": "2026-07-13T00:00:00+00:00",
                    "pair_hints": ["USD_JPY"],
                }
            ],
        }
        manager.news_watch_enabled = lambda: True
        manager.news_watch_scan_interval_seconds = lambda: 3600

        assert manager.event_scan_due() is True
        manager.run_event_scan(reason="loop")

        latest = live.advisor.read_json(data_dir / "latest_news_technical_watch.json", {})
        assert latest["status"] == "no_active_watches"
        assert latest["active_watch_count"] == 0
