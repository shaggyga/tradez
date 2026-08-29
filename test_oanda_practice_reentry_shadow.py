from pathlib import Path
from types import SimpleNamespace

import trad.oanda_practice_reentry_shadow as reentry
from trad.oanda_practice_reentry_shadow import (
    cooldown_counterfactual,
    retryable_broker_error,
)
from trad.oanda_practice_shadow_strategy_lab import OandaApiError


def test_cooldown_counterfactual_scores_rapid_loss_reentry():
    transactions = [
        {
            "id": "1",
            "time": "2026-08-03T04:50:00Z",
            "type": "MARKET_ORDER",
            "instrument": "USD_JPY",
            "units": "100",
            "clientExtensions": {
                "tag": "strategy_lab_top",
                "comment": "momentum.loose|h3600|c0.56",
            },
        },
        {
            "id": "2",
            "time": "2026-08-03T04:50:00Z",
            "type": "ORDER_FILL",
            "orderID": "1",
            "price": "156.50",
            "tradeOpened": {"tradeID": "2", "units": "100"},
        },
        {
            "id": "3",
            "time": "2026-08-03T04:57:51Z",
            "type": "ORDER_FILL",
            "reason": "STOP_LOSS_ORDER",
            "tradesClosed": [
                {"tradeID": "2", "price": "156.45", "realizedPL": "-0.04"}
            ],
        },
        {
            "id": "4",
            "time": "2026-08-03T04:57:58Z",
            "type": "MARKET_ORDER",
            "instrument": "USD_JPY",
            "units": "130",
            "clientExtensions": {
                "tag": "strategy_lab_top",
                "comment": "rank.loose|h43200|c0.60",
            },
        },
        {
            "id": "5",
            "time": "2026-08-03T04:57:58Z",
            "type": "ORDER_FILL",
            "orderID": "4",
            "price": "156.48",
            "tradeOpened": {"tradeID": "5", "units": "130"},
        },
        {
            "id": "6",
            "time": "2026-08-03T05:01:11Z",
            "type": "ORDER_FILL",
            "reason": "STOP_LOSS_ORDER",
            "tradesClosed": [
                {"tradeID": "5", "price": "156.43", "realizedPL": "-0.0411"}
            ],
        },
    ]

    result = cooldown_counterfactual(transactions, [5, 60, 300])

    assert result["episode_count"] == 2
    assert result["reentry_count"] == 1
    assert result["reentries"][0]["gap_after_prior_exit_sec"] == 7.0
    assert result["cooldown_arms"]["5"]["blocked_entries"] == 0
    assert result["cooldown_arms"]["60"]["blocked_losses"] == 1
    assert result["cooldown_arms"]["300"]["counterfactual_pl_delta_if_blocked"] == 0.0411
    assert result["reentry_direction_summary"]["same_direction"] == {
        "entries": 1,
        "resolved_entries": 1,
        "wins": 0,
        "losses": 1,
        "actual_pl": -0.0411,
        "avg_actual_pl": -0.0411,
    }
    assert result["reentry_direction_summary"]["direction_reversal"]["entries"] == 0
    assert result["cooldown_arms"]["60"]["by_reentry_type"]["same_direction"][
        "losses"
    ] == 1


def test_only_transient_broker_errors_keep_reentry_worker_alive():
    assert retryable_broker_error(OandaApiError("maintenance", status=503))
    assert retryable_broker_error(OandaApiError("rate limited", status=429))
    assert not retryable_broker_error(OandaApiError("unauthorized", status=401))
    assert not retryable_broker_error(OandaApiError("network", status=None))


def test_successful_read_clears_retained_broker_http_status(monkeypatch):
    class FakeHeartbeat:
        def __init__(self, *_args, **_kwargs):
            self.updates = []

        def start(self):
            return self

        def update(self, **fields):
            self.updates.append(fields)

        def close(self):
            return None

    heartbeat = FakeHeartbeat()
    monkeypatch.setattr(
        reentry,
        "parse_args",
        lambda _argv: SimpleNamespace(
            duration_sec=0.0,
            heartbeat_state=Path("heartbeat.json"),
            output=Path("output.json"),
        ),
    )
    monkeypatch.setattr(reentry.lab, "WorkerHeartbeat", lambda *_a, **_k: heartbeat)
    monkeypatch.setattr(
        reentry,
        "run_once",
        lambda _args: {
            "last_transaction_id": "2461",
            "episode_count": 29,
            "reentry_count": 25,
            "cooldown_arms": {},
        },
    )

    assert reentry.main([]) == 0
    observing = next(item for item in heartbeat.updates if item["phase"] == "observing")
    assert observing["broker_http_status"] is None
    assert observing["consecutive_broker_errors"] == 0
    assert observing["fail_closed"] is False
