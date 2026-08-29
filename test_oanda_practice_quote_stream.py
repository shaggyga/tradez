from pathlib import Path

import oanda_practice_quote_stream as quotes


def test_quote_stream_has_no_order_capability():
    assert quotes.CAN_PLACE_ORDERS is False
    assert quotes.ACCOUNT_SCOPE == "practice_007_read_only_quotes"


def test_quote_stream_runs_with_read_only_components(monkeypatch, tmp_path):
    events = []

    class FakeHeartbeat:
        def start(self):
            return self

        def update(self, **fields):
            events.append(fields)

        def close(self):
            events.append({"closed": True})

    class FakeClient:
        def __init__(self, token):
            self.token = token

        def close(self):
            events.append({"client_closed": True})

    class FakeStream:
        def __init__(self, *args, **kwargs):
            events.append({"producer": kwargs["research_snapshot_producer"]})
            events.append(
                {"minimum_quotes": kwargs["research_snapshot_min_instruments"]}
            )

        def start(self):
            events.append({"stream_started": True})

        def wait_ready(self, timeout):
            return True

        def stats(self):
            return {"connected": True, "quoted_instruments": 68}

        def stop(self):
            events.append({"stream_stopped": True})

    monkeypatch.setattr(quotes.lab, "WorkerHeartbeat", lambda *a, **k: FakeHeartbeat())
    monkeypatch.setattr(quotes.lab, "read_credentials", lambda *a, **k: ("token", "101-001-37981792-007"))
    monkeypatch.setattr(quotes.lab, "MarketDataClient", FakeClient)
    monkeypatch.setattr(quotes.lab, "selected_instruments", lambda *a, **k: ["EUR_USD"])
    monkeypatch.setattr(quotes.lab, "account_pip_sizes", lambda *a, **k: {"EUR_USD": 0.0001})
    monkeypatch.setattr(quotes.lab, "MultiPriceStream", FakeStream)
    monkeypatch.setattr(quotes.lab, "close_log_handles", lambda: None)

    result = quotes.main(
        [
            "--duration-sec",
            "1",
            "--heartbeat-state",
            str(tmp_path / "heartbeat.json"),
            "--research-market-quote-snapshot",
            str(tmp_path / "quotes.json"),
        ]
    )

    assert result == 0
    assert {"producer": quotes.PRODUCER} in events
    assert {"minimum_quotes": 1} in events
    assert {"stream_started": True} in events
    assert {"stream_stopped": True} in events
    assert not any(event.get("can_place_orders") for event in events)
