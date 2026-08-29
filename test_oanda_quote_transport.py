import time

from trad import oanda_quote_transport as transport


def wait_written(publisher, generation, timeout=3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if publisher.stats()["written_generation"] >= generation:
            return
        time.sleep(0.01)
    raise AssertionError(publisher.stats())


def test_sqlite_snapshot_remains_authoritative_when_json_mirror_is_locked(
    monkeypatch,
    tmp_path,
):
    path = tmp_path / "quotes.json"
    monkeypatch.setattr(
        transport,
        "_atomic_json_mirror",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            PermissionError("dashboard lock")
        ),
    )
    publisher = transport.QuoteSnapshotPublisher(path)
    try:
        generation = publisher.submit(
            {
                "generated_utc": "2026-08-03T12:00:00+00:00",
                "producer": "test",
                "quote_count": 1,
                "quotes": {
                    "EUR_USD": {"bid": 1.1, "ask": 1.1002, "pip": 0.0001}
                },
            }
        )
        wait_written(publisher, generation)
        stats = publisher.stats()
        payload = transport.load_quote_snapshot(path)
    finally:
        publisher.close()

    assert stats["mirror_error"].startswith("PermissionError")
    assert payload["quote_count"] == 1
    assert payload["transport"]["source"] == "sqlite_wal"


def test_submit_coalesces_without_waiting_for_disk(monkeypatch, tmp_path):
    path = tmp_path / "quotes.json"
    original = transport._atomic_json_mirror

    def slow_mirror(*args, **kwargs):
        time.sleep(0.05)
        return original(*args, **kwargs)

    monkeypatch.setattr(transport, "_atomic_json_mirror", slow_mirror)
    publisher = transport.QuoteSnapshotPublisher(path)
    try:
        started = time.perf_counter()
        generations = [
            publisher.submit(
                {
                    "generated_utc": "2026-08-03T12:00:00+00:00",
                    "producer": "test",
                    "quote_count": 1,
                    "sequence": sequence,
                    "quotes": {},
                }
            )
            for sequence in range(20)
        ]
        submit_ms = (time.perf_counter() - started) * 1000.0
        wait_written(publisher, generations[-1])
        stats = publisher.stats()
        payload = transport.load_quote_snapshot(path)
    finally:
        publisher.close()

    assert submit_ms < 40.0
    assert stats["coalesced_snapshots"] > 0
    assert payload["sequence"] == 19
