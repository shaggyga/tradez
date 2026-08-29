import sqlite3

import oanda_signal_feed_wal_maintenance as maintenance


def test_passive_checkpoint_reports_frames_without_truncating_live_wal(tmp_path):
    database = tmp_path / "feed.sqlite"
    writer = sqlite3.connect(database)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE rows(value BLOB)")
        writer.executemany(
            "INSERT INTO rows(value) VALUES (?)",
            [(b"x" * 4096,) for _ in range(32)],
        )
        writer.commit()

        result = maintenance.checkpoint_once(database, threshold_bytes=1)

        assert result["status"] == "ok"
        assert result["mode"] == "PASSIVE"
        assert result["wal_bytes_before"] > 0
        assert result["checkpointed_frames"] > 0
        assert result["can_place_orders"] is False
        assert result["research_only"] is True
    finally:
        writer.close()
