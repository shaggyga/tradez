"""Offline audit: a collector transaction can cross the fast-lane scan cursor.

Uses production fast-lane code, AST-isolated fixture/checker functions and
disposable SQLite files only. Does not import runtime entrypoints or use network.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import json
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(r"C:\Users\zmoor\Documents\forex\trad")
spec = importlib.util.spec_from_file_location(
    "audit_fastlane_helpers", ROOT / "tools/forex_fastlane_fault_reproduction.py"
)
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)
fast, governance = helpers.fast, helpers.governance
check, make_news, make_registry = helpers.build_helpers()

with tempfile.TemporaryDirectory(prefix="forex_late_commit_audit_") as temporary:
    root = Path(temporary)
    database, news, state = root / "registry.sqlite", root / "news.sqlite", root / "state.json"
    make_registry(database)
    first_seen = (fast.ACTIVATED_UTC + dt.timedelta(seconds=1)).isoformat()
    make_news(news, first_seen=first_seen, last_seen=first_seen)
    paths = dict(news_database=news, database_path=database, state_path=state)
    baseline = fast.run(**paths, now=fast.ACTIVATED_UTC + dt.timedelta(seconds=2))
    assert baseline["new_receipt_count"] == 1

    writer = sqlite3.connect(news)
    writer.execute("PRAGMA journal_mode=WAL")
    original = list(writer.execute("SELECT * FROM articles").fetchone())
    original[0] = "article-committed-after-scan"
    seen_late = (fast.ACTIVATED_UTC + dt.timedelta(seconds=20)).isoformat()
    original[6] = original[7] = original[8] = seen_late
    writer.execute("INSERT INTO articles VALUES (" + ",".join("?" * len(original)) + ")", original)
    assert writer.in_transaction

    during = fast.run(**paths, now=fast.ACTIVATED_UTC + dt.timedelta(seconds=32))
    writer.commit()
    writer.close()
    after_time = fast.ACTIVATED_UTC + dt.timedelta(seconds=62)
    after = fast.run(**paths, now=after_time)
    available = list(governance.article_events(
        news, changed_after_utc=fast.ACTIVATED_UTC.isoformat(),
        changed_before_or_at_utc=after_time.isoformat(),
        include_verified_structured_recheck=False, change_clock="first_seen_utc",
    ))
    reader = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    receipts = [row[0] for row in reader.execute("SELECT provider_event_id FROM news_fast_lane_import_receipts")]
    reader.close()
    integrity = check(after, database_path=database, cutoff_epoch=after_time.timestamp())
    result = {
        "fault_reproduced": len(available) == 2 and "article-committed-after-scan" not in receipts,
        "scope": "real overlapping WAL writer and production reader on temporary miniature SQLite files; no sleeps, network, broker or production DB",
        "baseline_new_receipts": baseline["new_receipt_count"],
        "late_row_first_seen_utc": seen_late,
        "during_uncommitted_write": {key: during[key] for key in ("status", "input_row_count", "next_scan_cursor_utc")},
        "after_collector_commit": {key: after[key] for key in ("status", "input_row_count", "scan_cursor_utc", "next_scan_cursor_utc", "total_receipt_count")},
        "raw_rows_visible_after_commit": [row["event_id"] for row in available],
        "fast_lane_receipted_provider_ids": receipts,
        "integrity_ok_despite_omission": integrity["ok"],
        "historical_occurrence": "not_established",
        "scope_of_omission": "fast-lane receipt; full governance reconciliation may ingest the row separately",
    }
    assert result["fault_reproduced"]
    assert result["integrity_ok_despite_omission"]
    print(json.dumps(result, indent=2))
