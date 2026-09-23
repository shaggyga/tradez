from __future__ import annotations

import json
import sys

import oanda_disk_event7_guard as guard


def test_guard_refuses_to_start_after_new_event(monkeypatch, tmp_path) -> None:
    report = tmp_path / "guard.json"
    monkeypatch.setattr(guard, "latest_event7_record", lambda: 101)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "oanda_disk_event7_guard.py",
            "--baseline-record",
            "100",
            "--report",
            str(report),
            "--",
            "python",
            "never_started.py",
        ],
    )

    assert guard.main() == 3
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["status"] == "refused_new_event7_before_start"
    assert payload["latest_event7_record"] == 101


def test_windows_process_tree_orders_descendants_before_parent(monkeypatch) -> None:
    class Completed:
        returncode = 0
        stdout = json.dumps(
            [
                {"ProcessId": 10, "ParentProcessId": 1},
                {"ProcessId": 11, "ParentProcessId": 10},
                {"ProcessId": 12, "ParentProcessId": 11},
                {"ProcessId": 99, "ParentProcessId": 1},
            ]
        )

    monkeypatch.setattr(guard.subprocess, "run", lambda *args, **kwargs: Completed())

    assert guard.windows_process_tree(10) == [12, 11, 10]
