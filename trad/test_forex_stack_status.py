from __future__ import annotations

import json
from datetime import datetime, timezone

import forex_stack_status as status


def test_read_json_handles_powershell_utf8_bom(tmp_path) -> None:
    path = tmp_path / "watchdog.json"
    path.write_text(
        '\ufeff{"time": "2026-06-30T23:50:39.7402798-04:00"}',
        encoding="utf-8",
    )

    payload = status.read_json(path, {})

    assert payload["time"].endswith("-04:00")


def test_utc_age_minutes_handles_powershell_fractional_seconds() -> None:
    age = status.utc_age_minutes("2026-06-30T23:50:39.7402798-04:00")

    assert age is not None


def test_merge_process_rows_dedupes_by_pid_and_command() -> None:
    rows = status.merge_process_rows(
        [{"pid": "1", "command": "python a.py"}],
        [
            {"pid": "1", "command": "python a.py"},
            {"pid": "2", "command": "python b.py"},
        ],
    )

    assert rows == [
        {"pid": "1", "command": "python a.py"},
        {"pid": "2", "command": "python b.py"},
    ]


def test_trainer_control_state_summary_reports_only_noncompleted_jobs(tmp_path) -> None:
    now = datetime.now(timezone.utc).isoformat()
    path = tmp_path / "research_only_state.json"
    path.write_text(
        json.dumps(
            {
                "last_tick_utc": now,
                "latest_missed_spike_backtest": {
                    "status": "error",
                    "time_utc": now,
                },
                "latest_reporting_extensions": {
                    "status": "completed",
                    "time_utc": now,
                },
            }
        ),
        encoding="utf-8",
    )

    text = status.trainer_control_state_summary(path)

    assert "control_age=" in text
    assert "missed=error" in text
    assert "reporting=" not in text


def test_live_day_monitor_status_summary_includes_pid_and_accounts(tmp_path) -> None:
    now = datetime.now(timezone.utc).isoformat()
    path = tmp_path / "latest_status.json"
    path.write_text(
        json.dumps(
            {
                "time_utc": now,
                "status": "ok",
                "processes": {
                    "day_monitor": {"running": True, "pid": "40108"},
                },
                "accounts": {
                    "gpt": {"open_trades": 0, "nav": 47.96},
                    "tech": {"open_trades": 1, "nav": 9.72},
                },
            }
        ),
        encoding="utf-8",
    )
    original = status.LIVE_DAY_MONITOR_STATUS_PATH
    status.LIVE_DAY_MONITOR_STATUS_PATH = path
    try:
        text = status.live_day_monitor_status_summary()
    finally:
        status.LIVE_DAY_MONITOR_STATUS_PATH = original

    assert "day_monitor:status=ok" in text
    assert "pid=40108" in text
    assert "gpt:open=0 nav=47.96" in text
    assert "tech:open=1 nav=9.72" in text
