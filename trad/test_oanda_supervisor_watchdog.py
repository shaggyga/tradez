from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parent


def test_watchdog_is_canonical_single_instance_and_fail_closed():
    text = (ROOT / "oanda_supervisor_watchdog.ps1").read_text(encoding="utf-8")
    assert "Local\\ForexSupervisorWatchdogV1_" in text
    assert "lease_owned = $true" in text
    assert "The D: legacy workspace is never a valid watchdog target" in text
    assert "real_money_enabled = $false" in text
    assert "can_place_orders = $false" in text
    assert "can_change_authorization = $false" in text


def test_watchdog_recovery_is_bounded_and_preserves_python_children():
    text = (ROOT / "oanda_supervisor_watchdog.ps1").read_text(encoding="utf-8")
    assert "MaximumRestartsPerWindow = 3" in text
    assert "restart_circuit_open" in text
    assert "python_children_preserved = $true" in text
    assert '"-File", $Launcher' in text
    assert "Stop-Process -Id ([int]$supervisors[0].ProcessId)" in text
    assert "Stop-Process" in text
    assert "python" not in "\n".join(
        line for line in text.splitlines() if "Stop-Process" in line
    ).lower()


def test_watchdog_writes_atomic_lease_heartbeat_and_incidents():
    text = (ROOT / "oanda_supervisor_watchdog.ps1").read_text(encoding="utf-8")
    assert "oanda_supervisor_watchdog_v1.json" in text
    assert "oanda_supervisor_watchdog_lease_v1.json" in text
    assert "oanda_supervisor_watchdog_incidents_v1.jsonl" in text
    assert "Write-AtomicJson" in text
    assert "Move-Item -LiteralPath $temporary" in text
    assert "Get-ProjectPythonInventory" in text


def test_watchdog_launcher_is_hidden_and_idempotent():
    text = (ROOT / "start_oanda_supervisor_watchdog.ps1").read_text(
        encoding="utf-8"
    )
    assert "Get-CimInstance Win32_Process" in text
    assert "if ($existing.Count -gt 0) { exit 0 }" in text
    assert "-WindowStyle Hidden" in text
    assert "oanda_supervisor_watchdog.ps1" in text
