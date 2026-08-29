from pathlib import Path
import re


ROOT = Path(__file__).resolve().parent
SUPERVISOR = ROOT / "oanda_always_on_supervisor.ps1"


def _integrity_freshness_block() -> str:
    text = SUPERVISOR.read_text(encoding="utf-8")
    match = re.search(
        r'-Name\s+"project_integrity_audit".*?'
        r'-Freshness\s+@\{(?P<block>.*?)\n\s*\}',
        text,
        flags=re.DOTALL,
    )
    assert match is not None
    return match.group("block")


def test_integrity_watchdog_allows_a_measured_cold_scan_to_finish() -> None:
    block = _integrity_freshness_block()
    max_age = int(re.search(r"MaxAgeSec\s*=\s*(\d+)", block).group(1))
    startup_grace = int(
        re.search(r"StartupGraceSec\s*=\s*(\d+)", block).group(1)
    )
    assert max_age >= 2700
    assert startup_grace >= 3600
    assert startup_grace > max_age


def test_integrity_watchdog_still_has_a_finite_staleness_bound() -> None:
    block = _integrity_freshness_block()
    max_age = int(re.search(r"MaxAgeSec\s*=\s*(\d+)", block).group(1))
    startup_grace = int(
        re.search(r"StartupGraceSec\s*=\s*(\d+)", block).group(1)
    )
    assert max_age <= 3600
    assert startup_grace <= 7200


def test_version_gated_heartbeat_reads_retry_transient_windows_denials() -> None:
    text = SUPERVISOR.read_text(encoding="utf-8")
    assert "function Read-JsonFileWithRetry" in text
    assert "[int]$MaximumAttempts = 4" in text
    assert "[System.IO.File]::ReadAllText" in text
    assert "ConvertFrom-Json -ErrorAction Stop" in text
    assert "Start-Sleep -Milliseconds" in text
    assert (
        "$heartbeat = Read-JsonFileWithRetry -LiteralPath $item.FullName"
        in text
    )
    assert (
        "$heartbeat = Get-Content -LiteralPath $item.FullName -Raw | "
        "ConvertFrom-Json"
        not in text
    )
