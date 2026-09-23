"""The Windows entrypoint must fail when its native child actually fails."""
from pathlib import Path
import os
import shutil
import subprocess

import pytest


@pytest.mark.skipif(os.name != "nt", reason="Windows launcher contract")
def test_native_failure_propagates_without_success_message(tmp_path: Path):
    fake = tmp_path / "failing_interpreter.cmd"
    fake.write_text("@echo injected-native-failure\n@exit /b 37\n", encoding="ascii")
    shell = shutil.which("pwsh") or shutil.which("powershell")
    assert shell, "Windows validation requires PowerShell"
    result = subprocess.run([shell, "-NoProfile", "-NonInteractive", "-File",
                             str(Path(__file__).with_name("verify_alignment_integrity.ps1")),
                             "-PythonExecutable", str(fake)], capture_output=True, text=True, timeout=30)
    assert result.returncode != 0
    assert "injected-native-failure" in result.stdout
    assert "exit code 37" in result.stderr
    assert "verification passed" not in result.stdout.lower()

