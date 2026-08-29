from argparse import Namespace
from pathlib import Path

from oanda_wait_for_private_creds import inspect_creds, supervisor_command


def test_inspect_creds_accepts_practice_007_without_returning_secrets(tmp_path: Path) -> None:
    secret = "practice-token-value-that-must-not-be-persisted"
    creds = tmp_path / "creds"
    creds.write_text(
        f'OANDA_API_KEY = "{secret}"\n'
        'OANDA_ACCOUNT_ID_DUM4 = "101-001-37981792-007"\n',
        encoding="utf-8",
    )

    result = inspect_creds(creds)

    assert result["valid"] is True
    assert result["practice_007"] is True
    assert secret not in repr(result)


def test_inspect_creds_rejects_wrong_practice_account(tmp_path: Path) -> None:
    creds = tmp_path / "creds.py"
    creds.write_text(
        'OANDA_API_TOKEN = "practice-token-value-that-is-long-enough"\n'
        'OANDA_ACCOUNT_ID_DUM4 = "101-001-37981792-005"\n',
        encoding="utf-8",
    )

    result = inspect_creds(creds)

    assert result["valid"] is False
    assert result["reason"] == "account_key_does_not_resolve_to_007"


def test_supervisor_command_is_explicitly_scoped_to_practice_007(tmp_path: Path) -> None:
    root = tmp_path / "project"
    creds = tmp_path / "private" / "creds"
    args = Namespace(
        supervisor=tmp_path / "oanda_always_on_supervisor.ps1",
        root=root,
        account_key="OANDA_ACCOUNT_ID_DUM4",
        run_label="unified-signal-confidence-matrix-v10",
        child_duration_sec=604800,
        python=tmp_path / "python.exe",
        safe_core_only=False,
        enable_crypto=False,
    )

    command = supervisor_command(args, creds)

    assert command[command.index("-AccountKey") + 1] == "OANDA_ACCOUNT_ID_DUM4"
    assert command[command.index("-RunLabel") + 1] == (
        "unified-signal-confidence-matrix-v10"
    )
    assert command[command.index("-CredsPath") + 1] == str(creds)
    assert "-EnableCrypto" not in command
