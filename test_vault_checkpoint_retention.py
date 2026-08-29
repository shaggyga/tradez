import json
import tempfile
from pathlib import Path

from tools.vault_checkpoint_retention import run


def test_recoverable_retention_keeps_current_and_verifies_moves() -> None:
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        source = root / "vault" / "projects" / "forex"
        quarantine = root / "quarantine"
        source.mkdir(parents=True)
        (source / "forex_model_checkpoint_current.zip").write_bytes(b"current")
        (source / "forex_model_checkpoint_current.manifest.json").write_text("{}")
        (source / "forex_model_checkpoint_20260801_000000.zip").write_bytes(b"old")
        (source / "forex_model_checkpoint_yv2_jgly.tmp").write_bytes(b"temp")
        plan = run(source, quarantine, False)
        assert not plan["applied"] and len(plan["files"]) == 2
        applied = run(source, quarantine, True)
        assert applied["applied"] and all(row["status"] == "moved_verified" for row in applied["files"])
        assert (source / "forex_model_checkpoint_current.zip").read_bytes() == b"current"
        assert not (source / "forex_model_checkpoint_20260801_000000.zip").exists()
        manifest = json.loads((quarantine / "VAULT_CHECKPOINT_RETENTION_MANIFEST_20260809.json").read_text())
        assert manifest["verified_bytes"] == 7
