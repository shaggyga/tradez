from __future__ import annotations

import ast
import json
from pathlib import Path
import subprocess
import sys

import pytest

import oanda_sequential_all68_portfolio_batch_replay_verify_config as launcher


ROOT = Path(__file__).resolve().parent
EXPANSION_CONFIG = (
    ROOT
    / "config"
    / "sequential_all68_portfolio_batch_replay_wednesday_expansion_v1.json"
)


def test_launcher_verifies_explicit_expansion_config() -> None:
    completed = subprocess.run(
        [sys.executable, str(launcher.__file__), "--config", str(EXPANSION_CONFIG)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    result = json.loads(completed.stdout)
    assert result["verified"] is True
    assert result["cohort_id"] == "seq_a68_wed_exp_v1.e7de4ecdd0255eb13306"
    assert result["reconstructed_global_clocks"] == 336
    assert result["reconstructed_pair_contexts"] == 22848


def test_launcher_rejects_path_escape() -> None:
    with pytest.raises(ValueError, match="escapes project root"):
        launcher.configure(ROOT.parent / "outside.json")


def test_launcher_does_not_import_replay_producer_or_core() -> None:
    tree = ast.parse(Path(launcher.__file__).read_text(encoding="utf-8"))
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(str(node.module or ""))
    assert "oanda_sequential_all68_portfolio_batch_replay" not in imports
    assert not any(name.startswith("src.forex_system.research") for name in imports)
