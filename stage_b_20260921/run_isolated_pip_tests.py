"""Run the stage-B legacy pip-unit tests without touching the live project."""
from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "source"
TEST = ROOT / "test_legacy_rotation_pip_units.py"
RESULT = ROOT / "PIP_UNIT_TEST_RESULTS.json"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    os.environ.update(
        {
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "OMP_NUM_THREADS": "1",
            "OPENBLAS_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
        }
    )
    if not SOURCE.is_dir() or not TEST.is_file():
        raise SystemExit("stage_input_missing")
    before = {
        path.name: sha256(path)
        for path in (
            SOURCE / "oanda_indicator_alignment_research.py",
            SOURCE / "oanda_primary_forecast_rotation_bot.py",
            SOURCE / "config" / "pair_local_operational_v2_20260913.json",
        )
    }
    sys.path[:0] = [str(SOURCE), str(ROOT)]
    import pytest

    exit_code = pytest.main(["-q", "--noconftest", str(TEST)])
    after = {
        path.name: sha256(path)
        for path in (
            SOURCE / "oanda_indicator_alignment_research.py",
            SOURCE / "oanda_primary_forecast_rotation_bot.py",
            SOURCE / "config" / "pair_local_operational_v2_20260913.json",
        )
    }
    RESULT.write_text(
        json.dumps(
            {
                "scope": "isolated_legacy_pip_unit_repair",
                "test": TEST.name,
                "exit_code": exit_code,
                "source_inputs_unchanged_during_test": before == after,
                "input_sha256": after,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
