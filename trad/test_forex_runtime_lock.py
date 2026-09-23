from __future__ import annotations

import forex_runtime_lock as runtime_lock


def test_canonical_name_matches_python_package_normalization() -> None:
    assert runtime_lock.canonical_name("PyTorch_Forecasting") == "pytorch-forecasting"
    assert runtime_lock.canonical_name("chronos.forecasting") == "chronos-forecasting"


def test_emit_requirements_supports_bootstrap_and_exact(tmp_path) -> None:
    lock = {
        "profiles": {
            "core": {
                "bootstrap_requirements": ["torch==1.0"],
                "packages": [
                    {"name": "Torch", "version": "1.0"},
                    {"name": "NumPy", "version": "2.0"},
                ],
            }
        }
    }
    bootstrap = runtime_lock.emit_requirements(lock, tmp_path / "bootstrap", False)
    exact = runtime_lock.emit_requirements(lock, tmp_path / "exact", True)
    assert (tmp_path / "bootstrap" / "requirements-core-bootstrap.txt").read_text().strip() == "torch==1.0"
    assert "NumPy==2.0" in (tmp_path / "exact" / "requirements-core-exact.txt").read_text()
    assert len(bootstrap) == len(exact) == 1
