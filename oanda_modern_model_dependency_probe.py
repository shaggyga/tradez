#!/usr/bin/env python3
"""Record modern-model dependency availability without installing packages."""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import json
import os
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = (
    PROJECT_ROOT
    / "data"
    / "oanda_training_manager"
    / "model_space"
    / "modern_model_dependency_status_latest.json"
)

PACKAGE_SPECS = {
    "catboost": {
        "module": "catboost",
        "distribution": "catboost",
        "required_for": ["catboost_tabular"],
    },
    "ngboost": {
        "module": "ngboost",
        "distribution": "ngboost",
        "required_for": ["ngboost_probabilistic_tabular"],
    },
    "lightgbm": {
        "module": "lightgbm",
        "distribution": "lightgbm",
        "required_for": ["lightgbm_tabular"],
    },
    "xgboost": {
        "module": "xgboost",
        "distribution": "xgboost",
        "required_for": ["xgboost_tabular"],
    },
    "torch": {
        "module": "torch",
        "distribution": "torch",
        "required_for": [
            "tft",
            "patchtst",
            "nhits",
            "deepar",
            "lstm",
            "gru",
            "tcn",
            "cross_pair_gnn",
            "mamba_s4",
        ],
    },
    "pytorch_forecasting": {
        "module": "pytorch_forecasting",
        "distribution": "pytorch-forecasting",
        "required_for": ["tft", "deepar"],
    },
    "neuralforecast": {
        "module": "neuralforecast",
        "distribution": "neuralforecast",
        "required_for": ["patchtst", "nhits", "lstm", "gru", "tcn"],
    },
    "chronos": {
        "module": "chronos",
        "distribution": "chronos-forecasting",
        "required_for": ["chronos_foundation_baseline"],
    },
    "timesfm": {
        "module": "timesfm",
        "distribution": "timesfm",
        "required_for": ["timesfm_foundation_baseline"],
    },
    "uni2ts": {
        "module": "uni2ts",
        "distribution": "uni2ts",
        "required_for": ["moirai_foundation_baseline"],
    },
    "transformers": {
        "module": "transformers",
        "distribution": "transformers",
        "required_for": ["tiny_time_mixer", "lag_llama", "toto"],
    },
    "torch_geometric": {
        "module": "torch_geometric",
        "distribution": "torch-geometric",
        "required_for": ["cross_pair_gnn"],
    },
    "mamba_ssm": {
        "module": "mamba_ssm",
        "distribution": "mamba-ssm",
        "required_for": ["mamba_s4"],
    },
    "stable_baselines3": {
        "module": "stable_baselines3",
        "distribution": "stable-baselines3",
        "required_for": ["ppo", "sac", "dqn"],
    },
}


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def package_status(name: str, spec: Dict[str, Any]) -> Dict[str, Any]:
    installed = importlib.util.find_spec(str(spec["module"])) is not None
    version = ""
    if installed:
        try:
            version = importlib.metadata.version(str(spec["distribution"]))
        except importlib.metadata.PackageNotFoundError:
            version = "unknown"
    return {
        "installed": installed,
        "version": version,
        "module": spec["module"],
        "distribution": spec["distribution"],
        "required_for": list(spec["required_for"]),
    }


def build_report() -> Dict[str, Any]:
    packages = {
        name: package_status(name, spec)
        for name, spec in PACKAGE_SPECS.items()
    }
    return {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "execution_policy": "shadow_only",
        "network_accessed": False,
        "packages_installed_by_probe": False,
        "python": {
            "executable": sys.executable,
            "version": platform.python_version(),
            "platform": platform.platform(),
        },
        "packages": packages,
        "available_count": sum(
            int(status["installed"]) for status in packages.values()
        ),
        "missing_count": sum(
            int(not status["installed"]) for status in packages.values()
        ),
    }


def write_report(path: Path, report: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temp.write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(temp, path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report()
    write_report(args.output, report)
    print(json.dumps({
        "output": str(args.output),
        "available_count": report["available_count"],
        "missing_count": report["missing_count"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
