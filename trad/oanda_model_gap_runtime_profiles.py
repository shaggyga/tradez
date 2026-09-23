#!/usr/bin/env python3
"""Probe isolated modern-model runtimes without importing them into one process."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "model_gap_runtime_profiles.json"


PROBE_CODE = r'''
import importlib
import importlib.metadata
import json
import sys

packages = sys.argv[1:]
if "--symbols" in packages:
    split = packages.index("--symbols")
    symbol_specs = packages[split + 1:]
    packages = packages[:split]
else:
    symbol_specs = []
rows = {}
for package in packages:
    module, _, distribution = package.partition(":")
    try:
        importlib.import_module(module)
        status = "imported"
        error = ""
    except Exception as exc:
        status = "import_failed"
        error = f"{type(exc).__name__}: {exc}"
    try:
        version = importlib.metadata.version(distribution or module)
    except importlib.metadata.PackageNotFoundError:
        version = ""
    rows[module] = {"status": status, "version": version, "error": error}
symbols = {}
for spec in symbol_specs:
    module, _, dotted_name = spec.partition(":")
    try:
        value = importlib.import_module(module)
        for part in dotted_name.split("."):
            value = getattr(value, part)
        symbols[spec] = {"status": "available", "error": ""}
    except Exception as exc:
        symbols[spec] = {"status": "missing", "error": f"{type(exc).__name__}: {exc}"}
print(json.dumps({"python": sys.version, "executable": sys.executable, "packages": rows, "symbols": symbols}))
'''


PROFILE_PACKAGES = {
    "core_timeseries": (
        "torch:torch",
        "neuralforecast:neuralforecast",
        "pytorch_forecasting:pytorch-forecasting",
        "catboost:catboost",
        "ngboost:ngboost",
        "chronos:chronos-forecasting",
        "timesfm:timesfm",
        "stable_baselines3:stable-baselines3",
        "vowpalwabbit:vowpalwabbit",
    ),
    "foundation_moirai": (
        "torch:torch",
        "uni2ts:uni2ts",
        "gluonts:gluonts",
        "lag_llama:lag-llama",
    ),
    "foundation_toto": ("torch:torch", "toto2:toto-2"),
    "foundation_ttm": ("torch:torch", "tsfm_public:granite-tsfm"),
}

PROFILE_SYMBOLS = {
    "core_timeseries": (
        "chronos:Chronos2Pipeline.predict_quantiles",
        "timesfm:TimesFM_2p5_200M_torch",
        "neuralforecast.models:PatchTST",
        "neuralforecast.models:StemGNN",
        "stable_baselines3:PPO",
        "vowpalwabbit:pyvw.Workspace",
    ),
    "foundation_moirai": (
        "uni2ts.model.moirai:MoiraiForecast",
        "uni2ts.model.moirai:MoiraiModule",
        "uni2ts.model.moirai_moe:MoiraiMoEForecast",
        "uni2ts.model.moirai_moe:MoiraiMoEModule",
        "lag_llama.gluon.estimator:LagLlamaEstimator",
    ),
    "foundation_toto": ("toto2:Toto2Model.forecast",),
    "foundation_ttm": (
        "tsfm_public.models.tinytimemixer:TinyTimeMixerForPrediction",
    ),
}


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def expand_path(value: str) -> Path:
    runtime_root = os.environ.get(
        "FOREX_MODEL_RUNTIME_ROOT",
        str(ROOT.parent / "runtime" / "model_gap"),
    )
    value = value.replace("%FOREX_MODEL_RUNTIME_ROOT%", runtime_root)
    expanded = os.path.expandvars(value)
    if "%" in expanded:
        for key, item in os.environ.items():
            expanded = expanded.replace(f"%{key}%", item)
    return Path(expanded)


def probe_profile(
    name: str,
    value: dict[str, Any],
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    python = expand_path(str(value["python"]))
    result: dict[str, Any] = {
        "profile": name,
        "python": str(python),
        "models": list(value["models"]),
        "account_wired": False,
    }
    if not python.is_file():
        return {**result, "status": "runtime_missing", "reason": "python executable absent"}
    environment = os.environ.copy()
    configured_pythonpath = [
        str(expand_path(str(path)).resolve())
        for path in value.get("pythonpath", [])
    ]
    if configured_pythonpath:
        existing = environment.get("PYTHONPATH", "")
        environment["PYTHONPATH"] = os.pathsep.join(
            [*configured_pythonpath, *([existing] if existing else [])]
        )
    completed = subprocess.run(
        [
            str(python),
            "-c",
            PROBE_CODE,
            *PROFILE_PACKAGES.get(name, ()),
            "--symbols",
            *PROFILE_SYMBOLS.get(name, ()),
        ],
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
        env=environment,
    )
    if completed.returncode != 0:
        return {
            **result,
            "status": "probe_failed",
            "reason": completed.stderr.strip() or completed.stdout.strip(),
        }
    detail = json.loads(completed.stdout)
    imported = all(row["status"] == "imported" for row in detail["packages"].values())
    symbols_available = all(
        row["status"] == "available" for row in detail["symbols"].values()
    )
    return {
        **result,
        "status": "runtime_available" if imported and symbols_available else "import_failed",
        "detail": detail,
    }


def build_report(
    config_path: Path = DEFAULT_CONFIG,
    probe_timeout_seconds: int = 120,
) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    profiles = [
        probe_profile(name, value, timeout_seconds=probe_timeout_seconds)
        for name, value in config["profiles"].items()
    ]
    source_pins: list[dict[str, Any]] = []
    for model, item in config.get("official_source_pins", {}).items():
        path = expand_path(str(item["path"]))
        commit_file = path / "PINNED_COMMIT.txt"
        observed_commit = (
            commit_file.read_text(encoding="utf-8").strip()
            if commit_file.is_file()
            else ""
        )
        source_pins.append(
            {
                "model": model,
                "path": str(path),
                "expected_commit": str(item["commit"]),
                "observed_commit": observed_commit,
                "status": (
                    "source_pinned"
                    if path.is_dir() and observed_commit == str(item["commit"])
                    else "source_pin_mismatch"
                ),
                "account_wired": False,
            }
        )
    model_runtime = [
        {
            "model": model,
            "profile": row["profile"],
            "status": row["status"],
            "account_wired": False,
        }
        for row in profiles
        for model in row["models"]
    ]
    profiled_models = {row["model"] for row in model_runtime}
    model_runtime.extend(
        {
            "model": model,
            "profile": "explicit_blocker",
            "status": "blocked",
            "reason": reason,
            "account_wired": False,
        }
        for model, reason in config["explicit_blockers"].items()
        if model not in profiled_models
    )
    represented_models = {row["model"] for row in model_runtime}
    model_runtime.extend(
        {
            "model": row["model"],
            "profile": "official_source_pin",
            "status": row["status"],
            "account_wired": False,
        }
        for row in source_pins
        if row["model"] not in represented_models
    )
    return {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "config": str(config_path.resolve()),
        "execution_policy": "dependency_probe_only_no_weight_download_no_account_wiring",
        "profiles": profiles,
        "model_runtime": model_runtime,
        "official_source_pins": source_pins,
        "explicit_blockers": config["explicit_blockers"],
        "summary": {
            "profiles": len(profiles),
            "runtime_available": sum(row["status"] == "runtime_available" for row in profiles),
            "failed": (
                sum(row["status"] != "runtime_available" for row in profiles)
                + sum(row["status"] != "source_pinned" for row in source_pins)
            ),
            "models_runtime_available": sum(
                row["status"] == "runtime_available" for row in model_runtime
            ),
            "source_pins_valid": sum(
                row["status"] == "source_pinned" for row in source_pins
            ),
            "account_wired": 0,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--probe-timeout-sec", type=int, default=120)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(args.config, probe_timeout_seconds=max(1, args.probe_timeout_sec))
    payload = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0 if report["summary"]["failed"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
