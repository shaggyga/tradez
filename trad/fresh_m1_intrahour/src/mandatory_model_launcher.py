from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from .sarima_sweep_launcher import find_sarima_runtime


ENGINE_ROOT = Path(__file__).resolve().parents[1]
WORKER = ENGINE_ROOT / "mandatory_model_validation_worker.py"


def build_mandatory_validation_command(
    output_dir: Path,
    *,
    start: str,
    end: str,
    tier: str,
    pairs: list[str],
    workers: int,
    maxiter: int,
    quick: bool,
) -> list[str]:
    if tier != "tier1":
        raise ValueError("mandatory validation is research-only and bounded to tier1")
    command = [
        str(find_sarima_runtime()),
        str(WORKER),
        "--output-dir",
        str(output_dir),
        "--start",
        start,
        "--end",
        end,
        "--tier",
        tier,
        "--pairs",
        ",".join(pairs),
        "--workers",
        str(max(1, int(workers))),
        "--maxiter",
        str(max(1, int(maxiter))),
    ]
    if quick:
        command.append("--quick")
    return command


def run_mandatory_model_validation(
    cfg: dict[str, Any],
    output_dir: Path,
    *,
    start: str,
    end: str,
    tier: str,
    pairs: list[str] | None,
    workers: int = 3,
    maxiter: int = 35,
    quick: bool = False,
) -> Path:
    execution = cfg.get("execution", {})
    enabled = [
        key
        for key in ("live_execution_enabled", "demo_execution_enabled", "oanda_execution_enabled")
        if bool(execution.get(key, False))
    ]
    if enabled:
        raise RuntimeError(f"refusing mandatory research run with execution enabled: {enabled}")
    selected_pairs = pairs or list(cfg.get("tier1_pairs", []))
    if quick:
        selected_pairs = selected_pairs[:2]
    if not selected_pairs:
        raise ValueError("no Tier-1 pairs selected")
    output_dir.mkdir(parents=True, exist_ok=True)
    command = build_mandatory_validation_command(
        output_dir,
        start=start,
        end=end,
        tier=tier,
        pairs=selected_pairs,
        workers=workers,
        maxiter=maxiter,
        quick=quick,
    )
    subprocess.run(command, cwd=ENGINE_ROOT.parent, check=True)
    summary_path = output_dir / "MANDATORY_MODEL_FINAL_SUMMARY.json"
    if not summary_path.is_file():
        raise RuntimeError(f"mandatory validation did not create {summary_path}")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if summary.get("verdict") != "PASS":
        raise RuntimeError(f"mandatory validation failed: {summary}")
    safety = summary.get("safety", {})
    if any(
        bool(safety.get(key, False))
        for key in (
            "live_execution_enabled",
            "demo_execution_enabled",
            "oanda_execution_enabled",
            "mt5_execution_enabled",
            "order_placement_used",
            "serialized_models_loaded",
        )
    ):
        raise RuntimeError(f"mandatory validation safety invariant failed: {safety}")
    return summary_path

