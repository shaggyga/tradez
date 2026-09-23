from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any


ENGINE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = ENGINE_ROOT.parent
WORKER = ENGINE_ROOT / "sarima_sweep_worker.py"
LEGACY_SITE_PACKAGES = PROJECT_ROOT / "..venv" / "Lib" / "site-packages"


def find_sarima_runtime() -> Path:
    runtime_root = (
        Path.home()
        / "AppData"
        / "Roaming"
        / "uv"
        / "python"
    )
    candidates = sorted(
        runtime_root.glob("cpython-3.12*-windows-x86_64-none/python.exe"),
        reverse=True,
    )
    if not candidates:
        raise RuntimeError("cached CPython 3.12 runtime not found")
    if not (LEGACY_SITE_PACKAGES / "statsmodels").exists():
        raise RuntimeError("existing statsmodels site-packages not found")
    return candidates[0]


def build_sarima_command(
    output_dir: Path,
    *,
    start: str,
    end: str,
    tier: str,
    pairs: str | None,
    labels_path: Path | None,
    workers: int,
    maxiter_screen: int,
    maxiter_final: int,
    refine_per_category: int,
    max_specs: int | None,
    quick: bool,
    force_data: bool,
    force_screen: bool,
    force_refinement: bool,
) -> list[str]:
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
        "--workers",
        str(int(workers)),
        "--maxiter-screen",
        str(int(maxiter_screen)),
        "--maxiter-final",
        str(int(maxiter_final)),
        "--refine-per-category",
        str(int(refine_per_category)),
    ]
    if pairs:
        command.extend(["--pairs", pairs])
    if labels_path:
        command.extend(["--labels-path", str(labels_path)])
    if max_specs:
        command.extend(["--max-specs", str(int(max_specs))])
    if quick:
        command.append("--quick")
    if force_data:
        command.append("--force-data")
    if force_screen:
        command.append("--force-screen")
    if force_refinement:
        command.append("--force-refinement")
    return command


def run_full_sarima_sweep(
    cfg: dict[str, Any],
    output_dir: Path,
    *,
    start: str,
    end: str,
    tier: str,
    pairs: str | None = None,
    labels_path: Path | None = None,
    workers: int = 3,
    maxiter_screen: int = 8,
    maxiter_final: int = 35,
    refine_per_category: int = 5,
    max_specs: int | None = None,
    quick: bool = False,
    force_data: bool = False,
    force_screen: bool = False,
    force_refinement: bool = False,
) -> Path:
    del cfg
    output_dir.mkdir(parents=True, exist_ok=True)
    command = build_sarima_command(
        output_dir,
        start=start,
        end=end,
        tier=tier,
        pairs=pairs,
        labels_path=labels_path,
        workers=workers,
        maxiter_screen=maxiter_screen,
        maxiter_final=maxiter_final,
        refine_per_category=refine_per_category,
        max_specs=max_specs,
        quick=quick,
        force_data=force_data,
        force_screen=force_screen,
        force_refinement=force_refinement,
    )
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)
    report = output_dir / "SARIMA_FINAL_SUMMARY.json"
    if not report.exists():
        raise RuntimeError(f"SARIMA worker completed without report: {report}")
    return report
