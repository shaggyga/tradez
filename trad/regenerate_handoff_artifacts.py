#!/usr/bin/env python3
"""Regenerate large OANDA handoff artifacts instead of shipping them.

This script rebuilds the large H1/H4 artifacts that are intentionally omitted
from lightweight handoff packets:

* all68 feature roots for H1/H4, if missing
* technical_spike_research_<timeframe>_step1.parquet
* all68_latest_hgb_reversal_<timeframe>_full_trader_style_backtest/candidate_rows.csv

It calls the existing project builders so the generated files keep the same
schema and model semantics as the original reports.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


ROOT = Path(__file__).resolve().parent
REPORTS = ROOT / "data" / "oanda_training_manager" / "reports"
RESEARCH = ROOT / "data" / "oanda_training_manager" / "continuous_research"
FEATURE_PARENT = ROOT / "data" / "all68_weekly_move_study"
SETTINGS_CSV = REPORTS / "all68_latest_hgb_reversal_pool_settings_technical_full_only.csv"
MANIFEST_ROOT = REPORTS / "handoff_regeneration"


@dataclass(frozen=True)
class TimeframeSpec:
    name: str
    feature_timeframe: str
    feature_slug: str
    dataset_suffix: str
    legacy_output_name: str

    @property
    def feature_root(self) -> Path:
        return FEATURE_PARENT / f"features_{self.feature_slug}"

    @property
    def report_root(self) -> Path:
        return FEATURE_PARENT / f"reports_{self.feature_slug}"

    @property
    def dataset_path(self) -> Path:
        return RESEARCH / f"technical_spike_research_{self.dataset_suffix}.parquet"

    @property
    def dataset_metadata_path(self) -> Path:
        return RESEARCH / f"technical_spike_research_{self.dataset_suffix}.metadata.json"

    @property
    def legacy_output_dir(self) -> Path:
        return REPORTS / self.legacy_output_name

    @property
    def legacy_candidate_rows(self) -> Path:
        return self.legacy_output_dir / "candidate_rows.csv"


TIMEFRAMES: Dict[str, TimeframeSpec] = {
    "m30": TimeframeSpec(
        name="m30",
        feature_timeframe="M30",
        feature_slug="30m",
        dataset_suffix="30m_step1",
        legacy_output_name="all68_latest_hgb_reversal_m30_full_trader_style_backtest",
    ),
    "h1": TimeframeSpec(
        name="h1",
        feature_timeframe="H1",
        feature_slug="1h",
        dataset_suffix="1h_step1",
        legacy_output_name="all68_latest_hgb_reversal_h1_full_trader_style_backtest",
    ),
    "h4": TimeframeSpec(
        name="h4",
        feature_timeframe="H4",
        feature_slug="4h",
        dataset_suffix="4h_step1",
        legacy_output_name="all68_latest_hgb_reversal_h4_full_trader_style_backtest",
    ),
}


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT.resolve()))
    except Exception:
        return str(path)


def file_rows_csv(path: Path) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0
    with path.open("rb") as handle:
        return max(0, sum(1 for _ in handle) - 1)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def artifact_state(path: Path, *, rows: bool = False, checksum: bool = False) -> Dict[str, Any]:
    state: Dict[str, Any] = {
        "path": rel(path),
        "exists": path.exists(),
        "bytes": path.stat().st_size if path.exists() else 0,
        "mtime_utc": (
            datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
            if path.exists()
            else ""
        ),
    }
    if rows:
        state["rows"] = file_rows_csv(path)
    if checksum and path.exists() and path.is_file():
        state["sha256"] = sha256_file(path)
    return state


def feature_state(spec: TimeframeSpec) -> Dict[str, Any]:
    files = sorted(spec.feature_root.glob("*.parquet")) if spec.feature_root.exists() else []
    return {
        "path": rel(spec.feature_root),
        "exists": spec.feature_root.exists(),
        "feature_files": len(files),
        "complete_68_pair_set": len(files) == 68,
        "bytes": sum(path.stat().st_size for path in files),
    }


def run_command(
    command: Sequence[str],
    *,
    env_updates: Dict[str, str] | None,
    dry_run: bool,
    label: str,
) -> Dict[str, Any]:
    env = os.environ.copy()
    env["TRAD_PROJECT_ROOT"] = str(ROOT)
    env["PYTHONUTF8"] = "1"
    if env_updates:
        env.update(env_updates)
    record: Dict[str, Any] = {
        "label": label,
        "command": list(command),
        "cwd": str(ROOT),
        "env_updates": env_updates or {},
        "dry_run": dry_run,
    }
    print(json.dumps(record, sort_keys=True), flush=True)
    if dry_run:
        record["returncode"] = None
        return record
    completed = subprocess.run(
        list(command),
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    record["returncode"] = completed.returncode
    record["stdout_tail"] = completed.stdout[-4000:]
    record["stderr_tail"] = completed.stderr[-4000:]
    if completed.returncode != 0:
        print(completed.stdout[-4000:], end="", flush=True)
        print(completed.stderr[-4000:], end="", file=sys.stderr, flush=True)
        raise RuntimeError(f"{label} failed with returncode={completed.returncode}")
    return record


def build_features(spec: TimeframeSpec, args: argparse.Namespace, commands: List[Dict[str, Any]]) -> None:
    state = feature_state(spec)
    if args.skip_features:
        return
    if state["complete_68_pair_set"] and not (args.force or args.force_features):
        print(f"[skip] {spec.name} feature root already has 68 parquet files: {rel(spec.feature_root)}", flush=True)
        return
    command = [
        sys.executable,
        str(ROOT / "all68_weekly_missed_move_study.py"),
        "--feature-timeframe",
        spec.feature_timeframe,
        "--feature-root",
        str(spec.feature_root),
        "--report-root",
        str(spec.report_root),
        "--build-features-only",
    ]
    if not (args.force or args.force_features):
        command.append("--reuse-features")
    commands.append(
        run_command(command, env_updates=None, dry_run=args.dry_run, label=f"build_features_{spec.name}")
    )


def build_dataset(spec: TimeframeSpec, args: argparse.Namespace, commands: List[Dict[str, Any]]) -> None:
    if args.skip_datasets:
        return
    if spec.dataset_path.exists() and not (args.force or args.force_datasets):
        print(f"[skip] {spec.name} technical dataset exists: {rel(spec.dataset_path)}", flush=True)
        return
    if not feature_state(spec)["complete_68_pair_set"] and not args.dry_run:
        raise RuntimeError(f"{spec.name} feature root is incomplete: {spec.feature_root}")
    command = [
        sys.executable,
        "-c",
        (
            "from oanda_gpt_training_strategy_manager import "
            "build_technical_spike_research_dataset; "
            "print(build_technical_spike_research_dataset())"
        ),
    ]
    commands.append(
        run_command(
            command,
            env_updates={
                "OANDA_TECHNICAL_FEATURE_ROOT": str(spec.feature_root),
                "OANDA_TECHNICAL_RESEARCH_DATASET_SUFFIX": spec.dataset_suffix,
                "OANDA_TECHNICAL_RESEARCH_SAMPLE_STEP": "1",
            },
            dry_run=args.dry_run,
            label=f"build_dataset_{spec.name}",
        )
    )


def build_legacy_candidates(
    spec: TimeframeSpec,
    args: argparse.Namespace,
    commands: List[Dict[str, Any]],
) -> None:
    if args.skip_candidates:
        return
    if spec.legacy_candidate_rows.exists() and not (args.force or args.force_candidates):
        print(f"[skip] {spec.name} candidate rows exist: {rel(spec.legacy_candidate_rows)}", flush=True)
        return
    if not SETTINGS_CSV.exists() and not args.dry_run:
        raise FileNotFoundError(f"Missing settings CSV: {SETTINGS_CSV}")
    if not spec.dataset_path.exists() and not args.dry_run:
        raise FileNotFoundError(f"Missing dataset for {spec.name}: {spec.dataset_path}")
    command = [
        sys.executable,
        str(ROOT / "oanda_live_style_pair_portfolio_backtest.py"),
        "--settings-csv",
        str(SETTINGS_CSV),
        "--output-dir",
        str(spec.legacy_output_dir),
        "--train-row-cap",
        str(args.train_row_cap),
        "--n-jobs",
        str(args.n_jobs),
        "--min-train-rows",
        "5000",
        "--min-calibration-rows",
        "250",
        "--min-test-rows",
        "250",
        "--min-pair-calibration-rows",
        "20",
        "--min-pair-calibration-trades",
        "3",
        "--thresholds",
        "0.50:0.95:0.025",
    ]
    commands.append(
        run_command(
            command,
            env_updates={
                "OANDA_TECHNICAL_FEATURE_ROOT": str(spec.feature_root),
                "OANDA_TECHNICAL_RESEARCH_DATASET_SUFFIX": spec.dataset_suffix,
                "OANDA_TECHNICAL_RESEARCH_SAMPLE_STEP": "1",
                "OANDA_CONTINUOUS_RESEARCH_VALIDATION_MAX_TRAIN_ROWS_CAP": str(max(10000, args.train_row_cap)),
                "OANDA_CONTINUOUS_RESEARCH_SCREEN_MAX_TRAIN_ROWS_CAP": str(max(10000, min(args.train_row_cap, 30000))),
                "OANDA_CONTINUOUS_RESEARCH_N_JOBS": str(max(1, args.n_jobs)),
            },
            dry_run=args.dry_run,
            label=f"build_candidates_{spec.name}",
        )
    )


def verify_timeframe(spec: TimeframeSpec, *, checksum: bool) -> Dict[str, Any]:
    return {
        "timeframe": spec.name,
        "features": feature_state(spec),
        "technical_dataset": artifact_state(spec.dataset_path, checksum=checksum),
        "technical_dataset_metadata": artifact_state(spec.dataset_metadata_path, checksum=checksum),
        "legacy_candidate_rows": artifact_state(spec.legacy_candidate_rows, rows=True, checksum=checksum),
        "legacy_summary": artifact_state(spec.legacy_output_dir / "summary.json", checksum=checksum),
    }


def parse_timeframes(args: argparse.Namespace) -> List[TimeframeSpec]:
    names = [item.strip().lower() for item in str(args.timeframes).split(",") if item.strip()]
    if args.include_m30 and "m30" not in names:
        names.insert(0, "m30")
    specs: List[TimeframeSpec] = []
    for name in names:
        if name not in TIMEFRAMES:
            raise SystemExit(f"Unsupported timeframe {name!r}; expected one of {sorted(TIMEFRAMES)}")
        specs.append(TIMEFRAMES[name])
    return specs


def write_manifest(payload: Dict[str, Any]) -> Path:
    MANIFEST_ROOT.mkdir(parents=True, exist_ok=True)
    path = MANIFEST_ROOT / f"regeneration_manifest_{utc_stamp()}.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    return path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--timeframes",
        default="h1,h4",
        help="Comma-separated subset of m30,h1,h4. Default repairs the original H1/H4 handoff.",
    )
    parser.add_argument(
        "--include-m30",
        action="store_true",
        help="Also regenerate/check the M30 dataset and legacy candidate rows used by the current broader setup.",
    )
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true", help="Rebuild all selected artifacts even if present.")
    parser.add_argument("--force-features", action="store_true")
    parser.add_argument("--force-datasets", action="store_true")
    parser.add_argument("--force-candidates", action="store_true")
    parser.add_argument("--skip-features", action="store_true")
    parser.add_argument("--skip-datasets", action="store_true")
    parser.add_argument("--skip-candidates", action="store_true")
    parser.add_argument("--checksums", action="store_true", help="Compute SHA256 for generated/verified files.")
    parser.add_argument("--train-row-cap", type=int, default=30000)
    parser.add_argument("--n-jobs", type=int, default=4)
    args = parser.parse_args(argv)

    specs = parse_timeframes(args)
    commands: List[Dict[str, Any]] = []
    before = [verify_timeframe(spec, checksum=False) for spec in specs]

    if not args.verify_only:
        for spec in specs:
            build_features(spec, args, commands)
            build_dataset(spec, args, commands)
            build_legacy_candidates(spec, args, commands)

    after = [verify_timeframe(spec, checksum=args.checksums) for spec in specs]
    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "script": rel(Path(__file__)),
        "purpose": "Regenerate omitted large H1/H4 handoff artifacts from local candles/features/code.",
        "root": str(ROOT),
        "settings_csv": artifact_state(SETTINGS_CSV, rows=True, checksum=args.checksums),
        "arguments": vars(args),
        "before": before,
        "after": after,
        "commands": commands,
        "notes": [
            "This does not copy or export large parquet/CSV files.",
            "Exact reproducibility depends on the local candle history and feature roots matching the source machine.",
            "The current primary live account uses additional dual-history calibration CSVs; this script repairs the original H1/H4 handoff candidate rows and can rebuild M30/H1/H4 research datasets.",
        ],
    }
    manifest = write_manifest(payload)
    print(json.dumps({"manifest": str(manifest), "timeframes": [spec.name for spec in specs]}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
