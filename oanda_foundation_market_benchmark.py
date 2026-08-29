#!/usr/bin/env python3
"""Orchestrate identical frozen foundation-model market scoring across runtimes."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import oanda_model_gap_runtime_profiles as runtime_profiles


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
DEFAULT_WEIGHTS = ROOT / "config" / "foundation_model_weights.json"
DEFAULT_RUNTIMES = ROOT / "config" / "model_gap_runtime_profiles.json"
DEFAULT_FIXTURE = DATA_ROOT / "model_space" / "validation_fixtures" / "foundation_m1_holdout_v1.npz"
DEFAULT_REPORT = DATA_ROOT / "reports" / "modern_model_gap" / "foundation_market_benchmark_latest.json"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def output_tail(value: str | None, limit: int = 4000) -> str:
    return (value or "")[-limit:]


def reusable_result(
    path: Path,
    model: str,
    weights: Path,
    fixture: Path,
    requested_device: str | None = None,
) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        result = json.loads(path.read_text(encoding="utf-8-sig"))
        matches = (
            result.get("model") == model
            and result.get("status") == "bounded_market_scored"
            and Path(str(result.get("weights"))).resolve() == weights.resolve()
            and Path(str(result.get("fixture"))).resolve() == fixture.resolve()
            and int(result.get("summary", {}).get("n", 0)) > 0
            and (
                requested_device is None
                or result.get("requested_device") == requested_device
            )
        )
    except (OSError, TypeError, ValueError):
        return None
    return result if matches else None


def combine_fixture_results(
    model: str,
    weights: Path,
    fixture_results: list[dict[str, Any]],
) -> dict[str, Any]:
    cells = [cell for result in fixture_results for cell in result.get("cells") or []]
    total = sum(int((result.get("summary") or {}).get("n") or 0) for result in fixture_results)

    def weighted(field: str) -> float:
        if not total:
            return 0.0
        return float(
            sum(
                float((result.get("summary") or {}).get(field) or 0.0)
                * int((result.get("summary") or {}).get("n") or 0)
                for result in fixture_results
            )
            / total
        )

    return {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "model": model,
        "status": "bounded_market_scored",
        "validation_level": "frozen_zero_shot_chronological_holdout_multiframe",
        "weights": str(weights.resolve()),
        "fixtures": [result.get("fixture") for result in fixture_results],
        "devices": sorted({str(result.get("device") or "unknown") for result in fixture_results}),
        "cells": cells,
        "summary": {
            "n": total,
            "direction_accuracy": weighted("direction_accuracy"),
            "win_rate": weighted("win_rate"),
            "avg_net_pips": weighted("avg_net_pips"),
            "sum_net_pips": float(
                sum(float((result.get("summary") or {}).get("sum_net_pips") or 0.0) for result in fixture_results)
            ),
        },
        "production_eligible": False,
        "account_wired": False,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weights-config", type=Path, default=DEFAULT_WEIGHTS)
    parser.add_argument("--runtime-config", type=Path, default=DEFAULT_RUNTIMES)
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--additional-fixture", type=Path, action="append", default=[])
    parser.add_argument("--models", default="")
    parser.add_argument("--output", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--device", choices=("auto", "cpu", "cuda"), default="auto")
    parser.add_argument("--stop-on-error", action="store_true")
    parser.add_argument("--reuse-completed", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    weights = json.loads(args.weights_config.read_text(encoding="utf-8-sig"))["models"]
    profiles = json.loads(args.runtime_config.read_text(encoding="utf-8-sig"))["profiles"]
    requested = {value.strip() for value in args.models.split(",") if value.strip()}
    fixtures = [args.fixture.resolve(), *(path.resolve() for path in args.additional_fixture)]
    records: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    requested_models = 0
    base_environment = os.environ.copy()
    base_environment["PYTHONUTF8"] = "1"
    for model, value in weights.items():
        if requested and model not in requested:
            continue
        requested_models += 1
        profile_name = str(value["profile"])
        profile = profiles.get(profile_name)
        destination = runtime_profiles.expand_path(str(value["destination"])).resolve()
        if profile is None:
            record = {"model": model, "status": "runtime_profile_missing"}
            records.append({**record, "account_wired": False})
        else:
            python = runtime_profiles.expand_path(str(profile["python"])).resolve()
            environment = base_environment.copy()
            profile_paths = [
                str(runtime_profiles.expand_path(str(path)).resolve())
                for path in profile.get("pythonpath", [])
            ]
            existing_pythonpath = environment.get("PYTHONPATH", "")
            environment["PYTHONPATH"] = os.pathsep.join(
                [str(ROOT), *profile_paths, *([existing_pythonpath] if existing_pythonpath else [])]
            )
            if not python.is_file():
                record = {"model": model, "status": "runtime_missing", "python": str(python)}
                records.append({**record, "account_wired": False})
            elif not (destination / "WEIGHT_MANIFEST.json").is_file():
                record = {"model": model, "status": "weights_missing", "weights": str(destination)}
                records.append({**record, "account_wired": False})
            else:
                fixture_results: list[dict[str, Any]] = []
                for fixture in fixtures:
                    suffix = f"_{fixture.stem}" if len(fixtures) > 1 else ""
                    output = args.output.parent / f"foundation_{model}{suffix}_market_latest.json"
                    command = [
                        str(python),
                        str(ROOT / "oanda_foundation_market_worker.py"),
                        "--model",
                        model,
                        "--weights",
                        str(destination),
                        "--fixture",
                        str(fixture),
                        "--output",
                        str(output),
                        "--device",
                        args.device,
                    ]
                    reused = (
                        reusable_result(
                            output,
                            model,
                            destination,
                            fixture,
                            requested_device=args.device,
                        )
                        if args.reuse_completed
                        else None
                    )
                    if reused is not None:
                        print(f"[foundation-benchmark] {model} {fixture.stem} (verified reuse)", flush=True)
                        record = {
                            "model": model,
                            "fixture": str(fixture),
                            "status": "completed",
                            "returncode": 0,
                            "output": str(output),
                            "reused": True,
                            "stdout_tail": "verified existing market result",
                            "stderr_tail": "",
                        }
                        fixture_results.append(reused)
                    else:
                        print(f"[foundation-benchmark] {model} {fixture.stem}", flush=True)
                        completed = subprocess.run(
                            command,
                            cwd=ROOT,
                            env=environment,
                            capture_output=True,
                            text=True,
                            encoding="utf-8",
                            errors="replace",
                            check=False,
                        )
                        record = {
                            "model": model,
                            "fixture": str(fixture),
                            "status": "completed" if completed.returncode == 0 else "failed",
                            "returncode": completed.returncode,
                            "output": str(output),
                            "reused": False,
                            "stdout_tail": output_tail(completed.stdout),
                            "stderr_tail": output_tail(completed.stderr),
                        }
                        if completed.returncode == 0 and output.is_file():
                            fixture_results.append(json.loads(output.read_text(encoding="utf-8-sig")))
                    records.append({**record, "account_wired": False})
                    if record["status"] == "failed" and args.stop_on_error:
                        break
                if len(fixture_results) == len(fixtures):
                    results.append(combine_fixture_results(model, destination, fixture_results))
        write_json_atomic(
            args.output,
            {
                "schema_version": 1,
                "generated_utc": utc_iso(),
                "status": "running",
                "records": records,
                "results": results,
                "account_wired": False,
            },
        )
        if records[-1]["status"] in {"failed", "runtime_missing"} and args.stop_on_error:
            break
    ranked = sorted(
        results,
        key=lambda row: (
            row["summary"]["avg_net_pips"],
            row["summary"]["direction_accuracy"],
        ),
        reverse=True,
    )
    report = {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "status": "complete",
        "execution_policy": "shadow_only_no_account_wiring",
        "requested_device": args.device,
        "fixtures": [str(path) for path in fixtures],
        "records": records,
        "results": results,
        "ranking": [row["model"] for row in ranked],
        "summary": {
            "requested": requested_models,
            "scored": len(results),
            "failed_or_blocked": requested_models - len(results),
            "production_eligible": 0,
            "account_wired": 0,
        },
        "account_wired": False,
    }
    write_json_atomic(args.output, report)
    print(json.dumps(report["summary"], indent=2))
    return 0 if len(results) == requested_models else 1


if __name__ == "__main__":
    raise SystemExit(main())
