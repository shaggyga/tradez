#!/usr/bin/env python3
"""Continuously validate the signal stack and checkpoint proven changes.

Fast market-data and prediction workers are supervised elsewhere. This loop is
the slower control plane: refresh contracts, recreate gap coverage, run focused
tests, summarize live challenger evidence, then sync the source/results to the
vault only after the validation stage passes.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
TRAD = ROOT / "trad"
DATA = TRAD / "data" / "oanda_training_manager"
DEFAULT_STATE = DATA / "state" / "continuous_improvement_loop_v1.json"
DEFAULT_VAULT = Path.home() / "OneDrive" / "thevault" / "projects" / "forex"
MIN_CONTINUOUS_VAULT_INTERVAL_SEC = 14400.0
FOCUSED_TESTS = (
    "trad.test_oanda_signal_contribution_feed",
    "trad.test_oanda_model_gap_live_signal_producer",
    "trad.test_oanda_model_gap_live_signal_worker",
    "trad.test_oanda_order_position_book_collector",
    "trad.test_oanda_prospective_live_panel",
    "trad.test_oanda_prospective_model_refresh",
    "trad.test_oanda_practice_shadow_strategy_lab",
    "trad.test_oanda_signal_combination_audit",
    "trad.test_oanda_model_gap_registry",
    "trad.test_oanda_news_outcome_improvement_audit",
    "trad.test_oanda_historical_miss_regression",
    "trad.test_forex_model_vault_sync",
    "trad.test_oanda_continuous_improvement_loop",
)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        max_replace_attempts = 9
        for attempt in range(max_replace_attempts):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt == max_replace_attempts - 1:
                    raise
                time.sleep(min(0.25, 0.01 * (2**attempt)))
    finally:
        temporary.unlink(missing_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def run_command(name: str, command: list[str], timeout_sec: int) -> dict[str, Any]:
    started = time.time()
    try:
        environment = os.environ.copy()
        existing_pythonpath = environment.get("PYTHONPATH", "")
        environment["PYTHONPATH"] = os.pathsep.join(
            value
            for value in (str(ROOT), str(TRAD), existing_pythonpath)
            if value
        )
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            timeout=timeout_sec,
            check=False,
        )
        return {
            "name": name,
            "status": "passed" if completed.returncode == 0 else "failed",
            "returncode": completed.returncode,
            "elapsed_sec": round(time.time() - started, 3),
            "stdout_tail": completed.stdout[-4000:],
            "stderr_tail": completed.stderr[-4000:],
            "command": command,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "name": name,
            "status": "timeout",
            "returncode": None,
            "elapsed_sec": round(time.time() - started, 3),
            "stdout_tail": str(exc.stdout or "")[-4000:],
            "stderr_tail": str(exc.stderr or "")[-4000:],
            "command": command,
        }
    except Exception as exc:
        return {
            "name": name,
            "status": "error",
            "returncode": None,
            "elapsed_sec": round(time.time() - started, 3),
            "stderr_tail": f"{type(exc).__name__}: {exc}",
            "command": command,
        }


def runtime_health(now: float) -> dict[str, Any]:
    paths = {
        "strategy_lab": DATA / "state" / "strategy_lab_heartbeat_v1.json",
        "model_gap_live": DATA / "state" / "model_gap_live_worker_v1.json",
        "pricing_depth": DATA / "prospective_depth_parquet" / "collector_state.json",
        "order_position_book": DATA / "prospective_order_position_book" / "collector_state.json",
    }
    rows = {}
    for name, path in paths.items():
        age = None
        if path.is_file():
            age = max(0.0, now - path.stat().st_mtime)
        rows[name] = {
            "path": str(path.resolve()),
            "exists": path.is_file(),
            "age_sec": None if age is None else round(age, 3),
            "fresh": age is not None and age <= (1800.0 if name == "order_position_book" else 600.0),
            "state": read_json(path),
        }
    return {
        "workers": rows,
        "fresh_workers": sum(bool(row["fresh"]) for row in rows.values()),
        "expected_workers": len(rows),
        "credentials_in_vault_required": False,
    }


def validation_commands() -> list[tuple[str, list[str], int]]:
    return [
        (
            "feature_space_refresh",
            [sys.executable, str(TRAD / "oanda_model_feature_space.py")],
            300,
        ),
        (
            "signal_engine_contract",
            [sys.executable, str(TRAD / "oanda_signal_engine_refresh_audit.py")],
            300,
        ),
        (
            "prospective_panel_integrity",
            [
                sys.executable,
                str(TRAD / "oanda_prospective_live_panel.py"),
                "--validate-existing",
            ],
            120,
        ),
        (
            "prospective_model_refresh",
            [sys.executable, str(TRAD / "oanda_prospective_model_refresh.py")],
            1800,
        ),
        (
            "model_gap_completion",
            [
                sys.executable,
                str(TRAD / "oanda_model_gap_completion.py"),
            ],
            600,
        ),
        (
            "model_gap_unified_report",
            [
                sys.executable,
                str(TRAD / "oanda_model_gap_unified_report.py"),
            ],
            600,
        ),
        (
            "news_outcome_improvement_record_integrity",
            [
                sys.executable,
                str(TRAD / "oanda_news_outcome_improvement_audit.py"),
                "--validate-existing",
            ],
            120,
        ),
        (
            "focused_tests",
            [sys.executable, "-m", "unittest", *FOCUSED_TESTS],
            1800,
        ),
    ]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--vault", type=Path, default=DEFAULT_VAULT)
    parser.add_argument("--interval-sec", type=float, default=60.0)
    parser.add_argument("--validation-interval-sec", type=float, default=3600.0)
    parser.add_argument("--vault-interval-sec", type=float, default=3600.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--no-vault-sync", action="store_true")
    parser.add_argument(
        "--defer-initial-vault-sync",
        action="store_true",
        default=True,
        help="Wait one vault interval before the first checkpoint in this process.",
    )
    parser.add_argument(
        "--immediate-initial-vault-sync",
        dest="defer_initial_vault_sync",
        action="store_false",
        help="Checkpoint immediately after validation (intended for manual one-shot runs).",
    )
    args = parser.parse_args(argv)
    if min(
        args.interval_sec,
        args.validation_interval_sec,
        args.vault_interval_sec,
        args.duration_sec,
    ) <= 0.0:
        raise SystemExit("loop timing arguments must be positive")
    return args


def run(args: argparse.Namespace) -> int:
    global ROOT, TRAD, DATA
    ROOT = args.root.resolve()
    TRAD = ROOT / "trad"
    DATA = TRAD / "data" / "oanda_training_manager"
    if not args.once:
        args.vault_interval_sec = max(
            args.vault_interval_sec,
            MIN_CONTINUOUS_VAULT_INTERVAL_SEC,
        )
    stop_at = time.monotonic() + args.duration_sec
    next_validation = 0.0
    next_vault = (
        time.monotonic() + args.vault_interval_sec
        if args.defer_initial_vault_sync
        else 0.0
    )
    cycles = 0
    last_validation: list[dict[str, Any]] = []
    validation_passed = False
    last_vault: dict[str, Any] = {}
    while time.monotonic() < stop_at:
        now = time.time()
        phase = "observing"
        if time.monotonic() >= next_validation:
            phase = "validating"
            last_validation = [
                run_command(name, command, timeout)
                for name, command, timeout in validation_commands()
            ]
            validation_passed = all(row["status"] == "passed" for row in last_validation)
            next_validation = time.monotonic() + args.validation_interval_sec
        if (
            validation_passed
            and not args.no_vault_sync
            and time.monotonic() >= next_vault
        ):
            phase = "vault_sync"
            last_vault = run_command(
                "validated_vault_sync",
                [
                    sys.executable,
                    str(TRAD / "forex_model_vault_sync.py"),
                    "--root",
                    str(ROOT),
                    "--destination",
                    str(args.vault.resolve()),
                ],
                1800,
            )
            next_vault = time.monotonic() + args.vault_interval_sec
        live_state = read_json(DATA / "state" / "model_gap_live_worker_v1.json")
        refresh_state = read_json(DATA / "state" / "prospective_model_refresh_v1.json")
        completion = read_json(DATA / "model_space" / "model_gap_completion_latest.json")
        completion_summary = completion.get("summary") or {}
        cycles += 1
        payload = {
            "schema_version": 1,
            "updated_utc": utc_iso(),
            "phase": phase,
            "control_role": "validation_service_under_improvement_control_engine",
            "cycles": cycles,
            "root": str(ROOT),
            "python": sys.executable,
            "validation_passed": validation_passed,
            "validation": last_validation,
            "runtime_health": runtime_health(now),
            "live_model_evidence": live_state.get("ledger") or {},
            "model_expansion": {
                "registered_gap_models": 30,
                "currently_live_artifact_models": (
                    (live_state.get("producer") or {}).get("loaded_models") or []
                ),
                "prospective_refresh": refresh_state,
                "all_fresh_outputs_route_to_007_research_feed": True,
                "unsupported_or_unfitted_models_emit_forecasts": False,
                "next_action": "accumulate executable live outcomes, retrain on new feature archive, then rerun purged validation",
                "evidence_complete": bool(completion_summary.get("evidence_complete")),
                "pending_evidence_gaps": completion_summary.get("incomplete_reasons") or [],
            },
            "vault_sync": last_vault,
            "vault_interval_sec": args.vault_interval_sec,
            "initial_vault_sync_deferred": args.defer_initial_vault_sync,
            "contract": {
                "order_of_operations": [
                    "collect",
                    "predict",
                    "mature_outcomes",
                    "validate",
                    "expand_or_retrain",
                    "validate_again",
                    "vault_sync",
                ],
                "vault_sync_requires_code_validation": True,
                "vault_sync_does_not_claim_evidence_completion": True,
                "account_promotion_requires_independent_live_evidence": True,
                "credentials_copied_to_vault": False,
                "account_orders_submitted_by_this_loop": 0,
            },
        }
        atomic_json(args.state, payload)
        if args.once:
            return 0 if validation_passed else 2
        time.sleep(min(args.interval_sec, max(0.0, stop_at - time.monotonic())))
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
