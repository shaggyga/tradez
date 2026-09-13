#!/usr/bin/env python3
"""Retrying, incident-logged all-68 executable-move census worker.

Version 3 preserves the separated capture/evaluation architecture from V2,
but makes minute-bound capture failures durable.  A transient source-read or
database error is retried inside the same declared minute until the frozen
commit deadline.  Every failed attempt and every missed wake-up is appended to
an immutable incident log.  The worker is research-only and has no broker,
authorization, promotion, or order surface.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import datetime as dt
import json
import os
from pathlib import Path
import time
from typing import Any, Iterator, Mapping, Sequence

import oanda_executable_move_census_v2 as prior


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
LOGS = ROOT / "data" / "oanda_training_manager" / "logs"
CONFIG = ROOT / "config" / "executable_move_census_v3_20260901f.json"
QUOTES = STATE / "practice_007_market_quotes_v1.json"
DATABASE = STATE / "executable_move_census_v3_20260901f.sqlite"
LATEST = STATE / "executable_move_census_latest_v3_20260901f.json"
CAPTURE_HEARTBEAT = (
    STATE / "executable_move_census_capture_heartbeat_v3_20260901f.json"
)
EVALUATOR_HEARTBEAT = (
    STATE / "executable_move_census_evaluator_heartbeat_v3_20260901f.json"
)
CAPTURE_INCIDENTS = (
    LOGS / "executable_move_census_capture_incidents_v3_20260901f.jsonl"
)

UTC = dt.timezone.utc
CAPTURE_HEARTBEAT_SCHEMA = "executable_move_census_capture_heartbeat_v3"
EVALUATOR_HEARTBEAT_SCHEMA = "executable_move_census_evaluator_heartbeat_v3"
WORKER_CONTRACT_ID = "all68_capture_retry_incident_log_v3_20260901"


def producer_sha256() -> str:
    return prior.base.sha(Path(__file__).read_bytes())


@contextmanager
def producer_identity() -> Iterator[None]:
    """Make the inherited database contract bind this V3 source file."""

    original = prior.producer_sha256
    prior.producer_sha256 = producer_sha256
    try:
        yield
    finally:
        prior.producer_sha256 = original


def capture_once(
    *,
    config_path: Path = CONFIG,
    quotes_path: Path = QUOTES,
    database_path: Path = DATABASE,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    with producer_identity():
        return prior.capture_once(
            config_path=config_path,
            quotes_path=quotes_path,
            database_path=database_path,
            now=now,
        )


def evaluate_once(
    *,
    config_path: Path = CONFIG,
    database_path: Path = DATABASE,
    output_path: Path = LATEST,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    with producer_identity():
        return prior.evaluate_once(
            config_path=config_path,
            database_path=database_path,
            output_path=output_path,
            now=now,
        )


def heartbeat(
    *, schema: str, mode: str, payload: Mapping[str, Any], error: str = ""
) -> dict[str, Any]:
    value = prior.heartbeat(schema=schema, mode=mode, payload=payload, error=error)
    value["contract_id"] = WORKER_CONTRACT_ID
    return value


def append_incident(path: Path, payload: Mapping[str, Any]) -> None:
    """Durably append one diagnostic without changing any evidence row."""

    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(dict(payload), sort_keys=True, separators=(",", ":"))
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(line + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _iso_epoch(value: float) -> str:
    return prior.base.iso(dt.datetime.fromtimestamp(value, UTC))


def _incident(
    *,
    incident_type: str,
    target_epoch: float,
    attempt: int,
    error: str,
    terminal: bool,
) -> dict[str, Any]:
    return {
        "schema_version": "executable_move_census_capture_incident_v1",
        "contract_id": WORKER_CONTRACT_ID,
        "observed_utc": prior.base.iso(dt.datetime.now(UTC)),
        "target_scheduled_utc": _iso_epoch(target_epoch),
        "incident_type": incident_type,
        "attempt": int(attempt),
        "error": error[:1000],
        "terminal_for_target_minute": bool(terminal),
        "research_only": True,
        "can_trade": False,
        "can_authorize": False,
        "can_promote": False,
    }


def capture_target_with_retries(
    args: argparse.Namespace,
    *,
    target_epoch: float,
    last_result: Mapping[str, Any],
    capture_operation: Any = None,
    clock: Any = None,
    sleeper: Any = None,
) -> tuple[dict[str, Any], str, bool, int]:
    """Attempt one declared minute until success or its frozen deadline.

    The injectable functions keep the retry and incident behavior directly
    falsifiable without sleeping through a real minute in tests.
    """

    operation = capture_operation or capture_once
    now_epoch = clock or time.time
    sleep = sleeper or time.sleep
    deadline_epoch = target_epoch + 50
    attempt = 0
    completed = False
    result = dict(last_result)
    last_error = ""
    while now_epoch() < deadline_epoch:
        attempt += 1
        try:
            result = operation(
                config_path=args.config,
                quotes_path=args.quotes,
                database_path=args.database,
            )
            completed = True
            last_error = ""
            hb = heartbeat(
                schema=CAPTURE_HEARTBEAT_SCHEMA,
                mode="capture",
                payload=result,
            )
            hb.update({
                "capture_target_utc": _iso_epoch(target_epoch),
                "attempt_count": attempt,
                "retry_deadline_utc": _iso_epoch(deadline_epoch),
                "incident_log": str(args.incident_log.resolve()),
            })
            prior.base.write_json_atomic(args.heartbeat, hb)
            break
        except Exception as exc:  # fail closed and retain exact error
            last_error = f"{type(exc).__name__}: {exc}"
            append_incident(
                args.incident_log,
                _incident(
                    incident_type="capture_attempt_failed",
                    target_epoch=target_epoch,
                    attempt=attempt,
                    error=last_error,
                    terminal=False,
                ),
            )
            hb = heartbeat(
                schema=CAPTURE_HEARTBEAT_SCHEMA,
                mode="capture",
                payload=result,
                error=last_error,
            )
            hb.update({
                "phase": "retrying_capture_before_deadline",
                "capture_target_utc": _iso_epoch(target_epoch),
                "attempt_count": attempt,
                "retry_deadline_utc": _iso_epoch(deadline_epoch),
                "incident_log": str(args.incident_log.resolve()),
            })
            prior.base.write_json_atomic(args.heartbeat, hb)
            if "source_contract_mismatch" in last_error:
                break
            sleep(min(0.5, 0.05 * attempt))
    if not completed:
        append_incident(
            args.incident_log,
            _incident(
                incident_type="capture_target_terminal_failure",
                target_epoch=target_epoch,
                attempt=attempt,
                error=last_error or "capture_deadline_elapsed",
                terminal=True,
            ),
        )
    return dict(result), last_error, completed, attempt


def capture_loop(args: argparse.Namespace) -> None:
    started = time.monotonic()
    if args.once:
        result = capture_once(
            config_path=args.config,
            quotes_path=args.quotes,
            database_path=args.database,
        )
        prior.base.write_json_atomic(
            args.heartbeat,
            heartbeat(schema=CAPTURE_HEARTBEAT_SCHEMA, mode="capture", payload=result),
        )
        return

    cadence = 60
    now = dt.datetime.now(UTC)
    next_epoch = ((int(now.timestamp()) // cadence) + 1) * cadence + 1
    last_result: dict[str, Any] = {
        "status": "collecting_pre_activation",
        "frame_count": 0,
        "latest_frame_utc": None,
        "frame_inserted": False,
    }
    last_error = ""
    while True:
        now = dt.datetime.now(UTC)
        if now.timestamp() >= next_epoch:
            expected_target = next_epoch - 1
            current_target = (int(now.timestamp()) // cadence) * cadence
            if current_target > expected_target:
                skipped = expected_target
                while skipped < current_target:
                    append_incident(
                        args.incident_log,
                        _incident(
                            incident_type="capture_wakeup_missed_deadline",
                            target_epoch=skipped,
                            attempt=0,
                            error=(
                                f"worker_woke_at={prior.base.iso(now)};"
                                f"expected_boundary={_iso_epoch(expected_target + 1)}"
                            ),
                            terminal=True,
                        ),
                    )
                    skipped += cadence
                expected_target = current_target

            result, last_error, completed, _ = capture_target_with_retries(
                args,
                target_epoch=expected_target,
                last_result=last_result,
            )
            if completed:
                last_result = result
                last_error = ""
            current_epoch = int(dt.datetime.now(UTC).timestamp())
            next_epoch = ((current_epoch // cadence) + 1) * cadence + 1
        else:
            wait_payload = {
                **last_result,
                "status": "awaiting_absolute_minute_boundary",
                "frame_inserted": False,
            }
            hb = heartbeat(
                schema=CAPTURE_HEARTBEAT_SCHEMA,
                mode="capture",
                payload=wait_payload,
                error=last_error,
            )
            hb["phase"] = (
                "awaiting_absolute_minute_boundary_after_error"
                if last_error
                else "awaiting_absolute_minute_boundary"
            )
            hb["next_capture_utc"] = _iso_epoch(next_epoch)
            hb["incident_log"] = str(args.incident_log.resolve())
            prior.base.write_json_atomic(args.heartbeat, hb)
        if args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec:
            return
        time.sleep(min(1.0, max(0.1, next_epoch - time.time())))


def evaluator_loop(args: argparse.Namespace) -> None:
    started = time.monotonic()
    while True:
        try:
            result = evaluate_once(
                config_path=args.config,
                database_path=args.database,
                output_path=args.output,
            )
            hb = heartbeat(
                schema=EVALUATOR_HEARTBEAT_SCHEMA,
                mode="evaluate",
                payload={
                    "status": "ok",
                    "frame_count": int(result.get("frame_count") or 0),
                    "latest_frame_utc": result.get("latest_frame_utc"),
                    "frame_inserted": False,
                },
            )
        except Exception as exc:
            hb = heartbeat(
                schema=EVALUATOR_HEARTBEAT_SCHEMA,
                mode="evaluate",
                payload={},
                error=f"{type(exc).__name__}: {exc}",
            )
        prior.base.write_json_atomic(args.heartbeat, hb)
        if args.once or (
            args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec
        ):
            return
        time.sleep(max(1.0, args.interval_sec))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("capture", "evaluate"))
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--quotes", type=Path, default=QUOTES)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--output", type=Path, default=LATEST)
    parser.add_argument("--heartbeat", type=Path)
    parser.add_argument("--incident-log", type=Path, default=CAPTURE_INCIDENTS)
    parser.add_argument("--worker-id", default="")
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    if args.heartbeat is None:
        args.heartbeat = (
            CAPTURE_HEARTBEAT if args.mode == "capture" else EVALUATOR_HEARTBEAT
        )
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.mode == "capture":
        capture_loop(args)
    else:
        evaluator_loop(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
