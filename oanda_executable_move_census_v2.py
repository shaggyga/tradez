#!/usr/bin/env python3
"""Separated all-68 executable-move capture and evaluation workers.

The ``capture`` mode performs only the minute-bound quote read and durable
frame commit.  The ``evaluate`` mode performs every cold reconstruction,
window maturity, review-queue, and report operation in a separate process.
Both modes are research-only and have no broker, order, authorization, or
promotion surface.  No prior census row is imported or backfilled.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import datetime as dt
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterator, Mapping, Sequence

import oanda_executable_move_census_v1 as base


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
CONFIG = ROOT / "config" / "executable_move_census_v2_20260901c.json"
QUOTES = STATE / "practice_007_market_quotes_v1.json"
DATABASE = STATE / "executable_move_census_v2_20260901c.sqlite"
LATEST = STATE / "executable_move_census_latest_v2_20260901c.json"
CAPTURE_HEARTBEAT = STATE / "executable_move_census_capture_heartbeat_v2_20260901c.json"
EVALUATOR_HEARTBEAT = STATE / "executable_move_census_evaluator_heartbeat_v2_20260901c.json"

UTC = dt.timezone.utc
CAPTURE_HEARTBEAT_SCHEMA = "executable_move_census_capture_heartbeat_v2"
EVALUATOR_HEARTBEAT_SCHEMA = "executable_move_census_evaluator_heartbeat_v2"


def producer_sha256() -> str:
    return base.sha(Path(__file__).read_bytes())


@contextmanager
def producer_identity() -> Iterator[None]:
    original = base.source_code_sha
    base.source_code_sha = producer_sha256
    try:
        yield
    finally:
        base.source_code_sha = original


def open_database(
    path: Path, cfg: Mapping[str, Any], config_raw: bytes
) -> sqlite3.Connection:
    with producer_identity():
        return base.open_db(path, cfg, config_raw)


def preactivation_payload(cfg: Mapping[str, Any], observed: dt.datetime) -> dict[str, Any]:
    return {
        "schema_version": "executable_move_census_latest_v1",
        "generated_utc": base.iso(observed),
        "cohort_id": cfg["cohort_id"],
        "status": "collecting_pre_activation",
        "activation_utc": cfg["activation_utc"],
        "instrument_count": 68,
        "side_count": 136,
        "horizons_min": cfg["horizons_min"],
        "research_only": True,
        "can_trade": False,
        "can_authorize": False,
        "can_promote": False,
        "frame_count": 0,
        "horizons": [],
        "top_cleared_paths": [],
        "review_queue": {},
    }


def validate_source_contract(
    cfg: Mapping[str, Any], raw: bytes
) -> Mapping[str, Any]:
    """Fail before activation or insertion when the live source identity drifts."""

    payload = json.loads(raw)
    observed_schema = str(payload.get("schema_version"))
    observed_producer = str(payload.get("producer"))
    expected_schema = str(cfg["source_schema_version"])
    expected_producer = str(cfg["source_producer"])
    if (
        observed_schema != expected_schema
        or observed_producer != expected_producer
    ):
        raise RuntimeError(
            "source_contract_mismatch:"
            f"schema={observed_schema!r}!={expected_schema!r}:"
            f"producer={observed_producer!r}!={expected_producer!r}"
        )
    return payload


def capture_once(
    *,
    config_path: Path = CONFIG,
    quotes_path: Path = QUOTES,
    database_path: Path = DATABASE,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    cfg, config_raw = base.load_config(config_path)
    raw = quotes_path.read_bytes()
    validate_source_contract(cfg, raw)
    read_started = (now or dt.datetime.now(UTC)).astimezone(UTC)
    activation = base.parse_utc(cfg["activation_utc"])
    if activation is None or read_started < activation:
        return {
            "status": "collecting_pre_activation",
            "activation_utc": cfg["activation_utc"],
            "frame_inserted": False,
            "frame_count": 0,
            "latest_frame_utc": None,
        }
    captured = (now or dt.datetime.now(UTC)).astimezone(UTC)
    frame, rows = base.build_frame(
        cfg, raw, read_started, captured, captured
    )
    scheduled = base.parse_utc(frame["scheduled_utc"])
    if scheduled is None:
        raise ValueError("invalid_scheduled_clock")
    inserted = False
    last_error = ""
    attempt = 0
    while dt.datetime.now(UTC) < scheduled + dt.timedelta(seconds=50):
        attempt += 1
        connection: sqlite3.Connection | None = None
        try:
            connection = open_database(database_path, cfg, config_raw)
            inserted = base.insert_frame(
                connection, frame, rows, cfg, precommit=now
            )
            count = int(connection.execute("SELECT COUNT(*) FROM frames").fetchone()[0])
            latest = connection.execute(
                "SELECT MAX(scheduled_utc) FROM frames"
            ).fetchone()[0]
            return {
                "status": "ok",
                "frame_inserted": bool(inserted),
                "frame_id": frame["frame_id"],
                "scheduled_utc": frame["scheduled_utc"],
                "captured_utc": frame["captured_utc"],
                "frame_count": count,
                "latest_frame_utc": latest,
                "attempt_count": attempt,
                "commit_before_cold_work": True,
            }
        except sqlite3.OperationalError as exc:
            last_error = str(exc)
            if "locked" not in last_error.lower():
                raise
            time.sleep(min(0.5, 0.05 * attempt))
        finally:
            if connection is not None:
                connection.close()
    raise TimeoutError(f"capture_commit_deadline_exceeded:{last_error}")


def evaluate_once(
    *,
    config_path: Path = CONFIG,
    database_path: Path = DATABASE,
    output_path: Path = LATEST,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    cfg, config_raw = base.load_config(config_path)
    generated = (now or dt.datetime.now(UTC)).astimezone(UTC)
    activation = base.parse_utc(cfg["activation_utc"])
    if activation is None or generated < activation:
        payload = preactivation_payload(cfg, generated)
        payload["content_sha256_excluding_this_field"] = base.sha(
            base.canonical(payload)
        )
        base.write_json_atomic(output_path, payload)
        return payload
    connection = open_database(database_path, cfg, config_raw)
    try:
        base.reconcile_window_evaluations(connection, cfg, generated)
        base.finalize_factor_episodes(connection, cfg, generated)
        payload = base.build_latest(connection, cfg, generated)
        payload["frame_inserted"] = False
        base.populate_review_queue(connection, payload, cfg)
        payload.pop("_all_cleared_paths", None)
        payload.pop("_all_path_clears", None)
        payload["content_sha256_excluding_this_field"] = base.sha(
            base.canonical(payload)
        )
        base.write_json_atomic(output_path, payload)
        return payload
    finally:
        connection.close()


def heartbeat(
    *, schema: str, mode: str, payload: Mapping[str, Any], error: str = ""
) -> dict[str, Any]:
    return {
        "schema_version": schema,
        "contract_id": "all68_capture_cold_work_separation_v2_20260901",
        "generated_utc": base.iso(dt.datetime.now(UTC)),
        "mode": mode,
        "status": "degraded" if error else str(payload.get("status") or "ok"),
        "phase": "error" if error else "idle",
        "frame_count": int(payload.get("frame_count") or 0),
        "latest_frame_utc": payload.get("latest_frame_utc"),
        "frame_inserted": bool(payload.get("frame_inserted")),
        "error": error,
        "research_only": True,
        "can_trade": False,
        "can_authorize": False,
        "can_promote": False,
    }


def capture_loop(args: argparse.Namespace) -> None:
    started = time.monotonic()
    if args.once:
        result = capture_once(
            config_path=args.config,
            quotes_path=args.quotes,
            database_path=args.database,
        )
        base.write_json_atomic(
            args.heartbeat,
            heartbeat(schema=CAPTURE_HEARTBEAT_SCHEMA, mode="capture", payload=result),
        )
        return
    now = dt.datetime.now(UTC)
    cadence = 60
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
            try:
                result = capture_once(
                    config_path=args.config,
                    quotes_path=args.quotes,
                    database_path=args.database,
                )
                last_result = dict(result)
                last_error = ""
                hb = heartbeat(
                    schema=CAPTURE_HEARTBEAT_SCHEMA,
                    mode="capture",
                    payload=result,
                )
            except Exception as exc:
                result = dict(last_result)
                last_error = f"{type(exc).__name__}: {exc}"
                hb = heartbeat(
                    schema=CAPTURE_HEARTBEAT_SCHEMA,
                    mode="capture",
                    payload=result,
                    error=last_error,
                )
            base.write_json_atomic(args.heartbeat, hb)
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
            hb["next_capture_utc"] = base.iso(
                dt.datetime.fromtimestamp(next_epoch, UTC)
            )
            base.write_json_atomic(args.heartbeat, hb)
        if args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec:
            return
        time.sleep(min(1.0, max(0.1, next_epoch - time.time())))


def evaluator_loop(args: argparse.Namespace) -> None:
    started = time.monotonic()
    while True:
        try:
            payload = evaluate_once(
                config_path=args.config,
                database_path=args.database,
                output_path=args.output,
            )
            hb = heartbeat(
                schema=EVALUATOR_HEARTBEAT_SCHEMA,
                mode="evaluate",
                payload=payload,
            )
        except Exception as exc:
            hb = heartbeat(
                schema=EVALUATOR_HEARTBEAT_SCHEMA,
                mode="evaluate",
                payload={},
                error=f"{type(exc).__name__}: {exc}",
            )
        base.write_json_atomic(args.heartbeat, hb)
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
