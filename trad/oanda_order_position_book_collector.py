#!/usr/bin/env python3
"""Collect compact OANDA order/position-book features for shadow research.

These endpoints describe distributions of OANDA client orders and positions;
they are not a centralized interbank limit-order book. The collector stores
derived, low-frequency features and never submits an order.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

try:
    import oanda_gpt_training_strategy_manager as manager
except ModuleNotFoundError:
    from trad import oanda_gpt_training_strategy_manager as manager


ROOT = manager.TRAINING_ROOT / "prospective_order_position_book"
LATEST_PATH = manager.TRAINING_ROOT / "state" / "oanda_order_position_book_latest_v1.json"
STATE_PATH = ROOT / "collector_state.json"
ERROR_PATH = ROOT / "collector_errors.jsonl"
WINDOWS_PIPS = (5, 10, 25, 50)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def finite(value: Any, default: float = 0.0) -> float:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return default
    return output if math.isfinite(output) else default


def pip_size(instrument: str) -> float:
    return 0.01 if instrument.endswith("_JPY") else 0.0001


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def append_error(**payload: Any) -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    with ERROR_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"time_utc": utc_now().isoformat(), **payload}) + "\n")


def permanently_unavailable(response: dict[str, Any]) -> bool:
    try:
        status = int(response.get("_http_status") or 0)
    except (TypeError, ValueError):
        status = 0
    message = str(
        response.get("_error_text") or response.get("_exception") or ""
    ).lower()
    return status in {400, 404} and (
        "not a valid instrument" in message
        or "invalid instrument" in message
    )


def concentration(values: list[float]) -> float:
    total = sum(max(0.0, value) for value in values)
    if total <= 0.0:
        return 0.0
    return sum((max(0.0, value) / total) ** 2 for value in values)


def summarize_book(
    instrument: str,
    kind: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    key = "orderBook" if kind == "order" else "positionBook"
    book = payload.get(key) or {}
    buckets = [row for row in book.get("buckets") or [] if isinstance(row, dict)]
    if not buckets:
        raise ValueError(f"{key} response has no buckets")
    current = finite(book.get("price"))
    if current <= 0.0:
        raise ValueError(f"{key} response has no current price")
    pip = pip_size(instrument)
    parsed = []
    for bucket in buckets:
        price = finite(bucket.get("price"))
        long_count = max(0.0, finite(bucket.get("longCountPercent")))
        short_count = max(0.0, finite(bucket.get("shortCountPercent")))
        if price <= 0.0:
            continue
        parsed.append(
            {
                "distance_pips": (price - current) / pip,
                "long": long_count,
                "short": short_count,
            }
        )
    if not parsed:
        raise ValueError(f"{key} response has no valid buckets")
    prefix = f"{kind}_book"
    output: dict[str, Any] = {
        f"{prefix}_time_utc": str(book.get("time") or ""),
        f"{prefix}_price": current,
        f"{prefix}_bucket_width": finite(book.get("bucketWidth")),
        f"{prefix}_bucket_count": len(parsed),
        f"{prefix}_concentration": concentration(
            [row["long"] + row["short"] for row in parsed]
        ),
    }
    for window in WINDOWS_PIPS:
        selected = [
            row
            for row in parsed
            if abs(row["distance_pips"]) <= window + 1e-9
        ]
        long_total = sum(row["long"] for row in selected)
        short_total = sum(row["short"] for row in selected)
        denominator = long_total + short_total
        output[f"{prefix}_near_{window}_imbalance"] = (
            (long_total - short_total) / denominator if denominator > 0.0 else 0.0
        )
    above = [row for row in parsed if 0.0 < row["distance_pips"] <= 25.0]
    below = [row for row in parsed if -25.0 <= row["distance_pips"] < 0.0]
    output[f"{prefix}_above_25_net"] = sum(
        row["long"] - row["short"] for row in above
    )
    output[f"{prefix}_below_25_net"] = sum(
        row["long"] - row["short"] for row in below
    )
    long_peak = max(parsed, key=lambda row: row["long"])
    short_peak = max(parsed, key=lambda row: row["short"])
    output[f"{prefix}_long_peak_distance_pips"] = long_peak["distance_pips"]
    output[f"{prefix}_short_peak_distance_pips"] = short_peak["distance_pips"]
    return output


def write_partition(rows: list[dict[str, Any]], collected: datetime) -> Path | None:
    if not rows:
        return None
    directory = (
        ROOT
        / f"date={collected.strftime('%Y%m%d')}"
        / f"hour={collected.strftime('%H')}"
    )
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"books_{os.getpid()}_{time.time_ns()}.parquet"
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    pq.write_table(
        pa.Table.from_pylist(rows),
        temporary,
        compression="zstd",
        use_dictionary=True,
    )
    os.replace(temporary, path)
    return path


def collect_cycle(
    client: manager.OandaClient,
    instruments: list[str],
    request_pause_sec: float,
) -> tuple[list[dict[str, Any]], dict[str, int], set[str]]:
    collected = utc_now()
    output: list[dict[str, Any]] = []
    errors = {"order": 0, "position": 0}
    unavailable: set[str] = set()
    for instrument in instruments:
        permanent_failures = 0
        endpoint_successes = 0
        row: dict[str, Any] = {
            "instrument": instrument,
            "collected_utc": collected.isoformat(),
            "source": "oanda_v20_aggregate_client_books",
        }
        for kind, endpoint in (
            ("order", "orderBook"),
            ("position", "positionBook"),
        ):
            response = client.request(
                "GET",
                f"/v3/instruments/{instrument}/{endpoint}",
            )
            if response.get("_error"):
                errors[kind] += 1
                permanent_failures += int(permanently_unavailable(response))
                append_error(
                    instrument=instrument,
                    kind=kind,
                    status=response.get("_http_status"),
                    error=str(response.get("_error_text") or response.get("_exception") or "")[:500],
                )
            else:
                endpoint_successes += 1
                try:
                    row.update(summarize_book(instrument, kind, response))
                except Exception as exc:
                    errors[kind] += 1
                    append_error(
                        instrument=instrument,
                        kind=kind,
                        error=f"{type(exc).__name__}: {exc}",
                    )
            if request_pause_sec > 0.0:
                time.sleep(request_pause_sec)
        if any(name.startswith("order_book_") or name.startswith("position_book_") for name in row):
            output.append(row)
        if permanent_failures == 2 and endpoint_successes == 0:
            unavailable.add(instrument)
    return output, errors, unavailable


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs", nargs="+", default=[])
    parser.add_argument("--all-tradeable", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=900.0)
    parser.add_argument("--request-pause-sec", type=float, default=0.10)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument("--latest-state", type=Path, default=LATEST_PATH)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    if args.interval_sec <= 0.0 or args.request_pause_sec < 0.0 or args.duration_sec <= 0.0:
        raise SystemExit("collector timing arguments are invalid")
    return args


def run(args: argparse.Namespace) -> int:
    manager.ensure_dirs()
    token, base_url, account_id = manager.resolve_oanda_creds(
        manager.load_creds(manager.CREDS_PATH)
    )
    if "practice" not in base_url.lower():
        raise SystemExit("book collection is restricted to the OANDA practice environment")
    client = manager.OandaClient(token, base_url, account_id)
    instruments = sorted(set(args.pairs))
    if args.all_tradeable or not instruments:
        instruments = client.list_instruments()
    requested_instruments = list(instruments)
    unavailable_instruments: set[str] = set()
    stop_at = time.monotonic() + args.duration_sec
    cycles = 0
    rows_collected = 0
    errors = 0
    last_partition = ""
    while time.monotonic() < stop_at:
        started = time.monotonic()
        rows, cycle_errors, cycle_unavailable = collect_cycle(
            client,
            instruments,
            args.request_pause_sec,
        )
        if args.all_tradeable and cycle_unavailable:
            unavailable_instruments.update(cycle_unavailable)
            instruments = [
                instrument
                for instrument in instruments
                if instrument not in unavailable_instruments
            ]
        collected_epoch = time.time()
        path = write_partition(rows, datetime.fromtimestamp(collected_epoch, timezone.utc))
        if path is not None:
            last_partition = str(path.resolve())
        latest = {
            "schema_version": 1,
            "generated_epoch": collected_epoch,
            "generated_utc": datetime.fromtimestamp(collected_epoch, timezone.utc).isoformat(),
            "source": "OANDA aggregate client order and position books; not centralized FX depth",
            "instrument_count": len(rows),
            "instruments": {
                str(row["instrument"]): {
                    key: value
                    for key, value in row.items()
                    if key not in {"instrument", "source"}
                }
                for row in rows
            },
        }
        atomic_json(args.latest_state, latest)
        cycles += 1
        rows_collected += len(rows)
        errors += sum(cycle_errors.values())
        state = {
            "schema_version": 1,
            "updated_utc": utc_now().isoformat(),
            "status": "running",
            "account_suffix": account_id[-4:],
            "requested_instruments": len(requested_instruments),
            "active_instruments": len(instruments),
            "instruments": len(instruments),
            "permanently_unavailable_instruments": sorted(unavailable_instruments),
            "permanently_unavailable_count": len(unavailable_instruments),
            "cycles": cycles,
            "rows_collected": rows_collected,
            "errors": errors,
            "last_cycle_errors": cycle_errors,
            "last_partition": last_partition,
            "latest_state": str(args.latest_state.resolve()),
            "interval_sec": args.interval_sec,
            "request_pause_sec": args.request_pause_sec,
            "storage": "derived rows in hourly zstd parquet; latest state is atomic JSON",
            "account_orders_submitted": 0,
        }
        atomic_json(STATE_PATH, state)
        if args.once:
            print(json.dumps(state, indent=2, sort_keys=True))
            return 0
        elapsed = time.monotonic() - started
        time.sleep(min(max(0.0, args.interval_sec - elapsed), max(0.0, stop_at - time.monotonic())))
    final_state = {
        "schema_version": 1,
        "updated_utc": utc_now().isoformat(),
        "status": "complete",
        "cycles": cycles,
        "rows_collected": rows_collected,
        "errors": errors,
        "last_partition": last_partition,
        "account_orders_submitted": 0,
    }
    atomic_json(STATE_PATH, final_state)
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
