#!/usr/bin/env python3
"""Download Dukascopy XAUUSD M1 candle data and convert it to CSV.

Dukascopy stores candle files as LZMA-compressed `.bi5` files. For candle files,
each record is:

    >5if: seconds_from_midnight, open, close, low, high, volume

For XAUUSD the OHLC integers are scaled by 1000 by default.
"""

from __future__ import annotations

import argparse
import csv
import lzma
import os
import struct
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


SCRIPT_DIR = Path(__file__).resolve().parent
DATA_ROOT = SCRIPT_DIR / "data" / "gold_m1_liquidity_scalper"
DUKASCOPY_BASE_URL = "https://datafeed.dukascopy.com/datafeed"
RECORD_SIZE = 24


@dataclass(frozen=True)
class Candle:
    time_utc: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def daterange(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def dukascopy_url(symbol: str, day: date, side: str) -> str:
    month_index = day.month - 1
    return (
        f"{DUKASCOPY_BASE_URL}/{symbol.upper()}/"
        f"{day.year:04d}/{month_index:02d}/{day.day:02d}/"
        f"{side.upper()}_candles_min_1.bi5"
    )


def raw_path(root: Path, symbol: str, day: date, side: str) -> Path:
    month_index = day.month - 1
    return (
        root
        / "raw"
        / "dukascopy"
        / symbol.upper()
        / f"{day.year:04d}"
        / f"{month_index:02d}"
        / f"{day.day:02d}"
        / f"{side.upper()}_candles_min_1.bi5"
    )


def download_file(url: str, path: Path, timeout: int, force: bool, retries: int) -> bool:
    if path.exists() and path.stat().st_size > 0 and not force:
        return True
    path.parent.mkdir(parents=True, exist_ok=True)
    request = Request(url, headers={"User-Agent": "gold-m1-research/1.0"})
    last_error: Exception | None = None
    for attempt in range(max(1, retries + 1)):
        try:
            with urlopen(request, timeout=timeout) as response:
                content = response.read()
            break
        except HTTPError as exc:
            if exc.code in {403, 404}:
                return False
            last_error = exc
        except (URLError, TimeoutError) as exc:
            last_error = exc
        if attempt < retries:
            time.sleep(1.0 + attempt)
    else:
        if last_error is not None:
            print(f"skip_download={url} error={last_error}")
        return False
    if not content:
        return False
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_bytes(content)
    os.replace(tmp, path)
    return True


def parse_bi5(path: Path, day: date, scale: float, keep_empty: bool) -> list[Candle]:
    raw = lzma.decompress(path.read_bytes())
    if len(raw) % RECORD_SIZE != 0:
        raise ValueError(f"Unexpected Dukascopy candle byte length in {path}: {len(raw)}")

    base = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    candles: list[Candle] = []
    for offset in range(0, len(raw), RECORD_SIZE):
        seconds, open_i, close_i, low_i, high_i, volume = struct.unpack(
            ">5if",
            raw[offset : offset + RECORD_SIZE],
        )
        open_price = open_i / scale
        close_price = close_i / scale
        low_price = low_i / scale
        high_price = high_i / scale
        is_empty = (
            volume <= 0.0
            and open_i == close_i
            and open_i == low_i
            and open_i == high_i
        )
        if is_empty and not keep_empty:
            continue
        candles.append(
            Candle(
                time_utc=base + timedelta(seconds=int(seconds)),
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=float(volume),
            )
        )
    return candles


def atomic_write_csv(path: Path, candles: Sequence[Candle]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["time_utc", "open", "high", "low", "close", "volume"],
        )
        writer.writeheader()
        for candle in candles:
            writer.writerow(
                {
                    "time_utc": candle.time_utc.isoformat(),
                    "open": f"{candle.open:.3f}",
                    "high": f"{candle.high:.3f}",
                    "low": f"{candle.low:.3f}",
                    "close": f"{candle.close:.3f}",
                    "volume": f"{candle.volume:.8f}",
                }
            )
    os.replace(tmp, path)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Dukascopy XAUUSD M1 candles to CSV.")
    parser.add_argument("--symbol", default="XAUUSD", help="Dukascopy symbol, default XAUUSD.")
    parser.add_argument("--side", default="BID", choices=["BID", "ASK"], help="Candle side.")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD.")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD.")
    parser.add_argument("--output", type=Path, default=None, help="Output CSV path.")
    parser.add_argument("--data-root", type=Path, default=DATA_ROOT, help="Raw/cache data root.")
    parser.add_argument("--scale", type=float, default=1000.0, help="OHLC integer scale.")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--sleep", type=float, default=0.05, help="Pause between daily downloads.")
    parser.add_argument("--force", action="store_true", help="Re-download existing daily raw files.")
    parser.add_argument("--keep-empty", action="store_true", help="Keep zero-volume flat minutes.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    start = parse_date(args.start)
    end = parse_date(args.end)
    if end < start:
        raise ValueError("--end must be on or after --start")

    output = args.output or (
        args.data_root
        / "candles"
        / f"{args.symbol.upper()}_{args.side.upper()}_M1_{start.isoformat()}_{end.isoformat()}.csv"
    )

    all_candles: list[Candle] = []
    downloaded_days = 0
    missing_days: list[str] = []

    for day in daterange(start, end):
        url = dukascopy_url(args.symbol, day, args.side)
        path = raw_path(args.data_root, args.symbol, day, args.side)
        ok = download_file(url, path, timeout=args.timeout, force=args.force, retries=args.retries)
        if not ok:
            missing_days.append(day.isoformat())
            continue
        downloaded_days += 1
        try:
            all_candles.extend(parse_bi5(path, day, scale=args.scale, keep_empty=args.keep_empty))
        except (lzma.LZMAError, ValueError) as exc:
            print(f"skip_parse={path} error={exc}")
            missing_days.append(day.isoformat())
        if args.sleep > 0:
            time.sleep(args.sleep)

    all_candles.sort(key=lambda candle: candle.time_utc)
    atomic_write_csv(output, all_candles)
    print(
        "\n".join(
            [
                f"output={output}",
                f"candles={len(all_candles)}",
                f"downloaded_days={downloaded_days}",
                f"missing_days={','.join(missing_days)}",
            ]
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
