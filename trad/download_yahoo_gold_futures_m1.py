#!/usr/bin/env python3
"""Download recent Yahoo Finance GC=F 1-minute candles to CSV.

Yahoo's chart endpoint is useful as a free futures-side research proxy, but it
only exposes a limited recent intraday window and is not a substitute for a
broker or paid CME data feed.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import quote
from urllib.request import Request, urlopen


SCRIPT_DIR = Path(__file__).resolve().parent
DATA_ROOT = SCRIPT_DIR / "data" / "gold_m1_liquidity_scalper"
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def fetch_chart(symbol: str, interval: str, range_value: str, timeout: int) -> dict[str, Any]:
    url = (
        YAHOO_CHART_URL.format(symbol=quote(symbol, safe=""))
        + f"?interval={quote(interval)}&range={quote(range_value)}&includePrePost=true"
    )
    request = Request(url, headers={"User-Agent": "gold-m1-research/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def write_csv(path: Path, payload: dict[str, Any]) -> int:
    result = (payload.get("chart", {}).get("result") or [None])[0]
    if not result:
        raise ValueError(f"Yahoo chart returned no result: {payload}")
    timestamps = result.get("timestamp") or []
    quote_data = (result.get("indicators", {}).get("quote") or [None])[0]
    if not quote_data:
        raise ValueError("Yahoo chart returned no quote data")

    columns = {
        name: quote_data.get(name) or [None] * len(timestamps)
        for name in ["open", "high", "low", "close", "volume"]
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    rows = 0
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["time_utc", "open", "high", "low", "close", "volume"],
        )
        writer.writeheader()
        for index, ts in enumerate(timestamps):
            open_price = columns["open"][index]
            high_price = columns["high"][index]
            low_price = columns["low"][index]
            close_price = columns["close"][index]
            volume = columns["volume"][index]
            if any(value is None for value in [open_price, high_price, low_price, close_price]):
                continue
            writer.writerow(
                {
                    "time_utc": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                    "open": f"{float(open_price):.3f}",
                    "high": f"{float(high_price):.3f}",
                    "low": f"{float(low_price):.3f}",
                    "close": f"{float(close_price):.3f}",
                    "volume": int(volume or 0),
                }
            )
            rows += 1
    os.replace(tmp, path)
    return rows


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download recent Yahoo GC=F M1 candles to CSV.")
    parser.add_argument("--symbol", default="GC=F")
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--range", default="5d")
    parser.add_argument("--output", type=Path, default=DATA_ROOT / "candles" / "GC_F_YAHOO_M1_5D.csv")
    parser.add_argument("--raw-output", type=Path, default=DATA_ROOT / "raw" / "yahoo_GC_F_1m_5d.json")
    parser.add_argument("--timeout", type=int, default=30)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    payload = fetch_chart(args.symbol, args.interval, args.range, args.timeout)
    atomic_write_text(args.raw_output, json.dumps(payload, separators=(",", ":")))
    rows = write_csv(args.output, payload)
    print(f"output={args.output}")
    print(f"raw_output={args.raw_output}")
    print(f"candles={rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
