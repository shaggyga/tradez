#!/usr/bin/env python3
"""Dedicated, order-incapable OANDA practice quote publisher.

This process isolates research quote freshness from strategy construction and
candidate ranking.  It has no executor, order client, or account-write path.
"""

from __future__ import annotations

import math
import time
from pathlib import Path

try:
    import oanda_practice_shadow_strategy_lab as lab
except ModuleNotFoundError:  # Package import used by tests.
    from trad import oanda_practice_shadow_strategy_lab as lab

try:
    from oanda_quote_intensity_shadow import SignedQuoteIntensityTracker
except ModuleNotFoundError:
    from trad.oanda_quote_intensity_shadow import SignedQuoteIntensityTracker


CAN_PLACE_ORDERS = False
ACCOUNT_SCOPE = "practice_007_read_only_quotes"
PRODUCER = "practice_007_dedicated_quote_stream"


def main(argv: list[str] | None = None) -> int:
    args = lab.parse_args(argv)
    if "practice" not in str(lab.BASE_URL).lower():
        raise SystemExit("The quote stream is restricted to the OANDA practice endpoint.")
    if args.research_market_quote_snapshot is None:
        raise SystemExit("A research quote snapshot path is required.")

    heartbeat = lab.WorkerHeartbeat(
        args.heartbeat_state,
        worker="oanda_practice_quote_stream",
        role="practice_007_quote_stream",
    ).start()
    heartbeat.update(phase="reading_practice_credentials")
    token, account_id = lab.read_credentials(
        args.creds,
        args.account_key,
        args.account_id,
    )
    heartbeat.update(phase="loading_practice_instruments")
    client = lab.MarketDataClient(token)
    stream = None
    intensity = None
    try:
        instruments = lab.selected_instruments(args, token, account_id)
        if not instruments:
            raise SystemExit("No tradeable FX instruments selected.")
        pip_sizes = lab.account_pip_sizes(client, account_id)
        snapshot_path = Path(args.research_market_quote_snapshot)
        intensity = SignedQuoteIntensityTracker(
            snapshot_path.with_name("practice_007_quote_intensity_shadow_v1.sqlite"),
            snapshot_path.with_name("practice_007_quote_intensity_shadow_v1.json"),
        )
        stream = lab.MultiPriceStream(
            lambda: lab.read_credentials(
                args.creds,
                args.account_key,
                args.account_id,
            ),
            instruments,
            lambda event, **fields: None,
            research_snapshot_path=Path(
                args.research_market_quote_snapshot
            ),
            research_snapshot_interval_sec=(
                args.research_market_quote_snapshot_sec
            ),
            research_snapshot_producer=PRODUCER,
            research_snapshot_min_instruments=max(
                1,
                math.ceil(len(instruments) * 0.95),
            ),
            pip_sizes=pip_sizes,
            quote_observer=intensity.observe,
        )
        heartbeat.update(
            phase="starting_price_stream",
            account_suffix=account_id[-4:],
            instrument_count=len(instruments),
            can_place_orders=CAN_PLACE_ORDERS,
            real_money_routing=False,
        )
        stream.start()
        stream.wait_ready(args.stream_start_timeout_sec)
        stop_at = time.monotonic() + args.duration_sec
        while time.monotonic() < stop_at:
            intensity.flush(include_current=True)
            heartbeat.update(
                phase="streaming",
                account_suffix=account_id[-4:],
                instrument_count=len(instruments),
                can_place_orders=CAN_PLACE_ORDERS,
                real_money_routing=False,
                output=str(Path(args.research_market_quote_snapshot).resolve()),
                stream=stream.stats(),
                quote_intensity=intensity.snapshot(),
            )
            time.sleep(min(5.0, max(0.0, stop_at - time.monotonic())))
    finally:
        if stream is not None:
            stream.stop()
        if intensity is not None:
            intensity.close()
        heartbeat.close()
        client.close()
        lab.close_log_handles()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
