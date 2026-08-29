#!/usr/bin/env python3
"""Public Coinbase live-data recorder and cost-aware shadow signal tracker."""

from __future__ import annotations

import argparse
import json
import math
import signal
import sqlite3
import statistics
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import websocket


WS_URL = "wss://advanced-trade-ws.coinbase.com"
DEFAULT_PRODUCTS = ("BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "ADA-USD", "AVAX-USD", "LINK-USD")
DEFAULT_HORIZONS = (5, 30, 60, 300)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if math.isfinite(result) else 0.0


def parse_time_ns(value: Any) -> int:
    text = str(value or "").strip().replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(text).timestamp() * 1_000_000_000)
    except ValueError:
        return time.time_ns()


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


class CryptoShadowTracker:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.products = tuple(dict.fromkeys(args.products))
        self.horizons = tuple(sorted(set(args.horizons)))
        self.data_dir = args.data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.database_path = self.data_dir / "coinbase_shadow_live.sqlite3"
        self.state_path = self.data_dir / "coinbase_shadow_state.json"
        self.connection = sqlite3.connect(self.database_path)
        self.connection.row_factory = sqlite3.Row
        self._create_schema()
        self.prices: dict[str, deque[tuple[int, float]]] = defaultdict(lambda: deque(maxlen=50_000))
        self.trade_flow: dict[str, deque[tuple[int, float]]] = defaultdict(lambda: deque(maxlen=50_000))
        self.latest: dict[str, dict[str, float]] = {}
        self.summary_stats: dict[tuple[str, int], dict[str, float]] = {}
        self.last_signal_ns: dict[str, int] = defaultdict(int)
        self.started_ns = time.time_ns()
        self.last_commit = time.monotonic()
        self.last_state = 0.0
        self.message_count = 0
        self.ticker_count = 0
        self.trade_count = 0
        self.signal_count = 0
        self.matured_count = 0
        self.sequence_gaps = 0
        self.last_sequence: dict[str, int] = {}
        self.stop = False
        self._restore_state()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            CREATE TABLE IF NOT EXISTS ticks (
                id INTEGER PRIMARY KEY, time_ns INTEGER NOT NULL, product_id TEXT NOT NULL,
                price REAL NOT NULL, best_bid REAL NOT NULL, best_ask REAL NOT NULL,
                best_bid_quantity REAL NOT NULL, best_ask_quantity REAL NOT NULL,
                volume_24h REAL NOT NULL, sequence_num INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ticks_product_time ON ticks(product_id, time_ns);
            CREATE TABLE IF NOT EXISTS market_trades (
                trade_id TEXT NOT NULL, time_ns INTEGER NOT NULL, product_id TEXT NOT NULL,
                price REAL NOT NULL, size REAL NOT NULL, maker_side TEXT NOT NULL,
                PRIMARY KEY(product_id, trade_id)
            );
            CREATE INDEX IF NOT EXISTS idx_trades_product_time ON market_trades(product_id, time_ns);
            CREATE TABLE IF NOT EXISTS shadow_predictions (
                id INTEGER PRIMARY KEY, origin_time_ns INTEGER NOT NULL, target_time_ns INTEGER NOT NULL,
                outcome_time_ns INTEGER, product_id TEXT NOT NULL, model_id TEXT NOT NULL,
                horizon_sec INTEGER NOT NULL, side TEXT NOT NULL, entry_price REAL NOT NULL,
                signal_value REAL NOT NULL, spread_bps REAL NOT NULL, assumed_fee_bps REAL NOT NULL,
                status TEXT NOT NULL, outcome_price REAL, gross_bps REAL, net_bps REAL,
                direction_correct INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_crypto_predictions_pending
                ON shadow_predictions(product_id, status, target_time_ns);
            """
        )
        self.connection.commit()

    def _restore_state(self) -> None:
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if not isinstance(payload, dict):
            return
        for name in (
            "message_count",
            "ticker_count",
            "trade_count",
            "signal_count",
            "matured_count",
            "sequence_gaps",
        ):
            setattr(self, name, int(safe_float(payload.get(name))))
        latest = payload.get("latest")
        if isinstance(latest, dict):
            self.latest = {
                str(product): dict(row)
                for product, row in latest.items()
                if str(product) in self.products and isinstance(row, dict)
            }
        for row in payload.get("model_summaries") or []:
            if not isinstance(row, dict):
                continue
            model_id = str(row.get("model_id") or "")
            horizon = int(safe_float(row.get("horizon_sec")))
            count = int(safe_float(row.get("matured")))
            if not model_id or horizon <= 0 or count <= 0:
                continue
            self.summary_stats[(model_id, horizon)] = {
                "count": float(count),
                "correct": count * safe_float(row.get("direction_accuracy")) / 100.0,
                "gross": count * safe_float(row.get("average_gross_bps")),
                "net": count * safe_float(row.get("average_net_bps")),
                "wins": count * safe_float(row.get("net_win_rate")) / 100.0,
            }

    def _observe_summary(
        self,
        model_id: str,
        horizon_sec: int,
        gross_bps: float,
        net_bps: float,
        direction_correct: bool,
    ) -> None:
        stats = self.summary_stats.setdefault(
            (model_id, horizon_sec),
            {"count": 0.0, "correct": 0.0, "gross": 0.0, "net": 0.0, "wins": 0.0},
        )
        stats["count"] += 1.0
        stats["correct"] += float(direction_correct)
        stats["gross"] += gross_bps
        stats["net"] += net_bps
        stats["wins"] += float(net_bps > 0.0)

    def subscribe(self, ws: websocket.WebSocketApp) -> None:
        ws.send(json.dumps({"type": "subscribe", "channel": "heartbeats"}))
        for channel in ("ticker", "market_trades"):
            ws.send(json.dumps({"type": "subscribe", "channel": channel, "product_ids": list(self.products)}))
        print(json.dumps({"event": "crypto_stream_connected", "time": utc_now(), "products": self.products}), flush=True)

    def on_message(self, _ws: websocket.WebSocketApp, raw: str) -> None:
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            return
        self.message_count += 1
        channel = str(message.get("channel") or "")
        sequence = int(safe_float(message.get("sequence_num")))
        previous = self.last_sequence.get(channel)
        if previous is not None and sequence > previous + 1:
            self.sequence_gaps += sequence - previous - 1
        self.last_sequence[channel] = sequence
        received_ns = parse_time_ns(message.get("timestamp"))
        if channel in {"ticker", "ticker_batch"}:
            for event in message.get("events") or []:
                for ticker in event.get("tickers") or []:
                    self.record_ticker(ticker, received_ns, sequence)
        elif channel == "market_trades":
            for event in message.get("events") or []:
                for trade in event.get("trades") or []:
                    self.record_trade(trade)
        if time.monotonic() - self.last_commit >= 1.0:
            self.connection.commit()
            self.last_commit = time.monotonic()
        if time.monotonic() - self.last_state >= self.args.state_interval_sec:
            self.write_state()

    def record_ticker(self, ticker: dict[str, Any], time_ns: int, sequence: int) -> None:
        product = str(ticker.get("product_id") or "")
        price = safe_float(ticker.get("price"))
        if product not in self.products or price <= 0:
            return
        bid = safe_float(ticker.get("best_bid")) or price
        ask = safe_float(ticker.get("best_ask")) or price
        bid_quantity = safe_float(ticker.get("best_bid_quantity"))
        ask_quantity = safe_float(ticker.get("best_ask_quantity"))
        volume = safe_float(ticker.get("volume_24_h"))
        self.connection.execute(
            "INSERT INTO ticks(time_ns, product_id, price, best_bid, best_ask, best_bid_quantity, best_ask_quantity, volume_24h, sequence_num) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (time_ns, product, price, bid, ask, bid_quantity, ask_quantity, volume, sequence),
        )
        self.ticker_count += 1
        self.prices[product].append((time_ns, price))
        self.latest[product] = {
            "price": price,
            "bid": bid,
            "ask": ask,
            "bid_quantity": bid_quantity,
            "ask_quantity": ask_quantity,
            "volume_24h": volume,
            "time_ns": time_ns,
        }
        self._prune(product, time_ns)
        self.mature(product, time_ns, price)
        if time_ns - self.last_signal_ns[product] >= int(self.args.signal_interval_sec * 1_000_000_000):
            self.emit_signals(product, time_ns)
            self.last_signal_ns[product] = time_ns

    def record_trade(self, trade: dict[str, Any]) -> None:
        product = str(trade.get("product_id") or "")
        if product not in self.products:
            return
        trade_id = str(trade.get("trade_id") or "")
        time_ns = parse_time_ns(trade.get("time"))
        price = safe_float(trade.get("price"))
        size = safe_float(trade.get("size"))
        maker_side = str(trade.get("side") or "").upper()
        if not trade_id or price <= 0 or size <= 0:
            return
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO market_trades VALUES (?, ?, ?, ?, ?, ?)",
            (trade_id, time_ns, product, price, size, maker_side),
        )
        if cursor.rowcount:
            self.trade_count += 1
            aggressive_sign = -1.0 if maker_side == "BUY" else 1.0
            self.trade_flow[product].append((time_ns, aggressive_sign * price * size))

    def _prune(self, product: str, now_ns: int) -> None:
        cutoff = now_ns - 10 * 60 * 1_000_000_000
        while self.prices[product] and self.prices[product][0][0] < cutoff:
            self.prices[product].popleft()
        while self.trade_flow[product] and self.trade_flow[product][0][0] < cutoff:
            self.trade_flow[product].popleft()

    def price_at(self, product: str, target_ns: int) -> float | None:
        for timestamp, price in reversed(self.prices[product]):
            if timestamp <= target_ns:
                return price
        return None

    def model_signals(self, product: str, now_ns: int) -> dict[str, float]:
        latest = self.latest[product]
        price = latest["price"]
        price_10 = self.price_at(product, now_ns - 10_000_000_000)
        price_30 = self.price_at(product, now_ns - 30_000_000_000)
        if not price_10 or not price_30:
            return {}
        momentum = math.log(price / price_10) * 10_000
        recent_prices = [value for timestamp, value in self.prices[product] if timestamp >= now_ns - 30_000_000_000]
        mean = statistics.fmean(recent_prices) if recent_prices else price
        stdev = statistics.pstdev(recent_prices) if len(recent_prices) > 2 else 0.0
        z_score = (price - mean) / stdev if stdev > 0 else 0.0
        flow_values = [value for timestamp, value in self.trade_flow[product] if timestamp >= now_ns - 10_000_000_000]
        flow_total = sum(abs(value) for value in flow_values)
        flow_imbalance = sum(flow_values) / flow_total if flow_total else 0.0
        book_total = latest["bid_quantity"] + latest["ask_quantity"]
        book_imbalance = (latest["bid_quantity"] - latest["ask_quantity"]) / book_total if book_total else 0.0
        signals = {
            "momentum_10s": momentum,
            "mean_reversion_30s": -z_score,
            "trade_flow_10s": flow_imbalance,
            "top_book_imbalance": book_imbalance,
        }
        votes = [math.copysign(1.0, value) for value in signals.values() if abs(value) > 1e-12]
        signals["equal_vote_ensemble"] = sum(votes) / len(votes) if votes else 0.0
        return signals

    def emit_signals(self, product: str, now_ns: int) -> None:
        latest = self.latest[product]
        price = latest["price"]
        spread_bps = max(0.0, (latest["ask"] - latest["bid"]) / price * 10_000)
        for model_id, value in self.model_signals(product, now_ns).items():
            if abs(value) < self.args.min_signal:
                continue
            side = "buy" if value > 0 else "sell"
            for horizon in self.horizons:
                self.connection.execute(
                    """
                    INSERT INTO shadow_predictions(
                        origin_time_ns, target_time_ns, product_id, model_id, horizon_sec,
                        side, entry_price, signal_value, spread_bps, assumed_fee_bps, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                    """,
                    (now_ns, now_ns + horizon * 1_000_000_000, product, model_id, horizon, side, price, value, spread_bps, self.args.round_trip_fee_bps),
                )
                self.signal_count += 1

    def mature(self, product: str, now_ns: int, price: float) -> None:
        pending = self.connection.execute(
            """
            SELECT id, model_id, horizon_sec, side, entry_price, spread_bps, assumed_fee_bps
            FROM shadow_predictions
            WHERE product_id = ? AND status = 'pending' AND target_time_ns <= ?
            """,
            (product, now_ns),
        ).fetchall()
        for row in pending:
            direction = 1.0 if row["side"] == "buy" else -1.0
            gross_bps = direction * (price / safe_float(row["entry_price"]) - 1.0) * 10_000
            net_bps = gross_bps - safe_float(row["spread_bps"]) - safe_float(row["assumed_fee_bps"])
            self.connection.execute(
                "UPDATE shadow_predictions SET status = 'matured', outcome_time_ns = ?, outcome_price = ?, gross_bps = ?, net_bps = ?, direction_correct = ? WHERE id = ?",
                (now_ns, price, gross_bps, net_bps, int(gross_bps > 0), row["id"]),
            )
            self.matured_count += 1
            self._observe_summary(
                str(row["model_id"]),
                int(row["horizon_sec"]),
                gross_bps,
                net_bps,
                gross_bps > 0.0,
            )

    def write_state(self) -> None:
        summaries = []
        for (model_id, horizon_sec), stats in sorted(self.summary_stats.items()):
            count = max(1.0, stats["count"])
            summaries.append(
                {
                    "model_id": model_id,
                    "horizon_sec": horizon_sec,
                    "matured": int(stats["count"]),
                    "direction_accuracy": round(100.0 * stats["correct"] / count, 3),
                    "average_gross_bps": round(stats["gross"] / count, 4),
                    "average_net_bps": round(stats["net"] / count, 4),
                    "net_win_rate": round(100.0 * stats["wins"] / count, 3),
                }
            )
        atomic_json(
            self.state_path,
            {
                "generated_utc": utc_now(),
                "active": True,
                "shadow_only": True,
                "places_orders": False,
                "source": WS_URL,
                "products": self.products,
                "horizons_sec": self.horizons,
                "database": str(self.database_path),
                "message_count": self.message_count,
                "ticker_count": self.ticker_count,
                "trade_count": self.trade_count,
                "signal_count": self.signal_count,
                "matured_count": self.matured_count,
                "sequence_gaps": self.sequence_gaps,
                "assumed_round_trip_fee_bps": self.args.round_trip_fee_bps,
                "latest": self.latest,
                "model_summaries": summaries,
            },
        )
        self.last_state = time.monotonic()

    def run(self) -> int:
        while not self.stop:
            app = websocket.WebSocketApp(
                WS_URL,
                on_open=self.subscribe,
                on_message=self.on_message,
                on_error=lambda _ws, error: print(json.dumps({"event": "crypto_stream_error", "time": utc_now(), "error": str(error)}), flush=True),
                on_close=lambda _ws, code, message: print(json.dumps({"event": "crypto_stream_closed", "time": utc_now(), "code": code, "message": message}), flush=True),
            )
            app.run_forever(ping_interval=20, ping_timeout=10)
            if not self.stop:
                time.sleep(2)
        self.connection.commit()
        self.write_state()
        self.connection.close()
        return 0


def parse_args() -> argparse.Namespace:
    default_data = Path(r"D:\crypto_shadow\data") if Path("D:/").exists() else Path(__file__).resolve().parent / "data" / "crypto_shadow"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--products", nargs="+", default=list(DEFAULT_PRODUCTS))
    parser.add_argument("--horizons", nargs="+", type=int, default=list(DEFAULT_HORIZONS))
    parser.add_argument("--data-dir", type=Path, default=default_data)
    parser.add_argument("--signal-interval-sec", type=float, default=5.0)
    parser.add_argument("--state-interval-sec", type=float, default=5.0)
    parser.add_argument("--round-trip-fee-bps", type=float, default=60.0)
    parser.add_argument("--min-signal", type=float, default=0.01)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tracker = CryptoShadowTracker(args)

    def stop_handler(_signum: int, _frame: Any) -> None:
        tracker.stop = True

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    return tracker.run()


if __name__ == "__main__":
    raise SystemExit(main())
