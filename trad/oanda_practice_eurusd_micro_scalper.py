#!/usr/bin/env python3
"""Fault-tolerant OANDA practice-account micro scalper.

This runner is intentionally practice-only. It keeps broker-side protection on
every trade, bounds price slippage and account risk, reconciles uncertain broker
responses, and records enough execution telemetry for later strategy analysis.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import secrets
import statistics
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import requests


ROOT = Path(__file__).resolve().parent
DEFAULT_CREDS = ROOT / "creds"
DEFAULT_LOG_DIR = ROOT / "data" / "oanda_training_manager" / "logs"
BASE_URL = "https://api-fxpractice.oanda.com"
STREAM_URL = "https://stream-fxpractice.oanda.com"
INSTRUMENT = "EUR_USD"
PIP = 0.0001
INSTRUMENT_META_CACHE: dict[tuple[str, str], tuple["InstrumentMeta", float]] = {}
QUOTE_TO_ACCOUNT_RATE_CACHE: dict[tuple[str, str, str], float] = {}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""


def cfg_value(text: str, *names: str) -> str:
    for name in names:
        env_value = os.environ.get(name)
        if env_value:
            return env_value.strip()
        pattern = r'''(?m)^\s*{}\s*=\s*["']?([^"'\r\n#]+)["']?'''.format(re.escape(name))
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()
    return ""


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def normalize_instrument(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "")).upper().strip("_")
    if not re.fullmatch(r"[A-Z0-9]{2,10}_[A-Z0-9]{2,10}", normalized):
        raise argparse.ArgumentTypeError("instrument must look like EUR_USD, GBP_USD, USD_JPY, etc.")
    return normalized


def quote_currency(instrument: str) -> str:
    parts = normalize_instrument(instrument).split("_", 1)
    return parts[1]


def infer_pip_size(instrument: str) -> float:
    return 0.01 if quote_currency(instrument) == "JPY" else 0.0001


def parse_rfc3339(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = str(value).strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    cleaned = re.sub(r"(\.\d{6})\d+(?=[+-]\d\d:\d\d$)", r"\1", cleaned)
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def ema_series(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (max(1, period) + 1.0)
    output = [values[0]]
    for value in values[1:]:
        output.append(alpha * value + (1.0 - alpha) * output[-1])
    return output


def rsi_series(values: list[float], period: int = 14) -> list[float | None]:
    output: list[float | None] = [None] * len(values)
    if len(values) <= period:
        return output
    gains: list[float] = []
    losses: list[float] = []
    for previous, current in zip(values, values[1:]):
        change = current - previous
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    output[period] = 100.0 if avg_loss == 0.0 else 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    for index in range(period + 1, len(values)):
        avg_gain = ((period - 1) * avg_gain + gains[index - 1]) / period
        avg_loss = ((period - 1) * avg_loss + losses[index - 1]) / period
        output[index] = 100.0 if avg_loss == 0.0 else 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    return output


def macd_histogram(values: list[float]) -> list[float]:
    fast = ema_series(values, 12)
    slow = ema_series(values, 26)
    macd = [left - right for left, right in zip(fast, slow)]
    signal = ema_series(macd, 9)
    return [left - right for left, right in zip(macd, signal)]


def candle_closes(candles: Iterable[dict[str, Any]], component: str = "mid") -> list[float]:
    return [safe_float(candle.get(component, {}).get("c")) for candle in candles]


def atr_pips(candles: list[dict[str, Any]], period: int = 14) -> float:
    if len(candles) < period + 1:
        return 0.0
    true_ranges: list[float] = []
    previous_close: float | None = None
    for candle in candles:
        mid = candle.get("mid") or {}
        high = safe_float(mid.get("h"))
        low = safe_float(mid.get("l"))
        close = safe_float(mid.get("c"))
        if previous_close is None:
            true_range = high - low
        else:
            true_range = max(high - low, abs(high - previous_close), abs(low - previous_close))
        true_ranges.append(max(0.0, true_range))
        previous_close = close
    return statistics.fmean(true_ranges[-period:]) / PIP


def candle_spread_pips(candle: dict[str, Any]) -> float | None:
    bid = candle.get("bid") or {}
    ask = candle.get("ask") or {}
    if not bid or not ask:
        return None
    spread = (safe_float(ask.get("c")) - safe_float(bid.get("c"))) / PIP
    return spread if spread >= 0.0 else None


@dataclass(frozen=True)
class Quote:
    bid: float
    ask: float
    time: str
    tradeable: bool = True
    received_monotonic: float = field(default_factory=time.monotonic)
    received_wall_time: float = field(default_factory=time.time)
    source: str = "rest"

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread_pips(self) -> float:
        return (self.ask - self.bid) / PIP

    @property
    def local_age_sec(self) -> float:
        return max(0.0, time.monotonic() - self.received_monotonic)

    @property
    def server_age_sec(self) -> float:
        parsed = parse_rfc3339(self.time)
        if parsed is None:
            return math.inf
        return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())

    @property
    def server_clock_offset_sec(self) -> float:
        parsed = parse_rfc3339(self.time)
        if parsed is None:
            return math.nan
        return parsed.timestamp() - self.received_wall_time


@dataclass(frozen=True)
class InstrumentMeta:
    display_precision: int = 5
    margin_rate: float = 0.02
    minimum_trade_size: int = 1


@dataclass(frozen=True)
class SizingDecision:
    units: int
    risk_budget: float
    actual_risk: float
    actual_risk_pct: float
    estimated_margin: float
    risk_limited_units: int
    margin_limited_units: int
    reason: str = ""


@dataclass
class TradeContext:
    trade_id: str
    client_id: str
    direction: str
    units: int
    entry_price: float
    entry_time: str
    entry_transaction_id: str
    signal_quote: Quote
    entry_quote: Quote
    stop_pips: float
    target_pips: float
    risk_amount: float
    fill_slippage_pips: float
    signal_meta: dict[str, Any]
    opened_monotonic: float = field(default_factory=time.monotonic)
    mfe_pips: float = 0.0
    mae_pips: float = 0.0
    spreads: list[float] = field(default_factory=list)


@dataclass(frozen=True)
class TradeOutcome:
    realized_pl: float
    realized_r: float
    reason: str
    close_price: float | None
    close_time: str
    transaction_id: str


class FatalSessionError(RuntimeError):
    pass


class OandaApiError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        method: str = "",
        endpoint: str = "",
        request_id: str = "",
        payload: Any = None,
        outcome_uncertain: bool = False,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.method = method
        self.endpoint = endpoint
        self.request_id = request_id
        self.payload = payload
        self.outcome_uncertain = outcome_uncertain


def quote_from_price_payload(payload: dict[str, Any], source: str) -> Quote:
    bids = payload.get("bids") or []
    asks = payload.get("asks") or []
    if not bids or not asks:
        raise ValueError("OANDA price payload did not contain bid and ask liquidity")
    status = str(payload.get("status") or "").lower()
    tradeable = bool(payload.get("tradeable", status in {"", "tradeable"}))
    return Quote(
        bid=safe_float(bids[0].get("price")),
        ask=safe_float(asks[0].get("price")),
        time=str(payload.get("time") or ""),
        tradeable=tradeable,
        source=source,
    )


def calculate_sizing(
    *,
    balance: float,
    margin_available: float,
    quote_mid: float,
    stop_pips: float,
    risk_per_trade_pct: float,
    fixed_units: int,
    max_units: int,
    minimum_units: int,
    margin_rate: float,
    max_margin_fraction: float,
    quote_to_account_rate: float = 1.0,
) -> SizingDecision:
    risk_budget = max(0.0, balance * risk_per_trade_pct / 100.0)
    conversion = max(PIP, quote_to_account_rate)
    risk_per_unit = max(PIP, stop_pips * PIP * conversion)
    risk_limited = max(0, math.floor(risk_budget / risk_per_unit))
    margin_budget = max(0.0, margin_available * max_margin_fraction)
    margin_per_unit = max(PIP, quote_mid * max(PIP, margin_rate) * conversion)
    margin_limited = max(0, math.floor(margin_budget / margin_per_unit))
    requested = fixed_units if fixed_units > 0 else risk_limited
    limits = [requested, risk_limited, margin_limited]
    if max_units > 0:
        limits.append(max_units)
    units = max(0, min(limits))
    if units < minimum_units:
        return SizingDecision(
            units=0,
            risk_budget=risk_budget,
            actual_risk=0.0,
            actual_risk_pct=0.0,
            estimated_margin=0.0,
            risk_limited_units=risk_limited,
            margin_limited_units=margin_limited,
            reason="below_minimum_trade_size",
        )
    actual_risk = units * risk_per_unit
    estimated_margin = units * margin_per_unit
    reasons: list[str] = []
    if fixed_units > 0 and units < fixed_units:
        reasons.append("fixed_units_capped")
    if units == risk_limited:
        reasons.append("risk_cap")
    if units == margin_limited:
        reasons.append("margin_cap")
    if max_units > 0 and units == max_units:
        reasons.append("max_units_cap")
    return SizingDecision(
        units=units,
        risk_budget=risk_budget,
        actual_risk=actual_risk,
        actual_risk_pct=0.0 if balance <= 0.0 else actual_risk / balance * 100.0,
        estimated_margin=estimated_margin,
        risk_limited_units=risk_limited,
        margin_limited_units=margin_limited,
        reason=",".join(dict.fromkeys(reasons)),
    )


def extract_trade_close(
    transactions: Iterable[dict[str, Any]], trade_id: str
) -> dict[str, Any] | None:
    match: dict[str, Any] | None = None
    for transaction in transactions:
        if str(transaction.get("type") or "").upper() != "ORDER_FILL":
            continue
        closed_items = list(transaction.get("tradesClosed") or [])
        reduced = transaction.get("tradeReduced")
        if isinstance(reduced, dict):
            closed_items.append(reduced)
        for item in closed_items:
            if str(item.get("tradeID") or "") != str(trade_id):
                continue
            match = {
                "transaction_id": str(transaction.get("id") or ""),
                "reason": str(transaction.get("reason") or "ORDER_FILL"),
                "price": safe_float(transaction.get("price"), math.nan),
                "time": str(transaction.get("time") or ""),
                "realized_pl": safe_float(item.get("realizedPL"), safe_float(transaction.get("pl"))),
                "account_balance": safe_float(transaction.get("accountBalance"), math.nan),
            }
    return match


class PriceStream:
    def __init__(
        self,
        credential_provider: Callable[[], tuple[str, str]],
        log: Callable[..., None],
        reconnect_base_sec: float,
    ) -> None:
        self.credential_provider = credential_provider
        self.log = log
        self.reconnect_base_sec = max(0.5, reconnect_base_sec)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._latest: Quote | None = None
        self._thread: threading.Thread | None = None
        self._response: requests.Response | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="oanda-price-stream", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        response = self._response
        if response is not None:
            try:
                response.close()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=7.0)

    def wait_ready(self, timeout: float) -> bool:
        return self._ready.wait(timeout=max(0.0, timeout))

    def latest(self) -> Quote | None:
        with self._lock:
            return self._latest

    def _run(self) -> None:
        failures = 0
        while not self._stop.is_set():
            response: requests.Response | None = None
            try:
                token, account_id = self.credential_provider()
                response = requests.get(
                    f"{STREAM_URL}/v3/accounts/{account_id}/pricing/stream",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Accept-Datetime-Format": "RFC3339",
                    },
                    params={"instruments": INSTRUMENT, "snapshot": "true"},
                    stream=True,
                    timeout=(10, 15),
                )
                self._response = response
                if response.status_code >= 400:
                    request_id = str(response.headers.get("RequestID") or "")
                    self.log(
                        "price_stream_http_error",
                        status=response.status_code,
                        request_id=request_id,
                    )
                    failures += 1
                else:
                    failures = 0
                    self.log("price_stream_connected")
                    for raw_line in response.iter_lines():
                        if self._stop.is_set():
                            break
                        if not raw_line:
                            continue
                        try:
                            payload = json.loads(raw_line.decode("utf-8"))
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            continue
                        if str(payload.get("type") or "").upper() != "PRICE":
                            continue
                        quote = quote_from_price_payload(payload, "stream")
                        with self._lock:
                            self._latest = quote
                        self._ready.set()
            except requests.RequestException as exc:
                failures += 1
                if not self._stop.is_set():
                    self.log("price_stream_error", error_type=type(exc).__name__, error=str(exc)[:300])
            except Exception as exc:
                failures += 1
                if not self._stop.is_set():
                    self.log("price_stream_error", error_type=type(exc).__name__, error=str(exc)[:300])
            finally:
                if response is not None:
                    response.close()
                self._response = None
            if self._stop.is_set():
                break
            delay = min(15.0, self.reconnect_base_sec * (2 ** min(failures, 4)))
            self._stop.wait(delay)


class PracticeScalper:
    def __init__(self, args: argparse.Namespace) -> None:
        global INSTRUMENT, PIP
        self.args = args
        self.args.instrument = normalize_instrument(self.args.instrument)
        INSTRUMENT = self.args.instrument
        PIP = infer_pip_size(INSTRUMENT)
        self.log_lock = threading.Lock()
        self.notice_times: dict[str, float] = {}
        if args.log_file:
            args.log_file.parent.mkdir(parents=True, exist_ok=True)
            self.log_path = args.log_file
        else:
            args.log_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
            instrument_slug = INSTRUMENT.lower()
            strategy_slug = re.sub(r"[^a-z0-9]+", "_", str(args.strategy).lower()).strip("_")
            account_slug = (
                re.sub(r"[^a-z0-9]+", "_", str(args.account_key or args.account_id or "default").lower()).strip("_")
                or "default"
            )
            self.log_path = (
                args.log_dir / f"practice_{instrument_slug}_{strategy_slug}_{account_slug}_micro_scalper_v9_{stamp}.jsonl"
            )

        self.token, self.account_id = self._read_credentials()
        self.session = requests.Session()
        self._apply_session_headers()
        self.price_stream: PriceStream | None = None
        self.account_snapshot, self.last_transaction_id = self.account_details()
        self.account_currency = str(self.account_snapshot.get("currency") or "USD").upper()
        if self.account_currency != "USD":
            raise SystemExit("Dynamic risk sizing currently requires a USD-denominated practice account.")
        self.instrument_meta = self.load_instrument_meta()
        self.quote_to_account_rate = self.load_quote_to_account_rate()
        self.start_balance = safe_float(self.account_snapshot.get("balance"))
        self.trade_count = 0
        self.consecutive_losses = 0
        self.next_entry_time = 0.0
        self.last_signal_candle_time = ""
        self.supports_client_extensions = not bool(self.account_snapshot.get("mt4AccountID"))

    def _read_credentials(self) -> tuple[str, str]:
        creds = read_text(self.args.creds)
        token = cfg_value(creds, "OANDA_API_KEY", "OANDA_API_TOKEN")
        account_id = str(getattr(self.args, "account_id", "") or "").strip()
        account_key = str(getattr(self.args, "account_key", "") or "").strip()
        if not account_id and account_key:
            account_id = cfg_value(creds, account_key)
        if not account_id:
            account_id = cfg_value(creds, "OANDA_ACCOUNT_ID_GPT", "OANDA_ACCOUNT_ID_MAJ")
        if not token or not account_id:
            raise FatalSessionError("Missing OANDA practice token or account id.")
        return token, account_id

    def _stream_credentials(self) -> tuple[str, str]:
        token, account_id = self._read_credentials()
        if account_id != self.account_id:
            raise FatalSessionError("Configured OANDA account changed while the bot was running.")
        return token, account_id

    def _apply_session_headers(self) -> None:
        self.session.headers.clear()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "Accept-Datetime-Format": "RFC3339",
            }
        )

    def _reset_session(self, *, reload_credentials: bool) -> None:
        if reload_credentials:
            token, account_id = self._read_credentials()
            if account_id != self.account_id:
                raise FatalSessionError("Configured OANDA account changed during authentication recovery.")
            self.token = token
        try:
            self.session.close()
        except Exception:
            pass
        self.session = requests.Session()
        self._apply_session_headers()

    def log(self, event: str, **fields: Any) -> None:
        payload = {"time": utc_now(), "event": event, **fields}
        line = json.dumps(payload, sort_keys=True, default=str)
        with self.log_lock:
            with self.log_path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
            print(line, flush=True)

    def log_rate_limited(self, key: str, interval_sec: float, event: str, **fields: Any) -> None:
        now = time.monotonic()
        if now - self.notice_times.get(key, -math.inf) < interval_sec:
            return
        self.notice_times[key] = now
        self.log(event, **fields)

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        client_request_id: str = "",
    ) -> dict[str, Any]:
        method = method.upper().strip()
        read_only = method in {"GET", "HEAD", "OPTIONS"}
        max_attempts = 1 + max(0, self.args.api_retries) if read_only else 1
        retryable_statuses = {408, 409, 425, 429, 500, 502, 503, 504}
        safe_path = path.replace(self.account_id, f"...{self.account_id[-4:]}")
        for attempt in range(1, max_attempts + 1):
            headers = {"ClientRequestID": client_request_id} if client_request_id else None
            try:
                response = self.session.request(
                    method,
                    f"{BASE_URL}{path}",
                    params=params,
                    json=body,
                    headers=headers,
                    timeout=self.args.request_timeout_sec,
                )
            except requests.RequestException as exc:
                if read_only and attempt < max_attempts:
                    delay = min(15.0, self.args.api_retry_base_sec * (2 ** (attempt - 1)))
                    self.log(
                        "api_retry",
                        method=method,
                        endpoint=safe_path,
                        attempt=attempt,
                        delay_sec=round(delay, 2),
                        error_type=type(exc).__name__,
                        error=str(exc)[:300],
                    )
                    self._reset_session(reload_credentials=False)
                    time.sleep(delay)
                    continue
                raise OandaApiError(
                    f"OANDA {method} {safe_path} network failure: {exc}",
                    method=method,
                    endpoint=safe_path,
                    outcome_uncertain=not read_only,
                ) from exc

            request_id = str(response.headers.get("RequestID") or response.headers.get("request-id") or "")
            try:
                payload: Any = response.json() if response.text.strip() else {}
            except ValueError:
                payload = {"raw": response.text[:500]}

            if response.status_code == 401 and read_only and attempt < max_attempts:
                delay = min(15.0, self.args.api_retry_base_sec * (2 ** (attempt - 1)))
                self.log(
                    "api_auth_retry",
                    method=method,
                    endpoint=safe_path,
                    attempt=attempt,
                    delay_sec=round(delay, 2),
                    request_id=request_id,
                )
                self._reset_session(reload_credentials=True)
                time.sleep(delay)
                continue

            if response.status_code in retryable_statuses and read_only and attempt < max_attempts:
                delay = min(15.0, self.args.api_retry_base_sec * (2 ** (attempt - 1)))
                self.log(
                    "api_retry",
                    method=method,
                    endpoint=safe_path,
                    attempt=attempt,
                    status=response.status_code,
                    request_id=request_id,
                    delay_sec=round(delay, 2),
                )
                self._reset_session(reload_credentials=False)
                time.sleep(delay)
                continue

            if response.status_code >= 400:
                uncertain = not read_only and response.status_code in retryable_statuses
                raise OandaApiError(
                    f"OANDA {method} {safe_path} failed {response.status_code}: {payload}",
                    status=response.status_code,
                    method=method,
                    endpoint=safe_path,
                    request_id=request_id,
                    payload=payload,
                    outcome_uncertain=uncertain,
                )
            if not isinstance(payload, dict):
                if read_only and attempt < max_attempts:
                    time.sleep(self.args.api_retry_base_sec)
                    continue
                raise OandaApiError(
                    f"OANDA {method} {safe_path} returned a non-object response",
                    status=response.status_code,
                    method=method,
                    endpoint=safe_path,
                    request_id=request_id,
                    payload=payload,
                    outcome_uncertain=not read_only,
                )
            if request_id:
                payload.setdefault("_requestID", request_id)
            return payload
        raise OandaApiError(f"OANDA {method} {safe_path} exhausted retries", method=method, endpoint=safe_path)

    def account_details(self) -> tuple[dict[str, Any], str]:
        payload = self.request("GET", f"/v3/accounts/{self.account_id}")
        account = payload.get("account") or {}
        return account, str(payload.get("lastTransactionID") or account.get("lastTransactionID") or "")

    def summary(self) -> dict[str, Any]:
        payload = self.request("GET", f"/v3/accounts/{self.account_id}/summary")
        return payload.get("account") or {}

    def load_instrument_meta(self) -> InstrumentMeta:
        global PIP
        cache_key = (self.account_id, INSTRUMENT)
        cached = INSTRUMENT_META_CACHE.get(cache_key)
        if cached is not None:
            meta, pip_size = cached
            PIP = pip_size
            return meta
        payload = self.request(
            "GET",
            f"/v3/accounts/{self.account_id}/instruments",
            params={"instruments": INSTRUMENT},
        )
        instrument = (payload.get("instruments") or [{}])[0]
        pip_location = int(safe_float(instrument.get("pipLocation"), math.log10(PIP)))
        PIP = 10.0**pip_location
        meta = InstrumentMeta(
            display_precision=max(1, int(instrument.get("displayPrecision") or 5)),
            margin_rate=max(PIP, safe_float(instrument.get("marginRate"), 0.02)),
            minimum_trade_size=max(1, int(safe_float(instrument.get("minimumTradeSize"), 1.0))),
        )
        INSTRUMENT_META_CACHE[cache_key] = (meta, PIP)
        return meta

    def fmt_price(self, value: float) -> str:
        return f"{value:.{self.instrument_meta.display_precision}f}"

    def rest_quote(self) -> Quote:
        payload = self.request(
            "GET",
            f"/v3/accounts/{self.account_id}/pricing",
            params={"instruments": INSTRUMENT, "includeUnitsAvailable": "false"},
        )
        price = (payload.get("prices") or [{}])[0]
        return quote_from_price_payload(price, "rest")

    def pricing_mid(self, instrument: str) -> float:
        payload = self.request(
            "GET",
            f"/v3/accounts/{self.account_id}/pricing",
            params={"instruments": normalize_instrument(instrument), "includeUnitsAvailable": "false"},
        )
        return quote_from_price_payload((payload.get("prices") or [{}])[0], "conversion").mid

    def load_quote_to_account_rate(self) -> float:
        quote = quote_currency(INSTRUMENT)
        cache_key = (self.account_id, quote, self.account_currency)
        cached = QUOTE_TO_ACCOUNT_RATE_CACHE.get(cache_key)
        if cached is not None:
            return cached
        if quote == self.account_currency:
            QUOTE_TO_ACCOUNT_RATE_CACHE[cache_key] = 1.0
            return 1.0
        direct = f"{quote}_{self.account_currency}"
        try:
            rate = self.pricing_mid(direct)
            if rate > 0.0:
                QUOTE_TO_ACCOUNT_RATE_CACHE[cache_key] = rate
                return rate
        except OandaApiError as exc:
            self.log("conversion_rate_direct_unavailable", instrument=direct, status=exc.status)
        inverse = f"{self.account_currency}_{quote}"
        try:
            inverse_rate = self.pricing_mid(inverse)
            if inverse_rate > 0.0:
                rate = 1.0 / inverse_rate
                QUOTE_TO_ACCOUNT_RATE_CACHE[cache_key] = rate
                return rate
        except OandaApiError as exc:
            self.log("conversion_rate_inverse_unavailable", instrument=inverse, status=exc.status)
        raise SystemExit(f"Could not resolve {quote}->{self.account_currency} conversion for {INSTRUMENT}.")

    def quote(self) -> Quote:
        if self.price_stream is not None:
            streamed = self.price_stream.latest()
            if streamed is not None and streamed.local_age_sec <= self.args.max_quote_age_sec:
                return streamed
            self.log_rate_limited(
                "stream_quote_fallback",
                30.0,
                "price_stream_fallback",
                available=streamed is not None,
                local_age_sec=None if streamed is None else round(streamed.local_age_sec, 3),
            )
        return self.rest_quote()

    def candles(self, granularity: str, count: int) -> list[dict[str, Any]]:
        payload = self.request(
            "GET",
            f"/v3/instruments/{INSTRUMENT}/candles",
            params={"price": "MBA", "granularity": granularity, "count": str(count)},
        )
        candles = payload.get("candles") or []
        return [candle for candle in candles if isinstance(candle, dict) and candle.get("complete")]

    def open_trades(self) -> list[dict[str, Any]]:
        payload = self.request("GET", f"/v3/accounts/{self.account_id}/openTrades")
        return [trade for trade in payload.get("trades") or [] if isinstance(trade, dict)]

    def current_instrument_trades(self) -> list[dict[str, Any]]:
        return [trade for trade in self.open_trades() if trade.get("instrument") == INSTRUMENT]

    def get_trade(self, trade_id: str) -> dict[str, Any]:
        payload = self.request("GET", f"/v3/accounts/{self.account_id}/trades/{trade_id}")
        return payload.get("trade") or {}

    def transactions_since(self, transaction_id: str) -> dict[str, Any]:
        if not transaction_id:
            return {"transactions": [], "lastTransactionID": self.last_transaction_id}
        return self.request(
            "GET",
            f"/v3/accounts/{self.account_id}/transactions/sinceid",
            params={"id": transaction_id},
        )

    def poll_account_changes(self) -> dict[str, Any]:
        if not self.last_transaction_id:
            self.account_snapshot, self.last_transaction_id = self.account_details()
            return {"changes": {}, "state": {}, "lastTransactionID": self.last_transaction_id}
        payload = self.request(
            "GET",
            f"/v3/accounts/{self.account_id}/changes",
            params={"sinceTransactionID": self.last_transaction_id},
        )
        returned = str(payload.get("lastTransactionID") or "")
        if returned:
            self.last_transaction_id = returned
        return payload

    def target_pips(self) -> float:
        if self.args.take_profit_pips > 0.0:
            return self.args.take_profit_pips
        return self.args.stop_loss_pips * self.args.take_profit_r

    def strategy_names(self) -> list[str]:
        names = [self.args.strategy]
        for name in str(self.args.shadow_strategies or "").split(","):
            cleaned = name.strip()
            if cleaned and cleaned not in names:
                names.append(cleaned)
        allowed = {"momentum", "pullback", "macd_rsi_reversal"}
        invalid = [name for name in names if name not in allowed]
        if invalid:
            raise FatalSessionError(f"Unknown shadow strategy: {', '.join(invalid)}")
        return names

    def load_signal_candles(self, strategies: list[str]) -> dict[str, list[dict[str, Any]]]:
        data = {"M1": self.candles("M1", 90), "M5": self.candles("M5", 50)}
        if "macd_rsi_reversal" in strategies:
            data["M10"] = self.candles("M10", 90)
            data["M30"] = self.candles("M30", 90)
        return data

    def common_signal_meta(
        self, quote: Quote, candle_sets: dict[str, list[dict[str, Any]]]
    ) -> tuple[dict[str, Any], str | None]:
        m1 = candle_sets.get("M1") or []
        m5 = candle_sets.get("M5") or []
        if len(m1) < 30 or len(m5) < 22:
            return {"m1_count": len(m1), "m5_count": len(m5)}, "insufficient_candles"
        closes = candle_closes(m1)
        m5_closes = candle_closes(m5)
        last = closes[-1]
        window = closes[-20:]
        low = min(window)
        high = max(window)
        pos20 = 0.5 if math.isclose(high, low) else (last - low) / (high - low)
        r1 = (last - closes[-2]) / PIP
        r3 = (last - closes[-4]) / PIP
        r5 = (last - closes[-6]) / PIP
        m5_r1 = (m5_closes[-1] - m5_closes[-2]) / PIP
        m5_r3 = (m5_closes[-1] - m5_closes[-4]) / PIP
        m5_atr = atr_pips(m5)
        historical_spreads = [spread for spread in (candle_spread_pips(c) for c in m1[-20:]) if spread is not None]
        median_spread = statistics.median(historical_spreads) if historical_spreads else quote.spread_pips
        candle_time = str(m1[-1].get("time") or "")
        candle_start = parse_rfc3339(candle_time)
        quote_timestamp = parse_rfc3339(quote.time)
        signal_age = math.inf
        if candle_start is not None:
            if quote_timestamp is not None:
                signal_age = max(
                    0.0,
                    (quote_timestamp - candle_start).total_seconds() + quote.local_age_sec - 60.0,
                )
            else:
                signal_age = max(0.0, (datetime.now(timezone.utc) - candle_start).total_seconds() - 60.0)
        min_signal = max(self.args.min_signal_pips, quote.spread_pips * self.args.min_signal_to_spread)
        meta = {
            "last": last,
            "r1_pips": round(r1, 2),
            "r3_pips": round(r3, 2),
            "r5_pips": round(r5, 2),
            "m5_r1_pips": round(m5_r1, 2),
            "m5_r3_pips": round(m5_r3, 2),
            "pos20": round(pos20, 3),
            "m1_candle_time": candle_time,
            "m5_candle_time": m5[-1].get("time"),
            "spread_pips": round(quote.spread_pips, 2),
            "median_m1_spread_pips": round(median_spread, 2),
            "m5_atr14_pips": round(m5_atr, 2),
            "spread_to_m5_atr": None if m5_atr <= 0.0 else round(quote.spread_pips / m5_atr, 3),
            "required_signal_pips": round(min_signal, 2),
            "signal_age_sec": None if not math.isfinite(signal_age) else round(signal_age, 2),
            "quote_time": quote.time,
            "quote_source": quote.source,
            "quote_local_age_sec": round(quote.local_age_sec, 3),
            "quote_server_age_sec": round(quote.server_age_sec, 3),
            "quote_server_minus_local_sec": None
            if not math.isfinite(quote.server_clock_offset_sec)
            else round(quote.server_clock_offset_sec, 3),
            "target_pips": round(self.target_pips(), 2),
        }
        if not quote.tradeable:
            return meta, "quote_not_tradeable"
        if quote.local_age_sec > self.args.max_quote_age_sec or quote.server_age_sec > self.args.max_quote_server_age_sec:
            return meta, "stale_quote"
        if signal_age > self.args.max_signal_age_sec:
            return meta, "stale_signal_candle"
        if m5_atr > 0.0 and quote.spread_pips / m5_atr > self.args.max_spread_atr_fraction:
            return meta, "spread_too_large_for_volatility"
        if self.target_pips() < quote.spread_pips * self.args.min_reward_to_spread:
            return meta, "target_too_small_for_spread"
        return meta, None

    def evaluate_strategy(
        self,
        name: str,
        quote: Quote,
        candle_sets: dict[str, list[dict[str, Any]]],
        common: dict[str, Any],
    ) -> tuple[str | None, dict[str, Any]]:
        m1 = candle_sets["M1"]
        m5 = candle_sets["M5"]
        closes = candle_closes(m1)
        m5_closes = candle_closes(m5)
        last = closes[-1]
        r1 = safe_float(common.get("r1_pips"))
        r3 = safe_float(common.get("r3_pips"))
        r5 = safe_float(common.get("r5_pips"))
        m5_r1 = safe_float(common.get("m5_r1_pips"))
        m5_r3 = safe_float(common.get("m5_r3_pips"))
        pos20 = safe_float(common.get("pos20"), 0.5)
        min_signal = safe_float(common.get("required_signal_pips"))
        direction: str | None = None
        details: dict[str, Any] = {"strategy": name}

        if name == "momentum":
            if (
                r1 >= self.args.min_r1_pips
                and r3 >= min_signal
                and r5 >= self.args.min_signal_pips
                and m5_r1 >= self.args.min_m5_signal_pips
                and m5_r3 >= self.args.min_m5_signal_pips
                and pos20 >= self.args.long_pos20_min
            ):
                direction = "buy"
            elif (
                r1 <= -self.args.min_r1_pips
                and r3 <= -min_signal
                and r5 <= -self.args.min_signal_pips
                and m5_r1 <= -self.args.min_m5_signal_pips
                and m5_r3 <= -self.args.min_m5_signal_pips
                and pos20 <= self.args.short_pos20_max
            ):
                direction = "sell"

        elif name == "pullback":
            m1_ema9 = ema_series(closes, 9)
            m5_fast = ema_series(m5_closes, 8)
            m5_slow = ema_series(m5_closes, 21)
            details.update(
                {
                    "m1_ema9": round(m1_ema9[-1], 6),
                    "m5_ema8": round(m5_fast[-1], 6),
                    "m5_ema21": round(m5_slow[-1], 6),
                }
            )
            if (
                m5_fast[-1] > m5_slow[-1]
                and m5_r3 >= self.args.min_m5_signal_pips
                and closes[-2] <= m1_ema9[-2]
                and last > m1_ema9[-1]
                and r1 >= self.args.min_r1_pips
                and 0.35 <= pos20 <= 0.90
            ):
                direction = "buy"
            elif (
                m5_fast[-1] < m5_slow[-1]
                and m5_r3 <= -self.args.min_m5_signal_pips
                and closes[-2] >= m1_ema9[-2]
                and last < m1_ema9[-1]
                and r1 <= -self.args.min_r1_pips
                and 0.10 <= pos20 <= 0.65
            ):
                direction = "sell"

        elif name == "macd_rsi_reversal":
            m10 = candle_sets.get("M10") or []
            m30 = candle_sets.get("M30") or []
            if len(m10) < 35 or len(m30) < 35:
                return None, {**common, **details, "reason": "insufficient_multitimeframe_candles"}
            m1_rsi = rsi_series(closes)
            m10_rsi = rsi_series(candle_closes(m10))
            m1_hist = macd_histogram(closes)
            m30_closes = candle_closes(m30)
            m30_fast = ema_series(m30_closes, 12)
            m30_slow = ema_series(m30_closes, 26)
            previous_rsi = m1_rsi[-2]
            current_rsi = m1_rsi[-1]
            current_m10_rsi = m10_rsi[-1]
            details.update(
                {
                    "m1_rsi14": None if current_rsi is None else round(current_rsi, 2),
                    "m10_rsi14": None if current_m10_rsi is None else round(current_m10_rsi, 2),
                    "m1_macd_hist": round(m1_hist[-1], 8),
                    "m1_macd_hist_prev": round(m1_hist[-2], 8),
                    "m30_ema12": round(m30_fast[-1], 6),
                    "m30_ema26": round(m30_slow[-1], 6),
                }
            )
            if previous_rsi is not None and current_rsi is not None and current_m10_rsi is not None:
                if (
                    m30_fast[-1] >= m30_slow[-1]
                    and current_m10_rsi <= 45.0
                    and previous_rsi <= self.args.rsi_oversold
                    and current_rsi > self.args.rsi_oversold
                    and m1_hist[-1] > m1_hist[-2]
                ):
                    direction = "buy"
                elif (
                    m30_fast[-1] <= m30_slow[-1]
                    and current_m10_rsi >= 55.0
                    and previous_rsi >= self.args.rsi_overbought
                    and current_rsi < self.args.rsi_overbought
                    and m1_hist[-1] < m1_hist[-2]
                ):
                    direction = "sell"

        meta = {**common, **details}
        if direction:
            entry_side = quote.ask if direction == "buy" else quote.bid
            adverse_drift = (entry_side - last) / PIP if direction == "buy" else (last - entry_side) / PIP
            meta["entry_drift_pips"] = round(adverse_drift, 2)
            if adverse_drift > self.args.max_entry_drift_pips:
                return None, {**meta, "reason": "entry_drift_too_large"}
            return direction, meta
        return None, {**meta, "reason": "no_setup"}

    def signal_bundle(
        self, quote: Quote
    ) -> tuple[str | None, dict[str, Any], dict[str, tuple[str | None, dict[str, Any]]]]:
        strategies = self.strategy_names()
        candle_sets = self.load_signal_candles(strategies)
        common, rejection = self.common_signal_meta(quote, candle_sets)
        if rejection:
            rejected = {name: (None, {**common, "strategy": name, "reason": rejection}) for name in strategies}
            return None, rejected[self.args.strategy][1], rejected
        evaluated = {
            name: self.evaluate_strategy(name, quote, candle_sets, common) for name in strategies
        }
        primary_direction, primary_meta = evaluated[self.args.strategy]
        return primary_direction, primary_meta, evaluated

    def sizing_decision(self, account: dict[str, Any], quote: Quote) -> SizingDecision:
        balance = safe_float(account.get("balance"), self.start_balance)
        margin_available = safe_float(account.get("marginAvailable"), balance)
        return calculate_sizing(
            balance=balance,
            margin_available=margin_available,
            quote_mid=quote.mid,
            quote_to_account_rate=self.quote_to_account_rate,
            stop_pips=self.args.stop_loss_pips,
            risk_per_trade_pct=self.args.risk_per_trade_pct,
            fixed_units=self.args.units,
            max_units=self.args.max_units,
            minimum_units=self.instrument_meta.minimum_trade_size,
            margin_rate=self.instrument_meta.margin_rate,
            max_margin_fraction=self.args.max_margin_fraction,
        )

    def build_client_id(self) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        prefix = re.sub(r"[^a-z0-9]", "", INSTRUMENT.lower())[:12]
        return f"{prefix}-v9-{stamp}-{secrets.token_hex(3)}"

    def reconcile_uncertain_order(
        self, client_id: str, since_transaction_id: str, expected_units: int
    ) -> dict[str, Any] | None:
        for attempt in range(1, self.args.order_reconcile_attempts + 1):
            if self.supports_client_extensions:
                try:
                    trade = self.get_trade(f"@{client_id}")
                    if trade:
                        return {
                            "lastTransactionID": self.last_transaction_id,
                            "orderFillTransaction": {
                                "id": "reconciled",
                                "time": trade.get("openTime"),
                                "price": trade.get("price"),
                                "tradeOpened": {
                                    "tradeID": trade.get("id"),
                                    "units": trade.get("initialUnits") or trade.get("currentUnits"),
                                    "initialMarginRequired": "",
                                },
                            },
                        }
                except OandaApiError as exc:
                    if exc.status != 404:
                        self.log("order_reconcile_read_error", attempt=attempt, error=str(exc)[:500])
            try:
                data = self.transactions_since(since_transaction_id)
                transactions = data.get("transactions") or []
                order_ids: set[str] = set()
                for transaction in transactions:
                    extensions = transaction.get("clientExtensions") or {}
                    if extensions.get("id") == client_id:
                        order_ids.add(str(transaction.get("id") or transaction.get("orderID") or ""))
                candidate_fills: list[dict[str, Any]] = []
                for transaction in transactions:
                    if str(transaction.get("type") or "").upper() != "ORDER_FILL":
                        continue
                    trade_opened = transaction.get("tradeOpened") or {}
                    if not trade_opened:
                        continue
                    exact_order = str(transaction.get("orderID") or "") in order_ids
                    fallback_match = (
                        transaction.get("instrument") == INSTRUMENT
                        and int(safe_float(transaction.get("units"))) == expected_units
                    )
                    if exact_order or fallback_match:
                        candidate_fills.append(transaction)
                if len(candidate_fills) == 1:
                    fill = candidate_fills[0]
                    self.last_transaction_id = str(data.get("lastTransactionID") or self.last_transaction_id)
                    return {"lastTransactionID": self.last_transaction_id, "orderFillTransaction": fill}
            except OandaApiError as exc:
                self.log("order_reconcile_read_error", attempt=attempt, error=str(exc)[:500])
            time.sleep(self.args.order_reconcile_delay_sec)
        return None

    def place_order(
        self, direction: str, signal_quote: Quote, signal_meta: dict[str, Any]
    ) -> TradeContext | None:
        quote = self.quote()
        if not quote.tradeable or quote.local_age_sec > self.args.max_quote_age_sec:
            self.log("order_skipped", reason="fresh_tradeable_quote_unavailable")
            return None
        signal_side = signal_quote.ask if direction == "buy" else signal_quote.bid
        current_side = quote.ask if direction == "buy" else quote.bid
        adverse_move = (current_side - signal_side) / PIP if direction == "buy" else (signal_side - current_side) / PIP
        if adverse_move > self.args.max_entry_drift_pips:
            self.log("order_skipped", reason="quote_moved_after_signal", adverse_move_pips=round(adverse_move, 2))
            return None

        account = self.summary()
        sizing = self.sizing_decision(account, quote)
        if sizing.units <= 0:
            self.log("order_skipped", reason=sizing.reason, sizing=sizing.__dict__)
            return None
        signed_units = sizing.units if direction == "buy" else -sizing.units
        target_pips = self.target_pips()
        if direction == "buy":
            take_profit = quote.ask + target_pips * PIP
            stop_loss = quote.ask - self.args.stop_loss_pips * PIP
            price_bound = quote.ask + self.args.max_entry_slippage_pips * PIP
        else:
            take_profit = quote.bid - target_pips * PIP
            stop_loss = quote.bid + self.args.stop_loss_pips * PIP
            price_bound = quote.bid - self.args.max_entry_slippage_pips * PIP

        client_id = self.build_client_id()
        order: dict[str, Any] = {
            "type": "MARKET",
            "instrument": INSTRUMENT,
            "units": str(signed_units),
            "timeInForce": "FOK",
            "positionFill": "OPEN_ONLY",
            "priceBound": self.fmt_price(price_bound),
            "takeProfitOnFill": {"price": self.fmt_price(take_profit), "timeInForce": "GTC"},
            "stopLossOnFill": {"price": self.fmt_price(stop_loss), "timeInForce": "GTC"},
        }
        trailing_pips = self.args.stop_loss_pips * self.args.trailing_stop_r
        if trailing_pips > 0.0:
            order["trailingStopLossOnFill"] = {
                "distance": self.fmt_price(trailing_pips * PIP),
                "timeInForce": "GTC",
            }
        if self.supports_client_extensions:
            extensions = {"id": client_id, "tag": f"{INSTRUMENT.lower()}_micro_v9", "comment": self.args.strategy}
            order["clientExtensions"] = extensions
            order["tradeClientExtensions"] = extensions
        body = {"order": order}
        if self.args.dry_run:
            self.log(
                "dry_run_order",
                direction=direction,
                units=signed_units,
                signal_bid=signal_quote.bid,
                signal_ask=signal_quote.ask,
                signal_quote_time=signal_quote.time,
                signal_quote_source=signal_quote.source,
                submit_bid=quote.bid,
                submit_ask=quote.ask,
                submit_quote_time=quote.time,
                submit_quote_source=quote.source,
                submit_spread_pips=round(quote.spread_pips, 2),
                signal_to_submit_adverse_move_pips=round(adverse_move, 3),
                price_bound=self.fmt_price(price_bound),
                take_profit=self.fmt_price(take_profit),
                stop_loss=self.fmt_price(stop_loss),
                sizing=sizing.__dict__,
                signal=signal_meta,
            )
            return None

        before_transaction_id = self.last_transaction_id
        try:
            payload = self.request(
                "POST",
                f"/v3/accounts/{self.account_id}/orders",
                body=body,
                client_request_id=client_id,
            )
        except OandaApiError as exc:
            self.log(
                "order_submit_error",
                client_id=client_id,
                status=exc.status,
                request_id=exc.request_id,
                outcome_uncertain=exc.outcome_uncertain,
                error=str(exc)[:800],
            )
            if exc.status in {401, 403}:
                raise FatalSessionError(
                    f"Order authorization failed with HTTP {exc.status}; no further entries will be submitted."
                ) from exc
            if not exc.outcome_uncertain:
                return None
            payload = self.reconcile_uncertain_order(client_id, before_transaction_id, signed_units)
            if payload is None:
                raise FatalSessionError(
                    "Order response was uncertain and account reconciliation could not prove whether it filled."
                ) from exc
            self.log("order_submit_reconciled", client_id=client_id)

        fill = payload.get("orderFillTransaction") or {}
        trade_opened = fill.get("tradeOpened") or {}
        trade_id = str(trade_opened.get("tradeID") or "")
        if not trade_id:
            self.log(
                "order_not_filled",
                client_id=client_id,
                request_id=payload.get("_requestID"),
                cancel=payload.get("orderCancelTransaction"),
                reject=payload.get("orderRejectTransaction"),
            )
            return None
        returned_transaction_id = str(payload.get("lastTransactionID") or fill.get("id") or "")
        if returned_transaction_id:
            self.last_transaction_id = returned_transaction_id
        fill_price = safe_float(fill.get("price"), current_side)
        fill_slippage = (fill_price - quote.ask) / PIP if direction == "buy" else (quote.bid - fill_price) / PIP
        self.trade_count += 1
        context = TradeContext(
            trade_id=trade_id,
            client_id=client_id,
            direction=direction,
            units=sizing.units,
            entry_price=fill_price,
            entry_time=str(fill.get("time") or utc_now()),
            entry_transaction_id=str(fill.get("id") or before_transaction_id),
            signal_quote=signal_quote,
            entry_quote=quote,
            stop_pips=self.args.stop_loss_pips,
            target_pips=target_pips,
            risk_amount=max(PIP, sizing.actual_risk),
            fill_slippage_pips=fill_slippage,
            signal_meta=signal_meta,
            spreads=[quote.spread_pips],
        )
        self.log(
            "order_filled",
            client_id=client_id,
            request_id=payload.get("_requestID"),
            direction=direction,
            units=sizing.units,
            fill_price=fill.get("price"),
            trade_id=trade_id,
            transaction_id=fill.get("id"),
            fill_slippage_pips=round(fill_slippage, 3),
            signal_bid=signal_quote.bid,
            signal_ask=signal_quote.ask,
            signal_quote_time=signal_quote.time,
            signal_quote_source=signal_quote.source,
            submit_bid=quote.bid,
            submit_ask=quote.ask,
            submit_quote_time=quote.time,
            submit_quote_source=quote.source,
            signal_to_submit_adverse_move_pips=round(adverse_move, 3),
            price_bound=self.fmt_price(price_bound),
            half_spread_cost=fill.get("halfSpreadCost"),
            initial_margin_required=trade_opened.get("initialMarginRequired"),
            estimated_margin=round(sizing.estimated_margin, 4),
            risk_amount=round(sizing.actual_risk, 4),
            risk_pct=round(sizing.actual_risk_pct, 4),
            take_profit=self.fmt_price(take_profit),
            stop_loss=self.fmt_price(stop_loss),
            signal=signal_meta,
        )
        return context

    def close_trade(self, context: TradeContext, reason: str) -> dict[str, Any] | None:
        if self.args.dry_run:
            self.log("dry_run_close", trade_id=context.trade_id, reason=reason)
            return None
        try:
            payload = self.request(
                "PUT",
                f"/v3/accounts/{self.account_id}/trades/{context.trade_id}/close",
                body={"units": "ALL"},
                client_request_id=f"close-{context.client_id}"[:128],
            )
            returned = str(payload.get("lastTransactionID") or "")
            if returned:
                self.last_transaction_id = returned
            fill = payload.get("orderFillTransaction") or {}
            self.log(
                "close_requested",
                trade_id=context.trade_id,
                reason=reason,
                request_id=payload.get("_requestID"),
                transaction_id=fill.get("id"),
                fill_price=fill.get("price"),
                realized_pl=fill.get("pl"),
            )
            return payload
        except OandaApiError as exc:
            self.log(
                "close_request_error",
                trade_id=context.trade_id,
                reason=reason,
                request_id=exc.request_id,
                outcome_uncertain=exc.outcome_uncertain,
                error=str(exc)[:800],
            )
            if not exc.outcome_uncertain:
                raise
            return None

    def update_excursion(self, context: TradeContext, quote: Quote) -> None:
        if context.direction == "buy":
            net_pips = (quote.bid - context.entry_price) / PIP
        else:
            net_pips = (context.entry_price - quote.ask) / PIP
        context.mfe_pips = max(context.mfe_pips, net_pips)
        context.mae_pips = min(context.mae_pips, net_pips)
        context.spreads.append(quote.spread_pips)

    def finalize_trade(
        self,
        context: TradeContext,
        trade: dict[str, Any] | None,
        requested_reason: str,
    ) -> TradeOutcome:
        transaction_data: dict[str, Any] = {"transactions": []}
        try:
            transaction_data = self.transactions_since(context.entry_transaction_id)
            returned = str(transaction_data.get("lastTransactionID") or "")
            if returned:
                self.last_transaction_id = returned
        except OandaApiError as exc:
            self.log("trade_finalize_transaction_error", trade_id=context.trade_id, error=str(exc)[:500])
        close = extract_trade_close(transaction_data.get("transactions") or [], context.trade_id)
        trade = trade or {}
        realized = safe_float(
            None if close is None else close.get("realized_pl"),
            safe_float(trade.get("realizedPL")),
        )
        close_price_value = None if close is None else safe_float(close.get("price"), math.nan)
        if close_price_value is not None and not math.isfinite(close_price_value):
            close_price_value = None
        if close_price_value is None:
            average_close = safe_float(trade.get("averageClosePrice"), math.nan)
            close_price_value = average_close if math.isfinite(average_close) else None
        close_time = str((close or {}).get("time") or trade.get("closeTime") or utc_now())
        close_reason = str((close or {}).get("reason") or requested_reason or "trade_closed")
        transaction_id = str((close or {}).get("transaction_id") or "")
        realized_r = realized / context.risk_amount
        duration = max(0.0, time.monotonic() - context.opened_monotonic)
        balance: float | None = None
        try:
            balance = safe_float(self.summary().get("balance"))
        except OandaApiError:
            pass
        self.log(
            "trade_result",
            trade_id=context.trade_id,
            client_id=context.client_id,
            direction=context.direction,
            units=context.units,
            entry_price=context.entry_price,
            close_price=close_price_value,
            close_time=close_time,
            close_reason=close_reason,
            requested_reason=requested_reason,
            close_transaction_id=transaction_id,
            realized_pl=round(realized, 6),
            realized_r=round(realized_r, 4),
            risk_amount=round(context.risk_amount, 6),
            duration_sec=round(duration, 2),
            mfe_pips=round(context.mfe_pips, 3),
            mae_pips=round(context.mae_pips, 3),
            signal_spread_pips=round(context.signal_quote.spread_pips, 3),
            entry_spread_pips=round(context.entry_quote.spread_pips, 3),
            min_spread_pips=round(min(context.spreads), 3) if context.spreads else None,
            max_spread_pips=round(max(context.spreads), 3) if context.spreads else None,
            fill_slippage_pips=round(context.fill_slippage_pips, 3),
            balance=balance,
            signal=context.signal_meta,
            signal_bid=context.signal_quote.bid,
            signal_ask=context.signal_quote.ask,
            signal_quote_time=context.signal_quote.time,
            signal_quote_source=context.signal_quote.source,
            submit_bid=context.entry_quote.bid,
            submit_ask=context.entry_quote.ask,
            submit_quote_time=context.entry_quote.time,
            submit_quote_source=context.entry_quote.source,
        )
        return TradeOutcome(
            realized_pl=realized,
            realized_r=realized_r,
            reason=close_reason,
            close_price=close_price_value,
            close_time=close_time,
            transaction_id=transaction_id,
        )

    def confirm_closed(self, context: TradeContext, requested_reason: str) -> TradeOutcome:
        last_trade: dict[str, Any] | None = None
        for attempt in range(1, self.args.close_confirm_attempts + 1):
            try:
                self.poll_account_changes()
                last_trade = self.get_trade(context.trade_id)
                if str(last_trade.get("state") or "").upper() != "OPEN":
                    return self.finalize_trade(context, last_trade, requested_reason)
            except OandaApiError as exc:
                if exc.status == 404:
                    return self.finalize_trade(context, last_trade, requested_reason)
                self.log("close_confirm_error", trade_id=context.trade_id, attempt=attempt, error=str(exc)[:500])
            time.sleep(self.args.account_poll_sec)
        raise FatalSessionError(
            f"Could not confirm whether trade {context.trade_id} closed; no further entries will be submitted."
        )

    def monitor_trade(self, context: TradeContext) -> TradeOutcome:
        outage_started: float | None = None
        while time.monotonic() - context.opened_monotonic < self.args.max_hold_sec:
            try:
                quote = self.quote()
                if quote.tradeable and quote.local_age_sec <= self.args.max_quote_age_sec:
                    self.update_excursion(context, quote)
                self.poll_account_changes()
                trade = self.get_trade(context.trade_id)
                outage_started = None
            except OandaApiError as exc:
                if outage_started is None:
                    outage_started = time.monotonic()
                outage = time.monotonic() - outage_started
                self.log_rate_limited(
                    f"monitor-{context.trade_id}",
                    10.0,
                    "monitor_error",
                    trade_id=context.trade_id,
                    outage_sec=round(outage, 2),
                    request_id=exc.request_id,
                    error=str(exc)[:500],
                )
                if outage >= self.args.max_monitor_outage_sec:
                    raise FatalSessionError(
                        "Trade monitoring API outage exceeded its limit. Broker-side stop and target remain attached."
                    ) from exc
                time.sleep(self.args.account_poll_sec)
                continue

            if str(trade.get("state") or "").upper() != "OPEN":
                return self.finalize_trade(context, trade, "broker_dependent_order")
            unrealized = safe_float(trade.get("unrealizedPL"))
            current_r = unrealized / context.risk_amount
            elapsed = time.monotonic() - context.opened_monotonic
            if elapsed >= self.args.profit_lock_after_sec and current_r >= self.args.profit_lock_r:
                self.close_trade(context, "profit_lock_r")
                return self.confirm_closed(context, "profit_lock_r")
            if elapsed >= self.args.min_hold_sec and current_r <= -abs(self.args.fast_cut_r):
                self.close_trade(context, "fast_cut_r")
                return self.confirm_closed(context, "fast_cut_r")
            time.sleep(self.args.account_poll_sec)

        try:
            trade = self.get_trade(context.trade_id)
            if str(trade.get("state") or "").upper() != "OPEN":
                return self.finalize_trade(context, trade, "broker_dependent_order")
        except OandaApiError as exc:
            if exc.status == 404:
                return self.finalize_trade(context, None, "broker_dependent_order")
            raise FatalSessionError("Could not determine trade state at maximum holding time.") from exc
        self.close_trade(context, "max_hold_sec")
        return self.confirm_closed(context, "max_hold_sec")

    def session_loss_cap(self) -> float:
        caps: list[float] = []
        if self.args.max_session_loss > 0.0:
            caps.append(self.args.max_session_loss)
        if self.args.max_session_loss_pct > 0.0:
            caps.append(self.start_balance * self.args.max_session_loss_pct / 100.0)
        return min(caps) if caps else math.inf

    def next_candle_check_time(self, candle_time: str, quote: Quote | None = None) -> float:
        parsed = parse_rfc3339(candle_time)
        if parsed is None:
            return time.time() + self.args.signal_check_fallback_sec
        local_now = time.time()
        target_server_time = parsed.timestamp() + 120.0 + self.args.candle_close_delay_sec
        if quote is not None:
            quote_time = parse_rfc3339(quote.time)
            if quote_time is not None:
                estimated_server_now = quote_time.timestamp() + quote.local_age_sec
                return local_now + max(0.25, target_server_time - estimated_server_now)
        return max(local_now + 0.25, target_server_time)

    def _run_loop(self) -> str:
        startup_trades = [
            trade
            for trade in self.account_snapshot.get("trades") or []
            if trade.get("instrument") == INSTRUMENT and str(trade.get("state") or "OPEN").upper() == "OPEN"
        ]
        if not startup_trades:
            startup_trades = self.current_instrument_trades()
        if startup_trades:
            self.log(
                "startup_reconciliation_stop",
                reason="existing_instrument_trade",
                instrument=INSTRUMENT,
                trade_ids=[trade.get("id") for trade in startup_trades],
            )
            raise FatalSessionError(f"An existing {INSTRUMENT} trade is open; v9 will not adopt or modify it.")

        if self.args.use_price_stream:
            self.price_stream = PriceStream(
                self._stream_credentials,
                self.log,
                self.args.stream_reconnect_base_sec,
            )
            self.price_stream.start()
            ready = self.price_stream.wait_ready(self.args.stream_start_timeout_sec)
            self.log("price_stream_start", ready=ready)

        stop_at = time.monotonic() + self.args.duration_sec
        next_signal_wall = 0.0
        loss_cap = self.session_loss_cap()
        while time.monotonic() < stop_at and self.trade_count < self.args.max_trades:
            if time.monotonic() < self.next_entry_time:
                time.sleep(min(self.args.idle_sleep_sec, self.next_entry_time - time.monotonic()))
                continue
            if time.time() < next_signal_wall:
                time.sleep(min(self.args.idle_sleep_sec, next_signal_wall - time.time()))
                continue

            account = self.summary()
            current_balance = safe_float(account.get("balance"), self.start_balance)
            session_pl = current_balance - self.start_balance
            if session_pl <= -loss_cap:
                self.log("session_loss_cap_hit", session_pl=round(session_pl, 4), loss_cap=round(loss_cap, 4))
                return "session_loss_cap"
            if self.consecutive_losses >= self.args.max_consecutive_losses:
                self.log("consecutive_loss_cap_hit", consecutive_losses=self.consecutive_losses)
                return "consecutive_loss_cap"
            existing = self.current_instrument_trades()
            if existing:
                raise FatalSessionError(f"An untracked {INSTRUMENT} trade appeared while the bot was flat.")

            quote = self.quote()
            if not quote.tradeable:
                self.log_rate_limited("nontradeable", 30.0, "skip_quote", reason="not_tradeable")
                next_signal_wall = time.time() + self.args.signal_check_fallback_sec
                continue
            if quote.local_age_sec > self.args.max_quote_age_sec or quote.server_age_sec > self.args.max_quote_server_age_sec:
                self.log_rate_limited(
                    "stale-quote",
                    30.0,
                    "skip_quote",
                    reason="stale",
                    local_age_sec=round(quote.local_age_sec, 3),
                    server_age_sec=round(quote.server_age_sec, 3),
                )
                next_signal_wall = time.time() + self.args.signal_check_fallback_sec
                continue
            if quote.spread_pips > self.args.max_spread_pips + self.args.spread_epsilon_pips:
                self.log(
                    "skip_spread",
                    bid=quote.bid,
                    ask=quote.ask,
                    spread_pips=round(quote.spread_pips, 2),
                )
                next_signal_wall = time.time() + self.args.signal_check_fallback_sec
                continue

            direction, meta, evaluated = self.signal_bundle(quote)
            candle_time = str(meta.get("m1_candle_time") or "")
            next_signal_wall = self.next_candle_check_time(candle_time, quote)
            if candle_time and candle_time == self.last_signal_candle_time:
                next_signal_wall = time.time() + self.args.signal_check_fallback_sec
                continue
            self.last_signal_candle_time = candle_time
            for strategy, (shadow_direction, shadow_meta) in evaluated.items():
                if strategy == self.args.strategy:
                    continue
                self.log(
                    "shadow_signal",
                    strategy=strategy,
                    direction=shadow_direction,
                    bid=quote.bid,
                    ask=quote.ask,
                    signal=shadow_meta,
                )
            if not direction:
                self.log(
                    "skip_signal",
                    strategy=self.args.strategy,
                    bid=quote.bid,
                    ask=quote.ask,
                    spread_pips=round(quote.spread_pips, 2),
                    signal=meta,
                )
                continue

            context = self.place_order(direction, quote, meta)
            if context is None:
                continue
            outcome = self.monitor_trade(context)
            self.next_entry_time = time.monotonic() + self.args.cooldown_sec
            if outcome.realized_r < -abs(self.args.scratch_r):
                self.consecutive_losses += 1
                self.next_entry_time = time.monotonic() + max(
                    self.args.cooldown_sec, self.args.loss_cooldown_sec
                )
            elif outcome.realized_r > abs(self.args.scratch_r):
                self.consecutive_losses = 0
            else:
                self.log(
                    "scratch_trade",
                    trade_id=context.trade_id,
                    realized_pl=round(outcome.realized_pl, 6),
                    realized_r=round(outcome.realized_r, 4),
                    scratch_r=self.args.scratch_r,
                )
        return "duration_or_trade_limit"

    def run(self) -> int:
        self.log(
            "session_start",
            version=9,
            account_suffix=self.account_id[-4:],
            start_balance=round(self.start_balance, 4),
            dry_run=self.args.dry_run,
            instrument=INSTRUMENT,
            pip_size=PIP,
            quote_to_account_rate=round(self.quote_to_account_rate, 8),
            strategy=self.args.strategy,
            shadow_strategies=self.strategy_names()[1:],
            duration_sec=self.args.duration_sec,
            max_trades=self.args.max_trades,
            fixed_units=self.args.units,
            max_units=self.args.max_units,
            risk_per_trade_pct=self.args.risk_per_trade_pct,
            max_session_loss_pct=self.args.max_session_loss_pct,
            max_spread_pips=self.args.max_spread_pips,
            stop_loss_pips=self.args.stop_loss_pips,
            target_pips=self.target_pips(),
            profit_lock_r=self.args.profit_lock_r,
            fast_cut_r=self.args.fast_cut_r,
            use_price_stream=self.args.use_price_stream,
            supports_client_extensions=self.supports_client_extensions,
        )
        status = "unknown"
        exit_code = 0
        try:
            status = self._run_loop()
        except KeyboardInterrupt:
            status = "interrupted"
            exit_code = 130
            self.log("session_stop", reason=status)
        except FatalSessionError as exc:
            status = "fatal_session_error"
            exit_code = 2
            self.log("session_stop", reason=status, error=str(exc)[:1200])
        except Exception as exc:
            status = "unhandled_error"
            exit_code = 3
            self.log(
                "fatal_error",
                error_type=type(exc).__name__,
                error=str(exc)[:1200],
                traceback=traceback.format_exc()[-5000:],
            )
        finally:
            if self.price_stream is not None:
                self.price_stream.stop()
            final_balance: float | None = None
            try:
                final_balance = safe_float(self.summary().get("balance"))
            except Exception as exc:
                self.log("final_balance_error", error=str(exc)[:500])
            self.log(
                "session_end",
                status=status,
                trades=self.trade_count,
                consecutive_losses=self.consecutive_losses,
                start_balance=round(self.start_balance, 4),
                final_balance=None if final_balance is None else round(final_balance, 4),
                realized_delta=None
                if final_balance is None
                else round(final_balance - self.start_balance, 4),
                log_path=str(self.log_path),
            )
        return exit_code


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    parser.add_argument("--account-key", default="", help="Name of the account id variable in the creds file.")
    parser.add_argument("--account-id", default="", help="Explicit OANDA practice account id.")
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--log-file", type=Path, default=None)
    parser.add_argument("--instrument", type=normalize_instrument, default=INSTRUMENT)
    parser.add_argument("--duration-sec", type=int, default=600)
    parser.add_argument("--idle-sleep-sec", type=float, default=1.0)
    parser.add_argument("--account-poll-sec", type=float, default=2.0)
    parser.add_argument("--signal-check-fallback-sec", type=float, default=5.0)
    parser.add_argument("--candle-close-delay-sec", type=float, default=2.0)
    parser.add_argument("--strategy", choices=("momentum", "pullback", "macd_rsi_reversal"), default="pullback")
    parser.add_argument("--shadow-strategies", default="momentum,macd_rsi_reversal")
    parser.add_argument("--units", type=int, default=0, help="Optional fixed-unit request; risk and margin caps still apply.")
    parser.add_argument("--max-units", type=int, default=250)
    parser.add_argument("--risk-per-trade-pct", type=float, default=0.25)
    parser.add_argument("--max-margin-fraction", type=float, default=0.25)
    parser.add_argument("--max-trades", type=int, default=5)
    parser.add_argument("--max-session-loss", type=float, default=1.00)
    parser.add_argument("--max-session-loss-pct", type=float, default=1.00)
    parser.add_argument("--max-spread-pips", type=float, default=1.8)
    parser.add_argument("--spread-epsilon-pips", type=float, default=0.02)
    parser.add_argument("--max-spread-atr-fraction", type=float, default=0.55)
    parser.add_argument("--min-reward-to-spread", type=float, default=4.0)
    parser.add_argument("--min-signal-pips", type=float, default=1.0)
    parser.add_argument("--min-signal-to-spread", type=float, default=2.5)
    parser.add_argument("--min-r1-pips", type=float, default=0.1)
    parser.add_argument("--min-m5-signal-pips", type=float, default=0.3)
    parser.add_argument("--long-pos20-min", type=float, default=0.65)
    parser.add_argument("--short-pos20-max", type=float, default=0.35)
    parser.add_argument("--rsi-oversold", type=float, default=35.0)
    parser.add_argument("--rsi-overbought", type=float, default=65.0)
    parser.add_argument("--stop-loss-pips", type=float, default=5.0)
    parser.add_argument("--take-profit-pips", type=float, default=0.0)
    parser.add_argument("--take-profit-r", type=float, default=1.6)
    parser.add_argument("--trailing-stop-r", type=float, default=1.0)
    parser.add_argument("--min-hold-sec", type=float, default=30.0)
    parser.add_argument("--max-hold-sec", type=float, default=180.0)
    parser.add_argument("--fast-cut-r", type=float, default=0.65)
    parser.add_argument("--profit-lock-after-sec", type=float, default=120.0)
    parser.add_argument("--profit-lock-r", type=float, default=0.75)
    parser.add_argument("--scratch-r", type=float, default=0.10)
    parser.add_argument("--cooldown-sec", type=float, default=120.0)
    parser.add_argument("--loss-cooldown-sec", type=float, default=300.0)
    parser.add_argument("--max-consecutive-losses", type=int, default=2)
    parser.add_argument("--max-entry-slippage-pips", type=float, default=0.5)
    parser.add_argument("--max-entry-drift-pips", type=float, default=1.0)
    parser.add_argument("--max-signal-age-sec", type=float, default=20.0)
    parser.add_argument("--max-quote-age-sec", type=float, default=2.0)
    parser.add_argument("--max-quote-server-age-sec", type=float, default=5.0)
    parser.add_argument("--max-monitor-outage-sec", type=float, default=60.0)
    parser.add_argument("--request-timeout-sec", type=float, default=20.0)
    parser.add_argument("--api-retries", type=int, default=3)
    parser.add_argument("--api-retry-base-sec", type=float, default=1.5)
    parser.add_argument("--order-reconcile-attempts", type=int, default=3)
    parser.add_argument("--order-reconcile-delay-sec", type=float, default=1.0)
    parser.add_argument("--close-confirm-attempts", type=int, default=5)
    parser.add_argument("--stream-start-timeout-sec", type=float, default=8.0)
    parser.add_argument("--stream-reconnect-base-sec", type=float, default=1.0)
    parser.add_argument(
        "--use-price-stream",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--no-price-stream", action="store_false", dest="use_price_stream")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    positive_fields = {
        "duration_sec": args.duration_sec,
        "idle_sleep_sec": args.idle_sleep_sec,
        "account_poll_sec": args.account_poll_sec,
        "risk_per_trade_pct": args.risk_per_trade_pct,
        "stop_loss_pips": args.stop_loss_pips,
        "take_profit_r": args.take_profit_r,
        "max_quote_age_sec": args.max_quote_age_sec,
        "max_quote_server_age_sec": args.max_quote_server_age_sec,
    }
    invalid = [name for name, value in positive_fields.items() if value <= 0]
    if invalid:
        raise SystemExit(f"These arguments must be positive: {', '.join(invalid)}")
    if args.units < 0 or args.max_units <= 0:
        raise SystemExit("--units cannot be negative and --max-units must be positive")
    if not 0.0 < args.max_margin_fraction <= 1.0:
        raise SystemExit("--max-margin-fraction must be in (0, 1]")
    if args.take_profit_pips < 0.0 or args.trailing_stop_r < 0.0:
        raise SystemExit("Take-profit pips and trailing-stop R cannot be negative")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return PracticeScalper(args).run()
    except FatalSessionError as exc:
        print(json.dumps({"time": utc_now(), "event": "startup_error", "error": str(exc)}), flush=True)
        return 2
    except Exception as exc:
        print(
            json.dumps(
                {
                    "time": utc_now(),
                    "event": "startup_error",
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:1200],
                }
            ),
            flush=True,
        )
        return 3


if __name__ == "__main__":
    sys.exit(main())
