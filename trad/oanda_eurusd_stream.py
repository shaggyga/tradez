#!/usr/bin/env python3
"""Read-only EUR/USD stream parsing and configured practice access.

No strategy imports, account writes, orders, model work, or canonical cache writes.
Only the Python standard library and the existing exclusive HTTP listener are used.
"""
from __future__ import annotations

import argparse
import ast
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import math
import os
from pathlib import Path
import re
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parent
STREAM = 'https://stream-fxpractice.oanda.com'
INSTRUMENT = 'EUR_USD'
MAX_EVENTS = 3600


def credentials() -> tuple[str, str]:
    """Use the same credential aliases as the existing practice quote helpers."""
    path = Path(os.environ.get('OANDA_CREDS_PATH', str(ROOT / 'creds')))
    if path.drive.upper() == 'D:':
        raise ValueError('Configured credential path is outside this task scope')
    values = {}
    if path.is_file():
        for line in path.read_text(encoding='utf-8', errors='ignore').splitlines():
            if '=' not in line or line.lstrip().startswith('#'):
                continue
            key, value = (part.strip() for part in line.split('=', 1))
            if not re.fullmatch(r'OANDA_[A-Z0-9_]+', key):
                continue
            try:
                values[key] = ast.literal_eval(value)
            except (ValueError, SyntaxError):
                values[key] = value.strip('"\'')
    for key, value in os.environ.items():
        if key.startswith('OANDA_'):
            values[key] = value
    token = next((str(values[k]) for k in ('OANDA_API_KEY', 'OANDA_ACCESS_TOKEN',
                 'OANDA_TOKEN', 'OANDA_API_TOKEN') if values.get(k)), '')
    account = next((str(values[k]) for k in ('OANDA_ACCOUNT_ID_DUM1',
                   'OANDA_ACCOUNT_ID_DUM4', 'OANDA_ACCOUNT_ID') if values.get(k)), '')
    if not token or not account:
        raise ValueError('Practice quote credentials are not configured')
    return token, account


def parse_quote(payload: dict, received_epoch: float, generation: int,
                initial_snapshot: bool) -> dict | None:
    """Keep actual broker time; reject bad quotes instead of making replacements."""
    if payload.get('type') != 'PRICE' or payload.get('instrument') != INSTRUMENT:
        return None
    try:
        bid = Decimal(payload['bids'][0]['price'])
        ask = Decimal(payload['asks'][0]['price'])
        if not bid.is_finite() or not ask.is_finite() or not (0 < bid <= ask):
            return None
        stamp = payload['time']
        if not isinstance(stamp, str):
            return None
        dt = datetime.fromisoformat(stamp.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            return None
        epoch = dt.timestamp()
        numbers = [float(bid), float(ask), float((bid + ask) / 2),
                   float((ask - bid) / Decimal('0.0001'))]
        if not all(math.isfinite(x) for x in numbers + [epoch, received_epoch]):
            return None
    except (KeyError, IndexError, TypeError, ValueError, InvalidOperation, OverflowError):
        return None
    return dict(instrument=INSTRUMENT, bid=numbers[0], ask=numbers[1], mid=numbers[2],
                spread_pips=numbers[3], broker_time=stamp, broker_epoch=epoch,
                received_epoch=received_epoch, timestamp_age_ms=(received_epoch-epoch)*1000,
                generation=generation, initial_snapshot=initial_snapshot,
                tradeable=payload.get('status') == 'tradeable')

