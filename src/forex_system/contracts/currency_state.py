"""Versioned contract helpers for the research-only currency-state engine."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


SOURCE_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONTRACT_PATH = SOURCE_ROOT / "config" / "currency_state_engine_v2.json"


def canonical_json(payload: Any) -> str:
    """Return the stable JSON representation used for IDs and hashes."""

    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _connected(currencies: tuple[str, ...], instruments: tuple[str, ...]) -> bool:
    adjacency = {currency: set() for currency in currencies}
    for instrument in instruments:
        base, quote = instrument.split("_")
        adjacency[base].add(quote)
        adjacency[quote].add(base)
    pending = [currencies[0]] if currencies else []
    visited: set[str] = set()
    while pending:
        currency = pending.pop()
        if currency in visited:
            continue
        visited.add(currency)
        pending.extend(sorted(adjacency[currency] - visited))
    return visited == set(currencies)


def validate_contract(payload: Mapping[str, Any]) -> None:
    """Fail closed when the supposedly canonical universe is malformed."""

    if int(payload.get("schema_version") or 0) != 1:
        raise ValueError("currency-state contract schema_version must be 1")
    if payload.get("research_only") is not True:
        raise ValueError("currency-state contract must remain research-only")
    if payload.get("execution_eligible") is not False:
        raise ValueError("currency-state contract cannot be execution eligible")
    if payload.get("can_place_orders") is not False:
        raise ValueError("currency-state contract cannot place orders")
    if payload.get("supported_execution_decision") != "no_trade":
        raise ValueError("currency-state contract must support no_trade only")

    currencies = tuple(str(value) for value in payload.get("currencies") or ())
    instruments = tuple(str(value) for value in payload.get("instruments") or ())
    horizons = tuple(int(value) for value in payload.get("horizons_sec") or ())
    if len(currencies) != 21 or len(set(currencies)) != 21:
        raise ValueError("currency-state contract requires 21 unique currencies")
    if len(instruments) != 68 or len(set(instruments)) != 68:
        raise ValueError("currency-state contract requires 68 unique instruments")
    if not horizons or len(set(horizons)) != len(horizons) or min(horizons) <= 0:
        raise ValueError("currency-state horizons must be unique positive seconds")

    known = set(currencies)
    for instrument in instruments:
        parts = instrument.split("_")
        if len(parts) != 2 or parts[0] == parts[1]:
            raise ValueError(f"invalid instrument {instrument!r}")
        if any(part not in known for part in parts):
            raise ValueError(f"instrument outside canonical currencies: {instrument}")
    if not _connected(currencies, instruments):
        raise ValueError("canonical 68-edge universe must connect all 21 currencies")

    channel_ids = [
        str(row.get("id"))
        for row in payload.get("component_channels") or ()
        if isinstance(row, Mapping)
    ]
    if len(channel_ids) != len(set(channel_ids)) or not channel_ids:
        raise ValueError("component channel IDs must be nonempty and unique")


def load_contract(path: Path = DEFAULT_CONTRACT_PATH) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    validate_contract(payload)
    material = dict(payload)
    material["contract_sha256"] = stable_hash(payload)
    material["contract_path"] = str(path.resolve())
    return material


__all__ = [
    "DEFAULT_CONTRACT_PATH",
    "canonical_json",
    "load_contract",
    "stable_hash",
    "validate_contract",
]
