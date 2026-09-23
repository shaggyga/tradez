#!/usr/bin/env python3
"""Small authoritative fallback for OANDA FX pip locations.

Live and replay code should prefer the exact `pip`/`pipLocation` carried by
venue metadata.  This module exists for retained files and diagnostics whose
schema predates that field.  Quote-currency heuristics are not sufficient:
HUF and THB use a 0.01 pip in this 68-instrument universe, while HKD_JPY uses
0.0001 despite its JPY quote currency.
"""

from __future__ import annotations

from typing import Any, Mapping


PIP_LOCATION_MINUS2 = frozenset(
    {
        "AUD_JPY",
        "CAD_JPY",
        "CHF_JPY",
        "EUR_HUF",
        "EUR_JPY",
        "GBP_JPY",
        "NZD_JPY",
        "SGD_JPY",
        "TRY_JPY",
        "USD_HUF",
        "USD_JPY",
        "USD_THB",
        "ZAR_JPY",
    }
)


def fallback_pip_size(instrument: str) -> float:
    """Return the validated pip size when venue metadata is unavailable."""
    normalized = str(instrument or "").strip().upper().replace("/", "_")
    return 0.01 if normalized in PIP_LOCATION_MINUS2 else 0.0001


def resolve_pip_size(instrument: str, *metadata: Mapping[str, Any] | None) -> float:
    """Prefer a valid venue value, falling back to the audited instrument map."""
    for row in metadata:
        if not isinstance(row, Mapping):
            continue
        for key in ("pip", "pip_size"):
            try:
                value = float(row.get(key))
            except (TypeError, ValueError):
                continue
            if value > 0.0:
                return value
        try:
            location = int(row.get("pipLocation"))
        except (TypeError, ValueError):
            continue
        value = 10.0**location
        if value > 0.0:
            return value
    return fallback_pip_size(instrument)


def pips_multiplier(instrument: str) -> float:
    return 1.0 / fallback_pip_size(instrument)


__all__ = [
    "PIP_LOCATION_MINUS2",
    "fallback_pip_size",
    "resolve_pip_size",
    "pips_multiplier",
]
