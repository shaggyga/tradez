#!/usr/bin/env python3
"""Terminal tombstone for prospective official-event response V2."""

from __future__ import annotations

import json

try:
    from forex_system.ingestion.prospective_event_response_v2_tombstone import (
        ProspectiveEventResponseV2RetiredError,
        raise_retired_before_ledger_open,
    )
except ModuleNotFoundError:
    from src.forex_system.ingestion.prospective_event_response_v2_tombstone import (
        ProspectiveEventResponseV2RetiredError,
        raise_retired_before_ledger_open,
    )


def main() -> int:
    try:
        raise_retired_before_ledger_open()
    except ProspectiveEventResponseV2RetiredError as exc:
        print(json.dumps({"status": "retired_terminal", "error": str(exc)}, sort_keys=True))
        return 2
    raise AssertionError("retirement tombstone unexpectedly returned")


if __name__ == "__main__":
    raise SystemExit(main())
