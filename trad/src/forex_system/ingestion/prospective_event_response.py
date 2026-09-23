"""Terminal response-V2 tombstone plus safe frozen calculation compatibility.

All V2 ledger surfaces fail through the terminal tombstone before SQLite can
be imported or a connection can be attempted.  Deterministic calculations are
provided by an explicitly named hash-verified helper module so response V3 can
remain operational without depending on the retired V2 writer.
"""

from __future__ import annotations

from typing import Any

from .prospective_event_response_v2_frozen_helpers import (
    CONTRACT_ID,
    SCHEMA_VERSION,
    ProspectiveEventResponseError,
    build_event_plans,
    build_samples,
    collection_workset,
    completed_candle_candidates,
    load_contract,
    mature_outcome_rows,
    quote_snapshot_candidates,
)
from .prospective_event_response_v2_tombstone import (
    ProspectiveEventResponseV2RetiredError,
    raise_retired_before_ledger_open,
)


def open_ledger(*args: Any, **kwargs: Any) -> None:
    raise_retired_before_ledger_open(*args, **kwargs)


def register_frozen_cohort(*args: Any, **kwargs: Any) -> None:
    raise_retired_before_ledger_open(*args, **kwargs)


def append_cycle(*args: Any, **kwargs: Any) -> None:
    raise_retired_before_ledger_open(*args, **kwargs)


def read_plans_and_samples(*args: Any, **kwargs: Any) -> None:
    raise_retired_before_ledger_open(*args, **kwargs)


def ledger_summary(*args: Any, **kwargs: Any) -> None:
    raise_retired_before_ledger_open(*args, **kwargs)


__all__ = [
    "CONTRACT_ID",
    "ProspectiveEventResponseError",
    "ProspectiveEventResponseV2RetiredError",
    "SCHEMA_VERSION",
    "append_cycle",
    "build_event_plans",
    "build_samples",
    "collection_workset",
    "completed_candle_candidates",
    "ledger_summary",
    "load_contract",
    "mature_outcome_rows",
    "open_ledger",
    "quote_snapshot_candidates",
    "read_plans_and_samples",
    "register_frozen_cohort",
]
