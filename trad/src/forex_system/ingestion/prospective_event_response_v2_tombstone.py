"""Fail-closed tombstone for the retired prospective-response V2 writer."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


SOURCE_ROOT = Path(__file__).resolve().parents[3]
RETIREMENT_PATH = (
    SOURCE_ROOT / "config" / "prospective_event_response_capture_v2_retirement.json"
)
RETIRED_LEDGER_PATH = (
    SOURCE_ROOT
    / "data"
    / "oanda_training_manager"
    / "research_ledgers"
    / "prospective_event_response_v2.sqlite"
)
RETIREMENT_REASON = "clock_dependency_superseded_before_any_sample"


class ProspectiveEventResponseV2RetiredError(RuntimeError):
    """Raised before any retired V2 ledger connection can be opened."""


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def verify_retirement_tombstone() -> dict[str, Any]:
    try:
        record = json.loads(RETIREMENT_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise ProspectiveEventResponseV2RetiredError(
            "prospective_event_response_v2_retirement_record_unavailable"
        ) from exc
    if not isinstance(record, Mapping):
        raise ProspectiveEventResponseV2RetiredError(
            "prospective_event_response_v2_retirement_record_invalid"
        )
    if record.get("status") != "retired_terminal" or record.get("terminal") is not True:
        raise ProspectiveEventResponseV2RetiredError(
            "prospective_event_response_v2_retirement_not_terminal"
        )
    if record.get("retirement_reason") != RETIREMENT_REASON:
        raise ProspectiveEventResponseV2RetiredError(
            "prospective_event_response_v2_retirement_reason_mismatch"
        )
    policy = record.get("policy")
    if not isinstance(policy, Mapping) or any(
        policy.get(key) is not True
        for key in ("never_resume", "never_backfill", "never_mutate_retired_ledger")
    ):
        raise ProspectiveEventResponseV2RetiredError(
            "prospective_event_response_v2_retirement_policy_invalid"
        )
    frozen = record.get("frozen_ledger")
    if not isinstance(frozen, Mapping):
        raise ProspectiveEventResponseV2RetiredError(
            "prospective_event_response_v2_frozen_ledger_missing"
        )
    if RETIRED_LEDGER_PATH.stat().st_size != int(frozen.get("byte_length") or -1):
        raise ProspectiveEventResponseV2RetiredError(
            "prospective_event_response_v2_ledger_length_changed"
        )
    if _sha256_path(RETIRED_LEDGER_PATH) != str(frozen.get("sha256") or ""):
        raise ProspectiveEventResponseV2RetiredError(
            "prospective_event_response_v2_ledger_hash_changed"
        )
    implementations = record.get("frozen_implementation_artifacts")
    if not isinstance(implementations, Mapping):
        raise ProspectiveEventResponseV2RetiredError(
            "prospective_event_response_v2_frozen_implementation_missing"
        )
    for relative, expected in implementations.items():
        path = SOURCE_ROOT / str(relative)
        if not path.is_file() or _sha256_path(path) != str(expected):
            raise ProspectiveEventResponseV2RetiredError(
                "prospective_event_response_v2_frozen_implementation_hash_changed"
            )
    return dict(record)


def raise_retired_before_ledger_open(*_: Any, **__: Any) -> None:
    """Verify frozen evidence, then fail before SQLite is imported or opened."""

    verify_retirement_tombstone()
    raise ProspectiveEventResponseV2RetiredError(
        f"prospective_event_response_v2_retired:{RETIREMENT_REASON}"
    )


__all__ = [
    "ProspectiveEventResponseV2RetiredError",
    "RETIREMENT_PATH",
    "RETIREMENT_REASON",
    "RETIRED_LEDGER_PATH",
    "raise_retired_before_ledger_open",
    "verify_retirement_tombstone",
]
