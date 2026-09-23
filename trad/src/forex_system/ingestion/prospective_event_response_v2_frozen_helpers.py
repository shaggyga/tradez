"""Hash-verified pure helpers from the retired response-V2 implementation.

The original implementation bytes are archived under the hash recorded in the
terminal retirement tombstone.  This compatibility surface exposes only its
pre-ledger, deterministic calculations because prospective-response V3 uses
two of them.  This module exposes no ledger function and never imports SQLite.
"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path
from typing import Any

from .prospective_event_response_v2_tombstone import (
    ProspectiveEventResponseV2RetiredError,
)


SOURCE_ROOT = Path(__file__).resolve().parents[3]
FROZEN_IMPLEMENTATION = (
    SOURCE_ROOT
    / "artifacts"
    / "retired_contracts"
    / "prospective_event_response_v2_08bfa4b0bb213cae.py.frozen"
)
FROZEN_IMPLEMENTATION_SHA256 = (
    "08bfa4b0bb213caefac910de76b05f22ca8aa5b89ca142a0eef5297e3296156d"
)
_PURE_FUNCTIONS = {
    "_parse_utc",
    "_iso",
    "_canonical_json",
    "_sha256_json",
    "_finite",
    "_strings",
    "_id",
    "load_contract",
    "_exclusion",
    "build_event_plans",
    "quote_snapshot_candidates",
    "completed_candle_candidates",
    "build_samples",
    "mature_outcome_rows",
    "collection_workset",
}


def _load_frozen_pure_surface() -> dict[str, Any]:
    source = FROZEN_IMPLEMENTATION.read_bytes()
    if hashlib.sha256(source).hexdigest() != FROZEN_IMPLEMENTATION_SHA256:
        raise ProspectiveEventResponseV2RetiredError(
            "prospective_event_response_v2_frozen_implementation_hash_changed"
        )
    tree = ast.parse(source.decode("utf-8"), filename=str(FROZEN_IMPLEMENTATION))
    body: list[ast.stmt] = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == "__future__":
            body.append(node)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [alias.name for alias in node.names]
            if "sqlite3" not in names:
                body.append(node)
        elif isinstance(node, ast.Assign):
            targets = {
                target.id for target in node.targets if isinstance(target, ast.Name)
            }
            if targets.intersection({"UTC", "SCHEMA_VERSION", "CONTRACT_ID"}):
                body.append(node)
        elif isinstance(node, ast.ClassDef) and node.name == "ProspectiveEventResponseError":
            body.append(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in _PURE_FUNCTIONS:
            body.append(node)
    namespace: dict[str, Any] = {"__name__": "_retired_v2_pure_surface"}
    exec(compile(ast.Module(body=body, type_ignores=[]), str(FROZEN_IMPLEMENTATION), "exec"), namespace)
    missing = _PURE_FUNCTIONS.difference(namespace)
    if missing or "ProspectiveEventResponseError" not in namespace:
        raise ProspectiveEventResponseV2RetiredError(
            "prospective_event_response_v2_pure_surface_incomplete"
        )
    return namespace


_PURE = _load_frozen_pure_surface()
UTC = _PURE["UTC"]
SCHEMA_VERSION = _PURE["SCHEMA_VERSION"]
CONTRACT_ID = _PURE["CONTRACT_ID"]
ProspectiveEventResponseError = _PURE["ProspectiveEventResponseError"]
load_contract = _PURE["load_contract"]
build_event_plans = _PURE["build_event_plans"]
quote_snapshot_candidates = _PURE["quote_snapshot_candidates"]
completed_candle_candidates = _PURE["completed_candle_candidates"]
build_samples = _PURE["build_samples"]
mature_outcome_rows = _PURE["mature_outcome_rows"]
collection_workset = _PURE["collection_workset"]


__all__ = [
    "CONTRACT_ID",
    "ProspectiveEventResponseError",
    "ProspectiveEventResponseV2RetiredError",
    "SCHEMA_VERSION",
    "build_event_plans",
    "build_samples",
    "collection_workset",
    "completed_candle_candidates",
    "load_contract",
    "mature_outcome_rows",
    "quote_snapshot_candidates",
]
