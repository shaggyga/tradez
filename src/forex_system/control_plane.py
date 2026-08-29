"""Read-only access to the canonical project layout and safety contract."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


SOURCE_ROOT = Path(__file__).resolve().parents[2]
LAYOUT_PATH = SOURCE_ROOT / "config" / "project_layout_v1.json"


def load_layout(path: Path = LAYOUT_PATH) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("broker_environment") != "practice":
        raise ValueError("project layout must remain practice-only")
    if payload.get("real_money_enabled") is not False:
        raise ValueError("real-money execution must be disabled")
    if payload.get("migration_mode") != "compatibility_first":
        raise ValueError("only compatibility-first migration is supported")
    return payload


def domain_map(path: Path = LAYOUT_PATH) -> dict[str, dict[str, Any]]:
    layout = load_layout(path)
    return {str(item["name"]): dict(item) for item in layout["domains"]}
