#!/usr/bin/env python3
"""Frozen incumbent policy used by the practice-007 signal executor."""

from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


class FrozenExecutionPolicy:
    """Read-only account guard; it can downweight or veto but never promote."""

    def __init__(self, state_path: Path, *, refresh_sec: float = 15.0) -> None:
        self.state_path = Path(state_path)
        self.refresh_sec = max(5.0, float(refresh_sec))
        self.state: dict[str, Any] = {}
        self.mtime_ns = -1
        self.last_check_monotonic = -math.inf
        self.refresh(force=True)

    def refresh(self, *, force: bool = False) -> dict[str, Any]:
        now = time.monotonic()
        if not force and now - self.last_check_monotonic < self.refresh_sec:
            return self.state
        self.last_check_monotonic = now
        try:
            stat = self.state_path.stat()
            if not force and stat.st_mtime_ns == self.mtime_ns:
                return self.state
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return self.state
        if isinstance(payload, dict):
            self.state = payload
            self.mtime_ns = stat.st_mtime_ns
        return self.state

    @property
    def active(self) -> bool:
        self.refresh()
        return bool(
            self.state.get("status") == "active"
            and self.state.get("account_scope") == "practice_007_only"
        )

    @property
    def policy_id(self) -> str:
        self.refresh()
        return str(self.state.get("policy_id") or "")

    @staticmethod
    def _cell_keys(row: dict[str, Any], horizon_sec: int) -> tuple[str, ...]:
        instrument = str(row.get("instrument") or "")
        model = str(row.get("model_id") or row.get("lane_id") or "unknown")
        timeframe = str(row.get("input_timeframe") or "unknown")
        horizon = max(0, int(horizon_sec))
        return (
            f"{instrument}|{model}|{timeframe}|{horizon}",
            f"{instrument}|*|{timeframe}|{horizon}",
            f"*|{model}|{timeframe}|{horizon}",
            f"{instrument}|{model}|*|{horizon}",
        )

    def evidence(self, row: dict[str, Any], horizon_sec: int) -> dict[str, Any]:
        self.refresh()
        cells = self.state.get("cells") or {}
        for key in self._cell_keys(row, horizon_sec):
            selected = cells.get(key)
            if isinstance(selected, dict):
                return {**selected, "cell_key": key}
        return {}

    def apply_ranked_candidate(self, row: dict[str, Any]) -> None:
        """Apply frozen cell evidence to every horizon point in-place."""

        if not self.active:
            return
        gates = self.state.get("gates") or {}
        minimum_samples = max(1, int(_finite(gates.get("negative_veto_min_samples"), 30)))
        minimum_ratio = max(1.0, _finite(gates.get("minimum_gross_to_spread"), 1.15))
        curve = row.get("signal_horizon_curve") or []
        points = curve if isinstance(curve, list) and curve else [row]
        selected_horizon = int(_finite(row.get("execution_horizon_sec")))
        for point in points:
            if not isinstance(point, dict):
                continue
            horizon = int(_finite(point.get("horizon_sec"), selected_horizon))
            synthetic = {**row, **point}
            evidence = self.evidence(synthetic, horizon)
            blockers = list(point.get("signal_blocked_by") or [])
            if (
                evidence.get("negative_evidence")
                and int(_finite(evidence.get("sample_count"))) >= minimum_samples
            ):
                blockers.append("frozen_policy_negative_cell")
            ratio = point.get("gross_to_spread")
            if ratio is not None and _finite(ratio) < minimum_ratio:
                blockers.append("frozen_policy_spread_buffer")
            if blockers:
                point["signal_blocked_by"] = sorted(set(blockers))
                point["signal_eligible"] = False
            evidence_strength = max(
                0.0,
                min(1.0, _finite(evidence.get("evidence_strength"))),
            )
            if evidence.get("eligible"):
                point["matrix_input_weight"] = round(
                    max(
                        0.05,
                        _finite(point.get("matrix_input_weight"), 1.0)
                        * (0.60 + 0.40 * evidence_strength),
                    ),
                    8,
                )
            elif evidence:
                point["matrix_input_weight"] = round(
                    max(
                        0.05,
                        _finite(point.get("matrix_input_weight"), 1.0) * 0.50,
                    ),
                    8,
                )
            point["frozen_policy_evidence"] = evidence
            point["execution_policy_id"] = self.policy_id
            if horizon == selected_horizon and point is not row:
                for key in (
                    "signal_eligible",
                    "signal_blocked_by",
                    "matrix_input_weight",
                    "frozen_policy_evidence",
                    "execution_policy_id",
                ):
                    if key in point:
                        row[key] = point[key]

    def exit_policy(
        self,
        *,
        family: str,
        horizon_sec: int,
    ) -> dict[str, Any]:
        if not self.active:
            return {}
        horizon = (self.state.get("exit_policies") or {}).get(str(int(horizon_sec))) or {}
        family_policy = (horizon.get("families") or {}).get(str(family))
        if isinstance(family_policy, dict) and family_policy.get("eligible"):
            return family_policy
        overall = horizon.get("overall") or {}
        return overall if isinstance(overall, dict) and overall.get("eligible") else {}

    def summary(self) -> dict[str, Any]:
        self.refresh()
        return {
            "state": str(self.state_path.resolve()),
            "active": self.active,
            "policy_id": self.policy_id,
            "status": self.state.get("status") or "missing",
            "effective_utc": self.state.get("effective_utc"),
            "frozen_until_utc": self.state.get("frozen_until_utc"),
            "cell_count": len(self.state.get("cells") or {}),
            "exit_horizon_count": len(self.state.get("exit_policies") or {}),
        }


__all__ = ["FrozenExecutionPolicy"]
