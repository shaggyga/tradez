#!/usr/bin/env python3
"""Run bounded, shadow-only qualifications for every modern model-gap family."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

import oanda_cross_pair_graph_adapter as graph
import oanda_decision_model_adapters as decision
import oanda_model_gap_registry as registry
import oanda_model_gap_runtime_profiles as runtime_profiles
import oanda_neuralforecast_family_adapters as neural
import oanda_representation_learning_adapters as representation
import oanda_state_space_adapters as state_space


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _capture(model: str, function: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return function()
    except Exception as exc:
        return {
            "model": model,
            "status": "qualification_failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "account_wired": False,
            "production_eligible": False,
        }


def decision_fixture(rows: int = 64) -> tuple[np.ndarray, np.ndarray]:
    time = np.arange(rows, dtype=np.float32)
    contexts = np.column_stack(
        [np.sin(time / 5), np.cos(time / 9), np.sin(time / 17), time / rows]
    ).astype(np.float32)
    directional = 0.35 * contexts[:, 0] + 0.15 * contexts[:, 1]
    cost = 0.04
    action_rewards = np.column_stack(
        [-directional - cost, np.zeros(rows), directional - cost]
    ).astype(np.float32)
    return contexts, action_rewards


def graph_fixture(points: int = 48) -> pd.DataFrame:
    times = pd.date_range("2026-01-01", periods=points, freq="min", tz="UTC")
    rows: list[dict[str, Any]] = []
    instruments = ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_CAD")
    for index, instrument in enumerate(instruments):
        values = np.sin(np.arange(points) / 4 + index * 0.25)
        rows.extend(
            {"timestamp_utc": time, "instrument": instrument, "return_pips": float(value)}
            for time, value in zip(times, values)
        )
    return pd.DataFrame(rows)


def qualify_graph() -> dict[str, Any]:
    frame = graph_fixture()
    cutoff = pd.Timestamp("2026-01-01T00:40:00Z")
    snapshot = graph.build_lagged_graph(
        frame,
        cutoff,
        min_observations=16,
        min_abs_correlation=0.1,
    )
    adjacency = graph.adjacency_matrix(snapshot)
    return {
        "model": "temporal_cross_pair_gnn",
        "status": "synthetic_qualified",
        "nodes": len(snapshot.instruments),
        "edges": len(snapshot.edges),
        "source_max_utc": snapshot.source_max_utc,
        "as_of_utc": snapshot.as_of_utc,
        "strictly_lagged": pd.Timestamp(snapshot.source_max_utc) < cutoff,
        "adjacency_finite": bool(np.isfinite(adjacency).all()),
        "runtime": "Nixtla StemGNN plus audited lag-only graph builder",
        "account_wired": False,
        "production_eligible": False,
    }


def qualify_decision_models() -> list[dict[str, Any]]:
    contexts, rewards = decision_fixture()
    return [
        _capture(
            "contextual_bandit",
            lambda: decision.qualify_contextual_bandit(contexts, rewards),
        ),
        *(
            _capture(
                name,
                lambda name=name: decision.qualify_sb3_agent(
                    name,
                    contexts,
                    rewards,
                    total_timesteps=32,
                ),
            )
            for name in ("ppo", "sac", "dqn")
        ),
    ]


def state_space_status() -> list[dict[str, Any]]:
    try:
        import torch

        s4 = state_space.S4OfficialAdapter()
        layer = s4.build_layer(4)
        layer.eval()
        with torch.no_grad():
            output = layer(torch.randn(2, 4, 16))
        values = output[0] if isinstance(output, tuple) else output
        if not torch.isfinite(values).all():
            raise RuntimeError("official S4 layer returned non-finite values")
        s4_status = {
            "model": "s4",
            "status": "synthetic_qualified",
            "evidence_level": "synthetic_qualified",
            "checkout": str(s4.checkout),
            "output_shape": list(values.shape),
            "account_wired": False,
            "production_eligible": False,
        }
    except Exception as exc:
        s4_status = {
            "model": "s4",
            "status": "blocked",
            "evidence_level": "adapter_implemented",
            "reason": str(exc),
            "account_wired": False,
            "production_eligible": False,
        }
    mamba_reason = state_space.MambaOfficialAdapter.runtime_blocker()
    return [
        s4_status,
        {
            "model": "mamba",
            "status": "blocked" if mamba_reason else "runtime_available",
            "evidence_level": "adapter_implemented" if mamba_reason else "runtime_available",
            "reason": mamba_reason,
            "account_wired": False,
            "production_eligible": False,
        },
    ]


def run_all() -> dict[str, Any]:
    registry_report = registry.build_probe_report()
    runtime_report = runtime_profiles.build_report()
    neural_report = neural.run_qualification(
        neural.ALL_MODEL_NAMES,
        neural.synthetic_series(),
        horizon=2,
        input_size=12,
        max_steps=1,
        source="deterministic_synthetic_series_v1",
    )
    representation_report = representation.synthetic_qualification()
    graph_result = _capture("temporal_cross_pair_gnn", qualify_graph)
    decisions = qualify_decision_models()
    states = state_space_status()
    results = [
        *neural_report["results"],
        graph_result,
        *decisions,
        *representation_report["results"],
        *states,
    ]
    return {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "scope": "bounded synthetic adapter qualification",
        "execution_policy": "shadow_only_no_account_wiring_no_implicit_weight_download",
        "registry": registry_report,
        "runtime_profiles": runtime_report,
        "results": results,
        "summary": {
            "results": len(results),
            "synthetic_qualified": sum(row["status"] == "synthetic_qualified" for row in results),
            "runtime_available": sum(row["status"] == "runtime_available" for row in results),
            "blocked": sum(row["status"] == "blocked" for row in results),
            "failed": sum(row["status"] == "qualification_failed" for row in results),
            "account_wired": sum(bool(row.get("account_wired")) for row in results),
            "production_eligible": sum(bool(row.get("production_eligible")) for row in results),
            "runtime_profiles_available": runtime_report["summary"]["runtime_available"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = run_all()
    payload = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(args.output.suffix + f".{os.getpid()}.tmp")
        temporary.write_text(payload + "\n", encoding="utf-8")
        os.replace(temporary, args.output)
    print(payload)
    return 0 if report["summary"]["failed"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
