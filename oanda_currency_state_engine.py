#!/usr/bin/env python3
"""Build the read-only 21-currency/68-edge diagnostic snapshot.

This compatibility entrypoint performs no broker calls and has no execution or
authorization adapter.  It reads the bounded local quote-history artifact and
writes a research-only state/report pair.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

try:
    from forex_system.contracts.currency_state import load_contract
    from forex_system.features.currency_state_engine import build_currency_state_snapshot
except ModuleNotFoundError:  # Direct execution from the compatibility root.
    from src.forex_system.contracts.currency_state import load_contract
    from src.forex_system.features.currency_state_engine import (
        build_currency_state_snapshot,
    )


ROOT = Path(__file__).resolve().parent
DEFAULT_CONTRACT = ROOT / "config" / "currency_state_engine_v2.json"
DEFAULT_SOURCE_DEPTH = ROOT / "config" / "official_currency_source_depth_v1.json"
DEFAULT_HISTORY = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "market_sentiment_ticker"
    / "quote_history.json"
)
DEFAULT_OUTPUT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "currency_state_engine_v2.json"
)
DEFAULT_REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "currency_state_engine"
    / "CURRENCY_STATE_ENGINE_CURRENT.md"
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def load_source_depth_reference(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("currencies") or []
    categories = payload.get("categories") or []
    if payload.get("research_only") is not True:
        raise ValueError("official source-depth contract must remain research-only")
    if payload.get("execution_eligible") is not False:
        raise ValueError("official source-depth contract cannot be execution eligible")
    if float(payload.get("currency_strength_weight", -1.0)) != 0.0:
        raise ValueError("official source-depth contract must have zero strength weight")
    if len(rows) != 21 or len({row.get("currency") for row in rows}) != 21:
        raise ValueError("official source-depth contract must cover exactly 21 currencies")
    if not categories or any(
        not row.get(category) for row in rows for category in categories
    ):
        raise ValueError("official source-depth transport coverage is incomplete")
    return {
        "official_source_depth_path": str(path.resolve()),
        "official_source_depth_sha256": file_sha256(path),
        "official_source_depth_contract_id": payload.get("contract_id"),
        "official_source_transport_currency_count": 21,
        "official_source_transport_category_count": len(categories),
        "official_source_transport_complete": True,
        "official_source_currency_strength_weight": 0.0,
    }


def render_markdown(snapshot: Mapping[str, Any]) -> str:
    lines = [
        "# Currency State Engine - current diagnostic",
        "",
        f"Generated: `{snapshot['generated_utc']}`",
        "",
        "Research-only 21-currency reconstruction. Observed price response is not a forecast, and no edge is execution eligible.",
        "",
        f"- Snapshot: `{snapshot['snapshot_id']}`",
        f"- Contract: `{snapshot['contract_id']}`",
        f"- Knowledge cutoff: `{snapshot['decision_cutoff_utc']}`",
        f"- Completed-bar cutoff: `{snapshot['completed_bar_cutoff_utc']}`",
        f"- Universe: **{snapshot['currency_count']} currencies / {snapshot['instrument_count']} instruments**",
        f"- Official transport inventory: **{snapshot['input_refs'].get('official_source_transport_currency_count', 0)}/21 currencies across {snapshot['input_refs'].get('official_source_transport_category_count', 0)} economic categories**; predictor weight **0.0**",
        f"- Supported execution decision: **{snapshot['supported_execution_decision']}**",
        "",
        "| Horizon | Fresh pair observations | Exact-horizon rows | Active currencies | Components | Residual RMSE (bps) | Weighted equivalent N |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for horizon in sorted(snapshot["horizons"], key=int):
        payload = snapshot["horizons"][horizon]
        solver = payload["solver"]
        lines.append(
            "| {minutes}m | {observations} | {exact} | {currencies} | {components} | {residual} | {effective} |".format(
                minutes=int(horizon) // 60,
                observations=int(solver.get("observation_count") or 0),
                exact=sum(
                    row.get("exact_horizon_observation") is True
                    for row in payload["pair_edges"].values()
                ),
                currencies=len(solver.get("active_currencies") or []),
                components=len(solver.get("components") or []),
                residual=(
                    "n/a"
                    if solver.get("residual_rmse_bps") is None
                    else f"{float(solver['residual_rmse_bps']):.3f}"
                ),
                effective=(
                    "n/a"
                    if solver.get("weighted_observation_equivalent") is None
                    else f"{float(solver['weighted_observation_equivalent']):.1f}"
                ),
            )
        )
    lines.extend(["", "## Observed response extremes", ""])
    for horizon in sorted(snapshot["horizons"], key=int):
        rows = snapshot["horizons"][horizon]["currencies"]
        available = [
            row
            for row in rows.values()
            if row.get("observed_currency_return_bps") is not None
        ]
        available.sort(key=lambda row: float(row["observed_currency_return_bps"]))
        unavailable = sorted(
            row["currency"]
            for row in rows.values()
            if row.get("observed_currency_return_bps") is None
        )
        if available:
            weakest = available[0]
            strongest = available[-1]
            lines.append(
                f"- {int(horizon) // 60}m: strongest observed **{strongest['currency']} "
                f"{float(strongest['observed_currency_return_bps']):+.3f} bps**; "
                f"weakest observed **{weakest['currency']} "
                f"{float(weakest['observed_currency_return_bps']):+.3f} bps**; "
                f"unavailable: {', '.join(unavailable) if unavailable else 'none'}."
            )
        else:
            lines.append(f"- {int(horizon) // 60}m: no synchronized component available.")
    lines.extend(
        [
            "",
            "## Safety and interpretation",
            "",
            "- Every horizon emits all 21 currency rows and all 68 pair edges; missing or stale inputs remain unavailable rather than becoming zero.",
            "- Pair algebra is base state minus quote state. A leave-one-edge-out factor is retained so a pair cannot appear to confirm itself merely by entering its own currency solve.",
            "- Structured official facts, causal consensus, intraday rate repricing, semantic analogs, calibrated magnitude, and after-cost allocator value are not connected in this milestone.",
            "- Consequently, forecast means, cost-clear probabilities, expected net pips, allocator ranks, and execution eligibility are null/false for every edge.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    *,
    contract_path: Path = DEFAULT_CONTRACT,
    source_depth_path: Path = DEFAULT_SOURCE_DEPTH,
    history_path: Path = DEFAULT_HISTORY,
    output_path: Path = DEFAULT_OUTPUT,
    report_path: Path = DEFAULT_REPORT,
    decision_cutoff_utc: str | None = None,
) -> dict[str, Any]:
    contract = load_contract(contract_path)
    source_depth_refs = load_source_depth_reference(source_depth_path)
    history = json.loads(history_path.read_text(encoding="utf-8"))
    snapshot = build_currency_state_snapshot(
        history,
        contract=contract,
        decision_cutoff_utc=decision_cutoff_utc,
        input_refs={
            "quote_history_path": str(history_path.resolve()),
            "quote_history_sha256": file_sha256(history_path),
            "quote_history_generated_utc": history.get("generated_utc"),
            "contract_path": str(contract_path.resolve()),
            "contract_sha256": contract["contract_sha256"],
            **source_depth_refs,
        },
    )
    snapshot["generated_utc"] = dt.datetime.now(tz=dt.timezone.utc).isoformat()
    atomic_text(output_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_text(report_path, render_markdown(snapshot))
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--source-depth", type=Path, default=DEFAULT_SOURCE_DEPTH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--decision-cutoff-utc")
    args = parser.parse_args()
    snapshot = run(
        contract_path=args.contract,
        source_depth_path=args.source_depth,
        history_path=args.history,
        output_path=args.output,
        report_path=args.report,
        decision_cutoff_utc=args.decision_cutoff_utc,
    )
    print(
        json.dumps(
            {
                "snapshot_id": snapshot["snapshot_id"],
                "output": str(args.output),
                "report": str(args.report),
                "supported_execution_decision": snapshot["supported_execution_decision"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
