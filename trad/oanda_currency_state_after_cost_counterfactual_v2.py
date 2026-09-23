#!/usr/bin/env python3
"""Publish the v2 fail-closed after-cost research grid from local artifacts."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

try:
    from forex_system.contracts.currency_state import load_contract
    from forex_system.research.currency_state_after_cost_counterfactual_v2 import build_after_cost_counterfactual_v2
    from forex_system.research.currency_state_response_timing_arms import build_response_timing_arms
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import load_contract
    from src.forex_system.research.currency_state_after_cost_counterfactual_v2 import build_after_cost_counterfactual_v2
    from src.forex_system.research.currency_state_response_timing_arms import build_response_timing_arms


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "currency_state_after_cost_counterfactual_v2.json"
MANIFEST = ROOT / "config" / "currency_state_after_cost_counterfactual_v2_manifest.json"
RESPONSE_CONFIG = ROOT / "config" / "currency_state_response_timing_arms_v1.json"
STATE_CONFIG = ROOT / "config" / "currency_state_engine_v2.json"
CONTEXT = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_official_context_v1.json"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_after_cost_counterfactual_v2.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "currency_state_after_cost_counterfactual" / "CURRENCY_STATE_AFTER_COST_COUNTERFACTUAL_V2_CURRENT.md"


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_manifest_files(manifest: Mapping[str, Any]) -> None:
    for label, row in (manifest.get("artifacts") or {}).items():
        path = ROOT / str(row["relative_path"])
        if not path.is_file(): raise RuntimeError(f"manifest artifact missing: {label}: {path}")
        actual = file_sha256(path)
        if actual != row["sha256"]: raise RuntimeError(f"manifest artifact hash mismatch: {label}")


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8"); os.replace(temporary, path)


def render(snapshot: Mapping[str, Any]) -> str:
    summary = snapshot["summary"]; blockers = Counter(summary["blocker_counts"])
    lines = [
        "# CurrencyState after-cost counterfactual v2 - current", "",
        f"Generated: `{snapshot['generated_utc']}`", "",
        "Research-only. The v1 cohort is preserved but engineering-superseded; no lifecycle state was changed.", "",
        f"- Contract/cohort: `{snapshot['counterfactual_contract_id']}` / `{snapshot['counterfactual_cohort_id']}`",
        f"- Supersedes: `{snapshot['supersedes_contract_id']}` / `{snapshot['supersedes_cohort_id']}`",
        f"- Frozen manifest: `{snapshot['frozen_manifest']['manifest_id']}` / `{snapshot['frozen_manifest']['manifest_sha256']}`",
        f"- Response: `{snapshot['response_snapshot_id']}` / `{snapshot['response_snapshot_sha256']}`",
        f"- Universe: **{snapshot['instrument_count']}/68**, hash `{snapshot['instrument_universe_sha256']}`",
        f"- Grid rows: **{summary['row_count']}**",
        f"- Quotes/verifier/economics admissible: **{summary['admissible_quote_count']} / {summary['admissible_verifier_count']} / {summary['economics_admissible_count']}**",
        f"- Ranked/selectable/hold-switch: **{summary['ranked_count']} / {summary['selectable_count']} / {summary['hold_switch_count']}**",
        f"- Supported execution decision: **{snapshot['supported_execution_decision']}**", "", "## Blockers", "",
        "| Blocker | Rows |", "|---|---:|",
    ]
    lines.extend(f"| `{name}` | {count} |" for name, count in sorted(blockers.items()))
    lines += ["", "## Hardening", "", "- Economics binds the exact response snapshot ID/SHA, exact response-record hash, and exact forecast cutoff.", "- A separate fresh immutable OANDA Practice-007 quote envelope is mandatory; completed-minute bid/ask is not described as current executable spread.", "- Calibration/effective N must come from a verifier record bound to dataset, episode-dedup, factor-dedup, and outcome-ledger hashes.", "- Cost accounting separates current entry half-spread, expected exit half-spread, entry/exit slippage, entry/exit latency, and rotation-close cost.", "- Hold/switch additionally requires immutable Practice-007 account, position, trade, units, entry, ledger, close quote, capacity, conflict, calibration, fact-lineage, and cost evidence.", "- Missing evidence remains null/no_trade.", ""]
    return "\n".join(lines)


def load_optional(path: Path | None) -> Mapping[str, Any] | None:
    return None if path is None else json.loads(path.read_text(encoding="utf-8"))


def run(*, response_path: Path | None = None, context_path: Path = CONTEXT, config_path: Path = CONFIG, manifest_path: Path = MANIFEST, quote_path: Path | None = None, verifier_path: Path | None = None, economics_path: Path | None = None, hold_path: Path | None = None, output_path: Path = OUTPUT, report_path: Path = REPORT) -> dict[str, Any]:
    contract = json.loads(config_path.read_text(encoding="utf-8")); manifest = json.loads(manifest_path.read_text(encoding="utf-8")); verify_manifest_files(manifest)
    if response_path is None:
        context = json.loads(context_path.read_text(encoding="utf-8"))
        response = build_response_timing_arms(context, state_contract=load_contract(STATE_CONFIG), arm_contract=json.loads(RESPONSE_CONFIG.read_text(encoding="utf-8")))
    else: response = json.loads(response_path.read_text(encoding="utf-8"))
    snapshot = build_after_cost_counterfactual_v2(response, contract=contract, frozen_manifest=manifest, venue_quotes=load_optional(quote_path), verifier_evidence=load_optional(verifier_path), economics_inputs=load_optional(economics_path), hold_switch_inputs=load_optional(hold_path))
    snapshot["generated_utc"] = dt.datetime.now(tz=dt.timezone.utc).isoformat()
    atomic_text(output_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n"); atomic_text(report_path, render(snapshot)); return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("response", "context", "config", "manifest", "quotes", "verifier", "economics", "hold", "output", "report"):
        defaults = {"context": CONTEXT, "config": CONFIG, "manifest": MANIFEST, "output": OUTPUT, "report": REPORT}
        parser.add_argument(f"--{name}", type=Path, default=defaults.get(name))
    args = parser.parse_args(); snapshot = run(response_path=args.response, context_path=args.context, config_path=args.config, manifest_path=args.manifest, quote_path=args.quotes, verifier_path=args.verifier, economics_path=args.economics, hold_path=args.hold, output_path=args.output, report_path=args.report)
    print(json.dumps({"snapshot_id": snapshot["snapshot_id"], "cohort_id": snapshot["counterfactual_cohort_id"], **{key: snapshot["summary"][key] for key in ("row_count", "economics_admissible_count", "ranked_count", "selectable_count", "hold_switch_count")}, "supported_execution_decision": snapshot["supported_execution_decision"]}, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
