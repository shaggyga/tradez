#!/usr/bin/env python3
"""Publish the immutable, fail-closed v3 after-cost research grid."""

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
    from forex_system.research.currency_state_after_cost_counterfactual_v3 import build_after_cost_counterfactual_v3
    from forex_system.research.currency_state_response_timing_arms import build_response_timing_arms
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import load_contract
    from src.forex_system.research.currency_state_after_cost_counterfactual_v3 import build_after_cost_counterfactual_v3
    from src.forex_system.research.currency_state_response_timing_arms import build_response_timing_arms


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "currency_state_after_cost_counterfactual_v3.json"
MANIFEST = ROOT / "config" / "currency_state_after_cost_counterfactual_v3_manifest.json"
RESPONSE_CONFIG = ROOT / "config" / "currency_state_response_timing_arms_v1.json"
STATE_CONFIG = ROOT / "config" / "currency_state_engine_v2.json"
CONTEXT = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_official_context_v1.json"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_after_cost_counterfactual_v3.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "currency_state_after_cost_counterfactual" / "CURRENCY_STATE_AFTER_COST_COUNTERFACTUAL_V3_CURRENT.md"


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_manifest_files(manifest: Mapping[str, Any]) -> None:
    for label, row in (manifest.get("artifacts") or {}).items():
        path = ROOT / str(row["relative_path"])
        if not path.is_file():
            raise RuntimeError(f"manifest artifact missing: {label}: {path}")
        if file_sha256(path) != row["sha256"]:
            raise RuntimeError(f"manifest artifact hash mismatch: {label}")


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def load_optional(path: Path | None) -> Mapping[str, Any] | None:
    return None if path is None else json.loads(path.read_text(encoding="utf-8"))


def render(snapshot: Mapping[str, Any]) -> str:
    summary = snapshot["summary"]
    blockers = Counter(summary["blocker_counts"])
    lines = [
        "# CurrencyState after-cost counterfactual v3 - current", "",
        f"Generated: `{snapshot['generated_utc']}`", "",
        "Research-only. v1 and v2 are preserved as zero-evidence engineering-superseded cohorts; no lifecycle or execution state was changed.", "",
        f"- Contract/cohort: `{snapshot['counterfactual_contract_id']}` / `{snapshot['counterfactual_cohort_id']}`",
        f"- Supersedes: `{snapshot['supersedes_contract_id']}` / `{snapshot['supersedes_cohort_id']}`",
        f"- Frozen manifest: `{snapshot['frozen_manifest']['manifest_id']}` / `{snapshot['frozen_manifest']['manifest_sha256']}`",
        f"- Response: `{snapshot['response_snapshot_id']}` / `{snapshot['response_snapshot_sha256']}`",
        f"- Universe/grid: **{snapshot['instrument_count']}/68**, **{summary['row_count']}** rows",
        f"- Admissible quotes/verifiers/economics/account records: **{summary['admissible_quote_count']} / {summary['admissible_verifier_count']} / {summary['economics_admissible_count']} / {summary['admissible_account_state_record_count']}**",
        f"- Ranked/selectable/hold-switch: **{summary['ranked_count']} / {summary['selectable_count']} / {summary['hold_switch_count']}**",
        f"- Strict input rejections: **{summary['strict_input_rejection_count']}**",
        f"- Supported execution decision: **{snapshot['supported_execution_decision']}**", "",
        "## Blockers", "", "| Blocker | Rows |", "|---|---:|",
    ]
    lines.extend(f"| `{name}` | {count} |" for name, count in sorted(blockers.items()))
    lines += [
        "", "## v3 boundary", "",
        "- Every supplied record and full envelope is consumer-rehashed with canonical sorted JSON.",
        "- Producer contract, cohort, module identity, and module bytes are frozen by the manifest.",
        "- Verifier evidence is exact arm/instrument/horizon/direction/response/forecast/model bound and single-use.",
        "- Forecast-time economics must be evaluated after the exact executable quote and verifier response binding exist.",
        "- Flat and rotation economics bind recomputed Practice-007 account, capacity, conflict, position, and ledger records.",
        "- Hold direction must agree with signed units, position, response, verifier, and the canonical forecast record.",
        "- Canonical admissible record/envelope hashes are retained in rows, allocations, and hold/switch comparisons.",
        "- Missing or rejected evidence remains null and no_trade.", "",
    ]
    return "\n".join(lines)


def run(
    *, response_path: Path | None = None, context_path: Path = CONTEXT,
    config_path: Path = CONFIG, manifest_path: Path = MANIFEST,
    quote_path: Path | None = None, verifier_path: Path | None = None,
    economics_path: Path | None = None, hold_path: Path | None = None,
    account_path: Path | None = None, output_path: Path = OUTPUT,
    report_path: Path = REPORT,
) -> dict[str, Any]:
    contract = json.loads(config_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    verify_manifest_files(manifest)
    if response_path is None:
        context = json.loads(context_path.read_text(encoding="utf-8"))
        response = build_response_timing_arms(
            context, state_contract=load_contract(STATE_CONFIG),
            arm_contract=json.loads(RESPONSE_CONFIG.read_text(encoding="utf-8")),
        )
    else:
        response = json.loads(response_path.read_text(encoding="utf-8"))
    snapshot = build_after_cost_counterfactual_v3(
        response, contract=contract, frozen_manifest=manifest,
        venue_quotes=load_optional(quote_path),
        verifier_evidence=load_optional(verifier_path),
        economics_inputs=load_optional(economics_path),
        hold_switch_inputs=load_optional(hold_path),
        account_state=load_optional(account_path),
    )
    snapshot["generated_utc"] = dt.datetime.now(tz=dt.timezone.utc).isoformat()
    atomic_text(output_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_text(report_path, render(snapshot))
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    defaults = {
        "context": CONTEXT, "config": CONFIG, "manifest": MANIFEST,
        "output": OUTPUT, "report": REPORT,
    }
    for name in (
        "response", "context", "config", "manifest", "quotes", "verifier",
        "economics", "hold", "account", "output", "report",
    ):
        parser.add_argument(f"--{name}", type=Path, default=defaults.get(name))
    args = parser.parse_args()
    snapshot = run(
        response_path=args.response, context_path=args.context,
        config_path=args.config, manifest_path=args.manifest,
        quote_path=args.quotes, verifier_path=args.verifier,
        economics_path=args.economics, hold_path=args.hold,
        account_path=args.account, output_path=args.output,
        report_path=args.report,
    )
    print(json.dumps({
        "snapshot_id": snapshot["snapshot_id"],
        "cohort_id": snapshot["counterfactual_cohort_id"],
        **{key: snapshot["summary"][key] for key in (
            "row_count", "economics_admissible_count", "ranked_count",
            "selectable_count", "hold_switch_count",
        )},
        "supported_execution_decision": snapshot["supported_execution_decision"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
