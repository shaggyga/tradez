#!/usr/bin/env python3
"""Publish the engineering-blocked, fail-closed v4 after-cost proof grid.

The operational entry point has no config or manifest override.  Nonempty
input envelopes are rejected until the append-only research genealogy contains
the exact external trust anchor for the canonical manifest and its artifact
bytes.  Until real append-only producers are integrated, zero-input generation
is the only permitted engineering initialization mode.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

try:
    from forex_system.contracts.currency_state import load_contract
    from forex_system.research.currency_state_after_cost_counterfactual_v2 import _validate_manifest
    from forex_system.research.currency_state_after_cost_counterfactual_v4 import (
        EXPECTED_ARTIFACT_PATHS,
        build_after_cost_counterfactual_v4,
    )
    from forex_system.research.currency_state_response_timing_arms import build_response_timing_arms
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import load_contract
    from src.forex_system.research.currency_state_after_cost_counterfactual_v2 import _validate_manifest
    from src.forex_system.research.currency_state_after_cost_counterfactual_v4 import (
        EXPECTED_ARTIFACT_PATHS,
        build_after_cost_counterfactual_v4,
    )
    from src.forex_system.research.currency_state_response_timing_arms import build_response_timing_arms


ROOT = Path(__file__).resolve().parent
PRODUCER_INTEGRATION_ENABLED = False
CONFIG = ROOT / "config" / "currency_state_after_cost_counterfactual_v4.json"
MANIFEST = ROOT / "config" / "currency_state_after_cost_counterfactual_v4_manifest.json"
UPSTREAM_CONFIG = ROOT / "config" / "currency_state_after_cost_counterfactual_v3.json"
UPSTREAM_MANIFEST = ROOT / "config" / "currency_state_after_cost_counterfactual_v3_manifest.json"
RESPONSE_CONFIG = ROOT / "config" / "currency_state_response_timing_arms_v1.json"
STATE_CONFIG = ROOT / "config" / "currency_state_engine_v2.json"
CONTEXT = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_official_context_v1.json"
GENEALOGY = ROOT / "data" / "oanda_training_manager" / "state" / "research_genealogy_v1.sqlite"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_after_cost_counterfactual_v4.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "currency_state_after_cost_counterfactual" / "CURRENCY_STATE_AFTER_COST_COUNTERFACTUAL_V4_CURRENT.md"


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def load_optional(path: Path | None) -> Mapping[str, Any] | None:
    return None if path is None else json.loads(path.read_text(encoding="utf-8"))


def verify_manifest_files(manifest: Mapping[str, Any]) -> None:
    actual_paths = {
        label: str(row.get("relative_path") or "").replace("\\", "/")
        for label, row in (manifest.get("artifacts") or {}).items()
    }
    if actual_paths != EXPECTED_ARTIFACT_PATHS:
        raise RuntimeError("canonical v4 manifest label-to-path map mismatch")
    for label, row in manifest["artifacts"].items():
        path = ROOT / row["relative_path"]
        if not path.is_file():
            raise RuntimeError(f"manifest artifact missing: {label}: {path}")
        if file_sha256(path) != row["sha256"]:
            raise RuntimeError(f"manifest artifact hash mismatch: {label}")


def external_trust_anchor(
    contract: Mapping[str, Any], manifest: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = _validate_manifest(manifest, contract)
    return {
        "anchor_schema": "currency_state_after_cost_v4_external_trust_anchor_v1",
        "manifest_relative_path": "config/currency_state_after_cost_counterfactual_v4_manifest.json",
        "manifest_file_sha256": file_sha256(MANIFEST),
        "manifest_id": normalized["manifest_id"],
        "manifest_sha256": normalized["manifest_sha256"],
        "artifacts": normalized["artifacts"],
        "counterfactual_v4_module_sha256": normalized["artifacts"]["counterfactual_v4_module"]["sha256"],
        "counterfactual_v4_config_sha256": normalized["artifacts"]["counterfactual_v4_config"]["sha256"],
        "counterfactual_v4_cli_sha256": normalized["artifacts"]["counterfactual_v4_cli"]["sha256"],
    }


def genealogy_trust_state(
    genealogy_path: Path,
    *,
    contract: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> tuple[bool, str, dict[str, Any]]:
    expected = external_trust_anchor(contract, manifest)
    if not genealogy_path.is_file():
        return False, "append_only_genealogy_database_missing", expected
    try:
        connection = sqlite3.connect(f"file:{genealogy_path.as_posix()}?mode=ro", uri=True)
        try:
            row = connection.execute(
                "SELECT definition_json FROM experiments WHERE hypothesis_id=?",
                (contract["counterfactual_cohort_id"],),
            ).fetchone()
        finally:
            connection.close()
    except sqlite3.Error:
        return False, "append_only_genealogy_read_failed", expected
    if row is None:
        return False, "v4_append_only_genealogy_definition_missing", expected
    try:
        definition = json.loads(row[0])
    except (TypeError, ValueError, json.JSONDecodeError):
        return False, "v4_append_only_genealogy_definition_invalid", expected
    if definition.get("v4_external_trust_anchor") != expected:
        return False, "v4_append_only_genealogy_trust_anchor_mismatch", expected
    return True, "v4_append_only_genealogy_trust_anchor_exact", expected


def render(snapshot: Mapping[str, Any]) -> str:
    summary = snapshot["summary"]
    lines = [
        "# CurrencyState after-cost counterfactual v4 - current", "",
        f"Generated: `{snapshot['generated_utc']}`", "",
        "**Engineering blocked / zero evidence. This is not a production-ready producer integration.**", "",
        f"- Contract/cohort: `{snapshot['counterfactual_contract_id']}` / `{snapshot['counterfactual_cohort_id']}`",
        f"- Frozen manifest: `{snapshot['frozen_manifest']['manifest_id']}` / `{snapshot['frozen_manifest']['manifest_sha256']}`",
        f"- Trust state: `{snapshot['operational_trust_state']}`",
        f"- Producer integration: `{snapshot['producer_integration_state']}`",
        f"- Universe/grid: **{snapshot['instrument_count']}/68**, **{summary['row_count']}** rows",
        f"- Submitted/admissible economics: **{summary['submitted_envelope_count']} envelopes / {summary['economics_admissible_count']} rows**",
        f"- Ranked/selectable/hold-switch: **{summary['ranked_count']} / {summary['selectable_count']} / {summary['hold_switch_count']}**",
        f"- Supported execution decision: **{snapshot['supported_execution_decision']}**", "",
        "## Boundary", "",
        "- V3 files and evidence remain byte-for-byte preserved and are not migrated.",
        "- Any collision or constituent rejection atomically invalidates its complete submitted envelope.",
        "- Open-position rotation requires a separately bound current close quote and recomputed close spread, slippage, and latency.",
        "- V4 supports at most one open position; larger account states fail closed.",
        "- Nonempty input is blocked until exact append-only genealogy registration and real producer integration exist.",
        "- All rows remain research-only `no_trade`.", "",
        "## Blockers", "", "| Blocker | Rows |", "|---|---:|",
    ]
    lines.extend(
        f"| `{name}` | {count} |"
        for name, count in sorted(Counter(summary["blocker_counts"]).items())
    )
    return "\n".join(lines) + "\n"


def run(
    *,
    response_path: Path | None = None,
    context_path: Path = CONTEXT,
    quote_path: Path | None = None,
    verifier_path: Path | None = None,
    economics_path: Path | None = None,
    hold_path: Path | None = None,
    account_path: Path | None = None,
    genealogy_path: Path = GENEALOGY,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
) -> dict[str, Any]:
    # Canonical config and manifest paths are intentionally not parameters.
    contract = json.loads(CONFIG.read_text(encoding="utf-8"))
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    upstream_contract = json.loads(UPSTREAM_CONFIG.read_text(encoding="utf-8"))
    upstream_manifest = json.loads(UPSTREAM_MANIFEST.read_text(encoding="utf-8"))
    verify_manifest_files(manifest)
    trusted, trust_state, expected_anchor = genealogy_trust_state(
        genealogy_path, contract=contract, manifest=manifest,
    )
    input_paths = (quote_path, verifier_path, economics_path, hold_path, account_path)
    if any(path is not None for path in input_paths) and not PRODUCER_INTEGRATION_ENABLED:
        raise RuntimeError(
            "nonempty v4 input blocked: real append-only envelope producer integration is missing"
        )
    if any(path is not None for path in input_paths) and not trusted:
        raise RuntimeError(
            "nonempty v4 input blocked: exact append-only genealogy trust anchor is absent or mismatched"
        )
    if response_path is None:
        context = json.loads(context_path.read_text(encoding="utf-8"))
        response = build_response_timing_arms(
            context, state_contract=load_contract(STATE_CONFIG),
            arm_contract=json.loads(RESPONSE_CONFIG.read_text(encoding="utf-8")),
        )
    else:
        response = json.loads(response_path.read_text(encoding="utf-8"))
    snapshot = build_after_cost_counterfactual_v4(
        response, contract=contract, frozen_manifest=manifest,
        upstream_v3_contract=upstream_contract,
        upstream_v3_manifest=upstream_manifest,
        venue_quotes=load_optional(quote_path),
        verifier_evidence=load_optional(verifier_path),
        economics_inputs=load_optional(economics_path),
        hold_switch_inputs=load_optional(hold_path),
        account_state=load_optional(account_path),
    )
    if not trusted and any(snapshot["summary"].get(field, 0) for field in (
        "economics_admissible_count", "ranked_count", "selectable_count", "hold_switch_count",
    )):
        raise RuntimeError("unregistered v4 initialization produced evidence")
    snapshot["generated_utc"] = dt.datetime.now(tz=dt.timezone.utc).isoformat()
    snapshot["operational_trust_state"] = trust_state
    snapshot["expected_external_trust_anchor"] = expected_anchor
    snapshot["producer_integration_state"] = "producer_integration_missing"
    snapshot["registration_status"] = "engineering_blocked_zero_evidence"
    snapshot["research_only"] = True
    snapshot["execution_eligible"] = False
    snapshot["can_place_orders"] = False
    atomic_text(output_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_text(report_path, render(snapshot))
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("response", "quotes", "verifier", "economics", "hold", "account"):
        parser.add_argument(f"--{name}", type=Path)
    parser.add_argument("--context", type=Path, default=CONTEXT)
    parser.add_argument("--genealogy", type=Path, default=GENEALOGY)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args()
    snapshot = run(
        response_path=args.response, context_path=args.context,
        quote_path=args.quotes, verifier_path=args.verifier,
        economics_path=args.economics, hold_path=args.hold,
        account_path=args.account, genealogy_path=args.genealogy,
        output_path=args.output, report_path=args.report,
    )
    print(json.dumps({
        "snapshot_id": snapshot["snapshot_id"],
        "cohort_id": snapshot["counterfactual_cohort_id"],
        "operational_trust_state": snapshot["operational_trust_state"],
        "producer_integration_state": snapshot["producer_integration_state"],
        **{key: snapshot["summary"][key] for key in (
            "row_count", "economics_admissible_count", "ranked_count",
            "selectable_count", "hold_switch_count",
        )},
        "supported_execution_decision": snapshot["supported_execution_decision"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
