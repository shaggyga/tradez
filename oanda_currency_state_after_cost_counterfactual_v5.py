#!/usr/bin/env python3
"""Publish the immutable, engineering-blocked V5 after-cost proof grid.

The canonical genealogy and persistent identity-registry paths are constants,
not caller inputs.  Every nonempty operational input is blocked because no
real append-only producer integration exists.  Zero-input publication remains
research-only/no-trade and does not create or modify either registry.
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
    from forex_system.contracts.currency_state import load_contract, stable_hash
    from forex_system.research.currency_state_after_cost_counterfactual_v2 import _validate_manifest
    from forex_system.research.currency_state_after_cost_counterfactual_v5 import (
        EXPECTED_ARTIFACT_PATHS,
        build_after_cost_counterfactual_v5,
    )
    from forex_system.research.currency_state_after_cost_registrar_v5 import (
        EXPERIMENT_COLUMNS,
        canonical_json,
        registrar_definition_v5,
    )
    from forex_system.research.currency_state_response_timing_arms import build_response_timing_arms
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import load_contract, stable_hash
    from src.forex_system.research.currency_state_after_cost_counterfactual_v2 import _validate_manifest
    from src.forex_system.research.currency_state_after_cost_counterfactual_v5 import (
        EXPECTED_ARTIFACT_PATHS,
        build_after_cost_counterfactual_v5,
    )
    from src.forex_system.research.currency_state_after_cost_registrar_v5 import (
        EXPERIMENT_COLUMNS,
        canonical_json,
        registrar_definition_v5,
    )
    from src.forex_system.research.currency_state_response_timing_arms import build_response_timing_arms


ROOT = Path(__file__).resolve().parent
PRODUCER_INTEGRATION_ENABLED = False
CONFIG = ROOT / "config" / "currency_state_after_cost_counterfactual_v5.json"
MANIFEST = ROOT / "config" / "currency_state_after_cost_counterfactual_v5_manifest.json"
UPSTREAM_V4_CONFIG = ROOT / "config" / "currency_state_after_cost_counterfactual_v4.json"
UPSTREAM_V4_MANIFEST = ROOT / "config" / "currency_state_after_cost_counterfactual_v4_manifest.json"
UPSTREAM_V3_CONFIG = ROOT / "config" / "currency_state_after_cost_counterfactual_v3.json"
UPSTREAM_V3_MANIFEST = ROOT / "config" / "currency_state_after_cost_counterfactual_v3_manifest.json"
RESPONSE_CONFIG = ROOT / "config" / "currency_state_response_timing_arms_v1.json"
STATE_CONFIG = ROOT / "config" / "currency_state_engine_v2.json"
CONTEXT = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_official_context_v1.json"
# Security boundaries: neither path is accepted by run() or argparse.
GENEALOGY = ROOT / "data" / "oanda_training_manager" / "state" / "research_genealogy_v1.sqlite"
IDENTITY_REGISTRY = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_after_cost_identity_registry_v5.sqlite"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_after_cost_counterfactual_v5.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "currency_state_after_cost_counterfactual" / "CURRENCY_STATE_AFTER_COST_COUNTERFACTUAL_V5_CURRENT.md"


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
    actual = {
        label: str(row.get("relative_path") or "").replace("\\", "/")
        for label, row in (manifest.get("artifacts") or {}).items()
    }
    if actual != EXPECTED_ARTIFACT_PATHS:
        raise RuntimeError("canonical v5 manifest label-to-path map mismatch")
    root_resolved = ROOT.resolve()
    for label, row in manifest["artifacts"].items():
        path = (ROOT / row["relative_path"]).resolve()
        try:
            path.relative_to(root_resolved)
        except ValueError as exc:
            raise RuntimeError(f"manifest artifact escapes root: {label}") from exc
        if not path.is_file() or file_sha256(path) != row["sha256"]:
            raise RuntimeError(f"manifest artifact missing or hash mismatch: {label}")


def external_trust_anchor(
    contract: Mapping[str, Any], manifest: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = _validate_manifest(manifest, contract)
    return {
        "anchor_schema": "currency_state_after_cost_v5_external_trust_anchor_v1",
        "manifest_relative_path": "config/currency_state_after_cost_counterfactual_v5_manifest.json",
        "manifest_file_sha256": file_sha256(MANIFEST),
        "manifest_id": normalized["manifest_id"],
        "manifest_sha256": normalized["manifest_sha256"],
        "artifacts": normalized["artifacts"],
        "counterfactual_v5_module_sha256": normalized["artifacts"]["counterfactual_v5_module"]["sha256"],
        "counterfactual_v5_registrar_sha256": normalized["artifacts"]["counterfactual_v5_registrar"]["sha256"],
        "counterfactual_v5_config_sha256": normalized["artifacts"]["counterfactual_v5_config"]["sha256"],
        "counterfactual_v5_cli_sha256": normalized["artifacts"]["counterfactual_v5_cli"]["sha256"],
    }


def _independently_validate_stored_definition(
    stored: Mapping[str, Any], expected: Mapping[str, Any],
) -> tuple[bool, str]:
    try:
        decoded = json.loads(str(stored["definition_json"]))
    except (TypeError, ValueError, json.JSONDecodeError):
        return False, "v5_genealogy_definition_json_invalid"
    if canonical_json(decoded) != stored["definition_json"]:
        return False, "v5_genealogy_definition_json_not_canonical"
    if stable_hash(decoded) != stored["definition_sha256"]:
        return False, "v5_genealogy_definition_sha256_recompute_mismatch"
    if dict(stored) != dict(expected):
        return False, "v5_genealogy_exact_registrar_definition_mismatch"
    return True, "v5_genealogy_exact_registrar_definition_verified"


def _read_exact_genealogy_definition(
    path: Path, expected: Mapping[str, Any],
) -> tuple[bool, str]:
    """Testable read-only primitive; run() never accepts or forwards a path."""
    if not path.is_file():
        return False, "canonical_append_only_genealogy_database_missing"
    try:
        connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            actual_columns = tuple(
                row[1] for row in connection.execute("PRAGMA table_info(experiments)")
            )
            if actual_columns != EXPERIMENT_COLUMNS:
                return False, "wrong_genealogy_registry_schema"
            row = connection.execute(
                f"SELECT {','.join(EXPERIMENT_COLUMNS)} FROM experiments WHERE hypothesis_id=?",
                (expected["hypothesis_id"],),
            ).fetchone()
        finally:
            connection.close()
    except sqlite3.Error:
        return False, "canonical_append_only_genealogy_read_failed"
    if row is None:
        return False, "v5_append_only_genealogy_definition_missing"
    return _independently_validate_stored_definition(dict(row), expected)


def genealogy_trust_state(
    *, contract: Mapping[str, Any], manifest: Mapping[str, Any],
) -> tuple[bool, str, dict[str, Any], dict[str, Any]]:
    """Read only the canonical genealogy and verify the complete registrar row."""
    anchor = external_trust_anchor(contract, manifest)
    expected = registrar_definition_v5(contract=contract, external_anchor=anchor)
    if GENEALOGY.resolve() != (ROOT / contract["genealogy_contract"]["canonical_relative_path"]).resolve():
        return False, "v5_canonical_genealogy_path_constant_mismatch", anchor, expected
    trusted, state = _read_exact_genealogy_definition(GENEALOGY, expected)
    return trusted, state, anchor, expected


def render(snapshot: Mapping[str, Any]) -> str:
    summary = snapshot["summary"]
    lines = [
        "# CurrencyState after-cost counterfactual V5 - current", "",
        f"Generated: `{snapshot['generated_utc']}`", "",
        "**Engineering blocked / zero evidence. Not a producer integration.**", "",
        f"- Contract/cohort: `{snapshot['counterfactual_contract_id']}` / `{snapshot['counterfactual_cohort_id']}`",
        f"- Frozen manifest: `{snapshot['frozen_manifest']['manifest_id']}` / `{snapshot['frozen_manifest']['manifest_sha256']}`",
        f"- Genealogy trust: `{snapshot['operational_trust_state']}`",
        f"- Identity registry: `{snapshot['identity_registry_state']['state']}`",
        f"- Producer integration: `{snapshot['producer_integration_state']}`",
        f"- Grid: **{summary['row_count']}** rows; economics/ranked/selectable/hold: "
        f"**{summary['economics_admissible_count']} / {summary['ranked_count']} / "
        f"{summary['selectable_count']} / {summary['hold_switch_count']}**",
        f"- Supported execution decision: **{snapshot['supported_execution_decision']}**", "",
        "## Boundaries", "",
        "- V3 and V4 bytes/evidence are preserved and not migrated.",
        "- The genealogy path is hard-pinned and cannot be supplied by a caller.",
        "- Trust requires every stored experiment column, canonical definition JSON, and recomputed definition SHA-256 to match the registrar definition.",
        "- A future nonempty producer must bind replay, payload, and record identities in the append-only cohort registry.",
        "- The present CLI unconditionally blocks every nonempty input.",
        "- Every row is research-only `no_trade`.", "",
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
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
) -> dict[str, Any]:
    contract = json.loads(CONFIG.read_text(encoding="utf-8"))
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    upstream_v4_contract = json.loads(UPSTREAM_V4_CONFIG.read_text(encoding="utf-8"))
    upstream_v4_manifest = json.loads(UPSTREAM_V4_MANIFEST.read_text(encoding="utf-8"))
    upstream_v3_contract = json.loads(UPSTREAM_V3_CONFIG.read_text(encoding="utf-8"))
    upstream_v3_manifest = json.loads(UPSTREAM_V3_MANIFEST.read_text(encoding="utf-8"))
    verify_manifest_files(manifest)
    trusted, trust_state, anchor, expected_definition = genealogy_trust_state(
        contract=contract, manifest=manifest,
    )
    input_paths = (quote_path, verifier_path, economics_path, hold_path, account_path)
    if any(path is not None for path in input_paths):
        if not PRODUCER_INTEGRATION_ENABLED:
            raise RuntimeError(
                "nonempty v5 input blocked: real append-only producer integration is missing"
            )
        raise RuntimeError(
            "nonempty v5 input blocked: this engineering cohort can never be producer-enabled"
        )
    if response_path is None:
        context = json.loads(context_path.read_text(encoding="utf-8"))
        response = build_response_timing_arms(
            context, state_contract=load_contract(STATE_CONFIG),
            arm_contract=json.loads(RESPONSE_CONFIG.read_text(encoding="utf-8")),
        )
    else:
        response = json.loads(response_path.read_text(encoding="utf-8"))
    snapshot = build_after_cost_counterfactual_v5(
        response,
        contract=contract, frozen_manifest=manifest,
        upstream_v4_contract=upstream_v4_contract,
        upstream_v4_manifest=upstream_v4_manifest,
        upstream_v3_contract=upstream_v3_contract,
        upstream_v3_manifest=upstream_v3_manifest,
    )
    if any(snapshot["summary"].get(field, 0) for field in (
        "submitted_envelope_count", "submitted_record_count",
        "economics_admissible_count", "ranked_count", "selectable_count",
        "hold_switch_count",
    )):
        raise RuntimeError("v5 zero-input initialization produced evidence")
    snapshot["generated_utc"] = dt.datetime.now(tz=dt.timezone.utc).isoformat()
    snapshot["operational_trust_state"] = trust_state
    snapshot["expected_external_trust_anchor"] = anchor
    snapshot["expected_registrar_definition"] = expected_definition
    snapshot["canonical_genealogy_relative_path"] = contract["genealogy_contract"]["canonical_relative_path"]
    snapshot["canonical_identity_registry_relative_path"] = contract["identity_registry_contract"]["canonical_relative_path"]
    snapshot["producer_integration_state"] = "producer_integration_missing"
    snapshot["registration_status"] = "engineering_blocked_zero_evidence"
    snapshot["research_only"] = True
    snapshot["execution_eligible"] = False
    snapshot["can_place_orders"] = False
    if trusted:
        snapshot["registration_status"] = "engineering_registered_zero_evidence"
    atomic_text(output_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_text(report_path, render(snapshot))
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("response", "quotes", "verifier", "economics", "hold", "account"):
        parser.add_argument(f"--{name}", type=Path)
    parser.add_argument("--context", type=Path, default=CONTEXT)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args()
    snapshot = run(
        response_path=args.response, context_path=args.context,
        quote_path=args.quotes, verifier_path=args.verifier,
        economics_path=args.economics, hold_path=args.hold,
        account_path=args.account, output_path=args.output,
        report_path=args.report,
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
