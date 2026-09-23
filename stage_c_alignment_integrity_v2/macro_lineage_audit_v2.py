"""Create a static, no-network lineage receipt for non-GPT macro inputs."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
TRAD = ROOT.parent / "trad"
REGISTRY = TRAD / "config" / "official_exact_event_clock_gap_registry_v1.json"
CONSENSUS_CONTRACT = TRAD / "docs" / "MACRO_CONSENSUS_IMPORT_CONTRACT.md"
LEDGER_SOURCE = TRAD / "oanda_macro_surprise_ledger.py"
CLOCK_AUDIT_SOURCE = TRAD / "oanda_official_exact_event_clock_gap_audit.py"
sys.path.insert(0, str(ROOT))
from publication import RunPublisher, effective_run_identity, sha256_file

REQUIRED = {"macro_lineage_audit.json"}


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode()


def inspect() -> dict[str, Any]:
    registry = json.loads(REGISTRY.read_text(encoding="utf-8"))
    currencies = registry.get("currencies")
    if registry.get("research_only") is not True or registry.get("execution_eligible") is not False or registry.get("direction_policy") != "abstain":
        raise ValueError("official_clock_registry_must_remain_direction_free")
    if not isinstance(currencies, list) or not currencies:
        raise ValueError("official_clock_currency_registry_required")
    rows = []
    for row in currencies:
        stats, policy = row.get("domestic_statistics") or {}, row.get("domestic_policy") or {}
        if stats.get("status") == "exact_minute_available" and not stats.get("source_id"):
            raise ValueError("exact_clock_requires_source_identity")
        rows.append({"currency": row.get("currency"), "statistics_status": stats.get("status"), "statistics_source_id": stats.get("source_id"),
                     "policy_status": policy.get("status"), "policy_timing_precision": policy.get("timing_precision"),
                     "linked_driver": (row.get("linked_driver") or {}).get("driver_currency")})
    contract_text = CONSENSUS_CONTRACT.read_text(encoding="utf-8")
    ledger_text = LEDGER_SOURCE.read_text(encoding="utf-8")
    required_contract_phrases = ["response_completed_utc", "provider_snapshot_sha256", "after release time", "consensus_source_unavailable"]
    if any(phrase not in contract_text for phrase in required_contract_phrases):
        raise ValueError("consensus_contract_missing_causality_guard")
    required_ledger_phrases = ["validate_consensus_projection_row", "actual_present_at_capture", "archived_snapshot_matches", "captured >= scheduled"]
    if any(phrase not in ledger_text for phrase in required_ledger_phrases):
        raise ValueError("macro_ledger_missing_causality_guard")
    return {"schema_version": "macro_lineage_audit.v2", "status": "non_gpt_macro_contracts_bound_no_live_input_claim",
            "sources": {"official_clock_registry": {"path": str(REGISTRY), "sha256": sha256_file(REGISTRY)}, "consensus_import_contract": {"path": str(CONSENSUS_CONTRACT), "sha256": sha256_file(CONSENSUS_CONTRACT)},
                        "macro_ledger_source": {"path": str(LEDGER_SOURCE), "sha256": sha256_file(LEDGER_SOURCE)}, "exact_clock_audit_source": {"path": str(CLOCK_AUDIT_SOURCE), "sha256": sha256_file(CLOCK_AUDIT_SOURCE)}},
            "official_exact_clock_rows": rows,
            "contracts_verified": ["official clock registry is research-only, execution-ineligible and abstains direction", "exact domestic clock rows carry source identities", "consensus requires trusted pre-release response-completion and archived raw snapshot", "post-release/revised/actual-present consensus is rejected from causal use"],
            "limitations": ["no source was fetched and no current event availability was asserted", "no historical consensus archive was accepted by this audit", "numeric surprise remains unavailable until a qualifying pre-release capture exists", "GPT/advisor comparisons are deferred"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    audit = inspect()
    identity = effective_run_identity(contract={"schema": "macro_lineage_binding.v2", "mode": "static_no_network", "required_payloads": sorted(REQUIRED)}, dependency_hashes={"registry": sha256_file(REGISTRY), "consensus_contract": sha256_file(CONSENSUS_CONTRACT), "ledger_source": sha256_file(LEDGER_SOURCE), "clock_audit_source": sha256_file(CLOCK_AUDIT_SOURCE), "audit_source": sha256_file(Path(__file__))})
    publisher = RunPublisher(ROOT / "runs", args.run_id, identity)
    publisher.acquire()
    try:
        audit["run_identity_fingerprint"] = identity["fingerprint"]
        publisher.complete([publisher.write_or_validate_payload("macro_lineage_audit.json", canonical_bytes(audit))], REQUIRED)
    except Exception:
        publisher.release()
        raise
    print(f"bound {len(audit['official_exact_clock_rows'])} official-clock currency rows: {args.run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
