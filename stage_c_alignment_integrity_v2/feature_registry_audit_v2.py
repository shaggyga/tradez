"""Bind the retained rolling technical registry to its current pure generator."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parent
TRAD = WORKSPACE / "trad"
SOURCE = TRAD / "oanda_rolling_technical_features_v1.py"
PANEL_SOURCE = TRAD / "oanda_rolling_technical_panel_v1.py"
STORED_REGISTRY = TRAD / "docs" / "validation" / "rolling_training_windows_20260915" / "FEATURE_REGISTRY.json"
sys.path.insert(0, str(ROOT))
from publication import RunPublisher, effective_run_identity, sha256_file

REQUIRED = {"feature_registry.json", "feature_registry_audit.json"}


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode()


def load_kernel(path: Path) -> Any:
    spec = importlib.util.spec_from_file_location("rolling_technical_features_v1_audit", path)
    if spec is None or spec.loader is None:
        raise ValueError("rolling_kernel_not_loadable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def inspect_registry(source: Path = SOURCE, stored_registry: Path = STORED_REGISTRY) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    kernel = load_kernel(source)
    panel = load_kernel(PANEL_SOURCE)
    generated = kernel.feature_registry()
    stored = json.loads(stored_registry.read_text(encoding="utf-8"))
    if not isinstance(generated, list) or not isinstance(stored, list) or not generated:
        raise ValueError("nonempty_registry_lists_required")
    names = [row.get("name") for row in generated]
    required_fields = {"name", "family", "unit", "lookback_bars", "lookback_seconds", "availability", "gap_policy", "missing_policy", "model_input"}
    if len(names) != len(set(names)) or any(not isinstance(name, str) or not name.startswith("m1__") for name in names):
        raise ValueError("unique_canonical_m1_feature_names_required")
    if any(required_fields.difference(row) for row in generated):
        raise ValueError("feature_timing_metadata_missing")
    if any(row["availability"] != "last_required_completed_bar_end" or row["missing_policy"] != "NaN; no imputation" for row in generated):
        raise ValueError("feature_causality_or_missingness_contract_changed")
    if any(type(row["lookback_bars"]) is not int or row["lookback_bars"] < 1 or row["lookback_seconds"] != row["lookback_bars"] * 60 for row in generated):
        raise ValueError("feature_lookback_contract_invalid")
    stored_technical = [row for row in stored if isinstance(row.get("name"), str) and row["name"].startswith("m1__")]
    stored_peer = [row for row in stored if isinstance(row.get("name"), str) and row["name"].startswith("peer__")]
    if len(stored_technical) + len(stored_peer) != len(stored) or generated != stored_technical:
        raise ValueError("stored_technical_feature_registry_drifted_from_current_kernel")
    peer_required = {"name", "family", "unit", "lookback_bars", "clock", "horizon_minutes", "missing_policy", "model_input", "source_feature", "self_pair_excluded", "minimum_distinct_peer_pairs_per_leg"}
    if not stored_peer or any(peer_required.difference(row) or row["missing_policy"] != "None; no imputation or horizon substitution" for row in stored_peer):
        raise ValueError("peer_registry_entries_are_not_explicit")
    if stored_peer != panel.panel_registry():
        raise ValueError("stored_peer_feature_registry_drifted_from_current_panel")
    metadata = kernel.schema_metadata()
    if metadata["feature_count"] != len(generated) or metadata["max_lookback_bars"] != max(row["lookback_bars"] for row in generated):
        raise ValueError("kernel_metadata_disagrees_with_registry")
    audit = {"schema_version": "feature_registry_audit.v2", "status": "bound_technical_kernel_peer_population_pending",
             "source": {"path": str(source), "sha256": sha256_file(source)}, "peer_panel_source": {"path": str(PANEL_SOURCE), "sha256": sha256_file(PANEL_SOURCE)},
             "stored_registry": {"path": str(stored_registry), "sha256": sha256_file(stored_registry)},
             "kernel_metadata": metadata, "counts": {"stored_features": len(stored), "technical_kernel_features": len(generated), "peer_registry_features": len(stored_peer),
             "technical_families": dict(sorted(Counter(row["family"] for row in generated).items())), "model_inputs": sum(row["model_input"] is True for row in stored)},
             "contracts_verified": ["unique canonical M1 names", "completed-bar availability", "exact elapsed lookback seconds", "NaN/no-imputation missingness", "216 stored technical rows equal current pure kernel", "12 stored peer rows equal current exact-clock peer panel"],
             "limitations": ["peer availability still depends on contemporaneous constituent coverage", "macro feature population is not established", "source does not establish historical first-known availability", "registry binding is not model-fit or forecast evidence"]}
    return stored, audit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    generated, audit = inspect_registry()
    identity = effective_run_identity(contract={"schema": "feature_registry_binding.v2", "kind": "technical_m1_and_exact_clock_peer", "required_payloads": sorted(REQUIRED)}, dependency_hashes={"kernel_source": sha256_file(SOURCE), "peer_panel_source": sha256_file(PANEL_SOURCE), "stored_registry": sha256_file(STORED_REGISTRY), "audit_source": sha256_file(Path(__file__))})
    publisher = RunPublisher(ROOT / "runs", args.run_id, identity)
    publisher.acquire()
    try:
        audit["run_identity_fingerprint"] = identity["fingerprint"]
        payloads = [publisher.write_or_validate_payload("feature_registry.json", canonical_bytes(generated)), publisher.write_or_validate_payload("feature_registry_audit.json", canonical_bytes(audit))]
        publisher.complete(payloads, REQUIRED)
    except Exception:
        publisher.release()
        raise
    print(f"bound {audit['counts']['technical_kernel_features']} technical and {audit['counts']['peer_registry_features']} peer registry features: {args.run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
