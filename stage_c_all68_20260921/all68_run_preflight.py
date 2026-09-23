"""Read-only preflight for a new isolated all-68 offline experiment."""
from __future__ import annotations

import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CHECKPOINT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex\RECOVERY_CHECKPOINT_20260921")
OUTPUT = ROOT / "ALL68_RUN_PREFLIGHT.json"


def main() -> int:
    manifest = json.loads((CHECKPOINT / "inputs" / "INPUT_ARCHIVES.json").read_text(encoding="utf-8"))
    long = next(item for item in manifest["archives"] if item["path"] == "inputs/long_m1_68.zip")
    prior = json.loads((ROOT / "ALL68_INPUT_PREFLIGHT.json").read_text(encoding="utf-8"))
    archive_pairs = {item["instrument"] for item in long["members"]}
    pip_pairs = set(prior["pip_metadata"]["pairs"])
    guard = json.loads((ROOT / "ALL68_MODEL_ADMISSION_GUARD.json").read_text(encoding="utf-8"))
    result = {"schema": "all68_isolated_run_preflight_v1", "status": "passed_for_offline_research_only", "checks": {"archive_member_count": len(archive_pairs), "pip_pair_count": len(pip_pairs), "instrument_sets_match": archive_pairs == pip_pairs, "admission_guard_denies_existing_models": guard["admission_allowed"] is False, "execution_ready": False, "advisor_gpt_deferred": True}, "source_hashes": {"long_archive_declared_sha256": long["sha256"], "pip_metadata_sha256": prior["pip_metadata"]["sha256"]}, "next_experiment_rule": "new candidate must use fresh run id and may not overwrite retained benchmarks"}
    if not (len(archive_pairs) == 68 and archive_pairs == pip_pairs and guard["admission_allowed"] is False):
        raise RuntimeError("all68 offline preflight failed")
    temporary = OUTPUT.with_suffix(OUTPUT.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, OUTPUT)
    print("all68 offline preflight passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
