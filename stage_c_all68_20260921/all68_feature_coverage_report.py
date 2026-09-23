"""Report matched feature-and-target eligibility from persisted baseline parts."""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent
PARTS = ROOT / "calendar_baseline_v1_parts"
OUTPUT = ROOT / "ALL68_FEATURE_TARGET_ELIGIBILITY.json"


def main() -> int:
    rows = []
    for path in sorted(PARTS.glob("*.npz")):
        if ".tmp." in path.name:
            continue
        with np.load(path) as part:
            counts = {}
            for days in (1, 2, 5):
                prefix = f"d{days}_"
                counts[str(days)] = {"matured_train_feature_target_rows": int(len(part[prefix + "train_y"])), "held_forward_feature_target_rows": int(len(part[prefix + "test_y"]))}
            rows.append({"instrument": path.stem, "eligibility_by_trading_days": counts})
    if len(rows) != 68:
        raise RuntimeError(f"expected 68 final parts, found {len(rows)}")
    payload = {"schema": "all68_feature_target_eligibility_v1", "status": "completed_matched_eligibility_report", "scope": "rows with all five features present and endpoint target available; not raw data coverage", "instrument_count": len(rows), "per_instrument": rows}
    temporary = OUTPUT.with_suffix(OUTPUT.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, OUTPUT)
    print("feature-target eligibility report complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
