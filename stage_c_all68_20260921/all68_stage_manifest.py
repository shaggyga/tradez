"""Hash the Stage C implementation and primary published artifacts for handoff."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "ALL68_STAGE_MANIFEST.json"


def file_row(path: Path) -> dict:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": path.name, "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def main() -> int:
    sources = sorted([*ROOT.glob("*.py"), *ROOT.glob("*.ps1")])
    outputs = sorted([*ROOT.glob("ALL68_*.json"), *ROOT.glob("ALL68_*FORECAST_TAPE.jsonl.gz")])
    payload = {"schema": "all68_stage_manifest_v1", "scope": "Stage C source files and primary published outputs; resumable parts excluded", "source_files": [file_row(path) for path in sources], "primary_outputs": [file_row(path) for path in outputs if path.name != OUTPUT.name]}
    temporary = OUTPUT.with_suffix(OUTPUT.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, OUTPUT)
    print(f"stage manifest complete: {len(sources)} sources, {len(payload['primary_outputs'])} outputs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
