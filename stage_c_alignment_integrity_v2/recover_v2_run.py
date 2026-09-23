"""Package and relocate-verify one completed v2 run without changing its outputs."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from publication import package_completed_run, restore_completed_run, verify_completed_run


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--restore-to", type=Path, required=True)
    args = parser.parse_args()
    source = ROOT / "runs" / args.run_id
    identity = json.loads((source / "RUN_IDENTITY.json").read_text(encoding="utf-8"))
    verify_completed_run(source, identity)
    args.package.parent.mkdir(parents=True, exist_ok=True)
    package = package_completed_run(source, args.package)
    restored = restore_completed_run(args.package, args.restore_to, identity)
    print(json.dumps({"schema_version": "all68_recovery_receipt.v2", "source": str(source), "package": package,
                      "restored_to": str(args.restore_to), "restored_payload_count": len(restored["payloads"]),
                      "run_identity_fingerprint": identity["fingerprint"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
