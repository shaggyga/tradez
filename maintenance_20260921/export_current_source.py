"""Use the existing reviewed exporter, with no private credential-file reads."""
import importlib.util
import json
import os
import sys
from pathlib import Path

ROOT = Path(r"C:\Users\zmoor\Documents\forex\trad")
OUT = Path(__file__).resolve().parent
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location("reviewed_worktree_exporter", ROOT / "tools/vault_worktree_snapshot.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
try:
    result = module.sync_worktree_snapshot(ROOT, OUT / "checkpoint", private_files=[])
    result["credential_scan_scope"] = "Existing source-pattern rules; private credential files were not opened or compared."
    (OUT / "SOURCE_EXPORT_RECEIPT.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["verification"]["status"], "files": result["verification"]["files_verified"], "compiled": result["verification"]["python_files_compiled"], "snapshot_id": result["snapshot_id"], "dirty": result["worktree_dirty"]}))
except Exception as exc:
    # Exporter messages contain filenames/rule identifiers, never suspected values.
    print(json.dumps({"status": "failed", "type": type(exc).__name__, "message": str(exc)}))
    raise SystemExit(1)
