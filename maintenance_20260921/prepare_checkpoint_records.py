"""Prepare portable audit, environment and explicitly selected source supplements."""
from pathlib import Path
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import shutil
import stat
import sys
import zipfile
from datetime import datetime, timezone

BASE = Path(__file__).resolve().parent
ROOT = BASE.parent / "trad"
OUT = BASE / "checkpoint"
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))
from tools.vault_worktree_snapshot import audit_payload, excluded_reason, regular_file


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


copied = []
for source in sorted((BASE.parent / "audit_20260921").rglob("*")):
    if not source.is_file():
        continue
    rel = source.relative_to(BASE.parent / "audit_20260921")
    if source.is_symlink() or source.is_junction():
        raise RuntimeError("Audit links prohibited")
    if source.suffix.lower() in {".pyc", ".pyo"}:
        continue
    raw = source.read_bytes()
    if source.suffix.lower() in {".json", ".md", ".py", ".txt", ".csv", ".xml", ".log"}:
        audit_payload(rel.as_posix(), raw, set())
    target = OUT / "audit_20260921" / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(raw)
    copied.append({"path": target.relative_to(OUT).as_posix(), "bytes": len(raw), "sha256": sha(raw), "original_path": str(source)})
write_json(OUT / "audit_20260921/COPY_RECEIPT.json", {"files": copied, "scope": "Exact original initial/deep audit artifacts; bytecode omitted; original manifests retain their original machine paths as provenance"})

env = OUT / "environment"
env.mkdir(exist_ok=True)
packages = sorted({(d.metadata["Name"], d.version) for d in importlib.metadata.distributions() if d.metadata.get("Name")})
write_json(env / "ENVIRONMENT.json", {"observed_utc": datetime.now(timezone.utc).isoformat(), "executable_provenance": sys.executable, "python": sys.version, "platform": sys.platform, "packages": [{"name": n, "version": v} for n, v in packages], "limits": "Observed distribution metadata only; no wheel archive or hash lock, no fresh environment installation, no claim all package versions are publicly available or portable."})
(env / "requirements-observed.txt").write_text("# Observed existing Windows Python3.12 environment; review before a separate isolated install.\n" + "\n".join(f"{n}=={v}" for n, v in packages) + "\n", encoding="utf-8")

supplement_paths = []
for filename in ("audit_mandatory_vault.py", "config.json", "feature_forecast_archive.py", "mandatory_model_validation_worker.py", "model_library_catalogue.py", "README.md", "run_pipeline.py", "sarima_sweep_worker.py"):
    supplement_paths.append((ROOT / "fresh_m1_intrahour" / filename, "trad/fresh_m1_intrahour/" + filename))
supplement_paths.append((BASE.parent / "direction_decision_20260911/src/signed_cost_models_v1.py", "direction_decision_20260911/src/signed_cost_models_v1.py"))
members = []
supp = OUT / "supplements"
supp.mkdir(exist_ok=True)
zip_path = supp / "selected_ignored_source.zip"
with zipfile.ZipFile(zip_path, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
    for source, member in supplement_paths:
        regular_file(BASE.parent.resolve(), source.relative_to(BASE.parent).as_posix())
        if excluded_reason(member):
            raise RuntimeError(f"Supplement excluded by existing source rules: {member}")
        before = source.stat()
        raw = source.read_bytes()
        audit_payload(member, raw, set())
        if source.suffix == ".py":
            compile(raw, member, "exec", dont_inherit=True)
        after = source.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise RuntimeError("Supplement changed during read")
        info = zipfile.ZipInfo(member, date_time=(1980, 1, 1, 0, 0, 0))
        info.create_system = 3
        info.external_attr = (stat.S_IFREG | 0o644) << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        archive.writestr(info, raw)
        members.append({"path": member, "bytes": len(raw), "sha256": sha(raw), "original_path": str(source)})
write_json(supp / "SUPPLEMENT_ARCHIVE.json", {"path": "supplements/selected_ignored_source.zip", "target_prefix": "", "bytes": zip_path.stat().st_size, "sha256": sha(zip_path.read_bytes()), "members": members, "scope": "Nine explicitly selected source/config/docs omitted by Git/default exporter; not all ignored artifacts or historical source versions"})
print(json.dumps({"audit_files_copied": len(copied), "environment_packages": len(packages), "supplement_files": len(members)}))
