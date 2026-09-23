"""Preserve exact previously audited C-drive inputs; no data repair or execution."""
from pathlib import Path
import csv
import hashlib
import json
import stat
import zipfile
from datetime import datetime, timezone

BASE = Path(__file__).resolve().parent
AUDIT = BASE.parent / "audit_20260921/deep_audit_02"
OUT = BASE / "checkpoint/inputs"
OUT.mkdir(parents=True, exist_ok=True)


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


archives = []
specs = [
    ("HISTORY_RECONCILED_COVERAGE.csv", "path", "sha256", "long_m1_68.zip", "inputs/m1_reacquired_20260725", zipfile.ZIP_STORED),
    ("NATIVE_HISTORY_OVERLAP_COVERAGE.csv", "native_path", "native_sha256", "native_m1_68.zip", "inputs/native_candles", zipfile.ZIP_DEFLATED),
]
for table, path_key, hash_key, archive_name, target_prefix, compression in specs:
    with (AUDIT / table).open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 68 and len({row["instrument"] for row in rows}) == 68
    target = OUT / archive_name
    if target.exists():
        raise RuntimeError("Refusing to overwrite existing archive")
    members = []
    with zipfile.ZipFile(target, "x", compression=compression, compresslevel=6 if compression else None, allowZip64=True) as archive:
        for row in rows:
            source = Path(row[path_key]).resolve()
            expected_root = Path(r"C:\Users\zmoor\AppData\Local\ForexResearchData\m1_reacquired_20260725") if path_key == "path" else BASE.parent / "trad/data/oanda_training_manager/candles"
            if not source.is_relative_to(expected_root.resolve()) or source.is_symlink() or source.is_junction():
                raise RuntimeError("Input escaped explicit C source root")
            before = source.stat()
            h = hashlib.sha256()
            info = zipfile.ZipInfo(source.name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o644) << 16
            info.compress_type = compression
            with source.open("rb") as inp, archive.open(info, "w", force_zip64=True) as out:
                for block in iter(lambda: inp.read(1024 * 1024), b""):
                    h.update(block)
                    out.write(block)
            after = source.stat()
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns) or h.hexdigest() != row[hash_key]:
                raise RuntimeError(f"Previously audited input changed: {row['instrument']}")
            members.append({"path": source.name, "bytes": after.st_size, "sha256": h.hexdigest(), "instrument": row["instrument"], "original_path": str(source)})
    descriptor = {"path": "inputs/" + archive_name, "target_prefix": target_prefix, "bytes": target.stat().st_size, "sha256": sha(target), "members": members}
    archives.append(descriptor)
    print(json.dumps({"archive": archive_name, "files": len(members), "archive_bytes": descriptor["bytes"], "status": "exact_audit_hashes_match"}), flush=True)

receipt = {"schema": "forex_portable_audited_inputs_v1", "created_utc": datetime.now(timezone.utc).isoformat(), "scope": "Exact raw audited long and native histories; separate vintages, no merge or correction", "native_spread_defect_rows": 26824, "row_availability_provenance": "Unknown original arrival/revision history; see included audit", "archives": archives}
(OUT / "INPUT_ARCHIVES.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
