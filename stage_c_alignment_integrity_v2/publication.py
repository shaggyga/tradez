"""Single-writer publication, identity validation and compact fixture recovery."""
from __future__ import annotations

import hashlib
import ctypes
import json
import os
import secrets
import socket
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
import re
from typing import Any

from contracts import canonical_bytes, fingerprint


class RunIdentityMismatch(RuntimeError):
    pass


class RunAlreadyOwned(RuntimeError):
    pass


def _utc_epoch(value: str) -> float:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise RunIdentityMismatch("lock heartbeat must include a timezone")
    return parsed.timestamp()


def _local_pid_is_live(pid: int) -> bool:
    if type(pid) is not int or pid <= 0:
        return True  # An invalid or unknown owner is never proof of death.
    if os.name == "nt":
        # os.kill(pid, 0) can terminate a Windows process. Only query a handle.
        from ctypes import wintypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        kernel.WaitForSingleObject.restype = wintypes.DWORD
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.CloseHandle.restype = wintypes.BOOL
        handle = kernel.OpenProcess(0x00100000, False, pid)  # SYNCHRONIZE only
        if not handle:
            return ctypes.get_last_error() != 87  # only ERROR_INVALID_PARAMETER establishes no PID
        try:
            return kernel.WaitForSingleObject(handle, 0) != 0
        finally:
            kernel.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True  # Unknown probe errors must not authorize reclamation.
    return True


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_relative(path: str) -> PurePosixPath:
    if not isinstance(path, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", path) or path.endswith("."):
        raise ValueError("artifact paths must be one contained filename")
    relative = PurePosixPath(path)
    if relative.stem.split('.')[0].upper() in {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}:
        raise ValueError("Windows device artifact name refused")
    return relative


def _regular(path: Path) -> bool:
    return path.is_file() and not path.is_symlink() and not path.is_junction()


def _atomic(path: Path, payload: bytes) -> None:
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".partial-", delete=False) as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _descriptor(path: Path) -> dict[str, Any]:
    if not _regular(path):
        raise RunIdentityMismatch(f"not a regular artifact: {path.name}")
    return {"path": path.name, "bytes": path.stat().st_size, "sha256": sha256_file(path)}


def effective_run_identity(*, contract: dict[str, Any], dependency_hashes: dict[str, str]) -> dict[str, Any]:
    if not isinstance(contract, dict) or not contract or not isinstance(dependency_hashes, dict) or not dependency_hashes:
        raise ValueError("contract and dependency hashes are required")
    if any(not isinstance(key, str) or not key or not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value) for key, value in dependency_hashes.items()):
        raise ValueError("dependency hashes must be SHA-256 hex")
    unsigned = {"schema_version": "all68_run_identity.v2", "contract": contract,
                "dependency_hashes": dict(sorted(dependency_hashes.items()))}
    return {**unsigned, "fingerprint": fingerprint(unsigned)}


def validate_run_identity(identity: dict[str, Any]) -> None:
    if not isinstance(identity, dict) or set(identity) != {"schema_version", "contract", "dependency_hashes", "fingerprint"}:
        raise RunIdentityMismatch("invalid run identity fields")
    try:
        expected = effective_run_identity(contract=identity["contract"], dependency_hashes=identity["dependency_hashes"])
    except (ValueError, TypeError, AttributeError) as exc:
        raise RunIdentityMismatch("invalid contract or dependency hashes") from exc
    supplied = identity.get("fingerprint")
    unsigned = {key: value for key, value in identity.items() if key != "fingerprint"}
    if supplied != fingerprint(unsigned) or identity != expected:
        raise RunIdentityMismatch("run identity fingerprint does not match its stored dependencies")


class RunPublisher:
    """Own one run directory with a kernel lock and matching owner metadata."""

    def __init__(self, parent: Path, run_id: str, identity: dict[str, Any]):
        if not run_id or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789_-" for char in run_id):
            raise ValueError("run_id must be lowercase letters, digits, _ or -")
        validate_run_identity(identity)
        self.parent, self.root, self.identity = parent, parent / run_id, identity
        self.lock = self.root / ".owner.lock"
        self.identity_path = self.root / "RUN_IDENTITY.json"
        self._owner_token: str | None = None
        self._guard_handle = None
        self.journal_path = self.root / "PAYLOAD_JOURNAL.json"

    def _take_guard(self) -> None:
        if self._guard_handle is not None:
            raise RunAlreadyOwned("publisher already owns the run")
        self.root.mkdir(parents=True, exist_ok=True)
        if self.root.is_symlink() or self.root.is_junction():
            raise RunIdentityMismatch("run root must not be a link")
        path = self.root / ".writer.guard"
        if path.is_symlink() or path.is_junction():
            raise RunIdentityMismatch("writer guard must not be a link")
        handle = path.open("a+b")
        if path.stat().st_size == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise RunAlreadyOwned("run has an active writer OS lock") from exc
        self._guard_handle = handle

    def _drop_guard(self) -> None:
        if self._guard_handle is not None:
            self._guard_handle.close()  # kernel releases ownership even after a process crash
            self._guard_handle = None

    def _require_owner(self) -> None:
        if self._guard_handle is None or self._owner_token is None or not _regular(self.lock):
            raise RunAlreadyOwned("this publisher does not own the run lock")
        lock = json.loads(self.lock.read_text(encoding="utf-8"))
        if lock.get("owner_token") != self._owner_token or lock.get("identity") != self.identity["fingerprint"]:
            raise RunAlreadyOwned("this publisher no longer owns the run lock")

    def acquire(self, *, recover: bool = False, lease_seconds: float = 0) -> None:
        if self._guard_handle is not None:
            raise RunAlreadyOwned("publisher already owns the run")
        self._take_guard()
        try:
            self._acquire_locked(recover=recover, lease_seconds=lease_seconds)
        except BaseException:
            self._drop_guard()
            raise

    def _acquire_locked(self, *, recover: bool, lease_seconds: float) -> None:
        completion = self.root / "COMPLETION_MANIFEST.json"
        if completion.exists():
            existing = verify_completed_run(self.root, self.identity)
            validate_run_identity(existing["run_identity"])
            if existing["run_identity"]["fingerprint"] != self.identity["fingerprint"]:
                raise RunIdentityMismatch("completed run has a different dependency identity; fork a new run id")
            raise RunAlreadyOwned("run is already complete")
        if self.lock.exists():
            if not recover:
                raise RunAlreadyOwned("run has writer metadata; explicit recovery required")
            self._reclaim_locked(lease_seconds)
        if self.identity_path.exists():
            if not _regular(self.identity_path):
                raise RunIdentityMismatch("run identity must be a regular file")
            persisted = json.loads(self.identity_path.read_text(encoding="utf-8"))
            validate_run_identity(persisted)
            if persisted["fingerprint"] != self.identity["fingerprint"]:
                raise RunIdentityMismatch("incomplete run has a different dependency identity; fork a new run id")
        else:
            _atomic(self.identity_path, canonical_bytes(self.identity) + b"\n")
        token = secrets.token_hex(32)
        if self.lock.exists():
            raise RunAlreadyOwned("run has writer metadata; inspect heartbeat before recovery")
        now = datetime.now(timezone.utc).isoformat()
        # The kernel guard makes the absence check exclusive. Atomic metadata
        # publication leaves either no owner or one complete owner after death.
        _atomic(self.lock, canonical_bytes({"pid": os.getpid(), "hostname": socket.gethostname(),
                 "identity": self.identity["fingerprint"], "owner_token": token,
                 "acquired_utc": now, "heartbeat_utc": now}) + b"\n")
        self._owner_token = token

    def reclaim_expired_lock(self, lease_seconds: float, now_epoch: float | None = None) -> None:
        """Reclaim only an expired, locally-dead writer; never delete a live lock."""
        self._take_guard()
        try:
            self._reclaim_locked(lease_seconds, now_epoch)
        finally:
            self._drop_guard()

    def _reclaim_locked(self, lease_seconds: float, now_epoch: float | None = None) -> None:
        if lease_seconds < 0:
            raise ValueError("lease_seconds must be nonnegative")
        if not self.lock.exists():
            return
        if not _regular(self.lock):
            raise RunIdentityMismatch("owner metadata must be a regular file")
        lock = json.loads(self.lock.read_text(encoding="utf-8"))
        if lock.get("identity") != self.identity["fingerprint"]:
            raise RunIdentityMismatch("stale-lock identity differs; refuse recovery")
        heartbeat = _utc_epoch(str(lock.get("heartbeat_utc") or ""))
        now = datetime.now(timezone.utc).timestamp() if now_epoch is None else float(now_epoch)
        if now - heartbeat <= lease_seconds:
            raise RunAlreadyOwned("writer heartbeat lease has not expired")
        if lock.get("hostname") != socket.gethostname():
            raise RunAlreadyOwned("expired lock belongs to another host; require explicit handoff")
        if _local_pid_is_live(lock.get("pid", 0)):
            raise RunAlreadyOwned("expired lock owner process is still live")
        token = str(lock.get("owner_token") or "")
        if not token:
            raise RunIdentityMismatch("lock has no owner token")
        # Every acquisition and reclamation holds the same kernel guard, so
        # another reclaimer cannot remove a newer writer's metadata.
        self.lock.unlink()

    def heartbeat(self) -> None:
        self._require_owner()
        lock = json.loads(self.lock.read_text(encoding="utf-8"))
        lock["heartbeat_utc"] = datetime.now(timezone.utc).isoformat()
        _atomic(self.lock, canonical_bytes(lock) + b"\n")

    def release(self) -> None:
        self._require_owner()
        self.lock.unlink()
        self._owner_token = None
        self._drop_guard()

    def _journal(self) -> dict[str, Any]:
        if not self.journal_path.exists():
            return {"schema_version": "payload_journal.v2", "identity": self.identity["fingerprint"], "payloads": {}}
        if not _regular(self.journal_path):
            raise RunIdentityMismatch("payload journal must be regular")
        journal = json.loads(self.journal_path.read_text(encoding="utf-8"))
        if journal.get("schema_version") != "payload_journal.v2" or journal.get("identity") != self.identity["fingerprint"] or not isinstance(journal.get("payloads"), dict):
            raise RunIdentityMismatch("payload journal identity mismatch")
        return journal

    def _record_payload(self, descriptor: dict[str, Any]) -> None:
        journal = self._journal()
        if any(name.casefold() == descriptor["path"].casefold() and name != descriptor["path"] for name in journal["payloads"]):
            raise RunIdentityMismatch("case-colliding payload name")
        old = journal["payloads"].get(descriptor["path"])
        if old is not None and old != descriptor:
            raise RunIdentityMismatch(f"journal mismatch: {descriptor['path']}")
        journal["payloads"][descriptor["path"]] = descriptor
        _atomic(self.journal_path, canonical_bytes(journal) + b"\n")

    def read_verified_payload(self, relative_path: str) -> bytes | None:
        self._require_owner()
        name = _safe_relative(relative_path).name
        descriptor = self._journal()["payloads"].get(name)
        if descriptor is None:
            return None  # an unjournaled file is not a reusable cache
        path = self.root / name
        if _descriptor(path) != descriptor:
            raise RunIdentityMismatch(f"corrupt cached payload: {name}")
        return path.read_bytes()

    def write_payload(self, relative_path: str, payload: bytes) -> dict[str, Any]:
        self._require_owner()
        name = _safe_relative(relative_path)
        if name.name.casefold() in {"run_identity.json", "completion_manifest.json", "payload_journal.json"}:
            raise ValueError("reserved publication artifact name")
        destination = self.root / name.name
        if destination.exists():
            raise FileExistsError("published payload is immutable")
        _atomic(destination, payload)
        descriptor = _descriptor(destination)
        self._record_payload(descriptor)
        self.heartbeat()
        return descriptor

    def write_or_validate_payload(self, relative_path: str, payload: bytes) -> dict[str, Any]:
        """Resume deterministic output only when the existing bytes are exact."""
        self._require_owner()
        name = _safe_relative(relative_path)
        if name.name.casefold() in {"run_identity.json", "completion_manifest.json", "payload_journal.json"}:
            raise ValueError("reserved publication artifact name")
        destination = self.root / name.name
        if not destination.exists():
            return self.write_payload(relative_path, payload)
        expected = hashlib.sha256(payload).hexdigest()
        if not _regular(destination) or destination.stat().st_size != len(payload) or sha256_file(destination) != expected:
            raise RunIdentityMismatch(f"partial payload does not match deterministic resume bytes: {relative_path}")
        self._record_payload(_descriptor(destination))
        self.heartbeat()
        return {"path": name.as_posix(), "bytes": destination.stat().st_size, "sha256": expected}

    def complete(self, payloads: list[dict[str, Any]], required_payloads: set[str]) -> Path:
        self._require_owner()
        _validate_inventory(sorted(required_payloads), payloads, self.identity)
        for item in payloads:
            name = _safe_relative(item["path"])
            path = self.root / name.name
            if _descriptor(path) != item:
                raise RunIdentityMismatch(f"payload verification failed: {item['path']}")
        manifest = {"schema_version": "all68_completion_manifest.v2", "completed_utc": datetime.now(timezone.utc).isoformat(),
                    "run_identity": self.identity, "required_payloads": sorted(required_payloads),
                    "payloads": sorted(payloads, key=lambda row: row["path"])}
        target = self.root / "COMPLETION_MANIFEST.json"
        if target.exists():
            raise FileExistsError("completion is immutable")
        _atomic(target, canonical_bytes(manifest) + b"\n")
        self.release()
        return target


def _validate_inventory(required: Any, payloads: Any, identity: dict[str, Any]) -> None:
    if not isinstance(required, list) or not required or any(not isinstance(name, str) for name in required):
        raise RunIdentityMismatch("completion requires a nonempty payload inventory")
    if len(set(name.casefold() for name in required)) != len(required):
        raise RunIdentityMismatch("duplicate or case-colliding required payload")
    for name in required:
        _safe_relative(name)
        if name.casefold() in {"run_identity.json", "completion_manifest.json", "payload_journal.json"}:
            raise RunIdentityMismatch("reserved payload in inventory")
    bound = identity["contract"].get("required_payloads")
    if bound is not None and (not isinstance(bound, list) or sorted(bound) != sorted(required)):
        raise RunIdentityMismatch("payload inventory differs from identity contract")
    if not isinstance(payloads, list) or len(payloads) != len(required):
        raise RunIdentityMismatch("completion payload set differs from required inventory")
    names = []
    for item in payloads:
        if not isinstance(item, dict) or set(item) != {"path", "bytes", "sha256"}:
            raise RunIdentityMismatch("invalid payload descriptor")
        if type(item["bytes"]) is not int or item["bytes"] < 0 or not isinstance(item["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", item["sha256"]):
            raise RunIdentityMismatch("invalid payload size or hash")
        _safe_relative(item["path"])
        names.append(item["path"])
    if sorted(names) != sorted(required):
        raise RunIdentityMismatch("completion manifest payload inventory is inconsistent")


def verify_completed_run(root: Path, expected_identity: dict[str, Any]) -> dict[str, Any]:
    validate_run_identity(expected_identity)
    if root.is_symlink() or root.is_junction():
        raise RunIdentityMismatch("run root must not be a link")
    manifest_path = root / "COMPLETION_MANIFEST.json"
    if not _regular(manifest_path):
        raise RunIdentityMismatch("run has no completion manifest")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("schema_version") != "all68_completion_manifest.v2":
        raise RunIdentityMismatch("invalid completion schema")
    validate_run_identity(manifest.get("run_identity"))
    if manifest["run_identity"]["fingerprint"] != expected_identity["fingerprint"]:
        raise RunIdentityMismatch("dependency identity mismatch; refuse resume")
    identity_path = root / "RUN_IDENTITY.json"
    if not _regular(identity_path):
        raise RunIdentityMismatch("missing persisted run identity")
    persisted = json.loads(identity_path.read_text(encoding="utf-8"))
    validate_run_identity(persisted)
    if persisted != expected_identity:
        raise RunIdentityMismatch("persisted identity mismatch")
    required, payloads = manifest.get("required_payloads"), manifest.get("payloads")
    _validate_inventory(required, payloads, expected_identity)
    for item in payloads:
        name = _safe_relative(item["path"])
        path = root / name.name
        if not _regular(path) or _descriptor(path) != item:
            raise RunIdentityMismatch(f"corrupt or missing payload: {item['path']}")
    return manifest


def package_completed_run(source: Path, package: Path) -> dict[str, Any]:
    manifest = json.loads((source / "COMPLETION_MANIFEST.json").read_text(encoding="utf-8"))
    validate_run_identity(manifest["run_identity"])
    verify_completed_run(source, manifest["run_identity"])
    if package.exists():
        raise FileExistsError("fixture package is immutable")
    members = ["RUN_IDENTITY.json", "COMPLETION_MANIFEST.json", *manifest["required_payloads"]]
    with zipfile.ZipFile(package, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in members:
            archive.write(source / name, arcname=name)
    return {"package": package.name, "sha256": sha256_file(package), "members": members}


def restore_completed_run(package: Path, destination: Path, expected_identity: dict[str, Any], *, expected_sha256: str | None = None) -> dict[str, Any]:
    validate_run_identity(expected_identity)
    if expected_sha256 is not None and sha256_file(package) != expected_sha256:
        raise RunIdentityMismatch("package hash mismatch")
    if destination.is_symlink() or destination.is_junction():
        raise RunIdentityMismatch("restore destination must not be a link")
    if destination.exists() and any(destination.iterdir()):
        raise FileExistsError("restore destination must be empty")
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(package) as archive:
        names = archive.namelist()
        if len(set(name.casefold() for name in names)) != len(names):
            raise RunIdentityMismatch("duplicate archive member")
        for name in names:
            _safe_relative(name)
        if "COMPLETION_MANIFEST.json" not in names:
            raise RunIdentityMismatch("package lacks completion manifest")
        manifest = json.loads(archive.read("COMPLETION_MANIFEST.json"))
        if manifest.get("run_identity") != expected_identity:
            raise RunIdentityMismatch("package dependency identity mismatch")
        _validate_inventory(manifest.get("required_payloads"), manifest.get("payloads"), expected_identity)
        if set(names) != {"RUN_IDENTITY.json", "COMPLETION_MANIFEST.json", *manifest["required_payloads"]}:
            raise RunIdentityMismatch("unexpected archive inventory")
        for name in names:
            relative = _safe_relative(name)
            with archive.open(name) as source, (destination / relative.name).open("xb") as output:
                output.write(source.read())
    return verify_completed_run(destination, expected_identity)
