"""Real subprocess evidence for publication; all processes are owned fixtures."""
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import zipfile

import pytest

from publication import (RunAlreadyOwned, RunIdentityMismatch, RunPublisher,
                         _local_pid_is_live, effective_run_identity,
                         package_completed_run, restore_completed_run,
                         sha256_file, verify_completed_run)


STAGE = Path(__file__).resolve().parent
PAYLOADS = {"a.json": b'{"a":1}\n', "b.json": b'{"b":2}\n'}
IDENTITY = effective_run_identity(contract={"fixture": "process-boundaries.v2", "required_payloads": sorted(PAYLOADS)},
                                  dependency_hashes={"fixture": "a" * 64})
WORKER = r'''
import json, os, pathlib, sys, time
sys.path.insert(0, sys.argv[1])
from publication import RunPublisher, RunAlreadyOwned
parent, mode, identity = pathlib.Path(sys.argv[2]), sys.argv[3], json.loads(sys.argv[4])
if mode == "crash-before-owner-metadata":
    import publication
    original_atomic = publication._atomic
    def crash_on_owner(path, payload):
        if path.name == ".owner.lock": os._exit(91)
        return original_atomic(path, payload)
    publication._atomic = crash_on_owner
p = RunPublisher(parent, "fixture", identity)
try:
    p.acquire(recover=mode.startswith("resume"))
except RunAlreadyOwned:
    sys.exit(23)
if mode == "hold":
    (parent / "ready").write_text(str(os.getpid()))
    while True:
        time.sleep(0.02)
if mode == "crash-before-payload": os._exit(91)
rows = [p.write_or_validate_payload("a.json", b'{"a":1}\n')]
if mode == "crash-after-first": os._exit(91)
rows.append(p.write_or_validate_payload("b.json", b'{"b":2}\n'))
if mode == "crash-after-all": os._exit(91)
if mode == "crash-during-completion":
    p.release = lambda: os._exit(91)
p.complete(rows, {"a.json", "b.json"})
'''


def command(parent: Path, mode: str) -> list[str]:
    return [sys.executable, "-I", "-B", "-c", WORKER, str(STAGE), str(parent), mode, json.dumps(IDENTITY)]


def run_worker(parent: Path, mode: str) -> subprocess.CompletedProcess:
    return subprocess.run(command(parent, mode), capture_output=True, text=True, timeout=30)


def test_live_pid_probe_and_two_process_writer_exclusion(tmp_path: Path):
    child = subprocess.Popen(command(tmp_path, "hold"), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 15
        while not (tmp_path / "ready").exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert (tmp_path / "ready").exists(), child.communicate(timeout=2)
        assert _local_pid_is_live(child.pid)
        assert child.poll() is None, "PID liveness probe must not terminate its subject"
        contender = run_worker(tmp_path, "resume")
        assert contender.returncode == 23, contender.stderr
        with pytest.raises(RunAlreadyOwned, match="active writer"):
            RunPublisher(tmp_path, "fixture", IDENTITY).reclaim_expired_lock(0)
        assert child.poll() is None
        assert not (tmp_path / "fixture" / "COMPLETION_MANIFEST.json").exists()
    finally:
        if child.poll() is None:
            child.terminate()
        child.communicate(timeout=15)
    assert not _local_pid_is_live(child.pid)
    recovered = run_worker(tmp_path, "resume")
    assert recovered.returncode == 0, recovered.stderr
    verify_completed_run(tmp_path / "fixture", IDENTITY)


@pytest.mark.parametrize("boundary", ["crash-before-owner-metadata", "crash-before-payload", "crash-after-first", "crash-after-all", "crash-during-completion"])
def test_real_process_crash_boundaries_match_uninterrupted(tmp_path: Path, boundary: str):
    clean = run_worker(tmp_path / "clean", "normal")
    assert clean.returncode == 0, clean.stderr
    crashed = run_worker(tmp_path / "crashed", boundary)
    assert crashed.returncode == 91, crashed.stderr
    root = tmp_path / "crashed" / "fixture"
    if boundary == "crash-during-completion":
        verify_completed_run(root, IDENTITY)
    else:
        assert not (root / "COMPLETION_MANIFEST.json").exists()
        resumed = run_worker(tmp_path / "crashed", "resume")
        assert resumed.returncode == 0, resumed.stderr
    expected = verify_completed_run(tmp_path / "clean" / "fixture", IDENTITY)
    actual = verify_completed_run(root, IDENTITY)
    assert expected["payloads"] == actual["payloads"]
    for name, payload in PAYLOADS.items():
        assert (root / name).read_bytes() == payload


def test_dead_writer_identity_change_and_cache_corruption_fail_closed(tmp_path: Path):
    assert run_worker(tmp_path, "crash-after-first").returncode == 91
    changed = effective_run_identity(contract={**IDENTITY["contract"], "fixture": "changed"},
                                     dependency_hashes=IDENTITY["dependency_hashes"])
    with pytest.raises(RunIdentityMismatch, match="identity"):
        RunPublisher(tmp_path, "fixture", changed).acquire(recover=True)
    publisher = RunPublisher(tmp_path, "fixture", IDENTITY)
    publisher.acquire(recover=True)
    try:
        assert publisher.read_verified_payload("a.json") == PAYLOADS["a.json"]
        (publisher.root / "a.json").write_bytes(b"corrupted")
        with pytest.raises(RunIdentityMismatch, match="corrupt"):
            publisher.read_verified_payload("a.json")
        with pytest.raises(RunIdentityMismatch, match="deterministic"):
            publisher.write_or_validate_payload("a.json", PAYLOADS["a.json"])
        assert not (publisher.root / "COMPLETION_MANIFEST.json").exists()
    finally:
        publisher.release()


def test_live_metadata_without_os_guard_cannot_be_reclaimed(tmp_path: Path):
    publisher = RunPublisher(tmp_path, "fixture", IDENTITY)
    publisher.acquire()
    publisher.release()
    publisher.lock.write_text(json.dumps({"pid": os.getpid(), "hostname": socket.gethostname(),
                                          "heartbeat_utc": "2000-01-01T00:00:00Z", "owner_token": "fixture",
                                          "identity": IDENTITY["fingerprint"]}))
    with pytest.raises(RunAlreadyOwned, match="still live"):
        RunPublisher(tmp_path, "fixture", IDENTITY).acquire(recover=True)
    assert publisher.lock.exists()


@pytest.mark.parametrize("name", ["../outside", "..\\outside", "C:outside", "data.json:stream", "CON", "nul.txt", "x.", "RUN_IDENTITY.json", "COMPLETION_MANIFEST.json", "PAYLOAD_JOURNAL.json"])
def test_windows_unsafe_or_reserved_payload_names_are_refused(tmp_path: Path, name: str):
    publisher = RunPublisher(tmp_path, "fixture", IDENTITY)
    publisher.acquire()
    try:
        with pytest.raises(ValueError):
            publisher.write_or_validate_payload(name, b"bad")
    finally:
        publisher.release()


@pytest.mark.parametrize("mutation", ["empty", "duplicate", "drop", "persisted_identity", "hash", "schema"])
def test_completed_inventory_tampering_is_rejected(tmp_path: Path, mutation: str):
    assert run_worker(tmp_path, "normal").returncode == 0
    root = tmp_path / "fixture"
    path = root / "COMPLETION_MANIFEST.json"
    manifest = json.loads(path.read_text())
    if mutation == "empty":
        manifest["required_payloads"], manifest["payloads"] = [], []
    elif mutation == "duplicate":
        manifest["payloads"].append(manifest["payloads"][0])
    elif mutation == "drop":
        manifest["required_payloads"].pop()
        manifest["payloads"].pop()
    elif mutation == "persisted_identity":
        (root / "RUN_IDENTITY.json").write_text("{}")
    elif mutation == "hash":
        manifest["payloads"][0]["sha256"] = "0" * 64
    else:
        manifest["schema_version"] = "unsupported"
    path.write_text(json.dumps(manifest))
    with pytest.raises(RunIdentityMismatch):
        verify_completed_run(root, IDENTITY)


def test_result_archive_external_hash_and_unexpected_member_are_rejected(tmp_path: Path):
    assert run_worker(tmp_path / "source", "normal").returncode == 0
    package = tmp_path / "result.zip"
    receipt = package_completed_run(tmp_path / "source" / "fixture", package)
    with pytest.raises(RunIdentityMismatch, match="package hash"):
        restore_completed_run(package, tmp_path / "wrong-hash", IDENTITY, expected_sha256="0" * 64)
    restore_completed_run(package, tmp_path / "valid", IDENTITY, expected_sha256=receipt["sha256"])
    with zipfile.ZipFile(package, "a") as archive:
        archive.writestr("extra.json", "{}")
    with pytest.raises(RunIdentityMismatch, match="unexpected archive"):
        restore_completed_run(package, tmp_path / "extra", IDENTITY, expected_sha256=sha256_file(package))
