from __future__ import annotations

import ast
import gzip
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

from oanda_sequential_replay_source_pack import build, contained_path
from oanda_sequential_replay_source_pack_verifier import (
    IDENTITY_KEYS,
    read_gzip_bounded,
    stable_hash,
    verify,
)


ROOT = Path(__file__).resolve().parent
BASE_CONFIG = json.loads(
    (ROOT / "config" / "sequential_replay_source_pack_v1.json").read_text(encoding="utf-8")
)
HEADER = ",".join(BASE_CONFIG["source"]["required_columns"])
BASE_EPOCH = 1_800_000_000


def _iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000000Z")


def _source_payload(instrument: str, *, missing: set[int] | None = None) -> str:
    first = BASE_EPOCH - 61 * 60
    rows = [HEADER]
    for offset in range(78):
        epoch = first + offset * 60
        if missing and epoch in missing:
            continue
        stamp = _iso(epoch)
        rows.append(
            f"{stamp},{stamp},{instrument},M1,1.1000,1.1002,1.0998,1.1001,"
            "1.0999,1.1001,1.0997,1.1000,1.1001,1.1003,1.0999,1.1002,2.0,10"
        )
    return "\n".join(rows) + "\n"


def _fixture_root(tmp_path: Path, *, missing_instrument: str | None = None) -> tuple[Path, Path]:
    root = tmp_path / "project"
    candles = root / "candles"
    core = root / "src" / "forex_system" / "research"
    candles.mkdir(parents=True)
    core.mkdir(parents=True)
    shutil.copyfile(ROOT / "oanda_sequential_replay_source_pack.py", root / "oanda_sequential_replay_source_pack.py")
    shutil.copyfile(
        ROOT / "src" / "forex_system" / "research" / "sequential_replay_source_pack_v1.py",
        core / "sequential_replay_source_pack_v1.py",
    )
    for instrument in BASE_CONFIG["source"]["expected_instruments"]:
        missing = {BASE_EPOCH + 60} if instrument == missing_instrument else None
        (candles / f"{instrument}_M1.csv").write_text(
            _source_payload(instrument, missing=missing), encoding="utf-8"
        )
    config = json.loads(json.dumps(BASE_CONFIG))
    config["source"]["relative_root"] = "candles"
    config["source"]["maximum_source_bytes_per_instrument"] = 1_000_000
    config["schedule"]["sessions"] = [
        {
            "session_key": "fixture_a",
            "start_utc": _iso(BASE_EPOCH),
            "end_utc_exclusive": _iso(BASE_EPOCH + 5 * 60),
        },
        {
            "session_key": "fixture_b",
            "start_utc": _iso(BASE_EPOCH + 10 * 60),
            "end_utc_exclusive": _iso(BASE_EPOCH + 15 * 60),
        },
    ]
    config["storage"]["artifact_relative_root"] = "artifacts"
    config["storage"]["maximum_archive_raw_bytes"] = 1_000_000
    config["storage"]["maximum_archive_gzip_bytes"] = 500_000
    config_path = root / "source_pack_config.json"
    config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return root, config_path


def test_clean_fixture_is_independently_reconstructed(tmp_path: Path) -> None:
    root, config_path = _fixture_root(tmp_path)
    state = build(config_path, root=root)
    receipt = verify(config_path, root=root)
    assert receipt["verified"] is True
    assert receipt["failures"] == []
    assert state["scheduled_global_clock_count"] == 2
    assert state["scheduled_pair_context_count"] == 136
    assert state["market_repetition_count"] == 2


def test_archive_tampering_is_detected(tmp_path: Path) -> None:
    root, config_path = _fixture_root(tmp_path)
    state = build(config_path, root=root)
    stored = state["source_manifest"][0]
    archive = root / "artifacts" / stored["archive_relative_path"]
    payload = bytearray(archive.read_bytes())
    payload[len(payload) // 2] ^= 0x01
    archive.write_bytes(payload)
    receipt = verify(config_path, root=root)
    assert receipt["verified"] is False
    assert any(
        failure.startswith(("slice_error:", "archive_source_mismatch:", "manifest_rebuild_mismatch:"))
        for failure in receipt["failures"]
    )


def test_raw_archive_limit_is_enforced_before_return(tmp_path: Path) -> None:
    archive = tmp_path / "bomb.csv.gz"
    archive.write_bytes(gzip.compress(b"x" * 4097, mtime=0))
    with pytest.raises(ValueError, match="oversized_archive_raw"):
        read_gzip_bounded(archive, 4096)


def test_path_traversal_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="outside project root"):
        contained_path(tmp_path, "../escape")


def test_missing_quote_preserves_predeclared_global_clocks(tmp_path: Path) -> None:
    root, config_path = _fixture_root(tmp_path, missing_instrument="EUR_USD")
    state = build(config_path, root=root)
    receipt = verify(config_path, root=root)
    eur_rows = [row for row in state["source_manifest"] if row["instrument"] == "EUR_USD"]
    assert receipt["verified"] is True
    assert state["scheduled_global_clock_count"] == 2
    assert state["scheduled_pair_context_count"] == 136
    assert len(eur_rows) == 2
    assert sum(int(row["execution_ready_clock_count"]) for row in eur_rows) < 2
    assert state["coverage"]["execution_failure_count"] >= 1


def test_verifier_has_no_producer_or_core_import() -> None:
    path = ROOT / "oanda_sequential_replay_source_pack_verifier.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(str(node.module or ""))
    joined = "\n".join(imported).lower()
    assert "oanda_sequential_replay_source_pack" not in joined
    assert "sequential_replay_source_pack_v1" not in joined


def test_verifier_rejects_duplicate_manifest_identity_hiding_missing_pair(tmp_path: Path) -> None:
    root, config_path = _fixture_root(tmp_path)
    state = build(config_path, root=root)
    rows = [dict(row) for row in state["source_manifest"][:-1]]
    rows.append(dict(state["source_manifest"][0]))
    rows.sort(key=lambda row: (row["session_key"], row["instrument"]))
    state["source_manifest"] = rows
    state["source_manifest_sha256"] = stable_hash(rows)
    identities = [{key: row[key] for key in IDENTITY_KEYS} for row in rows]
    material = dict(state["material_contract"])
    material["slice_identity_manifest"] = identities
    state["material_contract"] = material
    state["material_sha256"] = stable_hash(material)
    state["pack_id"] = "sequential_replay_source_pack_v1." + state["material_sha256"][:20]
    output = root / "artifacts"
    manifest = output / "packs" / state["pack_id"] / "manifest.json"
    manifest.parent.mkdir(parents=True)
    encoded = json.dumps(state, indent=2, sort_keys=True) + "\n"
    manifest.write_text(encoded, encoding="utf-8")
    (output / "sequential_replay_source_pack_v1.json").write_text(encoded, encoding="utf-8")
    receipt = verify(config_path, root=root)
    assert receipt["verified"] is False
    assert any("manifest_identity" in failure for failure in receipt["failures"])


def test_verifier_rejects_tampered_aggregate_coverage(tmp_path: Path) -> None:
    root, config_path = _fixture_root(tmp_path)
    state = build(config_path, root=root)
    state["coverage"]["fully_ready_pair_context_count"] += 10_000
    output = root / "artifacts"
    encoded = json.dumps(state, indent=2, sort_keys=True) + "\n"
    (output / "packs" / state["pack_id"] / "manifest.json").write_text(encoded, encoding="utf-8")
    (output / "sequential_replay_source_pack_v1.json").write_text(encoded, encoding="utf-8")
    receipt = verify(config_path, root=root)
    assert receipt["verified"] is False
    assert "coverage_summary" in receipt["failures"]


@pytest.mark.parametrize(
    ("key", "forged"),
    [
        ("research_only", False),
        ("execution_eligible", True),
        ("proof_eligible", True),
        ("can_place_orders", True),
        ("can_authorize", True),
        ("broker_access", True),
        ("account_access", True),
        ("supported_decision", "trade"),
    ],
)
def test_verifier_rejects_forged_pack_safety_state(
    tmp_path: Path, key: str, forged: object,
) -> None:
    root, config_path = _fixture_root(tmp_path)
    state = build(config_path, root=root)
    state[key] = forged
    output = root / "artifacts"
    encoded = json.dumps(state, indent=2, sort_keys=True) + "\n"
    (output / "packs" / state["pack_id"] / "manifest.json").write_text(
        encoded, encoding="utf-8"
    )
    (output / "sequential_replay_source_pack_v1.json").write_text(
        encoded, encoding="utf-8"
    )
    receipt = verify(config_path, root=root)
    assert receipt["verified"] is False
    assert f"state_safety_{key}" in receipt["failures"]


def test_metadata_only_overlap_sessions_share_the_same_utc_block() -> None:
    windows = {
        (
            datetime.fromtimestamp(
                int(datetime.fromisoformat(row["start_utc"].replace("Z", "+00:00")).timestamp()),
                tz=timezone.utc,
            ).strftime("%H:%M"),
            datetime.fromtimestamp(
                int(datetime.fromisoformat(row["end_utc_exclusive"].replace("Z", "+00:00")).timestamp()),
                tz=timezone.utc,
            ).strftime("%H:%M"),
        )
        for row in BASE_CONFIG["schedule"]["sessions"]
    }
    assert windows == {("12:00", "16:00")}


def test_in_root_source_link_is_rejected_by_runner(tmp_path: Path) -> None:
    root, config_path = _fixture_root(tmp_path)
    source = root / "candles" / "EUR_USD_M1.csv"
    target = root / "candles" / "EUR_USD_M1.backup.csv"
    source.replace(target)
    try:
        os.symlink(target, source)
    except OSError as exc:
        pytest.skip(f"symlink unavailable on this platform: {exc}")
    with pytest.raises(ValueError, match="linked source file rejected"):
        build(config_path, root=root)


def test_out_of_window_source_append_keeps_pack_rerunnable(tmp_path: Path) -> None:
    root, config_path = _fixture_root(tmp_path)
    first = build(config_path, root=root)
    source = root / "candles" / "EUR_USD_M1.csv"
    appended_epoch = BASE_EPOCH + 30 * 60
    stamp = _iso(appended_epoch)
    with source.open("a", encoding="utf-8", newline="") as stream:
        stream.write(
            f"{stamp},{stamp},EUR_USD,M1,1.1000,1.1002,1.0998,1.1001,"
            "1.0999,1.1001,1.0997,1.1000,1.1001,1.1003,1.0999,1.1002,2.0,10\n"
        )
    second = build(config_path, root=root)
    assert second["pack_id"] == first["pack_id"]
    assert second["source_manifest_sha256"] == first["source_manifest_sha256"]


def test_contract_declares_compressed_archive_size_bound() -> None:
    storage = BASE_CONFIG["storage"]
    assert 0 < int(storage["maximum_archive_gzip_bytes"]) <= int(storage["maximum_archive_raw_bytes"])
