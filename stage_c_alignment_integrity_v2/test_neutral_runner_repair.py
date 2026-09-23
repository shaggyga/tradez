"""Exercise the actual bounded CLI and Parquet consumer, including child death."""
from __future__ import annotations

import io
import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import all68_neutral_runner_v2 as runner
from portable_checkpoint_v2 import make_fixture
from publication import sha256_file

ORIGIN = "2024-06-24T00:00:00+00:00"


def invoke(root: Path, run_id: str, *options: str, archive: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-I", "-B", str(Path(runner.__file__)), "--run-id", run_id,
                           "--archive", str(archive or root / "inputs" / "long_m1_68.zip"),
                           "--input-manifest", str(root / "inputs" / "INPUT_ARCHIVES.json"),
                           "--runs-dir", str(root / "runs"), *options],
                          capture_output=True, text=True, timeout=45, check=False)


def successful(result: subprocess.CompletedProcess) -> None:
    assert result.returncode == 0, result.stdout + result.stderr


def payloads(root: Path, run_id: str) -> dict[str, bytes]:
    return {name: (root / "runs" / run_id / name).read_bytes() for name in runner.REQUIRED_PAYLOADS}


@pytest.mark.parametrize("stage", ["inputs", "first-payload", "payloads", "completion"])
def test_real_cli_crash_and_archive_free_resume_match_uninterrupted(tmp_path: Path, stage: str):
    make_fixture(tmp_path)
    successful(invoke(tmp_path, "uninterrupted"))
    result = invoke(tmp_path, "interrupted", "--test-crash-after", stage)
    assert result.returncode == 91, result.stdout + result.stderr
    manifest = tmp_path / "runs" / "interrupted" / "COMPLETION_MANIFEST.json"
    assert manifest.exists() is (stage == "completion")
    # Missing archive is intentional proof that recovery consumes its verified
    # origin snapshot; rehashing or extraction would fail this real CLI call.
    successful(invoke(tmp_path, "interrupted", "--resume", "--report-only", archive=tmp_path / "absent.zip"))
    assert payloads(tmp_path, "interrupted") == payloads(tmp_path, "uninterrupted")
    report = json.loads(payloads(tmp_path, "interrupted")["run_report.json"])
    assert report["issuance"]["eligible_count"] == 68
    assert "generated_utc" not in report
    assert str(tmp_path) not in json.dumps(report)


def rewrite_fixture(root: Path, *, future_close: float | None = None, remove_future: bool = False,
                    origin_close: float | None = None, origin_text: str | None = None) -> None:
    manifest_path = root / "inputs" / "INPUT_ARCHIVES.json"
    manifest = json.loads(manifest_path.read_text())
    archive_path = root / "inputs" / "long_m1_68.zip"
    with zipfile.ZipFile(archive_path) as archive:
        contents = {name: archive.read(name) for name in archive.namelist()}
    for row in manifest["archives"][0]["members"]:
        table = pq.read_table(pa.BufferReader(contents[row["path"]]))
        times, closes = table["datetime"].to_pylist(), table["close"].to_pylist()
        if future_close is not None:
            closes[-1] = future_close
        if origin_close is not None:
            closes[1] = origin_close
        if origin_text is not None:
            times[1] = origin_text
        if remove_future:
            times, closes = times[:-1], closes[:-1]
        buffer = io.BytesIO()
        pq.write_table(pa.table({"datetime": times, "close": closes}), buffer, row_group_size=1)
        contents[row["path"]] = buffer.getvalue()
        row.update({"bytes": len(buffer.getvalue()), "sha256": runner.hashlib.sha256(buffer.getvalue()).hexdigest()})
    with zipfile.ZipFile(archive_path, "w") as archive:
        for name, content in contents.items():
            archive.writestr(name, content)
    manifest["archives"][0].update({"bytes": archive_path.stat().st_size, "sha256": sha256_file(archive_path)})
    manifest_path.write_bytes(runner.json_bytes(manifest))


def test_real_cli_future_endpoint_mutation_and_removal_leave_full_forecast_tape_unchanged(tmp_path: Path):
    make_fixture(tmp_path)
    successful(invoke(tmp_path, "before"))
    rewrite_fixture(tmp_path, future_close=float("inf"))
    successful(invoke(tmp_path, "perturbed"))
    rewrite_fixture(tmp_path, remove_future=True)
    successful(invoke(tmp_path, "removed"))
    for name in ("forecasts.jsonl", "forecast_coverage.jsonl", "outcomes.jsonl"):
        assert payloads(tmp_path, "before")[name] == payloads(tmp_path, "perturbed")[name] == payloads(tmp_path, "removed")[name]
    assert len(payloads(tmp_path, "before")["forecasts.jsonl"].splitlines()) == 68
    # Dependency identities must change when raw source bytes change, even when
    # the predictions are causally identical.
    assert payloads(tmp_path, "before")["run_report.json"] != payloads(tmp_path, "perturbed")["run_report.json"]


@pytest.mark.parametrize("origin_text", ["2024-06-24T00:00:00Z", "2024-06-23T20:00:00-04:00"])
def test_reader_finds_utc_equivalent_origin_in_second_row_group(tmp_path: Path, origin_text: str):
    make_fixture(tmp_path)
    rewrite_fixture(tmp_path, origin_text=origin_text)
    successful(invoke(tmp_path, "equivalent"))
    report = json.loads(payloads(tmp_path, "equivalent")["run_report.json"])
    assert report["issuance"]["eligible_count"] == 68


@pytest.mark.parametrize("close", [float("inf"), float("nan"), -1.0, 0.0])
def test_reader_blocks_nonfinite_or_nonpositive_origin(tmp_path: Path, close: float):
    make_fixture(tmp_path)
    rewrite_fixture(tmp_path, origin_close=close)
    members, _ = runner.read_long_members(tmp_path / "inputs" / "INPUT_ARCHIVES.json")
    rows = runner.origin_coverage(members, runner.parse_utc_epoch(ORIGIN), tmp_path / "inputs" / "long_m1_68.zip", verify_members=True)
    assert len(rows) == 68
    assert all(row["status"] == "blocked" and row["reason"] == "origin_close_invalid" for row in rows)


def test_resume_rejects_tampered_origin_snapshot_without_archive_access(tmp_path: Path):
    make_fixture(tmp_path)
    result = invoke(tmp_path, "damaged", "--test-crash-after", "inputs")
    assert result.returncode == 91, result.stderr
    path = tmp_path / "runs" / "damaged" / "origin_inputs.json"
    data = json.loads(path.read_bytes())
    data["coverage"][0]["origin_close"] = 99.0
    path.write_bytes(runner.json_bytes(data))
    result = invoke(tmp_path, "damaged", "--resume", "--report-only", archive=tmp_path / "absent.zip")
    assert result.returncode != 0
    assert "hash" in result.stderr.lower() or "corrupt" in result.stderr.lower()
    assert not (path.parent / "COMPLETION_MANIFEST.json").exists()


def test_fresh_report_only_refuses_to_extract_inputs(tmp_path: Path):
    make_fixture(tmp_path)
    result = invoke(tmp_path, "fresh", "--report-only", archive=tmp_path / "absent.zip")
    assert result.returncode != 0
    assert "report_only_requires_verified_origin_inputs" in result.stderr


def test_member_hash_disagreement_refuses_publication(tmp_path: Path):
    make_fixture(tmp_path)
    path = tmp_path / "inputs" / "INPUT_ARCHIVES.json"
    manifest = json.loads(path.read_bytes())
    manifest["archives"][0]["members"][0]["sha256"] = "0" * 64
    path.write_bytes(runner.json_bytes(manifest))
    result = invoke(tmp_path, "bad-member")
    assert result.returncode != 0
    assert "archive_member_integrity_mismatch" in result.stderr
    assert not (tmp_path / "runs" / "bad-member" / "COMPLETION_MANIFEST.json").exists()
