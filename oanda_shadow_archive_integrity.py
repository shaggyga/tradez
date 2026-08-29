#!/usr/bin/env python3
"""Read-only integrity census for compacted shadow-outcome Parquet evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from collections import Counter

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parent
ARCHIVE = ROOT / "data" / "oanda_training_manager" / "shadow_outcome_archive"
STATE = ROOT / "data" / "oanda_training_manager" / "state" / "shadow_archive_integrity_v1.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "storage" / "SHADOW_ARCHIVE_INTEGRITY_CURRENT.md"
ROLLUP = ROOT / "data" / "oanda_training_manager" / "state" / "shadow_outcome_rollups_v1.sqlite"
SOURCE = ROOT / "data" / "oanda_training_manager" / "state" / "strategy_shadow_outcomes_v1.sqlite"
EDGE_EVIDENCE = ROOT / "data" / "oanda_training_manager" / "state" / "edge_evidence_v1.sqlite"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1_048_576), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def inside(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def audit(archive: Path) -> dict[str, Any]:
    generated = datetime.now(timezone.utc).isoformat()
    parquet_paths = sorted(archive.rglob("*.parquet")) if archive.is_dir() else []
    manifest_paths = sorted(archive.rglob("*.manifest.json")) if archive.is_dir() else []
    failures: list[dict[str, Any]] = []
    readable = total_rows = total_bytes = 0
    metadata: dict[Path, tuple[int, int]] = {}
    rows_by_observed_date: Counter[str] = Counter()
    row_id_chunks_by_observed_date: dict[str, list[pa.Array]] = {}
    for path in parquet_paths:
        try:
            parquet = pq.ParquetFile(path)
            rows = int(parquet.metadata.num_rows)
            columns = len(parquet.schema_arrow.names)
            metadata[path.resolve()] = (rows, columns)
            readable += 1; total_rows += rows; total_bytes += int(path.stat().st_size)
            relative_parent = path.parent.resolve().relative_to(archive.resolve())
            partition = relative_parent.parts[-1] if relative_parent.parts else "unpartitioned"
            rows_by_observed_date[str(partition)] += rows
            if "row_id" in parquet.schema_arrow.names:
                row_ids = parquet.read(columns=["row_id"]).column(0)
                row_id_chunks_by_observed_date.setdefault(str(partition), []).extend(
                    list(row_ids.chunks)
                )
        except Exception as exc:
            failures.append({"path": str(path), "failure": "unreadable_parquet", "error": f"{type(exc).__name__}: {exc}"})
    validated: set[Path] = set()
    validated_success = 0
    for manifest_path in manifest_paths:
        try:
            value = json.loads(manifest_path.read_text(encoding="utf-8"))
            target = Path(str(value.get("parquet_path") or manifest_path.with_suffix(".parquet"))).resolve()
            if not inside(target, archive):
                raise ValueError("manifest target escapes archive root")
            if target in validated:
                raise ValueError("duplicate manifest target")
            validated.add(target)
            if target not in metadata:
                raise ValueError("manifest target is missing or unreadable")
            rows, columns = metadata[target]
            if rows != int(value.get("row_count") or -1):
                raise ValueError("row count mismatch")
            if columns != int(value.get("column_count") or -1):
                raise ValueError("column count mismatch")
            if target.stat().st_size != int(value.get("size_bytes") or -1):
                raise ValueError("file size mismatch")
            if sha256_file(target) != str(value.get("sha256") or ""):
                raise ValueError("sha256 mismatch")
            validated_success += 1
        except Exception as exc:
            failures.append({"path": str(manifest_path), "failure": "manifest_validation", "error": f"{type(exc).__name__}: {exc}"})
    legacy = len(set(metadata) - validated)
    unique_rows_by_observed_date = {
        day: int(pc.count_distinct(pa.chunked_array(chunks)).as_py() or 0)
        for day, chunks in sorted(row_id_chunks_by_observed_date.items())
    }
    duplicate_rows_by_observed_date = {
        day: int(rows_by_observed_date.get(day, 0) - unique_count)
        for day, unique_count in unique_rows_by_observed_date.items()
        if int(rows_by_observed_date.get(day, 0) - unique_count) > 0
    }
    total_unique_rows = (
        sum(unique_rows_by_observed_date.values())
        + int(rows_by_observed_date.get("unpartitioned", 0))
        - int(unique_rows_by_observed_date.get("unpartitioned", 0))
    )
    status = "failed" if failures else "ok_with_legacy_unmanifested" if legacy else "ok"
    return {
        "schema_version": 1, "generated_utc": generated, "status": status,
        "archive_root": str(archive.resolve()), "parquet_parts": len(parquet_paths),
        "readable_parquet_parts": readable, "manifest_parts": len(manifest_paths),
        "validated_manifest_parts": validated_success,
        "legacy_unmanifested_parts": legacy, "total_rows": total_rows,
        "rows_by_observed_date": dict(sorted(rows_by_observed_date.items())),
        "unique_rows_by_observed_date": unique_rows_by_observed_date,
        "duplicate_rows_by_observed_date": duplicate_rows_by_observed_date,
        "total_unique_rows": total_unique_rows,
        "duplicate_rows": total_rows - total_unique_rows,
        "total_bytes": total_bytes, "failures": failures,
        "policy": {
            "new_deletion_requires_predelete_manifest_validation": True,
            "legacy_unmanifested_parts_are_readable_but_not_retroactively_certified": True,
            "audit_is_read_only": True,
        },
        "research_only": True, "can_place_orders": False, "can_promote": False,
    }


def reconcile_detail_counts(
    result: dict[str, Any], *, rollup: Path, source: Path,
    evidence: Path | None = None,
) -> dict[str, Any]:
    reconciliation = {
        "rollup_sample_count": None, "live_detail_rows": None,
        "archived_detail_rows": int(
            result.get("total_unique_rows") or result.get("total_rows") or 0
        ),
        "detail_gap_vs_rollup": None,
        "interpretation": "unavailable",
    }
    if not rollup.is_file() or not source.is_file():
        return reconciliation
    rollup_db = sqlite3.connect(f"file:{rollup.resolve().as_posix()}?mode=ro", uri=True)
    source_db = sqlite3.connect(f"file:{source.resolve().as_posix()}?mode=ro", uri=True)
    try:
        rollup_by_date = {
            str(day): int(count)
            for day, count in rollup_db.execute(
                """SELECT substr(hour_utc,1,10), COALESCE(SUM(sample_count),0)
                   FROM hourly_rollups WHERE source_name=? GROUP BY 1""",
                (source.stem,),
            )
        }
        live_by_date = {
            str(day): int(count)
            for day, count in source_db.execute(
                "SELECT substr(observed_utc,1,10), COUNT(*) FROM outcomes GROUP BY 1"
            )
        }
    finally:
        rollup_db.close(); source_db.close()
    archived_by_date = {
        str(day): int(count)
        for day, count in (
            result.get("unique_rows_by_observed_date")
            or result.get("rows_by_observed_date") or {}
        ).items()
        if str(day) != "unpartitioned"
    }
    snapshot_by_date: dict[str, dict[str, Any]] = {}
    if evidence is not None and evidence.is_file():
        evidence_db = sqlite3.connect(
            f"file:{evidence.resolve().as_posix()}?mode=ro", uri=True
        )
        try:
            if evidence_db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='immutable_daily_snapshots'"
            ).fetchone():
                snapshot_by_date = {
                    str(day): {
                        "row_count": int(count), "sha256": str(digest),
                    }
                    for day, count, digest in evidence_db.execute(
                        "SELECT utc_day,row_count,evidence_sha256 "
                        "FROM immutable_daily_snapshots"
                    )
                }
        finally:
            evidence_db.close()
    dates = sorted(
        set(rollup_by_date) | set(live_by_date) | set(archived_by_date)
        | set(snapshot_by_date)
    )
    by_date = [
        {
            "observed_date": day,
            "rollup_rows": rollup_by_date.get(day, 0),
            "archived_rows": archived_by_date.get(day, 0),
            "live_rows": live_by_date.get(day, 0),
            "detail_gap": (
                rollup_by_date.get(day, 0)
                - archived_by_date.get(day, 0)
                - live_by_date.get(day, 0)
            ),
            "frozen_snapshot_rows": (
                snapshot_by_date.get(day, {}).get("row_count")
            ),
            "frozen_snapshot_sha256": (
                snapshot_by_date.get(day, {}).get("sha256")
            ),
            "snapshot_matches_rollup": (
                snapshot_by_date.get(day, {}).get("row_count")
                == rollup_by_date.get(day, 0)
                if day in snapshot_by_date else None
            ),
        }
        for day in dates
    ]
    rollup_count = sum(rollup_by_date.values())
    live_count = sum(live_by_date.values())
    archived_unique_count = sum(archived_by_date.values())
    gap = rollup_count - archived_unique_count - live_count
    positive_gap_dates = [row for row in by_date if row["detail_gap"] > 0]
    rollup_lag_dates = [row for row in by_date if row["detail_gap"] < 0]
    historical_missing_detail_rows = sum(
        int(row["detail_gap"]) for row in positive_gap_dates
    )
    live_detail_ahead_of_rollup_rows = sum(
        -int(row["detail_gap"]) for row in rollup_lag_dates
    )
    reconciliation.update({
        "rollup_sample_count": rollup_count,
        "live_detail_rows": live_count,
        "archived_raw_rows": int(result.get("total_rows") or 0),
        "archived_duplicate_rows": int(result.get("duplicate_rows") or 0),
        "archived_detail_rows": archived_unique_count,
        "detail_gap_vs_rollup": gap,
        "historical_missing_detail_rows": historical_missing_detail_rows,
        "live_detail_ahead_of_rollup_rows": live_detail_ahead_of_rollup_rows,
        "by_observed_date": by_date,
        "nonzero_gap_dates": [row for row in by_date if row["detail_gap"] != 0],
        "positive_gap_dates": positive_gap_dates,
        "rollup_lag_dates": rollup_lag_dates,
        "gap_dates_have_matching_frozen_snapshot": all(
            row["snapshot_matches_rollup"] is True
            for row in positive_gap_dates
        ),
        "interpretation": (
            "exact_detail_reconciled" if not positive_gap_dates and not rollup_lag_dates else
            "live_detail_ahead_of_rollup_expected" if not positive_gap_dates else
            "aggregate_and_predelete_digest_preserved_but_exact_detail_missing"
            if all(
                row["snapshot_matches_rollup"] is True
                for row in positive_gap_dates
            ) else
            "aggregate_rollup_preserved_but_exact_historical_detail_gap_detected"
        ),
    })
    return reconciliation


def run(
    archive: Path = ARCHIVE, state: Path = STATE, report: Path = REPORT,
    rollup: Path = ROLLUP, source: Path = SOURCE,
    evidence: Path | None = EDGE_EVIDENCE,
) -> dict[str, Any]:
    result = audit(archive)
    result["detail_reconciliation"] = reconcile_detail_counts(
        result, rollup=rollup, source=source, evidence=evidence
    )
    if (
        result["status"] != "failed"
        and int(
            result["detail_reconciliation"].get("historical_missing_detail_rows") or 0
        ) > 0
    ):
        result["status"] = "degraded_historical_detail_gap"
    atomic(state, json.dumps(result, indent=2, sort_keys=True))
    lines = [
        "# Shadow archive integrity", "", f"Generated: `{result['generated_utc']}`", "",
        f"- Status: **{result['status']}**",
        f"- Readable Parquet: **{result['readable_parquet_parts']} / {result['parquet_parts']}**",
        f"- Manifest-validated parts: **{result['validated_manifest_parts']} / {result['manifest_parts']}**",
        f"- Legacy readable but unmanifested parts: **{result['legacy_unmanifested_parts']}**",
        f"- Archived raw / unique detail rows: **{result['total_rows']:,} / {result.get('total_unique_rows', result['total_rows']):,}**",
        f"- Exact duplicate archived rows excluded from reconciliation: **{result.get('duplicate_rows', 0):,}**",
        f"- Historical exact-detail gap versus rollups: **{result['detail_reconciliation'].get('detail_gap_vs_rollup')}**",
        f"- Failures: **{len(result['failures'])}**", "",
        "## Detail reconciliation by observed date", "",
        "| Date | Rollup | Archive | Live | Gap | Frozen snapshot |",
        "|---|---:|---:|---:|---:|---:|",
        *[
            f"| {row['observed_date']} | {row['rollup_rows']:,} | {row['archived_rows']:,} | {row['live_rows']:,} | {row['detail_gap']:,} | {row['frozen_snapshot_rows'] if row['frozen_snapshot_rows'] is not None else 'n/a'} |"
            for row in result["detail_reconciliation"].get("by_observed_date") or []
        ], "",
        "This audit is read-only. Legacy parts are not retroactively called certified; all new deletion remains gated by pre-deletion validation.", "",
    ]
    atomic(report, "\n".join(lines))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, default=ARCHIVE)
    parser.add_argument("--state", type=Path, default=STATE)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--rollup", type=Path, default=ROLLUP)
    parser.add_argument("--source", type=Path, default=SOURCE)
    parser.add_argument("--evidence", type=Path, default=EDGE_EVIDENCE)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        result = run(
            args.archive, args.state, args.report, args.rollup, args.source,
            args.evidence,
        )
        if result["status"] == "failed":
            return 1
        if args.interval_sec <= 0.0 or (
            args.duration_sec > 0.0 and time.monotonic() - started >= args.duration_sec
        ):
            return 0
        time.sleep(max(300.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
