#!/usr/bin/env python3
"""Roll up shadow outcomes and archive retained detail to compressed Parquet."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # Detailed rows are never deleted without Parquet support.
    pa = None
    pq = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(1_048_576)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def validate_archive_part(
    path: Path,
    *,
    expected_rows: int,
    expected_columns: set[str],
) -> dict[str, Any]:
    """Verify a Parquet part before any corresponding SQLite deletion."""

    if pq is None or not path.is_file():
        raise ValueError("archive part is unavailable")
    parquet = pq.ParquetFile(path)
    observed_rows = int(parquet.metadata.num_rows)
    observed_columns = set(parquet.schema_arrow.names)
    if observed_rows != int(expected_rows):
        raise ValueError(
            f"archive row count mismatch: expected {expected_rows}, got {observed_rows}"
        )
    if observed_columns != set(expected_columns):
        raise ValueError("archive schema does not match the source rows")
    return {
        "parquet_path": str(path.resolve()),
        "row_count": observed_rows,
        "column_count": len(observed_columns),
        "size_bytes": int(path.stat().st_size),
        "sha256": sha256_file(path),
    }


class ShadowOutcomeCompactor:
    """Maintain compact hourly aggregates before pruning detailed SQLite rows."""

    def __init__(
        self,
        sources: list[Path],
        rollup_database: Path,
        archive_root: Path,
        state_path: Path,
        *,
        retention_days: int = 30,
        batch_size: int = 10000,
        source_busy_timeout_ms: int = 2000,
        archive_pause_sec: float = 0.1,
        max_rollup_batches: int = 8,
        max_archive_batches: int = 8,
    ) -> None:
        self.sources = [Path(path) for path in sources]
        self.rollup_database = Path(rollup_database)
        self.archive_root = Path(archive_root)
        self.state_path = Path(state_path)
        self.retention_days = max(1, int(retention_days))
        self.batch_size = max(100, int(batch_size))
        self.source_busy_timeout_ms = max(100, int(source_busy_timeout_ms))
        self.archive_pause_sec = max(0.0, float(archive_pause_sec))
        self.max_rollup_batches = max(1, int(max_rollup_batches))
        self.max_archive_batches = max(1, int(max_archive_batches))
        self.rollup_database.parent.mkdir(parents=True, exist_ok=True)
        self.archive_root.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.rollup_database, timeout=120.0)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS rollup_meta (
                source_path TEXT PRIMARY KEY,
                last_row_id INTEGER NOT NULL,
                updated_utc TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS hourly_rollups (
                source_name TEXT NOT NULL,
                hour_utc TEXT NOT NULL,
                family TEXT NOT NULL,
                lane_id TEXT NOT NULL,
                model_id TEXT NOT NULL,
                input_timeframe TEXT NOT NULL,
                horizon_sec INTEGER NOT NULL,
                instrument TEXT NOT NULL,
                kind TEXT NOT NULL,
                sample_count INTEGER NOT NULL,
                win_count INTEGER NOT NULL,
                sum_pips REAL NOT NULL,
                sum_sq_pips REAL NOT NULL,
                updated_utc TEXT NOT NULL,
                PRIMARY KEY (
                    source_name, hour_utc, family, lane_id, model_id,
                    input_timeframe, horizon_sec, instrument, kind
                )
            );
            """
        )
        self.connection.commit()

    @staticmethod
    def _source_key(source: Path) -> str:
        return str(source.resolve())

    def _rollup_cursor(self, source: Path) -> int:
        row = self.connection.execute(
            "SELECT last_row_id FROM rollup_meta WHERE source_path = ?",
            (self._source_key(source),),
        ).fetchone()
        return 0 if row is None else int(row[0])

    def rollup_source(self, source: Path) -> dict[str, Any]:
        if not source.is_file():
            return {
                "source": str(source),
                "status": "missing",
                "new_rows": 0,
                "last_row_id": self._rollup_cursor(source),
            }
        cursor = self._rollup_cursor(source)
        new_rows = 0
        rollup_batches = 0
        reader = sqlite3.connect(
            f"file:{source.as_posix()}?mode=ro",
            uri=True,
            timeout=120.0,
        )
        reader.row_factory = sqlite3.Row
        reader.execute("PRAGMA busy_timeout=120000")
        while rollup_batches < self.max_rollup_batches:
            rows = reader.execute(
                """
                SELECT row_id, observed_utc, family, lane_id, model_id,
                       input_timeframe, horizon_sec, instrument, kind,
                       theoretical_pips
                FROM outcomes
                WHERE row_id > ?
                ORDER BY row_id
                LIMIT ?
                """,
                (cursor, self.batch_size),
            ).fetchall()
            if not rows:
                break
            rollup_batches += 1
            aggregates: dict[tuple[Any, ...], list[float]] = defaultdict(
                lambda: [0.0, 0.0, 0.0, 0.0]
            )
            for row in rows:
                cursor = max(cursor, int(row["row_id"]))
                new_rows += 1
                pips = finite(row["theoretical_pips"])
                key = (
                    source.stem,
                    str(row["observed_utc"])[:13] + ":00:00+00:00",
                    str(row["family"]),
                    str(row["lane_id"]),
                    str(row["model_id"]),
                    str(row["input_timeframe"]),
                    int(row["horizon_sec"]),
                    str(row["instrument"]),
                    str(row["kind"]),
                )
                aggregate = aggregates[key]
                aggregate[0] += 1
                aggregate[1] += int(pips > 0.0)
                aggregate[2] += pips
                aggregate[3] += pips * pips
            stamp = utc_now()
            with self.connection:
                self.connection.executemany(
                    """
                    INSERT INTO hourly_rollups VALUES (
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
                    ON CONFLICT (
                        source_name, hour_utc, family, lane_id, model_id,
                        input_timeframe, horizon_sec, instrument, kind
                    ) DO UPDATE SET
                        sample_count=sample_count + excluded.sample_count,
                        win_count=win_count + excluded.win_count,
                        sum_pips=sum_pips + excluded.sum_pips,
                        sum_sq_pips=sum_sq_pips + excluded.sum_sq_pips,
                        updated_utc=excluded.updated_utc
                    """,
                    [
                        (*key, int(values[0]), int(values[1]), values[2], values[3], stamp)
                        for key, values in aggregates.items()
                    ],
                )
                self.connection.execute(
                    """
                    INSERT INTO rollup_meta(source_path, last_row_id, updated_utc)
                    VALUES (?, ?, ?)
                    ON CONFLICT(source_path) DO UPDATE SET
                        last_row_id=excluded.last_row_id,
                        updated_utc=excluded.updated_utc
                    """,
                    (self._source_key(source), cursor, stamp),
                )
        reader.close()
        return {
            "source": str(source.resolve()),
            "status": "rolled_up",
            "new_rows": new_rows,
            "last_row_id": cursor,
            "rollup_batches": rollup_batches,
            "rollup_batch_limit": self.max_rollup_batches,
        }

    def archive_source(self, source: Path, cutoff_utc: str) -> dict[str, Any]:
        result = {
            "archived_rows": 0,
            "archive_parts": 0,
            "validated_archive_parts": 0,
            "manifest_count": 0,
            "parquet_available": bool(pa is not None and pq is not None),
            "cutoff_utc": cutoff_utc,
        }
        if not source.is_file() or pa is None or pq is None:
            result["status"] = (
                "missing" if not source.is_file() else "parquet_unavailable_no_delete"
            )
            return result
        writer = sqlite3.connect(
            source,
            timeout=self.source_busy_timeout_ms / 1000.0,
        )
        writer.row_factory = sqlite3.Row
        writer.execute(f"PRAGMA busy_timeout={self.source_busy_timeout_ms}")
        archive_batches = 0
        lock_deferred = False
        while archive_batches < self.max_archive_batches:
            oldest = writer.execute(
                """
                SELECT substr(observed_utc, 1, 10) AS observed_date
                FROM outcomes
                WHERE observed_utc < ?
                ORDER BY observed_utc, row_id
                LIMIT 1
                """,
                (cutoff_utc,),
            ).fetchone()
            if oldest is None:
                break
            observed_date = str(oldest["observed_date"] or "unknown")
            rows = writer.execute(
                """
                SELECT *
                FROM outcomes
                WHERE observed_utc < ? AND substr(observed_utc, 1, 10) = ?
                ORDER BY row_id
                LIMIT ?
                """,
                (cutoff_utc, observed_date, self.batch_size),
            ).fetchall()
            if not rows:
                break
            first_id = int(rows[0]["row_id"])
            last_id = int(rows[-1]["row_id"])
            partition = self.archive_root / source.stem / observed_date
            partition.mkdir(parents=True, exist_ok=True)
            destination = partition / f"rows_{first_id:012d}_{last_id:012d}.parquet"
            if not destination.is_file():
                temporary = destination.with_suffix(".tmp.parquet")
                table = pa.Table.from_pylist([dict(row) for row in rows])
                pq.write_table(
                    table,
                    temporary,
                    compression="zstd",
                    use_dictionary=True,
                    write_statistics=True,
                )
                os.replace(temporary, destination)
            manifest = destination.with_suffix(".manifest.json")
            try:
                verification = validate_archive_part(
                    destination,
                    expected_rows=len(rows),
                    expected_columns=set(rows[0].keys()),
                )
                manifest_payload = {
                    "schema_version": 1,
                    "created_utc": utc_now(),
                    "source_database": str(source.resolve()),
                    "source_table": "outcomes",
                    "observed_date": observed_date,
                    "cutoff_utc": cutoff_utc,
                    "first_row_id": first_id,
                    "last_row_id": last_id,
                    **verification,
                }
                atomic_json(manifest, manifest_payload)
            except Exception as exc:
                result["status"] = "archive_validation_failed_no_delete"
                result["last_storage_error"] = f"{type(exc).__name__}: {exc}"
                break
            try:
                with writer:
                    deleted = writer.execute(
                        """
                        DELETE FROM outcomes
                        WHERE row_id >= ? AND row_id <= ? AND observed_utc < ?
                          AND substr(observed_utc, 1, 10) = ?
                        """,
                        (first_id, last_id, cutoff_utc, observed_date),
                    ).rowcount
            except sqlite3.OperationalError as exc:
                writer.rollback()
                if "locked" not in str(exc).lower():
                    writer.close()
                    raise
                lock_deferred = True
                result["last_storage_error"] = str(exc)
                break
            result["archived_rows"] += max(0, int(deleted))
            result["archive_parts"] += 1
            result["validated_archive_parts"] += 1
            result["manifest_count"] += int(manifest.is_file())
            result["last_manifest"] = str(manifest.resolve())
            archive_batches += 1
            if self.archive_pause_sec:
                time.sleep(self.archive_pause_sec)
        try:
            writer.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except sqlite3.Error:
            pass
        remaining = int(
            writer.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0]
        )
        page_size = int(writer.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(writer.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = int(writer.execute("PRAGMA freelist_count").fetchone()[0])
        allocated_bytes = page_size * page_count
        reclaimable_bytes = page_size * freelist_count
        reclaimable_ratio = (
            reclaimable_bytes / allocated_bytes if allocated_bytes > 0 else 0.0
        )
        wal_path = Path(str(source) + "-wal")
        result["storage"] = {
            "page_size": page_size,
            "page_count": page_count,
            "freelist_count": freelist_count,
            "allocated_bytes": allocated_bytes,
            "reclaimable_bytes": reclaimable_bytes,
            "reclaimable_ratio": round(reclaimable_ratio, 6),
            "estimated_live_bytes": max(0, allocated_bytes - reclaimable_bytes),
            "wal_bytes": wal_path.stat().st_size if wal_path.is_file() else 0,
            "reclamation_recommended": bool(
                reclaimable_bytes >= 1_073_741_824 or reclaimable_ratio >= 0.20
            ),
            "reclamation_policy": (
                "diagnostic_only; use a separately validated offline VACUUM INTO "
                "or partition migration, never an automatic live VACUUM"
            ),
        }
        writer.close()
        result["remaining_rows"] = remaining
        if result.get("status") == "archive_validation_failed_no_delete":
            pass
        elif lock_deferred:
            result["status"] = "archive_deferred_database_busy"
        elif archive_batches >= self.max_archive_batches:
            result["status"] = "archive_batch_limit_reached"
        else:
            result["status"] = "archived"
        return result

    def run_once(self) -> dict[str, Any]:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=self.retention_days)
        ).isoformat()
        source_states = []
        for source in self.sources:
            rollup = self.rollup_source(source)
            archive = self.archive_source(source, cutoff)
            size_bytes = source.stat().st_size if source.is_file() else 0
            source_states.append(
                {
                    **rollup,
                    **archive,
                    "size_bytes": size_bytes,
                }
            )
        rollup_rows = int(
            self.connection.execute(
                "SELECT COUNT(*) FROM hourly_rollups"
            ).fetchone()[0]
        )
        state = {
            "schema_version": 1,
            "updated_utc": utc_now(),
            "retention_days": self.retention_days,
            "archive_root": str(self.archive_root.resolve()),
            "rollup_database": str(self.rollup_database.resolve()),
            "rollup_rows": rollup_rows,
            "parquet_available": bool(pa is not None and pq is not None),
            "sources": source_states,
            "policy": (
                "roll up every new row; archive detail older than retention; "
                "delete only after a compressed Parquet part exists"
            ),
            "archive_delete_contract": (
                "validated_parquet_same_observed_date_row_id_range_and_cutoff"
            ),
        }
        atomic_json(self.state_path, state)
        return state

    def close(self) -> None:
        self.connection.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", action="append", type=Path, required=True)
    parser.add_argument("--rollup-database", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--retention-days", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--source-busy-timeout-ms", type=int, default=2000)
    parser.add_argument("--archive-pause-sec", type=float, default=0.1)
    parser.add_argument("--max-rollup-batches", type=int, default=8)
    parser.add_argument("--max-archive-batches", type=int, default=8)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    args = parser.parse_args(argv)
    if (
        args.retention_days <= 0
        or args.batch_size <= 0
        or args.source_busy_timeout_ms <= 0
        or args.archive_pause_sec < 0.0
        or args.max_rollup_batches <= 0
        or args.max_archive_batches <= 0
        or args.interval_sec < 0.0
    ):
        raise SystemExit("retention, batch, timeout, and interval values are invalid")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    compactor = ShadowOutcomeCompactor(
        args.source,
        args.rollup_database,
        args.archive_root,
        args.state,
        retention_days=args.retention_days,
        batch_size=args.batch_size,
        source_busy_timeout_ms=args.source_busy_timeout_ms,
        archive_pause_sec=args.archive_pause_sec,
        max_rollup_batches=args.max_rollup_batches,
        max_archive_batches=args.max_archive_batches,
    )
    try:
        while True:
            compactor.run_once()
            if args.interval_sec <= 0.0:
                break
            time.sleep(args.interval_sec)
    finally:
        compactor.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
