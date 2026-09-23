#!/usr/bin/env python3
"""Merge isolated read-only monitor databases without losing newer columns."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Iterable

try:
    from .practice_007_forecast_outcome_monitor import connect_database
except ImportError:
    from practice_007_forecast_outcome_monitor import connect_database


MONITOR_TABLES = (
    "monitor_samples",
    "quote_samples",
    "technical_samples",
    "signal_rows",
    "account_positions",
    "coverage_samples",
    "forecasts",
)


def table_columns(
    connection: sqlite3.Connection,
    table: str,
) -> list[str]:
    return [
        str(row[1])
        for row in connection.execute(f'PRAGMA table_info("{table}")')
    ]


def merge_database(
    target: sqlite3.Connection,
    source_path: Path,
    batch_size: int = 10_000,
) -> dict[str, int]:
    source = sqlite3.connect(source_path, timeout=30.0)
    try:
        source_columns = {
            table: table_columns(source, table) for table in MONITOR_TABLES
        }
        merged: dict[str, int] = {}
        for table in MONITOR_TABLES:
            available = source_columns[table]
            if not available:
                merged[table] = 0
                continue
            target_available = set(table_columns(target, table))
            columns = [
                column for column in available if column in target_available
            ]
            quoted_columns = ", ".join(f'"{column}"' for column in columns)
            placeholders = ", ".join("?" for _ in columns)
            insert_sql = (
                f'INSERT OR IGNORE INTO "{table}" ({quoted_columns}) '
                f"VALUES ({placeholders})"
            )
            cursor = source.execute(
                f'SELECT {quoted_columns} FROM "{table}"'
            )
            before = target.total_changes
            while rows := cursor.fetchmany(batch_size):
                target.executemany(insert_sql, rows)
            merged[table] = target.total_changes - before
        target.commit()
        return merged
    finally:
        source.close()


def merge_monitor_databases(
    source_paths: Iterable[Path],
    output_path: Path,
) -> dict[str, object]:
    sources = [
        path.resolve()
        for path in source_paths
        if path.exists() and path.resolve() != output_path.resolve()
    ]
    if output_path.exists():
        raise FileExistsError(f"Output database already exists: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    target = connect_database(output_path)
    try:
        source_results = []
        for source in sources:
            source_results.append(
                {
                    "source": str(source),
                    "inserted": merge_database(target, source),
                }
            )
        totals = {
            table: target.execute(
                f'SELECT COUNT(*) FROM "{table}"'
            ).fetchone()[0]
            for table in MONITOR_TABLES
        }
        target.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return {
            "output": str(output_path.resolve()),
            "source_count": len(sources),
            "sources": source_results,
            "totals": totals,
        }
    finally:
        target.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--source",
        type=Path,
        action="append",
        required=True,
        help="Source SQLite path; repeat for each monitor segment.",
    )
    parser.add_argument("--summary-json", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = merge_monitor_databases(args.source, args.output)
    rendered = json.dumps(summary, indent=2, sort_keys=True)
    if args.summary_json:
        args.summary_json.parent.mkdir(parents=True, exist_ok=True)
        args.summary_json.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
