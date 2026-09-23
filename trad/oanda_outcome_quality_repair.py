"""Audit and quarantine OANDA outcomes scored from stale target quotes."""

from __future__ import annotations

import argparse
import json
import sqlite3
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=60.0)
    connection.execute("PRAGMA busy_timeout = 60000")
    return connection


def shadow_invalid_where() -> str:
    return """
        observed_utc >= ?
        AND (
            julianday(entry_time) + horizon_sec / 86400.0
            - julianday(exit_time)
        ) * 86400.0 > ?
    """


def combination_invalid_where() -> str:
    return """
        snapshots.snapshot_id >= ?
        AND snapshots.created_utc >= ?
        AND outcomes.snapshot_id = snapshots.snapshot_id
        AND (
            julianday(snapshots.created_utc) + outcomes.horizon_sec / 86400.0
            - julianday(outcomes.outcome_time)
        ) * 86400.0 > ?
    """


def snapshot_start_key(start_utc: str) -> str:
    parsed = datetime.fromisoformat(start_utc.replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S")


def audit_shadow(path: Path, start_utc: str, max_gap_sec: float) -> dict[str, Any]:
    connection = connect(path)
    rows = connection.execute(
        f"""
        SELECT kind, family, COUNT(*)
        FROM outcomes INDEXED BY shadow_outcomes_time
        WHERE {shadow_invalid_where()}
        GROUP BY kind, family
        ORDER BY kind, family
        """,
        (start_utc, max_gap_sec),
    ).fetchall()
    connection.close()
    return {
        "count": sum(int(row[2]) for row in rows),
        "groups": [
            {"kind": str(kind), "family": str(family), "count": int(count)}
            for kind, family, count in rows
        ],
    }


def audit_combination(path: Path, start_utc: str, max_gap_sec: float) -> int:
    connection = connect(path)
    count = int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM snapshots AS snapshots INDEXED BY sqlite_autoindex_snapshots_1
            CROSS JOIN outcomes AS outcomes INDEXED BY sqlite_autoindex_outcomes_1
            WHERE {combination_invalid_where()}
            """,
            (snapshot_start_key(start_utc), start_utc, max_gap_sec),
        ).fetchone()[0]
    )
    connection.close()
    return count


def quarantine_shadow(
    source: Path,
    quarantine: Path,
    start_utc: str,
    max_gap_sec: float,
    repaired_utc: str,
) -> int:
    connection = connect(source)
    connection.execute("ATTACH DATABASE ? AS quarantine", (str(quarantine),))
    connection.execute("BEGIN IMMEDIATE")
    connection.execute(
        f"""
        CREATE TABLE quarantine.shadow_outcomes AS
        SELECT outcomes.*,
               (
                   julianday(entry_time) + horizon_sec / 86400.0
                   - julianday(exit_time)
               ) * 86400.0 AS target_quote_gap_sec,
               'target_quote_stale_or_nontradeable' AS repair_reason,
               ? AS quarantined_utc
        FROM outcomes INDEXED BY shadow_outcomes_time
        WHERE {shadow_invalid_where()}
        """,
        (repaired_utc, start_utc, max_gap_sec),
    )
    connection.execute(
        "CREATE UNIQUE INDEX quarantine.shadow_outcome_key "
        "ON shadow_outcomes(event_id, horizon_sec)"
    )
    count = int(
        connection.execute(
            "SELECT COUNT(*) FROM quarantine.shadow_outcomes"
        ).fetchone()[0]
    )
    connection.execute(
        "DELETE FROM outcomes WHERE row_id IN "
        "(SELECT row_id FROM quarantine.shadow_outcomes)"
    )
    connection.commit()
    connection.close()
    return count


def delete_matching_outcomes(path: Path, quarantine: Path) -> int:
    connection = connect(path)
    connection.execute("ATTACH DATABASE ? AS quarantine", (str(quarantine),))
    row_ids = [
        int(row[0])
        for row in connection.execute(
            """
            SELECT outcomes.row_id
            FROM quarantine.shadow_outcomes AS invalid NOT INDEXED
            CROSS JOIN outcomes AS outcomes INDEXED BY sqlite_autoindex_outcomes_1
            WHERE outcomes.event_id = invalid.event_id
              AND outcomes.horizon_sec = invalid.horizon_sec
            """
        )
    ]
    if not row_ids:
        connection.close()
        return 0
    connection.execute("BEGIN IMMEDIATE")
    for offset in range(0, len(row_ids), 500):
        batch = row_ids[offset : offset + 500]
        placeholders = ",".join("?" for _ in batch)
        connection.execute(
            f"DELETE FROM outcomes WHERE row_id IN ({placeholders})",
            batch,
        )
    connection.commit()
    connection.close()
    return len(row_ids)


def quarantine_combination(
    path: Path,
    quarantine: Path,
    start_utc: str,
    max_gap_sec: float,
    repaired_utc: str,
) -> int:
    connection = connect(path)
    connection.execute("ATTACH DATABASE ? AS quarantine", (str(quarantine),))
    connection.execute("BEGIN IMMEDIATE")
    table_exists = bool(
        connection.execute(
            "SELECT 1 FROM quarantine.sqlite_master "
            "WHERE type = 'table' AND name = 'combination_outcomes'"
        ).fetchone()
    )
    if not table_exists:
        connection.execute(
            f"""
            CREATE TABLE quarantine.combination_outcomes AS
            SELECT outcomes.*,
                   snapshots.origin_time,
                   snapshots.created_utc,
                   (
                       julianday(snapshots.created_utc)
                       + outcomes.horizon_sec / 86400.0
                       - julianday(outcomes.outcome_time)
                   ) * 86400.0 AS target_quote_gap_sec,
                   'target_quote_stale_or_nontradeable' AS repair_reason,
                   ? AS quarantined_utc
            FROM snapshots AS snapshots INDEXED BY sqlite_autoindex_snapshots_1
            CROSS JOIN outcomes AS outcomes INDEXED BY sqlite_autoindex_outcomes_1
            WHERE {combination_invalid_where()}
            """,
            (
                repaired_utc,
                snapshot_start_key(start_utc),
                start_utc,
                max_gap_sec,
            ),
        )
    count = int(
        connection.execute(
            "SELECT COUNT(*) FROM quarantine.combination_outcomes"
        ).fetchone()[0]
    )
    connection.execute(
        "DELETE FROM outcomes WHERE row_id IN "
        "(SELECT row_id FROM quarantine.combination_outcomes)"
    )
    connection.commit()
    connection.close()
    return count


def archive_derived_database(path: Path, quarantine: Path) -> dict[str, Any]:
    destination = quarantine.with_name(
        f"{quarantine.stem}_{path.stem}{path.suffix}"
    )
    if destination.exists():
        return {
            "archived": str(destination),
            "bytes": destination.stat().st_size,
            "already_archived": True,
        }
    size = path.stat().st_size if path.exists() else 0
    if path.exists():
        shutil.move(str(path), str(destination))
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(path) + suffix)
        if sidecar.exists():
            shutil.move(str(sidecar), str(destination) + suffix)
    return {
        "archived": str(destination),
        "bytes": size,
        "already_archived": False,
    }


def parse_args() -> argparse.Namespace:
    default_root = Path(__file__).resolve().parent / "data" / "oanda_training_manager"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=default_root)
    parser.add_argument("--start-utc", required=True)
    parser.add_argument("--max-target-quote-gap-sec", type=float, default=30.0)
    parser.add_argument("--quarantine", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.data_root.resolve()
    state = root / "state"
    source = state / "strategy_shadow_outcomes_v1.sqlite"
    combination = state / "signal_combination_audit_v1.sqlite"
    repaired_utc = utc_now()
    audit = {
        "schema_version": 1,
        "generated_utc": repaired_utc,
        "mode": "apply" if args.apply else "audit",
        "start_utc": args.start_utc,
        "max_target_quote_gap_sec": args.max_target_quote_gap_sec,
        "source": str(source),
        "shadow": audit_shadow(
            source, args.start_utc, args.max_target_quote_gap_sec
        ),
        "combination_count": audit_combination(
            combination, args.start_utc, args.max_target_quote_gap_sec
        ),
    }
    if args.apply:
        quarantine = (
            args.quarantine
            or root
            / "_archive"
            / f"outcome_quality_quarantine_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sqlite"
        ).resolve()
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        if quarantine.exists() and not args.resume:
            raise SystemExit(f"Refusing to overwrite quarantine: {quarantine}")
        audit["quarantine"] = str(quarantine)
        if quarantine.exists():
            quarantine_connection = connect(quarantine)
            shadow_quarantined = int(
                quarantine_connection.execute(
                    "SELECT COUNT(*) FROM shadow_outcomes"
                ).fetchone()[0]
            )
            quarantine_connection.close()
        else:
            shadow_quarantined = quarantine_shadow(
                source,
                quarantine,
                args.start_utc,
                args.max_target_quote_gap_sec,
                repaired_utc,
            )
        audit["applied"] = {
            "shadow_quarantined": shadow_quarantined,
            "promotion_deleted": delete_matching_outcomes(
                state / "lane_promotion_v1.sqlite", quarantine
            ),
            "exit_fit_deleted": delete_matching_outcomes(
                state / "strategy_exit_fit_v1.sqlite", quarantine
            ),
            "combination_quarantined": quarantine_combination(
                combination,
                quarantine,
                args.start_utc,
                args.max_target_quote_gap_sec,
                repaired_utc,
            ),
            "rollups_reset": archive_derived_database(
                state / "shadow_outcome_rollups_v1.sqlite", quarantine
            ),
            "calibration_reset": archive_derived_database(
                state / "timeframe_matrix_calibration_v1.sqlite", quarantine
            ),
        }
        audit["post_apply"] = {
            "shadow": audit_shadow(
                source, args.start_utc, args.max_target_quote_gap_sec
            ),
            "combination_count": audit_combination(
                combination, args.start_utc, args.max_target_quote_gap_sec
            ),
        }
    report = args.report or root / "reports" / "outcome_quality_repair_latest.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(audit, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
