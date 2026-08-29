#!/usr/bin/env python3
"""Read-only command-line queries for the durable significant-move catalog."""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta
import json
import math
from pathlib import Path
import sqlite3
import sys
from typing import Any, Sequence


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_DATABASE = SCRIPT_DIR / "data" / "significant_moves" / "significant_moves.sqlite"
TABLE_NAME = "significant_moves"

COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "move_id": ("move_id", "id"),
    "instrument": ("instrument", "pair", "symbol"),
    "base_currency": ("base_currency", "base_ccy", "base"),
    "quote_currency": ("quote_currency", "quote_ccy", "quote"),
    "direction": ("direction", "move_direction", "side"),
    "start": (
        "start_timestamp",
        "start_time",
        "start_ts",
        "window_start",
        "start_time_utc",
    ),
    "end": (
        "end_timestamp",
        "end_time",
        "end_ts",
        "window_end",
        "end_time_utc",
    ),
    "session": ("session", "dominant_session", "primary_session", "start_session"),
    "pips": (
        "absolute_pip_change",
        "abs_pip_change",
        "abs_pips",
        "absolute_pips",
        "pip_change_abs",
        "move_pips",
        "pip_magnitude",
        "signed_pip_change",
        "pip_change",
    ),
    "score": (
        "significance_score",
        "score",
        "severity_score",
        "volatility_adjusted_significance_score",
        "significance",
        "severity",
    ),
}

SIGNED_PIP_COLUMNS = {"signed_pip_change", "pip_change"}


class QueryError(RuntimeError):
    """A user-facing query or catalog error."""


def quote_identifier(value: str) -> str:
    """Quote a SQLite identifier obtained from trusted schema metadata."""

    return '"' + value.replace('"', '""') + '"'


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def nonnegative_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("must be a finite number that is zero or greater")
    return parsed


def normalize_instrument(value: str) -> str:
    normalized = value.strip().upper().replace("/", "_").replace("-", "_")
    if len(normalized) == 6 and "_" not in normalized:
        normalized = f"{normalized[:3]}_{normalized[3:]}"
    if not normalized:
        raise argparse.ArgumentTypeError("instrument must not be empty")
    return normalized


def normalize_currency(value: str) -> str:
    normalized = value.strip().upper()
    if len(normalized) != 3 or not normalized.isalpha():
        raise argparse.ArgumentTypeError("currency must be a three-letter code, such as USD")
    return normalized


def normalize_session(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise argparse.ArgumentTypeError("session must not be empty")
    return normalized


def validate_date_bound(value: str) -> str:
    """Validate an ISO date/datetime while preserving its SQLite-friendly text."""

    candidate = value.strip()
    try:
        if len(candidate) == 10:
            date.fromisoformat(candidate)
        else:
            datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "must be an ISO date or datetime, for example 2026-01-31 or 2026-01-31T12:00:00Z"
        ) from exc
    return candidate


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query the read-only significant-move SQLite catalog.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DATABASE, help="catalog database")
    parser.add_argument("--instrument", type=normalize_instrument, help="exact pair, e.g. EUR_USD")
    parser.add_argument("--currency", type=normalize_currency, help="base or quote currency")
    parser.add_argument(
        "--direction",
        choices=("up", "down", "long", "short"),
        help="move direction; up/long and down/short are treated as synonyms",
    )
    parser.add_argument("--start", type=validate_date_bound, help="earliest move-start date/time")
    parser.add_argument("--end", type=validate_date_bound, help="latest move-start date/time")
    parser.add_argument("--session", type=normalize_session, help="exact session name")
    parser.add_argument("--min-pips", type=nonnegative_float, help="minimum absolute pip movement")
    parser.add_argument("--min-score", type=nonnegative_float, help="minimum significance score")
    parser.add_argument("--top", type=positive_int, default=25, help="maximum rows or groups")
    parser.add_argument(
        "--group-by",
        choices=("none", "instrument", "currency", "year", "month", "session"),
        default="none",
        help="aggregate matched moves before output",
    )
    parser.add_argument(
        "--output",
        choices=("table", "csv", "json"),
        default="table",
        help="stdout format",
    )
    return parser.parse_args(argv)


def open_catalog(path: Path) -> sqlite3.Connection:
    path = path.expanduser().resolve()
    if not path.is_file():
        raise QueryError(f"catalog database does not exist: {path}")
    try:
        connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        connection.execute("PRAGMA query_only = ON")
    except sqlite3.Error as exc:
        raise QueryError(f"cannot open catalog database {path}: {exc}") from exc
    return connection


def catalog_columns(connection: sqlite3.Connection) -> list[str]:
    try:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (TABLE_NAME,),
        ).fetchone()
        if exists is None:
            raise QueryError(f"database has no {TABLE_NAME!r} table")
        rows = connection.execute(f"PRAGMA table_info({quote_identifier(TABLE_NAME)})").fetchall()
    except sqlite3.Error as exc:
        raise QueryError(f"cannot inspect {TABLE_NAME!r}: {exc}") from exc
    columns = [str(row[1]) for row in rows]
    if not columns:
        raise QueryError(f"table {TABLE_NAME!r} has no columns")
    return columns


def resolve_columns(columns: Sequence[str]) -> dict[str, str | None]:
    by_lower = {column.lower(): column for column in columns}
    return {
        role: next((by_lower[name.lower()] for name in aliases if name.lower() in by_lower), None)
        for role, aliases in COLUMN_ALIASES.items()
    }


def required_column(schema: dict[str, str | None], role: str, option: str) -> str:
    column = schema.get(role)
    if column is None:
        aliases = ", ".join(COLUMN_ALIASES[role])
        raise QueryError(f"{option} requires a catalog column matching one of: {aliases}")
    return column


def pip_expression(schema: dict[str, str | None], prefix: str = "") -> str:
    column = required_column(schema, "pips", "pip filtering or ranking")
    expression = f"{prefix}{quote_identifier(column)}"
    if column.lower() in SIGNED_PIP_COLUMNS:
        return f"ABS({expression})"
    return expression


def plain_expression(schema: dict[str, str | None], role: str, prefix: str = "") -> str:
    column = schema.get(role)
    return "NULL" if column is None else f"{prefix}{quote_identifier(column)}"


def build_filters(
    args: argparse.Namespace,
    schema: dict[str, str | None],
) -> tuple[list[str], list[Any]]:
    clauses: list[str] = []
    parameters: list[Any] = []

    if args.instrument:
        column = required_column(schema, "instrument", "--instrument")
        clauses.append(
            f"UPPER(REPLACE(REPLACE({quote_identifier(column)}, '/', '_'), '-', '_')) = ?"
        )
        parameters.append(args.instrument)

    if args.currency:
        currency_terms: list[str] = []
        instrument = schema.get("instrument")
        normalized_instrument = (
            f"REPLACE(REPLACE(UPPER({quote_identifier(instrument)}), '/', '_'), '-', '_')"
            if instrument
            else None
        )
        if schema.get("base_currency"):
            currency_terms.append(f"UPPER({quote_identifier(schema['base_currency'])}) = ?")
        elif normalized_instrument:
            currency_terms.append(f"SUBSTR({normalized_instrument}, 1, 3) = ?")
        if schema.get("quote_currency"):
            currency_terms.append(f"UPPER({quote_identifier(schema['quote_currency'])}) = ?")
        elif normalized_instrument:
            currency_terms.append(f"SUBSTR({normalized_instrument}, -3, 3) = ?")
        if currency_terms:
            clauses.append("(" + " OR ".join(currency_terms) + ")")
            parameters.extend([args.currency] * len(currency_terms))
        else:
            required_column(schema, "instrument", "--currency")
            raise QueryError("--currency requires base/quote currency or instrument columns")

    if args.direction:
        column = required_column(schema, "direction", "--direction")
        synonyms = ("up", "long") if args.direction in {"up", "long"} else ("down", "short")
        clauses.append(f"LOWER({quote_identifier(column)}) IN (?, ?)")
        parameters.extend(synonyms)

    if args.start:
        column = required_column(schema, "start", "--start")
        clauses.append(f"JULIANDAY({quote_identifier(column)}) >= JULIANDAY(?)")
        parameters.append(args.start)

    if args.end:
        column = required_column(schema, "start", "--end")
        if len(args.end) == 10:
            exclusive_end = date.fromisoformat(args.end) + timedelta(days=1)
            clauses.append(f"JULIANDAY({quote_identifier(column)}) < JULIANDAY(?)")
            parameters.append(exclusive_end.isoformat())
        else:
            clauses.append(f"JULIANDAY({quote_identifier(column)}) <= JULIANDAY(?)")
            parameters.append(args.end)

    if args.session:
        column = required_column(schema, "session", "--session")
        clauses.append(f"LOWER({quote_identifier(column)}) = LOWER(?)")
        parameters.append(args.session)

    if args.min_pips is not None:
        clauses.append(f"{pip_expression(schema)} >= ?")
        parameters.append(args.min_pips)

    if args.min_score is not None:
        column = required_column(schema, "score", "--min-score")
        clauses.append(f"{quote_identifier(column)} >= ?")
        parameters.append(args.min_score)

    return clauses, parameters


def aggregate_columns(schema: dict[str, str | None], prefix: str = "") -> str:
    start = plain_expression(schema, "start", prefix)
    end = plain_expression(schema, "end", prefix)
    pips = "NULL" if schema.get("pips") is None else pip_expression(schema, prefix)
    score = plain_expression(schema, "score", prefix)
    return (
        "COUNT(*) AS move_count, "
        f"MIN({start}) AS first_start, "
        f"MAX({end}) AS last_end, "
        f"ROUND(AVG({pips}), 3) AS average_abs_pips, "
        f"ROUND(MAX({pips}), 3) AS max_abs_pips, "
        f"ROUND(AVG({score}), 4) AS average_score, "
        f"ROUND(MAX({score}), 4) AS max_score"
    )


def build_query(
    args: argparse.Namespace,
    schema: dict[str, str | None],
) -> tuple[str, list[Any]]:
    clauses, parameters = build_filters(args, schema)
    where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
    table = quote_identifier(TABLE_NAME)

    if args.group_by == "none":
        order_terms: list[str] = []
        if schema.get("score"):
            order_terms.append(f"{quote_identifier(schema['score'])} DESC")
        if schema.get("pips"):
            order_terms.append(f"{pip_expression(schema)} DESC")
        if schema.get("start"):
            order_terms.append(f"{quote_identifier(schema['start'])} DESC")
        order_sql = " ORDER BY " + ", ".join(order_terms) if order_terms else ""
        return f"SELECT * FROM {table}{where_sql}{order_sql} LIMIT ?", [*parameters, args.top]

    filtered = f"WITH filtered AS (SELECT * FROM {table}{where_sql})"

    if args.group_by == "currency":
        base = schema.get("base_currency")
        quote = schema.get("quote_currency")
        instrument = schema.get("instrument")
        if base and quote:
            base_expression = f"UPPER({quote_identifier(base)})"
            quote_expression = f"UPPER({quote_identifier(quote)})"
        elif instrument:
            normalized = (
                f"REPLACE(REPLACE(UPPER({quote_identifier(instrument)}), '/', '_'), '-', '_')"
            )
            base_expression = f"SUBSTR({normalized}, 1, 3)"
            quote_expression = f"SUBSTR({normalized}, -3, 3)"
        else:
            raise QueryError("--group-by currency requires base/quote currency or instrument columns")

        start = plain_expression(schema, "start")
        end = plain_expression(schema, "end")
        pips = "NULL" if schema.get("pips") is None else pip_expression(schema)
        score = plain_expression(schema, "score")
        payload = (
            f"{start} AS start_value, {end} AS end_value, "
            f"{pips} AS pips_value, {score} AS score_value"
        )
        sql = (
            f"{filtered}, currency_rows AS ("
            f"SELECT {base_expression} AS currency, {payload} FROM filtered UNION ALL "
            f"SELECT {quote_expression} AS currency, {payload} FROM filtered"
            ") "
            "SELECT currency, COUNT(*) AS move_count, "
            "MIN(start_value) AS first_start, MAX(end_value) AS last_end, "
            "ROUND(AVG(pips_value), 3) AS average_abs_pips, "
            "ROUND(MAX(pips_value), 3) AS max_abs_pips, "
            "ROUND(AVG(score_value), 4) AS average_score, "
            "ROUND(MAX(score_value), 4) AS max_score "
            "FROM currency_rows WHERE currency IS NOT NULL AND currency <> '' "
            "GROUP BY currency ORDER BY move_count DESC, max_score DESC, max_abs_pips DESC LIMIT ?"
        )
        return sql, [*parameters, args.top]

    if args.group_by == "instrument":
        column = required_column(schema, "instrument", "--group-by instrument")
        group_expression = (
            f"REPLACE(REPLACE(UPPER({quote_identifier(column)}), '/', '_'), '-', '_')"
        )
        group_alias = "instrument"
    elif args.group_by == "session":
        column = required_column(schema, "session", "--group-by session")
        group_expression = f"LOWER({quote_identifier(column)})"
        group_alias = "session"
    elif args.group_by == "year":
        column = required_column(schema, "start", "--group-by year")
        group_expression = f"STRFTIME('%Y', {quote_identifier(column)})"
        group_alias = "year"
    elif args.group_by == "month":
        column = required_column(schema, "start", "--group-by month")
        group_expression = f"STRFTIME('%Y-%m', {quote_identifier(column)})"
        group_alias = "month"
    else:  # argparse choices make this defensive branch unreachable.
        raise QueryError(f"unsupported grouping: {args.group_by}")

    sql = (
        f"{filtered} SELECT {group_expression} AS {quote_identifier(group_alias)}, "
        f"{aggregate_columns(schema)} FROM filtered "
        f"WHERE {group_expression} IS NOT NULL AND {group_expression} <> '' "
        f"GROUP BY {group_expression} "
        "ORDER BY move_count DESC, max_score DESC, max_abs_pips DESC LIMIT ?"
    )
    return sql, [*parameters, args.top]


def stringify(value: Any, max_width: int = 40) -> str:
    if value is None:
        rendered = ""
    elif isinstance(value, float):
        rendered = format(value, ".10g")
    elif isinstance(value, bytes):
        rendered = value.hex()
    else:
        rendered = str(value)
    rendered = rendered.replace("\r", " ").replace("\n", " ")
    if len(rendered) > max_width:
        return rendered[: max_width - 3] + "..."
    return rendered


def compact_ungrouped_table(
    headers: Sequence[str], rows: Sequence[Sequence[Any]]
) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Keep interactive output readable; CSV and JSON still expose all fields."""
    preferred = [
        "move_id",
        "instrument",
        "start_timestamp",
        "end_timestamp",
        "horizon_minutes",
        "direction",
        "absolute_pip_change",
        "absolute_percentage_return",
        "atr_normalized_move",
        "path_efficiency_ratio",
        "net_move_after_cost_pips",
        "significance_score",
        "start_session",
    ]
    lookup = {header.lower(): index for index, header in enumerate(headers)}
    selected = [(name, lookup[name]) for name in preferred if name in lookup]
    if not selected:
        return list(headers), [tuple(row) for row in rows]
    compact_headers = [name for name, _ in selected]
    compact_rows = [tuple(row[index] for _, index in selected) for row in rows]
    return compact_headers, compact_rows


def write_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    if not rows:
        print("No matching moves.")
        return
    rendered_rows = [[stringify(value) for value in row] for row in rows]
    widths = [
        min(40, max(len(str(header)), *(len(row[index]) for row in rendered_rows)))
        for index, header in enumerate(headers)
    ]

    def line(values: Sequence[str]) -> str:
        return " | ".join(value.ljust(widths[index]) for index, value in enumerate(values))

    print(line([str(header) for header in headers]))
    print("-+-".join("-" * width for width in widths))
    for row in rendered_rows:
        print(line(row))


def write_csv(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(headers)
    writer.writerows(rows)


def json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.hex()
    return value


def write_json(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    records = [
        {header: json_value(row[index]) for index, header in enumerate(headers)} for row in rows
    ]
    json.dump(records, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")


def run(args: argparse.Namespace) -> int:
    connection = open_catalog(args.db)
    try:
        schema = resolve_columns(catalog_columns(connection))
        sql, parameters = build_query(args, schema)
        try:
            cursor = connection.execute(sql, parameters)
            headers = [str(item[0]) for item in cursor.description]
            rows = cursor.fetchall()
        except sqlite3.Error as exc:
            raise QueryError(f"query failed: {exc}") from exc
    finally:
        connection.close()

    if args.output == "csv":
        write_csv(headers, rows)
    elif args.output == "json":
        write_json(headers, rows)
    else:
        if args.group_by == "none":
            headers, rows = compact_ungrouped_table(headers, rows)
        write_table(headers, rows)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return run(parse_args(argv))
    except QueryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except BrokenPipeError:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
