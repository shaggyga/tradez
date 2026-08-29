#!/usr/bin/env python3
"""Compile ten uncovered movement-first FX cases under one causal-time contract.

This is a retrospective research diagnostic.  It never turns an explanation
into a forecast, never changes a lifecycle state, and has no broker surface.
The compiler binds hand-verified source clocks to the immutable unmatched
factor-episode queue and records whether the source was actually knowable
before, during, or only after the hindsight-selected movement interval.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "uncovered_factor_case_batch_v1.json"
DATABASE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "spike_blurb_factor_reconstruction_v1.sqlite"
)
REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "major_move_case_audits"
    / "ten_uncovered_factor_cases_v1"
)
OUTPUT_JSON = REPORT_ROOT / "TEN_UNCOVERED_FACTOR_CASES_V1.json"
OUTPUT_MD = REPORT_ROOT / "TEN_UNCOVERED_FACTOR_CASES_V1.md"
OUTPUT_LOG = REPORT_ROOT / "TEN_UNCOVERED_FACTOR_CASE_LOG_V1.jsonl"
CONTRACT_ID = "uncovered_factor_case_batch_v1_20260821"
UPSTREAM_CONTRACT_ID = "spike_blurb_unmatched_episode_queue_v2_20260820"
UTC = dt.timezone.utc

EXACT_CLOCK_QUALITIES = {
    "official_exact_release_clock",
    "official_exact_event_clock",
    "provider_exact_release_clock",
    "reputable_secondary_exact_publication_clock",
}


class CaseAuditError(RuntimeError):
    """Raised when a retrospective case could be misrepresented."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_utc(value: Any, *, allow_none: bool = False) -> dt.datetime | None:
    if value is None and allow_none:
        return None
    text = str(value or "").strip()
    if not text:
        if allow_none:
            return None
        raise CaseAuditError("timestamp_missing")
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CaseAuditError(f"timestamp_invalid:{text}") from exc
    if parsed.tzinfo is None:
        raise CaseAuditError(f"timestamp_timezone_missing:{text}")
    return parsed.astimezone(UTC)


def iso_utc(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def relative_timing_state(
    clock: dt.datetime | None,
    start: dt.datetime,
    end: dt.datetime,
) -> str:
    if clock is None:
        return "clock_unverified"
    if clock < start:
        return "known_before_selected_interval"
    if clock == start:
        return "known_at_selected_start"
    if clock <= end:
        return "known_during_selected_interval"
    return "known_after_selected_interval"


def _required_keys(value: Mapping[str, Any], required: set[str], label: str) -> None:
    missing = required - set(value)
    if missing:
        raise CaseAuditError(f"{label}_keys_missing:{','.join(sorted(missing))}")


def validate_config(config: Mapping[str, Any]) -> None:
    _required_keys(
        config,
        {
            "schema_version",
            "contract_id",
            "upstream_contract_id",
            "purpose",
            "research_only",
            "execution_eligible",
            "logic_upgrades",
            "cases",
        },
        "config",
    )
    if config["schema_version"] != 1 or config["contract_id"] != CONTRACT_ID:
        raise CaseAuditError("config_identity_mismatch")
    if config["upstream_contract_id"] != UPSTREAM_CONTRACT_ID:
        raise CaseAuditError("upstream_contract_mismatch")
    if config["research_only"] is not True or config["execution_eligible"] is not False:
        raise CaseAuditError("config_must_remain_inert")
    upgrades = config["logic_upgrades"]
    cases = config["cases"]
    if not isinstance(upgrades, list) or not isinstance(cases, list):
        raise CaseAuditError("config_collections_invalid")
    if len(cases) != 10:
        raise CaseAuditError(f"exactly_ten_cases_required:{len(cases)}")
    upgrade_ids = [str(row.get("upgrade_id") or "") for row in upgrades]
    if "" in upgrade_ids or len(upgrade_ids) != len(set(upgrade_ids)):
        raise CaseAuditError("logic_upgrade_ids_invalid")
    case_ids: list[str] = []
    episode_ids: list[str] = []
    for case in cases:
        if not isinstance(case, Mapping):
            raise CaseAuditError("case_not_object")
        _required_keys(
            case,
            {
                "case_id",
                "episode_id",
                "event_family",
                "source_hypothesis",
                "observed_factor_key",
                "primary_clock_id",
                "source_clocks",
                "measurable_facts",
                "attribution_state",
                "missing_evidence",
                "logic_upgrade_ids",
                "case_note",
            },
            "case",
        )
        case_id = str(case["case_id"])
        episode_id = str(case["episode_id"])
        if not case_id or not episode_id:
            raise CaseAuditError("case_identity_empty")
        case_ids.append(case_id)
        episode_ids.append(episode_id)
        clocks = case["source_clocks"]
        if not isinstance(clocks, list) or not clocks:
            raise CaseAuditError(f"source_clocks_empty:{case_id}")
        clock_ids: list[str] = []
        for clock in clocks:
            _required_keys(
                clock,
                {
                    "clock_id",
                    "at_utc",
                    "timestamp_quality",
                    "source_role",
                    "source_url",
                    "directional_use_state",
                },
                "clock",
            )
            clock_ids.append(str(clock["clock_id"]))
            parse_utc(clock["at_utc"], allow_none=True)
            if clock["timestamp_quality"] not in EXACT_CLOCK_QUALITIES | {
                "date_only_primary",
                "reported_original_clock_unverified",
            }:
                raise CaseAuditError(f"clock_quality_invalid:{case_id}")
            if not str(clock["source_url"]).startswith("https://"):
                raise CaseAuditError(f"clock_url_invalid:{case_id}")
        if len(clock_ids) != len(set(clock_ids)):
            raise CaseAuditError(f"clock_ids_duplicate:{case_id}")
        if str(case["primary_clock_id"]) not in set(clock_ids):
            raise CaseAuditError(f"primary_clock_missing:{case_id}")
        unknown = set(case["logic_upgrade_ids"]) - set(upgrade_ids)
        if unknown:
            raise CaseAuditError(f"unknown_logic_upgrade:{case_id}:{sorted(unknown)}")
    if len(case_ids) != len(set(case_ids)):
        raise CaseAuditError("case_ids_duplicate")
    if len(episode_ids) != len(set(episode_ids)):
        raise CaseAuditError("episode_ids_duplicate")


def _load_episode(connection: sqlite3.Connection, episode_id: str) -> dict[str, Any]:
    row = connection.execute(
        "SELECT * FROM unmatched_research_episodes WHERE episode_id=?",
        (episode_id,),
    ).fetchone()
    if row is None:
        raise CaseAuditError(f"episode_missing:{episode_id}")
    return dict(row)


def _compile_case(case: Mapping[str, Any], episode: Mapping[str, Any]) -> dict[str, Any]:
    case_id = str(case["case_id"])
    if episode["contract_id"] != UPSTREAM_CONTRACT_ID:
        raise CaseAuditError(f"episode_contract_mismatch:{case_id}")
    if episode["causal_driver_state"] != "unresolved_do_not_force":
        raise CaseAuditError(f"episode_already_resolved:{case_id}")
    if episode["research_only"] != 1 or episode["execution_eligible"] != 0:
        raise CaseAuditError(f"episode_not_inert:{case_id}")
    if str(episode["hindsight_factor_key"]) != str(case["observed_factor_key"]):
        raise CaseAuditError(f"observed_factor_mismatch:{case_id}")
    start = parse_utc(episode["representative_start_utc"])
    end = parse_utc(episode["representative_end_utc"])
    assert start is not None and end is not None
    if end <= start:
        raise CaseAuditError(f"episode_chronology_invalid:{case_id}")

    clock_rows: list[dict[str, Any]] = []
    for raw in case["source_clocks"]:
        at = parse_utc(raw["at_utc"], allow_none=True)
        exact = bool(at is not None and raw["timestamp_quality"] in EXACT_CLOCK_QUALITIES)
        delta = None if at is None else (at - start).total_seconds() / 60.0
        clock_rows.append(
            {
                **dict(raw),
                "at_utc": None if at is None else iso_utc(at),
                "exact_clock_eligible": exact,
                "timing_state": relative_timing_state(at, start, end),
                "minutes_from_selected_start": None if delta is None else round(delta, 3),
            }
        )
    primary = next(row for row in clock_rows if row["clock_id"] == case["primary_clock_id"])
    exact_clocks = [
        parse_utc(row["at_utc"])
        for row in clock_rows
        if row["exact_clock_eligible"] and row["at_utc"] is not None
    ]
    exact_values = [value for value in exact_clocks if value is not None]
    earliest = min(exact_values) if exact_values else None
    earliest_state = relative_timing_state(earliest, start, end)
    pre_source_minutes = (
        max(0.0, (earliest - start).total_seconds() / 60.0)
        if earliest is not None
        else None
    )
    source_owns_onset = earliest is not None and earliest <= start
    source_inside = earliest is not None and start < earliest <= end
    return {
        "case_id": case_id,
        "contract_id": CONTRACT_ID,
        "episode_id": episode["episode_id"],
        "upstream_priority_rank": int(episode["priority_rank"]),
        "market_episode_15m": episode["market_episode_15m"],
        "representative_move_id": episode["representative_move_id"],
        "representative_instrument": episode["representative_instrument"],
        "representative_start_utc": iso_utc(start),
        "representative_end_utc": iso_utc(end),
        "representative_best_direction": episode["representative_best_direction"],
        "representative_net_pips": round(float(episode["representative_net_pips"]), 6),
        "representative_movement_bps": round(
            float(episode["representative_movement_bps"]), 6
        ),
        "representative_spread_pips": round(
            float(episode["representative_spread_pips"]), 6
        ),
        "liquidity_bucket": episode["liquidity_bucket"],
        "observed_hindsight_factor": episode["hindsight_factor_key"],
        "factor_breadth_instruments": int(episode["instrument_count"]),
        "factor_member_jobs": int(episode["member_job_count"]),
        "factor_instruments": json.loads(episode["instruments_json"]),
        "event_family": case["event_family"],
        "source_hypothesis": case["source_hypothesis"],
        "primary_clock_id": case["primary_clock_id"],
        "primary_clock_timing_state": primary["timing_state"],
        "earliest_exact_clock_utc": None if earliest is None else iso_utc(earliest),
        "earliest_exact_clock_timing_state": earliest_state,
        "hindsight_interval_precedes_source_minutes": (
            None if pre_source_minutes is None else round(pre_source_minutes, 3)
        ),
        "source_candidate_known_by_selected_start": source_owns_onset,
        "source_candidate_arrived_during_selected_interval": source_inside,
        "source_clocks": clock_rows,
        "measurable_facts": case["measurable_facts"],
        "attribution_state": case["attribution_state"],
        "missing_evidence": case["missing_evidence"],
        "logic_upgrade_ids": case["logic_upgrade_ids"],
        "case_note": case["case_note"],
        "outcome_selected": True,
        "forecast_proof_eligible": False,
        "research_only": True,
        "execution_eligible": False,
        "execution_decision": "no_trade",
    }


def compile_batch(
    config_path: Path = CONFIG,
    database_path: Path = DATABASE,
) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    validate_config(config)
    connection = sqlite3.connect(
        f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True, timeout=60
    )
    connection.row_factory = sqlite3.Row
    try:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity != "ok":
            raise CaseAuditError(f"sqlite_integrity_failed:{integrity}")
        prior_links = {
            str(row[0])
            for row in connection.execute(
                "SELECT episode_id FROM verified_external_source_episode_links"
            )
        }
        compiled: list[dict[str, Any]] = []
        for case in config["cases"]:
            if str(case["episode_id"]) in prior_links:
                raise CaseAuditError(f"episode_already_covered:{case['episode_id']}")
            episode = _load_episode(connection, str(case["episode_id"]))
            compiled.append(_compile_case(case, episode))
    finally:
        connection.close()

    timing_counts = Counter(row["earliest_exact_clock_timing_state"] for row in compiled)
    attribution_counts = Counter(row["attribution_state"] for row in compiled)
    upgrade_counts: Counter[str] = Counter()
    for row in compiled:
        upgrade_counts.update(row["logic_upgrade_ids"])
        row["case_sha256"] = stable_hash(row)
    return {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "upstream_contract_id": UPSTREAM_CONTRACT_ID,
        "generated_utc": iso_utc(dt.datetime.now(tz=UTC)),
        "config_path": str(config_path.resolve()),
        "config_sha256": file_hash(config_path),
        "database_path": str(database_path.resolve()),
        "sqlite_integrity": integrity,
        "case_count": len(compiled),
        "unique_episode_count": len({row["episode_id"] for row in compiled}),
        "unique_market_date_count": len(
            {row["representative_start_utc"][:10] for row in compiled}
        ),
        "timing_state_counts": dict(sorted(timing_counts.items())),
        "attribution_state_counts": dict(sorted(attribution_counts.items())),
        "logic_upgrades": config["logic_upgrades"],
        "logic_upgrade_case_counts": dict(sorted(upgrade_counts.items())),
        "cases": compiled,
        "limitations": [
            "The movement interval and profitable side were selected with hindsight.",
            "The archived reconstruction retains executable endpoints and factor breadth, not a complete pre-event 21-currency rank snapshot for every historical minute.",
            "A plausible source match is not causal proof and cannot authorize execution.",
            "Missing pre-release consensus and rate repricing keep semantic direction research-only.",
        ],
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
    }


def render_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Ten uncovered factor-move case audits V1",
        "",
        f"Generated: {payload['generated_utc']}",
        "",
        "Retrospective movement-first research only. These cases cannot authorize an order or relax a gate.",
        "",
        "## Batch result",
        "",
        f"- Cases: **{payload['case_count']}**, all unique factor episodes and market dates.",
        f"- Source-clock disposition: `{canonical_json(payload['timing_state_counts'])}`.",
        f"- Attribution disposition: `{canonical_json(payload['attribution_state_counts'])}`.",
        "- Execution decision: **no_trade**.",
        "",
        "## Cases",
        "",
        "| # | Case | Move | Breadth | Earliest exact source clock | Clock ownership | Attribution |",
        "|---:|---|---|---:|---|---|---|",
    ]
    for index, row in enumerate(payload["cases"], 1):
        lines.append(
            f"| {index} | `{row['case_id']}` | {row['representative_instrument']} "
            f"{row['representative_best_direction']} {row['representative_net_pips']:+.1f}p | "
            f"{row['factor_breadth_instruments']} | {row['earliest_exact_clock_utc'] or 'unverified'} | "
            f"{row['earliest_exact_clock_timing_state']} | {row['attribution_state']} |"
        )
    for index, row in enumerate(payload["cases"], 1):
        lines.extend(
            [
                "",
                f"### {index}. {row['case_id']}",
                "",
                f"- Path: **{row['representative_instrument']} {row['representative_best_direction']} "
                f"{row['representative_net_pips']:+.1f} after-cost pips**, "
                f"{row['representative_start_utc']} to {row['representative_end_utc']}.",
                f"- Factor: `{row['observed_hindsight_factor']}` across "
                f"**{row['factor_breadth_instruments']}** instruments; this is one episode, not "
                f"{row['factor_breadth_instruments']} independent wins.",
                f"- Source hypothesis: {row['source_hypothesis']}",
                f"- Timing: earliest exact retained clock `{row['earliest_exact_clock_utc'] or 'unverified'}` "
                f"was `{row['earliest_exact_clock_timing_state']}`.",
                f"- Attribution: `{row['attribution_state']}`.",
                f"- Missing evidence: {', '.join(row['missing_evidence']) or 'none'}.",
                f"- Logic upgrades: {', '.join(row['logic_upgrade_ids'])}.",
                f"- Conclusion: {row['case_note']}",
                "- Source clocks:",
            ]
        )
        for clock in row["source_clocks"]:
            lines.append(
                f"  - `{clock['clock_id']}` — {clock['at_utc'] or 'time unverified'}; "
                f"{clock['timing_state']}; [{clock['source_role']}]({clock['source_url']})."
            )
    lines.extend(["", "## General logic upgrades", ""])
    for upgrade in payload["logic_upgrades"]:
        lines.append(
            f"- `{upgrade['upgrade_id']}` ({payload['logic_upgrade_case_counts'].get(upgrade['upgrade_id'], 0)} cases): "
            f"{upgrade['definition']}"
        )
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {value}" for value in payload["limitations"])
    lines.extend(["", "Supported decision: **no_trade**.", ""])
    return "\n".join(lines)


def write_outputs(
    payload: Mapping[str, Any],
    output_json: Path = OUTPUT_JSON,
    output_md: Path = OUTPUT_MD,
    output_log: Path = OUTPUT_LOG,
) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output_md.write_text(render_markdown(payload), encoding="utf-8")
    existing: dict[str, str] = {}
    if output_log.exists():
        for line in output_log.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            existing[str(row["case_id"])] = str(row["case_sha256"])
    additions: list[str] = []
    for case in payload["cases"]:
        case_id = str(case["case_id"])
        digest = str(case["case_sha256"])
        if case_id in existing and existing[case_id] != digest:
            raise CaseAuditError(f"case_log_identity_collision:{case_id}")
        if case_id not in existing:
            additions.append(canonical_json(case))
    if additions:
        with output_log.open("a", encoding="utf-8", newline="\n") as handle:
            for line in additions:
                handle.write(line + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--output-json", type=Path, default=OUTPUT_JSON)
    parser.add_argument("--output-md", type=Path, default=OUTPUT_MD)
    parser.add_argument("--output-log", type=Path, default=OUTPUT_LOG)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = compile_batch(args.config, args.database)
    write_outputs(payload, args.output_json, args.output_md, args.output_log)
    print(
        canonical_json(
            {
                "case_count": payload["case_count"],
                "execution_decision": payload["execution_decision"],
                "output_json": str(args.output_json.resolve()),
                "output_md": str(args.output_md.resolve()),
                "output_log": str(args.output_log.resolve()),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
