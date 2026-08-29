#!/usr/bin/env python3
"""Audit ten distinct current-week FX moves against causal source clocks.

The audit is deliberately movement-first.  It measures the executable path at
the declared event/move clock, checks what the collector actually knew, derives
pre-event technical state using only earlier candles, and estimates broad
currency-factor response across the complete 68-pair OANDA practice universe.

This module is retrospective research only.  It cannot promote a hypothesis,
authorize an account, or submit/close an order.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import hashlib
import json
import sqlite3
import statistics
from pathlib import Path
from typing import Any, Mapping, Sequence

from oanda_instrument_pips import fallback_pip_size


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "current_week_move_case_audit_v1.json"
CANDLE_ROOT = ROOT / "data" / "oanda_training_manager" / "candles"
NEWS_DB = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "local_news_sentiment_v1.sqlite"
)
REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "major_move_case_audits"
    / "ten_current_week_move_cases_v1"
)
OUTPUT_JSON = REPORT_ROOT / "TEN_CURRENT_WEEK_MOVE_CASES_V1.json"
OUTPUT_MD = REPORT_ROOT / "TEN_CURRENT_WEEK_MOVE_CASES_V1.md"
OUTPUT_LOG = REPORT_ROOT / "TEN_CURRENT_WEEK_MOVE_CASE_LOG_V1.jsonl"
CONTRACT_ID = "current_week_move_case_audit_v1_20260821"
UTC = dt.timezone.utc


class WeeklyCaseError(RuntimeError):
    """Raised when an audit input could create misleading evidence."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def parse_utc(value: Any) -> dt.datetime:
    text = str(value or "").strip()
    if not text:
        raise WeeklyCaseError("timestamp_missing")
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WeeklyCaseError(f"timestamp_invalid:{text}") from exc
    if parsed.tzinfo is None:
        raise WeeklyCaseError(f"timestamp_timezone_missing:{text}")
    return parsed.astimezone(UTC)


def iso_utc(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def pip_size(instrument: str) -> float:
    return fallback_pip_size(instrument)


def validate_config(config: Mapping[str, Any]) -> None:
    required = {
        "schema_version",
        "contract_id",
        "research_only",
        "execution_eligible",
        "week_start_utc",
        "week_end_utc",
        "logic_upgrades",
        "cases",
    }
    missing = required - set(config)
    if missing:
        raise WeeklyCaseError(f"config_keys_missing:{sorted(missing)}")
    if config["schema_version"] != 1 or config["contract_id"] != CONTRACT_ID:
        raise WeeklyCaseError("config_identity_mismatch")
    if config["research_only"] is not True or config["execution_eligible"] is not False:
        raise WeeklyCaseError("config_must_remain_inert")
    start = parse_utc(config["week_start_utc"])
    end = parse_utc(config["week_end_utc"])
    if end <= start:
        raise WeeklyCaseError("week_window_invalid")
    cases = config["cases"]
    if not isinstance(cases, list) or len(cases) != 10:
        raise WeeklyCaseError(f"exactly_ten_cases_required:{len(cases) if isinstance(cases, list) else 'invalid'}")
    upgrades = config["logic_upgrades"]
    if not isinstance(upgrades, list):
        raise WeeklyCaseError("logic_upgrades_invalid")
    upgrade_ids = {str(row.get("upgrade_id") or "") for row in upgrades}
    if "" in upgrade_ids or len(upgrade_ids) != len(upgrades):
        raise WeeklyCaseError("logic_upgrade_ids_invalid")
    case_ids: set[str] = set()
    for case in cases:
        required_case = {
            "case_id",
            "event_time_utc",
            "event_family",
            "representative_pair",
            "horizon_minutes",
            "source_event_ids",
            "source_hypothesis_currency_scores",
            "measurable_facts",
            "known_problem",
            "logic_upgrade_ids",
            "case_note",
        }
        missing_case = required_case - set(case)
        if missing_case:
            raise WeeklyCaseError(f"case_keys_missing:{sorted(missing_case)}")
        case_id = str(case["case_id"])
        if not case_id or case_id in case_ids:
            raise WeeklyCaseError(f"case_id_invalid:{case_id}")
        case_ids.add(case_id)
        at = parse_utc(case["event_time_utc"])
        if not start <= at < end:
            raise WeeklyCaseError(f"case_outside_week:{case_id}")
        instrument = str(case["representative_pair"])
        if instrument.count("_") != 1:
            raise WeeklyCaseError(f"instrument_invalid:{case_id}")
        horizon = case["horizon_minutes"]
        if type(horizon) is not int or not 5 <= horizon <= 120:
            raise WeeklyCaseError(f"horizon_invalid:{case_id}")
        event_ids = case["source_event_ids"]
        if not isinstance(event_ids, list) or len(event_ids) != len(set(event_ids)):
            raise WeeklyCaseError(f"source_event_ids_invalid:{case_id}")
        if not set(case["logic_upgrade_ids"]).issubset(upgrade_ids):
            raise WeeklyCaseError(f"unknown_logic_upgrade:{case_id}")


def _load_news_events(path: Path, event_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    if not event_ids:
        return {}
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" for _ in event_ids)
        rows = connection.execute(
            f"SELECT * FROM articles WHERE event_id IN ({placeholders})", tuple(event_ids)
        ).fetchall()
    finally:
        connection.close()
    output = {str(row["event_id"]): dict(row) for row in rows}
    missing = set(event_ids) - set(output)
    if missing:
        raise WeeklyCaseError(f"source_events_missing:{sorted(missing)}")
    return output


def _load_candles(
    candle_root: Path,
    start: dt.datetime,
    end: dt.datetime,
) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {}
    for path in sorted(candle_root.glob("*_M1.csv")):
        instrument = path.name.removesuffix("_M1.csv")
        rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8", newline="") as handle:
            for raw in csv.DictReader(handle):
                stamp_text = str(raw.get("datetime") or raw.get("time") or "")
                if not stamp_text:
                    continue
                try:
                    stamp = parse_utc(stamp_text)
                except WeeklyCaseError:
                    continue
                if stamp < start or stamp > end:
                    continue
                try:
                    bid_open = float(raw["bid_open"])
                    ask_open = float(raw["ask_open"])
                    bid_close = float(raw["bid_close"])
                    ask_close = float(raw["ask_close"])
                except (KeyError, TypeError, ValueError):
                    continue
                rows.append(
                    {
                        "timestamp": stamp,
                        "bid_open": bid_open,
                        "ask_open": ask_open,
                        "bid_close": bid_close,
                        "ask_close": ask_close,
                        "mid_open": (bid_open + ask_open) / 2.0,
                        "mid_close": (bid_close + ask_close) / 2.0,
                    }
                )
        if rows:
            rows.sort(key=lambda row: row["timestamp"])
            output[instrument] = rows
    return output


def _point_at_or_after(
    rows: Sequence[Mapping[str, Any]], at: dt.datetime, max_delay_seconds: int = 180
) -> Mapping[str, Any] | None:
    times = [row["timestamp"] for row in rows]
    index = bisect.bisect_left(times, at)
    if index >= len(rows):
        return None
    point = rows[index]
    if (point["timestamp"] - at).total_seconds() > max_delay_seconds:
        return None
    return point


def _point_at_or_before(
    rows: Sequence[Mapping[str, Any]], at: dt.datetime, max_delay_seconds: int = 180
) -> Mapping[str, Any] | None:
    times = [row["timestamp"] for row in rows]
    index = bisect.bisect_right(times, at) - 1
    if index < 0:
        return None
    point = rows[index]
    if (at - point["timestamp"]).total_seconds() > max_delay_seconds:
        return None
    return point


def _pair_path(
    panel: Mapping[str, Sequence[Mapping[str, Any]]],
    instrument: str,
    start: dt.datetime,
    end: dt.datetime,
) -> dict[str, Any]:
    rows = panel.get(instrument) or []
    first = _point_at_or_after(rows, start)
    last = _point_at_or_after(rows, end)
    if first is None or last is None:
        raise WeeklyCaseError(f"price_path_missing:{instrument}:{iso_utc(start)}:{iso_utc(end)}")
    pip = pip_size(instrument)
    mid_pips = (float(last["mid_close"]) - float(first["mid_open"])) / pip
    observed_side = 1 if mid_pips > 0 else -1 if mid_pips < 0 else 0
    long_net = (float(last["bid_close"]) - float(first["ask_open"])) / pip
    short_net = (float(first["bid_open"]) - float(last["ask_close"])) / pip
    return {
        "start_utc": iso_utc(first["timestamp"]),
        "end_utc": iso_utc(last["timestamp"]),
        "observed_side": "long" if observed_side > 0 else "short" if observed_side < 0 else "flat",
        "midpoint_move_pips": round(mid_pips, 4),
        "long_after_cost_pips": round(long_net, 4),
        "short_after_cost_pips": round(short_net, 4),
        "observed_after_cost_pips": round(long_net if observed_side > 0 else short_net if observed_side < 0 else 0.0, 4),
        "entry_spread_pips": round((float(first["ask_open"]) - float(first["bid_open"])) / pip, 4),
    }


def _prior_return(
    panel: Mapping[str, Sequence[Mapping[str, Any]]],
    instrument: str,
    at: dt.datetime,
    minutes: int,
) -> float | None:
    rows = panel.get(instrument) or []
    earlier = _point_at_or_before(rows, at - dt.timedelta(minutes=minutes))
    current = _point_at_or_before(rows, at - dt.timedelta(seconds=1))
    if earlier is None or current is None:
        return None
    return round((float(current["mid_close"]) - float(earlier["mid_open"])) / pip_size(instrument), 4)


def _factor_scores(
    panel: Mapping[str, Sequence[Mapping[str, Any]]],
    start: dt.datetime,
    end: dt.datetime,
) -> dict[str, Any]:
    contributions: dict[str, list[float]] = {}
    usable = 0
    for instrument, rows in panel.items():
        first = _point_at_or_after(rows, start)
        last = _point_at_or_after(rows, end)
        if first is None or last is None:
            continue
        initial = float(first["mid_open"])
        if initial <= 0:
            continue
        move_bps = (float(last["mid_close"]) / initial - 1.0) * 10_000.0
        base, quote = instrument.split("_", 1)
        contributions.setdefault(base, []).append(move_bps)
        contributions.setdefault(quote, []).append(-move_bps)
        usable += 1
    scores = {
        currency: round(statistics.median(values), 4)
        for currency, values in contributions.items()
        if values
    }
    ordered = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    return {
        "usable_pair_count": usable,
        "currency_count": len(scores),
        "scores_bps": scores,
        "ranking": [currency for currency, _ in ordered],
        "strongest": [
            {"currency": currency, "score_bps": score} for currency, score in ordered[:5]
        ],
        "weakest": [
            {"currency": currency, "score_bps": score} for currency, score in ordered[-5:]
        ],
    }


def _technical_state(prior_5: float | None, prior_60: float | None, observed: str) -> str:
    sign = 1 if observed == "long" else -1 if observed == "short" else 0
    values = [value for value in (prior_5, prior_60) if value is not None and value != 0]
    if not sign or not values:
        return "unavailable"
    aligned = sum((1 if value > 0 else -1) == sign for value in values)
    if aligned == len(values):
        return "pre_event_momentum_aligned"
    if aligned == 0:
        return "pre_event_momentum_opposed"
    return "pre_event_momentum_mixed"


def _source_pair_score(case: Mapping[str, Any]) -> float:
    base, quote = str(case["representative_pair"]).split("_", 1)
    scores = case["source_hypothesis_currency_scores"]
    return float(scores.get(base, 0.0)) - float(scores.get(quote, 0.0))


def _compile_case(
    case: Mapping[str, Any],
    panel: Mapping[str, Sequence[Mapping[str, Any]]],
    events: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    event_time = parse_utc(case["event_time_utc"])
    horizon = int(case["horizon_minutes"])
    end = event_time + dt.timedelta(minutes=horizon)
    event_path = _pair_path(panel, str(case["representative_pair"]), event_time, end)
    source_rows = [events[event_id] for event_id in case["source_event_ids"]]
    source_rows.sort(key=lambda row: parse_utc(row["first_seen_utc"]))
    source_latency = None
    source_capture_to_event_end = None
    source_capture_full_horizon = None
    if source_rows:
        first_seen = parse_utc(source_rows[0]["first_seen_utc"])
        source_latency = round((first_seen - event_time).total_seconds(), 3)
        if first_seen <= end:
            source_capture_to_event_end = _pair_path(
                panel, str(case["representative_pair"]), first_seen, end
            )
        source_capture_full_horizon = _pair_path(
            panel,
            str(case["representative_pair"]),
            first_seen,
            first_seen + dt.timedelta(minutes=horizon),
        )
    prior_5 = _prior_return(panel, str(case["representative_pair"]), event_time, 5)
    prior_60 = _prior_return(panel, str(case["representative_pair"]), event_time, 60)
    source_score = _source_pair_score(case)
    observed_sign = 1 if event_path["observed_side"] == "long" else -1 if event_path["observed_side"] == "short" else 0
    if not source_rows:
        attribution = "no_causal_source_candidate"
    elif source_score == 0:
        attribution = "source_caught_direction_unresolved"
    elif observed_sign == (1 if source_score > 0 else -1):
        attribution = "source_hypothesis_aligned"
    else:
        attribution = "source_hypothesis_opposed"
    factor = _factor_scores(panel, event_time, end)
    base, quote = str(case["representative_pair"]).split("_", 1)
    result = {
        **dict(case),
        "event_time_utc": iso_utc(event_time),
        "event_path": event_path,
        "source_pair_score": round(source_score, 6),
        "source_latency_seconds": source_latency,
        "source_capture_to_event_horizon": source_capture_to_event_end,
        "source_capture_full_horizon_after_seen": source_capture_full_horizon,
        "attribution_state": attribution,
        "pre_event_5m_pips": prior_5,
        "pre_event_60m_pips": prior_60,
        "technical_state": _technical_state(prior_5, prior_60, event_path["observed_side"]),
        "factor_response": factor,
        "representative_factor_spread_bps": round(
            float(factor["scores_bps"].get(base, 0.0))
            - float(factor["scores_bps"].get(quote, 0.0)),
            4,
        ),
        "source_records": [
            {
                "event_id": row["event_id"],
                "source_id": row["source_id"],
                "published_utc": row["published_utc"],
                "first_seen_utc": row["first_seen_utc"],
                "headline": row["headline"],
                "source_url": row["source_url"],
                "directional_bias_json": row["directional_bias_json"],
                "directional_confidence": row["directional_confidence"],
            }
            for row in source_rows
        ],
        "research_only": True,
        "execution_eligible": False,
        "forecast_proof_eligible": False,
    }
    result["case_hash"] = stable_hash(result)
    return result


def compile_audit(
    config_path: Path = CONFIG,
    candle_root: Path = CANDLE_ROOT,
    news_db: Path = NEWS_DB,
) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    validate_config(config)
    event_ids = [
        event_id for case in config["cases"] for event_id in case["source_event_ids"]
    ]
    events = _load_news_events(news_db, event_ids)
    event_times = [parse_utc(case["event_time_utc"]) for case in config["cases"]]
    maximum_horizon = max(int(case["horizon_minutes"]) for case in config["cases"])
    panel = _load_candles(
        candle_root,
        min(event_times) - dt.timedelta(minutes=65),
        max(event_times) + dt.timedelta(minutes=maximum_horizon + 10),
    )
    if len(panel) != 68:
        raise WeeklyCaseError(f"exact_68_pair_panel_required:{len(panel)}")
    cases = [_compile_case(case, panel, events) for case in config["cases"]]
    payload = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "generated_utc": iso_utc(dt.datetime.now(UTC)),
        "week_start_utc": config["week_start_utc"],
        "week_end_utc": config["week_end_utc"],
        "case_count": len(cases),
        "pair_universe_count": len(panel),
        "source_caught_case_count": sum(bool(case["source_records"]) for case in cases),
        "direction_resolved_case_count": sum(case["source_pair_score"] != 0 for case in cases),
        "direction_aligned_case_count": sum(case["attribution_state"] == "source_hypothesis_aligned" for case in cases),
        "direction_opposed_case_count": sum(case["attribution_state"] == "source_hypothesis_opposed" for case in cases),
        "direction_unresolved_case_count": sum(case["attribution_state"] == "source_caught_direction_unresolved" for case in cases),
        "no_source_case_count": sum(case["attribution_state"] == "no_causal_source_candidate" for case in cases),
        "logic_upgrades": config["logic_upgrades"],
        "cases": cases,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
    }
    payload["audit_hash"] = stable_hash(payload)
    return payload


def render_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Ten current-week move cases",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "This is a retrospective, movement-first diagnostic. It does not turn an explanation into an executable forecast.",
        "",
        "## Summary",
        "",
        f"- Cases: **{payload['case_count']}** across the exact **{payload['pair_universe_count']}/68** price panel.",
        f"- Official/secondary source caught: **{payload['source_caught_case_count']}**.",
        f"- Direction resolved: **{payload['direction_resolved_case_count']}**; aligned **{payload['direction_aligned_case_count']}**, opposed **{payload['direction_opposed_case_count']}**.",
        f"- Source caught but direction unresolved: **{payload['direction_unresolved_case_count']}**; no causal source: **{payload['no_source_case_count']}**.",
        "- Operational disposition: **no_trade**.",
        "",
        "| Case | Clock | Pair | Horizon | Move | Net observed | Source latency | Attribution | Technical |",
        "|---|---|---|---:|---:|---:|---:|---|---|",
    ]
    for case in payload["cases"]:
        path = case["event_path"]
        latency = "n/a" if case["source_latency_seconds"] is None else f"{case['source_latency_seconds']:.1f}s"
        lines.append(
            f"| {case['case_id']} | {case['event_time_utc']} | {case['representative_pair']} | "
            f"{case['horizon_minutes']}m | {path['midpoint_move_pips']:+.1f}p | "
            f"{path['observed_after_cost_pips']:+.1f}p | {latency} | "
            f"{case['attribution_state']} | {case['technical_state']} |"
        )
    lines.extend(["", "## Case findings", ""])
    for case in payload["cases"]:
        factor = case["factor_response"]
        lines.extend(
            [
                f"### {case['case_id']}",
                "",
                f"- {case['case_note']}",
                f"- Problem: `{case['known_problem']}`.",
                f"- Price: {case['event_path']['observed_side']} {case['event_path']['midpoint_move_pips']:+.2f} pips midpoint; {case['event_path']['observed_after_cost_pips']:+.2f} pips after executable spread.",
                f"- Pre-event momentum: 5m `{case['pre_event_5m_pips']}`, 60m `{case['pre_event_60m_pips']}`; `{case['technical_state']}`.",
                f"- Factor breadth: {factor['usable_pair_count']}/68 pairs, strongest `{factor['strongest'][0] if factor['strongest'] else {}}`, weakest `{factor['weakest'][0] if factor['weakest'] else {}}`.",
                f"- Attribution: `{case['attribution_state']}`; source score `{case['source_pair_score']:+.3f}`.",
                f"- Facts: {'; '.join(str(value) for value in case['measurable_facts'])}",
                f"- Upgrades: {', '.join(case['logic_upgrade_ids'])}",
                "",
            ]
        )
    lines.extend(["## Logic upgrades", ""])
    for item in payload["logic_upgrades"]:
        lines.append(f"- `{item['upgrade_id']}` — {item['status']}: {item['description']}")
    lines.extend(["", f"Audit hash: `{payload['audit_hash']}`", ""])
    return "\n".join(lines)


def write_outputs(payload: Mapping[str, Any]) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    OUTPUT_MD.write_text(render_markdown(payload), encoding="utf-8")
    OUTPUT_LOG.write_text(
        "".join(canonical_json(case) + "\n" for case in payload["cases"]),
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--candle-root", type=Path, default=CANDLE_ROOT)
    parser.add_argument("--news-database", type=Path, default=NEWS_DB)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = compile_audit(args.config, args.candle_root, args.news_database)
    write_outputs(payload)
    print(
        json.dumps(
            {
                "output": str(OUTPUT_MD),
                "case_count": payload["case_count"],
                "source_caught": payload["source_caught_case_count"],
                "aligned": payload["direction_aligned_case_count"],
                "opposed": payload["direction_opposed_case_count"],
                "unresolved": payload["direction_unresolved_case_count"],
                "no_source": payload["no_source_case_count"],
                "execution_decision": payload["execution_decision"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
