#!/usr/bin/env python3
"""Govern the pure-change/direct-source and broad strategy-space branch.

This coordinator does not place orders, promote strategies, or mutate source
evidence.  It inventories the executable opportunity census, the exhaustive
moving-average and multi-timeframe rule grammars, compact candle readiness,
direct official source state, and retained headline/blurb coverage.  It writes
one fail-closed branch state, a human report, and an append-only JSONL run log.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any

import oanda_moving_average_crossover_sweep as ma_sweep
import oanda_multitimeframe_crossover_stack_sweep as stack_sweep


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "pure_change_strategy_space_v1.json"
STATE_ROOT = ROOT / "data" / "oanda_training_manager" / "state"
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports" / "pure_change_strategy_space"
STATE = STATE_ROOT / "pure_change_strategy_space_v1.json"
REPORT = REPORT_ROOT / "PURE_CHANGE_STRATEGY_SPACE_CURRENT.md"
LOG = REPORT_ROOT / "pure_change_strategy_space_watch.jsonl"
NEWS_DB = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment" / "local_news_sentiment_v1.sqlite"
CANDLE_ROOT = ROOT / "data" / "oanda_training_manager" / "candles"
OPPORTUNITY = ROOT / "data" / "oanda_training_manager" / "reports" / "sixty_eight_pair_combiner" / "SIXTY_EIGHT_PAIR_OPPORTUNITY_CENSUS_20260808.json"


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, sort_keys=True, default=str) + "\n")


def news_blurb_census(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"status": "missing", "path": str(path)}
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=30)
    try:
        scalar = lambda sql: int(connection.execute(sql).fetchone()[0])
        result = {
            "status": "ok",
            "path": str(path),
            "articles": scalar("SELECT COUNT(*) FROM articles"),
            "relevant_articles": scalar("SELECT COUNT(*) FROM articles WHERE relevant=1"),
            "articles_with_blurb": scalar("SELECT COUNT(*) FROM articles WHERE trim(coalesce(summary,''))<>''"),
            "official_verified_articles": scalar("SELECT COUNT(*) FROM articles WHERE source_verified=1"),
            "official_verified_with_blurb": scalar("SELECT COUNT(*) FROM articles WHERE source_verified=1 AND trim(coalesce(summary,''))<>''"),
            "source_count": scalar("SELECT COUNT(DISTINCT source_id) FROM articles"),
            "category_count": scalar("SELECT COUNT(DISTINCT category) FROM articles"),
            "story_clusters": scalar("SELECT COUNT(*) FROM topic_events"),
        }
        result["blurb_coverage"] = result["articles_with_blurb"] / result["articles"] if result["articles"] else None
        result["official_blurb_coverage"] = result["official_verified_with_blurb"] / result["official_verified_articles"] if result["official_verified_articles"] else None
        return result
    finally:
        connection.close()


def strategy_space(config: dict[str, Any]) -> dict[str, Any]:
    moving = config["moving_average_space"]
    ma_pairs = ma_sweep.parse_ma_pairs(",".join(moving["ma_kind_pairs"]))
    windows = ma_sweep.window_pairs_for_preset(str(moving["preset"]))
    configurations = ma_sweep.build_configurations(
        moving["timeframes"], ma_pairs, windows, moving["confirmations"]
    )
    stack = config["stack_space"]
    stack_configurations = stack_sweep.build_stack_configurations(
        stack["trigger_timeframes"],
        [stack_sweep.parse_cross_spec(value) for value in stack["trigger_specs"]],
        stack["trigger_confirmations"],
    )
    candle_files = sorted(CANDLE_ROOT.glob("*_M1.csv"))
    return {
        "ma_window_pairs": len(windows),
        "ma_configurations": len(configurations),
        "ma_horizons": len(moving["horizons_minutes"]),
        "ma_pair_horizon_evaluations_at_68_pairs": len(configurations) * len(moving["horizons_minutes"]) * 68,
        "multitimeframe_stack_configurations": len(stack_configurations),
        "existing_rule_engines": [
            {"path": value, "present": (ROOT / value).is_file()}
            for value in config["existing_rule_engines"]
        ],
        "compact_m1_pair_files": len(candle_files),
        "compact_m1_bytes": sum(path.stat().st_size for path in candle_files),
        "sweep_ready": len(candle_files) == 68,
        "bootstrap_command": "python oanda_all68_m1_forward_updater.py --bootstrap-all-priced --max-requests-per-pair 1",
        "historical_backfill_command": "python oanda_all68_m1_forward_updater.py --bootstrap-all-priced --max-requests-per-pair 0 --backfill-requests-per-pair 1",
        "exhaustive_ma_command": "python oanda_moving_average_crossover_sweep.py --source-dir data/oanda_training_manager/candles --preset exhaustive --timeframes M1,M2,M3,M4,M5,M6,M8,M10,M12,M15,M20,M30,M45,H1,H2,H3,H4,H6,H8,H12,D1",
    }


def source_state() -> dict[str, Any]:
    macro = read_json(STATE_ROOT / "macro_surprise_v1.json")
    treasury = read_json(STATE_ROOT / "us_treasury_yield_prospective_v1.json")
    cftc = read_json(STATE_ROOT / "cftc_currency_positioning_shadow_v1.json")
    gdelt = read_json(STATE_ROOT / "gdelt_attention_magnitude_prospective_v1.json")
    alfred = read_json(STATE_ROOT / "alfred_vintage_prospective_v1.json")
    return {
        "macro_surprise": {
            "status": macro.get("status"),
            "release_count": macro.get("release_count"),
            "actual_count": macro.get("actual_count"),
            "consensus_count": macro.get("consensus_count"),
            "standardized_surprise_count": macro.get("standardized_surprise_count"),
        },
        "us_treasury": {
            "status": treasury.get("status"),
            "bootstrap_rows": (treasury.get("totals") or {}).get("bootstrap_rows"),
            "prospective_eligible_rows": (treasury.get("totals") or {}).get("prospective_eligible_rows"),
        },
        "cftc": {
            "currency_count": cftc.get("currency_count"),
            "historical_causal_backtest_ready": cftc.get("historical_causal_backtest_ready"),
            "account_eligible": cftc.get("account_eligible"),
        },
        "gdelt": {
            "status": gdelt.get("status"),
            "forecasts": (gdelt.get("totals") or {}).get("forecasts"),
            "matured": (gdelt.get("totals") or {}).get("matured"),
        },
        "alfred": {
            "status": alfred.get("status"),
            "credential_present": alfred.get("credential_present"),
        },
    }


def opportunity_summary(path: Path) -> dict[str, Any]:
    payload = read_json(path)
    result: dict[str, Any] = {}
    for horizon in (300, 600, 900):
        row = (payload.get("census") or {}).get(str(horizon)) or {}
        rolling = row.get("rolling_every_minute") or {}
        result[str(horizon)] = {
            "aligned_pair_clear_rate": row.get("full_day_pair_window_clear_rate"),
            "rolling_pair_clear_rate": rolling.get("full_day_pair_window_clear_rate"),
            "rolling_any_clear_timestamp_rate": rolling.get("full_day_any_clear_timestamp_rate"),
            "rolling_path_clear_rate": rolling.get("full_day_path_pair_window_clear_rate"),
            "rolling_any_path_clear_timestamp_rate": rolling.get("full_day_any_path_clear_timestamp_rate"),
        }
    return result


def historical_attribution_state(config: dict[str, Any]) -> dict[str, Any]:
    contract = config.get("historical_news_attribution") or {}
    bundle = Path(str(contract.get("vault_path") or ""))
    schema = ROOT / str(contract.get("canonical_schema") or "")
    expected_sha = str(contract.get("bundle_sha256") or "")
    observed_sha = sha256_file(bundle)
    return {
        "bundle_path": str(bundle),
        "bundle_present": bundle.is_file(),
        "expected_sha256": expected_sha,
        "observed_sha256": observed_sha,
        "bundle_integrity_ok": bool(expected_sha and observed_sha == expected_sha),
        "schema_path": str(schema),
        "schema_present": schema.is_file(),
        "schema_sha256": sha256_file(schema),
        "mode": contract.get("mode"),
        "execution_eligible": False,
    }


def render(payload: dict[str, Any]) -> str:
    blurbs = payload["blurb_dataset"]
    space = payload["strategy_space"]
    sources = payload["source_state"]
    historical = payload["historical_news_attribution"]
    lines = [
        "# Pure Change and Strategy-Space Research Branch",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Research-only. This branch cannot promote, authorize, or place an order.",
        "",
        "## Opportunity-first contract",
        "",
        "The branch first measures whether executable 5/10/15-minute movement exists. Endpoint and any-minute path opportunity are hindsight diagnostics; only later frozen causal rules may predict them.",
        "",
        "| Horizon | Rolling endpoint pair-clear | Times with any endpoint clear | Rolling path pair-clear | Times with any path clear |",
        "|---:|---:|---:|---:|---:|",
    ]
    for horizon, row in payload["opportunity"].items():
        fmt = lambda value: "n/a" if value is None else f"{float(value):.1%}"
        lines.append(
            f"| {int(horizon)//60}m | {fmt(row['rolling_pair_clear_rate'])} | {fmt(row['rolling_any_clear_timestamp_rate'])} | {fmt(row['rolling_path_clear_rate'])} | {fmt(row['rolling_any_path_clear_timestamp_rate'])} |"
        )
    lines += [
        "",
        "## Governed brute-force net",
        "",
        f"- Exhaustive MA window pairs: **{space['ma_window_pairs']:,}**.",
        f"- MA/timeframe/kind/confirmation configurations: **{space['ma_configurations']:,}**.",
        f"- Nominal 68-pair/horizon evaluations: **{space['ma_pair_horizon_evaluations_at_68_pairs']:,}**.",
        f"- Predeclared multi-timeframe stack configurations: **{space['multitimeframe_stack_configurations']:,}**.",
        f"- Compact M1 archive: **{space['compact_m1_pair_files']} / 68 pairs**; sweep ready: **{space['sweep_ready']}**.",
        "- Discovery samples the strongest signal-unique rules; validation ranks them; holdout is only reported after selection; any selected result must open a later untouched cohort.",
        "",
        "## Blurb and official-source coverage",
        "",
        f"- Articles: **{blurbs.get('articles', 0):,}** from **{blurbs.get('source_count', 0)}** sources; relevant: **{blurbs.get('relevant_articles', 0):,}**.",
        f"- Retained nonempty blurbs/summaries: **{blurbs.get('articles_with_blurb', 0):,}** ({(blurbs.get('blurb_coverage') or 0):.1%}).",
        f"- Verified official articles: **{blurbs.get('official_verified_articles', 0):,}**; with blurbs: **{blurbs.get('official_verified_with_blurb', 0):,}** ({(blurbs.get('official_blurb_coverage') or 0):.1%}).",
        f"- Story/topic clusters: **{blurbs.get('story_clusters', 0):,}**.",
        "- Headlines and retained blurbs exist, but most official records still expose metadata/headlines rather than a complete numeric policy delta. Missing detail is unavailable, never neutral.",
        "",
        "## Recovered historical attribution asset",
        "",
        f"- Bundle present and hash-verified: **{historical['bundle_present']} / {historical['bundle_integrity_ok']}**.",
        f"- Canonical episode schema present: **{historical['schema_present']}** (`{historical['schema_sha256'][:16]}`).",
        "- The bundle supplies movement-first 6h-to-30d episode discovery and detailed entry/exit evidence. Exit and later evidence remain ex-post only; they cannot become entry-time features.",
        "- Generated macro tags and narrative rules remain hypothesis generators, not proof or executable policy.",
        "",
        "## Direct-source state",
        "",
        f"- Macro: {sources['macro_surprise']}.",
        f"- Treasury: {sources['us_treasury']}.",
        f"- CFTC: {sources['cftc']}.",
        f"- GDELT: {sources['gdelt']}.",
        f"- ALFRED: {sources['alfred']}.",
        "",
        "## Active gates",
        "",
    ]
    lines.extend(f"- {item}" for item in payload["blockers"])
    return "\n".join(lines) + "\n"


def run(config_path: Path = CONFIG, state_path: Path = STATE, report_path: Path = REPORT, log_path: Path = LOG) -> dict[str, Any]:
    config = read_json(config_path)
    config_sha = sha256_file(config_path)
    collector_sha = sha256_file(Path(__file__).resolve())
    identity = hashlib.sha256(f"{config_sha}|{collector_sha}".encode()).hexdigest()
    generated = utc_now()
    space = strategy_space(config)
    blurbs = news_blurb_census(NEWS_DB)
    sources = source_state()
    historical = historical_attribution_state(config)
    free_bytes = shutil.disk_usage(ROOT).free
    blockers: list[str] = []
    if not space["sweep_ready"]:
        blockers.append("Canonical compact BAM M1 archive is incomplete; bootstrap through the practice-only GET collector before running full sweeps.")
    if not sources["macro_surprise"].get("consensus_count"):
        blockers.append("Pre-release consensus is still absent, so actual-versus-consensus policy/macro direction cannot be tested causally.")
    if not sources["us_treasury"].get("prospective_eligible_rows"):
        blockers.append("U.S. Treasury rows are bootstrap/current-view only; matched cross-currency OIS or policy-path repricing remains missing.")
    if not historical["bundle_integrity_ok"] or not historical["schema_present"]:
        blockers.append("Recovered news-blurb attribution asset or its leakage-safe canonical schema is missing or hash-invalid.")
    blockers.extend([
        "FX options IV/skew and carry/forward/funding sources remain blocked until a lawful timestamp-safe source is connected.",
        "A brute-force winner is discovery only; it must survive multiplicity control, signal-independence pruning, untouched confirmation, and cost stress.",
    ])
    payload = {
        "schema_version": 1,
        "generated_utc": generated,
        "cohort_id": f"pure_change_strategy_space_v1.discovery.20260809.{identity[:16]}",
        "research_only": True,
        "execution_eligible": False,
        "can_promote": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
        "status": "ready_for_full_sweep" if space["sweep_ready"] else "collecting_inputs",
        "collection_cohort": {
            "cohort_id": f"pure_change_strategy_space_v1.discovery.20260809.{identity[:16]}",
            "collector_sha256": collector_sha,
            "config_sha256": config_sha,
            "material_change_requires_new_cohort": True,
        },
        "contract": config,
        "totals": {
            "configured_source_families": len(config["source_purity_ladder"]),
            "ma_configurations": space["ma_configurations"],
            "stack_configurations": space["multitimeframe_stack_configurations"],
            "compact_m1_pair_files": space["compact_m1_pair_files"],
        },
        "strategy_space": space,
        "blurb_dataset": blurbs,
        "source_state": sources,
        "historical_news_attribution": historical,
        "opportunity": opportunity_summary(OPPORTUNITY),
        "storage": {"free_bytes": free_bytes, "free_gib": free_bytes / (1024 ** 3)},
        "blockers": blockers,
    }
    atomic_text(state_path, json.dumps(payload, indent=2, sort_keys=True, default=str))
    atomic_text(report_path, render(payload))
    append_jsonl(log_path, {
        "generated_utc": generated,
        "cohort_id": payload["cohort_id"],
        "status": payload["status"],
        "totals": payload["totals"],
        "opportunity": payload["opportunity"],
        "blockers": blockers,
        "free_gib": payload["storage"]["free_gib"],
    })
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--state", type=Path, default=STATE)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--log", type=Path, default=LOG)
    args = parser.parse_args()
    payload = run(args.config, args.state, args.report, args.log)
    print(json.dumps({
        "cohort_id": payload["cohort_id"],
        "status": payload["status"],
        "ma_configurations": payload["strategy_space"]["ma_configurations"],
        "stack_configurations": payload["strategy_space"]["multitimeframe_stack_configurations"],
        "compact_m1_pair_files": payload["strategy_space"]["compact_m1_pair_files"],
        "free_gib": payload["storage"]["free_gib"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
