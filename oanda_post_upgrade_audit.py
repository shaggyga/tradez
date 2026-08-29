#!/usr/bin/env python3
"""Generate the point-in-time post-upgrade FX proof/governance audit."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
NEWS = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
DEFAULT_OUTPUT = ROOT / "COMPLETE_SOURCE_AUDIT_POST_UPGRADE_20260806.md"


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def integer(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def report_value(value: Any, *, current: bool = True) -> str:
    """Render a current snapshot value without turning missing data into zero."""

    if not current or value is None:
        return "unknown"
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return "unknown"
    return str(value)


def report_currentness(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return "unknown"


def report_account_errors(value: Any) -> str:
    if not isinstance(value, list):
        return "unknown"
    if not value:
        return "none"
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def generate(output: Path = DEFAULT_OUTPUT) -> Path:
    edge = read_json(STATE / "edge_evidence_v1.json")
    cohorts = read_json(STATE / "proof_cohort_registry_v1.json")
    proof = read_json(STATE / "proof_shadow_predictors_v1.json")
    macro = read_json(STATE / "macro_surprise_v1.json")
    news = read_json(NEWS / "collector_latest_v1.json")
    account = read_json(STATE / "account_007_dashboard_v1.json")
    governance = edge.get("prospective_governance") or {}
    census = governance.get("evidence_census") or {}
    multiplicity = governance.get("multiple_testing") or {}
    ladder = governance.get("graduation_ladder") or {}
    integrity = edge.get("integrity") or {}
    aggregate = account.get("aggregate") or {}
    snapshot_state = report_value(aggregate.get("snapshot_state"))
    account_values_current = aggregate.get("account_values_current") is True
    positions_current = aggregate.get("positions_current") is True
    orders_current = aggregate.get("orders_current") is True
    source_health = news.get("source_health") or {}
    now = datetime.now(timezone.utc).isoformat()
    lines = [
        "# Complete Forex source audit — post-upgrade proof and governance state",
        "",
        f"Generated: `{now}` from `C:\\Users\\zmoor\\Documents\\forex\\trad`.",
        "",
        "This document replaces the earlier 11:10 UTC inventory as the current",
        "point-in-time architecture and evidence scorecard. The original is",
        "preserved as `COMPLETE_SOURCE_AUDIT_20260806.md` and explicitly labeled",
        "as the pre-upgrade baseline; incompatible counts are not merged.",
        "",
        "## Decision",
        "",
        f"- Allocator: **{(edge.get('shadow_allocator') or {}).get('action', 'no_trade')}**.",
        f"- Canonical cells: `{integer(edge.get('cell_count')):,}`.",
        f"- Multiplicity-adjusted discovery candidates: `{integer(edge.get('discovery_candidate_count')):,}`.",
        f"- Locked prospective confirmations: `{integer(edge.get('confirmation_passing_count')):,}`.",
        "- Practice canary auto-routing: disabled.",
        "- Real-money routing: disabled and outside this phase.",
        "",
        "## Canonical proof contract",
        "",
        "- Forecasts, outcomes, integrity misses, cohort definitions, transitions,",
        "  completed-day snapshots, and governance snapshots are append-only.",
        "- Evaluation unit: family × pair × horizon × UTC session × executable",
        "  spread/liquidity bucket, deduplicated by signed currency factor and",
        "  market episode.",
        "- Promotion requires discovery control, time-uniform monitoring control,",
        "  and a later untouched confirmation cohort. A discovery window cannot",
        "  certify itself.",
        "- Minimum economic effects are explicit by horizon and liquidity. Small",
        "  positive EV is not a sufficient trading result.",
        "- Pooled graph/tabular estimates may prioritize research, but direct",
        "  cell-level prospective evidence is mandatory.",
        "",
        "## Evidence census",
        "",
        f"- Usable exact-horizon signal outcomes: `{integer(integrity.get('usable_signal_outcomes')):,}`.",
        f"- Raw-N percentiles: `{json.dumps(census.get('raw_n_distribution') or {}, sort_keys=True)}`.",
        f"- Effective-N percentiles: `{json.dumps(census.get('effective_n_distribution') or {}, sort_keys=True)}`.",
        f"- Effective N ≥25/50/100/200/500: `{json.dumps(census.get('effective_n_threshold_counts') or {}, sort_keys=True)}`.",
        f"- Positive point EV / positive unadjusted LCB / multiplicity survivors: `{integer(census.get('positive_point_ev_cells'))}` / `{integer(census.get('positive_unadjusted_lcb_cells'))}` / `{integer(census.get('multiplicity_survivor_cells'))}`.",
        f"- Economically inadequate at current power: `{integer(census.get('economically_inadequate_with_current_power_cells'))}` cells.",
        f"- Blocked only by sample size: `{integer(census.get('blocked_only_by_sample_size_cells'))}` cells.",
        f"- Hierarchical FDR survivors: `{integer(multiplicity.get('family_survivors'))}` family/horizon and `{integer(multiplicity.get('cell_survivors'))}` cell.",
        f"- Governance fingerprint: `{governance.get('governance_sha256', 'pending-refresh')}`.",
        "",
        "## Frozen proof cohorts",
        "",
        "| Family | Active cohort | Prior/superseded forecasts excluded | Governed forecasts | Matured |",
        "|---|---|---:|---:|---:|",
    ]
    governed_by_family = {
        str(row.get("family")): row
        for row in ((governance.get("proof_cohorts") or {}).get("cohorts") or [])
    }
    active_ids = set((cohorts.get("active_cohorts") or {}).values())
    for row in cohorts.get("cohorts") or []:
        if row.get("cohort_id") not in active_ids:
            continue
        family = str(row.get("family"))
        evidence = governed_by_family.get(family) or {}
        lines.append(
            f"| {family} | `{row.get('cohort_id')}` | {integer(row.get('pre_governance_forecast_count'))} | {integer(evidence.get('forecast_count'))} | {integer(evidence.get('matured_outcome_count'))} |"
        )
    lines.extend(
        [
            "",
            f"The proof worker is observation-only, has `{integer(proof.get('errors'))}` errors, and cannot place orders.",
            "",
            "## Graduation ladder",
            "",
            f"- Stage A contract validity: `{(ladder.get('A_contract_validity') or {}).get('passed', False)}`.",
            f"- Stage B discovery candidates: `{integer((ladder.get('B_discovery_candidate') or {}).get('count'))}`.",
            f"- Stage C locked confirmations: `{integer((ladder.get('C_locked_prospective_confirmation') or {}).get('count'))}`.",
            f"- Stage D practice canaries: `{integer((ladder.get('D_practice_canary') or {}).get('count'))}`.",
            "",
            "## News and macro state",
            "",
            f"- News sources: `{integer(source_health.get('configured'))}` configured, `{integer(news.get('operational_sources'))}` operational, `{integer(source_health.get('healthy'))}` healthy.",
            f"- Macro ledger: `{integer(macro.get('release_count'))}` releases, `{integer(macro.get('scheduled_release_count'))}` scheduled, `{integer(macro.get('actual_count'))}` actuals.",
            f"- Causal pre-release consensus observations: `{integer(macro.get('causal_consensus_observation_count'))}`; usable actual-plus-consensus surprises: `{integer(macro.get('actual_and_consensus_count'))}`.",
            "- Post-release consensus remains rejected unless a verifiable causal",
            "  pre-release snapshot exists. Narrative news cannot substitute.",
            "",
            "## Account and execution separation",
            "",
            f"- Account snapshot state: `{snapshot_state}`.",
            f"- Currentness — account values / positions / orders: `{report_currentness(aggregate.get('account_values_current'))}` / `{report_currentness(aggregate.get('positions_current'))}` / `{report_currentness(aggregate.get('orders_current'))}`.",
            f"- Current account errors: `{report_account_errors(aggregate.get('current_errors'))}`.",
            f"- Practice 007 balance/NAV: `{report_value(aggregate.get('balance'), current=account_values_current)}` / `{report_value(aggregate.get('nav'), current=account_values_current)}`.",
            f"- Open trades/orders: `{report_value(aggregate.get('openTradeCount'), current=positions_current)}` / `{report_value(aggregate.get('pendingOrderCount'), current=orders_current)}`.",
            "- Proof and governance modules have no order route. Confirmed research",
            "  would still require a separately locked Practice 007 canary whose",
            "  purpose is execution validation, not rediscovery of profitability.",
            "",
            "## Remaining blockers",
            "",
            "1. Governed H1 proof forecasts need later executable-side maturities.",
            "2. No discovery candidate survives the full multiplicity, sequential,",
            "   economic-effect, stability, cost, and concentration contract.",
            "3. The macro ledger still lacks a causal consensus source.",
            "4. Order book, position book, and informative pricing depth remain",
            "   unavailable and excluded.",
            "5. One-bar/delayed-entry path stress requires a timestamp-complete",
            "   executable quote path before any candidate confirmation.",
            "",
            "The phase succeeds either by confirming robust positive after-cost edge",
            "or by accumulating enough power to reject the configured minimum",
            "economic effect. `no_trade` is therefore a valid and expected result.",
            "",
        ]
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text("\n".join(lines), encoding="utf-8")
    temporary.replace(output)
    return output


if __name__ == "__main__":
    print(generate())
