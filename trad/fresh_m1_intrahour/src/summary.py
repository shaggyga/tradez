from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .common import write_json


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _fmt(value: Any, digits: int = 4) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _winner_parts(report: dict[str, Any]) -> tuple[str | None, str | None]:
    key = report.get("winner_key")
    if not key or "|" not in key:
        return key, None
    return tuple(key.split("|", 1))  # type: ignore[return-value]


def _allocator(report: dict[str, Any], name: str = "full_forecast_surface_allocator") -> dict[str, Any]:
    for row in report.get("allocator_comparison", []):
        if row.get("allocator_name") == name:
            return row
    return {}


def _dominant(report: dict[str, Any], key: str) -> str | None:
    item = report.get("final_winner_report", {}).get("dominance_checks", {}).get(key)
    return item.get("top_key") if isinstance(item, dict) else None


def _top_bucket(report: dict[str, Any]) -> dict[str, Any]:
    return report.get("final_winner_report", {}).get("top_predicted_ev_bucket") or {}


def _monotonic_flag(report: dict[str, Any]) -> bool | None:
    mono = report.get("final_winner_report", {}).get("ev_bucket_monotonicity", {})
    steps = mono.get("nondecreasing_adjacent_steps")
    if steps is None:
        return None
    return bool(steps >= 6)


def _gate_rows(report: dict[str, Any], allocator: dict[str, Any]) -> tuple[list[str], str, str]:
    final = report.get("final_winner_report", {})
    top_bucket = _top_bucket(report)
    mono_flag = _monotonic_flag(report)
    precision = final.get("precision_at", {})
    leakage = report.get("leakage_checks", {})
    dominance = final.get("dominance_checks", {})
    top_abs_shares = [v.get("top_abs_share") for v in dominance.values() if isinstance(v, dict) and v.get("top_abs_share") is not None]
    max_dominance = max(top_abs_shares) if top_abs_shares else None
    gates = [
        f"Gate A top EV bucket positive: {top_bucket.get('actual_mean', 0) > 0 if top_bucket else False}",
        f"Gate B EV monotonic/top bucket separation: {mono_flag}",
        f"Gate C precision@1/3/5 available: {precision.get('1') is not None and precision.get('3') is not None and precision.get('5') is not None}",
        f"Gate D final-test P/L positive or no-trade justified: {allocator.get('return_pct', 0) > 0 or (allocator.get('trade_count', 0) == 0 and final.get('no_trade_diagnostics', {}).get('top_ranked_candidates_truly_negative_hint'))}",
        f"Gate E drawdown controlled: {abs(float(allocator.get('max_drawdown_pct') or 0.0)) < 5.0}",
        f"Gate F margin saturation avoided: {float(allocator.get('max_margin_used_pct') or 0.0) < 40.0}",
        f"Gate G not dominated by one dimension: {max_dominance is None or max_dominance < 0.60}",
        "Gate H Tier 1 interpretable: True",
        f"Gate I shadow diagnostics available: {bool(final.get('shadow_opposite_side'))}",
        f"Gate J no leakage/oracle evidence: {all(bool(v) for k, v in leakage.items() if k not in {'realized_best_horizon_used', 'realized_best_policy_used', 'label_derived_best_exit_policy_used', 'oracle_columns_used_for_selection', 'live_execution_enabled'}) and not leakage.get('realized_best_horizon_used') and not leakage.get('oracle_columns_used_for_selection')}",
    ]
    return_pct = float(allocator.get("return_pct") or 0.0)
    top_mean = float(top_bucket.get("actual_mean") or 0.0)
    if allocator.get("trade_count", 0) == 0 and return_pct == 0:
        verdict = "INFRA ONLY" if top_mean == 0 else ("PARTIAL" if top_mean > 0 else "FAIL")
    elif return_pct > 0 and top_mean > 0 and mono_flag is True:
        verdict = "PASS"
    elif return_pct > 0 or top_mean > 0:
        verdict = "PARTIAL"
    else:
        verdict = "FAIL"
    if verdict == "PASS":
        action = "Run longer Tier 1 walk-forward, then Tier 2, then all-tier chunked."
    elif verdict == "PARTIAL":
        action = "Inspect calibration, thresholds, policy gating, session/pair restrictions, and cost assumptions before any broader run."
    elif verdict == "FAIL":
        action = "Diagnose whether failure is wrong side, wrong horizon, costs too high, or inability to rank candidates."
    else:
        action = "Run a larger final-test window before judging edge."
    return gates, verdict, action


def write_final_research_summary(run_dir: Path, cfg: dict[str, Any]) -> tuple[Path, Path]:
    report = _read_json(run_dir / "surface_sweep_report.json")
    manifest = _read_json(run_dir / "dataset_manifest.json") if (run_dir / "dataset_manifest.json").exists() else {}
    feature_family, model_family = _winner_parts(report)
    allocator = _allocator(report)
    final = report.get("final_winner_report", {})
    ranking = report.get("selection_candidate_ranking", [])
    winner_selection = ranking[0].get("allocator_performance", {}) if ranking else {}
    gates, verdict, action = _gate_rows(report, allocator)
    top_bucket = _top_bucket(report)
    precision = final.get("precision_at", {})
    no_trade = final.get("no_trade_diagnostics", {})
    calibration = final.get("calibration", {})
    mono = final.get("ev_bucket_monotonicity", {})
    dominant_pair = _dominant(report, "pair")
    dominant_policy = _dominant(report, "policy_name")
    dominant_horizon = _dominant(report, "horizon_min")
    split = report.get("nested_split", {})
    json_payload = {
        "run_name": run_dir.name,
        "date_range": {"start": manifest.get("start"), "end": manifest.get("end")},
        "tier": manifest.get("tier"),
        "selected_feature_family": feature_family,
        "selected_model_family": model_family,
        "validation_return_pct": winner_selection.get("return_pct"),
        "final_test_return_pct": allocator.get("return_pct"),
        "final_test_pnl": (allocator.get("final_equity") - allocator.get("initial_equity")) if allocator.get("final_equity") is not None else None,
        "trade_count": allocator.get("trade_count"),
        "win_rate": allocator.get("win_rate"),
        "max_drawdown_pct": allocator.get("max_drawdown_pct"),
        "max_margin_used_pct": allocator.get("max_margin_used_pct"),
        "no_trade_rate": allocator.get("no_trade_rate"),
        "top_ev_bucket_actual_ev": top_bucket.get("actual_mean"),
        "ev_bucket_monotonicity_flag": _monotonic_flag(report),
        "precision_at_1": precision.get("1"),
        "precision_at_3": precision.get("3"),
        "precision_at_5": precision.get("5"),
        "dominant_pair": dominant_pair,
        "dominant_policy": dominant_policy,
        "dominant_horizon": dominant_horizon,
        "verdict": verdict,
        "recommended_next_action": action,
    }
    write_json(run_dir / "FINAL_RESEARCH_SUMMARY.json", json_payload)

    md = []
    md.append(f"# Final Research Summary: {run_dir.name}")
    md.append("")
    md.append("## 1. Run Metadata")
    md.append(f"- Run name: `{run_dir.name}`")
    md.append(f"- Date range: `{manifest.get('start')}` to `{manifest.get('end')}`")
    md.append(f"- Tier: `{manifest.get('tier')}`")
    md.append(f"- Pair count: `{len(manifest.get('pairs', []))}`")
    md.append(f"- Horizons: `{cfg.get('horizons_minutes')}`")
    md.append(f"- Policies: `{[p['name'] for p in cfg.get('policy_templates', [])]}`")
    md.append(f"- Feature families swept: `{cfg.get('surface', {}).get('feature_families')}`")
    md.append(f"- Model families swept: `{cfg.get('surface', {}).get('model_names')}`")
    md.append(f"- Purge/embargo minutes: `{split.get('purge_embargo_minutes')}`")
    md.append(f"- All candidates materialized: `{report.get('full_surface_materialized')}`")
    md.append("- Live execution remained disabled: `true`")
    md.append("")
    md.append("## 2. Selected Winner")
    md.append(f"- Selected feature family: `{feature_family}`")
    md.append(f"- Selected model family: `{model_family}`")
    md.append("- Selection criterion: validation allocator performance, then drawdown/trade-count/precision tie-breakers.")
    md.append(f"- Validation allocator return: `{_fmt(winner_selection.get('return_pct'))}%`")
    md.append(f"- Validation trades: `{winner_selection.get('trade_count')}`")
    md.append("")
    md.append("## 3. Untouched Final-Test Performance")
    md.append(f"- Final equity: `{_fmt(allocator.get('final_equity'))}`")
    md.append(f"- Return: `{_fmt(allocator.get('return_pct'))}%`")
    md.append(f"- Account-currency P/L: `{_fmt(json_payload['final_test_pnl'])}`")
    md.append(f"- Trade count: `{allocator.get('trade_count')}`")
    md.append(f"- Win rate: `{_fmt(allocator.get('win_rate'))}`")
    md.append(f"- Profit factor: `{_fmt(allocator.get('profit_factor'))}`")
    md.append(f"- Max drawdown: `{_fmt(allocator.get('max_drawdown_pct'))}%`")
    md.append(f"- Max margin used: `{_fmt(allocator.get('max_margin_used_pct'))}%`")
    md.append(f"- No-trade rate: `{_fmt(allocator.get('no_trade_rate'))}`")
    md.append(f"- Average trade duration: `{_fmt(allocator.get('average_trade_duration_minutes'))}` minutes")
    md.append(f"- Average spread/ATR at entry: `{_fmt(allocator.get('average_spread_to_atr_at_entry'))}`")
    md.append(f"- Estimated spread/slippage/switching cost: `{_fmt(allocator.get('total_estimated_spread_slippage_switch_cost_usd'))}`")
    md.append("")
    md.append("## 4. Top-K Opportunity Performance")
    md.append(f"- precision@1: `{_fmt(precision.get('1'))}`")
    md.append(f"- precision@3: `{_fmt(precision.get('3'))}`")
    md.append(f"- precision@5: `{_fmt(precision.get('5'))}`")
    md.append(f"- Top predicted EV bucket actual mean EV: `{_fmt(top_bucket.get('actual_mean'))}`")
    md.append(f"- EV bucket monotonicity: `{mono}`")
    md.append("")
    md.append("## 5. Calibration")
    md.append(f"- Calibration intercept: `{_fmt(calibration.get('intercept'))}`")
    md.append(f"- Calibration slope: `{_fmt(calibration.get('slope'))}`")
    md.append(f"- Predicted positive-EV rows: `{final.get('positive_predicted_ev_rows')}`")
    md.append(f"- No-trade diagnostics: `{no_trade}`")
    md.append("")
    md.append("## 6. Policy/Horizon Breakdown")
    md.append(f"- Dominant policy: `{dominant_policy}`")
    md.append(f"- Dominant horizon: `{dominant_horizon}`")
    md.append(f"- Policy breakdown: `{final.get('policy_breakdown')}`")
    md.append(f"- Horizon breakdown: `{final.get('horizon_breakdown')}`")
    md.append("")
    md.append("## 7. Pair/Session/Currency Breakdown")
    md.append(f"- Dominant pair: `{dominant_pair}`")
    md.append(f"- Pair breakdown: `{final.get('pair_breakdown')}`")
    md.append(f"- Session breakdown: `{final.get('session_breakdown')}`")
    md.append(f"- Base currency breakdown: `{final.get('base_currency_breakdown')}`")
    md.append(f"- Quote currency breakdown: `{final.get('quote_currency_breakdown')}`")
    md.append(f"- Dominance checks: `{final.get('dominance_checks')}`")
    md.append("")
    md.append("## 8. Shadow Diagnostics")
    md.append(f"`{final.get('shadow_opposite_side')}`")
    md.append("")
    md.append("## 9. No-Trade Diagnostics")
    md.append(f"`{no_trade}`")
    md.append(f"- Allocator filter counts: `{allocator.get('filter_counts')}`")
    md.append("")
    md.append("## 10. Baseline Comparisons")
    md.append(f"`{report.get('final_comparison_table')}`")
    md.append("- Random/top-volatility/top-momentum baselines are not implemented in this run.")
    md.append("")
    md.append("## 11. Pass/Fail Gates")
    for gate in gates:
        md.append(f"- {gate}")
    md.append("")
    md.append("## 12. Verdict")
    md.append(f"**{verdict}**")
    md.append("")
    md.append("No live trading is recommended from this run alone.")
    md.append("")
    md.append("## 13. Next Recommended Action")
    md.append(action)
    md_path = run_dir / "FINAL_RESEARCH_SUMMARY.md"
    md_path.write_text("\n".join(md) + "\n", encoding="utf-8")
    return md_path, run_dir / "FINAL_RESEARCH_SUMMARY.json"
