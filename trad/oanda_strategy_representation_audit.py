#!/usr/bin/env python3
"""Build a truthful strategy-coverage and independence audit.

The strategy lab exposes many named families and parameter lanes.  This audit
separates nominal breadth from independent archetype breadth, observed outcome
coverage, and validated executable evidence.  It is read-only with respect to
broker and execution state.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from oanda_signal_combination_audit import rule_feature_domain_summary
    from oanda_strategy_archetypes import (
        ARCHETYPE_DEFINITIONS,
        strategy_archetype,
    )
except ModuleNotFoundError:
    from trad.oanda_signal_combination_audit import rule_feature_domain_summary
    from trad.oanda_strategy_archetypes import (
        ARCHETYPE_DEFINITIONS,
        strategy_archetype,
    )


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
PATTERN_SUMMARY = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "all68_pattern_research_20260802"
    / "summary.json"
)
DEFAULT_STATE_OUTPUT = STATE / "strategy_representation_audit_v1.json"
DEFAULT_REPORT_OUTPUT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "strategy_quality_pass_20260802"
    / "STRATEGY_REPRESENTATION_AUDIT_20260802.md"
)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    for _ in range(3):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except (OSError, json.JSONDecodeError):
            continue
    return {}


def source_constants(path: Path) -> dict[str, Any]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    requested = {"FAMILIES", "DEFAULT_FAMILIES", "PROFILES"}
    output: dict[str, Any] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in requested:
                continue
            output[target.id] = ast.literal_eval(node.value)
    missing = requested.difference(output)
    if missing:
        raise RuntimeError(f"Missing strategy constants: {sorted(missing)}")
    return output


def _sample_bin(value: int) -> str:
    if value <= 0:
        return "0"
    if value < 100:
        return "1-99"
    if value < 1_000:
        return "100-999"
    if value < 10_000:
        return "1,000-9,999"
    return "10,000+"


def build_audit(
    *,
    families: list[str] | tuple[str, ...],
    default_families: list[str] | tuple[str, ...],
    profiles: list[str] | tuple[str, ...],
    promotion: dict[str, Any],
    exit_fit: dict[str, Any],
    calibration: dict[str, Any],
    pattern_summary: dict[str, Any],
    last_signal: dict[str, Any] | None = None,
    combination_artifacts: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    catalogue = [str(value) for value in families]
    default = [str(value) for value in default_families]
    profile_names = [str(value) for value in profiles]
    expected_lanes = {
        f"{family}.{profile}"
        for family in default
        for profile in profile_names
    } | {"ahl_multihorizon_trend.shadow"}

    evidence = [
        row
        for row in promotion.get("signal_evidence") or []
        if isinstance(row, dict)
    ]
    evidence_lanes = {
        str(row.get("lane_id") or "")
        for row in evidence
        if str(row.get("lane_id") or "")
    }
    promotion_blocker_counts = Counter(
        str(reason)
        for row in evidence
        for reason in (row.get("blocked_by") or [])
        if str(reason)
    )
    blocker_count_distribution = Counter(
        len(row.get("blocked_by") or []) for row in evidence
    )
    near_eligible_evidence = sorted(
        (
            {
                "lane_id": str(row.get("lane_id") or ""),
                "family": str(row.get("family") or ""),
                "archetype": strategy_archetype(str(row.get("family") or "")),
                "horizon_sec": int(row.get("horizon_sec") or 0),
                "blocked_by": [str(value) for value in row.get("blocked_by") or []],
                "sample_count": int(row.get("sample_count") or 0),
                "pair_count": int(row.get("pair_count") or 0),
                "score": float(row.get("score") or 0.0),
                "training_average_pips": float(
                    (row.get("training") or {}).get("avg") or 0.0
                ),
                "holdout_average_pips": float(
                    (row.get("holdout") or {}).get("avg") or 0.0
                ),
            }
            for row in evidence
            if 0 < len(row.get("blocked_by") or []) <= 2
        ),
        key=lambda row: (len(row["blocked_by"]), -row["score"]),
    )[:12]
    observed_expected = expected_lanes.intersection(evidence_lanes)
    family_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in evidence:
        family_rows[str(row.get("family") or "")].append(row)

    catalogue_family_metrics: list[dict[str, Any]] = []
    for family in sorted(catalogue):
        rows = family_rows.get(family, [])
        expected_for_family = sorted(
            lane for lane in expected_lanes if lane.rsplit(".", 1)[0] == family
        )
        observed_for_family = sorted(set(expected_for_family).intersection(evidence_lanes))
        catalogue_family_metrics.append(
            {
                "family": family,
                "archetype": strategy_archetype(family),
                "expected_lanes": len(expected_for_family),
                "observed_lanes": len(observed_for_family),
                "missing_lanes": sorted(set(expected_for_family) - set(observed_for_family)),
                "evidence_rows": len(rows),
                "horizon_count": len(
                    {int(row.get("horizon_sec") or 0) for row in rows}
                ),
                "maximum_samples": max(
                    [int(row.get("sample_count") or 0) for row in rows],
                    default=0,
                ),
                "maximum_pair_breadth": max(
                    [int(row.get("pair_count") or 0) for row in rows],
                    default=0,
                ),
                "eligible_evidence": sum(bool(row.get("eligible")) for row in rows),
            }
        )

    archetype_families: dict[str, list[str]] = defaultdict(list)
    for family in catalogue:
        archetype_families[strategy_archetype(family)].append(family)
    family_metric_by_name = {
        str(row["family"]): row for row in catalogue_family_metrics
    }
    archetypes = [
        {
            "archetype": archetype,
            **ARCHETYPE_DEFINITIONS[archetype],
            "family_count": len(names),
            "families": sorted(names),
            "expected_lanes": sum(
                int(family_metric_by_name[name]["expected_lanes"])
                for name in names
            ),
            "observed_lanes": sum(
                int(family_metric_by_name[name]["observed_lanes"])
                for name in names
            ),
            "evidence_rows": sum(
                int(family_metric_by_name[name]["evidence_rows"])
                for name in names
            ),
            "eligible_evidence": sum(
                int(family_metric_by_name[name]["eligible_evidence"])
                for name in names
            ),
        }
        for archetype, names in sorted(archetype_families.items())
    ]
    for row in archetypes:
        expected = int(row["expected_lanes"])
        observed = int(row["observed_lanes"])
        row["observed_lane_pct"] = (
            round(100.0 * observed / expected, 3) if expected else None
        )

    sample_bins = Counter(
        _sample_bin(int(row["maximum_samples"]))
        for row in catalogue_family_metrics
    )
    eligible_exit = exit_fit.get("eligible") or {}
    exit_horizon_rows: list[dict[str, Any]] = []
    sampled_exit_scopes: list[dict[str, Any]] = []
    for horizon_key, horizon_state in sorted(
        (exit_fit.get("horizon_states") or {}).items(),
        key=lambda item: int(item[0]),
    ):
        if not isinstance(horizon_state, dict):
            continue
        horizon = int(horizon_key)
        global_fit = horizon_state.get("global") or {}
        if isinstance(global_fit, dict) and global_fit:
            holdout = global_fit.get("holdout") or {}
            exit_horizon_rows.append(
                {
                    "horizon_sec": horizon,
                    "sample_count": int(global_fit.get("sample_count") or 0),
                    "eligible": bool(global_fit.get("eligible")),
                    "blocked_by": [
                        str(value) for value in global_fit.get("blocked_by") or []
                    ],
                    "holdout_average_pips": float(holdout.get("avg_pips") or 0.0),
                    "holdout_lower_confidence_pips": float(
                        holdout.get("lower_confidence_pips") or 0.0
                    ),
                    "holdout_profit_factor": float(
                        holdout.get("profit_factor") or 0.0
                    ),
                }
            )
        for bucket, label in (
            ("top_families", "family"),
            ("top_lanes", "lane_id"),
            ("top_pairs", "instrument"),
            ("top_regimes", "regime"),
        ):
            for row in horizon_state.get(bucket) or []:
                if not isinstance(row, dict):
                    continue
                sampled_exit_scopes.append(
                    {
                        "horizon_sec": horizon,
                        "scope": bucket,
                        "scope_key": str(row.get(label) or ""),
                        "sample_count": int(row.get("sample_count") or 0),
                        "eligible": bool(row.get("eligible")),
                        "blocked_by": [
                            str(value) for value in row.get("blocked_by") or []
                        ],
                        "holdout_average_pips": float(
                            (row.get("holdout") or {}).get("avg_pips") or 0.0
                        ),
                        "holdout_lower_confidence_pips": float(
                            (row.get("holdout") or {}).get(
                                "lower_confidence_pips"
                            )
                            or 0.0
                        ),
                    }
                )
    exit_blocker_counts = Counter(
        reason
        for row in sampled_exit_scopes
        for reason in row["blocked_by"]
    )
    exit_blocker_depth = Counter(
        len(row["blocked_by"]) for row in sampled_exit_scopes
    )
    closest_exit_scopes = sorted(
        sampled_exit_scopes,
        key=lambda row: (
            len(row["blocked_by"]),
            -row["holdout_lower_confidence_pips"],
            -row["sample_count"],
        ),
    )[:12]
    ready_surfaces = int(calibration.get("ready_surface_count") or 0)
    surface_count = int(calibration.get("surface_count") or 0)
    event_family_count = len(archetype_families.get("event_fundamental", []))
    top_signals = [
        row
        for row in (last_signal or {}).get("top_signals") or []
        if isinstance(row, dict)
    ]
    operational_rows: list[dict[str, Any]] = []
    operational_unclassified: set[str] = set()
    for row in top_signals:
        component_families = sorted(
            {
                str(value)
                for value in row.get("component_families") or []
                if str(value)
            }
        )
        component_archetypes = sorted(
            {strategy_archetype(family) for family in component_families}
        )
        operational_unclassified.update(
            family
            for family in component_families
            if strategy_archetype(family) == "unclassified"
        )
        operational_rows.append(
            {
                "instrument": str(row.get("instrument") or ""),
                "direction": str(row.get("direction") or ""),
                "family_count": len(component_families),
                "archetype_count": len(component_archetypes),
                "families": component_families,
                "archetypes": component_archetypes,
                "agreement_family_count": int(
                    row.get("agreement_family_count") or 0
                ),
                "opposing_family_count": int(row.get("opposing_family_count") or 0),
            }
        )
    average_operational_families = (
        sum(row["family_count"] for row in operational_rows) / len(operational_rows)
        if operational_rows
        else 0.0
    )
    average_operational_archetypes = (
        sum(row["archetype_count"] for row in operational_rows) / len(operational_rows)
        if operational_rows
        else 0.0
    )
    combination_rows: list[dict[str, Any]] = []
    for name, payload in sorted((combination_artifacts or {}).items()):
        rules = [
            row for row in payload.get("rules") or [] if isinstance(row, dict)
        ]
        summaries = [
            rule_feature_domain_summary(row.get("conditions") or [])
            for row in rules
        ]
        raw_conditions = sum(
            int(row.get("condition_count") or len(row.get("conditions") or []))
            for row in rules
        )
        independent_domains = sum(
            int(row.get("feature_domain_count") or summary["feature_domain_count"])
            for row, summary in zip(rules, summaries)
        )
        redundant_rules = sum(
            int(summary["redundant_condition_count"]) > 0
            for summary in summaries
        )
        unclassified = sorted(
            {
                feature
                for summary in summaries
                for feature in summary["unclassified_features"]
            }
        )
        combination_rows.append(
            {
                "artifact": name,
                "generated_at": payload.get("generated_at"),
                "rule_count": len(rules),
                "account_eligible_rule_count": int(
                    payload.get("account_eligible_rule_count") or 0
                ),
                "raw_condition_count": raw_conditions,
                "independent_feature_domain_count": independent_domains,
                "average_conditions_per_rule": round(
                    raw_conditions / len(rules), 3
                ) if rules else 0.0,
                "average_domains_per_rule": round(
                    independent_domains / len(rules), 3
                ) if rules else 0.0,
                "rules_with_same_domain_redundancy": redundant_rules,
                "redundant_rule_pct": round(
                    100.0 * redundant_rules / len(rules), 3
                ) if rules else 0.0,
                "unclassified_features": unclassified,
                "domain_fields_live": bool(payload.get("feature_domain_policy")),
            }
        )
    combination_rule_count = sum(row["rule_count"] for row in combination_rows)
    combination_redundant_rules = sum(
        row["rules_with_same_domain_redundancy"] for row in combination_rows
    )
    combination_unclassified = sorted(
        {
            feature
            for row in combination_rows
            for feature in row["unclassified_features"]
        }
    )
    gaps: list[dict[str, Any]] = []
    if event_family_count == 0:
        gaps.append(
            {
                "gap": "event_fundamental_strategy",
                "severity": "high",
                "evidence": "News/event data is an influence and trial ablation, not a default independent strategy family.",
            }
        )
    if not promotion.get("eligible_count"):
        gaps.append(
            {
                "gap": "validated_entry_strategy",
                "severity": "critical",
                "evidence": "No lane/horizon promotion evidence is currently eligible.",
            }
        )
    if not sum(int(value or 0) for value in eligible_exit.values()):
        gaps.append(
            {
                "gap": "validated_exit_strategy",
                "severity": "critical",
                "evidence": "No exit-fit scope is eligible despite bounded path fitting.",
            }
        )
    if ready_surfaces <= 5:
        gaps.append(
            {
                "gap": "pair_intrahour_calibration",
                "severity": "critical",
                "evidence": f"Only {ready_surfaces} of {surface_count} calibration surfaces are validation-ready.",
            }
        )
    if len(observed_expected) < len(expected_lanes):
        gaps.append(
            {
                "gap": "lane_outcome_coverage",
                "severity": "high",
                "evidence": f"{len(observed_expected)} of {len(expected_lanes)} expected lanes have promotion evidence.",
            }
        )
    if combination_redundant_rules:
        gaps.append(
            {
                "gap": "combination_condition_depth_inflation",
                "severity": "high",
                "evidence": (
                    f"{combination_redundant_rules} of {combination_rule_count} "
                    "validated combination rules contain multiple conditions from "
                    "the same information domain."
                ),
            }
        )

    return {
        "schema_version": 1,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "status": "shadow_audit",
        "execution_effect": "none",
        "independence_policy": "one_normalized_vote_per_archetype_shadow_v1",
        "counts": {
            "catalogue_families": len(catalogue),
            "independent_archetypes": len(archetypes),
            "profiles": len(profile_names),
            "expected_lanes": len(expected_lanes),
            "observed_expected_lanes": len(observed_expected),
            "observed_expected_lane_pct": round(
                100.0 * len(observed_expected) / len(expected_lanes), 3
            )
            if expected_lanes
            else 0.0,
            "promotion_evidence_rows": len(evidence),
            "promotion_eligible_rows": sum(bool(row.get("eligible")) for row in evidence),
            "promotion_eligible_lanes": int(promotion.get("eligible_lane_count") or 0),
            "promotion_rows_one_blocker": int(blocker_count_distribution.get(1, 0)),
            "promotion_rows_two_blockers": int(blocker_count_distribution.get(2, 0)),
            "exit_fit_signal_rows": int(
                (exit_fit.get("counts") or {}).get("fit_signal_rows") or 0
            ),
            "exit_eligible_scopes": sum(int(value or 0) for value in eligible_exit.values()),
            "exit_global_horizons": len(exit_horizon_rows),
            "exit_global_positive_holdout_horizons": sum(
                row["holdout_average_pips"] > 0.0 for row in exit_horizon_rows
            ),
            "exit_sampled_top_scopes": len(sampled_exit_scopes),
            "calibration_surfaces": surface_count,
            "calibration_ready_surfaces": ready_surfaces,
            "pattern_combinations_tested": int(
                pattern_summary.get("combinations_tested") or 0
            ),
            "pattern_combinations_replicated": int(
                pattern_summary.get("replicated_profitable_count") or 0
            ),
            "event_fundamental_families": event_family_count,
            "latest_top_signals": len(operational_rows),
            "latest_top_signal_average_families": round(
                average_operational_families, 3
            ),
            "latest_top_signal_average_archetypes": round(
                average_operational_archetypes, 3
            ),
            "latest_top_signal_unclassified_families": len(
                operational_unclassified
            ),
            "combination_rules": combination_rule_count,
            "combination_rules_with_same_domain_redundancy": (
                combination_redundant_rules
            ),
            "combination_unclassified_features": len(
                combination_unclassified
            ),
        },
        "sample_coverage_bins": dict(sorted(sample_bins.items())),
        "promotion_blockers": {
            "thresholds": dict(promotion.get("thresholds") or {}),
            "counts": dict(promotion_blocker_counts.most_common()),
            "percentages": {
                reason: round(100.0 * count / len(evidence), 3)
                if evidence
                else 0.0
                for reason, count in promotion_blocker_counts.most_common()
            },
            "blocked_count_distribution": {
                str(count): rows
                for count, rows in sorted(blocker_count_distribution.items())
            },
            "near_eligible_evidence": near_eligible_evidence,
            "policy": "diagnostic_only_no_threshold_relaxation",
        },
        "exit_fit_blockers": {
            "global_horizons": exit_horizon_rows,
            "sampled_top_scope_count": len(sampled_exit_scopes),
            "counts": dict(exit_blocker_counts.most_common()),
            "blocked_count_distribution": {
                str(count): rows
                for count, rows in sorted(exit_blocker_depth.items())
            },
            "closest_sampled_scopes": closest_exit_scopes,
            "policy": "diagnostic_only_no_eligibility_change",
        },
        "archetypes": archetypes,
        "families": catalogue_family_metrics,
        "latest_operational_snapshot": {
            "observed_at": (last_signal or {}).get("observed_at"),
            "signals": operational_rows,
            "unclassified_families": sorted(operational_unclassified),
            "all_component_families_mapped": not operational_unclassified,
            "all_signals_one_family_per_archetype": bool(operational_rows)
            and all(
                row["family_count"] == row["archetype_count"]
                for row in operational_rows
            ),
        },
        "combination_feature_domains": {
            "policy": "independent_information_domain_count_shadow_v1",
            "artifacts": combination_rows,
            "unclassified_features": combination_unclassified,
            "execution_effect": "none",
        },
        "gaps": gaps,
        "interpretation": {
            "nominal_technical_breadth": "broad",
            "independent_strategy_breadth": "limited",
            "validated_executable_breadth": "none_currently_eligible",
            "raw_family_votes_are_independent": False,
            "latest_top_signal_component_independence": (
                "one_family_per_archetype"
                if operational_rows
                and all(
                    row["family_count"] == row["archetype_count"]
                    for row in operational_rows
                )
                else "mixed_or_unavailable"
            ),
        },
    }


def markdown_report(audit: dict[str, Any]) -> str:
    counts = audit["counts"]
    lines = [
        "# Strategy Representation Audit — 2026-08-02",
        "",
        "## Outcome",
        "",
        "The lab has broad technical hypothesis coverage, but raw family names are not independent strategy evidence. The new archetype layer is diagnostic-only and does not change execution eligibility.",
        "",
        "## Coverage",
        "",
        f"- {counts['catalogue_families']} named strategy families grouped into {counts['independent_archetypes']} independent archetypes.",
        f"- {counts['observed_expected_lanes']} of {counts['expected_lanes']} expected lane variants ({counts['observed_expected_lane_pct']}%) have promotion evidence.",
        f"- {counts['promotion_evidence_rows']} promotion evidence rows; {counts['promotion_eligible_rows']} eligible rows and {counts['promotion_eligible_lanes']} eligible lanes.",
        f"- {counts['exit_fit_signal_rows']} bounded exit-fit signals; {counts['exit_eligible_scopes']} eligible exit scopes.",
        f"- {counts['calibration_ready_surfaces']} of {counts['calibration_surfaces']} calibration surfaces are validation-ready.",
        f"- {counts['pattern_combinations_replicated']} of {counts['pattern_combinations_tested']} broad all-pair pattern combinations replicated.",
        f"- Latest stored top-signal snapshot: {counts['latest_top_signals']} signals averaging {counts['latest_top_signal_average_families']} component families and {counts['latest_top_signal_average_archetypes']} independent archetypes.",
        f"- Combination artifacts: {counts['combination_rules']} rules, of which {counts['combination_rules_with_same_domain_redundancy']} contain redundant same-domain conditions; {counts['combination_unclassified_features']} features are unclassified.",
        "",
        "## Independent archetypes",
        "",
    ]
    for row in audit["archetypes"]:
        coverage = (
            f"; {row['observed_lanes']}/{row['expected_lanes']} expected lanes observed"
            if row["expected_lanes"]
            else "; catalogue-only (no default lanes)"
        )
        lines.append(
            f"- **{row['label']}** ({row['family_count']} families): "
            + ", ".join(row["families"])
            + coverage
        )
    lines.extend(["", "## Promotion blockers", ""])
    blocker_counts = audit["promotion_blockers"]["counts"]
    blocker_percentages = audit["promotion_blockers"]["percentages"]
    for reason, count in list(blocker_counts.items())[:8]:
        lines.append(
            f"- **{reason}**: {count} rows ({blocker_percentages[reason]}%)."
        )
    lines.append(
        f"- Near eligibility: {counts['promotion_rows_one_blocker']} rows have one blocker and "
        f"{counts['promotion_rows_two_blockers']} have two; thresholds were not relaxed."
    )
    lines.extend(["", "## Exit-fit blockers", ""])
    lines.append(
        f"- {counts['exit_global_positive_holdout_horizons']} of "
        f"{counts['exit_global_horizons']} global horizons have positive outer-holdout average pips."
    )
    exit_counts = audit["exit_fit_blockers"]["counts"]
    for reason, count in list(exit_counts.items())[:8]:
        lines.append(f"- **{reason}**: {count} sampled top scopes.")
    closest = audit["exit_fit_blockers"]["closest_sampled_scopes"]
    if closest:
        lines.append(
            f"- Closest sampled scope still has {len(closest[0]['blocked_by'])} blockers; eligibility was not changed."
        )
    lines.extend(["", "## Combination feature domains", ""])
    for row in audit["combination_feature_domains"]["artifacts"]:
        lines.append(
            f"- **{row['artifact']}**: {row['rule_count']} rules; "
            f"{row['average_conditions_per_rule']} raw conditions versus "
            f"{row['average_domains_per_rule']} independent domains per rule; "
            f"{row['redundant_rule_pct']}% contain same-domain redundancy."
        )
    lines.extend(["", "## Material gaps", ""])
    for row in audit["gaps"]:
        lines.append(f"- **{row['severity']} — {row['gap']}**: {row['evidence']}")
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "Raw family agreement remains available for diagnostics. Independent archetype agreement is emitted beside it under `one_normalized_vote_per_archetype_shadow_v1`; it is shadow-only until chronological forward validation supports using it as an execution gate.",
            "",
        ]
    )
    return "\n".join(lines)


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            Path(temporary).unlink(missing_ok=True)
        except OSError:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-output", type=Path, default=DEFAULT_STATE_OUTPUT)
    parser.add_argument("--report-output", type=Path, default=DEFAULT_REPORT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    constants = source_constants(ROOT / "oanda_practice_shadow_strategy_lab.py")
    audit = build_audit(
        families=constants["FAMILIES"],
        default_families=constants["DEFAULT_FAMILIES"],
        profiles=constants["PROFILES"],
        promotion=load_json(STATE / "lane_promotion_v1.json"),
        exit_fit=load_json(STATE / "strategy_exit_fit_v1.json"),
        calibration=load_json(STATE / "timeframe_matrix_calibration_v1.json"),
        pattern_summary=load_json(PATTERN_SUMMARY),
        last_signal=load_json(STATE / "practice_007_last_signal_v1.json"),
        combination_artifacts={
            "standard": load_json(STATE / "signal_combination_audit_v1.json"),
            "deep": load_json(STATE / "signal_combination_deep_v1.json"),
            "historical": load_json(
                STATE / "signal_combination_historical_v1.json"
            ),
        },
    )
    atomic_text(args.state_output, json.dumps(audit, indent=2, sort_keys=True) + "\n")
    atomic_text(args.report_output, markdown_report(audit))
    print(json.dumps({"status": audit["status"], **audit["counts"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
