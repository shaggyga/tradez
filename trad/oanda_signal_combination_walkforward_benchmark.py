#!/usr/bin/env python3
"""Compare legacy and replicated signal-combination rules on an untouched tail."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from oanda_signal_combination_audit import (
        chronological_partitions,
        fuzzy_membership,
        mine_fuzzy_rules,
        read_training_rows,
    )
except ModuleNotFoundError:
    from trad.oanda_signal_combination_audit import (
        chronological_partitions,
        fuzzy_membership,
        mine_fuzzy_rules,
        read_training_rows,
    )


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATABASE = (
    PROJECT_ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "signal_combination_audit_v1.sqlite"
)
PROFILE_THRESHOLDS = {
    "loose": (0.45, 80.0, 30.0, 0.010, 0.03),
    "fast": (0.55, 100.0, 40.0, 0.020, 0.08),
    "balanced": (0.65, 160.0, 60.0, 0.030, 0.15),
    "strict": (0.75, 250.0, 100.0, 0.040, 0.25),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def membership(rule: dict[str, Any], features: dict[str, float]) -> float:
    values: list[float] = []
    for condition in rule.get("conditions") or []:
        name = str(condition.get("feature") or "")
        if name not in features:
            return 0.0
        values.append(
            fuzzy_membership(
                finite(features[name]),
                str(condition.get("operator") or ">="),
                finite(condition.get("threshold")),
                finite(condition.get("width"), 1.0),
            )
        )
    return min(values) if values else 0.0


def rule_passes_profile(rule: dict[str, Any], value: float, profile: str) -> bool:
    min_membership, min_train, min_holdout, min_edge, min_net = PROFILE_THRESHOLDS[profile]
    training = rule.get("training") or {}
    holdout = rule.get("holdout") or {}
    return bool(
        rule.get("account_eligible")
        and value >= min_membership
        and len(rule.get("conditions") or []) >= 2
        and finite(training.get("weighted_support")) >= min_train
        and finite(holdout.get("weighted_support")) >= min_holdout
        and finite(holdout.get("lower_probability_edge")) >= min_edge
        and finite(holdout.get("expected_net_pips")) >= min_net
    )


def evaluate(
    rules: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    profile: str,
) -> dict[str, Any]:
    net: list[float] = []
    correct = 0
    for row in rows:
        scored: list[tuple[float, float, dict[str, Any]]] = []
        for rule in rules:
            value = membership(rule, row.get("features") or {})
            if not rule_passes_profile(rule, value, profile):
                continue
            holdout = rule.get("holdout") or {}
            score = (
                value
                * max(0.0, finite(holdout.get("lower_probability_edge")))
                * math.log1p(max(0.0, finite(holdout.get("weighted_support"))))
            )
            scored.append((score, value, rule))
        if not scored:
            continue
        _, _, best = max(scored, key=lambda item: (item[0], item[1]))
        direction = str(best.get("predicted_direction") or "")
        signed_move = finite(row.get("signed_move_pips"))
        correct += int((signed_move > 0.0) == (direction == "buy"))
        net.append(
            finite(
                row.get("long_net_pips")
                if direction == "buy"
                else row.get("short_net_pips")
            )
        )
    count = len(net)
    return {
        "signals": count,
        "coverage": round(count / max(1, len(rows)), 6),
        "directional_accuracy": round(correct / count, 6) if count else None,
        "average_net_pips": round(sum(net) / count, 6) if count else None,
        "net_positive_rate": round(sum(value > 0.0 for value in net) / count, 6)
        if count
        else None,
        "total_net_pips": round(sum(net), 4),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--horizons", default="300,900,1800,3600")
    parser.add_argument("--max-rows", type=int, default=6000)
    parser.add_argument("--development-fraction", type=float, default=0.80)
    parser.add_argument("--max-rules", type=int, default=40)
    parser.add_argument("--max-conditions", type=int, default=3)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    args.horizons = sorted(
        {int(value.strip()) for value in args.horizons.split(",") if value.strip()}
    )
    if not args.horizons or min(args.horizons) <= 0:
        raise SystemExit("horizons must be positive")
    if args.max_rows < 500 or not 0.5 <= args.development_fraction < 0.95:
        raise SystemExit("max rows or development fraction is outside the safe range")
    return args


def main() -> int:
    args = parse_args()
    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "database": str(args.database.resolve()),
        "policy": "outer chronological tail is never used by either rule fit",
        "config": {
            "horizons_sec": args.horizons,
            "max_rows": args.max_rows,
            "development_fraction": args.development_fraction,
            "max_rules": args.max_rules,
            "max_conditions": args.max_conditions,
        },
        "horizons": {},
    }
    for horizon in args.horizons:
        rows = read_training_rows(args.database, horizon, args.max_rows)
        ordered, development_slice, outer_slice, _, outer_partition = (
            chronological_partitions(
                rows,
                horizon,
                1,
                train_fraction=args.development_fraction,
            )
        )
        development = ordered[development_slice]
        outer = ordered[outer_slice]
        horizon_report: dict[str, Any] = {
            "rows": len(rows),
            "development_rows": len(development),
            "outer_rows": len(outer),
            "outer_partition": outer_partition,
            "modes": {},
        }
        for name, blocks in (("legacy_single_holdout", 1), ("replicated_two_block", 2)):
            rules = mine_fuzzy_rules(
                development,
                horizon,
                minimum_train_support=80,
                minimum_holdout_support=30,
                max_rules=args.max_rules,
                max_conditions=args.max_conditions,
                beam_width=24,
                expansion_conditions=20,
                maximum_base_features=24,
                conditions_per_feature=2,
                chronological_validation_blocks=blocks,
            )
            horizon_report["modes"][name] = {
                "rule_count": len(rules),
                "account_eligible_rule_count": sum(
                    bool(rule.get("account_eligible")) for rule in rules
                ),
                "profiles": {
                    profile: evaluate(rules, outer, profile)
                    for profile in PROFILE_THRESHOLDS
                },
            }
        report["horizons"][str(horizon)] = horizon_report
        print(
            json.dumps(
                {"horizon_sec": horizon, **horizon_report},
                separators=(",", ":"),
            ),
            flush=True,
        )
    report["completed_utc"] = utc_now()
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
