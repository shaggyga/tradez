#!/usr/bin/env python3
"""Continuously fit fuzzy signal-combination rules from the -007 audit store."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from oanda_signal_combination_audit import (
        mine_fuzzy_rules,
        read_training_rows,
        write_rule_state,
    )
except ModuleNotFoundError:
    from trad.oanda_signal_combination_audit import (
        mine_fuzzy_rules,
        read_training_rows,
        write_rule_state,
    )


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_STATE_DIR = PROJECT_ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_DATABASE = DEFAULT_STATE_DIR / "signal_combination_audit_v1.sqlite"
DEFAULT_STATE = DEFAULT_STATE_DIR / "signal_combination_audit_v1.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument(
        "--horizons",
        default="60,120,180,300,600,900,1800,3600,7200,10800,14400",
    )
    parser.add_argument("--max-rows-per-horizon", type=int, default=12000)
    parser.add_argument("--minimum-train-support", type=int, default=80)
    parser.add_argument("--minimum-holdout-support", type=int, default=30)
    parser.add_argument(
        "--chronological-validation-blocks",
        type=int,
        choices=(1, 2),
        default=2,
        help="Use separate selection/final blocks (2) or the legacy single holdout (1).",
    )
    parser.add_argument("--max-rules-per-horizon", type=int, default=80)
    parser.add_argument("--max-conditions", type=int, default=3)
    parser.add_argument("--beam-width", type=int, default=24)
    parser.add_argument("--expansion-conditions", type=int, default=20)
    parser.add_argument("--maximum-base-features", type=int, default=24)
    parser.add_argument("--conditions-per-feature", type=int, default=2)
    parser.add_argument("--minimum-independent-time-buckets", type=int, default=3)
    parser.add_argument("--minimum-instruments", type=int, default=3)
    parser.add_argument("--minimum-positive-time-bucket-fraction", type=float, default=2.0 / 3.0)
    parser.add_argument("--minimum-positive-instrument-fraction", type=float, default=0.60)
    parser.add_argument("--maximum-instrument-weight-fraction", type=float, default=0.50)
    parser.add_argument(
        "--horizon-batch-size",
        type=int,
        default=0,
        help="Fit this many rotating horizons per pass; 0 fits every horizon.",
    )
    parser.add_argument("--cursor-state", type=Path, default=None)
    parser.add_argument(
        "--retain-previous-on-empty",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--interval-sec", type=float, default=300.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    args.horizons = sorted({int(value.strip()) for value in args.horizons.split(",") if value.strip()})
    if not args.horizons or min(args.horizons) <= 0:
        raise SystemExit("horizons must be positive")
    if min(
        args.max_rows_per_horizon,
        args.minimum_train_support,
        args.minimum_holdout_support,
        args.max_rules_per_horizon,
        args.max_conditions,
        args.beam_width,
        args.expansion_conditions,
        args.maximum_base_features,
        args.conditions_per_feature,
        args.minimum_independent_time_buckets,
        args.minimum_instruments,
    ) <= 0 or args.interval_sec <= 0.0:
        raise SystemExit("fit limits and interval must be positive")
    if args.max_conditions < 2 or args.max_conditions > 10:
        raise SystemExit("max conditions must be between 2 and 10")
    if args.horizon_batch_size < 0:
        raise SystemExit("horizon batch size cannot be negative")
    for name in (
        "minimum_positive_time_bucket_fraction",
        "minimum_positive_instrument_fraction",
        "maximum_instrument_weight_fraction",
    ):
        if not 0.0 < getattr(args, name) <= 1.0:
            raise SystemExit(f"{name.replace('_', ' ')} must be in (0, 1]")
    return args


def read_json(path: Path) -> dict:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def atomic_json(path: Path, payload: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def horizon_batch(args: argparse.Namespace) -> tuple[list[int], Path | None, int]:
    if args.horizon_batch_size <= 0 or args.horizon_batch_size >= len(args.horizons):
        return list(args.horizons), None, 0
    cursor_path = args.cursor_state or args.state.with_name(args.state.stem + "_cursor.json")
    cursor = int(read_json(cursor_path).get("next_index") or 0) % len(args.horizons)
    count = min(args.horizon_batch_size, len(args.horizons))
    selected = [args.horizons[(cursor + offset) % len(args.horizons)] for offset in range(count)]
    return selected, cursor_path, (cursor + count) % len(args.horizons)


def retained_shadow_rule(
    previous_rule: dict,
    *,
    chronological_validation_blocks: int,
) -> dict:
    """Keep a failed re-fit for audit without letting it steer execution."""

    retained = dict(previous_rule)
    previous_eligible = bool(retained.get("account_eligible", True))
    compatible = bool(
        chronological_validation_blocks == 1
        or (
            isinstance(retained.get("selection"), dict)
            and isinstance(retained.get("final_holdout"), dict)
        )
    )
    retained.update(
        {
            "account_eligible": False,
            "previous_account_eligible": previous_eligible,
            "retained_previous_fit": True,
            "retained_reason": "no_new_chronologically_replicated_rules",
            "validation_schema_compatible": compatible,
            "shadow_reason": "stale_no_new_chronological_replication",
        }
    )
    return retained


def _utc_epoch(value: object) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def rule_replication_signature(rule: dict) -> str:
    conditions = sorted(
        (
            str(condition.get("feature") or ""),
            str(condition.get("operator") or ""),
            str(condition.get("label") or ""),
        )
        for condition in rule.get("conditions") or []
    )
    return json.dumps(
        {
            "horizon_sec": int(rule.get("horizon_sec") or 0),
            "direction": str(rule.get("predicted_direction") or ""),
            "conditions": conditions,
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def apply_forward_refit_confirmation(
    fitted: list[dict],
    previous: list[dict],
    *,
    validated_utc: str,
) -> list[dict]:
    """Require a rule regime to recur for one forecast horizon before promotion."""

    validated_epoch = _utc_epoch(validated_utc)
    if validated_epoch is None:
        raise ValueError("validated_utc must be an ISO-8601 timestamp")
    previous_by_signature: dict[str, dict] = {}
    for rule in previous:
        if not isinstance(rule, dict) or not rule.get("first_replicated_utc"):
            continue
        signature = str(rule.get("replication_signature") or rule_replication_signature(rule))
        existing = previous_by_signature.get(signature)
        if existing is None or str(rule.get("first_replicated_utc")) < str(
            existing.get("first_replicated_utc")
        ):
            previous_by_signature[signature] = rule

    output: list[dict] = []
    for source in fitted:
        rule = dict(source)
        signature = rule_replication_signature(rule)
        prior = previous_by_signature.get(signature)
        first_utc = (
            str(prior.get("first_replicated_utc"))
            if prior is not None
            else validated_utc
        )
        first_epoch = _utc_epoch(first_utc)
        if first_epoch is None:
            first_utc = validated_utc
            first_epoch = validated_epoch
        horizon = max(1, int(rule.get("horizon_sec") or 0))
        required = max(300, horizon)
        age = max(0.0, validated_epoch - first_epoch)
        historical_eligible = bool(rule.get("account_eligible", True))
        confirmed = bool(historical_eligible and prior is not None and age >= required)
        rule.update(
            {
                "replication_signature": signature,
                "first_replicated_utc": first_utc,
                "last_replicated_utc": validated_utc,
                "forward_confirmation_age_sec": round(age, 3),
                "forward_confirmation_required_sec": required,
                "forward_refit_confirmed": confirmed,
                "historical_account_eligible": historical_eligible,
                "account_eligible": confirmed,
            }
        )
        if historical_eligible and not confirmed:
            rule["shadow_reason"] = "awaiting_forward_refit_confirmation"
        output.append(rule)
    return output


def carry_forward_unselected_rules(
    previous_rules: list[dict],
    selected_horizons: set[int],
    *,
    chronological_validation_blocks: int,
) -> list[dict]:
    """Carry untouched horizons without preserving incompatible eligibility."""

    output: list[dict] = []
    for source in previous_rules:
        if int(source.get("horizon_sec") or 0) in selected_horizons:
            continue
        compatible = bool(
            chronological_validation_blocks == 1
            or (
                isinstance(source.get("selection"), dict)
                and isinstance(source.get("final_holdout"), dict)
                and bool(source.get("replication_signature"))
                and bool(source.get("first_replicated_utc"))
            )
        )
        output.append(
            dict(source)
            if compatible
            else retained_shadow_rule(
                source,
                chronological_validation_blocks=chronological_validation_blocks,
            )
        )
    return output


def fit_once(args: argparse.Namespace) -> dict:
    selected_horizons, cursor_path, next_index = horizon_batch(args)
    selected_set = set(selected_horizons)
    previous = read_json(args.state)
    rules: list[dict] = (
        carry_forward_unselected_rules(
            list(previous.get("rules") or []),
            selected_set,
            chronological_validation_blocks=args.chronological_validation_blocks,
        )
        if cursor_path is not None
        else []
    )
    horizon_rows: dict[str, int] = {
        str(key): int(value)
        for key, value in (previous.get("horizon_fit_rows") or {}).items()
    }
    validated_utc = datetime.now(timezone.utc).isoformat()
    if args.database.is_file():
        for horizon in selected_horizons:
            rows = read_training_rows(args.database, horizon, args.max_rows_per_horizon)
            horizon_rows[str(horizon)] = len(rows)
            fitted = mine_fuzzy_rules(
                rows,
                horizon,
                minimum_train_support=args.minimum_train_support,
                minimum_holdout_support=args.minimum_holdout_support,
                max_rules=args.max_rules_per_horizon,
                max_conditions=args.max_conditions,
                beam_width=args.beam_width,
                expansion_conditions=args.expansion_conditions,
                maximum_base_features=args.maximum_base_features,
                conditions_per_feature=args.conditions_per_feature,
                minimum_independent_time_buckets=args.minimum_independent_time_buckets,
                minimum_instruments=args.minimum_instruments,
                minimum_positive_time_bucket_fraction=args.minimum_positive_time_bucket_fraction,
                minimum_positive_instrument_fraction=args.minimum_positive_instrument_fraction,
                maximum_instrument_weight_fraction=args.maximum_instrument_weight_fraction,
                chronological_validation_blocks=args.chronological_validation_blocks,
            )
            if fitted:
                previous_horizon_rules = [
                    rule
                    for rule in previous.get("rules") or []
                    if int(rule.get("horizon_sec") or 0) == horizon
                ]
                rules.extend(
                    apply_forward_refit_confirmation(
                        fitted,
                        previous_horizon_rules,
                        validated_utc=validated_utc,
                    )
                )
            elif args.retain_previous_on_empty:
                for previous_rule in previous.get("rules") or []:
                    if int(previous_rule.get("horizon_sec") or 0) != horizon:
                        continue
                    rules.append(
                        retained_shadow_rule(
                            previous_rule,
                            chronological_validation_blocks=(
                                args.chronological_validation_blocks
                            ),
                        )
                    )
    rules.sort(
        key=lambda rule: (
            float(rule.get("score") or 0.0),
            float((rule.get("holdout") or {}).get("weighted_support") or 0.0),
        ),
        reverse=True,
    )
    state = write_rule_state(
        args.state,
        args.database,
        rules,
        horizon_rows,
        {
            "max_rows_per_horizon": args.max_rows_per_horizon,
            "minimum_train_support": args.minimum_train_support,
            "minimum_holdout_support": args.minimum_holdout_support,
            "max_rules_per_horizon": args.max_rules_per_horizon,
            "max_conditions": args.max_conditions,
            "beam_width": args.beam_width,
            "expansion_conditions": args.expansion_conditions,
            "maximum_base_features": args.maximum_base_features,
            "conditions_per_feature": args.conditions_per_feature,
            "minimum_independent_time_buckets": args.minimum_independent_time_buckets,
            "minimum_instruments": args.minimum_instruments,
            "minimum_positive_time_bucket_fraction": args.minimum_positive_time_bucket_fraction,
            "minimum_positive_instrument_fraction": args.minimum_positive_instrument_fraction,
            "maximum_instrument_weight_fraction": args.maximum_instrument_weight_fraction,
            "chronological_validation_blocks": args.chronological_validation_blocks,
            "horizon_batch_size": args.horizon_batch_size,
            "last_fitted_horizons": selected_horizons,
            "retain_previous_on_empty": args.retain_previous_on_empty,
        },
    )
    if cursor_path is not None:
        atomic_json(
            cursor_path,
            {
                "next_index": next_index,
                "last_fitted_horizons": selected_horizons,
                "updated_at": state.get("generated_at"),
            },
        )
    return state


def main() -> int:
    args = parse_args()
    while True:
        try:
            state = fit_once(args)
            print(
                f"{state['generated_at']} rules={state['validated_rule_count']} "
                f"snapshots={state['counts']['snapshots']} outcomes={state['counts']['outcomes']}",
                flush=True,
            )
        except Exception as exc:
            print(f"signal combination fit error: {type(exc).__name__}: {exc}", flush=True)
        if args.once:
            return 0
        time.sleep(args.interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
