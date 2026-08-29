#!/usr/bin/env python3
"""Evidence-gated router for independent FX specialists (shadow only)."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


REGIME_ARCHETYPES: dict[str, tuple[str, ...]] = {
    "news_event": ("news_event",),
    "liquidity": ("microstructure",),
    "cross_currency": ("currency_factor",),
    "technical": ("price_panel", "currency_factor"),
    "mixed": ("news_event", "microstructure", "currency_factor", "price_panel"),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def panel_evidence(paths: Iterable[Path]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for path in paths:
        payload = load_json(Path(path))
        for label, model in (payload.get("horizons") or {}).items():
            selection = model.get("selection") or {}
            holdout = model.get("untouched_holdout") or {}
            method = str(
                (model.get("selected_hyperparameters") or {}).get("target_method")
                or "direct_net"
            )
            archetype = (
                "currency_factor"
                if method.startswith("currency_factor")
                else "price_panel"
            )
            output.append(
                {
                    "specialist": f"panel_{label}_{method}",
                    "archetype": archetype,
                    "horizon_sec": int(finite(model.get("horizon_sec"))),
                    "selection_average_net": finite(selection.get("average_net_pips")),
                    "selection_ci_lower": finite(selection.get("block_ci90_lower_pips")),
                    "holdout_average_net": finite(holdout.get("average_net_pips")),
                    "holdout_ci_lower": finite(holdout.get("block_ci90_lower_pips")),
                    "independent_blocks": int(holdout.get("independent_time_blocks") or 0),
                    "observations": int(holdout.get("trades") or 0),
                    "source_decision": str(model.get("shadow_decision") or ""),
                    "source": str(Path(path).resolve()),
                }
            )
            for candidate_method, diagnostic in (
                model.get("method_selection_leaders") or {}
            ).items():
                if not str(candidate_method).startswith("currency_factor"):
                    continue
                diagnostic_selection = diagnostic.get("selection") or {}
                diagnostic_holdout = diagnostic.get("untouched_holdout_diagnostic") or {}
                output.append(
                    {
                        "specialist": f"panel_{label}_{candidate_method}",
                        "archetype": "currency_factor",
                        "horizon_sec": int(finite(model.get("horizon_sec"))),
                        "selection_average_net": finite(
                            diagnostic_selection.get("average_net_pips")
                        ),
                        "selection_ci_lower": finite(
                            diagnostic_selection.get("block_ci90_lower_pips")
                        ),
                        "holdout_average_net": finite(
                            diagnostic_holdout.get("average_net_pips")
                        ),
                        "holdout_ci_lower": finite(
                            diagnostic_holdout.get("block_ci90_lower_pips")
                        ),
                        "independent_blocks": int(
                            diagnostic_holdout.get("independent_time_blocks") or 0
                        ),
                        "observations": int(diagnostic_holdout.get("trades") or 0),
                        "source_decision": "diagnostic_only",
                        "source": str(Path(path).resolve()),
                    }
                )
    return output


def news_evidence(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path)
    output: list[dict[str, Any]] = []
    for label, model in (payload.get("horizons") or {}).items():
        selection = (
            (model.get("selected_policy") or {}).get("selection")
            or {}
        )
        holdout = model.get("untouched_holdout") or {}
        output.append(
            {
                "specialist": f"news_event_{label}m",
                "archetype": "news_event",
                "horizon_sec": int(label) * 60,
                "selection_average_net": finite(selection.get("average_net_pips")),
                "selection_ci_lower": finite(selection.get("ci90_lower_pips")),
                "holdout_average_net": finite(holdout.get("average_net_pips")),
                "holdout_ci_lower": finite(holdout.get("ci90_lower_pips")),
                "independent_blocks": int(holdout.get("episodes") or 0),
                "observations": int(model.get("episodes") or 0),
                "source_decision": str(model.get("shadow_decision") or ""),
                "source": str(Path(path).resolve()),
            }
        )
    return output


def microstructure_evidence(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path)
    calibration = payload.get("calibration") or {}
    holdout = payload.get("holdout") or {}
    selected = holdout.get("selected") or {}
    selection_ok = calibration.get("selection_status") == "selected_on_calibration"
    lower = finite(selected.get("bootstrap_mean_net_pips_lower_95"))
    return [
        {
            "specialist": "two_stage_microstructure",
            "archetype": "microstructure",
            "horizon_sec": 0,
            "selection_average_net": 1.0 if selection_ok else 0.0,
            "selection_ci_lower": 1.0 if selection_ok else 0.0,
            "holdout_average_net": finite(selected.get("mean_net_pips")),
            "holdout_ci_lower": lower,
            "independent_blocks": 0,
            "observations": int(selected.get("trades") or 0),
            "source_decision": (
                "forward_observe_only"
                if bool(holdout.get("production_gate_passed"))
                else "no_trade_failed_microstructure_gate"
            ),
            "source": str(Path(path).resolve()),
        }
    ]


def evidence_checks(row: Mapping[str, Any], minimum_blocks: int = 20) -> dict[str, bool]:
    return {
        "source_forward_observe": str(row.get("source_decision")) == "forward_observe_only",
        "positive_selection_average": finite(row.get("selection_average_net")) > 0.0,
        "positive_selection_lower_bound": finite(row.get("selection_ci_lower")) > 0.0,
        "positive_holdout_average": finite(row.get("holdout_average_net")) > 0.0,
        "positive_holdout_lower_bound": finite(row.get("holdout_ci_lower")) > 0.0,
        "minimum_independent_blocks": int(row.get("independent_blocks") or 0) >= minimum_blocks,
    }


def route_specialists(
    regime: str,
    candidates: Iterable[Mapping[str, Any]],
    minimum_blocks: int = 20,
) -> dict[str, Any]:
    regime = str(regime or "mixed")
    allowed = set(REGIME_ARCHETYPES.get(regime, REGIME_ARCHETYPES["mixed"]))
    audited: list[dict[str, Any]] = []
    for raw in candidates:
        row = dict(raw)
        checks = evidence_checks(row, minimum_blocks=minimum_blocks)
        row["checks"] = checks
        row["evidence_passed"] = all(checks.values())
        row["regime_allowed"] = str(row.get("archetype")) in allowed
        audited.append(row)
    eligible = [
        row for row in audited if row["evidence_passed"] and row["regime_allowed"]
    ]
    # One specialist per independent archetype prevents same-domain model
    # variants from recreating the inflated family-count consensus problem.
    selected_by_archetype: dict[str, dict[str, Any]] = {}
    for row in sorted(
        eligible,
        key=lambda item: (
            finite(item.get("holdout_ci_lower")),
            finite(item.get("holdout_average_net")),
            int(item.get("independent_blocks") or 0),
        ),
        reverse=True,
    ):
        selected_by_archetype.setdefault(str(row.get("archetype")), row)
    selected = list(selected_by_archetype.values())
    return {
        "regime": regime,
        "allowed_archetypes": sorted(allowed),
        "decision": "observe_specialists" if selected else "no_trade_no_validated_specialist",
        "selected": selected,
        "selected_independent_archetypes": sorted(selected_by_archetype),
        "audited": audited,
        "account_eligible": False,
        "execution_adapter": False,
    }


def run_audit(
    panel_paths: Iterable[Path],
    news_path: Path,
    microstructure_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    candidates = panel_evidence(panel_paths)
    candidates.extend(news_evidence(news_path))
    candidates.extend(microstructure_evidence(microstructure_path))
    routes = {
        regime: route_specialists(regime, candidates)
        for regime in REGIME_ARCHETYPES
    }
    payload = {
        "schema_version": 1,
        "generated_at": utc_now(),
        "router": "independent_specialist_evidence_router_v1",
        "research_only": True,
        "shadow_only": True,
        "account_eligible": False,
        "execution_adapter": False,
        "candidate_count": len(candidates),
        "routes": routes,
    }
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(output_path)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel", type=Path, action="append", required=True)
    parser.add_argument("--news", type=Path, required=True)
    parser.add_argument("--microstructure", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run_audit(args.panel, args.news, args.microstructure, args.output)
    print(json.dumps({
        "output": str(args.output.resolve()),
        "candidate_count": result["candidate_count"],
        "mixed_decision": result["routes"]["mixed"]["decision"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "evidence_checks",
    "microstructure_evidence",
    "news_evidence",
    "panel_evidence",
    "route_specialists",
    "run_audit",
]
