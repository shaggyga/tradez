from __future__ import annotations

import json
from pathlib import Path

try:
    from oanda_ma_feature_grid_intensive_audit import main as audit_main
except ModuleNotFoundError:
    from trad.oanda_ma_feature_grid_intensive_audit import main as audit_main


def passing_metrics() -> dict[str, float | int]:
    return {
        "n": 1000,
        "direction_accuracy": 0.55,
        "direction_accuracy_lower_95": 0.53,
        "macro_direction_accuracy": 0.55,
        "macro_direction_accuracy_lower_95": 0.52,
        "balanced_accuracy": 0.54,
        "macro_balanced_accuracy": 0.53,
        "macro_balanced_accuracy_lower_95": 0.51,
        "predicted_up_fraction": 0.52,
        "executable_average_net_pips": 0.4,
        "pair_average_net_lower_95_pips": 0.1,
        "executable_average_net_cost_units": 0.3,
        "pair_average_net_cost_units_lower_95": 0.08,
        "positive_pair_fraction": 0.65,
        "within_pair_signed_pip_correlation": 0.08,
        "magnitude_pip_correlation": 0.30,
        "within_pair_magnitude_pip_correlation": 0.12,
        "movement_gate_entry_n": 100,
        "movement_gate_pair_count": 10,
        "movement_gate_average_net_pips": 0.8,
        "movement_gate_pair_average_net_lower_95": 0.2,
        "movement_gate_average_net_cost_units": 0.5,
        "movement_gate_pair_average_net_cost_units_lower_95": 0.1,
        "movement_gate_direction_accuracy_lower_95": 0.53,
        "movement_gate_macro_direction_accuracy_lower_95": 0.53,
        "movement_gate_positive_pair_fraction": 0.70,
        "movement_gate_profit_factor": 1.30,
    }


def test_intensive_audit_counts_only_strict_purged_replication(
    tmp_path: Path,
) -> None:
    report_root = tmp_path / "reports"
    run_dir = report_root / "ma_feature_grid_test_purged"
    run_dir.mkdir(parents=True)
    report = {
        "generated_at": "2026-07-27T00:00:00+00:00",
        "instrument_count": 10,
        "fit": {
            "estimator_type": "ridge",
            "target_space": "local_scale",
            "pair_context": "none",
            "split_policy": "global_time_purged",
            "samples_per_pair_timeframe": 1000,
        },
        "timeframe_reports": [
            {
                "timeframe": "M5",
                "status": "fitted",
                "replicated_movement_gate_horizons": [300],
                "metrics": {
                    "validation": {"300": passing_metrics()},
                    "holdout": {"300": passing_metrics()},
                },
            }
        ],
    }
    (run_dir / "ma_feature_grid_latest.json").write_text(
        json.dumps(report),
        encoding="utf-8",
    )
    output_dir = report_root / "ma_feature_grid_intensive_audit"

    assert (
        audit_main(
            [
                "--report-root",
                str(report_root),
                "--output-dir",
                str(output_dir),
            ]
        )
        == 0
    )

    payload = json.loads(
        (
            output_dir / "ma_feature_grid_intensive_audit_latest.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["strict_direction_cost_replications"] == 1
    assert payload["strict_movement_gate_replications"] == 1
    assert payload["execution_authorized"] is False
    assert payload["top_diagnostic_cells"][0]["timeframe"] == "M5"
    assert payload["top_movement_gate_cells"][0]["horizon_sec"] == 300
