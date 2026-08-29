from __future__ import annotations

import numpy as np
import pandas as pd

import oanda_live_signal_gate_audit as audit


def outcome_frame(events: int = 240, profitable: bool = True) -> pd.DataFrame:
    rows = []
    for index in range(events):
        for model, edge in (("weak", 0.1), ("strong", 1.0)):
            rows.append(
                {
                    "candidate_id": f"{index}-{model}",
                    "horizon_sec": 300,
                    "family": "test",
                    "model_id": model,
                    "instrument": "EUR_USD",
                    "input_timeframe": "M1",
                    "generated_epoch": 1_700_000_000 + index * 60,
                    "observed_epoch": 1_700_000_300 + index * 60,
                    "direction": "buy",
                    "probability_up": 0.56 if model == "strong" else 0.51,
                    "predicted_signed_pips": edge + 1.0,
                    "predicted_magnitude_pips": edge + 1.0,
                    "entry_spread_pips": 1.0,
                    "direction_correct": 1 if profitable else 0,
                    "executable_profitable": 1 if profitable else 0,
                    "executable_net_pips": 1.0 if profitable else -1.0,
                    "snapshot_id": f"snapshot-{index}",
                }
            )
    return pd.DataFrame(rows)


def test_consolidation_keeps_one_causal_choice_per_event() -> None:
    consolidated = audit.consolidate_forecasts(outcome_frame(10))

    assert len(consolidated) == 10
    assert consolidated["model_id"].eq("strong").all()
    assert np.allclose(consolidated["predicted_edge_pips"], 1.0)


def test_audit_uses_chronological_holdout_and_never_authorizes_account() -> None:
    report = audit.audit_frame(
        outcome_frame(),
        minimum_fit_samples=20,
        minimum_fit_blocks=3,
        block_sec=300,
    )

    assert report["status"] == "shadow_candidate_not_account_authorized"
    assert report["account_wired"] is False
    assert report["contract"]["selection_uses_fit_only"] is True
    assert report["rows"]["raw"] == 480
    assert report["rows"]["consolidated"] == 240
    assert report["selected_gate_holdout"]["average_net_pips"] == 1.0


def test_negative_outcomes_do_not_produce_candidate() -> None:
    report = audit.audit_frame(
        outcome_frame(profitable=False),
        minimum_fit_samples=20,
        minimum_fit_blocks=3,
        block_sec=300,
    )

    assert report["status"] == "no_stable_positive_gate"
    assert report["selected_gate_holdout"]["average_net_pips"] == -1.0
