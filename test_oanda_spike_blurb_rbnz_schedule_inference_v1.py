from __future__ import annotations

from pathlib import Path

import pytest

import oanda_spike_blurb_rbnz_schedule_inference_v1 as module


def test_exact_randomization_treats_each_member_of_trio_as_exchangeable() -> None:
    assert module.exact_randomization_pvalue([(1.0, 0.0, 0.0)]) == pytest.approx(1 / 3)
    assert module.exact_randomization_pvalue([(1.0, 0.0, 0.0)] * 2) == pytest.approx(1 / 9)
    assert module.exact_randomization_pvalue([(0.0, 0.0, 0.0)] * 3) == 1.0


def test_holm_adjustment_is_monotone_and_familywise() -> None:
    rows = [{"randomization_pvalue": value} for value in (0.001, 0.01, 0.04, 0.5)]
    module.holm_adjust(rows)
    adjusted = [row["holm_adjusted_pvalue"] for row in rows]
    assert adjusted == pytest.approx([0.004, 0.03, 0.08, 0.5])


def test_live_frozen_inputs_produce_all_cells_with_no_trade_missing_policy(tmp_path: Path) -> None:
    report = module.run(report_root=tmp_path)
    assert report["cell_count"] == 16
    assert report["missing_detection_policy"] == "explicit_no_trade_zero_return"
    assert report["multiplicity_method"] == "holm_familywise_16_cells"
    assert report["execution_eligible"] is False
    assert report["forecast_proof_eligible"] is False
    assert report["supported_execution_decision"] == "no_trade"
    assert all(row["independent_scheduled_event_count"] == 12 for row in report["cells"])
    assert all(row["matched_control_count"] == 24 for row in report["cells"])


def test_inference_module_is_read_only_and_nonoperational() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "mode=ro" in source
    assert "/orders" not in source
    assert "/trades" not in source
    assert "api-fxtrade.oanda.com" not in source
    assert "supported_execution_decision\": \"no_trade" in source
