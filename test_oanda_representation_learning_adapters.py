from __future__ import annotations

import numpy as np

import oanda_representation_learning_adapters as representation


def test_chronological_window_split_and_train_only_normalization() -> None:
    values = np.column_stack(
        [np.arange(96, dtype=np.float32), np.sin(np.arange(96) / 5)]
    )
    split = representation.chronological_windows(values)
    assert split.train_target_max_index < split.holdout_target_min_index
    assert split.train_x.shape[1:] == split.holdout_x.shape[1:]
    assert np.allclose(split.train_x.mean(axis=(0, 1)), 0.0, atol=1e-5)


def test_all_representation_modes_execute_a_real_training_step() -> None:
    report = representation.synthetic_qualification()
    assert report["split"]["chronological"]
    assert {row["model"] for row in report["results"]} == {
        "self_supervised_pretraining",
        "contrastive_learning",
        "multi_task_learning",
        "transfer_learning",
        "knowledge_distillation",
    }
    assert all(row["status"] == "synthetic_qualified" for row in report["results"])
    assert all(not row["account_wired"] for row in report["results"])
