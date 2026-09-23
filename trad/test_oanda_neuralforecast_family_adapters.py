from __future__ import annotations

import numpy as np

import oanda_neuralforecast_family_adapters as families


def test_trainer_accelerator_allows_explicit_cpu(monkeypatch) -> None:
    monkeypatch.setenv("OANDA_NEURAL_ACCELERATOR", "cpu")
    assert families.trainer_accelerator() == "cpu"


def test_synthetic_series_is_balanced_and_deterministic() -> None:
    first = families.synthetic_series(4, 32)
    second = families.synthetic_series(4, 32)
    assert first.groupby("unique_id").size().eq(32).all()
    assert np.allclose(first["y"], second["y"])


def test_every_neuralforecast_model_builds_real_runtime() -> None:
    for name in families.ALL_MODEL_NAMES:
        model = families.make_model(name, horizon=2, input_size=12, max_steps=1, n_series=4)
        assert model.alias == name


def test_sequence_index_axis_preserves_each_series_order() -> None:
    frame = families.synthetic_series(2, 24)
    frame.loc[frame["unique_id"] == "S1", "ds"] += np.timedelta64(2, "h")
    indexed = families.sequence_index_axis(frame)
    assert indexed.groupby("unique_id")["ds"].apply(list).eq([list(range(24))] * 2).all()
    assert indexed.groupby("unique_id")["source_ds"].apply(lambda x: x.is_monotonic_increasing).all()
