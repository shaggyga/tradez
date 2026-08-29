from __future__ import annotations

import numpy as np
import pytest

import oanda_foundation_model_adapters as adapters


class _FakeTensor:
    def __init__(self, ndim: int) -> None:
        self.ndim = ndim

    def unsqueeze(self, axis: int):
        return _FakeTensor(self.ndim + 1)


class _FakeTorch:
    float32 = "float32"

    @staticmethod
    def as_tensor(value, dtype=None):
        return _FakeTensor(np.asarray(value).ndim)


def test_all_foundation_adapters_are_registered() -> None:
    assert set(adapters.available_adapters()) == {
        "chronos_2",
        "timesfm_2_5",
        "timesfm_icf",
        "moirai",
        "moirai_moe",
        "tiny_time_mixer",
        "toto_2",
        "lag_llama",
    }


def test_chronos_context_adds_series_and_variates_axes() -> None:
    one_dimensional = adapters._chronos_context_tensor(np.arange(12), _FakeTorch)
    multivariate = adapters._chronos_context_tensor(np.ones((3, 12)), _FakeTorch)

    assert one_dimensional.ndim == 3
    assert multivariate.ndim == 3


def test_request_refuses_implicit_weight_download() -> None:
    request = adapters.ForecastRequest(
        context=np.arange(16, dtype=float),
        horizon=2,
        model_ref="provider/remote-model",
    )
    with pytest.raises(adapters.FoundationAdapterError, match="allow_download"):
        request.validated()


def test_timesfm_icf_never_silently_substitutes_public_timesfm(tmp_path) -> None:
    request = adapters.ForecastRequest(
        context=np.arange(16, dtype=float),
        horizon=2,
        model_ref=str(tmp_path),
    )
    with pytest.raises(adapters.FoundationAdapterError, match="substituting"):
        adapters.TimesFMICFAdapter().forecast(request)


def test_fixed_length_foundation_output_uses_nearest_future_steps() -> None:
    values = np.arange(5, dtype=float)

    result = adapters._first_horizon(values, 2)

    np.testing.assert_array_equal(result, np.array([0.0, 1.0]))


def test_causal_left_pad_meets_minimum_and_patch_multiple() -> None:
    values, padding = adapters._causal_left_pad(
        np.array([1.0, 2.0, 3.0]), minimum_length=5, length_multiple=4
    )

    np.testing.assert_array_equal(
        values, np.array([1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 3.0])
    )
    assert len(values) == 8
    assert padding == 5
