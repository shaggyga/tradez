#!/usr/bin/env python3
"""Lazy, network-gated adapters for time-series foundation models."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np


class FoundationAdapterError(RuntimeError):
    """Raised when a foundation adapter cannot satisfy its strict runtime contract."""


@dataclass(frozen=True)
class ForecastRequest:
    context: np.ndarray
    horizon: int
    model_ref: str
    allow_download: bool = False
    interval_seconds: int = 60
    device: str = "cpu"

    def validated(self) -> "ForecastRequest":
        values = np.asarray(self.context, dtype=np.float32)
        if values.ndim not in (1, 2):
            raise ValueError("context must have shape (time,) or (variates, time)")
        if values.shape[-1] < 8:
            raise ValueError("context must contain at least eight observations")
        if not np.isfinite(values).all():
            raise ValueError("context contains non-finite values")
        if self.horizon < 1:
            raise ValueError("horizon must be positive")
        if self.interval_seconds < 1:
            raise ValueError("interval_seconds must be positive")
        if not self.allow_download and not Path(self.model_ref).exists():
            raise FoundationAdapterError(
                "model_ref must be a local path unless allow_download=True was explicitly set"
            )
        return ForecastRequest(
            context=values,
            horizon=self.horizon,
            model_ref=self.model_ref,
            allow_download=self.allow_download,
            interval_seconds=self.interval_seconds,
            device=self.device,
        )


@dataclass(frozen=True)
class ForecastResult:
    model_id: str
    median: np.ndarray
    lower: np.ndarray | None = None
    upper: np.ndarray | None = None
    samples: np.ndarray | None = None
    metadata: dict[str, Any] | None = None

    def validated(self, request: ForecastRequest) -> "ForecastResult":
        median = np.asarray(self.median, dtype=np.float64)
        if median.shape[-1] != request.horizon:
            raise FoundationAdapterError(
                f"forecast length {median.shape[-1]} does not match horizon {request.horizon}"
            )
        if not np.isfinite(median).all():
            raise FoundationAdapterError("forecast contains non-finite values")
        return self


def _import(module: str, distribution: str) -> Any:
    try:
        return importlib.import_module(module)
    except ImportError as exc:
        raise FoundationAdapterError(
            f"missing optional dependency {distribution}; install it in the research runtime"
        ) from exc


def _numpy(value: Any) -> np.ndarray:
    if hasattr(value, "detach"):
        value = value.detach()
    if hasattr(value, "cpu"):
        value = value.cpu()
    if hasattr(value, "numpy"):
        value = value.numpy()
    return np.asarray(value)


def _last_horizon(value: Any, horizon: int) -> np.ndarray:
    array = _numpy(value).squeeze()
    if array.ndim == 0 or array.shape[-1] < horizon:
        raise FoundationAdapterError("official runtime returned an unexpected forecast shape")
    return np.asarray(array[..., -horizon:], dtype=np.float64)


def _first_horizon(value: Any, horizon: int) -> np.ndarray:
    array = _numpy(value).squeeze()
    if array.ndim == 0 or array.shape[-1] < horizon:
        raise FoundationAdapterError("official runtime returned an unexpected forecast shape")
    return np.asarray(array[..., :horizon], dtype=np.float64)


def _chronos_context_tensor(context: np.ndarray, torch: Any) -> Any:
    tensor = torch.as_tensor(context, dtype=torch.float32)
    if tensor.ndim == 1:
        tensor = tensor.unsqueeze(0).unsqueeze(0)
    elif tensor.ndim == 2:
        tensor = tensor.unsqueeze(0)
    if tensor.ndim != 3:
        raise FoundationAdapterError(
            "Chronos-2 context must have shape (series, variates, history)"
        )
    return tensor


def _causal_left_pad(
    context: np.ndarray,
    *,
    minimum_length: int = 1,
    length_multiple: int = 1,
) -> tuple[np.ndarray, int]:
    """Meet fixed-context contracts without introducing future observations."""
    values = np.asarray(context, dtype=np.float32).reshape(-1)
    minimum = max(1, int(minimum_length))
    multiple = max(1, int(length_multiple))
    target = int(np.ceil(max(minimum, len(values)) / multiple)) * multiple
    if len(values) >= target:
        return values[-target:], 0
    padding = target - len(values)
    return np.pad(values, (padding, 0), mode="edge"), padding


class FoundationAdapter:
    model_id = ""

    def forecast(self, request: ForecastRequest) -> ForecastResult:
        raise NotImplementedError


class Chronos2Adapter(FoundationAdapter):
    model_id = "chronos_2"

    def __init__(self) -> None:
        self._pipeline: Any | None = None
        self._pipeline_key: tuple[str, str] | None = None

    def forecast(self, request: ForecastRequest) -> ForecastResult:
        req = request.validated()
        chronos = _import("chronos", "chronos-forecasting")
        torch = _import("torch", "torch")
        key = (req.model_ref, req.device)
        if self._pipeline is None or self._pipeline_key != key:
            self._pipeline = chronos.Chronos2Pipeline.from_pretrained(
                req.model_ref,
                device_map=req.device,
                local_files_only=not req.allow_download,
            )
            self._pipeline_key = key
        pipeline = self._pipeline
        context = _chronos_context_tensor(req.context, torch)
        quantiles, means = pipeline.predict_quantiles(
            context,
            prediction_length=req.horizon,
            quantile_levels=[0.1, 0.5, 0.9],
        )
        quantile_array = np.stack([_numpy(value) for value in quantiles])
        mean_array = np.stack([_numpy(value) for value in means])
        result = ForecastResult(
            self.model_id,
            _last_horizon(quantile_array[..., 1], req.horizon),
            _last_horizon(quantile_array[..., 0], req.horizon),
            _last_horizon(quantile_array[..., 2], req.horizon),
            metadata={
                "frozen": True,
                "network_allowed": req.allow_download,
                "mean_forecast": _last_horizon(mean_array, req.horizon).tolist(),
            },
        )
        return result.validated(req)


class TimesFM25Adapter(FoundationAdapter):
    model_id = "timesfm_2_5"

    def __init__(self) -> None:
        self._model: Any | None = None
        self._model_key: tuple[str, int, int] | None = None

    def forecast(self, request: ForecastRequest) -> ForecastResult:
        req = request.validated()
        timesfm = _import("timesfm", "timesfm[torch]")
        key = (req.model_ref, int(req.context.shape[-1]), req.horizon)
        if self._model is None or self._model_key != key:
            self._model = timesfm.TimesFM_2p5_200M_torch.from_pretrained(
                req.model_ref,
                local_files_only=not req.allow_download,
            )
            self._model.compile(
                timesfm.ForecastConfig(
                    max_context=max(32, int(req.context.shape[-1])),
                    max_horizon=req.horizon,
                    normalize_inputs=True,
                    use_continuous_quantile_head=True,
                    force_flip_invariance=True,
                    infer_is_positive=False,
                    fix_quantile_crossing=True,
                )
            )
            self._model_key = key
        model = self._model
        series = req.context if req.context.ndim == 2 else req.context[None, :]
        point, quantiles = model.forecast(horizon=req.horizon, inputs=list(series))
        point_array = _last_horizon(point, req.horizon)
        quantile_array = _numpy(quantiles)
        lower = quantile_array[..., 1] if quantile_array.shape[-1] >= 10 else None
        upper = quantile_array[..., -1] if quantile_array.shape[-1] >= 10 else None
        result = ForecastResult(
            self.model_id,
            point_array,
            lower,
            upper,
            metadata={"frozen": True, "network_allowed": req.allow_download},
        )
        return result.validated(req)


class TimesFMICFAdapter(FoundationAdapter):
    model_id = "timesfm_icf"

    def forecast(self, request: ForecastRequest) -> ForecastResult:
        request.validated()
        raise FoundationAdapterError(
            "TimesFM-ICF is a published research model, but no public runtime was found in "
            "the audited official TimesFM repository; substituting TimesFM 2.5 is prohibited"
        )


class _MoiraiBaseAdapter(FoundationAdapter):
    module_name = ""
    forecast_name = ""
    default_ref = ""
    patch_size: int | str = "auto"

    def __init__(self) -> None:
        self._weights: Any | None = None
        self._weights_ref = ""

    def forecast(self, request: ForecastRequest) -> ForecastResult:
        req = request.validated()
        if req.context.ndim != 1:
            raise ValueError("Moirai adapter currently requires a univariate context")
        pd = _import("pandas", "pandas")
        _import("uni2ts", "uni2ts")
        module = importlib.import_module(self.module_name)
        gluonts = _import("gluonts.dataset.pandas", "gluonts")
        index = pd.date_range(
            "2000-01-01",
            periods=len(req.context),
            freq=pd.Timedelta(seconds=req.interval_seconds),
        )
        dataset = gluonts.PandasDataset(dict(series=pd.Series(req.context, index=index)))
        forecast_class = getattr(module, self.forecast_name)
        weight_class = getattr(module, self.forecast_name.replace("Forecast", "Module"))
        if self._weights is None or self._weights_ref != req.model_ref:
            self._weights = weight_class.from_pretrained(
                req.model_ref,
                local_files_only=not req.allow_download,
            )
            self._weights_ref = req.model_ref
        model = forecast_class(
            module=self._weights,
            prediction_length=req.horizon,
            context_length=len(req.context),
            patch_size=self.patch_size,
            num_samples=100,
            target_dim=1,
            feat_dynamic_real_dim=dataset.num_feat_dynamic_real,
            past_feat_dynamic_real_dim=dataset.num_past_feat_dynamic_real,
        )
        forecast = next(iter(model.create_predictor(batch_size=1).predict(dataset)))
        median = _last_horizon(forecast.quantile(0.5), req.horizon)
        result = ForecastResult(
            self.model_id,
            median,
            _last_horizon(forecast.quantile(0.1), req.horizon),
            _last_horizon(forecast.quantile(0.9), req.horizon),
            metadata={"frozen": True, "network_allowed": req.allow_download},
        )
        return result.validated(req)


class MoiraiAdapter(_MoiraiBaseAdapter):
    model_id = "moirai"
    module_name = "uni2ts.model.moirai"
    forecast_name = "MoiraiForecast"


class MoiraiMoEAdapter(_MoiraiBaseAdapter):
    model_id = "moirai_moe"
    module_name = "uni2ts.model.moirai_moe"
    forecast_name = "MoiraiMoEForecast"
    patch_size = 16


class TinyTimeMixerAdapter(FoundationAdapter):
    model_id = "tiny_time_mixer"

    def __init__(self) -> None:
        self._model: Any | None = None
        self._model_key: tuple[str, str] | None = None

    def forecast(self, request: ForecastRequest) -> ForecastResult:
        req = request.validated()
        if req.context.ndim != 1:
            raise ValueError("TinyTimeMixer adapter currently requires a univariate context")
        torch = _import("torch", "torch")
        try:
            module = importlib.import_module("tsfm_public.models.tinytimemixer")
            model_class = getattr(module, "TinyTimeMixerForPrediction")
        except (ImportError, AttributeError) as exc:
            raise FoundationAdapterError(
                "missing IBM granite-tsfm TinyTimeMixer runtime"
            ) from exc
        key = (req.model_ref, req.device)
        if self._model is None or self._model_key != key:
            self._model = model_class.from_pretrained(
                req.model_ref,
                local_files_only=not req.allow_download,
            ).to(req.device)
            self._model_key = key
        model = self._model
        model.eval()
        context, padding = _causal_left_pad(
            req.context,
            minimum_length=int(getattr(model.config, "context_length", len(req.context))),
        )
        past_values = torch.as_tensor(
            context[None, :, None], dtype=torch.float32, device=req.device
        )
        with torch.no_grad():
            outputs = model(past_values=past_values)
        raw = getattr(outputs, "prediction_outputs", None)
        if raw is None:
            raise FoundationAdapterError("TinyTimeMixer returned no prediction_outputs")
        result = ForecastResult(
            self.model_id,
            _first_horizon(raw, req.horizon),
            metadata={
                "frozen": True,
                "network_allowed": req.allow_download,
                "causal_left_padding_steps": padding,
            },
        )
        return result.validated(req)


class Toto2Adapter(FoundationAdapter):
    model_id = "toto_2"

    def __init__(self) -> None:
        self._model: Any | None = None
        self._model_key: tuple[str, str] | None = None

    def forecast(self, request: ForecastRequest) -> ForecastResult:
        req = request.validated()
        toto2 = _import("toto2", "toto-2")
        torch = _import("torch", "torch")
        key = (req.model_ref, req.device)
        if self._model is None or self._model_key != key:
            self._model = toto2.Toto2Model.from_pretrained(
                req.model_ref,
                local_files_only=not req.allow_download,
            ).to(req.device).eval()
            self._model_key = key
        model = self._model
        context, padding = _causal_left_pad(
            req.context,
            length_multiple=int(getattr(model.config, "patch_size", 1)),
        )
        target = torch.as_tensor(context, dtype=torch.float32, device=req.device)
        if target.ndim == 1:
            target = target.unsqueeze(0)
        target = target.unsqueeze(0)
        inputs = {
            "target": target,
            "target_mask": torch.ones_like(target, dtype=torch.bool),
            "series_ids": torch.zeros(target.shape[:2], dtype=torch.long, device=req.device),
        }
        with torch.no_grad():
            quantiles = model.forecast(
                inputs,
                horizon=req.horizon,
                decode_block_size=None,
                has_missing_values=False,
            )
        array = _numpy(quantiles)
        result = ForecastResult(
            self.model_id,
            _last_horizon(array[4], req.horizon),
            _last_horizon(array[0], req.horizon),
            _last_horizon(array[-1], req.horizon),
            metadata={
                "frozen": True,
                "network_allowed": req.allow_download,
                "causal_left_padding_steps": padding,
            },
        )
        return result.validated(req)


class LagLlamaAdapter(FoundationAdapter):
    model_id = "lag_llama"

    def __init__(self, official_runner: Callable[[ForecastRequest], Any] | None = None) -> None:
        self.official_runner = official_runner
        self._predictor: Any | None = None
        self._predictor_key: tuple[str, int, int, str] | None = None

    @staticmethod
    def _checkpoint(model_ref: str) -> Path:
        path = Path(model_ref)
        if path.is_file():
            return path
        candidates = (path / "lag-llama.ckpt", path / "lag_llama.ckpt")
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        raise FoundationAdapterError(
            f"Lag-Llama checkpoint was not found under {path}"
        )

    def _forecast_official(self, req: ForecastRequest) -> ForecastResult:
        torch = _import("torch", "torch")
        pd = _import("pandas", "pandas")
        gluonts = _import("gluonts.dataset.common", "gluonts")
        estimator_module = _import(
            "lag_llama.gluon.estimator", "pinned official Lag-Llama checkout"
        )
        checkpoint = self._checkpoint(req.model_ref).resolve()
        context_length = int(req.context.shape[-1])
        key = (str(checkpoint), context_length, req.horizon, req.device)
        if self._predictor is None or self._predictor_key != key:
            state = torch.load(checkpoint, map_location=req.device, weights_only=False)
            model_kwargs = (state.get("hyper_parameters") or {}).get("model_kwargs") or {}
            trained_context = max(1, int(model_kwargs.get("context_length") or 32))
            rope_scaling = {
                "type": "linear",
                "factor": max(
                    1.0,
                    float(context_length + req.horizon) / float(trained_context),
                ),
            }
            estimator = estimator_module.LagLlamaEstimator(
                ckpt_path=str(checkpoint),
                prediction_length=req.horizon,
                context_length=context_length,
                input_size=int(model_kwargs.get("input_size") or 1),
                n_layer=int(model_kwargs.get("n_layer") or 8),
                n_embd_per_head=int(model_kwargs.get("n_embd_per_head") or 16),
                n_head=int(model_kwargs.get("n_head") or 9),
                scaling=model_kwargs.get("scaling") or "robust",
                time_feat=bool(model_kwargs.get("time_feat", False)),
                rope_scaling=rope_scaling,
                batch_size=1,
                num_parallel_samples=20,
                device=torch.device(req.device),
            )
            module = estimator.create_lightning_module()
            module.eval()
            self._predictor = estimator.create_predictor(
                estimator.create_transformation(), module
            )
            self._predictor_key = key
        frequency = f"{req.interval_seconds}s"
        target = np.asarray(req.context, dtype=np.float32).reshape(-1)
        dataset = gluonts.ListDataset(
            [{"start": pd.Period("2000-01-01", freq=frequency), "target": target}],
            freq=frequency,
        )
        forecast = next(iter(self._predictor.predict(dataset)))
        samples = np.asarray(forecast.samples, dtype=np.float64)
        result = ForecastResult(
            self.model_id,
            _last_horizon(forecast.quantile(0.5), req.horizon),
            _last_horizon(forecast.quantile(0.1), req.horizon),
            _last_horizon(forecast.quantile(0.9), req.horizon),
            samples=samples,
            metadata={
                "frozen": True,
                "network_allowed": req.allow_download,
                "official_checkpoint": str(checkpoint),
            },
        )
        return result.validated(req)

    def forecast(self, request: ForecastRequest) -> ForecastResult:
        req = request.validated()
        _import("lag_llama", "official Lag-Llama checkout")
        if self.official_runner is not None:
            raw = self.official_runner(req)
            result = ForecastResult(
                self.model_id,
                _last_horizon(raw, req.horizon),
                metadata={"frozen": True, "network_allowed": req.allow_download},
            )
            return result.validated(req)
        return self._forecast_official(req)


ADAPTERS: dict[str, type[FoundationAdapter]] = {
    adapter.model_id: adapter
    for adapter in (
        Chronos2Adapter,
        TimesFM25Adapter,
        TimesFMICFAdapter,
        MoiraiAdapter,
        MoiraiMoEAdapter,
        TinyTimeMixerAdapter,
        Toto2Adapter,
        LagLlamaAdapter,
    )
}


def available_adapters() -> Iterable[str]:
    return tuple(sorted(ADAPTERS))


def get_adapter(model_id: str) -> FoundationAdapter:
    try:
        return ADAPTERS[model_id]()
    except KeyError as exc:
        raise ValueError(f"unsupported foundation model: {model_id}") from exc
