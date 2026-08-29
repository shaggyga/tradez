#!/usr/bin/env python3
"""Canonical registry and evidence contract for modern FX model challengers."""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import json
import os
import platform
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


EVIDENCE_LEVELS = (
    "catalogued",
    "adapter_implemented",
    "runtime_available",
    "synthetic_qualified",
    "bounded_market_qualified",
    "production_eligible",
)


@dataclass(frozen=True)
class ModelSpec:
    model_id: str
    family: str
    provider: str
    dependency_module: str
    distribution: str
    adapter_module: str
    official_url: str
    runtime_policy: str
    weight_policy: str = "none"
    host_constraint: str = ""
    path_environment: str = ""
    notes: str = ""


def _spec(
    model_id: str,
    family: str,
    provider: str,
    dependency_module: str,
    distribution: str,
    adapter_module: str,
    official_url: str,
    runtime_policy: str,
    **kwargs: str,
) -> ModelSpec:
    return ModelSpec(
        model_id=model_id,
        family=family,
        provider=provider,
        dependency_module=dependency_module,
        distribution=distribution,
        adapter_module=adapter_module,
        official_url=official_url,
        runtime_policy=runtime_policy,
        **kwargs,
    )


MODEL_SPECS = (
    _spec(
        "catboost",
        "modern_tabular_probabilistic",
        "CatBoost",
        "catboost",
        "catboost",
        "oanda_shared_panel_model_benchmark",
        "https://catboost.ai/en/docs/",
        "supervised_shadow_only",
    ),
    _spec(
        "ngboost",
        "modern_tabular_probabilistic",
        "Stanford ML Group",
        "ngboost",
        "ngboost",
        "oanda_shared_panel_model_benchmark",
        "https://stanfordmlgroup.github.io/projects/ngboost/",
        "supervised_shadow_only",
    ),
    *(
        _spec(
            model_id,
            "major_neural_forecasters",
            provider,
            module,
            distribution,
            adapter,
            url,
            "purged_sequence_shadow_only",
        )
        for model_id, provider, module, distribution, adapter, url in (
            (
                "tft",
                "PyTorch Forecasting",
                "pytorch_forecasting",
                "pytorch-forecasting",
                "oanda_shared_panel_neural_benchmark",
                "https://pytorch-forecasting.readthedocs.io/",
            ),
            (
                "deepar",
                "PyTorch Forecasting",
                "pytorch_forecasting",
                "pytorch-forecasting",
                "oanda_shared_panel_neural_benchmark",
                "https://pytorch-forecasting.readthedocs.io/",
            ),
            *(
                (
                    name,
                    "Nixtla NeuralForecast",
                    "neuralforecast",
                    "neuralforecast",
                    "oanda_neuralforecast_family_adapters",
                    "https://nixtlaverse.nixtla.io/neuralforecast/models.html",
                )
                for name in ("patchtst", "nbeats", "nhits", "lstm", "gru", "tcn")
            ),
        )
    ),
    *(
        _spec(
            model_id,
            "time_series_foundation_models",
            provider,
            module,
            distribution,
            "oanda_foundation_model_adapters",
            url,
            "frozen_zero_shot_shadow_only",
            weight_policy="explicit_local_or_opt_in_download",
            notes=notes,
        )
        for model_id, provider, module, distribution, url, notes in (
            (
                "chronos_2",
                "Amazon Science",
                "chronos",
                "chronos-forecasting",
                "https://github.com/amazon-science/chronos-forecasting",
                "Chronos-2 univariate, multivariate, and covariate inference.",
            ),
            (
                "timesfm_2_5",
                "Google Research",
                "timesfm",
                "timesfm",
                "https://github.com/google-research/timesfm",
                "TimesFM 2.5 public open-weight runtime.",
            ),
            (
                "timesfm_icf",
                "Google Research",
                "",
                "",
                "https://www.research.google/blog/time-series-foundation-models-can-be-few-shot-learners/",
                "Published research path; no public runtime was present in the official repository audit.",
            ),
            (
                "moirai",
                "Salesforce AI Research",
                "uni2ts",
                "uni2ts",
                "https://github.com/SalesforceAIResearch/uni2ts",
                "Moirai frozen zero-shot inference through GluonTS.",
            ),
            (
                "moirai_moe",
                "Salesforce AI Research",
                "uni2ts",
                "uni2ts",
                "https://github.com/SalesforceAIResearch/uni2ts",
                "Moirai-MoE frozen zero-shot inference through GluonTS.",
            ),
            (
                "tiny_time_mixer",
                "IBM Granite TSFM",
                "tsfm_public",
                "granite-tsfm",
                "https://github.com/ibm-granite/granite-tsfm",
                "TinyTimeMixer inference through IBM Granite TSFM's Transformers-compatible class.",
            ),
            (
                "toto_2",
                "Datadog",
                "toto2",
                "toto-2",
                "https://github.com/DataDog/toto",
                "Toto 2.0 quantile forecast adapter.",
            ),
            (
                "lag_llama",
                "Lag-Llama authors",
                "lag_llama",
                "",
                "https://github.com/time-series-foundation-models/lag-llama",
                "Official checkout and checkpoint are required.",
            ),
        )
    ),
    _spec(
        "temporal_cross_pair_gnn",
        "cross_pair_graph_forecasting",
        "Nixtla NeuralForecast",
        "neuralforecast",
        "neuralforecast",
        "oanda_cross_pair_graph_adapter",
        "https://nixtlaverse.nixtla.io/neuralforecast/",
        "lagged_edges_shadow_only",
        notes="StemGNN runtime with a separately audited lag-only FX graph builder.",
    ),
    _spec(
        "s4",
        "neural_state_space",
        "state-spaces",
        "",
        "",
        "oanda_state_space_adapters",
        "https://github.com/state-spaces/s4",
        "official_checkout_shadow_only",
        path_environment="S4_OFFICIAL_PATH",
        notes="Official project has no pip package; adapter requires a pinned checkout.",
    ),
    _spec(
        "mamba",
        "neural_state_space",
        "state-spaces",
        "mamba_ssm",
        "mamba-ssm",
        "oanda_state_space_adapters",
        "https://github.com/state-spaces/mamba",
        "official_runtime_shadow_only",
        host_constraint="linux_nvidia_cuda",
        notes="Official runtime requires Linux, NVIDIA GPU, and CUDA.",
    ),
    _spec(
        "contextual_bandit",
        "decision_learning",
        "Vowpal Wabbit",
        "vowpalwabbit",
        "vowpalwabbit",
        "oanda_decision_model_adapters",
        "https://vowpalwabbit.org/docs/vowpal_wabbit/python/latest/tutorials/python_Contextual_bandits_and_Vowpal_Wabbit.html",
        "offline_fixed_actions_shadow_only",
    ),
    *(
        _spec(
            name,
            "decision_learning",
            "Stable-Baselines3",
            "stable_baselines3",
            "stable-baselines3",
            "oanda_decision_model_adapters",
            "https://stable-baselines3.readthedocs.io/",
            "offline_fixed_actions_shadow_only",
        )
        for name in ("ppo", "sac", "dqn")
    ),
    *(
        _spec(
            name,
            "representation_and_transfer_learning",
            provider,
            "torch",
            "torch",
            "oanda_representation_learning_adapters",
            url,
            "frozen_split_shadow_only",
            notes=notes,
        )
        for name, provider, url, notes in (
            (
                "self_supervised_pretraining",
                "PyTorch adapter",
                "https://pytorch.org/docs/stable/index.html",
                "Masked reconstruction pretraining with train-only normalization.",
            ),
            (
                "contrastive_learning",
                "TS2Vec-compatible adapter",
                "https://github.com/zhihanyue/ts2vec",
                "Hierarchical temporal contrastive objective; official TS2Vec checkout may be used.",
            ),
            (
                "multi_task_learning",
                "PyTorch adapter",
                "https://pytorch.org/docs/stable/index.html",
                "Shared encoder with direction, return, and movement heads.",
            ),
            (
                "transfer_learning",
                "PyTorch adapter",
                "https://pytorch.org/docs/stable/index.html",
                "Frozen-then-unfrozen encoder transfer with source/target split provenance.",
            ),
            (
                "knowledge_distillation",
                "PyTorch adapter",
                "https://pytorch.org/docs/stable/index.html",
                "Frozen teacher logits and held-out student evaluation.",
            ),
        )
    ),
)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def distribution_version(name: str) -> str:
    if not name:
        return ""
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return ""


def _module_available(name: str) -> bool:
    return bool(name and importlib.util.find_spec(name) is not None)


def _default_source_checkout(model_id: str) -> Path | None:
    names = {"s4": "state-spaces-s4", "lag_llama": "lag-llama"}
    name = names.get(model_id)
    if not name:
        return None
    runtime_root = os.environ.get("FOREX_MODEL_RUNTIME_ROOT", "").strip()
    if runtime_root:
        candidate = Path(runtime_root) / "official_sources" / name
    else:
        local_app_data = os.environ.get("LOCALAPPDATA", "").strip()
        if not local_app_data:
            return None
        candidate = Path(local_app_data) / "CodexRuntimes" / "official_sources" / name
    return candidate if candidate.is_dir() else None


def _host_block(spec: ModelSpec) -> str:
    if spec.host_constraint == "linux_nvidia_cuda":
        if platform.system() != "Linux":
            return "official runtime requires Linux"
        try:
            import torch

            if not torch.cuda.is_available():
                return "official runtime requires an NVIDIA CUDA device"
        except ImportError:
            return "PyTorch is unavailable"
    if spec.path_environment:
        configured = os.environ.get(spec.path_environment, "")
        default_checkout = _default_source_checkout(spec.model_id)
        if not configured and default_checkout is None:
            return f"{spec.path_environment} is not configured"
        if configured and not Path(configured).is_dir():
            return f"{spec.path_environment} does not point to a directory"
    if spec.model_id == "timesfm_icf":
        return "no public TimesFM-ICF runtime was found in the official source audit"
    return ""


def probe_spec(spec: ModelSpec) -> dict[str, object]:
    block = _host_block(spec)
    source_checkout = _default_source_checkout(spec.model_id)
    dependency_available = _module_available(spec.dependency_module) or (
        spec.model_id == "s4" and source_checkout is not None
    )
    if block:
        status = "blocked"
        evidence = "adapter_implemented"
        reason = block
    elif not dependency_available:
        status = "dependency_missing"
        evidence = "adapter_implemented"
        reason = f"missing module {spec.dependency_module or '(official checkout)'}"
    else:
        status = "runtime_available"
        evidence = "runtime_available"
        reason = ""
    return {
        **asdict(spec),
        "adapter_implemented": True,
        "account_wired": False,
        "dependency_available": dependency_available,
        "dependency_version": distribution_version(spec.distribution),
        "official_source_checkout": str(source_checkout or ""),
        "runtime_status": status,
        "runtime_reason": reason,
        "evidence_level": evidence,
    }


def validate_evidence_level(level: str) -> None:
    if level not in EVIDENCE_LEVELS:
        raise ValueError(f"unsupported evidence level: {level}")


def evidence_at_least(actual: str, required: str) -> bool:
    validate_evidence_level(actual)
    validate_evidence_level(required)
    return EVIDENCE_LEVELS.index(actual) >= EVIDENCE_LEVELS.index(required)


def build_probe_report(specs: Iterable[ModelSpec] = MODEL_SPECS) -> dict[str, object]:
    models = [probe_spec(spec) for spec in specs]
    return {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "scope": "OANDA Forex research and shadow forecasting only",
        "execution_policy": "no account wiring; no weight download without explicit opt-in",
        "python": sys.version,
        "platform": platform.platform(),
        "evidence_levels": list(EVIDENCE_LEVELS),
        "models": models,
        "summary": {
            "models": len(models),
            "families": len({str(row["family"]) for row in models}),
            "runtime_available": sum(row["runtime_status"] == "runtime_available" for row in models),
            "dependency_missing": sum(row["runtime_status"] == "dependency_missing" for row in models),
            "blocked": sum(row["runtime_status"] == "blocked" for row in models),
            "account_wired": 0,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_probe_report()
    payload = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
