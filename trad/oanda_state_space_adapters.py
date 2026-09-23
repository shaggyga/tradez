#!/usr/bin/env python3
"""Official-runtime adapters for S4 and Mamba shadow experiments."""

from __future__ import annotations

import importlib.util
import os
import platform
from pathlib import Path
from typing import Any


class StateSpaceRuntimeError(RuntimeError):
    pass


def default_s4_checkout() -> Path | None:
    runtime_root = os.environ.get("FOREX_MODEL_RUNTIME_ROOT", "").strip()
    if runtime_root:
        candidate = Path(runtime_root) / "official_sources" / "state-spaces-s4"
    else:
        local_app_data = os.environ.get("LOCALAPPDATA", "").strip()
        if not local_app_data:
            return None
        candidate = (
            Path(local_app_data)
            / "CodexRuntimes"
            / "official_sources"
            / "state-spaces-s4"
        )
    return candidate if candidate.is_dir() else None


def _official_s4_source(root: Path) -> Path:
    candidates = (
        root / "models" / "s4" / "s4.py",
        root / "src" / "models" / "sequence" / "ss" / "s4.py",
    )
    for path in candidates:
        if path.is_file():
            return path
    raise StateSpaceRuntimeError(
        f"no official S4 source module was found under pinned checkout {root}"
    )


class S4OfficialAdapter:
    """Load an S4 layer only from a pinned official state-spaces/s4 checkout."""

    model_id = "s4"

    def __init__(self, checkout: Path | None = None) -> None:
        if checkout is None:
            raw = os.environ.get("S4_OFFICIAL_PATH", "").strip()
            configured = Path(raw) if raw else default_s4_checkout()
            if configured is None:
                raise StateSpaceRuntimeError(
                    "S4_OFFICIAL_PATH is not configured and the pinned default checkout is absent"
                )
        else:
            configured = checkout
        self.checkout = configured.resolve()

    def load_module(self) -> Any:
        source = _official_s4_source(self.checkout)
        spec = importlib.util.spec_from_file_location("official_state_spaces_s4", source)
        if spec is None or spec.loader is None:
            raise StateSpaceRuntimeError(f"cannot load official S4 module from {source}")
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as exc:
            raise StateSpaceRuntimeError(
                f"official S4 checkout failed to import without modification: {exc}"
            ) from exc
        return module

    def build_layer(self, d_model: int, **kwargs: Any) -> Any:
        if d_model < 1:
            raise ValueError("d_model must be positive")
        module = self.load_module()
        layer_class = (
            getattr(module, "S4Block", None)
            or getattr(module, "S4", None)
            or getattr(module, "S4D", None)
        )
        if layer_class is None:
            raise StateSpaceRuntimeError(
                "official checkout exposes none of S4Block, S4, or S4D"
            )
        return layer_class(d_model=d_model, **kwargs)


class MambaOfficialAdapter:
    """Build the official Mamba block; no CPU or Windows substitute is allowed."""

    model_id = "mamba"

    @staticmethod
    def runtime_blocker() -> str:
        if platform.system() != "Linux":
            return "official Mamba requires Linux"
        try:
            import torch
        except ImportError:
            return "PyTorch is unavailable"
        if not torch.cuda.is_available():
            return "official Mamba requires an NVIDIA CUDA device"
        return ""

    def build_layer(
        self,
        d_model: int,
        d_state: int = 16,
        d_conv: int = 4,
        expand: int = 2,
    ) -> Any:
        block = self.runtime_blocker()
        if block:
            raise StateSpaceRuntimeError(block)
        try:
            from mamba_ssm import Mamba
        except ImportError as exc:
            raise StateSpaceRuntimeError("mamba-ssm is not installed") from exc
        return Mamba(d_model=d_model, d_state=d_state, d_conv=d_conv, expand=expand)
