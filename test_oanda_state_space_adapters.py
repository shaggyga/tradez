from __future__ import annotations

import platform

import pytest

import oanda_state_space_adapters as adapters


def test_s4_requires_an_explicit_or_pinned_default_checkout(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("S4_OFFICIAL_PATH", raising=False)
    monkeypatch.delenv("FOREX_MODEL_RUNTIME_ROOT", raising=False)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
    with pytest.raises(adapters.StateSpaceRuntimeError, match="not configured"):
        adapters.S4OfficialAdapter()


def test_mamba_never_uses_a_windows_cpu_substitute() -> None:
    if platform.system() != "Linux":
        assert "Linux" in adapters.MambaOfficialAdapter.runtime_blocker()
        with pytest.raises(adapters.StateSpaceRuntimeError, match="Linux"):
            adapters.MambaOfficialAdapter().build_layer(8)
