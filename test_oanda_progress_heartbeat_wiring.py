from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import oanda_causal_level_band_prospective as causal
import oanda_direct_source_response as direct


class FakeHeartbeat:
    instances: list["FakeHeartbeat"] = []

    def __init__(self, path: Path, **metadata: Any) -> None:
        self.path = Path(path)
        self.metadata = metadata
        self.phases: list[str] = []
        self.closed = False
        self.__class__.instances.append(self)

    def __enter__(self) -> "FakeHeartbeat":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.closed = True

    def mark_progress(self, *, phase: str, **_details: Any) -> None:
        self.phases.append(phase)

    def update(self, *, phase: str, **_details: Any) -> None:
        self.phases.append(phase)


def test_causal_main_wires_distinct_independent_progress_heartbeat(
    tmp_path: Path,
    monkeypatch,
) -> None:
    received: list[FakeHeartbeat] = []

    class FakeWorker:
        def __init__(self, **_paths: Any) -> None:
            pass

        def run_once(self, *, heartbeat: FakeHeartbeat) -> dict[str, Any]:
            received.append(heartbeat)
            heartbeat.mark_progress(phase="fake_work")
            return {"runtime_seconds": 0.01}

        def close(self) -> None:
            pass

    FakeHeartbeat.instances.clear()
    heartbeat_path = tmp_path / "causal-progress.json"
    monkeypatch.setattr(causal, "WorkerHeartbeat", FakeHeartbeat)
    monkeypatch.setattr(causal, "ProspectiveLevelBandWorker", FakeWorker)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "oanda_causal_level_band_prospective.py",
            "--once",
            "--progress-heartbeat",
            str(heartbeat_path),
        ],
    )

    assert causal.main() == 0
    instance = FakeHeartbeat.instances[-1]
    assert received == [instance]
    assert instance.path == heartbeat_path
    assert instance.metadata["worker"] == "causal_level_band_prospective"
    assert instance.phases == ["starting_cycle", "fake_work", "cycle_complete"]
    assert instance.closed is True
    assert causal.DEFAULT_PROGRESS_HEARTBEAT != causal.DEFAULT_HEARTBEAT


def test_direct_main_wires_independent_progress_heartbeat(
    tmp_path: Path,
    monkeypatch,
) -> None:
    received: list[FakeHeartbeat] = []

    def fake_run_once(*_args: Any, heartbeat: FakeHeartbeat, **_kwargs: Any) -> dict:
        received.append(heartbeat)
        heartbeat.mark_progress(phase="fake_work")
        return {}

    FakeHeartbeat.instances.clear()
    heartbeat_path = tmp_path / "direct-progress.json"
    monkeypatch.setattr(direct, "WorkerHeartbeat", FakeHeartbeat)
    monkeypatch.setattr(direct, "run_once", fake_run_once)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "oanda_direct_source_response.py",
            "--once",
            "--progress-heartbeat",
            str(heartbeat_path),
        ],
    )

    assert direct.main() == 0
    instance = FakeHeartbeat.instances[-1]
    assert received == [instance]
    assert instance.path == heartbeat_path
    assert instance.metadata["worker"] == "direct_source_response"
    assert instance.phases == ["starting_cycle", "fake_work", "cycle_complete"]
    assert instance.closed is True
