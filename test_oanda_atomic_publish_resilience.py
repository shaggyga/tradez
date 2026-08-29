from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

import pytest

import oanda_edge_evidence_worker as edge_worker
import oanda_executable_opportunity_prospective as opportunity
import oanda_live_move_news_snapshot as live_move
import oanda_official_release_fast_mapper as fast_mapper
import oanda_official_release_fast_response_watch as response_watch
import oanda_source_conditioned_currency_rank_v1 as currency_rank


@dataclass(frozen=True)
class AtomicCase:
    name: str
    module: ModuleType
    publish: Callable[[Path], None]
    read: Callable[[Path], Any]


CASES = (
    AtomicCase(
        "edge_evidence",
        edge_worker,
        lambda path: edge_worker.atomic_json(path, {"ok": True}),
        lambda path: json.loads(path.read_text(encoding="utf-8")),
    ),
    AtomicCase(
        "executable_opportunity",
        opportunity,
        lambda path: opportunity.atomic(path, "published\n"),
        lambda path: path.read_text(encoding="utf-8"),
    ),
    AtomicCase(
        "live_move_news",
        live_move,
        lambda path: live_move.atomic_json(path, {"ok": True}),
        lambda path: json.loads(path.read_text(encoding="utf-8")),
    ),
    AtomicCase(
        "official_fast_mapper",
        fast_mapper,
        lambda path: fast_mapper.write_json_atomic(path, {"ok": True}),
        lambda path: json.loads(path.read_text(encoding="utf-8")),
    ),
    AtomicCase(
        "official_response_watch",
        response_watch,
        lambda path: response_watch.atomic_write_json(path, {"ok": True}),
        lambda path: json.loads(path.read_text(encoding="utf-8")),
    ),
    AtomicCase(
        "source_conditioned_currency_rank",
        currency_rank,
        lambda path: currency_rank.atomic_write(path, "published\n"),
        lambda path: path.read_text(encoding="utf-8"),
    ),
)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_atomic_publish_retries_transient_windows_replace_denial(
    case: AtomicCase,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "state.json"
    real_replace = case.module.os.replace
    attempts: list[tuple[Path, Path]] = []

    def flaky_replace(source: Path, destination: Path) -> None:
        attempts.append((Path(source), Path(destination)))
        if len(attempts) == 1:
            raise PermissionError("transient target lock")
        real_replace(source, destination)

    monkeypatch.setattr(case.module.os, "replace", flaky_replace)
    monkeypatch.setattr(case.module.time, "sleep", lambda _: None)

    case.publish(target)

    assert case.read(target) in ({"ok": True}, "published\n")
    assert len(attempts) == 2
    assert attempts[0][0] == attempts[1][0]
    assert attempts[0][0].name.startswith(f".{target.name}.")
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_atomic_publish_cleans_temp_and_raises_after_bounded_retries(
    case: AtomicCase,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "state.json"
    attempts = 0

    def denied_replace(_source: Path, _destination: Path) -> None:
        nonlocal attempts
        attempts += 1
        raise PermissionError("persistent target lock")

    monkeypatch.setattr(case.module.os, "replace", denied_replace)
    monkeypatch.setattr(case.module.time, "sleep", lambda _: None)

    with pytest.raises(PermissionError, match="persistent target lock"):
        case.publish(target)

    assert attempts == 8
    assert not target.exists()
    assert not list(tmp_path.glob("*.tmp"))
