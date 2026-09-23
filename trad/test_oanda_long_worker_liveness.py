from pathlib import Path
import re


ROOT = Path(__file__).resolve().parent
SUPERVISOR = ROOT / "oanda_always_on_supervisor.ps1"


def _worker_block(name: str) -> str:
    text = SUPERVISOR.read_text(encoding="utf-8")
    match = re.search(
        rf'-Name\s+"{re.escape(name)}"(?P<block>.*?)(?=\n\s*\$managed\s*\+=\s*Start-ManagedProcess|\Z)',
        text,
        flags=re.DOTALL,
    )
    assert match is not None
    return match.group("block")


def test_long_workers_are_supervised_by_independent_heartbeats() -> None:
    expected = {
        "macro_surprise_ledger": (
            "--heartbeat",
            "macro_surprise_heartbeat_v1.json",
        ),
        "macro_release_breakout_research": (
            "--heartbeat",
            "macro_release_breakout_research_heartbeat_v1.json"
        ),
        "currency_macro_release_breakout_research": (
            "--heartbeat",
            "currency_macro_release_breakout_research_heartbeat_v1.json"
        ),
        "all68_m1_forward_archive": (
            "--heartbeat",
            "all68_m1_forward_update_heartbeat_v1.json",
        ),
        "direct_source_response": (
            "--progress-heartbeat",
            "direct_source_response_progress_heartbeat_v1.json",
        ),
        "causal_level_band_prospective": (
            "--progress-heartbeat",
            "causal_level_band_prospective_progress_heartbeat_v1.json",
        ),
    }
    for worker, (argument, heartbeat) in expected.items():
        block = _worker_block(worker)
        assert f'"{argument}", (Join-Path $State "{heartbeat}")' in block
        assert f'LiteralPath = (Join-Path $State "{heartbeat}")' in block
        assert "MaxProgressAgeSec" in block
        assert "MaxAgeSec = 30" in block


def test_causal_and_direct_progress_gates_cover_active_cycle_phases() -> None:
    expected_phases = {
        "direct_source_response": (
            "ingesting_macro",
            "maturing_targets",
            "publishing_cycle",
        ),
        "causal_level_band_prospective": (
            "refreshing_contexts",
            "maturing_outcomes",
            "publishing_cycle",
        ),
    }
    for worker, phases in expected_phases.items():
        block = _worker_block(worker)
        for phase in phases:
            assert f'"{phase}"' in block


def test_archive_progress_gate_tracks_pair_progress() -> None:
    block = _worker_block("all68_m1_forward_archive")
    assert 'ProgressPhases = @("updating_pairs")' in block
    assert "MaxProgressAgeSec = 300" in block
