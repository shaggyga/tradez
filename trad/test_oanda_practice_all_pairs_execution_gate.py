import argparse
import importlib
import sys
import types
from pathlib import Path


def import_subject():
    scalper = types.ModuleType("oanda_practice_eurusd_micro_scalper")
    scalper.DEFAULT_CREDS = Path("creds")
    scalper.DEFAULT_LOG_DIR = Path("data/test_logs")

    class FatalSessionError(Exception):
        pass

    class OandaApiError(Exception):
        pass

    class PracticeScalper:
        pass

    scalper.FatalSessionError = FatalSessionError
    scalper.OandaApiError = OandaApiError
    scalper.PracticeScalper = PracticeScalper
    scalper.infer_pip_size = lambda instrument: 0.0001
    scalper.normalize_instrument = lambda x: x.strip().upper().replace("/", "_")
    scalper.parse_args = lambda argv=None: argparse.Namespace()
    scalper.quote_from_price_payload = lambda *args, **kwargs: None
    scalper.safe_float = lambda value, default=0.0: float(value) if value not in (None, "") else default

    pair = types.ModuleType("oanda_practice_pair_rotation_scalper")
    pair.oanda_get = lambda *args, **kwargs: None
    pair.read_credentials = lambda *args, **kwargs: ("token", "001-001-000000-006")
    pair.tradeable_currency_instruments = lambda *args, **kwargs: []

    sys.modules["oanda_practice_eurusd_micro_scalper"] = scalper
    sys.modules["oanda_practice_pair_rotation_scalper"] = pair
    sys.modules.pop("oanda_practice_all_pairs_opportunity_scalper", None)
    return importlib.import_module("oanda_practice_all_pairs_opportunity_scalper")


def test_execution_gate_loss_lock_blocks_live_even_with_allow_flag(tmp_path):
    subject = import_subject()
    lock = tmp_path / "lock.json"
    lock.write_text('{"locked": true, "reason": "fixture_loss"}', encoding="utf-8")
    args = subject.parse_args([
        "--duration-sec", "1",
        "--execution-loss-lock-file", str(lock),
        "--allow-unproven-execution",
    ])
    assert subject.execution_gate_rejection(args, "001-001-000000-006") == "execution_loss_lock:fixture_loss"


def test_execution_gate_dry_run_bypasses_loss_lock(tmp_path):
    subject = import_subject()
    lock = tmp_path / "lock.json"
    lock.write_text('{"locked": true, "reason": "fixture_loss"}', encoding="utf-8")
    args = subject.parse_args(["--duration-sec", "1", "--execution-loss-lock-file", str(lock), "--dry-run"])
    assert subject.execution_gate_rejection(args, "001-001-000000-006") is None


def test_execution_gate_requires_proof_when_no_lock(tmp_path):
    subject = import_subject()
    lock = tmp_path / "missing_lock.json"
    proof = tmp_path / "missing_proof.json"
    args = subject.parse_args([
        "--duration-sec", "1",
        "--execution-loss-lock-file", str(lock),
        "--execution-proof-file", str(proof),
    ])
    assert subject.execution_gate_rejection(args, "001-001-000000-006") == "missing_execution_proof_file"


def test_execution_gate_accepts_matching_proof_when_unlocked(tmp_path):
    subject = import_subject()
    lock = tmp_path / "missing_lock.json"
    proof = tmp_path / "proof.json"
    proof.write_text(
        '{"allow_live_execution": true, "account_suffix": "-006", "strategy": "pullback"}',
        encoding="utf-8",
    )
    args = subject.parse_args([
        "--duration-sec", "1",
        "--execution-loss-lock-file", str(lock),
        "--execution-proof-file", str(proof),
        "--strategy", "pullback",
    ])
    assert subject.execution_gate_rejection(args, "001-001-000000-006") is None
