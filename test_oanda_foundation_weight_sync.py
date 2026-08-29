from __future__ import annotations

import oanda_foundation_weight_sync as sync


def test_output_tail_handles_failed_subprocess_reader() -> None:
    assert sync.output_tail(None) == ""
    assert sync.output_tail("abcdef", 3) == "def"
