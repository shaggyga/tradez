from __future__ import annotations

import gzip
from hashlib import sha256

from src.forex_system.research.sequential_deterministic_io_v1 import (
    CANONICAL_GZIP_OS_BYTE,
    canonical_gzip,
)


FIXTURE = b"sequential-cross-runtime-fixture\n"
EXPECTED_HEX = "1f8b08000000000002ff2b4e2d2c4dcd2bc94cccd14d2eca2f2ed62d2a05f2725375d3322b4a4a8b52b900ab818f6321000000"


def test_canonical_gzip_has_portable_frozen_header_and_round_trip() -> None:
    payload = canonical_gzip(FIXTURE, compresslevel=9)
    assert payload[:3] == b"\x1f\x8b\x08"
    assert payload[3] == 0
    assert payload[4:8] == b"\x00\x00\x00\x00"
    assert payload[9] == CANONICAL_GZIP_OS_BYTE == 0xFF
    assert gzip.decompress(payload) == FIXTURE


def test_canonical_gzip_is_byte_exact_and_repeatable() -> None:
    first = canonical_gzip(FIXTURE, compresslevel=9)
    second = canonical_gzip(FIXTURE, compresslevel=9)
    assert first == second
    assert first.hex() == EXPECTED_HEX
    assert sha256(first).hexdigest() == sha256(bytes.fromhex(EXPECTED_HEX)).hexdigest()
