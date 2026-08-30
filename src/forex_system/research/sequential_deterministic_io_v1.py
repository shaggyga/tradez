"""Cross-runtime deterministic binary encodings for sequential research artifacts."""

from __future__ import annotations

import gzip
import io


CANONICAL_GZIP_OS_BYTE = 0xFF


def canonical_gzip(raw: bytes, compresslevel: int = 9) -> bytes:
    """Return an RFC-1952 stream stable across CPython 3.12 and 3.13.

    ``gzip.compress(..., mtime=0)`` delegates to zlib in some CPython versions,
    which leaks the host OS byte into byte 9 of the header. ``GzipFile`` emits
    the portable 0xFF marker; the explicit normalization is a defensive frozen
    contract and does not affect the compressed body, CRC, or raw-size trailer.
    """
    level = int(compresslevel)
    if not 0 <= level <= 9:
        raise ValueError("gzip compresslevel must be between 0 and 9")
    buffer = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", compresslevel=level, fileobj=buffer, mtime=0) as stream:
        stream.write(bytes(raw))
    payload = bytearray(buffer.getvalue())
    if len(payload) < 18 or payload[:3] != b"\x1f\x8b\x08":
        raise ValueError("canonical gzip encoder produced invalid header")
    payload[9] = CANONICAL_GZIP_OS_BYTE
    return bytes(payload)


__all__ = ["CANONICAL_GZIP_OS_BYTE", "canonical_gzip"]
