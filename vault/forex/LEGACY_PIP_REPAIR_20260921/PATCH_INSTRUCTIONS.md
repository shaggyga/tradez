# Reviewed integration patch

Apply this only to an isolated source copy, after verifying that the reviewed
metadata still has SHA-256
`c9464343f012e0bdd83582480a0044bf1c6774eef2622f312bcdd4599e3869c5`.

In `oanda_indicator_alignment_research.py`, add `hashlib` and
`functools.lru_cache` imports. Beside `PROJECT_ROOT`, add:

```python
PIP_METADATA_PATH = PROJECT_ROOT / "config" / "pair_local_operational_v2_20260913.json"
PIP_METADATA_SHA256 = "c9464343f012e0bdd83582480a0044bf1c6774eef2622f312bcdd4599e3869c5"
```

Replace the legacy `pips_size` shortcut with this helper and lookup:

```python
@lru_cache(maxsize=1)
def registered_pip_sizes() -> Dict[str, float]:
    path = PIP_METADATA_PATH
    if any(item.is_symlink() or item.is_junction() for item in (path, *path.parents)):
        raise ValueError("pip_metadata_reparse_refused")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != PIP_METADATA_SHA256:
        raise ValueError("pip_metadata_source_changed")
    pairs = json.loads(raw).get("pairs")
    if not isinstance(pairs, dict) or len(pairs) != 68:
        raise ValueError("pip_metadata_requires_all68")
    sizes: Dict[str, float] = {}
    for pair, value in pairs.items():
        try:
            pip = float(value["pip_size"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid_registered_pip_size") from exc
        if not math.isfinite(pip) or pip <= 0:
            raise ValueError("invalid_registered_pip_size")
        sizes[str(pair).upper()] = pip
    return sizes


def pips_size(instrument: str) -> float:
    normalized = instrument.upper().replace("/", "_")
    try:
        return registered_pip_sizes()[normalized]
    except KeyError:
        raise ValueError("unregistered_instrument_pip_size") from None
```

Then run the saved regression test against an isolated copy of the rotation
consumer. Do not merge this patch merely because it passes the unit test; the
pending all-68 chronological replay is the next integration gate.
