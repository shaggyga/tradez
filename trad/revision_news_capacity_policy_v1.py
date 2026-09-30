"""Explicit process-local resource policy for the September30 news successor.

The original store format, source identities, immutable receipts and their clocks
are unchanged. Only named resource ceilings change. A successor transport and IO
source graph must include this file; original workers never import this module.
This is not authority to reinterpret old captures under a new policy.
"""
from pathlib import Path
import hashlib
import importlib

SCHEMA = 'revision_news_capacity_policy_v1_20260930'
MiB = 1024 ** 2
GiB = 1024 ** 3
# (exact original source, {name: (original, successor)}). No clock, freshness,
# batch, per-object, expansion-per-step or numerical/model limit is changed.
# Read transactions gain a five-second ceiling; nonauthorizing cold replay gains
# ten minutes. Current capture freshness and its 30-second outer bound do not change.
POLICY = {
    'compact_projection_store_v1': (
        'c13b3a0f73949be8155e1e626462607207cad99eabaf7b90db964487111c67b0', {
            'MAX_OBJECTS': (20000, 65536),
            'MAX_COMPRESSED': (192*MiB, 768*MiB),
            'MAX_EXPANDED_PREFIX': (2*GiB, 8*GiB)}),
    'projection_revision_admission_v1': (
        '28e4738317c3166deb656f9935e356dc8314110d056e0a471fe8eb96b766da3d', {
            'MAX_DATABASE': (384*MiB, 1536*MiB),
            'MAX_CAPTURE_SECONDS': (1.0, 5.0),
            'MAX_COLD_READ_SECONDS': (300, 600),
            'MAX_JSON_TOTAL': (256*MiB, 512*MiB),
            'MAX_CAPTURE_BYTES': (256*MiB, 1024*MiB),
            'MAX_ATTEMPTS': (4096, 16384)}),
    'projection_revision_consumer_v1': (
        '5c885ae837394398a2dba3dd0b8050305ac17cb95cc5e05578042ab2551ec485', {
            'MAX_DATABASE': (96*MiB, 384*MiB),
            'MAX_JSON': (64*MiB, 128*MiB),
            'MAX_OBSERVATIONS': (4096, 16384),
            'MAX_SCAN_OBJECTS': (4096, 16384)}),
    'projection_revision_consumer_v2': (
        '6936a4e24a06e3c21c468004ac5478ba4a882c3928943fd09e9dc9da15bc7fe6', {
            'MAX_CACHE_BYTES': (192*MiB, 384*MiB),
            'MAX_CACHE_PAYLOAD': (128*MiB, 256*MiB),
            'MAX_INVENTORY_ROWS': (12288, 49152)}),
}
_installed = False


def _owners():
    root = Path(__file__).absolute().parent
    result = {}
    # Import the complete graph before applying values: derived legacy defaults
    # must be authenticated against the original values, not import ordering.
    for name, (expected, _) in POLICY.items():
        path = root / (name + '.py')
        raw = path.read_bytes()
        if len(raw) > 2*MiB or hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError('capacity_original_source_changed:' + name)
        owner = importlib.import_module(name)
        if Path(owner.__file__).absolute() != path:
            raise ValueError('capacity_original_owner_path_changed:' + name)
        result[name] = owner
    return result


def install():
    """Install once before opening any stores; reject unexpected mutations."""
    global _installed
    owners = _owners()
    for name, (_, limits) in POLICY.items():
        for field, (old, new) in limits.items():
            expected = new if _installed else old
            if type(getattr(owners[name], field)) is not type(expected) or getattr(owners[name], field) != expected:
                raise ValueError('capacity_unexpected_runtime_limit:' + name + '.' + field)
    if not _installed:
        for name, (_, limits) in POLICY.items():
            for field, (_, new) in limits.items():
                setattr(owners[name], field, new)
        _installed = True
    return source_binding()


def source_binding():
    """Every successor boundary verifies the installed policy and binds its bytes."""
    if not _installed:
        raise ValueError('capacity_policy_not_installed')
    owners = _owners()
    for name, (_, limits) in POLICY.items():
        for field, (_, expected) in limits.items():
            if type(getattr(owners[name], field)) is not type(expected) or getattr(owners[name], field) != expected:
                raise ValueError('capacity_runtime_policy_changed:' + name + '.' + field)
    path = Path(__file__).absolute()
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest()}
