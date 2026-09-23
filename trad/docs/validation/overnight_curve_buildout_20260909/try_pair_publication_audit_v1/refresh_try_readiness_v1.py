"""Read the original bounded readiness tail again; no fit, database or quote read."""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1] / 'trad'
SOURCE = HERE / 'audit_try_pair_publication_v1.py'
SOURCE_SHA = '8e3188c7f4a977c2cccae9dc478c063b4193f7fc44c1765f80da08afaec35f79'
REGISTRY = ROOT / 'config/joint_price_news_study_v3_20260908.json'
REGISTRY_SHA = 'ee075e69e80ca56dfdf45abe1f7a1a301612176e67f7622eab1adf2bd9af8771'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def deny_external(event, args):
    if event.startswith('sqlite3.connect') or event in ('socket.connect', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'):
        raise RuntimeError('readiness_refresh_forbids_database_network_or_process')


def bindings():
    raw = REGISTRY.read_bytes()
    if sha(raw) != REGISTRY_SHA or sha(SOURCE.read_bytes()) != SOURCE_SHA:
        raise ValueError('accepted_readiness_source_changed')
    expected = json.loads(raw)['source_bindings']
    for name, digest in expected.items():
        if sha((ROOT / name).read_bytes()) != digest:
            raise ValueError('registered_source_changed')
    return expected


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    output = parser.parse_args().output.absolute()
    if output.parent != HERE or output.suffix != '.json' or output.exists() or output.is_symlink():
        raise ValueError('fresh_bounded_output_required')
    sys.addaudithook(deny_external)
    own = Path(__file__).read_bytes()
    started = time.time()
    before = bindings()
    spec = importlib.util.spec_from_file_location('accepted_try_readiness_audit', SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.registry()
    original = module.readiness_tail()
    after = bindings()
    finished = time.time()
    if not (before == after and Path(__file__).read_bytes() == own and math.isfinite(started)
            and math.isfinite(finished) and 0 < started <= original['read_started_epoch']
            <= original['read_completed_epoch'] <= finished):
        raise ValueError('readiness_observation_integrity')
    result = dict(schema_version='try_readiness_tail_refresh_v1_20260909',
        status='retained_readiness_tail_observed', started_epoch=started, completed_epoch=finished,
        helper_sha256=sha(own), accepted_reader_sha256=SOURCE_SHA, registry_sha256=REGISTRY_SHA,
        registered_sources_unchanged=True, source_bindings=before, original_readiness_tail=original,
        database_reads=0, network_requests=0, forecasts_issued=0, source_or_runtime_writes=False,
        limitations=['The latest retained record keeps its original event clock and may be older than this read.',
            'Only the accepted bounded complete-line tail was read; absent records do not prove empty full history.',
            'Training readiness was not recomputed, and no missing rows, clocks or forecasts were invented.'])
    raw = (json.dumps(result, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    if len(raw) > 4 * 1024 * 1024:
        raise ValueError('readiness_output_bound')
    with output.open('xb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    if output.read_bytes() != raw:
        raise ValueError('readiness_output_readback')
    print(json.dumps(dict(path=str(output), sha256=sha(raw), bytes=len(raw),
        latest={pair: value['latest'] for pair, value in original['per_pair'].items()})))


if __name__ == '__main__':
    main()
