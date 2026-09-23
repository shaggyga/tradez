"""Retain exact verified early-close receipts as continuation dependencies."""
from pathlib import Path
import hashlib
import json
import os
import time

BASE = Path(__file__).resolve().parent
ROOT = BASE.parent / 'trad'
FILES = [
    (ROOT / 'FOREX_OVERNIGHT_CURVE_BUILDOUT_VALIDATION_20260909.json',
     'ad6877e724d7bfeaac4118e6e715ef0fa5504b78c83c684c37a1fd2915a0cfcc'),
    (BASE / 'vault_publication_final_001/OVERNIGHT_VAULT_PUBLICATION_20260909.json',
     'c540753d8fc026131d591cf2faa7b14098049412cad645cf455c4e0a1852e9c1'),
]


def main():
    output = BASE / 'continuation_prior_publication_001'
    if output.exists():
        raise ValueError('fresh_prior_publication_directory_required')
    payloads = []
    for path, expected in FILES:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 4 * 1024 * 1024:
            raise ValueError('prior_receipt_file_bound')
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError('prior_receipt_changed')
        payloads.append((path, expected, raw))
    output.mkdir()
    records = []
    for path, digest, raw in payloads:
        target = output / path.name
        with target.open('xb') as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        if target.read_bytes() != raw or path.read_bytes() != raw:
            raise ValueError('prior_receipt_retention_changed')
        records.append(dict(original_path=str(path), retained_path=str(target), sha256=digest, bytes=len(raw)))
    value = dict(schema_version='continuation_prior_publication_retention_v1_20260909',
        retained_epoch=time.time(), records=records,
        early_close_report_sha256='fc3777520d1c2c5d3b1801b74e76274c77607d044e073c279beaecc11100adea',
        prior_archive_sha256='5c29f4ca4fb606c8a621d8d5c3cda423cb84a5204848b07419c5a621982c8b67',
        scope='Dated 629-member early-close validation and its successful publication receipt; not a continuation result or future source publication.',
        originals_changed=False, runtime_changes=False, vault_writes=False)
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    target = output / 'PRIOR_PUBLICATION_RETENTION_20260909.json'
    with target.open('xb') as handle:
        handle.write(raw)
    print(json.dumps(dict(path=str(target), sha256=hashlib.sha256(raw).hexdigest(), retained_files=len(records))))


if __name__ == '__main__':
    main()
