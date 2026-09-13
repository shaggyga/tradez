"""Reuse reviewed handoff implementations with two fixed continuation paths.

The published early-close report, validation and source archive remain intact.
The original implementations and their source pins are not edited. Only the
module-local report/output path constants are redirected for this invocation.
"""
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
BASE = Path(__file__).resolve().parent
ROOT = BASE.parent / 'trad'
REPORT = 'docs/FOREX_CONTINUATION_TO_0900_20260909.md'
VALIDATION = 'FOREX_CONTINUATION_TO_0900_VALIDATION_20260909.json'
IMPLEMENTATIONS = {
    'validate': ('assemble_final_validation_v1.py', '578cd4c2a8695da67d05c563b2fc721d04d69e54a04389e953d7ffbdd6a60dbf'),
    'publish': ('publish_overnight_vault_v1.py', '122486189afba8925010643c4d3e1dda88bef18409d604074de43eae63547fe5'),
}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def save(path, value):
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    with path.open('xb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    if path.read_bytes() != raw:
        raise ValueError('continuation_invocation_readback')
    return dict(path=str(path), sha256=sha(raw), bytes=len(raw))


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--phase', required=True, choices=tuple(IMPLEMENTATIONS))
    args, forwarded = parser.parse_known_args()
    name, expected = IMPLEMENTATIONS[args.phase]
    source = BASE / name
    raw = source.read_bytes()
    if sha(raw) != expected:
        raise ValueError('reviewed_handoff_implementation_changed')
    own = Path(__file__).read_bytes()
    invocation = BASE / ('CONTINUATION_' + args.phase.upper() + '_INVOCATION_20260909.json')
    started = time.time()
    if not math.isfinite(started) or started <= 0:
        raise ValueError('continuation_handoff_invalid_start_clock')
    record = dict(schema_version='continuation_fixed_path_handoff_invocation_v1_20260909',
        phase=args.phase, started_epoch=started, wrapper_sha256=sha(own),
        reviewed_implementation=dict(path=str(source), sha256=expected),
        fixed_report=str(ROOT / REPORT), fixed_validation=str(ROOT / VALIDATION),
        scope='Original reviewed logic with two fixed module-local path overrides; no original source or early-close artifact mutation.',
        broker_requests=False, registry_changes=False, runtime_changes=False)
    # Each phase has an exclusive invocation record; no implicit retry or overwrite.
    save(invocation, record)
    spec = importlib.util.spec_from_file_location('reviewed_continuation_' + args.phase, source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if args.phase == 'validate':
        module.REPORT = ROOT / REPORT
        module.OUTPUT = ROOT / VALIDATION
    else:
        module.REPORT = REPORT
        module.VALIDATION = VALIDATION
    old_args = sys.argv
    try:
        sys.argv = [str(source), *forwarded]
        module.main()
    finally:
        sys.argv = old_args
    if sha(source.read_bytes()) != expected or Path(__file__).read_bytes() != own:
        raise ValueError('continuation_handoff_source_changed')
    finished = time.time()
    if not math.isfinite(finished) or finished < started:
        raise ValueError('continuation_handoff_invalid_completion_clock')
    completed = dict(record, completed_epoch=finished, status='reviewed_handoff_returned_successfully',
        invocation_sha256=sha(invocation.read_bytes()))
    result = save(BASE / ('CONTINUATION_' + args.phase.upper() + '_COMPLETED_20260909.json'), completed)
    print(json.dumps(dict(continuation_phase=args.phase, wrapper_receipt=result)))


if __name__ == '__main__':
    main()
